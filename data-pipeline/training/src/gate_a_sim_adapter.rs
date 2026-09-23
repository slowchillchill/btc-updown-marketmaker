use std::collections::VecDeque;
use std::sync::Arc;

use contracts::{CanonicalEventRecordV1, ExecutionMarketEventV3, Sha256Hex};

use crate::PublishedGateAFinal;
use crate::gate_a::{GateAArtifacts, GateAError, PlatformReplayCursor, VisibilityReplayCursor};

pub use contracts::ReplayIndexClosureV1;

struct PositionedCanonicalRecord {
    logical_position: u64,
    index_at_utc_ms: i64,
    record: CanonicalEventRecordV1,
}

trait CanonicalIndexReplayCursor {
    fn next_logical_position(&self) -> u64;

    fn next_page(&mut self, limit: usize) -> Result<Vec<CanonicalEventRecordV1>, GateAError>;

    fn index_at_utc_ms(record: &CanonicalEventRecordV1) -> i64;
}

impl CanonicalIndexReplayCursor for PlatformReplayCursor {
    fn next_logical_position(&self) -> u64 {
        PlatformReplayCursor::next_logical_position(self)
    }

    fn next_page(&mut self, limit: usize) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        PlatformReplayCursor::next_page(self, limit)
    }

    fn index_at_utc_ms(record: &CanonicalEventRecordV1) -> i64 {
        match record {
            CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
                event.time.source_event_at_utc_ms
            }
            CanonicalEventRecordV1::ReferenceEvent(event) => event.time.source_event_at_utc_ms,
        }
        .expect("平台回放读取器只返回具有事件时间索引的记录")
    }
}

impl CanonicalIndexReplayCursor for VisibilityReplayCursor {
    fn next_logical_position(&self) -> u64 {
        VisibilityReplayCursor::next_logical_position(self)
    }

    fn next_page(&mut self, limit: usize) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        VisibilityReplayCursor::next_page(self, limit)
    }

    fn index_at_utc_ms(record: &CanonicalEventRecordV1) -> i64 {
        match record {
            CanonicalEventRecordV1::ExecutionMarketEvent(event) => event.time.received_at_utc_ms,
            CanonicalEventRecordV1::ReferenceEvent(event) => event.time.received_at_utc_ms,
        }
    }
}

struct ReplayPath<C> {
    cursor: C,
    buffered: VecDeque<PositionedCanonicalRecord>,
}

impl<C: CanonicalIndexReplayCursor> ReplayPath<C> {
    fn new(cursor: C) -> Self {
        Self {
            cursor,
            buffered: VecDeque::new(),
        }
    }

    fn next_logical_position(&self) -> u64 {
        self.buffered.front().map_or_else(
            || self.cursor.next_logical_position(),
            |record| record.logical_position,
        )
    }

    fn fill_page(&mut self, max_page_rows: usize) -> Result<(), GateAError> {
        if !self.buffered.is_empty() {
            return Ok(());
        }
        let page_start = self.cursor.next_logical_position();
        let page = self.cursor.next_page(max_page_rows)?;
        self.buffered
            .extend(page.into_iter().enumerate().map(|(offset, record)| {
                PositionedCanonicalRecord {
                    logical_position: page_start
                        + u64::try_from(offset).expect("页偏移必须适合 u64"),
                    index_at_utc_ms: C::index_at_utc_ms(&record),
                    record,
                }
            }));
        Ok(())
    }

    fn next_bucket(
        &mut self,
        max_page_rows: usize,
    ) -> Result<Option<ClosedExecutionMarketIndexBucketV1>, GateAError> {
        self.fill_page(max_page_rows)?;
        let Some(first) = self.buffered.pop_front() else {
            return Ok(None);
        };
        let index_at_utc_ms = first.index_at_utc_ms;
        let mut records = vec![first];
        let closure = loop {
            self.fill_page(max_page_rows)?;
            let Some(next_index_at_utc_ms) =
                self.buffered.front().map(|record| record.index_at_utc_ms)
            else {
                break ReplayIndexClosureV1::EndOfInput;
            };
            if next_index_at_utc_ms != index_at_utc_ms {
                break ReplayIndexClosureV1::NextGlobalEventAt(next_index_at_utc_ms);
            }
            records.push(
                self.buffered
                    .pop_front()
                    .expect("刚刚读取的同毫秒 lookahead 必须仍存在"),
            );
        };

        Ok(Some(ClosedExecutionMarketIndexBucketV1::from_records(
            index_at_utc_ms,
            records,
            closure,
        )))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosedExecutionMarketIndexBucketV1 {
    index_at_utc_ms: i64,
    start_logical_position: u64,
    next_logical_position: u64,
    event_logical_positions: Arc<[u64]>,
    events: Arc<[ExecutionMarketEventV3]>,
    closure: ReplayIndexClosureV1,
}

impl ClosedExecutionMarketIndexBucketV1 {
    fn from_records(
        index_at_utc_ms: i64,
        records: Vec<PositionedCanonicalRecord>,
        closure: ReplayIndexClosureV1,
    ) -> Self {
        let start_logical_position = records
            .first()
            .expect("闭合桶必须至少包含一条全局索引记录")
            .logical_position;
        let next_logical_position = records
            .last()
            .expect("闭合桶必须至少包含一条全局索引记录")
            .logical_position
            + 1;
        let mut event_logical_positions = Vec::new();
        let mut events = Vec::new();
        for record in records {
            if let CanonicalEventRecordV1::ExecutionMarketEvent(event) = record.record {
                event_logical_positions.push(record.logical_position);
                events.push(*event);
            }
        }
        Self {
            index_at_utc_ms,
            start_logical_position,
            next_logical_position,
            event_logical_positions: Arc::from(event_logical_positions),
            events: Arc::from(events),
            closure,
        }
    }

    pub const fn index_at_utc_ms(&self) -> i64 {
        self.index_at_utc_ms
    }

    pub const fn start_logical_position(&self) -> u64 {
        self.start_logical_position
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.next_logical_position
    }

    pub fn event_logical_positions(&self) -> &[u64] {
        &self.event_logical_positions
    }

    pub fn events(&self) -> &Arc<[ExecutionMarketEventV3]> {
        &self.events
    }

    pub const fn closure(&self) -> ReplayIndexClosureV1 {
        self.closure
    }
}

pub struct GateASimulationReplayAdapter {
    platform: ReplayPath<PlatformReplayCursor>,
    visibility: ReplayPath<VisibilityReplayCursor>,
    max_page_rows: usize,
}

impl GateASimulationReplayAdapter {
    pub fn open(
        final_gate_a: &PublishedGateAFinal,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        Self::open_candidate(
            final_gate_a.artifacts(),
            batch_rows,
            max_batch_bytes,
            max_page_rows,
        )
    }

    #[cfg(test)]
    pub(crate) fn open_candidate_for_tests(
        candidate: &GateAArtifacts,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        Self::open_candidate(candidate, batch_rows, max_batch_bytes, max_page_rows)
    }

    fn open_candidate(
        candidate: &GateAArtifacts,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        Ok(Self {
            platform: ReplayPath::new(PlatformReplayCursor::open(
                candidate,
                None,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?),
            visibility: ReplayPath::new(VisibilityReplayCursor::open(
                candidate,
                None,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?),
            max_page_rows,
        })
    }

    pub fn canonical_event_store_manifest_sha256(&self) -> &Sha256Hex {
        self.platform.cursor.canonical_event_store_manifest_sha256()
    }

    pub fn event_time_index_manifest_sha256(&self) -> &Sha256Hex {
        self.platform.cursor.event_time_index_manifest_sha256()
    }

    pub fn visibility_index_manifest_sha256(&self) -> &Sha256Hex {
        self.visibility.cursor.visibility_index_manifest_sha256()
    }

    pub fn platform_next_logical_position(&self) -> u64 {
        self.platform.next_logical_position()
    }

    pub fn visibility_next_logical_position(&self) -> u64 {
        self.visibility.next_logical_position()
    }

    pub const fn max_page_rows(&self) -> usize {
        self.max_page_rows
    }

    pub fn next_platform_bucket(
        &mut self,
    ) -> Result<Option<ClosedExecutionMarketIndexBucketV1>, GateAError> {
        self.platform.next_bucket(self.max_page_rows)
    }

    pub fn next_visibility_bucket(
        &mut self,
    ) -> Result<Option<ClosedExecutionMarketIndexBucketV1>, GateAError> {
        self.visibility.next_bucket(self.max_page_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter(fixture: &crate::gate_a::tests::MicroCandidate) -> GateASimulationReplayAdapter {
        GateASimulationReplayAdapter::open_candidate_for_tests(
            &fixture.artifacts,
            fixture.output_config.sort.batch_rows,
            fixture.output_config.sort.max_run_bytes,
            1,
        )
        .unwrap()
    }

    fn event_ids(bucket: &ClosedExecutionMarketIndexBucketV1) -> Vec<&str> {
        bucket
            .events()
            .iter()
            .map(|event| event.event_id.0.as_str())
            .collect()
    }

    #[test]
    fn platform_replay_closes_cross_page_milliseconds_before_filtering_references() {
        let fixture = crate::gate_a::tests::build_sim_adapter_candidate();
        let mut adapter = adapter(&fixture);

        assert_eq!(adapter.max_page_rows(), 1);
        assert_eq!(adapter.platform_next_logical_position(), 0);
        assert_eq!(
            adapter.canonical_event_store_manifest_sha256(),
            fixture.artifacts.canonical_event_store.manifest_sha256()
        );
        assert_eq!(
            adapter.event_time_index_manifest_sha256(),
            fixture.artifacts.event_time_index.manifest_sha256()
        );

        let first = adapter.next_platform_bucket().unwrap().unwrap();
        assert_eq!(first.index_at_utc_ms(), 1_000);
        assert_eq!(
            (
                first.start_logical_position(),
                first.next_logical_position()
            ),
            (0, 3)
        );
        assert_eq!(first.event_logical_positions(), &[0, 2]);
        assert_eq!(
            event_ids(&first),
            vec![
                fixture.event_ids[0].0.as_str(),
                fixture.event_ids[2].0.as_str()
            ]
        );
        assert_eq!(
            first.closure(),
            ReplayIndexClosureV1::NextGlobalEventAt(3_000)
        );
        assert_eq!(adapter.platform_next_logical_position(), 3);

        let market = adapter.next_platform_bucket().unwrap().unwrap();
        assert_eq!(market.index_at_utc_ms(), 3_000);
        assert_eq!(
            (
                market.start_logical_position(),
                market.next_logical_position()
            ),
            (3, 4)
        );
        assert_eq!(market.event_logical_positions(), &[3]);
        assert_eq!(event_ids(&market), vec![fixture.event_ids[4].0.as_str()]);
        assert_eq!(
            market.closure(),
            ReplayIndexClosureV1::NextGlobalEventAt(4_000)
        );
        assert_eq!(adapter.platform_next_logical_position(), 4);

        let references_only_eof = adapter.next_platform_bucket().unwrap().unwrap();
        assert_eq!(references_only_eof.index_at_utc_ms(), 4_000);
        assert_eq!(
            (
                references_only_eof.start_logical_position(),
                references_only_eof.next_logical_position()
            ),
            (4, 5)
        );
        assert!(references_only_eof.event_logical_positions().is_empty());
        assert!(references_only_eof.events().is_empty());
        assert_eq!(
            references_only_eof.closure(),
            ReplayIndexClosureV1::EndOfInput
        );
        assert_eq!(adapter.platform_next_logical_position(), 5);
        assert!(adapter.next_platform_bucket().unwrap().is_none());
    }

    #[test]
    fn visibility_replay_keeps_native_positions_and_reference_only_cross_page_bucket() {
        let fixture = crate::gate_a::tests::build_sim_adapter_candidate();
        let mut adapter = adapter(&fixture);

        assert_eq!(adapter.visibility_next_logical_position(), 0);
        assert_eq!(
            adapter.visibility_index_manifest_sha256(),
            fixture.artifacts.visibility_index.manifest_sha256()
        );

        let references_only = adapter.next_visibility_bucket().unwrap().unwrap();
        assert_eq!(references_only.index_at_utc_ms(), 5_000);
        assert_eq!(
            (
                references_only.start_logical_position(),
                references_only.next_logical_position()
            ),
            (0, 2)
        );
        assert!(references_only.event_logical_positions().is_empty());
        assert!(references_only.events().is_empty());
        assert_eq!(
            references_only.closure(),
            ReplayIndexClosureV1::NextGlobalEventAt(5_500)
        );
        assert_eq!(adapter.visibility_next_logical_position(), 2);

        for (time, position, event_id, closure) in [
            (
                5_500,
                2,
                fixture.event_ids[0].0.as_str(),
                ReplayIndexClosureV1::NextGlobalEventAt(6_000),
            ),
            (
                6_000,
                3,
                fixture.event_ids[2].0.as_str(),
                ReplayIndexClosureV1::NextGlobalEventAt(7_000),
            ),
            (
                7_000,
                4,
                fixture.event_ids[4].0.as_str(),
                ReplayIndexClosureV1::EndOfInput,
            ),
        ] {
            let bucket = adapter.next_visibility_bucket().unwrap().unwrap();
            assert_eq!(bucket.index_at_utc_ms(), time);
            assert_eq!(
                (
                    bucket.start_logical_position(),
                    bucket.next_logical_position()
                ),
                (position, position + 1)
            );
            assert_eq!(bucket.event_logical_positions(), &[position]);
            assert_eq!(event_ids(&bucket), vec![event_id]);
            assert_eq!(bucket.closure(), closure);
        }
        assert_eq!(adapter.visibility_next_logical_position(), 5);
        assert!(adapter.next_visibility_bucket().unwrap().is_none());
    }
}
