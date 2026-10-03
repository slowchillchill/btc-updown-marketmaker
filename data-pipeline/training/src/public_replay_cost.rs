//! 仅测试构建的有限重放诊断；默认关闭，不提供生产接口。
use super::*;
use std::{cell::RefCell, time::Instant};
const PRIMARY: [&str; 6] = [
    "main_drain",
    "identity",
    "dependencies",
    "prepare",
    "core_push",
    "into_owned",
];
const RETAIN_SCANS: usize = 128;
const RETAIN_SLOW: usize = 16;
thread_local! { static STATE: RefCell<Option<Report>> = const { RefCell::new(None) }; }
#[derive(Default, Serialize)]
pub(crate) struct Times {
    calls: u64,
    completed: u64,
    ns: u128,
    max_ns: u128,
    rows: BTreeMap<&'static str, u64>,
}
impl Times {
    fn add(&mut self, ns: u128, completed: bool) {
        self.calls += 1;
        self.completed += u64::from(completed);
        self.ns += ns;
        self.max_ns = self.max_ns.max(ns);
    }
}
#[derive(Default, Clone, Serialize)]
pub(crate) struct Context {
    expected_input_position: u64,
    input_position: Option<u64>,
    kind: String,
    target_condition: Option<String>,
    historical_member_conditions: Vec<String>,
    source_message_id: Option<String>,
    dependency_refs: Vec<CanonicalInputRecordRefV1>,
}
#[derive(Default, Serialize)]
struct Entry {
    context: Context,
    outcome: String,
    total_ns: u128,
    primary_ns: BTreeMap<&'static str, u128>,
    nested_ns: BTreeMap<&'static str, u128>,
    residual_ns: i128,
    failed_spans: Vec<&'static str>,
}
#[derive(Default, Serialize)]
pub(crate) struct Scan {
    kind: &'static str,
    context: Context,
    cutoff: Option<u64>,
    cursor_before: Option<u64>,
    cursor_after: Option<u64>,
    open_attempted: bool,
    open_succeeded: bool,
    completed: bool,
    exhausted: Option<bool>,
    rows: BTreeMap<&'static str, u64>,
    twap_encountered: u64,
    first_twap_input: Option<u64>,
    last_twap_input: Option<u64>,
    matched_records: u64,
    returned: Option<usize>,
    ns: u128,
}
#[derive(Default, Serialize)]
struct ScanTotals {
    invocations: u64,
    open_attempts: u64,
    open_successes: u64,
    completed: u64,
    rows: u64,
    row_kinds: BTreeMap<&'static str, u64>,
    twap_encountered: u64,
    matched_records: u64,
    returned: u64,
    ns: u128,
}
impl ScanTotals {
    fn add(&mut self, d: &Scan) {
        self.invocations += 1;
        self.open_attempts += u64::from(d.open_attempted);
        self.open_successes += u64::from(d.open_succeeded);
        self.completed += u64::from(d.completed);
        self.rows += d.rows.values().sum::<u64>();
        for (kind, count) in &d.rows {
            *self.row_kinds.entry(kind).or_default() += count;
        }
        self.twap_encountered += d.twap_encountered;
        self.matched_records += d.matched_records;
        self.returned += d.returned.unwrap_or(0) as u64;
        self.ns += d.ns;
    }
}
#[derive(Serialize)]
struct PositionScans {
    context: Context,
    kinds: BTreeMap<&'static str, ScanTotals>,
}
#[derive(Default, Serialize)]
pub(crate) struct Report {
    entry_calls: u64,
    returned_inputs: u64,
    eof_returns: u64,
    failed_entries: u64,
    entry_total_ns: u128,
    residual_total_ns: i128,
    negative_residual_entries: u64,
    primary: BTreeMap<&'static str, Times>,
    nested: BTreeMap<&'static str, Times>,
    by_return_kind: BTreeMap<String, Times>,
    scan_totals: BTreeMap<&'static str, ScanTotals>,
    scan_by_input: BTreeMap<u64, PositionScans>,
    scans_without_input_context: u64,
    scans: Vec<Scan>,
    scans_omitted: u64,
    slowest_entries: Vec<Entry>,
    slow_entries_omitted: u64,
    last_entry: Option<Entry>,
    #[serde(skip)]
    current: Option<Entry>,
}
pub(crate) struct Session;
impl Session {
    pub(crate) fn enable() -> Self {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            assert!(s.is_none(), "诊断会话不可嵌套");
            *s = Some(Report::default());
        });
        Self
    }
    pub(crate) fn finish(self) -> Report {
        STATE.with(|s| s.borrow_mut().take().unwrap())
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        STATE.with(|s| {
            s.borrow_mut().take();
        });
    }
}
fn started() -> Option<Instant> {
    STATE.with(|s| s.borrow().as_ref().map(|_| Instant::now()))
}
pub(crate) fn kind(input: &RecordedIngressV1) -> &'static str {
    match &input.ingress {
        DptIngressV1::EndOfInput => "end_of_input",
        DptIngressV1::Aborted { .. } => "aborted",
        DptIngressV1::Message { row, .. } => match row {
            TypedRawRowV2::Existing(row) => match row {
                TypedRawRow::PolymarketOrderbook(_) => "polymarket_orderbook",
                TypedRawRow::PolymarketMarketMetadata(_) => "metadata_existing",
                TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_) => "chainlink_spot",
                TypedRawRow::BinanceDepthSnapshot(_) => "binance_depth_snapshot",
                TypedRawRow::BinanceDepthUpdate(_) => "binance_depth_update",
                TypedRawRow::BinanceAggTrade(_) => "binance_agg_trade",
            },
            TypedRawRowV2::PolymarketMarketMetadataV6(row) => match row.as_ref() {
                PolymarketMarketMetadataV6Input::Existing(_) => "metadata_v6_existing",
                PolymarketMarketMetadataV6Input::AnchorRule(_) => "anchor_rule",
            },
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(_) => "chainlink_twap",
            TypedRawRowV2::HistoricalPriceAnchorRepairBatch(_) => "historical_anchor_repair",
        },
    }
}
fn row_kind(row: &PublicStoredRowV1) -> &'static str {
    match row {
        PublicStoredRowV1::Input(_) => "input",
        PublicStoredRowV1::Canonical { .. } => "canonical",
        PublicStoredRowV1::Report { .. } => "report",
        PublicStoredRowV1::PmReport { .. } => "pm_report",
        PublicStoredRowV1::PmStepBinding(_) => "pm_step_binding",
        PublicStoredRowV1::Group(_) => "group",
        PublicStoredRowV1::Observation(_) => "observation",
        PublicStoredRowV1::SourceBinding(_) => "source_binding",
    }
}
pub(crate) struct EntryGuard(Option<Instant>);
impl EntryGuard {
    pub(crate) fn new(expected: u64) -> Self {
        let start = started();
        if start.is_some() {
            STATE.with(|s| {
                s.borrow_mut().as_mut().unwrap().current = Some(Entry {
                    context: Context {
                        expected_input_position: expected,
                        kind: "unresolved".into(),
                        ..Context::default()
                    },
                    outcome: "error".into(),
                    ..Entry::default()
                })
            });
        }
        Self(start)
    }
    pub(crate) fn bind(&self, input: &RecordedIngressV1, target: Option<&MarketIdentityV2>) {
        if self.0.is_none() {
            return;
        }
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let entry = s.as_mut().unwrap().current.as_mut().unwrap();
            entry.context.input_position = Some(input.input_position);
            entry.context.kind = kind(input).into();
            entry.context.target_condition = target.map(|m| m.condition_id.0.clone());
            entry.context.dependency_refs = input.dependency_record_refs.clone();
            if let DptIngressV1::Message {
                row,
                source_evidence,
            } = &input.ingress
            {
                entry.context.source_message_id = Some(source_evidence.source_message_id.0.clone());
                if let TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch) = row {
                    entry.context.historical_member_conditions =
                        batch.rows.iter().map(|r| r.condition_id.clone()).collect();
                }
            }
        });
    }
    pub(crate) fn returned(&self, eof: bool) {
        if self.0.is_some() {
            STATE.with(|s| {
                s.borrow_mut()
                    .as_mut()
                    .unwrap()
                    .current
                    .as_mut()
                    .unwrap()
                    .outcome = if eof { "eof" } else { "input" }.into()
            });
        }
    }
}
impl Drop for EntryGuard {
    fn drop(&mut self) {
        let Some(start) = self.0 else {
            return;
        };
        let ns = start.elapsed().as_nanos();
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            let mut e = r.current.take().unwrap();
            e.total_ns = ns;
            e.residual_ns = ns as i128 - e.primary_ns.values().sum::<u128>() as i128;
            r.entry_calls += 1;
            r.entry_total_ns += ns;
            r.residual_total_ns += e.residual_ns;
            r.negative_residual_entries += u64::from(e.residual_ns < 0);
            match e.outcome.as_str() {
                "input" => r.returned_inputs += 1,
                "eof" => r.eof_returns += 1,
                _ => r.failed_entries += 1,
            };
            let k = if e.outcome == "eof" {
                "eof".into()
            } else {
                e.context.kind.clone()
            };
            r.by_return_kind
                .entry(k)
                .or_default()
                .add(ns, e.outcome != "error");
            r.last_entry = Some(e);
            let e = r.last_entry.as_ref().unwrap();
            if r.slowest_entries.len() < RETAIN_SLOW
                || ns > r.slowest_entries.last().unwrap().total_ns
            {
                // 只克隆至多16个小型定位项，不复制业务载荷。
                let v = Entry {
                    context: e.context.clone(),
                    outcome: e.outcome.clone(),
                    total_ns: e.total_ns,
                    primary_ns: e.primary_ns.clone(),
                    nested_ns: e.nested_ns.clone(),
                    residual_ns: e.residual_ns,
                    failed_spans: e.failed_spans.clone(),
                };
                r.slowest_entries.push(v);
                r.slowest_entries
                    .sort_by_key(|e| std::cmp::Reverse(e.total_ns));
                r.slowest_entries.truncate(RETAIN_SLOW);
            }
            r.slow_entries_omitted = r.entry_calls.saturating_sub(r.slowest_entries.len() as u64);
        });
    }
}
pub(crate) struct Span {
    start: Option<Instant>,
    kind: &'static str,
    completed: bool,
    rows: BTreeMap<&'static str, u64>,
}
impl Span {
    pub(crate) fn new(kind: &'static str) -> Self {
        Self {
            start: started(),
            kind,
            completed: false,
            rows: BTreeMap::new(),
        }
    }
    pub(crate) fn row(&mut self, row: &PublicStoredRowV1) {
        if self.start.is_some() {
            *self.rows.entry(row_kind(row)).or_default() += 1;
        }
    }
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        let ns = start.elapsed().as_nanos();
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            let t = r.primary.entry(self.kind).or_default();
            t.add(ns, self.completed);
            for (k, n) in &self.rows {
                *t.rows.entry(k).or_default() += n;
            }
            if let Some(e) = r.current.as_mut() {
                *e.primary_ns.entry(self.kind).or_default() += ns;
                if !self.completed {
                    e.failed_spans.push(self.kind);
                }
            }
        });
    }
}
pub(crate) struct ScanGuard {
    start: Option<Instant>,
    data: Option<Scan>,
}
impl ScanGuard {
    pub(crate) fn new(
        kind: &'static str,
        cursor: Option<u64>,
        cutoff: Option<u64>,
        opens: bool,
    ) -> Self {
        let start = started();
        let data = start.map(|_| {
            STATE.with(|s| Scan {
                kind,
                context: s
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .current
                    .as_ref()
                    .map(|e| e.context.clone())
                    .unwrap_or_default(),
                cursor_before: cursor,
                cutoff,
                open_attempted: opens,
                ..Scan::default()
            })
        });
        Self { start, data }
    }
    pub(crate) fn opened(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.open_succeeded = true;
        }
    }
    pub(crate) fn row(&mut self, row: &PublicStoredRowV1) {
        if let Some(d) = self.data.as_mut() {
            *d.rows.entry(row_kind(row)).or_default() += 1;
            if let PublicStoredRowV1::Canonical {
                binding,
                record: CanonicalEventRecordV2::ReferenceEvent(event),
                ..
            } = row
                && matches!(
                    event.payload,
                    CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_)
                )
            {
                d.twap_encountered += 1;
                d.first_twap_input
                    .get_or_insert(binding.position.closed_at_input_position);
                d.last_twap_input = Some(binding.position.closed_at_input_position);
            }
        }
    }
    pub(crate) fn matched(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.matched_records += 1;
        }
    }
    pub(crate) fn complete(
        &mut self,
        returned: usize,
        cursor: Option<u64>,
        exhausted: Option<bool>,
    ) {
        if let Some(d) = self.data.as_mut() {
            d.completed = true;
            d.returned = Some(returned);
            d.cursor_after = cursor;
            d.exhausted = exhausted;
        }
    }
}
impl Drop for ScanGuard {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        let ns = start.elapsed().as_nanos();
        let mut d = self.data.take().unwrap();
        d.ns = ns;
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            r.scan_totals.entry(d.kind).or_default().add(&d);
            if let Some(position) = d.context.input_position {
                r.scan_by_input
                    .entry(position)
                    .or_insert_with(|| PositionScans {
                        context: d.context.clone(),
                        kinds: BTreeMap::new(),
                    })
                    .kinds
                    .entry(d.kind)
                    .or_default()
                    .add(&d);
            } else {
                r.scans_without_input_context += 1;
            }
            if matches!(d.kind, "candidate_open" | "candidate_page") {
                r.nested.entry(d.kind).or_default().add(ns, d.completed);
                if let Some(e) = r.current.as_mut() {
                    *e.nested_ns.entry(d.kind).or_default() += ns;
                    if !d.completed {
                        e.failed_spans.push(d.kind);
                    }
                }
            }
            if r.scans.len() < RETAIN_SCANS {
                r.scans.push(d);
            } else {
                r.scans_omitted += 1;
            }
        });
    }
}
pub(crate) fn report_value(report: Report) -> serde_json::Value {
    let prepare_ns = report.primary.get("prepare").map_or(0, |t| t.ns);
    let candidate_ns = report.nested.values().map(|t| t.ns).sum::<u128>();
    let mut value = serde_json::to_value(report).unwrap();
    value["prepare_remaining_ns"] = serde_json::json!(prepare_ns as i128 - candidate_ns as i128);
    value["retention"] = serde_json::json!({"scan_descriptors":"first 128 actual scan/open/page invocations; total by kind and omitted count retained","scan_by_input":"all actually scanning Input positions with per-kind counts/rows/time; no per-row payload","slow_entries":"16 slowest whole entry calls; total and omitted count retained"});
    value["time_accounting"] = serde_json::json!({"primary":PRIMARY,"residual":"signed entry total minus non-overlapping primary spans; no zero clamp","nested":"candidate_open/candidate_page are contained in prepare and must not be added to primary; scan elapsed is a subset, not extra primary time","overhead":"clock checks and local counting are inside this test diagnosis; bookkeeping at scope destruction falls within enclosing span or residual; overhead not independently isolated","twap_encountered":"TWAP canonical rows actually decoded before cursor/cutoff filtering; may include the first row beyond cutoff; returned counts are successful page output only"});
    value
}
pub(crate) fn reader_prefix(replay: &PublicReplayV1) -> Option<CommittedIngressPrefixV1> {
    replay.reader.prefix.clone()
}
