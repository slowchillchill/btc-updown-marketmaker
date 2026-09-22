//! 原接纳序重放；历史只在各来源已经证明的前序首项之间选择。
use crate::public_recording::*;
use crate::{ExternalSorter, RunCursor, SortConfig, SortRow};
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::TypedRawRow,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HistoricalSourceV1 {
    Metadata,
    Polymarket,
    BinanceDepth,
    AggregateTrade,
    ChainlinkSpot,
    Twap,
}
impl HistoricalSourceV1 {
    fn index(self) -> usize {
        self as usize
    }
}
/// 来源读取器给出的已证明代际排名与本代接纳序；不是物理路径或文件行号。
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HistoricalIngressV1 {
    pub source: HistoricalSourceV1,
    pub source_order_key: Vec<u8>,
    pub ingress: DptIngressV1,
}
/// 分来源外排后归并只比较六个首项；深度快照/更新必须使用同一个 BinanceDepth 来源。
pub struct HistoricalMergeV1 {
    cursors: Vec<RunCursor>,
    heads: Vec<Option<HistoricalIngressV1>>,
    stream: String,
    position: u64,
    sort_metrics: Vec<crate::SortMetrics>,
    twap_path: PathBuf,
    // 与 TWAP 外排原序一一对应；不保存载荷或业务选择结果。
    twap_positions: Vec<u64>,
    page_rows: usize,
    page_bytes: usize,
}
impl HistoricalMergeV1 {
    fn prepare(
        root: &Path,
        config: &SortConfig,
        stream: String,
        input: impl IntoIterator<Item = RecordingResult<HistoricalIngressV1>>,
    ) -> RecordingResult<Self> {
        fs::create_dir(root)?;
        let mut sorters = Vec::new();
        for i in 0..6 {
            let path = root.join(i.to_string());
            fs::create_dir(&path)?;
            sorters.push(ExternalSorter::new(&path, config)?);
        }
        for entry in input {
            let entry = entry?;
            let key = entry.source_order_key.clone();
            sorters[entry.source.index()]
                .push(SortRow::from_sort_key(key, serde_json::to_vec(&entry)?))?;
        }
        let mut cursors = Vec::new();
        let mut heads = Vec::new();
        let mut sort_metrics = Vec::new();
        let mut twap_path = PathBuf::new();
        for (index, sorter) in sorters.into_iter().enumerate() {
            let output = sorter.finish()?;
            if index == HistoricalSourceV1::Twap.index() {
                twap_path = output.output_path.clone();
            }
            sort_metrics.push(output.metrics.clone());
            let mut cursor =
                RunCursor::open(&output.output_path, config.batch_rows, config.max_run_bytes)?;
            heads.push(
                cursor
                    .next_row()?
                    .map(|r| serde_json::from_slice(r.payload()))
                    .transpose()?,
            );
            cursors.push(cursor);
        }
        Ok(Self {
            cursors,
            heads,
            stream,
            position: 0,
            sort_metrics,
            twap_path,
            twap_positions: Vec::new(),
            page_rows: config.batch_rows,
            page_bytes: config.max_run_bytes,
        })
    }
    pub fn sort_metrics(&self) -> &[crate::SortMetrics] {
        &self.sort_metrics
    }
    pub fn next_input(&mut self) -> RecordingResult<Option<RecordedIngressV1>> {
        let chosen = self
            .heads
            .iter()
            .enumerate()
            .filter_map(|(i, h)| h.as_ref().map(|h| (i, h)))
            .min_by_key(|(_, h)| match &h.ingress {
                DptIngressV1::Message {
                    source_evidence: s, ..
                } => (
                    s.received_at_utc_ms,
                    h.source,
                    s.source_message_id.0.as_str(),
                ),
                _ => unreachable!("历史来源读取器仅提供实际来源消息"),
            })
            .map(|(i, _)| i);
        let Some(i) = chosen else { return Ok(None) };
        let entry = self.heads[i].take().unwrap();
        self.heads[i] = self.cursors[i]
            .next_row()?
            .map(|r| serde_json::from_slice(r.payload()))
            .transpose()?;
        if i == HistoricalSourceV1::Twap.index() {
            self.twap_positions.push(self.position);
        }
        let result = RecordedIngressV1 {
            input_stream_id: self.stream.clone(),
            input_position: self.position,
            ingress: entry.ingress,
            dependency_record_refs: vec![],
        };
        self.position += 1;
        Ok(Some(result))
    }
    /// 有限 core/record 共用准备入口；只按需重读 TWAP 外排页，索引仅保存已接纳原坐标。
    pub fn prepare_input<'a>(
        &self,
        core: &PublicObservationCoreV1,
        input: &'a mut RecordedIngressV1,
    ) -> RecordingResult<PreparedIngressV1<'a>> {
        let Some(market) = reference_market_for_input(core, input) else {
            return Ok(prepare_public_ingress_v1(
                core,
                input,
                &BTreeMap::new(),
                &[],
            )?);
        };
        // 选择器先检查规则/正式锚价；不需要候选时不打开外排文件。
        let mut reader = None;
        let mut ordinal = 0;
        Ok(prepare_public_ingress_with_reference_v1(
            core,
            input,
            &BTreeMap::new(),
            &[],
            &market,
            |cursor| {
                let page = (|| -> RecordingResult<_> {
                    if reader.is_none() {
                        reader = Some(RunCursor::open(
                            &self.twap_path,
                            self.page_rows,
                            self.page_bytes,
                        )?);
                    }
                    let reader = reader.as_mut().unwrap();
                    let mut values = Vec::new();
                    let mut next = cursor;
                    while ordinal < self.twap_positions.len() && values.len() < self.page_rows {
                        let position = self.twap_positions[ordinal];
                        ordinal += 1;
                        let row = reader.next_row()?.ok_or("TWAP 外排记录缺失")?;
                        if position < cursor.next_input_position {
                            continue;
                        }
                        let entry: HistoricalIngressV1 = serde_json::from_slice(row.payload())?;
                        let DptIngressV1::Message {
                            row: TypedRawRowV2::PolymarketRtdsChainlinkTwap(row),
                            source_evidence,
                        } = entry.ingress
                        else {
                            return Err("TWAP 外排记录类型不符".into());
                        };
                        let event = canonicalize_chainlink_twap_v1(
                            &row,
                            &source_evidence,
                            &core.header().canonical_rule_version,
                        )?;
                        let source = &event.lineage.source_evidence[0];
                        let pos = PublicFactPositionV1 {
                            input_stream_id: self.stream.clone(),
                            closed_at_input_position: position,
                            fact_ordinal: 0,
                        };
                        values.push(TemporaryReferenceCandidateV1 {
                            record_ref: CanonicalInputRecordRefV1 {
                                input_stream_id: self.stream.clone(),
                                input_position: position,
                            },
                            anchor: DirectObservationAnchorV1 {
                                field_path: if row.window_s == 30 {
                                    "chainlink_twap_thirty"
                                } else {
                                    "chainlink_twap_sixty"
                                }
                                .into(),
                                position: pos,
                                group_id: source.source_message_id.clone(),
                                source_message_id: source.source_message_id.clone(),
                                source_event_at_utc_ms: source.source_event_at_utc_ms,
                                received_at_utc_ms: source.received_at_utc_ms,
                            },
                            canonical_fact: event,
                        });
                        next.next_input_position = position + 1;
                    }
                    Ok((values, next, ordinal == self.twap_positions.len()))
                })();
                page.map_err(|error| data_pipeline_core::ContractError::InvalidField {
                    field: "reference_page".into(),
                    reason: error.to_string(),
                })
            },
        )?)
    }
}

/// 每次调用沿唯一稳定记录读取；不把页末当正常业务 EOF。
pub struct PublicReplayV1 {
    reader: PublicRecordingReaderV1,
    pub core: PublicObservationCoreV1,
    page_rows: usize,
    page_bytes: usize,
}
impl PublicReplayV1 {
    pub fn open(root: &Path, page_rows: usize, page_bytes: usize) -> RecordingResult<Self> {
        let reader = PublicRecordingReaderV1::recover(root, page_rows, page_bytes)?;
        let core = PublicObservationCoreV1::new(reader.header.clone())?;
        Ok(Self {
            reader,
            core,
            page_rows,
            page_bytes,
        })
    }
    pub fn next_applied(
        &mut self,
    ) -> RecordingResult<Option<(RecordedIngressV1, PublicAppliedIngressV1<'static>)>> {
        Ok(self
            .next_prepared_applied()?
            .map(|(input, applied, _)| (input, applied)))
    }
    /// 移交本次来源准备对象；模拟器消费时不重新编码身份或准备盘口摘要。
    pub fn next_prepared_applied(
        &mut self,
    ) -> RecordingResult<
        Option<(
            RecordedIngressV1,
            PublicAppliedIngressV1<'static>,
            Option<data_pipeline_core::pm_reduction::PreparedPmMessageV1>,
        )>,
    > {
        while let Some(row) = self.reader.next_row()? {
            if let PublicStoredRowV1::Input(mut input) = row {
                let root = self.reader.root().to_path_buf();
                let cutoff = input.input_position;
                let target = reference_market_for_input(&self.core, &input);
                let identity_sources = read_historical_identity_sources(
                    &root,
                    &input,
                    self.page_rows,
                    self.page_bytes,
                )?;
                let deps = read_dependencies(
                    &root,
                    &input.dependency_record_refs,
                    self.page_rows,
                    self.page_bytes,
                )?;
                let result = if let Some(market) = target {
                    let mut page_reader = None;
                    let prepared = prepare_public_ingress_with_reference_v1(
                        &self.core,
                        &mut input,
                        &identity_sources,
                        &deps,
                        &market,
                        |cursor| {
                            (|| -> RecordingResult<_> {
                                if page_reader.is_none() {
                                    page_reader = Some(ReferenceCandidateReaderV1::open(
                                        &root,
                                        self.page_rows,
                                        self.page_bytes,
                                        cutoff,
                                    )?);
                                }
                                page_reader.as_mut().unwrap().page(cursor)
                            })()
                            .map_err(|e| {
                                data_pipeline_core::ContractError::InvalidField {
                                    field: "reference_page".into(),
                                    reason: e.to_string(),
                                }
                            })
                        },
                    )?;
                    let applied = self.core.push(&prepared)?.into_owned();
                    let pm = match prepared.current {
                        PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
                        _ => None,
                    };
                    (applied, pm)
                } else {
                    let prepared =
                        prepare_public_ingress_v1(&self.core, &input, &identity_sources, &deps)?;
                    let applied = self.core.push(&prepared)?.into_owned();
                    let pm = match prepared.current {
                        PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
                        _ => None,
                    };
                    (applied, pm)
                };
                return Ok(Some((input, result.0, result.1)));
            }
        }
        Ok(None)
    }
    pub fn into_core(self) -> PublicObservationCoreV1 {
        self.core
    }
}
/// 原修复成员与唯一规范事实同序；恢复原证据键，不以市场条件替代身份键域。
fn read_historical_identity_sources(
    root: &Path,
    input: &RecordedIngressV1,
    rows: usize,
    bytes: usize,
) -> RecordingResult<BTreeMap<String, SourceEvidenceRefV3>> {
    let mut sources = BTreeMap::new();
    let DptIngressV1::Message {
        row: TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch),
        ..
    } = &input.ingress
    else {
        return Ok(sources);
    };
    let mut members = batch.rows.iter();
    let mut reader = PublicRecordingReaderV1::recover(root, rows, bytes)?;
    while let Some(row) = reader.next_row()? {
        if let PublicStoredRowV1::Canonical {
            binding,
            record: CanonicalEventRecordV2::ReferenceEvent(event),
            ..
        } = row
        {
            if binding.position.input_stream_id != input.input_stream_id
                || binding.position.closed_at_input_position != input.input_position
            {
                continue;
            }
            if let CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(value) = event.payload
            {
                if let PriceAnchorApplicationV1::HistoricalRepair {
                    historical_identity_evidence,
                    ..
                } = value.anchor.application
                {
                    if let Some(member) = members.next() {
                        let ids: Vec<String> =
                            serde_json::from_str(&member.historical_identity_evidence_ids_json)?;
                        sources.extend(ids.into_iter().zip(historical_identity_evidence));
                    }
                }
            }
        }
    }
    Ok(sources)
}
fn metadata_reference_market(
    core: &PublicObservationCoreV1,
    row: &data_pipeline_core::typed_raw::PolymarketMarketMetadataRow,
) -> Option<MarketIdentityV2> {
    core.pm_context(&row.condition_id)
        .map(|c| c.market)
        .or_else(|| {
            (row.metadata_row_kind
                == data_pipeline_core::typed_raw::PolymarketMetadataRowKind::MetadataSnapshot)
                .then(|| MarketIdentityV2 {
                    condition_id: ConditionId(row.condition_id.clone()),
                    clob_market_id: ClobMarketId(row.clob_market_id.clone()),
                    window: MarketWindowV2 {
                        start_at_utc_ms: row.market_start_utc,
                        end_at_utc_ms: row.market_end_utc,
                    },
                })
        })
}
/// 确定目标关联；候选适用性和首条判断仍仅归纯选择函数。
pub fn reference_market_for_input(
    core: &PublicObservationCoreV1,
    input: &RecordedIngressV1,
) -> Option<MarketIdentityV2> {
    let DptIngressV1::Message { row, .. } = &input.ingress else {
        return None;
    };
    match row {
        TypedRawRowV2::PolymarketMarketMetadataV6(r) => match r.as_ref() {
            PolymarketMarketMetadataV6Input::AnchorRule(r) => {
                core.pm_context(&r.condition_id).map(|c| c.market)
            }
            PolymarketMarketMetadataV6Input::Existing(r) => metadata_reference_market(core, r),
        },
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(r)) => {
            metadata_reference_market(core, r)
        }
        TypedRawRowV2::PolymarketRtdsChainlinkTwap(r) => core.fixed_markets().find(|m| {
            r.common.payload_timestamp >= m.window.start_at_utc_ms
                && r.common.payload_timestamp < m.window.end_at_utc_ms
        }),
        _ => None,
    }
}
pub fn read_dependencies(
    root: &Path,
    refs: &[CanonicalInputRecordRefV1],
    rows: usize,
    bytes: usize,
) -> RecordingResult<Vec<CanonicalReferenceEventV2>> {
    if refs.is_empty() {
        return Ok(vec![]);
    }
    let mut reader = PublicRecordingReaderV1::recover(root, rows, bytes)?;
    let mut output = Vec::new();
    while let Some(row) = reader.next_row()? {
        if let PublicStoredRowV1::Canonical {
            binding,
            record: CanonicalEventRecordV2::ReferenceEvent(event),
            ..
        } = row
        {
            if refs.iter().any(|r| {
                r.input_position == binding.position.closed_at_input_position
                    && r.input_stream_id == binding.position.input_stream_id
            }) {
                output.push(event)
            }
        }
    }
    Ok(output)
}
/// 顺序页只供料，不预选首条或缓存全部历史 TWAP。
pub struct ReferenceCandidateReaderV1 {
    reader: PublicRecordingReaderV1,
    limit: usize,
    cutoff: u64,
}
impl ReferenceCandidateReaderV1 {
    pub fn open(root: &Path, rows: usize, bytes: usize, cutoff: u64) -> RecordingResult<Self> {
        Ok(Self {
            reader: PublicRecordingReaderV1::recover(root, rows, bytes)?,
            limit: rows,
            cutoff,
        })
    }
    /// 在线来源准备绑定写入方实际交付的稳定凭据，不扫描活跃尾部。
    pub fn open_committed(
        root: &Path,
        prefix: Option<CommittedIngressPrefixV1>,
        rows: usize,
        bytes: usize,
        cutoff: u64,
    ) -> RecordingResult<Self> {
        Ok(Self {
            reader: PublicRecordingReaderV1::open_committed(root, prefix, rows, bytes)?,
            limit: rows,
            cutoff,
        })
    }
    pub fn page(
        &mut self,
        cursor: TemporaryReferenceCursorV1,
    ) -> RecordingResult<(
        Vec<TemporaryReferenceCandidateV1>,
        TemporaryReferenceCursorV1,
        bool,
    )> {
        let mut values = Vec::new();
        let mut next = cursor;
        while values.len() < self.limit {
            let Some(row) = self.reader.next_row()? else {
                return Ok((values, next, true));
            };
            if let PublicStoredRowV1::Canonical {
                binding,
                record: CanonicalEventRecordV2::ReferenceEvent(event),
                ..
            } = row
            {
                let pos = binding.position;
                if pos.closed_at_input_position > self.cutoff {
                    return Ok((values, next, true));
                }
                if pos.closed_at_input_position < cursor.next_input_position {
                    continue;
                }
                if !matches!(
                    event.payload,
                    CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_)
                ) {
                    continue;
                }
                next.next_input_position = pos.closed_at_input_position + 1;
                values.push(reference_candidate(pos, event));
            }
        }
        Ok((values, next, false))
    }
}

/// 可重复打开的强类型来源入口；预扫描只保存原采集代际起点，不缓存历史载荷。
pub fn prepare_historical_rows_v1<I>(
    root: &Path,
    config: &crate::canonical_run::CanonicalRunConfig,
    stream: String,
    mut read: impl FnMut() -> RecordingResult<I>,
) -> RecordingResult<HistoricalMergeV1>
where
    I: Iterator<Item = RecordingResult<TypedRawRowV2>>,
{
    use crate::canonical_run::{CaptureCoordinate, rank_epochs, update_epoch_start};
    let mut pm = BTreeMap::new();
    let mut depth = BTreeMap::new();
    for row in read()? {
        let row = row?;
        use data_pipeline_core::typed_raw::TypedRawRow;
        let (starts, kind, epoch, coordinate) = match &row {
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) => (
                &mut pm,
                "polymarket_market",
                &row.source_sequence_epoch_id,
                CaptureCoordinate {
                    admitted_at_utc_ms: row.collector_admission_time_utc_ms,
                    received_at_utc_ms: row.timestamp_received,
                    collector_admission_sequence: row.collector_admission_sequence,
                },
            ),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(row)) => (
                &mut depth,
                "binance_depth",
                &row.source_sequence_epoch_id,
                CaptureCoordinate {
                    admitted_at_utc_ms: row.collector_admission_time_utc_ms,
                    received_at_utc_ms: row.timestamp_received,
                    collector_admission_sequence: row.collector_admission_sequence,
                },
            ),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(row)) => (
                &mut depth,
                "binance_depth",
                &row.source_sequence_epoch_id,
                CaptureCoordinate {
                    admitted_at_utc_ms: row.collector_admission_time_utc_ms,
                    received_at_utc_ms: row.timestamp_received,
                    collector_admission_sequence: row.collector_admission_sequence,
                },
            ),
            _ => continue,
        };
        update_epoch_start(
            starts,
            epoch,
            coordinate,
            kind,
            config.max_epoch_count_per_source,
        )?;
    }
    let pm = rank_epochs(pm, "polymarket_market")?;
    let depth = rank_epochs(depth, "binance_depth")?;
    let entries = read()?.map(|row| -> RecordingResult<HistoricalIngressV1> {
        let row = row?;
        let evidence = source_evidence_v3_from_typed_raw_v2(&row)?;
        let source = match evidence.source_kind {
            CanonicalSourceKindV2::PolymarketMarketMetadata => HistoricalSourceV1::Metadata,
            CanonicalSourceKindV2::PolymarketOrderbook => HistoricalSourceV1::Polymarket,
            CanonicalSourceKindV2::BinanceDepthSnapshot
            | CanonicalSourceKindV2::BinanceDepthUpdate => HistoricalSourceV1::BinanceDepth,
            CanonicalSourceKindV2::BinanceAggtrade => HistoricalSourceV1::AggregateTrade,
            CanonicalSourceKindV2::PolymarketRtdsChainlinkBtcUsd => {
                HistoricalSourceV1::ChainlinkSpot
            }
            CanonicalSourceKindV2::PolymarketRtdsChainlinkTwap => HistoricalSourceV1::Twap,
        };
        let mut key = Vec::new();
        if matches!(
            source,
            HistoricalSourceV1::Polymarket | HistoricalSourceV1::BinanceDepth
        ) {
            let c = evidence.collector_admission.as_ref().unwrap();
            let ranks = if source == HistoricalSourceV1::Polymarket {
                &pm
            } else {
                &depth
            };
            key.extend(ranks[&c.source_sequence_epoch_id].to_be_bytes());
            key.extend(c.collector_admission_sequence.to_be_bytes());
        } else if let Some(c) = &evidence.collector_admission {
            key.extend(((c.admitted_at_utc_ms as u64) ^ (1 << 63)).to_be_bytes());
            key.extend(c.collector_admission_sequence.to_be_bytes());
        } else {
            // 无统一接纳坐标的已记录RTDS，用其原接收事实及既有语义身份确定有限约定。
            key.extend(((evidence.received_at_utc_ms as u64) ^ (1 << 63)).to_be_bytes());
        }
        key.extend(evidence.source_message_id.0.as_bytes());
        Ok(HistoricalIngressV1 {
            source,
            source_order_key: key,
            ingress: DptIngressV1::Message {
                row,
                source_evidence: evidence,
            },
        })
    });
    HistoricalMergeV1::prepare(root, &config.sort, stream, entries)
}
/// 文件仅作为供料位置；其顺序/路径不进入新历史排序键。
pub fn prepare_historical_files_v1(
    root: &Path,
    files: &[&crate::ValidatedDirectSourceParquetFileV3],
    config: &crate::canonical_run::CanonicalRunConfig,
    stream: String,
) -> RecordingResult<HistoricalMergeV1> {
    prepare_historical_rows_v1(root, config, stream, || {
        Ok(TypedFileRowsV1 {
            files: files.iter(),
            stream: None,
            pending: Vec::new().into_iter(),
            rows: config.decode_batch_rows,
            bytes: config.max_decode_batch_bytes,
        })
    })
}
struct TypedFileRowsV1<'a> {
    files: std::slice::Iter<'a, &'a crate::ValidatedDirectSourceParquetFileV3>,
    stream: Option<crate::typed_raw::TypedRawParquetStream<TypedRawRowV2>>,
    pending: std::vec::IntoIter<TypedRawRowV2>,
    rows: usize,
    bytes: usize,
}
impl Iterator for TypedFileRowsV1<'_> {
    type Item = RecordingResult<TypedRawRowV2>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.next() {
                return Some(Ok(row));
            }
            if let Some(stream) = &mut self.stream {
                match stream.next() {
                    Some(Ok(batch)) => {
                        self.pending = batch.into_iter();
                        continue;
                    }
                    Some(Err(e)) => return Some(Err(e.into())),
                    None => {}
                }
            }
            let file = self.files.next()?;
            match crate::public_input::open_public_input_bounded_v1(file, self.rows, self.bytes) {
                Ok(stream) => self.stream = Some(stream),
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

/// 同一规范参考事实的页表达；只包装原位置和证据，不判断适用性。
pub(crate) fn reference_candidate(
    pos: PublicFactPositionV1,
    event: CanonicalReferenceEventV2,
) -> TemporaryReferenceCandidateV1 {
    let source = &event.lineage.source_evidence[0];
    TemporaryReferenceCandidateV1 {
        record_ref: CanonicalInputRecordRefV1 {
            input_stream_id: pos.input_stream_id.clone(),
            input_position: pos.closed_at_input_position,
        },
        anchor: DirectObservationAnchorV1 {
            field_path: match &event.payload {
                CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(v) if v.window_s == 30 => {
                    "chainlink_twap_thirty"
                }
                _ => "chainlink_twap_sixty",
            }
            .into(),
            position: pos,
            group_id: source.source_message_id.clone(),
            source_message_id: source.source_message_id.clone(),
            source_event_at_utc_ms: source.source_event_at_utc_ms,
            received_at_utc_ms: source.received_at_utc_ms,
        },
        canonical_fact: event,
    }
}
