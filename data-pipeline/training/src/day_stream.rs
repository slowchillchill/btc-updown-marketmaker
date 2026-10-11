//! 日作业的原始文件游标。公共来源顺序由原准入坐标确定，文件路径仅定位输入。
use crate::{
    ValidatedDirectSourceParquetFileV3, public_input::*, public_recording::RecordingResult,
    typed_raw::TypedRawCursorV1,
};
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::*, public_reference::*};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

#[derive(Clone, Serialize, Deserialize)]
struct FileCursorV1 {
    file: usize,
    cursor: TypedRawCursorV1,
    order_position: usize,
    pending: BTreeMap<u64, TypedRawRowV2>,
}
struct FileStreamV1<'a> {
    file: &'a ValidatedDirectSourceParquetFileV3,
    saved: FileCursorV1,
    stream: Option<crate::typed_raw::TypedRawParquetStream<TypedRawRowV2>>,
}
impl FileStreamV1<'_> {
    fn head(
        &mut self,
        physical: u64,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<&TypedRawRowV2> {
        if !self.saved.pending.contains_key(&physical) {
            self.saved.pending.clear();
            if self.stream.is_none() {
                let mut cursor = self.saved.cursor.clone();
                cursor.physical_row = physical;
                self.stream = Some(open_validated_public_input_at_v1(
                    self.file, rows, bytes, &cursor,
                )?);
            }
            let stream = self.stream.as_mut().unwrap();
            if stream.cursor().physical_row != physical {
                stream.seek_physical(physical)?;
            }
            let batch = stream.next().transpose()?.ok_or("顺序索引指向原文件EOF")?;
            self.saved.pending.extend(
                batch
                    .into_iter()
                    .enumerate()
                    .map(|(i, row)| (physical + i as u64, row)),
            );
            self.saved.cursor = stream.cursor();
        }
        self.saved
            .pending
            .get(&physical)
            .ok_or_else(|| "顺序索引的原行缺失".into())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DayIngressCheckpointV1 {
    files: Vec<FileCursorV1>,
    order: Arc<Vec<Vec<crate::day_order::LocatedInputV1>>>,
    order_reference: Option<DayOrderReferenceV1>,
    epochs: BTreeMap<(u8, String), u64>,
    stream: String,
    position: u64,
    ended: bool,
    initial: VecDeque<(TypedRawRowV2, Option<SourceArtifactBindingV1>)>,
    twap: Vec<(u64, PolymarketRtdsChainlinkTwapInput, SourceEvidenceRefV3)>,
}
#[derive(Clone, Serialize, Deserialize)]
struct DayOrderReferenceV1 {
    path: std::path::PathBuf,
    sha256: String,
}
fn index_hash(path: &std::path::Path) -> RecordingResult<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = vec![0u8; 1 << 20];
    loop {
        let count = file.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
/// 六个逻辑来源的首项归并；快照和更新共用 BinanceDepth 来源。
pub struct DayIngressV1<'a> {
    files: Vec<FileStreamV1<'a>>,
    saved: DayIngressCheckpointV1,
    rows: usize,
    bytes: usize,
    last_initialization_binding: Option<SourceArtifactBindingV1>,
    source_minima: Vec<Vec<Option<i64>>>,
    pm_minima: Vec<Vec<Option<(u64, i64, u64, u64)>>>,
    heads: BTreeMap<(u8, u64, u64, String, usize), i64>,
}
pub(crate) fn source(evidence: &SourceEvidenceRefV3) -> usize {
    match evidence.source_kind {
        CanonicalSourceKindV2::PolymarketMarketMetadata => 0,
        CanonicalSourceKindV2::PolymarketOrderbook => 1,
        CanonicalSourceKindV2::BinanceDepthSnapshot | CanonicalSourceKindV2::BinanceDepthUpdate => {
            2
        }
        CanonicalSourceKindV2::BinanceAggtrade => 3,
        CanonicalSourceKindV2::PolymarketRtdsChainlinkBtcUsd => 4,
        CanonicalSourceKindV2::PolymarketRtdsChainlinkTwap => 5,
    }
}
fn admission(row: &TypedRawRowV2, e: &SourceEvidenceRefV3) -> (i64, u64) {
    if let Some(c) = &e.collector_admission {
        return (c.admitted_at_utc_ms, c.collector_admission_sequence);
    }
    match row {
        TypedRawRowV2::Existing(
            data_pipeline_core::typed_raw::TypedRawRow::PolymarketMarketMetadata(r),
        ) => (
            r.collector_admission_time_utc_ms,
            r.collector_admission_sequence,
        ),
        TypedRawRowV2::PolymarketMarketMetadataV6(r) => match r.as_ref() {
            PolymarketMarketMetadataV6Input::Existing(r) => (
                r.collector_admission_time_utc_ms,
                r.collector_admission_sequence,
            ),
            PolymarketMarketMetadataV6Input::AnchorRule(r) => (
                r.collector_admission_time_utc_ms,
                r.collector_admission_sequence,
            ),
        },
        _ => (e.received_at_utc_ms, 0),
    }
}
impl<'a> DayIngressV1<'a> {
    pub fn open(
        files: &[&'a ValidatedDirectSourceParquetFileV3],
        stream: String,
        rows: usize,
        bytes: usize,
        received_range: std::ops::Range<i64>,
        initial: Vec<(TypedRawRowV2, Option<SourceArtifactBindingV1>)>,
        max_epochs: usize,
    ) -> RecordingResult<Self> {
        let cursors = files
            .iter()
            .enumerate()
            .map(|(file, _)| FileCursorV1 {
                file,
                cursor: TypedRawCursorV1 {
                    row_group: 0,
                    row_in_group: 0,
                    physical_row: 0,
                },
                order_position: 0,
                pending: BTreeMap::new(),
            })
            .collect();
        let epochs = epoch_ranks(files, rows, &received_range, max_epochs)?;
        let order = crate::day_order::locate(files, rows, &received_range, &epochs)?;
        let mut keyed = initial
            .into_iter()
            .map(|(row, binding)| {
                let e = source_evidence_v3_from_typed_raw_v2(&row)?;
                Ok(((admission(&row, &e), e.source_message_id.0), row, binding))
            })
            .collect::<RecordingResult<Vec<_>>>()?;
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        let initial = keyed
            .into_iter()
            .map(|(_, row, binding)| (row, binding))
            .collect::<Vec<_>>();
        let saved = DayIngressCheckpointV1 {
            files: cursors,
            order: Arc::new(order),
            order_reference: None,
            epochs,
            stream,
            position: 0,
            ended: false,
            initial: initial.into(),
            twap: Vec::new(),
        };
        Self::restore(files, saved, rows, bytes, received_range)
    }
    pub fn restore(
        files: &[&'a ValidatedDirectSourceParquetFileV3],
        mut saved: DayIngressCheckpointV1,
        rows: usize,
        bytes: usize,
        _received_range: std::ops::Range<i64>,
    ) -> RecordingResult<Self> {
        if let Some(reference) = &saved.order_reference {
            if index_hash(&reference.path)? != reference.sha256 {
                return Err("原日顺序索引损坏".into());
            }
            saved.order = ciborium::de::from_reader(std::io::BufReader::new(std::fs::File::open(
                &reference.path,
            )?))?;
        }
        let mut source_minima = Vec::new();
        let mut pm_minima = Vec::new();
        let mut heads = BTreeMap::new();
        for (i, order) in saved.order.iter().enumerate() {
            let mut minima = vec![None; order.len() + 1];
            let mut pm = vec![None; order.len() + 1];
            for j in (0..order.len()).rev() {
                let c = &order[j];
                minima[j] = minima[j + 1].into_iter().chain(c.source_at).min();
                let key =
                    (c.source == 1).then_some((c.first, c.source_at.unwrap_or(0), c.sequence, 0));
                pm[j] = pm[j + 1].into_iter().chain(key).min();
            }
            source_minima.push(minima);
            pm_minima.push(pm);
            if let Some(c) = order.get(saved.files[i].order_position) {
                heads.insert(
                    (c.source, c.first, c.sequence, c.identity.clone(), i),
                    c.received,
                );
            }
        }
        Ok(Self {
            files: saved
                .files
                .iter()
                .map(|cursor| FileStreamV1 {
                    file: files[cursor.file],
                    saved: cursor.clone(),
                    stream: None,
                })
                .collect(),
            saved,
            rows,
            bytes,
            last_initialization_binding: None,
            source_minima,
            pm_minima,
            heads,
        })
    }
    /// 不可变窄列顺序索引只保存一次；周期检查点只引用它并保存消费位置。
    pub fn persist_order_index(&mut self, path: &std::path::Path) -> RecordingResult<()> {
        if self.saved.order_reference.is_some() {
            return Ok(());
        }
        use std::io::Write;
        std::fs::create_dir_all(path.parent().ok_or("日顺序索引没有父目录")?)?;
        let mut path = path.to_owned();
        let mut suffix = 0u64;
        while path.exists() {
            suffix += 1;
            path = path.with_file_name(format!("source-order-{suffix:020}.cbor"));
        }
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        ciborium::ser::into_writer(&self.saved.order, &mut file)?;
        file.flush()?;
        file.sync_all()?;
        std::fs::File::open(path.parent().unwrap())?.sync_all()?;
        self.saved.order_reference = Some(DayOrderReferenceV1 {
            sha256: index_hash(&path)?,
            path,
        });
        Ok(())
    }
    pub fn checkpoint(&self) -> DayIngressCheckpointV1 {
        let mut saved = self.saved.clone();
        saved.files = self.files.iter().map(|file| file.saved.clone()).collect();
        if saved.order_reference.is_some() {
            saved.order = Arc::new(Vec::new());
        }
        saved
    }
    /// 全部未消费的原行都在最小顺序索引中；批次预读不推进消费位置。
    pub fn unread_source_lower_bound(&self) -> RecordingResult<Option<i64>> {
        let mut minimum = self
            .heads
            .keys()
            .filter_map(|key| self.source_minima[key.4][self.files[key.4].saved.order_position])
            .min();
        for (row, _) in &self.saved.initial {
            minimum = minimum
                .into_iter()
                .chain(source_evidence_v3_from_typed_raw_v2(row)?.source_event_at_utc_ms)
                .min();
        }
        Ok(minimum)
    }
    pub fn unread_pm_lower_bound(&self) -> Option<(u64, i64, u64, u64)> {
        self.heads
            .keys()
            .filter_map(|key| self.pm_minima[key.4][self.files[key.4].saved.order_position])
            .min()
    }
    pub fn epochs(&self) -> &BTreeMap<(u8, String), u64> {
        &self.saved.epochs
    }
    pub fn initialization_binding(&self) -> Option<&SourceArtifactBindingV1> {
        self.last_initialization_binding.as_ref()
    }
    pub fn next_input(&mut self) -> RecordingResult<Option<RecordedIngressV1>> {
        if self.saved.ended {
            return Ok(None);
        }
        let mut source_heads: BTreeMap<u8, ((u8, u64, u64, String, usize), i64)> = BTreeMap::new();
        for s in 0..6u8 {
            if let Some((key, received)) = self
                .heads
                .range((s, 0, 0, String::new(), 0)..)
                .next()
                .filter(|(key, _)| key.0 == s)
            {
                source_heads.insert(s, (key.clone(), *received));
            }
        }
        let mut initial_head = None;
        if let Some((row, _)) = self.saved.initial.front() {
            let e = source_evidence_v3_from_typed_raw_v2(row)?;
            let a = admission(row, &e);
            let key = (
                0,
                (a.0 as u64) ^ (1 << 63),
                a.1,
                e.source_message_id.0.clone(),
                usize::MAX,
            );
            if source_heads.get(&0).is_none_or(|(raw, _)| key < *raw) {
                source_heads.insert(0, (key.clone(), e.received_at_utc_ms));
                initial_head = Some(e);
            }
        }
        let chosen = source_heads
            .into_iter()
            .min_by(|(sa, (ka, ra)), (sb, (kb, rb))| (ra, sa, &ka.3).cmp(&(rb, sb, &kb.3)));
        self.last_initialization_binding = None;
        let ingress = if let Some((_, (key, _))) = chosen {
            let (row, source_evidence) = if key.4 == usize::MAX {
                let (row, binding) = self.saved.initial.pop_front().unwrap();
                self.last_initialization_binding = binding;
                (row, initial_head.unwrap())
            } else {
                let i = key.4;
                self.heads.remove(&key);
                let coordinate = &self.saved.order[i][self.files[i].saved.order_position];
                self.files[i].head(coordinate.physical_row, self.rows, self.bytes)?;
                let row = self.files[i]
                    .saved
                    .pending
                    .remove(&coordinate.physical_row)
                    .unwrap();
                let evidence = source_evidence_v3_from_typed_raw_v2(&row)?;
                self.files[i].saved.order_position += 1;
                if let Some(next) = self.saved.order[i].get(self.files[i].saved.order_position) {
                    self.heads.insert(
                        (
                            next.source,
                            next.first,
                            next.sequence,
                            next.identity.clone(),
                            i,
                        ),
                        next.received,
                    );
                }
                (row, evidence)
            };
            if let TypedRawRowV2::PolymarketRtdsChainlinkTwap(ref twap) = row {
                self.saved
                    .twap
                    .push((self.saved.position, twap.clone(), source_evidence.clone()));
            }
            DptIngressV1::Message {
                row,
                source_evidence,
            }
        } else {
            self.saved.ended = true;
            DptIngressV1::EndOfInput
        };
        let result = RecordedIngressV1 {
            input_stream_id: self.saved.stream.clone(),
            input_position: self.saved.position,
            ingress,
            dependency_record_refs: Vec::new(),
        };
        self.saved.position += 1;
        Ok(Some(result))
    }
    pub fn prepare_input<'b>(
        &self,
        core: &PublicObservationCoreV1,
        input: &'b mut RecordedIngressV1,
    ) -> RecordingResult<PreparedIngressV1<'b>> {
        let Some(market) = crate::public_replay::reference_market_for_input(core, input) else {
            return Ok(prepare_public_ingress_v1(
                core,
                input,
                &BTreeMap::new(),
                &[],
            )?);
        };
        Ok(prepare_public_ingress_with_reference_v1(
            core,
            input,
            &BTreeMap::new(),
            &[],
            &market,
            |cursor| {
                let start = self
                    .saved
                    .twap
                    .partition_point(|(position, _, _)| *position < cursor.next_input_position);
                let page = &self.saved.twap
                    [start..self.saved.twap.len().min(start.saturating_add(self.rows))];
                let values: Vec<_> = page
                    .iter()
                    .map(|(position, row, source)| {
                        let event = canonicalize_chainlink_twap_v1(
                            row,
                            source,
                            &core.header().canonical_rule_version,
                        )?;
                        Ok(TemporaryReferenceCandidateV1 {
                            canonical_fact: event.clone(),
                            record_ref: CanonicalInputRecordRefV1 {
                                input_stream_id: self.saved.stream.clone(),
                                input_position: *position,
                            },
                            anchor: DirectObservationAnchorV1 {
                                field_path: if row.window_s == 30 {
                                    "chainlink_twap_thirty"
                                } else {
                                    "chainlink_twap_sixty"
                                }
                                .into(),
                                position: PublicFactPositionV1 {
                                    input_stream_id: self.saved.stream.clone(),
                                    closed_at_input_position: *position,
                                    fact_ordinal: 0,
                                },
                                group_id: source.source_message_id.clone(),
                                source_message_id: source.source_message_id.clone(),
                                source_event_at_utc_ms: source.source_event_at_utc_ms,
                                received_at_utc_ms: source.received_at_utc_ms,
                            },
                        })
                    })
                    .collect::<Result<_, data_pipeline_core::ContractError>>()?;
                let next_input_position = values
                    .last()
                    .map(|v| v.record_ref.input_position + 1)
                    .unwrap_or(cursor.next_input_position);
                let exhausted = start + page.len() == self.saved.twap.len();
                Ok((
                    values,
                    TemporaryReferenceCursorV1 {
                        next_input_position,
                    },
                    exhausted,
                ))
            },
        )?)
    }
}

// 只扫真实准入窄列，不解码盘口、编码宽录制或归约公共前缀。
fn epoch_ranks(
    files: &[&ValidatedDirectSourceParquetFileV3],
    rows: usize,
    range: &std::ops::Range<i64>,
    limit: usize,
) -> RecordingResult<BTreeMap<(u8, String), u64>> {
    use crate::canonical_run::{CaptureCoordinate, rank_epochs, update_epoch_start};
    use arrow_array::{Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
    use rayon::prelude::*;
    let parts: Vec<RecordingResult<_>> = files
        .par_iter()
        .filter(|file| {
            matches!(
                file.source_kind(),
                "polymarket_market" | "binance_depth_snapshot" | "binance_depth_update"
            )
        })
        .map(|file| {
            let source = if file.source_kind() == "polymarket_market" {
                1u8
            } else {
                2u8
            };
            let kind = if source == 1 {
                "polymarket_market"
            } else {
                "binance_depth"
            };
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file.file_path())?)?;
            let projection = ProjectionMask::columns(
                builder.parquet_schema(),
                [
                    "timestamp_received",
                    "source_sequence_epoch_id",
                    "collector_admission_sequence",
                    "collector_admission_time_utc_ms",
                ],
            );
            let mut starts = BTreeMap::new();
            for batch in builder
                .with_projection(projection)
                .with_batch_size(rows)
                .build()?
            {
                let batch = batch?;
                let epoch = batch
                    .column_by_name("source_sequence_epoch_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let received = batch
                    .column_by_name("timestamp_received")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let admitted = batch
                    .column_by_name("collector_admission_time_utc_ms")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let sequence = batch
                    .column_by_name("collector_admission_sequence")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    if !range.contains(&received.value(row)) {
                        continue;
                    }
                    if epoch.is_null(row)
                        || received.is_null(row)
                        || admitted.is_null(row)
                        || sequence.is_null(row)
                    {
                        continue;
                    }
                    update_epoch_start(
                        &mut starts,
                        epoch.value(row),
                        CaptureCoordinate {
                            admitted_at_utc_ms: admitted.value(row),
                            received_at_utc_ms: received.value(row),
                            collector_admission_sequence: sequence.value(row),
                        },
                        kind,
                        limit,
                    )?;
                }
            }
            Ok((source, starts))
        })
        .collect();
    let mut starts = [BTreeMap::new(), BTreeMap::new()];
    for part in parts {
        let (source, entries) = part?;
        for (epoch, coordinate) in entries {
            update_epoch_start(
                &mut starts[(source - 1) as usize],
                &epoch,
                coordinate,
                if source == 1 {
                    "polymarket_market"
                } else {
                    "binance_depth"
                },
                limit,
            )?;
        }
    }
    let mut epochs = BTreeMap::new();
    for (index, entries) in starts.into_iter().enumerate() {
        for (epoch, rank) in rank_epochs(
            entries,
            if index == 0 {
                "polymarket_market"
            } else {
                "binance_depth"
            },
        )? {
            epochs.insert(((index + 1) as u8, epoch), rank as u64);
        }
    }
    Ok(epochs)
}
