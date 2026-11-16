//! 冻结场景与原隐藏事实合流；公开接收路径由原日生产者独立交付。
use crate::MaterialError;
use contracts::*;
use serde::{Deserialize, Serialize};
use sim_system::{offline_scene::*, public_consumption::*};
use std::{collections::BTreeMap, iter::Peekable, sync::Arc};
pub const ORDERING_VERSION: &str = "offline-scene-before-reference-before-pm-v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OfflineSceneMaterialV1 {
    pub scene: PreparedScene,
    pub markets: BTreeMap<String, MarketIdentityV2>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub market_assets: BTreeMap<String, [AssetIdentityV1; 2]>,
    pub source_manifests: Vec<(String, String)>,
}
pub(crate) struct SceneHiddenSourceV1<
    I: Iterator<Item = Result<AcceptedHiddenStepBucketV1, MaterialError>>,
> {
    original: Peekable<I>,
    material: Arc<OfflineSceneMaterialV1>,
    events: Peekable<Box<dyn Iterator<Item = Result<Event, MaterialError>> + Send>>,
    index: u64,
    last_source: Option<StreamingHiddenSourceBindingV1>,
}
impl<I: Iterator<Item = Result<AcceptedHiddenStepBucketV1, MaterialError>>> SceneHiddenSourceV1<I> {
    pub(crate) fn from_reader(
        original: I,
        material: Arc<OfflineSceneMaterialV1>,
        events: Box<dyn Iterator<Item = Result<Event, MaterialError>> + Send>,
    ) -> Self {
        Self {
            original: original.peekable(),
            material,
            events: events.peekable(),
            index: 0,
            last_source: None,
        }
    }
}
impl<I: Iterator<Item = Result<AcceptedHiddenStepBucketV1, MaterialError>>> Iterator
    for SceneHiddenSourceV1<I>
{
    type Item = Result<AcceptedHiddenStepBucketV1, MaterialError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.original.peek().is_some_and(Result::is_err) {
            return self.original.next();
        }
        let original_at = self
            .original
            .peek()
            .and_then(|x| x.as_ref().ok())
            .map(|x| x.reference().bucket_time_utc_ms());
        if self.events.peek().is_some_and(Result::is_err) {
            return self.events.next().map(|e| e.map(|_| unreachable!()));
        }
        let scene_at = self
            .events
            .peek()
            .and_then(|e| e.as_ref().ok())
            .map(|e| e.at_ms);
        let at = original_at.into_iter().chain(scene_at).min()?;
        // 预读只取得来源绑定；对外发布内容及时间继续使用原链。
        if self.last_source.is_none() {
            if let Some(Ok(first)) = self.original.peek() {
                let HiddenBucketReferenceV2::Streaming(r) = first.reference() else {
                    return Some(Err("离线场景日来源需要正式流式输入".into()));
                };
                self.last_source = Some(r.source.clone());
            }
        }
        let mut records = Vec::new();
        while self
            .events
            .peek()
            .and_then(|e| e.as_ref().ok())
            .is_some_and(|e| e.at_ms == at)
        {
            let event = self.events.next().unwrap().unwrap();
            let Some(market) = self.material.markets.get(&event.level.condition) else {
                return Some(Err("冻结场景缺少正式市场身份".into()));
            };
            records.push(HiddenExecutionRecordV1::OfflineScene {
                market: market.clone(),
                assets: self
                    .material
                    .market_assets
                    .get(&event.level.condition)
                    .cloned(),
                event,
            });
        }
        if original_at == Some(at) {
            let original = self.original.next().unwrap().unwrap();
            let HiddenBucketReferenceV2::Streaming(reference) = original.reference() else {
                return Some(Err("离线场景日来源需要正式流式输入".into()));
            };
            self.last_source = Some(reference.source.clone());
            records.extend_from_slice(original.records());
        }
        if self.original.peek().is_some_and(Result::is_err) {
            return self.original.next();
        }
        let next_original = self
            .original
            .peek()
            .and_then(|x| x.as_ref().ok())
            .map(|x| x.reference().bucket_time_utc_ms());
        if self.events.peek().is_some_and(Result::is_err) {
            return self.events.next().map(|e| e.map(|_| unreachable!()));
        }
        let next_scene = self
            .events
            .peek()
            .and_then(|e| e.as_ref().ok())
            .map(|e| e.at_ms);
        let closure = next_original.into_iter().chain(next_scene).min().map_or(
            ReplayIndexClosureV1::EndOfInput,
            ReplayIndexClosureV1::NextGlobalEventAt,
        );
        let Some(mut source) = self.last_source.clone() else {
            return Some(Err("冻结场景没有可复现的原始来源绑定".into()));
        };
        source.ordering_version = ORDERING_VERSION.into();
        let start = self.index;
        self.index += records.len() as u64;
        Some(Ok(AcceptedHiddenStepBucketV1::from_streaming(
            ClosedStreamingHiddenStepBucketV1 {
                source,
                bucket_time_utc_ms: at,
                index_start: start,
                index_end: self.index,
                closure,
            },
            records,
        )))
    }
}

/// 准备阶段复用原日供料及正式归约，不建立另一份原始解码或观察副本。
pub fn collect_scene_facts_v1(
    config: &crate::HistoricalPreparationConfigV1,
    target_start: i64,
    target_end: i64,
    progress_seconds: u64,
) -> Result<
    (
        Analysis,
        BTreeMap<String, MarketIdentityV2>,
        BTreeMap<String, [AssetIdentityV1; 2]>,
        Vec<(String, String)>,
    ),
    MaterialError,
> {
    use data_pipeline_core::pm_reduction::PmStepChangeV1 as S;
    use data_pipeline_training::{
        day_producer::DayProducerV1, day_stream::DayIngressV1,
        hidden_preparation::ResolvedHiddenRecordV1,
        historical_initialization::MetadataInitializationSelectionV1,
    };
    use rust_decimal::Decimal;
    let inputs =
        data_pipeline_training::freeze_public_source_input_set_v1(&config.scope.manifest_paths)?;
    let admitted = inputs.admitted_daily_inputs();
    // 冻结场景只消费PM；来源仍引用原清单，不把中途Binance切片送入无前序状态的Core。
    let files = admitted
        .iter()
        .flat_map(|d| d.files())
        .filter(|f| {
            matches!(
                f.source_kind(),
                "polymarket_market" | "polymarket_market_metadata"
            )
        })
        .collect::<Vec<_>>();
    let selection = config
        .metadata_initialization
        .as_ref()
        .map(|c| {
            MetadataInitializationSelectionV1::open(
                c,
                config.read_page_rows,
                config.max_read_batch_bytes,
            )
        })
        .transpose()?;
    let initial = selection
        .as_ref()
        .map(|s| {
            s.read(config.read_page_rows, config.max_read_batch_bytes)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let ingress = DayIngressV1::open(
        &files,
        config.header.input_stream_id.clone(),
        config.read_page_rows,
        config.max_read_batch_bytes,
        config.scope.received_start_utc_ms..config.scope.received_end_utc_ms,
        initial,
        config.canonical.max_epoch_count_per_source,
    )?;
    let mut producer = DayProducerV1::new(
        ingress,
        config.effective_header(),
        Sha256Hex(inputs.input_set_identity_sha256().into()),
        config.hidden_stream_id.clone(),
    )?;
    let mut facts = Vec::new();
    let mut markets = BTreeMap::new();
    let mut market_assets = BTreeMap::new();
    let mut tokens = BTreeMap::<String, Vec<String>>::new();
    let mut quantities = BTreeMap::<Level, u128>::new();
    let units = |s: &str| -> Result<u128, MaterialError> {
        let d: Decimal = s.parse()?;
        scene_micro_units(d)
    };
    let price =
        |s: &str| -> Result<u32, MaterialError> { u32::try_from(units(s)?).map_err(Into::into) };
    let started = std::time::Instant::now();
    let mut progress = started;
    let mut count = 0u64;
    let mut sequence = 0u64;
    while let Some(applied) = producer.next_applied()? {
        count += 1;
        for (id, context) in &applied.contexts {
            if context.market.window.start_at_utc_ms >= target_start
                && context.market.window.start_at_utc_ms < target_end
            {
                markets
                    .entry(id.clone())
                    .or_insert_with(|| context.market.clone());
                market_assets.entry(id.clone()).or_insert(
                    context
                        .assets
                        .iter()
                        .map(|a| a.asset.clone())
                        .collect::<Vec<_>>()
                        .try_into()
                        .map_err(|_| "冻结场景市场不是二元身份")?,
                );
                tokens.entry(id.clone()).or_insert_with(|| {
                    context
                        .assets
                        .iter()
                        .map(|a| a.asset.token_id.0.clone())
                        .collect()
                });
            }
        }
        for bucket in applied.hidden {
            for record in bucket.records {
                let ResolvedHiddenRecordV1::Step { stored, market } = record else {
                    continue;
                };
                let Some(pair) = tokens.get(&market.condition_id.0) else {
                    continue;
                };
                let step = stored.step;
                let opposite = |token: &str| -> Result<String, MaterialError> {
                    pair.iter()
                        .find(|t| t.as_str() != token)
                        .cloned()
                        .ok_or_else(|| "二元镜像缺少互补token".into())
                };
                let canonical =
                    |token: &str, side: BookSide, p: u32| -> Result<Level, MaterialError> {
                        Ok(Level {
                            condition: market.condition_id.0.clone(),
                            buy_token: if side == BookSide::Bid {
                                token.into()
                            } else {
                                opposite(token)?
                            },
                            price_micro: if side == BookSide::Bid {
                                p
                            } else {
                                1_000_000 - p
                            },
                        })
                    };
                let mut push = |level: Level, kind: FactKind| {
                    facts.push(Fact {
                        sequence,
                        received_at_ms: step.direct_time.received_at_utc_ms,
                        source_at_ms: step.direct_time.source_event_at_utc_ms,
                        level,
                        kind,
                    });
                    sequence += 1;
                };
                match step.change {
                    S::InitialSnapshot {
                        asset, bids, asks, ..
                    } => {
                        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
                            for level in levels.iter() {
                                let l = canonical(
                                    &asset.token_id.0,
                                    side,
                                    price(&level.price_pusd_per_share.0)?,
                                )?;
                                let after = units(&level.quantity_shares.0)?;
                                match quantities.insert(l.clone(), after) {
                                    None => push(l, FactKind::Initial(after)),
                                    Some(before) if before != after => {
                                        push(l, FactKind::Recovery { before, after })
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    S::LevelChange { transition: t, .. } => {
                        let l = canonical(
                            &t.asset.token_id.0,
                            t.side,
                            price(&t.price_pusd_per_share.0)?,
                        )?;
                        let after = units(&t.quantity_after_shares.0)?;
                        let before = quantities.insert(l.clone(), after).unwrap_or(0);
                        if before != after {
                            push(
                                l,
                                if t.basis == LevelExecutionBasis::BookResyncDeltaInference {
                                    FactKind::Recovery { before, after }
                                } else if after > before {
                                    FactKind::Add(after - before)
                                } else {
                                    FactKind::Reduce(before - after)
                                },
                            );
                        }
                    }
                    S::PublicTrade { trade } => {
                        let l = canonical(
                            &trade.asset.token_id.0,
                            if trade.aggressor_side == TradeSide::Sell {
                                BookSide::Bid
                            } else {
                                BookSide::Ask
                            },
                            price(&trade.price_pusd_per_share.0)?,
                        )?;
                        push(l, FactKind::Trade(units(&trade.quantity_shares.0)?));
                    }
                    _ => {}
                }
            }
        }
        if progress_seconds > 0 && progress.elapsed().as_secs() >= progress_seconds {
            eprintln!(
                "场景准备: inputs={count}, facts={}, markets={}, elapsed_seconds={}, total_inputs=unknown",
                facts.len(),
                markets.len(),
                started.elapsed().as_secs()
            );
            progress = std::time::Instant::now();
        }
    }
    let manifests = admitted
        .iter()
        .map(|d| (d.utc_day().to_string(), d.manifest_sha256().to_string()))
        .collect();
    Ok((Analysis::new(facts), markets, market_assets, manifests))
}

#[derive(Serialize, Deserialize)]
struct StoredSceneV1 {
    model_version: String,
    parameters: Parameters,
    report: PreparationReport,
    levels: Vec<Level>,
    events: Vec<(i64, u64, usize, EventKind)>,
    markets: BTreeMap<String, MarketIdentityV2>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    market_assets: BTreeMap<String, [AssetIdentityV1; 2]>,
    source_manifests: Vec<(String, String)>,
}
impl OfflineSceneMaterialV1 {
    /// 持久化只保留一次价位身份，不在每条外部事件复制长token字符串。
    pub fn write_compact(self, path: &std::path::Path) -> Result<(), MaterialError> {
        let mut ids = BTreeMap::<Level, usize>::new();
        let mut levels = Vec::new();
        let mut events = Vec::with_capacity(self.scene.events.len());
        for event in self.scene.events {
            let index = *ids.entry(event.level.clone()).or_insert_with(|| {
                let i = levels.len();
                levels.push(event.level);
                i
            });
            events.push((event.at_ms, event.source_sequence, index, event.kind));
        }
        let stored = StoredSceneV1 {
            model_version: self.scene.model_version,
            parameters: self.scene.parameters,
            report: self.scene.report,
            levels,
            events,
            markets: self.markets,
            market_assets: self.market_assets,
            source_manifests: self.source_manifests,
        };
        serde_json::to_writer(
            std::io::BufWriter::new(std::fs::File::create(path)?),
            &stored,
        )?;
        Ok(())
    }
}

/// A small authenticated manifest points to an immutable, losslessly compressed
/// event stream. Event order is the global preparation emission order.
#[derive(Serialize, Deserialize)]
struct StreamManifestV2 {
    storage_version: String,
    material: OfflineSceneMaterialV1,
    event_file: String,
    event_sha256: String,
    event_count: u64,
}
pub type SceneEventReader = Box<dyn Iterator<Item = Result<Event, MaterialError>> + Send>;
pub struct SceneStreamWriterV2 {
    path: std::path::PathBuf,
    payload: std::path::PathBuf,
    encoder: zstd::stream::write::Encoder<'static, std::io::BufWriter<std::fs::File>>,
    events: u64,
    last_at: Option<i64>,
}
fn file_sha256(path: &std::path::Path) -> Result<String, MaterialError> {
    file_sha256_with_progress(path, 0)
}
fn file_sha256_with_progress(
    path: &std::path::Path,
    interval_seconds: u64,
) -> Result<String, MaterialError> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let total = file.metadata()?.len();
    let started = std::time::Instant::now();
    let mut last = started;
    let mut read = 0_u64;
    if interval_seconds > 0 {
        eprintln!(
            "offline_scene_sha path={} bytes=0/{} elapsed_seconds=0",
            path.display(),
            total
        );
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
        read += n as u64;
        if interval_seconds > 0 && last.elapsed().as_secs() >= interval_seconds {
            eprintln!(
                "offline_scene_sha path={} bytes={}/{} elapsed_seconds={:.3}",
                path.display(),
                read,
                total,
                started.elapsed().as_secs_f64()
            );
            last = std::time::Instant::now();
        }
    }
    if interval_seconds > 0 {
        eprintln!(
            "offline_scene_sha_complete path={} bytes={}/{} elapsed_seconds={:.3}",
            path.display(),
            read,
            total,
            started.elapsed().as_secs_f64()
        );
    }
    Ok(format!("{:x}", digest.finalize()))
}
impl SceneStreamWriterV2 {
    pub fn create(path: &std::path::Path) -> Result<Self, MaterialError> {
        let payload = path.with_extension("events.zst");
        let mut encoder = zstd::stream::write::Encoder::new(
            std::io::BufWriter::new(std::fs::File::create(&payload)?),
            3,
        )?;
        encoder.include_checksum(true)?;
        Ok(Self {
            path: path.into(),
            payload,
            encoder,
            events: 0,
            last_at: None,
        })
    }
    pub fn push(&mut self, event: Event) -> Result<(), MaterialError> {
        use std::io::Write;
        if self.last_at.is_some_and(|last| event.at_ms < last) {
            return Err("场景内部时间不递增".into());
        }
        self.last_at = Some(event.at_ms);
        serde_json::to_writer(&mut self.encoder, &event)?;
        self.encoder.write_all(b"\n")?;
        self.events += 1;
        Ok(())
    }
    pub fn finish(self, material: OfflineSceneMaterialV1) -> Result<u64, MaterialError> {
        use std::io::Write;
        if !material.scene.events.is_empty() {
            return Err("流式场景头不能包含另一份事件".into());
        }
        self.encoder.finish()?.flush()?;
        let manifest = StreamManifestV2 {
            storage_version: "OFFLINE_SCENE_JSONL_ZSTD_V2".into(),
            material,
            event_file: self
                .payload
                .file_name()
                .ok_or("缺少场景文件名")?
                .to_str()
                .ok_or("场景文件名不是UTF8")?
                .into(),
            event_sha256: file_sha256(&self.payload)?,
            event_count: self.events,
        };
        let mut manifest_file = std::io::BufWriter::new(std::fs::File::create(&self.path)?);
        serde_json::to_writer_pretty(&mut manifest_file, &manifest)?;
        manifest_file.flush()?;
        Ok(std::fs::metadata(self.path)?.len() + std::fs::metadata(self.payload)?.len())
    }
}
struct CheckedSceneEvents<I> {
    inner: I,
    remaining: u64,
    last_at: Option<i64>,
    ended: bool,
}
impl<I: Iterator<Item = Result<Event, serde_json::Error>>> Iterator for CheckedSceneEvents<I> {
    type Item = Result<Event, MaterialError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        match self.inner.next() {
            Some(Ok(event))
                if self.remaining > 0 && self.last_at.is_none_or(|last| event.at_ms >= last) =>
            {
                self.remaining -= 1;
                self.last_at = Some(event.at_ms);
                Some(Ok(event))
            }
            Some(Ok(_)) => {
                self.ended = true;
                Some(Err("冻结场景事件数或顺序不符".into()))
            }
            Some(Err(e)) => {
                self.ended = true;
                Some(Err(e.into()))
            }
            None => {
                self.ended = true;
                if self.remaining == 0 {
                    None
                } else {
                    Some(Err("冻结场景事件提前结束".into()))
                }
            }
        }
    }
}
/// Legacy compact inputs remain readable. New streams retain only their next
/// event; the hidden source retains the complete current millisecond bucket.
pub fn open_scene_material(
    path: &std::path::Path,
) -> Result<(Arc<OfflineSceneMaterialV1>, SceneEventReader, Sha256Hex), MaterialError> {
    open_scene_material_with_progress(path, 0)
}
pub fn open_scene_material_with_progress(
    path: &std::path::Path,
    interval_seconds: u64,
) -> Result<(Arc<OfflineSceneMaterialV1>, SceneEventReader, Sha256Hex), MaterialError> {
    let sha = Sha256Hex(file_sha256_with_progress(path, interval_seconds)?);
    let file = std::fs::File::open(path)?;
    if path.extension().is_some_and(|e| e == "scene") {
        let manifest: StreamManifestV2 = serde_json::from_reader(std::io::BufReader::new(file))?;
        if manifest.storage_version != "OFFLINE_SCENE_JSONL_ZSTD_V2"
            || !manifest.material.scene.events.is_empty()
        {
            return Err("离线场景存储版本或头不符".into());
        }
        let name = std::path::Path::new(&manifest.event_file);
        if name.components().count() != 1
            || !matches!(
                name.components().next(),
                Some(std::path::Component::Normal(_))
            )
        {
            return Err("离线场景载荷路径越界".into());
        }
        let payload = path.parent().ok_or("离线场景没有父目录")?.join(name);
        if file_sha256_with_progress(&payload, interval_seconds)? != manifest.event_sha256 {
            return Err("离线场景载荷SHA256不符".into());
        }
        let decoder = zstd::stream::read::Decoder::new(std::fs::File::open(payload)?)?;
        let events = serde_json::Deserializer::from_reader(std::io::BufReader::new(decoder))
            .into_iter::<Event>();
        Ok((
            Arc::new(manifest.material),
            Box::new(CheckedSceneEvents {
                inner: events,
                remaining: manifest.event_count,
                last_at: None,
                ended: false,
            }),
            sha,
        ))
    } else {
        let stored: StoredSceneV1 = serde_json::from_reader(std::io::BufReader::new(file))?;
        let levels = stored.levels;
        let events = stored
            .events
            .into_iter()
            .map(move |(at_ms, source_sequence, index, kind)| {
                Ok(Event {
                    at_ms,
                    source_sequence,
                    level: levels.get(index).ok_or("场景价位引用不存在")?.clone(),
                    kind,
                })
            });
        Ok((
            Arc::new(OfflineSceneMaterialV1 {
                scene: PreparedScene {
                    model_version: stored.model_version,
                    parameters: stored.parameters,
                    events: Vec::new(),
                    report: stored.report,
                },
                markets: stored.markets,
                market_assets: stored.market_assets,
                source_manifests: stored.source_manifests,
            }),
            Box::new(events),
            sha,
        ))
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    #[test]
    fn compressed_scene_preserves_u128_targets_and_rejects_corrupt_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.scene");
        let event = Event {
            at_ms: 1,
            source_sequence: 99,
            level: Level {
                condition: "c".into(),
                buy_token: "t".into(),
                price_micro: 500_000,
            },
            kind: EventKind::Cancel {
                targets: vec![Slice {
                    segment: 42,
                    start: 1,
                    end: u128::MAX,
                }],
                recovery: false,
            },
        };
        let parameters = Parameters {
            association_ms: 250,
            allocation: Allocation::Nearest,
            cancellation: Cancellation::SegmentProportionalV2,
            internal_time: InternalTime::SourceWhenPresent,
            seed: 7,
            association_trade_shift_ms: 0,
        };
        let mut writer = SceneStreamWriterV2::create(&path).unwrap();
        writer.push(event.clone()).unwrap();
        writer
            .finish(OfflineSceneMaterialV1 {
                scene: PreparedScene {
                    model_version: MODEL_VERSION.into(),
                    parameters,
                    report: PreparationReport::default(),
                    events: Vec::new(),
                },
                markets: BTreeMap::new(),
                market_assets: BTreeMap::new(),
                source_manifests: Vec::new(),
            })
            .unwrap();
        let (_, mut reader, _) = open_scene_material(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), event);
        assert!(reader.next().is_none());
        std::fs::write(path.with_extension("events.zst"), b"corrupt").unwrap();
        assert!(
            open_scene_material(&path)
                .err()
                .unwrap()
                .to_string()
                .contains("SHA256")
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RankedSceneEvent {
    rank: usize,
    event: Event,
}
#[derive(Debug, Serialize)]
pub struct ScenePreparationCostV2 {
    pub events: u64,
    pub output_bytes: u64,
    pub quantity_plan_seconds: f64,
    pub construction_seconds: f64,
    pub merge_seconds: f64,
    pub temporary_logical_bytes: u64,
    pub condition_workers: usize,
    pub report: PreparationReport,
}
/// Independent conditions write bounded compressed temporary streams. No worker
/// waits for another condition's output, so a late full buffer cannot deadlock
/// the next global event. Only the merge clamps initialization timestamps.
pub fn prepare_condition_parallel_scene_v2(
    analysis: &Analysis,
    parameters: Parameters,
    markets: &BTreeMap<String, MarketIdentityV2>,
    market_assets: &BTreeMap<String, [AssetIdentityV1; 2]>,
    source_manifests: &[(String, String)],
    path: &std::path::Path,
    execution_start: i64,
    condition_workers: usize,
    progress_seconds: u64,
) -> Result<ScenePreparationCostV2, MaterialError> {
    prepare_condition_parallel_scene_with_temporary_root_v2(
        analysis,
        parameters,
        markets,
        market_assets,
        source_manifests,
        path,
        execution_start,
        condition_workers,
        progress_seconds,
        None,
    )
}
/// The optional fresh directory belongs to this one scene. Temporary streams
/// may live on a different filesystem from the final authenticated material.
pub fn prepare_condition_parallel_scene_with_temporary_root_v2(
    analysis: &Analysis,
    parameters: Parameters,
    markets: &BTreeMap<String, MarketIdentityV2>,
    market_assets: &BTreeMap<String, [AssetIdentityV1; 2]>,
    source_manifests: &[(String, String)],
    path: &std::path::Path,
    execution_start: i64,
    condition_workers: usize,
    progress_seconds: u64,
    condition_temporary_root: Option<&std::path::Path>,
) -> Result<ScenePreparationCostV2, MaterialError> {
    use std::{
        io::Write,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };
    if condition_workers == 0 {
        return Err("condition_workers需要大于零".into());
    }
    if let Some(directory) = condition_temporary_root {
        // A failed job keeps its files. Never reuse that scene's directory.
        std::fs::create_dir(directory)?;
    }
    let started = Instant::now();
    eprintln!("场景数量预计划: start, total_conditions={}", markets.len());
    let mut last_plan = Instant::now();
    let mut phase = String::new();
    let plan = analysis.condition_plan_with_progress(parameters.clone(),|current,completed,total| {
        if current!=phase || completed==total || (progress_seconds>0 && last_plan.elapsed().as_secs()>=progress_seconds) {
            eprintln!("场景数量预计划: phase={current}, completed={completed}, total={total}, elapsed_seconds={}",started.elapsed().as_secs());
            phase=current.into();last_plan=Instant::now();
        }
    });
    let quantity_plan_seconds = started.elapsed().as_secs_f64();
    eprintln!(
        "场景数量预计划完成: conditions={}, elapsed_seconds={quantity_plan_seconds}",
        plan.condition_count()
    );
    let pending = Mutex::new(0..plan.condition_count());
    let completed = AtomicUsize::new(0);
    let results = std::thread::scope(
        |scope| -> Result<Vec<(std::path::PathBuf, u64, PreparationReport)>, MaterialError> {
            let mut handles = Vec::new();
            for _ in 0..condition_workers.min(plan.condition_count()) {
                let plan = &plan;
                let pending = &pending;
                let completed = &completed;
                handles.push(scope.spawn(move || -> Result<Vec<(std::path::PathBuf,u64,PreparationReport)>,MaterialError> {
                let mut results=Vec::new();
                loop {
                    let Some(index)=pending.lock().unwrap().next() else {break;};
                    let temp=match condition_temporary_root {
                        Some(directory)=>directory.join(format!("condition-{index}.jsonl.zst")),
                        None=>path.with_extension(format!("condition-{index}.jsonl.zst")),
                    };
                    let mut encoder=zstd::stream::write::Encoder::new(std::io::BufWriter::new(std::fs::File::create(&temp)?),3)?;
                    encoder.include_checksum(true)?;
                    let mut last=Instant::now();let mut count=0u64;
                    let scene=plan.prepare_condition(index,|phase,done,total| {
                        if done==total || (progress_seconds>0 && last.elapsed().as_secs()>=progress_seconds) {
                            eprintln!("场景市场构造: condition={}, phase={phase}, completed={done}, total={total}, elapsed_seconds={}",plan.condition_name(index),started.elapsed().as_secs());last=Instant::now();
                        }
                    },|rank,event| -> Result<(),MaterialError> {
                        serde_json::to_writer(&mut encoder,&RankedSceneEvent {rank,event})?;encoder.write_all(b"\n")?;count+=1;Ok(())
                    })?;
                    encoder.finish()?.flush()?;
                    results.push((temp,count,scene.report));
                    let done=completed.fetch_add(1,Ordering::Relaxed)+1;
                    eprintln!("场景市场完成: completed={done}, total={}, elapsed_seconds={}",plan.condition_count(),started.elapsed().as_secs());
                }
                Ok(results)
            }));
            }
            let mut results = Vec::new();
            for handle in handles {
                results.extend(handle.join().map_err(|_| "市场构造线程panic")??);
            }
            Ok(results)
        },
    )?;
    let construction_seconds = started.elapsed().as_secs_f64() - quantity_plan_seconds;
    let report = plan.combine_reports(results.iter().map(|(_, _, r)| r.clone()));
    let expected_events: u64 = results.iter().map(|(_, n, _)| *n).sum();
    let temporary_logical_bytes = results
        .iter()
        .map(|(p, _, _)| std::fs::metadata(p).map(|m| m.len()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum();
    let merge_start = Instant::now();
    eprintln!("场景保序合流: events={expected_events}, temporary_bytes={temporary_logical_bytes}");
    type RankedReader = Box<dyn Iterator<Item = Result<RankedSceneEvent, serde_json::Error>>>;
    let mut readers = Vec::<RankedReader>::new();
    let mut heads = Vec::new();
    let mut heap = std::collections::BinaryHeap::new();
    for (path, _, _) in &results {
        let decoder = zstd::stream::read::Decoder::new(std::fs::File::open(path)?)?;
        let mut reader: RankedReader = Box::new(
            serde_json::Deserializer::from_reader(std::io::BufReader::new(decoder)).into_iter(),
        );
        let head = reader.next().transpose()?;
        if let Some(head) = &head {
            heap.push(std::cmp::Reverse((head.rank, readers.len())));
        }
        readers.push(reader);
        heads.push(head);
    }
    let mut writer = SceneStreamWriterV2::create(path)?;
    let mut events = 0;
    let mut previous_rank = None;
    let mut last = Instant::now();
    while let Some(std::cmp::Reverse((rank, index))) = heap.pop() {
        if previous_rank.is_some_and(|previous| rank < previous) {
            return Err("临时场景全局顺序不符".into());
        }
        previous_rank = Some(rank);
        let mut event = heads[index].take().unwrap().event;
        event.at_ms = event.at_ms.max(execution_start);
        writer.push(event)?;
        events += 1;
        heads[index] = readers[index].next().transpose()?;
        if let Some(head) = &heads[index] {
            heap.push(std::cmp::Reverse((head.rank, index)));
        }
        if progress_seconds > 0 && last.elapsed().as_secs() >= progress_seconds {
            eprintln!(
                "场景保序合流: completed={events}, total={expected_events}, elapsed_seconds={}",
                merge_start.elapsed().as_secs()
            );
            last = Instant::now();
        }
    }
    if events != expected_events {
        return Err("临时场景合流事件数不符".into());
    }
    let output_bytes = writer.finish(OfflineSceneMaterialV1 {
        scene: PreparedScene {
            model_version: if parameters.cancellation == Cancellation::Proportional {
                FRAGMENT_MODEL_VERSION
            } else {
                MODEL_VERSION
            }
            .into(),
            parameters,
            events: Vec::new(),
            report: report.clone(),
        },
        markets: markets.clone(),
        market_assets: market_assets.clone(),
        source_manifests: source_manifests.to_vec(),
    })?;
    // The complete authenticated final stream now contains every event. These
    // exact owned temporary paths are reconstructible, unused intermediate files.
    drop(readers);
    for (path, _, _) in results {
        std::fs::remove_file(path)?;
    }
    if let Some(directory) = condition_temporary_root {
        std::fs::remove_dir(directory)?;
    }
    Ok(ScenePreparationCostV2 {
        events,
        output_bytes,
        quantity_plan_seconds,
        construction_seconds,
        merge_seconds: merge_start.elapsed().as_secs_f64(),
        temporary_logical_bytes,
        condition_workers,
        report,
    })
}
