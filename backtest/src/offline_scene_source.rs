//! 冻结场景与原隐藏事实合流；公开接收路径由原日生产者独立交付。
use crate::MaterialError;
use contracts::*;
use serde::{Deserialize, Serialize};
use sim_system::{offline_scene::*, public_consumption::*};
use std::{collections::BTreeMap, iter::Peekable, sync::Arc};
#[path = "scene_cache.rs"]
mod scene_cache;
pub use scene_cache::{
    SharedSceneDiagnosticsV1, SharedSceneEventsV1, VerifiedOfflineSceneV1,
    verify_scene_material_with_progress,
};
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
// 三张身份表在首次登记时一起建立，此后仅检查二元性，不重复复制原身份。
fn remember_scene_market<E>(
    markets: &mut BTreeMap<String, MarketIdentityV2>,
    assets: &mut BTreeMap<String, [AssetIdentityV1; 2]>,
    tokens: &mut BTreeMap<String, Vec<String>>,
    id: &str,
    context: &data_pipeline_core::pm_reduction::PmMarketContextV1<E>,
) -> Result<bool, MaterialError> {
    if context.assets.len() != 2 { return Err("冻结场景市场不是二元身份".into()); }
    if markets.contains_key(id) { return Ok(false); }
    markets.insert(id.into(), context.market.clone());
    assets.insert(id.into(), [context.assets[0].asset.clone(), context.assets[1].asset.clone()]);
    tokens.insert(id.into(), context.assets.iter().map(|a| a.asset.token_id.0.clone()).collect());
    Ok(true)
}

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
    #[cfg(feature = "stage-timing")]
    let mut identity_work = (0_u64, 0_u64, 0_u128);
    #[cfg(feature = "stage-timing")]
    let mut fact_generation_ns = 0_u128;
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
        #[cfg(feature = "stage-timing")]
        let identity_started = std::time::Instant::now();
        for (id, context) in &applied.contexts {
            if context.market.window.start_at_utc_ms >= target_start
                && context.market.window.start_at_utc_ms < target_end
            {
                let _built = remember_scene_market(&mut markets, &mut market_assets, &mut tokens, id, context)?;
                #[cfg(feature = "stage-timing")]
                { identity_work.0 += 1; identity_work.1 += u64::from(_built); }
            }
        }
        #[cfg(feature = "stage-timing")]
        { identity_work.2 += identity_started.elapsed().as_nanos(); }
        #[cfg(feature = "stage-timing")]
        let facts_started = std::time::Instant::now();
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
        #[cfg(feature = "stage-timing")]
        { fact_generation_ns += facts_started.elapsed().as_nanos(); }
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
    let fact_count = facts.len();
    #[cfg(feature = "stage-timing")]
    let analysis_started = std::time::Instant::now();
    let analysis = Analysis::new(facts);
    #[cfg(feature = "stage-timing")]
    eprintln!("scene_collection_work={}", serde_json::json!({"inputs":count,"facts":fact_count,"markets":markets.len(),"elapsed_ns":started.elapsed().as_nanos(),"fact_generation_ns":fact_generation_ns,"analysis_index_ns":analysis_started.elapsed().as_nanos(),"producer":producer.stage_timings(),"lower_bound_work":producer.ingress.lower_bound_work(),"identity_contexts":identity_work.0,"asset_constructions":identity_work.1,"identity_ns":identity_work.2}));
    let manifests = admitted
        .iter()
        .map(|d| (d.utc_day().to_string(), d.manifest_sha256().to_string()))
        .collect();
    #[cfg(not(feature = "stage-timing"))]
    let _ = fact_count;
    Ok((analysis, markets, market_assets, manifests))
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
    encoder: zstd::stream::write::Encoder<
        'static,
        std::io::BufWriter<scene_preparation::HashingWriter<std::fs::File>>,
    >,
    events: u64,
    last_at: Option<i64>,
}
fn file_sha256_reader_with_progress(
    path: &std::path::Path,
    mut file: impl std::io::Read,
    total: u64,
    interval_seconds: u64,
) -> Result<String, MaterialError> {
    use sha2::{Digest, Sha256};
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
        Self::create_with_merge_workers(path, 1)
    }
    fn create_with_merge_workers(
        path: &std::path::Path,
        workers: usize,
    ) -> Result<Self, MaterialError> {
        if workers == 0 || workers > u32::MAX as usize {
            return Err("compression workers out of range".into());
        }
        let payload = path.with_extension("events.zst");
        let sink = scene_preparation::HashingWriter::new(std::fs::File::create(&payload)?);
        let mut encoder = zstd::stream::write::Encoder::new(std::io::BufWriter::new(sink), 3)?;
        encoder.include_checksum(true)?;
        if workers > 1 {
            encoder.multithread((workers - 1) as u32)?;
        }
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
        self.finish_with_thread_sample(material)
            .map(|(bytes, _)| bytes)
    }
    fn finish_with_thread_sample(
        self,
        material: OfflineSceneMaterialV1,
    ) -> Result<(u64, Option<usize>), MaterialError> {
        if !material.scene.events.is_empty() {
            return Err("流式场景头不能包含另一份事件".into());
        }
        let sink = self
            .encoder
            .finish()?
            .into_inner()
            .map_err(|error| error.into_error())?;
        let peak_process_threads = sink.peak_process_threads;
        let event_sha256 = sink.finish();
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
            event_sha256,
            event_count: self.events,
        };
        scene_preparation::publish_manifest(&self.path, &manifest)?;
        Ok((
            std::fs::metadata(self.path)?.len() + std::fs::metadata(self.payload)?.len(),
            peak_process_threads,
        ))
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
    let verified = verify_scene_material_with_progress(path, interval_seconds)?;
    Ok((
        verified.material(),
        verified.independent_reader()?,
        verified.identity().clone(),
    ))
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
    Ok(prepare_condition_parallel_scene_with_options_v3(
        analysis,
        parameters,
        markets,
        market_assets,
        source_manifests,
        path,
        execution_start,
        condition_workers,
        1,
        progress_seconds,
        condition_temporary_root,
    )?
    .cost)
}

#[path = "scene_preparation.rs"]
mod scene_preparation;
pub use scene_preparation::{ScenePreparationCostV3, prepare_condition_parallel_scene_with_options_v3};
