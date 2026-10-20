//! 连续零账户公共消费；不建立策略、SIM会话或经济账本。
use crate::{DelayScheduleV1, HistoricalPreparationConfigV1, MaterialError, timing::DeliveryTimes};
use contracts::*;
use data_pipeline_core::{public_input::DptIngressV1, public_statistics::*};
use data_pipeline_training::{
    day_producer::*, day_stream::DayIngressV1, historical_initialization::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sim_system::platform_rules::*;
use sim_system::public_consumption::{
    PublicConsumptionTimeSourceV1, original_public_group_visible_at_utc_ms,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicObservationMarketsV1 {
    All,
    ConditionIds { condition_ids: Vec<String> },
    WindowStarts { start_utc_ms: i64, end_utc_ms: i64 },
}
impl PublicObservationMarketsV1 {
    fn includes(&self, m: &MarketIdentityV2) -> bool {
        match self {
            Self::All => true,
            Self::ConditionIds { condition_ids } => condition_ids.contains(&m.condition_id.0),
            Self::WindowStarts {
                start_utc_ms,
                end_utc_ms,
            } => (*start_utc_ms..*end_utc_ms).contains(&m.window.start_at_utc_ms),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicExportRulesV1 {
    pub snapshot_path: std::path::PathBuf,
    pub expected_file_sha256: Sha256Hex,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuousPublicExportConfigV1 {
    pub rules: PublicExportRulesV1,
    pub preparation: HistoricalPreparationConfigV1,
    pub markets: PublicObservationMarketsV1,
    pub statistics: PublicStatisticsConfigV1,
    pub public_delivery_delay_ms: DelayScheduleV1,
    pub observe_after_groups: bool,
    /// 声明逻辑UTC查询；读取前先消费全部应用时间<=t的组。
    pub query_times_utc_ms: Vec<i64>,
    pub export_new_trades: bool,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct ContinuousPublicExportProgressV1 {
    pub stage: &'static str,
    pub scanned_rows: u64,
    pub processed_inputs: u64,
    pub applied_groups: u64,
    pub new_trades: u64,
    pub elapsed_seconds: f64,
    pub last_applied_at_utc_ms: Option<i64>,
    pub last_group: Option<PublicGroupReferenceV1>,
    pub natural_end: bool,
    pub stopped: bool,
}
/// JSONL投影只保存变化；每项ObservedFact的来源锚点使用单项字典引用。
#[derive(Default)]
struct CompactWriter {
    anchors: BTreeMap<String, (String, u64)>,
    next_anchor: u64,
    sources: BTreeMap<SourceStreamScopeV1, SourceStreamProgressV1>,
    previous_sources: Option<SourceProgressSnapshotsV1>,
    markets: BTreeMap<String, BTreeMap<String, Value>>,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
    quotes: BTreeMap<String, BookQuoteProjection>,
}
struct BookQuoteProjection {
    bids: Arc<ObservedFactV1<Vec<PriceLevelV1>>>,
    asks: Arc<ObservedFactV1<Vec<PriceLevelV1>>>,
    best_bid: Option<Option<PriceLevelV1>>,
    best_ask: Option<Option<PriceLevelV1>>,
}
impl CompactWriter {
    fn fact<T: Serialize>(
        &mut self,
        slot: &str,
        f: &ObservedFactV1<T>,
        sink: &mut impl FnMut(Value) -> Result<(), MaterialError>,
    ) -> Result<Value, MaterialError> {
        let mut refs = Vec::with_capacity(f.direct_anchors.len());
        for (index, anchor) in f.direct_anchors.iter().enumerate() {
            let key = serde_json::to_string(anchor)?;
            let slot = format!("{slot}/{index}");
            let id = if let Some((old, id)) = self.anchors.get(&slot).filter(|(old, _)| old == &key)
            {
                let _ = old;
                *id
            } else {
                let id = self.next_anchor;
                self.next_anchor += 1;
                sink(json!({"record_type":"source_anchor","id":id,"anchor":anchor}))?;
                self.anchors.insert(slot, (key, id));
                id
            };
            refs.push(id);
        }
        Ok(
            json!({"value":f.value,"quality":f.quality,"anchor_refs":refs,"dependency_frontier":f.dependency_frontier}),
        )
    }
    fn observe(
        &mut self,
        o: &MarketBusinessObservationV1,
        h: &PublicStatisticsHistoryV1,
        t: i64,
        query: bool,
        frontier: Option<i64>,
        sink: &mut impl FnMut(Value) -> Result<(), MaterialError>,
    ) -> Result<(), MaterialError> {
        if let Some(values) = o.source_progress.shared_values() {
            if let Some(previous) = &self.previous_sources {
                for change in previous.diff(values) {
                    use imbl::ordmap::DiffItem;
                    match change {
                        DiffItem::Add(_, p) | DiffItem::Update { new: (_, p), .. } => {
                            sink(json!({"record_type":"source_change","progress":p}))?
                        }
                        DiffItem::Remove(scope, _) => sink(
                            json!({"record_type":"source_change","scope":scope,"progress":null}),
                        )?,
                    }
                }
            } else {
                for p in values.values() {
                    sink(json!({"record_type":"source_change","progress":p}))?;
                }
            }
            self.previous_sources = Some(values.clone());
        } else {
            for p in o.source_progress.iter() {
                if self.sources.get(&p.scope) != Some(p) {
                    sink(json!({"record_type":"source_change","progress":p}))?;
                    self.sources.insert(p.scope.clone(), p.clone());
                }
            }
        }
        let s = h.read(o, t);
        let mut state: BTreeMap<String, Value> = BTreeMap::new();
        state.insert("market".into(), json!(o.market));
        state.insert(
            "rules".into(),
            json!(
                self.rules
                    .as_ref()
                    .unwrap()
                    .public_rule_context(o.market.clone())
            ),
        );
        macro_rules! fact {($($f:ident),*) => {$(state.insert(stringify!($f).into(),self.fact(&format!("{}/{}",o.market.condition_id.0,stringify!($f)),&o.$f,sink)?);)*};}
        fact!(
            official_anchor,
            settlement_reference,
            lifecycle,
            active,
            closed,
            accepting_orders,
            terminal,
            chainlink_spot,
            chainlink_twap_thirty,
            chainlink_twap_sixty,
            binance_trade
        );
        state.insert("temporary_reference".into(), json!(o.temporary_reference));
        state.insert("effective_reference".into(), json!(o.effective_reference));
        let mut assets = Vec::new();
        for a in &o.assets {
            // 直接BBA/估值买一不改变簿报价；只有正式簿变化才重新选择档位。
            let key = &a.asset.token_id.0;
            if self.quotes.get(key).is_none_or(|old| {
                !Arc::ptr_eq(&old.bids, &a.bids) || !Arc::ptr_eq(&old.asks, &a.asks)
            }) {
                let best_level = |book: &ObservedFactV1<Vec<PriceLevelV1>>, side: BookSide| {
                    book.value.as_ref().map(|levels| {
                        levels
                            .iter()
                            .filter_map(|level| {
                                let price = data_pipeline_core::parse_canonical_decimal(
                                    &level.price_pusd_per_share,
                                    "public export price",
                                )
                                .ok()?;
                                let quantity = data_pipeline_core::parse_canonical_decimal(
                                    &level.quantity_shares,
                                    "public export quantity",
                                )
                                .ok()?;
                                (price > rust_decimal::Decimal::ZERO
                                    && quantity > rust_decimal::Decimal::ZERO)
                                    .then_some((price, level))
                            })
                            .max_by(|(left, _), (right, _)| {
                                if side == BookSide::Bid {
                                    left.cmp(right)
                                } else {
                                    right.cmp(left)
                                }
                            })
                            .map(|(_, level)| level.clone())
                    })
                };
                self.quotes.insert(
                    key.clone(),
                    BookQuoteProjection {
                        bids: a.bids.clone(),
                        asks: a.asks.clone(),
                        best_bid: best_level(&a.bids, BookSide::Bid),
                        best_ask: best_level(&a.asks, BookSide::Ask),
                    },
                );
            }
            let quote = &self.quotes[key];
            let book_best_bid_state = match &quote.best_bid {
                None => "unknown",
                Some(None) => "known_empty",
                Some(Some(_)) => "price",
            };
            let bid_price = quote
                .best_bid
                .as_ref()
                .and_then(|level| level.as_ref())
                .map(|level| level.price_pusd_per_share.clone());
            let bid_quantity = quote
                .best_bid
                .as_ref()
                .and_then(|level| level.as_ref())
                .map(|level| level.quantity_shares.clone());
            let best_ask = quote.best_ask.clone();
            assets.push(json!({"asset":a.asset,"direct_bba":self.fact(&format!("{}/bba",a.asset.token_id.0),&a.best_bid_ask,sink)?,"valuation_best_bid":self.fact(&format!("{}/valuation_bid",a.asset.token_id.0),&a.valuation_best_bid,sink)?,"valuation_best_bid_state":match &a.valuation_best_bid.value {None=>"unknown",Some(None)=>"known_empty",Some(Some(_))=>"price"},"book_best_bid":bid_price,"book_best_bid_state":book_best_bid_state,"book_best_bid_quantity_shares":bid_quantity,"book_best_ask":best_ask,"bid_quality":a.bids.quality,"ask_quality":a.asks.quality,"bid_materialized":a.bids.value.is_some(),"ask_materialized":a.asks.value.is_some(),"tick":self.fact(&format!("{}/tick",a.asset.token_id.0),&a.tick,sink)?}));
        }
        state.insert("assets".into(), json!(assets));
        let statistics = json!({"depth":s.depth,"blb":{"definition_version":s.blb.definition_version,"status":s.blb.status,"unknown_reason":s.blb.unknown_reason,"midpoint_usdt_per_btc":s.blb.midpoint_usdt_per_btc,"official_k_usd_per_btc":s.blb.official_k_usd_per_btc,"distance":s.blb.distance,"side":s.blb.side,"at_anchor":s.blb.at_anchor,"visible_level_count":s.blb.visible_level_count,"visible_quantity_btc":s.blb.visible_quantity_btc,"visible_notional_usdt":s.blb.visible_notional_usdt,"interval_coverage":s.blb.interval_coverage},"momentum":s.momentum,"spot_variance":s.spot_variance,"spot_integrals":s.spot_integrals,"latest_trade_pressure":s.latest_trade_pressure,"basis":s.basis,"pm_quantities":s.pm_quantities,"source_applied_at_utc_ms":s.source_applied_at_utc_ms,"source_received_age_ms":s.source_received_age_ms,"since_market_start_ms":s.since_market_start_ms,"until_market_end_ms":s.until_market_end_ms});
        for (key, value) in statistics.as_object().unwrap() {
            state.insert(format!("statistics/{key}"), value.clone());
        }
        let previous = self
            .markets
            .entry(o.market.condition_id.0.clone())
            .or_default();
        let changes: BTreeMap<_, _> = state
            .iter()
            .filter(|(k, v)| previous.get(*k) != Some(*v))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        sink(
            json!({"record_type":if query {"query"}else{"observation"},"condition_id":o.market.condition_id,"group":h.group,"applied_at_utc_ms":h.applied_at_utc_ms,"observed_at_utc_ms":t,"delivery_frontier_exclusive_utc_ms":frontier,"changes":changes}),
        )?;
        *previous = state;
        Ok(())
    }
}

/// 仅索引已公开市场的固定窗口；随单调应用/查询时间移动，不展开历史市场观察。
#[derive(Default)]
struct ObservationSelection {
    previous: Option<MarketObservationSnapshotsV1>,
    starts: BTreeMap<i64, BTreeSet<String>>,
    ends: BTreeMap<i64, BTreeSet<String>>,
    active: BTreeSet<String>,
}
impl ObservationSelection {
    fn sync(
        &mut self,
        observations: &MarketObservationSetV1,
        selection: &PublicObservationMarketsV1,
    ) {
        let Some((markets, _)) = observations.shared_parts() else {
            return;
        };
        let changes = if let Some(previous) = &self.previous {
            previous
                .diff(markets)
                .filter_map(|change| {
                    use imbl::ordmap::DiffItem;
                    match change {
                        DiffItem::Add(_, m) => Some((None, Some(m.market.clone()))),
                        DiffItem::Remove(_, m) => Some((Some(m.market.clone()), None)),
                        DiffItem::Update {
                            old: (_, old),
                            new: (_, new),
                        } if old.market != new.market => {
                            Some((Some(old.market.clone()), Some(new.market.clone())))
                        }
                        _ => None,
                    }
                })
                .collect::<Vec<_>>()
        } else {
            markets
                .values()
                .map(|m| (None, Some(m.market.clone())))
                .collect()
        };
        for (old, new) in changes {
            if let Some(old) = old {
                let id = &old.condition_id.0;
                self.active.remove(id);
                if let Some(ids) = self.starts.get_mut(&old.window.start_at_utc_ms) {
                    ids.remove(id);
                }
                if let Some(ids) = self.ends.get_mut(&old.window.end_at_utc_ms) {
                    ids.remove(id);
                }
            }
            if let Some(new) = new.filter(|m| selection.includes(m)) {
                self.starts
                    .entry(new.window.start_at_utc_ms)
                    .or_default()
                    .insert(new.condition_id.0.clone());
                self.ends
                    .entry(new.window.end_at_utc_ms)
                    .or_default()
                    .insert(new.condition_id.0);
            }
        }
        self.previous = Some(markets.clone());
    }
    fn read<'a>(
        &mut self,
        observations: &'a MarketObservationSetV1,
        t: i64,
        selection: &PublicObservationMarketsV1,
    ) -> Vec<&'a MarketBusinessObservationV1> {
        if observations.shared_parts().is_none() {
            return observations
                .iter()
                .filter(|o| {
                    selection.includes(&o.market)
                        && o.market.window.start_at_utc_ms <= t
                        && t <= o.market.window.end_at_utc_ms
                })
                .collect();
        }
        while self
            .starts
            .first_key_value()
            .is_some_and(|(at, _)| *at <= t)
        {
            let (_, ids) = self.starts.pop_first().unwrap();
            self.active.extend(ids);
        }
        while self.ends.first_key_value().is_some_and(|(at, _)| *at < t) {
            let (_, ids) = self.ends.pop_first().unwrap();
            for id in ids {
                self.active.remove(&id);
            }
        }
        self.active
            .iter()
            .filter_map(|id| observations.get_market(id))
            .collect()
    }
}

#[derive(Debug)]
struct ExportStopped;
impl std::fmt::Display for ExportStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("continuous public export stopped")
    }
}
impl std::error::Error for ExportStopped {}

pub fn export_continuous_public_v1(
    config: &ContinuousPublicExportConfigV1,
    mut sink: impl FnMut(Value) -> Result<(), MaterialError>,
    progress: impl Fn(&ContinuousPublicExportProgressV1) + Sync,
    should_stop: impl Fn() -> bool + Sync,
) -> Result<ContinuousPublicExportProgressV1, MaterialError> {
    export_with_frames(config, &mut sink, progress, should_stop, true, |_, _, _| {
        Ok(())
    })
}
/// 重用原Parquet/公共组/统计，只交付显式查询帧；不创建账户或保存特征副本。
pub fn visit_continuous_public_frames_v1(
    config: &ContinuousPublicExportConfigV1,
    frame_sink: impl FnMut(
        i64,
        &PublicGroupReferenceV1,
        Vec<shared_run::FrozenMarketReadV1>,
    ) -> Result<(), MaterialError>,
    progress: impl Fn(&ContinuousPublicExportProgressV1) + Sync,
) -> Result<ContinuousPublicExportProgressV1, MaterialError> {
    export_with_frames(config, |_| Ok(()), progress, || false, false, frame_sink)
}
fn export_with_frames(
    config: &ContinuousPublicExportConfigV1,
    mut sink: impl FnMut(Value) -> Result<(), MaterialError>,
    progress: impl Fn(&ContinuousPublicExportProgressV1) + Sync,
    should_stop: impl Fn() -> bool + Sync,
    compact: bool,
    mut frame_sink: impl FnMut(
        i64,
        &PublicGroupReferenceV1,
        Vec<shared_run::FrozenMarketReadV1>,
    ) -> Result<(), MaterialError>,
) -> Result<ContinuousPublicExportProgressV1, MaterialError> {
    let started = Instant::now();
    let mut state = ContinuousPublicExportProgressV1::default();
    let mut report = |stage| {
        state.stage = stage;
        state.elapsed_seconds = started.elapsed().as_secs_f64();
        progress(&state);
    };
    report("admission");
    if compact {
        sink(
            json!({"record_type":"config","schema_version":"continuous-public-export-v1","config":config,"requested_header":config.preparation.effective_header()}),
        )?;
    }
    if should_stop() {
        state.stopped = true;
        return finish(state, &started, &mut sink, &progress);
    }
    let p = &config.preparation;
    let mut preparation_rows = 0;
    let mut notice = |notice| {
        use data_pipeline_training::historical_preparation_progress::HistoricalPreparationNoticeV1 as N;
        match notice {
            N::AdmissionFile { rows, .. } | N::RawBatch { rows, .. } => preparation_rows += rows,
            _ => (),
        }
        let stage = ContinuousPublicExportProgressV1 {
            stage: "admission_or_initialization",
            scanned_rows: preparation_rows,
            elapsed_seconds: started.elapsed().as_secs_f64(),
            ..Default::default()
        };
        progress(&stage);
    };
    let notifier=data_pipeline_training::historical_preparation_progress::HistoricalPreparationNotifierV1::new(&mut notice);
    let inputs = data_pipeline_training::freeze_public_source_input_set_with_progress_v1(
        &p.scope.manifest_paths,
        Some(&notifier),
    )?;
    report("initialization");
    let selection = p
        .metadata_initialization
        .as_ref()
        .map(|initial| {
            if initial.received_start_utc_ms != p.scope.received_start_utc_ms
                || initial.received_end_utc_ms != p.scope.received_end_utc_ms
            {
                return Err::<_, MaterialError>("初始化用途的研究范围与输入范围不一致".into());
            }
            MetadataInitializationSelectionV1::open_with_progress(
                initial,
                p.read_page_rows,
                p.max_read_batch_bytes,
                Some(&notifier),
            )
        })
        .transpose()?;
    let initial = selection
        .as_ref()
        .map(|s| {
            s.read(p.read_page_rows, p.max_read_batch_bytes)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let source_identity = match (&selection, &p.metadata_initialization) {
        (Some(selection), Some(initial)) => {
            selection.source_identity(inputs.input_set_identity_sha256(), initial)?
        }
        _ => Sha256Hex(inputs.input_set_identity_sha256().into()),
    };
    let causal_start = initial
        .iter()
        .map(|(row, _)| {
            let e =
                data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(row)?;
            Ok::<_, MaterialError>(
                e.collector_admission
                    .as_ref()
                    .map_or(e.received_at_utc_ms, |a| {
                        a.admitted_at_utc_ms.max(e.received_at_utc_ms)
                    }),
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .chain([p.scope.received_start_utc_ms])
        .min()
        .unwrap();
    let admitted = inputs.admitted_daily_inputs();
    let files = admitted.iter().flat_map(|d| d.files()).collect::<Vec<_>>();
    let scanned = AtomicU64::new(0);
    report("order_location");
    let control = |rows| {
        let n = scanned.fetch_add(rows, Ordering::Relaxed) + rows;
        progress(&ContinuousPublicExportProgressV1 {
            stage: "order_location",
            scanned_rows: n,
            elapsed_seconds: started.elapsed().as_secs_f64(),
            ..Default::default()
        });
        if should_stop() {
            Err::<(), MaterialError>(ExportStopped.into())
        } else {
            Ok(())
        }
    };
    let ingress = DayIngressV1::open_with_control(
        &files,
        p.header.input_stream_id.clone(),
        p.read_page_rows,
        p.max_read_batch_bytes,
        p.scope.received_start_utc_ms..p.scope.received_end_utc_ms,
        initial,
        p.canonical.max_epoch_count_per_source,
        Some(&control),
    );
    state.scanned_rows = scanned.load(Ordering::Relaxed);
    let ingress = match ingress {
        Ok(v) => v,
        Err(e) if e.is::<ExportStopped>() => {
            state.stopped = true;
            return finish(state, &started, &mut sink, &progress);
        }
        Err(e) => return Err(e),
    };
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: config.rules.snapshot_path.clone(),
        expected_file_sha256: config.rules.expected_file_sha256.clone(),
        replay_causal_start_utc_ms: causal_start,
    })?;
    let mut header = p.effective_header();
    header.input_scope = format!(
        "{};received=[{},{});source={}",
        header.input_scope,
        p.scope.received_start_utc_ms,
        p.scope.received_end_utc_ms,
        source_identity.0
    );
    let identity = PublicStatisticsIdentityV1 {
        input_set_identity: source_identity.clone(),
        stream_header: header.clone(),
        rules_identity: rules.file_sha256().clone(),
        delivery_policy: format!(
            "visible-plus-delay/monotonic-max/v1:{}",
            serde_json::to_string(&config.public_delivery_delay_ms)?
        ),
    };
    if compact {
        sink(
            json!({"record_type":"identity","replay_causal_start_utc_ms":causal_start,"statistics_identity":identity,"statistics_config":config.statistics,"definition_version":PUBLIC_STATISTICS_VERSION_V1,"initialization_files":selection.as_ref().map(|s|&s.files)}),
        )?;
    }
    let stream = PublicStatisticsStreamV1::new(identity, config.statistics.clone())?;
    let mut producer = DayProducerV1::new_public_only(ingress, header, source_identity)?;
    producer
        .core
        .set_observation_requirements(PublicObservationRequirementsV1::FULL);
    producer.core.enable_source_progress();
    let mut times = DeliveryTimes {
        delays: config.public_delivery_delay_ms.clone(),
        last: None,
    };
    let mut writer = CompactWriter {
        rules: Some(rules),
        ..Default::default()
    };
    let mut selected = ObservationSelection::default();
    let mut queries = config.query_times_utc_ms.clone();
    queries.sort();
    queries.dedup();
    let mut query_index = 0;
    let mut current: Option<(Arc<PublicStatisticsHistoryV1>, Arc<ClosedBusinessGroupV1>)> = None;
    state.stage = "public_application";
    while !should_stop() {
        let Some(a) = producer.next_applied()? else {
            state.natural_end = true;
            break;
        };
        state.processed_inputs += 1;
        for g in &a.applied.closed_groups {
            let reference = PublicGroupReferenceV1 {
                input_stream_id: a.applied.applied_prefix.input_stream_id.clone(),
                applied_prefix: a.applied.applied_prefix.clone(),
                group_position: g.group_position,
                group_id: g.group_id.clone(),
                closed_at_input_position: g.closed_at_input_position,
            };
            let visible = original_public_group_visible_at_utc_ms(&a.input, g);
            let at = times.execution_time_for_visible_group(&reference, visible)?;
            while query_index < queries.len() && queries[query_index] < at {
                emit_query(
                    queries[query_index],
                    Some(at.min(p.scope.received_end_utc_ms)),
                    &current,
                    &config.markets,
                    &mut selected,
                    &mut writer,
                    &mut sink,
                    compact,
                    &mut frame_sink,
                )?;
                query_index += 1;
            }
            let history =
                PreparedPublicStatisticsV1::default().consume(&stream, reference.clone(), at, g)?;
            state.applied_groups += 1;
            state.last_applied_at_utc_ms = Some(at);
            state.last_group = Some(reference.clone());
            selected.sync(&g.observations, &config.markets);
            if let Some(h) = history {
                if compact && config.observe_after_groups {
                    for o in selected.read(&g.observations, at, &config.markets) {
                        writer.observe(&o, &h, at, false, None, &mut sink)?;
                    }
                }
                current = Some((h, g.clone()));
            }
            for trade in a
                .new_public_trades
                .iter()
                .filter(|trade| g.records.iter().any(|f| f.position == trade.position))
            {
                state.new_trades += 1;
                if compact
                    && config.export_new_trades
                    && g.observations
                        .get_market(&trade.market.condition_id.0)
                        .is_some_and(|o| config.markets.includes(&o.market))
                {
                    sink(
                        json!({"record_type":"trade","group":reference,"applied_at_utc_ms":at,"accepted":trade}),
                    )?;
                }
            }
            if compact {
                sink(
                    json!({"record_type":"group_applied","group":reference,"applied_at_utc_ms":at,"source_visible_lower_bound_utc_ms":visible,"delivery_frontier_exclusive_utc_ms":at.min(p.scope.received_end_utc_ms)}),
                )?;
            }
        }
        state.elapsed_seconds = started.elapsed().as_secs_f64();
        progress(&state);
        if matches!(a.input.ingress, DptIngressV1::EndOfInput) {
            state.natural_end = true;
            break;
        }
    }
    state.stopped = !state.natural_end;
    // EOF证明声明输入已耗尽，不宣称范围之外完整。范围内延迟交付组已全部应用。
    let frontier = state.natural_end.then_some(p.scope.received_end_utc_ms);
    if state.natural_end {
        while query_index < queries.len() {
            emit_query(
                queries[query_index],
                frontier,
                &current,
                &config.markets,
                &mut selected,
                &mut writer,
                &mut sink,
                compact,
                &mut frame_sink,
            )?;
            query_index += 1;
        }
    }
    finish(state, &started, &mut sink, &progress)
}
fn emit_query(
    t: i64,
    frontier: Option<i64>,
    current: &Option<(Arc<PublicStatisticsHistoryV1>, Arc<ClosedBusinessGroupV1>)>,
    markets: &PublicObservationMarketsV1,
    selected: &mut ObservationSelection,
    writer: &mut CompactWriter,
    sink: &mut impl FnMut(Value) -> Result<(), MaterialError>,
    compact: bool,
    frame_sink: &mut impl FnMut(
        i64,
        &PublicGroupReferenceV1,
        Vec<shared_run::FrozenMarketReadV1>,
    ) -> Result<(), MaterialError>,
) -> Result<(), MaterialError> {
    let mut observed_markets = Vec::new();
    if let Some((h, g)) = current {
        let mut frames = Vec::new();
        let visible = if compact {
            selected.read(&g.observations, t, markets)
        } else {
            g.observations
                .iter()
                .filter(|o| markets.includes(&o.market))
                .collect()
        };
        for o in visible {
            if compact {
                observed_markets.push(o.market.condition_id.clone());
                writer.observe(o, h, t, true, frontier, sink)?;
            } else {
                frames.push(shared_run::FrozenMarketReadV1 {
                    statistics: Some(h.read(o, t)),
                    rule_context: writer
                        .rules
                        .as_ref()
                        .unwrap()
                        .public_rule_context(o.market.clone())
                        .into(),
                    observation: o.clone(),
                });
            }
        }
        if !compact {
            frame_sink(t, &h.group, frames)?;
        }
    }
    if compact {
        sink(
            json!({"record_type":"query_boundary","observed_at_utc_ms":t,"delivery_frontier_exclusive_utc_ms":frontier,"input_frontier_past_query":frontier.is_some_and(|f|f>t),"has_applied_prefix":current.is_some(),"observed_markets":observed_markets}),
        )?;
    }
    Ok(())
}
fn finish(
    mut state: ContinuousPublicExportProgressV1,
    started: &Instant,
    sink: &mut impl FnMut(Value) -> Result<(), MaterialError>,
    progress: &impl Fn(&ContinuousPublicExportProgressV1),
) -> Result<ContinuousPublicExportProgressV1, MaterialError> {
    state.stage = if state.stopped {
        "stopped"
    } else {
        "natural_end"
    };
    state.elapsed_seconds = started.elapsed().as_secs_f64();
    sink(
        json!({"record_type":"completion","progress":state,"total_inputs":null,"remaining_seconds":null}),
    )?;
    progress(&state);
    Ok(state)
}
#[cfg(test)]
#[path = "continuous_export_tests.rs"]
mod tests;
