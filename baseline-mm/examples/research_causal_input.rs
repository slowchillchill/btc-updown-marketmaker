//! #185 一次原公共reader遍历的多市场因果诊断及事后标签连接；不运行策略或账户。
use contracts::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sim_system::public_consumption::{PublicConsumptionTimeSourceV1, RecordedPublicGroupSourceV1};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::PathBuf,
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const REMAINING_SECONDS: [i64; 8] = [300, 240, 180, 120, 60, 30, 10, 1];
const WINDOWS_SECONDS: [i64; 5] = [15, 30, 60, 120, 300];

#[derive(Deserialize, Serialize)]
struct Config {
    public_root: PathBuf,
    trial_report_path: PathBuf,
    target_markets: Vec<TargetMarket>,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    purpose: String,
}
#[derive(Clone, Deserialize, Serialize)]
struct TargetMarket {
    condition_id: ConditionId,
    window: MarketWindowV2,
}
#[derive(Deserialize)]
struct Scope {
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
}
#[derive(Deserialize)]
struct Initialization {
    input_positions: BTreeSet<u64>,
}
#[derive(Deserialize)]
struct HiddenBinding {
    prefix: CommittedIngressPrefixV1,
}
#[derive(Deserialize)]
struct TrialBinding {
    scope: Scope,
    initialization: Initialization,
    hidden_preparation: HiddenBinding,
    input_set_identity_sha256: Sha256Hex,
    source_identity_sha256: Sha256Hex,
}
#[derive(Default)]
struct OriginalVisibility(Option<i64>);
impl PublicConsumptionTimeSourceV1 for OriginalVisibility {
    fn execution_time_for_group(
        &mut self,
        _: &PublicGroupReferenceV1,
    ) -> std::result::Result<i64, sim_system::execution::SimRunActorErrorV2> {
        Err(
            sim_system::execution::SimRunActorErrorV2::PublicConsumptionSource(
                "需要原来源可见时刻".into(),
            ),
        )
    }
    fn execution_time_for_visible_group(
        &mut self,
        _: &PublicGroupReferenceV1,
        visible: i64,
    ) -> std::result::Result<i64, sim_system::execution::SimRunActorErrorV2> {
        let at = self.0.map_or(visible, |last| last.max(visible));
        self.0 = Some(at);
        Ok(at)
    }
}
fn usable<T>(fact: &ObservedFactV1<T>) -> bool {
    fact.value.is_some()
        && matches!(
            fact.quality,
            MarketDataQuality::Fresh | MarketDataQuality::Stale
        )
}
// 与当前probability::fact_time的接收时间轴一致，不使用source_event时间代替。
fn received<T>(fact: &ObservedFactV1<T>, fallback: i64) -> i64 {
    fact.direct_anchors
        .iter()
        .map(|a| a.received_at_utc_ms)
        .max()
        .unwrap_or(fallback)
}
fn fact_summary<T>(fact: &ObservedFactV1<T>, at: i64) -> Value {
    json!({"value_present":fact.value.is_some(),"quality":fact.quality,"usable_fresh_or_stale":usable(fact),
        "received_at_utc_ms":received(fact,at),"direct_anchors":fact.direct_anchors})
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SpotUpdate {
    group_position: u64,
    first_visible_at_utc_ms: i64,
    received_at_utc_ms: i64,
    source_ids: Vec<Sha256Hex>,
    new_source_ids: Vec<Sha256Hex>,
    source_event_at_utc_ms: Vec<Option<i64>>,
    price_usd_per_btc: Option<ExactDecimal>,
    quality: MarketDataQuality,
    initialization_source_group: bool,
}
impl SpotUpdate {
    fn price(&self) -> Result<Option<Decimal>> {
        if !matches!(
            self.quality,
            MarketDataQuality::Fresh | MarketDataQuality::Stale
        ) {
            return Ok(None);
        }
        self.price_usd_per_btc
            .as_ref()
            .map(|v| v.0.parse().map_err(Into::into))
            .transpose()
    }
    fn path_time(&self) -> Result<i64> {
        // 无价/失效观察本身没有新的价格接收坐标，缺失从实际可见时刻表达。
        Ok(if self.price()?.is_some() {
            self.received_at_utc_ms
        } else {
            self.first_visible_at_utc_ms
        })
    }
}
#[derive(Default)]
struct History {
    last: Option<ObservedFactV1<ChainlinkBtcUsdReferenceV1>>,
    seen: BTreeSet<Sha256Hex>,
    updates: Vec<SpotUpdate>,
    repeated_observations: u64,
}
impl History {
    fn observe(&mut self, o: &MarketBusinessObservationV1, at: i64, initial: bool, research: bool) {
        if self.last.as_ref() == Some(&o.chainlink_spot) {
            if research {
                self.repeated_observations += 1;
            }
            return;
        }
        let f = &o.chainlink_spot;
        let ids = f
            .direct_anchors
            .iter()
            .map(|a| a.source_message_id.clone())
            .collect::<Vec<_>>();
        let new = ids
            .iter()
            .filter(|id| self.seen.insert((*id).clone()))
            .cloned()
            .collect();
        self.updates.push(SpotUpdate {
            group_position: o.group_position,
            first_visible_at_utc_ms: at,
            received_at_utc_ms: received(f, at),
            source_ids: ids,
            new_source_ids: new,
            source_event_at_utc_ms: f
                .direct_anchors
                .iter()
                .map(|a| a.source_event_at_utc_ms)
                .collect(),
            price_usd_per_btc: f.value.as_ref().map(|p| p.price_usd_per_btc.clone()),
            quality: f.quality,
            initialization_source_group: initial,
        });
        self.last = Some(f.clone());
    }
    fn integral(&self, from: i64, to: i64, cutoff: i64) -> Result<Value> {
        let to = to.min(cutoff).max(from);
        let mut points = Vec::new();
        let mut previous = None;
        let mut regressions = Vec::new();
        for u in self
            .updates
            .iter()
            .filter(|u| u.first_visible_at_utc_ms <= cutoff)
        {
            let t = u.path_time()?;
            if t > cutoff {
                continue;
            }
            if previous.is_some_and(|last| t < last) {
                regressions.push(u);
                continue; // 不重排、不把回退更新插回已冻结的过去。
            }
            previous = Some(t);
            points.push((t, u));
        }
        let mut support = None;
        for &(t, u) in &points {
            if t <= from {
                support = Some(u);
            }
        }
        let left = support;
        let mut cursor = from;
        let mut segments = Vec::new();
        let mut integral = Decimal::ZERO;
        let mut covered = 0i64;
        let mut add = |until: i64, support: Option<&SpotUpdate>| -> Result<()> {
            if until <= cursor {
                return Ok(());
            }
            let price = support.map(SpotUpdate::price).transpose()?.flatten();
            let duration = until - cursor;
            let part = price.map(|p| p * Decimal::from(duration) / Decimal::from(1000));
            if let Some(v) = part {
                integral += v;
                covered += duration;
            }
            segments.push(
                json!({"from_received_axis_utc_ms":cursor,"to_received_axis_utc_ms":until,
                "duration_ms":duration,"price_usd_per_btc":price.map(|p|p.normalize().to_string()),
                "price_times_seconds":part.map(|p|p.normalize().to_string()),"support":support}),
            );
            cursor = until;
            Ok(())
        };
        for &(t, u) in &points {
            if t > from && t <= to {
                add(t, support)?;
                support = Some(u);
            }
        }
        add(to, support)?;
        let missing = to - from - covered;
        let complete = cutoff >= from && missing == 0 && regressions.is_empty();
        Ok(
            json!({"from_utc_ms":from,"to_utc_ms":to,"frozen_visible_cutoff_utc_ms":cutoff,
            "window_has_started":cutoff>=from,"left_support":if cutoff>=from {left}else{None},"segments":segments,"covered_ms":covered,"missing_ms":missing,
            "known_price_times_seconds":if covered>0 {Some(integral.normalize().to_string())}else{None},"complete_hold_approximation":complete,
            "mean_price_if_complete":if complete && to>from {Some((integral*Decimal::from(1000)/Decimal::from(to-from)).normalize().to_string())}else{None},
            "received_time_regressions_not_backfilled":regressions,
            "note":"仅截止前已可见的spot在接收时间轴分段持价；未知段不填零，rolling TWAP不参与；缺价从可见时刻起，接收回退保留但不倒灌，此近似不是官方积分。"}),
        )
    }
    fn window(&self, cutoff: i64, seconds: i64) -> Result<Value> {
        Ok(self.window_with_pairs(cutoff, seconds)?.0)
    }
    fn window_with_pairs(&self, cutoff: i64, seconds: i64) -> Result<(Value, Vec<Value>)> {
        let from = cutoff - seconds * 1000;
        let mut pairs = 0u64;
        let mut zero_time = 0u64;
        let mut backward = 0u64;
        let mut squared = Decimal::ZERO;
        let mut elapsed = 0i64;
        let mut prior: Option<(usize, i64, Decimal)> = None;
        let mut pair_refs = Vec::new();
        for (index, u) in self
            .updates
            .iter()
            .enumerate()
            .filter(|(_, u)| u.first_visible_at_utc_ms <= cutoff)
        {
            let Some(p) = u.price()? else {
                prior = None;
                continue;
            };
            if u.new_source_ids.is_empty() {
                continue;
            }
            let t = u.received_at_utc_ms;
            if t < from || t > cutoff {
                continue;
            }
            if let Some((i0, t0, p0)) = prior {
                let change_squared = (t >= t0).then(|| (p - p0) * (p - p0));
                pair_refs.push(json!({"left_update_index":i0,"right_update_index":index,
                    "left_group_position":self.updates[i0].group_position,"right_group_position":u.group_position,
                    "elapsed_ms":t-t0,"classification":if t<t0 {"backward"}else if t==t0 {"same_time"}else {"positive_time"},
                    "included_in_squared_sum":t>=t0,
                    "squared_price_change":change_squared.map(|v|v.normalize().to_string()),
                    "positive_elapsed_ms":(t-t0).max(0)}));
                if t < t0 {
                    backward += 1;
                } else {
                    pairs += 1;
                    squared += change_squared.unwrap();
                    if t == t0 {
                        zero_time += 1;
                    } else {
                        elapsed += t - t0;
                    }
                }
            }
            prior = Some((index, t, p));
        }
        Ok((
            json!({"window_seconds":seconds,"both_price_endpoints_received_inside_window":true,
            "price_pairs":pairs,"same_received_time_pairs":zero_time,"received_time_backward_pairs":backward,
            "squared_price_change_sum":squared.normalize().to_string(),"positive_pair_elapsed_ms":elapsed,
            "squared_change_per_second_description":if elapsed>0 && zero_time==0 && backward==0 {Some((squared*Decimal::from(1000)/Decimal::from(elapsed)).normalize().to_string())}else{None},
            "hold_coverage":self.integral(from,cutoff,cutoff)?,"note":"同毫秒不同来源保留为零时长价格对；不据此产生sigma或initial_sigma，描述量不是拟合参数。"}),
            pair_refs,
        ))
    }
}
fn reference(o: &MarketBusinessObservationV1) -> Value {
    match &o.effective_reference {
        EffectiveMarketReferenceV1::Missing => json!({"kind":"missing"}),
        EffectiveMarketReferenceV1::TemporaryTwap(v) => {
            json!({"kind":"temporary_twap","value":v.value,"source":v.direct_anchor})
        }
        EffectiveMarketReferenceV1::OfficialAnchor(v) => {
            json!({"kind":"official","value":v.value,"quality":v.quality,"direct_anchors":v.direct_anchors})
        }
    }
}
fn availability(o: &MarketBusinessObservationV1, at: i64) -> Value {
    json!({"spot":fact_summary(&o.chainlink_spot,at),"binance_trade":fact_summary(&o.binance_trade,at),
        "binance_depth":fact_summary(&o.binance_depth,at),"twap_30":fact_summary(&o.chainlink_twap_thirty,at),
        "twap_60":fact_summary(&o.chainlink_twap_sixty,at),"settlement_reference":o.settlement_reference,
        "assets":o.assets.iter().map(|a|json!({"asset":a.asset,"bids":fact_summary(&a.bids,at),"asks":fact_summary(&a.asks,at),
            "tick":a.tick,"valuation_best_bid":a.valuation_best_bid})).collect::<Vec<_>>()})
}
#[derive(Default, Serialize)]
struct CrossSource {
    denominator: u64,
    simultaneously_usable: u64,
    min_received_delta_ms: Option<i64>,
    max_received_delta_ms: Option<i64>,
    min_source_delta_ms: Option<i64>,
    max_source_delta_ms: Option<i64>,
}
impl CrossSource {
    fn observe<T>(
        &mut self,
        spot: &ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
        other: &ObservedFactV1<T>,
        at: i64,
    ) {
        self.denominator += 1;
        if !usable(spot) || !usable(other) || received(spot, at) > at || received(other, at) > at {
            return;
        }
        self.simultaneously_usable += 1;
        let d = received(other, at) - received(spot, at);
        self.min_received_delta_ms = Some(self.min_received_delta_ms.map_or(d, |v| v.min(d)));
        self.max_received_delta_ms = Some(self.max_received_delta_ms.map_or(d, |v| v.max(d)));
        if let (Some(s), Some(o)) = (
            spot.direct_anchors
                .iter()
                .filter_map(|a| a.source_event_at_utc_ms)
                .max(),
            other
                .direct_anchors
                .iter()
                .filter_map(|a| a.source_event_at_utc_ms)
                .max(),
        ) {
            let d = o - s;
            self.min_source_delta_ms = Some(self.min_source_delta_ms.map_or(d, |v| v.min(d)));
            self.max_source_delta_ms = Some(self.max_source_delta_ms.map_or(d, |v| v.max(d)));
        }
    }
}
#[derive(Default)]
struct Analysis {
    history: History,
    latest: Option<MarketBusinessObservationV1>,
    last_visible: Option<i64>,
    reference_last: Option<Value>,
    reference_changes: Vec<Value>,
    width_counts: BTreeMap<usize, u64>,
    market_counts: BTreeMap<String, u64>,
    catalog: BTreeMap<String, Value>,
    field_counts: BTreeMap<String, BTreeMap<String, u64>>,
    source_ids: BTreeMap<String, BTreeSet<Sha256Hex>>,
    initial_groups: u64,
    research_groups: u64,
    initial_observations: u64,
    research_observations: u64,
    target_research_observations: u64,
    target_initialization_observations: u64,
    target_pre_window_observations: u64,
    target_post_window_observations: u64,
    groups_visible_at_or_after_right_boundary: u64,
    cross_trade: CrossSource,
    cross_depth: CrossSource,
}
impl Analysis {
    fn consume(
        &mut self,
        g: &ClosedBusinessGroupV1,
        at: i64,
        initial: bool,
        target: &ConditionId,
        start: i64,
        end: i64,
    ) {
        *self.width_counts.entry(g.observations.len()).or_default() += 1;
        if at >= end {
            self.groups_visible_at_or_after_right_boundary += 1;
        }
        if initial {
            self.initial_groups += 1;
            self.initial_observations += g.observations.len() as u64;
        } else if at >= start && at < end {
            self.research_groups += 1;
            self.research_observations += g.observations.len() as u64;
        }
        for o in &g.observations {
            self.catalog
                .entry(o.market.condition_id.0.clone())
                .or_insert_with(|| json!(o.market));
            if o.market.condition_id == *target {
                if initial {
                    self.target_initialization_observations += 1;
                } else if at < start {
                    self.target_pre_window_observations += 1;
                } else if at >= end {
                    self.target_post_window_observations += 1;
                }
            }
            // 窗后组只计规模；事后标签由独立投影保存，不倒灌冻结特征。
            if at >= end {
                continue;
            }
            if !initial && at >= start && at < end {
                *self
                    .market_counts
                    .entry(o.market.condition_id.0.clone())
                    .or_default() += 1;
            }
            if o.market.condition_id != *target {
                continue;
            }
            self.history
                .observe(o, at, initial, !initial && at >= start);
            self.latest = Some(o.clone());
            self.last_visible = Some(at);
            let r = reference(o);
            if self.reference_last.as_ref() != Some(&r) {
                self.reference_changes.push(json!({"first_visible_at_utc_ms":at,"group_position":g.group_position,"initialization_source_group":initial,"reference":r}));
                self.reference_last = Some(r);
            }
            if initial || at < start || at >= end {
                continue;
            }
            self.target_research_observations += 1;
            self.cross_trade
                .observe(&o.chainlink_spot, &o.binance_trade, at);
            self.cross_depth
                .observe(&o.chainlink_spot, &o.binance_depth, at);
            macro_rules! count {
                ($name:expr,$f:expr) => {{
                    let f = $f;
                    let key = format!(
                        "{}:{:?}",
                        if f.value.is_some() {
                            "present"
                        } else {
                            "absent"
                        },
                        f.quality
                    );
                    *self
                        .field_counts
                        .entry($name.into())
                        .or_default()
                        .entry(key)
                        .or_default() += 1;
                    for a in &f.direct_anchors {
                        self.source_ids
                            .entry($name.into())
                            .or_default()
                            .insert(a.source_message_id.clone());
                    }
                }};
            }
            count!("spot", &o.chainlink_spot);
            count!("binance_trade", &o.binance_trade);
            count!("binance_depth", &o.binance_depth);
            count!("twap_30", &o.chainlink_twap_thirty);
            count!("twap_60", &o.chainlink_twap_sixty);
            for a in &o.assets {
                for (name, f) in [("bids", &a.bids), ("asks", &a.asks)] {
                    count!(&format!("asset/{}/{name}", a.asset.outcome.label), f);
                }
                count!(&format!("asset/{}/tick", a.asset.outcome.label), &a.tick);
            }
        }
    }
    fn snapshot(&self, cutoff: i64, end: i64) -> Result<Value> {
        Ok(
            json!({"frozen_visible_cutoff_utc_ms":cutoff,"remaining_seconds":(end-cutoff)/1000,
            "last_actual_target_observation_visible_at_utc_ms":self.last_visible,
            "reference":self.latest.as_ref().map(reference),"availability":self.latest.as_ref().map(|o|availability(o,cutoff)),
            "terminal_for_target":self.latest.as_ref().map(|o|&o.terminal),
            "settlement_window_hold":self.history.integral(end-60_000,cutoff.min(end).max(end-60_000),cutoff)?,
            "descriptive_windows":WINDOWS_SECONDS.iter().map(|&w|self.history.window(cutoff,w)).collect::<Result<Vec<_>>>()?}),
        )
    }
}
#[derive(Default)]
struct Sampler {
    next: usize,
    frozen: Vec<Value>,
}
impl Sampler {
    fn before(&mut self, next_group_at: i64, analysis: &Analysis, end: i64) -> Result<()> {
        self.before_with(next_group_at, end, |cutoff| analysis.snapshot(cutoff, end))
    }
    fn before_with(
        &mut self,
        next_group_at: i64,
        end: i64,
        mut snapshot: impl FnMut(i64) -> Result<Value>,
    ) -> Result<()> {
        // 严格小于下一组：相同可见毫秒的所有完整组先消费，再冻结。
        while self.next < REMAINING_SECONDS.len()
            && end - REMAINING_SECONDS[self.next] * 1000 < next_group_at
        {
            self.frozen
                .push(snapshot(end - REMAINING_SECONDS[self.next] * 1000)?);
            self.next += 1;
        }
        Ok(())
    }
}
#[derive(Default)]
struct LabelHistory {
    last: Option<ObservedFactV1<MarketResolutionTerminalV3>>,
    changes: Vec<Value>,
    first_usable: Option<Value>,
}
impl LabelHistory {
    fn observe(&mut self, o: &MarketBusinessObservationV1, at: i64) {
        if self.last.as_ref() == Some(&o.terminal) {
            return;
        }
        let event = json!({"condition_id":o.market.condition_id,"group_position":o.group_position,
            "first_visible_at_utc_ms":at,"fact":o.terminal});
        if self.first_usable.is_none() && usable(&o.terminal) {
            self.first_usable = Some(event.clone());
        }
        self.changes.push(event);
        self.last = Some(o.terminal.clone());
    }
}
struct TargetRun {
    target: TargetMarket,
    analysis: Analysis,
    sampler: Sampler,
    labels: LabelHistory,
}
impl TargetRun {
    fn new(target: TargetMarket) -> Self {
        Self {
            target,
            analysis: Analysis::default(),
            sampler: Sampler::default(),
            labels: LabelHistory::default(),
        }
    }
    fn consume(&mut self, g: &ClosedBusinessGroupV1, at: i64, initial: bool) -> Result<()> {
        let start = self.target.window.start_at_utc_ms;
        let end = self.target.window.end_at_utc_ms;
        self.sampler.before(at, &self.analysis, end)?;
        self.analysis
            .consume(g, at, initial, &self.target.condition_id, start, end);
        for o in &g.observations {
            if o.market.condition_id == self.target.condition_id {
                self.labels.observe(o, at);
            }
        }
        Ok(())
    }
    fn finish(mut self) -> Result<Value> {
        let end = self.target.window.end_at_utc_ms;
        self.sampler.before(i64::MAX, &self.analysis, end)?;
        let target = self
            .analysis
            .latest
            .as_ref()
            .ok_or("原材料中没有指定目标市场")?;
        if target.market.window != self.target.window {
            return Err("指定目标市场窗口与实际公共市场不一致".into());
        }
        let a = self.analysis;
        let boundary = a.snapshot(end, end)?;
        let inside_spot_sources = a.source_ids.get("spot").map_or(0, BTreeSet::len);
        Ok(
            json!({"target":self.target,"market_catalog":a.catalog,"research_observations_by_market":a.market_counts,
            "source_width_and_repetition":{"initialization_groups":a.initial_groups,"research_groups":a.research_groups,
                "initialization_observations":a.initial_observations,"research_observations":a.research_observations,
                "groups_visible_at_or_after_right_boundary":a.groups_visible_at_or_after_right_boundary,
                "observations_per_complete_group":a.width_counts,"target_research_observations":a.target_research_observations,
                "target_initialization_observations":a.target_initialization_observations,
                "target_pre_window_observations":a.target_pre_window_observations,"target_post_window_observations":a.target_post_window_observations,
                "distinct_source_ids_by_field_inside_window":a.source_ids.iter().map(|(k,v)|(k,v.len())).collect::<BTreeMap<_,_>>(),
                "distinct_spot_sources_visible_before_right_boundary_including_pre_window":a.history.seen.len(),
                "target_repeated_spot_observations_inside_window":a.history.repeated_observations,
                "target_observations_per_distinct_spot_source_inside_window":if inside_spot_sources==0 {None}else{Some(a.target_research_observations as f64/inside_spot_sources as f64)}},
            "spot_updates_original_visible_order":a.history.updates,"reference_changes_original_visible_order":a.reference_changes,
            "field_quality_counts":a.field_counts,"cross_source_simultaneous_visibility":{"spot_and_trade":a.cross_trade,"spot_and_depth":a.cross_depth,
                "delta_definition":"other minus spot; repeated-state description, not causal lead or fitted basis"},
            "sampling_remaining_seconds":REMAINING_SECONDS,"frozen_sampling":self.sampler.frozen,
            "market_right_boundary_hold_diagnostic":{"not_a_new_public_event_or_terminal_label":true,"cutoff_utc_ms":end,
                "last_actual_visible_at_utc_ms":a.last_visible,"hold_tail_to_boundary_ms":a.last_visible.map(|last|end-last),"diagnostic":boundary},
            "posthoc_label_qualification":{"condition_id":self.target.condition_id,"first_usable_terminal":self.labels.first_usable,
                "latest_terminal_usable":self.labels.last.as_ref().is_some_and(usable),"changes_original_visible_order":self.labels.changes,
                "labels_not_backfilled_into_frozen_features":true,"brier_or_probability_fit_performed":false}}),
        )
    }
}
fn analyze(config: &Config) -> Result<Value> {
    let binding: TrialBinding = serde_json::from_slice(&fs::read(&config.trial_report_path)?)?;
    let actual_prefix: CommittedIngressPrefixV1 = serde_json::from_slice(&fs::read(
        config
            .public_root
            .join(&binding.hidden_preparation.prefix.stable_storage_ref)
            .join("prefix.json"),
    )?)?;
    let source = RecordedPublicGroupSourceV1::open(
        &config.public_root,
        config.read_page_rows,
        config.max_read_batch_bytes,
        OriginalVisibility::default(),
    )?;
    let mut targets = config
        .target_markets
        .iter()
        .cloned()
        .map(TargetRun::new)
        .collect::<Vec<_>>();
    let mut group_counts = BTreeMap::<String, u64>::new();
    let mut observation_counts = BTreeMap::<String, u64>::new();
    let mut widths = BTreeMap::<usize, u64>::new();
    let mut catalog = BTreeMap::<String, Value>::new();
    let feature_end = config
        .target_markets
        .iter()
        .map(|t| t.window.end_at_utc_ms)
        .max();
    for delivered in source {
        let delivered = delivered.map_err(|e| format!("{e:?}"))?;
        let at = delivered.delivered_at_utc_ms;
        let g = &delivered.prepared.group;
        let initial = !g.member_input_positions.is_empty()
            && g.member_input_positions
                .iter()
                .all(|p| binding.initialization.input_positions.contains(p));
        let section = if initial {
            "initialization"
        } else if at < binding.scope.received_start_utc_ms {
            "before_material_start"
        } else if at >= binding.scope.received_end_utc_ms {
            "at_or_after_material_right_boundary"
        } else if feature_end.is_some_and(|end| at >= end) {
            "label_tail"
        } else {
            "feature_range"
        };
        *group_counts.entry(section.into()).or_default() += 1;
        *observation_counts.entry(section.into()).or_default() += g.observations.len() as u64;
        *widths.entry(g.observations.len()).or_default() += 1;
        for o in &g.observations {
            catalog
                .entry(o.market.condition_id.0.clone())
                .or_insert_with(|| json!(o.market));
        }
        for target in &mut targets {
            target.consume(g, at, initial)?;
        }
    }
    Ok(
        json!({"schema":"complementary_pair_causal_labels_diagnostic_v1","effective_config":config,
        "material_scope":{"received_start_utc_ms":binding.scope.received_start_utc_ms,"received_end_utc_ms":binding.scope.received_end_utc_ms},
        "normal_prefix":actual_prefix,"prepared_expected_normal_prefix":binding.hidden_preparation.prefix,
        "input_set_identity_sha256":binding.input_set_identity_sha256,"source_identity_sha256":binding.source_identity_sha256,
        "material_counts":{"groups_by_section":group_counts,"observations_by_section":observation_counts,
            "observations_per_complete_group":widths,"market_catalog":catalog},
        "markets":targets.into_iter().map(TargetRun::finish).collect::<Result<Vec<_>>>()?,
        "method_note":"一次原reader遍历，各市场按自身八截止和右边界冻结；窗前合法历史与窗内分母分列。事后同condition标签质量/来源另存，不回填特征、不计算概率/参数/收益。"}),
    )
}
#[path = "research_causal_input/first_fit_hour.rs"]
mod first_fit_hour;
#[path = "research_causal_input/window_statistics.rs"]
mod window_statistics;

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() == 3 && args[0] == "--retained-windows" {
        return window_statistics::run(&args[1], &args[2]);
    }
    if args.len() == 3 && args[0] == "--first-fit-hour" {
        return first_fit_hour::run(&args[1], &args[2]);
    }
    if args.len() != 2 {
        return Err("用法 research_causal_input <CONFIG_JSON> <REPORT_JSON> 或 --retained-windows <INPUT_JSON> <REPORT_JSON>".into());
    }
    let config: Config = serde_json::from_slice(&fs::read(&args[0])?)?;
    let before = fs::read_to_string("/proc/self/io")?;
    let began = Instant::now();
    let mut report = analyze(&config)?;
    report["measurement"] = json!({"wall_seconds":began.elapsed().as_secs_f64(),"proc_io_before":before,"proc_io_after":fs::read_to_string("/proc/self/io")?,"proc_status_after":fs::read_to_string("/proc/self/status")?});
    fs::write(&args[1], serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../shared-run/tests/common/mod.rs"]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn fixture() -> ClosedBusinessGroupV1 {
        let temp = tempfile::tempdir().unwrap();
        let public = temp.path().join("public");
        common::recorded_memory_complete(&public);
        let groups =
            RecordedPublicGroupSourceV1::open(&public, 1, 1 << 20, OriginalVisibility::default())
                .unwrap();
        groups
            .last()
            .unwrap()
            .unwrap()
            .prepared
            .group
            .as_ref()
            .clone()
    }
    pub(super) fn spot(id: u64, t: i64, price: &str) -> ObservedFactV1<ChainlinkBtcUsdReferenceV1> {
        ObservedFactV1 {
            value: Some(ChainlinkBtcUsdReferenceV1 {
                symbol: "btc/usd".into(),
                price_usd_per_btc: ExactDecimal(price.into()),
                message_timestamp_utc_ms: t - 100,
            }),
            quality: MarketDataQuality::Fresh,
            direct_anchors: vec![DirectObservationAnchorV1 {
                field_path: "chainlink_spot".into(),
                position: PublicFactPositionV1 {
                    input_stream_id: "research-test".into(),
                    closed_at_input_position: id,
                    fact_ordinal: 0,
                },
                group_id: Sha256Hex(format!("{id:064x}")),
                source_message_id: Sha256Hex(format!("{id:064x}")),
                source_event_at_utc_ms: Some(t - 100),
                received_at_utc_ms: t,
            }],
            dependency_frontier: None,
        }
    }
    fn observe(
        h: &mut History,
        o: &mut MarketBusinessObservationV1,
        id: u64,
        t: i64,
        visible: i64,
        p: &str,
    ) {
        o.group_position = id;
        o.chainlink_spot = spot(id, t, p);
        h.observe(o, visible, false, true);
    }
    #[test]
    fn repeated_observations_and_same_time_sources_are_not_collapsed_into_one_innovation() {
        let mut o = fixture().observations.remove(0);
        let mut h = History::default();
        observe(&mut h, &mut o, 1, 1000, 1100, "100");
        h.observe(&o, 1200, false, true);
        observe(&mut h, &mut o, 2, 1000, 1300, "102");
        assert_eq!(h.updates.len(), 2);
        assert_eq!(h.seen.len(), 2);
        assert_eq!(h.repeated_observations, 1);
        let w = h.window(2000, 15).unwrap();
        assert_eq!(w["price_pairs"], 1);
        assert_eq!(w["same_received_time_pairs"], 1);
        assert_eq!(w["squared_price_change_sum"], "4");
        assert!(w["squared_change_per_second_description"].is_null());
        let i = h.integral(1000, 2000, 2000).unwrap();
        assert_eq!(i["known_price_times_seconds"], "102");
        assert_eq!(i["left_support"]["source_ids"][0], format!("{:064x}", 2));
    }
    #[test]
    fn hand_calculated_receive_axis_integral_and_missing_left_price_or_twap() {
        let mut o = fixture().observations.remove(0);
        let mut h = History::default();
        observe(&mut h, &mut o, 1, 500, 600, "100");
        observe(&mut h, &mut o, 2, 2000, 2100, "110");
        let i = h.integral(1000, 4000, 4000).unwrap();
        assert_eq!(i["covered_ms"], 3000);
        assert_eq!(i["known_price_times_seconds"], "320");
        assert_eq!(i["complete_hold_approximation"], true);
        let missing = h.integral(0, 1000, 1000).unwrap();
        assert_eq!(missing["missing_ms"], 500);
        assert_eq!(missing["complete_hold_approximation"], false);
        o.chainlink_spot = ObservedFactV1::default();
        o.chainlink_twap_sixty = ObservedFactV1 {
            value: Some(ChainlinkBtcUsdTwapReferenceV1 {
                symbol: "btc/usd".into(),
                window_s: 60,
                price_usd_per_btc: ExactDecimal("999".into()),
                payload_timestamp_utc_ms: 2500,
                message_timestamp_utc_ms: 2500,
            }),
            quality: MarketDataQuality::Fresh,
            ..Default::default()
        };
        h.observe(&o, 2500, false, true);
        let gap = h.integral(2000, 4000, 4000).unwrap();
        assert_eq!(gap["missing_ms"], 1500);
        assert_eq!(gap["known_price_times_seconds"], "55");
        assert!(gap["mean_price_if_complete"].is_null());
        let only_twap = History::default().integral(0, 1000, 1000).unwrap();
        assert_eq!(only_twap["missing_ms"], 1000);
        assert!(only_twap["known_price_times_seconds"].is_null());
        o.chainlink_spot = spot(3, 3000, "120");
        o.chainlink_spot.quality = MarketDataQuality::ConfirmedGap;
        h.observe(&o, 3000, false, true);
        assert_eq!(h.integral(3000, 4000, 4000).unwrap()["missing_ms"], 1000);
    }
    #[test]
    fn late_prices_and_anchor_do_not_modify_frozen_cutoff_and_same_millisecond_closes_first() {
        let mut g = fixture();
        let mut o = g.observations.remove(0);
        o.market.window = MarketWindowV2 {
            start_at_utc_ms: 0,
            end_at_utc_ms: 300_000,
        };
        o.effective_reference = EffectiveMarketReferenceV1::Missing;
        o.terminal = ObservedFactV1::default();
        o.chainlink_spot = spot(1, 1000, "100");
        g.observations = vec![o];
        let target = g.observations[0].market.condition_id.clone();
        let mut a = Analysis::default();
        let mut s = Sampler::default();
        s.before(1100, &a, 300_000).unwrap();
        a.consume(&g, 1100, false, &target, 0, 300_000);
        s.before(60_000, &a, 300_000).unwrap();
        assert_eq!(s.frozen.len(), 1);
        g.group_position += 1;
        g.observations[0].chainlink_spot = spot(2, 59_000, "110");
        a.consume(&g, 60_000, false, &target, 0, 300_000);
        s.before(60_001, &a, 300_000).unwrap();
        assert_eq!(s.frozen.len(), 2);
        assert_eq!(
            s.frozen[1]["descriptive_windows"][2]["hold_coverage"]["left_support"].is_null(),
            true
        );
        assert_eq!(
            s.frozen[1]["availability"]["spot"]["direct_anchors"][0]["source_message_id"],
            format!("{:064x}", 2)
        );
        let frozen = s.frozen[1].clone();
        g.group_position += 1;
        g.observations[0].chainlink_spot = spot(3, 59_500, "999");
        g.observations[0].effective_reference =
            EffectiveMarketReferenceV1::OfficialAnchor(ObservedFactV1 {
                value: Some(MarketPriceAnchorV1 {
                    price_usd_per_btc: ExactDecimal("999".into()),
                    source: MarketAnchorSourceV1::OfficialCryptoPriceOpenPrice,
                    application: PriceAnchorApplicationV1::Observed,
                }),
                quality: MarketDataQuality::Fresh,
                ..Default::default()
            });
        a.consume(&g, 70_000, false, &target, 0, 300_000);
        assert_eq!(s.frozen[1], frozen);
        assert_eq!(s.frozen[1]["reference"]["kind"], "missing");
        assert_eq!(
            a.history.integral(1000, 60_000, 60_000).unwrap()["known_price_times_seconds"],
            "5910"
        );
    }
    #[test]
    fn wrong_market_terminal_and_initialization_observations_do_not_enter_target_labels_or_denominators()
     {
        let mut g = fixture();
        let other = g.observations[0].clone();
        assert!(usable(&other.terminal));
        let mut target_o = other.clone();
        target_o.market.condition_id = ConditionId(format!("0x{}", "2".repeat(64)));
        target_o.market.clob_market_id = ClobMarketId("3195577".into());
        target_o.market.window = MarketWindowV2 {
            start_at_utc_ms: 0,
            end_at_utc_ms: 300_000,
        };
        target_o.assets[0].asset.token_id = TokenId("200".into());
        target_o.assets[1].asset.token_id = TokenId("300".into());
        target_o.terminal = ObservedFactV1::default();
        target_o.chainlink_spot = spot(1, -1000, "100");
        let target = target_o.market.condition_id.clone();
        g.observations = vec![other, target_o];
        let mut a = Analysis::default();
        a.consume(&g, -500, true, &target, 0, 300_000);
        assert_eq!(a.target_research_observations, 0);
        assert_eq!(a.initial_observations, 2);
        assert!(a.latest.as_ref().unwrap().terminal.value.is_none());
        a.consume(&g, 1000, false, &target, 0, 300_000);
        assert_eq!(a.target_research_observations, 1);
        assert_eq!(a.research_observations, 2);
        assert_eq!(
            a.history.integral(0, 1000, 1000).unwrap()["known_price_times_seconds"],
            "100"
        );
        assert!(a.latest.as_ref().unwrap().terminal.value.is_none());
        let frozen = a.snapshot(300_000, 300_000).unwrap();
        g.observations[1].terminal = g.observations[0].terminal.clone();
        g.observations[1].chainlink_spot = spot(2, 299_000, "999");
        a.consume(&g, 300_000, false, &target, 0, 300_000);
        assert_eq!(a.snapshot(300_000, 300_000).unwrap(), frozen);
        assert!(a.latest.as_ref().unwrap().terminal.value.is_none());
        assert_eq!(a.groups_visible_at_or_after_right_boundary, 1);
    }
    #[test]
    fn two_market_cutoffs_and_posthoc_quality_changes_remain_separate() {
        let mut g = fixture();
        let resolved = g.observations[0].terminal.clone();
        let mut first = g.observations[0].clone();
        first.market.window = MarketWindowV2 {
            start_at_utc_ms: 0,
            end_at_utc_ms: 300_000,
        };
        first.terminal = ObservedFactV1::default();
        first.chainlink_spot = spot(1, 1000, "100");
        let mut second = first.clone();
        second.market.condition_id = ConditionId(format!("0x{}", "3".repeat(64)));
        second.market.clob_market_id = ClobMarketId("3195578".into());
        second.market.window = MarketWindowV2 {
            start_at_utc_ms: 300_000,
            end_at_utc_ms: 600_000,
        };
        for (i, asset) in second.assets.iter_mut().enumerate() {
            asset.asset.token_id = TokenId(format!("{}", 400 + i));
        }
        let mut second_resolved = resolved.clone();
        let MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            payout_vector,
            ..
        } = second_resolved.value.as_mut().unwrap();
        for (entry, asset) in payout_vector.iter_mut().zip(&second.assets) {
            if entry.asset.token_id.0 == *winning_asset_id {
                *winning_asset_id = asset.asset.token_id.0.clone();
            }
            entry.asset = asset.asset.clone();
        }
        let mut a = TargetRun::new(TargetMarket {
            condition_id: first.market.condition_id.clone(),
            window: first.market.window.clone(),
        });
        let mut b = TargetRun::new(TargetMarket {
            condition_id: second.market.condition_id.clone(),
            window: second.market.window.clone(),
        });
        g.observations = vec![first, second];
        a.consume(&g, 1100, false).unwrap();
        b.consume(&g, 1100, false).unwrap();
        // 同一个组在第一市场右端，在第二市场左端；前者不能倒灌，后者可读已见前序。
        g.group_position += 1;
        for o in &mut g.observations {
            o.group_position = g.group_position;
            o.chainlink_spot = spot(2, 299_000, "110");
        }
        a.consume(&g, 300_000, false).unwrap();
        b.consume(&g, 300_000, false).unwrap();
        assert_eq!(a.sampler.frozen.len(), 8);
        assert_eq!(b.sampler.frozen.len(), 0);
        g.group_position += 1;
        g.observations[0].terminal = resolved;
        a.consume(&g, 360_000, false).unwrap();
        b.consume(&g, 360_000, false).unwrap();
        assert_eq!(b.sampler.frozen.len(), 1);
        assert_eq!(b.sampler.frozen[0]["remaining_seconds"], 300);
        assert_eq!(
            b.sampler.frozen[0]["availability"]["spot"]["direct_anchors"][0]["source_message_id"],
            format!("{:064x}", 2)
        );
        assert!(b.labels.first_usable.is_none());
        let frozen = a.sampler.frozen.clone();
        let boundary = a.analysis.snapshot(300_000, 300_000).unwrap();
        g.group_position += 1;
        g.observations[0].terminal.quality = MarketDataQuality::Invalid;
        for o in &mut g.observations {
            o.group_position = g.group_position;
            o.chainlink_spot = spot(3, 399_000, "999");
        }
        a.consume(&g, 400_000, false).unwrap();
        b.consume(&g, 400_000, false).unwrap();
        assert_eq!(a.sampler.frozen, frozen);
        assert_eq!(a.analysis.snapshot(300_000, 300_000).unwrap(), boundary);
        assert_eq!(
            a.labels.first_usable.as_ref().unwrap()["first_visible_at_utc_ms"],
            360_000
        );
        assert_eq!(
            a.labels.last.as_ref().unwrap().quality,
            MarketDataQuality::Invalid
        );
        assert!(a.labels.changes.last().unwrap()["fact"]["value"].is_object());
        g.group_position += 1;
        g.observations[1].terminal = second_resolved;
        a.consume(&g, 650_000, false).unwrap();
        b.consume(&g, 650_000, false).unwrap();
        let a = a.finish().unwrap();
        let b = b.finish().unwrap();
        assert_eq!(
            a["posthoc_label_qualification"]["latest_terminal_usable"],
            false
        );
        assert_eq!(
            b["posthoc_label_qualification"]["latest_terminal_usable"],
            true
        );
        assert_eq!(
            b["posthoc_label_qualification"]["first_usable_terminal"]["condition_id"],
            b["target"]["condition_id"]
        );
        assert_eq!(
            b["source_width_and_repetition"]["target_pre_window_observations"],
            1
        );
        assert_eq!(
            b["source_width_and_repetition"]["target_repeated_spot_observations_inside_window"],
            1
        );
        assert_eq!(b["frozen_sampling"].as_array().unwrap().len(), 8);
        assert!(
            a["frozen_sampling"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["terminal_for_target"]["value"].is_null())
        );
        assert!(b["market_right_boundary_hold_diagnostic"]["diagnostic"]["terminal_for_target"]["value"].is_null());
    }
    #[test]
    fn received_time_regression_is_preserved_and_not_reordered_into_history() {
        let mut o = fixture().observations.remove(0);
        let mut h = History::default();
        observe(&mut h, &mut o, 1, 1000, 1200, "100");
        observe(&mut h, &mut o, 2, 2000, 2200, "110");
        observe(&mut h, &mut o, 3, 1500, 3000, "999");
        let i = h.integral(1000, 4000, 4000).unwrap();
        assert_eq!(
            i["received_time_regressions_not_backfilled"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(i["complete_hold_approximation"], false);
        assert_eq!(i["known_price_times_seconds"], "320");
        assert_eq!(h.updates[2].received_at_utc_ms, 1500);
    }
}
