mod account;
use account::DecisionReads;
mod construction;
mod health;
pub use health::{SourceHealthFactsV1, source_health_failure_v1, source_health_state_v1};
mod mechanics;
mod quoting;
mod repair;
mod types;
pub use types::*;
mod config;
use super::common::SignalMemory;
use super::common::probability;
pub use super::common::{
    SpotSampleAcceptanceV1, SpotVariancePairV1, SpotVarianceStatisticsV1, direct_received_time_v1,
    eligible_chainlink_spot_v1, spot_price_number_v1, spot_sample_acceptance_v1,
    spot_variance_pair_v1, spot_variance_statistics_v1,
};
mod public_entry;
pub use super::common::PublicProbabilityInputsV4;
use super::common::public_probability;
pub use super::common::{EstimateUnavailableV1, ProbabilityEstimateV1};
pub use config::*;
pub use public_entry::*;

use ::execution_normalization::MemoryIdentityKeyV1;
use ::execution_normalization::SourceNeutralEntityKeyV1;
use contracts::{economics::EconomicRational as R, *};
use ledger::*;
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use shared_run::*;
use sim_system::{
    execution::{RequestContextV2, RequestEffectV1},
    platform_rules::SealedProcessRuleSnapshotV3,
};
use std::{collections::BTreeMap, sync::Arc};

pub const STRATEGY_ID: &str = "complementary_pair";
pub const RULE_VERSION: &str = "candidate_v3";
pub const PUBLIC_RULE_VERSION: &str = "candidate_v4_public_c0_c1";
pub const LOW_PRICE_RULE_VERSION: &str = "candidate_v5_public_low_price";

pub use super::common::StrategyErrorV1;
use super::common::limit_leg;
use super::common::{
    decimal, exact, fd, float, floor_decimal, fraction_units, known, r, rational, value,
};
fn platform_id<C>(o: &ExecutionOrderRead<'_, C>) -> String {
    match &o.order_identity.key {
        MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) => {
            source_order_id.clone()
        }
        _ => unreachable!("公共订单身份"),
    }
}

use super::common::RemainingRepairEstimate;

/// 只保存信号、高水位、终止与未决增量占用；库存/费用/订单每轮重新读取公共事实。
pub struct ComplementaryPairV1 {
    config: ComplementaryPairConfigV3,
    public_entry_veto: Option<bool>,
    initial_pair_low_price_veto: Option<bool>,
    entry_market_range: Option<EntryMarketRangeV1>,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    namespace: String,
    next_request: u64,
    continuity: StrategyContinuityV2,
    signals: BTreeMap<String, SignalMemory>,
    profit_pending: BTreeMap<String, ProfitAllocationV2>,
    // 仅研究编码缓存，不参与决策或接续状态；最多保存上一份真实诊断。
    research_diagnostic_cache: std::cell::RefCell<Option<(ComplementaryPairDiagnosticV1, u64)>>,
}

#[derive(Clone)]
struct PendingLeg {
    token: TokenId,
    quantity: R,
    cost: R,
    price: Decimal,
    order_id: Option<String>,
    unknown: bool,
}
struct MarketState {
    pending: Vec<PendingLeg>,
    orphan: [Decimal; 2],
    orphan_principal: R,
    orphan_fee: R,
    investment: R,
    exposure: [R; 2],
    unknown: bool,
}
struct PairObservation<'a> {
    original: &'a MarketBusinessObservationV1,
    assets: [&'a AssetBusinessObservationV1; 2],
    // 只在一次计算内复用；没有跨观察或账户前沿的状态。
    top_asks: [std::cell::OnceCell<Result<Option<Decimal>, &'static str>>; 2],
}
impl PairObservation<'_> {
    fn top_ask(&self, side: usize) -> Result<Option<Decimal>, StrategyErrorV1> {
        self.top_asks[side]
            .get_or_init(|| {
                value(&self.assets[side].asks)
                    .map(|levels| {
                        levels
                            .iter()
                            .try_fold(None, |best: Option<Decimal>, level| {
                                let price = Decimal::from_str_exact(&level.price_pusd_per_share.0)
                                    .map_err(|_| "非规范公共金额")?;
                                Ok(best.map_or(Some(price), |p| Some(p.min(price))))
                            })
                    })
                    .unwrap_or(Ok(None))
            })
            .as_ref()
            .copied()
            .map_err(|message| StrategyErrorV1::Arithmetic(message))
    }
}
impl std::ops::Deref for PairObservation<'_> {
    type Target = MarketBusinessObservationV1;
    fn deref(&self) -> &Self::Target {
        self.original
    }
}
struct PairMarketRead<'a> {
    observation: PairObservation<'a>,
    rule_context: &'a PublicTradingRuleContextV1,
    statistics: Option<&'a PublicMarketStatisticsV1>,
}
impl<'a> PairMarketRead<'a> {
    fn from_read(f: &'a impl StrategyMarketReadV1) -> Result<Self, StrategyErrorV1> {
        let o = f.observation();
        let up = o
            .assets
            .iter()
            .find(|a| a.asset.outcome.label.eq_ignore_ascii_case("Up"));
        let down = o
            .assets
            .iter()
            .find(|a| a.asset.outcome.label.eq_ignore_ascii_case("Down"));
        let (Some(up), Some(down)) = (up, down) else {
            return Err(StrategyErrorV1::Arithmetic("策略要求Up/Down互补资产身份"));
        };
        if o.assets.len() != 2 {
            return Err(StrategyErrorV1::Arithmetic("策略要求Up/Down互补资产身份"));
        }
        Ok(Self {
            observation: PairObservation {
                original: o,
                assets: [up, down],
                top_asks: Default::default(),
            },
            rule_context: f.rule_context(),
            statistics: f.statistics(),
        })
    }
}

impl StrategyV1 for ComplementaryPairV1 {
    type Error = StrategyErrorV1;
    type Diagnostics = ComplementaryPairDiagnosticV1;
    fn research_diagnostics(&self, diagnostics: &Self::Diagnostics) -> serde_json::Value {
        serde_json::to_value(diagnostics).expect("真实策略诊断可序列化")
    }
    fn research_evaluated_markets(&self, diagnostics: &Self::Diagnostics) -> Vec<ConditionId> {
        diagnostics
            .markets
            .iter()
            .map(|m| m.condition_id.clone())
            .collect()
    }
    fn research_compact_diagnostics(&self, diagnostics: &Self::Diagnostics) -> serde_json::Value {
        let mut stable = diagnostics.clone();
        let dynamic = diagnostics
            .markets
            .iter()
            .map(|m| {
                serde_json::json!({
                    "condition_id":m.condition_id,"probability":m.probability,"urgency":m.urgency
                })
            })
            .collect::<Vec<_>>();
        for m in &mut stable.markets {
            m.probability =
                ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::MissingReference);
            m.urgency = UrgencyStateV1 {
                trading_remaining_ms: 0,
                window_remaining_ms: 0,
                settlement_window_start_utc_ms: None,
                standardized_distance: None,
                uncertainty_usd: None,
                information_age_ms: None,
                binance_innovation_usd: None,
                orphan_cost_pusd: r(Decimal::ZERO).public(),
                free_cash_pusd: ExactDecimal("0".into()),
                unresolved_commitment_count: 0,
                repair_ask_depth_shares: None,
                maker_path_ms: 0,
                active_path_ms: 0,
            };
        }
        let mut cache = self.research_diagnostic_cache.borrow_mut();
        let changed = cache.as_ref().is_none_or(|(old, _)| old != &stable);
        let version = cache.as_ref().map_or(0, |(_, v)| *v + u64::from(changed));
        let definition = changed.then(|| {
            let mut value = serde_json::to_value(&stable).expect("真实诊断可序列化");
            for market in value["markets"].as_array_mut().unwrap() {
                market.as_object_mut().unwrap().remove("probability");
                market.as_object_mut().unwrap().remove("urgency");
            }
            value
        });
        if changed {
            *cache = Some((stable, version));
        }
        serde_json::json!({"format":"stable-diagnostics-v1","version":version,
            "definition":definition,"dynamic":dynamic})
    }
    fn research_request_markets(&self, diagnostics: &Self::Diagnostics) -> Vec<ConditionId> {
        diagnostics
            .markets
            .iter()
            .filter(|d| d.choice == MarketChoiceV1::NewPair)
            .map(|d| d.condition_id.clone())
            .collect()
    }
    fn requires_trade_economics_details(&self) -> bool {
        false
    }
    fn observation_requirements(&self) -> contracts::PublicObservationRequirementsV1 {
        contracts::PublicObservationRequirementsV1 {
            binance_depth_quantities: self.config.probability.binance_pressure_weight_usd != 0.0
                || self.config.probability.binance_trade_pressure_weight_usd != 0.0,
        }
    }
    fn uses_source_health(&self) -> bool {
        true
    }
    fn public_health_schedule(&self) -> Option<Box<dyn StrategyPublicHealthScheduleV1>> {
        Some(Box::new(super::common::PublicHealthSchedule {
            health: self.config.health.clone(),
            probability: self.config.probability.clone(),
        }))
    }
    fn next_public_health_evaluation_at(
        &self,
        input: &[&MarketBusinessObservationV1],
        calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        self.public_health_deadline(input, calculation, at)
    }
    fn next_health_evaluation_at(
        &self,
        input: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        self.health_deadline(input, calculation, at)
    }
    fn qualify_intent_public(
        &self,
        group: &ClosedBusinessGroupV1,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        Ok(self.qualify_public_group(group, calculation, index, at))
    }
    fn qualify_intent(
        &mut self,
        input: &StrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        Ok(self.qualify_health(input, calculation, index, at))
    }
    fn invalidated(
        &mut self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        result: &InvalidatedBatchV1,
    ) {
        // 只登记真实已发前缀；尚未尝试的利润支持不形成策略占用。
        self.record_profit_allocations(calculation, &result.sent, None);
    }
    fn uses_borrowed_input(&self) -> bool {
        true
    }
    fn selects_relevant_markets(&self) -> bool {
        true
    }
    fn historical_market_requirements(&self) -> Vec<ConditionId> {
        let mut ids: std::collections::BTreeSet<_> = self
            .profit_pending
            .values()
            .map(|a| a.condition_id.0.clone())
            .collect();
        ids.extend(self.continuity.repair_market_ends.keys().cloned());
        ids.into_iter().map(ConditionId).collect()
    }
    fn calculation_market_requirements(
        &self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
    ) -> Vec<ConditionId> {
        match &calculation.output {
            StrategyOutputV1::Continue { diagnostics, .. } => diagnostics
                .action_health_dependencies
                .iter()
                .map(|d| d.condition_id.clone())
                .collect(),
            _ => Vec::new(),
        }
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        now: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        self.decide_input(input, now)
    }
    fn decide_borrowed(
        &mut self,
        input: &BorrowedStrategyInputV1<'_>,
        now: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        self.decide_input(input, now)
    }
    fn qualify_intent_borrowed(
        &mut self,
        input: &BorrowedStrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        Ok(self.qualify_health(input, calculation, index, at))
    }
    fn dispatched(
        &mut self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        let (sent, uncertain) = match result {
            Ok(receipts) => (receipts.as_slice(), None),
            Err(e) => (
                e.sent.as_slice(),
                (e.failed_request_effect == Some(RequestEffectV1::MayHaveRequestEffect))
                    .then_some(e.attempted_intent_index)
                    .flatten(),
            ),
        };
        self.record_profit_allocations(calculation, sent, uncertain);
    }
}

impl ComplementaryPairV1 {
    fn record_profit_allocations(
        &mut self,
        calculation: &StrategyCalculationV1<ComplementaryPairDiagnosticV1>,
        sent: &[SentIntentReceiptV1],
        uncertain: Option<usize>,
    ) {
        let StrategyOutputV1::Continue {
            diagnostics,
            intents,
            ..
        } = &calculation.output
        else {
            return;
        };
        for allocation in &diagnostics.profit_allocations {
            if sent
                .iter()
                .any(|receipt| receipt.context.global_request_id == allocation.global_request_id)
                || uncertain
                    .and_then(|i| intents.get(i))
                    .is_some_and(|i| i.context.global_request_id == allocation.global_request_id)
            {
                self.profit_pending
                    .insert(allocation.global_request_id.clone(), allocation.clone());
            }
        }
    }
}

impl ComplementaryPairV1 {
    fn decide_input(
        &mut self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
        now: i64,
    ) -> Result<StrategyOutputV1<ComplementaryPairDiagnosticV1>, StrategyErrorV1> {
        let (pnl, equity) = Self::profit_and_equity(input)?;
        let reads = DecisionReads::new(&input.action_before);
        let occupied = self.reconcile_profit(input)?;
        let mut profit = pnl.clone().max(R::zero()).sub(&occupied).max(R::zero());
        let mut high =
            rational(&self.continuity.confirmed_high_water_pusd)?.max(r(Decimal::from(2000)));
        if let ExecutionReadValueV1::Known(e) = &equity {
            let e = rational(e)?;
            high = high.max(e.clone());
            if e <= high.mul(&r(Decimal::new(7, 1))) {
                self.continuity.terminating = true;
            }
        }
        self.continuity.confirmed_high_water_pusd = high.public();
        // 公共观察按token排列，经济池按登记资产排列；业务Up/Down必须按身份连接。
        let historical: std::collections::BTreeSet<_> = self
            .historical_market_requirements()
            .into_iter()
            .map(|id| id.0)
            .collect();
        let frames = input
            .markets
            .iter()
            .filter(|f| {
                let o = f.observation();
                let id = &o.market.condition_id;
                now < o.market.window.end_at_utc_ms
                    || historical.contains(&id.0)
                    || reads.commitments.contains_key(id.0.as_str())
                    || reads.orders_by_market.contains_key(id.0.as_str())
                    || input.action_before.market_funds(id).has_obligation()
                    || input.action_before.market_economics(id).is_some_and(|m| {
                        m.unresolved && m.orphan_shares.iter().any(|q| *q > Decimal::ZERO)
                    })
            })
            .map(PairMarketRead::from_read)
            .collect::<Result<Vec<_>, _>>()?;
        let states = frames
            .iter()
            .map(|f| self.state(input, f, &reads))
            .collect::<Result<Vec<_>, _>>()?;
        let mut account_exposure = states.iter().fold(R::zero(), |a, s| {
            a.add(&s.exposure[0].clone().max(s.exposure[1].clone()))
        });
        let mut risk_exists = false;
        for (f, s) in frames.iter().zip(&states) {
            let has =
                s.orphan.iter().any(|q| *q > Decimal::ZERO) || !s.pending.is_empty() || s.unknown;
            risk_exists |= has;
            if self.continuity.terminating && has {
                self.continuity
                    .repair_market_ends
                    .entry(f.observation.market.condition_id.0.clone())
                    .or_insert(f.observation.market.window.end_at_utc_ms);
            }
        }
        let mut cash = input.action_before.cash_available().max(Decimal::ZERO);
        let mut diagnostic = ComplementaryPairDiagnosticV1 {
            strategy_id: STRATEGY_ID.into(),
            rule_version: self.continuity.rule_version.clone(),
            risk_equity_pusd: equity.clone(),
            high_water_pusd: high.public(),
            terminating: self.continuity.terminating,
            qualified_net_pnl_pusd: pnl.public(),
            available_profit_pusd: profit.public(),
            markets: vec![],
            profit_allocations: vec![],
            action_health_dependencies: vec![],
        };
        if self.continuity.terminating
            && ((!risk_exists && input.action_before.pending_noe_record_count() == 0)
                || (!self.continuity.repair_market_ends.is_empty()
                    && self
                        .continuity
                        .repair_market_ends
                        .values()
                        .all(|end| *end <= now)))
        {
            return Ok(StrategyOutputV1::Exit {
                diagnostics: diagnostic,
            });
        }
        let mut intents = Vec::new();
        let mut next = None;
        let spot_scope = SourceStreamScopeV1 {
            source: SourceStreamKindV1::ChainlinkSpot,
            condition_id: None,
            asset_or_symbol: "btc/usd".into(),
        };
        for (f, s) in frames.iter().zip(&states) {
            let o = &f.observation;
            let probability_sources = self.probability_sources(o);
            let mut pair_sources = probability_sources.clone();
            pair_sources.extend(
                o.assets
                    .iter()
                    .map(|a| Self::book_scope(o, &a.asset.token_id)),
            );
            let source_health_states = self.health_states(o, &pair_sources, now);
            let healthy = |sources: &[SourceStreamScopeV1]| {
                source_health_states
                    .iter()
                    .filter(|s| sources.contains(&s.scope))
                    .all(|s| s.state.failure().is_none())
            };
            let probability_healthy = healthy(&probability_sources);
            let source_health = source_health_states
                .iter()
                .filter_map(|s| {
                    s.state.failure().map(|failure| SourceHealthIssueV1 {
                        scope: s.scope.clone(),
                        failure,
                    })
                })
                .collect();
            let spot_healthy = self
                .source_health_state(o, &spot_scope, now)
                .failure()
                .is_none();
            let (p, public_inputs) = if self.public_entry_veto.is_some() {
                if probability_healthy {
                    let (p, evidence) =
                        public_probability::estimate(o, f.statistics, &self.config.probability)?;
                    (p, Some(evidence))
                } else {
                    (
                        ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::UnhealthySource),
                        None,
                    )
                }
            } else {
                let price = if probability_healthy || spot_healthy {
                    self.signals
                        .entry(o.market.condition_id.0.clone())
                        .or_default()
                        .maintain_history(o, now, &self.config.probability, spot_healthy)?
                } else {
                    None
                };
                let p = if probability_healthy {
                    self.signals
                        .get_mut(&o.market.condition_id.0)
                        .expect("合格概率已维护信号记忆")
                        .estimate(o, now, &self.config.probability, price)?
                } else {
                    ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::UnhealthySource)
                };
                (p, None)
            };
            let missing = s
                .orphan
                .iter()
                .position(|q| *q > Decimal::ZERO)
                .map(|held| 1 - held);
            let urgency = self.urgency(o, now, &p, s, cash, missing)?;
            let mut d = MarketDecisionDiagnosticV1 {
                condition_id: o.market.condition_id.clone(),
                pair_entry: PairEntryEvaluationV1 {
                    price_reference: Some(self.config.quote.initial_pair_price_reference),
                    market_eligible: self
                        .entry_market_range
                        .is_none_or(|r| r.contains(o.market.window.start_at_utc_ms)),
                    ..Default::default()
                },
                probability: p.clone(),
                public_inputs,
                urgency,
                choice: MarketChoiceV1::Wait,
                unavailable: vec![],
                comparison: None,
                maximum_repair_price: None,
                source_health,
                source_health_states: source_health_states.clone(),
            };
            let trading = now >= o.market.window.start_at_utc_ms
                && now < o.market.window.end_at_utc_ms
                && value(&o.accepting_orders) == Some(&true)
                && value(&o.closed) != Some(&true);
            let initial = intents.len();
            let mut repair_sources = o
                .assets
                .iter()
                .map(|a| Self::book_scope(o, &a.asset.token_id))
                .collect::<Vec<_>>();
            if p.up().is_some() {
                repair_sources.extend(probability_sources);
            }
            let repair_healthy = healthy(&repair_sources);
            if trading && repair_healthy {
                self.repair(
                    input,
                    f,
                    now,
                    s,
                    &p,
                    &mut cash,
                    &mut account_exposure,
                    &mut profit,
                    &mut intents,
                    &mut d,
                    &mut diagnostic.profit_allocations,
                )?;
            }
            let targets = self.initial_pair_targets(o, &p, s)?;
            d.pair_entry.record_targets(&targets);
            if trading {
                // 单次目标结构优先维护/修复；保留孤腿时仍可在容量与经营条件下新配对。
                if !self.continuity.terminating
                    && d.source_health.is_empty()
                    && matches!(equity, ExecutionReadValueV1::Known(_))
                    && intents.len() == initial
                    && !matches!(
                        d.choice,
                        MarketChoiceV1::Maintain | MarketChoiceV1::CancelBeforeActiveRepair
                    )
                {
                    self.new_pair(
                        input,
                        f,
                        now,
                        s,
                        &p,
                        targets,
                        &mut cash,
                        &mut account_exposure,
                        &mut intents,
                        &mut d,
                    )?;
                }
                next = Some(next.map_or(o.market.window.end_at_utc_ms, |t: i64| {
                    t.min(o.market.window.end_at_utc_ms)
                }));
                if let Some(rule) = value(&o.settlement_reference) {
                    let at = o.market.window.end_at_utc_ms - probability::window(*rule);
                    if at > now {
                        next = Some(next.unwrap().min(at));
                    }
                }
                // 路径余量与窗口边界触发时间评估，不增加固定频率或最长等待。
                for path in [
                    self.config.quote.execution_path_ms,
                    self.config.repair.maker_path_ms,
                    self.config.repair.active_path_ms,
                ] {
                    let at = o
                        .market
                        .window
                        .end_at_utc_ms
                        .saturating_sub(i64::try_from(path).unwrap_or(i64::MAX));
                    if at > now {
                        next = Some(next.unwrap().min(at));
                    }
                }
            }
            let restricted = s.unknown
                || !d.source_health.is_empty()
                || s.investment > r(Decimal::from(1000))
                || s.exposure[0].clone().max(s.exposure[1].clone())
                    > r(self.config.risk.per_market_orphan_limit_pusd)
                || account_exposure > r(self.config.risk.account_orphan_limit_pusd)
                || self.continuity.terminating
                || !matches!(equity, ExecutionReadValueV1::Known(_))
                || p.up().is_none()
                || !trading;
            let cap = d.maximum_repair_price.as_ref().map(decimal).transpose()?;
            let mut cancel = Vec::new();
            for pending in &s.pending {
                let qualified = missing.is_some_and(|side| {
                    o.assets
                        .get(side)
                        .is_some_and(|a| a.asset.token_id == pending.token)
                }) && cap.is_some_and(|cap| pending.price <= cap)
                    && !s.unknown;
                let price_invalid = !(self.config.quote.initial_pair_price_reference
                    == InitialPairPriceReferenceV1::FairValue
                    && targets.iter().all(Result::is_err))
                    && o.assets
                        .iter()
                        .position(|a| a.asset.token_id == pending.token)
                        .is_none_or(|side| match targets[side] {
                            Ok(target) => pending.price > target,
                            Err(_) => {
                                self.config.quote.initial_pair_price_reference
                                    == InitialPairPriceReferenceV1::BestBid
                            }
                        });
                if (restricted || price_invalid)
                    && !qualified
                    && let Some(id) = &pending.order_id
                {
                    cancel.push(id.clone());
                }
            }
            let cancel = self.cancel_available(input, cancel);
            if !cancel.is_empty() {
                intents.push(self.cancel(cancel)?);
            }
            for item in &intents[initial..] {
                let (sources, repair_token) = match item.intent.as_ref() {
                    ModelIntentV1::SubmitSingleBuy { order, .. } => {
                        (repair_sources.clone(), Some(order.token_id.clone()))
                    }
                    ModelIntentV1::SubmitPairedBuy { .. } => (pair_sources.clone(), None),
                    _ => continue,
                };
                diagnostic
                    .action_health_dependencies
                    .push(ActionHealthDependencyV1 {
                        global_request_id: item.context.global_request_id.clone(),
                        condition_id: o.market.condition_id.clone(),
                        sources,
                        independent_held_token: repair_token.as_ref().and_then(|_| {
                            p.up()
                                .is_none()
                                .then(|| {
                                    missing.map(|side| o.assets[1 - side].asset.token_id.clone())
                                })
                                .flatten()
                        }),
                        repair_token,
                        business: Self::business_dependency_from_reads(
                            &input.action_before,
                            &reads,
                            &o.market.condition_id,
                            &[],
                        )
                        .ok_or(StrategyErrorV1::Arithmetic("动作缺公共市场经济事实"))?,
                    });
            }
            diagnostic.markets.push(d);
        }
        let mut canceled = std::collections::BTreeSet::new();
        intents.retain_mut(|item| {
            if let ModelIntentV1::CancelOrders { platform_order_ids } =
                Arc::make_mut(&mut item.intent)
            {
                platform_order_ids.retain(|id| canceled.insert(id.clone()));
                !platform_order_ids.is_empty()
            } else {
                true
            }
        });
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: next,
            diagnostics: diagnostic,
        })
    }
}
