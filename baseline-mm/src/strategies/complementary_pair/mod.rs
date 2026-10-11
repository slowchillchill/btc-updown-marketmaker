mod account;
mod construction;
mod health;
pub use health::{SourceHealthFactsV1, source_health_failure_v1, source_health_state_v1};
mod mechanics;
mod quoting;
mod repair;
mod types;
pub use types::*;
mod config;
mod spot_statistics;
pub use spot_statistics::*;
mod probability;
pub use config::*;
pub use probability::*;

use ::execution_normalization::MemoryIdentityKeyV1;
use ::execution_normalization::SourceNeutralEntityKeyV1;
use contracts::{economics::EconomicRational as R, *};
use ledger::*;
use num_bigint::{BigInt, BigUint};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use shared_run::*;
use sim_system::{
    execution::{RequestContextV2, RequestEffectV1},
    platform_rules::SealedProcessRuleSnapshotV3,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

pub const STRATEGY_ID: &str = "complementary_pair";
pub const RULE_VERSION: &str = "candidate_v1";

#[derive(Debug)]
pub enum StrategyErrorV1 {
    Configuration(String),
    Arithmetic(&'static str),
    Cost(String),
}
impl fmt::Display for StrategyErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(s) => write!(f, "策略配置：{s}"),
            Self::Arithmetic(s) => write!(f, "策略计算：{s}"),
            Self::Cost(s) => write!(f, "公共成本：{s}"),
        }
    }
}
impl std::error::Error for StrategyErrorV1 {}
fn decimal(v: &ExactDecimal) -> Result<Decimal, StrategyErrorV1> {
    Decimal::from_str_exact(&v.0).map_err(|_| StrategyErrorV1::Arithmetic("非规范公共金额"))
}
fn r(v: Decimal) -> R {
    R::from_decimal(v)
}
fn rational(v: &ExactRationalV1) -> Result<R, StrategyErrorV1> {
    R::try_from(v).map_err(|_| StrategyErrorV1::Arithmetic("非规范公共分数"))
}
fn float(v: &Decimal) -> Result<f64, StrategyErrorV1> {
    v.to_f64()
        .ok_or(StrategyErrorV1::Arithmetic("浮点映射溢出"))
}
fn fd(v: f64) -> Result<Decimal, StrategyErrorV1> {
    Decimal::from_f64_retain(v).ok_or(StrategyErrorV1::Arithmetic("统计值无法映射合法报价"))
}
fn exact(v: Decimal) -> ExactDecimal {
    ExactDecimal(v.normalize().to_string())
}
fn value<T>(f: &ObservedFactV1<T>) -> Option<&T> {
    matches!(
        f.quality,
        MarketDataQuality::Fresh | MarketDataQuality::Stale
    )
    .then_some(f.value.as_ref())
    .flatten()
}
fn known<T>(v: &ExecutionReadValueV1<T>) -> Option<&T> {
    match v {
        ExecutionReadValueV1::Known(v) => Some(v),
        _ => None,
    }
}
fn fraction_units(n: &BigUint, places: u32) -> R {
    R::ratio(BigInt::from(n.clone()), BigInt::from(10_u32).pow(places))
}
fn floor_decimal(v: &R, places: u32) -> Result<Decimal, StrategyErrorV1> {
    let n = v.numerator() * BigInt::from(10_u32).pow(places) / v.denominator();
    let n = n
        .to_i128()
        .ok_or(StrategyErrorV1::Arithmetic("精确金额不能编码为Decimal"))?;
    Decimal::try_from_i128_with_scale(n, places)
        .map_err(|_| StrategyErrorV1::Arithmetic("编码金额溢出"))
}
fn limit_leg(i: &ModelIntentV1, ordinal: u16) -> Option<(&ConditionId, &BuyLegIntentV1)> {
    match i {
        ModelIntentV1::SubmitSingleBuy {
            condition_id,
            order,
        } if ordinal == 0 => Some((condition_id, order)),
        ModelIntentV1::SubmitPairedBuy {
            condition_id,
            orders,
        } => orders.get(usize::from(ordinal)).map(|l| (condition_id, l)),
        _ => None,
    }
}
fn ord_fee(
    input: &StrategyInputV1,
    id: Option<&::execution_normalization::MemoryLogicalIdentityV1>,
) -> Result<Option<R>, StrategyErrorV1> {
    let Some(id) = id else {
        return Ok(None);
    };
    input
        .action_before
        .order_funds()
        .find(|f| f.order_identity == id)
        .and_then(|f| known(&f.immediate_fee_encumbrance_pusd).cloned())
        .map(|v| decimal(&v).map(r))
        .transpose()
}
fn platform_id<C>(o: &ExecutionOrderRead<'_, C>) -> String {
    match &o.order_identity.key {
        MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) => {
            source_order_id.clone()
        }
        _ => unreachable!("公共订单身份"),
    }
}

/// 只保存信号、高水位、终止与未决增量占用；库存/费用/订单每轮重新读取公共事实。
pub struct ComplementaryPairV1 {
    config: ComplementaryPairConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    namespace: String,
    next_request: u64,
    continuity: StrategyContinuityV1,
    signals: BTreeMap<String, SignalMemory>,
    profit_pending: BTreeMap<String, ProfitAllocationV1>,
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
}
impl<'a> PairMarketRead<'a> {
    fn from_frozen(f: &'a FrozenMarketReadV1) -> Result<Self, StrategyErrorV1> {
        let o = &f.observation;
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
            rule_context: &f.rule_context,
        })
    }
}

impl StrategyV1 for ComplementaryPairV1 {
    type Error = StrategyErrorV1;
    type Diagnostics = ComplementaryPairDiagnosticV1;
    fn observation_requirements(&self) -> contracts::PublicObservationRequirementsV1 {
        contracts::PublicObservationRequirementsV1 {
            binance_depth_quantities: self.config.probability.binance_pressure_weight_usd != 0.0
                || self.config.probability.binance_trade_pressure_weight_usd != 0.0,
        }
    }
    fn uses_source_health(&self) -> bool {
        true
    }
    fn next_health_evaluation_at(
        &self,
        input: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        self.health_deadline(input, calculation, at)
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
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        now: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        let (pnl, equity) = Self::profit_and_equity(input)?;
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
        let frames = input
            .markets
            .iter()
            .map(PairMarketRead::from_frozen)
            .collect::<Result<Vec<_>, _>>()?;
        let states = frames
            .iter()
            .map(|f| self.state(input, f))
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
        let economics = input.action_before.economics();
        let mut cash = decimal(&economics.cash.actual_available_pusd)?
            .min(decimal(&economics.cash.maker_available_pusd)?)
            .max(Decimal::ZERO);
        let mut diagnostic = ComplementaryPairDiagnosticV1 {
            strategy_id: STRATEGY_ID.into(),
            rule_version: RULE_VERSION.into(),
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
            let p = if probability_healthy {
                self.signals
                    .entry(o.market.condition_id.0.clone())
                    .or_default()
                    .estimate(o, now, &self.config.probability)?
            } else {
                ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::UnhealthySource)
            };
            let missing = s
                .orphan
                .iter()
                .position(|q| *q > Decimal::ZERO)
                .map(|held| 1 - held);
            let urgency = self.urgency(o, now, &p, s, cash, missing)?;
            let mut d = MarketDecisionDiagnosticV1 {
                condition_id: o.market.condition_id.clone(),
                probability: p.clone(),
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
            if trading {
                if repair_healthy {
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
            let mut repair_remaining = missing
                .map(|held_side| r(s.orphan[1 - held_side]))
                .unwrap_or_else(R::zero);
            let cap = d.maximum_repair_price.as_ref().map(decimal).transpose()?;
            let targets = self.quotes(o, &p, s)?;
            let mut cancel = Vec::new();
            for pending in &s.pending {
                let qualified = missing.is_some_and(|side| {
                    o.assets
                        .get(side)
                        .is_some_and(|a| a.asset.token_id == pending.token)
                }) && cap.is_some_and(|cap| pending.price <= cap)
                    && !s.unknown
                    && pending.quantity <= repair_remaining;
                if qualified {
                    repair_remaining = repair_remaining.sub(&pending.quantity);
                }
                let price_invalid = targets.is_some_and(|targets| {
                    o.assets
                        .iter()
                        .position(|a| a.asset.token_id == pending.token)
                        .is_none_or(|side| pending.price > targets[side])
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
                        business: Self::business_dependency(
                            &input.action_before,
                            &input.trade_economics,
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
