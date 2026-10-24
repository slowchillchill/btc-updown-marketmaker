//! D-0376 持续库存候选。普通报价独立于旧配对策略的选择器和经营门。
use super::common::*;
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
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
mod account;
use account::*;
mod quote;
use quote::*;
mod health;

pub const CONTINUOUS_INVENTORY_STRATEGY_ID: &str = "continuous_inventory";
pub const CONTINUOUS_INVENTORY_RULE_VERSION: &str = "candidate_v1_continuous_inventory";
pub const CONTINUOUS_INVENTORY_QUOTE_SPACE_RULE_VERSION: &str =
    "candidate_v2_continuous_inventory_quote_space";
pub const CONTINUOUS_INVENTORY_NO_ACTIVE_REPLENISHMENT_RULE_VERSION: &str =
    "candidate_v3_continuous_inventory_no_active_replenishment";

fn ordinary_active_replenishment_default() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuousInventoryConfigV1 {
    pub health: SourceHealthConfigV1,
    pub probability_reference: ProbabilityConfigV1,
    pub capacity_shares: Decimal,
    pub inventory_skew_ticks_per_capacity: Decimal,
    /// 两腿共同额外报价空间，单位为两腿最大tick的整数倍；缺省兼容原报价。
    #[serde(default)]
    pub extra_quote_space_ticks: u32,
    /// 关闭时仅抑制经过原可行性检查的普通active正量，保留initial、受限修复和旧单维护。
    #[serde(default = "ordinary_active_replenishment_default")]
    pub enable_ordinary_active_replenishment: bool,
    pub initial_cash_pusd: Decimal,
    pub market_investment_limit_pusd: Decimal,
    pub per_market_orphan_limit_pusd: Decimal,
    pub account_orphan_limit_pusd: Decimal,
    pub drawdown_fraction: Decimal,
    pub repair: RepairConfigV2,
}
impl ContinuousInventoryConfigV1 {
    pub fn rule_version(&self) -> &'static str {
        if !self.enable_ordinary_active_replenishment {
            CONTINUOUS_INVENTORY_NO_ACTIVE_REPLENISHMENT_RULE_VERSION
        } else if self.extra_quote_space_ticks == 0 {
            CONTINUOUS_INVENTORY_RULE_VERSION
        } else {
            CONTINUOUS_INVENTORY_QUOTE_SPACE_RULE_VERSION
        }
    }
    pub fn parse_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContinuousInventoryContinuityV1 {
    pub rule_version: String,
    pub high_water_pusd: ExactRationalV1,
    pub terminating: bool,
    pub repair_market_ends: BTreeMap<String, i64>,
    pub next_request_sequence: u64,
    pub unresolved_profit_allocations: BTreeMap<String, ProfitAllocationV2>,
}
impl ContinuousInventoryContinuityV1 {
    pub fn new_account_for_config(config: &ContinuousInventoryConfigV1) -> Self {
        Self {
            rule_version: config.rule_version().into(),
            ..Self::new_account(config.initial_cash_pusd)
        }
    }
    pub fn new_account(initial_cash: Decimal) -> Self {
        Self {
            rule_version: CONTINUOUS_INVENTORY_RULE_VERSION.into(),
            high_water_pusd: r(initial_cash).public(),
            terminating: false,
            repair_market_ends: BTreeMap::new(),
            next_request_sequence: 0,
            unresolved_profit_allocations: BTreeMap::new(),
        }
    }
}
/// 存在即表示本计算在原稳定插点实际抑制了普通active正量；不是完整机会分母。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrdinaryActiveReplenishmentSuppressionV1 {
    pub original_quantities: [ExactDecimal; 2],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InventoryMarketDiagnosticV1 {
    pub condition_id: ConditionId,
    pub tokens: [TokenId; 2],
    pub books: Option<[InventoryBookV1; 2]>,
    pub center: Option<ExactRationalV1>,
    pub half_spread: Option<ExactRationalV1>,
    pub skew: Option<ExactRationalV1>,
    pub prices: [Option<ExactDecimal>; 2],
    pub h: [ExactDecimal; 2],
    pub t: [ExactDecimal; 2],
    pub difference: ExactDecimal,
    pub coverage: [ExactDecimal; 2],
    pub deficit: [ExactDecimal; 2],
    pub initial: bool,
    pub exposure: [ExactRationalV1; 2],
    pub market_exposure: ExactRationalV1,
    pub account_exposure_before: ExactRationalV1,
    pub account_exposure_after: ExactRationalV1,
    pub investment: ExactRationalV1,
    pub cash_before: ExactDecimal,
    pub cash_after: ExactDecimal,
    pub risk_quantities: [ExactDecimal; 2],
    pub quantities: [ExactDecimal; 2],
    pub ordinary_active_replenishment_suppression: Option<OrdinaryActiveReplenishmentSuppressionV1>,
    pub unavailable: Vec<String>,
    pub source_health: Vec<SourceHealthStatusV1>,
    pub obligation_count: usize,
    pub cancellations: Vec<(String, String)>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InventoryActionDependencyV1 {
    pub request_id: String,
    pub condition_id: ConditionId,
    pub sources: Vec<SourceStreamScopeV1>,
    pub independent_held_token: Option<TokenId>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InventoryAccountDependencyV1 {
    pub condition_id: ConditionId,
    pub tokens: [TokenId; 2],
    pub h: [ExactDecimal; 2],
    pub orphan_cost: ExactRationalV1,
    pub funds: ExactRationalV1,
    pub unknown: bool,
    pub obligations: Vec<ObligationDiagnosticV1>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InventoryReservationDependencyV1 {
    pub request_identity: LedgerRequestIdentityV1,
    pub condition_id: ConditionId,
    pub state: ReservationStatusV1,
    pub principal: ExactDecimal,
    pub fee: ExactDecimal,
    pub ledger_entry_sequence: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContinuousInventoryDiagnosticV1 {
    pub strategy_id: String,
    #[serde(default = "ordinary_active_replenishment_default")]
    pub enable_ordinary_active_replenishment: bool,
    pub rule_version: String,
    pub risk_equity: ExecutionReadValueV1<ExactRationalV1>,
    pub high_water: ExactRationalV1,
    pub terminating: bool,
    pub available_profit: ExactRationalV1,
    pub markets: Vec<InventoryMarketDiagnosticV1>,
    pub profit_allocations: Vec<ProfitAllocationV2>,
    pub action_dependencies: Vec<InventoryActionDependencyV1>,
    pub account_dependencies: Vec<InventoryAccountDependencyV1>,
    pub reservations: Vec<InventoryReservationDependencyV1>,
}
pub struct ContinuousInventoryV1 {
    config: ContinuousInventoryConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    namespace: String,
    continuity: ContinuousInventoryContinuityV1,
    entry_market_range: Option<EntryMarketRangeV1>,
    diagnostic_cache: std::cell::RefCell<
        Option<(
            serde_json::Value,
            u64,
            BTreeMap<String, ObligationDiagnosticV1>,
            BTreeMap<LedgerRequestIdentityV1, InventoryReservationDependencyV1>,
        )>,
    >,
}
impl ContinuousInventoryV1 {
    pub fn new(
        config: ContinuousInventoryConfigV1,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        namespace: String,
        continuity: ContinuousInventoryContinuityV1,
    ) -> Result<Self, StrategyErrorV1> {
        if namespace.is_empty()
            || namespace.trim() != namespace
            || config.health.normal_exceeds_max()
            || continuity.rule_version != config.rule_version()
        {
            return Err(StrategyErrorV1::Configuration(
                "新候选命名空间、健康边界或接续版本无效；不迁移旧策略记忆".into(),
            ));
        }
        if config.capacity_shares <= Decimal::ZERO
            || config.initial_cash_pusd <= Decimal::ZERO
            || config.per_market_orphan_limit_pusd <= Decimal::ZERO
            || !(Decimal::ZERO..=Decimal::ONE).contains(&config.drawdown_fraction)
            || [
                config.inventory_skew_ticks_per_capacity,
                config.market_investment_limit_pusd,
                config.account_orphan_limit_pusd,
                config.repair.base_loss_allowance_per_share,
                config.repair.time_loss_allowance_per_share,
                config.repair.inventory_loss_allowance_per_share,
            ]
            .iter()
            .any(|x| *x < Decimal::ZERO)
        {
            return Err(StrategyErrorV1::Configuration(
                "容量、金额和成本参数不在定义域".into(),
            ));
        }
        if config.repair.enable_active_repair
            || !config.repair.maker_fill_weight.is_finite()
            || !(0.0..=1.0).contains(&config.repair.maker_fill_weight)
            || config.repair.wait_price_improvement_per_share < Decimal::ZERO
            || config.repair.residual_risk_weight < Decimal::ZERO
        {
            return Err(StrategyErrorV1::Configuration(
                "本候选FAK必须关闭；原终止选择参数须合法".into(),
            ));
        }
        let p = &config.probability_reference;
        if [
            p.initial_sigma_usd_per_sqrt_second,
            p.estimation_error_usd,
            p.binance_innovation_weight,
            p.binance_pressure_weight_usd,
            p.binance_trade_pressure_weight_usd,
        ]
        .iter()
        .any(|v| !v.is_finite())
            || p.initial_sigma_usd_per_sqrt_second < 0.0
            || p.estimation_error_usd < 0.0
        {
            return Err(StrategyErrorV1::Configuration(
                "概率参考参数不在定义域".into(),
            ));
        }
        rational(&continuity.high_water_pusd)?;
        Ok(Self {
            config,
            rules,
            namespace,
            continuity,
            entry_market_range: None,
            diagnostic_cache: Default::default(),
        })
    }
    pub fn with_entry_market_range(mut self, range: Option<EntryMarketRangeV1>) -> Self {
        self.entry_market_range = range;
        self
    }
    pub fn config(&self) -> &ContinuousInventoryConfigV1 {
        &self.config
    }
    pub fn continuity(&self) -> ContinuousInventoryContinuityV1 {
        self.continuity.clone()
    }
    fn id(&mut self) -> Result<String, StrategyErrorV1> {
        let n = self.continuity.next_request_sequence;
        self.continuity.next_request_sequence = n
            .checked_add(1)
            .ok_or(StrategyErrorV1::Arithmetic("请求序号溢出"))?;
        Ok(format!("{}:{n}", self.namespace))
    }
    fn loss_allowance(
        &self,
        o: &MarketBusinessObservationV1,
        state: &InventoryState,
        now: i64,
    ) -> Result<R, StrategyErrorV1> {
        loss_allowance_v1(
            &self.config.repair,
            self.config.per_market_orphan_limit_pusd,
            &state.principal.add(&state.fee),
            o.market.window.start_at_utc_ms,
            o.market.window.end_at_utc_ms,
            now,
        )
        .map(r)
    }
    fn decide_input(
        &mut self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl NecessaryActionAccountReadV2,
        >,
        now: i64,
    ) -> Result<StrategyOutputV1<ContinuousInventoryDiagnosticV1>, StrategyErrorV1> {
        let read = &input.action_before;
        let (pnl, equity) = read.profit_and_equity();
        let occupied = remaining_inventory_profit_occupancy(
            read,
            &mut self.continuity.unresolved_profit_allocations,
        )?;
        let mut profit = pnl.max(R::zero()).sub(&occupied).max(R::zero());
        let mut high =
            rational(&self.continuity.high_water_pusd)?.max(r(self.config.initial_cash_pusd));
        if let ExecutionReadValueV1::Known(equity) = &equity {
            high = high.max(equity.clone());
            if *equity <= high.mul(&r(Decimal::ONE - self.config.drawdown_fraction)) {
                self.continuity.terminating = true;
            }
        }
        self.continuity.high_water_pusd = high.public();
        let mut diagnostic = ContinuousInventoryDiagnosticV1 {
            strategy_id: CONTINUOUS_INVENTORY_STRATEGY_ID.into(),
            enable_ordinary_active_replenishment: self.config.enable_ordinary_active_replenishment,
            rule_version: self.config.rule_version().into(),
            risk_equity: match &equity {
                ExecutionReadValueV1::Known(e) => ExecutionReadValueV1::Known(e.public()),
                _ => ExecutionReadValueV1::Unavailable,
            },
            high_water: high.public(),
            terminating: self.continuity.terminating,
            available_profit: profit.public(),
            markets: vec![],
            profit_allocations: vec![],
            action_dependencies: vec![],
            account_dependencies: vec![],
            reservations: read
                .request_funds()
                .map(|f| InventoryReservationDependencyV1 {
                    request_identity: f.request_identity.clone(),
                    condition_id: f.condition_id.clone(),
                    state: f.reservation_state,
                    principal: f.principal_pusd,
                    fee: f.immediate_fee_pusd,
                    ledger_entry_sequence: f.last_ledger_entry_seq,
                })
                .collect(),
        };
        let mut population = BTreeMap::new();
        for frame in &input.markets {
            let o = frame.observation();
            if let Some(assets) = pair_assets(o) {
                population.insert(
                    o.market.condition_id.0.clone(),
                    assets.map(|a| a.asset.token_id.clone()),
                );
            }
        }
        for condition in read.risk_market_ids() {
            if !population.contains_key(&condition.0) {
                if let Some(assets) = read.market_economics(&condition).and_then(|m| m.assets) {
                    if let (Some(up), Some(down)) = (
                        assets
                            .iter()
                            .find(|a| a.outcome.label.eq_ignore_ascii_case("Up")),
                        assets
                            .iter()
                            .find(|a| a.outcome.label.eq_ignore_ascii_case("Down")),
                    ) {
                        population.insert(
                            condition.0.clone(),
                            [up.token_id.clone(), down.token_id.clone()],
                        );
                    }
                }
            }
        }
        let mut states = BTreeMap::new();
        for (id, tokens) in &population {
            states.insert(
                id.clone(),
                inventory(read, &ConditionId(id.clone()), tokens)?,
            );
        }
        diagnostic.account_dependencies = population
            .iter()
            .map(|(id, tokens)| {
                let state = &states[id];
                InventoryAccountDependencyV1 {
                    condition_id: ConditionId(id.clone()),
                    tokens: tokens.clone(),
                    h: state.h.map(exact),
                    orphan_cost: state.principal.add(&state.fee).public(),
                    funds: state.funds.public(),
                    unknown: state.unknown,
                    obligations: state
                        .obligations
                        .iter()
                        .map(|o| o.diagnostic.clone())
                        .collect(),
                }
            })
            .collect();
        let account_unknown = states.values().any(|s| s.unknown)
            || read
                .risk_market_ids()
                .iter()
                .any(|id| !states.contains_key(&id.0));
        let mut account_exposure = states
            .values()
            .fold(R::zero(), |sum, s| sum.add(&s.market_exposure()));
        let risk_exists = states.values().any(InventoryState::active) || account_unknown;
        for frame in &input.markets {
            let o = frame.observation();
            if self.continuity.terminating
                && states
                    .get(&o.market.condition_id.0)
                    .is_some_and(InventoryState::active)
            {
                self.continuity
                    .repair_market_ends
                    .entry(o.market.condition_id.0.clone())
                    .or_insert(o.market.window.end_at_utc_ms);
            }
        }
        if self.continuity.terminating
            && ((!risk_exists && read.pending_noe_record_count() == 0)
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
        let mut cash = read.cash_available().max(Decimal::ZERO);
        let mut intents = Vec::new();
        let mut next = None;
        // population 是 condition 稳定顺序；每市场立即扣现金和更新账户最坏侧风险。
        for (id, tokens) in &population {
            let Some(frame) = input
                .markets
                .iter()
                .find(|f| f.observation().market.condition_id.0 == *id)
            else {
                continue;
            };
            let o = frame.observation();
            let Some(assets) = pair_assets(o) else {
                continue;
            };
            let state = &states[id];
            let before_exposure = account_exposure.clone();
            let mut d = InventoryMarketDiagnosticV1 {
                condition_id: o.market.condition_id.clone(),
                tokens: tokens.clone(),
                books: None,
                center: None,
                half_spread: None,
                skew: None,
                prices: [None, None],
                h: state.h.map(exact),
                t: state.t.map(exact),
                difference: exact(state.h[0] + state.t[0] - state.h[1] - state.t[1]),
                coverage: [exact(Decimal::ZERO), exact(Decimal::ZERO)],
                deficit: [exact(Decimal::ZERO), exact(Decimal::ZERO)],
                initial: !state.active(),
                exposure: std::array::from_fn(|i| state.exposure[i].public()),
                market_exposure: state.market_exposure().public(),
                account_exposure_before: before_exposure.public(),
                account_exposure_after: before_exposure.public(),
                investment: state.investment.public(),
                cash_before: exact(cash),
                cash_after: exact(cash),
                risk_quantities: [exact(Decimal::ZERO), exact(Decimal::ZERO)],
                quantities: [exact(Decimal::ZERO), exact(Decimal::ZERO)],
                ordinary_active_replenishment_suppression: None,
                unavailable: vec![],
                source_health: self.health_states(o, &self.normal_sources(o), now),
                obligation_count: state.obligations.len(),
                cancellations: vec![],
            };
            let trading = now >= o.market.window.start_at_utc_ms
                && now < o.market.window.end_at_utc_ms
                && value(&o.accepting_orders) == Some(&true)
                && value(&o.closed) != Some(&true);
            if now < o.market.window.end_at_utc_ms {
                next = Some(next.map_or(o.market.window.end_at_utc_ms, |n: i64| {
                    n.min(o.market.window.end_at_utc_ms)
                }));
            }
            let books = match (book(assets[0])?, book(assets[1])?) {
                (Some(up), Some(down)) => Some([up, down]),
                _ => None,
            };
            if !trading {
                d.unavailable.push("market_not_trading".into());
            }
            if account_unknown {
                d.unavailable.push("account_obligation_unknown".into());
            }
            let normal_health = d.source_health.iter().all(|s| s.state.failure().is_none());
            let normal = trading
                && normal_health
                && matches!(equity, ExecutionReadValueV1::Known(_))
                && !self.continuity.terminating
                && !account_unknown
                && self
                    .entry_market_range
                    .is_none_or(|range| range.contains(o.market.window.start_at_utc_ms));
            if !normal_health {
                d.unavailable.push("source_health".into());
            }
            if let Some(books) = books {
                let (center, half, skew, mut raw) = targets(
                    &books,
                    decimal(&d.difference)?,
                    self.config.capacity_shares,
                    self.config.inventory_skew_ticks_per_capacity,
                    self.config.extra_quote_space_ticks,
                );
                d.center = Some(center.public());
                d.half_spread = Some(half.public());
                d.skew = Some(skew.public());
                d.books = Some(books.clone());
                let allowance = self.loss_allowance(o, state, now)?;
                let orphan_cost = state.principal.add(&state.fee);
                // 成本下压先于最终价容量；利润价格支撑分母固定为 capacity，而不是后续补量。
                for side in 0..2 {
                    if state.h[1 - side] > Decimal::ZERO {
                        let unit = orphan_cost.divided_by(&r(state.h[1 - side]));
                        let cap = r(Decimal::ONE)
                            .add(&allowance)
                            .sub(&unit)
                            .add(&profit.divided_by(&r(self.config.capacity_shares)));
                        raw[side] = raw[side].clone().min(cap);
                    }
                }
                let prices = [
                    floor_price(&raw[0], &books[0])?,
                    floor_price(&raw[1], &books[1])?,
                ];
                d.prices = prices.map(|p| p.map(exact));
                let coverage: [Decimal; 2] = std::array::from_fn(|i| {
                    prices[i].map_or(Decimal::ZERO, |p| state.coverage(&tokens[i], p))
                });
                let deficits =
                    coverage.map(|c| (self.config.capacity_shares - c).max(Decimal::ZERO));
                d.coverage = coverage.map(exact);
                d.deficit = deficits.map(exact);
                let over_limit = state.market_exposure()
                    > r(self.config.per_market_orphan_limit_pusd)
                    || account_exposure > r(self.config.account_orphan_limit_pusd);
                let restricted = !normal || over_limit;
                // 受限责任仅允许实际 H 的独立降险。T 和 E 都不能借作成功成本池。
                let mut repair_side = None;
                if restricted && trading && !state.unknown {
                    if let Some(held) = state.h.iter().position(|q| *q > Decimal::ZERO) {
                        let side = 1 - held;
                        if self.independent_repair_allowed(
                            o,
                            frame.statistics(),
                            state,
                            side,
                            prices[side],
                            now,
                        )? {
                            repair_side = Some(side);
                        }
                    }
                }
                let mut a = [0u128; 2];
                let normal_limit = r(self.config.per_market_orphan_limit_pusd).min(
                    r(self.config.account_orphan_limit_pusd)
                        .sub(&account_exposure.sub(&state.market_exposure())),
                );
                for side in 0..2 {
                    let Some(price) = prices[side] else {
                        continue;
                    };
                    let max_q = if restricted {
                        if repair_side == Some(side) {
                            deficits[side].min(
                                (state.h[1 - side] - state.old_quantity(&tokens[side]))
                                    .max(Decimal::ZERO),
                            )
                        } else {
                            Decimal::ZERO
                        }
                    } else {
                        deficits[side]
                    };
                    let space = normal_limit.sub(&state.exposure[side]).max(R::zero());
                    a[side] = maximum(centishares(max_q)?, |n| {
                        let cost = limit_cost(
                            &self.rules,
                            shares(n)?,
                            price,
                            &exact(books[side].tick),
                            true,
                        )?;
                        Ok(restricted || cost <= space)
                    })?;
                }
                if d.initial {
                    let equal = a[0].min(a[1]);
                    a = [equal; 2];
                }
                d.risk_quantities = [exact(shares(a[0])?), exact(shares(a[1])?)];
                let budget = r(cash).min(
                    r(self.config.market_investment_limit_pusd)
                        .sub(&state.investment)
                        .max(R::zero()),
                );
                let future_gap = std::array::from_fn::<_, 2, _>(|side| {
                    (state.h[1 - side] - state.old_quantity(&tokens[side])).max(Decimal::ZERO)
                });
                let costs = |q: [u128; 2]| -> Result<[R; 2], StrategyErrorV1> {
                    Ok([
                        if let Some(p) = prices[0] {
                            limit_cost(&self.rules, shares(q[0])?, p, &exact(books[0].tick), true)?
                        } else {
                            R::zero()
                        },
                        if let Some(p) = prices[1] {
                            limit_cost(&self.rules, shares(q[1])?, p, &exact(books[1].tick), true)?
                        } else {
                            R::zero()
                        },
                    ])
                };
                let feasible = |q: [u128; 2]| -> Result<bool, StrategyErrorV1> {
                    let cost = costs(q)?;
                    let future = r((shares(q[0])? - future_gap[0]).max(Decimal::ZERO))
                        .mul(&r(books[1].ask))
                        .max(
                            r((shares(q[1])? - future_gap[1]).max(Decimal::ZERO))
                                .mul(&r(books[0].ask)),
                        );
                    Ok(cost[0].add(&cost[1]).add(&future) <= budget)
                };
                let mut q = highest_vector(a, feasible)?;
                let minimum = decimal(&frame.rule_context().limit_min_quantity_shares)?;
                for side in 0..2 {
                    if shares(q[side])? < minimum {
                        q[side] = 0;
                    }
                }
                if d.initial && q.contains(&0) {
                    q = [0, 0];
                }
                if !feasible(q)? {
                    q = [0, 0];
                }
                let cost = costs(q)?;
                let mut support = [R::zero(), R::zero()];
                for side in 0..2 {
                    // 旧未成交承诺不能证明本次新增不会先配入H；只按实际H可配量占用本次支持。
                    let paired = shares(q[side])?.min(state.h[1 - side]);
                    if paired > Decimal::ZERO && state.h[1 - side] > Decimal::ZERO {
                        let unit = state.principal.divided_by(&r(state.h[1 - side]));
                        support[side] = profit.clone().min(
                            unit.add(&r(prices[side].expect("正补量有合法价")))
                                .sub(&r(Decimal::ONE))
                                .max(R::zero())
                                .mul(&r(paired)),
                        );
                    }
                }
                if support[0].add(&support[1]) > profit {
                    q = [0, 0];
                }
                if !self.config.enable_ordinary_active_replenishment
                    && !restricted
                    && !d.initial
                    && q.iter().any(|quantity| *quantity > 0)
                {
                    d.ordinary_active_replenishment_suppression =
                        Some(OrdinaryActiveReplenishmentSuppressionV1 {
                            original_quantities: [exact(shares(q[0])?), exact(shares(q[1])?)],
                        });
                    q = [0, 0];
                }
                let mut legs = Vec::new();
                for side in 0..2 {
                    if q[side] > 0 {
                        legs.push((
                            BuyLegIntentV1 {
                                token_id: tokens[side].clone(),
                                order: BuyOrderIntentV1::GtcLimit {
                                    limit_price_pusd_per_share: prices[side]
                                        .expect("正补量有合法价"),
                                    size_shares: shares(q[side])?,
                                    post_only: true,
                                },
                            },
                            cost[side].clone(),
                            R::zero(),
                        ));
                    }
                }
                if !legs.is_empty() {
                    let item = reserved_buy_item(self.id()?, &o.market.condition_id, legs)?;
                    for (ordinal, reserve) in item.reserves.iter().enumerate() {
                        let (_, leg) = limit_leg(&item.intent, ordinal as u16).expect("原腿");
                        let side = tokens
                            .iter()
                            .position(|t| *t == leg.token_id)
                            .expect("原token");
                        if support[side].is_positive() {
                            diagnostic.profit_allocations.push(ProfitAllocationV2 {
                                global_request_id: item.context.global_request_id.clone(),
                                reserve_identity: reserve.reserve.request_identity.clone(),
                                condition_id: o.market.condition_id.clone(),
                                token_id: tokens[side].clone(),
                                estimated_principal_pusd: cost[side].public(),
                                estimated_fee_pusd: R::zero().public(),
                                estimated_shares: r(shares(q[side])?).public(),
                                original_orphan_shares: r(state.h[1 - side]).public(),
                                original_orphan_principal_per_share: state
                                    .principal
                                    .divided_by(&r(state.h[1 - side]))
                                    .public(),
                                estimate_cutoff: read.cutoff().clone(),
                                allocated_profit_pusd: support[side].public(),
                            });
                        }
                    }
                    diagnostic
                        .action_dependencies
                        .push(InventoryActionDependencyV1 {
                            request_id: item.context.global_request_id.clone(),
                            condition_id: o.market.condition_id.clone(),
                            sources: if let Some(side) = repair_side {
                                self.repair_sources(o, side)
                            } else {
                                self.normal_sources(o)
                            },
                            independent_held_token: repair_side
                                .map(|side| tokens[1 - side].clone()),
                        });
                    let spend = cost[0].add(&cost[1]);
                    cash -= floor_decimal(&spend, 6)?;
                    profit = profit.sub(&support[0]).sub(&support[1]);
                    account_exposure = account_exposure.sub(&state.market_exposure()).add(
                        &state.exposure[0]
                            .add(&cost[0])
                            .max(state.exposure[1].add(&cost[1])),
                    );
                    d.quantities = [exact(shares(q[0])?), exact(shares(q[1])?)];
                    d.cash_after = exact(cash);
                    d.account_exposure_after = account_exposure.public();
                    intents.push(item);
                }
                self.cancel_invalid_orders(
                    input,
                    o,
                    state,
                    &prices,
                    restricted,
                    repair_side,
                    &allowance,
                    &profit,
                    &mut intents,
                    &mut d,
                )?;
            } else {
                d.unavailable
                    .push("materialized_book_or_tick_unavailable".into());
                // 无法报价不免除已知旧单的健康、规则及真实成本责任。
                let allowance = self.loss_allowance(o, state, now)?;
                let over_limit = state.market_exposure()
                    > r(self.config.per_market_orphan_limit_pusd)
                    || account_exposure > r(self.config.account_orphan_limit_pusd);
                self.cancel_invalid_orders(
                    input,
                    o,
                    state,
                    &[None, None],
                    !normal || over_limit,
                    None,
                    &allowance,
                    &profit,
                    &mut intents,
                    &mut d,
                )?;
            }
            diagnostic.markets.push(d);
        }
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: next,
            diagnostics: diagnostic,
        })
    }
}
fn pair_assets(o: &MarketBusinessObservationV1) -> Option<[&AssetBusinessObservationV1; 2]> {
    if o.assets.len() != 2 {
        return None;
    }
    Some([
        o.assets
            .iter()
            .find(|a| a.asset.outcome.label.eq_ignore_ascii_case("Up"))?,
        o.assets
            .iter()
            .find(|a| a.asset.outcome.label.eq_ignore_ascii_case("Down"))?,
    ])
}

#[cfg(test)]
mod configuration_tests {
    use super::*;
    const PARAMETERS: &str =
        include_str!("../../../configs/strategies/continuous_inventory/adopted-v1.toml");
    #[test]
    fn ordinary_replenishment_default_bool_domain_and_independent_version() {
        let explicit = ContinuousInventoryConfigV1::parse_toml(PARAMETERS).unwrap();
        let missing = PARAMETERS.replace("enable_ordinary_active_replenishment = true\n", "");
        assert_eq!(
            ContinuousInventoryConfigV1::parse_toml(&missing).unwrap(),
            explicit
        );
        let mut json = serde_json::to_value(&explicit).unwrap();
        assert_eq!(json["enable_ordinary_active_replenishment"], true);
        json.as_object_mut()
            .unwrap()
            .remove("enable_ordinary_active_replenishment");
        assert_eq!(
            serde_json::from_value::<ContinuousInventoryConfigV1>(json.clone()).unwrap(),
            explicit
        );
        for enabled in [true, false] {
            for w in [0, 1, u32::MAX] {
                json["enable_ordinary_active_replenishment"] = serde_json::json!(enabled);
                json["extra_quote_space_ticks"] = serde_json::json!(w);
                let c: ContinuousInventoryConfigV1 = serde_json::from_value(json.clone()).unwrap();
                let expected = if !enabled {
                    CONTINUOUS_INVENTORY_NO_ACTIVE_REPLENISHMENT_RULE_VERSION
                } else if w == 0 {
                    CONTINUOUS_INVENTORY_RULE_VERSION
                } else {
                    CONTINUOUS_INVENTORY_QUOTE_SPACE_RULE_VERSION
                };
                assert_eq!(c.rule_version(), expected);
                assert_eq!(
                    ContinuousInventoryContinuityV1::new_account_for_config(&c).rule_version,
                    expected
                );
            }
        }
        for raw in ["0", "1", "null", "\"false\""] {
            json["enable_ordinary_active_replenishment"] = serde_json::from_str(raw).unwrap();
            assert!(serde_json::from_value::<ContinuousInventoryConfigV1>(json.clone()).is_err());
            assert!(
                ContinuousInventoryConfigV1::parse_toml(&PARAMETERS.replace(
                    "enable_ordinary_active_replenishment = true",
                    &format!("enable_ordinary_active_replenishment = {raw}")
                ))
                .is_err()
            );
        }
        assert!(
            !ContinuousInventoryConfigV1::parse_toml(&PARAMETERS.replace(
                "enable_ordinary_active_replenishment = true",
                "enable_ordinary_active_replenishment = false"
            ))
            .unwrap()
            .enable_ordinary_active_replenishment
        );
    }
    #[test]
    fn quote_space_defaults_integer_domain_and_continuity_identity() {
        let missing = PARAMETERS.replace("extra_quote_space_ticks = 0\n", "");
        let legacy = ContinuousInventoryConfigV1::parse_toml(&missing).unwrap();
        let explicit = ContinuousInventoryConfigV1::parse_toml(PARAMETERS).unwrap();
        assert_eq!(legacy, explicit);
        assert_eq!(legacy.rule_version(), CONTINUOUS_INVENTORY_RULE_VERSION);
        assert_eq!(
            ContinuousInventoryContinuityV1::new_account(legacy.initial_cash_pusd),
            ContinuousInventoryContinuityV1::new_account_for_config(&legacy)
        );
        let mut json = serde_json::to_value(&legacy).unwrap();
        assert_eq!(json["extra_quote_space_ticks"], 0, "完整生效配置不省略零");
        json.as_object_mut()
            .unwrap()
            .remove("extra_quote_space_ticks");
        assert_eq!(
            serde_json::from_value::<ContinuousInventoryConfigV1>(json.clone()).unwrap(),
            legacy
        );
        for w in [0, 1, 2, u32::MAX] {
            json["extra_quote_space_ticks"] = serde_json::json!(w);
            let c: ContinuousInventoryConfigV1 = serde_json::from_value(json.clone()).unwrap();
            let toml = PARAMETERS.replace(
                "extra_quote_space_ticks = 0",
                &format!("extra_quote_space_ticks = {w}"),
            );
            assert_eq!(ContinuousInventoryConfigV1::parse_toml(&toml).unwrap(), c);
            assert_eq!(c.extra_quote_space_ticks, w);
            let expected = if w == 0 {
                CONTINUOUS_INVENTORY_RULE_VERSION
            } else {
                CONTINUOUS_INVENTORY_QUOTE_SPACE_RULE_VERSION
            };
            assert_eq!(c.rule_version(), expected);
            assert_eq!(
                ContinuousInventoryContinuityV1::new_account_for_config(&c).rule_version,
                expected
            );
        }
        for raw in ["-1", "0.5", "4294967296"] {
            json["extra_quote_space_ticks"] = serde_json::from_str(raw).unwrap();
            assert!(serde_json::from_value::<ContinuousInventoryConfigV1>(json.clone()).is_err());
            assert!(
                ContinuousInventoryConfigV1::parse_toml(&PARAMETERS.replace(
                    "extra_quote_space_ticks = 0",
                    &format!("extra_quote_space_ticks = {raw}")
                ))
                .is_err()
            );
        }
    }
}
