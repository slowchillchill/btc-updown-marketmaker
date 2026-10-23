//! 同步按需数值读取；完整冻结仅在消费者显式请求时生成。
use crate::*;
use ::execution_normalization::MemoryLogicalIdentityV1 as Identity;
use contracts::{economics::EconomicRational as R, *};
use ledger::*;
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
};

#[derive(Clone)]
pub struct OwnedActionAccountV2 {
    pub(crate) learning: Option<crate::LearningAccountReadV1>,
    pub(crate) ledger: LedgerProjectionV2,
    pub(crate) economics: AccountEconomicsReadV3,
    pub(crate) request_trades: BTreeMap<String, Vec<Option<FrozenTradeEconomicsV1>>>,
}
pub trait ActionAccountReadV2 {
    fn full(&self) -> &OwnedActionAccountV2;
    fn request_trade_economics(
        &self,
        request: &str,
    ) -> ExecutionReadValueV1<Vec<TradeEconomicReadV1<'_>>>;
    fn cash_available(&self) -> Decimal;
    fn profit_and_equity(&self) -> (R, ExecutionReadValueV1<R>);
    fn market(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1>;
    fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1;
    /// 账户全部仍有风险/义务的市场身份；未提供当前行情不会使旧责任消失。
    fn risk_market_ids(&self) -> Vec<ConditionId> {
        let full = self.full();
        full.economics
            .markets
            .iter()
            .filter(|m| {
                m.settlement_state == EconomicSettlementStateV1::Unresolved
                    && m.orphan_shares.iter().any(|q| amount(q) > Decimal::ZERO)
            })
            .map(|m| m.condition_id.clone())
            .chain(
                full.ledger
                    .market_projections
                    .iter()
                    .filter(|m| self.market_funds(&m.condition_id).has_obligation())
                    .map(|m| m.condition_id.clone()),
            )
            .map(|id| (id.0.clone(), id))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect()
    }
}
fn amount(v: &ExactDecimal) -> Decimal {
    Decimal::from_str_exact(&v.0).expect("原精确金额")
}
fn rational(v: &ExactRationalV1) -> R {
    R::try_from(v).expect("原精确有理数")
}
// 活动订单、Ledger 未闭合成交及已观察但尚待应用的原 NOE 的有界并集。
// 查同订单所有成交才能区分成功应用量和仍占用的量；不复制全部历史。
pub(crate) fn obligation_order_ids<R: Clone + std::fmt::Debug + Eq>(
    ledger: &LedgerWriterV1,
    registry: &SharedExecutionRegistry<R>,
    pending: &[::execution_normalization::NoeConsumedRecordRef<R>],
) -> BTreeSet<Identity> {
    let read = ledger.necessary_read_v2().expect("原已开户");
    registry
        .unfinished_orders()
        .map(|o| o.order_identity.clone())
        .chain(
            read.unclosed_trade_identities()
                .filter_map(|id| read.trade_economics(id))
                .map(|t| t.fact.order_identity.clone()),
        )
        .chain(
            pending
                .iter()
                .filter_map(crate::registry::trade)
                .map(|t| t.order_identity.clone()),
        )
        .collect()
}
pub(crate) fn request_trade_identities<R: Clone + std::fmt::Debug + Eq>(
    registry: &SharedExecutionRegistry<R>,
    orders: &BTreeSet<Identity>,
) -> BTreeMap<String, Vec<Identity>> {
    let mut requests: BTreeMap<String, Vec<Identity>> = BTreeMap::new();
    for order in orders {
        if let Some(origin) = registry.order(order).and_then(|o| o.origin) {
            requests
                .entry(origin.intent.global_request_id().to_owned())
                .or_default()
                .extend(registry.order_trade_identities(order).cloned());
        }
    }
    for ids in requests.values_mut() {
        ids.sort();
        ids.dedup();
    }
    requests
}
/// 原 Registry 已观察成交与同冻结 Ledger owner 的应用状态。
/// `ledger == None` 是完整 owner 按该身份查无记录，不是成功或已应用。
#[derive(Clone)]
pub struct ObservedTradeApplicationV1<R> {
    /// Registry 已观察的原 NOE；只由 Ledger owner 提供的未闭合项没有此引用。
    pub observed: Option<::execution_normalization::NoeConsumedRecordRef<R>>,
    pub ledger: Option<FrozenTradeEconomicsV1>,
}
impl<R: Clone + std::fmt::Debug + Eq> ObservedTradeApplicationV1<R> {
    pub fn fact(
        &self,
    ) -> &contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2<Identity> {
        if let Some(record) = &self.observed {
            crate::registry::trade(record).expect("原成交引用")
        } else {
            &self.ledger.as_ref().expect("原 owner 成交").fact
        }
    }
    pub fn success_applied(&self) -> bool {
        self.ledger.as_ref().is_some_and(|t| t.success_applied)
    }
}
pub(crate) fn freeze_obligation_trades<R: Clone + std::fmt::Debug + Eq>(
    ledger: &LedgerWriterV1,
    registry: &SharedExecutionRegistry<R>,
    orders: &BTreeSet<Identity>,
) -> Vec<ObservedTradeApplicationV1<R>> {
    let read = ledger.necessary_read_v2().expect("原已开户");
    let mut trades = BTreeMap::new();
    for record in orders.iter().flat_map(|id| registry.order_trades(id)) {
        if let Some(fact) = crate::registry::trade(record) {
            trades
                .entry(fact.trade_identity.clone())
                .or_insert_with(|| ObservedTradeApplicationV1 {
                    observed: Some(record.clone()),
                    ledger: read
                        .trade_economics(&fact.trade_identity)
                        .map(FrozenTradeEconomicsV1::from),
                });
        }
    }
    // 原 owner 已接纳而 Registry 尚无关联的成交也不能消失；由消费者按未知关联处理。
    for id in read.unclosed_trade_identities() {
        if let Some(t) = read.trade_economics(id) {
            trades
                .entry(id.clone())
                .or_insert_with(|| ObservedTradeApplicationV1 {
                    observed: None,
                    ledger: Some(FrozenTradeEconomicsV1::from(t)),
                });
        }
    }
    trades.into_values().collect()
}
pub(crate) fn freeze_request_trades(
    ledger: &LedgerWriterV1,
    ids: &BTreeMap<String, Vec<Identity>>,
) -> BTreeMap<String, Vec<Option<FrozenTradeEconomicsV1>>> {
    let read = ledger.necessary_read_v2().expect("原已开户");
    ids.iter()
        .map(|(request, ids)| {
            (
                request.clone(),
                ids.iter()
                    .map(|id| read.trade_economics(id).map(FrozenTradeEconomicsV1::from))
                    .collect(),
            )
        })
        .collect()
}
impl ActionAccountReadV2 for OwnedActionAccountV2 {
    fn request_trade_economics(
        &self,
        request: &str,
    ) -> ExecutionReadValueV1<Vec<TradeEconomicReadV1<'_>>> {
        self.request_trades
            .get(request)
            .into_iter()
            .flatten()
            .map(|t| t.as_ref().map(FrozenTradeEconomicsV1::as_read))
            .collect::<Option<Vec<_>>>()
            .map(ExecutionReadValueV1::Known)
            .unwrap_or(ExecutionReadValueV1::Unavailable)
    }
    fn full(&self) -> &Self {
        self
    }
    fn cash_available(&self) -> Decimal {
        amount(&self.economics.cash.actual_available_pusd)
    }
    fn profit_and_equity(&self) -> (R, ExecutionReadValueV1<R>) {
        let mut pnl = R::zero();
        let mut unpaid = R::zero();
        for m in &self.economics.markets {
            pnl = pnl.add(&rational(&m.confirmed_net_pnl_pusd));
            for b in &m.rebates {
                unpaid = unpaid.add(&rational(&b.confirmed_unpaid_pusd));
            }
        }
        let equity = match &self.economics.conservative_nav_pusd {
            ExecutionReadValueV1::Known(v) => ExecutionReadValueV1::Known(rational(v).sub(&unpaid)),
            _ => ExecutionReadValueV1::Unavailable,
        };
        (pnl.sub(&unpaid), equity)
    }
    fn market(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1> {
        let m = self
            .economics
            .markets
            .iter()
            .find(|m| m.condition_id == *condition)?;
        Some(MarketEconomicNumbersV1 {
            assets: m.assets.clone(),
            unresolved: m.settlement_state == EconomicSettlementStateV1::Unresolved,
            orphan_shares: std::array::from_fn(|i| amount(&m.orphan_shares[i])),
            orphan_principal: rational(&m.unresolved_orphan_principal_cost_pusd),
            orphan_fee: match &m.unpaired_success_fee_pusd {
                ExecutionReadValueV1::Known(v) => ExecutionReadValueV1::Known(rational(v)),
                ExecutionReadValueV1::NotApplicable => ExecutionReadValueV1::NotApplicable,
                _ => ExecutionReadValueV1::Unavailable,
            },
            successful_investment: rational(&m.successful_investment_pusd),
            success_shares: std::array::from_fn(|i| amount(&m.success_shares[i])),
            success_principal: std::array::from_fn(|i| rational(&m.success_principal_cost_pusd[i])),
            actual_fees: std::array::from_fn(|i| amount(&m.executed[i].actual_fee_pusd)),
        })
    }
    fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1 {
        self.ledger
            .market_projections
            .iter()
            .find(|m| m.condition_id == *condition)
            .map(|m| MarketFundsNumbersV1 {
                principal: amount(&m.principal_encumbrance_pusd),
                fee: amount(&m.immediate_taker_fee_encumbrance_pusd),
                local_principal: amount(&m.local_pending_principal_pusd),
                local_fee: amount(&m.local_pending_immediate_fee_pusd),
                platform_pending_principal: amount(
                    &m.platform_confirmation_pending_economic_principal_pusd,
                ),
                platform_pending_fee: amount(
                    &m.platform_confirmation_pending_economic_immediate_fee_pusd,
                ),
                pending_count: m.local_before_send_count
                    + m.local_after_send_unknown_count
                    + m.platform_confirmation_pending_economic_count
                    + m.open_order_count
                    + m.pending_fill_count,
            })
            .unwrap_or_default()
    }
}
pub struct BorrowedActionAccountV2<'a> {
    ledger: &'a LedgerWriterV1,
    pub(crate) driver: Option<&'a sim_system::execution::PublicConsumptionStateV1>,
    observations: Vec<&'a MarketBusinessObservationV1>,
    equity: ExecutionReadValueV1<R>,
    full: OnceLock<OwnedActionAccountV2>,
    request_trade_ids: BTreeMap<String, Vec<Identity>>,
}
impl<'a> BorrowedActionAccountV2<'a> {
    pub(crate) fn new(
        ledger: &'a LedgerWriterV1,
        request_trade_ids: BTreeMap<String, Vec<Identity>>,
        frames: &[sim_system::public_consumption::PublicObservationFrameV1<'a>],
        driver: Option<&'a sim_system::execution::PublicConsumptionStateV1>,
    ) -> Option<Self> {
        let read = ledger.necessary_read_v2()?;
        let nav = conservative_nav(&read, |id| {
            if let Some(driver) = driver {
                driver.observation_at_current_prefix(id)
            } else {
                frames
                    .binary_search_by(|f| f.observation.market.condition_id.0.as_str().cmp(id))
                    .ok()
                    .map(|i| frames[i].observation)
                    .or_else(|| {
                        frames
                            .iter()
                            .find(|f| f.observation.market.condition_id.0 == id)
                            .map(|f| f.observation)
                    })
            }
        });
        let equity = match nav {
            ExecutionReadValueV1::Known(n) => {
                ExecutionReadValueV1::Known(n.sub(&read.account().confirmed_unpaid_rebate))
            }
            _ => ExecutionReadValueV1::Unavailable,
        };
        Some(Self {
            ledger,
            request_trade_ids,
            driver,
            observations: if driver.is_some() {
                Vec::new()
            } else {
                frames.iter().map(|f| f.observation).collect()
            },
            equity,
            full: OnceLock::new(),
        })
    }
}
impl ActionAccountReadV2 for BorrowedActionAccountV2<'_> {
    fn request_trade_economics(
        &self,
        request: &str,
    ) -> ExecutionReadValueV1<Vec<TradeEconomicReadV1<'_>>> {
        let read = self.ledger.necessary_read_v2().expect("原已开户");
        self.request_trade_ids
            .get(request)
            .into_iter()
            .flatten()
            .map(|id| read.trade_economics(id))
            .collect::<Option<Vec<_>>>()
            .map(ExecutionReadValueV1::Known)
            .unwrap_or(ExecutionReadValueV1::Unavailable)
    }
    fn full(&self) -> &OwnedActionAccountV2 {
        self.full.get_or_init(|| {
            let read = self.ledger.economic_read().expect("原已开户前缀");
            let economics = if let Some(driver) = self.driver {
                AccountEconomicsReadV3::from_ledger_and_observations(
                    &read,
                    driver.observations_at_current_prefix(),
                )
            } else {
                AccountEconomicsReadV3::from_ledger_and_observations(
                    &read,
                    self.observations.iter().copied(),
                )
            };
            OwnedActionAccountV2 {
                learning: None,
                request_trades: freeze_request_trades(self.ledger, &self.request_trade_ids),
                ledger: read.into_base(),
                economics,
            }
        })
    }
    fn cash_available(&self) -> Decimal {
        self.ledger.necessary_read_v2().expect("原已开户").cash().0
    }
    fn risk_market_ids(&self) -> Vec<ConditionId> {
        let read = self.ledger.necessary_read_v2().expect("原已开户");
        read.marked_markets()
            .chain(read.funds_markets())
            .map(str::to_owned)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(ConditionId)
            .collect()
    }

    fn profit_and_equity(&self) -> (R, ExecutionReadValueV1<R>) {
        let read = self.ledger.necessary_read_v2().expect("原已开户");
        let totals = read.account();
        (
            totals
                .confirmed_net_pnl
                .sub(&totals.confirmed_unpaid_rebate),
            self.equity.clone(),
        )
    }
    fn market(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1> {
        self.ledger.necessary_read_v2()?.market(condition)
    }
    fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1 {
        self.ledger
            .necessary_read_v2()
            .expect("原已开户")
            .market_funds(condition)
    }
}

/// 原完整报告的 NAV 数值读取；报价来自消费者实际公开前缀。
pub fn conservative_nav<'a>(
    read: &LedgerNecessaryReadV2<'_>,
    mut observation: impl FnMut(&str) -> Option<&'a MarketBusinessObservationV1>,
) -> ExecutionReadValueV1<R> {
    let (actual, reserved) = read.cash();
    let mut nav = Some(
        R::from_decimal(actual)
            .add(&R::from_decimal(reserved))
            .add(&read.account().conservative_positions),
    );
    for id in read.marked_markets() {
        let (assets, shares) = read.marked_position(id).expect("原未决市场");
        let observation = observation(id);
        for (side, shares) in shares
            .iter()
            .enumerate()
            .filter(|(_, q)| **q > Decimal::ZERO)
        {
            let price = assets
                .and_then(|a| a.get(side))
                .and_then(|asset| {
                    observation.and_then(|o| o.assets.iter().find(|a| a.asset == *asset))
                })
                .map(|a| &a.valuation_best_bid)
                .filter(|q| q.quality == MarketDataQuality::Fresh)
                .and_then(|q| q.value.as_ref());
            match (&mut nav, price) {
                (Some(nav), Some(price)) => {
                    *nav = nav.add(&R::from_decimal(*shares).mul(&R::from_decimal(
                        price.as_ref().map(amount).unwrap_or(Decimal::ZERO),
                    )))
                }
                _ => nav = None,
            }
        }
    }
    nav.map(ExecutionReadValueV1::Known)
        .unwrap_or(ExecutionReadValueV1::Unavailable)
}
