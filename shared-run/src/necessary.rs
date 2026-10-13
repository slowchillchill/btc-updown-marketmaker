//! 同步按需数值读取；完整冻结仅在消费者显式请求时生成。
use crate::*;
use contracts::{economics::EconomicRational as R, *};
use ledger::*;
use rust_decimal::Decimal;
use std::sync::OnceLock;

#[derive(Clone)]
pub struct OwnedActionAccountV1 {
    pub(crate) ledger: LedgerProjectionV2,
    pub(crate) economics: AccountEconomicsReadV2,
}
pub trait ActionAccountReadV1 {
    fn full(&self) -> &OwnedActionAccountV1;
    fn cash_available(&self) -> (Decimal, Decimal);
    fn profit_and_equity(&self) -> (R, ExecutionReadValueV1<R>);
    fn market(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1>;
    fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1;
}
fn amount(v: &ExactDecimal) -> Decimal {
    Decimal::from_str_exact(&v.0).expect("原精确金额")
}
fn rational(v: &ExactRationalV1) -> R {
    R::try_from(v).expect("原精确有理数")
}
impl ActionAccountReadV1 for OwnedActionAccountV1 {
    fn full(&self) -> &Self {
        self
    }
    fn cash_available(&self) -> (Decimal, Decimal) {
        (
            amount(&self.economics.cash.actual_available_pusd),
            amount(&self.economics.cash.maker_available_pusd),
        )
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
pub struct BorrowedActionAccountV1<'a> {
    ledger: &'a LedgerWriterV1,
    pub(crate) driver: Option<&'a sim_system::execution::PublicConsumptionStateV1>,
    observations: Vec<&'a MarketBusinessObservationV1>,
    equity: ExecutionReadValueV1<R>,
    full: OnceLock<OwnedActionAccountV1>,
}
impl<'a> BorrowedActionAccountV1<'a> {
    pub(crate) fn new(
        ledger: &'a LedgerWriterV1,
        frames: &[sim_system::public_consumption::PublicObservationFrameV1<'a>],
        driver: Option<&'a sim_system::execution::PublicConsumptionStateV1>,
    ) -> Option<Self> {
        let read = ledger.necessary_read_v1()?;
        let (actual, _, reserved) = read.cash();
        let totals = read.account();
        let mut nav = Some(
            R::from_decimal(actual)
                .add(&R::from_decimal(reserved))
                .add(&totals.conservative_positions),
        );

        for id in read.marked_markets() {
            let market = read.market(&ConditionId(id.into())).expect("原经济市场");
            let observation = if let Some(driver) = driver {
                let observations = driver.observations_at_current_prefix();
                observations
                    .binary_search_by(|o| o.market.condition_id.0.as_str().cmp(id))
                    .ok()
                    .map(|i| &observations[i])
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
            };
            for (i, shares) in market
                .orphan_shares
                .iter()
                .enumerate()
                .filter(|(_, q)| **q > Decimal::ZERO)
            {
                let price = market
                    .assets
                    .as_ref()
                    .and_then(|a| a.get(i))
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
        let equity = nav
            .map(|n| ExecutionReadValueV1::Known(n.sub(&totals.confirmed_unpaid_rebate)))
            .unwrap_or(ExecutionReadValueV1::Unavailable);
        Some(Self {
            ledger,
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
impl ActionAccountReadV1 for BorrowedActionAccountV1<'_> {
    fn full(&self) -> &OwnedActionAccountV1 {
        self.full.get_or_init(|| {
            let read = self.ledger.economic_read().expect("原已开户前缀");
            let economics = if let Some(driver) = self.driver {
                AccountEconomicsReadV2::from_ledger_and_observations(
                    &read,
                    driver.observations_at_current_prefix(),
                )
            } else {
                AccountEconomicsReadV2::from_ledger_and_observations(
                    &read,
                    self.observations.iter().copied(),
                )
            };
            OwnedActionAccountV1 {
                ledger: read.into_base(),
                economics,
            }
        })
    }
    fn cash_available(&self) -> (Decimal, Decimal) {
        let (a, m, _) = self.ledger.necessary_read_v1().expect("原已开户").cash();
        (a, m)
    }
    fn profit_and_equity(&self) -> (R, ExecutionReadValueV1<R>) {
        let read = self.ledger.necessary_read_v1().expect("原已开户");
        let totals = read.account();
        (
            totals
                .confirmed_net_pnl
                .sub(&totals.confirmed_unpaid_rebate),
            self.equity.clone(),
        )
    }
    fn market(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1> {
        self.ledger.necessary_read_v1()?.market(condition)
    }
    fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1 {
        self.ledger
            .necessary_read_v1()
            .expect("原已开户")
            .market_funds(condition)
    }
}
