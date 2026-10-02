//! 同一动作前 Ledger 前缀的只读经济组合，不产生 NAV 分录。
use contracts::economics::{EconomicRational as R, SettlementCostAttributionV1};
use contracts::{ExactDecimal, ExactRationalV1, ExecutionReadValueV1};
use ledger::{DualCashReadV1, LedgerEconomicReadV1, MarketEconomicTotalsReadV1};
use rust_decimal::Decimal;

pub type MarketEconomicsReadV1 = MarketEconomicTotalsReadV1;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountEconomicsReadV1 {
    pub cash: DualCashReadV1,
    pub markets: Vec<MarketEconomicsReadV1>,
    pub conservative_nav_pusd: ExactRationalV1,
    pub worst_case_maker_nav_pusd: ExactRationalV1,
    pub directional_residual_pusd: ExactRationalV1,
    pub other_account_residual_pusd: ExactRationalV1,
    pub other_receivable_pusd: ExactRationalV1,
    pub other_liability_pusd: ExactRationalV1,
}
/// best bid 标记语义；V1 保留历史判定前零估值，不用于当前动作读取。
pub const ACCOUNT_ECONOMICS_VERSION: &str = "account_economics_v2";
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AccountEconomicsReadV2 {
    pub cash: DualCashReadV1,
    pub markets: Vec<MarketEconomicsReadV1>,
    pub conservative_nav_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub worst_case_maker_nav_pusd: ExactRationalV1,
    pub directional_residual_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub other_account_residual_pusd: ExactRationalV1,
    pub other_receivable_pusd: ExactRationalV1,
    pub other_liability_pusd: ExactRationalV1,
}
impl AccountEconomicsReadV2 {
    pub fn from_ledger_and_observations<'a>(
        read: &LedgerEconomicReadV1<'_>,
        observations: impl IntoIterator<Item = &'a contracts::MarketBusinessObservationV1>,
    ) -> Self {
        use contracts::ExecutionReadValueV1::{Known, Unavailable};
        let observations: std::collections::BTreeMap<_, _> = observations
            .into_iter()
            .map(|o| (&o.market.condition_id.0, o))
            .collect();
        let base = AccountEconomicsReadV1::from_ledger(read);
        let mut mark = Some(R::zero());
        for market in &base.markets {
            if market.settlement_state != ledger::EconomicSettlementStateV1::Unresolved {
                continue;
            }
            for (side, shares) in market.orphan_shares.iter().enumerate() {
                let shares = amount(shares);
                if !shares.is_positive() {
                    continue;
                }
                let quote = market
                    .assets
                    .as_ref()
                    .and_then(|assets| assets.get(side))
                    .and_then(|asset| {
                        observations
                            .get(&market.condition_id.0)
                            .and_then(|o| o.assets.iter().find(|a| a.asset == *asset))
                    })
                    .map(|a| &a.valuation_best_bid);
                let price = quote
                    .filter(|q| q.quality == contracts::MarketDataQuality::Fresh)
                    .and_then(|q| q.value.as_ref());
                match (mark.as_mut(), price) {
                    (Some(total), Some(price)) => {
                        *total = total
                            .add(&shares.mul(&price.as_ref().map(amount).unwrap_or_else(R::zero)))
                    }
                    _ => mark = None,
                }
            }
        }
        let conservative_nav_pusd = mark
            .as_ref()
            .map(|m| Known(rational(&base.conservative_nav_pusd).add(m).public()))
            .unwrap_or(Unavailable);
        let directional_residual_pusd = mark
            .as_ref()
            .map(|m| Known(rational(&base.directional_residual_pusd).add(m).public()))
            .unwrap_or(Unavailable);
        Self {
            cash: base.cash,
            markets: base.markets,
            conservative_nav_pusd,
            worst_case_maker_nav_pusd: base.worst_case_maker_nav_pusd,
            directional_residual_pusd,
            other_account_residual_pusd: base.other_account_residual_pusd,
            other_receivable_pusd: base.other_receivable_pusd,
            other_liability_pusd: base.other_liability_pusd,
        }
    }
}
fn amount(value: &ExactDecimal) -> R {
    R::from_decimal(Decimal::from_str_exact(&value.0).expect("Ledger 原精确金额"))
}
fn rational(value: &ExactRationalV1) -> R {
    R::try_from(value).expect("Ledger 规范有理数")
}
impl AccountEconomicsReadV1 {
    pub fn from_ledger(read: &LedgerEconomicReadV1<'_>) -> Self {
        let cash = read.cash().clone();
        let markets: Vec<_> = read.market_totals().collect();
        let mut conservative =
            amount(&cash.actual_available_pusd).add(&amount(&cash.actual_reserved_pusd));
        let mut maker = amount(&cash.maker_available_pusd).add(&amount(&cash.maker_reserved_pusd));
        let mut directional = amount(&read.residual_cash().directional_cash_pusd);
        let other = amount(&read.residual_cash().other_cash_pusd);
        for market in &markets {
            let common = amount(&market.guaranteed_position_value_pusd)
                .add(&amount(&market.paired_resolution_receivable_pusd))
                .add(&rational(&market.rebates[0].confirmed_unpaid_pusd))
                .add(&rational(&market.rebates[1].confirmed_unpaid_pusd));
            let orphan = amount(&market.winner_orphan_receivable_pusd);
            let fee = amount(&market.unpaid_settlement_fee_pusd);
            let taker_fee = amount(&market.unpaid_taker_fee_pusd);
            conservative = conservative
                .add(&common)
                .add(&orphan)
                .sub(&fee)
                .sub(&taker_fee);
            maker = maker.add(&common).sub(&taker_fee);
            directional = directional.add(&orphan);
            match market.unpaid_settlement_fee_attribution {
                ExecutionReadValueV1::Known(SettlementCostAttributionV1::Maker) => {
                    maker = maker.sub(&fee)
                }
                ExecutionReadValueV1::Known(SettlementCostAttributionV1::Directional) => {
                    directional = directional.sub(&fee)
                }
                _ => {}
            }
        }
        Self {
            cash,
            markets,
            conservative_nav_pusd: conservative.public(),
            worst_case_maker_nav_pusd: maker.public(),
            directional_residual_pusd: directional.public(),
            other_account_residual_pusd: other.public(),
            other_receivable_pusd: R::zero().public(),
            other_liability_pusd: R::zero().public(),
        }
    }
}

/// 模拟结算宿主只处理原请求的公开观察；没有 LIVE 定时器或自动重试政策。
#[derive(Default)]
pub struct SettlementCoordinatorV1 {
    handled: std::collections::BTreeMap<String, contracts::economics::SettlementObservationV1>,
}
impl SettlementCoordinatorV1 {
    pub(crate) fn advance<C: Clone + std::fmt::Debug + Eq>(
        &mut self,
        registry: &crate::SharedExecutionRegistry<C>,
        read: &LedgerEconomicReadV1<'_>,
        account: &contracts::AccountInstanceKeyV1,
        decision: &mut sim_system::execution::PublicGroupDecisionV1<'_>,
    ) -> Result<(), sim_system::execution::SimRunActorErrorV2> {
        use contracts::economics::*;
        use execution_normalization::{MemoryIdentityKeyV1, SourceNeutralEntityKeyV1};
        for resolution in read.resolutions() {
            let MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Resolution {
                source_resolution_id,
            }) = &resolution.resolved.resolution_identity.key
            else {
                continue;
            };
            let identity = SettlementRequestIdV1 {
                account: account.clone(),
                condition_id: resolution.resolved.condition_id.clone(),
                resolution_identity: source_resolution_id.clone(),
                policy_version: "RESOLUTION_REDEEM_ONLY".into(),
            };
            if let Some(request) = resolution.request {
                if matches!(
                    request.outcome,
                    SettlementRequestOutcomeV1::FinalSuccess
                        | SettlementRequestOutcomeV1::FinalFailureNoEffect
                ) {
                    continue;
                }
                let key = &identity.condition_id.0;
                if self.handled.get(key) == Some(&request.observation) {
                    continue;
                }
                match request.observation {
                    SettlementObservationV1::ObservationTimeout => {
                        decision.query_resolution_redeem_v1(identity.clone())?
                    }
                    SettlementObservationV1::RetryAuthorized => {
                        decision.request_resolution_redeem_v1(SettlementRequestV1 {
                            identity: identity.clone(),
                            index_sets: [
                                "1".parse().expect("固定 index set"),
                                "2".parse().expect("固定 index set"),
                            ],
                        })?;
                    }
                    _ => {}
                }
                self.handled.insert(key.clone(), request.observation);
            } else if !registry.all_commitments().any(|commitment| {
                matches!(commitment.origin.intent.intent(),
                    contracts::ModelIntentV1::SubmitSingleBuy { condition_id, .. }
                    | contracts::ModelIntentV1::SubmitPairedBuy { condition_id, .. }
                    if condition_id == &resolution.resolved.condition_id)
            }) && resolution.economic_predecessors_closed
                == ExecutionReadValueV1::Known(true)
                && (amount(&resolution.paired_receivable_pusd).is_positive()
                    || amount(&resolution.winner_orphan_receivable_pusd).is_positive())
            {
                decision.request_resolution_redeem_v1(SettlementRequestV1 {
                    identity,
                    index_sets: [
                        "1".parse().expect("固定 index set"),
                        "2".parse().expect("固定 index set"),
                    ],
                })?;
            }
        }
        Ok(())
    }
}
