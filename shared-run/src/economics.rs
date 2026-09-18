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
    pub(crate) fn advance<
        I: Iterator<
            Item = Result<
                sim_system::public_consumption::PublicGroupDeliveryV1,
                sim_system::execution::SimRunActorErrorV2,
            >,
        >,
    >(
        &mut self,
        read: &LedgerEconomicReadV1<'_>,
        account: &contracts::AccountInstanceKeyV1,
        decision: &mut sim_system::execution::PublicGroupDecisionV1<'_, I>,
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
            } else if resolution.redeem_ready == ExecutionReadValueV1::Known(true)
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
