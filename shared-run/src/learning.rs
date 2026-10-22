//! 模型之外的公共经济覆盖读取；复用原Ledger持仓/本金预留归约与平台资格。
use crate::*;
use contracts::{economics::EconomicRational as R, *};
use ledger::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObligationPrincipalReadV1 {
    pub condition_id: ConditionId,
    /// 原Ledger本金分项，含平台待经济项一次；不含费或持仓成本。
    pub principal_upper_pusd: ExecutionReadValueV1<ExactRationalV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearningAccountReadV1 {
    pub confirmed: std::sync::Arc<ConfirmedEconomicsReadV1>,
    pub obligations: Vec<ObligationPrincipalReadV1>,
    pub pending_noe_record_count: usize,
    pub qualified_rebates: Vec<QualifiedRebateReadV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directed_confirmations: Option<DirectedConfirmationsReadV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualifiedRebateReadV1 {
    pub identity: String,
    pub kind: contracts::economics::RebateKindV1,
    pub attribution_day_start_utc_ms: i64,
    pub result: ExecutionReadValueV1<contracts::economics::RebateEntitlementReadV1<String>>,
}
impl<P: SharedOrderEvidence> SharedExecution<P> {
    /// 资格读取来自原平台已闭隐藏前沿，仅为确认标签，不写较早Actor观察。
    pub fn learning_account_at_v1(
        &self,
        driver: &sim_system::execution::PublicConsumptionStateV1,
    ) -> Option<LearningAccountReadV1> {
        let mut value = self.learning_account_read_v1()?;
        for day in self.ledger().economic_read()?.rebate_days() {
            value.qualified_rebates.push(QualifiedRebateReadV1 {
                identity: format!("rebate:{:?}:{}", day.kind, day.attribution_day_start_utc_ms),
                kind: day.kind,
                attribution_day_start_utc_ms: day.attribution_day_start_utc_ms,
                result: driver
                    .rebate_entitlement_read_v1(day.kind, day.attribution_day_start_utc_ms),
            });
        }
        Some(value)
    }
    pub fn learning_account_read_v1(&self) -> Option<LearningAccountReadV1> {
        let read = self.ledger().economic_read()?;
        let necessary = self.ledger().necessary_read_v2()?;
        let obligations = necessary
            .funds_markets()
            .filter_map(|id| {
                let condition_id = ConditionId(id.into());
                let funds = necessary.market_funds(&condition_id);
                let principal =
                    R::from_decimal(funds.principal).add(&R::from_decimal(funds.local_principal));
                (!principal.is_zero()).then(|| ObligationPrincipalReadV1 {
                    condition_id,
                    principal_upper_pusd: ExecutionReadValueV1::Known(principal.public()),
                })
            })
            .collect();
        Some(LearningAccountReadV1 {
            confirmed: read.confirmed_economics_v1(),
            obligations,
            pending_noe_record_count: self.pending_noe_record_count(),
            qualified_rebates: Vec::new(),
            directed_confirmations: None,
        })
    }
}

/// 仅显式学习消费者取得的原公共事实；不会进入普通账户观察DTO。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectedConfirmationsReadV1 {
    pub positions: std::sync::Arc<Vec<ConfirmedPositionReadV1>>,
    pub commitments: Vec<DirectedCommitmentReadV1>,
    pub unattributed_active_orders: bool,
    pub requests: std::collections::BTreeMap<String, Option<Vec<DirectedTradeReadV1>>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectedCommitmentReadV1 {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub original_order: BuyOrderIntentV1,
    pub minimum_shares: Option<ExactDecimal>,
    pub remaining_target_shares: ExecutionReadValueV1<ExactDecimal>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectedTradeReadV1 {
    pub trade_identity: ::execution_normalization::MemoryLogicalIdentityV1,
    pub reported_final_success: bool,
    pub applied: Option<std::sync::Arc<TradeConfirmationReadV1>>,
}
impl<P: SharedOrderEvidence> SharedExecution<P> {
    pub fn learning_account_with_requests_v1(
        &self,
        driver: &sim_system::execution::PublicConsumptionStateV1,
        requests: Option<&[String]>,
    ) -> Option<LearningAccountReadV1> {
        let mut read = self.learning_account_at_v1(driver)?;
        if let Some(requests) = requests {
            let necessary = self.ledger().necessary_read_v2()?;
            read.directed_confirmations = Some(DirectedConfirmationsReadV1 {
                positions: necessary.confirmed_positions_v1(),
                unattributed_active_orders: self
                    .registry()
                    .unfinished_orders()
                    .any(|o| o.origin.is_none()),
                commitments: self
                    .registry()
                    .all_commitments()
                    .filter_map(|c| {
                        let (condition, leg) = match c.origin.intent.intent() {
                            ModelIntentV1::SubmitSingleBuy {
                                condition_id,
                                order,
                            } => (condition_id, order),
                            ModelIntentV1::SubmitPairedBuy {
                                condition_id,
                                orders,
                            } => (
                                condition_id,
                                orders.get(usize::from(c.origin.batch_item_ordinal))?,
                            ),
                            _ => return None,
                        };
                        Some(DirectedCommitmentReadV1 {
                            condition_id: condition.clone(),
                            token_id: leg.token_id.clone(),
                            original_order: leg.order.clone(),
                            minimum_shares: self
                                .registry()
                                .observed_rule_context(condition)
                                .map(|r| r.limit_min_quantity_shares.clone()),
                            remaining_target_shares: c.remaining_target_shares,
                        })
                    })
                    .collect(),
                requests: requests
                    .iter()
                    .map(|request| {
                        (
                            request.clone(),
                            self.registry()
                                .request_trade_identities_v1(request)
                                .map(|trades| {
                                    trades
                                        .into_iter()
                                        .map(|(id, reported_final_success)| DirectedTradeReadV1 {
                                            trade_identity: id.clone(),
                                            reported_final_success,
                                            applied: necessary.trade_confirmation_v1(id),
                                        })
                                        .collect()
                                }),
                        )
                    })
                    .collect(),
            });
        }
        Some(read)
    }
}
