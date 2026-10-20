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
        })
    }
}
