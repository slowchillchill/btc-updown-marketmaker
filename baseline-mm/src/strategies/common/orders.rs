//! 原精确 LIMIT 成本及意图/整腿预留构造；不做策略筛选。
use super::*;
use ledger::*;
use shared_run::*;
use sim_system::{execution::RequestContextV2, platform_rules::SealedProcessRuleSnapshotV3};
use std::sync::Arc;
pub(crate) fn limit_cost(
    rules: &SealedProcessRuleSnapshotV3,
    q: Decimal,
    p: Decimal,
    tick: &ExactDecimal,
    post_only: bool,
) -> Result<R, StrategyErrorV1> {
    let index = (p / decimal(tick)?)
        .to_u16()
        .ok_or(StrategyErrorV1::Arithmetic("价格索引不能编码"))?;
    let n = (q * Decimal::from(100))
        .to_u128()
        .map(BigUint::from)
        .ok_or(StrategyErrorV1::Arithmetic("份额轴不能编码"))?;
    let cost = rules
        .limit_buy_cost_v1(&n, index, tick, post_only)
        .map_err(|e| StrategyErrorV1::Cost(e.to_string()))?;
    Ok(fraction_units(&cost.principal_micro, 6).add(&fraction_units(&cost.fee_1e5_units, 5)))
}
pub(crate) fn reserved_buy_item(
    id: String,
    condition: &ConditionId,
    legs: Vec<(BuyLegIntentV1, R, R)>,
) -> Result<ReservedIntentV1, StrategyErrorV1> {
    let reserves = legs
        .iter()
        .enumerate()
        .map(|(i, (_, principal, fee))| {
            Ok(IntentReserveV1 {
                batch_item_ordinal: i as u16,
                reserve: ReserveIntentV1 {
                    request_identity: LedgerRequestIdentityV1(format!("{id}:{i}")),
                    condition_id: condition.clone(),
                    requested_principal_pusd: exact(floor_decimal(principal, 6)?),
                    immediate_fee_reserve_pusd: exact(floor_decimal(fee, 5)?),
                },
            })
        })
        .collect::<Result<Vec<_>, StrategyErrorV1>>()?;
    let intent = if legs.len() == 2 {
        ModelIntentV1::SubmitPairedBuy {
            condition_id: condition.clone(),
            orders: [legs[0].0.clone(), legs[1].0.clone()],
        }
    } else {
        ModelIntentV1::SubmitSingleBuy {
            condition_id: condition.clone(),
            order: legs[0].0.clone(),
        }
    };
    Ok(ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id,
            caller_correlation_id: None,
        },
        intent: Arc::new(intent),
        reserves,
    })
}
