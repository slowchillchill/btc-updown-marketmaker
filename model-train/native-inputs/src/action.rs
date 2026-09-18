//! 给定已选族后的推断解码；不计算六族支持集或训练概率。
use crate::{
    NativeInputGroup,
    prefix::{Family, Leg, Lifetime, PrefixState, Stage, Variant},
};
use contracts::{BuyLegIntentV1, BuyOrderIntentV1, ExecutionReadValueV1, ModelIntentV1};
use execution_normalization::{MemoryIdentityKeyV1, SourceNeutralEntityKeyV1};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use shared_run::{CancelObjectReadV1, SharedExecutionV1};
use sim_system::execution::{
    MutationRequestReceiptV2, PublicGroupDecisionV1, RequestContextV2, SimRunActorErrorV2,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum CancelFamily {
    Single,
    Paired,
    Market,
}
#[derive(Clone, Copy, Debug)]
pub enum SelectedAction<'a> {
    NoTrade,
    Cancel(CancelFamily),
    Buy(&'a PrefixState),
}
/// 参数图原输出及该块实际成员数；padding 不参与选择。
pub struct CancelScoreBlock {
    pub scores: Vec<f32>,
    pub valid_len: usize,
}
#[derive(Debug)]
pub enum ActionError<E> {
    Input(crate::InputError),
    Scoring(E),
    NoObjectForSelectedFamily,
    Execution(SimRunActorErrorV2),
}
impl NativeInputGroup {
    /// 已选参数的精确装配；合法性及 F.4 编码域由原选择责任保证。
    pub fn buy_intent(
        &self,
        row: usize,
        state: &PrefixState,
    ) -> Result<ModelIntentV1, crate::InputError> {
        use crate::InputError::IncompleteBuyPrefix;
        let frame = &self.rows[row];
        let action = frame.action_context.as_ref().ok_or(IncompleteBuyPrefix)?;
        let n = state
            .quantity_hundredths
            .as_ref()
            .ok_or(IncompleteBuyPrefix)?;
        fn decimal(coefficient: &BigUint, scale: u32) -> Decimal {
            Decimal::from_i128_with_scale(coefficient.to_i128().expect("F.4 已选编码域"), scale)
                .normalize()
        }
        let q = decimal(n, 2);
        let order = |i: usize| -> Result<BuyLegIntentV1, crate::InputError> {
            let selected = &state.legs[i];
            let k = selected.price_index.as_ref().ok_or(IncompleteBuyPrefix)?;
            let tick = action.ticks[i]
                .as_ref()
                .ok_or(IncompleteBuyPrefix)?
                .public();
            let numerator: BigUint = tick.numerator.parse().expect("合法正 tick");
            let denominator: BigUint = tick.denominator.parse().expect("合法正 tick");
            let m = k * numerator * BigUint::from(10000u32) / denominator;
            let p = decimal(&m, 4);
            let value = match selected.variant.ok_or(IncompleteBuyPrefix)? {
                Variant::GtcLimit => BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: p,
                    size_shares: q,
                    post_only: selected.post_only.ok_or(IncompleteBuyPrefix)?,
                },
                Variant::GtdLimit => {
                    let lifetime = match selected.lifetime.ok_or(IncompleteBuyPrefix)? {
                        Lifetime::Seconds180 => 180,
                        Lifetime::Seconds240 => 240,
                        Lifetime::Seconds300 => 300,
                    };
                    let millis = i128::from(self.cutoff.decision_time().logical_time_utc_ms());
                    let anchor = millis.div_euclid(1000) + i128::from(millis.rem_euclid(1000) != 0);
                    BuyOrderIntentV1::GtdLimit {
                        limit_price_pusd_per_share: p,
                        size_shares: q,
                        post_only: selected.post_only.ok_or(IncompleteBuyPrefix)?,
                        expires_at_utc_sec: (anchor + 60 + lifetime)
                            .try_into()
                            .expect("i64 毫秒的 GTD 秒值"),
                    }
                }
                Variant::FokMarket => BuyOrderIntentV1::FokMarket {
                    spend_pusd: decimal(&(n * &m), 6),
                    worst_price_pusd_per_share: p,
                },
                Variant::FakMarket => BuyOrderIntentV1::FakMarket {
                    spend_pusd: decimal(&(n * &m), 6),
                    worst_price_pusd_per_share: p,
                },
            };
            Ok(BuyLegIntentV1 {
                token_id: action.tokens[i].clone().ok_or(IncompleteBuyPrefix)?,
                order: value,
            })
        };
        let condition_id = frame.hidden_state_key.condition_id.clone();
        match state.family {
            Some(Family::SubmitSingleBuy) => Ok(ModelIntentV1::SubmitSingleBuy {
                condition_id,
                order: order(usize::from(
                    state.single_leg.ok_or(IncompleteBuyPrefix)? == Leg::B,
                ))?,
            }),
            Some(Family::SubmitPairedBuy) => Ok(ModelIntentV1::SubmitPairedBuy {
                condition_id,
                orders: [order(0)?, order(1)?],
            }),
            _ => Err(IncompleteBuyPrefix),
        }
    }
    /// 各块保持 cancel 的完整原顺序；只对给定族比较，严格大于才替换。
    pub fn cancel_intent(
        &self,
        family: CancelFamily,
        blocks: &[CancelScoreBlock],
    ) -> Result<Option<ModelIntentV1>, crate::InputError> {
        let read = self
            .action_before
            .as_ref()
            .ok_or(crate::InputError::MissingAssociation)?;
        let ExecutionReadValueV1::Known(objects) = read.cancel_objects() else {
            return Err(crate::InputError::MissingAssociation);
        };
        let scores = blocks
            .iter()
            .flat_map(|b| b.scores.iter().take(b.valid_len));
        let mut best = None;
        for (object, &score) in objects.into_iter().zip(scores) {
            if !matches!(
                (family, &object),
                (CancelFamily::Single, CancelObjectReadV1::Single { .. })
                    | (CancelFamily::Paired, CancelObjectReadV1::Paired { .. })
                    | (CancelFamily::Market, CancelObjectReadV1::Market { .. })
            ) {
                continue;
            }
            if best.as_ref().is_none_or(|(_, prior)| score > *prior) {
                best = Some((object, score));
            }
        }
        fn id(order: &shared_run::ExecutionOrderReadV1<'_>) -> String {
            let MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) =
                &order.order_identity.key
            else {
                unreachable!("public cancel object is an order")
            };
            source_order_id.clone()
        }
        Ok(best.map(|(object, _)| match object {
            CancelObjectReadV1::Single { order } => ModelIntentV1::CancelOrder {
                platform_order_id: id(&order),
            },
            CancelObjectReadV1::Paired { orders, .. } => ModelIntentV1::CancelOrders {
                platform_order_ids: orders.iter().map(id).collect(),
            },
            CancelObjectReadV1::Market { condition_id, .. } => ModelIntentV1::CancelMarketOrders {
                condition_id: condition_id.clone(),
            },
        }))
    }
    /// 整组状态消费完成后，对同组 DecisionRequired 行调用。
    /// score 返回覆盖本组完整 cancel 人口的有序有效块；图消费者只评分，
    /// 身份、意图及发送继续留在原生宿主和共享执行。
    pub async fn execute_selected<E>(
        &self,
        row: usize,
        selected: SelectedAction<'_>,
        shared: &mut SharedExecutionV1,
        decision: &mut PublicGroupDecisionV1<'_>,
        context: RequestContextV2,
        score: impl FnOnce(&Self, usize, &[f32; 60]) -> Result<Vec<CancelScoreBlock>, E>,
    ) -> Result<Option<MutationRequestReceiptV2>, ActionError<E>> {
        let intent = match selected {
            SelectedAction::NoTrade => None,
            SelectedAction::Buy(state) => Some(Arc::new(
                self.buy_intent(row, state).map_err(ActionError::Input)?,
            )),
            SelectedAction::Cancel(family) => {
                let action = self.rows[row]
                    .action_context
                    .as_ref()
                    .ok_or(ActionError::Input(crate::InputError::MissingAssociation))?;
                let prefix = action.prefix_features(&PrefixState {
                    family: Some(match family {
                        CancelFamily::Single => Family::CancelOrder,
                        CancelFamily::Paired => Family::CancelOrders,
                        CancelFamily::Market => Family::CancelMarketOrders,
                    }),
                    stage: Stage::CancelObject,
                    ..Default::default()
                });
                let blocks = score(self, row, &prefix).map_err(ActionError::Scoring)?;
                let intent = self
                    .cancel_intent(family, &blocks)
                    .map_err(ActionError::Input)?
                    .ok_or(ActionError::NoObjectForSelectedFamily)?;
                Some(Arc::new(intent))
            }
        };
        shared
            .execute(decision, context, intent)
            .await
            .map_err(ActionError::Execution)
    }
}
