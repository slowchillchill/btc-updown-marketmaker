//! 公共策略调用与批次执行；不拥有报价、风控、历史驱动或退出排空政策。
use crate::*;
use ::execution_normalization::MemoryLogicalIdentityV1;
use contracts::{execution_normalization::SourceNeutralTradeLifecycleResultV2, *};
use ledger::*;
use sim_system::{execution::*, public_consumption::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenTradeEconomicsV1 {
    pub trade_identity: MemoryLogicalIdentityV1,
    pub fact: SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
    pub economic_applied: bool,
    pub success_applied: bool,
    pub actual_net_fee_pusd: ExactDecimal,
    pub unpaired_contribution_shares: ExecutionReadValueV1<ExactRationalV1>,
}
#[derive(Clone)]
pub struct FrozenMarketReadV1 {
    pub observation: MarketBusinessObservationV1,
    pub rule_context: Arc<PublicTradingRuleContextV1>,
}
pub struct StrategyInputV1 {
    pub action_before: ActionBeforeReadV2,
    pub markets: Vec<FrozenMarketReadV1>,
    pub trade_economics: Vec<FrozenTradeEconomicsV1>,
}
/// 每笔新增资金的原腿序号与预留；取消意图没有新增资金项。
#[derive(Clone, Debug)]
pub struct IntentReserveV1 {
    pub batch_item_ordinal: u16,
    pub reserve: ReserveIntentV1,
}
/// 调用方保留原意图及独立请求身份；预留金额由消费者按其批准成本口径提供。
#[derive(Clone, Debug)]
pub struct ReservedIntentV1 {
    pub context: RequestContextV2,
    pub intent: Arc<ModelIntentV1>,
    pub reserves: Vec<IntentReserveV1>,
}
pub enum StrategyOutputV1<D = ()> {
    Continue {
        intents: Vec<ReservedIntentV1>,
        next_evaluation_at_utc_ms: Option<i64>,
        diagnostics: D,
    },
    Exit {
        diagnostics: D,
    },
}
pub trait StrategyV1 {
    type Error;
    type Diagnostics;
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        calculation_started_at_utc_ms: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error>;
    /// 原分派结果通知；业务资源记忆由策略维护，公共层不解释诊断。
    fn dispatched(
        &mut self,
        _calculation: &StrategyCalculationV1<Self::Diagnostics>,
        _result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
    }
}
pub struct StrategyCalculationV1<D = ()> {
    pub input_cutoff: TrustedGroupDecisionCutoffV1,
    pub calculation_started_at_utc_ms: i64,
    pub output: StrategyOutputV1<D>,
}
pub struct CompletedStrategyCalculationV1<D = ()> {
    pub calculation: StrategyCalculationV1<D>,
    pub calculation_completed_at_utc_ms: i64,
}
impl<D> StrategyCalculationV1<D> {
    /// 完成时间由在线实际测量或已批准离线时序提供，没有默认计算延迟。
    pub fn complete(
        self,
        calculation_completed_at_utc_ms: i64,
    ) -> CompletedStrategyCalculationV1<D> {
        CompletedStrategyCalculationV1 {
            calculation: self,
            calculation_completed_at_utc_ms,
        }
    }
}
impl StrategyInputV1 {
    pub fn calculate<S: StrategyV1>(
        &self,
        strategy: &mut S,
        calculation_started_at_utc_ms: i64,
    ) -> Result<StrategyCalculationV1<S::Diagnostics>, S::Error> {
        Ok(StrategyCalculationV1 {
            input_cutoff: self.action_before.cutoff().clone(),
            calculation_started_at_utc_ms,
            output: strategy.decide(self, calculation_started_at_utc_ms)?,
        })
    }
}
#[derive(Debug)]
pub struct SentIntentReceiptV1 {
    pub context: RequestContextV2,
    pub receipt: MutationRequestReceiptV2,
}
#[derive(Debug)]
pub struct BatchExecutionErrorV1 {
    pub error: SharedActionErrorV1,
    pub sent: Vec<SentIntentReceiptV1>,
    /// 已进入原Bridge而返回错误的项；是否可能有效果另由原路径表达。
    pub attempted_intent_index: Option<usize>,
    pub failed_request_effect: Option<RequestEffectV1>,
    pub release_errors: Vec<LedgerErrorV1>,
}
pub enum StrategyDispatchV1<D = ()> {
    Continue {
        calculation: CompletedStrategyCalculationV1<D>,
        sent_at_utc_ms: i64,
        receipts: Vec<SentIntentReceiptV1>,
    },
    /// 仅返回退出决定；此路径不推进、发送、排空或等待任何业务对象。
    Exit {
        calculation: CompletedStrategyCalculationV1<D>,
    },
}

impl SharedExecutionV2 {
    pub fn strategy_input(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        let Some(cutoff) = self.prepare_action_group(driver)? else {
            return Ok(None);
        };
        let decision = driver.decision().expect("原公共组");
        let frames = decision.frames();
        let Some(action_before) = self.freeze_action_before(cutoff, &frames) else {
            return Ok(None);
        };
        Ok(Some(StrategyInputV1 {
            action_before,
            trade_economics: self
                .ledger()
                .economic_read()
                .expect("同截止已开户")
                .trades()
                .map(|t| FrozenTradeEconomicsV1 {
                    trade_identity: t.trade_identity.clone(),
                    fact: t.fact.clone(),
                    economic_applied: t.economic_applied,
                    success_applied: t.success_applied,
                    actual_net_fee_pusd: t.actual_net_fee_pusd,
                    unpaired_contribution_shares: t.unpaired_contribution_shares,
                })
                .collect(),
            markets: frames
                .iter()
                .map(|f| FrozenMarketReadV1 {
                    observation: f.observation.clone(),
                    rule_context: f.rule_context.clone(),
                })
                .collect(),
        }))
    }
    pub async fn dispatch_strategy<S: StrategyV1>(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        strategy: &mut S,
        calculation: CompletedStrategyCalculationV1<S::Diagnostics>,
    ) -> Result<StrategyDispatchV1<S::Diagnostics>, BatchExecutionErrorV1> {
        match &calculation.calculation.output {
            StrategyOutputV1::Exit { .. } => Ok(StrategyDispatchV1::Exit { calculation }),
            StrategyOutputV1::Continue { intents, .. } => {
                let sent_at_utc_ms = decision.sent_at_utc_ms();
                let result = self.execute_batch_reserved(decision, intents).await;
                strategy.dispatched(&calculation.calculation, &result);
                let receipts = result?;
                Ok(StrategyDispatchV1::Continue {
                    calculation,
                    sent_at_utc_ms,
                    receipts,
                })
            }
        }
    }
    /// 全批先按原 Ledger 检查预留，再按原序通过 Bridge；不保证平台原子效果。
    pub async fn execute_batch_reserved(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        intents: &[ReservedIntentV1],
    ) -> Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1> {
        let current: BTreeMap<_, _> = self
            .ledger()
            .funds_read_v1()
            .requests()
            .map(|r| (r.request_identity.clone(), r.reservation_state))
            .collect();
        // 原Reserved结果是粘滞决定，不是当前资金承诺。全批检查先于任何新预留或发送。
        // 已发请求可凭原接收回执／NOE响应重读幂等结果，不要求其已转换的预留仍活动。
        for item in intents {
            for reserve in &item.reserves {
                if current
                    .get(&reserve.reserve.request_identity)
                    .is_some_and(|state| {
                        matches!(
                            state,
                            ReservationStatusV1::Released | ReservationStatusV1::Converted
                        )
                    })
                    && !self
                        .registry()
                        .has_sim_submission_evidence(&item.context.global_request_id)
                {
                    return Err(BatchExecutionErrorV1 {
                        error: SharedActionErrorV1::Sim(
                            SimRunActorErrorV2::InvalidBusinessRequest(
                                "当前预留已结束且原请求没有实际提交证据",
                            ),
                        ),
                        sent: vec![],
                        attempted_intent_index: None,
                        failed_request_effect: None,
                        release_errors: vec![],
                    });
                }
            }
        }
        let mut owned: BTreeSet<_> = intents
            .iter()
            .flat_map(|i| i.reserves.iter())
            .map(|r| r.reserve.request_identity.clone())
            .filter(|id| !current.contains_key(id))
            .collect();
        let reserves: Vec<_> = intents
            .iter()
            .flat_map(|i| i.reserves.iter().map(|r| r.reserve.clone()))
            .collect();
        let outcome = self
            .reserve_action_v2(&reserves)
            .map_err(SharedActionErrorV1::Ledger);
        let error = match outcome {
            Ok(outcomes)
                if outcomes
                    .iter()
                    .any(|o| !matches!(o, LedgerReservationOutcomeV1::Reserved { .. })) =>
            {
                Some(SharedActionErrorV1::Reservation(outcomes))
            }
            Ok(_) => None,
            Err(error) => Some(error),
        };
        if let Some(error) = error {
            return Err(BatchExecutionErrorV1 {
                error,
                sent: vec![],
                attempted_intent_index: None,
                failed_request_effect: None,
                release_errors: self.release_unsent_intents(intents, &owned),
            });
        }
        let mut sent = Vec::new();
        let mut batch_ids = BTreeSet::new();
        let mut batch_reserve_ids = BTreeSet::new();
        for (index, item) in intents.iter().enumerate() {
            // D-0271每意图独立身份；已有单意图的“首次冻结”行为不反向改变。
            let identity_error = if !batch_ids.insert(&item.context.global_request_id) {
                Some("同批意图必须使用独立 global_request_id")
            } else if self
                .registry()
                .frozen_intent(&item.context.global_request_id)
                .is_some_and(|frozen| frozen.intent() != item.intent.as_ref())
            {
                Some("global_request_id 已绑定不同原意图")
            } else {
                None
            };
            if let Some(message) = identity_error {
                return Err(BatchExecutionErrorV1 {
                    error: SharedActionErrorV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                        message,
                    )),
                    sent,
                    attempted_intent_index: None,
                    failed_request_effect: None,
                    release_errors: self.release_unsent_intents(&intents[index..], &owned),
                });
            }
            for reserve in &item.reserves {
                let submission = LedgerSubmissionItemIdentityV1 {
                    global_request_id: item.context.global_request_id.clone(),
                    batch_item_ordinal: reserve.batch_item_ordinal,
                };
                let id = &reserve.reserve.request_identity;
                let funds = self.ledger().funds_read_v1();
                if !batch_reserve_ids.insert(id.clone())
                    || (current.contains_key(id)
                        && funds.submission_request(&submission) != Some(id))
                {
                    return Err(BatchExecutionErrorV1 {
                        error: SharedActionErrorV1::Sim(
                            SimRunActorErrorV2::InvalidBusinessRequest(
                                "预留请求身份已属于其他提交项",
                            ),
                        ),
                        sent,
                        attempted_intent_index: None,
                        failed_request_effect: None,
                        release_errors: self.release_unsent_intents(&intents[index..], &owned),
                    });
                }
                let event = LedgerLocalEventIdentityV1(
                    serde_json::to_string(&("submission-started", &submission)).expect("原身份"),
                );
                if let Err(error) = self.record_submission_started(
                    &event,
                    &reserve.reserve.request_identity,
                    &submission,
                ) {
                    return Err(BatchExecutionErrorV1 {
                        error: SharedActionErrorV1::Ledger(error),
                        sent,
                        attempted_intent_index: None,
                        failed_request_effect: None,
                        release_errors: self.release_unsent_intents(&intents[index..], &owned),
                    });
                }
            }
            match self
                .execute_with_request_effect(
                    decision,
                    item.context.clone(),
                    Some(item.intent.clone()),
                )
                .await
            {
                Ok(Some(receipt)) => {
                    // 已登记／提交成功，不再属于本批明确未发占用。
                    for reserve in &item.reserves {
                        owned.remove(&reserve.reserve.request_identity);
                    }
                    sent.push(SentIntentReceiptV1 {
                        context: item.context.clone(),
                        receipt,
                    });
                }
                Ok(None) => unreachable!("批次每项有明确原意图"),
                Err(failure) => {
                    let unsent_start = if failure.effect == RequestEffectV1::NoRequestEffect {
                        index
                    } else {
                        for reserve in &item.reserves {
                            owned.remove(&reserve.reserve.request_identity);
                        }
                        index + 1
                    };
                    return Err(BatchExecutionErrorV1 {
                        error: SharedActionErrorV1::Sim(failure.error),
                        sent,
                        attempted_intent_index: Some(index),
                        failed_request_effect: Some(failure.effect),
                        release_errors: self
                            .release_unsent_intents(&intents[unsent_start..], &owned),
                    });
                }
            }
        }
        Ok(sent)
    }
    fn release_unsent_intents(
        &mut self,
        intents: &[ReservedIntentV1],
        owned: &BTreeSet<LedgerRequestIdentityV1>,
    ) -> Vec<LedgerErrorV1> {
        let mut errors = Vec::new();
        for item in intents {
            for reserve in &item.reserves {
                let id = &reserve.reserve.request_identity;
                let active = self.ledger().funds_read_v1().requests().any(|r| {
                    r.request_identity == id
                        && matches!(
                            r.reservation_state,
                            ReservationStatusV1::ReservedBeforeSend
                                | ReservationStatusV1::SubmittedUnknown
                        )
                });
                if active && owned.contains(id) {
                    let event = LedgerLocalEventIdentityV1(
                        serde_json::to_string(&("batch-known-unsent", id)).expect("原身份"),
                    );
                    if let Err(error) = self.release_known_unsent(&event, id) {
                        errors.push(error)
                    }
                }
            }
        }
        errors
    }
}
