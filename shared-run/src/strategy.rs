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
/// 健康读取借用原公开观察，避免重复克隆盘口；账户来自同一真实 Ledger/Registry。
pub struct StrategyCurrentReadV1<'a> {
    pub action_before: ActionBeforeReadV2,
    pub trade_economics: Vec<FrozenTradeEconomicsV1>,
    pub markets: Vec<PublicObservationFrameV1<'a>>,
}
/// 每笔新增资金的原腿序号与预留；取消意图没有新增资金项。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IntentReserveV1 {
    pub batch_item_ordinal: u16,
    pub reserve: ReserveIntentV1,
}
/// 调用方保留原意图及独立请求身份；预留金额由消费者按其批准成本口径提供。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReservedIntentV1 {
    pub context: RequestContextV2,
    pub intent: Arc<ModelIntentV1>,
    pub reserves: Vec<IntentReserveV1>,
}
#[derive(serde::Serialize, serde::Deserialize)]
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
/// 策略定义原因及作用范围，公共层仅携带并执行判定。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentInvalidationReasonV1 {
    pub reason_code: String,
    pub scopes: Vec<SourceStreamScopeV1>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntentQualificationV1 {
    Qualified,
    Invalidated(IntentInvalidationReasonV1),
}
#[derive(Debug)]
pub struct InvalidatedBatchV1 {
    pub sent: Vec<SentIntentReceiptV1>,
    /// 此项失效；其后的所有项只因停批而未尝试。
    pub invalidated_intent_index: usize,
    pub reason: IntentInvalidationReasonV1,
    pub observed_cutoff: TrustedGroupDecisionCutoffV1,
    pub detected_at_utc_ms: i64,
    pub release_errors: Vec<LedgerErrorV1>,
}
#[derive(Debug)]
pub enum QualifiedBatchErrorV1<E> {
    Execution(BatchExecutionErrorV1),
    Qualification {
        error: E,
        sent: Vec<SentIntentReceiptV1>,
        release_errors: Vec<LedgerErrorV1>,
    },
}
impl<E> From<BatchExecutionErrorV1> for QualifiedBatchErrorV1<E> {
    fn from(error: BatchExecutionErrorV1) -> Self {
        Self::Execution(error)
    }
}
#[derive(Debug)]
pub enum QualifiedBatchResultV1 {
    Sent(Vec<SentIntentReceiptV1>),
    Invalidated(Box<InvalidatedBatchV1>),
}
pub trait StrategyV1 {
    type Error;
    type Diagnostics;
    /// 默认不选用；旧调用方继续使用原回调、时序与组节奏。
    fn uses_source_health(&self) -> bool {
        false
    }
    /// 返回严格晚于当前时刻的必要期限；没有固定节拍或公共阈值。
    fn next_health_evaluation_at(
        &self,
        _input: &StrategyCurrentReadV1<'_>,
        _calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        _at: i64,
    ) -> Option<i64> {
        None
    }
    /// 当前合法事实与原冻结决定分别提供，不能以新事实改写原业务值。
    fn qualify_intent(
        &mut self,
        _input: &StrategyCurrentReadV1<'_>,
        _calculation: &StrategyCalculationV1<Self::Diagnostics>,
        _intent_index: usize,
        _at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        Ok(IntentQualificationV1::Qualified)
    }
    fn invalidated(
        &mut self,
        _calculation: &StrategyCalculationV1<Self::Diagnostics>,
        _result: &InvalidatedBatchV1,
    ) {
    }
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
#[derive(serde::Serialize, serde::Deserialize)]
pub struct StrategyCalculationV1<D = ()> {
    #[serde(with = "sim_system::public_consumption::checkpoint::cutoff")]
    pub input_cutoff: TrustedGroupDecisionCutoffV1,
    pub calculation_started_at_utc_ms: i64,
    pub output: StrategyOutputV1<D>,
}
#[derive(serde::Serialize, serde::Deserialize)]
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

pub enum QualifiedStrategyDispatchV1<D> {
    Dispatched {
        dispatch: StrategyDispatchV1<D>,
        qualification_calls: u64,
    },
    Invalidated {
        calculation: CompletedStrategyCalculationV1<D>,
        result: Box<InvalidatedBatchV1>,
        qualification_calls: u64,
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
        Ok(self.input_from_frames(cutoff, &frames))
    }
    /// 静默时刻及待发复核使用当前受信观察，旧 strategy_input 保留组快照语义。
    pub fn strategy_observation_input(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        self.apply_closed()?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        let frames = driver.frames_at_observation();
        self.observe_group(&frames);
        Ok(self.input_from_frames(cutoff, &frames))
    }
    pub fn strategy_current_read<'a>(
        &mut self,
        driver: &'a PublicConsumptionStateV1,
    ) -> Result<Option<StrategyCurrentReadV1<'a>>, LedgerErrorV1> {
        self.apply_closed()?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        let frames = driver.frames_at_observation();
        self.observe_group(&frames);
        Ok(self.current_read_from_frames(cutoff, frames))
    }
    fn current_read_from_frames<'a>(
        &self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: Vec<PublicObservationFrameV1<'a>>,
    ) -> Option<StrategyCurrentReadV1<'a>> {
        Some(StrategyCurrentReadV1 {
            action_before: self.freeze_action_before(cutoff, &frames)?,
            markets: frames,
            trade_economics: self.freeze_trade_economics(),
        })
    }
    fn freeze_trade_economics(&self) -> Vec<FrozenTradeEconomicsV1> {
        self.ledger()
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
            .collect()
    }
    fn input_from_frames(
        &self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: &[PublicObservationFrameV1<'_>],
    ) -> Option<StrategyInputV1> {
        let action_before = self.freeze_action_before(cutoff, frames)?;
        Some(StrategyInputV1 {
            action_before,
            trade_economics: self.freeze_trade_economics(),
            markets: frames
                .iter()
                .map(|f| FrozenMarketReadV1 {
                    observation: f.observation.clone(),
                    rule_context: f.rule_context.clone(),
                })
                .collect(),
        })
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
    /// 计算或等待期间复核尚未尝试项；尚未预留，因此此处没有资金释放。
    pub fn revalidate_calculation<S: StrategyV1>(
        &self,
        strategy: &mut S,
        calculation: &StrategyCalculationV1<S::Diagnostics>,
        input: &StrategyCurrentReadV1<'_>,
        at: i64,
    ) -> Result<(Option<InvalidatedBatchV1>, u64), S::Error> {
        let mut calls = 0;
        if let StrategyOutputV1::Continue { intents, .. } = &calculation.output {
            for (index, intent) in intents.iter().enumerate() {
                if self
                    .registry()
                    .has_sim_submission_evidence(&intent.context.global_request_id)
                {
                    continue;
                }
                calls += 1;
                if let IntentQualificationV1::Invalidated(reason) =
                    strategy.qualify_intent(input, calculation, index, at)?
                {
                    let result = InvalidatedBatchV1 {
                        sent: vec![],
                        invalidated_intent_index: index,
                        reason,
                        observed_cutoff: input.action_before.cutoff().clone(),
                        detected_at_utc_ms: at,
                        release_errors: vec![],
                    };
                    strategy.invalidated(calculation, &result);
                    return Ok((Some(result), calls));
                }
            }
        }
        Ok((None, calls))
    }
    pub async fn dispatch_strategy_qualified<S: StrategyV1>(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        strategy: &mut S,
        calculation: CompletedStrategyCalculationV1<S::Diagnostics>,
    ) -> Result<QualifiedStrategyDispatchV1<S::Diagnostics>, QualifiedBatchErrorV1<S::Error>> {
        let StrategyOutputV1::Continue { intents, .. } = &calculation.calculation.output else {
            return Ok(QualifiedStrategyDispatchV1::Dispatched {
                dispatch: StrategyDispatchV1::Exit { calculation },
                qualification_calls: 0,
            });
        };
        let sent_at_utc_ms = decision.sent_at_utc_ms();
        let cutoff = decision.frames_at_observation()[0].decision_cutoff.clone();
        let mut qualification_calls = 0;
        let result = self
            .execute_batch_qualified(
                decision,
                intents,
                Some(&cutoff),
                |execution, current_decision, index, at| {
                    let frames = current_decision.frames_at_observation();
                    let input = execution
                        .current_read_from_frames(frames[0].decision_cutoff.clone(), frames)
                        .expect("合法观察与已开户账本");
                    qualification_calls += 1;
                    strategy.qualify_intent(&input, &calculation.calculation, index, at)
                },
            )
            .await;
        match result {
            Ok(QualifiedBatchResultV1::Sent(sent)) => {
                let original = Ok(sent);
                strategy.dispatched(&calculation.calculation, &original);
                Ok(QualifiedStrategyDispatchV1::Dispatched {
                    dispatch: StrategyDispatchV1::Continue {
                        calculation,
                        sent_at_utc_ms,
                        receipts: original.map_err(QualifiedBatchErrorV1::Execution)?,
                    },
                    qualification_calls,
                })
            }
            Ok(QualifiedBatchResultV1::Invalidated(result)) => {
                strategy.invalidated(&calculation.calculation, &result);
                Ok(QualifiedStrategyDispatchV1::Invalidated {
                    calculation,
                    result,
                    qualification_calls,
                })
            }
            Err(QualifiedBatchErrorV1::Execution(error)) => {
                let original = Err(error);
                strategy.dispatched(&calculation.calculation, &original);
                match original {
                    Err(error) => Err(QualifiedBatchErrorV1::Execution(error)),
                    Ok(_) => unreachable!("原发送错误"),
                }
            }
            Err(other) => Err(other),
        }
    }
    /// 全批先按原 Ledger 检查预留，再按原序通过 Bridge；不保证平台原子效果。
    pub async fn execute_batch_reserved(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        intents: &[ReservedIntentV1],
    ) -> Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1> {
        let result = self
            .execute_batch_qualified::<std::convert::Infallible>(
                decision,
                intents,
                None,
                |_, _, _, _| Ok(IntentQualificationV1::Qualified),
            )
            .await;
        match result {
            Ok(QualifiedBatchResultV1::Sent(sent)) => Ok(sent),
            Ok(QualifiedBatchResultV1::Invalidated(_)) => unreachable!("旧消费者无资格失效"),
            Err(QualifiedBatchErrorV1::Execution(error)) => Err(error),
            Err(QualifiedBatchErrorV1::Qualification { error, .. }) => match error {},
        }
    }
    /// 每项在原提交记录之前复核，整批预留与原请求效果链继续复用。
    async fn execute_batch_qualified<E>(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        intents: &[ReservedIntentV1],
        observed_cutoff: Option<&TrustedGroupDecisionCutoffV1>,
        mut qualify: impl FnMut(
            &Self,
            &PublicGroupDecisionV1<'_>,
            usize,
            i64,
        ) -> Result<IntentQualificationV1, E>,
    ) -> Result<QualifiedBatchResultV1, QualifiedBatchErrorV1<E>> {
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
                    }
                    .into());
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
            }
            .into());
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
                }
                .into());
            }
            let qualification = if self
                .registry()
                .has_sim_submission_evidence(&item.context.global_request_id)
            {
                Ok(IntentQualificationV1::Qualified)
            } else {
                qualify(self, decision, index, decision.sent_at_utc_ms())
            };
            match qualification {
                Ok(IntentQualificationV1::Qualified) => {}
                Ok(IntentQualificationV1::Invalidated(reason)) => {
                    return Ok(QualifiedBatchResultV1::Invalidated(Box::new(
                        InvalidatedBatchV1 {
                            sent,
                            invalidated_intent_index: index,
                            reason,
                            observed_cutoff: observed_cutoff
                                .expect("启用资格复核提供当前合法截止")
                                .clone(),
                            detected_at_utc_ms: decision.sent_at_utc_ms(),
                            release_errors: self.release_unsent_intents(&intents[index..], &owned),
                        },
                    )));
                }
                Err(error) => {
                    return Err(QualifiedBatchErrorV1::Qualification {
                        error,
                        sent,
                        release_errors: self.release_unsent_intents(&intents[index..], &owned),
                    });
                }
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
                    }
                    .into());
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
                    }
                    .into());
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
                    }
                    .into());
                }
            }
        }
        Ok(QualifiedBatchResultV1::Sent(sent))
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

/// 策略自身持有的私有记忆；宿主只协调其检查点，不解释策略规则。
pub trait CheckpointStrategyV1: StrategyV1 {
    fn write_strategy_checkpoint(
        &self,
        writer: &mut dyn std::io::Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn restore_strategy_checkpoint(
        &mut self,
        reader: &mut dyn std::io::Read,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
