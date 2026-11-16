//! 策略只读业务视图。原执行坐标、恢复引用和错误证据由宿主保存。
use crate::*;
use contracts::*;
use serde::{Deserialize, Serialize};
use sim_system::{execution::*, public_consumption::*};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExecutionTimeV1 {
    logical_time_utc_ms: i64,
}
impl StrategyExecutionTimeV1 {
    pub fn logical_time_utc_ms(&self) -> i64 {
        self.logical_time_utc_ms
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyPublicGroupReceiptV1 {
    group: PublicGroupReferenceV1,
    execution_time: StrategyExecutionTimeV1,
}
impl StrategyPublicGroupReceiptV1 {
    pub fn group(&self) -> &PublicGroupReferenceV1 {
        &self.group
    }
    pub fn execution_time(&self) -> &StrategyExecutionTimeV1 {
        &self.execution_time
    }
}
impl From<&PublicGroupAppliedReceiptV1> for StrategyPublicGroupReceiptV1 {
    fn from(value: &PublicGroupAppliedReceiptV1) -> Self {
        Self {
            group: value.group().clone(),
            execution_time: StrategyExecutionTimeV1 {
                logical_time_utc_ms: value.execution_time().logical_time_utc_ms(),
            },
        }
    }
}
/// 已交付公开组、实际读取时刻与已交付私有反馈前缀；不持有原宿主对象。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyObservationCutoffV1 {
    receipt: StrategyPublicGroupReceiptV1,
    decision_time: StrategyExecutionTimeV1,
    private_feedback_sequence: u64,
}
impl StrategyObservationCutoffV1 {
    pub fn receipt(&self) -> &StrategyPublicGroupReceiptV1 {
        &self.receipt
    }
    pub fn decision_time(&self) -> &StrategyExecutionTimeV1 {
        &self.decision_time
    }
    pub fn private_feedback_sequence(&self) -> u64 {
        self.private_feedback_sequence
    }
}
impl From<&TrustedGroupDecisionCutoffV1> for StrategyObservationCutoffV1 {
    fn from(value: &TrustedGroupDecisionCutoffV1) -> Self {
        Self {
            receipt: value.receipt().into(),
            decision_time: StrategyExecutionTimeV1 {
                logical_time_utc_ms: value.decision_time().logical_time_utc_ms(),
            },
            private_feedback_sequence: value.private_feedback_sequence(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyMarketFrameTimingV1 {
    pub market_cutoff: StrategyObservationCutoffV1,
    pub earliest_start_at_utc_ms: i64,
}
impl From<&MarketFrameTimingV1> for StrategyMarketFrameTimingV1 {
    fn from(value: &MarketFrameTimingV1) -> Self {
        Self {
            market_cutoff: (&value.market_cutoff).into(),
            earliest_start_at_utc_ms: value.earliest_start_at_utc_ms,
        }
    }
}
/// 借用原公开业务数据，不携带 SIM 原 receipt/cutoff。
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StrategyMarketReadFrameV1<'a> {
    pub statistics: Option<Arc<PublicMarketStatisticsV1>>,
    pub observation: &'a MarketBusinessObservationV1,
    pub rule_context: Arc<PublicTradingRuleContextV1>,
    pub receipt: StrategyPublicGroupReceiptV1,
    pub decision_cutoff: StrategyObservationCutoffV1,
}
impl<'a> From<&PublicObservationFrameV1<'a>> for StrategyMarketReadFrameV1<'a> {
    fn from(value: &PublicObservationFrameV1<'a>) -> Self {
        Self {
            statistics: value.statistics.clone(),
            observation: value.observation,
            rule_context: value.rule_context.clone(),
            receipt: (&value.receipt).into(),
            decision_cutoff: (&value.decision_cutoff).into(),
        }
    }
}
pub fn strategy_market_views<'a>(
    frames: &[PublicObservationFrameV1<'a>],
) -> Vec<StrategyMarketReadFrameV1<'a>> {
    frames.iter().map(Into::into).collect()
}
impl StrategyMarketReadV1 for StrategyMarketReadFrameV1<'_> {
    fn observation(&self) -> &MarketBusinessObservationV1 {
        self.observation
    }
    fn statistics(&self) -> Option<&PublicMarketStatisticsV1> {
        self.statistics.as_deref()
    }
    fn rule_context(&self) -> &Arc<PublicTradingRuleContextV1> {
        &self.rule_context
    }
}
/// 回调只借用策略自己的输出，并持有不含隐藏执行坐标的截止投影。
#[derive(Serialize)]
pub struct StrategyCalculationReadV1<'a, D = ()> {
    pub market_frame_timing: Option<StrategyMarketFrameTimingV1>,
    pub input_cutoff: StrategyObservationCutoffV1,
    pub calculation_started_at_utc_ms: i64,
    pub output: &'a StrategyOutputV1<D>,
}
impl<D> StrategyCalculationV1<D> {
    pub fn strategy_view(&self) -> StrategyCalculationReadV1<'_, D> {
        StrategyCalculationReadV1 {
            market_frame_timing: self.market_frame_timing.as_ref().map(Into::into),
            input_cutoff: (&self.input_cutoff).into(),
            calculation_started_at_utc_ms: self.calculation_started_at_utc_ms,
            output: &self.output,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyMutationReceiptV1 {
    platform_arrived_at_utc_ms: i64,
    scheduled_action_count: u64,
}
impl StrategyMutationReceiptV1 {
    pub fn platform_arrived_at_utc_ms(&self) -> i64 {
        self.platform_arrived_at_utc_ms
    }
    pub fn scheduled_action_count(&self) -> u64 {
        self.scheduled_action_count
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct StrategySentIntentReceiptV1 {
    pub sent_at_utc_ms: i64,
    pub newly_registered: bool,
    pub observed_cutoff: StrategyObservationCutoffV1,
    pub context: RequestContextV2,
    pub receipt: StrategyMutationReceiptV1,
}
impl From<&SentIntentReceiptV1> for StrategySentIntentReceiptV1 {
    fn from(value: &SentIntentReceiptV1) -> Self {
        Self {
            sent_at_utc_ms: value.sent_at_utc_ms,
            newly_registered: value.newly_registered,
            observed_cutoff: (&value.observed_cutoff).into(),
            context: value.context.clone(),
            receipt: StrategyMutationReceiptV1 {
                platform_arrived_at_utc_ms: value.receipt.platform_arrived_at_utc_ms(),
                scheduled_action_count: value.receipt.scheduled_action_count(),
            },
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum StrategyActionErrorV1 {
    Ledger,
    Reservation,
    Sim,
}
#[derive(Clone, Debug, Serialize)]
pub struct StrategyBatchExecutionErrorV1 {
    pub error: StrategyActionErrorV1,
    pub sent: Vec<StrategySentIntentReceiptV1>,
    pub attempted_intent_index: Option<usize>,
    pub failed_request_effect: Option<RequestEffectV1>,
    pub release_error_count: usize,
    local_funds_rejection: bool,
}
impl StrategyBatchExecutionErrorV1 {
    pub fn is_local_funds_rejection(&self) -> bool {
        self.local_funds_rejection
    }
}
impl From<&BatchExecutionErrorV1> for StrategyBatchExecutionErrorV1 {
    fn from(value: &BatchExecutionErrorV1) -> Self {
        Self {
            error: match &value.error {
                SharedActionErrorV1::Ledger(_) => StrategyActionErrorV1::Ledger,
                SharedActionErrorV1::Reservation(_) => StrategyActionErrorV1::Reservation,
                SharedActionErrorV1::Sim(_) => StrategyActionErrorV1::Sim,
            },
            sent: value.sent.iter().map(Into::into).collect(),
            attempted_intent_index: value.attempted_intent_index,
            failed_request_effect: value.failed_request_effect,
            release_error_count: value.release_errors.len(),
            local_funds_rejection: value.is_local_funds_rejection(),
        }
    }
}
pub fn strategy_dispatch_result(
    value: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
) -> Result<Vec<StrategySentIntentReceiptV1>, StrategyBatchExecutionErrorV1> {
    match value {
        Ok(sent) => Ok(sent.iter().map(Into::into).collect()),
        Err(error) => Err(error.into()),
    }
}
#[derive(Clone, Debug)]
pub struct StrategyInvalidatedBatchV1 {
    pub sent: Vec<StrategySentIntentReceiptV1>,
    pub invalidated_intent_index: usize,
    pub reason: IntentInvalidationReasonV1,
    pub observed_cutoff: StrategyObservationCutoffV1,
    pub detected_at_utc_ms: i64,
    pub release_error_count: usize,
}
impl InvalidatedBatchV1 {
    pub fn strategy_view(&self) -> StrategyInvalidatedBatchV1 {
        StrategyInvalidatedBatchV1 {
            sent: self.sent.iter().map(Into::into).collect(),
            invalidated_intent_index: self.invalidated_intent_index,
            reason: self.reason.clone(),
            observed_cutoff: (&self.observed_cutoff).into(),
            detected_at_utc_ms: self.detected_at_utc_ms,
            release_error_count: self.release_errors.len(),
        }
    }
}
