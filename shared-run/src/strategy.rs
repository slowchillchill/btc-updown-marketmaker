//! 公共策略调用与批次执行；不拥有报价、风控、历史驱动或退出排空政策。
use crate::*;
use ::execution_normalization::MemoryLogicalIdentityV1;
use contracts::{execution_normalization::SourceNeutralTradeLifecycleResultV2, *};
use ledger::*;
use sim_system::{execution::*, public_consumption::*};
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenTradeEconomicsV1 {
    pub trade_identity: MemoryLogicalIdentityV1,
    pub fact: SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
    pub economic_applied: bool,
    pub success_applied: bool,
    pub actual_net_fee_pusd: ExactDecimal,
    pub unpaired_contribution_shares: ExecutionReadValueV1<ExactRationalV1>,
}
impl From<TradeEconomicReadV1<'_>> for FrozenTradeEconomicsV1 {
    fn from(t: TradeEconomicReadV1<'_>) -> Self {
        Self {
            trade_identity: t.trade_identity.clone(),
            fact: t.fact.clone(),
            economic_applied: t.economic_applied,
            success_applied: t.success_applied,
            actual_net_fee_pusd: t.actual_net_fee_pusd,
            unpaired_contribution_shares: t.unpaired_contribution_shares,
        }
    }
}
impl FrozenTradeEconomicsV1 {
    pub(crate) fn as_read(&self) -> TradeEconomicReadV1<'_> {
        TradeEconomicReadV1 {
            trade_identity: &self.trade_identity,
            fact: &self.fact,
            economic_applied: self.economic_applied,
            success_applied: self.success_applied,
            actual_net_fee_pusd: self.actual_net_fee_pusd.clone(),
            unpaired_contribution_shares: self.unpaired_contribution_shares.clone(),
        }
    }
}
#[derive(Clone)]
pub struct FrozenMarketReadV1 {
    pub statistics: Option<Arc<PublicMarketStatisticsV1>>,
    pub observation: MarketBusinessObservationV1,
    pub rule_context: Arc<PublicTradingRuleContextV1>,
}
pub trait StrategyMarketReadV1 {
    fn observation(&self) -> &MarketBusinessObservationV1;
    fn rule_context(&self) -> &Arc<PublicTradingRuleContextV1>;
}
impl StrategyMarketReadV1 for FrozenMarketReadV1 {
    fn observation(&self) -> &MarketBusinessObservationV1 {
        &self.observation
    }
    fn rule_context(&self) -> &Arc<PublicTradingRuleContextV1> {
        &self.rule_context
    }
}
impl StrategyMarketReadV1 for PublicObservationFrameV1<'_> {
    fn observation(&self) -> &MarketBusinessObservationV1 {
        self.observation
    }
    fn rule_context(&self) -> &Arc<PublicTradingRuleContextV1> {
        &self.rule_context
    }
}
/// 市场帧的原公开截止与显式最早处理时刻；账户截止另由 action_before 持有。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MarketFrameTimingV1 {
    #[serde(with = "sim_system::public_consumption::checkpoint::cutoff")]
    pub market_cutoff: TrustedGroupDecisionCutoffV1,
    pub earliest_start_at_utc_ms: i64,
}
pub struct FrozenStrategyMarketFrameV1 {
    pub timing: MarketFrameTimingV1,
    pub markets: Vec<FrozenMarketReadV1>,
}
pub struct StrategyInputV1<
    G = SharedExecutionRegistryV2,
    M = FrozenMarketReadV1,
    A = OwnedActionAccountV2,
> {
    pub market_frame_timing: Option<MarketFrameTimingV1>,
    pub action_before: ActionBeforeReadV2<G, A>,
    pub markets: Vec<M>,
    pub trade_economics: Vec<FrozenTradeEconomicsV1>,
}
/// 健康读取借用原公开观察，避免重复克隆盘口；账户来自同一真实 Ledger/Registry。
pub struct StrategyCurrentReadV1<'a, G = SharedExecutionRegistryV2, A = OwnedActionAccountV2> {
    pub action_before: ActionBeforeReadV2<G, A>,
    pub trade_economics: Vec<FrozenTradeEconomicsV1>,
    pub markets: Vec<PublicObservationFrameV1<'a>>,
}
pub type BorrowedStrategyInputV1<'a> = StrategyInputV1<
    &'a SharedExecutionRegistryV2,
    PublicObservationFrameV1<'a>,
    BorrowedActionAccountV2<'a>,
>;
pub type BorrowedStrategyCurrentReadV1<'a> =
    StrategyCurrentReadV1<'a, &'a SharedExecutionRegistryV2, BorrowedActionAccountV2<'a>>;
enum CurrentRead<'a> {
    Owned(StrategyCurrentReadV1<'a>),
    Borrowed(BorrowedStrategyCurrentReadV1<'a>),
}
impl CurrentRead<'_> {
    fn cutoff(&self) -> &TrustedGroupDecisionCutoffV1 {
        match self {
            Self::Owned(r) => r.action_before.cutoff(),
            Self::Borrowed(r) => r.action_before.cutoff(),
        }
    }
    fn qualify<S: StrategyV1>(
        &self,
        strategy: &mut S,
        calculation: &StrategyCalculationV1<S::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, S::Error> {
        match self {
            Self::Owned(r) => strategy.qualify_intent(r, calculation, index, at),
            Self::Borrowed(r) => strategy.qualify_intent_borrowed(r, calculation, index, at),
        }
    }
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
    /// 只序列化真实诊断；默认消费者没有研究诊断。
    fn research_diagnostics(&self, _diagnostics: &Self::Diagnostics) -> serde_json::Value {
        serde_json::Value::Null
    }
    /// 研究记录的稳定诊断与逐次标量；默认保持原完整诊断。
    fn research_compact_diagnostics(&self, diagnostics: &Self::Diagnostics) -> serde_json::Value {
        self.research_diagnostics(diagnostics)
    }
    fn research_evaluated_markets(&self, _diagnostics: &Self::Diagnostics) -> Vec<ConditionId> {
        Vec::new()
    }
    /// 只标识真实新双腿请求涉及的市场，不计算影子动作。
    fn research_request_markets(&self, _diagnostics: &Self::Diagnostics) -> Vec<ConditionId> {
        Vec::new()
    }
    /// 仅选择同步读取表示；默认仍构造原完整拥有式输入。
    fn uses_borrowed_input(&self) -> bool {
        false
    }
    fn selects_relevant_markets(&self) -> bool {
        false
    }
    fn historical_market_requirements(&self) -> Vec<ConditionId> {
        Vec::new()
    }
    fn calculation_market_requirements(
        &self,
        _calculation: &StrategyCalculationV1<Self::Diagnostics>,
    ) -> Vec<ConditionId> {
        Vec::new()
    }
    fn decide_borrowed(
        &mut self,
        input: &BorrowedStrategyInputV1<'_>,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        self.decide(&input.to_owned_input(), at)
    }
    fn qualify_intent_borrowed(
        &mut self,
        input: &BorrowedStrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let owned = StrategyCurrentReadV1 {
            action_before: input.action_before.to_owned_read(),
            markets: input.markets.clone(),
            trade_economics: input.trade_economics.clone(),
        };
        self.qualify_intent(&owned, calculation, index, at)
    }
    /// 默认保留逐笔冻结；只消费原 owner 费用分配的策略可显式不请求明细。
    fn requires_trade_economics_details(&self) -> bool {
        true
    }
    /// 默认完整投影；日生产者取所有实际候选的需求并集。
    fn observation_requirements(&self) -> contracts::PublicObservationRequirementsV1 {
        contracts::PublicObservationRequirementsV1::FULL
    }
    /// 默认不选用；旧调用方继续使用原回调、时序与组节奏。
    fn uses_source_health(&self) -> bool {
        false
    }
    /// 返回严格晚于当前时刻的必要期限；没有固定节拍或公共阈值。
    fn next_health_evaluation_at(
        &self,
        _markets: &[PublicObservationFrameV1<'_>],
        _calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        _at: i64,
    ) -> Option<i64> {
        None
    }
    /// 每个成功消费的公开组只检查冻结动作的公开依赖；不读取账户或重算策略。
    fn qualify_intent_public(
        &self,
        _group: &ClosedBusinessGroupV1,
        _calculation: &StrategyCalculationV1<Self::Diagnostics>,
        _intent_index: usize,
        _at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        Ok(IntentQualificationV1::Qualified)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_frame_timing: Option<MarketFrameTimingV1>,
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
            market_frame_timing: self.market_frame_timing.clone(),
            input_cutoff: self.action_before.cutoff().clone(),
            calculation_started_at_utc_ms,
            output: strategy.decide(self, calculation_started_at_utc_ms)?,
        })
    }
}
impl BorrowedStrategyInputV1<'_> {
    pub fn to_owned_input(&self) -> StrategyInputV1 {
        StrategyInputV1 {
            market_frame_timing: self.market_frame_timing.clone(),
            action_before: self.action_before.to_owned_read(),
            markets: self
                .action_before
                .account
                .driver
                .map(|d| d.frames_at_observation())
                .unwrap_or_else(|| self.markets.clone())
                .iter()
                .map(|f| FrozenMarketReadV1 {
                    statistics: f.statistics.clone(),
                    observation: f.observation.clone(),
                    rule_context: f.rule_context.clone(),
                })
                .collect(),
            trade_economics: self.trade_economics.clone(),
        }
    }
    pub fn calculate_borrowed<S: StrategyV1>(
        &self,
        strategy: &mut S,
        at: i64,
    ) -> Result<StrategyCalculationV1<S::Diagnostics>, S::Error> {
        Ok(StrategyCalculationV1 {
            market_frame_timing: self.market_frame_timing.clone(),
            input_cutoff: self.action_before.cutoff().clone(),
            calculation_started_at_utc_ms: at,
            output: strategy.decide_borrowed(self, at)?,
        })
    }
}
#[derive(Clone, Debug, serde::Serialize)]
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
impl BatchExecutionErrorV1 {
    /// 整批零发送且释放成功的本地真实现金不足；其他拒绝或未知效果仍是错误。
    pub fn is_local_funds_rejection(&self) -> bool {
        self.sent.is_empty()
            && self.attempted_intent_index.is_none()
            && self.failed_request_effect.is_none()
            && self.release_errors.is_empty()
            && matches!(&self.error, SharedActionErrorV1::Reservation(outcomes)
                if outcomes.iter().any(|o| matches!(o, LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds))
                && outcomes.iter().all(|o| matches!(o, LedgerReservationOutcomeV1::Reserved { .. }
                    | LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds)))
    }
}
/// 错误路径移动交回原计算，正常分派不复制整批意图。
pub struct StrategyDispatchFailureV1<D, E> {
    pub calculation: CompletedStrategyCalculationV1<D>,
    pub error: E,
}
impl<D, E: std::fmt::Debug> std::fmt::Debug for StrategyDispatchFailureV1<D, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrategyDispatchFailureV1")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
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
    /// 先领取/登记同一截止，再只读借用；返回值释放前不能推进SharedExecution或driver。
    pub fn strategy_borrowed_observation_input<'a>(
        &'a mut self,
        driver: &'a mut PublicConsumptionStateV1,
        details: bool,
    ) -> Result<Option<BorrowedStrategyInputV1<'a>>, LedgerErrorV1> {
        self.apply_closed()?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        let frames = driver.frames_at_observation();
        self.observe_group(&frames);
        let Some(action_before) =
            self.borrow_necessary_action_before(cutoff, &frames, Some(driver))
        else {
            return Ok(None);
        };
        Ok(Some(StrategyInputV1 {
            market_frame_timing: None,
            action_before,
            trade_economics: if details {
                self.freeze_trade_economics()
            } else {
                Vec::new()
            },
            markets: frames,
        }))
    }
    pub fn strategy_selected_observation_input<'a>(
        &'a mut self,
        driver: &'a mut PublicConsumptionStateV1,
        details: bool,
        historical: &[ConditionId],
    ) -> Result<Option<BorrowedStrategyInputV1<'a>>, LedgerErrorV1> {
        self.apply_closed()?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        self.observe_public_rules(driver);
        let frames = self.selected_observations(driver, historical);
        self.observe_group(&frames);
        let Some(action_before) =
            self.borrow_necessary_action_before(cutoff, &frames, Some(driver))
        else {
            return Ok(None);
        };
        Ok(Some(StrategyInputV1 {
            market_frame_timing: None,
            action_before,
            markets: frames,
            trade_economics: if details {
                self.freeze_trade_economics()
            } else {
                Vec::new()
            },
        }))
    }
    fn selected_observations<'a>(
        &self,
        driver: &'a PublicConsumptionStateV1,
        historical: &[ConditionId],
    ) -> Vec<PublicObservationFrameV1<'a>> {
        let mut ids = std::collections::BTreeSet::new();
        ids.extend(historical.iter().map(|id| id.0.clone()));
        if let Some(read) = self.ledger().necessary_read_v2() {
            ids.extend(
                read.marked_markets()
                    .chain(read.funds_markets())
                    .map(str::to_owned),
            );
        }
        ids.extend(
            self.registry()
                .unfinished_orders()
                .filter_map(|o| o.condition_id)
                .map(|id| id.0.clone()),
        );
        ids.extend(self.registry().all_commitments().filter_map(
            |c| match c.origin.intent.intent() {
                ModelIntentV1::SubmitSingleBuy { condition_id, .. }
                | ModelIntentV1::SubmitPairedBuy { condition_id, .. } => {
                    Some(condition_id.0.clone())
                }
                _ => None,
            },
        ));
        // 身份集合只用于本次选择；返回观察借用原 driver，不借用临时集合。
        driver.selected_frames_at_observation(ids.iter().map(String::as_str))
    }
    pub fn strategy_selected_current_observations<'a>(
        &mut self,
        driver: &'a PublicConsumptionStateV1,
        historical: &[ConditionId],
    ) -> Result<Vec<PublicObservationFrameV1<'a>>, LedgerErrorV1> {
        self.apply_closed()?;
        self.observe_public_rules(driver);
        let frames = self.selected_observations(driver, historical);
        self.observe_group(&frames);
        Ok(frames)
    }
    /// 原市场帧保持冻结；账户、在途、已合法交付反馈及估值取当前真实观察前缀。
    /// 本方法不推进SIM时钟，宿主必须先沿原驱动链到达处理开始时刻。
    pub fn strategy_input_for_market_frame(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
        frame: FrozenStrategyMarketFrameV1,
        details: bool,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        self.apply_closed()?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        let current = driver.frames_at_observation();
        self.observe_group(&current);
        let Some(action_before) = self.freeze_action_before(cutoff, &current) else {
            return Ok(None);
        };
        Ok(Some(StrategyInputV1 {
            market_frame_timing: Some(frame.timing),
            action_before,
            markets: frame.markets,
            trade_economics: if details {
                self.freeze_trade_economics()
            } else {
                Vec::new()
            },
        }))
    }
    pub fn strategy_input(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        self.strategy_input_with_trade_details(driver, true)
    }
    pub fn strategy_input_with_trade_details(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
        details: bool,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        let Some(cutoff) = self.prepare_action_group(driver)? else {
            return Ok(None);
        };
        let decision = driver.decision().expect("原公共组");
        let frames = decision.frames();
        Ok(self.input_from_frames(cutoff, &frames, details))
    }
    /// 静默时刻及待发复核使用当前受信观察，旧 strategy_input 保留组快照语义。
    pub fn strategy_observation_input(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        self.strategy_observation_input_with_trade_details(driver, true)
    }
    pub fn strategy_observation_input_with_trade_details(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
        details: bool,
    ) -> Result<Option<StrategyInputV1>, LedgerErrorV1> {
        self.apply_closed()?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        let frames = driver.frames_at_observation();
        self.observe_group(&frames);
        Ok(self.input_from_frames(cutoff, &frames, details))
    }
    pub fn strategy_current_read<'a>(
        &mut self,
        driver: &'a PublicConsumptionStateV1,
    ) -> Result<Option<StrategyCurrentReadV1<'a>>, LedgerErrorV1> {
        let frames = self.strategy_current_observations(driver)?;
        let Some(cutoff) = driver.observation_cutoff() else {
            return Ok(None);
        };
        Ok(self.current_read_from_frames(cutoff, frames, true))
    }
    /// 同一真实观察保留领取及登记副作用；期限消费者不需要冻结账户历史。
    pub fn strategy_current_observations<'a>(
        &mut self,
        driver: &'a PublicConsumptionStateV1,
    ) -> Result<Vec<PublicObservationFrameV1<'a>>, LedgerErrorV1> {
        self.apply_closed()?;
        let frames = driver.frames_at_observation();
        self.observe_group(&frames);
        Ok(frames)
    }
    fn current_read_from_frames<'a>(
        &self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: Vec<PublicObservationFrameV1<'a>>,
        details: bool,
    ) -> Option<StrategyCurrentReadV1<'a>> {
        Some(StrategyCurrentReadV1 {
            action_before: self.freeze_action_before(cutoff, &frames)?,
            markets: frames,
            trade_economics: if details {
                self.freeze_trade_economics()
            } else {
                Vec::new()
            },
        })
    }
    fn current_strategy_read<'a>(
        &'a self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: Vec<PublicObservationFrameV1<'a>>,
        details: bool,
        borrowed: bool,
    ) -> Option<CurrentRead<'a>> {
        if borrowed {
            Some(CurrentRead::Borrowed(StrategyCurrentReadV1 {
                action_before: self.borrow_necessary_action_before(cutoff, &frames, None)?,
                trade_economics: if details {
                    self.freeze_trade_economics()
                } else {
                    Vec::new()
                },
                markets: frames,
            }))
        } else {
            self.current_read_from_frames(cutoff, frames, details)
                .map(CurrentRead::Owned)
        }
    }
    fn freeze_trade_economics(&self) -> Vec<FrozenTradeEconomicsV1> {
        self.ledger()
            .economic_read()
            .expect("同截止已开户")
            .trades()
            .map(FrozenTradeEconomicsV1::from)
            .collect()
    }
    fn input_from_frames(
        &self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: &[PublicObservationFrameV1<'_>],
        details: bool,
    ) -> Option<StrategyInputV1> {
        let action_before = self.freeze_action_before(cutoff, frames)?;
        Some(StrategyInputV1 {
            market_frame_timing: None,
            action_before,
            trade_economics: if details {
                self.freeze_trade_economics()
            } else {
                Vec::new()
            },
            markets: frames
                .iter()
                .map(|f| FrozenMarketReadV1 {
                    statistics: f.statistics.clone(),
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
    ) -> Result<
        StrategyDispatchV1<S::Diagnostics>,
        StrategyDispatchFailureV1<S::Diagnostics, BatchExecutionErrorV1>,
    > {
        match &calculation.calculation.output {
            StrategyOutputV1::Exit { .. } => Ok(StrategyDispatchV1::Exit { calculation }),
            StrategyOutputV1::Continue { intents, .. } => {
                let sent_at_utc_ms = decision.sent_at_utc_ms();
                let result = self.execute_batch_reserved(decision, intents).await;
                strategy.dispatched(&calculation.calculation, &result);
                let receipts = match result {
                    Ok(receipts) => receipts,
                    Err(error) => return Err(StrategyDispatchFailureV1 { calculation, error }),
                };
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
        frames: &[PublicObservationFrameV1<'_>],
        at: i64,
    ) -> Result<(Option<InvalidatedBatchV1>, u64), S::Error> {
        let mut calls = 0;
        let mut input = None;
        if let StrategyOutputV1::Continue { intents, .. } = &calculation.output {
            for (index, intent) in intents.iter().enumerate() {
                if self
                    .registry()
                    .has_sim_submission_evidence(&intent.context.global_request_id)
                {
                    continue;
                }
                let input = input.get_or_insert_with(|| {
                    self.current_strategy_read(
                        frames[0].decision_cutoff.clone(),
                        frames.to_vec(),
                        strategy.requires_trade_economics_details(),
                        strategy.uses_borrowed_input(),
                    )
                    .expect("原已开户观察")
                });
                calls += 1;
                if let IntentQualificationV1::Invalidated(reason) =
                    input.qualify(strategy, calculation, index, at)?
                {
                    let result = InvalidatedBatchV1 {
                        sent: vec![],
                        invalidated_intent_index: index,
                        reason,
                        observed_cutoff: input.cutoff().clone(),
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
    ) -> Result<
        QualifiedStrategyDispatchV1<S::Diagnostics>,
        StrategyDispatchFailureV1<S::Diagnostics, QualifiedBatchErrorV1<S::Error>>,
    > {
        let StrategyOutputV1::Continue { intents, .. } = &calculation.calculation.output else {
            return Ok(QualifiedStrategyDispatchV1::Dispatched {
                dispatch: StrategyDispatchV1::Exit { calculation },
                qualification_calls: 0,
            });
        };
        let sent_at_utc_ms = decision.sent_at_utc_ms();
        let cutoff = decision
            .observation_driver()
            .observation_cutoff()
            .expect("当前受信观察截止");
        let mut qualification_calls = 0;
        let result = self
            .execute_batch_qualified(
                decision,
                intents,
                Some(&cutoff),
                |execution, current_decision, index, at| {
                    let frames = if strategy.selects_relevant_markets() {
                        let mut historical = strategy.historical_market_requirements();
                        historical.extend(
                            strategy.calculation_market_requirements(&calculation.calculation),
                        );
                        execution.selected_observations(
                            current_decision.observation_driver(),
                            &historical,
                        )
                    } else {
                        current_decision.frames_at_observation()
                    };
                    let input = execution
                        .current_strategy_read(
                            frames[0].decision_cutoff.clone(),
                            frames,
                            strategy.requires_trade_economics_details(),
                            strategy.uses_borrowed_input(),
                        )
                        .expect("合法观察与已开户账本");
                    qualification_calls += 1;
                    input.qualify(strategy, &calculation.calculation, index, at)
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
                        receipts: original.expect("已匹配成功发送结果"),
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
                    Err(error) => Err(StrategyDispatchFailureV1 {
                        calculation,
                        error: QualifiedBatchErrorV1::Execution(error),
                    }),
                    Ok(_) => unreachable!("原发送错误"),
                }
            }
            Err(error) => Err(StrategyDispatchFailureV1 { calculation, error }),
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
        let current = self.ledger().funds_read_v1();
        let mut existing = BTreeSet::new();
        // 原Reserved结果是粘滞决定，不是当前资金承诺。全批检查先于任何新预留或发送。
        // 已发请求可凭原接收回执／NOE响应重读幂等结果，不要求其已转换的预留仍活动。
        for item in intents {
            for reserve in &item.reserves {
                let request = current.request(&reserve.reserve.request_identity);
                if request.is_some() {
                    existing.insert(reserve.reserve.request_identity.clone());
                }
                if request.is_some_and(|state| {
                    matches!(
                        state.reservation_state,
                        ReservationStatusV1::Released | ReservationStatusV1::Converted
                    )
                }) && !self
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
            .filter(|id| !existing.contains(id))
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
            let prepared = match intent_bridge::IntentBridge::bind_sim_group(decision)
                .prepare(item.context.clone(), &item.intent)
            {
                Ok(prepared) => prepared,
                Err(failure) => {
                    return Err(BatchExecutionErrorV1 {
                        error: SharedActionErrorV1::Sim(failure.error),
                        sent,
                        attempted_intent_index: Some(index),
                        failed_request_effect: Some(failure.effect),
                        release_errors: self.release_unsent_intents(&intents[index..], &owned),
                    }
                    .into());
                }
            };
            for reserve in &item.reserves {
                let submission = LedgerSubmissionItemIdentityV1 {
                    global_request_id: item.context.global_request_id.clone(),
                    batch_item_ordinal: reserve.batch_item_ordinal,
                };
                let id = &reserve.reserve.request_identity;
                let funds = self.ledger().funds_read_v1();
                if !batch_reserve_ids.insert(id.clone())
                    || (existing.contains(id) && funds.submission_request(&submission) != Some(id))
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
                let registration =
                    if let Some(id) = prepared.order_id(submission.batch_item_ordinal) {
                        self.record_prepared_submission_started(
                            &event,
                            &reserve.reserve.request_identity,
                            &submission,
                            &id,
                        )
                    } else {
                        self.record_submission_started(
                            &event,
                            &reserve.reserve.request_identity,
                            &submission,
                        )
                    };
                if let Err(error) = registration {
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
                .execute_prepared_with_request_effect(
                    decision,
                    item.context.clone(),
                    item.intent.clone(),
                    prepared,
                )
                .map(Some)
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
                let active = self.ledger().funds_read_v1().request(id).is_some_and(|r| {
                    matches!(
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
