//! StrategyV1 的在线宿主；公开期限补查只使用真实不可变组，不读取过去账户。
use crate::*;
use contracts::*;
use std::sync::Arc;

/// 单一计算期间保留原组 Arc；完成后立即释放，不序列化盘口或账户历史。
/// at 是该组在本宿主的真实应用时刻；期限补查不声称过去已执行过观察。
#[derive(Default)]
pub(crate) struct CalculationPublicHistoryV1 {
    groups: Vec<(Arc<ClosedBusinessGroupV1>, i64)>,
}
pub(crate) struct PublicCalculationInvalidationV1 {
    pub intent_index: usize,
    pub reason: IntentInvalidationReasonV1,
    pub due_at_utc_ms: i64,
}
impl CalculationPublicHistoryV1 {
    pub fn push(&mut self, group: Arc<ClosedBusinessGroupV1>, applied_at: i64) {
        self.groups.push((group, applied_at));
    }
    /// 检查起点、所有相邻组静默区间和末组到完成区间。相同毫秒先真实新组。
    /// 只要旧前缀上曾失效，即使后继恢复也返回首次失效。
    pub fn first_invalidation<S: StrategyV1>(
        &self,
        strategy: &S,
        calculation: &StrategyCalculationV1<S::Diagnostics>,
        completed_at: i64,
        checks: &mut u64,
    ) -> Result<Option<PublicCalculationInvalidationV1>, S::Error> {
        let StrategyOutputV1::Continue { intents, .. } = &calculation.output else {
            return Ok(None);
        };
        let selected = strategy
            .selects_relevant_markets()
            .then(|| strategy.calculation_market_requirements(calculation));
        for (position, (group, applied_at)) in self.groups.iter().enumerate() {
            let mut at = (*applied_at).max(calculation.calculation_started_at_utc_ms);
            let next_group_at = self.groups.get(position + 1).map(|(_, at)| *at);
            let end = next_group_at.unwrap_or(completed_at);
            // 同毫秒也是不同已消费前缀；每个前缀先检查一次，再考虑静默区间。
            let observations: Vec<_> = match &selected {
                Some(ids) => ids
                    .iter()
                    .filter_map(|id| group.observations.get_market(&id.0))
                    .collect(),
                None => group.observations.iter().collect(),
            };
            loop {
                for index in 0..intents.len() {
                    *checks += 1;
                    if let IntentQualificationV1::Invalidated(reason) =
                        strategy.qualify_intent_public(group, calculation, index, at)?
                    {
                        return Ok(Some(PublicCalculationInvalidationV1 {
                            intent_index: index,
                            reason,
                            due_at_utc_ms: at,
                        }));
                    }
                }
                let due = strategy
                    .next_public_health_evaluation_at(&observations, Some(calculation), at)
                    .into_iter()
                    .chain(
                        observations
                            .iter()
                            .map(|o| o.market.window.end_at_utc_ms)
                            .filter(|end| *end > at),
                    )
                    .min();
                match due.filter(|due| *due > at && *due <= end && next_group_at != Some(*due)) {
                    Some(due) => at = due,
                    None => {
                        // 生命周期等公开条件也可能在静默区间改变；不只检查来源期限。
                        let last = if next_group_at.is_some() {
                            end.saturating_sub(1)
                        } else {
                            end
                        };
                        if last > at {
                            at = last;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}

use crate::paper::{PaperError, PaperRuntimeConfigV1, assemble, enroll_markets};
use data_pipeline_training::public_online::*;
use serde::{Deserialize, Serialize};
use sim_system::{execution::*, platform_rules::*, public_consumption::*};
use std::future::Future;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperStrategyConfigV1 {
    #[serde(flatten)]
    pub runtime: PaperRuntimeConfigV1,
    /// 由启动配置冻结原统计身份及窗口；实际应用时间由持续驱动分配。
    pub statistics_input_set_identity: Sha256Hex,
    pub statistics: PublicStatisticsConfigV1,
}
#[derive(Default, Debug, Serialize)]
pub struct PaperStrategyMeasurementsV1 {
    pub applied_inputs: u64,
    pub applied_groups: u64,
    pub health_or_obligation_wakes: u64,
    pub calculations_started: u64,
    pub calculations_completed: u64,
    pub calculation_wall_ns: u128,
    pub peak_retained_public_groups: usize,
    pub retained_public_groups_at_end: usize,
    pub public_qualification_checks: u64,
    pub calculations_invalidated: u64,
    pub last_mathematical_invalid_at_utc_ms: Option<i64>,
    pub last_invalid_detected_at_utc_ms: Option<i64>,
    pub frozen_markets: u64,
    /// 由正式返回回执证明的新登记数（含失效/错误前缀）；不含当前失败未知项。
    pub actual_sent_intents: u64,
    pub peak_request_recording_groups: usize,
}
#[derive(Debug, Serialize)]
pub enum PaperStrategyEndV1 {
    StrategyExit,
    Input(PublicOnlineOutcomeV1),
}
#[derive(Debug, Serialize)]
pub struct PaperStrategyReportV1 {
    pub outcome: PaperStrategyEndV1,
    pub measurements: PaperStrategyMeasurementsV1,
    pub last_actual_observation_at_utc_ms: Option<i64>,
    pub dpt_measurements: PublicOnlineMeasurementsV1,
    pub last_received_transport_sequence: u64,
    pub sim_noe_ledger_recording_drained: bool,
    /// Exit不等待稳定；已有稳定前缀以DPT测量为准，其余尾部未证明落盘。
    pub tail_stability_unknown: bool,
    pub collector_exit: Option<i32>,
    /// 普通返回时只保存一次原owner投影；进程Exit不保证返回报告。
    pub ledger_consumed: Option<ledger::LedgerProjectionV2>,
}
/// 失败保留当前已证明前缀，不能把错误或未知尾部写成正常结束。
#[derive(Debug, Serialize)]
pub struct PaperStrategyFailureV1 {
    pub reason: String,
    pub input_outcome: Option<PaperStrategyEndV1>,
    pub measurements: PaperStrategyMeasurementsV1,
    pub last_actual_observation_at_utc_ms: Option<i64>,
    pub dpt_measurements: PublicOnlineMeasurementsV1,
    pub last_received_transport_sequence: u64,
    pub ledger_consumed: Option<ledger::LedgerProjectionV2>,
    pub tail_stability_unknown: bool,
}
impl std::fmt::Display for PaperStrategyFailureV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).map_err(|_| std::fmt::Error)?
        )
    }
}
impl std::error::Error for PaperStrategyFailureV1 {}

// 唯一本次宿主的退出通知所有权。终态/错误/取消释放后，旧计算线程不能
// 终止后续调用者。持锁调用短时通知使“已领取Exit”和“宿主退役”互斥。
struct ExitNotificationOwnerV1 {
    callback: Arc<std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>>,
}
impl ExitNotificationOwnerV1 {
    fn new(callback: Option<Box<dyn FnOnce() + Send>>) -> Self {
        Self {
            callback: Arc::new(std::sync::Mutex::new(callback)),
        }
    }
    fn retire(&self) {
        self.callback.lock().unwrap().take();
    }
}
impl Drop for ExitNotificationOwnerV1 {
    fn drop(&mut self) {
        self.retire();
    }
}

type WorkerResult<S> = (
    S,
    Result<StrategyCalculationV1<<S as StrategyV1>::Diagnostics>, <S as StrategyV1>::Error>,
    u128,
);
struct OnlineStrategyState<S: DetachedStrategyV1> {
    strategy: Option<S>,
    exit_notification: Arc<std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>>,
    worker: Option<tokio::sync::oneshot::Receiver<WorkerResult<S>>>,
    history: CalculationPublicHistoryV1,
    latest: Option<Arc<ClosedBusinessGroupV1>>,
    schedule: Option<Box<dyn StrategyPublicHealthScheduleV1>>,
    next_strategy_at: Option<i64>,
    // 原公开schedule已派生、尚未由真实观察处理的绝对期限。
    next_health_at: Option<i64>,
    needs_calculation: bool,
    measurements: PaperStrategyMeasurementsV1,
}
impl<S> OnlineStrategyState<S>
where
    S: DetachedStrategyV1 + Send + 'static,
    S::Diagnostics: Send + 'static,
    S::Error: std::fmt::Debug + std::fmt::Display + Send + 'static,
{
    fn group_applied(&mut self, group: Arc<ClosedBusinessGroupV1>, at: i64) {
        if self.worker.is_some() {
            self.history.push(group.clone(), at);
            self.measurements.peak_retained_public_groups = self
                .measurements
                .peak_retained_public_groups
                .max(self.history.groups.len());
        }
        self.latest = Some(group);
        self.measurements.applied_groups += 1;
        self.needs_calculation = true;
    }
    /// 只在原owner完成真实观察后消费期限。CPU时间变化不能覆盖未处理期限。
    fn health_observed(&mut self, driver: &ContinuousPublicConsumptionV1) {
        let Some(cutoff) = driver.observation_cutoff() else {
            return;
        };
        let at = cutoff.decision_time().logical_time_utc_ms();
        if self.next_health_at.is_some_and(|due| due <= at) {
            self.needs_calculation = true;
        }
        let frames = driver.frames_at_observation();
        let observations: Vec<_> = frames.iter().map(|frame| frame.observation).collect();
        self.next_health_at = self
            .schedule
            .as_ref()
            .and_then(|schedule| schedule.next_evaluation_at(&observations, at));
    }
    fn next_wake_after(
        &self,
        driver: &ContinuousPublicConsumptionV1,
        shared: &SharedExecutionV2,
    ) -> Option<std::time::Duration> {
        let now = driver.execution_clock_now_utc_ms();
        self.next_strategy_at
            .into_iter()
            .chain(self.next_health_at)
            .chain(
                shared
                    .next_pending_feedback_received_at_utc_ms()
                    .and_then(|at| at.checked_add(1)),
            )
            .map(|at| {
                std::time::Duration::from_millis((i128::from(at) - i128::from(now)).max(0) as u64)
            })
            .chain(driver.next_wake_after())
            .min()
    }
    fn start_calculation(
        &mut self,
        driver: &mut ContinuousPublicConsumptionV1,
        shared: &mut SharedExecutionV2,
    ) -> Result<(), PaperError> {
        if self.worker.is_some() || !self.needs_calculation || self.latest.is_none() {
            return Ok(());
        }
        let Some(strategy) = self.strategy.as_ref() else {
            return Ok(());
        };
        let now = driver.execution_clock_now_utc_ms();
        if strategy.active_until_utc_ms().is_some_and(|at| now >= at) {
            self.needs_calculation = false;
            self.next_strategy_at = None;
            return Ok(());
        }
        driver.observe()?;
        self.health_observed(driver);
        let strategy = self.strategy.as_ref().unwrap();
        let input = if strategy.selects_relevant_markets() {
            shared.strategy_selected_observation_input(
                driver,
                strategy.requires_trade_economics_details(),
                &strategy.historical_market_requirements(),
            )?
        } else {
            shared.strategy_borrowed_observation_input(
                driver,
                strategy.requires_trade_economics_details(),
            )?
        };
        let Some(input) = input else {
            return Ok(());
        };
        let input = input.freeze_for_detached();
        let at = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        self.measurements.frozen_markets += input.markets.len() as u64;
        self.history = CalculationPublicHistoryV1::default();
        self.history.push(self.latest.as_ref().unwrap().clone(), at);
        self.measurements.peak_retained_public_groups =
            self.measurements.peak_retained_public_groups.max(1);
        let mut strategy = self.strategy.take().unwrap();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // 线程只拥有策略和一次冻结，不拥有driver/Bridge/socket，不能提交业务。
        // 不用spawn_blocking，宿主Exit时runtime不必等待阻塞计算线程结束。
        let exit_notification = self.exit_notification.clone();
        #[cfg(test)]
        let completed_notice = WORKER_RESULT_SENT_NOTICE.with(|notice| notice.borrow().clone());
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let result = strategy
                .decide_detached(&input, at)
                .map(|output| StrategyCalculationV1 {
                    market_frame_timing: input.market_frame_timing.clone(),
                    input_cutoff: input.action_before.cutoff().clone(),
                    calculation_started_at_utc_ms: at,
                    output,
                });
            if matches!(
                &result,
                Ok(StrategyCalculationV1 {
                    output: StrategyOutputV1::Exit { .. },
                    ..
                })
            ) {
                let mut owner = exit_notification.lock().unwrap();
                if let Some(notify) = owner.take() {
                    // 在发送结果、报告、日志和排空之前通知组合根。
                    notify();
                }
            }
            let _ = sender.send((strategy, result, started.elapsed().as_nanos()));
            #[cfg(test)]
            if let Some(notice) = completed_notice {
                let _ = notice.send(());
            }
        });
        self.worker = Some(receiver);
        #[cfg(test)]
        RECORDING_PAUSE_AFTER_CALCULATION.with(|hook| {
            if let Some(attach) = hook.borrow_mut().take() {
                attach(driver);
            }
        });
        self.needs_calculation = false;
        self.next_strategy_at = None;
        self.measurements.calculations_started += 1;
        Ok(())
    }
    async fn calculation_finished(
        &mut self,
        result: WorkerResult<S>,
        driver: &mut ContinuousPublicConsumptionV1,
        shared: &mut SharedExecutionV2,
    ) -> Result<bool, PaperError> {
        let (mut strategy, calculation, elapsed) = result;
        self.worker = None;
        self.measurements.calculations_completed += 1;
        self.measurements.calculation_wall_ns += elapsed;
        let calculation = calculation.map_err(|e| format!("策略计算失败：{e}"))?;
        if matches!(calculation.output, StrategyOutputV1::Exit { .. }) {
            // 立即终止整个宿主；不再创建输入、业务请求或排空future。
            self.strategy = Some(strategy);
            self.history = CalculationPublicHistoryV1::default();
            return Ok(true);
        }
        let input_feedback_sequence = calculation.input_cutoff.private_feedback_sequence();
        let detected_at = driver.execution_clock_now_utc_ms();
        let invalid = self
            .history
            .first_invalidation(
                &strategy,
                &calculation,
                detected_at,
                &mut self.measurements.public_qualification_checks,
            )
            .map_err(|e| format!("公开资格读取失败：{e}"))?;
        self.history = CalculationPublicHistoryV1::default();
        self.next_strategy_at = match &calculation.output {
            StrategyOutputV1::Continue {
                next_evaluation_at_utc_ms,
                ..
            } => *next_evaluation_at_utc_ms,
            _ => None,
        };
        driver.observe()?;
        shared.apply_closed()?;
        self.health_observed(driver);
        if let Some(invalid) = invalid {
            self.measurements.calculations_invalidated += 1;
            self.measurements.last_mathematical_invalid_at_utc_ms = Some(invalid.due_at_utc_ms);
            self.measurements.last_invalid_detected_at_utc_ms = Some(detected_at);
            strategy.invalidated(
                &calculation,
                &InvalidatedBatchV1 {
                    sent: vec![],
                    invalidated_intent_index: invalid.intent_index,
                    reason: invalid.reason,
                    observed_cutoff: driver.observation_cutoff().expect("真实计算已有观察"),
                    detected_at_utc_ms: detected_at,
                    release_errors: vec![],
                },
            );
            self.needs_calculation = true;
        } else if !strategy
            .active_until_utc_ms()
            .is_some_and(|at| driver.execution_clock_now_utc_ms() >= at)
        {
            let cutoff = driver.observation_cutoff().expect("真实计算已有观察");
            let mut decision = driver.send_decision(cutoff)?;
            let completed = calculation.complete(detected_at);
            match shared
                .dispatch_strategy_qualified(&mut decision, &mut strategy, completed)
                .await
                .map_err(|error| {
                    let sent = match &error.error {
                        QualifiedBatchErrorV1::Execution(batch) => &batch.sent,
                        QualifiedBatchErrorV1::Qualification { sent, .. } => sent,
                    };
                    // 错误返回也保留已证明的新登记前缀；当前失败项效果未知不冒充已发。
                    self.measurements.actual_sent_intents +=
                        sent.iter().filter(|r| r.newly_registered).count() as u64;
                    format!("策略分派失败：{error:?}")
                })? {
                QualifiedStrategyDispatchV1::Dispatched {
                    dispatch: StrategyDispatchV1::Continue { receipts, .. },
                    ..
                } => {
                    self.measurements.actual_sent_intents +=
                        receipts.iter().filter(|r| r.newly_registered).count() as u64
                }
                QualifiedStrategyDispatchV1::Invalidated { result, .. } => {
                    self.measurements.actual_sent_intents +=
                        result.sent.iter().filter(|r| r.newly_registered).count() as u64;
                    self.needs_calculation = true;
                    self.measurements.calculations_invalidated += 1;
                }
                _ => {}
            }
        }
        // 完成/逐项分派本身也可能实际领取新反馈。结果线程优先于timer时，
        // 不丢掉这次真实变化；只比较同一owner的已领取序号，不按时间轮询。
        if driver
            .observation_cutoff()
            .is_some_and(|cutoff| cutoff.private_feedback_sequence() > input_feedback_sequence)
        {
            self.needs_calculation = true;
        }
        self.health_observed(driver);
        self.strategy = Some(strategy);
        Ok(false)
    }
}

/// 可控进度读取的必要标量；调用方决定打印/查询频率，不复制公共状态。
pub struct PaperStrategyProgressV1<'a> {
    pub phase: &'static str,
    pub elapsed: std::time::Duration,
    pub measurements: &'a PaperStrategyMeasurementsV1,
    pub calculation_in_flight: bool,
    /// 连续输入总量和剩余时间没有已知值。
    pub total_inputs: Option<u64>,
}

/// binary组合根和受控socket测试使用同一宿主。
/// StrategyExit不等待stop_and_drain；普通输入End/Stopped保留原排空边界。
/// 库返回模式需宿主线程能继续执行；同步记录等待中不保证立即返回。
/// 正式程序通过独立Exit通知入口终止进程。
pub async fn run_paper_strategy_v1<S>(
    config: &PaperStrategyConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: S,
    stop: impl Future<Output = ()>,
) -> Result<PaperStrategyReportV1, PaperError>
where
    S: DetachedStrategyV1 + Send + 'static,
    S::Diagnostics: Send + std::fmt::Debug + 'static,
    S::Error: std::fmt::Debug + std::fmt::Display + Send + 'static,
{
    run_paper_strategy_with_progress_v1(config, rules, strategy, stop, |_| {}).await
}

/// 与普通入口共用实现；进度回调必须短时返回，不拥有运行器或策略。
pub async fn run_paper_strategy_with_progress_v1<S>(
    config: &PaperStrategyConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: S,
    stop: impl Future<Output = ()>,
    progress_callback: impl FnMut(PaperStrategyProgressV1<'_>),
) -> Result<PaperStrategyReportV1, PaperError>
where
    S: DetachedStrategyV1 + Send + 'static,
    S::Diagnostics: Send + std::fmt::Debug + 'static,
    S::Error: std::fmt::Debug + std::fmt::Display + Send + 'static,
{
    run_paper_strategy_inner_v1(config, rules, strategy, stop, progress_callback, None).await
}

/// 程序组合根的退出入口。仅真正Ok Exit调用on_exit；该回调不得等待记录/报告。
/// 回调只属于本次活动宿主，终态、错误或future取消后不会由旧worker调用。
pub async fn run_paper_strategy_with_exit_v1<S>(
    config: &PaperStrategyConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: S,
    stop: impl Future<Output = ()>,
    progress_callback: impl FnMut(PaperStrategyProgressV1<'_>),
    on_exit: impl FnOnce() + Send + 'static,
) -> Result<PaperStrategyReportV1, PaperError>
where
    S: DetachedStrategyV1 + Send + 'static,
    S::Diagnostics: Send + std::fmt::Debug + 'static,
    S::Error: std::fmt::Debug + std::fmt::Display + Send + 'static,
{
    run_paper_strategy_inner_v1(
        config,
        rules,
        strategy,
        stop,
        progress_callback,
        Some(Box::new(on_exit)),
    )
    .await
}

async fn run_paper_strategy_inner_v1<S>(
    config: &PaperStrategyConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: S,
    stop: impl Future<Output = ()>,
    mut progress_callback: impl FnMut(PaperStrategyProgressV1<'_>),
    on_exit: Option<Box<dyn FnOnce() + Send>>,
) -> Result<PaperStrategyReportV1, PaperError>
where
    S: DetachedStrategyV1 + Send + 'static,
    S::Diagnostics: Send + std::fmt::Debug + 'static,
    S::Error: std::fmt::Debug + std::fmt::Display + Send + 'static,
{
    let exit_owner = ExitNotificationOwnerV1::new(on_exit);
    let started = std::time::Instant::now();
    let statistics = data_pipeline_core::public_statistics::PublicStatisticsStreamV1::new(
        PublicStatisticsIdentityV1 {
            input_set_identity: config.statistics_input_set_identity.clone(),
            stream_header: config.runtime.public_online.header.clone(),
            rules_identity: rules.file_sha256().clone(),
            delivery_policy: "paper-actual-utc-monotonic-application-v1".into(),
        },
        config.statistics.clone(),
    )?;
    let preparing = PaperStrategyMeasurementsV1::default();
    progress_callback(PaperStrategyProgressV1 {
        phase: "准备唯一账户与原记录器",
        elapsed: started.elapsed(),
        measurements: &preparing,
        calculation_in_flight: false,
        total_inputs: None,
    });
    let (mut driver, mut shared) = assemble(
        &config.runtime,
        rules,
        Some(GammaResolutionPolicyV1::SourceTimeOnly),
    )?;
    driver.enable_public_statistics(statistics)?;
    let mut progress = PublicOnlineProgressV1::default();
    let mut online = config.runtime.public_online.clone();
    online.source_progress |= strategy.requires_source_progress() || strategy.uses_source_health();
    let schedule = strategy.public_health_schedule();
    let mut state = OnlineStrategyState {
        strategy: Some(strategy),
        exit_notification: exit_owner.callback.clone(),
        worker: None,
        history: CalculationPublicHistoryV1::default(),
        latest: None,
        schedule,
        next_strategy_at: None,
        next_health_at: None,
        needs_calculation: false,
        measurements: Default::default(),
    };
    let running = async {
    let stream = tokio::net::UnixStream::connect(&config.runtime.socket_path).await?;
    let mut session =
        PublicOnlineSessionV1::start(stream, &config.runtime.dpt_root, online, &mut progress)
            .await?;
    tokio::pin!(stop);
    let outcome = 'input: loop {
        progress_callback(PaperStrategyProgressV1 {
            phase: "等待输入与必要唤醒",
            elapsed: started.elapsed(),
            measurements: &state.measurements,
            calculation_in_flight: state.worker.is_some(),
            total_inputs: None,
        });
        let next = session.next_applied(&mut stop);
        tokio::pin!(next);
        let next = loop {
            let delay = state.next_wake_after(&driver, &shared);
            let timer = async {
                match delay {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending().await,
                }
            };
            let worker = async {
                match &mut state.worker {
                    Some(receiver) => receiver
                        .await
                        .map_err(|e| format!("策略计算线程未返回：{e}")),
                    None => std::future::pending().await,
                }
            };
            tokio::select! { biased;
                result = worker => {
                    progress_callback(PaperStrategyProgressV1 { phase: "领取计算结果、同步反馈与逐项分派", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                    if state.calculation_finished(result?, &mut driver, &mut shared).await? {
                        break 'input PaperStrategyEndV1::StrategyExit;
                    }
                    progress_callback(PaperStrategyProgressV1 { phase: "同步必要冻结并启动单一计算", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                    state.start_calculation(&mut driver, &mut shared)?;
                    progress_callback(PaperStrategyProgressV1 { phase: "处理计算结果后等待", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                }
                next = &mut next => break next?,
                _ = timer => {
                    progress_callback(PaperStrategyProgressV1 { phase: "推进原必要义务与当前观察", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                    state.measurements.health_or_obligation_wakes += 1;
                    driver.advance_due()?;
                    driver.observe()?;
                    shared.apply_closed()?;
                    state.health_observed(&driver);
                    let now = driver.execution_clock_now_utc_ms();
                    if state.next_strategy_at.is_some_and(|at| at <= now) { state.next_strategy_at = None; }
                    state.needs_calculation = true;
                    progress_callback(PaperStrategyProgressV1 { phase: "同步必要冻结并启动单一计算", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                    state.start_calculation(&mut driver, &mut shared)?;
                    progress_callback(PaperStrategyProgressV1 { phase: "处理必要唤醒后等待", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                }
            }
        };
        match next {
            PublicOnlineNextV1::Stopped(session) => {
                exit_owner.retire();
                progress_callback(PaperStrategyProgressV1 { phase: "停止输入并排空DPT已接纳记录", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });

                break PaperStrategyEndV1::Input(PublicOnlineOutcomeV1::Stopped(
                    session.stop_and_drain().await?,
                ));
            }
            PublicOnlineNextV1::Applied(mut pending) => {
                for group in PreparedPublicGroupV1::from_online(&mut pending) {
                    progress_callback(PaperStrategyProgressV1 { phase: "登记并应用公共组及同步反馈", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                    enroll_markets(&mut driver, &group)?;
                    let (receipt, _) = driver.apply_group(group.clone())?;
                    shared.prepare_action_group(&mut driver)?;
                    state.group_applied(
                        group.group.clone(),
                        receipt.execution_time().logical_time_utc_ms(),
                    );
                    state.health_observed(&driver);
                }
                // Record→confirm保留同一future。唯有整个宿主Exit/错误可终止它，
                // 普通计算结果不能取消该段后继续session或提前发送。
                state.measurements.applied_inputs += 1;
                progress_callback(PaperStrategyProgressV1 {
                    phase: "记录并确认已应用输入",
                    elapsed: started.elapsed(),
                    measurements: &state.measurements,
                    calculation_in_flight: state.worker.is_some(),
                    total_inputs: None,
                });
                let complete = pending.complete();
                tokio::pin!(complete);
                let mut finished = None;
                let completed = if let Some(receiver) = &mut state.worker {
                    tokio::select! { biased;
                        result = receiver => {
                            let result = result.map_err(|e| format!("策略计算线程未返回：{e}"))?;
                            if matches!(&result.1, Ok(StrategyCalculationV1 { output: StrategyOutputV1::Exit { .. }, .. })) {
                                progress_callback(PaperStrategyProgressV1 { phase: "处理真实Exit计算结果", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                                state.calculation_finished(result, &mut driver, &mut shared).await?;
                                break 'input PaperStrategyEndV1::StrategyExit;
                            }
                            finished = Some(result);
                            complete.await?
                        }
                        completed = &mut complete => completed?,
                    }
                } else {
                    complete.await?
                };
                match completed {
                    PublicOnlineCompletedInputV1::Continue(next) => session = next,
                    PublicOnlineCompletedInputV1::End(end) => {
                        exit_owner.retire();
                        break PaperStrategyEndV1::Input(PublicOnlineOutcomeV1::End(end));
                    }
                }
                if let Some(result) = finished {
                    progress_callback(PaperStrategyProgressV1 { phase: "领取计算结果、同步反馈与逐项分派", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                    if state
                        .calculation_finished(result, &mut driver, &mut shared)
                        .await?
                    {
                        break 'input PaperStrategyEndV1::StrategyExit;
                    }
                }
                progress_callback(PaperStrategyProgressV1 { phase: "同步必要冻结并启动单一计算", elapsed: started.elapsed(), measurements: &state.measurements, calculation_in_flight: state.worker.is_some(), total_inputs: None });
                state.start_calculation(&mut driver, &mut shared)?;
            }
        }
    };
    Ok::<_, PaperError>(outcome)
    }.await;
    exit_owner.retire();
    let last_at = driver
        .observation_cutoff()
        .map(|c| c.decision_time().logical_time_utc_ms());
    let outcome = match running {
        Ok(outcome) => outcome,
        Err(error) => {
            let error = progress.failure(error);
            return Err(Box::new(PaperStrategyFailureV1 {
                reason: error.to_string(),
                input_outcome: None,
                measurements: state.measurements,
                last_actual_observation_at_utc_ms: last_at,
                dpt_measurements: progress.measurements,
                last_received_transport_sequence: progress.last_received_transport_sequence,
                ledger_consumed: shared.ledger().projection_v2(),
                tail_stability_unknown: true,
            }));
        }
    };
    let exit = matches!(outcome, PaperStrategyEndV1::StrategyExit);
    state.measurements.peak_request_recording_groups = driver.request_recording_peak_groups();
    state.measurements.retained_public_groups_at_end = state.history.groups.len();
    if !exit {
        progress_callback(PaperStrategyProgressV1 {
            phase: "排空SIM、NOE和Ledger已接纳记录",
            elapsed: started.elapsed(),
            measurements: &state.measurements,
            calculation_in_flight: state.worker.is_some(),
            total_inputs: None,
        });
        if let Err(error) = driver.stop_and_drain() {
            return Err(Box::new(PaperStrategyFailureV1 {
                reason: error.to_string(),
                input_outcome: Some(outcome),
                measurements: state.measurements,
                last_actual_observation_at_utc_ms: last_at,
                dpt_measurements: progress.measurements,
                last_received_transport_sequence: progress.last_received_transport_sequence,
                ledger_consumed: shared.ledger().projection_v2(),
                tail_stability_unknown: true,
            }));
        }
    }
    // Exit只释放后台所有权，不join未完成计算或记录；程序组合根负责立即返回退出。
    Ok(PaperStrategyReportV1 {
        outcome,
        measurements: state.measurements,
        last_actual_observation_at_utc_ms: last_at,
        dpt_measurements: progress.measurements,
        last_received_transport_sequence: progress.last_received_transport_sequence,
        sim_noe_ledger_recording_drained: !exit,
        tail_stability_unknown: exit,
        collector_exit: None,
        ledger_consumed: shared.ledger().projection_v2(),
    })
}

#[cfg(test)]
mod exit_notification_tests {
    use super::*;
    #[test]
    fn retired_host_cannot_receive_old_worker_exit() {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = hits.clone();
        let owner = ExitNotificationOwnerV1::new(Some(Box::new(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })));
        let worker = owner.callback.clone();
        drop(owner);
        let mut slot = worker.lock().unwrap();
        if let Some(notify) = slot.take() {
            notify();
        }
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
thread_local! {
    static WORKER_RESULT_SENT_NOTICE: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> = const { std::cell::RefCell::new(None) };
    static RECORDING_PAUSE_AFTER_CALCULATION: std::cell::RefCell<Option<Box<dyn FnOnce(&mut ContinuousPublicConsumptionV1)>>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
#[path = "../tests/support/paper_exit_wait.rs"]
mod paper_exit_wait;
