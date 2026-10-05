use crate::{MaterialError, *};
use contracts::{execution_normalization::*, *};
use ledger::{AccountAdmissionPolicyV2, LedgerProjectionV2, LedgerWriterV1};
use serde::{Deserialize, Serialize};
use shared_run::*;
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2,
    latency_profile::load_embedded_proxy_latency_profile, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyDescriptorV1 {
    pub strategy_id: String,
    pub strategy_rule_version: String,
    pub effective_parameters: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalSessionV1 {
    pub run_id: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_instance_id: String,
    pub creation_request_id: String,
    pub response_bridge_binding_id: String,
    pub initial_account_state: InitialAccountStateV2,
    pub session_seed: Sha256Hex,
}
impl HistoricalSessionV1 {
    fn spec(&self) -> SessionSpecV3 {
        SessionSpecV3 {
            run_id: self.run_id.clone(),
            session_id: self.session_id.clone(),
            account_id: self.account_id.clone(),
            caller_role: SimCallerRoleV1::HistoricalBacktest,
            caller_instance_id: self.caller_instance_id.clone(),
            creation_request_id: self.creation_request_id.clone(),
            response_bridge_binding_id: self.response_bridge_binding_id.clone(),
            initial_account_state: self.initial_account_state.clone(),
            session_seed: self.session_seed.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalRunConfigV1 {
    pub preparation: HistoricalPreparationConfigV1,
    pub timing: HistoricalTimingV1,
    pub session: HistoricalSessionV1,
    pub strategy: StrategyDescriptorV1,
    pub implementation_identity: ImplementationIdentityV1,
    pub finality_policy_path: PathBuf,
    pub sim_log: PathBuf,
    pub noe_root: PathBuf,
    pub ledger_root: PathBuf,
    pub metadata_path: PathBuf,
    pub result_path: PathBuf,
    pub activation_id: String,
    pub consumption_sequence_ref: String,
    pub noe_implementation_identity: String,
    pub admission_policy: AccountAdmissionPolicyV2,
    pub max_group_bytes: usize,
    pub finite_in_flight_groups: usize,
}
/// 材料范围/来源只由已发布描述提供；每次运行另给独立账户、策略及输出。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalPreparedRunConfigV1 {
    pub timing: HistoricalTimingV1,
    pub session: HistoricalSessionV1,
    pub strategy: StrategyDescriptorV1,
    pub implementation_identity: ImplementationIdentityV1,
    pub finality_policy_path: PathBuf,
    pub sim_log: PathBuf,
    pub noe_root: PathBuf,
    pub ledger_root: PathBuf,
    pub metadata_path: PathBuf,
    pub result_path: PathBuf,
    pub activation_id: String,
    pub consumption_sequence_ref: String,
    pub noe_implementation_identity: String,
    pub admission_policy: AccountAdmissionPolicyV2,
    pub max_group_bytes: usize,
    pub finite_in_flight_groups: usize,
    /// 库默认关闭；CLI默认30秒，0关闭。输出失败不替代业务错误。
    #[serde(default)]
    pub replay_progress_interval_seconds: u64,
}
impl From<&HistoricalRunConfigV1> for HistoricalPreparedRunConfigV1 {
    fn from(c: &HistoricalRunConfigV1) -> Self {
        Self {
            timing: c.timing.clone(),
            session: c.session.clone(),
            strategy: c.strategy.clone(),
            implementation_identity: c.implementation_identity.clone(),
            finality_policy_path: c.finality_policy_path.clone(),
            sim_log: c.sim_log.clone(),
            noe_root: c.noe_root.clone(),
            ledger_root: c.ledger_root.clone(),
            metadata_path: c.metadata_path.clone(),
            result_path: c.result_path.clone(),
            activation_id: c.activation_id.clone(),
            consumption_sequence_ref: c.consumption_sequence_ref.clone(),
            noe_implementation_identity: c.noe_implementation_identity.clone(),
            admission_policy: c.admission_policy,
            max_group_bytes: c.max_group_bytes,
            finite_in_flight_groups: c.finite_in_flight_groups,
            replay_progress_interval_seconds: 0,
        }
    }
}
/// 受信内部观察消费回调；每个公共组成功应用后、下一公共组或隐藏桶前调用一次。
///
/// 可变借用用于复用 Shared 的反馈消费和当前组账户冻结，不能视作类型层只读。
/// 调用方只能消费当前观察，不得提交意图、改变时钟或主动推进来源/隐藏桶。
/// 模型暖态范围及诊断输出由调用方维护，不改变历史宿主的策略和时序。
pub trait HistoricalPublicGroupObserverV1 {
    fn observe_public_group(
        &mut self,
        shared: &mut SharedExecutionV2,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<(), MaterialError>;
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HistoricalEndReasonV1 {
    RangeEnd,
    StrategyExit,
    Failure,
}
#[derive(Clone, Debug, Serialize)]
pub struct CalculationTimingRecordV1 {
    pub observation_at_utc_ms: i64,
    pub calculation_started_at_utc_ms: i64,
    pub calculation_completed_at_utc_ms: i64,
    pub scheduled_send_at_utc_ms: i64,
    pub actual_sent_at_utc_ms: Option<i64>,
}
/// 只汇总实际公开交付，不复制逐决策账本；来源与经营阶段分别记录。
#[derive(Clone, Debug, Default, Serialize)]
pub struct HistoricalDeliveryPhaseSummaryV1 {
    pub groups: u64,
    pub initialization_source_groups: u64,
    pub research_groups_with_initialization_dependencies: u64,
    pub first_group_position: Option<u64>,
    pub last_group_position: Option<u64>,
    pub first_delivered_at_utc_ms: Option<i64>,
    pub last_delivered_at_utc_ms: Option<i64>,
}
impl HistoricalDeliveryPhaseSummaryV1 {
    fn add(&mut self, position: u64, at: i64, initial: bool, dependency: bool) {
        self.groups += 1;
        self.initialization_source_groups += u64::from(initial);
        self.research_groups_with_initialization_dependencies += u64::from(!initial && dependency);
        self.first_group_position.get_or_insert(position);
        self.first_delivered_at_utc_ms.get_or_insert(at);
        self.last_group_position = Some(position);
        self.last_delivered_at_utc_ms = Some(at);
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct HistoricalDeliverySummaryV1 {
    pub before_operating_start: HistoricalDeliveryPhaseSummaryV1,
    pub within_operating_range: HistoricalDeliveryPhaseSummaryV1,
}
/// 经济读取与未闭合对象来自同一公共状态。Exit/失败的内存读取不声称已经落盘。
#[derive(Debug, Serialize)]
pub struct SourceHealthRunSummaryV1 {
    pub qualification_calls: u64,
    pub deadline_wakeups: u64,
    pub invalidated_batches: u64,
    pub last_invalidated_calculation_timing: Option<CalculationTimingRecordV1>,
}
#[derive(Debug, Serialize)]
pub struct HistoricalRunReportV1 {
    pub end_reason: HistoricalEndReasonV1,
    pub last_observed_at_utc_ms: Option<i64>,
    pub observation_cutoff: Option<TrustedGroupDecisionCutoffV1>,
    pub recording_drained: bool,
    pub durable_noe_prefix: u64,
    pub ledger_consumed_memory: Option<LedgerProjectionV2>,
    /// 与 ledger_consumed_memory 同次读取，按 observation_cutoff 的公开观察估值。
    /// 不表示所有已观察反馈均已被 Ledger 消费或已经持久化。
    pub account_economics_memory: Option<AccountEconomicsReadV2>,
    pub conservative_nav_pusd: Option<ExecutionReadValueV1<ExactRationalV1>>,
    pub worst_case_maker_nav_pusd: Option<ExactRationalV1>,
    pub unclosed_commitments: usize,
    pub pending_noe_records: usize,
    pub pending_calculation: bool,
    pub next_required_at_utc_ms: Option<i64>,
    pub economics_version: String,
    pub last_dispatched_calculation_timing: Option<CalculationTimingRecordV1>,
    pub pending_calculation_timing: Option<CalculationTimingRecordV1>,
    pub calculation_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_health: Option<SourceHealthRunSummaryV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_summary: Option<HistoricalDeliverySummaryV1>,
}
#[derive(Debug)]
pub enum HistoricalFailureCauseV1<E> {
    Material(MaterialError),
    Sim(SimRunActorErrorV2),
    Ledger(ledger::LedgerErrorV1),
    Settlement(SharedSettlementErrorV1),
    Strategy(E),
    Dispatch(BatchExecutionErrorV1),
    Qualification {
        error: E,
        sent: Vec<SentIntentReceiptV1>,
        release_errors: Vec<ledger::LedgerErrorV1>,
    },
}
// 失败时补充既有供料上下文；输出失败不替代原 SIM 错误。
fn sim_call_failure<E>(
    call: &str,
    requested_at_utc_ms: Option<i64>,
    public_group_before_iteration: Option<u64>,
    hidden_bucket_at_utc_ms: Option<i64>,
    cause: SimRunActorErrorV2,
) -> HistoricalFailureCauseV1<E> {
    use std::io::Write;
    let context = serde_json::json!({
        "schema": "historical_sim_call_failure_v1",
        "call": call,
        "requested_at_utc_ms": requested_at_utc_ms,
        "public_group_before_iteration": public_group_before_iteration,
        "hidden_bucket_at_utc_ms": hidden_bucket_at_utc_ms,
        "error": format!("{cause:?}"),
    });
    let _ = writeln!(std::io::stderr().lock(), "{context}");
    HistoricalFailureCauseV1::Sim(cause)
}
#[derive(Debug)]
pub struct HistoricalRunFailureV1<E> {
    pub cause: HistoricalFailureCauseV1<E>,
    pub report: Option<HistoricalRunReportV1>,
}
struct Pending<D> {
    calculation: CompletedStrategyCalculationV1<D>,
    completed_at: i64,
    send_at: i64,
    record: CalculationTimingRecordV1,
    health_invalidated: bool,
}
fn assemble(
    config: &HistoricalPreparedRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    input_identity: Sha256Hex,
) -> Result<
    (
        PublicSimExecutionV1,
        SharedExecutionV2,
        MemoryNoeObservation<MeasuredCancellationEvidence>,
    ),
    MaterialError,
> {
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&std::fs::read(
        &config.finality_policy_path,
    )?)?);
    let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(
        &serde_json::to_vec(&SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
            markets: vec![],
        })?,
    )?);
    // SIM 自身原经验时延不替换为宿主计算/交付延迟。
    let latency = load_embedded_proxy_latency_profile()?;
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        run_id: config.session.run_id.clone(),
        durability_role: RunDurabilityRoleV2::FiniteHistorical,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: config.implementation_identity.clone(),
        market_event_source_identity_sha256: Some(input_identity.clone()),
    };
    let reference = |position| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!(
            "{}#input={position}",
            config.sim_log.display()
        )),
    };
    let port = MemoryNoeProjectionPortV4::new(
        &config.noe_root,
        opened.clone(),
        reference(0),
        reference(1),
        config.session.creation_request_id.clone(),
        config.noe_implementation_identity.clone(),
    );
    let observation = port.observation();
    let public = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id,
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut execution = PublicSimExecutionV1::create_new_historical(
        &config.sim_log,
        public,
        rules,
        finality,
        scenario,
        latency,
        Box::new(port),
        config.max_group_bytes,
        config.finite_in_flight_groups,
    )?;
    loop {
        match execution.create_session(config.session.spec()) {
            Err(SimRunActorErrorV2::RecordingBackpressured) => {
                execution.wait_for_recording_capacity()?
            }
            result => {
                result?;
                break;
            }
        }
    }
    let mut ledger = LedgerWriterV1::new_with_admission_v2(config.admission_policy);
    execution.bind_ledger_recording_v2(&mut ledger, &config.ledger_root)?;
    let shared = SharedExecutionV2::from_opening(ledger, observation.clone())?
        .ok_or("原开户未产生公共事实")?;
    loop {
        match execution.activate_session(config.activation_id.clone()) {
            Err(SimRunActorErrorV2::RecordingBackpressured) => {
                execution.wait_for_recording_capacity()?
            }
            result => {
                result?;
                break;
            }
        }
    }
    Ok((execution, shared, observation))
}
fn report<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>(
    driver: &FinitePublicConsumptionV1<I>,
    shared: &SharedExecutionV2,
    durable: u64,
    reason: HistoricalEndReasonV1,
    timing: Option<CalculationTimingRecordV1>,
    pending_timing: Option<CalculationTimingRecordV1>,
    count: usize,
) -> HistoricalRunReportV1 {
    let observation_cutoff = driver.observation_cutoff();
    let frames = driver.frames_at_observation();
    let economics = shared.ledger().economic_read().map(|read| {
        AccountEconomicsReadV2::from_ledger_and_observations(
            &read,
            frames.iter().map(|frame| frame.observation),
        )
    });
    HistoricalRunReportV1 {
        end_reason: reason,
        last_observed_at_utc_ms: observation_cutoff
            .as_ref()
            .map(|cutoff| cutoff.decision_time().logical_time_utc_ms()),
        observation_cutoff,
        recording_drained: false,
        durable_noe_prefix: durable,
        ledger_consumed_memory: shared.ledger().projection_v2(),
        conservative_nav_pusd: economics
            .as_ref()
            .map(|read| read.conservative_nav_pusd.clone()),
        worst_case_maker_nav_pusd: economics
            .as_ref()
            .map(|read| read.worst_case_maker_nav_pusd.clone()),
        account_economics_memory: economics,
        unclosed_commitments: shared.registry().all_commitments().count(),
        pending_noe_records: shared.pending_noe_record_count(),
        pending_calculation: pending_timing.is_some(),
        next_required_at_utc_ms: driver
            .next_required_logical_drive()
            .map(|next| next.required_at_utc_ms),
        economics_version: ACCOUNT_ECONOMICS_VERSION.into(),
        last_dispatched_calculation_timing: timing,
        pending_calculation_timing: pending_timing,
        calculation_count: count,
        source_health: None,
        delivery_summary: None,
    }
}
/// 一个账户、一个共享执行与一个策略贯穿整个显式历史范围。策略错误与原分派错误原样交回。
pub async fn run_historical_v1<S: StrategyV1>(
    config: &HistoricalRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    let prepared =
        prepare_historical_run_v1(&config.preparation).map_err(|cause| HistoricalRunFailureV1 {
            cause: HistoricalFailureCauseV1::Material(cause),
            report: None,
        })?;
    let material = PreparedHistoricalMaterialV1::from_prepared(&config.preparation, &prepared)
        .map_err(material_failure)?;
    let mut effective_config = config.clone();
    effective_config.preparation.header = config.preparation.effective_header();
    let metadata_config =
        serde_json::to_value(effective_config).map_err(|e| material_failure(e.into()))?;
    run_material(
        &HistoricalPreparedRunConfigV1::from(config),
        &material,
        prepared,
        metadata_config,
        rules,
        strategy,
        None,
    )
    .await
}
fn material_failure<E>(cause: MaterialError) -> HistoricalRunFailureV1<E> {
    HistoricalRunFailureV1 {
        cause: HistoricalFailureCauseV1::Material(cause),
        report: None,
    }
}
/// 每次独立装配SIM/共享执行/NOE/Ledger。调用者必须传入新的策略实例；不重置任意&mut Strategy。
pub async fn run_prepared_historical_v1<S: StrategyV1>(
    description_path: &std::path::Path,
    config: &HistoricalPreparedRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    run_prepared_material(description_path, config, rules, strategy, None).await
}
/// 在原历史装配和事件循环中逐组观察，包含经营范围开始前的已准备初始化组。
/// 观察错误沿原失败报告返回；不把失败封为正常结束，也不继续消费后续组。
pub async fn run_prepared_historical_with_observer_v1<S: StrategyV1>(
    description_path: &std::path::Path,
    config: &HistoricalPreparedRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
    observer: &mut dyn HistoricalPublicGroupObserverV1,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    run_prepared_material(description_path, config, rules, strategy, Some(observer)).await
}
async fn run_prepared_material<S: StrategyV1>(
    description_path: &std::path::Path,
    config: &HistoricalPreparedRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
    observer: Option<&mut dyn HistoricalPublicGroupObserverV1>,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    let material =
        PreparedHistoricalMaterialV1::read(description_path).map_err(material_failure)?;
    let root = description_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let prepared = material.resolve(root).map_err(material_failure)?;
    let metadata_config = serde_json::to_value(config).map_err(|e| material_failure(e.into()))?;
    run_material(
        config,
        &material,
        prepared,
        metadata_config,
        rules,
        strategy,
        observer,
    )
    .await
}
async fn run_material<S: StrategyV1>(
    config: &HistoricalPreparedRunConfigV1,
    material: &PreparedHistoricalMaterialV1,
    prepared: PreparedHistoricalRunV1,
    metadata_config: serde_json::Value,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
    mut observer: Option<&mut dyn HistoricalPublicGroupObserverV1>,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    let metadata = serde_json::json!({"config": metadata_config, "prepared_material": material, "source_manifests": prepared.source_manifests, "input_set_identity_sha256": prepared.input_set_identity_sha256, "source_identity_sha256": prepared.source_identity_sha256, "initialization": prepared.initialization, "public_prefix": prepared.hidden.prefix, "rules_file_sha256": rules.file_sha256(), "caller_role": SimCallerRoleV1::HistoricalBacktest, "recording_role": RunDurabilityRoleV2::FiniteHistorical, "economics_version": ACCOUNT_ECONOMICS_VERSION});
    let health_enabled = strategy.uses_source_health();
    let start = || -> Result<_, MaterialError> {
        serde_json::to_writer(std::fs::File::create(&config.metadata_path)?, &metadata)?;
        let (execution, shared, observation) =
            assemble(config, rules, prepared.source_identity_sha256.clone())?;
        let mut source = RecordedPublicGroupSourceV1::open_committed(
            &prepared.public_root,
            prepared.hidden.prefix.clone(),
            material.preparation.read_page_rows,
            material.preparation.max_read_batch_bytes,
            crate::timing::DeliveryTimes {
                delays: config.timing.public_delivery_delay_ms.clone(),
                last: None,
            },
        )?;
        if source.header() != &material.preparation.header {
            return Err("描述与公共流头不符".into());
        }
        if health_enabled {
            source.enable_source_progress();
        }
        source.set_progress(
            std::time::Duration::from_secs(config.replay_progress_interval_seconds),
            std::io::stderr(),
        );
        let hidden = RecordedHiddenBucketSourceV1::open(
            &prepared.hidden,
            &prepared.public_root,
            material.preparation.read_page_rows,
            material.preparation.max_read_batch_bytes,
        )?;
        Ok((
            FinitePublicConsumptionV1::new(
                execution,
                source,
                config.consumption_sequence_ref.clone(),
            ),
            shared,
            observation,
            hidden,
        ))
    };
    let (mut driver, mut shared, observation, mut hidden) =
        start().map_err(|cause| HistoricalRunFailureV1 {
            cause: HistoricalFailureCauseV1::Material(cause),
            report: None,
        })?;
    let mut pending: Option<Pending<S::Diagnostics>> = None;
    let mut start_at = None;
    let mut dirty = false;
    let mut calculation_index = 0;
    let mut timing = None;
    let mut evaluation_at = None;
    let mut health_at = None;
    let mut health_summary = SourceHealthRunSummaryV1 {
        qualification_calls: 0,
        deadline_wakeups: 0,
        invalidated_batches: 0,
        last_invalidated_calculation_timing: None,
    };
    let mut unconsumed_required_trigger_at = None;
    let mut coordinator = SettlementCoordinatorV1::default();
    let mut delivery_summary = HistoricalDeliverySummaryV1::default();
    let end = material.preparation.scope.received_end_utc_ms;
    // 无初始化的旧调用保持原触发行为；新调用严格以经营区间控制回调。
    let operating_start = material
        .preparation
        .metadata_initialization
        .as_ref()
        .map(|_| material.preparation.scope.received_start_utc_ms);
    let running = async {
        let mut next_hidden = hidden
            .next_bucket()
            .map_err(HistoricalFailureCauseV1::Material)?;
        loop {
            let public_group_before_iteration = driver
                .observation_cutoff()
                .map(|cutoff| cutoff.receipt().group().group_position);
            let hidden_at = next_hidden
                .as_ref()
                .map(|bucket| bucket.reference().bucket_time_utc_ms);
            let public_at = driver.next_group_at().map_err(|cause| {
                sim_call_failure(
                    "next_group_at",
                    None,
                    public_group_before_iteration,
                    hidden_at,
                    cause,
                )
            })?;
            let required = driver
                .next_required_logical_drive()
                .map(|next| next.required_at_utc_ms);
            let pending_at = pending.as_ref().map(|p| {
                if matches!(
                    p.calculation.calculation.output,
                    StrategyOutputV1::Exit { .. }
                ) || p.health_invalidated
                {
                    p.completed_at
                } else {
                    p.send_at
                }
            });
            let at = [
                public_at,
                hidden_at,
                required,
                start_at.filter(|_| pending.is_none()),
                evaluation_at.filter(|_| pending.is_none()),
                pending_at,
                health_at,
            ]
            .into_iter()
            .flatten()
            .filter(|at| *at < end)
            .min();
            let Some(at) = at else {
                break;
            };
            let sim_error = |call, requested_at, cause| {
                sim_call_failure(
                    call,
                    Some(requested_at),
                    public_group_before_iteration,
                    hidden_at,
                    cause,
                )
            };
            let operating = operating_start.is_none_or(|start| at >= start);
            if pending_at == Some(at)
                && pending.as_ref().is_some_and(|p| {
                    matches!(
                        p.calculation.calculation.output,
                        StrategyOutputV1::Exit { .. }
                    )
                })
            {
                return Ok(HistoricalEndReasonV1::StrategyExit);
            }
            let health_due = health_at == Some(at);
            if health_due {
                health_at = None;
                health_summary.deadline_wakeups += 1;
                // 到期期限要求重评；空批或仍合格的待发批完成后也须承接。
                dirty = true;
            }
            if evaluation_at == Some(at) && pending.is_none() {
                evaluation_at = None;
                dirty = true;
            }
            // 当前未消费的原驱动只通知一次；实际隐藏/义务驱动之后重新接纳触发。
            // 即时计算/发送不是消费原义务，不能在每轮分派后重新制造同一通知。
            if required != unconsumed_required_trigger_at {
                unconsumed_required_trigger_at = None;
            }
            if operating && required == Some(at) && unconsumed_required_trigger_at.is_none() {
                unconsumed_required_trigger_at = Some(at);
                dirty = true;
            }
            // 更早原义务先推进；本毫秒仍开放，全部真实公开组保持原序。
            driver
                .wait_for_recording_capacity()
                .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
            driver
                .advance_before(at)
                .map_err(|cause| sim_error("advance_before", at, cause))?;
            while driver
                .next_group_at()
                .map_err(|cause| sim_error("next_group_at", at, cause))?
                == Some(at)
            {
                let mut delivered_group = None;
                if let Some(group) = driver
                    .next_prepared_group()
                    .map_err(|cause| sim_error("next_prepared_group", at, cause))?
                {
                    if operating_start.is_some() {
                        let initial = prepared
                            .initialization
                            .group_is_initialization(&group.group.member_input_positions);
                        let dependency =
                            prepared.initialization.group_has_initialization_dependency(
                                &group.group.member_input_positions,
                                group.group.closed_at_input_position,
                            ) || group.group.observations.iter().any(|o| {
                                prepared
                                    .initialization
                                    .observation_has_initialization_dependency(o)
                            });
                        delivered_group = Some((group.group.group_position, initial, dependency));
                    }
                    for context in &group.pm_contexts {
                        if !driver.has_enrolled_market(&context.market.condition_id) {
                            driver.wait_for_recording_capacity().map_err(|cause| {
                                sim_error("wait_for_recording_capacity", at, cause)
                            })?;
                            driver
                                .enroll_market(MarketEnrollmentRequestV1 {
                                    enrollment_request_id: context.market.condition_id.0.clone(),
                                    market: prepared
                                        .markets
                                        .get(&context.market.condition_id.0)
                                        .ok_or_else(|| {
                                            HistoricalFailureCauseV1::Material(
                                                "已准备材料缺少公开市场目录项".into(),
                                            )
                                        })?
                                        .clone(),
                                })
                                .map_err(|cause| sim_error("enroll_market", at, cause))?;
                        }
                    }
                }
                driver
                    .wait_for_recording_capacity()
                    .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                driver
                    .next_group()
                    .map_err(|cause| sim_error("next_group", at, cause))?;
                if let Some((position, initial, dependency)) = delivered_group {
                    let phase = if operating {
                        &mut delivery_summary.within_operating_range
                    } else {
                        &mut delivery_summary.before_operating_start
                    };
                    phase.add(position, at, initial, dependency);
                }
                if let Some(observer) = observer.as_deref_mut() {
                    observer
                        .observe_public_group(&mut shared, &mut driver)
                        .map_err(HistoricalFailureCauseV1::Material)?;
                }
                dirty |= operating;
            }
            // 启用时以同一真实驱动器签发当前观察；不提前闭合本毫秒隐藏桶。
            let current_input = if health_enabled {
                driver
                    .observe_at(at)
                    .map_err(|cause| sim_error("observe_at", at, cause))?;
                shared
                    .strategy_current_read(&driver)
                    .map_err(HistoricalFailureCauseV1::Ledger)?
            } else {
                None
            };
            if health_enabled && let Some(input) = &current_input {
                if let Some(p) = pending.as_mut()
                    && !p.health_invalidated
                    && pending_at != Some(at)
                {
                    let (invalidated, calls) = shared
                        .revalidate_calculation(strategy, &p.calculation.calculation, input, at)
                        .map_err(HistoricalFailureCauseV1::Strategy)?;
                    health_summary.qualification_calls += calls;
                    if invalidated.is_some() {
                        p.health_invalidated = true;
                        p.completed_at = p.completed_at.max(at);
                        health_summary.invalidated_batches += 1;
                        health_summary.last_invalidated_calculation_timing = Some(p.record.clone());
                        dirty = true;
                    }
                }
                health_at = strategy
                    .next_health_evaluation_at(
                        input,
                        pending
                            .as_ref()
                            .filter(|p| !p.health_invalidated)
                            .map(|p| &p.calculation.calculation),
                        at,
                    )
                    .filter(|next| *next > at);
            }
            drop(current_input);
            // 已冻结发送先于本毫秒完整隐藏桶闭合，允许原 SIM 请求/行情竞速。
            if pending_at == Some(at) {
                let p = pending.take().expect("待完成计算");
                if matches!(
                    p.calculation.calculation.output,
                    StrategyOutputV1::Exit { .. }
                ) {
                    return Ok(HistoricalEndReasonV1::StrategyExit);
                }
                if p.health_invalidated {
                    dirty = true;
                } else {
                    driver
                        .wait_for_recording_capacity()
                        .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                    let mut decision = driver
                        .send_decision_at(p.calculation.calculation.input_cutoff.clone(), at)
                        .map_err(|cause| sim_error("send_decision_at", at, cause))?;
                    let dispatched = if health_enabled {
                        let result = shared
                            .dispatch_strategy_qualified(&mut decision, strategy, p.calculation)
                            .await;
                        match result {
                            Ok(QualifiedStrategyDispatchV1::Dispatched {
                                dispatch,
                                qualification_calls,
                            }) => {
                                health_summary.qualification_calls += qualification_calls;
                                Some(dispatch)
                            }
                            Ok(QualifiedStrategyDispatchV1::Invalidated {
                                result,
                                qualification_calls,
                                ..
                            }) => {
                                health_summary.qualification_calls += qualification_calls;
                                health_summary.invalidated_batches += 1;
                                health_summary.last_invalidated_calculation_timing =
                                    Some(p.record.clone());
                                dirty = true;
                                if !result.release_errors.is_empty() {
                                    return Err(HistoricalFailureCauseV1::Ledger(
                                        result.release_errors.into_iter().next().unwrap(),
                                    ));
                                }
                                None
                            }
                            Err(QualifiedBatchErrorV1::Execution(error)) => {
                                return Err(HistoricalFailureCauseV1::Dispatch(error));
                            }
                            Err(QualifiedBatchErrorV1::Qualification {
                                error,
                                sent,
                                release_errors,
                            }) => {
                                return Err(HistoricalFailureCauseV1::Qualification {
                                    error,
                                    sent,
                                    release_errors,
                                });
                            }
                        }
                    } else {
                        Some(
                            shared
                                .dispatch_strategy(&mut decision, strategy, p.calculation)
                                .await
                                .map_err(HistoricalFailureCauseV1::Dispatch)?,
                        )
                    };
                    if let Some(StrategyDispatchV1::Continue {
                        calculation,
                        sent_at_utc_ms,
                        ..
                    }) = dispatched
                    {
                        let mut record = p.record;
                        record.actual_sent_at_utc_ms = Some(sent_at_utc_ms);
                        timing = Some(record);
                        if let StrategyOutputV1::Continue {
                            next_evaluation_at_utc_ms,
                            ..
                        } = calculation.calculation.output
                        {
                            evaluation_at = next_evaluation_at_utc_ms.map(|next| next.max(at));
                        }
                    }
                }
            }
            if hidden_at == Some(at) {
                let bucket = next_hidden.take().expect("真实隐藏桶");
                for record in bucket.records() {
                    let market = match record {
                        HiddenExecutionRecordV1::Step { market, .. } => Some(market),
                        HiddenExecutionRecordV1::MarketFact(fact) => Some(&fact.market),
                        HiddenExecutionRecordV1::Reference(_) => None,
                    };
                    if let Some(market) = market
                        && !driver.has_enrolled_market(&market.condition_id)
                    {
                        driver
                            .wait_for_recording_capacity()
                            .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                        driver
                            .enroll_market(MarketEnrollmentRequestV1 {
                                enrollment_request_id: market.condition_id.0.clone(),
                                market: prepared
                                    .markets
                                    .get(&market.condition_id.0)
                                    .ok_or_else(|| {
                                        HistoricalFailureCauseV1::Material(
                                            "已准备材料缺少隐藏市场目录项".into(),
                                        )
                                    })?
                                    .clone(),
                            })
                            .map_err(|cause| sim_error("enroll_market", at, cause))?;
                    }
                }
                driver
                    .wait_for_recording_capacity()
                    .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                driver
                    .hidden_bucket(bucket)
                    .map_err(|cause| sim_error("hidden_bucket", at, cause))?;
                unconsumed_required_trigger_at = None;
                next_hidden = hidden
                    .next_bucket()
                    .map_err(HistoricalFailureCauseV1::Material)?;
            }
            driver
                .wait_for_recording_capacity()
                .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
            driver
                .observe_at(at)
                .map_err(|cause| sim_error("observe_at", at, cause))?;
            if operating {
                shared
                    .advance_settlement(&mut coordinator, &mut driver)
                    .map_err(HistoricalFailureCauseV1::Settlement)?;
            }
            if pending.is_none() && dirty && start_at.is_none() {
                start_at = Some(
                    crate::timing::after(
                        at,
                        config
                            .timing
                            .calculation_start_delay_ms
                            .at(calculation_index)
                            .map_err(HistoricalFailureCauseV1::Sim)?,
                    )
                    .map_err(HistoricalFailureCauseV1::Sim)?,
                );
            }
            if pending.is_none() && start_at == Some(at) {
                start_at = None;
                let input = if health_enabled {
                    shared.strategy_observation_input(&mut driver)
                } else {
                    shared.strategy_input(&mut driver)
                }
                .map_err(HistoricalFailureCauseV1::Ledger)?;
                if let Some(input) = input {
                    let calculation = input
                        .calculate(strategy, at)
                        .map_err(HistoricalFailureCauseV1::Strategy)?;
                    let observation_at = calculation
                        .input_cutoff
                        .decision_time()
                        .logical_time_utc_ms();
                    let completed_at = crate::timing::after(
                        at,
                        config
                            .timing
                            .calculation_duration_ms
                            .at(calculation_index)
                            .map_err(HistoricalFailureCauseV1::Sim)?,
                    )
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                    let send_at = crate::timing::after(
                        completed_at,
                        config
                            .timing
                            .dispatch_delay_ms
                            .at(calculation_index)
                            .map_err(HistoricalFailureCauseV1::Sim)?,
                    )
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                    if health_enabled
                        && let Some(current) = shared
                            .strategy_current_read(&driver)
                            .map_err(HistoricalFailureCauseV1::Ledger)?
                    {
                        health_at = strategy
                            .next_health_evaluation_at(&current, Some(&calculation), at)
                            .filter(|next| *next > at);
                    }
                    pending = Some(Pending {
                        calculation: calculation.complete(completed_at),
                        health_invalidated: false,
                        completed_at,
                        send_at,
                        record: CalculationTimingRecordV1 {
                            observation_at_utc_ms: observation_at,
                            calculation_started_at_utc_ms: at,
                            calculation_completed_at_utc_ms: completed_at,
                            scheduled_send_at_utc_ms: send_at,
                            actual_sent_at_utc_ms: None,
                        },
                    });
                    calculation_index += 1;
                    dirty = false;
                }
            }
            // 严格更早的原义务由下一事件证明；在本毫秒无剩余事件时推进闭合。
            let immediate_pending = pending.as_ref().is_some_and(|p| {
                p.send_at == at
                    || (p.health_invalidated && p.completed_at == at)
                    || (matches!(
                        p.calculation.calculation.output,
                        StrategyOutputV1::Exit { .. }
                    ) && p.completed_at == at)
            });
            let immediate_start =
                pending.is_none() && (start_at == Some(at) || evaluation_at == Some(at));
            if required == Some(at) && !immediate_pending && !immediate_start {
                driver
                    .advance_before(at.checked_add(1).ok_or_else(|| {
                        HistoricalFailureCauseV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                            "历史时序溢出",
                        ))
                    })?)
                    .map_err(|cause| sim_error("advance_before_due_close", at + 1, cause))?;
                unconsumed_required_trigger_at = None;
            }
        }
        driver
            .finish_input_at(end.checked_sub(1).ok_or_else(|| {
                HistoricalFailureCauseV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                    "历史范围结束时刻溢出",
                ))
            })?)
            .map_err(|cause| {
                sim_call_failure(
                    "finish_input_at",
                    Some(end - 1),
                    driver
                        .observation_cutoff()
                        .map(|cutoff| cutoff.receipt().group().group_position),
                    None,
                    cause,
                )
            })?;
        shared
            .advance_settlement(&mut coordinator, &mut driver)
            .map_err(HistoricalFailureCauseV1::Settlement)?;
        Ok(HistoricalEndReasonV1::RangeEnd)
    }
    .await;
    match running {
        Ok(reason) => {
            let normal = reason == HistoricalEndReasonV1::RangeEnd;
            let mut result = report(
                &driver,
                &shared,
                observation.durable_through(),
                reason,
                timing,
                pending.as_ref().map(|p| p.record.clone()),
                calculation_index,
            );
            result.delivery_summary = operating_start.map(|_| delivery_summary.clone());
            result.source_health = health_enabled.then_some(health_summary);
            if normal {
                driver
                    .stop_and_drain()
                    .map_err(|cause| HistoricalRunFailureV1 {
                        cause: HistoricalFailureCauseV1::Sim(cause),
                        report: None,
                    })?;
                result.recording_drained = true;
                result.durable_noe_prefix = observation.durable_through();
                serde_json::to_writer(
                    std::fs::File::create(&config.result_path).map_err(|cause| {
                        HistoricalRunFailureV1 {
                            cause: HistoricalFailureCauseV1::Material(Box::new(cause)),
                            report: None,
                        }
                    })?,
                    &result,
                )
                .map_err(|cause| HistoricalRunFailureV1 {
                    cause: HistoricalFailureCauseV1::Material(Box::new(cause)),
                    report: None,
                })?;
            }
            Ok(result)
        }
        Err(cause) => {
            let mut failure_report = report(
                &driver,
                &shared,
                observation.durable_through(),
                HistoricalEndReasonV1::Failure,
                timing,
                pending.as_ref().map(|p| p.record.clone()),
                calculation_index,
            );
            failure_report.delivery_summary = operating_start.map(|_| delivery_summary.clone());
            failure_report.source_health = health_enabled.then_some(health_summary);
            Err(HistoricalRunFailureV1 {
                cause,
                report: Some(failure_report),
            })
        }
    }
}
