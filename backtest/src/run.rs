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
    /// 日作业将策略的固定请求命名空间纳入恢复身份；旧入口由原命令提供。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_request_namespace: Option<String>,
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
            strategy_request_namespace: None,
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HistoricalEndReasonV1 {
    RangeEnd,
    StrategyExit,
    BudgetStop,
    Failure,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CalculationTimingRecordV1 {
    pub observation_at_utc_ms: i64,
    pub calculation_started_at_utc_ms: i64,
    pub calculation_completed_at_utc_ms: i64,
    pub scheduled_send_at_utc_ms: i64,
    pub actual_sent_at_utc_ms: Option<i64>,
}
/// 只汇总实际公开交付，不复制逐决策账本；来源与经营阶段分别记录。
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HistoricalDeliverySummaryV1 {
    pub before_operating_start: HistoricalDeliveryPhaseSummaryV1,
    pub within_operating_range: HistoricalDeliveryPhaseSummaryV1,
}
/// 经济读取与未闭合对象来自同一公共状态。Exit/失败的内存读取不声称已经落盘。
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceHealthRunSummaryV1 {
    pub qualification_calls: u64,
    pub deadline_wakeups: u64,
    pub invalidated_batches: u64,
    pub last_invalidated_calculation_timing: Option<CalculationTimingRecordV1>,
}
#[derive(Debug, Serialize)]
pub struct HistoricalRunReportV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub research: Option<crate::research_summary::ResearchSummaryV1>,
    pub day_economics: HistoricalDayEconomicsSummaryV1,
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
#[derive(Serialize, Deserialize)]
pub(crate) struct Pending<D> {
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
    day_binding: Option<HistoricalDayLogBindingV1>,
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
    let mut execution = match day_binding {
        Some(binding) => PublicSimExecutionV1::create_new_historical_day(
            &config.sim_log,
            public,
            rules,
            finality,
            scenario,
            latency,
            Box::new(port),
            config.max_group_bytes,
            config.finite_in_flight_groups,
            binding,
        ),
        None => PublicSimExecutionV1::create_new_historical(
            &config.sim_log,
            public,
            rules,
            finality,
            scenario,
            latency,
            Box::new(port),
            config.max_group_bytes,
            config.finite_in_flight_groups,
        ),
    }?;
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
fn sample_day_economics<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>(
    driver: &FinitePublicConsumptionV1<I>,
    shared: &SharedExecutionV2,
    at: i64,
    state: &mut crate::day_economics::DayEconomicsStateV1,
) {
    if let Some(read) = shared.ledger().economic_read() {
        let frames = driver.frames_at_observation();
        let economics = AccountEconomicsReadV2::from_ledger_and_observations(
            &read,
            frames.iter().map(|f| f.observation),
        );
        state.sample(at, Some((&read, &economics)));
    } else {
        state.sample(at, None);
    }
}
fn research_snapshot<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>(
    driver: &FinitePublicConsumptionV1<I>,
    shared: &SharedExecutionV2,
    boundary: i64,
    health: &SourceHealthRunSummaryV1,
    pending: Option<CalculationTimingRecordV1>,
    day: &crate::day_economics::DayEconomicsStateV1,
) -> crate::research_summary::ResearchSnapshotV1 {
    let frames = driver.frames_at_observation();
    let economics = shared.ledger().economic_read().map(|read| {
        AccountEconomicsReadV2::from_ledger_and_observations(
            &read,
            frames.iter().map(|f| f.observation),
        )
    });
    crate::research_summary::snapshot(
        boundary,
        driver.observation_cutoff(),
        shared,
        economics.as_ref(),
        health,
        pending,
        day,
    )
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
        research: None,
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
        day_economics: crate::day_economics::DayEconomicsStateV1::default().summary(),
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
    run_prepared_material(description_path, config, rules, strategy).await
}
async fn run_prepared_material<S: StrategyV1>(
    description_path: &std::path::Path,
    config: &HistoricalPreparedRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
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
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    let mut source = RecordedPublicGroupSourceV1::open_committed(
        &prepared.public_root,
        prepared.hidden.prefix.clone(),
        material.preparation.read_page_rows,
        material.preparation.max_read_batch_bytes,
        crate::timing::DeliveryTimes {
            delays: config.timing.public_delivery_delay_ms.clone(),
            last: None,
        },
    )
    .map_err(material_failure)?;
    if source.header() != &material.preparation.header {
        return Err(material_failure("描述与公共流头不符".into()));
    }
    if strategy.uses_source_health() {
        source.enable_source_progress();
    }
    source.set_progress(
        std::time::Duration::from_secs(config.replay_progress_interval_seconds),
        std::io::stderr(),
    );
    let mut hidden = RecordedHiddenBucketSourceV1::open(
        &prepared.hidden,
        &prepared.public_root,
        material.preparation.read_page_rows,
        material.preparation.max_read_batch_bytes,
    )
    .map_err(material_failure)?;
    run_material_with_sources(
        config,
        material,
        prepared,
        metadata_config,
        rules,
        strategy,
        source,
        std::iter::from_fn(move || hidden.next_bucket().transpose()),
    )
    .await
}
/// 原账户循环接收已准备的公共组与完整隐藏桶；不另建回测、撮合或账本循环。
pub(crate) async fn run_material_with_sources<S, I, H>(
    config: &HistoricalPreparedRunConfigV1,
    material: &PreparedHistoricalMaterialV1,
    prepared: PreparedHistoricalRunV1,
    metadata_config: serde_json::Value,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
    source: I,
    hidden: H,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>>
where
    S: StrategyV1,
    I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>,
    H: Iterator<
        Item = Result<sim_system::public_consumption::AcceptedHiddenStepBucketV1, MaterialError>,
    >,
{
    let metadata = serde_json::json!({"config": metadata_config, "prepared_material": material, "source_manifests": prepared.source_manifests, "input_set_identity_sha256": prepared.input_set_identity_sha256, "source_identity_sha256": prepared.source_identity_sha256, "initialization": prepared.initialization, "public_prefix": prepared.hidden.prefix, "rules_file_sha256": rules.file_sha256(), "maker_model_version": rules.maker_model_version_for(SimCallerRoleV1::HistoricalBacktest), "caller_role": SimCallerRoleV1::HistoricalBacktest, "recording_role": RunDurabilityRoleV2::FiniteHistorical, "economics_version": ACCOUNT_ECONOMICS_VERSION});
    let context = HistoricalRuntimeContextV1 {
        start: material.preparation.scope.received_start_utc_ms,
        source_identity: prepared.source_identity_sha256,
        day_log_binding: None,
        end: material.preparation.scope.received_end_utc_ms,
        operating_start: material
            .preparation
            .metadata_initialization
            .as_ref()
            .map(|_| material.preparation.scope.received_start_utc_ms),
        catalog: Arc::new(std::sync::RwLock::new(HistoricalRuntimeCatalogV1 {
            markets: prepared.markets,
            initialization: prepared.initialization,
        })),
    };
    run_context_with_sources(config, context, metadata, rules, strategy, source, hidden).await
}
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct HistoricalRuntimeCatalogV1 {
    pub markets: std::collections::BTreeMap<String, PlatformMarketV1>,
    pub initialization: HistoricalInitializationProvenanceV1,
}
pub(crate) struct HistoricalRuntimeContextV1 {
    pub start: i64,
    pub source_identity: Sha256Hex,
    pub day_log_binding: Option<HistoricalDayLogBindingV1>,
    pub end: i64,
    pub operating_start: Option<i64>,
    pub catalog: Arc<std::sync::RwLock<HistoricalRuntimeCatalogV1>>,
}
pub(crate) async fn run_context_with_sources<S, I, H>(
    config: &HistoricalPreparedRunConfigV1,
    context: HistoricalRuntimeContextV1,
    metadata: serde_json::Value,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
    source: I,
    hidden: H,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>>
where
    S: StrategyV1,
    I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>,
    H: Iterator<Item = Result<AcceptedHiddenStepBucketV1, MaterialError>>,
{
    run_context_with_hook(
        config,
        context,
        metadata,
        rules,
        strategy,
        source,
        hidden,
        NoRuntimeCheckpointV1,
    )
    .await
}
pub(crate) async fn run_context_with_hook<S, I, H, C>(
    config: &HistoricalPreparedRunConfigV1,
    context: HistoricalRuntimeContextV1,
    metadata: serde_json::Value,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
    source: I,
    mut hidden: H,
    mut checkpoint: C,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>>
where
    S: StrategyV1,
    I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>,
    H: Iterator<Item = Result<AcceptedHiddenStepBucketV1, MaterialError>>,
    C: RuntimeCheckpointV1<S, I>,
{
    #[cfg(feature = "stage-timing")]
    let mut stages =
        crate::stage_timing::StageTimings::new(format!("candidate:{}", config.session.run_id));
    let health_enabled = strategy.uses_source_health();
    let resumed = checkpoint
        .restore(config, rules.clone(), strategy)
        .map_err(material_failure)?;
    let mut saved_loop = None;
    let start = || -> Result<_, MaterialError> {
        serde_json::to_writer(std::fs::File::create(&config.metadata_path)?, &metadata)?;
        if let Some((state, shared, observation, saved)) = resumed {
            saved_loop = Some(saved);
            Ok((
                FinitePublicConsumptionV1::from_checkpoint_state(state, source),
                shared,
                observation,
            ))
        } else {
            let (execution, shared, observation) = assemble(
                config,
                rules,
                context.source_identity.clone(),
                context.day_log_binding.clone(),
            )?;
            Ok((
                FinitePublicConsumptionV1::new(
                    execution,
                    source,
                    config.consumption_sequence_ref.clone(),
                ),
                shared,
                observation,
            ))
        }
    };
    let (mut driver, mut shared, observation) =
        measure_stage!(stages, "runtime_open_and_account_assembly", start()).map_err(|cause| {
            HistoricalRunFailureV1 {
                cause: HistoricalFailureCauseV1::Material(cause),
                report: None,
            }
        })?;
    driver
        .enable_finite_recording_wait()
        .map_err(|cause| HistoricalRunFailureV1 {
            cause: HistoricalFailureCauseV1::Sim(cause),
            report: None,
        })?;
    checkpoint.started();
    let fresh = saved_loop.is_none();
    let RuntimeLoopCheckpointV1 {
        mut pending,
        mut start_at,
        mut dirty,
        mut calculation_index,
        mut timing,
        mut evaluation_at,
        mut health_at,
        mut health_summary,
        mut unconsumed_required_trigger_at,
        mut delivery_summary,
        mut day_economics,
        mut research,
    } = saved_loop.unwrap_or_default();
    let end = context.end;
    let operating_start = context.operating_start;
    let economics_start = context.start.max(operating_start.unwrap_or(context.start));
    if fresh {
        sample_day_economics(&driver, &shared, economics_start, &mut day_economics);
    }
    if research.is_none() && checkpoint.research_enabled() {
        let baseline_at = driver.observation_cutoff().map_or(economics_start, |c| {
            c.decision_time().logical_time_utc_ms().max(economics_start)
        });
        let baseline = research_snapshot(
            &driver,
            &shared,
            baseline_at,
            &health_summary,
            pending.as_ref().map(|p| p.record.clone()),
            &day_economics,
        );
        research = Some(crate::research_summary::ResearchStateV1::new(
            baseline, fresh,
        ));
    }
    let running = async {
        let mut next_hidden = checkpoint
            .restored_hidden()
            .or_else(|| measure_stage!(stages, "hidden_source_wait_and_acquisition", hidden.next()))
            .transpose()
            .map_err(HistoricalFailureCauseV1::Material)?;
        loop {
            let public_group_before_iteration = driver
                .observation_cutoff()
                .map(|cutoff| cutoff.receipt().group().group_position);
            let hidden_at = next_hidden
                .as_ref()
                .map(|bucket| bucket.reference().bucket_time_utc_ms());
            let public_at = measure_stage!(
                stages,
                "sim_next_group_at_including_source_wait",
                driver.next_group_at()
            )
            .map_err(|cause| {
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
                // 无事件小时先读取当前已完成事实，再执行原范围尾部。
                if let Some(summary) = &mut research {
                    while summary.next_boundary < end {
                        let boundary = summary.next_boundary;
                        day_economics.finish_duration(boundary);
                        summary.push(research_snapshot(
                            &driver,
                            &shared,
                            boundary,
                            &health_summary,
                            pending.as_ref().map(|p| p.record.clone()),
                            &day_economics,
                        ));
                        #[cfg(feature = "stage-timing")]
                        stages.emit_hour(boundary);
                    }
                }
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
            if let Some(summary) = &mut research {
                while summary.next_boundary <= at {
                    let boundary = summary.next_boundary;
                    day_economics.finish_duration(boundary);
                    summary.push(research_snapshot(
                        &driver,
                        &shared,
                        boundary,
                        &health_summary,
                        pending.as_ref().map(|p| p.record.clone()),
                        &day_economics,
                    ));
                    #[cfg(feature = "stage-timing")]
                    stages.emit_hour(boundary);
                }
            }
            let operating = operating_start.is_none_or(|start| at >= start);
            if pending_at == Some(at)
                && pending.as_ref().is_some_and(|p| {
                    matches!(
                        p.calculation.calculation.output,
                        StrategyOutputV1::Exit { .. }
                    )
                })
            {
                sample_day_economics(&driver, &shared, at, &mut day_economics);
                return Ok((HistoricalEndReasonV1::StrategyExit, at));
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
            measure_stage!(
                stages,
                "recording_stabilization_wait",
                driver.wait_for_recording_capacity()
            )
            .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
            measure_stage!(
                stages,
                "sim_advance_before_including_noe",
                driver.advance_before(at)
            )
            .map_err(|cause| sim_error("advance_before", at, cause))?;
            while measure_stage!(
                stages,
                "sim_next_group_at_including_source_wait",
                driver.next_group_at()
            )
            .map_err(|cause| sim_error("next_group_at", at, cause))?
                == Some(at)
            {
                let mut delivered_group = None;
                let prepared_group = driver
                    .next_prepared_group()
                    .map_err(|cause| sim_error("next_prepared_group", at, cause))?;
                if let Some(group) = &prepared_group {
                    if operating_start.is_some() {
                        let catalog = context.catalog.read().unwrap();
                        let initial = catalog
                            .initialization
                            .group_is_initialization(&group.group.member_input_positions);
                        let dependency =
                            catalog.initialization.group_has_initialization_dependency(
                                &group.group.member_input_positions,
                                group.group.closed_at_input_position,
                            ) || group.group.observations.iter().any(|o| {
                                catalog
                                    .initialization
                                    .observation_has_initialization_dependency(o)
                            });
                        delivered_group = Some((group.group.group_position, initial, dependency));
                    }
                    for pm_context in &group.pm_contexts {
                        if !driver.has_enrolled_market(&pm_context.market.condition_id) {
                            measure_stage!(
                                stages,
                                "recording_stabilization_wait",
                                driver.wait_for_recording_capacity()
                            )
                            .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                            driver
                                .enroll_market(MarketEnrollmentRequestV1 {
                                    enrollment_request_id: pm_context.market.condition_id.0.clone(),
                                    market: context
                                        .catalog
                                        .read()
                                        .unwrap()
                                        .markets
                                        .get(&pm_context.market.condition_id.0)
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
                measure_stage!(
                    stages,
                    "recording_stabilization_wait",
                    driver.wait_for_recording_capacity()
                )
                .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                let applied =
                    measure_stage!(stages, "sim_next_group_including_noe", driver.next_group())
                        .map_err(|cause| sim_error("next_group", at, cause))?;
                // 每个合法组都检查，包括发送恰好到期的同毫秒；恢复不能复活旧决定。
                if health_enabled
                    && let Some(p) = pending.as_mut()
                    && !p.health_invalidated
                    && let Some(group) = &prepared_group
                    && let Some((receipt, cutoff)) = applied
                    && let StrategyOutputV1::Continue { intents, .. } =
                        &p.calculation.calculation.output
                {
                    let detected_at = receipt.execution_time().logical_time_utc_ms();
                    for index in 0..intents.len() {
                        health_summary.qualification_calls += 1;
                        if let IntentQualificationV1::Invalidated(reason) = measure_stage!(
                            stages,
                            "public_qualification",
                            strategy.qualify_intent_public(
                                &group.group,
                                &p.calculation.calculation,
                                index,
                                detected_at,
                            )
                        )
                        .map_err(HistoricalFailureCauseV1::Strategy)?
                        {
                            let result = InvalidatedBatchV1 {
                                sent: vec![],
                                invalidated_intent_index: index,
                                reason,
                                observed_cutoff: cutoff,
                                detected_at_utc_ms: detected_at,
                                release_errors: vec![],
                            };
                            strategy.invalidated(&p.calculation.calculation, &result);
                            p.health_invalidated = true;
                            p.completed_at = p.completed_at.max(detected_at);
                            health_summary.invalidated_batches += 1;
                            health_summary.last_invalidated_calculation_timing =
                                Some(p.record.clone());
                            dirty = true;
                            break;
                        }
                    }
                }
                if let Some((position, initial, dependency)) = delivered_group {
                    let phase = if operating {
                        &mut delivery_summary.within_operating_range
                    } else {
                        &mut delivery_summary.before_operating_start
                    };
                    phase.add(position, at, initial, dependency);
                }
                dirty |= operating;
            }
            // 启用时以同一真实驱动器签发当前观察；不提前闭合本毫秒隐藏桶。
            let current_input = if health_enabled {
                driver
                    .observe_at(at)
                    .map_err(|cause| sim_error("observe_at", at, cause))?;
                measure_stage!(
                    stages,
                    "current_public_observation_assembly",
                    shared.strategy_current_observations(&driver)
                )
                .map_err(HistoricalFailureCauseV1::Ledger)?
            } else {
                Vec::new()
            };
            if health_enabled && !current_input.is_empty() {
                if let Some(p) = pending.as_mut()
                    && !p.health_invalidated
                    && pending_at != Some(at)
                {
                    let (invalidated, calls) = measure_stage!(
                        stages,
                        "revalidation_including_current_read_and_full_qualification",
                        shared.revalidate_calculation(
                            strategy,
                            &p.calculation.calculation,
                            &current_input,
                            at,
                        )
                    )
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
                health_at = measure_stage!(
                    stages,
                    "next_health_evaluation_including_dependency_lookup",
                    strategy.next_health_evaluation_at(
                        &current_input,
                        pending
                            .as_ref()
                            .filter(|p| !p.health_invalidated)
                            .map(|p| &p.calculation.calculation),
                        at,
                    )
                )
                .filter(|next| *next > at);
            }
            drop(current_input);
            // 公开组可令已算完的批次立即到期，不能沿用循环开头的旧发送时间。
            let pending_due = pending.as_ref().is_some_and(|p| {
                if p.health_invalidated {
                    p.completed_at == at
                } else {
                    p.send_at == at
                }
            });
            // 已冻结发送先于本毫秒完整隐藏桶闭合，允许原 SIM 请求/行情竞速。
            if pending_due {
                let p = pending.take().expect("待完成计算");
                if matches!(
                    p.calculation.calculation.output,
                    StrategyOutputV1::Exit { .. }
                ) {
                    return Ok((HistoricalEndReasonV1::StrategyExit, at));
                }
                if p.health_invalidated {
                    dirty = true;
                } else {
                    measure_stage!(
                        stages,
                        "recording_stabilization_wait",
                        driver.wait_for_recording_capacity()
                    )
                    .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                    let mut decision = driver
                        .send_decision_at(p.calculation.calculation.input_cutoff.clone(), at)
                        .map_err(|cause| sim_error("send_decision_at", at, cause))?;
                    let dispatched = if health_enabled {
                        let result = measure_stage!(
                            stages,
                            "dispatch_including_reserve_full_qualification_bridge_sim",
                            shared
                                .dispatch_strategy_qualified(&mut decision, strategy, p.calculation)
                                .await
                        );
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
                            measure_stage!(
                                stages,
                                "dispatch_including_reserve_bridge_sim",
                                shared
                                    .dispatch_strategy(&mut decision, strategy, p.calculation)
                                    .await
                            )
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
                        HiddenExecutionRecordV1::Reference(_)
                        | HiddenExecutionRecordV1::BinanceDepthReference { .. } => None,
                    };
                    if let Some(market) = market
                        && !driver.has_enrolled_market(&market.condition_id)
                    {
                        measure_stage!(
                            stages,
                            "recording_stabilization_wait",
                            driver.wait_for_recording_capacity()
                        )
                        .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                        driver
                            .enroll_market(MarketEnrollmentRequestV1 {
                                enrollment_request_id: market.condition_id.0.clone(),
                                market: context
                                    .catalog
                                    .read()
                                    .unwrap()
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
                measure_stage!(
                    stages,
                    "recording_stabilization_wait",
                    driver.wait_for_recording_capacity()
                )
                .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
                measure_stage!(
                    stages,
                    "sim_hidden_bucket_including_noe",
                    driver.hidden_bucket(bucket)
                )
                .map_err(|cause| sim_error("hidden_bucket", at, cause))?;
                unconsumed_required_trigger_at = None;
                next_hidden =
                    measure_stage!(stages, "hidden_source_wait_and_acquisition", hidden.next())
                        .transpose()
                        .map_err(HistoricalFailureCauseV1::Material)?;
            }
            measure_stage!(
                stages,
                "recording_stabilization_wait",
                driver.wait_for_recording_capacity()
            )
            .map_err(|cause| sim_error("wait_for_recording_capacity", at, cause))?;
            driver
                .observe_at(at)
                .map_err(|cause| sim_error("observe_at", at, cause))?;
            if operating {
                measure_stage!(
                    stages,
                    "apply_closed_including_noe_claim_and_ledger",
                    shared.apply_closed()
                )
                .map_err(HistoricalFailureCauseV1::Ledger)?;
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
                let calculation = if health_enabled && strategy.uses_borrowed_input() {
                    let input = measure_stage!(
                        stages,
                        "strategy_input_assembly",
                        shared.strategy_borrowed_observation_input(
                            &mut driver,
                            strategy.requires_trade_economics_details()
                        )
                    )
                    .map_err(HistoricalFailureCauseV1::Ledger)?;
                    input
                        .map(|input| {
                            measure_stage!(
                                stages,
                                "calculation_including_cutoff_copy_and_decide",
                                input.calculate_borrowed(strategy, at)
                            )
                        })
                        .transpose()
                        .map_err(HistoricalFailureCauseV1::Strategy)?
                } else {
                    let input = if health_enabled {
                        measure_stage!(
                            stages,
                            "strategy_input_assembly",
                            shared.strategy_observation_input_with_trade_details(
                                &mut driver,
                                strategy.requires_trade_economics_details(),
                            )
                        )
                    } else {
                        measure_stage!(
                            stages,
                            "strategy_input_assembly",
                            shared.strategy_input_with_trade_details(
                                &mut driver,
                                strategy.requires_trade_economics_details(),
                            )
                        )
                    }
                    .map_err(HistoricalFailureCauseV1::Ledger)?;
                    input
                        .map(|input| {
                            measure_stage!(
                                stages,
                                "calculation_including_cutoff_copy_and_decide",
                                input.calculate(strategy, at)
                            )
                        })
                        .transpose()
                        .map_err(HistoricalFailureCauseV1::Strategy)?
                };
                if let Some(calculation) = calculation {
                    if let Some(counts) = research.as_mut().and_then(|s| s.decisions.as_mut()) {
                        counts.record(&calculation.output);
                    }
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
                    if health_enabled {
                        health_at = measure_stage!(
                            stages,
                            "next_health_evaluation_including_dependency_lookup",
                            strategy.next_health_evaluation_at(
                                &driver.frames_at_observation(),
                                Some(&calculation),
                                at,
                            )
                        )
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
                measure_stage!(
                    stages,
                    "sim_advance_before_including_noe",
                    driver.advance_before(at.checked_add(1).ok_or_else(|| {
                        HistoricalFailureCauseV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                            "历史时序溢出",
                        ))
                    })?)
                )
                .map_err(|cause| sim_error("advance_before_due_close", at + 1, cause))?;
                unconsumed_required_trigger_at = None;
            }
            // 初始化及隐藏来源可早于经营范围；保留其业务时刻，不让统计前沿倒退。
            if at >= economics_start {
                sample_day_economics(&driver, &shared, at, &mut day_economics);
            }
            checkpoint
                .save(
                    &mut driver,
                    &shared,
                    strategy,
                    RuntimeLoopCheckpointRefV1 {
                        pending: &pending,
                        start_at,
                        dirty,
                        calculation_index,
                        timing: &timing,
                        evaluation_at,
                        health_at,
                        health_summary: &health_summary,
                        unconsumed_required_trigger_at,
                        delivery_summary: &delivery_summary,
                        day_economics: &day_economics,
                        research: &research,
                    },
                    next_hidden.as_ref(),
                )
                .map_err(HistoricalFailureCauseV1::Material)?;
            if checkpoint.budget_stopped() {
                // 同毫秒已算完的Exit沿原下一轮立即终止；未到期的Exit仍随Running保存。
                if pending.as_ref().is_some_and(|p| {
                    p.completed_at == at
                        && matches!(
                            p.calculation.calculation.output,
                            StrategyOutputV1::Exit { .. }
                        )
                }) {
                    sample_day_economics(&driver, &shared, at, &mut day_economics);
                    return Ok((HistoricalEndReasonV1::StrategyExit, at));
                }
                return Ok((HistoricalEndReasonV1::BudgetStop, at));
            }
        }
        let tail_at = end.checked_sub(1).ok_or_else(|| {
            HistoricalFailureCauseV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                "历史范围结束时刻溢出",
            ))
        })?;
        driver.finish_input_at(tail_at).map_err(|cause| {
            sim_call_failure(
                "finish_input_at",
                Some(tail_at),
                driver
                    .observation_cutoff()
                    .map(|cutoff| cutoff.receipt().group().group_position),
                None,
                cause,
            )
        })?;
        measure_stage!(
            stages,
            "apply_closed_including_noe_claim_and_ledger",
            shared.apply_closed()
        )
        .map_err(HistoricalFailureCauseV1::Ledger)?;
        // 原尾观察已领取经济反馈；在该观察时点更新统计，随后延续至范围结束。
        if tail_at >= economics_start {
            sample_day_economics(&driver, &shared, tail_at, &mut day_economics);
        }
        if let Some(summary) = &mut research {
            while summary.next_boundary <= end {
                let boundary = summary.next_boundary;
                day_economics.finish_duration(boundary);
                summary.push(research_snapshot(
                    &driver,
                    &shared,
                    boundary,
                    &health_summary,
                    pending.as_ref().map(|p| p.record.clone()),
                    &day_economics,
                ));
                #[cfg(feature = "stage-timing")]
                stages.emit_hour(boundary);
            }
        }
        day_economics.finish_duration(end);
        Ok((HistoricalEndReasonV1::RangeEnd, end))
    }
    .await;
    match running {
        Ok((reason, ended_at)) => {
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
            result.day_economics = day_economics.summary();
            result.research = research.as_ref().map(|summary| {
                summary.summary(Some(crate::research_summary::snapshot(
                    ended_at,
                    result.observation_cutoff.clone(),
                    &shared,
                    result.account_economics_memory.as_ref(),
                    &health_summary,
                    result.pending_calculation_timing.clone(),
                    &day_economics,
                )))
            });
            result.source_health = health_enabled.then_some(health_summary);
            if normal {
                measure_stage!(stages, "recording_stop_and_drain", driver.stop_and_drain())
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
            failure_report.day_economics = day_economics.summary();
            failure_report.research = research.as_ref().map(|summary| {
                summary.summary(Some(crate::research_summary::snapshot(
                    failure_report
                        .last_observed_at_utc_ms
                        .unwrap_or(economics_start),
                    failure_report.observation_cutoff.clone(),
                    &shared,
                    failure_report.account_economics_memory.as_ref(),
                    &health_summary,
                    failure_report.pending_calculation_timing.clone(),
                    &day_economics,
                )))
            });
            failure_report.source_health = health_enabled.then_some(health_summary);
            Err(HistoricalRunFailureV1 {
                cause,
                report: Some(failure_report),
            })
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct RuntimeLoopCheckpointV1<D> {
    pending: Option<Pending<D>>,
    start_at: Option<i64>,
    dirty: bool,
    calculation_index: usize,
    timing: Option<CalculationTimingRecordV1>,
    evaluation_at: Option<i64>,
    health_at: Option<i64>,
    health_summary: SourceHealthRunSummaryV1,
    unconsumed_required_trigger_at: Option<i64>,
    delivery_summary: HistoricalDeliverySummaryV1,
    day_economics: crate::day_economics::DayEconomicsStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    research: Option<crate::research_summary::ResearchStateV1>,
}
impl<D> Default for RuntimeLoopCheckpointV1<D> {
    fn default() -> Self {
        Self {
            pending: None,
            start_at: None,
            dirty: false,
            calculation_index: 0,
            timing: None,
            evaluation_at: None,
            health_at: None,
            health_summary: SourceHealthRunSummaryV1 {
                qualification_calls: 0,
                deadline_wakeups: 0,
                invalidated_batches: 0,
                last_invalidated_calculation_timing: None,
            },
            unconsumed_required_trigger_at: None,
            delivery_summary: Default::default(),
            day_economics: Default::default(),
            research: None,
        }
    }
}
#[derive(Serialize)]
pub(crate) struct RuntimeLoopCheckpointRefV1<'a, D> {
    pending: &'a Option<Pending<D>>,
    start_at: Option<i64>,
    dirty: bool,
    calculation_index: usize,
    timing: &'a Option<CalculationTimingRecordV1>,
    evaluation_at: Option<i64>,
    health_at: Option<i64>,
    health_summary: &'a SourceHealthRunSummaryV1,
    unconsumed_required_trigger_at: Option<i64>,
    delivery_summary: &'a HistoricalDeliverySummaryV1,
    day_economics: &'a crate::day_economics::DayEconomicsStateV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    research: &'a Option<crate::research_summary::ResearchStateV1>,
}
pub(crate) type RestoredRuntimeV1<D> = (
    PublicConsumptionStateV1,
    SharedExecutionV2,
    MemoryNoeObservationV4,
    RuntimeLoopCheckpointV1<D>,
);
pub(crate) trait RuntimeCheckpointV1<
    S: StrategyV1,
    I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>,
>
{
    fn started(&mut self) {}
    fn research_enabled(&self) -> bool {
        false
    }
    fn budget_stopped(&self) -> bool {
        false
    }
    fn restore(
        &mut self,
        _config: &HistoricalPreparedRunConfigV1,
        _rules: Arc<SealedProcessRuleSnapshotV3>,
        _strategy: &mut S,
    ) -> Result<Option<RestoredRuntimeV1<S::Diagnostics>>, MaterialError> {
        Ok(None)
    }
    fn restored_hidden(&mut self) -> Option<Result<AcceptedHiddenStepBucketV1, MaterialError>> {
        None
    }
    fn save(
        &mut self,
        driver: &mut FinitePublicConsumptionV1<I>,
        shared: &SharedExecutionV2,
        strategy: &S,
        state: RuntimeLoopCheckpointRefV1<'_, S::Diagnostics>,
        next_hidden: Option<&AcceptedHiddenStepBucketV1>,
    ) -> Result<(), MaterialError>;
}
struct NoRuntimeCheckpointV1;
impl<S: StrategyV1, I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>
    RuntimeCheckpointV1<S, I> for NoRuntimeCheckpointV1
{
    fn save(
        &mut self,
        _driver: &mut FinitePublicConsumptionV1<I>,
        _shared: &SharedExecutionV2,
        _strategy: &S,
        _state: RuntimeLoopCheckpointRefV1<'_, S::Diagnostics>,
        _next_hidden: Option<&AcceptedHiddenStepBucketV1>,
    ) -> Result<(), MaterialError> {
        Ok(())
    }
}
