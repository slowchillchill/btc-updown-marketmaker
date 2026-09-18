use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1,
};
use ledger::{
    AccountAdmissionPolicyV1, LedgerApplyOutcomeV1, LedgerErrorV1, LedgerWriterV1,
};

use super::*;

pub(super) const GATE_B_RUNNER_IDENTITY: &str = "sim-gate-b-explicit-flow-v1";

#[derive(Debug)]
pub(super) struct GateBArgs {
    pub(super) gate_a_root: PathBuf,
    pub(super) reference_market: ReferenceMarketSpecV1,
    pub(super) run_id: String,
    pub(super) initial_available_pusd: ExactDecimal,
    pub(super) per_market_all_in_commitment_limit_pusd: ExactDecimal,
}

struct GateBFlowPreparation<'a> {
    output_dir: PathBuf,
    gate: &'a GateBArgs,
    implementation_identity: ImplementationIdentityV1,
    market_event_source_identity_sha256: Sha256Hex,
    rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
    finality: Arc<TradeFinalityPolicyV2>,
    scenario: Arc<SealedPlatformScenarioV1>,
    latency: Arc<EmpiricalDelayProfile>,
}

struct PreparedGateBFlow {
    execution: SimExecution,
    noe_observation: sim_system::execution::SimV2NoeObservationV1,
    ledger: LedgerWriterV1,
    applied_noe_count: usize,
    opening_fact: Arc<contracts::AccountOpeningFactV2>,
}

struct FinishedGateBFlow {
    candidate: sim_system::execution::UnpublishedRunArtifactCandidateV2,
    ledger: LedgerWriterV1,
    applied_noe_count: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum FinishGateBFlowError {
    Completion(SimRunActorErrorV2),
    Ledger(String),
}

impl std::fmt::Display for FinishGateBFlowError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completion(error) => write!(formatter, "Gate B CleanRun 完成门失败：{error}"),
            Self::Ledger(error) => formatter.write_str(error),
        }
    }
}

fn prepare_gate_b_flow(input: GateBFlowPreparation<'_>) -> Result<PreparedGateBFlow, String> {
    let creation_request_id = format!("{}-create", input.gate.run_id);
    let opened = RunOpenedV2 {
        projection_version: Default::default(),
        run_id: input.gate.run_id.clone(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: input.rule_snapshot.file_sha256().clone(),
        trade_finality_policy_file_sha256: input.finality.policy_id().clone(),
        platform_scenario_file_sha256: input.scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: input.latency.file_sha256().clone(),
        implementation_identity: input.implementation_identity,
        market_event_source_identity_sha256: Some(input.market_event_source_identity_sha256),
    };
    let noe_port = sim_system::execution::SimV2NoeProjectionPortV1::new(
        input.output_dir.join("noe"),
        opened.clone(),
        DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Identity(input.gate.run_id.clone()),
        },
        DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Identity(creation_request_id.clone()),
        },
        creation_request_id.clone(),
        GATE_B_RUNNER_IDENTITY.to_owned(),
    );
    let noe_observation = noe_port.observation();
    let mut execution = SimExecution::create_new(
        &input.output_dir.join("sim-event-log-v2.working.jsonl"),
        opened,
        Arc::clone(&input.rule_snapshot),
        Arc::clone(&input.finality),
        Arc::clone(&input.scenario),
        Arc::clone(&input.latency),
        Box::new(noe_port),
        MAX_GROUP_BYTES,
    )
    .map_err(|error| format!("创建 Gate B SimExecution 失败：{error}"))?;
    let session = execution
        .create_session(SessionSpecV3 {
            run_id: input.gate.run_id.clone(),
            session_id: format!("{}-session", input.gate.run_id),
            account_id: format!("{}-account", input.gate.run_id),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: GATE_B_RUNNER_IDENTITY.to_owned(),
            creation_request_id,
            response_bridge_binding_id: format!("{}-bridge", input.gate.run_id),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: input.gate.initial_available_pusd.clone(),
            },
            session_seed: Sha256Hex(format!(
                "{:x}",
                Sha256::digest(input.gate.run_id.as_bytes())
            )),
        })
        .map_err(|error| format!("创建 Gate B 会话失败：{error}"))?;
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: input
            .gate
            .per_market_all_in_commitment_limit_pusd
            .clone(),
    });
    let mut applied_noe_count = 0_usize;
    apply_new_noe_results(&noe_observation, &mut applied_noe_count, &mut ledger)?;
    ledger
        .projection()
        .ok_or_else(|| "Gate B 开户 NOE 未形成 Ledger 初始化投影".to_owned())?;

    Ok(PreparedGateBFlow {
        execution,
        noe_observation,
        ledger,
        applied_noe_count,
        opening_fact: session.opening_fact,
    })
}

fn finish_gate_b_flow<F>(
    mut prepared: PreparedGateBFlow,
    read_bucket: F,
) -> Result<FinishedGateBFlow, FinishGateBFlowError>
where
    F: FnMut(
        &DurableMarketBucketReferenceV2,
    ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
{
    let candidate = prepared
        .execution
        .complete_clean_run(read_bucket)
        .map_err(FinishGateBFlowError::Completion)?;
    apply_new_noe_results(
        &prepared.noe_observation,
        &mut prepared.applied_noe_count,
        &mut prepared.ledger,
    )
    .map_err(FinishGateBFlowError::Ledger)?;
    Ok(FinishedGateBFlow {
        candidate,
        ledger: prepared.ledger,
        applied_noe_count: prepared.applied_noe_count,
    })
}

pub(super) fn run(args: Args, _run_started: Instant) -> Result<(), String> {
    let gate = args
        .gate_b
        .as_ref()
        .expect("parser 已绑定 Gate B 参数");
    if args.output_dir.exists() {
        return Err(format!(
            "Gate B 运行目录已存在：{}",
            args.output_dir.display()
        ));
    }
    let parent = args
        .output_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).map_err(|error| format!("创建 Gate B 父目录失败：{error}"))?;
    fs::create_dir(&args.output_dir)
        .map_err(|error| format!("创建 Gate B 运行目录失败：{error}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("创建 Gate B CURRENT_QUERY runtime 失败：{error}"))?;
    let rule_snapshot = runtime
        .block_on(query_and_seal_current_process_snapshot_v3(
            CurrentProcessSnapshotRequestV1 {
                reference_market: gate.reference_market.clone(),
                target_path: args.output_dir.join("platform-rule-snapshot-v3.json"),
                implementation_identity: args.identity.clone(),
            },
        ))
        .map_err(|error| format!("Gate B runner 唯一 CURRENT_QUERY 失败：{error}"))?;
    let validation_work = parent.join(format!(
        ".gate-b-final-validation-{}",
        std::process::id()
    ));
    let gate_a = open_published_gate_a_final(&gate.gate_a_root, &validation_work)
        .map_err(|error| format!("只读打开显式 Gate A final 失败：{error}"))?;
    if validation_work.exists() {
        return Err("Gate A 外置 validation 工作目录未清理".to_owned());
    }
    let scenario = scenario_from_gate_a(&gate_a)?;
    let finality = successful_finality()?;
    let latency = load_embedded_proxy_latency_profile()
        .map_err(|error| format!("加载 Gate B 时延档失败：{error}"))?;
    let output = &gate_a.run_manifest().output_config;
    let mut adapter = GateASimulationReplayAdapter::open(
        &gate_a,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        output.sort.batch_rows,
    )
    .map_err(|error| format!("打开显式 Gate A 双路适配器失败：{error}"))?;

    let mut prepared = prepare_gate_b_flow(GateBFlowPreparation {
        output_dir: args.output_dir.clone(),
        gate,
        implementation_identity: args.identity.clone(),
        market_event_source_identity_sha256: adapter
            .canonical_event_store_manifest_sha256()
            .clone(),
        rule_snapshot: Arc::clone(&rule_snapshot),
        finality: Arc::clone(&finality),
        scenario: Arc::clone(&scenario),
        latency: Arc::clone(&latency),
    })?;
    prepared
        .execution
        .activate_session(format!(
            "{}-activate",
            prepared.opening_fact.account_instance_key.run_id
        ))
        .map_err(|error| format!("激活 Gate B 会话失败：{error}"))?;

    let mut platform = adapter
        .next_platform_bucket()
        .map_err(|error| format!("读取 Gate B 平台首桶失败：{error}"))?;
    let mut visibility = adapter
        .next_visibility_bucket()
        .map_err(|error| format!("读取 Gate B 可见性首桶失败：{error}"))?;
    while platform.is_some() || visibility.is_some() {
        let next_bucket_at = match (&platform, &visibility) {
            (Some(left), Some(right)) => left.index_at_utc_ms().min(right.index_at_utc_ms()),
            (Some(bucket), None) | (None, Some(bucket)) => bucket.index_at_utc_ms(),
            (None, None) => unreachable!(),
        };
        if let Some(required) = prepared.execution.next_required_logical_drive()
            && required.required_at_utc_ms < next_bucket_at
        {
            let next_index_position = match required.path {
                MarketEventPathV1::HiddenPlatform => platform.as_ref().map_or_else(
                    || adapter.platform_next_logical_position(),
                    ClosedExecutionMarketIndexBucketV1::start_logical_position,
                ),
                MarketEventPathV1::PublicVisibility => visibility.as_ref().map_or_else(
                    || adapter.visibility_next_logical_position(),
                    ClosedExecutionMarketIndexBucketV1::start_logical_position,
                ),
            };
            prepared
                .execution
                .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position,
                })
                .map_err(|error| format!("Gate B 必要逻辑前缀推进失败：{error}"))?;
            apply_new_noe_results(
                &prepared.noe_observation,
                &mut prepared.applied_noe_count,
                &mut prepared.ledger,
            )?;
            continue;
        }

        let path = match (&platform, &visibility) {
            (Some(left), Some(right)) if left.index_at_utc_ms() <= right.index_at_utc_ms() => {
                MarketEventPathV1::HiddenPlatform
            }
            (Some(_), None) => MarketEventPathV1::HiddenPlatform,
            _ => MarketEventPathV1::PublicVisibility,
        };
        let bucket = match path {
            MarketEventPathV1::HiddenPlatform => platform.take().expect("平台桶存在"),
            MarketEventPathV1::PublicVisibility => visibility.take().expect("可见性桶存在"),
        };
        prepared
            .execution
            .drive_closed_market_bucket(gate_a_bucket(path, &bucket))
            .map_err(|error| format!("Gate B 真实双路回放失败：{error}"))?;
        apply_new_noe_results(
            &prepared.noe_observation,
            &mut prepared.applied_noe_count,
            &mut prepared.ledger,
        )?;
        match path {
            MarketEventPathV1::HiddenPlatform => {
                platform = adapter
                    .next_platform_bucket()
                    .map_err(|error| format!("读取 Gate B 平台下一桶失败：{error}"))?;
            }
            MarketEventPathV1::PublicVisibility => {
                visibility = adapter
                    .next_visibility_bucket()
                    .map_err(|error| format!("读取 Gate B 可见性下一桶失败：{error}"))?;
            }
        }
    }
    if let Some(required) = prepared.execution.next_required_logical_drive() {
        return Err(format!("Gate B 双路结束后仍有必要逻辑推进：{required:?}"));
    }
    let mut recovery_adapter = GateASimulationReplayAdapter::open(
        &gate_a,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        output.sort.batch_rows,
    )
    .map_err(|error| format!("重新打开 Gate A 冷恢复适配器失败：{error}"))?;
    let finished = finish_gate_b_flow(prepared, |reference| {
        let bucket = match reference.path {
            MarketEventPathV1::HiddenPlatform => recovery_adapter.next_platform_bucket(),
            MarketEventPathV1::PublicVisibility => recovery_adapter.next_visibility_bucket(),
        }
        .map_err(|_| {
            SimRunActorErrorV2::CompletionNotReady("冷恢复读取 Gate A 市场桶失败")
        })?
        .ok_or(SimRunActorErrorV2::CompletionNotReady(
            "冷恢复缺少日志引用的 Gate A 市场桶",
        ))?;
        exact_recovery_bucket_body(reference, gate_a_bucket(reference.path, &bucket))
    })
    .map_err(|error| error.to_string())?;
    let projection = finished
        .ledger
        .projection()
        .ok_or_else(|| "Gate B 真实 NOE 流未形成 Ledger 投影".to_owned())?;
    println!(
        "gate_b_flow=PASS receipt_id={} noe_result_references={} ledger_entry_seq={} clean_run_accepted_inputs={} output={}",
        gate_a.receipt().receipt_id.0,
        finished.applied_noe_count,
        projection.ledger_entry_seq,
        finished.candidate.accepted_input_count(),
        args.output_dir.display()
    );
    Ok(())
}

fn exact_recovery_bucket_body(
    requested: &DurableMarketBucketReferenceV2,
    accepted: AcceptedClosedMarketBucketV2,
) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2> {
    if &accepted.reference != requested {
        return Err(SimRunActorErrorV2::CompletionNotReady(
            "冷恢复 Gate A 市场桶引用不匹配",
        ));
    }
    Ok(accepted.events)
}

fn scenario_from_gate_a(
    gate_a: &ValidatedPublishedGateAFinal,
) -> Result<Arc<SealedPlatformScenarioV1>, String> {
    let output = &gate_a.run_manifest().output_config;
    let mut adapter = GateASimulationReplayAdapter::open(
        gate_a,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        output.sort.batch_rows,
    )
    .map_err(|error| format!("打开 Gate A 市场身份扫描失败：{error}"))?;
    let market = loop {
        let Some(bucket) = adapter
            .next_platform_bucket()
            .map_err(|error| format!("扫描 Gate A 平台事件失败：{error}"))?
        else {
            return Err("Gate A final 缺少可构造模拟场景的市场身份".to_owned());
        };
        if let Some(market) = bucket.events().iter().find_map(|event| {
            let ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) =
                &event.payload
            else {
                return None;
            };
            let assets = <[AssetIdentityV1; 2]>::try_from(transition.assets.clone()).ok()?;
            Some(PlatformMarketV1::try_new(event.market.clone(), assets))
        }) {
            break market.map_err(|error| format!("构造 Gate B 市场失败：{error}"))?;
        }
    };
    let scenario = PlatformScenarioV1::try_new(vec![market])
        .map_err(|error| format!("构造 Gate B 单市场场景失败：{error}"))?;
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        market_set_sha256: scenario.market_set_sha256().clone(),
        markets: scenario.market_set().markets().to_vec(),
    };
    let bytes = serde_json::to_vec(&artifact)
        .map_err(|error| format!("编码 Gate B 单市场场景失败：{error}"))?;
    SealedPlatformScenarioV1::decode_and_seal(&bytes)
        .map(Arc::new)
        .map_err(|error| format!("密封 Gate B 单市场场景失败：{error}"))
}

fn apply_new_noe_results(
    observation: &sim_system::execution::SimV2NoeObservationV1,
    applied_count: &mut usize,
    ledger: &mut LedgerWriterV1,
) -> Result<(), String> {
    let references = observation.result_references();
    for reference in references.iter().skip(*applied_count) {
        match ledger.apply_noe_result(reference) {
            Ok(LedgerApplyOutcomeV1::NewlyApplied { .. })
            | Ok(LedgerApplyOutcomeV1::AlreadyApplied { .. })
            | Err(LedgerErrorV1::NonEconomicRecord) => {}
            Ok(LedgerApplyOutcomeV1::MissingPlatformPredecessor {
                expected_account_transition_seq,
                found_account_transition_seq,
            }) => {
                return Err(format!(
                    "Gate B NOE 原序缺少平台前驱：expected={expected_account_transition_seq}, found={found_account_transition_seq}"
                ));
            }
            Err(error) => return Err(format!("Gate B 应用 NOE 结果引用失败：{error}")),
        }
    }
    *applied_count = references.len();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const RULE_FIXTURE_SHA256: &str =
        "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54";

    fn prepare_test_flow(
        output_dir: PathBuf,
        run_id: &str,
        initial_available_pusd: &str,
        per_market_limit_pusd: &str,
    ) -> Result<PreparedGateBFlow, String> {
        // 复用既有 official-V3-shape 测试坐标；它不是 Gate A receipt 或 #30 正式数据证据。
        let gate = GateBArgs {
            gate_a_root: PathBuf::new(),
            reference_market: reference_market(),
            run_id: run_id.to_owned(),
            initial_available_pusd: ExactDecimal(initial_available_pusd.to_owned()),
            per_market_all_in_commitment_limit_pusd: ExactDecimal(
                per_market_limit_pusd.to_owned(),
            ),
        };
        let rule_snapshot = load_historical_process_snapshot_v3(
            HistoricalProcessSnapshotRequestV1 {
                snapshot_path: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/platform-rule-snapshot-v3-gate-b.json"),
                expected_file_sha256: Sha256Hex(RULE_FIXTURE_SHA256.to_owned()),
                replay_causal_start_utc_ms: COMPONENT_CAUSAL_START_UTC_MS,
            },
        )
        .map_err(|error| format!("加载 Gate B 测试规则快照失败：{error}"))?;
        let rule_snapshot_file_sha256 = rule_snapshot.file_sha256().clone();
        fs::create_dir(&output_dir).map_err(|error| format!("创建 Gate B 测试目录失败：{error}"))?;
        let prepared = prepare_gate_b_flow(GateBFlowPreparation {
            output_dir: output_dir.clone(),
            gate: &gate,
            implementation_identity: ImplementationIdentityV1 {
                git_commit: "1".repeat(40),
                source_identity_sha256: Sha256Hex("2".repeat(64)),
                cargo_lock_sha256: Sha256Hex("3".repeat(64)),
            },
            market_event_source_identity_sha256: Sha256Hex("4".repeat(64)),
            rule_snapshot,
            finality: successful_finality()?,
            scenario: component_fixed_scenario()?,
            latency: load_embedded_proxy_latency_profile()
                .map_err(|error| format!("加载 Gate B 测试时延档失败：{error}"))?,
        })?;
        let first_line = fs::read_to_string(output_dir.join("sim-event-log-v2.working.jsonl"))
            .map_err(|error| format!("读取 Gate B 测试运行日志失败：{error}"))?
            .lines()
            .next()
            .ok_or_else(|| "Gate B 测试运行日志缺少 RunOpened".to_owned())?
            .to_owned();
        let envelope: Value = serde_json::from_str(&first_line)
            .map_err(|error| format!("解码 Gate B 测试 RunOpened 信封失败：{error}"))?;
        let opened: RunOpenedV2 = serde_json::from_value(
            envelope
                .get("body")
                .cloned()
                .ok_or_else(|| "Gate B 测试运行日志缺少 RunOpened body".to_owned())?,
        )
        .map_err(|error| format!("解码 Gate B 测试 RunOpened 失败：{error}"))?;
        assert_eq!(
            opened.platform_rule_snapshot_file_sha256,
            rule_snapshot_file_sha256,
            "Gate B RunOpened 必须绑定传入规则快照 SHA-256"
        );
        Ok(prepared)
    }

    #[test]
    fn current_query_precedes_gate_a_open_and_run_artifacts() {
        let temp = tempfile::tempdir().expect("创建 Gate B CURRENT_QUERY 临时目录");
        let output = temp.path().join("run");
        let missing_gate_a = temp.path().join("missing-gate-a");
        let arguments = vec![
            "sim_measurement".to_owned(),
            "--gate-b-flow".to_owned(),
            "--output-dir".to_owned(),
            output.to_string_lossy().into_owned(),
            "--git-commit".to_owned(),
            "1".repeat(40),
            "--source-identity-sha256".to_owned(),
            "2".repeat(64),
            "--cargo-lock-sha256".to_owned(),
            "3".repeat(64),
            "--gate-a-root".to_owned(),
            missing_gate_a.to_string_lossy().into_owned(),
            "--reference-condition-id".to_owned(),
            "not-a-condition".to_owned(),
            "--reference-clob-market-id".to_owned(),
            "3278040".to_owned(),
            "--reference-market-slug".to_owned(),
            "btc-updown-5m-offline-order-check".to_owned(),
            "--reference-up-token-id".to_owned(),
            TOKEN_UP.to_owned(),
            "--reference-up-outcome".to_owned(),
            "Up".to_owned(),
            "--reference-down-token-id".to_owned(),
            TOKEN_DOWN.to_owned(),
            "--reference-down-outcome".to_owned(),
            "Down".to_owned(),
            "--run-id".to_owned(),
            "gate-b-query-order".to_owned(),
            "--initial-available-pusd".to_owned(),
            "2000".to_owned(),
            "--per-market-all-in-commitment-limit-pusd".to_owned(),
            "1000".to_owned(),
        ];
        let args = parse_args_from(arguments).expect("完整 Gate B 参数必须解析");
        let error = run(args, Instant::now()).expect_err("非法参考身份必须在联网前拒绝");
        assert!(error.contains("CURRENT_QUERY"), "unexpected error: {error}");
        assert!(output.is_dir());
        assert!(!output.join("platform-rule-snapshot-v3.json").exists());
        assert!(!output.join("sim-event-log-v2.working.jsonl").exists());
        assert!(!output.join("noe").exists());
    }

    #[test]
    fn preparation_uses_actual_context_opens_ledger_then_activates_once() {
        let temp = tempfile::tempdir().expect("创建 Gate B preparation 临时目录");
        let mut prepared = prepare_test_flow(
            temp.path().join("prepared"),
            "gate-b-preparation",
            "2000",
            "1000",
        )
        .expect("真实 Gate B preparation 必须成功");
        let actual_creation_request_id = prepared.opening_fact.creation_request_id.clone();
        let first_reference = prepared
            .noe_observation
            .result_references()
            .into_iter()
            .next()
            .expect("真实开户必须发布 NOE 引用");
        let resolved = execution_normalization::read_noe_result_v1(&first_reference)
            .expect("必须能从真实 NOE 引用回读上下文");
        assert_eq!(
            resolved.context.source.run_opened.source,
            ImmutableSourceReferenceV1::Identity("gate-b-preparation".to_owned())
        );
        assert_eq!(
            resolved.context.source.account_opening.source,
            ImmutableSourceReferenceV1::Identity(actual_creation_request_id.clone())
        );
        assert_eq!(
            resolved.context.source.account_opening_evidence_identity,
            actual_creation_request_id
        );
        assert_eq!(
            prepared
                .ledger
                .projection()
                .expect("prepare 已形成开户投影")
                .platform_confirmed_projection
                .available_pusd,
            ExactDecimal("2000".to_owned())
        );
        assert_eq!(
            prepared
                .ledger
                .admission_policy()
                .per_market_all_in_commitment_limit_pusd,
            ExactDecimal("1000".to_owned())
        );
        assert_eq!(
            prepared
                .execution
                .activate_session("gate-b-preparation-activate".to_owned())
                .expect("开户投影形成后的第一次真实激活必须成功"),
            sim_system::execution::SessionInitializationControlResultV2::Activated
        );
    }

    #[test]
    fn finish_requires_real_clean_run_and_returns_the_completed_flow() {
        let temp = tempfile::tempdir().expect("创建 Gate B CleanRun 临时目录");
        let mut incomplete = prepare_test_flow(
            temp.path().join("incomplete"),
            "gate-b-incomplete",
            "2000",
            "1000",
        )
        .expect("未完成轨迹 preparation 必须成功");
        assert_eq!(
            incomplete
                .execution
                .activate_session("gate-b-incomplete-activate".to_owned())
                .expect("未完成轨迹必须真实激活"),
            sim_system::execution::SessionInitializationControlResultV2::Activated
        );
        assert!(matches!(
            finish_gate_b_flow(incomplete, |_| Err(
                SimRunActorErrorV2::CompletionNotReady("不应读取未完成轨迹")
            )),
            Err(FinishGateBFlowError::Completion(
                SimRunActorErrorV2::CompletionNotReady(_)
            ))
        ));

        let complete_output = temp.path().join("complete");
        let mut complete = prepare_test_flow(
            complete_output.clone(),
            "gate-b-complete",
            "2000",
            "1000",
        )
        .expect("完整轨迹 preparation 必须成功");
        assert_eq!(
            complete
                .execution
                .activate_session("gate-b-complete-activate".to_owned())
                .expect("完整轨迹必须真实激活"),
            sim_system::execution::SessionInitializationControlResultV2::Activated
        );
        let scenario = component_fixed_scenario().expect("固定 component 场景必须有效");
        let buckets = golden_buckets_with_shift(&scenario, COMPONENT_TIME_SHIFT_MS)
            .expect("固定 component 桶必须有效");
        let mut ordered = buckets.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|(reference, _)| {
            (reference.bucket_at_utc_ms, reference.path)
        });
        let mut next_bucket = 0_usize;
        while next_bucket < ordered.len() {
            drive_required_before_next_bucket(
                &mut complete.execution,
                &ordered,
                &mut next_bucket,
            )
            .expect("完整轨迹的必要逻辑前缀必须成功");
            apply_new_noe_results(
                &complete.noe_observation,
                &mut complete.applied_noe_count,
                &mut complete.ledger,
            )
            .expect("逻辑前缀 NOE 必须原序进入 Ledger");
            drive_real_bucket(&mut complete.execution, ordered[next_bucket])
                .expect("完整轨迹真实桶必须成功");
            apply_new_noe_results(
                &complete.noe_observation,
                &mut complete.applied_noe_count,
                &mut complete.ledger,
            )
            .expect("市场桶 NOE 必须原序进入 Ledger");
            next_bucket += 1;
        }
        let observation = complete.noe_observation.clone();
        let finished = finish_gate_b_flow(complete, bucket_reader(&buckets))
            .expect("完整轨迹必须经过真实 CleanRun 完成门");
        assert_eq!(finished.candidate.run_id(), "gate-b-complete");
        assert_eq!(
            finished.candidate.log_path(),
            complete_output.join("sim-event-log-v2.working.jsonl")
        );
        assert_eq!(
            finished.applied_noe_count,
            observation.result_references().len()
        );
        assert!(finished.ledger.projection().is_some());
    }

    #[test]
    fn recovery_reader_rejects_a_substituted_bucket_reference() {
        let requested = DurableMarketBucketReferenceV2 {
            path: MarketEventPathV1::HiddenPlatform,
            bucket_at_utc_ms: 100,
            start_index_position: 0,
            next_index_position: 1,
            closure: MillisecondBucketClosureV1::EndOfInput,
        };
        let substituted = AcceptedClosedMarketBucketV2 {
            reference: DurableMarketBucketReferenceV2 {
                bucket_at_utc_ms: 101,
                ..requested.clone()
            },
            events: Arc::from([]),
        };
        assert_eq!(
            exact_recovery_bucket_body(&requested, substituted).unwrap_err(),
            SimRunActorErrorV2::CompletionNotReady("冷恢复 Gate A 市场桶引用不匹配")
        );
    }
}
