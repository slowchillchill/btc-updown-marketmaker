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
    pub(super) historical_rule_snapshot: PathBuf,
    pub(super) historical_rule_snapshot_sha256: Sha256Hex,
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
    let validation_work = parent.join(format!(
        ".gate-b-final-validation-{}",
        std::process::id()
    ));
    let gate_a = open_published_gate_a_final(&gate.gate_a_root, &validation_work)
        .map_err(|error| format!("只读打开显式 Gate A final 失败：{error}"))?;
    if validation_work.exists() {
        return Err("Gate A 外置 validation 工作目录未清理".to_owned());
    }
    let rule_snapshot = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: gate.historical_rule_snapshot.clone(),
        expected_file_sha256: gate.historical_rule_snapshot_sha256.clone(),
        replay_causal_start_utc_ms: gate_a.run_manifest().received_start_utc_ms,
    })
    .map_err(|error| format!("加载显式历史规则快照失败：{error}"))?;
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

    fs::create_dir(&args.output_dir)
        .map_err(|error| format!("创建 Gate B 运行目录失败：{error}"))?;
    let run_id = format!(
        "{GATE_B_RUNNER_IDENTITY}-{}",
        gate_a.receipt().receipt_id.0
    );
    let opened = RunOpenedV2 {
        run_id: run_id.clone(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rule_snapshot.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: args.identity.clone(),
        market_event_source_identity_sha256: adapter
            .canonical_event_store_manifest_sha256()
            .clone(),
    };
    let source_reference = |domain: &str| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Sha256(Sha256Hex(format!(
            "{:x}",
            Sha256::digest(format!("{domain}\0{run_id}").as_bytes())
        ))),
    };
    let noe_port = sim_system::execution::SimV2NoeProjectionPortV1::new(
        args.output_dir.join("noe"),
        opened.clone(),
        source_reference("run-opened"),
        source_reference("account-opening"),
        format!("{run_id}-opening"),
        GATE_B_RUNNER_IDENTITY.to_owned(),
    );
    let noe_observation = noe_port.observation();
    let mut execution = SimExecution::create_new(
        &args.output_dir.join("sim-event-log-v2.working.jsonl"),
        opened,
        Arc::clone(&rule_snapshot),
        Arc::clone(&finality),
        Arc::clone(&scenario),
        Arc::clone(&latency),
        Box::new(noe_port),
        MAX_GROUP_BYTES,
    )
    .map_err(|error| format!("创建 Gate B SimExecution 失败：{error}"))?;
    execution
        .create_session(SessionSpecV3 {
            run_id: run_id.clone(),
            session_id: format!("{run_id}-session"),
            account_id: format!("{run_id}-account"),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: GATE_B_RUNNER_IDENTITY.to_owned(),
            creation_request_id: format!("{run_id}-create"),
            response_bridge_binding_id: format!("{run_id}-bridge"),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("1000".to_owned()),
            },
            session_seed: Sha256Hex(format!("{:x}", Sha256::digest(run_id.as_bytes()))),
        })
        .map_err(|error| format!("创建 Gate B 会话失败：{error}"))?;
    execution
        .activate_session(format!("{run_id}-activate"))
        .map_err(|error| format!("激活 Gate B 会话失败：{error}"))?;

    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".to_owned()),
    });
    let mut applied_noe_count = 0_usize;
    apply_new_noe_results(&noe_observation, &mut applied_noe_count, &mut ledger)?;

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
        if let Some(required) = execution.next_required_logical_drive()
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
            execution
                .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position,
                })
                .map_err(|error| format!("Gate B 必要逻辑前缀推进失败：{error}"))?;
            apply_new_noe_results(&noe_observation, &mut applied_noe_count, &mut ledger)?;
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
        execution
            .drive_closed_market_bucket(gate_a_bucket(path, &bucket))
            .map_err(|error| format!("Gate B 真实双路回放失败：{error}"))?;
        apply_new_noe_results(&noe_observation, &mut applied_noe_count, &mut ledger)?;
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
    if let Some(required) = execution.next_required_logical_drive() {
        return Err(format!("Gate B 双路结束后仍有必要逻辑推进：{required:?}"));
    }
    let references = noe_observation.result_references();
    let projection = ledger
        .projection()
        .ok_or_else(|| "Gate B 真实 NOE 流未形成 Ledger 投影".to_owned())?;
    println!(
        "gate_b_flow=PASS receipt_id={} noe_result_references={} ledger_entry_seq={} output={}",
        gate_a.receipt().receipt_id.0,
        references.len(),
        projection.ledger_entry_seq,
        args.output_dir.display()
    );
    Ok(())
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
