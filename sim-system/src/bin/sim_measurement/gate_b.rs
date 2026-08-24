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
    pub(super) run_id: String,
    pub(super) initial_available_pusd: ExactDecimal,
    pub(super) per_market_all_in_commitment_limit_pusd: ExactDecimal,
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
    let run_id = gate.run_id.clone();
    let creation_request_id = format!("{run_id}-create");
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
    let (run_opened_reference, account_opening_reference, account_opening_evidence_identity) =
        noe_context_sources(&run_id, &creation_request_id);
    let noe_port = sim_system::execution::SimV2NoeProjectionPortV1::new(
        args.output_dir.join("noe"),
        opened.clone(),
        run_opened_reference,
        account_opening_reference,
        account_opening_evidence_identity,
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
            creation_request_id: creation_request_id.clone(),
            response_bridge_binding_id: format!("{run_id}-bridge"),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: gate.initial_available_pusd.clone(),
            },
            session_seed: Sha256Hex(format!("{:x}", Sha256::digest(run_id.as_bytes()))),
        })
        .map_err(|error| format!("创建 Gate B 会话失败：{error}"))?;
    let (mut ledger, mut applied_noe_count) = initialize_before_activation(
        || {
            let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
                per_market_all_in_commitment_limit_pusd: gate
                    .per_market_all_in_commitment_limit_pusd
                    .clone(),
            });
            let mut applied_noe_count = 0_usize;
            apply_new_noe_results(&noe_observation, &mut applied_noe_count, &mut ledger)?;
            ledger
                .projection()
                .ok_or_else(|| "Gate B 开户 NOE 未形成 Ledger 初始化投影".to_owned())?;
            Ok((ledger, applied_noe_count))
        },
        || {
            execution
                .activate_session(format!("{run_id}-activate"))
                .map(|_| ())
                .map_err(|error| format!("激活 Gate B 会话失败：{error}"))
        },
    )?;

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
    let mut recovery_adapter = GateASimulationReplayAdapter::open(
        &gate_a,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        output.sort.batch_rows,
    )
    .map_err(|error| format!("重新打开 Gate A 冷恢复适配器失败：{error}"))?;
    let candidate = require_clean_run_completion(execution.complete_clean_run(|reference| {
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
    }))?;
    apply_new_noe_results(&noe_observation, &mut applied_noe_count, &mut ledger)?;
    let references = noe_observation.result_references();
    let projection = ledger
        .projection()
        .ok_or_else(|| "Gate B 真实 NOE 流未形成 Ledger 投影".to_owned())?;
    println!(
        "gate_b_flow=PASS receipt_id={} noe_result_references={} ledger_entry_seq={} clean_run_accepted_inputs={} output={}",
        gate_a.receipt().receipt_id.0,
        references.len(),
        projection.ledger_entry_seq,
        candidate.accepted_input_count(),
        args.output_dir.display()
    );
    Ok(())
}

fn noe_context_sources(
    run_id: &str,
    creation_request_id: &str,
) -> (
    DurableSourceReferenceV1,
    DurableSourceReferenceV1,
    String,
) {
    (
        DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Identity(run_id.to_owned()),
        },
        DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Identity(creation_request_id.to_owned()),
        },
        creation_request_id.to_owned(),
    )
}

fn initialize_before_activation<T>(
    initialize: impl FnOnce() -> Result<T, String>,
    activate: impl FnOnce() -> Result<(), String>,
) -> Result<T, String> {
    let initialized = initialize()?;
    activate()?;
    Ok(initialized)
}

fn require_clean_run_completion(
    result: Result<
        sim_system::execution::UnpublishedRunArtifactCandidateV2,
        SimRunActorErrorV2,
    >,
) -> Result<sim_system::execution::UnpublishedRunArtifactCandidateV2, String> {
    result.map_err(|error| format!("Gate B CleanRun 完成门失败：{error}"))
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
    use std::cell::Cell;

    use super::*;

    #[test]
    fn public_context_identities_match_the_run_and_actual_opening_request() {
        let (run_opened, account_opening, evidence_identity) =
            noe_context_sources("user-run-id", "user-run-id-create");
        assert_eq!(
            run_opened.source,
            ImmutableSourceReferenceV1::Identity("user-run-id".to_owned())
        );
        assert_eq!(
            account_opening.source,
            ImmutableSourceReferenceV1::Identity("user-run-id-create".to_owned())
        );
        assert_eq!(evidence_identity, "user-run-id-create");
    }

    #[test]
    fn ledger_initialization_finishes_before_session_activation() {
        let stage = Cell::new(0_u8);
        let result = initialize_before_activation(
            || {
                assert_eq!(stage.get(), 0);
                stage.set(1);
                Ok("ledger-opened")
            },
            || {
                assert_eq!(stage.get(), 1);
                stage.set(2);
                Ok(())
            },
        )
        .expect("固定顺序必须成功");
        assert_eq!(result, "ledger-opened");
        assert_eq!(stage.get(), 2);
    }

    #[test]
    fn clean_run_failure_cannot_be_reported_as_success() {
        let result = require_clean_run_completion(Err(
            SimRunActorErrorV2::CompletionNotReady("输入尚未封闭"),
        ));
        assert_eq!(
            result.unwrap_err(),
            "Gate B CleanRun 完成门失败：运行完成门尚未满足：输入尚未封闭"
        );
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
