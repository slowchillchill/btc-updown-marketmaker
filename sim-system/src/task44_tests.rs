use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use contracts::{
    AssetIdentityV1, CanonicalEventId, EventLineageV2, EventQualityV2, EventTimeV2, ExactDecimal,
    ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, MarketIdentityV2,
    MarketResolutionTerminalV3, OutcomeIdentityV1, PayoutEntryV1, Sha256Hex, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::artifacts::{
    SIMULATION_RUN_AUDIT_FILE_NAME_V1, SimulationDeterministicScriptContractV1,
    SimulationRunArtifactErrorV1, SimulationRunIntentBindingV1, SimulationWorkloadKindV1,
};
use crate::execution::{
    DownstreamDurabilityProofV1, OrderRequestV1, PaperMarketEventRecoveryReadErrorV1,
    PaperMarketEventRecoveryReaderV1, SessionRetirementFailureV1, SimExecutionProcessV1,
    SimExecutionSessionV1, SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1,
    SubmissionResponseAcceptanceV1, SubmissionResponseBridgeV1,
};
use crate::finality::successful_policy_for_tests;
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MillisecondBucketClosureV1,
};
use crate::platform_rules::tests::{
    historical_snapshot_for_sim_tests_with_fee_schedule, successful_snapshot_for_sim_tests,
};
use crate::platform_rules::{RecoveryProcessSnapshotRequestV1, recover_process_snapshot};
use crate::resource::{
    ResourceLimitDimensionV1, ResourceLimitKindV1, ResourceLimitScopeV1, ResourceLimitV1,
    SimulationResourcePolicyV1,
};
use crate::scenario::PlatformScenarioV1;
use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, RequestRegistrationDispositionV1,
    SessionError, SessionSpecV2, SimCallerRoleV1,
};
use crate::task21_tests::fresh_quality;
use crate::task22_tests::{
    context_for_arrival, initial_events, market, orderbook_evidence, scenario, session_spec,
};
const RUN_ID: &str = "task44-component-golden-run";

fn golden_asset(token_id: u64, index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token_id.to_string()),
        outcome: OutcomeIdentityV1 {
            index,
            label: label.to_owned(),
        },
    }
}

fn golden_resolution(
    market: &MarketIdentityV2,
    first_token: u64,
    sequence: u64,
) -> ExecutionMarketEventV3 {
    let source_at_utc_ms = market.window.end_at_utc_ms + 10_000;
    golden_sealed_event(
        market,
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
            MarketResolutionTerminalV3::Resolved {
                winning_asset_id: first_token.to_string(),
                winning_outcome: "Up".to_owned(),
                payout_vector: vec![
                    PayoutEntryV1 {
                        asset: golden_asset(first_token, 0, "Up"),
                        payout_pusd_per_share: ExactDecimal("1".to_owned()),
                    },
                    PayoutEntryV1 {
                        asset: golden_asset(first_token + 1, 1, "Down"),
                        payout_pusd_per_share: ExactDecimal("0".to_owned()),
                    },
                ],
                mapping_rule_version: "task44-component-golden-resolution-v1".to_owned(),
            },
        ),
        source_at_utc_ms,
        source_at_utc_ms + 10_000,
        sequence,
        fresh_quality(),
    )
}

fn golden_sealed_event(
    market: &MarketIdentityV2,
    payload: ExecutionMarketEventPayloadV3,
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    sequence: u64,
    quality: EventQualityV2,
) -> ExecutionMarketEventV3 {
    let evidence = orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence);
    let mut event = ExecutionMarketEventV3 {
        schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        event_id: CanonicalEventId("0".repeat(64)),
        market: market.clone(),
        time: EventTimeV2 {
            source_event_at_utc_ms: Some(source_at_utc_ms),
            received_at_utc_ms,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "task44-component-golden-rule-v1".to_owned(),
            source_evidence: vec![evidence],
        },
        quality,
        payload,
    };
    event.event_id = derive_execution_market_event_v3_id(&event).expect("推导正式黄金事件身份");
    event
}

fn sha256_serializable<T: Serialize>(domain: &str, value: &T) -> Sha256Hex {
    let encoded = serde_json::to_vec(value).expect("编码正式 runner 哈希正文");
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

#[derive(Clone, Default)]
struct BridgeProbeV1 {
    callback_count: Arc<AtomicUsize>,
}

impl BridgeProbeV1 {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let probes = std::array::from_fn::<_, 11, _>(|_| Arc::clone(&self.callback_count));
        let [
            post_order,
            post_orders,
            cancel_order,
            cancel_orders,
            cancel_all,
            order,
            orders,
            trades,
            order_lifecycle,
            settlement,
            finality,
        ] = probes;
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                post_orders.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_order.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_orders.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_all.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                order.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                orders.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                trades.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                order_lifecycle.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                settlement.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                finality.fetch_add(1, Ordering::SeqCst);
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
        )
    }

    fn callback_count(&self) -> usize {
        self.callback_count.load(Ordering::SeqCst)
    }
}

struct PersistentSessionPairV1 {
    _directory: tempfile::TempDir,
    persistence_root: PathBuf,
    process: SimExecutionProcessV1,
    profile: Arc<EmpiricalDelayProfile>,
    first: SimExecutionSessionV1,
    second: SimExecutionSessionV1,
    first_control: crate::session::SessionCreationControlResultV2,
    second_control: crate::session::SessionCreationControlResultV2,
    first_probe: BridgeProbeV1,
    second_probe: BridgeProbeV1,
}

impl PersistentSessionPairV1 {
    async fn new(resource_policy: SimulationResourcePolicyV1) -> Self {
        let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
        let profile = load_embedded_proxy_latency_profile().expect("任务 4.4 代理时延配置档");
        let scenario = scenario();
        let intent = SimulationRunIntentBindingV1::try_new(
            rules.snapshot().provenance.implementation_identity.clone(),
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("任务 4.4 运行意图");
        let directory = tempfile::tempdir().expect("任务 4.4 耐久临时目录");
        let persistence_root = directory.path().to_path_buf();
        let source_identity = Sha256Hex(format!(
            "{:x}",
            Sha256::digest(b"TASK44_COMPONENT_GOLDEN_EVENT_SOURCE_V1")
        ));
        let mut process =
            SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
                RUN_ID.to_owned(),
                successful_policy_for_tests(),
                rules,
                Arc::clone(&profile),
                Arc::clone(&scenario),
                intent,
                source_identity,
                persistence_root.clone(),
                resource_policy,
            )
            .expect("任务 4.4 持久运行根");

        let mut first_spec = model_train_spec(&scenario, "task44-trajectory-a", 'a');
        let mut second_spec = model_train_spec(&scenario, "task44-trajectory-b", 'b');
        first_spec.run_id = RUN_ID.to_owned();
        second_spec.run_id = RUN_ID.to_owned();
        let first_probe = BridgeProbeV1::default();
        let second_probe = BridgeProbeV1::default();
        let first_prepared = process
            .create_session(first_spec.clone(), first_probe.bridge())
            .expect("创建轨迹 A");
        let second_prepared = process
            .create_session(second_spec.clone(), second_probe.bridge())
            .expect("创建轨迹 B");
        process
            .activate_session(activation_request(&first_spec, &first_prepared.control))
            .expect("激活轨迹 A");
        process
            .activate_session(activation_request(&second_spec, &second_prepared.control))
            .expect("激活轨迹 B");

        Self {
            _directory: directory,
            persistence_root,
            process,
            profile,
            first: first_prepared.session,
            second: second_prepared.session,
            first_control: first_prepared.control,
            second_control: second_prepared.control,
            first_probe,
            second_probe,
        }
    }
}

fn model_train_spec(
    scenario: &crate::execution::PlatformScenarioV1,
    session_id: &str,
    seed_digit: char,
) -> SessionSpecV2 {
    let mut spec = session_spec(scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.session_seed = Sha256Hex(seed_digit.to_string().repeat(64));
    spec
}

fn activation_request(
    spec: &SessionSpecV2,
    control: &crate::session::SessionCreationControlResultV2,
) -> ActivateSessionRequestV1 {
    ActivateSessionRequestV1 {
        session_id: spec.session_id.clone(),
        account_instance_key: control.account_instance_key.clone(),
        opening_anchor_id: control.opening_anchor.anchor_id.clone(),
        opening_anchor_sha256: control.opening_anchor.anchor_sha256.clone(),
        activation_id: format!("activate-{}", spec.session_id),
    }
}

fn failure_request(
    spec: &SessionSpecV2,
    control: &crate::session::SessionCreationControlResultV2,
) -> FailSessionInitializationRequestV1 {
    FailSessionInitializationRequestV1 {
        session_id: spec.session_id.clone(),
        account_instance_key: control.account_instance_key.clone(),
        opening_anchor_id: control.opening_anchor.anchor_id.clone(),
        opening_anchor_sha256: control.opening_anchor.anchor_sha256.clone(),
        failure_id: format!("fail-{}", spec.session_id),
    }
}

fn order_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
) -> OrderRequestV1 {
    OrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            desired_arrival_at_utc_ms,
        ),
        platform_order_id: "f".repeat(64),
    }
}

fn durable_fact_entries(persistence_root: &Path) -> Vec<(u64, Value)> {
    let mut entries = std::fs::read_dir(persistence_root.join("evidence"))
        .expect("读取任务 4.4 仅追加证据目录")
        .map(|entry| entry.expect("读取任务 4.4 证据目录项"))
        .filter(|entry| entry.file_type().expect("读取证据类型").is_file())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| !name.starts_with('.') && name.ends_with(".json"))
        })
        .map(|entry| {
            let envelope: Value = serde_json::from_slice(
                &std::fs::read(entry.path()).expect("读取任务 4.4 证据正文"),
            )
            .expect("解码任务 4.4 证据信封");
            let sequence = envelope
                .get("sequence")
                .and_then(Value::as_u64)
                .expect("任务 4.4 证据必须包含序号");
            let fact = envelope
                .pointer("/body/fact")
                .cloned()
                .expect("任务 4.4 证据必须包含强类型事实");
            (sequence, fact)
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|(sequence, _)| *sequence);
    entries
}

#[derive(Debug, PartialEq, Eq)]
struct DurableRootIdentityV1 {
    current_bytes: Vec<u8>,
    files: Vec<(String, Sha256Hex)>,
    candidate_paths: Vec<String>,
}

fn durable_root_identity_v1(persistence_root: &Path) -> DurableRootIdentityV1 {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut Vec<(String, Sha256Hex)>,
        candidate_paths: &mut Vec<String>,
    ) {
        for entry in fs::read_dir(directory).expect("读取任务 4.4 耐久目录") {
            let entry = entry.expect("读取任务 4.4 耐久目录项");
            let path = entry.path();
            let file_type = entry.file_type().expect("读取任务 4.4 耐久目录项类型");
            let relative = path
                .strip_prefix(root)
                .expect("耐久目录项必须位于运行根内")
                .to_string_lossy()
                .into_owned();
            if relative.contains(".candidate") {
                candidate_paths.push(relative.clone());
            }
            if file_type.is_dir() {
                visit(root, &path, files, candidate_paths);
            } else if file_type.is_file() {
                files.push((
                    relative,
                    sha256_bytes(&fs::read(&path).expect("读取任务 4.4 耐久文件")),
                ));
            }
        }
    }

    let mut files = Vec::new();
    let mut candidate_paths = Vec::new();
    visit(
        persistence_root,
        persistence_root,
        &mut files,
        &mut candidate_paths,
    );
    files.sort_by(|left, right| left.0.cmp(&right.0));
    candidate_paths.sort();
    DurableRootIdentityV1 {
        current_bytes: fs::read(persistence_root.join("CURRENT"))
            .expect("读取任务 4.4 CURRENT 原始字节"),
        files,
        candidate_paths,
    }
}

fn apply_task442_dual_time_bucket(
    session: &mut SimExecutionSessionV1,
    hidden: bool,
    at_utc_ms: i64,
    closure: MillisecondBucketClosureV1,
    events: &[ExecutionMarketEventV3],
) {
    let encoded = events
        .iter()
        .map(|event| encode_execution_market_event_v3(event).expect("编码任务 4.4.2 真实事件"))
        .collect::<Vec<_>>();
    let start = if hidden {
        session.platform_event_index_position()
    } else {
        session.public_event_index_position()
    };
    let next = start + u64::try_from(encoded.len()).expect("任务 4.4.2 事件数适合 u64");
    let bucket = if hidden {
        ClosedMarketEventBucketV1::hidden(at_utc_ms, start, next, closure, encoded)
    } else {
        ClosedMarketEventBucketV1::public(at_utc_ms, start, next, closure, encoded)
    };
    assert_eq!(
        session.apply_closed_market_event_bucket(bucket),
        Ok(MarketBucketApplyDispositionV1::Applied),
        "非输入配额关闭后，既有会话仍必须接纳真实绑定流或空 EOF"
    );
}

fn complete_task442_clean_session(
    process: &mut SimExecutionProcessV1,
    mut session: SimExecutionSessionV1,
    control: &crate::session::SessionCreationControlResultV2,
) {
    let initial = initial_events("0.01", "0.6", "1000");
    apply_task442_dual_time_bucket(
        &mut session,
        true,
        110_000,
        MillisecondBucketClosureV1::NextEventAt(510_000),
        &initial,
    );
    apply_task442_dual_time_bucket(
        &mut session,
        false,
        120_000,
        MillisecondBucketClosureV1::NextEventAt(520_000),
        &initial,
    );
    apply_task442_dual_time_bucket(
        &mut session,
        true,
        150_000,
        MillisecondBucketClosureV1::NextEventAt(510_000),
        &[],
    );
    assert!(matches!(
        session
            .drive_next_execution_action_through(150_000)
            .expect("在终态事件前处理任务 4.4.2 已登记动作"),
        SubmissionDriveDispositionV1::NoAction
            | SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true
            }
    ));
    apply_task442_dual_time_bucket(
        &mut session,
        true,
        500_000,
        MillisecondBucketClosureV1::NextEventAt(510_000),
        &[],
    );
    assert_eq!(
        session.drive_next_execution_action_through(500_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    let resolution = golden_resolution(&market(), 1, 3);
    apply_task442_dual_time_bucket(
        &mut session,
        true,
        510_000,
        MillisecondBucketClosureV1::NextEventAt(530_000),
        std::slice::from_ref(&resolution),
    );
    apply_task442_dual_time_bucket(
        &mut session,
        false,
        520_000,
        MillisecondBucketClosureV1::NextEventAt(540_000),
        std::slice::from_ref(&resolution),
    );
    apply_task442_dual_time_bucket(
        &mut session,
        true,
        530_000,
        MillisecondBucketClosureV1::EndOfInput,
        &[],
    );
    apply_task442_dual_time_bucket(
        &mut session,
        false,
        540_000,
        MillisecondBucketClosureV1::EndOfInput,
        &[],
    );
    assert_eq!(
        session.drive_next_execution_action_through(1_000_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    while matches!(
        session
            .drive_private_delivery_through(1_000_000)
            .expect("交付任务 4.4.2 终态反馈"),
        SubmissionDeliveryDispositionV1::Delivered
    ) {}
    let condition_id = market().condition_id.0;
    let diagnostics = session.task41_diagnostics_for_tests(&condition_id);
    let mut confirmations = diagnostics
        .confirmed_direct_response_confirmations
        .iter()
        .chain(
            diagnostics
                .confirmed_lifecycle_feedback_confirmations
                .iter(),
        )
        .cloned()
        .collect::<Vec<_>>();
    confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    confirmations.dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    for durable_through in confirmations {
        process
            .advance_downstream_durability(
                &mut session,
                DownstreamDurabilityProofV1 {
                    account_instance_key: control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("推进任务 4.4.2 强下游水位");
    }
    process
        .evict_terminal_market(&mut session, &condition_id)
        .expect("驱逐任务 4.4.2 终态市场");
    match process.complete_clean_run_session(session) {
        Ok(()) => {}
        Err(SessionRetirementFailureV1::Rejected { error, .. }) => {
            panic!("任务 4.4.2 clean run 完成被拒：{error}")
        }
        Err(SessionRetirementFailureV1::RecoveryRequired { error }) => {
            panic!("任务 4.4.2 clean run 完成要求恢复：{error}")
        }
    }
}

struct PersistentSingleSessionV1 {
    _directory: tempfile::TempDir,
    persistence_root: PathBuf,
    process: SimExecutionProcessV1,
    profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
    spec: SessionSpecV2,
    control: crate::session::SessionCreationControlResultV2,
    session: SimExecutionSessionV1,
}

impl PersistentSingleSessionV1 {
    async fn new(run_id: &str, resource_policy: SimulationResourcePolicyV1) -> Self {
        let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
        let profile = load_embedded_proxy_latency_profile().expect("任务 4.4.2 代理时延配置档");
        let scenario = scenario();
        let intent = SimulationRunIntentBindingV1::try_new(
            rules.snapshot().provenance.implementation_identity.clone(),
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("任务 4.4.2 运行意图");
        let directory = tempfile::tempdir().expect("任务 4.4.2 耐久临时目录");
        let persistence_root = directory.path().join("run");
        let mut process =
            SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
                run_id.to_owned(),
                successful_policy_for_tests(),
                rules,
                Arc::clone(&profile),
                Arc::clone(&scenario),
                intent,
                sha256_serializable("TASK44_QUOTA_CLOSURE_SOURCE_V1", &run_id),
                persistence_root.clone(),
                resource_policy,
            )
            .expect("创建任务 4.4.2 持久运行根");
        let mut spec = model_train_spec(&scenario, &format!("{run_id}-a"), 'a');
        spec.run_id = run_id.to_owned();
        let prepared = process
            .create_session(spec.clone(), BridgeProbeV1::default().bridge())
            .expect("创建任务 4.4.2 既有会话 A");
        process
            .activate_session(activation_request(&spec, &prepared.control))
            .expect("激活任务 4.4.2 既有会话 A");
        Self {
            _directory: directory,
            persistence_root,
            process,
            profile,
            scenario,
            spec,
            control: prepared.control,
            session: prepared.session,
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task442_request_limit_still_allows_bound_input_clean_completion_and_seal() {
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.session.request_queue = ResourceLimitV1::new(1, policy.session.request_queue.max_bytes);
    let fixture = PersistentSingleSessionV1::new("task442-request-closed-clean", policy).await;
    let PersistentSingleSessionV1 {
        _directory,
        persistence_root,
        mut process,
        profile,
        control,
        mut session,
        ..
    } = fixture;
    let limit = match session.order(order_request(
        &session,
        &profile,
        "task442-request-limit-first",
        1,
        150_000,
    )) {
        Err(SessionError::ResourceLimitReached(limit)) => limit,
        other => panic!("首个真实请求必须命中压低后的请求配额，实际为 {other:?}"),
    };
    assert_eq!(limit.scope, ResourceLimitScopeV1::Session);
    assert_eq!(limit.kind, ResourceLimitKindV1::RequestQueue);
    complete_task442_clean_session(&mut process, session, &control);
    let receipt = process
        .seal_run_artifacts()
        .expect("请求配额关闭后的既有 clean run 必须仍可封存");
    assert!(receipt.artifact_directory.is_dir());
    assert_eq!(
        durable_fact_entries(&persistence_root)
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "后续绑定流、空 EOF、clean completion 与封存不得复制请求限额事实"
    );
    drop(_directory);
}

#[tokio::test(flavor = "current_thread")]
async fn task442_run_sessions_limit_still_allows_existing_clean_completion_and_seal() {
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.run.sessions = ResourceLimitV1::new(1, policy.run.sessions.max_bytes);
    let mut fixture =
        PersistentSingleSessionV1::new("task442-run-sessions-closed-clean", policy).await;
    let mut blocked = model_train_spec(
        &fixture.scenario,
        "task442-run-sessions-closed-clean-b",
        'b',
    );
    blocked.run_id = fixture.spec.run_id.clone();
    let limit = match fixture
        .process
        .create_session(blocked, BridgeProbeV1::default().bridge())
    {
        Err(SessionError::ResourceLimitReached(limit)) => limit,
        Ok(_) => panic!("第二个真实会话必须命中运行级会话数上限"),
        Err(other) => panic!("第二个真实会话必须返回强类型资源限额，实际为 {other:?}"),
    };
    assert_eq!(limit.scope, ResourceLimitScopeV1::Run);
    assert_eq!(limit.kind, ResourceLimitKindV1::Sessions);
    complete_task442_clean_session(&mut fixture.process, fixture.session, &fixture.control);
    let receipt = fixture
        .process
        .seal_run_artifacts()
        .expect("运行级会话接入关闭后的既有 clean run 必须仍可封存");
    assert!(receipt.artifact_directory.is_dir());
    assert_eq!(
        durable_fact_entries(&fixture.persistence_root)
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "会话接入失败与既有运行封存必须保留一项可区分限额事实"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task442_local_limit_renews_as_one_shared_barrier_and_tail_reuses_it_after_recovery() {
    let directory = tempfile::tempdir().expect("创建组合资源屏障临时目录");
    let baseline_root = directory.path().join("baseline");
    let persistence_root = directory.path().join("run");
    let current_rules = successful_snapshot_for_sim_tests().await;
    let recovery_path = directory.path().join("platform-rule-snapshot.json");
    fs::write(&recovery_path, current_rules.file_bytes()).expect("写入组合屏障恢复规则");
    let profile = load_embedded_proxy_latency_profile().expect("加载组合屏障时延配置档");
    let scenario = scenario();
    let source_identity = sha256_serializable(
        "TASK442_COMBINED_LIMIT_EVENT_SOURCE_V1",
        &"task442-combined-limit",
    );
    let intent = || {
        SimulationRunIntentBindingV1::try_new(
            current_rules
                .snapshot()
                .provenance
                .implementation_identity
                .clone(),
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("构造组合屏障运行意图")
    };
    let run_id = "task442-combined-limit";
    let mut spec = session_spec(&scenario, SimCallerRoleV1::Paper, "task442-combined-a");
    spec.run_id = run_id.to_owned();

    let (baseline_run_records, baseline_evidence_records) = {
        let mut process =
            SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
                run_id.to_owned(),
                successful_policy_for_tests(),
                current_rules.clone(),
                Arc::clone(&profile),
                Arc::clone(&scenario),
                intent(),
                source_identity.clone(),
                baseline_root,
                SimulationResourcePolicyV1::component_golden_v1(),
            )
            .expect("创建组合屏障基线运行根");
        let prepared = process
            .create_session(spec.clone(), BridgeProbeV1::default().bridge())
            .expect("创建组合屏障基线会话");
        process
            .activate_session(activation_request(&spec, &prepared.control))
            .expect("激活组合屏障基线会话");
        let resources = process
            .resource_snapshot_for_tests()
            .expect("读取组合屏障基线资源");
        (
            resources.run.in_memory_state.records,
            resources.durable.evidence.records,
        )
    };

    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.session.request_queue = ResourceLimitV1::new(1, policy.session.request_queue.max_bytes);
    policy.run.in_memory_state = ResourceLimitV1::new(
        baseline_run_records.checked_add(1).expect("运行记录硬上限"),
        policy.run.in_memory_state.max_bytes,
    );
    policy.durable.evidence = ResourceLimitV1::new(
        baseline_evidence_records
            .checked_add(1)
            .expect("证据记录硬上限"),
        policy.durable.evidence.max_bytes,
    );
    assert_eq!(
        policy.durable.failure_reserve.run_checkpoint_growth.records,
        1
    );
    assert_eq!(policy.durable.failure_reserve.evidence.records, 1);

    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            run_id.to_owned(),
            successful_policy_for_tests(),
            current_rules.clone(),
            Arc::clone(&profile),
            Arc::clone(&scenario),
            intent(),
            source_identity.clone(),
            persistence_root.clone(),
            policy,
        )
        .expect("创建组合资源屏障运行根");
    let probe = BridgeProbeV1::default();
    let prepared = process
        .create_session(spec.clone(), probe.bridge())
        .expect("创建组合资源屏障会话");
    process
        .activate_session(activation_request(&spec, &prepared.control))
        .expect("激活组合资源屏障会话");
    let mut session = prepared.session;
    let shared = match session.order(order_request(
        &session,
        &profile,
        "task442-combined-local-trigger",
        1,
        150_000,
    )) {
        Err(SessionError::ResourceLimitReached(limit)) => limit,
        other => panic!("局部失败续租不足必须原子提升为共享屏障，实际为 {other:?}"),
    };
    assert_eq!(shared.scope, ResourceLimitScopeV1::Run);
    assert_eq!(shared.kind, ResourceLimitKindV1::InMemoryState);
    let facts_after_barrier = durable_fact_entries(&persistence_root);
    let resource_limits = facts_after_barrier
        .iter()
        .filter_map(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED"))
        .collect::<Vec<_>>();
    assert_eq!(resource_limits.len(), 1, "组合屏障必须只使用一个失败事实");
    assert_eq!(
        resource_limits[0]
            .pointer("/triggering_session_limit/scope")
            .and_then(Value::as_str),
        Some("SESSION")
    );
    assert_eq!(
        resource_limits[0]
            .pointer("/triggering_session_limit/kind")
            .and_then(Value::as_str),
        Some("REQUEST_QUEUE")
    );
    let account_after_barrier = session.account_snapshot_for_tests();
    let cursor_after_barrier = session.model_visible_cursor();
    let diagnostics_after_barrier = session.task41_diagnostics_for_tests(&market().condition_id.0);
    let callbacks_after_barrier = probe.callback_count();
    let durable_after_barrier = durable_root_identity_v1(&persistence_root);
    assert!(
        durable_after_barrier.candidate_paths.is_empty(),
        "组合屏障提交后不得残留候选文件"
    );
    let tail_events = initial_events("0.01", "0.6", "1000")
        .iter()
        .map(|event| encode_execution_market_event_v3(event).expect("编码组合屏障尾部输入"))
        .collect::<Vec<_>>();
    let tail = ClosedMarketEventBucketV1::public(
        120_000,
        0,
        u64::try_from(tail_events.len()).expect("组合屏障尾部事件数适合 u64"),
        MillisecondBucketClosureV1::NextEventAt(120_001),
        tail_events,
    );
    let tail_result = session.apply_closed_market_event_bucket(tail.clone());
    assert!(
        matches!(
            &tail_result,
            Err(crate::market::MarketEventErrorV1::Session(
                SessionError::ResourceLimitReached(repeated)
            )) if repeated == &shared
        ),
        "共享屏障后超硬上限尾部必须返回原屏障，实际为 {tail_result:?}"
    );
    assert_eq!(
        durable_fact_entries(&persistence_root),
        facts_after_barrier,
        "共享屏障后尾部候选超过硬上限时必须复用原屏障且零落盘"
    );
    assert_eq!(
        durable_root_identity_v1(&persistence_root),
        durable_after_barrier,
        "共享屏障后尾部候选超过硬上限时 CURRENT、检查点、证据和候选目录必须逐字节不变"
    );
    assert_eq!(session.account_snapshot_for_tests(), account_after_barrier);
    assert_eq!(session.model_visible_cursor(), cursor_after_barrier);
    assert_eq!(
        session.task41_diagnostics_for_tests(&market().condition_id.0),
        diagnostics_after_barrier
    );
    assert_eq!(probe.callback_count(), callbacks_after_barrier);
    drop(session);
    drop(process);

    let recovery_rules = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
        snapshot_path: recovery_path,
        expected_file_sha256: current_rules.file_sha256().clone(),
        expected_implementation_identity: current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
    })
    .expect("加载组合屏障恢复规则");
    let reader = PaperMarketEventRecoveryReaderV1::try_new(source_identity, |_request| {
        Err(PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(
            "组合屏障前未提交市场桶".to_owned(),
        ))
    })
    .expect("构造组合屏障恢复读取端口");
    let mut recovered = SimExecutionProcessV1::recover_paper_from_local_persistence(
        successful_policy_for_tests(),
        recovery_rules,
        profile,
        scenario,
        reader,
        persistence_root.clone(),
    )
    .expect("组合资源屏障必须可恢复");
    let recovered_probe = BridgeProbeV1::default();
    let mut recovered_session = recovered
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            recovered_probe.bridge(),
        )
        .expect("重新附接组合屏障会话");
    assert_eq!(
        recovered_session.account_snapshot_for_tests(),
        account_after_barrier
    );
    assert_eq!(
        recovered_session.model_visible_cursor(),
        cursor_after_barrier
    );
    assert_eq!(
        recovered_session.task41_diagnostics_for_tests(&market().condition_id.0),
        diagnostics_after_barrier
    );
    assert_eq!(recovered_probe.callback_count(), 0);
    assert_eq!(
        durable_root_identity_v1(&persistence_root),
        durable_after_barrier,
        "恢复不得改写组合屏障的 CURRENT、检查点或证据"
    );
    assert!(matches!(
        recovered_session.apply_closed_market_event_bucket(tail),
        Err(crate::market::MarketEventErrorV1::Session(
            SessionError::ResourceLimitReached(repeated)
        )) if repeated == shared
    ));
    assert_eq!(
        durable_fact_entries(&persistence_root),
        facts_after_barrier,
        "恢复后的尾部重试不得追加第二个失败事实"
    );
    assert_eq!(
        durable_root_identity_v1(&persistence_root),
        durable_after_barrier,
        "恢复后的越硬限尾部重试必须保持全部耐久文件身份不变"
    );
    assert_eq!(
        recovered_session.account_snapshot_for_tests(),
        account_after_barrier
    );
    assert_eq!(
        recovered_session.model_visible_cursor(),
        cursor_after_barrier
    );
    assert_eq!(
        recovered_session.task41_diagnostics_for_tests(&market().condition_id.0),
        diagnostics_after_barrier
    );
    assert_eq!(recovered_probe.callback_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn task442_input_limit_renews_as_shared_barrier_and_preserves_rejected_cursor_after_recovery()
{
    let directory = tempfile::tempdir().expect("创建输入组合屏障临时目录");
    let baseline_root = directory.path().join("baseline");
    let persistence_root = directory.path().join("run");
    let current_rules = successful_snapshot_for_sim_tests().await;
    let recovery_path = directory.path().join("platform-rule-snapshot.json");
    fs::write(&recovery_path, current_rules.file_bytes()).expect("写入输入组合屏障恢复规则");
    let profile = load_embedded_proxy_latency_profile().expect("加载输入组合屏障时延配置档");
    let scenario = scenario();
    let source_identity = sha256_serializable(
        "TASK442_INPUT_COMBINED_LIMIT_EVENT_SOURCE_V1",
        &"task442-input-combined-limit",
    );
    let intent = || {
        SimulationRunIntentBindingV1::try_new(
            current_rules
                .snapshot()
                .provenance
                .implementation_identity
                .clone(),
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("构造输入组合屏障运行意图")
    };
    let run_id = "task442-input-combined-limit";
    let mut spec = session_spec(
        &scenario,
        SimCallerRoleV1::Paper,
        "task442-input-combined-a",
    );
    spec.run_id = run_id.to_owned();

    let (baseline_run_records, baseline_evidence_records) = {
        let mut process =
            SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
                run_id.to_owned(),
                successful_policy_for_tests(),
                current_rules.clone(),
                Arc::clone(&profile),
                Arc::clone(&scenario),
                intent(),
                source_identity.clone(),
                baseline_root,
                SimulationResourcePolicyV1::component_golden_v1(),
            )
            .expect("创建输入组合屏障基线运行根");
        let prepared = process
            .create_session(spec.clone(), BridgeProbeV1::default().bridge())
            .expect("创建输入组合屏障基线会话");
        process
            .activate_session(activation_request(&spec, &prepared.control))
            .expect("激活输入组合屏障基线会话");
        let resources = process
            .resource_snapshot_for_tests()
            .expect("读取输入组合屏障基线资源");
        (
            resources.run.in_memory_state.records,
            resources.durable.evidence.records,
        )
    };

    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.session.input_queue = ResourceLimitV1::new(1, policy.session.input_queue.max_bytes);
    policy.run.in_memory_state = ResourceLimitV1::new(
        baseline_run_records.checked_add(1).expect("运行记录硬上限"),
        policy.run.in_memory_state.max_bytes,
    );
    policy.durable.evidence = ResourceLimitV1::new(
        baseline_evidence_records
            .checked_add(1)
            .expect("证据记录硬上限"),
        policy.durable.evidence.max_bytes,
    );

    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            run_id.to_owned(),
            successful_policy_for_tests(),
            current_rules.clone(),
            Arc::clone(&profile),
            Arc::clone(&scenario),
            intent(),
            source_identity.clone(),
            persistence_root.clone(),
            policy,
        )
        .expect("创建输入组合资源屏障运行根");
    let probe = BridgeProbeV1::default();
    let prepared = process
        .create_session(spec.clone(), probe.bridge())
        .expect("创建输入组合资源屏障会话");
    process
        .activate_session(activation_request(&spec, &prepared.control))
        .expect("激活输入组合资源屏障会话");
    let mut session = prepared.session;
    let account_before = session.account_snapshot_for_tests();
    let cursor_before = session.model_visible_cursor();
    let diagnostics_before = session.task41_diagnostics_for_tests(&market().condition_id.0);
    let callbacks_before = probe.callback_count();
    let encoded = initial_events("0.01", "0.6", "1000")
        .iter()
        .map(|event| encode_execution_market_event_v3(event).expect("编码输入组合屏障事件"))
        .collect::<Vec<_>>();
    let bucket = ClosedMarketEventBucketV1::public(
        120_000,
        0,
        u64::try_from(encoded.len()).expect("输入组合屏障事件数适合 u64"),
        MillisecondBucketClosureV1::NextEventAt(120_001),
        encoded,
    );
    let shared = match session.apply_closed_market_event_bucket(bucket.clone()) {
        Err(crate::market::MarketEventErrorV1::Session(SessionError::ResourceLimitReached(
            limit,
        ))) => limit,
        other => panic!("输入局部失败续租不足必须原子提升为共享屏障，实际为 {other:?}"),
    };
    assert_eq!(shared.scope, ResourceLimitScopeV1::Run);
    assert_eq!(shared.kind, ResourceLimitKindV1::InMemoryState);
    assert_eq!(session.account_snapshot_for_tests(), account_before);
    assert_eq!(session.model_visible_cursor(), cursor_before);
    assert_eq!(
        session.task41_diagnostics_for_tests(&market().condition_id.0),
        diagnostics_before
    );
    assert_eq!(probe.callback_count(), callbacks_before);

    let facts_after_barrier = durable_fact_entries(&persistence_root);
    let resource_limits = facts_after_barrier
        .iter()
        .filter_map(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED"))
        .collect::<Vec<_>>();
    assert_eq!(
        resource_limits.len(),
        1,
        "输入组合屏障必须只使用一个失败事实"
    );
    assert_eq!(
        resource_limits[0]
            .pointer("/triggering_session_limit/scope")
            .and_then(Value::as_str),
        Some("SESSION")
    );
    assert_eq!(
        resource_limits[0]
            .pointer("/triggering_session_limit/kind")
            .and_then(Value::as_str),
        Some("INPUT_QUEUE")
    );
    assert_eq!(
        facts_after_barrier
            .iter()
            .filter(|(_, fact)| fact.get("PLATFORM_BUCKET_APPLIED").is_some())
            .count(),
        0,
        "被输入组合屏障拒绝的正文不得形成市场桶事实"
    );
    let durable_after_barrier = durable_root_identity_v1(&persistence_root);
    assert!(
        durable_after_barrier.candidate_paths.is_empty(),
        "输入组合屏障提交后不得残留候选文件"
    );
    let empty_eof = ClosedMarketEventBucketV1::public(
        120_001,
        0,
        0,
        MillisecondBucketClosureV1::EndOfInput,
        Vec::<Vec<u8>>::new(),
    );
    for rejected in [bucket.clone(), empty_eof.clone()] {
        assert!(matches!(
            session.apply_closed_market_event_bucket(rejected),
            Err(crate::market::MarketEventErrorV1::Session(
                SessionError::ResourceLimitReached(repeated)
            )) if repeated == shared
        ));
        assert_eq!(
            durable_root_identity_v1(&persistence_root),
            durable_after_barrier,
            "输入组合屏障后的原桶和同游标空 EOF 都必须保持耐久前缀逐字节不变"
        );
        assert_eq!(session.account_snapshot_for_tests(), account_before);
        assert_eq!(session.model_visible_cursor(), cursor_before);
        assert_eq!(
            session.task41_diagnostics_for_tests(&market().condition_id.0),
            diagnostics_before
        );
        assert_eq!(probe.callback_count(), callbacks_before);
    }
    drop(session);
    drop(process);

    let recovery_rules = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
        snapshot_path: recovery_path,
        expected_file_sha256: current_rules.file_sha256().clone(),
        expected_implementation_identity: current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
    })
    .expect("加载输入组合屏障恢复规则");
    let reader = PaperMarketEventRecoveryReaderV1::try_new(source_identity, |_request| {
        Err(PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(
            "输入组合屏障前未提交市场桶".to_owned(),
        ))
    })
    .expect("构造输入组合屏障恢复读取端口");
    let mut recovered = SimExecutionProcessV1::recover_paper_from_local_persistence(
        successful_policy_for_tests(),
        recovery_rules,
        profile,
        scenario,
        reader,
        persistence_root.clone(),
    )
    .expect("输入组合资源屏障必须可恢复");
    let recovered_probe = BridgeProbeV1::default();
    let mut recovered_session = recovered
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            recovered_probe.bridge(),
        )
        .expect("重新附接输入组合屏障会话");
    assert_eq!(
        recovered_session.account_snapshot_for_tests(),
        account_before
    );
    assert_eq!(recovered_session.model_visible_cursor(), cursor_before);
    assert_eq!(
        recovered_session.task41_diagnostics_for_tests(&market().condition_id.0),
        diagnostics_before
    );
    assert_eq!(recovered_probe.callback_count(), 0);
    assert_eq!(
        durable_root_identity_v1(&persistence_root),
        durable_after_barrier,
        "恢复输入组合屏障不得改写 CURRENT、检查点或证据"
    );
    for rejected in [bucket, empty_eof] {
        assert!(matches!(
            recovered_session.apply_closed_market_event_bucket(rejected),
            Err(crate::market::MarketEventErrorV1::Session(
                SessionError::ResourceLimitReached(repeated)
            )) if repeated == shared
        ));
        assert_eq!(
            durable_root_identity_v1(&persistence_root),
            durable_after_barrier,
            "恢复后的原桶和同游标空 EOF 都必须保持耐久前缀逐字节不变"
        );
        assert_eq!(
            recovered_session.account_snapshot_for_tests(),
            account_before
        );
        assert_eq!(recovered_session.model_visible_cursor(), cursor_before);
        assert_eq!(
            recovered_session.task41_diagnostics_for_tests(&market().condition_id.0),
            diagnostics_before
        );
        assert_eq!(recovered_probe.callback_count(), 0);
    }
    assert_eq!(durable_fact_entries(&persistence_root), facts_after_barrier);
}

#[tokio::test(flavor = "current_thread")]
async fn task442_uncreated_session_tombstones_are_bounded_retried_and_recovered() {
    let directory = tempfile::tempdir().expect("创建未创建会话墓碑临时目录");
    let baseline_root = directory.path().join("baseline");
    let persistence_root = directory.path().join("run");
    let current_rules = successful_snapshot_for_sim_tests().await;
    let recovery_path = directory.path().join("platform-rule-snapshot.json");
    fs::write(&recovery_path, current_rules.file_bytes()).expect("写入墓碑恢复规则");
    let profile = load_embedded_proxy_latency_profile().expect("加载墓碑时延配置档");
    let scenario = scenario();
    let source_identity = sha256_serializable(
        "TASK442_UNCREATED_TOMBSTONE_SOURCE_V1",
        &"task442-uncreated-tombstones",
    );
    let run_id = "task442-uncreated-tombstones";
    let intent = || {
        SimulationRunIntentBindingV1::try_new(
            current_rules
                .snapshot()
                .provenance
                .implementation_identity
                .clone(),
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("构造未创建会话墓碑运行意图")
    };
    let (baseline_run_records, baseline_evidence_records) = {
        let process =
            SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
                run_id.to_owned(),
                successful_policy_for_tests(),
                current_rules.clone(),
                Arc::clone(&profile),
                Arc::clone(&scenario),
                intent(),
                source_identity.clone(),
                baseline_root,
                SimulationResourcePolicyV1::component_golden_v1(),
            )
            .expect("创建墓碑基线运行根");
        let resources = process
            .resource_snapshot_for_tests()
            .expect("读取墓碑基线资源");
        (
            resources.run.in_memory_state.records,
            resources.durable.evidence.records,
        )
    };
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.session.in_memory_state =
        ResourceLimitV1::new(0, policy.session.in_memory_state.max_bytes);
    policy.run.in_memory_state = ResourceLimitV1::new(
        baseline_run_records
            .checked_add(3)
            .expect("墓碑运行记录上限"),
        policy.run.in_memory_state.max_bytes,
    );
    policy.durable.evidence = ResourceLimitV1::new(
        baseline_evidence_records
            .checked_add(3)
            .expect("墓碑证据记录上限"),
        policy.durable.evidence.max_bytes,
    );
    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            run_id.to_owned(),
            successful_policy_for_tests(),
            current_rules.clone(),
            Arc::clone(&profile),
            Arc::clone(&scenario),
            intent(),
            source_identity.clone(),
            persistence_root.clone(),
            policy,
        )
        .expect("创建墓碑限额运行根");
    let spec = |suffix: &str| {
        let mut spec = session_spec(
            &scenario,
            SimCallerRoleV1::Paper,
            &format!("task442-uncreated-{suffix}"),
        );
        spec.run_id = run_id.to_owned();
        spec
    };
    let first_spec = spec("a");
    let second_spec = spec("b");
    let third_spec = spec("c");
    let fourth_spec = spec("d");

    let first_limit =
        match process.create_session(first_spec.clone(), BridgeProbeV1::default().bridge()) {
            Err(SessionError::ResourceLimitReached(limit)) => limit,
            Ok(_) => panic!("第一个未创建会话必须形成局部墓碑"),
            Err(other) => panic!("第一个墓碑必须返回强类型限额，实际为 {other:?}"),
        };
    assert_eq!(first_limit.scope, ResourceLimitScopeV1::Session);
    assert_eq!(first_limit.kind, ResourceLimitKindV1::InMemoryState);
    assert_eq!(
        process.retry_create_session(&first_spec),
        Err(SessionError::ResourceLimitReached(first_limit.clone()))
    );
    let facts_after_first = durable_fact_entries(&persistence_root);
    assert_eq!(
        process.retry_create_session(&first_spec),
        Err(SessionError::ResourceLimitReached(first_limit.clone()))
    );
    assert_eq!(durable_fact_entries(&persistence_root), facts_after_first);

    let second_limit =
        match process.create_session(second_spec.clone(), BridgeProbeV1::default().bridge()) {
            Err(SessionError::ResourceLimitReached(limit)) => limit,
            Ok(_) => panic!("第二个未创建会话必须形成局部墓碑"),
            Err(other) => panic!("第二个墓碑必须返回强类型限额，实际为 {other:?}"),
        };
    assert_eq!(second_limit.scope, ResourceLimitScopeV1::Session);
    assert_eq!(second_limit.kind, ResourceLimitKindV1::InMemoryState);
    let facts_after_second = durable_fact_entries(&persistence_root);
    assert_eq!(
        facts_after_second
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        2
    );
    let resources_after_second = process
        .resource_snapshot_for_tests()
        .expect("读取两个墓碑后的资源");
    assert_eq!(
        resources_after_second.run.in_memory_state.records,
        baseline_run_records + 2
    );
    assert_eq!(resources_after_second.run.sessions.records, 0);
    drop(process);

    let recover = || {
        let recovery_rules = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
            snapshot_path: recovery_path.clone(),
            expected_file_sha256: current_rules.file_sha256().clone(),
            expected_implementation_identity: current_rules
                .snapshot()
                .provenance
                .implementation_identity
                .clone(),
        })
        .expect("加载墓碑恢复规则");
        let reader =
            PaperMarketEventRecoveryReaderV1::try_new(source_identity.clone(), |_request| {
                Err(PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(
                    "未创建会话不得形成市场桶恢复范围".to_owned(),
                ))
            })
            .expect("构造墓碑恢复读取端口");
        SimExecutionProcessV1::recover_paper_from_local_persistence(
            successful_policy_for_tests(),
            recovery_rules,
            Arc::clone(&profile),
            Arc::clone(&scenario),
            reader,
            persistence_root.clone(),
        )
        .expect("恢复未创建会话墓碑")
    };
    let mut recovered = recover();
    assert_eq!(
        recovered.retry_create_session(&first_spec),
        Err(SessionError::ResourceLimitReached(first_limit))
    );
    assert_eq!(
        recovered.retry_create_session(&second_spec),
        Err(SessionError::ResourceLimitReached(second_limit))
    );
    assert_eq!(durable_fact_entries(&persistence_root), facts_after_second);

    let shared =
        match recovered.create_session(third_spec.clone(), BridgeProbeV1::default().bridge()) {
            Err(SessionError::ResourceLimitReached(limit)) => limit,
            Ok(_) => panic!("第三个未创建会话必须有界提升为共享屏障"),
            Err(other) => panic!("第三个墓碑提升必须返回强类型限额，实际为 {other:?}"),
        };
    assert_eq!(shared.scope, ResourceLimitScopeV1::Run);
    assert_eq!(shared.kind, ResourceLimitKindV1::InMemoryState);
    let facts_after_shared = durable_fact_entries(&persistence_root);
    assert_eq!(
        facts_after_shared
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        3,
        "第三个尝试必须以单个组合共享事实续租"
    );
    assert_eq!(
        facts_after_shared
            .last()
            .and_then(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED"))
            .and_then(|body| body.get("triggering_session_limit"))
            .and_then(|value| value.get("scope"))
            .and_then(Value::as_str),
        Some("SESSION")
    );
    drop(recovered);

    let mut recovered = recover();
    assert_eq!(
        recovered.retry_create_session(&first_spec),
        Err(SessionError::ResourceLimitReached(shared.clone())),
        "共享屏障形成后必须优先复用唯一运行级失败事实"
    );
    assert!(matches!(
        recovered.create_session(fourth_spec, BridgeProbeV1::default().bridge()),
        Err(SessionError::ResourceLimitReached(repeated)) if repeated == shared
    ));
    assert_eq!(
        durable_fact_entries(&persistence_root),
        facts_after_shared,
        "共享屏障后的新会话重试不得追加第四个失败事实"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task442_session_quota_closes_only_the_affected_trajectory() {
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.session.request_queue = ResourceLimitV1::new(2, policy.session.request_queue.max_bytes);
    policy.run.sessions = ResourceLimitV1::new(2, policy.run.sessions.max_bytes);
    let mut fixture = PersistentSessionPairV1::new(policy).await;

    let first_account_before = fixture.first.account_snapshot_for_tests();
    let second_account_before = fixture.second.account_snapshot_for_tests();
    let second_cursor_before = fixture.second.model_visible_cursor();
    let second_output_before = fixture
        .second
        .task41_diagnostics_for_tests(&market().condition_id.0);
    let first_request = order_request(
        &fixture.first,
        &fixture.profile,
        "task442-a-first",
        1,
        150_000,
    );
    assert_eq!(
        fixture
            .first
            .order(first_request.clone())
            .expect("轨迹 A 首个请求应恰好落在配额内")
            .receipt
            .disposition,
        RequestRegistrationDispositionV1::NewLogicalRequest
    );

    let limit = match fixture.first.order(order_request(
        &fixture.first,
        &fixture.profile,
        "task442-a-over-limit",
        2,
        160_000,
    )) {
        Err(SessionError::ResourceLimitReached(limit)) => limit,
        other => panic!("轨迹 A 第二个请求必须形成会话级请求队列超限，实际为 {other:?}"),
    };
    assert_eq!(limit.scope, ResourceLimitScopeV1::Session);
    assert_eq!(limit.kind, ResourceLimitKindV1::RequestQueue);
    assert_eq!(limit.exceeded_dimension, ResourceLimitDimensionV1::Records);
    assert_eq!(limit.limit.max_records, 2);
    assert!(limit.candidate.records > limit.limit.max_records);

    assert_eq!(
        fixture.first.account_snapshot_for_tests(),
        first_account_before
    );
    assert_eq!(
        fixture.second.account_snapshot_for_tests(),
        second_account_before
    );
    assert_eq!(fixture.second.model_visible_cursor(), second_cursor_before);
    assert_eq!(
        fixture
            .second
            .task41_diagnostics_for_tests(&market().condition_id.0),
        second_output_before,
        "轨迹 A 的局部超限不得改变轨迹 B 的目录、输出或查询状态"
    );
    assert_eq!(fixture.first_probe.callback_count(), 0);
    assert_eq!(fixture.second_probe.callback_count(), 0);

    let evidence_after_limit = durable_fact_entries(&fixture.persistence_root);
    let resource_limits = evidence_after_limit
        .iter()
        .filter_map(|(sequence, fact)| {
            fact.get("RESOURCE_LIMIT_REACHED")
                .map(|body| (*sequence, body))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        resource_limits.len(),
        1,
        "局部超限必须且只能持久化一项失败事实"
    );
    let (limit_sequence, persisted_limit) = resource_limits[0];
    assert_eq!(
        persisted_limit.get("session_id").and_then(Value::as_str),
        Some(fixture.first.session_spec().session_id.as_str())
    );
    assert_eq!(
        persisted_limit
            .pointer("/limit/scope")
            .and_then(Value::as_str),
        Some("SESSION")
    );
    assert_eq!(
        persisted_limit
            .pointer("/limit/kind")
            .and_then(Value::as_str),
        Some("REQUEST_QUEUE")
    );

    let evidence_count_before_retry = evidence_after_limit.len();
    assert_eq!(
        fixture
            .first
            .order(first_request)
            .expect("受限会话必须仍允许既有请求的精确物理重投")
            .receipt
            .disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        durable_fact_entries(&fixture.persistence_root).len(),
        evidence_count_before_retry,
        "物理重投不得伪造另一项失败或请求事实"
    );
    assert!(matches!(
        fixture.first.order(order_request(
            &fixture.first,
            &fixture.profile,
            "task442-a-still-closed",
            3,
            170_000,
        )),
        Err(SessionError::ResourceLimitReached(repeated)) if repeated == limit
    ));

    assert_eq!(
        fixture
            .second
            .order(order_request(
                &fixture.second,
                &fixture.profile,
                "task442-b-still-open",
                1,
                150_000,
            ))
            .expect("轨迹 B 必须仍可登记新逻辑请求")
            .receipt
            .disposition,
        RequestRegistrationDispositionV1::NewLogicalRequest
    );
    let final_evidence = durable_fact_entries(&fixture.persistence_root);
    let second_registration_sequence = final_evidence
        .iter()
        .filter_map(|(sequence, fact)| {
            fact.get("REQUEST_REGISTERED")
                .filter(|body| {
                    body.get("session_id").and_then(Value::as_str)
                        == Some(fixture.second.session_spec().session_id.as_str())
                })
                .map(|_| *sequence)
        })
        .next_back()
        .expect("轨迹 B 新请求必须进入同一仅追加证据前缀");
    assert!(second_registration_sequence > limit_sequence);
    assert_eq!(
        fixture.first.account_snapshot_for_tests(),
        first_account_before
    );
    assert_eq!(
        fixture.second.account_snapshot_for_tests(),
        second_account_before
    );
    assert_eq!(
        final_evidence
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "已关闭会话的后续新接入必须复用原失败事实"
    );

    let first_session_id = fixture.first.session_spec().session_id.clone();
    let second_session_id = fixture.second.session_spec().session_id.clone();
    complete_task442_clean_session(
        &mut fixture.process,
        fixture.second,
        &fixture.second_control,
    );
    let after_second_completion = fixture
        .process
        .resource_snapshot_for_tests()
        .expect("读取轨迹 B 完成后的真实运行资源状态");
    assert!(
        after_second_completion
            .sessions
            .contains_key(&first_session_id)
    );
    assert!(
        !after_second_completion
            .sessions
            .contains_key(&second_session_id),
        "轨迹 A 的局部限额不得阻断轨迹 B 真实完成"
    );
    complete_task442_clean_session(&mut fixture.process, fixture.first, &fixture.first_control);
    assert!(
        fixture
            .process
            .resource_snapshot_for_tests()
            .expect("读取两轨迹完成后的真实运行资源状态")
            .sessions
            .is_empty(),
        "两条既有轨迹都必须通过真实 clean-run 路径移出活动会话"
    );
    let completed_session_ids = durable_fact_entries(&fixture.persistence_root)
        .iter()
        .filter_map(|(_, fact)| {
            fact.get("SESSION_CLEAN_RUN_COMPLETED")
                .and_then(|body| body.get("session_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        completed_session_ids,
        BTreeSet::from([first_session_id, second_session_id]),
        "两条轨迹必须各自留下唯一 clean-run 完成事实"
    );
    let receipt = fixture
        .process
        .seal_run_artifacts()
        .expect("会话局部限额后两条既有轨迹必须仍可封存");
    assert!(receipt.artifact_directory.is_dir());
}

#[tokio::test(flavor = "current_thread")]
async fn task442_shared_persistence_failure_stops_both_ingresses_without_cross_pollution() {
    let mut fixture =
        PersistentSessionPairV1::new(SimulationResourcePolicyV1::component_golden_v1()).await;
    let first_account_before = fixture.first.account_snapshot_for_tests();
    let second_account_before = fixture.second.account_snapshot_for_tests();
    let first_cursor_before = fixture.first.model_visible_cursor();
    let second_cursor_before = fixture.second.model_visible_cursor();
    let first_output_before = fixture
        .first
        .task41_diagnostics_for_tests(&market().condition_id.0);
    let second_output_before = fixture
        .second
        .task41_diagnostics_for_tests(&market().condition_id.0);
    let committed_evidence_before = durable_fact_entries(&fixture.persistence_root);

    fixture
        .process
        .fail_next_durable_commit_after_checkpoint_before_evidence_for_tests();
    assert!(matches!(
        fixture.first.order(order_request(
            &fixture.first,
            &fixture.profile,
            "task442-shared-failure-a",
            1,
            150_000,
        )),
        Err(SessionError::DurabilityRecoveryRequired(_))
    ));
    assert!(matches!(
        fixture.second.order(order_request(
            &fixture.second,
            &fixture.profile,
            "task442-shared-failure-b",
            1,
            150_000,
        )),
        Err(SessionError::DurabilityRecoveryRequired(_))
    ));
    assert!(matches!(
        fixture.first.order(order_request(
            &fixture.first,
            &fixture.profile,
            "task442-shared-failure-a-retry",
            2,
            160_000,
        )),
        Err(SessionError::DurabilityRecoveryRequired(_))
    ));

    assert_eq!(
        durable_fact_entries(&fixture.persistence_root),
        committed_evidence_before,
        "检查点发布后、证据提交前的结果不确定故障不得把半项事实伪装为已提交"
    );
    assert_eq!(
        fixture.first.account_snapshot_for_tests(),
        first_account_before
    );
    assert_eq!(
        fixture.second.account_snapshot_for_tests(),
        second_account_before
    );
    assert_eq!(fixture.first.model_visible_cursor(), first_cursor_before);
    assert_eq!(fixture.second.model_visible_cursor(), second_cursor_before);
    assert_eq!(
        fixture
            .first
            .task41_diagnostics_for_tests(&market().condition_id.0),
        first_output_before
    );
    assert_eq!(
        fixture
            .second
            .task41_diagnostics_for_tests(&market().condition_id.0),
        second_output_before
    );
    assert_eq!(fixture.first_probe.callback_count(), 0);
    assert_eq!(fixture.second_probe.callback_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn task442_run_input_limit_persists_typed_failure_and_recovers() {
    let directory = tempfile::tempdir().expect("创建运行级输入限额临时目录");
    let persistence_root = directory.path().join("run");
    let current_rules = successful_snapshot_for_sim_tests().await;
    let recovery_path = directory.path().join("platform-rule-snapshot.json");
    std::fs::write(&recovery_path, current_rules.file_bytes()).expect("写入运行级限额恢复规则");
    let profile = load_embedded_proxy_latency_profile().expect("任务 4.4 代理时延配置档");
    let scenario = scenario();
    let source_identity = Sha256Hex(format!(
        "{:x}",
        Sha256::digest(b"TASK44_RUN_INPUT_LIMIT_EVENT_SOURCE_V1")
    ));
    let intent = SimulationRunIntentBindingV1::try_new(
        current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
        SimulationWorkloadKindV1::RealVertical,
        SimulationDeterministicScriptContractV1::RealVerticalV1,
    )
    .expect("运行级输入限额意图");
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.run.input_queue = ResourceLimitV1::new(1, policy.run.input_queue.max_bytes);
    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            "task442-run-input-limit".to_owned(),
            successful_policy_for_tests(),
            current_rules.clone(),
            Arc::clone(&profile),
            Arc::clone(&scenario),
            intent,
            source_identity.clone(),
            persistence_root.clone(),
            policy,
        )
        .expect("创建运行级输入限额运行根");
    let mut spec = session_spec(
        &scenario,
        SimCallerRoleV1::Paper,
        "task442-run-input-session",
    );
    spec.run_id = "task442-run-input-limit".to_owned();
    let prepared = process
        .create_session(spec.clone(), BridgeProbeV1::default().bridge())
        .expect("创建运行级输入限额会话");
    process
        .activate_session(activation_request(&spec, &prepared.control))
        .expect("激活运行级输入限额会话");
    let mut session = prepared.session;
    let events = initial_events("0.01", "0.6", "1000");
    let encoded = events
        .iter()
        .map(|event| encode_execution_market_event_v3(event).expect("编码真实闭桶输入"))
        .collect::<Vec<_>>();
    let bucket = ClosedMarketEventBucketV1::public(
        120_000,
        0,
        2,
        MillisecondBucketClosureV1::NextEventAt(120_001),
        encoded,
    );
    let limit = match session.apply_closed_market_event_bucket(bucket.clone()) {
        Err(crate::market::MarketEventErrorV1::Session(SessionError::ResourceLimitReached(
            limit,
        ))) => limit,
        other => panic!("真实双事件闭桶必须命中运行级输入队列上限，实际为 {other:?}"),
    };
    assert_eq!(limit.scope, ResourceLimitScopeV1::Run);
    assert_eq!(limit.kind, ResourceLimitKindV1::InputQueue);
    assert_eq!(limit.exceeded_dimension, ResourceLimitDimensionV1::Records);
    assert_eq!(limit.limit.max_records, 1);
    assert!(limit.candidate.records > limit.limit.max_records);
    let facts = durable_fact_entries(&persistence_root);
    assert_eq!(
        facts
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "运行级输入超限必须留下唯一失败事实"
    );
    assert_eq!(
        facts
            .iter()
            .filter(|(_, fact)| fact.get("PLATFORM_BUCKET_APPLIED").is_some())
            .count(),
        0,
        "被拒闭桶不得伪装为已经应用"
    );
    let empty_eof = ClosedMarketEventBucketV1::public(
        120_001,
        0,
        0,
        MillisecondBucketClosureV1::EndOfInput,
        Vec::<Vec<u8>>::new(),
    );
    assert!(matches!(
        session.apply_closed_market_event_bucket(empty_eof.clone()),
        Err(crate::market::MarketEventErrorV1::Session(SessionError::ResourceLimitReached(
            repeated
        ))) if repeated == limit
    ));
    assert_eq!(
        durable_fact_entries(&persistence_root)
            .iter()
            .filter(|(_, fact)| fact.get("PLATFORM_BUCKET_APPLIED").is_some())
            .count(),
        0,
        "输入正文被拒后不得用同游标空 EOF 跳过缺口并伪造 Exhausted"
    );
    let mut session = match process.complete_clean_run_session(session) {
        Err(SessionRetirementFailureV1::Rejected {
            error: SessionError::RetentionBarrierNotSatisfied(_),
            session,
        }) => *session,
        Ok(()) | Err(_) => panic!("被拒输入与假 EOF 不得完成 clean run"),
    };
    session = match process.retire_session(session) {
        Err(SessionRetirementFailureV1::Rejected {
            error: SessionError::RetentionBarrierNotSatisfied(_),
            session,
        }) => *session,
        Ok(()) | Err(_) => panic!("被拒输入与假 EOF 不得退役模拟盘会话"),
    };
    assert!(matches!(
        process.seal_run_artifacts(),
        Err(SimulationRunArtifactErrorV1::RunNotReady(
            "仍有尚未退休的活动会话"
        ))
    ));
    let facts_after_rejected_completion = durable_fact_entries(&persistence_root);
    assert!(
        facts_after_rejected_completion.iter().all(|(_, fact)| {
            fact.get("SESSION_CLEAN_RUN_COMPLETED").is_none()
                && fact.get("SESSION_RETENTION_EXPIRED").is_none()
                && fact.get("RUN_ARTIFACT_INPUTS_SEALED").is_none()
        }),
        "输入超限后不得伪造会话完成、退役或运行封口事实"
    );
    assert!(
        !persistence_root.join("artifacts").exists(),
        "RunNotReady 不得暴露正式产物根"
    );
    drop(session);
    drop(process);

    let recovery_rules = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
        snapshot_path: recovery_path,
        expected_file_sha256: current_rules.file_sha256().clone(),
        expected_implementation_identity: current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
    })
    .expect("加载运行级输入限额恢复规则");
    let reader = PaperMarketEventRecoveryReaderV1::try_new(source_identity, |_request| {
        Err(PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(
            "被运行级资源门拒绝的闭桶不应进入恢复读取范围".to_owned(),
        ))
    })
    .expect("构造未提交桶的窄恢复读取端口");
    let mut recovered = SimExecutionProcessV1::recover_paper_from_local_persistence(
        successful_policy_for_tests(),
        recovery_rules,
        profile,
        scenario,
        reader,
        persistence_root.clone(),
    )
    .expect("运行级资源失败事实必须可恢复");
    let mut recovered_session = recovered
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            BridgeProbeV1::default().bridge(),
        )
        .expect("重新附接运行级资源失败会话");
    assert!(matches!(
        recovered_session.apply_closed_market_event_bucket(bucket),
        Err(crate::market::MarketEventErrorV1::Session(SessionError::ResourceLimitReached(
            repeated
        ))) if repeated == limit
    ));
    assert!(matches!(
        recovered_session.apply_closed_market_event_bucket(empty_eof),
        Err(crate::market::MarketEventErrorV1::Session(SessionError::ResourceLimitReached(
            repeated
        ))) if repeated == limit
    ));
    assert!(matches!(
        recovered.seal_run_artifacts(),
        Err(SimulationRunArtifactErrorV1::RunNotReady(
            "仍有尚未退休的活动会话"
        ))
    ));
    assert_eq!(
        durable_fact_entries(&persistence_root)
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "恢复后的重试必须复用同一运行级失败事实"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task442_artifact_limit_after_seal_is_persisted_and_recoverable() {
    let directory = tempfile::tempdir().expect("创建任务 4.4 产物限额临时目录");
    let persistence_root = directory.path().join("run");
    let current_rules = successful_snapshot_for_sim_tests().await;
    let recovery_path = directory.path().join("platform-rule-snapshot.json");
    std::fs::write(&recovery_path, current_rules.file_bytes()).expect("写入任务 4.4 恢复规则快照");
    let recovery_rules = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
        snapshot_path: recovery_path,
        expected_file_sha256: current_rules.file_sha256().clone(),
        expected_implementation_identity: current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
    })
    .expect("重载任务 4.4 恢复规则快照");
    let profile = load_embedded_proxy_latency_profile().expect("任务 4.4 代理时延配置档");
    let scenario = scenario();
    let intent = SimulationRunIntentBindingV1::try_new(
        current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
        SimulationWorkloadKindV1::RealVertical,
        SimulationDeterministicScriptContractV1::RealVerticalV1,
    )
    .expect("任务 4.4 产物限额运行意图");
    let source_identity = Sha256Hex(format!(
        "{:x}",
        Sha256::digest(b"TASK44_ARTIFACT_LIMIT_EVENT_SOURCE_V1")
    ));
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.durable.artifacts = ResourceLimitV1::new(64, 1 << 20);
    policy.durable.runtime_root = ResourceLimitV1::new(1_000_000, 16 << 20);
    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            RUN_ID.to_owned(),
            successful_policy_for_tests(),
            Arc::clone(&current_rules),
            Arc::clone(&profile),
            Arc::clone(&scenario),
            intent,
            source_identity,
            persistence_root.clone(),
            policy,
        )
        .expect("创建任务 4.4 产物限额运行根");
    let probe = BridgeProbeV1::default();
    let mut spec = session_spec(
        &scenario,
        SimCallerRoleV1::Paper,
        "task442-artifact-limit-session",
    );
    spec.run_id = RUN_ID.to_owned();
    let prepared = process
        .create_session(spec.clone(), probe.bridge())
        .expect("创建待失败关闭的模拟盘会话");
    process
        .fail_session_initialization(failure_request(&spec, &prepared.control))
        .expect("经真实初始化控制入口关闭会话");
    drop(prepared.session);

    process.fail_before_artifact_bundle_publish_once_for_tests();
    assert!(matches!(
        process.seal_run_artifacts(),
        Err(SimulationRunArtifactErrorV1::Durability(_))
    ));
    let artifact_root = persistence_root.join("artifacts");
    let candidate = std::fs::read_dir(&artifact_root)
        .expect("读取注入发布故障后留下的正式候选")
        .map(|entry| entry.expect("读取正式候选目录项"))
        .find(|entry| {
            entry.file_type().is_ok_and(|file_type| file_type.is_dir())
                && entry.file_name().to_string_lossy().starts_with('.')
                && entry.file_name().to_string_lossy().ends_with(".candidate")
        })
        .expect("发布前故障必须留下唯一候选")
        .path();
    let oversized_member = candidate.join(SIMULATION_RUN_AUDIT_FILE_NAME_V1);
    assert!(oversized_member.is_file(), "放大前必须是正式声明成员");
    std::fs::write(&oversized_member, vec![b'x'; (2 << 20) + 1])
        .expect("放大一个合法命名的正式候选成员");

    let limit = match process.seal_run_artifacts() {
        Err(SimulationRunArtifactErrorV1::ResourceLimitReached(limit)) => limit,
        other => panic!("正式包预览必须命中产物硬上限，实际为 {other:?}"),
    };
    assert_eq!(limit.scope, ResourceLimitScopeV1::Durable);
    assert_eq!(limit.kind, ResourceLimitKindV1::Artifacts);
    assert_eq!(limit.exceeded_dimension, ResourceLimitDimensionV1::Bytes);
    assert_eq!(limit.limit.max_bytes, 1 << 20);
    assert!(limit.candidate.bytes > limit.limit.max_bytes);
    assert!(!candidate.exists(), "持久失败事实写入前必须清理已验证候选");
    assert!(
        !persistence_root.join("artifacts").exists()
            || std::fs::read_dir(persistence_root.join("artifacts"))
                .expect("读取空产物目录")
                .next()
                .is_none(),
        "预览超限不得暴露正式包或候选包"
    );
    let facts = durable_fact_entries(&persistence_root);
    let persisted = facts
        .iter()
        .filter_map(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED"))
        .collect::<Vec<_>>();
    assert_eq!(persisted.len(), 1, "封口后的耐久超限必须持久化唯一失败事实");
    assert!(persisted[0].get("session_id").is_some_and(Value::is_null));
    assert_eq!(
        persisted[0].pointer("/limit/kind").and_then(Value::as_str),
        Some("ARTIFACTS")
    );
    drop(process);

    let mut recovered = SimExecutionProcessV1::recover_for_artifact_publication(
        successful_policy_for_tests(),
        Arc::clone(&recovery_rules),
        Arc::clone(&profile),
        Arc::clone(&scenario),
        persistence_root.clone(),
    )
    .expect("产物预览超限后必须仍可恢复封口运行");
    let recovered_receipt = recovered
        .seal_run_artifacts()
        .expect("遗留放大候选清理后，规范正式包在硬限内必须成功发布");
    assert!(recovered_receipt.artifact_directory.is_dir());
    assert_eq!(
        durable_fact_entries(&persistence_root)
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "恢复后成功发布规范包不得追加第二个失败事实"
    );

    let record_run_id = "task442-artifact-record-limit-run".to_owned();
    let record_root = directory.path().join("record-limit-run");
    let record_intent = SimulationRunIntentBindingV1::try_new(
        current_rules
            .snapshot()
            .provenance
            .implementation_identity
            .clone(),
        SimulationWorkloadKindV1::RealVertical,
        SimulationDeterministicScriptContractV1::RealVerticalV1,
    )
    .expect("构造规范六成员固有限额运行意图");
    let mut record_policy = SimulationResourcePolicyV1::component_golden_v1();
    record_policy.durable.artifacts = ResourceLimitV1::new(5, u64::MAX);
    let mut record_process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            record_run_id.clone(),
            successful_policy_for_tests(),
            current_rules,
            Arc::clone(&profile),
            Arc::clone(&scenario),
            record_intent,
            Sha256Hex(format!(
                "{:x}",
                Sha256::digest(b"TASK44_ARTIFACT_RECORD_LIMIT_EVENT_SOURCE_V1")
            )),
            record_root.clone(),
            record_policy,
        )
        .expect("创建规范六成员固有限额运行根");
    let mut record_spec = session_spec(
        &scenario,
        SimCallerRoleV1::Paper,
        "task442-artifact-record-limit-session",
    );
    record_spec.run_id = record_run_id;
    let record_prepared = record_process
        .create_session(record_spec.clone(), BridgeProbeV1::default().bridge())
        .expect("创建规范六成员固有限额会话");
    record_process
        .fail_session_initialization(failure_request(&record_spec, &record_prepared.control))
        .expect("关闭规范六成员固有限额会话");
    drop(record_prepared.session);
    let record_limit = match record_process.seal_run_artifacts() {
        Err(SimulationRunArtifactErrorV1::ResourceLimitReached(limit)) => limit,
        other => panic!("规范六成员必须命中固有记录限额，实际为 {other:?}"),
    };
    assert_eq!(record_limit.kind, ResourceLimitKindV1::Artifacts);
    assert_eq!(
        record_limit.exceeded_dimension,
        ResourceLimitDimensionV1::Records
    );
    assert_eq!(record_limit.limit.max_records, 5);
    assert_eq!(record_limit.candidate.records, 6);
    drop(record_process);
    let mut record_recovered = SimExecutionProcessV1::recover_for_artifact_publication(
        successful_policy_for_tests(),
        recovery_rules,
        profile,
        scenario,
        record_root.clone(),
    )
    .expect("恢复规范六成员固有限额运行");
    let repeated_record_result = record_recovered.seal_run_artifacts();
    assert!(
        matches!(
            &repeated_record_result,
            Err(SimulationRunArtifactErrorV1::ResourceLimitReached(repeated))
                if repeated == &record_limit
        ),
        "规范六成员恢复重试必须返回原固有限额 {record_limit:?}，实际为 {repeated_record_result:?}"
    );
    assert_eq!(
        durable_fact_entries(&record_root)
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "规范六成员固有超限恢复重试必须复用同一失败事实"
    );
}
