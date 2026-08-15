use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use contracts::{
    AccountInstanceKeyV1, AssetIdentityV1, BookSide, CanonicalEventId, ClobMarketId, ConditionId,
    EventLineageV2, EventQualityV2, EventTimeV2, ExactDecimal, ExecutionMarketEventPayloadV3,
    ExecutionMarketEventV3, InitialL2SnapshotV2, LevelExecutionBasis, LevelExecutionTransitionV3,
    MarketDataQuality, MarketIdentityV2, MarketResolutionTerminalV3, MarketWindowV2,
    OutcomeIdentityV1, PayoutEntryV1, PlatformAccountDeltaPredecessorV1, PlatformAccountDeltaV1,
    PriceLevelV1, QueueResultQuality, ResyncQuality, Sha256Hex, SourceAttributionQuality, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::artifacts::{
    SIMULATION_RUN_AUDIT_FILE_NAME_V1, SIMULATION_RUN_MANIFEST_FILE_NAME_V1,
    SimulationDeterministicScriptContractV1, SimulationResourceObservationV1,
    SimulationRunArtifactErrorV1, SimulationRunAuditV1, SimulationRunIntentBindingV1,
    SimulationRunManifestV1, SimulationWorkloadKindV1,
};
use crate::execution::{
    AccountDeltaReferenceV1, CancelAllOrdersDirectResponseV1, CancelAllOrdersRequestV1,
    CancelOrderDirectResponseV1, CancelOrderItemV1, CancelOrderRequestV1,
    CancelOrdersDirectResponseV1, CancelOrdersRequestV1, CancelTargetOutcomeV1,
    CancelTargetResponseV1, DownstreamDurabilityProofV1, InitialAccountStateV1,
    OrderDirectResponseV1, OrderLifecycleFeedbackMetadataV1, OrderLifecycleFeedbackV1,
    OrderProjectionV1, OrderQueryResultV1, OrderRequestV1, OrdersDirectResponseV1, OrdersFilterV1,
    OrdersQueryResultV1, OrdersRequestV1, PaperMarketEventRecoveryReadErrorV1,
    PaperMarketEventRecoveryReaderV1, PostOrderDirectResponseV1, PostOrderItemV1,
    PostOrderRequestV1, PostOrdersDirectResponseV1, PostOrdersRequestV1,
    ResourceTransactionObservationV1, SessionRetirementFailureV1, SettlementLifecycleEventV1,
    SettlementLifecycleFeedbackMetadataV1, SettlementLifecycleFeedbackV1, SimBuyOrderKindV1,
    SimBuyOrderV1, SimExecutionProcessV1, SimExecutionSessionV1, SubmissionDeliveryDispositionV1,
    SubmissionDriveDispositionV1, SubmissionItemOutcomeV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, SubmissionResponseItemV1, SubmissionResponseMetadataV1,
    TaskQueryMethodV1, TaskQueryProjectionSourceV1, TaskQuerySourceDiagnosticV1,
    TradeLifecycleFeedbackMetadataV1, TradeLifecycleFeedbackV1, TradeProjectionV1,
    TradesDirectResponseV1, TradesFilterV1, TradesQueryResultV1, TradesRequestV1,
};
use crate::finality::successful_policy_for_tests;
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventPathV1,
    MillisecondBucketClosureV1,
};
use crate::measurement::{
    COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1, COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1,
    COMPONENT_GOLDEN_TRIAL_ORDER_V1, CgroupMemoryEventsV1, CgroupMemoryLimitV1, CgroupPidsLimitV1,
    CgroupV2ResourceSnapshotV1, ComponentGoldenEventKindCountsV1, ComponentGoldenLoadContractV1,
    ComponentGoldenMeasurementModeV1, ComponentGoldenMeasurementReportV1,
    ComponentGoldenQueryMethodBreakdownV1, ComponentGoldenQueryMethodsV1,
    ComponentGoldenQuerySourceBreakdownV1, ComponentGoldenRunMeasurementV1,
    ComponentGoldenTrajectoryV1, ComponentGoldenTrialIdV1, ComponentGoldenTrialReportV1,
    FROZEN_BUDGET_VERIFICATION_TRIALS_V1, FailureReserveObservationV1,
    FrozenBudgetVerificationTrialIdV1, FrozenBudgetVerificationTrialV1, FrozenResourceBudgetsV1,
    INDEPENDENT_FAILURE_RESERVE_PROBE_SCHEMA_V1, IndependentFailureReserveProbeV1,
    InternalResourceSnapshotV1, LogicalRollingCycleIdV1, LogicalRollingCycleV1,
    MEASUREMENT_LATENCY_KINDS_V1, MeasuredResourceSnapshotV1, MeasurementBuildGroupV1,
    MeasurementBuildIdentityV1, MeasurementCachePreconditionV1, MeasurementEnvironmentIdentityV1,
    MeasurementFilesystemIdentityV1, MeasurementHostIdentityV1, MeasurementLatencyKindV1,
    MeasurementRunIdentityV1, MeasurementRuntimeHardGuardsV1, MeasurementTrialOutcomeV1,
    ProcfsIoSnapshotV1, ProcfsProcessResourceSnapshotV1, RawLatencySamplesNsV1,
    ResourceBudgetPathV1, ResourceTransactionIncrementV1, SealedRunResourceEvidenceV1,
    SimulationRunArtifactReceiptBindingV1, SystemdServiceLifecycleResultV1,
    TRIAL_LIFECYCLE_TAIL_EVIDENCE_SCHEMA_V1, ThroughputBreakdownV1,
    ThroughputRatesPerSecondMilliV1, TrajectoryLogicalComponentV1, TrajectoryLogicalSummaryInputV1,
    TrajectoryRuntimeBindingV1, TrajectoryThroughputV1, TrajectoryTrialResultV1,
    TrialLifecycleTailBindingV1, TrialLifecycleTailEvidenceV1, TrialThroughputV1,
    derive_failure_reserve_observation_v1, derive_resource_budget_observations_v1,
    freeze_resource_budgets_v1, lifecycle_process_peaks_v1, validate_lifecycle_tail_binding_v1,
};
use crate::platform_rules::tests::{
    historical_snapshot_for_sim_tests_with_fee_schedule,
    historical_snapshot_for_task44_with_fee_schedule_and_identity,
    successful_snapshot_for_sim_tests,
};
use crate::platform_rules::{
    HistoricalProcessSnapshotRequestV1, ImplementationIdentityV1, RecoveryProcessSnapshotRequestV1,
    SealedProcessRuleSnapshot, SnapshotAccessMode, load_historical_process_snapshot,
    recover_process_snapshot,
};
use crate::resource::{
    COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1, ResourceLimitDimensionV1, ResourceLimitKindV1,
    ResourceLimitScopeV1, ResourceLimitV1, ResourceQuantityV1, SimulationFailureEvidenceReserveV1,
    SimulationResourcePolicyV1, SimulationResourceUsageV1,
};
use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};
use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, InputPathSealOutcomeV1,
    RequestRegistrationDispositionV1, SessionError, SessionSpecV2, SimCallerRoleV1,
};
use crate::task21_tests::fresh_quality;
use crate::task22_tests::{
    context_for_arrival, initial_events, market, orderbook_evidence, scenario, session_spec,
};

const RUN_ID: &str = "task44-component-golden-run";

#[derive(Clone)]
struct GoldenInputV1 {
    scenario: Arc<PlatformScenarioV1>,
    markets: [PlatformMarketV1; 2],
    groups: Vec<GoldenEventGroupV1>,
    stream_sha256: Sha256Hex,
}

#[derive(Clone)]
struct GoldenEventGroupV1 {
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    events: Vec<ExecutionMarketEventV3>,
}

impl GoldenInputV1 {
    fn new() -> Self {
        let markets = [
            golden_market(1, 100_000, 400_000),
            golden_market(2, 250_000, 550_000),
        ];
        let m1 = markets[0].market().clone();
        let m2 = markets[1].market().clone();
        let groups = vec![
            GoldenEventGroupV1 {
                source_at_utc_ms: 110_000,
                received_at_utc_ms: 120_000,
                events: golden_initial_events(&m1, 1, 1),
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 260_000,
                received_at_utc_ms: 270_000,
                events: golden_initial_events(&m2, 3, 3),
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 310_000,
                received_at_utc_ms: 320_000,
                events: vec![golden_queue_transition(&m1, 1, 3, "0", "510")],
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 330_000,
                received_at_utc_ms: 340_000,
                events: vec![golden_queue_transition(&m1, 1, 4, "510", "0")],
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 410_000,
                received_at_utc_ms: 420_000,
                events: vec![golden_resolution(&m1, 1, 5)],
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 480_000,
                received_at_utc_ms: 490_000,
                events: vec![golden_queue_transition(&m2, 3, 6, "0", "5")],
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 540_000,
                received_at_utc_ms: 550_000,
                events: vec![golden_queue_transition(&m2, 3, 7, "5", "0")],
            },
            GoldenEventGroupV1 {
                source_at_utc_ms: 560_000,
                received_at_utc_ms: 570_000,
                events: vec![golden_resolution(&m2, 3, 8)],
            },
        ];
        let encoded = groups
            .iter()
            .flat_map(|group| group.events.iter())
            .map(|event| encode_execution_market_event_v3(event).expect("编码正式黄金事件"))
            .collect::<Vec<_>>();
        let stream_sha256 = sha256_serializable("TASK44_COMPONENT_GOLDEN_STREAM_V1", &encoded);
        let unique = groups
            .iter()
            .flat_map(|group| group.events.iter().map(|event| event.event_id.0.clone()))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            unique.len(),
            10,
            "正式黄金流必须由真实正文形成十个唯一事件身份"
        );
        assert_eq!(groups.iter().map(|group| group.events.len()).max(), Some(2));
        let scenario =
            Arc::new(PlatformScenarioV1::try_new(markets.to_vec()).expect("构造两个重叠黄金市场"));
        Self {
            scenario,
            markets,
            groups,
            stream_sha256,
        }
    }
}

fn golden_market(index: u32, start_at_utc_ms: i64, end_at_utc_ms: i64) -> PlatformMarketV1 {
    let condition = index.to_string().repeat(64);
    let first_token = u64::from(index) * 2 - 1;
    PlatformMarketV1::try_new(
        MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{condition}")),
            clob_market_id: ClobMarketId(format!("440{index}")),
            window: MarketWindowV2 {
                start_at_utc_ms,
                end_at_utc_ms,
            },
        },
        [
            golden_asset(first_token, 0, "Up"),
            golden_asset(first_token + 1, 1, "Down"),
        ],
    )
    .expect("构造正式黄金市场")
}

fn golden_asset(token_id: u64, index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token_id.to_string()),
        outcome: OutcomeIdentityV1 {
            index,
            label: label.to_owned(),
        },
    }
}

fn golden_initial_events(
    market: &MarketIdentityV2,
    first_token: u64,
    first_sequence: u64,
) -> Vec<ExecutionMarketEventV3> {
    [
        (golden_asset(first_token, 0, "Up"), first_sequence),
        (golden_asset(first_token + 1, 1, "Down"), first_sequence + 1),
    ]
    .into_iter()
    .map(|(asset, sequence)| {
        let asks = (0..9)
            .map(|offset| PriceLevelV1 {
                price_pusd_per_share: ExactDecimal(
                    Decimal::new(50 + offset, 2).normalize().to_string(),
                ),
                quantity_shares: ExactDecimal("1".to_owned()),
            })
            .collect();
        golden_sealed_event(
            market,
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
                book_digest_sha256: sha256_serializable(
                    "TASK44_INITIAL_BOOK_V1",
                    &(market, &asset, sequence),
                ),
                asset,
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
                source_sequence_epoch_id: "task44-component-golden-epoch-v1".to_owned(),
                source_cursor: sequence,
                bids: Vec::new(),
                asks,
            }),
            market.window.start_at_utc_ms + 10_000,
            market.window.start_at_utc_ms + 20_000,
            sequence,
            fresh_quality(),
        )
    })
    .collect()
}

fn golden_queue_transition(
    market: &MarketIdentityV2,
    token_id: u64,
    sequence: u64,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    let before_decimal = before.parse::<Decimal>().expect("黄金队列旧数量");
    let after_decimal = after.parse::<Decimal>().expect("黄金队列新数量");
    let source_at_utc_ms = match sequence {
        3 => 310_000,
        4 => 330_000,
        6 => 480_000,
        7 => 540_000,
        _ => panic!("正式黄金队列转换序号不受支持"),
    };
    let received_at_utc_ms = source_at_utc_ms + 10_000;
    golden_sealed_event(
        market,
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: sha256_serializable(
                "TASK44_QUEUE_CAUSAL_GROUP_V1",
                &(market, sequence),
            ),
            market: market.clone(),
            asset: golden_asset(token_id, 0, "Up"),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.3".to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(
                (after_decimal - before_decimal)
                    .max(Decimal::ZERO)
                    .normalize()
                    .to_string(),
            ),
            net_reduced_quantity_shares: ExactDecimal(
                (before_decimal - after_decimal)
                    .max(Decimal::ZERO)
                    .normalize()
                    .to_string(),
            ),
            queue_consumption_quantity_shares: ExactDecimal(
                (before_decimal - after_decimal)
                    .max(Decimal::ZERO)
                    .normalize()
                    .to_string(),
            ),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at_utc_ms,
            visible_at_utc_ms: received_at_utc_ms,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        source_at_utc_ms,
        received_at_utc_ms,
        sequence,
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
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

fn required_lower_hex_environment(name: &str, length: usize) -> String {
    let value = std::env::var(name)
        .unwrap_or_else(|_| panic!("正式 COMPONENT_GOLDEN_V1 缺少固定环境变量 {name}"));
    assert_eq!(value.len(), length, "{name} 长度非法");
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{name} 必须是小写十六进制"
    );
    value
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("sim-system crate 必须位于工作区根下")
        .to_path_buf()
}

fn command_stdout(command: &mut Command, label: &str) -> String {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("执行{label}失败：{error}"));
    assert!(
        output.status.success(),
        "{label}失败：{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap_or_else(|error| panic!("{label}输出不是 UTF-8：{error}"))
        .trim()
        .to_owned()
}

fn actual_source_tree_sha256() -> Sha256Hex {
    let root = repository_root();
    let output = Command::new("git")
        .args(["ls-files", "-co", "--exclude-standard", "-z"])
        .current_dir(&root)
        .output()
        .expect("枚举正式测量源树");
    assert!(output.status.success(), "git ls-files 必须成功");
    let mut paths = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8(path.to_vec()).expect("正式测量源树路径必须是 UTF-8"))
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    let mut hasher = Sha256::new();
    hasher.update(b"SIM_COMPONENT_GOLDEN_SOURCE_TREE_V1");
    for relative in paths {
        let bytes = fs::read(root.join(&relative))
            .unwrap_or_else(|error| panic!("读取正式测量源树成员 {relative} 失败：{error}"));
        hasher.update((relative.len() as u64).to_be_bytes());
        hasher.update(relative.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

fn formal_implementation_identity_from_environment() -> ImplementationIdentityV1 {
    let git_commit = required_lower_hex_environment("SIM_COMPONENT_GOLDEN_GIT_COMMIT", 40);
    let source_identity_sha256 = Sha256Hex(required_lower_hex_environment(
        "SIM_COMPONENT_GOLDEN_SOURCE_TREE_SHA256",
        64,
    ));
    let cargo_lock_sha256 = Sha256Hex(required_lower_hex_environment(
        "SIM_COMPONENT_GOLDEN_CARGO_LOCK_SHA256",
        64,
    ));
    let root = repository_root();
    let actual_commit = command_stdout(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&root),
        "读取当前 Git 提交",
    );
    assert_eq!(
        git_commit, actual_commit,
        "测量提交环境变量与当前仓库不一致"
    );
    assert_eq!(
        source_identity_sha256,
        actual_source_tree_sha256(),
        "测量源树环境变量与当前工作树正文不一致"
    );
    assert_eq!(
        cargo_lock_sha256,
        sha256_bytes(&fs::read(root.join("Cargo.lock")).expect("读取 Cargo.lock")),
        "测量 Cargo.lock 环境变量与当前锁文件不一致"
    );
    ImplementationIdentityV1 {
        git_commit,
        source_identity_sha256,
        cargo_lock_sha256,
    }
}

fn formal_output_root_from_environment() -> PathBuf {
    let root = PathBuf::from(
        std::env::var("SIM_COMPONENT_GOLDEN_OUTPUT_ROOT")
            .expect("正式 COMPONENT_GOLDEN_V1 缺少 SIM_COMPONENT_GOLDEN_OUTPUT_ROOT"),
    );
    assert!(root.is_absolute(), "正式测量输出根必须是绝对路径");
    fs::create_dir_all(&root).expect("创建正式测量输出根");
    fs::canonicalize(root).expect("规范化正式测量输出根")
}

fn formal_existing_output_root_from_environment() -> PathBuf {
    let root = PathBuf::from(
        std::env::var("SIM_COMPONENT_GOLDEN_OUTPUT_ROOT")
            .expect("正式 COMPONENT_GOLDEN_V1 缺少 SIM_COMPONENT_GOLDEN_OUTPUT_ROOT"),
    );
    assert!(root.is_absolute(), "正式测量输出根必须是绝对路径");
    assert!(root.is_dir(), "冻结策略预检只接受已存在的正式测量根");
    fs::canonicalize(root).expect("规范化已存在的正式测量根")
}

const FORMAL_FIXED_CONTEXT_FILE_V1: &str = "component-golden-fixed-context.json";
const FORMAL_FROZEN_BUDGETS_FILE_V1: &str = "frozen-resource-budgets.json";
const FORMAL_FINAL_REPORT_FILE_V1: &str = "component-golden-measurement-report.json";
const FORMAL_FINAL_REPORT_CONTENT_SHA_FILE_V1: &str =
    "component-golden-measurement-report.content.sha256";
const FORMAL_TRIAL_FILE_V1: &str = "component-golden-trial.json";
const FORMAL_LIFECYCLE_TAIL_FILE_V1: &str = "component-golden-lifecycle-tail.json";
const FORMAL_FAILURE_RESERVE_FILE_V1: &str = "component-golden-failure-reserve.json";
const FORMAL_RELEASE_BINARIES_DIRECTORY_V1: &str = "release-binaries";
const FORMAL_RULE_SNAPSHOTS_DIRECTORY_V1: &str = "rule-snapshots";
const FORMAL_RULE_REPLAY_CAUSAL_START_UTC_MS_V1: i64 = 100_000;
const FORMAL_B1_MEMORY_MAX_BYTES_V1: u64 = 120 * 1024 * 1024 * 1024;
const FORMAL_B1_MEMORY_SWAP_MAX_BYTES_V1: u64 = 8 * 1024 * 1024 * 1024;
const FORMAL_PIDS_MAX_V1: u64 = 4_096;
const FORMAL_LIFECYCLE_HANDSHAKE_SCHEMA_V1: &str = "component_golden_lifecycle_handshake_v1";
const FORMAL_RUN_ID_HASH_DOMAIN_V1: &str = "TASK44_COMPONENT_GOLDEN_FORMAL_RUN_ID_V1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FormalFilesystemFixedContextV1 {
    measurement_root_sha256: Sha256Hex,
    mount_identity_sha256: Sha256Hex,
    filesystem_type: String,
    block_size_bytes: u64,
    total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FormalHistoricalRuleSnapshotBindingV1 {
    snapshot_path: PathBuf,
    file_sha256: Sha256Hex,
    content_sha256: Sha256Hex,
    semantic_rules_sha256: Sha256Hex,
    replay_causal_start_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FormalMeasurementFixedContextV1 {
    schema_version: String,
    release_unit_test_binary: PathBuf,
    host: MeasurementHostIdentityV1,
    build: MeasurementBuildIdentityV1,
    filesystem: FormalFilesystemFixedContextV1,
    input_stream_sha256: Sha256Hex,
    runner_sha256: Sha256Hex,
    cache_precondition: MeasurementCachePreconditionV1,
    cache_precondition_sha256: Sha256Hex,
    historical_rules: FormalHistoricalRuleSnapshotBindingV1,
}

fn sha256_sidecar_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .expect("正式 JSON 路径必须有文件名")
        .to_os_string();
    name.push(".sha256");
    path.with_file_name(name)
}

fn formal_json_and_sidecar_exist_v1(path: &Path) -> bool {
    path.is_file() && sha256_sidecar_path(path).is_file()
}

#[test]
fn task441_ready_is_visible_only_after_json_and_sha256_sidecar_exist() {
    let directory = tempfile::tempdir().expect("创建 ready 可见性临时目录");
    let ready = directory.path().join("ready.json");
    File::create(&ready).expect("创建未完成 ready JSON");
    assert!(!formal_json_and_sidecar_exist_v1(&ready));
    File::create(sha256_sidecar_path(&ready)).expect("创建 ready SHA-256 摘要");
    assert!(formal_json_and_sidecar_exist_v1(&ready));
}

fn atomic_write_json_with_sha256<T: Serialize>(path: &Path, value: &T) -> Sha256Hex {
    let bytes = serde_json::to_vec_pretty(value).expect("编码正式 JSON 产物");
    let sha256 = sha256_bytes(&bytes);
    let sidecar = sha256_sidecar_path(path);
    assert!(
        !path.exists(),
        "正式 JSON 产物不得覆盖既有文件：{}",
        path.display()
    );
    assert!(
        !sidecar.exists(),
        "正式 JSON 摘要不得覆盖既有文件：{}",
        sidecar.display()
    );
    let parent = path.parent().expect("正式 JSON 产物必须有父目录");
    fs::create_dir_all(parent).expect("创建正式 JSON 产物父目录");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间不得早于 Unix epoch")
        .as_nanos();
    let candidate_suffix = format!("{}-{nonce}.candidate", std::process::id());
    let json_candidate = parent.join(format!(
        ".{}-{candidate_suffix}",
        path.file_name()
            .expect("正式 JSON 路径必须有文件名")
            .to_string_lossy()
    ));
    let sha_candidate = parent.join(format!(
        ".{}-{candidate_suffix}",
        sidecar
            .file_name()
            .expect("正式 JSON 摘要必须有文件名")
            .to_string_lossy()
    ));
    let write_candidate = |candidate: &Path, body: &[u8]| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(candidate)
            .unwrap_or_else(|error| panic!("创建正式候选 {} 失败：{error}", candidate.display()));
        file.write_all(body)
            .unwrap_or_else(|error| panic!("写入正式候选 {} 失败：{error}", candidate.display()));
        file.sync_all()
            .unwrap_or_else(|error| panic!("同步正式候选 {} 失败：{error}", candidate.display()));
    };
    write_candidate(&json_candidate, &bytes);
    write_candidate(&sha_candidate, format!("{}\n", sha256.0).as_bytes());
    fs::rename(&json_candidate, path).expect("原子发布正式 JSON 产物");
    fs::rename(&sha_candidate, &sidecar).expect("原子发布正式 JSON 摘要");
    File::open(parent)
        .expect("打开正式 JSON 父目录")
        .sync_all()
        .expect("同步正式 JSON 父目录");
    sha256
}

fn read_json_with_sha256<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let bytes = fs::read(path)
        .unwrap_or_else(|error| panic!("读取正式 JSON {} 失败：{error}", path.display()));
    let expected = fs::read_to_string(sha256_sidecar_path(path))
        .unwrap_or_else(|error| panic!("读取正式 JSON 摘要 {} 失败：{error}", path.display()));
    let actual = sha256_bytes(&bytes);
    assert_eq!(expected.trim(), actual.0, "正式 JSON 文件 SHA-256 不匹配");
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("严格解码正式 JSON {} 失败：{error}", path.display()))
}

fn atomic_write_text(path: &Path, body: &str) {
    assert!(
        !path.exists(),
        "正式文本产物不得覆盖既有文件：{}",
        path.display()
    );
    let parent = path.parent().expect("正式文本产物必须有父目录");
    fs::create_dir_all(parent).expect("创建正式文本产物父目录");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间不得早于 Unix epoch")
        .as_nanos();
    let candidate = parent.join(format!(
        ".{}-{}-{nonce}.candidate",
        path.file_name()
            .expect("正式文本路径必须有文件名")
            .to_string_lossy(),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&candidate)
        .expect("创建正式文本候选");
    file.write_all(body.as_bytes()).expect("写入正式文本候选");
    file.sync_all().expect("同步正式文本候选");
    fs::rename(&candidate, path).expect("原子发布正式文本产物");
    File::open(parent)
        .expect("打开正式文本父目录")
        .sync_all()
        .expect("同步正式文本父目录");
}

fn formal_build_group_v1() -> String {
    let group = std::env::var("SIM_COMPONENT_GOLDEN_BUILD_GROUP")
        .expect("正式 COMPONENT_GOLDEN_V1 缺少 SIM_COMPONENT_GOLDEN_BUILD_GROUP");
    assert!(
        matches!(group.as_str(), "B1" | "B2"),
        "构建组只能是 B1 或 B2"
    );
    group
}

fn formal_release_binary_paths_v1(output_root: &Path, build_group: &str) -> Vec<PathBuf> {
    assert!(matches!(build_group, "B1" | "B2"));
    let directory = output_root.join(FORMAL_RELEASE_BINARIES_DIRECTORY_V1);
    if !directory.is_dir() {
        return Vec::new();
    }
    let prefix = format!("{build_group}-");
    let suffix = ".sim-system";
    let mut paths = fs::read_dir(&directory)
        .expect("读取正式 release 二进制快照目录")
        .filter_map(|entry| {
            let entry = entry.expect("读取正式 release 二进制快照目录项");
            let file_type = entry.file_type().expect("读取正式 release 二进制文件类型");
            let name = entry.file_name();
            let name = name
                .to_str()
                .expect("正式 release 二进制文件名必须为 UTF-8");
            (file_type.is_file() && name.starts_with(&prefix) && name.ends_with(suffix))
                .then(|| entry.path())
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn assert_single_formal_release_binary_v1(output_root: &Path, build_group: &str, expected: &Path) {
    let paths = formal_release_binary_paths_v1(output_root, build_group);
    assert_eq!(
        paths.len(),
        1,
        "每个正式构建组必须恰好存在一份 release 二进制"
    );
    assert_eq!(
        paths[0].canonicalize().expect("规范化实际 release 二进制"),
        expected.canonicalize().expect("规范化预期 release 二进制"),
        "构建组唯一 release 二进制必须与固定上下文一致"
    );
}

fn snapshot_release_binary_v1(output_root: &Path, build_group: &str, source: &Path) -> PathBuf {
    assert!(matches!(build_group, "B1" | "B2"));
    let source_bytes = fs::read(source).expect("读取待快照 release 测试二进制");
    let source_sha256 = sha256_bytes(&source_bytes);
    let directory = output_root.join(FORMAL_RELEASE_BINARIES_DIRECTORY_V1);
    fs::create_dir_all(&directory).expect("创建正式 release 二进制快照目录");
    let target = directory.join(format!("{build_group}-{}.sim-system", source_sha256.0));
    let existing = formal_release_binary_paths_v1(output_root, build_group);
    assert!(
        existing.is_empty() || existing == [target.clone()],
        "同一正式构建组不得生成第二份 release 二进制"
    );
    if target.exists() {
        assert_eq!(
            sha256_bytes(&fs::read(&target).expect("重读既有 release 二进制快照")),
            source_sha256,
            "既有 release 二进制快照正文冲突"
        );
        let target = target
            .canonicalize()
            .expect("规范化既有 release 二进制快照");
        assert_single_formal_release_binary_v1(output_root, build_group, &target);
        return target;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间不得早于 Unix epoch")
        .as_nanos();
    let candidate = directory.join(format!(
        ".{build_group}-{}-{nonce}.candidate",
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&candidate)
        .expect("创建 release 二进制快照候选");
    file.write_all(&source_bytes)
        .expect("写入 release 二进制快照候选");
    file.sync_all().expect("同步 release 二进制快照候选");
    fs::set_permissions(
        &candidate,
        fs::metadata(source)
            .expect("读取 release 二进制权限")
            .permissions(),
    )
    .expect("复制 release 二进制权限");
    File::open(&candidate)
        .expect("重开 release 二进制快照候选")
        .sync_all()
        .expect("同步 release 二进制快照候选权限");
    fs::rename(&candidate, &target).expect("原子发布 release 二进制快照");
    File::open(&directory)
        .expect("打开 release 二进制快照目录")
        .sync_all()
        .expect("同步 release 二进制快照目录");
    let target = target.canonicalize().expect("规范化 release 二进制快照");
    assert_single_formal_release_binary_v1(output_root, build_group, &target);
    target
}

fn validate_release_binary_binding_v1(
    actual: &Path,
    expected: &Path,
    expected_sha256: &Sha256Hex,
) -> Result<(), String> {
    let actual = actual
        .canonicalize()
        .map_err(|error| format!("规范化当前 release 测试二进制失败：{error}"))?;
    let expected = expected
        .canonicalize()
        .map_err(|error| format!("规范化固定 release 测试二进制失败：{error}"))?;
    if actual != expected {
        return Err(format!(
            "当前 release 测试二进制 {} 与固定二进制 {} 不同",
            actual.display(),
            expected.display()
        ));
    }
    let actual_sha256 = sha256_bytes(
        &fs::read(&actual).map_err(|error| format!("读取当前 release 测试二进制失败：{error}"))?,
    );
    if &actual_sha256 != expected_sha256 {
        return Err("当前 release 测试二进制 SHA-256 与固定构建身份不同".to_owned());
    }
    Ok(())
}

fn validate_current_release_binary_v1(context: &FormalMeasurementFixedContextV1) {
    let current_exe = std::env::current_exe().expect("读取当前 release 测试二进制路径");
    validate_release_binary_binding_v1(
        &current_exe,
        &context.release_unit_test_binary,
        &context.build.release_binary_sha256,
    )
    .unwrap_or_else(|error| panic!("正式子进程构建身份不匹配：{error}"));
}

fn formal_rule_semantic_sha256_v1(rules: &SealedProcessRuleSnapshot) -> Sha256Hex {
    let snapshot = rules.snapshot();
    sha256_serializable(
        "TASK44_FORMAL_RULE_SEMANTICS_V1",
        &(
            &snapshot.schema_version,
            &snapshot.synchronized_platform_rules,
            &snapshot.documented_platform_rules,
            &snapshot.project_account_rules,
        ),
    )
}

fn formal_rule_snapshot_paths_v1(output_root: &Path, build_group: &str) -> Vec<PathBuf> {
    assert!(matches!(build_group, "B1" | "B2"));
    let directory = output_root.join(FORMAL_RULE_SNAPSHOTS_DIRECTORY_V1);
    if !directory.is_dir() {
        return Vec::new();
    }
    let prefix = format!("{build_group}-");
    let suffix = ".platform-rule-snapshot.json";
    let mut paths = fs::read_dir(&directory)
        .expect("读取正式历史规则快照目录")
        .filter_map(|entry| {
            let entry = entry.expect("读取正式历史规则快照目录项");
            let file_type = entry.file_type().expect("读取正式历史规则快照文件类型");
            let name = entry.file_name();
            let name = name.to_str().expect("正式历史规则快照文件名必须为 UTF-8");
            (file_type.is_file() && name.starts_with(&prefix) && name.ends_with(suffix))
                .then(|| entry.path())
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn assert_single_formal_rule_snapshot_v1(output_root: &Path, build_group: &str, expected: &Path) {
    let paths = formal_rule_snapshot_paths_v1(output_root, build_group);
    assert_eq!(paths.len(), 1, "每个正式构建组必须恰好存在一份历史规则快照");
    assert_eq!(
        paths[0].canonicalize().expect("规范化实际历史规则快照"),
        expected.canonicalize().expect("规范化预期历史规则快照"),
        "构建组唯一历史规则快照必须与固定上下文一致"
    );
}

fn snapshot_formal_historical_rules_v1(
    output_root: &Path,
    build_group: &str,
    rules: &SealedProcessRuleSnapshot,
) -> FormalHistoricalRuleSnapshotBindingV1 {
    assert!(matches!(build_group, "B1" | "B2"));
    assert_eq!(rules.access_mode(), SnapshotAccessMode::HistoricalAsOf);
    assert!(
        rules.snapshot().provenance.rules_observed_at_utc_ms
            <= FORMAL_RULE_REPLAY_CAUSAL_START_UTC_MS_V1,
        "正式固定历史规则的观测时点不得晚于黄金输入首个因果时点"
    );
    let directory = output_root.join(FORMAL_RULE_SNAPSHOTS_DIRECTORY_V1);
    fs::create_dir_all(&directory).expect("创建正式历史规则快照目录");
    let target = directory.join(format!(
        "{build_group}-{}.platform-rule-snapshot.json",
        rules.file_sha256().0
    ));
    let existing = formal_rule_snapshot_paths_v1(output_root, build_group);
    assert!(
        existing.is_empty() || existing == [target.clone()],
        "同一正式构建组不得生成第二份历史规则快照"
    );
    if target.exists() {
        assert_eq!(
            fs::read(&target).expect("重读既有正式历史规则快照"),
            rules.file_bytes(),
            "既有正式历史规则快照正文冲突"
        );
    } else {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("系统时间不得早于 Unix epoch")
            .as_nanos();
        let candidate = directory.join(format!(
            ".{build_group}-{}-{nonce}.candidate",
            std::process::id()
        ));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
            .expect("创建正式历史规则快照候选");
        file.write_all(rules.file_bytes())
            .expect("写入正式历史规则快照候选");
        file.sync_all().expect("同步正式历史规则快照候选");
        fs::rename(&candidate, &target).expect("原子发布正式历史规则快照");
        File::open(&directory)
            .expect("打开正式历史规则快照目录")
            .sync_all()
            .expect("同步正式历史规则快照目录");
    }
    assert_single_formal_rule_snapshot_v1(output_root, build_group, &target);
    FormalHistoricalRuleSnapshotBindingV1 {
        snapshot_path: target.canonicalize().expect("规范化正式历史规则快照路径"),
        file_sha256: rules.file_sha256().clone(),
        content_sha256: rules.snapshot().content_sha256.clone(),
        semantic_rules_sha256: formal_rule_semantic_sha256_v1(rules),
        replay_causal_start_utc_ms: FORMAL_RULE_REPLAY_CAUSAL_START_UTC_MS_V1,
    }
}

fn load_formal_historical_rules_v1(
    context: &FormalMeasurementFixedContextV1,
    expected_implementation_identity: &ImplementationIdentityV1,
) -> Arc<SealedProcessRuleSnapshot> {
    let binding = &context.historical_rules;
    assert_eq!(
        binding.replay_causal_start_utc_ms, FORMAL_RULE_REPLAY_CAUSAL_START_UTC_MS_V1,
        "正式历史规则回放起点必须固定为黄金输入首个因果时点"
    );
    let rules = load_historical_process_snapshot(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: binding.snapshot_path.clone(),
        expected_file_sha256: binding.file_sha256.clone(),
        replay_causal_start_utc_ms: binding.replay_causal_start_utc_ms,
    })
    .expect("离线加载正式历史规则快照");
    assert_eq!(rules.access_mode(), SnapshotAccessMode::HistoricalAsOf);
    assert_eq!(rules.file_sha256(), &binding.file_sha256);
    assert_eq!(rules.snapshot().content_sha256, binding.content_sha256);
    assert_eq!(
        formal_rule_semantic_sha256_v1(&rules),
        binding.semantic_rules_sha256
    );
    assert_eq!(
        &rules.snapshot().provenance.implementation_identity,
        expected_implementation_identity,
        "正式历史规则快照必须绑定当前构建组实现身份"
    );
    rules
}

#[test]
fn formal_release_binary_snapshots_survive_later_builds() {
    let directory = tempfile::tempdir().expect("正式二进制快照测试目录");
    let source = directory.path().join("sim-system-test-binary");
    fs::write(&source, b"B1-binary").expect("写入 B1 测试二进制");
    let b1 = snapshot_release_binary_v1(directory.path(), "B1", &source);
    fs::write(&source, b"B2-binary").expect("覆盖 Cargo 复用的测试二进制路径");
    let b2 = snapshot_release_binary_v1(directory.path(), "B2", &source);
    assert_ne!(b1, b2);
    assert_eq!(fs::read(&b1).expect("重读 B1 快照"), b"B1-binary");
    assert_eq!(fs::read(&b2).expect("重读 B2 快照"), b"B2-binary");
    let b2_sha256 = sha256_bytes(&fs::read(&b2).expect("读取 B2 快照摘要正文"));
    assert!(validate_release_binary_binding_v1(&b2, &b2, &b2_sha256).is_ok());
    assert!(
        validate_release_binary_binding_v1(&source, &b2, &b2_sha256).is_err(),
        "即使正文相同，未直接执行固定快照路径也必须拒绝"
    );
    assert!(
        validate_release_binary_binding_v1(&b2, &b2, &Sha256Hex("0".repeat(64))).is_err(),
        "固定构建摘要被篡改时必须拒绝"
    );
    let second_b1 = b1
        .parent()
        .expect("release 二进制快照必须有父目录")
        .join(format!("B1-{}.sim-system", "f".repeat(64)));
    fs::write(&second_b1, b"second-B1-binary").expect("写入第二份 B1 二进制反例");
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            snapshot_release_binary_v1(directory.path(), "B1", &source)
        }))
        .is_err(),
        "同一构建组存在第二份 release 二进制时必须失败关闭"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn formal_historical_rules_are_reused_offline_and_reject_tampering() {
    let implementation_identity = ImplementationIdentityV1 {
        git_commit: "a".repeat(40),
        source_identity_sha256: Sha256Hex("b".repeat(64)),
        cargo_lock_sha256: Sha256Hex("c".repeat(64)),
    };
    let generated = historical_snapshot_for_task44_with_fee_schedule_and_identity(
        "0.07",
        1,
        implementation_identity.clone(),
    )
    .await;
    let directory = tempfile::tempdir().expect("正式历史规则快照测试目录");
    let binding = snapshot_formal_historical_rules_v1(directory.path(), "B1", &generated);
    let load_offline = || {
        load_historical_process_snapshot(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: binding.snapshot_path.clone(),
            expected_file_sha256: binding.file_sha256.clone(),
            replay_causal_start_utc_ms: binding.replay_causal_start_utc_ms,
        })
    };

    let first = load_offline().expect("首次离线加载固定历史规则快照");
    let second = load_offline().expect("再次离线复用同一历史规则快照");
    assert_eq!(first.file_bytes(), second.file_bytes());
    assert_eq!(first.file_sha256(), &binding.file_sha256);
    assert_eq!(first.snapshot().content_sha256, binding.content_sha256);
    assert_eq!(
        formal_rule_semantic_sha256_v1(&first),
        binding.semantic_rules_sha256
    );
    assert_eq!(
        first.snapshot().provenance.implementation_identity,
        implementation_identity
    );

    let second_group_snapshot = binding
        .snapshot_path
        .parent()
        .expect("正式规则快照必须有父目录")
        .join(format!("B1-{}.platform-rule-snapshot.json", "f".repeat(64)));
    fs::write(&second_group_snapshot, first.file_bytes()).expect("写入第二份同组规则快照反例");
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            snapshot_formal_historical_rules_v1(directory.path(), "B1", &generated)
        }))
        .is_err(),
        "同一构建组存在第二份正式规则快照时必须失败关闭"
    );
    fs::remove_file(second_group_snapshot).expect("移除同组规则快照反例");
    assert_single_formal_rule_snapshot_v1(directory.path(), "B1", &binding.snapshot_path);

    let mut tampered = fs::read(&binding.snapshot_path).expect("读取待篡改规则快照");
    tampered[0] ^= 1;
    fs::write(&binding.snapshot_path, tampered).expect("写入篡改规则快照");
    assert!(
        load_offline().is_err(),
        "固定历史规则快照任一字节被篡改后必须拒绝加载"
    );
}

fn build_formal_fixed_context_v1(
    output_root: &Path,
    implementation_identity: &ImplementationIdentityV1,
    input_stream_sha256: Sha256Hex,
    release_unit_test_binary: PathBuf,
    historical_rules: FormalHistoricalRuleSnapshotBindingV1,
) -> FormalMeasurementFixedContextV1 {
    let current_exe = std::env::current_exe().expect("读取正式 release 测试二进制路径");
    assert!(
        current_exe
            .components()
            .any(|component| component.as_os_str() == "release"),
        "正式 COMPONENT_GOLDEN_V1 必须使用 release 构建"
    );
    let meminfo = read_required_text("/proc/meminfo", "/proc/meminfo");
    let mem_kib = |key| {
        parse_proc_key_value(&meminfo, key)
            .checked_mul(1024)
            .expect("/proc/meminfo KiB 转字节溢出")
    };
    let rustc = command_stdout(Command::new("rustc").arg("-vV"), "读取 rustc 构建身份");
    let target_triple = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .expect("rustc -vV 缺少 host")
        .to_owned();
    let stat = rustix::fs::statvfs(output_root).expect("读取正式测量文件系统容量");
    let canonical_root = output_root
        .canonicalize()
        .expect("规范化正式测量文件系统根");
    let mountinfo = read_required_text("/proc/self/mountinfo", "/proc/self/mountinfo");
    let canonical_text = canonical_root.to_string_lossy();
    let mount_line = mountinfo
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let mount_point = fields.get(4)?;
            (*mount_point == "/"
                || canonical_text == *mount_point
                || canonical_text
                    .strip_prefix(*mount_point)
                    .is_some_and(|suffix| suffix.starts_with('/')))
            .then_some((mount_point.len(), line))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, line)| line)
        .expect("正式测量根必须命中 /proc/self/mountinfo");
    let filesystem_type = mount_line
        .split(" - ")
        .nth(1)
        .and_then(|right| right.split_whitespace().next())
        .expect("mountinfo 缺少文件系统类型")
        .to_owned();
    let block_size_bytes = stat.f_frsize.max(stat.f_bsize);
    let runner_path = repository_root().join(file!());
    let release_unit_test_binary = release_unit_test_binary
        .canonicalize()
        .expect("规范化正式 release 测试二进制快照路径");
    assert_eq!(
        sha256_bytes(&fs::read(&current_exe).expect("读取当前 release 测试二进制")),
        sha256_bytes(
            &fs::read(&release_unit_test_binary).expect("读取正式 release 测试二进制快照")
        ),
        "固定上下文必须快照当前执行的同一 release 二进制"
    );
    let release_binary_sha256 = sha256_bytes(
        &fs::read(&release_unit_test_binary).expect("读取正式 release 测试二进制快照"),
    );
    let cache_precondition = MeasurementCachePreconditionV1::formal_v1();
    let cache_precondition_sha256 = cache_precondition
        .content_sha256()
        .expect("推导固定缓存前提身份");
    FormalMeasurementFixedContextV1 {
        schema_version: "component_golden_fixed_context_v1".to_owned(),
        release_unit_test_binary,
        host: MeasurementHostIdentityV1 {
            machine_identity_sha256: sha256_bytes(
                &fs::read("/etc/machine-id").expect("读取主机身份"),
            ),
            boot_identity_sha256: sha256_bytes(
                &fs::read("/proc/sys/kernel/random/boot_id").expect("读取启动身份"),
            ),
            os_release_sha256: sha256_bytes(
                &fs::read("/etc/os-release").expect("读取系统发布身份"),
            ),
            kernel_release: command_stdout(Command::new("uname").arg("-r"), "读取内核版本"),
            architecture: command_stdout(Command::new("uname").arg("-m"), "读取机器架构"),
            online_cpu_count: u32::try_from(
                std::thread::available_parallelism()
                    .expect("读取在线 CPU 数")
                    .get(),
            )
            .expect("在线 CPU 数适合 u32"),
            page_size_bytes: command_stdout(Command::new("getconf").arg("PAGESIZE"), "读取页大小")
                .parse()
                .expect("解析页大小"),
            physical_memory_bytes: mem_kib("MemTotal"),
            swap_total_bytes: mem_kib("SwapTotal"),
        },
        build: MeasurementBuildIdentityV1 {
            release_binary_sha256,
            source_tree_sha256: implementation_identity.source_identity_sha256.clone(),
            cargo_lock_sha256: implementation_identity.cargo_lock_sha256.clone(),
            source_commit: implementation_identity.git_commit.clone(),
            rustc_version: rustc,
            target_triple,
            cargo_profile: "release".to_owned(),
        },
        filesystem: FormalFilesystemFixedContextV1 {
            measurement_root_sha256: sha256_bytes(canonical_text.as_bytes()),
            mount_identity_sha256: sha256_bytes(mount_line.as_bytes()),
            filesystem_type,
            block_size_bytes,
            total_bytes: stat
                .f_blocks
                .checked_mul(block_size_bytes)
                .expect("文件系统总字节溢出"),
        },
        input_stream_sha256,
        runner_sha256: sha256_bytes(&fs::read(runner_path).expect("读取正式 runner 源码")),
        cache_precondition,
        cache_precondition_sha256,
        historical_rules,
    }
}

fn formal_fixed_context_path_v1(output_root: &Path) -> PathBuf {
    let group = formal_build_group_v1();
    output_root.join(format!("{group}-{FORMAL_FIXED_CONTEXT_FILE_V1}"))
}

fn formal_fixed_context_v1(output_root: &Path) -> FormalMeasurementFixedContextV1 {
    read_json_with_sha256(&formal_fixed_context_path_v1(output_root))
}

fn implementation_identity_from_fixed_context_v1(
    context: &FormalMeasurementFixedContextV1,
) -> ImplementationIdentityV1 {
    ImplementationIdentityV1 {
        git_commit: context.build.source_commit.clone(),
        source_identity_sha256: context.build.source_tree_sha256.clone(),
        cargo_lock_sha256: context.build.cargo_lock_sha256.clone(),
    }
}

fn validate_fixed_context_artifacts_v1(
    output_root: &Path,
    build_group: &str,
    context: &FormalMeasurementFixedContextV1,
    input_stream_sha256: &Sha256Hex,
    verify_current_source_tree: bool,
) {
    assert!(matches!(build_group, "B1" | "B2"));
    assert_eq!(context.schema_version, "component_golden_fixed_context_v1");
    assert_eq!(&context.input_stream_sha256, input_stream_sha256);
    assert_single_formal_release_binary_v1(
        output_root,
        build_group,
        &context.release_unit_test_binary,
    );
    assert_eq!(
        sha256_bytes(
            &fs::read(&context.release_unit_test_binary)
                .expect("FINALIZE 重新读取固定 release 测试二进制"),
        ),
        context.build.release_binary_sha256,
        "FINALIZE 必须重新核验固定 release 测试二进制 SHA-256"
    );
    assert_single_formal_rule_snapshot_v1(
        output_root,
        build_group,
        &context.historical_rules.snapshot_path,
    );
    let implementation_identity = implementation_identity_from_fixed_context_v1(context);
    let _ = load_formal_historical_rules_v1(context, &implementation_identity);
    assert_eq!(
        sha256_bytes(
            &fs::read(repository_root().join(file!())).expect("FINALIZE 重新读取正式 runner 源码"),
        ),
        context.runner_sha256,
        "B1/B2 必须绑定同一正式 runner 源码"
    );
    assert_eq!(
        context.cache_precondition,
        MeasurementCachePreconditionV1::formal_v1()
    );
    assert_eq!(
        context.cache_precondition_sha256,
        context
            .cache_precondition
            .content_sha256()
            .expect("FINALIZE 重算缓存前提身份")
    );
    if verify_current_source_tree {
        assert_eq!(
            actual_source_tree_sha256(),
            context.build.source_tree_sha256,
            "FINALIZE 必须重新核验 B2 当前源树身份"
        );
        assert_eq!(
            sha256_bytes(&fs::read(repository_root().join("Cargo.lock")).expect("读取 Cargo.lock")),
            context.build.cargo_lock_sha256,
            "FINALIZE 必须重新核验 B2 Cargo.lock 身份"
        );
    }
}

fn formal_measurement_environment_from_fixed_context_v1(
    output_root: &Path,
    input_stream_sha256: Sha256Hex,
) -> MeasurementEnvironmentIdentityV1 {
    let context = formal_fixed_context_v1(output_root);
    assert_eq!(
        context.schema_version, "component_golden_fixed_context_v1",
        "正式固定上下文版本非法"
    );
    assert_eq!(context.input_stream_sha256, input_stream_sha256);
    validate_current_release_binary_v1(&context);
    let canonical_root = output_root.canonicalize().expect("规范化正式测量输出根");
    assert_eq!(
        sha256_bytes(canonical_root.to_string_lossy().as_bytes()),
        context.filesystem.measurement_root_sha256,
        "正式子进程输出根必须与 bootstrap 固定上下文一致"
    );
    let stat = rustix::fs::statvfs(output_root).expect("读取当前试次文件系统可用容量");
    let block_size_bytes = stat.f_frsize.max(stat.f_bsize);
    assert_eq!(block_size_bytes, context.filesystem.block_size_bytes);
    assert_eq!(
        stat.f_blocks
            .checked_mul(block_size_bytes)
            .expect("当前试次文件系统总字节溢出"),
        context.filesystem.total_bytes
    );
    MeasurementEnvironmentIdentityV1 {
        host: context.host,
        build: context.build,
        filesystem: MeasurementFilesystemIdentityV1 {
            measurement_root_sha256: context.filesystem.measurement_root_sha256,
            mount_identity_sha256: context.filesystem.mount_identity_sha256,
            filesystem_type: context.filesystem.filesystem_type,
            block_size_bytes,
            total_bytes: context.filesystem.total_bytes,
            available_bytes_before: stat
                .f_bavail
                .checked_mul(block_size_bytes)
                .expect("当前试次文件系统可用字节溢出"),
        },
        cgroup_path_sha256: sha256_bytes(cgroup_v2_path().to_string_lossy().as_bytes()),
        input_stream_sha256,
        runner_sha256: context.runner_sha256,
        cache_precondition: context.cache_precondition,
        cache_precondition_sha256: context.cache_precondition_sha256,
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "正式 COMPONENT_GOLDEN_V1 固定上下文必须在八个计量 scope 外显式准备"]
async fn task441_component_golden_prepare_fixed_context() {
    let output_root = formal_output_root_from_environment();
    let implementation_identity = formal_implementation_identity_from_environment();
    let input = GoldenInputV1::new();
    let build_group = formal_build_group_v1();
    let current_exe = std::env::current_exe()
        .expect("读取待快照 release 测试二进制")
        .canonicalize()
        .expect("规范化待快照 release 测试二进制");
    let release_binary = snapshot_release_binary_v1(&output_root, &build_group, &current_exe);
    let generated_rules = historical_snapshot_for_task44_with_fee_schedule_and_identity(
        "0.07",
        1,
        implementation_identity.clone(),
    )
    .await;
    let historical_rules =
        snapshot_formal_historical_rules_v1(&output_root, &build_group, &generated_rules);
    let context = build_formal_fixed_context_v1(
        &output_root,
        &implementation_identity,
        input.stream_sha256,
        release_binary,
        historical_rules,
    );
    let path = formal_fixed_context_path_v1(&output_root);
    let sha256 = atomic_write_json_with_sha256(&path, &context);
    eprintln!(
        "COMPONENT_GOLDEN_V1 fixed_context={} sha256={}",
        path.display(),
        sha256.0
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "正式失败保留探针必须在八个性能试次之外显式执行"]
async fn task441_component_golden_measure_failure_reserve() {
    let output_root = formal_output_root_from_environment();
    let context_path = output_root.join(format!("B1-{FORMAL_FIXED_CONTEXT_FILE_V1}"));
    let context: FormalMeasurementFixedContextV1 = read_json_with_sha256(&context_path);
    validate_current_release_binary_v1(&context);
    assert_single_formal_rule_snapshot_v1(
        &output_root,
        "B1",
        &context.historical_rules.snapshot_path,
    );
    let implementation_identity = implementation_identity_from_fixed_context_v1(&context);
    let rules = load_formal_historical_rules_v1(&context, &implementation_identity);
    let probe_directory = output_root.join("failure-reserve-probe");
    assert!(
        !probe_directory.exists(),
        "失败保留探针必须使用全新持久目录"
    );
    fs::create_dir_all(&probe_directory).expect("创建失败保留探针目录");
    let run_id = formal_run_id_v1(
        &context.filesystem.measurement_root_sha256,
        "FAILURE_RESERVE",
    );
    let b1_policy = SimulationResourcePolicyV1::component_golden_v1();
    let probe_policy = formal_failure_probe_policy_v1();
    let observation = measure_real_failure_reserve_observation_v1(
        &run_id,
        probe_directory.join("runtime"),
        rules,
        probe_policy.clone(),
    )
    .await;
    observation.validate().expect("正式失败续租强观测必须有效");
    assert_eq!(
        observation
            .one_failure_reserve
            .run_checkpoint_growth
            .records,
        1
    );
    for quantity in [
        observation.one_failure_reserve.evidence,
        observation.one_failure_reserve.checkpoints,
        observation.one_failure_reserve.runtime_root,
    ] {
        assert!(quantity.records > 0 && quantity.bytes > 0);
    }
    let probe = IndependentFailureReserveProbeV1 {
        schema_version: INDEPENDENT_FAILURE_RESERVE_PROBE_SCHEMA_V1.to_owned(),
        suite_measurement_root_sha256: context.filesystem.measurement_root_sha256.clone(),
        fixed_context_file_sha256: sha256_bytes(
            &fs::read(&context_path).expect("重读 B1 固定上下文正文"),
        ),
        build: context.build.clone(),
        input_stream_sha256: context.input_stream_sha256.clone(),
        market_event_source_identity_sha256: sha256_bytes(
            b"TASK44_COMPONENT_GOLDEN_FAILURE_RESERVE_EVENT_SOURCE_V1",
        ),
        runner_sha256: context.runner_sha256.clone(),
        rule_snapshot_file_sha256: context.historical_rules.file_sha256.clone(),
        rule_snapshot_content_sha256: context.historical_rules.content_sha256.clone(),
        semantic_rules_sha256: context.historical_rules.semantic_rules_sha256.clone(),
        resource_policy_id: COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1.to_owned(),
        b1_resource_policy: b1_policy.clone(),
        b1_resource_policy_sha256: b1_policy
            .content_sha256()
            .expect("重算 B1 预冻结资源策略身份"),
        probe_resource_policy: probe_policy.clone(),
        probe_resource_policy_sha256: probe_policy
            .content_sha256()
            .expect("重算正式失败探针资源策略身份"),
        run_id,
        workload_kind: "COMPONENT_GOLDEN".to_owned(),
        deterministic_script_contract: "COMPONENT_GOLDEN_V1".to_owned(),
        observation,
    };
    probe.validate().expect("正式失败探针信封必须有效");
    let output_path = output_root.join(FORMAL_FAILURE_RESERVE_FILE_V1);
    let sha256 = atomic_write_json_with_sha256(&output_path, &probe);
    eprintln!(
        "COMPONENT_GOLDEN_V1 failure_reserve_observation={} sha256={}",
        output_path.display(),
        sha256.0
    );
}

fn validate_formal_failure_reserve_probe_v1(
    output_root: &Path,
    b1_context: &FormalMeasurementFixedContextV1,
    expected_b1_resource_policy_sha256: &Sha256Hex,
    expected_run_id: &str,
    probe: &IndependentFailureReserveProbeV1,
) {
    probe.validate().expect("独立失败探针信封必须有效");
    let context_path = output_root.join(format!("B1-{FORMAL_FIXED_CONTEXT_FILE_V1}"));
    assert_eq!(
        probe.suite_measurement_root_sha256, b1_context.filesystem.measurement_root_sha256,
        "失败探针必须绑定当前套件输出根身份"
    );
    assert_eq!(
        probe.fixed_context_file_sha256,
        sha256_bytes(&fs::read(context_path).expect("重读 B1 固定上下文")),
        "失败探针必须绑定当前套件 B1 固定上下文文件"
    );
    assert_eq!(probe.build, b1_context.build);
    assert_eq!(
        probe.run_id, expected_run_id,
        "失败探针必须使用当前套件固定宽度的正式运行身份"
    );
    assert_eq!(probe.input_stream_sha256, b1_context.input_stream_sha256);
    assert_eq!(
        probe.market_event_source_identity_sha256,
        sha256_bytes(b"TASK44_COMPONENT_GOLDEN_FAILURE_RESERVE_EVENT_SOURCE_V1"),
        "失败探针必须绑定固定组件黄金事件源身份"
    );
    assert_eq!(probe.runner_sha256, b1_context.runner_sha256);
    assert_eq!(
        probe.rule_snapshot_file_sha256,
        b1_context.historical_rules.file_sha256
    );
    assert_eq!(
        probe.rule_snapshot_content_sha256,
        b1_context.historical_rules.content_sha256
    );
    assert_eq!(
        probe.semantic_rules_sha256,
        b1_context.historical_rules.semantic_rules_sha256
    );
    assert_eq!(
        &probe.b1_resource_policy_sha256, expected_b1_resource_policy_sha256,
        "失败探针必须绑定六个 B1 试次的同一预冻结资源策略"
    );
    assert_eq!(
        probe.probe_resource_policy,
        {
            let mut expected = probe.b1_resource_policy.clone();
            expected.run.input_queue = ResourceLimitV1::new(1, expected.run.input_queue.max_bytes);
            expected
        },
        "失败探针必须是 B1 预冻结策略的唯一固定 InputQueue 变体"
    );

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FormalFailureProbeRunCreatedV1 {
        run_id: String,
        market_event_source_identity_sha256: Sha256Hex,
        run_intent_binding: SimulationRunIntentBindingV1,
        resource_policy: SimulationResourcePolicyV1,
        resource_policy_sha256: Sha256Hex,
    }

    let facts = durable_fact_entries(&output_root.join("failure-reserve-probe/runtime"));
    let run_created = facts
        .iter()
        .filter_map(|(_, fact)| fact.get("RUN_CREATED"))
        .collect::<Vec<_>>();
    assert_eq!(run_created.len(), 1, "失败探针必须只有一个运行创建事实");
    let run_created: FormalFailureProbeRunCreatedV1 =
        serde_json::from_value(run_created[0].clone()).expect("强类型解码失败探针运行创建事实");
    assert_eq!(
        run_created.run_id, probe.run_id,
        "失败探针运行事实必须绑定信封运行身份"
    );
    assert_eq!(
        run_created.market_event_source_identity_sha256, probe.market_event_source_identity_sha256,
        "失败探针运行事实必须绑定同一输入流身份"
    );
    assert_eq!(run_created.resource_policy, probe.probe_resource_policy);
    assert_eq!(
        run_created.resource_policy_sha256, probe.probe_resource_policy_sha256,
        "失败探针信封必须绑定真实持久运行创建事实中的探针策略身份"
    );
    assert_eq!(
        run_created.run_intent_binding.workload_kind,
        SimulationWorkloadKindV1::ComponentGolden
    );
    assert_eq!(
        run_created.run_intent_binding.deterministic_script_contract,
        SimulationDeterministicScriptContractV1::ComponentGoldenV1
    );
}

fn runtime_hard_guards_v1(snapshot: &MeasuredResourceSnapshotV1) -> MeasurementRuntimeHardGuardsV1 {
    let CgroupMemoryLimitV1::Bytes {
        bytes: cgroup_memory_max_bytes,
    } = snapshot.cgroup_v2.memory_limit
    else {
        panic!("正式测量要求有限 cgroup memory.max")
    };
    let CgroupMemoryLimitV1::Bytes {
        bytes: cgroup_memory_swap_max_bytes,
    } = snapshot.cgroup_v2.memory_swap_limit
    else {
        panic!("正式测量要求有限 cgroup memory.swap.max")
    };
    let CgroupPidsLimitV1::Count {
        count: cgroup_pids_max_count,
    } = snapshot.cgroup_v2.pids_limit
    else {
        panic!("正式测量要求有限 cgroup pids.max")
    };
    MeasurementRuntimeHardGuardsV1 {
        cgroup_memory_max_bytes,
        cgroup_memory_swap_max_bytes,
        cgroup_pids_max_count,
        file_descriptor_soft_limit: snapshot.procfs.file_descriptor_soft_limit,
        file_descriptor_hard_limit: snapshot.procfs.file_descriptor_hard_limit,
    }
}

fn unique_smoke_run_root(output_root: &Path) -> (String, PathBuf) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间不得早于 Unix epoch")
        .as_nanos();
    let suffix = format!("{}-{nonce}", std::process::id());
    (
        format!("task44-component-golden-smoke-{suffix}"),
        output_root.join("smoke-runs").join(suffix),
    )
}

fn read_required_text(path: impl AsRef<Path>, label: &str) -> String {
    fs::read_to_string(path.as_ref())
        .unwrap_or_else(|error| panic!("读取{label} {} 失败：{error}", path.as_ref().display()))
        .trim()
        .to_owned()
}

fn parse_proc_key_value(text: &str, key: &str) -> u64 {
    text.lines()
        .find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            (candidate.trim() == key)
                .then(|| value.split_whitespace().next()?.parse::<u64>().ok())
                .flatten()
        })
        .unwrap_or_else(|| panic!("/proc 正文缺少 {key}"))
}

fn parse_space_key_value(text: &str, key: &str) -> u64 {
    text.lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == key)
                .then(|| fields.next()?.parse::<u64>().ok())
                .flatten()
        })
        .unwrap_or_else(|| panic!("空格分隔正文缺少 {key}"))
}

fn procfs_snapshot_v1() -> ProcfsProcessResourceSnapshotV1 {
    procfs_snapshot_for_pid_v1(std::process::id())
}

fn procfs_snapshot_for_pid_v1(pid: u32) -> ProcfsProcessResourceSnapshotV1 {
    let root = PathBuf::from(format!("/proc/{pid}"));
    let status = read_required_text(root.join("status"), "/proc/<pid>/status");
    let io = read_required_text(root.join("io"), "/proc/<pid>/io");
    let stat = read_required_text(root.join("stat"), "/proc/<pid>/stat");
    let stat_fields = stat
        .rsplit_once(')')
        .expect("/proc/self/stat 必须含进程名闭括号")
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let limits = read_required_text(root.join("limits"), "/proc/<pid>/limits");
    let open_files = limits
        .lines()
        .find(|line| line.starts_with("Max open files"))
        .expect("/proc/self/limits 缺少 Max open files")
        .split_whitespace()
        .collect::<Vec<_>>();
    let kib = |key| {
        parse_proc_key_value(&status, key)
            .checked_mul(1024)
            .expect("/proc KiB 转字节溢出")
    };
    let io_value = |key| parse_proc_key_value(&io, key);
    let process_start_time_ticks = stat_fields[19].parse().expect("解析进程启动时间 ticks");
    assert!(
        process_start_time_ticks > 0,
        "进程启动时间 ticks 必须大于零"
    );
    ProcfsProcessResourceSnapshotV1 {
        pid,
        process_start_time_ticks,
        vm_rss_bytes: kib("VmRSS"),
        vm_hwm_bytes: kib("VmHWM"),
        vm_swap_bytes: kib("VmSwap"),
        thread_count: u32::try_from(parse_proc_key_value(&status, "Threads"))
            .expect("线程数适合 u32"),
        open_file_descriptor_count: u64::try_from(
            fs::read_dir(root.join("fd"))
                .expect("读取 /proc/<pid>/fd")
                .count(),
        )
        .expect("文件描述符数适合 u64"),
        file_descriptor_soft_limit: open_files[3].parse().expect("解析文件描述符软上限"),
        file_descriptor_hard_limit: open_files[4].parse().expect("解析文件描述符硬上限"),
        user_cpu_ticks: stat_fields[11].parse().expect("解析进程用户 CPU ticks"),
        system_cpu_ticks: stat_fields[12].parse().expect("解析进程系统 CPU ticks"),
        io: ProcfsIoSnapshotV1 {
            rchar_bytes: io_value("rchar"),
            wchar_bytes: io_value("wchar"),
            read_syscalls: io_value("syscr"),
            write_syscalls: io_value("syscw"),
            storage_read_bytes: io_value("read_bytes"),
            storage_write_bytes: io_value("write_bytes"),
            cancelled_write_bytes: io_value("cancelled_write_bytes"),
        },
    }
}

fn cgroup_v2_path() -> PathBuf {
    let cgroup = read_required_text("/proc/self/cgroup", "/proc/self/cgroup");
    let relative = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .expect("正式测量必须运行于 cgroup v2")
        .trim_start_matches('/');
    Path::new("/sys/fs/cgroup").join(relative)
}

fn cgroup_u64(root: &Path, name: &str) -> u64 {
    read_required_text(root.join(name), name)
        .parse()
        .unwrap_or_else(|error| panic!("解析 cgroup {name} 失败：{error}"))
}

fn cgroup_memory_limit(root: &Path, name: &str) -> CgroupMemoryLimitV1 {
    let value = read_required_text(root.join(name), name);
    if value == "max" {
        CgroupMemoryLimitV1::Unbounded
    } else {
        CgroupMemoryLimitV1::Bytes {
            bytes: value
                .parse()
                .unwrap_or_else(|error| panic!("解析 cgroup {name} 失败：{error}")),
        }
    }
}

fn cgroup_pids_limit(root: &Path) -> CgroupPidsLimitV1 {
    let value = read_required_text(root.join("pids.max"), "pids.max");
    if value == "max" {
        CgroupPidsLimitV1::Unbounded
    } else {
        CgroupPidsLimitV1::Count {
            count: value.parse().expect("解析 cgroup pids.max"),
        }
    }
}

fn cgroup_v2_snapshot_v1() -> CgroupV2ResourceSnapshotV1 {
    let root = cgroup_v2_path();
    cgroup_v2_snapshot_at_v1(&root)
}

fn cgroup_v2_snapshot_at_v1(root: &Path) -> CgroupV2ResourceSnapshotV1 {
    let events = read_required_text(root.join("memory.events"), "memory.events");
    let cpu = read_required_text(root.join("cpu.stat"), "cpu.stat");
    let event = |key| parse_space_key_value(&events, key);
    let peak = |name: &str| {
        let path = root.join(name);
        path.is_file().then(|| cgroup_u64(root, name))
    };
    CgroupV2ResourceSnapshotV1 {
        memory_current_bytes: cgroup_u64(root, "memory.current"),
        memory_peak_bytes: peak("memory.peak"),
        memory_limit: cgroup_memory_limit(root, "memory.max"),
        memory_swap_current_bytes: cgroup_u64(root, "memory.swap.current"),
        memory_swap_peak_bytes: peak("memory.swap.peak"),
        memory_swap_limit: cgroup_memory_limit(root, "memory.swap.max"),
        memory_events: CgroupMemoryEventsV1 {
            low: event("low"),
            high: event("high"),
            max: event("max"),
            oom: event("oom"),
            oom_kill: event("oom_kill"),
            oom_group_kill: event("oom_group_kill"),
        },
        cpu_usage_usec: parse_space_key_value(&cpu, "usage_usec"),
        pids_current: cgroup_u64(root, "pids.current"),
        pids_limit: cgroup_pids_limit(root),
    }
}

fn orchestrator_is_outside_trial_cgroup_v1(orchestrator: &Path, trial: &Path) -> bool {
    !orchestrator.starts_with(trial)
}

#[test]
fn task441_orchestrator_cgroup_must_not_equal_or_descend_from_trial_cgroup() {
    let trial = Path::new("/sys/fs/cgroup/user.slice/trial.service");
    assert!(!orchestrator_is_outside_trial_cgroup_v1(trial, trial));
    assert!(!orchestrator_is_outside_trial_cgroup_v1(
        Path::new("/sys/fs/cgroup/user.slice/trial.service/supervisor"),
        trial,
    ));
    assert!(orchestrator_is_outside_trial_cgroup_v1(
        Path::new("/sys/fs/cgroup/user.slice/app.slice/supervisor.scope"),
        trial,
    ));
}

struct GoldenResourceSamplerV1 {
    started: Instant,
    session_ids: BTreeMap<ComponentGoldenTrajectoryV1, String>,
    snapshots: Vec<MeasuredResourceSnapshotV1>,
}

impl GoldenResourceSamplerV1 {
    fn new(session_ids: BTreeMap<ComponentGoldenTrajectoryV1, String>) -> Self {
        Self {
            started: Instant::now(),
            session_ids,
            snapshots: Vec::new(),
        }
    }

    fn capture(&mut self, diagnostic: crate::execution::SimulationResourceDiagnosticV1) -> u64 {
        let sample_index = u64::try_from(self.snapshots.len()).expect("资源样本数适合 u64");
        let trajectory_sessions = self
            .session_ids
            .iter()
            .filter_map(|(trajectory, session_id)| {
                diagnostic
                    .sessions
                    .get(session_id)
                    .cloned()
                    .map(|usage| (*trajectory, usage))
            })
            .collect();
        self.snapshots.push(MeasuredResourceSnapshotV1 {
            sample_index,
            elapsed_ns: elapsed_ns(self.started),
            procfs: procfs_snapshot_v1(),
            cgroup_v2: cgroup_v2_snapshot_v1(),
            internal: InternalResourceSnapshotV1 {
                trajectory_sessions,
                shared_run: diagnostic.run,
                durable: diagnostic.durable,
                high_water: diagnostic.high_water,
            },
        });
        sample_index
    }
}

fn capture_golden_resource_snapshot(
    process: &Arc<Mutex<SimExecutionProcessV1>>,
    sampler: &Arc<Mutex<GoldenResourceSamplerV1>>,
) -> u64 {
    let diagnostic = process
        .lock()
        .expect("黄金运行根锁")
        .resource_snapshot_for_tests()
        .expect("读取黄金内部资源快照");
    sampler.lock().expect("黄金资源采样锁").capture(diagnostic)
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

const OMITTED_RUN_SCOPED_IDENTITY_V1: &str = "RUN_SCOPED_ACCOUNT_EVIDENCE_IDENTITY";

fn omitted_run_scoped_sha256_v1() -> Sha256Hex {
    Sha256Hex("0".repeat(64))
}

fn normalize_logical_account_key_v1(key: &mut AccountInstanceKeyV1) {
    key.run_id = OMITTED_RUN_SCOPED_IDENTITY_V1.to_owned();
}

fn normalize_logical_account_delta_reference_v1(reference: &mut AccountDeltaReferenceV1) {
    reference.delta_id = OMITTED_RUN_SCOPED_IDENTITY_V1.to_owned();
    reference.delta_sha256 = omitted_run_scoped_sha256_v1();
}

fn normalize_logical_account_delta_v1(delta: &mut PlatformAccountDeltaV1) {
    delta.delta_id = OMITTED_RUN_SCOPED_IDENTITY_V1.to_owned();
    delta.delta_sha256 = omitted_run_scoped_sha256_v1();
    normalize_logical_account_key_v1(&mut delta.account_instance_key);
    match &mut delta.predecessor {
        PlatformAccountDeltaPredecessorV1::OpeningAnchor {
            anchor_id,
            anchor_sha256,
        } => {
            *anchor_id = OMITTED_RUN_SCOPED_IDENTITY_V1.to_owned();
            *anchor_sha256 = omitted_run_scoped_sha256_v1();
        }
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            ..
        } => {
            *delta_id = OMITTED_RUN_SCOPED_IDENTITY_V1.to_owned();
            *delta_sha256 = omitted_run_scoped_sha256_v1();
        }
    }
    delta.account_summary_before_sha256 = omitted_run_scoped_sha256_v1();
    delta.account_summary_after_sha256 = omitted_run_scoped_sha256_v1();
}

#[test]
fn logical_account_projection_omits_only_run_scoped_evidence_identity() {
    let key = AccountInstanceKeyV1 {
        run_id: "run-a".to_owned(),
        caller_role: contracts::SimCallerRoleV1::ModelTrain,
        caller_instance_id: "caller-a".to_owned(),
        account_id: "account-a".to_owned(),
        platform_source: contracts::PlatformSourceV1::Simulated,
    };
    let mut left = PlatformAccountDeltaV1 {
        schema_version: contracts::PLATFORM_ACCOUNT_DELTA_SCHEMA_V1.to_owned(),
        delta_id: "delta-a".to_owned(),
        delta_sha256: Sha256Hex("1".repeat(64)),
        account_instance_key: key.clone(),
        account_transition_seq: 7,
        predecessor: PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id: "previous-a".to_owned(),
            delta_sha256: Sha256Hex("2".repeat(64)),
            account_transition_seq: 6,
        },
        reason: contracts::PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied,
        reason_identity: "trade-a".to_owned(),
        condition_id: Some(ConditionId(format!("0x{}", "3".repeat(64)))),
        platform_effective_at_utc_ms: 410_000,
        balance_delta: contracts::PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: ExactDecimal("1".to_owned()),
            available_pusd_delta: ExactDecimal("2".to_owned()),
            principal_encumbrance_pusd_delta: ExactDecimal("-3".to_owned()),
            immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
            outcome_balance_deltas: Vec::new(),
            maker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            maker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
        },
        account_summary_before_sha256: Sha256Hex("4".repeat(64)),
        account_summary_after_sha256: Sha256Hex("5".repeat(64)),
    };
    let mut right = left.clone();
    right.delta_id = "delta-b".to_owned();
    right.delta_sha256 = Sha256Hex("6".repeat(64));
    right.account_instance_key.run_id = "run-b".to_owned();
    right.predecessor = PlatformAccountDeltaPredecessorV1::AccountDelta {
        delta_id: "previous-b".to_owned(),
        delta_sha256: Sha256Hex("7".repeat(64)),
        account_transition_seq: 6,
    };
    right.account_summary_before_sha256 = Sha256Hex("8".repeat(64));
    right.account_summary_after_sha256 = Sha256Hex("9".repeat(64));

    normalize_logical_account_delta_v1(&mut left);
    normalize_logical_account_delta_v1(&mut right);
    assert_eq!(left, right, "仅物理账户证据身份不同不应改变逻辑投影");
    right.account_transition_seq += 1;
    assert_ne!(left, right, "业务账户迁移序号必须保留在逻辑投影中");
    right.account_transition_seq -= 1;
    right.platform_effective_at_utc_ms += 1;
    assert_ne!(left, right, "平台生效时点必须保留在逻辑投影中");
}

fn normalize_logical_submission_metadata_v1(
    metadata: &mut SubmissionResponseMetadataV1,
    semantic_rules_sha256: &Sha256Hex,
) {
    metadata.rule_snapshot_sha256 = semantic_rules_sha256.clone();
    metadata.payload_sha256 = omitted_run_scoped_sha256_v1();
}

fn normalize_logical_submission_item_v1(item: &mut SubmissionResponseItemV1) {
    if let SubmissionItemOutcomeV1::Accepted(accepted) = &mut item.outcome {
        for reference in &mut accepted.account_deltas {
            normalize_logical_account_delta_reference_v1(reference);
        }
    }
}

fn normalize_logical_cancel_target_v1(target: &mut CancelTargetResponseV1) {
    if let CancelTargetOutcomeV1::Canceled(measurement) = &mut target.outcome
        && let Some(reference) = &mut measurement.account_delta
    {
        normalize_logical_account_delta_reference_v1(reference);
    }
}

fn normalize_logical_order_feedback_metadata_v1(metadata: &mut OrderLifecycleFeedbackMetadataV1) {
    normalize_logical_account_key_v1(&mut metadata.account_instance_key);
    metadata.payload_sha256 = omitted_run_scoped_sha256_v1();
}

#[test]
fn logical_feedback_projection_preserves_visibility_and_sequence_semantics() {
    let mut left = OrderLifecycleFeedbackMetadataV1 {
        schema_version: crate::execution::ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1.to_owned(),
        evidence_source: crate::execution::SIMULATED_EXECUTION_EVIDENCE_V1.to_owned(),
        account_instance_key: AccountInstanceKeyV1 {
            run_id: "run-a".to_owned(),
            caller_role: contracts::SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-a".to_owned(),
            account_id: "account-a".to_owned(),
            platform_source: contracts::PlatformSourceV1::Simulated,
        },
        session_id: "session-a".to_owned(),
        delivery_id: "delivery-a".to_owned(),
        platform_event_at_utc_ms: 100,
        feedback_received_at_utc_ms: 110,
        private_emission_sequence: 4,
        private_feedback_sequence: 5,
        payload_sha256: Sha256Hex("1".repeat(64)),
    };
    let mut right = left.clone();
    right.account_instance_key.run_id = "run-b".to_owned();
    right.payload_sha256 = Sha256Hex("2".repeat(64));
    normalize_logical_order_feedback_metadata_v1(&mut left);
    normalize_logical_order_feedback_metadata_v1(&mut right);
    assert_eq!(left, right, "仅运行证据身份不同不应改变反馈逻辑投影");

    right.feedback_received_at_utc_ms += 1;
    assert_ne!(left, right, "反馈接收时点必须保留在逻辑投影中");
    right.feedback_received_at_utc_ms -= 1;
    right.private_emission_sequence += 1;
    assert_ne!(left, right, "私有发射序号必须保留在逻辑投影中");
    right.private_emission_sequence -= 1;
    right.private_feedback_sequence += 1;
    assert_ne!(left, right, "私有反馈序号必须保留在逻辑投影中");
}

fn normalize_logical_settlement_feedback_metadata_v1(
    metadata: &mut SettlementLifecycleFeedbackMetadataV1,
) {
    normalize_logical_account_key_v1(&mut metadata.account_instance_key);
    metadata.payload_sha256 = omitted_run_scoped_sha256_v1();
}

fn normalize_logical_trade_feedback_metadata_v1(metadata: &mut TradeLifecycleFeedbackMetadataV1) {
    normalize_logical_account_key_v1(&mut metadata.account_instance_key);
    metadata.payload_sha256 = omitted_run_scoped_sha256_v1();
}

fn normalize_logical_settlement_event_v1(event: &mut SettlementLifecycleEventV1) {
    match event {
        SettlementLifecycleEventV1::MakerRebateAccrualFinalized { account_delta, .. }
        | SettlementLifecycleEventV1::MakerRebateDayFinalized { account_delta, .. }
        | SettlementLifecycleEventV1::TakerRebateDayFinalized { account_delta, .. }
        | SettlementLifecycleEventV1::AutoRedeemApplied { account_delta, .. } => {
            if let Some(delta) = account_delta {
                normalize_logical_account_delta_v1(delta);
            }
        }
        SettlementLifecycleEventV1::TakerRebateAccrualApplied { account_delta, .. }
        | SettlementLifecycleEventV1::MakerRebatePaid { account_delta, .. }
        | SettlementLifecycleEventV1::TakerRebatePaid { account_delta, .. } => {
            normalize_logical_account_delta_v1(account_delta);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
enum GoldenLogicalBridgeOutputV1 {
    PostOrder(PostOrderDirectResponseV1),
    PostOrders(PostOrdersDirectResponseV1),
    CancelOrder(CancelOrderDirectResponseV1),
    CancelOrders(CancelOrdersDirectResponseV1),
    CancelAllOrders(CancelAllOrdersDirectResponseV1),
    Order(OrderDirectResponseV1),
    Orders(OrdersDirectResponseV1),
    Trades(TradesDirectResponseV1),
    OrderLifecycle(OrderLifecycleFeedbackV1),
    SettlementLifecycle(SettlementLifecycleFeedbackV1),
    TradeLifecycle(TradeLifecycleFeedbackV1),
}

impl GoldenLogicalBridgeOutputV1 {
    fn direct_global_request_id(&self) -> Option<&str> {
        match self {
            Self::PostOrder(response) => Some(&response.metadata.global_request_id),
            Self::PostOrders(response) => Some(&response.metadata.global_request_id),
            Self::CancelOrder(response) => Some(&response.metadata.global_request_id),
            Self::CancelOrders(response) => Some(&response.metadata.global_request_id),
            Self::CancelAllOrders(response) => Some(&response.metadata.global_request_id),
            Self::Order(response) => Some(&response.metadata.global_request_id),
            Self::Orders(response) => Some(&response.metadata.global_request_id),
            Self::Trades(response) => Some(&response.metadata.global_request_id),
            Self::OrderLifecycle(_) | Self::SettlementLifecycle(_) | Self::TradeLifecycle(_) => {
                None
            }
        }
    }

    fn is_order_component(&self) -> bool {
        matches!(
            self,
            Self::PostOrder(_)
                | Self::PostOrders(_)
                | Self::CancelOrder(_)
                | Self::CancelOrders(_)
                | Self::CancelAllOrders(_)
                | Self::Order(_)
                | Self::Orders(_)
        )
    }

    fn is_trade_component(&self) -> bool {
        matches!(self, Self::Trades(_) | Self::TradeLifecycle(_))
    }

    fn is_feedback(&self) -> bool {
        matches!(
            self,
            Self::OrderLifecycle(_) | Self::SettlementLifecycle(_) | Self::TradeLifecycle(_)
        )
    }

    fn is_finality(&self) -> bool {
        matches!(self, Self::TradeLifecycle(_))
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::PostOrder(_) => "POST_ORDER",
            Self::PostOrders(_) => "POST_ORDERS",
            Self::CancelOrder(_) => "CANCEL_ORDER",
            Self::CancelOrders(_) => "CANCEL_ORDERS",
            Self::CancelAllOrders(_) => "CANCEL_ALL_ORDERS",
            Self::Order(_) => "ORDER",
            Self::Orders(_) => "ORDERS",
            Self::Trades(_) => "TRADES",
            Self::OrderLifecycle(_) => "ORDER_LIFECYCLE",
            Self::SettlementLifecycle(_) => "SETTLEMENT_LIFECYCLE",
            Self::TradeLifecycle(_) => "TRADE_LIFECYCLE",
        }
    }
}

#[derive(Clone, Default)]
struct GoldenCapturesV1 {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    post_orders: Arc<Mutex<Vec<Arc<PostOrdersDirectResponseV1>>>>,
    cancel_order: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    cancel_orders: Arc<Mutex<Vec<Arc<CancelOrdersDirectResponseV1>>>>,
    cancel_all: Arc<Mutex<Vec<Arc<CancelAllOrdersDirectResponseV1>>>>,
    order: Arc<Mutex<Vec<Arc<OrderDirectResponseV1>>>>,
    orders: Arc<Mutex<Vec<Arc<OrdersDirectResponseV1>>>>,
    trades: Arc<Mutex<Vec<Arc<TradesDirectResponseV1>>>>,
    logical_outputs: Arc<Mutex<Vec<GoldenLogicalBridgeOutputV1>>>,
}

impl GoldenCapturesV1 {
    fn bridge(&self, semantic_rules_sha256: &Sha256Hex) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let post_orders = Arc::clone(&self.post_orders);
        let cancel_order = Arc::clone(&self.cancel_order);
        let cancel_orders = Arc::clone(&self.cancel_orders);
        let cancel_all = Arc::clone(&self.cancel_all);
        let order = Arc::clone(&self.order);
        let orders = Arc::clone(&self.orders);
        let trades = Arc::clone(&self.trades);
        let outputs = std::array::from_fn::<_, 11, _>(|_| Arc::clone(&self.logical_outputs));
        let semantic_rules = std::array::from_fn::<_, 8, _>(|_| semantic_rules_sha256.clone());
        let [r0, r1, r2, r3, r4, r5, r6, r7] = semantic_rules;
        let [o0, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10] = outputs;
        SubmissionResponseBridgeV1::new(
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r0);
                normalize_logical_submission_item_v1(&mut logical.item);
                record_bridge_output(&o0, GoldenLogicalBridgeOutputV1::PostOrder(logical));
                post_order
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r1);
                for item in &mut logical.items {
                    normalize_logical_submission_item_v1(item);
                }
                record_bridge_output(&o1, GoldenLogicalBridgeOutputV1::PostOrders(logical));
                post_orders
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r2);
                normalize_logical_cancel_target_v1(&mut logical.target);
                record_bridge_output(&o2, GoldenLogicalBridgeOutputV1::CancelOrder(logical));
                cancel_order
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r3);
                for target in &mut logical.targets {
                    normalize_logical_cancel_target_v1(target);
                }
                record_bridge_output(&o3, GoldenLogicalBridgeOutputV1::CancelOrders(logical));
                cancel_orders
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r4);
                for target in &mut logical.targets {
                    normalize_logical_cancel_target_v1(target);
                }
                record_bridge_output(&o4, GoldenLogicalBridgeOutputV1::CancelAllOrders(logical));
                cancel_all
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r5);
                record_bridge_output(&o5, GoldenLogicalBridgeOutputV1::Order(logical));
                order
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r6);
                record_bridge_output(&o6, GoldenLogicalBridgeOutputV1::Orders(logical));
                orders
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_submission_metadata_v1(&mut logical.metadata, &r7);
                record_bridge_output(&o7, GoldenLogicalBridgeOutputV1::Trades(logical));
                trades
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_order_feedback_metadata_v1(&mut logical.metadata);
                normalize_logical_account_delta_v1(&mut logical.account_delta);
                record_bridge_output(&o8, GoldenLogicalBridgeOutputV1::OrderLifecycle(logical));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_trade_feedback_metadata_v1(&mut logical.metadata);
                if let Some(reference) = &mut logical.economic_application_delta {
                    normalize_logical_account_delta_reference_v1(reference);
                }
                if let Some(delta) = &mut logical.account_delta {
                    normalize_logical_account_delta_v1(delta);
                }
                record_bridge_output(&o9, GoldenLogicalBridgeOutputV1::TradeLifecycle(logical));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                let mut logical = response.as_ref().clone();
                normalize_logical_settlement_feedback_metadata_v1(&mut logical.metadata);
                normalize_logical_settlement_event_v1(&mut logical.event);
                record_bridge_output(
                    &o10,
                    GoldenLogicalBridgeOutputV1::SettlementLifecycle(logical),
                );
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
        )
    }
}

fn record_bridge_output(
    output: &Mutex<Vec<GoldenLogicalBridgeOutputV1>>,
    value: GoldenLogicalBridgeOutputV1,
) {
    output.lock().expect("正式 runner 输出锁").push(value);
}

const GOLDEN_LOGICAL_MARKET_SNAPSHOT_SCHEMA_V1: &str =
    "component_golden_logical_market_snapshot_v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct GoldenLogicalMarketSnapshotV1 {
    schema_version: String,
    condition_id: ConditionId,
    market_external_book_level_count: u64,
    execution_queue_level_count: u64,
    execution_queue_segment_count: u64,
    execution_queue_debt_base_units: u128,
    input_seal: Option<(MarketEventPathV1, InputPathSealOutcomeV1)>,
    hot_orders: Vec<OrderProjectionV1>,
    compact_orders: Vec<OrderProjectionV1>,
    hot_trades: Vec<TradeProjectionV1>,
    compact_trades: Vec<TradeProjectionV1>,
    retained_rebate_fill_state_count: u64,
    query_root_body_count: u64,
    issued_cursor_count: u64,
    earliest_retained_root_sequence: u64,
    pending_direct_response_count: u64,
    pending_lifecycle_feedback_count: u64,
    compacted_confirmed_private_delivery_count: u64,
    full_confirmed_private_delivery_count: u64,
}

fn logical_diagnostic_count_v1(value: usize, field: &str) -> u64 {
    u64::try_from(value).unwrap_or_else(|_| panic!("黄金逻辑诊断字段 {field} 适合 u64"))
}

fn logical_market_snapshot_from_diagnostic_v1(
    condition_id: &str,
    diagnostic: crate::execution::Task41DiagnosticsV1,
) -> GoldenLogicalMarketSnapshotV1 {
    GoldenLogicalMarketSnapshotV1 {
        schema_version: GOLDEN_LOGICAL_MARKET_SNAPSHOT_SCHEMA_V1.to_owned(),
        condition_id: ConditionId(condition_id.to_owned()),
        market_external_book_level_count: logical_diagnostic_count_v1(
            diagnostic.market_external_book_level_count,
            "market_external_book_level_count",
        ),
        execution_queue_level_count: logical_diagnostic_count_v1(
            diagnostic.execution_queue_level_count,
            "execution_queue_level_count",
        ),
        execution_queue_segment_count: logical_diagnostic_count_v1(
            diagnostic.execution_queue_segment_count,
            "execution_queue_segment_count",
        ),
        execution_queue_debt_base_units: diagnostic.execution_queue_debt_base_units,
        input_seal: diagnostic.input_seal,
        hot_orders: diagnostic.hot_orders,
        compact_orders: diagnostic.compact_orders,
        hot_trades: diagnostic.hot_trades,
        compact_trades: diagnostic.compact_trades,
        retained_rebate_fill_state_count: logical_diagnostic_count_v1(
            diagnostic.retained_rebate_fill_state_count,
            "retained_rebate_fill_state_count",
        ),
        query_root_body_count: logical_diagnostic_count_v1(
            diagnostic.query_root_body_count,
            "query_root_body_count",
        ),
        issued_cursor_count: logical_diagnostic_count_v1(
            diagnostic.issued_cursor_count,
            "issued_cursor_count",
        ),
        earliest_retained_root_sequence: diagnostic.earliest_retained_root_sequence,
        pending_direct_response_count: logical_diagnostic_count_v1(
            diagnostic.pending_direct_response_count,
            "pending_direct_response_count",
        ),
        pending_lifecycle_feedback_count: logical_diagnostic_count_v1(
            diagnostic.pending_lifecycle_feedback_count,
            "pending_lifecycle_feedback_count",
        ),
        compacted_confirmed_private_delivery_count: logical_diagnostic_count_v1(
            diagnostic.compacted_confirmed_private_delivery_count,
            "compacted_confirmed_private_delivery_count",
        ),
        full_confirmed_private_delivery_count: logical_diagnostic_count_v1(
            diagnostic.full_confirmed_private_delivery_count,
            "full_confirmed_private_delivery_count",
        ),
    }
}

fn logical_market_snapshot_v1(
    session: &SimExecutionSessionV1,
    condition_id: &str,
) -> GoldenLogicalMarketSnapshotV1 {
    logical_market_snapshot_from_diagnostic_v1(
        condition_id,
        session.task41_diagnostics_for_tests(condition_id),
    )
}

#[test]
fn logical_market_snapshot_preserves_market_queue_and_input_seal_semantics() {
    let snapshot = logical_market_snapshot_from_diagnostic_v1(
        "condition-a",
        crate::execution::Task41DiagnosticsV1 {
            market_external_book_level_count: 2,
            execution_queue_level_count: 3,
            execution_queue_segment_count: 4,
            execution_queue_debt_base_units: 5,
            input_seal: Some((
                MarketEventPathV1::HiddenPlatform,
                InputPathSealOutcomeV1::Exhausted,
            )),
            hot_orders: Vec::new(),
            compact_orders: Vec::new(),
            hot_trades: Vec::new(),
            compact_trades: Vec::new(),
            retained_rebate_fill_state_count: 6,
            query_root_body_count: 7,
            issued_cursor_count: 8,
            earliest_retained_root_sequence: 9,
            pending_direct_response_count: 10,
            pending_lifecycle_feedback_count: 11,
            compacted_confirmed_private_delivery_count: 12,
            full_confirmed_private_delivery_count: 13,
            confirmed_direct_response_confirmations: Vec::new(),
            confirmed_lifecycle_feedback_confirmations: Vec::new(),
        },
    );
    let digest = |value: &GoldenLogicalMarketSnapshotV1| {
        sha256_serializable("TASK44_LOGICAL_MARKET_SNAPSHOT_TEST_V1", value)
    };
    let expected = digest(&snapshot);

    let mut changed = snapshot.clone();
    changed.condition_id = ConditionId("condition-b".to_owned());
    assert_ne!(digest(&changed), expected, "市场身份必须进入逻辑摘要");

    let mut changed = snapshot.clone();
    changed.market_external_book_level_count += 1;
    assert_ne!(digest(&changed), expected, "外部簿状态必须进入逻辑摘要");

    let mut changed = snapshot.clone();
    changed.execution_queue_segment_count += 1;
    changed.execution_queue_debt_base_units += 1;
    assert_ne!(
        digest(&changed),
        expected,
        "执行队列业务状态必须进入逻辑摘要"
    );

    let mut changed = snapshot;
    changed.input_seal = None;
    assert_ne!(digest(&changed), expected, "输入封口状态必须进入逻辑摘要");
}

struct DeferredCallLatencyV1 {
    started: Instant,
    query: bool,
}

fn defer_call_latency_v1(
    pending: &mut BTreeMap<String, DeferredCallLatencyV1>,
    global_request_id: String,
    started: Instant,
    query: bool,
) {
    assert!(
        pending
            .insert(global_request_id, DeferredCallLatencyV1 { started, query })
            .is_none(),
        "同一全局请求不得重复登记延迟结算"
    );
}

fn settle_deferred_call_latency_v1(
    pending: &mut BTreeMap<String, DeferredCallLatencyV1>,
    global_request_id: &str,
    request_latency_ns: &mut Vec<u64>,
    query_latency_ns: &mut Vec<u64>,
) {
    let Some(deferred) = pending.remove(global_request_id) else {
        return;
    };
    let elapsed = elapsed_ns(deferred.started);
    if deferred.query {
        query_latency_ns.push(elapsed);
    } else {
        request_latency_ns.push(elapsed);
    }
}

#[test]
fn deferred_call_latency_is_recorded_only_after_matching_bridge_delivery() {
    let mut pending = BTreeMap::new();
    let mut request_latency_ns = Vec::new();
    let mut query_latency_ns = Vec::new();
    defer_call_latency_v1(
        &mut pending,
        "deferred-request".to_owned(),
        Instant::now(),
        false,
    );
    assert!(request_latency_ns.is_empty());
    settle_deferred_call_latency_v1(
        &mut pending,
        "different-request",
        &mut request_latency_ns,
        &mut query_latency_ns,
    );
    assert!(request_latency_ns.is_empty());
    assert_eq!(pending.len(), 1);
    settle_deferred_call_latency_v1(
        &mut pending,
        "deferred-request",
        &mut request_latency_ns,
        &mut query_latency_ns,
    );
    assert_eq!(request_latency_ns.len(), 1);
    assert!(request_latency_ns[0] > 0);
    assert!(query_latency_ns.is_empty() && pending.is_empty());
}

struct GoldenTrajectoryStateV1 {
    trajectory: char,
    semantic_rules_sha256: Sha256Hex,
    control: crate::session::SessionCreationControlResultV2,
    session: SimExecutionSessionV1,
    captures: GoldenCapturesV1,
    caller_sequence: u64,
    path_deliveries: u16,
    top_level_calls: u16,
    request_batch_items: u64,
    unique_fills: u64,
    affected_orders: u64,
    event_kind_path_deliveries: ComponentGoldenEventKindCountsV1,
    event_latency_ns: Vec<u64>,
    request_latency_ns: Vec<u64>,
    query_latency_ns: Vec<u64>,
    deferred_call_latency: BTreeMap<String, DeferredCallLatencyV1>,
    checkpoint_latency_ns: Vec<u64>,
    retention_latency_ns: Vec<u64>,
    trajectory_started: Option<Instant>,
    first_page_count: usize,
    continuation_page_count: usize,
    first_market_hot: Option<GoldenLogicalMarketSnapshotV1>,
    first_market_compact: Option<GoldenLogicalMarketSnapshotV1>,
    first_market_expired: Option<GoldenLogicalMarketSnapshotV1>,
    second_market_hot: Option<GoldenLogicalMarketSnapshotV1>,
    second_market_compact: Option<GoldenLogicalMarketSnapshotV1>,
    second_market_expired: Option<GoldenLogicalMarketSnapshotV1>,
    first_market_sample_indexes: Option<[u64; 3]>,
    second_market_sample_indexes: Option<[u64; 3]>,
    first_market_completion_ns: Option<u64>,
    second_market_completion_ns: Option<u64>,
    feedback_was_bounded: bool,
    first_market_redeemed_before_second_request: bool,
    downstream_durable_through: u64,
    last_request_sent_at_utc_ms: Option<i64>,
    hidden_closed_through_utc_ms: i64,
    public_closed_through_utc_ms: i64,
}

impl GoldenTrajectoryStateV1 {
    fn new(
        trajectory: char,
        semantic_rules_sha256: Sha256Hex,
        control: crate::session::SessionCreationControlResultV2,
        session: SimExecutionSessionV1,
        captures: GoldenCapturesV1,
    ) -> Self {
        Self {
            trajectory,
            semantic_rules_sha256,
            control,
            session,
            captures,
            caller_sequence: 0,
            path_deliveries: 0,
            top_level_calls: 0,
            request_batch_items: 0,
            unique_fills: 0,
            affected_orders: 0,
            event_kind_path_deliveries: ComponentGoldenEventKindCountsV1::default(),
            event_latency_ns: Vec::new(),
            request_latency_ns: Vec::new(),
            query_latency_ns: Vec::new(),
            deferred_call_latency: BTreeMap::new(),
            checkpoint_latency_ns: Vec::new(),
            retention_latency_ns: Vec::new(),
            trajectory_started: None,
            first_page_count: 0,
            continuation_page_count: 0,
            first_market_hot: None,
            first_market_compact: None,
            first_market_expired: None,
            second_market_hot: None,
            second_market_compact: None,
            second_market_expired: None,
            first_market_sample_indexes: None,
            second_market_sample_indexes: None,
            first_market_completion_ns: None,
            second_market_completion_ns: None,
            feedback_was_bounded: false,
            first_market_redeemed_before_second_request: false,
            downstream_durable_through: 0,
            last_request_sent_at_utc_ms: None,
            hidden_closed_through_utc_ms: 0,
            public_closed_through_utc_ms: 0,
        }
    }

    fn begin_timed_execution(&mut self) {
        assert!(
            self.trajectory_started.replace(Instant::now()).is_none(),
            "每条黄金轨迹只能开始计时一次"
        );
    }

    fn elapsed_from_start(&self) -> u64 {
        elapsed_ns(
            self.trajectory_started
                .expect("黄金轨迹必须在执行业务阶段前开始计时"),
        )
    }

    fn next_context(
        &mut self,
        profile: &EmpiricalDelayProfile,
        operation: &str,
        desired_arrival_at_utc_ms: i64,
    ) -> crate::session::RequestContextV1 {
        self.caller_sequence += 1;
        let desired_arrival_at_utc_ms = self
            .session
            .logical_platform_driven_through_for_tests()
            .map_or(desired_arrival_at_utc_ms, |closed_through| {
                desired_arrival_at_utc_ms.max(closed_through.saturating_add(1))
            });
        let mut context = context_for_arrival(
            &self.session,
            profile,
            &format!("task44-{}-{operation}", self.trajectory),
            self.caller_sequence,
            desired_arrival_at_utc_ms,
        );
        context.request_sent_at_utc_ms = context.request_sent_at_utc_ms.max(
            self.last_request_sent_at_utc_ms
                .map_or(self.public_closed_through_utc_ms, |last| {
                    last.max(self.public_closed_through_utc_ms)
                }),
        );
        self.last_request_sent_at_utc_ms = Some(context.request_sent_at_utc_ms);
        context
    }

    fn record_call(&mut self, started: Instant, query: bool) {
        self.top_level_calls = self.top_level_calls.checked_add(1).expect("顶层调用计数");
        let elapsed = elapsed_ns(started);
        if query {
            self.query_latency_ns.push(elapsed);
        } else {
            self.request_latency_ns.push(elapsed);
        }
    }

    fn defer_call(&mut self, global_request_id: String, started: Instant, query: bool) {
        self.top_level_calls = self.top_level_calls.checked_add(1).expect("顶层调用计数");
        defer_call_latency_v1(
            &mut self.deferred_call_latency,
            global_request_id,
            started,
            query,
        );
    }

    fn settle_deferred_call(&mut self, global_request_id: &str) {
        settle_deferred_call_latency_v1(
            &mut self.deferred_call_latency,
            global_request_id,
            &mut self.request_latency_ns,
            &mut self.query_latency_ns,
        );
    }

    fn settle_delivered_calls_since(&mut self, first_new_output_index: usize) {
        let global_request_ids = {
            let outputs = self
                .captures
                .logical_outputs
                .lock()
                .expect("读取黄金桥输出");
            assert!(
                first_new_output_index <= outputs.len(),
                "私有交付批次起始位置不得超过已捕获输出"
            );
            outputs[first_new_output_index..]
                .iter()
                .filter_map(GoldenLogicalBridgeOutputV1::direct_global_request_id)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        for global_request_id in global_request_ids {
            self.settle_deferred_call(&global_request_id);
        }
    }
}

fn record_golden_event_kind_path_deliveries_v1(
    counts: &mut ComponentGoldenEventKindCountsV1,
    events: &[ExecutionMarketEventV3],
) {
    for event in events {
        let count = match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(_) => &mut counts.initial_l2_snapshot,
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(_) => {
                &mut counts.level_execution_transition
            }
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => {
                &mut counts.market_resolution_terminal
            }
            _ => panic!("正式黄金负载出现未预登记的事件种类"),
        };
        *count = count.checked_add(1).expect("黄金事件种类路径计数");
    }
}

fn golden_query_source_breakdown_v1(
    diagnostics: &[TaskQuerySourceDiagnosticV1],
) -> ComponentGoldenQuerySourceBreakdownV1 {
    let unique_request_ids = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.global_request_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        unique_request_ids.len(),
        diagnostics.len(),
        "每个黄金查询只能形成一项真实来源诊断"
    );
    let mut breakdown = ComponentGoldenQuerySourceBreakdownV1::default();
    for diagnostic in diagnostics {
        let methods = match diagnostic.source {
            TaskQueryProjectionSourceV1::HotState => &mut breakdown.hot_state,
            TaskQueryProjectionSourceV1::RetainedProjection => &mut breakdown.retained_projection,
            TaskQueryProjectionSourceV1::Mixed | TaskQueryProjectionSourceV1::NoRows => {
                panic!("正式黄金查询必须由单一热状态或保留投影形成非空结果")
            }
        };
        let method = match diagnostic.method {
            TaskQueryMethodV1::Order => &mut methods.order,
            TaskQueryMethodV1::Orders => &mut methods.orders,
            TaskQueryMethodV1::Trades => &mut methods.trades,
        };
        method.calls = method.calls.checked_add(1).expect("黄金查询来源调用计数");
        method.pages = method
            .pages
            .checked_add(diagnostic.pages)
            .expect("黄金查询来源分页计数");
        method.return_rows = method
            .return_rows
            .checked_add(diagnostic.return_rows)
            .expect("黄金查询来源返回行计数");
    }
    let expected = ComponentGoldenQuerySourceBreakdownV1 {
        hot_state: ComponentGoldenQueryMethodsV1 {
            order: ComponentGoldenQueryMethodBreakdownV1 {
                calls: 2,
                pages: 0,
                return_rows: 2,
            },
            orders: ComponentGoldenQueryMethodBreakdownV1 {
                calls: 2,
                pages: 2,
                return_rows: 102,
            },
            trades: ComponentGoldenQueryMethodBreakdownV1 {
                calls: 2,
                pages: 2,
                return_rows: 109,
            },
        },
        retained_projection: ComponentGoldenQueryMethodsV1::default(),
    };
    assert_eq!(
        breakdown, expected,
        "黄金查询必须按真实决策分支记录方法、分页、返回行和热态/保留投影来源"
    );
    breakdown
}

fn apply_golden_group(
    state: &mut GoldenTrajectoryStateV1,
    input: &GoldenInputV1,
    group_index: usize,
) {
    let group = &input.groups[group_index];
    let hidden_closure = input
        .groups
        .get(group_index + 1)
        .map_or(MillisecondBucketClosureV1::EndOfInput, |next| {
            MillisecondBucketClosureV1::NextEventAt(next.source_at_utc_ms)
        });
    let public_closure = input
        .groups
        .get(group_index + 1)
        .map_or(MillisecondBucketClosureV1::EndOfInput, |next| {
            MillisecondBucketClosureV1::NextEventAt(next.received_at_utc_ms)
        });
    let encoded = group
        .events
        .iter()
        .map(|event| encode_execution_market_event_v3(event).expect("编码黄金事件桶"))
        .collect::<Vec<_>>();
    for (hidden, at_utc_ms, closure) in [
        (true, group.source_at_utc_ms, hidden_closure),
        (false, group.received_at_utc_ms, public_closure),
    ] {
        let start = if hidden {
            state.session.platform_event_index_position()
        } else {
            state.session.public_event_index_position()
        };
        let next = start + u64::try_from(encoded.len()).expect("黄金事件数适合 u64");
        let bucket = if hidden {
            ClosedMarketEventBucketV1::hidden(at_utc_ms, start, next, closure, encoded.clone())
        } else {
            ClosedMarketEventBucketV1::public(at_utc_ms, start, next, closure, encoded.clone())
        };
        let started = Instant::now();
        assert_eq!(
            state.session.apply_closed_market_event_bucket(bucket),
            Ok(MarketBucketApplyDispositionV1::Applied),
            "正式黄金事件路径必须真实应用"
        );
        let latency = elapsed_ns(started);
        state.event_latency_ns.push(latency);
        record_golden_event_kind_path_deliveries_v1(
            &mut state.event_kind_path_deliveries,
            &group.events,
        );
        state.path_deliveries = state
            .path_deliveries
            .checked_add(u16::try_from(encoded.len()).expect("黄金路径计数适合 u16"))
            .expect("黄金路径计数");
        if hidden {
            state.hidden_closed_through_utc_ms = at_utc_ms;
        } else {
            state.public_closed_through_utc_ms = at_utc_ms;
        }
    }
}

fn close_empty_prefix_through(
    state: &mut GoldenTrajectoryStateV1,
    hidden: bool,
    at_utc_ms: i64,
    next_event_at_utc_ms: i64,
) {
    let closed = if hidden {
        &mut state.hidden_closed_through_utc_ms
    } else {
        &mut state.public_closed_through_utc_ms
    };
    if *closed >= at_utc_ms {
        return;
    }
    assert!(at_utc_ms < next_event_at_utc_ms);
    let position = if hidden {
        state.session.platform_event_index_position()
    } else {
        state.session.public_event_index_position()
    };
    let bucket = if hidden {
        ClosedMarketEventBucketV1::hidden(
            at_utc_ms,
            position,
            position,
            MillisecondBucketClosureV1::NextEventAt(next_event_at_utc_ms),
            Vec::<Vec<u8>>::new(),
        )
    } else {
        ClosedMarketEventBucketV1::public(
            at_utc_ms,
            position,
            position,
            MillisecondBucketClosureV1::NextEventAt(next_event_at_utc_ms),
            Vec::<Vec<u8>>::new(),
        )
    };
    let started = Instant::now();
    assert_eq!(
        state.session.apply_closed_market_event_bucket(bucket),
        Ok(MarketBucketApplyDispositionV1::Applied),
        "正式 runner 必须经真实空闭桶证明下一规范事件前没有隐藏事件"
    );
    state.event_latency_ns.push(elapsed_ns(started));
    *closed = at_utc_ms;
}

fn drive_registered_request(
    state: &mut GoldenTrajectoryStateV1,
    effective_times: &[i64],
    inbound_delay_ms: u64,
    deliver: bool,
    next_source_at_utc_ms: i64,
    next_received_at_utc_ms: i64,
) {
    assert!(!effective_times.is_empty());
    for (index, effective_at) in effective_times.iter().enumerate() {
        close_empty_prefix_through(state, true, *effective_at, next_source_at_utc_ms);
        assert_eq!(
            state
                .session
                .drive_next_execution_action_through(*effective_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: index + 1 == effective_times.len(),
            })
        );
    }
    let response_at = effective_times[effective_times.len() - 1]
        + i64::try_from(inbound_delay_ms).expect("黄金响应时延适合 i64");
    close_empty_prefix_through(state, true, response_at, next_source_at_utc_ms);
    assert_eq!(
        state
            .session
            .drive_next_execution_action_through(response_at),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    close_empty_prefix_through(state, false, response_at, next_received_at_utc_ms);
    if deliver {
        drain_private_through(state, response_at);
    }
}

fn drain_private_through(state: &mut GoldenTrajectoryStateV1, through_utc_ms: i64) {
    loop {
        let first_new_output_index = state
            .captures
            .logical_outputs
            .lock()
            .expect("读取黄金桥输出")
            .len();
        let disposition = state
            .session
            .drive_private_delivery_through(through_utc_ms)
            .expect("按私有序号排空黄金反馈");
        match disposition {
            SubmissionDeliveryDispositionV1::Delivered => {
                state.settle_delivered_calls_since(first_new_output_index);
                continue;
            }
            SubmissionDeliveryDispositionV1::NoVisibleResponse => break,
            other => panic!("黄金桥不应阻塞或关闭，实际为 {other:?}"),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_call_latency_settles_on_the_matching_real_bridge_delivery() {
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let profile = load_embedded_proxy_latency_profile().expect("加载延后响应时延配置档");
    let scenario = scenario();
    let run_id = "task44-deferred-bridge";
    let captures = GoldenCapturesV1::default();
    let semantic_rules_sha256 = formal_rule_semantic_sha256_v1(&rules);
    let mut process = SimExecutionProcessV1::try_new(
        run_id.to_owned(),
        successful_policy_for_tests(),
        rules,
        Arc::clone(&profile),
        Arc::clone(&scenario),
    )
    .expect("创建延后响应桥测试运行根");
    let mut spec = model_train_spec(&scenario, "task44-deferred-bridge-session", 'd');
    spec.run_id = run_id.to_owned();
    let prepared = process
        .create_session(spec.clone(), captures.bridge(&semantic_rules_sha256))
        .expect("创建延后响应桥测试会话");
    process
        .activate_session(activation_request(&spec, &prepared.control))
        .expect("激活延后响应桥测试会话");
    let mut state = GoldenTrajectoryStateV1::new(
        'd',
        semantic_rules_sha256,
        prepared.control,
        prepared.session,
        captures,
    );
    let mut response_at = 0;
    for index in 0..2 {
        let started = Instant::now();
        let context = state.next_context(
            &profile,
            &format!("deferred-order-query-{index}"),
            100_000 + i64::from(index) * 10_000,
        );
        let global_request_id = context.global_request_id.clone();
        let registration = state
            .session
            .order(OrderRequestV1 {
                context,
                platform_order_id: "a".repeat(64),
            })
            .expect("登记延后交付的真实查询");
        response_at = registration.receipt.transit.platform_arrived_at_utc_ms
            + i64::try_from(registration.receipt.transit.inbound_delay_ms)
                .expect("延后响应时延适合 i64");
        drive_registered_request(
            &mut state,
            &[registration.receipt.transit.platform_arrived_at_utc_ms],
            registration.receipt.transit.inbound_delay_ms,
            false,
            1_000_000,
            1_000_000,
        );
        state.defer_call(global_request_id, started, true);
    }
    assert!(state.query_latency_ns.is_empty());
    assert_eq!(state.deferred_call_latency.len(), 2);
    drain_private_through(&mut state, response_at);
    assert_eq!(state.query_latency_ns.len(), 2);
    assert!(state.query_latency_ns.iter().all(|elapsed| *elapsed > 0));
    assert!(state.deferred_call_latency.is_empty());
    assert_eq!(state.top_level_calls, 2);
}

fn accepted_order_id(outcome: &SubmissionItemOutcomeV1) -> String {
    match outcome {
        SubmissionItemOutcomeV1::Accepted(accepted) => accepted.simulated_platform_order_id.clone(),
        other => panic!("正式黄金提交必须接受，实际为 {other:?}"),
    }
}

fn golden_gtc_item(market: &PlatformMarketV1, item_id: String, price: Decimal) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id,
        order: SimBuyOrderV1 {
            condition_id: market.market().condition_id.clone(),
            token_id: market.assets()[0].token_id.clone(),
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: price,
                size_shares: Decimal::new(5, 0),
                post_only: true,
            },
        },
    }
}

fn golden_fak_item(market: &PlatformMarketV1, item_id: String) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id,
        order: SimBuyOrderV1 {
            condition_id: market.market().condition_id.clone(),
            token_id: market.assets()[0].token_id.clone(),
            kind: SimBuyOrderKindV1::FakMarket {
                spend_pusd: Decimal::new(6, 0),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            },
        },
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos().max(1)).unwrap_or(u64::MAX)
}

fn trajectory_elapsed_covers_completion_v1(
    trajectory_elapsed_ns: u64,
    completion_started_ns: u64,
    completion_latency_ns: u64,
) -> bool {
    completion_started_ns
        .checked_add(completion_latency_ns)
        .is_some_and(|completion_finished_ns| completion_finished_ns <= trajectory_elapsed_ns)
}

#[test]
fn trajectory_elapsed_must_cover_the_final_completion_transaction() {
    assert!(trajectory_elapsed_covers_completion_v1(140, 100, 40));
    assert!(
        !trajectory_elapsed_covers_completion_v1(100, 100, 40),
        "在 clean completion 前冻结的逐轨迹墙钟必须被反例拒绝"
    );
}

fn diagnostic_quantity(value: &GoldenLogicalMarketSnapshotV1, hot: bool) -> ResourceQuantityV1 {
    let records = if hot {
        value.hot_orders.len() + value.hot_trades.len()
    } else {
        value.compact_orders.len() + value.compact_trades.len()
    };
    ResourceQuantityV1::new(
        u64::try_from(records).expect("黄金逻辑诊断记录数适合 u64"),
        u64::try_from(serde_json::to_vec(value).expect("编码黄金滚动诊断").len())
            .expect("黄金滚动诊断字节适合 u64"),
    )
}

fn budget_quantities(
    usage: &SimulationResourceUsageV1,
) -> BTreeMap<ResourceBudgetPathV1, ResourceQuantityV1> {
    BTreeMap::from([
        (
            ResourceBudgetPathV1::SessionInMemoryState,
            usage.session.in_memory_state,
        ),
        (
            ResourceBudgetPathV1::SessionHotMarkets,
            usage.session.hot_markets,
        ),
        (
            ResourceBudgetPathV1::SessionInputQueue,
            usage.session.input_queue,
        ),
        (
            ResourceBudgetPathV1::SessionRequestQueue,
            usage.session.request_queue,
        ),
        (
            ResourceBudgetPathV1::SessionFeedbackQueue,
            usage.session.feedback_queue,
        ),
        (
            ResourceBudgetPathV1::SessionActiveOrders,
            usage.session.active_orders,
        ),
        (
            ResourceBudgetPathV1::SessionInternalEvents,
            usage.session.internal_events,
        ),
        (
            ResourceBudgetPathV1::SessionQueryCache,
            usage.session.query_cache,
        ),
        (ResourceBudgetPathV1::SessionIndexes, usage.session.indexes),
        (
            ResourceBudgetPathV1::RunInMemoryState,
            usage.run.in_memory_state,
        ),
        (ResourceBudgetPathV1::RunSessions, usage.run.sessions),
        (ResourceBudgetPathV1::RunHotMarkets, usage.run.hot_markets),
        (ResourceBudgetPathV1::RunInputQueue, usage.run.input_queue),
        (
            ResourceBudgetPathV1::RunRequestQueue,
            usage.run.request_queue,
        ),
        (
            ResourceBudgetPathV1::RunFeedbackQueue,
            usage.run.feedback_queue,
        ),
        (
            ResourceBudgetPathV1::RunActiveOrders,
            usage.run.active_orders,
        ),
        (
            ResourceBudgetPathV1::RunInternalEvents,
            usage.run.internal_events,
        ),
        (ResourceBudgetPathV1::RunQueryCache, usage.run.query_cache),
        (ResourceBudgetPathV1::RunIndexes, usage.run.indexes),
        (
            ResourceBudgetPathV1::RunCapitalFence,
            usage.run.capital_fence,
        ),
        (
            ResourceBudgetPathV1::DurableEvidence,
            usage.durable.evidence,
        ),
        (
            ResourceBudgetPathV1::DurableCheckpoints,
            usage.durable.checkpoints,
        ),
        (
            ResourceBudgetPathV1::DurableArtifacts,
            usage.durable.artifacts,
        ),
        (
            ResourceBudgetPathV1::DurableRuntimeRoot,
            usage.durable.runtime_root,
        ),
    ])
}

fn transaction_resource_increments_v1(
    observations: &[ResourceTransactionObservationV1],
) -> Vec<ResourceTransactionIncrementV1> {
    observations
        .iter()
        .enumerate()
        .map(|(index, observation)| {
            let before = budget_quantities(&observation.maintenance_steady_before);
            let peak = budget_quantities(&observation.during_commit_peak);
            let increments = peak
                .iter()
                .map(|(path, quantity)| {
                    let before = before.get(path).copied().unwrap_or_default();
                    (
                        *path,
                        ResourceQuantityV1::new(
                            quantity.records.saturating_sub(before.records),
                            quantity.bytes.saturating_sub(before.bytes),
                        ),
                    )
                })
                .collect();
            ResourceTransactionIncrementV1 {
                transaction_index: u64::try_from(index).expect("逐事务资源序号适合 u64"),
                increments,
            }
        })
        .collect()
}

#[test]
fn transaction_increment_uses_each_commit_steady_to_peak_pair() {
    let observation = |before_bytes, peak_bytes, after_bytes| {
        let mut before = SimulationResourceUsageV1::default();
        before.durable.checkpoints.bytes = before_bytes;
        before.durable.runtime_root.bytes = before_bytes;
        let mut peak = SimulationResourceUsageV1::default();
        peak.durable.checkpoints.bytes = peak_bytes;
        peak.durable.runtime_root.bytes = peak_bytes;
        let mut after = SimulationResourceUsageV1::default();
        after.durable.checkpoints.bytes = after_bytes;
        after.durable.runtime_root.bytes = after_bytes;
        ResourceTransactionObservationV1 {
            maintenance_steady_before: before,
            during_commit_peak: peak,
            maintenance_succeeded_steady_after: after,
        }
    };
    let observations = [
        observation(0, 100, 100),
        observation(100, 200, 100),
        observation(100, 1_100, 1_000),
    ];
    let increments = transaction_resource_increments_v1(&observations);
    assert_eq!(
        increments[2].increments[&ResourceBudgetPathV1::DurableCheckpoints].bytes,
        1_000,
        "第三项必须按本事务提交前稳态求增量，不能按相邻提交峰值误算为900"
    );
    assert_eq!(
        increments[2].increments[&ResourceBudgetPathV1::DurableRuntimeRoot].bytes,
        1_000,
        "运行根也必须包含本事务完整写时复制新增量"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn transaction_increment_uses_the_current_session_on_both_sides() {
    let mut fixture =
        PersistentSessionPairV1::new(SimulationResourcePolicyV1::component_golden_v1()).await;
    let initial_observations = fixture
        .process
        .resource_transaction_observations_for_tests()
        .expect("读取双会话创建资源证据");
    let second_creation = initial_observations
        .get(2)
        .expect("双会话夹具第三项事实必须创建轨迹 B");
    assert_eq!(
        second_creation
            .maintenance_steady_before
            .session
            .in_memory_state,
        ResourceQuantityV1::default(),
        "创建轨迹 B 前的同目标会话基线必须为零，不能退化为轨迹 A 的最大值"
    );
    assert!(
        second_creation
            .during_commit_peak
            .session
            .in_memory_state
            .records
            > 0
    );
    fixture
        .first
        .order(order_request(
            &fixture.first,
            &fixture.profile,
            "task441-amax-session-a",
            1,
            150_000,
        ))
        .expect("先让轨迹 A 保留较大的请求队列基线");
    let before = fixture
        .process
        .resource_snapshot_for_tests()
        .expect("读取轨迹 B 事务前资源诊断");
    let first_id = fixture.first.session_spec().session_id.clone();
    let second_id = fixture.second.session_spec().session_id.clone();
    assert!(
        before.sessions[&first_id].request_queue.records
            > before.sessions[&second_id].request_queue.records,
        "反例必须先形成 A 高、B 低的会话请求队列"
    );
    let observation_count = fixture
        .process
        .resource_transaction_observations_for_tests()
        .expect("读取 B 事务前资源证据数量")
        .len();

    fixture
        .second
        .order(order_request(
            &fixture.second,
            &fixture.profile,
            "task441-amax-session-b",
            1,
            150_000,
        ))
        .expect("轨迹 B 请求必须成功提交");
    let observations = fixture
        .process
        .resource_transaction_observations_for_tests()
        .expect("读取 B 事务后的资源证据");
    assert_eq!(observations.len(), observation_count + 1);
    let observation = observations.last().expect("B 事务必须有资源证据");
    assert_eq!(
        observation.maintenance_steady_before.session.request_queue,
        before.sessions[&second_id].request_queue,
        "同事务 before 必须选择轨迹 B，不能取轨迹 A 的逐维最大值"
    );
    assert!(
        observation.during_commit_peak.session.request_queue.records
            > observation
                .maintenance_steady_before
                .session
                .request_queue
                .records,
        "轨迹 B 的真实请求队列增长不得被轨迹 A 的高水位抵消"
    );
    let increment = transaction_resource_increments_v1(std::slice::from_ref(observation));
    assert!(increment[0].increments[&ResourceBudgetPathV1::SessionRequestQueue].records > 0);
}

fn latency_samples_v1(samples: Vec<u64>) -> RawLatencySamplesNsV1 {
    RawLatencySamplesNsV1::from_samples(samples).expect("黄金延迟类别必须有真实纳秒样本")
}

fn trajectory_result_v1(
    completed: &GoldenTrajectoryCompletedV1,
    workload_contract_sha256: &Sha256Hex,
) -> TrajectoryTrialResultV1 {
    let trajectory = match completed.trajectory {
        'a' => ComponentGoldenTrajectoryV1::A,
        'b' => ComponentGoldenTrajectoryV1::B,
        other => panic!("未知黄金轨迹 {other}"),
    };
    let logical_components = completed
        .logical_components
        .iter()
        .map(|(name, sha)| {
            let component = match *name {
                "account" => TrajectoryLogicalComponentV1::Account,
                "markets" => TrajectoryLogicalComponentV1::Markets,
                "orders" => TrajectoryLogicalComponentV1::Orders,
                "trades" => TrajectoryLogicalComponentV1::Trades,
                "request_results" => TrajectoryLogicalComponentV1::RequestResults,
                "feedback" => TrajectoryLogicalComponentV1::Feedback,
                "finality" => TrajectoryLogicalComponentV1::Finality,
                "retention" => TrajectoryLogicalComponentV1::Retention,
                other => panic!("未知黄金逻辑成员 {other}"),
            };
            (component, sha.clone())
        })
        .collect();
    let logical_summary_input = TrajectoryLogicalSummaryInputV1 {
        schema_version: crate::measurement::TRAJECTORY_LOGICAL_SUMMARY_SCHEMA_V1.to_owned(),
        workload_contract_sha256: workload_contract_sha256.clone(),
        trajectory,
        logical_component_sha256: logical_components,
    };
    let runtime_binding = TrajectoryRuntimeBindingV1 {
        session_id: completed.session_id.clone(),
        session_seed_sha256: completed.seed.clone(),
        response_bridge_binding_id: completed.bridge_id.clone(),
        account_instance_key: completed.account_key.clone(),
    };
    assert_eq!(
        completed.seed.0,
        match trajectory {
            ComponentGoldenTrajectoryV1::A => "a".repeat(64),
            ComponentGoldenTrajectoryV1::B => "b".repeat(64),
        },
        "报告种子编号必须绑定真实会话种子"
    );
    let throughput = TrajectoryThroughputV1 {
        completed_path_deliveries: completed.path_deliveries,
        completed_top_level_calls: completed.top_level_calls,
        breakdown: completed.throughput_breakdown,
        elapsed_ns: completed.elapsed_ns,
        rates_per_second_milli: ThroughputRatesPerSecondMilliV1::from_counts(
            completed.path_deliveries,
            completed.top_level_calls,
            completed.throughput_breakdown,
            completed.elapsed_ns,
        )
        .expect("逐轨迹分栏吞吐率必须可由真实计数与墙钟重算"),
    };
    let latency = BTreeMap::from([
        (
            MeasurementLatencyKindV1::EventApplication,
            latency_samples_v1(completed.event_latency_ns.clone()),
        ),
        (
            MeasurementLatencyKindV1::RequestResponse,
            latency_samples_v1(completed.request_latency_ns.clone()),
        ),
        (
            MeasurementLatencyKindV1::QueryResponse,
            latency_samples_v1(completed.query_latency_ns.clone()),
        ),
        (
            MeasurementLatencyKindV1::CheckpointOrSeal,
            latency_samples_v1(completed.checkpoint_latency_ns.clone()),
        ),
        (
            MeasurementLatencyKindV1::RetentionRolling,
            latency_samples_v1(completed.retention_latency_ns.clone()),
        ),
        (
            MeasurementLatencyKindV1::TrialEndToEnd,
            latency_samples_v1(vec![completed.elapsed_ns]),
        ),
    ]);
    let cycle = |cycle_id,
                 indexes: [u64; 3],
                 completion_ns,
                 hot: &GoldenLogicalMarketSnapshotV1,
                 compact: &GoldenLogicalMarketSnapshotV1| {
        let compact_quantity = diagnostic_quantity(compact, false);
        LogicalRollingCycleV1 {
            cycle_id,
            hot_snapshot_index: indexes[0],
            compact_snapshot_index: indexes[1],
            expired_snapshot_index: indexes[2],
            logical_completion_ns: completion_ns,
            hot_state_high_water: diagnostic_quantity(hot, true),
            compact_projection_high_water: compact_quantity,
            expired_deleted: compact_quantity,
        }
    };
    let rolling_cycles = BTreeMap::from([
        (
            LogicalRollingCycleIdV1::FirstMarket,
            cycle(
                LogicalRollingCycleIdV1::FirstMarket,
                completed.first_market_sample_indexes,
                completed.first_market_completion_ns,
                &completed.first_market_hot,
                &completed.first_market_compact,
            ),
        ),
        (
            LogicalRollingCycleIdV1::SecondMarket,
            cycle(
                LogicalRollingCycleIdV1::SecondMarket,
                completed.second_market_sample_indexes,
                completed.second_market_completion_ns,
                &completed.second_market_hot,
                &completed.second_market_compact,
            ),
        ),
    ]);
    assert_eq!(
        diagnostic_quantity(&completed.first_market_expired, false).records,
        0
    );
    assert_eq!(
        diagnostic_quantity(&completed.second_market_expired, false).records,
        0
    );
    let logical_summary_sha256 = logical_summary_input
        .content_sha256()
        .expect("推导黄金轨迹逻辑摘要");
    TrajectoryTrialResultV1 {
        runtime_binding,
        logical_summary_input,
        logical_summary_sha256,
        throughput,
        latency,
        rolling_cycles,
    }
}

fn trial_throughput_v1(
    trajectories: &BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
    elapsed_ns: u64,
) -> TrialThroughputV1 {
    let mut breakdown = ThroughputBreakdownV1::default();
    for result in trajectories.values() {
        breakdown = breakdown
            .checked_add(result.throughput.breakdown)
            .expect("两条黄金轨迹吞吐分栏必须可精确相加");
    }
    assert_eq!(
        breakdown.event_kind_path_deliveries,
        ComponentGoldenEventKindCountsV1 {
            initial_l2_snapshot: 16,
            level_execution_transition: 16,
            market_resolution_terminal: 8,
        }
    );
    assert_eq!(breakdown.query_calls, 12);
    assert_eq!(
        breakdown.query_pages, 8,
        "两条轨迹各四个真实分页响应，单项查询不得计页"
    );
    assert_eq!(breakdown.query_return_rows, 426);
    assert_eq!(
        breakdown.query_sources.hot_state.order.calls
            + breakdown.query_sources.hot_state.orders.calls
            + breakdown.query_sources.hot_state.trades.calls,
        12
    );
    assert_eq!(
        breakdown.query_sources.retained_projection,
        ComponentGoldenQueryMethodsV1::default()
    );
    TrialThroughputV1 {
        completed_path_deliveries: 40,
        completed_top_level_calls: 38,
        breakdown,
        elapsed_ns,
        rates_per_second_milli: ThroughputRatesPerSecondMilliV1::from_counts(
            40, 38, breakdown, elapsed_ns,
        )
        .expect("试次分栏吞吐率必须可由真实计数与墙钟重算"),
    }
}

fn aggregate_latency_v1(
    trajectories: &BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
    trial_elapsed_ns: u64,
) -> BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1> {
    MEASUREMENT_LATENCY_KINDS_V1
        .into_iter()
        .map(|kind| {
            let samples = if kind == MeasurementLatencyKindV1::TrialEndToEnd {
                vec![trial_elapsed_ns]
            } else {
                trajectories
                    .values()
                    .flat_map(|result| result.latency[&kind].samples_ns.iter().copied())
                    .collect::<Vec<_>>()
            };
            (kind, latency_samples_v1(samples))
        })
        .collect()
}

fn run_golden_stage_one(
    state: &mut GoldenTrajectoryStateV1,
    input: &GoldenInputV1,
    profile: &EmpiricalDelayProfile,
) -> Vec<String> {
    apply_golden_group(state, input, 0);
    let mut platform_order_ids = Vec::new();
    for (batch_index, batch_size) in [15_usize, 15, 15, 15, 15, 15, 12].into_iter().enumerate() {
        let started = Instant::now();
        let operation = format!("post-orders-{batch_index}");
        let request = PostOrdersRequestV1 {
            context: state.next_context(profile, &operation, 140_000 + batch_index as i64 * 5_000),
            idempotency_key: format!("task44-{}-{operation}-idem", state.trajectory),
            items: (0..batch_size)
                .map(|item_index| {
                    golden_gtc_item(
                        &input.markets[0],
                        format!(
                            "task44-{}-batch-{batch_index}-item-{item_index}",
                            state.trajectory
                        ),
                        Decimal::new(3, 1),
                    )
                })
                .collect(),
        };
        let registration = state
            .session
            .post_orders(request)
            .expect("登记黄金批量挂单");
        state.request_batch_items = state
            .request_batch_items
            .checked_add(u64::try_from(registration.items.len()).expect("批量挂单数适合 u64"))
            .expect("批量请求项目计数");
        let mut effective_times = registration
            .items
            .iter()
            .map(|item| item.effective_processing_at_utc_ms)
            .collect::<Vec<_>>();
        effective_times.sort_unstable();
        drive_registered_request(
            state,
            &effective_times,
            registration.receipt.transit.inbound_delay_ms,
            true,
            input.groups[1].source_at_utc_ms,
            input.groups[1].received_at_utc_ms,
        );
        let response = state
            .captures
            .post_orders
            .lock()
            .expect("读取黄金批量提交响应")
            .last()
            .cloned()
            .expect("黄金批量提交必须返回直接响应");
        assert_eq!(response.items.len(), batch_size);
        platform_order_ids.extend(
            response
                .items
                .iter()
                .map(|item| accepted_order_id(&item.outcome)),
        );
        state.record_call(started, false);
    }
    assert_eq!(platform_order_ids.len(), 102);

    let first_page_started = Instant::now();
    let first_page_context = state.next_context(profile, "orders-first-page", 180_000);
    let first_page_registration = state
        .session
        .orders(OrdersRequestV1 {
            context: first_page_context,
            filter: OrdersFilterV1 {
                condition_id: Some(input.markets[0].market().condition_id.clone()),
                token_id: None,
            },
            cursor: None,
        })
        .expect("登记黄金订单第一页查询");
    drive_registered_request(
        state,
        &[first_page_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        first_page_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[1].source_at_utc_ms,
        input.groups[1].received_at_utc_ms,
    );
    let first_page = state
        .captures
        .orders
        .lock()
        .expect("读取黄金订单第一页")
        .last()
        .cloned()
        .expect("黄金订单第一页必须返回");
    let OrdersQueryResultV1::Page(first_page) = &first_page.result else {
        panic!("黄金订单第一页不得越过保留期")
    };
    state.first_page_count = first_page.items.len();
    assert_eq!(state.first_page_count, 100);
    let cursor = first_page
        .next_cursor
        .clone()
        .expect("102 单必须形成续页游标");
    state.record_call(first_page_started, true);

    let continuation_started = Instant::now();
    let continuation_context = state.next_context(profile, "orders-continuation", 185_000);
    let continuation_registration = state
        .session
        .orders(OrdersRequestV1 {
            context: continuation_context,
            filter: OrdersFilterV1 {
                condition_id: Some(input.markets[0].market().condition_id.clone()),
                token_id: None,
            },
            cursor: Some(cursor),
        })
        .expect("登记黄金订单续页查询");
    drive_registered_request(
        state,
        &[continuation_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        continuation_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[1].source_at_utc_ms,
        input.groups[1].received_at_utc_ms,
    );
    let continuation = state
        .captures
        .orders
        .lock()
        .expect("读取黄金订单续页")
        .last()
        .cloned()
        .expect("黄金订单续页必须返回");
    let OrdersQueryResultV1::Page(continuation) = &continuation.result else {
        panic!("黄金订单续页不得越过保留期")
    };
    state.continuation_page_count = continuation.items.len();
    assert_eq!(state.continuation_page_count, 2);
    assert!(continuation.next_cursor.is_none());
    state.record_call(continuation_started, true);

    let order_started = Instant::now();
    let order_context = state.next_context(profile, "order-before-queue", 190_000);
    let order_registration = state
        .session
        .order(OrderRequestV1 {
            context: order_context,
            platform_order_id: platform_order_ids[0].clone(),
        })
        .expect("登记黄金单项订单查询");
    drive_registered_request(
        state,
        &[order_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        order_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[1].source_at_utc_ms,
        input.groups[1].received_at_utc_ms,
    );
    state.record_call(order_started, true);

    apply_golden_group(state, input, 1);

    let extra_started = Instant::now();
    let extra_context = state.next_context(profile, "post-order-cancel-target", 280_000);
    let extra_registration = state
        .session
        .post_order(PostOrderRequestV1 {
            context: extra_context,
            idempotency_key: format!("task44-{}-extra-gtc-idem", state.trajectory),
            item: golden_gtc_item(
                &input.markets[0],
                format!("task44-{}-extra-gtc", state.trajectory),
                Decimal::new(31, 2),
            ),
        })
        .expect("登记黄金额外可撤挂单");
    drive_registered_request(
        state,
        &[extra_registration.item.effective_processing_at_utc_ms],
        extra_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[2].source_at_utc_ms,
        input.groups[2].received_at_utc_ms,
    );
    let extra_id = accepted_order_id(
        &state
            .captures
            .post_order
            .lock()
            .expect("读取额外挂单响应")
            .last()
            .expect("额外挂单必须返回")
            .item
            .outcome,
    );
    platform_order_ids.push(extra_id);
    state.record_call(extra_started, false);

    let fak_started = Instant::now();
    let fak_context = state.next_context(profile, "post-order-market-one-fak", 290_000);
    let fak_registration = state
        .session
        .post_order(PostOrderRequestV1 {
            context: fak_context,
            idempotency_key: format!("task44-{}-m1-fak-idem", state.trajectory),
            item: golden_fak_item(
                &input.markets[0],
                format!("task44-{}-m1-fak", state.trajectory),
            ),
        })
        .expect("登记黄金第一市场 FAK");
    drive_registered_request(
        state,
        &[fak_registration.item.effective_processing_at_utc_ms],
        fak_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[2].source_at_utc_ms,
        input.groups[2].received_at_utc_ms,
    );
    let first_fak = state
        .captures
        .post_order
        .lock()
        .expect("读取第一市场 FAK 响应")
        .last()
        .cloned()
        .expect("第一市场 FAK 必须返回");
    let SubmissionItemOutcomeV1::Accepted(first_fak) = &first_fak.item.outcome else {
        panic!("第一市场 FAK 必须成功")
    };
    assert_eq!(
        first_fak.fills.len(),
        9,
        "九个真实 ask 段应形成九个成交身份"
    );
    state.record_call(fak_started, false);

    let trades_started = Instant::now();
    let trades_context = state.next_context(profile, "trades-before-queue", 300_000);
    let trades_registration = state
        .session
        .trades(TradesRequestV1 {
            context: trades_context,
            filter: TradesFilterV1 {
                condition_id: Some(input.markets[0].market().condition_id.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("登记队列减量前成交查询");
    drive_registered_request(
        state,
        &[trades_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        trades_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[2].source_at_utc_ms,
        input.groups[2].received_at_utc_ms,
    );
    let trades = state
        .captures
        .trades
        .lock()
        .expect("读取队列减量前成交查询")
        .last()
        .cloned()
        .expect("队列减量前成交查询必须返回");
    let TradesQueryResultV1::Page(trades) = &trades.result else {
        panic!("黄金成交查询不得越过保留期")
    };
    assert_eq!(trades.items.len(), 9);
    state.record_call(trades_started, true);
    platform_order_ids
}

fn advance_all_confirmed_outputs(
    process: &Arc<Mutex<SimExecutionProcessV1>>,
    state: &mut GoldenTrajectoryStateV1,
) {
    let first_condition = state
        .control
        .opening_anchor
        .initial_market_set_sha256
        .0
        .clone();
    let _ = first_condition;
    let mut confirmations = Vec::new();
    for condition_id in state.session.session_spec().market_set_sha256.0.split('\0') {
        let _ = condition_id;
    }
    // 两个市场的诊断共享同一条私有序号链；从任一仍存在的市场读取即可。
    let condition_id = if state
        .session
        .task41_diagnostics_for_tests(&format!("0x{}", "1".repeat(64)))
        .confirmed_direct_response_confirmations
        .is_empty()
        && state
            .session
            .task41_diagnostics_for_tests(&format!("0x{}", "1".repeat(64)))
            .confirmed_lifecycle_feedback_confirmations
            .is_empty()
    {
        format!("0x{}", "2".repeat(64))
    } else {
        format!("0x{}", "1".repeat(64))
    };
    let diagnostic = state.session.task41_diagnostics_for_tests(&condition_id);
    confirmations.extend(diagnostic.confirmed_direct_response_confirmations);
    confirmations.extend(diagnostic.confirmed_lifecycle_feedback_confirmations);
    confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    confirmations.dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    let already_durable_through = state.downstream_durable_through;
    for durable_through in confirmations
        .into_iter()
        .filter(|confirmation| confirmation.private_feedback_sequence > already_durable_through)
    {
        let sequence = durable_through.private_feedback_sequence;
        let started = Instant::now();
        process
            .lock()
            .expect("黄金运行根锁")
            .advance_downstream_durability(
                &mut state.session,
                DownstreamDurabilityProofV1 {
                    account_instance_key: state.control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("推进黄金强下游耐久水位");
        state.checkpoint_latency_ns.push(elapsed_ns(started));
        state.downstream_durable_through = sequence;
    }
}

fn run_golden_stage_two(
    state: &mut GoldenTrajectoryStateV1,
    input: &GoldenInputV1,
    profile: &EmpiricalDelayProfile,
    process: &Arc<Mutex<SimExecutionProcessV1>>,
    sampler: &Arc<Mutex<GoldenResourceSamplerV1>>,
    platform_order_ids: &[String],
) {
    apply_golden_group(state, input, 2);
    apply_golden_group(state, input, 3);
    let queued = state
        .session
        .task41_diagnostics_for_tests(&input.markets[0].market().condition_id.0);
    assert!(queued.pending_lifecycle_feedback_count > 0);
    assert!(queued.pending_lifecycle_feedback_count < 4_096);
    state.feedback_was_bounded = true;
    let request_latency_before_deferred = state.request_latency_ns.len();
    let query_latency_before_deferred = state.query_latency_ns.len();

    let cancel_started = Instant::now();
    let cancel_context = state.next_context(profile, "cancel-order", 350_000);
    let cancel_request_id = cancel_context.global_request_id.clone();
    let cancel_registration = state
        .session
        .cancel_order(CancelOrderRequestV1 {
            context: cancel_context,
            idempotency_key: format!("task44-{}-cancel-order-idem", state.trajectory),
            item: CancelOrderItemV1 {
                item_id: format!("task44-{}-cancel-order-item", state.trajectory),
                platform_order_id: platform_order_ids[102].clone(),
            },
        })
        .expect("登记黄金单项撤单");
    drive_registered_request(
        state,
        &[cancel_registration.item.effective_processing_at_utc_ms],
        cancel_registration.receipt.transit.inbound_delay_ms,
        false,
        input.groups[4].source_at_utc_ms,
        input.groups[4].received_at_utc_ms,
    );
    state.defer_call(cancel_request_id, cancel_started, false);

    let cancel_batch_started = Instant::now();
    let cancel_batch_context = state.next_context(profile, "cancel-orders", 355_000);
    let cancel_batch_request_id = cancel_batch_context.global_request_id.clone();
    let cancel_batch_registration = state
        .session
        .cancel_orders(CancelOrdersRequestV1 {
            context: cancel_batch_context,
            idempotency_key: format!("task44-{}-cancel-orders-idem", state.trajectory),
            items: [0_usize, 1, 2, 0]
                .into_iter()
                .enumerate()
                .map(|(input_ordinal, target)| CancelOrderItemV1 {
                    item_id: format!(
                        "task44-{}-cancel-batch-item-{input_ordinal}",
                        state.trajectory
                    ),
                    platform_order_id: platform_order_ids[target].clone(),
                })
                .collect(),
        })
        .expect("登记黄金批量撤单");
    state.request_batch_items = state
        .request_batch_items
        .checked_add(
            u64::try_from(cancel_batch_registration.items.len()).expect("批量撤单数适合 u64"),
        )
        .expect("批量请求项目计数");
    let mut effective_times = cancel_batch_registration
        .items
        .iter()
        .filter(|item| {
            cancel_batch_registration
                .items
                .iter()
                .position(|candidate| {
                    candidate.scheduled_action_ordinal == item.scheduled_action_ordinal
                })
                == cancel_batch_registration
                    .items
                    .iter()
                    .position(|candidate| candidate == *item)
        })
        .map(|item| item.effective_processing_at_utc_ms)
        .collect::<Vec<_>>();
    effective_times.sort_unstable();
    assert_eq!(
        effective_times.len(),
        3,
        "四个输入必须折叠为三个唯一撤单动作"
    );
    drive_registered_request(
        state,
        &effective_times[..1],
        cancel_batch_registration.receipt.transit.inbound_delay_ms,
        false,
        input.groups[4].source_at_utc_ms,
        input.groups[4].received_at_utc_ms,
    );
    state.defer_call(cancel_batch_request_id, cancel_batch_started, false);

    let cancel_all_started = Instant::now();
    let cancel_all_context = state.next_context(profile, "cancel-all-orders", 360_000);
    let cancel_all_request_id = cancel_all_context.global_request_id.clone();
    let cancel_all_registration = state
        .session
        .cancel_all_orders(CancelAllOrdersRequestV1 {
            context: cancel_all_context,
            idempotency_key: format!("task44-{}-cancel-all-idem", state.trajectory),
        })
        .expect("登记黄金全撤");
    drive_registered_request(
        state,
        &[cancel_all_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        cancel_all_registration.receipt.transit.inbound_delay_ms,
        false,
        input.groups[4].source_at_utc_ms,
        input.groups[4].received_at_utc_ms,
    );
    state.defer_call(cancel_all_request_id, cancel_all_started, false);

    let order_started = Instant::now();
    let order_context = state.next_context(profile, "order-after-queue", 365_000);
    let order_request_id = order_context.global_request_id.clone();
    let order_registration = state
        .session
        .order(OrderRequestV1 {
            context: order_context,
            platform_order_id: platform_order_ids[0].clone(),
        })
        .expect("登记队列减量后单项订单查询");
    drive_registered_request(
        state,
        &[order_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        order_registration.receipt.transit.inbound_delay_ms,
        false,
        input.groups[4].source_at_utc_ms,
        input.groups[4].received_at_utc_ms,
    );
    state.defer_call(order_request_id, order_started, true);

    let trades_started = Instant::now();
    let trades_context = state.next_context(profile, "trades-after-queue", 370_000);
    let trades_request_id = trades_context.global_request_id.clone();
    let trades_registration = state
        .session
        .trades(TradesRequestV1 {
            context: trades_context,
            filter: TradesFilterV1 {
                condition_id: Some(input.markets[0].market().condition_id.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("登记队列减量后成交查询");
    drive_registered_request(
        state,
        &[trades_registration
            .receipt
            .transit
            .platform_arrived_at_utc_ms],
        trades_registration.receipt.transit.inbound_delay_ms,
        false,
        input.groups[4].source_at_utc_ms,
        input.groups[4].received_at_utc_ms,
    );
    state.defer_call(trades_request_id, trades_started, true);
    assert_eq!(
        state.request_latency_ns.len(),
        request_latency_before_deferred,
        "延后交付的三个撤单请求不得在桥回调前停表"
    );
    assert_eq!(
        state.query_latency_ns.len(),
        query_latency_before_deferred,
        "延后交付的两个查询请求不得在桥回调前停表"
    );
    assert_eq!(state.deferred_call_latency.len(), 5);
    close_empty_prefix_through(state, true, 380_000, input.groups[4].source_at_utc_ms);
    let started = Instant::now();
    assert_eq!(
        state.session.drive_next_execution_action_through(380_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    close_empty_prefix_through(state, false, 380_000, input.groups[4].received_at_utc_ms);
    drain_private_through(state, 380_000);
    assert!(state.deferred_call_latency.is_empty());
    assert_eq!(
        state.request_latency_ns.len(),
        request_latency_before_deferred + 3
    );
    assert_eq!(
        state.query_latency_ns.len(),
        query_latency_before_deferred + 2
    );

    apply_golden_group(state, input, 4);
    close_empty_prefix_through(state, true, 430_000, input.groups[5].source_at_utc_ms);
    let started = Instant::now();
    assert_eq!(
        state.session.drive_next_execution_action_through(430_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    close_empty_prefix_through(state, false, 430_000, input.groups[5].received_at_utc_ms);
    drain_private_through(state, 430_000);
    let condition_id = &input.markets[0].market().condition_id.0;
    let hot = state.session.task41_diagnostics_for_tests(condition_id);
    assert_eq!(
        hot.hot_orders.len(),
        104,
        "102 挂单、额外挂单与 FAK 均须进入真实订单状态"
    );
    assert_eq!(
        hot.hot_trades.len(),
        111,
        "102 个 maker 成交加九个 taker 成交"
    );
    assert_eq!(
        hot.compact_orders.len(),
        104,
        "驱逐前必须已形成精确终态订单投影"
    );
    assert_eq!(
        hot.compact_trades.len(),
        111,
        "驱逐前必须已形成精确终态成交投影"
    );
    state.unique_fills += u64::try_from(hot.hot_trades.len()).expect("第一市场成交数适合 u64");
    state.affected_orders += u64::try_from(hot.hot_orders.len()).expect("第一市场订单数适合 u64");
    state.first_market_hot = Some(logical_market_snapshot_v1(&state.session, condition_id));
    let hot_sample_index = capture_golden_resource_snapshot(process, sampler);
    let account = state.session.account_snapshot_for_tests();
    assert_eq!(account.principal_encumbrance_pusd.0, "0");
    assert!(
        account
            .outcome_balances
            .iter()
            .filter(|balance| balance.token_id == input.markets[0].assets()[0].token_id)
            .all(|balance| balance.balance_shares.0 == "0"),
        "迟到判定后第一市场胜出代币必须自动赎回"
    );

    advance_all_confirmed_outputs(process, state);
    let started = Instant::now();
    process
        .lock()
        .expect("黄金运行根锁")
        .evict_terminal_market(&mut state.session, condition_id)
        .expect("驱逐第一市场热状态");
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    let compact = state.session.task41_diagnostics_for_tests(condition_id);
    assert!(compact.hot_orders.is_empty() && compact.hot_trades.is_empty());
    assert_eq!(compact.compact_orders.len(), 104);
    assert_eq!(compact.compact_trades.len(), 111);
    state.first_market_compact = Some(logical_market_snapshot_v1(&state.session, condition_id));
    let compact_sample_index = capture_golden_resource_snapshot(process, sampler);
    let retention_started = Instant::now();
    state
        .session
        .advance_order_history_retention_floor(450_000)
        .expect("推进第一市场订单保留水位");
    state
        .session
        .advance_trade_history_retention_floor(450_000)
        .expect("推进第一市场成交保留水位");
    state
        .retention_latency_ns
        .push(elapsed_ns(retention_started));
    let expired = state.session.task41_diagnostics_for_tests(condition_id);
    assert!(expired.compact_orders.is_empty() && expired.compact_trades.is_empty());
    state.first_market_expired = Some(logical_market_snapshot_v1(&state.session, condition_id));
    let expired_sample_index = capture_golden_resource_snapshot(process, sampler);
    state.first_market_sample_indexes =
        Some([hot_sample_index, compact_sample_index, expired_sample_index]);
    state.first_market_completion_ns = Some(state.elapsed_from_start());
}

fn run_golden_stage_three(
    mut state: GoldenTrajectoryStateV1,
    input: &GoldenInputV1,
    profile: &EmpiricalDelayProfile,
    process: &Arc<Mutex<SimExecutionProcessV1>>,
    sampler: &Arc<Mutex<GoldenResourceSamplerV1>>,
) -> GoldenTrajectoryCompletedV1 {
    let before_second = state.session.account_snapshot_for_tests();
    let fak_started = Instant::now();
    let fak_context = state.next_context(profile, "post-order-market-two-fak", 450_000);
    let fak_registration = state
        .session
        .post_order(PostOrderRequestV1 {
            context: fak_context,
            idempotency_key: format!("task44-{}-m2-fak-idem", state.trajectory),
            item: golden_fak_item(
                &input.markets[1],
                format!("task44-{}-m2-fak", state.trajectory),
            ),
        })
        .expect("第一市场赎回后登记第二市场 FAK");
    drive_registered_request(
        &mut state,
        &[fak_registration.item.effective_processing_at_utc_ms],
        fak_registration.receipt.transit.inbound_delay_ms,
        true,
        input.groups[5].source_at_utc_ms,
        input.groups[5].received_at_utc_ms,
    );
    let second_fak = state
        .captures
        .post_order
        .lock()
        .expect("读取第二市场 FAK 响应")
        .last()
        .cloned()
        .expect("第二市场 FAK 必须返回");
    let SubmissionItemOutcomeV1::Accepted(second_fak) = &second_fak.item.outcome else {
        panic!("第一市场释放和赎回后第二市场请求必须成功")
    };
    assert_eq!(second_fak.fills.len(), 9);
    state.first_market_redeemed_before_second_request = before_second
        .outcome_balances
        .iter()
        .filter(|balance| {
            input.markets[0]
                .assets()
                .iter()
                .any(|asset| asset.token_id == balance.token_id)
        })
        .all(|balance| balance.balance_shares.0 == "0")
        && before_second.principal_encumbrance_pusd.0 == "0";
    assert!(state.first_market_redeemed_before_second_request);
    state.record_call(fak_started, false);
    assert_eq!(state.top_level_calls, 19);

    apply_golden_group(&mut state, input, 5);
    close_empty_prefix_through(&mut state, true, 510_000, input.groups[6].source_at_utc_ms);
    let started = Instant::now();
    assert_eq!(
        state.session.drive_next_execution_action_through(510_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    apply_golden_group(&mut state, input, 6);
    apply_golden_group(&mut state, input, 7);
    let started = Instant::now();
    assert_eq!(
        state.session.drive_next_execution_action_through(580_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    drain_private_through(&mut state, 580_000);
    assert_eq!(state.path_deliveries, 20);
    let condition_id = &input.markets[1].market().condition_id.0;
    let hot = state.session.task41_diagnostics_for_tests(condition_id);
    assert_eq!(hot.hot_orders.len(), 1);
    assert_eq!(hot.hot_trades.len(), 9);
    state.unique_fills += u64::try_from(hot.hot_trades.len()).expect("第二市场成交数适合 u64");
    state.affected_orders += u64::try_from(hot.hot_orders.len()).expect("第二市场订单数适合 u64");
    state.second_market_hot = Some(logical_market_snapshot_v1(&state.session, condition_id));
    let hot_sample_index = capture_golden_resource_snapshot(process, sampler);
    advance_all_confirmed_outputs(process, &mut state);
    let started = Instant::now();
    process
        .lock()
        .expect("黄金运行根锁")
        .evict_terminal_market(&mut state.session, condition_id)
        .expect("驱逐第二市场热状态");
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    let compact = state.session.task41_diagnostics_for_tests(condition_id);
    assert!(compact.hot_orders.is_empty() && compact.hot_trades.is_empty());
    assert_eq!(compact.compact_orders.len(), 1);
    assert_eq!(compact.compact_trades.len(), 9);
    state.second_market_compact = Some(logical_market_snapshot_v1(&state.session, condition_id));
    let compact_sample_index = capture_golden_resource_snapshot(process, sampler);
    let retention_started = Instant::now();
    state
        .session
        .advance_order_history_retention_floor(600_000)
        .expect("推进第二市场订单保留水位");
    state
        .session
        .advance_trade_history_retention_floor(600_000)
        .expect("推进第二市场成交保留水位");
    state
        .retention_latency_ns
        .push(elapsed_ns(retention_started));
    let expired = state.session.task41_diagnostics_for_tests(condition_id);
    assert!(expired.compact_orders.is_empty() && expired.compact_trades.is_empty());
    state.second_market_expired = Some(logical_market_snapshot_v1(&state.session, condition_id));
    let expired_sample_index = capture_golden_resource_snapshot(process, sampler);
    state.second_market_sample_indexes =
        Some([hot_sample_index, compact_sample_index, expired_sample_index]);
    state.second_market_completion_ns = Some(state.elapsed_from_start());

    while let Some(clock_at) = state.session.next_rebate_clock_at_utc_ms_for_tests() {
        let started = Instant::now();
        assert_eq!(
            state.session.drive_next_execution_action_through(clock_at),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        state.checkpoint_latency_ns.push(elapsed_ns(started));
        let delivery_through = clock_at
            + i64::try_from(profile.statistics().max_delay_ms).expect("黄金时延上界适合 i64");
        let started = Instant::now();
        assert_eq!(
            state
                .session
                .drive_next_execution_action_through(delivery_through),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        state.checkpoint_latency_ns.push(elapsed_ns(started));
        drain_private_through(&mut state, delivery_through);
    }
    advance_all_confirmed_outputs(process, &mut state);
    assert!(
        state.deferred_call_latency.is_empty(),
        "所有延后结算的直接响应必须已在真实桥交付后完成计时"
    );
    assert_eq!(state.request_latency_ns.len(), 13);
    assert_eq!(state.query_latency_ns.len(), 6);

    let account = state.session.account_snapshot_for_tests();
    let first_market =
        logical_market_snapshot_v1(&state.session, &input.markets[0].market().condition_id.0);
    let second_market = logical_market_snapshot_v1(&state.session, condition_id);
    let outputs = state
        .captures
        .logical_outputs
        .lock()
        .expect("读取黄金桥输出")
        .clone();
    assert_eq!(
        state
            .captures
            .post_orders
            .lock()
            .expect("批量提交响应锁")
            .len(),
        7
    );
    assert_eq!(
        state
            .captures
            .post_order
            .lock()
            .expect("单项提交响应锁")
            .len(),
        3
    );
    assert_eq!(
        state
            .captures
            .cancel_order
            .lock()
            .expect("单撤响应锁")
            .len(),
        1
    );
    assert_eq!(
        state
            .captures
            .cancel_orders
            .lock()
            .expect("批撤响应锁")
            .len(),
        1
    );
    assert_eq!(
        state.captures.cancel_all.lock().expect("全撤响应锁").len(),
        1
    );
    assert_eq!(state.captures.order.lock().expect("订单响应锁").len(), 2);
    assert_eq!(state.captures.orders.lock().expect("订单页响应锁").len(), 2);
    assert_eq!(state.captures.trades.lock().expect("成交响应锁").len(), 2);
    assert_eq!(state.first_page_count, 100);
    assert_eq!(state.continuation_page_count, 2);
    assert!(state.feedback_was_bounded);

    let order_queries = state.captures.order.lock().expect("订单响应锁").clone();
    let orders_queries = state.captures.orders.lock().expect("订单页响应锁").clone();
    let trades_queries = state.captures.trades.lock().expect("成交响应锁").clone();
    let [first_trades, second_trades] = trades_queries.as_slice() else {
        panic!("黄金轨迹必须形成两次成交分页查询")
    };
    let (TradesQueryResultV1::Page(first_trades), TradesQueryResultV1::Page(second_trades)) =
        (&first_trades.result, &second_trades.result)
    else {
        panic!("黄金成交查询不得越过保留期")
    };
    assert_eq!(first_trades.items.len(), 9);
    assert_eq!(second_trades.items.len(), 100);
    assert!(
        second_trades.next_cursor.is_some(),
        "第二次成交查询必须如实保留未读取续页，不能把首个100行误当完整历史"
    );
    let mut query_pages = 0_u64;
    let mut query_return_rows = 0_u64;
    for response in &order_queries {
        if matches!(response.result, OrderQueryResultV1::Found(_)) {
            query_return_rows += 1;
        }
    }
    for response in &orders_queries {
        if let OrdersQueryResultV1::Page(page) = &response.result {
            query_pages += 1;
            query_return_rows += u64::try_from(page.items.len()).expect("订单页行数适合 u64");
        }
    }
    for response in &trades_queries {
        if let TradesQueryResultV1::Page(page) = &response.result {
            query_pages += 1;
            query_return_rows += u64::try_from(page.items.len()).expect("成交页行数适合 u64");
        }
    }
    let output_count = |kind: &str| {
        u64::try_from(
            outputs
                .iter()
                .filter(|output| output.kind() == kind)
                .count(),
        )
        .expect("反馈计数适合 u64")
    };
    let query_sources =
        golden_query_source_breakdown_v1(state.session.query_source_diagnostics_for_tests());
    assert_eq!(
        state.event_kind_path_deliveries,
        ComponentGoldenEventKindCountsV1 {
            initial_l2_snapshot: 8,
            level_execution_transition: 8,
            market_resolution_terminal: 4,
        },
        "黄金事件种类必须从两条真实投递路径累计"
    );
    let throughput_breakdown = ThroughputBreakdownV1 {
        event_kind_path_deliveries: state.event_kind_path_deliveries,
        request_batch_items: state.request_batch_items,
        unique_fills: state.unique_fills,
        affected_orders: state.affected_orders,
        order_feedback: output_count("ORDER_LIFECYCLE"),
        trade_feedback: output_count("TRADE_LIFECYCLE"),
        settlement_feedback: output_count("SETTLEMENT_LIFECYCLE"),
        query_calls: u64::try_from(
            order_queries.len() + orders_queries.len() + trades_queries.len(),
        )
        .expect("查询调用数适合 u64"),
        query_pages,
        query_return_rows,
        query_sources,
    };
    assert_eq!(throughput_breakdown.request_batch_items, 106);
    assert_eq!(throughput_breakdown.unique_fills, 120);
    assert_eq!(throughput_breakdown.affected_orders, 105);
    assert_eq!(throughput_breakdown.query_calls, 6);
    assert_eq!(
        throughput_breakdown.query_pages, 4,
        "单项订单查询命中只计返回行，不得冒充分页"
    );
    assert_eq!(throughput_breakdown.query_return_rows, 213);

    let order_outputs = outputs
        .iter()
        .filter(|output| output.is_order_component())
        .cloned()
        .collect::<Vec<_>>();
    let trade_outputs = outputs
        .iter()
        .filter(|output| output.is_trade_component())
        .cloned()
        .collect::<Vec<_>>();
    let feedback_outputs = outputs
        .iter()
        .filter(|output| output.is_feedback())
        .cloned()
        .collect::<Vec<_>>();
    let finality_outputs = outputs
        .iter()
        .filter(|output| output.is_finality())
        .cloned()
        .collect::<Vec<_>>();
    let semantic_rules_sha256 = &state.semantic_rules_sha256;
    let logical_components = BTreeMap::from([
        (
            "account",
            sha256_serializable("TASK44_ACCOUNT_V1", &(semantic_rules_sha256, &account)),
        ),
        (
            "markets",
            sha256_serializable(
                "TASK44_MARKETS_V1",
                &(semantic_rules_sha256, first_market, second_market),
            ),
        ),
        (
            "orders",
            sha256_serializable("TASK44_ORDERS_V1", &(semantic_rules_sha256, &order_outputs)),
        ),
        (
            "trades",
            sha256_serializable("TASK44_TRADES_V1", &(semantic_rules_sha256, &trade_outputs)),
        ),
        (
            "request_results",
            sha256_serializable(
                "TASK44_REQUEST_RESULTS_V1",
                &(semantic_rules_sha256, &outputs),
            ),
        ),
        (
            "feedback",
            sha256_serializable(
                "TASK44_FEEDBACK_V1",
                &(semantic_rules_sha256, &feedback_outputs),
            ),
        ),
        (
            "finality",
            sha256_serializable(
                "TASK44_FINALITY_V1",
                &(semantic_rules_sha256, &finality_outputs),
            ),
        ),
        (
            "retention",
            sha256_serializable(
                "TASK44_RETENTION_V1",
                &(
                    semantic_rules_sha256,
                    &state.first_market_hot,
                    &state.first_market_compact,
                    &state.first_market_expired,
                    &state.second_market_hot,
                    &state.second_market_compact,
                    &state.second_market_expired,
                ),
            ),
        ),
    ]);
    let session_id = state.session.session_spec().session_id.clone();
    let bridge_id = state
        .session
        .session_spec()
        .response_bridge_binding_id
        .clone();
    let account_key = state.control.account_instance_key.clone();
    let seed = state.session.session_spec().session_seed.clone();
    let trajectory_started = state
        .trajectory_started
        .expect("黄金轨迹必须在 clean completion 前开始计时");
    let completion_started_ns = elapsed_ns(trajectory_started);
    let completion_started = Instant::now();
    match process
        .lock()
        .expect("黄金运行根锁")
        .complete_clean_run_session(state.session)
    {
        Ok(()) => {}
        Err(SessionRetirementFailureV1::Rejected { error, .. }) => {
            panic!("黄金 clean run 完成被拒：{error}")
        }
        Err(SessionRetirementFailureV1::RecoveryRequired { error }) => {
            panic!("黄金 clean run 完成要求恢复：{error}")
        }
    }
    let completion_latency_ns = elapsed_ns(completion_started);
    let elapsed = elapsed_ns(trajectory_started);
    assert!(
        trajectory_elapsed_covers_completion_v1(
            elapsed,
            completion_started_ns,
            completion_latency_ns,
        ),
        "逐轨迹端到端墙钟必须覆盖最终 clean completion 耐久事务"
    );
    let mut checkpoint_latency_ns = state.checkpoint_latency_ns;
    checkpoint_latency_ns.push(completion_latency_ns);
    GoldenTrajectoryCompletedV1 {
        trajectory: state.trajectory,
        session_id,
        bridge_id,
        account_key,
        seed,
        logical_components,
        path_deliveries: state.path_deliveries,
        top_level_calls: state.top_level_calls,
        throughput_breakdown,
        event_latency_ns: state.event_latency_ns,
        request_latency_ns: state.request_latency_ns,
        query_latency_ns: state.query_latency_ns,
        checkpoint_latency_ns,
        retention_latency_ns: state.retention_latency_ns,
        elapsed_ns: elapsed,
        first_market_hot: state.first_market_hot.expect("第一市场热态快照"),
        first_market_compact: state.first_market_compact.expect("第一市场紧凑快照"),
        first_market_expired: state.first_market_expired.expect("第一市场过期快照"),
        second_market_hot: state.second_market_hot.expect("第二市场热态快照"),
        second_market_compact: state.second_market_compact.expect("第二市场紧凑快照"),
        second_market_expired: state.second_market_expired.expect("第二市场过期快照"),
        first_market_sample_indexes: state
            .first_market_sample_indexes
            .expect("第一市场资源样本索引"),
        second_market_sample_indexes: state
            .second_market_sample_indexes
            .expect("第二市场资源样本索引"),
        first_market_completion_ns: state
            .first_market_completion_ns
            .expect("第一市场滚动完成时长"),
        second_market_completion_ns: state
            .second_market_completion_ns
            .expect("第二市场滚动完成时长"),
    }
}

struct GoldenTrajectoryCompletedV1 {
    trajectory: char,
    session_id: String,
    bridge_id: String,
    account_key: AccountInstanceKeyV1,
    seed: Sha256Hex,
    logical_components: BTreeMap<&'static str, Sha256Hex>,
    path_deliveries: u16,
    top_level_calls: u16,
    throughput_breakdown: ThroughputBreakdownV1,
    event_latency_ns: Vec<u64>,
    request_latency_ns: Vec<u64>,
    query_latency_ns: Vec<u64>,
    checkpoint_latency_ns: Vec<u64>,
    retention_latency_ns: Vec<u64>,
    elapsed_ns: u64,
    first_market_hot: GoldenLogicalMarketSnapshotV1,
    first_market_compact: GoldenLogicalMarketSnapshotV1,
    first_market_expired: GoldenLogicalMarketSnapshotV1,
    second_market_hot: GoldenLogicalMarketSnapshotV1,
    second_market_compact: GoldenLogicalMarketSnapshotV1,
    second_market_expired: GoldenLogicalMarketSnapshotV1,
    first_market_sample_indexes: [u64; 3],
    second_market_sample_indexes: [u64; 3],
    first_market_completion_ns: u64,
    second_market_completion_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FormalTrialSelectorV1 {
    Component(ComponentGoldenTrialIdV1),
    Verification(FrozenBudgetVerificationTrialIdV1),
}

impl FormalTrialSelectorV1 {
    fn from_environment() -> Self {
        match std::env::var("SIM_COMPONENT_GOLDEN_TRIAL_ID")
            .expect("正式单试次缺少 SIM_COMPONENT_GOLDEN_TRIAL_ID")
            .as_str()
        {
            "S1" => Self::Component(ComponentGoldenTrialIdV1::S1),
            "P1" => Self::Component(ComponentGoldenTrialIdV1::P1),
            "P2" => Self::Component(ComponentGoldenTrialIdV1::P2),
            "S2" => Self::Component(ComponentGoldenTrialIdV1::S2),
            "S3" => Self::Component(ComponentGoldenTrialIdV1::S3),
            "P3" => Self::Component(ComponentGoldenTrialIdV1::P3),
            "VERIFY_SERIAL" => Self::Verification(FrozenBudgetVerificationTrialIdV1::Serial),
            "VERIFY_PARALLEL" => Self::Verification(FrozenBudgetVerificationTrialIdV1::Parallel),
            other => panic!("未知正式单试次身份 {other}"),
        }
    }

    const fn mode(self) -> ComponentGoldenMeasurementModeV1 {
        match self {
            Self::Component(id) => id.mode(),
            Self::Verification(id) => id.mode(),
        }
    }

    const fn measurement_identity(self) -> MeasurementRunIdentityV1 {
        match self {
            Self::Component(id) => MeasurementRunIdentityV1::ComponentTrial(id),
            Self::Verification(id) => MeasurementRunIdentityV1::FrozenBudgetVerification(id),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Component(ComponentGoldenTrialIdV1::S1) => "S1",
            Self::Component(ComponentGoldenTrialIdV1::P1) => "P1",
            Self::Component(ComponentGoldenTrialIdV1::P2) => "P2",
            Self::Component(ComponentGoldenTrialIdV1::S2) => "S2",
            Self::Component(ComponentGoldenTrialIdV1::S3) => "S3",
            Self::Component(ComponentGoldenTrialIdV1::P3) => "P3",
            Self::Verification(FrozenBudgetVerificationTrialIdV1::Serial) => "VERIFY_SERIAL",
            Self::Verification(FrozenBudgetVerificationTrialIdV1::Parallel) => "VERIFY_PARALLEL",
        }
    }

    fn is_valid_for_build_group(self, build_group: &str) -> bool {
        matches!(
            (build_group, self),
            ("B1", Self::Component(_)) | ("B2", Self::Verification(_))
        )
    }
}

#[test]
fn task441_formal_selector_is_strictly_partitioned_between_b1_and_b2() {
    assert!(
        FormalTrialSelectorV1::Component(ComponentGoldenTrialIdV1::S1)
            .is_valid_for_build_group("B1")
    );
    assert!(
        FormalTrialSelectorV1::Verification(FrozenBudgetVerificationTrialIdV1::Serial)
            .is_valid_for_build_group("B2")
    );
    assert!(
        !FormalTrialSelectorV1::Verification(FrozenBudgetVerificationTrialIdV1::Parallel)
            .is_valid_for_build_group("B1")
    );
    assert!(
        !FormalTrialSelectorV1::Component(ComponentGoldenTrialIdV1::P3)
            .is_valid_for_build_group("B2")
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "trial_kind",
    content = "report",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
enum PersistedFormalTrialReportV1 {
    Component(ComponentGoldenTrialReportV1),
    Verification(FrozenBudgetVerificationTrialV1),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedFormalTrialV1 {
    schema_version: String,
    run_id: String,
    runtime_root_relative_path: String,
    artifact_directory_relative_path: String,
    trial: PersistedFormalTrialReportV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FormalLifecycleReadyV1 {
    schema_version: String,
    token: String,
    selector_label: String,
    run_id: String,
    pid: u32,
    main_trial_json_sha256: Sha256Hex,
    main_trial_json_bytes: u64,
}

fn formal_lifecycle_handshake_root_v1() -> PathBuf {
    let root = PathBuf::from(
        std::env::var("SIM_COMPONENT_GOLDEN_LIFECYCLE_HANDSHAKE_ROOT")
            .expect("正式单试次必须由生命周期外层编排并提供握手根"),
    );
    assert!(root.is_absolute(), "生命周期握手根必须为绝对路径");
    assert!(root.is_dir(), "生命周期握手根必须由外层预先创建");
    fs::canonicalize(root).expect("规范化生命周期握手根")
}

fn formal_lifecycle_token_v1() -> String {
    let token = std::env::var("SIM_COMPONENT_GOLDEN_LIFECYCLE_TOKEN")
        .expect("正式单试次必须由生命周期外层提供唯一握手令牌");
    assert!(
        token.len() == 64
            && token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "生命周期握手令牌必须为 64 位小写十六进制"
    );
    token
}

fn publish_lifecycle_ready_and_wait_for_ack_v1(
    selector: FormalTrialSelectorV1,
    run_id: &str,
    trial_path: &Path,
    trial_sha256: Sha256Hex,
) -> ! {
    let handshake_root = formal_lifecycle_handshake_root_v1();
    let token = formal_lifecycle_token_v1();
    let ready_path = handshake_root.join("ready.json");
    let ack_path = handshake_root.join("ack.txt");
    assert!(!ready_path.exists() && !ack_path.exists());
    let ready = FormalLifecycleReadyV1 {
        schema_version: FORMAL_LIFECYCLE_HANDSHAKE_SCHEMA_V1.to_owned(),
        token: token.clone(),
        selector_label: selector.label().to_owned(),
        run_id: run_id.to_owned(),
        pid: std::process::id(),
        main_trial_json_sha256: trial_sha256,
        main_trial_json_bytes: fs::metadata(trial_path)
            .expect("读取主试次 JSON 元数据")
            .len(),
    };
    atomic_write_json_with_sha256(&ready_path, &ready);
    let started = Instant::now();
    loop {
        if ack_path.is_file() {
            let mut ack = [0_u8; 65];
            File::open(&ack_path)
                .expect("打开生命周期放行确认")
                .read_exact(&mut ack)
                .expect("读取固定长度生命周期放行确认");
            assert_eq!(&ack[..64], token.as_bytes());
            assert_eq!(ack[64], b'\n');
            std::process::exit(0);
        }
        assert!(
            started.elapsed() < Duration::from_secs(5 * 60),
            "生命周期外层未在五分钟内采集尾部证据并放行"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn parse_systemd_show_v1(unit: &str) -> BTreeMap<String, String> {
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            unit,
            "--property=ActiveState,SubState,Result,ExecMainStatus,MainPID,InvocationID,ControlGroup,MemoryPeak,MemorySwapPeak",
        ])
        .output()
        .expect("读取正式试次 systemd 属性");
    assert!(
        output.status.success(),
        "读取正式试次 systemd 属性失败：{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("systemd 属性必须为 UTF-8")
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn required_systemd_property_v1<'a>(values: &'a BTreeMap<String, String>, key: &str) -> &'a str {
    values
        .get(key)
        .filter(|value| !value.is_empty() && value.as_str() != "[not set]")
        .unwrap_or_else(|| panic!("systemd 属性 {key} 缺失或未设置"))
}

fn systemd_u64_v1(values: &BTreeMap<String, String>, key: &str) -> u64 {
    required_systemd_property_v1(values, key)
        .parse()
        .unwrap_or_else(|error| panic!("解析 systemd {key} 失败：{error}"))
}

fn finalized_systemd_lifecycle_result_v1(
    expected_invocation_id: &str,
    values: &BTreeMap<String, String>,
) -> SystemdServiceLifecycleResultV1 {
    assert_eq!(required_systemd_property_v1(values, "MainPID"), "0");
    assert_eq!(
        required_systemd_property_v1(values, "InvocationID"),
        expected_invocation_id,
        "systemd 终态必须延续存活时 InvocationID"
    );
    SystemdServiceLifecycleResultV1 {
        result: required_systemd_property_v1(values, "Result").to_owned(),
        exec_main_status: required_systemd_property_v1(values, "ExecMainStatus")
            .parse()
            .expect("解析 systemd ExecMainStatus"),
        memory_peak_bytes: systemd_u64_v1(values, "MemoryPeak"),
        memory_swap_peak_bytes: systemd_u64_v1(values, "MemorySwapPeak"),
    }
}

#[test]
fn task441_systemd_remain_after_exit_keeps_terminal_identity_without_control_group() {
    let invocation_id = "1".repeat(32);
    let values = BTreeMap::from([
        ("MainPID".to_owned(), "0".to_owned()),
        ("InvocationID".to_owned(), invocation_id.clone()),
        ("ControlGroup".to_owned(), String::new()),
        ("Result".to_owned(), "success".to_owned()),
        ("ExecMainStatus".to_owned(), "0".to_owned()),
        ("MemoryPeak".to_owned(), "331120640".to_owned()),
        ("MemorySwapPeak".to_owned(), "0".to_owned()),
    ]);
    assert_eq!(
        finalized_systemd_lifecycle_result_v1(&invocation_id, &values),
        SystemdServiceLifecycleResultV1 {
            result: "success".to_owned(),
            exec_main_status: 0,
            memory_peak_bytes: 331_120_640,
            memory_swap_peak_bytes: 0,
        }
    );
}

fn formal_systemd_failure_diagnostic_v1(unit: &str) -> String {
    let output = Command::new("journalctl")
        .args(["--user", "--unit", unit, "--no-pager", "-n", "80"])
        .output()
        .expect("读取正式试次 systemd 日志");
    format!(
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

struct FormalSystemdUnitCleanupV1(String);

impl Drop for FormalSystemdUnitCleanupV1 {
    fn drop(&mut self) {
        let _ = Command::new("systemctl")
            .args(["--user", "stop", &self.0])
            .status();
        let _ = Command::new("systemctl")
            .args(["--user", "reset-failed", &self.0])
            .status();
    }
}

#[allow(clippy::too_many_arguments)]
fn start_formal_trial_service_v1(
    unit: &str,
    selector: FormalTrialSelectorV1,
    output_root: &Path,
    build_group: &str,
    run_nonce: &str,
    handshake_root: &Path,
    token: &str,
    release_binary: &Path,
    memory_max_bytes: u64,
    memory_swap_max_bytes: u64,
) {
    let setenv = |key: &str, value: &str| format!("--setenv={key}={value}");
    let output_root_text = output_root.to_string_lossy().into_owned();
    let handshake_root_text = handshake_root.to_string_lossy().into_owned();
    let mut command = Command::new("systemd-run");
    command
        .args(["--user", "--quiet"])
        .arg(format!("--unit={unit}"))
        .args([
            "--property=Type=exec",
            "--property=RemainAfterExit=yes",
            "--property=MemoryAccounting=yes",
            "--property=CPUAccounting=yes",
            "--property=IOAccounting=yes",
        ])
        .arg(format!("--property=MemoryMax={memory_max_bytes}"))
        .arg(format!("--property=MemorySwapMax={memory_swap_max_bytes}"))
        .arg(format!("--property=TasksMax={FORMAL_PIDS_MAX_V1}"))
        .arg(setenv(
            "SIM_COMPONENT_GOLDEN_OUTPUT_ROOT",
            &output_root_text,
        ))
        .arg(setenv("SIM_COMPONENT_GOLDEN_BUILD_GROUP", build_group))
        .arg(setenv("SIM_COMPONENT_GOLDEN_TRIAL_ID", selector.label()))
        .arg(setenv("SIM_COMPONENT_GOLDEN_RUN_NONCE", run_nonce))
        .arg(setenv(
            "SIM_COMPONENT_GOLDEN_LIFECYCLE_HANDSHAKE_ROOT",
            &handshake_root_text,
        ))
        .arg(setenv("SIM_COMPONENT_GOLDEN_LIFECYCLE_TOKEN", token))
        .arg(release_binary)
        .args([
            "task44_tests::task441_component_golden_formal_trial_runner",
            "--ignored",
            "--exact",
            "--nocapture",
        ]);
    let output = command.output().expect("启动正式试次 systemd service");
    assert!(
        output.status.success(),
        "启动正式试次 systemd service 失败：stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn component_trial_report_v1(
    trial_id: ComponentGoldenTrialIdV1,
    raw: &GoldenTrialRawV1,
) -> ComponentGoldenTrialReportV1 {
    let measurement = &raw.preseal_measurement;
    ComponentGoldenTrialReportV1 {
        trial_id,
        mode: trial_id.mode(),
        worker_count: trial_id.worker_count(),
        discarded_warmup_samples: 0,
        preseal_measurement: Some(measurement.clone()),
        run_artifact_receipt: Some(raw.artifact_receipt_binding.clone()),
        sealed_resource_evidence: Some(sealed_resource_evidence_v1(raw)),
        outcome: MeasurementTrialOutcomeV1::Succeeded,
        throughput: measurement.throughput.clone(),
        latency: outer_latency_v1(measurement, raw.seal_latency_ns),
        resource_snapshots: measurement.resource_snapshots.clone(),
        transaction_resource_increments: measurement.transaction_resource_increments.clone(),
        trajectories: measurement.trajectories.clone(),
    }
}

fn verification_trial_report_v1(
    verification_id: FrozenBudgetVerificationTrialIdV1,
    frozen_budgets_sha256: Sha256Hex,
    raw: &GoldenTrialRawV1,
) -> FrozenBudgetVerificationTrialV1 {
    let measurement = &raw.preseal_measurement;
    FrozenBudgetVerificationTrialV1 {
        verification_id,
        mode: verification_id.mode(),
        worker_count: verification_id.worker_count(),
        discarded_warmup_samples: 0,
        workload_contract_sha256: measurement.workload_contract_sha256.clone(),
        frozen_budgets_sha256,
        preseal_measurement: Some(measurement.clone()),
        run_artifact_receipt: Some(raw.artifact_receipt_binding.clone()),
        sealed_resource_evidence: Some(sealed_resource_evidence_v1(raw)),
        outcome: MeasurementTrialOutcomeV1::Succeeded,
        throughput: measurement.throughput.clone(),
        latency: outer_latency_v1(measurement, raw.seal_latency_ns),
        resource_snapshots: measurement.resource_snapshots.clone(),
        trajectories: measurement.trajectories.clone(),
    }
}

fn sealed_resource_evidence_v1(raw: &GoldenTrialRawV1) -> SealedRunResourceEvidenceV1 {
    let postseal_resource_snapshot = raw
        .resource_snapshots
        .last()
        .expect("正式试次必须有封口后资源样本")
        .clone();
    SealedRunResourceEvidenceV1 {
        audit_file_sha256: raw.audit_file_sha256.clone(),
        run_artifact_receipt: raw.artifact_receipt_binding.clone(),
        resource_policy_id: raw.sealed_resource_observation.resource_policy_id.clone(),
        resource_policy_sha256: raw
            .sealed_resource_observation
            .resource_policy_sha256
            .clone(),
        high_water: raw.sealed_resource_observation.resource_high_water.clone(),
        failure_reserve_observation: raw.failure_reserve_observation.clone(),
        durable_transaction_resource_increments: raw.sealed_transaction_resource_increments.clone(),
        artifact_publication_increment: budget_quantities(&raw.artifact_publication_increment),
        postseal_resource_snapshot,
        seal_latency_ns: raw.seal_latency_ns,
    }
}

fn outer_latency_v1(
    measurement: &ComponentGoldenRunMeasurementV1,
    seal_latency_ns: u64,
) -> BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1> {
    let mut latency = measurement.latency.clone();
    let checkpoint_or_seal = latency
        .get_mut(&MeasurementLatencyKindV1::CheckpointOrSeal)
        .expect("封口前黄金延迟必须含 CheckpointOrSeal");
    checkpoint_or_seal.samples_ns.push(seal_latency_ns);
    *checkpoint_or_seal =
        RawLatencySamplesNsV1::from_samples(checkpoint_or_seal.samples_ns.clone())
            .expect("重新计算含真实 seal 样本的最近秩延迟");
    latency
}

struct GoldenTrialRawV1 {
    run_id: String,
    trajectories: [GoldenTrajectoryCompletedV1; 2],
    resource_observations: Vec<ResourceTransactionObservationV1>,
    resource_snapshots: Vec<MeasuredResourceSnapshotV1>,
    sealed_resource_observation: SimulationResourceObservationV1,
    failure_reserve_observation: FailureReserveObservationV1,
    sealed_transaction_resource_increments: Vec<ResourceTransactionIncrementV1>,
    artifact_publication_increment: SimulationResourceUsageV1,
    audit_file_sha256: Sha256Hex,
    preseal_measurement: ComponentGoldenRunMeasurementV1,
    artifact_receipt_binding: SimulationRunArtifactReceiptBindingV1,
    artifact_directory: PathBuf,
    artifact_receipt: Value,
    elapsed_ns: u64,
    seal_latency_ns: u64,
}

async fn run_golden_trial_v1(
    parallel: bool,
    measurement_run_identity: MeasurementRunIdentityV1,
    run_id: String,
    persistence_root: PathBuf,
    rules: Arc<SealedProcessRuleSnapshot>,
    environment: MeasurementEnvironmentIdentityV1,
    resource_policy: SimulationResourcePolicyV1,
) -> GoldenTrialRawV1 {
    let input = Arc::new(GoldenInputV1::new());
    let semantic_rules_sha256 = formal_rule_semantic_sha256_v1(&rules);
    let profile = load_embedded_proxy_latency_profile().expect("加载正式黄金代理时延配置档");
    let intent = SimulationRunIntentBindingV1::try_new(
        rules.snapshot().provenance.implementation_identity.clone(),
        SimulationWorkloadKindV1::ComponentGolden,
        SimulationDeterministicScriptContractV1::ComponentGoldenV1,
    )
    .expect("构造正式黄金运行意图");
    assert!(
        !persistence_root.exists(),
        "每个正式试次必须使用全新持久输出根"
    );
    fs::create_dir_all(
        persistence_root
            .parent()
            .expect("正式试次持久根必须有父目录"),
    )
    .expect("创建正式试次持久根父目录");
    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            run_id.clone(),
            successful_policy_for_tests(),
            rules,
            Arc::clone(&profile),
            Arc::clone(&input.scenario),
            intent,
            input.stream_sha256.clone(),
            persistence_root,
            resource_policy,
        )
        .expect("创建正式黄金持久运行根");
    let mut prepared = Vec::new();
    for (trajectory, seed_digit) in [('a', 'a'), ('b', 'b')] {
        let captures = GoldenCapturesV1::default();
        let mut spec = model_train_spec(
            &input.scenario,
            &format!("task44-component-golden-trajectory-{trajectory}"),
            seed_digit,
        );
        spec.run_id = run_id.clone();
        spec.initial_account_state =
            InitialAccountStateV1::try_new(Decimal::new(2_000, 0)).expect("黄金初始本金 2000 pUSD");
        spec.response_outbox_capacity = NonZeroUsize::new(4_096).expect("黄金输出容量为正");
        let created = process
            .create_session(spec.clone(), captures.bridge(&semantic_rules_sha256))
            .expect("创建正式黄金轨迹");
        process
            .activate_session(activation_request(&spec, &created.control))
            .expect("激活正式黄金轨迹");
        prepared.push(GoldenTrajectoryStateV1::new(
            trajectory,
            semantic_rules_sha256.clone(),
            created.control,
            created.session,
            captures,
        ));
    }
    let session_ids = BTreeMap::from([
        (
            ComponentGoldenTrajectoryV1::A,
            prepared[0].session.session_spec().session_id.clone(),
        ),
        (
            ComponentGoldenTrajectoryV1::B,
            prepared[1].session.session_spec().session_id.clone(),
        ),
    ]);
    let process = Arc::new(Mutex::new(process));
    let sampler = Arc::new(Mutex::new(GoldenResourceSamplerV1::new(session_ids)));
    capture_golden_resource_snapshot(&process, &sampler);
    let trial_started = Instant::now();
    let trajectories = if parallel {
        let barrier = Arc::new(Barrier::new(2));
        let handles = prepared
            .into_iter()
            .map(|mut state| {
                let input = Arc::clone(&input);
                let profile = Arc::clone(&profile);
                let process = Arc::clone(&process);
                let sampler = Arc::clone(&sampler);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    state.begin_timed_execution();
                    let order_ids = run_golden_stage_one(&mut state, &input, &profile);
                    barrier.wait();
                    run_golden_stage_two(
                        &mut state, &input, &profile, &process, &sampler, &order_ids,
                    );
                    barrier.wait();
                    run_golden_stage_three(state, &input, &profile, &process, &sampler)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("正式黄金并行工作线程"))
            .collect::<Vec<_>>()
    } else {
        prepared
            .into_iter()
            .map(|mut state| {
                state.begin_timed_execution();
                let order_ids = run_golden_stage_one(&mut state, &input, &profile);
                run_golden_stage_two(&mut state, &input, &profile, &process, &sampler, &order_ids);
                run_golden_stage_three(state, &input, &profile, &process, &sampler)
            })
            .collect::<Vec<_>>()
    };
    let trial_elapsed_ns = elapsed_ns(trial_started);
    let mut process = Arc::try_unwrap(process)
        .unwrap_or_else(|_| panic!("黄金工作线程结束后不得保留运行根引用"))
        .into_inner()
        .expect("取回黄金运行根");
    sampler.lock().expect("黄金资源采样锁").capture(
        process
            .resource_snapshot_for_tests()
            .expect("读取黄金封口前资源"),
    );
    let preseal_resource_observations = process
        .resource_transaction_observations_for_tests()
        .expect("读取黄金真实逐事务资源观测");
    let trajectories: [GoldenTrajectoryCompletedV1; 2] = trajectories
        .try_into()
        .unwrap_or_else(|_| panic!("正式黄金必须完成两条轨迹"));
    assert_eq!(trajectories[0].trajectory, 'a');
    assert_eq!(trajectories[1].trajectory, 'b');
    assert!(
        trajectories
            .iter()
            .all(|trajectory| trajectory.elapsed_ns <= trial_elapsed_ns),
        "每条轨迹的独立执行时长不得超过覆盖整个试次的外层时长"
    );
    if !parallel {
        assert!(
            trajectories
                .iter()
                .map(|trajectory| trajectory.elapsed_ns)
                .sum::<u64>()
                <= trial_elapsed_ns,
            "串行轨迹必须各自独立起算；两条轨迹时长之和不得超过外层试次时长"
        );
    }
    assert_eq!(
        trajectories
            .iter()
            .map(|result| result.path_deliveries)
            .sum::<u16>(),
        40
    );
    assert_eq!(
        trajectories
            .iter()
            .map(|result| result.top_level_calls)
            .sum::<u16>(),
        38
    );
    let workload = ComponentGoldenLoadContractV1::component_golden_v1();
    let workload_contract_sha256 = workload.content_sha256().expect("推导黄金负载合同身份");
    let trajectories_report = trajectories
        .iter()
        .map(|completed| {
            let result = trajectory_result_v1(completed, &workload_contract_sha256);
            (result.logical_summary_input.trajectory, result)
        })
        .collect::<BTreeMap<_, _>>();
    let resource_snapshots = sampler.lock().expect("黄金资源采样锁").snapshots.clone();
    let measurement = ComponentGoldenRunMeasurementV1 {
        schema_version: COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1.to_owned(),
        run_id: run_id.clone(),
        measurement_run_identity,
        workload_contract_sha256,
        environment,
        runtime_hard_guards: runtime_hard_guards_v1(
            resource_snapshots
                .first()
                .expect("黄金测量必须有首个资源样本"),
        ),
        throughput: trial_throughput_v1(&trajectories_report, trial_elapsed_ns),
        latency: aggregate_latency_v1(&trajectories_report, trial_elapsed_ns),
        resource_snapshots: resource_snapshots.clone(),
        transaction_resource_increments: transaction_resource_increments_v1(
            &preseal_resource_observations,
        ),
        trajectories: trajectories_report,
    };
    measurement
        .validate()
        .expect("封口前黄金测量正文必须完整有效");
    let preseal_measurement = measurement.clone();
    let measurement_sha256 = process
        .record_component_golden_measurement(measurement)
        .expect("真实记录封口前黄金测量事实");
    let seal_started = Instant::now();
    let artifact_receipt = process.seal_run_artifacts().expect("封存正式黄金产物");
    let seal_latency_ns = elapsed_ns(seal_started);
    let audit_bytes = fs::read(
        artifact_receipt
            .artifact_directory
            .join(SIMULATION_RUN_AUDIT_FILE_NAME_V1),
    )
    .expect("读取正式黄金审计产物");
    let audit_file_sha256 = sha256_bytes(&audit_bytes);
    let audit: SimulationRunAuditV1 =
        serde_json::from_slice(&audit_bytes).expect("独立严格解码正式黄金审计产物");
    let manifest: SimulationRunManifestV1 = serde_json::from_slice(
        &fs::read(
            artifact_receipt
                .artifact_directory
                .join(SIMULATION_RUN_MANIFEST_FILE_NAME_V1),
        )
        .expect("读取正式黄金清单产物"),
    )
    .expect("独立严格解码正式黄金清单产物");
    assert_eq!(
        audit.component_golden_measurement_sha256.as_ref(),
        Some(&measurement_sha256),
        "审计产物必须绑定同一封口前测量身份"
    );
    assert_eq!(
        manifest.component_golden_measurement_sha256.as_ref(),
        Some(&measurement_sha256),
        "正式清单必须绑定同一封口前测量身份"
    );
    let embedded = audit
        .component_golden_measurement
        .as_ref()
        .expect("审计产物必须嵌入封口前测量正文");
    assert_eq!(
        embedded.content_sha256().expect("重算审计内测量身份"),
        measurement_sha256
    );
    sampler.lock().expect("黄金资源采样锁").capture(
        process
            .resource_snapshot_for_tests()
            .expect("读取黄金封口后资源"),
    );
    let resource_observations = process
        .resource_transaction_observations_for_tests()
        .expect("读取黄金完整证据前缀的真实逐事务资源观测");
    assert_eq!(
        u64::try_from(resource_observations.len()).expect("证据数量适合 u64"),
        audit.through_commit_sequence,
        "逐事务资源观测必须覆盖完整封口证据前缀"
    );
    let sealed_transaction_resource_increments =
        transaction_resource_increments_v1(&resource_observations);
    let postseal_snapshot = sampler
        .lock()
        .expect("黄金资源采样锁")
        .snapshots
        .last()
        .expect("黄金封口后必须有资源样本")
        .clone();
    let last_durable_transaction = &resource_observations
        .last()
        .expect("封口证据前缀必须有最后一项真实资源观测")
        .maintenance_succeeded_steady_after
        .durable;
    let mut artifact_publication_increment = SimulationResourceUsageV1::default();
    artifact_publication_increment.durable.artifacts = ResourceQuantityV1::new(
        postseal_snapshot
            .internal
            .durable
            .artifacts
            .records
            .checked_sub(last_durable_transaction.artifacts.records)
            .expect("artifact publish 记录增量不得回退"),
        postseal_snapshot
            .internal
            .durable
            .artifacts
            .bytes
            .checked_sub(last_durable_transaction.artifacts.bytes)
            .expect("artifact publish 字节增量不得回退"),
    );
    artifact_publication_increment.durable.runtime_root = ResourceQuantityV1::new(
        postseal_snapshot
            .internal
            .durable
            .runtime_root
            .records
            .checked_sub(last_durable_transaction.runtime_root.records)
            .expect("artifact publish 运行根记录增量不得回退"),
        postseal_snapshot
            .internal
            .durable
            .runtime_root
            .bytes
            .checked_sub(last_durable_transaction.runtime_root.bytes)
            .expect("artifact publish 运行根字节增量不得回退"),
    );
    assert_eq!(artifact_publication_increment.durable.artifacts.records, 6);
    assert!(artifact_publication_increment.durable.artifacts.bytes > 0);
    assert!(artifact_publication_increment.durable.runtime_root.records > 0);
    assert!(artifact_publication_increment.durable.runtime_root.bytes > 0);
    let sealed_resource_observation = audit.resource_observation.clone();
    let (maintenance_steady_high_water, one_failure_reserve) = process
        .failure_reserve_observation_for_tests()
        .expect("读取 seal 与产物发布后的失败续租强观测");
    let failure_reserve_observation = FailureReserveObservationV1 {
        maintenance_steady_high_water,
        one_failure_reserve,
    };
    failure_reserve_observation
        .validate()
        .expect("seal 与产物发布后的失败续租强观测必须有效");
    let artifact_receipt_binding = SimulationRunArtifactReceiptBindingV1 {
        run_id: run_id.clone(),
        measurement_run_identity,
        manifest_sha256: artifact_receipt.manifest_sha256.clone(),
        through_commit_sequence: artifact_receipt.through_commit_sequence,
        evidence_head_sha256: artifact_receipt.evidence_head_sha256.clone(),
    };
    let artifact_directory = artifact_receipt.artifact_directory.clone();
    let artifact_receipt = serde_json::json!({
        "manifest_sha256": artifact_receipt.manifest_sha256,
        "through_commit_sequence": artifact_receipt.through_commit_sequence,
        "evidence_head_sha256": artifact_receipt.evidence_head_sha256,
        "artifact_directory_exists": artifact_receipt.artifact_directory.is_dir(),
        "measurement_sha256": measurement_sha256,
    });
    GoldenTrialRawV1 {
        run_id,
        trajectories,
        resource_observations,
        resource_snapshots: Arc::try_unwrap(sampler)
            .unwrap_or_else(|_| panic!("黄金工作线程结束后不得保留采样器引用"))
            .into_inner()
            .expect("取回黄金资源采样器")
            .snapshots,
        sealed_resource_observation,
        failure_reserve_observation,
        sealed_transaction_resource_increments,
        artifact_publication_increment,
        audit_file_sha256,
        preseal_measurement,
        artifact_receipt_binding,
        artifact_directory,
        artifact_receipt,
        elapsed_ns: trial_elapsed_ns,
        seal_latency_ns,
    }
}

fn measured_positive_quantity_delta_v1(
    candidate: ResourceQuantityV1,
    baseline: ResourceQuantityV1,
    label: &str,
) -> ResourceQuantityV1 {
    let records = candidate
        .records
        .checked_sub(baseline.records)
        .unwrap_or_else(|| panic!("{label} 失败事务候选记录数不得低于提交前实际占用"));
    let bytes = candidate
        .bytes
        .checked_sub(baseline.bytes)
        .unwrap_or_else(|| panic!("{label} 失败事务候选字节不得低于提交前实际占用"));
    assert!(records > 0, "{label} 失败事务必须真实追加记录");
    assert!(bytes > 0, "{label} 失败事务必须真实追加字节");
    ResourceQuantityV1::new(records, bytes)
}

fn formal_failure_probe_policy_v1() -> SimulationResourcePolicyV1 {
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.run.input_queue = ResourceLimitV1::new(1, policy.run.input_queue.max_bytes);
    policy
}

async fn measure_real_failure_reserve_observation_v1(
    run_id: &str,
    persistence_root: PathBuf,
    rules: Arc<SealedProcessRuleSnapshot>,
    policy: SimulationResourcePolicyV1,
) -> FailureReserveObservationV1 {
    assert!(!persistence_root.exists(), "失败事务探针必须使用全新持久根");
    let profile = load_embedded_proxy_latency_profile().expect("加载失败事务探针时延配置档");
    let scenario = scenario();
    let intent = SimulationRunIntentBindingV1::try_new(
        rules.snapshot().provenance.implementation_identity.clone(),
        SimulationWorkloadKindV1::ComponentGolden,
        SimulationDeterministicScriptContractV1::ComponentGoldenV1,
    )
    .expect("构造失败事务探针运行意图");
    let mut process =
        SimExecutionProcessV1::try_new_with_local_persistence_and_resource_policy_for_tests(
            run_id.to_owned(),
            successful_policy_for_tests(),
            rules,
            Arc::clone(&profile),
            Arc::clone(&scenario),
            intent,
            sha256_bytes(b"TASK44_COMPONENT_GOLDEN_FAILURE_RESERVE_EVENT_SOURCE_V1"),
            persistence_root.clone(),
            policy,
        )
        .expect("创建真实失败事务探针");
    let mut spec = session_spec(
        &scenario,
        SimCallerRoleV1::ModelTrain,
        &format!("{run_id}-session"),
    );
    spec.run_id = run_id.to_owned();
    let prepared = process
        .create_session(spec.clone(), BridgeProbeV1::default().bridge())
        .expect("创建真实失败事务探针会话");
    process
        .activate_session(activation_request(&spec, &prepared.control))
        .expect("激活真实失败事务探针会话");
    let before = process
        .resource_snapshot_for_tests()
        .expect("读取失败事务提交前实际资源");
    let transactions_before = process
        .resource_transaction_observations_for_tests()
        .expect("读取失败事务提交前证据资源")
        .len();
    let encoded = initial_events("0.01", "0.6", "1000")
        .into_iter()
        .map(|event| encode_execution_market_event_v3(&event).expect("编码失败事务真实闭桶"))
        .collect::<Vec<_>>();
    let mut session = prepared.session;
    let limit = match session.apply_closed_market_event_bucket(ClosedMarketEventBucketV1::public(
        120_000,
        0,
        2,
        MillisecondBucketClosureV1::NextEventAt(120_001),
        encoded,
    )) {
        Err(crate::market::MarketEventErrorV1::Session(SessionError::ResourceLimitReached(
            limit,
        ))) => limit,
        other => panic!("失败事务探针必须真实命中 Run/InputQueue，实际为 {other:?}"),
    };
    assert_eq!(limit.scope, ResourceLimitScopeV1::Run);
    assert_eq!(limit.kind, ResourceLimitKindV1::InputQueue);
    let transactions_after = process
        .resource_transaction_observations_for_tests()
        .expect("读取失败事实提交后的真实事务资源");
    assert_eq!(
        transactions_after.len(),
        transactions_before + 1,
        "失败探针必须只追加一项 ResourceLimitReached 事务"
    );
    assert_eq!(
        durable_fact_entries(&persistence_root)
            .iter()
            .filter(|(_, fact)| fact.get("RESOURCE_LIMIT_REACHED").is_some())
            .count(),
        1,
        "失败探针必须持久化唯一 ResourceLimitReached 事实"
    );
    let failure_candidate = transactions_after
        .last()
        .expect("失败探针必须有最后一项事务资源")
        .during_commit_peak
        .clone();
    assert_eq!(
        failure_candidate.run.in_memory_state.records,
        before.run.in_memory_state.records + 1,
        "失败准入必须把一条局部 ResourceLimit 墓碑计入运行内存记录"
    );
    assert_eq!(
        failure_candidate.run.sessions.records, before.run.sessions.records,
        "失败准入只增加运行检查点字节，不增加运行会话记录"
    );
    let run_checkpoint_growth = failure_candidate
        .run
        .in_memory_state
        .bytes
        .checked_sub(before.run.in_memory_state.bytes)
        .expect("失败事务候选运行检查点字节不得低于提交前实际占用");
    let run_sessions_growth = failure_candidate
        .run
        .sessions
        .bytes
        .checked_sub(before.run.sessions.bytes)
        .expect("失败事务候选运行会话字节不得低于提交前实际占用");
    assert_eq!(
        run_sessions_growth, run_checkpoint_growth,
        "运行内存与会话两路必须复用同一完整检查点字节增量"
    );
    assert!(run_checkpoint_growth > 0, "失败事务必须增加运行检查点字节");
    let low_state_failure_delta = SimulationFailureEvidenceReserveV1 {
        run_checkpoint_growth: ResourceQuantityV1::new(1, run_checkpoint_growth),
        evidence: measured_positive_quantity_delta_v1(
            failure_candidate.durable.evidence,
            before.durable.evidence,
            "evidence",
        ),
        checkpoints: measured_positive_quantity_delta_v1(
            failure_candidate.durable.checkpoints,
            before.durable.checkpoints,
            "checkpoints",
        ),
        runtime_root: measured_positive_quantity_delta_v1(
            failure_candidate.durable.runtime_root,
            before.durable.runtime_root,
            "runtime_root",
        ),
    };
    assert_eq!(low_state_failure_delta.evidence.records, 1);
    assert_eq!(low_state_failure_delta.checkpoints.records, 1);
    assert_eq!(low_state_failure_delta.runtime_root.records, 3);

    let (maintenance_steady_high_water, one_failure_reserve) = process
        .failure_reserve_observation_for_tests()
        .expect("读取动态单槽续租失败预留观测");
    let observation = FailureReserveObservationV1 {
        maintenance_steady_high_water,
        one_failure_reserve,
    };
    observation
        .validate()
        .expect("动态单槽续租失败预留观测必须保持完整失败事实形态");
    observation
}

#[tokio::test(flavor = "current_thread")]
async fn task441_failure_probe_returns_dynamic_steady_and_full_failure_shape() {
    let directory = tempfile::tempdir().expect("创建失败续租强观测临时目录");
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let observation = measure_real_failure_reserve_observation_v1(
        "task441-dynamic-failure-reserve-observation",
        directory.path().join("runtime"),
        rules,
        formal_failure_probe_policy_v1(),
    )
    .await;

    observation.validate().expect("失败续租强观测必须有效");
    let steady = &observation.maintenance_steady_high_water.observed;
    assert!(steady.run.in_memory_state.records > 0);
    assert!(steady.run.in_memory_state.bytes > 0);
    assert!(steady.run.sessions.records > 0);
    assert!(steady.run.sessions.bytes > 0);
    assert!(steady.durable.evidence.records > 0 && steady.durable.evidence.bytes > 0);
    assert!(steady.durable.checkpoints.records > 0 && steady.durable.checkpoints.bytes > 0);
    assert!(steady.durable.runtime_root.records > 0 && steady.durable.runtime_root.bytes > 0);
    assert_eq!(
        observation
            .one_failure_reserve
            .run_checkpoint_growth
            .records,
        1
    );
    assert_eq!(observation.one_failure_reserve.evidence.records, 1);
    assert_eq!(observation.one_failure_reserve.checkpoints.records, 1);
    assert_eq!(observation.one_failure_reserve.runtime_root.records, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn task441_fixed_width_formal_run_ids_keep_real_failure_shape_equal() {
    let directory = tempfile::tempdir().expect("创建固定宽度运行身份临时目录");
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let first_run_id = formal_run_id_v1(&sha256_bytes(b"suite-a"), "P2");
    let second_run_id = formal_run_id_v1(&sha256_bytes(b"suite-b"), "VERIFY_PARALLEL");
    assert_ne!(first_run_id, second_run_id);
    assert_eq!(first_run_id.len(), second_run_id.len());

    let first = measure_real_failure_reserve_observation_v1(
        &first_run_id,
        directory.path().join("first-runtime"),
        Arc::clone(&rules),
        formal_failure_probe_policy_v1(),
    )
    .await;
    let second = measure_real_failure_reserve_observation_v1(
        &second_run_id,
        directory.path().join("second-runtime"),
        rules,
        formal_failure_probe_policy_v1(),
    )
    .await;
    assert_eq!(
        first.one_failure_reserve, second.one_failure_reserve,
        "同一策略与状态下，不同套件身份及 B1/B2 标签不得改变最大失败事实形态"
    );
}

fn formal_run_nonce_v1() -> String {
    let nonce = std::env::var("SIM_COMPONENT_GOLDEN_RUN_NONCE")
        .expect("正式单试次缺少 SIM_COMPONENT_GOLDEN_RUN_NONCE");
    assert!(
        !nonce.is_empty() && nonce.len() <= 64,
        "正式 run nonce 长度非法"
    );
    assert!(
        nonce
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        "正式 run nonce 只能含 ASCII 字母、数字、连字符或下划线"
    );
    nonce
}

fn formal_run_id_v1(suite_identity: &Sha256Hex, logical_identity: &str) -> String {
    let identity = sha256_serializable(
        FORMAL_RUN_ID_HASH_DOMAIN_V1,
        &(suite_identity, logical_identity),
    );
    format!("task44-component-golden-{}", identity.0)
}

#[test]
fn task441_formal_run_ids_have_one_fixed_width_across_b1_b2_and_probe() {
    let suite_identity = sha256_bytes(b"stage4-suite");
    let identities = [
        "S1",
        "P1",
        "P2",
        "S2",
        "S3",
        "P3",
        "VERIFY_SERIAL",
        "VERIFY_PARALLEL",
        "FAILURE_RESERVE",
    ];
    let run_ids = identities
        .into_iter()
        .map(|identity| formal_run_id_v1(&suite_identity, identity))
        .collect::<BTreeSet<_>>();
    assert_eq!(run_ids.len(), identities.len());
    assert!(
        run_ids
            .iter()
            .all(|run_id| run_id.len() == "task44-component-golden-".len() + 64)
    );
    assert_eq!(
        run_ids
            .iter()
            .map(String::len)
            .collect::<BTreeSet<_>>()
            .len(),
        1,
        "B1、B2 与独立探针的正式运行身份必须固定宽度，避免物理身份长度污染失败检查点预留"
    );
    assert!(run_ids.iter().all(|run_id| run_id.len() <= 256));
    assert!(run_ids.iter().all(|run_id| {
        run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    }));
}

fn assert_sealed_resource_observation_v1(raw: &GoldenTrialRawV1) {
    let postseal = raw
        .resource_snapshots
        .last()
        .expect("正式试次必须保留封口后资源样本");
    let artifact_usage = postseal.internal.durable.artifacts;
    assert_eq!(artifact_usage.records, 6, "正式规范包必须含六个文件");
    assert!(artifact_usage.bytes > 0, "正式规范包必须有真实字节占用");
    let usage = SimulationResourceUsageV1 {
        durable: postseal.internal.durable.clone(),
        ..SimulationResourceUsageV1::default()
    };
    assert!(
        raw.sealed_resource_observation
            .resource_high_water
            .covers(&usage),
        "manifest 绑定审计中的 seal fixed-point 高水位必须覆盖封口后实际耐久占用"
    );
    assert_eq!(
        raw.sealed_resource_observation.resource_high_water, postseal.internal.high_water,
        "严格解码 audit 的 seal fixed-point 高水位必须等于封口后只读诊断"
    );
    assert_eq!(
        raw.sealed_transaction_resource_increments.len(),
        usize::try_from(raw.artifact_receipt_binding.through_commit_sequence)
            .expect("正式证据序号适合 usize"),
        "封口后耐久事务增量必须恰好覆盖完整 evidence 前缀"
    );
}

fn persisted_formal_trial_v1(
    selector: FormalTrialSelectorV1,
    trial_directory: &Path,
    raw: &GoldenTrialRawV1,
    frozen_budgets_sha256: Option<Sha256Hex>,
) -> PersistedFormalTrialV1 {
    assert_sealed_resource_observation_v1(raw);
    let relative_artifact_directory = raw
        .artifact_directory
        .strip_prefix(trial_directory)
        .expect("正式产物目录必须位于当前唯一试次目录内")
        .to_string_lossy()
        .into_owned();
    assert!(!relative_artifact_directory.starts_with('/'));
    let trial = match selector {
        FormalTrialSelectorV1::Component(trial_id) => {
            assert!(frozen_budgets_sha256.is_none());
            PersistedFormalTrialReportV1::Component(component_trial_report_v1(trial_id, raw))
        }
        FormalTrialSelectorV1::Verification(verification_id) => {
            PersistedFormalTrialReportV1::Verification(verification_trial_report_v1(
                verification_id,
                frozen_budgets_sha256.expect("冻结后复核必须绑定预算 SHA-256"),
                raw,
            ))
        }
    };
    PersistedFormalTrialV1 {
        schema_version: "component_golden_persisted_trial_v1".to_owned(),
        run_id: raw.run_id.clone(),
        runtime_root_relative_path: "runtime".to_owned(),
        artifact_directory_relative_path: relative_artifact_directory,
        trial,
    }
}

fn persisted_trial_measurement_and_receipt_v1(
    persisted: &PersistedFormalTrialV1,
) -> (
    &ComponentGoldenRunMeasurementV1,
    &SimulationRunArtifactReceiptBindingV1,
) {
    match &persisted.trial {
        PersistedFormalTrialReportV1::Component(report) => (
            report
                .preseal_measurement
                .as_ref()
                .expect("成功正式试次必须有封口前测量正文"),
            report
                .run_artifact_receipt
                .as_ref()
                .expect("成功正式试次必须有运行产物回执"),
        ),
        PersistedFormalTrialReportV1::Verification(report) => (
            report
                .preseal_measurement
                .as_ref()
                .expect("成功冻结复核必须有封口前测量正文"),
            report
                .run_artifact_receipt
                .as_ref()
                .expect("成功冻结复核必须有运行产物回执"),
        ),
    }
}

fn persisted_trial_sealed_resource_evidence_v1(
    persisted: &PersistedFormalTrialV1,
) -> &SealedRunResourceEvidenceV1 {
    match &persisted.trial {
        PersistedFormalTrialReportV1::Component(report) => report
            .sealed_resource_evidence
            .as_ref()
            .expect("成功正式试次必须有封口后资源证据"),
        PersistedFormalTrialReportV1::Verification(report) => report
            .sealed_resource_evidence
            .as_ref()
            .expect("成功冻结复核必须有封口后资源证据"),
    }
}

fn read_complete_resource_observation_prefix_v1(
    runtime_root: &Path,
    expected_receipt: &SimulationRunArtifactReceiptBindingV1,
) -> (Vec<ResourceTransactionObservationV1>, u64) {
    let evidence_directory = runtime_root.join("evidence");
    let mut members = fs::read_dir(&evidence_directory)
        .expect("聚合阶段读取完整 evidence 目录")
        .map(|entry| entry.expect("聚合阶段读取 evidence 目录项"))
        .filter(|entry| entry.file_type().expect("读取 evidence 文件类型").is_file())
        .filter_map(|entry| {
            let name = entry
                .file_name()
                .into_string()
                .expect("evidence 文件名必须是 UTF-8");
            (!name.starts_with('.') && name.ends_with(".json")).then_some((name, entry.path()))
        })
        .map(|(name, path)| {
            let stem = name
                .strip_suffix(".json")
                .expect("evidence 文件必须以 .json 结尾");
            let (sequence, declared_sha256) = stem
                .split_once('-')
                .expect("evidence 文件名必须含序号和 SHA-256");
            assert_eq!(sequence.len(), 20, "evidence 序号必须固定二十位");
            assert_eq!(
                declared_sha256.len(),
                64,
                "evidence 文件名 SHA-256 长度非法"
            );
            assert!(
                declared_sha256
                    .bytes()
                    .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) })
            );
            (
                sequence.parse::<u64>().expect("解析 evidence 文件序号"),
                Sha256Hex(declared_sha256.to_owned()),
                path,
            )
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|(sequence, _, _)| *sequence);
    assert_eq!(
        u64::try_from(members.len()).expect("evidence 数量适合 u64"),
        expected_receipt.through_commit_sequence,
        "聚合阶段必须枚举完整连续证据前缀"
    );
    let mut predecessor = None::<Sha256Hex>;
    let mut total_bytes = 0_u64;
    let mut observations = Vec::with_capacity(members.len());
    for (index, (sequence, declared_sha256, path)) in members.into_iter().enumerate() {
        let expected_sequence = u64::try_from(index).expect("evidence 序号适合 u64") + 1;
        assert_eq!(sequence, expected_sequence, "evidence 序号必须从一连续递增");
        let bytes = fs::read(&path).expect("聚合阶段读取 evidence 正文");
        total_bytes = total_bytes
            .checked_add(u64::try_from(bytes.len()).expect("evidence 字节适合 u64"))
            .expect("evidence 前缀字节求和溢出");
        let actual_sha256 = sha256_bytes(&bytes);
        assert_eq!(
            actual_sha256, declared_sha256,
            "evidence 文件名摘要不可伪造"
        );
        let envelope: Value =
            serde_json::from_slice(&bytes).expect("严格解码聚合阶段 evidence 信封 JSON");
        assert_eq!(
            envelope.get("sequence").and_then(Value::as_u64),
            Some(sequence),
            "evidence 信封序号必须匹配文件名"
        );
        match predecessor.as_ref() {
            Some(expected) => assert_eq!(
                envelope
                    .get("predecessor_evidence_sha256")
                    .and_then(Value::as_str),
                Some(expected.0.as_str()),
                "evidence 前驱链必须连续"
            ),
            None => assert!(
                envelope
                    .get("predecessor_evidence_sha256")
                    .is_some_and(Value::is_null),
                "首项 evidence 必须没有前驱"
            ),
        }
        let decode_usage = |pointer: &str, label: &str| {
            serde_json::from_value::<SimulationResourceUsageV1>(
                envelope
                    .pointer(pointer)
                    .cloned()
                    .unwrap_or_else(|| panic!("每项正式 evidence 必须含 {label}")),
            )
            .unwrap_or_else(|error| panic!("严格解码每项 evidence 的{label}失败：{error}"))
        };
        observations.push(ResourceTransactionObservationV1 {
            maintenance_steady_before: decode_usage(
                "/body/maintenance_steady_before_resource_usage",
                "提交前维护稳态资源使用量",
            ),
            during_commit_peak: decode_usage("/body/resource_usage", "提交期间峰值资源使用量"),
            maintenance_succeeded_steady_after: decode_usage(
                "/body/maintenance_succeeded_steady_after_resource_usage",
                "维护成功后稳态资源使用量",
            ),
        });
        predecessor = Some(actual_sha256);
    }
    assert_eq!(
        predecessor.as_ref(),
        Some(&expected_receipt.evidence_head_sha256),
        "完整 evidence 前缀末项必须等于回执证据头"
    );
    (observations, total_bytes)
}

fn validate_persisted_formal_trial_v1(
    output_root: &Path,
    selector: FormalTrialSelectorV1,
    persisted: &PersistedFormalTrialV1,
) {
    assert_eq!(
        persisted.schema_version, "component_golden_persisted_trial_v1",
        "正式单试次外壳版本非法"
    );
    match (&persisted.trial, selector) {
        (
            PersistedFormalTrialReportV1::Component(report),
            FormalTrialSelectorV1::Component(expected),
        ) => assert_eq!(report.trial_id, expected),
        (
            PersistedFormalTrialReportV1::Verification(report),
            FormalTrialSelectorV1::Verification(expected),
        ) => assert_eq!(report.verification_id, expected),
        _ => panic!("正式单试次外壳种类与固定目录身份不一致"),
    }
    let (measurement, receipt) = persisted_trial_measurement_and_receipt_v1(persisted);
    let sealed = persisted_trial_sealed_resource_evidence_v1(persisted);
    measurement
        .validate()
        .expect("正式单试次封口前测量必须有效");
    assert_eq!(persisted.run_id, measurement.run_id);
    assert_eq!(persisted.run_id, receipt.run_id);
    assert_eq!(
        receipt.measurement_run_identity,
        selector.measurement_identity()
    );
    assert_eq!(
        measurement.measurement_run_identity,
        selector.measurement_identity()
    );
    let relative = Path::new(&persisted.artifact_directory_relative_path);
    assert!(!relative.is_absolute(), "正式产物相对路径不得是绝对路径");
    assert!(
        relative
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_))),
        "正式产物相对路径不得含父级、根或平台前缀"
    );
    let trial_directory = output_root.join("trials").join(selector.label());
    let runtime_relative = Path::new(&persisted.runtime_root_relative_path);
    assert!(
        !runtime_relative.is_absolute()
            && runtime_relative
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
        "正式运行根相对路径不得越界"
    );
    let runtime_root = trial_directory.join(runtime_relative);
    assert!(runtime_root.is_dir(), "正式持久运行根必须仍然存在");
    let artifact_directory = trial_directory.join(relative);
    assert!(artifact_directory.is_dir(), "正式产物目录必须仍然存在");
    assert_eq!(
        artifact_directory
            .file_name()
            .and_then(|name| name.to_str()),
        Some(receipt.manifest_sha256.0.as_str()),
        "正式内容寻址目录必须由回执 manifest SHA-256 命名"
    );
    let manifest_path = artifact_directory.join(SIMULATION_RUN_MANIFEST_FILE_NAME_V1);
    let audit_path = artifact_directory.join(SIMULATION_RUN_AUDIT_FILE_NAME_V1);
    let manifest_bytes = fs::read(&manifest_path).expect("读取聚合阶段正式 manifest");
    let audit_bytes = fs::read(&audit_path).expect("读取聚合阶段正式 audit");
    assert_eq!(sha256_bytes(&manifest_bytes), receipt.manifest_sha256);
    let manifest: SimulationRunManifestV1 =
        serde_json::from_slice(&manifest_bytes).expect("严格解码聚合阶段正式 manifest");
    let audit: SimulationRunAuditV1 =
        serde_json::from_slice(&audit_bytes).expect("严格解码聚合阶段正式 audit");
    assert_eq!(manifest.run_id, persisted.run_id);
    assert_eq!(audit.run_id, persisted.run_id);
    assert_eq!(
        audit.through_commit_sequence,
        receipt.through_commit_sequence
    );
    assert_eq!(
        manifest.evidence_prefix.through_commit_sequence,
        receipt.through_commit_sequence
    );
    assert_eq!(
        manifest.evidence_prefix.evidence_head_sha256,
        receipt.evidence_head_sha256
    );
    assert_eq!(
        manifest.component_golden_measurement_sha256.as_ref(),
        Some(
            &measurement
                .content_sha256()
                .expect("重算聚合阶段封口前测量身份")
        )
    );
    assert_eq!(
        audit.component_golden_measurement.as_ref(),
        Some(measurement),
        "聚合必须从正式 audit 独立复核同一测量正文"
    );
    assert_eq!(
        audit.resource_observation.resource_policy_id,
        sealed.resource_policy_id
    );
    assert_eq!(
        audit.resource_observation.resource_policy_sha256,
        sealed.resource_policy_sha256
    );
    assert_eq!(
        audit.resource_observation.resource_high_water,
        sealed.high_water
    );
    assert_eq!(sha256_bytes(&audit_bytes), sealed.audit_file_sha256);
    let (resource_observations, evidence_bytes) =
        read_complete_resource_observation_prefix_v1(&runtime_root, receipt);
    assert_eq!(
        manifest.evidence_prefix.evidence_count,
        receipt.through_commit_sequence
    );
    assert_eq!(manifest.evidence_prefix.evidence_bytes, evidence_bytes);
    assert_eq!(
        audit.resource_observation.evidence_file_count,
        receipt.through_commit_sequence
    );
    assert_eq!(audit.resource_observation.evidence_bytes, evidence_bytes);
    let expected_evidence_increments = transaction_resource_increments_v1(&resource_observations);
    assert_eq!(
        sealed
            .durable_transaction_resource_increments
            .get(..expected_evidence_increments.len()),
        Some(expected_evidence_increments.as_slice()),
        "聚合必须从完整 evidence 前缀独立重算每项资源增量"
    );
    assert_eq!(
        audit.resource_observation.resource_high_water,
        sealed.postseal_resource_snapshot.internal.high_water
    );
    let mut bundle_bytes = u64::try_from(manifest_bytes.len()).expect("manifest 字节适合 u64");
    for member in &manifest.artifacts {
        let member_relative = Path::new(&member.relative_path);
        assert!(
            member_relative
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
            "manifest 成员路径不得越界"
        );
        let bytes =
            fs::read(artifact_directory.join(member_relative)).expect("读取 manifest 声明成员");
        assert_eq!(
            u64::try_from(bytes.len()).expect("产物成员字节适合 u64"),
            member.byte_length
        );
        assert_eq!(sha256_bytes(&bytes), member.file_sha256);
        bundle_bytes = bundle_bytes
            .checked_add(member.byte_length)
            .expect("正式包成员字节求和溢出");
    }
    assert_eq!(
        sealed.artifact_publication_increment[&ResourceBudgetPathV1::DurableArtifacts],
        ResourceQuantityV1::new(
            u64::try_from(manifest.artifacts.len() + 1).expect("正式包成员数适合 u64"),
            bundle_bytes,
        ),
        "artifact publish 独立增量必须由 manifest 与六个真实文件重算"
    );
    assert_eq!(
        sealed.artifact_publication_increment[&ResourceBudgetPathV1::DurableArtifacts].records,
        6
    );
    assert!(
        sealed.artifact_publication_increment[&ResourceBudgetPathV1::DurableRuntimeRoot].records
            > 0
    );
    assert!(sealed.high_water.covers(&SimulationResourceUsageV1 {
        durable: sealed.postseal_resource_snapshot.internal.durable.clone(),
        ..SimulationResourceUsageV1::default()
    }));
    assert_eq!(
        sealed.durable_transaction_resource_increments.len(),
        usize::try_from(receipt.through_commit_sequence).expect("证据序号适合 usize")
    );
    for (index, increment) in sealed
        .durable_transaction_resource_increments
        .iter()
        .enumerate()
    {
        assert_eq!(
            increment.transaction_index,
            u64::try_from(index).expect("事务序号适合 u64")
        );
        assert_eq!(increment.increments.len(), 24);
    }
}

fn read_persisted_formal_trial_v1(
    output_root: &Path,
    selector: FormalTrialSelectorV1,
) -> PersistedFormalTrialV1 {
    let path = output_root
        .join("trials")
        .join(selector.label())
        .join(FORMAL_TRIAL_FILE_V1);
    let persisted = read_json_with_sha256(&path);
    validate_persisted_formal_trial_v1(output_root, selector, &persisted);
    persisted
}

fn read_formal_lifecycle_tail_v1(
    output_root: &Path,
    selector: FormalTrialSelectorV1,
    build_group: MeasurementBuildGroupV1,
    build_group_label: &str,
    persisted: &PersistedFormalTrialV1,
) -> TrialLifecycleTailEvidenceV1 {
    let trial_directory = output_root.join("trials").join(selector.label());
    let trial_path = trial_directory.join(FORMAL_TRIAL_FILE_V1);
    let trial_bytes = fs::read(&trial_path).expect("聚合阶段读取主试次 JSON");
    let trial_sha256 = sha256_bytes(&trial_bytes);
    let fixed_context_path = output_root.join(format!(
        "{build_group_label}-{FORMAL_FIXED_CONTEXT_FILE_V1}"
    ));
    let fixed_context_file_sha256 =
        sha256_bytes(&fs::read(&fixed_context_path).expect("聚合阶段读取固定上下文文件"));
    let evidence: TrialLifecycleTailEvidenceV1 =
        read_json_with_sha256(&trial_directory.join(FORMAL_LIFECYCLE_TAIL_FILE_V1));
    let (measurement, _) = persisted_trial_measurement_and_receipt_v1(persisted);
    validate_lifecycle_tail_binding_v1(
        &evidence,
        &TrialLifecycleTailBindingV1 {
            build_group,
            measurement_run_identity: selector.measurement_identity(),
            selector_label: selector.label(),
            run_id: &persisted.run_id,
            environment: &measurement.environment,
            runtime_hard_guards: measurement.runtime_hard_guards,
            last_resource_snapshot: &persisted_trial_sealed_resource_evidence_v1(persisted)
                .postseal_resource_snapshot,
            fixed_context_file_sha256: &fixed_context_file_sha256,
            main_trial_json_sha256: &trial_sha256,
            main_trial_json_bytes: u64::try_from(trial_bytes.len())
                .expect("主试次 JSON 字节数适合 u64"),
        },
    )
    .expect("聚合阶段必须严格重证完整生命周期尾部信封与主试次绑定");
    let first_process = &measurement.resource_snapshots[0].procfs;
    assert_eq!(evidence.final_process.pid, first_process.pid);
    assert_eq!(
        evidence.final_process.process_start_time_ticks, first_process.process_start_time_ticks,
        "生命周期尾部证据必须延续同一主试次进程启动身份"
    );
    evidence
}

fn process_peaks_v1(
    lifecycle: &BTreeMap<ComponentGoldenTrialIdV1, TrialLifecycleTailEvidenceV1>,
) -> (u64, u64, u64) {
    lifecycle_process_peaks_v1(&lifecycle.values().cloned().collect::<Vec<_>>())
        .expect("从六份完整生命周期尾部证据重算进程峰值")
}

#[test]
#[ignore = "冻结后生产策略映射必须由固定 B2 二进制在创建复核目录前显式核对"]
fn task441_component_golden_validate_frozen_policy_mapping() {
    let output_root = formal_existing_output_root_from_environment();
    assert_eq!(formal_build_group_v1(), "B2");
    let context = formal_fixed_context_v1(&output_root);
    let current_exe = std::env::current_exe()
        .expect("读取 B2 策略预检二进制路径")
        .canonicalize()
        .expect("规范化 B2 策略预检二进制路径");
    assert_eq!(
        current_exe, context.release_unit_test_binary,
        "冻结策略预检必须直接执行固定上下文中的同一 B2 二进制"
    );
    assert_eq!(
        sha256_bytes(&fs::read(&current_exe).expect("读取 B2 策略预检二进制")),
        context.build.release_binary_sha256,
        "冻结策略预检二进制正文必须匹配 B2 固定上下文"
    );

    let frozen: FrozenResourceBudgetsV1 =
        read_json_with_sha256(&output_root.join(FORMAL_FROZEN_BUDGETS_FILE_V1));
    let expected = frozen
        .to_resource_policy_v1()
        .expect("把冻结预算转换为预期生产策略");
    let production = SimulationResourcePolicyV1::component_golden_v1();
    assert_eq!(
        production, expected,
        "B2 生产 component_golden_v1() 必须精确承载冻结二十四路预算与失败保留"
    );
    assert_eq!(
        production.content_sha256().expect("重算 B2 生产策略身份"),
        expected.content_sha256().expect("重算冻结预期策略身份"),
        "B2 生产策略身份必须等于冻结预算机械映射身份"
    );
}

#[test]
#[ignore = "正式 COMPONENT_GOLDEN_V1 单试次必须由 cgroup 外层编排完整生命周期证据"]
fn task441_component_golden_formal_trial_orchestrator() {
    let selector = FormalTrialSelectorV1::from_environment();
    let output_root = formal_existing_output_root_from_environment();
    let build_group = formal_build_group_v1();
    let run_nonce = formal_run_nonce_v1();
    assert!(
        selector.is_valid_for_build_group(&build_group),
        "B1 只允许 S1/P1/P2/S2/S3/P3，B2 只允许 VERIFY_SERIAL/VERIFY_PARALLEL"
    );
    let context = formal_fixed_context_v1(&output_root);
    validate_current_release_binary_v1(&context);
    let build_group_identity = match build_group.as_str() {
        "B1" => MeasurementBuildGroupV1::B1,
        "B2" => MeasurementBuildGroupV1::B2,
        _ => unreachable!("formal_build_group_v1 已验证构建组"),
    };
    let (memory_max_bytes, memory_swap_max_bytes) = match build_group_identity {
        MeasurementBuildGroupV1::B1 => (
            FORMAL_B1_MEMORY_MAX_BYTES_V1,
            FORMAL_B1_MEMORY_SWAP_MAX_BYTES_V1,
        ),
        MeasurementBuildGroupV1::B2 => {
            let frozen: FrozenResourceBudgetsV1 =
                read_json_with_sha256(&output_root.join(FORMAL_FROZEN_BUDGETS_FILE_V1));
            (frozen.process_memory_max_bytes, 0)
        }
    };
    let token = sha256_bytes(
        format!(
            "{build_group}|{}|{run_nonce}|{}|{}",
            selector.label(),
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("系统时间不得早于 Unix epoch")
                .as_nanos()
        )
        .as_bytes(),
    )
    .0;
    let handshake_root = output_root
        .join("lifecycle-handshakes")
        .join(format!("{}-{token}", selector.label()));
    fs::create_dir_all(&handshake_root).expect("创建正式生命周期唯一握手根");
    assert_eq!(
        fs::read_dir(&handshake_root)
            .expect("读取正式生命周期握手根")
            .count(),
        0,
        "正式生命周期唯一握手根必须为空"
    );
    let unit = format!(
        "sim-component-golden-{}-{}-{}.service",
        build_group.to_ascii_lowercase(),
        selector.label().to_ascii_lowercase().replace('_', "-"),
        &token[..16]
    );
    start_formal_trial_service_v1(
        &unit,
        selector,
        &output_root,
        &build_group,
        &run_nonce,
        &handshake_root,
        &token,
        &context.release_unit_test_binary,
        memory_max_bytes,
        memory_swap_max_bytes,
    );
    let _cleanup = FormalSystemdUnitCleanupV1(unit.clone());
    let ready_path = handshake_root.join("ready.json");
    let started = Instant::now();
    loop {
        if formal_json_and_sidecar_exist_v1(&ready_path) {
            break;
        }
        let state = parse_systemd_show_v1(&unit);
        let active_state = required_systemd_property_v1(&state, "ActiveState");
        assert!(
            !matches!(active_state, "failed" | "inactive"),
            "正式试次在 ready 前退出：{}",
            formal_systemd_failure_diagnostic_v1(&unit)
        );
        assert!(
            started.elapsed() < Duration::from_secs(30 * 60),
            "正式试次未在三十分钟内写出主 JSON 与 ready 握手"
        );
        thread::sleep(Duration::from_millis(100));
    }

    let ready: FormalLifecycleReadyV1 = read_json_with_sha256(&ready_path);
    assert_eq!(ready.schema_version, FORMAL_LIFECYCLE_HANDSHAKE_SCHEMA_V1);
    assert_eq!(ready.token, token);
    assert_eq!(ready.selector_label, selector.label());
    let active = parse_systemd_show_v1(&unit);
    assert_eq!(
        required_systemd_property_v1(&active, "ActiveState"),
        "active"
    );
    assert_eq!(required_systemd_property_v1(&active, "SubState"), "running");
    let main_pid =
        u32::try_from(systemd_u64_v1(&active, "MainPID")).expect("systemd MainPID 适合 u32");
    assert_eq!(main_pid, ready.pid, "ready PID 必须等于 systemd MainPID");
    let invocation_id = required_systemd_property_v1(&active, "InvocationID").to_owned();
    let control_group = required_systemd_property_v1(&active, "ControlGroup").to_owned();
    let cgroup_root =
        fs::canonicalize(Path::new("/sys/fs/cgroup").join(control_group.trim_start_matches('/')))
            .expect("规范化子试次 cgroup 路径");
    let orchestrator_cgroup =
        fs::canonicalize(cgroup_v2_path()).expect("规范化生命周期外层 cgroup 路径");
    assert!(
        orchestrator_is_outside_trial_cgroup_v1(&orchestrator_cgroup, &cgroup_root),
        "生命周期外层编排进程不得等于或位于子试次 cgroup 之下"
    );
    let trial_path = output_root
        .join("trials")
        .join(selector.label())
        .join(FORMAL_TRIAL_FILE_V1);
    let trial_bytes = fs::read(&trial_path).expect("外层读取主试次 JSON");
    let trial_sha256 = sha256_bytes(&trial_bytes);
    assert_eq!(ready.main_trial_json_sha256, trial_sha256);
    assert_eq!(
        ready.main_trial_json_bytes,
        u64::try_from(trial_bytes.len()).expect("主试次 JSON 字节数适合 u64")
    );
    let persisted: PersistedFormalTrialV1 = read_json_with_sha256(&trial_path);
    validate_persisted_formal_trial_v1(&output_root, selector, &persisted);
    assert_eq!(ready.run_id, persisted.run_id);
    let (measurement, _) = persisted_trial_measurement_and_receipt_v1(&persisted);
    assert_eq!(measurement.run_id, ready.run_id);

    let final_process = procfs_snapshot_for_pid_v1(main_pid);
    let final_cgroup_v2 = cgroup_v2_snapshot_at_v1(&cgroup_root);
    atomic_write_text(&handshake_root.join("ack.txt"), &format!("{token}\n"));
    let exit_started = Instant::now();
    let final_systemd = loop {
        let state = parse_systemd_show_v1(&unit);
        let active_state = required_systemd_property_v1(&state, "ActiveState");
        let sub_state = required_systemd_property_v1(&state, "SubState");
        if active_state == "active" && sub_state == "exited" {
            break state;
        }
        assert!(
            active_state != "failed",
            "正式试次 systemd 终态失败：{}",
            formal_systemd_failure_diagnostic_v1(&unit)
        );
        assert!(
            exit_started.elapsed() < Duration::from_secs(2 * 60),
            "正式试次收到 ack 后未在两分钟内正常退出"
        );
        thread::sleep(Duration::from_millis(20));
    };
    let systemd = finalized_systemd_lifecycle_result_v1(&invocation_id, &final_systemd);
    let fixed_context_path =
        output_root.join(format!("{build_group}-{FORMAL_FIXED_CONTEXT_FILE_V1}"));
    let fixed_context_file_sha256 =
        sha256_bytes(&fs::read(&fixed_context_path).expect("读取构建组固定上下文文件"));
    let evidence = TrialLifecycleTailEvidenceV1 {
        schema_version: TRIAL_LIFECYCLE_TAIL_EVIDENCE_SCHEMA_V1.to_owned(),
        build_group: build_group_identity,
        suite_measurement_root_sha256: context.filesystem.measurement_root_sha256.clone(),
        fixed_context_file_sha256: fixed_context_file_sha256.clone(),
        measurement_run_identity: selector.measurement_identity(),
        selector_label: selector.label().to_owned(),
        run_id: persisted.run_id.clone(),
        build: context.build.clone(),
        release_binary_sha256: context.build.release_binary_sha256.clone(),
        systemd_unit: unit.clone(),
        systemd_invocation_id: invocation_id,
        systemd_control_group: control_group,
        cgroup_path_sha256: sha256_bytes(cgroup_root.to_string_lossy().as_bytes()),
        main_pid,
        main_trial_json_sha256: trial_sha256.clone(),
        main_trial_json_bytes: u64::try_from(trial_bytes.len())
            .expect("主试次 JSON 字节数适合 u64"),
        final_process,
        final_cgroup_v2,
        systemd,
    };
    validate_lifecycle_tail_binding_v1(
        &evidence,
        &TrialLifecycleTailBindingV1 {
            build_group: build_group_identity,
            measurement_run_identity: selector.measurement_identity(),
            selector_label: selector.label(),
            run_id: &persisted.run_id,
            environment: &measurement.environment,
            runtime_hard_guards: measurement.runtime_hard_guards,
            last_resource_snapshot: &persisted_trial_sealed_resource_evidence_v1(&persisted)
                .postseal_resource_snapshot,
            fixed_context_file_sha256: &fixed_context_file_sha256,
            main_trial_json_sha256: &trial_sha256,
            main_trial_json_bytes: u64::try_from(trial_bytes.len())
                .expect("主试次 JSON 字节数适合 u64"),
        },
    )
    .expect("完整试次生命周期尾部证据必须绑定真实主试次与运行硬门");
    let lifecycle_path = output_root
        .join("trials")
        .join(selector.label())
        .join(FORMAL_LIFECYCLE_TAIL_FILE_V1);
    let lifecycle_sha256 = atomic_write_json_with_sha256(&lifecycle_path, &evidence);
    eprintln!(
        "COMPONENT_GOLDEN_V1 lifecycle_tail={} sha256={} systemd_memory_peak_bytes={}",
        lifecycle_path.display(),
        lifecycle_sha256.0,
        evidence.systemd.memory_peak_bytes
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "正式 COMPONENT_GOLDEN_V1 单试次由已编译发布测试二进制在独立 scope 中执行"]
async fn task441_component_golden_formal_trial_runner() {
    let selector = FormalTrialSelectorV1::from_environment();
    let output_root = formal_output_root_from_environment();
    let build_group = formal_build_group_v1();
    assert!(
        selector.is_valid_for_build_group(&build_group),
        "正式子 runner 拒绝构建组与 selector 错配"
    );
    let fixed_context = formal_fixed_context_v1(&output_root);
    let implementation_identity = implementation_identity_from_fixed_context_v1(&fixed_context);
    let rules = load_formal_historical_rules_v1(&fixed_context, &implementation_identity);
    let input = GoldenInputV1::new();
    let environment =
        formal_measurement_environment_from_fixed_context_v1(&output_root, input.stream_sha256);
    let trial_directory = output_root.join("trials").join(selector.label());
    assert!(
        !trial_directory.exists(),
        "每个正式试次必须使用全新唯一目录：{}",
        trial_directory.display()
    );
    fs::create_dir_all(&trial_directory).expect("创建正式唯一试次目录");
    let run_id = formal_run_id_v1(
        &fixed_context.filesystem.measurement_root_sha256,
        selector.label(),
    );
    let (resource_policy, frozen_budgets_sha256) = match selector {
        FormalTrialSelectorV1::Component(_) => {
            (SimulationResourcePolicyV1::component_golden_v1(), None)
        }
        FormalTrialSelectorV1::Verification(_) => {
            let frozen: FrozenResourceBudgetsV1 =
                read_json_with_sha256(&output_root.join(FORMAL_FROZEN_BUDGETS_FILE_V1));
            let sha256 = frozen.content_sha256().expect("重算冻结预算内容身份");
            let expected = frozen
                .to_resource_policy_v1()
                .expect("把冻结预算转换为预期正式硬限策略");
            let production = SimulationResourcePolicyV1::component_golden_v1();
            assert_eq!(
                production, expected,
                "冻结后复核必须由 B2 生产 component_golden_v1() 精确承载已冻结二十四路预算与失败保留"
            );
            (production, Some(sha256))
        }
    };
    let raw = run_golden_trial_v1(
        selector.mode() == ComponentGoldenMeasurementModeV1::Parallel,
        selector.measurement_identity(),
        run_id.clone(),
        trial_directory.join("runtime"),
        rules,
        environment,
        resource_policy,
    )
    .await;
    let persisted =
        persisted_formal_trial_v1(selector, &trial_directory, &raw, frozen_budgets_sha256);
    let trial_path = trial_directory.join(FORMAL_TRIAL_FILE_V1);
    let output_sha256 = atomic_write_json_with_sha256(&trial_path, &persisted);
    eprintln!(
        "COMPONENT_GOLDEN_V1 trial={} run_id={} output={} sha256={}",
        selector.label(),
        run_id,
        trial_path.display(),
        output_sha256.0
    );
    drop(persisted);
    drop(raw);
    publish_lifecycle_ready_and_wait_for_ack_v1(selector, &run_id, &trial_path, output_sha256);
}

#[test]
#[ignore = "正式 COMPONENT_GOLDEN_V1 聚合由已编译发布测试二进制显式执行"]
fn task441_component_golden_formal_report_aggregator() {
    let phase = std::env::var("SIM_COMPONENT_GOLDEN_AGGREGATE_PHASE")
        .expect("正式聚合缺少 SIM_COMPONENT_GOLDEN_AGGREGATE_PHASE");
    assert!(matches!(phase.as_str(), "FREEZE" | "FINALIZE"));
    let output_root = formal_output_root_from_environment();
    let input = GoldenInputV1::new();
    let b1_context: FormalMeasurementFixedContextV1 =
        read_json_with_sha256(&output_root.join(format!("B1-{FORMAL_FIXED_CONTEXT_FILE_V1}")));
    validate_fixed_context_artifacts_v1(
        &output_root,
        "B1",
        &b1_context,
        &input.stream_sha256,
        phase == "FREEZE",
    );
    if phase == "FREEZE" {
        validate_current_release_binary_v1(&b1_context);
    }
    let mut trials = BTreeMap::new();
    let mut b1_lifecycle_tail_evidence = BTreeMap::new();
    for trial_id in COMPONENT_GOLDEN_TRIAL_ORDER_V1 {
        let selector = FormalTrialSelectorV1::Component(trial_id);
        let persisted = read_persisted_formal_trial_v1(&output_root, selector);
        assert_eq!(
            persisted.run_id,
            formal_run_id_v1(
                &b1_context.filesystem.measurement_root_sha256,
                selector.label(),
            ),
            "B1 正式试次必须使用当前套件固定宽度的运行身份"
        );
        let lifecycle = read_formal_lifecycle_tail_v1(
            &output_root,
            selector,
            MeasurementBuildGroupV1::B1,
            "B1",
            &persisted,
        );
        let PersistedFormalTrialReportV1::Component(report) = &persisted.trial else {
            panic!("六次预冻结目录必须只含 ComponentGoldenTrialReportV1")
        };
        assert!(trials.insert(trial_id, report.clone()).is_none());
        assert!(
            b1_lifecycle_tail_evidence
                .insert(trial_id, lifecycle)
                .is_none()
        );
    }
    let first_measurement = trials
        .get(&ComponentGoldenTrialIdV1::S1)
        .and_then(|trial| trial.preseal_measurement.as_ref())
        .expect("S1 必须有封口前测量正文");
    assert_eq!(first_measurement.environment.host, b1_context.host);
    assert_eq!(first_measurement.environment.build, b1_context.build);
    let mut cgroup_paths = BTreeSet::new();
    for report in trials.values() {
        let measurement = report
            .preseal_measurement
            .as_ref()
            .expect("成功正式试次必须有封口前测量正文");
        assert_eq!(measurement.environment.host, b1_context.host);
        assert_eq!(measurement.environment.build, b1_context.build);
        assert_eq!(
            measurement.environment.filesystem.measurement_root_sha256,
            b1_context.filesystem.measurement_root_sha256
        );
        assert_eq!(
            measurement.environment.filesystem.mount_identity_sha256,
            b1_context.filesystem.mount_identity_sha256
        );
        assert_eq!(
            measurement.environment.filesystem.filesystem_type,
            b1_context.filesystem.filesystem_type
        );
        assert_eq!(
            measurement.environment.filesystem.block_size_bytes,
            b1_context.filesystem.block_size_bytes
        );
        assert_eq!(
            measurement.environment.filesystem.total_bytes,
            b1_context.filesystem.total_bytes
        );
        assert_eq!(
            measurement.environment.input_stream_sha256,
            b1_context.input_stream_sha256
        );
        assert_eq!(
            measurement.environment.runner_sha256,
            b1_context.runner_sha256
        );
        assert_eq!(
            measurement.environment.cache_precondition,
            b1_context.cache_precondition
        );
        assert_eq!(
            measurement.environment.cache_precondition_sha256,
            b1_context.cache_precondition_sha256
        );
        assert!(cgroup_paths.insert(measurement.environment.cgroup_path_sha256.clone()));
        assert_eq!(
            measurement.runtime_hard_guards,
            first_measurement.runtime_hard_guards
        );
        assert_eq!(
            measurement.workload_contract_sha256,
            first_measurement.workload_contract_sha256
        );
    }
    assert_eq!(
        cgroup_paths.len(),
        6,
        "六个初测试次必须来自六个不同 cgroup 路径"
    );
    let observations = derive_resource_budget_observations_v1(&trials)
        .expect("从六试次强类型封口后资源证据推导 Pmax 与 Amax");
    let (_, _, process_memory_peak_bytes) = process_peaks_v1(&b1_lifecycle_tail_evidence);
    let independent_failure_reserve_probe: IndependentFailureReserveProbeV1 =
        read_json_with_sha256(&output_root.join(FORMAL_FAILURE_RESERVE_FILE_V1));
    let b1_resource_policy_sha256 = &trials[&ComponentGoldenTrialIdV1::S1]
        .sealed_resource_evidence
        .as_ref()
        .expect("S1 必须有封口后资源证据")
        .resource_policy_sha256;
    validate_formal_failure_reserve_probe_v1(
        &output_root,
        &b1_context,
        b1_resource_policy_sha256,
        &formal_run_id_v1(
            &b1_context.filesystem.measurement_root_sha256,
            "FAILURE_RESERVE",
        ),
        &independent_failure_reserve_probe,
    );
    for quantity in [
        independent_failure_reserve_probe
            .observation
            .one_failure_reserve
            .evidence,
        independent_failure_reserve_probe
            .observation
            .one_failure_reserve
            .checkpoints,
        independent_failure_reserve_probe
            .observation
            .one_failure_reserve
            .runtime_root,
    ] {
        assert!(quantity.records > 0 && quantity.bytes > 0);
    }
    let failure_reserve_observation = derive_failure_reserve_observation_v1(
        &trials,
        &independent_failure_reserve_probe.observation,
    )
    .expect("从六个成功 B1 seal 与独立真实探针逐维重算失败续租观测");
    let expected_frozen = freeze_resource_budgets_v1(
        &observations,
        process_memory_peak_bytes,
        &failure_reserve_observation.maintenance_steady_high_water,
        failure_reserve_observation.one_failure_reserve,
    )
    .expect("从六个独立进程的 Pmax/Amax 与动态失败续租观测冻结资源预算");
    if phase == "FREEZE" {
        let frozen_content_sha256 = expected_frozen
            .content_sha256()
            .expect("重算冻结预算内容身份");
        let output_path = output_root.join(FORMAL_FROZEN_BUDGETS_FILE_V1);
        let file_sha256 = atomic_write_json_with_sha256(&output_path, &expected_frozen);
        eprintln!(
            "COMPONENT_GOLDEN_V1 frozen_budgets={} content_sha256={} file_sha256={}",
            output_path.display(),
            frozen_content_sha256.0,
            file_sha256.0
        );
        return;
    }

    let frozen: FrozenResourceBudgetsV1 =
        read_json_with_sha256(&output_root.join(FORMAL_FROZEN_BUDGETS_FILE_V1));
    assert_eq!(
        frozen, expected_frozen,
        "FINALIZE 必须复用查看两次复核结果前已冻结的同一预算正文"
    );
    let b2_context: FormalMeasurementFixedContextV1 =
        read_json_with_sha256(&output_root.join(format!("B2-{FORMAL_FIXED_CONTEXT_FILE_V1}")));
    validate_fixed_context_artifacts_v1(
        &output_root,
        "B2",
        &b2_context,
        &input.stream_sha256,
        true,
    );
    validate_current_release_binary_v1(&b2_context);
    assert_eq!(
        b1_context.historical_rules.semantic_rules_sha256,
        b2_context.historical_rules.semantic_rules_sha256,
        "FINALIZE 要求 B1 与 B2 固定历史规则快照的业务语义完全相同"
    );
    assert_eq!(b1_context.host, b2_context.host);
    assert_eq!(b1_context.filesystem, b2_context.filesystem);
    assert_eq!(
        b1_context.input_stream_sha256,
        b2_context.input_stream_sha256
    );
    assert_eq!(b1_context.runner_sha256, b2_context.runner_sha256);
    assert_eq!(b1_context.cache_precondition, b2_context.cache_precondition);
    assert_eq!(
        b1_context.cache_precondition_sha256,
        b2_context.cache_precondition_sha256
    );
    assert_ne!(
        b1_context.build.release_binary_sha256, b2_context.build.release_binary_sha256,
        "B2 必须是机械落入冻结生产策略后重新构建的不同 release 二进制"
    );
    assert_ne!(
        b1_context.build.source_tree_sha256, b2_context.build.source_tree_sha256,
        "B2 源树身份必须反映冻结生产策略落地"
    );

    let mut verification_trials = BTreeMap::new();
    let mut b2_lifecycle_tail_evidence = BTreeMap::new();
    for verification_id in FROZEN_BUDGET_VERIFICATION_TRIALS_V1 {
        let selector = FormalTrialSelectorV1::Verification(verification_id);
        let persisted = read_persisted_formal_trial_v1(&output_root, selector);
        assert_eq!(
            persisted.run_id,
            formal_run_id_v1(
                &b1_context.filesystem.measurement_root_sha256,
                selector.label(),
            ),
            "B2 正式复核必须使用当前套件固定宽度的运行身份"
        );
        let lifecycle = read_formal_lifecycle_tail_v1(
            &output_root,
            selector,
            MeasurementBuildGroupV1::B2,
            "B2",
            &persisted,
        );
        let PersistedFormalTrialReportV1::Verification(report) = &persisted.trial else {
            panic!("冻结后复核目录必须只含 FrozenBudgetVerificationTrialV1")
        };
        let measurement = report
            .preseal_measurement
            .as_ref()
            .expect("冻结后成功复核必须含 preseal measurement");
        assert_eq!(measurement.environment.host, b2_context.host);
        assert_eq!(measurement.environment.build, b2_context.build);
        assert!(
            verification_trials
                .insert(verification_id, report.clone())
                .is_none()
        );
        assert!(
            b2_lifecycle_tail_evidence
                .insert(verification_id, lifecycle)
                .is_none()
        );
    }

    let workload_contract = ComponentGoldenLoadContractV1::component_golden_v1();
    let workload_contract_sha256 = workload_contract
        .content_sha256()
        .expect("重算最终报告负载合同身份");
    assert_eq!(
        workload_contract_sha256,
        first_measurement.workload_contract_sha256
    );
    let (process_resident_peak_bytes, process_swap_peak_bytes, process_memory_peak_bytes) =
        process_peaks_v1(&b1_lifecycle_tail_evidence);
    let report = ComponentGoldenMeasurementReportV1 {
        schema_version: COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1.to_owned(),
        workload_contract,
        workload_contract_sha256,
        environment: first_measurement.environment.clone(),
        runtime_hard_guards: first_measurement.runtime_hard_guards,
        trials,
        independent_failure_reserve_probe: Some(independent_failure_reserve_probe),
        budget_observations: observations,
        process_resident_peak_bytes,
        process_swap_peak_bytes,
        process_memory_peak_bytes,
        frozen_budgets: frozen,
        b1_lifecycle_tail_evidence,
        b2_lifecycle_tail_evidence,
        frozen_budget_verification_trials: verification_trials,
    };
    report
        .validate_complete()
        .expect("最终 COMPONENT_GOLDEN_V1 报告必须完整闭合八个独立进程");
    let report_content_sha256 = report.content_sha256().expect("重算最终报告内容身份");
    let output_path = output_root.join(FORMAL_FINAL_REPORT_FILE_V1);
    let report_file_sha256 = atomic_write_json_with_sha256(&output_path, &report);
    let decoded: ComponentGoldenMeasurementReportV1 = read_json_with_sha256(&output_path);
    decoded
        .validate_complete()
        .expect("独立严格解码后的最终报告必须仍然完整有效");
    assert_eq!(
        decoded.content_sha256().expect("重算落盘报告内容身份"),
        report_content_sha256
    );
    atomic_write_text(
        &output_root.join(FORMAL_FINAL_REPORT_CONTENT_SHA_FILE_V1),
        &format!("{}\n", report_content_sha256.0),
    );
    eprintln!(
        "COMPONENT_GOLDEN_V1 final_report={} content_sha256={} file_sha256={}",
        output_path.display(),
        report_content_sha256.0,
        report_file_sha256.0
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "正式 COMPONENT_GOLDEN_V1 runner 由发布构建显式执行"]
async fn task441_component_golden_single_trial_executes_real_contract() {
    let output_root = formal_output_root_from_environment();
    let fixed_context = formal_fixed_context_v1(&output_root);
    let implementation_identity = implementation_identity_from_fixed_context_v1(&fixed_context);
    let rules = load_formal_historical_rules_v1(&fixed_context, &implementation_identity);
    let input = GoldenInputV1::new();
    let environment =
        formal_measurement_environment_from_fixed_context_v1(&output_root, input.stream_sha256);
    let (run_id, persistence_root) = unique_smoke_run_root(&output_root);
    let trial = run_golden_trial_v1(
        false,
        MeasurementRunIdentityV1::ComponentTrial(ComponentGoldenTrialIdV1::S1),
        run_id,
        persistence_root,
        rules,
        environment,
        SimulationResourcePolicyV1::component_golden_v1(),
    )
    .await;
    assert_eq!(trial.trajectories[0].path_deliveries, 20);
    assert_eq!(trial.trajectories[1].path_deliveries, 20);
    assert!(!trial.resource_observations.is_empty());
    assert!(trial.artifact_receipt.is_object());
    assert!(trial.elapsed_ns > 0);
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
