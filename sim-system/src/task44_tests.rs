use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use contracts::{
    AccountInstanceKeyV1, AssetIdentityV1, BookSide, CanonicalEventId, ClobMarketId, ConditionId,
    EventLineageV2, EventQualityV2, EventTimeV2, ExactDecimal, ExecutionMarketEventPayloadV3,
    ExecutionMarketEventV3, InitialL2SnapshotV2, LevelExecutionBasis, LevelExecutionTransitionV3,
    MarketDataQuality, MarketIdentityV2, MarketResolutionTerminalV3, MarketWindowV2,
    OutcomeIdentityV1, PayoutEntryV1, PriceLevelV1, QueueResultQuality, ResyncQuality, Sha256Hex,
    SourceAttributionQuality, TokenId,
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
    CancelAllOrdersDirectResponseV1, CancelAllOrdersRequestV1, CancelOrderDirectResponseV1,
    CancelOrderItemV1, CancelOrderRequestV1, CancelOrdersDirectResponseV1, CancelOrdersRequestV1,
    DownstreamDurabilityProofV1, InitialAccountStateV1, OrderDirectResponseV1, OrderQueryResultV1,
    OrderRequestV1, OrdersDirectResponseV1, OrdersFilterV1, OrdersQueryResultV1, OrdersRequestV1,
    PaperMarketEventRecoveryReadErrorV1, PaperMarketEventRecoveryReaderV1,
    PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1, PostOrdersDirectResponseV1,
    PostOrdersRequestV1, SessionRetirementFailureV1, SimBuyOrderKindV1, SimBuyOrderV1,
    SimExecutionProcessV1, SimExecutionSessionV1, SubmissionDeliveryDispositionV1,
    SubmissionDriveDispositionV1, SubmissionItemOutcomeV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, TradesDirectResponseV1, TradesFilterV1, TradesQueryResultV1,
    TradesRequestV1,
};
use crate::finality::successful_policy_for_tests;
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MillisecondBucketClosureV1,
};
use crate::measurement::{
    COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1, COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1,
    COMPONENT_GOLDEN_TRIAL_ORDER_V1, CgroupMemoryEventsV1, CgroupMemoryLimitV1, CgroupPidsLimitV1,
    CgroupV2ResourceSnapshotV1, ComponentGoldenLoadContractV1, ComponentGoldenMeasurementModeV1,
    ComponentGoldenMeasurementReportV1, ComponentGoldenRunMeasurementV1,
    ComponentGoldenTrajectoryV1, ComponentGoldenTrialIdV1, ComponentGoldenTrialReportV1,
    FROZEN_BUDGET_VERIFICATION_TRIALS_V1, FrozenBudgetVerificationTrialIdV1,
    FrozenBudgetVerificationTrialV1, FrozenResourceBudgetsV1, InternalResourceSnapshotV1,
    LogicalRollingCycleIdV1, LogicalRollingCycleV1, MEASUREMENT_LATENCY_KINDS_V1,
    MeasuredResourceSnapshotV1, MeasurementBuildIdentityV1, MeasurementCachePreconditionV1,
    MeasurementEnvironmentIdentityV1, MeasurementFilesystemIdentityV1, MeasurementHostIdentityV1,
    MeasurementLatencyKindV1, MeasurementRunIdentityV1, MeasurementRuntimeHardGuardsV1,
    MeasurementTrialOutcomeV1, ProcfsIoSnapshotV1, ProcfsProcessResourceSnapshotV1,
    RawLatencySamplesNsV1, ResourceBudgetPathV1, ResourceTransactionIncrementV1,
    SealedRunResourceEvidenceV1, SimulationRunArtifactReceiptBindingV1, ThroughputBreakdownV1,
    TrajectoryLogicalComponentV1, TrajectoryLogicalSummaryInputV1, TrajectoryRuntimeBindingV1,
    TrajectoryThroughputV1, TrajectoryTrialResultV1, TrialThroughputV1,
    derive_resource_budget_observations_v1, freeze_resource_budgets_v1,
};
use crate::platform_rules::tests::{
    historical_snapshot_for_sim_tests_with_fee_schedule,
    historical_snapshot_for_sim_tests_with_fee_schedule_and_identity,
    successful_snapshot_for_sim_tests,
};
use crate::platform_rules::{
    ImplementationIdentityV1, RecoveryProcessSnapshotRequestV1, recover_process_snapshot,
};
use crate::resource::{
    ResourceLimitDimensionV1, ResourceLimitKindV1, ResourceLimitScopeV1, ResourceLimitV1,
    ResourceQuantityV1, SimulationFailureEvidenceReserveV1, SimulationResourcePolicyV1,
    SimulationResourceUsageV1,
};
use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};
use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, RequestRegistrationDispositionV1,
    SessionError, SessionSpecV2, SimCallerRoleV1,
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

const FORMAL_FIXED_CONTEXT_FILE_V1: &str = "component-golden-fixed-context.json";
const FORMAL_FROZEN_BUDGETS_FILE_V1: &str = "frozen-resource-budgets.json";
const FORMAL_FINAL_REPORT_FILE_V1: &str = "component-golden-measurement-report.json";
const FORMAL_FINAL_REPORT_CONTENT_SHA_FILE_V1: &str =
    "component-golden-measurement-report.content.sha256";
const FORMAL_TRIAL_FILE_V1: &str = "component-golden-trial.json";
const FORMAL_FAILURE_RESERVE_FILE_V1: &str = "component-golden-failure-reserve.json";

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
}

fn sha256_sidecar_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .expect("正式 JSON 路径必须有文件名")
        .to_os_string();
    name.push(".sha256");
    path.with_file_name(name)
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

fn build_formal_fixed_context_v1(
    output_root: &Path,
    implementation_identity: &ImplementationIdentityV1,
    input_stream_sha256: Sha256Hex,
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
    let release_unit_test_binary = current_exe
        .canonicalize()
        .expect("规范化正式 release 测试二进制路径");
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
            release_binary_sha256: sha256_bytes(
                &fs::read(current_exe).expect("读取正式 release 测试二进制"),
            ),
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
    }
}

fn formal_fixed_context_path_v1(output_root: &Path) -> PathBuf {
    let group = std::env::var("SIM_COMPONENT_GOLDEN_BUILD_GROUP")
        .expect("正式 COMPONENT_GOLDEN_V1 缺少 SIM_COMPONENT_GOLDEN_BUILD_GROUP");
    assert!(
        matches!(group.as_str(), "B1" | "B2"),
        "构建组只能是 B1 或 B2"
    );
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
    context: &FormalMeasurementFixedContextV1,
    input_stream_sha256: &Sha256Hex,
    verify_current_source_tree: bool,
) {
    assert_eq!(context.schema_version, "component_golden_fixed_context_v1");
    assert_eq!(&context.input_stream_sha256, input_stream_sha256);
    assert_eq!(
        sha256_bytes(
            &fs::read(&context.release_unit_test_binary)
                .expect("FINALIZE 重新读取固定 release 测试二进制"),
        ),
        context.build.release_binary_sha256,
        "FINALIZE 必须重新核验固定 release 测试二进制 SHA-256"
    );
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
    let current_exe = std::env::current_exe()
        .expect("读取正式子进程二进制路径")
        .canonicalize()
        .expect("规范化正式子进程二进制路径");
    assert_eq!(
        current_exe, context.release_unit_test_binary,
        "正式子进程必须直接执行 bootstrap 冻结的同一测试二进制"
    );
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

#[test]
#[ignore = "正式 COMPONENT_GOLDEN_V1 固定上下文必须在八个计量 scope 外显式准备"]
fn task441_component_golden_prepare_fixed_context() {
    let output_root = formal_output_root_from_environment();
    let implementation_identity = formal_implementation_identity_from_environment();
    let input = GoldenInputV1::new();
    let context =
        build_formal_fixed_context_v1(&output_root, &implementation_identity, input.stream_sha256);
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
    let context: FormalMeasurementFixedContextV1 =
        read_json_with_sha256(&output_root.join(format!("B1-{FORMAL_FIXED_CONTEXT_FILE_V1}")));
    let implementation_identity = implementation_identity_from_fixed_context_v1(&context);
    let nonce = formal_run_nonce_v1();
    let probe_directory = output_root.join("failure-reserve-probe");
    assert!(
        !probe_directory.exists(),
        "失败保留探针必须使用全新持久目录"
    );
    fs::create_dir_all(&probe_directory).expect("创建失败保留探针目录");
    let reserve = measure_real_failure_reserve_v1(
        &format!("task44-component-golden-{nonce}-failure-reserve"),
        probe_directory.join("runtime"),
        implementation_identity,
    )
    .await;
    for quantity in [reserve.evidence, reserve.checkpoints, reserve.runtime_root] {
        assert!(quantity.records > 0 && quantity.bytes > 0);
    }
    let output_path = output_root.join(FORMAL_FAILURE_RESERVE_FILE_V1);
    let sha256 = atomic_write_json_with_sha256(&output_path, &reserve);
    eprintln!(
        "COMPONENT_GOLDEN_V1 failure_reserve={} sha256={}",
        output_path.display(),
        sha256.0
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
    let status = read_required_text("/proc/self/status", "/proc/self/status");
    let io = read_required_text("/proc/self/io", "/proc/self/io");
    let stat = read_required_text("/proc/self/stat", "/proc/self/stat");
    let stat_fields = stat
        .rsplit_once(')')
        .expect("/proc/self/stat 必须含进程名闭括号")
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let limits = read_required_text("/proc/self/limits", "/proc/self/limits");
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
        pid: std::process::id(),
        process_start_time_ticks,
        vm_rss_bytes: kib("VmRSS"),
        vm_hwm_bytes: kib("VmHWM"),
        vm_swap_bytes: kib("VmSwap"),
        thread_count: u32::try_from(parse_proc_key_value(&status, "Threads"))
            .expect("线程数适合 u32"),
        open_file_descriptor_count: u64::try_from(
            fs::read_dir("/proc/self/fd")
                .expect("读取 /proc/self/fd")
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
    let events = read_required_text(root.join("memory.events"), "memory.events");
    let cpu = read_required_text(root.join("cpu.stat"), "cpu.stat");
    let event = |key| parse_space_key_value(&events, key);
    let peak = |name: &str| {
        let path = root.join(name);
        path.is_file().then(|| cgroup_u64(&root, name))
    };
    CgroupV2ResourceSnapshotV1 {
        memory_current_bytes: cgroup_u64(&root, "memory.current"),
        memory_peak_bytes: peak("memory.peak"),
        memory_limit: cgroup_memory_limit(&root, "memory.max"),
        memory_swap_current_bytes: cgroup_u64(&root, "memory.swap.current"),
        memory_swap_peak_bytes: peak("memory.swap.peak"),
        memory_swap_limit: cgroup_memory_limit(&root, "memory.swap.max"),
        memory_events: CgroupMemoryEventsV1 {
            low: event("low"),
            high: event("high"),
            max: event("max"),
            oom: event("oom"),
            oom_kill: event("oom_kill"),
            oom_group_kill: event("oom_group_kill"),
        },
        cpu_usage_usec: parse_space_key_value(&cpu, "usage_usec"),
        pids_current: cgroup_u64(&root, "pids.current"),
        pids_limit: cgroup_pids_limit(&root),
    }
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
    canonical_outputs: Arc<Mutex<Vec<Value>>>,
}

impl GoldenCapturesV1 {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let post_orders = Arc::clone(&self.post_orders);
        let cancel_order = Arc::clone(&self.cancel_order);
        let cancel_orders = Arc::clone(&self.cancel_orders);
        let cancel_all = Arc::clone(&self.cancel_all);
        let order = Arc::clone(&self.order);
        let orders = Arc::clone(&self.orders);
        let trades = Arc::clone(&self.trades);
        let outputs = std::array::from_fn::<_, 11, _>(|_| Arc::clone(&self.canonical_outputs));
        let [o0, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10] = outputs;
        SubmissionResponseBridgeV1::new(
            move |response| {
                record_bridge_output(&o0, "POST_ORDER", response.as_ref());
                post_order
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o1, "POST_ORDERS", response.as_ref());
                post_orders
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o2, "CANCEL_ORDER", response.as_ref());
                cancel_order
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o3, "CANCEL_ORDERS", response.as_ref());
                cancel_orders
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o4, "CANCEL_ALL_ORDERS", response.as_ref());
                cancel_all
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o5, "ORDER", response.as_ref());
                order
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o6, "ORDERS", response.as_ref());
                orders
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o7, "TRADES", response.as_ref());
                trades
                    .lock()
                    .expect("正式 runner 响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o8, "ORDER_LIFECYCLE", response.as_ref());
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o9, "SETTLEMENT_LIFECYCLE", response.as_ref());
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                record_bridge_output(&o10, "TRADE_LIFECYCLE", response.as_ref());
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
        )
    }
}

fn record_bridge_output<T: Serialize>(output: &Mutex<Vec<Value>>, kind: &str, value: &T) {
    output
        .lock()
        .expect("正式 runner 输出锁")
        .push(serde_json::json!({
            "kind": kind,
            "body": value,
        }));
}

struct GoldenTrajectoryStateV1 {
    trajectory: char,
    control: crate::session::SessionCreationControlResultV2,
    session: SimExecutionSessionV1,
    captures: GoldenCapturesV1,
    caller_sequence: u64,
    path_deliveries: u16,
    top_level_calls: u16,
    request_batch_items: u64,
    unique_fills: u64,
    affected_orders: u64,
    event_latency_ns: Vec<u64>,
    request_latency_ns: Vec<u64>,
    query_latency_ns: Vec<u64>,
    checkpoint_latency_ns: Vec<u64>,
    retention_latency_ns: Vec<u64>,
    trajectory_started: Instant,
    first_page_count: usize,
    continuation_page_count: usize,
    first_market_hot: Option<Value>,
    first_market_compact: Option<Value>,
    first_market_expired: Option<Value>,
    second_market_hot: Option<Value>,
    second_market_compact: Option<Value>,
    second_market_expired: Option<Value>,
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
        control: crate::session::SessionCreationControlResultV2,
        session: SimExecutionSessionV1,
        captures: GoldenCapturesV1,
    ) -> Self {
        Self {
            trajectory,
            control,
            session,
            captures,
            caller_sequence: 0,
            path_deliveries: 0,
            top_level_calls: 0,
            request_batch_items: 0,
            unique_fills: 0,
            affected_orders: 0,
            event_latency_ns: Vec::new(),
            request_latency_ns: Vec::new(),
            query_latency_ns: Vec::new(),
            checkpoint_latency_ns: Vec::new(),
            retention_latency_ns: Vec::new(),
            trajectory_started: Instant::now(),
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
        let disposition = state
            .session
            .drive_private_delivery_through(through_utc_ms)
            .expect("按私有序号排空黄金反馈");
        match disposition {
            SubmissionDeliveryDispositionV1::Delivered => continue,
            SubmissionDeliveryDispositionV1::NoVisibleResponse => break,
            other => panic!("黄金桥不应阻塞或关闭，实际为 {other:?}"),
        }
    }
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

fn throughput_rate_milli(completed: u64, elapsed_ns: u64) -> u64 {
    u64::try_from(
        u128::from(completed)
            .checked_mul(1_000_000_000_000)
            .expect("黄金吞吐分子溢出")
            / u128::from(elapsed_ns.max(1)),
    )
    .expect("黄金吞吐速率适合 u64")
}

fn value_array_len(value: &Value, key: &str) -> u64 {
    u64::try_from(
        value
            .get(key)
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("黄金诊断缺少数组 {key}"))
            .len(),
    )
    .expect("黄金诊断数组长度适合 u64")
}

fn diagnostic_quantity(value: &Value, hot: bool) -> ResourceQuantityV1 {
    let prefix = if hot { "hot" } else { "compact" };
    ResourceQuantityV1::new(
        value_array_len(value, &format!("{prefix}_orders"))
            + value_array_len(value, &format!("{prefix}_trades")),
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
    usages: &[SimulationResourceUsageV1],
) -> Vec<ResourceTransactionIncrementV1> {
    let mut previous = BTreeMap::<ResourceBudgetPathV1, ResourceQuantityV1>::new();
    usages
        .iter()
        .enumerate()
        .map(|(index, usage)| {
            let current = budget_quantities(usage);
            let increments = current
                .iter()
                .map(|(path, quantity)| {
                    let before = previous.get(path).copied().unwrap_or_default();
                    (
                        *path,
                        ResourceQuantityV1::new(
                            quantity.records.saturating_sub(before.records),
                            quantity.bytes.saturating_sub(before.bytes),
                        ),
                    )
                })
                .collect();
            previous = current;
            ResourceTransactionIncrementV1 {
                transaction_index: u64::try_from(index).expect("逐事务资源序号适合 u64"),
                increments,
            }
        })
        .collect()
}

fn latency_samples_v1(samples: Vec<u64>) -> RawLatencySamplesNsV1 {
    RawLatencySamplesNsV1::from_samples(samples).expect("黄金延迟类别必须有真实纳秒样本")
}

fn trajectory_result_v1(
    completed: &GoldenTrajectoryCompletedV1,
    workload_contract_sha256: &Sha256Hex,
    trial_elapsed_ns: u64,
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
        completed_work_items_per_second_milli: throughput_rate_milli(
            u64::from(completed.path_deliveries) + u64::from(completed.top_level_calls),
            completed.elapsed_ns,
        ),
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
            latency_samples_v1(vec![trial_elapsed_ns]),
        ),
    ]);
    let cycle = |cycle_id, indexes: [u64; 3], completion_ns, hot: &Value, compact: &Value| {
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
        let next = result.throughput.breakdown;
        breakdown.request_batch_items += next.request_batch_items;
        breakdown.unique_fills += next.unique_fills;
        breakdown.affected_orders += next.affected_orders;
        breakdown.order_feedback += next.order_feedback;
        breakdown.trade_feedback += next.trade_feedback;
        breakdown.settlement_feedback += next.settlement_feedback;
        breakdown.query_calls += next.query_calls;
        breakdown.query_pages += next.query_pages;
        breakdown.query_return_rows += next.query_return_rows;
    }
    TrialThroughputV1 {
        completed_path_deliveries: 40,
        completed_top_level_calls: 38,
        breakdown,
        elapsed_ns,
        completed_work_items_per_second_milli: throughput_rate_milli(78, elapsed_ns),
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

fn diagnostic_value(session: &SimExecutionSessionV1, condition_id: &str) -> Value {
    let diagnostic = session.task41_diagnostics_for_tests(condition_id);
    serde_json::json!({
        "hot_orders": diagnostic.hot_orders,
        "compact_orders": diagnostic.compact_orders,
        "hot_trades": diagnostic.hot_trades,
        "compact_trades": diagnostic.compact_trades,
        "pending_direct": diagnostic.pending_direct_response_count,
        "pending_lifecycle": diagnostic.pending_lifecycle_feedback_count,
        "query_roots": diagnostic.query_root_body_count,
        "issued_cursors": diagnostic.issued_cursor_count,
        "retained_rebate_fills": diagnostic.retained_rebate_fill_state_count,
    })
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

    let cancel_started = Instant::now();
    let cancel_context = state.next_context(profile, "cancel-order", 350_000);
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
    state.record_call(cancel_started, false);

    let cancel_batch_started = Instant::now();
    let cancel_batch_context = state.next_context(profile, "cancel-orders", 355_000);
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
    state.record_call(cancel_batch_started, false);

    let cancel_all_started = Instant::now();
    let cancel_all_context = state.next_context(profile, "cancel-all-orders", 360_000);
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
    state.record_call(cancel_all_started, false);

    let order_started = Instant::now();
    let order_context = state.next_context(profile, "order-after-queue", 365_000);
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
    state.record_call(order_started, true);

    let trades_started = Instant::now();
    let trades_context = state.next_context(profile, "trades-after-queue", 370_000);
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
    state.record_call(trades_started, true);
    close_empty_prefix_through(state, true, 380_000, input.groups[4].source_at_utc_ms);
    let started = Instant::now();
    assert_eq!(
        state.session.drive_next_execution_action_through(380_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    state.checkpoint_latency_ns.push(elapsed_ns(started));
    close_empty_prefix_through(state, false, 380_000, input.groups[4].received_at_utc_ms);
    drain_private_through(state, 380_000);

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
    state.first_market_hot = Some(diagnostic_value(&state.session, condition_id));
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
    state.first_market_compact = Some(diagnostic_value(&state.session, condition_id));
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
    state.first_market_expired = Some(diagnostic_value(&state.session, condition_id));
    let expired_sample_index = capture_golden_resource_snapshot(process, sampler);
    state.first_market_sample_indexes =
        Some([hot_sample_index, compact_sample_index, expired_sample_index]);
    state.first_market_completion_ns = Some(elapsed_ns(state.trajectory_started));
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
    state.second_market_hot = Some(diagnostic_value(&state.session, condition_id));
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
    state.second_market_compact = Some(diagnostic_value(&state.session, condition_id));
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
    state.second_market_expired = Some(diagnostic_value(&state.session, condition_id));
    let expired_sample_index = capture_golden_resource_snapshot(process, sampler);
    state.second_market_sample_indexes =
        Some([hot_sample_index, compact_sample_index, expired_sample_index]);
    state.second_market_completion_ns = Some(elapsed_ns(state.trajectory_started));

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

    let account = state.session.account_snapshot_for_tests();
    let first_market = diagnostic_value(&state.session, &input.markets[0].market().condition_id.0);
    let second_market = diagnostic_value(&state.session, condition_id);
    let outputs = state
        .captures
        .canonical_outputs
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
    let mut query_pages = 0_u64;
    let mut query_return_rows = 0_u64;
    for response in &order_queries {
        if matches!(response.result, OrderQueryResultV1::Found(_)) {
            query_pages += 1;
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
                .filter(|output| output.get("kind").and_then(Value::as_str) == Some(kind))
                .count(),
        )
        .expect("反馈计数适合 u64")
    };
    let throughput_breakdown = ThroughputBreakdownV1 {
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
    };
    assert_eq!(throughput_breakdown.request_batch_items, 106);
    assert_eq!(throughput_breakdown.unique_fills, 120);
    assert_eq!(throughput_breakdown.affected_orders, 105);
    assert_eq!(throughput_breakdown.query_calls, 6);
    assert!(throughput_breakdown.query_return_rows >= 102);

    let logical_components = BTreeMap::from([
        (
            "account",
            sha256_serializable("TASK44_ACCOUNT_V1", &account),
        ),
        (
            "markets",
            sha256_serializable("TASK44_MARKETS_V1", &(first_market, second_market)),
        ),
        (
            "orders",
            sha256_serializable(
                "TASK44_ORDERS_V1",
                &outputs
                    .iter()
                    .filter(|output| {
                        matches!(
                            output.get("kind").and_then(Value::as_str),
                            Some(
                                "POST_ORDER"
                                    | "POST_ORDERS"
                                    | "CANCEL_ORDER"
                                    | "CANCEL_ORDERS"
                                    | "CANCEL_ALL_ORDERS"
                                    | "ORDER"
                                    | "ORDERS"
                            )
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        ),
        (
            "trades",
            sha256_serializable(
                "TASK44_TRADES_V1",
                &outputs
                    .iter()
                    .filter(|output| {
                        matches!(
                            output.get("kind").and_then(Value::as_str),
                            Some("TRADES" | "TRADE_LIFECYCLE")
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        ),
        (
            "request_results",
            sha256_serializable("TASK44_REQUEST_RESULTS_V1", &outputs),
        ),
        (
            "feedback",
            sha256_serializable(
                "TASK44_FEEDBACK_V1",
                &outputs
                    .iter()
                    .filter(|output| {
                        output
                            .get("kind")
                            .and_then(Value::as_str)
                            .is_some_and(|kind| kind.contains("LIFECYCLE"))
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        ),
        (
            "finality",
            sha256_serializable(
                "TASK44_FINALITY_V1",
                &outputs
                    .iter()
                    .filter(|output| {
                        output.get("kind").and_then(Value::as_str) == Some("TRADE_LIFECYCLE")
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        ),
        (
            "retention",
            sha256_serializable(
                "TASK44_RETENTION_V1",
                &(
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
    let elapsed = elapsed_ns(state.trajectory_started);
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
    let mut checkpoint_latency_ns = state.checkpoint_latency_ns;
    checkpoint_latency_ns.push(elapsed_ns(completion_started));
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
    first_market_hot: Value,
    first_market_compact: Value,
    first_market_expired: Value,
    second_market_hot: Value,
    second_market_compact: Value,
    second_market_expired: Value,
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
    resource_usages: Vec<crate::resource::SimulationResourceUsageV1>,
    resource_snapshots: Vec<MeasuredResourceSnapshotV1>,
    sealed_resource_observation: SimulationResourceObservationV1,
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
    implementation_identity: ImplementationIdentityV1,
    environment: MeasurementEnvironmentIdentityV1,
    resource_policy: SimulationResourcePolicyV1,
) -> GoldenTrialRawV1 {
    let input = Arc::new(GoldenInputV1::new());
    let profile = load_embedded_proxy_latency_profile().expect("加载正式黄金代理时延配置档");
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule_and_identity(
        "0.07",
        1,
        implementation_identity,
    )
    .await;
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
            .create_session(spec.clone(), captures.bridge())
            .expect("创建正式黄金轨迹");
        process
            .activate_session(activation_request(&spec, &created.control))
            .expect("激活正式黄金轨迹");
        prepared.push(GoldenTrajectoryStateV1::new(
            trajectory,
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
    let preseal_resource_usages = process
        .resource_transaction_usages_for_tests()
        .expect("读取黄金真实逐事务资源使用量");
    let trajectories: [GoldenTrajectoryCompletedV1; 2] = trajectories
        .try_into()
        .unwrap_or_else(|_| panic!("正式黄金必须完成两条轨迹"));
    assert_eq!(trajectories[0].trajectory, 'a');
    assert_eq!(trajectories[1].trajectory, 'b');
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
            let result =
                trajectory_result_v1(completed, &workload_contract_sha256, trial_elapsed_ns);
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
            &preseal_resource_usages,
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
    let resource_usages = process
        .resource_transaction_usages_for_tests()
        .expect("读取黄金完整证据前缀的真实逐事务资源使用量");
    assert_eq!(
        u64::try_from(resource_usages.len()).expect("证据数量适合 u64"),
        audit.through_commit_sequence,
        "逐事务资源使用量必须覆盖完整封口证据前缀"
    );
    let sealed_transaction_resource_increments =
        transaction_resource_increments_v1(&resource_usages);
    let postseal_snapshot = sampler
        .lock()
        .expect("黄金资源采样锁")
        .snapshots
        .last()
        .expect("黄金封口后必须有资源样本")
        .clone();
    let last_durable_transaction = &resource_usages
        .last()
        .expect("封口证据前缀必须有最后一项真实资源使用量")
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
        resource_usages,
        resource_snapshots: Arc::try_unwrap(sampler)
            .unwrap_or_else(|_| panic!("黄金工作线程结束后不得保留采样器引用"))
            .into_inner()
            .expect("取回黄金资源采样器")
            .snapshots,
        sealed_resource_observation,
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

async fn measure_real_failure_reserve_v1(
    run_id: &str,
    persistence_root: PathBuf,
    implementation_identity: ImplementationIdentityV1,
) -> SimulationFailureEvidenceReserveV1 {
    assert!(!persistence_root.exists(), "失败事务探针必须使用全新持久根");
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule_and_identity(
        "0.07",
        1,
        implementation_identity,
    )
    .await;
    let profile = load_embedded_proxy_latency_profile().expect("加载失败事务探针时延配置档");
    let scenario = scenario();
    let intent = SimulationRunIntentBindingV1::try_new(
        rules.snapshot().provenance.implementation_identity.clone(),
        SimulationWorkloadKindV1::RealVertical,
        SimulationDeterministicScriptContractV1::RealVerticalV1,
    )
    .expect("构造失败事务探针运行意图");
    let mut policy = SimulationResourcePolicyV1::component_golden_v1();
    policy.run.input_queue = ResourceLimitV1::new(1, policy.run.input_queue.max_bytes);
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
        .expect("读取失败事务提交前实际资源")
        .durable;
    let transactions_before = process
        .resource_transaction_usages_for_tests()
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
        .resource_transaction_usages_for_tests()
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
    let failure_candidate = &transactions_after
        .last()
        .expect("失败探针必须有最后一项事务资源")
        .durable;
    SimulationFailureEvidenceReserveV1 {
        evidence: measured_positive_quantity_delta_v1(
            failure_candidate.evidence,
            before.evidence,
            "evidence",
        ),
        checkpoints: measured_positive_quantity_delta_v1(
            failure_candidate.checkpoints,
            before.checkpoints,
            "checkpoints",
        ),
        runtime_root: measured_positive_quantity_delta_v1(
            failure_candidate.runtime_root,
            before.runtime_root,
            "runtime_root",
        ),
    }
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

fn read_complete_resource_usage_prefix_v1(
    runtime_root: &Path,
    expected_receipt: &SimulationRunArtifactReceiptBindingV1,
) -> (Vec<SimulationResourceUsageV1>, u64) {
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
    let mut usages = Vec::with_capacity(members.len());
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
        let usage: SimulationResourceUsageV1 = serde_json::from_value(
            envelope
                .pointer("/body/resource_usage")
                .cloned()
                .expect("每项正式 evidence 必须含 resource_usage"),
        )
        .expect("严格解码每项 evidence 的强类型资源使用量");
        usages.push(usage);
        predecessor = Some(actual_sha256);
    }
    assert_eq!(
        predecessor.as_ref(),
        Some(&expected_receipt.evidence_head_sha256),
        "完整 evidence 前缀末项必须等于回执证据头"
    );
    (usages, total_bytes)
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
    let (resource_usages, evidence_bytes) =
        read_complete_resource_usage_prefix_v1(&runtime_root, receipt);
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
    let expected_evidence_increments = transaction_resource_increments_v1(&resource_usages);
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

fn process_peaks_v1(
    trials: &BTreeMap<ComponentGoldenTrialIdV1, ComponentGoldenTrialReportV1>,
) -> (u64, u64, u64) {
    let mut resident = 0;
    let mut swap = 0;
    let mut combined = 0;
    for snapshot in trials.values().flat_map(|trial| {
        trial.resource_snapshots.iter().chain(
            trial
                .sealed_resource_evidence
                .as_ref()
                .into_iter()
                .map(|sealed| &sealed.postseal_resource_snapshot),
        )
    }) {
        resident = resident.max(snapshot.procfs.vm_hwm_bytes);
        swap = swap.max(snapshot.procfs.vm_swap_bytes);
        let proc_peak = snapshot
            .procfs
            .vm_hwm_bytes
            .checked_add(snapshot.procfs.vm_swap_bytes)
            .expect("进程内存与交换峰值求和溢出");
        let cgroup_peak = snapshot
            .cgroup_v2
            .memory_peak_bytes
            .expect("正式试次必须有 cgroup memory.peak")
            .checked_add(
                snapshot
                    .cgroup_v2
                    .memory_swap_peak_bytes
                    .expect("正式试次必须有 cgroup memory.swap.peak"),
            )
            .expect("cgroup 内存与交换峰值求和溢出");
        combined = combined.max(proc_peak.max(cgroup_peak));
    }
    assert!(resident > 0 && combined > 0);
    (resident, swap, combined)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "正式 COMPONENT_GOLDEN_V1 单试次由已编译发布测试二进制在独立 scope 中执行"]
async fn task441_component_golden_formal_trial_runner() {
    let selector = FormalTrialSelectorV1::from_environment();
    let output_root = formal_output_root_from_environment();
    let fixed_context = formal_fixed_context_v1(&output_root);
    let implementation_identity = implementation_identity_from_fixed_context_v1(&fixed_context);
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
    let nonce = formal_run_nonce_v1();
    let run_id = format!(
        "task44-component-golden-{}-{}",
        nonce,
        selector.label().to_ascii_lowercase()
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
        implementation_identity.clone(),
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
    validate_fixed_context_artifacts_v1(&b1_context, &input.stream_sha256, phase == "FREEZE");
    let mut trials = BTreeMap::new();
    for trial_id in COMPONENT_GOLDEN_TRIAL_ORDER_V1 {
        let persisted = read_persisted_formal_trial_v1(
            &output_root,
            FormalTrialSelectorV1::Component(trial_id),
        );
        let PersistedFormalTrialReportV1::Component(report) = &persisted.trial else {
            panic!("六次预冻结目录必须只含 ComponentGoldenTrialReportV1")
        };
        assert!(trials.insert(trial_id, report.clone()).is_none());
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
    let (_, _, process_memory_peak_bytes) = process_peaks_v1(&trials);
    let failure_reserve: SimulationFailureEvidenceReserveV1 =
        read_json_with_sha256(&output_root.join(FORMAL_FAILURE_RESERVE_FILE_V1));
    for quantity in [
        failure_reserve.evidence,
        failure_reserve.checkpoints,
        failure_reserve.runtime_root,
    ] {
        assert!(quantity.records > 0 && quantity.bytes > 0);
    }
    let expected_frozen =
        freeze_resource_budgets_v1(&observations, process_memory_peak_bytes, failure_reserve)
            .expect("从六个独立进程的 seal 绑定证据冻结资源预算");
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
    validate_fixed_context_artifacts_v1(&b2_context, &input.stream_sha256, true);
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
    for verification_id in FROZEN_BUDGET_VERIFICATION_TRIALS_V1 {
        let persisted = read_persisted_formal_trial_v1(
            &output_root,
            FormalTrialSelectorV1::Verification(verification_id),
        );
        let PersistedFormalTrialReportV1::Verification(report) = persisted.trial else {
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
                .insert(verification_id, report)
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
        process_peaks_v1(&trials);
    let report = ComponentGoldenMeasurementReportV1 {
        schema_version: COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1.to_owned(),
        workload_contract,
        workload_contract_sha256,
        environment: first_measurement.environment.clone(),
        runtime_hard_guards: first_measurement.runtime_hard_guards,
        trials,
        budget_observations: observations,
        process_resident_peak_bytes,
        process_swap_peak_bytes,
        process_memory_peak_bytes,
        frozen_budgets: frozen,
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
    let input = GoldenInputV1::new();
    let environment =
        formal_measurement_environment_from_fixed_context_v1(&output_root, input.stream_sha256);
    let (run_id, persistence_root) = unique_smoke_run_root(&output_root);
    let trial = run_golden_trial_v1(
        false,
        MeasurementRunIdentityV1::ComponentTrial(ComponentGoldenTrialIdV1::S1),
        run_id,
        persistence_root,
        implementation_identity,
        environment,
        SimulationResourcePolicyV1::component_golden_v1(),
    )
    .await;
    assert_eq!(trial.trajectories[0].path_deliveries, 20);
    assert_eq!(trial.trajectories[1].path_deliveries, 20);
    assert!(!trial.resource_usages.is_empty());
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
