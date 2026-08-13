use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use contracts::{
    AssetIdentityV1, BestBidAskV2, BookSide, ClobMarketId, ConditionId, EventQualityV2,
    ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, LevelExecutionBasis,
    LevelExecutionTransitionV3, MarketDataQuality, MarketIdentityV2, MarketResolutionTerminalV3,
    MarketTradingStateV2, MarketWindowV2, OutcomeIdentityV1, PayoutEntryV1, QueueResultQuality,
    ResyncQuality, Sha256Hex, SourceAttributionQuality, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::artifacts::{
    PLATFORM_RULE_SNAPSHOT_FILE_NAME_V1, PLATFORM_SCENARIO_FILE_NAME_V1,
    PROXY_LATENCY_PROFILE_FILE_NAME_V1, SIMULATION_RUN_AUDIT_FILE_NAME_V1,
    SIMULATION_RUN_MANIFEST_FILE_NAME_V1, SimulationDeterministicScriptContractV1,
    SimulationInputPathV1, SimulationPrivateDeliveryKindV1, SimulationRunArtifactErrorV1,
    SimulationRunAuditV1, SimulationRunIntentBindingV1, SimulationRunManifestV1,
    SimulationSessionCompletionKindV1, SimulationSessionLifecycleV1, SimulationWorkloadKindV1,
    TRADE_FINALITY_POLICY_FILE_NAME_V1,
};
use crate::execution::{
    CancelNotCanceledReasonV1, CancelOrderDirectResponseV1, CancelOrderItemV1,
    CancelOrderRequestV1, CancelTargetOutcomeV1, DownstreamDurabilityProofV1,
    OrderDirectResponseV1, OrderLifecycleEventKindV1, OrderLifecycleFeedbackV1, OrderQueryResultV1,
    OrderRequestV1, OrdersDirectResponseV1, OrdersFilterV1, OrdersQueryResultV1, OrdersRequestV1,
    PaperMarketEventRecoveryReadErrorV1, PaperMarketEventRecoveryReadRequestV1,
    PaperMarketEventRecoveryReadV1, PaperMarketEventRecoveryReaderV1, PostOrderDirectResponseV1,
    PostOrderItemV1, PostOrderRequestV1, SessionRetirementFailureV1, SimBuyOrderKindV1,
    SimBuyOrderV1, SimExecutionPersistenceErrorV1, SimExecutionProcessV1, SimExecutionSessionV1,
    SimulatedOrderKnownStatusV1, SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1,
    SubmissionItemOutcomeV1, SubmissionRejectedReasonV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, TradeLiquidityRoleV1, TradesDirectResponseV1, TradesFilterV1,
    TradesQueryResultV1, TradesRequestV1,
};
use crate::finality::{
    TradeFinalityPolicyV1, TradeFinalityStateV1, policy_for_tests, successful_policy_for_tests,
};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventPathV1,
    MarketLifecyclePhaseV1, MillisecondBucketClosureV1,
};
use crate::platform_rules::tests::{
    historical_snapshot_for_sim_tests_with_fee_schedule, successful_snapshot_for_sim_tests,
};
use crate::platform_rules::{
    RecoveryProcessSnapshotRequestV1, SealedProcessRuleSnapshot, recover_process_snapshot,
};
use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};
use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, MarketEnrollmentRequestV1,
    SessionCreationControlResultV2, SessionCreationDispositionV2, SessionError,
    SessionInitializationControlDispositionV1, SessionInitializationStateV1, SessionSpecV2,
    SimCallerRoleV1,
};
use crate::task21_tests::fresh_quality;
use crate::task22_tests::{
    asset, context_for_arrival, initial_events, market, orderbook_evidence, scenario, sealed_event,
    session_spec,
};

const RUN_ID: &str = "component-test-run";
const MARKET_END_AT_UTC_MS: i64 = 500_000;
const FULL_EVENT_TAPE_CANARY: &str = "TASK431_FULL_EVENT_TAPE_CANARY";
const ALTERNATE_FULL_EVENT_TAPE_CANARY: &str = "TASK431_FULL_EVENT_TAPE_CANARZ";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceEnvelopeAuditWireV1 {
    schema_version: String,
    sequence: u64,
    predecessor_evidence_sha256: Option<Sha256Hex>,
    body: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableEvidenceAuditWireV1 {
    schema_version: String,
    fact: Value,
    query_projection_revision: Option<u64>,
    logical_platform_driven_through_utc_ms: Option<i64>,
    private_fact_schedule_closed_through_utc_ms: Option<i64>,
    account_deltas: Vec<Value>,
    logical_action_results: Vec<Value>,
    private_output_drafts: Vec<Value>,
    transient_input_session_id: Option<String>,
    transient_input: Value,
    resource_usage: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlatformBucketAppliedAuditWireV1 {
    session_id: String,
    bucket: DurableMarketBucketReferenceAuditWireV1,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableMarketBucketReferenceAuditWireV1 {
    path: MarketEventPathV1,
    bucket_at_utc_ms: i64,
    start_index_position: u64,
    next_index_position: u64,
    closure: MillisecondBucketClosureV1,
    bucket_sha256: Sha256Hex,
}

#[derive(Clone)]
struct TestMarketEventSourceV1 {
    identity_sha256: Sha256Hex,
    buckets:
        Arc<Mutex<BTreeMap<PaperMarketEventRecoveryReadRequestV1, PaperMarketEventRecoveryReadV1>>>,
}

impl TestMarketEventSourceV1 {
    fn new() -> Self {
        Self {
            identity_sha256: Sha256Hex(format!(
                "{:x}",
                Sha256::digest(b"SIM_TASK42_TEST_MARKET_EVENT_SOURCE_V1")
            )),
            buckets: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn record(
        &self,
        request: PaperMarketEventRecoveryReadRequestV1,
        read: PaperMarketEventRecoveryReadV1,
    ) {
        self.buckets
            .lock()
            .expect("任务 4.2 市场事件源锁")
            .insert(request, read);
    }

    fn reader(&self) -> PaperMarketEventRecoveryReaderV1 {
        let buckets = Arc::clone(&self.buckets);
        PaperMarketEventRecoveryReaderV1::try_new(self.identity_sha256.clone(), move |request| {
            buckets
                .lock()
                .map_err(|_| {
                    PaperMarketEventRecoveryReadErrorV1::SourceUnavailable(
                        "测试市场事件源锁不可用".to_owned(),
                    )
                })?
                .get(&request)
                .cloned()
                .ok_or_else(|| {
                    PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(format!(
                        "测试市场事件桶不存在：{}:{}..{}",
                        request.bucket_at_utc_ms,
                        request.start_visibility_index_position,
                        request.next_visibility_index_position
                    ))
                })
        })
        .expect("任务 4.2 测试市场事件源身份合法")
    }
}

const GOLDEN_MARKET_EVENT_SOURCE_SCHEMA_V1: &str = "sim_golden_market_event_source_v1";

#[derive(Clone)]
struct GoldenMarketEventBucketInputV1 {
    bucket_at_utc_ms: i64,
    start_visibility_index_position: u64,
    next_visibility_index_position: u64,
    events: Vec<ExecutionMarketEventV3>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoldenMarketEventRecordV1 {
    bucket_at_utc_ms: i64,
    start_visibility_index_position: u64,
    next_visibility_index_position: u64,
    object_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoldenMarketEventManifestV1 {
    schema_version: String,
    complete: bool,
    records: Vec<GoldenMarketEventRecordV1>,
}

#[derive(Clone)]
struct GoldenMarketEventSourceV1 {
    directory: Arc<tempfile::TempDir>,
    identity_sha256: Sha256Hex,
    reads: Arc<Mutex<Vec<PaperMarketEventRecoveryReadRequestV1>>>,
}

impl GoldenMarketEventSourceV1 {
    fn write(buckets: Vec<GoldenMarketEventBucketInputV1>, complete: bool) -> Self {
        let directory = Arc::new(tempfile::tempdir().expect("创建黄金市场事件源目录"));
        let objects = directory.path().join("objects");
        std::fs::create_dir(&objects).expect("创建黄金市场事件对象目录");
        let mut records = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            assert!(
                !bucket.events.is_empty(),
                "黄金持久源只保存真实事件桶；空驱动桶由位置和下一事件见证派生"
            );
            let encoded_events = bucket
                .events
                .iter()
                .map(|event| encode_execution_market_event_v3(event).expect("编码黄金规范事件"))
                .collect::<Vec<_>>();
            ClosedMarketEventBucketV1::public(
                bucket.bucket_at_utc_ms,
                bucket.start_visibility_index_position,
                bucket.next_visibility_index_position,
                MillisecondBucketClosureV1::NextEventAt(bucket.bucket_at_utc_ms + 1),
                encoded_events.clone(),
            )
            .validated_durable_reference_for_recovery()
            .expect("黄金持久源只接受规范公开事件桶");
            let object = serde_json::to_vec(&encoded_events).expect("编码黄金市场事件对象");
            let object_sha256 = Sha256Hex(format!("{:x}", Sha256::digest(&object)));
            std::fs::write(objects.join(format!("{}.json", object_sha256.0)), object)
                .expect("写入内容寻址黄金市场事件对象");
            records.push(GoldenMarketEventRecordV1 {
                bucket_at_utc_ms: bucket.bucket_at_utc_ms,
                start_visibility_index_position: bucket.start_visibility_index_position,
                next_visibility_index_position: bucket.next_visibility_index_position,
                object_sha256,
            });
        }
        let manifest = GoldenMarketEventManifestV1 {
            schema_version: GOLDEN_MARKET_EVENT_SOURCE_SCHEMA_V1.to_owned(),
            complete,
            records,
        };
        let manifest_bytes = serde_json::to_vec(&manifest).expect("编码黄金市场事件源清单");
        let identity_sha256 = Sha256Hex(format!("{:x}", Sha256::digest(&manifest_bytes)));
        std::fs::write(directory.path().join("manifest.json"), manifest_bytes)
            .expect("写入黄金市场事件源清单");
        Self {
            directory,
            identity_sha256,
            reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn reader(&self) -> PaperMarketEventRecoveryReaderV1 {
        self.reader_bound_to(self.identity_sha256.clone())
    }

    fn reader_bound_to(
        &self,
        asserted_identity_sha256: Sha256Hex,
    ) -> PaperMarketEventRecoveryReaderV1 {
        let directory = self.directory.path().to_path_buf();
        let verified_identity_sha256 = asserted_identity_sha256.clone();
        let reads = Arc::clone(&self.reads);
        PaperMarketEventRecoveryReaderV1::try_new(asserted_identity_sha256, move |request| {
            reads
                .lock()
                .map_err(|_| {
                    PaperMarketEventRecoveryReadErrorV1::SourceUnavailable(
                        "黄金市场事件源读取记录锁不可用".to_owned(),
                    )
                })?
                .push(request.clone());
            Self::read_from_disk(&directory, &verified_identity_sha256, &request)
        })
        .expect("黄金市场事件源身份合法")
    }

    fn closed_bucket(
        &self,
        request: PaperMarketEventRecoveryReadRequestV1,
    ) -> ClosedMarketEventBucketV1 {
        let read = Self::read_from_disk(self.directory.path(), &self.identity_sha256, &request)
            .expect("读取黄金市场事件桶");
        let closure = read
            .next_event_at_utc_ms
            .map_or(MillisecondBucketClosureV1::EndOfInput, |next_at| {
                MillisecondBucketClosureV1::NextEventAt(next_at)
            });
        Self::closed_bucket_from_read(request, read, closure)
    }

    fn closed_bucket_with_closure(
        &self,
        request: PaperMarketEventRecoveryReadRequestV1,
        closure: MillisecondBucketClosureV1,
    ) -> ClosedMarketEventBucketV1 {
        let read = Self::read_from_disk(self.directory.path(), &self.identity_sha256, &request)
            .expect("读取待伪造闭合见证的黄金市场事件桶");
        Self::closed_bucket_from_read(request, read, closure)
    }

    fn closed_bucket_from_read(
        request: PaperMarketEventRecoveryReadRequestV1,
        read: PaperMarketEventRecoveryReadV1,
        closure: MillisecondBucketClosureV1,
    ) -> ClosedMarketEventBucketV1 {
        ClosedMarketEventBucketV1::public(
            request.bucket_at_utc_ms,
            request.start_visibility_index_position,
            request.next_visibility_index_position,
            closure,
            read.encoded_events,
        )
    }

    fn remove_object_for(&self, request: &PaperMarketEventRecoveryReadRequestV1) {
        let manifest = self.read_manifest().expect("读取待损坏黄金源清单");
        let record = manifest
            .records
            .iter()
            .find(|record| Self::record_matches(record, request))
            .expect("黄金源必须包含待删除对象");
        std::fs::remove_file(
            self.directory
                .path()
                .join("objects")
                .join(format!("{}.json", record.object_sha256.0)),
        )
        .expect("删除指定黄金市场事件对象");
    }

    fn reads(&self) -> Vec<PaperMarketEventRecoveryReadRequestV1> {
        self.reads.lock().expect("黄金市场事件源读取记录锁").clone()
    }

    fn read_manifest(
        &self,
    ) -> Result<GoldenMarketEventManifestV1, PaperMarketEventRecoveryReadErrorV1> {
        Self::read_manifest_from(self.directory.path(), &self.identity_sha256)
    }

    fn read_from_disk(
        directory: &Path,
        expected_identity_sha256: &Sha256Hex,
        request: &PaperMarketEventRecoveryReadRequestV1,
    ) -> Result<PaperMarketEventRecoveryReadV1, PaperMarketEventRecoveryReadErrorV1> {
        let manifest = Self::read_manifest_from(directory, expected_identity_sha256)?;
        Self::validate_manifest_chain(&manifest)?;
        let exact_index = manifest
            .records
            .iter()
            .position(|record| Self::record_matches(record, request));
        let (encoded_events, next_record_index) = if let Some(index) = exact_index {
            (
                Self::read_object(directory, &manifest.records[index])?,
                index + 1,
            )
        } else {
            if request.start_visibility_index_position != request.next_visibility_index_position {
                return Err(PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(
                    format!(
                        "黄金源不含闭桶 {}:{}..{}",
                        request.bucket_at_utc_ms,
                        request.start_visibility_index_position,
                        request.next_visibility_index_position
                    ),
                ));
            }
            let next_record_index = manifest.records.partition_point(|record| {
                record.start_visibility_index_position < request.next_visibility_index_position
            });
            let previous = next_record_index
                .checked_sub(1)
                .and_then(|index| manifest.records.get(index));
            let next = manifest.records.get(next_record_index);
            if previous.is_none() && request.start_visibility_index_position != 0
                || previous.is_some_and(|record| {
                    record.next_visibility_index_position != request.start_visibility_index_position
                        || request.bucket_at_utc_ms <= record.bucket_at_utc_ms
                })
                || next.is_some_and(|record| {
                    record.start_visibility_index_position != request.next_visibility_index_position
                        || request.bucket_at_utc_ms >= record.bucket_at_utc_ms
                })
            {
                return Err(PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(
                    "黄金源不能从声明位置派生该空闭桶".to_owned(),
                ));
            }
            (Vec::new(), next_record_index)
        };
        let next_event_at_utc_ms = if let Some(next) = manifest.records.get(next_record_index) {
            Self::read_object(directory, next).map_err(|error| {
                PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(format!(
                    "无法从完整下一对象证明闭桶尾部：{error}"
                ))
            })?;
            Some(next.bucket_at_utc_ms)
        } else if manifest.complete {
            None
        } else {
            return Err(PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(
                "黄金源尾部未封存，不能把缺失后缀解释为 EOF".to_owned(),
            ));
        };
        Ok(PaperMarketEventRecoveryReadV1 {
            encoded_events,
            next_event_at_utc_ms,
        })
    }

    fn read_manifest_from(
        directory: &Path,
        expected_identity_sha256: &Sha256Hex,
    ) -> Result<GoldenMarketEventManifestV1, PaperMarketEventRecoveryReadErrorV1> {
        let bytes = std::fs::read(directory.join("manifest.json")).map_err(|error| {
            PaperMarketEventRecoveryReadErrorV1::SourceUnavailable(error.to_string())
        })?;
        let actual_identity_sha256 = Sha256Hex(format!("{:x}", Sha256::digest(&bytes)));
        if &actual_identity_sha256 != expected_identity_sha256 {
            return Err(PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(
                "黄金源清单内容身份不匹配".to_owned(),
            ));
        }
        let manifest: GoldenMarketEventManifestV1 =
            serde_json::from_slice(&bytes).map_err(|error| {
                PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(error.to_string())
            })?;
        if manifest.schema_version != GOLDEN_MARKET_EVENT_SOURCE_SCHEMA_V1 {
            return Err(PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(
                "黄金源清单版本不受支持".to_owned(),
            ));
        }
        Ok(manifest)
    }

    fn validate_manifest_chain(
        manifest: &GoldenMarketEventManifestV1,
    ) -> Result<(), PaperMarketEventRecoveryReadErrorV1> {
        for (index, record) in manifest.records.iter().enumerate() {
            if record.bucket_at_utc_ms <= 0
                || record.next_visibility_index_position < record.start_visibility_index_position
                || (index == 0 && record.start_visibility_index_position != 0)
            {
                return Err(PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(
                    "黄金源清单包含非法闭桶坐标".to_owned(),
                ));
            }
            if let Some(previous) = index
                .checked_sub(1)
                .and_then(|index| manifest.records.get(index))
                && (record.bucket_at_utc_ms <= previous.bucket_at_utc_ms
                    || record.start_visibility_index_position
                        != previous.next_visibility_index_position)
            {
                return Err(PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(
                    "黄金源清单闭桶链不连续".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn read_object(
        directory: &Path,
        record: &GoldenMarketEventRecordV1,
    ) -> Result<Vec<Vec<u8>>, PaperMarketEventRecoveryReadErrorV1> {
        let path = directory
            .join("objects")
            .join(format!("{}.json", record.object_sha256.0));
        let bytes = std::fs::read(&path).map_err(|error| {
            PaperMarketEventRecoveryReadErrorV1::RangeUnavailable(format!(
                "内容寻址对象 {} 不可用：{error}",
                record.object_sha256.0
            ))
        })?;
        let actual_sha256 = Sha256Hex(format!("{:x}", Sha256::digest(&bytes)));
        if actual_sha256 != record.object_sha256 {
            return Err(PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(
                "内容寻址对象正文与名称不匹配".to_owned(),
            ));
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            PaperMarketEventRecoveryReadErrorV1::SourceIntegrity(error.to_string())
        })
    }

    fn record_matches(
        record: &GoldenMarketEventRecordV1,
        request: &PaperMarketEventRecoveryReadRequestV1,
    ) -> bool {
        record.bucket_at_utc_ms == request.bucket_at_utc_ms
            && record.start_visibility_index_position == request.start_visibility_index_position
            && record.next_visibility_index_position == request.next_visibility_index_position
    }
}

#[derive(Clone)]
struct RecoveryDependenciesV1 {
    finality_policy: Arc<TradeFinalityPolicyV1>,
    current_rules: Arc<SealedProcessRuleSnapshot>,
    recovery_rules: Arc<SealedProcessRuleSnapshot>,
    latency_profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
    market_event_source: TestMarketEventSourceV1,
}

impl RecoveryDependenciesV1 {
    async fn new(finality_policy: Arc<TradeFinalityPolicyV1>) -> Self {
        let current_rules = successful_snapshot_for_sim_tests().await;
        Self::with_rules(finality_policy, current_rules)
    }

    fn with_rules(
        finality_policy: Arc<TradeFinalityPolicyV1>,
        current_rules: Arc<SealedProcessRuleSnapshot>,
    ) -> Self {
        let directory = tempfile::tempdir().expect("创建同源规则恢复夹具目录");
        let path = directory.path().join("platform-rule-snapshot.json");
        std::fs::write(&path, current_rules.file_bytes()).expect("写入同源规则恢复夹具");
        let recovery_rules = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
            snapshot_path: path,
            expected_file_sha256: current_rules.file_sha256().clone(),
            expected_implementation_identity: current_rules
                .snapshot()
                .provenance
                .implementation_identity
                .clone(),
        })
        .expect("从同一 CURRENT_QUERY 正文经真实 RECOVERY 入口重载");
        Self {
            finality_policy,
            current_rules,
            recovery_rules,
            latency_profile: load_embedded_proxy_latency_profile().expect("代理时延配置档"),
            scenario: scenario(),
            market_event_source: TestMarketEventSourceV1::new(),
        }
    }

    fn new_process(
        &self,
        persistence_root: PathBuf,
    ) -> Result<SimExecutionProcessV1, SimExecutionPersistenceErrorV1> {
        self.new_process_with_source(
            self.market_event_source.identity_sha256.clone(),
            persistence_root,
        )
    }

    fn new_process_with_source(
        &self,
        source_identity_sha256: Sha256Hex,
        persistence_root: PathBuf,
    ) -> Result<SimExecutionProcessV1, SimExecutionPersistenceErrorV1> {
        let run_intent_binding = SimulationRunIntentBindingV1::try_new(
            self.current_rules
                .snapshot()
                .provenance
                .implementation_identity
                .clone(),
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("构造测试运行意图绑定");
        SimExecutionProcessV1::try_new_with_local_persistence(
            RUN_ID.to_owned(),
            Arc::clone(&self.finality_policy),
            Arc::clone(&self.current_rules),
            Arc::clone(&self.latency_profile),
            Arc::clone(&self.scenario),
            run_intent_binding,
            source_identity_sha256,
            persistence_root,
        )
    }

    fn recover(
        &self,
        persistence_root: PathBuf,
    ) -> Result<SimExecutionProcessV1, SimExecutionPersistenceErrorV1> {
        self.recover_with_reader(self.market_event_source.reader(), persistence_root)
    }

    fn recover_with_reader(
        &self,
        reader: PaperMarketEventRecoveryReaderV1,
        persistence_root: PathBuf,
    ) -> Result<SimExecutionProcessV1, SimExecutionPersistenceErrorV1> {
        SimExecutionProcessV1::recover_paper_from_local_persistence(
            Arc::clone(&self.finality_policy),
            Arc::clone(&self.recovery_rules),
            Arc::clone(&self.latency_profile),
            Arc::clone(&self.scenario),
            reader,
            persistence_root,
        )
    }

    fn recover_for_artifact_publication(
        &self,
        persistence_root: PathBuf,
    ) -> Result<SimExecutionProcessV1, SimExecutionPersistenceErrorV1> {
        SimExecutionProcessV1::recover_for_artifact_publication(
            Arc::clone(&self.finality_policy),
            Arc::clone(&self.recovery_rules),
            Arc::clone(&self.latency_profile),
            Arc::clone(&self.scenario),
            persistence_root,
        )
    }
}

#[derive(Clone, Copy)]
enum OrderBridgeAcceptanceV1 {
    Accepted,
    Full,
}

#[derive(Clone, Default)]
struct DirectCapturesV1 {
    post_orders: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    cancel_orders: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    order_queries: Arc<Mutex<Vec<Arc<OrderDirectResponseV1>>>>,
    orders_queries: Arc<Mutex<Vec<Arc<OrdersDirectResponseV1>>>>,
    trade_queries: Arc<Mutex<Vec<Arc<TradesDirectResponseV1>>>>,
    order_lifecycle: Arc<Mutex<Vec<Arc<OrderLifecycleFeedbackV1>>>>,
}

impl DirectCapturesV1 {
    fn bridge(&self, order_acceptance: OrderBridgeAcceptanceV1) -> SubmissionResponseBridgeV1 {
        self.bridge_with_optional_lifetime_marker(order_acceptance, None)
    }

    fn bridge_with_lifetime_marker(
        &self,
        order_acceptance: OrderBridgeAcceptanceV1,
        lifetime_marker: Arc<()>,
    ) -> SubmissionResponseBridgeV1 {
        self.bridge_with_optional_lifetime_marker(order_acceptance, Some(lifetime_marker))
    }

    fn bridge_with_optional_lifetime_marker(
        &self,
        order_acceptance: OrderBridgeAcceptanceV1,
        lifetime_marker: Option<Arc<()>>,
    ) -> SubmissionResponseBridgeV1 {
        let post_orders = Arc::clone(&self.post_orders);
        let cancel_orders = Arc::clone(&self.cancel_orders);
        let order_queries = Arc::clone(&self.order_queries);
        let orders_queries = Arc::clone(&self.orders_queries);
        let trade_queries = Arc::clone(&self.trade_queries);
        let order_lifecycle = Arc::clone(&self.order_lifecycle);
        SubmissionResponseBridgeV1::new(
            move |response| {
                let _lifetime_marker = &lifetime_marker;
                post_orders
                    .lock()
                    .expect("任务 4.2 单项提交响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_orders
                    .lock()
                    .expect("任务 4.2 单项撤单响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                order_queries
                    .lock()
                    .expect("任务 4.2 订单查询响应捕获锁")
                    .push(Arc::clone(&response));
                match order_acceptance {
                    OrderBridgeAcceptanceV1::Accepted => SubmissionResponseAcceptanceV1::Accepted(
                        response.metadata.delivery_confirmation(),
                    ),
                    OrderBridgeAcceptanceV1::Full => SubmissionResponseAcceptanceV1::Full,
                }
            },
            move |response| {
                orders_queries
                    .lock()
                    .expect("任务 4.2 订单列表响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                trade_queries
                    .lock()
                    .expect("任务 4.2 成交查询响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |feedback| {
                order_lifecycle
                    .lock()
                    .expect("任务 4.2 委托生命周期反馈捕获锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        )
    }

    fn last_post_order(&self) -> Arc<PostOrderDirectResponseV1> {
        self.post_orders
            .lock()
            .expect("任务 4.2 单项提交响应捕获锁")
            .last()
            .cloned()
            .expect("必须捕获单项提交响应")
    }

    fn last_order_query(&self) -> Arc<OrderDirectResponseV1> {
        self.order_queries
            .lock()
            .expect("任务 4.2 订单查询响应捕获锁")
            .last()
            .cloned()
            .expect("必须捕获订单查询响应")
    }

    fn last_cancel_order(&self) -> Arc<CancelOrderDirectResponseV1> {
        self.cancel_orders
            .lock()
            .expect("任务 4.2 单项撤单响应捕获锁")
            .last()
            .cloned()
            .expect("必须捕获单项撤单响应")
    }

    fn last_orders_query(&self) -> Arc<OrdersDirectResponseV1> {
        self.orders_queries
            .lock()
            .expect("任务 4.2 订单列表响应捕获锁")
            .last()
            .cloned()
            .expect("必须捕获订单列表响应")
    }

    fn order_query_count(&self) -> usize {
        self.order_queries
            .lock()
            .expect("任务 4.2 订单查询响应捕获锁")
            .len()
    }

    fn last_trade_query(&self) -> Arc<TradesDirectResponseV1> {
        self.trade_queries
            .lock()
            .expect("任务 4.2 成交查询响应捕获锁")
            .last()
            .cloned()
            .expect("必须捕获成交查询响应")
    }

    fn order_queries(&self) -> Vec<Arc<OrderDirectResponseV1>> {
        self.order_queries
            .lock()
            .expect("任务 4.2 订单查询响应捕获锁")
            .clone()
    }

    fn order_lifecycle(&self) -> Vec<Arc<OrderLifecycleFeedbackV1>> {
        self.order_lifecycle
            .lock()
            .expect("任务 4.2 委托生命周期反馈捕获锁")
            .clone()
    }
}

fn paper_spec(dependencies: &RecoveryDependenciesV1, session_id: &str) -> SessionSpecV2 {
    let mut spec = session_spec(&dependencies.scenario, SimCallerRoleV1::Paper, session_id);
    spec.response_outbox_capacity = NonZeroUsize::new(64).expect("正响应容量");
    spec
}

fn activation_request(
    spec: &SessionSpecV2,
    control: &SessionCreationControlResultV2,
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
    control: &SessionCreationControlResultV2,
) -> FailSessionInitializationRequestV1 {
    FailSessionInitializationRequestV1 {
        session_id: spec.session_id.clone(),
        account_instance_key: control.account_instance_key.clone(),
        opening_anchor_id: control.opening_anchor.anchor_id.clone(),
        opening_anchor_sha256: control.opening_anchor.anchor_sha256.clone(),
        failure_id: format!("fail-{}", spec.session_id),
    }
}

fn public_bucket(
    dependencies: &RecoveryDependenciesV1,
    session: &SimExecutionSessionV1,
    at_utc_ms: i64,
    closure: MillisecondBucketClosureV1,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    let start = session.public_event_index_position();
    let next = start + u64::try_from(events.len()).expect("测试事件数适合 u64");
    let encoded_events = events
        .iter()
        .map(|event| encode_execution_market_event_v3(event).expect("规范事件编码"))
        .collect::<Vec<_>>();
    dependencies.market_event_source.record(
        PaperMarketEventRecoveryReadRequestV1 {
            bucket_at_utc_ms: at_utc_ms,
            start_visibility_index_position: start,
            next_visibility_index_position: next,
        },
        PaperMarketEventRecoveryReadV1 {
            encoded_events: encoded_events.clone(),
            next_event_at_utc_ms: match closure {
                MillisecondBucketClosureV1::NextEventAt(next_at) => Some(next_at),
                MillisecondBucketClosureV1::EndOfInput => None,
            },
        },
    );
    ClosedMarketEventBucketV1::public(at_utc_ms, start, next, closure, encoded_events)
}

fn hidden_bucket(
    session: &SimExecutionSessionV1,
    at_utc_ms: i64,
    closure: MillisecondBucketClosureV1,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    let start = session.platform_event_index_position();
    let next = start + u64::try_from(events.len()).expect("测试事件数适合 u64");
    ClosedMarketEventBucketV1::hidden(
        at_utc_ms,
        start,
        next,
        closure,
        events
            .iter()
            .map(|event| encode_execution_market_event_v3(event).expect("规范事件编码"))
            .collect::<Vec<_>>(),
    )
}

fn apply_public_empty(
    dependencies: &RecoveryDependenciesV1,
    session: &mut SimExecutionSessionV1,
    at_utc_ms: i64,
) -> ClosedMarketEventBucketV1 {
    let bucket = public_bucket(
        dependencies,
        session,
        at_utc_ms,
        MillisecondBucketClosureV1::NextEventAt(at_utc_ms + 1),
        &[],
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    bucket
}

fn load_initial_market(
    dependencies: &RecoveryDependenciesV1,
    session: &mut SimExecutionSessionV1,
    tick: &str,
    best_ask: &str,
    ask_quantity: &str,
) -> u64 {
    let events = initial_events(tick, best_ask, ask_quantity);
    let bucket = public_bucket(
        dependencies,
        session,
        120_000,
        MillisecondBucketClosureV1::NextEventAt(120_001),
        &events,
    );
    session
        .apply_closed_market_event_bucket(bucket)
        .expect("任务 4.2 初始公开市场桶");
    2
}

fn response_received_at(
    receipt: &crate::execution::RequestRegistrationReceiptV1,
    platform_responded_at_utc_ms: i64,
) -> i64 {
    platform_responded_at_utc_ms
        + i64::try_from(receipt.transit.inbound_delay_ms).expect("测试入站时延适合 i64")
}

fn gtc_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            desired_arrival_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market().condition_id,
                token_id: asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(3, 1),
                    size_shares: Decimal::new(5, 0),
                    post_only: true,
                },
            },
        },
    }
}

fn fak_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            desired_arrival_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market().condition_id,
                token_id: asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: Decimal::new(3, 0),
                    worst_price_pusd_per_share: Decimal::new(6, 1),
                },
            },
        },
    }
}

fn query_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
) -> OrderRequestV1 {
    query_request_at(session, profile, request_id, 1, 150_000)
}

fn query_request_at(
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

fn drive_one_registered_action(
    dependencies: &RecoveryDependenciesV1,
    session: &mut SimExecutionSessionV1,
    effective_at_utc_ms: i64,
) {
    apply_public_empty(dependencies, session, effective_at_utc_ms);
    assert_eq!(
        session.drive_next_execution_action_through(effective_at_utc_ms),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
}

fn appended_market() -> PlatformMarketV1 {
    PlatformMarketV1::try_new(
        MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", "2".repeat(64))),
            clob_market_id: ClobMarketId("2002".to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: 600_000,
                end_at_utc_ms: 900_000,
            },
        },
        [
            AssetIdentityV1 {
                token_id: TokenId("3".to_owned()),
                outcome: OutcomeIdentityV1 {
                    index: 0,
                    label: "Up".to_owned(),
                },
            },
            AssetIdentityV1 {
                token_id: TokenId("4".to_owned()),
                outcome: OutcomeIdentityV1 {
                    index: 1,
                    label: "Down".to_owned(),
                },
            },
        ],
    )
    .expect("追加市场夹具")
}

fn resolution_event(source_at_utc_ms: i64, received_at_utc_ms: i64) -> ExecutionMarketEventV3 {
    resolution_event_for_outcome_with_cursor(source_at_utc_ms, received_at_utc_ms, true, 3)
}

fn maker_bid_transition(
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    source_sequence: u64,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    let before_decimal = before.parse::<Decimal>().expect("maker 旧数量");
    let after_decimal = after.parse::<Decimal>().expect("maker 新数量");
    let added = (after_decimal - before_decimal).max(Decimal::ZERO);
    let reduced = (before_decimal - after_decimal).max(Decimal::ZERO);
    sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: Sha256Hex("8".repeat(64)),
            market: market(),
            asset: asset("1", 0, "Up"),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.3".to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(added.normalize().to_string()),
            net_reduced_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            queue_consumption_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at_utc_ms,
            visible_at_utc_ms: received_at_utc_ms,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        orderbook_evidence(source_at_utc_ms, received_at_utc_ms, source_sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}

fn resolution_event_for_outcome_with_cursor(
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    up_wins: bool,
    source_cursor: u64,
) -> ExecutionMarketEventV3 {
    let (winning_asset_id, winning_outcome, up_payout, down_payout) = if up_wins {
        ("1", "Up", "1", "0")
    } else {
        ("2", "Down", "0", "1")
    };
    sealed_event(
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
            MarketResolutionTerminalV3::Resolved {
                winning_asset_id: winning_asset_id.to_owned(),
                winning_outcome: winning_outcome.to_owned(),
                payout_vector: vec![
                    PayoutEntryV1 {
                        asset: asset("1", 0, "Up"),
                        payout_pusd_per_share: ExactDecimal(up_payout.to_owned()),
                    },
                    PayoutEntryV1 {
                        asset: asset("2", 1, "Down"),
                        payout_pusd_per_share: ExactDecimal(down_payout.to_owned()),
                    },
                ],
                mapping_rule_version: "task42-resolution-v1".to_owned(),
            },
        ),
        orderbook_evidence(source_at_utc_ms, received_at_utc_ms, source_cursor),
        fresh_quality(),
    )
}

fn end_marker_event() -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
            active: Some(false),
            closed: None,
            accepting_orders: None,
        }),
        orderbook_evidence(490_000, MARKET_END_AT_UTC_MS, 3),
        fresh_quality(),
    )
}

#[derive(Serialize)]
struct CurrentContentForHashV1<'a> {
    schema_version: &'a str,
    through_commit_sequence: u64,
    evidence_head_sha256: &'a str,
    checkpoint: &'a Value,
}

fn latest_checkpoint_path(persistence_root: &Path) -> PathBuf {
    let mut members = std::fs::read_dir(persistence_root.join("checkpoints"))
        .expect("读取任务 4.2 不可变检查点目录")
        .map(|entry| entry.expect("读取不可变检查点目录项"))
        .filter(|entry| entry.file_type().expect("读取检查点类型").is_file())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| !name.starts_with('.') && name.ends_with(".json"))
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    members.sort();
    members.pop().expect("必须存在已提交的不可变检查点")
}

fn evidence_path_for_sequence(persistence_root: &Path, sequence: u64) -> PathBuf {
    let prefix = format!("{sequence:020}-");
    let members = std::fs::read_dir(persistence_root.join("evidence"))
        .expect("读取任务 4.2 仅追加证据目录")
        .map(|entry| entry.expect("读取仅追加证据目录项"))
        .filter(|entry| entry.file_type().expect("读取证据类型").is_file())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".json"))
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    assert_eq!(members.len(), 1, "每个已提交序号必须只有一份不可变证据");
    members.into_iter().next().expect("已确认唯一证据")
}

fn mutate_latest_checkpoint(
    persistence_root: &Path,
    rebind_latest_evidence: bool,
    mutate: impl FnOnce(&mut Value),
) {
    let checkpoint_path = latest_checkpoint_path(persistence_root);
    let mut checkpoint_file: Value = serde_json::from_slice(
        &std::fs::read(&checkpoint_path).expect("读取任务 4.2 不可变检查点"),
    )
    .expect("解码任务 4.2 不可变检查点");
    mutate(
        checkpoint_file
            .pointer_mut("/content/checkpoint")
            .expect("不可变代必须包含运行检查点正文"),
    );
    let content = checkpoint_file
        .get_mut("content")
        .and_then(Value::as_object_mut)
        .expect("不可变检查点 content 对象");
    let sequence = content
        .get("through_commit_sequence")
        .and_then(Value::as_u64)
        .expect("检查点 through_commit_sequence");
    if rebind_latest_evidence {
        let evidence_path = evidence_path_for_sequence(persistence_root, sequence);
        let envelope: Value =
            serde_json::from_slice(&std::fs::read(&evidence_path).expect("读取最新证据信封"))
                .expect("解码最新证据信封");
        assert_eq!(
            envelope.get("sequence").and_then(Value::as_u64),
            Some(sequence)
        );
        let evidence_bytes = serde_json::to_vec(&envelope).expect("编码重绑后的最新证据信封");
        let evidence_sha256 = format!("{:x}", Sha256::digest(&evidence_bytes));
        let replacement_evidence_path = persistence_root
            .join("evidence")
            .join(format!("{sequence:020}-{evidence_sha256}.json"));
        std::fs::remove_file(&evidence_path).expect("替换临时夹具中的最新证据信封");
        std::fs::write(replacement_evidence_path, evidence_bytes)
            .expect("写入重绑后的最新证据信封");
        content["evidence_head_sha256"] = Value::String(evidence_sha256);
    }
    let hash_material = CurrentContentForHashV1 {
        schema_version: content
            .get("schema_version")
            .and_then(Value::as_str)
            .expect("检查点 schema_version"),
        through_commit_sequence: content
            .get("through_commit_sequence")
            .and_then(Value::as_u64)
            .expect("检查点 through_commit_sequence"),
        evidence_head_sha256: content
            .get("evidence_head_sha256")
            .and_then(Value::as_str)
            .expect("检查点 evidence_head_sha256"),
        checkpoint: content.get("checkpoint").expect("运行检查点正文"),
    };
    let digest =
        Sha256::digest(serde_json::to_vec(&hash_material).expect("编码不可变检查点完整正文"));
    let checkpoint_sha256 = format!("{digest:x}");
    checkpoint_file["checkpoint_sha256"] = Value::String(checkpoint_sha256.clone());
    let replacement_path = persistence_root
        .join("checkpoints")
        .join(format!("{sequence:020}-{checkpoint_sha256}.json"));
    std::fs::remove_file(&checkpoint_path).expect("替换临时夹具中的原不可变检查点");
    std::fs::write(
        replacement_path,
        serde_json::to_vec(&checkpoint_file).expect("编码被修改的不可变检查点"),
    )
    .expect("写入被修改的不可变检查点");

    let current_path = persistence_root.join("CURRENT");
    let mut current: Value =
        serde_json::from_slice(&std::fs::read(&current_path).expect("读取任务 4.2 CURRENT 指针"))
            .expect("解码任务 4.2 CURRENT 指针");
    current["checkpoint_sha256"] = Value::String(checkpoint_sha256);
    std::fs::write(
        current_path,
        serde_json::to_vec(&current).expect("编码被修改的 CURRENT 指针"),
    )
    .expect("写回被修改的 CURRENT 指针");
}

fn evidence_file_names(persistence_root: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(persistence_root.join("evidence"))
        .expect("读取任务 4.2 仅追加证据目录")
        .map(|entry| {
            entry
                .expect("读取仅追加证据目录项")
                .file_name()
                .into_string()
                .expect("证据文件名必须是 UTF-8")
        })
        .filter(|name| !name.starts_with('.'))
        .collect::<Vec<_>>();
    names.sort();
    names
}

struct PersistedQueryFixtureV1 {
    _directory: tempfile::TempDir,
    persistence_root: PathBuf,
    dependencies: RecoveryDependenciesV1,
    spec: SessionSpecV2,
}

async fn persisted_query_fixture(
    session_id: &str,
    acceptance: OrderBridgeAcceptanceV1,
) -> PersistedQueryFixtureV1 {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, session_id);
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建持久查询夹具运行根");
    let prepared = process
        .create_session(spec.clone(), captures.bridge(acceptance))
        .expect("创建持久查询夹具会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活持久查询夹具会话");
    let registration = session
        .order(query_request(
            &session,
            &dependencies.latency_profile,
            &format!("{session_id}-query"),
        ))
        .expect("登记持久查询夹具请求");
    let effective_at = registration.receipt.transit.platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, effective_at);
    let received_at = response_received_at(&registration.receipt, effective_at);
    apply_public_empty(&dependencies, &mut session, received_at);
    let expected = match acceptance {
        OrderBridgeAcceptanceV1::Accepted => SubmissionDeliveryDispositionV1::Delivered,
        OrderBridgeAcceptanceV1::Full => SubmissionDeliveryDispositionV1::Full,
    };
    assert_eq!(
        session.drive_private_delivery_through(received_at),
        Ok(expected)
    );
    assert_eq!(captures.order_query_count(), 1);
    drop(session);
    drop(process);
    PersistedQueryFixtureV1 {
        _directory: directory,
        persistence_root,
        dependencies,
        spec,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task421_paper_business_state_round_trip_preserves_authoritative_state() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task421-state-roundtrip");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建持久模拟盘运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建 PREPARED 会话");
    assert_eq!(
        prepared.control.disposition,
        SessionCreationDispositionV2::NewPrepared
    );
    let control = prepared.control;
    let mut session = prepared.session;
    assert_eq!(
        process
            .activate_session(activation_request(&spec, &control))
            .expect("激活模拟盘会话")
            .disposition,
        SessionInitializationControlDispositionV1::Activated
    );

    let enrollment_request = MarketEnrollmentRequestV1 {
        enrollment_request_id: "task421-enroll-appended".to_owned(),
        market: appended_market(),
    };
    let enrollment = process
        .enroll_market(&mut session, enrollment_request.clone())
        .expect("业务输入前追加市场");
    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let request = gtc_request(
        &session,
        &dependencies.latency_profile,
        "task421-open-maker",
        1,
        150_000,
    );
    let registration = session.post_order(request.clone()).expect("登记开放委托");
    drive_one_registered_action(
        &dependencies,
        &mut session,
        registration.item.effective_processing_at_utc_ms,
    );
    let received_at = response_received_at(
        &registration.receipt,
        registration.item.effective_processing_at_utc_ms,
    );
    apply_public_empty(&dependencies, &mut session, received_at);
    assert_eq!(
        session.drive_private_delivery_through(received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );

    let mut idempotent_request = request.clone();
    idempotent_request.context = context_for_arrival(
        &session,
        &dependencies.latency_profile,
        "task421-open-maker-idempotent-retry",
        2,
        175_000,
    );
    let idempotent_registration = session
        .post_order(idempotent_request.clone())
        .expect("以新逻辑请求复用同一业务效果");
    drive_one_registered_action(
        &dependencies,
        &mut session,
        idempotent_registration.item.effective_processing_at_utc_ms,
    );
    let idempotent_received_at = response_received_at(
        &idempotent_registration.receipt,
        idempotent_registration.item.effective_processing_at_utc_ms,
    );
    apply_public_empty(&dependencies, &mut session, idempotent_received_at);
    assert_eq!(
        session.drive_private_delivery_through(idempotent_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );

    let pending_request = gtc_request(
        &session,
        &dependencies.latency_profile,
        "task421-registered-before-crash",
        3,
        200_000,
    );
    let pending_registration = session
        .post_order(pending_request.clone())
        .expect("登记但暂不执行第二笔委托");
    let pending_effective_at = pending_registration.item.effective_processing_at_utc_ms;

    let condition_id = market().condition_id.0;
    let before_diagnostics = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(before_diagnostics.hot_orders.len(), 1);
    assert_eq!(
        before_diagnostics.hot_orders[0].status.known,
        Some(SimulatedOrderKnownStatusV1::Live)
    );
    let platform_order_id = before_diagnostics.hot_orders[0].platform_order_id.clone();
    let before_order = session
        .simulated_order(&platform_order_id)
        .expect("恢复前开放委托")
        .clone();
    let before_account = session.account_snapshot_for_tests();
    let before_sequence = session.next_account_transition_seq();
    let before_public_position = session.public_event_index_position();
    let before_revision = session.query_projection_revision();

    drop(session);
    drop(process);

    let recovered_captures = DirectCapturesV1::default();
    assert!(matches!(
        SimExecutionProcessV1::recover_paper_from_local_persistence(
            Arc::clone(&dependencies.finality_policy),
            Arc::clone(&dependencies.current_rules),
            Arc::clone(&dependencies.latency_profile),
            Arc::clone(&dependencies.scenario),
            dependencies.market_event_source.reader(),
            persistence_root.clone(),
        ),
        Err(SimExecutionPersistenceErrorV1::ProcessRoot(_))
    ));
    let mut recovered_process = dependencies
        .recover(persistence_root)
        .expect("恢复持久模拟盘运行根");
    let mut recovered = recovered_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            recovered_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑恢复会话");
    assert_eq!(recovered.account_snapshot_for_tests(), before_account);
    assert_eq!(recovered.next_account_transition_seq(), before_sequence);
    assert_eq!(
        recovered.public_event_index_position(),
        before_public_position
    );
    assert_eq!(recovered.query_projection_revision(), before_revision);
    assert_eq!(
        recovered.simulated_order(&platform_order_id),
        Some(&before_order)
    );
    assert_eq!(
        recovered.task41_diagnostics_for_tests(&condition_id),
        before_diagnostics
    );
    assert!(
        recovered
            .platform_market_state(&enrollment_request.market.market().condition_id.0)
            .is_some()
    );
    assert_eq!(
        recovered_process
            .enroll_market(&mut recovered, enrollment_request)
            .expect("恢复后重试市场追加"),
        enrollment
    );
    assert_eq!(
        recovered
            .post_order(request)
            .expect("恢复后重试业务请求")
            .receipt
            .disposition,
        crate::session::RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        recovered
            .post_order(idempotent_request)
            .expect("恢复后重试既有效果逻辑请求")
            .receipt
            .disposition,
        crate::session::RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        recovered
            .post_order(pending_request)
            .expect("恢复后重试已登记但未执行的业务请求")
            .receipt
            .disposition,
        crate::session::RequestRegistrationDispositionV1::PhysicalRetry
    );
    drive_one_registered_action(&dependencies, &mut recovered, pending_effective_at);
    assert_eq!(
        recovered
            .task41_diagnostics_for_tests(&condition_id)
            .hot_orders
            .len(),
        before_diagnostics.hot_orders.len() + 1
    );
    assert_eq!(recovered.next_account_transition_seq(), before_sequence + 1);
}

#[tokio::test(flavor = "current_thread")]
async fn task422_prepared_and_confirmed_outputs_recover_until_strong_durability_proof() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let spec = paper_spec(&dependencies, "task422-output-windows");
    let full_captures = DirectCapturesV1::default();
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建持久模拟盘运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            full_captures.bridge(OrderBridgeAcceptanceV1::Full),
        )
        .expect("创建输出恢复会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活输出恢复会话");
    let request = query_request(&session, &dependencies.latency_profile, "task422-query");
    let registration = session.order(request).expect("登记查询请求");
    let effective_at = registration.receipt.transit.platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, effective_at);
    let received_at = response_received_at(&registration.receipt, effective_at);
    apply_public_empty(&dependencies, &mut session, received_at);
    assert_eq!(
        session.drive_private_delivery_through(received_at),
        Ok(SubmissionDeliveryDispositionV1::Full),
        "桥拒收前必须已耐久准备完整输出"
    );
    let prepared_body = full_captures.last_order_query();
    assert_eq!(full_captures.order_query_count(), 1);

    drop(session);
    drop(process);

    let accepting_captures = DirectCapturesV1::default();
    let mut recovered_process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复桥拒收前状态");
    let mut recovered = recovered_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            accepting_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑桥拒收前会话");
    assert_eq!(
        recovered.drive_private_delivery_through(received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let confirmed_body = accepting_captures.last_order_query();
    assert_eq!(confirmed_body, prepared_body, "恢复必须重投同一完整正文");
    let confirmation = confirmed_body.metadata.delivery_confirmation();

    let mut jumped = confirmation.clone();
    jumped.private_feedback_sequence += 1;
    assert_eq!(
        recovered_process.advance_downstream_durability(
            &mut recovered,
            DownstreamDurabilityProofV1 {
                account_instance_key: control.account_instance_key.clone(),
                durable_through: jumped,
            },
        ),
        Err(SessionError::ResponseDeliveryIdentityConflict)
    );
    let mut wrong_account = control.account_instance_key.clone();
    wrong_account.account_id.push_str("-wrong");
    assert_eq!(
        recovered_process.advance_downstream_durability(
            &mut recovered,
            DownstreamDurabilityProofV1 {
                account_instance_key: wrong_account,
                durable_through: confirmation.clone(),
            },
        ),
        Err(SessionError::ResponseDeliveryIdentityConflict)
    );
    let mut wrong_delivery = confirmation.clone();
    wrong_delivery.delivery_id.push_str("-wrong");
    assert_eq!(
        recovered_process.advance_downstream_durability(
            &mut recovered,
            DownstreamDurabilityProofV1 {
                account_instance_key: control.account_instance_key.clone(),
                durable_through: wrong_delivery,
            },
        ),
        Err(SessionError::ResponseDeliveryIdentityConflict)
    );

    drop(recovered);
    drop(recovered_process);

    let durable_captures = DirectCapturesV1::default();
    let mut durable_process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复已确认但下游未耐久状态");
    let mut durable_session = durable_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            durable_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑已确认会话");
    let proof = DownstreamDurabilityProofV1 {
        account_instance_key: control.account_instance_key.clone(),
        durable_through: confirmation,
    };
    durable_process
        .advance_downstream_durability(&mut durable_session, proof.clone())
        .expect("接受下一个已确认输出的下游耐久证明");
    durable_process
        .advance_downstream_durability(&mut durable_session, proof)
        .expect("同一下游耐久证明必须幂等");
    assert_eq!(
        durable_session.drive_private_delivery_through(received_at),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse),
        "W+1 证明先到时不得再重投 W+1"
    );
    assert_eq!(durable_captures.order_query_count(), 0);

    drop(durable_session);
    drop(durable_process);

    let final_captures = DirectCapturesV1::default();
    let mut final_process = dependencies
        .recover(persistence_root)
        .expect("再次恢复下游耐久状态");
    let mut final_session = final_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            final_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("再次重绑下游耐久会话");
    assert_eq!(
        final_session.drive_private_delivery_through(received_at),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    assert_eq!(final_captures.order_query_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn task422_confirmed_output_does_not_consume_capacity_and_replays_contiguously() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let mut spec = paper_spec(&dependencies, "task422-capacity-one");
    spec.response_outbox_capacity = NonZeroUsize::new(1).expect("容量一");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建容量一持久运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建容量一会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活容量一会话");

    let first = session
        .order(query_request_at(
            &session,
            &dependencies.latency_profile,
            "task422-capacity-first",
            1,
            150_000,
        ))
        .expect("登记第一项查询");
    let first_effective_at = first.receipt.transit.platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, first_effective_at);
    let first_received_at = response_received_at(&first.receipt, first_effective_at);
    apply_public_empty(&dependencies, &mut session, first_received_at);
    assert_eq!(
        session.drive_private_delivery_through(first_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let first_body = captures.last_order_query();

    let second = session
        .order(query_request_at(
            &session,
            &dependencies.latency_profile,
            "task422-capacity-second",
            2,
            250_000,
        ))
        .expect("登记第二项查询");
    let second_effective_at = second.receipt.transit.platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, second_effective_at);
    let second_received_at = response_received_at(&second.receipt, second_effective_at);
    apply_public_empty(&dependencies, &mut session, second_received_at);
    assert_eq!(
        session.drive_private_delivery_through(second_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered),
        "已由桥确认但未推进下游耐久水位的输出不得占用待交付容量"
    );
    let second_body = captures.last_order_query();
    assert_eq!(captures.order_query_count(), 2);
    assert_eq!(
        second_body.metadata.private_feedback_sequence,
        first_body.metadata.private_feedback_sequence + 1
    );
    drop(session);
    drop(process);

    let replay_captures = DirectCapturesV1::default();
    let mut replay_process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复两个已确认但未下游耐久的输出");
    let mut replay_session = replay_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            replay_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑连续重放会话");
    assert_eq!(
        replay_session.drive_private_delivery_through(second_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        replay_session.drive_private_delivery_through(second_received_at),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    assert_eq!(
        replay_captures.order_queries(),
        vec![Arc::clone(&first_body), Arc::clone(&second_body)],
        "恢复必须按 W+1 连续重投原 sequence 与完整正文"
    );
    drop(replay_session);
    drop(replay_process);

    let proof_captures = DirectCapturesV1::default();
    let mut proof_process = dependencies
        .recover(persistence_root.clone())
        .expect("再次恢复两个已确认输出");
    let mut proof_session = proof_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            proof_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑先补证明会话");
    proof_process
        .advance_downstream_durability(
            &mut proof_session,
            DownstreamDurabilityProofV1 {
                account_instance_key: control.account_instance_key.clone(),
                durable_through: first_body.metadata.delivery_confirmation(),
            },
        )
        .expect("先补交第一项下游耐久证明");
    assert_eq!(
        proof_session.drive_private_delivery_through(second_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        proof_captures.order_queries(),
        vec![Arc::clone(&second_body)]
    );
    proof_process
        .advance_downstream_durability(
            &mut proof_session,
            DownstreamDurabilityProofV1 {
                account_instance_key: control.account_instance_key.clone(),
                durable_through: second_body.metadata.delivery_confirmation(),
            },
        )
        .expect("推进第二项下游耐久证明");
    assert_eq!(
        proof_session.drive_private_delivery_through(second_received_at),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    drop(proof_session);
    drop(proof_process);

    let final_captures = DirectCapturesV1::default();
    let mut final_process = dependencies
        .recover(persistence_root)
        .expect("恢复已推进两项下游耐久水位的状态");
    let mut final_session = final_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            final_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑最终会话");
    assert_eq!(
        final_session.drive_private_delivery_through(second_received_at),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    assert_eq!(final_captures.order_query_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn task421_three_initialization_states_recover_with_original_idempotency() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let prepared_spec = paper_spec(&dependencies, "task421-prepared");
    let active_spec = paper_spec(&dependencies, "task421-active");
    let failed_spec = paper_spec(&dependencies, "task421-failed");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建三态持久运行根");

    let prepared = process
        .create_session(
            prepared_spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建 PREPARED 会话");
    let prepared_control = prepared.control;
    let prepared_session = prepared.session;

    let active = process
        .create_session(
            active_spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建 ACTIVE 候选会话");
    let active_control = active.control;
    let active_session = active.session;
    let active_request = activation_request(&active_spec, &active_control);
    assert_eq!(
        process
            .activate_session(active_request.clone())
            .expect("激活 ACTIVE 会话")
            .snapshot
            .state,
        SessionInitializationStateV1::Active
    );

    let failed = process
        .create_session(
            failed_spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建 FAILED 候选会话");
    let failed_control = failed.control;
    let failed_session = failed.session;
    let failed_request = failure_request(&failed_spec, &failed_control);
    assert_eq!(
        process
            .fail_session_initialization(failed_request.clone())
            .expect("关闭 FAILED 会话")
            .snapshot
            .state,
        SessionInitializationStateV1::InitializationFailedClosed
    );

    drop(prepared_session);
    drop(active_session);
    drop(failed_session);
    drop(process);

    let mut recovered_process = dependencies
        .recover(persistence_root)
        .expect("恢复三种初始化状态");
    assert_eq!(
        recovered_process
            .retry_create_session(&prepared_spec)
            .expect("重试 PREPARED 创建")
            .disposition,
        SessionCreationDispositionV2::ExistingPrepared
    );
    assert!(matches!(
        recovered_process.reattach_recovered_paper_session(
            &prepared_spec.session_id,
            "wrong-bridge",
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SimExecutionPersistenceErrorV1::ResponseBridgeBindingConflict)
    ));
    let mut recovered_prepared = recovered_process
        .reattach_recovered_paper_session(
            &prepared_spec.session_id,
            &prepared_spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑 PREPARED 会话");
    assert!(matches!(
        recovered_prepared.order(query_request(
            &recovered_prepared,
            &dependencies.latency_profile,
            "task421-prepared-before-activation",
        )),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert_eq!(
        recovered_process
            .activate_session(activation_request(&prepared_spec, &prepared_control))
            .expect("恢复后激活 PREPARED 会话")
            .disposition,
        SessionInitializationControlDispositionV1::Activated
    );
    recovered_prepared
        .order(query_request(
            &recovered_prepared,
            &dependencies.latency_profile,
            "task421-prepared-after-activation",
        ))
        .expect("恢复会话必须共享根的唯一初始化状态");

    assert_eq!(
        recovered_process
            .activate_session(active_request)
            .expect("恢复后重试 ACTIVE 激活")
            .disposition,
        SessionInitializationControlDispositionV1::ExistingActive
    );
    let mut recovered_active = recovered_process
        .reattach_recovered_paper_session(
            &active_spec.session_id,
            &active_spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑 ACTIVE 会话");
    recovered_active
        .order(query_request(
            &recovered_active,
            &dependencies.latency_profile,
            "task421-active-request",
        ))
        .expect("ACTIVE 会话恢复后仍接受业务请求");

    assert_eq!(
        recovered_process
            .fail_session_initialization(failed_request)
            .expect("恢复后重试 FAILED 终态")
            .disposition,
        SessionInitializationControlDispositionV1::ExistingFailedClosed
    );
    assert_eq!(
        recovered_process.activate_session(activation_request(&failed_spec, &failed_control,)),
        Err(SessionError::InitializationFailedClosed)
    );
    let mut recovered_failed = recovered_process
        .reattach_recovered_paper_session(
            &failed_spec.session_id,
            &failed_spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑 FAILED 会话");
    assert!(matches!(
        recovered_failed.order(query_request(
            &recovered_failed,
            &dependencies.latency_profile,
            "task421-failed-request",
        )),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::InitializationFailedClosed
        ))
    ));

    let non_paper = session_spec(
        &dependencies.scenario,
        SimCallerRoleV1::ModelTrain,
        "task421-non-paper",
    );
    assert!(matches!(
        recovered_process.create_session(
            non_paper,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SessionError::InvalidSessionSpec(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task421_recovers_between_end_and_resolution_then_keeps_terminal_retry_idempotent() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task421-end-resolution");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建 end/resolution 持久运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建 end/resolution 会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活 end/resolution 会话");
    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let registration = session
        .post_order(gtc_request(
            &session,
            &dependencies.latency_profile,
            "task421-end-open-order",
            1,
            150_000,
        ))
        .expect("登记判定前开放委托");
    drive_one_registered_action(
        &dependencies,
        &mut session,
        registration.item.effective_processing_at_utc_ms,
    );
    apply_public_empty(&dependencies, &mut session, MARKET_END_AT_UTC_MS);
    let condition_id = market().condition_id.0;
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("end_at 后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    let after_end_account = session.account_snapshot_for_tests();
    let after_end_sequence = session.next_account_transition_seq();
    let after_end_diagnostics = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(after_end_diagnostics.hot_orders.len(), 1);
    assert_eq!(
        after_end_diagnostics.hot_orders[0].status.known,
        Some(SimulatedOrderKnownStatusV1::Live)
    );
    let platform_order_id = after_end_diagnostics.hot_orders[0]
        .platform_order_id
        .clone();

    drop(session);
    drop(process);

    let mut resolution_process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复 end_at 与 resolution 之间状态");
    let mut resolution_session = resolution_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑 end_at 后会话");
    assert_eq!(
        resolution_session.account_snapshot_for_tests(),
        after_end_account
    );
    assert_eq!(
        resolution_session.next_account_transition_seq(),
        after_end_sequence
    );
    assert_eq!(
        resolution_session.task41_diagnostics_for_tests(&condition_id),
        after_end_diagnostics
    );
    let resolution = resolution_event(510_000, 510_100);
    let resolution_bucket = public_bucket(
        &dependencies,
        &resolution_session,
        510_100,
        MillisecondBucketClosureV1::NextEventAt(510_101),
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        resolution_session.apply_closed_market_event_bucket(resolution_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        resolution_session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        resolution_session
            .platform_market_state(&condition_id)
            .expect("判定后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(
        resolution_session.principal_encumbered_pusd(),
        Decimal::ZERO
    );
    let terminal_account = resolution_session.account_snapshot_for_tests();
    let terminal_sequence = resolution_session.next_account_transition_seq();
    let terminal_diagnostics = resolution_session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(
        terminal_diagnostics.hot_orders[0].status.known,
        Some(SimulatedOrderKnownStatusV1::CanceledMarketResolved)
    );
    assert!(terminal_diagnostics.pending_lifecycle_feedback_count > 0);
    assert!(captures.order_lifecycle().is_empty());

    drop(resolution_session);
    drop(resolution_process);

    let mut terminal_process = dependencies
        .recover(persistence_root)
        .expect("恢复判定终态");
    let mut terminal_session = terminal_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑判定终态会话");
    assert_eq!(
        terminal_session.account_snapshot_for_tests(),
        terminal_account
    );
    assert_eq!(
        terminal_session.next_account_transition_seq(),
        terminal_sequence
    );
    assert_eq!(
        terminal_session.task41_diagnostics_for_tests(&condition_id),
        terminal_diagnostics
    );
    apply_public_empty(&dependencies, &mut terminal_session, 1_000_000);
    assert_eq!(
        terminal_session.drive_private_delivery_through(1_000_000),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let lifecycle_feedbacks = captures.order_lifecycle();
    assert!(lifecycle_feedbacks.iter().any(|feedback| {
        feedback.platform_order_id == platform_order_id
            && feedback.event_kind == OrderLifecycleEventKindV1::MarketResolvedCanceled
    }));
    assert_eq!(
        terminal_session.account_snapshot_for_tests(),
        terminal_account
    );
    assert_eq!(
        terminal_session.next_account_transition_seq(),
        terminal_sequence
    );
    assert_eq!(
        terminal_session.drive_private_delivery_through(1_000_000),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    assert_eq!(captures.order_lifecycle(), lifecycle_feedbacks);
}

#[tokio::test(flavor = "current_thread")]
async fn task421_resolution_waits_for_recovered_finality_predecessor() {
    let finality_policy = policy_for_tests(
        "task421-pending-resolution",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, 400_000),
        ],
        1,
        Vec::new(),
    );
    let dependencies = RecoveryDependenciesV1::new(finality_policy).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task421-resolution-finality");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建判定等待最终性的持久运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建判定等待最终性会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活判定等待最终性会话");
    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let registration = session
        .post_order(fak_request(
            &session,
            &dependencies.latency_profile,
            "task421-pending-fill",
            1,
            200_000,
        ))
        .expect("登记产生延迟最终性成交的 FAK");
    let effective_at = registration.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, effective_at);
    let finality_at = effective_at + 400_000;
    let condition_id = market().condition_id.0;
    let before_resolution = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(before_resolution.hot_trades.len(), 1);
    let trade_id = before_resolution.hot_trades[0].simulated_trade_id.clone();
    assert_eq!(
        before_resolution.hot_trades[0].status.known,
        Some(crate::execution::SimulatedTradeKnownStatusV1::Matched)
    );

    apply_public_empty(&dependencies, &mut session, MARKET_END_AT_UTC_MS);
    let resolution = resolution_event(550_000, 550_100);
    let resolution_bucket = public_bucket(
        &dependencies,
        &session,
        550_100,
        MillisecondBucketClosureV1::NextEventAt(550_101),
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("等待最终性的市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    let pending_account = session.account_snapshot_for_tests();
    let pending_sequence = session.next_account_transition_seq();
    let pending_diagnostics = session.task41_diagnostics_for_tests(&condition_id);
    let pending_trade = pending_diagnostics
        .hot_trades
        .iter()
        .find(|trade| trade.simulated_trade_id == trade_id)
        .expect("判定等待期成交");
    assert_eq!(
        pending_trade.status.known,
        Some(crate::execution::SimulatedTradeKnownStatusV1::Mined)
    );

    drop(session);
    drop(process);

    let mut recovered_process = dependencies
        .recover(persistence_root)
        .expect("恢复判定等待最终性状态");
    let mut recovered = recovered_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑判定等待最终性会话");
    assert_eq!(recovered.account_snapshot_for_tests(), pending_account);
    assert_eq!(recovered.next_account_transition_seq(), pending_sequence);
    assert_eq!(
        recovered.task41_diagnostics_for_tests(&condition_id),
        pending_diagnostics
    );
    apply_public_empty(&dependencies, &mut recovered, finality_at);
    assert_eq!(
        recovered.drive_next_execution_action_through(finality_at),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    assert_eq!(
        recovered
            .platform_market_state(&condition_id)
            .expect("最终性前驱完成后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    let terminal_diagnostics = recovered.task41_diagnostics_for_tests(&condition_id);
    let terminal_trade = terminal_diagnostics
        .hot_trades
        .iter()
        .find(|trade| trade.simulated_trade_id == trade_id)
        .expect("最终性完成后成交");
    assert_eq!(
        terminal_trade.status.known,
        Some(crate::execution::SimulatedTradeKnownStatusV1::Confirmed)
    );
    assert_ne!(recovered.account_snapshot_for_tests(), pending_account);
    assert!(recovered.next_account_transition_seq() > pending_sequence);
}

#[tokio::test(flavor = "current_thread")]
async fn task421_recovery_rejects_missing_scheduled_finality_after_storage_rebind() {
    let finality_policy = policy_for_tests(
        "task421-missing-scheduled-finality",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, 400_000),
        ],
        1,
        Vec::new(),
    );
    let dependencies = RecoveryDependenciesV1::new(finality_policy).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task421-missing-scheduled-finality");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建最终性调度损坏夹具运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建最终性调度损坏夹具会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活最终性调度损坏夹具会话");
    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let registration = session
        .post_order(fak_request(
            &session,
            &dependencies.latency_profile,
            "task421-missing-finality-fill",
            1,
            200_000,
        ))
        .expect("登记真实待最终性成交");
    drive_one_registered_action(
        &dependencies,
        &mut session,
        registration.item.effective_processing_at_utc_ms,
    );
    apply_public_empty(&dependencies, &mut session, MARKET_END_AT_UTC_MS);
    let resolution = resolution_event(550_000, 550_100);
    let resolution_bucket = public_bucket(
        &dependencies,
        &session,
        550_100,
        MillisecondBucketClosureV1::NextEventAt(550_101),
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let condition_id = market().condition_id.0;
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("损坏前判定等待市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(
        session
            .task41_diagnostics_for_tests(&condition_id)
            .hot_trades[0]
            .status
            .known,
        Some(crate::execution::SimulatedTradeKnownStatusV1::Mined)
    );
    drop(session);
    drop(process);

    mutate_latest_checkpoint(&persistence_root, true, |checkpoint| {
        let scheduled = checkpoint
            .pointer_mut(&format!(
                "/sessions/{}/session/execution_state/scheduled_finality",
                spec.session_id
            ))
            .and_then(Value::as_array_mut)
            .expect("真实待最终性成交必须有调度集合");
        assert_eq!(scheduled.len(), 1, "反例只删除唯一下一最终性前驱");
        scheduled.clear();
    });
    assert!(matches!(
        dependencies.recover(persistence_root),
        Err(SimExecutionPersistenceErrorV1::Session(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task421_recovery_rejects_checkpoint_body_rewrite_when_evidence_is_unchanged() {
    let fixture = persisted_query_fixture(
        "task421-prepared-closure-truncation",
        OrderBridgeAcceptanceV1::Full,
    )
    .await;
    mutate_latest_checkpoint(&fixture.persistence_root, false, |checkpoint| {
        let session = checkpoint
            .get_mut("sessions")
            .and_then(Value::as_object_mut)
            .and_then(|sessions| sessions.get_mut(&fixture.spec.session_id))
            .expect("必须有待交付输出的会话");
        let state = session
            .pointer_mut("/session/state")
            .expect("必须有会话私有事实状态");
        let pending = state
            .get_mut("pending_private_facts")
            .and_then(Value::as_array_mut)
            .expect("待交付私有事实必须为数组");
        assert_eq!(pending.len(), 1, "反例必须来自真实 Full 桥状态");
        pending.clear();
        let keys = state
            .get_mut("private_fact_keys")
            .and_then(Value::as_object_mut)
            .expect("私有事实键索引");
        assert_eq!(keys.len(), 1);
        keys.clear();
        let emissions = state
            .get_mut("used_private_emission_sequences")
            .and_then(Value::as_array_mut)
            .expect("私有发射序号集");
        assert_eq!(emissions.as_slice(), &[Value::from(1)]);
        emissions.clear();
        let entries = session
            .pointer_mut("/direct_response_outbox/entries")
            .and_then(Value::as_object_mut)
            .expect("直接响应发件箱");
        assert_eq!(entries.len(), 1);
        entries.clear();
    });
    assert!(matches!(
        fixture
            .dependencies
            .recover(fixture.persistence_root.clone()),
        Err(SimExecutionPersistenceErrorV1::Session(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task421_recovery_rejects_unreferenced_query_generation_state() {
    let fixture = persisted_query_fixture(
        "task421-unreferenced-query-generation",
        OrderBridgeAcceptanceV1::Accepted,
    )
    .await;
    mutate_latest_checkpoint(&fixture.persistence_root, true, |checkpoint| {
        let next_root_sequence = checkpoint
            .pointer_mut(&format!(
                "/sessions/{}/query_state/next_root_sequence",
                fixture.spec.session_id
            ))
            .and_then(|value| value.as_u64())
            .expect("查询检查点必须包含下一根序号");
        *checkpoint
            .pointer_mut(&format!(
                "/sessions/{}/query_state/next_root_sequence",
                fixture.spec.session_id
            ))
            .expect("查询检查点必须包含可写下一根序号") = Value::from(
            next_root_sequence
                .checked_add(1)
                .expect("测试查询根序号可以推进"),
        );
    });
    assert!(matches!(
        fixture.dependencies.recover(fixture.persistence_root),
        Err(SimExecutionPersistenceErrorV1::Session(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task421_recovery_rejects_raised_logical_platform_watermark() {
    let fixture = persisted_query_fixture(
        "task421-raised-logical-platform-watermark",
        OrderBridgeAcceptanceV1::Accepted,
    )
    .await;
    mutate_latest_checkpoint(&fixture.persistence_root, true, |checkpoint| {
        let session_path = format!("/sessions/{}/session", fixture.spec.session_id);
        let closed_action_path = format!("{session_path}/state/closed_action_through_utc_ms");
        let gtd_path = format!("{session_path}/execution_state/gtd_advanced_through_utc_ms");
        let current = checkpoint
            .pointer(&closed_action_path)
            .and_then(Value::as_i64)
            .expect("真实持久查询必须推进逻辑平台水位");
        let raised = current.checked_add(1).expect("测试逻辑平台水位可以推进");
        *checkpoint
            .pointer_mut(&closed_action_path)
            .expect("检查点必须包含可写逻辑动作水位") = Value::from(raised);
        *checkpoint
            .pointer_mut(&gtd_path)
            .expect("检查点必须包含可写 GTD 水位") = Value::from(raised);
    });
    assert!(matches!(
        fixture.dependencies.recover(fixture.persistence_root),
        Err(SimExecutionPersistenceErrorV1::Session(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task421_latest_immutable_generation_survives_stale_or_missing_current_pointer() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task421-current-pointer");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建 CURRENT 指针恢复夹具运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建 CURRENT 指针恢复夹具会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活 CURRENT 指针恢复夹具会话");
    let stale_current =
        std::fs::read(persistence_root.join("CURRENT")).expect("保留旧 CURRENT 指针");

    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let registration = session
        .post_order(gtc_request(
            &session,
            &dependencies.latency_profile,
            "task421-current-latest-order",
            1,
            150_000,
        ))
        .expect("登记 CURRENT 旧指针后的新业务状态");
    drive_one_registered_action(
        &dependencies,
        &mut session,
        registration.item.effective_processing_at_utc_ms,
    );
    let condition_id = market().condition_id.0;
    let latest_diagnostics = session.task41_diagnostics_for_tests(&condition_id);
    let latest_account = session.account_snapshot_for_tests();
    let latest_sequence = session.next_account_transition_seq();
    drop(session);
    drop(process);

    std::fs::write(persistence_root.join("CURRENT"), stale_current)
        .expect("将 CURRENT 回滚为旧指针");
    let mut stale_pointer_process = dependencies
        .recover(persistence_root.clone())
        .expect("旧 CURRENT 不得使不可变最新代回滚");
    let stale_pointer_session = stale_pointer_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑旧 CURRENT 指针恢复会话");
    assert_eq!(
        stale_pointer_session.task41_diagnostics_for_tests(&condition_id),
        latest_diagnostics
    );
    assert_eq!(
        stale_pointer_session.account_snapshot_for_tests(),
        latest_account
    );
    assert_eq!(
        stale_pointer_session.next_account_transition_seq(),
        latest_sequence
    );
    drop(stale_pointer_session);
    drop(stale_pointer_process);

    let evidence_before = evidence_file_names(&persistence_root);
    assert!(!evidence_before.is_empty());
    std::fs::remove_file(persistence_root.join("CURRENT"))
        .expect("构造 CURRENT 缺失但不删除仅追加证据的边界");
    let mut missing_pointer_process = dependencies
        .recover(persistence_root.clone())
        .expect("缺失 CURRENT 时应从最新完整不可变代恢复");
    let missing_pointer_session = missing_pointer_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑缺失 CURRENT 指针恢复会话");
    assert_eq!(
        missing_pointer_session.task41_diagnostics_for_tests(&condition_id),
        latest_diagnostics
    );
    assert_eq!(
        missing_pointer_session.account_snapshot_for_tests(),
        latest_account
    );
    assert_eq!(
        evidence_file_names(&persistence_root),
        evidence_before,
        "恢复不得删除最新不可变代所引用的仅追加证据"
    );
    assert!(persistence_root.join("CURRENT").is_file());
}

#[tokio::test(flavor = "current_thread")]
async fn task421_failure_before_checkpoint_publish_has_no_external_success_or_reserved_request() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task421-before-checkpoint-publish");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建检查点发布前失败夹具运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建检查点发布前失败夹具会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活检查点发布前失败夹具会话");
    let request = query_request(
        &session,
        &dependencies.latency_profile,
        "task421-before-checkpoint-publish-query",
    );
    process.fail_next_durable_commit_before_publish_for_tests();
    assert!(matches!(
        session.order(request.clone()),
        Err(SessionError::Durability(_))
    ));
    assert_eq!(captures.order_query_count(), 0);
    drop(session);
    drop(process);

    let recovered_captures = DirectCapturesV1::default();
    let mut recovered_process = dependencies
        .recover(persistence_root)
        .expect("检查点未发布时必须恢复上一完整代");
    let mut recovered = recovered_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            recovered_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑检查点未发布会话");
    assert_eq!(
        recovered
            .order(request)
            .expect("未发布请求恢复后必须仍是新逻辑请求")
            .receipt
            .disposition,
        crate::session::RequestRegistrationDispositionV1::NewLogicalRequest
    );
}

fn golden_recovery_request(
    bucket_at_utc_ms: i64,
    start_visibility_index_position: u64,
    next_visibility_index_position: u64,
) -> PaperMarketEventRecoveryReadRequestV1 {
    PaperMarketEventRecoveryReadRequestV1 {
        bucket_at_utc_ms,
        start_visibility_index_position,
        next_visibility_index_position,
    }
}

fn minimal_golden_market_event_source(up_wins: bool) -> GoldenMarketEventSourceV1 {
    GoldenMarketEventSourceV1::write(
        vec![
            GoldenMarketEventBucketInputV1 {
                bucket_at_utc_ms: 120_000,
                start_visibility_index_position: 0,
                next_visibility_index_position: 2,
                events: initial_events("0.01", "0.6", "1000").to_vec(),
            },
            GoldenMarketEventBucketInputV1 {
                bucket_at_utc_ms: MARKET_END_AT_UTC_MS,
                start_visibility_index_position: 2,
                next_visibility_index_position: 3,
                events: vec![end_marker_event()],
            },
            GoldenMarketEventBucketInputV1 {
                bucket_at_utc_ms: 510_100,
                start_visibility_index_position: 3,
                next_visibility_index_position: 4,
                events: vec![resolution_event_for_outcome_with_cursor(
                    510_000, 510_100, up_wins, 4,
                )],
            },
        ],
        true,
    )
}

struct PersistedGoldenAnchorV1 {
    _persistence_directory: tempfile::TempDir,
    persistence_root: PathBuf,
    dependencies: RecoveryDependenciesV1,
    source: GoldenMarketEventSourceV1,
}

async fn persisted_golden_anchor(
    anchor_closure_override: Option<MillisecondBucketClosureV1>,
) -> PersistedGoldenAnchorV1 {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let source = minimal_golden_market_event_source(true);
    let persistence_directory = tempfile::tempdir().expect("创建黄金恢复锚点持久目录");
    let persistence_root = persistence_directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task422a-golden-anchor");
    let mut process = dependencies
        .new_process_with_source(source.identity_sha256.clone(), persistence_root.clone())
        .expect("创建绑定黄金源的持久运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建黄金恢复锚点会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活黄金恢复锚点会话");
    assert_eq!(
        session.apply_closed_market_event_bucket(
            source.closed_bucket(golden_recovery_request(120_000, 0, 2))
        ),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let anchor_request = golden_recovery_request(MARKET_END_AT_UTC_MS, 2, 3);
    let anchor = if let Some(closure) = anchor_closure_override {
        let canonical = source.closed_bucket(anchor_request.clone());
        let forged = source.closed_bucket_with_closure(anchor_request.clone(), closure);
        assert_eq!(
            canonical.durable_reference().bucket_sha256,
            forged.durable_reference().bucket_sha256,
            "闭桶正文摘要不含闭合见证，恢复必须独立核对来源见证"
        );
        assert_ne!(
            canonical.durable_reference().closure,
            forged.durable_reference().closure
        );
        forged
    } else {
        source.closed_bucket(anchor_request.clone())
    };
    assert_eq!(
        session.apply_closed_market_event_bucket(anchor),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    drop(session);
    drop(process);
    PersistedGoldenAnchorV1 {
        _persistence_directory: persistence_directory,
        persistence_root,
        dependencies,
        source,
    }
}

fn market_event_source_error(
    result: Result<SimExecutionProcessV1, SimExecutionPersistenceErrorV1>,
) -> String {
    match result {
        Err(SimExecutionPersistenceErrorV1::MarketEventSource(message)) => message,
        Err(error) => panic!("预期市场事件源恢复拒绝，实际为：{error}"),
        Ok(_) => panic!("预期市场事件源恢复拒绝，实际恢复成功"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task422a_golden_source_rereads_anchor_and_continues_once_from_next_position() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let source = GoldenMarketEventSourceV1::write(
        vec![
            GoldenMarketEventBucketInputV1 {
                bucket_at_utc_ms: 120_000,
                start_visibility_index_position: 0,
                next_visibility_index_position: 2,
                events: initial_events("0.01", "0.6", "1000").to_vec(),
            },
            GoldenMarketEventBucketInputV1 {
                bucket_at_utc_ms: MARKET_END_AT_UTC_MS,
                start_visibility_index_position: 2,
                next_visibility_index_position: 3,
                events: vec![end_marker_event()],
            },
            GoldenMarketEventBucketInputV1 {
                bucket_at_utc_ms: 510_100,
                start_visibility_index_position: 3,
                next_visibility_index_position: 4,
                events: vec![resolution_event_for_outcome_with_cursor(
                    510_000, 510_100, true, 4,
                )],
            },
        ],
        true,
    );
    let persistence_directory = tempfile::tempdir().expect("创建黄金恢复正例持久目录");
    let persistence_root = persistence_directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task422a-golden-positive");
    let mut process = dependencies
        .new_process_with_source(source.identity_sha256.clone(), persistence_root.clone())
        .expect("创建黄金恢复正例运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建黄金恢复正例会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活黄金恢复正例会话");
    assert_eq!(
        session.apply_closed_market_event_bucket(
            source.closed_bucket(golden_recovery_request(120_000, 0, 2))
        ),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let registration = session
        .post_order(gtc_request(
            &session,
            &dependencies.latency_profile,
            "task422a-open-order",
            1,
            150_000,
        ))
        .expect("登记黄金恢复正例开放委托");
    assert_eq!(
        registration.item.effective_processing_at_utc_ms, 150_000,
        "黄金源固定动作闭桶必须与真实排定时点相同"
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(
            source.closed_bucket(golden_recovery_request(150_000, 2, 2))
        ),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(150_000),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(source.closed_bucket(golden_recovery_request(
            MARKET_END_AT_UTC_MS,
            2,
            3
        ))),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let condition_id = market().condition_id.0;
    let after_end_account = session.account_snapshot_for_tests();
    let after_end_sequence = session.next_account_transition_seq();
    let after_end_diagnostics = session.task41_diagnostics_for_tests(&condition_id);
    let platform_order_id = after_end_diagnostics.hot_orders[0]
        .platform_order_id
        .clone();
    let evidence_before_recovery = evidence_file_names(&persistence_root);
    drop(session);
    drop(process);

    let anchor_request = golden_recovery_request(MARKET_END_AT_UTC_MS, 2, 3);
    let mut recovered_process = dependencies
        .recover_with_reader(source.reader(), persistence_root.clone())
        .expect("从黄金持久源重读最后完整闭桶");
    assert_eq!(source.reads(), vec![anchor_request]);
    assert_eq!(
        evidence_file_names(&persistence_root),
        evidence_before_recovery
    );
    let mut recovered = recovered_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重绑黄金恢复正例会话");
    assert_eq!(recovered.account_snapshot_for_tests(), after_end_account);
    assert_eq!(recovered.next_account_transition_seq(), after_end_sequence);
    assert_eq!(
        recovered.task41_diagnostics_for_tests(&condition_id),
        after_end_diagnostics
    );
    assert_eq!(recovered.public_event_index_position(), 3);

    let resolution_bucket = source.closed_bucket(golden_recovery_request(510_100, 3, 4));
    assert_eq!(
        recovered.apply_closed_market_event_bucket(resolution_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let terminal_account = recovered.account_snapshot_for_tests();
    let terminal_sequence = recovered.next_account_transition_seq();
    assert_eq!(
        recovered.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(recovered.account_snapshot_for_tests(), terminal_account);
    assert_eq!(recovered.next_account_transition_seq(), terminal_sequence);
    assert_eq!(
        recovered.drive_next_execution_action_through(1_000_000),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    while matches!(
        recovered
            .drive_private_delivery_through(1_000_000)
            .expect("交付黄金恢复后的待可见输出"),
        SubmissionDeliveryDispositionV1::Delivered
    ) {}
    let lifecycle_feedbacks = captures.order_lifecycle();
    assert_eq!(
        lifecycle_feedbacks
            .iter()
            .filter(|feedback| {
                feedback.platform_order_id == platform_order_id
                    && feedback.event_kind == OrderLifecycleEventKindV1::MarketResolvedCanceled
            })
            .count(),
        1
    );
    assert_eq!(
        recovered.drive_private_delivery_through(1_000_000),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    assert_eq!(captures.order_lifecycle(), lifecycle_feedbacks);
}

#[tokio::test(flavor = "current_thread")]
async fn task422a_recovery_rejects_reader_whose_actual_source_differs_from_declared_identity() {
    let fixture = persisted_golden_anchor(None).await;
    let different_source = minimal_golden_market_event_source(false);
    let original_manifest = fixture.source.read_manifest().expect("读取原黄金源清单");
    let different_manifest = different_source.read_manifest().expect("读取异源黄金清单");
    assert_eq!(
        original_manifest.records[..2],
        different_manifest.records[..2]
    );
    assert_ne!(
        fixture.source.identity_sha256,
        different_source.identity_sha256
    );
    let asserted_identity_sha256 = fixture.source.identity_sha256.clone();
    let message = market_event_source_error(fixture.dependencies.recover_with_reader(
        different_source.reader_bound_to(asserted_identity_sha256),
        fixture.persistence_root,
    ));
    assert!(message.contains("黄金源清单内容身份不匹配"));
    assert_eq!(
        different_source.reads(),
        vec![golden_recovery_request(MARKET_END_AT_UTC_MS, 2, 3)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task422a_recovery_rejects_missing_anchor_object() {
    let fixture = persisted_golden_anchor(None).await;
    fixture
        .source
        .remove_object_for(&golden_recovery_request(MARKET_END_AT_UTC_MS, 2, 3));
    let message = market_event_source_error(
        fixture
            .dependencies
            .recover_with_reader(fixture.source.reader(), fixture.persistence_root),
    );
    assert!(message.contains("内容寻址对象"));
    assert!(message.contains("不可用"));
}

#[tokio::test(flavor = "current_thread")]
async fn task422a_recovery_rejects_truncated_tail_instead_of_treating_it_as_eof() {
    let fixture = persisted_golden_anchor(None).await;
    fixture
        .source
        .remove_object_for(&golden_recovery_request(510_100, 3, 4));
    let message = market_event_source_error(
        fixture
            .dependencies
            .recover_with_reader(fixture.source.reader(), fixture.persistence_root),
    );
    assert!(message.contains("无法从完整下一对象证明闭桶尾部"));
}

#[tokio::test(flavor = "current_thread")]
async fn task422a_recovery_rejects_forged_closure_even_when_bucket_body_hash_is_unchanged() {
    let fixture =
        persisted_golden_anchor(Some(MillisecondBucketClosureV1::NextEventAt(510_200))).await;
    let message = market_event_source_error(
        fixture
            .dependencies
            .recover_with_reader(fixture.source.reader(), fixture.persistence_root),
    );
    assert!(message.contains("闭合见证与耐久引用不一致"));
}

#[tokio::test(flavor = "current_thread")]
async fn task423_hot_eviction_waits_for_durable_barriers_and_recovers_from_compact_state() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2.3 热驱逐持久目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task423-hot-eviction");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建任务 4.2.3 热驱逐运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建任务 4.2.3 热驱逐会话");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活任务 4.2.3 热驱逐会话");
    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let maker_request = gtc_request(
        &session,
        &dependencies.latency_profile,
        "task423-terminal-maker",
        1,
        150_000,
    );
    let maker_registration = session
        .post_order(maker_request)
        .expect("登记任务 4.2.3 真实 maker 成交请求");
    let maker_effective_at = maker_registration.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, maker_effective_at);
    let maker_queue_tail =
        maker_bid_transition(maker_effective_at + 1, maker_effective_at + 1, 3, "0", "5");
    let maker_queue_tail_bucket = public_bucket(
        &dependencies,
        &session,
        maker_effective_at + 1,
        MillisecondBucketClosureV1::NextEventAt(maker_effective_at + 2),
        std::slice::from_ref(&maker_queue_tail),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(maker_queue_tail_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let maker_depletion =
        maker_bid_transition(maker_effective_at + 2, maker_effective_at + 2, 4, "5", "0");
    let maker_depletion_bucket = public_bucket(
        &dependencies,
        &session,
        maker_effective_at + 2,
        MillisecondBucketClosureV1::NextEventAt(maker_effective_at + 3),
        std::slice::from_ref(&maker_depletion),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(maker_depletion_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    apply_public_empty(&dependencies, &mut session, maker_effective_at + 22);
    assert_eq!(
        session.drive_next_execution_action_through(maker_effective_at + 22),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    let order_request = fak_request(
        &session,
        &dependencies.latency_profile,
        "task423-terminal-fill",
        2,
        200_000,
    );
    let order_registration = session
        .post_order(order_request.clone())
        .expect("登记任务 4.2.3 终态成交请求");
    let order_effective_at = order_registration.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, order_effective_at);
    apply_public_empty(&dependencies, &mut session, order_effective_at + 20);
    assert_eq!(
        session.drive_next_execution_action_through(order_effective_at + 20),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    apply_public_empty(&dependencies, &mut session, MARKET_END_AT_UTC_MS);
    let resolution = resolution_event_for_outcome_with_cursor(510_000, 510_100, true, 5);
    let resolution_bucket = public_bucket(
        &dependencies,
        &session,
        510_100,
        MillisecondBucketClosureV1::NextEventAt(510_101),
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let condition_id = market().condition_id.0;
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("任务 4.2.3 驱逐前市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    apply_public_empty(&dependencies, &mut session, 1_000_000);
    assert_eq!(
        session.drive_private_delivery_through(1_000_000),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let terminal = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(terminal.hot_orders.len(), 2);
    assert_eq!(terminal.hot_trades.len(), 2);
    assert!(
        terminal
            .hot_trades
            .iter()
            .any(|trade| trade.liquidity_role == TradeLiquidityRoleV1::MakerInferred)
    );
    assert!(
        terminal
            .hot_trades
            .iter()
            .any(|trade| trade.liquidity_role == TradeLiquidityRoleV1::Taker)
    );
    assert_eq!(terminal.compact_orders, terminal.hot_orders);
    assert_eq!(terminal.compact_trades, terminal.hot_trades);
    let platform_order_id = terminal.hot_orders[0].platform_order_id.clone();

    let query_request = OrderRequestV1 {
        context: context_for_arrival(
            &session,
            &dependencies.latency_profile,
            "task423-terminal-query",
            3,
            1_100_000,
        ),
        platform_order_id: platform_order_id.clone(),
    };
    let query_registration = session
        .order(query_request.clone())
        .expect("登记任务 4.2.3 终态订单查询");
    let query_effective_at = query_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, query_effective_at);
    let query_received_at = response_received_at(&query_registration.receipt, query_effective_at);
    apply_public_empty(&dependencies, &mut session, query_received_at);
    assert_eq!(
        session.drive_private_delivery_through(query_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let before_query = captures.last_order_query();
    assert!(matches!(
        &before_query.result,
        OrderQueryResultV1::Found(order) if order.platform_order_id == platform_order_id
    ));
    let simulated_trade_id = terminal.hot_trades[0].simulated_trade_id.clone();
    let trade_query_request = TradesRequestV1 {
        context: context_for_arrival(
            &session,
            &dependencies.latency_profile,
            "task423-terminal-trade-query",
            4,
            1_110_000,
        ),
        filter: TradesFilterV1 {
            trade_id: Some(simulated_trade_id.clone()),
            ..TradesFilterV1::default()
        },
        cursor: None,
    };
    let trade_query_registration = session
        .trades(trade_query_request.clone())
        .expect("登记任务 4.2.3 终态成交查询");
    let trade_query_effective_at = trade_query_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, trade_query_effective_at);
    let trade_query_received_at =
        response_received_at(&trade_query_registration.receipt, trade_query_effective_at);
    apply_public_empty(&dependencies, &mut session, trade_query_received_at);
    assert_eq!(
        session.drive_private_delivery_through(trade_query_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let before_trade_query = captures.last_trade_query();
    assert!(matches!(
        &before_trade_query.result,
        crate::execution::TradesQueryResultV1::Page(page)
            if page.items.len() == 1
                && page.items[0].simulated_trade_id == simulated_trade_id
    ));
    let hot_orders_registration = session
        .orders(OrdersRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-terminal-orders",
                5,
                1_120_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("登记驱逐前活动订单列表查询");
    let hot_orders_at = hot_orders_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, hot_orders_at);
    let hot_orders_received_at =
        response_received_at(&hot_orders_registration.receipt, hot_orders_at);
    apply_public_empty(&dependencies, &mut session, hot_orders_received_at);
    assert_eq!(
        session.drive_private_delivery_through(hot_orders_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let OrdersQueryResultV1::Page(hot_orders_page) = &captures.last_orders_query().result else {
        panic!("终态订单不得出现在活动订单列表")
    };
    assert!(hot_orders_page.items.is_empty());

    let hot_cancel_registration = session
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-terminal-cancel",
                6,
                1_130_000,
            ),
            idempotency_key: "task423-terminal-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task423-terminal-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("登记驱逐前终态订单撤单");
    let hot_cancel_at = hot_cancel_registration.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, hot_cancel_at);
    let hot_cancel_received_at =
        response_received_at(&hot_cancel_registration.receipt, hot_cancel_at);
    apply_public_empty(&dependencies, &mut session, hot_cancel_received_at);
    assert_eq!(
        session.drive_private_delivery_through(hot_cancel_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let hot_cancel_outcome = captures.last_cancel_order().target.outcome.clone();
    assert_eq!(
        hot_cancel_outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(
            CancelNotCanceledReasonV1::NoCancellableRemainder,
        )
    );
    let before_eviction_market = session
        .platform_market_state(&condition_id)
        .expect("屏障拒绝前终态市场仍在热集合")
        .clone();
    let before_eviction = session.task41_diagnostics_for_tests(&condition_id);
    let mut confirmations = before_eviction
        .confirmed_direct_response_confirmations
        .iter()
        .chain(
            before_eviction
                .confirmed_lifecycle_feedback_confirmations
                .iter(),
        )
        .cloned()
        .collect::<Vec<_>>();
    confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    confirmations.dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    assert!(
        !confirmations.is_empty(),
        "终态订单、成交与查询必须形成待下游耐久的私有输出"
    );

    assert!(matches!(
        process.evict_terminal_market(&mut session, &condition_id),
        Err(SessionError::RetentionBarrierNotSatisfied(
            "强下游耐久水位尚未越过全部已确认输出"
        ))
    ));
    assert_eq!(
        session.task41_diagnostics_for_tests(&condition_id),
        before_eviction
    );
    assert_eq!(
        session.platform_market_state(&condition_id),
        Some(&before_eviction_market)
    );

    let mut durable_through_sequence = 0;
    for durable_through in confirmations {
        durable_through_sequence = durable_through.private_feedback_sequence;
        process
            .advance_downstream_durability(
                &mut session,
                DownstreamDurabilityProofV1 {
                    account_instance_key: control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("按连续私有序号补齐任务 4.2.3 下游耐久屏障");
    }
    let ready_to_evict = session.task41_diagnostics_for_tests(&condition_id);
    process.fail_next_durable_commit_before_publish_for_tests();
    assert!(matches!(
        process.evict_terminal_market(&mut session, &condition_id),
        Err(SessionError::Durability(_))
    ));
    assert_eq!(
        session.task41_diagnostics_for_tests(&condition_id),
        ready_to_evict
    );
    assert_eq!(
        session.platform_market_state(&condition_id),
        Some(&before_eviction_market)
    );

    drop(session);
    drop(process);
    let recovered_captures = DirectCapturesV1::default();
    let mut recovered_process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复提交前驱逐故障后的完整热状态");
    let mut recovered = recovered_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            recovered_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重附接提交前驱逐故障后的会话");
    assert_eq!(
        recovered.task41_diagnostics_for_tests(&condition_id),
        ready_to_evict
    );
    assert_eq!(
        recovered.platform_market_state(&condition_id),
        Some(&before_eviction_market)
    );
    recovered_process
        .evict_terminal_market(&mut recovered, &condition_id)
        .expect("恢复后跨过全部屏障才能驱逐终态市场热状态");
    let evicted = recovered.task41_diagnostics_for_tests(&condition_id);
    assert!(evicted.hot_orders.is_empty());
    assert!(evicted.hot_trades.is_empty());
    assert_eq!(evicted.compact_orders, ready_to_evict.compact_orders);
    assert_eq!(evicted.compact_trades, ready_to_evict.compact_trades);
    assert!(recovered.platform_market_state(&condition_id).is_none());
    assert_eq!(
        recovered
            .post_order(order_request)
            .expect("驱逐后原业务请求物理重试")
            .receipt
            .disposition,
        crate::session::RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        recovered
            .order(query_request)
            .expect("驱逐后原查询物理重试")
            .receipt
            .disposition,
        crate::session::RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(recovered_captures.order_query_count(), 0);

    let evicted_orders_registration = recovered
        .orders(OrdersRequestV1 {
            context: context_for_arrival(
                &recovered,
                &dependencies.latency_profile,
                "task423-evicted-orders",
                7,
                1_200_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("登记驱逐后活动订单列表查询");
    let evicted_orders_at = evicted_orders_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut recovered, evicted_orders_at);
    let evicted_orders_received_at =
        response_received_at(&evicted_orders_registration.receipt, evicted_orders_at);
    apply_public_empty(&dependencies, &mut recovered, evicted_orders_received_at);
    assert_eq!(
        recovered.drive_private_delivery_through(evicted_orders_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let OrdersQueryResultV1::Page(evicted_orders_page) =
        &recovered_captures.last_orders_query().result
    else {
        panic!("驱逐后活动订单列表必须返回正常页")
    };
    assert!(evicted_orders_page.items.is_empty());

    let evicted_cancel_registration = recovered
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &recovered,
                &dependencies.latency_profile,
                "task423-evicted-cancel",
                8,
                1_210_000,
            ),
            idempotency_key: "task423-evicted-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task423-evicted-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("登记驱逐后终态订单撤单");
    let evicted_cancel_at = evicted_cancel_registration
        .item
        .effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut recovered, evicted_cancel_at);
    let evicted_cancel_received_at =
        response_received_at(&evicted_cancel_registration.receipt, evicted_cancel_at);
    apply_public_empty(&dependencies, &mut recovered, evicted_cancel_received_at);
    assert_eq!(
        recovered.drive_private_delivery_through(evicted_cancel_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        recovered_captures.last_cancel_order().target.outcome,
        hot_cancel_outcome,
        "终态订单撤单结果不得因热状态驱逐改变"
    );
    let after_eviction_outputs = recovered.task41_diagnostics_for_tests(&condition_id);
    let mut after_eviction_confirmations = after_eviction_outputs
        .confirmed_direct_response_confirmations
        .iter()
        .chain(
            after_eviction_outputs
                .confirmed_lifecycle_feedback_confirmations
                .iter(),
        )
        .filter(|confirmation| confirmation.private_feedback_sequence > durable_through_sequence)
        .cloned()
        .collect::<Vec<_>>();
    after_eviction_confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    after_eviction_confirmations
        .dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    for durable_through in after_eviction_confirmations {
        recovered_process
            .advance_downstream_durability(
                &mut recovered,
                DownstreamDurabilityProofV1 {
                    account_instance_key: control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("紧凑重启前补齐驱逐后查询与撤单输出的下游耐久屏障");
    }

    drop(recovered);
    drop(recovered_process);
    let compact_captures = DirectCapturesV1::default();
    let mut compact_process = dependencies
        .recover(persistence_root)
        .expect("从已删除热状态的紧凑检查点恢复");
    let mut compact_session = compact_process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            compact_captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("重附接紧凑恢复会话");
    assert!(
        compact_session
            .task41_diagnostics_for_tests(&condition_id)
            .hot_orders
            .is_empty()
    );
    let compact_query = OrderRequestV1 {
        context: context_for_arrival(
            &compact_session,
            &dependencies.latency_profile,
            "task423-compact-query",
            9,
            1_400_000,
        ),
        platform_order_id: platform_order_id.clone(),
    };
    let compact_registration = compact_session
        .order(compact_query)
        .expect("紧凑恢复后登记等价订单查询");
    let compact_effective_at = compact_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut compact_session, compact_effective_at);
    let compact_received_at =
        response_received_at(&compact_registration.receipt, compact_effective_at);
    apply_public_empty(&dependencies, &mut compact_session, compact_received_at);
    assert_eq!(
        compact_session.drive_private_delivery_through(compact_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        compact_captures.last_order_query().result,
        before_query.result,
        "热状态驱逐并跨进程恢复后查询正文必须等价"
    );
    let compact_trade_query = TradesRequestV1 {
        context: context_for_arrival(
            &compact_session,
            &dependencies.latency_profile,
            "task423-compact-trade-query",
            10,
            1_410_000,
        ),
        filter: TradesFilterV1 {
            trade_id: Some(simulated_trade_id),
            ..TradesFilterV1::default()
        },
        cursor: None,
    };
    let compact_trade_registration = compact_session
        .trades(compact_trade_query)
        .expect("紧凑恢复后登记等价成交查询");
    let compact_trade_effective_at = compact_trade_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(
        &dependencies,
        &mut compact_session,
        compact_trade_effective_at,
    );
    let compact_trade_received_at = response_received_at(
        &compact_trade_registration.receipt,
        compact_trade_effective_at,
    );
    apply_public_empty(
        &dependencies,
        &mut compact_session,
        compact_trade_received_at,
    );
    assert_eq!(
        compact_session.drive_private_delivery_through(compact_trade_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let TradesQueryResultV1::Page(compact_page) =
        compact_captures.last_trade_query().result.clone()
    else {
        panic!("紧凑恢复后成交查询必须返回保留期内结果页");
    };
    let TradesQueryResultV1::Page(before_page) = before_trade_query.result.clone() else {
        panic!("热状态驱逐前成交查询必须返回保留期内结果页");
    };
    assert_eq!(
        compact_page.items, before_page.items,
        "热状态驱逐并跨进程恢复后成交业务投影必须等价"
    );

    let compact_orders_registration = compact_session
        .orders(OrdersRequestV1 {
            context: context_for_arrival(
                &compact_session,
                &dependencies.latency_profile,
                "task423-compact-orders",
                11,
                1_420_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("紧凑恢复后登记活动订单列表查询");
    let compact_orders_at = compact_orders_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut compact_session, compact_orders_at);
    let compact_orders_received_at =
        response_received_at(&compact_orders_registration.receipt, compact_orders_at);
    apply_public_empty(
        &dependencies,
        &mut compact_session,
        compact_orders_received_at,
    );
    assert_eq!(
        compact_session.drive_private_delivery_through(compact_orders_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let OrdersQueryResultV1::Page(compact_orders_page) =
        &compact_captures.last_orders_query().result
    else {
        panic!("紧凑恢复后活动订单列表必须返回正常页")
    };
    assert!(compact_orders_page.items.is_empty());

    let compact_cancel_registration = compact_session
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &compact_session,
                &dependencies.latency_profile,
                "task423-compact-cancel",
                12,
                1_430_000,
            ),
            idempotency_key: "task423-compact-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task423-compact-cancel-item".to_owned(),
                platform_order_id,
            },
        })
        .expect("紧凑恢复后登记终态订单撤单");
    let compact_cancel_at = compact_cancel_registration
        .item
        .effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut compact_session, compact_cancel_at);
    let compact_cancel_received_at =
        response_received_at(&compact_cancel_registration.receipt, compact_cancel_at);
    apply_public_empty(
        &dependencies,
        &mut compact_session,
        compact_cancel_received_at,
    );
    assert_eq!(
        compact_session.drive_private_delivery_through(compact_cancel_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        compact_captures.last_cancel_order().target.outcome,
        hot_cancel_outcome,
        "终态订单撤单结果在紧凑恢复后必须保持等价"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task423_task431_retention_expiry_removes_session_and_seals_auditable_artifacts() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.2.3 会话退休持久目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task423-retired-session");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建任务 4.2.3 会话退休运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建任务 4.2.3 会话退休夹具");
    let control = prepared.control;
    let mut session = prepared.session;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活任务 4.2.3 会话退休夹具");
    load_initial_market(&dependencies, &mut session, "0.01", "0.6", "1000");
    let fill_request = fak_request(
        &session,
        &dependencies.latency_profile,
        "task423-retirement-terminal-fill",
        1,
        200_000,
    );
    let fill_registration = session
        .post_order(fill_request)
        .expect("登记任务 4.2.3 会话退休终态成交请求");
    let fill_effective_at = fill_registration.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, fill_effective_at);
    apply_public_empty(&dependencies, &mut session, fill_effective_at + 20);
    assert_eq!(
        session.drive_next_execution_action_through(fill_effective_at + 20),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    session
        .advance_order_history_retention_floor(600_000)
        .expect("先于终态化推进终态订单保留水位");
    session
        .advance_trade_history_retention_floor(600_000)
        .expect("先于终态化推进终态成交保留水位");
    apply_public_empty(&dependencies, &mut session, MARKET_END_AT_UTC_MS);
    let resolution = resolution_event(510_000, 510_100);
    let terminal_bucket = public_bucket(
        &dependencies,
        &session,
        510_100,
        MillisecondBucketClosureV1::NextEventAt(1_000_000),
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(terminal_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let condition_id = market().condition_id.0;
    apply_public_empty(&dependencies, &mut session, 550_000);
    while matches!(
        session
            .drive_private_delivery_through(550_000)
            .expect("交付任务 4.2.3 终态成交输出"),
        SubmissionDeliveryDispositionV1::Delivered
    ) {}
    let terminal = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(terminal.hot_orders.len(), 1);
    assert_eq!(terminal.hot_trades.len(), 1);
    let platform_order_id = terminal.hot_orders[0].platform_order_id.clone();
    let simulated_trade_id = terminal.hot_trades[0].simulated_trade_id.clone();

    let pre_eviction_order = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-expired-hot-order",
                2,
                610_000,
            ),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("登记驱逐前已过保留水位的终态订单查询");
    let pre_eviction_order_at = pre_eviction_order
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, pre_eviction_order_at);
    let pre_eviction_order_received_at =
        response_received_at(&pre_eviction_order.receipt, pre_eviction_order_at);
    apply_public_empty(&dependencies, &mut session, pre_eviction_order_received_at);
    assert_eq!(
        session.drive_private_delivery_through(pre_eviction_order_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let pre_eviction_order_result = captures.last_order_query().result.clone();
    assert!(matches!(
        &pre_eviction_order_result,
        OrderQueryResultV1::OutOfRetention(_)
    ));

    let pre_eviction_trade = session
        .trades(TradesRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-expired-hot-trade",
                3,
                620_000,
            ),
            filter: TradesFilterV1 {
                trade_id: Some(simulated_trade_id.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("登记驱逐前已过保留水位的终态成交查询");
    let pre_eviction_trade_at = pre_eviction_trade
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, pre_eviction_trade_at);
    let pre_eviction_trade_received_at =
        response_received_at(&pre_eviction_trade.receipt, pre_eviction_trade_at);
    apply_public_empty(&dependencies, &mut session, pre_eviction_trade_received_at);
    assert_eq!(
        session.drive_private_delivery_through(pre_eviction_trade_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert!(matches!(
        captures.last_trade_query().result,
        TradesQueryResultV1::OutOfRetention(_)
    ));

    let pre_eviction_cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-expired-hot-cancel",
                4,
                630_000,
            ),
            idempotency_key: "task423-expired-hot-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task423-expired-hot-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("登记驱逐前已过保留水位的终态订单撤单");
    let pre_eviction_cancel_at = pre_eviction_cancel.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, pre_eviction_cancel_at);
    let pre_eviction_cancel_received_at =
        response_received_at(&pre_eviction_cancel.receipt, pre_eviction_cancel_at);
    apply_public_empty(&dependencies, &mut session, pre_eviction_cancel_received_at);
    assert_eq!(
        session.drive_private_delivery_through(pre_eviction_cancel_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let pre_eviction_cancel_outcome = captures.last_cancel_order().target.outcome.clone();
    assert_eq!(
        pre_eviction_cancel_outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(CancelNotCanceledReasonV1::NotFoundOrNotOwned,)
    );

    let before_eviction = session.task41_diagnostics_for_tests(&condition_id);
    assert!(!before_eviction.hot_orders.is_empty());
    assert!(!before_eviction.hot_trades.is_empty());
    assert!(before_eviction.retained_rebate_fill_state_count > 0);
    assert!(before_eviction.compact_orders.is_empty());
    assert!(before_eviction.compact_trades.is_empty());
    let mut confirmations = before_eviction
        .confirmed_direct_response_confirmations
        .iter()
        .chain(
            before_eviction
                .confirmed_lifecycle_feedback_confirmations
                .iter(),
        )
        .cloned()
        .collect::<Vec<_>>();
    confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    confirmations.dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    let mut durable_through_sequence = confirmations
        .last()
        .map_or(0, |confirmation| confirmation.private_feedback_sequence);
    for durable_through in confirmations {
        durable_through_sequence = durable_through.private_feedback_sequence;
        process
            .advance_downstream_durability(
                &mut session,
                DownstreamDurabilityProofV1 {
                    account_instance_key: control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("补齐驱逐前终态输出的下游耐久屏障");
    }
    process
        .evict_terminal_market(&mut session, &condition_id)
        .expect("保留水位先推进后仍可驱逐终态市场");
    let after_eviction = session.task41_diagnostics_for_tests(&condition_id);
    assert!(after_eviction.hot_orders.is_empty());
    assert!(after_eviction.hot_trades.is_empty());
    assert_eq!(after_eviction.retained_rebate_fill_state_count, 0);
    assert!(after_eviction.compact_orders.is_empty());
    assert!(after_eviction.compact_trades.is_empty());

    let post_eviction_order = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-expired-compact-order",
                5,
                700_000,
            ),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("登记驱逐后已过保留水位的终态订单查询");
    let post_eviction_order_at = post_eviction_order
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, post_eviction_order_at);
    let post_eviction_order_received_at =
        response_received_at(&post_eviction_order.receipt, post_eviction_order_at);
    apply_public_empty(&dependencies, &mut session, post_eviction_order_received_at);
    assert_eq!(
        session.drive_private_delivery_through(post_eviction_order_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        captures.last_order_query().result,
        pre_eviction_order_result,
        "终态订单查询结果不得因热态驱逐改变"
    );

    let post_eviction_trade = session
        .trades(TradesRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-expired-compact-trade",
                6,
                710_000,
            ),
            filter: TradesFilterV1 {
                trade_id: Some(simulated_trade_id),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("登记驱逐后已过保留水位的终态成交查询");
    let post_eviction_trade_at = post_eviction_trade
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, post_eviction_trade_at);
    let post_eviction_trade_received_at =
        response_received_at(&post_eviction_trade.receipt, post_eviction_trade_at);
    apply_public_empty(&dependencies, &mut session, post_eviction_trade_received_at);
    assert_eq!(
        session.drive_private_delivery_through(post_eviction_trade_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert!(matches!(
        captures.last_trade_query().result,
        TradesQueryResultV1::OutOfRetention(_)
    ));

    let post_eviction_cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-expired-compact-cancel",
                7,
                715_000,
            ),
            idempotency_key: "task423-expired-compact-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task423-expired-compact-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("登记驱逐后已过保留水位的终态订单撤单");
    let post_eviction_cancel_at = post_eviction_cancel.item.effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, post_eviction_cancel_at);
    let post_eviction_cancel_received_at =
        response_received_at(&post_eviction_cancel.receipt, post_eviction_cancel_at);
    apply_public_empty(
        &dependencies,
        &mut session,
        post_eviction_cancel_received_at,
    );
    assert_eq!(
        session.drive_private_delivery_through(post_eviction_cancel_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        captures.last_cancel_order().target.outcome,
        pre_eviction_cancel_outcome,
        "已过保留水位的终态撤单结果不得因热状态驱逐改变"
    );

    let post_eviction_buy = gtc_request(
        &session,
        &dependencies.latency_profile,
        "task423-post-eviction-buy",
        8,
        720_000,
    );
    let post_eviction_buy_registration = session
        .post_order(post_eviction_buy)
        .expect("驱逐后新买入请求仍须形成确定性拒绝响应");
    let post_eviction_buy_at = post_eviction_buy_registration
        .item
        .effective_processing_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, post_eviction_buy_at);
    let post_eviction_buy_received_at = response_received_at(
        &post_eviction_buy_registration.receipt,
        post_eviction_buy_at,
    );
    apply_public_empty(&dependencies, &mut session, post_eviction_buy_received_at);
    assert_eq!(
        session.drive_private_delivery_through(post_eviction_buy_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert!(matches!(
        captures.last_post_order().item.outcome,
        SubmissionItemOutcomeV1::RejectedNoEffect(SubmissionRejectedReasonV1::EndAtReached)
    ));

    let retained_root_query = session
        .orders(OrdersRequestV1 {
            context: context_for_arrival(
                &session,
                &dependencies.latency_profile,
                "task423-retirement-root-barrier",
                9,
                725_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("登记会话退休前尚未跨过水位的查询根");
    let retained_root_query_at = retained_root_query
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    drive_one_registered_action(&dependencies, &mut session, retained_root_query_at);
    let retained_root_query_received_at =
        response_received_at(&retained_root_query.receipt, retained_root_query_at);
    apply_public_empty(&dependencies, &mut session, retained_root_query_received_at);
    assert_eq!(
        session.drive_private_delivery_through(retained_root_query_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let OrdersQueryResultV1::Page(retained_root_page) = &captures.last_orders_query().result else {
        panic!("会话退休查询根夹具必须返回正常页")
    };
    assert!(retained_root_page.items.is_empty());

    let account_before_late_event = session.account_snapshot_for_tests();
    let account_seq_before_late_event = session.next_account_transition_seq();
    let revision_before_late_event = session.query_projection_revision();
    let mut late_best_bid_ask = sealed_event(
        ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
            asset: asset("1", 0, "Up"),
            best_bid_pusd_per_share: Some(ExactDecimal("0.45".to_owned())),
            best_ask_pusd_per_share: Some(ExactDecimal("0.55".to_owned())),
        }),
        orderbook_evidence(729_900, 730_000, 4),
        fresh_quality(),
    );
    late_best_bid_ask.lineage.canonical_rule_version = FULL_EVENT_TAPE_CANARY.to_owned();
    late_best_bid_ask.event_id = derive_execution_market_event_v3_id(&late_best_bid_ask)
        .expect("重算带完整事件带 canary 的规范事件身份");
    let late_bucket = public_bucket(
        &dependencies,
        &session,
        730_000,
        MillisecondBucketClosureV1::NextEventAt(730_001),
        std::slice::from_ref(&late_best_bid_ask),
    );
    let late_bucket_reference = late_bucket.durable_reference();
    let late_event_bytes = encode_execution_market_event_v3(&late_best_bid_ask)
        .expect("编码带完整事件带 canary 的规范事件");
    let canary_offset = late_event_bytes
        .windows(FULL_EVENT_TAPE_CANARY.len())
        .position(|window| window == FULL_EVENT_TAPE_CANARY.as_bytes())
        .expect("规范事件编码必须真实包含完整事件带 canary");
    let mut alternate_late_event_bytes = late_event_bytes.clone();
    alternate_late_event_bytes
        [canary_offset..canary_offset + ALTERNATE_FULL_EVENT_TAPE_CANARY.len()]
        .copy_from_slice(ALTERNATE_FULL_EVENT_TAPE_CANARY.as_bytes());
    assert_eq!(
        late_event_bytes.len(),
        alternate_late_event_bytes.len(),
        "反例必须只改变同长度规范事件正文"
    );
    let alternate_late_bucket = ClosedMarketEventBucketV1::public(
        late_bucket_reference.bucket_at_utc_ms,
        late_bucket_reference.start_index_position,
        late_bucket_reference.next_index_position,
        late_bucket_reference.closure,
        vec![alternate_late_event_bytes],
    );
    assert_ne!(
        late_bucket_reference.bucket_sha256,
        alternate_late_bucket.durable_reference().bucket_sha256,
        "桶摘要必须对同长度规范事件正文变化敏感"
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(late_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert!(session.platform_market_state(&condition_id).is_none());
    assert_eq!(
        session.account_snapshot_for_tests(),
        account_before_late_event
    );
    assert_eq!(
        session.next_account_transition_seq(),
        account_seq_before_late_event
    );
    assert_eq!(
        session.query_projection_revision(),
        revision_before_late_event
    );

    let seal_bucket = public_bucket(
        &dependencies,
        &session,
        1_000_000,
        MillisecondBucketClosureV1::EndOfInput,
        &[],
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(seal_bucket),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    while matches!(
        session
            .drive_private_delivery_through(1_000_000)
            .expect("交付驱逐后剩余输出"),
        SubmissionDeliveryDispositionV1::Delivered
    ) {}
    let after_eviction_outputs = session.task41_diagnostics_for_tests(&condition_id);
    let mut later_confirmations = after_eviction_outputs
        .confirmed_direct_response_confirmations
        .iter()
        .chain(
            after_eviction_outputs
                .confirmed_lifecycle_feedback_confirmations
                .iter(),
        )
        .filter(|confirmation| confirmation.private_feedback_sequence > durable_through_sequence)
        .cloned()
        .collect::<Vec<_>>();
    later_confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    later_confirmations.dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    for durable_through in later_confirmations {
        durable_through_sequence = durable_through.private_feedback_sequence;
        process
            .advance_downstream_durability(
                &mut session,
                DownstreamDurabilityProofV1 {
                    account_instance_key: control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("补齐驱逐后输出的下游耐久屏障");
    }
    let before_retention_floors = session.task41_diagnostics_for_tests(&condition_id);
    session = match process.retire_session(session) {
        Err(SessionRetirementFailureV1::Rejected {
            error:
                SessionError::RetentionBarrierNotSatisfied(
                    "市场、查询、输出、输入终态或会话静默屏障尚未全部满足",
                ),
            session,
        }) => *session,
        Ok(()) | Err(_) => panic!("保留水位未越过时必须拒绝退休并归还原会话句柄"),
    };
    assert_eq!(
        session.task41_diagnostics_for_tests(&condition_id),
        before_retention_floors,
        "保留水位未越过时退休失败不得改变会话状态"
    );
    assert_eq!(
        session.drive_private_delivery_through(1_000_000),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse),
        "退休失败后旧句柄仍必须可用，而不能提前进入过期态"
    );
    session
        .advance_query_root_retention_floor(session.next_query_root_sequence())
        .expect("推进会话查询根保留水位");
    session
        .advance_order_history_retention_floor(1_000_000)
        .expect("推进会话订单保留水位");
    session
        .advance_trade_history_retention_floor(1_000_000)
        .expect("推进会话成交保留水位");

    let rebate_clock_at = session
        .next_rebate_clock_at_utc_ms_for_tests()
        .expect("带正手续费的终态成交必须保留未来返佣日结时钟");
    session = match process.retire_session(session) {
        Err(SessionRetirementFailureV1::Rejected {
            error:
                SessionError::RetentionBarrierNotSatisfied("返佣日结或付款时钟尚未全部闭合"),
            session,
        }) => *session,
        Ok(()) | Err(_) => panic!("仅剩未来返佣时钟未闭合时必须拒绝退休并归还原会话句柄"),
    };
    assert_eq!(
        session.drive_next_execution_action_through(rebate_clock_at),
        Ok(SubmissionDriveDispositionV1::NoAction),
        "退休前必须通过统一逻辑驱动形成到期返佣日结与付款事实"
    );
    let rebate_delivery_through = rebate_clock_at
        .checked_add(
            i64::try_from(dependencies.latency_profile.statistics().max_delay_ms)
                .expect("代理时延上界适合 i64"),
        )
        .expect("返佣交付截止时点不得溢出");
    assert_eq!(
        session.drive_next_execution_action_through(rebate_delivery_through),
        Ok(SubmissionDriveDispositionV1::NoAction),
        "返佣事实交付前必须通过真实耐久入口闭合平台与私有计划水位"
    );
    assert_eq!(
        session.drive_private_delivery_through(rebate_delivery_through),
        Ok(SubmissionDeliveryDispositionV1::Delivered),
        "到期返佣事实必须在退休前完成真实私有交付"
    );
    assert_eq!(
        session.next_rebate_clock_at_utc_ms_for_tests(),
        None,
        "退休前不得遗留未来返佣日结或付款时钟"
    );
    let rebate_outputs = session.task41_diagnostics_for_tests(&condition_id);
    let mut rebate_confirmations = rebate_outputs
        .confirmed_direct_response_confirmations
        .iter()
        .chain(
            rebate_outputs
                .confirmed_lifecycle_feedback_confirmations
                .iter(),
        )
        .filter(|confirmation| confirmation.private_feedback_sequence > durable_through_sequence)
        .cloned()
        .collect::<Vec<_>>();
    rebate_confirmations.sort_by_key(|confirmation| confirmation.private_feedback_sequence);
    rebate_confirmations.dedup_by_key(|confirmation| confirmation.private_feedback_sequence);
    assert!(
        !rebate_confirmations.is_empty(),
        "返佣时钟推进必须形成退休屏障可见的耐久输出"
    );
    let before_rebate_durability = session.task41_diagnostics_for_tests(&condition_id);
    session = match process.retire_session(session) {
        Err(SessionRetirementFailureV1::Rejected {
            error:
                SessionError::RetentionBarrierNotSatisfied(
                    "市场、查询、输出、输入终态或会话静默屏障尚未全部满足",
                ),
            session,
        }) => *session,
        Ok(()) | Err(_) => panic!("返佣输出尚未跨过强下游水位时必须拒绝退休并归还句柄"),
    };
    assert_eq!(
        session.task41_diagnostics_for_tests(&condition_id),
        before_rebate_durability,
        "强下游耐久屏障拒绝退休时不得改变会话状态"
    );
    for durable_through in rebate_confirmations {
        process
            .advance_downstream_durability(
                &mut session,
                DownstreamDurabilityProofV1 {
                    account_instance_key: control.account_instance_key.clone(),
                    durable_through,
                },
            )
            .expect("补齐返佣日结与付款输出的下游耐久屏障");
    }

    process.fail_next_durable_commit_before_publish_for_tests();
    session = match process.retire_session(session) {
        Err(SessionRetirementFailureV1::Rejected {
            error: SessionError::Durability(_),
            session,
        }) => *session,
        Ok(()) | Err(_) => panic!("退休提交前故障必须归还原会话句柄"),
    };
    assert_eq!(
        session.advance_order_history_retention_floor(rebate_delivery_through),
        Ok(()),
        "明确未提交的退休失败必须归还可继续真实耐久提交的原句柄"
    );
    drop(session);
    drop(process);

    let mut process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复退休提交前故障后的完整会话");
    let checkpoint_fault_bridge_lifetime = Arc::new(());
    let checkpoint_fault_bridge_lifetime_weak = Arc::downgrade(&checkpoint_fault_bridge_lifetime);
    let session = process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge_with_lifetime_marker(
                OrderBridgeAcceptanceV1::Accepted,
                checkpoint_fault_bridge_lifetime,
            ),
        )
        .expect("退休提交前故障后旧会话仍可恢复");
    process.fail_next_durable_commit_after_checkpoint_before_evidence_for_tests();
    assert!(matches!(
        process.retire_session(session),
        Err(SessionRetirementFailureV1::RecoveryRequired {
            error: SessionError::DurabilityRecoveryRequired(_),
        })
    ));
    assert!(
        checkpoint_fault_bridge_lifetime_weak.upgrade().is_none(),
        "检查点已经发布后必须消费结果不确定的会话句柄"
    );
    let mut blocked_spec = spec.clone();
    blocked_spec.session_id = "task423-poisoned-run-session".to_owned();
    blocked_spec.account_id = "task423-poisoned-run-account".to_owned();
    blocked_spec.creation_request_id = "task423-poisoned-run-create".to_owned();
    blocked_spec.response_bridge_binding_id = "task423-poisoned-run-bridge".to_owned();
    assert!(
        matches!(
            process.create_session(
                blocked_spec,
                captures.bridge(OrderBridgeAcceptanceV1::Accepted),
            ),
            Err(SessionError::DurabilityRecoveryRequired(_))
        ),
        "提交结果不确定后同一运行根必须粘滞禁写，不能生成下一代证据"
    );
    drop(process);

    let mut process = dependencies
        .recover(persistence_root.clone())
        .expect("检查点已发布但证据未提交时重启必须恢复旧会话");
    let evidence_fault_bridge_lifetime = Arc::new(());
    let evidence_fault_bridge_lifetime_weak = Arc::downgrade(&evidence_fault_bridge_lifetime);
    let session = process
        .reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge_with_lifetime_marker(
                OrderBridgeAcceptanceV1::Accepted,
                evidence_fault_bridge_lifetime,
            ),
        )
        .expect("检查点超前孤儿不得遮蔽旧会话恢复");
    process.fail_next_durable_commit_after_evidence_commit_for_tests();
    assert!(matches!(
        process.retire_session(session),
        Err(SessionRetirementFailureV1::RecoveryRequired {
            error: SessionError::DurabilityRecoveryRequired(_),
        })
    ));
    assert!(
        evidence_fault_bridge_lifetime_weak.upgrade().is_none(),
        "证据已提交后必须消费会话句柄并等待重启裁决"
    );
    drop(process);

    let mut recovered_process = dependencies
        .recover(persistence_root.clone())
        .expect("恢复只保留资本围栏的运行根");
    assert!(matches!(
        recovered_process.reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SimExecutionPersistenceErrorV1::SessionRetentionExpired(session_id))
            if session_id == spec.session_id
    ));
    let mut same_run_duplicate = spec.clone();
    same_run_duplicate.session_id = "task423-same-run-duplicate".to_owned();
    same_run_duplicate.creation_request_id = "task423-same-run-duplicate-create".to_owned();
    same_run_duplicate.response_bridge_binding_id = "task423-same-run-duplicate-bridge".to_owned();
    let same_run_duplicate_result = recovered_process.create_session(
        same_run_duplicate,
        captures.bridge(OrderBridgeAcceptanceV1::Accepted),
    );
    match same_run_duplicate_result {
        Err(SessionError::AccountInstanceAlreadyReserved) => {}
        Err(error) => panic!("退休恢复后资本围栏返回了错误类别：{error:?}"),
        Ok(_) => panic!("退休恢复后资本围栏不得允许重复本金"),
    }

    let forbidden_canary_root = persistence_root.join("task431-forbidden-canaries");
    std::fs::create_dir(&forbidden_canary_root).expect("创建任务 4.3 禁止内容夹具目录");
    let forbidden_canaries = [
        ("wallet.txt", "TASK431_WALLET_SECRET_CANARY"),
        ("model.bin", "TASK431_MODEL_STATE_CANARY"),
        ("typed_raw.json", "TASK431_TYPED_RAW_CANARY"),
        ("ledger.json", "TASK431_TRADING_LEDGER_CANARY"),
        ("full-event-tape.json", FULL_EVENT_TAPE_CANARY),
    ];
    for (name, canary) in forbidden_canaries {
        std::fs::write(forbidden_canary_root.join(name), canary)
            .expect("写入任务 4.3 禁止内容 canary");
    }

    recovered_process.fail_before_artifact_bundle_publish_once_for_tests();
    let injected_artifact_failure = recovered_process.seal_run_artifacts();
    assert!(
        matches!(
            injected_artifact_failure,
            Err(SimulationRunArtifactErrorV1::Durability(_))
        ),
        "候选发布前故障必须返回明确耐久失败：{injected_artifact_failure:?}"
    );
    let artifact_parent = persistence_root.join("artifacts");
    let published_before_retry = std::fs::read_dir(&artifact_parent)
        .expect("扫描发布失败后的运行产物父目录")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_ok_and(|file_type| file_type.is_dir())
                && !entry.file_name().to_string_lossy().starts_with('.')
        })
        .count();
    assert_eq!(
        published_before_retry, 0,
        "候选目录同步后的故障不得暴露半包正式目录"
    );
    let mut post_seal_spec = paper_spec(&dependencies, "task431-post-seal-session");
    post_seal_spec.account_id = "task431-post-seal-account".to_owned();
    post_seal_spec.creation_request_id = "task431-post-seal-create".to_owned();
    post_seal_spec.response_bridge_binding_id = "task431-post-seal-bridge".to_owned();
    assert!(matches!(
        recovered_process.create_session(
            post_seal_spec,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SessionError::RunArtifactInputsSealed)
    ));

    let artifact_receipt = recovered_process
        .seal_run_artifacts()
        .expect("候选发布故障后必须幂等重试并原子发布完整运行产物");
    assert_eq!(
        recovered_process
            .seal_run_artifacts()
            .expect("同一封口前缀重复生成必须复用完全相同的正式产物"),
        artifact_receipt
    );
    assert_eq!(
        artifact_receipt.artifact_directory,
        artifact_parent.join(&artifact_receipt.manifest_sha256.0)
    );

    let expected_member_names = [
        PLATFORM_RULE_SNAPSHOT_FILE_NAME_V1,
        PLATFORM_SCENARIO_FILE_NAME_V1,
        PROXY_LATENCY_PROFILE_FILE_NAME_V1,
        SIMULATION_RUN_AUDIT_FILE_NAME_V1,
        SIMULATION_RUN_MANIFEST_FILE_NAME_V1,
        TRADE_FINALITY_POLICY_FILE_NAME_V1,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<std::collections::BTreeSet<_>>();
    let actual_member_names = std::fs::read_dir(&artifact_receipt.artifact_directory)
        .expect("扫描正式运行产物目录")
        .map(|entry| {
            let entry = entry.expect("读取正式运行产物成员");
            assert!(
                entry
                    .file_type()
                    .expect("读取正式运行产物成员类型")
                    .is_file(),
                "正式运行产物只允许固定普通文件"
            );
            entry
                .file_name()
                .into_string()
                .expect("正式运行产物文件名必须是 UTF-8")
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual_member_names, expected_member_names);

    let manifest_bytes = std::fs::read(
        artifact_receipt
            .artifact_directory
            .join(SIMULATION_RUN_MANIFEST_FILE_NAME_V1),
    )
    .expect("读取运行清单正文");
    assert_eq!(
        Sha256Hex(format!("{:x}", Sha256::digest(&manifest_bytes))),
        artifact_receipt.manifest_sha256
    );
    let manifest: SimulationRunManifestV1 =
        serde_json::from_slice(&manifest_bytes).expect("严格解码运行清单正文");
    assert_eq!(manifest.run_id, RUN_ID);
    assert_eq!(
        Sha256Hex(format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&manifest.run_intent_binding).expect("编码强类型运行意图正文")
            )
        )),
        manifest.run_intent_content_sha256,
        "运行意图身份必须能从清单内嵌的强类型正文独立重算"
    );
    assert_eq!(
        manifest.evidence_prefix.through_commit_sequence,
        artifact_receipt.through_commit_sequence
    );
    assert_eq!(
        manifest.evidence_prefix.evidence_head_sha256,
        artifact_receipt.evidence_head_sha256
    );
    assert_eq!(manifest.evidence_prefix.run_root_relative_path, "evidence");
    let declared_member_names = manifest
        .artifacts
        .iter()
        .map(|member| member.relative_path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        declared_member_names,
        expected_member_names
            .iter()
            .filter(|name| name.as_str() != SIMULATION_RUN_MANIFEST_FILE_NAME_V1)
            .cloned()
            .collect()
    );
    let mut published_bytes = manifest_bytes.clone();
    for member in &manifest.artifacts {
        let bytes = std::fs::read(
            artifact_receipt
                .artifact_directory
                .join(&member.relative_path),
        )
        .expect("读取清单声明的运行产物成员");
        assert_eq!(
            u64::try_from(bytes.len()).expect("测试成员字节数适合 u64"),
            member.byte_length
        );
        assert_eq!(
            Sha256Hex(format!("{:x}", Sha256::digest(&bytes))),
            member.file_sha256
        );
        published_bytes.extend_from_slice(&bytes);
    }
    let audit_bytes = std::fs::read(
        artifact_receipt
            .artifact_directory
            .join(SIMULATION_RUN_AUDIT_FILE_NAME_V1),
    )
    .expect("读取运行审计正文");
    let audit: SimulationRunAuditV1 =
        serde_json::from_slice(&audit_bytes).expect("严格解码运行审计正文");
    assert_eq!(audit.run_id, RUN_ID);
    assert_eq!(
        audit.through_commit_sequence,
        artifact_receipt.through_commit_sequence
    );
    assert_eq!(audit.capital_issuance_fence_count, 1);
    assert_eq!(audit.sessions.len(), 1);
    let retired_audit = &audit.sessions[0];
    assert_eq!(retired_audit.session_id, spec.session_id);
    assert_eq!(
        retired_audit.account.account_instance_key,
        control.account_instance_key
    );
    assert_eq!(
        retired_audit.account.opening_anchor_id,
        control.opening_anchor.anchor_id
    );
    assert!(matches!(
        retired_audit.lifecycle,
        SimulationSessionLifecycleV1::ActiveCompleted {
            completion_kind: SimulationSessionCompletionKindV1::PaperRetentionExpired,
            ..
        }
    ));
    assert!(retired_audit.account.account_delta_count > 0);
    assert!(!retired_audit.requests.is_empty());
    assert!(!retired_audit.feedback.deliveries.is_empty());
    assert_eq!(
        retired_audit.feedback.process_confirmed_through,
        retired_audit.feedback.downstream_durable_through
    );
    assert_eq!(
        usize::try_from(retired_audit.feedback.process_confirmed_through)
            .expect("反馈水位适合 usize"),
        retired_audit.feedback.deliveries.len()
    );
    for request in retired_audit.requests.values() {
        assert!(!request.logical_actions.is_empty());
        assert_eq!(
            retired_audit
                .feedback
                .deliveries
                .get(&request.direct_delivery_sequence)
                .expect("每个请求必须引用一个已闭合直接响应")
                .kind,
            SimulationPrivateDeliveryKindV1::Direct
        );
    }

    let evidence_directory =
        persistence_root.join(&manifest.evidence_prefix.run_root_relative_path);
    assert_eq!(
        evidence_directory,
        artifact_receipt
            .artifact_directory
            .parent()
            .and_then(Path::parent)
            .expect("正式产物目录必须位于运行根 artifacts 子目录")
            .join("evidence"),
        "证据引用必须按明确的运行根基准解析"
    );
    let mut evidence_entries = std::fs::read_dir(&evidence_directory)
        .expect("独立扫描原始耐久证据目录")
        .map(|entry| entry.expect("读取原始耐久证据目录项"))
        .collect::<Vec<_>>();
    evidence_entries.sort_by_key(std::fs::DirEntry::file_name);
    assert_eq!(
        u64::try_from(evidence_entries.len()).expect("证据数量适合 u64"),
        manifest.evidence_prefix.evidence_count
    );
    let mut evidence_bytes = 0_u64;
    let mut evidence_head = None;
    let mut matching_canary_bucket_references = 0_u64;
    for (index, entry) in evidence_entries.iter().enumerate() {
        let name = entry
            .file_name()
            .into_string()
            .expect("证据文件名必须是 UTF-8");
        let (sequence_text, digest_json) = name.split_once('-').expect("证据文件名含序号分隔符");
        let sequence = sequence_text.parse::<u64>().expect("证据序号必须可解析");
        assert_eq!(
            sequence,
            u64::try_from(index + 1).expect("测试证据序号适合 u64")
        );
        let digest_text = digest_json.strip_suffix(".json").expect("证据文件名后缀");
        let bytes = std::fs::read(entry.path()).expect("独立读取原始耐久证据文件");
        let envelope: EvidenceEnvelopeAuditWireV1 =
            serde_json::from_slice(&bytes).expect("独立严格解码证据提交信封");
        assert_eq!(envelope.schema_version, "sim_local_evidence_envelope_v1");
        assert_eq!(envelope.sequence, sequence);
        assert_eq!(
            envelope.predecessor_evidence_sha256, evidence_head,
            "清单引用的完整证据前缀必须保持逐项前驱链"
        );
        let durable_evidence: DurableEvidenceAuditWireV1 =
            serde_json::from_str(envelope.body.get()).expect("独立严格解码耐久业务证据正文");
        assert_eq!(
            durable_evidence.schema_version,
            "sim_durable_transaction_evidence_v1"
        );
        let _ = (
            &durable_evidence.query_projection_revision,
            &durable_evidence.logical_platform_driven_through_utc_ms,
            &durable_evidence.private_fact_schedule_closed_through_utc_ms,
            &durable_evidence.account_deltas,
            &durable_evidence.logical_action_results,
            &durable_evidence.private_output_drafts,
            &durable_evidence.transient_input_session_id,
            &durable_evidence.transient_input,
            &durable_evidence.resource_usage,
        );
        if let Some(payload) = durable_evidence.fact.get("PLATFORM_BUCKET_APPLIED") {
            let applied: PlatformBucketAppliedAuditWireV1 =
                serde_json::from_value(payload.clone()).expect("独立严格解码平台桶事实");
            if applied.bucket.path == late_bucket_reference.path
                && applied.bucket.bucket_at_utc_ms == late_bucket_reference.bucket_at_utc_ms
                && applied.bucket.start_index_position == late_bucket_reference.start_index_position
                && applied.bucket.next_index_position == late_bucket_reference.next_index_position
            {
                assert_eq!(applied.session_id, spec.session_id);
                assert_eq!(applied.bucket.closure, late_bucket_reference.closure);
                assert_eq!(
                    applied.bucket.bucket_sha256, late_bucket_reference.bucket_sha256,
                    "耐久桶事实必须绑定带 canary 的完整规范事件正文摘要"
                );
                matching_canary_bucket_references += 1;
            }
        }
        published_bytes.extend_from_slice(&bytes);
        evidence_bytes = evidence_bytes
            .checked_add(u64::try_from(bytes.len()).expect("证据字节数适合 u64"))
            .expect("证据总字节数不溢出");
        let digest = Sha256Hex(format!("{:x}", Sha256::digest(&bytes)));
        assert_eq!(digest.0, digest_text);
        evidence_head = Some(digest);
    }
    assert_eq!(
        matching_canary_bucket_references, 1,
        "完整证据前缀必须且只能包含一个精确匹配的 canary 桶引用"
    );
    for (_, canary) in forbidden_canaries {
        assert!(
            !published_bytes
                .windows(canary.len())
                .any(|window| window == canary.as_bytes()),
            "正式六文件与清单引用的证据前缀不得包含禁止内容 canary：{canary}"
        );
    }
    assert_eq!(evidence_bytes, manifest.evidence_prefix.evidence_bytes);
    assert_eq!(
        evidence_head.expect("封口运行必须有证据链头"),
        manifest.evidence_prefix.evidence_head_sha256
    );
    assert_eq!(
        audit.resource_observation.evidence_file_count,
        manifest.evidence_prefix.evidence_count
    );
    assert_eq!(
        audit.resource_observation.evidence_bytes,
        manifest.evidence_prefix.evidence_bytes
    );
    assert!(audit.invariants.evidence_predecessor_chain_verified);
    assert!(audit.invariants.durable_projection_verified);
    assert!(audit.invariants.account_delta_chains_verified);
    assert!(audit.invariants.private_output_closure_verified);
    assert!(
        audit
            .invariants
            .completion_facts_and_durable_closure_verified
    );

    drop(recovered_process);
    let mut sealed_process = dependencies
        .recover(persistence_root.clone())
        .expect("跨进程恢复已经封口并发布产物的运行根");
    assert!(matches!(
        sealed_process.reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SimExecutionPersistenceErrorV1::RunArtifactInputsSealed)
    ));
    assert_eq!(
        sealed_process
            .seal_run_artifacts()
            .expect("跨进程重复封存必须复用同一正式产物"),
        artifact_receipt
    );

    let other_run_id = "task423-other-run";
    let mut other_process = SimExecutionProcessV1::try_new(
        other_run_id.to_owned(),
        Arc::clone(&dependencies.finality_policy),
        Arc::clone(&dependencies.current_rules),
        Arc::clone(&dependencies.latency_profile),
        Arc::clone(&dependencies.scenario),
    )
    .expect("创建不同运行根");
    let mut other_run_spec = spec;
    other_run_spec.run_id = other_run_id.to_owned();
    other_run_spec.session_id = "task423-other-run-session".to_owned();
    other_run_spec.creation_request_id = "task423-other-run-create".to_owned();
    other_run_spec.response_bridge_binding_id = "task423-other-run-bridge".to_owned();
    assert_eq!(
        other_process
            .create_session(
                other_run_spec,
                captures.bridge(OrderBridgeAcceptanceV1::Accepted),
            )
            .expect("同一业务账户在不同 run 不受旧资本围栏影响")
            .control
            .disposition,
        SessionCreationDispositionV2::NewPrepared
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task431_failed_initialization_seals_with_typed_lifecycle_and_rejects_writes() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let directory = tempfile::tempdir().expect("创建任务 4.3 失败初始化持久目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = paper_spec(&dependencies, "task431-failed-initialization");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建任务 4.3 失败初始化运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建任务 4.3 待初始化会话");
    let control = prepared.control;
    let mut session = prepared.session;
    assert!(matches!(
        process.seal_run_artifacts(),
        Err(SimulationRunArtifactErrorV1::RunNotReady(
            "仍有未完成初始化决定的会话"
        ))
    ));
    assert_eq!(
        process
            .fail_session_initialization(failure_request(&spec, &control))
            .expect("封口拒绝后仍可通过真实控制面关闭初始化")
            .snapshot
            .state,
        SessionInitializationStateV1::InitializationFailedClosed
    );

    let receipt = process
        .seal_run_artifacts()
        .expect("失败关闭且无业务状态的运行必须可封存");
    let audit: SimulationRunAuditV1 = serde_json::from_slice(
        &std::fs::read(
            receipt
                .artifact_directory
                .join(SIMULATION_RUN_AUDIT_FILE_NAME_V1),
        )
        .expect("读取失败初始化运行审计"),
    )
    .expect("严格解码失败初始化运行审计");
    assert_eq!(audit.sessions.len(), 1);
    assert_eq!(audit.sessions[0].session_id, spec.session_id);
    assert!(matches!(
        &audit.sessions[0].lifecycle,
        SimulationSessionLifecycleV1::InitializationFailedClosed { failure_id, .. }
            if failure_id == &format!("fail-{}", spec.session_id)
    ));
    assert_eq!(audit.sessions[0].account.account_delta_count, 0);
    assert!(audit.sessions[0].requests.is_empty());
    assert!(audit.sessions[0].feedback.deliveries.is_empty());
    assert_eq!(audit.capital_issuance_fence_count, 1);

    let mut blocked_spec = paper_spec(&dependencies, "task431-blocked-after-seal");
    blocked_spec.account_id = "task431-blocked-after-seal-account".to_owned();
    blocked_spec.creation_request_id = "task431-blocked-after-seal-create".to_owned();
    blocked_spec.response_bridge_binding_id = "task431-blocked-after-seal-bridge".to_owned();
    assert!(matches!(
        process.create_session(
            blocked_spec,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SessionError::RunArtifactInputsSealed)
    ));
    assert!(matches!(
        process.fail_session_initialization(failure_request(&spec, &control)),
        Err(SessionError::RunArtifactInputsSealed)
    ));
    let evidence_before_stale_handle = evidence_file_names(&persistence_root);
    let stale_handle_result = session.drive_private_delivery_through(0);
    assert!(
        matches!(
            stale_handle_result,
            Err(SessionError::RunArtifactInputsSealed)
        ),
        "封口后旧句柄必须被同一共享门拒绝：{stale_handle_result:?}"
    );
    assert_eq!(
        evidence_file_names(&persistence_root),
        evidence_before_stale_handle,
        "封口前已存在的会话句柄不得追加任何耐久事实"
    );
    drop(session);
    drop(process);

    let mut recovered = dependencies
        .recover(persistence_root)
        .expect("恢复失败初始化且已经封口的运行");
    assert!(matches!(
        recovered.reattach_recovered_paper_session(
            &spec.session_id,
            &spec.response_bridge_binding_id,
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        ),
        Err(SimExecutionPersistenceErrorV1::RunArtifactInputsSealed)
    ));
    assert_eq!(
        recovered
            .seal_run_artifacts()
            .expect("恢复后重复封存必须复用同一产物"),
        receipt
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task431_training_and_offline_clean_runs_publish_without_business_resume() {
    let historical_rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let dependencies =
        RecoveryDependenciesV1::with_rules(successful_policy_for_tests(), historical_rules);

    for (role, suffix) in [
        (SimCallerRoleV1::ModelTrain, "model-train"),
        (SimCallerRoleV1::OfflineEvaluation, "offline-evaluation"),
    ] {
        let directory = tempfile::tempdir().expect("创建训练或离线评估耐久目录");
        let persistence_root = directory.path().to_path_buf();
        let captures = DirectCapturesV1::default();
        let mut spec = session_spec(&dependencies.scenario, role, suffix);
        spec.response_outbox_capacity = NonZeroUsize::new(64).expect("正响应容量");
        let mut process = dependencies
            .new_process(persistence_root.clone())
            .expect("创建训练或离线评估耐久运行根");
        let prepared = process
            .create_session(
                spec.clone(),
                captures.bridge(OrderBridgeAcceptanceV1::Accepted),
            )
            .expect("创建训练或离线评估会话");
        let control = prepared.control;
        let mut session = prepared.session;
        process
            .activate_session(activation_request(&spec, &control))
            .expect("激活训练或离线评估会话");

        let initial = initial_events("0.01", "0.6", "1000");
        session
            .apply_closed_market_event_bucket(hidden_bucket(
                &session,
                110_000,
                MillisecondBucketClosureV1::NextEventAt(490_000),
                &initial,
            ))
            .expect("应用隐藏权威初始市场桶");
        session
            .apply_closed_market_event_bucket(public_bucket(
                &dependencies,
                &session,
                120_000,
                MillisecondBucketClosureV1::NextEventAt(MARKET_END_AT_UTC_MS),
                &initial,
            ))
            .expect("应用公开可见初始市场桶");
        session
            .apply_closed_market_event_bucket(hidden_bucket(
                &session,
                MARKET_END_AT_UTC_MS,
                MillisecondBucketClosureV1::NextEventAt(510_000),
                &[],
            ))
            .expect("隐藏权威路径推进到市场结束");
        session
            .apply_closed_market_event_bucket(public_bucket(
                &dependencies,
                &session,
                MARKET_END_AT_UTC_MS,
                MillisecondBucketClosureV1::NextEventAt(510_100),
                &[],
            ))
            .expect("公开可见路径推进到市场结束");
        let resolution = resolution_event(510_000, 510_100);
        session
            .apply_closed_market_event_bucket(hidden_bucket(
                &session,
                510_000,
                MillisecondBucketClosureV1::EndOfInput,
                std::slice::from_ref(&resolution),
            ))
            .expect("隐藏权威路径形成判定并封存");
        session
            .apply_closed_market_event_bucket(public_bucket(
                &dependencies,
                &session,
                510_100,
                MillisecondBucketClosureV1::NextEventAt(520_000),
                std::slice::from_ref(&resolution),
            ))
            .expect("公开可见路径形成判定但暂不封存");
        let condition_id = market().condition_id.0;
        assert_eq!(
            session.drive_next_execution_action_through(519_000),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        while matches!(
            session
                .drive_private_delivery_through(519_000)
                .expect("交付训练或离线评估终态反馈"),
            SubmissionDeliveryDispositionV1::Delivered
        ) {}
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
                .expect("推进训练或离线评估终态反馈的强下游水位");
        }
        process
            .evict_terminal_market(&mut session, &condition_id)
            .expect("训练或离线评估必须成对驱逐隐藏与公开热市场状态");

        session = match process.complete_clean_run_session(session) {
            Err(SessionRetirementFailureV1::Rejected {
                error:
                    SessionError::RetentionBarrierNotSatisfied(
                        "市场、查询、输出、输入终态或会话静默屏障尚未全部满足",
                    ),
                session,
            }) => *session,
            Ok(()) | Err(_) => panic!("只有隐藏路径封存时不得完成 clean run"),
        };
        session
            .apply_closed_market_event_bucket(public_bucket(
                &dependencies,
                &session,
                520_000,
                MillisecondBucketClosureV1::EndOfInput,
                &[],
            ))
            .expect("公开可见路径补齐 EOF 封存");
        assert!(
            process.complete_clean_run_session(session).is_ok(),
            "双输入路径和全部义务闭合后必须完成 clean run"
        );
        drop(process);

        assert!(matches!(
            dependencies.recover(persistence_root.clone()),
            Err(SimExecutionPersistenceErrorV1::Session(message))
                if message.contains("非模拟盘会话")
        ));
        let mut artifact_process = dependencies
            .recover_for_artifact_publication(persistence_root.clone())
            .expect("clean completion 后崩溃必须只能恢复产物发布能力");
        if role == SimCallerRoleV1::ModelTrain {
            artifact_process.fail_before_artifact_bundle_publish_once_for_tests();
            assert!(matches!(
                artifact_process.seal_run_artifacts(),
                Err(SimulationRunArtifactErrorV1::Durability(_))
            ));
            drop(artifact_process);
            artifact_process = dependencies
                .recover_for_artifact_publication(persistence_root.clone())
                .expect("运行已封口但产物未发布时必须可恢复发布");
        }
        let receipt = artifact_process
            .seal_run_artifacts()
            .expect("训练或离线评估运行必须发布同一运行清单与审计正文");
        let audit: SimulationRunAuditV1 = serde_json::from_slice(
            &std::fs::read(
                receipt
                    .artifact_directory
                    .join(SIMULATION_RUN_AUDIT_FILE_NAME_V1),
            )
            .expect("读取训练或离线评估运行审计正文"),
        )
        .expect("严格解码训练或离线评估运行审计正文");
        assert!(matches!(
            audit.sessions.as_slice(),
            [crate::artifacts::SimulationSessionAuditV1 {
                lifecycle:
                    SimulationSessionLifecycleV1::ActiveCompleted {
                        completion_kind: SimulationSessionCompletionKindV1::CleanRunCompleted,
                        ..
                    },
                input_ranges,
                ..
            }] if input_ranges.len() == 2
                && input_ranges
                    .iter()
                    .any(|range| range.path == SimulationInputPathV1::HiddenPlatform)
                && input_ranges
                    .iter()
                    .any(|range| range.path == SimulationInputPathV1::PublicVisibility)
        ));

        let mut blocked_spec = session_spec(
            &dependencies.scenario,
            role,
            &format!("{suffix}-blocked-after-seal"),
        );
        blocked_spec.account_id = format!("{suffix}-blocked-account");
        blocked_spec.creation_request_id = format!("{suffix}-blocked-create");
        blocked_spec.response_bridge_binding_id = format!("{suffix}-blocked-bridge");
        assert!(matches!(
            artifact_process.create_session(
                blocked_spec,
                captures.bridge(OrderBridgeAcceptanceV1::Accepted),
            ),
            Err(SessionError::RunArtifactInputsSealed)
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task431_active_training_checkpoint_cannot_resume_or_publish_artifacts() {
    let historical_rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let dependencies =
        RecoveryDependenciesV1::with_rules(successful_policy_for_tests(), historical_rules);
    let directory = tempfile::tempdir().expect("创建活动训练耐久目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let mut spec = session_spec(
        &dependencies.scenario,
        SimCallerRoleV1::ModelTrain,
        "task431-active-training",
    );
    spec.response_outbox_capacity = NonZeroUsize::new(64).expect("正响应容量");
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建活动训练耐久运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建活动训练会话");
    let control = prepared.control;
    process
        .activate_session(activation_request(&spec, &control))
        .expect("激活训练会话");
    drop(prepared.session);
    drop(process);

    assert!(matches!(
        dependencies.recover(persistence_root.clone()),
        Err(SimExecutionPersistenceErrorV1::Session(message))
            if message.contains("非模拟盘会话")
    ));
    assert!(matches!(
        dependencies.recover_for_artifact_publication(persistence_root),
        Err(SimExecutionPersistenceErrorV1::Session(message))
            if message.contains("仍有尚未退休的活动会话")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task431_nonpaper_failed_initialization_only_publishes_in_the_live_process() {
    let historical_rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let dependencies =
        RecoveryDependenciesV1::with_rules(successful_policy_for_tests(), historical_rules);
    let directory = tempfile::tempdir().expect("创建失败离线评估耐久目录");
    let persistence_root = directory.path().to_path_buf();
    let captures = DirectCapturesV1::default();
    let spec = session_spec(
        &dependencies.scenario,
        SimCallerRoleV1::OfflineEvaluation,
        "task431-failed-offline",
    );
    let mut process = dependencies
        .new_process(persistence_root.clone())
        .expect("创建失败离线评估运行根");
    let prepared = process
        .create_session(
            spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建待失败离线评估会话");
    process
        .fail_session_initialization(failure_request(&spec, &prepared.control))
        .expect("以真实入口关闭离线评估初始化");
    process
        .seal_run_artifacts()
        .expect("仍存活的失败运行必须生成带失败决定的审计产物");
    drop(prepared.session);
    drop(process);

    assert!(matches!(
        dependencies.recover_for_artifact_publication(persistence_root),
        Err(SimExecutionPersistenceErrorV1::Session(message))
            if message.contains("只有 clean run 完成后")
    ));

    let crash_directory = tempfile::tempdir().expect("创建崩溃前未封存的失败离线评估目录");
    let crash_persistence_root = crash_directory.path().to_path_buf();
    let crash_spec = session_spec(
        &dependencies.scenario,
        SimCallerRoleV1::OfflineEvaluation,
        "task431-failed-offline-before-seal",
    );
    let mut crash_process = dependencies
        .new_process(crash_persistence_root.clone())
        .expect("创建崩溃前未封存的失败离线评估运行根");
    let crash_prepared = crash_process
        .create_session(
            crash_spec.clone(),
            captures.bridge(OrderBridgeAcceptanceV1::Accepted),
        )
        .expect("创建崩溃前待失败的离线评估会话");
    crash_process
        .fail_session_initialization(failure_request(&crash_spec, &crash_prepared.control))
        .expect("以真实入口形成尚未发布产物的失败离线评估事实");
    drop(crash_prepared.session);
    drop(crash_process);
    assert!(matches!(
        dependencies.recover_for_artifact_publication(crash_persistence_root),
        Err(SimExecutionPersistenceErrorV1::Session(message))
            if message.contains("只有 clean run 完成后")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task431_run_intent_rejects_secret_text_and_foreign_implementation_identity() {
    let dependencies = RecoveryDependenciesV1::new(successful_policy_for_tests()).await;
    let actual_identity = dependencies
        .current_rules
        .snapshot()
        .provenance
        .implementation_identity
        .clone();
    let forbidden_wire = serde_json::json!({
        "schema_version": crate::artifacts::SIMULATION_RUN_INTENT_BINDING_SCHEMA_V1,
        "implementation_identity": actual_identity.clone(),
        "workload_kind": "COMPONENT_GOLDEN",
        "deterministic_script_contract": "COMPONENT_GOLDEN_V1",
        "workload_identity_sha256": "TASK431_WALLET_SECRET_CANARY",
    });
    assert!(
        serde_json::from_value::<SimulationRunIntentBindingV1>(forbidden_wire).is_err(),
        "运行意图强类型边界不得再接收携带任意正文的调用方自报摘要字段"
    );

    let mut foreign_identity = actual_identity;
    foreign_identity.git_commit = if foreign_identity.git_commit == "f".repeat(40) {
        "e".repeat(40)
    } else {
        "f".repeat(40)
    };
    let foreign_binding = SimulationRunIntentBindingV1::try_new(
        foreign_identity,
        SimulationWorkloadKindV1::ComponentGolden,
        SimulationDeterministicScriptContractV1::ComponentGoldenV1,
    )
    .expect("外来实现身份本身必须是合法强类型身份");
    let directory = tempfile::tempdir().expect("创建外来实现身份耐久目录");
    assert!(matches!(
        SimExecutionProcessV1::try_new_with_local_persistence(
            RUN_ID.to_owned(),
            Arc::clone(&dependencies.finality_policy),
            Arc::clone(&dependencies.current_rules),
            Arc::clone(&dependencies.latency_profile),
            Arc::clone(&dependencies.scenario),
            foreign_binding,
            dependencies.market_event_source.identity_sha256.clone(),
            directory.path().to_path_buf(),
        ),
        Err(SimExecutionPersistenceErrorV1::ProcessRoot(message))
            if message.contains("实现身份")
    ));
}
