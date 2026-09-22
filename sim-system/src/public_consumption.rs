//! 有限公共消费的受信边界；普通请求只能引用执行器已交付的组截止。
use crate::runtime_v2::BusinessRequestV2;
use contracts::*;
use data_pipeline_core::pm_reduction::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ActorTimeRecordV1 {
    pub run_id: String,
    pub drive_position: u64,
    pub logical_time_utc_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct TrustedActorExecutionTimeV1(pub(crate) ActorTimeRecordV1);
impl TrustedActorExecutionTimeV1 {
    pub fn run_id(&self) -> &str {
        &self.0.run_id
    }
    pub fn drive_position(&self) -> u64 {
        self.0.drive_position
    }
    pub fn logical_time_utc_ms(&self) -> i64 {
        self.0.logical_time_utc_ms
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PublicGroupAppliedReceiptV1 {
    pub(crate) session_id: String,
    pub(crate) group: PublicGroupReferenceV1,
    pub(crate) actor_input_sequence: u64,
    pub(crate) execution_time: TrustedActorExecutionTimeV1,
}
impl PublicGroupAppliedReceiptV1 {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn group(&self) -> &PublicGroupReferenceV1 {
        &self.group
    }
    pub fn actor_input_sequence(&self) -> u64 {
        self.actor_input_sequence
    }
    pub fn execution_time(&self) -> &TrustedActorExecutionTimeV1 {
        &self.execution_time
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TrustedGroupDecisionCutoffV1 {
    pub(crate) receipt: PublicGroupAppliedReceiptV1,
    pub(crate) decision_time: TrustedActorExecutionTimeV1,
    pub(crate) private_feedback_sequence: u64,
}
impl TrustedGroupDecisionCutoffV1 {
    pub fn receipt(&self) -> &PublicGroupAppliedReceiptV1 {
        &self.receipt
    }
    pub fn decision_time(&self) -> &TrustedActorExecutionTimeV1 {
        &self.decision_time
    }
    pub fn private_feedback_sequence(&self) -> u64 {
        self.private_feedback_sequence
    }
}
/// SIM 公共时钟关闭依据；有限两种变体保留原序列化形状。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicTimeClosureReasonV1 {
    NextGlobalEventAt(i64),
    EndOfInput,
    ExecutionClockAdvancedTo(i64),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TrustedPublicTimeClosureV1 {
    pub(crate) execution_time: TrustedActorExecutionTimeV1,
    pub(crate) consumption_sequence_ref: String,
    pub(crate) next_group_position: u64,
    pub(crate) closure: PublicTimeClosureReasonV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PublicObservationFrameV1<'a> {
    pub observation: &'a MarketBusinessObservationV1,
    pub rule_context: Arc<PublicTradingRuleContextV1>,
    pub receipt: PublicGroupAppliedReceiptV1,
    pub decision_cutoff: TrustedGroupDecisionCutoffV1,
}

/// 同一次 DPT 应用结果与原来源准备材料；不会在执行热路径重新准备身份。
pub struct PreparedPublicGroupV1 {
    pub reference: PublicGroupReferenceV1,
    pub group: Arc<ClosedBusinessGroupV1>,
    pub pm_inputs: Vec<(u64, PreparedPmMessageV1)>,
    pub pm_contexts: Vec<PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
}
#[cfg(any(test, feature = "public-consumption"))]
impl PreparedPublicGroupV1 {
    /// 同次在线应用材料：共享原组 Arc，移动 PM 原准备对象，不重解析或读盘。
    /// 宿主按返回组原序登记/消费后，再完成 pending 的记录确认。
    pub fn from_online<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        pending: &mut data_pipeline_training::public_online::PublicOnlinePendingV1<'_, S>,
    ) -> Vec<Arc<Self>> {
        let (applied, materials) = pending.materials();
        prepare_public_groups(
            applied,
            materials.first_canonical_position,
            &mut materials.prepared_pm,
            |condition| materials.pm_contexts.get(condition).cloned(),
        )
    }
}
#[cfg(any(test, feature = "public-consumption"))]
fn prepare_public_groups(
    applied: &AppliedIngressV1,
    mut canonical_position: u64,
    prepared_pm: &mut Option<PreparedPmMessageV1>,
    mut context: impl FnMut(
        &str,
    ) -> Option<PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
) -> Vec<Arc<PreparedPublicGroupV1>> {
    applied
        .closed_groups
        .iter()
        .map(|group| {
            let reference = PublicGroupReferenceV1 {
                input_stream_id: applied.applied_prefix.input_stream_id.clone(),
                applied_prefix: applied.applied_prefix.clone(),
                group_position: group.group_position,
                group_id: group.group_id.clone(),
                closed_at_input_position: group.closed_at_input_position,
            };
            let mut pm_inputs = Vec::new();
            let mut contexts = std::collections::BTreeMap::new();
            for fact in group.records.iter() {
                let market = match &fact.payload {
                    PublicBusinessPayloadV1::PolymarketMessage(message) => {
                        if let Some(pm) = prepared_pm.take() {
                            pm_inputs.push((canonical_position, pm));
                        }
                        Some(&message.market)
                    }
                    PublicBusinessPayloadV1::MarketFact { market, .. } => Some(market),
                    _ => None,
                };
                if let Some(market) = market
                    && let Some(value) = context(&market.condition_id.0)
                {
                    contexts.insert(market.condition_id.0.clone(), value);
                }
                canonical_position += 1;
            }
            Arc::new(PreparedPublicGroupV1 {
                reference,
                group: Arc::clone(group),
                pm_inputs,
                pm_contexts: contexts.into_values().collect(),
            })
        })
        .collect()
}

impl std::fmt::Debug for PreparedPublicGroupV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.reference.fmt(f)
    }
}
impl PartialEq for PreparedPublicGroupV1 {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference && self.group == other.group
    }
}
impl Eq for PreparedPublicGroupV1 {}

#[derive(Clone, Debug, Serialize)]
pub struct AcceptedPublicGroupV1 {
    pub(crate) reference: PublicGroupReferenceV1,
    pub(crate) execution_time: TrustedActorExecutionTimeV1,
    #[serde(skip_serializing)]
    pub(crate) prepared: Arc<PreparedPublicGroupV1>,
    #[serde(skip_serializing)]
    pub(crate) pm_materials: Vec<DeferredPublicPmReductionV1>,
    pub(crate) pm_step_bindings: Vec<PmDerivedStepBindingV1>,
    pub(crate) pm_reports: Vec<PmReductionReportV2<SourceEvidenceRefV3>>,
}
impl PartialEq for AcceptedPublicGroupV1 {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.execution_time == other.execution_time
            && self.prepared == other.prepared
            && self.pm_step_bindings == other.pm_step_bindings
            && self.pm_reports == other.pm_reports
    }
}
impl Eq for AcceptedPublicGroupV1 {}
impl AcceptedPublicGroupV1 {
    pub(crate) fn materialize(&mut self) -> Result<(), crate::event_log::SimEventLogErrorV2> {
        for material in std::mem::take(&mut self.pm_materials) {
            let result = materialize_pm_step_identity_v1(material.restore(&self.prepared))
                .map_err(|e| crate::event_log::SimEventLogErrorV2::Encode(e.to_string()))?;
            self.pm_step_bindings.extend(result.bindings);
            for report in result.reduction.reports {
                let operation_evidence = |items: Vec<
                    PmOperationEvidenceV2<PreparedPmEvidenceV1<SourceEvidenceRefV3>>,
                >| {
                    items
                        .into_iter()
                        .map(|item| PmOperationEvidenceV2 {
                            role: item.role,
                            source_evidence: item.source_evidence.source().clone(),
                        })
                        .collect()
                };
                let disposition = match report.disposition {
                    PmReductionDispositionV2::Produced { derived_id } => {
                        PmReductionDispositionV2::Produced { derived_id }
                    }
                    PmReductionDispositionV2::NoOutput {
                        operation_evidence: items,
                    } => PmReductionDispositionV2::NoOutput {
                        operation_evidence: operation_evidence(items),
                    },
                    PmReductionDispositionV2::Suppressed {
                        operation_evidence: items,
                    } => PmReductionDispositionV2::Suppressed {
                        operation_evidence: operation_evidence(items),
                    },
                };
                self.pm_reports.push(PmReductionReportV2 {
                    canonical_fact_id: report.canonical_fact_id,
                    member_index: report.member_index,
                    canonical_rule_version: report.canonical_rule_version,
                    scope: report.scope,
                    operation: report.operation,
                    reason_code: report.reason_code,
                    quantity_conservation: report.quantity_conservation,
                    disposition,
                });
            }
        }
        Ok(())
    }
}
#[derive(Debug)]
pub enum PublicActorInputV1 {
    Group(AcceptedPublicGroupV1),
    Request {
        cutoff: TrustedGroupDecisionCutoffV1,
        request: BusinessRequestV2,
    },
    CloseTime(TrustedPublicTimeClosureV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PublicExecutionIdentityV1 {
    Fact(PublicFactPositionV1),
    Step(PmDerivedStepPositionV1),
    Durable(CanonicalEventId),
}
impl crate::market::MarketEventIdentityV1 for PublicExecutionIdentityV1 {
    fn identity_key(&self) -> String {
        match self {
            Self::Fact(p) => format!(
                "public-fact:{}:{}:{}:{}",
                p.input_stream_id.len(),
                p.input_stream_id,
                p.closed_at_input_position,
                p.fact_ordinal
            ),
            Self::Step(p) => format!(
                "pm-step:{}:{}:{}:{:?}:{}",
                p.consumer_stream_id.len(),
                p.consumer_stream_id,
                p.canonical_record_position,
                p.member_index,
                p.step_index
            ),
            Self::Durable(id) => id.0.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedPublicGroupV1 {
    pub reference: PublicGroupReferenceV1,
    pub execution_time: ActorTimeRecordV1,
    pub pm_step_bindings: Vec<PmDerivedStepBindingV1>,
    pub pm_reports: Vec<PmReductionReportV2<SourceEvidenceRefV3>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedPublicReceiptV1 {
    pub session_id: String,
    pub group: PublicGroupReferenceV1,
    pub actor_input_sequence: u64,
    pub execution_time: ActorTimeRecordV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedGroupCutoffV1 {
    pub receipt: LoggedPublicReceiptV1,
    pub decision_time: ActorTimeRecordV1,
    pub private_feedback_sequence: u64,
}
impl LoggedGroupCutoffV1 {
    pub(crate) fn into_trusted(self) -> TrustedGroupDecisionCutoffV1 {
        TrustedGroupDecisionCutoffV1 {
            receipt: PublicGroupAppliedReceiptV1 {
                session_id: self.receipt.session_id,
                group: self.receipt.group,
                actor_input_sequence: self.receipt.actor_input_sequence,
                execution_time: TrustedActorExecutionTimeV1(self.receipt.execution_time),
            },
            decision_time: TrustedActorExecutionTimeV1(self.decision_time),
            private_feedback_sequence: self.private_feedback_sequence,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedPublicClosureV1 {
    pub execution_time: ActorTimeRecordV1,
    pub consumption_sequence_ref: String,
    pub next_group_position: u64,
    pub closure: PublicTimeClosureReasonV1,
}
impl LoggedPublicClosureV1 {
    pub(crate) fn into_trusted(self) -> TrustedPublicTimeClosureV1 {
        TrustedPublicTimeClosureV1 {
            execution_time: TrustedActorExecutionTimeV1(self.execution_time),
            consumption_sequence_ref: self.consumption_sequence_ref,
            next_group_position: self.next_group_position,
            closure: self.closure,
        }
    }
}

pub(crate) struct SimPmReadV1<'a> {
    pub evidence: &'a data_pipeline_core::public_observation_core::PmOwnedStateV1<
        PreparedPmEvidenceV1<SourceEvidenceRefV3>,
    >,
    pub markets: &'a crate::market::MarketStateStoreV2,
}
impl PmStateReadV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>> for SimPmReadV1<'_> {
    fn market(&self) -> &MarketIdentityV2 {
        self.evidence.market()
    }
    fn current_epoch(&self) -> Option<&str> {
        self.evidence.current_epoch()
    }
    fn asset_state(
        &self,
        token: &str,
    ) -> Option<PmAssetReadV1<'_, PreparedPmEvidenceV1<SourceEvidenceRefV3>>> {
        self.evidence.asset_state(token)
    }
    fn quantity(
        &self,
        token: &str,
        side: BookSide,
        price: rust_decimal::Decimal,
    ) -> rust_decimal::Decimal {
        self.markets
            .pm_quantity(&self.market().condition_id.0, token, side, price)
    }
    fn level_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: rust_decimal::Decimal,
    ) -> Option<PmEvidenceReadV1<'_, PreparedPmEvidenceV1<SourceEvidenceRefV3>>> {
        self.evidence.level_evidence(token, side, price)
    }
    fn absence_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: rust_decimal::Decimal,
    ) -> Option<PmEvidenceReadV1<'_, PreparedPmEvidenceV1<SourceEvidenceRefV3>>> {
        self.evidence.absence_evidence(token, side, price)
    }
    fn selected_level(
        &self,
        token: &str,
        side: BookSide,
        price: rust_decimal::Decimal,
    ) -> Option<&PmSelectedLevelV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>> {
        self.evidence.selected_level(token, side, price)
    }
    fn level_keys<'a>(
        &'a self,
        token: &'a str,
        side: BookSide,
    ) -> Box<dyn Iterator<Item = rust_decimal::Decimal> + 'a> {
        self.evidence.level_keys(token, side)
    }
}

pub trait PublicConsumptionMaterialReaderV1 {
    fn public_group(
        &mut self,
        reference: &PublicGroupReferenceV1,
    ) -> Result<Arc<PreparedPublicGroupV1>, crate::run_v2::SimRunActorErrorV2>;
    fn hidden_bucket(
        &mut self,
        reference: &ClosedHiddenStepBucketV1,
    ) -> Result<Arc<[HiddenExecutionRecordV1]>, crate::run_v2::SimRunActorErrorV2>;
}

/// 新运行的投影持有方同时拥有真实反馈交付前缀；整批接纳不等于模型交付。
pub trait PublicProjectionPortV1: crate::run_v2::BusinessProjectionPortV2 {
    fn delivered_private_feedback(&self, run_id: &str) -> u64;
}
pub(crate) struct PublicProjectionAdapterV1(pub Box<dyn PublicProjectionPortV1>);
impl crate::run_v2::BusinessProjectionPortV2 for PublicProjectionAdapterV1 {
    fn uses_background_normalization(&self) -> bool {
        self.0.uses_background_normalization()
    }
    fn accept_for_background(
        &mut self,
        batch: Arc<crate::run_v2::BusinessProjectionBatchPayloadV1>,
    ) -> Result<
        crate::run_v2::noe_projection::AcceptedNoeProjectionV1,
        crate::run_v2::noe_projection::MemoryNoeErrorV1,
    > {
        self.0.accept_for_background(batch)
    }
    fn close_receive_prefix(&mut self, at: i64) {
        self.0.close_receive_prefix(at);
    }
    fn observe_public_time(&mut self, time: &TrustedActorExecutionTimeV1) {
        self.0.observe_public_time(time);
    }

    fn try_accept(&mut self, batch: Arc<crate::run_v2::BusinessProjectionBatchV2>) -> bool {
        self.0.try_accept(batch)
    }
    fn try_accept_payload(
        &mut self,
        batch: Arc<crate::run_v2::BusinessProjectionBatchPayloadV1>,
    ) -> bool {
        self.0.try_accept_payload(batch)
    }
    fn durable_through(&self, run_id: &str) -> u64 {
        self.0.durable_through(run_id)
    }
    fn actual_delivered_private_feedback(&self, run_id: &str) -> Option<u64> {
        Some(self.0.delivered_private_feedback(run_id))
    }
}

/// 新消费运行固定采用异步记录；完整完成才正常封口，不提供事故账户恢复入口。
pub struct PublicRunOpenedV1 {
    pub projection_version: crate::runtime_v2::BusinessProjectionVersionV1,
    pub run_id: String,
    pub platform_rule_snapshot_file_sha256: Sha256Hex,
    pub trade_finality_policy_file_sha256: Sha256Hex,
    pub platform_scenario_file_sha256: Sha256Hex,
    pub proxy_latency_profile_file_sha256: Sha256Hex,
    pub implementation_identity: crate::platform_rules::ImplementationIdentityV1,
    /// 离线来源的真实摘要；在线来源不预先承诺未来输入。
    pub market_event_source_identity_sha256: Option<Sha256Hex>,
}
impl PublicRunOpenedV1 {
    pub(crate) fn into_record(self) -> crate::runtime_v2::RunOpenedV2 {
        crate::runtime_v2::RunOpenedV2 {
            projection_version: self.projection_version,
            run_id: self.run_id,
            durability_role: crate::runtime_v2::RunDurabilityRoleV2::CleanRun,
            platform_rule_snapshot_file_sha256: self.platform_rule_snapshot_file_sha256,
            trade_finality_policy_file_sha256: self.trade_finality_policy_file_sha256,
            platform_scenario_file_sha256: self.platform_scenario_file_sha256,
            proxy_latency_profile_file_sha256: self.proxy_latency_profile_file_sha256,
            implementation_identity: self.implementation_identity,
            market_event_source_identity_sha256: self.market_event_source_identity_sha256,
        }
    }
}

/// 有限来源提供的真实消费时序项；此时间由运行宿主或既有消费索引提供。
/// 迭代结束表示正常有限输入结束，读取失败必须返回 Err。
pub struct PublicGroupDeliveryV1 {
    pub prepared: Arc<PreparedPublicGroupV1>,
    pub delivered_at_utc_ms: i64,
}

/// 运行宿主持有真实消费时序，按原组引用读取已经记录的执行时间。
/// 普通请求接口不能用此能力构造截止；受信驱动器仅在实际完整应用后签发。
pub trait PublicConsumptionTimeSourceV1 {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, crate::run_v2::SimRunActorErrorV2>;
}

#[cfg(any(test, feature = "public-consumption"))]
pub struct RecordedPublicGroupSourceV1<T> {
    replay: data_pipeline_training::public_replay::PublicReplayV1,
    times: T,
    pending: std::collections::VecDeque<PublicGroupDeliveryV1>,
    normal_end: bool,
}
#[cfg(any(test, feature = "public-consumption"))]
impl<T: PublicConsumptionTimeSourceV1> RecordedPublicGroupSourceV1<T> {
    pub fn open(
        root: &std::path::Path,
        page_rows: usize,
        page_bytes: usize,
        times: T,
    ) -> data_pipeline_training::public_recording::RecordingResult<Self> {
        Ok(Self {
            replay: data_pipeline_training::public_replay::PublicReplayV1::open(
                root, page_rows, page_bytes,
            )?,
            times,
            pending: std::collections::VecDeque::new(),
            normal_end: false,
        })
    }
}
#[cfg(any(test, feature = "public-consumption"))]
impl<T: PublicConsumptionTimeSourceV1> Iterator for RecordedPublicGroupSourceV1<T> {
    type Item = Result<PublicGroupDeliveryV1, crate::run_v2::SimRunActorErrorV2>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Some(Ok(item));
            }
            let first_canonical_position = self.replay.core.next_canonical_position();
            let (input, applied, mut prepared_pm) = match self.replay.next_prepared_applied() {
                Ok(Some(result)) => result,
                Ok(None) if self.normal_end => return None,
                Ok(None) => {
                    return Some(Err(
                        crate::run_v2::SimRunActorErrorV2::PublicConsumptionSource(
                            "稳定前缀尚无正常 EndOfInput".into(),
                        ),
                    ));
                }
                Err(error) => {
                    return Some(Err(
                        crate::run_v2::SimRunActorErrorV2::PublicConsumptionSource(
                            error.to_string(),
                        ),
                    ));
                }
            };
            self.normal_end = matches!(
                input.ingress,
                data_pipeline_core::public_input::DptIngressV1::EndOfInput
            );
            for prepared in prepare_public_groups(
                &applied.applied,
                first_canonical_position,
                &mut prepared_pm,
                |condition| self.replay.core.prepared_pm_context(condition),
            ) {
                let at = match self.times.execution_time_for_group(&prepared.reference) {
                    Ok(at) => at,
                    Err(error) => return Some(Err(error)),
                };
                self.pending.push_back(PublicGroupDeliveryV1 {
                    delivered_at_utc_ms: at,
                    prepared,
                });
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HiddenExecutionRecordV1 {
    Step {
        market: MarketIdentityV2,
        step: PmReductionStepV1<'static, SourceEvidenceRefV3>,
    },
    MarketFact(CanonicalMarketFactV1),
    Reference(CanonicalReferenceEventV2),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AcceptedHiddenStepBucketV1 {
    pub(crate) reference: ClosedHiddenStepBucketV1,
    #[serde(skip_serializing)]
    pub(crate) records: Arc<[HiddenExecutionRecordV1]>,
}
impl AcceptedHiddenStepBucketV1 {
    pub fn reference(&self) -> &ClosedHiddenStepBucketV1 {
        &self.reference
    }
    pub fn records(&self) -> &[HiddenExecutionRecordV1] {
        &self.records
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedHiddenStepBucketV1 {
    pub reference: ClosedHiddenStepBucketV1,
}

#[cfg(any(test, feature = "public-consumption"))]
pub struct RecordedHiddenBucketSourceV1 {
    buckets: data_pipeline_training::hidden_preparation::HiddenBucketReaderV1,
    materials: data_pipeline_training::hidden_preparation::HiddenMaterialReaderV1,
    page: std::collections::VecDeque<(
        u64,
        data_pipeline_training::hidden_preparation::ResolvedHiddenRecordV1,
    )>,
}
#[cfg(any(test, feature = "public-consumption"))]
impl RecordedHiddenBucketSourceV1 {
    pub fn open(
        prepared: &data_pipeline_training::hidden_preparation::HiddenPreparationV1,
        root: &std::path::Path,
        page_rows: usize,
        page_bytes: usize,
    ) -> data_pipeline_training::public_recording::RecordingResult<Self> {
        Ok(Self {
            buckets: data_pipeline_training::hidden_preparation::HiddenBucketReaderV1::open(
                prepared, page_rows, page_bytes,
            )?,
            materials: data_pipeline_training::hidden_preparation::HiddenMaterialReaderV1::open(
                prepared, root, page_rows, page_bytes,
            )?,
            page: std::collections::VecDeque::new(),
        })
    }
    pub fn read_metrics(
        &self,
    ) -> &data_pipeline_training::hidden_preparation::HiddenMaterialReadMetricsV1 {
        self.materials.metrics()
    }
    pub fn next_bucket(
        &mut self,
    ) -> data_pipeline_training::public_recording::RecordingResult<Option<AcceptedHiddenStepBucketV1>>
    {
        use data_pipeline_training::hidden_preparation::ResolvedHiddenRecordV1;
        let Some(reference) = self.buckets.next_bucket()? else {
            return Ok(None);
        };
        let mut records = Vec::new();
        for _ in reference.index_start..reference.index_end {
            if self.page.is_empty() {
                self.page = self.materials.next_page()?.into();
            }
            let (_, record) = self.page.pop_front().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "隐藏完整桶引用的索引页缺失",
                )
            })?;
            records.push(match record {
                ResolvedHiddenRecordV1::Step { stored, market } => HiddenExecutionRecordV1::Step {
                    market,
                    step: stored.step,
                },
                ResolvedHiddenRecordV1::Reference {
                    record: CanonicalEventRecordV2::MarketFact(fact),
                    ..
                } => HiddenExecutionRecordV1::MarketFact(fact),
                ResolvedHiddenRecordV1::Reference {
                    record: CanonicalEventRecordV2::ReferenceEvent(event),
                    ..
                } => HiddenExecutionRecordV1::Reference(event),
                ResolvedHiddenRecordV1::Reference {
                    record: CanonicalEventRecordV2::PolymarketMessage(_),
                    ..
                } => unreachable!("PM 正文使用派生步骤索引"),
            });
        }
        Ok(Some(AcceptedHiddenStepBucketV1 {
            reference,
            records: records.into(),
        }))
    }
}

/// 同次步骤的前序与规则材料；完整快照数组继续由原准备对象持有。
#[derive(Clone, Debug)]
pub(crate) struct DeferredPublicPmReductionV1 {
    captured: data_pipeline_core::public_observation_core::PublicPmReductionV1<'static>,
}
impl DeferredPublicPmReductionV1 {
    pub(crate) fn capture(
        mut reduction: data_pipeline_core::public_observation_core::PublicPmReductionV1<'_>,
    ) -> Self {
        for step in &mut reduction.steps {
            if let PmStepChangeV1::InitialSnapshot { bids, asks, .. } = &mut step.change {
                *bids = std::borrow::Cow::Borrowed(&[]);
                *asks = std::borrow::Cow::Borrowed(&[]);
            }
        }
        Self {
            captured: reduction.into_owned(),
        }
    }
    fn restore<'a>(
        self,
        prepared: &'a PreparedPublicGroupV1,
    ) -> data_pipeline_core::public_observation_core::PublicPmReductionV1<'a> {
        let mut reduction: data_pipeline_core::public_observation_core::PublicPmReductionV1<'a> =
            self.captured;
        for step in &mut reduction.steps {
            if let PmStepChangeV1::InitialSnapshot { bids, asks, .. } = &mut step.change {
                let source = prepared
                    .pm_inputs
                    .iter()
                    .find(|(_, source)| source.message().event_id == step.canonical_fact_id)
                    .expect("同次快照的规范来源由已接纳组持有");
                let CanonicalPmPayloadV1::Book {
                    fact:
                        CanonicalBookFactV1::FullBook {
                            bids: source_bids,
                            asks: source_asks,
                            ..
                        },
                } = &source.1.message().payload
                else {
                    unreachable!("初始快照只来自同次完整 Book")
                };
                *bids = std::borrow::Cow::Borrowed(source_bids);
                *asks = std::borrow::Cow::Borrowed(source_asks);
            }
        }
        reduction
    }
}

/// 新消费当前所属资源；异步日志字节和稳定序号由后台独占，未在此读取。
#[cfg(feature = "measurement")]
#[derive(Clone, Copy, Debug, Serialize)]
pub struct PublicConsumptionResourceSnapshotV1 {
    /// 独立于 business 字节的证据对象计数；未测堆/字符串容量、临时 pm_materials 和后台共享材料。
    pub public_pm_evidence: data_pipeline_core::public_observation_core::PmEvidenceObjectCountsV1,
    pub business: crate::resource::ResourceQuantityV1,
    pub pending_projection: crate::resource::ResourceQuantityV1,
    pub applied_input_sequence: u64,
}
