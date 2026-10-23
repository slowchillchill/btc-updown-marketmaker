//! 真实新运行的前台归一化与后台物化接缝。
use super::*;
use contracts::execution_normalization::{
    SourceNeutralControlResultV2, SourceNeutralTradeLifecycleResultV2,
};
use execution_normalization::{
    MemoryEconomicFactReferenceV1 as FactReference, MemoryLogicalIdentityV1 as Identity,
    NormalizationBatch, NormalizationCore, NormalizationRecord, StagedNormalizationWriter,
};
use execution_normalization::{
    NoeBatchDurabilityV1, NoeConsumedRecordRef, NoeConsumedRecordRefV1, NoeConsumptionSource,
    NoeConsumptionVisibilityV1,
};
use std::collections::{BTreeMap, VecDeque};

type Trade = SourceNeutralTradeLifecycleResultV2<Identity>;
type Control<P> = ControlFor<P, Identity>;
pub type MemoryNoeBatch<P> = NormalizationBatch<Trade, Control<P>, Identity, FactReference>;
pub type MemoryNoeRecord<P> = NormalizationRecord<Trade, Control<P>, Identity, FactReference>;
type Core<P> = NormalizationCore<Trade, Control<P>, Identity, FactReference>;
type Writer<P> = StagedNormalizationWriter<SourceNeutralTradeLifecycleResultV2, ControlFor<P>>;

mod sealed {
    pub trait OrderEvidence {}
    impl OrderEvidence for super::LegacyOrderEvidence {}
    impl OrderEvidence for super::MeasuredOrderEvidence {}
    impl OrderEvidence for super::MeasuredCancellationEvidence {}
}
// 仅连接已有两种订单证据表示；每个端口的类型在创建时固定，同一运行不切换版本。
pub trait MemoryOrderEvidence:
    sealed::OrderEvidence
    + OrderEvidenceProjection<Identity>
    + OrderEvidenceProjection
    + Clone
    + std::fmt::Debug
    + Default
{
    fn accept_consumption(
        source: &mut NoeConsumptionSource<Self::Completion>,
        context: Arc<ExecutionNormalizationContextV1>,
        batch: Arc<MemoryNoeBatch<Self>>,
    ) -> Option<NoeBatchDurabilityV1>;
    fn consumed_record(record: &NoeConsumedRecordRef<Self::Completion>) -> &MemoryNoeRecord<Self>;
    fn contract_version() -> &'static str;
    fn into_background(accepted: AcceptedMemoryProjection<Self>) -> AcceptedNoeProjectionV1;
}
impl MemoryOrderEvidence for LegacyOrderEvidence {
    fn accept_consumption(
        _: &mut NoeConsumptionSource<Self::Completion>,
        _: Arc<ExecutionNormalizationContextV1>,
        _: Arc<MemoryNoeBatch<Self>>,
    ) -> Option<NoeBatchDurabilityV1> {
        None
    }
    fn consumed_record(_: &NoeConsumedRecordRef<Self::Completion>) -> &MemoryNoeRecord<Self> {
        unreachable!("旧端口不产生V3领取引用")
    }

    fn contract_version() -> &'static str {
        <SourceNeutralTradeLifecycleResultV2 as NoeTradePayload>::CONTRACT_VERSION
    }
    fn into_background(accepted: AcceptedMemoryProjection<Self>) -> AcceptedNoeProjectionV1 {
        AcceptedNoeProjectionV1::Legacy(accepted)
    }
}
impl MemoryOrderEvidence for MeasuredOrderEvidence {
    fn accept_consumption(
        source: &mut NoeConsumptionSource<Self::Completion>,
        context: Arc<ExecutionNormalizationContextV1>,
        batch: Arc<MemoryNoeBatch<Self>>,
    ) -> Option<NoeBatchDurabilityV1> {
        Some(source.accept(context, batch))
    }
    fn consumed_record(record: &NoeConsumedRecordRef<Self::Completion>) -> &MemoryNoeRecord<Self> {
        record.record()
    }

    fn contract_version() -> &'static str {
        <SourceNeutralTradeLifecycleResultV2 as NoeTradePayload<SourceNeutralControlResultV2>>::CONTRACT_VERSION
    }
    fn into_background(accepted: AcceptedMemoryProjection<Self>) -> AcceptedNoeProjectionV1 {
        AcceptedNoeProjectionV1::Measured(accepted)
    }
}
impl MemoryOrderEvidence for MeasuredCancellationEvidence {
    fn accept_consumption(
        source: &mut NoeConsumptionSource<Self::Completion>,
        context: Arc<ExecutionNormalizationContextV1>,
        batch: Arc<MemoryNoeBatch<Self>>,
    ) -> Option<NoeBatchDurabilityV1> {
        Some(source.accept(context, batch))
    }
    fn consumed_record(record: &NoeConsumedRecordRef<Self::Completion>) -> &MemoryNoeRecord<Self> {
        record.record()
    }

    fn contract_version() -> &'static str {
        <SourceNeutralTradeLifecycleResultV2 as NoeTradePayload<
            contracts::execution_normalization::SourceNeutralControlResultV3,
        >>::CONTRACT_VERSION
    }
    fn into_background(accepted: AcceptedMemoryProjection<Self>) -> AcceptedNoeProjectionV1 {
        AcceptedNoeProjectionV1::Cancellation(accepted)
    }
}
pub type MemoryNoeProjectionPortV2 = MemoryNoeProjectionPort<LegacyOrderEvidence>;
pub type MemoryNoeProjectionPortV3 = MemoryNoeProjectionPort<MeasuredOrderEvidence>;
pub type MemoryNoeObservationV2 = MemoryNoeObservation<LegacyOrderEvidence>;
pub type MemoryNoeObservationV3 = MemoryNoeObservation<MeasuredOrderEvidence>;
pub type MemoryNoeProjectionPortV4 = MemoryNoeProjectionPort<MeasuredCancellationEvidence>;
pub type MemoryNoeObservationV4 = MemoryNoeObservation<MeasuredCancellationEvidence>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryNoeErrorV1 {
    InvalidProjection(&'static str),
    Normalization(execution_normalization::CoreErrorV1),
    Durable(DurableErrorV1),
}
impl std::fmt::Display for MemoryNoeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProjection(message) => write!(f, "NOE 投影不符：{message}"),
            Self::Normalization(error) => error.fmt(f),
            Self::Durable(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for MemoryNoeErrorV1 {}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "MemoryNoeBatch<P>: serde::Serialize, P::Completion: serde::Serialize",
    deserialize = "MemoryNoeBatch<P>: serde::Deserialize<'de>, P::Completion: serde::Deserialize<'de>"
))]
struct ObservationState<P: MemoryOrderEvidence> {
    accepted: u64,
    durable: u64,
    delivered: u64,
    closed_receive_time: Option<i64>,
    public_visible_time: Option<i64>,
    feedback: VecDeque<PendingFeedback<P>>,
    #[serde(default)]
    durable_directory: Option<PathBuf>,
    consumption: NoeConsumptionSource<P::Completion>,
}
impl<P: MemoryOrderEvidence> Default for ObservationState<P> {
    fn default() -> Self {
        Self {
            accepted: 0,
            durable: 0,
            delivered: 0,
            closed_receive_time: None,
            public_visible_time: None,
            feedback: VecDeque::new(),
            durable_directory: None,
            consumption: NoeConsumptionSource::default(),
        }
    }
}
// 消费者仅保留同一不可变批的 Arc 与未交付记录坐标，不复制归一化正文。
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "MemoryNoeBatch<P>: serde::Serialize",
    deserialize = "MemoryNoeBatch<P>: serde::Deserialize<'de>"
))]
struct PendingFeedback<P: MemoryOrderEvidence> {
    batch: Arc<MemoryNoeBatch<P>>,
    ordinals: Vec<usize>,
}
#[derive(Clone, Debug)]
enum FeedbackRecord<P: MemoryOrderEvidence> {
    Legacy {
        batch: Arc<MemoryNoeBatch<P>>,
        ordinal: usize,
    },
    Measured(NoeConsumedRecordRef<P::Completion>),
}
#[derive(Clone, Debug)]
pub struct MemoryNoeFeedback<P: MemoryOrderEvidence> {
    record: FeedbackRecord<P>,
    delivery_sequence: u64,
}
impl<P: MemoryOrderEvidence> MemoryNoeFeedback<P> {
    pub fn delivery_sequence(&self) -> u64 {
        self.delivery_sequence
    }
    pub fn record(&self) -> &MemoryNoeRecord<P> {
        match &self.record {
            FeedbackRecord::Legacy { batch, ordinal } => &batch.records[*ordinal],
            FeedbackRecord::Measured(record) => P::consumed_record(record),
        }
    }
}
impl MemoryNoeFeedback<MeasuredOrderEvidence> {
    pub fn consumed_record(&self) -> &NoeConsumedRecordRefV1 {
        let FeedbackRecord::Measured(record) = &self.record else {
            unreachable!("V3真实领取")
        };
        record
    }
}
#[derive(Clone, Default)]
pub struct MemoryNoeObservation<P: MemoryOrderEvidence>(Arc<Mutex<ObservationState<P>>>);
impl<P: MemoryOrderEvidence> MemoryNoeObservation<P> {
    pub fn accepted_through(&self) -> u64 {
        self.0.lock().unwrap().accepted
    }
    pub fn durable_through(&self) -> u64 {
        self.0.lock().unwrap().durable
    }
    pub fn delivered_through(&self) -> u64 {
        self.0.lock().unwrap().delivered
    }
    pub fn result_references(&self) -> Result<Vec<NoeResultReferenceV1>, DurableErrorV1>
    where
        SourceNeutralTradeLifecycleResultV2: NoeTradePayload<ControlFor<P>>,
        ControlFor<P>: serde::de::DeserializeOwned,
    {
        let state = self.0.lock().unwrap();
        let Some(directory) = &state.durable_directory else {
            return Ok(Vec::new());
        };
        let directory = directory.clone();
        let through = state.durable;
        drop(state);
        Writer::<P>::published_result_references(&directory, through)
    }
    /// 唯一实际反馈消费者只领取已闭合且不晚于成功公开组受信时点的反馈。
    /// 按接收时间及原稳定发射序分配交付序；平台预取/闭合不能推进公开可见时点。
    /// 同一源反馈的多个 NOE 记录共享交付序；原来源字段不改号。返回值不能作为耐久引用。
    pub fn take_closed_feedback(&self) -> Vec<MemoryNoeFeedback<P>> {
        let mut state = self.0.lock().unwrap();
        let (Some(closed), Some(visible)) = (state.closed_receive_time, state.public_visible_time)
        else {
            return Vec::new();
        };
        if state.consumption.opening_record().is_some() {
            let records = state.consumption.take_closed(closed, visible);
            state.delivered = state.consumption.delivered_through();
            return records
                .into_iter()
                .map(|record| {
                    let NoeConsumptionVisibilityV1::Feedback {
                        delivery_sequence, ..
                    } = *record.visibility()
                    else {
                        unreachable!("反馈领取不含开户")
                    };
                    MemoryNoeFeedback {
                        record: FeedbackRecord::Measured(record),
                        delivery_sequence,
                    }
                })
                .collect();
        }
        let mut ready = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(pending) = state.feedback.pop_front() {
            let batch = pending.batch;
            let mut retained = Vec::new();
            for ordinal in pending.ordinals {
                let record = &batch.records[ordinal];
                if let Some((received, sequence)) = feedback_coordinate::<P>(record) {
                    if received <= closed && received <= visible {
                        ready.push((
                            received,
                            sequence,
                            record.source_order().ordinal,
                            MemoryNoeFeedback {
                                record: FeedbackRecord::Legacy {
                                    batch: Arc::clone(&batch),
                                    ordinal,
                                },
                                delivery_sequence: 0,
                            },
                        ));
                    } else {
                        retained.push(ordinal);
                    }
                }
            }
            if !retained.is_empty() {
                remaining.push_back(PendingFeedback {
                    batch,
                    ordinals: retained,
                });
            }
        }
        state.feedback = remaining;
        ready.sort_by_key(|(received, sequence, ordinal, _)| (*received, *sequence, *ordinal));
        let mut event = None;
        ready
            .into_iter()
            .map(|(received, source_sequence, _, mut feedback)| {
                if event != Some((received, source_sequence)) {
                    state.delivered = state
                        .delivered
                        .checked_add(1)
                        .expect("私有反馈交付序不得溢出");
                    event = Some((received, source_sequence));
                }
                feedback.delivery_sequence = state.delivered;
                feedback
            })
            .collect()
    }
}
impl<P: MemoryOrderEvidence> MemoryNoeObservation<P> {
    pub fn opening_record(&self) -> Option<NoeConsumedRecordRef<P::Completion>> {
        self.0.lock().unwrap().consumption.opening_record()
    }
    pub fn take_closed_records(&self) -> Vec<NoeConsumedRecordRef<P::Completion>> {
        let mut admitted = self.0.lock().unwrap().consumption.take_admitted_records();
        admitted.extend(self.take_closed_feedback().into_iter().map(|feedback| {
            let FeedbackRecord::Measured(record) = feedback.record else {
                unreachable!("V3真实领取")
            };
            record
        }));
        admitted
    }
}
fn feedback_coordinate<P: MemoryOrderEvidence>(record: &MemoryNoeRecord<P>) -> Option<(i64, u64)> {
    match record.source_reference().record {
        SimV2SourceRecordReferenceV1::DirectResponse {
            private_feedback_sequence,
            response_received_at_utc_ms,
            ..
        } => Some((response_received_at_utc_ms, private_feedback_sequence)),
        SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence,
            feedback_received_at_utc_ms,
            ..
        }
        | SimV2SourceRecordReferenceV1::TradeLifecycle {
            private_feedback_sequence,
            feedback_received_at_utc_ms,
            ..
        }
        | SimV2SourceRecordReferenceV1::SettlementLifecycle {
            private_feedback_sequence,
            feedback_received_at_utc_ms,
            ..
        } => Some((feedback_received_at_utc_ms, private_feedback_sequence)),
        _ => None,
    }
}

/// 移交给现有 SIM 后台 Group 的材料，没有自己的线程或通道。
pub struct AcceptedMemoryProjection<P: MemoryOrderEvidence> {
    root: Arc<PathBuf>,
    context: Arc<ExecutionNormalizationContextV1>,
    batch: Arc<MemoryNoeBatch<P>>,
    observation: MemoryNoeObservation<P>,
    durability: Option<NoeBatchDurabilityV1>,
}
pub enum AcceptedNoeProjectionV1 {
    Legacy(AcceptedMemoryProjection<LegacyOrderEvidence>),
    Measured(AcceptedMemoryProjection<MeasuredOrderEvidence>),
    Cancellation(AcceptedMemoryProjection<MeasuredCancellationEvidence>),
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum BackgroundNoeCheckpointV1 {
    Legacy(execution_normalization::durable::StagedNormalizationCheckpointV1),
    Measured(execution_normalization::durable::StagedNormalizationCheckpointV1),
    Cancellation(execution_normalization::durable::StagedNormalizationCheckpointV1),
}
pub(crate) enum BackgroundNoeWriterV1 {
    Legacy(BackgroundNoeWriter<LegacyOrderEvidence>),
    Measured(BackgroundNoeWriter<MeasuredOrderEvidence>),
    Cancellation(BackgroundNoeWriter<MeasuredCancellationEvidence>),
}
pub(crate) struct BackgroundNoeWriter<P: MemoryOrderEvidence> {
    writer: Writer<P>,
    observation: MemoryNoeObservation<P>,
    pending_mappings: BTreeMap<u64, NoeBatchDurabilityV1>,
}
impl<P: MemoryOrderEvidence> BackgroundNoeWriter<P>
where
    SourceNeutralTradeLifecycleResultV2: NoeTradePayload<ControlFor<P>>,
    ControlFor<P>: serde::de::DeserializeOwned,
{
    fn create(
        accepted: &AcceptedMemoryProjection<P>,
        batched: bool,
    ) -> Result<Self, DurableErrorV1> {
        let writer = if batched {
            Writer::<P>::create_batched(&accepted.root, (*accepted.context).clone())?
        } else {
            Writer::<P>::create(&accepted.root, (*accepted.context).clone())?
        };
        accepted.observation.0.lock().unwrap().durable_directory =
            Some(writer.context_directory().to_owned());
        Ok(Self {
            writer,
            observation: accepted.observation.clone(),
            pending_mappings: BTreeMap::new(),
        })
    }
    fn stage(
        &mut self,
        batch: &MemoryNoeBatch<P>,
        durability: Option<NoeBatchDurabilityV1>,
    ) -> Result<(), DurableErrorV1> {
        self.writer.stage(batch)?;
        if let Some(mapping) = durability {
            self.pending_mappings
                .insert(batch.source_watermark.projection_batch_sequence, mapping);
        }
        Ok(())
    }
    fn stabilize(&mut self) -> Result<(), DurableErrorV1> {
        let observation = self.observation.clone();
        let mappings = &mut self.pending_mappings;
        self.writer.stabilize(
            || {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("UTC 在 epoch 之后")
                    .as_millis() as i64
            },
            |commit| {
                if let Some(mapping) =
                    mappings.remove(&commit.boundary.source_watermark.projection_batch_sequence)
                {
                    mapping.publish(&commit.container_reference);
                }
                let mut state = observation.0.lock().unwrap();
                state.durable = commit.boundary.source_watermark.projection_batch_sequence;
            },
        )
    }
}
impl BackgroundNoeWriterV1 {
    pub(crate) fn checkpoint(&self) -> BackgroundNoeCheckpointV1 {
        match self {
            Self::Legacy(w) => BackgroundNoeCheckpointV1::Legacy(w.writer.checkpoint()),
            Self::Measured(w) => BackgroundNoeCheckpointV1::Measured(w.writer.checkpoint()),
            Self::Cancellation(w) => BackgroundNoeCheckpointV1::Cancellation(w.writer.checkpoint()),
        }
    }
    pub(crate) fn restore_v4(
        saved: BackgroundNoeCheckpointV1,
        observation: MemoryNoeObservationV4,
    ) -> Result<Self, DurableErrorV1> {
        let BackgroundNoeCheckpointV1::Cancellation(saved) = saved else {
            return Err(DurableErrorV1::ContainerDecode {
                path: PathBuf::new(),
                message: "检查点 NOE 表示与原 V4 端口不同".into(),
            });
        };
        Ok(Self::Cancellation(BackgroundNoeWriter {
            writer: Writer::<MeasuredCancellationEvidence>::restore_checkpoint(saved)?,
            observation,
            pending_mappings: Default::default(),
        }))
    }

    pub(crate) fn stage(
        writer: &mut Option<Self>,
        accepted: AcceptedNoeProjectionV1,
        batched: bool,
    ) -> Result<(), DurableErrorV1> {
        match accepted {
            AcceptedNoeProjectionV1::Legacy(accepted) => {
                if writer.is_none() {
                    *writer = Some(Self::Legacy(BackgroundNoeWriter::create(
                        &accepted, batched,
                    )?));
                }
                let Some(Self::Legacy(writer)) = writer else {
                    unreachable!("同一运行端口的订单证据类型固定")
                };
                writer.stage(&accepted.batch, accepted.durability)
            }
            AcceptedNoeProjectionV1::Measured(accepted) => {
                if writer.is_none() {
                    *writer = Some(Self::Measured(BackgroundNoeWriter::create(
                        &accepted, batched,
                    )?));
                }
                let Some(Self::Measured(writer)) = writer else {
                    unreachable!("同一运行端口的订单证据类型固定")
                };
                writer.stage(&accepted.batch, accepted.durability)
            }
            AcceptedNoeProjectionV1::Cancellation(accepted) => {
                if writer.is_none() {
                    *writer = Some(Self::Cancellation(BackgroundNoeWriter::create(
                        &accepted, batched,
                    )?));
                }
                let Some(Self::Cancellation(writer)) = writer else {
                    unreachable!("同一运行端口的订单证据类型固定")
                };
                writer.stage(&accepted.batch, accepted.durability)
            }
        }
    }
    pub(crate) fn needs_stability(&self) -> bool {
        match self {
            Self::Legacy(writer) => writer.writer.needs_stability(),
            Self::Measured(writer) => writer.writer.needs_stability(),
            Self::Cancellation(writer) => writer.writer.needs_stability(),
        }
    }
    pub(crate) fn stabilize(&mut self) -> Result<(), DurableErrorV1> {
        match self {
            Self::Legacy(writer) => writer.stabilize(),
            Self::Measured(writer) => writer.stabilize(),
            Self::Cancellation(writer) => writer.stabilize(),
        }
    }
}

pub struct MemoryNoeProjectionPort<P: MemoryOrderEvidence> {
    root: Arc<PathBuf>,
    opened: RunOpenedV2,
    run_opened_reference: DurableSourceReferenceV1,
    account_opening_reference: DurableSourceReferenceV1,
    account_opening_evidence_identity: String,
    implementation_identity: String,
    context: Option<Arc<ExecutionNormalizationContextV1>>,
    core: Option<Core<P>>,
    observation: MemoryNoeObservation<P>,
}
impl<P: MemoryOrderEvidence> MemoryNoeProjectionPort<P> {
    pub fn new(
        root: impl AsRef<Path>,
        opened: RunOpenedV2,
        run_opened_reference: DurableSourceReferenceV1,
        account_opening_reference: DurableSourceReferenceV1,
        account_opening_evidence_identity: String,
        implementation_identity: String,
    ) -> Self {
        Self {
            root: Arc::new(root.as_ref().to_owned()),
            opened,
            run_opened_reference,
            account_opening_reference,
            account_opening_evidence_identity,
            implementation_identity,
            context: None,
            core: None,
            observation: MemoryNoeObservation::<P>::default(),
        }
    }
    pub fn observation(&self) -> MemoryNoeObservation<P> {
        self.observation.clone()
    }
    fn accept(
        &mut self,
        payload: Arc<BusinessProjectionBatchPayloadV1>,
    ) -> Result<AcceptedNoeProjectionV1, MemoryNoeErrorV1> {
        let batch = P::batch::<contracts::TradeLifecycleFeedbackV3, Identity>(payload.as_ref())
            .ok_or(MemoryNoeErrorV1::InvalidProjection(
                "运行与端口的投影版本不符",
            ))?;
        if batch.run_id != self.opened.run_id {
            return Err(MemoryNoeErrorV1::InvalidProjection("来源运行与端口不符"));
        }
        if let Some(context) = &self.context {
            if batch.account_opening.as_ref().is_some_and(|opening| {
                opening.account_instance_key != context.source.account_instance_key
            }) {
                return Err(MemoryNoeErrorV1::InvalidProjection("来源账户与上下文不符"));
            }
        }
        if self.core.is_none() {
            let opening = batch
                .account_opening
                .as_ref()
                .ok_or(MemoryNoeErrorV1::InvalidProjection("首批缺少开户事实"))?;
            let context = execution_normalization::sim_v2_context(
                SimV2ContextSeedV1 {
                    run_id: self.opened.run_id.clone(),
                    account_instance_key: opening.account_instance_key.clone(),
                    run_opened: self.run_opened_reference.clone(),
                    account_opening: self.account_opening_reference.clone(),
                    account_opening_evidence_identity: self
                        .account_opening_evidence_identity
                        .clone(),
                    implementation_identity: self.implementation_identity.clone(),
                },
                P::contract_version(),
            )
            .map_err(MemoryNoeErrorV1::Durable)?;
            self.core = Some(Core::<P>::new(context.clone()));
            self.context = Some(Arc::new(context));
        }
        let context = self.context.as_ref().unwrap();
        let core = self.core.as_mut().unwrap();
        let mapper = BatchMapper::<
            contracts::TradeLifecycleFeedbackV3,
            P,
            Identity,
            FactReference,
        >::with_account(context, Arc::clone(core.account_identity()), &batch);
        let input = mapper
            .map()
            .map_err(|()| MemoryNoeErrorV1::InvalidProjection("来源与上下文不符"))?;
        let batch = Arc::new(
            core.normalize_and_accept(input)
                .map_err(MemoryNoeErrorV1::Normalization)?,
        );
        let mut state = self.observation.0.lock().unwrap();
        state.accepted = batch.source_watermark.projection_batch_sequence;
        let durability =
            P::accept_consumption(&mut state.consumption, context.clone(), batch.clone());
        if durability.is_none() {
            let ordinals = batch
                .records
                .iter()
                .enumerate()
                .filter_map(|(ordinal, record)| feedback_coordinate::<P>(record).map(|_| ordinal))
                .collect::<Vec<_>>();
            if !ordinals.is_empty() {
                state.feedback.push_back(PendingFeedback {
                    batch: Arc::clone(&batch),
                    ordinals,
                });
            }
        }
        Ok(P::into_background(AcceptedMemoryProjection {
            root: Arc::clone(&self.root),
            context: Arc::clone(context),
            batch,
            observation: self.observation.clone(),
            durability,
        }))
    }
}
impl<P: MemoryOrderEvidence> BusinessProjectionPortV2 for MemoryNoeProjectionPort<P> {
    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, MemoryNoeErrorV1> {
        let mut bytes = Vec::new();
        self.write_checkpoint(&mut bytes).map_err(|e| {
            MemoryNoeErrorV1::Durable(DurableErrorV1::ContainerDecode {
                path: (*self.root).clone(),
                message: e.to_string(),
            })
        })?;
        Ok(Some(bytes))
    }

    fn try_accept(&mut self, _: Arc<BusinessProjectionBatchV2>) -> bool {
        false
    }
    fn durable_through(&self, run_id: &str) -> u64 {
        if run_id == self.opened.run_id {
            self.observation.durable_through()
        } else {
            0
        }
    }
    fn uses_background_normalization(&self) -> bool {
        true
    }
    fn accept_for_background(
        &mut self,
        batch: Arc<BusinessProjectionBatchPayloadV1>,
    ) -> Result<AcceptedNoeProjectionV1, MemoryNoeErrorV1> {
        self.accept(batch)
    }
    fn close_receive_prefix(&mut self, at: i64) {
        self.observation.0.lock().unwrap().closed_receive_time = Some(at);
    }
    fn observe_public_time(
        &mut self,
        time: &crate::public_consumption::TrustedActorExecutionTimeV1,
    ) {
        self.observation.0.lock().unwrap().public_visible_time = Some(time.logical_time_utc_ms());
    }
}
impl<P: MemoryOrderEvidence> crate::public_consumption::PublicProjectionPortV1
    for MemoryNoeProjectionPort<P>
{
    fn delivered_private_feedback(&self, run_id: &str) -> u64 {
        if run_id == self.opened.run_id {
            self.observation.delivered_through()
        } else {
            0
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "Core<P>:serde::Serialize, ObservationState<P>:serde::Serialize",
    deserialize = "Core<P>:serde::Deserialize<'de>, ObservationState<P>:serde::Deserialize<'de>"
))]
struct MemoryNoePortCheckpointV1<P: MemoryOrderEvidence> {
    root: Arc<PathBuf>,
    opened: RunOpenedV2,
    run_opened_reference: DurableSourceReferenceV1,
    account_opening_reference: DurableSourceReferenceV1,
    account_opening_evidence_identity: String,
    implementation_identity: String,
    context: Option<Arc<ExecutionNormalizationContextV1>>,
    core: Option<Core<P>>,
    observation: ObservationState<P>,
}
#[derive(serde::Serialize)]
#[serde(bound(serialize = "Core<P>:serde::Serialize, ObservationState<P>:serde::Serialize"))]
struct MemoryNoePortCheckpointRefV1<'a, P: MemoryOrderEvidence> {
    root: &'a Arc<PathBuf>,
    opened: &'a RunOpenedV2,
    run_opened_reference: &'a DurableSourceReferenceV1,
    account_opening_reference: &'a DurableSourceReferenceV1,
    account_opening_evidence_identity: &'a str,
    implementation_identity: &'a str,
    context: &'a Option<Arc<ExecutionNormalizationContextV1>>,
    core: &'a Option<Core<P>>,
    observation: &'a ObservationState<P>,
}
impl<P: MemoryOrderEvidence> MemoryNoeProjectionPort<P> {
    pub fn write_checkpoint(
        &self,
        writer: impl std::io::Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let observation = self.observation.0.lock().unwrap();
        ciborium::ser::into_writer(
            &MemoryNoePortCheckpointRefV1 {
                root: &self.root,
                opened: &self.opened,
                run_opened_reference: &self.run_opened_reference,
                account_opening_reference: &self.account_opening_reference,
                account_opening_evidence_identity: &self.account_opening_evidence_identity,
                implementation_identity: &self.implementation_identity,
                context: &self.context,
                core: &self.core,
                observation: &observation,
            },
            writer,
        )?;
        Ok(())
    }
    pub fn restore_checkpoint(
        reader: impl std::io::Read,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        P::Completion: serde::de::DeserializeOwned,
        Control<P>: serde::de::DeserializeOwned,
    {
        let saved: MemoryNoePortCheckpointV1<P> = ciborium::de::from_reader(reader)?;
        Ok(Self {
            root: saved.root,
            opened: saved.opened,
            run_opened_reference: saved.run_opened_reference,
            account_opening_reference: saved.account_opening_reference,
            account_opening_evidence_identity: saved.account_opening_evidence_identity,
            implementation_identity: saved.implementation_identity,
            context: saved.context,
            core: saved.core,
            observation: MemoryNoeObservation(Arc::new(Mutex::new(saved.observation))),
        })
    }
}

impl MemoryNoeObservation<MeasuredCancellationEvidence> {
    /// V4正式消费源的最早未领取接收时刻；不推进前沿或公开可见性。
    pub fn next_pending_feedback_received_at_utc_ms(&self) -> Option<i64> {
        self.0
            .lock()
            .unwrap()
            .consumption
            .next_pending_feedback_received_at_utc_ms()
    }
}
