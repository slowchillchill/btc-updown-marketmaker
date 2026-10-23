//! 在线交接所属方：来源准备、唯一核心、当前借用消费及唯一后台记录。
//! 没有历史排序、账户、模型或时钟关闭调度。正常结束仅来自生产者 End。
use crate::{
    gate_a::GateAOutputConfig,
    public_recording::*,
    public_replay::{ReferenceCandidateReaderV1, reference_candidate, reference_market_for_input},
};
use contracts::*;
use data_collect::handoff::HandoffReceiverV2;
use data_pipeline_core::{public_input::*, public_observation_core::*, public_reference::*};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicOnlineConfigV1 {
    /// 显式采集正式来源进展；旧消费者反序列化时保持关闭。
    #[serde(default)]
    pub source_progress: bool,
    pub header: PublicInputStreamHeaderV1,
    pub recording: GateAOutputConfig,
    /// 包含后台当前正在写入的对象；不是字节预算或生产默认。
    pub recording_in_flight_inputs: NonZeroUsize,
    pub stabilize_every_inputs: NonZeroU64,
    pub read_page_rows: NonZeroUsize,
    pub max_read_batch_bytes: NonZeroUsize,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct PublicOnlineMeasurementsV1 {
    pub applied_inputs: u64,
    pub consumed_groups: u64,
    pub consumed_observations: u64,
    pub peak_recording_in_flight_inputs: usize,
    pub peak_unstable_reference_candidates: usize,
    pub peak_reference_page_candidates: usize,
    pub stable_prefix: Option<CommittedIngressPrefixV1>,
}
#[derive(Clone, Debug, Serialize)]
pub struct PublicOnlineCompletionV1 {
    pub session: DataCollectHandoffSessionV2,
    pub last_received_transport_sequence: u64,
    pub measurements: PublicOnlineMeasurementsV1,
    pub committed_prefix: CommittedIngressPrefixV1,
}
#[derive(Default)]
struct Shared {
    prefix: Option<CommittedIngressPrefixV1>,
    // 唯一记录所有者的未稳定依赖材料；稳定后释放，不保留全历史。
    tail: VecDeque<TemporaryReferenceCandidateV1>,
    failure: Option<String>,
}
struct Pending {
    input: RecordedIngressV1,
    result: PublicAppliedIngressV1<'static>,
    _capacity: OwnedSemaphorePermit,
}
enum Command {
    Record(Box<Pending>),
    Finish(oneshot::Sender<RecordingResult<Option<CommittedIngressPrefixV1>>>),
}
struct Background {
    sender: mpsc::UnboundedSender<Command>,
    capacity: Arc<Semaphore>,
    shared: Arc<Mutex<Shared>>,
    failure_notice: Arc<Notify>,
}
impl Background {
    fn start(root: &Path, config: &PublicOnlineConfigV1) -> RecordingResult<Self> {
        let writer =
            PublicRecordingWriterV1::create(root, &config.header, config.recording.clone())?;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let capacity = Arc::new(Semaphore::new(config.recording_in_flight_inputs.get()));
        let shared = Arc::new(Mutex::new(Shared::default()));
        let failure_notice = Arc::new(Notify::new());
        let notice = failure_notice.clone();
        let state = shared.clone();
        let slots = capacity.clone();
        let interval = config.stabilize_every_inputs.get();
        #[cfg(test)]
        let before_write = test_control::take();
        std::thread::Builder::new()
            .name("public-recording".into())
            .spawn(move || {
                let mut writer = writer;
                let result = (|| -> RecordingResult<()> {
                    while let Some(command) = receiver.blocking_recv() {
                        match command {
                            Command::Record(pending) => {
                                #[cfg(test)]
                                if let Some(hook) = &before_write {
                                    hook();
                                }
                                let position = pending.input.input_position + 1;
                                writer.record(pending.input, pending.result)?;
                                if position.is_multiple_of(interval) {
                                    publish(&mut writer, &state)?;
                                }
                                // 容量包含实际写入过程，正常返回后释放。
                                drop(pending._capacity);
                            }
                            Command::Finish(reply) => {
                                let prefix = publish(&mut writer, &state)?;
                                let _ = reply.send(Ok(prefix));
                                return Ok(());
                            }
                        }
                    }
                    // 宿主取消/断连只关闭通道，不合成 End，也不主动稳定尾部。
                    Ok(())
                })();
                if let Err(error) = result {
                    state.lock().unwrap().failure = Some(error.to_string());
                    notice.notify_one();
                }
                slots.close();
            })?;
        Ok(Self {
            sender,
            capacity,
            shared,
            failure_notice,
        })
    }
    fn check(&self) -> RecordingResult<()> {
        if let Some(reason) = &self.shared.lock().unwrap().failure {
            return Err(io::Error::other(format!("公共记录后台失败：{reason}")).into());
        }
        Ok(())
    }
    async fn failure(&self) -> RecordingResult<()> {
        let notified = self.failure_notice.notified();
        self.check()?;
        notified.await;
        self.check()
    }
    async fn reserve(&self) -> RecordingResult<OwnedSemaphorePermit> {
        let permit = self.capacity.clone().acquire_owned().await;
        self.check()?;
        permit.map_err(|_| io::Error::other("公共记录后台已停止").into())
    }
    async fn finish(&self) -> RecordingResult<Option<CommittedIngressPrefixV1>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Finish(sender))
            .map_err(|_| io::Error::other("公共记录后台已停止"))?;
        let result = receiver.await;
        self.check()?;
        result.map_err(|_| io::Error::other("公共记录后台未返回稳定凭据"))?
    }
}
fn publish(
    writer: &mut PublicRecordingWriterV1,
    state: &Mutex<Shared>,
) -> RecordingResult<Option<CommittedIngressPrefixV1>> {
    let prefix = writer.stabilize()?;
    let mut state = state.lock().unwrap();
    if let Some(prefix) = &prefix {
        while state
            .tail
            .front()
            .is_some_and(|c| c.record_ref.input_position < prefix.next_input_position)
        {
            state.tail.pop_front();
        }
    }
    state.prefix = prefix.clone();
    Ok(prefix)
}

/// 当前消费者只借用本次完整结果；无编码/写盘等待进入核心及此回调。
/// 本函数属于来源后台宿主。宿主取消应丢弃此 future 和连接，不恢复半帧。
/// 如果消费回调失败，可能已发生核心应用；整体未完成，不声称回滚。
pub async fn consume_public_online_v1<S, F>(
    stream: S,
    recording_root: &Path,
    config: PublicOnlineConfigV1,
    consume: F,
) -> RecordingResult<PublicOnlineCompletionV1>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(&AppliedIngressV1) -> RecordingResult<()>,
{
    consume_public_online_until_v1(
        stream,
        recording_root,
        config,
        consume,
        std::future::pending(),
    )
    .await
}

/// 宿主显式取消沿未完成错误返回已有计量，不合成 End 或恢复连接。
pub async fn consume_public_online_until_v1<S, F>(
    stream: S,
    recording_root: &Path,
    config: PublicOnlineConfigV1,
    consume: F,
    cancel: impl std::future::Future<Output = RecordingResult<()>>,
) -> RecordingResult<PublicOnlineCompletionV1>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(&AppliedIngressV1) -> RecordingResult<()>,
{
    match consume_public_online_with_stop_v1(
        stream,
        recording_root,
        config,
        consume,
        cancel,
        std::future::pending(),
    )
    .await?
    {
        PublicOnlineOutcomeV1::End(completion) => Ok(completion),
        PublicOnlineOutcomeV1::Stopped(_) => unreachable!("此入口没有计划停止信号"),
    }
}

/// 计划停止只在输入应用、记录移交和交接确认完成的边界生效。
/// Stopped 仅证明当前消费者的既有前缀稳定，不证明生产者正常结束。
#[derive(Clone, Debug, Serialize)]
pub struct PublicOnlineStoppedV1 {
    pub session: DataCollectHandoffSessionV2,
    pub last_received_transport_sequence: u64,
    pub measurements: PublicOnlineMeasurementsV1,
}
#[derive(Clone, Debug, Serialize)]
pub enum PublicOnlineOutcomeV1 {
    End(PublicOnlineCompletionV1),
    Stopped(PublicOnlineStoppedV1),
}

/// cancel 保持未完成错误；stop 独立请求排空既有前缀。停止后丢弃连接，不恢复半帧。
pub async fn consume_public_online_with_stop_v1<S, F>(
    stream: S,
    recording_root: &Path,
    config: PublicOnlineConfigV1,
    mut consume: F,
    cancel: impl std::future::Future<Output = RecordingResult<()>>,
    stop: impl std::future::Future<Output = ()>,
) -> RecordingResult<PublicOnlineOutcomeV1>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(&AppliedIngressV1) -> RecordingResult<()>,
{
    // 保留旧启动顺序：核心/记录创建错误不进入运行错误包装，也不被 cancel 抢先。
    let mut core = PublicObservationCoreV1::new(config.header.clone())?;
    if config.source_progress {
        core.enable_source_progress();
    }
    let background = Background::start(recording_root, &config)?;
    let mut progress = PublicOnlineProgressV1 {
        shared: Some(background.shared.clone()),
        ..Default::default()
    };
    let outcome = {
        let running = async {
            let mut session = PublicOnlineSessionV1::start_initialized(
                stream,
                recording_root,
                config,
                &mut progress,
                core,
                background,
            )
            .await?;
            tokio::pin!(stop);
            loop {
                match session.next_applied(&mut stop).await? {
                    PublicOnlineNextV1::Stopped(session) => {
                        return session
                            .stop_and_drain()
                            .await
                            .map(PublicOnlineOutcomeV1::Stopped);
                    }
                    PublicOnlineNextV1::Applied(pending) => {
                        consume(pending.applied())?;
                        match pending.complete().await? {
                            PublicOnlineCompletedInputV1::Continue(next) => session = next,
                            PublicOnlineCompletedInputV1::End(end) => {
                                return Ok(PublicOnlineOutcomeV1::End(end));
                            }
                        }
                    }
                }
            }
        };
        tokio::select! {
            result = running => result,
            result = cancel => Err(result.err().unwrap_or_else(|| io::Error::other("宿主取消：未完成，不合成正常 End").into())),
        }
    };
    outcome.map_err(|error| progress.failure(error))
}

/// 原在线循环的实际计量，只有一份。置于宿主作用域以便显式取消后仍可读取。
#[derive(Default)]
pub struct PublicOnlineProgressV1 {
    pub measurements: PublicOnlineMeasurementsV1,
    pub last_received_transport_sequence: u64,
    shared: Option<Arc<Mutex<Shared>>>,
}
impl PublicOnlineProgressV1 {
    pub fn failure(
        &mut self,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        if let Some(shared) = &self.shared {
            self.measurements.stable_prefix = shared.lock().unwrap().prefix.clone();
        }
        Box::new(PublicOnlineFailureV1 {
            reason: error.to_string(),
            last_received_transport_sequence: self.last_received_transport_sequence,
            measurements: self.measurements.clone(),
        })
    }
}

/// 原接收器、核心与唯一记录后台。每次只把所有权交给一个输入。
/// 等待 future 必须在 timer 分支之间保留；丢弃它意味着放弃本连接，不能恢复半帧。
pub struct PublicOnlineSessionV1<'a, S> {
    receiver: HandoffReceiverV2<S>,
    core: PublicObservationCoreV1,
    background: Background,
    recording_root: std::path::PathBuf,
    config: PublicOnlineConfigV1,
    progress: &'a mut PublicOnlineProgressV1,
}

/// 按值交接原对象；不为缩小枚举新增逐输入堆分配。
pub enum PublicOnlineNextV1<'a, S> {
    Applied(PublicOnlinePendingV1<'a, S>),
    /// 停止已成立。退出 timer 选择后调用 stop_and_drain；这里尚未等待后台。
    Stopped(PublicOnlineSessionV1<'a, S>),
}
pub enum PublicOnlineCompletedInputV1<'a, S> {
    Continue(PublicOnlineSessionV1<'a, S>),
    End(PublicOnlineCompletionV1),
}

/// 同次准备的 PM 原对象和规范位置起点；没有重解析或独立位置计数器。
pub struct PublicOnlineMaterialsV1 {
    pub first_canonical_position: u64,
    pub prepared_pm: Option<data_pipeline_core::pm_reduction::PreparedPmMessageV1>,
    pub pm_contexts: BTreeMap<
        String,
        data_pipeline_core::pm_reduction::PmMarketContextV1<
            data_pipeline_core::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV3>,
        >,
    >,
}

/// 未完成输入拥有整个 session；丢弃它即结束本会话，不能漏记后继续 next。
/// 消费者先同步使用 applied/materials，再完整 await complete；期间不插 timer 或计划 stop。
pub struct PublicOnlinePendingV1<'a, S> {
    session: PublicOnlineSessionV1<'a, S>,
    pending: Pending,
    transport: Option<u64>,
    ended: bool,
    materials: PublicOnlineMaterialsV1,
}
impl<'a, S: AsyncRead + AsyncWrite + Unpin> PublicOnlineSessionV1<'a, S> {
    pub async fn start(
        stream: S,
        recording_root: &Path,
        config: PublicOnlineConfigV1,
        progress: &'a mut PublicOnlineProgressV1,
    ) -> RecordingResult<Self> {
        let mut core = PublicObservationCoreV1::new(config.header.clone())?;
        if config.source_progress {
            core.enable_source_progress();
        }
        let background = Background::start(recording_root, &config)?;
        progress.shared = Some(background.shared.clone());
        Self::start_initialized(stream, recording_root, config, progress, core, background).await
    }
    async fn start_initialized(
        stream: S,
        recording_root: &Path,
        config: PublicOnlineConfigV1,
        progress: &'a mut PublicOnlineProgressV1,
        core: PublicObservationCoreV1,
        background: Background,
    ) -> RecordingResult<Self> {
        let receiver = HandoffReceiverV2::start(stream).await?;
        Ok(Self {
            receiver,
            core,
            background,
            recording_root: recording_root.to_owned(),
            config,
            progress,
        })
    }

    /// 仅读取/异步容量等待可与 timer 协作。stop 只返回所有权，不在此排空。
    /// 同步 prepare/core.push 完成后交出材料，后续由 complete 完成原记录确认段。
    pub async fn next_applied(
        mut self,
        stop: impl std::future::Future<Output = ()>,
    ) -> RecordingResult<PublicOnlineNextV1<'a, S>> {
        let (permit, frame) = tokio::select! {
            biased;
            result = self.background.failure() => { result?; return Err(io::Error::other("公共记录后台停止").into()); }
            () = stop => return Ok(PublicOnlineNextV1::Stopped(self)),
            result = async {
                let permit = self.background.reserve().await?;
                let frame = self.receiver.next_frame().await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((permit, frame))
            } => result?,
        };
        self.background.check()?;
        let Self {
            core,
            background,
            recording_root,
            config,
            progress,
            ..
        } = &mut self;
        let first_canonical_position = core.next_canonical_position();
        let (ingress, transport, ended) = match frame {
            DataCollectHandoffV2::Message {
                row,
                transport_sequence,
            } => {
                let source_evidence = source_evidence_v3_from_typed_raw_v2(&row)?;
                (
                    DptIngressV1::Message {
                        row,
                        source_evidence,
                    },
                    Some(transport_sequence),
                    false,
                )
            }
            DataCollectHandoffV2::End { .. } => (DptIngressV1::EndOfInput, None, true),
            DataCollectHandoffV2::Aborted { reason, .. } => {
                return Err(io::Error::other(format!("来源 Aborted：{reason}")).into());
            }
            _ => unreachable!("原交接接收器已核对控制帧"),
        };
        let mut input = RecordedIngressV1 {
            input_stream_id: config.header.input_stream_id.clone(),
            input_position: progress.measurements.applied_inputs,
            ingress,
            dependency_record_refs: Vec::new(),
        };
        let identities = BTreeMap::new();
        let mut page_peak = 0;
        let market = reference_market_for_input(core, &input);
        let mut prepared = prepare_public_ingress_v1(core, &input, &identities, &[])?;
        // 本条 TWAP 也参与既定候选选择；沿已准备规范事实，不重复规范化。
        let candidate = match &prepared.current {
            PreparedCanonicalInputV1::ChainlinkTwap(event) => Some(reference_candidate(
                PublicFactPositionV1 {
                    input_stream_id: input.input_stream_id.clone(),
                    closed_at_input_position: input.input_position,
                    fact_ordinal: 0,
                },
                event.clone(),
            )),
            _ => None,
        };
        if let Some(market) = market {
            let state = background.shared.lock().unwrap();
            let mut reader = None;
            let mut disk_done = false;
            let cutoff = input.input_position;
            prepare_temporary_reference_for_market_v1(core, &mut prepared, &market, |cursor| {
                let page = (|| -> RecordingResult<_> {
                    if !disk_done {
                        if reader.is_none() {
                            reader = Some(ReferenceCandidateReaderV1::open_committed(
                                recording_root,
                                state.prefix.clone(),
                                config.read_page_rows.get(),
                                config.max_read_batch_bytes.get(),
                                cutoff,
                            )?);
                        }
                        let (values, next, done) = reader.as_mut().unwrap().page(cursor)?;
                        disk_done = done;
                        page_peak = page_peak.max(values.len());
                        if !values.is_empty() || !done {
                            return Ok((values, next, false));
                        }
                    }
                    let mut next = cursor;
                    let values: Vec<_> = state
                        .tail
                        .iter()
                        .chain(candidate.iter())
                        .filter(|c| {
                            c.record_ref.input_position >= cursor.next_input_position
                                && c.record_ref.input_position <= cutoff
                        })
                        .take(config.read_page_rows.get())
                        .cloned()
                        .collect();
                    if let Some(last) = values.last() {
                        next.next_input_position = last.record_ref.input_position + 1;
                    }
                    let done = !state.tail.iter().chain(candidate.iter()).any(|c| {
                        c.record_ref.input_position >= next.next_input_position
                            && c.record_ref.input_position <= cutoff
                    });
                    page_peak = page_peak.max(values.len());
                    Ok((values, next, done))
                })();
                page.map_err(|e| data_pipeline_core::ContractError::InvalidField {
                    field: "reference_page".into(),
                    reason: e.to_string(),
                })
            })?;
        }
        let applied = core.push(&prepared)?.into_owned();
        progress.measurements.applied_inputs += 1;
        progress.measurements.peak_reference_page_candidates = progress
            .measurements
            .peak_reference_page_candidates
            .max(page_peak);
        let candidate = candidate.filter(|c| {
            applied
                .applied
                .closed_groups
                .iter()
                .any(|g| g.records.iter().any(|f| f.position == c.anchor.position))
        });
        let dependencies = prepared.recorded.dependency_record_refs.clone();
        let prepared_pm = match prepared.current {
            PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
            _ => None,
        };
        input.dependency_record_refs = dependencies;
        let mut pm_contexts = BTreeMap::new();
        for fact in applied
            .applied
            .closed_groups
            .iter()
            .flat_map(|g| g.records.iter())
        {
            let market = match &fact.payload {
                PublicBusinessPayloadV1::PolymarketMessage(message) => Some(&message.market),
                PublicBusinessPayloadV1::MarketFact { market, .. } => Some(market),
                _ => None,
            };
            if let Some(market) = market
                && let Some(context) = core.prepared_pm_context(&market.condition_id.0)
            {
                pm_contexts.insert(market.condition_id.0.clone(), context);
            }
        }
        {
            let mut state = background.shared.lock().unwrap();
            if let Some(candidate) = candidate {
                state.tail.push_back(candidate);
            }
            progress.measurements.peak_unstable_reference_candidates = progress
                .measurements
                .peak_unstable_reference_candidates
                .max(state.tail.len());
            progress.measurements.stable_prefix = state.prefix.clone();
        }
        progress.measurements.peak_recording_in_flight_inputs = progress
            .measurements
            .peak_recording_in_flight_inputs
            .max(config.recording_in_flight_inputs.get() - background.capacity.available_permits());
        Ok(PublicOnlineNextV1::Applied(PublicOnlinePendingV1 {
            session: self,
            pending: Pending {
                input,
                result: applied,
                _capacity: permit,
            },
            transport,
            ended,
            materials: PublicOnlineMaterialsV1 {
                first_canonical_position,
                prepared_pm,
                pm_contexts,
            },
        }))
    }

    /// 只排空已接纳输入，调用前已离开等待/timer 选择，不合成 End。
    pub async fn stop_and_drain(self) -> RecordingResult<PublicOnlineStoppedV1> {
        self.progress.measurements.stable_prefix = self.background.finish().await?;
        Ok(PublicOnlineStoppedV1 {
            session: self.receiver.session().clone(),
            last_received_transport_sequence: self.progress.last_received_transport_sequence,
            measurements: self.progress.measurements.clone(),
        })
    }
}
impl<'a, S: AsyncRead + AsyncWrite + Unpin> PublicOnlinePendingV1<'a, S> {
    pub fn applied(&self) -> &AppliedIngressV1 {
        &self.pending.result.applied
    }
    pub fn materials(&mut self) -> (&AppliedIngressV1, &mut PublicOnlineMaterialsV1) {
        (&self.pending.result.applied, &mut self.materials)
    }
    pub fn input(&self) -> &RecordedIngressV1 {
        &self.pending.input
    }
    /// 原 Record → confirm；实际 End 紧接原 finish。整段不与 timer/计划 stop 竞争。
    /// 显式 cancel 仍可由旧外层取消整个会话，返回未完成错误。
    pub async fn complete(self) -> RecordingResult<PublicOnlineCompletedInputV1<'a, S>> {
        let Self {
            mut session,
            pending,
            transport,
            ended,
            ..
        } = self;
        let measurements = &mut session.progress.measurements;
        measurements.consumed_groups += pending.result.applied.closed_groups.len() as u64;
        measurements.consumed_observations += pending
            .result
            .applied
            .closed_groups
            .iter()
            .map(|g| g.observations.len() as u64)
            .sum::<u64>();
        session
            .background
            .sender
            .send(Command::Record(Box::new(pending)))
            .map_err(|_| io::Error::other("公共记录后台已停止"))?;
        if let Some(sequence) = transport {
            session.receiver.confirm(sequence).await?;
            session.progress.last_received_transport_sequence = sequence;
        }
        if ended {
            let committed_prefix = session
                .background
                .finish()
                .await?
                .ok_or("没有可签发的稳定输入")?;
            measurements.stable_prefix = Some(committed_prefix.clone());
            Ok(PublicOnlineCompletedInputV1::End(
                PublicOnlineCompletionV1 {
                    session: session.receiver.session().clone(),
                    last_received_transport_sequence: session.receiver.last_confirmed_sequence(),
                    measurements: measurements.clone(),
                    committed_prefix,
                },
            ))
        } else {
            Ok(PublicOnlineCompletedInputV1::Continue(session))
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PublicOnlineFailureV1 {
    pub reason: String,
    pub last_received_transport_sequence: u64,
    pub measurements: PublicOnlineMeasurementsV1,
}
impl std::fmt::Display for PublicOnlineFailureV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).map_err(|_| std::fmt::Error)?
        )
    }
}
impl std::error::Error for PublicOnlineFailureV1 {}

#[cfg(test)]
mod test_control {
    use std::{cell::RefCell, sync::Arc};
    pub type Hook = Arc<dyn Fn() + Send + Sync>;
    thread_local! { static HOOK: RefCell<Option<Hook>> = RefCell::new(None); }
    pub fn set(hook: Hook) {
        HOOK.set(Some(hook));
    }
    pub fn take() -> Option<Hook> {
        HOOK.take()
    }
}
#[cfg(test)]
#[path = "public_online_tests.rs"]
mod tests;
