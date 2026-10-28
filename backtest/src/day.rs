//! 原文件日作业与独立候选装配；公共 Core 只由当天生产线程拥有。
use crate::day_state::*;
use crate::{
    run::{
        HistoricalRuntimeCatalogV1, HistoricalRuntimeContextV1, HistoricalSendBoundaryV1,
        run_context_with_hook,
    },
    *,
};
use contracts::*;
use data_pipeline_training::{
    day_producer::DayProducerV1, day_stream::DayIngressV1, historical_initialization::*,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shared_run::StrategyV1;
use sim_system::{
    platform_rules::SealedProcessRuleSnapshotV3, public_consumption::*, scenario::PlatformMarketV1,
};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalDayConfigV1 {
    pub preparation: HistoricalPreparationConfigV1,
    pub candidates: Vec<HistoricalPreparedRunConfigV1>,
    pub progress_interval_seconds: u64,
    /// 生产领先的软目标；消费者仍需关闭隐藏桶时继续必要供料。
    #[serde(default)]
    pub producer_lead_groups: Option<usize>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DayFailureSourceV1 {
    Candidate { index: usize, run_id: String },
    Producer,
    DaySetup,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CandidateOutcomeV1 {
    Complete,
    OriginalFailure,
    CollateralStop,
    NoReport,
}
#[derive(Debug)]
pub struct HistoricalDaysFailureV1 {
    pub cause: MaterialError,
    pub source: DayFailureSourceV1,
    pub days: Vec<HistoricalDayResultV1>,
}
impl std::fmt::Display for HistoricalDaysFailureV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "日研究失败 {:?}: {}", self.source, self.cause)
    }
}
impl std::error::Error for HistoricalDaysFailureV1 {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
#[derive(Debug)]
struct CandidateFailureV1 {
    cause: MaterialError,
    report: Option<serde_json::Value>,
}
impl std::fmt::Display for CandidateFailureV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for CandidateFailureV1 {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
#[derive(Debug, Serialize)]
pub struct HistoricalDayCandidateResultV1 {
    pub run_id: String,
    pub comparison_identity_sha256: Sha256Hex,
    pub initial_available_pusd: ExactDecimal,
    pub report: serde_json::Value,
    pub outcome: CandidateOutcomeV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct HistoricalDayResultV1 {
    pub input_set_identity_sha256: Sha256Hex,
    pub public_applied_inputs: u64,
    pub candidates: Vec<HistoricalDayCandidateResultV1>,
}
/// 显式研究运行选项，不进入业务身份或原配置合同。
#[derive(Clone, Debug)]
pub struct HistoricalDayRunOptionsV1 {
    pub research_summary: bool,
    pub public_observer: Option<Arc<dyn HistoricalPublicObserverV1>>,
    /// 正式预算从输入准备之前开始；候选在完整事件边界返回已有成绩。
    pub started: Instant,
    pub budget_stop_after: Option<Duration>,
    /// 显式执行模式的公共队列逻辑载荷字节软目标；缺省保留原领先组策略。
    pub producer_payload_bytes: Option<u64>,
}
impl Default for HistoricalDayRunOptionsV1 {
    fn default() -> Self {
        Self {
            research_summary: false,
            public_observer: None,
            started: Instant::now(),
            budget_stop_after: None,
            producer_payload_bytes: None,
        }
    }
}
/// 每个日期、每个候选在自己的线程内创建原策略和原运行对象。
/// 同日候选只共享不可变公共组和隐藏桶，不共享账户、时钟或反馈。
pub fn run_historical_days_v1<S, F>(
    days: &[HistoricalDayConfigV1],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
) -> Result<Vec<HistoricalDayResultV1>, MaterialError>
where
    S: StrategyV1,
    S::Diagnostics: Serialize,
    S::Error: std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    let cpus = std::thread::available_parallelism()?.get();
    let widest = days
        .iter()
        .map(|d| d.candidates.len() + 1)
        .max()
        .unwrap_or(1);
    run_historical_days_with_parallelism_v1(days, rules, factory, (cpus / widest).max(1))
}
/// 显式日期并行调度；每个日期内部的候选保留自己的运行线程。
pub fn run_historical_days_with_parallelism_v1<S, F>(
    days: &[HistoricalDayConfigV1],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
    parallel_dates: usize,
) -> Result<Vec<HistoricalDayResultV1>, MaterialError>
where
    S: StrategyV1,
    S::Diagnostics: Serialize,
    S::Error: std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    run_historical_days_with_options_v1(
        days,
        rules,
        factory,
        parallel_dates,
        &HistoricalDayRunOptionsV1::default(),
    )
}
/// 原入口默认关闭研究功能；此入口显式启用小时汇总及预算退出。
pub fn run_historical_days_with_options_v1<S, F>(
    days: &[HistoricalDayConfigV1],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
    parallel_dates: usize,
    options: &HistoricalDayRunOptionsV1,
) -> Result<Vec<HistoricalDayResultV1>, MaterialError>
where
    S: StrategyV1,
    S::Diagnostics: Serialize,
    S::Error: std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    run_historical_days_with_boundary_v1(
        days,
        rules,
        factory,
        parallel_dates,
        options,
        HistoricalSendBoundaryV1::ActualSendBoundary,
    )
}
/// 显式保留宿主原反馈领取时点；复用同一日生产、调度和账户循环。
pub fn run_historical_days_host_managed_with_options_v1<S, F>(
    days: &[HistoricalDayConfigV1],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
    parallel_dates: usize,
    options: &HistoricalDayRunOptionsV1,
) -> Result<Vec<HistoricalDayResultV1>, MaterialError>
where
    S: StrategyV1,
    S::Diagnostics: Serialize,
    S::Error: std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    run_historical_days_with_boundary_v1(
        days,
        rules,
        factory,
        parallel_dates,
        options,
        HistoricalSendBoundaryV1::HostManaged,
    )
}
fn run_historical_days_with_boundary_v1<S, F>(
    days: &[HistoricalDayConfigV1],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
    parallel_dates: usize,
    options: &HistoricalDayRunOptionsV1,
    send_boundary: HistoricalSendBoundaryV1,
) -> Result<Vec<HistoricalDayResultV1>, MaterialError>
where
    S: StrategyV1,
    S::Diagnostics: Serialize,
    S::Error: std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    let mut results = Vec::new();
    for batch in days.chunks(parallel_dates.max(1)) {
        let values = std::thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|day| {
                    let rules = rules.clone();
                    scope
                        .spawn(move || run_day::<S, F>(day, rules, factory, options, send_boundary))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|_| Err("日线程异常退出".into())))
                .collect::<Vec<_>>()
        });
        let mut root = None;
        for value in values {
            match value {
                Ok(day) => results.push(day),
                Err(error) => match error.downcast::<HistoricalDaysFailureV1>() {
                    Ok(mut failure) => {
                        results.append(&mut failure.days);
                        if root.is_none() {
                            root = Some((failure.source, failure.cause));
                        }
                    }
                    Err(error) => {
                        if root.is_none() {
                            root = Some((DayFailureSourceV1::DaySetup, error));
                        }
                    }
                },
            }
        }
        if let Some((source, cause)) = root {
            return Err(Box::new(HistoricalDaysFailureV1 {
                source,
                cause,
                days: results,
            }));
        }
    }
    Ok(results)
}

fn run_day<S, F>(
    day: &HistoricalDayConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
    options: &HistoricalDayRunOptionsV1,
    send_boundary: HistoricalSendBoundaryV1,
) -> Result<HistoricalDayResultV1, MaterialError>
where
    S: StrategyV1,
    S::Diagnostics: Serialize,
    S::Error: std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    let mut effective_day = day.clone();
    for candidate in &mut effective_day.candidates {
        candidate.effective_entry_market_scope = candidate
            .entry_market_scope
            .as_ref()
            .map(|p| {
                p.expand(
                    day.preparation.scope.received_start_utc_ms,
                    day.preparation.scope.received_end_utc_ms,
                )
            })
            .transpose()?;
    }
    let day = &effective_day;
    let config = &day.preparation;
    let inputs =
        data_pipeline_training::freeze_public_source_input_set_v1(&config.scope.manifest_paths)?;
    let admitted = inputs.admitted_daily_inputs();
    let files = admitted
        .iter()
        .flat_map(|day| day.files())
        .collect::<Vec<_>>();
    let selection = config
        .metadata_initialization
        .as_ref()
        .map(|initial| {
            if initial.received_start_utc_ms != config.scope.received_start_utc_ms
                || initial.received_end_utc_ms != config.scope.received_end_utc_ms
            {
                return Err::<_, MaterialError>("初始化用途的研究范围与日范围不一致".into());
            }
            Ok(MetadataInitializationSelectionV1::open(
                initial,
                config.read_page_rows,
                config.max_read_batch_bytes,
            )?)
        })
        .transpose()?;
    let input_identity = Sha256Hex(inputs.input_set_identity_sha256().into());
    let source_identity = match (&selection, &config.metadata_initialization) {
        (Some(selection), Some(initial)) => {
            selection.source_identity(inputs.input_set_identity_sha256(), initial)?
        }
        _ => input_identity.clone(),
    };
    let initial = selection
        .as_ref()
        .map(|s| {
            s.read(config.read_page_rows, config.max_read_batch_bytes)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    // 初始化行在创建线程前已完整选择；来源身份不会依赖消费者速度。
    // 位置仍只在真正应用输入时登记。
    let mut classification_provenance = HistoricalInitializationProvenanceV1 {
        source_message_ids: initial
            .iter()
            .filter_map(|(_, binding)| binding.as_ref().map(|b| b.source_message_id.0.clone()))
            .collect(),
        ..Default::default()
    };
    let mut initialization_classifier =
        crate::preparation::InitializationDependencyClassifier::default();
    let mut bound_options = options.clone();
    if let Some(observer) = &options.public_observer {
        if let Some(bound) = observer.for_stream(&config.header.input_stream_id)? {
            bound_options.public_observer = Some(bound);
        }
    }
    let options = &bound_options;
    let hub = DayHubV1::new(&day.candidates);
    hub.0.0.lock().unwrap().research_summary = options.research_summary;
    let mut count = 0;
    let (mut producer, catalog) = {
        let catalog = Arc::new(RwLock::new(HistoricalRuntimeCatalogV1 {
            markets: Default::default(),
            initialization: HistoricalInitializationProvenanceV1 {
                source_identity_version: selection
                    .as_ref()
                    .map(|_| HISTORICAL_INITIALIZATION_SOURCE_IDENTITY_VERSION.into()),
                files: selection
                    .as_ref()
                    .map(|s| s.files.clone())
                    .unwrap_or_default(),
                ..Default::default()
            },
        }));
        let ingress = DayIngressV1::open(
            &files,
            config.header.input_stream_id.clone(),
            config.read_page_rows,
            config.max_read_batch_bytes,
            config.scope.received_start_utc_ms..config.scope.received_end_utc_ms,
            initial,
            config.canonical.max_epoch_count_per_source,
        )?;
        (
            DayProducerV1::new(
                ingress,
                config.effective_header(),
                input_identity.clone(),
                config.hidden_stream_id.clone(),
            )?,
            catalog,
        )
    };
    // 同输入/规则/参数和交付时序复用一个统计流；不同交付时序保持各自历史。
    let mut statistics: Vec<
        Option<Arc<data_pipeline_core::public_statistics::PublicStatisticsStreamV1>>,
    > = Vec::new();
    for (index, candidate) in day.candidates.iter().enumerate() {
        let previous = day.candidates[..index].iter().position(|previous| {
            previous.public_statistics == candidate.public_statistics
                && previous.timing.public_delivery_delay_ms
                    == candidate.timing.public_delivery_delay_ms
        });
        let stream = match previous {
            Some(previous) => statistics[previous].clone(),
            None => crate::run::statistics_stream(
                candidate,
                source_identity.clone(),
                config.effective_header(),
                rules.file_sha256().clone(),
            )?,
        };
        statistics.push(stream);
    }
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for (index, candidate) in day.candidates.iter().enumerate() {
            let context = HistoricalRuntimeContextV1 {
                send_boundary,
                statistics: statistics[index].clone(),
                public_observer: options.public_observer.clone(),
                start: config.scope.received_start_utc_ms,
                source_identity: source_identity.clone(),
                day_log_binding: Some(sim_system::public_consumption::HistoricalDayLogBindingV1 {
                    input_stream_id: config.header.input_stream_id.clone(),
                    consumption_sequence_ref: candidate.consumption_sequence_ref.clone(),
                    hidden_input_set_identity_sha256: input_identity.clone(),
                    hidden_consumer_stream_id: config.hidden_stream_id.clone(),
                    hidden_ordering_version:
                        data_pipeline_training::hidden_stream::STREAMING_HIDDEN_ORDERING_VERSION
                            .into(),
                }),
                end: config.scope.received_end_utc_ms,
                operating_start: config
                    .metadata_initialization
                    .as_ref()
                    .map(|_| config.scope.received_start_utc_ms),
                catalog: catalog.clone(),
            };
            let rules = rules.clone();
            let hub = hub.clone();
            let metadata = serde_json::json!({"config":candidate,"day":day,"input_set_identity_sha256":input_identity,"source_identity_sha256":source_identity,
                "source_manifests":admitted.iter().map(|d|(d.utc_day(),d.manifest_sha256())).collect::<Vec<_>>(),"rules_file_sha256":rules.file_sha256(),"maker_model_version":rules.maker_model_version_for(candidate.session.caller_role),"caller_role":candidate.session.caller_role,"input_lifecycle_policy":candidate.session.input_lifecycle_policy,"entry":"original-parquet-day-v1"});
            workers.push((
                index,
                scope.spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> Result<_, MaterialError> {
                            let mut strategy = factory(candidate)?;
                            {
                                let mut buffer = hub.0.0.lock().unwrap();
                                buffer.candidates[index].source_progress =
                                    Some(strategy.requires_source_progress());
                                buffer.candidates[index].source_health =
                                    Some(strategy.uses_source_health());
                                buffer.candidates[index].requirements =
                                    Some(if candidate.public_statistics.is_some() {
                                        // F6需要全部可见深度数量，不被策略自己的精简投影请求删去。
                                        PublicObservationRequirementsV1::FULL
                                    } else {
                                        strategy.observation_requirements()
                                    });
                                hub.0.1.control.notify_all();
                            }
                            let delivery = DayLocalDeliveryCacheV1::shared(&hub, index);
                            let public =
                                DayPublicSourceV1::new(hub.clone(), index, delivery.clone());
                            let hidden =
                                DayHiddenSourceV1::new(hub.clone(), index, delivery.clone());
                            let control = DayCandidateControlV1::new(hub.clone(), index, delivery);
                            let runtime = tokio::runtime::Builder::new_current_thread().build()?;
                            let report = runtime
                                .block_on(run_context_with_hook(
                                    candidate,
                                    context,
                                    metadata,
                                    rules.clone(),
                                    &mut strategy,
                                    public,
                                    hidden,
                                    control,
                                ))
                                .map_err(|mut failure| -> MaterialError {
                                    let report =
                                        failure.report.take().map(serde_json::to_value).transpose();
                                    // 报告编码错误不能替换原运行原因。
                                    let report = match report {
                                        Ok(value) => value,
                                        Err(error) => {
                                            eprintln!("失败部分报告编码错误: {error}");
                                            None
                                        }
                                    };
                                    Box::new(CandidateFailureV1 {
                                        cause: Box::new(failure),
                                        report,
                                    })
                                })?;
                            Ok(HistoricalDayCandidateResultV1 {
                                run_id: candidate.session.run_id.clone(),
                                comparison_identity_sha256: comparison_identity(
                                    candidate,
                                    rules.file_sha256(),
                                ),
                                initial_available_pusd: candidate
                                    .session
                                    .initial_account_state
                                    .available_pusd
                                    .clone(),
                                report: serde_json::to_value(report)?,
                                outcome: CandidateOutcomeV1::Complete,
                                error: None,
                            })
                        },
                    ))
                    .unwrap_or_else(|_| Err("候选线程异常退出".into()));
                    hub.finish(index, &result, &candidate.session.run_id);
                    result.map(|value| (index, value))
                }),
            ));
        }
        #[cfg(feature = "stage-timing")]
        let mut stages = crate::stage_timing::StageTimings::new("day_producer".into());
        let started = Instant::now();
        let mut progress = Instant::now();
        let lead = day
            .producer_lead_groups
            .unwrap_or(std::thread::available_parallelism()?.get() * 1024);
        let mut ended = false;
        let mut byte_accounting_seconds = 0.0;
        let production=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||->Result<(),MaterialError> {
            loop {
                {
                    let buffer=hub.0.0.lock().unwrap();
                    if let Some(error)=&buffer.failure {return Err(error.message.clone().into());}
                    if day.progress_interval_seconds>0 && progress.elapsed()>=Duration::from_secs(day.progress_interval_seconds) {
                        eprintln!("日作业: phase={}, inputs={count}, active_candidates={}, retained_public_groups={}, retained_hidden_buckets={}, elapsed_seconds={}, total_inputs=unknown",if ended {"consuming"} else {"producing"},buffer.candidates.iter().filter(|c|c.active).count(),buffer.public.len(),buffer.hidden.len(),started.elapsed().as_secs());progress=Instant::now();
                    }
                    if buffer.candidates.iter().any(|c|c.active&&(!c.runtime_started||c.source_health.is_none()||c.source_progress.is_none()||c.requirements.is_none())) {let _=measure_stage!(stages, ProducerWaitStartupOrCompletion, hub.0.1.control.wait_timeout(buffer,Duration::from_millis(20))).unwrap();continue;}
                    if options.public_observer.is_some() || buffer.candidates.iter().any(|c|c.source_health==Some(true)||c.source_progress==Some(true)) {producer.core.enable_source_progress();}
                    if count == 0 {
                        producer.core.set_observation_requirements(contracts::PublicObservationRequirementsV1 {
                            binance_depth_quantities: options.public_observer.as_ref().is_some_and(|o| o.requirements().binance_depth_quantities) || buffer.candidates.iter().filter(|c|c.active).any(|c|c.requirements.is_some_and(|r| r.binance_depth_quantities)),
                        });
                    }
                    if buffer.candidates.iter().all(|c| !c.active) {
                        drop(buffer);
                        return Ok(());
                    }
                    if !hub.0.1.budget_stop_requested.load(std::sync::atomic::Ordering::Acquire) && options.budget_stop_after.is_some_and(|after| options.started.elapsed() >= after) {
                        hub.0.1.budget_stop_requested.store(true, std::sync::atomic::Ordering::Release);
                        hub.0.1.notify_all();
                    }
                    if ended {let _=measure_stage!(stages, ProducerWaitStartupOrCompletion, hub.0.1.control.wait_timeout(buffer,Duration::from_millis(20))).unwrap();continue;}
                    // 已有可消费材料时让慢候选追上；等待必要完整隐藏桶时继续供料，避免互等。
                    let backlog = options.producer_payload_bytes.map_or(buffer.public.len()+buffer.hidden.len()>lead, |limit| buffer.retained_payload_bytes >= limit && buffer.retained_payload_bytes > 0);
                    if backlog && buffer.candidates.iter().any(|c|c.active&&!c.waiting) {
                        let wait_started=Instant::now();
                        let (mut buffer, _)=measure_stage!(stages, ProducerBackpressureWait, hub.0.1.control.wait_timeout(buffer,Duration::from_millis(10))).unwrap();
                        buffer.producer_wait_seconds += wait_started.elapsed().as_secs_f64();continue;
                    }
                }
                let Some(mut next)=measure_stage!(stages, NextAppliedIncludingDecodeCoreHidden, producer.next_applied())? else {if let Some(observer)=&options.public_observer {observer.production_finished()?;} ended=true;let mut buffer=hub.0.0.lock().unwrap();buffer.production=ProductionV1::Ended;hub.0.1.notify_all();continue;};
                #[cfg(feature = "stage-timing")]
                let publish_started = Instant::now();
                #[cfg(feature = "stage-timing")]
                let diagnostic_sample = stages.next_diagnostic_sample();
                count+=1;
                let mut research_public = Vec::new();
                if let Some(observer) = &options.public_observer {
                    for group in &next.applied.closed_groups {
                        let reference = PublicGroupReferenceV1 {
                            input_stream_id: next.applied.applied_prefix.input_stream_id.clone(),
                            applied_prefix: next.applied.applied_prefix.clone(),
                            group_position: group.group_position,
                            group_id: group.group_id.clone(),
                            closed_at_input_position: group.closed_at_input_position,
                        };
                        research_public.push(measure_stage!(stages, ResearchPublicObservation, observer.observe_at(&reference, group, original_public_group_visible_at_utc_ms(&next.input, group)))?);
                    }
                }
                let new_markets = next.applied.closed_groups.iter().flat_map(|g|g.records.iter()).filter_map(|fact| {
                    if let contracts::PublicBusinessPayloadV1::MarketFact { market, payload: contracts::CanonicalMarketFactPayloadV1::FixedIdentity { assets } } = &fact.payload {
                        Some((market, assets))
                    } else { None }
                }).collect::<Vec<_>>();
                if next.initialization_binding.is_some() || !new_markets.is_empty() {
                    #[cfg(feature = "stage-timing")]
                    let catalog_started = stages.start(crate::stage_timing::Stage::ProducerCatalogUpdate);
                    let mut catalog=catalog.write().unwrap();
                    if let Some(binding)=&next.initialization_binding {catalog.initialization.input_positions.insert(next.input.input_position);catalog.initialization.source_message_ids.insert(binding.source_message_id.0.clone());}
                    for (market, assets) in new_markets {
                        catalog.markets.entry(market.condition_id.0.clone()).or_insert(PlatformMarketV1::try_new(market.clone(), assets.clone().try_into().map_err(|_|"原登记市场不是二元市场")?)?);
                    }
                    #[cfg(feature = "stage-timing")]
                    stages.finish(crate::stage_timing::Stage::ProducerCatalogUpdate, catalog_started);
                }
                #[cfg(feature = "stage-timing")]
                let classification_started = stages.start(crate::stage_timing::Stage::ProducerInitializationClassification);
                if next.initialization_binding.is_some() {classification_provenance.input_positions.insert(next.input.input_position);}
                let classifications = next.applied.closed_groups.iter().map(|g| {
                    let initial = classification_provenance.group_is_initialization(&g.member_input_positions);
                    let (observation_dependency, work) = initialization_classifier.classify(&classification_provenance, &g.observations);
                    #[cfg(feature="stage-timing")]
                    stages.add_initialization_work(work.markets,work.source_ids);
                    #[cfg(not(feature="stage-timing"))]
                    let _ = work;
                    let dependency = classification_provenance.group_has_initialization_dependency(&g.member_input_positions,g.closed_at_input_position) || observation_dependency;
                    (initial,dependency)
                }).collect::<Vec<_>>();
                #[cfg(feature="stage-timing")]
                stages.finish(crate::stage_timing::Stage::ProducerInitializationClassification, classification_started);
                #[cfg(feature="stage-timing")]
                let construction_started = diagnostic_sample.then(Instant::now);
                let groups=PreparedPublicGroupV1::from_day(&mut next).into_iter().zip(classifications).enumerate().map(|(index,(delivery, classification))| DayPublicDeliveryV1 {delivery, initialization: config.metadata_initialization.as_ref().map(|_|classification), research_public: research_public.get(index).cloned()}).collect::<Vec<_>>();
                let hidden=next.hidden.into_iter().map(AcceptedHiddenStepBucketV1::from_day).collect::<Vec<_>>();
                // 复用原PM输入大小提示，计容器/数组，不重新序列化或展开完整观察。
                let (public_sizes,hidden_sizes)=if options.producer_payload_bytes.is_some() {
                    let accounting_started=Instant::now();
                    let source_pm_hint=data_pipeline_training::day_producer::retained_pm_input_bytes_hint(&next.input) as u64;
                    let public=groups.iter().map(|group|public_payload_hint(group,source_pm_hint)).collect::<Vec<_>>();
                    let hidden=hidden.iter().map(|bucket|(std::mem::size_of_val(bucket)+std::mem::size_of_val(bucket.records())) as u64).collect::<Vec<_>>();
                    byte_accounting_seconds+=accounting_started.elapsed().as_secs_f64();
                    (public,hidden)
                }else{(Vec::new(),Vec::new())};
                #[cfg(feature = "stage-timing")]
                if let Some(started) = construction_started { stages.record(crate::stage_timing::Stage::ProducerDeliveryConstruction, started.elapsed()); }
                if !groups.is_empty() || hidden.len() != 0 {
                    #[cfg(feature = "stage-timing")]
                    let lock_started = diagnostic_sample.then(Instant::now);
                    let mut buffer=hub.0.0.lock().unwrap();
                    #[cfg(feature = "stage-timing")]
                    if let Some(started) = lock_started { stages.record(crate::stage_timing::Stage::ProducerHubLockWait, started.elapsed()); }
                    #[cfg(feature = "stage-timing")]
                    let held_started = diagnostic_sample.then(Instant::now);
                    let added=public_sizes.iter().chain(hidden_sizes.iter()).fold(0u64,|sum,value|sum.saturating_add(*value));
                    buffer.retained_payload_bytes=buffer.retained_payload_bytes.saturating_add(added);
                    buffer.peak_payload_bytes=buffer.peak_payload_bytes.max(buffer.retained_payload_bytes);
                    buffer.public_payload_sizes.extend(public_sizes);buffer.hidden_payload_sizes.extend(hidden_sizes);
                    if !groups.is_empty() {buffer.public.extend(groups);hub.0.1.public.notify_all();}
                    if hidden.len()!=0 {buffer.hidden.extend(hidden);hub.0.1.hidden.notify_all();}
                    #[cfg(feature = "stage-timing")]
                    if let Some(started) = held_started { stages.record(crate::stage_timing::Stage::ProducerHubPublish, started.elapsed()); }
                }
                #[cfg(feature = "stage-timing")]
                stages.record(crate::stage_timing::Stage::DeliveryAssemblyAndPublishIncludingLock, publish_started.elapsed());
            }
        })).unwrap_or_else(|_|Err("日公共生产异常退出".into()));
        if let Err(error) = &production {
            let mut buffer = hub.0.0.lock().unwrap();
            buffer.production = ProductionV1::Failed(error.to_string());
            if buffer.failure.is_none() {
                buffer.failure = Some(DayFailureSignalV1 {
                    source: DayFailureSourceV1::Producer,
                    message: error.to_string(),
                });
            }
            hub.0.1.notify_all();
        }
        #[cfg(feature = "stage-timing")]
        {
            use std::io::Write;
            // 正常结束、预算停止和失败都保留累计明细；诊断失败不替换业务结果。
            if let Ok(timings) = serde_json::to_string(producer.stage_timings()) {
                let _ = writeln!(std::io::stderr(), "producer_internal_timing={timings}");
            }
            let _ = writeln!(
                std::io::stderr(),
                "producer_depth_state_sizes={}",
                serde_json::json!({"public_applied_inputs":count,
                    "bids_asks_deleted_bids_deleted_asks":producer.core.current_binance_depth_sizes()})
            );
        }
        if let Some(limit) = options.producer_payload_bytes {
            let buffer = hub.0.0.lock().unwrap();
            eprintln!(
                "public_queue_bytes={}",
                serde_json::json!({"target_bytes":limit,"current_logical_payload_bytes":buffer.retained_payload_bytes,"peak_logical_payload_bytes":buffer.peak_payload_bytes,"producer_wait_seconds_including_lock_reacquire":buffer.producer_wait_seconds,"byte_accounting_seconds":byte_accounting_seconds,"definition":"known inline/container capacities plus original PM input byte hint; shared payload charged per queue entry; shared snapshot/book/depth array capacities included without materializing observations; allocator, nested strings/evidence/statistics/opaque payload beyond hint Unknown; soft accounting target, not RSS or hard bound"})
            );
        }
        let mut candidates = Vec::new();
        let mut failures = Vec::new();
        for (index, worker) in workers {
            match worker
                .join()
                .unwrap_or_else(|_| Err("候选线程异常退出".into()))
            {
                Ok(value) => candidates.push(value),
                Err(error) => {
                    let (cause, report) = match error.downcast::<CandidateFailureV1>() {
                        Ok(failure) => (failure.cause, failure.report),
                        Err(error) => (error, None),
                    };
                    let candidate = &day.candidates[index];
                    candidates.push((
                        index,
                        HistoricalDayCandidateResultV1 {
                            run_id: candidate.session.run_id.clone(),
                            comparison_identity_sha256: comparison_identity(
                                candidate,
                                rules.file_sha256(),
                            ),
                            initial_available_pusd: candidate
                                .session
                                .initial_account_state
                                .available_pusd
                                .clone(),
                            outcome: if report.is_some() {
                                CandidateOutcomeV1::CollateralStop
                            } else {
                                CandidateOutcomeV1::NoReport
                            },
                            report: report.unwrap_or(serde_json::Value::Null),
                            error: Some(cause.to_string()),
                        },
                    ));
                    failures.push((index, cause));
                }
            }
        }
        // 所有候选已交回最后的冻结请求；正常、提前结束与错误路径都显式检查尾部。
        let final_output = options
            .public_observer
            .as_ref()
            .map(|o| o.finish())
            .transpose();
        let buffer = hub.0.0.lock().unwrap();
        candidates.sort_by_key(|(index, _)| *index);
        let candidates = candidates.into_iter().map(|(_, value)| value).collect();
        let failure_signal = buffer.failure.clone();
        drop(buffer);
        let mut day_result = HistoricalDayResultV1 {
            input_set_identity_sha256: input_identity,
            public_applied_inputs: count,
            candidates,
        };
        if let Some(signal) = failure_signal {
            let cause = match &signal.source {
                DayFailureSourceV1::Candidate { index, .. } => {
                    for candidate in &mut day_result.candidates {
                        if candidate.run_id == day.candidates[*index].session.run_id
                            && candidate.report != serde_json::Value::Null
                        {
                            candidate.outcome = CandidateOutcomeV1::OriginalFailure;
                        }
                    }
                    failures
                        .into_iter()
                        .find(|(i, _)| i == index)
                        .map(|(_, e)| e)
                        .unwrap_or_else(|| signal.message.clone().into())
                }
                _ => production
                    .err()
                    .unwrap_or_else(|| signal.message.clone().into()),
            };
            return Err(Box::new(HistoricalDaysFailureV1 {
                source: signal.source,
                cause,
                days: vec![day_result],
            }) as MaterialError);
        }
        production?;
        if let Err(cause) = final_output {
            return Err(Box::new(HistoricalDaysFailureV1 {
                source: DayFailureSourceV1::Producer,
                cause,
                days: vec![day_result],
            }) as MaterialError);
        }
        Ok(day_result)
    })
}
fn comparison_identity(candidate: &HistoricalPreparedRunConfigV1, rules: &Sha256Hex) -> Sha256Hex {
    Sha256Hex(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                &candidate.strategy,
                &candidate.timing,
                &candidate.admission_policy,
                &candidate.implementation_identity,
                rules,
                &candidate.entry_market_scope,
                candidate.effective_entry_market_scope.as_ref().map(|e| (
                    i128::from(e.input_end_utc_ms) - i128::from(e.input_start_utc_ms),
                    e.input_start_utc_ms.rem_euclid(86_400_000)
                ))
            ))
            .expect("已构造候选的可序列化比较配置")
        )
    ))
}

#[cfg(test)]
mod entry_identity_tests {
    use super::*;
    #[test]
    fn public_payload_hint_counts_arc_slots_per_item_without_shared_allocation_dedup() {
        let temporary;
        let root = if let Some(parent) = std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT") {
            let root = std::path::PathBuf::from(parent).join("arc-logical-hint");
            std::fs::create_dir_all(&root).unwrap();
            root
        } else {
            temporary = tempfile::tempdir().unwrap();
            temporary.path().to_owned()
        };
        let (config, _) = crate::test_common::setup(&root);
        let prepared = crate::prepare_historical_run_v1(&config.preparation).unwrap();
        let mut source = RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            config.preparation.read_page_rows,
            config.preparation.max_read_batch_bytes,
            crate::timing::DeliveryTimes {
                delays: crate::DelayScheduleV1::Fixed(0),
                last: None,
            },
        )
        .unwrap();
        let delivery = source.next().unwrap().unwrap();
        let mut item = DayPublicDeliveryV1::from(delivery);
        assert!(!item.delivery.prepared.pm_contexts.is_empty());
        let values = serde_json::to_value(&item.delivery.prepared.pm_contexts).unwrap();
        let capacity = item.delivery.prepared.pm_contexts.capacity();
        let old_hint = public_payload_hint(&item, 37);
        let contexts = &mut Arc::get_mut(&mut item.delivery.prepared)
            .expect("正式构造返回的这一准备组尚未交给第二消费者")
            .pm_contexts;
        contexts.reserve(4);
        let extra_slots = contexts.capacity() - capacity;
        assert!(extra_slots > 0);
        let slot_bytes = std::mem::size_of_val(&contexts[0]);
        let new_hint = public_payload_hint(&item, 37);
        assert_eq!(new_hint - old_hint, (extra_slots * slot_bytes) as u64);
        assert_eq!(
            serde_json::to_value(&item.delivery.prepared.pm_contexts).unwrap(),
            values,
            "容器扩容不改变正式上下文成员及值"
        );
        let second_item = item.clone();
        assert!(Arc::ptr_eq(
            &item.delivery.prepared,
            &second_item.delivery.prepared
        ));
        assert_eq!(public_payload_hint(&second_item, 37), new_hint);
        assert_eq!(public_payload_hint(&item, 0) + 37, new_hint);
        let pm_delivery = loop {
            let delivery = source.next().unwrap().expect("正式原件包含PM消息");
            if !delivery.prepared.pm_inputs.is_empty() {
                break delivery;
            }
        };
        let mut item = DayPublicDeliveryV1::from(pm_delivery);
        let prepared = &item.delivery.prepared;
        let original = prepared.pm_inputs[0].1.message();
        let shared = prepared
            .group
            .records
            .iter()
            .find_map(|fact| match &fact.payload {
                PublicBusinessPayloadV1::PolymarketMessage(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(std::ptr::eq(original, shared.as_ref()));
        assert_eq!(original, shared.as_ref());
        let values = serde_json::to_value(&prepared.group).unwrap();
        let capacity = prepared.pm_inputs.capacity();
        let old_hint = public_payload_hint(&item, 37);
        let inputs = &mut Arc::get_mut(&mut item.delivery.prepared).unwrap().pm_inputs;
        inputs.reserve(4);
        let extra_slots = inputs.capacity() - capacity;
        let slot_bytes = std::mem::size_of_val(&inputs[0]);
        assert!(extra_slots > 0);
        assert_eq!(
            public_payload_hint(&item, 37) - old_hint,
            (extra_slots * slot_bytes) as u64,
        );
        assert_eq!(
            serde_json::to_value(&item.delivery.prepared.group).unwrap(),
            values,
        );
        assert_eq!(
            public_payload_hint(&item.clone(), 37),
            public_payload_hint(&item, 37),
        );
    }

    #[test]
    fn identity_distinguishes_policy_and_shape_but_aggregates_dates() {
        let temp = tempfile::tempdir().unwrap();
        let (run, rules) = crate::test_common::setup(temp.path());
        let mut candidate = HistoricalPreparedRunConfigV1::from(&run);
        let old = comparison_identity(&candidate, rules.file_sha256());
        candidate.entry_market_scope = Some(EntryMarketScopeV1::default());
        candidate.effective_entry_market_scope = Some(
            candidate
                .entry_market_scope
                .as_ref()
                .unwrap()
                .expand(1000, 2000)
                .unwrap(),
        );
        let zero = comparison_identity(&candidate, rules.file_sha256());
        assert_ne!(old, zero);
        candidate.effective_entry_market_scope = Some(
            candidate
                .entry_market_scope
                .as_ref()
                .unwrap()
                .expand(86_401_000, 86_402_000)
                .unwrap(),
        );
        assert_eq!(zero, comparison_identity(&candidate, rules.file_sha256()));
        candidate
            .entry_market_scope
            .as_mut()
            .unwrap()
            .start_offset_ms = 5;
        assert_ne!(zero, comparison_identity(&candidate, rules.file_sha256()));
        candidate
            .entry_market_scope
            .as_mut()
            .unwrap()
            .start_offset_ms = 0;
        candidate.effective_entry_market_scope = Some(
            candidate
                .entry_market_scope
                .as_ref()
                .unwrap()
                .expand(1000, 3000)
                .unwrap(),
        );
        assert_ne!(zero, comparison_identity(&candidate, rules.file_sha256()));
    }
}

// 字节提示只计已知布局/容量及既有PM源提示，不做JSON遍历或展开惰性观察。
fn public_payload_hint(item: &DayPublicDeliveryV1, source_pm_hint: u64) -> u64 {
    let prepared = &item.delivery.prepared;
    let group = &prepared.group;
    let mut known = std::mem::size_of_val(item)
        + std::mem::size_of_val(&**prepared)
        + std::mem::size_of_val(&**group)
        + std::mem::size_of_val(&*group.records)
        + group.member_input_positions.capacity() * std::mem::size_of::<u64>()
        + prepared.pm_inputs.capacity()
            * std::mem::size_of::<(u64, data_pipeline_core::pm_reduction::PreparedPmMessageV1)>();
    // 每项逐持有者计Arc指向的已知消息布局；payload动态载荷仍复用原源提示。
    // 不按指针或跨队列项去重，不把此逻辑口径当物理RSS。
    known += prepared
        .pm_inputs
        .iter()
        .map(|(_, pm)| std::mem::size_of_val(pm.message()))
        .sum::<usize>();
    known += group
        .records
        .iter()
        .filter_map(|fact| match &fact.payload {
            PublicBusinessPayloadV1::PolymarketMessage(message) => {
                Some(std::mem::size_of_val(message.as_ref()))
            }
            _ => None,
        })
        .sum::<usize>();
    // 每队列项计Arc槽位和已用上下文的已知布局；共享不作跨项去重，不是RSS。
    known += prepared.pm_contexts.capacity()
        * std::mem::size_of::<
            Arc<data_pipeline_core::pm_reduction::PmMarketContextV1<
                data_pipeline_core::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV3>,
            >>,
        >()
        + prepared
            .pm_contexts
            .iter()
            .map(|context| std::mem::size_of_val(&**context))
            .sum::<usize>();
    if let Some((markets, shared)) = group.observations.shared_parts() {
        known += std::mem::size_of_val(&**shared)
            + markets.len() * std::mem::size_of::<MarketObservationSnapshotV1>();
        for snapshot in markets.values() {
            known += snapshot.assets.capacity() * std::mem::size_of::<AssetBusinessObservationV1>();
            for asset in &snapshot.assets {
                for book in [&asset.bids, &asset.asks] {
                    if let Some(levels) = &book.value {
                        known += levels.capacity() * std::mem::size_of::<PriceLevelV1>();
                    }
                }
            }
        }
        if let Some(depth) = shared
            .binance_depth
            .value
            .as_ref()
            .and_then(BinanceDepthObservationV1::full)
        {
            known += (depth.bids.capacity() + depth.asks.capacity())
                * std::mem::size_of::<ReferencePriceLevelV1>();
        }
    } else {
        known += group.observations.len() * std::mem::size_of::<MarketBusinessObservationV1>();
    }
    (known as u64).saturating_add(source_pm_hint)
}
