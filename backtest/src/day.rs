//! 原文件日作业与独立候选装配；公共 Core 只由当天生产线程拥有。
use crate::day_state::*;
use crate::{
    run::{HistoricalRuntimeCatalogV1, HistoricalRuntimeContextV1, run_context_with_hook},
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
}
impl Default for HistoricalDayRunOptionsV1 {
    fn default() -> Self {
        Self {
            research_summary: false,
            public_observer: None,
            started: Instant::now(),
            budget_stop_after: None,
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
    let mut results = Vec::new();
    for batch in days.chunks(parallel_dates.max(1)) {
        let values = std::thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|day| {
                    let rules = rules.clone();
                    scope.spawn(move || run_day::<S, F>(day, rules, factory, options))
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
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for (index, candidate) in day.candidates.iter().enumerate() {
            let context = HistoricalRuntimeContextV1 {
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
                "source_manifests":admitted.iter().map(|d|(d.utc_day(),d.manifest_sha256())).collect::<Vec<_>>(),"rules_file_sha256":rules.file_sha256(),"maker_model_version":rules.maker_model_version_for(contracts::SimCallerRoleV1::HistoricalBacktest),"entry":"original-parquet-day-v1"});
            workers.push((
                index,
                scope.spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> Result<_, MaterialError> {
                            let mut strategy = factory(candidate)?;
                            {
                                let mut buffer = hub.0.0.lock().unwrap();
                                buffer.candidates[index].source_health =
                                    Some(strategy.uses_source_health());
                                buffer.candidates[index].requirements =
                                    Some(strategy.observation_requirements());
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
        let production=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||->Result<(),MaterialError> {
            loop {
                {
                    let buffer=hub.0.0.lock().unwrap();
                    if let Some(error)=&buffer.failure {return Err(error.message.clone().into());}
                    if day.progress_interval_seconds>0 && progress.elapsed()>=Duration::from_secs(day.progress_interval_seconds) {
                        eprintln!("日作业: phase={}, inputs={count}, active_candidates={}, retained_public_groups={}, retained_hidden_buckets={}, elapsed_seconds={}, total_inputs=unknown",if ended {"consuming"} else {"producing"},buffer.candidates.iter().filter(|c|c.active).count(),buffer.public.len(),buffer.hidden.len(),started.elapsed().as_secs());progress=Instant::now();
                    }
                    if buffer.candidates.iter().any(|c|c.active&&(!c.runtime_started||c.source_health.is_none()||c.requirements.is_none())) {let _=measure_stage!(stages, ProducerWaitStartupOrCompletion, hub.0.1.control.wait_timeout(buffer,Duration::from_millis(20))).unwrap();continue;}
                    if options.public_observer.is_some() || buffer.candidates.iter().any(|c|c.source_health==Some(true)) {producer.core.enable_source_progress();}
                    if count == 0 {
                        producer.core.set_observation_requirements(contracts::PublicObservationRequirementsV1 {
                            binance_depth_quantities: options.public_observer.as_ref().is_some_and(|o| o.requirements().binance_depth_quantities) || buffer.candidates.iter().filter(|c|c.active).any(|c|c.requirements.is_some_and(|r| r.binance_depth_quantities)),
                        });
                    }
                    if buffer.candidates.iter().all(|c| !c.active) {
                        drop(buffer);
                        if !ended { if let Some(observer)=&options.public_observer {observer.finish()?;} }
                        return Ok(());
                    }
                    if !hub.0.1.budget_stop_requested.load(std::sync::atomic::Ordering::Acquire) && options.budget_stop_after.is_some_and(|after| options.started.elapsed() >= after) {
                        hub.0.1.budget_stop_requested.store(true, std::sync::atomic::Ordering::Release);
                        hub.0.1.notify_all();
                    }
                    if ended {let _=measure_stage!(stages, ProducerWaitStartupOrCompletion, hub.0.1.control.wait_timeout(buffer,Duration::from_millis(20))).unwrap();continue;}
                    // 已有可消费材料时让慢候选追上；等待必要完整隐藏桶时继续供料，避免互等。
                    if buffer.public.len()+buffer.hidden.len()>lead && buffer.candidates.iter().any(|c|c.active&&!c.waiting) {
                        let _=measure_stage!(stages, ProducerBackpressureWait, hub.0.1.control.wait_timeout(buffer,Duration::from_millis(10))).unwrap();continue;
                    }
                }
                let Some(mut next)=measure_stage!(stages, NextAppliedIncludingDecodeCoreHidden, producer.next_applied())? else {if let Some(observer) = &options.public_observer { observer.finish()?; } ended=true;let mut buffer=hub.0.0.lock().unwrap();buffer.production=ProductionV1::Ended;hub.0.1.notify_all();continue;};
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
                        research_public.push(observer.observe(&reference, group)?);
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
                let hidden=next.hidden.into_iter().map(AcceptedHiddenStepBucketV1::from_day);
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
