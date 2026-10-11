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
use shared_run::CheckpointStrategyV1;
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
    pub checkpoint_root: std::path::PathBuf,
    pub checkpoint_interval_seconds: u64,
    pub progress_interval_seconds: u64,
    /// 可选事件周期，适用于确定性工程验证或明确的日作业配置。
    #[serde(default)]
    pub checkpoint_every_applied_inputs: Option<u64>,
    #[serde(default)]
    pub checkpoint_every_candidate_events: Option<u64>,
    /// 生产领先的软目标；消费者仍需关闭隐藏桶时继续必要供料。
    #[serde(default)]
    pub producer_lead_groups: Option<usize>,
}
#[derive(Debug, Serialize)]
pub struct HistoricalDayCandidateResultV1 {
    pub run_id: String,
    pub comparison_identity_sha256: Sha256Hex,
    pub initial_available_pusd: ExactDecimal,
    pub report: serde_json::Value,
    pub reused: bool,
}
#[derive(Debug, Serialize)]
pub struct HistoricalDayResultV1 {
    pub input_set_identity_sha256: Sha256Hex,
    pub public_applied_inputs: u64,
    pub candidates: Vec<HistoricalDayCandidateResultV1>,
}
/// 每个日期、每个候选在自己的线程内创建原策略和原运行对象。
/// 同日候选只共享不可变公共组和隐藏桶，不共享账户、时钟或反馈。
pub fn run_historical_days_v1<S, F>(
    days: &[HistoricalDayConfigV1],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
) -> Result<Vec<HistoricalDayResultV1>, MaterialError>
where
    S: CheckpointStrategyV1,
    S::Diagnostics: Serialize + serde::de::DeserializeOwned,
    S::Error: std::fmt::Debug,
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
    S: CheckpointStrategyV1,
    S::Diagnostics: Serialize + serde::de::DeserializeOwned,
    S::Error: std::fmt::Debug,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
    let mut results = Vec::new();
    for batch in days.chunks(parallel_dates.max(1)) {
        let values = std::thread::scope(|scope| {
            let handles = batch
                .iter()
                .map(|day| {
                    let rules = rules.clone();
                    scope.spawn(move || run_day::<S, F>(day, rules, factory))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .map_err(|_| -> MaterialError { "日线程异常退出".into() })?
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        results.extend(values);
    }
    Ok(results)
}

fn run_day<S, F>(
    day: &HistoricalDayConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    factory: &F,
) -> Result<HistoricalDayResultV1, MaterialError>
where
    S: CheckpointStrategyV1,
    S::Diagnostics: Serialize + serde::de::DeserializeOwned,
    S::Error: std::fmt::Debug,
    F: Fn(&HistoricalPreparedRunConfigV1) -> Result<S, MaterialError> + Sync,
{
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
    // 恢复绑定业务输入、版本和原持久写入器；调度与进度配置可在重启时调整。
    let mut candidate_identities = day.candidates.clone();
    for candidate in &mut candidate_identities {
        candidate.replay_progress_interval_seconds = 0;
        candidate.max_group_bytes = 0;
        candidate.finite_in_flight_groups = 0;
    }
    let mut identity = Sha256::new();
    identity.update(DAY_CHECKPOINT_FORMAT);
    identity.update(serde_json::to_vec(&(
        config.scope.received_start_utc_ms,
        config.scope.received_end_utc_ms,
        config.effective_header(),
        &config.hidden_stream_id,
        &candidate_identities,
    ))?);
    identity.update(serde_json::to_vec(&(
        input_identity.clone(),
        source_identity.clone(),
        rules.file_sha256(),
    ))?);
    for candidate in &day.candidates {
        identity.update(std::fs::read(&candidate.finality_policy_path)?);
    }
    let identity = format!("{:x}", identity.finalize());
    let restored = read_publication(&day.checkpoint_root, &identity)?;
    if let Some((_, saved)) = &restored {
        if saved.completed.iter().all(Option::is_some)
            && saved.completed.len() == day.candidates.len()
        {
            return Ok(HistoricalDayResultV1 {
                input_set_identity_sha256: input_identity,
                public_applied_inputs: saved.applied,
                candidates: day
                    .candidates
                    .iter()
                    .zip(&saved.completed)
                    .map(|(c, r)| HistoricalDayCandidateResultV1 {
                        run_id: c.session.run_id.clone(),
                        comparison_identity_sha256: comparison_identity(c, rules.file_sha256()),
                        initial_available_pusd: c
                            .session
                            .initial_account_state
                            .available_pusd
                            .clone(),
                        report: r.clone().unwrap(),
                        reused: true,
                    })
                    .collect(),
            });
        }
    }
    let hub = DayHubV1::new(&day.candidates);
    let mut candidates = Vec::new();
    let mut count = 0;
    let mut generation = restored.as_ref().map_or(0, |(p, _)| p.generation + 1);
    if day.checkpoint_root.is_dir() {
        for entry in std::fs::read_dir(&day.checkpoint_root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(number) = name
                .strip_prefix("checkpoint-")
                .filter(|n| n.len() == 20 && n.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|n| n.parse::<u64>().ok())
            {
                generation = generation.max(number.checked_add(1).ok_or("日检查点代数溢出")?);
            }
        }
    }
    let (mut producer, catalog) = if let Some((published, saved)) = restored {
        count = saved.applied;
        let mut buffer = hub.0.0.lock().unwrap();
        let DayCheckpointStateV2::Running {
            core,
            producer: source,
            catalog,
            public_base,
            public,
            hidden_base,
            hidden,
            cursors,
        } = saved.state
        else {
            return Err("完成日结果缺少对应候选报告".into());
        };
        buffer.public_base = public_base;
        buffer.public = public.restore()?.into();
        buffer.hidden_base = hidden_base;
        buffer.hidden = hidden
            .into_iter()
            .map(HiddenBucketCheckpointV1::restore)
            .collect();
        for (i, (cursor, complete)) in cursors.into_iter().zip(saved.completed).enumerate() {
            buffer.candidates[i].cursor = cursor;
            buffer.candidates[i].active = complete.is_none();
            buffer.candidates[i].complete = complete;
            if buffer.candidates[i].active {
                let path = day
                    .checkpoint_root
                    .join(format!("checkpoint-{:020}", published.generation))
                    .join(format!("candidate-{i:06}.cbor"));
                candidates.push(Some(std::fs::read(path)?));
            } else {
                candidates.push(None);
            }
        }
        let core =
            data_pipeline_core::public_observation_core::PublicObservationCoreV1::read_checkpoint(
                core.as_slice(),
            )?;
        let producer = DayProducerV1::restore(
            core,
            &files,
            source.as_slice(),
            config.read_page_rows,
            config.max_read_batch_bytes,
            config.scope.received_start_utc_ms..config.scope.received_end_utc_ms,
        )?;
        (producer, Arc::new(RwLock::new(catalog)))
    } else {
        candidates.resize_with(day.candidates.len(), || None);
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
    producer
        .ingress
        .persist_order_index(&day.checkpoint_root.join("source-order.cbor"))?;
    hub.0.0.lock().unwrap().generation = generation;
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for (index, (candidate, saved)) in day.candidates.iter().zip(candidates).enumerate() {
            if !hub.0.0.lock().unwrap().candidates[index].active {
                continue;
            }
            let context = HistoricalRuntimeContextV1 {
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
                "source_manifests":admitted.iter().map(|d|(d.utc_day(),d.manifest_sha256())).collect::<Vec<_>>(),"rules_file_sha256":rules.file_sha256(),"entry":"original-parquet-day-v1"});
            workers.push(scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || -> Result<_, MaterialError> {
                        let mut strategy = factory(candidate)?;
                        {
                            let mut buffer = hub.0.0.lock().unwrap();
                            buffer.candidates[index].source_health =
                                Some(strategy.uses_source_health());
                            buffer.candidates[index].requirements =
                                Some(strategy.observation_requirements());
                            hub.0.1.notify_all();
                        }
                        let mut saved: Option<CandidateCheckpointV1<S::Diagnostics>> = saved
                            .map(|bytes| ciborium::de::from_reader(bytes.as_slice()))
                            .transpose()?;
                        let prefetched = saved
                            .as_mut()
                            .and_then(|s| s.prefetched.take())
                            .map(|g| g.restore())
                            .transpose()?
                            .into_iter()
                            .collect();
                        let public = DayPublicSourceV1::new(hub.clone(), index, prefetched);
                        let hidden = DayHiddenSourceV1::new(hub.clone(), index);
                        let checkpoint = DayCandidateCheckpointV1::new(
                            hub.clone(),
                            index,
                            day.checkpoint_root.clone(),
                            saved,
                            day.checkpoint_every_candidate_events,
                        );
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
                                checkpoint,
                            ))
                            .map_err(|e| -> MaterialError {
                                format!("候选 {} 失败: {e:?}", candidate.session.run_id).into()
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
                            reused: false,
                        })
                    },
                ))
                .unwrap_or_else(|_| Err("候选线程异常退出".into()));
                hub.finish(index, &result);
                result.map(|value| (index, value))
            }));
        }
        let started = Instant::now();
        let mut progress = Instant::now();
        let mut last_checkpoint = Instant::now();
        let mut last_count = count;
        let lead = day
            .producer_lead_groups
            .unwrap_or(std::thread::available_parallelism()?.get() * 1024);
        let mut ended = false;
        let production=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||->Result<(),MaterialError> {
            loop {
                {
                    let mut buffer=hub.0.0.lock().unwrap();
                    if let Some(error)=&buffer.failure {return Err(error.clone().into());}
                    if day.progress_interval_seconds>0 && progress.elapsed()>=Duration::from_secs(day.progress_interval_seconds) {
                        eprintln!("日作业: phase={}, inputs={count}, active_candidates={}, retained_public_groups={}, retained_hidden_buckets={}, elapsed_seconds={}, total_inputs=unknown",if ended {"consuming"} else if buffer.requested.is_some() {"checkpoint"} else {"producing"},buffer.candidates.iter().filter(|c|c.active).count(),buffer.public.len(),buffer.hidden.len(),started.elapsed().as_secs());progress=Instant::now();
                    }
                    if buffer.candidates.iter().any(|c|c.active&&(!c.runtime_started||c.source_health.is_none()||c.requirements.is_none())) {let _=hub.0.1.wait_timeout(buffer,Duration::from_millis(20)).unwrap();continue;}
                    if buffer.candidates.iter().any(|c|c.source_health==Some(true)) {producer.core.enable_source_progress();}
                    if count == 0 {
                        producer.core.set_observation_requirements(contracts::PublicObservationRequirementsV1 {
                            binance_depth_quantities: buffer.candidates.iter().filter(|c|c.active).any(|c|c.requirements.is_some_and(|r| r.binance_depth_quantities)),
                        });
                    }
                    let complete=buffer.candidates.iter().all(|c|!c.active);
                    let due=(day.checkpoint_interval_seconds>0 && last_checkpoint.elapsed()>=Duration::from_secs(day.checkpoint_interval_seconds))
                        ||day.checkpoint_every_applied_inputs.is_some_and(|n|n>0 && count-last_count>=n);
                    if buffer.requested.is_none() && (due||complete) {buffer.requested=Some(generation);hub.0.1.notify_all();}
                    let ready=buffer.requested.is_some() && buffer.candidates.iter().all(|c|!c.active||c.ready.as_ref().map(|(g,_)|*g)==buffer.requested);
                    if ready {
                        // 前一完整点独立保存；完成后只保留版本、输入、计数和完整报告。
                        let completed=buffer.candidates.iter().map(|c|c.complete.clone()).collect::<Vec<_>>();
                        let catalog = catalog.read().unwrap();
                        let state = if complete {
                            DayCheckpointStateV2::Complete
                        } else {
                            let mut core=Vec::new();producer.core.write_checkpoint(&mut core)?;
                            let mut source=Vec::new();producer.write_checkpoint(&mut source)?;
                            DayCheckpointStateV2::Running {
                                core, producer: source, catalog: &*catalog,
                                public_base:buffer.public_base, public:checkpoint::DeliveryBatch::capture(buffer.public.iter()),
                                hidden_base:buffer.hidden_base, hidden:buffer.hidden.iter().map(HiddenBucketCheckpointV1::capture).collect(),
                                cursors:buffer.candidates.iter().map(|c|c.cursor.clone()).collect(),
                            }
                        };
                        let sha256 = write_cbor(&day.checkpoint_root.join(format!("checkpoint-{generation:020}")).join("day.cbor"),
                            &DayCheckpointV1 { format:DAY_CHECKPOINT_FORMAT.into(),identity:identity.clone(),generation,applied:count,state,completed })?;
                        let mut files = vec![CheckpointFileV1 {path:"day.cbor".into(),sha256}];
                        for (index,candidate) in buffer.candidates.iter().enumerate().filter(|(_,c)|c.active) {
                            files.push(CheckpointFileV1 {path:format!("candidate-{index:06}.cbor"),sha256:candidate.ready.as_ref().unwrap().1.clone()});
                        }
                        publish(&day.checkpoint_root,&identity,generation,files)?;
                        buffer.requested=None;generation+=1;buffer.generation=generation;hub.0.1.notify_all();last_checkpoint=Instant::now();last_count=count;
                        if complete {return Ok(());}
                    }
                    if ended {let _=hub.0.1.wait_timeout(buffer,Duration::from_millis(20)).unwrap();continue;}
                    // 已有可消费材料时让慢候选追上；等待必要完整隐藏桶时继续供料，避免互等。
                    if buffer.public.len()+buffer.hidden.len()>lead && buffer.candidates.iter().any(|c|c.active&&!c.waiting&&(buffer.requested.is_none()||c.ready.as_ref().map(|(g,_)|*g)!=buffer.requested)) {
                        let _=hub.0.1.wait_timeout(buffer,Duration::from_millis(10)).unwrap();continue;
                    }
                }
                let Some(mut next)=producer.next_applied()? else {ended=true;let mut buffer=hub.0.0.lock().unwrap();buffer.production=ProductionV1::Ended;hub.0.1.notify_all();continue;};
                count+=1;
                let new_markets = next.applied.closed_groups.iter().flat_map(|g|g.records.iter()).filter_map(|fact| {
                    if let contracts::PublicBusinessPayloadV1::MarketFact { market, payload: contracts::CanonicalMarketFactPayloadV1::FixedIdentity { assets } } = &fact.payload {
                        Some((market, assets))
                    } else { None }
                }).collect::<Vec<_>>();
                if next.initialization_binding.is_some() || !new_markets.is_empty() {
                    let mut catalog=catalog.write().unwrap();
                    if let Some(binding)=&next.initialization_binding {catalog.initialization.input_positions.insert(next.input.input_position);catalog.initialization.source_message_ids.insert(binding.source_message_id.0.clone());}
                    for (market, assets) in new_markets {
                        catalog.markets.entry(market.condition_id.0.clone()).or_insert(PlatformMarketV1::try_new(market.clone(), assets.clone().try_into().map_err(|_|"原登记市场不是二元市场")?)?);
                    }
                }
                let groups=PreparedPublicGroupV1::from_day(&mut next);
                let hidden=next.hidden.into_iter().map(AcceptedHiddenStepBucketV1::from_day);
                let mut buffer=hub.0.0.lock().unwrap();buffer.public.extend(groups);buffer.hidden.extend(hidden);hub.0.1.notify_all();
            }
        })).unwrap_or_else(|_|Err("日公共生产异常退出".into()));
        if let Err(error) = &production {
            let mut buffer = hub.0.0.lock().unwrap();
            buffer.production = ProductionV1::Failed(error.to_string());
            buffer.failure = Some(error.to_string());
            buffer.requested = None;
            hub.0.1.notify_all();
        }
        let mut candidates = Vec::new();
        for worker in workers {
            candidates.push(
                worker
                    .join()
                    .map_err(|_| -> MaterialError { "候选线程异常退出".into() })??,
            );
        }
        production?;
        let buffer = hub.0.0.lock().unwrap();
        for (index, candidate) in day.candidates.iter().enumerate() {
            if !candidates.iter().any(|(i, _)| *i == index) {
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
                        report: buffer.candidates[index]
                            .complete
                            .clone()
                            .ok_or("恢复的完成记录缺失")?,
                        reused: true,
                    },
                ));
            }
        }
        candidates.sort_by_key(|(index, _)| *index);
        let candidates = candidates.into_iter().map(|(_, value)| value).collect();
        Ok(HistoricalDayResultV1 {
            input_set_identity_sha256: input_identity,
            public_applied_inputs: count,
            candidates,
        })
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
                rules
            ))
            .expect("已构造候选的可序列化比较配置")
        )
    ))
}
