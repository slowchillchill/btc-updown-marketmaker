//! 原日配置的薄装配；不读取或准备另一份行情格式。
use crate::{
    ConsumerConfig, ModelConsumer,
    worker::{ModelWorker, WorkerConfig},
};
use backtest::*;
use contracts::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sim_system::platform_rules::*;
use std::time::Instant;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerRunConfig {
    pub worker: WorkerConfig,
    pub rules: crate::ConsumerRulesV1,
    pub days: Vec<HistoricalDayConfigV1>,
    pub parallel_dates: usize,
}
pub fn run_consumer(config: &ConsumerRunConfig) -> Result<Value, MaterialError> {
    run_consumer_with_opener(config, crate::rules::open_consumer_rules)
}
pub(crate) fn run_consumer_with_opener(
    config: &ConsumerRunConfig,
    open: impl FnOnce(&ConsumerRunConfig) -> Result<crate::rules::OpenedRulesV1, MaterialError>,
) -> Result<Value, MaterialError> {
    let first_observation = std::sync::Arc::new(crate::first_observation::FirstObservation::new(
        Instant::now(),
    ));
    let current = crate::public_input::bind(config);
    let config = &current;
    let started = Instant::now();
    for day in &config.days {
        for candidate in &day.candidates {
            if candidate.session.caller_role != SimCallerRoleV1::ModelTrain {
                return Err("本入口须显式ModelTrain调用身份".into());
            }
            if candidate.timing.decision_mode != HistoricalDecisionModeV1::Fixed100msFifo {
                return Err("新B消费者须显式fixed100ms_fifo".into());
            }
            if candidate.public_statistics.is_none() {
                return Err("模型消费者需要显式公共统计完整配置".into());
            }
        }
    }
    let opened = open(config)?;
    let effective = crate::rules::bind_consumer(config, &opened.binding);
    let worker = ModelWorker::start(config.worker.clone())?;
    let mut summary =
        run_with_worker_observed(&effective, worker, opened.rules, started, first_observation)?;
    summary["rules_opened_via"] = serde_json::to_value(opened.opened_via)?;
    Ok(summary)
}
pub fn run_with_worker(
    config: &ConsumerRunConfig,
    worker: ModelWorker,
    rules: std::sync::Arc<SealedProcessRuleSnapshotV3>,
    started: Instant,
) -> Result<Value, MaterialError> {
    let first_observation =
        std::sync::Arc::new(crate::first_observation::FirstObservation::new(started));
    run_with_worker_observed(config, worker, rules, started, first_observation)
}
pub(crate) fn run_with_worker_observed(
    config: &ConsumerRunConfig,
    worker: ModelWorker,
    rules: std::sync::Arc<SealedProcessRuleSnapshotV3>,
    started: Instant,
    first_observation: std::sync::Arc<crate::first_observation::FirstObservation>,
) -> Result<Value, MaterialError> {
    let canonical;
    let config = if config.rules.frozen().is_some() {
        config
    } else {
        canonical = crate::rules::bind_consumer(
            config,
            &crate::FrozenRuleBindingV1::from_sealed(&rules, rules.access_mode()),
        );
        &canonical
    };
    let current = crate::public_input::bind(config);
    let config = &current;
    let before = ProcessCountersV1::read().ok();
    let mut days = config.days.clone();
    for day in &mut days {
        for candidate in &mut day.candidates {
            // B默认声明短窗完整范围，不继承A/旧B首末扣除。
            candidate
                .entry_market_scope
                .get_or_insert_with(EntryMarketScopeV1::default);
        }
    }
    let result = run_historical_days_with_parallelism_v1(
        &days,
        rules.clone(),
        &|c| {
            let consumer: ConsumerConfig =
                serde_json::from_value(c.strategy.effective_parameters["model_consumer"].clone())?;
            ModelConsumer::new(
                consumer,
                worker.clone(),
                rules.clone(),
                c.effective_entry_market_scope
                    .clone()
                    .ok_or("原day未展开声明市场范围")?,
                config.rules.frozen().ok_or("缺已解析规则绑定")?,
            )
            .map(|consumer| consumer.with_first_observation(first_observation.clone()))
            .map_err(Into::into)
        },
        config.parallel_dates,
    )?;
    let metrics = worker.request(json!({"phase":"metrics"}))?;
    let elapsed = started.elapsed().as_secs_f64();
    Ok(
        json!({"first_observation":first_observation.snapshot(),"consumer_version":"b-model-consumer-v1","effective_config":config,"days":result,"model":metrics,"worker_ipc":worker.io_metrics(),"elapsed_seconds":elapsed,"calls_per_second":metrics["forwards"].as_u64().unwrap_or(0) as f64/elapsed,"rust_process":process_measurement_v1(before.as_ref(),ProcessCountersV1::read().ok().as_ref(),process_clock_ticks_per_second_v1(),elapsed),"limits":"仅接线/推理，无学习或容量结论"}),
    )
}
