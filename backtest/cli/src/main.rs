use backtest::*;
use baseline_mm::strategies::complementary_pair::*;
use serde::{Deserialize, Serialize};
use sim_system::platform_rules::*;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandConfig<R = HistoricalRunConfigV1> {
    run: R,
    rules: HistoricalProcessSnapshotRequestConfig,
    strategy: ComplementaryPairConfigV1,
    request_namespace: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalProcessSnapshotRequestConfig {
    snapshot_path: PathBuf,
    expected_file_sha256: contracts::Sha256Hex,
    replay_causal_start_utc_ms: i64,
}
type CommandError = Box<dyn std::error::Error + Send + Sync>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DayCandidateCommand {
    run: HistoricalPreparedRunConfigV1,
    strategy: ComplementaryPairConfigV1,
    request_namespace: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DayCommand {
    preparation: HistoricalPreparationConfigV1,
    candidates: Vec<DayCandidateCommand>,
    checkpoint_root: PathBuf,
    checkpoint_interval_seconds: u64,
    #[serde(default)]
    checkpoint_every_applied_inputs: Option<u64>,
    #[serde(default)]
    checkpoint_every_candidate_events: Option<u64>,
    #[serde(default)]
    producer_lead_groups: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DaysCommand {
    rules: HistoricalProcessSnapshotRequestConfig,
    days: Vec<DayCommand>,
    #[serde(default)]
    parallel_dates: Option<usize>,
}
fn load_days_command(mut value: serde_json::Value) -> Result<DaysCommand, CommandError> {
    let overrides = value
        .as_object_mut()
        .ok_or("日配置不是对象")?
        .remove("strategy");
    let days = value
        .get_mut("days")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("日配置缺少days")?;
    for day in days {
        for candidate in day
            .get_mut("candidates")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or("日期缺少candidates")?
        {
            if let Some(overrides) = &overrides {
                let strategy = candidate
                    .get_mut("strategy")
                    .and_then(serde_json::Value::as_object_mut)
                    .ok_or("候选缺少完整strategy")?;
                for (section, fields) in overrides.as_object().ok_or("策略覆盖不是对象")? {
                    let target = strategy
                        .get_mut(section)
                        .and_then(serde_json::Value::as_object_mut)
                        .ok_or("完整策略缺少覆盖区段")?;
                    for (field, value) in fields.as_object().ok_or("策略覆盖区段不是对象")?
                    {
                        target.insert(field.clone(), value.clone());
                    }
                }
            }
            let strategy: ComplementaryPairConfigV1 = serde_json::from_value(
                candidate
                    .get("strategy")
                    .cloned()
                    .ok_or("候选缺少完整strategy")?,
            )?;
            candidate.get_mut("run").and_then(serde_json::Value::as_object_mut).ok_or("候选缺少宿主run")?
                .insert("strategy".into(),serde_json::json!({"strategy_id":STRATEGY_ID,"strategy_rule_version":RULE_VERSION,"effective_parameters":strategy}));
        }
    }
    Ok(serde_json::from_value(value)?)
}
#[derive(Clone, Copy)]
enum ParameterType {
    Decimal,
    U64,
    F64,
    Weights,
    Bool,
}
impl ParameterType {
    fn parse(self, raw: &str) -> Result<serde_json::Value, CommandError> {
        fn json<T: serde::de::DeserializeOwned + Serialize>(
            raw: &str,
        ) -> Result<serde_json::Value, CommandError> {
            Ok(serde_json::to_value(serde_json::from_str::<T>(raw)?)?)
        }
        match self {
            Self::Decimal => Ok(serde_json::Value::String(
                raw.parse::<rust_decimal::Decimal>()?.to_string(),
            )),
            Self::U64 => json::<u64>(raw),
            Self::F64 => json::<f64>(raw),
            Self::Weights => json::<[f64; 4]>(raw),
            Self::Bool => json::<bool>(raw),
        }
    }
}
// 原策略字段类型决定覆盖解析，不依赖文件中的占位值或复制策略默认。
const PARAMETERS: &[(&str, ParameterType)] = &[
    ("health.polymarket_silence_normal_ms", ParameterType::U64),
    ("health.polymarket_silence_ms", ParameterType::U64),
    (
        "health.polymarket_event_delay_normal_ms",
        ParameterType::U64,
    ),
    ("health.polymarket_event_delay_ms", ParameterType::U64),
    ("health.chainlink_silence_normal_ms", ParameterType::U64),
    ("health.chainlink_silence_ms", ParameterType::U64),
    ("health.chainlink_event_delay_normal_ms", ParameterType::U64),
    ("health.chainlink_event_delay_ms", ParameterType::U64),
    ("health.binance_trade_silence_normal_ms", ParameterType::U64),
    ("health.binance_trade_silence_ms", ParameterType::U64),
    (
        "health.binance_trade_event_delay_normal_ms",
        ParameterType::U64,
    ),
    ("health.binance_trade_event_delay_ms", ParameterType::U64),
    ("health.binance_depth_silence_normal_ms", ParameterType::U64),
    ("health.binance_depth_silence_ms", ParameterType::U64),
    (
        "health.binance_depth_event_delay_normal_ms",
        ParameterType::U64,
    ),
    ("health.binance_depth_event_delay_ms", ParameterType::U64),
    ("probability.volatility_window_ms", ParameterType::U64),
    (
        "probability.initial_sigma_usd_per_sqrt_second",
        ParameterType::F64,
    ),
    ("probability.estimation_error_usd", ParameterType::F64),
    ("probability.binance_innovation_weight", ParameterType::F64),
    (
        "probability.binance_pressure_weight_usd",
        ParameterType::F64,
    ),
    (
        "probability.binance_trade_pressure_weight_usd",
        ParameterType::F64,
    ),
    ("quote.base_pair_shares", ParameterType::Decimal),
    ("quote.base_half_spread", ParameterType::Decimal),
    ("quote.inventory_shift_per_pusd", ParameterType::Decimal),
    ("quote.uncertainty_spread_weight", ParameterType::Decimal),
    ("quote.signal_spread_weight", ParameterType::Decimal),
    ("quote.joint_fill_scenario_weights", ParameterType::Weights),
    (
        "quote.minimum_business_result_per_share",
        ParameterType::Decimal,
    ),
    ("quote.execution_path_ms", ParameterType::U64),
    (
        "repair.base_loss_allowance_per_share",
        ParameterType::Decimal,
    ),
    (
        "repair.time_loss_allowance_per_share",
        ParameterType::Decimal,
    ),
    (
        "repair.inventory_loss_allowance_per_share",
        ParameterType::Decimal,
    ),
    ("repair.maximum_overfill_shares", ParameterType::Decimal),
    ("repair.maximum_overfill_pusd", ParameterType::Decimal),
    ("repair.maker_fill_weight", ParameterType::F64),
    (
        "repair.wait_price_improvement_per_share",
        ParameterType::Decimal,
    ),
    ("repair.residual_risk_weight", ParameterType::Decimal),
    ("repair.maker_path_ms", ParameterType::U64),
    ("repair.active_path_ms", ParameterType::U64),
    ("repair.enable_active_repair", ParameterType::Bool),
    ("risk.per_market_orphan_limit_pusd", ParameterType::Decimal),
    ("risk.account_orphan_limit_pusd", ParameterType::Decimal),
];
fn load_value(args: impl IntoIterator<Item = String>) -> Result<serde_json::Value, CommandError> {
    let mut args = args.into_iter();
    let mut config_file = None;
    let mut overrides = BTreeMap::new();
    while let Some(option) = args.next() {
        if option == "--config" {
            config_file = Some(args.next().ok_or("--config 缺少文件路径")?);
            continue;
        }
        let name = option
            .strip_prefix("--strategy.")
            .ok_or("未知命令参数；使用 --config 和 --strategy.<字段> <值>")?;
        let kind = PARAMETERS
            .iter()
            .find(|(field, _)| *field == name)
            .map(|(_, kind)| *kind)
            .ok_or_else(|| format!("未知策略参数 {name}"))?;
        overrides.insert(
            name.to_owned(),
            (kind, args.next().ok_or("策略覆盖缺少显式值")?),
        );
    }
    let content =
        std::fs::read_to_string(config_file.ok_or("必须通过 --config 显式指定启动配置")?)?;
    let mut value: serde_json::Value = if content.trim_start().starts_with('{') {
        serde_json::from_str(&content)?
    } else {
        serde_json::to_value(toml::from_str::<toml::Value>(&content)?)?
    };
    for (name, (kind, raw)) in overrides {
        let (section, field) = name.split_once('.').expect("固定参数路径");
        let strategy = value
            .as_object_mut()
            .ok_or("启动配置不是对象")?
            .entry("strategy")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("策略配置不是对象")?;
        let section = strategy
            .entry(section)
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("策略区段不是对象")?;
        section.insert(field.into(), kind.parse(&raw)?);
    }
    Ok(value)
}
fn load_config<R: serde::de::DeserializeOwned>(
    args: impl IntoIterator<Item = String>,
) -> Result<CommandConfig<R>, CommandError> {
    let mut value = load_value(args)?;
    let strategy: ComplementaryPairConfigV1 =
        serde_json::from_value(value.get("strategy").cloned().ok_or("配置缺少策略参数")?)?;
    let descriptor = StrategyDescriptorV1 {
        strategy_id: STRATEGY_ID.into(),
        strategy_rule_version: RULE_VERSION.into(),
        effective_parameters: serde_json::to_value(&strategy)?,
    };
    value
        .get_mut("run")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("配置缺少公共运行区段")?
        .insert("strategy".into(), serde_json::to_value(descriptor)?);
    // 策略自身没有正式数值默认：不复制默认值，缺失必填字段沿原类型报错。
    Ok(serde_json::from_value(value)?)
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Run,
    Prepare,
    Reuse,
    Day,
}
fn mode_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(Mode, u64, Option<PathBuf>, Vec<String>), CommandError> {
    let mut args = args.into_iter();
    let mut mode = Mode::Run;
    let mut interval = 30;
    let mut description = None;
    let mut remaining = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = match args.next().as_deref() {
                    Some("run") => Mode::Run,
                    Some("prepare") => Mode::Prepare,
                    Some("reuse") => Mode::Reuse,
                    Some("day") => Mode::Day,
                    _ => return Err("--mode 必须为run、prepare、reuse或day".into()),
                }
            }
            "--prepared" => {
                description = Some(args.next().ok_or("--prepared 缺少描述路径")?.into())
            }
            "--progress-interval-seconds" => {
                interval = args.next().ok_or("进度间隔缺值")?.parse()?
            }
            _ => remaining.push(arg),
        }
    }
    Ok((mode, interval, description, remaining))
}
#[derive(Default)]
struct ResearchOptions {
    summary: bool,
    budget_seconds: Option<u64>,
    reserve_seconds: Option<u64>,
}
fn research_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(ResearchOptions, Vec<String>), CommandError> {
    let mut result = ResearchOptions::default();
    let mut remaining = vec![];
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--research-summary" => result.summary = true,
            "--wall-budget-seconds" => {
                result.budget_seconds = Some(args.next().ok_or("墙钟预算缺值")?.parse()?)
            }
            "--checkpoint-reserve-seconds" => {
                result.reserve_seconds = Some(args.next().ok_or("保存预留缺值")?.parse()?)
            }
            _ => remaining.push(arg),
        }
    }
    if let Some(budget) = result.budget_seconds {
        if budget <= result.reserve_seconds.unwrap_or(900) {
            return Err("墙钟预算须大于保存/汇总预留".into());
        }
    }
    Ok((result, remaining))
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if std::env::args().any(|arg| arg == "--help") {
        println!(
            "公共历史回测：--mode run（默认兼容原prepare+run）|prepare|reuse|day\nprepare: --config <HistoricalPreparationConfigV1 JSON/TOML>\nreuse: --prepared <prepared-run-v1.json> --config <独立运行 JSON/TOML>\nday: --config <日期及独立候选 JSON/TOML>；直接原Parquet供料，checkpoint_root存在时恢复，完成点复用\n--progress-interval-seconds <秒>（默认30，0关闭）\nday可显式启用--research-summary、--wall-budget-seconds <秒>、--checkpoint-reserve-seconds <秒>（默认900）\n全部策略字段可覆盖（day对各候选显式覆盖）：\n{}",
            PARAMETERS
                .iter()
                .map(|(field, _)| *field)
                .collect::<Vec<_>>()
                .join("\n")
        );
        return;
    }
    let outcome = async {
        let started = std::time::Instant::now();
        let (research, args) = research_args(std::env::args().skip(1))?;
        let (mode, interval, description, args) = mode_args(args)?;
        if mode != Mode::Day && (research.summary || research.budget_seconds.is_some() || research.reserve_seconds.is_some()) {
            return Err("研究汇总及预算选项仅适用于day入口".into());
        }
        // 外层硬截止覆盖输入准备、运行及最后输出；只保留最后成功发布点。
        let _budget_guard = research.budget_seconds.map(|budget| {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            std::thread::spawn(move || {
                let remaining=std::time::Duration::from_secs(budget).saturating_sub(started.elapsed());
                if matches!(rx.recv_timeout(remaining), Err(std::sync::mpsc::RecvTimeoutError::Timeout)) {
                    eprintln!("正式研究墙钟预算硬截止；仅承认最后成功发布检查点，未输出阶段计时为未知");
                    std::process::exit(124);
                }
            });
            tx
        });
        if mode==Mode::Day {
            let command=load_days_command(load_value(args)?)?;
            let rules=load_rules(command.rules)?;
            let mut days=Vec::new();
            for day in command.days {
                let mut candidates=Vec::new();
                for candidate in day.candidates {
                    let mut run=candidate.run;
                    run.strategy=StrategyDescriptorV1 {strategy_id:STRATEGY_ID.into(),strategy_rule_version:RULE_VERSION.into(),effective_parameters:serde_json::to_value(&candidate.strategy)?};
                    run.replay_progress_interval_seconds=interval;
                    run.strategy_request_namespace=Some(candidate.request_namespace);
                    candidates.push(run);
                }
                days.push(HistoricalDayConfigV1 {preparation:day.preparation,candidates,checkpoint_root:day.checkpoint_root,
                    checkpoint_interval_seconds:day.checkpoint_interval_seconds,progress_interval_seconds:interval,
                    checkpoint_every_applied_inputs:day.checkpoint_every_applied_inputs,checkpoint_every_candidate_events:day.checkpoint_every_candidate_events,producer_lead_groups:day.producer_lead_groups});
            }
            let factory=|run:&HistoricalPreparedRunConfigV1|->Result<ComplementaryPairV1,MaterialError> {
                let strategy=serde_json::from_value(run.strategy.effective_parameters.clone())?;
                let namespace=run.strategy_request_namespace.as_ref().ok_or("候选请求命名空间缺失")?.clone();
                Ok(ComplementaryPairV1::new(strategy,rules.clone(),namespace,StrategyContinuityV1::new_account())?)
            };
            let options=HistoricalDayRunOptionsV1 { research_summary:research.summary, started,
                budget_save_after:research.budget_seconds.map(|budget| std::time::Duration::from_secs(budget-research.reserve_seconds.unwrap_or(900))) };
            let results=if research.summary || research.budget_seconds.is_some() {
                run_historical_days_with_options_v1(&days,rules.clone(),&factory,command.parallel_dates.unwrap_or(1),&options)?
            } else if let Some(parallel)=command.parallel_dates {run_historical_days_with_parallelism_v1(&days,rules.clone(),&factory,parallel)?}
                else {run_historical_days_v1(&days,rules.clone(),&factory)?};
            // 连续六小时研究不日化；保留整段和小时原结果，旧默认日汇总仍保持。
            if research.summary {
                println!("{}",serde_json::json!({"days":results,"window_summary":"各候选research.final_snapshot及hours；非完整账户日，不日化"})); return Ok(());
            }
            let summaries=summarize_independent_historical_days_v1(&results)?;
            println!("{}",serde_json::json!({"days":results,"summaries":summaries}));return Ok(());
        }
        if mode == Mode::Prepare {
            let config: HistoricalPreparationConfigV1 = serde_json::from_value(load_value(args)?)?;
            let path = prepare_and_publish_historical_run_v1(&config,
                Some(HistoricalPreparationProgressV1::new(std::time::Duration::from_secs(interval), std::io::stderr())))?;
            println!("{}", serde_json::json!({"prepared_description": path}));
            return Ok(());
        }
        if mode == Mode::Reuse {
            let mut config: CommandConfig<HistoricalPreparedRunConfigV1> = load_config(args)?;
            config.run.replay_progress_interval_seconds = interval;
            let rules = load_rules(config.rules)?;
            let mut strategy = ComplementaryPairV1::new(config.strategy, rules.clone(), config.request_namespace, StrategyContinuityV1::new_account())?;
            let path = description.ok_or("reuse模式必须提供--prepared描述路径")?;
            let started = std::time::Instant::now();
            let output_ok = interval != 0 && emit_stage(&serde_json::json!({"stage":"opening_prepared_run", "processed_groups":null, "elapsed_seconds":started.elapsed().as_secs_f64(), "total":null, "eta":null}));
            let result = run_prepared_historical_v1(&path, &config.run, rules, &mut strategy).await;
            if output_ok { emit_stage(&serde_json::json!({"stage":"prepared_run_ended", "success":result.is_ok(), "elapsed_seconds":started.elapsed().as_secs_f64(), "eta":null})); }
            return print_result(result);
        }
        let config: CommandConfig = load_config(args)?;
        let rules = load_rules(config.rules)?;
        let mut strategy = ComplementaryPairV1::new(config.strategy, rules.clone(), config.request_namespace, StrategyContinuityV1::new_account())?;
        print_result(run_historical_v1(&config.run, rules, &mut strategy).await)
    }.await;
    if let Err(error) = outcome as Result<(), CommandError> {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
// 阶段输出仅诊断，故障停该输出，不替代业务结果。
fn emit_stage(value: &serde_json::Value) -> bool {
    use std::io::Write;
    let mut output = std::io::stderr().lock();
    serde_json::to_writer(&mut output, value).is_ok() && writeln!(output).is_ok()
}
fn load_rules(
    config: HistoricalProcessSnapshotRequestConfig,
) -> Result<std::sync::Arc<SealedProcessRuleSnapshotV3>, CommandError> {
    Ok(load_historical_process_snapshot_v3(
        HistoricalProcessSnapshotRequestV1 {
            snapshot_path: config.snapshot_path,
            expected_file_sha256: config.expected_file_sha256,
            replay_causal_start_utc_ms: config.replay_causal_start_utc_ms,
        },
    )?)
}
fn print_result<E: std::fmt::Debug>(
    result: Result<HistoricalRunReportV1, HistoricalRunFailureV1<E>>,
) -> Result<(), CommandError> {
    match result {
        Ok(report) => {
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Err(failure) => Err(format!("历史运行失败：{:?}", failure).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cli_parameters_cover_actual_strategy_fields() {
        let config = ComplementaryPairConfigV1::parse_toml(include_str!(
            "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
        ))
        .unwrap();
        let value = serde_json::to_value(config).unwrap();
        let actual = value
            .as_object()
            .unwrap()
            .iter()
            .flat_map(|(section, fields)| {
                fields
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(move |field| format!("{section}.{field}"))
            })
            .collect::<std::collections::BTreeSet<_>>();
        let supported = PARAMETERS
            .iter()
            .map(|(field, _)| (*field).to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, supported);
        for (name, kind) in PARAMETERS {
            let (section, field) = name.split_once('.').unwrap();
            let original = &value[section][field];
            let raw = original
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| original.to_string());
            assert_eq!(
                kind.parse(&raw).unwrap(),
                *original,
                "{name} 类型应沿原策略序列化"
            );
        }
    }
}

#[cfg(test)]
mod research_option_tests {
    use super::*;
    #[test]
    fn explicit_budget_preserves_original_args_and_requires_save_reserve() {
        let (r, args) = research_args(
            [
                "--research-summary",
                "--wall-budget-seconds",
                "25200",
                "--checkpoint-reserve-seconds",
                "900",
                "--mode",
                "day",
                "--config",
                "fixture.json",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert!(r.summary);
        assert_eq!(r.budget_seconds, Some(25200));
        assert_eq!(r.reserve_seconds, Some(900));
        assert_eq!(args, ["--mode", "day", "--config", "fixture.json"]);
        assert!(research_args(["--wall-budget-seconds", "900"].map(str::to_owned)).is_err());
        let (r, args) = research_args(["--mode", "day"].map(str::to_owned)).unwrap();
        assert!(!r.summary && r.budget_seconds.is_none());
        assert_eq!(args, ["--mode", "day"]);
    }
}
