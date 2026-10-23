mod inventory_research;
mod selected_strategy;
mod streaming_research;
use backtest::*;
use baseline_mm::strategies::complementary_pair::*;
use baseline_mm::strategies::continuous_inventory::*;
use selected_strategy::SelectedStrategy;
use serde::{Deserialize, Serialize};
use sim_system::platform_rules::*;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum StrategyParameters {
    Continuous(ContinuousInventoryConfigV1),
    LowPrice(ComplementaryPairConfigV5),
    Public(ComplementaryPairConfigV4),
    Legacy(ComplementaryPairConfigV3),
}
impl StrategyParameters {
    fn version(&self) -> &'static str {
        match self {
            Self::Continuous(_) => CONTINUOUS_INVENTORY_RULE_VERSION,
            Self::LowPrice(_) => LOW_PRICE_RULE_VERSION,
            Self::Public(_) => PUBLIC_RULE_VERSION,
            Self::Legacy(_) => RULE_VERSION,
        }
    }
    fn identity(&self) -> &'static str {
        match self {
            Self::Continuous(_) => CONTINUOUS_INVENTORY_STRATEGY_ID,
            _ => STRATEGY_ID,
        }
    }
    fn build(
        self,
        rules: std::sync::Arc<SealedProcessRuleSnapshotV3>,
        namespace: String,
    ) -> Result<SelectedStrategy, StrategyErrorV1> {
        match self {
            Self::Continuous(config) => {
                let continuity =
                    ContinuousInventoryContinuityV1::new_account(config.initial_cash_pusd);
                ContinuousInventoryV1::new(config, rules, namespace, continuity)
                    .map(SelectedStrategy::Inventory)
            }
            Self::LowPrice(config) => ComplementaryPairV1::new_low_price(
                config,
                rules,
                namespace,
                StrategyContinuityV2::new_low_price_account(),
            )
            .map(SelectedStrategy::Legacy),
            Self::Public(config) => ComplementaryPairV1::new_public(
                config,
                rules,
                namespace,
                StrategyContinuityV2::new_public_account(),
            )
            .map(SelectedStrategy::Legacy),
            Self::Legacy(config) => ComplementaryPairV1::new(
                config,
                rules,
                namespace,
                StrategyContinuityV2::new_account(),
            )
            .map(SelectedStrategy::Legacy),
        }
    }
}
fn require_parameter_version(
    run: &serde_json::Value,
    parameters: &StrategyParameters,
) -> Result<(), CommandError> {
    if let Some(identity) = run.get("strategy").and_then(|s| s.get("strategy_id")) {
        if identity.as_str() != Some(parameters.identity()) {
            return Err("策略身份与实际参数类型不同，不能重标旧结果".into());
        }
    }
    if let Some(version) = run
        .get("strategy")
        .and_then(|s| s.get("strategy_rule_version"))
    {
        if version.as_str() != Some(parameters.version()) {
            return Err("策略参数类型与声明的正式版本不同".into());
        }
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandConfig<R = HistoricalRunConfigV1> {
    run: R,
    rules: HistoricalProcessSnapshotRequestConfig,
    strategy: StrategyParameters,
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
    strategy: StrategyParameters,
    request_namespace: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DayCommand {
    preparation: HistoricalPreparationConfigV1,
    candidates: Vec<DayCandidateCommand>,
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
fn require_current_rule(run: &serde_json::Value) -> Result<(), CommandError> {
    if let Some(version) = run
        .get("strategy")
        .and_then(|s| s.get("strategy_rule_version"))
    {
        if !matches!(
            version.as_str(),
            Some(
                RULE_VERSION
                    | PUBLIC_RULE_VERSION
                    | LOW_PRICE_RULE_VERSION
                    | CONTINUOUS_INVENTORY_RULE_VERSION
            )
        ) {
            return Err(format!("策略版本{version}不受当前入口支持；请生成准确版本材料，不能覆盖旧candidate_v1/candidate_v2历史包").into());
        }
    }
    Ok(())
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
            require_current_rule(candidate.get("run").ok_or("候选缺少run")?)?;
            if let Some(overrides) = &overrides {
                let strategy = candidate
                    .get_mut("strategy")
                    .and_then(serde_json::Value::as_object_mut)
                    .ok_or("候选缺少完整strategy")?;
                for (section, fields) in overrides.as_object().ok_or("策略覆盖不是对象")? {
                    let inventory = strategy.contains_key("probability_reference")
                        || strategy
                            .keys()
                            .any(|k| INVENTORY_PARAMETERS.contains(&k.as_str()));
                    let new_field = section == "probability_reference"
                        || INVENTORY_PARAMETERS.contains(&section.as_str());
                    let old_field = matches!(
                        section.as_str(),
                        "quote"
                            | "probability"
                            | "risk"
                            | "blb_decrease_veto"
                            | "initial_pair_low_price_veto"
                    );
                    // 混合对照的全局覆盖只作用于该字段所属候选，公共 health/repair 仍作用于双方。
                    if (new_field && !inventory) || (old_field && inventory) {
                        continue;
                    }
                    if new_field && !fields.is_object() {
                        strategy.insert(section.clone(), fields.clone());
                        continue;
                    }

                    if matches!(
                        section.as_str(),
                        "blb_decrease_veto" | "initial_pair_low_price_veto"
                    ) {
                        strategy.insert(section.clone(), fields.clone());
                        continue;
                    }
                    let strategy = if strategy.contains_key("parameters") {
                        strategy
                            .get_mut("parameters")
                            .and_then(serde_json::Value::as_object_mut)
                            .ok_or("公共策略parameters不是对象")?
                    } else {
                        &mut *strategy
                    };
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
            let strategy: StrategyParameters = serde_json::from_value(
                candidate
                    .get("strategy")
                    .cloned()
                    .ok_or("候选缺少完整strategy")?,
            )?;
            require_parameter_version(candidate.get("run").ok_or("候选缺少run")?, &strategy)?;
            candidate.get_mut("run").and_then(serde_json::Value::as_object_mut).ok_or("候选缺少宿主run")?
                .insert("strategy".into(),serde_json::json!({"strategy_id":strategy.identity(),"strategy_rule_version":strategy.version(),"effective_parameters":strategy}));
        }
    }
    Ok(serde_json::from_value(value)?)
}
#[derive(Clone, Copy)]
enum ParameterType {
    PriceReference,
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
            Self::PriceReference => Ok(serde_json::to_value(serde_json::from_value::<
                InitialPairPriceReferenceV1,
            >(
                serde_json::Value::String(raw.into()),
            )?)?),
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
const INVENTORY_PARAMETERS: &[&str] = &[
    "capacity_shares",
    "inventory_skew_ticks_per_capacity",
    "initial_cash_pusd",
    "market_investment_limit_pusd",
    "per_market_orphan_limit_pusd",
    "account_orphan_limit_pusd",
    "drawdown_fraction",
];
const PARAMETERS: &[(&str, ParameterType)] = &[
    ("capacity_shares", ParameterType::Decimal),
    ("inventory_skew_ticks_per_capacity", ParameterType::Decimal),
    ("initial_cash_pusd", ParameterType::Decimal),
    ("market_investment_limit_pusd", ParameterType::Decimal),
    ("per_market_orphan_limit_pusd", ParameterType::Decimal),
    ("account_orphan_limit_pusd", ParameterType::Decimal),
    ("drawdown_fraction", ParameterType::Decimal),
    (
        "probability_reference.volatility_window_ms",
        ParameterType::U64,
    ),
    (
        "probability_reference.initial_sigma_usd_per_sqrt_second",
        ParameterType::F64,
    ),
    (
        "probability_reference.estimation_error_usd",
        ParameterType::F64,
    ),
    (
        "probability_reference.binance_innovation_weight",
        ParameterType::F64,
    ),
    (
        "probability_reference.binance_pressure_weight_usd",
        ParameterType::F64,
    ),
    (
        "probability_reference.binance_trade_pressure_weight_usd",
        ParameterType::F64,
    ),
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
    (
        "quote.initial_pair_price_reference",
        ParameterType::PriceReference,
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
    let mut blb_override = None;
    let mut low_price_override = None;
    let mut entry_overrides = BTreeMap::<String, u64>::new();
    while let Some(option) = args.next() {
        if option == "--initial-pair-low-price-veto" {
            low_price_override = Some(
                args.next()
                    .ok_or("初始批低价否决缺少显式布尔值")?
                    .parse::<bool>()?,
            );
            continue;
        }
        if option == "--blb-decrease-veto" {
            blb_override = Some(
                args.next()
                    .ok_or("BLB否决缺少显式布尔值")?
                    .parse::<bool>()?,
            );
            continue;
        }
        if option == "--config" {
            config_file = Some(args.next().ok_or("--config 缺少文件路径")?);
            continue;
        }
        if let Some(field) = match option.as_str() {
            "--entry-market-start-offset-ms" => Some("start_offset_ms"),
            "--entry-market-end-margin-ms" => Some("end_margin_ms"),
            _ => None,
        } {
            entry_overrides.insert(
                field.into(),
                args.next().ok_or("开仓范围缺少毫秒值")?.parse()?,
            );
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
    if let Some(veto) = blb_override {
        value
            .as_object_mut()
            .ok_or("启动配置不是对象")?
            .entry("strategy")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("策略配置不是对象")?
            .insert("blb_decrease_veto".into(), veto.into());
    }
    if let Some(veto) = low_price_override {
        value
            .as_object_mut()
            .ok_or("启动配置不是对象")?
            .entry("strategy")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("策略配置不是对象")?
            .insert("initial_pair_low_price_veto".into(), veto.into());
    }
    if !entry_overrides.is_empty() {
        apply_entry_overrides(&mut value, &entry_overrides)?;
    }
    for (name, (kind, raw)) in overrides {
        let path = name.split_once('.');
        let strategy = value
            .as_object_mut()
            .ok_or("启动配置不是对象")?
            .entry("strategy")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("策略配置不是对象")?;
        let strategy = if strategy.contains_key("parameters") {
            strategy
                .get_mut("parameters")
                .and_then(serde_json::Value::as_object_mut)
                .ok_or("公共策略parameters不是对象")?
        } else {
            strategy
        };
        let Some((section, field)) = path else {
            strategy.insert(name, kind.parse(&raw)?);
            continue;
        };
        let section = strategy
            .entry(section)
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("策略区段不是对象")?;
        section.insert(field.into(), kind.parse(&raw)?);
    }
    Ok(value)
}
fn apply_entry_overrides(
    value: &mut serde_json::Value,
    overrides: &BTreeMap<String, u64>,
) -> Result<(), CommandError> {
    fn merge(
        run: &mut serde_json::Value,
        values: &BTreeMap<String, u64>,
    ) -> Result<(), CommandError> {
        let run = run.as_object_mut().ok_or("候选run不是对象")?;
        let scope = run
            .entry("entry_market_scope")
            .or_insert_with(|| serde_json::json!({}));
        if scope.is_null() {
            *scope = serde_json::json!({});
        }
        let scope = scope.as_object_mut().ok_or("开仓范围不是对象")?;
        for (name, value) in values {
            scope.insert(name.clone(), (*value).into());
        }
        Ok(())
    }
    if let Some(days) = value.get_mut("days").and_then(|v| v.as_array_mut()) {
        for day in days {
            for candidate in day
                .get_mut("candidates")
                .and_then(|v| v.as_array_mut())
                .ok_or("日期缺少候选")?
            {
                merge(candidate.get_mut("run").ok_or("候选缺少run")?, overrides)?;
            }
        }
    } else {
        merge(
            value.get_mut("run").ok_or("开仓范围只适用于运行配置")?,
            overrides,
        )?;
    }
    Ok(())
}
fn load_config<R: serde::de::DeserializeOwned>(
    args: impl IntoIterator<Item = String>,
) -> Result<CommandConfig<R>, CommandError> {
    let mut value = load_value(args)?;
    require_current_rule(value.get("run").ok_or("配置缺少run")?)?;
    let strategy: StrategyParameters =
        serde_json::from_value(value.get("strategy").cloned().ok_or("配置缺少策略参数")?)?;
    require_parameter_version(value.get("run").ok_or("配置缺少run")?, &strategy)?;
    let descriptor = StrategyDescriptorV1 {
        strategy_id: strategy.identity().into(),
        strategy_rule_version: strategy.version().into(),
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
    evidence_root: Option<PathBuf>,
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
            "--research-evidence-root" => {
                result.evidence_root = Some(args.next().ok_or("研究证据缺少输出目录")?.into())
            }
            "--wall-budget-seconds" => {
                result.budget_seconds = Some(args.next().ok_or("墙钟预算缺值")?.parse()?)
            }
            "--summary-reserve-seconds" => {
                result.reserve_seconds = Some(args.next().ok_or("汇总预留缺值")?.parse()?)
            }
            _ => remaining.push(arg),
        }
    }
    if let Some(budget) = result.budget_seconds {
        if budget <= result.reserve_seconds.unwrap_or(900) {
            return Err("墙钟预算须大于汇总输出预留".into());
        }
    }
    Ok((result, remaining))
}
fn write_day_failure_json(value: &serde_json::Value) -> std::io::Result<()> {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    serde_json::to_writer(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if std::env::args().any(|arg| arg == "--help") {
        println!(
            "公共历史回测：--mode run（默认兼容原prepare+run）|prepare|reuse|day\nprepare: --config <HistoricalPreparationConfigV1 JSON/TOML>\nreuse: --prepared <prepared-run-v1.json> --config <独立运行 JSON/TOML>\nday: --config <日期及独立候选 JSON/TOML>；直接原Parquet供料，每次从头运行，不保存或恢复检查点\n--progress-interval-seconds <秒>（默认30，0关闭）\nday可显式启用--research-summary、--wall-budget-seconds <秒>、--summary-reserve-seconds <秒>（默认900）\nday可选--entry-market-start-offset-ms <非负毫秒>、--entry-market-end-margin-ms <非负毫秒>：声明输入[S,E)上的S+a≤market.start<E-b；无配置不限制，显式0/0限制；单端沿文件补值后缺值取0。\nday可选--research-evidence-root <新目录>，同次公共生产输出BLB/PM与真实决策，失败返回部分结果。\n公共候选 candidate_v4_public_c0_c1 使用parameters内完整42参数及blb_decrease_veto；--blb-decrease-veto true|false覆盖新候选唯一开关。\n低价候选 candidate_v5_public_low_price 使用parameters内原42参数和initial_pair_low_price_veto；--initial-pair-low-price-veto true|false覆盖唯一开关，固定沿C0不叠BLB。\n连续库存 candidate_v1_continuous_inventory 使用独立完整参数，strategy_id=continuous_inventory；同一day可与candidate_v5_public_low_price/off独立账户对照。新参数含capacity_shares、inventory_skew_ticks_per_capacity、初始现金/投入/风险/回撤、probability_reference及原health/repair；不恢复或重标旧记忆。\n全部策略字段可覆盖（混合day按字段所属候选覆盖，health/repair同时作用）：\n{}",
            PARAMETERS
                .iter()
                .map(|(field, _)| *field)
                .collect::<Vec<_>>()
                .join("\n")
        );
        return;
    }
    let day_requested = std::env::args()
        .collect::<Vec<_>>()
        .windows(2)
        .any(|args| args == ["--mode", "day"]);
    let mut day_failure_json_attempted = false;
    let outcome = async {
        let started = std::time::Instant::now();
        let (research, args) = research_args(std::env::args().skip(1))?;
        let (mode, interval, description, args) = mode_args(args)?;
        if mode != Mode::Day && (research.summary || research.budget_seconds.is_some() || research.reserve_seconds.is_some() || research.evidence_root.is_some()) {
            return Err("研究汇总及预算选项仅适用于day入口".into());
        }
        // 外层硬截止覆盖输入准备、运行及最后输出；汇总预留用于返回已有成绩。
        let _budget_guard = research.budget_seconds.map(|budget| {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            std::thread::spawn(move || {
                let remaining=std::time::Duration::from_secs(budget).saturating_sub(started.elapsed());
                if matches!(rx.recv_timeout(remaining), Err(std::sync::mpsc::RecvTimeoutError::Timeout)) {
                    eprintln!("正式研究墙钟预算硬截止；未返回的成绩和阶段计时为未知，本次不可续跑");
                    std::process::exit(124);
                }
            });
            tx
        });
        if mode==Mode::Day {
            let command=load_days_command(load_value(args)?)?;
            let rules=load_rules(command.rules)?;
            let inventory_comparison = command.days.iter().flat_map(|day| &day.candidates).any(|candidate| matches!(candidate.strategy,StrategyParameters::Continuous(_)));
            let observer:Option<std::sync::Arc<dyn HistoricalPublicObserverV1>> = research.evidence_root.as_ref().map(|root| ->Result<_,MaterialError> {
                if inventory_comparison {Ok(std::sync::Arc::new(inventory_research::InventoryResearchV1::open(root)?) as std::sync::Arc<dyn HistoricalPublicObserverV1>)}
                else {Ok(std::sync::Arc::new(streaming_research::StreamingResearchV1::open(root)?) as std::sync::Arc<dyn HistoricalPublicObserverV1>)}
            }).transpose()?;
            let mut days=Vec::new();
            for day in command.days {
                let mut candidates=Vec::new();
                for candidate in day.candidates {
                    let mut run=candidate.run;
                    run.strategy=StrategyDescriptorV1 {strategy_id:candidate.strategy.identity().into(),strategy_rule_version:candidate.strategy.version().into(),effective_parameters:serde_json::to_value(&candidate.strategy)?};
                    run.effective_entry_market_scope=run.entry_market_scope.as_ref().map(|p| p.expand(day.preparation.scope.received_start_utc_ms,day.preparation.scope.received_end_utc_ms)).transpose()?;
                    if let Some(root) = &research.evidence_root {
                        run.research_evidence_path=Some(root.join(format!("candidate-{}-{}.jsonl",days.len(),candidates.len())));
                    }
                    run.replay_progress_interval_seconds=interval;
                    run.strategy_request_namespace=Some(candidate.request_namespace);
                    candidates.push(run);
                }
                days.push(HistoricalDayConfigV1 {preparation:day.preparation,candidates,progress_interval_seconds:interval,producer_lead_groups:day.producer_lead_groups});
            }
            let factory=|run:&HistoricalPreparedRunConfigV1|->Result<SelectedStrategy,MaterialError> {
                let strategy: StrategyParameters=serde_json::from_value(run.strategy.effective_parameters.clone())?;
                let namespace=run.strategy_request_namespace.as_ref().ok_or("候选请求命名空间缺失")?.clone();
                Ok(strategy.build(rules.clone(),namespace)?.with_entry_market_range(run.effective_entry_market_scope.as_ref().map(|e| EntryMarketRangeV1 { start_at_utc_ms:e.market_start_inclusive_utc_ms,end_at_utc_ms:e.market_start_exclusive_utc_ms })))
            };
            let options=HistoricalDayRunOptionsV1 { research_summary:research.summary, started, public_observer:observer,
                budget_stop_after:research.budget_seconds.map(|budget| std::time::Duration::from_secs(budget-research.reserve_seconds.unwrap_or(900))) };
            let result=if research.summary || research.budget_seconds.is_some() || research.evidence_root.is_some() {
                run_historical_days_with_options_v1(&days,rules.clone(),&factory,command.parallel_dates.unwrap_or(1),&options)
            } else if let Some(parallel)=command.parallel_dates {run_historical_days_with_parallelism_v1(&days,rules.clone(),&factory,parallel)}
                else {run_historical_days_v1(&days,rules.clone(),&factory)};
            let results = match result {
                Ok(results)=>results,
                Err(error)=>{
                    let value=if let Some(failure)=error.downcast_ref::<HistoricalDaysFailureV1>() {
                        serde_json::json!({"status":"failed","error":failure.cause.to_string(),"failure_source":failure.source,"days":failure.days})
                    } else {serde_json::json!({"status":"failed","error":error.to_string(),"days":[]})};
                    day_failure_json_attempted=true;
                    if let Err(output)=write_day_failure_json(&value) {eprintln!("失败部分报告输出错误: {output}");}
                    return Err(error);
                }
            };
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
            let path = description.ok_or("reuse模式必须提供--prepared描述路径")?;
            let material = PreparedHistoricalMaterialV1::read(&path)?;
            config.run.effective_entry_market_scope = config.run.entry_market_scope.as_ref().map(|p| p.expand(material.preparation.scope.received_start_utc_ms,material.preparation.scope.received_end_utc_ms)).transpose()?;
            let mut strategy = config.strategy.build(rules.clone(), config.request_namespace)?.with_entry_market_range(config.run.effective_entry_market_scope.as_ref().map(entry_range));
            let started = std::time::Instant::now();
            let output_ok = interval != 0 && emit_stage(&serde_json::json!({"stage":"opening_prepared_run", "processed_groups":null, "elapsed_seconds":started.elapsed().as_secs_f64(), "total":null, "eta":null}));
            let result = run_prepared_historical_v1(&path, &config.run, rules, &mut strategy).await;
            if output_ok { emit_stage(&serde_json::json!({"stage":"prepared_run_ended", "success":result.is_ok(), "elapsed_seconds":started.elapsed().as_secs_f64(), "eta":null})); }
            return print_result(result);
        }
        let config: CommandConfig = load_config(args)?;
        let rules = load_rules(config.rules)?;
        let scope = config.run.entry_market_scope.as_ref().map(|p| p.expand(config.run.preparation.scope.received_start_utc_ms,config.run.preparation.scope.received_end_utc_ms)).transpose()?;
        let mut strategy = config.strategy.build(rules.clone(), config.request_namespace)?.with_entry_market_range(scope.as_ref().map(entry_range));
        print_result(run_historical_v1(&config.run, rules, &mut strategy).await)
    }.await;
    if let Err(error) = outcome as Result<(), CommandError> {
        if day_requested && !day_failure_json_attempted {
            if let Err(output) = write_day_failure_json(
                &serde_json::json!({"status":"failed","error":error.to_string(),"days":[]}),
            ) {
                eprintln!("失败结果输出错误: {output}");
            }
        }
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
fn entry_range(e: &EffectiveEntryMarketScopeV1) -> EntryMarketRangeV1 {
    EntryMarketRangeV1 {
        start_at_utc_ms: e.market_start_inclusive_utc_ms,
        end_at_utc_ms: e.market_start_exclusive_utc_ms,
    }
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
        let config = ComplementaryPairConfigV3::parse_toml(include_str!(
            "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering-v3.toml"
        ))
        .unwrap();
        let value = serde_json::to_value(config).unwrap();
        let inventory = serde_json::to_value(
            ContinuousInventoryConfigV1::parse_toml(include_str!(
                "../../../baseline-mm/configs/strategies/continuous_inventory/adopted-v1.toml"
            ))
            .unwrap(),
        )
        .unwrap();
        let actual = [&value, &inventory]
            .into_iter()
            .flat_map(|config| {
                config
                    .as_object()
                    .unwrap()
                    .iter()
                    .flat_map(|(section, fields)| {
                        if let Some(fields) = fields.as_object() {
                            fields
                                .keys()
                                .map(|field| format!("{section}.{field}"))
                                .collect::<Vec<_>>()
                        } else {
                            vec![section.clone()]
                        }
                    })
            })
            .collect::<std::collections::BTreeSet<_>>();
        let supported = PARAMETERS
            .iter()
            .map(|(field, _)| (*field).to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, supported);
        for (name, kind) in PARAMETERS {
            let pointer = format!("/{}", name.replace('.', "/"));
            let original = value
                .pointer(&pointer)
                .or_else(|| inventory.pointer(&pointer))
                .expect("已枚举实际字段");
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
    fn explicit_budget_preserves_original_args_and_requires_summary_reserve() {
        let (r, args) = research_args(
            [
                "--research-summary",
                "--wall-budget-seconds",
                "25200",
                "--summary-reserve-seconds",
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

#[cfg(test)]
mod entry_scope_tests {
    use super::*;
    #[test]
    fn cli_single_end_preserves_file_other_end_and_unconfigured_is_distinct() {
        let mut v = serde_json::json!({"days":[{"candidates":[{"run":{"entry_market_scope":{"start_offset_ms":13,"end_margin_ms":27}}},{"run":{}}]}]});
        apply_entry_overrides(&mut v, &BTreeMap::from([("start_offset_ms".into(), 0)])).unwrap();
        let runs = &v["days"][0]["candidates"];
        let a: EntryMarketScopeV1 =
            serde_json::from_value(runs[0]["run"]["entry_market_scope"].clone()).unwrap();
        let b: EntryMarketScopeV1 =
            serde_json::from_value(runs[1]["run"]["entry_market_scope"].clone()).unwrap();
        assert_eq!((a.start_offset_ms, a.end_margin_ms), (0, 27));
        assert_eq!((b.start_offset_ms, b.end_margin_ms), (0, 0));
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), r#"{"days":[{"candidates":[{"run":{}}]}]}"#).unwrap();
        let absent = load_value(["--config".into(), file.path().display().to_string()]).unwrap();
        assert!(
            absent["days"][0]["candidates"][0]["run"]
                .get("entry_market_scope")
                .is_none()
        );
        let explicit = load_value([
            "--config".into(),
            file.path().display().to_string(),
            "--entry-market-end-margin-ms".into(),
            "0".into(),
        ])
        .unwrap();
        assert!(explicit["days"][0]["candidates"][0]["run"]["entry_market_scope"].is_object());
    }
}

#[cfg(test)]
mod public_candidate_tests {
    use super::*;
    #[test]
    fn public_and_legacy_parameter_versions_and_switch_override() {
        let file = tempfile::NamedTempFile::new().unwrap();
        // 与生产入口相同：先加载完整 JSON Value，再解释具体策略；保留原 f64 数值。
        std::fs::write(
            file.path(),
            format!(
                "{{\"strategy\":{}}}",
                include_str!(
                    "../../../baseline-mm/configs/strategies/complementary_pair/public-c0.json"
                )
            ),
        )
        .unwrap();
        let value = load_value(["--config".into(), file.path().display().to_string()]).unwrap();
        let public: StrategyParameters = serde_json::from_value(value["strategy"].clone()).unwrap();
        assert_eq!(public.version(), PUBLIC_RULE_VERSION);
        let StrategyParameters::Public(c) = public else {
            panic!()
        };
        let legacy: StrategyParameters =
            serde_json::from_value(serde_json::to_value(&c.parameters).unwrap()).unwrap();
        assert_eq!(legacy.version(), RULE_VERSION);
        assert!(
            require_parameter_version(
                &serde_json::json!({"strategy":{"strategy_rule_version":RULE_VERSION}}),
                &StrategyParameters::Public(c.clone())
            )
            .is_err()
        );
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            serde_json::to_vec(&serde_json::json!({"strategy":c})).unwrap(),
        )
        .unwrap();
        let value = load_value([
            "--config".into(),
            file.path().display().to_string(),
            "--blb-decrease-veto".into(),
            "true".into(),
            "--strategy.quote.base_pair_shares".into(),
            "12".into(),
        ])
        .unwrap();
        let parsed: ComplementaryPairConfigV4 =
            serde_json::from_value(value["strategy"].clone()).unwrap();
        assert!(parsed.blb_decrease_veto);
        assert_eq!(
            parsed.parameters.quote.base_pair_shares,
            rust_decimal::Decimal::from(12)
        );
    }
}

#[cfg(test)]
mod low_price_candidate_tests {
    use super::*;
    #[test]
    fn low_price_config_roundtrip_versions_and_explicit_cli_override() {
        let c: ComplementaryPairConfigV5 = serde_json::from_str(include_str!(
            "../../../baseline-mm/configs/strategies/complementary_pair/public-low-price-off.json"
        ))
        .unwrap();
        let parsed: StrategyParameters =
            serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        assert_eq!(parsed.version(), LOW_PRICE_RULE_VERSION);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            serde_json::to_vec(&serde_json::json!({"strategy":c})).unwrap(),
        )
        .unwrap();
        let value = load_value([
            "--config".into(),
            file.path().display().to_string(),
            "--initial-pair-low-price-veto".into(),
            "true".into(),
            "--strategy.quote.base_pair_shares".into(),
            "12".into(),
        ])
        .unwrap();
        let parsed: ComplementaryPairConfigV5 =
            serde_json::from_value(value["strategy"].clone()).unwrap();
        assert!(parsed.initial_pair_low_price_veto);
        assert_eq!(
            parsed.parameters.quote.base_pair_shares,
            rust_decimal::Decimal::from(12)
        );
        let roundtrip: ComplementaryPairConfigV5 =
            serde_json::from_str(&serde_json::to_string(&parsed).unwrap()).unwrap();
        assert_eq!(roundtrip.parameters, parsed.parameters);
        assert_eq!(
            roundtrip.initial_pair_low_price_veto,
            parsed.initial_pair_low_price_veto
        );
        assert!(
            require_parameter_version(
                &serde_json::json!({"strategy":{"strategy_rule_version":PUBLIC_RULE_VERSION}}),
                &StrategyParameters::LowPrice(parsed)
            )
            .is_err()
        );
    }
}
