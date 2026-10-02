use backtest::*;
use baseline_mm::strategies::complementary_pair::*;
use serde::{Deserialize, Serialize};
use sim_system::platform_rules::*;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandConfig {
    run: HistoricalRunConfigV1,
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
fn load_config(
    args: impl IntoIterator<Item = String>,
) -> Result<CommandConfig, Box<dyn std::error::Error + Send + Sync>> {
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
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if std::env::args().any(|arg| arg == "--help") {
        println!(
            "单次公共历史回测：backtest-cli --config <启动 JSON/TOML> [--strategy.<字段> <显式值>]\n无热重载。全部策略字段可覆盖：\n{}",
            PARAMETERS
                .iter()
                .map(|(field, _)| *field)
                .collect::<Vec<_>>()
                .join("\n")
        );
        return;
    }
    let outcome = async {
        let mut config = load_config(std::env::args().skip(1))?;
        let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: config.rules.snapshot_path,
            expected_file_sha256: config.rules.expected_file_sha256,
            replay_causal_start_utc_ms: config.rules.replay_causal_start_utc_ms,
        })?;
        config.run.strategy = StrategyDescriptorV1 {
            strategy_id: STRATEGY_ID.into(),
            strategy_rule_version: RULE_VERSION.into(),
            effective_parameters: serde_json::to_value(&config.strategy)?,
        };
        let mut strategy = ComplementaryPairV1::new(
            config.strategy,
            rules.clone(),
            config.request_namespace,
            StrategyContinuityV1::new_account(),
        )?;
        match run_historical_v1(&config.run, rules, &mut strategy).await {
            Ok(report) => {
                println!("{}", serde_json::to_string(&report)?);
                Ok(())
            }
            Err(failure) => Err(format!("历史运行失败：{:?}", failure).into()),
        }
    }
    .await;
    if let Err(error) = outcome as Result<(), Box<dyn std::error::Error + Send + Sync>> {
        eprintln!("{error}");
        std::process::exit(1);
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
