//! 模型边界只投影必要公开值，不归约行情或另存经济状态。
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use shared_run::StrategyInputV1;

pub const ENCODING_VERSION: &str = "b-public-token-log1p-v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncodingConfig {
    pub field_buckets: u32,
    pub category_buckets: u32,
}
#[derive(Clone, Debug, Serialize)]
pub struct Token(pub u32, pub u32, pub f32, pub u8);
fn bucket(s: &str, count: u32) -> u32 {
    // 稳定FNV-1a；哈希投影仅供神经编码，不决定动作资格、份额或资金域。
    let mut h = 0xcbf29ce484222325u64;
    for b in s.bytes() {
        h = (h ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
    (h % u64::from(count)) as u32
}
fn flatten(v: &Value, path: &str, c: &EncodingConfig, out: &mut Vec<Token>) {
    match v {
        Value::Object(m) => {
            for (k, v) in m {
                flatten(v, &format!("{path}/{k}"), c, out);
            }
        }
        Value::Array(a) => {
            out.push(Token(
                bucket(&format!("{path}/length"), c.field_buckets),
                0,
                (a.len() as f32).ln_1p(),
                0,
            ));
            for (i, v) in a.iter().enumerate() {
                flatten(v, &format!("{path}/{i}"), c, out);
            }
        }
        _ => {
            let numeric = match v {
                Value::Number(n) => n.as_f64(),
                Value::String(s) => s.parse::<f64>().ok(),
                _ => None,
            };
            let (category, value, kind) = if let Some(n) = numeric.filter(|n| n.is_finite()) {
                (0, (n.signum() * n.abs().ln_1p()) as f32, 0)
            } else {
                match v {
                    Value::Null => (0, 0.0, 1),
                    Value::Bool(b) => (0, f32::from(*b), 2),
                    Value::String(s) => (bucket(s, c.category_buckets), 0.0, 3),
                    _ => unreachable!(),
                }
            };
            out.push(Token(bucket(path, c.field_buckets), category, value, kind));
        }
    }
}
pub fn tokens(v: &Value, c: &EncodingConfig) -> Vec<Token> {
    let mut out = Vec::new();
    flatten(v, "", c, &mut out);
    out
}
pub fn market_projection(m: &shared_run::FrozenMarketReadV1, at: i64) -> Value {
    let o = &m.observation;
    // F0价格/深度/参考、来源质量和证据直接引用；排除原组事件载荷和全历史。
    let mut f0 = json!({"market":o.market,"assets":o.assets,"lifecycle":o.lifecycle,"active":o.active,"closed":o.closed,"accepting_orders":o.accepting_orders,"terminal":o.terminal,"official_anchor":o.official_anchor,"settlement_reference":o.settlement_reference,"temporary_reference":o.temporary_reference,"effective_reference":o.effective_reference,"chainlink_spot":o.chainlink_spot,"chainlink_twap_thirty":o.chainlink_twap_thirty,"chainlink_twap_sixty":o.chainlink_twap_sixty,"binance_depth":o.binance_depth,"binance_trade":o.binance_trade,"source_progress":o.source_progress,"rules":m.rule_context});
    // 仅投影年龄，不将旧证据重标为新消息。
    fn ages(v: &mut Value, at: i64) {
        match v {
            Value::Object(m) => {
                let age = m
                    .get("received_at_utc_ms")
                    .and_then(Value::as_i64)
                    .map(|t| at - t);
                for child in m.values_mut() {
                    ages(child, at);
                }
                if let Some(age) = age {
                    m.insert("model_received_age_ms".into(), age.into());
                }
            }
            Value::Array(a) => {
                for child in a {
                    ages(child, at);
                }
            }
            _ => {}
        }
    }
    ages(&mut f0, at);
    let statistics=m.statistics.as_ref().map(|s|json!({"definition_version":s.definition_version,"config":s.config,"applied_at_utc_ms":s.applied_at_utc_ms,"observed_at_utc_ms":s.observed_at_utc_ms,"since_market_start_ms":s.since_market_start_ms,"until_market_end_ms":s.until_market_end_ms,"depth":s.depth,"blb":s.blb,"momentum":s.momentum,"spot_variance":s.spot_variance,"spot_integrals":s.spot_integrals,"latest_trade_pressure":s.latest_trade_pressure,"basis":s.basis,"pm_quantities":s.pm_quantities,"source_applied_at_utc_ms":s.source_applied_at_utc_ms,"source_received_age_ms":s.source_received_age_ms}));
    json!({"f0":f0,"f1_f9":statistics})
}
pub fn time_projection(input: &StrategyInputV1, at: i64) -> Value {
    let market = input
        .market_frame_timing
        .as_ref()
        .map(|t| t.market_cutoff.decision_time().logical_time_utc_ms());
    let earliest = input
        .market_frame_timing
        .as_ref()
        .map(|t| t.earliest_start_at_utc_ms);
    json!({"market_cutoff_ms":market,"earliest_start_ms":earliest,"processing_start_ms":at,"account_cutoff_ms":input.action_before.cutoff().decision_time().logical_time_utc_ms(),"frame_age_ms":market.map(|t|at-t),"queue_wait_ms":earliest.map(|t|at-t),"pending_noe_record_count":input.action_before.pending_noe_record_count()})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zero_null_and_not_materialized_remain_distinct() {
        let c = EncodingConfig {
            field_buckets: 4096,
            category_buckets: 4096,
        };
        assert_ne!(
            serde_json::to_value(tokens(&json!({"x":"0"}), &c)).unwrap(),
            serde_json::to_value(tokens(&json!({"x":null}), &c)).unwrap()
        );
        assert_ne!(
            serde_json::to_value(tokens(&json!({"x":"NotMaterialized"}), &c)).unwrap(),
            serde_json::to_value(tokens(&json!({"x":"Unavailable"}), &c)).unwrap()
        );
    }
}
