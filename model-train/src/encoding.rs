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
/// 借用根子值并覆盖本次public；同类型Map保留原插入/替换遍历语义。
/// Null/非对象继续走原索引赋值，保留原构造和panic边界。
pub(crate) fn tokens_with_public(
    private: &Value,
    public: &Value,
    c: &EncodingConfig,
) -> Vec<Token> {
    let Value::Object(values) = private else {
        let mut projection = private.clone();
        projection["public"] = public.clone();
        return tokens(&projection, c);
    };
    let mut keys = serde_json::Map::new();
    for key in values.keys() {
        keys.insert(key.clone(), Value::Null);
    }
    keys.insert("public".into(), Value::Null);
    let mut out = Vec::new();
    for key in keys.keys() {
        let value = if key == "public" {
            public
        } else {
            &values[key]
        };
        flatten(value, &format!("/{key}"), c, &mut out);
    }
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

#[cfg(test)]
mod borrow_tests {
    use super::*;
    fn bits(tokens: Vec<Token>) -> Vec<(u32, u32, u32, u8)> {
        tokens
            .into_iter()
            .map(|Token(f, c, v, k)| (f, c, v.to_bits(), k))
            .collect()
    }
    fn reference(private: &Value, public: &Value, c: &EncodingConfig) -> Vec<Token> {
        let mut projected = private.clone();
        projected["public"] = public.clone();
        tokens(&projected, c)
    }
    #[test]
    fn borrowed_public_matches_original_bits_order_override_and_null() {
        let c = EncodingConfig {
            field_buckets: 4096,
            category_buckets: 4096,
        };
        let public =
            json!({"nested":[0,"-0.125",null,true,["Unknown","Unavailable","NotMaterialized"]]});
        for private in [
            json!({"z":"tail","a":false,"middle":[1,2]}),
            json!({"a":"-1.5","public":{"old":"Unavailable"},"z":null}),
            json!({"public":false}),
            Value::Null,
        ] {
            let before = private.clone();
            assert_eq!(
                bits(tokens_with_public(&private, &public, &c)),
                bits(reference(&private, &public, &c))
            );
            assert_eq!(private, before);
        }
        let mut p = serde_json::Map::new();
        p.insert("z".into(), json!(false));
        p.insert("public".into(), json!("old"));
        p.insert("a".into(), json!(true));
        let mut expected = Vec::new();
        // 独立列出根路径，按原Map插入/替换之后的次序拼接，各值原flatten复用。
        for key in p.keys() {
            match key.as_str() {
                "z" => flatten(&json!(false), "/z", &c, &mut expected),
                "a" => flatten(&json!(true), "/a", &c, &mut expected),
                "public" => flatten(&public, "/public", &c, &mut expected),
                _ => unreachable!(),
            }
        }
        assert_eq!(
            bits(tokens_with_public(&Value::Object(p), &public, &c)),
            bits(expected)
        );
        let encoded = tokens_with_public(&json!({"a":true}), &json!([null, 0]), &c);
        assert!(encoded.iter().any(|t| t.0 == bucket("/public/length", 4096)
            && t.1 == 0
            && t.2.to_bits() == (3f32.ln()).to_bits()
            && t.3 == 0));
        assert!(
            encoded
                .iter()
                .any(|t| t.0 == bucket("/public/0", 4096) && t.2.to_bits() == 0 && t.3 == 1)
        );
        assert!(
            encoded
                .iter()
                .any(|t| t.0 == bucket("/public/1", 4096) && t.2.to_bits() == 0 && t.3 == 0)
        );
    }
    #[test]
    fn non_object_keeps_original_index_panic() {
        let c = EncodingConfig {
            field_buckets: 4096,
            category_buckets: 4096,
        };
        let private = json!([1, 2]);
        let public = json!({"x":1});
        assert!(std::panic::catch_unwind(|| reference(&private, &public, &c)).is_err());
        assert!(std::panic::catch_unwind(|| tokens_with_public(&private, &public, &c)).is_err());
    }
}
