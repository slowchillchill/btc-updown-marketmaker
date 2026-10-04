//! 仅对最近原批的已有字段作只读匹配；没有公共归约、历史索引或第二事件模式。
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{str::FromStr, time::Instant};

#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
pub struct BrowseQuery {
    pub page: Option<u64>,
    pub kind: String,
    pub condition: String,
    pub position_domain: String,
    pub position_start: String,
    pub position_end: String,
    pub clock: String,
    pub time_start: String,
    pub time_end: String,
}
struct Range {
    start: Option<BigInt>,
    end: Option<BigInt>,
}
impl Range {
    fn parse(start: &str, end: &str) -> Result<Self, String> {
        let parse = |v: &str| {
            if v.is_empty() {
                Ok(None)
            } else {
                BigInt::from_str(v)
                    .map(Some)
                    .map_err(|_| "范围端点须为整数".to_string())
            }
        };
        let range = Self {
            start: parse(start)?,
            end: parse(end)?,
        };
        if let (Some(start), Some(end)) = (&range.start, &range.end)
            && start > end
        {
            return Err("范围起点不能大于终点".into());
        }
        Ok(range)
    }
    fn contains(&self, value: &str) -> Option<bool> {
        let value = BigInt::from_str(value).ok()?;
        Some(
            self.start.as_ref().is_none_or(|s| value >= *s)
                && self.end.as_ref().is_none_or(|e| value <= *e),
        )
    }
}
pub struct BatchMatcher<'a> {
    query: &'a BrowseQuery,
    position: Range,
    time: Range,
}
impl<'a> BatchMatcher<'a> {
    pub fn new(query: &'a BrowseQuery) -> Result<Self, String> {
        if (!query.position_start.is_empty() || !query.position_end.is_empty())
            && query.position_domain.is_empty()
        {
            return Err("位置端点须指定位置域".into());
        }
        if (!query.time_start.is_empty() || !query.time_end.is_empty()) && query.clock.is_empty() {
            return Err("时间端点须指定原时钟".into());
        }
        if ![
            "",
            "storage_row",
            "input_position",
            "canonical_position",
            "group_position",
            "observation_position",
        ]
        .contains(&query.position_domain.as_str())
        {
            return Err("未知公共位置域".into());
        }
        Ok(Self {
            query,
            position: Range::parse(&query.position_start, &query.position_end)?,
            time: Range::parse(&query.time_start, &query.time_end)?,
        })
    }
    pub fn cache_key(&self) -> Result<String, String> {
        let mut value = serde_json::to_value(self.query).map_err(|e| e.to_string())?;
        value.as_object_mut().expect("查询结构").remove("page");
        serde_json::to_string(&value).map_err(|e| e.to_string())
    }
    pub fn inspect(&self, batch: &[u8]) -> Result<Value, String> {
        let started = Instant::now();
        // 只解析当前最近编码批用于查询，不缓存第二份全量 JSON，不再次解码原 Parquet。
        let batch: Value = serde_json::from_slice(batch).map_err(|e| e.to_string())?;
        let rows = batch["rows"].as_array().ok_or("原批缺 rows")?;
        let start = BigInt::from_str(batch["row_start"].as_str().ok_or("原批缺偏移")?)
            .map_err(|e| e.to_string())?;
        let mut offsets = Vec::new();
        let mut unknown = 0_usize;
        let mut mismatch = 0_usize;
        for (i, row) in rows.iter().enumerate() {
            let (kind, value) = row
                .as_object()
                .and_then(|r| r.iter().next())
                .ok_or("原记录没有类型")?;
            let mut tests = Vec::new();
            if !self.query.kind.is_empty() {
                tests.push(Some(kind == &self.query.kind));
            }
            if !self.query.condition.is_empty() {
                let markets = markets(kind, value);
                tests.push(if markets.is_empty() {
                    None
                } else {
                    Some(markets.iter().any(|m| *m == self.query.condition))
                });
            }
            if !self.query.position_domain.is_empty() {
                let pos = if self.query.position_domain == "storage_row" {
                    Some((&start + BigInt::from(i)).to_string())
                } else {
                    value[&self.query.position_domain]
                        .as_str()
                        .or_else(|| {
                            (kind == "PmStepBinding"
                                && self.query.position_domain == "canonical_position")
                                .then(|| value["position"]["canonical_record_position"].as_str())
                                .flatten()
                        })
                        .map(str::to_owned)
                };
                tests.push(pos.as_deref().and_then(|v| self.position.contains(v)));
            }
            if !self.query.clock.is_empty() {
                let times = times(kind, value, &self.query.clock, &self.query.condition);
                let checks: Vec<bool> =
                    times.iter().filter_map(|v| self.time.contains(v)).collect();
                tests.push(if checks.is_empty() {
                    None
                } else {
                    Some(checks.contains(&true))
                });
            }
            if tests.contains(&Some(false)) {
                mismatch += 1;
            } else if tests.contains(&None) {
                unknown += 1;
            } else {
                offsets.push(i.to_string());
            }
        }
        Ok(
            json!({"matched_offsets":offsets,"scanned_rows":rows.len().to_string(),"unknown_rows":unknown.to_string(),"mismatch_rows":mismatch.to_string(),
            "query_elapsed_us":started.elapsed().as_micros().to_string(),"scope":"当前单批；命中位置是原批内偏移，原序保留"}),
        )
    }
}
fn market(value: &Value) -> Option<&str> {
    value.as_str().or_else(|| value["condition_id"].as_str())
}
fn native_input(value: &Value) -> Option<&Value> {
    let raw = &value["ingress"]["Message"]["row"];
    raw["Existing"]
        .as_object()
        .or_else(|| raw["PolymarketMarketMetadataV6"].as_object())
        .and_then(|v| v.values().next())
}
fn canonical(value: &Value) -> Option<(&str, &Value)> {
    value["record"]
        .as_object()
        .and_then(|v| v.iter().next())
        .map(|(k, v)| (k.as_str(), v))
}
fn markets<'a>(kind: &str, value: &'a Value) -> Vec<&'a str> {
    let mut out = Vec::new();
    match kind {
        "Input" => {
            if let Some(native) = native_input(value) {
                out.extend(market(&native["market"]));
                out.extend(market(&native["condition_id"]));
            }
            if let Some(rows) =
                value["ingress"]["Message"]["row"]["HistoricalPriceAnchorRepairBatch"]["rows"]
                    .as_array()
            {
                for row in rows {
                    out.extend(market(&row["condition_id"]));
                }
            }
        }
        "Canonical" => {
            if let Some((kind, native)) = canonical(value) {
                out.extend(market(&native["market"]));
                if kind == "ReferenceEvent" {
                    out.extend(market(&native["payload"]["reference"]["market"]));
                }
            }
        }
        "Observation" => out.extend(market(&value["market"])),
        _ => {}
    }
    out
}
fn times<'a>(kind: &str, value: &'a Value, clock: &str, condition: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut add = |v: &'a Value, label: &str, market: Option<&str>| {
        if label == clock
            && (condition.is_empty() || market == Some(condition))
            && let Some(v) = v.as_str()
            && v.strip_prefix('-')
                .unwrap_or(v)
                .chars()
                .all(|c| c.is_ascii_digit())
            && !v.is_empty()
            && v != "-"
        {
            out.push(v);
        }
    };
    let event = |v: &'a Value,
                 label: &str,
                 market: Option<&str>,
                 add: &mut dyn FnMut(&'a Value, &str, Option<&str>)| {
        add(
            &v["source_event_at_utc_ms"],
            &format!("{label} · 来源事件"),
            market,
        );
        add(&v["received_at_utc_ms"], &format!("{label} · 接收"), market);
    };
    let evidence = |values: &'a Value,
                    market: Option<&str>,
                    add: &mut dyn FnMut(&'a Value, &str, Option<&str>)| {
        if let Some(values) = values.as_array() {
            for v in values {
                event(v, "来源证据", market, add);
            }
        }
    };
    match kind {
        "Input" => {
            let native = native_input(value);
            let c =
                native.and_then(|n| market(&n["condition_id"]).or_else(|| market(&n["market"])));
            event(
                &value["ingress"]["Message"]["source_evidence"],
                "源消息",
                c,
                &mut add,
            );
            if let Some(native) = native {
                for (field, label) in [
                    ("metadata_fetch_start_utc", "请求时钟"),
                    ("http_received_at_utc", "HTTP 接收"),
                    ("anchor_source_computed_at_utc", "锚价源计算"),
                    ("collector_admission_time_utc_ms", "采集接纳"),
                ] {
                    add(&native[field], label, c);
                }
            }
            let repair = &value["ingress"]["Message"]["row"]["HistoricalPriceAnchorRepairBatch"];
            add(
                &repair["response"]["received_at_utc_ms"],
                "历史补查接收",
                None,
            );
            if let Some(rows) = repair["rows"].as_array() {
                for row in rows {
                    let c = market(&row["condition_id"]);
                    for (field, label) in [
                        ("source_event_at_utc_ms", "历史来源事件"),
                        ("response_received_at_utc_ms", "历史补查接收"),
                        ("historical_identity_ready_at_utc_ms", "历史身份就绪"),
                    ] {
                        add(&row[field], label, c);
                    }
                }
            }
        }
        "Canonical" => {
            if let Some((variant, native)) = canonical(value) {
                let c = market(&native["market"]).or_else(|| {
                    (variant == "ReferenceEvent")
                        .then(|| market(&native["payload"]["reference"]["market"]))
                        .flatten()
                });
                for (field, label) in [
                    ("time", "规范事件"),
                    ("direct_time", "直接来源"),
                    ("dependency_time", "依赖来源"),
                ] {
                    event(&native[field], label, c, &mut add);
                }
                evidence(&native["lineage"]["source_evidence"], c, &mut add);
                let app = &native["payload"]["reference"]["anchor"]["application"];
                if app["kind"] == "historical_repair" {
                    add(
                        &app["historical_identity_ready_at_utc_ms"],
                        "历史身份就绪",
                        c,
                    );
                    add(&app["effective_from_utc_ms"], "历史生效", c);
                    evidence(&app["historical_identity_evidence"], c, &mut add);
                }
            }
        }
        "PmReport" => {
            for variant in ["NoOutput", "Suppressed"] {
                if let Some(rows) =
                    value["report"]["disposition"][variant]["operation_evidence"].as_array()
                {
                    for row in rows {
                        event(&row["source_evidence"], "来源证据", None, &mut add);
                    }
                }
            }
        }
        "Report" => {
            if let Some(rows) =
                value["report"]["report"]["disposition"]["operation_evidence"].as_array()
            {
                for row in rows {
                    event(&row["source_evidence"], "来源证据", None, &mut add);
                }
            }
        }
        _ => {}
    }
    out
}
