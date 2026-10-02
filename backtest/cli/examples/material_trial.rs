//! #185 最早小范围材料试制：原准备入口、公开可见观察统计，无策略或账户。
use backtest::{HistoricalPreparationConfigV1, MaterialError, prepare_historical_run_v1};
use contracts::*;
use serde::Serialize;
use serde_json::{Value, json};
use sim_system::public_consumption::{PublicConsumptionTimeSourceV1, RecordedPublicGroupSourceV1};
use std::{collections::BTreeMap, path::Path, time::Instant};

#[derive(Default)]
struct OriginalVisibility(Option<i64>);
impl PublicConsumptionTimeSourceV1 for OriginalVisibility {
    fn execution_time_for_group(
        &mut self,
        _: &PublicGroupReferenceV1,
    ) -> Result<i64, sim_system::execution::SimRunActorErrorV2> {
        Err(
            sim_system::execution::SimRunActorErrorV2::PublicConsumptionSource(
                "需要原来源可见时刻".into(),
            ),
        )
    }
    fn execution_time_for_visible_group(
        &mut self,
        _: &PublicGroupReferenceV1,
        visible: i64,
    ) -> Result<i64, sim_system::execution::SimRunActorErrorV2> {
        let at = self.0.map_or(visible, |last| last.max(visible));
        self.0 = Some(at);
        Ok(at)
    }
}
#[derive(Serialize)]
struct ProcessCounters {
    io: BTreeMap<String, u64>,
    user_ticks: u64,
    system_ticks: u64,
}
impl ProcessCounters {
    fn read() -> Result<Self, MaterialError> {
        let io = std::fs::read_to_string("/proc/self/io")?
            .lines()
            .map(|line| {
                let (k, v) = line.split_once(':').ok_or("I/O字段缺失")?;
                Ok((k.to_owned(), v.trim().parse()?))
            })
            .collect::<Result<_, MaterialError>>()?;
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let fields = stat
            .rsplit_once(") ")
            .ok_or("stat字段缺失")?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        Ok(Self {
            io,
            user_ticks: fields[11].parse()?,
            system_ticks: fields[12].parse()?,
        })
    }
    fn since(self, before: Self, hz: f64, wall: f64) -> Value {
        json!({"wall_seconds":wall, "user_seconds":(self.user_ticks-before.user_ticks) as f64/hz,
            "system_seconds":(self.system_ticks-before.system_ticks) as f64/hz,
            "proc_io":self.io.into_iter().map(|(k,v)| { let delta=v-before.io[&k]; (k,delta) }).collect::<BTreeMap<_,_>>()})
    }
}
#[derive(Default, Serialize)]
struct Counts {
    denominator: u64,
    categories: BTreeMap<String, u64>,
    first_examples: BTreeMap<String, Value>,
}
impl Counts {
    fn add(&mut self, category: String, at: i64, position: u64) {
        self.denominator += 1;
        *self.categories.entry(category.clone()).or_default() += 1;
        self.first_examples
            .entry(category)
            .or_insert(json!({"visible_at_utc_ms":at,"group_position":position}));
    }
}
fn quality<T>(f: &ObservedFactV1<T>) -> String {
    format!(
        "{}:{:?}",
        if f.value.is_some() {
            "present"
        } else {
            "absent"
        },
        f.quality
    )
}
fn best_bid(f: &ObservedFactV1<Option<ExactDecimal>>) -> String {
    let availability = match (&f.value, f.quality) {
        (Some(Some(_)), MarketDataQuality::Fresh) => "known_price",
        (Some(None), MarketDataQuality::Fresh) => "known_empty",
        _ => "unavailable",
    };
    format!("{availability}:{:?}", f.quality)
}
fn reference(f: &EffectiveMarketReferenceV1) -> String {
    match f {
        EffectiveMarketReferenceV1::Missing => "missing".into(),
        EffectiveMarketReferenceV1::TemporaryTwap(v) => {
            format!("temporary_twap_{}s", v.value.window_s)
        }
        EffectiveMarketReferenceV1::OfficialAnchor(v) => format!("official:{}", quality(v)),
    }
}
#[derive(Default, Serialize)]
struct MarketStats {
    identity: Option<MarketIdentityV2>,
    observations: u64,
    first_visible_at: Option<i64>,
    last_visible_at: Option<i64>,
    fields: BTreeMap<String, Counts>,
    first_fresh_spot_source_at: Option<i64>,
    last_fresh_spot_source_at: Option<i64>,
    distinct_fresh_spot_samples: u64,
    max_forward_spot_source_gap_ms: i64,
    source_time_regressions: u64,
    #[serde(skip)]
    last_spot_id: Option<Sha256Hex>,
}
impl MarketStats {
    fn add(&mut self, key: &str, category: String, at: i64, position: u64) {
        self.fields
            .entry(key.into())
            .or_default()
            .add(category, at, position);
    }
    fn observe(&mut self, o: &MarketBusinessObservationV1, at: i64) {
        self.identity.get_or_insert_with(|| o.market.clone());
        self.observations += 1;
        self.first_visible_at.get_or_insert(at);
        self.last_visible_at = Some(at);
        let position = o.group_position;
        self.add(
            "effective_reference",
            reference(&o.effective_reference),
            at,
            position,
        );
        macro_rules! fields { ($($field:ident),*) => {$(self.add(stringify!($field),quality(&o.$field),at,position);)*}; }
        fields!(
            official_anchor,
            settlement_reference,
            lifecycle,
            active,
            closed,
            accepting_orders,
            terminal,
            chainlink_spot,
            chainlink_twap_thirty,
            chainlink_twap_sixty,
            binance_depth,
            binance_trade
        );
        for (key, value) in [
            (
                "lifecycle_value",
                o.lifecycle.value.as_ref().map(|v| format!("{v:?}")),
            ),
            (
                "settlement_reference_value",
                o.settlement_reference
                    .value
                    .as_ref()
                    .map(|v| format!("{v:?}")),
            ),
        ] {
            self.add(key, value.unwrap_or("missing".into()), at, position);
        }
        self.add(
            "market_window",
            if at < o.market.window.start_at_utc_ms {
                "before"
            } else if at < o.market.window.end_at_utc_ms {
                "inside"
            } else {
                "after"
            }
            .into(),
            at,
            position,
        );
        if o.chainlink_spot.value.is_some() && o.chainlink_spot.quality == MarketDataQuality::Fresh
        {
            for anchor in &o.chainlink_spot.direct_anchors {
                if let Some(source_at) = anchor.source_event_at_utc_ms
                    && source_at <= at
                    && anchor.received_at_utc_ms <= at
                    && self.last_spot_id.as_ref() != Some(&anchor.source_message_id)
                {
                    self.first_fresh_spot_source_at.get_or_insert(source_at);
                    if let Some(last) = self.last_fresh_spot_source_at {
                        if source_at < last {
                            self.source_time_regressions += 1;
                        } else {
                            self.max_forward_spot_source_gap_ms =
                                self.max_forward_spot_source_gap_ms.max(source_at - last);
                        }
                    }
                    self.last_fresh_spot_source_at = Some(source_at);
                    self.last_spot_id = Some(anchor.source_message_id.clone());
                    self.distinct_fresh_spot_samples += 1;
                }
            }
        }
        // 仅描述当前公开因果样本的端点覆盖；不插值、不填历史、不运行概率算法。
        let history = match o.settlement_reference.value {
            None => "reference_missing",
            Some(BtcSettlementReferenceV1::ChainlinkSpot) => "spot_reference_no_twap_integral",
            Some(rule) => {
                let seconds = if rule == BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds {
                    30
                } else {
                    60
                };
                let start = o.market.window.end_at_utc_ms - seconds * 1000;
                if at < start {
                    "before_settlement_window"
                } else if self
                    .first_fresh_spot_source_at
                    .is_none_or(|first| first > start)
                {
                    "window_start_not_covered"
                } else {
                    "endpoints_present_not_integral_validated"
                }
            }
        };
        self.add("causal_window_history", history.into(), at, position);
        self.add("member_count", o.assets.len().to_string(), at, position);
        for asset in &o.assets {
            let prefix = format!("asset/{:?}/{}", asset.asset.outcome, asset.asset.token_id.0);
            for (field, category) in [
                ("bids", quality(&asset.bids)),
                ("asks", quality(&asset.asks)),
                ("tick", quality(&asset.tick)),
                ("public_trade", quality(&asset.public_trade)),
                ("best_bid_ask", quality(&asset.best_bid_ask)),
                ("valuation_best_bid", best_bid(&asset.valuation_best_bid)),
            ] {
                self.add(&format!("{prefix}/{field}"), category, at, position);
            }
        }
    }
}
fn save(path: &Path, v: &Value) -> Result<(), MaterialError> {
    std::fs::write(path, serde_json::to_vec_pretty(v)?)?;
    Ok(())
}
fn main() -> Result<(), MaterialError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("用法 material_trial <PREPARATION_JSON> <REPORT_JSON>".into());
    }
    let config: HistoricalPreparationConfigV1 = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let report_path = Path::new(&args[1]);
    let hz: f64 = String::from_utf8(
        std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()?
            .stdout,
    )?
    .trim()
    .parse()?;
    let mut report = json!({"schema":"complementary_pair_material_trial_v1", "clock_ticks_per_second":hz,
        "scope":config.scope,"status":"started", "statistics":null});
    // 私有报告保留完整有效配置及原错误；发布版由交付文档移除物理定位。
    report["effective_preparation"] = serde_json::to_value(&config)?;
    save(report_path, &report)?;
    let before = ProcessCounters::read()?;
    let began = Instant::now();
    let prepared = prepare_historical_run_v1(&config);
    report["prepare_measurement"] =
        ProcessCounters::read()?.since(before, hz, began.elapsed().as_secs_f64());
    let prepared = match prepared {
        Ok(value) => value,
        Err(error) => {
            report["status"] = json!("prepare_failed");
            report["original_error"] = json!(error.to_string());
            report["original_error_debug"] = json!(format!("{error:?}"));
            save(report_path, &report)?;
            return Err(error);
        }
    };
    report["input_set_identity_sha256"] = json!(prepared.input_set_identity_sha256);
    report["source_manifests"] = json!(prepared.source_manifests);
    report["prepared_markets"] = json!(prepared.markets.keys().collect::<Vec<_>>());
    report["hidden_preparation"] = json!(prepared.hidden);
    report["status"] = json!("prepared");
    save(report_path, &report)?;
    let before = ProcessCounters::read()?;
    let began = Instant::now();
    let stats = (|| -> Result<Value, MaterialError> {
        let source = RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            config.read_page_rows,
            config.max_read_batch_bytes,
            OriginalVisibility::default(),
        )?;
        let mut markets = BTreeMap::<String, MarketStats>::new();
        let mut groups = 0u64;
        for delivered in source {
            let delivered = delivered.map_err(|e| format!("{e:?}"))?;
            groups += 1;
            for o in &delivered.prepared.group.observations {
                markets
                    .entry(o.market.condition_id.0.clone())
                    .or_default()
                    .observe(o, delivered.delivered_at_utc_ms);
            }
        }
        Ok(json!({"public_groups":groups,"markets":markets}))
    })();
    report["statistics_measurement"] =
        ProcessCounters::read()?.since(before, hz, began.elapsed().as_secs_f64());
    match stats {
        Ok(value) => {
            report["statistics"] = value;
            report["status"] = json!("success");
        }
        Err(error) => {
            report["status"] = json!("statistics_failed");
            report["original_error"] = json!(error.to_string());
            save(report_path, &report)?;
            return Err(error);
        }
    }
    save(report_path, &report)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn valid_empty_bid_and_stale_retained_value_are_distinct() {
        let mut bid: ObservedFactV1<Option<ExactDecimal>> = ObservedFactV1 {
            value: Some(None),
            quality: MarketDataQuality::Fresh,
            ..Default::default()
        };
        assert_eq!(best_bid(&bid), "known_empty:Fresh");
        bid.quality = MarketDataQuality::ConfirmedGap;
        assert_eq!(best_bid(&bid), "unavailable:ConfirmedGap");
        bid.value = None;
        bid.quality = MarketDataQuality::UnknownNotObserved;
        assert_eq!(best_bid(&bid), "unavailable:UnknownNotObserved");
    }
}
