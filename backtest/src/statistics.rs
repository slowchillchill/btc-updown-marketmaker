//! 原材料试制通用统计；只保留计数、首例和最近来源，不持有完整组历史。
use crate::HistoricalInitializationProvenanceV1;
use contracts::*;
use data_pipeline_core::public_input::RecordedIngressV1;
use serde::Serialize;
use serde_json::{Value, json};
use sim_system::public_consumption::original_public_group_visible_at_utc_ms;
use std::collections::BTreeMap;

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

/// 与原material_trial完整statistics对象保持相同字段及序列化形状。
#[derive(Default, Serialize)]
pub struct HistoricalStatisticsV1 {
    public_groups: u64,
    markets: BTreeMap<String, MarketStats>,
    initialization_public_groups: u64,
    initialization_markets: BTreeMap<String, MarketStats>,
    research_groups_with_initialization_dependencies: u64,
}
#[derive(Default)]
pub(crate) struct HistoricalStatisticsCollectorV1 {
    statistics: HistoricalStatisticsV1,
    last_visible: Option<i64>,
}
impl HistoricalStatisticsCollectorV1 {
    pub(crate) fn observe(
        &mut self,
        input: &RecordedIngressV1,
        group: &ClosedBusinessGroupV1,
        initialization: &HistoricalInitializationProvenanceV1,
    ) {
        let visible = original_public_group_visible_at_utc_ms(input, group);
        let at = self.last_visible.map_or(visible, |last| last.max(visible));
        self.last_visible = Some(at);
        self.observe_at(group, at, initialization);
    }
    pub(crate) fn observe_at(
        &mut self,
        group: &ClosedBusinessGroupV1,
        at: i64,
        initialization: &HistoricalInitializationProvenanceV1,
    ) {
        let initial = initialization.group_is_initialization(&group.member_input_positions);
        if initial {
            self.statistics.initialization_public_groups += 1;
        } else {
            self.statistics.public_groups += 1;
            if initialization.group_has_initialization_dependency(
                &group.member_input_positions,
                group.closed_at_input_position,
            ) || initialization.observations_have_initialization_dependency(&group.observations)
            {
                self.statistics
                    .research_groups_with_initialization_dependencies += 1;
            }
        }
        for o in &group.observations {
            (if initial {
                &mut self.statistics.initialization_markets
            } else {
                &mut self.statistics.markets
            })
            .entry(o.market.condition_id.0.clone())
            .or_default()
            .observe(o, at);
        }
    }
    pub(crate) fn finish(self) -> HistoricalStatisticsV1 {
        self.statistics
    }
}
#[cfg(test)]
#[path = "statistics_tests.rs"]
mod tests;
