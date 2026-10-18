//! 同次日生产的只读研究装配。公共 backtest 不识别 BLB 或策略。
use backtest::{HistoricalPublicEvidenceV1, HistoricalPublicObserverV1, MaterialError};
use baseline_mm::blb_research::blb_research_v1;
use contracts::*;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
struct MarketCache {
    depth: Arc<ObservedFactV1<BinanceDepthObservationV1>>,
    anchor: ObservedFactV1<MarketPriceAnchorV1>,
    full_blb: Arc<Value>,
    scalar: Value,
    blb_group: PublicGroupReferenceV1,
    pm: Value,
    progress: SharedSourceProgressV1,
}
#[derive(Debug)]
struct State {
    writer: BufWriter<std::fs::File>,
    markets: BTreeMap<(String, String), MarketCache>,
    request_prefixes: std::collections::BTreeSet<(String, String)>,
}
#[derive(Debug)]
pub(super) struct StreamingResearchV1(Mutex<State>);
impl StreamingResearchV1 {
    pub fn open(root: &Path) -> Result<Self, MaterialError> {
        std::fs::create_dir(root)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("public.jsonl"))?;
        Ok(Self(Mutex::new(State {
            writer: BufWriter::with_capacity(1024 * 1024, file),
            markets: BTreeMap::new(),
            request_prefixes: Default::default(),
        })))
    }
}
fn line(w: &mut impl Write, v: &Value) -> Result<(), MaterialError> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}
fn best(levels: &Option<Vec<PriceLevelV1>>, bid: bool) -> Option<&PriceLevelV1> {
    let valid = levels.as_ref()?.iter().filter_map(|l| {
        let p = l.price_pusd_per_share.0.parse::<Decimal>().ok()?;
        let q = l.quantity_shares.0.parse::<Decimal>().ok()?;
        (q > Decimal::ZERO).then_some((p, l))
    });
    if bid {
        valid.max_by_key(|(p, _)| *p).map(|(_, l)| l)
    } else {
        valid.min_by_key(|(p, _)| *p).map(|(_, l)| l)
    }
}
fn pm(o: &MarketBusinessObservationV1) -> Value {
    Value::Array(o.assets.iter().map(|a|json!({
        "asset":a.asset,"bid":best(&a.bids.value,true),"ask":best(&a.asks.value,false),
        "bid_quality":a.bids.quality,"ask_quality":a.asks.quality,
        "bid_anchors":a.bids.direct_anchors,"ask_anchors":a.asks.direct_anchors,
        "bid_frontier":a.bids.dependency_frontier,"ask_frontier":a.asks.dependency_frontier,
        "best_bid_ask":a.best_bid_ask,"tick":a.tick
    })).collect())
}
impl HistoricalPublicObserverV1 for StreamingResearchV1 {
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1::FULL
    }
    fn observe(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        let mut state = self.0.lock().unwrap();
        let mut current = serde_json::Map::new();
        let mut request_evidence = BTreeMap::new();
        for o in group.observations.iter() {
            let key = (
                reference.input_stream_id.clone(),
                o.market.condition_id.0.clone(),
            );
            let old = state.markets.get(&key);
            let changed = old.is_none_or(|c| {
                !Arc::ptr_eq(&c.depth, &o.binance_depth) || c.anchor != o.official_anchor
            });
            let (full_blb, scalar, blb_group) = if changed {
                let full_blb = Arc::new(serde_json::to_value(blb_research_v1(o))?);
                let mut scalar = (*full_blb).clone();
                scalar
                    .as_object_mut()
                    .unwrap()
                    .remove("visible_positive_levels");
                (full_blb, scalar, reference.clone())
            } else {
                let c = old.unwrap();
                (c.full_blb.clone(), c.scalar.clone(), c.blb_group.clone())
            };
            let pm = pm(o);
            let pm_changed = old.is_none_or(|c| c.pm != pm);
            let progress_changed = old.is_none_or(|c| c.progress != o.source_progress);
            if changed {
                line(
                    &mut state.writer,
                    &json!({"record_type":"blb_state","group":reference,"condition_id":o.market.condition_id,"blb":scalar,"official_anchor":o.official_anchor,
                    "depth_quality":o.binance_depth.quality,"depth_anchors":o.binance_depth.direct_anchors,"depth_frontier":o.binance_depth.dependency_frontier}),
                )?;
            }
            if pm_changed || progress_changed {
                line(
                    &mut state.writer,
                    &json!({"record_type":"pm_source_state","group":reference,"condition_id":o.market.condition_id,"pm":pm,"source_progress":o.source_progress,"blb_group":blb_group}),
                )?;
            }
            current.insert(o.market.condition_id.0.clone(),json!({"group":reference,"blb_group":blb_group,"blb":scalar,"pm":pm,"source_progress":o.source_progress}));
            request_evidence.insert(o.market.condition_id.0.clone(), full_blb.clone());
            state.markets.insert(
                key,
                MarketCache {
                    depth: o.binance_depth.clone(),
                    anchor: o.official_anchor.clone(),
                    full_blb,
                    scalar,
                    blb_group,
                    pm,
                    progress: o.source_progress.clone(),
                },
            );
        }
        Ok(Arc::new(HistoricalPublicEvidenceV1 {
            scalars: Value::Object(current),
            request_evidence,
        }))
    }
    fn record_request_evidence(&self, evidence: &Value) -> Result<Value, MaterialError> {
        let mut state = self.0.lock().unwrap();
        let mut links = Vec::new();
        if let Some(rows) = evidence.as_array() {
            for row in rows {
                let key = (
                    serde_json::to_string(&row["group"])?,
                    serde_json::to_string(&row["condition_id"])?,
                );
                if !state.request_prefixes.contains(&key) {
                    line(
                        &mut state.writer,
                        &json!({"record_type":"original_pair_blb","evidence":row}),
                    )?;
                    state.request_prefixes.insert(key);
                }
                links.push(json!({"group":row["group"],"condition_id":row["condition_id"],"record_type":"original_pair_blb"}));
            }
        }
        // 请求证据可能晚于producer到达EOF；在消费者返回前传播该次写入失败。
        state.writer.flush()?;
        Ok(Value::Array(links))
    }
    fn finish(&self) -> Result<(), MaterialError> {
        self.0.lock().unwrap().writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::*;
    use std::sync::Arc;
    fn observed<T>(value: T) -> ObservedFactV1<T> {
        ObservedFactV1 {
            value: Some(value),
            ..Default::default()
        }
    }
    fn observation(k: Option<&str>) -> MarketBusinessObservationV1 {
        let level = |p: &str, q: &str| ReferencePriceLevelV1 {
            price_usdt_per_btc: ExactDecimal(p.into()),
            quantity_btc: ExactDecimal(q.into()),
        };
        MarketBusinessObservationV1 {
            schema_version: "market_business_observation_v1".into(),
            market: MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
                clob_market_id: ClobMarketId("3195576".into()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 0,
                    end_at_utc_ms: 300000,
                },
            },
            group_id: Sha256Hex("a".repeat(64)),
            group_position: 0,
            visible_prefix: AppliedIngressPrefixV1 {
                input_stream_id: "test".into(),
                next_input_position: 1,
            },
            assets: vec![],
            lifecycle: Default::default(),
            active: Default::default(),
            closed: Default::default(),
            accepting_orders: Default::default(),
            terminal: Default::default(),
            official_anchor: k
                .map(|k| {
                    observed(MarketPriceAnchorV1 {
                        price_usd_per_btc: ExactDecimal(k.into()),
                        source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
                        application: PriceAnchorApplicationV1::Observed,
                    })
                })
                .unwrap_or_default(),
            settlement_reference: Default::default(),
            temporary_reference: None,
            effective_reference: EffectiveMarketReferenceV1::Missing,
            chainlink_spot: Default::default(),
            chainlink_twap_thirty: Default::default(),
            chainlink_twap_sixty: Default::default(),
            binance_depth: Arc::new(observed(BinanceDepthObservationV1::Full(Arc::new(
                BinanceDepthStateReferenceV1 {
                    symbol: "BTCUSDT".into(),
                    last_update_id: 1,
                    bids: vec![level("99", "2"), level("98", "3"), level("97", "9")],
                    asks: vec![level("101", "2"), level("102", "3"), level("103", "9")],
                },
            )))),
            binance_trade: Default::default(),
            group_events: Arc::from([]),
            source_progress: Default::default(),
        }
    }

    fn deliver(
        sink: &StreamingResearchV1,
        mut o: MarketBusinessObservationV1,
        position: u64,
    ) -> Arc<Value> {
        o.group_position = position;
        o.visible_prefix.next_input_position = position + 1;
        let reference = PublicGroupReferenceV1 {
            input_stream_id: "test".into(),
            applied_prefix: o.visible_prefix.clone(),
            group_position: position,
            group_id: o.group_id.clone(),
            closed_at_input_position: position,
        };
        let group = ClosedBusinessGroupV1 {
            group_id: o.group_id.clone(),
            group_position: position,
            member_input_positions: vec![position],
            closed_at_input_position: position,
            records: Arc::from([]),
            observations: vec![o].into(),
        };
        Arc::new(sink.observe(&reference, &group).unwrap().scalars.clone())
    }
    #[test]
    fn same_prefix_function_unknown_k_revision_and_pm_independence() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("research");
        let sink = StreamingResearchV1::open(&root).unwrap();
        let unknown = deliver(&sink, observation(None), 0);
        let condition = format!("0x{}", "1".repeat(64));
        assert_eq!(
            unknown[&condition]["blb"]["unknown_reason"],
            "official_anchor_not_observed"
        );
        let mut o = observation(Some("98"));
        let early = deliver(&sink, o.clone(), 1);
        let mut expected = serde_json::to_value(blb_research_v1(&o)).unwrap();
        expected
            .as_object_mut()
            .unwrap()
            .remove("visible_positive_levels");
        assert_eq!(early[&condition]["blb"], expected);
        assert_eq!(early[&condition]["blb"]["visible_quantity_btc"], "5");
        o.assets.push(AssetBusinessObservationV1 {
            asset: AssetIdentityV1 {
                token_id: TokenId("11".into()),
                outcome: OutcomeIdentityV1 {
                    index: 0,
                    label: "Up".into(),
                },
            },
            bids: Arc::new(observed(vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal("0.4".into()),
                quantity_shares: ExactDecimal("7".into()),
            }])),
            asks: Default::default(),
            best_bid_ask: Default::default(),
            valuation_best_bid: Default::default(),
            tick: Default::default(),
            public_trade: Default::default(),
        });
        let pm = deliver(&sink, o.clone(), 2);
        assert_eq!(pm[&condition]["blb_group"]["group_position"], 1);
        assert_ne!(pm[&condition]["pm"], early[&condition]["pm"]);
        o.official_anchor.value.as_mut().unwrap().price_usd_per_btc = ExactDecimal("100".into());
        let revised = deliver(&sink, o, 3);
        assert_eq!(revised[&condition]["blb"]["status"], "at_anchor");
        assert!(revised[&condition]["blb"]["visible_quantity_btc"].is_null());
        assert_eq!(early[&condition]["blb"]["official_k_usd_per_btc"], "98");
        assert_eq!(early[&condition]["group"]["closed_at_input_position"], 1);
        let request = json!([{"condition_id":condition,"group":early[&condition]["group"],"blb":blb_research_v1(&observation(Some("98")))}]);
        let links = sink.record_request_evidence(&request).unwrap();
        assert_eq!(links, sink.record_request_evidence(&request).unwrap());
        assert!(links[0].get("blb").is_none(), "候选只持有共享逐档证据引用");
        sink.finish().unwrap();
        let records = std::fs::read_to_string(root.join("public.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            records
                .iter()
                .filter(|r| r["record_type"] == "original_pair_blb")
                .count(),
            1,
            "同原前缀与市场只输出一份逐档证据"
        );
        assert_eq!(
            records
                .iter()
                .find(|r| r["record_type"] == "original_pair_blb")
                .unwrap()["evidence"]["blb"]["visible_positive_levels"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            records
                .iter()
                .filter(|r| r["record_type"] == "blb_state")
                .count(),
            3,
            "PM独立变化不重算BLB"
        );
        assert!(records.iter().any(|r|r["record_type"]=="pm_source_state"&&r["group"]["group_position"]==2));
    }
    #[test]
    fn required_public_flush_failure_is_returned() {
        let sink = StreamingResearchV1(Mutex::new(State {
            writer: BufWriter::new(OpenOptions::new().write(true).open("/dev/full").unwrap()),
            markets: Default::default(),
            request_prefixes: Default::default(),
        }));
        let _ = deliver(&sink, observation(None), 0);
        assert!(sink.finish().is_err());
    }
}
