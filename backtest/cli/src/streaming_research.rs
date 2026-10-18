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
    pm_group: PublicGroupReferenceV1,
    sides: BTreeMap<(String, &'static str), SideCache>,
}
#[derive(Debug)]
struct SideCache {
    book: Arc<ObservedFactV1<Vec<PriceLevelV1>>>,
    group: PublicGroupReferenceV1,
}
#[derive(Debug)]
struct ProgressCache {
    progress: SharedSourceProgressV1,
    group: PublicGroupReferenceV1,
}
#[derive(Debug)]
struct State {
    writer: BufWriter<std::fs::File>,
    markets: BTreeMap<(String, String), MarketCache>,
    progress: BTreeMap<String, ProgressCache>,
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
            progress: BTreeMap::new(),
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
    Value::Array(
        o.assets
            .iter()
            .map(|a| {
                json!({
                    "asset":a.asset,"bid":best(&a.bids.value,true),"ask":best(&a.asks.value,false),
                    "bid_quality":a.bids.quality,"ask_quality":a.asks.quality,
                    "best_bid_ask":a.best_bid_ask,"tick":a.tick
                })
            })
            .collect(),
    )
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
            // 同组市场共享来源进展；按流独立写出一次，避免随市场重复。
            let progress = state.progress.get(&reference.input_stream_id);
            let progress_group = if progress.is_none_or(|c| c.progress != o.source_progress) {
                let updates = o
                    .source_progress
                    .iter()
                    .filter(|p| progress.and_then(|c| c.progress.get_scope(&p.scope)) != Some(*p))
                    .collect::<Vec<_>>();
                let removed = progress
                    .map(|c| {
                        c.progress
                            .iter()
                            .filter(|p| o.source_progress.get_scope(&p.scope).is_none())
                            .map(|p| &p.scope)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let row = json!({
                    "record_type":"source_progress", "group":reference,
                    "updates":updates, "removed":removed
                });
                line(&mut state.writer, &row)?;
                state.progress.insert(
                    reference.input_stream_id.clone(),
                    ProgressCache {
                        progress: o.source_progress.clone(),
                        group: reference.clone(),
                    },
                );
                reference.clone()
            } else {
                progress.unwrap().group.clone()
            };
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
            let pm_group = if pm_changed {
                reference.clone()
            } else {
                old.unwrap().pm_group.clone()
            };
            let mut sides = BTreeMap::new();
            let mut side_links = Vec::new();
            let mut changed_sides = Vec::new();
            for asset in &o.assets {
                for (side, book) in [("bid", &asset.bids), ("ask", &asset.asks)] {
                    let side_key = (asset.asset.token_id.0.clone(), side);
                    let prior = old.and_then(|c| c.sides.get(&side_key));
                    let changed = prior.is_none_or(|c| {
                        !Arc::ptr_eq(&c.book, book)
                            && (c.book.direct_anchors != book.direct_anchors
                                || c.book.dependency_frontier != book.dependency_frontier)
                    });
                    let source_group = if changed {
                        reference.clone()
                    } else {
                        prior.unwrap().group.clone()
                    };
                    if changed {
                        changed_sides.push(json!({
                            "record_type":"pm_side_sources", "group":reference,
                            "condition_id":o.market.condition_id, "token_id":asset.asset.token_id,
                            "side":side, "direct_anchors":book.direct_anchors,
                            "dependency_frontier":book.dependency_frontier
                        }));
                    }
                    side_links.push(
                        json!({"token_id":asset.asset.token_id,"side":side,"group":source_group}),
                    );
                    sides.insert(
                        side_key,
                        SideCache {
                            book: book.clone(),
                            group: source_group,
                        },
                    );
                }
            }
            if changed {
                line(
                    &mut state.writer,
                    &json!({"record_type":"blb_state","group":reference,"condition_id":o.market.condition_id,"blb":scalar,"official_anchor":o.official_anchor,
                    "depth_quality":o.binance_depth.quality,"depth_anchors":o.binance_depth.direct_anchors,"depth_frontier":o.binance_depth.dependency_frontier}),
                )?;
            }
            for row in changed_sides {
                line(&mut state.writer, &row)?;
            }
            if pm_changed {
                line(
                    &mut state.writer,
                    &json!({"record_type":"pm_state","group":reference,"condition_id":o.market.condition_id,"pm":pm}),
                )?;
            }
            current.insert(o.market.condition_id.0.clone(),json!({"group":reference,"blb_group":blb_group,"blb":scalar,"pm_group":pm_group,"pm_side_sources":side_links,"source_progress_group":progress_group}));
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
                    pm_group,
                    sides,
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
        o: MarketBusinessObservationV1,
        position: u64,
    ) -> Arc<Value> {
        deliver_many(sink, "test", vec![o], position)
    }
    fn deliver_many(
        sink: &StreamingResearchV1,
        stream: &str,
        mut observations: Vec<MarketBusinessObservationV1>,
        position: u64,
    ) -> Arc<Value> {
        let group_id = Sha256Hex(format!("{position:064x}"));
        let reference = PublicGroupReferenceV1 {
            input_stream_id: stream.into(),
            applied_prefix: AppliedIngressPrefixV1 {
                input_stream_id: stream.into(),
                next_input_position: position + 1,
            },
            group_position: position,
            group_id: group_id.clone(),
            closed_at_input_position: position,
        };
        for o in &mut observations {
            o.group_position = position;
            o.group_id = group_id.clone();
            o.visible_prefix = reference.applied_prefix.clone();
        }
        let group = ClosedBusinessGroupV1 {
            group_id,
            group_position: position,
            member_input_positions: vec![position],
            closed_at_input_position: position,
            records: Arc::from([]),
            observations: observations.into(),
        };
        Arc::new(sink.observe(&reference, &group).unwrap().scalars.clone())
    }
    fn records(root: &Path) -> Vec<Value> {
        std::fs::read_to_string(root.join("public.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
    fn asset() -> AssetBusinessObservationV1 {
        let mut book = observed(vec![PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.4".into()),
            quantity_shares: ExactDecimal("7".into()),
        }]);
        book.quality = MarketDataQuality::Fresh;
        book.direct_anchors = (0..256)
            .map(|i| DirectObservationAnchorV1 {
                field_path: format!("bids.{i}"),
                position: PublicFactPositionV1 {
                    input_stream_id: "test".into(),
                    closed_at_input_position: 0,
                    fact_ordinal: i,
                },
                group_id: Sha256Hex("0".repeat(64)),
                source_message_id: Sha256Hex(format!("{i:064x}")),
                source_event_at_utc_ms: Some(1000),
                received_at_utc_ms: 1001,
            })
            .collect();
        book.dependency_frontier = Some(AppliedIngressPrefixV1 {
            input_stream_id: "test".into(),
            next_input_position: 1,
        });
        AssetBusinessObservationV1 {
            asset: AssetIdentityV1 {
                token_id: TokenId("11".into()),
                outcome: OutcomeIdentityV1 {
                    index: 0,
                    label: "Up".into(),
                },
            },
            bids: Arc::new(book.clone()),
            asks: Arc::new(book),
            best_bid_ask: Default::default(),
            valuation_best_bid: Default::default(),
            tick: Default::default(),
            public_trade: Default::default(),
        }
    }
    fn progress(symbol: &str, id: u64) -> SourceStreamProgressV1 {
        SourceStreamProgressV1 {
            scope: SourceStreamScopeV1 {
                source: SourceStreamKindV1::BinanceAggregateTrade,
                condition_id: None,
                asset_or_symbol: symbol.into(),
            },
            last_valid_business_message: None,
            last_observed_message: SourceEvidenceRefV3 {
                source_kind: CanonicalSourceKindV2::BinanceAggtrade,
                source_message_id: Sha256Hex(format!("{id:064x}")),
                source_native_identity: None,
                source_event_at_utc_ms: Some(1000),
                received_at_utc_ms: 1001,
                collector_admission: None,
            },
            connection_generation: None,
            synchronization: SourceSynchronizationV1::Unknown,
            quality: EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
        }
    }
    // 只读消费者：按流累计增量，终点必须是引用的完整公共组身份。
    fn restore_progress(rows: &[Value], group: &Value) -> Vec<SourceStreamProgressV1> {
        let mut values = BTreeMap::new();
        for r in rows.iter().filter(|r| {
            r["record_type"] == "source_progress"
                && r["group"]["input_stream_id"] == group["input_stream_id"]
        }) {
            for scope in r["removed"].as_array().unwrap() {
                values
                    .remove(&serde_json::from_value::<SourceStreamScopeV1>(scope.clone()).unwrap());
            }
            for update in r["updates"].as_array().unwrap() {
                let p: SourceStreamProgressV1 = serde_json::from_value(update.clone()).unwrap();
                values.insert(p.scope.clone(), p);
            }
            if &r["group"] == group {
                return values.into_values().collect();
            }
        }
        panic!("没有找到准确来源进展终点");
    }
    fn resolve<'a>(rows: &'a [Value], kind: &str, group: &Value, condition: &str) -> &'a Value {
        rows.iter()
            .find(|r| {
                r["record_type"] == kind && &r["group"] == group && r["condition_id"] == condition
            })
            .unwrap()
    }
    #[test]
    fn actual_changes_share_sources_and_frozen_links_restore_exactly() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("research");
        let sink = StreamingResearchV1::open(&root).unwrap();
        let mut o = observation(Some("98"));
        o.assets.push(asset());
        let condition = o.market.condition_id.0.clone();
        o.source_progress = vec![progress("BTCUSDT", 1), progress("ETHUSDT", 2)].into();
        let mut other = o.clone();
        other.market.condition_id = ConditionId(format!("0x{}", "2".repeat(64)));
        let initial = deliver_many(&sink, "test", vec![o.clone(), other.clone()], 0);
        sink.finish().unwrap();
        let initial_bytes = std::fs::metadata(root.join("public.jsonl")).unwrap().len();
        // 20次全局进展只改变一个scope；两市场的全部PM来源保持原样。
        for position in 1..=20 {
            o.source_progress =
                vec![progress("BTCUSDT", position + 2), progress("ETHUSDT", 2)].into();
            other.source_progress = o.source_progress.clone();
            let current = deliver_many(&sink, "test", vec![o.clone(), other.clone()], position);
            assert_eq!(
                current[&condition]["pm_group"],
                initial[&condition]["pm_group"]
            );
            assert_eq!(
                current[&condition]["pm_side_sources"],
                initial[&condition]["pm_side_sources"]
            );
        }
        sink.finish().unwrap();
        let progress_only_bytes =
            std::fs::metadata(root.join("public.jsonl")).unwrap().len() - initial_bytes;
        let before_values = deliver(&sink, o.clone(), 21);
        // 仅价变而来源不变。
        Arc::make_mut(&mut o.assets[0].bids).value.as_mut().unwrap()[0].price_pusd_per_share =
            ExactDecimal("0.5".into());
        let value_changed = deliver(&sink, o.clone(), 22);
        assert_eq!(
            before_values[&condition]["pm_side_sources"],
            value_changed[&condition]["pm_side_sources"]
        );
        assert_ne!(
            before_values[&condition]["pm_group"],
            value_changed[&condition]["pm_group"]
        );
        // 仅买侧来源变而最优价不变，不复制未变卖侧数组。
        let bid = Arc::make_mut(&mut o.assets[0].bids);
        bid.direct_anchors[0].received_at_utc_ms = 1010;
        bid.dependency_frontier
            .as_mut()
            .unwrap()
            .next_input_position = 24;
        let source_changed = deliver(&sink, o.clone(), 23);
        assert_eq!(
            value_changed[&condition]["pm_group"],
            source_changed[&condition]["pm_group"]
        );
        assert_ne!(
            value_changed[&condition]["pm_side_sources"][0],
            source_changed[&condition]["pm_side_sources"][0]
        );
        assert_eq!(
            value_changed[&condition]["pm_side_sources"][1],
            source_changed[&condition]["pm_side_sources"][1]
        );
        // 删除scope同样可恢复；另一流相同市场/组序号必须使用独立身份。
        o.source_progress = vec![progress("BTCUSDT", 22)].into();
        let removed = deliver(&sink, o.clone(), 24);
        let second = deliver_many(&sink, "another-day", vec![o.clone()], 0);
        assert_ne!(
            initial[&condition]["pm_group"],
            second[&condition]["pm_group"]
        );
        sink.finish().unwrap();
        let rows = records(&root);
        let pm = resolve(
            &rows,
            "pm_state",
            &source_changed[&condition]["pm_group"],
            &condition,
        );
        assert_eq!(pm["pm"][0]["bid"]["price_pusd_per_share"], "0.5");
        for link in source_changed[&condition]["pm_side_sources"]
            .as_array()
            .unwrap()
        {
            let r = rows
                .iter()
                .find(|r| {
                    r["record_type"] == "pm_side_sources"
                        && r["group"] == link["group"]
                        && r["condition_id"] == condition
                        && r["token_id"] == link["token_id"]
                        && r["side"] == link["side"]
                })
                .unwrap();
            let expected = if link["side"] == "bid" {
                &o.assets[0].bids
            } else {
                &o.assets[0].asks
            };
            assert_eq!(
                r["direct_anchors"],
                serde_json::to_value(&expected.direct_anchors).unwrap()
            );
            assert_eq!(
                r["dependency_frontier"],
                serde_json::to_value(&expected.dependency_frontier).unwrap()
            );
        }
        assert_eq!(
            restore_progress(&rows, &initial[&condition]["source_progress_group"]),
            vec![progress("BTCUSDT", 1), progress("ETHUSDT", 2)]
        );
        assert_eq!(
            restore_progress(&rows, &removed[&condition]["source_progress_group"]),
            vec![progress("BTCUSDT", 22)]
        );
        assert_eq!(
            restore_progress(&rows, &second[&condition]["source_progress_group"]),
            vec![progress("BTCUSDT", 22)]
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["record_type"] == "source_progress")
                .count(),
            23
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["record_type"] == "pm_side_sources")
                .count(),
            7
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["record_type"] == "pm_state")
                .count(),
            4
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["record_type"] == "source_progress"
                    && r["group"]["group_position"].as_u64().unwrap() > 0
                    && r["group"]["group_position"].as_u64().unwrap() <= 20)
                .map(|r| r["updates"].as_array().unwrap().len())
                .sum::<usize>(),
            20
        );
        let public_bytes = std::fs::metadata(root.join("public.jsonl")).unwrap().len();
        let compact_bytes = serde_json::to_vec(initial.as_ref()).unwrap().len();
        println!(
            "finite_growth initial_public_bytes={initial_bytes} progress_only_20_groups_bytes={progress_only_bytes} total_public_bytes={public_bytes} compact_two_market_bytes={compact_bytes} pm_side_writes=7 pm_value_writes=4 progress_writes=23"
        );
        assert!(initial[&condition].get("pm").is_none());
        assert!(initial[&condition].get("source_progress").is_none());
        assert!(
            initial[&condition]["blb"]
                .get("visible_positive_levels")
                .is_none()
        );
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
        assert_ne!(pm[&condition]["pm_group"], early[&condition]["pm_group"]);
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
        assert!(
            records
                .iter()
                .any(|r| r["record_type"] == "pm_state" && r["group"]["group_position"] == 2)
        );
    }
    #[test]
    fn required_public_flush_failure_is_returned() {
        let sink = StreamingResearchV1(Mutex::new(State {
            writer: BufWriter::new(OpenOptions::new().write(true).open("/dev/full").unwrap()),
            markets: Default::default(),
            progress: Default::default(),
            request_prefixes: Default::default(),
        }));
        let _ = deliver(&sink, observation(None), 0);
        assert!(sink.finish().is_err());
    }
}
