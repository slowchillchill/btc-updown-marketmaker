//! 研究最小证据；公共核心和策略仍读取原完整合同。
use backtest::{
    HistoricalOriginalRequestV1, HistoricalPublicEvidenceV1, HistoricalPublicObserverV1,
    HistoricalRequestEvidenceV1, MaterialError,
};
use baseline_mm::blb_research::PreparedBlbDepthV1;
use contracts::*;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub(super) const FORMAT: &str = "minimal-research-v1";
#[derive(Debug)]
struct FrozenBlb {
    depth: Arc<PreparedBlbDepthV1>,
    anchor: ObservedFactV1<MarketPriceAnchorV1>,
    recorded: std::sync::atomic::AtomicBool,
    value: std::sync::OnceLock<Value>,
}
impl HistoricalRequestEvidenceV1 for FrozenBlb {
    fn value(&self) -> Result<Value, MaterialError> {
        Ok(self
            .value
            .get_or_init(|| {
                serde_json::to_value(self.depth.evaluate(&self.anchor, true)).expect("BLB可序列化")
            })
            .clone())
    }
    fn recorded(&self) -> &std::sync::atomic::AtomicBool {
        &self.recorded
    }
}
#[derive(Debug)]
struct MarketCache {
    snapshot: Option<Arc<MarketObservationSnapshotV1>>,
    depth: Arc<ObservedFactV1<BinanceDepthObservationV1>>,
    anchor: ObservedFactV1<MarketPriceAnchorV1>,
    blb: Value,
    blb_group: u64,
    pm: Value,
    pm_group: u64,
    frozen: Arc<FrozenBlb>,
}
#[derive(Debug)]
struct State {
    writer: BufWriter<std::fs::File>,
    markets: BTreeMap<String, MarketCache>,
    prepared: Option<(
        Arc<ObservedFactV1<BinanceDepthObservationV1>>,
        Arc<PreparedBlbDepthV1>,
    )>,
}
/// 根对象只在日期装配时创建流对象，逐组操作永远不获取根锁。
#[derive(Debug)]
pub(super) struct StreamingResearchV1 {
    root: PathBuf,
    state: Mutex<Option<State>>,
    next: std::sync::atomic::AtomicUsize,
}
impl StreamingResearchV1 {
    pub fn open(root: &Path) -> Result<Self, MaterialError> {
        std::fs::create_dir(root)?;
        Ok(Self {
            root: root.to_owned(),
            state: Mutex::new(None),
            next: Default::default(),
        })
    }
    fn stream(&self, stream: &str) -> Result<Self, MaterialError> {
        // 流身份保留于文件首行；装配时分配文件序号，避免用外部身份构造路径。
        let name = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.root.join(format!("public-{name}.jsonl")))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        line(
            &mut writer,
            &json!({"record_type":"format","format":FORMAT,"input_stream_id":stream,
            "interval_coverage":"unknown_no_coverage_evidence",
            "source_age":"latest_direct_received_proxy_not_all_dependencies"}),
        )?;
        Ok(Self {
            root: self.root.clone(),
            state: Mutex::new(Some(State {
                writer,
                markets: Default::default(),
                prepared: None,
            })),
            next: Default::default(),
        })
    }
}
fn line(w: &mut impl Write, v: &Value) -> Result<(), MaterialError> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}
fn latest(anchors: &[DirectObservationAnchorV1]) -> Option<i64> {
    anchors.iter().map(|a| a.received_at_utc_ms).max()
}
fn event_time(anchors: &[DirectObservationAnchorV1]) -> Option<i64> {
    anchors
        .iter()
        .filter_map(|a| a.source_event_at_utc_ms)
        .max()
}
fn fact<T: serde::Serialize>(f: &ObservedFactV1<T>) -> Value {
    json!({"value":f.value,"quality":f.quality,"received_ms":latest(&f.direct_anchors),"source_event_ms":event_time(&f.direct_anchors)})
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
fn pm(assets: &[AssetBusinessObservationV1]) -> Value {
    json!(assets.iter().map(|a|json!({"asset":a.asset,"bid":best(&a.bids.value,true),"ask":best(&a.asks.value,false),
        "bid_quality":a.bids.quality,"ask_quality":a.asks.quality,
        "best_bid_ask":fact(&a.best_bid_ask),"tick":fact(&a.tick)})).collect::<Vec<_>>())
}
impl HistoricalPublicObserverV1 for StreamingResearchV1 {
    fn for_stream(
        &self,
        stream: &str,
    ) -> Result<Option<Arc<dyn HistoricalPublicObserverV1>>, MaterialError> {
        Ok(Some(Arc::new(self.stream(stream)?)))
    }
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1::FULL
    }
    fn observe(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe_inner(reference, group, None)
    }
    fn observe_at(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
        visible_at_utc_ms: i64,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe_inner(reference, group, Some(visible_at_utc_ms))
    }
    fn record_request_evidence(
        &self,
        requests: &[HistoricalOriginalRequestV1],
    ) -> Result<Value, MaterialError> {
        if requests.is_empty() {
            return Ok(json!([]));
        }
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().ok_or("研究观察器尚未绑定输入流")?;
        let mut links = Vec::new();
        for request in requests {
            if !request
                .blb
                .recorded()
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                line(
                    &mut state.writer,
                    &json!({"record_type":"original_pair_blb","evidence":request.value()?}),
                )?;
                request
                    .blb
                    .recorded()
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            links.push(json!({"input_stream_id":request.group.input_stream_id,"group_position":request.group.group_position,"condition_id":request.condition_id,"record_type":"original_pair_blb"}));
        }
        state.writer.flush()?;
        Ok(json!(links))
    }
    fn production_finished(&self) -> Result<(), MaterialError> {
        if let Some(state) = self.state.lock().unwrap().as_mut() {
            line(&mut state.writer, &json!({"record_type":"production_end"}))?;
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), MaterialError> {
        if let Some(state) = self.state.lock().unwrap().as_mut() {
            state.writer.flush()?;
        }
        Ok(())
    }
}

impl StreamingResearchV1 {
    fn observe_inner(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
        visible_at_utc_ms: Option<i64>,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().ok_or("研究观察器尚未绑定输入流")?;
        line(
            &mut state.writer,
            &json!({"record_type":"group","group":reference,
            "visible_at_utc_ms":visible_at_utc_ms,
            "source_event_at_utc_ms":group.records.iter().filter_map(|r|r.direct_time.source_event_at_utc_ms).max()}),
        )?;
        let mut links = serde_json::Map::new();
        let mut request_evidence = BTreeMap::new();
        let mut present = BTreeSet::new();
        let shared = group.observations.shared_parts();
        let ids = if let Some((markets, _)) = shared {
            markets.keys().cloned().collect::<Vec<_>>()
        } else {
            group
                .observations
                .iter()
                .map(|o| o.market.condition_id.0.clone())
                .collect()
        };
        for id in ids {
            present.insert(id.clone());
            let snapshot = shared.and_then(|(m, _)| m.get(&id));
            let common = shared.map(|(_, g)| g);
            let full = if shared.is_none() {
                group.observations.get_market(&id)
            } else {
                None
            };
            let (depth, anchor, assets) = if let (Some(m), Some(g)) = (snapshot, common) {
                (&g.binance_depth, &m.official_anchor, &m.assets)
            } else {
                let o = full.unwrap();
                (&o.binance_depth, &o.official_anchor, &o.assets)
            };
            let old = state.markets.get(&id);
            if old.is_none() {
                let market = if let Some(m) = snapshot {
                    &m.market
                } else {
                    &full.unwrap().market
                };
                line(
                    &mut state.writer,
                    &json!({"record_type":"market","market":market}),
                )?;
            }
            let depth_changed = old.is_none_or(|c| !Arc::ptr_eq(&c.depth, depth));
            let anchor_changed = old.is_none_or(|c| c.anchor != *anchor);
            let snapshot_changed = old.is_none_or(|c| {
                snapshot.is_none_or(|m| c.snapshot.as_ref().is_none_or(|p| !Arc::ptr_eq(p, m)))
            });
            let (blb, blb_group, frozen) = if depth_changed || anchor_changed {
                let prepared = match &state.prepared {
                    Some((prior, p)) if Arc::ptr_eq(prior, depth) => p.clone(),
                    _ => {
                        let p = Arc::new(PreparedBlbDepthV1::new(depth.clone()));
                        state.prepared = Some((depth.clone(), p.clone()));
                        p
                    }
                };
                let mut scalar = serde_json::to_value(prepared.evaluate(anchor, false))?;
                scalar
                    .as_object_mut()
                    .unwrap()
                    .remove("visible_positive_levels");
                let value = json!({"blb":scalar,"official_anchor":fact(anchor),"depth_quality":depth.quality,
                    "depth_received_ms":latest(&depth.direct_anchors),"depth_source_event_ms":event_time(&depth.direct_anchors)});
                let changed = old.is_none_or(|c| c.blb != value);
                let version = if changed {
                    reference.group_position
                } else {
                    old.unwrap().blb_group
                };
                if changed {
                    line(
                        &mut state.writer,
                        &json!({"record_type":"blb_state","group_position":version,"condition_id":id,"state":value}),
                    )?;
                }
                (
                    value,
                    version,
                    Arc::new(FrozenBlb {
                        depth: prepared,
                        anchor: anchor.clone(),
                        recorded: Default::default(),
                        value: Default::default(),
                    }),
                )
            } else {
                let c = old.unwrap();
                (c.blb.clone(), c.blb_group, c.frozen.clone())
            };
            let (pm, pm_group) = if snapshot_changed {
                let value = pm(assets);
                let changed = old.is_none_or(|c| c.pm != value);
                let version = if changed {
                    reference.group_position
                } else {
                    old.unwrap().pm_group
                };
                if changed {
                    line(
                        &mut state.writer,
                        &json!({"record_type":"pm_state","group_position":version,"condition_id":id,"state":value}),
                    )?;
                }
                (value, version)
            } else {
                let c = old.unwrap();
                (c.pm.clone(), c.pm_group)
            };
            links.insert(id.clone(),json!({"input_stream_id":reference.input_stream_id,"blb_group":blb_group,"pm_group":pm_group}));
            request_evidence.insert(
                id.clone(),
                Arc::new(FrozenBlb {
                    depth: frozen.depth.clone(),
                    anchor: frozen.anchor.clone(),
                    recorded: Default::default(),
                    value: Default::default(),
                }) as Arc<dyn HistoricalRequestEvidenceV1>,
            );
            state.markets.insert(
                id,
                MarketCache {
                    snapshot: snapshot.cloned(),
                    depth: depth.clone(),
                    anchor: anchor.clone(),
                    blb,
                    blb_group,
                    pm,
                    pm_group,
                    frozen,
                },
            );
        }
        state.markets.retain(|id, _| present.contains(id));
        Ok(Arc::new(HistoricalPublicEvidenceV1 {
            scalars: Value::Object(links),
            request_evidence,
        }))
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

    fn deliver(
        sink: &StreamingResearchV1,
        mut o: MarketBusinessObservationV1,
        position: u64,
    ) -> Arc<HistoricalPublicEvidenceV1> {
        let stream = o.visible_prefix.input_stream_id.clone();
        let reference = PublicGroupReferenceV1 {
            input_stream_id: stream.clone(),
            applied_prefix: AppliedIngressPrefixV1 {
                input_stream_id: stream,
                next_input_position: position + 1,
            },
            group_position: position,
            group_id: Sha256Hex(format!("{position:064x}")),
            closed_at_input_position: position,
        };
        o.group_id = reference.group_id.clone();
        o.group_position = position;
        o.visible_prefix = reference.applied_prefix.clone();
        let group = ClosedBusinessGroupV1 {
            group_id: reference.group_id.clone(),
            group_position: position,
            member_input_positions: vec![position],
            closed_at_input_position: position,
            records: Arc::from([]),
            observations: vec![o].into(),
        };
        sink.observe(&reference, &group).unwrap()
    }
    fn rows(root: &Path, n: u64) -> Vec<Value> {
        std::fs::read_to_string(root.join(format!("public-{n}.jsonl")))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
    #[test]
    fn consumer_changes_unknowns_and_exact_request_prefixes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e");
        let factory = StreamingResearchV1::open(&root).unwrap();
        let sink = factory.stream("test").unwrap();
        let mut o = observation(Some("98"));
        o.assets.push(asset());
        let id = o.market.condition_id.0.clone();
        let first = deliver(&sink, o.clone(), 0);
        // 非消费的整侧来源变化不增加 PM 正文。
        Arc::make_mut(&mut o.assets[0].bids).direct_anchors[0].received_at_utc_ms = 5000;
        let unchanged = deliver(&sink, o.clone(), 1);
        assert_eq!(first.scalars, unchanged.scalars);
        // 深度来源接收时刻独立变化；数值与质量不变也需记录。
        Arc::make_mut(&mut o.binance_depth).direct_anchors =
            o.assets[0].bids.direct_anchors.clone();
        let source = deliver(&sink, o.clone(), 2);
        assert_ne!(
            unchanged.scalars[&id]["blb_group"],
            source.scalars[&id]["blb_group"]
        );
        // PM来源必须从直接BBA读取，不能取整侧来源。
        o.assets[0].best_bid_ask.direct_anchors = o.assets[0].bids.direct_anchors.clone();
        let bbo = deliver(&sink, o.clone(), 3);
        assert_ne!(
            source.scalars[&id]["pm_group"],
            bbo.scalars[&id]["pm_group"]
        );
        o.official_anchor = Default::default();
        let missing = deliver(&sink, o.clone(), 4);
        assert_ne!(
            bbo.scalars[&id]["blb_group"],
            missing.scalars[&id]["blb_group"]
        );
        sink.finish().unwrap(); // 模拟 producer EOF；更早冻结的真实请求随后到达。
        let group = PublicGroupReferenceV1 {
            input_stream_id: "test".into(),
            applied_prefix: AppliedIngressPrefixV1 {
                input_stream_id: "test".into(),
                next_input_position: 1,
            },
            group_position: 0,
            group_id: Sha256Hex(format!("{:064x}", 0)),
            closed_at_input_position: 0,
        };
        let request = HistoricalOriginalRequestV1 {
            group,
            condition_id: ConditionId(id.clone()),
            blb: first.request_evidence[&id].clone(),
        };
        sink.record_request_evidence(std::slice::from_ref(&request))
            .unwrap();
        sink.record_request_evidence(&[request]).unwrap();
        sink.finish().unwrap();
        let out = rows(&root, 0);
        assert_eq!(
            out.iter()
                .filter(|r| r["record_type"] == "original_pair_blb")
                .count(),
            1
        );
        assert_eq!(
            out.iter()
                .filter(|r| r["record_type"] == "pm_state")
                .count(),
            2
        );
        assert!(
            !serde_json::to_string(&out)
                .unwrap()
                .contains("dependency_frontier")
        );
        let unknown = out
            .iter()
            .find(|r| r["record_type"] == "blb_state" && r["group_position"] == 4)
            .unwrap();
        assert_eq!(unknown["state"]["blb"]["status"], "unknown");
        let original = out
            .iter()
            .find(|r| r["record_type"] == "original_pair_blb")
            .unwrap();
        assert_eq!(original["evidence"]["blb"]["visible_quantity_btc"], "5");
    }
    #[test]
    fn shared_market_transitions_restore_changed_states_and_old_frozen_requests() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e");
        let factory = StreamingResearchV1::open(&root).unwrap();
        let sink = factory.stream("test").unwrap();
        let mut o = observation(Some("98"));
        o.official_anchor.quality = MarketDataQuality::Fresh;
        Arc::make_mut(&mut o.binance_depth).quality = MarketDataQuality::Fresh;
        let id = o.market.condition_id.0.clone();
        let mut up = asset();
        // 有效的非交叉盘口；只在后面的步骤改变质量或删除正量档。
        Arc::make_mut(&mut up.asks).value.as_mut().unwrap()[0].price_pusd_per_share =
            ExactDecimal("0.6".into());
        let mut down = up.clone();
        down.asset = AssetIdentityV1 {
            token_id: TokenId("12".into()),
            outcome: OutcomeIdentityV1 {
                index: 1,
                label: "Down".into(),
            },
        };
        let mut market = Arc::new(MarketObservationSnapshotV1 {
            market: o.market.clone(),
            assets: vec![up, down],
            lifecycle: o.lifecycle.clone(),
            active: o.active.clone(),
            closed: o.closed.clone(),
            accepting_orders: o.accepting_orders.clone(),
            terminal: o.terminal.clone(),
            official_anchor: o.official_anchor.clone(),
            settlement_reference: o.settlement_reference.clone(),
            temporary_reference: o.temporary_reference.clone(),
            effective_reference: o.effective_reference.clone(),
        });
        let emit = |market: &Arc<MarketObservationSnapshotV1>, position: u64| {
            let reference = PublicGroupReferenceV1 {
                input_stream_id: "test".into(),
                applied_prefix: AppliedIngressPrefixV1 {
                    input_stream_id: "test".into(),
                    next_input_position: position + 1,
                },
                group_position: position,
                group_id: Sha256Hex(format!("{position:064x}")),
                closed_at_input_position: position,
            };
            let group = ClosedBusinessGroupV1 {
                group_id: reference.group_id.clone(),
                group_position: position,
                member_input_positions: vec![position],
                closed_at_input_position: position,
                records: Arc::from([]),
                observations: MarketObservationSetV1::shared(
                    [(id.clone(), market.clone())].into_iter().collect(),
                    MarketObservationGroupV1 {
                        schema_version: o.schema_version.clone(),
                        group_id: reference.group_id.clone(),
                        group_position: position,
                        visible_prefix: reference.applied_prefix.clone(),
                        chainlink_spot: Default::default(),
                        chainlink_twap_thirty: Default::default(),
                        chainlink_twap_sixty: Default::default(),
                        binance_depth: o.binance_depth.clone(),
                        binance_trade: Default::default(),
                        group_events: Arc::from([]),
                        source_progress: Default::default(),
                    },
                ),
            };
            let evidence = sink.observe_at(&reference, &group, 1000).unwrap();
            (reference, evidence)
        };
        let mut frozen = vec![emit(&market, 0)];
        // 同一个深度 M=100；仅把正式 K 从98修订为102，方向从bid改为ask。
        Arc::make_mut(&mut market)
            .official_anchor
            .value
            .as_mut()
            .unwrap()
            .price_usd_per_btc = ExactDecimal("102".into());
        frozen.push(emit(&market, 1));
        // 价量与来源时刻完全不变，只改变PM质量，BLB必须继续引用版本1。
        Arc::make_mut(&mut Arc::make_mut(&mut market).assets[0].bids).quality =
            MarketDataQuality::Stale;
        frozen.push(emit(&market, 2));
        // 删光原正量bid，随后变为未知；空和未知不能继续保留0.4/7。
        Arc::make_mut(&mut Arc::make_mut(&mut market).assets[0].bids).value = Some(vec![]);
        frozen.push(emit(&market, 3));
        let bid = Arc::make_mut(&mut Arc::make_mut(&mut market).assets[0].bids);
        bid.value = None;
        bid.quality = MarketDataQuality::UnknownNotObserved;
        frozen.push(emit(&market, 4));
        Arc::make_mut(&mut market).official_anchor = Default::default();
        frozen.push(emit(&market, 5));
        // 同一不可变市场Arc再次交付，走真实共享缓存的未变分支。
        frozen.push(emit(&market, 6));
        sink.finish().unwrap();
        // 旧请求从未预先求值；在全部状态改变后才生成逐档正文。
        let requests = frozen[..2]
            .iter()
            .map(|(group, evidence)| HistoricalOriginalRequestV1 {
                group: group.clone(),
                condition_id: ConditionId(id.clone()),
                blb: evidence.request_evidence[&id].clone(),
            })
            .collect::<Vec<_>>();
        let request_links = sink.record_request_evidence(&requests).unwrap();
        sink.finish().unwrap();
        let out = rows(&root, 0);
        for ((group, evidence), (blb_version, pm_version)) in
            frozen
                .iter()
                .zip([(0, 0), (1, 0), (1, 2), (1, 3), (1, 4), (5, 4), (5, 4)])
        {
            assert!(
                out.iter()
                    .any(|r| r["record_type"] == "group" && r["group"] == json!(group))
            );
            assert_eq!(
                evidence.scalars[&id],
                json!({
                    "input_stream_id":"test","blb_group":blb_version,"pm_group":pm_version
                })
            );
            for (kind, position) in [("blb_state", blb_version), ("pm_state", pm_version)] {
                assert!(out.iter().any(|r| r["record_type"] == kind
                    && r["condition_id"] == id
                    && r["group_position"] == position));
            }
        }
        let resolve = |kind: &str, position: u64| {
            &out.iter()
                .find(|r| {
                    r["record_type"] == kind
                        && r["condition_id"] == id
                        && r["group_position"] == position
                })
                .unwrap()["state"]
        };
        for (position, k, side, notional) in [(0, "98", "bid", "492"), (1, "102", "ask", "508")] {
            let blb = &resolve("blb_state", position)["blb"];
            assert_eq!(blb["official_k_usd_per_btc"], k);
            assert_eq!(blb["midpoint_usdt_per_btc"], "100");
            assert_eq!(blb["side"], side);
            assert_eq!(blb["visible_level_count"], 2);
            assert_eq!(blb["visible_quantity_btc"], "5");
            assert_eq!(blb["visible_notional_usdt"], notional);
        }
        assert_eq!(
            resolve("pm_state", 0)[0]["bid_quality"],
            json!(MarketDataQuality::Fresh)
        );
        assert_eq!(
            resolve("pm_state", 2)[0]["bid_quality"],
            json!(MarketDataQuality::Stale)
        );
        assert_eq!(
            resolve("pm_state", 0)[0]["bid"],
            resolve("pm_state", 2)[0]["bid"]
        );
        assert!(resolve("pm_state", 3)[0]["bid"].is_null());
        assert_eq!(
            resolve("pm_state", 3)[0]["bid_quality"],
            json!(MarketDataQuality::Stale)
        );
        assert!(resolve("pm_state", 4)[0]["bid"].is_null());
        assert_eq!(
            resolve("pm_state", 4)[0]["bid_quality"],
            json!(MarketDataQuality::UnknownNotObserved)
        );
        assert_eq!(resolve("blb_state", 5)["blb"]["status"], "unknown");
        assert!(resolve("blb_state", 5)["blb"]["visible_quantity_btc"].is_null());
        for (index, (k, side, prices)) in
            [("98", "bid", ["99", "98"]), ("102", "ask", ["101", "102"])]
                .into_iter()
                .enumerate()
        {
            let row = out
                .iter()
                .find(|r| {
                    r["record_type"] == "original_pair_blb"
                        && r["evidence"]["group"] == json!(frozen[index].0)
                })
                .unwrap();
            assert_eq!(row["evidence"]["condition_id"], id);
            let blb = &row["evidence"]["blb"];
            assert_eq!(blb["official_k_usd_per_btc"], k);
            assert_eq!(blb["side"], side);
            assert_eq!(
                blb["visible_positive_levels"],
                json!([
                    {"price_usdt_per_btc":prices[0],"quantity_btc":"2"},
                    {"price_usdt_per_btc":prices[1],"quantity_btc":"3"}
                ])
            );
            assert_eq!(request_links[index]["group_position"], index);
            assert_eq!(request_links[index]["input_stream_id"], "test");
        }
        assert_eq!(
            out.iter()
                .filter(|r| r["record_type"] == "blb_state")
                .count(),
            3
        );
        assert_eq!(
            out.iter()
                .filter(|r| r["record_type"] == "pm_state")
                .count(),
            4
        );
    }
    #[test]
    fn independent_stream_progress_and_empty_requests_never_take_writer_lock() {
        let t = tempfile::tempdir().unwrap();
        let factory = StreamingResearchV1::open(&t.path().join("e")).unwrap();
        let a = factory.stream("test").unwrap();
        let b = factory.stream("other").unwrap();
        // 确定性持有 A 的写出锁。空请求与 B 的观察必须都不等待该锁。
        let lock = a.state.lock().unwrap();
        assert_eq!(a.record_request_evidence(&[]).unwrap(), json!([]));
        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();
            s.spawn(move || {
                deliver(&b, observation(Some("98")), 0);
                tx.send(()).unwrap();
            });
            rx.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
        });
        drop(lock);
    }
    #[test]
    fn final_buffer_failure_is_reported() {
        let t = tempfile::tempdir().unwrap();
        let f = StreamingResearchV1::open(&t.path().join("e")).unwrap();
        let sink = f.stream("test").unwrap();
        sink.state.lock().unwrap().as_mut().unwrap().writer =
            BufWriter::new(OpenOptions::new().write(true).open("/dev/full").unwrap());
        deliver(&sink, observation(Some("98")), 0);
        assert!(sink.finish().is_err());
    }
}
