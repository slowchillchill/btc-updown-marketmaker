//! 持续经营的公共报价变化引用；没有 BLB 请求消费者，不计算/保存 BLB 档位。
use super::streaming_research::{line, pm};
use backtest::{
    HistoricalPublicEvidenceV1, HistoricalPublicObserverV1, HistoricalPublicScalarsV1,
    MaterialError,
};
use contracts::*;
use imbl::ordmap::DiffItem;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
#[derive(Debug)]
struct State {
    writer: BufWriter<std::fs::File>,
    markets: BTreeMap<String, (Value, u64)>,
    shared: Option<MarketObservationSnapshotsV1>,
    links: HistoricalPublicScalarsV1,
}
impl State {
    fn project(
        &mut self,
        reference: &PublicGroupReferenceV1,
        market: &MarketIdentityV2,
        assets: &[AssetBusinessObservationV1],
        at: Option<i64>,
    ) -> Result<(), MaterialError> {
        #[cfg(test)]
        PROJECTIONS.with(|n| n.set(n.get() + 1));
        let id = &market.condition_id.0;
        let mut value = pm(assets);
        for (projected, asset) in value
            .as_array_mut()
            .expect("两腿标量")
            .iter_mut()
            .zip(assets)
        {
            projected["bids_materialized"] = json!(asset.bids.value.is_some());
            projected["asks_materialized"] = json!(asset.asks.value.is_some());
        }
        let old = self.markets.get(id);
        let changed = old.is_none_or(|(old, _)| old != &value);
        let version = if changed {
            reference.group_position
        } else {
            old.unwrap().1
        };
        if changed {
            line(
                &mut self.writer,
                &json!({"record_type":"pm_state","group":reference,"visible_at_utc_ms":at,
                "market":market,"version":version,"state":value}),
            )?;
            self.markets.insert(id.clone(), (value, version));
        }
        // 再现市场恢复原历史版本；投影不变且引用仍在时连原映射节点也无需重写。
        if changed || !self.links.contains_key(id) {
            self.links.insert(
                id.clone(),
                json!({"input_stream_id":reference.input_stream_id,"pm_group":version}),
            );
        }
        Ok(())
    }
}
#[cfg(test)]
thread_local! { static PROJECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[derive(Debug)]
pub(super) struct InventoryResearchV1 {
    root: PathBuf,
    state: Mutex<Option<State>>,
    next: AtomicUsize,
}
impl InventoryResearchV1 {
    pub fn open(root: &Path) -> Result<Self, MaterialError> {
        std::fs::create_dir(root)?;
        Ok(Self {
            root: root.into(),
            state: Mutex::new(None),
            next: AtomicUsize::new(0),
        })
    }
    fn observe_inner(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
        at: Option<i64>,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().ok_or("持续经营观察器尚未绑定输入流")?;
        if let Some((markets, _)) = group.observations.shared_parts() {
            if let Some(previous) = state.shared.take() {
                // imbl 跳过共享节点；最坏仍是 O(n)，不把指针相等当成内容变化规则。
                for difference in previous.diff(markets) {
                    match difference {
                        DiffItem::Add(_, market)
                        | DiffItem::Update {
                            new: (_, market), ..
                        } => {
                            state.project(reference, &market.market, &market.assets, at)?;
                        }
                        DiffItem::Remove(id, _) => {
                            state.links.remove(id);
                        }
                    }
                }
            } else {
                state.links.clear();
                for market in markets.values() {
                    state.project(reference, &market.market, &market.assets, at)?;
                }
            }
            state.shared = Some(markets.clone());
        } else {
            // 完整输入没有共享差量保证；合法回退重建当前引用，保留历史投影与版本。
            state.shared = None;
            state.links.clear();
            for market in group.observations.iter() {
                state.project(reference, &market.market, &market.assets, at)?;
            }
        }
        Ok(Arc::new(HistoricalPublicEvidenceV1 {
            scalars: state.links.clone(),
            request_evidence: BTreeMap::new(),
        }))
    }
}
impl HistoricalPublicObserverV1 for InventoryResearchV1 {
    fn for_stream(
        &self,
        stream: &str,
    ) -> Result<Option<Arc<dyn HistoricalPublicObserverV1>>, MaterialError> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.root.join(format!("public-{n}.jsonl")))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        line(
            &mut writer,
            &json!({"record_type":"format","format":"continuous-inventory-public-v1","input_stream_id":stream,
            "blb":"not_requested","interval_coverage":"unknown_no_coverage_evidence"}),
        )?;
        Ok(Some(Arc::new(Self {
            root: self.root.clone(),
            state: Mutex::new(Some(State {
                writer,
                markets: BTreeMap::new(),
                shared: None,
                links: Default::default(),
            })),
            next: AtomicUsize::new(0),
        })))
    }
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1 {
            binance_depth_quantities: false,
        }
    }
    fn observe(
        &self,
        r: &PublicGroupReferenceV1,
        g: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe_inner(r, g, None)
    }
    fn observe_at(
        &self,
        r: &PublicGroupReferenceV1,
        g: &ClosedBusinessGroupV1,
        at: i64,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe_inner(r, g, Some(at))
    }
    fn production_finished(&self) -> Result<(), MaterialError> {
        if let Some(state) = self.state.lock().unwrap().as_mut() {
            line(
                &mut state.writer,
                &json!({"record_type":"public_input_end","meaning":"public producer natural EOF; account economic finality independent"}),
            )?;
            state.writer.flush()?;
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

#[cfg(test)]
mod tests {
    use super::super::streaming_research::tests::{asset, observation};
    use super::*;

    fn root(label: &str) -> (Option<tempfile::TempDir>, PathBuf) {
        let temporary = std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT")
            .is_none()
            .then(|| tempfile::tempdir().unwrap());
        let parent = temporary
            .as_ref()
            .map(|t| t.path().to_path_buf())
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").unwrap())
            });
        let path = parent.join(label);
        std::fs::create_dir_all(&path).unwrap();
        (temporary, path)
    }
    fn market(id: u64) -> Arc<MarketObservationSnapshotV1> {
        let mut o = observation(None);
        o.market.condition_id = ConditionId(format!("0x{id:064x}"));
        o.market.clob_market_id = ClobMarketId(id.to_string());
        o.assets = (0..2)
            .map(|leg| {
                let mut a = asset();
                a.asset.token_id = TokenId((id * 2 + leg).to_string());
                a.asset.outcome.index = leg as u32;
                a.asset.outcome.label = if leg == 0 { "Up" } else { "Down" }.into();
                a
            })
            .collect();
        Arc::new(MarketObservationSnapshotV1 {
            market: o.market,
            assets: o.assets,
            lifecycle: o.lifecycle,
            active: o.active,
            closed: o.closed,
            accepting_orders: o.accepting_orders,
            terminal: o.terminal,
            official_anchor: o.official_anchor,
            settlement_reference: o.settlement_reference,
            temporary_reference: o.temporary_reference,
            effective_reference: o.effective_reference,
        })
    }
    fn group(
        markets: MarketObservationSnapshotsV1,
        position: u64,
        shared: bool,
    ) -> (PublicGroupReferenceV1, ClosedBusinessGroupV1) {
        let o = observation(None);
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
        let mut observations = MarketObservationSetV1::shared(
            markets,
            MarketObservationGroupV1 {
                schema_version: o.schema_version,
                group_id: reference.group_id.clone(),
                group_position: position,
                visible_prefix: reference.applied_prefix.clone(),
                chainlink_spot: Default::default(),
                chainlink_twap_thirty: Default::default(),
                chainlink_twap_sixty: Default::default(),
                binance_depth: o.binance_depth,
                binance_trade: Default::default(),
                group_events: Arc::from([]),
                source_progress: Default::default(),
            },
        );
        if !shared {
            observations = observations.iter().cloned().collect::<Vec<_>>().into();
        }
        let group = ClosedBusinessGroupV1 {
            group_id: reference.group_id.clone(),
            group_position: position,
            member_input_positions: vec![position],
            closed_at_input_position: position,
            records: Arc::from([]),
            observations,
        };
        (reference, group)
    }
    fn bound(path: &Path) -> Arc<dyn HistoricalPublicObserverV1> {
        InventoryResearchV1::open(path)
            .unwrap()
            .for_stream("test")
            .unwrap()
            .unwrap()
    }
    fn output(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path.join("public-0.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn shared_delta_matches_full_projection_with_disappearance_switches_and_old_freeze() {
        let (_temporary, path) = root("inventory-delta");
        let optimized = bound(&path.join("shared"));
        let direct = bound(&path.join("full"));
        let a = market(1);
        let b = market(2);
        let a_id = a.market.condition_id.0.clone();
        let b_id = b.market.condition_id.0.clone();
        let mut markets: MarketObservationSnapshotsV1 =
            [(a_id.clone(), a.clone()), (b_id.clone(), b)]
                .into_iter()
                .collect();
        let mut old = None;
        for step in 0..10 {
            match step {
                // 非PM事实变化必须沿原投影比较，不能无依据生成新pm_state版本。
                2 => {
                    let mut m = markets[&a_id].as_ref().clone();
                    m.active = ObservedFactV1 {
                        value: Some(true),
                        quality: MarketDataQuality::Fresh,
                        ..Default::default()
                    };
                    markets.insert(a_id.clone(), Arc::new(m));
                }
                3 => {
                    let mut m = markets[&a_id].as_ref().clone();
                    Arc::make_mut(&mut m.assets[0].bids).quality = MarketDataQuality::Stale;
                    markets.insert(a_id.clone(), Arc::new(m));
                }
                4 => {
                    markets.remove(&a_id);
                }
                5 => {
                    let mut m = a.as_ref().clone();
                    Arc::make_mut(&mut m.assets[0].bids).quality = MarketDataQuality::Stale;
                    markets.insert(a_id.clone(), Arc::new(m));
                }
                6 => {
                    markets.remove(&a_id);
                }
                7 => {
                    markets.remove(&b_id);
                    let mut m = a.as_ref().clone();
                    Arc::make_mut(&mut m.assets[0].bids).quality = MarketDataQuality::Stale;
                    markets.insert(a_id.clone(), Arc::new(m));
                }
                8 => {
                    markets.clear();
                }
                9 => {
                    markets.insert(a_id.clone(), a.clone());
                }
                _ => {}
            }
            let (r, g) = group(markets.clone(), step, !matches!(step, 6 | 8));
            PROJECTIONS.with(|n| n.set(0));
            let evidence = optimized.observe_at(&r, &g, 10000 + step as i64).unwrap();
            let count = PROJECTIONS.with(|n| n.get());
            let (_, full) = group(markets.clone(), step, false);
            let baseline = direct.observe_at(&r, &full, 10000 + step as i64).unwrap();
            assert_eq!(evidence.scalars, baseline.scalars);
            match step {
                0 => {
                    assert_eq!(count, 2);
                    old = Some(evidence.clone());
                }
                1 => {
                    assert_eq!(count, 0);
                    assert!(evidence.scalars.ptr_eq(&old.as_ref().unwrap().scalars));
                }
                2 => {
                    assert_eq!(count, 1);
                    assert!(evidence.scalars.ptr_eq(&old.as_ref().unwrap().scalars));
                }
                3 | 5 | 7 => {
                    assert_eq!(evidence.scalars[&a_id]["pm_group"], 3);
                }
                4 | 6 | 8 => {
                    assert!(!evidence.scalars.contains_key(&a_id));
                }
                _ => {}
            }
            assert_eq!(old.as_ref().unwrap().scalars[&a_id]["pm_group"], 0);
            assert_eq!(old.as_ref().unwrap().scalars.len(), 2);
        }
        optimized.production_finished().unwrap();
        direct.production_finished().unwrap();
        assert_eq!(output(&path.join("shared")), output(&path.join("full")));
        let rows = output(&path.join("shared"));
        let versions = rows
            .iter()
            .filter(|r| r["record_type"] == "pm_state")
            .map(|r| r["version"].as_u64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(versions, vec![0, 0, 3, 9]);
    }

    #[test]
    fn unchanged_prices_still_preserve_original_quality_source_tick_and_materialization_projection()
    {
        let (_temporary, path) = root("inventory-facts");
        let sink = bound(&path.join("public"));
        let mut m = market(1).as_ref().clone();
        let id = m.market.condition_id.0.clone();
        let mut frozen = vec![];
        for step in 0..7 {
            match step {
                1 => {
                    m.assets[0].tick = ObservedFactV1 {
                        value: Some(ExactDecimal("0.01".into())),
                        quality: MarketDataQuality::Fresh,
                        ..Default::default()
                    };
                }
                2 => {
                    m.assets[0].tick.direct_anchors = m.assets[0].bids.direct_anchors[..1].to_vec();
                }
                3 => {
                    Arc::make_mut(&mut m.assets[0].bids).value = Some(vec![]);
                }
                4 => {
                    m.assets[0].bids = Default::default();
                }
                5 => {
                    m.assets[0].best_bid_ask = ObservedFactV1 {
                        value: Some(CanonicalBbaFactV1 {
                            best_bid_pusd_per_share: Some(ExactDecimal("0.4".into())),
                            best_ask_pusd_per_share: Some(ExactDecimal("0.6".into())),
                            spread_pusd_per_share: Some(ExactDecimal("0.2".into())),
                        }),
                        quality: MarketDataQuality::Fresh,
                        ..Default::default()
                    };
                }
                6 => {
                    m.assets[0].best_bid_ask.direct_anchors =
                        m.assets[0].tick.direct_anchors.clone();
                    m.assets[0].best_bid_ask.direct_anchors[0].received_at_utc_ms = 1002;
                }
                _ => {}
            }
            let (r, g) = group(
                [(id.clone(), Arc::new(m.clone()))].into_iter().collect(),
                step,
                true,
            );
            frozen.push(sink.observe_at(&r, &g, 10000 + step as i64).unwrap());
        }
        sink.finish().unwrap();
        let rows = output(&path.join("public"));
        let pm = rows
            .iter()
            .filter(|r| r["record_type"] == "pm_state")
            .collect::<Vec<_>>();
        assert_eq!(pm.len(), 7);
        for (i, row) in pm.iter().enumerate() {
            assert_eq!(row["version"], i as u64);
            assert_eq!(frozen[i].scalars[&id]["pm_group"], i as u64);
        }
        assert_eq!(pm[0]["state"][0]["bid"]["price_pusd_per_share"], "0.4");
        assert_eq!(pm[1]["state"][0]["bid"], pm[2]["state"][0]["bid"]);
        assert_eq!(pm[2]["state"][0]["tick"]["received_ms"], 1001);
        assert_eq!(pm[3]["state"][0]["bids_materialized"], true);
        assert_eq!(pm[4]["state"][0]["bids_materialized"], false);
        assert!(pm[5]["state"][0]["bid"].is_null());
        assert_eq!(
            pm[5]["state"][0]["best_bid_ask"]["value"]["best_bid_pusd_per_share"],
            "0.4"
        );
        assert_eq!(pm[6]["state"][0]["best_bid_ask"]["received_ms"], 1002);
    }

    #[test]
    #[ignore = "#378 成本例仅在准确签发后定向运行，不随普通回归支用额度"]
    fn cumulative_history_keeps_fixed_current_projection_work_and_reports_finite_cost() {
        let (_temporary, path) = root("inventory-growth");
        for count in [8, 128, 2048] {
            let sink = bound(&path.join(count.to_string()));
            let mut markets = MarketObservationSnapshotsV1::new();
            for i in 1..=count {
                let mut m = market(i).as_ref().clone();
                if i > 1 {
                    m.market.window.start_at_utc_ms = -300000 * i as i64;
                    m.market.window.end_at_utc_ms = m.market.window.start_at_utc_ms + 300000;
                }
                markets.insert(m.market.condition_id.0.clone(), Arc::new(m));
            }
            let (r, g) = group(markets.clone(), 0, true);
            let slow = sink.observe_at(&r, &g, 10000).unwrap();
            let id = format!("0x{:064x}", 1);
            PROJECTIONS.with(|n| n.set(0));
            let started = std::time::Instant::now();
            for p in 1..=100 {
                let mut m = markets[&id].as_ref().clone();
                Arc::make_mut(&mut m.assets[0].bids).value.as_mut().unwrap()[0]
                    .price_pusd_per_share =
                    ExactDecimal(if p % 2 == 0 { "0.4" } else { "0.5" }.into());
                markets.insert(id.clone(), Arc::new(m));
                let (r, g) = group(markets.clone(), p, true);
                let e = sink.observe_at(&r, &g, 10000).unwrap();
                assert_eq!(e.scalars.len(), count as usize);
            }
            let elapsed = started.elapsed();
            assert_eq!(PROJECTIONS.with(|n| n.get()), 100);
            assert_eq!(slow.scalars[&id]["pm_group"], 0);
            sink.finish().unwrap();
            let file = path.join(count.to_string()).join("public-0.jsonl");
            eprintln!(
                "inventory finite: historical_markets={count} groups=100 projected=100 wall_ns={} bytes={} slow_freeze_markets={}",
                elapsed.as_nanos(),
                file.metadata().unwrap().len(),
                slow.scalars.len()
            );
        }
    }
}
