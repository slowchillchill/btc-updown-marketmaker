//! 第一小时真实公共材料的研究累计；共用History和Sampler，不运行策略或账户。
use super::*;

#[derive(Deserialize, Serialize)]
struct HourConfig {
    public_root: PathBuf,
    trial_report_path: PathBuf,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    purpose: String,
}

// 不复制深度/L2或谱系。保留实际最新接收锚作为定位引用，并明确总锚数与省略数。
fn source_summary<T>(f: &ObservedFactV1<T>, at: i64) -> Value {
    let anchor = f.direct_anchors.iter().max_by_key(|a| a.received_at_utc_ms);
    json!({"value_present":f.value.is_some(),"quality":f.quality,"usable_fresh_or_stale":usable(f),
        "received_at_utc_ms":received(f,at),"direct_anchor_count":f.direct_anchors.len(),
        "latest_received_anchor":anchor,"omitted_direct_anchor_count":f.direct_anchors.len().saturating_sub(1)})
}
fn cross_prices(o: &MarketBusinessObservationV1, at: i64) -> Value {
    json!({"spot":{"source":source_summary(&o.chainlink_spot,at),"price_usd_per_btc":o.chainlink_spot.value.as_ref().map(|v|&v.price_usd_per_btc)},
        "binance_trade":{"source":source_summary(&o.binance_trade,at),"price_usdt_per_btc":o.binance_trade.value.as_ref().map(|v|&v.price_usdt_per_btc)},
        "binance_depth":{"source":source_summary(&o.binance_depth,at),
            "best_bid_price_usdt_per_btc":o.binance_depth.value.as_ref().and_then(|v|v.bids.first()).map(|v|&v.price_usdt_per_btc),
            "best_ask_price_usdt_per_btc":o.binance_depth.value.as_ref().and_then(|v|v.asks.first()).map(|v|&v.price_usdt_per_btc)}})
}
fn target_features(o: &MarketBusinessObservationV1, at: i64) -> Value {
    json!({"group_position":o.group_position,"first_visible_at_utc_ms":at,"reference":reference(o),
        "twap_30":source_summary(&o.chainlink_twap_thirty,at),
        "twap_60":source_summary(&o.chainlink_twap_sixty,at),
        "assets":o.assets.iter().map(|a|json!({"asset":a.asset,"bids":source_summary(&a.bids,at),
            "asks":source_summary(&a.asks,at),"tick":a.tick,"valuation_best_bid":a.valuation_best_bid})).collect::<Vec<_>>()})
}
fn window(history: &History, cutoff: i64, seconds: i64) -> Result<Value> {
    let (mut value, pairs) = history.window_with_pairs(cutoff, seconds)?;
    value["hold_coverage"] =
        window_statistics::compact_hold(&value["hold_coverage"], &history.updates);
    value["price_pair_refs"] = json!(pairs);
    Ok(value)
}
struct HourTarget {
    identity: MarketIdentityV2,
    latest: Option<Value>,
    sampler: Sampler,
    boundary: Option<Value>,
    labels: LabelHistory,
    observations: BTreeMap<&'static str, u64>,
}
impl HourTarget {
    fn snapshot(
        &self,
        history: &History,
        global: Option<&MarketBusinessObservationV1>,
        cutoff: i64,
    ) -> Result<Value> {
        let end = self.identity.window.end_at_utc_ms;
        Ok(
            json!({"frozen_visible_cutoff_utc_ms":cutoff,"remaining_seconds":(end-cutoff)/1000,
            "global_history_update_prefix_length":history.updates.len(),"latest_target_features":self.latest,
            "cross_source_prices":global.map(|o|cross_prices(o,cutoff)),
            "settlement_window_hold":window_statistics::compact_hold(&history.integral(end-60_000,cutoff.min(end).max(end-60_000),cutoff)?,&history.updates),
            "descriptive_windows":WINDOWS_SECONDS.iter().map(|&seconds|window(history,cutoff,seconds)).collect::<Result<Vec<_>>>()?}),
        )
    }
    fn freeze_before(
        &mut self,
        at: i64,
        history: &History,
        global: Option<&MarketBusinessObservationV1>,
    ) -> Result<()> {
        // 临时取出既有Sampler，回调只读当前目标与唯一History；没有第二套冻结算法。
        let mut sampler = std::mem::take(&mut self.sampler);
        sampler.before_with(at, self.identity.window.end_at_utc_ms, |cutoff| {
            self.snapshot(history, global, cutoff)
        })?;
        self.sampler = sampler;
        let end = self.identity.window.end_at_utc_ms;
        if self.boundary.is_none() && at >= end {
            self.boundary = Some(self.snapshot(history, global, end)?);
        }
        Ok(())
    }
    fn observe(&mut self, o: &MarketBusinessObservationV1, at: i64, initial: bool) {
        if o.market.condition_id != self.identity.condition_id {
            return;
        }
        self.labels.observe(o, at);
        let section = if initial {
            "initialization"
        } else if at < self.identity.window.start_at_utc_ms {
            "before_window"
        } else if at < self.identity.window.end_at_utc_ms {
            "inside_window"
        } else {
            "at_or_after_window_end"
        };
        *self.observations.entry(section).or_default() += 1;
        if at < self.identity.window.end_at_utc_ms {
            self.latest = Some(target_features(o, at));
        }
    }
    fn finish(self) -> Value {
        json!({"identity":self.identity,"observation_counts":self.observations,"frozen_sampling":self.sampler.frozen,
            "right_boundary_hold_diagnostic":self.boundary,
            "posthoc_label_qualification":{"condition_id":self.identity.condition_id,
                "first_usable_terminal":self.labels.first_usable,"latest_terminal_usable":self.labels.last.as_ref().is_some_and(usable),
                "changes_original_visible_order":self.labels.changes,"labels_not_backfilled_into_features":true}})
    }
}

// 每组的一份完整spot投影；原公共组保证这些全局字段共享，研究不新增拒绝门。
fn observe_group(
    history: &mut History,
    g: &ClosedBusinessGroupV1,
    at: i64,
    initial: bool,
    research: bool,
) -> bool {
    let before = history.updates.len();
    if let Some(o) = g.observations.first() {
        history.observe(o, at, initial, research);
    }
    history.updates.len() != before
}
fn analyze(config: &HourConfig) -> Result<Value> {
    let raw_trial: Value = serde_json::from_slice(&fs::read(&config.trial_report_path)?)?;
    let binding: TrialBinding = serde_json::from_value(raw_trial.clone())?;
    let start = binding.scope.received_start_utc_ms;
    let end = binding.scope.received_end_utc_ms;
    let identities = raw_trial["statistics"]["markets"]
        .as_object()
        .ok_or("原通用统计没有市场目录")?
        .values()
        .filter_map(|m| m["identity"].as_object().map(|_| m["identity"].clone()))
        .map(serde_json::from_value::<MarketIdentityV2>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut targets = identities
        .iter()
        .filter(|m| m.window.start_at_utc_ms >= start && m.window.end_at_utc_ms <= end)
        .cloned()
        .map(|identity| HourTarget {
            identity,
            latest: None,
            sampler: Sampler::default(),
            boundary: None,
            labels: LabelHistory::default(),
            observations: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    targets.sort_by_key(|m| m.identity.window.start_at_utc_ms);
    let slots = (start..end).step_by(300_000).map(|s|json!({"start_at_utc_ms":s,"end_at_utc_ms":s+300_000,
        "actual_condition_ids":targets.iter().filter(|m|m.identity.window.start_at_utc_ms==s && m.identity.window.end_at_utc_ms==s+300_000)
            .map(|m|&m.identity.condition_id).collect::<Vec<_>>()})).collect::<Vec<_>>();
    let actual_prefix: CommittedIngressPrefixV1 = serde_json::from_slice(&fs::read(
        config
            .public_root
            .join(&binding.hidden_preparation.prefix.stable_storage_ref)
            .join("prefix.json"),
    )?)?;
    let source = RecordedPublicGroupSourceV1::open(
        &config.public_root,
        config.read_page_rows,
        config.max_read_batch_bytes,
        OriginalVisibility::default(),
    )?;
    let mut history = History::default();
    let mut latest_global = None;
    let mut counts = BTreeMap::<String, u64>::new();
    let mut widths = BTreeMap::<usize, u64>::new();
    let mut qualities = BTreeMap::<String, u64>::new();
    let mut spot_facts = Vec::new();
    let mut anomalies = 0u64;
    let mut cross_states = Vec::new();
    let mut trade = CrossSource::default();
    let mut depth = CrossSource::default();
    for delivered in source {
        let delivered = delivered.map_err(|e| format!("{e:?}"))?;
        let g = &delivered.prepared.group;
        let at = delivered.delivered_at_utc_ms;
        let initial = !g.member_input_positions.is_empty()
            && g.member_input_positions
                .iter()
                .all(|p| binding.initialization.input_positions.contains(p));
        let research = !initial && at >= start && at < end;
        let section = if initial {
            "initialization"
        } else if at < start {
            "before_scope"
        } else if at < end {
            "research"
        } else {
            "at_or_after_scope_end"
        };
        *counts.entry(format!("{section}/groups")).or_default() += 1;
        *counts.entry(format!("{section}/observations")).or_default() +=
            g.observations.len() as u64;
        *widths.entry(g.observations.len()).or_default() += 1;
        for target in &mut targets {
            target.freeze_before(at, &history, latest_global.as_ref())?;
        }
        // 事实位置在原流中唯一；只遍历一次记录，不另建去重集合或重建quality。
        for fact in g.records.iter() {
            match &fact.payload {
                PublicBusinessPayloadV1::ReferenceFact(
                    CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(price),
                ) => {
                    spot_facts.push(json!({"position":fact.position,"group_position":g.group_position,"first_visible_at_utc_ms":at,
                        "section":section,"value":price,"direct_time":fact.direct_time,
                        "source_message_ids":fact.direct_sources.iter().map(|s|&s.source_message_id).collect::<Vec<_>>()}));
                }
                PublicBusinessPayloadV1::ReferenceFact(
                    CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(_),
                ) => {
                    anomalies += 1;
                }
                _ => {}
            }
        }
        // 右边界以半开范围保护冻结历史；范围外组仍计读取规模和同condition标签。
        if initial || at < end {
            let changed = observe_group(&mut history, g, at, initial, research);
            if let Some(o) = g.observations.first() {
                if research {
                    *qualities
                        .entry(serde_json::to_string(&o.chainlink_spot.quality)?)
                        .or_default() += 1;
                }
                latest_global = Some(o.clone());
                if changed && research {
                    trade.observe(&o.chainlink_spot, &o.binance_trade, at);
                    depth.observe(&o.chainlink_spot, &o.binance_depth, at);
                }
                if changed {
                    cross_states.push(json!({"spot_update_index":history.updates.len()-1,"group_position":g.group_position,
                    "first_visible_at_utc_ms":at,"initialization_source_group":initial,"prices":cross_prices(o,at)}));
                }
            }
        }
        for o in &g.observations {
            for target in &mut targets {
                target.observe(o, at, initial);
            }
        }
    }
    for target in &mut targets {
        target.freeze_before(i64::MAX, &history, latest_global.as_ref())?;
    }
    let global_window = window(&history, end, (end - start) / 1000)?;
    let global_hold =
        window_statistics::compact_hold(&history.integral(start, end, end)?, &history.updates);
    Ok(
        json!({"schema":"complementary_pair_first_fit_hour_v1","effective_config":config,
        "scope":{"received_start_utc_ms":start,"received_end_utc_ms":end},"input_set_identity_sha256":binding.input_set_identity_sha256,
        "source_identity_sha256":binding.source_identity_sha256,"normal_prefix":actual_prefix,
        "prepared_expected_normal_prefix":binding.hidden_preparation.prefix,"actual_market_catalog":identities,
        "expected_five_minute_slots":slots,"counts":counts,"observations_per_group":widths,
        "global_spot":{"updates_original_visible_order":history.updates,"unique_public_chainlink_facts":spot_facts,
            "distinct_observed_source_ids_including_initialization":history.seen.len(),"repeated_research_group_observations":history.repeated_observations,
            "quality_group_denominators":qualities,"rtds_anomaly_fact_count":anomalies,"adjacent_pair_description":global_window,
            "hour_hold_approximation":global_hold},"cross_source_at_spot_state_updates":cross_states,
        "cross_source_spot_update_descriptions":{"spot_and_trade":trade,"spot_and_depth":depth},
        "markets":targets.into_iter().map(HourTarget::finish).collect::<Vec<_>>(),
        "method_note":"每完整组最多一份完整spot观察形成唯一History；事实位置另按原records一次计数。交叉来源小投影按spot状态变化及冻结截止保存，最新接收锚/省略数明确，不存完整L2/谱系，不声称覆盖所有Binance独立创新。标签另存不参与特征。"}),
    )
}
pub(super) fn run(config_path: &str, output_path: &str) -> Result<()> {
    let config: HourConfig = serde_json::from_slice(&fs::read(config_path)?)?;
    let before = fs::read_to_string("/proc/self/io")?;
    let began = Instant::now();
    let mut report = analyze(&config)?;
    report["measurement"] = json!({"wall_seconds":began.elapsed().as_secs_f64(),"proc_io_before":before,
        "proc_io_after":fs::read_to_string("/proc/self/io")?,"proc_status_after":fs::read_to_string("/proc/self/status")?});
    fs::write(output_path, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{fixture, spot};
    fn target(o: &MarketBusinessObservationV1) -> HourTarget {
        HourTarget {
            identity: o.market.clone(),
            latest: None,
            sampler: Sampler::default(),
            boundary: None,
            labels: LabelHistory::default(),
            observations: BTreeMap::new(),
        }
    }
    #[test]
    fn many_market_projections_feed_one_history_and_empty_groups_feed_none() {
        let mut g = fixture();
        let mut o = g.observations[0].clone();
        o.chainlink_spot = spot(1, 1000, "100");
        let mut other = o.clone();
        other.market.condition_id = ConditionId(format!("0x{}", "2".repeat(64)));
        g.observations = vec![o.clone(), other, o.clone()];
        let mut a = History::default();
        assert!(observe_group(&mut a, &g, 1100, false, true));
        assert!(!observe_group(&mut a, &g, 1200, false, true));
        assert_eq!(a.updates.len(), 1);
        assert_eq!(a.repeated_observations, 1);
        g.observations.reverse();
        let mut b = History::default();
        observe_group(&mut b, &g, 1100, false, true);
        assert_eq!(
            serde_json::to_value(&a.updates).unwrap(),
            serde_json::to_value(&b.updates).unwrap()
        );
        g.observations.clear();
        assert!(!observe_group(&mut a, &g, 1300, false, true));
        assert_eq!(a.repeated_observations, 1);
    }
    #[test]
    fn complete_quality_and_anchors_are_preserved_and_anomaly_does_not_break_prior_price() {
        let mut g = fixture();
        g.observations.truncate(1);
        g.observations[0].chainlink_spot = ObservedFactV1::default();
        let mut h = History::default();
        observe_group(&mut h, &g, 0, true, false);
        assert_eq!(h.updates[0].quality, MarketDataQuality::UnknownNotObserved);
        g.observations[0].chainlink_spot = spot(1, 1000, "100");
        observe_group(&mut h, &g, 1100, false, true);
        // 原core的RTDS anomaly代表测试保证输出完整spot不变；研究消费相同投影不得人为断对。
        assert!(!observe_group(&mut h, &g, 1500, false, true));
        g.observations[0].chainlink_spot.quality = MarketDataQuality::Stale;
        assert!(observe_group(&mut h, &g, 1600, false, true));
        g.observations[0].chainlink_spot.direct_anchors[0].received_at_utc_ms = 900;
        assert!(observe_group(&mut h, &g, 1700, false, true));
        assert_eq!(h.updates.last().unwrap().received_at_utc_ms, 900);
        assert_eq!(h.integral(1000, 2000, 2000).unwrap()["covered_ms"], 1000);
        g.observations[0].chainlink_spot = ObservedFactV1::default();
        observe_group(&mut h, &g, 1800, false, true);
        assert_eq!(h.integral(1000, 2000, 2000).unwrap()["missing_ms"], 200);
    }
    #[test]
    fn global_history_freezes_whole_same_millisecond_group_and_boundary_excludes_later_label() {
        let mut g = fixture();
        g.observations.truncate(1);
        g.observations[0].market.window = MarketWindowV2 {
            start_at_utc_ms: 0,
            end_at_utc_ms: 300_000,
        };
        g.observations[0].terminal = ObservedFactV1::default();
        g.observations[0].chainlink_spot = spot(1, 1000, "100");
        let mut t = target(&g.observations[0]);
        let mut h = History::default();
        t.freeze_before(1100, &h, None).unwrap();
        observe_group(&mut h, &g, 1100, false, true);
        t.observe(&g.observations[0], 1100, false);
        t.freeze_before(60_000, &h, Some(&g.observations[0]))
            .unwrap();
        assert_eq!(t.sampler.frozen.len(), 1);
        g.observations[0].chainlink_spot = spot(2, 59_000, "110");
        observe_group(&mut h, &g, 60_000, false, true);
        t.observe(&g.observations[0], 60_000, false);
        t.freeze_before(60_001, &h, Some(&g.observations[0]))
            .unwrap();
        assert_eq!(
            t.sampler.frozen[1]["global_history_update_prefix_length"],
            2
        );
        let frozen = t.sampler.frozen[1].clone();
        t.freeze_before(300_000, &h, Some(&g.observations[0]))
            .unwrap();
        let boundary = t.boundary.clone();
        g.observations[0].chainlink_spot = spot(3, 59_500, "999");
        observe_group(&mut h, &g, 300_001, false, true);
        t.observe(&g.observations[0], 300_001, false);
        t.freeze_before(300_002, &h, Some(&g.observations[0]))
            .unwrap();
        assert_eq!(t.sampler.frozen[1], frozen);
        assert_eq!(t.boundary, boundary);
    }
}
