//! 本例的多scope统计；公共来源事实/健康判断继续复用原实现。
use super::*;
use data_pipeline_core::public_observation_core::PublicAppliedIngressV1;
use std::collections::{BTreeSet, HashSet};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ScopeWindow {
    label: String,
    scope: SourceStreamScopeV1,
    window: MarketWindowV2,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct TargetMarket {
    condition_id: ConditionId,
    window: MarketWindowV2,
}
fn contains(w: &MarketWindowV2, at: i64) -> bool {
    w.start_at_utc_ms <= at && at < w.end_at_utc_ms
}
#[derive(Default, Serialize)]
struct Reads {
    reads: u64,
    state_changes: u64,
    unchanged_projection_reads: u64,
    observed_message_identity_changes: u64,
    repeated_incoming_observed_identity: u64,
    valid_business_identity_updates: u64,
    valid_business_identity_changes: u64,
    repeated_incoming_valid_identity: u64,
    state_changes_without_valid_identity_update: u64,
    synchronization_required_reads: u64,
    connection_generation_unknown_reads: u64,
    synchronization_unknown_reads: u64,
    no_progress_reads: u64,
    new_valid_missing_event: u64,
    new_valid_event_after_receive: u64,
    new_valid_receive_after_frontier: u64,
}
struct Stats {
    spec: ScopeWindow,
    reads: Reads,
    update_buckets: BTreeMap<String, u64>,
    previous: Option<SourceStreamProgressV1>,
    seen_valid_ids: HashSet<String>,
    previous_valid: Option<(SourceEvidenceRefV3, i64)>,
    previous_scalar: Option<Value>,
    scalar_comparisons: u64,
    same_scalar: u64,
    same_ms: u64,
    cross_window_pairs: u64,
    first: Option<Value>,
    last: Option<Value>,
    before_support: Option<Value>,
    receive: Series,
    lag: Series,
    age: Series,
    segments: Vec<Segment>,
    longest: Vec<(u64, Value)>,
}
impl Stats {
    fn new(spec: ScopeWindow) -> Self {
        let start = spec.window.start_at_utc_ms;
        Self {
            spec,
            reads: Reads::default(),
            update_buckets: BTreeMap::new(),
            previous: None,
            seen_valid_ids: HashSet::new(),
            previous_valid: None,
            previous_scalar: None,
            scalar_comparisons: 0,
            same_scalar: 0,
            same_ms: 0,
            cross_window_pairs: 0,
            first: None,
            last: None,
            before_support: None,
            receive: Series::default(),
            lag: Series::default(),
            age: Series::default(),
            segments: vec![Segment {
                visible_at_utc_ms: start,
                facts: None,
            }],
            longest: vec![],
        }
    }
    fn observe(
        &mut self,
        p: Option<&SourceStreamProgressV1>,
        now: i64,
        incoming: Option<&SourceEvidenceRefV3>,
        input_position: u64,
        applied: &PublicAppliedIngressV1<'_>,
    ) {
        self.reads.reads += 1;
        let changed = p != self.previous.as_ref();
        self.reads.state_changes += u64::from(changed);
        self.reads.unchanged_projection_reads += u64::from(!changed);
        self.reads.no_progress_reads += u64::from(p.is_none());
        let prior = self.previous.as_ref();
        if let Some(p) = p {
            self.reads.observed_message_identity_changes += u64::from(prior.is_none_or(|old| {
                old.last_observed_message.source_message_id
                    != p.last_observed_message.source_message_id
            }));
            self.reads.repeated_incoming_observed_identity +=
                u64::from(incoming.is_some_and(|e| {
                    prior.is_some_and(|old| {
                        old.last_observed_message.source_message_id == e.source_message_id
                    })
                }));
            self.reads.synchronization_required_reads += u64::from(
                p.synchronization == SourceSynchronizationV1::SnapshotRequired
                    || p.quality.resync == ResyncQuality::Required,
            );
            self.reads.connection_generation_unknown_reads +=
                u64::from(p.connection_generation.is_none());
            self.reads.synchronization_unknown_reads +=
                u64::from(p.synchronization == SourceSynchronizationV1::Unknown);
        }
        let old_id = prior
            .and_then(|p| p.last_valid_business_message.as_ref())
            .map(|e| &e.source_message_id);
        self.reads.repeated_incoming_valid_identity += u64::from(
            incoming.is_some_and(|e| self.seen_valid_ids.contains(&e.source_message_id.0)),
        );
        let identity_change = p
            .and_then(|p| p.last_valid_business_message.as_ref())
            .filter(|e| Some(&e.source_message_id) != old_id);
        self.reads.valid_business_identity_changes += u64::from(identity_change.is_some());
        let update =
            identity_change.filter(|e| self.seen_valid_ids.insert(e.source_message_id.0.clone()));
        self.reads.valid_business_identity_updates += u64::from(update.is_some());
        self.reads.state_changes_without_valid_identity_update +=
            u64::from(changed && update.is_none());
        if let Some(e) = update {
            self.reads.new_valid_missing_event += u64::from(e.source_event_at_utc_ms.is_none());
            self.reads.new_valid_event_after_receive += u64::from(
                e.source_event_at_utc_ms
                    .is_some_and(|t| t > e.received_at_utc_ms),
            );
            self.reads.new_valid_receive_after_frontier += u64::from(e.received_at_utc_ms > now);
            let w = &self.spec.window;
            let in_window = contains(w, now) && contains(w, e.received_at_utc_ms);
            let bucket = if now < w.start_at_utc_ms {
                "before_window"
            } else if now >= w.end_at_utc_ms {
                "after_window"
            } else if in_window {
                "in_window"
            } else {
                "visible_in_window_receive_outside"
            };
            *self.update_buckets.entry(bucket.into()).or_default() += 1;
            let evidence = json!({"input_position":input_position,"source_message_id":e.source_message_id,
                "received_at_utc_ms":e.received_at_utc_ms,"source_event_at_utc_ms":e.source_event_at_utc_ms,"visible_frontier_utc_ms":now});
            let consistent = e.received_at_utc_ms <= now
                && e.source_event_at_utc_ms
                    .is_none_or(|t| t <= e.received_at_utc_ms);
            if now < w.start_at_utc_ms {
                self.before_support = Some(evidence.clone());
            }
            let scalar = minimal_scalar(&self.spec.scope, e, applied);
            if in_window {
                self.first.get_or_insert_with(|| evidence.clone());
                self.last = Some(evidence.clone());
                if let Some(value) = &scalar
                    && let Some((previous, visible)) = &self.previous_valid
                    && contains(w, previous.received_at_utc_ms)
                    && contains(w, *visible)
                    && self.previous_scalar.is_some()
                {
                    self.scalar_comparisons += 1;
                    self.same_scalar += u64::from(self.previous_scalar.as_ref() == Some(value));
                }
                self.lag.add(
                    e.source_event_at_utc_ms
                        .map(|t| i128::from(e.received_at_utc_ms) - i128::from(t)),
                    consistent,
                );
                self.age.add(
                    e.source_event_at_utc_ms
                        .map(|t| i128::from(now) - i128::from(t)),
                    consistent,
                );
            }
            if let Some((left, visible)) = &self.previous_valid {
                if in_window && contains(w, left.received_at_utc_ms) && contains(w, *visible) {
                    let gap =
                        i128::from(e.received_at_utc_ms) - i128::from(left.received_at_utc_ms);
                    self.same_ms += u64::from(gap == 0);
                    let coherent = consistent
                        && left.received_at_utc_ms <= *visible
                        && left
                            .source_event_at_utc_ms
                            .is_none_or(|t| t <= left.received_at_utc_ms);
                    self.receive.add(Some(gap), coherent);
                    if gap >= 0 && coherent {
                        self.longest.push((gap as u64,json!({"left":left,"left_visible_at_utc_ms":visible,"right":evidence,"interval_ms":gap})));
                        self.longest
                            .sort_unstable_by_key(|p| std::cmp::Reverse(p.0));
                        self.longest.truncate(5);
                    }
                } else if (*visible).min(now) < w.end_at_utc_ms
                    && (*visible).max(now) >= w.start_at_utc_ms
                {
                    self.cross_window_pairs += 1;
                }
            }
            self.previous_scalar = scalar;
            self.previous_valid = Some((e.clone(), now));
        }
        if now != i64::MIN {
            let facts = p.map(SourceHealthFactsV1::from);
            let at = now.clamp(
                self.spec.window.start_at_utc_ms,
                self.spec.window.end_at_utc_ms,
            );
            let last = self.segments.last_mut().expect("初始未知区间");
            if last.visible_at_utc_ms == at {
                last.facts = facts;
            } else if last.facts != facts {
                self.segments.push(Segment {
                    visible_at_utc_ms: at,
                    facts,
                });
            }
        }
        self.previous = p.cloned();
    }
    fn finish(mut self, index: usize, output: &Path) -> Result<Value> {
        let (receive, lag, age) = (
            self.receive.summary(),
            self.lag.summary(),
            self.age.summary(),
        );
        let mut silences: Vec<_> = quantiles(&self.receive.values)
            .values()
            .flatten()
            .copied()
            .collect();
        silences.sort_unstable();
        silences.dedup();
        let mut delays: Vec<_> = quantiles(&self.age.values)
            .values()
            .flatten()
            .copied()
            .collect();
        delays.sort_unstable();
        delays.dedup();
        let mut candidates = Vec::new();
        for silence in silences {
            for delay in &delays {
                let (overall, days) = coverage(
                    &self.segments,
                    self.spec.window.start_at_utc_ms,
                    self.spec.window.end_at_utc_ms,
                    silence,
                    *delay,
                )?;
                candidates.push(json!({"receive_silence_ms":silence,"event_delay_ms":delay,
                "receive_interval_at_or_above":self.receive.values.iter().filter(|v|**v>=silence).count(),"receive_interval_denominator":self.receive.values.len(),
                "receive_interval_exceed_fraction":(!self.receive.values.is_empty()).then(||self.receive.values.iter().filter(|v|**v>=silence).count() as f64/self.receive.values.len() as f64),
                "event_age_at_or_above":self.age.values.iter().filter(|v|**v>=*delay).count(),"event_age_denominator":self.age.values.len(),
                "event_age_exceed_fraction":(!self.age.values.is_empty()).then(||self.age.values.iter().filter(|v|**v>=*delay).count() as f64/self.age.values.len() as f64),"overall":overall,"daily_by_utc_start_ms":days}));
            }
        }
        let mut writer = BufWriter::new(fs::File::create(
            output.join(format!("scope-{index}-segments.jsonl")),
        )?);
        for s in &self.segments {
            serde_json::to_writer(&mut writer, s)?;
            writeln!(writer)?;
        }
        writer.flush()?;
        // 候选不可形成仍披露原政策中不随阈值改变的不可得/异常；MAX只是分类探针，不是候选。
        let (invariant, _) = coverage(
            &self.segments,
            self.spec.window.start_at_utc_ms,
            self.spec.window.end_at_utc_ms,
            i64::MAX as u64,
            i64::MAX as u64,
        )?;
        let early_failures: BTreeMap<_, _> = invariant
            .first_failure_ms
            .into_iter()
            .filter(|(reason, _)| {
                matches!(
                    reason.as_str(),
                    "UnknownBusinessProgress"
                        | "SynchronizationRequired"
                        | "Unavailable"
                        | "TimeInconsistent"
                )
            })
            .collect();
        let scalar_method = match self.spec.scope.source {
            SourceStreamKindV1::ChainlinkSpot
            | SourceStreamKindV1::ChainlinkTwapThirty
            | SourceStreamKindV1::ChainlinkTwapSixty => {
                "price_usd_per_btc字段表示相同；真实新消息仍分别计数"
            }
            SourceStreamKindV1::BinanceAggregateTrade => {
                "price_usdt_per_btc/quantity_btc/buyer_is_maker三标量相同；不是同一成交，不合并aggregate_trade_id"
            }
            _ => "未测完整盘口等值；不以顶价或部分盘口相同冒称整簿不变；原归约Confirm另列",
        };
        Ok(
            json!({"scope_window":self.spec,"reads":self.reads,"valid_update_buckets":self.update_buckets,
            "receive_interval":receive,"receive_to_source_event_lag":lag,"visible_frontier_to_event_age":age,
            "same_minimal_value_method":scalar_method,"minimal_value_comparison_denominator":self.scalar_comparisons,
            "same_minimal_value_updates":self.same_scalar,"same_millisecond_received_intervals":self.same_ms,
            "cross_window_pairs_excluded":self.cross_window_pairs,"before_window_last_confirmation_support":self.before_support,
            "first_in_window_confirmation":self.first,"last_in_window_confirmation":self.last,
            "unconfirmed_tail_ms":self.last.as_ref().and_then(|v|v["visible_frontier_utc_ms"].as_i64()).map(|t|self.spec.window.end_at_utc_ms-t),
            "unconfirmed_tail_after_last_in_window_receive_ms":self.last.as_ref().and_then(|v|v["received_at_utc_ms"].as_i64()).map(|t|self.spec.window.end_at_utc_ms-t),
            "no_in_window_confirmation_duration_ms":self.last.is_none().then_some(self.spec.window.end_at_utc_ms-self.spec.window.start_at_utc_ms),
            "segments":self.segments.len(),"invariant_unavailable_failure_ms":early_failures,
            "event_time_missing_evidence_ms":invariant.event_time_missing_evidence_ms,
            "unavailable_method":"只披露原判断中先于期限分支的未知业务/同步必要/不可用/时间异常；缺event独立范围单列，不把UnknownEventTime视为恒定互斥类别，候选首失败仍按原静默优先级",
            "window_ms":invariant.window_ms,"candidates":candidates,"candidate_status":if self.receive.values.is_empty()||self.age.values.is_empty(){"insufficient_samples"}else{"descriptive_only"}}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use data_pipeline_core::{public_input::*, typed_raw::*};
    use data_pipeline_training::public_recording::PublicRecordingWriterV1;
    fn pm(position: u64, epoch: &str, seq: u64, received: i64, token: &str) -> RecordedIngressV1 {
        let mut input = common::pm(
            position,
            seq,
            received - 1000,
            PolymarketOrderbookEvent::Book {
                asset_id: token.into(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.5".into(),
                    size: "100".into(),
                }],
                asks: vec![PolymarketPriceLevel {
                    price: "0.6".into(),
                    size: "100".into(),
                }],
                last_trade_price: None,
            },
        );
        if let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
            ..
        } = &mut input.ingress
        {
            row.source_sequence_epoch_id = epoch.into();
            row.timestamp_received = received;
            row.collector_admission_time_utc_ms = received + 1;
            // 正式record入口重建来源身份，不能修改row后沿用旧source_evidence。
            return common::record(
                position,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row.clone())),
            );
        }
        unreachable!()
    }
    fn spot(position: u64, received: i64, event: i64) -> RecordedIngressV1 {
        let mut row = common::reference_after(received, position).common;
        row.topic = "crypto_prices_chainlink".into();
        row.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
        row.timestamp_received = received;
        row.payload_timestamp = event;
        row.message_timestamp = received;
        row.age_ms = Some(received - event);
        common::record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row)),
        )
    }
    fn window() -> MarketWindowV2 {
        MarketWindowV2 {
            start_at_utc_ms: 22000,
            end_at_utc_ms: 22500,
        }
    }
    fn fixture(root: &Path) -> Config {
        let mut core = common::core();
        let mut writer =
            PublicRecordingWriterV1::create(root, core.header(), common::config()).unwrap();
        let mut other = common::meta(1);
        if let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row)),
            ..
        } = &mut other.ingress
        {
            row.condition_id = format!("0x{}", "2".repeat(64));
            row.clob_market_id = "3195577".into();
            row.asset_ids = "[\"30\",\"40\"]".into();
            other = common::record(
                1,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row.clone())),
            );
        }
        let duplicate = spot(5, 22000, 21000);
        let mut retry = duplicate.clone();
        retry.input_position = 6;
        let mut inputs = vec![
            common::meta(0),
            other,
            spot(2, 21000, 20000),
            pm(3, "epoch-a", 1, 22000, common::UP_TOKEN_ID),
            pm(4, "epoch-a", 2, 22000, common::DOWN_TOKEN_ID),
            duplicate,
            retry,
            pm(7, "epoch-b", 1, 22100, common::UP_TOKEN_ID),
            spot(8, 22200, 21100),
            spot(9, 22200, 21101),
            {
                let mut old = spot(5, 22000, 21000);
                old.input_position = 10;
                old
            },
            pm(11, "epoch-b", 2, 22300, common::DOWN_TOKEN_ID),
            spot(12, 22600, 22400),
        ];
        inputs.push(RecordedIngressV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position: 13,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        });
        for input in inputs {
            let applied = core
                .push(&prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap())
                .unwrap()
                .into_owned();
            writer.record(input, applied).unwrap();
        }
        let prefix = writer.stabilize().unwrap().unwrap();
        let capture = root
            .join(&prefix.stable_storage_ref)
            .join("captured.parquet");
        let pm_scope = |token: &str| SourceStreamScopeV1 {
            source: SourceStreamKindV1::PolymarketBook,
            condition_id: Some(ConditionId(common::CONDITION_ID.into())),
            asset_or_symbol: token.into(),
        };
        let global = |kind| SourceStreamScopeV1 {
            source: kind,
            condition_id: None,
            asset_or_symbol: "btc/usd".into(),
        };
        Config {
            public_root: root.into(),
            prefix,
            received_start_utc_ms: 22000,
            received_end_utc_ms: 22700,
            expected_capture_bytes: fs::metadata(&capture).unwrap().len(),
            expected_capture_sha256: hash(&capture).unwrap(),
            read_page_rows: 2,
            max_read_batch_bytes: 1 << 20,
            progress_interval_seconds: 0,
            selected_source_identity_sha256: "synthetic".into(),
            selection_evidence: json!([]),
            implementation_identity: json!("synthetic"),
            scopes: vec![
                ScopeWindow {
                    label: "up".into(),
                    scope: pm_scope(common::UP_TOKEN_ID),
                    window: window(),
                },
                ScopeWindow {
                    label: "down".into(),
                    scope: pm_scope(common::DOWN_TOKEN_ID),
                    window: window(),
                },
                ScopeWindow {
                    label: "spot".into(),
                    scope: global(SourceStreamKindV1::ChainlinkSpot),
                    window: window(),
                },
                ScopeWindow {
                    label: "twap30".into(),
                    scope: global(SourceStreamKindV1::ChainlinkTwapThirty),
                    window: window(),
                },
            ],
            target_markets: vec![TargetMarket {
                condition_id: ConditionId(common::CONDITION_ID.into()),
                window: window(),
            }],
            initialization_input_positions: vec![0, 1],
        }
    }
    #[test]
    fn original_multimarket_record_is_read_once_per_scope_and_epoch_does_not_refresh_other_asset() {
        let temp = tempfile::tempdir().unwrap();
        let c = fixture(&temp.path().join("public"));
        let r = replay(&c, temp.path()).unwrap();
        let scopes = r["scopes"].as_array().unwrap();
        let (up, down, spot, twap) = (&scopes[0], &scopes[1], &scopes[2], &scopes[3]);
        assert_eq!(r["final_core_object_counts"]["markets"], 2);
        assert_eq!(up["reads"]["valid_business_identity_updates"], 2);
        assert_eq!(down["reads"]["valid_business_identity_updates"], 2);
        assert_eq!(
            down["reads"]["state_changes_without_valid_identity_update"],
            1
        );
        assert_eq!(
            down["invariant_unavailable_failure_ms"]["SynchronizationRequired"],
            200
        );
        assert_eq!(spot["reads"]["valid_business_identity_updates"], 5);
        assert_eq!(spot["reads"]["repeated_incoming_observed_identity"], 1);
        assert_eq!(spot["reads"]["repeated_incoming_valid_identity"], 2);
        assert_eq!(spot["reads"]["valid_business_identity_changes"], 6);
        assert_eq!(spot["valid_update_buckets"]["in_window"], 3);
        assert_eq!(spot["receive_interval"]["valid_nonnegative_samples"], 2);
        assert_eq!(spot["receive_interval"]["quantiles_ms"]["p50"], 0);
        assert_eq!(
            spot["before_window_last_confirmation_support"]["received_at_utc_ms"],
            21000
        );
        assert_eq!(spot["same_minimal_value_updates"], 2);
        assert_eq!(spot["same_millisecond_received_intervals"], 1);
        assert_eq!(twap["candidate_status"], "insufficient_samples");
        assert!(twap["candidates"].as_array().unwrap().is_empty());
        assert_eq!(
            twap["invariant_unavailable_failure_ms"]["UnknownBusinessProgress"],
            500
        );
        assert_eq!(
            r["target_observation_counts"][common::CONDITION_ID]["in_target_window"],
            9
        );
        assert_eq!(r["group_buckets"]["initialization"]["observations"], 3);
        assert_eq!(r["group_buckets"]["analysis"]["observations"], 18);
        assert_eq!(r["group_buckets"]["tail"]["observations"], 2);
        for s in scopes {
            for c in s["candidates"].as_array().unwrap() {
                assert_eq!(c["overall"]["window_ms"], 500);
            }
        }
    }
}
fn minimal_scalar(
    scope: &SourceStreamScopeV1,
    e: &SourceEvidenceRefV3,
    applied: &PublicAppliedIngressV1<'_>,
) -> Option<Value> {
    for group in &applied.applied.closed_groups {
        for f in group.records.iter() {
            if !f
                .direct_sources
                .iter()
                .any(|s| s.source_message_id == e.source_message_id)
            {
                continue;
            }
            if let PublicBusinessPayloadV1::ReferenceFact(value) = &f.payload {
                match value {
                    CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(v)
                        if scope.source == SourceStreamKindV1::ChainlinkSpot =>
                    {
                        return Some(json!(v.price_usd_per_btc));
                    }
                    CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(v)
                        if (v.window_s == 30
                            && scope.source == SourceStreamKindV1::ChainlinkTwapThirty)
                            || (v.window_s == 60
                                && scope.source == SourceStreamKindV1::ChainlinkTwapSixty) =>
                    {
                        return Some(json!(v.price_usd_per_btc));
                    }
                    CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(v)
                        if scope.source == SourceStreamKindV1::BinanceAggregateTrade =>
                    {
                        return Some(json!([
                            v.price_usdt_per_btc.0,
                            v.quantity_btc.0,
                            v.buyer_is_maker.to_string()
                        ]));
                    }
                    _ => {}
                }
            }
        }
    }
    None
}
#[derive(Default, Serialize)]
struct GroupCounts {
    groups: u64,
    observations: u64,
}
#[derive(Default, Serialize)]
struct ObservationCounts {
    initialization: u64,
    in_target_window: u64,
    before_target_window: u64,
    after_target_window: u64,
}
pub(super) fn replay(c: &Config, output: &Path) -> Result<Value> {
    let mut replay = super::open_checked(c)?;
    let mut stats: Vec<_> = c.scopes.iter().cloned().map(Stats::new).collect();
    let mut counts = Counts::default();
    let mut group_buckets = BTreeMap::<String, GroupCounts>::new();
    let mut observations = BTreeMap::<String, ObservationCounts>::new();
    let mut target_windows_observed = BTreeMap::<String, MarketWindowV2>::new();
    let mut target_assets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut non_target = BTreeMap::<String, u64>::new();
    let mut report_reasons = BTreeMap::<String, u64>::new();
    let (mut now, mut end_seen) = (i64::MIN, false);
    let began = Instant::now();
    let mut last_print = Instant::now();
    let cutoff = c
        .scopes
        .iter()
        .map(|s| s.window.end_at_utc_ms)
        .max()
        .unwrap_or(c.received_end_utc_ms);
    while let Some((input, applied, _)) = replay.next_prepared_applied()? {
        if end_seen {
            return Err("正常End后仍有输入".into());
        }
        counts.applied_inputs += 1;
        for g in &applied.applied.closed_groups {
            now = now.max(
                sim_system::public_consumption::original_public_group_visible_at_utc_ms(&input, g),
            );
            let initialization = g
                .member_input_positions
                .iter()
                .any(|p| c.initialization_input_positions.contains(p));
            let bucket = if initialization {
                "initialization"
            } else if now < c.received_start_utc_ms {
                "before_analysis"
            } else if now < cutoff {
                "analysis"
            } else {
                "tail"
            };
            let b = group_buckets.entry(bucket.into()).or_default();
            b.groups += 1;
            b.observations += g.observations.len() as u64;
            counts.closed_groups += 1;
            counts.observations += g.observations.len() as u64;
            for f in g.records.iter() {
                if let PublicBusinessPayloadV1::ReferenceFact(v) = &f.payload {
                    counts.reference_facts += 1;
                    if let CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(a) = v {
                        *counts
                            .reference_anomalies
                            .entry(format!("{:?}", a.classification))
                            .or_default() += 1;
                    }
                }
            }
            for o in &g.observations {
                if let Some(t) = c
                    .target_markets
                    .iter()
                    .find(|t| t.condition_id == o.market.condition_id)
                {
                    target_windows_observed
                        .entry(t.condition_id.0.clone())
                        .or_insert_with(|| o.market.window.clone());
                    target_assets
                        .entry(t.condition_id.0.clone())
                        .or_default()
                        .extend(o.assets.iter().map(|a| a.asset.token_id.0.clone()));
                    let n = observations.entry(t.condition_id.0.clone()).or_default();
                    if initialization {
                        n.initialization += 1;
                    } else if contains(&t.window, now) {
                        n.in_target_window += 1;
                    } else if now < t.window.start_at_utc_ms {
                        n.before_target_window += 1;
                    } else {
                        n.after_target_window += 1;
                    }
                } else {
                    *non_target
                        .entry(o.market.condition_id.0.clone())
                        .or_default() += 1;
                }
            }
        }
        for batch in &applied.reductions {
            for report in &batch.reports {
                *report_reasons
                    .entry(format!(
                        "{:?}:{}:{}",
                        report.report.operation, report.report.scope, report.report.reason_code
                    ))
                    .or_default() += 1;
            }
        }
        if matches!(input.ingress, DptIngressV1::EndOfInput) {
            end_seen = true;
            counts.end_inputs += 1;
        } else {
            let incoming = match &input.ingress {
                DptIngressV1::Message {
                    source_evidence, ..
                } => Some(source_evidence),
                _ => None,
            };
            for s in &mut stats {
                s.observe(
                    replay.core.current_source_progress(&s.spec.scope),
                    now,
                    incoming,
                    input.input_position,
                    &applied,
                );
            }
        }
        if c.progress_interval_seconds > 0
            && last_print.elapsed() >= Duration::from_secs(c.progress_interval_seconds)
        {
            eprintln!(
                "{}",
                json!({"stage":"multi_scope_replay","applied_inputs":counts.applied_inputs,"total_inputs":c.prefix.next_input_position,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
            last_print = Instant::now();
        }
    }
    if !end_seen
        || counts.end_inputs != 1
        || counts.applied_inputs != c.prefix.next_input_position
        || counts.closed_groups != c.prefix.next_group_position
        || counts.observations != c.prefix.next_observation_position
        || replay.core.applied_prefix().next_input_position != c.prefix.next_input_position
    {
        return Err("End/EOF/输入/组/观察或发布前缀对账失败".into());
    }
    let mut results = Vec::new();
    if c.progress_interval_seconds > 0 {
        eprintln!(
            "{}",
            json!({"stage":"scope_analysis","scopes":stats.len(),"applied_inputs":counts.applied_inputs,"total_inputs":c.prefix.next_input_position,"elapsed_seconds":began.elapsed().as_secs_f64()})
        );
    }
    for (i, s) in stats.into_iter().enumerate() {
        results.push(s.finish(i, output)?);
    }
    let objects = replay.core.object_counts();
    Ok(
        json!({"mode":"explicit_scope_windows","capture_verified_before_open":{"bytes":c.expected_capture_bytes,"sha256":c.expected_capture_sha256},
        "prefix":c.prefix,"actual_applied_prefix":replay.core.applied_prefix(),"normal_end":end_seen,"eof":true,"counts":counts,
        "final_core_object_counts":{"markets":objects.markets,"assets":objects.assets},"group_buckets":group_buckets,
        "target_market_windows_observed":target_windows_observed,"target_asset_bindings_observed":target_assets,"target_observation_counts":observations,"non_target_observation_counts":non_target,"original_reduction_report_counts":report_reasons,
        "scopes":results,"method":"每输入应用后读取准确scope一次；最后有效消息身份变化与本次遍历首次见到的有效身份分别计数；只后者计新确认；状态/观察身份/重复投影/重复incoming身份独立分母。样本需两个接收/可见端点在各自半开有效窗；窗前事实仅支撑连续区间；尾段不入拟合。ceil(p*n)的p50/90/95/99去重至每scope最多16对，使用原共享健康判断；无合法样本无候选。"}),
    )
}
