//! #378 已签的两实例 BTC 有界诊断，非默认测试，不运行策略或真实数据。
use super::*;
use super::{fixture_execution_v4, fixture_input, fixture_recording};
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
#[path = "../../../backtest/examples/support/statistics_sources.rs"]
mod sources;
#[path = "../../../backtest/src/timing.rs"]
mod timing;

struct Material {
    core: PublicObservationCoreV1,
    groups: Vec<(u64, i64)>,
    old: Vec<(String, String)>,
    current: String,
    typed: usize,
}
fn market(i: usize) -> (String, String, [String; 2]) {
    if i == 127 {
        (
            super::fixture_market_identity::OTHER_CONDITION_ID.into(),
            "3195577".into(),
            ["21".into(), "4".into()],
        )
    } else if i == 0 {
        (
            fixture_input::CONDITION_ID.into(),
            "3195576".into(),
            ["20".into(), "3".into()],
        )
    } else {
        (
            format!("0x{:064x}", 1000 + i),
            (3195577 + i).to_string(),
            [(1000 + i * 2).to_string(), (1001 + i * 2).to_string()],
        )
    }
}
fn metadata(i: usize, received: i64, sequence: u64, active: bool, closed: bool) -> TypedRawRowV2 {
    let (condition, clob, tokens) = market(i);
    let mut m = fixture_input::metadata_snapshot_row();
    m.condition_id = condition;
    m.clob_market_id = clob;
    m.asset_ids = serde_json::to_string(&tokens).unwrap();
    m.market_start_utc = 620000;
    m.market_end_utc = if i == 127 { 690000 } else { 660000 };
    m.timestamp_received = received;
    m.collector_admission_sequence = sequence;
    m.collector_admission_time_utc_ms = received + 1;
    m.fixed_identity_verified_at_utc = 609999;
    m.metadata_fetch_start_utc = Some(received - 10);
    m.source_evidence_received_at_utc = Some(received);
    m.row_evidence_validation_completed_at_utc = Some(received + 1);
    m.closed = Some(closed);
    if active {
        m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
        m.market_runtime_role = MetadataMarketRuntimeRole::Current;
    }
    TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(m)))
}
fn anchor(i: usize, at: i64) -> TypedRawRowV2 {
    let (condition, clob, tokens) = market(i);
    let body = r#"{"eventMetadata":{"priceToBeat":64140}}"#;
    let input: MarketAnchorRuleUpdateInput = serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":at-1,
        "collector_admission_sequence":1, "collector_admission_time_utc_ms":at,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"synthetic-collector", "collector_region":"test",
        "slug":"btc-updown-5m-engineering", "condition_id":condition, "clob_market_id":clob, "asset_ids":serde_json::to_string(&tokens).unwrap(),
        "market_start_utc":620000, "market_end_utc":if i==127 {690000}else{660000}, "fixed_identity_verified_at_utc":609999,
        "market_lifecycle_state":"active", "market_runtime_role":"current", "metadata_fetch_start_utc":at-11,
        "source_endpoint":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering",
        "anchor_query_source_epoch_id":format!("btc-anchor-{i}"), "anchor_query_ordinal":1, "anchor_query_result":"observed",
        "price_to_beat_decimal":"64140", "price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":true,"twap_lookback_seconds":30,
        "http_received_at_utc":at-1,"http_request_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering",
        "http_response_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering", "http_status_code":200,
        "http_body_encoding":"utf8", "http_body":body,"http_body_sha256":format!("{:x}",Sha256::digest(body.as_bytes()))
    })).unwrap();
    TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
        PolymarketMarketMetadataV6Input::AnchorRule(input),
    ))
}
fn btc_batch(n: usize, end: i64) -> Vec<(i64, TypedRawRowV2)> {
    let sequence = n as u64 + 1;
    let v = end - 4;
    let depth = if n == 0 {
        let mut x = sources::snapshot("btc-depth", 100, sequence);
        x.timestamp_received = v - 1;
        x.collector_admission_time_utc_ms = v;
        x.snapshot_fetch_start_utc = v - 10;
        TypedRawRow::BinanceDepthSnapshot(x)
    } else {
        TypedRawRow::BinanceDepthUpdate(BinanceDepthUpdateRow {
            timestamp_received: v - 1,
            event_type: "depthUpdate".into(),
            event_time: v - 2,
            symbol: "BTCUSDT".into(),
            first_update_id: 100 + n as u64,
            final_update_id: 100 + n as u64,
            bids: vec![sources::level(
                "64100",
                if n % 2 == 0 { "0.7" } else { "0.8" },
            )],
            asks: vec![],
            stream: "btcusdt@depth".into(),
            snapshot_last_update_id: Some(100),
            local_u_before: if n == 1 { None } else { Some(99 + n as u64) },
            stitch_verdict: BinanceDepthStitchVerdict::Applied,
            stitch_evidence: Some("continuous_depth_update".into()),
            stitch_gap_reason: None,
            source_sequence_epoch_id: "btc-depth".into(),
            connection_generation: 1,
            depth_ingress_sequence: sequence,
            collector_admission_sequence: sequence,
            collector_admission_time_utc_ms: v,
            collector_host_id: "collector-1".into(),
            collector_region: "eu-west-1".into(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".into(),
            schema_version: "binance_depth_update_v4".into(),
        })
    };
    let mut trade = sources::trade(sequence, sequence, "64150");
    trade.timestamp_received = end - 4;
    trade.collector_admission_time_utc_ms = end - 3;
    if let BinanceAggTradeRecord::Trade {
        event_time,
        trade_time,
        ..
    } = &mut trade.record
    {
        *event_time = end - 5;
        *trade_time = end - 6;
    }
    trade.source_native_payload_json = format!(
        r#"{{"e":"aggTrade","E":{},"T":{},"s":"BTCUSDT","a":{},"p":"64150"}}"#,
        end - 5,
        end - 6,
        sequence
    );
    let mut spot = sources::twap_input(30).common;
    spot.topic = "crypto_prices_chainlink".into();
    spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    spot.value = format!("64150.{}", (end / 100) % 3);
    spot.timestamp_received = end - 2;
    spot.message_timestamp = end - 3;
    spot.payload_timestamp = end - 4;
    spot.age_ms = Some(2);
    spot.source_native_payload_json = format!(r#"{{"synthetic_sequence":{sequence}}}"#);
    let mut rows = vec![
        (v, TypedRawRowV2::Existing(depth)),
        (
            end - 3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade)),
        ),
        (
            end - 2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot)),
        ),
    ];
    for (window, v) in [(30, end - 1), (60, end)] {
        let mut x = sources::twap_input(window);
        x.common.timestamp_received = v - 1;
        x.common.message_timestamp = v - 2;
        x.common.payload_timestamp = v - 3;
        x.common.age_ms = Some(2);
        x.collector_admission_sequence = sequence;
        x.collector_admission_time_utc_ms = v;
        x.source_sequence_epoch_id = format!("btc-twap-{window}");
        rows.push((v, TypedRawRowV2::PolymarketRtdsChainlinkTwap(x)));
    }
    rows
}
fn material(root: &Path, long: bool) -> Material {
    let mut rows = Vec::new();
    let first = if long { 70700 } else { 370900 };
    for (n, end) in (first..=677400).step_by(100).enumerate() {
        rows.extend(btc_batch(n, end));
    }
    for i in 0..128 {
        rows.push((610001, metadata(i, 610000, i as u64 + 1, false, false)));
        rows.push((620001, metadata(i, 620000, 129 + i as u64, true, false)));
    }
    for i in 0..128 {
        let (condition, _, tokens) = market(i);
        for (leg, token) in tokens.into_iter().enumerate() {
            let mut event = fixture_input::book("10");
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut event {
                *asset_id = token;
            }
            let mut x =
                fixture_input::row("epoch-a", (i * 2 + leg + 1) as u64, 621000, 621001, event);
            x.market = condition.clone();
            rows.push((
                621002,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(x)),
            ));
        }
    }
    for (j, i) in (0..8).chain(std::iter::once(127)).enumerate() {
        let at = 622001 + j as i64;
        rows.push((at, anchor(i, at)));
    }
    for i in 0..127 {
        rows.push((665001, metadata(i, 665000, 257 + i as u64, true, true)));
    }
    rows.sort_by_key(|(v, _)| *v);
    let typed = rows.len();
    assert_eq!(typed, if long { 30988 } else { 15978 });
    let mut core = fixture_input::core();
    let mut writer = PublicRecordingWriterV1::create_historical(
        root,
        core.header(),
        fixture_recording::config(),
    )
    .unwrap();
    let mut groups = Vec::new();
    for (position, (_planned, raw)) in rows.into_iter().enumerate() {
        let input = fixture_input::record(position as u64, raw);
        let applied = fixture_recording::apply(&mut core, &input);
        for group in &applied.applied.closed_groups {
            let visible = original_public_group_visible_at_utc_ms(&input, group);
            groups.push((group.group_position, visible));
        }
        writer.record(input, applied).unwrap();
    }
    let input = fixture_recording::end(typed as u64);
    let applied = fixture_recording::apply(&mut core, &input);
    for group in &applied.applied.closed_groups {
        groups.push((
            group.group_position,
            original_public_group_visible_at_utc_ms(&input, group),
        ));
    }
    writer.record(input, applied).unwrap();
    writer.stabilize().unwrap();
    assert!(
        groups.windows(2).all(|p| p[0].1 <= p[1].1),
        "正式V顺序必须匹配已排序来源"
    );
    let old = (0..8)
        .map(|i| {
            let (id, _, tokens) = market(i);
            (id, tokens[0].clone())
        })
        .collect();
    Material {
        core,
        groups,
        old,
        current: market(127).0,
        typed,
    }
}

fn reset_work() {
    take();
    data_pipeline_core::public_statistics::observation_diagnostics::take();
    sim_system::public_consumption::observation_diagnostics::take();
}
fn work() -> serde_json::Value {
    serde_json::json!({"shared":take(),"statistics":data_pipeline_core::public_statistics::observation_diagnostics::take(),"frames":sim_system::public_consumption::observation_diagnostics::take()})
}
fn statistics_coverage(
    frames: &[PublicObservationFrameV1<'_>],
    at: i64,
    current: &str,
) -> serde_json::Value {
    assert_eq!(frames.len(), 9);
    let values:Vec<_>=frames.iter().map(|frame| {
        let s=frame.statistics.as_ref().expect("正式统计启用");
        assert_eq!(s.observed_at_utc_ms,at);
        assert!(s.momentum.iter().all(|m|m.change_usdt_per_btc.value.is_some() && m.left_applied_at_utc_ms.is_some() && m.right_applied_at_utc_ms.is_some()));
        assert!(s.spot_variance.iter().all(|v|v.sigma_usd_per_btc_per_sqrt_second.is_some()));
        assert!(s.spot_integrals.iter().all(|v|v.integral_usd_per_btc_seconds.value.is_some() && v.observed_seconds>0.));
        assert_eq!(frame.observation.official_anchor.value.as_ref().unwrap().price_usd_per_btc.0,"64140");
        if frame.observation.market.condition_id.0==current {
            assert!(s.blb_lookbacks.iter().all(|v|v.left.is_some() && v.right.is_some()));
        }else{assert!(s.blb_lookbacks.iter().all(|v|v.left.is_none() && v.right.is_none()));}
        serde_json::json!({"market":s.market,"momentum":s.momentum,"spot_variance":s.spot_variance,"integrals":s.spot_integrals,"blb":s.blb,"blb_lookbacks":s.blb_lookbacks})
    }).collect();
    serde_json::json!(values)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "#378 signed bounded BTC diagnostic; explicit one before/after only"]
async fn finite_btc_observation_diagnostic() {
    let root = PathBuf::from(std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("显式输出"));
    std::fs::create_dir_all(&root).unwrap();
    for (label, long) in [("short", false), ("long", true)] {
        let cell = root.join(label);
        std::fs::create_dir(&cell).unwrap();
        let prep_started = Instant::now();
        let m = material(&cell.join("dpt"), long);
        std::fs::write(
            cell.join("groups.json"),
            serde_json::to_vec(&m.groups).unwrap(),
        )
        .unwrap();
        let (mut execution, observation, rules) = fixture_execution_v4::execution_with_identity_v4(
            &cell.join("sim.jsonl"),
            &m.core,
            fixture_policy::SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "100",
            &format!("btc-{label}"),
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &cell.join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(
            &cell.join("dpt"),
            128,
            16 << 20,
            timing::DeliveryTimes {
                delays: timing::DelayScheduleV1::Fixed(0),
                last: None,
            },
        )
        .unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, format!("btc-{label}"));
        driver.enable_finite_recording_wait().unwrap();
        let config: PublicStatisticsConfigV1 = serde_json::from_str(include_str!(
            "../../../baseline-mm/configs/strategies/complementary_pair/public-statistics.json"
        ))
        .unwrap();
        let identity = PublicStatisticsIdentityV1 {
            input_set_identity: source_identity(&cell.join("dpt"), &[]),
            stream_header: m.core.header().clone(),
            rules_identity: rules.file_sha256().clone(),
            delivery_policy:
                "backtest::timing::DeliveryTimes/Fixed(0); original_public_group_visible_at_utc_ms"
                    .into(),
        };
        driver
            .enable_public_statistics(
                data_pipeline_core::public_statistics::PublicStatisticsStreamV1::new(
                    identity,
                    config.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        let mut next = 0usize;
        macro_rules! consume_through {
            ($at:expr) => {{
                while next < m.groups.len() && m.groups[next].1 <= $at {
                    let (receipt, cutoff) = driver.next_group().unwrap().unwrap();
                    assert_eq!(receipt.group().group_position, m.groups[next].0);
                    assert_eq!(
                        cutoff.decision_time().logical_time_utc_ms(),
                        m.groups[next].1
                    );
                    shared.apply_closed().unwrap();
                    next += 1;
                }
            }};
        }
        consume_through!(622009);
        driver.observe_at(622010).unwrap();
        drop(
            shared
                .strategy_selected_current_observations(&driver, &[])
                .unwrap(),
        );
        let mut receipts = Vec::new();
        for (i, (condition, token)) in m.old.iter().enumerate() {
            receipts.push(
                shared
                    .execute(
                        &mut driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: format!("old-{i}"),
                            caller_correlation_id: None,
                        },
                        Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                            condition_id: ConditionId(condition.clone()),
                            order: BuyLegIntentV1 {
                                token_id: TokenId(token.clone()),
                                order: BuyOrderIntentV1::GtcLimit {
                                    limit_price_pusd_per_share: Decimal::new(6, 1),
                                    size_shares: Decimal::from(10),
                                    post_only: false,
                                },
                            },
                        })),
                    )
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        consume_through!(671000);
        let public_only = driver.selected_frames_at_observation(std::iter::empty());
        assert_eq!(public_only.len(), 1);
        assert_eq!(public_only[0].observation.market.condition_id.0, m.current);
        drop(public_only);
        let frames = shared
            .strategy_selected_current_observations(&driver, &[])
            .unwrap();
        let selected: Vec<_> = frames
            .iter()
            .map(|f| f.observation.market.condition_id.0.clone())
            .collect();
        std::fs::write(cell.join("preparation_statistics.json"),serde_json::to_vec_pretty(&frames.iter().map(|f|serde_json::json!({"statistics":f.statistics,"anchor":f.observation.official_anchor,"cutoff":f.decision_cutoff})).collect::<Vec<_>>()).unwrap()).unwrap();
        let coverage = statistics_coverage(&frames, 671000, &m.current);
        drop(frames);
        let read = shared.ledger().necessary_read_v2().unwrap();
        let held: Vec<_> = read
            .marked_markets()
            .map(|id| {
                (
                    id.to_owned(),
                    read.marked_position(id).unwrap().1.map(|q| q.to_string()),
                )
            })
            .collect();
        let accepted = shared
            .registry()
            .orders()
            .filter(|o| o.accepted.is_some())
            .count();
        let private = driver
            .observation_cutoff()
            .unwrap()
            .private_feedback_sequence();
        let ledger_sequence = shared.ledger().applied_entry_sequence();
        let prepared = serde_json::json!({"typed_inputs":m.typed,"groups_before_measurement":next,"group_count":m.groups.len(),"selected":selected,"held":held,"cash":read.cash(),"accepted":accepted,"private_sequence":private,"ledger_sequence":ledger_sequence,"request_receipts":receipts,"statistics_config":config,"history_points":driver.public_statistics_prefix().unwrap().history_points(),"coverage":coverage,"preparation_ns":prep_started.elapsed().as_nanos(),"work":work(),"shared_observations":2,"direct_selections":1,"requests":8,"observe_at":1});
        std::fs::write(
            cell.join("prepared.json"),
            serde_json::to_vec_pretty(&prepared).unwrap(),
        )
        .unwrap();
        assert_eq!(driver.public_observation_count(), 128);
        assert_eq!(accepted, 8);
        assert_eq!(held.len(), 8);
        for (_, q) in &held {
            assert_eq!(
                q.iter()
                    .map(|s| s.parse::<Decimal>().unwrap())
                    .sum::<Decimal>(),
                Decimal::from(10)
            );
        }
        assert!(read.cash().0 > Decimal::ZERO);
        assert_eq!(selected.len(), 9);
        reset_work();
        let started = Instant::now();
        let mut observation_ns = 0u128;
        let mut advancement_ns = 0u128;
        let mut assertion_ns = 0u128;
        let mut release_ns = 0u128;
        let mut reads = 0;
        let mut sample = None;
        let mut last_group = 0;
        for batch in 1..=64 {
            let at = 671000 + batch * 100;
            let advance = Instant::now();
            consume_through!(at);
            advancement_ns += advance.elapsed().as_nanos();
            for repeat in 0..4 {
                if repeat == 2 {
                    let advance = Instant::now();
                    driver.observe_at(at + 1).unwrap();
                    advancement_ns += advance.elapsed().as_nanos();
                }
                let actual = at + i64::from(repeat >= 2);
                let measured = Instant::now();
                let frames = shared
                    .strategy_selected_current_observations(&driver, &[])
                    .unwrap();
                observation_ns += measured.elapsed().as_nanos();
                reads += 1;
                let check = Instant::now();
                assert_eq!(
                    frames
                        .iter()
                        .map(|f| f.observation.market.condition_id.0.clone())
                        .collect::<Vec<_>>(),
                    selected
                );
                assert_eq!(
                    frames[0]
                        .decision_cutoff
                        .decision_time()
                        .logical_time_utc_ms(),
                    actual
                );
                if batch == 1 {
                    let v = statistics_coverage(&frames, actual, &m.current);
                    if repeat == 0 {
                        sample = Some(v);
                    }
                }
                let group = frames[0].receipt.group().group_position;
                if repeat > 0 {
                    assert_eq!(group, last_group);
                }
                last_group = group;
                assert_eq!(
                    driver
                        .observation_cutoff()
                        .unwrap()
                        .private_feedback_sequence(),
                    private
                );
                assert_eq!(shared.ledger().applied_entry_sequence(), ledger_sequence);
                assertion_ns += check.elapsed().as_nanos();
                let release = Instant::now();
                drop(frames);
                release_ns += release.elapsed().as_nanos();
            }
        }
        let measurement = serde_json::json!({"case":label,"reads":reads,"loop_ns":started.elapsed().as_nanos(),"observation_ns":observation_ns,"advance_ns":advancement_ns,"assertion_ns":assertion_ns,"release_ns":release_ns,"work":work(),"rss_kib_sample":rss_kib(),"statistics_sample":sample,"groups_consumed":next,"measured_batches":64,"observe_at":64,"first_materializations":"unknown: contracts cache has no measurement readout","probe_disturbance":"unknown; all counters/timers included, nested times not additive"});
        std::fs::write(
            cell.join("measured.json"),
            serde_json::to_vec_pretty(&measurement).unwrap(),
        )
        .unwrap();
        eprintln!("btc_observation={measurement}");
        assert_eq!(reads, 256);
        let stopped = Instant::now();
        driver.stop_and_drain().unwrap();
        std::fs::write(cell.join("teardown.json"),serde_json::to_vec_pretty(&serde_json::json!({"ns":stopped.elapsed().as_nanos(),"status":"stopped_and_recording_drained","source_tail_consumed":false,"terminal":"unknown","natural_eof":false,"run_seal":false})).unwrap()).unwrap();
    }
}
