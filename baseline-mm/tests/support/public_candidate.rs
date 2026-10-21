use super::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_statistics::*, typed_raw::*,
};
use data_pipeline_training::public_recording::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
const T: i64 = 22_000;
fn public_config() -> PublicStatisticsConfigV1 {
    PublicStatisticsConfigV1 {
        momentum_windows_ms: vec![100, 200, 250, 500, 1000, 2000, 5000],
        spot_variance_windows_ms: vec![1000, 300000],
        terminal_integral_windows_ms: vec![60000],
        blb_lookback_windows_ms: vec![200],
    }
}
fn anchor(at: i64, ordinal: u64) -> TypedRawRowV2 {
    let input = serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":at,
        "collector_admission_sequence":ordinal, "collector_admission_time_utc_ms":at+1,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"host", "collector_region":"local",
        "slug":"btc-updown-5m", "condition_id":CONDITION_ID, "clob_market_id":"3195576", "asset_ids":"[\"20\",\"3\"]",
        "market_start_utc":20000,"market_end_utc":30000,"fixed_identity_verified_at_utc":9999,
        "market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":at-10,
        "anchor_query_source_epoch_id":"public-candidate-anchor","anchor_query_ordinal":ordinal,"anchor_query_result":"observed",
        "price_to_beat_decimal":"100000","price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":false,"http_received_at_utc":at,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/synthetic",
        "http_body_sha256":"a".repeat(64)
    })).unwrap();
    TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
        PolymarketMarketMetadataV6Input::AnchorRule(input),
    ))
}
fn depth(at: i64, sequence: u64, quantity: &str, bid: &str, ask: &str) -> TypedRawRowV2 {
    TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(BinanceDepthSnapshotRow {
        timestamp_received: at,
        snapshot_fetch_start_utc: at - 1,
        last_update_id: sequence * 10,
        bids: vec![
            DepthLevel {
                price: bid.into(),
                quantity: quantity.into(),
            },
            DepthLevel {
                price: if bid.parse::<i64>().unwrap() < 100000 {
                    "99996"
                } else {
                    "100000"
                }
                .into(),
                quantity: "2".into(),
            },
        ],
        asks: vec![DepthLevel {
            price: ask.into(),
            quantity: "3".into(),
        }],
        symbol: "BTCUSDT".into(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000".into(),
        snapshot_status: "complete".into(),
        source_sequence_epoch_id: "candidate-depth".into(),
        connection_generation: 1,
        depth_ingress_sequence: sequence * 2 - 1,
        collector_admission_sequence: sequence * 2 - 1,
        collector_admission_time_utc_ms: at + 1,
        collector_host_id: "host".into(),
        collector_region: "local".into(),
        schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA.into(),
    }))
}
fn depth_update(at: i64, n: u64) -> TypedRawRowV2 {
    TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(BinanceDepthUpdateRow {
        timestamp_received: at,
        event_type: "depthUpdate".into(),
        event_time: at,
        symbol: "BTCUSDT".into(),
        first_update_id: n * 10 + 1,
        final_update_id: n * 10 + 1,
        bids: vec![],
        asks: vec![],
        stream: "btcusdt@depth".into(),
        snapshot_last_update_id: Some(n * 10),
        local_u_before: None,
        stitch_verdict: BinanceDepthStitchVerdict::Applied,
        stitch_evidence: Some("first update bridges snapshot lastUpdateId+1".into()),
        stitch_gap_reason: None,
        source_sequence_epoch_id: "candidate-depth".into(),
        connection_generation: 1,
        depth_ingress_sequence: n * 2,
        collector_admission_sequence: n * 2,
        collector_admission_time_utc_ms: at + 1,
        collector_host_id: "host".into(),
        collector_region: "local".into(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".into(),
        schema_version: BINANCE_DEPTH_UPDATE_SCHEMA.into(),
    }))
}
fn twap(at: i64, n: u64) -> TypedRawRowV2 {
    let mut v = fixture_reference::reference_after(at, n);
    v.window_s = 60;
    v.full_accuracy_value = "100000000000000000000000".into();
    v.common.topic = "crypto_prices_twap_sixty".into();
    v.common.value = "100000".into();
    v.common.source_native_payload_json = serde_json::json!({"window_s":60}).to_string();
    TypedRawRowV2::PolymarketRtdsChainlinkTwap(v)
}
fn spot(at: i64, price: &str) -> TypedRawRowV2 {
    let mut s = fixture_reference::reference_after(at, 1).common;
    s.message_timestamp = at;
    s.payload_timestamp = at;
    s.age_ms = Some(0);
    s.value = price.into();
    s.topic = "crypto_prices_chainlink".into();
    s.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    s.source_native_payload_json =
        serde_json::json!({"symbol":"btc/usd","value":price,"timestamp":at}).to_string();
    TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(s))
}
fn trade(at: i64, n: u64) -> TypedRawRowV2 {
    TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(BinanceAggTradeRow {
        timestamp_received:at,rest_backfill_timestamp_received:None,ingest_source:"websocket".into(),rest_was_best_price_match:None,
        source_native_payload_json:serde_json::json!({"e":"aggTrade","E":at,"T":at,"s":"BTCUSDT","a":n,"p":"100000","q":"2","f":n*2,"l":n*2+1,"m":false}).to_string(),
        source_sequence_epoch_id:"candidate-trade".into(),connection_generation:1,websocket_ingress_sequence:n,rest_response_ingress_sequence:None,rest_response_item_index:None,collector_admission_sequence:n,collector_admission_time_utc_ms:at+1,collector_host_id:"host".into(),collector_region:"local".into(),source_endpoint:"wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into(),schema_version:BINANCE_AGGTRADE_SCHEMA.into(),
        record:BinanceAggTradeRecord::Trade {event_type:"aggTrade".into(),event_time:at,trade_time:at,symbol:"BTCUSDT".into(),aggregate_trade_id:n,price:"100000".into(),quantity:"2".into(),first_trade_id:n*2,last_trade_id:n*2+1,buyer_is_maker:false,stream:"btcusdt@aggTrade".into(),ws_ignore:false}
    }))
}
struct Material {
    core: PublicObservationCoreV1,
    groups: Vec<(Arc<ClosedBusinessGroupV1>, i64)>,
    input_hash: Sha256Hex,
}
fn material(root: &Path) -> Material {
    material_with_path(root, false)
}
fn material_with_path(root: &Path, path: bool) -> Material {
    material_custom(root, path, None, None)
}
fn material_custom(
    root: &Path,
    path: bool,
    late_bid: Option<&str>,
    empty_leg: Option<&str>,
) -> Material {
    let mut c = fixture_input::core();
    c.enable_source_progress();
    let mut writer =
        PublicRecordingWriterV1::create_historical(root, c.header(), fixture_recording::config())
            .unwrap();
    let mut active = fixture_input::metadata_snapshot_row();
    active.timestamp_received = 20000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20001;
    active.metadata_fetch_start_utc = Some(19990);
    active.source_evidence_received_at_utc = Some(20000);
    active.row_evidence_validation_completed_at_utc = Some(20001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut rows = vec![
        (
            10000,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                fixture_input::metadata_snapshot_row(),
            ))),
        ),
        (
            20000,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        (T - 100, anchor(T - 100, 3)),
    ];
    let mut seq = 0;
    let points = if path {
        vec![
            (T, "5", "100001", "100003"),
            (T + 100, "9", "99997", "99999"),
            (T + 200, "3", "100001", "100003"),
            (T + 400, "3", "100001", "100003"),
        ]
    } else {
        vec![
            (T, "5", "100001", "100003"),
            (T + 200, "3", "100001", "100003"),
            (T + 400, "3", "100001", "100003"),
            (T + 3800, "5", "100001", "100003"),
            (T + 4000, "3", "100001", "100003"),
        ]
    };
    for (i, (at, q, bid, ask)) in points.into_iter().enumerate() {
        for token in [UP_TOKEN_ID, DOWN_TOKEN_ID] {
            seq += 1;
            let mut event = fixture_input::book("100");
            if let PolymarketOrderbookEvent::Book { asset_id, bids, .. } = &mut event {
                *asset_id = token.into();
                bids[0].price = if at >= T + 4000 {
                    late_bid.unwrap_or("0.45")
                } else {
                    "0.45"
                }
                .into();
                if at >= T + 200 && empty_leg == Some(token) {
                    bids.clear();
                }
            }
            rows.push((
                at,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(fixture_input::row(
                    "epoch-a", seq, at, at, event,
                ))),
            ));
            if at >= T + 200 && empty_leg == Some(token) {
                seq += 1;
                rows.push((
                    at,
                    TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(fixture_input::row(
                        "epoch-a",
                        seq,
                        at,
                        at,
                        PolymarketOrderbookEvent::BestBidAsk {
                            asset_id: token.into(),
                            best_bid: Some("0.45".into()),
                            best_ask: Some("0.6".into()),
                            spread: Some("0.15".into()),
                        },
                    ))),
                ));
            }
        }
        rows.push((at, spot(at, "100000")));
        rows.push((at, trade(at, i as u64 + 1)));
        rows.push((at, twap(at, i as u64 + 1)));
        rows.push((at, depth(at, i as u64 + 1, q, bid, ask)));
        rows.push((at, depth_update(at, i as u64 + 1)));
    }
    if path {
        rows.push((T + 300, anchor(T + 300, 4)));
        rows.sort_by_key(|(t, _)| *t);
    }
    let mut groups = vec![];
    let mut hash = Sha256::new();
    let end_position = rows.len() as u64;
    for (position, (at, row)) in rows.into_iter().enumerate() {
        let ingress = fixture_input::record(position as u64, row);
        hash.update(serde_json::to_vec(&ingress).unwrap());
        let prepared = prepare_public_ingress_v1(&c, &ingress, &BTreeMap::new(), &[]).unwrap();
        let applied = c.push(&prepared).unwrap().into_owned();
        groups.extend(
            applied
                .applied
                .closed_groups
                .iter()
                .map(|g| (g.clone(), at + 2)),
        );
        writer.record(ingress, applied).unwrap();
    }
    let end = RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: end_position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    hash.update(serde_json::to_vec(&end).unwrap());
    let prepared = prepare_public_ingress_v1(&c, &end, &BTreeMap::new(), &[]).unwrap();
    let applied = c.push(&prepared).unwrap().into_owned();
    groups.extend(
        applied
            .applied
            .closed_groups
            .iter()
            .map(|g| (g.clone(), T + 4002)),
    );
    writer.record(end, applied).unwrap();
    writer.stabilize().unwrap();
    Material {
        core: c,
        groups,
        input_hash: Sha256Hex(format!("{:x}", hash.finalize())),
    }
}
fn stream(m: &Material, rules: &SealedProcessRuleSnapshotV3) -> Arc<PublicStatisticsStreamV1> {
    PublicStatisticsStreamV1::new(
        PublicStatisticsIdentityV1 {
            input_set_identity: m.input_hash.clone(),
            stream_header: m.core.header().clone(),
            rules_identity: rules.file_sha256().clone(),
            delivery_policy: "synthetic-original-group-times".into(),
        },
        public_config(),
    )
    .unwrap()
}
fn group_ref(g: &ClosedBusinessGroupV1) -> PublicGroupReferenceV1 {
    PublicGroupReferenceV1 {
        input_stream_id: "core-test".into(),
        applied_prefix: AppliedIngressPrefixV1 {
            input_stream_id: "core-test".into(),
            next_input_position: g.closed_at_input_position + 1,
        },
        group_position: g.group_position,
        group_id: g.group_id.clone(),
        closed_at_input_position: g.closed_at_input_position,
    }
}
type PublicDriver =
    FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<fixture_clock::TradingTimes>>;
fn setup_public(
    root: &Path,
) -> (
    SharedExecutionV2,
    PublicDriver,
    Arc<SealedProcessRuleSnapshotV3>,
    Material,
) {
    setup_public_custom(root, None, None)
}
fn setup_public_custom(
    root: &Path,
    late_bid: Option<&str>,
    empty_leg: Option<&str>,
) -> (
    SharedExecutionV2,
    PublicDriver,
    Arc<SealedProcessRuleSnapshotV3>,
    Material,
) {
    let dpt = root.join("dpt");
    let m = if late_bid.is_none() && empty_leg.is_none() {
        material(&dpt)
    } else {
        material_custom(&dpt, false, late_bid, empty_leg)
    };
    let (mut execution, observation, rules) = execution_with_rules_v4(
        &root.join("sim.jsonl"),
        &m.core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "2000",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(
        &dpt,
        16,
        1 << 20,
        fixture_clock::TradingTimes(m.groups.iter().map(|(_, t)| *t).collect()),
    )
    .unwrap();
    source.enable_source_progress();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "public-candidate".into());
    driver.enable_public_statistics(stream(&m, &rules)).unwrap();
    driver.enable_finite_recording_wait().unwrap();
    (shared, driver, rules, m)
}
#[tokio::test(flavor = "current_thread")]
async fn public_v4_real_core_history_and_a_request_c0_c1() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) = setup_public(temp.path());
    let last = m
        .groups
        .iter()
        .rev()
        .find(|(_, at)| *at == T + 202)
        .unwrap()
        .0
        .group_position;
    while driver
        .next_group()
        .unwrap()
        .unwrap()
        .0
        .group()
        .group_position
        < last
    {}
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let stats = input.markets[0].statistics.as_ref().unwrap();
    assert_eq!(
        stats
            .momentum
            .iter()
            .find(|v| v.window_ms == 200)
            .unwrap()
            .change_usdt_per_btc
            .value,
        Some(Decimal::ZERO)
    );
    assert_eq!(
        stats.blb_lookbacks[0]
            .left
            .as_ref()
            .unwrap()
            .visible_quantity_btc,
        Some(Decimal::from(7))
    );
    assert_eq!(
        stats.blb_lookbacks[0]
            .right
            .as_ref()
            .unwrap()
            .visible_quantity_btc,
        Some(Decimal::from(5))
    );
    for veto in [false, true] {
        let c: ComplementaryPairConfigV4 = serde_json::from_str(include_str!(
            "../../configs/strategies/complementary_pair/public-c0.json"
        ))
        .unwrap();
        let c = c.parameters;
        let mut strategy = ComplementaryPairV1::new_public(
            ComplementaryPairConfigV4 {
                parameters: c,
                blb_decrease_veto: veto,
            },
            rules.clone(),
            format!("public-{veto}"),
            StrategyContinuityV2::new_public_account(),
        )
        .unwrap();
        let out = strategy.decide(&input, T + 217).unwrap();
        let d = &diagnostic(&out).markets[0];
        assert!(d.source_health.is_empty(), "{:?}", d.source_health);
        let entry = d.pair_entry.public_entry.as_ref().unwrap();
        assert!(entry.c0_passed);
        assert_eq!(entry.observed_at_utc_ms, Some(T + 202));
        assert_eq!(
            entry.delta_quantity_btc,
            if veto { Some(Decimal::from(-2)) } else { None }
        );
        assert_eq!(
            intents(&out)
                .iter()
                .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. })),
            !veto
        );
        if !veto {
            let receipts = shared
                .execute_batch_reserved(&mut driver.decision().unwrap(), intents(&out))
                .await
                .unwrap();
            assert!(!receipts.is_empty());
        }
    }
    driver.stop_and_drain().unwrap();
}
fn new_public_strategy(rules: Arc<SealedProcessRuleSnapshotV3>, veto: bool) -> ComplementaryPairV1 {
    let mut c: ComplementaryPairConfigV4 = serde_json::from_str(include_str!(
        "../../configs/strategies/complementary_pair/public-c0.json"
    ))
    .unwrap();
    c.blb_decrease_veto = veto;
    ComplementaryPairV1::new_public(
        c,
        rules,
        format!("rules-{veto}"),
        StrategyContinuityV2::new_public_account(),
    )
    .unwrap()
}
#[tokio::test(flavor = "current_thread")]
async fn public_v4_declarative_rule_expectations_at_real_new_pair_branch() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) = setup_public(temp.path());
    let last = m
        .groups
        .iter()
        .rev()
        .find(|(_, at)| *at == T + 202)
        .unwrap()
        .0
        .group_position;
    while driver
        .next_group()
        .unwrap()
        .unwrap()
        .0
        .group()
        .group_position
        < last
    {}
    // R12/R13/R14 are also exercised with real application/cursor/order paths below.
    for (id, allowed, outcome) in [
        ("R01", false, "veto_decreasing"),
        ("R02", true, "non_decreasing"),
        ("R03", true, "non_decreasing"),
        ("R04", false, "veto_decreasing"),
        ("R05", false, "not_evaluated"),
        ("R06", false, "not_evaluated"),
        ("R07", false, "unreached"),
        ("R08", false, "not_evaluated"),
        ("R09", true, "incomparable"),
        ("R10", true, "incomparable"),
        ("R11", true, "incomparable"),
        ("R12", false, "veto_decreasing"),
        ("R13", false, "veto_decreasing"),
        ("R14", false, "veto_decreasing"),
        ("R15", true, "incomparable"),
        ("R16", false, "veto_decreasing"),
        ("at_anchor", true, "incomparable"),
        ("missing_Q", true, "incomparable"),
        ("changed_M", true, "incomparable"),
        ("changed_side", true, "incomparable"),
        ("changed_config", true, "incomparable"),
        ("invalid_depth", true, "incomparable"),
    ] {
        let mut input = shared.strategy_input(&mut driver).unwrap().unwrap();
        if id == "R07" {
            input.markets[0].observation.assets[0].bids = Arc::new(fact(vec![]));
        }
        if id == "R08" {
            let mut asks = input.markets[0].observation.assets[0].asks.as_ref().clone();
            asks.value.as_mut().unwrap()[0].price_pusd_per_share = ExactDecimal("0.4".into());
            input.markets[0].observation.assets[0].asks = Arc::new(asks);
        }
        let stats = Arc::make_mut(input.markets[0].statistics.as_mut().unwrap());
        if id == "R05" {
            stats
                .momentum
                .iter_mut()
                .find(|m| m.window_ms == 200)
                .unwrap()
                .change_usdt_per_btc = StatisticValueV1::known(Decimal::new(1, 1));
        }
        if id == "R06" {
            stats
                .momentum
                .iter_mut()
                .find(|m| m.window_ms == 200)
                .unwrap()
                .change_usdt_per_btc = StatisticValueV1::missing("left_midpoint_support_missing");
        }
        let look = &mut stats.blb_lookbacks[0];
        match id {
            "R02" => {
                Arc::make_mut(look.left.as_mut().unwrap()).visible_quantity_btc =
                    Some(Decimal::from(5))
            }
            "R03" => {
                Arc::make_mut(look.left.as_mut().unwrap()).visible_quantity_btc =
                    Some(Decimal::from(5));
                Arc::make_mut(look.right.as_mut().unwrap()).visible_quantity_btc =
                    Some(Decimal::from(7));
            }
            "R04" => {
                Arc::make_mut(look.left.as_mut().unwrap()).visible_quantity_btc =
                    Some(Decimal::TWO);
                Arc::make_mut(look.right.as_mut().unwrap()).visible_quantity_btc =
                    Some(Decimal::ZERO);
            }
            "R09" => Arc::make_mut(look.right.as_mut().unwrap()).official_k_usd_per_btc = None,
            "R10" => {
                Arc::make_mut(look.right.as_mut().unwrap())
                    .official_anchor
                    .direct_anchors[0]
                    .source_message_id = Sha256Hex("b".repeat(64))
            }
            "R11" => look.left = None,
            "R15" => {
                let p = Arc::make_mut(look.left.as_mut().unwrap());
                p.best_bid_usdt_per_btc = StatisticValueV1::known(Decimal::from(100003));
                p.best_ask_usdt_per_btc = StatisticValueV1::known(Decimal::from(100001));
            }
            "at_anchor" => Arc::make_mut(look.right.as_mut().unwrap()).at_anchor = true,
            "missing_Q" => Arc::make_mut(look.right.as_mut().unwrap()).visible_quantity_btc = None,
            "changed_M" => {
                Arc::make_mut(look.right.as_mut().unwrap()).midpoint_usdt_per_btc =
                    Some(Decimal::from(100003))
            }
            "changed_side" => Arc::make_mut(look.right.as_mut().unwrap()).side = Some("ask".into()),
            "changed_config" => {
                Arc::make_mut(&mut Arc::make_mut(look.right.as_mut().unwrap()).config)
                    .momentum_windows_ms
                    .push(201)
            }
            "invalid_depth" => {
                Arc::make_mut(look.right.as_mut().unwrap()).depth_quality =
                    MarketDataQuality::Invalid
            }
            "R16" => {
                for p in [&mut look.left, &mut look.right] {
                    let p = Arc::make_mut(p.as_mut().unwrap());
                    p.best_bid_usdt_per_btc = StatisticValueV1::known(Decimal::from(100002));
                    p.best_ask_usdt_per_btc = StatisticValueV1::known(Decimal::from(100002));
                }
            }
            _ => {}
        }
        let out = new_public_strategy(rules.clone(), true)
            .decide(&input, T + 217)
            .unwrap();
        let d = &diagnostic(&out).markets[0];
        assert_eq!(
            intents(&out)
                .iter()
                .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. })),
            allowed,
            "{id}: {d:?}"
        );
        assert_eq!(
            d.pair_entry
                .public_entry
                .as_ref()
                .map(|e| e.blb_outcome.as_str())
                .unwrap_or("unreached"),
            outcome,
            "{id}"
        );
        if outcome == "incomparable" {
            assert!(
                d.pair_entry
                    .public_entry
                    .as_ref()
                    .unwrap()
                    .delta_quantity_btc
                    .is_none()
            );
        }
    }
    driver.stop_and_drain().unwrap();
}
#[test]
fn public_v4_real_applied_history_freezes_same_ms_late_anchor_and_slow_cursor() {
    let temp = tempfile::tempdir().unwrap();
    let m = material_with_path(&temp.path().join("dpt"), true);
    // Same formal sealed rules as execution; only read from the finite fixture.
    let (_, _, rules) = execution_with_rules_v4(
        &temp.path().join("sim.jsonl"),
        &m.core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "2000",
    );
    let s = stream(&m, &rules);
    let prepared: Vec<_> = m
        .groups
        .iter()
        .map(|_| PreparedPublicStatisticsV1::default())
        .collect();
    let mut prefixes = vec![];
    for ((g, at), p) in m.groups.iter().zip(&prepared) {
        prefixes.push(p.consume(&s, group_ref(g), *at, g).unwrap().unwrap());
    }
    let i = m.groups.iter().rposition(|(_, t)| *t == T + 202).unwrap();
    let (g, at) = &m.groups[i];
    let o = g.observations.get_market(CONDITION_ID).unwrap();
    assert!(
        prefixes[0]
            .read(
                m.groups[0].0.observations.get_market(CONDITION_ID).unwrap(),
                m.groups[0].1
            )
            .blb
            .official_k_usd_per_btc
            .is_none()
    );
    let final_index = prefixes.len() - 1;
    let final_stat = prefixes[final_index].read(
        m.groups[final_index]
            .0
            .observations
            .get_market(CONDITION_ID)
            .unwrap(),
        m.groups[final_index].1,
    );
    assert_eq!(
        final_stat.blb.official_k_usd_per_btc,
        Some(Decimal::from(100000))
    );
    assert_ne!(
        final_stat.blb_lookbacks[0]
            .right
            .as_ref()
            .unwrap()
            .official_anchor
            .direct_anchors,
        prefixes[i].read(o, *at).blb_lookbacks[0]
            .right
            .as_ref()
            .unwrap()
            .official_anchor
            .direct_anchors
    );
    let frozen = prefixes[i].read(o, *at);
    let view = &frozen.blb_lookbacks[0];
    assert_eq!(
        view.left.as_ref().unwrap().visible_quantity_btc,
        Some(Decimal::from(7))
    );
    assert_eq!(
        view.right.as_ref().unwrap().visible_quantity_btc,
        Some(Decimal::from(5))
    );
    assert_eq!(
        frozen
            .momentum
            .iter()
            .find(|v| v.window_ms == 200)
            .unwrap()
            .change_usdt_per_btc
            .value,
        Some(Decimal::ZERO)
    );
    // A path below K returned to its starting interval. No path-constancy veto is invented.
    assert!(prefixes.iter().zip(&m.groups).any(|(h, (g, t))| {
        *t == T + 102
            && h.read(g.observations.get_market(CONDITION_ID).unwrap(), *t)
                .blb
                .side
                == Some("ask")
    }));
    let slow = prepared[i]
        .consume(&s, group_ref(g), *at, g)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&slow, &prefixes[i]));
    assert_eq!(slow.read(o, *at), frozen);
    // Earlier group of the same application millisecond cannot see the later depth update.
    let early = m.groups.iter().position(|(_, t)| *t == T + 202).unwrap();
    let (eg, et) = &m.groups[early];
    let old = prefixes[early].read(eg.observations.get_market(CONDITION_ID).unwrap(), *et);
    assert_ne!(old.group, frozen.group);
    assert_eq!(old.blb.visible_quantity_btc, Some(Decimal::from(3)));
    // Pure reads hold the applied endpoint; no source timestamp or sample is fabricated.
    let query = prefixes[i].read(o, *at + 50);
    assert_eq!(query.blb_lookbacks[0].right, view.right);
    assert_eq!(
        query.source_applied_at_utc_ms,
        frozen.source_applied_at_utc_ms
    );
}
#[tokio::test(flavor = "current_thread")]
async fn public_v4_probability_maps_public_f4_to_f8_and_frozen_time() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) = setup_public(temp.path());
    let last = m
        .groups
        .iter()
        .rev()
        .find(|(_, at)| *at == T + 202)
        .unwrap()
        .0
        .group_position;
    while driver
        .next_group()
        .unwrap()
        .unwrap()
        .0
        .group()
        .group_position
        < last
    {}
    let mut input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let stats = Arc::make_mut(input.markets[0].statistics.as_mut().unwrap());
    stats
        .spot_variance
        .iter_mut()
        .find(|v| v.window_ms == 300000)
        .unwrap()
        .sigma_usd_per_btc_per_sqrt_second = Some(3.);
    Arc::make_mut(&mut stats.depth).imbalance = StatisticValueV1::known(Decimal::new(25, 2));
    stats.latest_trade_pressure = StatisticValueV1::known(Decimal::new(2, 1));
    stats.basis[0]
        .as_mut()
        .unwrap()
        .innovation_usd_per_btc_numeric_proxy = Decimal::from(3);
    let mut c: ComplementaryPairConfigV4 = serde_json::from_str(include_str!(
        "../../configs/strategies/complementary_pair/public-c0.json"
    ))
    .unwrap();
    c.parameters.probability.binance_pressure_weight_usd = 4.;
    c.parameters.probability.binance_trade_pressure_weight_usd = 5.;
    let build = |c: ComplementaryPairConfigV4| {
        ComplementaryPairV1::new_public(
            c,
            rules.clone(),
            "public-probability".into(),
            StrategyContinuityV2::new_public_account(),
        )
        .unwrap()
    };
    let out = build(c.clone()).decide(&input, T + 217).unwrap();
    let d = &diagnostic(&out).markets[0];
    let ProbabilityEstimateV1::Known {
        mean_usd,
        sigma_usd_per_sqrt_second,
        binance_innovation_usd,
        ..
    } = &d.probability
    else {
        panic!("{d:?}")
    };
    assert_eq!(*sigma_usd_per_sqrt_second, 3.);
    assert_eq!(*binance_innovation_usd, 3.5);
    assert_eq!(*mean_usd, 100003.5);
    assert_eq!(
        d.public_inputs.as_ref().unwrap().observed_at_utc_ms,
        Some(T + 202)
    );
    // Public missing values remain None; strategy fallback does not manufacture factual zeros.
    let s = Arc::make_mut(input.markets[0].statistics.as_mut().unwrap());
    s.spot_variance.clear();
    s.basis = [None, None, None];
    Arc::make_mut(&mut s.depth).imbalance = StatisticValueV1::missing("not_materialized");
    s.latest_trade_pressure = StatisticValueV1::missing("not_materialized");
    let out = build(c.clone()).decide(&input, T + 217).unwrap();
    let d = &diagnostic(&out).markets[0];
    let e = d.public_inputs.as_ref().unwrap();
    assert!(e.initial_sigma_fallback);
    assert_eq!(e.public_sigma, None);
    assert_eq!(e.basis_innovation, None);
    assert_eq!(e.depth_imbalance, None);
    assert_eq!(e.latest_trade_pressure, None);
    assert!(
        matches!(d.probability,ProbabilityEstimateV1::Known {sigma_usd_per_sqrt_second,..} if sigma_usd_per_sqrt_second==c.parameters.probability.initial_sigma_usd_per_sqrt_second)
    );
    // Matching F5 is mandatory only inside the final settlement window.
    input.markets[0].observation.settlement_reference.value =
        Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds);
    let out = build(c.clone()).decide(&input, T + 217).unwrap();
    assert!(matches!(
        diagnostic(&out).markets[0].probability,
        ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::MissingWindowHistory)
    ));
    let s = Arc::make_mut(input.markets[0].statistics.as_mut().unwrap());
    let i = s
        .spot_integrals
        .iter_mut()
        .find(|i| i.window_ms == 60000)
        .unwrap();
    i.covered_from_utc_ms = Some(-30000);
    i.observed_through_utc_ms = T + 202;
    i.integral_usd_per_btc_seconds = StatisticValueV1::known(2_000_000.);
    let out = build(c.clone()).decide(&input, T + 217).unwrap();
    let ProbabilityEstimateV1::Known { mean_usd, .. } = diagnostic(&out).markets[0].probability
    else {
        panic!("F5 matching window must be read")
    };
    assert!((mean_usd - 46330.).abs() < 1e-8); // (2,000,000 + 7.798*100,000)/60
    // Spot wins while present; when absent, matching TWAP60 uses its own F8 channel.
    let original = driver.public_statistics_prefix().unwrap();
    let basis = original.read(&input.markets[0].observation, T + 202).basis[2].clone();
    let mut b = basis.unwrap();
    b.reference = BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds;
    b.innovation_usd_per_btc_numeric_proxy = Decimal::from(8);
    Arc::make_mut(input.markets[0].statistics.as_mut().unwrap()).basis[2] = Some(b);
    input.markets[0].observation.chainlink_spot = Default::default();
    let out = build(c).decide(&input, T + 217).unwrap();
    let d = &diagnostic(&out).markets[0];
    assert_eq!(
        d.public_inputs.as_ref().unwrap().reference_channel,
        Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds)
    );
    assert_eq!(
        d.public_inputs.as_ref().unwrap().basis_innovation,
        Some(Decimal::from(8))
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn public_v4_borrowed_frame_and_veto_preserve_old_order_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) = setup_public(temp.path());
    let first = m
        .groups
        .iter()
        .rev()
        .find(|(_, at)| *at == T + 202)
        .unwrap()
        .0
        .group_position;
    while driver
        .next_group()
        .unwrap()
        .unwrap()
        .0
        .group()
        .group_position
        < first
    {}
    let input = shared
        .strategy_observation_input_with_trade_details(&mut driver, false)
        .unwrap()
        .unwrap();
    let mut owned = new_public_strategy(rules.clone(), true);
    let expected = input.calculate(&mut owned, T + 217).unwrap();
    let mut borrowed_strategy = new_public_strategy(rules.clone(), true);
    let borrowed = shared
        .strategy_borrowed_observation_input(&mut driver, false)
        .unwrap()
        .unwrap();
    let actual = borrowed
        .calculate_borrowed(&mut borrowed_strategy, T + 217)
        .unwrap();
    assert_eq!(
        serde_json::to_value(expected).unwrap(),
        serde_json::to_value(actual).unwrap()
    );
    let old = buy(
        &rules,
        "old-high-maker",
        UP_TOKEN_ID,
        Decimal::from(10),
        Decimal::new(5, 1),
        true,
    );
    shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[old])
        .await
        .unwrap();
    let last = m
        .groups
        .iter()
        .rev()
        .find(|(_, at)| *at == T + 4002)
        .unwrap()
        .0
        .group_position;
    while driver
        .next_group()
        .unwrap()
        .unwrap()
        .0
        .group()
        .group_position
        < last
    {}
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let out = new_public_strategy(rules, true)
        .decide(&input, T + 4017)
        .unwrap();
    let d = &diagnostic(&out).markets[0];
    assert_eq!(
        d.pair_entry.public_entry.as_ref().unwrap().blb_outcome,
        "veto_decreasing",
        "{d:?}"
    );
    assert!(
        !intents(&out)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
    );
    assert!(
        intents(&out)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. })),
        "{d:?}"
    );
    driver.stop_and_drain().unwrap();
}

#[path = "low_price_candidate.rs"]
mod low_price_candidate;
