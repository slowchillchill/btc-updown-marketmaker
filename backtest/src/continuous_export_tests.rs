use super::*;
#[path = "../examples/support/continuous_fixture.rs"]
mod fixture;
use std::sync::Mutex;
const BASE: i64 = 1787529600000;
fn run(config: &ContinuousPublicExportConfigV1) -> (ContinuousPublicExportProgressV1, Vec<Value>) {
    let mut out = vec![];
    let state = export_continuous_public_v1(
        config,
        |v| {
            out.push(v);
            Ok(())
        },
        |_| {},
        || false,
    )
    .unwrap();
    (state, out)
}
#[test]
fn real_parquet_continuous_zero_account_same_ms_queries_and_compact_scalars() {
    let temp = tempfile::tempdir().unwrap();
    let c = fixture::build(temp.path(), 4);
    let (state, out) = run(&c);
    assert!(state.natural_end);
    assert!(!state.stopped);
    assert!(state.processed_inputs >= 13);
    assert_eq!(state.new_trades, 3); // 缺size消息未接纳。
    let trades = out
        .iter()
        .filter(|v| v["record_type"] == "trade")
        .collect::<Vec<_>>();
    assert_eq!(trades.len(), 3);
    assert!(
        trades
            .iter()
            .all(|v| v["accepted"]["trade"]["quantity_shares"] == "2")
    );
    assert!(
        trades
            .iter()
            .all(|v| v["accepted"]["trade"]["aggressor_side"] == "SELL")
    );
    let observations = out
        .iter()
        .filter(|v| v["record_type"] == "observation" && v["observed_at_utc_ms"] == BASE + 1201)
        .collect::<Vec<_>>();
    assert_eq!(observations.len(), 2);
    assert_ne!(observations[0]["group"], observations[1]["group"]);
    let query = out
        .iter()
        .find(|v| v["record_type"] == "query" && v["observed_at_utc_ms"] == BASE + 1201)
        .unwrap();
    assert_eq!(query["group"], observations[1]["group"]); // 同毫秒右端消费全部组。
    let boundaries = out
        .iter()
        .filter(|v| v["record_type"] == "query_boundary")
        .collect::<Vec<_>>();
    assert_eq!(boundaries[0]["input_frontier_past_query"], true);
    assert_eq!(
        boundaries.last().unwrap()["input_frontier_past_query"],
        false
    );
    let text = serde_json::to_string(&out).unwrap();
    assert!(!text.contains("visible_positive_levels"));
    assert!(!text.contains("\"group_events\""));
    assert!(!c.preparation.work_root.exists());
    assert!(!temp.path().join("sim.jsonl").exists());
    assert!(!temp.path().join("ledger").exists());
    let mut quiet = c.clone();
    quiet.observe_after_groups = false;
    quiet.export_new_trades = false;
    let (silent, rows) = run(&quiet);
    assert_eq!(state.applied_groups, silent.applied_groups);
    assert_eq!(state.new_trades, silent.new_trades);
    assert_eq!(state.last_group, silent.last_group);
    assert!(rows.iter().all(|v| v["record_type"] != "trade"));
}
#[test]
fn end_input_delay_not_truncated_and_queries_before_first_or_outside_market_are_explicit() {
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture::build(temp.path(), 0);
    c.public_delivery_delay_ms = DelayScheduleV1::Fixed(20000);
    c.query_times_utc_ms = vec![BASE - 1, BASE + 12000, BASE + 50000];
    let (state, out) = run(&c);
    assert!(state.last_applied_at_utc_ms.unwrap() > c.preparation.scope.received_end_utc_ms);
    let q = out
        .iter()
        .filter(|v| v["record_type"] == "query_boundary")
        .collect::<Vec<_>>();
    assert_eq!(q.len(), 3);
    assert_eq!(q[0]["has_applied_prefix"], false);
    assert_eq!(q[1]["input_frontier_past_query"], false);
    assert_eq!(q[2]["input_frontier_past_query"], false);
    assert!(out.iter().all(|v| v["record_type"] != "query"));
}
#[test]
fn preparation_and_running_stop_preserve_truthful_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let c = fixture::build(temp.path(), 4);
    let stop = std::sync::atomic::AtomicBool::new(false);
    let mut rows = vec![];
    let stopped = export_continuous_public_v1(
        &c,
        |v| {
            rows.push(v);
            Ok(())
        },
        |p| {
            if p.stage == "order_location" && p.scanned_rows > 0 {
                stop.store(true, Ordering::Relaxed);
            }
        },
        || stop.load(Ordering::Relaxed),
    )
    .unwrap();
    assert!(stopped.stopped);
    assert!(!stopped.natural_end);
    assert_eq!(stopped.applied_groups, 0);
    let stop = std::sync::atomic::AtomicBool::new(false);
    let stages = Mutex::new(vec![]);
    let stopped = export_continuous_public_v1(
        &c,
        |_| Ok(()),
        |p| {
            stages.lock().unwrap().push(p.stage);
            if p.applied_groups >= 3 {
                stop.store(true, Ordering::Relaxed);
            }
        },
        || stop.load(Ordering::Relaxed),
    )
    .unwrap();
    assert!(stopped.stopped);
    assert!(!stopped.natural_end);
    assert_eq!(stopped.applied_groups, 3);
    assert!(stopped.last_group.is_some());
}
fn query_states(rows: &[Value]) -> Vec<Value> {
    let mut states = BTreeMap::<String, serde_json::Map<String, Value>>::new();
    let mut out = vec![];
    for row in rows {
        let kind = row["record_type"].as_str().unwrap();
        if kind == "observation" || kind == "query" {
            let state = states
                .entry(row["condition_id"].as_str().unwrap().into())
                .or_default();
            for (k, v) in row["changes"].as_object().unwrap() {
                state.insert(k.clone(), v.clone());
            }
            if kind == "query" {
                let mut projection = state.clone();
                for v in projection.values_mut() {
                    strip_anchor_refs(v);
                }
                out.push(
                    json!({"group":row["group"],"t":row["observed_at_utc_ms"],"state":projection}),
                );
            }
        }
    }
    out
}
fn strip_anchor_refs(v: &mut Value) {
    match v {
        Value::Object(map) => {
            map.remove("anchor_refs");
            for v in map.values_mut() {
                strip_anchor_refs(v)
            }
        }
        Value::Array(a) => {
            for v in a {
                strip_anchor_refs(v)
            }
        }
        _ => (),
    }
}
#[test]
fn real_parquet_initialization_selection_and_query_output_switches_keep_statistics() {
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture::initialized(temp.path());
    let (state, rows) = run(&c);
    assert!(state.natural_end);
    assert!(
        !rows
            .iter()
            .filter(|v| v["record_type"] == "query")
            .collect::<Vec<_>>()
            .is_empty()
    );
    c.observe_after_groups = false;
    let (_, query_only) = run(&c);
    assert_eq!(query_states(&rows), query_states(&query_only));
    assert!(rows.iter().any(|r| {
        r["record_type"] == "identity"
            && r["initialization_files"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
    }));
}
#[test]
fn formal_duplicate_and_missing_size_are_not_reaccepted_and_valid_empty_book_is_zero() {
    use data_collect::polymarket_orderbook_v3::*;
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture::build(temp.path(), 0);
    c.preparation.scope.received_end_utc_ms = BASE + 10000;
    let mut trade = fixture::pm(10, 2300);
    trade.event = PolymarketOrderbookV3Event::LastTradePrice(PolymarketLastTradePriceV3 {
        asset_id: "20".into(),
        price: "0.4".into(),
        size: Some("2".into()),
        side: Some("SELL".into()),
        fee_rate_bps: None,
    });
    fixture::append_pm(temp.path(), &[trade.clone()], 2);
    fixture::append_pm(temp.path(), &[trade], 3);
    let mut empty = fixture::pm(11, 2400);
    if let PolymarketOrderbookV3Event::Book(b) = &mut empty.event {
        b.bids.clear();
        b.asks.clear();
    }
    fixture::append_pm(temp.path(), &[empty], 4);
    let (state, rows) = run(&c);
    assert_eq!(state.new_trades, 1);
    assert_eq!(
        rows.iter().filter(|r| r["record_type"] == "trade").count(),
        1
    );
    let observations = rows
        .iter()
        .filter(|r| r["record_type"] == "observation")
        .collect::<Vec<_>>();
    assert!(observations.iter().any(|r| {
        r["changes"]["assets"]
            .as_array()
            .is_some_and(|a| a.iter().any(|v| v["bid_materialized"] == false))
    }));
    assert!(
        observations
            .iter()
            .any(|r| r["changes"]["assets"]
                .as_array()
                .is_some_and(|a| a
                    .iter()
                    .any(|v| v["asset"]["token_id"] == "20"
                        && v["book_best_bid_state"] == "known_empty")))
    );
    assert!(
        observations
            .iter()
            .any(|r| r["changes"]["statistics/pm_quantities"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v["asset"]["token_id"] == "20"
                    && v["bid_shares"]["value"] == json!(rust_decimal::Decimal::ZERO)))),
        "PM quantities: {:?}",
        observations
            .iter()
            .map(|r| &r["changes"]["statistics/pm_quantities"])
            .collect::<Vec<_>>()
    );
}
#[test]
fn same_prefix_direct_read_matches_exported_public_statistics() {
    let temp = tempfile::tempdir().unwrap();
    let c = fixture::build(temp.path(), 0);
    let (_, rows) = run(&c);
    let input = data_pipeline_training::freeze_public_source_input_set_v1(
        &c.preparation.scope.manifest_paths,
    )
    .unwrap();
    let admitted = input.admitted_daily_inputs();
    let files = admitted.iter().flat_map(|d| d.files()).collect::<Vec<_>>();
    let ingress = DayIngressV1::open(
        &files,
        c.preparation.header.input_stream_id.clone(),
        c.preparation.read_page_rows,
        c.preparation.max_read_batch_bytes,
        c.preparation.scope.received_start_utc_ms..c.preparation.scope.received_end_utc_ms,
        vec![],
        c.preparation.canonical.max_epoch_count_per_source,
    )
    .unwrap();
    let mut producer = DayProducerV1::new_public_only(
        ingress,
        {
            let mut h = c.preparation.effective_header();
            h.input_scope = format!(
                "{};received=[{},{});source={}",
                h.input_scope,
                c.preparation.scope.received_start_utc_ms,
                c.preparation.scope.received_end_utc_ms,
                input.input_set_identity_sha256()
            );
            h
        },
        Sha256Hex(input.input_set_identity_sha256().into()),
    )
    .unwrap();
    producer
        .core
        .set_observation_requirements(PublicObservationRequirementsV1::FULL);
    producer.core.enable_source_progress();
    let identity: PublicStatisticsIdentityV1 = serde_json::from_value(
        rows.iter()
            .find(|r| r["record_type"] == "identity")
            .unwrap()["statistics_identity"]
            .clone(),
    )
    .unwrap();
    let stream = PublicStatisticsStreamV1::new(identity, c.statistics.clone()).unwrap();
    let mut times = DeliveryTimes {
        delays: c.public_delivery_delay_ms.clone(),
        last: None,
    };
    let mut state = BTreeMap::<String, serde_json::Map<String, Value>>::new();
    let mut snapshots = BTreeMap::new();
    for r in &rows {
        if r["record_type"] == "observation" {
            let market = r["condition_id"].as_str().unwrap().to_string();
            let s = state.entry(market.clone()).or_default();
            for (k, v) in r["changes"].as_object().unwrap() {
                s.insert(k.clone(), v.clone());
            }
            snapshots.insert(
                (r["group"]["group_position"].as_u64().unwrap(), market),
                s.clone(),
            );
        }
    }
    let mut checked = 0;
    while let Some(a) = producer.next_applied().unwrap() {
        for g in &a.applied.closed_groups {
            let reference = PublicGroupReferenceV1 {
                input_stream_id: a.applied.applied_prefix.input_stream_id.clone(),
                applied_prefix: a.applied.applied_prefix.clone(),
                group_position: g.group_position,
                group_id: g.group_id.clone(),
                closed_at_input_position: g.closed_at_input_position,
            };
            let at = times
                .execution_time_for_visible_group(
                    &reference,
                    original_public_group_visible_at_utc_ms(&a.input, g),
                )
                .unwrap();
            if let Some(h) = PreparedPublicStatisticsV1::default()
                .consume(&stream, reference, at, g)
                .unwrap()
            {
                for o in g.observations.iter() {
                    if let Some(s) =
                        snapshots.get(&(g.group_position, o.market.condition_id.0.clone()))
                    {
                        let direct = serde_json::to_value(h.read(&o, at)).unwrap();
                        for key in [
                            "depth",
                            "momentum",
                            "spot_variance",
                            "spot_integrals",
                            "latest_trade_pressure",
                            "basis",
                            "pm_quantities",
                            "source_applied_at_utc_ms",
                            "source_received_age_ms",
                        ] {
                            assert_eq!(s[&format!("statistics/{key}")], direct[key]);
                        }
                        checked += 1;
                    }
                }
            }
        }
    }
    assert!(checked >= 6);
}
#[test]
fn parquet_real_application_clock_same_ms_last_value_and_silent_window_left_support() {
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture::build(temp.path(), 0);
    fixture::append_reference_inputs(temp.path());
    c.query_times_utc_ms = vec![
        BASE + 2600,
        BASE + 2601,
        BASE + 3601,
        BASE + 3602,
        BASE + 4601,
    ];
    let (_, rows) = run(&c);
    let queries = query_states(&rows);
    let at = |t| queries.iter().find(|q| q["t"] == BASE + t).unwrap();
    assert_eq!(
        at(2600)["state"]["chainlink_spot"]["value"]["price_usd_per_btc"],
        "100"
    );
    // 来源接收2500会被depth准入2501单调化；旧2600冻结不见未来组。
    // 同接收时刻按正式来源身份排序，此夹具104在103之前应用，而非Parquet物理行序。
    assert_eq!(
        at(2601)["state"]["chainlink_spot"]["value"]["price_usd_per_btc"],
        "103"
    );
    assert_eq!(
        at(2601)["state"]["statistics/spot_variance"][0]["statistics"]["squared_change_sum_usd_squared"],
        9.0
    );
    assert_eq!(
        at(3601)["state"]["statistics/spot_variance"][0]["statistics"]["squared_change_sum_usd_squared"],
        9.0
    );
    assert_eq!(
        at(3602)["state"]["statistics/spot_variance"][0]["sigma_usd_per_btc_per_sqrt_second"],
        0.0
    );
    assert!(
        at(4601)["state"]["statistics/spot_variance"][0]["sigma_usd_per_btc_per_sqrt_second"]
            .is_null()
    );
    assert_eq!(
        at(3602)["state"]["statistics/momentum"][2]["change_usdt_per_btc"]["value"],
        "0"
    );
}
#[test]
fn parquet_late_k_never_backfills_frozen_blb_and_at_anchor_keeps_absent_aggregates() {
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture::build(temp.path(), 0);
    fixture::append_reference_inputs(temp.path());
    fixture::append_late_anchor(temp.path());
    c.query_times_utc_ms = vec![BASE + 1601, BASE + 2101, BASE + 2601];
    let (_, rows) = run(&c);
    let q = query_states(&rows);
    let at = |t| q.iter().find(|v| v["t"] == BASE + t).unwrap();
    assert!(at(1601)["state"]["official_anchor"]["value"].is_null());
    assert!(at(1601)["state"]["statistics/blb"]["official_k_usd_per_btc"].is_null());
    assert_eq!(at(2101)["state"]["statistics/blb"]["at_anchor"], true);
    assert!(at(2101)["state"]["statistics/blb"]["visible_quantity_btc"].is_null());
    assert_eq!(at(2601)["state"]["statistics/blb"]["side"], "bid");
    assert_eq!(
        at(2601)["state"]["statistics/blb"]["visible_level_count"],
        1
    );
    assert_eq!(
        at(2601)["state"]["statistics/blb"]["visible_quantity_btc"],
        "5"
    );
    assert!(at(1601)["state"]["statistics/blb"]["official_k_usd_per_btc"].is_null());
}
#[test]
fn parquet_bba_valuation_does_not_replace_nonempty_l2_best_bid() {
    use data_collect::polymarket_orderbook_v3::*;
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture::build(temp.path(), 0);
    c.preparation.scope.received_end_utc_ms = BASE + 10000;
    let mut book = fixture::pm(7, 2300);
    if let PolymarketOrderbookV3Event::Book(b) = &mut book.event {
        b.bids[0].price = "0.5".into();
    }
    let mut bba = fixture::pm(8, 2400);
    bba.event = PolymarketOrderbookV3Event::BestBidAsk(PolymarketBestBidAskV3 {
        asset_id: "20".into(),
        best_bid: Some("0.4".into()),
        best_ask: Some("0.6".into()),
        spread: Some("0.2".into()),
    });
    let mut no_bid = fixture::pm(9, 2500);
    no_bid.event = PolymarketOrderbookV3Event::BestBidAsk(PolymarketBestBidAskV3 {
        asset_id: "20".into(),
        best_bid: None,
        best_ask: Some("0.6".into()),
        spread: None,
    });
    fixture::append_pm(temp.path(), &[book, bba, no_bid], 2);
    let (state, rows) = run(&c);
    assert!(state.natural_end);
    let asset_at = |t| {
        rows.iter()
            .find(|r| r["record_type"] == "observation" && r["observed_at_utc_ms"] == BASE + t)
            .unwrap()["changes"]["assets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["asset"]["token_id"] == "20")
            .unwrap()
    };
    let changed_bba = asset_at(2501);
    assert_eq!(changed_bba["book_best_bid"], "0.5");
    assert_eq!(changed_bba["book_best_bid_quantity_shares"], "100");
    assert_eq!(changed_bba["valuation_best_bid"]["value"], "0.4");
    assert_eq!(
        changed_bba["direct_bba"]["value"]["best_bid_pusd_per_share"],
        "0.4"
    );
    let missing_bba = asset_at(2601);
    assert_eq!(missing_bba["book_best_bid"], "0.5");
    assert_eq!(missing_bba["book_best_bid_state"], "price");
    assert_eq!(missing_bba["book_best_bid_quantity_shares"], "100");
    assert!(missing_bba["valuation_best_bid"]["value"].is_null());
    assert_eq!(missing_bba["valuation_best_bid_state"], "known_empty");
    assert_eq!(missing_bba["bid_materialized"], true);
}
