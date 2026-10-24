//! #360 两合成日期、每日期旧V5/off与连续库存独立账户；只生成有限工程输入。
#[path = "../../examples/support/continuous_fixture.rs"]
mod fixture;
use backtest::*;
use contracts::*;
use data_collect::{source_hourly_layout::*, writer::TypedParquetSegmentWriter};
use serde_json::json;
use sim_system::platform_rules::ImplementationIdentityV1;
use std::path::{Path, PathBuf};
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(
        (3..=5).contains(&args.len()),
        "用法: inventory_candidate_fixture <新输出目录> <每日期点数> <准确ImplementationJSON> [extra_quote_space_ticks] [enable_ordinary_active_replenishment]"
    );
    let extra_quote_space_ticks: u32 = args.get(3).map(|n| n.parse().unwrap()).unwrap_or(0);
    let enable_ordinary_active_replenishment: bool =
        args.get(4).map(|n| n.parse().unwrap()).unwrap_or(true);
    let root = PathBuf::from(&args[0]);
    let count: u64 = args[1].parse().unwrap();
    assert!(count > 0);
    std::fs::create_dir(&root).unwrap();
    let mut days = Vec::new();
    let mut rules = None;
    for (index, date) in ["2026-08-24", "2026-08-25"].into_iter().enumerate() {
        let value = build_day(
            &root.join(date),
            count,
            fixture::common::BASE + index as i64 * 86_400_000,
            date,
            extra_quote_space_ticks,
            enable_ordinary_active_replenishment,
        );
        rules.get_or_insert(value["rules"].clone());
        days.push(value["days"][0].clone());
    }
    let source = rules.unwrap();
    let identity: ImplementationIdentityV1 =
        serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    let sealed = sim_system::platform_rules::derive_historical_maker_v2_snapshot_v3(
        sim_system::platform_rules::HistoricalProcessSnapshotRequestV1 {
            snapshot_path: serde_json::from_value(source["snapshot_path"].clone()).unwrap(),
            expected_file_sha256: serde_json::from_value(source["expected_file_sha256"].clone())
                .unwrap(),
            replay_causal_start_utc_ms: fixture::common::BASE,
        },
        root.join("rules-v2.json"),
        identity.clone(),
    )
    .unwrap();
    for day in &mut days {
        for c in day["candidates"].as_array_mut().unwrap() {
            c["run"]["implementation_identity"] = serde_json::to_value(&identity).unwrap();
        }
    }
    let value = json!({"rules":{"snapshot_path":sealed.path(),"expected_file_sha256":sealed.file_sha256(),"replay_causal_start_utc_ms":fixture::common::BASE},
        "parallel_dates":2,"days":days});
    std::fs::write(
        root.join("days.json"),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
    println!("{}", root.join("days.json").display());
}
fn build_day(
    root: &Path,
    count: u64,
    base: i64,
    utc_day: &str,
    extra_quote_space_ticks: u32,
    enable_ordinary_active_replenishment: bool,
) -> serde_json::Value {
    std::fs::create_dir(&root).unwrap();
    let (mut run, rules) =
        fixture::common::setup_day_with_embedded_rules(root, base, utc_day, false);
    // PM按一个正式源头序列生成，不能把较早时间追加到基础文件未来源序之后。
    let mut pm = vec![
        fixture::common::book(
            fixture::common::FIRST,
            "20",
            base + 1100,
            base + 1100,
            1,
            "0.6",
        ),
        fixture::common::book(
            fixture::common::FIRST,
            "3",
            base + 1100,
            base + 1100,
            2,
            "0.6",
        ),
        fixture::common::book(
            fixture::common::FIRST,
            "3",
            base + 1300,
            base + 1300,
            3,
            "0.65",
        ),
        fixture::common::book(
            fixture::common::FIRST,
            "20",
            base + 1701,
            base + 2200,
            4,
            "0.5",
        ),
    ];
    let mut depths = vec![];
    let mut updates = vec![];
    let mut spots = vec![];
    let mut trades = vec![];
    for n in 0..count {
        let at = base + 2200 + (n * 7000 / count) as i64;
        let id = n + 1;
        for (leg, token) in ["20", "3"].into_iter().enumerate() {
            let mut row = fixture::common::book(
                fixture::common::FIRST,
                token,
                at,
                at,
                5 + n * 2 + leg as u64,
                "0.6",
            );
            if at >= base + 2500 {
                if let data_collect::polymarket_orderbook_v3::PolymarketOrderbookV3Event::Book(
                    book,
                ) = &mut row.event
                {
                    book.bids[0].price = match (n / 25 + leg as u64) % 4 {
                        0 => "0.40",
                        1 => "0.38",
                        2 => "0.36",
                        _ => "0.39",
                    }
                    .into();
                }
            }
            pm.push(row);
            if n % 25 == 0 {
                use data_collect::polymarket_orderbook_v3::*;
                let mut row =
                    fixture::common::book(fixture::common::FIRST, token, at, at, 0, "0.6");
                row.event =
                    PolymarketOrderbookV3Event::LastTradePrice(PolymarketLastTradePriceV3 {
                        asset_id: token.into(),
                        price: "0.35".into(),
                        size: Some("105".into()),
                        side: Some("SELL".into()),
                        fee_rate_bps: None,
                    });
                pm.push(row);
            }
        }
        depths.push(BinanceDepthSnapshotHourlyRow {
            timestamp_received: at,
            snapshot_fetch_start_utc: at - 1,
            last_update_id: id * 10,
            bids: format!(r#"[["100","{}"]]"#, if n % 2 == 0 { 5 } else { 3 }),
            asks: r#"[["102","3"]]"#.into(),
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: Some("finite-depth".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(id * 2 - 1),
            collector_admission_sequence: Some(id * 2 - 1),
            collector_admission_time_utc_ms: Some(at + 1),
            collector_host_id: "synthetic".into(),
            collector_region: "test".into(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.into(),
        });
        updates.push(BinanceDepthUpdateHourlyRow {
            timestamp_received: at,
            event_type: "depthUpdate".into(),
            event_time: at,
            symbol: "BTCUSDT".into(),
            first_update_id: id * 10 + 1,
            final_update_id: id * 10 + 1,
            bids: format!(r#"[["100","{}"]]"#, if n % 2 == 0 { 5 } else { 3 }),
            asks: "[]".into(),
            stream: "btcusdt@depth".into(),
            snapshot_last_update_id: Some(id * 10),
            local_u_before: None,
            stitch_verdict: "applied".into(),
            stitch_evidence: data_collect::sources::binance::assess_depth_sequence(
                Some(id * 10),
                None,
                id * 10 + 1,
                id * 10 + 1,
            )
            .unwrap()
            .stitch_evidence,
            stitch_gap_reason: None,
            source_sequence_epoch_id: Some("finite-depth".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(id * 2),
            collector_admission_sequence: Some(id * 2),
            collector_admission_time_utc_ms: Some(at + 1),
            collector_host_id: "synthetic".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".into(),
            schema_version: BINANCE_DEPTH_UPDATE_SCHEMA_VERSION.into(),
        });
        spots.push(RtdsChainlinkBtcUsdHourlyRow {
            timestamp_received: at,
            message_timestamp: at,
            payload_timestamp: at,
            age_ms: Some(0),
            symbol: "btc/usd".into(),
            value: "100".into(),
            topic: "crypto_prices_chainlink".into(),
            message_type: "update".into(),
            freshness_status: "fresh".into(),
            subscription_filter: "{}".into(),
            source_native_payload_json:
                json!({"symbol":"btc/usd","value":"100","timestamp":at,"synthetic_ordinal":id})
                    .to_string(),
            collector_host_id: "synthetic".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: data_pipeline_core::typed_raw::POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA
                .into(),
        });
        trades.push(BinanceAggTradeHourlyRow {record_kind:"trade".into(),timestamp_received:at,rest_backfill_timestamp_received:None,ingest_source:"websocket".into(),event_type:Some("aggTrade".into()),event_time:Some(at),trade_time:Some(at),symbol:Some("BTCUSDT".into()),aggregate_trade_id:Some(id),price:Some("101".into()),quantity:Some("2".into()),first_trade_id:Some(id*2),last_trade_id:Some(id*2+1),buyer_is_maker:Some(false),stream:Some("btcusdt@aggTrade".into()),ws_ignore:Some(false),rest_was_best_price_match:None,source_native_payload_json:json!({"e":"aggTrade","E":at,"T":at,"s":"BTCUSDT","a":id,"p":"101","q":"2","f":id*2,"l":id*2+1,"m":false}).to_string(),source_sequence_epoch_id:"finite-trade".into(),connection_generation:1,websocket_ingress_sequence:Some(id),rest_response_ingress_sequence:None,rest_response_item_index:None,collector_admission_sequence:id,collector_admission_time_utc_ms:at+1,invalid_reason_code:None,invalid_reason_detail:None,payload_encoding:None,payload_length_bytes:None,payload_sha256:None,payload_capture_status:None,collector_host_id:"synthetic".into(),collector_region:"test".into(),source_endpoint:"wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into(),schema_version:BINANCE_AGGTRADE_SCHEMA_VERSION.into()});
    }
    pm.push(fixture::common::book(
        fixture::common::SECOND,
        "21",
        base + 10100,
        base + 10200,
        5 + count * 2,
        "0.6",
    ));
    pm.push(fixture::common::book(
        fixture::common::SECOND,
        "4",
        base + 10100,
        base + 10200,
        6 + count * 2,
        "0.6",
    ));
    let manifest = root.join("inputs/manifest.json");
    let mut m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    m["files"]
        .as_array_mut()
        .unwrap()
        .retain(|f| f["source_kind"] != "polymarket_market");
    std::fs::write(&manifest, serde_json::to_vec(&m).unwrap()).unwrap();
    for (index, row) in pm.iter_mut().enumerate() {
        row.envelope.collector_admission_sequence = index as u64 + 1;
    }
    fixture::append_pm(&root, &pm, 1);
    let sources = [
        (
            "binance_depth_snapshot",
            BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        ),
        ("binance_depth_update", BINANCE_DEPTH_UPDATE_SCHEMA_VERSION),
        (
            "polymarket_rtds_chainlink_btc_usd",
            data_pipeline_core::typed_raw::POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA,
        ),
        ("binance_aggtrade", BINANCE_AGGTRADE_SCHEMA_VERSION),
    ];
    for (source, version) in sources {
        let mut w = TypedParquetSegmentWriter::create_for_typed_source(
            &root.join("inputs"),
            source,
            version,
            source,
        )
        .unwrap();
        match source {
            "binance_depth_snapshot" => w.write_binance_depth_snapshot_rows(&depths).unwrap(),
            "binance_depth_update" => w.write_binance_depth_update_rows(&updates).unwrap(),
            "binance_aggtrade" => w.write_binance_aggtrade_rows(&trades).unwrap(),
            _ => w.write_rtds_chainlink_btc_usd_rows(&spots).unwrap(),
        };
        fixture::finish_source(&root, w, source, version, 0, count as usize);
    }
    // 沿正式Gamma锚字段，关闭TWAP结算；成本夹具不冒充具备60秒左支撑。
    use data_collect::polymarket_market_metadata_v6_layout::*;
    use sha2::{Digest, Sha256};
    let at = base + 2000;
    let body = r#"{"eventMetadata":{"priceToBeat":100}}"#;
    let row=PolymarketMarketMetadataV6Row::from_raw_value(json!({"schema_version":POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,"metadata_row_kind":"market_anchor_rule_update","timestamp_received":at,"metadata_origin":"gamma_event_by_slug","collector_host_id":"synthetic","collector_region":"test","slug":"btc-updown-5m-engineering","condition_id":fixture::common::FIRST,"clob_market_id":"3195576","asset_ids":"[\"20\",\"3\"]","market_start_utc":base+1000,"market_end_utc":base+10000,"fixed_identity_verified_at_utc":base-1,"market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":at-10,"anchor_query_source_epoch_id":"finite-anchor","anchor_query_ordinal":1,"anchor_query_result":"observed","price_to_beat_decimal":"100","price_to_beat_source_field":"eventMetadata.priceToBeat","twap_enabled":false,"http_received_at_utc":at,"http_request_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering","http_body_encoding":"utf8","http_body":body,"http_body_sha256":format!("{:x}",Sha256::digest(body.as_bytes()))}),4,at+1).unwrap();
    let mut w = TypedParquetSegmentWriter::create_for_typed_source(
        &root.join("inputs"),
        "anchor",
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    w.write_polymarket_market_metadata_v6_rows(&[row]).unwrap();
    fixture::finish_source(
        &root,
        w,
        "polymarket_market_metadata",
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
        1,
        1,
    );
    run.preparation.canonical.sort.max_run_rows = 1000;
    run.preparation.canonical.sort.max_temp_run_files = 10000;
    run.preparation.canonical.sort.max_temp_bytes = 1 << 30;
    run.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(0);
    run.timing.calculation_duration_ms = DelayScheduleV1::Fixed(15);
    run.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
    let statistics: PublicStatisticsConfigV1 = serde_json::from_str(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/public-statistics.json"
    ))
    .unwrap();
    run.public_statistics = Some(statistics);
    let version = "candidate_v5_public_low_price";
    let old_parameters = include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/public-low-price-off.json"
    );
    let mut new = serde_json::to_value(
        baseline_mm::ContinuousInventoryConfigV1::parse_toml(include_str!(
            "../../../baseline-mm/configs/strategies/continuous_inventory/adopted-v1.toml"
        ))
        .unwrap(),
    )
    .unwrap();
    new["extra_quote_space_ticks"] = json!(extra_quote_space_ticks);
    new["enable_ordinary_active_replenishment"] = json!(enable_ordinary_active_replenishment);
    // 工程小窗的全部生效值单独保存；不冒充研究完整八小时参数。
    new["probability_reference"]["binance_innovation_weight"] = json!(0.0);
    for (key, value) in new["health"].as_object_mut().unwrap() {
        *value = json!(if key.contains("normal") { 0 } else { 1_000_000 });
    }
    let configurations = [
        (
            "old-v5-off",
            serde_json::from_str::<serde_json::Value>(old_parameters).unwrap(),
        ),
        ("continuous", new),
    ];
    let candidates = configurations.map(|(name, parameters)| {
        let mut r = HistoricalPreparedRunConfigV1::from(&run);
        r.session.account_id = name.into();
        r.session.session_id = name.into();
        r.session.run_id = format!("{utc_day}-{name}");
        let out = root.join(name);
        std::fs::create_dir(&out).unwrap();
        r.sim_log = out.join("sim.jsonl");
        r.noe_root = out.join("noe");
        r.ledger_root = out.join("ledger");
        r.metadata_path = out.join("metadata.json");
        r.result_path = out.join("result.json");
        r.research_evidence_path = Some(out.join("decisions.jsonl"));
        r.strategy.strategy_id = if name == "continuous" {
            baseline_mm::CONTINUOUS_INVENTORY_STRATEGY_ID
        } else {
            baseline_mm::STRATEGY_ID
        }
        .into();
        r.strategy.strategy_rule_version = if name == "continuous" {
            serde_json::from_value::<baseline_mm::ContinuousInventoryConfigV1>(parameters.clone())
                .unwrap()
                .rule_version()
        } else {
            version
        }
        .into();
        json!({"run":r,"strategy":parameters,"request_namespace":format!("{utc_day}-{name}")})
    });
    let value = json!({"rules":{"snapshot_path":rules.path(),"expected_file_sha256":rules.file_sha256(),"replay_causal_start_utc_ms":run.preparation.scope.received_start_utc_ms},"parallel_dates":1,"days":[{"preparation":run.preparation,"producer_lead_groups":32,"candidates":candidates}]});
    value
}
