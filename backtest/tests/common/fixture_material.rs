use backtest::*;
use contracts::*;
use data_collect::{
    polymarket_market_metadata_layout::{
        PolymarketMarketMetadataRow, PolymarketMarketMetadataV5Row,
    },
    polymarket_orderbook_v3::{
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketBookV3, PolymarketOrderbookV3Envelope,
        PolymarketOrderbookV3Event, PolymarketOrderbookV3Message, PolymarketPriceLevelV3,
    },
    writer::{PolymarketOrderbookV3WriteSequenceState, TypedParquetSegmentWriter},
};
use data_pipeline_training::{
    SortConfig, canonical_run::CanonicalRunConfig, gate_a::GateAOutputConfig,
};
use ledger::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use sim_system::platform_rules::*;
use std::{path::Path, sync::Arc};
pub(crate) const BASE: i64 = 1787529600000;
pub(crate) const FIRST: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
pub(crate) const SECOND: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";
pub(crate) fn metadata(
    condition: &str,
    clob: &str,
    tokens: [&str; 2],
    at: i64,
    start: i64,
    end: i64,
    sequence: u64,
) -> PolymarketMarketMetadataV5Row {
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    let mut row:PolymarketMarketMetadataRow=serde_json::from_value(json!({
        "metadata_row_kind":"metadata_snapshot", "timestamp_received":at, "metadata_origin":"gamma_markets_discovery", "collector_host_id":"synthetic-collector", "collector_region":"test", "schema_version":"polymarket_market_metadata_v5", "slug":"btc-updown-5m-engineering", "condition_id":condition, "clob_market_id":clob, "asset_ids":serde_json::to_string(&tokens).unwrap(), "market_start_utc":start, "market_end_utc":end, "fixed_identity_verified_at_utc":at-1, "market_lifecycle_state":"prefetched", "market_runtime_role":"next", "source_endpoint":"https://gamma-api.polymarket.com/markets", "source_evidence_origin":"gamma_markets_discovery", "source_evidence_event_type":"gamma_markets_discovery_response", "source_evidence_received_at_utc":at, "row_evidence_validation_completed_at_utc":at+1, "source_evidence_payload_json":payload, "source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())), "metadata_fetch_start_utc":at-10, "closed":false, "outcomes_raw":"[\"UP\",\"DOWN\"]", "outcome_prices_raw":"[\"0.5\",\"0.5\"]", "active":true, "accepting_orders":true, "collector_admission_sequence":sequence, "collector_admission_time_utc_ms":at+1
    })).unwrap();
    row.collector_admission_sequence = sequence;
    row.collector_admission_time_utc_ms = at + 1;
    PolymarketMarketMetadataV5Row::from_common(row, None, None)
}
pub(crate) fn book(
    condition: &str,
    token: &str,
    source: i64,
    received: i64,
    sequence: u64,
    ask: &str,
) -> PolymarketOrderbookV3Message {
    PolymarketOrderbookV3Message {
        envelope: PolymarketOrderbookV3Envelope {
            timestamp_received: received,
            timestamp: Some(source),
            market: condition.into(),
            slug: Some("btc-updown-5m-engineering".into()),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
            source_sequence_epoch_id: "synthetic-epoch".into(),
            collector_admission_sequence: sequence,
            collector_admission_time_utc_ms: received + 1,
        },
        event: PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
            asset_id: token.into(),
            bids: vec![PolymarketPriceLevelV3 {
                price: "0.4".into(),
                size: "100".into(),
            }],
            asks: vec![PolymarketPriceLevelV3 {
                price: ask.into(),
                size: "100".into(),
            }],
            last_trade_price: None,
        }),
    }
}
// 仅终态专用夹具使用原 collector 定时状态行；不改变普通夹具。
fn local_transition(base: i64, end: bool) -> PolymarketMarketMetadataV5Row {
    let at = base + if end { 10000 } else { 1000 };
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    let mut row: PolymarketMarketMetadataRow = serde_json::from_value(json!({
        "metadata_row_kind": "market_state_transition", "timestamp_received": at,
        "metadata_origin": "local_market_state_timer", "collector_host_id": "synthetic-collector", "collector_region": "test",
        "schema_version": "polymarket_market_metadata_v5", "slug": "btc-updown-5m-engineering",
        "condition_id": FIRST, "clob_market_id": "3195576", "asset_ids": "[\"20\",\"3\"]",
        "market_start_utc": base+1000, "market_end_utc": base+10000, "fixed_identity_verified_at_utc": base-1,
        "market_lifecycle_state": "active", "market_runtime_role": if end { "resolution_pending" } else { "current" },
        "source_endpoint": "https://gamma-api.polymarket.com/markets",
        "source_evidence_origin": "gamma_markets_discovery", "source_evidence_event_type": "gamma_markets_discovery_response",
        "source_evidence_received_at_utc": base, "source_evidence_raw_sha256": format!("{:x}",Sha256::digest(payload.as_bytes())),
        "market_lifecycle_transition": if end { None } else { Some("prefetched_to_active") },
        "lifecycle_scheduled_at_utc": if end { None } else { Some(at) },
        "lifecycle_applied_at_utc": if end { None } else { Some(at) },
        "market_runtime_role_transition": if end { "current_to_resolution_pending" } else { "next_to_current" },
        "runtime_role_scheduled_at_utc": at, "runtime_role_applied_at_utc": at,
        "active_unavailable_reason": "not_applicable_for_row_kind", "response_fields_unavailable_reason": "not_applicable_for_row_kind",
        "resolution_budget_horizon_utc": if end { Some(base+10000+8*60_000) } else { None },
        "resolution_followup_status": if end { Some("pending") } else { None }
    })).unwrap();
    row.collector_admission_sequence = if end { 4 } else { 2 };
    row.collector_admission_time_utc_ms = at + 1;
    let row = PolymarketMarketMetadataV5Row::from_common(row, None, None);
    let validation = row.validate();
    assert!(
        validation.is_complete(),
        "collector 正式定时状态: {validation:?}"
    );
    row
}
fn material(
    root: &Path,
    base: i64,
    utc_day: &str,
    terminal: Option<(PolymarketMarketMetadataV5Row, PolymarketOrderbookV3Message)>,
    maker_trade: bool,
    first_market_end_utc_ms: Option<i64>,
) -> std::path::PathBuf {
    let day = root.join("inputs");
    std::fs::create_dir(&day).unwrap();
    let mut metadata_writer = TypedParquetSegmentWriter::create_for_typed_source(
        &day,
        "metadata",
        "polymarket_market_metadata_v5",
        "polymarket_market_metadata",
    )
    .unwrap();
    let (terminal_metadata, terminal_pm) =
        terminal.map_or((None, None), |(m, p)| (Some(m), Some(p)));
    let mut metadata_rows = vec![
        metadata(
            FIRST,
            "3195576",
            ["20", "3"],
            base,
            base + 1000,
            first_market_end_utc_ms.unwrap_or(base + 10000),
            1,
        ),
        metadata(
            SECOND,
            "3195577",
            ["21", "4"],
            base + 6000,
            base + 10000,
            base + 20000,
            if terminal_metadata.is_some() { 3 } else { 2 },
        ),
    ];
    if let Some(terminal) = terminal_metadata {
        metadata_rows.insert(1, local_transition(base, false));
        metadata_rows.push(local_transition(base, true));
        metadata_rows.push(terminal);
    }
    metadata_writer
        .write_polymarket_market_metadata_v5_rows(&metadata_rows)
        .unwrap();
    metadata_writer.append_key_value_metadata("data_collect_daily_file_fact_v1", json!({"source_kind":"polymarket_market_metadata","schema_version":"polymarket_market_metadata_v5","utc_hour":format!("{utc_day}T00"),"part":0,"rows":metadata_rows.len()}).to_string()).unwrap();
    let metadata = metadata_writer.finalize().unwrap();
    let mut pm_writer = TypedParquetSegmentWriter::create_for_typed_source(
        &day,
        "books",
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    let mut pm_rows = vec![
        book(FIRST, "20", base + 1100, base + 1100, 1, "0.6"),
        book(FIRST, "3", base + 1100, base + 1100, 2, "0.6"),
        // 隐藏撮合先在1700发生；公开直到2200才接收，不能提前更新策略盘口。
        book(FIRST, "3", base + 1300, base + 1300, 3, "0.65"),
        book(FIRST, "20", base + 1701, base + 2200, 4, "0.5"),
        book(SECOND, "21", base + 10100, base + 10200, 5, "0.6"),
        book(SECOND, "4", base + 10100, base + 10200, 6, "0.6"),
    ];
    if maker_trade {
        use data_collect::polymarket_orderbook_v3::{
            PolymarketLastTradePriceV3, PolymarketPriceChangeEntryV3, PolymarketPriceChangeV3,
        };
        let mut removal = book(FIRST, "20", base + 2500, base + 2500, 7, "0.5");
        removal.event = PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
            price_changes: vec![PolymarketPriceChangeEntryV3 {
                asset_id: "20".into(),
                price: "0.4".into(),
                size: Some("0".into()),
                side: "BUY".into(),
                best_bid: None,
                best_ask: None,
            }],
        });
        let mut trade = book(FIRST, "20", base + 4000, base + 4000, 8, "0.5");
        trade.event = PolymarketOrderbookV3Event::LastTradePrice(PolymarketLastTradePriceV3 {
            asset_id: "20".into(),
            price: "0.4".into(),
            size: Some("105".into()),
            side: Some("SELL".into()),
            fee_rate_bps: None,
        });
        pm_rows.extend([removal, trade]);
        pm_rows.sort_by_key(|r| {
            (
                r.envelope.timestamp_received,
                r.envelope.collector_admission_sequence,
            )
        });
        for (i, r) in pm_rows.iter_mut().enumerate() {
            r.envelope.collector_admission_sequence = i as u64 + 1;
        }
    }
    pm_rows.extend(terminal_pm);
    pm_writer
        .write_polymarket_orderbook_v3_messages(
            &pm_rows,
            &mut PolymarketOrderbookV3WriteSequenceState::default(),
        )
        .unwrap();
    pm_writer.append_key_value_metadata("data_collect_daily_file_fact_v1", json!({"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":format!("{utc_day}T00"),"part":0,"rows":pm_rows.len()}).to_string()).unwrap();
    let pm = pm_writer.finalize().unwrap();
    let files=[(&metadata,"polymarket_market_metadata","polymarket_market_metadata_v5"),(&pm,"polymarket_market",POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION)].map(|(file,source,version)|json!({"path":file.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":source,"schema_version":version,"utc_hour":format!("{utc_day}T00"),"part":0,"rows":file.row_count,"bytes":std::fs::metadata(&file.final_path).unwrap().len(),"sha256":file.sha256}));
    let manifest = json!({"schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":utc_day,"closure_reason":"utc_day_barrier","collection_day_coverage":{"schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known","first_started_utc_ms":base,"last_started_utc_ms":base,"ended_utc_ms":base+86400000,"reactivated_within_day":false},"producer_barriers":["binance_aggtrade","binance_depth","polymarket","polymarket_rtds"],"writer_finalize_acks":["binance_aggtrade","binance_depth_snapshot","binance_depth_update","polymarket_market","polymarket_market_metadata","polymarket_rtds_chainlink_btc_usd"],"files":files});
    let path = day.join("manifest.json");
    std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    path
}
pub(super) fn setup_day_with_terminal(
    root: &Path,
    base: i64,
    utc_day: &str,
    terminal: Option<(PolymarketMarketMetadataV5Row, PolymarketOrderbookV3Message)>,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    setup_day_material(root, base, utc_day, terminal, false, false, None)
}
#[allow(dead_code)] // 部分集成夹具只消费原场景；公共环境示例消费此场景。
pub(crate) fn setup_day_with_embedded_rules(
    root: &Path,
    base: i64,
    utc_day: &str,
    maker_trade: bool,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    setup_day_material(root, base, utc_day, None, maker_trade, true, None)
}
// 返佣边界夹具显式选市场结束时刻；普通调用保留原市场/输入身份。
#[allow(dead_code)]
pub(super) fn setup_day_with_rebate_end(
    root: &Path,
    base: i64,
    utc_day: &str,
    maker_trade: bool,
    first_market_end_utc_ms: Option<i64>,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    setup_day_material(
        root,
        base,
        utc_day,
        None,
        maker_trade,
        true,
        first_market_end_utc_ms,
    )
}
fn setup_day_material(
    root: &Path,
    base: i64,
    utc_day: &str,
    terminal: Option<(PolymarketMarketMetadataV5Row, PolymarketOrderbookV3Message)>,
    maker_trade: bool,
    embedded_rules: bool,
    first_market_end_utc_ms: Option<i64>,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    let manifest = material(
        root,
        base,
        utc_day,
        terminal,
        maker_trade,
        first_market_end_utc_ms,
    );
    let rule_path = if embedded_rules {
        let path = root.join("source-rules.json");
        std::fs::write(
            &path,
            include_bytes!(
                "../../../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json"
            ),
        )
        .unwrap();
        path
    } else {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|path| path.join("sim-system").is_dir())
            .unwrap();
        workspace.join("sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json")
    };
    let rule_sha = format!("{:x}", Sha256::digest(std::fs::read(&rule_path).unwrap()));
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: rule_path,
        expected_file_sha256: Sha256Hex(rule_sha),
        replay_causal_start_utc_ms: base,
    })
    .unwrap();
    let finality_path = root.join("finality.json");
    std::fs::write(&finality_path, br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":1},"failure_overrides":[]}"#).unwrap();
    let sort = SortConfig {
        max_run_rows: 100,
        max_run_bytes: 1 << 20,
        max_temp_run_files: 20,
        merge_fan_in: 4,
        merge_workers: 1,
        max_open_files: 8,
        batch_rows: 10,
        max_temp_bytes: 1 << 26,
    };
    let config = HistoricalRunConfigV1 {
        public_statistics: None,
        rebate_entitlement_queries: Vec::new(),
        entry_market_scope: None,
        research_evidence_path: None,
        gamma_resolution_policy: Default::default(),
        preparation: HistoricalPreparationConfigV1 {
            scope: HistoricalScopeV1 {
                manifest_paths: vec![manifest],
                received_start_utc_ms: base,
                received_end_utc_ms: base + 12000,
                purpose: "合成工程验证；非研究结论".into(),
            },
            metadata_initialization: None,
            work_root: root.join("prepared"),
            header: PublicInputStreamHeaderV1 {
                input_stream_id: "synthetic-history".into(),
                canonical_rule_version: "canonical-reduction-v1".into(),
                observation_version: "market_business_observation_v1".into(),
                ordering_version: "historical-source-head-v1".into(),
                input_scope: "explicit-manifests/synthetic".into(),
                max_pending_ws_terminals: 10,
                initial_state: PublicInitialStateV1::Empty,
            },
            canonical: CanonicalRunConfig {
                decode_batch_rows: 10,
                max_decode_batch_bytes: 1 << 20,
                max_temporary_row_bytes: 1 << 20,
                max_epoch_count_per_source: 10,
                max_pending_ws_terminals: 10,
                max_retained_reducer_entries: 100,
                canonical_rule_version: "canonical-reduction-v1".into(),
                sort: sort.clone(),
            },
            recording: GateAOutputConfig {
                max_artifact_payload_bytes: 1 << 20,
                max_capture_output_bytes: 1 << 25,
                max_candidate_directory_bytes: 1 << 26,
                max_observation_l2_levels: 100,
                sort,
            },
            read_page_rows: 10,
            max_read_batch_bytes: 1 << 20,
            hidden_stream_id: "synthetic-hidden".into(),
        },
        timing: HistoricalTimingV1 {
            decision_mode: backtest::HistoricalDecisionModeV1::EventDriven,
            public_delivery_delay_ms: DelayScheduleV1::Fixed(100),
            calculation_start_delay_ms: DelayScheduleV1::Fixed(0),
            calculation_duration_ms: DelayScheduleV1::Fixed(200),
            dispatch_delay_ms: DelayScheduleV1::Fixed(200),
        },
        session: HistoricalSessionV1 {
            caller_role: contracts::SimCallerRoleV1::HistoricalBacktest,
            input_lifecycle_policy:
                sim_system::public_consumption::InputLifecyclePolicyV1::RoleDefault,
            run_id: "synthetic-history".into(),
            session_id: "one-session".into(),
            account_id: "one-account".into(),
            caller_instance_id: "synthetic-host".into(),
            creation_request_id: "opening".into(),
            response_bridge_binding_id: "bridge".into(),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("2000".into()),
            },
            session_seed: Sha256Hex("e".repeat(64)),
        },
        strategy: StrategyDescriptorV1 {
            strategy_id: "engineering-observer".into(),
            strategy_rule_version: "test_v1".into(),
            effective_parameters: json!({"description":"全部值仅用于机制验证"}),
        },
        implementation_identity: ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        },
        finality_policy_path: finality_path,
        sim_log: root.join("sim.jsonl"),
        noe_root: root.join("noe"),
        ledger_root: root.join("ledger"),
        metadata_path: root.join("run.json"),
        result_path: root.join("result.json"),
        activation_id: "activate".into(),
        consumption_sequence_ref: "historical-host".into(),
        noe_implementation_identity: "public-v4".into(),
        admission_policy: AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        max_group_bytes: 1 << 24,
        finite_in_flight_groups: 128,
    };
    (config, rules)
}
