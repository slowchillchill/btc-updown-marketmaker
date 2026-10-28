//! #378 固定两个规模的正式统计成本入口；默认回归不执行。
use super::*;
use crate::public_input::*;
use crate::public_observation_core::{public_group_visible_at_utc_ms, tests as fixture};
use crate::public_reference::historical_response_source_evidence_v3;
use crate::typed_raw::{BinanceDepthStitchVerdict, DepthLevel, TypedRawRow};
use std::{cell::Cell, collections::BTreeMap, path::PathBuf, time::Instant};

thread_local! { static WORK: Cell<(u64, u64)> = const { Cell::new((0, 0)) }; }
pub(super) fn visit() {
    WORK.with(|w| {
        let (v, e) = w.get();
        w.set((v + 1, e));
    });
}
pub(super) fn evaluate() {
    WORK.with(|w| {
        let (v, e) = w.get();
        w.set((v, e + 1));
    });
}

#[test]
#[ignore = "#378 U2成本入口；仅按已绑定8市场/512consume定向执行"]
fn blb_maintenance_low_history_cost() {
    run(8);
}
#[test]
#[ignore = "#378 U3成本入口；仅按已绑定128市场/512consume定向执行"]
fn blb_maintenance_high_history_cost() {
    run(128);
}

fn run(seen: usize) {
    let whole_started = Instant::now();
    let root = PathBuf::from(
        std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("成本原件使用显式独立输出根"),
    );
    std::fs::create_dir_all(&root).unwrap();
    let preparation_started = Instant::now();
    let mut core = fixture::core();
    let template = super::reuse_tests::stream();
    let mut identity = template.identity().clone();
    identity.stream_header = core.header().clone();
    let mut config = template.config.as_ref().clone();
    config.blb_lookback_windows_ms = vec![200];
    let stream = PublicStatisticsStreamV1::new(identity, config).unwrap();
    drop(template);
    let mut metadata_groups = 0;
    for i in 0..seen {
        let mut row = fixture::metadata_snapshot_row();
        row.condition_id = format!("0x{:064x}", i + 1);
        row.clob_market_id = (3_195_576 + i).to_string();
        row.asset_ids = format!("[\"{}\",\"{}\"]", 100 + i * 2, 101 + i * 2);
        row.timestamp_received = 1_000;
        row.collector_admission_sequence = i as u64 + 1;
        row.collector_admission_time_utc_ms = 1_001;
        row.fixed_identity_verified_at_utc = 999;
        row.source_evidence_received_at_utc = Some(1_000);
        row.row_evidence_validation_completed_at_utc = Some(1_001);
        row.metadata_fetch_start_utc = Some(990);
        row.market_start_utc = if i < 2 { 20_000 } else { 2_000 };
        row.market_end_utc = if i < 2 { 30_000 } else { 10_000 };
        let input = fixture::record(
            i as u64,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row))),
        );
        let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap();
        metadata_groups += core.push(&prepared).unwrap().applied.closed_groups.len();
    }
    // 复用正式历史HTTP响应→准备→core路径；一个repair输入含seen个合法市场成员。
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".into(),
        request_url: "https://gamma-api.polymarket.com/events".into(),
        received_at_utc: "1970-01-01T00:00:20.500Z".into(),
        received_at_utc_ms: 20_500,
        receipt_observation: "curl_completed".into(),
        response_body_sha256: "a".repeat(64),
        response_body_path: "synthetic-response.json".into(),
        original_response_file: "/synthetic/response.json".into(),
        http_status_code: None,
        response_url: None,
    };
    let source = historical_response_source_evidence_v3(&response).unwrap();
    let mut identities = BTreeMap::new();
    let mut rows = Vec::new();
    for i in 0..seen {
        let id = format!("0x{:064x}", i + 1);
        let context = core.pm_context(&id).unwrap();
        let key = format!("identity-{i}");
        identities.insert(key.clone(), context.identity_source_evidence[0].clone());
        rows.push(HistoricalPriceAnchorRepairRowV1 {
            schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.into(),
            condition_id: id,
            clob_market_id: context.market.clob_market_id.0,
            slug: "btc-updown-5m".into(),
            asset_ids_json: format!("[\"{}\",\"{}\"]", 100 + i * 2, 101 + i * 2),
            market_start_at_utc_ms: context.market.window.start_at_utc_ms,
            market_end_at_utc_ms: context.market.window.end_at_utc_ms,
            anchor_source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
            price_to_beat_raw: "65000".into(),
            response_group_id: source.source_message_id.0.clone(),
            response_member_index: i as u32,
            response_received_at_utc_ms: response.received_at_utc_ms,
            response_sha256: response.response_body_sha256.clone(),
            source_value_path: format!("/{i}/eventMetadata/priceToBeat"),
            source_event_at_utc_ms: None,
            historical_identity_ready_at_utc_ms: 1_001,
            historical_identity_evidence_ids_json: format!("[\"{key}\"]"),
            repair_policy_version: "price_anchor_historical_repair_v1".into(),
        });
    }
    let repair_batch = HistoricalPriceAnchorRepairBatchInput {
        response_group_id: source.source_message_id.0.clone(),
        response,
        rows,
        identity_evidence: BTreeMap::new(),
    };
    let mut repair = fixture::record(
        seen as u64,
        TypedRawRowV2::HistoricalPriceAnchorRepairBatch(repair_batch),
    );
    repair.dependency_record_refs = (0..seen)
        .map(|i| CanonicalInputRecordRefV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position: i as u64,
        })
        .collect();
    let prepared = prepare_public_ingress_v1(&core, &repair, &identities, &[]).unwrap();
    let repair_applied = core.push(&prepared).unwrap().applied;
    let repair_groups = repair_applied.closed_groups.len();
    assert_eq!(
        repair_applied
            .closed_groups
            .iter()
            .map(|g| g.records.len())
            .sum::<usize>(),
        seen
    );
    drop(repair_applied);
    drop(prepared);
    std::fs::write(
        root.join("repair-input.json"),
        serde_json::to_vec_pretty(&repair).unwrap(),
    )
    .unwrap();
    drop((repair, identities, source));
    let mut inputs = Vec::new();
    for i in 0..512 {
        let received = 21_000 + i as i64;
        let quantity = if i % 2 == 0 { "1" } else { "2" };
        let typed = if i == 0 {
            let mut row = fixture::snapshot("blb-cost-depth", 100, 1);
            row.timestamp_received = received;
            row.snapshot_fetch_start_utc = received - 10;
            row.collector_admission_time_utc_ms = received + 1;
            row.bids = vec![DepthLevel {
                price: "65001".into(),
                quantity: quantity.into(),
            }];
            row.asks = vec![DepthLevel {
                price: "65003".into(),
                quantity: "1".into(),
            }];
            TypedRawRow::BinanceDepthSnapshot(row)
        } else {
            let mut row = fixture::update(
                "blb-cost-depth",
                100 + i,
                100 + i,
                i + 1,
                Some(100),
                if i == 1 { None } else { Some(99 + i) },
                BinanceDepthStitchVerdict::Applied,
                vec![DepthLevel {
                    price: "65001".into(),
                    quantity: quantity.into(),
                }],
            );
            row.timestamp_received = received;
            row.event_time = received - 1;
            row.collector_admission_time_utc_ms = received + 1;
            TypedRawRow::BinanceDepthUpdate(row)
        };
        inputs.push(fixture::record(
            seen as u64 + 1 + i,
            TypedRawRowV2::Existing(typed),
        ));
    }
    let preparation_seconds = preparation_started.elapsed().as_secs_f64();
    WORK.with(|w| w.set((0, 0)));
    let mut consume_ns = 0u128;
    let mut core_application_ns = 0u128;
    let mut first = None;
    let mut last = None;
    for (i, input) in inputs.iter().enumerate() {
        let started = Instant::now();
        let prepared = prepare_public_ingress_v1(&core, input, &BTreeMap::new(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().applied;
        core_application_ns += started.elapsed().as_nanos();
        assert_eq!(applied.closed_groups.len(), 1);
        let group = &applied.closed_groups[0];
        assert_eq!(group.member_input_positions, vec![input.input_position]);
        let at = public_group_visible_at_utc_ms(&input.ingress, &group.records);
        assert_eq!(at, 21_001 + i as i64);
        let reference = PublicGroupReferenceV1 {
            input_stream_id: input.input_stream_id.clone(),
            applied_prefix: applied.applied_prefix.clone(),
            group_position: group.group_position,
            group_id: group.group_id.clone(),
            closed_at_input_position: group.closed_at_input_position,
        };
        let owner = PreparedPublicStatisticsV1::default();
        let started = Instant::now();
        let history = owner
            .consume(&stream, reference.clone(), at, group)
            .unwrap()
            .unwrap();
        consume_ns += started.elapsed().as_nanos();
        assert_eq!(group.observations.len(), seen);
        if i == 0 || i == 199 || i == 200 || i == 511 {
            let selected = group
                .observations
                .iter()
                .filter(|o| o.market.window.end_at_utc_ms >= at)
                .collect::<Vec<_>>();
            assert_eq!(selected.len(), 2);
            for observation in selected {
                assert_eq!(observation.assets.len(), 2);
                let statistics = history.read(&observation, at);
                assert_eq!(statistics.blb.status, "visible_interval_only");
                assert_eq!(
                    statistics.blb.official_k_usd_per_btc,
                    Some(Decimal::from(65000))
                );
                assert_eq!(
                    statistics.blb.visible_quantity_btc,
                    Some(Decimal::from(1 + i % 2))
                );
                let lookback = &statistics.blb_lookbacks[0];
                assert_eq!(lookback.window_ms, 200);
                assert!(lookback.right.is_some());
                assert_eq!(lookback.left.is_some(), i >= 200);
                if i == 0 && first.is_none() {
                    first = Some((history.clone(), observation.clone(), statistics.clone(), at));
                }
            }
        }
        last = Some(history);
    }
    let work = WORK.with(Cell::get);
    let (first_history, first_observation, first_read, first_at) = first.unwrap();
    assert_eq!(first_history.read(&first_observation, first_at), first_read);
    let retained = last
        .as_ref()
        .unwrap()
        .blb_endpoints
        .iter()
        .map(|(id, points)| (id.clone(), points.len()))
        .collect::<Vec<_>>();
    assert_eq!(retained.len(), 2);
    let first_group = first_history.group.clone();
    let last_group = last.as_ref().unwrap().group.clone();
    let cleanup_started = Instant::now();
    drop((
        first_history,
        first_observation,
        first_read,
        last,
        inputs,
        core,
        stream,
    ));
    let cleanup_seconds = cleanup_started.elapsed().as_secs_f64();
    let whole_seconds = whole_started.elapsed().as_secs_f64();
    std::fs::write(root.join("blb-cost.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "seen_markets":seen,"current_blb_markets":2,"assets_per_market":2,"lookback_ms":200,
        "metadata_inputs":seen,"metadata_groups":metadata_groups,"repair_inputs":1,"repair_members":seen,"repair_groups":repair_groups,
        "depth_snapshot_inputs":1,"depth_update_inputs":511,"statistics_consumes":512,"total_inputs":seen+1+512,
        "first_group":first_group,"last_group":last_group,"retained_endpoints":retained,
        "endpoint_visits":work.0,"endpoint_evaluations":work.1,
        "preparation_seconds":preparation_seconds,"core_application_ns":core_application_ns,
        "statistics_consume_ns":consume_ns,"remaining_owner_drop_seconds":cleanup_seconds,"whole_through_drop_seconds":whole_seconds,
        "limits":"全部构造/操作/drop由外层time-v计wall/CPU/RSS；内部whole截至业务owner释放，报告/原件输出及临时值释放只在外层。内部计时不重加。两个线程局部累计/每consume时钟扰动包含且Unknown；无额外预热/规模/账户。512消费不含额外prepare消费。"
    })).unwrap()).unwrap();
}
