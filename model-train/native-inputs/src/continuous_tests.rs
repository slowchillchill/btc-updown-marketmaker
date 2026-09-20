//! 阶段6有限连续验收。公共消息经原DPT录制及消费，不构造ModelFrame。
use crate::public_input_tests::common::*;
use crate::*;
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use ledger::*;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::{collections::BTreeMap, path::Path};

fn next_recorded_group(
    driver: &mut FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>,
) {
    loop {
        match driver.next_group() {
            Err(SimRunActorErrorV2::RecordingBackpressured) => {
                driver.wait_for_recording_capacity().unwrap()
            }
            other => {
                other.unwrap().unwrap();
                return;
            }
        }
    }
}

pub(super) const START: i64 = 85_800_000; // D 23:50
pub(super) const FIRST: i64 = 86_100_000; // D 23:55
pub(super) const END: i64 = 90_000_000; // D+1 01:00

pub(super) fn condition(index: usize) -> String {
    format!("0x{:064x}", index + 1)
}
pub(super) fn tokens(index: usize) -> [String; 2] {
    [(20 + index * 2).to_string(), (21 + index * 2).to_string()]
}
fn metadata(index: usize, at: i64, active: bool, seq: u64) -> TypedRawRowV2 {
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)),
        ..
    } = meta(0).ingress
    else {
        unreachable!()
    };
    let start = FIRST + index as i64 * 300_000;
    m.condition_id = condition(index);
    m.clob_market_id = (3195576 + index).to_string();
    m.asset_ids = serde_json::to_string(&tokens(index)).unwrap();
    m.market_start_utc = start;
    m.market_end_utc = start + 300_000;
    m.timestamp_received = at - 2;
    m.collector_admission_sequence = seq;
    m.collector_admission_time_utc_ms = at - 1;
    m.fixed_identity_verified_at_utc = (start - 30_010).min(at - 10);
    m.metadata_fetch_start_utc = Some(at - 10);
    m.source_evidence_received_at_utc = Some(at - 2);
    m.row_evidence_validation_completed_at_utc = Some(at - 1);
    if active {
        m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
        m.market_runtime_role = MetadataMarketRuntimeRole::Current;
    }
    TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m))
}
fn book_row(index: usize, side: usize, at: i64, seq: u64) -> TypedRawRowV2 {
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut row)),
        ..
    } = pm(
        0,
        seq,
        at - 3,
        PolymarketOrderbookEvent::Book {
            asset_id: tokens(index)[side].clone(),
            bids: vec![PolymarketPriceLevel {
                price: "0.01".into(),
                size: "100".into(),
            }],
            asks: vec![PolymarketPriceLevel {
                price: if index == 1 { "0.99" } else { "0.6" }.into(),
                size: "100".into(),
            }],
            last_trade_price: Some("0.5".into()),
        },
    )
    .ingress
    else {
        unreachable!()
    };
    row.market = condition(index);
    row.source_sequence_epoch_id = format!("continuous-{index}");
    row.timestamp_received = at - 2;
    row.collector_admission_time_utc_ms = at - 1;
    TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row))
}
fn terminal_rows(index: usize, at: i64) -> [TypedRawRowV2; 2] {
    let start = FIRST + index as i64 * 300_000;
    let market = contracts::MarketIdentityV2 {
        condition_id: contracts::ConditionId(condition(index)),
        clob_market_id: contracts::ClobMarketId((3195576 + index).to_string()),
        window: contracts::MarketWindowV2 {
            start_at_utc_ms: start,
            end_at_utc_ms: start + 300_000,
        },
    };
    let assets = tokens(index);
    let mut m = terminal_metadata_row(&market);
    m.asset_ids = serde_json::to_string(&assets).unwrap();
    m.fixed_identity_verified_at_utc = (start - 30_010).min(at - 10);
    m.timestamp_received = at - 2;
    m.collector_admission_sequence = (at - START) as u64 + 1;
    m.collector_admission_time_utc_ms = at;
    m.source_evidence_received_at_utc = Some(at - 2);
    m.row_evidence_validation_completed_at_utc = Some(at - 1);
    m.lifecycle_applied_at_utc = Some(at);
    m.runtime_role_applied_at_utc = Some(at);
    m.ws_platform_timestamp_raw = Some((at - 3).to_string());
    m.ws_platform_timestamp_utc = Some(at - 3);
    m.winning_asset_id = Some(assets[0].clone());
    m.ws_claimant_source_sequence_epoch_id = Some(format!("continuous-{index}"));
    m.ws_claimant_collector_admission_sequence = Some(1_000_000_000);
    let mut ws = terminal_orderbook_row(&market);
    ws.source_sequence_epoch_id = format!("continuous-{index}");
    ws.collector_admission_sequence = 1_000_000_000;
    ws.timestamp = Some(at - 3);
    ws.timestamp_received = at - 2;
    ws.collector_admission_time_utc_ms = at - 1;
    if let PolymarketOrderbookEvent::MarketResolved {
        asset_ids,
        winning_asset_id,
        ..
    } = &mut ws.event
    {
        *asset_ids = assets.to_vec();
        *winning_asset_id = assets[0].clone();
    }
    [
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(m))),
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
    ]
}
fn anchor_row(index: usize, at: i64, formal: bool) -> TypedRawRowV2 {
    let start = FIRST + index as i64 * 300_000;
    let row = serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":at-2,
        "collector_admission_sequence":at as u64, "collector_admission_time_utc_ms":at-1,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"host", "collector_region":"hk",
        "slug":"btc-updown-5m", "condition_id":condition(index), "clob_market_id":(3195576+index).to_string(),
        "asset_ids":serde_json::to_string(&tokens(index)).unwrap(),
        "market_start_utc":start,"market_end_utc":start+300000,"fixed_identity_verified_at_utc":start-30010,
        "market_lifecycle_state":if at < start {"prefetched"} else {"active"},
        "market_runtime_role":if at < start {"next"} else {"current"},"metadata_fetch_start_utc":at-10,
        "anchor_query_source_epoch_id":format!("anchor-{index}"),"anchor_query_ordinal":if formal {2} else {1},"anchor_query_result":"observed",
        "price_to_beat_decimal":formal.then_some("64950.014218511440012345"),"price_to_beat_source_field":formal.then_some("eventMetadata.priceToBeat"),
        "twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":at-2,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m", "http_body_sha256":"123"
    })).unwrap();
    TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
        PolymarketMarketMetadataV6Input::AnchorRule(row),
    ))
}
fn recorded_continuous(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_scenario(root, None, 0)
}
fn recorded_scenario(
    root: &Path,
    scenario: Option<Scenario>,
    day_offset: i64,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_scenario_depth(root, scenario, day_offset, 100)
}
fn recorded_scenario_depth(
    root: &Path,
    scenario: Option<Scenario>,
    day_offset: i64,
    depth: i64,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut cfg = config();
    cfg.sort.batch_rows = 4096;
    cfg.sort.max_run_rows = 8192;
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), cfg).unwrap();
    let mut position = 0;
    let mut times = Vec::new();
    let mut candidate: Option<data_pipeline_core::public_reference::TemporaryReferenceCandidateV1> =
        None;
    let mut push = |row, at: i64, core: &mut PublicObservationCoreV1| {
        let mut row = shift_calendar(row, day_offset);
        if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(book)) = &mut row
            && let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut book.event
        {
            for level in bids.iter_mut().chain(asks) {
                level.size = depth.to_string();
            }
        }
        let at = at + day_offset;
        let mut input = record(position, row);
        let index = (at - day_offset - FIRST).div_euclid(300000);
        let market = if (0..13).contains(&index) {
            core.pm_context(&condition(index as usize))
                .map(|c| c.market)
        } else {
            None
        };
        let ids = BTreeMap::new();
        let prepared = if let Some(market) = market {
            prepare_public_ingress_with_reference_v1(
                core,
                &mut input,
                &ids,
                &[],
                &market,
                |cursor| {
                    let values = candidate
                        .iter()
                        .filter(|c| c.record_ref.input_position >= cursor.next_input_position)
                        .cloned()
                        .collect::<Vec<_>>();
                    let next = values.last().map_or(cursor.next_input_position, |c| {
                        c.record_ref.input_position + 1
                    });
                    Ok((
                        values,
                        data_pipeline_core::public_reference::TemporaryReferenceCursorV1 {
                            next_input_position: next,
                        },
                        true,
                    ))
                },
            )
            .unwrap()
        } else {
            prepare_public_ingress_v1(core, &input, &ids, &[]).unwrap()
        };
        let applied = core.push(&prepared).unwrap().into_owned();
        if let PreparedCanonicalInputV1::ChainlinkTwap(event) = &prepared.current
            && let Some(fact) = applied
                .applied
                .closed_groups
                .iter()
                .flat_map(|g| g.records.iter())
                .find(|f| !f.direct_sources.is_empty())
        {
            candidate = Some(
                data_pipeline_core::public_reference::TemporaryReferenceCandidateV1 {
                    canonical_fact: event.clone(),
                    record_ref: contracts::CanonicalInputRecordRefV1 {
                        input_stream_id: prepared.recorded.input_stream_id.clone(),
                        input_position: position,
                    },
                    anchor: contracts::DirectObservationAnchorV1 {
                        field_path: "chainlink_twap_thirty".into(),
                        position: fact.position.clone(),
                        group_id: prepared.prepared_group_id.clone().unwrap(),
                        source_message_id: fact.direct_sources[0].source_message_id.clone(),
                        source_event_at_utc_ms: fact.direct_sources[0].source_event_at_utc_ms,
                        received_at_utc_ms: fact.direct_sources[0].received_at_utc_ms,
                    },
                },
            );
        }
        times.extend(applied.applied.closed_groups.iter().map(|_| at));
        writer.record(input, applied).unwrap();
        position += 1;
    };
    for at in (START..=END).step_by(1000) {
        for index in 0..13 {
            let start = FIRST + index as i64 * 300_000;
            let prefetched = start - 30_000;
            if at == prefetched || at == start {
                push(
                    metadata(index, at, at == start, (at - START) as u64 + 1),
                    at,
                    &mut core,
                );
                if at < start {
                    for side in 0..2 {
                        push(
                            book_row(index, side, at, (index * 2 + side + 1) as u64),
                            at,
                            &mut core,
                        );
                    }
                }
            }
            if at == prefetched + 1000 {
                let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut row)) =
                    book_row(index, 1, at, at as u64)
                else {
                    unreachable!()
                };
                row.event = PolymarketOrderbookEvent::TickSizeChange {
                    asset_id: tokens(index)[1].clone(),
                    old_tick_size: Some("0.01".into()),
                    new_tick_size: "0.001".into(),
                };
                push(
                    TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
                    at,
                    &mut core,
                );
            }
            if at == start + 5000 {
                let mut late = reference_after(start + 998, (at - START) as u64 + 1000000);
                late.common.timestamp_received = at - 2;
                late.collector_admission_time_utc_ms = at - 1;
                late.full_accuracy_value = "70000000000000000000000".into();
                late.common.value = "70000".into();
                let row = TypedRawRowV2::PolymarketRtdsChainlinkTwap(late);
                push(row.clone(), at, &mut core);
                push(row, at, &mut core);
            }
            if at == start + 301_000 {
                for row in terminal_rows(index, at) {
                    push(row, at, &mut core);
                }
            }
            if at == start - 20_000 || at == start + 10_000 {
                push(anchor_row(index, at, at > start), at, &mut core);
            }
        }
        if let Some(scenario) = scenario {
            for row in economic_inputs(scenario, at) {
                push(row, at, &mut core);
            }
        }
        push(
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(
                at - 2,
                (at - START) as u64 + 1,
            )),
            at,
            &mut core,
        );
    }
    let input = RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap();
    let applied = core.push(&prepared).unwrap().into_owned();
    assert!(applied.applied.closed_groups.is_empty());
    writer.record(input, applied).unwrap();
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}

#[test]
#[ignore = "完整70分钟真实公开输入结构探针；单独显式运行"]
fn continuous_public_seventy_minutes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dpt");
    let started = std::time::Instant::now();
    let (core, times) = recorded_continuous(&path);
    eprintln!(
        "continuous fixture_seconds={}",
        started.elapsed().as_secs_f64()
    );
    let (mut execution, observation, rules) = execution_with_rules_v4(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "2000",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let mut native = NativeInputRoot::new("C-structure".into(), account, START, rules);
    let source = RecordedPublicGroupSourceV1::open(&path, 128, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "continuous".into());
    let mut frames = 0;
    let mut decisions = 0;
    let mut initializations = 0;
    let mut retirements = 0;
    let mut last = START;
    while driver.next_group().unwrap().is_some() {
        last = driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        if let Some(group) = native.consume(&mut shared, &mut driver).unwrap() {
            frames += group.rows.len();
            decisions += group
                .rows
                .iter()
                .filter(|r| r.role == InputRole::DecisionRequired)
                .count();
            initializations += group
                .pre_frame_hidden_ops
                .iter()
                .filter(|op| matches!(op, HiddenOp::InitializeZero { .. }))
                .count();
            retirements += group.post_frame_hidden_ops.len();
            if frames > 0 && frames % 128 == 0 {
                eprintln!("continuous at={last} frames={frames}");
            }
        }
        if last == END {
            break;
        }
    }
    driver.stop_and_drain().unwrap();
    assert_eq!(last, END);
    assert_eq!(initializations, 13);
    assert_eq!(retirements, 13);
    assert!(frames > 4200 && decisions > 3900);
    eprintln!(
        "continuous frames={frames} decisions={decisions} retirements={retirements} elapsed={}",
        started.elapsed().as_secs_f64()
    );
}

#[cfg(all(feature = "python", feature = "ort"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "三端真实128帧预热成本诊断；不是70分钟验收"]
async fn continuous_three_runtime_warmup_cost() {
    use std::sync::Arc;
    let directory = std::env::var("ACTOR_GRAPH_DIRECTORY").unwrap();
    let threads = std::env::var("ACTOR_THREADS")
        .unwrap_or("1".into())
        .parse()
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dpt");
    let (core, times) = recorded_continuous(&path);
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, threads);
    let mut roots = Vec::new();
    for index in 0..2 {
        let root = temp.path().join(format!("root-{index}"));
        std::fs::create_dir(&root).unwrap();
        let (mut execution, observation, rules) = execution_with_identity_v4(
            &root.join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "2000",
            &format!("warmup-{index}"),
        );
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
            .unwrap();
        let shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let account = shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key;
        let native = NativeInputRoot::new(format!("warmup-{index}"), account, START, rules);
        let source = RecordedPublicGroupSourceV1::open(&path, 128, 1 << 20, times.clone()).unwrap();
        roots.push((
            shared,
            native,
            FinitePublicConsumptionV1::new(execution, source, format!("warmup-{index}")),
        ));
    }
    assert_ne!(
        roots[0]
            .0
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key,
        roots[1]
            .0
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key
    );
    let started = std::time::Instant::now();
    while runtime.frames < 256 {
        let mut groups = Vec::new();
        let mut indices = Vec::new();
        for (index, (shared, native, driver)) in roots.iter_mut().enumerate() {
            assert!(driver.next_group().unwrap().is_some());
            if let Some(group) = native.consume(shared, driver).unwrap() {
                indices.push(index);
                groups.push(Arc::new(group));
            }
        }
        let actors = runtime.advance(&groups, true);
        for (index, actor) in actors {
            let (shared, _, driver) = &mut roots[indices[index]];
            driver.wait_for_recording_capacity().unwrap();
            actor
                .execute(
                    shared,
                    &mut driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("warm-{}-{}", indices[index], runtime.frames),
                        caller_correlation_id: None,
                    },
                )
                .await
                .unwrap();
        }
    }
    for (_, _, driver) in roots {
        driver.stop_and_drain().unwrap();
    }
    eprintln!(
        "three_runtime warmup_frames={} decisions={} seconds={}",
        runtime.frames,
        runtime.decisions,
        started.elapsed().as_secs_f64()
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Trade,
    Orphan,
    Rebate,
}
fn shift_calendar(row: TypedRawRowV2, offset: i64) -> TypedRawRowV2 {
    fn shift(value: &mut serde_json::Value, offset: i64) {
        match value {
            serde_json::Value::Object(values) => {
                for (name, value) in values {
                    if (name.contains("utc")
                        || [
                            "timestamp",
                            "timestamp_received",
                            "message_timestamp",
                            "payload_timestamp",
                        ]
                        .contains(&name.as_str()))
                        && value.is_i64()
                    {
                        *value = serde_json::json!(value.as_i64().unwrap() + offset);
                    } else if name == "ws_platform_timestamp_raw" && value.is_string() {
                        *value = serde_json::json!(
                            (value.as_str().unwrap().parse::<i64>().unwrap() + offset).to_string()
                        );
                    } else {
                        shift(value, offset);
                    }
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    shift(value, offset)
                }
            }
            _ => (),
        }
    }
    let mut value = serde_json::to_value(row).unwrap();
    shift(&mut value, offset);
    serde_json::from_value(value).unwrap()
}
fn price_change(index: usize, side: usize, at: i64, price: &str, size: &str) -> TypedRawRowV2 {
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut row)) =
        book_row(index, side, at, at as u64)
    else {
        unreachable!()
    };
    row.event = PolymarketOrderbookEvent::PriceChange {
        price_changes: vec![PolymarketPriceChangeEntry {
            asset_id: tokens(index)[side].clone(),
            price: price.into(),
            size: Some(size.into()),
            side: PolymarketSide::Buy,
            best_bid: None,
            best_ask: None,
        }],
    };
    TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row))
}
fn economic_inputs(scenario: Scenario, at: i64) -> Vec<TypedRawRowV2> {
    let changes: &[(i64, usize, usize, &str, &str)] = match scenario {
        Scenario::Trade => &[
            (105000, 0, 0, "0.2", "5"),
            (109000, 0, 0, "0.2", "0"),
            (175000, 0, 1, "0.3", "5"),
            (179000, 0, 1, "0.3", "0"),
            (235000, 0, 0, "0.4", "4"),
            (239000, 0, 0, "0.4", "0"),
            (335000, 1, 0, "0.1", "1"),
            (339000, 1, 0, "0.1", "0"),
            (435000, 1, 0, "0.9", "1"),
            (439000, 1, 0, "0.9", "0"),
            (535000, 1, 1, "0.2", "1"),
            (539000, 1, 1, "0.2", "0"),
        ],
        Scenario::Orphan => &[
            (35000, 0, 0, "0.2", "5"),
            (39000, 0, 0, "0.2", "0"),
            (335000, 1, 1, "0.2", "5"),
            (339000, 1, 1, "0.2", "0"),
        ],
        Scenario::Rebate => &[
            (35000, 0, 0, "0.5", "142.862857"),
            (39000, 0, 0, "0.5", "0"),
            (335000, 1, 0, "0.5", "142.868571"),
            (339000, 1, 0, "0.5", "0"),
        ],
    };
    changes
        .iter()
        .filter(|(time, ..)| at == FIRST + time)
        .map(|(_, index, side, price, size)| price_change(*index, *side, at, price, size))
        .collect()
}

// 只枚举原公共读取的现有引用槽；不声称是全堆Arc或后台队列引用计数。
fn source_sample(
    shared: &SharedExecutionV2,
    cutoff: Option<&TrustedGroupDecisionCutoffV1>,
) -> serde_json::Value {
    let mut slots = 0usize;
    let mut positions = std::collections::BTreeSet::new();
    let mut watermarks: BTreeMap<
        String,
        (
            contracts::execution_normalization::SimV2SourceWatermarkV1,
            contracts::execution_normalization::SimV2SourceWatermarkV1,
        ),
    > = BTreeMap::new();
    let registry = shared.registry();
    let mut order_count = 0usize;
    for order in registry.orders() {
        order_count += 1;
        for reference in [
            order.accepted,
            order.formed,
            order.latest_lifecycle,
            order.latest_direct_feedback,
        ]
        .into_iter()
        .flatten()
        .chain(registry.order_trades(order.order_identity))
        {
            slots += 1;
            let position = reference.position();
            let context = position.context_reference.0.clone();
            let watermark = position.source_watermark.clone();
            let bounds = watermarks
                .entry(context)
                .or_insert((watermark.clone(), watermark.clone()));
            if watermark.projection_batch_sequence < bounds.0.projection_batch_sequence {
                bounds.0 = watermark.clone();
            }
            if watermark.projection_batch_sequence > bounds.1.projection_batch_sequence {
                bounds.1 = watermark;
            }
            positions.insert(position);
        }
    }
    let projection = shared.ledger().projection_v2().unwrap();
    serde_json::json!({"cutoff":cutoff,"order_count":order_count,"commitment_count":registry.all_commitments().count(),
        "readable_order_source_reference_slots":slots,"unique_order_source_positions":positions.len(),"source_watermark_ranges":watermarks,
        "reference_scope":"registry order accepted/formed/latest lifecycle/latest direct/trades; not all heap Arc counts",
        "ledger":{"entry_sequence":projection.ledger_entry_seq,"platform_source_prefix":projection.platform_source_prefix,
            "ledger_local_prefix":projection.ledger_local_prefix,"available_through_utc_ms":projection.available_through_utc_ms}})
}

struct ContinuousRoot {
    shared: SharedExecutionV2,
    native: NativeInputRoot,
    driver: FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>,
    coordinator: SettlementCoordinatorV1,
    scenario: Scenario,
    offset: i64,
    last_script_at: Option<i64>,
    checks: std::collections::BTreeSet<&'static str>,
    last_rebate_sample: String,
    model_continuation_started: bool,
}
fn rational(n: &str, d: &str) -> contracts::economics::ExactRationalV1 {
    contracts::economics::ExactRationalV1 {
        numerator: n.into(),
        denominator: d.into(),
    }
}
impl ContinuousRoot {
    fn source_sample(&mut self) -> serde_json::Value {
        let cutoff = self.driver.decision().map(|d| d.cutoff().clone());
        source_sample(&self.shared, cutoff.as_ref())
    }
    fn new(
        root: &Path,
        recording: &Path,
        core: &PublicObservationCoreV1,
        times: TradingTimes,
        scenario: Scenario,
        offset: i64,
        identity: &str,
    ) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let mut policy: serde_json::Value = serde_json::from_slice(SUCCESS_POLICY).unwrap();
        policy["normal_path"]["steps"][1]["offset_ms"] = serde_json::json!(10000);
        policy["normal_path"]["steps"][2]["offset_ms"] = serde_json::json!(20000);
        if scenario == Scenario::Trade {
            let mut failed = policy["normal_path"].clone();
            failed["steps"][2]["state"] = serde_json::json!("FAILED");
            policy["failure_overrides"] = serde_json::json!([{"selector":{"selector_domain":"REQUEST","account_instance_key":contracts::AccountInstanceKeyV1 {
                run_id:identity.into(),caller_role:contracts::SimCallerRoleV1::Paper,caller_instance_id:"shared-caller".into(),account_id:"shared-account".into(),platform_source:contracts::PlatformSourceV1::Simulated},"global_request_id":"script-failed"},"path":failed}]);
        }
        let (mut execution, observation, rules) = execution_with_identity_v4(
            &root.join("sim.jsonl"),
            core,
            &serde_json::to_vec(&policy).unwrap(),
            None,
            "e".repeat(64),
            "2000",
            identity,
        );
        execution
            .configure_resolution_redeem_v1(sim_system::ResolutionRedeemScenarioV1 {
                steps: vec![
                    sim_system::ResolutionRedeemScenarioStepV1 {
                        offset_ms: 0,
                        event: sim_system::ResolutionRedeemScenarioEventV1::SubmissionReceipt(
                            sim_system::SettlementSubmissionReceiptV1::Registered,
                        ),
                    },
                    sim_system::ResolutionRedeemScenarioStepV1 {
                        offset_ms: 59394,
                        event: sim_system::ResolutionRedeemScenarioEventV1::Terminal(
                            sim_system::SettlementTerminalV1::Success,
                        ),
                    },
                    sim_system::ResolutionRedeemScenarioStepV1 {
                        offset_ms: 118788,
                        event: sim_system::ResolutionRedeemScenarioEventV1::RepeatPriorReceipt {
                            prior_step_index: 1,
                        },
                    },
                ],
                costs: vec![sim_system::ScenarioSettlementCostV1 {
                    correction_of: None,
                    cost_identity: "gas-1".into(),
                    recognition_offset_ms: 0,
                    payment_offset_ms: Some(59394),
                    amount_pusd: contracts::ExactDecimal("0.1".into()),
                }],
            })
            .unwrap();
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
            .unwrap();
        let shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let account = shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key;
        let native = NativeInputRoot::new(identity.into(), account, START + offset, rules);
        let source = RecordedPublicGroupSourceV1::open(recording, 128, 1 << 20, times).unwrap();
        Self {
            shared,
            native,
            driver: FinitePublicConsumptionV1::new(execution, source, identity.into()),
            coordinator: SettlementCoordinatorV1::default(),
            scenario,
            offset,
            last_script_at: None,
            checks: Default::default(),
            last_rebate_sample: String::new(),
            model_continuation_started: false,
        }
    }
    fn assert_coverage(&self) {
        for key in [
            "reference-missing",
            "reference-temporary",
            "late-same-timestamp-duplicate-keeps-temporary",
            "same-value-official-and-real-different-ticks",
        ] {
            assert!(self.checks.contains(key), "missing {key}");
        }
        let expected: &[&str] = match self.scenario {
            Scenario::Trade => &[
                "reservation-20-sent-unknown",
                "release-restores-2000",
                "pending-applied-five-success-zero",
                "maker-a-success",
                "paired-five",
                "failed-compensation",
                "gtc-pending-four-active-six",
                "gtc-failed-only-active-six",
                "fok-quote-budget-pending-no-open",
                "fak-quote-budget-pending-no-open",
                "pair-guarantee-to-receivable-nav-unchanged",
                "confirmed-fee-once-before-payment",
                "redeem-payment-fee-not-repeated",
                "repeated-settlement-receipt-no-second-effect",
                "partial-cost-0.7-profit-0.3",
                "different-day-below-threshold-0.0259",
            ],
            Scenario::Orphan => &[
                "winner-loser-directional-only",
                "unknown-credit-other-only-duplicate",
                "unknown-debit-maker-duplicate",
                "original-debit-refund-restores-maker-duplicate",
            ],
            Scenario::Rebate => &[
                "rebate-independent-contributions-nav-excludes-unmatured",
                "rebate-paid-exact-market-ratios",
            ],
        };
        for key in expected {
            assert!(self.checks.contains(key), "missing {key}");
        }
    }
    fn check_inputs(&mut self, group: &NativeInputGroup) {
        use crate::exact::*;
        let now = group.cutoff.decision_time().logical_time_utc_ms() - self.offset;
        for row in &group.rows {
            let index = (0..13)
                .find(|i| condition(*i) == row.hidden_state_key.condition_id.0)
                .unwrap();
            let age = now - (FIRST + index as i64 * 300000);
            if age == -29000 {
                assert_eq!(row.fast[3], 1.);
                self.checks.insert("reference-missing");
            }
            if age == 3000 || age == 6000 {
                assert_eq!(row.fast[4], 1.);
                assert_eq!(
                    row.fast[7],
                    signed_log_f32(&decimal(&contracts::ExactDecimal(
                        "64950.014218511440012345".into()
                    )))
                );
                self.checks.insert(if age == 3000 {
                    "reference-temporary"
                } else {
                    "late-same-timestamp-duplicate-keeps-temporary"
                });
            }
            if age == 11000 {
                assert_eq!(row.fast[5], 1.);
                assert_eq!(
                    row.fast[7],
                    signed_log_f32(&decimal(&contracts::ExactDecimal(
                        "64950.014218511440012345".into()
                    )))
                );
                assert_eq!(row.rule[0], signed_log_f32(&ratio(1, 100)));
                assert_eq!(row.rule[7], signed_log_f32(&ratio(1, 1000)));
                assert_eq!(&row.fast[806..], row.rule.as_slice());
                self.checks
                    .insert("same-value-official-and-real-different-ticks");
            }
        }
    }
    async fn send(&mut self, id: &str, intent: contracts::ModelIntentV1) {
        self.driver.wait_for_recording_capacity().unwrap();
        self.shared
            .execute(
                &mut self.driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: id.into(),
                    caller_correlation_id: None,
                },
                Some(std::sync::Arc::new(intent)),
            )
            .await
            .unwrap()
            .unwrap();
    }
    async fn buy(&mut self, index: usize, side: usize, price: &str, size: i64, id: &str) {
        self.send(
            id,
            contracts::ModelIntentV1::SubmitSingleBuy {
                condition_id: contracts::ConditionId(condition(index)),
                order: contracts::BuyLegIntentV1 {
                    token_id: contracts::TokenId(tokens(index)[side].clone()),
                    order: contracts::BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: price.parse().unwrap(),
                        size_shares: size.into(),
                        post_only: true,
                    },
                },
            },
        )
        .await;
    }
    async fn cancel(&mut self, index: usize, id: &str) {
        self.send(
            id,
            contracts::ModelIntentV1::CancelMarketOrders {
                condition_id: contracts::ConditionId(condition(index)),
            },
        )
        .await;
    }
    async fn script(&mut self) {
        use contracts::*;
        let elapsed = self
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms()
            - self.offset
            - FIRST;
        if self.last_script_at == Some(elapsed) {
            self.check_economics(elapsed);
            return;
        }
        self.last_script_at = Some(elapsed);
        // 明确冻结的场景输入，不是模型选择；只消费本组已可见截止。
        match (self.scenario, elapsed) {
            (Scenario::Trade, 1000) => {
                let items = (0..2)
                    .map(|ordinal| LedgerSubmissionItemIdentityV1 {
                        global_request_id: "script-reserve".into(),
                        batch_item_ordinal: ordinal,
                    })
                    .collect::<Vec<_>>();
                let reservations = items
                    .iter()
                    .map(|item| ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1(
                            serde_json::to_string(item).unwrap(),
                        ),
                        condition_id: ConditionId(condition(0)),
                        requested_principal_pusd: ExactDecimal("10".into()),
                        immediate_fee_reserve_pusd: ExactDecimal("0".into()),
                    })
                    .collect::<Vec<_>>();
                assert!(
                    self.shared
                        .reserve_action_v2(&reservations)
                        .unwrap()
                        .iter()
                        .all(|v| matches!(v, LedgerReservationOutcomeV1::Reserved { .. }))
                );
                for (reserve, item) in reservations.iter().zip(&items) {
                    self.shared
                        .record_submission_started(
                            &LedgerLocalEventIdentityV1(
                                serde_json::to_string(&("submission-started", item)).unwrap(),
                            ),
                            &reserve.request_identity,
                            item,
                        )
                        .unwrap();
                }
                let cash = self.shared.ledger().economic_read().unwrap().cash().clone();
                assert_eq!(
                    (
                        &cash.actual_available_pusd.0,
                        &cash.maker_available_pusd.0,
                        &cash.actual_reserved_pusd.0,
                        &cash.maker_reserved_pusd.0
                    ),
                    (&"1980".into(), &"1980".into(), &"20".into(), &"20".into())
                );
                self.checks.insert("reservation-20-sent-unknown");
                self.send(
                    "script-reserve",
                    ModelIntentV1::SubmitPairedBuy {
                        condition_id: ConditionId(condition(0)),
                        orders: tokens(0).map(|token| BuyLegIntentV1 {
                            token_id: TokenId(token),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: "0.4".parse().unwrap(),
                                size_shares: 25.into(),
                                post_only: true,
                            },
                        }),
                    },
                )
                .await;
            }
            (Scenario::Trade, 35000) => self.cancel(0, "script-release").await,
            (Scenario::Trade, 70000) => self.buy(0, 0, "0.2", 5, "script-maker-a").await,
            (Scenario::Trade, 140000) => self.buy(0, 1, "0.3", 5, "script-maker-b").await,
            (Scenario::Trade, 200000) => self.buy(0, 0, "0.4", 10, "script-failed").await,
            (Scenario::Trade, 290000) => self.cancel(0, "script-failed-remainder").await,
            (Scenario::Trade, 301000) => self.buy(1, 0, "0.1", 5, "script-pool-a").await,
            (Scenario::Trade, 375000) => self.cancel(1, "script-pool-cancel-a").await,
            (Scenario::Trade, 400000) => self.buy(1, 0, "0.9", 5, "script-pool-a2").await,
            (Scenario::Trade, 475000) => self.cancel(1, "script-pool-cancel-a2").await,
            (Scenario::Trade, 500000) => self.buy(1, 1, "0.2", 5, "script-pool-b").await,
            (Scenario::Trade, 601000) | (Scenario::Trade, 671000) => {
                let fok = elapsed == 601000;
                self.send(
                    if fok { "script-fok" } else { "script-fak" },
                    ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(condition(2)),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(tokens(2)[usize::from(!fok)].clone()),
                            order: if fok {
                                BuyOrderIntentV1::FokMarket {
                                    spend_pusd: 3.into(),
                                    worst_price_pusd_per_share: "0.6".parse().unwrap(),
                                }
                            } else {
                                BuyOrderIntentV1::FakMarket {
                                    spend_pusd: 3.into(),
                                    worst_price_pusd_per_share: "0.6".parse().unwrap(),
                                }
                            },
                        },
                    },
                )
                .await;
            }
            (Scenario::Orphan, 900000)
            | (Scenario::Orphan, 920000)
            | (Scenario::Orphan, 940000) => {
                let (identity, amount, correction) = match elapsed {
                    900000 => ("continuous-unknown-credit", "7", None),
                    920000 => ("continuous-unknown-debit", "-3", None),
                    _ => (
                        "continuous-refund-debit",
                        "3",
                        Some("continuous-unknown-debit"),
                    ),
                };
                let fact = contracts::economics::ExternalCashFactV1 {
                    identity: identity.into(),
                    signed_delta_pusd: ExactDecimal(amount.into()),
                    attribution: contracts::economics::ExternalCashAttributionV1::UnknownActivity,
                    correction_of: correction.map(str::to_owned),
                };
                self.driver
                    .decision()
                    .unwrap()
                    .apply_external_cash_fact_v1(fact.clone())
                    .unwrap();
                self.driver
                    .decision()
                    .unwrap()
                    .apply_external_cash_fact_v1(fact)
                    .unwrap();
            }
            (Scenario::Orphan, 1000) => self.buy(0, 0, "0.2", 5, "script-winner").await,
            (Scenario::Orphan, 301000) => self.buy(1, 1, "0.2", 5, "script-loser").await,
            (Scenario::Rebate, 1000) => self.buy(0, 0, "0.5", 143, "script-rebate-low").await,
            (Scenario::Rebate, 301000) => self.buy(1, 0, "0.5", 143, "script-rebate-high").await,
            _ => (),
        }
        self.check_economics(elapsed);
    }
    fn check_economics(&mut self, elapsed: i64) {
        use contracts::economics::*;
        let read = self.shared.ledger().economic_read().unwrap();
        if self.scenario == Scenario::Trade {
            if elapsed == 68000 {
                assert_eq!(read.cash().actual_available_pusd.0, "2000");
                assert_eq!(read.cash().actual_reserved_pusd.0, "0");
                self.checks.insert("release-restores-2000");
            }
            if elapsed == 125000 {
                let m = read
                    .market_totals()
                    .find(|m| m.condition_id.0 == condition(0))
                    .unwrap();
                assert_eq!(m.executed[0].shares.0, "5");
                assert_eq!(m.success_shares[0].0, "0");
                let balance = read
                    .base()
                    .platform_confirmed_projection
                    .outcome_balances
                    .iter()
                    .find(|b| b.token_id.0 == tokens(0)[0])
                    .unwrap();
                assert_eq!(balance.balance_shares.0, "5");
                assert_eq!(
                    AccountEconomicsReadV1::from_ledger(&read).worst_case_maker_nav_pusd,
                    rational("1999", "1")
                );

                self.checks.insert("pending-applied-five-success-zero");
            }
            if elapsed == 610000 || elapsed == 680000 {
                let id = if elapsed == 610000 {
                    "script-fok"
                } else {
                    "script-fak"
                };
                let order = self
                    .shared
                    .registry()
                    .orders()
                    .find(|o| {
                        o.origin
                            .as_ref()
                            .is_some_and(|o| o.intent.global_request_id() == id)
                    })
                    .unwrap();
                assert_eq!(
                    order.active_unfilled_shares,
                    contracts::ExecutionReadValueV1::NotApplicable
                );
                assert_eq!(
                    order.cancellable,
                    contracts::ExecutionReadValueV1::Known(false)
                );
                let commitment = self
                    .shared
                    .registry()
                    .all_commitments()
                    .find(|c| c.origin.intent.global_request_id() == id)
                    .unwrap();
                assert_eq!(
                    commitment.remaining_target_shares,
                    contracts::ExecutionReadValueV1::Known(contracts::ExactDecimal("5".into()))
                );
                self.checks.insert(if elapsed == 610000 {
                    "fok-quote-budget-pending-no-open"
                } else {
                    "fak-quote-budget-pending-no-open"
                });
            }
            if elapsed == 250000 || elapsed == 280000 {
                let order = self
                    .shared
                    .registry()
                    .orders()
                    .find(|o| {
                        o.origin
                            .as_ref()
                            .is_some_and(|o| o.intent.global_request_id() == "script-failed")
                    })
                    .unwrap();
                let commitment = self
                    .shared
                    .registry()
                    .all_commitments()
                    .find(|c| c.origin.intent.global_request_id() == "script-failed")
                    .unwrap();
                assert_eq!(
                    order.active_unfilled_shares,
                    contracts::ExecutionReadValueV1::Known(contracts::ExactDecimal("6".into()))
                );
                assert_eq!(
                    commitment.remaining_target_shares,
                    contracts::ExecutionReadValueV1::Known(contracts::ExactDecimal(
                        if elapsed == 250000 { "10" } else { "6" }.into()
                    ))
                );
                self.checks.insert(if elapsed == 250000 {
                    "gtc-pending-four-active-six"
                } else {
                    "gtc-failed-only-active-six"
                });
            }
            if elapsed == 135000 {
                let m = read
                    .market_totals()
                    .find(|m| m.condition_id.0 == condition(0))
                    .unwrap();
                assert_eq!(m.success_shares[0].0, "5");
                assert_eq!(read.cash().maker_available_pusd.0, "1999");
                self.checks.insert("maker-a-success");
            }
            if elapsed == 230000 || elapsed == 290000 {
                let nav = AccountEconomicsReadV1::from_ledger(&read);
                eprintln!(
                    "economic cut={elapsed} cash={:?} WCM={:?}",
                    read.cash(),
                    nav.worst_case_maker_nav_pusd
                );
                assert_eq!(
                    read.cash()
                        .actual_available_pusd
                        .0
                        .parse::<Decimal>()
                        .unwrap()
                        + read
                            .cash()
                            .actual_reserved_pusd
                            .0
                            .parse::<Decimal>()
                            .unwrap(),
                    "1997.5".parse::<Decimal>().unwrap()
                );
                assert_eq!(nav.worst_case_maker_nav_pusd, rational("4005", "2"));
                self.checks.insert(if elapsed == 230000 {
                    "paired-five"
                } else {
                    "failed-compensation"
                });
            }
            if (301000..=340000).contains(&elapsed) {
                let m = read
                    .market_totals()
                    .find(|m| m.condition_id.0 == condition(0))
                    .unwrap();
                if m.paired_resolution_receivable_pusd.0 == "5" {
                    assert_eq!(m.guaranteed_position_value_pusd.0, "0");
                    let fee = m.unpaid_settlement_fee_pusd.0.as_str();
                    assert_eq!(
                        AccountEconomicsReadV1::from_ledger(&read).worst_case_maker_nav_pusd,
                        if fee == "0" {
                            rational("4005", "2")
                        } else {
                            assert_eq!(fee, "0.1");
                            rational("10012", "5")
                        }
                    );
                    self.checks.insert(if fee == "0" {
                        "pair-guarantee-to-receivable-nav-unchanged"
                    } else {
                        "confirmed-fee-once-before-payment"
                    });
                }
            }
            if elapsed == 365000 || elapsed == 425000 {
                let m = read
                    .market_totals()
                    .find(|m| m.condition_id.0 == condition(0))
                    .unwrap();
                assert_eq!(m.paired_resolution_receivable_pusd.0, "0");
                assert_eq!(m.unpaid_settlement_fee_pusd.0, "0");
                assert_eq!(m.confirmed_net_pnl_pusd, rational("12", "5"));
                assert_eq!(
                    AccountEconomicsReadV1::from_ledger(&read).worst_case_maker_nav_pusd,
                    rational("20023", "10")
                );
                self.checks.insert(if elapsed == 365000 {
                    "redeem-payment-fee-not-repeated"
                } else {
                    "repeated-settlement-receipt-no-second-effect"
                });
            }
            if elapsed == 575000 {
                let pair = read
                    .paired_allocations()
                    .find(|p| p.quantity_shares.0 == "1")
                    .unwrap();
                assert_eq!(
                    pair.principal_cost_pusd,
                    [rational("1", "2"), rational("1", "5")]
                );
                let m = read
                    .market_totals()
                    .find(|m| m.condition_id.0 == condition(1))
                    .unwrap();
                assert_eq!(m.confirmed_net_pnl_pusd, rational("3", "10"));
                self.checks.insert("partial-cost-0.7-profit-0.3");
            }
        }
        if self.scenario == Scenario::Orphan && [910000, 930000, 950000].contains(&elapsed) {
            let nav = AccountEconomicsReadV1::from_ledger(&read);
            assert_eq!(
                nav.worst_case_maker_nav_pusd,
                rational(if elapsed == 930000 { "1995" } else { "1998" }, "1")
            );
            assert_eq!(
                nav.conservative_nav_pusd,
                rational(if elapsed == 930000 { "20069" } else { "20099" }, "10")
            );
            assert_eq!(nav.other_account_residual_pusd, rational("7", "1"));
            self.checks.insert(match elapsed {
                910000 => "unknown-credit-other-only-duplicate",
                930000 => "unknown-debit-maker-duplicate",
                _ => "original-debit-refund-restores-maker-duplicate",
            });
        }
        if self.scenario == Scenario::Rebate && elapsed == 750000 {
            let day = read
                .rebate_days()
                .find(|d| d.kind == RebateKindV1::Maker)
                .unwrap();
            let mut contributions = day
                .contributions
                .iter()
                .map(|c| c.contribution_pusd.0.as_str())
                .collect::<Vec<_>>();
            contributions.sort();
            assert_eq!(contributions, ["0.50002", "0.50004"]);
            // 原逐段微单位本金下取整：142862857/2=71431428、142868571/2=71434285。
            // 两赢家回款285.731428，方向结算费共0.2；不是从待测NAV反推期望。
            let nav = AccountEconomicsReadV1::from_ledger(&read);
            assert_eq!(
                nav.worst_case_maker_nav_pusd,
                rational("1857134287", "1000000")
            );
            assert_eq!(nav.conservative_nav_pusd, rational("428533143", "200000"));
            self.checks
                .insert("rebate-independent-contributions-nav-excludes-unmatured");
        }
        if self.scenario == Scenario::Orphan && elapsed == 850000 {
            let nav = AccountEconomicsReadV1::from_ledger(&read);
            assert_eq!(nav.worst_case_maker_nav_pusd, rational("1998", "1"));
            assert_eq!(nav.conservative_nav_pusd, rational("20029", "10"));
            assert_eq!(nav.directional_residual_pusd, rational("49", "10"));
            let loser = read
                .resolutions()
                .find(|r| r.resolved.condition_id.0 == condition(1))
                .unwrap();
            assert!(loser.request.is_none());
            self.checks.insert("winner-loser-directional-only");
        }
        if elapsed == 3600000 && self.scenario != Scenario::Orphan {
            let day = read
                .rebate_days()
                .find(|d| {
                    d.kind == RebateKindV1::Maker
                        && d.attribution_day_start_utc_ms
                            == (START + self.offset).div_euclid(86_400_000) * 86_400_000
                })
                .unwrap();
            if self.scenario == Scenario::Trade {
                assert_eq!(day.amounts.below_threshold_pusd, rational("259", "10000"));
                assert_eq!(day.amounts.locked_remainder_pusd, rational("0", "1"));
                assert_eq!(day.amounts.paid_pusd, rational("0", "1"));
                self.checks.insert("different-day-below-threshold-0.0259");
            } else {
                assert_eq!(day.amounts.paid_pusd, rational("1", "1"));
                if !self.model_continuation_started {
                    let nav = AccountEconomicsReadV1::from_ledger(&read);
                    assert_eq!(
                        nav.worst_case_maker_nav_pusd,
                        rational("1858134287", "1000000")
                    );
                    assert_eq!(nav.conservative_nav_pusd, rational("428733143", "200000"));
                    self.checks.insert("paid-rebate-nav-adds-one-once");
                }
                assert_eq!(day.amounts.locked_remainder_pusd, rational("3", "50000"));
                for (index, numerator) in [(0, "25001"), (1, "25002")] {
                    let m = read
                        .market_totals()
                        .find(|m| m.condition_id.0 == condition(index))
                        .unwrap();
                    assert_eq!(m.rebates[0].paid_pusd, rational(numerator, "50003"));
                }
                self.checks.insert("rebate-paid-exact-market-ratios");
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "三变体真实经济前史诊断，尚非三端完整矩阵"]
async fn continuous_economic_scenarios() {
    for (case, scenario) in [Scenario::Trade, Scenario::Orphan, Scenario::Rebate]
        .into_iter()
        .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let recording = temp.path().join("dpt");
        let offset = case as i64 * 86_400_000
            + if scenario == Scenario::Rebate {
                -300_000
            } else {
                0
            };
        let (core, times) = recorded_scenario(&recording, Some(scenario), offset);
        let mut root = ContinuousRoot::new(
            &temp.path().join("root"),
            &recording,
            &core,
            times,
            scenario,
            offset,
            &format!("economic-{case}"),
        );
        let mut frames = 0;
        loop {
            assert!(root.driver.next_group().unwrap().is_some());
            let at = root
                .driver
                .decision()
                .unwrap()
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            root.shared
                .advance_settlement(&mut root.coordinator, &mut root.driver)
                .unwrap();
            if let Some(group) = root
                .native
                .consume(&mut root.shared, &mut root.driver)
                .unwrap()
            {
                root.check_inputs(&group);
                frames += group.rows.len();
            }
            root.script().await;
            if at == END + offset {
                break;
            }
        }
        root.assert_coverage();
        eprintln!(
            "economics scenario={scenario:?} frames={frames} checks={:?} days={:?}",
            root.checks,
            root.shared
                .ledger()
                .economic_read()
                .unwrap()
                .rebate_days()
                .map(|day| (
                    day.kind,
                    day.attribution_day_start_utc_ms,
                    day.amounts.clone()
                ))
                .collect::<Vec<_>>()
        );
        root.driver.stop_and_drain().unwrap();
    }
}

// 只负责验收产物生命周期；异常保留实际写出的原记录，不改变业务错误或消费行为。
struct ObservationDirectory {
    directory: Option<tempfile::TempDir>,
    last_applied_cutoffs: Vec<i64>,
    stage: &'static str,
    current_groups: Vec<serde_json::Value>,
}
impl ObservationDirectory {
    fn new(parent: &Path, roots: usize) -> Self {
        Self {
            directory: Some(tempfile::tempdir_in(parent).unwrap()),
            last_applied_cutoffs: vec![0; roots],
            stage: "initialization",
            current_groups: vec![serde_json::Value::Null; roots],
        }
    }
    fn path(&self) -> &Path {
        self.directory.as_ref().unwrap().path()
    }
}
impl Drop for ObservationDirectory {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let path = self.directory.take().unwrap().keep();
            let detail = serde_json::json!({"stage":self.stage,"last_applied_cutoffs":self.last_applied_cutoffs,"current_groups":self.current_groups,"scope":"original failed observation; actual retained SIM/NOE/Ledger files, no successor consumption; full error in process log"});
            let _ = std::fs::write(path.join("failure-cutoff.json"), detail.to_string());
            eprintln!("failed observation retained at {} {detail}", path.display());
        }
    }
}

async fn warm_continuous_runtime(runtime: &mut crate::continuous_runtime::ThreeRuntime) -> f64 {
    use std::sync::Arc;
    let parent = std::env::var("CONTINUOUS_OUTPUT").unwrap();
    let mut warm = ObservationDirectory::new(Path::new(&parent), 1);
    let (core, times) = recorded_continuous(&warm.path().join("dpt"));
    let mut root = ContinuousRoot::new(
        &warm.path().join("root"),
        &warm.path().join("dpt"),
        &core,
        times,
        Scenario::Trade,
        0,
        "independent-warmup",
    );
    let started = std::time::Instant::now();
    while runtime.frames < 128 {
        warm.stage = "warmup-next-group";
        next_recorded_group(&mut root.driver);
        warm.last_applied_cutoffs[0] = root
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        warm.stage = "warmup-consume-state-actor-execute";
        if let Some(group) = root
            .native
            .consume(&mut root.shared, &mut root.driver)
            .unwrap()
        {
            for (_, actor) in runtime.advance(&[Arc::new(group)], true) {
                actor
                    .execute(
                        &mut root.shared,
                        &mut root.driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: format!("warm-{}", runtime.frames),
                            caller_correlation_id: None,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        root.driver.drain_measurement_observations();
    }
    assert_eq!(runtime.frames, 128, "独立预热以实际帧计数");
    root.driver.stop_and_drain().unwrap();
    drop(root.shared);
    drop(root.native);
    runtime.release_observation();
    started.elapsed().as_secs_f64()
}

// 从原录制及实际交付时刻独立列举合法行；不依赖 Native 的返回或模型帧总数。
fn recorded_frame_expectations(
    replay: &mut data_pipeline_training::public_replay::PublicReplayV1,
    times: &TradingTimes,
    started_at: i64,
    end: i64,
) -> BTreeMap<u64, Vec<(bool, String)>> {
    let mut expected = BTreeMap::new();
    while let Some((_, applied)) = replay.next_applied().unwrap() {
        for group in applied.applied.closed_groups {
            let at = times.0[group.group_position as usize];
            if at > end {
                continue;
            }
            let mut rows = group
                .observations
                .iter()
                .filter_map(|observation| {
                    let window = &observation.market.window;
                    if started_at > window.start_at_utc_ms - 30000
                        || at < window.start_at_utc_ms - 30000
                        || at >= window.end_at_utc_ms
                    {
                        return None;
                    }
                    Some((
                        at >= window.start_at_utc_ms,
                        observation.market.condition_id.0.clone(),
                    ))
                })
                .collect::<Vec<_>>();
            rows.sort();
            assert!(expected.insert(group.group_position, rows).is_none());
        }
    }
    expected
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "正式每配置/线程完整C1/C2三变体三次；需原图和证据目录"]
async fn continuous_three_runtime_matrix() {
    use pyo3::prelude::*;
    use std::{io::Write, sync::Arc};
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let output = std::env::var("CONTINUOUS_OUTPUT").unwrap();
    std::fs::create_dir_all(&output).unwrap();
    let threads = std::env::var("MODEL_THREADS")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut evidence = std::io::BufWriter::new(
        std::fs::File::create(Path::new(&output).join("samples.jsonl")).unwrap(),
    );
    let loading_started = std::time::Instant::now();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, threads);
    let loading_seconds = loading_started.elapsed().as_secs_f64();
    writeln!(evidence,"{}",serde_json::json!({"phase":"provenance","loading_seconds_all_three":loading_seconds,"value":runtime.provenance(),"build":"cargo test --release --features continuous-validation --locked --offline"})).unwrap();
    let warmup_seconds = warm_continuous_runtime(&mut runtime).await;
    writeln!(evidence,"{}",serde_json::json!({"phase":"warmup","frames":128,"seconds":warmup_seconds,"statistics":runtime.statistics(),"after_release":runtime.sample()})).unwrap();
    for roots_count in [1usize, 2] {
        let parameters: Vec<Vec<i64>> = Python::attach(|py| {
            let rng = py
                .import("random")
                .unwrap()
                .getattr("Random")
                .unwrap()
                .call1((742000 + roots_count,))
                .unwrap();
            (0..3)
                .map(|_| {
                    (0..roots_count)
                        .map(|_| {
                            rng.call_method1("randint", (100, 200))
                                .unwrap()
                                .extract()
                                .unwrap()
                        })
                        .collect()
                })
                .collect()
        });
        for (case, scenario) in [Scenario::Trade, Scenario::Orphan, Scenario::Rebate]
            .into_iter()
            .enumerate()
        {
            let offset = case as i64 * 86_400_000
                + if scenario == Scenario::Rebate {
                    -300000
                } else {
                    0
                };
            for repeat in 0..3 {
                let mut temp = ObservationDirectory::new(Path::new(&output), roots_count);
                let fixture_started = std::time::Instant::now();
                let mut roots = Vec::new();
                let mut recorded_frames = Vec::new();
                // 根索引同时用于独立账户身份和原录制路径，保留明确的根数边界。
                #[allow(clippy::needless_range_loop)]
                for index in 0..roots_count {
                    let recording = Path::new(&std::env::var("CONTINUOUS_RECORDINGS").unwrap())
                        .join(format!("C{roots_count}-{case}-{index}"));
                    let metadata: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(recording.join("fixture.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(metadata["depth"], parameters[case][index]);
                    assert_eq!(metadata["offset"], offset);
                    let times = TradingTimes(
                        serde_json::from_value(metadata["delivery_times"].clone()).unwrap(),
                    );
                    let mut replay = data_pipeline_training::public_replay::PublicReplayV1::open(
                        &recording.join("dpt"),
                        128,
                        1 << 20,
                    )
                    .unwrap();
                    let expected = recorded_frame_expectations(
                        &mut replay,
                        &times,
                        START + offset,
                        END + offset,
                    );
                    recorded_frames.push(expected);
                    let core = replay.into_core();
                    let recording = recording.join("dpt");
                    roots.push(ContinuousRoot::new(
                        &temp.path().join(format!("root-{index}")),
                        &recording,
                        &core,
                        times,
                        scenario,
                        offset,
                        &format!("C{roots_count}-{case}-{repeat}-{index}"),
                    ));
                }
                let first_frame = runtime.frames;
                let first_decision = runtime.decisions;
                let started = std::time::Instant::now();
                let mut next_sample = 128;
                let mut complete = vec![false; roots_count];
                let mut n2_seen = vec![false; roots_count];
                let mut executed_decisions = 0usize;
                let mut parameter_probe_decisions = 0usize;
                let mut group_times = Vec::new();
                let mut execute_times = Vec::new();
                let mut retired = 0;
                writeln!(evidence,"{}",serde_json::json!({"phase":"start","expected_frames_by_root":recorded_frames.iter().map(|groups|groups.values().map(Vec::len).sum::<usize>()).collect::<Vec<_>>(),"c":roots_count,"scenario":format!("{scenario:?}"),"repeat":repeat,"input_seed":742000+roots_count,"book_depths":parameters[case],"fixture_seconds":fixture_started.elapsed().as_secs_f64(),"resources":runtime.sample(),"sources":roots.iter_mut().map(ContinuousRoot::source_sample).collect::<Vec<_>>()})).unwrap();
                while complete.iter().any(|done| !*done) {
                    let group_started = std::time::Instant::now();
                    let mut groups = Vec::new();
                    let mut indices = Vec::new();
                    let mut cutoffs = Vec::new();
                    let mut retire_now = 0;
                    let mut rebate_changes = Vec::new();
                    for (index, root) in roots.iter_mut().enumerate() {
                        if complete[index] {
                            continue;
                        }
                        temp.stage = "next-group";
                        next_recorded_group(&mut root.driver);
                        let at = root
                            .driver
                            .decision()
                            .unwrap()
                            .cutoff()
                            .decision_time()
                            .logical_time_utc_ms();
                        cutoffs.push(at);
                        temp.last_applied_cutoffs[index] = at;
                        let cutoff = root.driver.decision().unwrap().cutoff().clone();
                        temp.current_groups[index] =
                            serde_json::json!({"phase":"before-exogenous","cutoff":cutoff});
                        let position = cutoff.receipt().group().group_position;
                        let expected_rows = recorded_frames[index]
                            .remove(&position)
                            .expect("原录制组只能消费一次");
                        temp.stage = "settlement-native-consume-script";
                        let _settlement_advanced = root
                            .shared
                            .advance_settlement(&mut root.coordinator, &mut root.driver)
                            .unwrap();
                        let cutoff = root.driver.decision().unwrap().cutoff().clone();
                        temp.current_groups[index] =
                            serde_json::json!({"phase":"after-exogenous","cutoff":cutoff});
                        let consumed = root
                            .native
                            .consume(&mut root.shared, &mut root.driver)
                            .unwrap();
                        let cutoff = root.driver.decision().unwrap().cutoff().clone();
                        temp.current_groups[index] =
                            serde_json::json!({"phase":"after-native-consume","cutoff":cutoff});
                        let group = consumed
                            .unwrap_or_else(|| panic!("原公共组未完成Native消费: {cutoff:?}"));
                        let actual_rows = group
                            .rows
                            .iter()
                            .map(|r| {
                                (
                                    r.role == InputRole::DecisionRequired,
                                    r.hidden_state_key.condition_id.0.clone(),
                                )
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(
                            actual_rows, expected_rows,
                            "原录制合法帧必须完整消费: {cutoff:?}"
                        );
                        {
                            root.check_inputs(&group);
                            if root.scenario == Scenario::Rebate && at == FIRST + offset + 3_600_000
                            {
                                for old in [condition(0), condition(1)] {
                                    assert!(group.old.iter().any(|c| c.0 == old));
                                    assert!(
                                        group.rows.iter().all(|r| r
                                            .hidden_state_key
                                            .condition_id
                                            .0
                                            != old)
                                    );
                                }
                                root.checks
                                    .insert("late-rebate-stays-old-without-gru-revival");
                            }
                            retire_now += group
                                .post_frame_hidden_ops
                                .iter()
                                .filter(|op| matches!(op, HiddenOp::Retire { .. }))
                                .count();
                            groups.push(Arc::new(group));
                            indices.push(index);
                        }
                        root.script().await;
                        let stage = format!(
                            "{:?}",
                            root.shared
                                .ledger()
                                .economic_read()
                                .unwrap()
                                .rebate_days()
                                .map(|d| (
                                    d.kind,
                                    d.attribution_day_start_utc_ms,
                                    d.amounts.accrued_unmatured_pusd,
                                    d.amounts.below_threshold_pusd,
                                    d.amounts.confirmed_unpaid_pusd,
                                    d.amounts.paid_pusd,
                                    d.amounts.locked_remainder_pusd
                                ))
                                .collect::<Vec<_>>()
                        );
                        if stage != root.last_rebate_sample {
                            rebate_changes.push((index, stage.clone()));
                            root.last_rebate_sample = stage;
                        }
                        complete[index] = at == END + offset;
                        root.driver.drain_measurement_observations();
                    }
                    // 所有根使用相同真实日历与脚本边界；以后续原始组继续各自隐藏历史。
                    let autonomous = cutoffs.iter().all(|at| *at >= FIRST + offset + 1_000_000);
                    if autonomous {
                        for root in &mut roots {
                            root.model_continuation_started = true;
                        }
                    }
                    let selected = groups
                        .iter()
                        .zip(&indices)
                        .map(|(group, &index)| {
                            autonomous
                                || (!n2_seen[index]
                                    && group.cancel.len() == 2
                                    && group
                                        .rows
                                        .iter()
                                        .any(|r| r.role == InputRole::DecisionRequired))
                        })
                        .collect::<Vec<_>>();
                    temp.stage = "three-runtime-state-and-parameters";
                    let actors = runtime.advance_selected(&groups, &selected);
                    for (index, actor) in actors {
                        let original = indices[index];
                        let root = &mut roots[original];
                        if !autonomous {
                            assert_eq!(groups[index].cancel.len(), 2);
                            assert!(!n2_seen[original]);
                            n2_seen[original] = true;
                            parameter_probe_decisions += 1;
                            writeln!(evidence,"{}",serde_json::json!({"phase":"n2-parameter-handoff","submitted":false,"c":roots_count,"scenario":format!("{scenario:?}"),"repeat":repeat,"root":original,
                                "frame":runtime.frames,"valid_blocks":[2],"path":actor.path,"intent":format!("{:?}",actor.intent().unwrap()),"sources":root.source_sample(),"runtime":runtime.sample()})).unwrap();
                            continue;
                        }
                        temp.stage = "execute-original-intent";
                        executed_decisions += 1;
                        let execute_started = std::time::Instant::now();
                        actor
                            .execute(
                                &mut root.shared,
                                &mut root.driver.decision().unwrap(),
                                RequestContextV2 {
                                    global_request_id: format!(
                                        "model-{original}-{}",
                                        runtime.frames
                                    ),
                                    caller_correlation_id: None,
                                },
                            )
                            .await
                            .unwrap();
                        execute_times.push(execute_started.elapsed().as_secs_f64());
                    }
                    let frames = runtime.frames - first_frame;
                    retired += retire_now;
                    if frames >= next_sample || retire_now > 0 || !rebate_changes.is_empty() {
                        let crossed = if frames >= next_sample {
                            let first = next_sample;
                            while next_sample <= frames {
                                next_sample += 128;
                            }
                            Some((first, next_sample - 128))
                        } else {
                            None
                        };
                        writeln!(evidence,"{}",serde_json::json!({"phase":"observe","c":roots_count,"scenario":format!("{scenario:?}"),"repeat":repeat,"frames":frames,"crossed_128_boundaries":crossed,"rebate_changes":rebate_changes,"cutoffs":cutoffs,"retired":retired,"cancel_objects":groups.iter().map(|g|g.cancel.len()).collect::<Vec<_>>(),"held_groups":groups.iter().map(Arc::strong_count).collect::<Vec<_>>(),"sources":roots.iter_mut().map(ContinuousRoot::source_sample).collect::<Vec<_>>(),"resources":runtime.sample(),"drivers":roots.iter().map(|r|r.driver.measurement_resources()).collect::<Vec<_>>()})).unwrap();
                    }
                    group_times.push(group_started.elapsed().as_secs_f64());
                }
                assert!(n2_seen.iter().all(|seen| *seen), "每根真实N=2两图交接");
                assert!(
                    recorded_frames.iter().all(BTreeMap::is_empty),
                    "截止内原录制组必须全部消费"
                );
                for (root_index, mut root) in roots.into_iter().enumerate() {
                    root.assert_coverage();
                    writeln!(evidence,"{}",serde_json::json!({"phase":"end-before-drain","c":roots_count,"scenario":format!("{scenario:?}"),"repeat":repeat,"sources":root.source_sample(),"checks":root.checks,"retained_orders":root.shared.registry().orders().map(|o|serde_json::json!({"identity":format!("{:?}",o.order_identity),"active_unfilled_shares":o.active_unfilled_shares,"cancellable":o.cancellable,"accepted":o.accepted.is_some(),"formed":o.formed.is_some()})).collect::<Vec<_>>(),"retained_commitments":root.shared.registry().all_commitments().map(|c|serde_json::json!({"request":c.origin.intent.global_request_id(),"ordinal":c.origin.batch_item_ordinal,"order_identity":c.order_identity.map(|id|format!("{id:?}")),"remaining_target_shares":c.remaining_target_shares})).collect::<Vec<_>>(),"resources":root.driver.measurement_resources()})).unwrap();
                    root.driver.stop_and_drain().unwrap();
                    use std::io::BufRead;
                    let file = std::fs::File::open(
                        temp.path().join(format!("root-{root_index}/sim.jsonl")),
                    )
                    .unwrap();
                    let mut closes = 0usize;
                    let mut before_next = 0usize;
                    for line in std::io::BufReader::new(file).lines() {
                        let value: serde_json::Value =
                            serde_json::from_str(&line.unwrap()).unwrap();
                        if value["kind"] != "ACCEPTED_INPUTS" {
                            continue;
                        }
                        for input in value["body"].as_array().unwrap() {
                            if input["input"] != "PUBLIC_CLOSE_TIME" {
                                continue;
                            }
                            closes += 1;
                            let body = &input["body"];
                            if let (Some(at), Some(next)) = (
                                body["execution_time"]["logical_time_utc_ms"].as_i64(),
                                body["closure"]["NextGlobalEventAt"].as_i64(),
                            ) {
                                before_next += usize::from(at.checked_add(1) == Some(next));
                            }
                        }
                    }
                    writeln!(evidence,"{}",serde_json::json!({"phase":"drained-close-cost","c":roots_count,"scenario":format!("{scenario:?}"),"repeat":repeat,"root":root_index,"close_time_count":closes,"close_at_next_minus_one_count":before_next,"timing_included_in":"group"})).unwrap();
                }
                let seconds = started.elapsed().as_secs_f64();
                runtime.release_observation();
                let summarize = |mut values: Vec<f64>| {
                    values.sort_by(f64::total_cmp);
                    if values.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!({"count":values.len(),"median_seconds":values[values.len()/2],"p95_seconds":values[(values.len()-1)*95/100],"max_seconds":values.last()})
                    }
                };
                writeln!(evidence,"{}",serde_json::json!({"phase":"end-after-release","held_observation_roots":0,"c":roots_count,"scenario":format!("{scenario:?}"),"repeat":repeat,"seconds":seconds,"frames":runtime.frames-first_frame,"compared_decisions":runtime.decisions-first_decision,"executed_decisions":executed_decisions,"parameter_probe_decisions":parameter_probe_decisions,"retired":retired,"statistics":runtime.statistics(),"group":summarize(group_times),"execute":summarize(execute_times),"resources":runtime.sample()})).unwrap();
                evidence.flush().unwrap();
                eprintln!(
                    "C{roots_count} {scenario:?} repeat={repeat} frames={} executed_decisions={executed_decisions} parameter_probes={parameter_probe_decisions} seconds={seconds:.3}",
                    runtime.frames - first_frame
                );
            }
        }
    }
}

// P 表期望独立写明；只聚合原公共读取，不从模型张量反算资金。
fn assert_population_economics(
    shared: &SharedExecutionV2,
    count: usize,
    removed: bool,
    j: usize,
    root: usize,
    at: i64,
) {
    use contracts::ExecutionReadValueV1::Known;
    use rust_decimal::Decimal;
    let (families, shares, principal, available) = match (count, removed) {
        (0, _) => ([0, 0, 1], ["0", "0"], ["0", "0"], "2000"),
        (1, false) => ([2, 1, 1], ["5", "5"], ["0.5", "0.5"], "1999"),
        (1, true) => ([1, 0, 1], ["5", "0"], ["0.5", "0"], "1999.5"),
        (85, false) => ([170, 85, 1], ["425", "425"], ["42.5", "42.5"], "1915"),
        (86, false) => ([172, 86, 1], ["430", "430"], ["43", "43"], "1914"),
        (86, true) => ([171, 85, 1], ["430", "425"], ["43", "42.5"], "1914.5"),
        _ => unreachable!(),
    };
    let decimal = |s: &str| s.parse::<Decimal>().unwrap();
    let known = |v: contracts::ExecutionReadValueV1<contracts::ExactDecimal>| match v {
        Known(v) => decimal(&v.0),
        other => panic!("P真实读取不可用: {other:?}"),
    };
    let registry = shared.registry();
    let Known(objects) = registry.cancel_objects() else {
        panic!("完整人口不可用")
    };
    let mut actual_families = [0usize; 3];
    for object in objects {
        actual_families[match object {
            CancelObjectReadV2::Single { .. } => 0,
            CancelObjectReadV2::Paired { .. } => 1,
            CancelObjectReadV2::Market { .. } => 2,
        }] += 1;
    }
    assert_eq!(actual_families, families);
    let funds = shared.ledger().funds_read_v1();
    let mut active = [Decimal::ZERO; 2];
    let mut money = [Decimal::ZERO; 2];
    let mut fee = [Decimal::ZERO; 2];
    for order in registry.orders() {
        let ordinal = order.origin.as_ref().unwrap().batch_item_ordinal as usize;
        active[ordinal] += known(order.active_unfilled_shares);
        let funding = funds.order(order.order_identity).unwrap();
        money[ordinal] += known(funding.principal_encumbrance_pusd);
        fee[ordinal] += known(funding.immediate_fee_encumbrance_pusd);
    }
    assert_eq!(active, shares.map(decimal));
    assert_eq!(money, principal.map(decimal));
    assert_eq!(fee, [Decimal::ZERO; 2]);
    let projection = funds.projection().unwrap();
    let applied = &projection.platform_confirmed_projection;
    assert_eq!(decimal(&applied.cash_pusd.0), decimal("2000"));
    assert_eq!(decimal(&applied.available_pusd.0), decimal(available));
    assert_eq!(
        decimal(&applied.principal_encumbrance_pusd.0),
        money.iter().sum()
    );
    assert_eq!(
        decimal(&applied.immediate_taker_fee_encumbrance_pusd.0),
        Decimal::ZERO
    );
    let economic = shared.ledger().economic_read().unwrap();
    let cash = economic.cash();
    assert_eq!(decimal(&cash.actual_available_pusd.0), decimal(available));
    assert_eq!(decimal(&cash.maker_available_pusd.0), decimal(available));
    assert_eq!(decimal(&cash.actual_reserved_pusd.0), money.iter().sum());
    assert_eq!(decimal(&cash.maker_reserved_pusd.0), money.iter().sum());
    eprintln!(
        "population_economics={}",
        serde_json::json!({"j":j,"root":root,"cutoff":at,"families":actual_families,"active_shares":active,"principal":money,"immediate_fee":fee,"applied":applied,"cash":{"actual_available":cash.actual_available_pusd,"actual_reserved":cash.actual_reserved_pusd,"maker_available":cash.maker_available_pusd,"maker_reserved":cash.maker_reserved_pusd}})
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "两MAIN的真实P3—P7人口/批次/分块/梯度；需原图"]
async fn continuous_real_population_probes() {
    use pyo3::prelude::*;
    use std::sync::Arc;
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let threads = std::env::var("MODEL_THREADS").unwrap().parse().unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, threads);
    runtime.enable_population();
    for j in 3usize..=7 {
        let probe_start_frame = runtime.frames;
        let probe_started = std::time::Instant::now();
        let counts = match j {
            3 => vec![0],
            4 => vec![1],
            5 => vec![85],
            6 => vec![86],
            _ => vec![0, 86],
        };
        let depths: Vec<i64> = Python::attach(|py| {
            let rng = py
                .import("random")
                .unwrap()
                .getattr("Random")
                .unwrap()
                .call1((742000 + j,))
                .unwrap();
            counts
                .iter()
                .map(|_| {
                    rng.call_method1("randint", (100, 200))
                        .unwrap()
                        .extract()
                        .unwrap()
                })
                .collect()
        });
        let mut roots = counts
            .iter()
            .enumerate()
            .map(|(i, count)| {
                crate::action_tests::Harness::named(
                    *count,
                    false,
                    false,
                    &format!("P{j}-root-{i}"),
                    &depths[i].to_string(),
                )
            })
            .collect::<Vec<_>>();
        for _ in 0..4 {
            let mut groups = Vec::new();
            for h in &mut roots {
                h.driver.next_group().unwrap().unwrap();
                groups.push(Arc::new(
                    h.native
                        .consume(&mut h.shared, &mut h.driver)
                        .unwrap()
                        .unwrap(),
                ));
            }
            assert!(runtime.advance(&groups, false).is_empty());
        }
        for (h, count) in roots.iter_mut().zip(&counts) {
            h.pairs(*count).await;
        }
        loop {
            let mut groups = Vec::new();
            let mut indices = Vec::new();
            for (index, h) in roots.iter_mut().enumerate() {
                h.driver.next_group().unwrap().unwrap();
                if let Some(group) = h.native.consume(&mut h.shared, &mut h.driver).unwrap() {
                    groups.push(Arc::new(group));
                    indices.push(index);
                }
            }
            let at = roots[0]
                .driver
                .decision()
                .unwrap()
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            assert!(
                groups
                    .iter()
                    .all(|g| g.cutoff.decision_time().logical_time_utc_ms() == at)
            );
            let actors = runtime.advance(&groups, at == 56000);
            if at == 36000 || at == 56000 {
                for (index, h) in roots.iter().enumerate() {
                    assert_population_economics(
                        &h.shared,
                        counts[index],
                        at == 56000 && [4, 6, 7].contains(&j),
                        j,
                        index,
                        at,
                    );
                }
                let sources = roots
                    .iter_mut()
                    .map(|h| {
                        let cutoff = h.driver.decision().unwrap().cutoff().clone();
                        source_sample(&h.shared, Some(&cutoff))
                    })
                    .collect::<Vec<_>>();
                eprintln!(
                    "population_cutpoint={}",
                    serde_json::json!({"j":j,"cutoff":at,"elapsed_from_preparation_seconds":probe_started.elapsed().as_secs_f64(),"actual_frames_this_probe":runtime.frames-probe_start_frame,"actual_frames_total":runtime.frames,"runtime":runtime.sample(),"sources":sources,
                    "root_mapping":indices,"valid_blocks":groups.iter().map(|g|(0..g.cancel.len()).step_by(256).map(|i|(g.cancel.len()-i).min(256)).collect::<Vec<_>>()).collect::<Vec<_>>() })
                );
            }
            if at == 36000 {
                for (group, &index) in groups.iter().zip(&indices) {
                    let count = &counts[index];
                    assert_eq!(group.cancel.len(), 3 * count + 1);
                    if [4, 6, 7].contains(&j) && *count > 0 {
                        let contracts::ExecutionReadValueV1::Known(objects) =
                            group.action_before.as_ref().unwrap().cancel_objects()
                        else {
                            panic!()
                        };
                        let target=objects.iter().position(|o|matches!(o,CancelObjectReadV2::Single{order} if order.origin.as_ref().is_some_and(|o|o.intent.global_request_id()==format!("pair-{}",count-1)&&o.batch_item_ordinal==1))).unwrap();
                        let mut actor = NativeActor::new(group.clone(), 0).unwrap();
                        actor.choose(3).unwrap();
                        actor.choose(target).unwrap();
                        let h = &mut roots[index];
                        actor
                            .execute(
                                &mut h.shared,
                                &mut h.driver.decision().unwrap(),
                                RequestContextV2 {
                                    global_request_id: "population-remove-one-leg".into(),
                                    caller_correlation_id: None,
                                },
                            )
                            .await
                            .unwrap();
                    }
                }
            }
            if at == 56000 {
                assert_eq!(indices, (0..roots.len()).collect::<Vec<_>>());
                let expected = match j {
                    3 => vec![1],
                    4 => vec![2],
                    5 => vec![256],
                    6 => vec![257],
                    _ => vec![1, 257],
                };
                assert_eq!(
                    groups.iter().map(|g| g.cancel.len()).collect::<Vec<_>>(),
                    expected
                );
                runtime.audit_population(&groups, 742000 + j);
                for (index, actor) in actors {
                    let h = &mut roots[indices[index]];
                    actor
                        .execute(
                            &mut h.shared,
                            &mut h.driver.decision().unwrap(),
                            RequestContextV2 {
                                global_request_id: format!("P{j}-actual-model-{index}"),
                                caller_correlation_id: None,
                            },
                        )
                        .await
                        .unwrap();
                }
                eprintln!(
                    "P{j} counts={expected:?} depths={depths:?} full_three_runtime_population_audit_passed"
                );
                break;
            }
        }
        // 原动作执行后沿真实因果组继续，直到真实EOF；无模型帧的组仍消费。
        let mut ended = vec![false; roots.len()];
        while ended.iter().any(|done| !*done) {
            let mut groups = Vec::new();
            for (index, h) in roots.iter_mut().enumerate() {
                if ended[index] {
                    continue;
                }
                if h.driver.next_group().unwrap().is_some() {
                    if let Some(group) = h.native.consume(&mut h.shared, &mut h.driver).unwrap() {
                        groups.push(Arc::new(group));
                    }
                } else {
                    ended[index] = true;
                }
            }
            runtime.advance(&groups, false);
        }
        for h in roots {
            h.driver.stop_and_drain().unwrap();
        }
        runtime.release_observation();
        eprintln!(
            "population_end={}",
            serde_json::json!({"j":j,"actual_frames":runtime.frames-probe_start_frame,"seconds":probe_started.elapsed().as_secs_f64(),"runtime":runtime.sample()})
        );
    }
}

#[test]
#[ignore = "保留C1/C2九份实际公共输入；正式矩阵只读复用"]
fn continuous_prepare_recordings() {
    use pyo3::prelude::*;
    let directory = std::env::var("CONTINUOUS_RECORDINGS").unwrap();
    assert!(!Path::new(&directory).exists(), "保留既有录制，禁止覆盖");
    std::fs::create_dir_all(&directory).unwrap();
    for j in [1usize, 2] {
        let rng = Python::attach(|py| {
            py.import("random")
                .unwrap()
                .getattr("Random")
                .unwrap()
                .call1((742000 + j,))
                .unwrap()
                .unbind()
        });
        for (case, scenario) in [Scenario::Trade, Scenario::Orphan, Scenario::Rebate]
            .into_iter()
            .enumerate()
        {
            for root in 0..j {
                let depth: i64 = Python::attach(|py| {
                    rng.call_method1(py, "randint", (100, 200))
                        .unwrap()
                        .extract(py)
                        .unwrap()
                });
                let offset = case as i64 * 86_400_000
                    + if scenario == Scenario::Rebate {
                        -300000
                    } else {
                        0
                    };
                let destination = Path::new(&directory).join(format!("C{j}-{case}-{root}"));
                std::fs::create_dir(&destination).unwrap();
                let (_, times) = recorded_scenario_depth(
                    &destination.join("dpt"),
                    Some(scenario),
                    offset,
                    depth,
                );
                std::fs::write(destination.join("fixture.json"),serde_json::to_vec_pretty(&serde_json::json!({"j":j,"scenario":format!("{scenario:?}"),"root":root,"input_seed":742000+j,"depth":depth,"offset":offset,"delivery_times":times.0})).unwrap()).unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "原真实128帧预热回归；需原图和证据目录"]
async fn continuous_actual_warmup_regression() {
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    let parent = std::env::var("ASSOCIATION_EVIDENCE").unwrap();
    std::fs::create_dir_all(&parent).unwrap();
    let temp = tempfile::tempdir_in(&parent).unwrap().keep();
    let recording = temp.join("dpt");
    eprintln!("preserved_case={}", temp.display());
    let (core, times) = recorded_continuous(&recording);
    let mut root = ContinuousRoot::new(
        &temp.join("root"),
        &recording,
        &core,
        times,
        Scenario::Trade,
        0,
        "independent-warmup",
    );
    let mut markets = BTreeMap::new();
    while runtime.frames < 128 {
        loop {
            root.driver.wait_for_recording_capacity().unwrap();
            match root.driver.next_group() {
                Err(SimRunActorErrorV2::RecordingBackpressured) => continue,
                other => {
                    other.unwrap().unwrap();
                    break;
                }
            }
        }
        let now = root
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        for frame in root.driver.decision().unwrap().frames() {
            markets.insert(
                frame.observation.market.condition_id.0.clone(),
                crate::root::MarketBinding {
                    market: frame.observation.market.clone(),
                    assets: frame
                        .observation
                        .assets
                        .iter()
                        .map(|a| a.asset.clone())
                        .collect(),
                },
            );
        }
        let result = root.native.consume(&mut root.shared, &mut root.driver);
        if now == 86_106_000 {
            let order = root
                .shared
                .registry()
                .orders()
                .find(|order| {
                    order.origin.as_ref().is_some_and(|o| {
                        o.intent.global_request_id() == "warm-44" && o.batch_item_ordinal == 1
                    })
                })
                .expect("原warm44第二腿同组已关联");
            assert!(order.formed.is_some() && order.accepted.is_some());
            eprintln!(
                "original_failure_cutoff={now} same_group_association={:?} accepted={:?}",
                order
                    .origin
                    .as_ref()
                    .map(|o| (o.intent.global_request_id(), o.batch_item_ordinal)),
                order
                    .accepted
                    .map(|r| (r.visibility(), r.record().source_reference()))
            );
        }
        if let Some(group) = result.unwrap() {
            for (_, actor) in runtime.advance(&[std::sync::Arc::new(group)], true) {
                eprintln!(
                    "actual warm cut={now} frame={} path={:?} intent={:?}",
                    runtime.frames,
                    actor.path,
                    actor.intent().unwrap()
                );
                actor
                    .execute(
                        &mut root.shared,
                        &mut root.driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: format!("warm-{}", runtime.frames),
                            caller_correlation_id: None,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
    }
    eprintln!(
        "warmup_sample_schema={}",
        serde_json::json!({"frames":runtime.frames,"runtime":runtime.sample(),"sources":root.source_sample()})
    );
    root.driver.stop_and_drain().unwrap();
    drop(root.shared);
    drop(root.native);
    runtime.release_observation();
    eprintln!("warmup_released_schema={}", runtime.sample());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "既有经济前史上的瞬时采样schema核对；不是正式三端矩阵"]
async fn continuous_source_sample_schema() {
    let temp = tempfile::tempdir().unwrap();
    let recording = temp.path().join("dpt");
    let (core, times) = recorded_scenario(&recording, Some(Scenario::Trade), 0);
    let mut root = ContinuousRoot::new(
        &temp.path().join("root"),
        &recording,
        &core,
        times,
        Scenario::Trade,
        0,
        "sample-schema",
    );
    let mut samples = vec![serde_json::json!({"phase":"start","sources":root.source_sample()})];
    let mut frames = 0;
    let mut phases = std::collections::BTreeSet::new();
    loop {
        next_recorded_group(&mut root.driver);
        let at = root
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        root.shared
            .advance_settlement(&mut root.coordinator, &mut root.driver)
            .unwrap();
        let group = root
            .native
            .consume(&mut root.shared, &mut root.driver)
            .unwrap();
        if let Some(group) = &group {
            root.check_inputs(group);
        }
        let retired = group.as_ref().is_some_and(|g| {
            g.post_frame_hidden_ops
                .iter()
                .any(|op| matches!(op, HiddenOp::Retire { .. }))
        });
        frames += group.as_ref().map_or(0, |g| g.rows.len());
        root.script().await;
        let mature = root
            .shared
            .ledger()
            .economic_read()
            .unwrap()
            .rebate_days()
            .any(|day| day.amounts.below_threshold_pusd != rational("0", "1"));
        for (phase, reached) in [
            ("128", frames >= 128),
            ("retire", retired),
            ("rebate-maturity", mature),
        ] {
            if reached && phases.insert(phase) {
                samples.push(serde_json::json!({"phase":phase,"frames":frames,"sources":root.source_sample()}));
            }
        }
        if at == END {
            break;
        }
    }
    assert_eq!(phases.len(), 3);
    root.assert_coverage();
    samples.push(serde_json::json!({"phase":"end-before-drain","frames":frames,"sources":root.source_sample()}));
    for sample in &samples {
        let value = &sample["sources"];
        for field in [
            "cutoff",
            "order_count",
            "commitment_count",
            "readable_order_source_reference_slots",
            "unique_order_source_positions",
            "source_watermark_ranges",
            "ledger",
        ] {
            assert!(value.get(field).is_some(), "{field}");
        }
        assert!(
            value["unique_order_source_positions"].as_u64().unwrap()
                <= value["readable_order_source_reference_slots"]
                    .as_u64()
                    .unwrap()
        );
    }
    root.driver.stop_and_drain().unwrap();
    drop(root.shared);
    drop(root.native);
    samples.push(serde_json::json!({"phase":"end-after-release","held_observation_roots":0}));
    let output = std::env::var("SAMPLE_SCHEMA_OUTPUT").unwrap();
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&serde_json::json!({"model_executed":false,"samples":samples}))
            .unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "原C2前史的N=2参数交接短检查；不提交探针意图"]
async fn continuous_n2_handoff_schema() {
    use std::sync::Arc;
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    let temp = tempfile::tempdir().unwrap();
    let recording = temp.path().join("dpt");
    let (core, times) = recorded_scenario(&recording, Some(Scenario::Trade), 0);
    let mut roots = (0..2)
        .map(|index| {
            ContinuousRoot::new(
                &temp.path().join(format!("root-{index}")),
                &recording,
                &core,
                times.clone(),
                Scenario::Trade,
                0,
                &format!("handoff-{index}"),
            )
        })
        .collect::<Vec<_>>();
    let mut seen = [false; 2];
    while !seen.iter().all(|v| *v) {
        let mut groups = Vec::new();
        let mut indices = Vec::new();
        for (index, root) in roots.iter_mut().enumerate() {
            next_recorded_group(&mut root.driver);
            let at = root
                .driver
                .decision()
                .unwrap()
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            assert!(at < FIRST + 1_000_000);
            root.shared
                .advance_settlement(&mut root.coordinator, &mut root.driver)
                .unwrap();
            if let Some(group) = root
                .native
                .consume(&mut root.shared, &mut root.driver)
                .unwrap()
            {
                groups.push(Arc::new(group));
                indices.push(index);
            }
            root.script().await;
        }
        let selected = groups
            .iter()
            .zip(&indices)
            .map(|(g, &i)| {
                !seen[i]
                    && g.cancel.len() == 2
                    && g.rows.iter().any(|r| r.role == InputRole::DecisionRequired)
            })
            .collect::<Vec<_>>();
        for (index, actor) in runtime.advance_selected(&groups, &selected) {
            let original = indices[index];
            assert!(!seen[original]);
            seen[original] = true;
            eprintln!(
                "n2_handoff_schema={}",
                serde_json::json!({"root":original,"submitted":false,"objects":groups[index].cancel.len(),"path":actor.path,"sources":roots[original].source_sample(),"runtime":runtime.sample()})
            );
        }
    }
    for root in roots {
        root.driver.stop_and_drain().unwrap();
    }
    runtime.release_observation();
    eprintln!("n2_handoff_released={}", runtime.sample());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "原BASELINE2失败组与下一真实形成反馈；需原录制/图"]
async fn continuous_late_formation_regression() {
    use contracts::ExecutionReadValueV1;
    use std::sync::Arc;
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let output = std::env::var("CONTINUOUS_OUTPUT").unwrap();
    std::fs::create_dir_all(&output).unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    warm_continuous_runtime(&mut runtime).await;
    runtime.statistics();
    let recording = Path::new(&std::env::var("CONTINUOUS_RECORDINGS").unwrap()).join("C1-0-0");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(recording.join("fixture.json")).unwrap()).unwrap();
    let times = TradingTimes(serde_json::from_value(metadata["delivery_times"].clone()).unwrap());
    let mut replay = data_pipeline_training::public_replay::PublicReplayV1::open(
        &recording.join("dpt"),
        128,
        1 << 20,
    )
    .unwrap();
    while replay.next_applied().unwrap().is_some() {}
    let core = replay.into_core();
    let mut root = ContinuousRoot::new(
        Path::new(&output),
        &recording.join("dpt"),
        &core,
        times,
        Scenario::Trade,
        0,
        "C1-0-0-0",
    );
    let mut n2_seen = false;
    let mut original_order = Vec::new();
    loop {
        next_recorded_group(&mut root.driver);
        let at = root
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        root.shared
            .advance_settlement(&mut root.coordinator, &mut root.driver)
            .unwrap();
        let group = root
            .native
            .consume(&mut root.shared, &mut root.driver)
            .unwrap()
            .unwrap();
        root.check_inputs(&group);
        root.script().await;
        let autonomous = at >= FIRST + 1_000_000;
        if autonomous {
            root.model_continuation_started = true;
        }
        if at == 87165000 || at == 87166000 {
            let read = group.action_before.as_ref().unwrap();
            let ExecutionReadValueV1::Known(objects) = read.cancel_objects() else {
                panic!("原完整对象")
            };
            assert_eq!(objects.len(), 4);
            assert_eq!(group.cancel.len(), 4);
            let ids = objects
                .iter()
                .filter_map(|object| match object {
                    shared_run::CancelObjectReadV2::Single { order } => {
                        Some(format!("{:?}", order.order_identity))
                    }
                    shared_run::CancelObjectReadV2::Paired { orders, .. } => {
                        assert_eq!(orders[0].origin.as_ref().unwrap().batch_item_ordinal, 0);
                        assert_eq!(orders[1].origin.as_ref().unwrap().batch_item_ordinal, 1);
                        None
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(ids.len(), 2);
            let second = read
                .orders()
                .find(|o| {
                    o.origin.as_ref().is_some_and(|o| {
                        o.intent.global_request_id() == "model-0-1379" && o.batch_item_ordinal == 1
                    })
                })
                .unwrap();
            assert!(second.accepted.is_some());
            assert_eq!(second.cancellable, ExecutionReadValueV1::Known(true));
            if at == 87165000 {
                assert!(second.formed.is_none());
                assert_eq!(&group.cancel[1][40..46], &[0., 0., 0., 0., 0., 1.]);
                original_order = ids;
            } else {
                assert!(second.formed.is_some());
                assert_eq!(ids, original_order);
                let formed_sequences = objects.iter().filter_map(|o| match o {
                    shared_run::CancelObjectReadV2::Single { order } => match order.formed.unwrap().record().source_reference().record {
                        contracts::execution_normalization::SimV2SourceRecordReferenceV1::OrderLifecycle { private_feedback_sequence, .. } => Some(private_feedback_sequence), _ => panic!("原形成引用"),
                    }, _ => None,
                }).collect::<Vec<_>>();
                assert!(formed_sequences[0] < formed_sequences[1]);
            }
            eprintln!(
                "late formation positive cutoff={at} objects={} formed={} source={}",
                group.cancel.len(),
                second.formed.is_some(),
                root.source_sample()
            );
        }
        let selected = autonomous
            || (!n2_seen
                && group.cancel.len() == 2
                && group
                    .rows
                    .iter()
                    .any(|r| r.role == InputRole::DecisionRequired));
        for (_, actor) in runtime.advance(&[Arc::new(group)], selected) {
            if !autonomous {
                n2_seen = true;
                continue;
            }
            // 到原失败组之前保持原自主路径；正例两截点只比较参数，不提交新意图干扰迟到形成。
            if at < 87165000 {
                actor
                    .execute(
                        &mut root.shared,
                        &mut root.driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: format!("model-0-{}", runtime.frames),
                            caller_correlation_id: None,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        root.driver.drain_measurement_observations();
        if at == 87166000 {
            break;
        }
    }
    assert!(n2_seen);
    root.driver.stop_and_drain().unwrap();
    runtime.release_observation();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "原贡献先于成交终局的真实三端有界回归；需原图/录制/永久证据目录"]
async fn continuous_contribution_before_success_regression() {
    use std::sync::Arc;
    let output = std::env::var("CONTINUOUS_OUTPUT").unwrap();
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    runtime.frames = 13601;
    let recording = Path::new(&std::env::var("CONTINUOUS_RECORDINGS").unwrap()).join("C1-1-0");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(recording.join("fixture.json")).unwrap()).unwrap();
    assert_eq!(metadata["offset"], 86400000);
    let times = TradingTimes(serde_json::from_value(metadata["delivery_times"].clone()).unwrap());
    let mut replay = data_pipeline_training::public_replay::PublicReplayV1::open(
        &recording.join("dpt"),
        128,
        1 << 20,
    )
    .unwrap();
    let mut expected =
        recorded_frame_expectations(&mut replay, &times, START + 86400000, 173733000);
    let core = replay.into_core();
    let mut root = ContinuousRoot::new(
        Path::new(&output),
        &recording.join("dpt"),
        &core,
        times,
        Scenario::Orphan,
        86400000,
        "C1-1-0-0",
    );
    let mut n2_seen = false;
    loop {
        next_recorded_group(&mut root.driver);
        let cutoff = root.driver.decision().unwrap().cutoff().clone();
        let at = cutoff.decision_time().logical_time_utc_ms();
        assert!(at <= 173733000, "禁止消费诊断上界之后组");

        let settled = root
            .shared
            .advance_settlement(&mut root.coordinator, &mut root.driver)
            .unwrap();
        assert!(settled, "原组事实必须完整应用: {cutoff:?}");
        let consumed = root
            .native
            .consume(&mut root.shared, &mut root.driver)
            .unwrap();
        let group = consumed.unwrap_or_else(|| panic!("原组必须完整消费: {cutoff:?}"));
        if at == 173732000 || at == 173733000 {
            assert_eq!(group.rows.len(), 1);
            assert_eq!(group.rows[0].role, InputRole::DecisionRequired);
            assert_eq!(group.rows[0].hidden_state_key.condition_id.0, condition(4));
            let read = root.shared.ledger().economic_read().unwrap();
            let target = |id: &execution_normalization::MemoryLogicalIdentityV1| matches!(&id.key,execution_normalization::MemoryIdentityKeyV1::Entity(execution_normalization::SourceNeutralEntityKeyV1::Trade{source_trade_id}) if source_trade_id=="sim-28");
            let success = read.successful_trades().any(|t| target(t.trade_identity));
            assert_eq!(success, at == 173733000);
            if !success {
                assert!(
                    read.paired_allocations()
                        .all(|p| !target(&p.source_trade_identity)
                            && p.contribution_refs.iter().flatten().all(|id| !target(id)))
                );
            }
            let day = read
                .rebate_days()
                .find(|d| {
                    d.kind == contracts::economics::RebateKindV1::Taker
                        && d.attribution_day_start_utc_ms == 172800000
                })
                .unwrap();
            let contributions = day
                .contributions
                .iter()
                .filter(|c| target(&c.trade_identity))
                .collect::<Vec<_>>();
            assert_eq!(contributions.len(), 1);
            assert_eq!(contributions[0].contribution_pusd.0, "0.0022528");
            assert_eq!(day.amounts.confirmed_unpaid_pusd, rational("0", "1"));
            assert_eq!(day.amounts.paid_pusd, rational("0", "1"));
            eprintln!(
                "contribution_before_success at={at} success={success} contribution_count={} amounts={:?} sources={}",
                contributions.len(),
                day.amounts,
                root.source_sample()
            );
        }
        let want = expected
            .remove(&cutoff.receipt().group().group_position)
            .unwrap();
        assert_eq!(
            group
                .rows
                .iter()
                .map(|r| (
                    r.role == InputRole::DecisionRequired,
                    r.hidden_state_key.condition_id.0.clone()
                ))
                .collect::<Vec<_>>(),
            want
        );
        root.check_inputs(&group);
        root.script().await;
        let autonomous = at >= FIRST + 86400000 + 1000000;
        if autonomous {
            root.model_continuation_started = true;
        }
        let selected = autonomous
            || (!n2_seen
                && group.cancel.len() == 2
                && group
                    .rows
                    .iter()
                    .any(|r| r.role == InputRole::DecisionRequired));
        let actors = runtime.advance_selected(&[Arc::new(group)], &[selected]);
        for (_, actor) in actors {
            if !autonomous {
                n2_seen = true;
                continue;
            }
            if at >= 173732000 {
                continue;
            }
            actor
                .execute(
                    &mut root.shared,
                    &mut root.driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("model-0-{}", runtime.frames),
                        caller_correlation_id: None,
                    },
                )
                .await
                .unwrap();
        }
        root.driver.drain_measurement_observations();
        if at == 173732000 {
            assert_eq!(runtime.frames, 13601 + 1456);
        }
        if at == 173733000 {
            assert_eq!(runtime.frames, 13601 + 1457);
            eprintln!(
                "contribution_regression_complete frames={} sources={}",
                runtime.frames,
                root.source_sample()
            );
            break;
        }
    }
    assert!(expected.is_empty());
    let sources = root.source_sample();
    root.driver.stop_and_drain().unwrap();
    eprintln!(
        "diagnostic_drained_already_accepted pre_drain_sources={} runtime={}",
        sources,
        runtime.sample()
    );
}

/// 原137/138现场的请求回放；不称为新版模型自主选择或正式连续矩阵。
#[tokio::test(flavor = "current_thread")]
#[ignore = "需要保留的原SIM/Ledger现场与原C1录制"]
async fn received_economics_pending_preserves_native_frame_and_frozen_read() {
    use contracts::*;
    use std::sync::Arc;
    let original = std::path::PathBuf::from(std::env::var("ORIGINAL_EXECUTION").unwrap());
    let output = std::path::PathBuf::from(std::env::var("CONTINUOUS_OUTPUT").unwrap());
    std::fs::create_dir_all(&output).unwrap();
    let recording =
        std::path::PathBuf::from(std::env::var("CONTINUOUS_RECORDINGS").unwrap()).join("C1-0-0");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(recording.join("fixture.json")).unwrap()).unwrap();
    let times = TradingTimes(serde_json::from_value(metadata["delivery_times"].clone()).unwrap());
    let mut replay = data_pipeline_training::public_replay::PublicReplayV1::open(
        &recording.join("dpt"),
        128,
        1 << 20,
    )
    .unwrap();
    while replay.next_applied().unwrap().is_some() {}
    let core = replay.into_core();
    let mut root = ContinuousRoot::new(
        &output,
        &recording.join("dpt"),
        &core,
        times,
        Scenario::Trade,
        0,
        "C1-0-1-0",
    );
    let mut requests: BTreeMap<u64, Vec<serde_json::Value>> = BTreeMap::new();
    for line in std::fs::read_to_string(original.join("sim.jsonl"))
        .unwrap()
        .lines()
    {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        if value["kind"] != "ACCEPTED_INPUTS" {
            continue;
        }
        for input in value["body"].as_array().unwrap() {
            if input["input"] != "PUBLIC_REQUEST" {
                continue;
            }
            let body = &input["body"];
            if body["request"]["request"]["context"]["global_request_id"]
                .as_str()
                .is_some_and(|s| s.starts_with("model-"))
            {
                requests
                    .entry(
                        body["cutoff"]["receipt"]["group"]["group_position"]
                            .as_u64()
                            .unwrap(),
                    )
                    .or_default()
                    .push(body["request"].clone());
            }
        }
    }
    let mut reservations = BTreeMap::new();
    for file in std::fs::read_dir(original.join("ledger")).unwrap() {
        let path = file.unwrap().path();
        if path.extension().is_none_or(|v| v != "jsonl") {
            continue;
        }
        for line in std::fs::read_to_string(path).unwrap().lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            let body = &value["payload"]["body"];
            if let Some(id) = body["transaction"]["source"]["LocalReservation"].as_str()
                && id.contains("model-")
            {
                let v = &body["reservations"]["requests"][id]["reservation"];
                reservations.insert(
                    id.to_owned(),
                    ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1(id.into()),
                        condition_id: serde_json::from_value(v["condition_id"].clone()).unwrap(),
                        requested_principal_pusd: serde_json::from_value(
                            v["principal_pusd"].clone(),
                        )
                        .unwrap(),
                        immediate_fee_reserve_pusd: serde_json::from_value(
                            v["immediate_fee_pusd"].clone(),
                        )
                        .unwrap(),
                    },
                );
            }
        }
    }
    fn leg(order: SimBuyOrderV1) -> BuyLegIntentV1 {
        BuyLegIntentV1 {
            token_id: order.token_id,
            order: match order.kind {
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share,
                    size_shares,
                    post_only,
                } => BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share,
                    size_shares,
                    post_only,
                },
                SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share,
                    size_shares,
                    post_only,
                    expires_at_utc_ms,
                } => {
                    assert_eq!(expires_at_utc_ms % 1000, 0);
                    BuyOrderIntentV1::GtdLimit {
                        limit_price_pusd_per_share,
                        size_shares,
                        post_only,
                        expires_at_utc_sec: expires_at_utc_ms / 1000,
                    }
                }
                SimBuyOrderKindV1::FokMarket {
                    spend_pusd,
                    worst_price_pusd_per_share,
                } => BuyOrderIntentV1::FokMarket {
                    spend_pusd,
                    worst_price_pusd_per_share,
                },
                SimBuyOrderKindV1::FakMarket {
                    spend_pusd,
                    worst_price_pusd_per_share,
                } => BuyOrderIntentV1::FakMarket {
                    spend_pusd,
                    worst_price_pusd_per_share,
                },
            },
        }
    }
    let mut frozen = None;
    let mut pending_prefix = None;
    let mut frames = 0;
    loop {
        next_recorded_group(&mut root.driver);
        let cutoff = root.driver.decision().unwrap().cutoff().clone();
        let at = cutoff.decision_time().logical_time_utc_ms();
        let advanced = root
            .shared
            .advance_settlement(&mut root.coordinator, &mut root.driver)
            .unwrap();
        let group = root
            .native
            .consume(&mut root.shared, &mut root.driver)
            .unwrap()
            .expect("经济等待不能隐去原合法组");
        frames += group.rows.len();
        root.check_inputs(&group);
        root.script().await;
        if at >= FIRST + 1_000_000 {
            root.model_continuation_started = true;
        }
        if at == 87349000 || at == 87350000 {
            let view = group.action_before.as_ref().unwrap();
            let pending = usize::from(at == 87349000);
            assert_eq!(view.pending_noe_record_count(), pending);
            assert_eq!(advanced, pending == 0);
            assert!(
                view.orders()
                    .any(|o| format!("{:?}", o.order_identity).contains("sim-46"))
            );
            let row = group
                .rows
                .iter()
                .find(|r| r.role == InputRole::DecisionRequired)
                .unwrap();
            assert_eq!(&row.slow[412..415], &[(1. + pending as f32).ln(), 1., 1.]);
            if pending == 1 {
                pending_prefix = Some(view.ledger().platform_source_prefix);
                assert!(view.orders().any(|o| {
                    o.origin.as_ref().is_some_and(|origin| {
                        origin.intent.global_request_id() == "model-0-6099"
                            && origin.batch_item_ordinal == 1
                    })
                }));
                assert_eq!(view.cutoff().receipt().group().group_position, 1610);
                assert_eq!(frames, 1481);
            } else {
                assert!(view.ledger().platform_source_prefix > pending_prefix.unwrap());
                for ordinal in [0, 1] {
                    assert!(view.orders().any(|o| {
                        o.accepted.is_some()
                            && o.origin.as_ref().is_some_and(|origin| {
                                origin.intent.global_request_id() == "model-0-6099"
                                    && origin.batch_item_ordinal == ordinal
                            })
                    }));
                }
                let old: &NativeInputGroup = frozen.as_ref().unwrap();
                assert_eq!(
                    old.action_before
                        .as_ref()
                        .unwrap()
                        .pending_noe_record_count(),
                    1
                );
                assert_eq!(
                    old.action_before
                        .as_ref()
                        .unwrap()
                        .ledger()
                        .platform_source_prefix,
                    pending_prefix.unwrap()
                );
                let before = root.shared.ledger().projection_v2().unwrap();
                assert!(root.shared.apply_closed().unwrap());
                assert_eq!(root.shared.ledger().projection_v2().unwrap(), before);
            }
            eprintln!(
                "received pending positive cutoff={at} frames={frames} pending={pending} source={}",
                root.source_sample()
            );
        }
        let position = cutoff.receipt().group().group_position;
        if let Some(original_requests) = requests.remove(&position) {
            for value in original_requests {
                let req = &value["request"];
                let context: RequestContextV2 =
                    serde_json::from_value(req["context"].clone()).unwrap();
                let orders: Vec<SimBuyOrderV1> = match value["method"].as_str().unwrap() {
                    "POST_ORDER" => {
                        vec![serde_json::from_value(req["item"]["order"].clone()).unwrap()]
                    }
                    "POST_ORDERS" => req["items"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            assert_eq!(v["batch_item_ordinal"], i);
                            serde_json::from_value(v["item"]["order"].clone()).unwrap()
                        })
                        .collect(),
                    _ => vec![],
                };
                let intent = if orders.is_empty() {
                    assert_eq!(value["method"], "CANCEL_MARKET_ORDERS");
                    ModelIntentV1::CancelMarketOrders {
                        condition_id: serde_json::from_value(req["condition_id"].clone()).unwrap(),
                    }
                } else {
                    let items = (0..orders.len())
                        .map(|i| LedgerSubmissionItemIdentityV1 {
                            global_request_id: context.global_request_id.clone(),
                            batch_item_ordinal: i as u16,
                        })
                        .collect::<Vec<_>>();
                    let reserves = items
                        .iter()
                        .map(|item| {
                            reservations
                                .remove(&serde_json::to_string(item).unwrap())
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    assert!(
                        root.shared
                            .reserve_action_v2(&reserves)
                            .unwrap()
                            .iter()
                            .all(|v| matches!(v, LedgerReservationOutcomeV1::Reserved { .. }))
                    );
                    for (reserve, item) in reserves.iter().zip(&items) {
                        root.shared
                            .record_submission_started(
                                &LedgerLocalEventIdentityV1(
                                    serde_json::to_string(&("submission-started", item)).unwrap(),
                                ),
                                &reserve.request_identity,
                                item,
                            )
                            .unwrap();
                    }
                    let condition_id = orders[0].condition_id.clone();
                    let mut legs = orders.into_iter().map(leg);
                    let first = legs.next().unwrap();
                    if let Some(second) = legs.next() {
                        assert!(legs.next().is_none());
                        ModelIntentV1::SubmitPairedBuy {
                            condition_id,
                            orders: [first, second],
                        }
                    } else {
                        ModelIntentV1::SubmitSingleBuy {
                            condition_id,
                            order: first,
                        }
                    }
                };
                root.driver.wait_for_recording_capacity().unwrap();
                root.shared
                    .execute(
                        &mut root.driver.decision().unwrap(),
                        context,
                        Some(Arc::new(intent)),
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
        if at == 87349000 {
            frozen = Some(group);
        }
        root.driver.drain_measurement_observations();
        if at == 87350000 {
            break;
        }
        assert!(at < 87350000);
    }
    assert!(requests.is_empty());
    root.driver.stop_and_drain().unwrap();
    let expected = std::fs::read_to_string(original.join("sim.jsonl")).unwrap();
    let actual = std::fs::read_to_string(output.join("sim.jsonl")).unwrap();
    let inputs = |text: &str| -> Vec<serde_json::Value> {
        text.lines()
            .flat_map(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                match value["kind"].as_str() {
                    Some("ACCEPTED_INPUTS") => value["body"].as_array().unwrap().clone(),
                    Some("ACCEPTED_INPUTS_V3") => {
                        value["body"]["inputs"].as_array().unwrap().clone()
                    }
                    _ => vec![],
                }
            })
            .collect()
    };
    let expected_inputs = inputs(&expected);
    let actual_inputs = inputs(&actual);
    assert!(
        actual_inputs.starts_with(&expected_inputs),
        "原输入/请求/身份/时间语义前缀保持；V4日志封装明确不同"
    );
    eprintln!(
        "replay_version_difference=RunOpened.projection_version V3->V4, AcceptedInputs->AcceptedInputsV3; prior_input_count={} current_input_count={}",
        expected_inputs.len(),
        actual_inputs.len()
    );
}
