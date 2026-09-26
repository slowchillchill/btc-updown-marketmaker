//! #166：真实 typed_raw→公共归约→SIM/NOE/Ledger→原生状态行的有限深盘口。
use crate::{
    public_input_tests::{common::*, exported_group},
    *,
};
use contracts::*;
use data_pipeline_core::{public_input::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use ledger::*;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};

fn pm_book(token: &str, bids: usize, asks: usize) -> PolymarketOrderbookEvent {
    let side = |count: usize, bid: bool| {
        (0..count)
            .map(|i| PolymarketPriceLevel {
                price: rust_decimal::Decimal::new(
                    if bid {
                        5000 - i as i64
                    } else {
                        6000 + i as i64
                    },
                    4,
                )
                .to_string(),
                size: (i + 1).to_string(),
            })
            .collect()
    };
    PolymarketOrderbookEvent::Book {
        asset_id: token.into(),
        bids: side(bids, true),
        asks: side(asks, false),
        last_trade_price: None,
    }
}

fn full_book_root() -> (
    tempfile::TempDir,
    SharedExecutionV2,
    NativeInputRoot,
    FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<MemoryRecordedTimes>>,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut active)),
        ..
    } = meta(0).ingress
    else {
        unreachable!()
    };
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let depth = BinanceDepthSnapshotRow {
        timestamp_received: 13_000,
        snapshot_fetch_start_utc: 12_900,
        last_update_id: 100,
        bids: (0..8)
            .map(|i| DepthLevel {
                price: format!("{}", 99999 - i),
                quantity: format!("{}", i + 1),
            })
            .collect(),
        asks: (0..7)
            .map(|i| DepthLevel {
                price: rust_decimal::Decimal::new(10_000_000 + i, 2).to_string(),
                quantity: format!("{}", i + 1),
            })
            .collect(),
        symbol: "BTCUSDT".into(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000".into(),
        snapshot_status: "complete".into(),
        source_sequence_epoch_id: "depth".into(),
        connection_generation: 1,
        depth_ingress_sequence: 1,
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: 13_001,
        collector_host_id: "collector-1".into(),
        collector_region: "eu-west-1".into(),
        schema_version: "binance_depth_snapshot_v3".into(),
    };
    let inputs = vec![
        meta(0),
        pm(1, 1, 11_000, pm_book(UP_TOKEN_ID, 257, 7)),
        pm(2, 2, 12_000, pm_book(DOWN_TOKEN_ID, 9, 6)),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(depth)),
        ),
        record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(active)),
        ),
        pm(5, 3, 21_000, pm_book(UP_TOKEN_ID, 7, 1)),
        pm(6, 4, 22_000, pm_book(UP_TOKEN_ID, 0, 0)),
        pm(7, 5, 23_000, pm_book(UP_TOKEN_ID, 257, 7)),
        record(
            8,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
        record(
            9,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        RecordedIngressV1 {
            input_stream_id: "core-test".into(),
            input_position: 10,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        },
    ];
    for input in inputs {
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    drop(writer);
    let (mut execution, observation, snapshot) = execution_with_rules_v4(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let native = NativeInputRoot::new("full-book".into(), account, -10_000, snapshot);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let driver = FinitePublicConsumptionV1::new(execution, source, "full-book".into());
    (temp, shared, native, driver)
}

#[test]
fn full_book_public_sequence_retains_all_levels_and_hidden_lifecycle() {
    let (_temp, mut shared, mut native, mut driver) = full_book_root();
    let mut exported = Vec::new();
    let mut timings = Vec::new();
    while driver.next_group().unwrap().is_some() {
        let before = std::time::Instant::now();
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        let elapsed = before.elapsed().as_secs_f64() * 1000.;
        timings.push(serde_json::json!({"consume_ms":elapsed,"rows":group.rows.iter().map(|r|r.book_levels.each_ref().map(Vec::len)).collect::<Vec<_>>()}));
        exported.push(exported_group(&group));
    }
    // 原生导出用于Python三端/混批验证和人工审查；同一原公共材料可由v4基线独立重放。
    if let Some(path) = std::env::var_os("NATIVE_BOOK_FIXTURE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec(&exported).unwrap()).unwrap();
    }
    if let Some(path) = std::env::var_os("NATIVE_BOOK_TIMING_OUTPUT") {
        std::fs::write(path, serde_json::to_vec(&timings).unwrap()).unwrap();
    }
    let rows: Vec<_> = exported
        .iter()
        .flat_map(|g| g["rows"].as_array().unwrap())
        .collect();
    assert!(rows.iter().any(|r| r["role"] == "ContextOnly"));
    assert!(rows.iter().any(|r| r["role"] == "DecisionRequired"));
    assert!(
        exported
            .iter()
            .any(|g| !g["post_frame_hidden_ops"].as_array().unwrap().is_empty())
    );
    let complete = rows
        .iter()
        .find(|r| {
            r["book_levels"][0].as_array().unwrap().len() == 257
                && r["book_levels"][5].as_array().unwrap().len() == 7
        })
        .unwrap();
    let books: BookLevels = serde_json::from_value(complete["book_levels"].clone()).unwrap();
    assert_eq!(books.each_ref().map(Vec::len), [257, 7, 9, 6, 8, 7]);
    let (depth, values, mask) = pad_books([&books]);
    assert_eq!(depth, 257);
    assert_eq!(mask.iter().filter(|v| **v).count(), 294);
    assert_eq!(&values[256 * 9..257 * 9], &books[0][256]);
    // 0.5000−0.4744先精确相减，PM深档有直接事实时间；BN无逐档时间。
    assert_eq!(
        books[0][256][8],
        crate::exact::signed_log_f32(&crate::exact::ratio(256, 10000))
    );
    assert_eq!(&books[0][256][6..8], &[1., 1.]);
    assert_eq!(&books[5][1][3..8], &[0.; 5]);
    assert_eq!(books[5][0][0], books[5][1][0]);
    assert_ne!(books[5][0][8], books[5][1][8]);
    assert!(
        rows.iter()
            .any(|r| r["book_levels"][0].as_array().unwrap().is_empty() && r["fast"][43] == 1.)
    );
    assert!(
        rows.iter()
            .any(|r| r["book_levels"][0].as_array().unwrap().is_empty() && r["fast"][43] == 0.)
    );
}

#[cfg(feature = "continuous-validation")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "有限深盘口三端和原执行入口；需新版本两图"]
async fn full_book_three_runtime_and_original_execution() {
    let directory = std::env::var("ACTOR_GRAPH_DIRECTORY").unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    let (_temp, mut shared, mut native, mut driver) = full_book_root();
    let mut lengths = Vec::new();
    let mut contexts = 0;
    let mut retired = 0;
    let mut executed = 0;
    while driver.next_group().unwrap().is_some() {
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        for row in &group.rows {
            lengths.push(row.book_levels.each_ref().map(Vec::len));
            contexts += usize::from(row.role == InputRole::ContextOnly);
        }
        retired += group.post_frame_hidden_ops.len();
        for (_, actor) in runtime.advance(&[std::sync::Arc::new(group)], true) {
            driver.wait_for_recording_capacity().unwrap();
            actor
                .execute(
                    &mut shared,
                    &mut driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("full-book-{}", runtime.frames),
                        caller_correlation_id: None,
                    },
                )
                .await
                .unwrap();
            executed += 1;
        }
    }
    driver.stop_and_drain().unwrap();
    assert!(
        contexts > 0 && retired > 0 && executed > 1,
        "contexts={contexts} retired={retired} executed={executed}"
    );
    assert_eq!(runtime.frames, lengths.len());
    assert_eq!(runtime.decisions, executed);
    for required in [[257, 7, 9, 6, 8, 7], [7, 1, 9, 6, 8, 7], [0, 0, 9, 6, 8, 7]] {
        assert!(lengths.contains(&required));
    }
    let sample = runtime.sample();
    assert_eq!(sample["native_hidden_rows"], 0);
    assert_eq!(sample["hidden_rows"], serde_json::json!([0, 0]));
    let evidence = serde_json::json!({
        "input_contract_version": "native_inputs_v5", "frames": runtime.frames,
        "context_frames": contexts, "decisions_executed": executed,
        "retire_operations": retired, "side_lengths": lengths,
        "provenance": runtime.provenance(), "final_sample": sample,
    });
    if let Some(path) = std::env::var_os("NATIVE_DEEP_EXECUTION_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
    eprintln!(
        "full_book_three_runtime frames={} decisions={} retired={}",
        runtime.frames, executed, retired
    );
}
