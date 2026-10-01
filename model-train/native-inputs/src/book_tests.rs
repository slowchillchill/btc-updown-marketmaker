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

fn full_book_root(
    trajectory: &str,
) -> (
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
    let native = NativeInputRoot::new(trajectory.into(), account, -10_000, snapshot);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let driver = FinitePublicConsumptionV1::new(execution, source, "full-book".into());
    (temp, shared, native, driver)
}

#[test]
fn full_book_public_sequence_retains_all_levels_and_hidden_lifecycle() {
    let (_temp, mut shared, mut native, mut driver) = full_book_root("full-book");
    let mut exported = Vec::new();
    let mut timings = Vec::new();
    while driver.next_group().unwrap().is_some() {
        let before = std::time::Instant::now();
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        let elapsed = before.elapsed().as_secs_f64() * 1000.;
        timings.push(serde_json::json!({"consume_ms":elapsed,"rows":group.rows.iter().map(|r|r.book_levels.each_ref().map(|side| side.len())).collect::<Vec<_>>()}));
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
    assert_eq!(
        books.each_ref().map(|side| side.len()),
        [257, 7, 9, 6, 8, 7]
    );
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
#[ignore = "有限深盘口三端和原执行入口；需新版本八图"]
async fn full_book_three_runtime_and_original_execution() {
    let directory = std::env::var("ACTOR_GRAPH_DIRECTORY").unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    let (_temp, mut shared, mut native, mut driver) = full_book_root("full-book");
    let mut lengths = Vec::new();
    let mut contexts = 0;
    let mut retired = 0;
    let mut executed = 0;
    while driver.next_group().unwrap().is_some() {
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        for row in &group.rows {
            lengths.push(row.book_levels.each_ref().map(|side| side.len()));
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
        "input_contract_version": "native_inputs_v7", "account_economics_version": shared_run::ACCOUNT_ECONOMICS_VERSION, "frames": runtime.frames,
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

#[test]
fn retained_groups_actors_and_packing_survive_remap_depth_changes_and_retirement() {
    use std::sync::Arc;
    let (_temp, mut shared, mut native, mut driver) = full_book_root("retained-a");
    let (_other_temp, mut other_shared, mut other_native, mut other_driver) =
        full_book_root("retained-b");
    let mut packed = PackedBooks::default();
    let mut saved = Vec::new();
    let mut actors = Vec::new();
    let mut weak = Vec::new();
    let mut retired = 0;
    while driver.next_group().unwrap().is_some() {
        other_driver.next_group().unwrap().unwrap();
        let a = Arc::new(native.consume(&mut shared, &mut driver).unwrap().unwrap());
        let b = Arc::new(
            other_native
                .consume(&mut other_shared, &mut other_driver)
                .unwrap()
                .unwrap(),
        );
        for group in [&a, &b] {
            retired += group.post_frame_hidden_ops.len();
            for (i, row) in group.rows.iter().enumerate() {
                weak.extend(
                    row.book_levels
                        .iter()
                        .filter(|v| !v.is_empty())
                        .map(Arc::downgrade),
                );
                if row.role == InputRole::DecisionRequired {
                    let actor = NativeActor::new(group.clone(), i).unwrap();
                    actors.push((actor.support().unwrap(), actor.prefix_features(), actor));
                }
            }
        }
        // 从两根的有效行中选取不同活动人口，并反转打包根映射。
        let rows = a
            .rows
            .iter()
            .chain(b.rows.iter().skip(usize::from(saved.len() % 2 == 0)))
            .collect::<Vec<_>>();
        for active in [rows.clone(), rows.into_iter().rev().collect()] {
            packed.update(
                active
                    .iter()
                    .map(|row| (&row.hidden_state_key, &row.book_levels)),
            );
            let (depth, values, mask) = pad_books(active.iter().map(|row| &row.book_levels));
            assert_eq!(packed.depth(), depth);
            assert_eq!(
                packed
                    .values()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
                values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            );
            assert_eq!(packed.mask(), mask);
        }
        saved.push((exported_group(&a), a));
        saved.push((exported_group(&b), b));
    }
    assert!(retired > 0 && !actors.is_empty());
    for (expected, group) in &saved {
        assert_eq!(&exported_group(group), expected);
    }
    for (support, prefix, actor) in &actors {
        assert_eq!(&actor.support().unwrap(), support);
        assert_eq!(&actor.prefix_features(), prefix);
    }
    let held = saved
        .iter()
        .flat_map(|(_, group)| &group.rows)
        .flat_map(|row| &row.book_levels)
        .map(|side| {
            (
                Arc::as_ptr(side),
                side.capacity() * std::mem::size_of::<[f32; 9]>(),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    eprintln!(
        "retained_ownership groups={} actors={} projected_versions={} projected_capacity_bytes={}",
        saved.len(),
        actors.len(),
        held.len(),
        held.values().sum::<usize>()
    );
    packed.clear();
    drop(saved);
    drop(actors);
    assert!(weak.iter().all(|side| side.upgrade().is_none()));
}

#[cfg(feature = "python")]
#[test]
#[ignore = "需锁定 Python 环境及 model-train 导入路径；真实共享输入与延后反传"]
fn native_shared_inputs_preserve_delayed_backward_and_detached_aliases() {
    use pyo3::prelude::*;
    use std::sync::Arc;
    let mut streams = Vec::new();
    for trajectory in ["backward-a", "backward-b"] {
        let (_temp, mut shared, mut native, mut driver) = full_book_root(trajectory);
        let mut groups = Vec::new();
        while driver.next_group().unwrap().is_some() {
            let group = Arc::new(native.consume(&mut shared, &mut driver).unwrap().unwrap());
            groups.push(Python::attach(|py| {
                Py::new(py, crate::python::PythonGroup { group }).unwrap()
            }));
        }
        streams.push(groups);
    }
    Python::attach(|py| {
        let results = py
            .import("model_train.input_validation")
            .unwrap()
            .getattr("retained_inputs")
            .unwrap()
            .call1((streams,))
            .unwrap();
        let result = py
            .import("json")
            .unwrap()
            .call_method1("dumps", (results,))
            .unwrap()
            .extract::<String>()
            .unwrap();
        if let Some(path) = std::env::var_os("NATIVE_INPUT_OWNERSHIP_OUTPUT") {
            std::fs::write(path, result.as_bytes()).unwrap();
        }
        eprintln!("{result}");
    });
}
