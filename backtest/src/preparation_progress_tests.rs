use super::*;
use crate::test_common as common;
use crate::*;
use std::{cell::Cell, collections::BTreeMap, path::Path};
#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);
impl Write for Output {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct Broken;
impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("输出损坏"))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, p: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for e in std::fs::read_dir(p).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                visit(root, &p, out);
            } else {
                out.insert(
                    p.strip_prefix(root).unwrap().to_string_lossy().into_owned(),
                    std::fs::read(p).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
#[test]
fn phase_progress_preserves_full_material_statistics_and_reconciles_consecutive_intervals() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = common::setup(temp.path());
    common::initialize_at(&mut config, common::BASE + 1000, &[common::FIRST]);
    let off = prepare_historical_run_with_statistics_v1(&config.preparation, None).unwrap();
    let output = Output::default();
    config.preparation.work_root = temp.path().join("on");
    let hz = process_clock_ticks_per_second_v1();
    let before = ProcessCountersV1::read().unwrap();
    let began = Instant::now();
    let progress = HistoricalPreparationProgressV1::new(Duration::from_secs(30), output.clone());
    let measurements = progress.measurements();
    let on =
        prepare_historical_run_with_statistics_v1(&config.preparation, Some(progress)).unwrap();
    let after = ProcessCountersV1::read().unwrap();
    let combined = process_measurement_v1(
        Some(&before),
        Some(&after),
        hz,
        began.elapsed().as_secs_f64(),
    );
    assert_eq!(json!(off.statistics), json!(on.statistics));
    assert_eq!(json!(off.prepared.markets), json!(on.prepared.markets));
    assert_eq!(
        json!(off.prepared.initialization),
        json!(on.prepared.initialization)
    );
    assert_eq!(
        bytes(&off.prepared.public_root),
        bytes(&on.prepared.public_root)
    );
    assert_eq!(
        bytes(&temp.path().join("prepared/hidden")),
        bytes(&config.preparation.work_root.join("hidden"))
    );
    let result = measurements.snapshot(&combined);
    assert_eq!(result["status"], "complete");
    let expected = [
        "admission",
        "initialization_selection",
        "epoch_pass",
        "second_supply_and_initial_sort",
        "source_sort_finish",
        "public_application_statistics_recording",
        "public_stabilize_and_catalog",
        "hidden_scan_and_supply",
        "hidden_messages_finish",
        "hidden_reduce_and_write",
        "hidden_finalize",
    ];
    let phases = result["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 11);
    for (phase, expected) in phases.iter().zip(expected) {
        assert_eq!(phase["stage"], expected);
        assert_eq!(phase["completed"], true);
        assert_eq!(phase["measurement"]["available"], true);
    }
    let work = &result["work"];
    assert_eq!(work["admission_files_completed"], 2);
    assert_eq!(work["admission_footer_rows"], 8);
    assert_eq!(work["known_daily_raw_files_per_pass"], 2);
    assert_eq!(work["known_daily_raw_rows_per_pass"], 8);
    assert_eq!(work["known_initialization_raw_rows_per_pass"], 2);
    assert_eq!(work["known_total_public_inputs"], 9);
    for pass in ["epoch_pass", "second_supply"] {
        assert_eq!(work[pass]["files_completed"], 2);
        assert_eq!(work[pass]["decoded_batches"], 2);
        assert_eq!(work[pass]["physical_rows"], 8);
        assert_eq!(work[pass]["selected_rows"], 7);
        // 初始化已在selection阶段真实读取；后续pass只复用选中行，不虚报I/O。
        assert_eq!(work[pass]["initialization_files_completed"], 0);
        assert_eq!(work[pass]["initialization_decoded_batches"], 0);
        assert_eq!(work[pass]["initialization_physical_rows"], 0);
        assert_eq!(work[pass]["initialization_selected_rows"], 1);
    }
    assert_eq!(
        work["initialization_selection"]["initialization_physical_rows"],
        2
    );
    assert_eq!(
        work["source_sort_metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["input_rows"].as_u64().unwrap())
            .sum::<u64>(),
        8
    );
    assert_eq!(work["applied_inputs"], 9);
    assert_eq!(work["generated_groups"], 8);
    assert_eq!(work["hidden_messages_supplied"], 6);
    assert_eq!(work["hidden_messages_reduced"], 6);
    assert_eq!(work["hidden_message_sort"]["input_rows"], 6);
    assert_eq!(
        work["hidden_index_sort"]["input_rows"].as_u64().unwrap(),
        work["hidden_steps_indexed"].as_u64().unwrap()
            + work["hidden_references_indexed"].as_u64().unwrap()
    );
    let mut reader = data_pipeline_training::public_recording::PublicRecordingReaderV1::recover(
        &on.prepared.public_root,
        1,
        1 << 20,
    )
    .unwrap();
    let mut logical_records = 0_u64;
    while reader.next_row().unwrap().is_some() {
        logical_records += 1;
    }
    // 隐藏扫描读取紧凑物理行；公共读取会展开 GroupObservationRange。
    // 用原 Parquet footer 独立核对物理数量，不复用隐藏扫描的计数实现。
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let mut physical_records = 0_u64;
    for entry in std::fs::read_dir(&on.prepared.public_root).unwrap() {
        let segment = entry.unwrap().path();
        if !segment
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("stable-")
        {
            continue;
        }
        let manifest = segment.join("captured.parts.json");
        let parts = if manifest.exists() {
            serde_json::from_reader::<_, data_pipeline_training::SortedRun>(
                std::fs::File::open(manifest).unwrap(),
            )
            .unwrap()
            .parts
        } else {
            vec!["captured.parquet".into()]
        };
        for part in parts {
            let parquet =
                SerializedFileReader::new(std::fs::File::open(segment.join(part)).unwrap())
                    .unwrap();
            physical_records +=
                u64::try_from(parquet.metadata().file_metadata().num_rows()).unwrap();
        }
    }
    assert!(logical_records > physical_records, "夹具实际覆盖紧凑行展开");
    assert_eq!(work["hidden_records_read"], physical_records);
    let wall_sum = phases
        .iter()
        .map(|p| p["measurement"]["wall_seconds"].as_f64().unwrap())
        .sum::<f64>();
    assert!(
        (wall_sum + result["residual"]["wall_seconds"].as_f64().unwrap()
            - combined["wall_seconds"].as_f64().unwrap())
        .abs()
            < 1e-9
    );
    for key in ["user_seconds", "system_seconds"] {
        let sum = phases
            .iter()
            .map(|p| p["measurement"][key].as_f64().unwrap())
            .sum::<f64>();
        assert!(
            (sum + result["residual"][key].as_f64().unwrap() - combined[key].as_f64().unwrap())
                .abs()
                < 1e-9
        );
    }
    for (key, v) in combined["proc_io"].as_object().unwrap() {
        let sum = phases
            .iter()
            .map(|p| p["measurement"]["proc_io"][key].as_u64().unwrap() as i128)
            .sum::<i128>();
        assert_eq!(
            sum + result["residual"]["proc_io"][key].as_i64().unwrap() as i128,
            v.as_u64().unwrap() as i128
        );
    }
    let logs = output.0.lock().unwrap().clone();
    let logs = std::str::from_utf8(&logs)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        logs.iter()
            .take(11)
            .map(|l| l["stage"].as_str().unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(logs.last().unwrap()["stage"], "complete");
    assert!(
        logs.iter()
            .take(5)
            .all(|l| l["known_total_inputs"].is_null())
    );
    assert_eq!(logs[5]["known_total_inputs"], 9);
    assert_eq!(logs.last().unwrap()["applied_input_fraction"], 1.0);
    assert!(logs.iter().all(|l| l["eta_seconds"].is_null()));
}
thread_local! {static SAMPLES:Cell<usize>=const {Cell::new(0)};}
fn counted_reader() -> Result<ProcessCountersV1, MaterialError> {
    SAMPLES.with(|v| v.set(v.get() + 1));
    ProcessCountersV1::read()
}
fn unavailable() -> Result<ProcessCountersV1, MaterialError> {
    Err("计量不可用".into())
}
#[test]
fn zero_interval_does_not_sample_or_print_and_retains_original_results() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let output = Output::default();
    let mut progress = HistoricalPreparationProgressV1::new(Duration::ZERO, output.clone());
    progress.set_reader(counted_reader);
    let report = progress.measurements();
    SAMPLES.with(|v| v.set(0));
    let result =
        prepare_historical_run_with_statistics_v1(&config.preparation, Some(progress)).unwrap();
    assert_eq!(SAMPLES.with(Cell::get), 0);
    assert!(output.0.lock().unwrap().is_empty());
    assert_eq!(json!(result.statistics)["public_groups"], 8);
    assert_eq!(
        report.snapshot(&json!({"wall_seconds":1}))["status"],
        "disabled"
    );
    let disabled = report.snapshot(&json!({"wall_seconds":1}));
    assert!(disabled["phases"].is_null());
    assert!(disabled["work"].is_null());
    assert!(disabled["residual"].is_null());
}
#[test]
fn unavailable_metrics_and_bad_output_preserve_original_write_error_and_no_complete() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = common::setup(temp.path());
    config.preparation.recording.max_capture_output_bytes = 1;
    let original = prepare_historical_run_v1(&config.preparation)
        .err()
        .unwrap()
        .to_string();
    for broken in [false, true] {
        config.preparation.work_root =
            temp.path()
                .join(if broken { "broken" } else { "unavailable" });
        let output = Output::default();
        let sink: Box<dyn Write + Send> = if broken {
            Box::new(Broken)
        } else {
            Box::new(output.clone())
        };
        let mut progress = HistoricalPreparationProgressV1::new(Duration::from_secs(30), sink);
        progress.set_reader(unavailable);
        let report = progress.measurements();
        let error = prepare_historical_run_with_statistics_v1(&config.preparation, Some(progress))
            .err()
            .unwrap();
        assert_eq!(error.to_string(), original);
        let result = report.snapshot(&json!({"wall_seconds":1}));
        assert_eq!(result["status"], "error");
        assert_eq!(
            result["failed_stage"],
            "public_application_statistics_recording"
        );
        assert!(result["counter_sample_failures"].as_u64().unwrap() > 0);
        assert!(
            result["phases"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| !v.is_null())
                .all(|v| v["measurement"]["available"] == false)
        );
        let logs = output.0.lock().unwrap().clone();
        if !broken {
            let logs = std::str::from_utf8(&logs)
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str::<Value>(l).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(logs.last().unwrap()["stage"], "error");
            assert!(logs.iter().all(|l| l["published"] == false));
        }
    }
}
#[test]
fn admission_and_initialization_failures_report_original_stage_without_success() {
    for initial in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, _) = common::setup(temp.path());
        if initial {
            common::initialize_at(&mut config, common::BASE + 1000, &[common::FIRST]);
            config
                .preparation
                .metadata_initialization
                .as_mut()
                .unwrap()
                .markets[0]
                .snapshot_source_message_id = contracts::Sha256Hex("0".repeat(64));
        } else {
            config.preparation.scope.manifest_paths = vec![temp.path().join("missing.json")];
        }
        let output = Output::default();
        let progress =
            HistoricalPreparationProgressV1::new(Duration::from_secs(30), output.clone());
        let report = progress.measurements();
        let error = prepare_historical_run_with_statistics_v1(&config.preparation, Some(progress))
            .err()
            .unwrap()
            .to_string();
        assert_eq!(
            error,
            prepare_historical_run_v1(&config.preparation)
                .err()
                .unwrap()
                .to_string()
        );
        let result = report.snapshot(&json!({"wall_seconds":1}));
        assert_eq!(result["status"], "error");
        assert_eq!(
            result["failed_stage"],
            if initial {
                "initialization_selection"
            } else {
                "admission"
            }
        );
        assert!(result["phases"][if initial { 2 } else { 1 }].is_null());
    }
}
