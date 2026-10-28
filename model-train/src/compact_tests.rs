//! #377 stdlib假worker，不装载Torch/模型，不消耗模型短槽。
use crate::encoding::{EncodingConfig, Token};
use crate::worker::{ModelWorker, PerformanceConfig, WorkerConfig};
use serde_json::json;
use std::path::Path;

fn worker(queue_bytes: u64) -> ModelWorker {
    ModelWorker::start(WorkerConfig {
        python: "python3".into(),
        script: Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compact-worker.py"),
        model: "unused".into(),
        device: "cpu".into(),
        torch_threads: 1,
        max_batch: 2,
        encoding: EncodingConfig {
            field_buckets: 4096,
            category_buckets: 4096,
        },
        performance: Some(PerformanceConfig {
            mode: "storage_v1".into(),
            native_library: "unused".into(),
            rust_threads: 16,
            host_cache_bytes: 1024,
            gpu_cache_bytes: 0,
            runtime_reserve_bytes: 0,
            streaming_block_bytes: 256,
            queue_bytes,
            torch_interop_threads: 1,
        }),
    })
    .unwrap()
}

#[test]
fn compact_protocol_preserves_bits_and_large_frame_over_queue_target() {
    let worker = worker(8);
    let response = worker
        .request_frame(
            json!({"phase":"inspect"}),
            vec![
                Token(u32::MAX, 0x80000001, -0.0, 3),
                Token(1, 2, f32::from_bits(0x3eaaaaab), 0),
            ],
            vec![vec![Token(9, 0, 1.0, 2)], vec![]],
        )
        .unwrap();
    assert_eq!(
        response,
        json!({"fields":[4294967295u64,1],"categories":[2147483649u64,2],"numeric_bits":[2147483648u64,1051372203],"kinds":[3,0],"market_lengths":[1,0]})
    );
    worker.shutdown().unwrap();
    assert!(
        worker.io_metrics()["retained_queue_peak_bytes"]
            .as_u64()
            .unwrap()
            > 8
    );
    assert!(worker.request(json!({"phase":"inspect"})).is_err());
    worker.shutdown().unwrap();
}

#[test]
fn independent_cancel_reaches_worker_while_request_waits_and_exit_is_waited() {
    let worker = worker(1024);
    let request_worker = worker.clone();
    let request =
        std::thread::spawn(move || request_worker.request(json!({"phase":"wait_cancel"})));
    // 写入可以早于worker读取；控制FD独立于被阻塞的请求管道。
    worker.cancel().unwrap();
    assert_eq!(request.join().unwrap().unwrap(), json!({"stopped":true}));
    worker.shutdown().unwrap();
}

#[test]
fn controlled_stop_keeps_original_error_envelope_without_relaxing_normal_requests() {
    let worker = worker(1024);
    assert!(worker.request(json!({"phase":"stopped_error"})).is_err());
    let stopped = worker
        .request_controlled_stop(json!({"phase":"stopped_error"}))
        .unwrap();
    assert_eq!(stopped["stopped_before_commit"], true);
    assert!(
        worker
            .request_controlled_stop(json!({"phase":"unknown"}))
            .is_err()
    );
    worker.shutdown().unwrap();
}

#[test]
fn production_cancel_control_stops_on_byte_and_controller_eof_without_torch() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let result = std::process::Command::new("python3")
        .arg(root.join("tests/fixtures/cancel-control-check.py"))
        .arg(root.join("python/model_worker.py"))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn buffer_observer_schema_and_alias_boundary_use_no_torch() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let result = std::process::Command::new("python3")
        .arg(root.join("tests/fixtures/buffer-observer-check.py"))
        .arg(root.join("python/performance_capture.py"))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
