use std::fs;
use std::path::{Path, PathBuf};

const README: &str = "data-collect/docs/training-collector-deploy-readme.md";
const HOT_DOC: &str = "data-collect/docs/paper-live-hot-path-integration.md";
const SYSTEMD: &str = "data-collect/deploy/systemd/data-collect-training-collector.service.example";
const EXPORT_SCRIPT: &str = "data-collect/scripts/export-release-artifacts.sh";

#[test]
fn training_deploy_readme_contains_binary_only_remote_flow() {
    let readme = read(README);

    for expected in [
        "cd /home/chillchill/btc-updown-marketmaker",
        "data-collect/scripts/export-release-artifacts.sh",
        "data-collect-training-collector",
        "data-collect-paper-hot",
        "data-collect-live-hot",
        "sha256sum -c SHA256SUMS",
        "mkdir -p ~/btc-updown-marketmaker/data-collect/releases",
        "~/btc-updown-marketmaker/datasets",
        "--max-runtime-secs 900",
        "systemctl --user show",
        "systemctl --user is-active --quiet",
        "binary sha256",
        "git commit",
        "config digest",
        "dataset root canonical path",
        "collector host",
        "collector region",
        "binary role",
        "不再检查 source_coverage.json",
        "不再检查 source_gaps.json",
        "不再检查 rollover_edges.json",
        "不得生成 write_receipts.jsonl",
        "不得生成 queue_samples.jsonl",
        "不得生成 retention_after_finalize_",
        "source_writer_stats",
        "shutdown_stage_timings",
        "shutdown_retention_scan_budget_ms",
        "retention_shutdown_scan.status=skipped",
        "run_manifest_atomic_write",
        "不得 enable --now",
        "paper/live 服务器不得运行 cargo",
        "live 小主机不得承担训练重处理",
    ] {
        assert!(readme.contains(expected), "README missing {expected}");
    }

    assert!(!readme.contains("/home/ubuntu/btc-updown-marketmaker"));
}

#[test]
fn systemd_example_uses_server_local_home_paths_and_live_mode() {
    let systemd = read(SYSTEMD);

    assert!(systemd.contains("WorkingDirectory=%h/btc-updown-marketmaker"));
    assert!(systemd.contains("DATA_COLLECT_RUNTIME_MODE=live"));
    assert!(systemd.contains(
        "%h/btc-updown-marketmaker/data-collect/current/bin/data-collect-training-collector"
    ));
    assert!(systemd.contains("--dataset-root %h/btc-updown-marketmaker/datasets"));
    assert!(systemd.contains("--collector-region eu-west-1"));
    assert!(!systemd.contains("/home/chillchill/btc-updown-marketmaker"));
}

#[test]
fn export_script_packages_binaries_docs_service_and_sha256s() {
    let script = read(EXPORT_SCRIPT);

    for expected in [
        "cargo build -p data-collect --release",
        "--bin data-collect-training-collector",
        "--bin data-collect-paper-hot",
        "--bin data-collect-live-hot",
        "bin/data-collect-training-collector",
        "bin/data-collect-paper-hot",
        "bin/data-collect-live-hot",
        "deploy/systemd/data-collect-training-collector.service.example",
        "docs/training-collector-deploy-readme.md",
        "docs/paper-live-hot-path-integration.md",
        "RELEASE_INFO.env",
        "SHA256SUMS",
    ] {
        assert!(script.contains(expected), "script missing {expected}");
    }
}

#[test]
fn paper_live_hot_path_doc_states_non_deployment_and_runtime_boundary() {
    let doc = read(HOT_DOC);

    for expected in [
        "本阶段不自动部署",
        "data-collect-paper-hot",
        "data-collect-live-hot",
        "runtime_receive_time_utc_ms",
        "paper/data_collect_handoff/hot.sock",
        "live/data_collect_handoff/hot.sock",
        "source_gap",
        "backpressure_state",
        "不接管交易逻辑",
        "NOE/ledger",
        "execution feedback",
        "当前阶段只验证 frame contract、heartbeat/status server 和 fake consumer",
        "DATA_COLLECT_RUNTIME_MODE=live",
        "data-collect-paper-hot --dataset-root ~/btc-updown-marketmaker/datasets",
        "data-collect-live-hot --dataset-root ~/btc-updown-marketmaker/datasets",
    ] {
        assert!(doc.contains(expected), "hot path doc missing {expected}");
    }

    assert!(!doc.contains("当前阶段已验证真实 paper/live external_market_event"));
}

fn read(path: &str) -> String {
    let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path_from_workspace = package_root.parent().unwrap().join(path);
    let path_from_package = package_root.join(path.strip_prefix("data-collect/").unwrap_or(path));
    let resolved = if path_from_workspace.is_file() {
        path_from_workspace
    } else {
        path_from_package
    };

    assert!(Path::new(&resolved).is_file(), "{path} must exist");
    fs::read_to_string(resolved).unwrap()
}
