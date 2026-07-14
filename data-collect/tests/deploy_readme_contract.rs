use std::fs;
use std::path::{Path, PathBuf};

const README: &str = "data-collect/docs/training-collector-deploy-readme.md";
const HOT_DOC: &str = "data-collect/docs/paper-live-hot-path-integration.md";
const SYSTEMD: &str = "data-collect/deploy/systemd/data-collect-training-collector.service";

#[test]
fn training_deploy_readme_describes_single_binary_single_unit_flow() {
    let readme = read(README);

    for expected in [
        "cargo build --release --locked -p data-collect --bin data-collect-training-collector",
        "target/release/data-collect-training-collector",
        "/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector",
        "/home/ubuntu/.config/systemd/user/data-collect-training-collector.service",
        "data-collect/deploy/systemd/data-collect-training-collector.service",
        "--dataset-root /home/ubuntu/btc-updown-marketmaker/datasets",
        "--collector-region eu-west-1",
        "--run-forever",
        "--max-dataset-bytes 50000000000",
        "Restart=on-failure",
        "RestartPreventExitStatus=75 76",
        "StartLimitIntervalSec=600",
        "StartLimitBurst=5",
        "Restart=no",
        "dataset_capacity_reached",
        "--help",
        "--version",
        "每个 UTC 日只能有一个 `manifest.json`",
        "data-pipeline 只消费 `closed` 日",
        "*.running",
        "manifest.json.tmp",
        "不生成独立报告",
        "远端服务器禁止编译 Rust",
        "paper/live 源码 binary 仍可分别",
        "failed_gate",
        "failed_long_running",
        "partial_replace",
    ] {
        assert!(readme.contains(expected), "README missing {expected}");
    }

    for forbidden in [
        "export-release-artifacts.sh",
        "train-data-collect-ver1-0",
        "data-collect-retention-admin",
        "SHA256SUMS",
        "RELEASE_INFO.env",
        "service.example",
        "retention receipt",
        "run_manifests/<run_id>",
        "data-collect/current",
    ] {
        assert!(
            !readme.contains(forbidden),
            "README retains obsolete deployment path {forbidden}"
        );
    }
}

#[test]
fn repository_has_one_authoritative_training_unit() {
    let systemd = read(SYSTEMD);
    let expected_exec_start = "ExecStart=/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector --dataset-root /home/ubuntu/btc-updown-marketmaker/datasets --collector-region eu-west-1 --run-forever --max-dataset-bytes 50000000000";

    assert!(systemd.contains(expected_exec_start));
    for expected in [
        "Restart=on-failure",
        "RestartSec=10",
        "RestartPreventExitStatus=75 76",
        "KillSignal=SIGTERM",
        "TimeoutStopSec=120",
        "StartLimitIntervalSec=600",
        "StartLimitBurst=5",
        "StandardOutput=journal",
        "StandardError=journal",
    ] {
        assert!(systemd.contains(expected), "unit missing {expected}");
    }
    for forbidden in [
        "Restart=always",
        "current/",
        "retention",
        "run-id",
        ".service.example",
    ] {
        assert!(
            !systemd.contains(forbidden),
            "unit retains obsolete setting {forbidden}"
        );
    }

    let unit_dir = resolve("data-collect/deploy/systemd");
    let mut files = fs::read_dir(unit_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    files.sort();
    assert_eq!(files, vec!["data-collect-training-collector.service"]);
}

#[test]
fn obsolete_release_retention_and_archive_entrypoints_are_absent() {
    for obsolete in [
        "data-collect/scripts/export-release-artifacts.sh",
        "data-collect/scripts/run_daily_new_datasets_archive.sh",
        "data-collect/tests/test_daily_new_datasets_archive.py",
        "data-collect/src/bin/data_collect_retention_admin.rs",
        "data-collect/src/retention.rs",
        "data-collect/src/retention_admin.rs",
    ] {
        assert!(
            !resolve(obsolete).exists(),
            "obsolete path remains: {obsolete}"
        );
    }
}

#[test]
fn paper_live_hot_path_doc_preserves_independent_runtime_boundary() {
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
        "只消费 `state=closed` 日期",
        "不使用独立 sidecar 或 receipt",
    ] {
        assert!(doc.contains(expected), "hot path doc missing {expected}");
    }

    assert!(!doc.contains("当前阶段已验证真实 paper/live external_market_event"));
    assert!(!doc.contains("retention receipt"));
}

fn resolve(path: &str) -> PathBuf {
    let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path_from_workspace = package_root.parent().unwrap().join(path);
    if path_from_workspace.exists() {
        path_from_workspace
    } else {
        package_root.join(path.strip_prefix("data-collect/").unwrap_or(path))
    }
}

fn read(path: &str) -> String {
    let resolved = resolve(path);
    assert!(Path::new(&resolved).is_file(), "{path} must exist");
    fs::read_to_string(resolved).unwrap()
}
