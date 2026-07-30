use std::fs;
use std::path::{Path, PathBuf};

const OPERATIONS: &str = "data-collect/docs/current/operations.md";
const INTERFACES: &str = "data-collect/docs/current/interfaces.md";
const LIMITATIONS: &str = "data-collect/docs/current/known-limitations.md";
const CONTRACTS: &str = "data-collect/docs/current/contracts.md";
const ACCEPTANCE: &str = "data-collect/docs/current/acceptance.md";
const MASTER_DEMAND: &str = "demand/data-collect-demand.md";
const DEPLOY_ROUTE: &str = "data-collect/docs/training-collector-deploy-readme.md";
const HOT_ROUTE: &str = "data-collect/docs/paper-live-hot-path-integration.md";
const SYSTEMD: &str = "data-collect/deploy/systemd/data-collect-training-collector.service";

#[test]
fn current_operations_describes_single_binary_single_unit_flow() {
    let operations = read(OPERATIONS);

    for expected in [
        "生命周期：`CURRENT_CONTRACT`",
        "执行授权：`none`",
        "clean、已 push 的完整 commit",
        "DATA_COLLECT_GIT_COMMIT=<完整 commit>",
        "服务器都不是 Rust 构建机",
        "/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector",
        "/home/ubuntu/btc-updown-marketmaker/datasets",
        "/home/ubuntu/.config/systemd/user/data-collect-training-collector.service",
        "data-collect/deploy/systemd/data-collect-training-collector.service",
        "Restart=on-failure",
        "RestartPreventExitStatus=75 76",
        "StartLimitIntervalSec=600",
        "StartLimitBurst=5",
        "## 3. Training 退出与容量",
        "`75/76` 映射只属于 training binary",
        "启动扫描已经达到上限",
        "不启动 source、不创建 typed raw/manifest/runtime",
        "运行期间达到上限",
        "drain/finalize 已接纳数据",
        "collector 不自动清理 finalized raw",
        "另行批准的部署或启动已经发生后",
        "本文件不授权停服或清理",
        "`.parquet.running` 不作为 finalized 数据读取",
        "不是当前部署路径",
        "另行批准的活动需求、计划与 changelog",
    ] {
        assert!(
            operations.contains(expected),
            "current operations missing {expected}"
        );
    }

    for forbidden in [
        "export-release-artifacts.sh",
        "train-data-collect-ver1-0",
        "data-collect-retention-admin",
        "## 任意时刻直接部署",
        "systemctl --user enable --now data-collect-training-collector.service",
        "preflight 通过后",
        "rm -rf",
    ] {
        assert!(
            !operations.contains(forbidden),
            "current operations retains obsolete deployment path {forbidden}"
        );
    }
}

#[test]
fn compatibility_docs_are_route_only_and_cannot_authorize_operations() {
    for (path, owner) in [(DEPLOY_ROUTE, OPERATIONS), (HOT_ROUTE, INTERFACES)] {
        let doc = read(path);
        assert!(doc.contains("生命周期：`ROUTE_ONLY`"));
        assert!(doc.contains("当前执行 authority：`none`"));
        assert!(doc.contains(owner));
        assert!(!doc.contains("systemctl --user enable --now"));
        assert!(!doc.contains("执行授权：`self`"));
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
        .filter(|name| {
            name.starts_with("data-collect-training-collector") && name.ends_with(".service")
        })
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
fn paper_live_current_docs_preserve_independent_runtime_boundary() {
    let interfaces = read(INTERFACES);
    let limitations = read(LIMITATIONS);
    let acceptance = read(ACCEPTANCE);

    for expected in [
        "data-collect-paper-hot",
        "data-collect-live-hot",
        "<dataset-root>/paper/data_collect_handoff/",
        "<dataset-root>/live/data_collect_handoff/",
        "data_collect_handoff_v1",
        "frame_type=source_status",
        "status=heartbeat",
        "external_market_event",
        "没有真实 source producer wiring",
        "runtime_receive_time_utc_ms",
        "append-only event log",
        "默认运行 `604800s`（7 天）后",
        "`COLLECTOR_REGION`、AWS IMDS",
        "文件内容不被解析为业务配置，也不进入 digest",
        "`--run-id` 当前只校验 trim 后非空",
    ] {
        assert!(
            interfaces.contains(expected),
            "current interfaces missing {expected}"
        );
    }

    for expected in [
        "真实 Polymarket、RTDS、Binance adapter 与 producer wiring 尚未实现",
        "当前不输出真实 `external_market_event`",
        "authoritative systemd unit",
        "不属于 `model-train`",
        "不是 `data-collect-training-collector`",
        "只有在用户决定返回 `data-collect` 时",
        "hot binary 的 startup receipt → 长运行 heartbeat server 路径尚无端到端测试",
        "confinement 写成已验收",
    ] {
        assert!(
            limitations.contains(expected),
            "known limitations missing {expected}"
        );
    }

    for expected in [
        "IMPLEMENTED_AND_COMPONENT_TESTED",
        "IMPLEMENTED_NOT_END_TO_END_TESTED",
        "DEFERRED_UNTIL_MODEL_TRAIN_COMPLETE_AND_USER_RETURN_DECISION",
        "当前测试没有直接调用实际",
    ] {
        assert!(
            acceptance.contains(expected),
            "current acceptance missing {expected}"
        );
    }
}

#[test]
fn current_docs_preserve_exact_capacity_and_rtds_boundaries() {
    for path in [CONTRACTS, MASTER_DEMAND] {
        let doc = read(path);
        for expected in [
            "startup",
            "直接退出 `75`",
            "每 `60s`",
            "drain/finalize",
            "`×2`",
            "`±20%`",
            "cap",
            "无独立 attempt",
            "SDK transport-control",
            "不等同于 channel",
        ] {
            assert!(doc.contains(expected), "{path} missing {expected}");
        }
    }

    let contracts = read(CONTRACTS);
    for expected in [
        "production `src/` delta 横跨四个文件",
        "`clob/ws/types/response.rs`",
        "`rtds/subscription.rs`",
        "`ws/config.rs` 与 `ws/connection.rs`",
        "tick_size_change",
        "`MidpointUpdate.timestamp`",
        "interested element 必须全部成功",
        "不泄露合法前缀",
        "Network error communicating with endpoint",
        "不含\n  `topic/type/timestamp/payload`",
        "target-like frame 仍是 sticky parser terminal",
        "最大 `2048` bytes",
        "connect timeout",
        "generation-bound\n  outbound fencing",
        "其中只有九个",
        "nullable Decimal 字段",
        "非空 malformed（含空白串）",
        "optional Decimal 空串和 required Decimal 空串仍报错",
        "configured capacity limit",
        "测试或另行批准配置可以显式覆盖",
    ] {
        assert!(
            contracts.contains(expected),
            "current contracts missing {expected}"
        );
    }
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
