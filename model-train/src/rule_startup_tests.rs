//! CPU/受控协议夹具。实际公共HTTP事务另由sim-system原ScriptedServer覆盖。
use super::*;
use crate::rules::{OpenedRulesV1, bind_consumer, open_rules_with};
use crate::{ConsumerRulesV1, RuleOpenV1};
use sim_system::{
    platform_rules::*,
    public_consumption::{GammaResolutionPolicyV1, InputLifecyclePolicyV1},
};
use std::{cell::Cell, sync::Arc};

fn prepared(
    root: &std::path::Path,
) -> (
    LearningJobConfig,
    Arc<SealedProcessRuleSnapshotV3>,
    std::path::PathBuf,
) {
    let (mut config, _) = super::learning_tests::collection(root, false);
    let source = crate::open_consumer_rules(&config.consumer).unwrap().rules;
    let final_path = root.join("final-current.json");
    config.consumer.rules = ConsumerRulesV1::explicit(RuleOpenV1::CurrentQuery {
        reference_market: source.reference_market().clone(),
        source_snapshot_path: root.join("scripted-source.json"),
        effective_config_path: root.join("effective-consumer.json"),
        snapshot_path: final_path.clone(),
        implementation_identity: source.implementation_identity().clone(),
        public_maker_roles: vec![SimCallerRoleV1::ModelTrain],
    });
    for day in &mut config.consumer.days {
        for (i, c) in day.candidates.iter_mut().enumerate() {
            let out = root.join(format!("base-account-{i}"));
            std::fs::create_dir(&out).unwrap();
            c.session.account_id = format!("startup-{i}");
            c.session.run_id = format!("startup-{i}");
            c.session.session_id = format!("startup-{i}");
            c.sim_log = out.join("sim.jsonl");
            c.noe_root = out.join("noe");
            c.ledger_root = out.join("ledger");
            c.metadata_path = out.join("run.json");
            c.result_path = out.join("result.json");
            c.research_evidence_path = Some(out.join("decisions.jsonl"));
            c.session.caller_role = SimCallerRoleV1::ModelTrain;
            c.session.input_lifecycle_policy = InputLifecyclePolicyV1::FiniteInput;
            c.timing.decision_mode = backtest::HistoricalDecisionModeV1::Fixed100msFifo;
            c.gamma_resolution_policy = GammaResolutionPolicyV1::QualifiedPublicConsumption;
            c.strategy.effective_parameters["model_consumer"]["learning"] = Value::Null;
        }
    }
    config.active_end_utc_ms = config.consumer.days[0]
        .preparation
        .scope
        .received_start_utc_ms;
    let control = root.join("protocol-control.json");
    let log = root.join("protocol.jsonl");
    std::fs::write(
        &control,
        serde_json::to_vec(&json!({"log":log,"sealed_rules":final_path,"effective_config":root.join("effective-consumer.json")})).unwrap(),
    )
    .unwrap();
    config.consumer.worker.python = "python3".into();
    config.consumer.worker.script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/rule-protocol-worker.py");
    config.consumer.worker.model = control;
    (config, source, log)
}
fn injected(
    config: &crate::ConsumerRunConfig,
    source: Arc<SealedProcessRuleSnapshotV3>,
    queries: &Cell<usize>,
) -> OpenedRulesV1 {
    open_rules_with(config, |_| {
        queries.set(queries.get() + 1);
        Ok(source)
    })
    .unwrap()
}
#[test]
fn rules_precede_worker_and_sim_completed_collection_and_consumed_recovery_do_not_requery() {
    let root = tempfile::tempdir().unwrap();
    let (mut config, source, log) = prepared(root.path());
    let queries = Cell::new(0);
    let opened = injected(&config.consumer, source, &queries);
    let canonical = bind_consumer(&config.consumer, &opened.binding);
    let saved: crate::ConsumerRunConfig = serde_json::from_slice(
        &std::fs::read(root.path().join("effective-consumer.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&saved).unwrap(),
        serde_json::to_value(&canonical).unwrap()
    );
    let binding = opened.binding.clone();
    let result = run_learning_job_with_opener(&config, |_| Ok(opened)).unwrap();
    assert_eq!(result["no_actor_samples"], true);
    assert_eq!(queries.get(), 1);
    let records = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records[0]["phase"], "worker_start");
    assert_eq!(records[1]["phase"], "learn_open");
    let group = records[1]["group"].clone();
    assert_eq!(
        group["binding"]["consumer"]["rules"]["binding"],
        json!(binding)
    );
    let complete = config.output_directory.join("completed-collection.json");
    let bytes = std::fs::read(&complete).unwrap();
    for ai in 0..2 {
        let trace = config
            .output_directory
            .join(format!("collection/day-0-account-{ai}/trajectory.jsonl"));
        let v: Value =
            serde_json::from_str(std::fs::read_to_string(trace).unwrap().trim()).unwrap();
        assert_eq!(v["record_type"], "rules_binding");
        assert_eq!(v["binding"], json!(binding));
    }
    let trace_bytes = (0..2)
        .map(|ai| {
            std::fs::metadata(
                config
                    .output_directory
                    .join(format!("collection/day-0-account-{ai}/trajectory.jsonl")),
            )
            .unwrap()
            .len()
        })
        .sum::<u64>();
    eprintln!(
        "CPU规则产物字节：最终封存={}，有效配置={}，两个N0账户轨迹绑定={}，完成采集={}",
        std::fs::metadata(&binding.snapshot_path).unwrap().len(),
        std::fs::metadata(root.path().join("effective-consumer.json"))
            .unwrap()
            .len(),
        trace_bytes,
        bytes.len()
    );
    config.consumer = canonical;
    config.attempt_report = root.path().join("recovery-report.json");
    let reopened = open_rules_with(&config.consumer, |_| panic!("恢复不得查询")).unwrap();
    assert_eq!(reopened.binding, binding);
    assert_eq!(reopened.opened_via, SnapshotAccessMode::Recovery);
    let result = run_learning_job_with_opener(&config, |_| Ok(reopened)).unwrap();
    assert_eq!(result["no_actor_samples"], true);
    assert_eq!(std::fs::read(&complete).unwrap(), bytes);
    // 假worker仅模拟已消费回执，核对原group协议，绝不执行optimizer/提交。
    std::fs::write(&config.consumer.worker.model,serde_json::to_vec(&json!({"log":log,"sealed_rules":binding.snapshot_path,"already_committed":true,"expected_group":group})).unwrap()).unwrap();
    config.attempt_report = root.path().join("consumed-report.json");
    let result = run_learning_job_with_opener(&config, |c| {
        open_rules_with(c, |_| panic!("已消费不得查询"))
    })
    .unwrap();
    assert_eq!(result["already_committed"], true);
    assert_eq!(std::fs::read(&complete).unwrap(), bytes);
    let before = std::fs::read(&log).unwrap();
    for implementation in [false, true] {
        let mut bad = config.clone();
        if let ConsumerRulesV1::Explicit(open) = &mut bad.consumer.rules {
            let RuleOpenV1::Recovery { binding } = open.as_mut() else {
                unreachable!()
            };
            if implementation {
                binding.implementation_identity.git_commit = "f".repeat(40);
            } else {
                binding.expected_file_sha256 = Sha256Hex("f".repeat(64));
            }
        }
        assert!(
            run_learning_job_with_opener(&bad, |c| open_rules_with(c, |_| panic!(
                "篡改恢复不得查询"
            )))
            .is_err()
        );
        assert_eq!(std::fs::read(&log).unwrap(), before);
    }
}
#[test]
fn failed_query_has_no_worker_or_sim_and_consumer_root_uses_same_startup() {
    let root = tempfile::tempdir().unwrap();
    let (config, source, log) = prepared(root.path());
    let failed = run_learning_job_with_opener(&config, |c| {
        open_rules_with(c, |_| {
            Err(RuleSnapshotError::InvalidConfig(
                "scripted query failure".into(),
            ))
        })
    });
    assert!(failed.is_err());
    assert!(!log.exists());
    assert!(!root.path().join("collection").exists());
    // 原消费者根也必须先解析规则；零调用策略截止不调用假worker的模型分支。
    let queries = Cell::new(0);
    let mut consumer = config.consumer.clone();
    for day in &mut consumer.days {
        for c in &mut day.candidates {
            let out = root
                .path()
                .join(format!("consumer-{}", c.session.account_id));
            std::fs::create_dir(&out).unwrap();
            c.strategy.effective_parameters["model_consumer"]["learning"] =
                serde_json::to_value(CollectionConfig {
                    group_id: "consumer-n0".into(),
                    active_end_utc_ms: config.active_end_utc_ms,
                    evidence_end_utc_ms: day.preparation.scope.received_end_utc_ms,
                    trajectory_path: out.join("trajectory.jsonl"),
                    collection_fault_after_call: None,
                })
                .unwrap();
        }
    }
    let result =
        crate::run::run_consumer_with_opener(&consumer, |c| Ok(injected(c, source, &queries)))
            .unwrap();
    assert_eq!(queries.get(), 1);
    assert_eq!(result["model"]["forwards"], 0);
}

#[test]
fn two_accounts_two_public_replays_share_sealed_rules_and_do_not_requery() {
    let root = tempfile::tempdir().unwrap();
    let (config, source, _) = prepared(root.path());
    let queries = Cell::new(0);
    let opened = injected(&config.consumer, source, &queries);
    let day = &config.consumer.days[0];
    let at = crate::learning_test_fixture::common::BASE + 2500;
    let times = [
        ("a".to_owned(), [at].into_iter().collect()),
        ("b".to_owned(), [at].into_iter().collect()),
    ]
    .into_iter()
    .collect();
    let mut seen = std::collections::BTreeMap::new();
    let mut original = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let streams = visit_day_frames(
            &opened.binding.public_rules(),
            Some(opened.rules.clone()),
            day,
            &times,
            |key, _, _, frames| {
                assert!(!frames.is_empty());
                for f in frames {
                    assert_eq!(
                        f.rule_context.snapshot_file_sha256,
                        *opened.rules.file_sha256()
                    );
                    assert_eq!(
                        f.statistics.as_ref().unwrap().identity.rules_identity,
                        *opened.rules.file_sha256()
                    );
                }
                let projected = frames
                    .iter()
                    .map(|f| crate::encoding::market_projection(f, at))
                    .collect::<Vec<_>>();
                let encoded = projected
                    .iter()
                    .map(|p| {
                        serde_json::to_value(crate::encoding::tokens(
                            p,
                            &config.consumer.worker.encoding,
                        ))
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                original.insert(key.to_owned(), (projected, encoded));
                *seen.entry(key.to_owned()).or_insert(0) += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(streams, 1);
    }
    assert_eq!(
        seen,
        [("a".to_owned(), 2), ("b".to_owned(), 2)]
            .into_iter()
            .collect()
    );
    let canonical = bind_consumer(&config.consumer, &opened.binding);
    let recovered = open_rules_with(&canonical, |_| panic!("公共再生恢复不能查询")).unwrap();
    assert_eq!(recovered.rules.access_mode(), SnapshotAccessMode::Recovery);
    visit_day_frames(
        &recovered.binding.public_rules(),
        Some(recovered.rules.clone()),
        day,
        &times,
        |key, _, _, frames| {
            let projected = frames
                .iter()
                .map(|f| crate::encoding::market_projection(f, at))
                .collect::<Vec<_>>();
            let encoded = projected
                .iter()
                .map(|p| {
                    serde_json::to_value(crate::encoding::tokens(
                        p,
                        &config.consumer.worker.encoding,
                    ))
                    .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                original[key],
                (projected, encoded),
                "RECOVERY打开方式不改变原公开输入/编码"
            );
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(queries.get(), 1);
}
