//! #363 无模型协议/成本夹具；成本探针显式选择，默认测试不运行规模工作量。
use super::*;
use std::{collections::BTreeMap, path::Path};

pub(crate) fn worker(
    root: &Path,
    max_batch: usize,
    capture: bool,
) -> (ModelWorker, std::path::PathBuf) {
    std::fs::create_dir_all(root).unwrap();
    let log = root.join("requests.jsonl");
    let control = root.join("control.json");
    std::fs::write(
        &control,
        serde_json::to_vec(&json!({"log":log,"capture":capture})).unwrap(),
    )
    .unwrap();
    let config = crate::worker::WorkerConfig {
        python: "python3".into(),
        script: Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ipc-worker.py"),
        model: control,
        device: "cpu".into(),
        torch_threads: 1,
        max_batch,
        encoding: EncodingConfig {
            field_buckets: 4096,
            category_buckets: 4096,
        },
    };
    (ModelWorker::start(config).unwrap(), log)
}
fn records(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}
fn consumer(root: &Path, learning: bool, capture: bool) -> (ModelConsumer, std::path::PathBuf) {
    std::fs::create_dir_all(root).unwrap();
    let (rules, day) = crate::learning::learning_tests::material(root);
    let (worker, log) = worker(&root.join("worker"), 3, capture);
    let run = crate::ConsumerRunConfig {
        rules: rules.into(),
        days: vec![day],
        parallel_dates: 1,
        worker: (*worker.config).clone(),
    };
    let opened = crate::open_consumer_rules(&run).unwrap();
    let config = ConsumerConfig {
        account_key: "ipc-account".into(),
        sampling_seed: 181,
        request_namespace: "ipc".into(),
        progress_every: 0,
        learning: learning.then(|| crate::learning::CollectionConfig {
            group_id: "ipc-group".into(),
            active_end_utc_ms: 300000,
            evidence_end_utc_ms: 301000,
            trajectory_path: root.join("trajectory.jsonl"),
            collection_fault_after_call: None,
            special: None,
        }),
    };
    let consumer = ModelConsumer::new(
        config,
        worker,
        opened.rules,
        backtest::EntryMarketScopeV1::default()
            .expand(0, 300000)
            .unwrap(),
        &opened.binding,
    )
    .unwrap();
    (consumer, log)
}
pub(crate) fn public_frames(root: &Path) -> Vec<FrozenMarketReadV1> {
    std::fs::create_dir_all(root).unwrap();
    let (rules, day) = crate::learning::learning_tests::material(root);
    let at = crate::learning_test_fixture::common::BASE + 2500;
    let queries: BTreeMap<String, std::collections::BTreeSet<i64>> =
        [("a".into(), [at].into_iter().collect())]
            .into_iter()
            .collect();
    let mut output = None;
    crate::learning::visit_day_frames(&rules, None, &day, &queries, |_, _, _, frames| {
        output = Some(frames.to_vec());
        Ok(())
    })
    .unwrap();
    output.unwrap()
}
pub(crate) fn row(frames: &[FrozenMarketReadV1], padding: usize) -> Value {
    let private = frames
        .iter()
        .map(|_| json!({"cash":"2000","commitments":[],"known":null}))
        .collect::<Vec<_>>();
    json!({"version":crate::public_input::TRAJECTORY_VERSION,"account":"ipc-account","call":17,
        "processing_start_utc_ms":crate::learning_test_fixture::common::BASE+2510,
        "market_ids":frames.iter().map(|f|&f.observation.market.condition_id).collect::<Vec<_>>(),
        "global_private":{"cash":"2000","orders":0,"unknown":null},"market_private":private,
        "options":[{"kind":"NoTrade"},{"kind":"Repair","market":0,"side":0}],"selected_option":1,
        "rpc":[{"request":{"phase":"post","ordinal":0,"side":0,"allowed":[false,true]},"response":{"post_only":true}},
            {"request":{"phase":"price","ordinal":0,"side":0,"maximum":10},"response":{"index":2}},
            {"request":{"phase":"quantity","count":"20"},"response":{"rank":"3"}}],
        "old_joint_logp":-0.5,"economic_identity":{"ledger_entry_seq":19,"necessary_unknown":["cost"]},
        "economic_audit":vec!["x".repeat(125);padding.div_ceil(128)]})
}
#[test]
fn ipc_exchange_preserves_lines_rpc_and_failed_request_is_not_recorded() {
    let root = tempfile::tempdir().unwrap();
    for learning in [false, true] {
        let (consumer, log) = consumer(
            &root.path().join(format!("learning-{learning}")),
            learning,
            true,
        );
        let mut elapsed = 0.0;
        let requests = [
            json!({"phase":"begin","account":"ipc-account","call":1,"global":[[1,2,0.5,0]],"markets":[],"options":[{"kind":"NoTrade"}]}),
            json!({"phase":"post","account":"ipc-account","call":1,"allowed":[false,true],"ordinal":0,"side":0}),
            json!({"phase":"price","account":"ipc-account","call":1,"maximum":10,"ordinal":0,"side":0}),
        ];
        for request in &requests {
            let response = consumer.exchange(request.clone(), &mut elapsed).unwrap();
            assert_eq!(response["phase"], request["phase"]);
            assert_eq!(response["account"], request["account"]);
            assert_eq!(response["call"], 1);
        }
        let logged = records(&log);
        assert_eq!(logged.len(), 4);
        for (record, request) in logged[1..].iter().zip(&requests) {
            assert_eq!(record["value"], json!({"batch":[request]}));
            let mut expected = serde_json::to_vec(&json!({"batch":[request]})).unwrap();
            expected.push(b'\n');
            use sha2::{Digest, Sha256};
            assert_eq!(record["sha256"], format!("{:x}", Sha256::digest(&expected)));
            assert_eq!(record["bytes"], expected.len());
        }
        assert_eq!(consumer.rpc.borrow().len(), if learning { 2 } else { 0 });
        if learning {
            for (rpc, request) in consumer.rpc.borrow().iter().zip(&requests[1..]) {
                assert_eq!(
                    rpc,
                    &json!({"request":request,"response":{"post_only":true,"index":2,"rank":3}})
                );
            }
        }
        assert!(
            consumer
                .exchange(json!({"phase":"post","boundary":"error"}), &mut elapsed)
                .unwrap_err()
                .contains("fixture error")
        );
        assert_eq!(consumer.rpc.borrow().len(), if learning { 2 } else { 0 });
    }
}
#[test]
fn ipc_worker_batch_boundaries_empty_tail_and_count_error_keep_original_policy() {
    let root = tempfile::tempdir().unwrap();
    let (worker, log) = worker(root.path(), 3, true);
    let requests = (0..8)
        .map(|i| json!({"account":format!("account-{i}"),"call":i,"phase":"begin"}))
        .collect::<Vec<_>>();
    let response = worker.request_batch(requests.clone()).unwrap();
    for (i, r) in response.iter().enumerate() {
        assert_eq!(r["account"], requests[i]["account"]);
        assert_eq!(r["call"], i);
    }
    let logged = records(&log);
    assert_eq!(
        logged
            .iter()
            .skip(1)
            .map(|r| r["value"]["batch"].as_array().unwrap().len())
            .collect::<Vec<_>>(),
        vec![3, 3, 2]
    );
    assert!(worker.request_batch(vec![]).unwrap().is_empty());
    assert_eq!(records(&log).len(), logged.len());
    assert!(
        worker
            .request(json!({"boundary":"count"}))
            .unwrap_err()
            .contains("批响应不匹配")
    );
}
fn cpu_ticks() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields = s[s.rfind(')').unwrap() + 2..]
        .split_whitespace()
        .collect::<Vec<_>>();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}
fn memory_status() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .filter(|s| s.starts_with("VmRSS:") || s.starts_with("VmHWM:"))
        .collect::<Vec<_>>()
        .join("; ")
}
/// 固定两档各64次。预先生成和克隆调用方输入单列，不将准备耗时混入管线。
#[test]
#[ignore = "#363显式CPU合成成本比较，不运行模型"]
fn ipc_cost_probe() {
    let out = std::path::PathBuf::from(std::env::var_os("IPC_COST_OUTPUT").expect("显式输出目录"));
    std::fs::create_dir(&out).unwrap();
    let ticks_per_second = String::from_utf8(
        std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .parse::<u64>()
    .unwrap();
    let mut costs = Vec::new();
    for target in [65536usize, 2097152] {
        let prepare = Instant::now();
        let prepare_cpu = cpu_ticks();
        let (consumer, digest_log) = consumer(&out.join(format!("chain-{target}")), false, false);
        let token = json!([1, 2, 0.5, 0]);
        let empty = json!({"phase":"begin","account":"ipc-account","call":1,"global":[],"markets":[],"options":[{"kind":"NoTrade"}]});
        let n = (target - serde_json::to_vec(&empty).unwrap().len())
            / (serde_json::to_vec(&token).unwrap().len() + 1);
        let request = json!({"phase":"begin","account":"ipc-account","call":1,"global":vec![token;n],"markets":[],"options":[{"kind":"NoTrade"}]});
        let requests = (0..64).map(|_| request.clone()).collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&request).unwrap().len();
        let prep_wall = prepare.elapsed().as_secs_f64();
        let prep_cpu = cpu_ticks() - prepare_cpu;
        eprintln!("IPC成本 chain target={target} actual_json={bytes} iterations=64");
        let begin = Instant::now();
        let cpu = cpu_ticks();
        let mem = memory_status();
        let mut elapsed = 0.0;
        for request in requests {
            consumer.exchange(request, &mut elapsed).unwrap();
        }
        costs.push(json!({"path":"strategy.exchange->worker->fake-process","target_json_bytes":target,"actual_request_json_bytes":bytes,"iterations":64,"prepare_wall_seconds":prep_wall,"prepare_cpu_ticks":prep_cpu,"wall_seconds":begin.elapsed().as_secs_f64(),"rust_cpu_ticks":cpu_ticks()-cpu,"ticks_per_second":ticks_per_second,"memory_before":mem,"memory_after":memory_status(),"worker_io":consumer.worker.io_metrics(),"request_line_digests":records(&digest_log)}));
        drop(consumer);
        let prepare = Instant::now();
        let prepare_cpu = cpu_ticks();
        // 两轮复用完全相同的冻结观测/规则。statistics=None是合法未物化状态；完整统计接线另由定向回归覆盖。
        let frozen = std::path::PathBuf::from(
            std::env::var_os("IPC_COST_FROZEN_FRAMES").expect("显式冻结夹具"),
        );
        let frames: Vec<FrozenMarketReadV1> = if frozen.exists() {
            let saved: Vec<(MarketBusinessObservationV1, PublicTradingRuleContextV1)> =
                serde_json::from_slice(&std::fs::read(&frozen).unwrap()).unwrap();
            saved
                .into_iter()
                .map(|(observation, rule_context)| FrozenMarketReadV1 {
                    observation,
                    rule_context: Arc::new(rule_context),
                    statistics: None,
                })
                .collect()
        } else {
            let mut frames = public_frames(&out.join(format!("public-{target}")));
            for f in &mut frames {
                f.statistics = None;
            }
            std::fs::write(
                &frozen,
                serde_json::to_vec(
                    &frames
                        .iter()
                        .map(|f| (&f.observation, f.rule_context.as_ref()))
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .unwrap();
            frames
        };
        let row = row(&frames, target);
        let label = json!({"advantage":1.25,"critic_target":-2,"old_logp":-0.5,"extra":{"necessary_unknown":[],"preserve":null}});
        let (worker, log) = worker(&out.join(format!("learn-{target}")), 3, false);
        let sample_frame =
            crate::learning::ipc_cost_frame(&row, &frames, &worker.config.encoding).unwrap();
        let sample_bytes =
            serde_json::to_vec(&json!({"phase":"learn_step","frame":sample_frame,"label":label}))
                .unwrap();
        std::fs::write(
            out.join(format!("sample-learn-request-{target}.json")),
            &sample_bytes,
        )
        .unwrap();
        let frame_size = sample_bytes.len();
        let prep_wall = prepare.elapsed().as_secs_f64();
        let prep_cpu = cpu_ticks() - prepare_cpu;
        eprintln!("IPC成本 learn target={target} request_json={frame_size} iterations=64");
        let begin = Instant::now();
        let cpu = cpu_ticks();
        let mem = memory_status();
        for _ in 0..64 {
            let frame =
                crate::learning::ipc_cost_frame(&row, &frames, &worker.config.encoding).unwrap();
            let request = json!({"phase":"learn_step","frame":frame,"label":label});
            worker.request(request).unwrap();
        }
        costs.push(json!({"path":"production-frame-compose->worker->fake-process","target_row_padding_bytes":target,"actual_row_json_bytes":serde_json::to_vec(&row).unwrap().len(),"actual_learn_request_json_bytes":frame_size,"iterations":64,"prepare_wall_seconds":prep_wall,"prepare_cpu_ticks":prep_cpu,"wall_seconds":begin.elapsed().as_secs_f64(),"rust_cpu_ticks":cpu_ticks()-cpu,"ticks_per_second":ticks_per_second,"memory_before":mem,"memory_after":memory_status(),"worker_io":worker.io_metrics(),"request_line_digests":records(&log)}));
    }
    std::fs::write(
        out.join("costs.json"),
        serde_json::to_vec_pretty(&costs).unwrap(),
    )
    .unwrap();
}
