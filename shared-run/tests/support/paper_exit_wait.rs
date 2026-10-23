//! 通过同一生产宿主，证明实际记录容量等待不会推迟正式程序Exit。
#![allow(dead_code, unused_imports)]
#[path = "../common/fixture_input.rs"]
mod fixture_input;
#[path = "../common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../common/fixture_reference.rs"]
mod fixture_reference;
#[path = "../common/fixture_reference_inputs.rs"]
mod fixture_reference_inputs;
mod common {
    pub use super::fixture_input::*;
    pub use super::fixture_policy::*;
    pub use super::fixture_recording::*;
    pub use super::fixture_reference_inputs::*;
}
use crate::{paper::*, paper_strategy::*, *};
use common::*;
use contracts::*;
use data_collect::handoff::{read_handoff_v2, write_handoff_v2};
use data_pipeline_core::{public_input::*, typed_raw::*};
use data_pipeline_training::{public_online::*, public_recording::*};
use sim_system::{
    execution::{BusinessProjectionVersionV1, RequestContextV2},
    latency_profile::*,
    platform_rules::*,
};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncReadExt,
    net::{UnixListener, UnixStream},
};
#[path = "../common/fixture_paper.rs"]
mod fixture_paper;
use fixture_paper::*;

struct ExitOnParentInstruction;
impl StrategyV1 for ExitOnParentInstruction {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        unreachable!()
    }
}
impl DetachedStrategyV1 for ExitOnParentInstruction {
    fn decide_detached(
        &mut self,
        _: &DetachedStrategyInputV1,
        _: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        use std::io::Read;
        let mut instruction = [0u8];
        std::io::stdin().read_exact(&mut instruction).unwrap();
        assert_eq!(instruction, [1]);
        Ok(StrategyOutputV1::Exit { diagnostics: () })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn process_exit_during_actual_recording_capacity_wait() {
    if let Some(config_path) = std::env::var_os("PAPER_BLOCKED_EXIT_CHILD") {
        let config: PaperStrategyConfigV1 =
            serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
        let (_, rules) = setup(
            &config
                .runtime
                .ledger_root
                .parent()
                .unwrap()
                .join("rules-fixture"),
        );
        let entered_path = config
            .runtime
            .ledger_root
            .parent()
            .unwrap()
            .join("host-entered-capacity-send");
        let ledger_root = config.runtime.ledger_root.clone();
        let baseline_path = ledger_root.parent().unwrap().join("ledger-at-pause.json");
        RECORDING_PAUSE_AFTER_CALCULATION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |driver| {
                let (notice, received) = std::sync::mpsc::sync_channel(1);
                let release = driver.pause_recording_for_measurement(notice);
                std::fs::write(
                    &baseline_path,
                    serde_json::to_vec(&ledger_files(&ledger_root)).unwrap(),
                )
                .unwrap();
                // 整个测试不发送release，也不因局部作用域结束而解暂停。
                std::mem::forget(release);
                std::thread::spawn(move || {
                    received.recv().unwrap();
                    std::fs::write(entered_path, b"original pending send reached").unwrap();
                });
            }));
        });
        let _ = run_paper_strategy_with_exit_v1(
            &config,
            rules,
            ExitOnParentInstruction,
            std::future::pending(),
            |_| {},
            || std::process::exit(0),
        )
        .await;
        panic!("普通process退出必须先于主线程返回和排空");
    }
    let evidence = PaperHostEvidenceCase::new("blocked-process-exit");
    std::fs::create_dir(evidence.path.join("rules-fixture")).unwrap();
    let (base, _) = setup(&evidence.path);
    let mut config = PaperStrategyConfigV1 {
        runtime: base.runtime,
        statistics_input_set_identity: Sha256Hex("f".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![],
            spot_variance_windows_ms: vec![],
            terminal_integral_windows_ms: vec![],
            blb_lookback_windows_ms: vec![],
        },
    };
    config.runtime.finite_in_flight_groups = 0;
    let config_path = evidence.path.join("child-config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "paper_strategy::paper_exit_wait::process_exit_during_actual_recording_capacity_wait",
            "--nocapture",
        ])
        .env("PAPER_BLOCKED_EXIT_CHILD", &config_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::fs::File::create(evidence.path.join("stdout.log")).unwrap())
        .stderr(std::fs::File::create(evidence.path.join("stderr.log")).unwrap())
        .spawn()
        .unwrap();
    let pid = child.id();
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::Message {
                transport_sequence: 2,
                row: book_row(1),
            },
        )
        .await
        .unwrap();
        let mut bytes = vec![];
        stream.read_to_end(&mut bytes).await.unwrap();
    };
    let control = async {
        loop {
            let wchan = std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/wchan"))
                .unwrap_or_default();
            if evidence.path.join("host-entered-capacity-send").exists() && wchan.contains("futex")
            {
                std::fs::write(evidence.path.join("observed-main-thread-wchan.txt"), wchan)
                    .unwrap();
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "子进程必须先进入原容量等待"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        use std::io::Write;
        child.stdin.take().unwrap().write_all(&[1]).unwrap();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let (status, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(control, peer)
    })
    .await
    .expect("受控子进程未结束；不强杀或解记录暂停伪造通过");
    assert!(status.success());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert!(
        !std::fs::read_to_string(&config.runtime.sim_log)
            .unwrap()
            .contains("\"RUN_SEAL\"")
    );
    let baseline: std::collections::BTreeMap<String, Vec<u8>> =
        serde_json::from_slice(&std::fs::read(evidence.path.join("ledger-at-pause.json")).unwrap())
            .unwrap();
    assert_eq!(
        ledger_files(&config.runtime.ledger_root),
        baseline,
        "暂停之后没有新Ledger落盘；开户原稳定前缀保持"
    );
}

fn ledger_files(root: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            assert!(e.file_type().unwrap().is_file());
            (
                e.file_name().to_str().unwrap().to_owned(),
                std::fs::read(e.path()).unwrap(),
            )
        })
        .collect()
}

struct FeedbackCompletionStrategy {
    phase: u8,
    response_received_at: Option<i64>,
    reads: Arc<std::sync::Mutex<Vec<(u8, u64, bool)>>>,
}
impl StrategyV1 for FeedbackCompletionStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        unreachable!()
    }
    fn qualify_intent_borrowed(
        &mut self,
        _: &BorrowedStrategyCurrentReadV1<'_>,
        _: &StrategyCalculationV1,
        _: usize,
        _: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        Ok(IntentQualificationV1::Qualified)
    }
    fn dispatched(
        &mut self,
        _: &StrategyCalculationV1,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        let sent = result.as_ref().unwrap();
        if let Some(receipt) = sent.first() {
            // 原固定seed/profile第一条DirectResponseInbound实际抽样=10（setup已核）。
            self.response_received_at = Some(receipt.receipt.platform_arrived_at_utc_ms() + 10);
        }
    }
}
impl DetachedStrategyV1 for FeedbackCompletionStrategy {
    fn decide_detached(
        &mut self,
        input: &DetachedStrategyInputV1,
        _: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        let completion = input.action_before.cancellation_requests(None);
        let completed = matches!(
            completion.latest_response,
            ExecutionReadValueV1::Known(Some(_))
        );
        self.reads.lock().unwrap().push((
            self.phase,
            input.action_before.cutoff().private_feedback_sequence(),
            completed,
        ));
        match self.phase {
            0 => {
                self.phase = 1;
                Ok(StrategyOutputV1::Continue {
                    intents: vec![ReservedIntentV1 {
                        context: context("feedback-dirty-cancel"),
                        intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                            condition_id: ConditionId(CONDITION_ID.into()),
                        }),
                        reserves: vec![],
                    }],
                    next_evaluation_at_utc_ms: None,
                    diagnostics: (),
                })
            }
            1 => {
                self.phase = 2;
                let until = self.response_received_at.unwrap() + 1;
                std::thread::sleep(Duration::from_millis((until - now()).max(0) as u64));
                Ok(StrategyOutputV1::Continue {
                    intents: vec![],
                    next_evaluation_at_utc_ms: None,
                    diagnostics: (),
                })
            }
            _ => {
                assert!(completed, "新计算必须实际读到已领取的原取消请求完成反馈");
                Ok(StrategyOutputV1::Exit { diagnostics: () })
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn worker_first_completion_consumes_feedback_and_recalculates_without_new_public_input() {
    let evidence = PaperHostEvidenceCase::new("worker-first-feedback-dirty");
    let (base, rules) = setup(&evidence.path);
    let config = PaperStrategyConfigV1 {
        runtime: base.runtime,
        statistics_input_set_identity: Sha256Hex("e".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![],
            spot_variance_windows_ms: vec![],
            terminal_integral_windows_ms: vec![],
            blb_lookback_windows_ms: vec![],
        },
    };
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let (sent, received) = std::sync::mpsc::channel();
    WORKER_RESULT_SENT_NOTICE.with(|notice| *notice.borrow_mut() = Some(sent));
    let reads = Arc::new(std::sync::Mutex::new(vec![]));
    let strategy = FeedbackCompletionStrategy {
        phase: 0,
        response_received_at: None,
        reads: reads.clone(),
    };
    let (first_sent, first_received) = tokio::sync::oneshot::channel();
    let mut first_sent = Some(first_sent);
    let mut held = false;
    let mut completed_notices = 0;
    let progress = |p: PaperStrategyProgressV1<'_>| {
        if p.measurements.actual_sent_intents == 1 {
            if let Some(sent) = first_sent.take() {
                sent.send(()).unwrap();
            }
        }
        if p.measurements.calculations_started == 2 && !held {
            held = true;
            // 受控调度：宿主在进入select之前等待实际第二个结果已入oneshot。
            // 同时原反馈timer已到；biased分支必须先执行真实完成逻辑。
            while completed_notices < 2 {
                received.recv().unwrap();
                completed_notices += 1;
            }
        }
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        first_received.await.unwrap();
        send(&mut stream, 2, book_row(1)).await;
        let mut bytes = vec![];
        stream.read_to_end(&mut bytes).await.unwrap();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_strategy_with_progress_v1(
                &config,
                rules,
                strategy,
                std::future::pending(),
                progress
            ),
            peer
        )
    })
    .await
    .expect("静默时实际新反馈必须触发最早合法重算");
    WORKER_RESULT_SENT_NOTICE.with(|notice| notice.borrow_mut().take());
    let report = run.unwrap();
    assert!(matches!(report.outcome, PaperStrategyEndV1::StrategyExit));
    assert_eq!(report.measurements.applied_inputs, 2);
    assert_eq!(
        report.measurements.health_or_obligation_wakes, 0,
        "新反馈在完成分支领取而非timer分支"
    );
    assert_eq!(report.measurements.calculations_started, 3);
    let reads = reads.lock().unwrap();
    assert_eq!(
        reads.as_slice(),
        &[(0, 0, false), (1, 0, false), (2, 1, true)]
    );
    std::fs::write(
        evidence.path.join("reads.json"),
        serde_json::to_vec_pretty(&*reads).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}

// 与正式健康schedule相同的严格未来约定；期限来自真实来源事实。
struct SilentHealthSchedule {
    scope: SourceStreamScopeV1,
    silence_ms: i64,
}
impl StrategyPublicHealthScheduleV1 for SilentHealthSchedule {
    fn next_evaluation_at(&self, markets: &[&MarketBusinessObservationV1], at: i64) -> Option<i64> {
        markets
            .iter()
            .filter_map(|market| {
                market
                    .source_progress
                    .get_scope(&self.scope)
                    .and_then(|p| p.last_valid_business_message.as_ref())
                    .map(|message| message.received_at_utc_ms + self.silence_ms)
            })
            .filter(|due| *due > at)
            .min()
    }
}
struct SilentHealthStrategy {
    schedule: SilentHealthSchedule,
    phase: u8,
    platform_arrived_at: Option<i64>,
    cancel_input_feedback: u64,
    reads: Arc<std::sync::Mutex<Vec<(u8, i64, i64, u64)>>>,
    qualified_at: Arc<std::sync::Mutex<Option<i64>>>,
}
impl StrategyV1 for SilentHealthStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        unreachable!()
    }
    fn uses_borrowed_input(&self) -> bool {
        true
    }
    fn uses_source_health(&self) -> bool {
        true
    }
    fn public_health_schedule(&self) -> Option<Box<dyn StrategyPublicHealthScheduleV1>> {
        Some(Box::new(SilentHealthSchedule {
            scope: self.schedule.scope.clone(),
            silence_ms: self.schedule.silence_ms,
        }))
    }
    fn qualify_intent_borrowed(
        &mut self,
        read: &BorrowedStrategyCurrentReadV1<'_>,
        _: &StrategyCalculationV1,
        _: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let received = read
            .markets
            .iter()
            .find_map(|frame| {
                frame
                    .observation
                    .source_progress
                    .get_scope(&self.schedule.scope)
                    .and_then(|p| p.last_valid_business_message.as_ref())
                    .map(|message| message.received_at_utc_ms)
            })
            .unwrap();
        assert!(
            at >= received + self.schedule.silence_ms,
            "风险取消只在原来源静默期限后产生"
        );
        assert_eq!(
            read.action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms(),
            at
        );
        *self.qualified_at.lock().unwrap() = Some(at);
        Ok(IntentQualificationV1::Qualified)
    }
    fn dispatched(
        &mut self,
        _: &StrategyCalculationV1,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        if let Some(receipt) = result.as_ref().unwrap().first() {
            self.platform_arrived_at = Some(receipt.receipt.platform_arrived_at_utc_ms());
        }
    }
}
impl DetachedStrategyV1 for SilentHealthStrategy {
    fn decide_detached(
        &mut self,
        input: &DetachedStrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        let empty = || StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        };
        let Some(received) = input.markets.iter().find_map(|market| {
            market
                .observation
                .source_progress
                .get_scope(&self.schedule.scope)
                .and_then(|p| p.last_valid_business_message.as_ref())
                .map(|message| message.received_at_utc_ms)
        }) else {
            return Ok(empty());
        };
        let due = received + self.schedule.silence_ms;
        let sequence = input.action_before.cutoff().private_feedback_sequence();
        self.reads
            .lock()
            .unwrap()
            .push((self.phase, at, due, sequence));
        match self.phase {
            0 => {
                assert!(at < due, "原冻结确在期限前；不改来源或执行时钟");
                self.phase = 1;
                // 有界计算负载跨过这次来源的准确绝对期限，不更改生产延迟。
                std::thread::sleep(Duration::from_millis((due - now()).max(0) as u64));
                Ok(empty())
            }
            1 => {
                assert!(at >= due, "已越过的未消费期限必须按当前合法观察立即重算");
                self.phase = 2;
                self.cancel_input_feedback = sequence;
                Ok(StrategyOutputV1::Continue {
                    intents: vec![ReservedIntentV1 {
                        context: context("silent-health-risk-cancel"),
                        intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                            condition_id: ConditionId(CONDITION_ID.into()),
                        }),
                        reserves: vec![],
                    }],
                    next_evaluation_at_utc_ms: None,
                    diagnostics: (),
                })
            }
            _ => {
                assert!(
                    sequence > self.cancel_input_feedback
                        || at >= self.platform_arrived_at.unwrap(),
                    "已消费健康期限不能在无新事实、无下一原义务的静默段持续空转"
                );
                if sequence > self.cancel_input_feedback {
                    Ok(StrategyOutputV1::Exit { diagnostics: () })
                } else {
                    Ok(empty()) // 原请求到达等真实义务允许唤醒，不冒充忙循环。
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn worker_first_empty_result_preserves_overdue_health_and_consumes_it_once() {
    let evidence = PaperHostEvidenceCase::new("worker-first-health-due");
    let (base, rules) = setup(&evidence.path);
    let config = PaperStrategyConfigV1 {
        runtime: base.runtime,
        statistics_input_set_identity: Sha256Hex("a".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![],
            spot_variance_windows_ms: vec![],
            terminal_integral_windows_ms: vec![],
            blb_lookback_windows_ms: vec![],
        },
    };
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let (sent, received) = std::sync::mpsc::channel();
    WORKER_RESULT_SENT_NOTICE.with(|notice| *notice.borrow_mut() = Some(sent));
    let reads = Arc::new(std::sync::Mutex::new(vec![]));
    let qualified_at = Arc::new(std::sync::Mutex::new(None));
    let strategy = SilentHealthStrategy {
        schedule: SilentHealthSchedule {
            silence_ms: 1000,
            scope: SourceStreamScopeV1 {
                source: SourceStreamKindV1::PolymarketBook,
                condition_id: Some(ConditionId(CONDITION_ID.into())),
                asset_or_symbol: UP_TOKEN_ID.into(),
            },
        },
        phase: 0,
        platform_arrived_at: None,
        cancel_input_feedback: 0,
        reads: reads.clone(),
        qualified_at: qualified_at.clone(),
    };
    let (ready, first_completed) = tokio::sync::oneshot::channel();
    let mut ready = Some(ready);
    let mut held = false;
    let mut notices = 0;
    let progress = |p: PaperStrategyProgressV1<'_>| {
        if p.measurements.calculations_completed == 1 {
            if let Some(ready) = ready.take() {
                ready.send(()).unwrap();
            }
        }
        if p.measurements.applied_inputs == 2 && p.measurements.calculations_started == 2 && !held {
            held = true;
            // 进入select前，两次结果均确实已入oneshot；第二次结果跨过原健康期限。
            while notices < 2 {
                received.recv().unwrap();
                notices += 1;
            }
        }
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        first_completed.await.unwrap();
        send(&mut stream, 2, book_row(1)).await;
        let mut bytes = vec![];
        stream.read_to_end(&mut bytes).await.unwrap();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_strategy_with_progress_v1(
                &config,
                rules,
                strategy,
                std::future::pending(),
                progress
            ),
            peer
        )
    })
    .await
    .expect("原来源静默期限不能因空Continue优先完成而丢失");
    WORKER_RESULT_SENT_NOTICE.with(|notice| notice.borrow_mut().take());
    let report = run.unwrap();
    let reads = reads.lock().unwrap();
    assert!(held);
    assert!(matches!(report.outcome, PaperStrategyEndV1::StrategyExit));
    assert_eq!(
        report.measurements.applied_inputs, 2,
        "没有新行情输入或End推进健康"
    );
    assert_eq!(report.measurements.actual_sent_intents, 1);
    assert_eq!(reads[0].0, 0);
    assert_eq!(reads[1].0, 1);
    assert_eq!(reads[0].3, 0);
    assert_eq!(reads[1].3, 0, "原健康重算之前没有私有反馈触发");
    assert!(reads[0].1 < reads[0].2 && reads[1].1 >= reads[0].2);
    assert!(qualified_at.lock().unwrap().is_some());
    let mut requests = vec![];
    sim_system::execution::visit_sim_requests_v1(
        &config.runtime.sim_log,
        config.runtime.max_group_bytes,
        |_, request, _| {
            requests.push(serde_json::to_value(request).unwrap());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        requests.len(),
        1,
        "真实原owner仅登记这次风险取消，不重复发送"
    );
    std::fs::write(
        evidence.path.join("requests.json"),
        serde_json::to_vec_pretty(&requests).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("reads.json"),
        serde_json::to_vec_pretty(&*reads).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("qualified-at.json"),
        serde_json::to_vec_pretty(&*qualified_at.lock().unwrap()).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}
