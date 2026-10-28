//! #378 有界观察查证；仅单元测试编译，所有计量均包含自身开销。
#[path = "../common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../common/fixture_execution_v4.rs"]
mod fixture_execution_v4;
#[path = "../common/fixture_input.rs"]
mod fixture_input;
#[path = "../common/fixture_market_identity.rs"]
mod fixture_market_identity;
#[path = "../common/fixture_nav.rs"]
mod fixture_nav;
#[path = "../common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../common/fixture_terminal.rs"]
mod fixture_terminal;
use crate::*;
use contracts::*;
use ledger::*;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sim_system::{execution::*, public_consumption::*};
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone, Default, Serialize)]
pub(crate) struct Counts {
    phase_calls: [u64; 4],
    phase_ns: [u128; 4],
    selection_input_ids: u64,
    selected_frames: u64,
    frames_with_statistics: u64,
    registry_calls: u64,
    registry_first: u64,
    registry_existing: u64,
}
thread_local! {static COUNTS:RefCell<Counts>=RefCell::new(Counts::default());}
pub(crate) fn phase(index: usize, elapsed: Duration) {
    COUNTS.with(|c| {
        let mut c = c.borrow_mut();
        c.phase_calls[index] += 1;
        c.phase_ns[index] += elapsed.as_nanos();
    });
}
pub(crate) fn selected(inputs: usize, frames: &[PublicObservationFrameV1<'_>]) {
    COUNTS.with(|c| {
        let mut c = c.borrow_mut();
        c.selection_input_ids += inputs as u64;
        c.selected_frames += frames.len() as u64;
        c.frames_with_statistics += frames.iter().filter(|f| f.statistics.is_some()).count() as u64;
    });
}
pub(crate) fn registered(first: bool) {
    COUNTS.with(|c| {
        let mut c = c.borrow_mut();
        c.registry_calls += 1;
        c.registry_first += u64::from(first);
        c.registry_existing += u64::from(!first);
    });
}
fn take() -> Counts {
    COUNTS.with(|c| std::mem::take(&mut *c.borrow_mut()))
}
fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|s| s.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn source_identity(root: &Path, times: &[i64]) -> Sha256Hex {
    fn files(root: &Path, paths: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files(&path, paths)
            } else {
                paths.push(path)
            }
        }
    }
    let mut paths = Vec::new();
    files(root, &mut paths);
    paths.sort();
    let mut hash = Sha256::new();
    for p in paths {
        hash.update(p.strip_prefix(root).unwrap().to_string_lossy().as_bytes());
        hash.update([0]);
        hash.update(std::fs::read(p).unwrap());
        hash.update([0]);
    }
    hash.update(serde_json::to_vec(times).unwrap());
    Sha256Hex(format!("{:x}", hash.finalize()))
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "#378 bounded release probe; explicit output directory, one before/after only"]
async fn finite_observation_scaling() {
    let root =
        PathBuf::from(std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("显式有限输出根"));
    std::fs::create_dir_all(&root).unwrap();
    let mut results = Vec::new();
    // A was measured once in 20-observation-before; preserve that original result.
    for (label, seen, responsibilities) in [("B", 128, 1), ("C", 128, 8)] {
        let cell = root.join(label);
        std::fs::create_dir(&cell).unwrap();
        let prep_started = Instant::now();
        let rss_before = rss_kib();
        let material = fixture_nav::recorded_observation_scaling(&cell.join("dpt"), seen);
        std::fs::write(
            cell.join("times.json"),
            serde_json::to_vec(&material.times.0).unwrap(),
        )
        .unwrap();
        let (mut execution, observation, rules) = fixture_execution_v4::execution_with_identity_v4(
            &cell.join("sim.jsonl"),
            &material.core,
            fixture_policy::SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "100",
            &format!("observation-{label}"),
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &cell.join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(
            &cell.join("dpt"),
            128,
            16 << 20,
            material.times.clone(),
        )
        .unwrap();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, format!("observation-{label}"));
        driver.enable_finite_recording_wait().unwrap();
        let config: PublicStatisticsConfigV1 = serde_json::from_str(include_str!(
            "../../../baseline-mm/configs/strategies/complementary_pair/public-statistics.json"
        ))
        .unwrap();
        let identity=PublicStatisticsIdentityV1 {
            input_set_identity:source_identity(&cell.join("dpt"),&material.times.0),
            stream_header:material.core.header().clone(),rules_identity:rules.file_sha256().clone(),
            delivery_policy:"RecordedPublicGroupSourceV1/TradingTimes:metadata10002/22000;books22000;closed66000;current71000+2n".into(),
        };
        driver
            .enable_public_statistics(
                data_pipeline_core::public_statistics::PublicStatisticsStreamV1::new(
                    identity,
                    config.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        take();
        for _ in 0..material.initial_groups {
            driver.next_group().unwrap().unwrap();
        }
        drop(
            shared
                .strategy_selected_current_observations(&driver, &[])
                .unwrap(),
        );
        let mut receipts = Vec::new();
        for (i, (condition, token)) in material
            .old_markets
            .iter()
            .take(responsibilities)
            .enumerate()
        {
            receipts.push(
                shared
                    .execute(
                        &mut driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: format!("old-{i}"),
                            caller_correlation_id: None,
                        },
                        Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                            condition_id: ConditionId(condition.clone()),
                            order: BuyLegIntentV1 {
                                token_id: TokenId(token.clone()),
                                order: BuyOrderIntentV1::GtcLimit {
                                    limit_price_pusd_per_share: Decimal::new(6, 1),
                                    size_shares: Decimal::from(10),
                                    post_only: false,
                                },
                            },
                        })),
                    )
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let mut feedback_reads = 0;
        for _ in material.initial_groups..material.before_updates {
            driver.next_group().unwrap().unwrap();
            shared.apply_closed().unwrap();
            feedback_reads += 1;
        }
        let public_only = driver.selected_frames_at_observation(std::iter::empty());
        assert_eq!(public_only.len(), 1);
        assert_eq!(
            public_only[0].observation.market.condition_id.0,
            material.current_market
        );
        drop(public_only);
        let frames = shared
            .strategy_selected_current_observations(&driver, &[])
            .unwrap();
        let selected_ids: Vec<_> = frames
            .iter()
            .map(|f| f.observation.market.condition_id.0.clone())
            .collect();
        let stats_present = frames.iter().filter(|f| f.statistics.is_some()).count();
        drop(frames);
        let cut = driver.observation_cutoff().unwrap();
        let private_before = cut.private_feedback_sequence();
        let ledger_before = shared.ledger().applied_entry_sequence();
        let read = shared.ledger().necessary_read_v2().unwrap();
        let held: Vec<_> = read
            .marked_markets()
            .map(|id| {
                (
                    id.to_owned(),
                    read.marked_position(id).unwrap().1.map(|q| q.to_string()),
                )
            })
            .collect();
        let accepted = shared
            .registry()
            .orders()
            .filter(|o| o.accepted.is_some())
            .count();
        let prepared = serde_json::json!({"seen":seen,"old_responsibilities":responsibilities,"selected":selected_ids,"accepted_orders":accepted,"held":held,"cash":read.cash(),"private_sequence":private_before,"ledger_sequence":ledger_before,"statistics_config":config,"statistics_frames":stats_present,"statistics_points":driver.public_statistics_prefix().map(|p|p.history_points()),"preparation_shared_observation_calls":2,"preparation_direct_selection_calls":1,"preparation_feedback_reads":feedback_reads,"preparation_public_groups":material.before_updates,"request_receipts":receipts});
        std::fs::write(
            cell.join("prepared.json"),
            serde_json::to_vec_pretty(&prepared).unwrap(),
        )
        .unwrap();
        assert_eq!(driver.public_observation_count(), seen);
        assert_eq!(accepted, responsibilities, "必须真实平台接纳");
        assert_eq!(held.len(), responsibilities, "必须真实Ledger责任");
        for (_, q) in &held {
            assert_eq!(
                q.iter()
                    .map(|x| x.parse::<Decimal>().unwrap())
                    .sum::<Decimal>(),
                Decimal::from(10)
            );
        }
        assert!(read.cash().0 > Decimal::ZERO, "100pUSD必须真实容纳原量");
        assert_eq!(selected_ids.len(), responsibilities + 1);
        assert_eq!(stats_present, selected_ids.len());
        assert!(selected_ids.windows(2).all(|p| p[0] < p[1]));
        let prep_ns = prep_started.elapsed().as_nanos();
        let preparation_counts = take();
        let rss_ready = rss_kib();
        let measured_started = Instant::now();
        let mut observation_ns = 0u128;
        for group in &material.update_groups {
            driver.next_group().unwrap().unwrap();
            let started = Instant::now();
            let frames = shared
                .strategy_selected_current_observations(&driver, &[])
                .unwrap();
            observation_ns += started.elapsed().as_nanos();
            assert_eq!(frames.len(), responsibilities + 1);
            assert!(
                frames
                    .iter()
                    .all(|f| f.statistics.is_some() && f.receipt.group().group_position == *group)
            );
            assert_eq!(
                frames
                    .iter()
                    .map(|f| f.observation.market.condition_id.0.clone())
                    .collect::<Vec<_>>(),
                selected_ids
            );
            drop(frames);
            assert_eq!(
                driver
                    .observation_cutoff()
                    .unwrap()
                    .private_feedback_sequence(),
                private_before,
                "计量中不能新增私人事实"
            );
            assert_eq!(
                shared.ledger().applied_entry_sequence(),
                ledger_before,
                "计量中真实资金/责任固定"
            );
        }
        let feed_and_measure_ns = measured_started.elapsed().as_nanos();
        let counts = take();
        let rss_after = rss_kib();
        let result = serde_json::json!({"case":label,"seen_markets":seen,"responsibility_markets":responsibilities,"selected_per_call":responsibilities+1,"preparation_ns":prep_ns,"preparation_counts":preparation_counts,"counts":counts,"observation_call_ns":observation_ns,"mean_observation_ns":observation_ns/512,"feed_assertions_and_observation_ns":feed_and_measure_ns,"rss_kib_samples":[rss_before,rss_ready,rss_after],"phase_order":["apply_closed","observe_public_rules","selection_and_statistics","observe_group"],"statistics_common_calculations":null,"statistics_common_scanned_points":null,"limits":"release lib-test; cfg(test) clocks/counters and ID counting included; no BTC input so common statistics unknown, not zero. Per-phase CPU/peak RSS Unknown; process measured externally. No strategy decisions or requests during512 measured observations."});
        std::fs::write(
            cell.join("measured.json"),
            serde_json::to_vec_pretty(&result).unwrap(),
        )
        .unwrap();
        assert_eq!(counts.phase_calls, [512; 4]);
        assert_eq!(counts.selected_frames, 512 * (responsibilities + 1) as u64);
        assert_eq!(counts.frames_with_statistics, counts.selected_frames);
        eprintln!("observation_scaling={result}");
        let teardown = Instant::now();
        driver.stop_and_drain().unwrap();
        std::fs::write(cell.join("teardown.json"),serde_json::to_vec_pretty(&serde_json::json!({"ns":teardown.elapsed().as_nanos(),"status":"stopped_and_recording_drained","extra_public_groups":0,"remaining_source_consumed":false,"market_terminal":"unknown","natural_eof":false,"run_seal":false,"shared_observation_calls":0,"counts":take(),"rss_kib":rss_kib()})).unwrap()).unwrap();
        results.push(result);
        std::fs::write(
            root.join("results.json"),
            serde_json::to_vec_pretty(&results).unwrap(),
        )
        .unwrap();
    }
}

#[cfg(feature = "observation-diagnostics")]
#[path = "observation_btc.rs"]
mod btc;
