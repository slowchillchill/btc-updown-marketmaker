use super::*;
use std::collections::BTreeMap;
fn frozen(
    frame: &[PublicObservationFrameV1<'_>],
    timing: &MarketFrameTimingV1,
) -> FrozenStrategyMarketFrameV1 {
    FrozenStrategyMarketFrameV1 {
        timing: timing.clone(),
        markets: frame
            .iter()
            .map(|f| FrozenMarketReadV1 {
                statistics: f.statistics.clone(),
                observation: f.observation.clone(),
                rule_context: f.rule_context.clone(),
            })
            .collect(),
    }
}
fn equivalent(actual: &HostStrategyInputV1, original: &HostStrategyInputV1) {
    assert_eq!(actual.market_frame_timing, original.market_frame_timing);
    assert_eq!(
        actual.action_before.cutoff(),
        original.action_before.cutoff()
    );
    assert_eq!(
        actual.action_before.account.ledger,
        original.action_before.account.ledger
    );
    assert_eq!(
        actual.action_before.account.economics,
        original.action_before.account.economics
    );
    assert_eq!(
        actual.action_before.account.request_trades,
        original.action_before.account.request_trades
    );
    assert_eq!(
        actual.action_before.account.learning,
        original.action_before.account.learning
    );
    assert_eq!(
        actual.action_before.pending_noe_record_count(),
        original.action_before.pending_noe_record_count()
    );
    assert_eq!(
        actual.action_before.orders().collect::<Vec<_>>(),
        original.action_before.orders().collect::<Vec<_>>()
    );
    assert_eq!(
        actual.action_before.commitments().collect::<Vec<_>>(),
        original.action_before.commitments().collect::<Vec<_>>()
    );
    assert_eq!(
        actual.action_before.obligation_trades(),
        original.action_before.obligation_trades()
    );
    assert_eq!(
        actual.action_before.cancel_objects(),
        original.action_before.cancel_objects()
    );
    assert_eq!(actual.trade_economics, original.trade_economics);
    assert_eq!(actual.markets.len(), original.markets.len());
    for (actual, original) in actual.markets.iter().zip(&original.markets) {
        assert_eq!(actual.observation, original.observation);
        assert_eq!(actual.statistics, original.statistics);
        assert_eq!(actual.rule_context, original.rule_context);
    }
}
fn counters() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields = s
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    (fields[11].parse().unwrap(), fields[12].parse().unwrap())
}

type Watch = Arc<std::sync::Mutex<BTreeMap<u64, std::sync::Weak<ClosedBusinessGroupV1>>>>;
struct SharedSource<I> {
    inner: I,
    watch: Watch,
}
impl<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>> Iterator
    for SharedSource<I>
{
    type Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| {
            let delivery = result?;
            assert!(
                delivery
                    .prepared
                    .group
                    .observations
                    .shared_parts()
                    .is_some(),
                "正式Core输出必须为Shared表示"
            );
            self.watch.lock().unwrap().insert(
                delivery.prepared.group.group_position,
                Arc::downgrade(&delivery.prepared.group),
            );
            Ok(delivery)
        })
    }
}
type Driver = FinitePublicConsumptionV1<
    SharedSource<RecordedPublicGroupSourceV1<fixture_clock::TradingTimes>>,
>;
struct Lane {
    shared: SharedExecutionV2,
    driver: Driver,
    timing: MarketFrameTimingV1,
    markets: Vec<FrozenMarketReadV1>,
    watch: Watch,
}
impl Lane {
    fn input(&mut self, reference: bool, details: bool, learning: bool) -> HostStrategyInputV1 {
        let frame = FrozenStrategyMarketFrameV1 {
            timing: self.timing.clone(),
            markets: self.markets.clone(),
        };
        let requests = ["old-0".to_owned()];
        let input = if reference {
            self.shared
                .strategy_input_for_market_frame_with_learning_requests_reference_for_measurement(
                    &mut self.driver,
                    frame,
                    details,
                    learning,
                    Some(&requests),
                )
        } else {
            self.shared
                .strategy_input_for_market_frame_with_learning_requests(
                    &mut self.driver,
                    frame,
                    details,
                    learning,
                    Some(&requests),
                )
        }
        .unwrap()
        .unwrap();
        for (actual, frozen) in input.markets.iter().zip(&self.markets) {
            assert!(Arc::ptr_eq(&actual.rule_context, &frozen.rule_context));
        }
        input
    }
    fn group(&self) -> Arc<ClosedBusinessGroupV1> {
        let position = self
            .driver
            .observation_cutoff()
            .unwrap()
            .receipt()
            .group()
            .group_position;
        self.watch.lock().unwrap()[&position]
            .upgrade()
            .expect("当前原组仍由driver持有")
    }
}
async fn lane(
    root: &Path,
    material: &fixture_nav::ObservationScalingMaterial,
    seen: usize,
    statistics: PublicStatisticsConfigV1,
) -> Lane {
    std::fs::create_dir(root).unwrap();
    let (mut execution, observation, rules) = fixture_execution_v4::execution_with_identity_v4(
        &root.join("sim.jsonl"),
        &material.core,
        fixture_policy::SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
        &format!("assembly-{seen}"),
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    // 两路径各自打开同一份原记录、创建独立Core/统计流/账户；不共享物化缓存或统计查询缓存。
    let source_root = root.parent().unwrap().join("dpt");
    let source =
        RecordedPublicGroupSourceV1::open(&source_root, 128, 16 << 20, material.times.clone())
            .unwrap();
    let watch = Watch::default();
    let mut driver = FinitePublicConsumptionV1::new(
        execution,
        SharedSource {
            inner: source,
            watch: watch.clone(),
        },
        format!("assembly-{seen}"),
    );
    driver.enable_finite_recording_wait().unwrap();
    driver
        .enable_public_statistics(
            data_pipeline_core::public_statistics::PublicStatisticsStreamV1::new(
                PublicStatisticsIdentityV1 {
                    input_set_identity: source_identity(&source_root, &material.times.0),
                    stream_header: material.core.header().clone(),
                    rules_identity: rules.file_sha256().clone(),
                    delivery_policy: "original observation scaling TradingTimes".into(),
                },
                statistics,
            )
            .unwrap(),
        )
        .unwrap();
    for _ in 0..material.initial_groups {
        driver.next_group().unwrap().unwrap();
    }
    drop(
        shared
            .strategy_selected_current_observations(&driver, &[])
            .unwrap(),
    );
    let (condition, token) = &material.old_markets[0];
    shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "old-0".into(),
                caller_correlation_id: None,
            },
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(condition.clone()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(token.clone()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: 10.into(),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap()
        .unwrap();
    for _ in material.initial_groups..material.before_updates {
        driver.next_group().unwrap().unwrap();
        shared.apply_closed().unwrap();
    }
    let cutoff = driver.observation_cutoff().unwrap();
    let timing = MarketFrameTimingV1 {
        earliest_start_at_utc_ms: cutoff.decision_time().logical_time_utc_ms(),
        market_cutoff: cutoff,
    };
    let frames = shared
        .strategy_selected_current_observations(&driver, &[])
        .unwrap();
    assert_eq!(frames.len(), 2);
    assert!(frames.iter().all(|f| f.statistics.is_some()));
    let markets = frozen(&frames, &timing).markets;
    drop(frames);
    Lane {
        shared,
        driver,
        timing,
        markets,
        watch,
    }
}
fn statistics() -> PublicStatisticsConfigV1 {
    serde_json::from_str(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/public-statistics.json"
    ))
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn shared_current_prefix_iteration_and_account_freeze_never_materialize_complete_vec() {
    let root = tempfile::tempdir().unwrap();
    let material = fixture_nav::recorded_observation_scaling(&root.path().join("dpt"), 20);
    let mut lane = lane(&root.path().join("lane"), &material, 20, statistics()).await;
    for _ in 0..4 {
        lane.driver.next_group().unwrap().unwrap();
        let group = lane.group();
        let (_, shared) = group.observations.shared_parts().unwrap();
        let ids = lane
            .driver
            .observations_at_current_prefix_iter()
            .map(|o| {
                assert!(
                    std::ptr::eq(
                        o,
                        lane.driver
                            .observation_at_current_prefix(&o.market.condition_id.0)
                            .unwrap()
                    ),
                    "迭代器保留原selected对象"
                );
                o.market.condition_id.0.clone()
            })
            .collect::<Vec<_>>();
        assert!(ids.windows(2).all(|p| p[0] < p[1]));
        assert_eq!(ids.len(), 20);
        assert_eq!(
            lane.driver.public_rule_contexts().count(),
            20,
            "规则追赶直接迭代"
        );
        drop(lane.input(false, true, true));
        let before = Arc::strong_count(&shared.binance_depth);
        // 兼容切片API仍可显式物化。若上面任何路径已暗中物化，此处增量会是0。
        let slice = lane.driver.observations_at_current_prefix();
        assert_eq!(slice.len(), 20);
        assert_eq!(
            Arc::strong_count(&shared.binance_depth) - before,
            20,
            "账户/规则/直接迭代没有先创建完整Vec"
        );
        assert_eq!(
            slice
                .iter()
                .map(|o| o.market.condition_id.0.clone())
                .collect::<Vec<_>>(),
            ids
        );
        for observation in lane.driver.observations_at_current_prefix_iter() {
            assert_eq!(observation.group_position, group.group_position);
        }
    }
    lane.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit #392 independent-path 20/70-market first-prefix and silent-rolling comparison"]
async fn current_prefix_account_assembly_matches_old_frames_and_measures_components() {
    let root =
        PathBuf::from(std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("独立输出根"));
    std::fs::create_dir(&root).unwrap();
    for seen in [20, 70] {
        let cell = root.join(format!("markets-{seen}"));
        std::fs::create_dir(&cell).unwrap();
        let material = fixture_nav::recorded_observation_scaling(&cell.join("dpt"), seen);
        let config = statistics();
        let mut lanes = [
            lane(&cell.join("reference"), &material, seen, config.clone()).await,
            lane(&cell.join("candidate"), &material, seen, config.clone()).await,
        ];
        let initial = lanes[0].driver.observation_cutoff().unwrap();
        assert_eq!(initial, lanes[1].driver.observation_cutoff().unwrap());
        for (details, learning) in [(false, false), (true, false), (false, true), (true, true)] {
            for lane in &mut lanes {
                lane.driver.observe_at(66_001).unwrap().unwrap();
            }
            let old = lanes[0].input(true, details, learning);
            let new = lanes[1].input(false, details, learning);
            equivalent(&new, &old);
        }
        let mut measurements = Vec::new();
        for phase in ["silent_rolling", "new_prefix_first_read"] {
            let calls = if phase == "silent_rolling" { 128 } else { 64 };
            for trial in 0..3 {
                let before = strategy_input_assembly_work_v1();
                let cpu_before = counters();
                let mut wall = [0u128; 2];
                let mut feed = [0u128; 2];
                let rss_before = rss_kib();
                for index in 0..calls {
                    let mut inputs = [None, None];
                    for lane_index in if trial % 2 == 0 { [0, 1] } else { [1, 0] } {
                        let lane = &mut lanes[lane_index];
                        let started = Instant::now();
                        if phase == "silent_rolling" {
                            lane.driver
                                .observe_at(66_002 + trial * 128 + index)
                                .unwrap()
                                .unwrap();
                        } else {
                            lane.driver.next_group().unwrap().unwrap();
                        }
                        feed[lane_index] += started.elapsed().as_nanos();
                        let started = Instant::now();
                        let input = lane.input(lane_index == 0, false, false);
                        wall[lane_index] += started.elapsed().as_nanos();
                        inputs[lane_index] = Some(input);
                    }
                    let original = inputs[0].as_ref().unwrap();
                    let actual = inputs[1].as_ref().unwrap();
                    equivalent(actual, original);
                    assert!(
                        actual
                            .action_before
                            .cutoff()
                            .decision_time()
                            .logical_time_utc_ms()
                            > lane_time(&lanes[1])
                    );
                }
                let cpu_after = counters();
                let after = strategy_input_assembly_work_v1();
                assert_eq!(
                    lanes[0].driver.observation_cutoff().unwrap(),
                    lanes[1].driver.observation_cutoff().unwrap()
                );
                for (mode, lane_index) in [("reference_frames", 0), ("current_prefix", 1)] {
                    assert_eq!(
                        after.modes[mode].calls - before.modes[mode].calls,
                        calls as u64
                    );
                    assert_eq!(
                        after.modes[mode].sampled_calls - before.modes[mode].sampled_calls,
                        calls as u64 / 64
                    );
                    let stages=after.modes[mode].stages.iter().map(|(name,s)|{
                        let b=before.modes[mode].stages.get(name);(*name,serde_json::json!({"samples":s.count-b.map_or(0,|v|v.count),"sampled_total_ns":s.total_ns-b.map_or(0,|v|v.total_ns)}))}).collect::<BTreeMap<_,_>>();
                    let result = serde_json::json!({"trial":trial,"phase":phase,"mode":mode,"calls":calls,"observed_markets":seen,"independent_input_states":true,"shared_observation_representation":true,
                        "wall_ns_input_assembly_only":wall[lane_index],"feed_or_observe_ns":feed[lane_index],"paired_loop_user_ticks_including_feed_and_assertions":cpu_after.0-cpu_before.0,
                        "paired_loop_system_ticks_including_feed_and_assertions":cpu_after.1-cpu_before.1,"per_mode_cpu_unknown":true,"rss_kib_samples":[rss_before,rss_kib()],"sample_stride":after.sample_stride,"stages":stages,
                        "statistics_config":config,"cutoff":lanes[lane_index].driver.observation_cutoff().unwrap()});
                    eprintln!("input_assembly_cost={result}");
                    measurements.push(result);
                }
            }
        }
        std::fs::write(
            cell.join("measurements.json"),
            serde_json::to_vec_pretty(&measurements).unwrap(),
        )
        .unwrap();
        for lane in lanes {
            lane.driver.stop_and_drain().unwrap();
        }
    }
}
fn lane_time(lane: &Lane) -> i64 {
    lane.timing.earliest_start_at_utc_ms
}
