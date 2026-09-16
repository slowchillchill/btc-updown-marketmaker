// 有限真实 SIM 请求轨迹：旧同步 NOE 持有量观察，不设性能通过阈值。
use super::*;
use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralEconomicDispositionV1,
};
use execution_normalization::{
    DurableContextStoreV2, NormalizationRecordV2, SourceNeutralEvidenceKindV2,
};

struct ObservedSyncPort {
    inner: Rc<RefCell<SimV2NoeProjectionPortV2>>,
    // 仅测试保留最后一个请求周期的真实投影，以验证重复接纳。
    suffix: Rc<RefCell<Vec<Arc<BusinessProjectionBatchPayloadV1>>>>,
}
impl BusinessProjectionPortV2 for ObservedSyncPort {
    fn try_accept(&mut self, _: Arc<BusinessProjectionBatchV2>) -> bool {
        panic!("本轨迹固定采用 V3 投影、NOE v2")
    }
    fn try_accept_payload(&mut self, payload: Arc<BusinessProjectionBatchPayloadV1>) -> bool {
        if !self
            .inner
            .borrow_mut()
            .try_accept_payload(Arc::clone(&payload))
        {
            return false;
        }
        self.suffix.borrow_mut().push(payload);
        true
    }
    fn durable_through(&self, run_id: &str) -> u64 {
        self.inner.borrow().durable_through(run_id)
    }
}
fn disk_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                disk_bytes(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}
fn resident_memory() -> serde_json::Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let bytes = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            * 1024
    };
    serde_json::json!({"current_rss_bytes": bytes("VmRSS:"), "process_cumulative_vm_hwm_bytes": bytes("VmHWM:")})
}
fn advance_feedback(execution: &mut SimExecution, through: i64) {
    while let Some(required) = execution.next_required_logical_drive() {
        if required.required_at_utc_ms > through {
            break;
        }
        execution
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: required.path,
                closed_through_utc_ms: required.required_at_utc_ms,
                next_index_position: 2,
            })
            .unwrap();
    }
}
fn assert_economics(store: &DurableContextStoreV2, orders: usize) {
    let mut accepted = 0;
    let mut canceled = 0;
    let mut available = Decimal::ZERO;
    for record in store.commits().iter().flat_map(|commit| &commit.records) {
        if let Some(fact) = record.economic_fact() {
            if let SourceNeutralEconomicDispositionV1::AccountTransition {
                transition,
                balance_delta,
                ..
            } = &fact.disposition
            {
                match transition {
                    SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied { .. } => {
                        accepted += 1;
                        assert!(balance_delta.available_pusd_delta.0.parse::<Decimal>().unwrap() < Decimal::ZERO);
                    }
                    SourceNeutralAccountTransitionKindV1::CallerCancellationEncumbranceReleased { .. } => canceled += 1,
                    other => panic!("本轨迹未请求的经济效果 {other:?}"),
                }
                available += balance_delta
                    .available_pusd_delta
                    .0
                    .parse::<Decimal>()
                    .unwrap();
            }
        }
    }
    assert_eq!(
        (accepted, canceled, available),
        (orders, orders, Decimal::ZERO)
    );
    assert_eq!(store.retained_index_counts().economic, 1 + 2 * orders);
}

#[tokio::test(flavor = "current_thread")]
async fn finite_real_orders_observe_sync_noe_history_and_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = std::env::var_os("NOE_FINITE_OBSERVATION_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| temporary.path().to_path_buf());
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(SIM_EVENT_LOG_FILE_NAME_V2);
    let root = directory.join("noe");
    let rules = successful_snapshot_v3_for_sim_tests().await;
    let policy = successful_policy_v2_for_tests();
    let market = market_with_window('1', "123", "1", "2", 1_000, 10_000_000);
    let scenario = sealed_scenario_for_markets(vec![market.clone()]);
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let mut run_opened = opened("noe-finite-business", &rules, &policy, &scenario, &latency);
    run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
    run_opened.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V3;
    let evidence = DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(path.to_string_lossy().into_owned()),
    };
    let port = Rc::new(RefCell::new(SimV2NoeProjectionPortV2::new(
        &root,
        run_opened.clone(),
        evidence.clone(),
        evidence,
        "opening".into(),
        "finite-real-orders".into(),
    )));
    let suffix = Rc::new(RefCell::new(Vec::new()));
    let mut execution = SimExecution::create_new(
        &path,
        run_opened,
        rules,
        policy,
        scenario,
        latency,
        Box::new(ObservedSyncPort {
            inner: port.clone(),
            suffix: suffix.clone(),
        }),
        MAX_GROUP_BYTES,
    )
    .unwrap();
    let mut session = spec("noe-finite-business", "create-noe-finite-business");
    session.caller_role = SimCallerRoleV1::Paper;
    session.initial_account_state.available_pusd = ExactDecimal("1000".into());
    execution.create_session(session).unwrap();
    execution.activate_session("activate".into()).unwrap();
    execution
        .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
            reference: DurableMarketBucketReferenceV2 {
                path: MarketEventPathV1::PublicVisibility,
                bucket_at_utc_ms: 1_100,
                start_index_position: 0,
                next_index_position: 2,
                closure: MillisecondBucketClosureV1::NextEventAt(10_000_001),
            },
            events: Arc::from(decoded_initial_events_for_market_with_quantity(
                &market, 1_100, 1_110, 1, "100",
            )),
        })
        .unwrap();
    let first_reference = port.borrow().result_references()[0].clone();
    let first_result = execution_normalization::read_noe_result_v2(&first_reference).unwrap();
    let started = std::time::Instant::now();
    let mut measurements = Vec::new();
    let mut old_manifests = Vec::new();
    for order in 1..=64 {
        suffix.borrow_mut().clear();
        let now = 10_000 + (order as i64 - 1) * 20_000;
        execution
            .post_order(
                request_cutoff(now),
                PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: format!("post-{order}"),
                        caller_correlation_id: None,
                    },
                    item: post_item(
                        &format!("post-item-{order}"),
                        &market.market().condition_id,
                        &TokenId("1".into()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.4".parse().unwrap(),
                            size_shares: "20".parse().unwrap(),
                            post_only: false,
                        },
                    ),
                },
            )
            .unwrap();
        advance_feedback(&mut execution, now + 5_000);
        execution
            .cancel_order(
                request_cutoff(now + 10_000),
                CancelOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: format!("cancel-{order}"),
                        caller_correlation_id: None,
                    },
                    item: CancelOrderItemV1 {
                        item_id: format!("cancel-item-{order}"),
                        platform_order_id: format!("sim-{order}"),
                    },
                },
            )
            .unwrap();
        advance_feedback(&mut execution, now + 15_000);
        if [16, 32, 64].contains(&order) {
            let live = port.borrow();
            let store = live.store_for_resource_observation();
            assert_economics(store, order);
            let batch_count = store.commits().len();
            let record_count: usize = store
                .commits()
                .iter()
                .map(|commit| commit.records.len())
                .sum();
            let reference_count = live.result_references().len();
            let control_records = store
                .commits()
                .iter()
                .flat_map(|commit| &commit.records)
                .filter(|record| {
                    matches!(
                        record,
                        NormalizationRecordV2::Evidence {
                            evidence: SourceNeutralEvidenceKindV2::Control { .. },
                            ..
                        }
                    )
                })
                .count();
            let economic_records = store
                .commits()
                .iter()
                .flat_map(|commit| &commit.records)
                .filter(|record| record.economic_fact().is_some())
                .count();
            assert_eq!(control_records, 2 * order);
            assert_eq!(economic_records, 1 + 2 * order);
            assert_eq!(record_count, reference_count);
            let memory_before_reopen = resident_memory();
            let reopen_started = std::time::Instant::now();
            let reopened =
                DurableContextStoreV2::open(&root, &store.context().context_reference).unwrap();
            let reopen_us = reopen_started.elapsed().as_micros();
            assert_eq!(reopened.commits(), store.commits());
            assert_eq!(
                reopened.retained_index_counts(),
                store.retained_index_counts()
            );
            assert_economics(&reopened, order);
            let memory_with_two_stores = resident_memory();
            drop(reopened);
            assert_eq!(
                execution_normalization::read_noe_result_v2(&first_reference).unwrap(),
                first_result
            );
            let manifest = live.run_consumption_manifest().unwrap();
            let manifest_ref = execution_normalization::publish_noe_run_manifest_v1(
                directory.join(format!("manifest-{order}.json")),
                &manifest,
            )
            .unwrap();
            assert_eq!(
                execution_normalization::read_noe_run_manifest_v1(&manifest_ref).unwrap(),
                manifest
            );
            old_manifests.push((manifest_ref, manifest));
            measurements.push(serde_json::json!({
                "completed_post_cancel_cycles": order, "projection_batches": batch_count,
                "normalization_records": record_count, "control_records": control_records, "economic_records": economic_records, "result_references": reference_count,
                "necessary_core_indexes": store.retained_index_counts(), "noe_disk_bytes": disk_bytes(&root),
                "elapsed_ms_including_previous_observations": started.elapsed().as_millis(),
                "memory_before_reopen": memory_before_reopen, "memory_with_two_stores": memory_with_two_stores,
                "reopen_elapsed_us": reopen_us,
            }));
            let before = (batch_count, record_count, store.retained_index_counts());
            drop(live);
            for payload in suffix.borrow().iter() {
                assert!(port.borrow_mut().try_accept_payload(Arc::clone(payload)));
            }
            let live = port.borrow();
            let store = live.store_for_resource_observation();
            assert_eq!(
                (
                    store.commits().len(),
                    store
                        .commits()
                        .iter()
                        .map(|c| c.records.len())
                        .sum::<usize>(),
                    store.retained_index_counts()
                ),
                before
            );
            assert_economics(store, order);
        }
    }
    for (reference, manifest) in old_manifests {
        assert_eq!(
            execution_normalization::read_noe_run_manifest_v1(&reference).unwrap(),
            manifest
        );
    }
    let report = serde_json::json!({
        "trajectory": "SIM Paper real GTC post and cancel; batch v3, NOE contract/container v2",
        "scope": "same process, one live old synchronous port plus SIM; second store exists only during reopen; VmHWM is process cumulative; quantities are observations, not limits",
        "prefixes": measurements,
    });
    std::fs::write(
        directory.join("resource-observation.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("{report}");
}
