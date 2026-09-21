async fn memory_execution_with_scenario_capacity<
    P: noe_projection::MemoryOrderEvidence + 'static,
>(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
    capacity: usize,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
) -> (
    PublicSimExecutionV1,
    noe_projection::MemoryNoeObservation<P>,
) {
    memory_execution_with_cash::<P>(path, scenario, role, capacity, rules, "10").await
}
async fn memory_execution_with_cash<P: noe_projection::MemoryOrderEvidence + 'static>(
    path: &Path, scenario: Arc<SealedPlatformScenarioV1>, role: SimCallerRoleV1,
    capacity: usize, rules: Option<Arc<SealedProcessRuleSnapshotV3>>, cash: &str,
) -> (PublicSimExecutionV1,noe_projection::MemoryNoeObservation<P>) {
    let rules = match rules {
        Some(rules) => rules,
        None => successful_snapshot_v3_for_sim_tests().await,
    };
    let finality = successful_policy_v2_for_tests();
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let mut opened = opened(
        "public-consumption-test",
        &rules,
        &finality,
        &scenario,
        &latency,
    );
    opened.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V3;
    let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
        source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
            path.display().to_string(),
        ),
    };
    let port = noe_projection::MemoryNoeProjectionPort::<P>::new(
        path.parent().unwrap().join("noe"),
        opened.clone(),
        reference.clone(),
        reference,
        "create-public".into(),
        "memory-noe-test".into(),
    );
    let observation = port.observation();
    let spec_opened = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id,
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut execution = PublicSimExecutionV1::create_new(
        path,
        spec_opened,
        rules,
        finality,
        scenario,
        latency,
        Box::new(port),
        MAX_GROUP_BYTES,
        capacity,
    )
    .unwrap();
    let mut session = spec("public-consumption-test", "create-public");
    session.caller_role = role;
    session.initial_account_state.available_pusd = ExactDecimal(cash.into());
    finite_feeding::feed(
        &mut execution,
        |execution| execution.create_session(session.clone()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )
    .unwrap();
    finite_feeding::feed(
        &mut execution,
        |execution| execution.activate_session("activate-public".into()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )
    .unwrap();
    (execution, observation)
}

fn close_at<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>(
    driver: &mut FinitePublicConsumptionV1<I>,
    at: i64,
) {
    let next = driver.last_receipt.as_ref().unwrap().group.group_position + 1;
    let closure = TrustedPublicTimeClosureV1 {
            execution_time: driver.time(at),
            consumption_sequence_ref: driver.consumption_sequence_ref.clone(),
            next_group_position: next,
            closure: PublicTimeClosureReasonV1::NextGlobalEventAt(if next <= 5 {
                26_000
            } else {
                TERMINAL_ADMITTED_TIME_MS
            }),
        };
    driver.execution.close_public_time(closure).unwrap();
    driver.drive_position += 1;
    driver.close_pending = false;
}

#[tokio::test(flavor = "current_thread")]
async fn real_memory_noe_delivers_closed_order_feedback_and_cancel_before_finish() {
    use ::execution_normalization::{
        MemoryIdentityKeyV1, NormalizationRecord, SourceNeutralEntityKeyV1,
        SourceNeutralEvidenceKind,
    };
    use contracts::execution_normalization::*;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let context = core.pm_context(CONDITION_ID).unwrap();
    let scenario = sealed_scenario_for_markets(vec![
        crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    ]);
    let path = temp.path().join("sim.jsonl");
    let (execution, observation) = memory_execution_with_scenario_capacity::<
        noe_projection::MeasuredOrderEvidence,
    >(&path, scenario, SimCallerRoleV1::Paper, 128, None)
    .await;
    let release = execution
        .actor
        .background_log
        .as_ref()
        .unwrap()
        .pause_for_tests(false);
    assert_eq!(observation.accepted_through(), 1);
    assert_eq!(observation.durable_through(), 0);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    driver.next_group().unwrap().unwrap();
    driver.next_group().unwrap().unwrap();
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "memory-buy".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "buy-item",
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into()),
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(2, 1),
                    size_shares: Decimal::from(5),
                    post_only: true,
                },
            ),
        }))
        .unwrap();
    for _ in 2..=4 {
        driver.next_group().unwrap().unwrap();
    }
    assert!(observation.take_closed_feedback().is_empty());
    close_at(&mut driver, 22_000);
    let due = driver
        .execution
        .actor
        .next_required_logical_drive()
        .unwrap()
        .required_at_utc_ms;
    close_at(&mut driver, due);
    let received = completed_post_response(&driver.execution.actor, "memory-buy")
        .unwrap()
        .metadata
        .response_received_at_utc_ms;
    assert!(received > due);
    assert_eq!(observation.delivered_through(), 0);
    close_at(&mut driver, received - 1);
    let before = observation.take_closed_feedback();
    assert!(
        before.iter().all(|feedback| !matches!(
            feedback.record().source_reference().record,
            SimV2SourceRecordReferenceV1::DirectResponse { .. }
        )),
        "未闭合响应接收时刻不可见"
    );
    close_at(&mut driver, received);
    assert!(
        observation.take_closed_feedback().is_empty(),
        "Paper 前缀关闭不能冒充公开消费时钟"
    );
    close_at(&mut driver, 25_999);
    assert!(observation.take_closed_feedback().is_empty());
    let (_, before_drain) = driver.next_group().unwrap().unwrap();
    assert_eq!(before_drain.private_feedback_sequence(), 0);
    let available = observation.take_closed_feedback();
    let delivered = available
        .iter()
        .filter(|feedback| {
            matches!(
                feedback.record().source_reference().record,
                SimV2SourceRecordReferenceV1::DirectResponse { .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(before.is_empty());
    assert!(!delivered.is_empty());
    assert!(
        delivered
            .iter()
            .all(|feedback| feedback.delivery_sequence() == 1)
    );
    assert!(delivered.iter().all(|feedback| matches!(
        feedback.record().source_reference().record,
        SimV2SourceRecordReferenceV1::DirectResponse {
            private_feedback_sequence: 2,
            ..
        }
    )));
    let order_identity = delivered
        .iter()
        .find_map(|feedback| match feedback.record() {
            NormalizationRecord::Evidence {
                evidence:
                    SourceNeutralEvidenceKind::Control {
                        result:
                            SourceNeutralControlResult::Submission {
                                outcome: SourceNeutralSubmissionOutcomeV2::Accepted { order, .. },
                            },
                    },
                ..
            } => Some(order.order_identity.clone()),
            _ => None,
        })
        .expect("真实 NOE 提交成功结果");
    let MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) =
        order_identity.key
    else {
        panic!("内存订单实体")
    };
    assert!(observation.delivered_through() > 0);
    assert_eq!(observation.durable_through(), 0);
    assert!(observation.result_references().is_empty());
    assert!(
        driver.execution.actor.pending_projections.is_empty(),
        "已完整移交的投影正文不再由 actor 重复保留"
    );
    let late = available
        .iter()
        .filter(|feedback| {
            matches!(
                feedback.record().source_reference().record,
                SimV2SourceRecordReferenceV1::OrderLifecycle { .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(
        late.len() >= 2,
        "同一订单事件同时携带生命周期与占用经济记录"
    );
    assert!(
        late.iter()
            .all(|feedback| feedback.delivery_sequence() == 2)
    );
    assert!(late.iter().all(|feedback| matches!(
        feedback.record().source_reference().record,
        SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence: 1,
            ..
        }
    )));
    assert_eq!(
        observation.delivered_through(),
        2,
        "晚到的先发射反馈仍推进一次交付位置"
    );
    assert!(observation.take_closed_feedback().is_empty());
    let cutoff = driver.decision().unwrap().cutoff().clone();
    assert_eq!(
        cutoff.private_feedback_sequence(),
        observation.delivered_through()
    );
    assert_eq!(before_drain.private_feedback_sequence(), 0);
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::CancelOrder(CancelOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "memory-cancel".into(),
                caller_correlation_id: None,
            },
            item: crate::execution::CancelOrderItemV1 {
                item_id: "cancel-item".into(),
                platform_order_id: source_order_id,
            },
        }))
        .unwrap();
    close_at(&mut driver, 26_000);
    assert_eq!(observation.durable_through(),0,"市场结束前暂停不阻断原业务链");
    release.send(()).unwrap();

    while let Some(required) = driver.execution.actor.next_required_logical_drive() {
        if required.required_at_utc_ms >= TERMINAL_ADMITTED_TIME_MS {
            break;
        }
        close_at(&mut driver, required.required_at_utc_ms);
    }
    while driver.next_group().unwrap().is_some() {}
    assert!(driver.execution.actor.unstable_market_ends.is_empty(),"已稳定市场截止不常驻历史集合");
    let accepted = observation.accepted_through();
    assert!(observation.durable_through()>0,"市场结束已自动稳定，无需等Finish");
    let mut materials = RecordedMaterials { root, hidden: None };
    driver.complete(&mut materials).unwrap();
    assert_eq!(observation.durable_through(), accepted);
    let references = observation.result_references();
    assert!(!references.is_empty());
    let resolved = references
        .iter()
        .map(::execution_normalization::read_noe_result_v3)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let deltas = resolved
        .iter()
        .filter_map(|value| value.record.economic_fact())
        .filter_map(|fact| match &fact.disposition {
            SourceNeutralEconomicDispositionV1::AccountTransition { balance_delta, .. } => {
                Some(balance_delta)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        deltas
            .iter()
            .any(|delta| delta.principal_encumbrance_pusd_delta.0 == "1")
    );
    assert!(
        deltas
            .iter()
            .any(|delta| delta.principal_encumbrance_pusd_delta.0 == "-1")
    );
    assert!(resolved.iter().any(|value| matches!(value.record.source_reference().record, SimV2SourceRecordReferenceV1::DirectResponse { private_feedback_sequence: 2, response_received_at_utc_ms, .. } if response_received_at_utc_ms == received)), "持久来源保持原发射序与真实接收时点");
    assert!(
        resolved
            .iter()
            .all(|value| value.boundary.durable_available_at_utc_ms > TERMINAL_ADMITTED_TIME_MS)
    );
    assert!(std::fs::read_to_string(path).unwrap().contains("RUN_SEAL"));
}

fn recorded_memory_complete(root: &Path) -> PublicObservationCoreV1 { recorded_memory_with_probe(root, false) }
fn recorded_memory_with_probe(root: &Path, probe: bool) -> PublicObservationCoreV1 {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let mut active = metadata_snapshot_row();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let active = record(
        1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
    );
    let mut inputs = vec![
        meta(0),
        active,
        pm(2, 1, 21_000, book("100")),
        pm(3, 2, 21_000, down),
        pm(
            4,
            3,
            21_001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80"))],
            },
        ),
        record(
            5,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                4,
                25_000,
                25_001,
                book("80"),
            ))),
        ),
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
        record(
            7,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        end(8),
    ];
    if probe {
        for input in &mut inputs[6..] {input.input_position+=1;}
        inputs.insert(6,record(6,TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",5,27_000,27_001,book("80"))))));
    }
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    core
}
struct MemoryRecordedTimes;
impl PublicConsumptionTimeSourceV1 for MemoryRecordedTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(match group.group_position {
            0 => SNAPSHOT_RECEIVED_AT,
            1..=4 => 22_000,
            5 => 26_000,
            _ => TERMINAL_ADMITTED_TIME_MS,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_memory_noe_stage_and_publish_failures_preserve_materials_without_seal() {
    for stable_publish in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_complete(&root);
        let context = core.pm_context(CONDITION_ID).unwrap();
        let scenario = sealed_scenario_for_markets(vec![
            crate::scenario::PlatformMarketV1::try_new(
                context.market,
                context
                    .assets
                    .into_iter()
                    .map(|a| a.asset)
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
            .unwrap(),
        ]);
        let path = temp.path().join("sim.jsonl");
        let (execution, observation) = memory_execution_with_scenario_capacity::<
            noe_projection::MeasuredOrderEvidence,
        >(&path, scenario, SimCallerRoleV1::Paper, 128, None)
        .await;
        // 此屏障前开户批已经真实暂存，后续写入暂停。
        let release = execution
            .actor
            .background_log
            .as_ref()
            .unwrap()
            .pause_for_tests(false);
        let context_directory = std::fs::read_dir(temp.path().join("noe"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let commits = context_directory.join("commits");
        let original_stage = commits.join(".00000000000000000000.parquet.tmp");
        let original_bytes = std::fs::read(&original_stage).unwrap();
        if stable_publish {
            std::fs::write(
                commits.join("00000000000000000000.parquet"),
                b"existing-target",
            )
            .unwrap();
        } else {
            std::fs::create_dir(commits.join(".00000000000000000001.parquet.tmp")).unwrap();
        }
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
        assert_eq!(observation.durable_through(),0);
        release.send(()).unwrap();
        let driving_error=loop {
            match driver.next_group() {Ok(Some(_))=>{},Ok(None)=>break None,Err(error)=>break Some(error)}
        };
        let error=if let Some(error)=driving_error {error} else {
            match driver.complete(&mut RecordedMaterials {root,hidden:None}) {Err(error)=>error,Ok(_)=>panic!("损坏现场不能完成")}
        };
        if stable_publish {
            assert!(matches!(
                error,
                SimRunActorErrorV2::Log(SimEventLogErrorV2::Noe(
                    ::execution_normalization::DurableErrorV1::CommitAlreadyExists {
                        commit_sequence: 0
                    }
                ))
            ));
            assert_eq!(
                std::fs::read(commits.join("00000000000000000000.parquet")).unwrap(),
                b"existing-target"
            );
        } else {
            assert!(matches!(
                error,
                SimRunActorErrorV2::Log(SimEventLogErrorV2::Noe(
                    ::execution_normalization::DurableErrorV1::Io {
                        operation: "写入归一化 Parquet 批",
                        ..
                    }
                ))
            ));
        }
        assert_eq!(
            std::fs::read(original_stage).unwrap(),
            original_bytes,
            "独立持有的原始批未被发布器删除"
        );
        assert_eq!(observation.durable_through(), 0);
        assert!(observation.result_references().is_empty());
        assert!(!std::fs::read_to_string(path).unwrap().contains("RUN_SEAL"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn finite_legacy_order_consumer_keeps_noe_v2_context_and_reader() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let context = core.pm_context(CONDITION_ID).unwrap();
    let scenario = sealed_scenario_for_markets(vec![
        crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    ]);
    let path = temp.path().join("sim.jsonl");
    // 与真实有限例程同一 LegacyOrderEvidence/v2 端口，而非将 Batch V3 自动升级成 NOE v3。
    let (execution, observation) = memory_execution_with_scenario_capacity::<
        noe_projection::LegacyOrderEvidence,
    >(&path, scenario, SimCallerRoleV1::Paper, 128, None)
    .await;
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    while driver.next_group().unwrap().is_some() {}
    assert!(observation.durable_through()>0,"市场结束前缀已自动稳定旧业务版本的新物理容器");
    assert_eq!(
        observation.delivered_through(),
        0,
        "无请求的有限例程没有虚构反馈交付"
    );
    driver
        .complete(&mut RecordedMaterials { root, hidden: None })
        .unwrap();
    for reference in observation.result_references() {
        let value = ::execution_normalization::read_noe_result_v2(&reference).unwrap();
        assert_eq!(value.context.contract_version, "execution_normalization_v2");
    }
    assert!(observation.durable_through() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn training_hidden_prefetch_cannot_advance_public_feedback_visibility_or_old_cutoff() {
    use contracts::execution_normalization::SimV2SourceRecordReferenceV1;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let context = core.pm_context(CONDITION_ID).unwrap();
    let scenario = sealed_scenario_for_markets(vec![
        crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    ]);
    let path = temp.path().join("sim.jsonl");
    let (execution, observation) = memory_execution_with_scenario_capacity::<
        noe_projection::MeasuredOrderEvidence,
    >(&path, scenario, SimCallerRoleV1::ModelTrain, 128, None)
    .await;
    let hidden = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden"),
        &config(),
        "memory-hidden-prefetch".into(),
    )
    .unwrap();
    let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let release = execution
        .actor
        .background_log
        .as_ref()
        .unwrap()
        .pause_for_tests(false);
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    driver.next_group().unwrap().unwrap();
    for at in [21_000, 21_001] {
        let bucket = buckets.next_bucket().unwrap().unwrap();
        assert_eq!(bucket.reference.bucket_time_utc_ms, at);
        driver.hidden_bucket(bucket).unwrap();
    }
    let (at_t, old_t_cutoff) = driver.next_group().unwrap().unwrap();
    assert_eq!(at_t.execution_time().logical_time_utc_ms(), 22_000);
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "memory-buy".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "buy-item",
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into()),
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(2, 1),
                    size_shares: Decimal::from(5),
                    post_only: true,
                },
            ),
        }))
        .unwrap();
    let bucket = buckets.next_bucket().unwrap().unwrap();
    let h = bucket.reference.bucket_time_utc_ms;
    assert_eq!(h, 25_000);
    driver.hidden_bucket(bucket).unwrap();
    let response = completed_post_response(&driver.execution.actor, "memory-buy").unwrap();
    let received = response.metadata.response_received_at_utc_ms;
    assert!(22_000 < received && received <= h);
    let future = observation.take_closed_feedback();
    if !future.is_empty() {
        release.send(()).unwrap();
        panic!(
            "旧实现反例：public T=22000，hidden H={h}，真实响应 R={received}；仅前缀闭合竟领取 {} 条未来 NOE 记录",
            future.len()
        );
    }
    let (_, same_time) = driver.next_group().unwrap().unwrap();
    assert_eq!(same_time.decision_time().logical_time_utc_ms(), 22_000);
    assert_eq!(same_time.private_feedback_sequence(), 0);
    assert!(observation.take_closed_feedback().is_empty());
    assert_eq!(
        driver
            .decision()
            .unwrap()
            .cutoff()
            .private_feedback_sequence(),
        0
    );
    for _ in 3..=4 {
        driver.next_group().unwrap().unwrap();
    }
    let (at_r, before_drain) = driver.next_group().unwrap().unwrap();
    assert_eq!(at_r.execution_time().logical_time_utc_ms(), 26_000);
    let frozen_frame = driver.frames()[0].decision_cutoff.clone();
    let delivered = observation.take_closed_feedback();
    assert!(!delivered.is_empty());
    assert_eq!(observation.delivered_through(), 2);
    let order_records = delivered
        .iter()
        .filter(|f| {
            matches!(
                f.record().source_reference().record,
                SimV2SourceRecordReferenceV1::OrderLifecycle {
                    private_feedback_sequence: 1,
                    ..
                }
            )
        })
        .collect::<Vec<_>>();
    assert!(order_records.len() >= 2);
    assert!(
        order_records
            .iter()
            .all(|f| f.delivery_sequence() == order_records[0].delivery_sequence())
    );
    assert!(observation.take_closed_feedback().is_empty());
    assert_eq!(observation.delivered_through(), 2);
    let new_cutoff = {
        let mut decision = driver.decision().unwrap();
        let cutoff = decision.cutoff().clone();
        assert_eq!(cutoff.receipt(), &at_r);
        assert_eq!(cutoff.decision_time(), at_r.execution_time());
        assert_eq!(cutoff.private_feedback_sequence(), 2);
        assert!(
            decision
                .frames()
                .iter()
                .all(|frame| frame.decision_cutoff == cutoff)
        );
        decision
            .request(BusinessRequestV2::CancelAllOrders(
                CancelAllOrdersRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: "training-visible-cancel".into(),
                        caller_correlation_id: None,
                    },
                },
            ))
            .unwrap();
        cutoff
    };
    for snapshot in [&old_t_cutoff, &same_time, &before_drain, &frozen_frame] {
        assert_eq!(snapshot.private_feedback_sequence(), 0);
    }
    assert_eq!(
        driver.frames()[0].decision_cutoff,
        frozen_frame,
        "驱动快照不被新 decision 回写"
    );
    assert_eq!(observation.durable_through(),0);
    release.send(()).unwrap();
    let delivered_before_end=observation.delivered_through();
    while let Some(bucket) = buckets.next_bucket().unwrap() {
        driver.hidden_bucket(bucket).unwrap();
    }
    assert!(driver.execution.actor.unstable_market_ends.is_empty());
    assert!(observation.durable_through()>0,"隐藏市场结束自动稳定");
    assert_eq!(observation.delivered_through(),delivered_before_end,"耐久稳定不前移公共反馈交付");
    while driver.next_group().unwrap().is_some() {}
    driver
        .complete(&mut RecordedMaterials {
            root,
            hidden: Some(hidden),
        })
        .unwrap();
    let (_, mut reader) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
    let mut saw_request = false;
    loop {
        let (next, group) = reader.next_group().unwrap();
        reader = next;
        let Some(group) = group else { break };
        for input in group {
            if let LoggedBusinessInputV2::PublicRequest { cutoff, request } = input {
                if request.context().global_request_id == "training-visible-cancel" {
                    assert_eq!(cutoff.into_trusted(), new_cutoff);
                    saw_request = true;
                }
            }
        }
    }
    assert!(saw_request);
    reader.finish_sealed_clean_run().unwrap();
    assert!(observation.result_references().iter().any(|reference| {
        let result = ::execution_normalization::read_noe_result_v3(reference).unwrap();
        matches!(result.record.source_reference().record, SimV2SourceRecordReferenceV1::DirectResponse { response_received_at_utc_ms, .. } if response_received_at_utc_ms == received)
    }));
}

#[cfg(feature="shared-execution")]
include!("shared_ledger_tests.rs");

struct SharedRecordedTimes;
impl PublicConsumptionTimeSourceV1 for SharedRecordedTimes {
fn execution_time_for_group(&mut self,group:&PublicGroupReferenceV1)->Result<i64,SimRunActorErrorV2>{Ok(match group.group_position {0=>SNAPSHOT_RECEIVED_AT,1..=4=>22_000,5=>26_000,6=>28_000,_=>TERMINAL_ADMITTED_TIME_MS})}
}

#[tokio::test(flavor = "current_thread")]
async fn empty_initial_markets_open_before_activation_and_do_not_forge_input_completion() {
    for complete in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("sim.jsonl");
        let rules = successful_snapshot_v3_for_sim_tests().await;
        let finality = successful_policy_v2_for_tests();
        let scenario = sealed_scenario_for_markets(Vec::new());
        let latency = load_embedded_proxy_latency_profile().unwrap();
        let mut header = opened(
            "public-consumption-test",
            &rules,
            &finality,
            &scenario,
            &latency,
        );
        header.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V3;
        header.market_event_source_identity_sha256 = None;
        let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
            source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
                path.display().to_string(),
            ),
        };
        let port =
            noe_projection::MemoryNoeProjectionPort::<noe_projection::MeasuredOrderEvidence>::new(
                temp.path().join("noe"),
                header.clone(),
                reference.clone(),
                reference,
                "create-public".into(),
                "empty-start".into(),
            );
        let observation = port.observation();
        let spec_opened = PublicRunOpenedV1 {
            projection_version: header.projection_version,
            run_id: header.run_id,
            platform_rule_snapshot_file_sha256: header.platform_rule_snapshot_file_sha256,
            trade_finality_policy_file_sha256: header.trade_finality_policy_file_sha256,
            platform_scenario_file_sha256: header.platform_scenario_file_sha256,
            proxy_latency_profile_file_sha256: header.proxy_latency_profile_file_sha256,
            implementation_identity: header.implementation_identity,
            market_event_source_identity_sha256: header.market_event_source_identity_sha256,
        };
        let mut execution = PublicSimExecutionV1::create_new(
            &path,
            spec_opened,
            rules,
            finality,
            scenario,
            latency,
            Box::new(port),
            MAX_GROUP_BYTES,
            8,
        )
        .unwrap();
        let release = execution
            .actor
            .background_log
            .as_ref()
            .unwrap()
            .pause_for_tests(false);
        let before = std::fs::read(&path).unwrap();
        assert!(observation.opening_record().is_none());
        let mut session = spec("public-consumption-test", "create-public");
        session.caller_role = SimCallerRoleV1::Paper;
        execution.create_session(session).unwrap();
        let opening = observation.opening_record().unwrap();
        assert_eq!(
            opening.visibility(),
            &::execution_normalization::NoeConsumptionVisibilityV1::Initialization
        );
        assert!(opening.durable_reference().is_none());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "开户不等待原后台编码或稳定"
        );
        let state = execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert_eq!(state.market_directory.markets().len(), 0);
        assert!(matches!(
            state.initialization,
            SessionInitializationV2::PreparedLedgerInit
        ));
        release.send(()).unwrap();
        execution.activate_session("activate-empty".into()).unwrap();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, std::iter::empty(), "empty-source".into());
        assert!(driver.next_group().unwrap().is_none());
        assert!(
            !driver
                .execution
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .public_visibility_frontier
                .sealed
        );
        if complete {
            assert!(matches!(
                driver.complete(&mut RecordedMaterials {
                    root: temp.path().join("unused"),
                    hidden: None
                }),
                Err(SimRunActorErrorV2::CompletionNotReady(
                    "市场输入路径尚未全部成功结束"
                ))
            ));
        } else {
            driver.stop_and_drain().unwrap();
            assert!(opening.durable_reference().is_some());
        }
        assert!(!std::fs::read_to_string(&path).unwrap().contains("RunSeal"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn empty_initial_markets_driver_enrollment_preserves_failures_idempotence_and_mode_freeze() {
    for role in [
        SimCallerRoleV1::Paper,
        SimCallerRoleV1::ModelTrain,
        SimCallerRoleV1::OfflineEvaluation,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_books(&root);
        let context = core.pm_context(CONDITION_ID).unwrap();
        let member = crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap();
        let mut different_market = member.market().clone();
        different_market.condition_id = ConditionId(format!("0x{}", "2".repeat(64)));
        different_market.clob_market_id = ClobMarketId("3195577".into());
        let mut assets = member.assets().clone();
        assets[0].token_id = TokenId("21".into());
        assets[1].token_id = TokenId("4".into());
        let different =
            crate::scenario::PlatformMarketV1::try_new(different_market.clone(), assets).unwrap();
        let (execution, _) =
            memory_execution_with_scenario_capacity::<noe_projection::MeasuredOrderEvidence>(
                &temp.path().join("sim.jsonl"),
                sealed_scenario_for_markets(Vec::new()),
                role,
                128,
                None,
            )
            .await;
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "enrollment".into());
        assert!(driver.rules.is_empty());
        assert_eq!(
            driver.enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "".into(),
                market: different.clone()
            }),
            Err(SimRunActorErrorV2::InvalidMarketEnrollment(
                "enrollment_request_id 为空或含首尾空白"
            ))
        );
        assert!(driver.rules.is_empty());
        let enrolled = driver
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "first".into(),
                market: member,
            })
            .unwrap();
        assert_eq!(driver.rules.len(), 1);
        let rule = Arc::clone(driver.rules.get(CONDITION_ID).unwrap());
        assert_eq!(
            driver
                .enroll_market(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "first".into(),
                    market: different.clone()
                })
                .unwrap(),
            enrolled
        );
        assert_eq!(driver.rules.len(), 1);
        assert!(!driver.rules.contains_key(&different_market.condition_id.0));
        assert!(Arc::ptr_eq(&rule, driver.rules.get(CONDITION_ID).unwrap()));
        driver.next_group().unwrap().unwrap();
        let later = driver.enroll_market(MarketEnrollmentRequestV1 {
            enrollment_request_id: "later".into(),
            market: different,
        });
        if role == SimCallerRoleV1::Paper {
            assert_eq!(later.unwrap().market, different_market);
            assert_eq!(driver.rules.len(), 2);
        } else {
            assert_eq!(
                later,
                Err(SimRunActorErrorV2::MarketEnrollmentFrozenAfterBusinessInput)
            );
            assert_eq!(driver.rules.len(), 1);
        }
        driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_idle_uses_real_noe_without_early_delivery() {
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_with_probe(&root, true);
    let context = core.pm_context(CONDITION_ID).unwrap();
    let scenario = sealed_scenario_for_markets(vec![
        crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    ]);
    let (execution, observation) =
        memory_execution_with_scenario_capacity::<noe_projection::MeasuredOrderEvidence>(
            &temp.path().join("sim.jsonl"),
            scenario,
            SimCallerRoleV1::Paper,
            128,
            None,
        )
        .await;
    let (mut driver, clock) = controlled_continuous(execution, 22_000);
    let mut source =
        RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    for _ in 0..5 {
        driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap();
    }
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "online-memory-buy".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "buy",
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into()),
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(2, 1),
                    size_shares: Decimal::from(5),
                    post_only: true,
                },
            ),
        }))
        .unwrap();
    let due = driver.required_at().unwrap();
    clock.store(due + 1, Ordering::SeqCst);
    driver.advance_due().unwrap();
    let received = completed_post_response(&driver.execution.actor, "online-memory-buy")
        .unwrap()
        .metadata
        .response_received_at_utc_ms;
    assert!(received > 22_000);
    clock.store((received + 1).max(26_000), Ordering::SeqCst);
    driver.advance_due().unwrap();
    assert!(observation.take_closed_feedback().is_empty());
    assert_eq!(
        driver
            .decision()
            .unwrap()
            .cutoff()
            .private_feedback_sequence(),
        0
    );
    let old_receipt = driver.last_receipt.as_ref().unwrap().clone();
    let receipt = driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap()
        .0;
    assert!(
        receipt.execution_time().logical_time_utc_ms()
            > old_receipt.execution_time().logical_time_utc_ms()
    );
    // 首个后继组只更新 visible，原关闭前缀仍未覆盖响应；不得承诺立即齐备。
    assert!(observation.take_closed_feedback().is_empty());
    clock.store((received + 2).max(28_000), Ordering::SeqCst);
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    assert!(!observation.take_closed_feedback().is_empty());
    assert!(
        driver
            .decision()
            .unwrap()
            .cutoff()
            .private_feedback_sequence()
            > 0
    );
    driver.stop_and_drain().unwrap();
}

fn recorded_late_five_minute_markets(
    root: &Path,
) -> (
    PublicObservationCoreV1,
    Vec<crate::scenario::PlatformMarketV1>,
) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    for position in 0..4 {
        let mut reference = twap_input(30);
        let received = 100_000 + position as i64;
        reference.common.timestamp_received = received;
        reference.common.message_timestamp = received - 1;
        reference.common.payload_timestamp = received - 2;
        reference.collector_admission_sequence = position + 1;
        reference.collector_admission_time_utc_ms = received + 1;
        let input = record(
            position,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference),
        );
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    let other = "0x2222222222222222222222222222222222222222222222222222222222222222";
    for (index, (condition, clob, assets, end_at)) in [
        (CONDITION_ID, CLOB_MARKET_ID, "[\"20\",\"3\"]", 600_000),
        (other, "3195577", "[\"21\",\"4\"]", 900_000),
    ]
    .into_iter()
    .enumerate()
    {
        let mut metadata = metadata_snapshot_row();
        let seen = (end_at - 50).min(400_000);
        metadata.condition_id = condition.into();
        metadata.clob_market_id = clob.into();
        metadata.asset_ids = assets.into();
        metadata.market_start_utc = end_at - 300_000;
        metadata.market_end_utc = end_at;
        metadata.timestamp_received = seen;
        metadata.collector_admission_sequence = index as u64 + 1;
        metadata.collector_admission_time_utc_ms = seen + 1;
        metadata.fixed_identity_verified_at_utc = seen - 1;
        metadata.metadata_fetch_start_utc = Some(seen - 10);
        metadata.source_evidence_received_at_utc = Some(seen);
        metadata.row_evidence_validation_completed_at_utc = Some(seen + 1);
        metadata.market_lifecycle_state = if seen < metadata.market_start_utc {
            MetadataMarketLifecycleState::Prefetched
        } else {
            MetadataMarketLifecycleState::Active
        };
        metadata.market_runtime_role = if seen < metadata.market_start_utc {
            MetadataMarketRuntimeRole::Next
        } else {
            MetadataMarketRuntimeRole::Current
        };
        let input = record(
            4 + index as u64,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
        );
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    let mut active = metadata_snapshot_row();
    active.condition_id = other.into();
    active.clob_market_id = "3195577".into();
    active.asset_ids = "[\"21\",\"4\"]".into();
    active.market_start_utc = 600_000;
    active.market_end_utc = 900_000;
    active.timestamp_received = 650_000;
    active.collector_admission_sequence = 3;
    active.collector_admission_time_utc_ms = 650_001;
    active.fixed_identity_verified_at_utc = 399_999;
    active.metadata_fetch_start_utc = Some(649_990);
    active.source_evidence_received_at_utc = Some(650_000);
    active.row_evidence_validation_completed_at_utc = Some(650_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let input = record(
        6,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
    );
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    for (index, token) in ["21", "4"].into_iter().enumerate() {
        let mut payload = book("100");
        if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut payload {
            *asset_id = token.into();
        }
        let mut raw = row(
            "late-markets",
            index as u64 + 1,
            650_010 + index as i64,
            650_011 + index as i64,
            payload,
        );
        raw.market = other.into();
        let input = record(
            7 + index as u64,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(raw)),
        );
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    for position in 9..12 {
        let mut reference = twap_input(30);
        let received = if position == 9 {
            650_015
        } else {
            920_000 + position as i64
        };
        reference.common.timestamp_received = received;
        reference.common.message_timestamp = received - 1;
        reference.common.payload_timestamp = received - 2;
        reference.collector_admission_sequence = position - 4;
        reference.collector_admission_time_utc_ms = reference.common.timestamp_received + 1;
        let input = record(
            position,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference),
        );
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let markets = [CONDITION_ID, other]
        .into_iter()
        .map(|condition| {
            let context = core.pm_context(condition).unwrap();
            assert_eq!(
                context.market.window.end_at_utc_ms - context.market.window.start_at_utc_ms,
                300_000
            );
            assert_eq!(context.market.window.start_at_utc_ms.rem_euclid(300_000), 0);
            crate::scenario::PlatformMarketV1::try_new(
                context.market,
                context
                    .assets
                    .into_iter()
                    .map(|a| a.asset)
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    (core, markets)
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_late_rebates_keep_due_but_use_actual_processing_time_and_real_noe_gates() {
    use contracts::execution_normalization::SimV2SourceRecordReferenceV1;
    use std::sync::atomic::Ordering;
    for mode in [
        "single-reference-timer",
        "closed-frontier-timer",
        "closed-frontier-group",
        "capacity-resume-timer",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (_, markets) = recorded_late_five_minute_markets(&root);
        let (execution, observation) =
            memory_execution_with_scenario_capacity::<noe_projection::MeasuredOrderEvidence>(
                &temp.path().join("sim.jsonl"),
                sealed_scenario_for_markets(vec![]),
                SimCallerRoleV1::Paper,
                if mode == "capacity-resume-timer" {
                    1
                } else {
                    128
                },
                None,
            )
            .await;
        let seed = execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .session_seed
            .clone();
        let latency = Arc::clone(&execution.actor._latency_profile);
        let (mut driver, clock) = controlled_continuous(execution, 900_100);
        let mut source =
            RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
        driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap();
        if mode != "single-reference-timer" {
            clock.store(900_101, Ordering::SeqCst);
            driver
                .apply_group(source.next().unwrap().unwrap().prepared)
                .unwrap();
            assert_eq!(
                driver
                    .execution
                    .actor
                    .state
                    .as_ref()
                    .unwrap()
                    .session
                    .as_ref()
                    .unwrap()
                    .public_visibility_frontier
                    .closed_through_utc_ms,
                Some(900_100)
            );
        }
        let frozen = driver.decision().unwrap().cutoff().clone();
        for (index, market) in markets.into_iter().enumerate() {
            driver.wait_for_recording_capacity().unwrap();
            driver
                .enroll_market(MarketEnrollmentRequestV1 {
                    enrollment_request_id: format!("late-{index}"),
                    market,
                })
                .unwrap();
        }
        let before = driver.execution.actor.applied_input_sequence;
        driver.advance_due().unwrap(); // now==c，不回关或忙转。
        assert_eq!(driver.execution.actor.applied_input_sequence, before);
        assert_eq!(
            driver.next_wake_after(),
            Some(std::time::Duration::from_millis(1))
        );
        let processed_at = if mode == "closed-frontier-group" {
            900_101
        } else {
            901_000
        };
        let resume = if mode == "capacity-resume-timer" {
            driver.wait_for_recording_capacity().unwrap();
            let release = driver
                .execution
                .actor
                .background_log
                .as_ref()
                .unwrap()
                .pause_for_tests(false);
            for id in ["capacity-first", "capacity-second"] {
                driver
                    .decision()
                    .unwrap()
                    .request(BusinessRequestV2::CancelAllOrders(
                        CancelAllOrdersRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: id.into(),
                                caller_correlation_id: None,
                            },
                        },
                    ))
                    .unwrap();
            }
            assert!(
                !driver
                    .execution
                    .actor
                    .background_log
                    .as_mut()
                    .unwrap()
                    .ready()
                    .unwrap()
            );
            // 先令 timer 确已到期，再在原容量释放前推进处理时钟，避免测试自身抢跑 now==c。
            clock.store(900_102, Ordering::SeqCst);
            let clock = Arc::clone(&clock);
            Some(std::thread::spawn(move || {
                clock.store(processed_at, Ordering::SeqCst);
                release.send(()).unwrap();
            }))
        } else {
            clock.store(processed_at, Ordering::SeqCst);
            None
        };
        if mode == "closed-frontier-group" {
            driver
                .apply_group(source.next().unwrap().unwrap().prepared)
                .unwrap();
        } else {
            driver.advance_due().unwrap();
        }
        if let Some(resume) = resume {
            resume.join().unwrap();
        }
        assert!(
            observation.take_closed_feedback().is_empty(),
            "{mode}: 不得借旧 visible 领取新生成反馈"
        );
        assert_eq!(
            driver
                .decision()
                .unwrap()
                .cutoff()
                .private_feedback_sequence(),
            0
        );
        assert_eq!(frozen.private_feedback_sequence(), 0);
        let sequence = driver.execution.actor.applied_input_sequence;
        driver.advance_due().unwrap();
        assert_eq!(
            driver.execution.actor.applied_input_sequence, sequence,
            "无新义务不重复经济结果"
        );
        clock.store(920_000, Ordering::SeqCst);
        driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap();
        // 第一组本身不保证关闭前沿已经覆盖接收时刻；第二组使原前缀真正闭合。
        let mut feedback = observation.take_closed_feedback();
        if feedback.is_empty() {
            clock.store(920_001, Ordering::SeqCst);
            // 此时参考组可能耗尽，后继真实元数据组已经沿原登记建立身份。
            driver
                .apply_group(source.next().unwrap().unwrap().prepared)
                .unwrap();
            feedback = observation.take_closed_feedback();
        }
        let mut due_times = Vec::new();
        for item in &feedback {
            if let SimV2SourceRecordReferenceV1::SettlementLifecycle {
                private_feedback_sequence,
                platform_event_at_utc_ms,
                feedback_received_at_utc_ms,
            } = item.record().source_reference().record
            {
                let delay = latency.sample_delay_ms(
                    seed.0.as_bytes(),
                    &private_feedback_sequence.to_string(),
                    DelaySampleDomainV1::LifecycleFeedbackInbound,
                );
                assert_eq!(
                    feedback_received_at_utc_ms,
                    processed_at + delay as i64,
                    "{mode}"
                );
                due_times.push(platform_event_at_utc_ms);
            }
        }
        due_times.sort();
        assert_eq!(due_times, vec![600_000, 900_000], "{mode}");
        assert!(observation.take_closed_feedback().is_empty());
        assert!(
            driver
                .decision()
                .unwrap()
                .cutoff()
                .private_feedback_sequence()
                > 0
        );
        assert_eq!(frozen.private_feedback_sequence(), 0);
        driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_normal_late_timer_preserves_due_delay_and_buy_between_rebates() {
    use contracts::execution_normalization::SimV2SourceRecordReferenceV1;
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (_, markets) = recorded_late_five_minute_markets(&root);
    let (execution, observation) =
        memory_execution_with_scenario_capacity::<noe_projection::MeasuredOrderEvidence>(
            &temp.path().join("sim.jsonl"),
            sealed_scenario_for_markets(vec![]),
            SimCallerRoleV1::Paper,
            128,
            None,
        )
        .await;
    let seed = execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .session_seed
        .clone();
    let latency = Arc::clone(&execution.actor._latency_profile);
    let (mut driver, clock) = controlled_continuous(execution, 500_000);
    let mut source =
        RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    for (index, market) in markets.into_iter().enumerate() {
        driver
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: format!("normal-{index}"),
                market,
            })
            .unwrap();
    }
    for _ in 1..6 {
        driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap();
    }
    clock.store(650_020, Ordering::SeqCst);
    for _ in 6..9 {
        driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap();
    }
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "between-rebates-buy".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "middle",
                &ConditionId(
                    "0x2222222222222222222222222222222222222222222222222222222222222222".into(),
                ),
                &TokenId("21".into()),
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(2, 1),
                    size_shares: Decimal::from(5),
                    post_only: true,
                },
            ),
        }))
        .unwrap();
    let due = driver.required_at().unwrap();
    assert!(600_000 < due && due < 900_000);
    clock.store(due, Ordering::SeqCst);
    let before = driver.execution.actor.applied_input_sequence;
    driver.advance_due().unwrap();
    assert_eq!(driver.execution.actor.applied_input_sequence, before);
    assert_eq!(
        driver.next_wake_after(),
        Some(std::time::Duration::from_millis(1))
    );
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    assert!(completed_post_response(&driver.execution.actor, "between-rebates-buy").is_none());
    clock.store(920_100, Ordering::SeqCst);
    driver.advance_due().unwrap();
    let response = completed_post_response(&driver.execution.actor, "between-rebates-buy").unwrap();
    assert!(matches!(
        response.item.outcome,
        PostOrderItemOutcomeV2::Accepted(_)
    ));
    assert_eq!(response.metadata.platform_responded_at_utc_ms, due);
    let buy_sequence = response.metadata.private_feedback_sequence;
    let mut records = observation.take_closed_feedback();
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    clock.store(920_101, Ordering::SeqCst);
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    records.extend(observation.take_closed_feedback());
    let mut settlement_sequences = Vec::new();
    for record in records {
        if let SimV2SourceRecordReferenceV1::SettlementLifecycle {
            private_feedback_sequence,
            platform_event_at_utc_ms,
            feedback_received_at_utc_ms,
        } = record.record().source_reference().record
        {
            let delay = latency.sample_delay_ms(
                seed.0.as_bytes(),
                &private_feedback_sequence.to_string(),
                DelaySampleDomainV1::LifecycleFeedbackInbound,
            );
            assert_eq!(
                feedback_received_at_utc_ms,
                platform_event_at_utc_ms + delay as i64,
                "普通 timer 虽晚处理，logical_at==due 时不改接收时间"
            );
            settlement_sequences.push((platform_event_at_utc_ms, private_feedback_sequence));
        }
    }
    settlement_sequences.sort();
    assert_eq!(settlement_sequences.len(), 2);
    assert_eq!(settlement_sequences[0].0, 600_000);
    assert_eq!(settlement_sequences[1].0, 900_000);
    assert!(settlement_sequences[0].1 < buy_sequence && buy_sequence < settlement_sequences[1].1);
    assert!(observation.take_closed_feedback().is_empty());
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn finite_paper_gap_closes_receipts_strictly_before_next_real_group() {
    use contracts::execution_normalization::SimV2SourceRecordReferenceV1;
    let mut original_received = None;
    for delta in [None, Some(-1), Some(0), Some(1)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_memory_complete(&root);
        let context = core.pm_context(CONDITION_ID).unwrap();
        let scenario = sealed_scenario_for_markets(vec![crate::scenario::PlatformMarketV1::try_new(
            context.market, context.assets.into_iter().map(|a| a.asset).collect::<Vec<_>>().try_into().unwrap(),
        ).unwrap()]);
        let (execution, observation) = memory_execution_with_scenario_capacity::<noe_projection::MeasuredOrderEvidence>(
            &temp.path().join("sim.jsonl"), scenario, SimCallerRoleV1::Paper, 128, None,
        ).await;
        let next_at = delta.map_or(26_000, |d| original_received.unwrap() + d);
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap()
            .enumerate().map(|(index, value)| value.map(|mut value| {
                if index == 5 { value.delivered_at_utc_ms = next_at; }
                value
            }));
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "receipt-gap".into());
        for _ in 0..5 { driver.next_group().unwrap().unwrap(); }
        driver.decision().unwrap().request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 { global_request_id: "gap-buy".into(), caller_correlation_id: None },
            item: post_item("buy", &ConditionId(CONDITION_ID.into()), &TokenId(UP_TOKEN_ID.into()),
                SimBuyOrderKindV1::GtcLimit { limit_price_pusd_per_share: Decimal::new(2,1), size_shares: Decimal::from(5), post_only: true }),
        })).unwrap();
        assert!(observation.take_closed_feedback().is_empty());
        let receipt = driver.next_group().unwrap().unwrap().0;
        assert_eq!(receipt.execution_time().logical_time_utc_ms(), next_at);
        let response = completed_post_response(&driver.execution.actor, "gap-buy").unwrap();
        let received = response.metadata.response_received_at_utc_ms;
        assert!(response.metadata.platform_responded_at_utc_ms < received);
        if let Some(expected) = original_received { assert_eq!(received, expected); }
        else { original_received = Some(received); }
        let records = observation.take_closed_feedback();
        let response_seen = records.iter().any(|r| matches!(r.record().source_reference().record,
            SimV2SourceRecordReferenceV1::DirectResponse { response_received_at_utc_ms, .. } if response_received_at_utc_ms == received));
        assert_eq!(response_seen, received < next_at, "=t及>t不得提前领取");
        assert_eq!(driver.execution.actor.state.as_ref().unwrap().session.as_ref().unwrap()
            .public_visibility_frontier.closed_through_utc_ms, Some(next_at - 1));
        driver.stop_and_drain().unwrap();
    }
}
