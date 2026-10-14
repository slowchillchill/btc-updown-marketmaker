// 真实终态行走原 DPT 准备与公共消费。身份快照和成交盘口为明确构造的有效夹具。
use super::*;
#[cfg(feature = "shared-execution")]
use crate::runtime_v2::BusinessProjectionVersionV1;

fn real_terminal_row(index: usize) -> PolymarketMarketMetadataRow {
    use data_pipeline_training::public_input::{PublicFiniteFileV1, open_finite_public_input_v1};
    let mut fragments: Vec<PublicFiniteFileV1> = serde_json::from_str(include_str!(
        "../tests/fixtures/settlement-real-fragments.json"
    ))
    .unwrap();
    let file = &mut fragments[index];
    file.path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(&file.path);
    let mut stream = open_finite_public_input_v1(file, 1, 1 << 20).unwrap();
    let mut rows = stream.next().unwrap().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(stream.next().is_none());
    let TypedRawRowV2::PolymarketMarketMetadataV6(row) = rows.pop().unwrap() else {
        panic!("真实V6元数据片段");
    };
    let PolymarketMarketMetadataV6Input::Existing(row) = *row else {
        panic!("真实终态行");
    };
    row
}
fn gamma_row() -> PolymarketMarketMetadataRow {
    real_terminal_row(0)
}
fn gamma_inputs(terminal: &PolymarketMarketMetadataRow) -> Vec<RecordedIngressV1> {
    let mut snapshot = metadata_snapshot_row();
    snapshot.condition_id = terminal.condition_id.clone();
    snapshot.clob_market_id = terminal.clob_market_id.clone();
    snapshot.asset_ids = terminal.asset_ids.clone();
    snapshot.slug = terminal.slug.clone();
    snapshot.market_start_utc = terminal.market_start_utc;
    snapshot.market_end_utc = terminal.market_end_utc;
    snapshot.timestamp_received = terminal.market_start_utc - 10_000;
    snapshot.collector_admission_time_utc_ms = snapshot.timestamp_received + 1;
    snapshot.fixed_identity_verified_at_utc = snapshot.timestamp_received - 1;
    snapshot.source_evidence_received_at_utc = Some(snapshot.timestamp_received);
    snapshot.row_evidence_validation_completed_at_utc = Some(snapshot.timestamp_received + 1);
    snapshot.metadata_fetch_start_utc = Some(snapshot.timestamp_received - 10);
    snapshot.outcomes_raw = terminal
        .outcomes_raw
        .clone()
        .or_else(|| Some(r#"["Up","Down"]"#.into()));
    let mut active = snapshot.clone();
    active.timestamp_received = terminal.market_start_utc;
    active.collector_admission_time_utc_ms = active.timestamp_received + 1;
    active.collector_admission_sequence = 2;
    active.source_evidence_received_at_utc = Some(active.timestamp_received);
    active.row_evidence_validation_completed_at_utc = Some(active.timestamp_received + 1);
    active.metadata_fetch_start_utc = Some(active.timestamp_received - 10);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut rows = vec![snapshot, active]
        .into_iter()
        .enumerate()
        .map(|(p, row)| {
            record(
                p as u64,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row))),
            )
        })
        .collect::<Vec<_>>();
    let tokens: Vec<String> = serde_json::from_str(&terminal.asset_ids).unwrap();
    for (i, token) in tokens.iter().enumerate() {
        let mut row = row(
            "gamma-trading",
            i as u64 + 1,
            terminal.market_start_utc + 1000,
            terminal.market_start_utc + 1000,
            book("100"),
        );
        row.market = terminal.condition_id.clone();
        row.slug = Some(terminal.slug.clone());
        if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut row.event {
            *asset_id = token.clone();
        }
        rows.push(record(
            rows.len() as u64,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
        ));
    }
    rows.push(record(
        rows.len() as u64,
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::Existing(terminal.clone()),
        )),
    ));
    rows.push(end(rows.len() as u64));
    rows
}
fn record_gamma(root: &Path, terminal: &PolymarketMarketMetadataRow) -> PublicObservationCoreV1 {
    let mut inputs = gamma_inputs(terminal);
    inputs.pop();
    let mut duplicate = terminal.clone();
    duplicate.collector_admission_sequence += 1;
    duplicate.timestamp_received += 1;
    duplicate.collector_admission_time_utc_ms += 1;
    duplicate.source_evidence_received_at_utc = Some(duplicate.timestamp_received);
    duplicate.row_evidence_validation_completed_at_utc = duplicate
        .row_evidence_validation_completed_at_utc
        .map(|t| t + 1);
    duplicate.lifecycle_applied_at_utc = duplicate.lifecycle_applied_at_utc.map(|t| t + 1);
    duplicate.runtime_role_applied_at_utc = duplicate.runtime_role_applied_at_utc.map(|t| t + 1);
    inputs.push(record(
        inputs.len() as u64,
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::Existing(duplicate),
        )),
    ));
    inputs.push(end(inputs.len() as u64));
    record_gamma_inputs(root, inputs)
}
fn record_gamma_inputs(root: &Path, inputs: Vec<RecordedIngressV1>) -> PublicObservationCoreV1 {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    core
}
fn scenario_for_gamma(
    core: &PublicObservationCoreV1,
    condition: &str,
) -> Arc<SealedPlatformScenarioV1> {
    let context = core.pm_context(condition).unwrap();
    sealed_scenario_for_markets(vec![
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
    ])
}
fn gamma_redemptions(delivered: &DeliveredFeedbackState) -> Vec<SettlementLifecycleFeedbackV2> {
    delivered
        .settlements
        .borrow()
        .iter()
        .filter(|f| {
            matches!(
                f.event,
                SettlementLifecycleEventV2::AutoRedeemApplied { .. }
            )
        })
        .cloned()
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn actual_gamma_only_takes_effect_after_successful_consumption_and_hidden_closure() {
    for policy in [
        GammaResolutionPolicyV1::SourceTimeOnly,
        GammaResolutionPolicyV1::QualifiedPublicConsumption,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let terminal = gamma_row();
        let core = record_gamma(&root, &terminal);
        let scenario = scenario_for_gamma(&core, &terminal.condition_id);
        let hidden = prepare_hidden_v1(
            core,
            PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
            &temp.path().join("hidden"),
            &config(),
            "gamma".into(),
        )
        .unwrap();
        let (mut execution, delivered) = public_execution_with_scenario_capacity_layout(
            &temp.path().join("sim.jsonl"),
            scenario,
            SimCallerRoleV1::HistoricalBacktest,
            32,
            None,
            None,
            true,
        )
        .await;
        execution
            .configure_gamma_resolution_policy_v1(policy)
            .unwrap();
        let mut source =
            RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
        let source = std::iter::from_fn(move || source.next_visible_group());
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "gamma".into());
        driver.enable_finite_recording_wait().unwrap();
        let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
        while let Some(bucket) = buckets.next_bucket().unwrap() {
            let at = bucket.reference.bucket_time_utc_ms();
            while driver
                .next_group_at()
                .unwrap()
                .is_some_and(|next| next <= at)
            {
                driver.next_group().unwrap().unwrap();
            }
            driver.hidden_bucket(bucket).unwrap();
        }
        assert!(gamma_redemptions(&delivered).is_empty());
        let consumed = loop {
            assert!(gamma_redemptions(&delivered).is_empty());
            let (receipt, _) = driver.next_group().unwrap().unwrap();
            if driver
                .last_prepared
                .as_ref()
                .unwrap()
                .group
                .records
                .iter()
                .any(|fact| {
                    matches!(
                        fact.payload,
                        PublicBusinessPayloadV1::MarketFact {
                            payload: CanonicalMarketFactPayloadV1::Terminal(_),
                            ..
                        }
                    )
                })
            {
                break receipt.execution_time().logical_time_utc_ms();
            }
        };
        assert!(consumed >= terminal.row_evidence_validation_completed_at_utc.unwrap());
        assert!(consumed > terminal.timestamp_received);
        assert!(
            gamma_redemptions(&delivered).is_empty(),
            "公开组不能提前闭合隐藏同毫秒"
        );
        let session = driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert_eq!(
            session.pending_gamma_resolutions.len(),
            usize::from(policy == GammaResolutionPolicyV1::QualifiedPublicConsumption)
        );
        // 真实序列化恢复保留原经济状态及待处理终态；旧字段不存在时保持旧政策。
        let mut state_bytes = Vec::new();
        ciborium::ser::into_writer(
            driver.execution.actor.state.as_ref().unwrap(),
            &mut state_bytes,
        )
        .unwrap();
        if policy.is_source_time_only() {
            let mut wire: ciborium::Value =
                ciborium::de::from_reader(state_bytes.as_slice()).unwrap();
            let ciborium::Value::Map(fields) = &mut wire else {
                unreachable!()
            };
            let session = fields
                .iter_mut()
                .find(|(key, _)| key.as_text() == Some("session"))
                .unwrap();
            let ciborium::Value::Map(fields) = &mut session.1 else {
                unreachable!()
            };
            fields.retain(|(key, _)| {
                !matches!(
                    key.as_text(),
                    Some("gamma_resolution_policy" | "pending_gamma_resolutions")
                )
            });
            state_bytes.clear();
            ciborium::ser::into_writer(&wire, &mut state_bytes).unwrap();
        }
        driver.execution.actor.state =
            Some(ciborium::de::from_reader(state_bytes.as_slice()).unwrap());
        // 与原有限宿主相同：先闭合严格更早的到期义务，不能跨过返佣市场前驱。
        driver.advance_before(consumed).unwrap();
        let next_index_position = driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .hidden_platform_frontier
            .next_index_position;
        driver
            .execution
            .actor
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: MarketEventPathV1::HiddenPlatform,
                closed_through_utc_ms: consumed,
                next_index_position,
            })
            .unwrap();
        let feedback = gamma_redemptions(&delivered);
        if policy.is_source_time_only() {
            assert!(feedback.is_empty());
        } else {
            assert_eq!(feedback.len(), 1);
            let SettlementLifecycleEventV2::AutoRedeemApplied {
                resolution_source_event_at_utc_ms,
                resolution_public_consumed_at_utc_ms,
                redeemed_pusd,
                ..
            } = &feedback[0].event
            else {
                unreachable!()
            };
            assert_eq!(*resolution_source_event_at_utc_ms, None);
            assert_eq!(*resolution_public_consumed_at_utc_ms, Some(consumed));
            assert_eq!(feedback[0].metadata.platform_event_at_utc_ms, consumed);
            assert_eq!(redeemed_pusd.0, "0");
        }
        let applied_sequence = driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .execution_state
            .next_account_transition_seq();
        let mut paid_state = Vec::new();
        ciborium::ser::into_writer(
            driver.execution.actor.state.as_ref().unwrap(),
            &mut paid_state,
        )
        .unwrap();
        driver.execution.actor.state =
            Some(ciborium::de::from_reader(paid_state.as_slice()).unwrap());
        // 原终态归约器仅报告同义重复，不生成新的公共事件或消费组。
        assert!(driver.next_group().unwrap().is_none());
        assert_eq!(
            gamma_redemptions(&delivered).len(),
            usize::from(!policy.is_source_time_only())
        );
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
                .execution_state
                .next_account_transition_seq(),
            applied_sequence
        );
        driver.stop_and_drain().unwrap();
    }
}

#[cfg(feature = "shared-execution")]
#[tokio::test(flavor = "current_thread")]
async fn gamma_same_millisecond_finality_and_full_checkpoint_pay_five_once() {
    use contracts::execution_normalization::{
        DurableSourceReferenceV1, ImmutableSourceReferenceV1,
    };
    use ledger::{AccountAdmissionPolicyV3, LedgerWriterV1};
    let temp = tempfile::tempdir().unwrap();
    let terminal = gamma_row();
    // 延迟交付真实 Gamma；与原已 MATCHED 成交的 MINED 时钟及隐藏行情桶同毫秒。
    // 不把该夹具的延迟消费时刻伪装成真实接收 .047 或平台源时间。
    let consumed = terminal.market_start_utc + 1_200_000;
    let mut inputs = gamma_inputs(&terminal);
    inputs.pop();
    let mut probe = twap_input(60);
    probe.common.timestamp_received = consumed + 20;
    probe.common.message_timestamp = consumed;
    probe.common.payload_timestamp = consumed;
    probe.common.age_ms = Some(20);
    probe.collector_admission_time_utc_ms = consumed + 21;
    inputs.push(record(
        inputs.len() as u64,
        TypedRawRowV2::PolymarketRtdsChainlinkTwap(probe),
    ));
    inputs.push(end(inputs.len() as u64));
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let scenario = scenario_for_gamma(&core, &terminal.condition_id);
    let hidden = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden"),
        &config(),
        "gamma-five".into(),
    )
    .unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut groups = Vec::new();
    while let Some(group) = source.next_visible_group() {
        groups.push(group.unwrap());
    }
    let gamma_position = groups
        .iter()
        .position(|g| {
            g.prepared.group.records.iter().any(|fact| {
                matches!(
                    fact.payload,
                    PublicBusinessPayloadV1::MarketFact {
                        payload: CanonicalMarketFactPayloadV1::Terminal(_),
                        ..
                    }
                )
            })
        })
        .unwrap();
    groups[gamma_position].delivered_at_utc_ms = consumed;
    let decision_at = groups[gamma_position - 1].delivered_at_utc_ms;
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let mut session = spec("gamma-five", "create-gamma-five");
    session.caller_role = SimCallerRoleV1::HistoricalBacktest;
    let matched_at = decision_at
        + i64::try_from(latency.sample_delay_ms(
            session.session_seed.0.as_bytes(),
            "gamma-buy",
            DelaySampleDomainV1::RequestOutbound,
        ))
        .unwrap();
    let mined_offset = consumed - matched_at;
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&serde_json::to_vec(&serde_json::json!({
        "schema_version":"trade_finality_policy_v2", "normal_path": {
            "steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":mined_offset},{"state":"CONFIRMED","offset_ms":mined_offset+10}],
            "economic_application_ordinal":1}, "failure_overrides":[]
    })).unwrap()).unwrap());
    let rules = successful_snapshot_v3_for_sim_tests().await;
    let mut opened = opened("gamma-five", &rules, &finality, &scenario, &latency);
    opened.projection_version = BusinessProjectionVersionV1::V4;
    let log = temp.path().join("sim.jsonl");
    let reference = DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(log.display().to_string()),
    };
    let port = MemoryNoeProjectionPortV4::new(
        temp.path().join("noe"),
        opened.clone(),
        reference.clone(),
        reference,
        "create-gamma-five".into(),
        "gamma-five".into(),
    );
    let observation = port.observation();
    let public_opened = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id,
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut execution = PublicSimExecutionV1::create_new_historical(
        &log,
        public_opened,
        rules.clone(),
        finality.clone(),
        scenario.clone(),
        latency.clone(),
        Box::new(port),
        MAX_GROUP_BYTES,
        32,
    )
    .unwrap();
    execution.create_session(session).unwrap();
    execution
        .activate_session("activate-gamma-five".into())
        .unwrap();
    execution
        .configure_gamma_resolution_policy_v1(GammaResolutionPolicyV1::QualifiedPublicConsumption)
        .unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    ledger
        .apply_noe_memory_v2(&observation.opening_record().unwrap())
        .unwrap();
    let mut driver = FinitePublicConsumptionV1::new(
        execution,
        groups.clone().into_iter().map(Ok),
        "gamma-five".into(),
    );
    driver.enable_finite_recording_wait().unwrap();
    let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let first = buckets.next_bucket().unwrap().unwrap();
    assert_eq!(
        first.reference.bucket_time_utc_ms(),
        terminal.market_start_utc + 1000
    );
    while driver
        .next_group_at()
        .unwrap()
        .is_some_and(|at| at <= first.reference.bucket_time_utc_ms())
    {
        driver.next_group().unwrap().unwrap();
    }
    driver.hidden_bucket(first).unwrap();
    for _ in 0..gamma_position - 2 {
        driver.next_group().unwrap().unwrap();
    }
    assert_eq!(
        driver
            .last_receipt
            .as_ref()
            .unwrap()
            .execution_time()
            .logical_time_utc_ms(),
        decision_at
    );
    let receipt = driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "gamma-buy".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "gamma-buy",
                &ConditionId(terminal.condition_id.clone()),
                &TokenId(terminal.winning_asset_id.clone().unwrap()),
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(6, 1),
                    size_shares: Decimal::from(5),
                    post_only: false,
                },
            ),
        }))
        .unwrap();
    assert_eq!(receipt.platform_arrived_at_utc_ms, matched_at);
    driver.advance_before(consumed).unwrap();
    assert_eq!(
        driver
            .next_required_logical_drive()
            .unwrap()
            .required_at_utc_ms,
        consumed
    );
    driver.next_group().unwrap().unwrap();
    let state = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert_eq!(state.pending_gamma_resolutions.len(), 1);
    assert!(
        state
            .execution_state
            .redeemed_market_resolution(&terminal.condition_id)
            .is_none()
    );
    let mut saved = Vec::new();
    driver.write_checkpoint(&mut saved).unwrap();
    let mut saved_ledger = Vec::new();
    ledger.write_checkpoint(&mut saved_ledger).unwrap();
    driver.stop_and_drain().unwrap();
    let (state, observation) = PublicConsumptionStateV1::restore_checkpoint(
        saved.as_slice(),
        rules,
        finality,
        scenario,
        latency,
        MAX_GROUP_BYTES,
        32,
    )
    .unwrap();
    let mut driver = FinitePublicConsumptionV1::from_checkpoint_state(
        state,
        groups[gamma_position + 1..].to_vec().into_iter().map(Ok),
    );
    driver.enable_finite_recording_wait().unwrap();
    ledger = LedgerWriterV1::restore_checkpoint(saved_ledger.as_slice()).unwrap();
    let same_ms = buckets.next_bucket().unwrap().unwrap();
    assert_eq!(same_ms.reference.bucket_time_utc_ms(), consumed);
    driver.hidden_bucket(same_ms).unwrap();
    let state = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert!(state.pending_gamma_resolutions.is_empty());
    assert!(
        state
            .execution_state
            .redeemed_market_resolution(&terminal.condition_id)
            .is_none(),
        "MINED不是CONFIRMED，不能提前释放"
    );
    let cash_before = state.execution_state.account_balances_for_tests().0;
    // 宿主先完成范围尾观察再结束公开输入；不能先用 next_group(None)封口后再观察。
    while driver.next_group_at().unwrap().is_some() {
        driver.next_group().unwrap().unwrap();
    }
    driver.finish_input_at(consumed + 60_000).unwrap();
    let cash_after = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .execution_state
        .account_balances_for_tests()
        .0;
    assert_eq!(cash_after - cash_before, Decimal::from(5));
    let records = observation.take_closed_records();
    let mut redemptions = 0;
    for record in records {
        if let ::execution_normalization::NormalizationRecord::Evidence { evidence, .. } =
            record.record()
        {
            if let ::execution_normalization::SourceNeutralEvidenceKind::SettlementLifecycle { result: contracts::execution_normalization::SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied { resolution_source_event_at_utc_ms, resolution_public_consumed_at_utc_ms, .. }, .. } = evidence {
                assert_eq!(*resolution_source_event_at_utc_ms, None);
                assert_eq!(*resolution_public_consumed_at_utc_ms, Some(consumed));
                redemptions += 1;
            }
        }
        ledger.apply_noe_memory_v2(&record).unwrap();
    }
    assert_eq!(redemptions, 1);
    assert_eq!(
        ledger
            .projection_v2()
            .unwrap()
            .platform_confirmed_projection
            .cash_pusd
            .0,
        cash_after.normalize().to_string()
    );
    assert_eq!(
        ledger
            .economic_read()
            .unwrap()
            .market_totals()
            .next()
            .unwrap()
            .settlement_state,
        ledger::EconomicSettlementStateV1::Redeemed
    );
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_outside_window_ws_result_stays_unconsumed() {
    let terminal = real_terminal_row(1);
    let cutoff = terminal.market_end_utc - 1; // 原00:00—04:00范围的最后允许毫秒。
    let source_time = terminal.ws_platform_timestamp_utc.unwrap();
    assert_eq!(source_time, 1_789_185_714_026);
    assert_eq!(terminal.timestamp_received, 1_789_185_714_033);
    assert!(source_time > cutoff);
    let tokens: Vec<String> = serde_json::from_str(&terminal.asset_ids).unwrap();
    let market = MarketIdentityV2 {
        condition_id: ConditionId(terminal.condition_id.clone()),
        clob_market_id: ClobMarketId(terminal.clob_market_id.clone()),
        window: MarketWindowV2 {
            start_at_utc_ms: terminal.market_start_utc,
            end_at_utc_ms: terminal.market_end_utc,
        },
    };
    let mut ws = terminal_orderbook_row(&market);
    ws.timestamp = Some(source_time);
    ws.timestamp_received = terminal.timestamp_received;
    ws.collector_admission_time_utc_ms = terminal.collector_admission_time_utc_ms;
    ws.source_sequence_epoch_id = terminal
        .ws_claimant_source_sequence_epoch_id
        .clone()
        .unwrap();
    ws.collector_admission_sequence = terminal.ws_claimant_collector_admission_sequence.unwrap();
    if let PolymarketOrderbookEvent::MarketResolved {
        asset_ids,
        winning_asset_id,
        winning_outcome,
        ..
    } = &mut ws.event
    {
        *asset_ids = tokens;
        *winning_asset_id = terminal.winning_asset_id.clone().unwrap();
        *winning_outcome = terminal.winning_outcome.clone().unwrap();
    }
    let mut inputs = gamma_inputs(&terminal);
    inputs.pop();
    inputs.push(record(
        inputs.len() as u64,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
    ));
    inputs.push(end(inputs.len() as u64));
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = record_gamma_inputs(&root, inputs);
    let scenario = scenario_for_gamma(&core, &terminal.condition_id);
    let hidden = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden"),
        &config(),
        "outside-ws".into(),
    )
    .unwrap();
    let (mut execution, delivered) = public_execution_with_scenario_capacity_layout(
        &temp.path().join("sim.jsonl"),
        scenario,
        SimCallerRoleV1::HistoricalBacktest,
        32,
        None,
        None,
        true,
    )
    .await;
    execution
        .configure_gamma_resolution_policy_v1(GammaResolutionPolicyV1::QualifiedPublicConsumption)
        .unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let source = std::iter::from_fn(move || source.next_visible_group());
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "outside-ws".into());
    driver.enable_finite_recording_wait().unwrap();
    let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    driver
        .hidden_bucket(buckets.next_bucket().unwrap().unwrap())
        .unwrap();
    let future = buckets.next_bucket().unwrap().unwrap();
    assert_eq!(future.reference.bucket_time_utc_ms(), source_time);
    while driver
        .next_group_at()
        .unwrap()
        .is_some_and(|at| at <= cutoff)
    {
        driver.next_group().unwrap().unwrap();
    }
    assert!(driver.next_group_at().unwrap().unwrap() > cutoff);
    driver.finish_input_at(cutoff).unwrap();
    let state = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert!(state.pending_gamma_resolutions.is_empty());
    assert!(
        state
            .execution_state
            .redeemed_market_resolution(&terminal.condition_id)
            .is_none()
    );
    assert!(gamma_redemptions(&delivered).is_empty());
    assert!(
        !delivered
            .settlements
            .borrow()
            .iter()
            .any(|f| matches!(f.event, SettlementLifecycleEventV2::ResolvedPosition { .. }))
    );
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn gamma_policy_cannot_redeem_closed_only_or_conflicting_terminal() {
    for conflict in [false, true] {
        let terminal = gamma_row();
        let mut inputs = gamma_inputs(&terminal);
        inputs.pop();
        if conflict {
            // 明确构造的同毫秒相反Gamma声明，原真实终态行仍保持原值。
            let mut opposite = terminal.clone();
            let tokens: Vec<String> = serde_json::from_str(&terminal.asset_ids).unwrap();
            opposite.winning_asset_id = Some(tokens[0].clone());
            opposite.winning_outcome = Some("Up".into());
            opposite.outcome_prices_raw = Some(r#"["1","0"]"#.into());
            opposite.collector_admission_sequence += 1;
            let mut payload: serde_json::Value =
                serde_json::from_str(opposite.source_evidence_payload_json.as_ref().unwrap())
                    .unwrap();
            payload["outcomePrices"] = serde_json::json!(r#"["1","0"]"#);
            let payload = serde_json::to_string(&payload).unwrap();
            opposite.source_evidence_raw_sha256 =
                Some(format!("{:x}", Sha256::digest(payload.as_bytes())));
            opposite.source_evidence_payload_json = Some(payload);
            inputs.push(record(
                inputs.len() as u64,
                TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                    PolymarketMarketMetadataV6Input::Existing(opposite),
                )),
            ));
        } else {
            // closed与输入耗尽没有赢家资格，不得生成平台结果。
            inputs.pop();
            for input in &mut inputs {
                if let DptIngressV1::Message {
                    row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row)),
                    source_evidence,
                } = &mut input.ingress
                {
                    row.closed = Some(true);
                    row.accepting_orders = Some(false);
                    *source_evidence =
                        source_evidence_v3_from_typed_raw_v2(&TypedRawRowV2::Existing(
                            TypedRawRow::PolymarketMarketMetadata(row.clone()),
                        ))
                        .unwrap();
                }
            }
        }
        inputs.push(end(inputs.len() as u64));
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = record_gamma_inputs(&root, inputs);
        let scenario = scenario_for_gamma(&core, &terminal.condition_id);
        let hidden = prepare_hidden_v1(
            core,
            PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
            &temp.path().join("hidden"),
            &config(),
            "gamma-negative".into(),
        )
        .unwrap();
        let (mut execution, delivered) = public_execution_with_scenario_capacity_layout(
            &temp.path().join("sim.jsonl"),
            scenario,
            SimCallerRoleV1::HistoricalBacktest,
            32,
            None,
            None,
            true,
        )
        .await;
        execution
            .configure_gamma_resolution_policy_v1(
                GammaResolutionPolicyV1::QualifiedPublicConsumption,
            )
            .unwrap();
        let mut source =
            RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
        let source = std::iter::from_fn(move || source.next_visible_group());
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "gamma-negative".into());
        driver.enable_finite_recording_wait().unwrap();
        let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
        driver
            .hidden_bucket(buckets.next_bucket().unwrap().unwrap())
            .unwrap();
        let mut saw_pending = false;
        while driver.next_group_at().unwrap().is_some() {
            driver.next_group().unwrap().unwrap();
            saw_pending |= !driver
                .execution
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .pending_gamma_resolutions
                .is_empty();
        }
        driver
            .finish_input_at(terminal.collector_admission_time_utc_ms)
            .unwrap();
        let state = driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert!(state.pending_gamma_resolutions.is_empty());
        assert!(
            state
                .execution_state
                .redeemed_market_resolution(&terminal.condition_id)
                .is_none()
        );
        assert!(gamma_redemptions(&delivered).is_empty());
        assert_eq!(saw_pending, conflict);
        driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn same_millisecond_hidden_ws_conflict_cancels_gamma_before_economic_application() {
    let terminal = gamma_row();
    let consumed = terminal.collector_admission_time_utc_ms;
    let tokens: Vec<String> = serde_json::from_str(&terminal.asset_ids).unwrap();
    let market = MarketIdentityV2 {
        condition_id: ConditionId(terminal.condition_id.clone()),
        clob_market_id: ClobMarketId(terminal.clob_market_id.clone()),
        window: MarketWindowV2 {
            start_at_utc_ms: terminal.market_start_utc,
            end_at_utc_ms: terminal.market_end_utc,
        },
    };
    let mut inputs = gamma_inputs(&terminal);
    inputs.pop();
    // 原归约入口生成合法有量/方向成交步骤；SIM截止后的行情不会生成新账户成交。
    // 同毫秒原隐藏排序把市场事实放在PM派生之前；测试沿此真实排序。
    let mut marker = row(
        "gamma-conflict",
        1,
        consumed,
        consumed + 10,
        PolymarketOrderbookEvent::LastTradePrice {
            asset_id: tokens[0].clone(),
            price: "0.6".into(),
            size: Some("1".into()),
            side: Some(PolymarketSide::Sell),
            fee_rate_bps: None,
        },
    );
    marker.market = terminal.condition_id.clone();
    inputs.push(record(
        inputs.len() as u64,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(marker)),
    ));
    let mut metadata = terminal_metadata_row(&market);
    metadata.asset_ids = terminal.asset_ids.clone();
    metadata.timestamp_received = consumed + 20;
    metadata.collector_admission_sequence = terminal.collector_admission_sequence + 1;
    metadata.collector_admission_time_utc_ms = consumed + 21;
    metadata.source_evidence_received_at_utc = Some(consumed + 20);
    metadata.row_evidence_validation_completed_at_utc = Some(consumed + 21);
    metadata.lifecycle_applied_at_utc = Some(consumed + 21);
    metadata.runtime_role_applied_at_utc = Some(consumed + 21);
    metadata.ws_platform_timestamp_raw = Some(consumed.to_string());
    metadata.ws_platform_timestamp_utc = Some(consumed);
    metadata.winning_asset_id = Some(tokens[0].clone());
    metadata.winning_outcome = Some("Up".into());
    metadata.fixed_identity_verified_at_utc = terminal.market_start_utc - 10_001;
    let mut ws = terminal_orderbook_row(&market);
    ws.timestamp = Some(consumed);
    ws.timestamp_received = consumed + 20;
    ws.collector_admission_time_utc_ms = consumed + 21;
    if let PolymarketOrderbookEvent::MarketResolved {
        asset_ids,
        winning_asset_id,
        winning_outcome,
        ..
    } = &mut ws.event
    {
        *asset_ids = tokens;
        *winning_asset_id = metadata.winning_asset_id.clone().unwrap();
        *winning_outcome = "Up".into();
    }
    inputs.push(record(
        inputs.len() as u64,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
    ));
    inputs.push(record(
        inputs.len() as u64,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
    ));
    inputs.push(end(inputs.len() as u64));
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = record_gamma_inputs(&root, inputs);
    let scenario = scenario_for_gamma(&core, &terminal.condition_id);
    let hidden = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden"),
        &config(),
        "gamma-hidden-conflict".into(),
    )
    .unwrap();
    let (mut execution, delivered) = public_execution_with_scenario_capacity_layout(
        &temp.path().join("sim.jsonl"),
        scenario,
        SimCallerRoleV1::HistoricalBacktest,
        32,
        None,
        None,
        true,
    )
    .await;
    execution
        .configure_gamma_resolution_policy_v1(GammaResolutionPolicyV1::QualifiedPublicConsumption)
        .unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let source = std::iter::from_fn(move || source.next_visible_group());
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "gamma-hidden-conflict".into());
    driver.enable_finite_recording_wait().unwrap();
    let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    driver
        .hidden_bucket(buckets.next_bucket().unwrap().unwrap())
        .unwrap();
    while driver
        .next_group_at()
        .unwrap()
        .is_some_and(|at| at <= consumed)
    {
        driver.next_group().unwrap().unwrap();
    }
    driver.advance_before(consumed).unwrap();
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
            .pending_gamma_resolutions
            .len(),
        1
    );
    assert!(gamma_redemptions(&delivered).is_empty());
    let conflict = buckets.next_bucket().unwrap().unwrap();
    assert_eq!(conflict.reference.bucket_time_utc_ms(), consumed);
    assert!(
        conflict
            .records
            .iter()
            .any(|record| matches!(record, HiddenExecutionRecordV1::Step { .. }))
    );
    assert!(conflict.records.iter().any(|record| matches!(record, HiddenExecutionRecordV1::MarketFact(fact) if matches!(fact.payload, CanonicalMarketFactPayloadV1::QualityBoundary(_)))));
    driver.hidden_bucket(conflict).unwrap();
    let state = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert!(state.pending_gamma_resolutions.is_empty());
    assert!(
        state
            .execution_state
            .redeemed_market_resolution(&terminal.condition_id)
            .is_none()
    );
    assert!(gamma_redemptions(&delivered).is_empty());
    assert!(
        !delivered
            .settlements
            .borrow()
            .iter()
            .any(|f| matches!(f.event, SettlementLifecycleEventV2::ResolvedPosition { .. }))
    );
    driver.stop_and_drain().unwrap();
}
