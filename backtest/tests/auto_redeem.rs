// 复用现有正式 DPT/SIM 夹具；没有请求式结算脚本或第二份账户归约。
#[path = "../../shared-run/tests/common/mod.rs"]
mod common;
use common::*;
use contracts::{economics::*, *};
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::sync::Arc;

fn rational(n: &str, d: &str) -> ExactRationalV1 {
    ExactRationalV1 {
        numerator: n.into(),
        denominator: d.into(),
    }
}

fn assert_no_requested_settlement(log: &std::path::Path) {
    let wire = std::fs::read_to_string(log).unwrap();
    let mut groups = 0;
    for line in wire.lines() {
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        let inputs = match record["kind"].as_str() {
            Some("ACCEPTED_INPUTS") => Some(record["body"].as_array().unwrap()),
            Some("ACCEPTED_INPUTS_V3") => Some(record["body"]["inputs"].as_array().unwrap()),
            _ => None,
        };
        if let Some(inputs) = inputs {
            groups += 1;
            for input in inputs {
                if input["input"] == "SETTLEMENT" {
                    for operation in ["Configure", "Request", "Query"] {
                        assert!(input["body"].get(operation).is_none(), "{input}");
                    }
                }
            }
        }
    }
    assert!(
        groups > 0,
        "必须检查实际接纳组，不能以不存在的日志标签证明无请求"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn original_auto_redeem_allocates_pair_and_orphan_once_and_preserves_pending() {
    for case in 0..4 {
        let mut temp = Some(tempfile::tempdir().unwrap());
        let path = if case == 3 {
            let path = temp.take().unwrap().keep();
            eprintln!("late finality case3 retained fixture={}", path.display());
            path
        } else {
            temp.as_ref().unwrap().path().to_path_buf()
        };
        let root = path.join("dpt");
        let (core, times) = recorded_trading_with_probes(&root, "10", None, 60_000, 80, "100", &[]);
        let policy = if case == 2 {
            // 判定先于真实成交终局，范围截止时前驱仍未闭合。
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":500000},{"state":"CONFIRMED","offset_ms":500010}],"economic_application_ordinal":1},"failure_overrides":[]}"#.as_slice()
        } else if case == 3 {
            // 同一判定先于成交终局，但原尾观察继续到前驱真实闭合。
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":45000},{"state":"CONFIRMED","offset_ms":45010}],"economic_application_ordinal":1},"failure_overrides":[]}"#.as_slice()
        } else {
            SUCCESS_POLICY
        };
        let log = path.join("sim.jsonl");
        let (mut execution, observation) = execution(&log, &core, policy);
        let feedback = observation.clone();
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        let ledger_root = path.join("ledger");
        execution
            .bind_ledger_recording_v2(&mut ledger, &ledger_root)
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "auto".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        for (token, quantity) in if case == 0 || case == 3 {
            vec![(UP_TOKEN_ID, 10), (DOWN_TOKEN_ID, 5)]
        } else {
            vec![(DOWN_TOKEN_ID, 5)]
        } {
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("buy-{token}"),
                        caller_correlation_id: None,
                    },
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(token.into()),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: Decimal::new(6, 1),
                                size_shares: Decimal::from(quantity),
                                post_only: false,
                            },
                        },
                    })),
                )
                .await
                .unwrap();
        }
        let mut saw_resolved_pending = false;
        while driver.next_group().unwrap().is_some() {
            shared.apply_closed().unwrap();
            let read = shared.ledger().economic_read().unwrap();
            for resolved in read.resolutions() {
                assert!(resolved.request.is_none());
                if case == 2 {
                    saw_resolved_pending = true;
                    assert_ne!(
                        read.market_totals().next().unwrap().settlement_state,
                        EconomicSettlementStateV1::Redeemed
                    );
                    assert!(read.successful_trades().next().is_none());
                    // 仅预留可用现金，尚无 MINED 扣款或自动兑现。
                    assert_eq!(
                        shared
                            .ledger()
                            .projection()
                            .unwrap()
                            .platform_confirmed_projection
                            .cash_pusd,
                        ExactDecimal("100".into())
                    );
                }
                if case == 3
                    && resolved.economic_predecessors_closed == ExecutionReadValueV1::Known(false)
                {
                    // 这里只证明本地前驱曾未齐；平台兑付时点另从原 NOE 事实核对。
                    saw_resolved_pending = true;
                }
            }
        }
        assert!(shared.apply_closed().unwrap());
        assert!(
            driver
                .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
                .is_none()
        );
        let read = shared.ledger().economic_read().unwrap();
        let market = read.market_totals().next().unwrap();
        let cash = read.cash().clone();
        if case == 3 {
            std::fs::write(path.join("case3-final.json"), serde_json::to_vec_pretty(&serde_json::json!({"market":market,"cash":cash,"account":shared.ledger().projection_v2().unwrap(),"conservative_nav":AccountEconomicsReadV1::from_ledger(&read).conservative_nav_pusd,"pending_noe":shared.pending_noe_record_count()})).unwrap()).unwrap();
        }
        if case != 2 {
            assert_eq!(market.settlement_state, EconomicSettlementStateV1::Redeemed);
            assert_eq!(market.paired_shares.0, "0");
            assert_eq!(market.orphan_shares.clone().map(|q| q.0), ["0", "0"]);
            assert_eq!(market.success_shares.clone().map(|q| q.0), ["0", "0"]);
            assert_eq!(
                market.success_principal_cost_pusd,
                [rational("0", "1"), rational("0", "1")]
            );
            assert_eq!(market.paired_resolution_receivable_pusd.0, "0");
            assert_eq!(market.winner_orphan_receivable_pusd.0, "0");
            // Up10、Down5 的本金为6+3；冻结费率 .07 的费用为 .168+.084。
            // 配对5归 maker，赢家孤腿5归 directional；总现金100-9-.252+10。
            assert_eq!(
                cash.maker_available_pusd.0,
                if case == 0 || case == 3 {
                    "95.748"
                } else {
                    "96.916"
                }
            );
            assert_eq!(
                cash.actual_available_pusd.0,
                if case == 0 || case == 3 {
                    "100.748"
                } else {
                    "96.916"
                }
            );
            assert_eq!(
                read.residual_cash().directional_cash_pusd.0,
                if case == 0 || case == 3 { "5" } else { "0" }
            );
            assert_eq!(
                AccountEconomicsReadV1::from_ledger(&read).conservative_nav_pusd,
                if case == 0 || case == 3 {
                    rational("25187", "250")
                } else {
                    rational("24229", "250")
                }
            );
        } else {
            assert!(saw_resolved_pending);
        }
        let expected_market = market;
        let mut checkpoint = Vec::new();
        shared.ledger().write_checkpoint(&mut checkpoint).unwrap();
        let restored = LedgerWriterV1::restore_checkpoint(checkpoint.as_slice()).unwrap();
        assert_eq!(restored.economic_read().unwrap().cash(), &cash);
        assert_eq!(
            restored
                .economic_read()
                .unwrap()
                .market_totals()
                .next()
                .unwrap(),
            expected_market
        );
        driver.stop_and_drain().unwrap();
        if case == 3 {
            use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
            use contracts::execution_normalization::{
                SimV2SourceRecordReferenceV1 as Source, SourceNeutralAccountTransitionKindV1,
                SourceNeutralEconomicDispositionV1,
                SourceNeutralSettlementLifecycleResultV1 as Settlement, SourceNeutralTradeStateV1,
            };
            let mut resolution_at = None;
            let mut confirmed = Vec::new();
            let mut autos = Vec::new();
            let mut auto_cash_deltas = Vec::new();
            for reference in feedback.result_references().unwrap() {
                let view =
                    ::execution_normalization::read_durable_noe_record_v1(&reference).unwrap();
                match view.record() {
                    NormalizationRecord::Evidence {
                        evidence:
                            SourceNeutralEvidenceKind::SettlementLifecycleV2 {
                                result: SourceNeutralSettlementLifecycleResultV2::Resolved { .. },
                            },
                        source_reference,
                        ..
                    } => {
                        let Source::SettlementLifecycle {
                            platform_event_at_utc_ms,
                            ..
                        } = &source_reference.record
                        else {
                            panic!("原判定来源")
                        };
                        resolution_at = Some(*platform_event_at_utc_ms);
                    }
                    NormalizationRecord::Evidence {
                        evidence: SourceNeutralEvidenceKind::TradeLifecycleV3 { result },
                        source_reference,
                        ..
                    } if result.result.state == SourceNeutralTradeStateV1::Confirmed => {
                        confirmed.push(source_reference.record.clone());
                    }
                    NormalizationRecord::Evidence {
                        evidence:
                            SourceNeutralEvidenceKind::SettlementLifecycle {
                                result: Settlement::AutoRedeemApplied { .. },
                            },
                        source_reference,
                        ..
                    } => autos.push(source_reference.record.clone()),
                    NormalizationRecord::EconomicFact { fact, .. } => {
                        if let SourceNeutralEconomicDispositionV1::AccountTransition {
                            transition:
                                SourceNeutralAccountTransitionKindV1::AutoRedeemApplied { .. },
                            balance_delta,
                            ..
                        } = &fact.disposition
                        {
                            auto_cash_deltas.push(balance_delta.cash_pusd_delta.clone());
                        }
                    }
                    _ => {}
                }
            }
            assert!(saw_resolved_pending);
            assert_eq!(confirmed.len(), 2);
            assert_eq!(autos.len(), 1, "原自动生命周期恰好一次");
            assert_eq!(auto_cash_deltas, [ExactDecimal("10".into())]);
            let Source::SettlementLifecycle {
                platform_event_at_utc_ms: auto_at,
                feedback_received_at_utc_ms: auto_received,
                ..
            } = &autos[0]
            else {
                panic!("原自动结算来源")
            };
            let mut confirmed_received_after_auto = false;
            for reference in &confirmed {
                let Source::TradeLifecycle {
                    platform_event_at_utc_ms,
                    feedback_received_at_utc_ms,
                    ..
                } = reference
                else {
                    panic!("原成交来源")
                };
                assert!(resolution_at.unwrap() < *platform_event_at_utc_ms);
                assert!(*platform_event_at_utc_ms <= *auto_at);
                confirmed_received_after_auto |= feedback_received_at_utc_ms > auto_received;
            }
            assert!(
                confirmed_received_after_auto,
                "原反馈先后必须覆盖这次失败条件"
            );
            eprintln!(
                "late finality original NOE: resolved={resolution_at:?}, confirmed={confirmed:?}, auto={autos:?}"
            );
        }
        let durable = LedgerWriterV1::open_durable(&ledger_root).unwrap();
        assert_eq!(durable.economic_read().unwrap().cash(), &cash);
        assert_eq!(
            durable
                .economic_read()
                .unwrap()
                .market_totals()
                .next()
                .unwrap(),
            expected_market
        );
        assert_no_requested_settlement(&log);
    }
}

// 复用正式原录制，只补两条有真实接收/准入时刻的 RTDS 观察，
// 让 61011ms 的 Auto 与 61012ms 的 Resolved 分别成为可见反馈。
fn recorded_auto_before_resolution(
    root: &std::path::Path,
) -> (
    data_pipeline_core::public_observation_core::PublicObservationCoreV1,
    TradingTimes,
) {
    use data_pipeline_core::public_input::*;
    use data_pipeline_training::public_recording::*;
    let original = root.with_extension("original");
    let (_, times) = recorded_trading_with_probes(&original, "10", None, 60_000, 0, "100", &[]);
    let prefix = serde_json::from_reader(
        std::fs::File::open(original.join("stable-00000000000000000000/prefix.json")).unwrap(),
    )
    .unwrap();
    let mut reader =
        PublicRecordingReaderV1::open_committed(&original, Some(prefix), 2, 1 << 20).unwrap();
    let mut inputs = Vec::new();
    while let Some(row) = reader.next_row().unwrap() {
        if let PublicStoredRowV1::Input(input) = row
            && !matches!(input.ingress, DptIngressV1::EndOfInput)
        {
            inputs.push(input);
        }
    }
    for (sequence, at) in [61_010, 61_011, 62_000].into_iter().enumerate() {
        inputs.push(record(
            inputs.len() as u64,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(at, sequence as u64 + 1)),
        ));
    }
    inputs.push(RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: inputs.len() as u64,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    });
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut delivery = times.0;
    delivery.extend([61_011, 61_012, 62_001]);
    for input in inputs {
        let prepared =
            prepare_public_ingress_v1(&core, &input, &std::collections::BTreeMap::new(), &[])
                .unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(delivery))
}

#[tokio::test(flavor = "current_thread")]
async fn v4_auto_waits_for_real_resolution_and_restores_original_pending_without_double_cash() {
    use sim_system::{
        finality::TradeFinalityPolicyV2, latency_profile::load_embedded_proxy_latency_profile,
        scenario::*,
    };
    let temp = tempfile::tempdir().unwrap().keep();
    eprintln!("V4 pending fixture outputs: {}", temp.display());
    let root = temp.as_path().join("dpt");
    let (core, times) = recorded_auto_before_resolution(&root);
    let (mut execution, observation, rules) = execution_with_rules_v4(
        &temp.as_path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let ledger_root = temp.as_path().join("ledger");
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let groups = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let consumed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = consumed.clone();
    let source = groups
        .clone()
        .into_iter()
        .inspect(move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .map(Ok);
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "auto-pending".into());
    driver.enable_finite_recording_wait().unwrap();
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "pure-loser".into(),
                caller_correlation_id: None,
            },
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(DOWN_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(5),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap();
    let sim = loop {
        driver.next_group().unwrap().unwrap();
        let done = shared.apply_closed().unwrap();
        if driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms()
            == 61_012
        {
            let mut sim = Vec::new();
            driver.write_checkpoint(&mut sim).unwrap();
            #[derive(serde::Deserialize)]
            struct DriverState {
                noe: Vec<u8>,
            }
            #[derive(serde::Deserialize, serde::Serialize)]
            struct Frontiers {
                closed_receive_time: Option<i64>,
                public_visible_time: Option<i64>,
            }
            #[derive(serde::Deserialize)]
            struct Port {
                observation: Frontiers,
            }
            let saved: DriverState = ciborium::de::from_reader(sim.as_slice()).unwrap();
            let saved: Port = ciborium::de::from_reader(saved.noe.as_slice()).unwrap();
            let mut pending = Vec::new();
            shared.write_checkpoint(&mut pending).unwrap();
            std::fs::write(temp.join("pending-sim.cbor"), &sim).unwrap();
            std::fs::write(temp.join("pending-shared.cbor"), &pending).unwrap();
            shared
                .ledger()
                .write_checkpoint(std::fs::File::create(temp.join("pending-ledger.cbor")).unwrap())
                .unwrap();
            eprintln!(
                "V4 pending done={done}, count={}, frontiers={}",
                shared.pending_noe_record_count(),
                serde_json::to_string(&saved.observation).unwrap()
            );
            #[derive(serde::Deserialize)]
            struct PendingRecords {
                pending: Vec<::execution_normalization::NoeConsumedRecordRefV2>,
            }
            let records: PendingRecords = ciborium::de::from_reader(pending.as_slice()).unwrap();
            let sources = records.pending.iter().map(|r| serde_json::json!({"position":r.position(),"visibility":r.visibility(),"source":r.record().source_reference()})).collect::<Vec<_>>();
            eprintln!(
                "V4 original pending references={}",
                serde_json::to_string(&sources).unwrap()
            );
            std::fs::write(
                temp.join("pending-frontiers.json"),
                serde_json::to_vec_pretty(&saved.observation).unwrap(),
            )
            .unwrap();
            std::fs::write(
                temp.join("pending-references.json"),
                serde_json::to_vec_pretty(&sources).unwrap(),
            )
            .unwrap();
            assert_eq!(saved.observation.closed_receive_time, Some(61_011));
            assert_eq!(saved.observation.public_visible_time, Some(61_012));
            assert!(!done);
            assert_eq!(
                shared.pending_noe_record_count(),
                2,
                "原 Auto 生命周期及账户变更均等待关联"
            );
            let read = shared.ledger().economic_read().unwrap();
            assert_eq!(read.cash().actual_available_pusd.0, "96.916");
            assert_eq!(
                read.market_totals().next().unwrap().settlement_state,
                EconomicSettlementStateV1::Unresolved
            );
            assert_eq!(
                shared
                    .ledger()
                    .projection_v2()
                    .unwrap()
                    .platform_confirmed_projection
                    .outcome_balances[0]
                    .balance_shares
                    .0,
                "5"
            );
            break sim;
        }
    };
    let cursor = consumed.load(std::sync::atomic::Ordering::Relaxed);
    let prefetched = driver.prefetched_group().unwrap();
    let mut ledger = Vec::new();
    shared.ledger().write_checkpoint(&mut ledger).unwrap();
    let mut registry = Vec::new();
    shared.write_checkpoint(&mut registry).unwrap();
    #[derive(serde::Deserialize)]
    struct Pending {
        pending: Vec<::execution_normalization::NoeConsumedRecordRefV2>,
    }
    let pending: Pending = ciborium::de::from_reader(registry.as_slice()).unwrap();
    assert_eq!(pending.pending.len(), 2);
    while driver.next_group().unwrap().is_some() {
        assert!(shared.apply_closed().unwrap());
    }
    let expected = shared.ledger().projection_v2().unwrap();
    let economic = shared.ledger().economic_read().unwrap();
    let market = economic.market_totals().next().unwrap();
    assert_eq!(market.settlement_state, EconomicSettlementStateV1::Redeemed);
    assert_eq!(market.success_shares.clone().map(|q| q.0), ["0", "0"]);
    assert_eq!(
        market.success_principal_cost_pusd,
        [rational("0", "1"), rational("0", "1")]
    );
    assert_eq!(economic.cash().actual_available_pusd.0, "96.916");
    assert_eq!(
        AccountEconomicsReadV1::from_ledger(&economic).conservative_nav_pusd,
        rational("24229", "250")
    );
    driver.stop_and_drain().unwrap();
    drop(shared);
    let scenario = PlatformScenarioV1::try_new(
        core.fixed_markets()
            .map(|market| {
                let context = core.pm_context(&market.condition_id.0).unwrap();
                PlatformMarketV1::try_new(
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
            .collect(),
    )
    .unwrap();
    let scenario = Arc::new(
        SealedPlatformScenarioV1::decode_and_seal(
            &serde_json::to_vec(&SimulationScenarioArtifactV1 {
                schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
                markets: scenario.market_set().markets().to_vec(),
            })
            .unwrap(),
        )
        .unwrap(),
    );
    let (mut state, observation) = PublicConsumptionStateV1::restore_checkpoint(
        sim.as_slice(),
        rules,
        Arc::new(TradeFinalityPolicyV2::decode_and_seal(SUCCESS_POLICY).unwrap()),
        scenario,
        load_embedded_proxy_latency_profile().unwrap(),
        2 << 20,
        128,
    )
    .unwrap();
    let mut ledger = LedgerWriterV1::restore_checkpoint(ledger.as_slice()).unwrap();
    for record in &pending.pending {
        assert!(matches!(
            ledger.apply_noe_memory_v2(record).unwrap(),
            LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation
        ));
    }
    state
        .reconnect_ledger_recording(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared =
        SharedExecutionV2::restore_checkpoint(ledger, observation, registry.as_slice()).unwrap();
    assert_eq!(shared.pending_noe_record_count(), 2);
    let source = prefetched
        .into_iter()
        .chain(groups.into_iter().skip(cursor))
        .map(Ok);
    let mut driver = FinitePublicConsumptionV1::from_checkpoint_state(state, source);
    driver.enable_finite_recording_wait().unwrap();
    while driver.next_group().unwrap().is_some() {
        assert!(shared.apply_closed().unwrap());
    }
    assert_eq!(shared.pending_noe_record_count(), 0);
    assert_eq!(shared.ledger().projection_v2().unwrap(), expected);
    assert_eq!(
        shared
            .ledger()
            .economic_read()
            .unwrap()
            .market_totals()
            .next()
            .unwrap(),
        market
    );
    let mut ledger = Vec::new();
    shared.ledger().write_checkpoint(&mut ledger).unwrap();
    let mut duplicate = LedgerWriterV1::restore_checkpoint(ledger.as_slice()).unwrap();
    for record in &pending.pending {
        assert!(matches!(
            duplicate.apply_noe_memory_v2(record).unwrap(),
            LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
        ));
    }
    assert_eq!(duplicate.projection_v2().unwrap(), expected);
    driver.stop_and_drain().unwrap();
    std::fs::remove_dir_all(&temp).unwrap();
}

#[path = "common/mod.rs"]
mod historical_common;

#[derive(Default)]
struct HistoricalBuy {
    bought: bool,
    rules: Option<Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>>,
}
impl shared_run::StrategyV1 for HistoricalBuy {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(
        &mut self,
        input: &shared_run::StrategyInputV1,
        _at: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        let mut intents = vec![];
        if !self.bought
            && input.markets.iter().any(|m| {
                m.observation.market.condition_id.0 == CONDITION_ID
                    && m.observation.assets.iter().all(|a| a.asks.value.is_some())
            })
        {
            self.bought = true;
            let cost = self
                .rules
                .as_ref()
                .unwrap()
                .limit_buy_cost_v1(
                    &num_bigint::BigUint::from(1000_u32),
                    60,
                    &ExactDecimal("0.01".into()),
                    false,
                )
                .unwrap();
            intents.push(shared_run::ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "historical-auto-buy".into(),
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(UP_TOKEN_ID.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(6, 1),
                            size_shares: Decimal::from(10),
                            post_only: false,
                        },
                    },
                }),
                reserves: vec![shared_run::IntentReserveV1 {
                    batch_item_ordinal: 0,
                    reserve: ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1("historical-auto-reserve".into()),
                        condition_id: ConditionId(CONDITION_ID.into()),
                        requested_principal_pusd: ExactDecimal(
                            (Decimal::from_str_exact(&cost.principal_micro.to_string()).unwrap()
                                / Decimal::from(1_000_000))
                            .to_string(),
                        ),
                        immediate_fee_reserve_pusd: ExactDecimal(
                            (Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()
                                / Decimal::from(100_000))
                            .to_string(),
                        ),
                    },
                }],
            });
        }
        Ok(shared_run::StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn historical_v4_host_applies_original_auto_feedback_in_loop_and_range_tail() {
    use data_collect::polymarket_market_metadata_layout::PolymarketMarketMetadataV5Row;
    use data_collect::polymarket_orderbook_v3::*;
    let base = historical_common::BASE;
    let market = MarketIdentityV2 {
        condition_id: ConditionId(CONDITION_ID.into()),
        clob_market_id: ClobMarketId("3195576".into()),
        window: MarketWindowV2 {
            start_at_utc_ms: base + 1000,
            end_at_utc_ms: base + 10000,
        },
    };
    for delay in [0, 100] {
        // 同一真实类型/原夹具，两个宿主交付档；终态、订单与平台反馈时序不变。
        let temp = tempfile::tempdir().unwrap().keep();
        eprintln!("historical auto delay={delay} fixture={}", temp.display());
        let pm = PolymarketOrderbookV3Message {
            envelope: PolymarketOrderbookV3Envelope {
                timestamp_received: base + 11001,
                timestamp: Some(base + 11000),
                market: CONDITION_ID.into(),
                slug: Some("btc-updown-5m-engineering".into()),
                collector_host_id: "synthetic-collector".into(),
                collector_region: "test".into(),
                source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
                schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
                source_sequence_epoch_id: "epoch-a".into(),
                collector_admission_sequence: 50,
                collector_admission_time_utc_ms: base + 11003,
            },
            event: PolymarketOrderbookV3Event::MarketResolved(PolymarketMarketResolvedV3 {
                clob_market_id: "3195576".into(),
                question: None,
                description: None,
                asset_ids: vec![UP_TOKEN_ID.into(), DOWN_TOKEN_ID.into()],
                outcomes: vec!["UP".into(), "DOWN".into()],
                event_message: None,
                winning_asset_id: UP_TOKEN_ID.into(),
                winning_outcome: "UP".into(),
            }),
        };
        // 与 common::metadata 相同，直接构造 collector 正式行。
        // 来源证据记录本夹具的同一 WS 事件；不把 DPT 内部 serde 表示当原始 schema。
        use sha2::Digest;
        let PolymarketOrderbookV3Event::MarketResolved(event) = &pm.event else {
            unreachable!()
        };
        let payload = serde_json::json!({
            "event_type": "market_resolved", "market": pm.envelope.market,
            "timestamp": pm.envelope.timestamp.unwrap().to_string(), "id": event.clob_market_id,
            "assets_ids": event.asset_ids, "outcomes": event.outcomes,
            "winning_asset_id": event.winning_asset_id, "winning_outcome": event.winning_outcome
        })
        .to_string();
        let mut metadata: data_collect::polymarket_market_metadata_layout::PolymarketMarketMetadataRow = serde_json::from_value(serde_json::json!({
            "metadata_row_kind": "resolution_terminal_ws", "timestamp_received": base+11001,
            "metadata_origin": "market_resolved_ws", "collector_host_id": "synthetic-collector", "collector_region": "test",
            "schema_version": "polymarket_market_metadata_v5", "slug": "btc-updown-5m-engineering",
            "condition_id": market.condition_id.0, "clob_market_id": market.clob_market_id.0,
            "asset_ids": serde_json::to_string(&[UP_TOKEN_ID,DOWN_TOKEN_ID]).unwrap(),
            "market_start_utc": market.window.start_at_utc_ms, "market_end_utc": market.window.end_at_utc_ms,
            "fixed_identity_verified_at_utc": base-1, "market_lifecycle_state": "retired", "market_runtime_role": "none",
            "source_endpoint": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            "source_evidence_origin": "market_resolved_ws", "source_evidence_event_type": "market_resolved", "source_evidence_channel": "market",
            "source_evidence_received_at_utc": base+11001, "row_evidence_validation_completed_at_utc": base+11002,
            "source_evidence_payload_json": payload, "source_evidence_raw_sha256": format!("{:x}",sha2::Sha256::digest(payload.as_bytes())),
            "market_lifecycle_transition": "active_to_retired", "lifecycle_applied_at_utc": base+11003,
            "market_runtime_role_transition": "resolution_pending_to_none", "runtime_role_applied_at_utc": base+11003,
            "active_unavailable_reason": "not_applicable_for_row_kind",
            "ws_platform_timestamp_raw": (base+11000).to_string(), "ws_platform_timestamp_utc": base+11000,
            "winning_asset_id": UP_TOKEN_ID, "winning_outcome": "UP", "resolution_evidence_source": "market_resolved_ws",
            "terminal_claim_owner": "market_resolved_ws", "resolution_budget_horizon_utc": market.window.end_at_utc_ms+8*60_000,
            "resolution_followup_status": "terminal_ws"
        })).unwrap();
        metadata.collector_admission_sequence = 41;
        metadata.collector_admission_time_utc_ms = base + 11003;
        let metadata =
            PolymarketMarketMetadataV5Row::from_common(metadata, Some("epoch-a".into()), Some(50));
        let validation = metadata.validate();
        assert!(
            validation.is_complete(),
            "collector 正式终态: {validation:?}"
        );
        let (mut config, rules) = historical_common::setup_with_terminal(&temp, metadata, pm);
        config.timing.public_delivery_delay_ms = backtest::DelayScheduleV1::Fixed(delay);
        let mut strategy = HistoricalBuy {
            rules: Some(rules.clone()),
            ..Default::default()
        };
        let report = backtest::run_historical_v1(&config, rules, &mut strategy)
            .await
            .unwrap();
        std::fs::write(
            temp.join("report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        assert!(strategy.bought);
        assert_eq!(report.end_reason, backtest::HistoricalEndReasonV1::RangeEnd);
        let read = report.account_economics_memory.unwrap();
        let market = read
            .markets
            .iter()
            .find(|m| m.condition_id.0 == CONDITION_ID)
            .unwrap();
        assert_eq!(market.settlement_state, EconomicSettlementStateV1::Redeemed);
        assert_eq!(market.success_shares.clone().map(|q| q.0), ["0", "0"]);
        assert_eq!(
            market.success_principal_cost_pusd,
            [rational("0", "1"), rational("0", "1")]
        );
        assert_eq!(market.paired_resolution_receivable_pusd.0, "0");
        assert_eq!(market.winner_orphan_receivable_pusd.0, "0");
        // 正常 start 的无盘口计算占用宿主，两个交付档的买单都在 source=1701
        // 的隐藏 ask=.5 之后到达；.6 是限价，不是成交价。
        // 10份本金5，冻结 rate=.07/exponent=1：10*.07*.5*.5=.175。
        // 胜方一次回款10，2000-5-.175+10=2004.825；未到账返佣不当现金。
        assert_eq!(market.executed[1].shares.0, "10");
        assert_eq!(market.executed[1].principal_pusd.0, "5");
        assert_eq!(market.executed[1].actual_fee_pusd.0, "0.175");
        assert_eq!(read.cash.actual_available_pusd.0, "2004.825");
        assert_eq!(read.cash.maker_available_pusd.0, "1994.825");
        assert_eq!(
            read.directional_residual_pusd,
            ExecutionReadValueV1::Known(rational("10", "1"))
        );
        assert_eq!(
            read.conservative_nav_pusd,
            ExecutionReadValueV1::Known(rational("80193", "40"))
        );
        assert_eq!(report.pending_noe_records, 0);
        assert_no_requested_settlement(&config.sim_log);
        std::fs::remove_dir_all(temp).unwrap();
    }
}

/// 原 PR278 材料和正式 V4 消费链，仅导出实际 Auto 前后作为 GPU 数值对照。
#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL GPU Auto 生命周期、双资产经济和公开 Ledger 对照"]
async fn gpu_auto_redeem_uses_original_pair_loser_and_empty_records() {
    let destination = std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    let mut cases = Vec::new();
    for case in ["pair", "loser", "empty"] {
        let path = destination.join(case);
        std::fs::create_dir_all(&path).unwrap();
        let root = path.join("dpt");
        let (core, times) = recorded_trading_with_probes(&root, "10", None, 60_000, 80, "100", &[]);
        let policy = if case == "pair" {
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":45000},{"state":"CONFIRMED","offset_ms":45010}],"economic_application_ordinal":1},"failure_overrides":[]}"#.as_slice()
        } else { SUCCESS_POLICY };
        let (execution, observation, _) = execution_with_rules_v4(&path.join("sim.jsonl"), &core, policy, None, "e".repeat(64), "100");
        let materials = Arc::new(std::sync::Mutex::new(Vec::<LedgerRecordMaterialV2>::new()));
        let output = materials.clone();
        let mut ledger = LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        ledger.bind_record_sink_v2(Box::new(move |material| output.lock().unwrap().push(material)));
        let mut shared = SharedExecutionV2::from_opening(ledger, observation).unwrap().unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, format!("gpu-auto-{case}"));
        for _ in 0..4 { driver.next_group().unwrap().unwrap(); }
        let buys = match case { "pair" => vec![(UP_TOKEN_ID, 10), (DOWN_TOKEN_ID, 5)], "loser" => vec![(DOWN_TOKEN_ID, 5)], _ => vec![] };
        for (token, quantity) in buys {
            shared.execute(&mut driver.decision().unwrap(), RequestContextV2 {global_request_id: format!("buy-{token}"), caller_correlation_id: None},
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {condition_id: ConditionId(CONDITION_ID.into()), order: BuyLegIntentV1 {
                    token_id: TokenId(token.into()), order: BuyOrderIntentV1::GtcLimit {limit_price_pusd_per_share: Decimal::new(6,1), size_shares: Decimal::from(quantity), post_only: false}}}))).await.unwrap();
        }
        while driver.next_group().unwrap().is_some() { shared.apply_closed().unwrap(); }
        assert!(shared.apply_closed().unwrap());
        driver.complete().unwrap();
        let snapshot = |ledger: &LedgerWriterV1| {
            let read = ledger.economic_read().unwrap();
            serde_json::json!({"projection":read.base(),"cash":read.cash(),"directional_cash":read.residual_cash().directional_cash_pusd,
                "markets":read.market_totals().collect::<Vec<_>>(),"resolved":read.resolutions().next().unwrap().resolved,
                "successful_trades":read.successful_trades().map(|trade|serde_json::json!({"fact":trade.terminal,"remaining_shares":trade.remaining_success_shares,"remaining_principal":trade.remaining_principal_cost_pusd})).collect::<Vec<_>>()})
        };
        let mut writer = LedgerBackgroundWriterV2::new(&path.join("prefix"));
        let mut before = None;
        let mut auto_records = Vec::new();
        let mut all_records = Vec::new();
        for material in materials.lock().unwrap().iter() {
            if let Some(record) = material.consumed_record_v2() {
                let record_json = serde_json::to_value(record.record()).unwrap();
                let automatic = record_json["Evidence"]["evidence"]["SettlementLifecycle"]["result"].get("AutoRedeemApplied").is_some()
                    || record_json["EconomicFactV2"]["fact"]["disposition"]["AccountTransition"]["transition"]["Existing"].get("AutoRedeemApplied").is_some()
                    || record_json["EconomicFact"]["fact"]["disposition"]["AccountTransition"]["transition"].get("AutoRedeemApplied").is_some();
                let row = serde_json::json!({"record":record.record(),"position":record.position(),"visibility":record.visibility(),"ledger_entry_seq":material.ledger_entry_seq()});
                all_records.push(row.clone());
                if automatic {
                    if before.is_none() { writer.finish().unwrap(); before = Some(snapshot(&LedgerWriterV1::open_durable(path.join("prefix")).unwrap())); }
                    auto_records.push(row);
                }
            }
            assert!(writer.write(material).unwrap());
        }
        writer.finish().unwrap();
        let after = snapshot(&LedgerWriterV1::open_durable(path.join("prefix")).unwrap());
        assert_eq!(auto_records.len(), if case == "empty" {1} else {2});
        assert_eq!(after["cash"]["actual_available_pusd"], match case {"pair" => "100.748", "loser" => "96.916", _ => "100"});
        assert_eq!(after["markets"][0]["settlement_state"], "Redeemed");
        cases.push(serde_json::json!({"case":case,"before":before.unwrap(),"after":after,"auto_records":auto_records,"all_records":all_records}));
    }
    let input = serde_json::json!({"cases":cases});
    std::fs::write(destination.join("original-auto.json"),serde_json::to_vec_pretty(&input).unwrap()).unwrap();
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_AUTO_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    let input_write = serde_json::to_writer(child.stdin.take().unwrap(),&input);
    let output = child.wait_with_output().unwrap();
    std::fs::write(destination.join("auto-python.stdout"),&output.stdout).unwrap();
    std::fs::write(destination.join("auto-python.stderr"),&output.stderr).unwrap();
    assert!(output.status.success(),"{:?}: {}",output.status,String::from_utf8_lossy(&output.stderr));
    input_write.unwrap();
    eprintln!("{}",String::from_utf8_lossy(&output.stdout));
}
