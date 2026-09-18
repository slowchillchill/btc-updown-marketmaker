use crate::public_input_tests::common::*;
use crate::*;
use ::execution_normalization::{MemoryIdentityKeyV1, SourceNeutralEntityKeyV1};
use contracts::*;
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::sync::Arc;

fn context(id: &str) -> RequestContextV2 {
    RequestContextV2 {
        global_request_id: id.into(),
        caller_correlation_id: None,
    }
}
fn leg(token: &str, price: Decimal) -> BuyLegIntentV1 {
    BuyLegIntentV1 {
        token_id: TokenId(token.into()),
        order: BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share: price,
            size_shares: Decimal::from(5),
            post_only: true,
        },
    }
}
fn platform(order: &ExecutionOrderReadV1<'_>) -> String {
    let MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) =
        &order.order_identity.key
    else {
        unreachable!()
    };
    source_order_id.clone()
}
fn blocks(scores: &[f32], lengths: &[usize]) -> Vec<CancelScoreBlock> {
    let mut offset = 0;
    lengths
        .iter()
        .map(|&length| {
            let mut block = vec![1e20; 256];
            block[..length].copy_from_slice(&scores[offset..offset + length]);
            offset += length;
            CancelScoreBlock {
                scores: block,
                valid_len: length,
            }
        })
        .collect()
}
struct Harness {
    _temp: tempfile::TempDir,
    shared: SharedExecutionV1,
    native: NativeInputRoot,
    driver: FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>,
    groups: Vec<serde_json::Value>,
}
impl Harness {
    fn new(pair_count: usize, maker: bool, multi: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = if multi {
            recorded_cancel_switch(&root)
        } else if maker {
            recorded_average_pool(&root, false)
        } else {
            recorded_trading_window(&root, "100", maker.then_some(("0.5", "5")), 320_000, true)
        };
        let (mut execution, observation) = execution_with_seed_and_cash(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "2000",
        );
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let account = shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key;
        let native = NativeInputRoot::new(
            format!("cancel-{pair_count}-{maker}-{multi}"),
            account,
            -10_000,
        );
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let driver = FinitePublicConsumptionV1::new(execution, source, "cancel".into());
        Self {
            _temp: temp,
            shared,
            native,
            driver,
            groups: vec![],
        }
    }
    fn next(&mut self) -> Option<NativeInputGroup> {
        self.driver.next_group().unwrap()?;
        let at = self
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let got = self.native.consume(&mut self.shared, &mut self.driver);
        eprintln!(
            "native at={at} result={:?}",
            got.as_ref().map(|g| g.as_ref().map(|g| g.cancel.len()))
        );
        let group = got.unwrap().unwrap();
        self.groups.push(serde_json::to_value(&group).unwrap());
        Some(group)
    }
    async fn pairs(&mut self, count: usize) {
        for i in 0..count {
            self.driver.wait_for_recording_capacity().unwrap();
            self.shared
                .execute(
                    &mut self.driver.decision().unwrap(),
                    context(&format!("pair-{i}")),
                    Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        orders: [
                            leg(UP_TOKEN_ID, Decimal::new(1, 1)),
                            leg(DOWN_TOKEN_ID, Decimal::new(1, 1)),
                        ],
                    })),
                )
                .await
                .unwrap();
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn complete_pair_terminal_leg_and_257_global_selection() {
    let mut h = Harness::new(86, false, false);
    for _ in 0..4 {
        h.next().unwrap();
    }
    h.pairs(86).await;
    let mut closed_b = None;
    let mut verified = false;
    while h.driver.next_group().unwrap().is_some() {
        h.shared.action_before(&mut h.driver).unwrap();
        let at = h
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        if ![36_000, 56_000].contains(&at) {
            continue;
        }
        let g = h
            .native
            .consume(&mut h.shared, &mut h.driver)
            .unwrap()
            .unwrap();
        if at == 36_000 {
            assert_eq!(g.cancel.len(), 259);
            let ExecutionReadValueV1::Known(objects) =
                g.action_before.as_ref().unwrap().cancel_objects()
            else {
                panic!()
            };
            let last = objects
                .iter()
                .enumerate()
                .find_map(|(i, o)| match o {
                    CancelObjectReadV1::Single { order }
                        if order.origin.as_ref().unwrap().intent.global_request_id()
                            == "pair-85"
                            && order.origin.as_ref().unwrap().batch_item_ordinal == 1 =>
                    {
                        Some((i, platform(order)))
                    }
                    _ => None,
                })
                .unwrap();
            let expected = objects
                .iter()
                .find_map(|o| match o {
                    CancelObjectReadV1::Paired { origin, orders }
                        if origin.global_request_id() == "pair-85" =>
                    {
                        assert_eq!(orders[0].origin.as_ref().unwrap().batch_item_ordinal, 0);
                        assert_eq!(orders[1].origin.as_ref().unwrap().batch_item_ordinal, 1);
                        Some(orders.iter().map(platform).collect::<Vec<_>>())
                    }
                    _ => None,
                })
                .unwrap();
            let mut scores = vec![0.; 259];
            scores[objects.iter().position(|o| matches!(o, CancelObjectReadV1::Paired { origin, .. } if origin.global_request_id()=="pair-85")).unwrap()] = 2.;
            assert_eq!(
                g.cancel_intent(CancelFamily::Paired, &blocks(&scores, &[256, 3]))
                    .unwrap(),
                Some(ModelIntentV1::CancelOrders {
                    platform_order_ids: expected
                })
            );
            scores.fill(0.);
            scores[last.0] = 2.;
            g.execute_selected(
                0,
                SelectedAction::Cancel(CancelFamily::Single),
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("close-b"),
                |_, _, _| Ok::<_, ()>(blocks(&scores, &[256, 3])),
            )
            .await
            .unwrap();
            // 原冻集合保持，发送不能抢先改写它。
            assert_eq!(g.cancel.len(), 259);
            closed_b = Some(last.1);
        }
        if at == 56_000 {
            assert_eq!(g.cancel.len(), 257);
            let ExecutionReadValueV1::Known(objects) =
                g.action_before.as_ref().unwrap().cancel_objects()
            else {
                panic!()
            };
            assert_eq!(
                objects
                    .iter()
                    .filter(|o| matches!(o, CancelObjectReadV1::Paired { .. }))
                    .count(),
                85
            );
            assert!(!objects.iter().any(|o|matches!(o,CancelObjectReadV1::Paired{origin,..} if origin.global_request_id()=="pair-85")));
            assert!(!objects.iter().any(
                |o| matches!(o,CancelObjectReadV1::Single{order} if Some(platform(order))==closed_b)
            ));
            // 相同真实257对象：末块市场对象；跨128边界的同族并列仍取原首项。
            for lengths in [&[256, 1][..], &[128, 129][..]] {
                let mut scores = vec![0.; 257];
                scores[256] = 3.;
                assert_eq!(
                    g.cancel_intent(CancelFamily::Market, &blocks(&scores, lengths))
                        .unwrap(),
                    Some(ModelIntentV1::CancelMarketOrders {
                        condition_id: ConditionId(CONDITION_ID.into())
                    })
                );
                scores.fill(0.);
                scores[0] = 2.;
                scores[170] = 2.;
                scores[256] = 100.;
                let CancelObjectReadV1::Single { order } = &objects[0] else {
                    panic!()
                };
                assert_eq!(
                    g.cancel_intent(CancelFamily::Single, &blocks(&scores, lengths))
                        .unwrap(),
                    Some(ModelIntentV1::CancelOrder {
                        platform_order_id: platform(order)
                    })
                );
            }
            verified = true;
            break;
        }
    }
    assert!(verified);
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn no_trade_keeps_order_and_later_real_fill() {
    let mut h = Harness::new(0, true, false);
    for _ in 0..4 {
        h.next().unwrap();
    }
    h.shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("maker"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: leg(UP_TOKEN_ID, Decimal::new(9, 1)),
            })),
        )
        .await
        .unwrap();
    let mut no_trade = false;
    let mut fill = false;
    while let Some(g) = h.next() {
        let at = g.cutoff.decision_time().logical_time_utc_ms();
        if at == 28_000 {
            let before = g.action_before.as_ref().unwrap();
            let orders = before.orders().count();
            assert_eq!(orders, 1);
            assert_eq!(
                before.orders().next().unwrap().cancellable,
                ExecutionReadValueV1::Known(true)
            );
            let funds = before.request_funds().count();
            let receipt = g
                .execute_selected(
                    0,
                    SelectedAction::NoTrade,
                    &mut h.shared,
                    &mut h.driver.decision().unwrap(),
                    context("no-trade"),
                    |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> {
                        panic!("NoTrade must not call parameters")
                    },
                )
                .await
                .unwrap();
            assert!(receipt.is_none());
            let after = h.shared.action_before(&mut h.driver).unwrap().unwrap();
            assert_eq!(after.orders().count(), orders);
            assert_eq!(after.request_funds().count(), funds);
            assert_eq!(
                after.economics().cash.actual_available_pusd,
                before.economics().cash.actual_available_pusd
            );
            no_trade = true;
        }
        if at == 56_000 {
            let orders = g
                .action_before
                .as_ref()
                .unwrap()
                .orders()
                .collect::<Vec<_>>();
            assert_eq!(orders.len(), 1);
            assert_eq!(
                orders[0].matched_shares,
                ExecutionReadValueV1::Known(ExactDecimal("1".into()))
            );
            fill = true;
            break;
        }
    }
    assert!(no_trade && fill);
    h.driver.stop_and_drain().unwrap();
    let log = std::fs::read_to_string(h._temp.path().join("sim.jsonl")).unwrap();
    assert!(!log.contains("no-trade"));
    assert!(!log.contains("CANCEL_ORDER"));
}

#[tokio::test(flavor = "current_thread")]
async fn market_cancel_uses_only_selected_condition() {
    let mut h = Harness::new(0, false, true);
    for _ in 0..4 {
        h.next().unwrap();
    }
    h.shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("old-market"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: leg(UP_TOKEN_ID, Decimal::new(1, 1)),
            })),
        )
        .await
        .unwrap();
    let mut new_submitted = false;
    let mut sent = false;
    let mut verified = false;
    while h.driver.next_group().unwrap().is_some() {
        h.shared.action_before(&mut h.driver).unwrap();
        let at = h
            .driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        if [26_000, 36_000, 290_000, 320_000, 322_000].contains(&at) {
            h.native
                .consume(&mut h.shared, &mut h.driver)
                .unwrap()
                .unwrap();
        }
        if at == 322_000 && !new_submitted {
            h.shared
                .execute(
                    &mut h.driver.decision().unwrap(),
                    context("new-market"),
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(OTHER_CONDITION_ID.into()),
                        order: leg("21", Decimal::new(1, 1)),
                    })),
                )
                .await
                .unwrap();
            new_submitted = true;
        }
        if ![336_000, 346_000, 356_000].contains(&at) {
            continue;
        }
        let g = h
            .native
            .consume(&mut h.shared, &mut h.driver)
            .unwrap()
            .unwrap();
        if at == 336_000 && !sent {
            let ExecutionReadValueV1::Known(objects) =
                g.action_before.as_ref().unwrap().cancel_objects()
            else {
                panic!()
            };
            assert_eq!(objects.len(), 4);
            let selected=objects.iter().position(|o|matches!(o,CancelObjectReadV1::Market{condition_id,..} if condition_id.0==CONDITION_ID)).unwrap();
            let mut scores = vec![0.; 4];
            scores[selected] = 1.;
            g.execute_selected(
                0,
                SelectedAction::Cancel(CancelFamily::Market),
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("cancel-one-market"),
                |_, _, _| Ok::<_, ()>(blocks(&scores, &[4])),
            )
            .await
            .unwrap();
            sent = true;
        }
        if at == 346_000 {
            assert_eq!(g.cutoff.private_feedback_sequence(), 5);
            assert!(
                g.action_before
                    .as_ref()
                    .unwrap()
                    .orders()
                    .all(|o| o.cancellable == ExecutionReadValueV1::Known(true))
            );
        }
        if at == 356_000 {
            assert_eq!(g.cutoff.private_feedback_sequence(), 7);
            assert_eq!(g.action_before.as_ref().unwrap().orders().count(), 1);
            assert_eq!(g.cancel.len(), 2);
            assert_eq!(h.shared.registry().orders().count(), 2);
            for order in h.shared.registry().orders() {
                let original = order.origin.as_ref().unwrap().intent.intent();
                let ModelIntentV1::SubmitSingleBuy { condition_id, .. } = original else {
                    panic!()
                };
                assert_eq!(
                    order.cancellable,
                    ExecutionReadValueV1::Known(condition_id.0 != CONDITION_ID)
                );
            }
            verified = true;
            break;
        }
    }
    assert!(sent && verified);
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires original graph archive and locked project Python"]
async fn actual_graph_pair_intent_execution_and_frozen_cancel_race() {
    use std::io::Write;
    let archive = std::env::var("NATIVE_CANCEL_GRAPH").expect("original MAIN-1x256 archive");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let mut h = Harness::new(1, false, false);
    for _ in 0..4 {
        h.next().unwrap();
    }
    h.pairs(1).await;
    let mut sent = false;
    let mut verified = false;
    while let Some(g) = h.next() {
        let at = g.cutoff.decision_time().logical_time_utc_ms();
        if at == 28_000 {
            let groups = &h.groups;
            let mut calls = 0;
            let result = g
                .execute_selected(
                    0,
                    SelectedAction::Cancel(CancelFamily::Paired),
                    &mut h.shared,
                    &mut h.driver.decision().unwrap(),
                    context("graph-pair"),
                    |_, row, prefix| {
                        calls += 1;
                        let request =
                            serde_json::json!({"groups":groups,"row":row,"prefix":prefix.to_vec()});
                        let mut process = std::process::Command::new(root.join(".venv/bin/python"))
                            .arg(root.join("tests/cancel_graph_probe.py"))
                            .arg(&archive)
                            .env("PYTHONPATH", root)
                            .env("OMP_NUM_THREADS", "1")
                            .env("MKL_NUM_THREADS", "1")
                            .stdin(std::process::Stdio::piped())
                            .stdout(std::process::Stdio::piped())
                            .stderr(std::process::Stdio::piped())
                            .spawn()
                            .unwrap();
                        process
                            .stdin
                            .take()
                            .unwrap()
                            .write_all(&serde_json::to_vec(&request).unwrap())
                            .unwrap();
                        let output = process.wait_with_output().unwrap();
                        assert!(
                            output.status.success(),
                            "{}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                        let raw: serde_json::Value =
                            serde_json::from_slice(&output.stdout).unwrap();
                        if let Some(path) = std::env::var_os("NATIVE_CANCEL_EVIDENCE") {
                            std::fs::write(
                                std::path::Path::new(&path).join("graph-request.json"),
                                serde_json::to_vec(&request).unwrap(),
                            )
                            .unwrap();
                            std::fs::write(
                                std::path::Path::new(&path).join("graph-scores.json"),
                                &output.stdout,
                            )
                            .unwrap();
                        }
                        Ok::<_, ()>(
                            raw.as_array()
                                .unwrap()
                                .iter()
                                .map(|v| CancelScoreBlock {
                                    scores: v["scores"]
                                        .as_array()
                                        .unwrap()
                                        .iter()
                                        .map(|x| x.as_f64().unwrap() as f32)
                                        .collect(),
                                    valid_len: v["valid_len"].as_u64().unwrap() as usize,
                                })
                                .collect(),
                        )
                    },
                )
                .await
                .unwrap();
            assert!(result.is_some());
            assert_eq!(calls, 1);
            assert_eq!(g.cancel.len(), 4);
            // 相同组原冻结对象仍可选择；竞争结果由真实取消反馈决定。
            g.execute_selected(
                0,
                SelectedAction::Cancel(CancelFamily::Single),
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("same-group-race"),
                |_, _, _| Ok::<_, ()>(blocks(&[0., 1., 100., 100.], &[4])),
            )
            .await
            .unwrap();
            sent = true;
        }
        if at == 56_000 {
            assert_eq!(g.action_before.as_ref().unwrap().orders().count(), 0);
            assert_eq!(h.shared.registry().orders().count(), 2);
            assert!(
                h.shared
                    .registry()
                    .orders()
                    .all(|o| o.cancellable == ExecutionReadValueV1::Known(false))
            );
            assert!(g.cancel.is_empty());
            verified = true;
            break;
        }
    }
    assert!(sent && verified);
    h.driver.stop_and_drain().unwrap();
}

fn recorded_cancel_switch(
    root: &std::path::Path,
) -> (
    data_pipeline_core::public_observation_core::PublicObservationCoreV1,
    TradingTimes,
) {
    use data_pipeline_core::{public_input::*, typed_raw::*};
    use data_pipeline_training::public_recording::*;
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut inputs = Vec::new();
    let mut times = Vec::new();
    for (other, prefetch, start) in [(false, 10_000, 20_000), (true, 290_000, 320_000)] {
        for (at, active) in [(prefetch, false), (start, true)] {
            let DptIngressV1::Message {
                row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)),
                ..
            } = meta(0).ingress
            else {
                unreachable!()
            };
            m.market_start_utc = start;
            m.market_end_utc = start + 300_000;
            m.timestamp_received = at;
            m.collector_admission_sequence = inputs.len() as u64 + 1;
            m.collector_admission_time_utc_ms = at + 1;
            m.metadata_fetch_start_utc = Some(at - 10);
            m.source_evidence_received_at_utc = Some(at);
            m.row_evidence_validation_completed_at_utc = Some(at + 1);
            m.fixed_identity_verified_at_utc = prefetch - 1;
            if other {
                m.condition_id = OTHER_CONDITION_ID.into();
                m.clob_market_id = "3195577".into();
                m.asset_ids = "[\"21\",\"4\"]".into();
            }
            if active {
                m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
                m.market_runtime_role = MetadataMarketRuntimeRole::Current;
            }
            inputs.push(record(
                inputs.len() as u64,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)),
            ));
            times.push(at);
        }
        let tokens = if other {
            ["21", "4"]
        } else {
            [UP_TOKEN_ID, DOWN_TOKEN_ID]
        };
        for token in tokens {
            let event = PolymarketOrderbookEvent::Book {
                asset_id: token.into(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.5".into(),
                    size: "100".into(),
                }],
                asks: vec![PolymarketPriceLevel {
                    price: "0.6".into(),
                    size: "100".into(),
                }],
                last_trade_price: None,
            };
            let pos = inputs.len() as u64;
            let mut input = pm(pos, pos, start + 1000, event);
            if let DptIngressV1::Message {
                row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)),
                ..
            } = &mut input.ingress
            {
                if other {
                    r.market = OTHER_CONDITION_ID.into();
                }
                r.timestamp_received = start + 1001;
                r.collector_admission_time_utc_ms = start + 1002;
            }
            let DptIngressV1::Message { row, .. } = input.ingress else {
                unreachable!()
            };
            inputs.push(record(pos, row));
            times.push(start + 2000);
        }
        for offset in if other {
            vec![6000, 16000, 26000, 36000]
        } else {
            vec![6000, 16000]
        } {
            let pos = inputs.len() as u64;
            let at = start + offset;
            let mut input = pm(
                pos,
                pos,
                at - 1000,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: tokens[0].into(),
                    price: "0.55".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            );
            if let DptIngressV1::Message {
                row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)),
                ..
            } = &mut input.ingress
            {
                if other {
                    r.market = OTHER_CONDITION_ID.into();
                }
                r.timestamp_received = at - 999;
                r.collector_admission_time_utc_ms = at - 998;
            }
            let DptIngressV1::Message { row, .. } = input.ingress else {
                unreachable!()
            };
            inputs.push(record(pos, row));
            times.push(at);
        }
    }
    for input in inputs {
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}

#[path = "buy_tests.rs"]
mod buy_tests;
