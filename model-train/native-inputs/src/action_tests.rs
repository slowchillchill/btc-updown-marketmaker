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
fn platform(order: &ExecutionOrderReadV2<'_>) -> String {
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
pub(super) struct Harness {
    _temp: tempfile::TempDir,
    pub(super) shared: SharedExecutionV2,
    pub(super) native: NativeInputRoot,
    pub(super) driver: FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>,
    groups: Vec<serde_json::Value>,
}

#[test]
fn long_window_fixture_delivers_terminal_and_tail_in_causal_order() {
    let mut h = Harness::new(0, false, false);
    let root = h._temp.path().join("dpt");
    let (_, times) = recorded_trading_window(
        &h._temp.path().join("delivery-diagnostic"),
        "100",
        None,
        320_000,
        true,
    );
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut previous = None;
    for item in source {
        let delivery = item.unwrap();
        let at = delivery.delivered_at_utc_ms;
        eprintln!(
            "fixture delivery position={} next={at} prior_frontier={previous:?}",
            delivery.prepared.reference.group_position
        );
        let result = h.driver.next_group();
        eprintln!("fixture consumption={result:?}");
        assert!(
            previous.is_none_or(|prior| at >= prior),
            "fixture delivery reversed: {previous:?} -> {at}"
        );
        result.unwrap().unwrap();
        h.native.consume(&mut h.shared, &mut h.driver).unwrap();
        previous = Some(at);
    }
    assert_eq!(previous, Some(331_000));
    assert!(h.driver.next_group().unwrap().is_none());
}
impl Harness {
    fn new(pair_count: usize, maker: bool, multi: bool) -> Self {
        Self::named(pair_count, maker, multi, "shared-combination", "100")
    }
    pub(super) fn named(
        pair_count: usize,
        maker: bool,
        multi: bool,
        identity: &str,
        depth: &str,
    ) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = if multi {
            recorded_cancel_switch(&root)
        } else if maker {
            recorded_average_pool(&root, false)
        } else {
            recorded_trading_window(&root, depth, maker.then_some(("0.5", "5")), 320_000, true)
        };
        let (mut execution, observation, rules) = execution_with_identity_v4(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "2000",
            identity,
        );
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let shared = SharedExecutionV2::from_opening(ledger, observation)
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
            rules.clone(),
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
    pub(super) async fn pairs(&mut self, count: usize) {
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
fn assert_native_cancel_families(group: Arc<NativeInputGroup>) {
    let ExecutionReadValueV1::Known(objects) =
        group.action_before.as_ref().unwrap().cancel_objects()
    else {
        panic!()
    };
    for family in 3..=5 {
        let mut actor = NativeActor::new(group.clone(), 0).unwrap();
        assert!(actor.support().unwrap()[family]);
        actor.choose(family).unwrap();
        let expected = objects
            .iter()
            .map(|object| {
                matches!(
                    (family, object),
                    (3, CancelObjectReadV2::Single { .. })
                        | (4, CancelObjectReadV2::Paired { .. })
                        | (5, CancelObjectReadV2::Market { .. })
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actor.support().unwrap(), expected);
        // 末个合法对象包含尾块；不能用首块或压缩后的索引替换原索引。
        let index = expected.iter().rposition(|legal| *legal).unwrap();
        actor.choose(index).unwrap();
        let intent = match &objects[index] {
            CancelObjectReadV2::Single { order } => ModelIntentV1::CancelOrder {
                platform_order_id: platform(order),
            },
            CancelObjectReadV2::Paired { orders, .. } => ModelIntentV1::CancelOrders {
                platform_order_ids: orders.iter().map(platform).collect(),
            },
            CancelObjectReadV2::Market { condition_id, .. } => ModelIntentV1::CancelMarketOrders {
                condition_id: (*condition_id).clone(),
            },
        };
        assert_eq!(actor.intent().unwrap(), Some(intent));
    }
    let mut no_trade = NativeActor::new(group, 0).unwrap();
    no_trade.choose(0).unwrap();
    assert!(no_trade.is_complete());
    assert_eq!(no_trade.intent().unwrap(), None);
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
        let g = Arc::new(
            h.native
                .consume(&mut h.shared, &mut h.driver)
                .unwrap()
                .unwrap(),
        );
        assert_native_cancel_families(g.clone());
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
                    CancelObjectReadV2::Single { order }
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
                    CancelObjectReadV2::Paired { origin, orders }
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
            scores[objects.iter().position(|o| matches!(o, CancelObjectReadV2::Paired { origin, .. } if origin.global_request_id()=="pair-85")).unwrap()] = 2.;
            assert_eq!(
                g.cancel_intent(CancelFamily::Paired, &blocks(&scores, &[256, 3]))
                    .unwrap(),
                Some(ModelIntentV1::CancelOrders {
                    platform_order_ids: expected
                })
            );
            scores.fill(0.);
            scores[last.0] = 2.;
            let mut actor = NativeActor::new(g.clone(), 0).unwrap();
            actor.choose(3).unwrap();
            actor.choose(last.0).unwrap();
            actor
                .execute(
                    &mut h.shared,
                    &mut h.driver.decision().unwrap(),
                    context("close-b"),
                )
                .await
                .unwrap()
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
                    .filter(|o| matches!(o, CancelObjectReadV2::Paired { .. }))
                    .count(),
                85
            );
            assert!(!objects.iter().any(|o|matches!(o,CancelObjectReadV2::Paired{origin,..} if origin.global_request_id()=="pair-85")));
            assert!(!objects.iter().any(
                |o| matches!(o,CancelObjectReadV2::Single{order} if Some(platform(order))==closed_b)
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
                let CancelObjectReadV2::Single { order } = &objects[0] else {
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
        let g = Arc::new(g);
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
            let mut actor = NativeActor::new(g.clone(), 0).unwrap();
            actor.choose(0).unwrap();
            let receipt = actor
                .execute(
                    &mut h.shared,
                    &mut h.driver.decision().unwrap(),
                    context("no-trade"),
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
            let selected=objects.iter().position(|o|matches!(o,CancelObjectReadV2::Market{condition_id,..} if condition_id.0==CONDITION_ID)).unwrap();
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
        if at == 346_000 || at == 356_000 {
            assert_eq!(g.cutoff.private_feedback_sequence(), 7);
            assert_eq!(g.action_before.as_ref().unwrap().orders().count(), 1);
            assert_eq!(g.cancel.len(), 3, "保留当前与旧条件的 Market 候选");
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
            if at == 356_000 {
                verified = true;
                break;
            }
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

#[cfg(all(feature = "python", feature = "ort"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicitly supplied exported graphs and matching Python wheel ORT library"]
async fn same_process_cancel_actor_all_objects_and_original_identities() {
    use pyo3::prelude::*;
    let directory = std::env::var("ACTOR_GRAPH_DIRECTORY").unwrap();
    ort::init_from(std::env::var("ORT_DYLIB_PATH").unwrap())
        .unwrap()
        .commit();
    let mut h = Harness::new(86, false, false);
    let mut history = Vec::new();
    for _ in 0..4 {
        history.push(Arc::new(h.next().unwrap()));
    }
    h.pairs(86).await;
    let g = loop {
        assert!(h.driver.next_group().unwrap().is_some());
        let Some(group) = h.native.consume(&mut h.shared, &mut h.driver).unwrap() else {
            continue;
        };
        let group = Arc::new(group);
        if group.cutoff.decision_time().logical_time_utc_ms() == 36_000 {
            break group;
        }
        history.push(group);
    };
    assert_eq!(g.cancel.len(), 259);
    type Step = (Vec<bool>, String, Vec<f32>, Vec<f32>);
    type Trace = (Vec<f32>, Vec<f32>);
    type PythonEvidence = (
        Vec<Trace>,
        Vec<Vec<usize>>,
        Vec<Vec<Step>>,
        usize,
        usize,
        Py<PyAny>,
    );
    let (traces, paths, records, layers, hidden, continuation): PythonEvidence =
        Python::attach(|py| {
            let module = py.import("model_train.actor_validation").unwrap();
            let group = Py::new(py, crate::python::PythonGroup { group: g.clone() }).unwrap();
            let history = crate::python::value(
                py,
                serde_json::to_value(history.iter().map(|g| g.as_ref()).collect::<Vec<_>>())
                    .unwrap(),
            )
            .unwrap();
            let directory = py
                .import("pathlib")
                .unwrap()
                .getattr("Path")
                .unwrap()
                .call1((&directory,))
                .unwrap();
            module
                .getattr("validate_cancel")
                .unwrap()
                .call1((group, history, directory))
                .unwrap()
                .extract()
                .unwrap()
        });
    let session = |file| {
        ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .with_inter_threads(1)
            .unwrap()
            .with_parallel_execution(false)
            .unwrap()
            .with_memory_pattern(false)
            .unwrap()
            .with_intra_op_spinning(false)
            .unwrap()
            .with_inter_op_spinning(false)
            .unwrap()
            .commit_from_file(std::path::Path::new(&directory).join(file))
            .unwrap()
    };
    let mut state = session("state_encoder.onnx");
    let mut h_prev = vec![0f32; layers * hidden];
    let mut z = Vec::new();
    let mut frame = 0;
    for group in history.iter().chain(std::iter::once(&g)) {
        for row in &group.rows {
            let output = state.run(ort::inputs![
                "fast_features" => ort::value::Tensor::from_array(([1,836], row.fast.clone())).unwrap(),
                "slow_features" => ort::value::Tensor::from_array(([1,433], row.slow.clone())).unwrap(),
                "h_prev" => ort::value::Tensor::from_array(([layers,1,hidden], h_prev)).unwrap()
            ]).unwrap();
            z = output["z_t"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            h_prev = output["h_next"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            for (got, want) in [(&z, &traces[frame].0), (&h_prev, &traces[frame].1)] {
                assert_eq!(got.len(), want.len());
                for (&a, &b) in got.iter().zip(want) {
                    assert!(finite_close(a, b));
                }
            }
            frame += 1;
        }
    }
    assert_eq!(frame, traces.len());
    let mut scorer = crate::ort_actor::OrtActorScorer {
        #[cfg(feature = "continuous-validation")]
        measurement: (0, None),
        session: session("action_decoder.onnx"),
        hidden,
    };
    let mut single = NativeActor::new(g.clone(), 0).unwrap();
    single.choose(3).unwrap();
    let family = NativeActor::new(g.clone(), 0).unwrap();
    let mut paired = NativeActor::new(g.clone(), 0).unwrap();
    paired.choose(4).unwrap();
    let reduced = scorer.reduced(&family, &z).unwrap();
    let rows = [
        (&single, z.as_slice(), reduced.as_slice()),
        (&family, z.as_slice(), reduced.as_slice()),
        (&paired, z.as_slice(), reduced.as_slice()),
    ];
    #[cfg(feature = "continuous-validation")]
    {
        scorer.measurement = (0, Some(Vec::new()));
    }
    let batch = scorer.scores_batch(&rows).unwrap();
    #[cfg(feature = "continuous-validation")]
    assert_eq!(
        scorer.measurement,
        (2, Some(vec![vec![256, 256, 256], vec![3, 3]]))
    );
    for (row, actual) in rows.iter().zip(batch) {
        let expected = scorer.scores(row.0, row.1, row.2).unwrap();
        assert_eq!(actual.len(), expected.len());
        assert!(
            actual
                .iter()
                .zip(expected)
                .all(|(&a, b)| finite_close(a, b))
        );
    }
    assert!(scorer.scores_batch(&[]).unwrap().is_empty());
    let mut actual = NativeActor::new(g.clone(), 0).unwrap();
    scorer.greedy(&mut actual, &z).unwrap();
    assert_eq!(actual.path, paths[0]);
    let reduced = scorer.reduced(&actual, &z).unwrap();
    for (path, steps) in paths.iter().zip(records) {
        let mut actor = NativeActor::new(g.clone(), 0).unwrap();
        for (&choice, (support, prefix, features, want)) in path.iter().zip(steps) {
            assert_eq!(actor.support().unwrap(), support);
            assert_eq!(format!("{:?}", actor.state), prefix);
            assert_eq!(actor.prefix_features().as_slice(), features);
            let got = scorer.scores(&actor, &z, &reduced).unwrap();
            assert_eq!(got.len(), want.len());
            for (&a, &b) in got.iter().zip(&want) {
                assert!(finite_close(a, b));
            }
            if actor.head_index() == 7 {
                assert_eq!(got.len(), 259);
                let mut best = None;
                for (i, (&legal, &value)) in support.iter().zip(&got).enumerate() {
                    if legal && best.is_none_or(|(_, old)| value > old) {
                        best = Some((i, value));
                    }
                }
                assert_eq!(best.unwrap().0, choice);
            }
            actor.choose(choice).unwrap();
        }
        if path[0] >= 3 {
            assert_eq!(
                actor.intent().unwrap(),
                g.cancel_index_intent(*path.last().unwrap())
                    .map(Some)
                    .unwrap()
            );
        }
    }
    // 只执行真实图实际选择，外生三族探针不覆盖模型决定。
    let intent = actual.intent().unwrap();
    let receipt = actual
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("actual-cancel-graph"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.is_some(), intent.is_some());
    eprintln!("cancel actual path={:?}, intent={intent:?}", actual.path);
    let mut successors = 0;
    loop {
        assert!(h.driver.next_group().unwrap().is_some());
        if let Some(successor) = h.native.consume(&mut h.shared, &mut h.driver).unwrap() {
            continue_three_runtime(
                Arc::new(successor),
                &continuation,
                &mut state,
                &mut h_prev,
                &mut scorer,
                layers,
                hidden,
            );
            successors += 1;
        }
        if h.driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms()
            == 56_000
        {
            break;
        }
    }
    assert_eq!(successors, 2);
    let after = h.shared.action_before(&mut h.driver).unwrap().unwrap();
    let before = g.action_before.as_ref().unwrap();
    match &intent {
        None => {
            assert_eq!(after.orders().count(), 172);
            assert!(
                after
                    .orders()
                    .all(|order| order.cancellable == ExecutionReadValueV1::Known(true))
            );
            assert_eq!(after.economics().cash, before.economics().cash);
            assert_eq!(
                after.request_funds().count(),
                before.request_funds().count()
            );
        }
        Some(ModelIntentV1::CancelOrder { platform_order_id }) => {
            assert_eq!(
                after
                    .orders()
                    .filter(|order| order.cancellable == ExecutionReadValueV1::Known(true))
                    .count(),
                171
            );
            assert!(
                after
                    .orders()
                    .filter(|order| platform(order) == *platform_order_id)
                    .all(|order| order.cancellable == ExecutionReadValueV1::Known(false))
            );
        }
        Some(ModelIntentV1::CancelOrders { platform_order_ids }) => {
            assert_eq!(
                after
                    .orders()
                    .filter(|order| order.cancellable == ExecutionReadValueV1::Known(true))
                    .count(),
                170
            );
            assert!(
                after
                    .orders()
                    .filter(|order| platform_order_ids.contains(&platform(order)))
                    .all(|order| order.cancellable == ExecutionReadValueV1::Known(false))
            );
        }
        Some(ModelIntentV1::CancelMarketOrders { .. }) => {
            assert!(
                after
                    .orders()
                    .all(|order| order.cancellable == ExecutionReadValueV1::Known(false))
            );
            assert_eq!(after.economics().cash.actual_available_pusd.0, "2000");
        }
        Some(ModelIntentV1::SubmitSingleBuy { .. } | ModelIntentV1::SubmitPairedBuy { .. }) => {
            // 接受／拒绝均由真实反馈消费；发送未知预留不得留在该反馈截点。
            assert!(
                after
                    .request_funds()
                    .all(|request| request.reservation_state
                        != ReservationStatusV1::SubmittedUnknown)
            );
        }
    }
    eprintln!(
        "cancel population=259 actual path={:?}, intent={intent:?}",
        actual.path
    );
    h.driver.stop_and_drain().unwrap();
}

fn finite_close(actual: f32, reference: f32) -> bool {
    actual.is_finite()
        && reference.is_finite()
        && (actual - reference).abs() <= 1e-5 + 1e-4 * reference.abs()
}

#[test]
fn numeric_comparison_rejects_nonfinite_reference_and_actual() {
    for (actual, reference) in [
        (f32::INFINITY, f32::INFINITY),
        (1., f32::INFINITY),
        (f32::NAN, 1.),
    ] {
        assert!(!finite_close(actual, reference));
    }
    assert!(finite_close(1., 1.));
}

#[cfg(all(feature = "python", feature = "ort"))]
fn continue_three_runtime(
    group: Arc<NativeInputGroup>,
    continuation: &pyo3::Py<pyo3::PyAny>,
    state: &mut ort::session::Session,
    previous_hidden: &mut Vec<f32>,
    scorer: &mut crate::ort_actor::OrtActorScorer,
    layers: usize,
    hidden: usize,
) {
    use pyo3::prelude::*;
    type Step = (Vec<bool>, String, Vec<f32>, Vec<f32>);
    type Evidence = (
        Vec<(Vec<f32>, Vec<f32>)>,
        Vec<(usize, Vec<usize>, Vec<Step>)>,
    );
    let (traces, decisions): Evidence = Python::attach(|py| {
        let native = Py::new(
            py,
            crate::python::PythonGroup {
                group: group.clone(),
            },
        )
        .unwrap();
        continuation
            .call1(py, (native,))
            .unwrap()
            .extract(py)
            .unwrap()
    });
    assert_eq!(traces.len(), group.rows.len());
    let mut decision_index = 0;
    for (row_index, row) in group.rows.iter().enumerate() {
        let output = state.run(ort::inputs![
            "fast_features" => ort::value::Tensor::from_array(([1,836],row.fast.clone())).unwrap(),
            "slow_features" => ort::value::Tensor::from_array(([1,433],row.slow.clone())).unwrap(),
            "h_prev" => ort::value::Tensor::from_array(([layers,1,hidden],previous_hidden.clone())).unwrap()
        ]).unwrap();
        let z = output["z_t"]
            .try_extract_tensor::<f32>()
            .unwrap()
            .1
            .to_vec();
        *previous_hidden = output["h_next"]
            .try_extract_tensor::<f32>()
            .unwrap()
            .1
            .to_vec();
        for (actual, reference) in [
            (&z, &traces[row_index].0),
            (&*previous_hidden, &traces[row_index].1),
        ] {
            assert_eq!(actual.len(), reference.len());
            assert!(
                actual
                    .iter()
                    .zip(reference)
                    .all(|(&a, &b)| finite_close(a, b))
            );
        }
        if row.role != InputRole::DecisionRequired {
            continue;
        }
        let (expected_row, path, steps) = &decisions[decision_index];
        assert_eq!(*expected_row, row_index);
        let mut actor = NativeActor::new(group.clone(), row_index).unwrap();
        scorer.greedy(&mut actor, &z).unwrap();
        assert_eq!(&actor.path, path);
        let mut replay = NativeActor::new(group.clone(), row_index).unwrap();
        let reduced = scorer.reduced(&replay, &z).unwrap();
        assert_eq!(path.len(), steps.len());
        for (&choice, (support, prefix, features, scores)) in path.iter().zip(steps) {
            assert_eq!(&replay.support().unwrap(), support);
            assert_eq!(&format!("{:?}", replay.state), prefix);
            assert_eq!(replay.prefix_features().as_slice(), features);
            assert!(features.iter().all(|v| v.is_finite()));
            let actual = scorer.scores(&replay, &z, &reduced).unwrap();
            assert_eq!(actual.len(), scores.len());
            assert!(actual.iter().zip(scores).all(|(&a, &b)| finite_close(a, b)));
            replay.choose(choice).unwrap();
        }
        assert_eq!(actor.intent().unwrap(), replay.intent().unwrap());
        eprintln!(
            "successor cutoff={} row={row_index} path={path:?}",
            group.cutoff.decision_time().logical_time_utc_ms()
        );
        decision_index += 1;
    }
    assert_eq!(decision_index, decisions.len());
    eprintln!(
        "successor group cutoff={} frames={} decisions={}",
        group.cutoff.decision_time().logical_time_utc_ms(),
        group.rows.len(),
        decisions.len()
    );
}
