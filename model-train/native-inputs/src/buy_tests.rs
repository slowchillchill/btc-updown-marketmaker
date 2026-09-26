use super::*;
use crate::prefix::*;
use data_pipeline_core::{public_input::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use num_bigint::BigUint;
fn selected(family: Family, variant: Variant, n: &str) -> PrefixState {
    PrefixState {
        family: Some(family),
        single_leg: Some(Leg::B),
        quantity_hundredths: Some(n.parse().unwrap()),
        legs: std::array::from_fn(|_| LegSelection {
            variant: Some(variant),
            post_only: Some(true),
            lifetime: Some(Lifetime::Seconds300),
            price_index: Some(BigUint::from(20u32)),
        }),
        ..Default::default()
    }
}
fn fixture() -> Harness {
    fixture_cash("2000")
}
fn fixture_cash(cash: &str) -> Harness {
    fixture_config(cash, ["0.0025", "0.005"])
}
fn fixture_config(cash: &str, ticks: [&str; 2]) -> Harness {
    fixture_config_at(cash, ticks, 24_999)
}
fn fixture_config_at(cash: &str, ticks: [&str; 2], intermediate_at: i64) -> Harness {
    fixture_config_times(cash, ticks, intermediate_at, &[35_000, 45_000, 55_000])
}
fn fixture_config_times(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
) -> Harness {
    fixture_config_depths(cash, ticks, intermediate_at, followups, ["100", "100"])
}
fn fixture_config_depths(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
    asks: [&str; 2],
) -> Harness {
    fixture_with_minimum(cash, ticks, intermediate_at, followups, asks, "5")
}
fn fixture_with_minimum(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
    asks: [&str; 2],
    minimum_shares: &str,
) -> Harness {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut rows = Vec::new();
    let mut times = Vec::new();
    for (at, active) in [(10_000, false), (20_000, true)] {
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)),
            ..
        } = meta(0).ingress
        else {
            unreachable!()
        };
        m.market_end_utc = 320_000;
        m.timestamp_received = at;
        m.collector_admission_sequence = rows.len() as u64 + 1;
        m.collector_admission_time_utc_ms = at + 1;
        // 同一固定身份的两次真实输入使用不同Gamma源顺序；DPT负责投影，夹具不重排原字段。
        let source_tokens = if active {
            [UP_TOKEN_ID, DOWN_TOKEN_ID]
        } else {
            [DOWN_TOKEN_ID, UP_TOKEN_ID]
        };
        m.outcomes_raw = Some(
            if active {
                r#"["UP","DOWN"]"#
            } else {
                r#"["DOWN","UP"]"#
            }
            .into(),
        );
        m.outcome_prices_raw = Some(
            if active {
                r#"["0.6","0.4"]"#
            } else {
                r#"["0.4","0.6"]"#
            }
            .into(),
        );
        let payload = serde_json::json!({"clobTokenIds":serde_json::to_string(&source_tokens).unwrap(), "orderPriceMinTickSize":"0.01"}).to_string();
        use sha2::{Digest, Sha256};
        m.source_evidence_raw_sha256 = Some(format!("{:x}", Sha256::digest(payload.as_bytes())));
        m.source_evidence_payload_json = Some(payload);
        m.metadata_fetch_start_utc = Some(at - 10);
        m.source_evidence_received_at_utc = Some(at);
        m.row_evidence_validation_completed_at_utc = Some(at + 1);
        if active {
            m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
            m.market_runtime_role = MetadataMarketRuntimeRole::Current;
        }
        rows.push(TypedRawRowV2::Existing(
            TypedRawRow::PolymarketMarketMetadata(m),
        ));
        times.push(at);
    }
    let mut events = Vec::new();
    for (token, ask_size) in [UP_TOKEN_ID, DOWN_TOKEN_ID].into_iter().zip(asks) {
        events.push((
            21_000,
            22_000,
            PolymarketOrderbookEvent::Book {
                asset_id: token.into(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.5".into(),
                    size: "100".into(),
                }],
                asks: vec![PolymarketPriceLevel {
                    price: "0.6".into(),
                    size: ask_size.into(),
                }],
                last_trade_price: None,
            },
        ));
    }
    for (at, visible, token, old, new) in [
        (23_000, 23_900, UP_TOKEN_ID, "0.01", ticks[0]),
        (23_001, 23_900, DOWN_TOKEN_ID, "0.01", ticks[1]),
        (
            24_000,
            intermediate_at,
            DOWN_TOKEN_ID,
            ticks[1],
            if ticks[1] == "0.005" {
                "0.01"
            } else {
                ticks[1]
            },
        ),
    ] {
        events.push((
            at,
            visible,
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: token.into(),
                old_tick_size: Some(old.into()),
                new_tick_size: new.into(),
            },
        ));
    }
    for &at in followups {
        events.push((
            at,
            at + 1000,
            PolymarketOrderbookEvent::LastTradePrice {
                asset_id: UP_TOKEN_ID.into(),
                price: "0.6".into(),
                size: None,
                side: None,
                fee_rate_bps: None,
            },
        ));
    }
    events.sort_by_key(|(_, visible, _)| *visible);
    for (at, visible, event) in events {
        let p = rows.len() as u64;
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut r)),
            ..
        } = pm(p, p, at, event).ingress
        else {
            unreachable!()
        };
        r.timestamp_received = at + 1;
        r.collector_admission_time_utc_ms = at + 2;
        rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
        times.push(visible);
    }
    for (i, row) in rows.into_iter().enumerate() {
        let input = record(i as u64, row);
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    let (mut execution, observation, rules) =
        execution_with_minimum_v4(&temp.path().join("sim.jsonl"), &core, cash, minimum_shares);
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let noe_context = observation
        .opening_record()
        .unwrap()
        .context()
        .context_reference
        .clone();
    let shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let native = NativeInputRoot::new("buy".into(), account, -10_000, rules.clone());
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, TradingTimes(times)).unwrap();
    Harness {
        _temp: temp,
        shared,
        native,
        driver: FinitePublicConsumptionV1::new(execution, source, "buy".into()),
        groups: vec![],
        noe_context,
    }
}
fn ready(h: &mut Harness) -> NativeInputGroup {
    for _ in 0..5 {
        h.next().unwrap();
    }
    h.next().unwrap()
}
fn leg_values(order: &BuyOrderIntentV1) -> (Decimal, Decimal, Decimal) {
    match order {
        BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share: p,
            size_shares: q,
            ..
        }
        | BuyOrderIntentV1::GtdLimit {
            limit_price_pusd_per_share: p,
            size_shares: q,
            ..
        } => (*q, *p, *q * *p),
        BuyOrderIntentV1::FokMarket {
            spend_pusd: p,
            worst_price_pusd_per_share: price,
        }
        | BuyOrderIntentV1::FakMarket {
            spend_pusd: p,
            worst_price_pusd_per_share: price,
        } => (*p / *price, *price, *p),
    }
}
#[test]
fn four_variants_keep_big_integers_ticks_and_frozen_identity() {
    let mut h = fixture();
    let mut g = ready(&mut h);
    let mut observation = h.driver.frames()[0].observation.clone();
    observation.assets.reverse();
    g.rows[0].action_context = Some(NativeActionContext::from_observation(
        &observation,
        h.driver.frames()[0].rule_context.clone(),
        g.rows[0].action_context.as_ref().unwrap().snapshot.clone(),
    ));
    for variant in [
        Variant::GtcLimit,
        Variant::GtdLimit,
        Variant::FokMarket,
        Variant::FakMarket,
    ] {
        for n in ["9007199254740993", "9007199254740994"] {
            let mut state = selected(Family::SubmitPairedBuy, variant, n);
            state.legs[0].post_only = Some(false);
            if matches!(variant, Variant::FokMarket | Variant::FakMarket) {
                for leg in &mut state.legs {
                    leg.post_only = None;
                    leg.lifetime = None;
                }
            }
            let ModelIntentV1::SubmitPairedBuy { orders, .. } = g.buy_intent(0, &state).unwrap()
            else {
                panic!()
            };
            assert_eq!(
                [orders[0].token_id.0.as_str(), orders[1].token_id.0.as_str()],
                [UP_TOKEN_ID, DOWN_TOKEN_ID]
            );
            match &orders[0].order {
                BuyOrderIntentV1::GtcLimit { post_only, .. }
                | BuyOrderIntentV1::GtdLimit { post_only, .. } => assert!(!post_only),
                _ => {}
            }
            let q = if n.ends_with('3') {
                Decimal::from_str_exact("90071992547409.93").unwrap()
            } else {
                Decimal::from_str_exact("90071992547409.94").unwrap()
            };
            assert_eq!(
                leg_values(&orders[0].order),
                (q, Decimal::new(5, 2), q * Decimal::new(5, 2))
            );
            assert_eq!(
                leg_values(&orders[1].order),
                (q, Decimal::new(1, 1), q * Decimal::new(1, 1))
            );
        }
    }
    let mut state = selected(Family::SubmitSingleBuy, Variant::GtcLimit, "500");
    let original = g.buy_intent(0, &state).unwrap();
    let next = h.next().unwrap();
    let ModelIntentV1::SubmitSingleBuy { order, .. } = next.buy_intent(0, &state).unwrap() else {
        panic!()
    };
    assert_eq!(order.token_id.0, DOWN_TOKEN_ID);
    assert_eq!(leg_values(&order.order).1, Decimal::new(2, 1));
    assert_eq!(g.buy_intent(0, &state).unwrap(), original);
    state.quantity_hundredths = None;
    assert!(matches!(
        g.buy_intent(0, &state),
        Err(InputError::IncompleteBuyPrefix)
    ));
    h.driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn exact_buy_assembly_reaches_original_requests_and_receipts() {
    let mut h = fixture();
    let g = ready(&mut h);
    for (i, lifetime, seconds) in [
        (0, Lifetime::Seconds180, 264),
        (1, Lifetime::Seconds240, 324),
        (2, Lifetime::Seconds300, 384),
    ] {
        let mut state = selected(Family::SubmitPairedBuy, Variant::GtdLimit, "500");
        for leg in &mut state.legs {
            leg.lifetime = Some(lifetime);
        }
        if i == 2 {
            state.legs[0].variant = Some(Variant::GtcLimit);
        }
        let intent = g.buy_intent(0, &state).unwrap();
        if let ModelIntentV1::SubmitPairedBuy { orders, .. } = &intent {
            for o in orders {
                if let BuyOrderIntentV1::GtdLimit {
                    expires_at_utc_sec, ..
                } = o.order
                {
                    assert_eq!(expires_at_utc_sec, seconds);
                }
            }
        }
        g.execute_selected(
            0,
            SelectedAction::Buy(&state),
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context(&format!("limit-{i}")),
            |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!("已选参数不评分") },
        )
        .await
        .unwrap()
        .unwrap();
    }
    for (id, variant) in [("fok", Variant::FokMarket), ("fak", Variant::FakMarket)] {
        let mut state = selected(Family::SubmitSingleBuy, variant, "500");
        state.legs[1].price_index = Some(BigUint::from(120u32));
        g.execute_selected(
            0,
            SelectedAction::Buy(&state),
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context(id),
            |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
        )
        .await
        .unwrap()
        .unwrap();
    }
    while let Some(group) = h.next() {
        if group.cutoff.decision_time().logical_time_utc_ms() == 56_000 {
            break;
        }
    }
    let orders = h.shared.registry().orders().collect::<Vec<_>>();
    assert_eq!(orders.len(), 8);
    for order in &orders {
        let origin = order.origin.as_ref().unwrap();
        match origin.intent.intent() {
            ModelIntentV1::SubmitPairedBuy { orders, .. } => assert_eq!(
                orders[origin.batch_item_ordinal as usize].token_id.0,
                if origin.batch_item_ordinal == 0 {
                    UP_TOKEN_ID
                } else {
                    DOWN_TOKEN_ID
                }
            ),
            ModelIntentV1::SubmitSingleBuy { order, .. } => {
                assert_eq!(order.token_id.0, DOWN_TOKEN_ID);
                assert_eq!(origin.batch_item_ordinal, 0);
            }
            _ => panic!(),
        };
        assert!(order.accepted.is_some());
        if matches!(
            origin.intent.intent(),
            ModelIntentV1::SubmitSingleBuy { .. }
        ) {
            assert!(
                matches!(&order.matched_shares, ExecutionReadValueV1::Known(q) if Decimal::from_str_exact(&q.0).unwrap() >= Decimal::from(5))
            );
        }
    }
    h.driver.stop_and_drain().unwrap();
    let log = std::fs::read_to_string(h._temp.path().join("sim.jsonl")).unwrap();
    for expiration in [264000, 324000, 384000] {
        assert!(log.contains(&format!("\"expires_at_utc_ms\":{expiration}")));
    }
}

#[test]
fn actor_complete_support_prices_and_shared_budget_match_independent_enumeration() {
    let mut h = fixture_cash("2.02");
    let g = ready(&mut h);
    let actor = NativeActor::new(Arc::new(g), 0).unwrap();
    let mut state = PrefixState {
        family: Some(Family::SubmitPairedBuy),
        ..Default::default()
    };
    // 未固定属性时，小数量有真实GTC续项；固定市价后每腿仍至少1 pUSD。
    for n in [1u32, 112, 113, 125, 499] {
        assert!(actor.fixed_completion(&n.into(), &state).unwrap());
    }
    for leg in &mut state.legs {
        leg.variant = Some(Variant::FokMarket);
    }
    // 实际两腿tick为.0025/.005；独立枚举原本金，不使用完成性函数作oracle。
    for n in 1u32..500 {
        let mut feasible = false;
        for ka in 1u32..400 {
            let a = n * ka * 25;
            if a < 1_000_000 {
                continue;
            }
            for kb in 1u32..200 {
                let b = n * kb * 50;
                if b >= 1_000_000 && a + b <= 2_020_000 {
                    feasible = true;
                    break;
                }
            }
            if feasible {
                break;
            }
        }
        assert_eq!(
            actor.fixed_completion(&n.into(), &state).unwrap(),
            feasible,
            "n={n}"
        );
    }
    state.quantity_hundredths = Some(500u32.into());
    state.legs[0].variant = Some(Variant::FokMarket);
    state.legs[1].variant = Some(Variant::GtcLimit);
    state.legs[1].post_only = Some(true);
    let mut actor = actor;
    actor.state = state;
    actor.state.stage = Stage::Price(Leg::A);
    let mask = actor.support().unwrap();
    for (at, &valid) in mask.iter().enumerate() {
        let k = at as u32 + 1;
        assert_eq!(
            valid,
            k < 400 && 500 * k * 25 >= 1_000_000 && 500 * k * 25 + 500 * 50 <= 2_020_000
        );
    }
    let fee = actor.price_fee_features().unwrap();
    let snapshot = &actor.group.rows[0]
        .action_context
        .as_ref()
        .unwrap()
        .snapshot;
    let expected = snapshot
        .market_buy_fee_estimate_v1(&500u32.into(), 80, &ExactDecimal("0.0025".into()))
        .unwrap();
    assert_eq!(
        fee[79],
        crate::exact::signed_log_f32(&crate::exact::ratio(
            num_bigint::BigInt::from(expected.fee_1e5_units),
            100_000
        ))
    );
    assert!(fee[399..].iter().all(|x| *x == 0.));
    actor.state.stage = Stage::Price(Leg::B);
    assert!(actor.price_fee_features().unwrap().iter().all(|x| *x == 0.));
}

#[tokio::test(flavor = "current_thread")]
async fn full_actor_retains_exact_pair_and_original_execution_reservations() {
    let mut h = fixture();
    let g = Arc::new(ready(&mut h));
    let mut actor = NativeActor::new(g.clone(), 0).unwrap();
    assert_eq!(actor.support().unwrap()[..3], [true, false, true]);
    actor.choose(2).unwrap();
    let n = BigUint::from(500u32);
    while actor.state.stage == Stage::QuantityBranch {
        let interval = actor.state.interval.as_ref().unwrap();
        let mid = (&interval.lo + &interval.hi) / 2u32;
        actor.choose(usize::from(n > mid)).unwrap();
    }
    // A全部属性、B全部属性，随后才是A/B价格。
    actor.choose(0).unwrap();
    actor.choose(1).unwrap();
    assert_eq!(actor.state.stage, Stage::Variant(Leg::B));
    actor.choose(1).unwrap();
    actor.choose(1).unwrap();
    actor.choose(0).unwrap();
    assert_eq!(actor.state.stage, Stage::Price(Leg::A));
    actor.choose(19).unwrap();
    actor.choose(19).unwrap();
    let intent = actor.intent().unwrap().unwrap();
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = &intent else {
        panic!()
    };
    assert_eq!(leg_values(&orders[0].order).2, Decimal::new(25, 2));
    assert_eq!(leg_values(&orders[1].order).2, Decimal::new(5, 1));
    let mut decision = h.driver.decision().unwrap();
    actor
        .execute(&mut h.shared, &mut decision, context("full-actor-pair"))
        .await
        .unwrap()
        .unwrap();
    let read = h.shared.ledger().economic_read().unwrap();
    assert_eq!(read.cash().actual_available_pusd.0, "1999.25");
    assert_eq!(
        g.action_before
            .as_ref()
            .unwrap()
            .economics()
            .cash
            .actual_available_pusd
            .0,
        "2000"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_market_repair_has_nonmonotone_quantity_holes_but_limit_does_not() {
    let mut h = fixture_config("2.038", ["0.1", "0.1"]);
    let _ = ready(&mut h);
    let receipt = h
        .shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("bootstrap-real-leg"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::FokMarket {
                        spend_pusd: Decimal::ONE,
                        worst_price_pusd_per_share: Decimal::new(9, 1),
                    },
                },
            })),
        )
        .await
        .unwrap();
    eprintln!(
        "bootstrap arrival={}",
        receipt.unwrap().platform_arrived_at_utc_ms()
    );
    let mut last = None;
    for _ in 0..4 {
        last = Some(h.next().unwrap());
    }
    let actor = NativeActor::new(Arc::new(last.unwrap()), 0).unwrap();
    h.driver.wait_for_recording_capacity().unwrap();
    assert_eq!(
        actor
            .group
            .action_before
            .as_ref()
            .unwrap()
            .economics()
            .cash
            .maker_available_pusd
            .0,
        "1.01"
    );
    let mut state = PrefixState {
        family: Some(Family::SubmitSingleBuy),
        single_leg: Some(Leg::B),
        ..Default::default()
    };
    for n in [1u32, 111, 112, 113, 125] {
        assert!(actor.fixed_completion(&n.into(), &state).unwrap());
    }
    state.legs[1].variant = Some(Variant::FokMarket);
    for (n, want) in [(111u32, false), (112, true), (113, false), (125, true)] {
        assert_eq!(
            actor.fixed_completion(&n.into(), &state).unwrap(),
            want,
            "n={n}"
        );
    }
    assert!(actor.root_upper(Family::SubmitSingleBuy).unwrap() <= BigUint::from(166u32));
}

#[cfg(all(feature = "python", feature = "ort"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicitly supplied exported graphs and matching Python wheel ORT library"]
async fn same_process_python_and_rust_ort_share_native_actor_and_final_intent() {
    for case in 0..3 {
        three_runtime_buy_case(case).await;
    }
}

#[cfg(all(feature = "python", feature = "ort"))]
async fn three_runtime_buy_case(case: u8) {
    use pyo3::prelude::*;
    let directory = std::env::var("ACTOR_GRAPH_DIRECTORY").unwrap();
    let library = std::env::var("ORT_DYLIB_PATH").unwrap();
    ort::init_from(&library).unwrap().commit();
    let mut h = match case {
        0 => fixture(),
        1 => fixture_with_minimum(
            "2000",
            ["0.0025", "0.005"],
            24_999,
            &[35_000, 45_000, 55_000],
            ["100", "100"],
            "10",
        ),
        2 => fixture_config_times(
            "2000",
            ["0.0025", "0.005"],
            24_999,
            &[202_999, 204_000, 205_000],
        ),
        _ => unreachable!(),
    };
    let history_native = (0..5)
        .map(|_| Arc::new(h.next().unwrap()))
        .collect::<Vec<_>>();
    let g = Arc::new(h.next().unwrap());
    let mut probe = NativeActor::new(g.clone(), 0).unwrap();
    probe.choose(2).unwrap();
    let quantity = if case == 1 { 1u32 } else { 500u32 };
    while probe.state.stage == Stage::QuantityBranch {
        let interval = probe.state.interval.as_ref().unwrap();
        let mid = (&interval.lo + &interval.hi) / 2u32;
        probe
            .choose(usize::from(BigUint::from(quantity) > mid))
            .unwrap();
    }
    if case == 0 {
        probe.choose(2).unwrap();
        probe.choose(3).unwrap();
        probe.choose(319).unwrap();
        probe.choose(159).unwrap();
    } else {
        for _ in 0..2 {
            probe.choose(usize::from(case == 2)).unwrap();
            probe.choose(1).unwrap();
            if case == 2 {
                probe.choose(0).unwrap();
            }
        }
        probe.choose(19).unwrap();
        probe.choose(19).unwrap();
    }
    let replay = probe.path.clone();
    type Step = (Vec<bool>, String, Vec<f32>, Vec<f32>);
    type Trace = (Vec<f32>, Vec<f32>);
    type PythonEvidence = (
        Vec<Trace>,
        Vec<usize>,
        Vec<Step>,
        Vec<Step>,
        Vec<Vec<f32>>,
        usize,
        usize,
        Py<PyAny>,
    );
    let (traces, path, actual_scores, scores, mixed_scores, layers, hidden, continuation): PythonEvidence =
        Python::attach(|py| {
            let module = py.import("model_train.actor_validation").unwrap();
            let group = Py::new(py, crate::python::PythonGroup { group: g.clone() }).unwrap();
            let history = crate::python::value(
                py,
                serde_json::Value::Array(h.groups[..h.groups.len() - 1].to_vec()),
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
                .getattr("validate_python")
                .unwrap()
                .call1((group, history, directory, replay.clone(), case == 0))
                .unwrap()
                .extract()
                .unwrap()
        });
    let mut state_graph = ort::session::Session::builder()
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
        .commit_from_file(std::path::Path::new(&directory).join("state_encoder.onnx"))
        .unwrap();
    let mut hidden_state = vec![0.; layers * hidden];
    let mut z = vec![];
    let mut frame = 0;
    for group in history_native.iter().chain(std::iter::once(&g)) {
        for row in &group.rows {
            assert_eq!(
                row.hidden_state_key.condition_id,
                g.rows[0].hidden_state_key.condition_id
            );
            let (depth, book, book_mask) = crate::pad_books([&row.book_levels]);
            let outputs=state_graph.run(ort::inputs![
                "fast_features"=>ort::value::Tensor::from_array(([1,596],row.fast.clone())).unwrap(),
                "slow_features"=>ort::value::Tensor::from_array(([1,433],row.slow.clone())).unwrap(),
                "book_levels" => ort::value::Tensor::from_array(([1,6,depth,9],book)).unwrap(),
                "book_mask" => ort::value::Tensor::from_array(([1,6,depth],book_mask)).unwrap(),
                "h_prev"=>ort::value::Tensor::from_array(([layers,1,hidden],hidden_state)).unwrap()
            ]).unwrap();
            z = outputs["z_t"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            hidden_state = outputs["h_next"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            for (actual, expected) in [(&z, &traces[frame].0), (&hidden_state, &traces[frame].1)] {
                assert_eq!(actual.len(), expected.len());
                for (&a, &b) in actual.iter().zip(expected) {
                    assert!(finite_close(a, b), "frame={frame}: {a} != {b}");
                }
            }
            frame += 1;
        }
    }
    assert_eq!(frame, traces.len());
    let session = ort::session::Session::builder()
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
        .commit_from_file(std::path::Path::new(&directory).join("action_decoder.onnx"))
        .unwrap();
    let mut scorer = crate::ort_actor::OrtActorScorer {
        #[cfg(feature = "continuous-validation")]
        measurement: (0, None),
        session,
        hidden,
    };
    let mut actor = NativeActor::new(g.clone(), 0).unwrap();
    scorer.greedy(&mut actor, &z).unwrap();
    assert_eq!(actor.path, path);
    let frozen_hidden = hidden_state.clone();
    for (selected, records) in [(&path, &actual_scores), (&replay, &scores)] {
        let mut reference = NativeActor::new(g.clone(), 0).unwrap();
        let reduced = scorer.reduced(&reference, &z).unwrap();
        assert_eq!(selected.len(), records.len());
        for (&chosen, (support, state, prefix, want)) in selected.iter().zip(records) {
            assert_eq!(&reference.support().unwrap(), support);
            assert_eq!(&format!("{:?}", reference.state), state);
            assert_eq!(reference.prefix_features().as_slice(), prefix);
            let got = scorer.scores(&reference, &z, &reduced).unwrap();
            assert_eq!(got.len(), want.len());
            for (&a, &b) in got.iter().zip(want) {
                assert!(finite_close(a, b), "{a} != {b}");
            }
            reference.choose(chosen).unwrap();
        }
        assert_eq!(
            reference.intent().unwrap(),
            if selected == &path {
                actor.intent().unwrap()
            } else {
                probe.intent().unwrap()
            }
        );
    }
    // 同一真实候选轴混合为一个批次，Rust ORT不能只验证逐行独立调用。
    let mut mixture = NativeActor::new(g.clone(), 0).unwrap();
    let mut prefixes = Vec::new();
    let mut fees = Vec::new();
    let mut supports = Vec::new();
    for &choice in &replay {
        if prefixes.is_empty() || mixture.head_index() == 3 {
            prefixes.extend(mixture.prefix_features());
            fees.extend(mixture.price_fee_features().unwrap());
            supports.push(mixture.support().unwrap());
        }
        mixture.choose(choice).unwrap();
    }
    assert_eq!(supports.len(), 3);
    let outputs = scorer.session.run(ort::inputs![
        "z_t" => ort::value::Tensor::from_array(([3, hidden], z.repeat(3))).unwrap(),
        "rule_features" => ort::value::Tensor::from_array(([3,30], g.rows[0].rule.repeat(3))).unwrap(),
        "prefix_features" => ort::value::Tensor::from_array(([3,60], prefixes)).unwrap(),
        "cancel_features" => ort::value::Tensor::from_array(([3,1,184], vec![0f32;3*184])).unwrap(),
        "cancel_mask" => ort::value::Tensor::from_array(([3,1], vec![false;3])).unwrap(),
        "cancel_reduced" => ort::value::Tensor::from_array(([3,3*hidden/2], vec![0f32;9*hidden/2])).unwrap(),
        "cancel_exists" => ort::value::Tensor::from_array(([3], vec![false;3])).unwrap(),
        "price_fee_features" => ort::value::Tensor::from_array(([3,9999], fees)).unwrap()
    ]).unwrap();
    let values = outputs["price_logits"]
        .try_extract_tensor::<f32>()
        .unwrap()
        .1;
    for (i, (actual, expected)) in values.chunks_exact(9999).zip(&mixed_scores).enumerate() {
        for (&a, &b) in actual.iter().zip(expected) {
            assert!(finite_close(a, b));
        }
        if i > 0 {
            let best = |values: &[f32]| {
                supports[i]
                    .iter()
                    .zip(values)
                    .enumerate()
                    .filter(|(_, (legal, _))| **legal)
                    .fold(None, |best: Option<(usize, f32)>, (index, (_, score))| {
                        if best.is_none_or(|(_, old)| *score > old) {
                            Some((index, *score))
                        } else {
                            best
                        }
                    })
                    .unwrap()
                    .0
            };
            assert_eq!(best(actual), best(expected));
        }
    }
    drop(outputs);
    assert_eq!(hidden_state, frozen_hidden);
    // 原市场案例执行实际贪心；新增反例执行已逐前缀比对的低量/GTD原意图。
    let executing = if case == 0 { &actor } else { &probe };
    let intent = executing.intent().unwrap();
    let receipt = executing
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("three-runtime-actual-intent"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.is_some(), intent.is_some());
    if case == 1 {
        let known = selected(Family::SubmitPairedBuy, Variant::GtcLimit, "1000");
        g.execute_selected(
            0,
            SelectedAction::Buy(&known),
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("three-runtime-known-before-unknown"),
            |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
        )
        .await
        .unwrap()
        .unwrap();
        h.shared
            .execute(
                &mut h.driver.decision().unwrap(),
                context("three-runtime-unknown-cancel"),
                Some(Arc::new(ModelIntentV1::CancelOrder {
                    platform_order_id: "sim-999".into(),
                })),
            )
            .await
            .unwrap()
            .unwrap();
    }
    let mut saw_unknown = false;
    let mut saw_expired = false;
    for _ in 0..4 {
        let successor = Arc::new(h.next().unwrap());
        if case == 1
            && successor
                .action_before
                .as_ref()
                .unwrap()
                .orders()
                .any(|o| o.condition_id.is_none())
        {
            for offset in [382, 385, 388, 391, 406] {
                assert_eq!(&successor.rows[0].slow[offset..offset + 3], &[0., 0., 1.]);
            }
            saw_unknown = true;
        }
        if case == 2 {
            let time = successor.cutoff.decision_time().logical_time_utc_ms();
            let reserve = &successor
                .action_before
                .as_ref()
                .unwrap()
                .economics()
                .cash
                .actual_reserved_pusd
                .0;
            if time == 203_999 {
                assert_eq!(reserve, "0.75");
            }
            if time >= 205_000 {
                assert_eq!(reserve, "0");
                saw_expired = true;
            }
        }
        continue_three_runtime(
            successor,
            &continuation,
            &mut state_graph,
            &mut hidden_state,
            &mut scorer,
            layers,
            hidden,
        );
    }
    if intent.is_none() {
        assert_eq!(h.shared.registry().orders().count(), 0);
        assert_eq!(
            h.shared
                .ledger()
                .economic_read()
                .unwrap()
                .cash()
                .actual_available_pusd
                .0,
            "2000"
        );
    }
    if case == 1 {
        assert!(saw_unknown);
        assert_eq!(
            h.shared
                .registry()
                .orders()
                .filter(|o| o.condition_id.is_some())
                .count(),
            2
        );
        // 只有10份已知订单占用本金；0.01份原请求已被SIM拒绝并全额释放。
        assert_eq!(
            h.shared
                .ledger()
                .economic_read()
                .unwrap()
                .cash()
                .actual_reserved_pusd
                .0,
            "1.5"
        );
    }
    if case == 2 {
        assert!(saw_expired);
    }
    eprintln!(
        "case={case}, actual graph path={path:?}, executed intent={intent:?}, successor_orders={}",
        h.shared.registry().orders().count()
    );
    h.driver.stop_and_drain().unwrap();
    if case != 0 {
        return;
    }
    // 独立真实前史上的 FOK/FAK 定向探针；不冒充模型选择。
    let mut h = fixture();
    let mut tested = NativeActor::new(Arc::new(ready(&mut h)), 0).unwrap();
    for chosen in replay {
        tested.choose(chosen).unwrap();
    }
    tested
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("three-runtime-original-intent"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        h.shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "1992"
    );
    for _ in 0..4 {
        h.next().unwrap();
    }
    assert_eq!(h.shared.registry().orders().count(), 2);
    assert_eq!(
        h.shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "1991.776"
    );
}

#[test]
fn cash_root_is_not_replaced_by_encoding_limit() {
    let mut h = fixture_cash("10000000000000000000000");
    let mut actor = NativeActor::new(Arc::new(ready(&mut h)), 0).unwrap();
    let original = actor.root_upper(Family::SubmitPairedBuy).unwrap();
    let limit = encoding_limit();
    assert!(original > limit);
    actor.choose(2).unwrap();
    assert_eq!(actor.state.interval.as_ref().unwrap().upper, original);
    assert!(actor.has_completion(&limit, &limit, &actor.state).unwrap());
    assert!(
        !actor
            .has_completion(&(&limit + 1u32), &original, &actor.state)
            .unwrap()
    );
    assert!(
        actor
            .has_completion(&(&limit - 1u32), &(&limit + 1u32), &actor.state)
            .unwrap()
    );
    assert_eq!(actor.state.interval.as_ref().unwrap().hi, original);
}

#[tokio::test(flavor = "current_thread")]
async fn actor_pair_reserves_once_and_rejected_leg_releases_without_erasing_other_fill() {
    // 原源事件 24000，Matched received=24171，首成交 received=24173，
    // Accepted received=24250。24172 截点只闭合到 24171。
    // 在真实两者之间读取，不能依赖旧驱动把已接收关联拖到下一组。
    let mut h = fixture_config_at("2000", ["0.0025", "0.005"], 24_172);
    let g = Arc::new(ready(&mut h));
    let mut actor = NativeActor::new(g, 0).unwrap();
    actor.choose(2).unwrap();
    while actor.state.stage == Stage::QuantityBranch {
        let interval = actor.state.interval.as_ref().unwrap();
        let mid = (&interval.lo + &interval.hi) / 2u32;
        actor
            .choose(usize::from(BigUint::from(500u32) > mid))
            .unwrap();
    }
    actor.choose(2).unwrap();
    actor.choose(3).unwrap();
    actor.choose(319).unwrap();
    actor.choose(39).unwrap();
    for _ in 0..2 {
        actor
            .execute(
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("mixed-actor-feedback"),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            h.shared
                .ledger()
                .economic_read()
                .unwrap()
                .cash()
                .actual_available_pusd
                .0,
            "1995"
        );
    }
    let original_commitments: Vec<_> = h
        .shared
        .registry()
        .all_commitments()
        .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
        .collect();
    assert_eq!(original_commitments.len(), 2);
    let intermediate = h.next().unwrap();
    assert_eq!(
        intermediate.cutoff.decision_time().logical_time_utc_ms(),
        24_172
    );
    assert_eq!(intermediate.cancel.len(), 1, "真实空市场仍有 Market 候选");
    assert_eq!(intermediate.cancel[0][2], 1.);
    let view = intermediate.action_before.as_ref().unwrap();
    let orders: Vec<_> = view.orders().collect();
    assert_eq!(orders.len(), 1, "不可撤但关联未齐的订单仍在动作快照中");
    let order = &orders[0];
    assert!(order.origin.is_none());
    assert!(order.accepted.is_none());
    assert!(order.formed.is_some());
    assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
    assert_eq!(order.matched_shares, ExecutionReadValueV1::Unavailable);
    assert_eq!(
        order.active_unfilled_shares,
        ExecutionReadValueV1::Unavailable
    );
    assert_eq!(
        view.commitments()
            .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
            .collect::<Vec<_>>(),
        original_commitments
    );
    let later = h.next().unwrap();
    assert_eq!(h.shared.registry().orders().count(), 1);
    assert_eq!(
        h.shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "1995.888"
    );
    let mut follow = NativeActor::new(Arc::new(later), 0).unwrap();
    follow.choose(1).unwrap();
    assert_eq!(follow.state.single_leg, Some(Leg::B));
    assert_eq!(
        follow.state.interval.as_ref().unwrap().upper,
        BigUint::from(666u32)
    );
    while follow.state.stage == Stage::QuantityBranch {
        let interval = follow.state.interval.as_ref().unwrap();
        let mid = (&interval.lo + &interval.hi) / 2u32;
        follow
            .choose(usize::from(BigUint::from(500u32) > mid))
            .unwrap();
    }
    follow.choose(0).unwrap();
    follow.choose(1).unwrap();
    follow.choose(9).unwrap();
    follow
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("actual-repair-commitment"),
        )
        .await
        .unwrap()
        .unwrap();
    let next = NativeActor::new(Arc::new(h.next().unwrap()), 0).unwrap();
    assert_eq!(
        next.root_upper(Family::SubmitSingleBuy).unwrap(),
        BigUint::from(166u32)
    );
}

#[test]
fn fixed_prefix_variants_and_common_budget_match_small_full_action_oracle() {
    let mut h = fixture_config("2.02", ["0.1", "0.1"]);
    let actor = NativeActor::new(Arc::new(ready(&mut h)), 0).unwrap();
    let attributes = [
        (Variant::GtcLimit, false),
        (Variant::GtcLimit, true),
        (Variant::GtdLimit, false),
        (Variant::GtdLimit, true),
        (Variant::FokMarket, false),
        (Variant::FakMarket, false),
    ];
    let oracle = |n: u64, k: u64, variant: Variant, post: bool| -> Option<u64> {
        let principal = n * k * 1000;
        match variant {
            Variant::FokMarket | Variant::FakMarket => {
                (principal >= 1_000_000).then_some(principal)
            }
            _ => {
                let m = k.min(5);
                let fee = if post || principal < 1_000_000 {
                    0
                } else {
                    (n * 7 * m * (10 - m) + 5) / 10
                };
                Some(principal + 10 * fee)
            }
        }
    };
    for n in [1u64, 100, 112, 113, 125, 499, 500, 501, 1000] {
        for &(a, pa) in &attributes {
            for &(b, pb) in &attributes {
                for ka in 1u64..=9 {
                    for kb in 1u64..=9 {
                        let want = oracle(n, ka, a, pa)
                            .zip(oracle(n, kb, b, pb))
                            .is_some_and(|(ca, cb)| ca + cb <= 2_020_000);
                        let state = PrefixState {
                            family: Some(Family::SubmitPairedBuy),
                            quantity_hundredths: Some(n.into()),
                            legs: [
                                LegSelection {
                                    variant: Some(a),
                                    post_only: Some(pa),
                                    lifetime: Some(Lifetime::Seconds180),
                                    price_index: Some(ka.into()),
                                },
                                LegSelection {
                                    variant: Some(b),
                                    post_only: Some(pb),
                                    lifetime: Some(Lifetime::Seconds300),
                                    price_index: Some(kb.into()),
                                },
                            ],
                            ..Default::default()
                        };
                        assert_eq!(
                            actor.fixed_completion(&n.into(), &state).unwrap(),
                            want,
                            "n={n} {a:?}/{pa}@{ka} {b:?}/{pb}@{kb}"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_single_repair_cap_counts_visible_success_and_nonterminal_commitment() {
    let times = [22_942, 24_017, 24_019]
        .into_iter()
        .chain((24_001..28_002).step_by(100))
        .collect::<Vec<_>>();
    let mut h = fixture_config_times("2000", ["0.1", "0.1"], 24_999, &times);
    ready(&mut h);
    async fn buy(h: &mut Harness, id: &str, token: &str, spend: &str) {
        h.driver.wait_for_recording_capacity().unwrap();
        h.shared
            .execute(
                &mut h.driver.decision().unwrap(),
                context(id),
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(token.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            size_shares: spend.parse().unwrap(),
                            limit_price_pusd_per_share: Decimal::new(6, 1),
                            post_only: false,
                        },
                    },
                })),
            )
            .await
            .unwrap()
            .unwrap();
    }
    buy(&mut h, "repair-a-ten", UP_TOKEN_ID, "10").await;
    let mut sent = false;

    let mut saw_pending = false;
    let mut saw_terminal = false;
    let mut saw_economic_pending = false;
    let mut early_order_group = None;
    while let Some(group) = h.next() {
        let group = Arc::new(group);
        let read = group.action_before.as_ref().unwrap();
        let at = read.cutoff().decision_time().logical_time_utc_ms();
        let Some(market) = read
            .economics()
            .markets
            .iter()
            .find(|m| m.condition_id.0 == CONDITION_ID)
        else {
            continue;
        };
        let assets = market.assets.as_ref().unwrap();
        let a = assets
            .iter()
            .position(|a| a.token_id.0 == UP_TOKEN_ID)
            .unwrap();
        let b = 1 - a;
        if at == 25_017 {
            let order = read
                .orders()
                .find(|o| o.token_id == Some(&TokenId(DOWN_TOKEN_ID.into())))
                .unwrap();
            assert!(order.origin.is_none() && order.accepted.is_none() && order.formed.is_none());
            assert_eq!(order.cancellable, ExecutionReadValueV1::Unavailable);
            let single = group
                .cancel
                .iter()
                .find(|v| v[0] == 1.)
                .expect("实际成交已证明可寻址 ID");
            assert_eq!(single[7], 1., "已到成交证明 B 腿");
            assert_eq!(&single[10..14], &[0.; 4], "原订单变体仍未知");
            assert_eq!(&single[56..60], &[0.; 4], "四个条件字段适用性仍未知");
            assert_eq!(&single[60..63], &[0., 0., 1.], "可撤性未知仍保留 Single");
            early_order_group = Some(group.clone());
        }
        if at == 25_019 {
            let order = read
                .orders()
                .find(|o| o.token_id == Some(&TokenId(DOWN_TOKEN_ID.into())))
                .unwrap();
            assert!(order.origin.is_some() && order.accepted.is_some() && order.formed.is_some());
            assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
            assert!(
                early_order_group
                    .as_ref()
                    .unwrap()
                    .action_before
                    .as_ref()
                    .unwrap()
                    .orders()
                    .any(|o| o.token_id == Some(&TokenId(DOWN_TOKEN_ID.into()))
                        && o.origin.is_none())
            );
        }
        if at == 23_942 {
            assert_eq!(market.success_shares[a].0, "0", "原经济前驱尚未到达");
            assert_eq!(
                market.repair_gap_shares[b].0, "10",
                "实际 FinalSuccess 已公开"
            );
            let actor = NativeActor::new(group.clone(), 0).unwrap();
            assert_eq!(
                actor.root_upper(Family::SubmitSingleBuy).unwrap(),
                BigUint::from(1000u32)
            );
            assert!(read.pending_noe_record_count() > 0);
            saw_economic_pending = true;
        }
        if !sent && market.success_shares[a].0.parse::<Decimal>().unwrap() == Decimal::from(10) {
            buy(&mut h, "repair-b-four", DOWN_TOKEN_ID, "5").await;
            eprintln!("repair send B cutoff={at}");
            sent = true;
            continue;
        }
        if sent {
            let actor = NativeActor::new(group.clone(), 0).unwrap();
            let gap = market.repair_gap_shares[b].0.parse::<Decimal>().unwrap();
            let commitment = h.shared.registry().committed_target_shares(
                &ConditionId(CONDITION_ID.into()),
                &TokenId(DOWN_TOKEN_ID.into()),
            );
            if commitment == ExecutionReadValueV1::Known(ExactDecimal("5".into())) {
                assert_eq!(gap, Decimal::from(10));
                assert_eq!(
                    actor.root_upper(Family::SubmitSingleBuy).unwrap(),
                    BigUint::from(500u32)
                );
                if !saw_pending {
                    eprintln!("repair pending cutoff={at}");
                }
                saw_pending = true;
            } else if commitment == ExecutionReadValueV1::Known(ExactDecimal("0".into())) {
                assert_eq!(gap, Decimal::from(5));
                assert_eq!(
                    actor.root_upper(Family::SubmitSingleBuy).unwrap(),
                    BigUint::from(500u32)
                );
                if !saw_terminal {
                    eprintln!("repair terminal cutoff={at}");
                }
                saw_terminal = true;
                if market.success_shares[b].0.parse::<Decimal>().unwrap() == Decimal::ZERO {
                    saw_economic_pending = true;
                    eprintln!(
                        "repair final-before-economic cutoff={at} pending={} gap={gap} cap=500",
                        read.pending_noe_record_count()
                    );
                }
            }
        }
        if at == 29_001 {
            break;
        }
    }
    assert!(
        sent && saw_pending && saw_terminal && early_order_group.is_some(),
        "真实前史与终局须实际覆盖"
    );
    let evidence = h._temp.keep();
    eprintln!("repair evidence {}", evidence.display());
    assert!(saw_economic_pending, "必须覆盖终局已公开但经济尚未应用");
}

#[cfg(feature = "python")]
#[test]
fn python_features_consumes_real_native_groups_with_the_original_public_shape() {
    use pyo3::prelude::*;
    let mut h = fixture_config_times(
        "2000",
        ["0.0025", "0.005"],
        24_999,
        &[35_000, 319_000, 320_000],
    );
    let mut groups = 0;
    loop {
        let group = h.next().expect("有限公开来源含到期后的组");
        let retired = group
            .post_frame_hidden_ops
            .iter()
            .any(|op| matches!(op, HiddenOp::Retire { .. }));
        let expected = serde_json::to_value(&group).unwrap();
        Python::attach(|py| {
            let expected = crate::python::value(py, expected).unwrap();
            let native = Py::new(
                py,
                crate::python::PythonGroup {
                    group: Arc::new(group),
                },
            )
            .unwrap();
            let actual = native.call_method0(py, "features").unwrap();
            assert!(actual.bind(py).eq(expected.bind(py)).unwrap());
            let rows = actual.bind(py).get_item("rows").unwrap();
            for row in rows.try_iter().unwrap() {
                let row = row.unwrap();
                assert!(!row.contains("action_context").unwrap());
                assert_eq!(row.get_item("slow").unwrap().len().unwrap(), 433);
                assert_eq!(row.get_item("fast").unwrap().len().unwrap(), 596);
            }
        });
        groups += 1;
        if retired {
            break;
        }
    }
    assert!(groups >= 6);
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_cancel_clears_known_account_aggregates() {
    let mut h = fixture();
    let g = ready(&mut h);
    let s = selected(Family::SubmitPairedBuy, Variant::GtcLimit, "500");
    g.execute_selected(
        0,
        SelectedAction::Buy(&s),
        &mut h.shared,
        &mut h.driver.decision().unwrap(),
        context("known-buy-before-unknown-cancel"),
        |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
    )
    .await
    .unwrap()
    .unwrap();
    h.shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("unknown-cancel"),
            Some(Arc::new(ModelIntentV1::CancelOrder {
                platform_order_id: "sim-999".into(),
            })),
        )
        .await
        .unwrap()
        .unwrap();
    let mut found = false;
    while let Some(g) = h.next() {
        let read = g.action_before.as_ref().unwrap();
        let unknown = read.orders().filter(|o| o.condition_id.is_none()).count();
        let known = read.orders().filter(|o| o.condition_id.is_some()).count();
        if unknown != 0 && known != 0 {
            let slow = &g.rows[0].slow;
            assert_eq!(unknown, 1);
            assert_eq!(known, 2);
            for offset in [382, 385, 388, 391, 406] {
                assert_eq!(&slow[offset..offset + 3], &[0., 0., 1.], "slot {offset}");
            }
            found = true;
            break;
        }
    }
    assert!(
        found,
        "formal unknown cancellation must appear in actual frozen account read"
    );
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn model_selects_low_limit_quantity_and_platform_refusal_releases_original_reservation() {
    for minimum in ["5", "10"] {
        for variant in 0..2 {
            for n in [1u32, 499, 500, 999, 1000, 1001] {
                let mut h = fixture_with_minimum(
                    "2000",
                    ["0.1", "0.1"],
                    24_999,
                    &[35_000, 45_000, 55_000],
                    ["100", "100"],
                    minimum,
                );
                let group = Arc::new(ready(&mut h));
                assert_eq!(
                    group.rows[0].rule[14],
                    crate::exact::signed_log_f32(&crate::exact::integer(
                        minimum.parse::<u32>().unwrap()
                    ))
                );
                assert_eq!(group.rows[0].rule[15], 1.);
                assert_eq!(&group.rows[0].fast[566..596], &group.rows[0].rule);
                let mut actor = NativeActor::new(group, 0).unwrap();
                actor.choose(2).unwrap();
                while actor.state.stage == Stage::QuantityBranch {
                    let interval = actor.state.interval.as_ref().unwrap();
                    let midpoint = (&interval.lo + &interval.hi) / 2u32;
                    actor
                        .choose(usize::from(BigUint::from(n) > midpoint))
                        .unwrap();
                }
                assert_eq!(actor.state.quantity_hundredths, Some(n.into()));
                for _ in 0..2 {
                    actor.choose(variant).unwrap(); // GTC或GTD LIMIT
                    actor.choose(1).unwrap(); // post-only
                    if variant == 1 {
                        actor.choose(2).unwrap();
                    } // 原编码300秒有效寿命
                }
                actor.choose(0).unwrap();
                actor.choose(0).unwrap();
                let original = actor.intent().unwrap().unwrap();
                let ModelIntentV1::SubmitPairedBuy { orders, .. } = &original else {
                    panic!()
                };
                for order in orders {
                    assert_eq!(leg_values(&order.order).0, Decimal::new(i64::from(n), 2));
                }
                actor
                    .execute(
                        &mut h.shared,
                        &mut h.driver.decision().unwrap(),
                        context("model-low-limit"),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let expected_reserve = Decimal::new(i64::from(n) * 2, 3);
                assert_eq!(
                    Decimal::from_str_exact(
                        &h.shared
                            .ledger()
                            .economic_read()
                            .unwrap()
                            .cash()
                            .actual_reserved_pusd
                            .0
                    )
                    .unwrap(),
                    expected_reserve
                );
                let next = h.next().unwrap();
                let below = n < minimum.parse::<u32>().unwrap() * 100;
                let cash = next
                    .action_before
                    .as_ref()
                    .unwrap()
                    .economics()
                    .cash
                    .clone();
                assert_eq!(
                    Decimal::from_str_exact(&cash.actual_reserved_pusd.0).unwrap(),
                    if below {
                        Decimal::ZERO
                    } else {
                        expected_reserve
                    }
                );
                assert_eq!(cash.actual_reserved_pusd, cash.maker_reserved_pusd);
                let registry = h.shared.registry();
                assert_eq!(registry.orders().count(), if below { 0 } else { 2 });
                for order in registry.orders() {
                    assert_eq!(order.origin.as_ref().unwrap().intent.intent(), &original);
                    assert!(order.accepted.is_some());
                }
                h.driver.stop_and_drain().unwrap();
                let noe = ::execution_normalization::DurableContextStoreV4::open(
                    h._temp.path().join("noe"),
                    &h.noe_context,
                )
                .unwrap();
                let rejected = noe.commits().iter().flat_map(|commit| &commit.records).filter(|record| matches!(record,
                ::execution_normalization::NormalizationRecord::Evidence {
                    evidence: ::execution_normalization::SourceNeutralEvidenceKind::Control {
                        result: contracts::execution_normalization::SourceNeutralControlResult::Submission {
                            outcome: contracts::execution_normalization::SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
                        },
                    }, ..
                })).count();
                assert_eq!(
                    rejected,
                    if below { 2 } else { 0 },
                    "minimum={minimum}, n={n}"
                );
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn current_gtd_expires_at_effective_deadline_through_noe_ledger_and_native() {
    for (lifetime, seconds) in [
        (Lifetime::Seconds180, 180i64),
        (Lifetime::Seconds240, 240),
        (Lifetime::Seconds300, 300),
    ] {
        let deadline = (24 + seconds) * 1000;
        let mut h = fixture_config_times(
            "2000",
            ["0.0025", "0.005"],
            24_999,
            &[deadline - 1001, deadline, deadline + 1000],
        );
        let group = ready(&mut h);
        assert_eq!(group.cutoff.decision_time().logical_time_utc_ms(), 23_900);
        let mut selection = selected(Family::SubmitPairedBuy, Variant::GtdLimit, "500");
        for leg in &mut selection.legs {
            leg.lifetime = Some(lifetime);
        }
        group
            .execute_selected(
                0,
                SelectedAction::Buy(&selection),
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("effective-gtd"),
                |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
            )
            .await
            .unwrap()
            .unwrap();
        let mut before = false;
        let mut after = false;
        while let Some(group) = h.next() {
            let at = group.cutoff.decision_time().logical_time_utc_ms();
            if at == deadline - 1 {
                if !group.rows.is_empty() {
                    assert_eq!(group.cancel.iter().filter(|v| v[0] == 1.).count(), 2);
                }
                assert_eq!(
                    group
                        .action_before
                        .as_ref()
                        .unwrap()
                        .economics()
                        .cash
                        .actual_reserved_pusd
                        .0,
                    "0.75"
                );
                before = true;
            }
            if at >= deadline + 1000 {
                assert!(group.cancel.iter().all(|v| v[0] != 1.));
                let cash = &group.action_before.as_ref().unwrap().economics().cash;
                assert_eq!(cash.actual_reserved_pusd.0, "0");
                assert_eq!(cash.maker_reserved_pusd.0, "0");
                for order in h.shared.registry().orders() {
                    let record = order.latest_lifecycle.unwrap();
                    let (source, received) = crate::values::times(Some(record)).unwrap();
                    assert_eq!(source, deadline);
                    assert!(received > source && received <= at);
                    let (_, original) = crate::values::leg(order.origin.as_ref().unwrap()).unwrap();
                    let BuyOrderIntentV1::GtdLimit {
                        expires_at_utc_sec, ..
                    } = original.order
                    else {
                        panic!()
                    };
                    assert_eq!(expires_at_utc_sec, deadline / 1000 + 60);
                }
                after = true;
                break;
            }
        }
        assert!(before && after);
        h.driver.stop_and_drain().unwrap();
    }
}
