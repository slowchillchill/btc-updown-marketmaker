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
    for token in [UP_TOKEN_ID, DOWN_TOKEN_ID] {
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
                    size: "100".into(),
                }],
                last_trade_price: None,
            },
        ));
    }
    for (at, visible, token, old, new) in [
        (23_000, 23_900, UP_TOKEN_ID, "0.01", "0.0025"),
        (23_001, 23_900, DOWN_TOKEN_ID, "0.01", "0.005"),
        (24_000, 24_999, DOWN_TOKEN_ID, "0.005", "0.01"),
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
    for at in [35_000, 45_000, 55_000] {
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
    let native = NativeInputRoot::new("buy".into(), account, -10_000);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, TradingTimes(times)).unwrap();
    Harness {
        _temp: temp,
        shared,
        native,
        driver: FinitePublicConsumptionV1::new(execution, source, "buy".into()),
        groups: vec![],
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
