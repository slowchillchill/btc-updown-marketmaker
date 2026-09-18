//! typed_raw → 公共归约的字段反例；完整 SIM/NOE/Ledger 宿主另由 public_input_tests 覆盖。
use crate::{exact::*, market::*, public_input_tests::common::*};
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::PublicObservationCoreV1, typed_raw::*,
};
fn push(
    core: &mut PublicObservationCoreV1,
    input: RecordedIngressV1,
) -> MarketBusinessObservationV1 {
    let p = prepare_public_ingress_v1(core, &input, &Default::default(), &[]).unwrap();
    let a = core.push(&p).unwrap();
    a.applied.closed_groups.last().unwrap().observations[0].clone()
}
fn level(p: &str, q: &str) -> PolymarketPriceLevel {
    PolymarketPriceLevel {
        price: p.into(),
        size: q.into(),
    }
}
fn book(q: &str) -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::Book {
        asset_id: UP_TOKEN_ID.into(),
        bids: vec![
            level("0.5", "1"),
            level("0.4", "2"),
            level("0.3", "3"),
            level("0.2", "4"),
            level("0.1", "5"),
            level("0.01", q),
        ],
        asks: vec![level("0.6", "2")],
        last_trade_price: None,
    }
}
fn bba() -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::BestBidAsk {
        asset_id: UP_TOKEN_ID.into(),
        best_bid: Some("0.4".into()),
        best_ask: Some("0.6".into()),
        spread: None,
    }
}
#[test]
fn distant_depth_and_previous_unknown_are_not_trade_or_cross_gap_changes() {
    let mut c = core();
    push(&mut c, meta(0));
    let mut h = MarketHistory::default();
    let o = push(&mut c, pm(1, 1, 21000, book("6")));
    let a = encode(&o, &[0.; 30], 23000, &mut h);
    assert_eq!(a[43], 1.);
    assert_eq!(a[45], signed_log_f32(&integer(21)));
    assert_eq!(a[47], 0.);
    let o = push(&mut c, pm(2, 2, 21001, book("16")));
    let b = encode(&o, &[0.; 30], 24000, &mut h);
    assert_eq!(b[45], signed_log_f32(&integer(31)));
    assert_eq!(b[46], signed_log_f32(&integer(10)));
    assert_eq!(b[47], 1.);
    for i in 0..5 {
        assert_eq!(&a[57 + 8 * i..60 + 8 * i], &b[57 + 8 * i..60 + 8 * i]);
    }
    // 同一个生产侧编码器的缺源读取，既有合法档位引用保持，前一帧未知不跨洞。
    let mut previous = super::market::SidePrevious::default();
    let mut out = [0.; 56];
    super::market::side(
        &mut out,
        0,
        Some(vec![(integer(1), integer(2))]),
        vec![],
        vec![],
        false,
        &mut previous,
    );
    super::market::side(&mut [0.; 56], 1, None, vec![], vec![], false, &mut previous);
    let mut restored = [0.; 56];
    super::market::side(
        &mut restored,
        2,
        Some(vec![(integer(1), integer(3))]),
        vec![],
        vec![],
        false,
        &mut previous,
    );
    assert_eq!(restored[4], 0.);
    assert_eq!(restored[55], 0.);
}
#[test]
fn same_value_new_fact_appends_but_unrelated_group_does_not_and_history_is_sixteen() {
    let mut c = core();
    push(&mut c, meta(0));
    let mut h = MarketHistory::default();
    let o = push(&mut c, pm(1, 1, 21000, bba()));
    let a = encode(&o, &[0.; 30], 23000, &mut h);
    assert_eq!(a[782], 1. / 16.);
    assert_eq!(a[412], 0.); // spread absent, direct time retained
    assert_eq!(a[413], 0.);
    assert_ne!(a[414], 0.);
    let o = push(&mut c, pm(2, 2, 21001, book("6")));
    let b = encode(&o, &[0.; 30], 24000, &mut h);
    assert_eq!(b[782], 1. / 16.);
    for i in 3..22 {
        let o = push(&mut c, pm(i, i, 21000 + i as i64, bba()));
        let b = encode(&o, &[0.; 30], 24000 + i as i64, &mut h);
        assert_eq!(b[782], ((i - 1).min(16) as f32) / 16.);
        assert_eq!(b[783], 0.);
        assert_eq!(b[784], 0.);
    }
    assert_eq!(h.series[0].len(), 16);
    assert!(h.series[1..].iter().all(|q| q.is_empty()));
}
#[test]
fn direct_time_missing_source_negative_age_and_multiple_anchors_use_independent_pairs() {
    // 时间摘要单元使用真实源锚点并仅删除来源时间，模拟公共可空字段。
    let mut c = core();
    push(&mut c, meta(0));
    let o = push(&mut c, pm(1, 1, 23000, bba()));
    let actual = &o
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
        .unwrap()
        .best_bid_ask
        .direct_anchors[0];
    assert_eq!(
        super::market::t5(21000, Some(actual)),
        [
            signed_log_f32(&integer(-2)),
            signed_log_f32(&integer(-1)),
            signed_log_f32(&integer(-1)),
            1.,
            1.
        ]
    );
    let mut missing = actual.clone();
    missing.source_event_at_utc_ms = None;
    assert_eq!(
        super::market::t5(22000, Some(&missing)),
        [0., 0., 0., 0., 1.]
    );
    let mut other = actual.clone();
    other.source_event_at_utc_ms = Some(20000);
    other.received_at_utc_ms = 24000;
    let e = super::market::envelope(21000, &[actual, &missing, &other]);
    assert_eq!(
        e,
        [
            signed_log_f32(&integer(-2)),
            signed_log_f32(&integer(1)),
            signed_log_f32(&integer(-3)),
            signed_log_f32(&integer(-1)),
            signed_log_f32(&integer(-1)),
            signed_log_f32(&integer(4)),
            round_f32(&ratio(2, 3)),
            1.,
            round_f32(&ratio(2, 3))
        ]
    );
}
#[test]
fn binance_partial_update_retains_prices_without_inventing_level_time_or_history() {
    let mut c = core();
    push(&mut c, meta(0));
    let dl = |p: &str, q: &str| DepthLevel {
        price: p.into(),
        quantity: q.into(),
    };
    let snapshot = BinanceDepthSnapshotRow {
        timestamp_received: 20001,
        snapshot_fetch_start_utc: 19000,
        last_update_id: 100,
        bids: vec![dl("64000", "0.5"), dl("64100", "0.7")],
        asks: vec![dl("64300", "0.3"), dl("64200", "0.4")],
        symbol: "BTCUSDT".into(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000".into(),
        snapshot_status: "complete".into(),
        source_sequence_epoch_id: "depth".into(),
        connection_generation: 1,
        depth_ingress_sequence: 1,
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: 20002,
        collector_host_id: "collector-1".into(),
        collector_region: "eu-west-1".into(),
        schema_version: "binance_depth_snapshot_v3".into(),
    };
    let a = push(
        &mut c,
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot)),
        ),
    );
    let update = BinanceDepthUpdateRow {
        timestamp_received: 22000,
        event_type: "depthUpdate".into(),
        event_time: 21000,
        symbol: "BTCUSDT".into(),
        first_update_id: 101,
        final_update_id: 101,
        bids: vec![dl("64100", "0.2")],
        asks: vec![],
        stream: "btcusdt@depth".into(),
        snapshot_last_update_id: Some(100),
        local_u_before: None,
        stitch_verdict: BinanceDepthStitchVerdict::Applied,
        stitch_evidence: Some("continuous_depth_update".into()),
        stitch_gap_reason: None,
        source_sequence_epoch_id: "depth".into(),
        connection_generation: 1,
        depth_ingress_sequence: 2,
        collector_admission_sequence: 2,
        collector_admission_time_utc_ms: 22001,
        collector_host_id: "collector-1".into(),
        collector_region: "eu-west-1".into(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".into(),
        schema_version: "binance_depth_update_v4".into(),
    };
    let b = push(
        &mut c,
        record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update)),
        ),
    );
    let mut h = MarketHistory::default();
    encode(&a, &[0.; 30], 23000, &mut h);
    let f = encode(&b, &[0.; 30], 24000, &mut h);
    assert_eq!(f[279], 1.);
    assert_eq!(f[281], signed_log_f32(&ratio(7, 10)));
    assert_eq!(f[282], signed_log_f32(&ratio(-1, 2)));
    assert_eq!(f[283], 1.);
    for start in [279, 335] {
        assert_eq!(&f[start + 5..start + 14], &[0.; 9]);
        for i in 0..2 {
            assert_eq!(f[start + 16 + 8 * i], 1.);
            assert_eq!(&f[start + 17 + 8 * i..start + 22 + 8 * i], &[0.; 5]);
        }
    }
    assert_eq!(f[335 + 14], signed_log_f32(&integer(64200)));
    assert_eq!(f[335 + 15], signed_log_f32(&ratio(4, 10)));
    assert_eq!(&f[790..794], &[0.; 4]);
    assert_eq!(f[575 + 17], signed_log_f32(&integer(1)));
    assert_ne!(f[575 + 23 + 6 * 9], 0.);
}
#[test]
fn full_group_statistics_keep_incomplete_members_and_original_order() {
    let mut c = core();
    push(&mut c, meta(0));
    let mut members = Vec::new();
    for (i, (p, q, s, fee)) in [
        ("0.2", Some("2"), Some(PolymarketSide::Buy), Some("3")),
        ("0.6", None, None, None),
        ("0.4", Some("3"), Some(PolymarketSide::Unknown), Some("5")),
    ]
    .into_iter()
    .enumerate()
    {
        let o = push(
            &mut c,
            pm(
                i as u64 + 1,
                i as u64 + 1,
                21000 + i as i64,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: p.into(),
                    size: q.map(str::to_owned),
                    side: s,
                    fee_rate_bps: fee.map(str::to_owned),
                },
            ),
        );
        members.extend(o.group_events);
    }
    // 同一生产统计器消费上述真实公共原成员；不构造模型帧或假完整成交。
    let mut rows = Vec::new();
    for event in &members {
        if let PublicBusinessPayloadV1::PolymarketMessage(m) = &event.payload
            && let CanonicalPmPayloadV1::PublicTrade {
                price_pusd_per_share,
                quantity_shares,
                side,
                fee_rate_bps,
                ..
            } = &m.payload
        {
            rows.push(super::market::Trade {
                p: decimal(price_pusd_per_share),
                q: quantity_shares.as_ref().map(decimal),
                side: *side,
                fee: fee_rate_bps.as_ref().map(decimal),
                time: (
                    event.direct_time.source_event_at_utc_ms,
                    Some(event.direct_time.received_at_utc_ms),
                ),
            });
        }
    }
    let mut out = [0.; 28];
    super::market::trades(&mut out, rows.into_iter(), 23000, true);
    assert_eq!(out[0], signed_log_f32(&integer(3)));
    assert_eq!(out[1], -0.6);
    assert_eq!(out[2], -0.2);
    assert_eq!(out[5], -0.2);
    assert_eq!(out[6], signed_log_f32(&integer(5)));
    assert_eq!(out[7], round_f32(&ratio(2, 3)));
    assert_eq!(out[8], signed_log_f32(&ratio(16, 10)));
    assert_eq!(out[11], signed_log_f32(&integer(1)));
    assert_eq!(out[12], signed_log_f32(&integer(1)));
    assert_eq!(out[17], signed_log_f32(&integer(4)));
    assert_eq!(out[18], round_f32(&ratio(2, 3)));
}
#[test]
fn same_frame_independent_ticks_and_exact_prefix_keep_unrounded_selection() {
    use crate::prefix::*;
    use num_bigint::BigUint;
    use sim_system::platform_rules::*;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let bytes = std::fs::read(&path).unwrap();
    use sha2::{Digest, Sha256};
    let snapshot = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: path.clone(),
        replay_causal_start_utc_ms: i64::MAX,
        expected_file_sha256: Sha256Hex(format!("{:x}", Sha256::digest(&bytes))),
    })
    .unwrap();
    let mut c = core();
    push(&mut c, meta(0));
    let o = push(
        &mut c,
        pm(
            1,
            1,
            21000,
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: DOWN_TOKEN_ID.into(),
                old_tick_size: Some("0.01".into()),
                new_tick_size: "0.001".into(),
            },
        ),
    );
    let r = rules(&o, &snapshot.public_rule_context(o.market.clone()), 23000);
    assert_eq!(r[0], signed_log_f32(&ratio(1, 100)));
    assert_eq!(r[7], signed_log_f32(&ratio(1, 1000)));
    assert_eq!(r[1], 1.);
    assert_eq!(r[8], 1.);
    assert_eq!(
        &encode(&o, &r, 23000, &mut MarketHistory::default())[806..],
        &r
    );
    let mut p = PrefixState {
        family: Some(Family::SubmitSingleBuy),
        stage: Stage::Price(Leg::B),
        single_leg: Some(Leg::B),
        ..Default::default()
    };
    p.select_quantity(BigUint::from(500u32));
    p.legs[1] = LegSelection {
        variant: Some(Variant::GtcLimit),
        post_only: Some(false),
        price_index: Some(BigUint::from(500u32)),
        ..Default::default()
    };
    let v = p.encode(&[Some(ratio(1, 100)), Some(ratio(1, 1000))]);
    assert_eq!(&v[24..42], &[0.; 18]);
    assert_eq!(v[57], 0.);
    assert_eq!(v[59], 1.);
    assert_eq!(v[48], 1.);
    assert_eq!(v[51], 1.);
    assert_eq!(&v[17..21], &[0.; 4]);
    let upper = (BigUint::from(1u8) << 200usize) + BigUint::from(1u8);
    p = PrefixState {
        family: Some(Family::SubmitSingleBuy),
        stage: Stage::QuantityBranch,
        single_leg: Some(Leg::B),
        ..Default::default()
    };
    p.interval = Some(QuantityInterval {
        upper: upper.clone(),
        lo: &upper - BigUint::from(1u8),
        hi: upper.clone(),
    });
    let v = p.encode(&[None, None]);
    assert_eq!(v[18], v[19]);
    assert_ne!(
        p.interval.as_ref().unwrap().lo,
        p.interval.as_ref().unwrap().hi
    );
}
#[test]
fn changing_real_midpoints_feed_exact_increment_rms() {
    let mut c = core();
    push(&mut c, meta(0));
    let mut h = MarketHistory::default();
    let mut result = [0.; 836];
    for (i, (bid, ask)) in [("0.1", "0.3"), ("0.3", "0.5"), ("0.2", "0.4")]
        .into_iter()
        .enumerate()
    {
        let o = push(
            &mut c,
            pm(
                i as u64 + 1,
                i as u64 + 1,
                21000 + i as i64,
                PolymarketOrderbookEvent::BestBidAsk {
                    asset_id: UP_TOKEN_ID.into(),
                    best_bid: Some(bid.into()),
                    best_ask: Some(ask.into()),
                    spread: Some("0.2".into()),
                },
            ),
        );
        result = encode(&o, &[0.; 30], 23000 + i as i64, &mut h);
    }
    assert_eq!(result[782], 3. / 16.);
    assert_eq!(result[783], signed_log_f32(&ratio(-1, 10)));
    // 独立 Decimal(precision=120) sqrt/ln：sqrt((.2²+.1²)/2)=sqrt(.025)。
    assert_eq!(result[784], sqrt_log_f32(&ratio(25, 1000)));
    assert_eq!(result[785], signed_log_f32(&ratio(2, 1000)));
    for (n, d, bits) in [
        (5, 100, 0x3e4ea56b),
        (1, 100, 0x3dc331fc),
        (7, 3, 0x3f6d5fa5),
    ] {
        assert_eq!(sqrt_log_f32(&ratio(n, d)).to_bits(), bits);
    }
}
