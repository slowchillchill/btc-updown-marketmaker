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
    let unchanged = encode(&o, &[0.; 30], 25000, &mut h);
    assert_eq!(unchanged[46], 0.);
    assert_eq!(unchanged[47], 1.);
    // 同一个生产侧编码器的缺源读取，既有合法档位引用保持，前一帧未知不跨洞。
    let mut previous = super::market::SidePrevious::default();
    let first = (ExactDecimal("1".into()), ExactDecimal("2".into()));
    let last = (ExactDecimal("1".into()), ExactDecimal("3".into()));
    let mut out = [0.; 16];
    super::market::side(
        &crate::exact::InputProjection::default(),
        &mut out,
        0,
        Some(vec![(&first.0, &first.1)]),
        vec![],
        "",
        false,
        &mut previous,
        &mut std::sync::Arc::default(),
        true,
    );
    super::market::side(
        &crate::exact::InputProjection::default(),
        &mut [0.; 16],
        1,
        None,
        vec![],
        "",
        false,
        &mut previous,
        &mut std::sync::Arc::default(),
        true,
    );
    let mut restored = [0.; 16];
    super::market::side(
        &crate::exact::InputProjection::default(),
        &mut restored,
        2,
        Some(vec![(&last.0, &last.1)]),
        vec![],
        "",
        false,
        &mut previous,
        &mut std::sync::Arc::default(),
        true,
    );
    assert_eq!(restored[4], 0.);
    assert_eq!(restored[15], 0.);
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
        super::market::t5(
            &crate::exact::InputProjection::default(),
            21000,
            Some(actual)
        ),
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
        super::market::t5(
            &crate::exact::InputProjection::default(),
            22000,
            Some(&missing)
        ),
        [0., 0., 0., 0., 1.]
    );
    let mut other = actual.clone();
    other.source_event_at_utc_ms = Some(20000);
    other.received_at_utc_ms = 24000;
    let e = super::market::envelope(
        &crate::exact::InputProjection::default(),
        21000,
        &[actual, &missing, &other],
    );
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
fn block_parallel_projection_keeps_coordinates_and_unique_direct_times() {
    let mut c = core();
    push(&mut c, meta(0));
    let observation = push(
        &mut c,
        pm(
            1,
            1,
            23_000,
            PolymarketOrderbookEvent::Book {
                asset_id: UP_TOKEN_ID.into(),
                bids: (0..129)
                    .map(|i| PolymarketPriceLevel {
                        price: rust_decimal::Decimal::new(5000 - i, 4).to_string(),
                        size: (i + 1).to_string(),
                    })
                    .collect(),
                asks: vec![],
                last_trade_price: None,
            },
        ),
    );
    let fact = &observation
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
        .unwrap()
        .bids;
    let values = fact.value.as_ref().unwrap();
    let levels: Vec<_> = values
        .iter()
        .map(|p| {
            (
                decimal(&p.price_pusd_per_share),
                decimal(&p.quantity_shares),
            )
        })
        .collect();
    let paths: Vec<_> = values
        .iter()
        .map(|p| format!("assets[{}].bids[{}]", UP_TOKEN_ID, p.price_pusd_per_share.0))
        .collect();
    let mut anchors = fact.direct_anchors.clone();
    // 已有合同允许缺少源时间；重复选中锚点不能被路径索引默默取第一条。
    anchors
        .iter_mut()
        .find(|a| a.field_path == paths[64])
        .unwrap()
        .source_event_at_utc_ms = None;
    anchors.push(
        anchors
            .iter()
            .find(|a| a.field_path == paths[0])
            .unwrap()
            .clone(),
    );
    for threads in [1, 4, 16] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        for now in [24_000, 25_000] {
            let mut out = [0.; 16];
            let mut rows = std::sync::Arc::default();
            let mut previous = super::market::SidePrevious::default();
            pool.install(|| {
                super::market::side(
                    &crate::exact::InputProjection::default(),
                    &mut out,
                    now,
                    Some(
                        values
                            .iter()
                            .map(|p| (&p.price_pusd_per_share, &p.quantity_shares))
                            .collect(),
                    ),
                    anchors.iter().collect(),
                    &format!("assets[{}].bids[", UP_TOKEN_ID),
                    true,
                    &mut previous,
                    &mut rows,
                    true,
                )
            });
            assert_eq!(rows.len(), levels.len());
            for (i, (p, q)) in levels.iter().enumerate() {
                let selected: Vec<_> = anchors
                    .iter()
                    .filter(|a| a.field_path == paths[i])
                    .collect();
                let expected_time = super::market::t5(
                    &crate::exact::InputProjection::default(),
                    now,
                    if selected.len() == 1 {
                        Some(selected[0])
                    } else {
                        None
                    },
                );
                let expected = [
                    round_f32(&p.mul(&integer(2)).sub(&integer(1))),
                    signed_log_f32(q),
                    1.,
                    expected_time[0],
                    expected_time[1],
                    expected_time[2],
                    expected_time[3],
                    expected_time[4],
                    signed_log_f32(&levels[0].0.sub(p)),
                ];
                assert_eq!(
                    rows[i].map(f32::to_bits),
                    expected.map(f32::to_bits),
                    "threads={threads} index={i}"
                );
            }
            assert_eq!(&rows[0][3..8], &[0.; 5]);
            assert_eq!(rows[64][6], 0.);
            assert_eq!(rows[64][7], 1.);
        }
    }
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
    let mut compact_core = core();
    let missing = push(&mut compact_core, meta(0));
    compact_core.set_observation_requirements(PublicObservationRequirementsV1 {
        binance_depth_quantities: false,
    });
    let compact = push(
        &mut compact_core,
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot.clone())),
        ),
    );
    let a = push(
        &mut c,
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot)),
        ),
    );
    assert!(missing.binance_depth.value.is_none());
    assert!(matches!(
        compact.binance_depth.value,
        Some(BinanceDepthObservationV1::NotMaterialized)
    ));
    assert!(a.binance_depth.value.as_ref().unwrap().full().is_some());
    assert_eq!(compact.binance_depth.quality, a.binance_depth.quality);
    let projection = crate::exact::InputProjection::default();
    let (full_values, full_books) = super::market::encode(
        &projection,
        &a,
        &[0.; 30],
        23000,
        &mut MarketHistory::default(),
    );
    let (compact_values, compact_books) = super::market::encode(
        &projection,
        &compact,
        &[0.; 30],
        23000,
        &mut MarketHistory::default(),
    );
    let (missing_values, _) = super::market::encode(
        &projection,
        &missing,
        &[0.; 30],
        23000,
        &mut MarketHistory::default(),
    );
    // 省略数量仍是同一深度事件与来源；质量不会被读成缺失。
    assert_eq!(&full_values[258..439], &compact_values[258..439]);
    assert_eq!(&full_values[113..119], &compact_values[113..119]);
    assert_ne!(&missing_values[113..119], &compact_values[113..119]);
    assert!(full_books[4..].iter().all(|side| !side.is_empty()));
    assert!(compact_books[4..].iter().all(|side| side.is_empty()));
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
        members.extend(o.group_events.iter().cloned());
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
    super::market::trades(
        &crate::exact::InputProjection::default(),
        &mut out,
        rows.into_iter(),
        23000,
        true,
    );
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
    let context = snapshot.public_rule_context(o.market.clone());
    let tail = static_rules(&context);
    let projection = InputProjection::default();
    let r = rules_with_static(&projection, &o, 23000, &tail);
    let later = rules_with_static(&projection, &o, 25000, &tail);
    assert_eq!(&r[14..], &later[14..]);
    assert_ne!(&r[..14], &later[..14]);
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
    assert_eq!(&v[22..29], &[0.; 7]);
    assert_eq!(v[33], 0.);
    assert_eq!(v[35], 1.);
    assert_eq!(v[29], 1.);
    assert_eq!(v[32], 1.);
    assert_eq!(&v[15..19], &[0.; 4]);
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
    assert_eq!(v[16], v[17]);
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
    // S1：已有非零波动后，非历史事件保持结果；同值新身份仍追加并淘汰。
    let previous_history = result[782..806].to_vec();
    let unchanged = push(&mut c, pm(4, 4, 21003, book("6")));
    let unchanged = encode(&unchanged, &[0.; 30], 23003, &mut h);
    assert_eq!(&unchanged[782..806], previous_history.as_slice());
    let mut expected_values = std::collections::VecDeque::from([
        (ratio(2, 10), 23000),
        (ratio(4, 10), 23001),
        (ratio(3, 10), 23002),
    ]);
    for i in 5..25 {
        let now = 23000 + i as i64;
        let o = push(
            &mut c,
            pm(
                i,
                i,
                21000 + i as i64,
                PolymarketOrderbookEvent::BestBidAsk {
                    asset_id: UP_TOKEN_ID.into(),
                    best_bid: Some("0.2".into()),
                    best_ask: Some("0.4".into()),
                    spread: Some("0.2".into()),
                },
            ),
        );
        let actual = encode(&o, &[0.; 30], now, &mut h);
        expected_values.push_back((ratio(3, 10), now));
        if expected_values.len() > 16 {
            expected_values.pop_front();
        }
        let squares = expected_values
            .iter()
            .zip(expected_values.iter().skip(1))
            .map(|(a, b)| {
                let difference = b.0.sub(&a.0);
                difference.mul(&difference)
            })
            .fold(integer(0), |a, b| a.add(&b));
        let expected = [
            (expected_values.len() as f32) / 16.,
            0.,
            sqrt_log_f32(&squares.divided_by(&integer(expected_values.len() - 1))),
            signed_log_f32(&ratio(now - expected_values.front().unwrap().1, 1000)),
        ];
        assert_eq!(
            actual[782..786]
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            expected.map(f32::to_bits)
        );
        assert!(actual[786..806].iter().all(|v| v.to_bits() == 0));
    }
    for (n, d, bits) in [
        (5, 100, 0x3e4ea56b),
        (1, 100, 0x3dc331fc),
        (7, 3, 0x3f6d5fa5),
    ] {
        assert_eq!(sqrt_log_f32(&ratio(n, d)).to_bits(), bits);
    }
}

#[test]
fn collected_sdk_reverse_order_replays_winner_and_native_a_b_without_label_inference() {
    use data_pipeline_training::{
        public_recording::PublicRecordingWriterV1, public_replay::PublicReplayV1,
    };
    use std::sync::Arc;
    // data-collect的正式SDK→发现→采集V6→终态测试逐值校验这个共同夹具。
    let rows: Vec<TypedRawRowV2> = serde_json::from_str(include_str!(
        "../../../data-pipeline/core/tests/fixtures/gamma-reversed-sdk-metadata-v6.json"
    ))
    .unwrap();
    let mut core = core();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("dpt");
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut hot = Vec::new();
    for (i, row) in rows.into_iter().enumerate() {
        let input = record(i as u64, row);
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        hot.push(applied.applied.closed_groups.last().unwrap().observations[0].clone());
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    drop(writer);
    let mut replay = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    let mut replayed = Vec::new();
    while let Some((_, applied)) = replay.next_applied().unwrap() {
        replayed.push(applied.applied.closed_groups.last().unwrap().observations[0].clone());
    }
    assert_eq!(hot, replayed);
    let (execution, _, snapshot) = execution_with_rules_v4(
        &directory.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "2000",
    );
    for observation in &replayed {
        let rules = Arc::new(snapshot.public_rule_context(observation.market.clone()));
        let context =
            crate::NativeActionContext::from_observation(observation, rules, snapshot.clone());
        assert_eq!(
            context.tokens,
            [Some(TokenId("101".into())), Some(TokenId("102".into()))]
        );
        let up = observation
            .assets
            .iter()
            .find(|a| a.asset.token_id.0 == "101")
            .unwrap();
        assert_eq!(up.asset.outcome.label, "Up");
        assert_eq!(up.asset.outcome.index, 0);
    }
    let MarketResolutionTerminalV3::Resolved {
        winning_asset_id,
        winning_outcome,
        payout_vector,
        ..
    } = replayed[1].terminal.value.as_ref().unwrap();
    assert_eq!(winning_asset_id, "101");
    assert_eq!(winning_outcome, "Up");
    for payout in payout_vector {
        assert_eq!(
            payout.payout_pusd_per_share.0,
            if payout.asset.token_id.0 == "101" {
                "1"
            } else {
                "0"
            }
        );
    }
    execution.stop_and_drain().unwrap();
}

// 保留原 v4 的独立预期坐标，验证 v5 前五档可恢复旧240槽及其余596槽。
// 仅测试使用；生产不能把 v4 向量补成 v5 全盘口。
fn encode(
    o: &MarketBusinessObservationV1,
    rule: &[f32; 30],
    now: i64,
    h: &mut MarketHistory,
) -> [f32; 836] {
    let (fast, books) =
        super::market::encode(&crate::exact::InputProjection::default(), o, rule, now, h);
    let starts = [57, 113, 175, 231, 293, 349];
    let mut out = [0.; 836];
    let mut target = 0;
    let mut source = 0;
    for (side, start) in starts.into_iter().enumerate() {
        out[target..start].copy_from_slice(&fast[source..source + start - target]);
        source += start - target;
        for (index, row) in books[side].iter().take(5).enumerate() {
            out[start + index * 8..start + (index + 1) * 8].copy_from_slice(&row[..8]);
        }
        target = start + 40;
    }
    out[target..].copy_from_slice(&fast[source..]);
    out
}

fn incremental_dependency_observations() -> Vec<MarketBusinessObservationV1> {
    let mut c = core();
    push(&mut c, meta(0));
    let mut output = vec![
        push(&mut c, pm(1, 1, 21000, book("6"))),
        push(&mut c, pm(2, 2, 22000, book("16"))),
        push(&mut c, pm(3, 3, 23000, bba())),
    ];
    for (index, prices) in [
        vec!["99999", "99998", "99997"],
        vec!["99999.01", "99999", "99998"],
        vec!["99999", "99998", "99997"],
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = index as u64 + 1;
        let at = 24000 + index as i64 * 1000;
        let snapshot = BinanceDepthSnapshotRow {
            timestamp_received: at,
            snapshot_fetch_start_utc: at - 10,
            last_update_id: 99 + sequence,
            bids: prices
                .into_iter()
                .map(|price| DepthLevel {
                    price: price.into(),
                    quantity: "2".into(),
                })
                .collect(),
            asks: vec![DepthLevel {
                price: "100000".into(),
                quantity: "3".into(),
            }],
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: "depth".into(),
            connection_generation: 1,
            depth_ingress_sequence: sequence,
            collector_admission_sequence: sequence,
            collector_admission_time_utc_ms: at + 1,
            collector_host_id: "collector-1".into(),
            collector_region: "eu-west-1".into(),
            schema_version: "binance_depth_snapshot_v3".into(),
        };
        output.push(push(
            &mut c,
            record(
                4 + index as u64,
                TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot)),
            ),
        ));
    }
    for (input, changes) in [
        (7, vec![("0.52", "4")]),
        (8, vec![("0.5", "0"), ("0.52", "0")]),
    ] {
        output.push(push(
            &mut c,
            pm(
                input,
                input - 3,
                20000 + input as i64 * 1000,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: changes
                        .into_iter()
                        .map(|(price, size)| PolymarketPriceChangeEntry {
                            asset_id: UP_TOKEN_ID.into(),
                            price: price.into(),
                            size: Some(size.into()),
                            side: PolymarketSide::Buy,
                            best_bid: None,
                            best_ask: None,
                        })
                        .collect(),
                },
            ),
        ));
    }
    output.push(push(
        &mut c,
        pm(
            9,
            6,
            30000,
            PolymarketOrderbookEvent::Book {
                asset_id: UP_TOKEN_ID.into(),
                bids: vec![],
                asks: vec![],
                last_trade_price: None,
            },
        ),
    ));
    output.push(push(&mut c, pm(10, 7, 31000, book("6"))));
    output
}

#[test]
fn incremental_inputs_keep_exact_best_dependencies_time_and_published_versions() {
    let mut history = MarketHistory::default();
    let observations = incremental_dependency_observations();
    let values = observations
        .iter()
        .enumerate()
        .map(|(i, o)| {
            super::market::encode(
                &crate::exact::InputProjection::default(),
                o,
                &[0.; 30],
                33000 + i as i64,
                &mut history,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(values[0].0[45], signed_log_f32(&integer(21)));
    assert_eq!(values[1].0[46], signed_log_f32(&integer(10)));
    assert_eq!(values[2].0[46], 0.);
    assert_ne!(&values[1].1[0][0][3..8], &values[2].1[0][0][3..8]);
    // 最优精确价变化，但价格投影碰撞：原99999档距离必须从0变成精确0.01。
    assert_eq!(
        values[3].1[4][0][0].to_bits(),
        values[4].1[4][0][0].to_bits()
    );
    assert_eq!(values[3].1[4][0][8], 0.);
    assert_eq!(
        values[4].1[4][1][8].to_bits(),
        signed_log_f32(&ratio(1, 100)).to_bits()
    );
    assert_eq!(values[5].1[4][0][8], 0.);
    assert_eq!(values[6].1[0].len(), 7);
    assert_eq!(values[7].1[0].len(), 5);
    assert!(values[8].1[0].is_empty());
    assert_eq!(values[9].1[0].len(), 6);
    // 早期结果仍由上面的values持有，后续重排/删除不能改写它们。
    assert_eq!(
        values[0].1[0][5][1].to_bits(),
        signed_log_f32(&integer(6)).to_bits()
    );
    assert_eq!(
        values[1].1[0][5][1].to_bits(),
        signed_log_f32(&integer(16)).to_bits()
    );
    assert!(std::sync::Arc::ptr_eq(&values[5].1[4], &values[6].1[4]));
}
