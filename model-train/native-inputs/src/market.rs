//! #6 的 836/30 槽；只消费公共所选事实。有限历史不保存第二份盘口。
use crate::exact::{decimal, integer, ratio, round_f32, signed_log_f32 as log};
use contracts::{economics::EconomicRational as R, *};
use std::collections::VecDeque;

fn visible<T>(f: &ObservedFactV1<T>) -> Option<&T> {
    matches!(
        f.quality,
        MarketDataQuality::Fresh | MarketDataQuality::Stale
    )
    .then_some(f.value.as_ref())
    .flatten()
}
fn quality(q: MarketDataQuality) -> [f32; 6] {
    let mut v = [0.; 6];
    v[match q {
        MarketDataQuality::Fresh => 0,
        MarketDataQuality::Stale => 1,
        MarketDataQuality::Missing => 2,
        MarketDataQuality::UnknownNotObserved => 3,
        MarketDataQuality::ConfirmedGap => 4,
        MarketDataQuality::Invalid => 5,
    }] = 1.;
    v
}
fn seconds(a: i64, b: i64) -> f32 {
    log(&ratio(i128::from(a) - i128::from(b), 1000))
}
fn price(p: &R, pm: bool) -> f32 {
    if pm {
        round_f32(&p.mul(&integer(2)).sub(&integer(1)))
    } else {
        log(p)
    }
}
fn fraction(n: usize, d: usize) -> f32 {
    if d == 0 { 0. } else { round_f32(&ratio(n, d)) }
}
fn sum<'a>(v: impl Iterator<Item = &'a R>) -> R {
    v.fold(R::zero(), |a, b| a.add(b))
}
fn anchors<'a, T>(f: &'a ObservedFactV1<T>, path: &str) -> Vec<&'a DirectObservationAnchorV1> {
    f.direct_anchors
        .iter()
        .filter(|a| a.field_path == path)
        .collect()
}
/// T5 只用于已选单项直接锚点；不取多来源的最大时间冒充字段时间。
pub(crate) fn t5(now: i64, a: Option<&DirectObservationAnchorV1>) -> [f32; 5] {
    let mut out = [0.; 5];
    if let Some(a) = a {
        out[1] = seconds(now, a.received_at_utc_ms);
        out[4] = 1.;
        if let Some(s) = a.source_event_at_utc_ms {
            out[0] = seconds(now, s);
            out[2] = seconds(a.received_at_utc_ms, s);
            out[3] = 1.;
        }
    }
    out
}
fn one<'a>(a: &[&'a DirectObservationAnchorV1]) -> Option<&'a DirectObservationAnchorV1> {
    if a.len() == 1 { Some(a[0]) } else { None }
}
#[derive(Default)]
struct TimeStats {
    count: usize,
    known: [usize; 3],
    bounds: [Option<(i128, i128)>; 3],
}
impl TimeStats {
    fn push(&mut self, now: i64, (s, r): (Option<i64>, Option<i64>)) {
        self.count += 1;
        let ds = [
            s.map(|s| i128::from(now) - i128::from(s)),
            r.map(|r| i128::from(now) - i128::from(r)),
            s.zip(r).map(|(s, r)| i128::from(r) - i128::from(s)),
        ];
        for (i, d) in ds.into_iter().enumerate() {
            if let Some(d) = d {
                self.known[i] += 1;
                self.bounds[i] =
                    Some(self.bounds[i].map_or((d, d), |(lo, hi)| (lo.min(d), hi.max(d))));
            }
        }
    }
    fn features(&self) -> [f32; 9] {
        let mut out = [0.; 9];
        for i in 0..3 {
            if let Some((lo, hi)) = self.bounds[i] {
                out[2 * i] = log(&ratio(lo, 1000));
                out[2 * i + 1] = log(&ratio(hi, 1000));
            }
            out[6 + i] = fraction(self.known[i], self.count);
        }
        out
    }
}
fn e9(now: i64, times: impl IntoIterator<Item = (Option<i64>, Option<i64>)>) -> [f32; 9] {
    let mut stats = TimeStats::default();
    for time in times {
        stats.push(now, time);
    }
    stats.features()
}
pub(crate) fn envelope(now: i64, a: &[&DirectObservationAnchorV1]) -> [f32; 9] {
    e9(
        now,
        a.iter()
            .map(|a| (a.source_event_at_utc_ms, Some(a.received_at_utc_ms))),
    )
}
fn v7(now: i64, value: Option<R>, a: Option<&DirectObservationAnchorV1>, pm: bool) -> [f32; 7] {
    let mut o = [0.; 7];
    if let Some(v) = value {
        o[0] = price(&v, pm);
        o[1] = 1.;
    }
    o[2..].copy_from_slice(&t5(now, a));
    o
}

pub(crate) fn rules(
    o: &MarketBusinessObservationV1,
    r: &PublicTradingRuleContextV1,
    now: i64,
) -> [f32; 30] {
    let mut out = [0.; 30];
    for i in 0..2 {
        if let Some(a) = o
            .assets
            .iter()
            .find(|a| a.asset.outcome.index as usize == i)
        {
            out[i * 7..i * 7 + 7].copy_from_slice(&v7(
                now,
                visible(&a.tick).map(decimal),
                one(&anchors(&a.tick, "tick")),
                false,
            ));
        }
    }
    for (slot, value) in [
        (14, decimal(&r.limit_min_quantity_shares)),
        (16, decimal(&r.market_buy_min_spend_pusd)),
        (20, ratio(r.taker_delay_ms, 1000)),
        (24, decimal(&r.fee_rate)),
        (26, integer(r.fee_exponent)),
    ] {
        out[slot] = log(&value);
        out[slot + 1] = 1.;
    }
    for (slot, v) in [
        (18, r.taker_delay_enabled),
        (22, r.fees_enabled),
        (28, r.fee_taker_only),
    ] {
        out[slot] = if v { 1. } else { 0. };
        out[slot + 1] = 1.;
    }
    out
}
#[derive(Clone, Default)]
pub(crate) struct SidePrevious {
    total: Option<R>,
    best: Option<R>,
}
#[derive(Clone)]
pub(crate) struct Sample {
    value: R,
    identity: Vec<DirectObservationAnchorV1>,
    at: i64,
}
#[derive(Clone, Default)]
pub(crate) struct MarketHistory {
    sides: [SidePrevious; 6],
    pub(crate) series: [VecDeque<Sample>; 6],
}

pub(crate) fn side(
    out: &mut [f32],
    now: i64,
    levels: Option<Vec<(R, R)>>,
    a: Vec<&DirectObservationAnchorV1>,
    paths: Vec<String>,
    pm: bool,
    previous: &mut SidePrevious,
) -> Option<(R, Vec<DirectObservationAnchorV1>)> {
    out[5..14].copy_from_slice(&envelope(now, &a));
    let mut next = SidePrevious::default();
    let mut best_identity = None;
    if let Some(levels) = levels {
        let levels: Vec<_> = levels.into_iter().filter(|(_, q)| q > &R::zero()).collect();
        out[0] = 1.;
        out[1] = log(&integer(levels.len()));
        let total = sum(levels.iter().map(|(_, q)| q));
        out[2] = log(&total);
        if let Some(old) = &previous.total {
            out[3] = log(&total.sub(old));
            out[4] = 1.;
        }
        next.total = Some(total);
        for (i, (p, q)) in levels.iter().take(5).enumerate() {
            let offset = 14 + 8 * i;
            out[offset] = price(p, pm);
            out[offset + 1] = log(q);
            out[offset + 2] = 1.;
            let aa: Vec<_> = a
                .iter()
                .copied()
                .filter(|a| paths.get(i).is_some_and(|p| p == &a.field_path))
                .collect();
            out[offset + 3..offset + 8].copy_from_slice(&t5(now, one(&aa)));
            if i == 0 {
                next.best = Some(p.clone());
                if let Some(old) = &previous.best {
                    out[54] = log(&p.sub(old));
                    out[55] = 1.;
                }
                if !aa.is_empty() {
                    best_identity = Some((p.clone(), aa.into_iter().cloned().collect()));
                }
            }
        }
    }
    *previous = next;
    best_identity
}
fn reference(out: &mut [f32], o: &MarketBusinessObservationV1, now: i64) {
    let selected = visible(&o.settlement_reference).map(|r| match r {
        BtcSettlementReferenceV1::ChainlinkSpot => 0,
        BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => 1,
        BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => 2,
    });
    if let Some(i) = selected {
        out[i] = 1.;
    }
    out[6] = if o.temporary_reference.is_some() {
        1.
    } else {
        0.
    };
    let effective = match &o.effective_reference {
        EffectiveMarketReferenceV1::Missing => {
            out[3] = 1.;
            None
        }
        EffectiveMarketReferenceV1::TemporaryTwap(t) => {
            out[4] = 1.;
            out[8..13].copy_from_slice(&t5(now, Some(&t.direct_anchor)));
            out[13] = log(&integer(t.value.window_s));
            Some(decimal(&t.value.price_usd_per_btc))
        }
        EffectiveMarketReferenceV1::OfficialAnchor(f) => {
            out[5] = 1.;
            out[8..13].copy_from_slice(&t5(now, one(&anchors(f, "official_anchor"))));
            visible(f).map(|v| decimal(&v.price_usd_per_btc))
        }
    };
    if let Some(p) = &effective {
        out[7] = log(p);
    }
    let vals = [
        visible(&o.chainlink_spot).map(|v| decimal(&v.price_usd_per_btc)),
        visible(&o.chainlink_twap_thirty).map(|v| decimal(&v.price_usd_per_btc)),
        visible(&o.chainlink_twap_sixty).map(|v| decimal(&v.price_usd_per_btc)),
    ];
    let aa = [
        anchors(&o.chainlink_spot, "chainlink_spot"),
        anchors(&o.chainlink_twap_thirty, "chainlink_twap_thirty"),
        anchors(&o.chainlink_twap_sixty, "chainlink_twap_sixty"),
    ];
    for i in 0..3 {
        out[14 + 7 * i..21 + 7 * i].copy_from_slice(&v7(now, vals[i].clone(), one(&aa[i]), false));
    }
    if let Some((p, e)) = selected
        .and_then(|i| vals[i].as_ref())
        .zip(effective.as_ref())
    {
        out[35] = log(&p.sub(e));
        out[36] = 1.;
    }
}
fn history(
    out: &mut [f32],
    q: &mut VecDeque<Sample>,
    value: Option<(R, Vec<DirectObservationAnchorV1>)>,
    now: i64,
) {
    if let Some((value, identity)) = value
        && !identity.is_empty()
        && q.back().is_none_or(|s| s.identity != identity)
    {
        q.push_back(Sample {
            value,
            identity,
            at: now,
        });
        if q.len() > 16 {
            q.pop_front();
        }
    }
    out[0] = fraction(q.len(), 16);
    if q.len() < 2 {
        return;
    }
    let last = &q[q.len() - 1];
    out[1] = log(&last.value.sub(&q[q.len() - 2].value));
    let squares = q
        .iter()
        .zip(q.iter().skip(1))
        .map(|(a, b)| {
            let d = b.value.sub(&a.value);
            d.mul(&d)
        })
        .fold(R::zero(), |a, b| a.add(&b));
    out[2] = crate::exact::sqrt_log_f32(&squares.divided_by(&integer(q.len() - 1)));
    out[3] = seconds(last.at, q[0].at);
}
fn lifecycle(out: &mut [f32], o: &MarketBusinessObservationV1, now: i64) {
    out[0] = seconds(now, o.market.window.start_at_utc_ms);
    out[1] = seconds(o.market.window.end_at_utc_ms, now);
    out[2..8].copy_from_slice(&quality(o.lifecycle.quality));
    if let Some(s) = visible(&o.lifecycle) {
        out[8] = 1.;
        let i = match s {
            MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Candidate,
            } => 0,
            MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Prefetched,
            } => 1,
            MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            } => 2,
            MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Retired,
            } => 3,
            MappedMarketLifecycleStateV1::UnknownUnmodeled { .. } => 4,
        };
        out[9 + i] = 1.;
    }
    out[14..23].copy_from_slice(&envelope(now, &anchors(&o.lifecycle, "lifecycle")));
    // 原 producer 只在至少一字段存在时输出 TradingState，三字段同次替换、同质量。
    let fields = [
        (&o.active, "active", 30),
        (&o.closed, "closed", 41),
        (&o.accepting_orders, "accepting_orders", 52),
    ];
    let whole = fields.iter().find(|(f, _, _)| f.value.is_some());
    out[23..29].copy_from_slice(&quality(
        whole.map_or(MarketDataQuality::UnknownNotObserved, |(f, _, _)| f.quality),
    ));
    out[29] = if whole.is_some() { 1. } else { 0. };
    for (f, name, i) in fields {
        if let Some(v) = visible(f) {
            out[i] = if *v { 1. } else { 0. };
            out[i + 1] = 1.;
        }
        out[i + 2..i + 11].copy_from_slice(&envelope(now, &anchors(f, name)));
    }
    out[63..69].copy_from_slice(&quality(o.terminal.quality));
    let a = anchors(&o.terminal, "terminal");
    out[72..81].copy_from_slice(&envelope(now, &a));
    out[83..92].copy_from_slice(&envelope(now, &a));
    out[94..103].copy_from_slice(&envelope(now, &a));
    if let Some(MarketResolutionTerminalV3::Resolved {
        winning_asset_id,
        payout_vector,
        ..
    }) = visible(&o.terminal)
    {
        out[69] = 1.;
        for asset in &o.assets {
            let i = asset.asset.outcome.index as usize;
            if i < 2 {
                if &asset.asset.token_id.0 == winning_asset_id {
                    out[70 + i] = 1.;
                }
                if let Some(p) = payout_vector.iter().find(|p| p.asset == asset.asset) {
                    out[81 + 11 * i] = price(&decimal(&p.payout_pusd_per_share), true);
                    out[82 + 11 * i] = 1.;
                }
            }
        }
    }
}
fn bba(
    out: &mut [f32],
    f: &ObservedFactV1<CanonicalBbaFactV1>,
    now: i64,
) -> Option<(R, Vec<DirectObservationAnchorV1>)> {
    out[..6].copy_from_slice(&quality(f.quality));
    let a = anchors(f, "best_bid_ask");
    let value = visible(f);
    out[6] = if value.is_some() { 1. } else { 0. };
    let fields = value
        .map(|v| {
            [
                v.best_bid_pusd_per_share.as_ref(),
                v.best_ask_pusd_per_share.as_ref(),
                v.spread_pusd_per_share.as_ref(),
            ]
        })
        .unwrap_or([None; 3]);
    for i in 0..3 {
        out[7 + 7 * i..14 + 7 * i].copy_from_slice(&v7(
            now,
            fields[i].map(decimal),
            one(&a),
            i < 2,
        ));
    }
    fields[0]
        .zip(fields[1])
        .filter(|_| !a.is_empty())
        .map(|(b, s)| {
            (
                decimal(b).add(&decimal(s)).divided_by(&integer(2)),
                a.into_iter().cloned().collect(),
            )
        })
}
fn pm_trade(out: &mut [f32], f: &ObservedFactV1<PublicTradeV2>, now: i64) {
    out[..6].copy_from_slice(&quality(f.quality));
    if let Some(t) = visible(f) {
        out[6] = 1.;
        out[7] = price(&decimal(&t.price_pusd_per_share), true);
        out[8] = log(&decimal(&t.quantity_shares));
        out[if t.aggressor_side == TradeSide::Buy {
            9
        } else {
            10
        }] = 1.;
        if let Some(fee) = &t.fee_rate_bps {
            out[11] = log(&decimal(fee));
            out[12] = 1.;
        }
    }
    out[13..].copy_from_slice(&t5(now, one(&anchors(f, "public_trade"))));
}
fn binance_trade(out: &mut [f32], f: &ObservedFactV1<BinanceAggregateTradeReferenceV1>, now: i64) {
    out[..6].copy_from_slice(&quality(f.quality));
    if let Some(t) = visible(f) {
        out[6] = 1.;
        out[7] = log(&decimal(&t.price_usdt_per_btc));
        out[8] = log(&decimal(&t.quantity_btc));
        out[9] = if t.buyer_is_maker { 1. } else { 0. };
    }
    out[10..].copy_from_slice(&t5(now, one(&anchors(f, "binance_trade"))));
}
pub(crate) struct Trade {
    pub(crate) p: R,
    pub(crate) q: Option<R>,
    pub(crate) side: Option<CanonicalPmSideV1>,
    pub(crate) fee: Option<R>,
    pub(crate) time: (Option<i64>, Option<i64>),
}
pub(crate) fn trades(out: &mut [f32], rows: impl Iterator<Item = Trade>, now: i64, pm: bool) {
    let mut n = 0usize;
    let mut first = None;
    let mut last = R::zero();
    let mut min = None::<R>;
    let mut max = None::<R>;
    let mut price_sum = R::zero();
    let mut qty = R::zero();
    let mut known_qty = 0usize;
    let mut amount = R::zero();
    let mut counts = [0usize; 4];
    let mut side_qty = [R::zero(), R::zero()];
    let mut side_known = [0usize; 2];
    let mut fees = R::zero();
    let mut known_fees = 0usize;
    let mut times = TimeStats::default();
    for t in rows {
        n += 1;
        if first.is_none() {
            first = Some(t.p.clone());
        }
        last = t.p.clone();
        min = Some(min.map_or_else(|| t.p.clone(), |v| v.min(t.p.clone())));
        max = Some(max.map_or_else(|| t.p.clone(), |v| v.max(t.p.clone())));
        price_sum = price_sum.add(&t.p);
        let side = match t.side {
            Some(CanonicalPmSideV1::Buy) => 0,
            Some(CanonicalPmSideV1::Sell) => 1,
            Some(CanonicalPmSideV1::Unknown) => 2,
            None => 3,
        };
        counts[side] += 1;
        if let Some(q) = t.q {
            qty = qty.add(&q);
            known_qty += 1;
            amount = amount.add(&t.p.mul(&q));
            if side < 2 {
                side_qty[side] = side_qty[side].add(&q);
                side_known[side] += 1;
            }
        }
        if let Some(f) = t.fee {
            fees = fees.add(&f);
            known_fees += 1;
        }
        times.push(now, t.time);
    }
    if n == 0 {
        return;
    }
    out[0] = log(&integer(n));
    for (i, p) in [
        first.unwrap(),
        last,
        min.unwrap(),
        max.unwrap(),
        price_sum.divided_by(&integer(n)),
    ]
    .iter()
    .enumerate()
    {
        out[1 + i] = price(p, pm);
    }
    out[6] = log(&qty);
    if pm {
        out[7] = fraction(known_qty, n);
        out[8] = log(&amount);
        for (i, c) in counts.into_iter().enumerate() {
            out[9 + i] = log(&integer(c));
        }
        for i in 0..2 {
            out[13 + i] = log(&side_qty[i]);
            out[15 + i] = fraction(side_known[i], counts[i]);
        }
        if known_fees > 0 {
            out[17] = log(&fees.divided_by(&integer(known_fees)));
        }
        out[18] = fraction(known_fees, n);
        out[19..].copy_from_slice(&times.features());
    } else {
        out[7] = log(&amount);
        for i in 0..2 {
            out[8 + i] = log(&integer(counts[i]));
            out[10 + i] = log(&side_qty[i]);
        }
        out[12..].copy_from_slice(&times.features());
    }
}
fn event_class(p: &PublicBusinessPayloadV1) -> (usize, usize) {
    match p {
        PublicBusinessPayloadV1::PolymarketMessage(m) => (
            match &m.payload {
                CanonicalPmPayloadV1::Book {
                    fact: CanonicalBookFactV1::FullBook { .. },
                } => 0,
                CanonicalPmPayloadV1::Book { .. } => 1,
                CanonicalPmPayloadV1::BestBidAsk { .. } => 2,
                CanonicalPmPayloadV1::TickSizeChange { .. } => 3,
                CanonicalPmPayloadV1::PublicTrade { .. } => 4,
                CanonicalPmPayloadV1::NewMarket { .. } => 5,
            },
            0,
        ),
        PublicBusinessPayloadV1::MarketFact { payload, .. } => (
            match payload {
                CanonicalMarketFactPayloadV1::FixedIdentity { .. } => 6,
                CanonicalMarketFactPayloadV1::InitialTick { .. } => 7,
                CanonicalMarketFactPayloadV1::LifecycleTransition(_)
                | CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot { .. } => 8,
                CanonicalMarketFactPayloadV1::TradingState(_) => 9,
                CanonicalMarketFactPayloadV1::Terminal(_) => 10,
                CanonicalMarketFactPayloadV1::QualityBoundary(_) => 11,
            },
            1,
        ),
        PublicBusinessPayloadV1::ReferenceFact(p) => match p {
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(_) => (12, 2),
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(t) => {
                if t.window_s == 30 {
                    (13, 3)
                } else {
                    (14, 4)
                }
            }
            CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(_) => (15, 5),
            CanonicalReferenceEventPayloadV2::MarketSettlementReferenceUpdate(_) => (16, 5),
            CanonicalReferenceEventPayloadV2::BinanceDepthState(_) => (17, 6),
            CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(_) => (18, 7),
            CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(a) => (
                match a.classification {
                    RtdsObservedAnomalyClassificationV1::InvalidType => 19,
                    RtdsObservedAnomalyClassificationV1::InvalidTopic => 20,
                    RtdsObservedAnomalyClassificationV1::InvalidSymbol => 21,
                    RtdsObservedAnomalyClassificationV1::InvalidTimestamp => 22,
                },
                8,
            ),
        },
    }
}
fn events(out: &mut [f32], o: &MarketBusinessObservationV1, now: i64) {
    let mut counts = [0usize; 23];
    let mut times: [TimeStats; 9] = std::array::from_fn(|_| TimeStats::default());
    for event in &o.group_events {
        let (c, t) = event_class(&event.payload);
        counts[c] += 1;
        for source in &event.direct_sources {
            times[t].push(
                now,
                (
                    source.source_event_at_utc_ms,
                    Some(source.received_at_utc_ms),
                ),
            );
        }
    }
    for i in 0..3 {
        let rows = o.group_events.iter().filter_map(|event| {
            let time = (
                event.direct_time.source_event_at_utc_ms,
                Some(event.direct_time.received_at_utc_ms),
            );
            match &event.payload {
                PublicBusinessPayloadV1::PolymarketMessage(m) => {
                    if let CanonicalPmPayloadV1::PublicTrade {
                        asset,
                        price_pusd_per_share,
                        quantity_shares,
                        side,
                        fee_rate_bps,
                    } = &m.payload
                    {
                        (i < 2
                            && o.assets
                                .iter()
                                .any(|a| a.asset == *asset && a.asset.outcome.index as usize == i))
                        .then(|| Trade {
                            p: decimal(price_pusd_per_share),
                            q: quantity_shares.as_ref().map(decimal),
                            side: *side,
                            fee: fee_rate_bps.as_ref().map(decimal),
                            time,
                        })
                    } else {
                        None
                    }
                }
                PublicBusinessPayloadV1::ReferenceFact(
                    CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(t),
                ) if i == 2 => Some(Trade {
                    p: decimal(&t.price_usdt_per_btc),
                    q: Some(decimal(&t.quantity_btc)),
                    side: Some(if t.buyer_is_maker {
                        CanonicalPmSideV1::Sell
                    } else {
                        CanonicalPmSideV1::Buy
                    }),
                    fee: None,
                    time,
                }),
                _ => None,
            }
        });
        let (start, end) = if i < 2 {
            (28 * i, 28 * (i + 1))
        } else {
            (56, 77)
        };
        trades(&mut out[start..end], rows, now, i < 2);
    }
    for (i, c) in counts.into_iter().enumerate() {
        out[77 + i] = log(&integer(c));
    }
    for (i, t) in times.into_iter().enumerate() {
        out[100 + 9 * i..109 + 9 * i].copy_from_slice(&t.features());
    }
}

pub(crate) fn encode(
    o: &MarketBusinessObservationV1,
    rule: &[f32; 30],
    now: i64,
    h: &mut MarketHistory,
) -> [f32; 836] {
    let mut out = [0.; 836];
    reference(&mut out[..37], o, now);
    let mut samples: [Option<(R, Vec<DirectObservationAnchorV1>)>; 6] =
        std::array::from_fn(|_| None);
    for i in 0..2 {
        if let Some(a) = o
            .assets
            .iter()
            .find(|a| a.asset.outcome.index as usize == i)
        {
            let base = 37 + 118 * i;
            out[base..base + 6].copy_from_slice(&quality(a.bids.quality));
            for (j, f) in [&a.bids, &a.asks].into_iter().enumerate() {
                let prefix = format!(
                    "assets[{}].{}[",
                    a.asset.token_id.0,
                    if j == 0 { "bids" } else { "asks" }
                );
                let aa = f
                    .direct_anchors
                    .iter()
                    .filter(|a| a.field_path.starts_with(&prefix))
                    .collect();
                let levels = visible(f);
                let paths = levels
                    .map(|v| {
                        v.iter()
                            .filter(|p| decimal(&p.quantity_shares) > R::zero())
                            .map(|p| format!("{}{}]", prefix, p.price_pusd_per_share.0))
                            .collect()
                    })
                    .unwrap_or_default();
                side(
                    &mut out[base + 6 + j * 56..base + 62 + j * 56],
                    now,
                    levels.map(|v| {
                        v.iter()
                            .map(|p| {
                                (
                                    decimal(&p.price_pusd_per_share),
                                    decimal(&p.quantity_shares),
                                )
                            })
                            .collect()
                    }),
                    aa,
                    paths,
                    true,
                    &mut h.sides[2 * i + j],
                );
            }
            samples[i] = bba(&mut out[391 + 28 * i..419 + 28 * i], &a.best_bid_ask, now);
            pm_trade(&mut out[447 + 18 * i..465 + 18 * i], &a.public_trade, now);
        }
    }
    out[273..279].copy_from_slice(&quality(o.binance_depth.quality));
    // 当前公共 Binance 输出只有整本锚点，没有侧/档位关联；合法价量仍输入。
    // 不把整本时间复制给各档，也不为中价历史制造双侧样本身份。
    for j in 0..2 {
        let levels = visible(&o.binance_depth).map(|d| if j == 0 { &d.bids } else { &d.asks });
        side(
            &mut out[279 + j * 56..335 + j * 56],
            now,
            levels.map(|v| {
                v.iter()
                    .map(|p| (decimal(&p.price_usdt_per_btc), decimal(&p.quantity_btc)))
                    .collect()
            }),
            vec![],
            vec![],
            false,
            &mut h.sides[4 + j],
        );
    }
    samples[3] = visible(&o.chainlink_spot).map(|v| {
        (
            decimal(&v.price_usd_per_btc),
            anchors(&o.chainlink_spot, "chainlink_spot")
                .into_iter()
                .cloned()
                .collect(),
        )
    });
    samples[4] = visible(&o.chainlink_twap_thirty).map(|v| {
        (
            decimal(&v.price_usd_per_btc),
            anchors(&o.chainlink_twap_thirty, "chainlink_twap_thirty")
                .into_iter()
                .cloned()
                .collect(),
        )
    });
    samples[5] = visible(&o.chainlink_twap_sixty).map(|v| {
        (
            decimal(&v.price_usd_per_btc),
            anchors(&o.chainlink_twap_sixty, "chainlink_twap_sixty")
                .into_iter()
                .cloned()
                .collect(),
        )
    });
    binance_trade(&mut out[483..498], &o.binance_trade, now);
    events(&mut out[498..679], o, now);
    lifecycle(&mut out[679..782], o, now);
    for (i, s) in samples.into_iter().enumerate() {
        history(&mut out[782 + 4 * i..786 + 4 * i], &mut h.series[i], s, now);
    }
    out[806..].copy_from_slice(rule);
    out
}
