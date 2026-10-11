//! 596/30 槽与六侧全量盘口；只消费公共所选事实，派生缓存和有限历史随市场退役。
use crate::exact::{InputProjection, decimal, integer, ratio, round_f32, signed_log_f32 as log};
use contracts::{economics::EconomicRational as R, *};
use rayon::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

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
fn seconds(projection: &InputProjection, a: i64, b: i64) -> f32 {
    projection.project(i128::from(a) - i128::from(b))
}
fn price(projection: &InputProjection, p: &R, pm: bool) -> f32 {
    if pm {
        round_f32(&p.mul(&integer(2)).sub(&integer(1)))
    } else {
        projection.log(p)
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
pub(crate) fn t5(
    projection: &InputProjection,
    now: i64,
    a: Option<&DirectObservationAnchorV1>,
) -> [f32; 5] {
    let mut out = [0.; 5];
    if let Some(a) = a {
        out[1] = seconds(projection, now, a.received_at_utc_ms);
        out[4] = 1.;
        if let Some(s) = a.source_event_at_utc_ms {
            out[0] = seconds(projection, now, s);
            out[2] = seconds(projection, a.received_at_utc_ms, s);
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
    fn features(&self, projection: &InputProjection) -> [f32; 9] {
        let mut out = [0.; 9];
        for i in 0..3 {
            if let Some((lo, hi)) = self.bounds[i] {
                out[2 * i] = projection.project(lo);
                out[2 * i + 1] = if hi == lo {
                    out[2 * i]
                } else {
                    projection.project(hi)
                };
            }
            out[6 + i] = fraction(self.known[i], self.count);
        }
        out
    }
}
fn e9(
    projection: &InputProjection,
    now: i64,
    times: impl IntoIterator<Item = (Option<i64>, Option<i64>)>,
) -> [f32; 9] {
    let mut stats = TimeStats::default();
    for time in times {
        stats.push(now, time);
    }
    stats.features(projection)
}
pub(crate) fn envelope(
    projection: &InputProjection,
    now: i64,
    a: &[&DirectObservationAnchorV1],
) -> [f32; 9] {
    e9(
        projection,
        now,
        a.iter()
            .map(|a| (a.source_event_at_utc_ms, Some(a.received_at_utc_ms))),
    )
}
fn v7(
    projection: &InputProjection,
    now: i64,
    value: Option<R>,
    a: Option<&DirectObservationAnchorV1>,
    pm: bool,
) -> [f32; 7] {
    let mut o = [0.; 7];
    if let Some(v) = value {
        o[0] = price(projection, &v, pm);
        o[1] = 1.;
    }
    o[2..].copy_from_slice(&t5(projection, now, a));
    o
}

pub(crate) fn rules_with_static(
    projection: &InputProjection,
    o: &MarketBusinessObservationV1,
    now: i64,
    tail: &[f32; 16],
) -> [f32; 30] {
    let mut out = [0.; 30];
    for i in 0..2 {
        if let Some(a) = o
            .assets
            .iter()
            .find(|a| a.asset.outcome.index as usize == i)
        {
            out[i * 7..i * 7 + 7].copy_from_slice(&v7(
                projection,
                now,
                visible(&a.tick).map(decimal),
                one(&anchors(&a.tick, "tick")),
                false,
            ));
        }
    }
    out[14..].copy_from_slice(tail);
    out
}
pub(crate) fn static_rules(r: &PublicTradingRuleContextV1) -> [f32; 16] {
    let mut out = [0.; 30];
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
    out[14..].try_into().unwrap()
}
#[derive(Clone, Default)]
pub(crate) struct SidePrevious {
    total: Option<R>,
    best: Option<R>,
    // 只保留当前冻结侧的可重建解析／投影；前帧业务统计独立保留。
    inputs: Option<SideInputs>,
}
#[derive(Clone)]
struct ParsedLevel {
    price_text: String,
    quantity_text: String,
    price: R,
    quantity: R,
    path: String,
    row: Option<usize>,
    time: Option<(i64, Option<i64>, i64)>,
}
impl ParsedLevel {
    fn new(p: &ExactDecimal, q: &ExactDecimal, prefix: &str) -> Self {
        Self {
            price_text: p.0.clone(),
            quantity_text: q.0.clone(),
            price: decimal(p),
            quantity: decimal(q),
            path: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}{}]", p.0)
            },
            row: None,
            time: None,
        }
    }
    fn quantity(&mut self, q: &ExactDecimal) -> bool {
        if self.quantity_text == q.0 {
            return false;
        }
        let value = decimal(q);
        let changed = value != self.quantity;
        self.quantity = value;
        self.quantity_text.clone_from(&q.0);
        changed
    }
}
#[derive(Clone, Default)]
struct SideInputs {
    prefix: String,
    levels: Vec<ParsedLevel>,
    // 已发布的侧从不原地修改；旧组继续持有它实际消费的版本。
    book: Arc<Vec<[f32; 9]>>,
    summary: [f32; 2],
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
    // 仅派生于对应 series；接纳来源后失效，完整编码时重算，随市场历史释放。
    history_features: [Option<[f32; 4]>; 6],
}

pub(crate) fn side(
    projection: &InputProjection,
    out: &mut [f32],
    now: i64,
    levels: Option<Vec<(&ExactDecimal, &ExactDecimal)>>,
    a: Vec<&DirectObservationAnchorV1>,
    prefix: &str,
    pm: bool,
    previous: &mut SidePrevious,
    book: &mut Arc<Vec<[f32; 9]>>,
    bid: bool,
) -> Option<(R, Vec<DirectObservationAnchorV1>)> {
    out[5..14].copy_from_slice(&envelope(projection, now, &a));
    let Some(levels) = levels else {
        *previous = SidePrevious::default();
        *book = Arc::default();
        return None;
    };
    let cached = previous
        .inputs
        .take()
        .filter(|value| value.prefix == prefix);
    let warm = cached.is_some();
    let mut cached = cached.unwrap_or_default();
    cached.prefix.clear();
    cached.prefix.push_str(prefix);
    let same_prices = levels.len() == cached.levels.len()
        && levels
            .iter()
            .zip(&cached.levels)
            .all(|((p, _), old)| p.0 == old.price_text);
    let mut quantity_changed = Vec::with_capacity(levels.len());
    if same_prices {
        for ((_, q), old) in levels.iter().zip(&mut cached.levels) {
            quantity_changed.push(old.quantity(q));
        }
    } else {
        // 结构改变才建立迁移索引。原十进制文本只用于复用解析结果，不作为经济价格。
        let mut old: HashMap<_, _> = std::mem::take(&mut cached.levels)
            .into_iter()
            .map(|entry| (entry.price_text.clone(), entry))
            .collect();
        for (p, q) in &levels {
            let mut entry = old
                .remove(&p.0)
                .unwrap_or_else(|| ParsedLevel::new(p, q, prefix));
            quantity_changed.push(entry.quantity(q));
            cached.levels.push(entry);
        }
    }
    let valid: Vec<_> = cached
        .levels
        .iter()
        .enumerate()
        .filter_map(|(i, level)| (level.quantity > R::zero()).then_some(i))
        .collect();
    let total = if warm && same_prices && !quantity_changed.iter().any(|v| *v) {
        previous.total.clone().unwrap()
    } else {
        cached.summary[0] = projection.log(&integer(valid.len()));
        let total = sum(valid.iter().map(|i| &cached.levels[*i].quantity));
        cached.summary[1] = projection.log(&total);
        total
    };
    out[0] = 1.;
    out[1..3].copy_from_slice(&cached.summary);
    if let Some(old) = &previous.total {
        out[3] = projection.log(&total.sub(old));
        out[4] = 1.;
    }
    let best = valid.first().map(|i| cached.levels[*i].price.clone());
    let best_changed = best != previous.best;
    if let Some((best, old)) = best.as_ref().zip(previous.best.as_ref()) {
        out[14] = projection.log(&best.sub(old));
        out[15] = 1.;
    }
    let mut by_path: HashMap<&str, Vec<&DirectObservationAnchorV1>> = HashMap::new();
    for anchor in &a {
        by_path.entry(&anchor.field_path).or_default().push(anchor);
    }
    let mut times = HashMap::new();
    let mut updates = Vec::with_capacity(valid.len());
    let mut changed = valid.len() != cached.book.len();
    let mut best_identity = None;
    for (row, &index) in valid.iter().enumerate() {
        let level = &cached.levels[index];
        let aa = by_path
            .get(level.path.as_str())
            .map_or(&[][..], Vec::as_slice);
        let anchor = one(aa);
        let time_key = anchor.map(|a| (now, a.source_event_at_utc_ms, a.received_at_utc_ms));
        let time = anchor.map_or([0.; 5], |a| {
            *times
                .entry((a.source_event_at_utc_ms, a.received_at_utc_ms))
                .or_insert_with(|| {
                    if level.time == time_key
                        && let Some(old) = level.row
                    {
                        cached.book[old][3..8].try_into().unwrap()
                    } else {
                        t5(projection, now, Some(a))
                    }
                })
        });
        changed |= level.row != Some(row)
            || quantity_changed[index]
            || best_changed
            || level.time != time_key;
        updates.push((index, time, time_key));
        if row == 0 && !aa.is_empty() {
            best_identity = Some((level.price.clone(), aa.iter().copied().cloned().collect()));
        }
    }
    if changed {
        let mut values = vec![[0.; 9]; valid.len()];
        values
            .par_chunks_mut(64)
            .enumerate()
            .for_each(|(chunk, rows)| {
                for (within, output) in rows.iter_mut().enumerate() {
                    let (index, time, _) = &updates[chunk * 64 + within];
                    let level = &cached.levels[*index];
                    if let Some(old) = level.row {
                        *output = cached.book[old];
                    } else {
                        output[0] = price(projection, &level.price, pm);
                        output[2] = 1.;
                    }
                    if level.row.is_none() || quantity_changed[*index] {
                        output[1] = projection.log(&level.quantity);
                    }
                    output[3..8].copy_from_slice(time);
                    if level.row.is_none() || best_changed {
                        let distance = if bid {
                            best.as_ref().unwrap().sub(&level.price)
                        } else {
                            level.price.sub(best.as_ref().unwrap())
                        };
                        output[8] = projection.log(&distance);
                    }
                }
            });
        cached.book = Arc::new(values);
    }
    for level in &mut cached.levels {
        level.row = None;
        level.time = None;
    }
    for (row, (index, _, time)) in updates.into_iter().enumerate() {
        cached.levels[index].row = Some(row);
        cached.levels[index].time = time;
    }
    *book = cached.book.clone();
    previous.total = Some(total);
    previous.best = best;
    previous.inputs = Some(cached);
    best_identity
}

fn reference(
    projection: &InputProjection,
    out: &mut [f32],
    o: &MarketBusinessObservationV1,
    now: i64,
) {
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
            out[8..13].copy_from_slice(&t5(projection, now, Some(&t.direct_anchor)));
            out[13] = projection.log(&integer(t.value.window_s));
            Some(decimal(&t.value.price_usd_per_btc))
        }
        EffectiveMarketReferenceV1::OfficialAnchor(f) => {
            out[5] = 1.;
            out[8..13].copy_from_slice(&t5(projection, now, one(&anchors(f, "official_anchor"))));
            visible(f).map(|v| decimal(&v.price_usd_per_btc))
        }
    };
    if let Some(p) = &effective {
        out[7] = projection.log(p);
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
        out[14 + 7 * i..21 + 7 * i].copy_from_slice(&v7(
            projection,
            now,
            vals[i].clone(),
            one(&aa[i]),
            false,
        ));
    }
    if let Some((p, e)) = selected
        .and_then(|i| vals[i].as_ref())
        .zip(effective.as_ref())
    {
        out[35] = projection.log(&p.sub(e));
        out[36] = 1.;
    }
}
impl MarketHistory {
    /// 只接纳六路真实来源样本；身份去重、原组时间与16条截断保持。
    pub(crate) fn observe_sources(&mut self, o: &MarketBusinessObservationV1, now: i64) {
        let mut samples: [Option<(R, Vec<DirectObservationAnchorV1>)>; 6] =
            std::array::from_fn(|_| None);
        for (i, sample) in samples.iter_mut().enumerate().take(2) {
            if let Some(asset) = o
                .assets
                .iter()
                .find(|a| a.asset.outcome.index as usize == i)
            {
                *sample = bba_sample(&asset.best_bid_ask);
            }
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
        for (i, sample) in samples.into_iter().enumerate() {
            let q = &mut self.series[i];
            if let Some((value, identity)) = sample
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
                self.history_features[i] = None;
            }
        }
    }
}
fn history(
    projection: &InputProjection,
    out: &mut [f32],
    q: &VecDeque<Sample>,
    cached: &mut Option<[f32; 4]>,
) {
    if let Some(value) = cached {
        out.copy_from_slice(value);
        return;
    }
    out[0] = fraction(q.len(), 16);
    if q.len() >= 2 {
        let last = &q[q.len() - 1];
        out[1] = projection.log(&last.value.sub(&q[q.len() - 2].value));
        let squares = q
            .iter()
            .zip(q.iter().skip(1))
            .map(|(a, b)| {
                let d = b.value.sub(&a.value);
                d.mul(&d)
            })
            .fold(R::zero(), |a, b| a.add(&b));
        out[2] = crate::exact::sqrt_log_f32(&squares.divided_by(&integer(q.len() - 1)));
        out[3] = seconds(projection, last.at, q[0].at);
    }
    *cached = Some(out.try_into().expect("固定四项历史特征"));
}
fn lifecycle(
    projection: &InputProjection,
    out: &mut [f32],
    o: &MarketBusinessObservationV1,
    now: i64,
) {
    out[0] = seconds(projection, now, o.market.window.start_at_utc_ms);
    out[1] = seconds(projection, o.market.window.end_at_utc_ms, now);
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
    out[14..23].copy_from_slice(&envelope(
        projection,
        now,
        &anchors(&o.lifecycle, "lifecycle"),
    ));
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
        out[i + 2..i + 11].copy_from_slice(&envelope(projection, now, &anchors(f, name)));
    }
    out[63..69].copy_from_slice(&quality(o.terminal.quality));
    let a = anchors(&o.terminal, "terminal");
    let terminal_envelope = envelope(projection, now, &a);
    out[72..81].copy_from_slice(&terminal_envelope);
    out[83..92].copy_from_slice(&terminal_envelope);
    out[94..103].copy_from_slice(&terminal_envelope);
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
                    out[81 + 11 * i] = price(projection, &decimal(&p.payout_pusd_per_share), true);
                    out[82 + 11 * i] = 1.;
                }
            }
        }
    }
}
fn bba(
    projection: &InputProjection,
    out: &mut [f32],
    f: &ObservedFactV1<CanonicalBbaFactV1>,
    now: i64,
) {
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
    let time = t5(projection, now, one(&a));
    for (i, field) in fields.iter().enumerate() {
        let start = 7 + 7 * i;
        if let Some(value) = field.map(decimal) {
            out[start] = price(projection, &value, i < 2);
            out[start + 1] = 1.;
        }
        out[start + 2..start + 7].copy_from_slice(&time);
    }
}
fn bba_sample(
    f: &ObservedFactV1<CanonicalBbaFactV1>,
) -> Option<(R, Vec<DirectObservationAnchorV1>)> {
    let value = visible(f)?;
    let a = anchors(f, "best_bid_ask");
    value
        .best_bid_pusd_per_share
        .as_ref()
        .zip(value.best_ask_pusd_per_share.as_ref())
        .filter(|_| !a.is_empty())
        .map(|(b, s)| {
            (
                decimal(b).add(&decimal(s)).divided_by(&integer(2)),
                a.into_iter().cloned().collect(),
            )
        })
}

fn pm_trade(
    projection: &InputProjection,
    out: &mut [f32],
    f: &ObservedFactV1<PublicTradeV2>,
    now: i64,
) {
    out[..6].copy_from_slice(&quality(f.quality));
    if let Some(t) = visible(f) {
        out[6] = 1.;
        out[7] = price(projection, &decimal(&t.price_pusd_per_share), true);
        out[8] = projection.log(&decimal(&t.quantity_shares));
        out[if t.aggressor_side == TradeSide::Buy {
            9
        } else {
            10
        }] = 1.;
        if let Some(fee) = &t.fee_rate_bps {
            out[11] = projection.log(&decimal(fee));
            out[12] = 1.;
        }
    }
    out[13..].copy_from_slice(&t5(projection, now, one(&anchors(f, "public_trade"))));
}
fn binance_trade(
    projection: &InputProjection,
    out: &mut [f32],
    f: &ObservedFactV1<BinanceAggregateTradeReferenceV1>,
    now: i64,
) {
    out[..6].copy_from_slice(&quality(f.quality));
    if let Some(t) = visible(f) {
        out[6] = 1.;
        out[7] = projection.log(&decimal(&t.price_usdt_per_btc));
        out[8] = projection.log(&decimal(&t.quantity_btc));
        out[9] = if t.buyer_is_maker { 1. } else { 0. };
    }
    out[10..].copy_from_slice(&t5(projection, now, one(&anchors(f, "binance_trade"))));
}
pub(crate) struct Trade {
    pub(crate) p: R,
    pub(crate) q: Option<R>,
    pub(crate) side: Option<CanonicalPmSideV1>,
    pub(crate) fee: Option<R>,
    pub(crate) time: (Option<i64>, Option<i64>),
}
pub(crate) fn trades(
    projection: &InputProjection,
    out: &mut [f32],
    rows: impl Iterator<Item = Trade>,
    now: i64,
    pm: bool,
) {
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
    out[0] = projection.log(&integer(n));
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
        out[1 + i] = price(projection, p, pm);
    }
    out[6] = projection.log(&qty);
    if pm {
        out[7] = fraction(known_qty, n);
        out[8] = projection.log(&amount);
        for (i, c) in counts.into_iter().enumerate() {
            out[9 + i] = projection.log(&integer(c));
        }
        for i in 0..2 {
            out[13 + i] = projection.log(&side_qty[i]);
            out[15 + i] = fraction(side_known[i], counts[i]);
        }
        if known_fees > 0 {
            out[17] = projection.log(&fees.divided_by(&integer(known_fees)));
        }
        out[18] = fraction(known_fees, n);
        out[19..].copy_from_slice(&times.features(projection));
    } else {
        out[7] = projection.log(&amount);
        for i in 0..2 {
            out[8 + i] = projection.log(&integer(counts[i]));
            out[10 + i] = projection.log(&side_qty[i]);
        }
        out[12..].copy_from_slice(&times.features(projection));
    }
}
fn event_class(p: &PublicBusinessPayloadV1) -> (usize, usize) {
    match p {
        PublicBusinessPayloadV1::BinanceDepthReference(_) => (17, 6),
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
fn events(
    projection: &InputProjection,
    out: &mut [f32],
    o: &MarketBusinessObservationV1,
    now: i64,
) {
    let mut counts = [0usize; 23];
    let mut times: [TimeStats; 9] = std::array::from_fn(|_| TimeStats::default());
    for event in o.group_events.iter() {
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
        trades(projection, &mut out[start..end], rows, now, i < 2);
    }
    for (i, c) in counts.into_iter().enumerate() {
        out[77 + i] = projection.log(&integer(c));
    }
    for (i, t) in times.into_iter().enumerate() {
        out[100 + 9 * i..109 + 9 * i].copy_from_slice(&t.features(projection));
    }
}

pub(crate) fn encode(
    projection: &InputProjection,
    o: &MarketBusinessObservationV1,
    rule: &[f32; 30],
    now: i64,
    h: &mut MarketHistory,
) -> ([f32; 596], BookLevels) {
    h.observe_sources(o, now);
    encode_features(projection, o, rule, now, h)
}
/// 编码已接纳来源的模型特征，不再接纳样本。
pub(crate) fn encode_features(
    projection: &InputProjection,
    o: &MarketBusinessObservationV1,
    rule: &[f32; 30],
    now: i64,
    h: &mut MarketHistory,
) -> ([f32; 596], BookLevels) {
    let mut out = [0.; 596];
    let mut book: BookLevels = std::array::from_fn(|_| Arc::default());
    reference(projection, &mut out[..37], o, now);
    // 六侧输入互不依赖；同一 Rayon 池按侧批量准备，结果仍按规范顺序写回。
    let mut side_features = [[0.; 16]; 6];
    h.sides
        .par_iter_mut()
        .zip(book.par_iter_mut())
        .zip(side_features.par_iter_mut())
        .enumerate()
        .for_each(|(index, ((previous, book), features))| {
            let bid = index % 2 == 0;
            if index < 4 {
                if let Some(asset) = o
                    .assets
                    .iter()
                    .find(|a| a.asset.outcome.index as usize == index / 2)
                {
                    let fact = if bid { &asset.bids } else { &asset.asks };
                    let prefix = format!(
                        "assets[{}].{}[",
                        asset.asset.token_id.0,
                        if bid { "bids" } else { "asks" }
                    );
                    let levels = visible(fact);
                    side(
                        projection,
                        features,
                        now,
                        levels.map(|v| {
                            v.iter()
                                .map(|p| (&p.price_pusd_per_share, &p.quantity_shares))
                                .collect()
                        }),
                        fact.direct_anchors
                            .iter()
                            .filter(|a| a.field_path.starts_with(&prefix))
                            .collect(),
                        &prefix,
                        true,
                        previous,
                        book,
                        bid,
                    );
                } else {
                    previous.inputs = None;
                }
            } else {
                let levels = visible(&o.binance_depth)
                    .and_then(BinanceDepthObservationV1::full)
                    .map(|d| if bid { &d.bids } else { &d.asks });
                side(
                    projection,
                    features,
                    now,
                    levels.map(|v| {
                        v.iter()
                            .map(|p| (&p.price_usdt_per_btc, &p.quantity_btc))
                            .collect()
                    }),
                    vec![],
                    "",
                    false,
                    previous,
                    book,
                    bid,
                );
            }
        });
    for i in 0..2 {
        if let Some(a) = o
            .assets
            .iter()
            .find(|a| a.asset.outcome.index as usize == i)
        {
            let base = 37 + 38 * i;
            out[base..base + 6].copy_from_slice(&quality(a.bids.quality));
            for j in 0..2 {
                out[base + 6 + j * 16..base + 22 + j * 16]
                    .copy_from_slice(&side_features[2 * i + j]);
            }
            bba(
                projection,
                &mut out[151 + 28 * i..179 + 28 * i],
                &a.best_bid_ask,
                now,
            );
            pm_trade(
                projection,
                &mut out[207 + 18 * i..225 + 18 * i],
                &a.public_trade,
                now,
            );
        }
    }
    out[113..119].copy_from_slice(&quality(o.binance_depth.quality));
    for j in 0..2 {
        out[119 + j * 16..135 + j * 16].copy_from_slice(&side_features[4 + j]);
    }
    binance_trade(projection, &mut out[243..258], &o.binance_trade, now);
    events(projection, &mut out[258..439], o, now);
    lifecycle(projection, &mut out[439..542], o, now);
    for i in 0..6 {
        history(
            projection,
            &mut out[542 + 4 * i..546 + 4 * i],
            &h.series[i],
            &mut h.history_features[i],
        );
    }
    out[566..].copy_from_slice(rule);
    (out, book)
}

/// 六侧唯一原生表示；长度由数组实际人口派生，不持久化补齐或长度副本。
pub type BookLevels = [Arc<Vec<[f32; 9]>>; 6];

/// 独立根合批边界；全空使用物理 N=1，全部 mask=false。
pub fn pad_books<'a>(
    books: impl IntoIterator<Item = &'a BookLevels>,
) -> (usize, Vec<f32>, Vec<bool>) {
    let books: Vec<_> = books.into_iter().collect();
    let depth = books
        .iter()
        .flat_map(|b| b.iter().map(|side| side.len()))
        .max()
        .unwrap_or(0)
        .max(1);
    let mut values = vec![0.; books.len() * 6 * depth * 9];
    let mut mask = vec![false; books.len() * 6 * depth];
    for (batch, book) in books.iter().enumerate() {
        for (side, levels) in book.iter().enumerate() {
            for (index, row) in levels.iter().enumerate() {
                let slot = (batch * 6 + side) * depth + index;
                values[slot * 9..(slot + 1) * 9].copy_from_slice(row);
                mask[slot] = true;
            }
        }
    }
    (depth, values, mask)
}
