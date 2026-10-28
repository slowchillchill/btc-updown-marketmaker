//! 公共组驱动的持久历史。节点按需共享，旧前缀永不读取当前流的可变状态。
use crate::{blb::PreparedBlbDepthV1, spot_statistics::spot_variance_statistics_v1};
use contracts::*;
use imbl::Vector;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, Weak};

/// 显式诊断构建的本线程累计值；不改变统计输入、缓存或结果。
#[cfg(feature = "observation-diagnostics")]
pub mod observation_diagnostics {
    use serde::Serialize;
    #[derive(Default, Serialize)]
    pub struct Work {
        pub reads: u64,
        pub market_hits: u64,
        pub market_builds: u64,
        pub common_hits: u64,
        pub common_computations: u64,
        pub variance_points: u64,
        pub integral_points: u64,
        pub partition_probes: u64,
        pub read_ns: u128,
        pub cache_lock_ns: u128,
        pub common_ns: u128,
        pub market_ns: u128,
    }
    thread_local! { static WORK: std::cell::RefCell<Work> = std::cell::RefCell::new(Work::default()); }
    pub(super) fn add(f: impl FnOnce(&mut Work)) {
        WORK.with(|w| f(&mut w.borrow_mut()));
    }
    pub fn take() -> Work {
        WORK.with(|w| std::mem::take(&mut *w.borrow_mut()))
    }
}

type Number = Result<Decimal, &'static str>;
fn number(v: &ExactDecimal) -> Number {
    v.0.parse().map_err(|_| "decimal_unrepresentable")
}
fn arithmetic(v: Option<Decimal>) -> Number {
    v.ok_or("decimal_arithmetic_overflow")
}
fn result(v: Number) -> StatisticValueV1<Decimal> {
    match v {
        Ok(v) => StatisticValueV1::known(v),
        Err(e) => StatisticValueV1::missing(e),
    }
}
fn subtract(a: Decimal, b: Decimal) -> Number {
    arithmetic(a.checked_sub(b))
}
fn ratio(a: Decimal, b: Decimal) -> Number {
    if b == Decimal::ZERO {
        Err("zero_denominator")
    } else {
        arithmetic(a.checked_div(b))
    }
}
fn sum<'a>(mut values: impl Iterator<Item = &'a ExactDecimal>) -> Number {
    values.try_fold(Decimal::ZERO, |s, v| arithmetic(s.checked_add(number(v)?)))
}
fn eligible<T>(fact: &ObservedFactV1<T>) -> Option<&T> {
    matches!(
        fact.quality,
        MarketDataQuality::Fresh | MarketDataQuality::Stale
    )
    .then_some(fact.value.as_ref())
    .flatten()
}
fn evidence<T>(fact: &ObservedFactV1<T>) -> Vec<DirectObservationAnchorV1> {
    fact.direct_anchors.clone()
}

impl PreparedBlbDepthV1 {
    /// 与 BLB 共用一次 Decimal 解析；不再次解码全部深度。
    pub fn public_depth_statistics(&self) -> PublicDepthStatisticsV1 {
        let parsed = self.parsed.as_ref().map_err(|e| *e);
        // 各侧独立保留已知价格；缺少对侧或数量和溢出不抹掉本侧最佳价。
        let bid = parsed.and_then(|(bids, _)| {
            bids.iter()
                .filter(|(p, q)| *p > Decimal::ZERO && *q > Decimal::ZERO)
                .map(|(p, _)| *p)
                .max()
                .ok_or("positive_bid_missing")
        });
        let ask = parsed.and_then(|(_, asks)| {
            asks.iter()
                .filter(|(p, q)| *p > Decimal::ZERO && *q > Decimal::ZERO)
                .map(|(p, _)| *p)
                .min()
                .ok_or("positive_ask_missing")
        });
        // 数量和即使有效空簿仍为零，BBA 缺侧不抹掉已知数量。
        let totals = self.parsed.as_ref().map_err(|e| *e).and_then(|(b, a)| {
            let s = |v: &Vec<(Decimal, Decimal)>| {
                v.iter()
                    .try_fold(Decimal::ZERO, |s, (_, q)| arithmetic(s.checked_add(*q)))
            };
            Ok((s(b)?, s(a)?))
        });
        PublicDepthStatisticsV1 {
            best_bid_usdt_per_btc: result(bid),
            best_ask_usdt_per_btc: result(ask),
            midpoint_usdt_per_btc: result(self.midpoint),
            spread_usdt_per_btc: result(ask.and_then(|a| bid.and_then(|b| subtract(a, b)))),
            bid_quantity_btc: result(totals.map(|v| v.0)),
            ask_quantity_btc: result(totals.map(|v| v.1)),
            imbalance: result(
                totals.and_then(|(b, a)| ratio(subtract(b, a)?, arithmetic(b.checked_add(a))?)),
            ),
            interval_coverage: "maintained_visible_levels_only",
        }
    }
}
/// 借用共同参考，不需要先出现市场，也不为每市场复制参考或深度。
struct References<'a> {
    binance_depth: &'a Arc<ObservedFactV1<BinanceDepthObservationV1>>,
    binance_trade: &'a ObservedFactV1<BinanceAggregateTradeReferenceV1>,
    chainlink_spot: &'a ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
    chainlink_twap_thirty: &'a ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    chainlink_twap_sixty: &'a ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
}
impl<'a> References<'a> {
    fn from_observations(observations: &'a MarketObservationSetV1) -> Option<Self> {
        if let Some((_, g)) = observations.shared_parts() {
            Some(Self {
                binance_depth: &g.binance_depth,
                binance_trade: &g.binance_trade,
                chainlink_spot: &g.chainlink_spot,
                chainlink_twap_thirty: &g.chainlink_twap_thirty,
                chainlink_twap_sixty: &g.chainlink_twap_sixty,
            })
        } else {
            observations.iter().next().map(|g| Self {
                binance_depth: &g.binance_depth,
                binance_trade: &g.binance_trade,
                chainlink_spot: &g.chainlink_spot,
                chainlink_twap_thirty: &g.chainlink_twap_thirty,
                chainlink_twap_sixty: &g.chainlink_twap_sixty,
            })
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Point {
    at: i64,
    price: Decimal,
    scalar: f64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Basis {
    binance: Decimal,
    reference: Decimal,
    group: PublicGroupReferenceV1,
    saved: Option<SavedBasis>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedBasis {
    initialized: bool,
    db: Decimal,
    dr: Decimal,
    innovation: Decimal,
    baseline_group: PublicGroupReferenceV1,
    related_group: PublicGroupReferenceV1,
    at: i64,
}
#[derive(Debug)]
struct CommonStatistics {
    momentum: Vec<PublicMomentumV1>,
    spot_variance: Vec<PublicSpotVarianceV1>,
}
/// 同一冻结历史的既有读取缓存；共同结果仅保留最后一个精确查询时点。
#[derive(Debug, Default)]
struct StatisticsReadCache {
    markets: std::collections::BTreeMap<String, (i64, Arc<PublicMarketStatisticsV1>)>,
    common: Option<(i64, CommonStatistics)>,
}
#[cfg(test)]
thread_local! {
    // 只用于有限成本夹具：实际共同计算次数与进入原方差算法的点数。
    static COMMON_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}
/// 纯历史状态可持久保存；观察、规则和账户仍引用各自正式合同。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicStatisticsHistoryV1 {
    pub identity: Arc<PublicStatisticsIdentityV1>,
    pub config: Arc<PublicStatisticsConfigV1>,
    pub group: PublicGroupReferenceV1,
    pub applied_at_utc_ms: i64,
    midpoint: Vector<Point>,
    #[serde(default)]
    blb_endpoints: imbl::OrdMap<String, Vector<Arc<PublicBlbEndpointV1>>>,
    // 只记上一组的共享输入形状；首次、跳组、非共享切换与冷恢复沿完整后备。
    #[serde(skip)]
    blb_shared_group: bool,
    active_integral_markets: imbl::OrdMap<String, MarketIdentityV2>,
    completed_integrals: imbl::OrdMap<String, Vec<PublicSpotIntegralV1>>,
    spot: Vector<Point>,
    evidence: [Vec<DirectObservationAnchorV1>; 5],
    source_applied: [Option<i64>; 5],
    basis: [Option<Basis>; 3],
    #[serde(skip)]
    prepared_depth: Option<Arc<PreparedBlbDepthV1>>,
    #[serde(skip)]
    depth: Option<Arc<PublicDepthStatisticsV1>>,
    #[serde(skip)]
    cache: Arc<Mutex<StatisticsReadCache>>,
}
impl PublicStatisticsHistoryV1 {
    pub fn history_points(&self) -> (usize, usize) {
        (self.midpoint.len(), self.spot.len())
    }
    fn retain_ms(&self) -> i64 {
        self.config
            .momentum_windows_ms
            .iter()
            .chain(&self.config.spot_variance_windows_ms)
            .chain(&self.config.terminal_integral_windows_ms)
            .copied()
            .max()
            .unwrap_or(0)
            .min(i64::MAX as u64) as i64
    }
    fn insert(points: &mut Vector<Point>, at: i64, price: Decimal, retain_ms: i64) {
        if points.back().is_some_and(|p| p.at == at) {
            points.pop_back();
        }
        points.push_back(Point {
            at,
            price,
            scalar: price.to_string().parse().expect("Decimal 可投影为有限 f64"),
        });
        let from = at.saturating_sub(retain_ms);
        // 方差右端等于左界时仍需其完整左支撑，因此保留第二点 < from 才删除。
        while points.len() > 2 && points[1].at < from {
            points.pop_front();
        }
    }
    fn advance(
        &mut self,
        group: PublicGroupReferenceV1,
        at: i64,
        observation: &References<'_>,
        records: &[PublicBusinessFactV1],
    ) {
        self.group = group;
        self.applied_at_utc_ms = at;
        self.cache = Default::default();
        for record in records {
            if let PublicBusinessPayloadV1::MarketFact { market, .. } = &record.payload {
                if !self
                    .completed_integrals
                    .contains_key(&market.condition_id.0)
                {
                    self.active_integral_markets
                        .insert(market.condition_id.0.clone(), market.clone());
                }
            }
        }
        // 终局积分在裁剪历史前保存紧凑结果；旧市场不会随新市场历史回收而丢失。
        let finished = self
            .active_integral_markets
            .values()
            .filter(|m| m.window.end_at_utc_ms <= at)
            .cloned()
            .collect::<Vec<_>>();
        for market in finished {
            self.completed_integrals.insert(
                market.condition_id.0.clone(),
                self.integrals(&market, market.window.end_at_utc_ms),
            );
            self.active_integral_markets.remove(&market.condition_id.0);
        }

        let current = [
            evidence(observation.binance_depth),
            evidence(observation.binance_trade),
            evidence(observation.chainlink_spot),
            evidence(observation.chainlink_twap_thirty),
            evidence(observation.chainlink_twap_sixty),
        ];
        let changed: [bool; 5] = std::array::from_fn(|i| current[i] != self.evidence[i]);
        let retain_ms = self.retain_ms();
        if self.prepared_depth.is_none() || changed[0] {
            let prepared = Arc::new(PreparedBlbDepthV1::new(observation.binance_depth.clone()));
            self.depth = Some(Arc::new(prepared.public_depth_statistics()));
            self.prepared_depth = Some(prepared);
        }
        if changed[0]
            && let Some(midpoint) = self
                .depth
                .as_ref()
                .and_then(|d| d.midpoint_usdt_per_btc.value)
        {
            Self::insert(&mut self.midpoint, at, midpoint, retain_ms);
        }
        if changed[2]
            && let Some(spot) =
                eligible(observation.chainlink_spot).and_then(|v| number(&v.price_usd_per_btc).ok())
        {
            Self::insert(&mut self.spot, at, spot, retain_ms);
        }
        let b =
            eligible(observation.binance_trade).and_then(|v| number(&v.price_usdt_per_btc).ok());
        let references = [
            eligible(observation.chainlink_spot).and_then(|v| number(&v.price_usd_per_btc).ok()),
            eligible(observation.chainlink_twap_thirty)
                .and_then(|v| number(&v.price_usd_per_btc).ok()),
            eligible(observation.chainlink_twap_sixty)
                .and_then(|v| number(&v.price_usd_per_btc).ok()),
        ];
        for i in 0..3 {
            if !(changed[1] || changed[i + 2]) {
                continue;
            }
            if let (Some(b), Some(r)) = (b, references[i]) {
                let initialized = self.basis[i].is_none();
                let base = self.basis[i].get_or_insert_with(|| Basis {
                    binance: b,
                    reference: r,
                    group: self.group.clone(),
                    saved: None,
                });
                let computed = subtract(b, base.binance)
                    .and_then(|db| Ok((db, subtract(r, base.reference)?)))
                    .and_then(|(db, dr)| Ok((db, dr, subtract(db, dr)?)));
                base.saved = computed.ok().map(|(db, dr, innovation)| SavedBasis {
                    initialized,
                    db,
                    dr,
                    innovation,
                    baseline_group: base.group.clone(),
                    related_group: self.group.clone(),
                    at,
                });
                if changed[i + 2] {
                    base.binance = b;
                    base.reference = r;
                    base.group = self.group.clone();
                }
            } else {
                self.basis[i] = None;
            }
        }
        for i in 0..5 {
            if changed[i] {
                self.source_applied[i] = Some(at);
            }
        }
        self.evidence = current;
    }
    fn retain_blb_endpoint(
        &mut self,
        market: &MarketIdentityV2,
        anchor: &ObservedFactV1<MarketPriceAnchorV1>,
        depth: &Arc<ObservedFactV1<BinanceDepthObservationV1>>,
    ) {
        #[cfg(test)]
        cost_tests::visit();
        let Some(window) = self.config.blb_lookback_windows_ms.iter().max().copied() else {
            return;
        };
        // 本声明的消费者是市场存续期内的新开仓；结束市场不再展开F2。
        // 旧冻结持有其原持久节点；当前流不保留整日历史市场副本。
        if market.window.end_at_utc_ms < self.applied_at_utc_ms {
            self.blb_endpoints.remove(&market.condition_id.0);
            return;
        }
        let id = &market.condition_id.0;
        let unchanged = self
            .blb_endpoints
            .get(id)
            .and_then(|v| v.back())
            .is_some_and(|old| {
                old.market == *market
                    && old.official_anchor == *anchor
                    && old.depth_anchors == depth.direct_anchors
                    && old.depth_quality == depth.quality
            });
        if !unchanged {
            #[cfg(test)]
            cost_tests::evaluate();
            let value = self
                .prepared_depth
                .as_ref()
                .expect("advance已准备深度")
                .evaluate(anchor, false);
            let f1 = self.depth.as_ref().expect("advance已准备F1");
            let next = Arc::new(PublicBlbEndpointV1 {
                definition_version: value.definition_version.into(),
                identity: self.identity.clone(),
                config: self.config.clone(),
                market: market.clone(),
                group: self.group.clone(),
                applied_at_utc_ms: self.applied_at_utc_ms,
                status: value.status.into(),
                unknown_reason: value.unknown_reason.map(str::to_owned),
                midpoint_usdt_per_btc: value.midpoint_usdt_per_btc,
                official_k_usd_per_btc: value.official_k_usd_per_btc,
                side: value.side.map(str::to_owned),
                at_anchor: value.at_anchor,
                visible_quantity_btc: value.visible_quantity_btc,
                official_anchor: anchor.clone(),
                best_bid_usdt_per_btc: f1.best_bid_usdt_per_btc.clone(),
                best_ask_usdt_per_btc: f1.best_ask_usdt_per_btc.clone(),
                depth_quality: depth.quality,
                depth_anchors: depth.direct_anchors.clone(),
            });
            let points = self.blb_endpoints.entry(id.clone()).or_default();
            if points
                .back()
                .is_some_and(|p| p.applied_at_utc_ms == next.applied_at_utc_ms)
            {
                points.pop_back();
            }
            points.push_back(next);
        }
        if let Some(points) = self.blb_endpoints.get_mut(id) {
            let from = self.applied_at_utc_ms.saturating_sub(window as i64);
            // 保留左端最后asof支撑；持久容器裁剪不修改任何旧冻结前缀。
            while points.len() > 1 && points[1].applied_at_utc_ms <= from {
                points.pop_front();
            }
        }
    }
    fn retain_blb(&mut self, group: &ClosedBusinessGroupV1, incremental: bool) {
        if self.config.blb_lookback_windows_ms.is_empty() {
            return;
        }
        if let Some((markets, common)) = group.observations.shared_parts() {
            if incremental {
                // 已保留端点涵盖仍有BLB用途的旧市场；只从本组正式事实补新市场。
                // 临时集合仅去重本次读取，不保存另一份市场生命周期状态。
                let mut ids = self
                    .blb_endpoints
                    .keys()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                for record in group.records.iter() {
                    let market = match &record.payload {
                        PublicBusinessPayloadV1::MarketFact { market, .. } => Some(market),
                        PublicBusinessPayloadV1::PolymarketMessage(message) => {
                            Some(&message.market)
                        }
                        _ => None,
                    };
                    if let Some(market) = market {
                        ids.insert(market.condition_id.0.clone());
                    }
                }
                for id in ids {
                    if let Some(market) = markets.get(&id) {
                        self.retain_blb_endpoint(
                            &market.market,
                            &market.official_anchor,
                            &common.binance_depth,
                        );
                    }
                }
            } else {
                for market in markets.values() {
                    self.retain_blb_endpoint(
                        &market.market,
                        &market.official_anchor,
                        &common.binance_depth,
                    );
                }
            }
            self.blb_shared_group = true;
        } else {
            for market in group.observations.iter() {
                self.retain_blb_endpoint(
                    &market.market,
                    &market.official_anchor,
                    &market.binance_depth,
                );
            }
            self.blb_shared_group = false;
        }
    }
    fn blb_lookbacks(&self, market: &MarketIdentityV2, t: i64) -> Vec<PublicBlbLookbackV1> {
        let point = |at| {
            (t <= market.window.end_at_utc_ms)
                .then_some(())
                .and_then(|_| {
                    self.blb_endpoints
                        .get(&market.condition_id.0)
                        .and_then(|points| {
                            let (mut lo, mut hi) = (0, points.len());
                            while lo < hi {
                                let mid = (lo + hi) / 2;
                                if points[mid].applied_at_utc_ms <= at {
                                    lo = mid + 1;
                                } else {
                                    hi = mid;
                                }
                            }
                            lo.checked_sub(1).map(|n| points[n].clone())
                        })
                })
        };
        self.config
            .blb_lookback_windows_ms
            .iter()
            .map(|window_ms| {
                let left_query_at_utc_ms = t.saturating_sub(*window_ms as i64);
                PublicBlbLookbackV1 {
                    window_ms: *window_ms,
                    left_query_at_utc_ms,
                    left: point(left_query_at_utc_ms),
                    right: point(t),
                }
            })
            .collect()
    }
    fn integrals(&self, market: &MarketIdentityV2, t: i64) -> Vec<PublicSpotIntegralV1> {
        self.config
            .terminal_integral_windows_ms
            .iter()
            .map(|h| {
                let from = market.window.end_at_utc_ms.saturating_sub(*h as i64);
                let end = t.min(market.window.end_at_utc_ms);
                let left = asof(&self.spot, from);
                let start = left
                    .as_ref()
                    .map(|_| from)
                    .or_else(|| self.spot.front().map(|p| p.at.max(from)))
                    .filter(|s| *s < end);
                let mut total = 0.;
                #[cfg(feature = "observation-diagnostics")]
                let mut scanned = 0u64;
                let mut cursor = start.unwrap_or(end);
                let mut held = asof(&self.spot, cursor).map(|p| p.scalar);
                for p in self
                    .spot
                    .iter()
                    .skip(partition(&self.spot, |p| p.at <= cursor))
                    .take_while(|p| p.at < end)
                {
                    #[cfg(feature = "observation-diagnostics")]
                    {
                        scanned += 1;
                    }
                    if let Some(s) = held {
                        total += s * (p.at - cursor) as f64 / 1000.;
                    }
                    cursor = p.at;
                    held = Some(p.scalar);
                }
                #[cfg(feature = "observation-diagnostics")]
                observation_diagnostics::add(|w| w.integral_points += scanned);
                if let Some(s) = held {
                    total += s * (end - cursor) as f64 / 1000.;
                }
                PublicSpotIntegralV1 {
                    window_ms: *h,
                    observed_through_utc_ms: end,
                    covered_from_utc_ms: start,
                    observed_seconds: start.map_or(0., |s| (end - s) as f64 / 1000.),
                    integral_usd_per_btc_seconds: if left.is_some() {
                        StatisticValueV1::known(total)
                    } else {
                        StatisticValueV1::missing("left_spot_support_missing")
                    },
                    coverage: if left.is_some() {
                        "observed_hold_path_not_continuity_proof".into()
                    } else {
                        "partial_missing_left_support".into()
                    },
                }
            })
            .collect()
    }
    fn read_common(&self, t: i64) -> CommonStatistics {
        #[cfg(feature = "observation-diagnostics")]
        let measured = std::time::Instant::now();
        #[cfg(test)]
        COMMON_WORK.with(|n| {
            let (calls, points) = n.get();
            n.set((calls + 1, points));
        });
        let at_or_before = |v: &Vector<Point>, at: i64| -> Option<Point> {
            let n = partition(v, |p| p.at <= at);
            n.checked_sub(1).map(|i| v[i].clone())
        };
        let momentum = self
            .config
            .momentum_windows_ms
            .iter()
            .map(|h| {
                let left = at_or_before(&self.midpoint, t.saturating_sub(*h as i64));
                let right = at_or_before(&self.midpoint, t);
                PublicMomentumV1 {
                    window_ms: *h,
                    change_usdt_per_btc: result(
                        left.as_ref()
                            .zip(right.as_ref())
                            .ok_or("left_midpoint_support_missing")
                            .and_then(|(l, r)| subtract(r.price, l.price)),
                    ),
                    left_applied_at_utc_ms: left.map(|p| p.at),
                    right_applied_at_utc_ms: right.map(|p| p.at),
                }
            })
            .collect();
        let spot_variance = self
            .config
            .spot_variance_windows_ms
            .iter()
            .map(|h| {
                let from = t.saturating_sub(*h as i64);
                let index = partition(&self.spot, |p| p.at < from).saturating_sub(1);
                #[cfg(feature = "observation-diagnostics")]
                let mut scanned = 0u64;
                let statistics = spot_variance_statistics_v1(
                    self.spot.iter().skip(index).filter(|p| p.at <= t).map(|p| {
                        #[cfg(feature = "observation-diagnostics")]
                        {
                            scanned += 1;
                        }
                        #[cfg(test)]
                        COMMON_WORK.with(|n| {
                            let (calls, points) = n.get();
                            n.set((calls, points + 1));
                        });
                        (p.at, p.scalar)
                    }),
                    from,
                );
                #[cfg(feature = "observation-diagnostics")]
                observation_diagnostics::add(|w| w.variance_points += scanned);
                PublicSpotVarianceV1 {
                    window_ms: *h,
                    sigma_usd_per_btc_per_sqrt_second: statistics.sigma_usd_per_sqrt_second(),
                    statistics,
                }
            })
            .collect();
        #[cfg(feature = "observation-diagnostics")]
        observation_diagnostics::add(|w| {
            w.common_computations += 1;
            w.common_ns += measured.elapsed().as_nanos();
        });
        CommonStatistics {
            momentum,
            spot_variance,
        }
    }
    /// 只能由已持有此合法前缀的读取层传入对应 F0；不触及未来流或账户状态。
    pub fn read(&self, o: &MarketBusinessObservationV1, t: i64) -> Arc<PublicMarketStatisticsV1> {
        #[cfg(feature = "observation-diagnostics")]
        let measured = std::time::Instant::now();
        let mut cache = self.cache.lock().unwrap();
        #[cfg(feature = "observation-diagnostics")]
        observation_diagnostics::add(|w| {
            w.reads += 1;
            w.cache_lock_ns += measured.elapsed().as_nanos();
        });
        let id = &o.market.condition_id.0;
        if let Some((old_t, result)) = cache.markets.get(id)
            && *old_t == t
        {
            #[cfg(feature = "observation-diagnostics")]
            observation_diagnostics::add(|w| {
                w.market_hits += 1;
                w.read_ns += measured.elapsed().as_nanos();
            });
            return result.clone();
        }
        #[cfg(feature = "observation-diagnostics")]
        if cache.common.as_ref().is_some_and(|(at, _)| *at == t) {
            observation_diagnostics::add(|w| w.common_hits += 1);
        }
        if cache.common.as_ref().is_none_or(|(old_t, _)| *old_t != t) {
            cache.common = Some((t, self.read_common(t)));
        }
        #[cfg(feature = "observation-diagnostics")]
        let market_measured = std::time::Instant::now();
        let common = &cache.common.as_ref().expect("已准备此时点共同结果").1;
        let spot_integrals = self
            .completed_integrals
            .get(&o.market.condition_id.0)
            .cloned()
            .unwrap_or_else(|| self.integrals(&o.market, t));
        let depth = self.depth.clone().unwrap_or_else(|| {
            Arc::new(PreparedBlbDepthV1::new(o.binance_depth.clone()).public_depth_statistics())
        });
        let pressure = eligible(&o.binance_trade)
            .ok_or("trade_not_observed")
            .and_then(|trade| {
                let q = number(&trade.quantity_btc)?;
                let b = depth
                    .bid_quantity_btc
                    .value
                    .ok_or("depth_quantities_missing")?;
                let a = depth
                    .ask_quantity_btc
                    .value
                    .ok_or("depth_quantities_missing")?;
                let signed = if trade.buyer_is_maker { -q } else { q };
                ratio(
                    signed,
                    arithmetic(q.checked_add(b).and_then(|v| v.checked_add(a)))?,
                )
            });
        let refs = [
            BtcSettlementReferenceV1::ChainlinkSpot,
            BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds,
            BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds,
        ];
        let out = Arc::new(PublicMarketStatisticsV1 {
            definition_version: PUBLIC_STATISTICS_VERSION_V1,
            identity: self.identity.clone(),
            config: self.config.clone(),
            group: self.group.clone(),
            applied_at_utc_ms: self.applied_at_utc_ms,
            observed_at_utc_ms: t,
            market: o.market.clone(),
            since_market_start_ms: t - o.market.window.start_at_utc_ms,
            until_market_end_ms: o.market.window.end_at_utc_ms - t,
            depth,
            blb_lookbacks: self.blb_lookbacks(&o.market, t),
            blb: self.prepared_depth.as_ref().map_or_else(
                || {
                    PreparedBlbDepthV1::new(o.binance_depth.clone())
                        .evaluate(&o.official_anchor, false)
                },
                |p| p.evaluate(&o.official_anchor, false),
            ),
            momentum: common.momentum.clone(),
            spot_variance: common.spot_variance.clone(),
            spot_integrals,
            latest_trade_pressure: result(pressure),
            basis: std::array::from_fn(|i| {
                self.basis[i]
                    .as_ref()
                    .and_then(|b| b.saved.as_ref())
                    .map(|s| PublicBasisInnovationV1 {
                        reference: refs[i],
                        initialized: s.initialized,
                        delta_binance_usdt_per_btc: s.db,
                        delta_reference_usd_per_btc: s.dr,
                        innovation_usd_per_btc_numeric_proxy: s.innovation,
                        baseline_group: s.baseline_group.clone(),
                        related_group: s.related_group.clone(),
                        related_applied_at_utc_ms: s.at,
                    })
            }),
            pm_quantities: o
                .assets
                .iter()
                .map(|asset| PublicAssetQuantitiesV1 {
                    asset: asset.asset.clone(),
                    bid_shares: result(
                        eligible(&asset.bids)
                            .ok_or("pm_bid_not_observed")
                            .and_then(|levels| sum(levels.iter().map(|l| &l.quantity_shares))),
                    ),
                    ask_shares: result(
                        eligible(&asset.asks)
                            .ok_or("pm_ask_not_observed")
                            .and_then(|levels| sum(levels.iter().map(|l| &l.quantity_shares))),
                    ),
                })
                .collect(),
            source_applied_at_utc_ms: self.source_applied,
            source_received_age_ms: std::array::from_fn(|i| {
                self.evidence[i]
                    .iter()
                    .map(|a| a.received_at_utc_ms)
                    .max()
                    .map(|at| t - at)
            }),
        });
        cache.markets.insert(id.clone(), (t, out.clone()));
        #[cfg(feature = "observation-diagnostics")]
        observation_diagnostics::add(|w| {
            w.market_builds += 1;
            w.market_ns += market_measured.elapsed().as_nanos();
            w.read_ns += measured.elapsed().as_nanos();
        });
        out
    }
}
fn asof(v: &Vector<Point>, at: i64) -> Option<Point> {
    let n = partition(v, |p| p.at <= at);
    n.checked_sub(1).map(|i| v[i].clone())
}
fn partition(points: &Vector<Point>, predicate: impl Fn(&Point) -> bool) -> usize {
    let (mut lo, mut hi) = (0, points.len());
    #[cfg(feature = "observation-diagnostics")]
    let mut probes = 0u64;
    while lo < hi {
        #[cfg(feature = "observation-diagnostics")]
        {
            probes += 1;
        }
        let m = (lo + hi) / 2;
        if predicate(&points[m]) {
            lo = m + 1;
        } else {
            hi = m;
        }
    }
    #[cfg(feature = "observation-diagnostics")]
    observation_diagnostics::add(|w| w.partition_probes += probes);
    lo
}
/// 宿主显式创建并交给同输入/时序的消费者；没有全局缓存或路线默认参数。
pub struct PublicStatisticsStreamV1 {
    identity: Arc<PublicStatisticsIdentityV1>,
    config: Arc<PublicStatisticsConfigV1>,
    latest: Mutex<Option<Arc<PublicStatisticsHistoryV1>>>,
}
impl PublicStatisticsStreamV1 {
    pub fn identity(&self) -> &PublicStatisticsIdentityV1 {
        &self.identity
    }
    pub fn new(
        identity: PublicStatisticsIdentityV1,
        config: PublicStatisticsConfigV1,
    ) -> Result<Arc<Self>, String> {
        if config
            .momentum_windows_ms
            .iter()
            .chain(&config.spot_variance_windows_ms)
            .chain(&config.terminal_integral_windows_ms)
            .chain(&config.blb_lookback_windows_ms)
            .any(|h| *h == 0 || *h > i64::MAX as u64)
        {
            return Err("公共统计窗口必须是正 i64 毫秒".into());
        }
        Ok(Arc::new(Self {
            identity: Arc::new(identity),
            config: Arc::new(config),
            latest: Mutex::new(None),
        }))
    }
    pub fn from_history(history: Arc<PublicStatisticsHistoryV1>) -> Arc<Self> {
        Arc::new(Self {
            identity: history.identity.clone(),
            config: history.config.clone(),
            latest: Mutex::new(Some(history)),
        })
    }
}
/// 缓存寿命随原 prepared 组；供料队列自然保留慢消费者所需前缀，不存全日副本。
#[derive(Default)]
pub struct PreparedPublicStatisticsV1(
    Mutex<
        Vec<(
            Weak<PublicStatisticsStreamV1>,
            Arc<PublicStatisticsHistoryV1>,
        )>,
    >,
);
impl PreparedPublicStatisticsV1 {
    pub fn consume(
        &self,
        stream: &Arc<PublicStatisticsStreamV1>,
        group: PublicGroupReferenceV1,
        at: i64,
        prepared_group: &ClosedBusinessGroupV1,
    ) -> Result<Option<Arc<PublicStatisticsHistoryV1>>, String> {
        let mut cache = self.0.lock().unwrap();
        if let Some((_, prefix)) = cache
            .iter()
            .find(|(s, _)| s.ptr_eq(&Arc::downgrade(stream)))
        {
            if prefix.group != group || prefix.applied_at_utc_ms != at {
                return Err("共享统计输入前缀或实际应用时间不同，必须使用独立输入身份".into());
            }
            return Ok(Some(prefix.clone()));
        }
        if stream.identity.stream_header.input_stream_id != group.input_stream_id {
            return Err("统计身份与公共输入流不同".into());
        }
        let Some(o) = References::from_observations(&prepared_group.observations) else {
            return Ok(None);
        };
        let mut latest = stream.latest.lock().unwrap();
        if latest.as_ref().is_some_and(|p| {
            p.group.group_position >= group.group_position || p.applied_at_utc_ms > at
        }) {
            return Err("统计组必须沿首次成功应用的公共顺序消费".into());
        }
        let incremental_blb = latest.as_ref().is_some_and(|h| {
            h.blb_shared_group
                && h.group.group_position.checked_add(1) == Some(group.group_position)
        });
        let mut next = latest
            .as_ref()
            .map(|h| h.as_ref().clone())
            .unwrap_or_else(|| PublicStatisticsHistoryV1 {
                identity: stream.identity.clone(),
                config: stream.config.clone(),
                group: group.clone(),
                applied_at_utc_ms: at,
                midpoint: Vector::new(),
                blb_endpoints: Default::default(),
                blb_shared_group: false,
                active_integral_markets: Default::default(),
                completed_integrals: Default::default(),
                spot: Vector::new(),
                evidence: Default::default(),
                source_applied: [None; 5],
                basis: Default::default(),
                prepared_depth: None,
                depth: None,
                cache: Default::default(),
            });
        next.advance(group, at, &o, &prepared_group.records);
        next.retain_blb(prepared_group, incremental_blb);
        let next = Arc::new(next);
        *latest = Some(next.clone());
        cache.retain(|(s, _)| s.strong_count() > 0);
        cache.push((Arc::downgrade(stream), next.clone()));
        Ok(Some(next))
    }
}

#[cfg(test)]
mod reuse_tests {
    use super::*;

    fn reference(position: u64) -> PublicGroupReferenceV1 {
        PublicGroupReferenceV1 {
            input_stream_id: "statistics-reuse".into(),
            applied_prefix: AppliedIngressPrefixV1 {
                input_stream_id: "statistics-reuse".into(),
                next_input_position: position + 1,
            },
            group_position: position,
            group_id: Sha256Hex(format!("{position:064x}")),
            closed_at_input_position: position,
        }
    }
    fn fact<T>(value: T, field: &str, position: u64, at: i64) -> ObservedFactV1<T> {
        ObservedFactV1 {
            value: Some(value),
            quality: MarketDataQuality::Fresh,
            direct_anchors: vec![DirectObservationAnchorV1 {
                field_path: field.into(),
                position: PublicFactPositionV1 {
                    input_stream_id: "statistics-reuse".into(),
                    closed_at_input_position: position,
                    fact_ordinal: 0,
                },
                group_id: reference(position).group_id,
                source_message_id: Sha256Hex(format!("{position:064x}")),
                source_event_at_utc_ms: Some(at),
                received_at_utc_ms: at,
            }],
            dependency_frontier: Some(reference(position).applied_prefix),
        }
    }
    fn group(position: u64, at: i64, price: i64) -> ClosedBusinessGroupV1 {
        let r = reference(position);
        let markets = (1..=2)
            .map(|id| {
                let market = MarketIdentityV2 {
                    condition_id: ConditionId(format!("0x{id:064x}")),
                    clob_market_id: ClobMarketId(id.to_string()),
                    window: MarketWindowV2 {
                        start_at_utc_ms: 0,
                        end_at_utc_ms: 300000,
                    },
                };
                let assets = (0..2)
                    .map(|leg| AssetBusinessObservationV1 {
                        asset: AssetIdentityV1 {
                            token_id: TokenId((id * 2 + leg).to_string()),
                            outcome: OutcomeIdentityV1 {
                                index: leg as u32,
                                label: if leg == 0 { "Up" } else { "Down" }.into(),
                            },
                        },
                        bids: Arc::new(Default::default()),
                        asks: Arc::new(fact(vec![], "asks", position, at)),
                        best_bid_ask: Default::default(),
                        valuation_best_bid: Default::default(),
                        tick: Default::default(),
                        public_trade: Default::default(),
                    })
                    .collect();
                (
                    market.condition_id.0.clone(),
                    Arc::new(MarketObservationSnapshotV1 {
                        market,
                        assets,
                        lifecycle: Default::default(),
                        active: fact(true, "active", position, at),
                        closed: fact(false, "closed", position, at),
                        accepting_orders: fact(true, "accepting_orders", position, at),
                        terminal: Default::default(),
                        official_anchor: Default::default(),
                        settlement_reference: Default::default(),
                        temporary_reference: None,
                        effective_reference: EffectiveMarketReferenceV1::Missing,
                    }),
                )
            })
            .collect();
        ClosedBusinessGroupV1 {
            group_id: r.group_id.clone(),
            group_position: position,
            member_input_positions: vec![position],
            closed_at_input_position: position,
            records: Arc::from([]),
            observations: MarketObservationSetV1::shared(
                markets,
                MarketObservationGroupV1 {
                    schema_version: "market_business_observation_v1".into(),
                    group_id: r.group_id,
                    group_position: position,
                    visible_prefix: r.applied_prefix,
                    chainlink_spot: fact(
                        ChainlinkBtcUsdReferenceV1 {
                            symbol: "BTCUSD".into(),
                            price_usd_per_btc: ExactDecimal(price.to_string()),
                            message_timestamp_utc_ms: at,
                        },
                        "chainlink_spot",
                        position,
                        at,
                    ),
                    chainlink_twap_thirty: Default::default(),
                    chainlink_twap_sixty: Default::default(),
                    binance_depth: Arc::new(fact(
                        BinanceDepthObservationV1::Full(Arc::new(BinanceDepthStateReferenceV1 {
                            symbol: "BTCUSDT".into(),
                            last_update_id: position,
                            bids: vec![ReferencePriceLevelV1 {
                                price_usdt_per_btc: ExactDecimal((price - 1).to_string()),
                                quantity_btc: ExactDecimal("1".into()),
                            }],
                            asks: vec![ReferencePriceLevelV1 {
                                price_usdt_per_btc: ExactDecimal((price + 1).to_string()),
                                quantity_btc: ExactDecimal("1".into()),
                            }],
                        })),
                        "binance_depth",
                        position,
                        at,
                    )),
                    binance_trade: Default::default(),
                    group_events: Arc::from([]),
                    source_progress: Default::default(),
                },
            ),
        }
    }
    pub(super) fn stream() -> Arc<PublicStatisticsStreamV1> {
        PublicStatisticsStreamV1::new(
            PublicStatisticsIdentityV1 {
                input_set_identity: Sha256Hex("1".repeat(64)),
                rules_identity: Sha256Hex("2".repeat(64)),
                delivery_policy: "synthetic-fixed-zero-delay".into(),
                stream_header: PublicInputStreamHeaderV1 {
                    input_stream_id: "statistics-reuse".into(),
                    canonical_rule_version: "canonical-v1".into(),
                    observation_version: "market_business_observation_v1".into(),
                    ordering_version: "input-order-v1".into(),
                    input_scope: "finite-synthetic".into(),
                    max_pending_ws_terminals: 8,
                    initial_state: PublicInitialStateV1::Empty,
                },
            },
            PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![1000],
                spot_variance_windows_ms: vec![1000],
                terminal_integral_windows_ms: vec![1000],
                blb_lookback_windows_ms: vec![],
            },
        )
        .unwrap()
    }

    #[test]
    fn exact_time_common_slot_matches_uncached_reads_and_preserves_same_ms_old_prefix() {
        let stream = stream();
        let mut prefixes = vec![];
        let mut groups = vec![];
        for (position, at, price) in [(0, 1000, 100), (1, 2000, 102), (2, 2000, 104)] {
            let g = group(position, at, price);
            let prepared = PreparedPublicStatisticsV1::default();
            let h = prepared
                .consume(&stream, reference(position), at, &g)
                .unwrap()
                .unwrap();
            assert!(Arc::ptr_eq(
                &h,
                &prepared
                    .consume(&stream, reference(position), at, &g)
                    .unwrap()
                    .unwrap()
            ));
            assert!(
                prepared
                    .consume(&stream, reference(position), at + 1, &g)
                    .is_err()
            );
            groups.push(g);
            prefixes.push(h);
        }
        let last = &prefixes[2];
        let observations = groups[2].observations.iter().cloned().collect::<Vec<_>>();
        // 每次直接读取的独立空缓存作参照，不复制统计公式。
        let expected = observations
            .iter()
            .map(|o| {
                let mut h = last.as_ref().clone();
                h.cache = Default::default();
                h.read(o, 3000)
            })
            .collect::<Vec<_>>();
        COMMON_WORK.with(|n| n.set((0, 0)));
        let first = last.read(&observations[0], 3000);
        let second = last.read(&observations[1], 3000);
        assert_eq!(first, expected[0]);
        assert_eq!(second, expected[1]);
        assert_eq!(COMMON_WORK.with(|n| n.get()), (1, 2));
        assert_eq!(
            first.spot_variance[0].sigma_usd_per_btc_per_sqrt_second,
            Some(4.0)
        );
        assert_eq!(
            first.momentum[0].change_usdt_per_btc.value,
            Some(Decimal::ZERO)
        );
        assert!(first.pm_quantities[0].bid_shares.value.is_none());
        assert_eq!(first.pm_quantities[0].ask_shares.value, Some(Decimal::ZERO));
        assert!(first.blb.official_k_usd_per_btc.is_none());
        assert!(first.blb.visible_quantity_btc.is_none());
        let later_query = last.read(&observations[0], 3001);
        assert!(
            later_query.spot_variance[0]
                .sigma_usd_per_btc_per_sqrt_second
                .is_none()
        );
        assert_eq!(last.read(&observations[0], 3000), first);
        assert_eq!(COMMON_WORK.with(|n| n.get()).0, 3);
        let next = group(3, 2000, 106);
        let next = PreparedPublicStatisticsV1::default()
            .consume(&stream, reference(3), 2000, &next)
            .unwrap()
            .unwrap();
        assert!(!Arc::ptr_eq(&last.cache, &next.cache));
        let updated = next.read(&observations[0], 3000);
        assert_eq!(
            updated.spot_variance[0].sigma_usd_per_btc_per_sqrt_second,
            Some(6.0)
        );
        assert_eq!(last.read(&observations[0], 3000), first);
        assert_eq!(
            prefixes[0]
                .read(&groups[0].observations[0], 1000)
                .spot_variance[0]
                .sigma_usd_per_btc_per_sqrt_second,
            None
        );
    }
}

#[cfg(test)]
#[path = "public_statistics_cost_tests.rs"]
mod cost_tests;

#[cfg(test)]
#[path = "public_statistics_blb_tests.rs"]
mod blb_reuse_tests;
