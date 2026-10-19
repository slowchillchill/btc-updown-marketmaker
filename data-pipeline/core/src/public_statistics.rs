//! 公共组驱动的持久历史。节点按需共享，旧前缀永不读取当前流的可变状态。
use crate::{blb::PreparedBlbDepthV1, spot_statistics::spot_variance_statistics_v1};
use contracts::*;
use imbl::Vector;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, Weak};

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
/// 纯历史状态可持久保存；观察、规则和账户仍引用各自正式合同。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicStatisticsHistoryV1 {
    pub identity: Arc<PublicStatisticsIdentityV1>,
    pub config: Arc<PublicStatisticsConfigV1>,
    pub group: PublicGroupReferenceV1,
    pub applied_at_utc_ms: i64,
    midpoint: Vector<Point>,
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
    cache: Arc<Mutex<std::collections::BTreeMap<String, (i64, Arc<PublicMarketStatisticsV1>)>>>,
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
                let mut cursor = start.unwrap_or(end);
                let mut held = asof(&self.spot, cursor).map(|p| p.scalar);
                for p in self
                    .spot
                    .iter()
                    .skip(partition(&self.spot, |p| p.at <= cursor))
                    .take_while(|p| p.at < end)
                {
                    if let Some(s) = held {
                        total += s * (p.at - cursor) as f64 / 1000.;
                    }
                    cursor = p.at;
                    held = Some(p.scalar);
                }
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
    /// 只能由已持有此合法前缀的读取层传入对应 F0；不触及未来流或账户状态。
    pub fn read(&self, o: &MarketBusinessObservationV1, t: i64) -> Arc<PublicMarketStatisticsV1> {
        let mut cache = self.cache.lock().unwrap();
        let id = &o.market.condition_id.0;
        if let Some((old_t, result)) = cache.get(id)
            && *old_t == t
        {
            return result.clone();
        }
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
                let statistics = spot_variance_statistics_v1(
                    self.spot
                        .iter()
                        .skip(index)
                        .filter(|p| p.at <= t)
                        .map(|p| (p.at, p.scalar)),
                    from,
                );
                PublicSpotVarianceV1 {
                    window_ms: *h,
                    sigma_usd_per_btc_per_sqrt_second: statistics.sigma_usd_per_sqrt_second(),
                    statistics,
                }
            })
            .collect();
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
            blb: self.prepared_depth.as_ref().map_or_else(
                || {
                    PreparedBlbDepthV1::new(o.binance_depth.clone())
                        .evaluate(&o.official_anchor, false)
                },
                |p| p.evaluate(&o.official_anchor, false),
            ),
            momentum,
            spot_variance,
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
        cache.insert(id.clone(), (t, out.clone()));
        out
    }
}
fn asof(v: &Vector<Point>, at: i64) -> Option<Point> {
    let n = partition(v, |p| p.at <= at);
    n.checked_sub(1).map(|i| v[i].clone())
}
fn partition(points: &Vector<Point>, predicate: impl Fn(&Point) -> bool) -> usize {
    let (mut lo, mut hi) = (0, points.len());
    while lo < hi {
        let m = (lo + hi) / 2;
        if predicate(&points[m]) {
            lo = m + 1;
        } else {
            hi = m;
        }
    }
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
        let mut next = latest
            .as_ref()
            .map(|h| h.as_ref().clone())
            .unwrap_or_else(|| PublicStatisticsHistoryV1 {
                identity: stream.identity.clone(),
                config: stream.config.clone(),
                group: group.clone(),
                applied_at_utc_ms: at,
                midpoint: Vector::new(),
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
        let next = Arc::new(next);
        *latest = Some(next.clone());
        cache.retain(|(s, _)| s.strong_count() > 0);
        cache.push((Arc::downgrade(stream), next.clone()));
        Ok(Some(next))
    }
}
