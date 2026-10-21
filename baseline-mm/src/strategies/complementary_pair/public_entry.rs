//! 只控制真实新初始批次；没有订单维护、撤换或修复副作用。
use super::*;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublicBlbEndpointEvidenceV4 {
    pub group: PublicGroupReferenceV1,
    pub applied_at_utc_ms: i64,
    pub official_anchor: ObservedFactV1<MarketPriceAnchorV1>,
    pub midpoint: Option<Decimal>,
    pub k: Option<Decimal>,
    pub side: Option<String>,
    pub quantity_btc: Option<Decimal>,
    pub status: String,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub depth_anchors: Vec<DirectObservationAnchorV1>,
}
impl From<&PublicBlbEndpointV1> for PublicBlbEndpointEvidenceV4 {
    fn from(v: &PublicBlbEndpointV1) -> Self {
        Self {
            group: v.group.clone(),
            applied_at_utc_ms: v.applied_at_utc_ms,
            official_anchor: v.official_anchor.clone(),
            midpoint: v.midpoint_usdt_per_btc,
            k: v.official_k_usd_per_btc,
            side: v.side.clone(),
            quantity_btc: v.visible_quantity_btc,
            status: v.status.clone(),
            best_bid: v.best_bid_usdt_per_btc.value,
            best_ask: v.best_ask_usdt_per_btc.value,
            depth_anchors: v.depth_anchors.clone(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublicEntryEvaluationV4 {
    pub observed_at_utc_ms: Option<i64>,
    pub group: Option<PublicGroupReferenceV1>,
    pub pm_quotes: Vec<[Option<Decimal>; 2]>,
    pub momentum_200ms: Option<Decimal>,
    pub momentum_left_applied_at_utc_ms: Option<i64>,
    pub momentum_right_applied_at_utc_ms: Option<i64>,
    pub c0_passed: bool,
    pub c0_reason: Option<String>,
    /// disabled / not_evaluated / incomparable / non_decreasing / veto_decreasing
    pub blb_outcome: String,
    pub blb_incomparable_reason: Option<String>,
    pub left_query_at_utc_ms: Option<i64>,
    pub left: Option<PublicBlbEndpointEvidenceV4>,
    pub right: Option<PublicBlbEndpointEvidenceV4>,
    pub delta_quantity_btc: Option<Decimal>,
    pub new_pair_request_id: Option<String>,
}
impl PublicEntryEvaluationV4 {
    pub fn allows_new_pair(&self) -> bool {
        self.c0_passed && self.blb_outcome != "veto_decreasing"
    }
}
fn positive_best(book: &ObservedFactV1<Vec<PriceLevelV1>>, bid: bool) -> Option<Decimal> {
    value(book)?
        .iter()
        .filter_map(|l| {
            let (p, q) = (
                decimal(&l.price_pusd_per_share).ok()?,
                decimal(&l.quantity_shares).ok()?,
            );
            (p > Decimal::ZERO && q > Decimal::ZERO).then_some(p)
        })
        .reduce(|a, b| if bid { a.max(b) } else { a.min(b) })
}
pub(super) fn evaluate(
    o: &PairObservation<'_>,
    stats: Option<&PublicMarketStatisticsV1>,
    veto: bool,
) -> PublicEntryEvaluationV4 {
    let momentum = stats.and_then(|s| s.momentum.iter().find(|m| m.window_ms == 200));
    let mut d = PublicEntryEvaluationV4 {
        observed_at_utc_ms: stats.map(|s| s.observed_at_utc_ms),
        group: stats.map(|s| s.group.clone()),
        pm_quotes: o
            .assets
            .iter()
            .map(|a| [positive_best(&a.bids, true), positive_best(&a.asks, false)])
            .collect(),
        momentum_200ms: momentum.and_then(|m| m.change_usdt_per_btc.value),
        momentum_left_applied_at_utc_ms: momentum.and_then(|m| m.left_applied_at_utc_ms),
        momentum_right_applied_at_utc_ms: momentum.and_then(|m| m.right_applied_at_utc_ms),
        c0_passed: false,
        c0_reason: None,
        blb_outcome: "not_evaluated".into(),
        blb_incomparable_reason: None,
        left_query_at_utc_ms: None,
        left: None,
        right: None,
        delta_quantity_btc: None,
        new_pair_request_id: None,
    };
    if d.pm_quotes.iter().any(|q| q.iter().any(Option::is_none)) {
        d.c0_reason = Some("pm_positive_book_quote_missing".into());
        return d;
    }
    if d.pm_quotes.iter().any(|q| q[0].unwrap() >= q[1].unwrap()) {
        d.c0_reason = Some("pm_book_not_strictly_uncrossed".into());
        return d;
    }
    if d.momentum_200ms != Some(Decimal::ZERO) {
        d.c0_reason = Some(
            if d.momentum_200ms.is_none() {
                "momentum_200ms_unknown"
            } else {
                "momentum_200ms_nonzero"
            }
            .into(),
        );
        return d;
    }
    d.c0_passed = true;
    if !veto {
        d.blb_outcome = "disabled".into();
        return d;
    }
    let lookback = stats.and_then(|s| s.blb_lookbacks.iter().find(|v| v.window_ms == 200));
    d.left_query_at_utc_ms = lookback.map(|v| v.left_query_at_utc_ms);
    d.left = lookback.and_then(|v| v.left.as_deref()).map(Into::into);
    d.right = lookback.and_then(|v| v.right.as_deref()).map(Into::into);
    let comparison = (|| -> Result<Decimal, &'static str> {
        let v = lookback.ok_or("lookback_not_materialized")?;
        let l = v.left.as_ref().ok_or("left_asof_support_missing")?;
        let r = v.right.as_ref().ok_or("right_endpoint_missing")?;
        if l.market != r.market || l.market != o.market {
            return Err("market_changed");
        }
        if l.definition_version != r.definition_version || l.config != r.config {
            return Err("definition_or_parameters_changed");
        }
        if l.identity != r.identity {
            return Err("public_input_clock_changed");
        }
        let normal = |p: &PublicBlbEndpointV1| {
            matches!(
                p.depth_quality,
                MarketDataQuality::Fresh | MarketDataQuality::Stale
            ) && match (p.best_bid_usdt_per_btc.value, p.best_ask_usdt_per_btc.value) {
                (Some(b), Some(a)) => b > Decimal::ZERO && a > Decimal::ZERO && b <= a,
                _ => false,
            }
        };
        if !normal(l) || !normal(r) {
            return Err("endpoint_binance_book_not_normal");
        }
        let (Some(lk), Some(rk)) = (l.official_k_usd_per_btc, r.official_k_usd_per_btc) else {
            return Err("official_k_missing");
        };
        if l.official_anchor.direct_anchors.is_empty()
            || r.official_anchor.direct_anchors.is_empty()
        {
            return Err("official_anchor_identity_missing");
        }
        if lk != rk
            || l.official_anchor.value != r.official_anchor.value
            || l.official_anchor.direct_anchors != r.official_anchor.direct_anchors
        {
            return Err("official_anchor_changed");
        }
        if l.at_anchor || r.at_anchor {
            return Err("at_anchor");
        }
        if l.midpoint_usdt_per_btc.is_none()
            || r.midpoint_usdt_per_btc.is_none()
            || l.midpoint_usdt_per_btc != r.midpoint_usdt_per_btc
        {
            return Err("midpoint_changed_or_unknown");
        }
        if !matches!(l.side.as_deref(), Some("bid" | "ask")) || l.side != r.side {
            return Err("blb_side_changed_or_unknown");
        }
        let (Some(lq), Some(rq)) = (l.visible_quantity_btc, r.visible_quantity_btc) else {
            return Err("quantity_not_materialized");
        };
        rq.checked_sub(lq).ok_or("quantity_difference_overflow")
    })();
    match comparison {
        Ok(delta) => {
            d.delta_quantity_btc = Some(delta);
            d.blb_outcome = if delta < Decimal::ZERO {
                "veto_decreasing"
            } else {
                "non_decreasing"
            }
            .into();
        }
        Err(reason) => {
            d.blb_outcome = "incomparable".into();
            d.blb_incomparable_reason = Some(reason.into());
        }
    }
    d
}

/// 未到后置分支时整个字段为None；报价、最终价和请求id复用原诊断。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialPairLowPriceEvaluationV5 {
    Disabled,
    NotBelowBestBid,
    VetoBelowBestBid,
    UnknownComparison,
}
impl InitialPairLowPriceEvaluationV5 {
    pub fn allows_new_pair(&self) -> bool {
        *self != Self::VetoBelowBestBid
    }
}
pub(super) fn evaluate_low_price(
    prices: [Decimal; 2],
    c0: &PublicEntryEvaluationV4,
    enabled: bool,
) -> InitialPairLowPriceEvaluationV5 {
    if !enabled {
        return InitialPairLowPriceEvaluationV5::Disabled;
    }
    // 真簿报价来自刚通过的同冻结C0，不再查询行情；最终prices已完成旧价复用。
    let below = [0, 1].map(|i| {
        c0.pm_quotes
            .get(i)
            .and_then(|q| q[0])
            .map(|bid| prices[i] < bid)
    });
    if below.contains(&Some(true)) {
        InitialPairLowPriceEvaluationV5::VetoBelowBestBid
    } else if below.contains(&None) {
        InitialPairLowPriceEvaluationV5::UnknownComparison
    } else {
        InitialPairLowPriceEvaluationV5::NotBelowBestBid
    }
}
