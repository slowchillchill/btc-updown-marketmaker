//! BLB 途中区间的纯研究量。正式锚点按原数值投影到 USDT，不作换汇。
use contracts::{MarketBusinessObservationV1, ReferencePriceLevelV1};
use rust_decimal::Decimal;
use serde::Serialize;

pub const BLB_RESEARCH_VERSION_V1: &str = "blb-visible-interval-v1";
#[derive(Clone, Debug, Serialize)]
pub struct BlbResearchV1 {
    pub definition_version: &'static str,
    pub status: &'static str,
    pub unknown_reason: Option<&'static str>,
    pub midpoint_usdt_per_btc: Option<Decimal>,
    pub official_k_usd_per_btc: Option<Decimal>,
    pub distance: Option<Decimal>,
    pub side: Option<&'static str>,
    pub at_anchor: bool,
    pub visible_positive_levels: Vec<ReferencePriceLevelV1>,
    pub visible_level_count: Option<usize>,
    pub visible_quantity_btc: Option<Decimal>,
    pub visible_notional_usdt: Option<Decimal>,
    /// 维护档位没有区间完整性证明；零可见量也不等于交易所零量。
    pub interval_coverage: &'static str,
}
pub fn blb_research_v1(observation: &MarketBusinessObservationV1) -> BlbResearchV1 {
    let mut out = BlbResearchV1 {
        definition_version: BLB_RESEARCH_VERSION_V1,
        status: "unknown",
        unknown_reason: None,
        midpoint_usdt_per_btc: None,
        official_k_usd_per_btc: None,
        distance: None,
        side: None,
        at_anchor: false,
        visible_positive_levels: vec![],
        visible_level_count: None,
        visible_quantity_btc: None,
        visible_notional_usdt: None,
        interval_coverage: "unknown_no_coverage_evidence",
    };
    let result = (|| -> Result<(), &'static str> {
        // 正式 K 缺失时仍尽量交付 M；不以 effective_reference 代替正式 K。
        out.official_k_usd_per_btc = observation
            .official_anchor
            .value
            .as_ref()
            .map(|a| a.price_usd_per_btc.0.parse())
            .transpose()
            .map_err(|_| "official_anchor_decimal_unrepresentable")?;
        let depth = observation
            .binance_depth
            .value
            .as_ref()
            .ok_or("binance_depth_not_observed")?
            .full()
            .ok_or("binance_depth_quantities_not_materialized")?;
        let parse =
            |levels: &[ReferencePriceLevelV1]| -> Result<Vec<(Decimal, Decimal)>, &'static str> {
                levels
                    .iter()
                    .map(|l| {
                        Ok((
                            l.price_usdt_per_btc
                                .0
                                .parse()
                                .map_err(|_| "depth_decimal_unrepresentable")?,
                            l.quantity_btc
                                .0
                                .parse()
                                .map_err(|_| "depth_decimal_unrepresentable")?,
                        ))
                    })
                    .collect()
            };
        let bids = parse(&depth.bids)?;
        let asks = parse(&depth.asks)?;
        let best_bid = bids
            .iter()
            .filter(|(p, q)| *p > Decimal::ZERO && *q > Decimal::ZERO)
            .map(|(p, _)| *p)
            .max()
            .ok_or("positive_bid_missing")?;
        let best_ask = asks
            .iter()
            .filter(|(p, q)| *p > Decimal::ZERO && *q > Decimal::ZERO)
            .map(|(p, _)| *p)
            .min()
            .ok_or("positive_ask_missing")?;
        let m = best_bid
            .checked_add(best_ask)
            .and_then(|v| v.checked_div(Decimal::TWO))
            .ok_or("decimal_arithmetic_overflow")?;
        out.midpoint_usdt_per_btc = Some(m);
        let k = out
            .official_k_usd_per_btc
            .ok_or("official_anchor_not_observed")?;
        out.distance = Some(m.checked_sub(k).ok_or("decimal_arithmetic_overflow")?.abs());
        if m == k {
            out.at_anchor = true;
            out.status = "at_anchor";
            return Ok(());
        }
        let (side, levels, parsed) = if m > k {
            ("bid", &depth.bids, &bids)
        } else {
            ("ask", &depth.asks, &asks)
        };
        out.side = Some(side);
        let mut q = Decimal::ZERO;
        let mut v = Decimal::ZERO;
        for (level, (p, size)) in levels.iter().zip(parsed) {
            if *size > Decimal::ZERO && *p >= m.min(k) && *p <= m.max(k) {
                q = q.checked_add(*size).ok_or("decimal_arithmetic_overflow")?;
                v = v
                    .checked_add(p.checked_mul(*size).ok_or("decimal_arithmetic_overflow")?)
                    .ok_or("decimal_arithmetic_overflow")?;
                out.visible_positive_levels.push(level.clone());
            }
        }
        out.visible_level_count = Some(out.visible_positive_levels.len());
        out.visible_quantity_btc = Some(q);
        out.visible_notional_usdt = Some(v);
        out.status = "visible_interval_only";
        Ok(())
    })();
    if let Err(reason) = result {
        out.status = "unknown";
        out.unknown_reason = Some(reason);
        out.visible_positive_levels.clear();
        out.visible_level_count = None;
        out.visible_quantity_btc = None;
        out.visible_notional_usdt = None;
    }
    out
}
#[cfg(test)]
#[path = "blb_research_tests.rs"]
mod tests;
