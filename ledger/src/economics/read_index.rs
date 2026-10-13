//! 原经济事实的增量读取索引；只存数量/金额与贡献身份，不复制成交载荷。
use super::*;
use std::collections::BTreeSet;

#[derive(Default, Serialize, Deserialize)]
pub(super) struct EconomicReadIndex {
    #[serde(skip)]
    pub account: std::sync::OnceLock<super::necessary::AccountIndex>,
    pub markets: BTreeMap<String, TradeTotals>,
    pub estimates: BTreeMap<(RebateKindV1, i64), BTreeMap<String, Estimate>>,
    pub contributed: BTreeSet<Identity>,
}
#[derive(Default, Serialize, Deserialize)]
pub(super) struct TradeTotals {
    pub final_success: BTreeMap<String, Decimal>,
    pub pending_principal: R,
    pub investment: R,
    pub unclosed: u64,
}
#[derive(Default, Serialize, Deserialize)]
pub(super) struct Estimate {
    pub amount: R,
    pub unknown: u64,
}
fn estimate(trade: &Trade) -> Option<Option<R>> {
    if matches!(
        trade.fact.economic_disposition,
        SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
            | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated
    ) {
        return None;
    }
    let fee = if role(trade.fact.liquidity_role) == 0 {
        let ExecutionReadValueV1::Known(fee) = &trade.maker_fee_equivalent else {
            return Some(None);
        };
        fee
    } else {
        &trade.fact.immediate_taker_fee_pusd
    };
    Some(Some(
        R::from_decimal(decimal(fee).expect("已验证费用")).mul(&R::from_decimal(
            if role(trade.fact.liquidity_role) == 0 {
                Decimal::new(2, 1)
            } else {
                Decimal::new(8, 2)
            },
        )),
    ))
}
impl EconomicReadIndex {
    pub fn from_state(state: &EconomicsState) -> Self {
        let mut index = Self::default();
        index.contributed.extend(state.days.values().flat_map(|d| {
            d.contributions
                .iter()
                .map(|(_, c)| c.trade_identity.clone())
        }));
        for (id, trade) in &state.trades {
            index.replace_trade(id, None, Some(trade));
        }
        index
    }
    fn adjust(&mut self, id: &Identity, t: &Trade, remove: bool) {
        let totals = self
            .markets
            .entry(t.fact.condition_id.0.clone())
            .or_default();
        let sign = if remove { -Decimal::ONE } else { Decimal::ONE };
        let amount = |v: &ExactDecimal| R::from_decimal(decimal(v).expect("已验证成交金额") * sign);
        if t.economic_applied && !t.success_applied {
            totals.pending_principal = totals
                .pending_principal
                .add(&amount(&t.fact.quote_amount_pusd));
        }
        if t.success_applied && t.economic_applied {
            totals.investment = totals
                .investment
                .add(&amount(&t.fact.quote_amount_pusd))
                .add(&R::from_decimal(t.net_fee * sign));
        }
        if t.fact.economic_disposition == SourceNeutralTradeEconomicDispositionV1::FinalSuccess {
            *totals
                .final_success
                .entry(t.fact.token_id.0.clone())
                .or_default() += decimal(&t.fact.base_quantity_shares).expect("原份额") * sign;
        }
        let unclosed = match t.fact.economic_disposition {
            SourceNeutralTradeEconomicDispositionV1::FinalSuccess => !t.success_applied,
            SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
            | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated => {
                t.economic_applied
            }
            _ => true,
        };
        if unclosed {
            if remove {
                totals.unclosed -= 1;
            } else {
                totals.unclosed += 1;
            }
        }
        self.adjust_estimate(id, t, remove);
    }
    fn adjust_estimate(&mut self, id: &Identity, t: &Trade, remove: bool) {
        let kind = if role(t.fact.liquidity_role) == 0 {
            RebateKindV1::Maker
        } else {
            RebateKindV1::Taker
        };
        let day = t.fact.matched_at_utc_ms.div_euclid(86_400_000) * 86_400_000;
        let markets = self.estimates.entry((kind, day)).or_default();
        if self.contributed.contains(id) {
            return;
        }
        let Some(value) = estimate(t) else {
            return;
        };
        let sum = markets.entry(t.fact.condition_id.0.clone()).or_default();
        match value {
            Some(value) => {
                sum.amount = if remove {
                    sum.amount.sub(&value)
                } else {
                    sum.amount.add(&value)
                };
            }
            None => {
                if remove {
                    sum.unknown -= 1;
                } else {
                    sum.unknown += 1;
                }
            }
        }
    }
    pub fn replace_trade(&mut self, id: &Identity, old: Option<&Trade>, new: Option<&Trade>) {
        if let Some(old) = old {
            self.adjust(id, old, true);
        }
        if let Some(new) = new {
            self.adjust(id, new, false);
        }
    }
    pub fn mark_contributed(&mut self, id: &Identity, trade: Option<&Trade>) {
        if self.contributed.contains(id) {
            return;
        }
        if let Some(trade) = trade {
            self.adjust_estimate(id, trade, true);
        }
        self.contributed.insert(id.clone());
    }
    pub fn estimated(
        &self,
        kind: RebateKindV1,
        condition: Option<&str>,
        day: Option<i64>,
    ) -> ExecutionReadValueV1<ExactRationalV1> {
        let mut amount = R::zero();
        for ((k, d), markets) in &self.estimates {
            if *k != kind || day.is_some_and(|day| day != *d) {
                continue;
            }
            if let Some(condition) = condition {
                if let Some(sum) = markets.get(condition) {
                    if sum.unknown != 0 {
                        return ExecutionReadValueV1::Unavailable;
                    }
                    amount = amount.add(&sum.amount);
                }
            } else {
                for sum in markets.values() {
                    if sum.unknown != 0 {
                        return ExecutionReadValueV1::Unavailable;
                    }
                    amount = amount.add(&sum.amount);
                }
            }
        }
        ExecutionReadValueV1::Known(amount.public())
    }
}
