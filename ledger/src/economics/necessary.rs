//! 同步消费者按需读取原数值；账户贡献归原经济索引维护。
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarketFundsNumbersV1 {
    pub principal: Decimal,
    pub fee: Decimal,
    pub local_principal: Decimal,
    pub local_fee: Decimal,
    pub platform_pending_principal: Decimal,
    pub platform_pending_fee: Decimal,
    pub pending_count: u64,
}
impl MarketFundsNumbersV1 {
    pub fn total(&self) -> R {
        R::from_decimal(self.principal)
            .add(&R::from_decimal(self.fee))
            .add(&R::from_decimal(self.local_principal))
            .add(&R::from_decimal(self.local_fee))
            .add(&R::from_decimal(self.platform_pending_principal))
            .add(&R::from_decimal(self.platform_pending_fee))
    }
    pub fn has_obligation(&self) -> bool {
        self.pending_count != 0 || !self.total().is_zero()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketEconomicNumbersV1 {
    pub assets: Option<[AssetIdentityV1; 2]>,
    pub unresolved: bool,
    pub orphan_shares: [Decimal; 2],
    pub orphan_principal: R,
    pub orphan_fee: ExecutionReadValueV1<R>,
    pub successful_investment: R,
    pub success_shares: [Decimal; 2],
    pub success_principal: [R; 2],
    pub actual_fees: [Decimal; 2],
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountEconomicNumbersV1 {
    pub confirmed_net_pnl: R,
    pub confirmed_unpaid_rebate: R,
    pub conservative_positions: R,
    pub committed_principal: R,
}
impl AccountEconomicNumbersV1 {
    fn adjust(&mut self, value: &Self, remove: bool) {
        let add = |a: &R, b: &R| if remove { a.sub(b) } else { a.add(b) };
        self.confirmed_net_pnl = add(&self.confirmed_net_pnl, &value.confirmed_net_pnl);
        self.confirmed_unpaid_rebate = add(
            &self.confirmed_unpaid_rebate,
            &value.confirmed_unpaid_rebate,
        );
        self.committed_principal = add(&self.committed_principal, &value.committed_principal);
        self.conservative_positions =
            add(&self.conservative_positions, &value.conservative_positions);
    }
}
#[derive(Default)]
pub(super) struct AccountIndex {
    contributions: BTreeMap<String, AccountEconomicNumbersV1>,
    pub totals: AccountEconomicNumbersV1,
    pub marked: BTreeSet<String>,
}
impl AccountIndex {
    pub fn from_state(state: &EconomicsState) -> Self {
        let mut index = Self::default();
        for id in state.markets.keys() {
            let pending = state
                .read_index
                .as_ref()
                .and_then(|i| i.markets.get(id))
                .map_or_else(R::zero, |t| t.pending_principal.clone());
            index.replace(id, state, pending);
        }
        index
    }
    pub fn replace(&mut self, id: &str, state: &EconomicsState, pending: R) {
        if let Some(old) = self.contributions.remove(id) {
            self.totals.adjust(&old, true);
        }
        self.marked.remove(id);
        let Some(m) = state.markets.get(id) else {
            return;
        };
        let unpaid = state
            .days
            .values()
            .fold(R::zero(), |s, d| s.add(&d.amounts(Some(id))[2]));
        let (pair, orphan) = m.receivables();
        let common = R::from_decimal(if m.resolution.is_none() {
            m.paired
        } else {
            Decimal::ZERO
        })
        .add(&R::from_decimal(pair))
        .add(&unpaid);
        let fee = R::from_decimal(m.settlement_recognized - m.settlement_paid);
        let taker = R::from_decimal(m.unpaid_taker);
        let value = AccountEconomicNumbersV1 {
            committed_principal: if m.resolution.as_ref().is_some_and(|r| {
                r.redeemed_pair > Decimal::ZERO
                    || r.redeemed_orphan.iter().any(|q| *q > Decimal::ZERO)
            }) {
                pending
            } else {
                pending.add(&m.success_cost[0]).add(&m.success_cost[1])
            },
            confirmed_net_pnl: m.pi.clone(),
            confirmed_unpaid_rebate: unpaid,
            conservative_positions: common.add(&R::from_decimal(orphan)).sub(&fee).sub(&taker),
        };
        if m.resolution.is_none() && m.pools.iter().any(|p| p.quantity > Decimal::ZERO) {
            self.marked.insert(id.into());
        }
        self.totals.adjust(&value, false);
        self.contributions.insert(id.into(), value);
    }
}
pub struct LedgerNecessaryReadV2<'a> {
    writer: &'a LedgerWriterV1,
}
impl LedgerWriterV1 {
    pub fn necessary_read_v2(&self) -> Option<LedgerNecessaryReadV2<'_>> {
        self.account.as_ref()?;
        Some(LedgerNecessaryReadV2 { writer: self })
    }
    pub(crate) fn necessary_market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1 {
        let id = &condition.0;
        let balances = self
            .account
            .as_ref()
            .and_then(|a| a.markets.get(id))
            .map(|m| &m.balances);
        let local = self.reservations.markets.get(id);
        let life = self.lifecycle.markets.get(id).map(|m| &m.totals);
        MarketFundsNumbersV1 {
            principal: balances.map_or(Decimal::ZERO, |b| b.principal_encumbrance_pusd)
                + life.map_or(Decimal::ZERO, |b| b.platform_pending_principal_pusd),
            fee: balances.map_or(Decimal::ZERO, |b| b.immediate_taker_fee_encumbrance_pusd)
                + life.map_or(Decimal::ZERO, |b| b.platform_pending_immediate_fee_pusd),
            local_principal: local.map_or(Decimal::ZERO, |b| b.principal_pusd),
            local_fee: local.map_or(Decimal::ZERO, |b| b.immediate_fee_pusd),
            platform_pending_principal: life
                .map_or(Decimal::ZERO, |b| b.platform_pending_principal_pusd),
            platform_pending_fee: life
                .map_or(Decimal::ZERO, |b| b.platform_pending_immediate_fee_pusd),
            pending_count: local.map_or(0, |b| b.before_send_count + b.after_send_unknown_count)
                + life.map_or(0, |b| {
                    b.platform_pending_economic_count + b.pending_fill_count + b.open_order_count
                }),
        }
    }
}
impl LedgerNecessaryReadV2<'_> {
    fn index(&self) -> &EconomicReadIndex {
        self.writer
            .economics
            .read_index
            .as_ref()
            .expect("新建或恢复的原经济索引")
    }
    fn account_index(&self) -> &AccountIndex {
        self.index()
            .account
            .get_or_init(|| AccountIndex::from_state(&self.writer.economics))
    }
    pub fn account(&self) -> &AccountEconomicNumbersV1 {
        &self.account_index().totals
    }
    pub fn cash(&self) -> (Decimal, Decimal) {
        self.writer.cash_balances().expect("已开户余额")
    }
    pub fn marked_markets(&self) -> impl Iterator<Item = &str> {
        self.account_index().marked.iter().map(String::as_str)
    }
    pub fn funds_markets(&self) -> impl Iterator<Item = &str> {
        self.writer
            .necessary_funds
            .get_or_init(|| {
                self.writer
                    .account
                    .iter()
                    .flat_map(|a| a.markets.keys())
                    .chain(self.writer.reservations.markets.keys())
                    .chain(self.writer.lifecycle.markets.keys())
                    .filter(|id| {
                        self.writer
                            .necessary_market_funds(&ConditionId((*id).clone()))
                            .has_obligation()
                    })
                    .cloned()
                    .collect()
            })
            .iter()
            .map(String::as_str)
    }
    pub fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1 {
        self.writer.necessary_market_funds(condition)
    }
    /// 读取原资产与池份额；调用方沿 marked_markets 选择未判定估值市场，不读取费用明细。
    pub fn marked_position(
        &self,
        id: &str,
    ) -> Option<(Option<&[AssetIdentityV1; 2]>, [Decimal; 2])> {
        let m = self.writer.economics.markets.get(id)?;
        Some((
            m.assets.as_ref(),
            std::array::from_fn(|i| m.pools[i].quantity),
        ))
    }
    pub fn market(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1> {
        let m = self.writer.economics.markets.get(&condition.0)?;
        let redeemed = m
            .resolution
            .as_ref()
            .map_or(Decimal::ZERO, |r| r.redeemed_pair);
        let orphan = m
            .resolution
            .as_ref()
            .map_or([Decimal::ZERO; 2], |r| r.redeemed_orphan);
        let fee = if let Some(f) = &m.orphan_fee {
            ExecutionReadValueV1::Known(f[0].add(&f[1]))
        } else {
            let mut fee = Some(R::zero());
            for (id, t) in self.writer.economics.trades.iter().filter(|(_, t)| {
                t.fact.condition_id == *condition && t.success_applied && t.economic_applied
            }) {
                let q = m.pool_contribution_shares.as_ref().map(|p| {
                    p.iter()
                        .find_map(|p| p.get(id))
                        .cloned()
                        .unwrap_or_default()
                });
                let original =
                    R::from_decimal(decimal(&t.fact.base_quantity_shares).expect("原份额"));
                match (&mut fee, q) {
                    (Some(sum), Some(q)) if !original.is_zero() => {
                        *sum = sum.add(&R::from_decimal(t.net_fee).mul(&q).divided_by(&original))
                    }
                    (_, None) => fee = None,
                    _ => {}
                }
            }
            fee.map(ExecutionReadValueV1::Known)
                .unwrap_or(ExecutionReadValueV1::Unavailable)
        };
        Some(MarketEconomicNumbersV1 {
            assets: m.assets.clone(),
            unresolved: m.resolution.is_none(),
            orphan_shares: std::array::from_fn(|i| m.pools[i].quantity - orphan[i]),
            orphan_principal: if m.resolution.is_none() {
                m.pools[0].cost.add(&m.pools[1].cost)
            } else {
                R::zero()
            },
            orphan_fee: fee,
            successful_investment: self
                .index()
                .markets
                .get(&condition.0)
                .map_or_else(R::zero, |t| t.investment.clone()),
            success_shares: std::array::from_fn(|i| m.success_quantity[i] - redeemed - orphan[i]),
            success_principal: std::array::from_fn(|i| {
                if redeemed > Decimal::ZERO || orphan[i] > Decimal::ZERO {
                    R::zero()
                } else {
                    m.success_cost[i].clone()
                }
            }),
            actual_fees: m.executed_fees,
        })
    }
}
