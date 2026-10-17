//! 原 Ledger 事务中的精确经济归属索引；NAV 由 shared-run 只读组合。
use super::*;
use contracts::economics::*;
use contracts::execution_normalization::*;
use contracts::{AssetIdentityV1, ExecutionReadValueV1, TradeLiquidityRoleV2};
use execution_normalization::{
    MemoryLogicalIdentityV1 as Identity, NormalizationRecord, SourceNeutralEvidenceKind,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

type R = EconomicRational;
#[path = "economics/read_index.rs"]
mod read_index;
use read_index::EconomicReadIndex;
#[path = "economics/necessary.rs"]
mod necessary;
pub use necessary::*;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Pool {
    pub quantity: Decimal,
    pub cost: R,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub fact: SourceNeutralTradeLifecycleResultV2<Identity>,
    pub maker_fee_equivalent: ExecutionReadValueV1<ExactDecimal>,
    pub economic_applied: bool,
    pub success_applied: bool,
    pub net_fee: Decimal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Resolution {
    pub fact: ResolvedPositionFactV1<Identity>,
    pub category: SettlementCostAttributionV1,
    pub request: Option<SettlementRequestFactV1<Identity>>,
    pub redeemed_pair: Decimal,
    pub redeemed_orphan: [Decimal; 2],
    // 原自动结算生命周期事实，包括没有账户 delta 的零余额终态。
    #[serde(default)]
    pub auto_redeemed: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Market {
    pub assets: Option<[AssetIdentityV1; 2]>,
    pub maker_accrual_finalized: bool,
    pub pools: [Pool; 2],
    pub paired: Decimal,
    pub paired_cost: [R; 2],
    // 只读成本解释的原成交贡献；旧持久补丁无此索引时保留未知，不猜测历史次序。
    #[serde(default)]
    pub pool_contribution_shares: Option<[BTreeMap<Identity, R>; 2]>,
    #[serde(default)]
    pub orphan_fee: Option<[R; 2]>,
    pub success_quantity: [Decimal; 2],
    pub success_cost: [R; 2],
    pub executed_quantity: [Decimal; 2],
    pub executed_principal: [Decimal; 2],
    pub executed_fees: [Decimal; 2],
    pub resolution: Option<Resolution>,
    pub unpaid_taker: Decimal,
    pub settlement_recognized: Decimal,
    pub settlement_paid: Decimal,
    pub confirmed_rebate: R,
    pub pi: R,
    pub maximum_loss: R,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RebateDay {
    pub kind: RebateKindV1,
    pub day: i64,
    pub contributions: Vec<(Identity, RebateContributionFactV1<Identity>)>,
    pub result: Option<(Identity, RebateDayResultV1<Identity>)>,
    pub paid: Decimal,
    #[serde(default)]
    pub contribution_totals: Option<BTreeMap<String, R>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Cost {
    pub source_order: execution_normalization::SourceOrderV1,
    pub fact: SettlementCostFactV1<Identity>,
    pub category: SettlementCostAttributionV1,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CashAttribution {
    pub directional_cash: Decimal,
    pub other_cash: Decimal,
    pub unpaid_taker: Decimal,
    pub unpaid_settlement: Decimal,
}
#[derive(Serialize)]
pub(crate) struct EconomicsState {
    pub external_cash: BTreeMap<Identity, bool>,
    pub cash: CashAttribution,
    pub markets: BTreeMap<String, Market>,
    pub trades: BTreeMap<Identity, Trade>,
    pub days: BTreeMap<(RebateKindV1, i64), RebateDay>,
    pub costs: BTreeMap<Identity, Cost>,
    // 经济效果可能先于首次 trade 详情公开；只保留尚待关联的原身份。
    pub unapportioned_trades: BTreeMap<Identity, (bool, Decimal)>,
    #[serde(default)]
    read_index: Option<EconomicReadIndex>,
}
impl<'de> Deserialize<'de> for EconomicsState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Facts {
            external_cash: BTreeMap<Identity, bool>,
            cash: CashAttribution,
            markets: BTreeMap<String, Market>,
            trades: BTreeMap<Identity, Trade>,
            days: BTreeMap<(RebateKindV1, i64), RebateDay>,
            costs: BTreeMap<Identity, Cost>,
            unapportioned_trades: BTreeMap<Identity, (bool, Decimal)>,
        }
        let facts = Facts::deserialize(d)?;
        let mut state = Self {
            external_cash: facts.external_cash,
            cash: facts.cash,
            markets: facts.markets,
            trades: facts.trades,
            days: facts.days,
            costs: facts.costs,
            unapportioned_trades: facts.unapportioned_trades,
            read_index: None,
        };
        state.read_index = Some(EconomicReadIndex::from_state(&state));
        Ok(state)
    }
}
impl Default for EconomicsState {
    fn default() -> Self {
        Self {
            external_cash: Default::default(),
            cash: Default::default(),
            markets: Default::default(),
            trades: Default::default(),
            days: Default::default(),
            costs: Default::default(),
            unapportioned_trades: Default::default(),
            read_index: Some(EconomicReadIndex::default()),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct EconomicsPatch {
    pub external_cash: Vec<(Identity, bool)>,
    pub cash: Option<CashAttribution>,
    pub markets: Vec<(String, Market)>,
    pub trades: Vec<(Identity, Trade)>,
    #[serde(alias = "days")]
    pub day_updates: Vec<((RebateKindV1, i64), RebateDayUpdate)>,
    pub costs: Vec<(Identity, Cost)>,
    pub unapportioned_trades: Vec<(Identity, Option<(bool, Decimal)>)>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RebateDayUpdate {
    Contributions {
        additions: Vec<(Identity, RebateContributionFactV1<Identity>)>,
    },
    Finalized {
        result: (Identity, RebateDayResultV1<Identity>),
    },
    Paid {
        paid: Decimal,
    },
    // 旧持久补丁保存完整日状态；冷恢复直接移动替换，不把历史误作新增贡献。
    #[serde(untagged)]
    Existing(RebateDay),
}

fn decimal(value: &ExactDecimal) -> Result<Decimal, LedgerErrorV1> {
    Decimal::from_str_exact(&value.0).map_err(|_| LedgerErrorV1::DecimalParse {
        field: "economic attribution",
        value: value.0.clone(),
    })
}
fn exact(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}
fn role(value: TradeLiquidityRoleV2) -> usize {
    match value {
        TradeLiquidityRoleV2::MakerInferred => 0,
        TradeLiquidityRoleV2::Taker => 1,
    }
}
fn add(a: Decimal, b: Decimal) -> Result<Decimal, LedgerErrorV1> {
    a.checked_add(b).ok_or(LedgerErrorV1::ArithmeticOverflow {
        field: "economic attribution",
    })
}
impl Market {
    fn payout(&self, leg: usize) -> Result<Decimal, LedgerErrorV1> {
        let Some(resolution) = &self.resolution else {
            return Ok(Decimal::ZERO);
        };
        let contracts::MarketResolutionTerminalV3::Resolved { payout_vector, .. } =
            &resolution.fact.payout_vector;
        let Some(assets) = &self.assets else {
            return Err(LedgerErrorV1::NonEconomicRecord);
        };
        payout_vector
            .iter()
            .find(|p| p.asset.token_id == assets[leg].token_id)
            .map(|p| decimal(&p.payout_pusd_per_share))
            .transpose()?
            .ok_or(LedgerErrorV1::NonEconomicRecord)
    }
    fn refresh_pi(&mut self) -> Result<(), LedgerErrorV1> {
        let mut pi = R::from_decimal(self.paired)
            .sub(&self.paired_cost[0])
            .sub(&self.paired_cost[1]);
        if self.resolution.is_some() {
            for leg in 0..2 {
                pi = pi
                    .add(
                        &R::from_decimal(self.pools[leg].quantity)
                            .mul(&R::from_decimal(self.payout(leg)?)),
                    )
                    .sub(&self.pools[leg].cost);
            }
        }
        pi = pi
            .add(&self.confirmed_rebate)
            .sub(&R::from_decimal(self.executed_fees[0]))
            .sub(&R::from_decimal(self.executed_fees[1]))
            .sub(&R::from_decimal(self.settlement_recognized));
        self.maximum_loss = self.maximum_loss.clone().max(R::zero().sub(&pi));
        self.pi = pi;
        Ok(())
    }
    fn apply_success(&mut self, trade: &Trade) -> Result<(), LedgerErrorV1> {
        let assets = self
            .assets
            .as_ref()
            .ok_or(LedgerErrorV1::NonEconomicRecord)?;
        let leg = assets
            .iter()
            .position(|a| a.token_id == trade.fact.token_id)
            .ok_or(LedgerErrorV1::NonEconomicRecord)?;
        let q = decimal(&trade.fact.base_quantity_shares)?;
        let c = R::from_decimal(decimal(&trade.fact.quote_amount_pusd)?);
        if self.pool_contribution_shares.is_none()
            && self.success_quantity.iter().all(Decimal::is_zero)
        {
            self.pool_contribution_shares = Some(std::array::from_fn(|_| BTreeMap::new()));
            self.orphan_fee = Some(std::array::from_fn(|_| R::zero()));
        }
        if let Some(contributions) = &mut self.pool_contribution_shares {
            contributions[leg].insert(trade.fact.trade_identity.clone(), R::from_decimal(q));
        }
        self.success_quantity[leg] = add(self.success_quantity[leg], q)?;
        self.success_cost[leg] = self.success_cost[leg].add(&c);
        if let Some(fees) = &mut self.orphan_fee {
            fees[leg] = fees[leg].add(&R::from_decimal(trade.net_fee));
        }
        let pool = &mut self.pools[leg];
        pool.quantity = add(pool.quantity, q)?;
        pool.cost = pool.cost.add(&c);
        let quantity = self.pools[0].quantity.min(self.pools[1].quantity);
        if quantity > Decimal::ZERO {
            let costs: [R; 2] = std::array::from_fn(|i| {
                R::from_decimal(quantity)
                    .mul(&self.pools[i].cost)
                    .divided_by(&R::from_decimal(self.pools[i].quantity))
            });
            self.paired = add(self.paired, quantity)?;
            for (i, cost) in costs.into_iter().enumerate() {
                if let Some(contributions) = &mut self.pool_contribution_shares {
                    let remaining_fraction = R::from_decimal(self.pools[i].quantity - quantity)
                        .divided_by(&R::from_decimal(self.pools[i].quantity));
                    contributions[i].retain(|_, shares| {
                        *shares = shares.mul(&remaining_fraction);
                        !shares.is_zero()
                    });
                }
                self.paired_cost[i] = self.paired_cost[i].add(&cost);
                if let Some(fees) = &mut self.orphan_fee {
                    fees[i] = fees[i].mul(
                        &R::from_decimal(self.pools[i].quantity - quantity)
                            .divided_by(&R::from_decimal(self.pools[i].quantity)),
                    );
                }
                self.pools[i].quantity -= quantity;
                self.pools[i].cost = self.pools[i].cost.sub(&cost);
            }
        }
        Ok(())
    }
}
impl EconomicsState {
    // 原结算读取与 Auto 归属共用本地已应用的经济前驱；平台发生与反馈可见分别推进。
    fn economic_predecessors_closed(&self, condition: &str) -> bool {
        if let Some(index) = &self.read_index {
            return self
                .markets
                .get(condition)
                .is_some_and(|m| m.maker_accrual_finalized)
                && index.markets.get(condition).is_none_or(|m| m.unclosed == 0);
        }
        self.markets
            .get(condition)
            .is_some_and(|market| market.maker_accrual_finalized)
            && !self.trades.values().any(|trade| {
                trade.fact.condition_id.0 == condition
                    && match trade.fact.economic_disposition {
                        SourceNeutralTradeEconomicDispositionV1::FinalSuccess => {
                            !trade.success_applied
                        }
                        SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
                        | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated => {
                            trade.economic_applied
                        }
                        _ => true,
                    }
            })
    }
    pub(crate) fn dependencies_present<R: Clone + std::fmt::Debug>(
        &self,
        record: &execution_normalization::NoeConsumedRecordRef<R>,
    ) -> bool {
        let resolution = |condition: &ConditionId| {
            self.markets
                .get(&condition.0)
                .is_some_and(|m| m.resolution.is_some())
        };
        let auto_resolution = |condition: &ConditionId, identity: &Identity| {
            self.markets
                .get(&condition.0)
                .and_then(|market| market.resolution.as_ref())
                .is_some_and(|resolution| resolution.fact.resolution_identity == *identity)
                && self.economic_predecessors_closed(&condition.0)
        };
        let contribution_ready = |c: &RebateContributionFactV1<Identity>| {
            self.trades
                .get(&c.trade_identity)
                .is_some_and(|t| t.success_applied)
        };
        match record.record() {
            // Auto 与 Resolved 的反馈按真实接收时刻领取，Auto 可先到。
            // 保留原领取引用，等待判定与经济成功池齐备，不以先到的 Auto 读取未完成配对。
            NormalizationRecord::Evidence {
                evidence:
                    SourceNeutralEvidenceKind::SettlementLifecycle {
                        result:
                            SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
                                condition_id,
                                resolution_identity,
                                ..
                            },
                    },
                ..
            } => auto_resolution(condition_id, resolution_identity),
            NormalizationRecord::EconomicFact { fact, .. } => match &fact.disposition {
                SourceNeutralEconomicDispositionV1::AccountTransition {
                    condition_id: Some(condition),
                    transition:
                        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
                            resolution_identity,
                        },
                    ..
                } => auto_resolution(condition, resolution_identity),
                _ => true,
            },
            NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::SettlementLifecycleV2 { result },
                ..
            } => match result {
                // 原 Taker 贡献已是独立事实；不以用户成交成功反馈的到达先后作为接纳条件。
                SourceNeutralSettlementLifecycleResultV2::Contribution(_) => true,
                SourceNeutralSettlementLifecycleResultV2::MakerAccrualFinalized {
                    contributions,
                    ..
                } => contributions.iter().all(contribution_ready),
                SourceNeutralSettlementLifecycleResultV2::DayFinalized(day) => {
                    day.contribution_refs.iter().all(|id| {
                        self.days
                            .get(&(day.kind, day.attribution_day_start_utc_ms))
                            .is_some_and(|d| {
                                d.contributions.iter().any(|(_, c)| &c.trade_identity == id)
                            })
                    })
                }
                SourceNeutralSettlementLifecycleResultV2::Payment(p) => self
                    .days
                    .get(&(p.kind, p.attribution_day_start_utc_ms))
                    .is_some_and(|d| d.result.is_some()),
                SourceNeutralSettlementLifecycleResultV2::Request(r) => {
                    resolution(&r.settlement_request_id.condition_id)
                }
                SourceNeutralSettlementLifecycleResultV2::Cost(c) => {
                    resolution(&c.settlement_request_id.condition_id)
                        && c.correction_of
                            .as_ref()
                            .is_none_or(|id| self.costs.contains_key(id))
                }
                SourceNeutralSettlementLifecycleResultV2::ExternalCash(c) => c
                    .correction_of
                    .as_ref()
                    .is_none_or(|id| self.external_cash.contains_key(id)),
                _ => true,
            },
            NormalizationRecord::EconomicFactV2 { fact, .. } => match &fact.disposition {
                SourceNeutralEconomicDispositionV2::AccountTransition {
                    transition,
                    condition_id,
                    ..
                } => match transition {
                    SourceNeutralAccountTransitionKindV2::Existing(
                        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
                            resolution_identity,
                        },
                    ) => condition_id
                        .as_ref()
                        .is_none_or(|condition| auto_resolution(condition, resolution_identity)),
                    SourceNeutralAccountTransitionKindV2::ExternalCashApplied { cash_identity } => {
                        self.external_cash.contains_key(cash_identity)
                    }
                    SourceNeutralAccountTransitionKindV2::ResolutionRedeemApplied {
                        settlement_request_id,
                        ..
                    }
                    | SourceNeutralAccountTransitionKindV2::SettlementCostRecognized {
                        settlement_request_id,
                        ..
                    }
                    | SourceNeutralAccountTransitionKindV2::SettlementCostPaid {
                        settlement_request_id,
                        ..
                    } => resolution(&settlement_request_id.condition_id),
                    _ => true,
                },
                _ => true,
            },
            _ => true,
        }
    }
    pub(crate) fn prepare(
        &self,
        effect: &LedgerCommittedEffectV1,
    ) -> Result<Option<EconomicsPatch>, LedgerErrorV1> {
        match &effect.transaction.source {
            LedgerTransactionSourceV1::Memory(source) => self.prepare_record(source),
            LedgerTransactionSourceV1::MemoryV2(source) => self.prepare_record(source),
            _ => Ok(None),
        }
    }
    fn prepare_record<R: Clone + std::fmt::Debug + Eq>(
        &self,
        source: &execution_normalization::NoeConsumedRecordRef<R>,
    ) -> Result<Option<EconomicsPatch>, LedgerErrorV1> {
        let mut patch = EconomicsPatch::default();
        match source.record() {
            NormalizationRecord::EconomicFact { fact, .. } => {
                self.transition(&fact.disposition.clone().into(), &mut patch)?
            }
            NormalizationRecord::EconomicFactV2 { fact, .. } => {
                self.transition(&fact.disposition, &mut patch)?
            }
            NormalizationRecord::Evidence {
                evidence,
                logical_identity,
                ..
            } => match evidence {
                SourceNeutralEvidenceKind::InitialMarket { result } => {
                    let mut market = self
                        .markets
                        .get(&result.market.condition_id.0)
                        .cloned()
                        .unwrap_or_default();
                    market.assets = Some(result.assets.clone());
                    patch
                        .markets
                        .push((result.market.condition_id.0.clone(), market));
                }
                SourceNeutralEvidenceKind::MarketEnrollment { result } => {
                    let mut market = self
                        .markets
                        .get(&result.market.condition_id.0)
                        .cloned()
                        .unwrap_or_default();
                    market.assets = Some(result.assets.clone());
                    patch
                        .markets
                        .push((result.market.condition_id.0.clone(), market));
                }
                SourceNeutralEvidenceKind::TradeLifecycle { result } => {
                    self.trade(result, ExecutionReadValueV1::Unavailable, &mut patch)?
                }
                SourceNeutralEvidenceKind::TradeLifecycleV3 { result } => self.trade(
                    &result.result,
                    result.maker_fee_equivalent_pusd.clone(),
                    &mut patch,
                )?,
                SourceNeutralEvidenceKind::SettlementLifecycle {
                    result:
                        SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
                            condition_id,
                            resolution_identity,
                            ..
                        },
                } => {
                    // 原 V1 记录可没有 V2 判定索引；只更新已关联的正式判定。
                    if let Some(mut market) = self.markets.get(&condition_id.0).cloned()
                        && let Some(resolution) = &mut market.resolution
                        && resolution.fact.resolution_identity == *resolution_identity
                    {
                        resolution.auto_redeemed = true;
                        patch.markets.push((condition_id.0.clone(), market));
                    }
                }
                SourceNeutralEvidenceKind::SettlementLifecycleV2 { result } => self.settlement(
                    logical_identity,
                    source.record().source_order(),
                    result,
                    &mut patch,
                )?,
                _ => {}
            },
            _ => {}
        }
        for (_, market) in &mut patch.markets {
            market.refresh_pi()?;
        }
        Ok(Some(patch))
    }
    fn trade(
        &self,
        fact: &SourceNeutralTradeLifecycleResultV2<Identity>,
        equivalent: ExecutionReadValueV1<ExactDecimal>,
        patch: &mut EconomicsPatch,
    ) -> Result<(), LedgerErrorV1> {
        let old = self.trades.get(&fact.trade_identity);
        let mut trade = old.cloned().unwrap_or_else(|| Trade {
            fact: fact.clone(),
            maker_fee_equivalent: equivalent.clone(),
            economic_applied: false,
            success_applied: false,
            net_fee: Decimal::ZERO,
        });
        // 终局观察与经济应用分开：即使经济前驱还未到，旧 Pending 也不能覆盖成功终局。
        let terminal = |d: &SourceNeutralTradeEconomicDispositionV1| {
            matches!(
                d,
                SourceNeutralTradeEconomicDispositionV1::FinalSuccess
                    | SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
                    | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated
            )
        };
        if !terminal(&trade.fact.economic_disposition) || terminal(&fact.economic_disposition) {
            trade.fact = fact.clone();
        }
        if !matches!(equivalent, ExecutionReadValueV1::Unavailable) {
            trade.maker_fee_equivalent = equivalent;
        }
        if let Some((applied, fee)) = self.unapportioned_trades.get(&fact.trade_identity) {
            trade.economic_applied = *applied;
            trade.net_fee = if *applied {
                decimal(&fact.immediate_taker_fee_pusd)?
            } else {
                *fee
            };
            patch
                .unapportioned_trades
                .push((fact.trade_identity.clone(), None));
        }
        let mut market = self
            .markets
            .get(&fact.condition_id.0)
            .cloned()
            .unwrap_or_default();
        let r = role(fact.liquidity_role);
        if old.is_none() {
            market.executed_quantity[r] = add(
                market.executed_quantity[r],
                decimal(&fact.base_quantity_shares)?,
            )?;
            market.executed_principal[r] = add(
                market.executed_principal[r],
                decimal(&fact.quote_amount_pusd)?,
            )?;
            market.executed_fees[r] = add(market.executed_fees[r], trade.net_fee)?;
        }
        Self::success(&mut market, &mut trade)?;
        patch.markets.push((fact.condition_id.0.clone(), market));
        patch.trades.push((fact.trade_identity.clone(), trade));
        Ok(())
    }
    fn success(market: &mut Market, trade: &mut Trade) -> Result<(), LedgerErrorV1> {
        if trade.economic_applied
            && !trade.success_applied
            && trade.fact.economic_disposition
                == SourceNeutralTradeEconomicDispositionV1::FinalSuccess
        {
            market.apply_success(trade)?;
            trade.success_applied = true;
        }
        Ok(())
    }
    fn transition(
        &self,
        disposition: &SourceNeutralEconomicDispositionV2<Identity>,
        patch: &mut EconomicsPatch,
    ) -> Result<(), LedgerErrorV1> {
        let mut cash = self.cash.clone();
        match disposition {
            SourceNeutralEconomicDispositionV2::AccountOpening { .. } => {}
            SourceNeutralEconomicDispositionV2::AccountTransition {
                transition,
                balance_delta,
                condition_id,
                ..
            } => {
                if let Some(condition) = condition_id {
                    let amount = decimal(&balance_delta.actual_taker_fee_liability_pusd_delta)?;
                    if !amount.is_zero() {
                        let mut market =
                            self.markets.get(&condition.0).cloned().unwrap_or_default();
                        market.unpaid_taker = add(market.unpaid_taker, amount)?;
                        patch.markets.push((condition.0.clone(), market));
                    }
                }
                let delta = decimal(&balance_delta.existing.cash_pusd_delta)?;
                cash.unpaid_taker = add(
                    cash.unpaid_taker,
                    decimal(&balance_delta.actual_taker_fee_liability_pusd_delta)?,
                )?;
                cash.unpaid_settlement = add(
                    cash.unpaid_settlement,
                    decimal(&balance_delta.settlement_fee_liability_pusd_delta)?,
                )?;
                match transition {
                    SourceNeutralAccountTransitionKindV2::Existing(t) => {
                        let auto_market = match t {
                            SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
                                resolution_identity,
                            } => condition_id.as_ref().and_then(|condition| {
                                let market = self.markets.get(&condition.0)?;
                                (market.resolution.as_ref()?.fact.resolution_identity
                                    == *resolution_identity)
                                    .then(|| (condition, market.clone()))
                            }),
                            _ => None,
                        };
                        if let Some((condition, mut market)) = auto_market {
                            let resolution = market.resolution.as_mut().expect("原判定身份已关联");
                            let paired = market.paired - resolution.redeemed_pair;
                            cash.directional_cash = add(cash.directional_cash, delta - paired)?;
                            resolution.redeemed_pair = market.paired;
                            resolution.redeemed_orphan =
                                [market.pools[0].quantity, market.pools[1].quantity];
                            patch.markets.push((condition.0.clone(), market));
                        }
                        match t {
                            SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied { trade_identity, .. }
                            | SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied { trade_identity, .. } => {
                                let applied = matches!(t, SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied { .. });
                                if let Some(existing) = self.trades.get(trade_identity) {
                                    let mut trade = existing.clone();
                                    trade.economic_applied = applied;
                                    let fee = if applied { decimal(&trade.fact.immediate_taker_fee_pusd)? } else { Decimal::ZERO };
                                    let mut market = self.markets.get(&trade.fact.condition_id.0).cloned().unwrap_or_default();
                                    let r = role(trade.fact.liquidity_role);
                                    market.executed_fees[r] = add(market.executed_fees[r], fee - trade.net_fee)?;
                                    trade.net_fee = fee;
                                    Self::success(&mut market, &mut trade)?;
                                    patch.markets.push((trade.fact.condition_id.0.clone(), market));
                                    patch.trades.push((trade_identity.clone(), trade));
                                } else {
                                    // 原量价详情到达后才能拆费用；经济已应用标志本身准确保存。
                                    patch.unapportioned_trades.push((trade_identity.clone(), Some((applied, Decimal::ZERO))));
                                }
                            }
                            _ => {},
                        }
                    }
                    SourceNeutralAccountTransitionKindV2::SettlementCostPaid {
                        settlement_request_id,
                        ..
                    } => {
                        let market = self
                            .markets
                            .get(&settlement_request_id.condition_id.0)
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                        match market
                            .resolution
                            .as_ref()
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?
                            .category
                        {
                            SettlementCostAttributionV1::Maker => {}
                            SettlementCostAttributionV1::Directional => {
                                cash.directional_cash = add(cash.directional_cash, delta)?
                            }
                        }
                    }
                    SourceNeutralAccountTransitionKindV2::ResolutionRedeemApplied {
                        settlement_request_id,
                        ..
                    } => {
                        let mut market = self
                            .markets
                            .get(&settlement_request_id.condition_id.0)
                            .cloned()
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                        let resolution = market
                            .resolution
                            .as_mut()
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                        let paired = market.paired - resolution.redeemed_pair;
                        cash.directional_cash = add(cash.directional_cash, delta - paired)?;
                        resolution.redeemed_pair = market.paired;
                        resolution.redeemed_orphan =
                            [market.pools[0].quantity, market.pools[1].quantity];
                        patch
                            .markets
                            .push((settlement_request_id.condition_id.0.clone(), market));
                    }
                    SourceNeutralAccountTransitionKindV2::SettlementCostRecognized { .. } => {}
                    SourceNeutralAccountTransitionKindV2::ExternalCashApplied { cash_identity } => {
                        if !*self
                            .external_cash
                            .get(cash_identity)
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?
                        {
                            cash.other_cash = add(cash.other_cash, delta)?;
                        }
                    }
                }
            }
        }
        patch.cash = Some(cash);
        Ok(())
    }
    fn settlement(
        &self,
        identity: &Identity,
        source_order: &execution_normalization::SourceOrderV1,
        fact: &SourceNeutralSettlementLifecycleResultV2<Identity>,
        patch: &mut EconomicsPatch,
    ) -> Result<(), LedgerErrorV1> {
        match fact {
            SourceNeutralSettlementLifecycleResultV2::ExternalCash(fact) => {
                let maker = match &fact.correction_of {
                    Some(original) => *self
                        .external_cash
                        .get(original)
                        .ok_or(LedgerErrorV1::NonEconomicRecord)?,
                    None => decimal(&fact.signed_delta_pusd)?.is_sign_negative(),
                };
                patch.external_cash.push((fact.identity.clone(), maker));
            }
            SourceNeutralSettlementLifecycleResultV2::MakerAccrualFinalized {
                condition_id,
                status,
                contributions,
            } => {
                let mut market = self
                    .markets
                    .get(&condition_id.0)
                    .cloned()
                    .unwrap_or_default();
                market.maker_accrual_finalized = !matches!(
                    status,
                    SourceNeutralMakerRebateAccrualStatusV1::UnknownIncomplete
                );
                let mut days: BTreeMap<_, Vec<(Identity, RebateContributionFactV1<Identity>)>> =
                    BTreeMap::new();
                for contribution in contributions {
                    let key = (contribution.kind, contribution.attribution_day_start_utc_ms);
                    let additions = days.entry(key).or_default();
                    if !self
                        .days
                        .get(&key)
                        .into_iter()
                        .flat_map(|day| &day.contributions)
                        .chain(additions.iter())
                        .any(|(_, c)| c.trade_identity == contribution.trade_identity)
                    {
                        additions.push((identity.clone(), contribution.clone()));
                    }
                }
                patch.day_updates.extend(
                    days.into_iter().map(|(key, additions)| {
                        (key, RebateDayUpdate::Contributions { additions })
                    }),
                );
                patch.markets.push((condition_id.0.clone(), market));
            }
            SourceNeutralSettlementLifecycleResultV2::Resolved(fact) => {
                let mut market = self
                    .markets
                    .get(&fact.condition_id.0)
                    .cloned()
                    .unwrap_or_default();
                if market.resolution.is_none() {
                    let category = if market.paired > Decimal::ZERO {
                        SettlementCostAttributionV1::Maker
                    } else {
                        SettlementCostAttributionV1::Directional
                    };
                    market.resolution = Some(Resolution {
                        fact: fact.clone(),
                        category,
                        request: None,
                        redeemed_pair: Decimal::ZERO,
                        redeemed_orphan: [Decimal::ZERO; 2],
                        auto_redeemed: false,
                    });
                }
                patch.markets.push((fact.condition_id.0.clone(), market));
            }
            SourceNeutralSettlementLifecycleResultV2::Request(fact) => {
                let mut market = self
                    .markets
                    .get(&fact.settlement_request_id.condition_id.0)
                    .cloned()
                    .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                let resolution = market
                    .resolution
                    .as_mut()
                    .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                if !matches!(
                    resolution.request.as_ref().map(|r| r.outcome),
                    Some(
                        SettlementRequestOutcomeV1::FinalSuccess
                            | SettlementRequestOutcomeV1::FinalFailureNoEffect
                    )
                ) {
                    resolution.request = Some(fact.clone());
                }
                patch
                    .markets
                    .push((fact.settlement_request_id.condition_id.0.clone(), market));
            }
            SourceNeutralSettlementLifecycleResultV2::Cost(fact) => {
                let mut market = self
                    .markets
                    .get(&fact.settlement_request_id.condition_id.0)
                    .cloned()
                    .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                let category = match &fact.correction_of {
                    Some(original) => {
                        self.costs
                            .get(original)
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?
                            .category
                    }
                    None => {
                        market
                            .resolution
                            .as_ref()
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?
                            .category
                    }
                };
                let old = self.costs.get(&fact.cost_identity);
                if old.is_some_and(|old| old.source_order >= *source_order) {
                    return Ok(());
                }
                market.settlement_recognized = add(
                    market.settlement_recognized,
                    decimal(&fact.recognized_pusd)?
                        - old
                            .map(|c| decimal(&c.fact.recognized_pusd))
                            .transpose()?
                            .unwrap_or_default(),
                )?;
                market.settlement_paid = add(
                    market.settlement_paid,
                    decimal(&fact.paid_pusd)?
                        - old
                            .map(|c| decimal(&c.fact.paid_pusd))
                            .transpose()?
                            .unwrap_or_default(),
                )?;
                patch.costs.push((
                    fact.cost_identity.clone(),
                    Cost {
                        source_order: source_order.clone(),
                        fact: fact.clone(),
                        category,
                    },
                ));
                patch
                    .markets
                    .push((fact.settlement_request_id.condition_id.0.clone(), market));
            }
            SourceNeutralSettlementLifecycleResultV2::Contribution(fact) => {
                let key = (fact.kind, fact.attribution_day_start_utc_ms);
                let additions = if self.days.get(&key).is_some_and(|day| {
                    day.contributions
                        .iter()
                        .any(|(_, c)| c.trade_identity == fact.trade_identity)
                }) {
                    Vec::new()
                } else {
                    vec![(identity.clone(), fact.clone())]
                };
                patch
                    .day_updates
                    .push((key, RebateDayUpdate::Contributions { additions }));
            }
            SourceNeutralSettlementLifecycleResultV2::DayFinalized(fact) => {
                let key = (fact.kind, fact.attribution_day_start_utc_ms);
                let day = self.days.get(&key);
                if day.is_none_or(|day| day.result.is_none()) {
                    let contributions = day
                        .map(|day| day.contributions.as_slice())
                        .unwrap_or_default();
                    let total = R::from_decimal(decimal(&fact.accrued_pusd)?);
                    if total.is_positive() && !contributions.is_empty() {
                        let payable = R::from_decimal(decimal(&fact.payable_pusd)?);
                        let mut changed = BTreeMap::new();
                        for (_, contribution) in contributions {
                            let market = changed
                                .entry(contribution.condition_id.0.clone())
                                .or_insert_with(|| {
                                    self.markets
                                        .get(&contribution.condition_id.0)
                                        .cloned()
                                        .unwrap_or_default()
                                });
                            let share = R::from_decimal(decimal(&contribution.contribution_pusd)?)
                                .divided_by(&total);
                            market.confirmed_rebate =
                                market.confirmed_rebate.add(&payable.mul(&share));
                        }
                        patch.markets.extend(changed);
                    }
                    patch.day_updates.push((
                        key,
                        RebateDayUpdate::Finalized {
                            result: (identity.clone(), fact.clone()),
                        },
                    ));
                }
            }
            SourceNeutralSettlementLifecycleResultV2::Payment(fact) => {
                let key = (fact.kind, fact.attribution_day_start_utc_ms);
                self.days
                    .get(&key)
                    .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                patch.day_updates.push((
                    key,
                    RebateDayUpdate::Paid {
                        paid: decimal(&fact.paid_pusd)?,
                    },
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn commit(&mut self, patch: EconomicsPatch) {
        let mut affected = std::collections::BTreeSet::new();
        if self
            .read_index
            .as_ref()
            .is_some_and(|i| i.account.get().is_some())
        {
            affected.extend(patch.markets.iter().map(|(id, _)| id.clone()));
            affected.extend(
                patch
                    .trades
                    .iter()
                    .map(|(_, t)| t.fact.condition_id.0.clone()),
            );
            let add_day_markets = |affected: &mut std::collections::BTreeSet<String>,
                                   day: &RebateDay| {
                if let Some(totals) = &day.contribution_totals {
                    affected.extend(totals.keys().cloned());
                } else {
                    affected.extend(
                        day.contributions
                            .iter()
                            .map(|(_, c)| c.condition_id.0.clone()),
                    );
                }
            };
            for (key, update) in &patch.day_updates {
                match update {
                    RebateDayUpdate::Contributions { additions } => {
                        affected.extend(additions.iter().map(|(_, c)| c.condition_id.0.clone()));
                    }
                    RebateDayUpdate::Existing(day) => {
                        if let Some(old) = self.days.get(key) {
                            add_day_markets(&mut affected, old);
                        }
                        add_day_markets(&mut affected, day);
                    }
                    RebateDayUpdate::Finalized { .. } | RebateDayUpdate::Paid { .. } => {
                        if let Some(day) = self.days.get(key) {
                            add_day_markets(&mut affected, day);
                        }
                    }
                }
            }
        }
        let mut index = self
            .read_index
            .take()
            .unwrap_or_else(|| EconomicReadIndex::from_state(self));
        for (id, trade) in &patch.trades {
            index.replace_trade(id, self.trades.get(id), Some(trade));
        }
        if let Some(cash) = patch.cash {
            self.cash = cash;
        }
        self.external_cash.extend(patch.external_cash);
        self.markets.extend(patch.markets);
        for (id, trade) in patch.trades {
            // 已配对后的费调整只按该原成交尚余贡献归属，不重扫所有成交。
            if let Some(old) = self.trades.get(&id)
                && old.success_applied
                && trade.success_applied
                && old.net_fee != trade.net_fee
                && let Some(market) = self.markets.get_mut(&trade.fact.condition_id.0)
                && let (Some(assets), Some(pools), Some(fees)) = (
                    &market.assets,
                    &market.pool_contribution_shares,
                    &mut market.orphan_fee,
                )
                && let Some(leg) = assets
                    .iter()
                    .position(|a| a.token_id == trade.fact.token_id)
                && let Some(q) = pools[leg].get(&id)
            {
                let quantity =
                    R::from_decimal(decimal(&trade.fact.base_quantity_shares).expect("原份额"));
                if !quantity.is_zero() {
                    fees[leg] = fees[leg].add(
                        &R::from_decimal(trade.net_fee - old.net_fee)
                            .mul(q)
                            .divided_by(&quantity),
                    );
                }
            }
            self.trades.insert(id, trade);
        }
        for (key, update) in patch.day_updates {
            let day = self.days.entry(key).or_insert_with(|| RebateDay {
                kind: key.0,
                day: key.1,
                contributions: Vec::new(),
                result: None,
                paid: Decimal::ZERO,
                contribution_totals: Some(BTreeMap::new()),
            });
            match update {
                RebateDayUpdate::Contributions { additions } => {
                    let totals = day.contribution_totals.get_or_insert_with(|| {
                        day.contributions
                            .iter()
                            .fold(BTreeMap::new(), |mut sums, (_, c)| {
                                let sum =
                                    sums.entry(c.condition_id.0.clone()).or_insert_with(R::zero);
                                *sum = sum.add(&R::from_decimal(
                                    decimal(&c.contribution_pusd).expect("原贡献"),
                                ));
                                sums
                            })
                    });
                    for (_, c) in &additions {
                        let sum = totals
                            .entry(c.condition_id.0.clone())
                            .or_insert_with(R::zero);
                        *sum = sum.add(&R::from_decimal(
                            decimal(&c.contribution_pusd).expect("原贡献"),
                        ));
                        index.mark_contributed(
                            &c.trade_identity,
                            self.trades.get(&c.trade_identity),
                        );
                    }
                    day.contributions.extend(additions);
                }
                RebateDayUpdate::Finalized { result } => day.result = Some(result),
                RebateDayUpdate::Paid { paid } => day.paid = paid,
                RebateDayUpdate::Existing(previous) => {
                    for (_, c) in &previous.contributions {
                        index.mark_contributed(
                            &c.trade_identity,
                            self.trades.get(&c.trade_identity),
                        );
                    }
                    *day = previous;
                }
            }
        }
        if let Some(account) = index.account.get_mut() {
            for id in affected {
                let pending = index
                    .markets
                    .get(&id)
                    .map_or_else(R::zero, |t| t.pending_principal.clone());
                account.replace(&id, self, pending);
            }
        }
        self.read_index = Some(index);
        self.costs.extend(patch.costs);
        for (identity, value) in patch.unapportioned_trades {
            if let Some(value) = value {
                self.unapportioned_trades.insert(identity, value);
            } else {
                self.unapportioned_trades.remove(&identity);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CashReadV1 {
    pub actual_available_pusd: ExactDecimal,
    pub actual_reserved_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResidualCashReadV1 {
    pub directional_cash_pusd: ExactDecimal,
    pub other_cash_pusd: ExactDecimal,
}
/// 原成交、应用状态和可选成本解释贡献；不将成功观察等同于经济已应用。
pub struct TradeEconomicReadV1<'a> {
    pub trade_identity: &'a Identity,
    pub fact: &'a SourceNeutralTradeLifecycleResultV2<Identity>,
    pub economic_applied: bool,
    pub success_applied: bool,
    pub actual_net_fee_pusd: ExactDecimal,
    /// 公共平均成本配平后的原成交残余贡献，不是可卖持仓或费用分录。
    pub unpaired_contribution_shares: ExecutionReadValueV1<ExactRationalV1>,
}
pub struct SuccessfulTradeReadV1<'a> {
    pub trade_identity: &'a Identity,
    pub terminal: &'a SourceNeutralTradeLifecycleResultV2<Identity>,
    pub remaining_success_shares: ExactDecimal,
    pub remaining_principal_cost_pusd: ExactRationalV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExecutedEconomicsReadV1 {
    pub shares: ExactDecimal,
    pub principal_pusd: ExactDecimal,
    pub actual_fee_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RebateAmountsReadV1 {
    pub estimated_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub accrued_unmatured_pusd: ExactRationalV1,
    pub below_threshold_pusd: ExactRationalV1,
    pub confirmed_unpaid_pusd: ExactRationalV1,
    pub paid_pusd: ExactRationalV1,
    pub locked_remainder_pusd: ExactRationalV1,
}
pub struct RebateDayReadV1<'a> {
    pub kind: RebateKindV1,
    pub attribution_day_start_utc_ms: i64,
    pub contributions: Vec<&'a RebateContributionFactV1<Identity>>,
    pub amounts: RebateAmountsReadV1,
}
pub struct ResolutionAccountReadV1<'a> {
    pub resolved: &'a ResolvedPositionFactV1<Identity>,
    pub request: Option<&'a SettlementRequestFactV1<Identity>>,
    pub settlement_cost_attribution: SettlementCostAttributionV1,
    pub economic_predecessors_closed: ExecutionReadValueV1<bool>,
    pub paired_receivable_pusd: ExactDecimal,
    pub winner_orphan_receivable_pusd: ExactDecimal,
}
pub struct SettlementCostReadV1<'a> {
    pub fact: &'a SettlementCostFactV1<Identity>,
    pub attribution: SettlementCostAttributionV1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum EconomicSettlementStateV1 {
    Unresolved,
    ResolvedNoCurrentPayout,
    ReceivableNotRequested,
    RequestPending,
    Redeemed,
    RequestFailedNoEffect,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MarketEconomicTotalsReadV1 {
    pub condition_id: ConditionId,
    pub assets: Option<[AssetIdentityV1; 2]>,
    pub success_shares: [ExactDecimal; 2],
    pub success_principal_cost_pusd: [ExactRationalV1; 2],
    pub repair_gap_shares: [ExactDecimal; 2],
    pub settlement_state: EconomicSettlementStateV1,
    pub unpaid_taker_fee_pusd: ExactDecimal,
    pub paired_shares: ExactDecimal,
    pub orphan_shares: [ExactDecimal; 2],
    pub executed: [ExecutedEconomicsReadV1; 2],
    pub rebates: [RebateAmountsReadV1; 2],
    pub guaranteed_position_value_pusd: ExactDecimal,
    pub paired_resolution_receivable_pusd: ExactDecimal,
    pub winner_orphan_receivable_pusd: ExactDecimal,
    pub unpaid_settlement_fee_pusd: ExactDecimal,
    pub unpaid_settlement_fee_attribution: ExecutionReadValueV1<SettlementCostAttributionV1>,
    pub confirmed_net_pnl_pusd: ExactRationalV1,
    pub confirmed_net_loss_pusd: ExactRationalV1,
    pub maximum_confirmed_net_loss_pusd: ExactRationalV1,
    pub unresolved_orphan_principal_cost_pusd: ExactRationalV1,
    pub unpaired_success_fee_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub successful_investment_pusd: ExactRationalV1,
}
pub struct LedgerEconomicReadV2<'a> {
    base: crate::projection::LedgerProjectionV2,
    cash: CashReadV1,
    residual: ResidualCashReadV1,
    state: &'a EconomicsState,
}
impl LedgerWriterV1 {
    pub(crate) fn cash_balances(&self) -> Result<(Decimal, Decimal), LedgerErrorV1> {
        let account = self
            .account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
        let pending = self.reservations.principal_pusd
            + self.reservations.immediate_fee_pusd
            + self.lifecycle.totals.platform_pending_principal_pusd
            + self.lifecycle.totals.platform_pending_immediate_fee_pusd;
        let reserved = account.balances.principal_encumbrance_pusd
            + account.balances.immediate_taker_fee_encumbrance_pusd
            + pending;
        Ok((account.balances.available_pusd - pending, reserved))
    }
    pub fn economic_read(&self) -> Option<LedgerEconomicReadV2<'_>> {
        let base = self.projection_v2()?;
        let (actual, reserved) = self.cash_balances().ok()?;
        let cash = CashReadV1 {
            actual_available_pusd: exact(actual),
            actual_reserved_pusd: exact(reserved),
        };
        Some(LedgerEconomicReadV2 {
            base,
            cash,
            residual: ResidualCashReadV1 {
                directional_cash_pusd: exact(self.economics.cash.directional_cash),
                other_cash_pusd: exact(self.economics.cash.other_cash),
            },
            state: &self.economics,
        })
    }
}
fn trade_economic_read<'a>(
    state: &'a EconomicsState,
    identity: &'a Identity,
    trade: &'a Trade,
) -> TradeEconomicReadV1<'a> {
    let contribution = if !trade.success_applied {
        ExecutionReadValueV1::NotApplicable
    } else {
        state
            .markets
            .get(&trade.fact.condition_id.0)
            .and_then(|m| m.pool_contribution_shares.as_ref())
            .map(|pools| {
                pools
                    .iter()
                    .find_map(|pool| pool.get(identity))
                    .cloned()
                    .unwrap_or_else(R::zero)
            })
            .map(|q| ExecutionReadValueV1::Known(q.public()))
            .unwrap_or(ExecutionReadValueV1::Unavailable)
    };
    TradeEconomicReadV1 {
        trade_identity: identity,
        fact: &trade.fact,
        economic_applied: trade.economic_applied,
        success_applied: trade.success_applied,
        actual_net_fee_pusd: exact(trade.net_fee),
        unpaired_contribution_shares: contribution,
    }
}

impl LedgerEconomicReadV2<'_> {
    /// 原经济已应用但尚未归还的本金；不含预留，不生成现金或费用分录。
    pub fn committed_principal_pusd(&self) -> ExactRationalV1 {
        if let Some(index) = &self.state.read_index {
            let mut principal = R::zero();
            for (condition, m) in &self.state.markets {
                if !m.resolution.as_ref().is_some_and(|r| {
                    r.redeemed_pair > Decimal::ZERO
                        || r.redeemed_orphan.iter().any(|q| *q > Decimal::ZERO)
                }) {
                    principal = principal.add(&m.success_cost[0]).add(&m.success_cost[1]);
                }
                if let Some(totals) = index.markets.get(condition) {
                    principal = principal.add(&totals.pending_principal);
                }
            }
            return principal.public();
        }
        let mut principal = R::zero();
        for trade in self.successful_trades() {
            principal =
                principal.add(&R::try_from(&trade.remaining_principal_cost_pusd).expect("原成本"));
        }
        for trade in self
            .trades()
            .filter(|t| t.economic_applied && !t.success_applied)
        {
            principal = principal.add(&R::from_decimal(
                decimal(&trade.fact.quote_amount_pusd).expect("原本金"),
            ));
        }
        principal.public()
    }
    fn unpaired_fee(
        &self,
        condition: &str,
        market: &Market,
    ) -> ExecutionReadValueV1<ExactRationalV1> {
        if let Some(fees) = &market.orphan_fee {
            return ExecutionReadValueV1::Known(fees[0].add(&fees[1]).public());
        }
        // 旧稀疏补丁没有此字段，沿原实际贡献读取，不伪造为零。
        let mut fee = R::zero();
        for trade in self.trades().filter(|t| {
            t.fact.condition_id.0 == condition && t.success_applied && t.economic_applied
        }) {
            let ExecutionReadValueV1::Known(q) = trade.unpaired_contribution_shares else {
                return ExecutionReadValueV1::Unavailable;
            };
            let original =
                R::from_decimal(decimal(&trade.fact.base_quantity_shares).expect("原份额"));
            if !original.is_zero() {
                fee = fee.add(
                    &R::from_decimal(decimal(&trade.actual_net_fee_pusd).expect("原费用"))
                        .mul(&R::try_from(&q).expect("原贡献"))
                        .divided_by(&original),
                );
            }
        }
        ExecutionReadValueV1::Known(fee.public())
    }
    pub fn into_base(self) -> crate::projection::LedgerProjectionV2 {
        self.base
    }
    pub fn base(&self) -> &crate::projection::LedgerProjectionV2 {
        &self.base
    }
    pub fn cash(&self) -> &CashReadV1 {
        &self.cash
    }
    pub fn residual_cash(&self) -> &ResidualCashReadV1 {
        &self.residual
    }
    pub fn settlement_costs(&self) -> impl Iterator<Item = SettlementCostReadV1<'_>> {
        self.state.costs.values().map(|cost| SettlementCostReadV1 {
            fact: &cost.fact,
            attribution: cost.category,
        })
    }
    pub fn trades(&self) -> impl Iterator<Item = TradeEconomicReadV1<'_>> {
        self.state
            .trades
            .iter()
            .map(|(identity, trade)| trade_economic_read(self.state, identity, trade))
    }
    pub fn successful_trades(&self) -> impl Iterator<Item = SuccessfulTradeReadV1<'_>> {
        self.state
            .trades
            .iter()
            .filter(|(_, t)| t.success_applied)
            .map(|(identity, t)| {
                let redeemed = self
                    .state
                    .markets
                    .get(&t.fact.condition_id.0)
                    .and_then(|m| m.resolution.as_ref())
                    .is_some_and(|r| {
                        r.redeemed_pair > Decimal::ZERO
                            || r.redeemed_orphan.iter().any(|q| *q > Decimal::ZERO)
                    });
                SuccessfulTradeReadV1 {
                    trade_identity: identity,
                    terminal: &t.fact,
                    remaining_success_shares: if redeemed {
                        exact(Decimal::ZERO)
                    } else {
                        t.fact.base_quantity_shares.clone()
                    },
                    remaining_principal_cost_pusd: if redeemed {
                        R::zero().public()
                    } else {
                        R::from_decimal(
                            decimal(&t.fact.quote_amount_pusd).expect("已验证原成交本金"),
                        )
                        .public()
                    },
                }
            })
    }
    pub fn rebate_days(&self) -> impl Iterator<Item = RebateDayReadV1<'_>> {
        let mut keys: std::collections::BTreeSet<_> = self.state.days.keys().copied().collect();
        if let Some(index) = &self.state.read_index {
            keys.extend(index.estimates.keys().copied());
        } else {
            keys.extend(self.state.trades.values().map(|t| {
                (
                    if role(t.fact.liquidity_role) == 0 {
                        RebateKindV1::Maker
                    } else {
                        RebateKindV1::Taker
                    },
                    t.fact.matched_at_utc_ms.div_euclid(86_400_000) * 86_400_000,
                )
            }));
        }
        keys.into_iter().map(|(kind, day)| {
            let stored = self.state.days.get(&(kind, day));
            let amounts = RebateAmountsReadV1::from_amounts(
                stored.map_or_else(|| std::array::from_fn(|_| R::zero()), |d| d.amounts(None)),
                self.estimated_rebate(kind, None, Some(day)),
            );
            RebateDayReadV1 {
                kind,
                attribution_day_start_utc_ms: day,
                contributions: stored
                    .into_iter()
                    .flat_map(|d| d.contributions.iter().map(|(_, c)| c))
                    .collect(),
                amounts,
            }
        })
    }
    fn estimated_rebate(
        &self,
        kind: RebateKindV1,
        condition: Option<&str>,
        day: Option<i64>,
    ) -> ExecutionReadValueV1<ExactRationalV1> {
        if let Some(index) = &self.state.read_index {
            return index.estimated(kind, condition, day);
        }
        let r = if kind == RebateKindV1::Maker { 0 } else { 1 };
        let mut estimated = R::zero();
        for (id, trade) in self.state.trades.iter().filter(|(_, t)| {
            role(t.fact.liquidity_role) == r
                && condition.is_none_or(|c| t.fact.condition_id.0 == c)
                && day.is_none_or(|d| {
                    t.fact.matched_at_utc_ms.div_euclid(86_400_000) * 86_400_000 == d
                })
        }) {
            if matches!(
                trade.fact.economic_disposition,
                SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
                    | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated
            ) || self.state.days.values().any(|d| {
                d.kind == kind && d.contributions.iter().any(|(_, c)| &c.trade_identity == id)
            }) {
                continue;
            }
            let fee = if r == 0 {
                match &trade.maker_fee_equivalent {
                    ExecutionReadValueV1::Known(fee) => fee,
                    _ => return ExecutionReadValueV1::Unavailable,
                }
            } else {
                &trade.fact.immediate_taker_fee_pusd
            };
            estimated = estimated.add(&R::from_decimal(decimal(fee).expect("原费用")).mul(
                &R::from_decimal(if r == 0 {
                    Decimal::new(2, 1)
                } else {
                    Decimal::new(8, 2)
                }),
            ));
        }
        ExecutionReadValueV1::Known(estimated.public())
    }
    pub fn resolutions(&self) -> impl Iterator<Item = ResolutionAccountReadV1<'_>> {
        self.state.markets.iter().filter_map(|(condition, market)| {
            let resolution = market.resolution.as_ref()?;
            let (pair, orphan) = market.receivables();
            Some(ResolutionAccountReadV1 {
                resolved: &resolution.fact,
                request: resolution.request.as_ref(),
                settlement_cost_attribution: resolution.category,
                economic_predecessors_closed: ExecutionReadValueV1::Known(
                    self.state.economic_predecessors_closed(condition),
                ),
                paired_receivable_pusd: exact(pair),
                winner_orphan_receivable_pusd: exact(orphan),
            })
        })
    }
    pub fn market_totals(&self) -> impl Iterator<Item = MarketEconomicTotalsReadV1> + '_ {
        self.state.markets.iter().map(|(condition, m)| {
            let (pair, orphan) = m.receivables();
            let redeemed = m
                .resolution
                .as_ref()
                .map(|r| r.redeemed_pair)
                .unwrap_or_default();
            let redeemed_orphan = m
                .resolution
                .as_ref()
                .map(|r| r.redeemed_orphan)
                .unwrap_or_default();
            let rebates = std::array::from_fn(|r| {
                let kind = if r == 0 {
                    RebateKindV1::Maker
                } else {
                    RebateKindV1::Taker
                };
                let mut amounts: [R; 5] = std::array::from_fn(|_| R::zero());
                for day in self.state.days.values().filter(|d| d.kind == kind) {
                    for (sum, value) in amounts.iter_mut().zip(day.amounts(Some(condition))) {
                        *sum = sum.add(&value);
                    }
                }
                RebateAmountsReadV1::from_amounts(
                    amounts,
                    self.estimated_rebate(kind, Some(condition), None),
                )
            });
            // 修复缺口只看已公开终局实际份额；经济成功池仍沿原入账条件更新。
            let mut final_success_quantity = [Decimal::ZERO; 2];
            if let Some(assets) = &m.assets {
                if let Some(index) = &self.state.read_index {
                    if let Some(totals) = index.markets.get(condition) {
                        final_success_quantity = std::array::from_fn(|i| {
                            *totals
                                .final_success
                                .get(&assets[i].token_id.0)
                                .unwrap_or(&Decimal::ZERO)
                        });
                    }
                } else {
                    for trade in self.state.trades.values().filter(|t| {
                        t.fact.condition_id.0 == *condition
                            && t.fact.economic_disposition
                                == SourceNeutralTradeEconomicDispositionV1::FinalSuccess
                    }) {
                        if let Some(i) = assets
                            .iter()
                            .position(|a| a.token_id == trade.fact.token_id)
                        {
                            final_success_quantity[i] +=
                                decimal(&trade.fact.base_quantity_shares).expect("原真实成交份额");
                        }
                    }
                }
            }
            MarketEconomicTotalsReadV1 {
                condition_id: ConditionId(condition.clone()),
                assets: m.assets.clone(),
                success_shares: std::array::from_fn(|i| {
                    exact(m.success_quantity[i] - redeemed - redeemed_orphan[i])
                }),
                success_principal_cost_pusd: std::array::from_fn(|i| {
                    if redeemed > Decimal::ZERO || redeemed_orphan[i] > Decimal::ZERO {
                        R::zero().public()
                    } else {
                        m.success_cost[i].public()
                    }
                }),
                repair_gap_shares: std::array::from_fn(|i| {
                    exact(
                        (final_success_quantity[1 - i] - final_success_quantity[i])
                            .max(Decimal::ZERO),
                    )
                }),
                unpaid_taker_fee_pusd: exact(m.unpaid_taker),
                settlement_state: match &m.resolution {
                    None => EconomicSettlementStateV1::Unresolved,
                    Some(resolution) if resolution.auto_redeemed => {
                        EconomicSettlementStateV1::Redeemed
                    }
                    Some(resolution) => match resolution.request.as_ref().map(|r| r.outcome) {
                        Some(SettlementRequestOutcomeV1::FinalSuccess)
                            if resolution.redeemed_pair > Decimal::ZERO
                                || resolution
                                    .redeemed_orphan
                                    .iter()
                                    .any(|q| *q > Decimal::ZERO) =>
                        {
                            EconomicSettlementStateV1::Redeemed
                        }
                        Some(SettlementRequestOutcomeV1::FinalFailureNoEffect) => {
                            EconomicSettlementStateV1::RequestFailedNoEffect
                        }
                        Some(_) => EconomicSettlementStateV1::RequestPending,
                        None if pair + orphan > Decimal::ZERO => {
                            EconomicSettlementStateV1::ReceivableNotRequested
                        }
                        None => EconomicSettlementStateV1::ResolvedNoCurrentPayout,
                    },
                },
                paired_shares: exact(m.paired - redeemed),
                orphan_shares: std::array::from_fn(|i| {
                    exact(m.pools[i].quantity - redeemed_orphan[i])
                }),
                executed: std::array::from_fn(|r| ExecutedEconomicsReadV1 {
                    shares: exact(m.executed_quantity[r]),
                    principal_pusd: exact(m.executed_principal[r]),
                    actual_fee_pusd: exact(m.executed_fees[r]),
                }),
                rebates,
                guaranteed_position_value_pusd: exact(if m.resolution.is_some() {
                    Decimal::ZERO
                } else {
                    m.paired
                }),
                paired_resolution_receivable_pusd: exact(pair),
                winner_orphan_receivable_pusd: exact(orphan),
                unpaid_settlement_fee_pusd: exact(m.settlement_recognized - m.settlement_paid),
                unpaid_settlement_fee_attribution: m
                    .resolution
                    .as_ref()
                    .map(|r| ExecutionReadValueV1::Known(r.category))
                    .unwrap_or(ExecutionReadValueV1::NotApplicable),
                confirmed_net_pnl_pusd: m.pi.public(),
                confirmed_net_loss_pusd: R::zero().sub(&m.pi).max(R::zero()).public(),
                maximum_confirmed_net_loss_pusd: m.maximum_loss.public(),
                unpaired_success_fee_pusd: self.unpaired_fee(condition, m),
                successful_investment_pusd: if let Some(index) = &self.state.read_index {
                    index
                        .markets
                        .get(condition)
                        .map_or_else(R::zero, |t| t.investment.clone())
                        .public()
                } else {
                    self.state
                        .trades
                        .values()
                        .filter(|t| {
                            t.fact.condition_id.0 == *condition
                                && t.success_applied
                                && t.economic_applied
                        })
                        .fold(R::zero(), |sum, t| {
                            sum.add(&R::from_decimal(
                                decimal(&t.fact.quote_amount_pusd).expect("原本金"),
                            ))
                            .add(&R::from_decimal(t.net_fee))
                        })
                        .public()
                },
                unresolved_orphan_principal_cost_pusd: if m.resolution.is_some() {
                    R::zero().public()
                } else {
                    m.pools[0].cost.add(&m.pools[1].cost).public()
                },
            }
        })
    }
}
impl Market {
    fn receivables(&self) -> (Decimal, Decimal) {
        let Some(resolution) = &self.resolution else {
            return (Decimal::ZERO, Decimal::ZERO);
        };
        let pair = self.paired - resolution.redeemed_pair;
        let orphan = (0..2)
            .map(|i| {
                (self.pools[i].quantity - resolution.redeemed_orphan[i])
                    * self.payout(i).expect("已验证判定映射")
            })
            .sum();
        (pair, orphan)
    }
}
impl RebateAmountsReadV1 {
    fn from_amounts(
        [accrued, below_threshold, unpaid, paid, locked]: [R; 5],
        estimated_pusd: ExecutionReadValueV1<ExactRationalV1>,
    ) -> Self {
        Self {
            estimated_pusd,
            accrued_unmatured_pusd: accrued.public(),
            below_threshold_pusd: below_threshold.public(),
            confirmed_unpaid_pusd: unpaid.public(),
            paid_pusd: paid.public(),
            locked_remainder_pusd: locked.public(),
        }
    }
}
impl RebateDay {
    fn amounts(&self, condition: Option<&str>) -> [R; 5] {
        let contribution = if let Some(totals) = &self.contribution_totals {
            condition.map_or_else(
                || totals.values().fold(R::zero(), |sum, value| sum.add(value)),
                |c| totals.get(c).cloned().unwrap_or_else(R::zero),
            )
        } else {
            self.contributions
                .iter()
                .filter(|(_, c)| condition.is_none_or(|m| m == c.condition_id.0))
                .fold(R::zero(), |sum, (_, c)| {
                    sum.add(&R::from_decimal(
                        decimal(&c.contribution_pusd).expect("已验证贡献"),
                    ))
                })
        };
        let mut amounts = std::array::from_fn(|_| R::zero());
        if let Some((_, result)) = &self.result {
            let accrued = decimal(&result.accrued_pusd).expect("已验证日累计");
            let total = R::from_decimal(accrued);
            if !total.is_zero() {
                let fraction = contribution.divided_by(&total);
                if accrued < Decimal::ONE {
                    amounts[1] = contribution;
                } else {
                    amounts[2] = R::from_decimal(
                        decimal(&result.payable_pusd).expect("已验证应付") - self.paid,
                    )
                    .mul(&fraction);
                    amounts[3] = R::from_decimal(self.paid).mul(&fraction);
                    amounts[4] = R::from_decimal(
                        decimal(&result.locked_remainder_pusd).expect("已验证余项"),
                    )
                    .mul(&fraction);
                }
            }
        } else {
            amounts[0] = contribution;
        }
        amounts
    }
}

#[cfg(test)]
mod rational_tests {
    use super::*;

    #[test]
    fn exact_zero_identities_keep_normalized_nonterminating_values_and_reject_zero_divisor() {
        let zero = EconomicRational::ratio(0.into(), (-7).into());
        assert_eq!(
            zero.public(),
            ExactRationalV1 {
                numerator: "0".into(),
                denominator: "1".into()
            }
        );
        for (n, d, expected_n, expected_d) in
            [(2, 3, "2", "3"), (-7, 3, "-7", "3"), (7, -3, "-7", "3")]
        {
            let value = EconomicRational::ratio(n.into(), d.into());
            let expected = ExactRationalV1 {
                numerator: expected_n.into(),
                denominator: expected_d.into(),
            };
            assert_eq!(value.add(&zero).public(), expected);
            assert_eq!(zero.add(&value).public(), expected);
            assert_eq!(value.sub(&zero), value);
            assert_eq!(value.mul(&zero), zero);
            assert_eq!(zero.divided_by(&value), zero);
        }
        assert!(std::panic::catch_unwind(|| EconomicRational::ratio(0.into(), 0.into())).is_err());
        assert!(std::panic::catch_unwind(|| zero.divided_by(&zero)).is_err());
    }

    #[test]
    fn integer_ratios_normalize_for_arithmetic_and_persistence() {
        let half = EconomicRational::from_decimal(Decimal::new(5, 1));
        for (n, d) in [("2", "4"), ("01", "02"), ("-1", "-2")] {
            let input = ExactRationalV1 {
                numerator: n.into(),
                denominator: d.into(),
            };
            let value = EconomicRational::try_from(&input).unwrap();
            assert_eq!(value, half);
            assert_eq!(
                value.add(&half),
                EconomicRational::from_decimal(Decimal::ONE)
            );
            let restored: EconomicRational =
                serde_json::from_str(&serde_json::to_string(&input).unwrap()).unwrap();
            assert_eq!(restored, half);
            assert_eq!(
                serde_json::to_string(&restored).unwrap(),
                r#"{"numerator":"1","denominator":"2"}"#
            );
        }
        let negative: EconomicRational =
            serde_json::from_str(r#"{"numerator":"1","denominator":"-2"}"#).unwrap();
        assert!(negative < EconomicRational::zero());
        assert_eq!(negative.add(&half), EconomicRational::zero());
        let zero: EconomicRational =
            serde_json::from_str(r#"{"numerator":"0","denominator":"-7"}"#).unwrap();
        assert_eq!(
            zero.public(),
            ExactRationalV1 {
                numerator: "0".into(),
                denominator: "1".into()
            }
        );
        for (n, d) in [("bad", "2"), ("1", "bad"), ("1", "0")] {
            assert!(
                EconomicRational::try_from(&ExactRationalV1 {
                    numerator: n.into(),
                    denominator: d.into()
                })
                .is_err()
            );
        }
    }
}
