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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Pool {
    pub quantity: Decimal,
    pub cost: R,
    pub contributors: Vec<Identity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FixedPairedAllocationV1 {
    pub source_trade_identity: Identity,
    pub quantity_shares: ExactDecimal,
    pub principal_cost_pusd: [ExactRationalV1; 2],
    pub contribution_refs: [Vec<Identity>; 2],
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
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Market {
    pub assets: Option<[AssetIdentityV1; 2]>,
    pub maker_accrual_finalized: bool,
    pub pools: [Pool; 2],
    pub paired: Decimal,
    pub paired_cost: [R; 2],
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
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Cost {
    pub source_order: execution_normalization::SourceOrderV1,
    pub fact: SettlementCostFactV1<Identity>,
    pub category: SettlementCostAttributionV1,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CashAttribution {
    pub maker_cash: Decimal,
    pub directional_cash: Decimal,
    pub other_cash: Decimal,
    pub unpaid_taker: Decimal,
    pub unpaid_settlement: Decimal,
}
#[derive(Default)]
pub(crate) struct EconomicsState {
    pub external_cash: BTreeMap<Identity, bool>,
    pub cash: CashAttribution,
    pub markets: BTreeMap<String, Market>,
    pub trades: BTreeMap<Identity, Trade>,
    pub days: BTreeMap<(RebateKindV1, i64), RebateDay>,
    pub costs: BTreeMap<Identity, Cost>,
    pub allocations: Vec<FixedPairedAllocationV1>,
    // 经济效果可能先于首次 trade 详情公开；只保留尚待关联的原身份。
    pub unapportioned_trades: BTreeMap<Identity, (bool, Decimal)>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct EconomicsPatch {
    pub external_cash: Vec<(Identity, bool)>,
    pub cash: Option<CashAttribution>,
    pub markets: Vec<(String, Market)>,
    pub trades: Vec<(Identity, Trade)>,
    pub days: Vec<((RebateKindV1, i64), RebateDay)>,
    pub costs: Vec<(Identity, Cost)>,
    pub allocations: Vec<FixedPairedAllocationV1>,
    pub unapportioned_trades: Vec<(Identity, Option<(bool, Decimal)>)>,
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
    fn apply_success(
        &mut self,
        trade: &Trade,
        patch: &mut EconomicsPatch,
    ) -> Result<(), LedgerErrorV1> {
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
        self.success_quantity[leg] = add(self.success_quantity[leg], q)?;
        self.success_cost[leg] = self.success_cost[leg].add(&c);
        let pool = &mut self.pools[leg];
        pool.quantity = add(pool.quantity, q)?;
        pool.cost = pool.cost.add(&c);
        pool.contributors.push(trade.fact.trade_identity.clone());
        let quantity = self.pools[0].quantity.min(self.pools[1].quantity);
        if quantity > Decimal::ZERO {
            let costs: [R; 2] = std::array::from_fn(|i| {
                R::from_decimal(quantity)
                    .mul(&self.pools[i].cost)
                    .divided_by(&R::from_decimal(self.pools[i].quantity))
            });
            patch.allocations.push(FixedPairedAllocationV1 {
                source_trade_identity: trade.fact.trade_identity.clone(),
                quantity_shares: exact(quantity),
                principal_cost_pusd: [costs[0].public(), costs[1].public()],
                contribution_refs: [
                    self.pools[0].contributors.clone(),
                    self.pools[1].contributors.clone(),
                ],
            });
            self.paired = add(self.paired, quantity)?;
            for (i, cost) in costs.into_iter().enumerate() {
                self.paired_cost[i] = self.paired_cost[i].add(&cost);
                self.pools[i].quantity -= quantity;
                self.pools[i].cost = self.pools[i].cost.sub(&cost);
                if self.pools[i].quantity.is_zero() {
                    self.pools[i].contributors.clear();
                }
            }
        }
        Ok(())
    }
}
impl EconomicsState {
    pub(crate) fn dependencies_present(&self, record: &NoeConsumedRecordRefV1) -> bool {
        let resolution = |condition: &ConditionId| {
            self.markets
                .get(&condition.0)
                .is_some_and(|m| m.resolution.is_some())
        };
        let contribution_ready = |c: &RebateContributionFactV1<Identity>| {
            self.trades
                .get(&c.trade_identity)
                .is_some_and(|t| t.success_applied)
        };
        match record.record() {
            NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::SettlementLifecycleV2 { result },
                ..
            } => match result {
                SourceNeutralSettlementLifecycleResultV2::Contribution(c) => contribution_ready(c),
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
                SourceNeutralEconomicDispositionV2::AccountTransition { transition, .. } => {
                    match transition {
                        SourceNeutralAccountTransitionKindV2::ExternalCashApplied {
                            cash_identity,
                        } => self.external_cash.contains_key(cash_identity),
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
                    }
                }
                _ => true,
            },
            _ => true,
        }
    }
    pub(crate) fn prepare(
        &self,
        effect: &LedgerCommittedEffectV1,
    ) -> Result<Option<EconomicsPatch>, LedgerErrorV1> {
        let LedgerTransactionSourceV1::Memory(source) = &effect.transaction.source else {
            return Ok(None);
        };
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
        Self::success(&mut market, &mut trade, patch)?;
        patch.markets.push((fact.condition_id.0.clone(), market));
        patch.trades.push((fact.trade_identity.clone(), trade));
        Ok(())
    }
    fn success(
        market: &mut Market,
        trade: &mut Trade,
        patch: &mut EconomicsPatch,
    ) -> Result<(), LedgerErrorV1> {
        if trade.economic_applied
            && !trade.success_applied
            && trade.fact.economic_disposition
                == SourceNeutralTradeEconomicDispositionV1::FinalSuccess
        {
            market.apply_success(trade, patch)?;
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
            SourceNeutralEconomicDispositionV2::AccountOpening { available_pusd } => {
                cash.maker_cash = decimal(available_pusd)?
            }
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
                        cash.maker_cash = add(cash.maker_cash, delta)?;
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
                                    Self::success(&mut market, &mut trade, patch)?;
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
                            SettlementCostAttributionV1::Maker => {
                                cash.maker_cash = add(cash.maker_cash, delta)?
                            }
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
                        cash.maker_cash = add(cash.maker_cash, paired)?;
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
                        if *self
                            .external_cash
                            .get(cash_identity)
                            .ok_or(LedgerErrorV1::NonEconomicRecord)?
                        {
                            cash.maker_cash = add(cash.maker_cash, delta)?;
                        } else {
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
                let mut days = BTreeMap::new();
                for contribution in contributions {
                    let key = (contribution.kind, contribution.attribution_day_start_utc_ms);
                    let day = days.entry(key).or_insert_with(|| {
                        self.days.get(&key).cloned().unwrap_or(RebateDay {
                            kind: key.0,
                            day: key.1,
                            contributions: Vec::new(),
                            result: None,
                            paid: Decimal::ZERO,
                        })
                    });
                    if !day
                        .contributions
                        .iter()
                        .any(|(_, c)| c.trade_identity == contribution.trade_identity)
                    {
                        day.contributions
                            .push((identity.clone(), contribution.clone()));
                    }
                }
                patch.days.extend(days);
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
                let mut day = self.days.get(&key).cloned().unwrap_or(RebateDay {
                    kind: key.0,
                    day: key.1,
                    contributions: Vec::new(),
                    result: None,
                    paid: Decimal::ZERO,
                });
                if !day
                    .contributions
                    .iter()
                    .any(|(_, c)| c.trade_identity == fact.trade_identity)
                {
                    day.contributions.push((identity.clone(), fact.clone()));
                }
                patch.days.push((key, day));
            }
            SourceNeutralSettlementLifecycleResultV2::DayFinalized(fact) => {
                let key = (fact.kind, fact.attribution_day_start_utc_ms);
                let mut day = self.days.get(&key).cloned().unwrap_or(RebateDay {
                    kind: key.0,
                    day: key.1,
                    contributions: Vec::new(),
                    result: None,
                    paid: Decimal::ZERO,
                });
                if day.result.is_none() {
                    let total = R::from_decimal(decimal(&fact.accrued_pusd)?);
                    if total.is_positive() {
                        let mut changed = BTreeMap::new();
                        for (_, contribution) in &day.contributions {
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
                            market.confirmed_rebate = market
                                .confirmed_rebate
                                .add(&R::from_decimal(decimal(&fact.payable_pusd)?).mul(&share));
                        }
                        patch.markets.extend(changed);
                    }
                    day.result = Some((identity.clone(), fact.clone()));
                }
                patch.days.push((key, day));
            }
            SourceNeutralSettlementLifecycleResultV2::Payment(fact) => {
                let key = (fact.kind, fact.attribution_day_start_utc_ms);
                let mut day = self
                    .days
                    .get(&key)
                    .cloned()
                    .ok_or(LedgerErrorV1::NonEconomicRecord)?;
                day.paid = decimal(&fact.paid_pusd)?;
                patch.days.push((key, day));
            }
        }
        Ok(())
    }
    pub(crate) fn commit(&mut self, patch: EconomicsPatch) {
        if let Some(cash) = patch.cash {
            self.cash = cash;
        }
        self.external_cash.extend(patch.external_cash);
        self.markets.extend(patch.markets);
        self.trades.extend(patch.trades);
        self.days.extend(patch.days);
        self.costs.extend(patch.costs);
        self.allocations.extend(patch.allocations);
        for (identity, value) in patch.unapportioned_trades {
            if let Some(value) = value {
                self.unapportioned_trades.insert(identity, value);
            } else {
                self.unapportioned_trades.remove(&identity);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DualCashReadV1 {
    pub actual_available_pusd: ExactDecimal,
    pub actual_reserved_pusd: ExactDecimal,
    pub maker_available_pusd: ExactDecimal,
    pub maker_reserved_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResidualCashReadV1 {
    pub directional_cash_pusd: ExactDecimal,
    pub other_cash_pusd: ExactDecimal,
}
pub struct SuccessfulTradeReadV1<'a> {
    pub trade_identity: &'a Identity,
    pub terminal: &'a SourceNeutralTradeLifecycleResultV2<Identity>,
    pub remaining_success_shares: ExactDecimal,
    pub remaining_principal_cost_pusd: ExactRationalV1,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutedEconomicsReadV1 {
    pub shares: ExactDecimal,
    pub principal_pusd: ExactDecimal,
    pub actual_fee_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebateAmountsReadV1 {
    pub estimated_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub accrued_unmatured_pusd: ExactRationalV1,
    pub below_threshold_pusd: ExactRationalV1,
    pub confirmed_unpaid_pusd: ExactRationalV1,
    pub paid_pusd: ExactRationalV1,
    pub locked_remainder_pusd: ExactRationalV1,
}
impl Default for RebateAmountsReadV1 {
    fn default() -> Self {
        Self {
            estimated_pusd: ExecutionReadValueV1::Known(R::zero().public()),
            accrued_unmatured_pusd: R::zero().public(),
            below_threshold_pusd: R::zero().public(),
            confirmed_unpaid_pusd: R::zero().public(),
            paid_pusd: R::zero().public(),
            locked_remainder_pusd: R::zero().public(),
        }
    }
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EconomicSettlementStateV1 {
    Unresolved,
    ResolvedNoCurrentPayout,
    ReceivableNotRequested,
    RequestPending,
    Redeemed,
    RequestFailedNoEffect,
}
#[derive(Clone, Debug, PartialEq, Eq)]
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
}
pub struct LedgerEconomicReadV1<'a> {
    base: crate::projection::LedgerProjectionV2,
    cash: DualCashReadV1,
    residual: ResidualCashReadV1,
    state: &'a EconomicsState,
}
impl LedgerWriterV1 {
    pub fn economic_read(&self) -> Option<LedgerEconomicReadV1<'_>> {
        let base = self.projection_v2()?;
        let account = self.account.as_ref()?;
        let pending = self.reservations.principal_pusd
            + self.reservations.immediate_fee_pusd
            + self.lifecycle.totals.platform_pending_principal_pusd
            + self.lifecycle.totals.platform_pending_immediate_fee_pusd;
        let reserved = account.balances.principal_encumbrance_pusd
            + account.balances.immediate_taker_fee_encumbrance_pusd
            + pending;
        let cash = DualCashReadV1 {
            actual_available_pusd: exact(account.balances.available_pusd - pending),
            actual_reserved_pusd: exact(reserved),
            maker_available_pusd: exact(self.economics.cash.maker_cash - reserved),
            maker_reserved_pusd: exact(reserved),
        };
        Some(LedgerEconomicReadV1 {
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
impl LedgerEconomicReadV1<'_> {
    pub fn base(&self) -> &crate::projection::LedgerProjectionV2 {
        &self.base
    }
    pub fn cash(&self) -> &DualCashReadV1 {
        &self.cash
    }
    pub fn residual_cash(&self) -> &ResidualCashReadV1 {
        &self.residual
    }
    pub fn paired_allocations(&self) -> impl Iterator<Item = &FixedPairedAllocationV1> {
        self.state.allocations.iter()
    }
    pub fn settlement_costs(&self) -> impl Iterator<Item = SettlementCostReadV1<'_>> {
        self.state.costs.values().map(|cost| SettlementCostReadV1 {
            fact: &cost.fact,
            attribution: cost.category,
        })
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
        keys.into_iter().map(|(kind, day)| {
            let stored = self.state.days.get(&(kind, day));
            let mut amounts = stored.map_or_else(RebateAmountsReadV1::default, |d| d.amounts(None));
            amounts.estimated_pusd = self.estimated_rebate(kind, None, Some(day));
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
            let pending = self.state.trades.values().any(|trade| {
                trade.fact.condition_id.0 == *condition
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
            });
            let (pair, orphan) = market.receivables();
            Some(ResolutionAccountReadV1 {
                resolved: &resolution.fact,
                request: resolution.request.as_ref(),
                settlement_cost_attribution: resolution.category,
                economic_predecessors_closed: ExecutionReadValueV1::Known(
                    !pending && market.maker_accrual_finalized,
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
                let mut result = RebateAmountsReadV1::default();
                for day in self.state.days.values().filter(|d| d.kind == kind) {
                    result.accumulate(&day.amounts(Some(condition)));
                }
                result.estimated_pusd = self.estimated_rebate(kind, Some(condition), None);
                result
            });
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
                    exact((m.success_quantity[1 - i] - m.success_quantity[i]).max(Decimal::ZERO))
                }),
                unpaid_taker_fee_pusd: exact(m.unpaid_taker),
                settlement_state: match &m.resolution {
                    None => EconomicSettlementStateV1::Unresolved,
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
    fn accumulate(&mut self, rhs: &Self) {
        for (a, b) in [
            (
                &mut self.accrued_unmatured_pusd,
                &rhs.accrued_unmatured_pusd,
            ),
            (&mut self.below_threshold_pusd, &rhs.below_threshold_pusd),
            (&mut self.confirmed_unpaid_pusd, &rhs.confirmed_unpaid_pusd),
            (&mut self.paid_pusd, &rhs.paid_pusd),
            (&mut self.locked_remainder_pusd, &rhs.locked_remainder_pusd),
        ] {
            *a = R::try_from(&*a)
                .expect("内部规范分数")
                .add(&R::try_from(b).expect("内部规范分数"))
                .public();
        }
    }
}
impl RebateDay {
    fn amounts(&self, condition: Option<&str>) -> RebateAmountsReadV1 {
        let contribution = self
            .contributions
            .iter()
            .filter(|(_, c)| condition.is_none_or(|m| m == c.condition_id.0))
            .fold(R::zero(), |sum, (_, c)| {
                sum.add(&R::from_decimal(
                    decimal(&c.contribution_pusd).expect("已验证贡献"),
                ))
            });
        let mut amounts = RebateAmountsReadV1::default();
        if let Some((_, result)) = &self.result {
            let total = R::from_decimal(decimal(&result.accrued_pusd).expect("已验证日累计"));
            if !total.is_zero() {
                let fraction = contribution.divided_by(&total);
                if decimal(&result.accrued_pusd).expect("已验证日累计") < Decimal::ONE {
                    amounts.below_threshold_pusd = contribution.public();
                } else {
                    amounts.confirmed_unpaid_pusd = R::from_decimal(
                        decimal(&result.payable_pusd).expect("已验证应付") - self.paid,
                    )
                    .mul(&fraction)
                    .public();
                    amounts.paid_pusd = R::from_decimal(self.paid).mul(&fraction).public();
                    amounts.locked_remainder_pusd = R::from_decimal(
                        decimal(&result.locked_remainder_pusd).expect("已验证余项"),
                    )
                    .mul(&fraction)
                    .public();
                }
            }
        } else {
            amounts.accrued_unmatured_pusd = contribution.public();
        }
        amounts
    }
}
