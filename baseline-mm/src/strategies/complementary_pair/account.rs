use super::*;

impl ComplementaryPairV1 {
    pub(super) fn state(
        &self,
        input: &StrategyInputV1,
        f: &PairMarketRead<'_>,
    ) -> Result<MarketState, StrategyErrorV1> {
        let o = &f.observation;
        let condition = &o.market.condition_id;
        let m = input
            .action_before
            .economics()
            .markets
            .iter()
            .find(|m| &m.condition_id == condition);
        let mut orphan = [Decimal::ZERO; 2];
        let mut principal = R::zero();
        let mut fee = R::zero();
        let mut unknown = false;
        if let Some(m) = m
            && m.settlement_state == EconomicSettlementStateV1::Unresolved
        {
            let assets = m
                .assets
                .as_ref()
                .ok_or(StrategyErrorV1::Arithmetic("公共持仓缺资产身份"))?;
            for (side, asset) in o.assets.iter().enumerate().take(2) {
                let i = assets
                    .iter()
                    .position(|a| a == &asset.asset)
                    .ok_or(StrategyErrorV1::Arithmetic("公共持仓资产不一致"))?;
                orphan[side] = decimal(&m.orphan_shares[i])?;
            }
            principal = rational(&m.unresolved_orphan_principal_cost_pusd)?;
            if let Some(orphan_fee) = known(&m.unpaired_success_fee_pusd) {
                fee = rational(orphan_fee)?;
            } else {
                unknown |= orphan.iter().any(|q| *q > Decimal::ZERO);
            }
        }
        let mut pending = Vec::new();
        let orders: Vec<_> = input.action_before.orders().collect();
        for c in input.action_before.commitments() {
            let Some((condition0, leg)) =
                limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)
            else {
                continue;
            };
            if condition0 != condition {
                continue;
            }
            let mut is_unknown = false;
            let (q, cost, p) = match &leg.order {
                BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share,
                    size_shares,
                    ..
                }
                | BuyOrderIntentV1::GtdLimit {
                    limit_price_pusd_per_share,
                    size_shares,
                    ..
                } => {
                    let q = match &c.remaining_target_shares {
                        ExecutionReadValueV1::Known(q) => decimal(q)?,
                        _ => {
                            is_unknown = true;
                            *size_shares
                        }
                    };
                    // Maker original post_only cost is exact principal; other LIMIT uses original maximum fee.
                    let post_only = match &leg.order {
                        BuyOrderIntentV1::GtcLimit { post_only, .. }
                        | BuyOrderIntentV1::GtdLimit { post_only, .. } => *post_only,
                        _ => false,
                    };
                    let cost = if post_only {
                        r(q).mul(&r(*limit_price_pusd_per_share))
                    } else {
                        // 已提交LIMIT可能沿旧tick；本金沿原价，费用读取原公共归属。
                        let fee = ord_fee(input, c.order_identity)?;
                        if fee.is_none() {
                            is_unknown = true;
                        }
                        r(q).mul(&r(*limit_price_pusd_per_share))
                            .add(&fee.unwrap_or_else(R::zero))
                    };
                    (r(q), cost, *limit_price_pusd_per_share)
                }
                BuyOrderIntentV1::FakMarket {
                    spend_pusd,
                    worst_price_pusd_per_share,
                }
                | BuyOrderIntentV1::FokMarket {
                    spend_pusd,
                    worst_price_pusd_per_share,
                } => {
                    let b = self
                        .rules
                        .sim_market_buy_bounds_v1(*spend_pusd)
                        .map_err(|e| StrategyErrorV1::Cost(e.to_string()))?;
                    (
                        fraction_units(&b.maximum_base_micro, 6),
                        fraction_units(&b.principal_micro, 6)
                            .add(&fraction_units(&b.maximum_fee_1e5_units, 5)),
                        *worst_price_pusd_per_share,
                    )
                }
            };
            let ord = c
                .order_identity
                .and_then(|id| orders.iter().find(|o| o.order_identity == id));
            pending.push(PendingLeg {
                token: leg.token_id.clone(),
                quantity: q,
                cost,
                price: p,
                order_id: ord.map(platform_id),
                unknown: is_unknown,
            });
        }
        let mut investment = m
            .map(|m| rational(&m.successful_investment_pusd))
            .transpose()?
            .unwrap_or_else(R::zero);
        let pending_bound = pending.iter().fold(R::zero(), |s, p| s.add(&p.cost));
        let projection = input
            .action_before
            .ledger()
            .market_projections
            .iter()
            .find(|p| &p.condition_id == condition);
        let funds = if let Some(p) = projection {
            r(decimal(&p.principal_encumbrance_pusd)?)
                .add(&r(decimal(&p.immediate_taker_fee_encumbrance_pusd)?))
                .add(&r(decimal(&p.local_pending_principal_pusd)?))
                .add(&r(decimal(&p.local_pending_immediate_fee_pusd)?))
                .add(&r(decimal(
                    &p.platform_confirmation_pending_economic_principal_pusd,
                )?))
                .add(&r(decimal(
                    &p.platform_confirmation_pending_economic_immediate_fee_pusd,
                )?))
        } else {
            R::zero()
        };
        investment = investment.add(&pending_bound.clone().max(funds.clone()));
        if let Some(p) = projection {
            // 尚未连接原承诺的预留、或待经济归约的平台确认，不能伪装成已无风险。
            unknown |= (pending.is_empty()
                && (decimal(&p.local_pending_principal_pusd)? > Decimal::ZERO
                    || decimal(&p.local_pending_immediate_fee_pusd)? > Decimal::ZERO))
                || decimal(&p.platform_confirmation_pending_economic_principal_pusd)?
                    > Decimal::ZERO
                || decimal(&p.platform_confirmation_pending_economic_immediate_fee_pusd)?
                    > Decimal::ZERO;
        }

        // 公共资金可能仍保留原最大费等已知保守余量；大小差异不是信息缺失。
        // 余量仍计入投入，并按两侧潜在暴露计入，不能据此释放资金。
        let additional_encumbrance = funds.sub(&pending_bound).max(R::zero());
        for o in orders.iter().filter(|o| o.condition_id == Some(condition)) {
            if o.origin.is_none() {
                unknown = true;
            }
        }
        unknown |= pending.iter().any(|p| p.unknown);
        let exposure = std::array::from_fn(|side| {
            let base = if orphan[side] > Decimal::ZERO {
                principal.add(&fee)
            } else {
                R::zero()
            };
            let base = base.add(&additional_encumbrance);
            if let Some(a) = o.assets.get(side) {
                pending
                    .iter()
                    .filter(|p| p.token == a.asset.token_id)
                    .fold(base, |s, p| s.add(&p.cost))
            } else {
                base
            }
        });
        Ok(MarketState {
            pending,
            orphan,
            orphan_principal: principal,
            orphan_fee: fee,
            investment,
            exposure,
            unknown,
        })
    }
    pub(super) fn limit_cost(
        &self,
        q: Decimal,
        p: Decimal,
        o: &MarketBusinessObservationV1,
        token: &TokenId,
        post_only: bool,
    ) -> Result<R, StrategyErrorV1> {
        let tick = o
            .assets
            .iter()
            .find(|a| &a.asset.token_id == token)
            .and_then(|a| value(&a.tick))
            .ok_or(StrategyErrorV1::Arithmetic("已提交LIMIT的tick读取缺失"))?;
        let td = decimal(tick)?;
        let index = (p / td)
            .to_u16()
            .ok_or(StrategyErrorV1::Arithmetic("价格索引不能编码"))?;
        let n = (q * Decimal::from(100))
            .to_u128()
            .map(BigUint::from)
            .ok_or(StrategyErrorV1::Arithmetic("份额轴不能编码"))?;
        let b = self
            .rules
            .limit_buy_cost_v1(&n, index, tick, post_only)
            .map_err(|e| StrategyErrorV1::Cost(e.to_string()))?;
        Ok(fraction_units(&b.principal_micro, 6).add(&fraction_units(&b.fee_1e5_units, 5)))
    }
}
