use super::*;

pub(super) struct DecisionReads<'a> {
    pub(super) orders: Vec<
        ExecutionOrderRead<'a, contracts::execution_normalization::CancellationRequestCompletedV1>,
    >,
    pub(super) orders_by_id:
        BTreeMap<&'a ::execution_normalization::MemoryLogicalIdentityV1, usize>,
    pub(super) orders_by_market: BTreeMap<&'a str, Vec<usize>>,
    pub(super) order_fees:
        BTreeMap<&'a ::execution_normalization::MemoryLogicalIdentityV1, Option<R>>,
    pub(super) commitments: BTreeMap<&'a str, Vec<IntentCommitmentReadV1<'a>>>,
}
impl<'a> DecisionReads<'a> {
    pub(super) fn new(
        read: &'a ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
    ) -> Self {
        let orders: Vec<_> = read.orders().collect();
        let orders_by_id = orders
            .iter()
            .enumerate()
            .map(|(i, o)| (o.order_identity, i))
            .collect();
        let mut orders_by_market: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (i, o) in orders.iter().enumerate() {
            if let Some(id) = o.condition_id {
                orders_by_market.entry(id.0.as_str()).or_default().push(i);
            }
        }
        let mut commitments: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for c in read.commitments() {
            if let Some((id, _)) = limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)
            {
                commitments.entry(id.0.as_str()).or_default().push(c);
            }
        }
        let order_fees = read
            .order_funds()
            .map(|f| {
                (
                    f.order_identity,
                    known(&f.immediate_fee_encumbrance_pusd)
                        .map(|q| r(Decimal::from_str_exact(&q.0).expect("原费用"))),
                )
            })
            .collect();
        Self {
            orders,
            orders_by_id,
            orders_by_market,
            commitments,
            order_fees,
        }
    }
}
impl ComplementaryPairV1 {
    pub(super) fn state(
        &self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
        f: &PairMarketRead<'_>,
        reads: &DecisionReads<'_>,
    ) -> Result<MarketState, StrategyErrorV1> {
        let o = &f.observation;
        let condition = &o.market.condition_id;
        let m = input.action_before.market_economics(condition);
        let mut orphan = [Decimal::ZERO; 2];
        let mut principal = R::zero();
        let mut fee = R::zero();
        let mut unknown = false;
        if let Some(m) = &m
            && m.unresolved
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
                orphan[side] = m.orphan_shares[i];
            }
            principal = m.orphan_principal.clone();
            if let Some(orphan_fee) = known(&m.orphan_fee) {
                fee = orphan_fee.clone();
            } else {
                unknown |= orphan.iter().any(|q| *q > Decimal::ZERO);
            }
        }
        let mut pending = Vec::new();
        for c in reads
            .commitments
            .get(condition.0.as_str())
            .into_iter()
            .flatten()
        {
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
                        let fee = c
                            .order_identity
                            .and_then(|id| reads.order_fees.get(id))
                            .cloned()
                            .flatten();
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
                    let estimate = self
                        .profit_pending
                        .get(c.origin.intent.global_request_id())
                        .map(|a| Self::remaining_estimate(&input.action_before, a))
                        .transpose()?
                        .flatten();
                    match estimate {
                        Some(e) => {
                            is_unknown |= e.unknown;
                            (
                                e.shares,
                                e.principal.add(&e.fee),
                                *worst_price_pusd_per_share,
                            )
                        }
                        None => {
                            is_unknown = true;
                            (R::zero(), r(*spend_pusd), *worst_price_pusd_per_share)
                        }
                    }
                }
            };
            let ord = c
                .order_identity
                .and_then(|id| reads.orders_by_id.get(id).map(|i| &reads.orders[*i]));
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
            .as_ref()
            .map(|m| m.successful_investment.clone())
            .unwrap_or_else(R::zero);
        let pending_bound = pending.iter().fold(R::zero(), |s, p| s.add(&p.cost));
        let projection = input.action_before.market_funds(condition);
        let funds = projection.total();
        investment = investment.add(&pending_bound.clone().max(funds.clone()));
        unknown |= pending.is_empty()
            && (projection.local_principal > Decimal::ZERO || projection.local_fee > Decimal::ZERO);

        // 公共资金可能仍保留原最大费等已知保守余量；大小差异不是信息缺失。
        // 余量仍计入投入，并按两侧潜在暴露计入，不能据此释放资金。
        let additional_encumbrance = funds.sub(&pending_bound).max(R::zero());
        for o in reads
            .orders_by_market
            .get(condition.0.as_str())
            .into_iter()
            .flatten()
            .map(|i| &reads.orders[*i])
        {
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
