use super::*;

impl ComplementaryPairV1 {
    pub(super) fn profit_and_equity(
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
    ) -> Result<(R, ExecutionReadValueV1<ExactRationalV1>), StrategyErrorV1> {
        Self::profit_and_equity_read(&input.action_before)
    }
    pub(super) fn profit_and_equity_read(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
    ) -> Result<(R, ExecutionReadValueV1<ExactRationalV1>), StrategyErrorV1> {
        let (pnl, equity) = read.profit_and_equity();
        Ok((
            pnl,
            match equity {
                ExecutionReadValueV1::Known(e) => ExecutionReadValueV1::Known(e.public()),
                _ => ExecutionReadValueV1::Unavailable,
            },
        ))
    }
    /// 未使用本金沿原场景；成交按原owner生命周期读取，扣款不等于成功配平。
    pub(super) fn remaining_estimate(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
        a: &ProfitAllocationV2,
    ) -> Result<Option<RemainingRepairEstimate>, StrategyErrorV1> {
        super::super::common::remaining_estimate(read, a)
    }
    pub(super) fn profit_occupancy(
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
        a: &ProfitAllocationV2,
    ) -> Result<Option<R>, StrategyErrorV1> {
        Self::profit_occupancy_read(&input.action_before, a)
    }
    pub(super) fn profit_occupancy_read(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
        a: &ProfitAllocationV2,
    ) -> Result<Option<R>, StrategyErrorV1> {
        let Some(remaining) = Self::remaining_estimate(read, a)? else {
            return Ok(None);
        };
        // 本金配平损益到success_applied才进入P；已应用费用只留公共P一次。
        // 原分配是利润资源上限，不是预计亏损或实际风险的上界。
        Ok(Some(
            rational(&a.allocated_profit_pusd)?.min(remaining.loss),
        ))
    }

    pub(super) fn reconcile_profit(
        &mut self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
    ) -> Result<R, StrategyErrorV1> {
        let mut total = R::zero();
        let mut remove = Vec::new();
        for (id, a) in &self.profit_pending {
            if let Some(occupied) = Self::profit_occupancy(input, a)? {
                total = total.add(&occupied);
            } else {
                remove.push(id.clone());
            }
        }
        for id in remove {
            self.profit_pending.remove(&id);
        }
        Ok(total)
    }
    pub(super) fn urgency(
        &self,
        o: &PairObservation<'_>,
        now: i64,
        p: &ProbabilityEstimateV1,
        s: &MarketState,
        cash: Decimal,
        side: Option<usize>,
    ) -> Result<UrgencyStateV1, StrategyErrorV1> {
        let (z, u, age, signal) = match p {
            ProbabilityEstimateV1::Known {
                standardized_distance,
                uncertainty_usd,
                information_age_ms,
                binance_innovation_usd,
                ..
            } => (
                *standardized_distance,
                Some(*uncertainty_usd),
                *information_age_ms,
                Some(*binance_innovation_usd),
            ),
            _ => (None, None, None, None),
        };
        let depth = side
            .and_then(|i| o.assets.get(i))
            .and_then(|a| value(&a.asks))
            .map(|levels| {
                levels.iter().try_fold(Decimal::ZERO, |s, p| {
                    Ok::<_, StrategyErrorV1>(s + decimal(&p.quantity_shares)?)
                })
            })
            .transpose()?;
        Ok(UrgencyStateV1 {
            trading_remaining_ms: o.market.window.end_at_utc_ms - now,
            window_remaining_ms: value(&o.settlement_reference)
                .map(|rule| {
                    (o.market.window.end_at_utc_ms - now)
                        .max(0)
                        .min(probability::window(*rule))
                })
                .unwrap_or(0),
            settlement_window_start_utc_ms: value(&o.settlement_reference)
                .map(|rule| o.market.window.end_at_utc_ms - probability::window(*rule)),
            standardized_distance: z,
            uncertainty_usd: u,
            information_age_ms: age,
            binance_innovation_usd: signal,
            orphan_cost_pusd: s.orphan_principal.add(&s.orphan_fee).public(),
            free_cash_pusd: exact(cash),
            unresolved_commitment_count: s.pending.len(),
            repair_ask_depth_shares: depth.map(exact),
            maker_path_ms: self.config.repair.maker_path_ms,
            active_path_ms: self.config.repair.active_path_ms,
        })
    }
    pub(super) fn price(
        &self,
        a: &AssetBusinessObservationV1,
        target: Decimal,
        best_ask: impl FnOnce() -> Result<Option<Decimal>, StrategyErrorV1>,
    ) -> Result<Option<Decimal>, StrategyErrorV1> {
        let Some(tick) = value(&a.tick) else {
            return Ok(None);
        };
        let tick = decimal(tick)?;
        if tick <= Decimal::ZERO {
            return Err(StrategyErrorV1::Arithmetic("公共tick非正"));
        }
        let mut p = (target.min(Decimal::ONE - tick) / tick).floor() * tick;
        if let Some(ask) = best_ask()? {
            p = p.min(ask - tick);
        }
        if p <= Decimal::ZERO {
            return Ok(None);
        }
        Ok(Some(p))
    }
    pub(super) fn quotes(
        &self,
        o: &PairObservation<'_>,
        p: &ProbabilityEstimateV1,
        s: &MarketState,
    ) -> Result<Option<[Decimal; 2]>, StrategyErrorV1> {
        let Some(up) = p.up() else {
            return Ok(None);
        };
        if o.assets.len() != 2 {
            return Ok(None);
        }
        let uncertainty = match p {
            ProbabilityEstimateV1::Known { up, .. } => fd(up * (1.0 - up))?,
            _ => Decimal::ZERO,
        };
        let signal = match p {
            ProbabilityEstimateV1::Known {
                binance_innovation_usd,
                mean_usd,
                ..
            } if *mean_usd != 0.0 => fd((binance_innovation_usd / mean_usd).abs())?,
            _ => Decimal::ZERO,
        };
        let spread = self.config.quote.base_half_spread
            + self.config.quote.uncertainty_spread_weight * uncertainty
            + self.config.quote.signal_spread_weight * signal;
        let difference = if s.orphan[0] > Decimal::ZERO {
            floor_decimal(&s.orphan_principal, 8)?
        } else {
            -floor_decimal(&s.orphan_principal, 8)?
        };
        let shift = self.config.quote.inventory_shift_per_pusd * difference;
        let up = fd(up)?;
        let (Some(a), Some(b)) = (
            self.price(o.assets[0], up - spread - shift, || o.top_ask(0))?,
            self.price(o.assets[1], Decimal::ONE - up - spread + shift, || {
                o.top_ask(1)
            })?,
        ) else {
            return Ok(None);
        };
        Ok(Some([a, b]))
    }
    /// 仅用于初始配对及一般维护；修复继续调用 quotes()。
    pub(super) fn initial_pair_targets(
        &self,
        o: &PairObservation<'_>,
        p: &ProbabilityEstimateV1,
        s: &MarketState,
    ) -> Result<[Result<Decimal, InitialPairPriceUnavailableV1>; 2], StrategyErrorV1> {
        use InitialPairPriceUnavailableV1 as U;
        if self.config.quote.initial_pair_price_reference == InitialPairPriceReferenceV1::FairValue
        {
            return Ok(match self.quotes(o, p, s)? {
                Some(prices) => prices.map(Ok),
                None => {
                    [Err(if p.up().is_none() {
                        U::EstimateRequired
                    } else {
                        U::NoLegalMakerPrice
                    }); 2]
                }
            });
        }
        let mut targets = [Err(U::MissingPositiveBid); 2];
        for (side, target) in targets.iter_mut().enumerate() {
            let Some(asset) = o.assets.get(side) else {
                continue;
            };
            let Some(levels) = value(&asset.bids) else {
                continue;
            };
            let mut bid = None;
            for level in levels {
                if decimal(&level.quantity_shares)? > Decimal::ZERO {
                    let price = decimal(&level.price_pusd_per_share)?;
                    bid = Some(bid.map_or(price, |old: Decimal| old.max(price)));
                }
            }
            if let Some(bid) = bid {
                *target = self
                    .price(asset, bid, || o.top_ask(side))?
                    .ok_or(U::NoLegalMakerPrice);
            }
        }
        Ok(targets)
    }
    pub(super) fn loss_allowance(
        &self,
        o: &MarketBusinessObservationV1,
        now: i64,
        s: &MarketState,
    ) -> Result<Decimal, StrategyErrorV1> {
        super::super::common::loss_allowance_v1(
            &self.config.repair,
            self.config.risk.per_market_orphan_limit_pusd,
            &s.orphan_principal.add(&s.orphan_fee),
            o.market.window.start_at_utc_ms,
            o.market.window.end_at_utc_ms,
            now,
        )
    }
    pub(super) fn cancel_available(
        &self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
        ids: Vec<String>,
    ) -> Vec<String> {
        ids.into_iter()
            .filter(|id| {
                let scope = CancellationScopeV1::Orders(vec![id.as_str()]);
                matches!(
                    input.action_before.pending_cancellation_count(Some(&scope)),
                    ExecutionReadValueV1::Known(0)
                )
            })
            .collect()
    }
    pub(super) fn order_item(
        &mut self,
        condition: &ConditionId,
        legs: Vec<(BuyLegIntentV1, R, R)>,
    ) -> Result<ReservedIntentV1, StrategyErrorV1> {
        super::super::common::reserved_buy_item(self.id()?, condition, legs)
    }
    pub(super) fn maker_leg(
        &self,
        a: &AssetBusinessObservationV1,
        q: Decimal,
        p: Decimal,
    ) -> BuyLegIntentV1 {
        BuyLegIntentV1 {
            token_id: a.asset.token_id.clone(),
            order: BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: p,
                size_shares: q,
                post_only: true,
            },
        }
    }
    pub(super) fn limit_budget(&self, cash: Decimal, s: &MarketState, cost: &R) -> bool {
        cost <= &r(cash) && s.investment.add(cost) <= r(Decimal::from(1000))
    }
}

#[cfg(test)]
#[path = "../../../tests/support/profit_lifecycle.rs"]
mod tests;
