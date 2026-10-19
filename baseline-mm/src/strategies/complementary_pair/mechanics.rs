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
        use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1 as D;
        let original_q = rational(&a.estimated_shares)?;
        let original_p = rational(&a.estimated_principal_pusd)?;
        let original_f = rational(&a.estimated_fee_pusd)?;
        let held_q = rational(&a.original_orphan_shares)?;
        let held_unit = rational(&a.original_orphan_principal_per_share)?;
        let loss = |q: &R, principal: &R, fees: &R, success: &R| {
            let paired = q.clone().min(held_q.sub(success).max(R::zero()));
            let paired_principal = if q.is_positive() {
                principal.mul(&paired).divided_by(q)
            } else {
                R::zero()
            };
            held_unit
                .mul(&paired)
                .add(&paired_principal)
                .sub(&paired)
                .add(fees)
                .max(R::zero())
        };
        let ExecutionReadValueV1::Known(trades) =
            read.request_trade_economics(&a.global_request_id)
        else {
            return Ok(Some(RemainingRepairEstimate {
                shares: original_q.clone(),
                principal: original_p.clone(),
                fee: original_f.clone(),
                loss: loss(&original_q, &original_p, &original_f, &R::zero()),
                unknown: true,
            }));
        };
        let active_local = read.request_funds().any(|f| {
            f.request_identity == &a.reserve_identity
                && !matches!(
                    f.reservation_state,
                    ReservationStatusV1::Converted | ReservationStatusV1::Released
                )
        });
        let commitments: Vec<_> = read
            .commitments()
            .filter(|c| c.origin.intent.global_request_id() == a.global_request_id)
            .collect();
        let mut success = R::zero();
        let mut pending_q = R::zero();
        let mut pending_p = R::zero();
        let mut pending_fee = R::zero();
        let mut pairing_q = R::zero();
        let mut pairing_p = R::zero();
        let mut unapplied_fee = R::zero();
        for t in &trades {
            let q = r(decimal(&t.fact.base_quantity_shares)?);
            let p = r(decimal(&t.fact.quote_amount_pusd)?);
            if t.success_applied {
                success = success.add(&q);
                continue;
            }
            let failed = matches!(
                t.fact.economic_disposition,
                D::FinalFailureUnapplied | D::FinalFailureCompensated
            );
            if failed && !t.economic_applied {
                continue;
            }
            let fee = r(decimal(if t.economic_applied {
                &t.actual_net_fee_pusd
            } else {
                &t.fact.immediate_taker_fee_pusd
            })?);
            pending_q = pending_q.add(&q);
            pending_p = pending_p.add(&p);
            pending_fee = pending_fee.add(&fee);
            if !failed {
                pairing_q = pairing_q.add(&q);
                pairing_p = pairing_p.add(&p);
                if !t.economic_applied {
                    unapplied_fee = unapplied_fee.add(&fee);
                }
            }
        }
        let mut unused_ratio = R::zero();
        let mut unknown = false;
        if active_local {
            let matched = trades.iter().try_fold(R::zero(), |sum, t| {
                Ok::<_, StrategyErrorV1>(sum.add(&r(decimal(&t.fact.quote_amount_pusd)?)))
            })?;
            unused_ratio = original_p
                .sub(&matched)
                .max(R::zero())
                .divided_by(&original_p);
        } else {
            for c in commitments {
                let amount = limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)
                    .is_some_and(|(_, l)| {
                        matches!(
                            l.order,
                            BuyOrderIntentV1::FakMarket { .. } | BuyOrderIntentV1::FokMarket { .. }
                        )
                    });
                let related: Vec<_> = trades
                    .iter()
                    .filter(|t| Some(t.fact.order_identity.clone()).as_ref() == c.order_identity)
                    .collect();
                let nonterminal = |t: &&TradeEconomicReadV1<'_>| {
                    !matches!(
                        t.fact.economic_disposition,
                        D::FinalSuccess | D::FinalFailureUnapplied | D::FinalFailureCompensated
                    )
                };
                let ratio = if amount {
                    let matched_unapplied = related
                        .iter()
                        .copied()
                        .filter(nonterminal)
                        .filter(|t| !t.economic_applied)
                        .try_fold(R::zero(), |s, t| {
                            Ok::<_, StrategyErrorV1>(s.add(&r(decimal(&t.fact.quote_amount_pusd)?)))
                        })?;
                    match c
                        .order_identity
                        .and_then(|id| read.order_funds().find(|f| f.order_identity == id))
                        .and_then(|f| known(&f.principal_encumbrance_pusd).cloned())
                    {
                        Some(p) => r(decimal(&p)?)
                            .sub(&matched_unapplied)
                            .max(R::zero())
                            .divided_by(&original_p),
                        None => {
                            unknown = true;
                            r(Decimal::ONE)
                        }
                    }
                } else {
                    let pending = related.iter().copied().filter(nonterminal).try_fold(
                        R::zero(),
                        |s, t| {
                            Ok::<_, StrategyErrorV1>(
                                s.add(&r(decimal(&t.fact.base_quantity_shares)?)),
                            )
                        },
                    )?;
                    match known(&c.remaining_target_shares) {
                        Some(q) => r(decimal(q)?)
                            .sub(&pending)
                            .max(R::zero())
                            .divided_by(&original_q),
                        None => {
                            unknown = true;
                            r(Decimal::ONE)
                        }
                    }
                };
                unused_ratio = unused_ratio.add(&ratio);
            }
        }
        let unused_ratio = unused_ratio.min(r(Decimal::ONE)).max(R::zero());
        let unused_q = original_q.mul(&unused_ratio);
        let unused_p = original_p.mul(&unused_ratio);
        let unused_f = original_f.mul(&unused_ratio);
        let shares = unused_q.add(&pending_q);
        let principal = unused_p.add(&pending_p);
        let fee = unused_f.add(&pending_fee);
        if !active_local && shares.is_zero() && principal.is_zero() && fee.is_zero() {
            return Ok(None);
        }
        Ok(Some(RemainingRepairEstimate {
            shares,
            principal,
            fee,
            unknown,
            loss: loss(
                &unused_q.add(&pairing_q),
                &unused_p.add(&pairing_p),
                &unused_f.add(&unapplied_fee),
                &success,
            ),
        }))
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
        let duration = (o.market.window.end_at_utc_ms - o.market.window.start_at_utc_ms).max(1);
        let elapsed = Decimal::from((now - o.market.window.start_at_utc_ms).clamp(0, duration))
            / Decimal::from(duration);
        let pressure = if self.config.risk.per_market_orphan_limit_pusd > Decimal::ZERO {
            floor_decimal(&s.orphan_principal.add(&s.orphan_fee), 8)?
                / self.config.risk.per_market_orphan_limit_pusd
        } else {
            Decimal::ZERO
        };
        Ok(self.config.repair.base_loss_allowance_per_share
            + self.config.repair.time_loss_allowance_per_share * elapsed
            + self.config.repair.inventory_loss_allowance_per_share * pressure)
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
        let id = self.id()?;
        let reserves = legs
            .iter()
            .enumerate()
            .map(|(i, (_, principal, fee))| {
                Ok(IntentReserveV1 {
                    batch_item_ordinal: i as u16,
                    reserve: ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1(format!("{id}:{i}")),
                        condition_id: condition.clone(),
                        requested_principal_pusd: exact(floor_decimal(principal, 6)?),
                        immediate_fee_reserve_pusd: exact(floor_decimal(fee, 5)?),
                    },
                })
            })
            .collect::<Result<Vec<_>, StrategyErrorV1>>()?;
        let intent = if legs.len() == 2 {
            ModelIntentV1::SubmitPairedBuy {
                condition_id: condition.clone(),
                orders: [legs[0].0.clone(), legs[1].0.clone()],
            }
        } else {
            ModelIntentV1::SubmitSingleBuy {
                condition_id: condition.clone(),
                order: legs[0].0.clone(),
            }
        };
        Ok(ReservedIntentV1 {
            context: RequestContextV2 {
                global_request_id: id,
                caller_correlation_id: None,
            },
            intent: Arc::new(intent),
            reserves,
        })
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
