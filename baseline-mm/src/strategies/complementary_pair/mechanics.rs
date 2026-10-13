use super::*;

impl ComplementaryPairV1 {
    pub(super) fn profit_and_equity(
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV1,
        >,
    ) -> Result<(R, ExecutionReadValueV1<ExactRationalV1>), StrategyErrorV1> {
        Self::profit_and_equity_read(&input.action_before)
    }
    pub(super) fn profit_and_equity_read(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV1,
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
    pub(super) fn profit_bound(
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV1,
        >,
        reads: &DecisionReads<'_>,
        a: &ProfitAllocationV1,
    ) -> Result<Option<R>, StrategyErrorV1> {
        Self::profit_bound_with_remaining(
            &input.action_before,
            a,
            reads.request_commitments.get(a.global_request_id.as_str()),
        )
    }
    pub(super) fn profit_bound_read(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV1,
        >,
        a: &ProfitAllocationV1,
    ) -> Result<Option<R>, StrategyErrorV1> {
        let mut remaining = R::zero();
        let mut has_commitment = false;
        let mut unknown = false;
        for c in read
            .commitments()
            .filter(|c| c.origin.intent.global_request_id() == a.global_request_id)
        {
            has_commitment = true;
            if a.original_shares.is_some() {
                if let Some(q) = known(&c.remaining_target_shares) {
                    remaining = remaining.add(&r(decimal(q)?));
                } else {
                    unknown = true;
                }
            } else {
                unknown = true;
            }
        }
        Self::profit_bound_with_remaining(read, a, has_commitment.then_some(&(remaining, unknown)))
    }
    fn profit_bound_with_remaining(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV1,
        >,
        a: &ProfitAllocationV1,
        commitment: Option<&(R, bool)>,
    ) -> Result<Option<R>, StrategyErrorV1> {
        let has_commitment = commitment.is_some();
        let (remaining, unknown) = commitment
            .map(|(q, u)| (q.clone(), *u || a.original_shares.is_none()))
            .unwrap_or_default();
        let active_request = read.request_funds().any(|r| {
            r.request_identity == &a.reserve_identity
                && !matches!(
                    r.reservation_state,
                    ReservationStatusV1::Converted | ReservationStatusV1::Released
                )
        });
        if !has_commitment && !active_request && read.pending_noe_record_count() == 0 {
            return Ok(None);
        }
        let original = rational(&a.allocated_profit_pusd)?;
        let bound = if unknown || active_request || read.pending_noe_record_count() > 0 {
            original.clone()
        } else {
            // 已应用本金配平亏损已在公共P中；只保留剩余份额的未决增量。
            remaining
                .mul(&rational(&a.incremental_loss_per_share)?)
                .add(&rational(&a.fee_bound_pusd)?)
        };
        Ok(Some(original.min(bound)))
    }
    pub(super) fn reconcile_profit(
        &mut self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV1,
        >,
        reads: &DecisionReads<'_>,
    ) -> Result<R, StrategyErrorV1> {
        let mut total = R::zero();
        let mut remove = Vec::new();
        for (id, a) in &self.profit_pending {
            if let Some(bound) = Self::profit_bound(input, reads, a)? {
                total = total.add(&bound);
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
            impl ActionAccountReadV1,
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
