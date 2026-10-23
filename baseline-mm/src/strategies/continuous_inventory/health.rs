use super::*;
use ::execution_normalization::{MemoryIdentityKeyV1, SourceNeutralEntityKeyV1};
use sim_system::public_consumption::PublicObservationFrameV1;

fn invalid(code: &str, scopes: Vec<SourceStreamScopeV1>) -> IntentQualificationV1 {
    IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
        reason_code: code.into(),
        scopes,
    })
}
impl ContinuousInventoryV1 {
    pub(super) fn normal_sources(
        &self,
        o: &MarketBusinessObservationV1,
    ) -> Vec<SourceStreamScopeV1> {
        normal_health_sources(&self.config.probability_reference, o)
    }

    pub(super) fn repair_sources(
        &self,
        o: &MarketBusinessObservationV1,
        _side: usize,
    ) -> Vec<SourceStreamScopeV1> {
        o.assets
            .iter()
            .map(|a| SourceStreamScopeV1 {
                source: SourceStreamKindV1::PolymarketBook,
                condition_id: Some(o.market.condition_id.clone()),
                asset_or_symbol: a.asset.token_id.0.clone(),
            })
            .collect()
    }
    pub(super) fn health_states(
        &self,
        o: &MarketBusinessObservationV1,
        scopes: &[SourceStreamScopeV1],
        at: i64,
    ) -> Vec<SourceHealthStatusV1> {
        scopes
            .iter()
            .map(|scope| SourceHealthStatusV1 {
                scope: scope.clone(),
                state: source_health_state_v1(
                    o.source_progress
                        .get_scope(scope)
                        .map(SourceHealthFactsV1::from)
                        .as_ref(),
                    self.config.health.limits(scope.source),
                    at,
                ),
            })
            .collect()
    }
    pub(super) fn independent_repair_allowed(
        &self,
        o: &MarketBusinessObservationV1,
        statistics: Option<&PublicMarketStatisticsV1>,
        state: &InventoryState,
        side: usize,
        price: Option<Decimal>,
        now: i64,
    ) -> Result<bool, StrategyErrorV1> {
        if state.h[1 - side] <= Decimal::ZERO
            || state.unknown
            || price.is_none()
            || self
                .health_states(o, &self.repair_sources(o, side), now)
                .iter()
                .any(|h| h.state.failure().is_some())
        {
            return Ok(false);
        }
        let Some(assets) = pair_assets(o) else {
            return Ok(false);
        };
        let mark = value(&assets[1 - side].valuation_best_bid)
            .map(|q| q.as_ref().map(decimal).transpose())
            .transpose()?
            .map(|q| q.unwrap_or(Decimal::ZERO));
        if mark.is_none() {
            return Ok(false);
        }
        if !self.continuity.terminating {
            return Ok(true);
        }
        if o.market.window.end_at_utc_ms - now
            <= i64::try_from(self.config.repair.maker_path_ms).unwrap_or(i64::MAX)
        {
            return Ok(false);
        }
        // 原终止选择使用实际估计或允许的持有腿标记价后备；c 不参与该估计。
        let sources = probability_sources(&self.config.probability_reference, o);
        let p = if self
            .health_states(o, &sources, now)
            .iter()
            .all(|h| h.state.failure().is_none())
        {
            public_probability::estimate(o, statistics, &self.config.probability_reference)?
                .0
                .up()
        } else {
            None
        };
        let basis = p
            .map(|p| if side == 0 { p } else { 1.0 - p })
            .unwrap_or(1.0 - float(&mark.expect("原独立标记"))?);
        let unit = state
            .principal
            .add(&state.fee)
            .divided_by(&r(state.h[1 - side]));
        let (retain, wait, maker) = maker_repair_values_v1(
            &self.config.repair,
            basis,
            price,
            &unit,
            &state.principal.add(&state.fee),
            self.config.per_market_orphan_limit_pusd,
            o.market.window.start_at_utc_ms,
            o.market.window.end_at_utc_ms,
            now,
        )?;
        Ok(maker.is_some_and(|m| m >= wait && m >= retain))
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn cancel_invalid_orders(
        &mut self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl NecessaryActionAccountReadV2,
        >,
        o: &MarketBusinessObservationV1,
        state: &InventoryState,
        _prices: &[Option<Decimal>; 2],
        restricted: bool,
        repair_side: Option<usize>,
        allowance: &R,
        _profit: &R,
        intents: &mut Vec<ReservedIntentV1>,
        d: &mut InventoryMarketDiagnosticV1,
    ) -> Result<(), StrategyErrorV1> {
        let Some(assets) = pair_assets(o) else {
            return Ok(());
        };
        let mut remaining_repair = repair_side.map_or(Decimal::ZERO, |side| {
            (state.h[1 - side] - state.t[side]).max(Decimal::ZERO)
        });
        let mut cancel = Vec::new();
        for order in input
            .action_before
            .orders()
            .filter(|order| order.condition_id == Some(&o.market.condition_id))
        {
            if order.cancellable != ExecutionReadValueV1::Known(true) {
                continue;
            }
            let Some(side) = order
                .token_id
                .and_then(|token| assets.iter().position(|a| &a.asset.token_id == token))
            else {
                continue;
            };
            let MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) =
                &order.order_identity.key
            else {
                continue;
            };
            let mut reason = None;
            let q = known(&order.active_unfilled_shares)
                .map(decimal)
                .transpose()?;
            let price = order
                .origin
                .as_ref()
                .and_then(|origin| limit_leg(origin.intent.intent(), origin.batch_item_ordinal))
                .and_then(|(_, leg)| match leg.order {
                    BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share,
                        ..
                    }
                    | BuyOrderIntentV1::GtdLimit {
                        limit_price_pusd_per_share,
                        ..
                    } => Some(limit_price_pusd_per_share),
                    _ => None,
                });
            if value(&assets[side].tick)
                .map(decimal)
                .transpose()?
                .is_none_or(|tick| {
                    tick <= Decimal::ZERO
                        || price.is_none_or(|price| {
                            price <= Decimal::ZERO
                                || price >= Decimal::ONE
                                || price % tick != Decimal::ZERO
                        })
                })
            {
                reason = Some("formal_tick_rule_unavailable_or_invalid");
            }
            let mut cost_valid = true;
            if state.h[1 - side] > Decimal::ZERO {
                let unit = state
                    .principal
                    .add(&state.fee)
                    .divided_by(&r(state.h[1 - side]));
                let support = order
                    .origin
                    .as_ref()
                    .and_then(|origin| {
                        self.continuity
                            .unresolved_profit_allocations
                            .get(origin.intent.global_request_id())
                    })
                    .filter(|allocation| {
                        allocation.condition_id == o.market.condition_id
                            && allocation.token_id == assets[side].asset.token_id
                    })
                    .map(|allocation| {
                        remaining_inventory_estimate(&input.action_before, allocation).map(|e| {
                            e.map(|e| {
                                e.loss.min(
                                    rational(&allocation.allocated_profit_pusd)
                                        .expect("原精确分配"),
                                )
                            })
                            .unwrap_or_default()
                        })
                    })
                    .transpose()?
                    .unwrap_or_default();
                let paired = q.map(|q| q.min(state.h[1 - side]));
                let cap = r(Decimal::ONE).add(allowance).sub(&unit).add(&if paired
                    .is_some_and(|q| q > Decimal::ZERO)
                {
                    support.divided_by(&r(paired.unwrap()))
                } else {
                    R::zero()
                });
                cost_valid = price.is_some_and(|p| r(p) <= cap);
                if !cost_valid {
                    reason = Some("successful_inventory_hard_cost");
                }
            }
            if restricted {
                if repair_side == Some(side)
                    && cost_valid
                    && q.is_some_and(|q| q <= remaining_repair)
                {
                    remaining_repair -= q.unwrap();
                } else {
                    reason = Some("restricted_risk_order");
                }
            }
            if let Some(reason) = reason {
                let scope = CancellationScopeV1::Orders(vec![source_order_id]);
                if matches!(
                    input.action_before.pending_cancellation_count(Some(&scope)),
                    ExecutionReadValueV1::Known(0)
                ) {
                    cancel.push(source_order_id.clone());
                    d.cancellations
                        .push((source_order_id.clone(), reason.into()));
                }
            }
        }
        if !cancel.is_empty() {
            intents.push(ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: self.id()?,
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::CancelOrders {
                    platform_order_ids: cancel,
                }),
                reserves: vec![],
            });
        }
        Ok(())
    }
    fn public_qualification(
        &self,
        o: &MarketBusinessObservationV1,
        item: &ReservedIntentV1,
        dependency: &InventoryActionDependencyV1,
        now: i64,
    ) -> Result<IntentQualificationV1, StrategyErrorV1> {
        if now < o.market.window.start_at_utc_ms
            || now >= o.market.window.end_at_utc_ms
            || value(&o.accepting_orders) != Some(&true)
            || value(&o.closed) == Some(&true)
        {
            return Ok(invalid("market_not_trading", dependency.sources.clone()));
        }
        let issues: Vec<_> = self
            .health_states(o, &dependency.sources, now)
            .into_iter()
            .filter(|s| s.state.failure().is_some())
            .map(|s| s.scope)
            .collect();
        if !issues.is_empty() {
            return Ok(invalid("source_health", issues));
        }
        let Some(assets) = pair_assets(o) else {
            return Ok(invalid("materialized_book_unavailable", vec![]));
        };
        for ordinal in 0..2 {
            if let Some((_, leg)) = limit_leg(&item.intent, ordinal) {
                let Some(asset) = assets.iter().find(|a| a.asset.token_id == leg.token_id) else {
                    return Ok(invalid("asset_unavailable", vec![]));
                };
                let Some(book) = book(asset)? else {
                    return Ok(invalid("materialized_book_unavailable", vec![]));
                };
                let BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: price,
                    ..
                } = leg.order
                else {
                    return Ok(invalid("maker_order_required", vec![]));
                };
                if price <= Decimal::ZERO
                    || price >= Decimal::ONE
                    || price % book.tick != Decimal::ZERO
                    || price > book.ask - book.tick
                {
                    return Ok(invalid("frozen_price_no_longer_legal", vec![]));
                }
            }
        }
        if let Some(held) = &dependency.independent_held_token {
            if assets
                .iter()
                .find(|a| &a.asset.token_id == held)
                .and_then(|a| value(&a.valuation_best_bid))
                .is_none()
            {
                return Ok(invalid("independent_held_mark_unavailable", vec![]));
            }
        }
        Ok(IntentQualificationV1::Qualified)
    }
    fn qualify_current(
        &self,
        input: &StrategyCurrentReadV1<
            '_,
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl NecessaryActionAccountReadV2,
        >,
        calculation: &StrategyCalculationV1<ContinuousInventoryDiagnosticV1>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, StrategyErrorV1> {
        let StrategyOutputV1::Continue {
            intents,
            diagnostics,
            ..
        } = &calculation.output
        else {
            return Ok(IntentQualificationV1::Qualified);
        };
        let item = &intents[index];
        if matches!(item.intent.as_ref(), ModelIntentV1::CancelOrders { .. }) {
            return Ok(IntentQualificationV1::Qualified);
        }
        let Some(dependency) = diagnostics
            .action_dependencies
            .iter()
            .find(|d| d.request_id == item.context.global_request_id)
        else {
            return Ok(invalid("action_dependency_unavailable", vec![]));
        };
        let Some(frame) = input
            .markets
            .iter()
            .find(|f| f.observation.market.condition_id == dependency.condition_id)
        else {
            return Ok(invalid("market_unavailable", vec![]));
        };
        let public = self.public_qualification(frame.observation, item, dependency, at)?;
        if matches!(public, IntentQualificationV1::Invalidated(_)) {
            return Ok(public);
        }
        let (_, equity) = input.action_before.profit_and_equity();
        if dependency.independent_held_token.is_none()
            && !matches!(&equity, ExecutionReadValueV1::Known(_))
        {
            return Ok(invalid("account_equity_unavailable", vec![]));
        }
        if dependency.independent_held_token.is_none()
            && let ExecutionReadValueV1::Known(equity) = equity
        {
            let high = rational(&self.continuity.high_water_pusd)?;
            if equity <= high.mul(&r(Decimal::ONE - self.config.drawdown_fraction)) {
                return Ok(invalid("confirmed_drawdown_termination", vec![]));
            }
        }
        let batch: BTreeSet<_> = intents
            .iter()
            .map(|i| i.context.global_request_id.as_str())
            .collect();
        let reserve_ids: BTreeSet<_> = intents
            .iter()
            .flat_map(|i| &i.reserves)
            .map(|r| &r.reserve.request_identity)
            .collect();
        let requests = input.action_before.request_funds().collect::<Vec<_>>();
        let relevant = |id: &ConditionId| {
            dependency.independent_held_token.is_none() || *id == dependency.condition_id
        };
        let current_external = requests
            .iter()
            .filter(|f| !reserve_ids.contains(f.request_identity) && relevant(f.condition_id))
            .map(|f| InventoryReservationDependencyV1 {
                request_identity: f.request_identity.clone(),
                condition_id: f.condition_id.clone(),
                state: f.reservation_state,
                principal: f.principal_pusd.clone(),
                fee: f.immediate_fee_pusd.clone(),
                ledger_entry_sequence: f.last_ledger_entry_seq,
            })
            .collect::<Vec<_>>();
        let original_external = diagnostics
            .reservations
            .iter()
            .filter(|f| !reserve_ids.contains(&f.request_identity) && relevant(&f.condition_id))
            .cloned()
            .collect::<Vec<_>>();
        if current_external != original_external {
            return Ok(invalid("account_reservation_facts_changed", vec![]));
        }
        for condition in input
            .action_before
            .risk_market_ids()
            .into_iter()
            .filter(relevant)
        {
            if !diagnostics
                .account_dependencies
                .iter()
                .any(|d| d.condition_id == condition)
            {
                return Ok(invalid("account_market_obligation_changed", vec![]));
            }
        }
        for original in diagnostics
            .account_dependencies
            .iter()
            .filter(|d| relevant(&d.condition_id))
        {
            let current = inventory(
                &input.action_before,
                &original.condition_id,
                &original.tokens,
            )?;
            let obligations = current
                .obligations
                .iter()
                .filter(|o| {
                    !o.diagnostic
                        .request_id
                        .as_deref()
                        .is_some_and(|id| batch.contains(id))
                })
                .map(|o| o.diagnostic.clone())
                .collect::<Vec<_>>();
            if current.h.map(exact) != original.h
                || current.principal.add(&current.fee).public() != original.orphan_cost
                || obligations != original.obligations
            {
                return Ok(invalid("action_business_facts_changed", vec![]));
            }
            // 原 owner 的本批资金从当前读取中剔除一次；其余市场/请求的义务必须与冻结相符。
            let mut batch_funds = R::zero();
            for r0 in requests.iter().filter(|r0| {
                r0.condition_id == &original.condition_id
                    && reserve_ids.contains(r0.request_identity)
            }) {
                batch_funds = batch_funds
                    .add(&r(decimal(&r0.principal_pusd)?))
                    .add(&r(decimal(&r0.immediate_fee_pusd)?));
            }
            for order in input
                .action_before
                .obligation_orders()
                .filter(|o| o.condition_id == Some(&original.condition_id))
            {
                let Some(origin) = order.origin else {
                    continue;
                };
                if !batch.contains(origin.intent.global_request_id()) {
                    continue;
                }
                let reserve = intents
                    .iter()
                    .find(|i| i.context.global_request_id == origin.intent.global_request_id())
                    .and_then(|i| {
                        i.reserves
                            .iter()
                            .find(|r| r.batch_item_ordinal == origin.batch_item_ordinal)
                    });
                let local = reserve.and_then(|r| {
                    requests
                        .iter()
                        .find(|f| f.request_identity == &r.reserve.request_identity)
                });
                if local.is_some_and(|r| {
                    !matches!(
                        r.reservation_state,
                        ReservationStatusV1::Converted | ReservationStatusV1::Released
                    )
                }) {
                    continue;
                }
                let Some(f) = input
                    .action_before
                    .order_funds()
                    .find(|f| f.order_identity == order.order_identity)
                else {
                    return Ok(invalid("batch_order_funds_unavailable", vec![]));
                };
                let (Some(p), Some(fee)) = (
                    known(&f.principal_encumbrance_pusd),
                    known(&f.immediate_fee_encumbrance_pusd),
                ) else {
                    return Ok(invalid("batch_order_funds_unavailable", vec![]));
                };
                batch_funds = batch_funds.add(&r(decimal(p)?)).add(&r(decimal(fee)?));
            }
            if current.funds.sub(&batch_funds).public() != original.funds {
                return Ok(invalid("account_funds_obligation_changed", vec![]));
            }
        }
        let (pnl, _) = input.action_before.profit_and_equity();
        let mut allocations = self.continuity.unresolved_profit_allocations.clone();
        let mut used =
            remaining_inventory_profit_occupancy(&input.action_before, &mut allocations)?;
        for allocation in &diagnostics.profit_allocations {
            if !allocations.contains_key(&allocation.global_request_id) {
                used = used.add(
                    &remaining_inventory_estimate(&input.action_before, allocation)?
                        .map(|e| {
                            e.loss.min(
                                rational(&allocation.allocated_profit_pusd).expect("原精确分配"),
                            )
                        })
                        .unwrap_or(rational(&allocation.allocated_profit_pusd)?),
                );
            }
        }
        if pnl.max(R::zero()) < used {
            return Ok(invalid("repair_profit_support_unavailable", vec![]));
        }
        Ok(IntentQualificationV1::Qualified)
    }
    fn record_allocations(
        &mut self,
        calculation: &StrategyCalculationV1<ContinuousInventoryDiagnosticV1>,
        sent: &[SentIntentReceiptV1],
        uncertain: Option<usize>,
    ) {
        let StrategyOutputV1::Continue {
            intents,
            diagnostics,
            ..
        } = &calculation.output
        else {
            return;
        };
        for allocation in &diagnostics.profit_allocations {
            if sent
                .iter()
                .any(|s| s.context.global_request_id == allocation.global_request_id)
                || uncertain
                    .and_then(|i| intents.get(i))
                    .is_some_and(|i| i.context.global_request_id == allocation.global_request_id)
            {
                self.continuity
                    .unresolved_profit_allocations
                    .insert(allocation.global_request_id.clone(), allocation.clone());
            }
        }
    }
}

impl StrategyV1 for ContinuousInventoryV1 {
    type Error = StrategyErrorV1;
    type Diagnostics = ContinuousInventoryDiagnosticV1;
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        self.decide_input(input, at)
    }
    fn decide_borrowed(
        &mut self,
        input: &BorrowedStrategyInputV1<'_>,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        self.decide_input(input, at)
    }
    fn uses_borrowed_input(&self) -> bool {
        true
    }
    fn selects_relevant_markets(&self) -> bool {
        true
    }
    fn requires_trade_economics_details(&self) -> bool {
        false
    }
    fn uses_source_health(&self) -> bool {
        true
    }
    fn observation_requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1 {
            binance_depth_quantities: self
                .config
                .probability_reference
                .binance_pressure_weight_usd
                != 0.0
                || self
                    .config
                    .probability_reference
                    .binance_trade_pressure_weight_usd
                    != 0.0,
        }
    }
    fn historical_market_requirements(&self) -> Vec<ConditionId> {
        self.continuity
            .repair_market_ends
            .keys()
            .cloned()
            .chain(
                self.continuity
                    .unresolved_profit_allocations
                    .values()
                    .map(|a| a.condition_id.0.clone()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(ConditionId)
            .collect()
    }
    fn calculation_market_requirements(
        &self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
    ) -> Vec<ConditionId> {
        match &calculation.output {
            StrategyOutputV1::Continue { diagnostics, .. } => diagnostics
                .action_dependencies
                .iter()
                .map(|d| d.condition_id.clone())
                .collect(),
            _ => vec![],
        }
    }
    fn public_health_schedule(&self) -> Option<Box<dyn StrategyPublicHealthScheduleV1>> {
        Some(Box::new(PublicHealthSchedule {
            health: self.config.health.clone(),
            probability: self.config.probability_reference.clone(),
        }))
    }
    fn next_health_evaluation_at(
        &self,
        frames: &[PublicObservationFrameV1<'_>],
        calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        self.next_public_health_evaluation_at(
            &frames.iter().map(|f| f.observation).collect::<Vec<_>>(),
            calculation,
            at,
        )
    }
    fn next_public_health_evaluation_at(
        &self,
        markets: &[&MarketBusinessObservationV1],
        calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        let dependencies = calculation
            .and_then(|c| match &c.output {
                StrategyOutputV1::Continue { diagnostics, .. } => {
                    Some(diagnostics.action_dependencies.as_slice())
                }
                _ => None,
            })
            .unwrap_or(&[]);
        public_health_deadline(
            &self.config.health,
            &self.config.probability_reference,
            markets,
            dependencies
                .iter()
                .map(|d| (&d.condition_id, d.sources.as_slice())),
            at,
        )
    }
    fn qualify_intent_public(
        &self,
        group: &ClosedBusinessGroupV1,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let StrategyOutputV1::Continue {
            intents,
            diagnostics,
            ..
        } = &calculation.output
        else {
            return Ok(IntentQualificationV1::Qualified);
        };
        let item = &intents[index];
        if matches!(item.intent.as_ref(), ModelIntentV1::CancelOrders { .. }) {
            return Ok(IntentQualificationV1::Qualified);
        }
        let Some(d) = diagnostics
            .action_dependencies
            .iter()
            .find(|d| d.request_id == item.context.global_request_id)
        else {
            return Ok(invalid("action_dependency_unavailable", vec![]));
        };
        let Some(o) = group.observations.get_market(&d.condition_id.0) else {
            return Ok(invalid("market_unavailable", d.sources.clone()));
        };
        self.public_qualification(o, item, d, at)
    }
    fn qualify_intent(
        &mut self,
        input: &StrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        self.qualify_current(input, calculation, index, at)
    }
    fn qualify_intent_borrowed(
        &mut self,
        input: &BorrowedStrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        self.qualify_current(input, calculation, index, at)
    }
    fn invalidated(
        &mut self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        result: &InvalidatedBatchV1,
    ) {
        self.record_allocations(calculation, &result.sent, None);
    }
    fn dispatched(
        &mut self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        match result {
            Ok(sent) => self.record_allocations(calculation, sent, None),
            Err(e) => self.record_allocations(
                calculation,
                &e.sent,
                (e.failed_request_effect == Some(RequestEffectV1::MayHaveRequestEffect))
                    .then_some(e.attempted_intent_index)
                    .flatten(),
            ),
        }
    }
    fn research_diagnostics(&self, d: &Self::Diagnostics) -> serde_json::Value {
        serde_json::to_value(d).expect("正式诊断")
    }
    fn research_evaluated_markets(&self, d: &Self::Diagnostics) -> Vec<ConditionId> {
        d.markets.iter().map(|m| m.condition_id.clone()).collect()
    }
    // 本候选没有逐请求 BLB 消费者；新单及经济关联由原意图和本候选诊断保存。
    fn research_request_markets(&self, _d: &Self::Diagnostics) -> Vec<ConditionId> {
        vec![]
    }
    fn research_compact_diagnostics(&self, d: &Self::Diagnostics) -> serde_json::Value {
        let scalar = serde_json::json!({"strategy_id":d.strategy_id,"rule_version":d.rule_version,
            "risk_equity":d.risk_equity,"high_water":d.high_water,"terminating":d.terminating,
            "available_profit":d.available_profit,"markets":d.markets,"profit_allocations":d.profit_allocations,
            "action_dependencies":d.action_dependencies,"reservation_count":d.reservations.len(),
            "account_dependencies":d.account_dependencies.iter().map(|a|serde_json::json!({
                "condition_id":a.condition_id,"tokens":a.tokens,"h":a.h,"orphan_cost":a.orphan_cost,
                "funds":a.funds,"unknown":a.unknown,"obligation_count":a.obligations.len()
            })).collect::<Vec<_>>()});
        let mut facts = BTreeMap::new();
        for account in &d.account_dependencies {
            for obligation in &account.obligations {
                let key = if let Some(id) = &obligation.trade_identity {
                    serde_json::json!(["trade", id])
                } else if let Some(request) = &obligation.request_id {
                    serde_json::json!(["leg", request, obligation.ordinal])
                } else {
                    serde_json::json!(["unassociated_order", obligation.order_identity])
                };
                facts.insert(
                    serde_json::to_string(&key).expect("原稳定身份"),
                    obligation.clone(),
                );
            }
        }
        let mut cache = self.diagnostic_cache.borrow_mut();
        let scalar_changed = cache.as_ref().is_none_or(|(old, _, _, _)| old != &scalar);
        let version = cache
            .as_ref()
            .map_or(0, |(_, v, _, _)| *v + u64::from(scalar_changed));
        let changes = facts
            .iter()
            .filter(|(id, fact)| {
                cache
                    .as_ref()
                    .is_none_or(|(_, _, old, _)| old.get(*id) != Some(*fact))
            })
            .map(|(id, fact)| serde_json::json!({"identity":id,"value":fact}))
            .collect::<Vec<_>>();
        let removed = cache
            .as_ref()
            .map(|(_, _, old, _)| {
                old.keys()
                    .filter(|id| !facts.contains_key(*id))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let reservations = d
            .reservations
            .iter()
            .map(|r| (r.request_identity.clone(), r.clone()))
            .collect::<BTreeMap<_, _>>();
        let reservation_changes = reservations
            .iter()
            .filter(|(id, fact)| {
                cache
                    .as_ref()
                    .is_none_or(|(_, _, _, old)| old.get(*id) != Some(*fact))
            })
            .map(|(id, fact)| serde_json::json!({"identity":id,"value":fact}))
            .collect::<Vec<_>>();
        let no_longer_reserved = cache
            .as_ref()
            .map(|(_, _, _, old)| {
                old.keys()
                    .filter(|id| !reservations.contains_key(*id))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let definition = scalar_changed.then(|| scalar.clone());
        *cache = Some((scalar, version, facts, reservations));
        serde_json::json!({"format":"continuous-inventory-changes-v1","version":version,"definition":definition,
            "obligation_changes":changes,"no_longer_unclosed":removed,
            "reservation_changes":reservation_changes,"no_longer_reserved":no_longer_reserved})
    }
}

impl CheckpointStrategyV1 for ContinuousInventoryV1 {
    fn write_strategy_checkpoint(
        &self,
        writer: &mut dyn std::io::Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ciborium::ser::into_writer(&(self.namespace.clone(), self.continuity()), writer)?;
        Ok(())
    }
    fn restore_strategy_checkpoint(
        &mut self,
        reader: &mut dyn std::io::Read,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (namespace, continuity): (String, ContinuousInventoryContinuityV1) =
            ciborium::de::from_reader(reader)?;
        *self = Self::new(
            self.config.clone(),
            self.rules.clone(),
            namespace,
            continuity,
        )?
        .with_entry_market_range(self.entry_market_range);
        Ok(())
    }
}

impl DetachedStrategyV1 for ContinuousInventoryV1 {
    fn decide_detached(
        &mut self,
        input: &DetachedStrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        self.decide_input(input, at)
    }
}
