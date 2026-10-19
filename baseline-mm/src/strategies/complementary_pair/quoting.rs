use super::*;

impl ComplementaryPairV1 {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_pair(
        &mut self,
        input: &StrategyInputV1<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl StrategyMarketReadV1,
            impl ActionAccountReadV2,
        >,
        f: &PairMarketRead<'_>,
        now: i64,
        s: &MarketState,
        p: &ProbabilityEstimateV1,
        targets: [Result<Decimal, InitialPairPriceUnavailableV1>; 2],
        cash: &mut Decimal,
        account_exposure: &mut R,
        intents: &mut Vec<ReservedIntentV1>,
        diag: &mut MarketDecisionDiagnosticV1,
    ) -> Result<(), StrategyErrorV1> {
        let o = &f.observation;
        diag.pair_entry.evaluated = true;
        // 买一能形成价格，仍不替代原经营评分所需概率。
        if p.up().is_none() {
            diag.unavailable.push(ActionUnavailableV1::EstimateRequired);
            return Ok(());
        }
        let [Ok(a), Ok(b)] = targets else {
            diag.unavailable
                .push(ActionUnavailableV1::NoLegalMakerPrice);
            return Ok(());
        };
        let mut prices = [a, b];
        if s.unknown {
            diag.unavailable
                .push(ActionUnavailableV1::UnknownCostOrCommitment);
            return Ok(());
        }
        let remaining = o.market.window.end_at_utc_ms - now;
        if remaining <= i64::try_from(self.config.quote.execution_path_ms).unwrap_or(i64::MAX) {
            diag.unavailable
                .push(ActionUnavailableV1::ActionPathEndsAfterWindow);
            return Ok(());
        }
        // 同侧已合格的低价目标可复用；添补另一侧时不再造第二个常驻价位。
        for (side, target) in prices.iter_mut().enumerate() {
            if let Some(old) = s
                .pending
                .iter()
                .filter(|leg| leg.token == o.assets[side].asset.token_id && leg.price <= *target)
                .map(|leg| leg.price)
                .min()
            {
                *target = old;
            }
        }
        diag.pair_entry.reused_prices = Some(prices.map(|p| ExactDecimal(p.to_string())));
        diag.pair_entry.bid_distance = Some([0, 1].map(|i| {
            o.assets[i]
                .bids
                .value
                .as_ref()
                .and_then(|levels| {
                    levels
                        .iter()
                        .filter(|l| decimal(&l.quantity_shares).is_ok_and(|q| q > Decimal::ZERO))
                        .filter_map(|l| decimal(&l.price_pusd_per_share).ok())
                        .max()
                })
                .map(|b| ExactDecimal((prices[i] - b).to_string()))
        }));
        diag.pair_entry.ask_distance = Some([0, 1].map(|i| {
            o.top_ask(i)
                .ok()
                .flatten()
                .map(|a| ExactDecimal((prices[i] - a).to_string()))
        }));
        let unit = prices[0] + prices[1];
        let fair = [p.up().unwrap(), 1.0 - p.up().unwrap()];
        let weights = self.config.quote.joint_fill_scenario_weights;
        let sum = weights.iter().sum::<f64>();
        let both = 1.0 - float(&unit)?;
        let mut single = [0.0; 2];
        let mut future_cash = Decimal::ZERO;
        for held in 0..2 {
            let other = 1 - held;
            let retain = fair[held] - float(&prices[held])?;
            // 可见卖价作为未来Maker修复价格情景；不保证成交，post_only费用为零。
            // FAK不在该乐观情景中获准，实际主动修复另用公共全域费/份额界。
            let ask = o.top_ask(other).ok().flatten();
            let repair =
                ask.map(|a| 1.0 - float(&prices[held]).unwrap_or(0.0) - float(&a).unwrap_or(0.0));
            single[held] = repair
                .map(|repair| {
                    self.config.repair.maker_fill_weight * repair
                        + (1.0 - self.config.repair.maker_fill_weight) * retain
                })
                .unwrap_or(retain);
            if let Some(ask) = ask {
                future_cash = future_cash.max(ask);
            }
        }
        let business = (weights[0] * both + weights[1] * single[0] + weights[2] * single[1]) / sum;
        let gate_passed = business >= float(&self.config.quote.minimum_business_result_per_share)?;
        diag.pair_entry.business_result_per_share = Some(business);
        diag.pair_entry.minimum_business_result_per_share = Some(ExactDecimal(
            self.config
                .quote
                .minimum_business_result_per_share
                .to_string(),
        ));
        diag.pair_entry.gate_passed = Some(gate_passed);
        if !gate_passed {
            diag.choice = MarketChoiceV1::Wait;
            return Ok(());
        }
        let exposure_max = s.exposure[0].clone().max(s.exposure[1].clone());
        let account_space = r(self.config.risk.account_orphan_limit_pusd)
            .sub(account_exposure)
            .max(R::zero());
        let market_space = r(self.config.risk.per_market_orphan_limit_pusd)
            .sub(&exposure_max)
            .max(R::zero());
        let risk_space = account_space.min(market_space);
        let max_leg = prices[0].max(prices[1]);
        let investment_space = r(Decimal::from(1000)).sub(&s.investment).max(R::zero());
        let mut q = self.config.quote.base_pair_shares;
        q = q.min(floor_decimal(
            &r(*cash).min(investment_space).divided_by(&r(unit)),
            2,
        )?);
        q = q.min(floor_decimal(&risk_space.divided_by(&r(max_leg)), 2)?);
        // 异步修复情景只取互斥两侧最大需要，不重复冻结两份未来修复现金。
        if future_cash > Decimal::ZERO {
            q = q.min(floor_decimal(
                &r(*cash)
                    .min(r(Decimal::from(1000)).sub(&s.investment).max(R::zero()))
                    .divided_by(&r(unit + future_cash)),
                2,
            )?);
        }
        let min = decimal(&f.rule_context.limit_min_quantity_shares)?;
        let invalid: Vec<_> = s
            .pending
            .iter()
            .filter(|leg| {
                o.assets
                    .iter()
                    .position(|a| a.asset.token_id == leg.token)
                    .is_none_or(|side| leg.price > prices[side])
            })
            .filter_map(|leg| leg.order_id.clone())
            .collect();
        let valid_targets = [0, 1].map(|side| {
            s.pending
                .iter()
                .any(|leg| leg.token == o.assets[side].asset.token_id && leg.price <= prices[side])
        });
        let no_new_needed = valid_targets.iter().all(|v| *v);
        diag.pair_entry.no_new_needed = Some(no_new_needed);
        diag.pair_entry.risk_space_pusd = Some(risk_space.public());
        diag.pair_entry.investment_space_pusd = Some(
            r(Decimal::from(1000))
                .sub(&s.investment)
                .max(R::zero())
                .public(),
        );
        diag.pair_entry.proposed_shares = Some(ExactDecimal(q.to_string()));
        diag.pair_entry.minimum_shares = Some(ExactDecimal(min.to_string()));
        if !no_new_needed && q >= min && diag.pair_entry.market_eligible {
            let costs = [
                self.limit_cost(q, prices[0], o, &o.assets[0].asset.token_id, true)?,
                self.limit_cost(q, prices[1], o, &o.assets[1].asset.token_id, true)?,
            ];
            let cost = costs[0].add(&costs[1]);
            let budget_passed = self.limit_budget(*cash, s, &cost);
            diag.pair_entry.limit_budget_passed = Some(budget_passed);
            if budget_passed {
                let legs = vec![
                    (
                        self.maker_leg(o.assets[0], q, prices[0]),
                        costs[0].clone(),
                        R::zero(),
                    ),
                    (
                        self.maker_leg(o.assets[1], q, prices[1]),
                        costs[1].clone(),
                        R::zero(),
                    ),
                ];
                intents.push(self.order_item(&o.market.condition_id, legs)?);
                *cash -= floor_decimal(&cost, 6)?;
                *account_exposure = account_exposure.add(&r(q * max_leg));
                diag.choice = MarketChoiceV1::NewPair;
            }
        } else if no_new_needed {
            diag.choice = MarketChoiceV1::Maintain;
        } else if q < min {
            diag.unavailable
                .push(ActionUnavailableV1::InventoryCapacity);
        }
        let ids = self.cancel_available(input, invalid);
        if !ids.is_empty() {
            intents.push(self.cancel(ids)?);
        }
        Ok(())
    }
}
