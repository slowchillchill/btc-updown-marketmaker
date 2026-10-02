use super::*;

impl ComplementaryPairV1 {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn repair(
        &mut self,
        input: &StrategyInputV1,
        f: &FrozenMarketReadV1,
        now: i64,
        s: &MarketState,
        p: &ProbabilityEstimateV1,
        cash: &mut Decimal,
        account_exposure: &mut R,
        profit: &mut R,
        intents: &mut Vec<ReservedIntentV1>,
        diag: &mut MarketDecisionDiagnosticV1,
        allocations: &mut Vec<ProfitAllocationV1>,
    ) -> Result<(), StrategyErrorV1> {
        let o = &f.observation;
        let Some(held) = s.orphan.iter().position(|q| *q > Decimal::ZERO) else {
            return Ok(());
        };
        let side = 1 - held;
        if o.assets.len() != 2 || s.unknown {
            diag.unavailable
                .push(ActionUnavailableV1::UnknownCostOrCommitment);
            return Ok(());
        }
        let a = &o.assets[side];
        let qold = s.orphan[held];
        let old_principal = s.orphan_principal.divided_by(&r(qold));
        let old_fee = s.orphan_fee.divided_by(&r(qold));
        let old_all = old_principal.add(&old_fee);
        let allowance = self.loss_allowance(o, now, s)?;
        let qmax = qold.min(self.config.quote.base_pair_shares);
        let min = decimal(&f.rule_context.limit_min_quantity_shares)?;
        let Some(tick) = value(&a.tick) else {
            diag.unavailable
                .push(ActionUnavailableV1::MissingBookOrTick);
            return Ok(());
        };
        let tick = decimal(tick)?;
        if tick <= Decimal::ZERO {
            return Err(StrategyErrorV1::Arithmetic("公共tick非正"));
        }
        let mut q = (qmax * Decimal::from(100)).floor() / Decimal::from(100);
        if q < min {
            diag.choice = MarketChoiceV1::RetainOrphan;
            return Ok(());
        }
        let cap = r(Decimal::ONE + allowance)
            .sub(&old_all)
            .add(&profit.divided_by(&r(q)));
        let old_support = self
            .profit_pending
            .values()
            .filter(|allocation| {
                allocation.condition_id == o.market.condition_id
                    && allocation.token_id == a.asset.token_id
            })
            .try_fold(R::zero(), |sum, allocation| {
                Ok::<_, StrategyErrorV1>(
                    sum.add(&Self::profit_bound(input, allocation)?.unwrap_or_else(R::zero)),
                )
            })?;
        let existing_quantity = s
            .pending
            .iter()
            .filter(|p| p.token == a.asset.token_id)
            .fold(R::zero(), |sum, p| sum.add(&p.quantity));
        let old_cap = r(Decimal::ONE + allowance)
            .sub(&old_all)
            .add(&old_support.divided_by(&existing_quantity.clone().max(r(q))));
        let maker_cap = old_cap.clone().max(cap.clone());
        let old_cap_price =
            (floor_decimal(&old_cap.clone().max(R::zero()), 8)?.min(Decimal::ONE - tick) / tick)
                .floor()
                * tick;
        let new_cap_price = floor_decimal(&cap.clone().max(R::zero()), 8)?.min(Decimal::ONE - tick);
        let new_cap_price = (new_cap_price / tick).floor() * tick;
        let cap_price = floor_decimal(&maker_cap.max(R::zero()), 8)?.min(Decimal::ONE - tick);
        let cap_price = (cap_price / tick).floor() * tick;
        let existing_cap = if existing_quantity.is_positive() {
            old_cap_price
        } else {
            cap_price
        };
        diag.maximum_repair_price = (existing_cap > Decimal::ZERO).then(|| exact(existing_cap));
        if cap_price <= Decimal::ZERO {
            diag.unavailable.push(ActionUnavailableV1::CostLine);
            diag.choice = MarketChoiceV1::RetainOrphan;
            return Ok(());
        }
        let fair = p.up().map(|up| if side == 0 { up } else { 1.0 - up });
        let own_quote = self.quotes(o, p, s)?.map(|q| q[side]);
        let visible_bid = value(&a.bids)
            .and_then(|ls| {
                ls.iter().max_by(|a, b| {
                    decimal(&a.price_pusd_per_share)
                        .unwrap_or_default()
                        .cmp(&decimal(&b.price_pusd_per_share).unwrap_or_default())
                })
            })
            .map(|l| decimal(&l.price_pusd_per_share))
            .transpose()?;
        let maker = self.price(
            a,
            own_quote
                .unwrap_or(visible_bid.map(|b| b + tick).unwrap_or(cap_price))
                .min(cap_price),
        )?;
        let old_mark = value(&o.assets[held].valuation_best_bid)
            .and_then(|p| p.as_ref())
            .map(|p| float(&decimal(p)?))
            .transpose()?;
        // 估计缺失时，这个比较可独立使用持有腿公开标记价；诊断仍不伪造公平价值。
        let basis = if let Some(f) = fair {
            Some(f)
        } else {
            old_mark.map(|m| 1.0 - m)
        };
        let Some(basis) = basis else {
            diag.unavailable.push(ActionUnavailableV1::EstimateRequired);
            return Ok(());
        };
        let duration =
            (o.market.window.end_at_utc_ms - o.market.window.start_at_utc_ms).max(1) as f64;
        let time_pressure =
            ((now - o.market.window.start_at_utc_ms) as f64 / duration).clamp(0.0, 1.0);
        let cost_per_share = float(&floor_decimal(&old_all, 16)?)?;
        let inventory_pressure = if self.config.risk.per_market_orphan_limit_pusd > Decimal::ZERO {
            float(&floor_decimal(&s.orphan_principal.add(&s.orphan_fee), 16)?)?
                / float(&self.config.risk.per_market_orphan_limit_pusd)?
        } else {
            0.0
        };
        // 各可行性门独立；比较只评价风险减少的经营价值，不允许时间覆盖未知/资金。
        let risk = float(&self.config.repair.residual_risk_weight)?
            * cost_per_share
            * (time_pressure + inventory_pressure);
        let retain = -risk;
        let remaining = o.market.window.end_at_utc_ms - now;
        let weight = self.config.repair.maker_fill_weight;
        let maker_score =
            maker.map(|mp| weight * (basis - float(&mp).unwrap_or(0.0)) + (1.0 - weight) * retain);
        let wait = retain
            + float(&self.config.repair.wait_price_improvement_per_share)? * (1.0 - time_pressure);
        let mut active = None;
        let mut active_fee = R::zero();
        let mut active_spend = Decimal::ZERO;
        let mut maximum_active_q = R::zero();
        if self.config.repair.enable_active_repair {
            if remaining <= i64::try_from(self.config.repair.active_path_ms).unwrap_or(i64::MAX) {
                diag.unavailable
                    .push(ActionUnavailableV1::ActionPathEndsAfterWindow);
            } else if let Some(asks) = value(&a.asks) {
                let mut total = Decimal::ZERO;
                let mut worst = Decimal::ZERO;
                for l in asks {
                    let price = decimal(&l.price_pusd_per_share)?;
                    let qty = decimal(&l.quantity_shares)?;
                    if price <= new_cap_price && price > Decimal::ZERO {
                        total += qty;
                        worst = worst.max(price);
                    }
                }
                if total > Decimal::ZERO && worst > Decimal::ZERO {
                    let target = q.min(total);
                    let spend = (target * worst * Decimal::from(1_000_000)).floor()
                        / Decimal::from(1_000_000);
                    if spend >= decimal(&f.rule_context.market_buy_min_spend_pusd)? {
                        let b = self
                            .rules
                            .sim_market_buy_bounds_v1(spend)
                            .map_err(|e| StrategyErrorV1::Cost(e.to_string()))?;
                        let maxq = fraction_units(&b.maximum_base_micro, 6);
                        let fee = fraction_units(&b.maximum_fee_1e5_units, 5);
                        // 全域数量界不能由当前盘口收紧。常见风险参数下FAK会不可证明。
                        let over = maxq.sub(&r(qold)).max(R::zero());
                        let reverse = over.mul(&r(worst)).add(&fee);
                        let cost = r(spend).add(&fee);
                        // 每个可达正微份片段：整笔最大费用除最小微份是保守逐档界。
                        let per_share = r(worst).add(&fee.divided_by(&r(Decimal::new(1, 6))));
                        if over > r(self.config.repair.maximum_overfill_shares)
                            || reverse > r(self.config.repair.maximum_overfill_pusd)
                            || reverse
                                > s.exposure[side]
                                    .clone()
                                    .max(r(self.config.risk.per_market_orphan_limit_pusd))
                            || account_exposure.add(&reverse.sub(&s.exposure[side]).max(R::zero()))
                                > r(self.config.risk.account_orphan_limit_pusd)
                                    .max(account_exposure.clone())
                        {
                            diag.unavailable
                                .push(ActionUnavailableV1::AmountOverfillBound);
                        } else if old_all.add(&per_share)
                            > r(Decimal::ONE + allowance).add(&profit.divided_by(&r(q)))
                        {
                            diag.unavailable.push(ActionUnavailableV1::CostLine);
                        } else if !self.limit_budget(*cash, s, &cost) {
                            diag.unavailable
                                .push(ActionUnavailableV1::InsufficientCashOrInvestment);
                        } else {
                            active = Some((worst, basis - float(&floor_decimal(&per_share, 16)?)?));
                            active_spend = spend;
                            active_fee = fee;
                            maximum_active_q = maxq;
                        }
                    }
                }
            }
        }
        let net = maker
            .map(|mp| r(Decimal::ONE).sub(&old_all).sub(&r(mp)))
            .unwrap_or_else(R::zero);
        diag.comparison = Some(RepairComparisonV1 {
            paired_net_per_share: net.public(),
            premium_per_share: fair
                .zip(maker)
                .map(|(fair, p)| r(p).sub(&r(fd(fair).unwrap_or_default())).public()),
            maker_value_per_share: maker_score,
            active_value_per_share: active.map(|(_, v)| v),
            wait_value_per_share: Some(wait),
            retain_value_per_share: retain,
        });
        let active_best = active
            .is_some_and(|(_, v)| v >= wait && v >= retain && maker_score.is_none_or(|m| v > m));
        let existing_missing: Vec<_> = s
            .pending
            .iter()
            .filter(|p| p.token == a.asset.token_id)
            .collect();
        // 对主动补腿，原侧任意旧单/未决事实均先处理；不在旧快照中撤后立即发。
        if active_best {
            if !existing_missing.is_empty() {
                let ids = self.cancel_available(
                    input,
                    existing_missing
                        .iter()
                        .filter_map(|p| p.order_id.clone())
                        .collect(),
                );
                if !ids.is_empty() {
                    intents.push(self.cancel(ids)?);
                }
                diag.choice = MarketChoiceV1::CancelBeforeActiveRepair;
                return Ok(());
            }
            if input.action_before.pending_noe_record_count() > 0 {
                diag.choice = MarketChoiceV1::Wait;
                return Ok(());
            }
            let (worst, _) = active.unwrap();
            let item = self.order_item(
                &o.market.condition_id,
                vec![(
                    BuyLegIntentV1 {
                        token_id: a.asset.token_id.clone(),
                        order: BuyOrderIntentV1::FakMarket {
                            spend_pusd: active_spend,
                            worst_price_pusd_per_share: worst,
                        },
                    },
                    r(active_spend),
                    active_fee.clone(),
                )],
            )?;
            let increment = r(active_spend).add(&active_fee);
            let used = profit.clone().min(increment.clone());
            let allocation = ProfitAllocationV1 {
                global_request_id: item.context.global_request_id.clone(),
                reserve_identity: item.reserves[0].reserve.request_identity.clone(),
                condition_id: o.market.condition_id.clone(),
                token_id: a.asset.token_id.clone(),
                principal_bound_pusd: r(active_spend).public(),
                fee_bound_pusd: active_fee.clone().public(),
                original_shares: None,
                incremental_loss_per_share: R::zero().public(),
                allocated_profit_pusd: used.clone().public(),
            };
            *profit = profit.sub(&used);
            *cash -= floor_decimal(&r(active_spend).add(&active_fee), 6)?;
            *account_exposure = account_exposure.add(
                &maximum_active_q
                    .sub(&r(qold))
                    .max(R::zero())
                    .mul(&r(worst))
                    .add(&active_fee)
                    .sub(&s.exposure[side])
                    .max(R::zero()),
            );
            allocations.push(allocation);
            intents.push(item);
            diag.choice = MarketChoiceV1::ActiveRepair;
            return Ok(());
        }
        let maker_best = maker_score.is_some_and(|m| m >= wait && m >= retain);
        if !maker_best {
            diag.choice = if wait >= retain {
                MarketChoiceV1::Wait
            } else {
                MarketChoiceV1::RetainOrphan
            };
            return Ok(());
        }
        let mp = maker.unwrap();
        if remaining <= i64::try_from(self.config.repair.maker_path_ms).unwrap_or(i64::MAX) {
            diag.unavailable
                .push(ActionUnavailableV1::ActionPathEndsAfterWindow);
            diag.choice = MarketChoiceV1::Wait;
            return Ok(());
        }
        let existing_q = existing_missing
            .iter()
            .fold(R::zero(), |a, p| a.add(&p.quantity));
        let invalid: Vec<_> = existing_missing
            .iter()
            .filter(|p| p.price > mp || p.price > old_cap_price || p.quantity > r(qold))
            .filter_map(|p| p.order_id.clone())
            .collect();
        let valid_existing = existing_missing
            .iter()
            .all(|p| p.price <= mp && p.price <= old_cap_price)
            && existing_q <= r(qold);
        if valid_existing && !existing_missing.is_empty() {
            diag.choice = MarketChoiceV1::Maintain;
            return Ok(());
        }
        // 旧请求已有支持只用于维护；新增/重叠订单不能再借同一占用。
        let mp = mp.min(new_cap_price);
        if mp <= Decimal::ZERO {
            diag.unavailable.push(ActionUnavailableV1::CostLine);
            return Ok(());
        }
        if let Some(comparison) = diag.comparison.as_mut() {
            comparison.paired_net_per_share = r(Decimal::ONE).sub(&old_all).sub(&r(mp)).public();
            comparison.premium_per_share = fair
                .map(|fair| fd(fair).map(|fair| r(mp).sub(&r(fair)).public()))
                .transpose()?;
            comparison.maker_value_per_share =
                Some(weight * (basis - float(&mp)?) + (1.0 - weight) * retain);
        }
        // 普通Maker撤换允许重叠，但过补、总投入、现金仍合并旧承诺计算。
        let reverse_capacity = r(self.config.repair.maximum_overfill_pusd)
            .min(r(self.config.risk.per_market_orphan_limit_pusd).max(s.exposure[side].clone()))
            .min(
                r(self.config.risk.account_orphan_limit_pusd)
                    .max(account_exposure.clone())
                    .sub(account_exposure)
                    .add(&s.exposure[side]),
            );
        let reverse_price = existing_missing
            .iter()
            .map(|p| p.price)
            .fold(mp, Decimal::max);
        let pending_fee = existing_missing.iter().fold(R::zero(), |sum, p| {
            sum.add(&p.cost.sub(&p.quantity.mul(&r(p.price))).max(R::zero()))
        });
        let over_capacity = r(self.config.repair.maximum_overfill_shares).min(
            reverse_capacity
                .sub(&pending_fee)
                .max(R::zero())
                .divided_by(&r(reverse_price)),
        );
        q = q.min(floor_decimal(
            &r(qold).add(&over_capacity).sub(&existing_q).max(R::zero()),
            2,
        )?);
        let over = existing_q.add(&r(q)).sub(&r(qold)).max(R::zero());
        let reverse = over.mul(&r(reverse_price)).add(&if over.is_positive() {
            pending_fee
        } else {
            R::zero()
        });
        let delta_risk = reverse.sub(&s.exposure[side]).max(R::zero());
        let buy_risk_ok = over <= r(self.config.repair.maximum_overfill_shares)
            && reverse <= r(self.config.repair.maximum_overfill_pusd)
            && reverse
                <= r(self.config.risk.per_market_orphan_limit_pusd).max(s.exposure[side].clone())
            && account_exposure.add(&delta_risk)
                <= r(self.config.risk.account_orphan_limit_pusd).max(account_exposure.clone());
        let remaining_budget =
            r(*cash).min(r(Decimal::from(1000)).sub(&s.investment).max(R::zero()));
        q = q.min(floor_decimal(&remaining_budget.divided_by(&r(mp)), 2)?);
        if q < min || !buy_risk_ok {
            diag.unavailable.push(if buy_risk_ok {
                ActionUnavailableV1::InsufficientCashOrInvestment
            } else {
                ActionUnavailableV1::InventoryCapacity
            });
        } else {
            let cost = self.limit_cost(q, mp, o, &a.asset.token_id, true)?;
            if self.limit_budget(*cash, s, &cost) {
                let increment_per_share = old_principal
                    .add(&r(mp))
                    .sub(&r(Decimal::ONE))
                    .max(R::zero());
                // 历史费已在P扣过，不再占用同一增量。
                let used = profit.clone().min(increment_per_share.mul(&r(q)));
                let item = self.order_item(
                    &o.market.condition_id,
                    vec![(self.maker_leg(a, q, mp), cost.clone(), R::zero())],
                )?;
                allocations.push(ProfitAllocationV1 {
                    global_request_id: item.context.global_request_id.clone(),
                    reserve_identity: item.reserves[0].reserve.request_identity.clone(),
                    condition_id: o.market.condition_id.clone(),
                    token_id: a.asset.token_id.clone(),
                    principal_bound_pusd: cost.clone().public(),
                    fee_bound_pusd: R::zero().public(),
                    original_shares: Some(exact(q)),
                    incremental_loss_per_share: increment_per_share.public(),
                    allocated_profit_pusd: used.clone().public(),
                });
                *profit = profit.sub(&used);
                *cash -= floor_decimal(&cost, 6)?;
                *account_exposure = account_exposure.add(&delta_risk);
                intents.push(item);
                diag.choice = MarketChoiceV1::MakerRepair;
            }
        }
        let ids = self.cancel_available(input, invalid);
        if !ids.is_empty() {
            intents.push(self.cancel(ids)?);
        }
        Ok(())
    }
}
