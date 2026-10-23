pub use super::super::common::{
    SourceHealthFactsV1, source_health_failure_v1, source_health_state_v1,
};
use super::*;

impl ComplementaryPairV1 {
    pub(super) fn book_scope(
        o: &MarketBusinessObservationV1,
        token: &TokenId,
    ) -> SourceStreamScopeV1 {
        SourceStreamScopeV1 {
            source: SourceStreamKindV1::PolymarketBook,
            condition_id: Some(o.market.condition_id.clone()),
            asset_or_symbol: token.0.clone(),
        }
    }
    pub(super) fn probability_sources(
        &self,
        o: &MarketBusinessObservationV1,
    ) -> Vec<SourceStreamScopeV1> {
        super::super::common::probability_sources(&self.config.probability, o)
    }

    pub(super) fn source_health_state(
        &self,
        o: &MarketBusinessObservationV1,
        scope: &SourceStreamScopeV1,
        at: i64,
    ) -> SourceHealthStateV1 {
        // 共享进展只借用所需来源；旧完整数组由 get_scope 保留原查找。
        let progress = o.source_progress.get_scope(scope);
        let facts = progress.map(SourceHealthFactsV1::from);
        source_health_state_v1(facts.as_ref(), self.config.health.limits(scope.source), at)
    }
    pub(super) fn health_states(
        &self,
        o: &MarketBusinessObservationV1,
        sources: &[SourceStreamScopeV1],
        at: i64,
    ) -> Vec<SourceHealthStatusV1> {
        sources
            .iter()
            .map(|scope| SourceHealthStatusV1 {
                scope: scope.clone(),
                state: self.source_health_state(o, scope, at),
            })
            .collect()
    }
    pub(super) fn health_issues(
        &self,
        o: &MarketBusinessObservationV1,
        sources: &[SourceStreamScopeV1],
        at: i64,
    ) -> Vec<SourceHealthIssueV1> {
        self.health_states(o, sources, at)
            .into_iter()
            .filter_map(|p| {
                p.state.failure().map(|failure| SourceHealthIssueV1 {
                    scope: p.scope,
                    failure,
                })
            })
            .collect()
    }
    pub(super) fn health_deadline(
        &self,
        input: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        calculation: Option<&StrategyCalculationV1<ComplementaryPairDiagnosticV1>>,
        at: i64,
    ) -> Option<i64> {
        self.public_health_deadline(
            &input.iter().map(|f| f.observation).collect::<Vec<_>>(),
            calculation,
            at,
        )
    }
    pub(super) fn public_health_deadline(
        &self,
        input: &[&MarketBusinessObservationV1],
        calculation: Option<&StrategyCalculationV1<ComplementaryPairDiagnosticV1>>,
        at: i64,
    ) -> Option<i64> {
        let frozen_dependencies = calculation
            .and_then(|c| match &c.output {
                StrategyOutputV1::Continue { diagnostics, .. } => {
                    Some(diagnostics.action_health_dependencies.as_slice())
                }
                _ => None,
            })
            .unwrap_or(&[]);
        super::super::common::public_health_deadline(
            &self.config.health,
            &self.config.probability,
            input,
            frozen_dependencies
                .iter()
                .map(|d| (&d.condition_id, d.sources.as_slice())),
            at,
        )
    }

    pub(super) fn business_dependency(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
        condition: &ConditionId,
        batch_ids: &[&str],
    ) -> Option<ActionBusinessDependencyV1> {
        let reads = DecisionReads::new(read);
        Self::business_dependency_from_reads(read, &reads, condition, batch_ids)
    }
    pub(super) fn business_dependency_from_reads(
        read: &ActionBeforeReadV2<
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
        reads: &DecisionReads<'_>,
        condition: &ConditionId,
        batch_ids: &[&str],
    ) -> Option<ActionBusinessDependencyV1> {
        let market = read.market_economics(condition)?;
        let commitments = reads
            .commitments
            .get(condition.0.as_str())
            .into_iter()
            .flatten()
            .filter_map(|c| {
                let (market, _) = limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)?;
                let request_id = c.origin.intent.global_request_id();
                (market == condition && !batch_ids.contains(&request_id)).then(|| {
                    ActionCommitmentDependencyV1 {
                        request_id: request_id.into(),
                        leg_ordinal: c.origin.batch_item_ordinal,
                        remaining_shares: c.remaining_target_shares.clone(),
                    }
                })
            })
            .collect();
        let unknown_order_ids = reads
            .orders_by_market
            .get(condition.0.as_str())
            .into_iter()
            .flatten()
            .map(|i| &reads.orders[*i])
            .filter(|o| o.condition_id == Some(condition) && o.origin.is_none())
            .map(|o| platform_id(o))
            .collect();
        Some(ActionBusinessDependencyV1 {
            orphan_shares: market
                .orphan_shares
                .map(|q| ExactDecimal(q.normalize().to_string())),
            orphan_principal: market.orphan_principal.public(),
            orphan_fee: known(&market.orphan_fee).map(R::public),
            success_shares: market
                .success_shares
                .map(|q| ExactDecimal(q.normalize().to_string())),
            success_principal: std::array::from_fn(|i| market.success_principal[i].public()),
            actual_fees: market
                .actual_fees
                .map(|q| ExactDecimal(q.normalize().to_string())),
            commitments,
            unknown_order_ids,
        })
    }
    pub(super) fn qualify_public_group(
        &self,
        group: &ClosedBusinessGroupV1,
        calculation: &StrategyCalculationV1<ComplementaryPairDiagnosticV1>,
        index: usize,
        at: i64,
    ) -> IntentQualificationV1 {
        let StrategyOutputV1::Continue {
            intents,
            diagnostics,
            ..
        } = &calculation.output
        else {
            return IntentQualificationV1::Qualified;
        };
        let item = &intents[index];
        if matches!(item.intent.as_ref(), ModelIntentV1::CancelOrders { .. }) {
            return IntentQualificationV1::Qualified;
        }
        let Some(dependency) = diagnostics
            .action_health_dependencies
            .iter()
            .find(|d| d.global_request_id == item.context.global_request_id)
        else {
            return IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
                reason_code: "action_dependency_unavailable".into(),
                scopes: vec![],
            });
        };
        let observation = group.observations.get_market(&dependency.condition_id.0);
        let Some(o) = observation else {
            return IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
                reason_code: "market_unavailable".into(),
                scopes: dependency.sources.clone(),
            });
        };
        self.qualify_public_observation(o, item.intent.as_ref(), dependency, at)
    }
    fn qualify_public_observation(
        &self,
        o: &MarketBusinessObservationV1,
        intent: &ModelIntentV1,
        dependency: &ActionHealthDependencyV1,
        at: i64,
    ) -> IntentQualificationV1 {
        let reject = |code: &str, scopes| {
            IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
                reason_code: code.into(),
                scopes,
            })
        };
        if at >= o.market.window.end_at_utc_ms
            || value(&o.accepting_orders) != Some(&true)
            || value(&o.closed) == Some(&true)
        {
            return reject("market_not_trading", dependency.sources.clone());
        }
        if dependency
            .sources
            .iter()
            .any(|scope| self.source_health_state(o, scope, at).failure().is_some())
        {
            let issues = self.health_issues(o, &dependency.sources, at);
            return reject(
                "source_health",
                issues.into_iter().map(|i| i.scope).collect(),
            );
        }
        if let Some(token) = &dependency.independent_held_token
            && o.assets
                .iter()
                .find(|a| &a.asset.token_id == token)
                .and_then(|a| value(&a.valuation_best_bid))
                .is_none()
        {
            return reject("repair_held_mark_unavailable", dependency.sources.clone());
        }
        let legs: &[BuyLegIntentV1] = match intent {
            ModelIntentV1::SubmitSingleBuy { order, .. } => std::slice::from_ref(order),
            ModelIntentV1::SubmitPairedBuy { orders, .. } => orders,
            _ => return reject("unsupported_buy_action", vec![]),
        };
        for leg in legs {
            if o.assets
                .iter()
                .find(|a| a.asset.token_id == leg.token_id)
                .is_none_or(|a| {
                    value(&a.tick).is_none()
                        || (matches!(leg.order, BuyOrderIntentV1::FakMarket { .. })
                            && value(&a.asks).is_none())
                })
            {
                return reject(
                    "action_book_or_tick_unavailable",
                    dependency.sources.clone(),
                );
            }
        }
        IntentQualificationV1::Qualified
    }
    pub(super) fn qualify_health(
        &self,
        input: &StrategyCurrentReadV1<
            '_,
            impl std::borrow::Borrow<SharedExecutionRegistryV2>,
            impl ActionAccountReadV2,
        >,
        calculation: &StrategyCalculationV1<ComplementaryPairDiagnosticV1>,
        index: usize,
        at: i64,
    ) -> IntentQualificationV1 {
        let reject = |code: &str, scopes| {
            IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
                reason_code: code.into(),
                scopes,
            })
        };
        let StrategyOutputV1::Continue {
            intents,
            diagnostics,
            ..
        } = &calculation.output
        else {
            return IntentQualificationV1::Qualified;
        };
        let item = &intents[index];
        if matches!(item.intent.as_ref(), ModelIntentV1::CancelOrders { .. }) {
            // 本策略只从原已知订单形成撤单；后来终态可令目标退出unfinished读取，
            // 不能因此吞掉同批仍需撤的其他目标。原Bridge/平台负责原目标结果。
            return IntentQualificationV1::Qualified;
        }
        let Some(dependency) = diagnostics
            .action_health_dependencies
            .iter()
            .find(|d| d.global_request_id == item.context.global_request_id)
        else {
            return reject("action_dependency_unavailable", vec![]);
        };
        let Some(frame) = input
            .markets
            .iter()
            .find(|f| f.observation.market.condition_id == dependency.condition_id)
        else {
            return reject("market_unavailable", dependency.sources.clone());
        };
        let o = frame.observation;
        let batch_ids: Vec<_> = intents
            .iter()
            .map(|i| i.context.global_request_id.as_str())
            .collect();
        if Self::business_dependency(&input.action_before, &dependency.condition_id, &batch_ids)
            .as_ref()
            != Some(&dependency.business)
        {
            return reject("action_business_facts_changed", vec![]);
        }

        let public = self.qualify_public_observation(o, item.intent.as_ref(), dependency, at);
        if matches!(public, IntentQualificationV1::Invalidated(_)) {
            return public;
        }
        if diagnostics.profit_allocations.iter().any(|a| {
            a.global_request_id == item.context.global_request_id
                && rational(&a.allocated_profit_pusd).is_ok_and(|p| p.is_positive())
        }) {
            let qualified = (|| -> Result<bool, StrategyErrorV1> {
                let (profit, _) = Self::profit_and_equity_read(&input.action_before)?;
                let mut occupied = R::zero();
                for allocation in self.profit_pending.values() {
                    occupied = occupied.add(
                        &Self::profit_occupancy_read(&input.action_before, allocation)?
                            .unwrap_or_else(R::zero),
                    );
                }
                for allocation in &diagnostics.profit_allocations {
                    if self
                        .profit_pending
                        .contains_key(&allocation.global_request_id)
                    {
                        continue;
                    }
                    let bound = Self::profit_occupancy_read(&input.action_before, allocation)?;
                    let known_completed = input.action_before.request_funds().any(|f| {
                        f.request_identity == &allocation.reserve_identity
                            && matches!(
                                f.reservation_state,
                                ReservationStatusV1::Converted | ReservationStatusV1::Released
                            )
                    });
                    occupied = occupied.add(&bound.unwrap_or(if known_completed {
                        R::zero()
                    } else {
                        rational(&allocation.allocated_profit_pusd)?
                    }));
                }
                Ok(profit.max(R::zero()) >= occupied)
            })();
            if !matches!(qualified, Ok(true)) {
                return reject("repair_profit_support_unavailable", vec![]);
            }
        }
        if let Some(token) = &dependency.repair_token {
            // 已到达的真实反馈可能消除原缺口；不能凭冻结的修复标签重复买入。
            let Some(m) = input
                .action_before
                .market_economics(&dependency.condition_id)
            else {
                return reject("repair_position_unavailable", vec![]);
            };
            let Some(assets) = &m.assets else {
                return reject("repair_position_unavailable", vec![]);
            };
            let Some(side) = assets.iter().position(|a| &a.token_id == token) else {
                return reject("repair_asset_unavailable", vec![]);
            };
            let held = 1 - side;
            if !m.unresolved || m.orphan_shares[held] <= Decimal::ZERO {
                return reject("repair_gap_unavailable", vec![]);
            }
            if matches!(
                item.intent.as_ref(),
                ModelIntentV1::SubmitSingleBuy {
                    order: BuyLegIntentV1 {
                        order: BuyOrderIntentV1::FakMarket { .. },
                        ..
                    },
                    ..
                }
            ) && input.action_before.pending_noe_record_count() > 0
            {
                return reject("repair_feedback_pending", vec![]);
            }
        }
        IntentQualificationV1::Qualified
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normal_equality_is_acceptable_max_equality_blocks_and_zero_equal_limits_terminate() {
        let f = SourceHealthFactsV1 {
            synchronization_required: false,
            availability: MarketDataQuality::Fresh,
            last_valid_message_times: Some((100, Some(100))),
        };
        let h = SourceHealthLimitsV1 {
            silence_normal_ms: 10,
            event_normal_ms: 10,
            silence_max_ms: 20,
            event_max_ms: 20,
        };
        assert_eq!(
            source_health_state_v1(Some(&f), h, 109),
            SourceHealthStateV1::Normal
        );
        assert_eq!(
            source_health_state_v1(Some(&f), h, 110),
            SourceHealthStateV1::Elevated
        );
        assert!(source_health_state_v1(Some(&f), h, 119).failure().is_none());
        assert!(matches!(
            source_health_state_v1(Some(&f), h, 120),
            SourceHealthStateV1::Exceeded(_)
        ));
        let missing_event = SourceHealthFactsV1 {
            last_valid_message_times: Some((100, None)),
            ..f.clone()
        };
        assert_eq!(
            source_health_state_v1(Some(&missing_event), h, 200),
            SourceHealthStateV1::Unavailable(SourceHealthFailureV1::UnknownEventTime)
        );
        let equal = SourceHealthLimitsV1 {
            silence_normal_ms: 0,
            event_normal_ms: 0,
            silence_max_ms: 0,
            event_max_ms: 0,
        };
        assert!(matches!(
            source_health_state_v1(Some(&f), equal, 100),
            SourceHealthStateV1::Exceeded(_)
        ));
        assert_eq!(
            source_health_state_v1(None, h, 100),
            SourceHealthStateV1::Unavailable(SourceHealthFailureV1::UnknownBusinessProgress)
        );
    }
}
