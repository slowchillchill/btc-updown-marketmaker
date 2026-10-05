use super::*;

/// 原来源进展的必要标量；不保存载荷，不新增公共质量或健康状态。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthFactsV1 {
    pub synchronization_required: bool,
    pub availability: MarketDataQuality,
    pub last_valid_message_times: Option<(i64, Option<i64>)>,
}
impl From<&SourceStreamProgressV1> for SourceHealthFactsV1 {
    fn from(p: &SourceStreamProgressV1) -> Self {
        Self {
            synchronization_required: p.synchronization
                == SourceSynchronizationV1::SnapshotRequired
                || p.quality.resync == ResyncQuality::Required,
            availability: p.quality.availability,
            last_valid_message_times: p
                .last_valid_business_message
                .as_ref()
                .map(|e| (e.received_at_utc_ms, e.source_event_at_utc_ms)),
        }
    }
}
/// 参数为原策略已转换的毫秒阈值；顺序、饱和加法及>=边界保持。
pub fn source_health_failure_v1(
    facts: Option<&SourceHealthFactsV1>,
    silence_ms: i64,
    event_delay_ms: i64,
    at: i64,
) -> Option<SourceHealthFailureV1> {
    use SourceHealthFailureV1 as F;
    let Some(f) = facts else {
        return Some(F::UnknownBusinessProgress);
    };
    if f.synchronization_required {
        return Some(F::SynchronizationRequired);
    }
    if !matches!(
        f.availability,
        MarketDataQuality::Fresh | MarketDataQuality::Stale
    ) {
        return Some(F::Unavailable);
    }
    let Some((received, event)) = f.last_valid_message_times else {
        return Some(F::UnknownBusinessProgress);
    };
    if received > at || event.is_some_and(|e| e > received) {
        Some(F::TimeInconsistent)
    } else if at >= received.saturating_add(silence_ms) {
        Some(F::ReceiveSilence)
    } else if let Some(e) = event {
        (at >= e.saturating_add(event_delay_ms)).then_some(F::EventDelay)
    } else {
        Some(F::UnknownEventTime)
    }
}

/// 与研究共用的三态；独立未知/质量异常不混入年龄分布。
pub fn source_health_state_v1(
    facts: Option<&SourceHealthFactsV1>,
    limits: SourceHealthLimitsV1,
    at: i64,
) -> SourceHealthStateV1 {
    // 必要事实未知/独立异常先单列；它们不是已知 S/A 的超限样本。
    if let Some(failure) = source_health_failure_v1(facts, i64::MAX, i64::MAX, at) {
        return SourceHealthStateV1::Unavailable(failure);
    }
    match source_health_failure_v1(facts, limits.silence_max_ms, limits.event_max_ms, at) {
        Some(f @ (SourceHealthFailureV1::ReceiveSilence | SourceHealthFailureV1::EventDelay)) => {
            SourceHealthStateV1::Exceeded(f)
        }
        Some(f) => SourceHealthStateV1::Unavailable(f),
        None => {
            let (received, event) = facts.unwrap().last_valid_message_times.unwrap();
            if at >= received.saturating_add(limits.silence_normal_ms)
                || at >= event.unwrap().saturating_add(limits.event_normal_ms)
            {
                SourceHealthStateV1::Elevated
            } else {
                SourceHealthStateV1::Normal
            }
        }
    }
}

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
        let chainlink = if eligible_chainlink_spot_v1(&o.chainlink_spot).is_some() {
            SourceStreamKindV1::ChainlinkSpot
        } else {
            match value(&o.settlement_reference) {
                Some(BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds) => {
                    SourceStreamKindV1::ChainlinkTwapThirty
                }
                Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds) => {
                    SourceStreamKindV1::ChainlinkTwapSixty
                }
                _ => SourceStreamKindV1::ChainlinkSpot,
            }
        };
        let mut sources = vec![SourceStreamScopeV1 {
            source: chainlink,
            condition_id: None,
            asset_or_symbol: "btc/usd".into(),
        }];
        let p = &self.config.probability;
        if p.binance_innovation_weight != 0.0 || p.binance_trade_pressure_weight_usd != 0.0 {
            sources.push(SourceStreamScopeV1 {
                source: SourceStreamKindV1::BinanceAggregateTrade,
                condition_id: None,
                asset_or_symbol: "BTCUSDT".into(),
            });
        }
        // 成交压力读取深度分母，即使独立深度系数为零仍参与。
        if p.binance_pressure_weight_usd != 0.0 || p.binance_trade_pressure_weight_usd != 0.0 {
            sources.push(SourceStreamScopeV1 {
                source: SourceStreamKindV1::BinanceDepth,
                condition_id: None,
                asset_or_symbol: "BTCUSDT".into(),
            });
        }
        sources
    }
    pub(super) fn pair_sources(&self, o: &MarketBusinessObservationV1) -> Vec<SourceStreamScopeV1> {
        let mut sources = self.probability_sources(o);
        sources.extend(
            o.assets
                .iter()
                .map(|a| Self::book_scope(o, &a.asset.token_id)),
        );
        sources
    }
    pub(super) fn health_states(
        &self,
        o: &MarketBusinessObservationV1,
        sources: &[SourceStreamScopeV1],
        at: i64,
    ) -> Vec<SourceHealthStatusV1> {
        sources
            .iter()
            .map(|scope| {
                let facts = o
                    .source_progress
                    .iter()
                    .find(|p| &p.scope == scope)
                    .map(SourceHealthFactsV1::from);
                SourceHealthStatusV1 {
                    scope: scope.clone(),
                    state: source_health_state_v1(
                        facts.as_ref(),
                        self.config.health.limits(scope.source),
                        at,
                    ),
                }
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
        input: &StrategyCurrentReadV1<'_>,
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
        input
            .markets
            .iter()
            .filter(|f| f.observation.market.window.end_at_utc_ms > at)
            .flat_map(|f| {
                let mut sources = self.pair_sources(f.observation);
                // 新现价可改变当前估计选源，但不能遮蔽待发决定的原TWAP期限。
                for dependency in frozen_dependencies
                    .iter()
                    .filter(|d| d.condition_id == f.observation.market.condition_id)
                {
                    for scope in &dependency.sources {
                        if !sources.contains(scope) {
                            sources.push(scope.clone());
                        }
                    }
                }
                sources.into_iter().flat_map(|scope| {
                    let h = self.config.health.limits(scope.source);
                    f.observation
                        .source_progress
                        .iter()
                        .find(|p| p.scope == scope)
                        .and_then(|p| p.last_valid_business_message.as_ref())
                        .map(|e| {
                            [
                                Some(e.received_at_utc_ms.saturating_add(h.silence_normal_ms)),
                                e.source_event_at_utc_ms
                                    .map(|t| t.saturating_add(h.event_normal_ms)),
                                Some(e.received_at_utc_ms.saturating_add(h.silence_max_ms)),
                                e.source_event_at_utc_ms
                                    .map(|t| t.saturating_add(h.event_max_ms)),
                            ]
                        })
                        .unwrap_or([None, None, None, None])
                        .into_iter()
                        .flatten()
                })
            })
            .filter(|deadline| *deadline > at)
            .min()
    }
    pub(super) fn business_dependency(
        read: &ActionBeforeReadV2,
        trades: &[FrozenTradeEconomicsV1],
        condition: &ConditionId,
        batch_ids: &[&str],
    ) -> Option<ActionBusinessDependencyV1> {
        let market = read
            .economics()
            .markets
            .iter()
            .find(|m| &m.condition_id == condition)?;
        let commitments = read
            .commitments()
            .filter_map(|c| {
                let (market, _) = limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)?;
                let request_id = c.origin.intent.global_request_id();
                (market == condition && !batch_ids.contains(&request_id)).then(|| {
                    ActionCommitmentDependencyV1 {
                        request_id: request_id.into(),
                        leg_ordinal: c.origin.batch_item_ordinal,
                        remaining_shares: c.remaining_target_shares,
                    }
                })
            })
            .collect();
        let unknown_order_ids = read
            .orders()
            .filter(|o| o.condition_id == Some(condition) && o.origin.is_none())
            .map(|o| platform_id(&o))
            .collect();
        Some(ActionBusinessDependencyV1 {
            orphan_shares: market.orphan_shares.clone(),
            orphan_principal: market.unresolved_orphan_principal_cost_pusd.clone(),
            orphan_fee: Self::orphan_fee(trades, condition)
                .ok()
                .and_then(|(fee, unknown)| (!unknown).then(|| fee.public())),
            success_shares: market.success_shares.clone(),
            success_principal: market.success_principal_cost_pusd.clone(),
            actual_fees: std::array::from_fn(|i| market.executed[i].actual_fee_pusd.clone()),
            commitments,
            unknown_order_ids,
        })
    }
    pub(super) fn qualify_health(
        &self,
        input: &StrategyCurrentReadV1<'_>,
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
        if Self::business_dependency(
            &input.action_before,
            &input.trade_economics,
            &dependency.condition_id,
            &batch_ids,
        )
        .as_ref()
            != Some(&dependency.business)
        {
            return reject("action_business_facts_changed", vec![]);
        }

        if at >= o.market.window.end_at_utc_ms
            || value(&o.accepting_orders) != Some(&true)
            || value(&o.closed) == Some(&true)
        {
            return reject("market_not_trading", dependency.sources.clone());
        }
        let issues = self.health_issues(o, &dependency.sources, at);
        if !issues.is_empty() {
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
        let legs: Vec<_> = match item.intent.as_ref() {
            ModelIntentV1::SubmitSingleBuy { order, .. } => vec![order],
            ModelIntentV1::SubmitPairedBuy { orders, .. } => orders.iter().collect(),
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
        if diagnostics.profit_allocations.iter().any(|a| {
            a.global_request_id == item.context.global_request_id
                && rational(&a.allocated_profit_pusd).is_ok_and(|p| p.is_positive())
        }) {
            let qualified = (|| -> Result<bool, StrategyErrorV1> {
                let (profit, _) = Self::profit_and_equity_read(&input.action_before)?;
                let mut occupied = R::zero();
                for allocation in self.profit_pending.values() {
                    occupied = occupied.add(
                        &Self::profit_bound_read(&input.action_before, allocation)?
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
                    let bound = Self::profit_bound_read(&input.action_before, allocation)?;
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
                .economics()
                .markets
                .iter()
                .find(|m| m.condition_id == dependency.condition_id)
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
            if m.settlement_state != EconomicSettlementStateV1::Unresolved
                || decimal(&m.orphan_shares[held]).is_err()
                || decimal(&m.orphan_shares[held]).is_ok_and(|q| q <= Decimal::ZERO)
            {
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
