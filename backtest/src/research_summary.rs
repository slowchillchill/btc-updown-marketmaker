//! 显式研究选项的低频投影；分类是计算成功事实，其余值均读取原owner。
use crate::run::{CalculationTimingRecordV1, SourceHealthRunSummaryV1};
use contracts::{economics::EconomicRational as R, *};
use serde::{Deserialize, Serialize};
use shared_run::{AccountEconomicsReadV2, SharedExecutionV2, StrategyOutputV1};
use sim_system::public_consumption::TrustedGroupDecisionCutoffV1;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionCountsV1 {
    pub successful: u64,
    pub no_trade: u64,
    pub actions: u64,
    pub exits: u64,
}
impl DecisionCountsV1 {
    pub(crate) fn record<D>(&mut self, output: &StrategyOutputV1<D>) {
        self.successful += 1;
        match output {
            StrategyOutputV1::Continue { intents, .. } if intents.is_empty() => self.no_trade += 1,
            StrategyOutputV1::Continue { .. } => self.actions += 1,
            StrategyOutputV1::Exit { .. } => self.exits += 1,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchSnapshotV1 {
    pub boundary_utc_ms: i64,
    pub observation_cutoff: serde_json::Value,
    pub metrics: BTreeMap<String, ExecutionReadValueV1<ExactRationalV1>>,
    pub counters: BTreeMap<String, u64>,
    pub decisions: Option<DecisionCountsV1>,
    pub pending_calculation: Option<CalculationTimingRecordV1>,
    /// 原累计统计；不将累计最大回撤相减解释为小时最大回撤。
    pub day_economics: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchHourV1 {
    pub start_utc_ms: i64,
    pub end_utc_ms: i64,
    pub cumulative: ResearchSnapshotV1,
    pub metric_changes: BTreeMap<String, ExecutionReadValueV1<ExactRationalV1>>,
    pub counter_changes: BTreeMap<String, i128>,
    pub decision_changes: Option<DecisionCountsV1>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchSummaryV1 {
    /// 旧检查点没有分类时保持None，不能从原total回造NoTrade。
    pub decisions: Option<DecisionCountsV1>,
    pub hours: Vec<ResearchHourV1>,
    pub final_snapshot: Option<ResearchSnapshotV1>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ResearchStateV1 {
    pub decisions: Option<DecisionCountsV1>,
    pub next_boundary: i64,
    pub baseline: ResearchSnapshotV1,
    pub hours: Vec<ResearchHourV1>,
}
fn rational(v: &ExactRationalV1) -> R {
    R::try_from(v).expect("原经济读取的规范有理数")
}
fn amount(v: &ExactDecimal) -> R {
    R::from_decimal(v.0.parse().expect("原经济读取的精确金额"))
}
impl ResearchStateV1 {
    pub fn new(mut baseline: ResearchSnapshotV1, fresh: bool) -> Self {
        baseline.decisions = fresh.then(DecisionCountsV1::default);
        Self {
            decisions: baseline.decisions.clone(),
            next_boundary: (baseline.boundary_utc_ms.div_euclid(3_600_000) + 1) * 3_600_000,
            baseline,
            hours: vec![],
        }
    }
    pub fn push(&mut self, mut snapshot: ResearchSnapshotV1) {
        snapshot.decisions = self.decisions.clone();
        let previous = self
            .hours
            .last()
            .map(|h| &h.cumulative)
            .unwrap_or(&self.baseline);
        let metric_changes = snapshot
            .metrics
            .iter()
            .map(|(k, v)| {
                let change = match (v, previous.metrics.get(k)) {
                    (ExecutionReadValueV1::Known(v), Some(ExecutionReadValueV1::Known(p))) => {
                        ExecutionReadValueV1::Known(rational(v).sub(&rational(p)).public())
                    }
                    _ => ExecutionReadValueV1::Unavailable,
                };
                (k.clone(), change)
            })
            .collect();
        let counter_changes = snapshot
            .counters
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    i128::from(*v) - i128::from(previous.counters.get(k).copied().unwrap_or(0)),
                )
            })
            .collect();
        let decision_changes = snapshot
            .decisions
            .as_ref()
            .zip(previous.decisions.as_ref())
            .map(|(a, b)| DecisionCountsV1 {
                successful: a.successful - b.successful,
                no_trade: a.no_trade - b.no_trade,
                actions: a.actions - b.actions,
                exits: a.exits - b.exits,
            });
        self.hours.push(ResearchHourV1 {
            start_utc_ms: previous.boundary_utc_ms,
            end_utc_ms: self.next_boundary,
            cumulative: snapshot,
            metric_changes,
            counter_changes,
            decision_changes,
        });
        self.next_boundary += 3_600_000;
    }
    pub fn summary(&self, mut final_snapshot: Option<ResearchSnapshotV1>) -> ResearchSummaryV1 {
        if let Some(s) = &mut final_snapshot {
            s.decisions = self.decisions.clone();
        }
        ResearchSummaryV1 {
            decisions: self.decisions.clone(),
            hours: self.hours.clone(),
            final_snapshot,
        }
    }
}

pub(crate) fn snapshot(
    boundary: i64,
    cutoff: Option<TrustedGroupDecisionCutoffV1>,
    shared: &SharedExecutionV2,
    economics: Option<&AccountEconomicsReadV2>,
    health: &SourceHealthRunSummaryV1,
    pending: Option<CalculationTimingRecordV1>,
    day_economics: &crate::day_economics::DayEconomicsStateV1,
) -> ResearchSnapshotV1 {
    use ExecutionReadValueV1::{Known, Unavailable};
    let mut metrics = BTreeMap::new();
    let mut add = |name: &str, value: R| {
        metrics.insert(name.to_owned(), Known(value.public()));
    };
    if let Some(e) = economics {
        add(
            "actual_available_pusd",
            amount(&e.cash.actual_available_pusd),
        );
        add("actual_reserved_pusd", amount(&e.cash.actual_reserved_pusd));
        add("maker_available_pusd", amount(&e.cash.maker_available_pusd));
        add("maker_reserved_pusd", amount(&e.cash.maker_reserved_pusd));
        add(
            "worst_case_maker_nav_pusd",
            rational(&e.worst_case_maker_nav_pusd),
        );
        let mut totals: BTreeMap<&str, R> = BTreeMap::new();
        for m in &e.markets {
            let mut sum = |key, value: R| {
                let entry = totals.entry(key).or_insert_with(R::zero);
                *entry = entry.add(&value);
            };
            sum(
                "confirmed_net_pnl_pusd",
                rational(&m.confirmed_net_pnl_pusd),
            );
            sum(
                "unresolved_orphan_principal_cost_pusd",
                rational(&m.unresolved_orphan_principal_cost_pusd),
            );
            sum(
                "orphan_shares",
                amount(&m.orphan_shares[0]).add(&amount(&m.orphan_shares[1])),
            );
            sum("paired_shares", amount(&m.paired_shares));
            for i in 0..2 {
                let executed = &m.executed[i];
                sum(
                    if i == 0 {
                        "maker_shares"
                    } else {
                        "taker_shares"
                    },
                    amount(&executed.shares),
                );
                sum(
                    if i == 0 {
                        "maker_principal_pusd"
                    } else {
                        "taker_principal_pusd"
                    },
                    amount(&executed.principal_pusd),
                );
                sum(
                    if i == 0 {
                        "maker_fee_pusd"
                    } else {
                        "taker_fee_pusd"
                    },
                    amount(&executed.actual_fee_pusd),
                );
                let rebate = &m.rebates[i];
                sum(
                    "confirmed_unpaid_rebates_pusd",
                    rational(&rebate.confirmed_unpaid_pusd),
                );
                sum(
                    "accrued_unmatured_rebates_pusd",
                    rational(&rebate.accrued_unmatured_pusd),
                );
                sum(
                    "below_threshold_rebates_pusd",
                    rational(&rebate.below_threshold_pusd),
                );
                sum(
                    "locked_remainder_rebates_pusd",
                    rational(&rebate.locked_remainder_pusd),
                );
                sum("paid_rebates_pusd", rational(&rebate.paid_pusd));
            }
        }
        for name in [
            "confirmed_net_pnl_pusd",
            "unresolved_orphan_principal_cost_pusd",
            "orphan_shares",
            "paired_shares",
            "maker_shares",
            "taker_shares",
            "maker_principal_pusd",
            "taker_principal_pusd",
            "maker_fee_pusd",
            "taker_fee_pusd",
            "confirmed_unpaid_rebates_pusd",
            "accrued_unmatured_rebates_pusd",
            "below_threshold_rebates_pusd",
            "locked_remainder_rebates_pusd",
            "paid_rebates_pusd",
        ] {
            add(name, totals.remove(name).unwrap_or_else(R::zero));
        }
        metrics.insert(
            "conservative_nav_pusd".into(),
            e.conservative_nav_pusd.clone(),
        );
    } else {
        metrics.insert("account_economics".into(), Unavailable);
    }
    let mut counters = BTreeMap::from([
        (
            "health_qualification_calls".into(),
            health.qualification_calls,
        ),
        ("health_deadline_wakeups".into(), health.deadline_wakeups),
        (
            "health_invalidated_batches".into(),
            health.invalidated_batches,
        ),
        (
            "unclosed_commitments".into(),
            shared.registry().all_commitments().count() as u64,
        ),
        (
            "pending_noe_records".into(),
            shared.pending_noe_record_count() as u64,
        ),
    ]);
    let requests = shared.registry().cumulative_request_totals();
    counters.extend([
        (
            "registered_requests_without_receipt".into(),
            requests.registered_requests_without_receipt,
        ),
        (
            "receipted_submission_requests".into(),
            requests.receipted_submission_requests,
        ),
        (
            "receipted_submission_requested_legs".into(),
            requests.receipted_submission_requested_legs,
        ),
        (
            "receipted_cancellation_requests".into(),
            requests.receipted_cancellation_requests,
        ),
        (
            "completed_receipted_cancellation_requests".into(),
            requests.completed_receipted_cancellation_requests,
        ),
        (
            "receipted_canceled_responses".into(),
            requests.receipted_cancellation_response_counts[0],
        ),
        (
            "receipted_not_canceled_responses".into(),
            requests.receipted_cancellation_response_counts[1],
        ),
        (
            "receipted_unknown_cancel_responses".into(),
            requests.receipted_cancellation_response_counts[2],
        ),
    ]);
    if let Some(read) = shared.ledger().economic_read() {
        let mut applied = [0; 2];
        let mut successful = [0; 2];
        for trade in read.trades() {
            let role = usize::from(trade.fact.liquidity_role == TradeLiquidityRoleV2::Taker);
            applied[role] += u64::from(trade.economic_applied);
            successful[role] += u64::from(trade.success_applied);
        }
        counters.extend([
            ("maker_economic_trade_count".into(), applied[0]),
            ("taker_economic_trade_count".into(), applied[1]),
            ("maker_success_trade_count".into(), successful[0]),
            ("taker_success_trade_count".into(), successful[1]),
        ]);
    }
    let summary = day_economics.summary();
    metrics.insert(
        "committed_capital_pusd_milliseconds".into(),
        summary.committed_capital_pusd_milliseconds.clone(),
    );
    counters.insert("measured_duration_ms".into(), summary.measured_duration_ms);
    counters.insert(
        "unknown_capital_duration_ms".into(),
        summary.unknown_capital_duration_ms,
    );
    let mut open = 0;
    let mut unknown = 0;
    for order in shared.registry().orders() {
        match order.active_unfilled_shares {
            Known(q) if amount(&q).is_positive() => open += 1,
            Unavailable => unknown += 1,
            _ => (),
        }
    }
    counters.insert("open_order_count".into(), open);
    counters.insert("unknown_order_state_count".into(), unknown);
    counters.insert(
        "pending_receipted_cancellation_requests".into(),
        requests.receipted_cancellation_requests
            - requests.completed_receipted_cancellation_requests,
    );
    ResearchSnapshotV1 {
        boundary_utc_ms: boundary,
        observation_cutoff: serde_json::to_value(cutoff).expect("原cutoff序列化"),
        metrics,
        counters,
        decisions: None,
        pending_calculation: pending,
        day_economics: serde_json::to_value(day_economics.summary()).expect("原资金统计序列化"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn successful_classification_and_legacy_unknown_survive_hour_checkpoint() {
        let mut counts = DecisionCountsV1::default();
        counts.record(&StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        });
        counts.record(&StrategyOutputV1::Continue {
            intents: vec![shared_run::ReservedIntentV1 {
                context: sim_system::execution::RequestContextV2 {
                    global_request_id: "cancel".into(),
                    caller_correlation_id: None,
                },
                intent: std::sync::Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId("fixture".into()),
                }),
                reserves: vec![],
            }],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        });
        counts.record(&StrategyOutputV1::Exit { diagnostics: () });
        assert_eq!(
            counts,
            DecisionCountsV1 {
                successful: 3,
                no_trade: 1,
                actions: 1,
                exits: 1
            }
        );
        let baseline = ResearchSnapshotV1 {
            boundary_utc_ms: 1_000,
            observation_cutoff: serde_json::Value::Null,
            metrics: BTreeMap::from([(
                "nav".into(),
                ExecutionReadValueV1::Known(R::from_decimal(10.into()).public()),
            )]),
            counters: BTreeMap::from([("open".into(), 2)]),
            decisions: None,
            pending_calculation: None,
            day_economics: serde_json::Value::Null,
        };
        let mut legacy = ResearchStateV1::new(baseline.clone(), false);
        let mut next = baseline.clone();
        next.boundary_utc_ms = 3_600_000;
        next.metrics.insert(
            "nav".into(),
            ExecutionReadValueV1::Known(R::from_decimal(9.into()).public()),
        );
        next.counters.insert("open".into(), 1);
        legacy.push(next);
        assert_eq!(legacy.hours[0].start_utc_ms, 1_000);
        assert_eq!(legacy.hours[0].counter_changes["open"], -1);
        assert_eq!(
            legacy.hours[0].metric_changes["nav"],
            ExecutionReadValueV1::Known(R::from_decimal((-1).into()).public())
        );
        let mut bytes = vec![];
        ciborium::into_writer(&legacy, &mut bytes).unwrap();
        let restored: ResearchStateV1 = ciborium::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(restored.next_boundary, 7_200_000);
        assert!(restored.decisions.is_none());
        assert!(restored.hours[0].decision_changes.is_none());
    }
}
