//! 显式有限的 RESOLUTION_REDEEM_ONLY 后端。调度、记录和资金仍归原执行器。
//! 首次请求后固定脚本：内存调度共享 Arc，Configure 的持久输入仍是原脚本结构。
//! 同一条件的重送沿首次登记请求和重试状态推进；费用只在确认或支付到点时解析。
use super::*;
use contracts::economics::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionRedeemScenarioV1 {
    pub steps: Vec<ResolutionRedeemScenarioStepV1>,
    pub costs: Vec<ScenarioSettlementCostV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionRedeemScenarioStepV1 {
    pub offset_ms: u64,
    pub event: ResolutionRedeemScenarioEventV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolutionRedeemScenarioEventV1 {
    SubmissionReceipt(SettlementSubmissionReceiptV1),
    ObservationTimeout,
    QueryReply(SettlementQueryReplyV1),
    RetryAuthorized,
    Terminal(SettlementTerminalV1),
    RepeatPriorReceipt { prior_step_index: usize },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementSubmissionReceiptV1 {
    Registered,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementQueryReplyV1 {
    Unknown,
    AcceptedPending,
    FinalSuccess,
    ConfirmedNoAcceptedOrSuccessfulTransaction,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementTerminalV1 {
    Success,
    FailureNoEffect,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioSettlementCostV1 {
    #[serde(default)]
    pub correction_of: Option<String>,
    pub cost_identity: String,
    pub recognition_offset_ms: u64,
    pub payment_offset_ms: Option<u64>,
    pub amount_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SettlementOperationV1 {
    ExternalCash {
        fact: ExternalCashFactV1<String>,
        at_utc_ms: i64,
    },
    Configure(ResolutionRedeemScenarioV1),
    Request {
        request: SettlementRequestV1,
        requested_at_utc_ms: i64,
    },
    Query {
        identity: SettlementRequestIdV1<String>,
        requested_at_utc_ms: i64,
    },
}
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RequestedSettlementStateV1 {
    pub scenario: Option<Arc<ResolutionRedeemScenarioV1>>,
    external_cash: BTreeSet<String>,
    requests: BTreeMap<String, RequestedSettlementV1>,
}
#[derive(Debug, PartialEq, Eq)]
struct RequestedSettlementV1 {
    request: SettlementRequestV1,
    first_request_at: i64,
    sends: u64,
    query_count: u64,
    consumed_queries: u64,
    last_query: Option<SettlementQueryReplyV1>,
    retry_authorized: bool,
    accepted: bool,
    succeeded: bool,
    step: usize,
    receipts: Vec<Option<SettlementLifecycleEffectV2>>,
    cost_status: Vec<(bool, bool)>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettlementSimulationReadV1 {
    pub request: SettlementRequestV1,
    pub sends: u64,
    pub queries: u64,
    pub final_success: bool,
}
impl ExecutionStateV2 {
    pub(crate) fn requested_settlement_resources(&self) -> crate::resource::ResourceQuantityV1 {
        let mut count = crate::resource::ResourceQuantityV1::default();
        if let Some(s) = &self.requested_settlement.scenario {
            count.records += 1 + s.steps.len() as u64 + s.costs.len() as u64;
            count.bytes += s
                .costs
                .iter()
                .map(|c| {
                    (c.cost_identity.len()
                        + c.amount_pusd.0.len()
                        + c.correction_of.as_ref().map_or(0, String::len))
                        as u64
                })
                .sum::<u64>();
        }
        for identity in &self.requested_settlement.external_cash {
            count.records += 1;
            count.bytes += identity.len() as u64;
        }
        for (key, r) in &self.requested_settlement.requests {
            count.records += 1 + r.cost_status.len() as u64;
            count.bytes += key.len() as u64
                + crate::run_v2::settlement_request_owned_bytes_v1(&r.request.identity);
            for receipt in r.receipts.iter().flatten() {
                count.records += 1;
                count.bytes += crate::run_v2::settlement_event_owned_bytes_v2(&receipt.event);
            }
        }
        count
    }

    pub(crate) fn settlement_operation(
        &mut self,
        operation: &SettlementOperationV1,
    ) -> Result<Vec<LifecycleFeedbackEffectV2>, MatchingStateErrorV1> {
        match operation {
            SettlementOperationV1::ExternalCash { fact, at_utc_ms } => {
                if self
                    .requested_settlement
                    .external_cash
                    .contains(&fact.identity)
                {
                    return Ok(Vec::new());
                }
                let amount = Decimal::from_str_exact(&fact.signed_delta_pusd.0)
                    .map_err(|_| MatchingStateErrorV1::QueuePredecessorConflict)?;
                let account_delta =
                    self.account
                        .apply_external_cash(&fact.identity, amount, *at_utc_ms)?;
                self.requested_settlement
                    .external_cash
                    .insert(fact.identity.clone());
                return Ok(vec![LifecycleFeedbackEffectV2::Settlement(
                    SettlementLifecycleEffectV2 {
                        repeated_at_utc_ms: None,
                        platform_event_at_utc_ms: *at_utc_ms,
                        event: SettlementLifecycleEventV2::ExternalCash {
                            fact: fact.clone(),
                            account_delta,
                        },
                    },
                )]);
            }
            SettlementOperationV1::Configure(scenario) => {
                // 脚本是显式运行输入，必须在其第一笔请求之前固定。
                if !self.requested_settlement.requests.is_empty() {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
                self.requested_settlement.scenario = Some(Arc::new(scenario.clone()));
            }
            SettlementOperationV1::Request {
                request,
                requested_at_utc_ms,
            } => {
                let Some(scenario) = &self.requested_settlement.scenario else {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                };
                let key = &request.identity.condition_id.0;
                if let Some(existing) = self.requested_settlement.requests.get_mut(key) {
                    if existing.retry_authorized
                        && !existing.accepted
                        && !existing.succeeded
                        && existing.last_query
                            == Some(
                                SettlementQueryReplyV1::ConfirmedNoAcceptedOrSuccessfulTransaction,
                            )
                    {
                        existing.sends += 1;
                        existing.retry_authorized = false;
                        existing.last_query = None;
                    }
                    return Ok(Vec::new());
                }
                let Some(resolution) = self.market_resolutions.get(key) else {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                };
                if request.identity.account != *self.account.account_instance_key()
                    || request.identity.resolution_identity != resolution.resolution_event_id
                    || request.identity.policy_version != "RESOLUTION_REDEEM_ONLY"
                    || request.index_sets != [U256::from(1u64), U256::from(2u64)]
                    || !resolution.pending_fill_sequences.is_empty()
                    || self
                        .maker_rebate
                        .markets
                        .get(key)
                        .and_then(|m| m.result)
                        .is_none()
                {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
                self.requested_settlement.requests.insert(
                    key.clone(),
                    RequestedSettlementV1 {
                        request: request.clone(),
                        first_request_at: *requested_at_utc_ms,
                        sends: 1,
                        query_count: 0,
                        consumed_queries: 0,
                        last_query: None,
                        retry_authorized: false,
                        accepted: false,
                        succeeded: false,
                        step: 0,
                        receipts: vec![None; scenario.steps.len()],
                        cost_status: vec![(false, false); scenario.costs.len()],
                    },
                );
            }
            SettlementOperationV1::Query { identity, .. } => {
                let Some(request) = self
                    .requested_settlement
                    .requests
                    .get_mut(&identity.condition_id.0)
                else {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                };
                if request.request.identity != *identity {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
                request.query_count += 1;
            }
        }
        Ok(Vec::new())
    }

    pub(crate) fn settlement_simulation_read(
        &self,
        condition: &ConditionId,
    ) -> Option<SettlementSimulationReadV1> {
        self.requested_settlement
            .requests
            .get(&condition.0)
            .map(|r| SettlementSimulationReadV1 {
                request: r.request.clone(),
                sends: r.sends,
                queries: r.query_count,
                final_success: r.succeeded,
            })
    }

    pub(crate) fn next_settlement_clock_at_utc_ms(&self) -> Option<i64> {
        let scenario = self.requested_settlement.scenario.as_ref()?;
        self.requested_settlement
            .requests
            .values()
            .flat_map(|r| {
                let step = scenario
                    .steps
                    .get(r.step)
                    .map(|s| r.first_request_at + s.offset_ms as i64);
                let costs = scenario.costs.iter().zip(&r.cost_status).flat_map(
                    |(c, &(recognized, paid))| {
                        [
                            (!recognized)
                                .then_some(r.first_request_at + c.recognition_offset_ms as i64),
                            if paid {
                                None
                            } else {
                                c.payment_offset_ms.map(|ms| r.first_request_at + ms as i64)
                            },
                        ]
                    },
                );
                std::iter::once(step).chain(costs).flatten()
            })
            .min()
    }

    pub(crate) fn advance_settlement_at(
        &mut self,
        at: i64,
    ) -> Result<Vec<LifecycleFeedbackEffectV2>, MatchingStateErrorV1> {
        let Some(scenario) = self.requested_settlement.scenario.clone() else {
            return Ok(Vec::new());
        };
        let keys: Vec<_> = self.requested_settlement.requests.keys().cloned().collect();
        let mut output = Vec::new();
        for key in keys {
            let mut r = self
                .requested_settlement
                .requests
                .remove(&key)
                .expect("已有请求");
            while let Some(step) = scenario
                .steps
                .get(r.step)
                .filter(|s| r.first_request_at + s.offset_ms as i64 == at)
            {
                use ResolutionRedeemScenarioEventV1 as E;
                let mut delta = None;
                let (outcome, observation) = match &step.event {
                    E::SubmissionReceipt(SettlementSubmissionReceiptV1::Registered) => {
                        r.accepted = true;
                        (
                            SettlementRequestOutcomeV1::Registered,
                            SettlementObservationV1::Registered,
                        )
                    }
                    E::SubmissionReceipt(SettlementSubmissionReceiptV1::Unknown) => (
                        SettlementRequestOutcomeV1::Pending,
                        SettlementObservationV1::SubmissionUnknown,
                    ),
                    E::ObservationTimeout => (
                        SettlementRequestOutcomeV1::Pending,
                        SettlementObservationV1::ObservationTimeout,
                    ),
                    E::QueryReply(reply) => {
                        if r.query_count == r.consumed_queries {
                            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                        }
                        r.consumed_queries += 1;
                        r.last_query = Some(*reply);
                        match reply {
                            SettlementQueryReplyV1::FinalSuccess => {
                                delta = self.apply_requested_redemption(&mut r, at)?;
                                (
                                    SettlementRequestOutcomeV1::FinalSuccess,
                                    SettlementObservationV1::QueryFinalSuccess,
                                )
                            }
                            SettlementQueryReplyV1::AcceptedPending => {
                                r.accepted = true;
                                (
                                    SettlementRequestOutcomeV1::Pending,
                                    SettlementObservationV1::QueryAcceptedPending,
                                )
                            }
                            SettlementQueryReplyV1::Unknown => (
                                SettlementRequestOutcomeV1::Pending,
                                SettlementObservationV1::QueryUnknown,
                            ),
                            SettlementQueryReplyV1::ConfirmedNoAcceptedOrSuccessfulTransaction => (
                                SettlementRequestOutcomeV1::Pending,
                                SettlementObservationV1::QueryConfirmedNoTransaction,
                            ),
                        }
                    }
                    E::RetryAuthorized => {
                        r.retry_authorized = !r.accepted && !r.succeeded && r.last_query == Some(SettlementQueryReplyV1::ConfirmedNoAcceptedOrSuccessfulTransaction);
                        (
                            SettlementRequestOutcomeV1::Pending,
                            if r.retry_authorized {
                                SettlementObservationV1::RetryAuthorized
                            } else {
                                SettlementObservationV1::RetryNotAuthorized
                            },
                        )
                    }
                    E::Terminal(SettlementTerminalV1::Success) => {
                        delta = self.apply_requested_redemption(&mut r, at)?;
                        (
                            SettlementRequestOutcomeV1::FinalSuccess,
                            SettlementObservationV1::TerminalSuccess,
                        )
                    }
                    E::Terminal(SettlementTerminalV1::FailureNoEffect) => (
                        SettlementRequestOutcomeV1::FinalFailureNoEffect,
                        SettlementObservationV1::TerminalFailureNoEffect,
                    ),
                    E::RepeatPriorReceipt { prior_step_index } => {
                        let mut receipt = r
                            .receipts
                            .get(*prior_step_index)
                            .and_then(Clone::clone)
                            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                        receipt.repeated_at_utc_ms = Some(at);
                        r.receipts[r.step] = Some(receipt.clone());
                        r.step += 1;
                        output.push(LifecycleFeedbackEffectV2::Settlement(receipt));
                        continue;
                    }
                };
                let receipt = SettlementLifecycleEffectV2 {
                    repeated_at_utc_ms: None,
                    platform_event_at_utc_ms: at,
                    event: SettlementLifecycleEventV2::RequestedSettlement {
                        result: RequestedSettlementResultV1::Request(SettlementRequestFactV1 {
                            index_sets: r.request.index_sets,
                            settlement_request_id: r.request.identity.clone(),
                            outcome,
                            observation,
                        }),
                        account_delta: delta,
                        settlement_fee_liability_pusd_delta: ExactDecimal("0".into()),
                    },
                };
                r.receipts[r.step] = Some(receipt.clone());
                r.step += 1;
                output.push(LifecycleFeedbackEffectV2::Settlement(receipt));
            }
            for (cost, status) in scenario.costs.iter().zip(&mut r.cost_status) {
                let recognize =
                    !status.0 && r.first_request_at + cost.recognition_offset_ms as i64 == at;
                let pay = !status.1
                    && cost
                        .payment_offset_ms
                        .is_some_and(|ms| r.first_request_at + ms as i64 == at);
                if !recognize && !pay {
                    continue;
                }
                let amount = Decimal::from_str_exact(&cost.amount_pusd.0)
                    .map_err(|_| MatchingStateErrorV1::QueuePredecessorConflict)?;
                for is_payment in [false, true] {
                    if !(if is_payment { pay } else { recognize }) {
                        continue;
                    }
                    if is_payment && !status.0 {
                        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                    }
                    let transition = if is_payment {
                        SourceNeutralAccountTransitionKindV2::SettlementCostPaid {
                            settlement_request_id: r.request.identity.clone(),
                            cost_identity: cost.cost_identity.clone(),
                        }
                    } else {
                        SourceNeutralAccountTransitionKindV2::SettlementCostRecognized {
                            settlement_request_id: r.request.identity.clone(),
                            cost_identity: cost.cost_identity.clone(),
                        }
                    };
                    let delta = self.account.apply_settlement_cost(
                        &r.request.identity.condition_id,
                        transition,
                        at,
                        if is_payment { amount } else { Decimal::ZERO },
                    )?;
                    if is_payment {
                        status.1 = true;
                    } else {
                        status.0 = true;
                    }
                    output.push(LifecycleFeedbackEffectV2::Settlement(
                        SettlementLifecycleEffectV2 {
                            repeated_at_utc_ms: None,
                            platform_event_at_utc_ms: at,
                            event: SettlementLifecycleEventV2::RequestedSettlement {
                                result: RequestedSettlementResultV1::Cost(SettlementCostFactV1 {
                                    settlement_request_id: r.request.identity.clone(),
                                    cost_identity: cost.cost_identity.clone(),
                                    recognized_pusd: cost.amount_pusd.clone(),
                                    paid_pusd: if status.1 {
                                        cost.amount_pusd.clone()
                                    } else {
                                        ExactDecimal("0".into())
                                    },
                                    correction_of: cost.correction_of.clone(),
                                }),
                                account_delta: Some(delta),
                                settlement_fee_liability_pusd_delta: ExactDecimal(
                                    (if is_payment { -amount } else { amount })
                                        .normalize()
                                        .to_string(),
                                ),
                            },
                        },
                    ));
                }
            }
            self.requested_settlement.requests.insert(key, r);
        }
        Ok(output)
    }

    fn apply_requested_redemption(
        &mut self,
        request: &mut RequestedSettlementV1,
        at: i64,
    ) -> Result<Option<PlatformAccountDeltaV2>, MatchingStateErrorV1> {
        if request.succeeded {
            return Ok(None);
        }
        let id = &request.request.identity;
        let obligation = self
            .market_resolutions
            .get(&id.condition_id.0)
            .expect("请求已绑定合格判定");
        let MarketResolutionTerminalV3::Resolved { payout_vector, .. } = &obligation.terminal;
        let payouts = [
            (
                payout_vector[0].asset.token_id.clone(),
                obligation.parsed_payouts[0],
            ),
            (
                payout_vector[1].asset.token_id.clone(),
                obligation.parsed_payouts[1],
            ),
        ];
        let mut redeemed = self.account.apply_auto_redeem(
            &id.resolution_identity,
            &id.condition_id,
            at,
            &payouts,
        )?;
        if let Some(delta) = &mut redeemed.account_delta {
            delta.reason = PlatformAccountDeltaReasonV2::RequestedSettlement(
                SourceNeutralAccountTransitionKindV2::ResolutionRedeemApplied {
                    resolution_identity: id.resolution_identity.clone(),
                    settlement_request_id: id.clone(),
                },
            );
        }
        self.market_resolutions
            .get_mut(&id.condition_id.0)
            .unwrap()
            .redeemed_at_utc_ms = Some(at);
        request.succeeded = true;
        request.accepted = true;
        Ok(redeemed.account_delta)
    }
}
