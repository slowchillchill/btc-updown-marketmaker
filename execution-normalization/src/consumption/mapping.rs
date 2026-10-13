//! 已解码耐久记录到内存身份的结构转换；其余载荷直接移动。
use super::*;
use contracts::economics::*;

fn logical(value: StableLogicalIdentityV1) -> Result<MemoryLogicalIdentityV1, serde_json::Error> {
    MemoryLogicalIdentityV1::deserialize(
        serde::de::value::StringDeserializer::<serde_json::Error>::new(value.0),
    )
}
fn memory_fact_reference(
    value: NoeEconomicFactReferenceV1,
) -> Result<MemoryEconomicFactReferenceV1, serde_json::Error> {
    MemoryEconomicFactReferenceV1::deserialize(serde::de::value::StringDeserializer::<
        serde_json::Error,
    >::new(value.0))
}

fn map_order_snapshot_v1(
    value: SourceNeutralOrderSnapshotV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralOrderSnapshotV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralOrderSnapshotV1 {
        order_identity,
        condition_id,
        token_id,
        state,
    } = value;
    Ok(SourceNeutralOrderSnapshotV1 {
        order_identity: logical(order_identity)?,
        condition_id,
        token_id,
        state,
    })
}

fn map_submission_outcome_v2(
    value: SourceNeutralSubmissionOutcomeV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralSubmissionOutcomeV2::Accepted {
            order,
            measurement,
            effective_processing_at_utc_ms,
        } => SourceNeutralSubmissionOutcomeV2::Accepted {
            order: map_order_snapshot_v1(order)?,
            measurement,
            effective_processing_at_utc_ms,
        },
        SourceNeutralSubmissionOutcomeV2::RejectedNoEffect => {
            SourceNeutralSubmissionOutcomeV2::RejectedNoEffect
        }
        SourceNeutralSubmissionOutcomeV2::EvidenceUnknownNoEffect => {
            SourceNeutralSubmissionOutcomeV2::EvidenceUnknownNoEffect
        }
    })
}

fn map_order_snapshot_v2(
    value: SourceNeutralOrderSnapshotV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralOrderSnapshotV2 {
        order_identity,
        condition_id,
        token_id,
        state,
        quantities,
    } = value;
    Ok(SourceNeutralOrderSnapshotV2 {
        order_identity: logical(order_identity)?,
        condition_id,
        token_id,
        state,
        quantities,
    })
}

fn map_order_query_result(
    value: SourceNeutralOrderQueryResult<SourceNeutralOrderSnapshotV2>,
) -> Result<
    SourceNeutralOrderQueryResult<SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>>,
    serde_json::Error,
> {
    Ok(match value {
        SourceNeutralOrderQueryResult::Found { order } => SourceNeutralOrderQueryResult::Found {
            order: map_order_snapshot_v2(order)?,
        },
        SourceNeutralOrderQueryResult::NotFound => SourceNeutralOrderQueryResult::NotFound,
        SourceNeutralOrderQueryResult::OutOfRetention(value) => {
            SourceNeutralOrderQueryResult::OutOfRetention(value)
        }
    })
}

fn map_orders_page(
    value: SourceNeutralOrdersPage<SourceNeutralOrderSnapshotV2>,
) -> Result<
    SourceNeutralOrdersPage<SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>>,
    serde_json::Error,
> {
    let SourceNeutralOrdersPage {
        root_sequence,
        items,
        next_cursor,
    } = value;
    Ok(SourceNeutralOrdersPage {
        root_sequence,
        items: items
            .into_iter()
            .map(map_order_snapshot_v2)
            .collect::<Result<_, serde_json::Error>>()?,
        next_cursor,
    })
}

fn map_orders_query_result(
    value: SourceNeutralOrdersQueryResult<SourceNeutralOrderSnapshotV2>,
) -> Result<
    SourceNeutralOrdersQueryResult<SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>>,
    serde_json::Error,
> {
    Ok(match value {
        SourceNeutralOrdersQueryResult::Page(value) => {
            SourceNeutralOrdersQueryResult::Page(map_orders_page(value)?)
        }
        SourceNeutralOrdersQueryResult::InvalidCursor => {
            SourceNeutralOrdersQueryResult::InvalidCursor
        }
        SourceNeutralOrdersQueryResult::OutOfRetention(value) => {
            SourceNeutralOrdersQueryResult::OutOfRetention(value)
        }
    })
}

fn map_trade_lifecycle_result_v1(
    value: SourceNeutralTradeLifecycleResultV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralTradeLifecycleResultV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralTradeLifecycleResultV1 {
        trade_identity,
        order_identity,
        condition_id,
        token_id,
        state,
        economic_disposition,
    } = value;
    Ok(SourceNeutralTradeLifecycleResultV1 {
        trade_identity: logical(trade_identity)?,
        order_identity: logical(order_identity)?,
        condition_id,
        token_id,
        state,
        economic_disposition,
    })
}

fn map_trades_page_v1(
    value: SourceNeutralTradesPageV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralTradesPageV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralTradesPageV1 {
        root_sequence,
        items,
        next_cursor,
    } = value;
    Ok(SourceNeutralTradesPageV1 {
        root_sequence,
        items: items
            .into_iter()
            .map(map_trade_lifecycle_result_v1)
            .collect::<Result<_, serde_json::Error>>()?,
        next_cursor,
    })
}

fn map_trades_query_result_v1(
    value: SourceNeutralTradesQueryResultV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralTradesQueryResultV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralTradesQueryResultV1::Page(value) => {
            SourceNeutralTradesQueryResultV1::Page(map_trades_page_v1(value)?)
        }
        SourceNeutralTradesQueryResultV1::InvalidCursor => {
            SourceNeutralTradesQueryResultV1::InvalidCursor
        }
        SourceNeutralTradesQueryResultV1::OutOfRetention(value) => {
            SourceNeutralTradesQueryResultV1::OutOfRetention(value)
        }
    })
}

fn map_control_result<R>(
    value: SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2,
        SourceNeutralOrderSnapshotV2,
        StableLogicalIdentityV1,
        R,
    >,
) -> Result<
    SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
        SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
        MemoryLogicalIdentityV1,
        R,
    >,
    serde_json::Error,
> {
    Ok(match value {
        SourceNeutralControlResult::CancellationRequestCompleted { result } => {
            SourceNeutralControlResult::CancellationRequestCompleted { result }
        }
        SourceNeutralControlResult::Submission { outcome } => {
            SourceNeutralControlResult::Submission {
                outcome: map_submission_outcome_v2(outcome)?,
            }
        }
        SourceNeutralControlResult::Cancellation {
            order_identity,
            outcome,
        } => SourceNeutralControlResult::Cancellation {
            order_identity: logical(order_identity)?,
            outcome,
        },
        SourceNeutralControlResult::OrderQuery { result } => {
            SourceNeutralControlResult::OrderQuery {
                result: map_order_query_result(result)?,
            }
        }
        SourceNeutralControlResult::OrdersQuery { result } => {
            SourceNeutralControlResult::OrdersQuery {
                result: map_orders_query_result(result)?,
            }
        }
        SourceNeutralControlResult::TradesQuery { result } => {
            SourceNeutralControlResult::TradesQuery {
                result: map_trades_query_result_v1(result)?,
            }
        }
    })
}

fn map_order_lifecycle_event_v1(
    value: SourceNeutralOrderLifecycleEventV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralOrderLifecycleEventV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralOrderLifecycleEventV1::Accepted { state } => {
            SourceNeutralOrderLifecycleEventV1::Accepted { state }
        }
        SourceNeutralOrderLifecycleEventV1::FakUnspentReleased => {
            SourceNeutralOrderLifecycleEventV1::FakUnspentReleased
        }
        SourceNeutralOrderLifecycleEventV1::VenueDustTerminal => {
            SourceNeutralOrderLifecycleEventV1::VenueDustTerminal
        }
        SourceNeutralOrderLifecycleEventV1::MarketResolvedCanceled {
            resolution_identity,
        } => SourceNeutralOrderLifecycleEventV1::MarketResolvedCanceled {
            resolution_identity: logical(resolution_identity)?,
        },
        SourceNeutralOrderLifecycleEventV1::CallerCanceled => {
            SourceNeutralOrderLifecycleEventV1::CallerCanceled
        }
        SourceNeutralOrderLifecycleEventV1::GtdExpired => {
            SourceNeutralOrderLifecycleEventV1::GtdExpired
        }
    })
}

fn map_order_lifecycle_result_v1(
    value: SourceNeutralOrderLifecycleResultV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralOrderLifecycleResultV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralOrderLifecycleResultV1 {
        order_identity,
        condition_id,
        token_id,
        event,
    } = value;
    Ok(SourceNeutralOrderLifecycleResultV1 {
        order_identity: logical(order_identity)?,
        condition_id,
        token_id,
        event: map_order_lifecycle_event_v1(event)?,
    })
}

fn map_trade_lifecycle_result_v2(
    value: SourceNeutralTradeLifecycleResultV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralTradeLifecycleResultV2 {
        trade_identity,
        order_identity,
        condition_id,
        token_id,
        state,
        economic_disposition,
        fill_sequence,
        matched_at_utc_ms,
        liquidity_role,
        source_price_pusd_per_share,
        base_quantity_shares,
        quote_amount_pusd,
        immediate_taker_fee_pusd,
        economic_application_account_transition_seq,
    } = value;
    Ok(SourceNeutralTradeLifecycleResultV2 {
        trade_identity: logical(trade_identity)?,
        order_identity: logical(order_identity)?,
        condition_id,
        token_id,
        state,
        economic_disposition,
        fill_sequence,
        matched_at_utc_ms,
        liquidity_role,
        source_price_pusd_per_share,
        base_quantity_shares,
        quote_amount_pusd,
        immediate_taker_fee_pusd,
        economic_application_account_transition_seq,
    })
}

fn map_settlement_lifecycle_result_v1(
    value: SourceNeutralSettlementLifecycleResultV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralSettlementLifecycleResultV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralSettlementLifecycleResultV1::MakerRebateAccrualFinalized {
            condition_id,
            status,
        } => SourceNeutralSettlementLifecycleResultV1::MakerRebateAccrualFinalized {
            condition_id,
            status,
        },
        SourceNeutralSettlementLifecycleResultV1::TakerRebateAccrualApplied {
            trade_identity,
            condition_id,
            attribution_day_start_utc_ms,
        } => SourceNeutralSettlementLifecycleResultV1::TakerRebateAccrualApplied {
            trade_identity: logical(trade_identity)?,
            condition_id,
            attribution_day_start_utc_ms,
        },
        SourceNeutralSettlementLifecycleResultV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms,
        } => SourceNeutralSettlementLifecycleResultV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms,
        },
        SourceNeutralSettlementLifecycleResultV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms,
        } => SourceNeutralSettlementLifecycleResultV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms,
        },
        SourceNeutralSettlementLifecycleResultV1::MakerRebatePaid {
            attribution_day_start_utc_ms,
        } => SourceNeutralSettlementLifecycleResultV1::MakerRebatePaid {
            attribution_day_start_utc_ms,
        },
        SourceNeutralSettlementLifecycleResultV1::TakerRebatePaid {
            attribution_day_start_utc_ms,
        } => SourceNeutralSettlementLifecycleResultV1::TakerRebatePaid {
            attribution_day_start_utc_ms,
        },
        SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
            condition_id,
            resolution_identity,
            resolution_source_event_at_utc_ms,
            resolution_public_consumed_at_utc_ms,
        } => SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
            condition_id,
            resolution_identity: logical(resolution_identity)?,
            resolution_source_event_at_utc_ms,
            resolution_public_consumed_at_utc_ms,
        },
    })
}

fn map_trade_lifecycle_result_v3(
    value: SourceNeutralTradeLifecycleResultV3<StableLogicalIdentityV1>,
) -> Result<SourceNeutralTradeLifecycleResultV3<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralTradeLifecycleResultV3 {
        result,
        maker_fee_equivalent_pusd,
    } = value;
    Ok(SourceNeutralTradeLifecycleResultV3 {
        result: map_trade_lifecycle_result_v2(result)?,
        maker_fee_equivalent_pusd,
    })
}

fn map_rebate_contribution_fact_v1(
    value: RebateContributionFactV1<StableLogicalIdentityV1>,
) -> Result<RebateContributionFactV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let RebateContributionFactV1 {
        trade_identity,
        condition_id,
        kind,
        attribution_day_start_utc_ms,
        contribution_pusd,
    } = value;
    Ok(RebateContributionFactV1 {
        trade_identity: logical(trade_identity)?,
        condition_id,
        kind,
        attribution_day_start_utc_ms,
        contribution_pusd,
    })
}

fn map_rebate_day_result_v1(
    value: RebateDayResultV1<StableLogicalIdentityV1>,
) -> Result<RebateDayResultV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let RebateDayResultV1 {
        kind,
        attribution_day_start_utc_ms,
        accrued_pusd,
        payable_pusd,
        locked_remainder_pusd,
        contribution_refs,
    } = value;
    Ok(RebateDayResultV1 {
        kind,
        attribution_day_start_utc_ms,
        accrued_pusd,
        payable_pusd,
        locked_remainder_pusd,
        contribution_refs: contribution_refs
            .into_iter()
            .map(logical)
            .collect::<Result<_, serde_json::Error>>()?,
    })
}

fn map_rebate_payment_fact_v1(
    value: RebatePaymentFactV1<StableLogicalIdentityV1>,
) -> Result<RebatePaymentFactV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let RebatePaymentFactV1 {
        kind,
        attribution_day_start_utc_ms,
        day_result_identity,
        paid_pusd,
    } = value;
    Ok(RebatePaymentFactV1 {
        kind,
        attribution_day_start_utc_ms,
        day_result_identity: logical(day_result_identity)?,
        paid_pusd,
    })
}

fn map_resolved_position_fact_v1(
    value: ResolvedPositionFactV1<StableLogicalIdentityV1>,
) -> Result<ResolvedPositionFactV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let ResolvedPositionFactV1 {
        condition_id,
        resolution_identity,
        payout_vector,
    } = value;
    Ok(ResolvedPositionFactV1 {
        condition_id,
        resolution_identity: logical(resolution_identity)?,
        payout_vector,
    })
}

fn map_settlement_request_id_v1(
    value: SettlementRequestIdV1<StableLogicalIdentityV1>,
) -> Result<SettlementRequestIdV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SettlementRequestIdV1 {
        account,
        condition_id,
        resolution_identity,
        policy_version,
    } = value;
    Ok(SettlementRequestIdV1 {
        account,
        condition_id,
        resolution_identity: logical(resolution_identity)?,
        policy_version,
    })
}

fn map_settlement_request_fact_v1(
    value: SettlementRequestFactV1<StableLogicalIdentityV1>,
) -> Result<SettlementRequestFactV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SettlementRequestFactV1 {
        index_sets,
        settlement_request_id,
        outcome,
        observation,
    } = value;
    Ok(SettlementRequestFactV1 {
        index_sets,
        settlement_request_id: map_settlement_request_id_v1(settlement_request_id)?,
        outcome,
        observation,
    })
}

fn map_settlement_cost_fact_v1(
    value: SettlementCostFactV1<StableLogicalIdentityV1>,
) -> Result<SettlementCostFactV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SettlementCostFactV1 {
        settlement_request_id,
        cost_identity,
        recognized_pusd,
        paid_pusd,
        correction_of,
    } = value;
    Ok(SettlementCostFactV1 {
        settlement_request_id: map_settlement_request_id_v1(settlement_request_id)?,
        cost_identity: logical(cost_identity)?,
        recognized_pusd,
        paid_pusd,
        correction_of: correction_of.map(logical).transpose()?,
    })
}

fn map_external_cash_fact_v1(
    value: ExternalCashFactV1<StableLogicalIdentityV1>,
) -> Result<ExternalCashFactV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    let ExternalCashFactV1 {
        identity,
        signed_delta_pusd,
        attribution,
        correction_of,
    } = value;
    Ok(ExternalCashFactV1 {
        identity: logical(identity)?,
        signed_delta_pusd,
        attribution,
        correction_of: correction_of.map(logical).transpose()?,
    })
}

fn map_settlement_lifecycle_result_v2(
    value: SourceNeutralSettlementLifecycleResultV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralSettlementLifecycleResultV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralSettlementLifecycleResultV2::MakerAccrualFinalized {
            condition_id,
            status,
            contributions,
        } => SourceNeutralSettlementLifecycleResultV2::MakerAccrualFinalized {
            condition_id,
            status,
            contributions: contributions
                .into_iter()
                .map(map_rebate_contribution_fact_v1)
                .collect::<Result<_, serde_json::Error>>()?,
        },
        SourceNeutralSettlementLifecycleResultV2::Contribution(value) => {
            SourceNeutralSettlementLifecycleResultV2::Contribution(map_rebate_contribution_fact_v1(
                value,
            )?)
        }
        SourceNeutralSettlementLifecycleResultV2::DayFinalized(value) => {
            SourceNeutralSettlementLifecycleResultV2::DayFinalized(map_rebate_day_result_v1(value)?)
        }
        SourceNeutralSettlementLifecycleResultV2::Payment(value) => {
            SourceNeutralSettlementLifecycleResultV2::Payment(map_rebate_payment_fact_v1(value)?)
        }
        SourceNeutralSettlementLifecycleResultV2::Resolved(value) => {
            SourceNeutralSettlementLifecycleResultV2::Resolved(map_resolved_position_fact_v1(
                value,
            )?)
        }
        SourceNeutralSettlementLifecycleResultV2::Request(value) => {
            SourceNeutralSettlementLifecycleResultV2::Request(map_settlement_request_fact_v1(
                value,
            )?)
        }
        SourceNeutralSettlementLifecycleResultV2::Cost(value) => {
            SourceNeutralSettlementLifecycleResultV2::Cost(map_settlement_cost_fact_v1(value)?)
        }
        SourceNeutralSettlementLifecycleResultV2::ExternalCash(value) => {
            SourceNeutralSettlementLifecycleResultV2::ExternalCash(map_external_cash_fact_v1(
                value,
            )?)
        }
    })
}

fn map_account_transition_kind_v1(
    value: SourceNeutralAccountTransitionKindV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralAccountTransitionKindV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity,
        } => SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: logical(order_identity)?,
        },
        SourceNeutralAccountTransitionKindV1::FakUnspentEncumbranceReleased { order_identity } => {
            SourceNeutralAccountTransitionKindV1::FakUnspentEncumbranceReleased {
                order_identity: logical(order_identity)?,
            }
        }
        SourceNeutralAccountTransitionKindV1::CallerCancellationEncumbranceReleased {
            order_identity,
        } => SourceNeutralAccountTransitionKindV1::CallerCancellationEncumbranceReleased {
            order_identity: logical(order_identity)?,
        },
        SourceNeutralAccountTransitionKindV1::GtdExpirationEncumbranceReleased {
            order_identity,
            encoded_expires_at_utc_ms,
        } => SourceNeutralAccountTransitionKindV1::GtdExpirationEncumbranceReleased {
            order_identity: logical(order_identity)?,
            encoded_expires_at_utc_ms,
        },
        SourceNeutralAccountTransitionKindV1::VenueDustEncumbranceReleased { order_identity } => {
            SourceNeutralAccountTransitionKindV1::VenueDustEncumbranceReleased {
                order_identity: logical(order_identity)?,
            }
        }
        SourceNeutralAccountTransitionKindV1::MarketResolutionEncumbranceReleased {
            resolution_identity,
            order_identity,
        } => SourceNeutralAccountTransitionKindV1::MarketResolutionEncumbranceReleased {
            resolution_identity: logical(resolution_identity)?,
            order_identity: logical(order_identity)?,
        },
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity,
            path_ordinal,
        } => SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: logical(trade_identity)?,
            path_ordinal,
        },
        SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased {
            trade_identity,
            path_ordinal,
        } => SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased {
            trade_identity: logical(trade_identity)?,
            path_ordinal,
        },
        SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity,
            path_ordinal,
        } => SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity: logical(trade_identity)?,
            path_ordinal,
        },
        SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized => {
            SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized
        }
        SourceNeutralAccountTransitionKindV1::TakerRebateAccrualApplied { trade_identity } => {
            SourceNeutralAccountTransitionKindV1::TakerRebateAccrualApplied {
                trade_identity: logical(trade_identity)?,
            }
        }
        SourceNeutralAccountTransitionKindV1::MakerRebateDayFinalized {
            attribution_day_utc_ms,
        } => SourceNeutralAccountTransitionKindV1::MakerRebateDayFinalized {
            attribution_day_utc_ms,
        },
        SourceNeutralAccountTransitionKindV1::TakerRebateDayFinalized {
            attribution_day_utc_ms,
        } => SourceNeutralAccountTransitionKindV1::TakerRebateDayFinalized {
            attribution_day_utc_ms,
        },
        SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
            attribution_day_utc_ms,
        } => SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
            attribution_day_utc_ms,
        },
        SourceNeutralAccountTransitionKindV1::TakerRebatePaid {
            attribution_day_utc_ms,
        } => SourceNeutralAccountTransitionKindV1::TakerRebatePaid {
            attribution_day_utc_ms,
        },
        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
            resolution_identity,
        } => SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
            resolution_identity: logical(resolution_identity)?,
        },
    })
}

fn map_account_transition_kind_v2(
    value: SourceNeutralAccountTransitionKindV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralAccountTransitionKindV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralAccountTransitionKindV2::Existing(value) => {
            SourceNeutralAccountTransitionKindV2::Existing(map_account_transition_kind_v1(value)?)
        }
        SourceNeutralAccountTransitionKindV2::ResolutionRedeemApplied {
            resolution_identity,
            settlement_request_id,
        } => SourceNeutralAccountTransitionKindV2::ResolutionRedeemApplied {
            resolution_identity: logical(resolution_identity)?,
            settlement_request_id: map_settlement_request_id_v1(settlement_request_id)?,
        },
        SourceNeutralAccountTransitionKindV2::SettlementCostRecognized {
            settlement_request_id,
            cost_identity,
        } => SourceNeutralAccountTransitionKindV2::SettlementCostRecognized {
            settlement_request_id: map_settlement_request_id_v1(settlement_request_id)?,
            cost_identity: logical(cost_identity)?,
        },
        SourceNeutralAccountTransitionKindV2::SettlementCostPaid {
            settlement_request_id,
            cost_identity,
        } => SourceNeutralAccountTransitionKindV2::SettlementCostPaid {
            settlement_request_id: map_settlement_request_id_v1(settlement_request_id)?,
            cost_identity: logical(cost_identity)?,
        },
        SourceNeutralAccountTransitionKindV2::ExternalCashApplied { cash_identity } => {
            SourceNeutralAccountTransitionKindV2::ExternalCashApplied {
                cash_identity: logical(cash_identity)?,
            }
        }
    })
}

fn map_economic_disposition_v2(
    value: SourceNeutralEconomicDispositionV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralEconomicDispositionV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralEconomicDispositionV2::AccountOpening { available_pusd } => {
            SourceNeutralEconomicDispositionV2::AccountOpening { available_pusd }
        }
        SourceNeutralEconomicDispositionV2::AccountTransition {
            economic_effective_at_utc_ms,
            condition_id,
            transition,
            balance_delta,
        } => SourceNeutralEconomicDispositionV2::AccountTransition {
            economic_effective_at_utc_ms,
            condition_id,
            transition: map_account_transition_kind_v2(transition)?,
            balance_delta,
        },
    })
}

fn map_economic_evidence_v2(
    value: SourceNeutralEconomicEvidenceV2<StableLogicalIdentityV1>,
) -> Result<SourceNeutralEconomicEvidenceV2<MemoryLogicalIdentityV1>, serde_json::Error> {
    let SourceNeutralEconomicEvidenceV2 {
        economic_key,
        disposition,
    } = value;
    Ok(SourceNeutralEconomicEvidenceV2 {
        economic_key,
        disposition: map_economic_disposition_v2(disposition)?,
    })
}

fn map_economic_evidence_v1(
    value: SourceNeutralEconomicEvidenceV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralEconomicEvidenceV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity,
            available_pusd,
        } => SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity,
            available_pusd,
        },
        SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq,
            economic_effective_at_utc_ms,
            condition_id,
            transition,
            balance_delta,
        } => SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq,
            economic_effective_at_utc_ms,
            condition_id,
            transition: map_account_transition_kind_v1(transition)?,
            balance_delta,
        },
    })
}

fn map_evidence_kind<R>(
    value: SourceNeutralEvidenceKind<
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResult<
            SourceNeutralSubmissionOutcomeV2,
            SourceNeutralOrderSnapshotV2,
            StableLogicalIdentityV1,
            R,
        >,
        StableLogicalIdentityV1,
    >,
) -> Result<
    SourceNeutralEvidenceKind<
        SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
        SourceNeutralControlResult<
            SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
            SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
            MemoryLogicalIdentityV1,
            R,
        >,
        MemoryLogicalIdentityV1,
    >,
    serde_json::Error,
> {
    Ok(match value {
        SourceNeutralEvidenceKind::InitialMarket { result } => {
            SourceNeutralEvidenceKind::InitialMarket { result }
        }
        SourceNeutralEvidenceKind::MarketEnrollment { result } => {
            SourceNeutralEvidenceKind::MarketEnrollment { result }
        }
        SourceNeutralEvidenceKind::Control { result } => SourceNeutralEvidenceKind::Control {
            result: map_control_result(result)?,
        },
        SourceNeutralEvidenceKind::OrderLifecycle { result } => {
            SourceNeutralEvidenceKind::OrderLifecycle {
                result: map_order_lifecycle_result_v1(result)?,
            }
        }
        SourceNeutralEvidenceKind::TradeLifecycle { result } => {
            SourceNeutralEvidenceKind::TradeLifecycle {
                result: map_trade_lifecycle_result_v2(result)?,
            }
        }
        SourceNeutralEvidenceKind::SettlementLifecycle { result } => {
            SourceNeutralEvidenceKind::SettlementLifecycle {
                result: map_settlement_lifecycle_result_v1(result)?,
            }
        }
        SourceNeutralEvidenceKind::TradeLifecycleV3 { result } => {
            SourceNeutralEvidenceKind::TradeLifecycleV3 {
                result: map_trade_lifecycle_result_v3(result)?,
            }
        }
        SourceNeutralEvidenceKind::SettlementLifecycleV2 { result } => {
            SourceNeutralEvidenceKind::SettlementLifecycleV2 {
                result: map_settlement_lifecycle_result_v2(result)?,
            }
        }
        SourceNeutralEvidenceKind::EconomicV2 { evidence } => {
            SourceNeutralEvidenceKind::EconomicV2 {
                evidence: map_economic_evidence_v2(evidence)?,
            }
        }
        SourceNeutralEvidenceKind::Economic { evidence } => SourceNeutralEvidenceKind::Economic {
            evidence: map_economic_evidence_v1(evidence)?,
        },
        SourceNeutralEvidenceKind::LateEconomicCoverage { economic_key } => {
            SourceNeutralEvidenceKind::LateEconomicCoverage { economic_key }
        }
        SourceNeutralEvidenceKind::Pending { scope } => {
            SourceNeutralEvidenceKind::Pending { scope }
        }
    })
}

fn map_economic_disposition_v1(
    value: SourceNeutralEconomicDispositionV1<StableLogicalIdentityV1>,
) -> Result<SourceNeutralEconomicDispositionV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } => {
            SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd }
        }
        SourceNeutralEconomicDispositionV1::AccountTransition {
            economic_effective_at_utc_ms,
            condition_id,
            transition,
            balance_delta,
        } => SourceNeutralEconomicDispositionV1::AccountTransition {
            economic_effective_at_utc_ms,
            condition_id,
            transition: map_account_transition_kind_v1(transition)?,
            balance_delta,
        },
    })
}

fn map_economic_fact_v1(
    value: SourceNeutralEconomicFactV1<StableLogicalIdentityV1, NoeEconomicFactReferenceV1>,
) -> Result<
    SourceNeutralEconomicFactV1<MemoryLogicalIdentityV1, MemoryEconomicFactReferenceV1>,
    serde_json::Error,
> {
    let SourceNeutralEconomicFactV1 {
        fact_reference,
        logical_identity,
        economic_identity,
        source_reference,
        disposition,
    } = value;
    Ok(SourceNeutralEconomicFactV1 {
        fact_reference: memory_fact_reference(fact_reference)?,
        logical_identity: logical(logical_identity)?,
        economic_identity,
        source_reference,
        disposition: map_economic_disposition_v1(disposition)?,
    })
}

fn map_economic_fact_v2(
    value: SourceNeutralEconomicFactV2<StableLogicalIdentityV1, NoeEconomicFactReferenceV1>,
) -> Result<
    SourceNeutralEconomicFactV2<MemoryLogicalIdentityV1, MemoryEconomicFactReferenceV1>,
    serde_json::Error,
> {
    let SourceNeutralEconomicFactV2 {
        fact_reference,
        logical_identity,
        economic_identity,
        source_reference,
        disposition,
    } = value;
    Ok(SourceNeutralEconomicFactV2 {
        fact_reference: memory_fact_reference(fact_reference)?,
        logical_identity: logical(logical_identity)?,
        economic_identity,
        source_reference,
        disposition: map_economic_disposition_v2(disposition)?,
    })
}

fn map_normalization_conflict_scope_v1(
    value: NormalizationConflictScopeV1<StableLogicalIdentityV1>,
) -> Result<NormalizationConflictScopeV1<MemoryLogicalIdentityV1>, serde_json::Error> {
    Ok(match value {
        NormalizationConflictScopeV1::LogicalIdentity { logical_identity } => {
            NormalizationConflictScopeV1::LogicalIdentity {
                logical_identity: logical(logical_identity)?,
            }
        }
        NormalizationConflictScopeV1::EconomicIdentity { economic_identity } => {
            NormalizationConflictScopeV1::EconomicIdentity { economic_identity }
        }
    })
}

pub(super) fn map_normalization_record<R>(
    value: NormalizationRecord<
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResult<
            SourceNeutralSubmissionOutcomeV2,
            SourceNeutralOrderSnapshotV2,
            StableLogicalIdentityV1,
            R,
        >,
        StableLogicalIdentityV1,
        NoeEconomicFactReferenceV1,
    >,
) -> Result<NoeMemoryRecord<R>, serde_json::Error> {
    Ok(match value {
        NormalizationRecord::Evidence {
            logical_identity,
            source_order,
            source_reference,
            evidence,
        } => NormalizationRecord::Evidence {
            logical_identity: logical(logical_identity)?,
            source_order,
            source_reference,
            evidence: map_evidence_kind(evidence)?,
        },
        NormalizationRecord::EconomicFact { source_order, fact } => {
            NormalizationRecord::EconomicFact {
                source_order,
                fact: map_economic_fact_v1(fact)?,
            }
        }
        NormalizationRecord::EconomicFactV2 { source_order, fact } => {
            NormalizationRecord::EconomicFactV2 {
                source_order,
                fact: map_economic_fact_v2(fact)?,
            }
        }
        NormalizationRecord::Duplicate {
            logical_identity,
            source_order,
            source_reference,
            original_source_reference,
        } => NormalizationRecord::Duplicate {
            logical_identity: logical(logical_identity)?,
            source_order,
            source_reference,
            original_source_reference,
        },
        NormalizationRecord::EconomicAlias {
            logical_identity,
            source_order,
            source_reference,
            decision,
            economic_identity,
            primary_logical_identity,
            fact_reference,
        } => NormalizationRecord::EconomicAlias {
            logical_identity: logical(logical_identity)?,
            source_order,
            source_reference,
            decision,
            economic_identity,
            primary_logical_identity: logical(primary_logical_identity)?,
            fact_reference: memory_fact_reference(fact_reference)?,
        },
        NormalizationRecord::Pending {
            logical_identity,
            source_order,
            source_reference,
            evidence,
            scope,
        } => NormalizationRecord::Pending {
            logical_identity: logical(logical_identity)?,
            source_order,
            source_reference,
            evidence: map_evidence_kind(evidence)?,
            scope,
        },
        NormalizationRecord::Conflict {
            logical_identity,
            source_order,
            source_reference,
            scope,
            conflicting_evidence,
        } => NormalizationRecord::Conflict {
            logical_identity: logical(logical_identity)?,
            source_order,
            source_reference,
            scope: map_normalization_conflict_scope_v1(scope)?,
            conflicting_evidence: map_evidence_kind(conflicting_evidence)?,
        },
    })
}
