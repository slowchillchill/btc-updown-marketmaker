use contracts::{
    AssetIdentityV1, BookSide, CanonicalEventId, CanonicalSourceKind, ClobMarketId,
    CollectorAdmissionCoordinateV1, ConditionId, EventLineageV2, EventQualityV2, EventTimeV2,
    ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialL2SnapshotV2,
    LevelExecutionBasis, LevelExecutionTransitionV3, MarketDataQuality, MarketIdentityV2,
    MarketWindowV2, OutcomeIdentityV1, PriceLevelV1, QueueResultQuality, ResyncQuality, Sha256Hex,
    SourceAttributionQuality, SourceEvidenceRefV2, TokenId,
};
use data_pipeline_core::derive_execution_market_event_v3_id;

pub(crate) fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("123".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 1_000,
            end_at_utc_ms: 2_000,
        },
    }
}

pub(crate) fn asset(token_id: &str, outcome_index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token_id.to_owned()),
        outcome: OutcomeIdentityV1 {
            index: outcome_index,
            label: label.to_owned(),
        },
    }
}

fn sha(byte: char) -> Sha256Hex {
    Sha256Hex(byte.to_string().repeat(64))
}

pub(crate) fn orderbook_evidence(
    source_at: i64,
    received_at: i64,
    sequence: u64,
) -> SourceEvidenceRefV2 {
    SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketOrderbook,
        source_native_identity: None,
        source_event_at_utc_ms: Some(source_at),
        received_at_utc_ms: received_at,
        collector_admission: Some(CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "epoch-1".to_owned(),
            collector_admission_sequence: sequence,
            admitted_at_utc_ms: received_at,
        }),
        input_manifest_sha256: sha('a'),
        input_file_sha256: sha('b'),
    }
}

pub(crate) fn fresh_quality() -> EventQualityV2 {
    EventQualityV2 {
        availability: MarketDataQuality::Fresh,
        resync: ResyncQuality::NotRequired,
    }
}

pub(crate) fn sealed_event(
    payload: ExecutionMarketEventPayloadV3,
    evidence: SourceEvidenceRefV2,
    quality: EventQualityV2,
) -> ExecutionMarketEventV3 {
    let mut event = ExecutionMarketEventV3 {
        schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        event_id: CanonicalEventId("0".repeat(64)),
        market: market(),
        time: EventTimeV2 {
            source_event_at_utc_ms: evidence.source_event_at_utc_ms,
            received_at_utc_ms: evidence.received_at_utc_ms,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "task21-rule-v1".to_owned(),
            source_evidence: vec![evidence],
        },
        quality,
        payload,
    };
    event.event_id = derive_execution_market_event_v3_id(&event).expect("event identity");
    event
}

pub(crate) fn initial_snapshot_event(
    token_id: &str,
    outcome_index: u32,
    label: &str,
    source_at: i64,
    received_at: i64,
    sequence: u64,
    quantity: &str,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
            asset: asset(token_id, outcome_index, label),
            initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            source_sequence_epoch_id: "epoch-1".to_owned(),
            source_cursor: sequence,
            book_digest_sha256: sha(if token_id == "1" { 'e' } else { 'f' }),
            bids: vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                quantity_shares: ExactDecimal(quantity.to_owned()),
            }],
            asks: vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal("0.6".to_owned()),
                quantity_shares: ExactDecimal("5".to_owned()),
            }],
        }),
        orderbook_evidence(source_at, received_at, sequence),
        fresh_quality(),
    )
}

pub(crate) fn level_transition_event(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    before: &str,
    after: &str,
    added: &str,
    reduced: &str,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: sha('9'),
            market: market(),
            asset: asset("1", 0, "Up"),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(added.to_owned()),
            net_reduced_quantity_shares: ExactDecimal(reduced.to_owned()),
            queue_consumption_quantity_shares: ExactDecimal(reduced.to_owned()),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at,
            visible_at_utc_ms: received_at,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}
