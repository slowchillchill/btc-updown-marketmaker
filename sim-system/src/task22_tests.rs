use contracts::{
    CanonicalEventId, CanonicalSourceKind, ClobMarketId, CollectorAdmissionCoordinateV1,
    ConditionId, EventLineageV2, EventQualityV2, EventTimeV2, ExecutionMarketEventPayloadV3,
    ExecutionMarketEventV3, MarketIdentityV2, MarketWindowV2, ResyncQuality, Sha256Hex,
    SourceEvidenceRefV2,
};
use data_pipeline_core::derive_execution_market_event_v3_id;

fn sha(byte: char) -> Sha256Hex {
    Sha256Hex(byte.to_string().repeat(64))
}

fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("123".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 100_000,
            end_at_utc_ms: 500_000,
        },
    }
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
            source_sequence_epoch_id: "task22-epoch".to_owned(),
            collector_admission_sequence: sequence,
            admitted_at_utc_ms: received_at,
        }),
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
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
            canonical_rule_version: "task22-rule-v1".to_owned(),
            source_evidence: vec![evidence],
        },
        quality: EventQualityV2 {
            availability: quality.availability,
            resync: match quality.resync {
                ResyncQuality::NotRequired => ResyncQuality::NotRequired,
                ResyncQuality::Required => ResyncQuality::Required,
                ResyncQuality::ResyncBoundary => ResyncQuality::ResyncBoundary,
            },
        },
        payload,
    };
    event.event_id = derive_execution_market_event_v3_id(&event).expect("event identity");
    event
}
