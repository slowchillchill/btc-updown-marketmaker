use contracts::*;
use data_pipeline_core::reduction::{
    ReductionDispositionV1, ReductionOperationKindV1, build_chainlink_reference_outcome_v1,
};
use data_pipeline_core::typed_raw::{
    BinanceAggTradeRecord, BinanceAggTradeRow, BinanceDepthSnapshotRow, BinanceDepthStitchVerdict,
    BinanceDepthUpdateRow, DepthLevel, MetadataMarketLifecycleState, MetadataMarketRuntimeRole,
    MetadataResolutionEvidenceSource, MetadataSourceEvidenceChannel,
    MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin, MetadataTerminalClaimOwner,
    PolymarketMarketMetadataRow, PolymarketMetadataOrigin, PolymarketMetadataRowKind,
    PolymarketOrderbookEvent, PolymarketOrderbookRow, PolymarketRtdsChainlinkBtcUsdRow,
    RtdsFreshnessStatus, TypedRawRow,
};
use data_pipeline_core::*;
use serde_json::Value;

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
}

fn event_id(character: char) -> CanonicalEventId {
    CanonicalEventId(character.to_string().repeat(64))
}

fn asset() -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId("123".to_owned()),
        outcome: OutcomeIdentityV1 {
            index: 0,
            label: "UP".to_owned(),
        },
    }
}

fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("3195576".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 1_000,
            end_at_utc_ms: 301_000,
        },
    }
}

fn orderbook_evidence() -> SourceEvidenceRefV2 {
    SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketOrderbook,
        source_native_identity: None,
        source_event_at_utc_ms: Some(1_000),
        received_at_utc_ms: 1_100,
        collector_admission: Some(CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "epoch-1".to_owned(),
            collector_admission_sequence: 7,
            admitted_at_utc_ms: 1_150,
        }),
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
    }
}

fn seal_execution_event(event: &mut ExecutionMarketEventV3) {
    event.event_id = event_id('0');
    event.event_id = derive_execution_market_event_v3_id(event).unwrap();
}

fn execution_event() -> ExecutionMarketEventV3 {
    let mut event = ExecutionMarketEventV3 {
        schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        event_id: event_id('0'),
        market: market(),
        time: EventTimeV2 {
            source_event_at_utc_ms: Some(1_000),
            received_at_utc_ms: 1_100,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "canonical-rule-v3".to_owned(),
            source_evidence: vec![orderbook_evidence()],
        },
        quality: EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
        payload: ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
            asset: asset(),
            old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
        }),
    };
    seal_execution_event(&mut event);
    event
}

fn reference_event() -> CanonicalReferenceEventV1 {
    let source = SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: sha('9'),
        }),
        source_event_at_utc_ms: Some(900),
        received_at_utc_ms: 1_100,
        collector_admission: None,
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
    };
    let mut event = CanonicalReferenceEventV1 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        event_id: event_id('0'),
        time: EventTimeV2 {
            source_event_at_utc_ms: Some(900),
            received_at_utc_ms: 1_100,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "canonical-rule-v3".to_owned(),
            source_evidence: vec![source],
        },
        quality: EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
        payload: CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(ChainlinkBtcUsdReferenceV1 {
            symbol: "btc/usd".to_owned(),
            price_usd_per_btc: ExactDecimal("64123.45".to_owned()),
            message_timestamp_utc_ms: 950,
        }),
    };
    event.event_id = derive_reference_event_v1_id(&event).unwrap();
    event
}

fn metadata_row(with_ws_platform_time: bool) -> PolymarketMarketMetadataRow {
    PolymarketMarketMetadataRow {
        metadata_row_kind: if with_ws_platform_time {
            PolymarketMetadataRowKind::ResolutionTerminalWs
        } else {
            PolymarketMetadataRowKind::MetadataSnapshot
        },
        timestamp_received: 1_100,
        collector_admission_sequence: 6,
        collector_admission_time_utc_ms: 1_150,
        metadata_origin: if with_ws_platform_time {
            PolymarketMetadataOrigin::MarketResolvedWs
        } else {
            PolymarketMetadataOrigin::GammaMarketsDiscovery
        },
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: "polymarket_market_metadata_v5".to_owned(),
        slug: "btc-updown".to_owned(),
        condition_id: format!("0x{}", "1".repeat(64)),
        clob_market_id: "3195576".to_owned(),
        asset_ids: r#"["123","456"]"#.to_owned(),
        market_start_utc: 100,
        market_end_utc: 300,
        fixed_identity_verified_at_utc: 500,
        market_lifecycle_state: if with_ws_platform_time {
            MetadataMarketLifecycleState::Retired
        } else {
            MetadataMarketLifecycleState::Candidate
        },
        market_runtime_role: if with_ws_platform_time {
            MetadataMarketRuntimeRole::ResolutionPending
        } else {
            MetadataMarketRuntimeRole::Next
        },
        source_endpoint: Some("https://example.invalid".to_owned()),
        source_endpoint_unavailable_reason: None,
        source_evidence_origin: Some(if with_ws_platform_time {
            MetadataSourceEvidenceOrigin::MarketResolvedWs
        } else {
            MetadataSourceEvidenceOrigin::GammaMarketsDiscovery
        }),
        source_evidence_event_type: Some(if with_ws_platform_time {
            MetadataSourceEvidenceEventType::MarketResolved
        } else {
            MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse
        }),
        source_evidence_channel: with_ws_platform_time
            .then_some(MetadataSourceEvidenceChannel::Market),
        source_evidence_received_at_utc: Some(1_100),
        row_evidence_validation_completed_at_utc: Some(1_120),
        source_evidence_payload_json: None,
        source_evidence_raw_sha256: Some("a".repeat(64)),
        market_lifecycle_transition: None,
        lifecycle_scheduled_at_utc: None,
        lifecycle_applied_at_utc: None,
        market_runtime_role_transition: None,
        runtime_role_scheduled_at_utc: None,
        runtime_role_applied_at_utc: None,
        metadata_fetch_start_utc: (!with_ws_platform_time).then_some(1_000),
        closed: Some(with_ws_platform_time),
        closed_time_raw: None,
        closed_time_utc: None,
        closed_time_check_passed: None,
        closed_time_check_failure_reason: None,
        automatically_resolved: None,
        uma_resolution_status: with_ws_platform_time.then(|| "resolved".to_owned()),
        outcomes_raw: Some(r#"["UP","DOWN"]"#.to_owned()),
        outcome_prices_raw: with_ws_platform_time.then(|| r#"["1","0"]"#.to_owned()),
        active: Some(!with_ws_platform_time),
        active_unavailable_reason: None,
        accepting_orders: Some(!with_ws_platform_time),
        ws_platform_timestamp_raw: with_ws_platform_time.then(|| "1000".to_owned()),
        ws_platform_timestamp_utc: with_ws_platform_time.then_some(1_000),
        winning_asset_id: with_ws_platform_time.then(|| "123".to_owned()),
        winning_outcome: with_ws_platform_time.then(|| "UP".to_owned()),
        resolution_evidence_source: with_ws_platform_time
            .then_some(MetadataResolutionEvidenceSource::MarketResolvedWs),
        terminal_claim_owner: with_ws_platform_time
            .then_some(MetadataTerminalClaimOwner::MarketResolvedWs),
        resolution_poll_attempt_ordinal: None,
        resolution_poll_scheduled_utc: None,
        resolution_poll_result: None,
        resolution_transport_error_class: None,
        resolution_transport_http_status: None,
        resolution_integrity_error_class: None,
        resolution_budget_horizon_utc: None,
        resolution_followup_status: None,
        resolution_poll_attempts_recorded: None,
        resolution_valid_response_count: None,
        resolution_transport_unavailable_count: None,
        resolution_missed_slot_count: None,
        last_valid_metadata_received_utc: None,
        terminal_predicate_passed: with_ws_platform_time.then_some(true),
        terminal_predicate_failure_reason: None,
        response_fields_unavailable_reason: None,
        ws_claimant_source_sequence_epoch_id: with_ws_platform_time.then(|| "epoch-ws".to_owned()),
        ws_claimant_collector_admission_sequence: with_ws_platform_time.then_some(5),
    }
}

#[test]
fn execution_v3_rejects_v2_and_legacy_payload_source_ids() {
    let legacy = include_bytes!("fixtures/execution_market_event_v2.json");
    assert!(matches!(
        decode_execution_market_event_v3(legacy),
        Err(ContractError::UnsupportedSchema { .. })
    ));

    let event = execution_event();
    let mut value = serde_json::to_value(&event).unwrap();
    value["payload"]["event"]["source_event_ids"] =
        Value::Array(vec![Value::String("a".repeat(64))]);
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(matches!(
        decode_execution_market_event_v3(&bytes),
        Err(ContractError::Json(_))
    ));
}

#[test]
fn execution_v3_rejects_non_event_min_order_size() {
    let mut value = serde_json::to_value(execution_event()).unwrap();
    value["payload"]["event"]["min_order_size"] = Value::String("5".to_owned());

    assert!(matches!(
        decode_execution_market_event_v3(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::Json(_))
    ));
}

#[test]
fn execution_v3_uses_lineage_as_the_only_source_truth() {
    let event = execution_event();
    let encoded = encode_execution_market_event_v3(&event).unwrap();
    let value: Value = serde_json::from_slice(&encoded).unwrap();
    assert!(value.pointer("/payload/event/source_event_ids").is_none());
    assert_eq!(decode_execution_market_event_v3(&encoded).unwrap(), event);

    let mut without_locator = execution_event();
    without_locator.lineage.source_evidence[0].collector_admission = None;
    seal_execution_event(&mut without_locator);
    assert!(validate_execution_market_event_v3(&without_locator).is_err());

    let mut fabricated_orderbook_identity = execution_event();
    let source = &mut fabricated_orderbook_identity.lineage.source_evidence[0];
    source.source_native_identity =
        Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256: sha('a') });
    source.collector_admission = None;
    seal_execution_event(&mut fabricated_orderbook_identity);
    assert!(validate_execution_market_event_v3(&fabricated_orderbook_identity).is_err());
}

#[test]
fn execution_v3_rejects_reference_sources() {
    let mut event = execution_event();
    event.lineage = reference_event().lineage;
    event.time = EventTimeV2 {
        source_event_at_utc_ms: Some(900),
        received_at_utc_ms: 1_100,
    };
    seal_execution_event(&mut event);
    assert!(validate_execution_market_event_v3(&event).is_err());
}

#[test]
fn physical_resharding_does_not_change_v3_identity() {
    let event = execution_event();
    let mut reshared = event.clone();
    reshared.lineage.source_evidence[0].input_manifest_sha256 = sha('e');
    reshared.lineage.source_evidence[0].input_file_sha256 = sha('f');
    reshared.event_id = event_id('0');
    reshared.event_id = derive_execution_market_event_v3_id(&reshared).unwrap();
    assert_eq!(event.event_id, reshared.event_id);
}

#[test]
fn reference_identity_excludes_physical_location_and_uses_its_own_domain() {
    let event = reference_event();
    let mut reshared = event.clone();
    reshared.lineage.source_evidence[0].input_manifest_sha256 = sha('e');
    reshared.lineage.source_evidence[0].input_file_sha256 = sha('f');
    reshared.event_id = event_id('0');
    reshared.event_id = derive_reference_event_v1_id(&reshared).unwrap();

    assert_eq!(event.event_id, reshared.event_id);
    assert_ne!(event.event_id, execution_event().event_id);

    let mut changed = event.clone();
    let CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(payload) = &mut changed.payload else {
        unreachable!();
    };
    payload.price_usd_per_btc = ExactDecimal("64123.46".to_owned());
    changed.event_id = event_id('0');
    changed.event_id = derive_reference_event_v1_id(&changed).unwrap();
    assert_ne!(event.event_id, changed.event_id);
}

#[test]
fn unified_record_projects_one_identity_to_the_applicable_indexes() {
    let execution = CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(execution_event()));
    let reference = CanonicalEventRecordV1::ReferenceEvent(Box::new(reference_event()));

    assert!(execution.as_execution_market_event().is_some());
    assert!(reference.as_execution_market_event().is_none());

    let execution_event_time = project_record_event_time_index_entry(&execution)
        .unwrap()
        .unwrap();
    let execution_visibility = project_record_visibility_index_entry(&execution).unwrap();
    assert_eq!(execution_event_time.event_id, execution_visibility.event_id);

    let reference_event_time = project_record_event_time_index_entry(&reference)
        .unwrap()
        .unwrap();
    let reference_visibility = project_record_visibility_index_entry(&reference).unwrap();
    assert_eq!(reference_event_time.event_id, reference_visibility.event_id);

    for record in [execution, reference] {
        let encoded = encode_canonical_event_record_v1(&record).unwrap();
        assert_eq!(decode_canonical_event_record_v1(&encoded).unwrap(), record);
        let mut with_unknown_field: Value = serde_json::from_slice(&encoded).unwrap();
        with_unknown_field["future_record_field"] = Value::Bool(true);
        assert!(
            decode_canonical_event_record_v1(&serde_json::to_vec(&with_unknown_field).unwrap())
                .is_err()
        );
    }

    let unknown = serde_json::json!({
        "record_type": "source_event",
        "record": {}
    });
    assert!(decode_canonical_event_record_v1(&serde_json::to_vec(&unknown).unwrap()).is_err());
}

#[test]
fn binance_snapshot_projects_only_to_the_visibility_index() {
    let evidence = SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::BinanceDepthSnapshot,
        source_native_identity: Some(SourceNativeIdentityV1::BinanceDepthSnapshot {
            last_update_id: 11,
        }),
        source_event_at_utc_ms: None,
        received_at_utc_ms: 1_100,
        collector_admission: Some(CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "epoch-1".to_owned(),
            collector_admission_sequence: 8,
            admitted_at_utc_ms: 1_150,
        }),
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
    };
    let mut reference = CanonicalReferenceEventV1 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        event_id: event_id('0'),
        time: EventTimeV2 {
            source_event_at_utc_ms: None,
            received_at_utc_ms: 1_100,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "canonical-rule-v3".to_owned(),
            source_evidence: vec![evidence],
        },
        quality: EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
        payload: CanonicalReferenceEventPayloadV1::BinanceDepthState(
            BinanceDepthStateReferenceV1 {
                symbol: "BTCUSDT".to_owned(),
                last_update_id: 11,
                bids: vec![ReferencePriceLevelV1 {
                    price_usdt_per_btc: ExactDecimal("64120".to_owned()),
                    quantity_btc: ExactDecimal("0.5".to_owned()),
                }],
                asks: vec![ReferencePriceLevelV1 {
                    price_usdt_per_btc: ExactDecimal("64130".to_owned()),
                    quantity_btc: ExactDecimal("0.4".to_owned()),
                }],
            },
        ),
    };
    reference.event_id = derive_reference_event_v1_id(&reference).unwrap();

    let mut snapshot_with_fabricated_source_time = reference.clone();
    snapshot_with_fabricated_source_time.lineage.source_evidence[0].source_event_at_utc_ms =
        Some(1_050);
    snapshot_with_fabricated_source_time
        .time
        .source_event_at_utc_ms = Some(1_050);
    snapshot_with_fabricated_source_time.event_id = event_id('0');
    snapshot_with_fabricated_source_time.event_id =
        derive_reference_event_v1_id(&snapshot_with_fabricated_source_time).unwrap();
    assert!(validate_reference_event_v1(&snapshot_with_fabricated_source_time).is_err());

    let mut update_reference = reference.clone();
    let update_source = &mut update_reference.lineage.source_evidence[0];
    update_source.source_kind = CanonicalSourceKind::BinanceDepthUpdate;
    update_source.source_native_identity = Some(SourceNativeIdentityV1::BinanceDepthUpdate {
        first_update_id: 12,
        final_update_id: 13,
    });
    update_source.source_event_at_utc_ms = Some(1_050);
    update_reference.time.source_event_at_utc_ms = Some(1_050);
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(payload) =
        &mut update_reference.payload
    else {
        unreachable!();
    };
    payload.last_update_id = 13;
    update_reference.event_id = event_id('0');
    update_reference.event_id = derive_reference_event_v1_id(&update_reference).unwrap();
    validate_reference_event_v1(&update_reference).unwrap();

    let record = CanonicalEventRecordV1::ReferenceEvent(Box::new(reference.clone()));

    assert_eq!(
        project_record_event_time_index_entry(&record).unwrap(),
        None
    );
    assert_eq!(
        project_record_visibility_index_entry(&record)
            .unwrap()
            .event_id,
        reference.event_id
    );
}

#[test]
fn reference_event_retains_auxiliary_source_timestamp() {
    let event = reference_event();
    validate_reference_event_v1(&event).unwrap();
    let CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(payload) = event.payload else {
        panic!("expected Chainlink reference payload");
    };
    assert_eq!(event.time.source_event_at_utc_ms, Some(900));
    assert_eq!(payload.message_timestamp_utc_ms, 950);
}

#[test]
fn reference_events_reject_impossible_direct_evidence() {
    let mut fabricated_chainlink_admission = reference_event();
    let source = &mut fabricated_chainlink_admission.lineage.source_evidence[0];
    source.source_native_identity = None;
    source.collector_admission = Some(CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: "invented-rtds-epoch".to_owned(),
        collector_admission_sequence: 1,
        admitted_at_utc_ms: 1_150,
    });
    fabricated_chainlink_admission.event_id = event_id('0');
    fabricated_chainlink_admission.event_id =
        derive_reference_event_v1_id(&fabricated_chainlink_admission).unwrap();
    assert!(validate_reference_event_v1(&fabricated_chainlink_admission).is_err());

    let mut chainlink_without_payload_time = reference_event();
    chainlink_without_payload_time.lineage.source_evidence[0].source_event_at_utc_ms = None;
    chainlink_without_payload_time.time.source_event_at_utc_ms = None;
    chainlink_without_payload_time.event_id = event_id('0');
    chainlink_without_payload_time.event_id =
        derive_reference_event_v1_id(&chainlink_without_payload_time).unwrap();
    assert!(validate_reference_event_v1(&chainlink_without_payload_time).is_err());

    let mut multiple_chainlink_inputs = reference_event();
    let mut earlier = multiple_chainlink_inputs.lineage.source_evidence[0].clone();
    earlier.source_native_identity =
        Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256: sha('8') });
    multiple_chainlink_inputs
        .lineage
        .source_evidence
        .insert(0, earlier);
    multiple_chainlink_inputs.event_id = event_id('0');
    multiple_chainlink_inputs.event_id =
        derive_reference_event_v1_id(&multiple_chainlink_inputs).unwrap();
    assert!(validate_reference_event_v1(&multiple_chainlink_inputs).is_err());

    let mut aggregate_trade = CanonicalReferenceEventV1 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        event_id: event_id('0'),
        time: EventTimeV2 {
            source_event_at_utc_ms: Some(1_070),
            received_at_utc_ms: 1_300,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "canonical-rule-v3".to_owned(),
            source_evidence: vec![SourceEvidenceRefV2 {
                source_kind: CanonicalSourceKind::BinanceAggtrade,
                source_native_identity: None,
                source_event_at_utc_ms: Some(1_070),
                received_at_utc_ms: 1_300,
                collector_admission: Some(CollectorAdmissionCoordinateV1 {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 10,
                    admitted_at_utc_ms: 1_350,
                }),
                input_manifest_sha256: sha('c'),
                input_file_sha256: sha('d'),
            }],
        },
        quality: EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
        payload: CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(
            BinanceAggregateTradeReferenceV1 {
                symbol: "BTCUSDT".to_owned(),
                aggregate_trade_id: 21,
                first_trade_id: 31,
                last_trade_id: 32,
                price_usdt_per_btc: ExactDecimal("64123.4".to_owned()),
                quantity_btc: ExactDecimal("0.01".to_owned()),
                buyer_is_maker: false,
                event_time_utc_ms: 1_080,
            },
        ),
    };
    aggregate_trade.event_id = derive_reference_event_v1_id(&aggregate_trade).unwrap();
    assert!(validate_reference_event_v1(&aggregate_trade).is_err());

    aggregate_trade.lineage.source_evidence[0].source_native_identity =
        Some(SourceNativeIdentityV1::BinanceAggregateTrade {
            aggregate_trade_id: 21,
        });
    aggregate_trade.lineage.source_evidence[0].source_event_at_utc_ms = None;
    aggregate_trade.time.source_event_at_utc_ms = None;
    aggregate_trade.event_id = event_id('0');
    aggregate_trade.event_id = derive_reference_event_v1_id(&aggregate_trade).unwrap();
    assert!(validate_reference_event_v1(&aggregate_trade).is_err());
}

#[test]
fn typed_raw_source_evidence_uses_the_frozen_time_mappings() {
    let metadata_without_ws_time =
        TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_row(false)));
    let metadata_with_ws_time = TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_row(true)));
    let orderbook = TypedRawRow::PolymarketOrderbook(PolymarketOrderbookRow {
        timestamp_received: 1_100,
        timestamp: Some(1_000),
        market: format!("0x{}", "1".repeat(64)),
        slug: Some("btc-updown".to_owned()),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://example.invalid".to_owned(),
        schema_version: "polymarket_orderbook_v3".to_owned(),
        source_sequence_epoch_id: "epoch-1".to_owned(),
        collector_admission_sequence: 7,
        collector_admission_time_utc_ms: 1_150,
        event: PolymarketOrderbookEvent::BestBidAsk {
            asset_id: "123".to_owned(),
            best_bid: Some("0.4".to_owned()),
            best_ask: Some("0.5".to_owned()),
            spread: Some("0.1".to_owned()),
        },
    });
    let rtds = TypedRawRow::PolymarketRtdsChainlinkBtcUsd(PolymarketRtdsChainlinkBtcUsdRow {
        timestamp_received: 1_100,
        message_timestamp: 950,
        payload_timestamp: 900,
        age_ms: Some(200),
        symbol: "btc/usd".to_owned(),
        value: "64123.45".to_owned(),
        topic: "crypto_prices_chainlink".to_owned(),
        message_type: "update".to_owned(),
        freshness_status: RtdsFreshnessStatus::Fresh,
        subscription_filter: "btc/usd".to_owned(),
        source_native_payload_json: r#"{"symbol":"btc/usd","timestamp":900,"value":"64123.45"}"#
            .to_owned(),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://example.invalid".to_owned(),
        schema_version: "polymarket_rtds_chainlink_btc_usd_v4".to_owned(),
    });
    let snapshot = TypedRawRow::BinanceDepthSnapshot(BinanceDepthSnapshotRow {
        timestamp_received: 1_100,
        snapshot_fetch_start_utc: 1_050,
        last_update_id: 11,
        bids: vec![DepthLevel {
            price: "64120.0".to_owned(),
            quantity: "0.5".to_owned(),
        }],
        asks: vec![DepthLevel {
            price: "64130.0".to_owned(),
            quantity: "0.4".to_owned(),
        }],
        symbol: "BTCUSDT".to_owned(),
        source_endpoint: "https://example.invalid".to_owned(),
        snapshot_status: "complete".to_owned(),
        source_sequence_epoch_id: "epoch-1".to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: 1,
        collector_admission_sequence: 8,
        collector_admission_time_utc_ms: 1_150,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: "binance_depth_snapshot_v3".to_owned(),
    });
    let update = TypedRawRow::BinanceDepthUpdate(BinanceDepthUpdateRow {
        timestamp_received: 1_200,
        event_type: "depthUpdate".to_owned(),
        event_time: 1_050,
        symbol: "BTCUSDT".to_owned(),
        first_update_id: 12,
        final_update_id: 13,
        bids: vec![DepthLevel {
            price: "64120.0".to_owned(),
            quantity: "0.6".to_owned(),
        }],
        asks: Vec::new(),
        stream: "btcusdt@depth".to_owned(),
        snapshot_last_update_id: Some(11),
        local_u_before: Some(11),
        stitch_verdict: BinanceDepthStitchVerdict::Applied,
        stitch_evidence: Some("contiguous".to_owned()),
        stitch_gap_reason: None,
        source_sequence_epoch_id: "epoch-1".to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: 2,
        collector_admission_sequence: 9,
        collector_admission_time_utc_ms: 1_250,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://example.invalid".to_owned(),
        schema_version: "binance_depth_update_v4".to_owned(),
    });
    let trade = TypedRawRow::BinanceAggTrade(BinanceAggTradeRow {
        timestamp_received: 1_300,
        rest_backfill_timestamp_received: None,
        ingest_source: "websocket".to_owned(),
        rest_was_best_price_match: None,
        source_native_payload_json: r#"{"a":21}"#.to_owned(),
        source_sequence_epoch_id: "epoch-1".to_owned(),
        connection_generation: 1,
        websocket_ingress_sequence: 3,
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        collector_admission_sequence: 10,
        collector_admission_time_utc_ms: 1_350,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://example.invalid".to_owned(),
        schema_version: "binance_aggtrade_v7".to_owned(),
        record: BinanceAggTradeRecord::Trade {
            event_type: "aggTrade".to_owned(),
            event_time: 1_080,
            trade_time: 1_070,
            symbol: "BTCUSDT".to_owned(),
            aggregate_trade_id: 21,
            price: "64123.4".to_owned(),
            quantity: "0.01".to_owned(),
            first_trade_id: 31,
            last_trade_id: 32,
            buyer_is_maker: false,
            stream: "btcusdt@aggTrade".to_owned(),
            ws_ignore: false,
        },
    });

    let expected = [
        (&metadata_without_ws_time, None, 1_100),
        (&metadata_with_ws_time, Some(1_000), 1_100),
        (&orderbook, Some(1_000), 1_100),
        (&rtds, Some(900), 1_100),
        (&snapshot, None, 1_100),
        (&update, Some(1_050), 1_200),
        (&trade, Some(1_070), 1_300),
    ];
    for (row, source_time, received_time) in expected {
        let evidence = source_evidence_from_typed_raw(row, &sha('c'), &sha('d')).unwrap();
        assert_eq!(
            data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(
                &data_pipeline_core::public_input::TypedRawRowV2::Existing(row.clone()),
            )
            .unwrap(),
            data_pipeline_core::public_reference::source_evidence_v3_from_existing(&evidence)
                .unwrap(),
        );
        assert_eq!(evidence.source_event_at_utc_ms, source_time);
        assert_eq!(evidence.received_at_utc_ms, received_time);
        assert!(
            evidence.source_native_identity.is_some() || evidence.collector_admission.is_some()
        );
        if matches!(
            row,
            TypedRawRow::PolymarketMarketMetadata(_)
                | TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_)
        ) {
            assert_eq!(evidence.collector_admission, None);
        }
    }

    let metadata_evidence =
        source_evidence_from_typed_raw(&metadata_with_ws_time, &sha('c'), &sha('d')).unwrap();
    let mut metadata_event = execution_event();
    metadata_event.time = EventTimeV2 {
        source_event_at_utc_ms: metadata_evidence.source_event_at_utc_ms,
        received_at_utc_ms: metadata_evidence.received_at_utc_ms,
    };
    metadata_event.lineage.source_evidence = vec![metadata_evidence];
    seal_execution_event(&mut metadata_event);
    validate_execution_market_event_v3(&metadata_event).unwrap();

    metadata_event.lineage.source_evidence[0].collector_admission =
        Some(CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "invented-metadata-epoch".to_owned(),
            collector_admission_sequence: 6,
            admitted_at_utc_ms: 1_150,
        });
    seal_execution_event(&mut metadata_event);
    assert!(validate_execution_market_event_v3(&metadata_event).is_err());

    let rtds_event = build_chainlink_reference_event_v1(
        match &rtds {
            TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => row,
            _ => unreachable!(),
        },
        &sha('c'),
        &sha('d'),
        "canonical-rule-v3",
    )
    .unwrap();
    assert_eq!(rtds_event.time.source_event_at_utc_ms, Some(900));
    let CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(payload) = &rtds_event.payload else {
        unreachable!();
    };
    assert_eq!(payload.message_timestamp_utc_ms, 950);
    let rtds_outcome = build_chainlink_reference_outcome_v1(
        match &rtds {
            TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => row,
            _ => unreachable!(),
        },
        &sha('c'),
        &sha('d'),
        "canonical-rule-v3",
    )
    .unwrap();
    assert_eq!(rtds_outcome.event.as_ref(), Some(&rtds_event));
    assert_eq!(
        rtds_outcome.report.operation,
        ReductionOperationKindV1::Merge
    );
    assert!(matches!(
        rtds_outcome.report.disposition,
        ReductionDispositionV1::Produced { result_event_id }
            if result_event_id == rtds_event.event_id
    ));

    let mut stale_rtds = match &rtds {
        TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => row.clone(),
        _ => unreachable!(),
    };
    stale_rtds.freshness_status = RtdsFreshnessStatus::Stale;
    let stale_event =
        build_chainlink_reference_event_v1(&stale_rtds, &sha('c'), &sha('d'), "canonical-rule-v3")
            .unwrap();
    assert_eq!(stale_event.quality.availability, MarketDataQuality::Stale);

    let trade_event = build_binance_aggregate_trade_reference_event_v1(
        match &trade {
            TypedRawRow::BinanceAggTrade(row) => row,
            _ => unreachable!(),
        },
        &sha('c'),
        &sha('d'),
        "canonical-rule-v3",
    )
    .unwrap();
    assert_eq!(trade_event.time.source_event_at_utc_ms, Some(1_070));
    let CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(payload) = &trade_event.payload
    else {
        unreachable!();
    };
    assert_eq!(payload.event_time_utc_ms, 1_080);

    let mut trade_without_admission = trade_event;
    trade_without_admission.lineage.source_evidence[0].collector_admission = None;
    trade_without_admission.event_id = event_id('0');
    trade_without_admission.event_id =
        derive_reference_event_v1_id(&trade_without_admission).unwrap();
    assert!(validate_reference_event_v1(&trade_without_admission).is_err());
}
