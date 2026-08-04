use std::cmp::Ordering;
use std::fmt::Write as _;

use contracts::*;
use data_pipeline_core::*;
use serde_json::{Value, json};

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
}

fn event_id(character: char) -> CanonicalEventId {
    CanonicalEventId(character.to_string().repeat(64))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

fn asset(token: &str, index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token.to_owned()),
        outcome: OutcomeIdentityV1 {
            index,
            label: label.to_owned(),
        },
    }
}

fn market() -> MarketIdentityV1 {
    MarketIdentityV1 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("3195576".to_owned()),
        window: MarketWindowV1 {
            window_id: sha('1'),
            start_at_utc_ms: 1_000,
            end_at_utc_ms: 301_000,
        },
    }
}

fn admission_time() -> EventTimeV1 {
    EventTimeV1 {
        source_event_at_utc_ms: Some(1_000),
        received_at_utc_ms: 1_100,
        collector_admission: Some(CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "epoch-1".to_owned(),
            collector_admission_sequence: 7,
            admitted_at_utc_ms: 1_150,
        }),
        available_at_utc_ms: 1_200,
        ordering_evidence_quality: OrderingEvidenceQuality::CollectorAdmissionCoordinate,
    }
}

fn admission_key(source_kind: CanonicalSourceKind) -> TotalOrderKeyV1 {
    TotalOrderKeyV1 {
        version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
        available_at_utc_ms: 1_200,
        source_kind,
        ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
            source_sequence_epoch_id: "epoch-1".to_owned(),
            collector_admission_sequence: 7,
        },
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: sha('7'),
        }),
        semantic_tiebreak_sha256: sha('0'),
    }
}

fn source_evidence(source_kind: CanonicalSourceKind) -> SourceEvidenceRefV1 {
    let (source_native_identity, ordering_coordinate) =
        if source_kind == CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd {
            (
                Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256: sha('9') }),
                OrderingCoordinateV1::ReceivedOnly {
                    received_at_utc_ms: 1_100,
                    stable_evidence_key_sha256: sha('9'),
                },
            )
        } else {
            (
                Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256: sha('7') }),
                OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 7,
                },
            )
        };
    SourceEvidenceRefV1 {
        source_kind,
        source_native_identity,
        ordering_coordinate,
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
    }
}

fn base_event() -> ExecutionMarketEventV1 {
    let source_kind = CanonicalSourceKind::PolymarketOrderbook;
    let mut event = ExecutionMarketEventV1 {
        schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V1.to_owned(),
        event_id: event_id('0'),
        market: market(),
        time: admission_time(),
        total_order_key: admission_key(source_kind),
        lineage: EventLineageV1 {
            canonical_rule_version: "canonical-rule-v1".to_owned(),
            source_evidence: vec![source_evidence(source_kind)],
            ordering_source_evidence_index: 0,
        },
        quality: EventQualityV1 {
            availability: MarketDataQuality::Fresh,
            ordering: OrderingEvidenceQuality::CollectorAdmissionCoordinate,
            resync: ResyncQuality::NotRequired,
        },
        payload: ExecutionMarketEventPayloadV1::TickSizeChange(TickSizeChangeV1 {
            asset: asset("123", 0, "UP"),
            old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
            source_event_ids: vec![event_id('a')],
        }),
    };
    seal_event(&mut event);
    event
}

fn seal_event(event: &mut ExecutionMarketEventV1) {
    event.event_id = event_id('0');
    event.total_order_key.semantic_tiebreak_sha256 = sha('0');
    event.event_id = derive_canonical_event_id(event).unwrap();
    event.total_order_key.semantic_tiebreak_sha256 = derive_semantic_tiebreak(event).unwrap();
}

fn event_with_payload(payload: ExecutionMarketEventPayloadV1) -> ExecutionMarketEventV1 {
    let mut event = base_event();
    event.payload = payload;
    seal_event(&mut event);
    event
}

fn rtds_event() -> ExecutionMarketEventV1 {
    let mut event = base_event();
    event.time = EventTimeV1 {
        source_event_at_utc_ms: Some(900),
        received_at_utc_ms: 1_100,
        collector_admission: None,
        available_at_utc_ms: 1_200,
        ordering_evidence_quality: OrderingEvidenceQuality::ReceivedTimeStableEvidence,
    };
    event.total_order_key = TotalOrderKeyV1 {
        version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
        available_at_utc_ms: 1_200,
        source_kind: CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
        ordering_coordinate: OrderingCoordinateV1::ReceivedOnly {
            received_at_utc_ms: 1_100,
            stable_evidence_key_sha256: sha('9'),
        },
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: sha('9'),
        }),
        semantic_tiebreak_sha256: sha('0'),
    };
    event.lineage.source_evidence = vec![source_evidence(
        CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
    )];
    event.quality.ordering = OrderingEvidenceQuality::ReceivedTimeStableEvidence;
    seal_event(&mut event);
    event
}

fn all_event_payloads() -> Vec<ExecutionMarketEventPayloadV1> {
    let market = market();
    let up = asset("123", 0, "UP");
    let down = asset("456", 1, "DOWN");
    let key = admission_key(CanonicalSourceKind::PolymarketMarketMetadata);
    vec![
        ExecutionMarketEventPayloadV1::MarketLifecycleTransition(MarketLifecycleTransitionV1 {
            previous_state: None,
            current_state: MappedMarketLifecycleStateV1::UnknownUnmodeled {
                raw_value: "future_state".to_owned(),
            },
            assets: vec![up.clone(), down.clone()],
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::InitialL2Snapshot(InitialL2SnapshotV1 {
            asset: up.clone(),
            initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            source_sequence_epoch_id: "epoch-1".to_owned(),
            source_cursor: 7,
            book_digest_sha256: sha('b'),
            bids: vec![
                PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                    quantity_shares: ExactDecimal("10".to_owned()),
                },
                PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.3".to_owned()),
                    quantity_shares: ExactDecimal("20".to_owned()),
                },
            ],
            asks: vec![
                PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.5".to_owned()),
                    quantity_shares: ExactDecimal("11".to_owned()),
                },
                PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.6".to_owned()),
                    quantity_shares: ExactDecimal("21".to_owned()),
                },
            ],
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::BestBidAsk(BestBidAskV1 {
            asset: up.clone(),
            best_bid_pusd_per_share: Some(ExactDecimal("0.4".to_owned())),
            best_ask_pusd_per_share: Some(ExactDecimal("0.5".to_owned())),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::TickSizeChange(TickSizeChangeV1 {
            asset: up.clone(),
            old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::PublicTrade(PublicTradeV1 {
            asset: up.clone(),
            trade_id: "trade-1".to_owned(),
            trade_id_source: PublicTradeIdSourceV1::DerivedSourceEvidence,
            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
            quantity_shares: ExactDecimal("2.5".to_owned()),
            aggressor_side: TradeSide::Buy,
            fee_rate_bps: Some(ExactDecimal("15".to_owned())),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::LevelExecutionTransition(LevelExecutionTransitionV1 {
            causal_group_id: sha('8'),
            market: market.clone(),
            asset: up.clone(),
            side: BookSide::Ask,
            price_pusd_per_share: ExactDecimal("0.5".to_owned()),
            quantity_before_shares: ExactDecimal("10".to_owned()),
            quantity_after_shares: ExactDecimal("7".to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("3".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal("0".to_owned()),
            net_reduced_quantity_shares: ExactDecimal("3".to_owned()),
            queue_consumption_quantity_shares: ExactDecimal("3".to_owned()),
            source_event_ids: vec![event_id('a')],
            basis: LevelExecutionBasis::TradeEvidence,
            group_available_at_utc_ms: 1_200,
            source_attribution_quality: SourceAttributionQuality::Exact,
            queue_result_quality: QueueResultQuality::Exact,
        }),
        ExecutionMarketEventPayloadV1::LevelExecutionTransition(LevelExecutionTransitionV1 {
            causal_group_id: sha('8'),
            market: market.clone(),
            asset: up.clone(),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
            quantity_before_shares: ExactDecimal("7".to_owned()),
            quantity_after_shares: ExactDecimal("5".to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal("0".to_owned()),
            net_reduced_quantity_shares: ExactDecimal("2".to_owned()),
            queue_consumption_quantity_shares: ExactDecimal("2".to_owned()),
            source_event_ids: vec![event_id('a')],
            basis: LevelExecutionBasis::LevelDeltaInference,
            group_available_at_utc_ms: 1_200,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        ExecutionMarketEventPayloadV1::LevelExecutionTransition(LevelExecutionTransitionV1 {
            causal_group_id: sha('8'),
            market: market.clone(),
            asset: up.clone(),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
            quantity_before_shares: ExactDecimal("5".to_owned()),
            quantity_after_shares: ExactDecimal("9".to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal("4".to_owned()),
            net_reduced_quantity_shares: ExactDecimal("0".to_owned()),
            queue_consumption_quantity_shares: ExactDecimal("0".to_owned()),
            source_event_ids: vec![event_id('a')],
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            group_available_at_utc_ms: 1_200,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        ExecutionMarketEventPayloadV1::QualityBoundary(QualityBoundaryV1 {
            scope: QualityScopeV1::Asset(up.clone()),
            previous_quality: MarketDataQuality::Fresh,
            current_quality: MarketDataQuality::ConfirmedGap,
            reason_code: "confirmed_sequence_gap".to_owned(),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::MarketTradingState(MarketTradingStateV1 {
            active: Some(true),
            closed: Some(false),
            accepting_orders: Some(true),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV1::MarketResolutionTerminal(
            MarketResolutionTerminalV1::Resolved {
                winning_asset_id: up.token_id.0.clone(),
                winning_outcome: up.outcome.label.clone(),
                payout_vector: vec![
                    PayoutEntryV1 {
                        asset: up,
                        payout_pusd_per_share: ExactDecimal("1".to_owned()),
                    },
                    PayoutEntryV1 {
                        asset: down,
                        payout_pusd_per_share: ExactDecimal("0".to_owned()),
                    },
                ],
                mapping_rule_version: "binary-resolution-v1".to_owned(),
                source_event_ids: vec![event_id('a')],
                terminal_watermark: TerminalWatermarkV1 {
                    last_total_order_key: key,
                    canonical_event_root_sha256: sha('6'),
                    lineage_identity_sha256: sha('5'),
                },
            },
        ),
    ]
}

fn base_observation(event: &ExecutionMarketEventV1) -> MarketObservationCausalShellV1 {
    let missing_components = REQUIRED_OBSERVATION_COMPONENTS
        .iter()
        .copied()
        .filter(|component| *component != ObservationComponentId::CurrentTick)
        .collect();
    let mut components = REQUIRED_OBSERVATION_COMPONENTS
        .iter()
        .copied()
        .map(|component| ObservationComponentStateV1 {
            component,
            quality: MarketDataQuality::Missing,
            as_of_available_at_utc_ms: None,
            information_age_ms: None,
            last_visible_event_id: None,
            state_digest_sha256: None,
        })
        .collect::<Vec<_>>();
    components[2] = ObservationComponentStateV1 {
        component: ObservationComponentId::CurrentTick,
        quality: MarketDataQuality::Fresh,
        as_of_available_at_utc_ms: Some(1_200),
        information_age_ms: Some(50),
        last_visible_event_id: Some(event.event_id.clone()),
        state_digest_sha256: Some(sha('a')),
    };
    let mut observation = MarketObservationCausalShellV1 {
        schema_version: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V1.to_owned(),
        observation_id: sha('0'),
        market: event.market.clone(),
        lineage: ObservationLineageV1 {
            execution_event_contract_version: EXECUTION_MARKET_EVENT_SCHEMA_V1.to_owned(),
            input_set_identity_sha256: sha('f'),
            contributing_event_ids: vec![event.event_id.clone()],
            last_finalized_order_key: event.total_order_key.clone(),
        },
        causal_cutoff: ObservationCausalCutoffV1 {
            available_at_utc_ms: event.time.available_at_utc_ms + 50,
            through_total_order_key: event.total_order_key.clone(),
        },
        market_state_digest_sha256: sha('e'),
        component_states: components,
        missing_mask: missing_components,
        canonical_hash_sha256: sha('0'),
    };
    seal_observation(&mut observation);
    observation
}

fn seal_observation(observation: &mut MarketObservationCausalShellV1) {
    observation.observation_id = sha('0');
    observation.canonical_hash_sha256 = sha('0');
    observation.observation_id = derive_observation_id(observation).unwrap();
    observation.canonical_hash_sha256 = derive_observation_hash(observation).unwrap();
}

fn matrix_with(statuses: impl Fn(usize) -> CapabilityStatus) -> GateACapabilityMatrixV1 {
    GateACapabilityMatrixV1 {
        schema_version: GATE_A_CAPABILITY_MATRIX_SCHEMA_V1.to_owned(),
        entries: REQUIRED_GATE_A_CAPABILITIES
            .iter()
            .copied()
            .enumerate()
            .map(|(index, capability)| {
                let status = statuses(index);
                GateACapabilityEntryV1 {
                    capability,
                    status,
                    contract_field_paths: vec!["phase0.contract".to_owned()],
                    evidence_ids: if status == CapabilityStatus::Pass {
                        vec![format!("phase0-evidence-{index}")]
                    } else {
                        Vec::new()
                    },
                    blocking_reason: (status == CapabilityStatus::Fail)
                        .then(|| format!("blocked-{index}")),
                }
            })
            .collect(),
    }
}

fn receipt_for(matrix: &GateACapabilityMatrixV1) -> GateAReceiptV1 {
    let mut receipt = GateAReceiptV1 {
        schema_version: GATE_A_RECEIPT_SCHEMA_V1.to_owned(),
        receipt_id: sha('0'),
        requirement_id: "DATA-PIPELINE-TRAINING-20260803".to_owned(),
        pipeline_core_identity_sha256: sha('1'),
        execution_event_contract_version: EXECUTION_MARKET_EVENT_SCHEMA_V1.to_owned(),
        observation_contract_version: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V1.to_owned(),
        dataset_lineage_root: "/home/chillchill/new-datasets".to_owned(),
        dataset_date_floor: "2026-07-30".to_owned(),
        covered_utc_dates: vec!["2026-07-30".to_owned(), "2026-08-02".to_owned()],
        input_set_identity_sha256: sha('2'),
        supported_schema_set_identity_sha256: sha('3'),
        input_manifest_set_sha256: sha('4'),
        canonical_event_tape_manifest_sha256: sha('5'),
        execution_event_tape_manifest_sha256: sha('6'),
        observation_manifest_sha256: sha('7'),
        capability_matrix_sha256: gate_a_capability_matrix_digest(matrix).unwrap(),
        run_manifest_sha256: sha('9'),
        training_dataset_ready: false,
        training_split_manifest_id: "N/A".to_owned(),
        training_split_manifest_sha256: "N/A".to_owned(),
    };
    receipt.receipt_id = derive_gate_a_receipt_id(&receipt).unwrap();
    receipt
}

#[test]
fn stable_event_encoding_matches_golden_and_roundtrips() {
    let event = base_event();
    let encoded = encode_execution_market_event(&event).unwrap();
    let actual = String::from_utf8(encoded.clone()).unwrap();
    let expected = include_str!("fixtures/execution_market_event_v1.json").trim_end();
    if actual != expected {
        eprintln!("ACTUAL_EVENT={actual}");
    }
    assert_eq!(actual, expected);
    assert_eq!(decode_execution_market_event(&encoded).unwrap(), event);
}

#[test]
fn every_frozen_event_payload_validates() {
    for payload in all_event_payloads() {
        validate_execution_market_event(&event_with_payload(payload)).unwrap();
    }
}

#[test]
fn initial_l2_snapshot_decodes_source_initial_tick() {
    let snapshot = event_with_payload(all_event_payloads().remove(1));
    let mut value = serde_json::to_value(snapshot).unwrap();
    value["payload"]["event"]["initial_tick_size_pusd_per_share"] = json!("0.01");

    let mut decoded: ExecutionMarketEventV1 = serde_json::from_value(value).unwrap();
    seal_event(&mut decoded);
    validate_execution_market_event(&decoded).unwrap();
}

#[test]
fn initial_l2_snapshot_requires_source_initial_tick() {
    let snapshot = event_with_payload(all_event_payloads().remove(1));
    let mut value = serde_json::to_value(snapshot).unwrap();
    value["payload"]["event"]
        .as_object_mut()
        .unwrap()
        .remove("initial_tick_size_pusd_per_share");

    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn initial_l2_snapshot_decodes_without_purpose() {
    let snapshot = event_with_payload(all_event_payloads().remove(1));
    let mut value = serde_json::to_value(snapshot).unwrap();
    value["payload"]["event"]
        .as_object_mut()
        .unwrap()
        .remove("purpose");

    let mut decoded: ExecutionMarketEventV1 = serde_json::from_value(value).unwrap();
    seal_event(&mut decoded);
    validate_execution_market_event(&decoded).unwrap();
}

#[test]
fn initial_l2_snapshot_rejects_legacy_purpose() {
    let snapshot = event_with_payload(all_event_payloads().remove(1));
    let mut value = serde_json::to_value(snapshot).unwrap();
    value["payload"]["event"]["purpose"] = json!("INITIAL_LOAD");

    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn tick_size_change_accepts_unknown_old_tick() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["payload"]["event"]["old_tick_size_pusd_per_share"] = Value::Null;

    let mut decoded: ExecutionMarketEventV1 = serde_json::from_value(value).unwrap();
    seal_event(&mut decoded);
    validate_execution_market_event(&decoded).unwrap();
    let encoded = encode_execution_market_event(&decoded).unwrap();
    assert_eq!(decode_execution_market_event(&encoded).unwrap(), decoded);

    let mut missing_value = serde_json::to_value(base_event()).unwrap();
    missing_value["payload"]["event"]
        .as_object_mut()
        .unwrap()
        .remove("old_tick_size_pusd_per_share");
    let mut missing: ExecutionMarketEventV1 = serde_json::from_value(missing_value).unwrap();
    seal_event(&mut missing);
    validate_execution_market_event(&missing).unwrap();
    let encoded_missing = encode_execution_market_event(&missing).unwrap();
    assert!(serde_json::from_slice::<Value>(&encoded_missing).unwrap()["payload"]["event"]
        ["old_tick_size_pusd_per_share"]
        .is_null());
    assert_eq!(
        decode_execution_market_event(&encoded_missing).unwrap(),
        missing
    );
}

#[test]
fn nonpositive_tick_is_rejected_at_decode_boundary() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["payload"]["event"]["new_tick_size_pusd_per_share"] = json!("0");
    let encoded = serde_json::to_vec(&value).unwrap();

    assert!(matches!(
        decode_execution_market_event(&encoded),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.new_tick_size_pusd_per_share"
    ));
}

#[test]
fn market_trading_state_roundtrips_independent_flags() {
    let lifecycle = event_with_payload(all_event_payloads().remove(0));
    let mut first_value = serde_json::to_value(lifecycle).unwrap();
    first_value["payload"]["event_type"] = json!("market_trading_state");
    first_value["payload"]["event"] = json!({
        "active": true,
        "closed": false,
        "accepting_orders": false,
        "source_event_ids": [event_id('a').0],
    });

    let mut first: ExecutionMarketEventV1 = serde_json::from_value(first_value.clone()).unwrap();
    seal_event(&mut first);
    validate_execution_market_event(&first).unwrap();
    let encoded = encode_execution_market_event(&first).unwrap();
    assert_eq!(decode_execution_market_event(&encoded).unwrap(), first);

    first_value["payload"]["event"]["accepting_orders"] = json!(true);
    let mut second: ExecutionMarketEventV1 = serde_json::from_value(first_value).unwrap();
    seal_event(&mut second);
    validate_execution_market_event(&second).unwrap();
    assert_ne!(first.event_id, second.event_id);
}

#[test]
fn market_trading_state_accepts_each_independent_optional_fact() {
    for state in [
        MarketTradingStateV1 {
            active: Some(true),
            closed: None,
            accepting_orders: None,
            source_event_ids: vec![event_id('a')],
        },
        MarketTradingStateV1 {
            active: None,
            closed: Some(false),
            accepting_orders: None,
            source_event_ids: vec![event_id('a')],
        },
        MarketTradingStateV1 {
            active: None,
            closed: None,
            accepting_orders: Some(true),
            source_event_ids: vec![event_id('a')],
        },
    ] {
        let event = event_with_payload(ExecutionMarketEventPayloadV1::MarketTradingState(state));
        let encoded = serde_json::to_vec(&event).unwrap();
        assert_eq!(decode_execution_market_event(&encoded).unwrap(), event);
    }
}

#[test]
fn public_trade_id_source_roundtrips() {
    let trade = event_with_payload(all_event_payloads().remove(4));
    let mut value = serde_json::to_value(trade).unwrap();
    value["payload"]["event"]["trade_id_source"] = json!("DERIVED_SOURCE_EVIDENCE");

    let mut decoded: ExecutionMarketEventV1 = serde_json::from_value(value).unwrap();
    seal_event(&mut decoded);
    validate_execution_market_event(&decoded).unwrap();
    let encoded = encode_execution_market_event(&decoded).unwrap();
    assert_eq!(decode_execution_market_event(&encoded).unwrap(), decoded);
}

#[test]
fn public_trade_id_source_is_required() {
    let trade = event_with_payload(all_event_payloads().remove(4));
    let mut value = serde_json::to_value(trade).unwrap();
    value["payload"]["event"]
        .as_object_mut()
        .unwrap()
        .remove("trade_id_source");

    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn public_trade_platform_id_source_changes_event_identity() {
    let derived = event_with_payload(all_event_payloads().remove(4));
    let mut platform_value = serde_json::to_value(&derived).unwrap();
    platform_value["payload"]["event"]["trade_id_source"] = json!("PLATFORM_TRANSACTION_HASH");
    let mut platform: ExecutionMarketEventV1 =
        serde_json::from_value(platform_value.clone()).unwrap();
    seal_event(&mut platform);
    validate_execution_market_event(&platform).unwrap();
    assert_ne!(derived.event_id, platform.event_id);
}

#[test]
fn public_trade_id_source_json_boundary_rejects_unknown_enum() {
    let trade = event_with_payload(all_event_payloads().remove(4));
    let mut value = serde_json::to_value(trade).unwrap();
    value["payload"]["event"]["trade_id_source"] = json!("UNKNOWN_SOURCE");
    let error = decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn explicit_unknown_lifecycle_code_roundtrips() {
    let lifecycle = event_with_payload(all_event_payloads().remove(0));
    let bytes = encode_execution_market_event(&lifecycle).unwrap();
    assert_eq!(decode_execution_market_event(&bytes).unwrap(), lifecycle);
}

#[test]
fn known_lifecycle_state_json_boundary_rejects_unmapped_value() {
    let known = event_with_payload(ExecutionMarketEventPayloadV1::MarketLifecycleTransition(
        MarketLifecycleTransitionV1 {
            previous_state: Some(MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Prefetched,
            }),
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            },
            assets: vec![asset("123", 0, "UP"), asset("456", 1, "DOWN")],
            source_event_ids: vec![event_id('a')],
        },
    ));
    validate_execution_market_event(&known).unwrap();
    let mut masquerading_known = serde_json::to_value(known).unwrap();
    masquerading_known["payload"]["event"]["current_state"]["value"] = json!("future_state");
    let error = decode_execution_market_event(&serde_json::to_vec(&masquerading_known).unwrap())
        .unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn event_json_boundary_rejects_unsupported_schema_version() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["schema_version"] = json!("execution_market_event_v2");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::UnsupportedSchema { contract, actual })
            if contract == "execution market event" && actual == "execution_market_event_v2"
    ));
}

#[test]
fn event_json_boundary_rejects_unknown_quality_enum() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["quality"]["availability"] = json!("future_quality");
    let error = decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn event_json_boundary_rejects_non_event_platform_rule_field() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["payload"]["event"]["min_order_size"] = json!("1");
    let error = decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn event_json_boundary_rejects_legacy_lineage_derivation_digest() {
    let mut legacy_lineage_digest = serde_json::to_value(base_event()).unwrap();
    legacy_lineage_digest["lineage"]["derivation_digest_sha256"] = json!(sha('7').0);
    let error = decode_execution_market_event(&serde_json::to_vec(&legacy_lineage_digest).unwrap())
        .unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn terminal_json_boundary_rejects_legacy_derivation_digest() {
    let terminal = event_with_payload(all_event_payloads().pop().unwrap());
    let mut legacy_terminal_digest = serde_json::to_value(terminal).unwrap();
    legacy_terminal_digest["payload"]["event"]["derivation_digest_sha256"] = json!(sha('7').0);
    let error =
        decode_execution_market_event(&serde_json::to_vec(&legacy_terminal_digest).unwrap())
            .unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn public_trade_fee_rate_is_preserved_in_json() {
    let trade = event_with_payload(all_event_payloads().remove(4));
    validate_execution_market_event(&trade).unwrap();
    assert!(
        serde_json::to_string(&trade)
            .unwrap()
            .contains("fee_rate_bps")
    );
}

#[test]
fn physical_lineage_changes_do_not_change_identity_but_semantics_do() {
    let event = base_event();
    let mut resharded = event.clone();
    resharded.lineage.source_evidence[0].input_manifest_sha256 = sha('1');
    resharded.lineage.source_evidence[0].input_file_sha256 = sha('2');
    assert_eq!(
        derive_canonical_event_id(&event).unwrap(),
        derive_canonical_event_id(&resharded).unwrap()
    );
    assert_eq!(
        derive_semantic_tiebreak(&event).unwrap(),
        derive_semantic_tiebreak(&resharded).unwrap()
    );
    validate_execution_market_event(&resharded).unwrap();

    let mut changed = event.clone();
    let ExecutionMarketEventPayloadV1::TickSizeChange(payload) = &mut changed.payload else {
        unreachable!();
    };
    payload.new_tick_size_pusd_per_share = ExactDecimal("0.002".to_owned());
    assert_ne!(
        derive_canonical_event_id(&event).unwrap(),
        derive_canonical_event_id(&changed).unwrap()
    );
}

#[test]
fn rtds_json_boundary_rejects_collector_admission_evidence_quality() {
    let mut mismatched_quality = serde_json::to_value(rtds_event()).unwrap();
    mismatched_quality["time"]["ordering_evidence_quality"] =
        json!("COLLECTOR_ADMISSION_COORDINATE");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&mismatched_quality).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "time.ordering_evidence_quality"
    ));
}

#[test]
fn collector_admission_json_boundary_rejects_receive_time_after_admission() {
    let mut received_after_admission = serde_json::to_value(base_event()).unwrap();
    received_after_admission["time"]["received_at_utc_ms"] = json!(1_160);
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&received_after_admission).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "time.received_at_utc_ms"
    ));
}

#[test]
fn rtds_json_boundary_rejects_forged_collector_admission_coordinate() {
    let orderbook = base_event();
    let orderbook_value = serde_json::to_value(&orderbook).unwrap();
    let mut forged_rtds_admission = serde_json::to_value(rtds_event()).unwrap();
    forged_rtds_admission["time"]["collector_admission"] =
        orderbook_value["time"]["collector_admission"].clone();
    forged_rtds_admission["time"]["ordering_evidence_quality"] =
        json!("COLLECTOR_ADMISSION_COORDINATE");
    forged_rtds_admission["quality"]["ordering"] = json!("COLLECTOR_ADMISSION_COORDINATE");
    forged_rtds_admission["total_order_key"]["ordering_coordinate"] =
        orderbook_value["total_order_key"]["ordering_coordinate"].clone();
    forged_rtds_admission["lineage"]["source_evidence"][0]["ordering_coordinate"] =
        orderbook_value["lineage"]["source_evidence"][0]["ordering_coordinate"].clone();
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&forged_rtds_admission).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.ordering_coordinate"
    ));
}

#[test]
fn non_rtds_json_boundary_rejects_received_only_coordinate_downgrade() {
    let rtds_value = serde_json::to_value(rtds_event()).unwrap();
    let mut downgraded_orderbook = serde_json::to_value(base_event()).unwrap();
    downgraded_orderbook["time"]["collector_admission"] = Value::Null;
    downgraded_orderbook["time"]["ordering_evidence_quality"] =
        json!("RECEIVED_TIME_STABLE_EVIDENCE");
    downgraded_orderbook["quality"]["ordering"] = json!("RECEIVED_TIME_STABLE_EVIDENCE");
    downgraded_orderbook["total_order_key"]["ordering_coordinate"] =
        rtds_value["total_order_key"]["ordering_coordinate"].clone();
    downgraded_orderbook["lineage"]["source_evidence"][0]["ordering_coordinate"] =
        rtds_value["lineage"]["source_evidence"][0]["ordering_coordinate"].clone();
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&downgraded_orderbook).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.ordering_coordinate"
    ));
}

#[test]
fn rtds_received_only_coordinate_rejects_received_after_availability_at_json_boundary() {
    let mut value = serde_json::to_value(rtds_event()).unwrap();
    value["total_order_key"]["ordering_coordinate"]["received_at_utc_ms"] = json!(1_201);

    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.ordering_coordinate.received_at_utc_ms"
    ));
}

#[test]
fn lifecycle_json_boundary_rejects_duplicate_outcome_index_for_distinct_tokens() {
    let payload =
        ExecutionMarketEventPayloadV1::MarketLifecycleTransition(MarketLifecycleTransitionV1 {
            previous_state: None,
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            },
            assets: vec![asset("123", 0, "UP"), asset("456", 1, "DOWN")],
            source_event_ids: vec![event_id('a')],
        });
    let mut value = serde_json::to_value(event_with_payload(payload)).unwrap();
    value["payload"]["event"]["assets"][1]["outcome"]["index"] = json!(0);

    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.assets[1].outcome.index"
    ));
}

#[test]
fn total_order_is_stable_for_same_millisecond_and_uses_no_input_position() {
    let mut events = vec![base_event(), rtds_event()];
    let mut metadata = base_event();
    metadata.total_order_key.source_kind = CanonicalSourceKind::PolymarketMarketMetadata;
    metadata.lineage.source_evidence[0].source_kind = CanonicalSourceKind::PolymarketMarketMetadata;
    seal_event(&mut metadata);
    events.push(metadata);

    let mut first = events.clone();
    first.sort_by(|left, right| {
        compare_total_order_keys(&left.total_order_key, &right.total_order_key).unwrap()
    });
    events.reverse();
    events.sort_by(|left, right| {
        compare_total_order_keys(&left.total_order_key, &right.total_order_key).unwrap()
    });
    assert_eq!(
        first
            .iter()
            .map(|event| &event.event_id)
            .collect::<Vec<_>>(),
        events
            .iter()
            .map(|event| &event.event_id)
            .collect::<Vec<_>>()
    );
    assert!(first.windows(2).all(|pair| {
        compare_total_order_keys(&pair[0].total_order_key, &pair[1].total_order_key).unwrap()
            == Ordering::Less
    }));
}

#[test]
fn total_order_key_bytes_match_frozen_vectors() {
    let vectors = [
        (
            "Binance 聚合成交来源与原生成交身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1_750_000_000_000,
                source_kind: CanonicalSourceKind::BinanceAggtrade,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 7,
                },
                source_native_identity: Some(SourceNativeIdentityV1::BinanceAggregateTrade {
                    aggregate_trade_id: 1,
                }),
                semantic_tiebreak_sha256: sha('0'),
            },
            concat!(
                "800001977420dc00",
                "00",
                "00",
                "65706f63682d310000",
                "0000000000000007",
                "01",
                "00",
                "0000000000000001",
                "00000000000000000000000000000000",
                "00000000000000000000000000000000",
            ),
        ),
        (
            "Binance 深度快照来源与原生快照身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1_750_000_000_000,
                source_kind: CanonicalSourceKind::BinanceDepthSnapshot,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 7,
                },
                source_native_identity: Some(SourceNativeIdentityV1::BinanceDepthSnapshot {
                    last_update_id: 2,
                }),
                semantic_tiebreak_sha256: sha('1'),
            },
            concat!(
                "800001977420dc00",
                "01",
                "00",
                "65706f63682d310000",
                "0000000000000007",
                "01",
                "01",
                "0000000000000002",
                "11111111111111111111111111111111",
                "11111111111111111111111111111111",
            ),
        ),
        (
            "Binance 深度更新来源与原生更新范围身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1_750_000_000_000,
                source_kind: CanonicalSourceKind::BinanceDepthUpdate,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 7,
                },
                source_native_identity: Some(SourceNativeIdentityV1::BinanceDepthUpdate {
                    first_update_id: 3,
                    final_update_id: 4,
                }),
                semantic_tiebreak_sha256: sha('2'),
            },
            concat!(
                "800001977420dc00",
                "02",
                "00",
                "65706f63682d310000",
                "0000000000000007",
                "01",
                "02",
                "0000000000000003",
                "0000000000000004",
                "22222222222222222222222222222222",
                "22222222222222222222222222222222",
            ),
        ),
        (
            "Polymarket 市场元数据来源与语义证据身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1_750_000_000_000,
                source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 7,
                },
                source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: sha('4'),
                }),
                semantic_tiebreak_sha256: sha('5'),
            },
            concat!(
                "800001977420dc00",
                "03",
                "00",
                "65706f63682d310000",
                "0000000000000007",
                "01",
                "03",
                "44444444444444444444444444444444",
                "44444444444444444444444444444444",
                "55555555555555555555555555555555",
                "55555555555555555555555555555555",
            ),
        ),
        (
            "Polymarket 订单簿来源与语义证据身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1_750_000_000_000,
                source_kind: CanonicalSourceKind::PolymarketOrderbook,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "epoch-1".to_owned(),
                    collector_admission_sequence: 7,
                },
                source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: sha('6'),
                }),
                semantic_tiebreak_sha256: sha('6'),
            },
            concat!(
                "800001977420dc00",
                "04",
                "00",
                "65706f63682d310000",
                "0000000000000007",
                "01",
                "03",
                "66666666666666666666666666666666",
                "66666666666666666666666666666666",
                "66666666666666666666666666666666",
                "66666666666666666666666666666666",
            ),
        ),
        (
            "Polymarket RTDS 来源、接收时间坐标与语义证据身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1_750_000_005_000,
                source_kind: CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
                ordering_coordinate: OrderingCoordinateV1::ReceivedOnly {
                    received_at_utc_ms: 1_750_000_004_900,
                    stable_evidence_key_sha256: sha('7'),
                },
                source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: sha('8'),
                }),
                semantic_tiebreak_sha256: sha('9'),
            },
            concat!(
                "800001977420ef88",
                "05",
                "01",
                "800001977420ef24",
                "77777777777777777777777777777777",
                "77777777777777777777777777777777",
                "01",
                "03",
                "88888888888888888888888888888888",
                "88888888888888888888888888888888",
                "99999999999999999999999999999999",
                "99999999999999999999999999999999",
            ),
        ),
    ];

    for (name, key, expected) in vectors {
        assert_eq!(
            bytes_to_hex(&total_order_key_bytes(&key).unwrap()),
            expected,
            "{name}"
        );
    }
}

#[test]
fn total_order_key_bytes_rejects_unknown_version() {
    let mut unknown_version = admission_key(CanonicalSourceKind::PolymarketOrderbook);
    unknown_version.version = "total_order_key_v2".to_owned();
    assert!(total_order_key_bytes(&unknown_version).is_err());
}

#[test]
fn event_json_boundary_rejects_noncanonical_decimal_encoding() {
    let mut malformed_decimal = serde_json::to_value(base_event()).unwrap();
    malformed_decimal["payload"]["event"]["new_tick_size_pusd_per_share"] = json!("1e-3");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&malformed_decimal).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.new_tick_size_pusd_per_share"
    ));
}

#[test]
fn level_transition_json_boundary_rejects_quantity_nonconservation() {
    let trade_transition = event_with_payload(all_event_payloads().remove(5));
    let mut broken_conservation = serde_json::to_value(trade_transition).unwrap();
    broken_conservation["payload"]["event"]["net_reduced_quantity_shares"] = json!("2");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&broken_conservation).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "payload.event"
    ));
}

#[test]
fn level_transition_json_boundary_rejects_duplicate_source_event_ids() {
    let trade_transition = event_with_payload(all_event_payloads().remove(5));
    let mut duplicate_source_event = serde_json::to_value(trade_transition).unwrap();
    duplicate_source_event["payload"]["event"]["source_event_ids"]
        .as_array_mut()
        .unwrap()
        .push(json!(event_id('a').0));
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&duplicate_source_event).unwrap()),
        Err(ContractError::DuplicateValue { field, .. })
            if field == "payload.event.source_event_ids"
    ));
}

#[test]
fn inferred_level_transition_json_boundary_rejects_false_exact_attribution() {
    let inferred_transition = event_with_payload(all_event_payloads().remove(6));
    let mut false_exact_attribution = serde_json::to_value(inferred_transition).unwrap();
    false_exact_attribution["payload"]["event"]["source_attribution_quality"] = json!("EXACT");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&false_exact_attribution).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.source_attribution_quality"
    ));
}

#[test]
fn terminal_json_boundary_rejects_incomplete_binary_payout() {
    let resolved = event_with_payload(all_event_payloads().pop().unwrap());
    let mut incomplete_payout = serde_json::to_value(resolved).unwrap();
    incomplete_payout["payload"]["event"]["payout_vector"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&incomplete_payout).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.payout_vector"
    ));
}

#[test]
fn terminal_json_boundary_rejects_non_unit_winner_payout() {
    let resolved = event_with_payload(all_event_payloads().pop().unwrap());
    let mut non_unit_winner = serde_json::to_value(resolved).unwrap();
    non_unit_winner["payload"]["event"]["payout_vector"][0]["payout_pusd_per_share"] = json!("0.5");
    non_unit_winner["payload"]["event"]["payout_vector"][1]["payout_pusd_per_share"] = json!("0.5");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&non_unit_winner).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.payout_vector"
    ));
}

#[test]
fn terminal_json_boundary_rejects_future_watermark() {
    let resolved = event_with_payload(all_event_payloads().pop().unwrap());
    let mut future_watermark = serde_json::to_value(resolved).unwrap();
    future_watermark["payload"]["event"]["terminal_watermark"]["last_total_order_key"]["available_at_utc_ms"] =
        json!(1_201);
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&future_watermark).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.terminal_watermark.last_total_order_key"
    ));
}

#[test]
fn waiting_exhausted_quarantine_is_a_legal_terminal() {
    let quarantine = MarketResolutionTerminalV1::WaitingExhaustedQuarantine {
        reason_code: "resolution_wait_exhausted".to_owned(),
        source_event_ids: vec![event_id('a')],
        terminal_watermark: TerminalWatermarkV1 {
            last_total_order_key: admission_key(CanonicalSourceKind::PolymarketMarketMetadata),
            canonical_event_root_sha256: sha('6'),
            lineage_identity_sha256: sha('5'),
        },
    };
    validate_execution_market_event(&event_with_payload(
        ExecutionMarketEventPayloadV1::MarketResolutionTerminal(quarantine),
    ))
    .unwrap();
}

#[test]
fn quarantine_json_boundary_rejects_empty_reason_code() {
    let quarantine = MarketResolutionTerminalV1::WaitingExhaustedQuarantine {
        reason_code: "resolution_wait_exhausted".to_owned(),
        source_event_ids: vec![event_id('a')],
        terminal_watermark: TerminalWatermarkV1 {
            last_total_order_key: admission_key(CanonicalSourceKind::PolymarketMarketMetadata),
            canonical_event_root_sha256: sha('6'),
            lineage_identity_sha256: sha('5'),
        },
    };
    let quarantine_event = event_with_payload(
        ExecutionMarketEventPayloadV1::MarketResolutionTerminal(quarantine),
    );
    let mut invalid_quarantine = serde_json::to_value(quarantine_event).unwrap();
    invalid_quarantine["payload"]["event"]["reason_code"] = json!("");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&invalid_quarantine).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.reason_code"
    ));
}

#[test]
fn event_json_boundary_rejects_noncanonical_condition_identity_encoding() {
    let mut invalid_condition_encoding = serde_json::to_value(base_event()).unwrap();
    invalid_condition_encoding["market"]["condition_id"] = json!(format!("0x{}", "A".repeat(64)));
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&invalid_condition_encoding).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "market.condition_id"
    ));
}

#[test]
fn event_json_boundary_rejects_noncanonical_source_digest_encoding() {
    let mut invalid_digest = serde_json::to_value(base_event()).unwrap();
    invalid_digest["lineage"]["source_evidence"][0]["input_file_sha256"] = json!("A".repeat(64));
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&invalid_digest).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "lineage.source_evidence[0].input_file_sha256"
    ));
}

#[test]
fn event_json_boundary_rejects_source_kind_and_native_identity_conflict() {
    let mut mismatched_source_identity = serde_json::to_value(base_event()).unwrap();
    let binance_identity = json!({
        "kind": "BINANCE_AGGREGATE_TRADE",
        "aggregate_trade_id": 7
    });
    mismatched_source_identity["total_order_key"]["source_native_identity"] =
        binance_identity.clone();
    mismatched_source_identity["lineage"]["source_evidence"][0]["source_native_identity"] =
        binance_identity;
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&mismatched_source_identity).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.source_native_identity"
    ));
}

#[test]
fn observation_encoding_matches_golden_and_roundtrips() {
    let event = base_event();
    let observation = base_observation(&event);
    let encoded = encode_market_observation_causal_shell(&observation).unwrap();
    let actual = String::from_utf8(encoded.clone()).unwrap();
    let expected = include_str!("fixtures/market_observation_causal_shell_v1.json").trim_end();
    if actual != expected {
        eprintln!("ACTUAL_OBSERVATION={actual}");
    }
    assert_eq!(actual, expected);
    assert_eq!(
        decode_market_observation_causal_shell(&encoded).unwrap(),
        observation
    );
    assert!(
        observation.causal_cutoff.available_at_utc_ms
            > observation
                .causal_cutoff
                .through_total_order_key
                .available_at_utc_ms
    );
}

#[test]
fn observation_json_boundary_rejects_future_component_evidence() {
    let event = base_event();
    let observation = base_observation(&event);
    let mut future_component = serde_json::to_value(observation).unwrap();
    future_component["component_states"][2]["as_of_available_at_utc_ms"] = json!(1_251);
    future_component["component_states"][2]["information_age_ms"] = json!(0);
    assert!(matches!(
        decode_market_observation_causal_shell(&serde_json::to_vec(&future_component).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "component_states[2].as_of_available_at_utc_ms"
    ));
}

#[test]
fn observation_json_boundary_rejects_visible_component_in_missing_mask() {
    let event = base_event();
    let observation = base_observation(&event);
    let mut missing_mask_mismatch = serde_json::to_value(observation).unwrap();
    missing_mask_mismatch["missing_mask"]
        .as_array_mut()
        .unwrap()
        .insert(2, json!("current_tick"));
    assert!(matches!(
        decode_market_observation_causal_shell(
            &serde_json::to_vec(&missing_mask_mismatch).unwrap()
        ),
        Err(ContractError::InvalidField { field, .. }) if field == "component_states[2]"
    ));
}

#[test]
fn observation_json_boundary_rejects_visible_event_outside_lineage() {
    let event = base_event();
    let observation = base_observation(&event);
    let mut outside_lineage = serde_json::to_value(observation).unwrap();
    outside_lineage["component_states"][2]["last_visible_event_id"] = json!(event_id('b').0);
    assert!(matches!(
        decode_market_observation_causal_shell(&serde_json::to_vec(&outside_lineage).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "component_states[2].last_visible_event_id"
    ));
}

#[test]
fn base_observation_visible_component_matches_tick_size_change_event() {
    let event = base_event();
    assert!(matches!(
        event.payload,
        ExecutionMarketEventPayloadV1::TickSizeChange(_)
    ));
    let observation = base_observation(&event);
    let visible = observation
        .component_states
        .iter()
        .find(|component| component.last_visible_event_id.as_ref() == Some(&event.event_id))
        .unwrap();

    assert_eq!(visible.component, ObservationComponentId::CurrentTick);
}

#[test]
fn base_observation_visible_component_is_not_older_than_its_event() {
    let event = base_event();
    let observation = base_observation(&event);
    let visible = observation
        .component_states
        .iter()
        .find(|component| component.last_visible_event_id.as_ref() == Some(&event.event_id))
        .unwrap();

    assert!(
        visible.as_of_available_at_utc_ms.unwrap() >= event.time.available_at_utc_ms,
        "visible component precedes its visible event"
    );
}

#[test]
fn gate_a_receipt_encoding_matches_golden_and_roundtrips() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let actual = String::from_utf8(encoded.clone()).unwrap();
    let expected = include_str!("fixtures/real_execution_tape_receipt_v1.json").trim_end();
    if actual != expected {
        eprintln!("ACTUAL_RECEIPT={actual}");
    }
    assert_eq!(actual, expected);
    assert_eq!(decode_gate_a_receipt(&encoded, &pass).unwrap(), ready);
}

#[test]
fn gate_a_receipt_json_boundary_rejects_training_dataset_ready_flag() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let ready_value: Value = serde_json::from_slice(&encoded).unwrap();
    let mut training_ready = ready_value;
    training_ready["training_dataset_ready"] = json!(true);
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&training_ready).unwrap(), &pass),
        Err(ContractError::InvalidField { field, .. }) if field == "training_dataset_ready"
    ));
}

#[test]
fn gate_a_receipt_json_boundary_rejects_assigned_training_split() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let mut assigned_split: Value = serde_json::from_slice(&encoded).unwrap();
    assigned_split["training_split_manifest_id"] = json!("split-1");
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&assigned_split).unwrap(), &pass),
        Err(ContractError::InvalidField { field, .. }) if field == "training_split_manifest"
    ));
}

#[test]
fn gate_a_receipt_json_boundary_rejects_unsorted_covered_dates() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let mut unsorted_dates: Value = serde_json::from_slice(&encoded).unwrap();
    unsorted_dates["covered_utc_dates"] = json!(["2026-08-02", "2026-07-30"]);
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&unsorted_dates).unwrap(), &pass),
        Err(ContractError::InvalidField { field, .. }) if field == "covered_utc_dates"
    ));
}

#[test]
fn gate_a_receipt_json_boundary_rejects_stale_content_address() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let mut stale_content_id: Value = serde_json::from_slice(&encoded).unwrap();
    stale_content_id["run_manifest_sha256"] = json!(sha('a').0);
    let error =
        decode_gate_a_receipt(&serde_json::to_vec(&stale_content_id).unwrap(), &pass).unwrap_err();
    assert!(matches!(
        error,
        ContractError::InvalidField { field, .. } if field == "receipt_id"
    ));
}

#[test]
fn gate_a_receipt_json_boundary_rejects_capability_matrix_digest_mismatch() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let mut changed_matrix_value = serde_json::to_value(&pass).unwrap();
    changed_matrix_value["entries"][0]["evidence_ids"] = json!(["replacement-evidence"]);
    let changed_matrix =
        decode_gate_a_capability_matrix(&serde_json::to_vec(&changed_matrix_value).unwrap())
            .unwrap();
    assert!(matches!(
        decode_gate_a_receipt(&encoded, &changed_matrix),
        Err(ContractError::InvalidField { field, .. }) if field == "capability_matrix_sha256"
    ));
}

#[test]
fn gate_a_receipt_json_boundary_rejects_mixed_capability_failure() {
    let mixed_failure = matrix_with(|index| {
        if index == 0 {
            CapabilityStatus::Pass
        } else {
            CapabilityStatus::Fail
        }
    });
    let receipt = receipt_for(&mixed_failure);
    let encoded = serde_json::to_vec(&receipt).unwrap();
    assert!(matches!(
        decode_gate_a_receipt(&encoded, &mixed_failure),
        Err(ContractError::InvalidField { field, .. }) if field == "capability_matrix"
    ));
}

#[test]
fn gate_a_receipt_encoding_omits_legacy_state_fields() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let value: Value = serde_json::from_slice(&encoded).unwrap();
    let fields = value.as_object().unwrap();
    assert!(!fields.contains_key("tape_assessment"));
    assert!(!fields.contains_key("published_artifact_kind"));
}

#[test]
fn gate_a_receipt_rejects_legacy_tape_assessment_field() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let mut legacy: Value = serde_json::from_slice(&encoded).unwrap();
    let fields = legacy.as_object_mut().unwrap();
    fields.insert("tape_assessment".to_owned(), json!("PASS"));
    let legacy = serde_json::to_vec(&legacy).unwrap();
    let error = decode_gate_a_receipt(&legacy, &pass).unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn gate_a_receipt_rejects_legacy_published_artifact_kind_field() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let ready = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&ready, &pass).unwrap();
    let mut legacy: Value = serde_json::from_slice(&encoded).unwrap();
    legacy.as_object_mut().unwrap().insert(
        "published_artifact_kind".to_owned(),
        json!("REAL_EXECUTION_TAPE_READY"),
    );
    let legacy = serde_json::to_vec(&legacy).unwrap();
    let error = decode_gate_a_receipt(&legacy, &pass).unwrap_err();
    assert!(matches!(error, ContractError::Json(_)));
}

#[test]
fn not_evaluated_is_not_a_capability_status() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let mut value = serde_json::to_value(&pass).unwrap();
    value["entries"][0]["status"] = json!("NOT_EVALUATED");
    let encoded = serde_json::to_vec(&value).unwrap();
    assert!(decode_gate_a_capability_matrix(&encoded).is_err());
}

#[test]
fn fail_capability_json_boundary_rejects_whitespace_only_blocking_reason() {
    let matrix = matrix_with(|_| CapabilityStatus::Fail);
    let mut value = serde_json::to_value(matrix).unwrap();
    value["entries"][0]["blocking_reason"] = json!(" \t ");

    let error = decode_gate_a_capability_matrix(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert!(matches!(
        error,
        ContractError::InvalidField { field, .. }
            if field == "entries[0].blocking_reason"
    ));
}

#[test]
fn capability_matrix_json_boundary_requires_complete_static_set() {
    let matrix = matrix_with(|_| CapabilityStatus::Pass);
    let mut incomplete_static_set = serde_json::to_value(matrix).unwrap();
    incomplete_static_set["entries"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(matches!(
        decode_gate_a_capability_matrix(&serde_json::to_vec(&incomplete_static_set).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "entries"
    ));
}

#[test]
fn pass_capability_json_boundary_rejects_blocking_reason() {
    let matrix = matrix_with(|_| CapabilityStatus::Pass);
    let mut pass_with_blocking_reason = serde_json::to_value(matrix).unwrap();
    pass_with_blocking_reason["entries"][0]["blocking_reason"] = json!("cannot-pass");
    assert!(matches!(
        decode_gate_a_capability_matrix(&serde_json::to_vec(&pass_with_blocking_reason).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "entries[0].blocking_reason"
    ));
}

#[test]
fn pass_capability_json_boundary_requires_evidence() {
    let matrix = matrix_with(|_| CapabilityStatus::Pass);
    let mut pass_without_evidence = serde_json::to_value(matrix).unwrap();
    pass_without_evidence["entries"][0]["evidence_ids"] = json!([]);
    assert!(matches!(
        decode_gate_a_capability_matrix(&serde_json::to_vec(&pass_without_evidence).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "entries[0].evidence_ids"
    ));
}

#[test]
fn dataset_floor_is_terminal_and_eligible_dates_remain_unassigned() {
    assert_eq!(
        classify_dataset_date("2026-07-29").unwrap(),
        DatasetDateEligibility::IneligibleBeforeDatasetFloor
    );
    assert_eq!(
        classify_dataset_date("2026-07-30").unwrap(),
        DatasetDateEligibility::Unassigned
    );
    assert_eq!(
        classify_dataset_date("2026-08-03").unwrap(),
        DatasetDateEligibility::Unassigned
    );
}

#[test]
fn dataset_date_rejects_invalid_calendar_date() {
    assert!(classify_dataset_date("2026-02-30").is_err());
}

#[test]
fn unknown_observation_field_is_rejected() {
    let event = base_event();
    let observation = base_observation(&event);
    let mut value = serde_json::to_value(observation).unwrap();
    let Value::Object(root) = &mut value else {
        unreachable!();
    };
    root.insert("future_label".to_owned(), json!(1));
    assert!(decode_market_observation_causal_shell(&serde_json::to_vec(&value).unwrap()).is_err());
}
