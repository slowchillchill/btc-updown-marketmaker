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

    assert!(serde_json::from_value::<ExecutionMarketEventV1>(value).is_err());
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

    assert!(serde_json::from_value::<ExecutionMarketEventV1>(value).is_err());
}

#[test]
fn initial_l2_snapshot_rejects_nonpositive_source_initial_tick() {
    let mut payload = all_event_payloads().remove(1);
    let ExecutionMarketEventPayloadV1::InitialL2Snapshot(snapshot) = &mut payload else {
        unreachable!();
    };
    snapshot.initial_tick_size_pusd_per_share = ExactDecimal("0".to_owned());
    let event = event_with_payload(payload);

    assert!(matches!(
        validate_execution_market_event(&event),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.initial_tick_size_pusd_per_share"
    ));
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
fn tick_size_change_rejects_nonpositive_known_old_tick() {
    let mut event = base_event();
    let ExecutionMarketEventPayloadV1::TickSizeChange(change) = &mut event.payload else {
        unreachable!();
    };
    change.old_tick_size_pusd_per_share = Some(ExactDecimal("0".to_owned()));
    seal_event(&mut event);

    assert!(matches!(
        validate_execution_market_event(&event),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.old_tick_size_pusd_per_share"
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
    for (field, value) in [
        ("active", true),
        ("closed", false),
        ("accepting_orders", true),
    ] {
        let lifecycle = event_with_payload(all_event_payloads().remove(0));
        let mut event_value = serde_json::to_value(lifecycle).unwrap();
        event_value["payload"]["event_type"] = json!("market_trading_state");
        event_value["payload"]["event"] = json!({
            "source_event_ids": [event_id('a').0],
        });
        event_value["payload"]["event"]
            .as_object_mut()
            .unwrap()
            .insert(field.to_owned(), json!(value));

        let mut event: ExecutionMarketEventV1 = serde_json::from_value(event_value).unwrap();
        seal_event(&mut event);
        validate_execution_market_event(&event).unwrap();
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

    assert!(serde_json::from_value::<ExecutionMarketEventV1>(value).is_err());
}

#[test]
fn public_trade_id_source_rejects_unknown_and_changes_identity() {
    let derived = event_with_payload(all_event_payloads().remove(4));
    let mut platform_value = serde_json::to_value(&derived).unwrap();
    platform_value["payload"]["event"]["trade_id_source"] = json!("PLATFORM_TRANSACTION_HASH");
    let mut platform: ExecutionMarketEventV1 =
        serde_json::from_value(platform_value.clone()).unwrap();
    seal_event(&mut platform);
    validate_execution_market_event(&platform).unwrap();
    assert_ne!(derived.event_id, platform.event_id);

    platform_value["payload"]["event"]["trade_id_source"] = json!("UNKNOWN_SOURCE");
    assert!(serde_json::from_value::<ExecutionMarketEventV1>(platform_value).is_err());
}

#[test]
fn explicit_unknown_code_roundtrips_but_unknown_schema_enum_and_field_fail() {
    let lifecycle = event_with_payload(all_event_payloads().remove(0));
    let bytes = encode_execution_market_event(&lifecycle).unwrap();
    assert_eq!(decode_execution_market_event(&bytes).unwrap(), lifecycle);

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
    assert!(
        decode_execution_market_event(&serde_json::to_vec(&masquerading_known).unwrap()).is_err()
    );

    let event = base_event();
    let mut value = serde_json::to_value(event).unwrap();
    value["schema_version"] = json!("execution_market_event_v2");
    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());

    value["schema_version"] = json!(EXECUTION_MARKET_EVENT_SCHEMA_V1);
    value["quality"]["availability"] = json!("future_quality");
    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());

    let mut value = serde_json::to_value(base_event()).unwrap();
    value["payload"]["event"]["min_order_size"] = json!("1");
    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());
    for forbidden in ["feeSchedule", "itode", "oas", "seconds_delay"] {
        let mut value = serde_json::to_value(base_event()).unwrap();
        value["payload"]["event"][forbidden] = json!("forbidden");
        assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    let mut legacy_lineage_digest = serde_json::to_value(base_event()).unwrap();
    legacy_lineage_digest["lineage"]["derivation_digest_sha256"] = json!(sha('7').0);
    assert!(
        decode_execution_market_event(&serde_json::to_vec(&legacy_lineage_digest).unwrap())
            .is_err()
    );

    let terminal = event_with_payload(all_event_payloads().pop().unwrap());
    let mut legacy_terminal_digest = serde_json::to_value(terminal).unwrap();
    legacy_terminal_digest["payload"]["event"]["derivation_digest_sha256"] = json!(sha('7').0);
    assert!(
        decode_execution_market_event(&serde_json::to_vec(&legacy_terminal_digest).unwrap())
            .is_err()
    );

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
fn rtds_uses_received_only_lower_quality_and_time_cannot_move_backwards() {
    validate_execution_market_event(&rtds_event()).unwrap();
    let mut invalid = rtds_event();
    invalid.time.available_at_utc_ms = 1_099;
    invalid.total_order_key.available_at_utc_ms = 1_099;
    seal_event(&mut invalid);
    assert!(validate_execution_market_event(&invalid).is_err());

    let mut mismatched = rtds_event();
    mismatched.time.ordering_evidence_quality =
        OrderingEvidenceQuality::CollectorAdmissionCoordinate;
    seal_event(&mut mismatched);
    assert!(validate_execution_market_event(&mismatched).is_err());

    let mut received_after_admission = base_event();
    received_after_admission.time.received_at_utc_ms = 1_160;
    seal_event(&mut received_after_admission);
    assert!(validate_execution_market_event(&received_after_admission).is_err());

    let mut forged_rtds_admission = rtds_event();
    let admission = CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: "forged-rtds-epoch".to_owned(),
        collector_admission_sequence: 1,
        admitted_at_utc_ms: 1_150,
    };
    let coordinate = OrderingCoordinateV1::CollectorAdmission {
        source_sequence_epoch_id: admission.source_sequence_epoch_id.clone(),
        collector_admission_sequence: admission.collector_admission_sequence,
    };
    forged_rtds_admission.time.collector_admission = Some(admission);
    forged_rtds_admission.time.ordering_evidence_quality =
        OrderingEvidenceQuality::CollectorAdmissionCoordinate;
    forged_rtds_admission.quality.ordering = OrderingEvidenceQuality::CollectorAdmissionCoordinate;
    forged_rtds_admission.total_order_key.ordering_coordinate = coordinate.clone();
    forged_rtds_admission.lineage.source_evidence[0].ordering_coordinate = coordinate;
    seal_event(&mut forged_rtds_admission);
    assert!(validate_execution_market_event(&forged_rtds_admission).is_err());

    let mut downgraded_orderbook = base_event();
    let received = OrderingCoordinateV1::ReceivedOnly {
        received_at_utc_ms: downgraded_orderbook.time.received_at_utc_ms,
        stable_evidence_key_sha256: sha('9'),
    };
    downgraded_orderbook.time.collector_admission = None;
    downgraded_orderbook.time.ordering_evidence_quality =
        OrderingEvidenceQuality::ReceivedTimeStableEvidence;
    downgraded_orderbook.quality.ordering = OrderingEvidenceQuality::ReceivedTimeStableEvidence;
    downgraded_orderbook.total_order_key.ordering_coordinate = received.clone();
    downgraded_orderbook.lineage.source_evidence[0].ordering_coordinate = received;
    seal_event(&mut downgraded_orderbook);
    assert!(validate_execution_market_event(&downgraded_orderbook).is_err());
}

#[test]
fn total_order_key_bytes_rejects_received_only_after_availability() {
    let mut key = rtds_event().total_order_key;
    let OrderingCoordinateV1::ReceivedOnly {
        received_at_utc_ms, ..
    } = &mut key.ordering_coordinate
    else {
        unreachable!();
    };
    *received_at_utc_ms = key.available_at_utc_ms + 1;

    assert!(matches!(
        total_order_key_bytes(&key),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.ordering_coordinate.received_at_utc_ms"
    ));
}

#[test]
fn observation_through_total_order_key_rejects_received_only_after_availability() {
    let event = rtds_event();
    let mut observation = base_observation(&event);
    let OrderingCoordinateV1::ReceivedOnly {
        received_at_utc_ms, ..
    } = &mut observation
        .causal_cutoff
        .through_total_order_key
        .ordering_coordinate
    else {
        unreachable!();
    };
    *received_at_utc_ms = observation
        .causal_cutoff
        .through_total_order_key
        .available_at_utc_ms
        + 1;
    seal_observation(&mut observation);

    assert!(matches!(
        validate_market_observation_causal_shell(&observation),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.ordering_coordinate.received_at_utc_ms"
    ));
}

#[test]
fn terminal_watermark_rejects_received_only_after_availability() {
    let mut payload = all_event_payloads().pop().unwrap();
    let ExecutionMarketEventPayloadV1::MarketResolutionTerminal(
        MarketResolutionTerminalV1::Resolved {
            terminal_watermark, ..
        },
    ) = &mut payload
    else {
        unreachable!();
    };
    terminal_watermark.last_total_order_key = rtds_event().total_order_key;
    let OrderingCoordinateV1::ReceivedOnly {
        received_at_utc_ms, ..
    } = &mut terminal_watermark.last_total_order_key.ordering_coordinate
    else {
        unreachable!();
    };
    *received_at_utc_ms = terminal_watermark.last_total_order_key.available_at_utc_ms + 1;
    let event = event_with_payload(payload);

    assert!(matches!(
        validate_execution_market_event(&event),
        Err(ContractError::InvalidField { field, .. })
            if field == "total_order_key.ordering_coordinate.received_at_utc_ms"
    ));
}

#[test]
fn unselected_rtds_source_evidence_rejects_received_only_after_event_availability() {
    let mut event = base_event();
    let mut unselected = source_evidence(CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd);
    let OrderingCoordinateV1::ReceivedOnly {
        received_at_utc_ms, ..
    } = &mut unselected.ordering_coordinate
    else {
        unreachable!();
    };
    *received_at_utc_ms = event.time.available_at_utc_ms + 1;
    event.lineage.source_evidence.push(unselected);
    seal_event(&mut event);

    assert!(matches!(
        validate_execution_market_event(&event),
        Err(ContractError::InvalidField { field, .. })
            if field == "lineage.source_evidence[1].ordering_coordinate.received_at_utc_ms"
    ));
}

#[test]
fn lifecycle_assets_reject_duplicate_outcome_index_for_distinct_tokens() {
    let payload =
        ExecutionMarketEventPayloadV1::MarketLifecycleTransition(MarketLifecycleTransitionV1 {
            previous_state: None,
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            },
            assets: vec![asset("123", 0, "UP"), asset("456", 0, "DOWN")],
            source_event_ids: vec![event_id('a')],
        });
    let event = event_with_payload(payload);

    assert!(matches!(
        validate_execution_market_event(&event),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.assets[1].outcome.index"
    ));
}

#[test]
fn terminal_payout_vector_rejects_duplicate_outcome_index_for_distinct_tokens() {
    let mut payload = all_event_payloads().pop().unwrap();
    let ExecutionMarketEventPayloadV1::MarketResolutionTerminal(
        MarketResolutionTerminalV1::Resolved { payout_vector, .. },
    ) = &mut payload
    else {
        unreachable!();
    };
    payout_vector[1].asset.outcome.index = payout_vector[0].asset.outcome.index;
    let event = event_with_payload(payload);

    assert!(matches!(
        validate_execution_market_event(&event),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.payout_vector[1].asset.outcome.index"
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
            "最小时间、NUL 转义和 Binance 聚合成交身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: i64::MIN,
                source_kind: CanonicalSourceKind::BinanceAggtrade,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "a\0b".to_owned(),
                    collector_admission_sequence: 1,
                },
                source_native_identity: Some(SourceNativeIdentityV1::BinanceAggregateTrade {
                    aggregate_trade_id: 1,
                }),
                semantic_tiebreak_sha256: sha('0'),
            },
            concat!(
                "0000000000000000",
                "00",
                "00",
                "6100ff620000",
                "0000000000000001",
                "01",
                "00",
                "0000000000000001",
                "00000000000000000000000000000000",
                "00000000000000000000000000000000",
            ),
        ),
        (
            "负一时间和 Binance 深度快照身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: -1,
                source_kind: CanonicalSourceKind::BinanceDepthSnapshot,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "b".to_owned(),
                    collector_admission_sequence: 2,
                },
                source_native_identity: Some(SourceNativeIdentityV1::BinanceDepthSnapshot {
                    last_update_id: 2,
                }),
                semantic_tiebreak_sha256: sha('1'),
            },
            concat!(
                "7fffffffffffffff",
                "01",
                "00",
                "620000",
                "0000000000000002",
                "01",
                "01",
                "0000000000000002",
                "11111111111111111111111111111111",
                "11111111111111111111111111111111",
            ),
        ),
        (
            "零时间和 Binance 深度更新范围身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 0,
                source_kind: CanonicalSourceKind::BinanceDepthUpdate,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "\0".to_owned(),
                    collector_admission_sequence: 3,
                },
                source_native_identity: Some(SourceNativeIdentityV1::BinanceDepthUpdate {
                    first_update_id: 3,
                    final_update_id: 4,
                }),
                semantic_tiebreak_sha256: sha('2'),
            },
            concat!(
                "8000000000000000",
                "02",
                "00",
                "00ff0000",
                "0000000000000003",
                "01",
                "02",
                "0000000000000003",
                "0000000000000004",
                "22222222222222222222222222222222",
                "22222222222222222222222222222222",
            ),
        ),
        (
            "正一时间和 Polymarket 语义证据身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: 1,
                source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "z".to_owned(),
                    collector_admission_sequence: 4,
                },
                source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: sha('4'),
                }),
                semantic_tiebreak_sha256: sha('5'),
            },
            concat!(
                "8000000000000001",
                "03",
                "00",
                "7a0000",
                "0000000000000004",
                "01",
                "03",
                "44444444444444444444444444444444",
                "44444444444444444444444444444444",
                "55555555555555555555555555555555",
                "55555555555555555555555555555555",
            ),
        ),
        (
            "最大时间和缺少源原生身份",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: i64::MAX,
                source_kind: CanonicalSourceKind::PolymarketOrderbook,
                ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                    source_sequence_epoch_id: "x".to_owned(),
                    collector_admission_sequence: 5,
                },
                source_native_identity: None,
                semantic_tiebreak_sha256: sha('6'),
            },
            concat!(
                "ffffffffffffffff",
                "04",
                "00",
                "780000",
                "0000000000000005",
                "00",
                "66666666666666666666666666666666",
                "66666666666666666666666666666666",
            ),
        ),
        (
            "RTDS 接收时间坐标和较低顺序证据",
            TotalOrderKeyV1 {
                version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
                available_at_utc_ms: -2,
                source_kind: CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
                ordering_coordinate: OrderingCoordinateV1::ReceivedOnly {
                    received_at_utc_ms: i64::MIN,
                    stable_evidence_key_sha256: sha('7'),
                },
                source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: sha('8'),
                }),
                semantic_tiebreak_sha256: sha('9'),
            },
            concat!(
                "7ffffffffffffffe",
                "05",
                "01",
                "0000000000000000",
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

    let mut unknown_version = admission_key(CanonicalSourceKind::PolymarketOrderbook);
    unknown_version.version = "total_order_key_v2".to_owned();
    assert!(total_order_key_bytes(&unknown_version).is_err());
}

#[test]
fn decimals_and_level_transition_invariants_fail_closed() {
    for value in [
        "+1",
        "01",
        "1.0",
        "-0",
        "1e-3",
        ".1",
        "1.",
        "79228162514264337593543950336",
        "-79228162514264337593543950336",
    ] {
        assert!(parse_canonical_decimal(&ExactDecimal(value.to_owned()), "fixture").is_err());
    }
    for value in ["0", "1", "-1", "0.001", "123.45"] {
        parse_canonical_decimal(&ExactDecimal(value.to_owned()), "fixture").unwrap();
    }

    let payload = all_event_payloads().remove(5);
    let ExecutionMarketEventPayloadV1::LevelExecutionTransition(mut level) = payload else {
        unreachable!();
    };
    level.net_reduced_quantity_shares = ExactDecimal("2".to_owned());
    assert!(
        validate_execution_market_event(&event_with_payload(
            ExecutionMarketEventPayloadV1::LevelExecutionTransition(level.clone())
        ))
        .is_err()
    );

    level.net_reduced_quantity_shares = ExactDecimal("3".to_owned());
    level.queue_consumption_quantity_shares = ExactDecimal("4".to_owned());
    assert!(
        validate_execution_market_event(&event_with_payload(
            ExecutionMarketEventPayloadV1::LevelExecutionTransition(level.clone())
        ))
        .is_err()
    );

    level.queue_consumption_quantity_shares = ExactDecimal("3".to_owned());
    level.source_event_ids.push(event_id('a'));
    assert!(
        validate_execution_market_event(&event_with_payload(
            ExecutionMarketEventPayloadV1::LevelExecutionTransition(level)
        ))
        .is_err()
    );

    let payload = all_event_payloads().remove(6);
    let ExecutionMarketEventPayloadV1::LevelExecutionTransition(mut inferred) = payload else {
        unreachable!();
    };
    inferred.source_attribution_quality = SourceAttributionQuality::Exact;
    assert!(
        validate_execution_market_event(&event_with_payload(
            ExecutionMarketEventPayloadV1::LevelExecutionTransition(inferred)
        ))
        .is_err()
    );
}

#[test]
fn terminal_payout_and_watermark_invariants_fail_closed() {
    let mut incomplete_payout = all_event_payloads().pop().unwrap();
    let ExecutionMarketEventPayloadV1::MarketResolutionTerminal(
        MarketResolutionTerminalV1::Resolved { payout_vector, .. },
    ) = &mut incomplete_payout
    else {
        unreachable!();
    };
    payout_vector.pop();
    assert!(validate_execution_market_event(&event_with_payload(incomplete_payout)).is_err());

    let mut non_unit_winner = all_event_payloads().pop().unwrap();
    let ExecutionMarketEventPayloadV1::MarketResolutionTerminal(
        MarketResolutionTerminalV1::Resolved { payout_vector, .. },
    ) = &mut non_unit_winner
    else {
        unreachable!();
    };
    payout_vector[0].payout_pusd_per_share = ExactDecimal("0.5".to_owned());
    payout_vector[1].payout_pusd_per_share = ExactDecimal("0.5".to_owned());
    assert!(validate_execution_market_event(&event_with_payload(non_unit_winner)).is_err());

    let mut future_watermark = all_event_payloads().pop().unwrap();
    let ExecutionMarketEventPayloadV1::MarketResolutionTerminal(
        MarketResolutionTerminalV1::Resolved {
            terminal_watermark, ..
        },
    ) = &mut future_watermark
    else {
        unreachable!();
    };
    terminal_watermark.last_total_order_key.available_at_utc_ms = 1_201;
    assert!(validate_execution_market_event(&event_with_payload(future_watermark)).is_err());

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
        ExecutionMarketEventPayloadV1::MarketResolutionTerminal(quarantine.clone()),
    ))
    .unwrap();

    let mut invalid_quarantine = quarantine;
    let MarketResolutionTerminalV1::WaitingExhaustedQuarantine { reason_code, .. } =
        &mut invalid_quarantine
    else {
        unreachable!();
    };
    reason_code.clear();
    assert!(
        validate_execution_market_event(&event_with_payload(
            ExecutionMarketEventPayloadV1::MarketResolutionTerminal(invalid_quarantine)
        ))
        .is_err()
    );
}

#[test]
fn identity_encodings_reject_noncanonical_hex_token_and_digest() {
    let mut invalid_condition = base_event();
    invalid_condition.market.condition_id = ConditionId("0xA1".to_owned());
    seal_event(&mut invalid_condition);
    assert!(validate_execution_market_event(&invalid_condition).is_err());

    let mut truncated_condition = base_event();
    truncated_condition.market.condition_id = ConditionId("0xaa".to_owned());
    seal_event(&mut truncated_condition);
    assert!(validate_execution_market_event(&truncated_condition).is_err());

    let mut invalid_clob_market = base_event();
    invalid_clob_market.market.clob_market_id = ClobMarketId("03195576".to_owned());
    seal_event(&mut invalid_clob_market);
    assert!(validate_execution_market_event(&invalid_clob_market).is_err());

    let mut invalid_token = base_event();
    let ExecutionMarketEventPayloadV1::TickSizeChange(payload) = &mut invalid_token.payload else {
        unreachable!();
    };
    payload.asset.token_id = TokenId("0123".to_owned());
    seal_event(&mut invalid_token);
    assert!(validate_execution_market_event(&invalid_token).is_err());

    let mut invalid_digest = base_event();
    invalid_digest.lineage.source_evidence[0].input_file_sha256 = Sha256Hex("A".repeat(64));
    assert!(validate_execution_market_event(&invalid_digest).is_err());

    let mut mismatched_native_identity = base_event();
    mismatched_native_identity
        .total_order_key
        .source_native_identity = Some(SourceNativeIdentityV1::BinanceAggregateTrade {
        aggregate_trade_id: 7,
    });
    mismatched_native_identity.lineage.source_evidence[0].source_native_identity =
        mismatched_native_identity
            .total_order_key
            .source_native_identity
            .clone();
    seal_event(&mut mismatched_native_identity);
    assert!(validate_execution_market_event(&mismatched_native_identity).is_err());
}

#[test]
fn observation_encoding_matches_golden_and_causal_rules_fail_closed() {
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

    let mut future = observation.clone();
    future.component_states[2].as_of_available_at_utc_ms = Some(1_251);
    future.component_states[2].information_age_ms = Some(0);
    seal_observation(&mut future);
    assert!(validate_market_observation_causal_shell(&future).is_err());

    let mut after_last_finalized = observation.clone();
    after_last_finalized.component_states[2].as_of_available_at_utc_ms = Some(1_240);
    after_last_finalized.component_states[2].information_age_ms = Some(10);
    seal_observation(&mut after_last_finalized);
    assert!(validate_market_observation_causal_shell(&after_last_finalized).is_err());

    let mut mask_mismatch = observation.clone();
    mask_mismatch
        .missing_mask
        .insert(2, ObservationComponentId::CurrentTick);
    seal_observation(&mut mask_mismatch);
    assert!(validate_market_observation_causal_shell(&mask_mismatch).is_err());

    let mut hidden_future = observation.clone();
    hidden_future.component_states[1].as_of_available_at_utc_ms = Some(9_999);
    hidden_future.component_states[1].information_age_ms = Some(0);
    hidden_future.component_states[1].last_visible_event_id = Some(event_id('b'));
    hidden_future.component_states[1].state_digest_sha256 = Some(sha('b'));
    seal_observation(&mut hidden_future);
    assert!(validate_market_observation_causal_shell(&hidden_future).is_err());

    let mut outside_lineage = observation.clone();
    outside_lineage.component_states[2].last_visible_event_id = Some(event_id('b'));
    seal_observation(&mut outside_lineage);
    assert!(validate_market_observation_causal_shell(&outside_lineage).is_err());

    let mut cutoff_before_event = observation.clone();
    cutoff_before_event.causal_cutoff.available_at_utc_ms = 1_199;
    seal_observation(&mut cutoff_before_event);
    assert!(validate_market_observation_causal_shell(&cutoff_before_event).is_err());

    let mut age_overflow = observation;
    age_overflow.causal_cutoff.available_at_utc_ms = i64::MAX;
    age_overflow.component_states[2].as_of_available_at_utc_ms = Some(i64::MIN);
    age_overflow.component_states[2].information_age_ms = Some(u64::MAX);
    seal_observation(&mut age_overflow);
    assert!(validate_market_observation_causal_shell(&age_overflow).is_err());
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
fn six_quality_states_remain_distinct() {
    let encoded = [
        MarketDataQuality::Fresh,
        MarketDataQuality::Stale,
        MarketDataQuality::Missing,
        MarketDataQuality::UnknownNotObserved,
        MarketDataQuality::ConfirmedGap,
        MarketDataQuality::Invalid,
    ]
    .map(|quality| serde_json::to_string(&quality).unwrap());
    assert_eq!(
        encoded
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        6
    );
}

#[test]
fn gate_a_receipt_encoding_matches_golden_and_states_cannot_masquerade() {
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

    let mut training_ready = ready.clone();
    training_ready.training_dataset_ready = true;
    training_ready.receipt_id = derive_gate_a_receipt_id(&training_ready).unwrap();
    assert!(validate_gate_a_receipt(&training_ready, &pass).is_err());

    let mut split = ready.clone();
    split.training_split_manifest_id = "split-1".to_owned();
    split.receipt_id = derive_gate_a_receipt_id(&split).unwrap();
    assert!(validate_gate_a_receipt(&split, &pass).is_err());

    let mut empty_dates = receipt_for(&pass);
    empty_dates.covered_utc_dates.clear();
    empty_dates.receipt_id = derive_gate_a_receipt_id(&empty_dates).unwrap();
    assert!(validate_gate_a_receipt(&empty_dates, &pass).is_err());

    for invalid_dates in [
        vec!["2026-08-02", "2026-07-30"],
        vec!["2026-07-30", "2026-07-30"],
        vec!["2026-07-29"],
    ] {
        let mut invalid_dates_receipt = receipt_for(&pass);
        invalid_dates_receipt.covered_utc_dates =
            invalid_dates.into_iter().map(str::to_owned).collect();
        invalid_dates_receipt.receipt_id =
            derive_gate_a_receipt_id(&invalid_dates_receipt).unwrap();
        assert!(validate_gate_a_receipt(&invalid_dates_receipt, &pass).is_err());
    }

    let mut stale_content_id = receipt_for(&pass);
    stale_content_id.run_manifest_sha256 = sha('a');
    let error = validate_gate_a_receipt(&stale_content_id, &pass).unwrap_err();
    assert!(matches!(
        error,
        ContractError::InvalidField { field, .. } if field == "receipt_id"
    ));

    let mut changed_matrix = pass.clone();
    changed_matrix.entries[0].evidence_ids = vec!["replacement-evidence".to_owned()];
    assert!(validate_gate_a_receipt(&ready, &changed_matrix).is_err());
}

#[test]
fn gate_a_receipt_rejects_all_fail_capability_matrix() {
    let fail = matrix_with(|_| CapabilityStatus::Fail);
    let receipt = receipt_for(&fail);
    assert!(validate_gate_a_receipt(&receipt, &fail).is_err());
}

#[test]
fn gate_a_receipt_rejects_mixed_fail_capability_matrix() {
    let mixed_failure = matrix_with(|index| {
        if index == 0 {
            CapabilityStatus::Pass
        } else {
            CapabilityStatus::Fail
        }
    });
    let receipt = receipt_for(&mixed_failure);
    assert!(validate_gate_a_receipt(&receipt, &mixed_failure).is_err());
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
    assert!(matches!(
        error,
        ContractError::Json(message) if message.contains("unknown field `tape_assessment`")
    ));
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
    assert!(matches!(
        error,
        ContractError::Json(message)
            if message.contains("unknown field `published_artifact_kind`")
    ));
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
fn capability_matrix_rejects_whitespace_only_contract_field_path() {
    let mut matrix = matrix_with(|_| CapabilityStatus::Pass);
    matrix.entries[0].contract_field_paths = vec![" \t ".to_owned()];

    let error = validate_gate_a_capability_matrix(&matrix).unwrap_err();
    assert!(matches!(
        error,
        ContractError::InvalidField { field, .. }
            if field == "entries[0].contract_field_paths"
    ));
}

#[test]
fn pass_capability_rejects_whitespace_only_evidence_id() {
    let mut matrix = matrix_with(|_| CapabilityStatus::Pass);
    matrix.entries[0].evidence_ids = vec![" \t ".to_owned()];

    let error = validate_gate_a_capability_matrix(&matrix).unwrap_err();
    assert!(matches!(
        error,
        ContractError::InvalidField { field, .. } if field == "entries[0].evidence_ids"
    ));
}

#[test]
fn fail_capability_rejects_whitespace_only_blocking_reason() {
    let mut matrix = matrix_with(|_| CapabilityStatus::Fail);
    matrix.entries[0].blocking_reason = Some(" \t ".to_owned());

    let error = validate_gate_a_capability_matrix(&matrix).unwrap_err();
    assert!(matches!(
        error,
        ContractError::InvalidField { field, .. }
            if field == "entries[0].blocking_reason"
    ));
}

#[test]
fn capability_matrix_requires_the_complete_static_set() {
    let mut matrix = matrix_with(|_| CapabilityStatus::Pass);
    validate_gate_a_capability_matrix(&matrix).unwrap();
    matrix.entries.pop();
    assert!(validate_gate_a_capability_matrix(&matrix).is_err());

    let mut matrix = matrix_with(|_| CapabilityStatus::Pass);
    matrix.entries.swap(0, 1);
    assert!(validate_gate_a_capability_matrix(&matrix).is_err());

    let mut pass_with_blocking_reason = matrix_with(|_| CapabilityStatus::Pass);
    pass_with_blocking_reason.entries[0].blocking_reason = Some("cannot-pass".to_owned());
    assert!(validate_gate_a_capability_matrix(&pass_with_blocking_reason).is_err());

    let mut pass_without_evidence = matrix_with(|_| CapabilityStatus::Pass);
    pass_without_evidence.entries[0].evidence_ids.clear();
    assert!(validate_gate_a_capability_matrix(&pass_without_evidence).is_err());

    let mut fail_without_reason = matrix_with(|_| CapabilityStatus::Fail);
    fail_without_reason.entries[0].blocking_reason = None;
    assert!(validate_gate_a_capability_matrix(&fail_without_reason).is_err());

    let mut fail_with_empty_reason = matrix_with(|_| CapabilityStatus::Fail);
    fail_with_empty_reason.entries[0].blocking_reason = Some(String::new());
    assert!(validate_gate_a_capability_matrix(&fail_with_empty_reason).is_err());
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
