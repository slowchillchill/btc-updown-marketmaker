use contracts::*;
use data_pipeline_core::*;
use serde_json::{Value, json};

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
}

fn event_id(character: char) -> CanonicalEventId {
    CanonicalEventId(character.to_string().repeat(64))
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

fn admission() -> CollectorAdmissionCoordinateV1 {
    CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: "epoch-1".to_owned(),
        collector_admission_sequence: 7,
        admitted_at_utc_ms: 1_150,
    }
}

fn source_evidence(source_kind: CanonicalSourceKind) -> SourceEvidenceRefV2 {
    let is_rtds = source_kind == CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd;
    SourceEvidenceRefV2 {
        source_kind,
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: if is_rtds { sha('9') } else { sha('7') },
        }),
        source_event_at_utc_ms: Some(if is_rtds { 900 } else { 1_000 }),
        received_at_utc_ms: 1_100,
        collector_admission: (!is_rtds).then(admission),
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
    }
}

fn base_event() -> ExecutionMarketEventV2 {
    let source_kind = CanonicalSourceKind::PolymarketOrderbook;
    let mut event = ExecutionMarketEventV2 {
        schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V2.to_owned(),
        event_id: event_id('0'),
        market: market(),
        time: EventTimeV2 {
            source_event_at_utc_ms: Some(1_000),
            received_at_utc_ms: 1_100,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "canonical-rule-v2".to_owned(),
            source_evidence: vec![source_evidence(source_kind)],
        },
        quality: EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
        payload: ExecutionMarketEventPayloadV2::TickSizeChange(TickSizeChangeV1 {
            asset: asset("123", 0, "UP"),
            old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
            source_event_ids: vec![event_id('a')],
        }),
    };
    seal_event(&mut event);
    event
}

fn seal_event(event: &mut ExecutionMarketEventV2) {
    event.event_id = event_id('0');
    event.event_id = derive_canonical_event_id(event).unwrap();
}

fn event_with_payload(payload: ExecutionMarketEventPayloadV2) -> ExecutionMarketEventV2 {
    let mut event = base_event();
    event.payload = payload;
    seal_event(&mut event);
    event
}

fn rtds_event() -> ExecutionMarketEventV2 {
    let mut event = base_event();
    event.time = EventTimeV2 {
        source_event_at_utc_ms: Some(900),
        received_at_utc_ms: 1_100,
    };
    event.lineage.source_evidence = vec![source_evidence(
        CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
    )];
    seal_event(&mut event);
    event
}

fn all_event_payloads() -> Vec<ExecutionMarketEventPayloadV2> {
    let market = market();
    let up = asset("123", 0, "UP");
    let down = asset("456", 1, "DOWN");
    vec![
        ExecutionMarketEventPayloadV2::MarketLifecycleTransition(MarketLifecycleTransitionV1 {
            previous_state: None,
            current_state: MappedMarketLifecycleStateV1::UnknownUnmodeled {
                raw_value: "future_state".to_owned(),
            },
            assets: vec![up.clone(), down.clone()],
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV2::InitialL2Snapshot(InitialL2SnapshotV1 {
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
        ExecutionMarketEventPayloadV2::BestBidAsk(BestBidAskV1 {
            asset: up.clone(),
            best_bid_pusd_per_share: Some(ExactDecimal("0.4".to_owned())),
            best_ask_pusd_per_share: Some(ExactDecimal("0.5".to_owned())),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV2::TickSizeChange(TickSizeChangeV1 {
            asset: up.clone(),
            old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV2::PublicTrade(PublicTradeV1 {
            asset: up.clone(),
            trade_id: "trade-1".to_owned(),
            trade_id_source: PublicTradeIdSourceV1::DerivedSourceEvidence,
            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
            quantity_shares: ExactDecimal("2.5".to_owned()),
            aggressor_side: TradeSide::Buy,
            fee_rate_bps: Some(ExactDecimal("15".to_owned())),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV2::LevelExecutionTransition(LevelExecutionTransitionV2 {
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
            platform_effective_at_utc_ms: 1_000,
            visible_at_utc_ms: 1_100,
            source_attribution_quality: SourceAttributionQuality::Exact,
            queue_result_quality: QueueResultQuality::Exact,
        }),
        ExecutionMarketEventPayloadV2::LevelExecutionTransition(LevelExecutionTransitionV2 {
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
            platform_effective_at_utc_ms: 1_000,
            visible_at_utc_ms: 1_100,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        ExecutionMarketEventPayloadV2::QualityBoundary(QualityBoundaryV1 {
            scope: QualityScopeV1::Asset(up.clone()),
            previous_quality: MarketDataQuality::Fresh,
            current_quality: MarketDataQuality::ConfirmedGap,
            reason_code: "confirmed_sequence_gap".to_owned(),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV2::MarketTradingState(MarketTradingStateV1 {
            active: Some(true),
            closed: Some(false),
            accepting_orders: Some(true),
            source_event_ids: vec![event_id('a')],
        }),
        ExecutionMarketEventPayloadV2::MarketResolutionTerminal(
            MarketResolutionTerminalV2::Resolved {
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
                terminal_watermark: TerminalWatermarkV2 {
                    last_source_event_at_utc_ms: Some(1_000),
                    last_received_at_utc_ms: 1_100,
                    canonical_event_root_sha256: sha('6'),
                    lineage_identity_sha256: sha('5'),
                },
            },
        ),
    ]
}

fn base_observation(event: &ExecutionMarketEventV2) -> MarketObservationCausalShellV2 {
    let missing_components = REQUIRED_OBSERVATION_COMPONENTS
        .iter()
        .copied()
        .filter(|component| *component != ObservationComponentId::CurrentTick)
        .collect();
    let mut components = REQUIRED_OBSERVATION_COMPONENTS
        .iter()
        .copied()
        .map(|component| ObservationComponentStateV2 {
            component,
            quality: MarketDataQuality::Missing,
            source_event_at_utc_ms: None,
            received_at_utc_ms: None,
            information_age_ms: None,
            observed_transport_delay_ms: None,
            last_visible_event_id: None,
            state_digest_sha256: None,
        })
        .collect::<Vec<_>>();
    components[2] = ObservationComponentStateV2 {
        component: ObservationComponentId::CurrentTick,
        quality: MarketDataQuality::Fresh,
        source_event_at_utc_ms: Some(1_000),
        received_at_utc_ms: Some(1_100),
        information_age_ms: Some(250),
        observed_transport_delay_ms: Some(100),
        last_visible_event_id: Some(event.event_id.clone()),
        state_digest_sha256: Some(sha('a')),
    };
    let mut observation = MarketObservationCausalShellV2 {
        schema_version: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V2.to_owned(),
        observation_id: sha('0'),
        market: event.market.clone(),
        lineage: ObservationLineageV2 {
            execution_event_contract_version: EXECUTION_MARKET_EVENT_SCHEMA_V2.to_owned(),
            input_set_identity_sha256: sha('f'),
            visibility_index_manifest_sha256: sha('d'),
            contributing_event_ids: vec![event.event_id.clone()],
        },
        visibility_cutoff: ObservationVisibilityCutoffV2 {
            received_at_utc_ms: 1_250,
        },
        market_state_digest_sha256: sha('e'),
        component_states: components,
        missing_mask: missing_components,
        canonical_hash_sha256: sha('0'),
    };
    seal_observation(&mut observation);
    observation
}

fn seal_observation(observation: &mut MarketObservationCausalShellV2) {
    observation.observation_id = sha('0');
    observation.canonical_hash_sha256 = sha('0');
    observation.observation_id = derive_observation_id(observation).unwrap();
    observation.canonical_hash_sha256 = derive_observation_hash(observation).unwrap();
}

fn matrix_with(statuses: impl Fn(usize) -> CapabilityStatus) -> GateACapabilityMatrixV2 {
    GateACapabilityMatrixV2 {
        schema_version: GATE_A_CAPABILITY_MATRIX_SCHEMA_V2.to_owned(),
        entries: REQUIRED_GATE_A_CAPABILITIES
            .iter()
            .copied()
            .enumerate()
            .map(|(index, capability)| {
                let status = statuses(index);
                GateACapabilityEntryV2 {
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

fn receipt_for(matrix: &GateACapabilityMatrixV2) -> GateAReceiptV2 {
    let mut receipt = GateAReceiptV2 {
        schema_version: GATE_A_RECEIPT_SCHEMA_V2.to_owned(),
        receipt_id: sha('0'),
        requirement_id: "DATA-PIPELINE-TRAINING-20260803".to_owned(),
        pipeline_core_identity_sha256: sha('1'),
        execution_event_contract_version: EXECUTION_MARKET_EVENT_SCHEMA_V2.to_owned(),
        observation_contract_version: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V2.to_owned(),
        event_time_index_contract_version: EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
        visibility_index_contract_version: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
        dataset_lineage_root: DATASET_LINEAGE_ROOT.to_owned(),
        dataset_date_floor: DATASET_DATE_FLOOR.to_owned(),
        covered_utc_dates: vec!["2026-07-30".to_owned(), "2026-08-02".to_owned()],
        input_set_identity_sha256: sha('2'),
        supported_schema_set_identity_sha256: sha('3'),
        input_manifest_set_sha256: sha('4'),
        canonical_event_store_manifest_sha256: sha('5'),
        event_time_index_manifest_sha256: sha('6'),
        visibility_index_manifest_sha256: sha('7'),
        observation_manifest_sha256: sha('8'),
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
fn stable_event_encoding_matches_v2_golden_and_roundtrips() {
    let event = base_event();
    let encoded = encode_execution_market_event(&event).unwrap();
    let actual = String::from_utf8(encoded.clone()).unwrap();
    let expected = include_str!("fixtures/execution_market_event_v2.json").trim_end();
    if actual != expected {
        eprintln!("ACTUAL_EVENT={actual}");
    }
    assert_eq!(actual, expected);
    assert_eq!(decode_execution_market_event(&encoded).unwrap(), event);
}

#[test]
fn event_projects_one_identity_to_both_time_indexes() {
    let event = base_event();
    let event_time = project_event_time_index_entry(&event)
        .unwrap()
        .expect("Polymarket orderbook event has source event time");
    let visibility = project_visibility_index_entry(&event).unwrap();

    assert_eq!(event_time.event_id, event.event_id);
    assert_eq!(visibility.event_id, event.event_id);
    assert_eq!(event_time.source_event_at_utc_ms, 1_000);
    assert_eq!(visibility.received_at_utc_ms, 1_100);
    assert_eq!(
        decode_event_time_index_entry(&encode_event_time_index_entry(&event_time).unwrap())
            .unwrap(),
        event_time
    );
    assert_eq!(
        decode_visibility_index_entry(&encode_visibility_index_entry(&visibility).unwrap())
            .unwrap(),
        visibility
    );
}

#[test]
fn every_frozen_event_payload_validates() {
    for payload in all_event_payloads() {
        validate_execution_market_event(&event_with_payload(payload)).unwrap();
    }
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
    let mut decoded: ExecutionMarketEventV2 = serde_json::from_value(value).unwrap();
    seal_event(&mut decoded);
    validate_execution_market_event(&decoded).unwrap();
}

#[test]
fn nonpositive_tick_is_rejected_at_decode_boundary() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["payload"]["event"]["new_tick_size_pusd_per_share"] = json!("0");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.new_tick_size_pusd_per_share"
    ));
}

#[test]
fn market_trading_state_roundtrips_independent_flags() {
    let payload = ExecutionMarketEventPayloadV2::MarketTradingState(MarketTradingStateV1 {
        active: Some(true),
        closed: Some(false),
        accepting_orders: Some(false),
        source_event_ids: vec![event_id('a')],
    });
    let first = event_with_payload(payload);
    let mut value = serde_json::to_value(&first).unwrap();
    value["payload"]["event"]["accepting_orders"] = json!(true);
    let mut second: ExecutionMarketEventV2 = serde_json::from_value(value).unwrap();
    seal_event(&mut second);
    validate_execution_market_event(&second).unwrap();
    assert_ne!(first.event_id, second.event_id);
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
    let mut value = serde_json::to_value(&derived).unwrap();
    value["payload"]["event"]["trade_id_source"] = json!("PLATFORM_TRANSACTION_HASH");
    let mut platform: ExecutionMarketEventV2 = serde_json::from_value(value).unwrap();
    seal_event(&mut platform);
    validate_execution_market_event(&platform).unwrap();
    assert_ne!(derived.event_id, platform.event_id);
}

#[test]
fn explicit_unknown_lifecycle_code_roundtrips() {
    let lifecycle = event_with_payload(all_event_payloads().remove(0));
    let bytes = encode_execution_market_event(&lifecycle).unwrap();
    assert_eq!(decode_execution_market_event(&bytes).unwrap(), lifecycle);
}

#[test]
fn event_json_boundary_rejects_v1_schema() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["schema_version"] = json!("execution_market_event_v1");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::UnsupportedSchema { contract, actual })
            if contract == "execution market event" && actual == "execution_market_event_v1"
    ));
}

#[test]
fn event_json_boundary_rejects_unknown_quality_enum() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["quality"]["availability"] = json!("future_quality");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::Json(_))
    ));
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
    validate_execution_market_event(&resharded).unwrap();

    let mut changed = event.clone();
    let ExecutionMarketEventPayloadV2::TickSizeChange(payload) = &mut changed.payload else {
        unreachable!();
    };
    payload.new_tick_size_pusd_per_share = ExactDecimal("0.002".to_owned());
    assert_ne!(
        derive_canonical_event_id(&event).unwrap(),
        derive_canonical_event_id(&changed).unwrap()
    );
}

#[test]
fn collector_admission_rejects_receive_time_after_admission() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["time"]["received_at_utc_ms"] = json!(1_160);
    value["lineage"]["source_evidence"][0]["received_at_utc_ms"] = json!(1_160);
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "lineage.source_evidence[0].received_at_utc_ms"
    ));
}

#[test]
fn polymarket_orderbook_requires_source_event_time() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["time"]["source_event_at_utc_ms"] = Value::Null;
    value["lineage"]["source_evidence"][0]["source_event_at_utc_ms"] = Value::Null;
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "lineage.source_evidence[0].source_event_at_utc_ms"
    ));
}

#[test]
fn rtds_event_without_collector_admission_remains_valid() {
    validate_execution_market_event(&rtds_event()).unwrap();
}

#[test]
fn lifecycle_rejects_duplicate_outcome_index_for_distinct_tokens() {
    let payload =
        ExecutionMarketEventPayloadV2::MarketLifecycleTransition(MarketLifecycleTransitionV1 {
            previous_state: None,
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            },
            assets: vec![asset("123", 0, "UP"), asset("456", 1, "DOWN")],
            source_event_ids: vec![event_id('a')],
        });
    let mut value = serde_json::to_value(event_with_payload(payload)).unwrap();
    value["payload"]["event"]["assets"][1]["outcome"]["index"] = json!(0);
    assert!(decode_execution_market_event(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn event_json_boundary_rejects_noncanonical_decimal_encoding() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["payload"]["event"]["new_tick_size_pusd_per_share"] = json!("1e-3");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.new_tick_size_pusd_per_share"
    ));
}

#[test]
fn level_transition_rejects_quantity_nonconservation() {
    let transition = event_with_payload(all_event_payloads().remove(5));
    let mut value = serde_json::to_value(transition).unwrap();
    value["payload"]["event"]["net_reduced_quantity_shares"] = json!("2");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "payload.event"
    ));
}

#[test]
fn derived_transition_uses_latest_source_and_receive_times_independently() {
    let mut event = event_with_payload(all_event_payloads().remove(5));
    let mut first = source_evidence(CanonicalSourceKind::PolymarketOrderbook);
    first.source_native_identity =
        Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256: sha('6') });
    first.source_event_at_utc_ms = Some(1_000);
    first.received_at_utc_ms = 1_300;
    first.collector_admission = None;
    let mut second = source_evidence(CanonicalSourceKind::PolymarketOrderbook);
    second.source_native_identity =
        Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256: sha('7') });
    second.source_event_at_utc_ms = Some(1_100);
    second.received_at_utc_ms = 1_200;
    second.collector_admission = None;
    event.lineage.source_evidence = vec![first, second];
    event.time = EventTimeV2 {
        source_event_at_utc_ms: Some(1_100),
        received_at_utc_ms: 1_300,
    };
    let ExecutionMarketEventPayloadV2::LevelExecutionTransition(transition) = &mut event.payload
    else {
        unreachable!();
    };
    transition.platform_effective_at_utc_ms = 1_100;
    transition.visible_at_utc_ms = 1_300;
    seal_event(&mut event);

    validate_execution_market_event(&event).unwrap();
}

#[test]
fn inferred_level_transition_rejects_false_exact_attribution() {
    let transition = event_with_payload(all_event_payloads().remove(6));
    let mut value = serde_json::to_value(transition).unwrap();
    value["payload"]["event"]["source_attribution_quality"] = json!("EXACT");
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.source_attribution_quality"
    ));
}

#[test]
fn terminal_rejects_future_source_watermark() {
    let resolved = event_with_payload(all_event_payloads().pop().unwrap());
    let mut value = serde_json::to_value(resolved).unwrap();
    value["payload"]["event"]["terminal_watermark"]["last_source_event_at_utc_ms"] = json!(1_001);
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.terminal_watermark.last_source_event_at_utc_ms"
    ));
}

#[test]
fn waiting_exhausted_quarantine_is_a_legal_terminal() {
    let quarantine = MarketResolutionTerminalV2::WaitingExhaustedQuarantine {
        reason_code: "resolution_wait_exhausted".to_owned(),
        source_event_ids: vec![event_id('a')],
        terminal_watermark: TerminalWatermarkV2 {
            last_source_event_at_utc_ms: Some(1_000),
            last_received_at_utc_ms: 1_100,
            canonical_event_root_sha256: sha('6'),
            lineage_identity_sha256: sha('5'),
        },
    };
    validate_execution_market_event(&event_with_payload(
        ExecutionMarketEventPayloadV2::MarketResolutionTerminal(quarantine),
    ))
    .unwrap();
}

#[test]
fn event_rejects_source_kind_and_native_identity_conflict() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["lineage"]["source_evidence"][0]["source_native_identity"] = json!({
        "kind": "BINANCE_AGGREGATE_TRADE",
        "aggregate_trade_id": 7
    });
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "lineage.source_evidence[0].source_native_identity"
    ));
}

#[test]
fn observation_encoding_matches_v2_golden_and_roundtrips() {
    let observation = base_observation(&base_event());
    let encoded = encode_market_observation_causal_shell(&observation).unwrap();
    let actual = String::from_utf8(encoded.clone()).unwrap();
    let expected = include_str!("fixtures/market_observation_causal_shell_v2.json").trim_end();
    if actual != expected {
        eprintln!("ACTUAL_OBSERVATION={actual}");
    }
    assert_eq!(actual, expected);
    assert_eq!(
        decode_market_observation_causal_shell(&encoded).unwrap(),
        observation
    );
}

#[test]
fn observation_rejects_component_received_after_visibility_cutoff() {
    let mut value = serde_json::to_value(base_observation(&base_event())).unwrap();
    value["component_states"][2]["received_at_utc_ms"] = json!(1_300);
    assert!(matches!(
        decode_market_observation_causal_shell(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "component_states[2].received_at_utc_ms"
    ));
}

#[test]
fn observation_visibility_relation_rejects_future_contributing_event() {
    let event = base_event();
    let observation = base_observation(&event);
    let visible_entry = project_visibility_index_entry(&event).unwrap();
    validate_market_observation_visibility_index_entries(
        &observation,
        std::slice::from_ref(&visible_entry),
    )
    .unwrap();

    let future_event_id = event_id('f');
    let mut with_future_contribution = observation;
    with_future_contribution
        .lineage
        .contributing_event_ids
        .push(future_event_id.clone());
    with_future_contribution
        .lineage
        .contributing_event_ids
        .sort_by(|left, right| left.0.cmp(&right.0));
    seal_observation(&mut with_future_contribution);
    validate_market_observation_causal_shell(&with_future_contribution).unwrap();

    let future_entry = VisibilityIndexEntryV2 {
        schema_version: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
        event_id: future_event_id,
        received_at_utc_ms: 1_300,
    };
    assert!(matches!(
        validate_market_observation_visibility_index_entries(
            &with_future_contribution,
            &[visible_entry, future_entry]
        ),
        Err(ContractError::InvalidField { field, .. })
            if field == "visibility_index_entries[1].received_at_utc_ms"
    ));
}

#[test]
fn observation_allows_unavailable_cross_clock_diagnostics() {
    let mut observation = base_observation(&base_event());
    let component = &mut observation.component_states[2];
    component.source_event_at_utc_ms = Some(1_300);
    component.information_age_ms = None;
    component.observed_transport_delay_ms = None;
    seal_observation(&mut observation);

    validate_market_observation_causal_shell(&observation).unwrap();
}

#[test]
fn observation_rejects_visible_event_outside_lineage() {
    let mut value = serde_json::to_value(base_observation(&base_event())).unwrap();
    value["component_states"][2]["last_visible_event_id"] = json!(event_id('b').0);
    assert!(matches!(
        decode_market_observation_causal_shell(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "component_states[2].last_visible_event_id"
    ));
}

#[test]
fn observation_rejects_visible_component_in_missing_mask() {
    let mut value = serde_json::to_value(base_observation(&base_event())).unwrap();
    value["missing_mask"]
        .as_array_mut()
        .unwrap()
        .insert(2, json!("current_tick"));
    assert!(decode_market_observation_causal_shell(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn gate_a_receipt_encoding_matches_v2_golden_and_roundtrips() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let receipt = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&receipt, &pass).unwrap();
    let actual = String::from_utf8(encoded.clone()).unwrap();
    let expected = include_str!("fixtures/real_execution_tape_receipt_v2.json").trim_end();
    if actual != expected {
        eprintln!("ACTUAL_RECEIPT={actual}");
    }
    assert_eq!(actual, expected);
    assert_eq!(decode_gate_a_receipt(&encoded, &pass).unwrap(), receipt);
}

#[test]
fn gate_a_receipt_binds_event_set_and_both_index_manifests() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let receipt = receipt_for(&pass);
    let encoded = encode_gate_a_receipt(&receipt, &pass).unwrap();

    for field in [
        "canonical_event_store_manifest_sha256",
        "event_time_index_manifest_sha256",
        "visibility_index_manifest_sha256",
    ] {
        let mut changed: Value = serde_json::from_slice(&encoded).unwrap();
        changed[field] = json!(sha('a').0);
        assert!(matches!(
            decode_gate_a_receipt(&serde_json::to_vec(&changed).unwrap(), &pass),
            Err(ContractError::InvalidField { field, .. }) if field == "receipt_id"
        ));
    }
}

#[test]
fn v1_gate_a_receipt_is_rejected_by_v2_decoder() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let legacy = include_bytes!("fixtures/legacy_real_execution_tape_receipt_v1.json");
    assert!(matches!(
        decode_gate_a_receipt(legacy, &pass),
        Err(ContractError::UnsupportedSchema { contract, actual })
            if contract == "Gate A receipt" && actual == "real_execution_tape_receipt_v1"
    ));
}

#[test]
fn gate_a_receipt_rejects_training_dataset_ready_flag() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let receipt = receipt_for(&pass);
    let mut value = serde_json::to_value(receipt).unwrap();
    value["training_dataset_ready"] = json!(true);
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&value).unwrap(), &pass),
        Err(ContractError::InvalidField { field, .. }) if field == "training_dataset_ready"
    ));
}

#[test]
fn gate_a_receipt_rejects_capability_matrix_digest_mismatch() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let encoded = encode_gate_a_receipt(&receipt_for(&pass), &pass).unwrap();
    let mut changed_value = serde_json::to_value(&pass).unwrap();
    changed_value["entries"][0]["evidence_ids"] = json!(["replacement-evidence"]);
    let changed =
        decode_gate_a_capability_matrix(&serde_json::to_vec(&changed_value).unwrap()).unwrap();
    assert!(matches!(
        decode_gate_a_receipt(&encoded, &changed),
        Err(ContractError::InvalidField { field, .. }) if field == "capability_matrix_sha256"
    ));
}

#[test]
fn capability_matrix_requires_complete_static_set() {
    let mut value = serde_json::to_value(matrix_with(|_| CapabilityStatus::Pass)).unwrap();
    value["entries"].as_array_mut().unwrap().pop();
    assert!(matches!(
        decode_gate_a_capability_matrix(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "entries"
    ));
}

#[test]
fn capability_entries_require_consistent_evidence_and_blocking_reason() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let mut without_evidence = serde_json::to_value(&pass).unwrap();
    without_evidence["entries"][0]["evidence_ids"] = json!([]);
    assert!(
        decode_gate_a_capability_matrix(&serde_json::to_vec(&without_evidence).unwrap()).is_err()
    );

    let mut with_reason = serde_json::to_value(pass).unwrap();
    with_reason["entries"][0]["blocking_reason"] = json!("cannot-pass");
    assert!(decode_gate_a_capability_matrix(&serde_json::to_vec(&with_reason).unwrap()).is_err());

    let failed = matrix_with(|_| CapabilityStatus::Fail);
    let mut without_reason = serde_json::to_value(failed).unwrap();
    without_reason["entries"][0]["blocking_reason"] = json!(" \t ");
    assert!(matches!(
        decode_gate_a_capability_matrix(&serde_json::to_vec(&without_reason).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "entries[0].blocking_reason"
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
    assert!(classify_dataset_date("2026-02-30").is_err());
}

#[test]
fn unknown_observation_field_is_rejected() {
    let mut value = serde_json::to_value(base_observation(&base_event())).unwrap();
    value["future_label"] = json!(1);
    assert!(decode_market_observation_causal_shell(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn event_rejects_noncanonical_condition_identity_encoding() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["market"]["condition_id"] = json!(format!("0x{}", "A".repeat(64)));
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. }) if field == "market.condition_id"
    ));
}

#[test]
fn event_rejects_noncanonical_source_digest_encoding() {
    let mut value = serde_json::to_value(base_event()).unwrap();
    value["lineage"]["source_evidence"][0]["input_file_sha256"] = json!("A".repeat(64));
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "lineage.source_evidence[0].input_file_sha256"
    ));
}

#[test]
fn level_transition_rejects_duplicate_source_event_ids() {
    let transition = event_with_payload(all_event_payloads().remove(5));
    let mut value = serde_json::to_value(transition).unwrap();
    value["payload"]["event"]["source_event_ids"]
        .as_array_mut()
        .unwrap()
        .push(json!(event_id('a').0));
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::DuplicateValue { field, .. })
            if field == "payload.event.source_event_ids"
    ));
}

#[test]
fn terminal_rejects_incomplete_binary_payout() {
    let resolved = event_with_payload(all_event_payloads().pop().unwrap());
    let mut value = serde_json::to_value(resolved).unwrap();
    value["payload"]["event"]["payout_vector"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(matches!(
        decode_execution_market_event(&serde_json::to_vec(&value).unwrap()),
        Err(ContractError::InvalidField { field, .. })
            if field == "payload.event.payout_vector"
    ));
}

#[test]
fn gate_a_receipt_rejects_assigned_training_split() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let receipt = receipt_for(&pass);
    let mut value = serde_json::to_value(receipt).unwrap();
    value["training_split_manifest_id"] = json!("split-1");
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&value).unwrap(), &pass),
        Err(ContractError::InvalidField { field, .. }) if field == "training_split_manifest"
    ));
}

#[test]
fn gate_a_receipt_rejects_unsorted_covered_dates() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let receipt = receipt_for(&pass);
    let mut value = serde_json::to_value(receipt).unwrap();
    value["covered_utc_dates"] = json!(["2026-08-02", "2026-07-30"]);
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&value).unwrap(), &pass),
        Err(ContractError::InvalidField { field, .. }) if field == "covered_utc_dates"
    ));
}

#[test]
fn gate_a_receipt_rejects_failed_capability_matrix() {
    let failed = matrix_with(|index| {
        if index == 0 {
            CapabilityStatus::Fail
        } else {
            CapabilityStatus::Pass
        }
    });
    let receipt = receipt_for(&failed);
    assert!(matches!(
        decode_gate_a_receipt(&serde_json::to_vec(&receipt).unwrap(), &failed),
        Err(ContractError::InvalidField { field, .. }) if field == "capability_matrix"
    ));
}

#[test]
fn capability_matrix_rejects_unknown_status() {
    let pass = matrix_with(|_| CapabilityStatus::Pass);
    let mut value = serde_json::to_value(pass).unwrap();
    value["entries"][0]["status"] = json!("NOT_EVALUATED");
    assert!(decode_gate_a_capability_matrix(&serde_json::to_vec(&value).unwrap()).is_err());
}
