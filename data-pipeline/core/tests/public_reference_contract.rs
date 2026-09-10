use contracts::ExactDecimal;
use data_pipeline_core::public_reference::{
    normalize_e18_price_decimal_v1, normalize_price_decimal_lexical_v1,
};

#[test]
fn new_price_boundary_preserves_source_digits_without_rounding() {
    for (raw, expected) in [
        ("64950.01421851144", "64950.01421851144"),
        ("6.495001421851144E+4", "64950.01421851144"),
        ("00064950.0142185114400", "64950.01421851144"),
        ("-0.000e999999999999999999999999", "0"),
        ("1e-28", "0.0000000000000000000000000001"),
    ] {
        assert_eq!(
            normalize_price_decimal_lexical_v1(raw, "price").unwrap(),
            ExactDecimal(expected.to_owned()),
        );
    }
    for raw in [
        "0.12345678901234567890123456789",
        "64950.0142185114400000000000001",
        "1e999999999999999999999999",
    ] {
        assert!(normalize_price_decimal_lexical_v1(raw, "price").is_err());
    }
    // 旧入口保留原合同；新增精确范围没有全局改变已发布数值行为。
    let legacy = data_pipeline_core::parse_canonical_decimal(
        &ExactDecimal("0.12345678901234567890123456789".to_owned()),
        "price",
    )
    .unwrap();
    assert_eq!(legacy.to_string(), "0.1234567890123456789012345679");
}

#[test]
fn e18_uses_the_original_integer_instead_of_a_display_price() {
    assert_eq!(
        normalize_e18_price_decimal_v1("64950014218511440012345", "full_accuracy_value").unwrap(),
        ExactDecimal("64950.014218511440012345".to_owned()),
    );
    assert_eq!(
        normalize_e18_price_decimal_v1("1", "full_accuracy_value").unwrap(),
        ExactDecimal("0.000000000000000001".to_owned()),
    );
    assert!(normalize_e18_price_decimal_v1("1.5", "full_accuracy_value").is_err());
}

fn market() -> contracts::MarketIdentityV2 {
    contracts::MarketIdentityV2 {
        condition_id: contracts::ConditionId("condition".to_owned()),
        clob_market_id: contracts::ClobMarketId("123".to_owned()),
        window: contracts::MarketWindowV2 {
            start_at_utc_ms: 1000,
            end_at_utc_ms: 301000,
        },
    }
}

fn anchor_input() -> data_pipeline_core::public_input::MarketAnchorRuleUpdateInput {
    serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":2000,
        "collector_admission_sequence":1, "collector_admission_time_utc_ms":2001,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"host", "collector_region":"hk",
        "slug":"test", "condition_id":"condition", "clob_market_id":"123", "asset_ids":"[\"1\",\"2\"]",
        "market_start_utc":1000,"market_end_utc":301000,"fixed_identity_verified_at_utc":900,
        "market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":1990,
        "anchor_query_source_epoch_id":"query-epoch","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"64950.01421851144","price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":2000,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/test",
        "http_body_sha256":"123"
    })).unwrap()
}

#[test]
fn official_anchor_and_rule_fields_keep_their_distinct_meaning() {
    use contracts::{
        BtcSettlementReferenceV1 as Mode, CanonicalReferenceEventPayloadV2 as Payload,
    };
    use data_pipeline_core::public_input::{MarketAnchorOriginV1, MarketAnchorQueryResultV1};
    use data_pipeline_core::public_reference::{
        canonicalize_market_reference_response_v1, market_anchor_source_evidence_v3,
    };
    let mut row = anchor_input();
    let source = market_anchor_source_evidence_v3(&row).unwrap();
    let events =
        canonicalize_market_reference_response_v1(&row, &market(), &source, "rule-v1").unwrap();
    assert_eq!(events.len(), 2);
    assert!(
        matches!(&events[0].payload,Payload::MarketPriceAnchorUpdate(update) if update.anchor.price_usd_per_btc.0=="64950.01421851144")
    );
    assert!(
        matches!(&events[1].payload,Payload::MarketSettlementReferenceUpdate(update) if update.reference==Mode::ChainlinkTwapThirtySeconds)
    );
    assert!(
        events
            .iter()
            .all(|event| event.time.source_event_at_utc_ms.is_none())
    );
    assert_eq!(
        events[0].lineage.source_evidence,
        events[1].lineage.source_evidence
    );
    row.metadata_origin = MarketAnchorOriginV1::WebsiteCryptoPrice;
    row.price_to_beat_source_field = Some("openPrice".to_owned());
    let events =
        canonicalize_market_reference_response_v1(&row, &market(), &source, "rule-v1").unwrap();
    assert_eq!(events.len(), 1); // 官网的请求配置不能发布一条 Gamma 规则。
    row.price_to_beat_decimal = None;
    assert!(
        canonicalize_market_reference_response_v1(&row, &market(), &source, "rule-v1")
            .unwrap()
            .is_empty()
    );
    row.metadata_origin = MarketAnchorOriginV1::GammaEventBySlug;
    row.twap_enabled = None;
    assert!(
        canonicalize_market_reference_response_v1(&row, &market(), &source, "rule-v1")
            .unwrap()
            .is_empty()
    );
    row.twap_enabled = Some(false);
    let events =
        canonicalize_market_reference_response_v1(&row, &market(), &source, "rule-v1").unwrap();
    assert!(
        matches!(&events[0].payload,Payload::MarketSettlementReferenceUpdate(update) if update.reference==Mode::ChainlinkSpot)
    );
    for result in [
        MarketAnchorQueryResultV1::HttpError,
        MarketAnchorQueryResultV1::DecodeError,
        MarketAnchorQueryResultV1::TransportError,
        MarketAnchorQueryResultV1::Timeout,
    ] {
        row.anchor_query_result = result;
        assert!(
            canonicalize_market_reference_response_v1(&row, &market(), &source, "rule-v1")
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn same_value_confirmations_and_price_corrections_remain_distinct_source_facts() {
    use data_pipeline_core::public_reference::{
        canonicalize_market_reference_response_v1, market_anchor_source_evidence_v3,
    };
    let mut row = anchor_input();
    let first_source = market_anchor_source_evidence_v3(&row).unwrap();
    let first =
        canonicalize_market_reference_response_v1(&row, &market(), &first_source, "rule-v1")
            .unwrap();
    row.timestamp_received += 10;
    row.collector_admission_time_utc_ms += 10;
    row.collector_admission_sequence += 1;
    row.anchor_query_ordinal += 1;
    let second_source = market_anchor_source_evidence_v3(&row).unwrap();
    let second =
        canonicalize_market_reference_response_v1(&row, &market(), &second_source, "rule-v1")
            .unwrap();
    assert_ne!(
        first_source.source_message_id,
        second_source.source_message_id
    );
    assert_ne!(first[0].event_id, second[0].event_id);
    assert_eq!(first[0].payload, second[0].payload);
    row.price_to_beat_decimal = Some("64950.01421851145".to_owned());
    let corrected =
        canonicalize_market_reference_response_v1(&row, &market(), &second_source, "rule-v1")
            .unwrap();
    assert_ne!(second[0].payload, corrected[0].payload);
}

fn twap_input(window_s: u64) -> data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
    use data_pipeline_core::typed_raw::{PolymarketRtdsChainlinkBtcUsdRow, RtdsFreshnessStatus};
    data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: 2000,
            message_timestamp: 1999,
            payload_timestamp: 1998,
            age_ms: Some(2),
            symbol: "btc/usd".to_owned(),
            value: "64950.01".to_owned(),
            topic: if window_s == 30 {
                "crypto_prices_twap_thirty"
            } else {
                "crypto_prices_twap_sixty"
            }
            .to_owned(),
            message_type: "update".to_owned(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".to_owned(),
            source_native_payload_json: format!("{{\"window_s\":{window_s}}}"),
            collector_host_id: "host".to_owned(),
            collector_region: "hk".to_owned(),
            source_endpoint: "wss://ws-live-data.polymarket.com".to_owned(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".to_owned(),
        },
        full_accuracy_value: "64950014218511440012345".to_owned(),
        window_s,
        source_sequence_epoch_id: "rtds-epoch".to_owned(),
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: 2001,
    }
}

#[test]
fn twap_windows_use_e18_and_existing_anomaly_classifications_never_become_prices() {
    use contracts::{CanonicalReferenceEventPayloadV2 as Payload, MarketDataQuality};
    use data_pipeline_core::public_reference::{
        canonicalize_chainlink_twap_v1, twap_source_evidence_v3,
    };
    use data_pipeline_core::typed_raw::RtdsFreshnessStatus;
    let mut ids = Vec::new();
    for window in [30, 60] {
        let row = twap_input(window);
        let source = twap_source_evidence_v3(&row).unwrap();
        let event = canonicalize_chainlink_twap_v1(&row, &source, "rule-v1").unwrap();
        let Payload::ChainlinkBtcUsdTwap(value) = &event.payload else {
            panic!()
        };
        assert_eq!(value.window_s, window);
        assert_eq!(value.price_usd_per_btc.0, "64950.014218511440012345");
        ids.push(event.event_id);
    }
    assert_ne!(ids[0], ids[1]);
    for status in [
        RtdsFreshnessStatus::InvalidType,
        RtdsFreshnessStatus::InvalidTopic,
        RtdsFreshnessStatus::InvalidSymbol,
        RtdsFreshnessStatus::InvalidTimestamp,
    ] {
        let mut row = twap_input(30);
        row.common.freshness_status = status;
        row.full_accuracy_value = "not-an-integer".to_owned();
        let source = twap_source_evidence_v3(&row).unwrap();
        if status == RtdsFreshnessStatus::InvalidTimestamp {
            assert_eq!(source.source_event_at_utc_ms, None);
        }
        let event = canonicalize_chainlink_twap_v1(&row, &source, "rule-v1").unwrap();
        assert_eq!(event.quality.availability, MarketDataQuality::Invalid);
        assert!(matches!(event.payload, Payload::RtdsObservedAnomaly(_)));
    }
}

#[test]
fn historical_application_keeps_late_receipt_and_original_identity_without_fake_epoch() {
    use contracts::{CanonicalReferenceEventPayloadV2 as Payload, PriceAnchorApplicationV1};
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reference::{
        canonicalize_historical_price_anchor_batch_v1, historical_response_source_evidence_v3,
    };
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".to_owned(),
        request_url: "https://gamma-api.polymarket.com/events".to_owned(),
        received_at_utc: "2026-09-08T04:00:00.123456Z".to_owned(),
        received_at_utc_ms: 1_788_840_000_123,
        receipt_observation: "curl_completed".to_owned(),
        response_body_sha256: "a".repeat(64),
        response_body_path: "responses/a.json".to_owned(),
        original_response_file: "/original/a.json".to_owned(),
        http_status_code: None,
        response_url: None,
    };
    let source = historical_response_source_evidence_v3(&response).unwrap();
    assert_eq!(source.collector_admission, None);
    assert_eq!(source.source_event_at_utc_ms, None);
    let mut relocated = response.clone();
    relocated.response_body_path = "other/a.json".to_owned();
    relocated.original_response_file = "/other/a.json".to_owned();
    assert_eq!(
        historical_response_source_evidence_v3(&relocated).unwrap(),
        source
    );
    let mut identity = source.clone();
    identity.received_at_utc_ms = 900;
    let identities = std::collections::BTreeMap::from([("original-row".to_owned(), identity)]);
    let rows = [900, 1050]
        .into_iter()
        .enumerate()
        .map(|(i, ready)| HistoricalPriceAnchorRepairRowV1 {
            schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.to_owned(),
            condition_id: format!("condition-{i}"),
            clob_market_id: format!("{i}"),
            slug: format!("slug-{i}"),
            asset_ids_json: "[\"1\",\"2\"]".to_owned(),
            market_start_at_utc_ms: 1000,
            market_end_at_utc_ms: 301000,
            anchor_source: contracts::MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
            price_to_beat_raw: "64950.01421851144".to_owned(),
            response_group_id: source.source_message_id.0.clone(),
            response_member_index: i as u32,
            response_received_at_utc_ms: response.received_at_utc_ms,
            response_sha256: response.response_body_sha256.clone(),
            source_value_path: format!("/{i}/eventMetadata/priceToBeat"),
            source_event_at_utc_ms: None,
            historical_identity_ready_at_utc_ms: ready,
            historical_identity_evidence_ids_json: "[\"original-row\"]".to_owned(),
            repair_policy_version: "price_anchor_historical_repair_v1".to_owned(),
        })
        .collect();
    let batch = HistoricalPriceAnchorRepairBatchInput {
        response_group_id: source.source_message_id.0.clone(),
        response,
        rows,
        identity_evidence: std::collections::BTreeMap::new(),
    };
    let events =
        canonicalize_historical_price_anchor_batch_v1(&batch, &identities, "rule-v1").unwrap();
    assert_eq!(events.len(), 2);
    let recorded = RecordedIngressV1 {
        input_stream_id: "repair".to_owned(),
        input_position: 0,
        ingress: DptIngressV1::Message {
            row: TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch.clone()),
            source_evidence: source.clone(),
        },
        dependency_record_refs: vec![],
    };
    let prepared = prepare_ingress_v1(&recorded, None, &identities, &[], "rule-v1").unwrap();
    let PreparedCanonicalInputV1::HistoricalAnchorRepair { facts, .. } = prepared.current else {
        panic!("repair")
    };
    assert_eq!(facts, events);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(
            event.time.received_at_utc_ms,
            batch.response.received_at_utc_ms
        );
        assert_eq!(event.lineage.source_evidence, vec![source.clone()]);
        let Payload::MarketPriceAnchorUpdate(update) = &event.payload else {
            panic!()
        };
        let PriceAnchorApplicationV1::HistoricalRepair {
            effective_from_utc_ms,
            historical_identity_evidence,
            ..
        } = &update.anchor.application
        else {
            panic!()
        };
        assert_eq!(*effective_from_utc_ms, [1000, 1050][i]);
        assert_eq!(historical_identity_evidence[0].received_at_utc_ms, 900);
        assert!(
            historical_identity_evidence[0]
                .collector_admission
                .is_none()
        );
    }
}

#[test]
fn prepared_reference_variants_keep_source_ids_anomalies_and_end_meanings() {
    use contracts::*;
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reduction::*;
    use data_pipeline_core::public_reference::*;
    use data_pipeline_core::typed_raw::{
        POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA, RtdsFreshnessStatus, TypedRawRow,
    };
    let mut rows = vec![
        TypedRawRowV2::PolymarketRtdsChainlinkTwap(twap_input(60)),
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::AnchorRule(anchor_input()),
        )),
    ];
    for status in [
        RtdsFreshnessStatus::Fresh,
        RtdsFreshnessStatus::InvalidType,
        RtdsFreshnessStatus::InvalidTopic,
        RtdsFreshnessStatus::InvalidSymbol,
        RtdsFreshnessStatus::InvalidTimestamp,
    ] {
        let mut row = twap_input(60).common;
        row.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.to_owned();
        row.topic = "crypto_prices_chainlink".to_owned();
        row.freshness_status = status;
        row.payload_timestamp = 2100;
        row.age_ms = Some(-100);
        rows.push(TypedRawRowV2::Existing(
            TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row),
        ));
    }
    for (position, row) in rows.into_iter().enumerate() {
        let source = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        let recorded = RecordedIngressV1 {
            input_stream_id: "references".to_owned(),
            input_position: position as u64,
            ingress: DptIngressV1::Message {
                row,
                source_evidence: source.clone(),
            },
            dependency_record_refs: vec![],
        };
        let prepared =
            prepare_ingress_v1(&recorded, None, &Default::default(), &[], "rule-v1").unwrap();
        assert_eq!(
            prepared.prepared_group_id.as_ref(),
            Some(&source.source_message_id)
        );
        let events = match prepared.current {
            PreparedCanonicalInputV1::ChainlinkTwap(event)
            | PreparedCanonicalInputV1::ChainlinkSpot(event) => vec![event],
            PreparedCanonicalInputV1::AnchorRule { facts, .. } => facts,
            _ => panic!("reference"),
        };
        if position >= 3 {
            assert!(matches!(
                events[0].payload,
                CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(_)
            ));
            assert_eq!(events[0].quality.availability, MarketDataQuality::Invalid);
            assert_eq!(
                events[0].time.source_event_at_utc_ms.is_none(),
                position == 6
            );
        }
        if position == 2 {
            assert_eq!(
                events[0].time.received_at_utc_ms - events[0].time.source_event_at_utc_ms.unwrap(),
                -100
            );
        }
        let ids = events
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        let context = PublicReductionContextV1 {
            input_stream_id: "references".to_owned(),
            closed_at_input_position: position as u64,
            first_fact_ordinal: 0,
            first_report_ordinal: 0,
            group_id: source.source_message_id.clone(),
            source_refs: vec![CanonicalInputRecordRefV1 {
                input_stream_id: "references".to_owned(),
                input_position: position as u64,
            }],
            direct_sources: vec![source],
        };
        let durable =
            materialize_public_reduction_v1(bind_prepared_reference_facts_v1(events, context))
                .unwrap();
        assert_eq!(
            durable
                .bindings
                .into_iter()
                .map(|binding| binding.event_id)
                .collect::<Vec<_>>(),
            ids
        );
    }
    for ingress in [
        DptIngressV1::EndOfInput,
        DptIngressV1::Aborted {
            reason: "original-disconnect".to_owned(),
        },
    ] {
        let input = RecordedIngressV1 {
            input_stream_id: "end".to_owned(),
            input_position: 7,
            ingress,
            dependency_record_refs: vec![],
        };
        let prepared =
            prepare_ingress_v1(&input, None, &Default::default(), &[], "rule-v1").unwrap();
        assert!(prepared.prepared_group_id.is_none());
        match (&input.ingress, prepared.current) {
            (DptIngressV1::EndOfInput, PreparedCanonicalInputV1::EndOfInput) => {}
            (
                DptIngressV1::Aborted { reason: original },
                PreparedCanonicalInputV1::Aborted { reason },
            ) => assert_eq!(reason, original),
            _ => panic!("不能把异常结束变成正常结束"),
        }
    }
}
