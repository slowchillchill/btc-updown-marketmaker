use super::*;
use crate::public_input::*;
use crate::public_observation_core::{public_group_visible_at_utc_ms, tests as fixture};
use crate::public_reference::historical_response_source_evidence_v3;
use crate::typed_raw::{BinanceDepthStitchVerdict, DepthLevel, TypedRawRow};
use std::{collections::BTreeMap, path::PathBuf};

fn depth(position: u64, sequence: u64, received: i64, quantity: &str) -> RecordedIngressV1 {
    let row = if sequence == 1 {
        let mut row = fixture::snapshot("blb-lifecycle", 100, 1);
        row.timestamp_received = received;
        row.snapshot_fetch_start_utc = received - 10;
        row.collector_admission_time_utc_ms = received + 1;
        row.bids = vec![DepthLevel {
            price: "65001".into(),
            quantity: quantity.into(),
        }];
        row.asks = vec![DepthLevel {
            price: "65003".into(),
            quantity: "1".into(),
        }];
        TypedRawRow::BinanceDepthSnapshot(row)
    } else {
        let mut row = fixture::update(
            "blb-lifecycle",
            99 + sequence,
            99 + sequence,
            sequence,
            Some(100),
            if sequence == 2 {
                None
            } else {
                Some(98 + sequence)
            },
            BinanceDepthStitchVerdict::Applied,
            vec![DepthLevel {
                price: "65001".into(),
                quantity: quantity.into(),
            }],
        );
        row.timestamp_received = received;
        row.event_time = received - 1;
        row.collector_admission_time_utc_ms = received + 1;
        TypedRawRow::BinanceDepthUpdate(row)
    };
    fixture::record(position, TypedRawRowV2::Existing(row))
}

#[test]
fn formal_blb_lifecycle_matches_full_fallback_and_cold_history() {
    let root = std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "blb-lifecycle-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            ))
        })
        .join("formal-blb-lifecycle");
    std::fs::create_dir_all(&root).unwrap();
    let mut core = fixture::core();
    let template = super::reuse_tests::stream();
    let mut identity = template.identity().clone();
    identity.stream_header = core.header().clone();
    let mut config = template.config.as_ref().clone();
    config.blb_lookback_windows_ms = vec![200];
    let mut shared = PublicStatisticsStreamV1::new(identity.clone(), config.clone()).unwrap();
    let full = PublicStatisticsStreamV1::new(identity, config).unwrap();
    let first_id = format!("0x{}", "1".repeat(64));
    let second_id = format!("0x{}", "2".repeat(64));
    let mut frozen = None;
    let mut same_ms = None;
    let mut consumed = 0;
    let mut applied_inputs = 0;
    let mut consume = |core: &mut crate::public_observation_core::PublicObservationCoreV1,
                       input: RecordedIngressV1,
                       identities: &BTreeMap<String, SourceEvidenceRefV3>| {
        std::fs::write(
            root.join(format!("input-{}.json", input.input_position)),
            serde_json::to_vec_pretty(&input).unwrap(),
        )
        .unwrap();
        let prepared = prepare_public_ingress_v1(core, &input, identities, &[]).unwrap();
        let applied = core.push(&prepared).unwrap().applied;
        applied_inputs += 1;
        for group in &applied.closed_groups {
            let at = public_group_visible_at_utc_ms(&input.ingress, &group.records);
            if at == 20_401 {
                continue; // 两个消费者都合法跳过同一组，下一组必须完整追赶。
            }
            let reference = PublicGroupReferenceV1 {
                input_stream_id: input.input_stream_id.clone(),
                applied_prefix: applied.applied_prefix.clone(),
                group_position: group.group_position,
                group_id: group.group_id.clone(),
                closed_at_input_position: group.closed_at_input_position,
            };
            // 原完整表示沿正式迭代展开，不复制BLB算法或填内部统计状态。
            let complete = ClosedBusinessGroupV1 {
                observations: group
                    .observations
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .into(),
                ..group.as_ref().clone()
            };
            let source = if at == 20_201 {
                &complete
            } else {
                group.as_ref()
            };
            let actual = PreparedPublicStatisticsV1::default()
                .consume(&shared, reference.clone(), at, source)
                .unwrap()
                .unwrap();
            let expected = PreparedPublicStatisticsV1::default()
                .consume(&full, reference, at, &complete)
                .unwrap()
                .unwrap();
            consumed += 1;
            std::fs::write(
                root.join(format!("prefix-{}.json", input.input_position)),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "at": at, "actual": actual, "full_fallback": expected
                }))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                serde_json::to_value(&actual).unwrap(),
                serde_json::to_value(&expected).unwrap(),
                "共享增量与原完整后备保留完全相同的持久统计值",
            );
            for observation in group.observations.iter() {
                assert_eq!(
                    actual.read(&observation, at),
                    expected.read(&observation, at)
                );
            }
            let observation = group.observations.get_market(&first_id).unwrap();
            let read = actual.read(observation, at);
            if at == 19_901 {
                assert!(at < observation.market.window.start_at_utc_ms);
                assert!(read.blb.official_k_usd_per_btc.is_none());
                assert!(read.blb_lookbacks[0].right.is_some());
                frozen = Some((actual.clone(), observation.clone(), read.clone(), at));
            }
            if at == 20_050 {
                assert_eq!(read.blb.status, "visible_interval_only");
                assert_eq!(read.blb.official_k_usd_per_btc, Some(Decimal::from(65000)));
                assert_eq!(read.blb.visible_quantity_btc, Some(Decimal::from(2)));
            }
            if at == 20_101 && same_ms.is_none() {
                assert_eq!(read.blb.visible_quantity_btc, Some(Decimal::ONE));
                same_ms = Some((actual.clone(), observation.clone(), read.clone(), at));
                let bytes = serde_json::to_vec(&actual).unwrap();
                let restored: PublicStatisticsHistoryV1 = serde_json::from_slice(&bytes).unwrap();
                assert!(!restored.blb_shared_group);
                assert_eq!(restored.read(observation, at), read);
                shared = PublicStatisticsStreamV1::from_history(Arc::new(restored));
            } else if at == 20_101 {
                assert_eq!(read.blb.visible_quantity_btc, Some(Decimal::from(2)));
                assert_eq!(
                    actual
                        .blb_endpoints
                        .get(&first_id)
                        .unwrap()
                        .iter()
                        .filter(|p| p.applied_at_utc_ms == at)
                        .count(),
                    1,
                );
            }
            if at == 20_201 {
                let left = read.blb_lookbacks[0].left.as_ref().unwrap();
                assert_eq!(left.applied_at_utc_ms, 20_001);
                assert!(left.official_k_usd_per_btc.is_none());
                assert!(read.blb_lookbacks[0].right.is_some());
            }
            if at == 20_500 {
                assert_eq!(at, observation.market.window.end_at_utc_ms);
                assert!(actual.blb_endpoints.contains_key(&first_id));
                assert!(read.blb_lookbacks[0].right.is_some());
            }
            if at == 20_501 {
                assert!(!actual.blb_endpoints.contains_key(&first_id));
                assert!(actual.blb_endpoints.contains_key(&second_id));
                assert!(read.blb_lookbacks[0].right.is_none());
            }
        }
    };
    let mut metadata = fixture::metadata_snapshot_row();
    metadata.timestamp_received = 1_000;
    metadata.collector_admission_time_utc_ms = 1_001;
    metadata.fixed_identity_verified_at_utc = 999;
    metadata.source_evidence_received_at_utc = Some(1_000);
    metadata.row_evidence_validation_completed_at_utc = Some(1_001);
    metadata.metadata_fetch_start_utc = Some(990);
    metadata.market_end_utc = 20_500;
    consume(
        &mut core,
        fixture::record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                metadata.clone(),
            ))),
        ),
        &BTreeMap::new(),
    );
    consume(&mut core, depth(1, 1, 19_900, "1"), &BTreeMap::new());
    metadata.condition_id = second_id.clone();
    metadata.clob_market_id = "3195577".into();
    metadata.asset_ids = "[\"21\",\"4\"]".into();
    metadata.collector_admission_sequence = 2;
    metadata.timestamp_received = 19_902;
    metadata.collector_admission_time_utc_ms = 19_903;
    metadata.fixed_identity_verified_at_utc = 19_901;
    metadata.source_evidence_received_at_utc = Some(19_902);
    metadata.row_evidence_validation_completed_at_utc = Some(19_903);
    metadata.metadata_fetch_start_utc = Some(19_890);
    metadata.market_start_utc = 20_400;
    metadata.market_end_utc = 20_800;
    consume(
        &mut core,
        fixture::record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
        ),
        &BTreeMap::new(),
    );
    consume(&mut core, depth(3, 2, 20_000, "2"), &BTreeMap::new());
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".into(),
        request_url: "https://gamma-api.polymarket.com/events".into(),
        received_at_utc: "1970-01-01T00:00:20.050Z".into(),
        received_at_utc_ms: 20_050,
        receipt_observation: "curl_completed".into(),
        response_body_sha256: "a".repeat(64),
        response_body_path: "synthetic-response.json".into(),
        original_response_file: "/synthetic/response.json".into(),
        http_status_code: None,
        response_url: None,
    };
    let source = historical_response_source_evidence_v3(&response).unwrap();
    let mut identities = BTreeMap::new();
    let rows = [first_id.clone(), second_id.clone()]
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            let context = core.pm_context(&id).unwrap();
            let key = format!("identity-{i}");
            identities.insert(key.clone(), context.identity_source_evidence[0].clone());
            HistoricalPriceAnchorRepairRowV1 {
                schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.into(),
                condition_id: id,
                clob_market_id: context.market.clob_market_id.0,
                slug: "btc-updown-5m".into(),
                asset_ids_json: serde_json::to_string(
                    &context
                        .assets
                        .iter()
                        .map(|a| &a.asset.token_id.0)
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
                market_start_at_utc_ms: context.market.window.start_at_utc_ms,
                market_end_at_utc_ms: context.market.window.end_at_utc_ms,
                anchor_source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
                price_to_beat_raw: "65000".into(),
                response_group_id: source.source_message_id.0.clone(),
                response_member_index: i as u32,
                response_received_at_utc_ms: response.received_at_utc_ms,
                response_sha256: response.response_body_sha256.clone(),
                source_value_path: format!("/{i}/eventMetadata/priceToBeat"),
                source_event_at_utc_ms: None,
                historical_identity_ready_at_utc_ms: [1_001, 19_903][i],
                historical_identity_evidence_ids_json: format!("[\"{key}\"]"),
                repair_policy_version: "price_anchor_historical_repair_v1".into(),
            }
        })
        .collect();
    let mut repair = fixture::record(
        4,
        TypedRawRowV2::HistoricalPriceAnchorRepairBatch(HistoricalPriceAnchorRepairBatchInput {
            response_group_id: source.source_message_id.0.clone(),
            response,
            rows,
            identity_evidence: BTreeMap::new(),
        }),
    );
    repair.dependency_record_refs = [0, 2]
        .into_iter()
        .map(|input_position| CanonicalInputRecordRefV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position,
        })
        .collect();
    consume(&mut core, repair, &identities);
    for (position, sequence, received, quantity) in [
        (5, 3, 20_100, "1"),
        (6, 4, 20_100, "2"),
        (7, 5, 20_200, "1"),
        (8, 6, 20_300, "2"),
        (9, 7, 20_400, "1"),
        (10, 8, 20_499, "2"),
        (11, 9, 20_500, "1"),
    ] {
        consume(
            &mut core,
            depth(position, sequence, received, quantity),
            &BTreeMap::new(),
        );
    }
    drop(consume);
    for (history, observation, read, at) in [frozen.unwrap(), same_ms.unwrap()] {
        assert_eq!(history.read(&observation, at), read);
    }
    assert_eq!(applied_inputs, 12);
    assert_eq!(consumed, 11);
}
