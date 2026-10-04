//! 有限展示验证材料：所属公共核心与原写入入口生成；非市场研究、非策略运行。
mod common {
    pub const FIRST: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    pub const SECOND: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
}
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::*,
};
use data_pipeline_training::public_recording::PublicRecordingWriterV1;
use std::{collections::BTreeMap, path::PathBuf};
fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("指定新建验证目录"));
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("inputs")).unwrap();
    let header = PublicInputStreamHeaderV1 {
        input_stream_id: "engineering-public-evidence".into(),
        canonical_rule_version: "rule-v1".into(),
        observation_version: "public-observation-v1".into(),
        ordering_version: "original-input-v1".into(),
        input_scope: "constructed-finite-visualization-test".into(),
        max_pending_ws_terminals: 2,
        initial_state: PublicInitialStateV1::Empty,
    };
    let mut core = PublicObservationCoreV1::new(header).unwrap();
    let mut writer = PublicRecordingWriterV1::create(
        &root.join("public"),
        core.header(),
        data_pipeline_training::gate_a_formal_output_config(),
    )
    .unwrap();
    let mut position = 0;
    let mut apply = |row: TypedRawRowV2,
                     identities: &BTreeMap<String, SourceEvidenceRefV3>,
                     dependencies: Vec<CanonicalInputRecordRefV1>,
                     core: &mut PublicObservationCoreV1| {
        let source = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        // SourceBinding 的物理摘要对应本验证实际写出的材料，避免占位摘要/不存在的文件。
        use sha2::{Digest, Sha256};
        let relative_path = format!("inputs/input-{position}.json");
        let bytes = serde_json::to_vec(&row).unwrap();
        std::fs::write(root.join(&relative_path), &bytes).unwrap();
        let file_sha256 = format!("{:x}", Sha256::digest(&bytes));
        let manifest = serde_json::to_vec(
            &serde_json::json!({"relative_path":relative_path,"file_sha256":file_sha256}),
        )
        .unwrap();
        std::fs::write(
            root.join(format!("inputs/manifest-{position}.json")),
            &manifest,
        )
        .unwrap();
        let manifest_sha256 = format!("{:x}", Sha256::digest(&manifest));
        let input = RecordedIngressV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position: position,
            ingress: DptIngressV1::Message {
                row,
                source_evidence: source.clone(),
            },
            dependency_record_refs: dependencies,
        };
        writer
            .record_source_binding(SourceArtifactBindingV1 {
                source_message_id: source.source_message_id,
                manifest_sha256: Sha256Hex(manifest_sha256),
                file_sha256: Sha256Hex(file_sha256),
                relative_path,
            })
            .unwrap();
        let prepared = prepare_public_ingress_v1(core, &input, identities, &[]).unwrap();
        let result = core.push(&prepared).unwrap().into_owned();
        writer.record(input, result).unwrap();
        position += 1;
    };
    let empty = BTreeMap::new();
    for (i, id) in [common::FIRST, common::SECOND].into_iter().enumerate() {
        // 最小合法原类型；字段取自所属核心的 metadata_snapshot_row 合同夹具。
        let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
        use sha2::{Digest, Sha256};
        let row:PolymarketMarketMetadataRow=serde_json::from_value(serde_json::json!({
            "metadata_row_kind":PolymarketMetadataRowKind::MetadataSnapshot,"timestamp_received":10000+i as i64,
            "collector_admission_sequence":i+1,"collector_admission_time_utc_ms":10001+i as i64,
            "metadata_origin":PolymarketMetadataOrigin::GammaMarketsDiscovery,"collector_host_id":"engineering","collector_region":"test",
            "schema_version":"polymarket_market_metadata_v5","slug":"btc-updown-5m-engineering",
            "condition_id":id,"clob_market_id":if i==0{"3195576"}else{"3195577"},"asset_ids":"[\"20\",\"3\"]",
            "market_start_utc":20000,"market_end_utc":30000,"fixed_identity_verified_at_utc":9999+i as i64,
            "market_lifecycle_state":MetadataMarketLifecycleState::Prefetched,"market_runtime_role":MetadataMarketRuntimeRole::Next,"source_endpoint":"https://gamma-api.polymarket.com/markets",
            "source_evidence_origin":MetadataSourceEvidenceOrigin::GammaMarketsDiscovery,"source_evidence_event_type":MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse,
            "source_evidence_received_at_utc":10000+i as i64,"row_evidence_validation_completed_at_utc":10001+i as i64,
            "source_evidence_payload_json":payload,"source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())),
            "metadata_fetch_start_utc":9990+i as i64,"closed":false,"outcomes_raw":"[\"UP\",\"DOWN\"]","outcome_prices_raw":"[\"0.5\",\"0.5\"]","active":true,"accepting_orders":true
        })).unwrap();
        apply(
            TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                PolymarketMarketMetadataV6Input::Existing(row),
            )),
            &empty,
            vec![],
            &mut core,
        );
    }
    let anchor:MarketAnchorRuleUpdateInput=serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6","timestamp_received":22000,
        "collector_admission_sequence":3,"collector_admission_time_utc_ms":22001,
        "metadata_origin":"gamma_event_by_slug","collector_host_id":"engineering","collector_region":"test",
        "slug":"btc-updown-5m-engineering","condition_id":common::FIRST,"clob_market_id":"3195576","asset_ids":"[\"20\",\"3\"]",
        "market_start_utc":20000,"market_end_utc":30000,"fixed_identity_verified_at_utc":9999,
        "market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":21990,
        "anchor_query_source_epoch_id":"engineering-query","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"64950.01421851144","price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":22000,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/engineering","http_body_sha256":"a".repeat(64)
    })).unwrap();
    apply(
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::AnchorRule(anchor),
        )),
        &empty,
        vec![],
        &mut core,
    );
    let twap = PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: 22000,
            message_timestamp: 21999,
            payload_timestamp: 21998,
            age_ms: Some(2),
            symbol: "btc/usd".into(),
            value: "64950.01".into(),
            topic: "crypto_prices_twap_thirty".into(),
            message_type: "update".into(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".into(),
            source_native_payload_json: "{\"window_s\":30}".into(),
            collector_host_id: "engineering".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".into(),
        },
        full_accuracy_value: "64950014218511440012345".into(),
        window_s: 30,
        source_sequence_epoch_id: "engineering-rtds".into(),
        collector_admission_sequence: 4,
        collector_admission_time_utc_ms: 22001,
    };
    apply(
        TypedRawRowV2::PolymarketRtdsChainlinkTwap(twap),
        &empty,
        vec![],
        &mut core,
    );
    for (seq, source) in [(5, 21997), (6, 21996)] {
        let row = PolymarketOrderbookRow {
            timestamp_received: 22000,
            timestamp: Some(source),
            market: common::FIRST.into(),
            slug: Some("btc-updown-5m-engineering".into()),
            collector_host_id: "engineering".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            schema_version: POLYMARKET_ORDERBOOK_SCHEMA.into(),
            source_sequence_epoch_id: "engineering-pm".into(),
            collector_admission_sequence: seq,
            collector_admission_time_utc_ms: 22001,
            event: PolymarketOrderbookEvent::Book {
                asset_id: "20".into(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.5".into(),
                    size: seq.to_string(),
                }],
                asks: vec![],
                last_trade_price: None,
            },
        };
        apply(
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
            &empty,
            vec![],
            &mut core,
        );
    }
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".into(),
        request_url: "https://gamma-api.polymarket.com/events".into(),
        received_at_utc: "2026-09-08T04:00:00.123456Z".into(),
        received_at_utc_ms: 1788840000123,
        receipt_observation: "curl_completed".into(),
        response_body_sha256: "a".repeat(64),
        response_body_path: "engineering-response.json".into(),
        original_response_file: "engineering-response.json".into(),
        http_status_code: None,
        response_url: None,
    };
    let source = historical_response_source_evidence_v3(&response).unwrap();
    let mut identities = BTreeMap::new();
    let rows = [common::FIRST, common::SECOND]
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            let context = core.pm_context(id).unwrap();
            let key = format!("identity-{i}");
            identities.insert(key.clone(), context.identity_source_evidence[0].clone());
            HistoricalPriceAnchorRepairRowV1 {
                schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.into(),
                condition_id: id.into(),
                clob_market_id: context.market.clob_market_id.0,
                slug: "btc-updown-5m-engineering".into(),
                asset_ids_json: "[\"20\",\"3\"]".into(),
                market_start_at_utc_ms: 20000,
                market_end_at_utc_ms: 30000,
                anchor_source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
                price_to_beat_raw: "65000.01234567890123456789".into(),
                response_group_id: source.source_message_id.0.clone(),
                response_member_index: i as u32,
                response_received_at_utc_ms: response.received_at_utc_ms,
                response_sha256: response.response_body_sha256.clone(),
                source_value_path: format!("/{i}/eventMetadata/priceToBeat"),
                source_event_at_utc_ms: None,
                historical_identity_ready_at_utc_ms: 10000 + i as i64,
                historical_identity_evidence_ids_json: format!("[\"{key}\"]"),
                repair_policy_version: "price_anchor_historical_repair_v1".into(),
            }
        })
        .collect();
    let refs = (0..2)
        .map(|i| CanonicalInputRecordRefV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position: i,
        })
        .collect();
    apply(
        TypedRawRowV2::HistoricalPriceAnchorRepairBatch(HistoricalPriceAnchorRepairBatchInput {
            response_group_id: source.source_message_id.0,
            response,
            rows,
            identity_evidence: BTreeMap::new(),
        }),
        &identities,
        refs,
        &mut core,
    );
    drop(apply);
    let end = RecordedIngressV1 {
        input_stream_id: core.header().input_stream_id.clone(),
        input_position: position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    let prepared = prepare_public_ingress_v1(&core, &end, &empty, &[]).unwrap();
    let result = core.push(&prepared).unwrap().into_owned();
    writer.record(end, result).unwrap();
    let prefix = writer.stabilize().unwrap().unwrap();
    let prefix_path = root.join("prefix.json");
    std::fs::write(&prefix_path, serde_json::to_vec_pretty(&prefix).unwrap()).unwrap();
    let sources = serde_json::json!({"runs":[{"label":"公共变体验证 · 非研究/策略运行","public_recording":{"root":root.join("public"),"committed_prefix":prefix_path,"page_rows":200,"max_batch_bytes":1048576}},{"label":"公共跨页验证 · 非研究/策略运行","public_recording":{"root":root.join("public"),"committed_prefix":prefix_path,"page_rows":10,"max_batch_bytes":1048576}}]});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec_pretty(&sources).unwrap(),
    )
    .unwrap();
    eprintln!(
        "有限公共验证完成：{} 输入 / {} 规范记录 / {} 公共组；所有业务材料来自公共核心及原写入接口。",
        prefix.next_input_position, prefix.next_canonical_position, prefix.next_group_position
    );
}
