use super::*;
use arrow_array::ArrayRef;
use data_collect::market_anchor::{
    MarketAnchorIdentity, MarketAnchorObservation, MarketAnchorOrigin, MarketAnchorQueryResult,
};
use data_collect::polymarket_market_metadata_layout::{
    MarketLifecycleState, MarketRuntimeRole, PolymarketMarketMetadataRow,
    PolymarketMarketMetadataV5Row,
};
use data_collect::polymarket_market_metadata_v6_layout::{
    MarketAnchorRuleUpdateRow, PolymarketMarketMetadataV6Row,
    polymarket_market_metadata_v6_rows_to_batch, polymarket_market_metadata_v6_schema,
};
use data_collect::source_hourly_layout::{
    RtdsChainlinkBtcUsdHourlyRow, RtdsChainlinkTwapHourlyRow,
};
use data_collect::writer::TypedParquetSegmentWriter;
use serde_json::json;

const RECEIVED: i64 = 1_785_369_600_050;

fn old_metadata() -> PolymarketMarketMetadataV5Row {
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    let mut row: PolymarketMarketMetadataRow = serde_json::from_value(json!({
        "metadata_row_kind":"metadata_snapshot", "timestamp_received": RECEIVED,
        "metadata_origin":"gamma_markets_discovery", "collector_host_id":"collector-1",
        "collector_region":"hk", "schema_version": METADATA_V5, "slug":"btc-updown-5m-test",
        "condition_id":"condition", "clob_market_id":"123", "asset_ids":"[\"1\",\"2\"]",
        "market_start_utc":RECEIVED+100, "market_end_utc":RECEIVED+300100,
        "fixed_identity_verified_at_utc":RECEIVED-1, "market_lifecycle_state":"prefetched",
        "market_runtime_role":"next", "source_endpoint":"https://gamma-api.polymarket.com/markets",
        "source_evidence_origin":"gamma_markets_discovery",
        "source_evidence_event_type":"gamma_markets_discovery_response",
        "source_evidence_received_at_utc":RECEIVED, "row_evidence_validation_completed_at_utc":RECEIVED+1,
        "source_evidence_payload_json":payload, "source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())),
        "metadata_fetch_start_utc":RECEIVED-10, "closed":false, "outcomes_raw":"[\"UP\",\"DOWN\"]",
        "outcome_prices_raw":"[\"0.5\",\"0.5\"]", "active":true, "accepting_orders":true
    })).unwrap();
    row.collector_admission_sequence = 1;
    row.collector_admission_time_utc_ms = RECEIVED + 1;
    PolymarketMarketMetadataV5Row::from_common(row, None, None)
}

fn anchor_row() -> PolymarketMarketMetadataV6Row {
    let mut observation = MarketAnchorObservation::unavailable(MarketAnchorQueryResult::Observed);
    observation.price_to_beat_decimal = Some("64950.014218511440012345".to_owned());
    observation.price_to_beat_source_field = Some("eventMetadata.priceToBeat".to_owned());
    observation.twap_enabled = Some(true);
    observation.twap_lookback_seconds = Some(30);
    let body = "{\"eventMetadata\":{\"priceToBeat\":64950.014218511440012345}}";
    observation.http_body = Some(body.to_owned());
    observation.http_body_encoding = Some("utf8".to_owned());
    observation.http_body_sha256 = Some(format!("{:x}", Sha256::digest(body.as_bytes())));
    observation.http_received_at_utc = Some(RECEIVED);
    observation.http_request_url =
        Some("https://gamma-api.polymarket.com/events/slug/test".to_owned());
    observation.http_response_url = observation.http_request_url.clone();
    observation.http_status_code = Some(200);
    PolymarketMarketMetadataV6Row::AnchorRule(MarketAnchorRuleUpdateRow {
        timestamp_received: RECEIVED,
        collector_admission_sequence: 2,
        collector_admission_time_utc_ms: RECEIVED + 1,
        metadata_origin: MarketAnchorOrigin::GammaEventBySlug,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "hk".to_owned(),
        identity: MarketAnchorIdentity {
            slug: "test".to_owned(),
            condition_id: "condition".to_owned(),
            clob_market_id: "123".to_owned(),
            asset_ids: "[\"1\",\"2\"]".to_owned(),
            market_start_utc: RECEIVED - 100,
            market_end_utc: RECEIVED + 299900,
            fixed_identity_verified_at_utc: RECEIVED - 200,
        },
        market_lifecycle_state: MarketLifecycleState::Active,
        market_runtime_role: MarketRuntimeRole::Current,
        metadata_fetch_start_utc: RECEIVED - 10,
        anchor_query_source_epoch_id: "source-epoch".to_owned(),
        anchor_query_ordinal: 1,
        observation,
    })
}

fn finalize_with_fact(
    mut writer: TypedParquetSegmentWriter,
    source: &str,
    version: &str,
    rows: u64,
) -> data_collect::writer::PublishedSegment {
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1", json!({
        "source_kind":source,"schema_version":version,"utc_hour":"2026-07-30T00","part":0,"rows":rows,
    }).to_string()).unwrap();
    writer.finalize().unwrap()
}

fn manifest_for(
    root: &Path,
    segment: &data_collect::writer::PublishedSegment,
    source: &str,
    version: &str,
    new: bool,
) -> PathBuf {
    let day = root.join("2026-07-30/typed_raw/utc_day=2026-07-30");
    let path = day.join("manifest.json");
    let relative = segment
        .final_path
        .strip_prefix(&day)
        .unwrap()
        .to_str()
        .unwrap();
    let writer_acks = if new {
        crate::PUBLIC_WRITER_FINALIZE_ACKS.as_slice()
    } else {
        crate::REQUIRED_WRITER_FINALIZE_ACKS.as_slice()
    };
    let manifest = json!({
        "schema_version":"data_collect_daily_manifest_v3", "state":"closed", "utc_day":"2026-07-30",
        "closure_reason":"utc_day_barrier", "collection_day_coverage":{
            "schema_version":"data_collect_collection_day_coverage_v1", "state":"ended_known",
            "first_started_utc_ms":1_785_369_600_000_i64, "last_started_utc_ms":1_785_369_600_000_i64,
            "ended_utc_ms":1_785_456_000_000_i64, "reactivated_within_day":false
        },
        "producer_barriers":crate::REQUIRED_PRODUCER_BARRIERS,
        "writer_finalize_acks":writer_acks,
        "files":[{"path":relative,"source_kind":source,"schema_version":version,
            "utc_hour":"2026-07-30T00","part":0,"rows":segment.row_count,
            "bytes":fs::metadata(&segment.final_path).unwrap().len(),"sha256":segment.sha256}]
    });
    fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    path
}

#[test]
fn collector_v6_and_legacy_rows_decode_through_the_explicit_versioned_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp
        .path()
        .join("2026-07-30/typed_raw/utc_day=2026-07-30/source_kind=polymarket_market_metadata");
    fs::create_dir_all(&dir).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_current_source(
        &dir,
        "polymarket_market_metadata_00",
        METADATA_SOURCE,
    )
    .unwrap();
    writer
        .write_polymarket_market_metadata_v6_rows(&[
            PolymarketMarketMetadataV6Row::Existing(old_metadata()),
            anchor_row(),
        ])
        .unwrap();
    let segment = finalize_with_fact(
        writer,
        METADATA_SOURCE,
        POLYMARKET_MARKET_METADATA_SCHEMA_V6,
        2,
    );
    let manifest = manifest_for(
        tmp.path(),
        &segment,
        METADATA_SOURCE,
        POLYMARKET_MARKET_METADATA_SCHEMA_V6,
        true,
    );
    assert!(crate::validate_daily_manifest_admission_at_root(tmp.path(), &manifest).is_err());
    let admitted =
        crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &manifest, true)
            .unwrap();
    assert!(typed_raw::open_validated_typed_raw_parquet(&admitted.files()[0], 1).is_err());
    let rows = open_validated_public_input_parquet_v1(&admitted.files()[0], 1)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    let TypedRawRowV2::PolymarketMarketMetadataV6(row) = &rows[0] else {
        panic!()
    };
    let PolymarketMarketMetadataV6Input::Existing(row) = row.as_ref() else {
        panic!()
    };
    assert_eq!(row.schema_version, METADATA_V5);
    assert_eq!(row.collector_admission_sequence, 1);
    let TypedRawRowV2::PolymarketMarketMetadataV6(row) = &rows[1] else {
        panic!()
    };
    let PolymarketMarketMetadataV6Input::AnchorRule(row) = row.as_ref() else {
        panic!()
    };
    assert_eq!(
        row.price_to_beat_decimal.as_deref(),
        Some("64950.014218511440012345")
    );
    assert_eq!(row.anchor_source_computed_at_utc, None);
    assert_eq!(row.http_received_at_utc, Some(RECEIVED));
    assert_eq!(row.collector_admission_sequence, 2);
    assert_eq!(row.twap_lookback_seconds, Some(30));
}

#[test]
fn public_admission_keeps_six_source_inputs_and_existing_damage_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp
        .path()
        .join("2026-07-30/typed_raw/utc_day=2026-07-30/source_kind=polymarket_market_metadata");
    fs::create_dir_all(&dir).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        &dir,
        "polymarket_market_metadata_00",
        METADATA_V5,
        METADATA_SOURCE,
    )
    .unwrap();
    writer
        .write_polymarket_market_metadata_v5_rows(&[old_metadata()])
        .unwrap();
    let segment = finalize_with_fact(writer, METADATA_SOURCE, METADATA_V5, 1);
    let manifest = manifest_for(tmp.path(), &segment, METADATA_SOURCE, METADATA_V5, false);
    let old = crate::validate_daily_manifest_admission_at_root(tmp.path(), &manifest).unwrap();
    let new =
        crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &manifest, true)
            .unwrap();
    let expected = typed_raw::open_validated_typed_raw_parquet(&old.files()[0], 1)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let actual = open_validated_public_input_parquet_v1(&new.files()[0], 1)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        actual,
        expected
            .into_iter()
            .map(TypedRawRowV2::Existing)
            .collect::<Vec<_>>()
    );
    let original: Value = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    for (key, value) in [
        ("state", json!("open")),
        ("writer_finalize_acks", json!([])),
        ("producer_barriers", json!([])),
    ] {
        let mut damaged = original.clone();
        damaged[key] = value;
        fs::write(&manifest, serde_json::to_vec(&damaged).unwrap()).unwrap();
        assert!(
            crate::validate_daily_manifest_admission_with_public_inputs(
                tmp.path(),
                &manifest,
                true
            )
            .is_err()
        );
    }
    let mut damaged = original.clone();
    damaged["files"][0]["rows"] = json!(2);
    fs::write(&manifest, serde_json::to_vec(&damaged).unwrap()).unwrap();
    assert!(
        crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &manifest, true)
            .is_err()
    );
    fs::write(&manifest, serde_json::to_vec(&original).unwrap()).unwrap();
    fs::write(&segment.final_path, b"damaged").unwrap();
    assert!(open_validated_public_input_parquet_v1(&new.files()[0], 1).is_err());
}

#[test]
fn actual_twap_writer_preserves_both_windows_and_full_integer() {
    let tmp = tempfile::tempdir().unwrap();
    let mut writer =
        TypedParquetSegmentWriter::create_for_current_source(tmp.path(), "twap", TWAP_SOURCE)
            .unwrap();
    let rows = [30_u64, 60]
        .into_iter()
        .enumerate()
        .map(|(i, window_s)| RtdsChainlinkTwapHourlyRow {
            common: RtdsChainlinkBtcUsdHourlyRow {
                timestamp_received: RECEIVED,
                message_timestamp: RECEIVED - 1,
                payload_timestamp: RECEIVED - 2,
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
                freshness_status: "fresh".to_owned(),
                subscription_filter: "{\"symbol\":\"btc/usd\"}".to_owned(),
                source_native_payload_json: "{}".to_owned(),
                collector_host_id: "host".to_owned(),
                collector_region: "hk".to_owned(),
                source_endpoint: "wss://ws-live-data.polymarket.com".to_owned(),
                schema_version: POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1.to_owned(),
            },
            full_accuracy_value: "64950014218511440012345".to_owned(),
            window_s,
            source_sequence_epoch_id: "rtds-epoch".to_owned(),
            collector_admission_sequence: i as u64 + 1,
            collector_admission_time_utc_ms: RECEIVED + 1,
        })
        .collect::<Vec<_>>();
    writer
        .write_rtds_chainlink_twap_rows_with_logical_bytes(&rows, 100)
        .unwrap();
    let output = writer.finalize().unwrap();
    let stream = open_parquet_with_decoder(
        ParquetInputFacts {
            path: &output.final_path,
            source_kind: TWAP_SOURCE,
            schema_version: POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1,
            bytes: fs::metadata(&output.final_path).unwrap().len(),
            rows: output.row_count,
            sha256: &output.sha256,
        },
        1,
        None,
        None,
        expected_public_schema,
        decode_public_batch,
    )
    .unwrap();
    let decoded = stream
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for (i, row) in decoded.iter().enumerate() {
        let TypedRawRowV2::PolymarketRtdsChainlinkTwap(row) = row else {
            panic!()
        };
        assert_eq!(row.window_s, [30, 60][i]);
        assert_eq!(row.full_accuracy_value, "64950014218511440012345");
        assert_eq!(row.common.value, "64950.01");
        assert_eq!(row.collector_admission_sequence, i as u64 + 1);
    }
}

#[test]
fn a_price_column_cannot_silently_change_to_float_or_another_schema_version() {
    let schema = polymarket_market_metadata_v6_schema();
    let batch =
        polymarket_market_metadata_v6_rows_to_batch(schema.clone(), &[anchor_row()]).unwrap();
    let price = schema.index_of("price_to_beat_decimal").unwrap();
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[price] = Field::new("price_to_beat_decimal", DataType::Float64, true);
    let mut columns = batch.columns().to_vec();
    columns[price] = Arc::new(arrow_array::Float64Array::from(vec![64950.01])) as ArrayRef;
    let invalid = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(matches!(
        decode_public_batch(
            METADATA_SOURCE,
            POLYMARKET_MARKET_METADATA_SCHEMA_V6,
            &invalid,
            0,
            None
        ),
        Err(TypedRawReadError::PhysicalSchema { .. })
    ));
    let mut columns = batch.columns().to_vec();
    columns[schema.index_of("schema_version").unwrap()] =
        Arc::new(StringArray::from(vec![METADATA_V5]));
    let invalid = RecordBatch::try_new(schema, columns).unwrap();
    assert!(
        decode_public_batch(
            METADATA_SOURCE,
            POLYMARKET_MARKET_METADATA_SCHEMA_V6,
            &invalid,
            0,
            None
        )
        .is_err()
    );
}

#[test]
fn historical_rows_cross_parquet_batches_without_splitting_the_actual_response() {
    use contracts::MarketAnchorSourceV1;
    use data_pipeline_core::public_reference::historical_response_source_evidence_v3;
    use parquet::arrow::ArrowWriter;

    let tmp = tempfile::tempdir().unwrap();
    let original_dir = tmp
        .path()
        .join("2026-07-30/typed_raw/utc_day=2026-07-30/source_kind=polymarket_market_metadata");
    fs::create_dir_all(&original_dir).unwrap();
    let original = old_metadata();
    let mut original_writer = TypedParquetSegmentWriter::create_for_typed_source(
        &original_dir,
        "polymarket_market_metadata_00",
        METADATA_V5,
        METADATA_SOURCE,
    )
    .unwrap();
    original_writer
        .write_polymarket_market_metadata_v5_rows(&[original.clone()])
        .unwrap();
    let original_file = finalize_with_fact(original_writer, METADATA_SOURCE, METADATA_V5, 1);
    let original_manifest = manifest_for(
        tmp.path(),
        &original_file,
        METADATA_SOURCE,
        METADATA_V5,
        false,
    );
    let original_manifest_sha = format!(
        "{:x}",
        Sha256::digest(fs::read(&original_manifest).unwrap())
    );
    let root = tmp.path().join("repair");
    fs::create_dir(&root).unwrap();
    let response_bytes = b"{\"eventMetadata\":{\"priceToBeat\":64950.014218511440012345}}";
    fs::write(root.join("response.json"), response_bytes).unwrap();
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".to_owned(),
        request_url: "https://gamma-api.polymarket.com/events".to_owned(),
        received_at_utc: "2026-09-08T04:00:00.123456Z".to_owned(),
        received_at_utc_ms: 1_788_840_000_123,
        receipt_observation: "curl_completed".to_owned(),
        response_body_sha256: format!("{:x}", Sha256::digest(response_bytes)),
        response_body_path: "response.json".to_owned(),
        original_response_file: "/retained/response.json".to_owned(),
        http_status_code: None,
        response_url: None,
    };
    let group = historical_response_source_evidence_v3(&response)
        .unwrap()
        .source_message_id
        .0;
    let identity = HistoricalIdentityRowEvidenceV1 {
        manifest_path: original_manifest.to_str().unwrap().to_owned(),
        manifest_sha256: original_manifest_sha.clone(),
        parquet_path: original_file.final_path.to_str().unwrap().to_owned(),
        parquet_sha256: original_file.sha256.clone(),
        row_index: 0,
        schema_version: METADATA_V5.to_owned(),
        metadata_row_kind: "metadata_snapshot".to_owned(),
        row_received_at_utc_ms: RECEIVED,
        fixed_identity_verified_at_utc_ms: RECEIVED - 1,
        source_evidence_origin: Some("gamma_markets_discovery".to_owned()),
        source_endpoint: Some("https://gamma-api.polymarket.com/markets".to_owned()),
        source_evidence_received_at_utc_ms: Some(RECEIVED),
        source_evidence_raw_sha256: Some(format!(
            "{:x}",
            Sha256::digest(br#"{"orderPriceMinTickSize":"0.01"}"#)
        )),
        collector_admission_sequence: Some(1),
        collector_admission_time_utc_ms: Some(RECEIVED + 1),
    };
    // 数值成员下标 2 与 10 分属两个读取批次；输入顺序不应被误当成字符串排序。
    let rows = [10_u32, 2].map(|member| HistoricalPriceAnchorRepairRowV1 {
        schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.to_owned(),
        condition_id: "condition".to_owned(),
        clob_market_id: "123".to_owned(),
        slug: "btc-updown-5m-test".to_owned(),
        asset_ids_json: "[\"1\",\"2\"]".to_owned(),
        market_start_at_utc_ms: RECEIVED + 100,
        market_end_at_utc_ms: RECEIVED + 300100,
        anchor_source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
        price_to_beat_raw: "64950.014218511440012345".to_owned(),
        response_group_id: group.clone(),
        response_member_index: member,
        response_received_at_utc_ms: response.received_at_utc_ms,
        response_sha256: response.response_body_sha256.clone(),
        source_value_path: format!("/{member}/eventMetadata/priceToBeat"),
        source_event_at_utc_ms: None,
        historical_identity_ready_at_utc_ms: RECEIVED - 1,
        historical_identity_evidence_ids_json: "[\"original-row\"]".to_owned(),
        repair_policy_version: "price_anchor_historical_repair_v1".to_owned(),
    });
    let schema =
        expected_public_schema(HISTORY_SOURCE, HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1).unwrap();
    let values = rows
        .iter()
        .map(|row| serde_json::to_value(row).unwrap())
        .collect::<Vec<_>>();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let cells = values
                .iter()
                .map(|value| &value[field.name()])
                .collect::<Vec<_>>();
            match field.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(
                    cells.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                )) as ArrayRef,
                DataType::UInt32 => Arc::new(UInt32Array::from(
                    cells
                        .iter()
                        .map(|value| value.as_u64().map(|value| value as u32))
                        .collect::<Vec<_>>(),
                )),
                DataType::Timestamp(..) => Arc::new(
                    TimestampMillisecondArray::from(
                        cells.iter().map(|value| value.as_i64()).collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
                _ => panic!(),
            }
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let output = root.join("anchors.parquet");
    let mut writer = ArrowWriter::try_new(File::create(&output).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let manifest = json!({
        "schema_version":HISTORICAL_PRICE_ANCHOR_REPAIR_MANIFEST_SCHEMA_V1,
        "response_groups":{group.clone():response}, "identity_evidence":{"original-row":identity},
        "input_manifests":[{"path":original_manifest,"sha256":original_manifest_sha}],
        "input_metadata_files":[{"path":original_file.final_path,"sha256":original_file.sha256,
            "rows":1,"bytes":fs::metadata(&original_file.final_path).unwrap().len(),"manifest_path":original_manifest}],
        "outputs":[{"path":"anchors.parquet","sha256":format!("{:x}",Sha256::digest(fs::read(&output).unwrap())),
            "bytes":fs::metadata(&output).unwrap().len(),"rows":2,"schema_version":HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1}],
    });
    let manifest_path = root.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let read = read_historical_price_anchor_repair_v1(&manifest_path, 1).unwrap();
    assert_eq!(read.batches.len(), 1);
    assert_eq!(
        read.batches[0]
            .rows
            .iter()
            .map(|row| row.response_member_index)
            .collect::<Vec<_>>(),
        vec![2, 10]
    );
    assert_eq!(read.batches[0].response.http_status_code, None);
    assert_eq!(read.batches[0].response.response_url, None);
    assert_eq!(
        read.batches[0].response.receipt_observation,
        "curl_completed"
    );
    assert_eq!(
        read.identity_source_evidence["original-row"].collector_admission,
        None
    );
    assert_eq!(
        read.identity_source_evidence["original-row"].received_at_utc_ms,
        RECEIVED
    );
    assert_eq!(
        read.batches[0].rows[0].price_to_beat_raw,
        "64950.014218511440012345"
    );
    fs::write(root.join("response.json"), b"changed response").unwrap();
    assert!(read_historical_price_anchor_repair_v1(&manifest_path, 1).is_err());
}

#[test]
fn new_source_files_require_the_twap_writer_finalize_ack() {
    for source in [METADATA_SOURCE, TWAP_SOURCE] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(format!(
            "2026-07-30/typed_raw/utc_day=2026-07-30/source_kind={source}"
        ));
        fs::create_dir_all(&dir).unwrap();
        let mut writer = TypedParquetSegmentWriter::create_for_current_source(
            &dir,
            &format!("{source}_00"),
            source,
        )
        .unwrap();
        if source == METADATA_SOURCE {
            writer
                .write_polymarket_market_metadata_v6_rows(&[anchor_row()])
                .unwrap();
        }
        let version = if source == METADATA_SOURCE {
            POLYMARKET_MARKET_METADATA_SCHEMA_V6
        } else {
            POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1
        };
        let segment = finalize_with_fact(
            writer,
            source,
            version,
            u64::from(source == METADATA_SOURCE),
        );
        let path = manifest_for(tmp.path(), &segment, source, version, true);
        crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &path, true)
            .unwrap();
        let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        manifest["writer_finalize_acks"]
            .as_array_mut()
            .unwrap()
            .retain(|value| value.as_str() != Some(TWAP_SOURCE));
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(
            matches!(
                crate::validate_daily_manifest_admission_with_public_inputs(
                    tmp.path(),
                    &path,
                    true
                ),
                Err(
                    crate::ManifestAdmissionValidationError::BarrierEvidenceMismatch {
                        evidence_kind: "writer_finalize_acks"
                    }
                ),
            ),
            "{source} 缺少第七个写入完成确认必须拒绝"
        );
    }
}

#[test]
fn a_new_day_without_parquet_still_requires_all_seven_writer_acks() {
    use data_collect::daily_manifest::{
        SourceOperationalCounterDelta, add_manifest_operational_counters,
    };
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp
        .path()
        .join("2026-07-30/typed_raw/utc_day=2026-07-30/source_kind=polymarket_market_metadata");
    fs::create_dir_all(&dir).unwrap();
    let writer = TypedParquetSegmentWriter::create_for_current_source(
        &dir,
        "polymarket_market_metadata_00",
        METADATA_SOURCE,
    )
    .unwrap();
    let segment = finalize_with_fact(
        writer,
        METADATA_SOURCE,
        POLYMARKET_MARKET_METADATA_SCHEMA_V6,
        0,
    );
    let path = manifest_for(
        tmp.path(),
        &segment,
        METADATA_SOURCE,
        POLYMARKET_MARKET_METADATA_SCHEMA_V6,
        true,
    );
    let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest["files"] = json!([]);
    fs::remove_file(&segment.final_path).unwrap();
    manifest["state"] = json!("open");
    manifest["build_commit"] = json!("fixture-build");
    manifest["collector_binary_sha256"] = json!("a".repeat(64));
    manifest["config_digest"] = json!("b".repeat(64));
    manifest["collector_host_id"] = json!("fixture-host");
    manifest["collector_region"] = json!("fixture-region");
    let deltas = crate::PUBLIC_WRITER_FINALIZE_ACKS
        .iter()
        .map(|source| {
            (
                (*source).to_owned(),
                SourceOperationalCounterDelta::default(),
            )
        })
        .collect();
    add_manifest_operational_counters(path.parent().unwrap(), &mut manifest, &deltas).unwrap();
    assert_eq!(manifest["source_counters"][TWAP_SOURCE]["rows"], json!(0));
    manifest["state"] = json!("closed");
    fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &path, true).unwrap();
    manifest["writer_finalize_acks"]
        .as_array_mut()
        .unwrap()
        .retain(|value| value.as_str() != Some(TWAP_SOURCE));
    fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(
        matches!(
            crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &path, true),
            Err(
                crate::ManifestAdmissionValidationError::BarrierEvidenceMismatch {
                    evidence_kind: "writer_finalize_acks"
                }
            ),
        ),
        "没有 Parquet 的新日仍有第七来源的零计数事实"
    );
    manifest["source_counters"]
        .as_object_mut()
        .unwrap()
        .remove(TWAP_SOURCE);
    fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    crate::validate_daily_manifest_admission_with_public_inputs(tmp.path(), &path, true).unwrap();
    crate::validate_daily_manifest_admission_at_root(tmp.path(), &path).unwrap();
}
