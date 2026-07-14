use arrow_schema::DataType;
use data_collect::polymarket_market_metadata_layout::{
    PolymarketMarketMetadataContext, PolymarketMarketMetadataRow, polymarket_market_metadata_schema,
};

fn context() -> PolymarketMarketMetadataContext {
    PolymarketMarketMetadataContext {
        timestamp_received_ms: 1_783_392_404_599,
        metadata_fetch_start_utc_ms: Some(1_783_392_404_100),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
        schema_version: "polymarket_market_metadata_v2".to_owned(),
    }
}

#[test]
fn new_market_metadata_row_keeps_condition_tokens_and_source() {
    let row = PolymarketMarketMetadataRow::new_market(
        "btc-updown-5m-1783392300",
        "0x0000000000000000000000000000000000000000000000000000000000000001",
        Some("clob-market-1"),
        &[
            "106585164761922456203746651621390029417453862034640469075081961934906147433548"
                .to_owned(),
            "206585164761922456203746651621390029417453862034640469075081961934906147433549"
                .to_owned(),
        ],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(true),
        Some(true),
        "prefetched",
        Some("0.01"),
        Some("1"),
        Some(r#"{"maker":0}"#),
        Some(r#"{"eligible":true}"#),
        Some(true),
        context(),
    );

    assert_eq!(row.metadata_source, "new_market");
    assert_eq!(row.slug, "btc-updown-5m-1783392300");
    assert_eq!(
        row.asset_ids.as_deref(),
        Some(
            r#"["106585164761922456203746651621390029417453862034640469075081961934906147433548","206585164761922456203746651621390029417453862034640469075081961934906147433549"]"#
        )
    );
    assert_eq!(row.same_condition_market_verified, Some(true));
    assert!(row.validate().is_complete());
}

#[test]
fn active_scan_and_fallback_close_metadata_rows_are_complete_with_evidence() {
    let active_scan = PolymarketMarketMetadataRow::active_scan(
        "btc-updown-5m-1783392300",
        "condition-1",
        Some("clob-market-1"),
        &["asset-1".to_owned(), "asset-2".to_owned()],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(true),
        Some(true),
        "active",
        Some("0.01"),
        Some("1"),
        Some(r#"{"maker":0}"#),
        Some(r#"{"eligible":true}"#),
        Some(true),
        context(),
    );
    assert_eq!(active_scan.metadata_source, "active_scan");
    assert!(active_scan.validate().is_complete());

    let fallback = PolymarketMarketMetadataRow::fallback_close(
        "btc-updown-5m-1783392300",
        "condition-1",
        Some("clob-market-1"),
        &["asset-1".to_owned(), "asset-2".to_owned()],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(false),
        Some(false),
        "retired",
        Some("0.01"),
        Some("1"),
        Some(r#"{"maker":0}"#),
        Some(r#"{"eligible":true}"#),
        Some(true),
        context(),
    );
    assert_eq!(fallback.metadata_source, "fallback_close");
    assert!(fallback.validate().is_complete());
}

#[test]
fn slug_only_candidate_is_not_complete_discovery() {
    let row = PolymarketMarketMetadataRow::candidate_slug_only(
        "btc-updown-5m-1783392300",
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        context(),
    );

    let report = row.validate();
    assert!(!report.is_complete());
    assert!(
        report
            .source_gaps
            .contains(&"slug_only_discovery".to_owned())
    );
    assert!(report.missing_fields.contains(&"condition_id".to_owned()));
    assert!(report.missing_fields.contains(&"asset_ids".to_owned()));
}

#[test]
fn schema_contains_metadata_columns() {
    let schema = polymarket_market_metadata_schema();
    for field in [
        "timestamp_received",
        "metadata_fetch_start_utc",
        "slug",
        "condition_id",
        "clob_market_id",
        "asset_ids",
        "market_start_utc",
        "market_end_utc",
        "active",
        "accepting_orders",
        "market_lifecycle_state",
        "tick_size",
        "min_size",
        "fee_schedule_raw",
        "reward_eligibility_raw",
        "metadata_unavailable_reason",
        "metadata_source",
        "same_condition_market_verified",
    ] {
        schema.field_with_name(field).expect(field);
    }
    assert!(schema.field_with_name("metadata_recv_time_utc").is_err());
    assert!(schema.field_with_name("token_ids").is_err());
    assert!(matches!(
        schema
            .field_with_name("timestamp_received")
            .expect("timestamp_received")
            .data_type(),
        DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC"
    ));
}
