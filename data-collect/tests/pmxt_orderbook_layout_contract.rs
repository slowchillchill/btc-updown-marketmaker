use arrow_schema::DataType;
use data_collect::polymarket_orderbook_layout::{
    PolymarketOrderbookContext, PolymarketOrderbookRow, polymarket_orderbook_schema,
};
use polymarket_client_sdk_v2::clob::ws::WsMessage;

fn context() -> PolymarketOrderbookContext {
    PolymarketOrderbookContext {
        timestamp_received_ms: 1_783_392_404_599,
        slug: Some("btc-updown-5m-1783392300".to_owned()),
        source_native_payload_json: r#"{"source":"polymarket_market","event_type":"book"}"#
            .to_owned(),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        schema_version: "polymarket_orderbook_v2".to_owned(),
    }
}

fn parse_ws_message(raw: &str) -> WsMessage {
    serde_json::from_str(raw).expect("sdk parses market channel message")
}

#[test]
fn sdk_book_row_preserves_full_depth_as_pmxt_price_size_arrays() {
    let WsMessage::Book(book) = parse_ws_message(
        r#"{
            "event_type": "book",
            "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "timestamp": "1783392404599",
            "bids": [{"price": "0.49", "size": "120.5"}],
            "asks": [{"price": "0.51", "size": "88"}],
            "hash": "book-hash"
        }"#,
    ) else {
        panic!("expected book message");
    };

    let row = PolymarketOrderbookRow::from_sdk_book(&book, context())
        .expect("book with full depth is valid");

    assert_eq!(row.event_type, "book");
    assert_eq!(row.timestamp, Some(1_783_392_404_599));
    assert_eq!(row.bids.as_deref(), Some(r#"[["0.49","120.5"]]"#));
    assert_eq!(row.asks.as_deref(), Some(r#"[["0.51","88"]]"#));
    assert_eq!(row.hash.as_deref(), Some("book-hash"));
    assert!(row.validate().is_complete());
}

#[test]
fn shrunken_book_raw_evidence_with_only_depth_lengths_is_incomplete() {
    let raw = r#"{
        "event_type": "book",
        "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
        "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
        "timestamp": 1783392404599,
        "bids_len": 12,
        "asks_len": 10,
        "hash": "book-hash"
    }"#;

    let err = PolymarketOrderbookRow::book_from_raw_json(raw, context()).unwrap_err();
    assert!(
        err.contains("bids") && err.contains("asks"),
        "error must identify missing full depth arrays: {err}"
    );
}

#[test]
fn price_change_rows_keep_price_size_side_best_bid_best_ask() {
    let WsMessage::PriceChange(price_change) = parse_ws_message(
        r#"{
            "event_type": "price_change",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1783392404600",
            "price_changes": [{
                "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
                "price": "0.52",
                "size": "10",
                "side": "BUY",
                "hash": "price-hash",
                "best_bid": "0.51",
                "best_ask": "0.53"
            }]
        }"#,
    ) else {
        panic!("expected price_change message");
    };

    let rows = PolymarketOrderbookRow::from_sdk_price_change(&price_change, context())
        .expect("price_change rows");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    assert_eq!(row.event_type, "price_change");
    assert_eq!(row.price.as_deref(), Some("0.52"));
    assert_eq!(row.size.as_deref(), Some("10"));
    assert_eq!(row.side.as_deref(), Some("BUY"));
    assert_eq!(row.best_bid.as_deref(), Some("0.51"));
    assert_eq!(row.best_ask.as_deref(), Some("0.53"));
    assert!(row.validate().is_complete());
}

#[test]
fn target_event_rows_keep_event_specific_nullable_fields() {
    let tick = PolymarketOrderbookRow::tick_size_change(
        "0x0000000000000000000000000000000000000000000000000000000000000003",
        "106585164761922456203746651621390029417453862034640469075081961934906147433548",
        1_783_392_404_601,
        "0.01",
        "0.001",
        context(),
    );
    assert_eq!(tick.old_tick_size.as_deref(), Some("0.01"));
    assert_eq!(tick.new_tick_size.as_deref(), Some("0.001"));
    assert!(tick.validate().is_complete());

    let last_trade = PolymarketOrderbookRow::last_trade_price(
        "0x0000000000000000000000000000000000000000000000000000000000000003",
        "106585164761922456203746651621390029417453862034640469075081961934906147433548",
        1_783_392_404_602,
        "0.50",
        Some("11"),
        Some("SELL"),
        Some("2.5"),
        Some("0xabc"),
        context(),
    );
    assert_eq!(last_trade.price.as_deref(), Some("0.50"));
    assert_eq!(last_trade.fee_rate_bps.as_deref(), Some("11"));
    assert_eq!(last_trade.transaction_hash.as_deref(), Some("0xabc"));
    assert!(last_trade.validate().is_complete());

    let resolved = PolymarketOrderbookRow::market_resolved(
        "0x0000000000000000000000000000000000000000000000000000000000000003",
        1_783_392_404_603,
        "106585164761922456203746651621390029417453862034640469075081961934906147433548",
        "Up",
        Some("resolved"),
        &[
            "106585164761922456203746651621390029417453862034640469075081961934906147433548"
                .to_owned(),
            "206585164761922456203746651621390029417453862034640469075081961934906147433549"
                .to_owned(),
        ],
        context(),
    );
    assert_eq!(
        resolved.winning_asset_id.as_deref(),
        Some("106585164761922456203746651621390029417453862034640469075081961934906147433548")
    );
    assert_eq!(resolved.winning_outcome.as_deref(), Some("Up"));
    assert!(resolved.resolved_event_asset_ids.is_some());
    assert!(resolved.validate().is_complete());
}

#[test]
fn schema_contains_pmxt_and_project_fields() {
    let schema = polymarket_orderbook_schema();

    for field in [
        "timestamp_received",
        "timestamp",
        "market",
        "event_type",
        "source_native_payload_json",
        "asset_id",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
        "collector_host_id",
        "collector_region",
        "source_endpoint",
        "schema_version",
    ] {
        schema.field_with_name(field).expect(field);
    }

    assert!(matches!(
        schema
            .field_with_name("timestamp_received")
            .expect("timestamp_received")
            .data_type(),
        DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC"
    ));
}
