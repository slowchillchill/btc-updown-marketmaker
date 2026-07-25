use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Schema, TimeUnit};
use data_collect::polymarket_orderbook_v3::{
    POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketOrderbookV3Admission,
    PolymarketOrderbookV3Event, PolymarketOrderbookV3Projection,
    polymarket_orderbook_v3_messages_from_batch, polymarket_orderbook_v3_messages_to_batch,
    polymarket_orderbook_v3_schema,
};
use polymarket_client_sdk_v2::clob::ws::WsMessage;
use polymarket_client_sdk_v2::clob::ws::interest::MessageInterest;
use polymarket_client_sdk_v2::clob::ws::types::response::parse_if_interested;

#[test]
fn polymarket_orderbook_v3_schema_contract_is_exact() {
    let schema = polymarket_orderbook_v3_schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.is_nullable()))
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            ("timestamp_received", false),
            ("timestamp", true),
            ("event_type", false),
            ("market", false),
            ("slug", true),
            ("collector_host_id", false),
            ("collector_region", false),
            ("source_endpoint", false),
            ("schema_version", false),
            ("source_sequence_epoch_id", false),
            ("collector_admission_sequence", false),
            ("collector_admission_time_utc_ms", false),
            ("asset_id", true),
            ("bids", true),
            ("asks", true),
            ("last_trade_price", true),
            ("price_changes", true),
            ("old_tick_size", true),
            ("new_tick_size", true),
            ("price", true),
            ("size", true),
            ("side", true),
            ("fee_rate_bps", true),
            ("best_bid", true),
            ("best_ask", true),
            ("spread", true),
            ("clob_market_id", true),
            ("question", true),
            ("description", true),
            ("asset_ids", true),
            ("outcomes", true),
            ("event_message", true),
            ("winning_asset_id", true),
            ("winning_outcome", true),
        ]
    );

    for name in [
        "timestamp_received",
        "timestamp",
        "collector_admission_time_utc_ms",
    ] {
        assert!(matches!(
            schema.field_with_name(name).unwrap().data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
                if timezone.as_ref() == "UTC"
        ));
    }
    assert!(matches!(
        schema
            .field_with_name("collector_admission_sequence")
            .unwrap()
            .data_type(),
        DataType::UInt64
    ));
    for name in [
        "event_type",
        "market",
        "slug",
        "collector_host_id",
        "collector_region",
        "source_endpoint",
        "schema_version",
        "source_sequence_epoch_id",
        "asset_id",
        "last_trade_price",
        "old_tick_size",
        "new_tick_size",
        "price",
        "size",
        "side",
        "fee_rate_bps",
        "best_bid",
        "best_ask",
        "spread",
        "clob_market_id",
        "question",
        "description",
        "winning_asset_id",
        "winning_outcome",
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Utf8,
            "{name}"
        );
    }

    assert_list_struct_children(&schema, "bids", &[("price", false), ("size", false)]);
    assert_list_struct_children(&schema, "asks", &[("price", false), ("size", false)]);
    assert_list_struct_children(
        &schema,
        "price_changes",
        &[
            ("asset_id", false),
            ("price", false),
            ("size", true),
            ("side", false),
            ("best_bid", true),
            ("best_ask", true),
        ],
    );
    for name in ["asset_ids", "outcomes"] {
        let DataType::List(item) = schema.field_with_name(name).unwrap().data_type() else {
            panic!("{name} must be a list");
        };
        assert_eq!(item.name(), "item");
        assert!(!item.is_nullable());
        assert_eq!(item.data_type(), &DataType::Utf8);
    }

    let DataType::Struct(event_message) =
        schema.field_with_name("event_message").unwrap().data_type()
    else {
        panic!("event_message must be a struct");
    };
    assert_eq!(
        event_message
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        vec![
            ("id", false),
            ("ticker", true),
            ("slug", true),
            ("title", true),
            ("description", true),
        ]
    );

    for excluded in [
        "id",
        "source_native_payload_json",
        "hash",
        "transaction_hash",
    ] {
        assert!(schema.field_with_name(excluded).is_err());
    }
}

fn assert_list_struct_children(
    schema: &arrow_schema::Schema,
    name: &str,
    expected: &[(&str, bool)],
) {
    let DataType::List(item) = schema.field_with_name(name).unwrap().data_type() else {
        panic!("{name} must be a list");
    };
    assert_eq!(item.name(), "item");
    assert!(!item.is_nullable());
    let DataType::Struct(children) = item.data_type() else {
        panic!("{name} items must be structs");
    };
    assert_eq!(
        children
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn polymarket_v3_seven_sdk_variants_build_one_target_admitted_domain_each() {
    let cases = [
        (
            "book",
            r#"{
                "event_type":"book",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "timestamp":null,
                "bids":[
                    {"price":"0.49","size":"10"},
                    {"price":"0.48","size":"12"}
                ],
                "asks":[],
                "last_trade_price":"0.50"
            }"#,
        ),
        (
            "price_change",
            r#"{
                "event_type":"price_change",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "timestamp":"1783392404600",
                "price_changes":[
                    {"asset_id":"101","price":"0.50","size":"10","side":"BUY","best_bid":"0.49"},
                    {"asset_id":"102","price":"0.51","size":null,"side":"SELL","best_ask":"0.52"}
                ]
            }"#,
        ),
        (
            "tick_size_change",
            r#"{
                "event_type":"tick_size_change",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "old_tick_size":null,
                "new_tick_size":"0.001",
                "timestamp":null
            }"#,
        ),
        (
            "last_trade_price",
            r#"{
                "event_type":"last_trade_price",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "price":"0.50",
                "side":null,
                "size":null,
                "fee_rate_bps":null,
                "timestamp":null
            }"#,
        ),
        (
            "best_bid_ask",
            r#"{
                "event_type":"best_bid_ask",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "best_bid":null,
                "best_ask":null,
                "spread":null,
                "timestamp":null
            }"#,
        ),
        (
            "new_market",
            r#"{
                "event_type":"new_market",
                "id":"market-current",
                "question":null,
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug":"btc-updown-5m-1783392300",
                "description":null,
                "assets_ids":["101","102"],
                "outcomes":["Up","Down"],
                "event_message":{
                    "id":"event-1",
                    "ticker":null,
                    "slug":"btc-updown",
                    "title":null,
                    "description":"event description"
                },
                "timestamp":null
            }"#,
        ),
        (
            "market_resolved",
            r#"{
                "event_type":"market_resolved",
                "id":"market-current",
                "question":null,
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug":null,
                "description":null,
                "assets_ids":["101","102"],
                "outcomes":["Up","Down"],
                "winning_asset_id":"101",
                "winning_outcome":"Up",
                "event_message":null,
                "timestamp":null
            }"#,
        ),
    ];

    let mut admitted = Vec::with_capacity(cases.len());
    for (index, (expected_event_type, raw)) in cases.into_iter().enumerate() {
        let mut parsed = parse_if_interested(raw.as_bytes(), &MessageInterest::ALL)
            .unwrap_or_else(|error| panic!("{expected_event_type} must decode: {error}"));
        assert_eq!(parsed.len(), 1, "{expected_event_type}");
        let sdk_message = parsed.pop().unwrap();
        let projection =
            PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
                .unwrap()
                .unwrap_or_else(|| panic!("{expected_event_type} must be a public market event"));
        let message = projection
            .admit(admission(u64::try_from(index + 1).unwrap()))
            .unwrap();

        assert_eq!(message.event_type(), expected_event_type);
        assert_eq!(
            message.envelope.schema_version,
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION
        );
        assert_eq!(
            message.envelope.collector_admission_sequence,
            u64::try_from(index + 1).unwrap()
        );
        admitted.push(message);
    }

    let batch = polymarket_orderbook_v3_messages_to_batch(&admitted).unwrap();
    assert_eq!(batch.num_rows(), 7);
    assert_eq!(
        polymarket_orderbook_v3_messages_from_batch(&batch).unwrap(),
        admitted
    );
}

#[test]
fn polymarket_v3_price_change_keeps_one_message_and_sdk_entry_order() {
    let mut parsed = parse_if_interested(
        r#"{
            "event_type":"price_change",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp":"1783392404600",
            "price_changes":[
                {"asset_id":"102","price":"0.51","size":"3","side":"SELL"},
                {"asset_id":"101","price":"0.49","size":"4","side":"BUY"},
                {"asset_id":"102","price":"0.52","size":"5","side":"SELL"}
            ]
        }"#
        .as_bytes(),
        &MessageInterest::ALL,
    )
    .unwrap();
    assert_eq!(parsed.len(), 1);
    let sdk_message = parsed.pop().unwrap();
    let message = PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
        .unwrap()
        .unwrap()
        .admit(admission(1))
        .unwrap();
    let batch = polymarket_orderbook_v3_messages_to_batch(std::slice::from_ref(&message)).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        polymarket_orderbook_v3_messages_from_batch(&batch).unwrap(),
        std::slice::from_ref(&message)
    );
    let PolymarketOrderbookV3Event::PriceChange(price_change) = &message.event else {
        panic!("expected price_change domain");
    };
    assert_eq!(
        price_change
            .price_changes
            .iter()
            .map(|change| change.asset_id.as_str())
            .collect::<Vec<_>>(),
        ["102", "101", "102"]
    );
}

#[test]
fn polymarket_v3_platform_duplicates_get_distinct_keys_and_retry_reuses_key() {
    let sdk_message = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "timestamp":"1783392404600",
            "bids":[],
            "asks":[]
        }"#,
    )
    .unwrap();
    let projection =
        PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
            .unwrap()
            .unwrap();
    let first = projection.clone().admit(admission(1)).unwrap();
    let duplicate = projection.admit(admission(2)).unwrap();
    let internal_retry = first.clone();

    assert_ne!(
        first.envelope.collector_admission_sequence,
        duplicate.envelope.collector_admission_sequence
    );
    assert_eq!(
        first.envelope.collector_admission_sequence,
        internal_retry.envelope.collector_admission_sequence
    );
    assert_eq!(
        first.envelope.source_sequence_epoch_id,
        internal_retry.envelope.source_sequence_epoch_id
    );
}

#[test]
fn polymarket_v3_reader_fails_closed_on_unknown_corrupt_or_contradictory_rows() {
    let sdk_message = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "timestamp":"1783392404600",
            "bids":[],
            "asks":[]
        }"#,
    )
    .unwrap();
    let message = PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
        .unwrap()
        .unwrap()
        .admit(admission(1))
        .unwrap();
    let batch = polymarket_orderbook_v3_messages_to_batch(&[message]).unwrap();

    let unknown_event = replace_utf8_column(&batch, "event_type", Some("future_variant"));
    assert!(
        polymarket_orderbook_v3_messages_from_batch(&unknown_event)
            .unwrap_err()
            .to_string()
            .contains("unknown Polymarket v3 persisted event discriminator")
    );

    let wrong_version =
        replace_utf8_column(&batch, "schema_version", Some("polymarket_orderbook_v999"));
    assert!(
        polymarket_orderbook_v3_messages_from_batch(&wrong_version)
            .unwrap_err()
            .to_string()
            .contains("unexpected schema version")
    );

    let missing_required = replace_utf8_column(&batch, "asset_id", None);
    assert!(
        polymarket_orderbook_v3_messages_from_batch(&missing_required)
            .unwrap_err()
            .to_string()
            .contains("required column asset_id is null")
    );

    let contradiction = replace_utf8_column(&batch, "price", Some("0.50"));
    assert!(
        polymarket_orderbook_v3_messages_from_batch(&contradiction)
            .unwrap_err()
            .to_string()
            .contains("contradictory field price")
    );

    let schema = batch.schema();
    let DataType::List(item) = schema.field_with_name("bids").unwrap().data_type() else {
        panic!("bids must be a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("bids items must be structs");
    };
    let null_nested_child = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("10")])) as ArrayRef,
        ],
        None,
    )
    .unwrap_err();
    assert!(
        null_nested_child
            .to_string()
            .contains("unmasked nulls for non-nullable StructArray field")
    );

    let retained_fields = batch
        .schema()
        .fields()
        .iter()
        .take(batch.num_columns() - 1)
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let missing_column = RecordBatch::try_new(
        Arc::new(Schema::new(retained_fields)),
        batch
            .columns()
            .iter()
            .take(batch.num_columns() - 1)
            .cloned()
            .collect(),
    )
    .unwrap();
    assert!(
        polymarket_orderbook_v3_messages_from_batch(&missing_column)
            .unwrap_err()
            .to_string()
            .contains("exact canonical schema")
    );
}

#[test]
fn polymarket_v3_codec_rejects_non_sdk_lexical_values_and_preserves_decimal_text() {
    let sdk_message = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "timestamp":"1783392404600",
            "bids":[{"price":"0.10","size":"0.1"}],
            "asks":[],
            "last_trade_price":null
        }"#,
    )
    .unwrap();
    let message = PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
        .unwrap()
        .unwrap()
        .admit(admission(1))
        .unwrap();
    let PolymarketOrderbookV3Event::Book(book) = &message.event else {
        panic!("expected book");
    };
    assert_eq!(book.bids[0].price, "0.10");
    assert_eq!(book.bids[0].size, "0.1");
    assert_eq!(book.last_trade_price, None);
    let batch = polymarket_orderbook_v3_messages_to_batch(std::slice::from_ref(&message)).unwrap();
    assert_eq!(
        polymarket_orderbook_v3_messages_from_batch(&batch).unwrap(),
        std::slice::from_ref(&message)
    );

    let mut invalid_market = message.clone();
    invalid_market.envelope.market = "not-a-condition-id".to_owned();
    assert!(
        polymarket_orderbook_v3_messages_to_batch(&[invalid_market])
            .unwrap_err()
            .to_string()
            .contains("SDK-compatible condition id")
    );

    let mut invalid_asset = message.clone();
    let PolymarketOrderbookV3Event::Book(book) = &mut invalid_asset.event else {
        unreachable!();
    };
    book.asset_id = "not-an-asset-id".to_owned();
    assert!(
        polymarket_orderbook_v3_messages_to_batch(&[invalid_asset])
            .unwrap_err()
            .to_string()
            .contains("SDK-compatible asset id")
    );

    let mut invalid_nested_decimal = message.clone();
    let PolymarketOrderbookV3Event::Book(book) = &mut invalid_nested_decimal.event else {
        unreachable!();
    };
    book.bids[0].price = "not-a-decimal".to_owned();
    assert!(
        polymarket_orderbook_v3_messages_to_batch(&[invalid_nested_decimal])
            .unwrap_err()
            .to_string()
            .contains("SDK-compatible decimal text")
    );

    let invalid_optional_decimal =
        replace_utf8_column(&batch, "last_trade_price", Some("not-a-decimal"));
    assert!(
        polymarket_orderbook_v3_messages_from_batch(&invalid_optional_decimal)
            .unwrap_err()
            .to_string()
            .contains("SDK-compatible decimal text")
    );

    let price_change = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"price_change",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp":"1783392404600",
            "price_changes":[
                {"asset_id":"101","price":"0.50","size":"1","side":"BUY"}
            ]
        }"#,
    )
    .unwrap();
    let mut price_change_message =
        PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&price_change)
            .unwrap()
            .unwrap()
            .admit(admission(2))
            .unwrap();
    let PolymarketOrderbookV3Event::PriceChange(price_change) = &mut price_change_message.event
    else {
        unreachable!();
    };
    price_change.price_changes[0].side = "HOLD".to_owned();
    assert!(
        polymarket_orderbook_v3_messages_to_batch(&[price_change_message])
            .unwrap_err()
            .to_string()
            .contains("SDK-compatible side")
    );
}

#[test]
fn polymarket_v3_official_parser_errors_never_become_empty_evidence_rows() {
    let error = parse_if_interested(
        br#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "bids":[],
            "asks":[]
        }"#,
        &MessageInterest::ALL,
    )
    .unwrap_err();
    assert!(!error.to_string().trim().is_empty());
}

fn replace_utf8_column(batch: &RecordBatch, name: &str, value: Option<&str>) -> RecordBatch {
    assert_eq!(batch.num_rows(), 1);
    let index = batch.schema().index_of(name).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(StringArray::from(vec![value])) as ArrayRef;
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn admission(sequence: u64) -> PolymarketOrderbookV3Admission {
    PolymarketOrderbookV3Admission {
        timestamp_received: 1_783_392_404_599,
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        source_sequence_epoch_id: "pm-source-epoch-1".to_owned(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: 1_783_392_404_600,
    }
}
