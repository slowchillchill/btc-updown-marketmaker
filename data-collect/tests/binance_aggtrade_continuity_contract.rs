use data_collect::binance_aggtrade::{
    BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, BinanceAggTradeIngressCoordinate,
    BinanceAggTradeIngressSequenceAllocator, BinanceAggTradeInvalidInput,
    BinanceAggTradeInvalidReason, BinanceAggTradeInvalidTransport, BinanceAggTradeOutput,
    BinanceAggTradeRawCapture, BinanceAggTradeRawCaptureStatus, BinanceWsAggTrade,
};
use serde_json::json;

fn wire(id: u64, price: &str) -> serde_json::Value {
    json!({
        "e": "aggTrade",
        "E": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
        "s": "BTCUSDT",
        "a": id,
        "p": price,
        "q": "0.001",
        "f": id * 10,
        "l": id * 10,
        "T": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
        "m": true,
        "M": false
    })
}

fn parse(
    allocator: &BinanceAggTradeIngressSequenceAllocator,
    id: u64,
    price: &str,
    received: i64,
) -> BinanceWsAggTrade {
    BinanceWsAggTrade::from_wire(
        &wire(id, price),
        "btcusdt@aggTrade",
        received,
        allocator.allocate_websocket().unwrap(),
    )
    .unwrap()
}

fn websocket_sequence(row: &BinanceWsAggTrade) -> u64 {
    let BinanceAggTradeIngressCoordinate::Websocket { sequence } = &row.ingress_coordinate;
    *sequence
}

#[test]
fn id_jump_and_descent_are_immediate_independent_websocket_facts() {
    let allocator = BinanceAggTradeIngressSequenceAllocator::default();
    let rows = [
        parse(&allocator, 100, "65000.10", 1_000),
        parse(&allocator, 103, "65000.30", 1_030),
        parse(&allocator, 99, "64999.90", 1_040),
    ]
    .map(BinanceAggTradeOutput::Websocket);

    assert_eq!(
        rows.iter()
            .map(BinanceAggTradeOutput::aggregate_trade_id)
            .collect::<Vec<_>>(),
        vec![100, 103, 99]
    );
    assert_eq!(
        rows.iter()
            .map(BinanceAggTradeOutput::websocket_ingress_sequence)
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3)]
    );
    assert_eq!(allocator.last_allocated(), 3);
}

#[test]
fn duplicate_and_invalid_frames_still_consume_websocket_ingress_sequence() {
    let allocator = BinanceAggTradeIngressSequenceAllocator::default();
    let original = parse(&allocator, 100, "65000.10", 1_000);
    let duplicate = parse(&allocator, 100, "65000.10", 1_001);
    let invalid_coordinate = allocator.allocate_websocket().unwrap();
    let invalid = BinanceAggTradeInvalidInput::new(
        BinanceAggTradeInvalidTransport::Websocket,
        invalid_coordinate,
        1_002,
        "wss://stream.binance.com/ws/btcusdt@aggTrade".to_owned(),
        BinanceAggTradeInvalidReason::InvalidJson,
        "expected value",
        BinanceAggTradeRawCapture::complete(b"{not-json".to_vec(), None).unwrap(),
    )
    .unwrap();

    assert_eq!(websocket_sequence(&original), 1);
    assert_eq!(websocket_sequence(&duplicate), 2);
    assert_eq!(
        invalid.ingress_coordinate,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 3 }
    );
    assert_eq!(allocator.last_allocated(), 3);
}

#[test]
fn invalid_websocket_frame_preserves_exact_input_between_valid_rows() {
    let allocator = BinanceAggTradeIngressSequenceAllocator::default();
    let before = parse(&allocator, 100, "65000.10", 1_000);
    let invalid = BinanceAggTradeInvalidInput::new(
        BinanceAggTradeInvalidTransport::Websocket,
        allocator.allocate_websocket().unwrap(),
        1_001,
        "wss://stream.binance.com/ws/btcusdt@aggTrade".to_owned(),
        BinanceAggTradeInvalidReason::InvalidJson,
        "expected value",
        BinanceAggTradeRawCapture::complete(b"{not-json".to_vec(), None).unwrap(),
    )
    .unwrap();
    let after = parse(&allocator, 104, "65000.40", 1_002);

    assert_eq!(websocket_sequence(&before), 1);
    assert_eq!(
        invalid.ingress_coordinate,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 2 }
    );
    assert_eq!(invalid.raw_capture.bytes, b"{not-json");
    assert_eq!(
        invalid.raw_capture.payload_sha256.as_deref(),
        Some("f1dec6e9ee608550bd1c39ff2b90134059bac5d02e4e78f6410aed2fbd870bd0")
    );
    assert_eq!(websocket_sequence(&after), 3);
    assert_eq!(after.shared.aggregate_trade_id, 104);
}

#[test]
fn oversize_websocket_frame_records_only_bounded_truthful_evidence() {
    let observed = u64::try_from(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES).unwrap() + 1;
    let capture = BinanceAggTradeRawCapture::oversize_not_captured(observed, None).unwrap();
    assert_eq!(
        capture.status,
        BinanceAggTradeRawCaptureStatus::OversizeNotCaptured
    );
    assert!(capture.bytes.is_empty());
    assert_eq!(capture.observed_bytes_at_least, observed);
    assert!(capture.payload_sha256.is_none());
}

#[test]
fn production_has_no_aggtrade_rest_recovery_path() {
    fn collect_rust_sources(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect_rust_sources(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    let crate_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut production_files = vec![crate_root.join("Cargo.toml")];
    collect_rust_sources(&crate_root.join("src"), &mut production_files);

    let forbidden = [
        "/api/v3/aggTrades",
        "BINANCE_AGGTRADE_REST",
        "BinanceRestAggTrade",
        "AggTradeBackfill",
        "BudgetedAggTradeBackfill",
        "PendingAggTrade",
        "AggTradeCoordinator",
        "fetch_binance_aggtrade_range",
        "active_backfill",
    ];
    for path in production_files {
        let source = std::fs::read_to_string(&path).unwrap();
        for symbol in forbidden {
            assert!(
                !source.contains(symbol),
                "aggTrade REST recovery symbol {symbol:?} remains in {}",
                path.display()
            );
        }
    }
}
