use data_collect::binance_aggtrade::{
    AggTradeBackfillRange, AggTradeCoordinator, AggTradeIngestSource,
    BinanceAggTradeContinuityState, BinanceRestAggTrade, BinanceWsAggTrade,
    fetch_binance_aggtrade_range, fetch_binance_aggtrade_range_once,
};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

fn ws(id: u64, price: &str, received: i64) -> BinanceWsAggTrade {
    BinanceWsAggTrade::from_wire(
        &json!({
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
        }),
        "btcusdt@aggTrade",
        received,
    )
    .unwrap()
}

fn rest(id: u64, price: &str, received: i64) -> BinanceRestAggTrade {
    BinanceRestAggTrade::from_wire(
        &json!({
            "a": id,
            "p": price,
            "q": "0.001",
            "f": id * 10,
            "l": id * 10,
            "T": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
            "m": true,
            "M": true
        }),
        received,
    )
    .unwrap()
}

#[test]
fn gap_100_to_103_backfills_101_102_and_merges_the_overlap() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 25_000);

    let first = coordinator
        .accept_websocket(ws(100, "65000.10", 1_000))
        .unwrap();
    assert_eq!(first.outputs.len(), 1);
    assert_eq!(
        first.outputs[0].ingest_source(),
        AggTradeIngestSource::Websocket
    );

    let gap = coordinator
        .accept_websocket(ws(103, "65000.30", 1_030))
        .unwrap();
    assert!(gap.outputs.is_empty());
    assert_eq!(
        gap.start_backfill,
        Some(AggTradeBackfillRange {
            start_id: 101,
            end_id: 103,
        })
    );
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .last_observed_id,
        103,
        "accepted forward WS facts advance observed state before backfill completes"
    );
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .last_contiguous_id,
        100
    );

    let page = coordinator
        .accept_rest_page(vec![
            rest(101, "65000.20", 2_000),
            rest(102, "65000.25", 2_000),
            rest(103, "65000.30", 2_000),
        ])
        .unwrap();
    assert_eq!(page.outputs.len(), 3);
    assert_eq!(
        page.outputs[0].ingest_source(),
        AggTradeIngestSource::RestBackfill
    );
    assert_eq!(
        page.outputs[1].ingest_source(),
        AggTradeIngestSource::RestBackfill
    );
    assert_eq!(
        page.outputs[2].ingest_source(),
        AggTradeIngestSource::WebsocketRestMerged
    );
    assert_eq!(page.outputs[2].timestamp_received(), 2_000);
    assert_eq!(page.outputs[2].websocket_timestamp_received(), Some(1_030));
    assert_eq!(
        page.outputs[2].rest_backfill_timestamp_received(),
        Some(2_000)
    );
    assert!(page.gaps.is_empty());

    let complete = coordinator.complete_backfill().unwrap();
    assert!(complete.outputs.is_empty());
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .epoch_start_id,
        100
    );
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .last_observed_id,
        103
    );
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .last_contiguous_id,
        103
    );
}

#[test]
fn websocket_and_rest_shared_field_conflict_preserves_both_facts() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 25_000);
    coordinator.accept_websocket(ws(100, "1", 1_000)).unwrap();
    coordinator
        .accept_websocket(ws(103, "3-ws", 1_030))
        .unwrap();

    let result = coordinator
        .accept_rest_page(vec![
            rest(101, "2", 2_000),
            rest(102, "2.5", 2_000),
            rest(103, "3-rest", 2_000),
        ])
        .unwrap();
    assert_eq!(result.outputs.len(), 4);
    assert_eq!(
        result.outputs[2].ingest_source(),
        AggTradeIngestSource::RestBackfill
    );
    assert_eq!(
        result.outputs[3].ingest_source(),
        AggTradeIngestSource::Websocket
    );
    assert_eq!(
        result.gaps,
        vec!["binance_aggtrade_ws_rest_conflict:id=103"]
    );
}

#[test]
fn failed_backfill_closes_the_old_epoch_and_does_not_chase_it_again() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 25_000);
    coordinator.accept_websocket(ws(100, "1", 1_000)).unwrap();
    coordinator.accept_websocket(ws(103, "3", 1_030)).unwrap();

    let failed = coordinator.fail_backfill("http_429_exhausted").unwrap();
    assert_eq!(failed.outputs.len(), 1);
    assert_eq!(failed.outputs[0].aggregate_trade_id(), 103);
    assert_eq!(
        failed.gaps,
        vec![
            "binance_aggtrade_backfill_failed:http_429_exhausted",
            "binance_aggtrade_unresolved_range:101-102",
        ]
    );
    let state = coordinator.state();
    assert_eq!(state.unresolved_ranges.len(), 1);
    assert_eq!(state.unresolved_ranges[0].start_id, 101);
    assert_eq!(state.unresolved_ranges[0].end_id, 102);
    assert_eq!(state.latest_epoch.as_ref().unwrap().epoch_start_id, 103);
    assert_eq!(state.latest_epoch.as_ref().unwrap().last_contiguous_id, 103);

    let forward = coordinator.accept_websocket(ws(104, "4", 1_040)).unwrap();
    assert_eq!(forward.outputs.len(), 1);
    assert_eq!(forward.start_backfill, None);
}

#[test]
fn failed_backfill_keeps_the_highest_already_observed_pending_id() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 25_000);
    coordinator.accept_websocket(ws(100, "1", 1_000)).unwrap();
    coordinator.accept_websocket(ws(105, "5", 1_050)).unwrap();
    coordinator.accept_websocket(ws(110, "10", 1_100)).unwrap();

    let failed = coordinator.fail_backfill("timeout").unwrap();
    assert_eq!(
        failed.start_backfill,
        Some(AggTradeBackfillRange {
            start_id: 106,
            end_id: 110,
        })
    );
    let epoch = coordinator.state().latest_epoch.as_ref().unwrap();
    assert_eq!(epoch.epoch_start_id, 105);
    assert_eq!(epoch.last_contiguous_id, 105);
    assert_eq!(epoch.last_observed_id, 110);
}

#[test]
fn coordinated_flush_closes_each_pending_gap_without_starting_more_backfill() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 25_000);
    coordinator.accept_websocket(ws(100, "1", 1_000)).unwrap();
    coordinator.accept_websocket(ws(105, "5", 1_050)).unwrap();
    coordinator.accept_websocket(ws(110, "10", 1_100)).unwrap();

    let flushed = coordinator
        .flush_pending_without_backfill("coordinated_shutdown")
        .unwrap();

    assert_eq!(
        flushed
            .outputs
            .iter()
            .map(|output| output.aggregate_trade_id())
            .collect::<Vec<_>>(),
        vec![105, 110]
    );
    assert_eq!(
        flushed.gaps,
        vec![
            "binance_aggtrade_backfill_failed:coordinated_shutdown",
            "binance_aggtrade_unresolved_range:101-104",
            "binance_aggtrade_backfill_failed:coordinated_shutdown",
            "binance_aggtrade_unresolved_range:106-109",
        ]
    );
    assert_eq!(flushed.start_backfill, None);
    assert_eq!(coordinator.pending_rows(), 0);
    assert_eq!(coordinator.active_backfill(), None);
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .last_contiguous_id,
        110
    );
}

#[test]
fn pending_overflow_is_recorded_and_all_websocket_facts_are_preserved() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 2);
    coordinator.accept_websocket(ws(100, "1", 1_000)).unwrap();
    coordinator.accept_websocket(ws(105, "5", 1_050)).unwrap();
    coordinator.accept_websocket(ws(106, "6", 1_060)).unwrap();

    let overflow = coordinator.accept_websocket(ws(107, "7", 1_070)).unwrap();
    assert_eq!(
        overflow
            .outputs
            .iter()
            .map(|output| output.aggregate_trade_id())
            .collect::<Vec<_>>(),
        vec![105, 106, 107]
    );
    assert!(
        overflow
            .gaps
            .iter()
            .any(|gap| gap == "binance_aggtrade_pending_buffer_overflow:limit=2")
    );
    assert!(
        overflow
            .gaps
            .iter()
            .any(|gap| gap == "binance_aggtrade_unresolved_range:101-104")
    );
    assert_eq!(coordinator.pending_rows(), 0);
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .epoch_start_id,
        105
    );
    assert_eq!(
        coordinator
            .state()
            .latest_epoch
            .as_ref()
            .unwrap()
            .last_contiguous_id,
        107
    );
}

#[test]
fn old_replay_never_moves_the_latest_epoch_cursor() {
    let mut coordinator =
        AggTradeCoordinator::with_pending_limit(BinanceAggTradeContinuityState::default(), 25_000);
    coordinator.accept_websocket(ws(100, "1", 1_000)).unwrap();
    coordinator.accept_websocket(ws(101, "2", 1_010)).unwrap();
    let before = coordinator.state().clone();

    let replay = coordinator
        .accept_websocket(ws(99, "old-conflict", 2_000))
        .unwrap();
    assert_eq!(
        replay.outputs.len(),
        1,
        "the caller has already decided this is not an exact duplicate"
    );
    assert_eq!(coordinator.state(), &before);
}

#[test]
fn transport_specific_m_fields_remain_distinct() {
    let websocket = ws(100, "1", 1_000);
    let rest = rest(100, "1", 2_000);

    assert!(!websocket.ws_ignore);
    assert!(rest.rest_was_best_price_match);
    assert_eq!(websocket.shared, rest.shared);
    assert_ne!(
        websocket.source_native_payload_json,
        rest.source_native_payload_json
    );
}

#[tokio::test]
async fn rest_backfill_uses_inclusive_from_id_and_paginates_after_1000_rows() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 8_192];
            let read = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8(request[..read].to_vec()).unwrap();
            let request_line = request.lines().next().unwrap().to_owned();
            requests.push(request_line.clone());
            let from_id = query_u64(&request_line, "fromId");
            let limit = query_u64(&request_line, "limit");
            let end_id = 1_102_u64;
            let page_end = end_id.min(from_id + limit - 1);
            let body = serde_json::to_string(
                &(from_id..=page_end)
                    .map(|id| {
                        json!({
                            "a": id,
                            "p": id.to_string(),
                            "q": "0.001",
                            "f": id * 10,
                            "l": id * 10,
                            "T": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
                            "m": true,
                            "M": false
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });

    let client = reqwest::Client::builder().build().unwrap();
    let endpoint = format!("http://{address}/api/v3/aggTrades");
    let rows = fetch_binance_aggtrade_range_once(
        &client,
        &endpoint,
        AggTradeBackfillRange {
            start_id: 101,
            end_id: 1_102,
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1_002);
    assert_eq!(rows.first().unwrap().shared.aggregate_trade_id, 101);
    assert_eq!(rows.last().unwrap().shared.aggregate_trade_id, 1_102);
    assert!(
        rows.iter()
            .all(|row| row.symbol == "BTCUSDT" && row.timestamp_received > 0)
    );

    let requests = server.await.unwrap();
    assert!(requests[0].contains("symbol=BTCUSDT"));
    assert!(requests[0].contains("fromId=101"));
    assert!(requests[0].contains("limit=1000"));
    assert!(requests[1].contains("fromId=1101"));
    assert!(requests[1].contains("limit=2"));
}

#[tokio::test]
async fn rest_backfill_rejects_empty_and_out_of_order_pages() {
    for body in [
        "[]".to_owned(),
        serde_json::to_string(&vec![rest_wire(101), rest_wire(103), rest_wire(102)]).unwrap(),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4_096];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client = reqwest::Client::builder().build().unwrap();
        let error = fetch_binance_aggtrade_range_once(
            &client,
            &format!("http://{address}/api/v3/aggTrades"),
            AggTradeBackfillRange {
                start_id: 101,
                end_id: 103,
            },
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("empty") || error.to_string().contains("sequence"),
            "unexpected error: {error}"
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn rest_backfill_retries_429_and_then_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for attempt in 1..=3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4_096];
            let _ = socket.read(&mut request).await.unwrap();
            if attempt < 3 {
                socket
                    .write_all(
                        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            } else {
                let body = serde_json::to_string(&vec![rest_wire(101)]).unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        }
    });
    let client = reqwest::Client::builder().build().unwrap();
    let rows = fetch_binance_aggtrade_range(
        &client,
        &format!("http://{address}/api/v3/aggTrades"),
        AggTradeBackfillRange {
            start_id: 101,
            end_id: 101,
        },
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].shared.aggregate_trade_id, 101);
    server.await.unwrap();
}

#[tokio::test]
async fn rest_backfill_request_timeout_is_a_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; 4_096];
        let _ = socket.read(&mut request).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(20))
        .build()
        .unwrap();
    let error = fetch_binance_aggtrade_range_once(
        &client,
        &format!("http://{address}/api/v3/aggTrades"),
        AggTradeBackfillRange {
            start_id: 101,
            end_id: 101,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("request failed"));
    server.await.unwrap();
}

fn query_u64(request_line: &str, key: &str) -> u64 {
    let query = request_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .split_once('?')
        .unwrap()
        .1;
    query
        .split('&')
        .find_map(|pair| {
            let (candidate, value) = pair.split_once('=')?;
            (candidate == key).then(|| value.parse::<u64>().unwrap())
        })
        .unwrap()
}

fn rest_wire(id: u64) -> serde_json::Value {
    json!({
        "a": id,
        "p": id.to_string(),
        "q": "0.001",
        "f": id * 10,
        "l": id * 10,
        "T": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
        "m": true,
        "M": false
    })
}
