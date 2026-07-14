use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::auth::Credentials;
use polymarket_client_sdk_v2::clob::ws::interest::{InterestTracker, MessageInterest};
use polymarket_client_sdk_v2::clob::ws::subscription::SubscriptionManager as MarketSubscriptionManager;
use polymarket_client_sdk_v2::clob::ws::types::request::SubscriptionRequest;
use polymarket_client_sdk_v2::clob::ws::types::response::{WsMessage, parse_if_interested};
use polymarket_client_sdk_v2::rtds::RtdsMessage;
use polymarket_client_sdk_v2::rtds::subscription::SimpleParser;
use polymarket_client_sdk_v2::types::B256;
use polymarket_client_sdk_v2::ws::config::Config;
use polymarket_client_sdk_v2::ws::connection::{
    ConnectionFault, ConnectionManager, ConnectionState,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};

fn parser_test_config() -> Config {
    let mut config = Config::default();
    config.reconnect.initial_backoff = Duration::from_millis(10);
    config.reconnect.max_backoff = Duration::from_millis(20);
    config.reconnect.backoff_multiplier = 1.0;
    config.reconnect.max_attempts = Some(3);
    config
}

fn generation_test_config() -> Config {
    let mut config = parser_test_config();
    config.reconnect.max_attempts = Some(50);
    config.heartbeat_interval = Duration::from_secs(30);
    config.heartbeat_timeout = Duration::from_secs(30);
    config
}

async fn next_json_request(socket: &mut WebSocketStream<TcpStream>) -> Value {
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Message::Text(text))) = socket.next().await {
                if text == "PING" {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                    continue;
                }
                return serde_json::from_str(&text).unwrap();
            }
        }
    })
    .await
    .expect("outbound JSON request")
}

async fn wait_for_connected(receiver: &mut watch::Receiver<ConnectionState>) {
    timeout(Duration::from_secs(2), async {
        loop {
            if receiver.borrow().is_connected() {
                return;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .expect("connected state");
}

async fn wait_for_non_connected(receiver: &mut watch::Receiver<ConnectionState>) {
    timeout(Duration::from_secs(2), async {
        loop {
            if !receiver.borrow().is_connected() {
                return;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .expect("non-connected state");
}

async fn one_frame_server(frame: &'static str) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        socket.send(Message::Text(frame.into())).await.unwrap();

        assert!(
            timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err(),
            "parser fault must not reconnect"
        );
    });
    (format!("ws://{address}"), server)
}

async fn wait_for_fault(
    receiver: &mut watch::Receiver<Option<ConnectionFault>>,
) -> ConnectionFault {
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(fault) = receiver.borrow().clone() {
                return fault;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .expect("terminal parser fault")
}

async fn wait_for_terminal_disconnected(receiver: &mut watch::Receiver<ConnectionState>) {
    timeout(Duration::from_secs(2), async {
        loop {
            if *receiver.borrow() == ConnectionState::Disconnected {
                return;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .expect("terminal disconnected state");
}

#[test]
fn malformed_uninterested_array_element_is_still_ignored() {
    let frame = r#"[
        {
            "event_type": "book",
            "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "timestamp": "1234567890",
            "bids": [],
            "asks": []
        },
        {
            "event_type": "price_change",
            "market": "not-a-market-id"
        },
        {
            "event_type": "future_event",
            "unknown_field": true
        }
    ]"#;

    let messages = parse_if_interested(frame.as_bytes(), &MessageInterest::BOOK).unwrap();
    assert_eq!(messages.len(), 1);
    assert!(matches!(&messages[0], WsMessage::Book(_)));
}

#[tokio::test]
async fn interested_clob_array_is_all_or_error_and_terminal() {
    let frame = r#"[
        {
            "event_type": "book",
            "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "timestamp": "1234567890",
            "bids": [],
            "asks": []
        },
        {
            "event_type": "book",
            "asset_id": "not-an-asset-id",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "timestamp": "1234567891",
            "bids": [],
            "asks": []
        }
    ]"#;
    let (endpoint, server) = one_frame_server(frame).await;
    let interest = Arc::new(InterestTracker::new());
    interest.add(MessageInterest::BOOK);
    let connection = ConnectionManager::<WsMessage, Arc<InterestTracker>>::new(
        endpoint,
        parser_test_config(),
        interest,
    )
    .unwrap();
    let mut messages = connection.subscribe();
    let mut faults = connection.fault_receiver();
    let mut states = connection.state_receiver();

    assert!(matches!(
        wait_for_fault(&mut faults).await,
        ConnectionFault::Parser(_)
    ));
    wait_for_terminal_disconnected(&mut states).await;
    assert_eq!(connection.state(), ConnectionState::Disconnected);
    assert!(
        timeout(Duration::from_millis(100), messages.recv())
            .await
            .is_err(),
        "a valid prefix must not be broadcast"
    );

    let late_faults = connection.fault_receiver();
    assert!(matches!(
        late_faults.borrow().as_ref(),
        Some(ConnectionFault::Parser(_))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn rtds_whole_frame_parser_fault_is_sticky_and_terminal() {
    let frame = r#"{"message":"Invalid request body","connectionId":"connection-123","requestId":"request-456","payload":{"secret":"PAYLOAD_MUST_NOT_LEAK"}}"#;
    let (endpoint, server) = one_frame_server(frame).await;
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        endpoint,
        parser_test_config(),
        SimpleParser::default(),
    )
    .unwrap();
    let mut messages = connection.subscribe();
    let mut faults = connection.fault_receiver();
    let mut states = connection.state_receiver();

    let fault = wait_for_fault(&mut faults).await;
    let ConnectionFault::Parser(message) = fault else {
        panic!("expected parser fault");
    };
    assert!(message.contains("missing field `topic`"));
    assert!(message.contains(&format!("frame_bytes={}", frame.len())));
    assert!(message.contains("top_level=object"));
    assert!(message.contains("message=\"Invalid request body\""));
    assert!(message.contains("connectionId=\"connection-123\""));
    assert!(message.contains("requestId=\"request-456\""));
    assert!(!message.contains("PAYLOAD_MUST_NOT_LEAK"));
    wait_for_terminal_disconnected(&mut states).await;
    assert_eq!(connection.state(), ConnectionState::Disconnected);
    assert!(
        timeout(Duration::from_millis(100), messages.recv())
            .await
            .is_err()
    );
    server.await.unwrap();
}

#[tokio::test]
async fn generation_zero_preserves_preconnect_requests_and_fifo() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        generation_test_config(),
        SimpleParser::default(),
    )
    .unwrap();
    connection.send(&json!({"sequence": 1})).unwrap();
    connection
        .send_authenticated(
            &SubscriptionRequest::user(Vec::new()),
            &Credentials::default(),
        )
        .unwrap();

    let listener = TcpListener::bind(address).await.unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        vec![
            next_json_request(&mut socket).await,
            next_json_request(&mut socket).await,
        ]
    });

    let requests = server.await.unwrap();
    assert_eq!(requests[0]["sequence"], 1);
    assert_eq!(requests[1]["type"], "user");
    assert!(requests[1]["auth"].is_object());
}

#[tokio::test]
async fn old_generation_requests_are_dropped_across_repeated_reconnects() {
    const FLAPS: usize = 8;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (server_connected_tx, mut server_connected_rx) = mpsc::unbounded_channel();
    let (close_tx, mut close_rx) = mpsc::unbounded_channel();
    let (accept_next_tx, mut accept_next_rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        for generation in 0..=FLAPS {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            server_connected_tx.send(generation).unwrap();
            if generation < FLAPS {
                close_rx.recv().await.unwrap();
                socket.send(Message::Close(None)).await.unwrap();
                drop(socket);
                accept_next_rx.recv().await.unwrap();
            } else {
                let requests = vec![
                    next_json_request(&mut socket).await,
                    next_json_request(&mut socket).await,
                ];
                assert!(
                    timeout(Duration::from_millis(200), next_json_request(&mut socket))
                        .await
                        .is_err(),
                    "old generations must not accumulate into the stable socket"
                );
                return requests;
            }
        }
        unreachable!("final stable connection returns requests")
    });

    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        generation_test_config(),
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();

    for generation in 0..FLAPS {
        assert_eq!(server_connected_rx.recv().await.unwrap(), generation);
        wait_for_connected(&mut states).await;
        close_tx.send(()).unwrap();
        wait_for_non_connected(&mut states).await;
        connection
            .send(&json!({"stale_generation": generation}))
            .unwrap();
        accept_next_tx.send(()).unwrap();
    }

    assert_eq!(server_connected_rx.recv().await.unwrap(), FLAPS);
    wait_for_connected(&mut states).await;
    connection.send(&json!({"sequence": 1})).unwrap();
    connection
        .send_authenticated(
            &SubscriptionRequest::user(Vec::new()),
            &Credentials::default(),
        )
        .unwrap();

    let requests = server.await.unwrap();
    assert_eq!(requests[0]["sequence"], 1);
    assert_eq!(requests[1]["type"], "user");
    assert!(requests[1]["auth"].is_object());
}

#[tokio::test]
async fn authenticated_subscription_manager_resubscribes_in_the_new_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let initial = next_json_request(&mut socket).await;
        sleep(Duration::from_millis(100)).await;
        socket.send(Message::Close(None)).await.unwrap();
        drop(socket);

        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let resubscription = next_json_request(&mut socket).await;
        vec![initial, resubscription]
    });

    let interest = Arc::new(InterestTracker::new());
    let connection = ConnectionManager::<WsMessage, Arc<InterestTracker>>::new(
        format!("ws://{address}"),
        generation_test_config(),
        Arc::clone(&interest),
    )
    .unwrap();
    let subscriptions = Arc::new(MarketSubscriptionManager::new(connection.clone(), interest));
    subscriptions.start_reconnection_handler();
    let mut states = connection.state_receiver();
    wait_for_connected(&mut states).await;
    drop(
        subscriptions
            .subscribe_user(vec![B256::ZERO], &Credentials::default())
            .unwrap(),
    );

    let requests = server.await.unwrap();
    assert_eq!(requests[0]["type"], "user");
    assert_eq!(requests[0]["markets"], requests[1]["markets"]);
    assert_eq!(requests[0]["auth"], requests[1]["auth"]);
}
