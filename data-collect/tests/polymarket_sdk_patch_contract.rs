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
use tokio::io::AsyncReadExt as _;
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

#[cfg(target_os = "linux")]
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
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
    let frame = r#"{"topic":"crypto_prices_chainlink","type":"update","timestamp":"INVALID_TIMESTAMP_MUST_NOT_LEAK","payload":{"secret":"PAYLOAD_MUST_NOT_LEAK"}}"#;
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
    assert!(message.contains(&format!("frame_bytes={}", frame.len())));
    assert!(message.contains("top_level=object"));
    assert!(message.contains("topic=\"crypto_prices_chainlink\""));
    assert!(message.contains("type=\"update\""));
    assert!(!message.contains("INVALID_TIMESTAMP_MUST_NOT_LEAK"));
    assert!(!message.contains("PAYLOAD_MUST_NOT_LEAK"));
    wait_for_terminal_disconnected(&mut states).await;
    assert_eq!(connection.state(), ConnectionState::Disconnected);
    assert!(
        timeout(Duration::from_millis(100), messages.recv())
            .await
            .is_err()
    );
    let late_faults = connection.fault_receiver();
    let late_message = match late_faults.borrow().as_ref() {
        Some(ConnectionFault::Parser(message)) => message.clone(),
        fault => panic!("expected late sticky parser fault, got {fault:?}"),
    };
    assert_eq!(late_message, message);
    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn clob_connection_reconnects_when_heartbeat_pong_is_missing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reconnected_tx, reconnected_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, mut finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first_socket = accept_async(stream).await.unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    first_socket.next().await,
                    Some(Ok(Message::Text(text))) if text == "PING"
                ) {
                    break;
                }
            }
        })
        .await
        .expect("first heartbeat PING");

        timeout(Duration::from_secs(2), async {
            loop {
                match first_socket.next().await {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        })
        .await
        .expect("heartbeat timeout must close the old socket");

        let (stream, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("heartbeat timeout must reconnect")
            .unwrap();
        let mut second_socket = accept_async(stream).await.unwrap();
        reconnected_tx.send(()).unwrap();
        loop {
            tokio::select! {
                result = &mut finish_rx => {
                    result.unwrap();
                    break;
                }
                message = second_socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) if text == "PING" => {
                            second_socket
                                .send(Message::Text("PONG".into()))
                                .await
                                .unwrap();
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
    });

    let interest = Arc::new(InterestTracker::new());
    interest.add(MessageInterest::BOOK);
    let mut config = generation_test_config();
    config.heartbeat_interval = Duration::from_millis(20);
    config.heartbeat_timeout = Duration::from_millis(30);
    let connection = ConnectionManager::<WsMessage, Arc<InterestTracker>>::new(
        format!("ws://{address}"),
        config,
        interest,
    )
    .unwrap();
    let mut states = connection.state_receiver();
    wait_for_connected(&mut states).await;
    let initial_generation = connection.connection_generation();

    timeout(Duration::from_secs(2), reconnected_rx)
        .await
        .expect("second connection after missing PONG")
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if connection.connection_generation() == initial_generation + 1
                && connection.state().is_connected()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("new connected generation");

    finish_tx.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_last_connection_manager_closes_socket_without_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (healthy_heartbeat_tx, healthy_heartbeat_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut ping_count = 0_u8;
        let mut healthy_heartbeat_tx = Some(healthy_heartbeat_tx);
        timeout(Duration::from_secs(2), async {
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) if text == "PING" => {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                        ping_count += 1;
                        if ping_count == 2
                            && let Some(sender) = healthy_heartbeat_tx.take()
                        {
                            sender.send(()).unwrap();
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        })
        .await
        .expect("last manager drop must close the active socket");
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "normal manager teardown must not reconnect"
        );
    });

    let mut config = generation_test_config();
    config.heartbeat_interval = Duration::from_millis(20);
    config.heartbeat_timeout = Duration::from_millis(200);
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        config,
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();
    wait_for_connected(&mut states).await;
    timeout(Duration::from_secs(2), healthy_heartbeat_rx)
        .await
        .expect("two healthy heartbeat PING/PONG cycles")
        .unwrap();
    drop(connection);

    let mut saw_disconnected = false;
    timeout(Duration::from_secs(2), async {
        loop {
            if *states.borrow_and_update() == ConnectionState::Disconnected {
                saw_disconnected = true;
            }
            if states.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .expect("connection state task convergence");
    assert!(saw_disconnected);

    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn generation_bound_clone_keeps_socket_until_the_last_sender_drops() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (healthy_heartbeat_tx, healthy_heartbeat_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, mut closed_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut ping_count = 0_u8;
        let mut healthy_heartbeat_tx = Some(healthy_heartbeat_tx);
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) if text == "PING" => {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                    ping_count += 1;
                    if ping_count == 2
                        && let Some(sender) = healthy_heartbeat_tx.take()
                    {
                        sender.send(()).unwrap();
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            }
        }
        closed_tx.send(()).unwrap();
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "last bound sender teardown must not reconnect"
        );
    });

    let mut config = generation_test_config();
    config.heartbeat_interval = Duration::from_millis(20);
    config.heartbeat_timeout = Duration::from_millis(200);
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        config,
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();
    wait_for_connected(&mut states).await;
    let bound = connection
        .bind_generation(connection.connection_generation())
        .unwrap();
    timeout(Duration::from_secs(2), healthy_heartbeat_rx)
        .await
        .expect("two healthy heartbeat PING/PONG cycles")
        .unwrap();

    drop(connection);
    assert!(
        timeout(Duration::from_millis(100), &mut closed_rx)
            .await
            .is_err(),
        "a live bound sender must keep the socket owned"
    );
    drop(bound);
    timeout(Duration::from_secs(2), &mut closed_rx)
        .await
        .expect("last bound sender closes the socket")
        .unwrap();
    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn heartbeat_task_panic_is_terminal_and_does_not_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        accepted_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                match socket.next().await {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        })
        .await
        .expect("terminal heartbeat task failure closes the socket");
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "terminal heartbeat task failure must not reconnect"
        );
    });

    let mut config = generation_test_config();
    config.heartbeat_interval = Duration::ZERO;
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        config,
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();
    timeout(Duration::from_secs(2), accepted_rx)
        .await
        .expect("initial connection")
        .unwrap();
    wait_for_terminal_disconnected(&mut states).await;
    assert_eq!(connection.connection_generation(), 0);
    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn server_tcp_eof_is_recoverable_and_advances_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (reconnected_tx, reconnected_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, mut finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let first_socket = accept_async(stream).await.unwrap();
        drop(first_socket);

        let (stream, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("reconnect after TCP EOF")
            .unwrap();
        let mut second_socket = accept_async(stream).await.unwrap();
        reconnected_tx.send(()).unwrap();
        loop {
            tokio::select! {
                result = &mut finish_rx => {
                    result.unwrap();
                    break;
                }
                message = second_socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) if text == "PING" => {
                            second_socket
                                .send(Message::Text("PONG".into()))
                                .await
                                .unwrap();
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
    });

    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        generation_test_config(),
        SimpleParser::default(),
    )
    .unwrap();
    let initial_generation = connection.connection_generation();
    timeout(Duration::from_secs(2), reconnected_rx)
        .await
        .expect("second connection after TCP EOF")
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if connection.connection_generation() == initial_generation + 1
                && connection.state().is_connected()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connected generation after TCP EOF");

    finish_tx.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_handshake_timeout_retries_without_exhausting_the_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stalled_handshake, _) = listener.accept().await.unwrap();
        let (stream, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("a stalled handshake must be cancelled and retried")
            .unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(text)) if text == "PING" => {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        drop(stalled_handshake);
    });

    let mut config = generation_test_config();
    config.connect_timeout = Some(Duration::from_millis(40));
    config.reconnect.max_attempts = None;
    config.reconnect.initial_backoff = Duration::from_millis(5);
    config.reconnect.max_backoff = Duration::from_millis(10);
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        config,
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();

    wait_for_connected(&mut states).await;
    assert_eq!(connection.connection_generation(), 0);
    drop(connection);
    timeout(Duration::from_secs(1), server)
        .await
        .expect("connection owner must stop after its last sender drops")
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_the_last_manager_during_a_stalled_handshake_cancels_without_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stalled_handshake, _) = listener.accept().await.unwrap();
        accepted_tx.send(()).unwrap();
        let mut buffer = [0_u8; 1024];
        timeout(Duration::from_millis(300), async {
            loop {
                match stalled_handshake.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
        .await
        .expect("the configured handshake deadline must release the half-open socket");
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "dropping the last manager must not reconnect after handshake cancellation"
        );
    });

    let mut config = generation_test_config();
    config.connect_timeout = Some(Duration::from_millis(40));
    config.reconnect.max_attempts = None;
    config.reconnect.initial_backoff = Duration::from_millis(200);
    config.reconnect.max_backoff = Duration::from_millis(200);
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        config,
        SimpleParser::default(),
    )
    .unwrap();

    accepted_rx.await.unwrap();
    drop(connection);
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

#[tokio::test(flavor = "current_thread")]
async fn one_hundred_natural_reconnects_keep_one_connection_loop_and_heartbeat() {
    const RECONNECTS: u64 = 100;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel();
    let (close_tx, mut close_rx) = mpsc::unbounded_channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        for generation in 0..=RECONNECTS {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            accepted_tx.send(generation).unwrap();
            if generation == RECONNECTS {
                finish_rx.await.unwrap();
                return;
            }
            loop {
                tokio::select! {
                    command = close_rx.recv() => {
                        assert_eq!(command.unwrap(), generation);
                        socket.send(Message::Close(None)).await.unwrap();
                        break;
                    }
                    message = socket.next() => {
                        match message {
                            Some(Ok(Message::Text(text))) if text == "PING" => {
                                socket.send(Message::Text("PONG".into())).await.unwrap();
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                            _ => {}
                        }
                    }
                }
            }
        }
    });

    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        generation_test_config(),
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();
    let messages = connection.subscribe();
    assert_eq!(accepted_rx.recv().await.unwrap(), 0);
    wait_for_connected(&mut states).await;
    let steady_tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    #[cfg(target_os = "linux")]
    let steady_fds = open_fd_count();

    for generation in 0..RECONNECTS {
        assert_eq!(connection.connection_generation(), generation);
        close_tx.send(generation).unwrap();
        assert_eq!(accepted_rx.recv().await.unwrap(), generation + 1);
        timeout(Duration::from_secs(2), async {
            loop {
                if connection.connection_generation() == generation + 1
                    && connection.state().is_connected()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("next natural connection generation");
        timeout(Duration::from_secs(1), async {
            loop {
                let alive = tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks();
                if alive <= steady_tasks {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old heartbeat task must not accumulate");
    }

    assert_eq!(connection.connection_generation(), RECONNECTS);
    assert_eq!(messages.len(), 0);
    #[cfg(target_os = "linux")]
    assert!(
        open_fd_count() <= steady_fds + 4,
        "natural reconnects must not retain one descriptor per generation"
    );
    finish_tx.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn generation_bound_sender_cannot_write_into_a_later_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        close_rx.await.unwrap();
        socket.send(Message::Close(None)).await.unwrap();
        drop(socket);

        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let request = next_json_request(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(200), next_json_request(&mut socket))
                .await
                .is_err(),
            "the bound old-generation request must be dropped"
        );
        request
    });

    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        generation_test_config(),
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();
    wait_for_connected(&mut states).await;
    let generation = connection.connection_generation();
    let bound = connection.bind_generation(generation).unwrap();
    close_tx.send(()).unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if connection.connection_generation() == generation + 1
                && connection.state().is_connected()
            {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .expect("natural reconnect after server Close");

    bound.send(&json!({"old": true})).unwrap();
    connection.send(&json!({"current": true})).unwrap();
    assert_eq!(server.await.unwrap(), json!({"current": true}));
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_inbound_frames_do_not_starve_outbound_ping_or_transport_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (outbound_tx, outbound_rx) = tokio::sync::oneshot::channel();
    let (ping_tx, ping_rx) = tokio::sync::oneshot::channel();
    let (close_tx, mut close_rx) = tokio::sync::oneshot::channel();
    let (reconnected_tx, reconnected_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut outbound_tx = Some(outbound_tx);
        let mut ping_tx = Some(ping_tx);
        let mut sequence = 0_u64;
        let mut ticker = tokio::time::interval(Duration::from_millis(1));
        loop {
            tokio::select! {
                result = &mut close_rx => {
                    result.unwrap();
                    socket.send(Message::Close(None)).await.unwrap();
                    break;
                }
                _ = ticker.tick() => {
                    for _ in 0..32 {
                        let frame = json!({
                            "topic": "crypto_prices_chainlink",
                            "type": "update",
                            "timestamp": sequence,
                            "payload": {"symbol": "btc/usd", "timestamp": sequence, "value": "1"}
                        });
                        if socket.send(Message::Text(frame.to_string().into())).await.is_err() {
                            break;
                        }
                        sequence += 1;
                    }
                }
                message = socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) if text == "PING" => {
                            if let Some(tx) = ping_tx.take() {
                                tx.send(()).unwrap();
                            }
                            socket.send(Message::Text("PONG".into())).await.unwrap();
                        }
                        Some(Ok(Message::Text(text))) => {
                            if serde_json::from_str::<Value>(&text).is_ok()
                                && let Some(tx) = outbound_tx.take()
                            {
                                tx.send(()).unwrap();
                            }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }

        let (stream, _) = listener.accept().await.unwrap();
        let _socket = accept_async(stream).await.unwrap();
        reconnected_tx.send(()).unwrap();
        finish_rx.await.unwrap();
    });

    let mut config = generation_test_config();
    config.heartbeat_interval = Duration::from_millis(20);
    config.heartbeat_timeout = Duration::from_millis(500);
    let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
        format!("ws://{address}"),
        config,
        SimpleParser::default(),
    )
    .unwrap();
    let mut states = connection.state_receiver();
    wait_for_connected(&mut states).await;
    let mut messages = connection.subscribe();
    let drain = tokio::spawn(async move { while messages.recv().await.is_ok() {} });

    connection.send(&json!({"control": "outbound"})).unwrap();
    timeout(Duration::from_secs(2), outbound_rx)
        .await
        .expect("outbound under continuous inbound")
        .unwrap();
    timeout(Duration::from_secs(2), ping_rx)
        .await
        .expect("heartbeat under continuous inbound")
        .unwrap();
    close_tx.send(()).unwrap();
    timeout(Duration::from_secs(2), reconnected_rx)
        .await
        .expect("reconnect under continuous inbound")
        .unwrap();

    finish_tx.send(()).unwrap();
    server.await.unwrap();
    drain.abort();
}

#[tokio::test]
async fn old_generation_requests_are_dropped_across_repeated_reconnects() {
    const FLAPS: usize = 100;

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
