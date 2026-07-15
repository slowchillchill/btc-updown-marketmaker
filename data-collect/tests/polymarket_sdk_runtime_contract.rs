use std::collections::BTreeSet;
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::types::response::WsMessage;
use polymarket_client_sdk_v2::rtds::Subscription;
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};

use data_collect::polymarket_sdk_runtime::{MarketSdkRuntime, RtdsSdkRuntime, SdkStreamError};

fn fast_config() -> Config {
    let mut config = Config::default();
    config.reconnect.max_attempts = Some(20);
    config.reconnect.initial_backoff = Duration::from_millis(5);
    config.reconnect.max_backoff = Duration::from_millis(10);
    config.reconnect.backoff_multiplier = 1.0;
    config.heartbeat_interval = Duration::from_secs(30);
    config.heartbeat_timeout = Duration::from_secs(30);
    config
}

#[cfg(target_os = "linux")]
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

fn exhausting_config() -> Config {
    let mut config = fast_config();
    config.reconnect.max_attempts = Some(2);
    config
}

fn assets(values: &[u64]) -> BTreeSet<U256> {
    values.iter().copied().map(U256::from).collect()
}

async fn next_text(socket: &mut WebSocketStream<TcpStream>) -> String {
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Ok(Message::Text(text))) = socket.next().await {
                if text == "PING" {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                    continue;
                }
                if serde_json::from_str::<Value>(&text).is_ok() {
                    return text.to_string();
                }
            }
        }
    })
    .await
    .expect("subscription request")
}

#[tokio::test(flavor = "current_thread")]
async fn disconnected_desired_updates_coalesce_to_one_final_subscription() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    let mut final_revision = 0;
    for value in 1..=10_000_u64 {
        final_revision = runtime
            .replace_desired_assets(assets(&[value, value + 1]))
            .unwrap();
    }

    let listener = TcpListener::bind(address).await.unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let request = next_text(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(200), next_text(&mut socket))
                .await
                .is_err(),
            "offline desired-state churn must not enqueue intermediate requests"
        );
        request
    });

    runtime
        .wait_desired_applied(final_revision, Duration::from_secs(2))
        .await
        .unwrap();
    let request: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
    assert_eq!(request["operation"], "subscribe");
    assert_eq!(request["assets_ids"], json!(["10000", "10001"]));
    assert_eq!(request["custom_feature_enabled"], true);
    assert_eq!(runtime.subscription_count(), 1);
}

#[tokio::test]
async fn connected_market_actor_applies_only_the_minimal_asset_diff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut requests = Vec::new();
        for _ in 0..3 {
            requests.push(next_text(&mut socket).await);
        }
        requests
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .replace_and_wait(assets(&[1, 2]), Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .replace_and_wait(assets(&[2, 3]), Duration::from_secs(2))
        .await
        .unwrap();

    let requests = server
        .await
        .unwrap()
        .into_iter()
        .map(|request| serde_json::from_str::<Value>(&request).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(requests[0]["operation"], "subscribe");
    assert_eq!(requests[0]["assets_ids"], json!(["1", "2"]));
    assert_eq!(requests[1]["operation"], "subscribe");
    assert_eq!(requests[1]["assets_ids"], json!(["3"]));
    assert_eq!(requests[2]["operation"], "unsubscribe");
    assert_eq!(requests[2]["assets_ids"], json!(["1"]));
}

#[tokio::test]
async fn market_resubscribe_is_serial_and_restores_the_same_assets() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut requests = Vec::new();
        for _ in 0..3 {
            requests.push(next_text(&mut socket).await);
        }
        requests
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .replace_and_wait(assets(&[1, 2]), Duration::from_secs(2))
        .await
        .unwrap();
    runtime.resubscribe(Duration::from_secs(2)).await.unwrap();

    let requests = server
        .await
        .unwrap()
        .into_iter()
        .map(|request| serde_json::from_str::<Value>(&request).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(requests[0]["operation"], "subscribe");
    assert_eq!(requests[0]["assets_ids"], json!(["1", "2"]));
    assert_eq!(requests[1]["operation"], "unsubscribe");
    assert_eq!(requests[1]["assets_ids"], json!(["1", "2"]));
    assert_eq!(requests[2]["operation"], "subscribe");
    assert_eq!(requests[2]["assets_ids"], json!(["1", "2"]));
}

#[tokio::test]
async fn reconnect_replays_only_the_latest_offline_desired_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (first_connection_closed_tx, first_connection_closed_rx) = oneshot::channel();
    let (allow_reconnect_tx, allow_reconnect_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let first_request = next_text(&mut socket).await;
        socket.send(Message::Close(None)).await.unwrap();
        drop(socket);
        first_connection_closed_tx.send(()).unwrap();

        allow_reconnect_rx.await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let reconnect_request = next_text(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(200), next_text(&mut socket))
                .await
                .is_err(),
            "one serial actor must not replay the old subscription beside the latest state"
        );
        vec![first_request, reconnect_request]
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .replace_and_wait(assets(&[1, 2]), Duration::from_secs(2))
        .await
        .unwrap();
    first_connection_closed_rx.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.connection_state().is_connected() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    let mut final_revision = 0;
    for value in 1..=10_000_u64 {
        final_revision = runtime
            .replace_desired_assets(assets(&[value, value + 1]))
            .unwrap();
    }
    allow_reconnect_tx.send(()).unwrap();
    runtime
        .wait_desired_applied(final_revision, Duration::from_secs(2))
        .await
        .unwrap();

    let requests = server
        .await
        .unwrap()
        .into_iter()
        .map(|request| serde_json::from_str::<Value>(&request).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(requests[0]["operation"], "subscribe");
    assert_eq!(requests[0]["assets_ids"], json!(["1", "2"]));
    assert_eq!(requests[1]["operation"], "subscribe");
    assert_eq!(requests[1]["assets_ids"], json!(["10000", "10001"]));
}

#[tokio::test]
async fn market_single_receiver_exposes_broadcast_lag() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        _ = next_text(&mut socket).await;
        for timestamp in 0..1_500_i64 {
            socket
                .send(Message::Text(
                    json!({
                        "event_type": "book",
                        "asset_id": "1",
                        "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "timestamp": timestamp.to_string(),
                        "bids": [],
                        "asks": []
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        sleep(Duration::from_secs(2)).await;
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .replace_and_wait(assets(&[1]), Duration::from_secs(2))
        .await
        .unwrap();
    sleep(Duration::from_millis(500)).await;
    assert!(matches!(
        runtime.recv(Duration::from_secs(1)).await,
        Err(SdkStreamError::Lagged(count)) if count > 0
    ));
    server.abort();
}

#[tokio::test]
async fn rtds_uses_one_direct_receiver_and_exposes_parser_fault() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        _ = next_text(&mut socket).await;
        socket
            .send(Message::Text(
                r#"{"topic":"crypto_prices_chainlink""#.into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .wait_connected(Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .subscribe(
            Subscription::chainlink_prices(Some("btc/usd".to_owned())),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(runtime.subscription_count(), 1);
    assert!(matches!(
        runtime.recv(Duration::from_secs(2)).await,
        Err(SdkStreamError::Parser(_))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn rtds_manager_resubscribes_in_the_new_connection_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let initial = next_text(&mut socket).await;
        socket.send(Message::Close(None)).await.unwrap();
        drop(socket);

        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let resubscription = next_text(&mut socket).await;
        socket
            .send(Message::Text(
                json!({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "timestamp": 1,
                    "payload": {
                        "symbol": "btc/usd",
                        "timestamp": 1,
                        "value": "100000"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        vec![initial, resubscription]
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .wait_connected(Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .subscribe(
            Subscription::chainlink_prices(Some("btc/usd".to_owned())),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(
        runtime
            .recv(Duration::from_secs(2))
            .await
            .unwrap()
            .is_some()
    );

    let requests = server
        .await
        .unwrap()
        .into_iter()
        .map(|request| serde_json::from_str::<Value>(&request).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(requests[0], requests[1]);
}

#[tokio::test(flavor = "current_thread")]
async fn rtds_one_shot_reconnect_replays_once_without_unsubscribe() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let initial = next_text(&mut socket).await;
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(Message::Text(text)) if text == "PING" => {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                }
                _ => {}
            }
        }
        drop(socket);

        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let replay = next_text(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(200), next_text(&mut socket))
                .await
                .is_err(),
            "a generation may receive only one complete RTDS replay"
        );
        vec![initial, replay]
    });

    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .subscribe(subscription, Duration::from_secs(2))
        .await
        .unwrap();
    let generation = runtime.connection_generation();
    assert!(runtime.request_reconnect(generation));
    for _ in 0..10_000 {
        assert!(!runtime.request_reconnect(generation));
    }
    timeout(Duration::from_secs(2), async {
        while runtime.connection_generation() == generation {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let revision = runtime.subscription_revision();
    runtime
        .wait_subscription_applied(revision, Duration::from_secs(2))
        .await
        .unwrap();

    let requests = server
        .await
        .unwrap()
        .into_iter()
        .map(|request| serde_json::from_str::<Value>(&request).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(requests[0]["action"], "subscribe");
    assert_eq!(requests[1]["action"], "subscribe");
    assert_eq!(requests[0]["subscriptions"], requests[1]["subscriptions"]);
}

#[tokio::test(flavor = "current_thread")]
async fn rtds_one_hundred_one_shots_keep_one_actor_receiver_and_replay_per_generation() {
    const RECONNECTS: u64 = 100;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (replay_tx, mut replay_rx) = mpsc::unbounded_channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        for generation in 0..=RECONNECTS {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let request: Value = serde_json::from_str(&next_text(&mut socket).await).unwrap();
            assert_eq!(request["action"], "subscribe");
            assert_eq!(request["subscriptions"].as_array().unwrap().len(), 1);
            replay_tx.send(generation).unwrap();
            if generation == RECONNECTS {
                finish_rx.await.unwrap();
                return;
            }
            while let Some(message) = socket.next().await {
                match message {
                    Ok(Message::Text(text)) if text == "PING" => {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                    }
                    Ok(Message::Text(text)) if serde_json::from_str::<Value>(&text).is_ok() => {
                        panic!("generation {generation} received a duplicate RTDS replay")
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        }
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .subscribe(
            Subscription::chainlink_prices(Some("btc/usd".to_owned())),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(replay_rx.recv().await.unwrap(), 0);
    let revision = runtime.subscription_revision();
    let steady_tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    #[cfg(target_os = "linux")]
    let steady_fds = open_fd_count();

    for generation in 0..RECONNECTS {
        assert_eq!(runtime.connection_generation(), generation);
        assert!(runtime.request_reconnect(generation));
        timeout(Duration::from_secs(2), async {
            while runtime.connection_generation() == generation {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("next RTDS generation");
        runtime
            .wait_subscription_applied(revision, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(runtime.connection_generation(), generation + 1);
        assert_eq!(replay_rx.recv().await.unwrap(), generation + 1);
        timeout(Duration::from_secs(1), async {
            loop {
                if tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks()
                    <= steady_tasks
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("RTDS actor/heartbeat tasks must not accumulate");
        assert_eq!(runtime.subscription_count(), 1);
    }

    #[cfg(target_os = "linux")]
    assert!(
        open_fd_count() <= steady_fds + 4,
        "RTDS reconnects must not retain one descriptor per generation"
    );

    finish_tx.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn rtds_stale_ticket_does_not_close_the_new_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        _ = next_text(&mut socket).await;
        socket.send(Message::Close(None)).await.unwrap();
        drop(socket);

        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let replay = next_text(&mut socket).await;
        sleep(Duration::from_millis(200)).await;
        socket
            .send(Message::Text(
                json!({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "timestamp": 1,
                    "payload": {
                        "symbol": "btc/usd",
                        "timestamp": 1,
                        "value": "100000"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        replay
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    let old_generation = runtime.connection_generation();
    runtime
        .subscribe(
            Subscription::chainlink_prices(Some("btc/usd".to_owned())),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.connection_generation() == old_generation {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!runtime.request_reconnect(old_generation));
    assert!(
        runtime
            .recv(Duration::from_secs(2))
            .await
            .unwrap()
            .is_some()
    );

    let request: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
    assert_eq!(request["action"], "subscribe");
}

#[tokio::test(flavor = "current_thread")]
async fn rtds_desired_before_actor_first_poll_is_applied() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        next_text(&mut socket).await
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    let revision = runtime
        .replace_subscription(Subscription::chainlink_prices(Some("btc/usd".to_owned())))
        .unwrap();
    runtime
        .wait_subscription_applied(revision, Duration::from_secs(2))
        .await
        .unwrap();

    let request: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
    assert_eq!(request["action"], "subscribe");
    assert_eq!(runtime.subscription_count(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn rtds_immediate_close_flaps_replay_only_the_latest_desired_subscription() {
    const FLAPS: usize = 8;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..FLAPS {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            socket.send(Message::Close(None)).await.unwrap();
            drop(socket);
        }

        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let request = next_text(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(200), next_text(&mut socket))
                .await
                .is_err(),
            "the stable RTDS epoch must receive only the latest desired subscription"
        );
        request
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    let mut final_revision = 0;
    for value in 1..=10_000_u64 {
        final_revision = runtime
            .replace_subscription(Subscription::chainlink_prices(Some(format!(
                "btc/usd-{value}"
            ))))
            .unwrap();
    }
    runtime
        .wait_subscription_applied(final_revision, Duration::from_secs(2))
        .await
        .unwrap();

    let request: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
    assert_eq!(request["action"], "subscribe");
    assert_eq!(
        request["subscriptions"][0]["filters"],
        r#"{"symbol":"btc/usd-10000"}"#
    );
}

#[tokio::test]
async fn rtds_single_receiver_exposes_broadcast_lag() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        _ = next_text(&mut socket).await;
        for timestamp in 0..1_500_i64 {
            socket
                .send(Message::Text(
                    json!({
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "timestamp": timestamp,
                        "payload": {
                            "symbol": "btc/usd",
                            "timestamp": timestamp,
                            "value": "100000"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        sleep(Duration::from_secs(2)).await;
    });

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .wait_connected(Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .subscribe(
            Subscription::chainlink_prices(Some("btc/usd".to_owned())),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(500)).await;
    assert!(matches!(
        runtime.recv(Duration::from_secs(1)).await,
        Err(SdkStreamError::Lagged(count)) if count > 0
    ));
    server.abort();
}

#[tokio::test]
async fn market_single_receiver_delivers_legal_typed_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        _ = next_text(&mut socket).await;
        socket
            .send(Message::Text(
                json!({
                    "event_type": "book",
                    "asset_id": "1",
                    "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "timestamp": "1",
                    "bids": [],
                    "asks": []
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .replace_and_wait(assets(&[1]), Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        runtime.recv(Duration::from_secs(2)).await.unwrap(),
        Some(WsMessage::Book(_))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn market_runtime_prefers_sticky_parser_fault_over_terminal_actor_exit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        _ = next_text(&mut socket).await;
        socket
            .send(Message::Text(
                r#"{"event_type":"book","asset_id":"1""#.into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    runtime
        .replace_and_wait(assets(&[1]), Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        runtime.recv(Duration::from_secs(2)).await,
        Err(SdkStreamError::Parser(_))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn market_wait_prefers_sticky_parser_fault_after_terminal_actor_exit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (server_connected_tx, server_connected_rx) = oneshot::channel();
    let (send_malformed_tx, send_malformed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        server_connected_tx.send(()).unwrap();
        send_malformed_rx.await.unwrap();
        socket
            .send(Message::Text(
                r#"{"event_type":"book","asset_id":"1""#.into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
    });

    let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), fast_config()).unwrap();
    server_connected_rx.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while !runtime.connection_state().is_connected() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    send_malformed_tx.send(()).unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.connection_state()
            != polymarket_client_sdk_v2::ws::connection::ConnectionState::Disconnected
        {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(10)).await;

    assert!(matches!(
        runtime
            .replace_and_wait(assets(&[1]), Duration::from_secs(1))
            .await,
        Err(SdkStreamError::Parser(_))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn market_retry_exhaustion_is_terminal_and_visible() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let mut runtime =
        MarketSdkRuntime::new(format!("ws://{address}"), exhausting_config()).unwrap();
    let revision = runtime.replace_desired_assets(assets(&[1])).unwrap();
    assert_eq!(
        runtime
            .wait_desired_applied(revision, Duration::from_secs(2))
            .await,
        Err(SdkStreamError::TerminalDisconnected)
    );
}

#[tokio::test]
async fn rtds_retry_exhaustion_is_terminal_and_visible() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), exhausting_config()).unwrap();
    assert_eq!(
        runtime.wait_connected(Duration::from_secs(2)).await,
        Err(SdkStreamError::TerminalDisconnected)
    );
}
