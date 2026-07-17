#![expect(
    clippy::module_name_repetitions,
    reason = "Connection types expose their domain in the name for clarity"
)]

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use backoff::backoff::Backoff as _;
use futures::{SinkExt as _, StreamExt as _};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use super::config::Config;
use super::error::WsError;
use super::traits::MessageParser;
use crate::auth::Credentials;
use crate::error::Kind;
use crate::ws::WithCredentials;
use crate::{Result, error::Error};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Broadcast channel capacity for incoming messages.
const BROADCAST_CAPACITY: usize = 1024;

/// Connection state tracking.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Attempting to connect
    Connecting,
    /// Successfully connected
    Connected {
        /// When the connection was established
        since: Instant,
    },
    /// Reconnecting after failure
    Reconnecting {
        /// Current reconnection attempt number
        attempt: u32,
    },
}

/// Terminal connection faults that must be handled by the caller.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionFault {
    /// The server frame could not be decoded by the configured typed parser.
    Parser(String),
}

enum ConnectionExit {
    NormalShutdown,
    Recoverable(Error),
    RequestedReconnect,
    Parser,
    Terminal(String),
}

enum HeartbeatExit {
    OwnerStopped,
    Recoverable(Error),
}

struct OutboundRequest {
    generation: u64,
    payload: String,
}

struct OutboundQueue {
    receiver: mpsc::UnboundedReceiver<OutboundRequest>,
    published_generation: Arc<AtomicU64>,
    active_generation: u64,
}

struct ConnectionSignals {
    state_tx: watch::Sender<ConnectionState>,
    fault_tx: watch::Sender<Option<ConnectionFault>>,
    reconnect_tx: watch::Sender<Option<u64>>,
    reconnect_rx: watch::Receiver<Option<u64>>,
}

#[derive(Debug)]
enum GenerationAdvanceError {
    SenderClosed,
    Overflow,
}

impl OutboundQueue {
    fn advance_generation(&mut self) -> std::result::Result<(), GenerationAdvanceError> {
        let Some(next_generation) = self.active_generation.checked_add(1) else {
            return Err(GenerationAdvanceError::Overflow);
        };

        // Only drain the finite snapshot that was queued before this handshake. The published
        // generation remains unchanged until the drain finishes, so every drained request is
        // from an older connection. A sender that already loaded the old generation but finishes
        // enqueueing later is still rejected by the active socket's generation check.
        let queued = self.receiver.len();
        for _ in 0..queued {
            match self.receiver.try_recv() {
                Ok(_request) => {}
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(GenerationAdvanceError::SenderClosed);
                }
            }
        }
        if self.receiver.is_closed() {
            return Err(GenerationAdvanceError::SenderClosed);
        }

        self.active_generation = next_generation;
        self.published_generation
            .store(next_generation, Ordering::Release);
        Ok(())
    }
}

impl ConnectionState {
    /// Check if the connection is currently active.
    #[must_use]
    pub const fn is_connected(self) -> bool {
        matches!(self, Self::Connected { .. })
    }
}

/// Manages WebSocket connection lifecycle, reconnection, and heartbeat.
///
/// This generic connection manager handles all WebSocket connection concerns:
/// - Establishing and maintaining connections
/// - Automatic reconnection with exponential backoff
/// - Heartbeat monitoring via PING/PONG
/// - Broadcasting messages to multiple subscribers
///
/// # Type Parameters
///
/// - `M`: Message type that implements [`DeserializeOwned`] among other "helper" types
/// - `P`: Parser type that implements [`MessageParser<M>`]
///
/// # Example
///
/// ```ignore
/// let parser = SimpleParser;
/// let connection = ConnectionManager::new(
///     "wss://example.com".to_owned(),
///     config,
///     parser,
/// )?;
///
/// // Subscribe to messages
/// let mut rx = connection.subscribe();
/// while let Ok(msg) = rx.recv().await {
///     println!("Received: {:?}", msg);
/// }
/// ```
#[derive(Clone)]
pub struct ConnectionManager<M, P>
where
    M: DeserializeOwned + Debug + Clone + Send + 'static,
    P: MessageParser<M>,
{
    /// Watch channel sender for state changes (enables reconnection detection)
    state_tx: watch::Sender<ConnectionState>,
    /// Watch channel receiver for state changes (for use in checking the current state)
    state_rx: watch::Receiver<ConnectionState>,
    /// Sticky terminal fault notification.
    fault_rx: watch::Receiver<Option<ConnectionFault>>,
    /// Sender channel for outgoing messages
    sender_tx: mpsc::UnboundedSender<OutboundRequest>,
    /// Generation assigned to newly enqueued outbound requests.
    outbound_generation: Arc<AtomicU64>,
    /// Optional generation fixed by a caller that must not send into a later socket.
    bound_outbound_generation: Option<u64>,
    /// Latest generation whose active socket should reconnect once.
    reconnect_tx: watch::Sender<Option<u64>>,
    /// Broadcast sender for incoming messages
    broadcast_tx: broadcast::Sender<M>,
    /// Phantom data for unused type parameters
    _phantom: PhantomData<P>,
}

impl<M, P> ConnectionManager<M, P>
where
    M: DeserializeOwned + Debug + Clone + Send + 'static,
    P: MessageParser<M>,
{
    /// Create a new connection manager and start the connection loop.
    ///
    /// The `parser` is used to deserialize incoming WebSocket messages.
    /// The connection loop runs in a background task and automatically
    /// handles reconnection according to the config's `ReconnectConfig`.
    pub fn new(endpoint: String, config: Config, parser: P) -> Result<Self> {
        let (sender_tx, sender_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);
        let (fault_tx, fault_rx) = watch::channel(None);
        let (reconnect_tx, reconnect_rx) = watch::channel(None);
        let outbound_generation = Arc::new(AtomicU64::new(0));
        let outbound_queue = OutboundQueue {
            receiver: sender_rx,
            published_generation: Arc::clone(&outbound_generation),
            active_generation: 0,
        };

        // Spawn connection task
        let connection_config = config;
        let connection_endpoint = endpoint;
        let broadcast_tx_clone = broadcast_tx.clone();
        let signals = ConnectionSignals {
            state_tx: state_tx.clone(),
            fault_tx,
            reconnect_tx: reconnect_tx.clone(),
            reconnect_rx,
        };

        tokio::spawn(async move {
            Self::connection_loop(
                connection_endpoint,
                connection_config,
                outbound_queue,
                broadcast_tx_clone,
                parser,
                signals,
            )
            .await;
        });

        Ok(Self {
            state_tx,
            state_rx,
            fault_rx,
            sender_tx,
            outbound_generation,
            bound_outbound_generation: None,
            reconnect_tx,
            broadcast_tx,
            _phantom: PhantomData,
        })
    }

    /// Main connection loop with automatic reconnection.
    async fn connection_loop(
        endpoint: String,
        config: Config,
        mut outbound: OutboundQueue,
        broadcast_tx: broadcast::Sender<M>,
        parser: P,
        mut signals: ConnectionSignals,
    ) {
        let mut attempt = 0_u32;
        let mut backoff: backoff::ExponentialBackoff = config.reconnect.clone().into();
        let mut has_connected = false;

        loop {
            // Check if ConnectionManager was dropped (all sender_tx instances gone)
            if outbound.receiver.is_closed() {
                #[cfg(feature = "tracing")]
                tracing::debug!("Sender channel closed, stopping connection loop");
                _ = signals.state_tx.send(ConnectionState::Disconnected);
                break;
            }

            _ = signals.state_tx.send(ConnectionState::Connecting);

            // Attempt connection
            match connect_async(&endpoint).await {
                Ok((ws_stream, _)) => {
                    attempt = 0;
                    backoff.reset();
                    if has_connected {
                        match outbound.advance_generation() {
                            Ok(()) => {}
                            Err(GenerationAdvanceError::SenderClosed) => {
                                _ = signals.state_tx.send(ConnectionState::Disconnected);
                                break;
                            }
                            Err(GenerationAdvanceError::Overflow) => {
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    "Connection generation overflow is a terminal invariant failure"
                                );
                                _ = signals.state_tx.send(ConnectionState::Disconnected);
                                break;
                            }
                        }
                    } else {
                        has_connected = true;
                    }
                    // A ticket for an older socket must not survive into the new Connected epoch.
                    // The state is still Connecting here, so a caller cannot submit a valid ticket
                    // for the new generation until after this reset has been observed locally.
                    signals.reconnect_tx.send_replace(None);
                    drop(signals.reconnect_rx.borrow_and_update());
                    _ = signals.state_tx.send(ConnectionState::Connected {
                        since: Instant::now(),
                    });

                    // Handle connection
                    match Self::handle_connection(
                        ws_stream,
                        &mut outbound,
                        &broadcast_tx,
                        config.clone(),
                        &parser,
                        &mut signals,
                    )
                    .await
                    {
                        ConnectionExit::NormalShutdown | ConnectionExit::Parser => {
                            _ = signals.state_tx.send(ConnectionState::Disconnected);
                            break;
                        }
                        ConnectionExit::RequestedReconnect => {}
                        ConnectionExit::Recoverable(e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!("Error handling connection: {e:?}");
                            #[cfg(not(feature = "tracing"))]
                            let _: &_ = &e;
                        }
                        ConnectionExit::Terminal(message) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(%message, "Terminal WebSocket connection task failure");
                            #[cfg(not(feature = "tracing"))]
                            let _: &_ = &message;
                            _ = signals.state_tx.send(ConnectionState::Disconnected);
                            break;
                        }
                    }
                }
                Err(e) => {
                    let error = Error::with_source(Kind::WebSocket, WsError::Connection(e));
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Unable to connect: {error:?}");
                    #[cfg(not(feature = "tracing"))]
                    let _: &_ = &error;
                    attempt = attempt.saturating_add(1);
                }
            }

            // Check if we should stop reconnecting
            if let Some(max) = config.reconnect.max_attempts
                && attempt >= max
            {
                _ = signals.state_tx.send(ConnectionState::Disconnected);
                break;
            }

            // Update state and wait with exponential backoff
            _ = signals
                .state_tx
                .send(ConnectionState::Reconnecting { attempt });

            if let Some(duration) = backoff.next_backoff() {
                sleep(duration).await;
            }
        }
    }

    /// Handle an active WebSocket connection.
    async fn handle_connection(
        ws_stream: WsStream,
        outbound: &mut OutboundQueue,
        broadcast_tx: &broadcast::Sender<M>,
        config: Config,
        parser: &P,
        signals: &mut ConnectionSignals,
    ) -> ConnectionExit {
        let (mut write, mut read) = ws_stream.split();

        // Channel to notify heartbeat loop when PONG is received
        let (pong_tx, pong_rx) = watch::channel(Instant::now());
        let (ping_tx, mut ping_rx) = mpsc::unbounded_channel();
        let state_rx = signals.state_tx.subscribe();

        let mut heartbeat_handle =
            tokio::spawn(
                async move { Self::heartbeat_loop(ping_tx, state_rx, &config, pong_rx).await },
            );

        loop {
            tokio::select! {
                // Handle incoming messages
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) if text == "PONG" => {
                            if pong_tx.send(Instant::now()).is_err() {
                                return Self::classify_heartbeat_result(
                                    (&mut heartbeat_handle).await,
                                );
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            #[cfg(feature = "tracing")]
                            tracing::trace!(%text, "Received WebSocket text message");

                            // Parse messages using the provided parser
                            match parser.parse(text.as_bytes()) {
                                Ok(messages) => {
                                    for message in messages {
                                        #[cfg(feature = "tracing")]
                                        tracing::trace!(?message, "Parsed WebSocket message");
                                        _ = broadcast_tx.send(message);
                                    }
                                }
                                Err(e) => {
                                    let message = e.to_string();
                                    signals.fault_tx.send_replace(Some(ConnectionFault::Parser(message.clone())));
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(error = %message, "Failed to parse WebSocket message");
                                    #[cfg(not(feature = "tracing"))]
                                    let _: &_ = &message;
                                    heartbeat_handle.abort();
                                    return ConnectionExit::Parser;
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            heartbeat_handle.abort();
                            return ConnectionExit::Recoverable(Error::with_source(
                                Kind::WebSocket,
                                WsError::ConnectionClosed,
                            ));
                        }
                        Some(Err(e)) => {
                            heartbeat_handle.abort();
                            return ConnectionExit::Recoverable(Error::with_source(
                                Kind::WebSocket,
                                WsError::Connection(e),
                            ));
                        }
                        Some(Ok(_)) => {
                            // Ignore binary frames and unsolicited PONG replies.
                        }
                    }
                }

                changed = signals.reconnect_rx.changed() => {
                    if changed.is_err() {
                        heartbeat_handle.abort();
                        return ConnectionExit::Terminal(
                            "reconnect control channel closed while its owner sender was live"
                                .to_owned(),
                        );
                    }
                    let requested_generation = *signals.reconnect_rx.borrow_and_update();
                    if requested_generation == Some(outbound.active_generation) {
                        heartbeat_handle.abort();
                        return ConnectionExit::RequestedReconnect;
                    }
                }

                // Handle outgoing messages from subscriptions
                request = outbound.receiver.recv() => {
                    let Some(request) = request else {
                        heartbeat_handle.abort();
                        return ConnectionExit::NormalShutdown;
                    };
                    if request.generation != outbound.active_generation {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            request_generation = request.generation,
                            connection_generation = outbound.active_generation,
                            "Dropping outbound request from an older connection generation"
                        );
                        continue;
                    }
                    if let Err(error) = write.send(Message::Text(request.payload.into())).await {
                        heartbeat_handle.abort();
                        return ConnectionExit::Recoverable(Error::with_source(
                            Kind::WebSocket,
                            WsError::Connection(error),
                        ));
                    }
                }

                // Handle PING requests from heartbeat loop
                ping = ping_rx.recv() => {
                    match ping {
                        Some(()) => {
                            if let Err(error) = write.send(Message::Text("PING".into())).await {
                                heartbeat_handle.abort();
                                return ConnectionExit::Recoverable(Error::with_source(
                                    Kind::WebSocket,
                                    WsError::Connection(error),
                                ));
                            }
                        }
                        None => {
                            return Self::classify_heartbeat_result(
                                (&mut heartbeat_handle).await,
                            );
                        }
                    }
                }

                heartbeat_result = &mut heartbeat_handle => {
                    return Self::classify_heartbeat_result(heartbeat_result);
                }
            }
        }
    }

    fn classify_heartbeat_result(
        result: std::result::Result<HeartbeatExit, tokio::task::JoinError>,
    ) -> ConnectionExit {
        match result {
            Ok(HeartbeatExit::Recoverable(error)) => ConnectionExit::Recoverable(error),
            Ok(HeartbeatExit::OwnerStopped) => ConnectionExit::Terminal(
                "heartbeat task stopped while the connection owner was still active".to_owned(),
            ),
            Err(error) => ConnectionExit::Terminal(format!("heartbeat task failed: {error}")),
        }
    }

    /// Heartbeat loop that sends PING messages and monitors PONG responses.
    async fn heartbeat_loop(
        ping_tx: mpsc::UnboundedSender<()>,
        state_rx: watch::Receiver<ConnectionState>,
        config: &Config,
        mut pong_rx: watch::Receiver<Instant>,
    ) -> HeartbeatExit {
        let mut ping_interval = interval(config.heartbeat_interval);

        loop {
            ping_interval.tick().await;

            // Check if still connected
            if !state_rx.borrow().is_connected() {
                return HeartbeatExit::OwnerStopped;
            }

            // Mark current PONG state as seen before sending PING
            // This prevents changed() from returning immediately due to a stale PONG
            drop(pong_rx.borrow_and_update());

            // Send PING request to message loop
            let ping_sent = Instant::now();
            if ping_tx.send(()).is_err() {
                // Message loop has terminated
                return HeartbeatExit::OwnerStopped;
            }

            // Wait for PONG within timeout
            let pong_result = timeout(config.heartbeat_timeout, pong_rx.changed()).await;

            match pong_result {
                Ok(Ok(())) => {
                    let last_pong = *pong_rx.borrow_and_update();
                    if last_pong < ping_sent {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            "PONG received but older than last PING, connection may be stale"
                        );
                        return HeartbeatExit::Recoverable(Error::with_source(
                            Kind::WebSocket,
                            WsError::Timeout,
                        ));
                    }
                }
                Ok(Err(_)) => {
                    // Channel closed, connection is terminating
                    return HeartbeatExit::OwnerStopped;
                }
                Err(_) => {
                    // Timeout waiting for PONG
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        "Heartbeat timeout: no PONG received within {:?}",
                        config.heartbeat_timeout
                    );
                    return HeartbeatExit::Recoverable(Error::with_source(
                        Kind::WebSocket,
                        WsError::Timeout,
                    ));
                }
            }
        }
    }

    /// Send a subscription request to the WebSocket server.
    pub fn send<R: Serialize>(&self, request: &R) -> Result<()> {
        let generation = self
            .bound_outbound_generation
            .unwrap_or_else(|| self.outbound_generation.load(Ordering::Acquire));
        let json = serde_json::to_string(request)?;
        self.sender_tx
            .send(OutboundRequest {
                generation,
                payload: json,
            })
            .map_err(|_e| WsError::ConnectionClosed)?;
        Ok(())
    }

    /// Send a subscription request to the WebSocket server.
    pub fn send_authenticated<R: WithCredentials>(
        &self,
        request: &R,
        credentials: &Credentials,
    ) -> Result<()> {
        let generation = self
            .bound_outbound_generation
            .unwrap_or_else(|| self.outbound_generation.load(Ordering::Acquire));
        let json = request.as_authenticated(credentials)?;
        self.sender_tx
            .send(OutboundRequest {
                generation,
                payload: json,
            })
            .map_err(|_e| WsError::ConnectionClosed)?;
        Ok(())
    }

    /// Return the generation assigned to the active connection and new outbound requests.
    #[must_use]
    pub fn connection_generation(&self) -> u64 {
        self.outbound_generation.load(Ordering::Acquire)
    }

    /// Clone this manager with outbound requests fixed to the observed active generation.
    ///
    /// This is intended for generation-scoped subscription actors. A request serialized after a
    /// concurrent reconnect still carries the caller's observed generation and is therefore
    /// rejected by a later socket instead of being misattributed to it.
    #[must_use]
    pub fn bind_generation(&self, observed_generation: u64) -> Option<Self>
    where
        P: Clone,
    {
        if !self.state().is_connected() || self.connection_generation() != observed_generation {
            return None;
        }
        let mut bound = self.clone();
        bound.bound_outbound_generation = Some(observed_generation);
        Some(bound)
    }

    /// Request a one-shot reconnect of the exact generation observed by the caller.
    ///
    /// Returns `true` only when this call publishes a new ticket. Requests made while the
    /// connection is not active, requests for stale or future generations, and duplicates are
    /// no-ops. The active socket validates the generation again before consuming the ticket.
    #[must_use]
    pub fn request_reconnect(&self, observed_generation: u64) -> bool {
        if !self.state().is_connected() || self.connection_generation() != observed_generation {
            return false;
        }

        self.reconnect_tx.send_if_modified(|ticket| {
            // Recheck while holding the watch value so a delayed stale caller cannot overwrite a
            // ticket published for a newer Connected epoch.
            if !self.state().is_connected()
                || self.connection_generation() != observed_generation
                || ticket.is_some_and(|generation| generation >= observed_generation)
            {
                return false;
            }
            *ticket = Some(observed_generation);
            true
        })
    }

    /// Get the current connection state.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Subscribe to incoming messages.
    ///
    /// Each call returns a new independent receiver. Multiple subscribers can
    /// receive messages concurrently without blocking each other.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<M> {
        self.broadcast_tx.subscribe()
    }

    /// Subscribe to connection state changes.
    ///
    /// Returns a receiver that notifies when the connection state changes.
    /// This is useful for detecting reconnections and re-establishing subscriptions.
    #[must_use]
    pub fn state_receiver(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }

    /// Subscribe to the sticky terminal connection fault.
    #[must_use]
    pub fn fault_receiver(&self) -> watch::Receiver<Option<ConnectionFault>> {
        self.fault_rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "tracing", feature = "rtds"))]
    struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    #[cfg(all(feature = "tracing", feature = "rtds"))]
    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("capture lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(Clone, Debug, serde::Deserialize)]
    struct TestMessage;

    struct TestParser;

    impl MessageParser<TestMessage> for TestParser {
        fn parse(&self, _bytes: &[u8]) -> Result<Vec<TestMessage>> {
            Ok(Vec::new())
        }
    }

    async fn locally_write_shutdown_socket() -> (
        WsStream,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::AsyncWriteExt as _;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            release_rx.await.unwrap();
        });
        let (mut socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
        socket.get_mut().shutdown().await.unwrap();
        (socket, release_tx, server)
    }

    fn connection_test_context() -> (
        mpsc::UnboundedSender<OutboundRequest>,
        OutboundQueue,
        broadcast::Sender<TestMessage>,
        ConnectionSignals,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let outbound = OutboundQueue {
            receiver,
            published_generation: Arc::new(AtomicU64::new(0)),
            active_generation: 0,
        };
        let (broadcast_tx, _) = broadcast::channel(1);
        let (state_tx, _) = watch::channel(ConnectionState::Connected {
            since: Instant::now(),
        });
        let (fault_tx, _) = watch::channel(None);
        let (reconnect_tx, reconnect_rx) = watch::channel(None);
        let signals = ConnectionSignals {
            state_tx,
            fault_tx,
            reconnect_tx,
            reconnect_rx,
        };
        (sender, outbound, broadcast_tx, signals)
    }

    fn assert_recoverable_connection_error(exit: ConnectionExit) {
        let ConnectionExit::Recoverable(error) = exit else {
            panic!("WebSocket write failure must be recoverable");
        };
        assert!(matches!(
            error.downcast_ref::<WsError>(),
            Some(WsError::Connection(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn outbound_write_failure_is_recoverable() {
        let (socket, release_tx, server) = locally_write_shutdown_socket().await;
        let (sender, mut outbound, broadcast_tx, mut signals) = connection_test_context();
        sender
            .send(OutboundRequest {
                generation: 0,
                payload: "outbound".to_owned(),
            })
            .unwrap();

        let exit = timeout(
            std::time::Duration::from_secs(1),
            ConnectionManager::<TestMessage, TestParser>::handle_connection(
                socket,
                &mut outbound,
                &broadcast_tx,
                Config::default(),
                &TestParser,
                &mut signals,
            ),
        )
        .await
        .expect("outbound write failure");
        assert_recoverable_connection_error(exit);
        release_tx.send(()).unwrap();
        timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("outbound write test server cleanup")
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_ping_write_failure_is_recoverable() {
        let (socket, release_tx, server) = locally_write_shutdown_socket().await;
        let (_sender, mut outbound, broadcast_tx, mut signals) = connection_test_context();
        let config = Config {
            heartbeat_interval: std::time::Duration::from_millis(1),
            heartbeat_timeout: std::time::Duration::from_secs(1),
            ..Config::default()
        };

        let exit = timeout(
            std::time::Duration::from_secs(1),
            ConnectionManager::<TestMessage, TestParser>::handle_connection(
                socket,
                &mut outbound,
                &broadcast_tx,
                config,
                &TestParser,
                &mut signals,
            ),
        )
        .await
        .expect("heartbeat PING write failure");
        assert_recoverable_connection_error(exit);
        release_tx.send(()).unwrap();
        timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("heartbeat PING test server cleanup")
            .unwrap();
    }

    #[tokio::test]
    async fn old_pong_is_a_recoverable_heartbeat_timeout() {
        let config = Config {
            heartbeat_interval: std::time::Duration::from_millis(1),
            heartbeat_timeout: std::time::Duration::from_secs(1),
            ..Config::default()
        };
        let (ping_tx, mut ping_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = watch::channel(ConnectionState::Connected {
            since: Instant::now(),
        });
        let old_pong = Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let (pong_tx, pong_rx) = watch::channel(Instant::now());

        let heartbeat = tokio::spawn(async move {
            ConnectionManager::<TestMessage, TestParser>::heartbeat_loop(
                ping_tx, state_rx, &config, pong_rx,
            )
            .await
        });
        timeout(std::time::Duration::from_secs(1), ping_rx.recv())
            .await
            .expect("heartbeat PING")
            .expect("heartbeat sender");
        pong_tx.send(old_pong).unwrap();

        match heartbeat.await.unwrap() {
            HeartbeatExit::Recoverable(error) => {
                assert!(matches!(
                    error.downcast_ref::<WsError>(),
                    Some(WsError::Timeout)
                ));
            }
            HeartbeatExit::OwnerStopped => panic!("old PONG must be recoverable"),
        }
        drop(state_tx);
    }

    #[cfg(all(feature = "tracing", feature = "rtds"))]
    #[tokio::test(flavor = "current_thread")]
    async fn rtds_raw_trace_is_explicit_but_warn_and_sticky_fault_are_bounded() {
        use crate::rtds::RtdsMessage;
        use crate::rtds::subscription::SimpleParser;
        use tracing_subscriber::layer::SubscriberExt as _;

        const INVALID_SCALAR: &str = "INVALID_TIMESTAMP_MUST_NOT_LEAK";
        const NESTED_SECRET: &str = "NESTED_PAYLOAD_MUST_NOT_LEAK";
        let frame = serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": INVALID_SCALAR,
            "payload": {"secret": NESTED_SECRET},
        })
        .to_string();

        let captured = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let writer_capture = Arc::clone(&captured);
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(move || CaptureWriter(Arc::clone(&writer_capture)))
            .with_ansi(false);
        let subscriber = tracing_subscriber::registry().with(layer);
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_frame = frame.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(server_frame.into()))
                .await
                .unwrap();
            assert!(
                timeout(std::time::Duration::from_millis(300), listener.accept())
                    .await
                    .is_err(),
                "parser fault must remain terminal"
            );
        });

        let config = Config {
            heartbeat_interval: std::time::Duration::from_secs(30),
            heartbeat_timeout: std::time::Duration::from_secs(30),
            ..Config::default()
        };
        let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(
            format!("ws://{address}"),
            config,
            SimpleParser::default(),
        )
        .unwrap();
        let mut faults = connection.fault_receiver();
        let sticky = timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(ConnectionFault::Parser(message)) = faults.borrow().clone() {
                    break message;
                }
                faults.changed().await.unwrap();
            }
        })
        .await
        .expect("sticky parser fault");
        assert!(!sticky.contains(INVALID_SCALAR));
        assert!(!sticky.contains(NESTED_SECRET));
        let diagnostic = sticky
            .split_once("rtds_parse_diagnostic=")
            .expect("bounded diagnostic")
            .1;
        assert!(diagnostic.len() <= 2_048);
        assert!(diagnostic.contains("sha256="));
        let late_faults = connection.fault_receiver();
        assert_eq!(
            late_faults.borrow().as_ref(),
            Some(&ConnectionFault::Parser(sticky.clone()))
        );
        server.await.unwrap();

        let output = String::from_utf8(captured.lock().expect("capture lock").clone()).unwrap();
        let trace = output
            .lines()
            .find(|line| line.contains("Received WebSocket text message"))
            .expect("approved pre-parse TRACE");
        assert!(trace.contains(INVALID_SCALAR));
        assert!(trace.contains(NESTED_SECRET));
        let warn = output
            .lines()
            .find(|line| line.contains("Failed to parse WebSocket message"))
            .expect("parser failure WARN");
        assert!(!warn.contains(INVALID_SCALAR));
        assert!(!warn.contains(NESTED_SECRET));
        assert!(warn.contains("rtds_parse_diagnostic="));
    }

    #[test]
    fn reconnect_generation_drains_the_queued_snapshot_before_publication() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let published_generation = Arc::new(AtomicU64::new(0));
        let mut outbound = OutboundQueue {
            receiver,
            published_generation: Arc::clone(&published_generation),
            active_generation: 0,
        };

        for sequence in 0..10_000 {
            sender
                .send(OutboundRequest {
                    generation: 0,
                    payload: sequence.to_string(),
                })
                .unwrap();
        }
        assert_eq!(outbound.receiver.len(), 10_000);

        outbound.advance_generation().unwrap();

        assert_eq!(outbound.receiver.len(), 0);
        assert_eq!(outbound.active_generation, 1);
        assert_eq!(published_generation.load(Ordering::Acquire), 1);

        sender
            .send(OutboundRequest {
                generation: 0,
                payload: "late-old".to_owned(),
            })
            .unwrap();
        sender
            .send(OutboundRequest {
                generation: 1,
                payload: "current".to_owned(),
            })
            .unwrap();
        let late_old = outbound.receiver.try_recv().unwrap();
        let current = outbound.receiver.try_recv().unwrap();
        assert_ne!(late_old.generation, outbound.active_generation);
        assert_eq!(current.generation, outbound.active_generation);
        assert_eq!(current.payload, "current");
    }

    #[test]
    fn reconnect_generation_does_not_wrap() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let published_generation = Arc::new(AtomicU64::new(u64::MAX));
        let mut outbound = OutboundQueue {
            receiver,
            published_generation: Arc::clone(&published_generation),
            active_generation: u64::MAX,
        };

        assert!(matches!(
            outbound.advance_generation(),
            Err(GenerationAdvanceError::Overflow)
        ));
        assert_eq!(published_generation.load(Ordering::Acquire), u64::MAX);
        drop(sender);
    }

    #[test]
    fn reconnect_generation_stops_when_all_senders_are_closed() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let published_generation = Arc::new(AtomicU64::new(0));
        let mut outbound = OutboundQueue {
            receiver,
            published_generation: Arc::clone(&published_generation),
            active_generation: 0,
        };
        drop(sender);

        assert!(matches!(
            outbound.advance_generation(),
            Err(GenerationAdvanceError::SenderClosed)
        ));
        assert_eq!(outbound.active_generation, 0);
        assert_eq!(published_generation.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn active_socket_drops_a_late_old_generation_before_writing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let message = timeout(std::time::Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(message.into_text().unwrap(), "current");
            socket.send(Message::Close(None)).await.unwrap();
        });
        let (socket, _) = connect_async(format!("ws://{address}")).await.unwrap();

        let (sender, receiver) = mpsc::unbounded_channel();
        let published_generation = Arc::new(AtomicU64::new(0));
        let mut outbound = OutboundQueue {
            receiver,
            published_generation,
            active_generation: 0,
        };
        let late_old = OutboundRequest {
            generation: 0,
            payload: "late-old".to_owned(),
        };
        outbound.advance_generation().unwrap();
        sender.send(late_old).unwrap();
        sender
            .send(OutboundRequest {
                generation: 1,
                payload: "current".to_owned(),
            })
            .unwrap();

        let (broadcast_tx, _) = broadcast::channel(1);
        let (state_tx, _state_rx) = watch::channel(ConnectionState::Connected {
            since: Instant::now(),
        });
        let (fault_tx, _fault_rx) = watch::channel(None);
        let (reconnect_tx, reconnect_rx) = watch::channel(None);
        let mut signals = ConnectionSignals {
            state_tx,
            fault_tx,
            reconnect_tx,
            reconnect_rx,
        };
        let result = ConnectionManager::<TestMessage, TestParser>::handle_connection(
            socket,
            &mut outbound,
            &broadcast_tx,
            Config::default(),
            &TestParser,
            &mut signals,
        )
        .await;

        assert!(matches!(result, ConnectionExit::Recoverable(_)));
        server.await.unwrap();
    }

    #[test]
    fn repeated_reconnects_do_not_grow_the_queued_high_water_mark() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let published_generation = Arc::new(AtomicU64::new(0));
        let mut outbound = OutboundQueue {
            receiver,
            published_generation,
            active_generation: 0,
        };
        let mut high_water = 0;

        for generation in 0..10_000 {
            sender
                .send(OutboundRequest {
                    generation,
                    payload: generation.to_string(),
                })
                .unwrap();
            high_water = high_water.max(outbound.receiver.len());
            outbound.advance_generation().unwrap();
            assert_eq!(outbound.receiver.len(), 0);
        }

        assert_eq!(high_water, 1);
        assert_eq!(outbound.active_generation, 10_000);
    }
}
