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
use tokio::sync::{broadcast, mpsc, oneshot, watch};
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

#[cfg(feature = "rtds")]
#[derive(Debug)]
struct RecoverableRtdsNetworkEnvelope {
    source: Error,
    diagnostic: String,
}

#[cfg(feature = "rtds")]
impl std::fmt::Display for RecoverableRtdsNetworkEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RTDS remote endpoint network error; rtds_parse_diagnostic={}",
            self.diagnostic
        )
    }
}

#[cfg(feature = "rtds")]
impl std::error::Error for RecoverableRtdsNetworkEnvelope {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(feature = "rtds")]
pub(crate) fn recoverable_rtds_network_envelope(source: Error, diagnostic: String) -> Error {
    Error::with_source(
        Kind::WebSocket,
        RecoverableRtdsNetworkEnvelope { source, diagnostic },
    )
}

enum ConnectionExit {
    NormalShutdown,
    Recoverable(Error),
    #[cfg(feature = "rtds")]
    RtdsControl,
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

struct PingRequest {
    write_ack_tx: Option<oneshot::Sender<()>>,
}

struct OutboundQueue {
    receiver: mpsc::UnboundedReceiver<OutboundRequest>,
    published_generation: Arc<AtomicU64>,
    active_generation: u64,
}

// 仅 RTDS 控制错误进入此阶段；其他连接仍在握手成功时重置。
struct ConnectionRecovery {
    backoff: backoff::ExponentialBackoff,
    awaiting_rtds_message: bool,
}

impl ConnectionRecovery {
    fn new(config: &Config) -> Self {
        Self {
            backoff: config.reconnect.clone().into(),
            awaiting_rtds_message: false,
        }
    }
}

struct ConnectionSignals {
    state_tx: watch::Sender<ConnectionState>,
    fault_tx: watch::Sender<Option<ConnectionFault>>,
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
/// - Heartbeat PING delivery with optional application-level text PONG monitoring
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
        let outbound_generation = Arc::new(AtomicU64::new(0));
        let outbound_queue = OutboundQueue {
            receiver: sender_rx,
            published_generation: Arc::clone(&outbound_generation),
            active_generation: 0,
        };

        // Spawn connection task
        let recovery = ConnectionRecovery::new(&config);
        let connection_config = config;
        let connection_endpoint = endpoint;
        let broadcast_tx_clone = broadcast_tx.clone();
        let signals = ConnectionSignals {
            state_tx: state_tx.clone(),
            fault_tx,
        };

        tokio::spawn(async move {
            Self::connection_loop(
                connection_endpoint,
                connection_config,
                outbound_queue,
                broadcast_tx_clone,
                parser,
                signals,
                recovery,
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
        mut recovery: ConnectionRecovery,
    ) {
        let mut attempt = 0_u32;
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

            // Bound only the current TCP/TLS/WebSocket handshake when configured. A successful
            // socket has no connection-lifetime timer and remains owned by `handle_connection`.
            let connected = if let Some(connect_timeout) = config.connect_timeout {
                match timeout(connect_timeout, connect_async(&endpoint)).await {
                    Ok(result) => result.map_err(|error| {
                        Error::with_source(Kind::WebSocket, WsError::Connection(error))
                    }),
                    Err(_) => Err(Error::with_source(Kind::WebSocket, WsError::Timeout)),
                }
            } else {
                connect_async(&endpoint).await.map_err(|error| {
                    Error::with_source(Kind::WebSocket, WsError::Connection(error))
                })
            };

            // Attempt connection
            match connected {
                Ok((ws_stream, _)) => {
                    attempt = 0;
                    if !recovery.awaiting_rtds_message {
                        recovery.backoff.reset();
                    }
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
                        &mut recovery,
                    )
                    .await
                    {
                        ConnectionExit::NormalShutdown | ConnectionExit::Parser => {
                            _ = signals.state_tx.send(ConnectionState::Disconnected);
                            break;
                        }
                        #[cfg(feature = "rtds")]
                        ConnectionExit::RtdsControl => {}
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
                Err(error) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Unable to connect: {error:?}");
                    #[cfg(not(feature = "tracing"))]
                    let _: &_ = &error;
                    attempt = attempt.saturating_add(1);
                }
            }

            if outbound.receiver.is_closed() {
                _ = signals.state_tx.send(ConnectionState::Disconnected);
                break;
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

            if let Some(duration) = recovery.backoff.next_backoff() {
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
        recovery: &mut ConnectionRecovery,
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
                                    if recovery.awaiting_rtds_message && !messages.is_empty() {
                                        recovery.backoff.reset();
                                        recovery.awaiting_rtds_message = false;
                                    }
                                    for message in messages {
                                        #[cfg(feature = "tracing")]
                                        tracing::trace!(?message, "Parsed WebSocket message");
                                        _ = broadcast_tx.send(message);
                                    }
                                }
                                Err(error) => {
                                    #[cfg(feature = "rtds")]
                                    if let Some(envelope) = error
                                        .downcast_ref::<RecoverableRtdsNetworkEnvelope>()
                                    {
                                        // 只输出受限诊断，避免递归 Debug 回显原始错误。
                                        eprintln!("RTDS recoverable control error: {}", envelope.diagnostic);
                                        recovery.awaiting_rtds_message = true;
                                        heartbeat_handle.abort();
                                        return ConnectionExit::RtdsControl;
                                    }

                                    let message = error.to_string();
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
                        Some(PingRequest { write_ack_tx }) => {
                            if let Err(error) = write.send(Message::Text("PING".into())).await {
                                heartbeat_handle.abort();
                                return ConnectionExit::Recoverable(Error::with_source(
                                    Kind::WebSocket,
                                    WsError::Connection(error),
                                ));
                            }
                            if let Some(write_ack_tx) = write_ack_tx
                                && write_ack_tx.send(()).is_err()
                            {
                                return Self::classify_heartbeat_result(
                                    (&mut heartbeat_handle).await,
                                );
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

    /// Heartbeat loop that sends PING messages and optionally monitors text PONG responses.
    async fn heartbeat_loop(
        ping_tx: mpsc::UnboundedSender<PingRequest>,
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

            if !config.require_text_pong {
                let (write_ack_tx, write_ack_rx) = oneshot::channel();
                if ping_tx
                    .send(PingRequest {
                        write_ack_tx: Some(write_ack_tx),
                    })
                    .is_err()
                {
                    return HeartbeatExit::OwnerStopped;
                }
                if write_ack_rx.await.is_err() {
                    return HeartbeatExit::OwnerStopped;
                }
                ping_interval.reset();
                continue;
            }

            // Mark current PONG state as seen before sending PING
            // This prevents changed() from returning immediately due to a stale PONG
            drop(pong_rx.borrow_and_update());

            // Send PING request to message loop
            let ping_sent = Instant::now();
            if ping_tx.send(PingRequest { write_ack_tx: None }).is_err() {
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

    #[cfg(feature = "rtds")]
    #[tokio::test(flavor = "current_thread")]
    async fn rtds_control_recovery_keeps_backoff_until_nonempty_typed_messages() {
        use crate::rtds::{RtdsMessage, subscription::SimpleParser};
        use std::time::Duration;

        // 用确定性退避输入检验真实握手、读取和重试循环。
        let config = Config {
            require_text_pong: false,
            heartbeat_interval: Duration::from_secs(30),
            ..Config::default()
        };
        let mut recovery = ConnectionRecovery::new(&config);
        recovery.backoff.initial_interval = Duration::from_millis(80);
        recovery.backoff.current_interval = Duration::from_millis(80);
        recovery.backoff.max_interval = Duration::from_millis(640);
        recovery.backoff.multiplier = 2.0;
        recovery.backoff.randomization_factor = 0.0;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut previous_sent = None;
            for (step, expected_ms) in [0, 80, 160, 320, 640, 80, 80, 80].into_iter().enumerate() {
                let (stream, _) = timeout(Duration::from_secs(3), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                if let Some(sent) = previous_sent {
                    let waited = Instant::now().duration_since(sent);
                    assert!(
                        waited >= Duration::from_millis(expected_ms - 10),
                        "step {step}: {waited:?} < {expected_ms}ms"
                    );
                    assert!(
                        waited < Duration::from_millis(expected_ms + 60),
                        "step {step}: {waited:?} > {expected_ms}ms"
                    );
                }
                if step == 1 {
                    // 恢复阶段内 TCP 成功，但 WebSocket 握手失败。
                    previous_sent = Some(Instant::now());
                    drop(stream);
                    continue;
                }
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                if step == 2 || step == 5 {
                    previous_sent = Some(Instant::now());
                    drop(socket); // 同时覆盖恢复阶段内和阶段外的 EOF。
                    continue;
                }
                if step == 3 {
                    for empty in ["PONG", "   ", "[]"] {
                        socket.send(Message::Text(empty.into())).await.unwrap();
                    }
                }
                if step == 4 {
                    socket.send(Message::Text(r#"{"topic":"crypto_prices_chainlink","type":"update","timestamp":1,"payload":{}}"#.into())).await.unwrap();
                    previous_sent = Some(Instant::now());
                    drop(socket); // 非空消息已结束恢复；后续普通握手再次恢复原重置行为。
                    continue;
                }
                let frame = match step {
                    0 => r#"{"message":"Network error communicating with endpoint"}"#,
                    3 => r#"{"message":"Too Many Requests"}"#,
                    6 => r#"{"message":"Endpoint request timed out"}"#,
                    7 => r#"{"message":"unknown endpoint error"}"#,
                    _ => unreachable!(),
                };
                previous_sent = Some(Instant::now());
                socket.send(Message::Text(frame.into())).await.unwrap();
                // 保持对端连接，直到 SDK 关闭当前代际。
                while let Some(Ok(message)) = socket.next().await {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                }
            }
        });
        let (_sender, receiver) = mpsc::unbounded_channel();
        let generation = Arc::new(AtomicU64::new(0));
        let outbound = OutboundQueue {
            receiver,
            published_generation: Arc::clone(&generation),
            active_generation: 0,
        };
        let (broadcast_tx, mut messages) = broadcast::channel(8);
        let (state_tx, _state_rx) = watch::channel(ConnectionState::Disconnected);
        let (fault_tx, fault_rx) = watch::channel(None);
        timeout(
            Duration::from_secs(10),
            ConnectionManager::<RtdsMessage, SimpleParser>::connection_loop(
                format!("ws://{address}"),
                config,
                outbound,
                broadcast_tx,
                SimpleParser::default(),
                ConnectionSignals { state_tx, fault_tx },
                recovery,
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(generation.load(Ordering::Acquire), 6);
        assert_eq!(messages.try_recv().unwrap().payload, serde_json::json!({}));
        assert!(messages.try_recv().is_err());
        assert!(matches!(
            &*fault_rx.borrow(),
            Some(ConnectionFault::Parser(_))
        ));
    }

    #[cfg(feature = "rtds")]
    #[test]
    fn rtds_control_diagnostics_are_once_and_bounded() {
        // 子进程捕获实际 stderr，分别验证有无 tracing 的构建。
        const CHILD: &str = "SDK_RTDS_DIAGNOSTIC_CHILD";
        if std::env::var_os(CHILD).is_some() {
            #[cfg(feature = "tracing")]
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .with_writer(std::io::stderr)
                .init();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                use crate::rtds::{RtdsMessage, subscription::SimpleParser};
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server = tokio::spawn(async move {
                    for message in ["Network error communicating with endpoint", "Too Many Requests", "Endpoint request timed out"] {
                        let (stream, _) = listener.accept().await.unwrap();
                        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                        // 据诊断重建控制对象，不冒充完整原始网络帧。
                        let mut frame = serde_json::json!({"message": message, "connectionId": "c", "requestId": "r", "private": {"secret": "RAW_SECRET_MUST_NOT_LEAK"}});
                        for index in 0..100 { frame[format!("extra_{index:03}_{}", "界".repeat(20))] = serde_json::json!(null); }
                        socket.send(Message::Text(frame.to_string().into())).await.unwrap();
                        while let Some(Ok(message)) = socket.next().await {
                            if matches!(message, Message::Close(_)) { break; }
                        }
                    }
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                    socket.send(Message::Text(r#"{"message":"unknown"}"#.into())).await.unwrap();
                });
                let mut config = Config::default();
                config.require_text_pong = false;
                config.reconnect.initial_backoff = std::time::Duration::from_millis(1);
                config.reconnect.max_backoff = std::time::Duration::from_millis(2);
                let connection = ConnectionManager::<RtdsMessage, SimpleParser>::new(format!("ws://{address}"), config, SimpleParser::default()).unwrap();
                let mut faults = connection.fault_receiver();
                timeout(std::time::Duration::from_secs(3), async {
                    while faults.borrow_and_update().is_none() { faults.changed().await.unwrap(); }
                }).await.unwrap();
                server.await.unwrap();
            });
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ws::connection::tests::rtds_control_diagnostics_are_once_and_bounded",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        let prefix = "RTDS recoverable control error: ";
        let lines: Vec<_> = stderr
            .lines()
            .filter(|line| line.starts_with(prefix))
            .collect();
        assert_eq!(lines.len(), 3, "{stderr}");
        for line in lines {
            assert!(line.len() <= prefix.len() + 2048);
            assert!(
                line.len() >= prefix.len() + 2000,
                "exercise truncation boundary"
            );
            assert!(line.contains("top_level=object"));
        }
        assert!(!stderr.contains("RAW_SECRET_MUST_NOT_LEAK"));
        assert!(!stderr.contains("RecoverableRtdsNetworkEnvelope"));
        assert!(!stderr.contains("Error handling connection"));
    }

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
        let signals = ConnectionSignals { state_tx, fault_tx };
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
                &mut ConnectionRecovery::new(&Config::default()),
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
                &mut ConnectionRecovery::new(&Config::default()),
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
        let ping = timeout(std::time::Duration::from_secs(1), ping_rx.recv())
            .await
            .expect("heartbeat PING")
            .expect("heartbeat sender");
        assert!(ping.write_ack_tx.is_none());
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

    #[tokio::test]
    async fn send_only_heartbeat_waits_for_write_ack_before_scheduling_next_ping() {
        let config = Config {
            heartbeat_interval: std::time::Duration::from_millis(20),
            heartbeat_timeout: std::time::Duration::from_millis(1),
            require_text_pong: false,
            ..Config::default()
        };
        let (ping_tx, mut ping_rx) = mpsc::unbounded_channel();
        let (_state_tx, state_rx) = watch::channel(ConnectionState::Connected {
            since: Instant::now(),
        });
        let (pong_tx, pong_rx) = watch::channel(Instant::now());

        let heartbeat = tokio::spawn(async move {
            ConnectionManager::<TestMessage, TestParser>::heartbeat_loop(
                ping_tx, state_rx, &config, pong_rx,
            )
            .await
        });
        let first = timeout(std::time::Duration::from_secs(1), ping_rx.recv())
            .await
            .expect("first send-only PING")
            .expect("heartbeat sender");
        let first_ack = first.write_ack_tx.expect("send-only PING write ack");
        tokio::time::sleep(std::time::Duration::from_millis(70)).await;
        assert!(
            matches!(ping_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "a blocked write must not accumulate PING requests"
        );
        first_ack.send(()).unwrap();
        pong_tx.send(Instant::now()).unwrap();
        assert!(
            timeout(std::time::Duration::from_millis(10), ping_rx.recv())
                .await
                .is_err(),
            "write completion or a late PONG must not trigger a catch-up PING burst"
        );
        let second = timeout(std::time::Duration::from_millis(50), ping_rx.recv())
            .await
            .expect("next PING after one full reset interval")
            .expect("heartbeat sender");
        assert!(second.write_ack_tx.is_some());
        drop(second);
        drop(ping_rx);

        match timeout(std::time::Duration::from_secs(1), heartbeat)
            .await
            .expect("heartbeat stops when its write ack closes")
            .unwrap()
        {
            HeartbeatExit::OwnerStopped => {}
            HeartbeatExit::Recoverable(_) => {
                panic!("send-only write-ack closure is an owner stop")
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_only_heartbeat_ping_write_failure_is_recoverable() {
        let (socket, release_tx, server) = locally_write_shutdown_socket().await;
        let (_sender, mut outbound, broadcast_tx, mut signals) = connection_test_context();
        let config = Config {
            heartbeat_interval: std::time::Duration::from_millis(1),
            heartbeat_timeout: std::time::Duration::from_secs(1),
            require_text_pong: false,
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
                &mut ConnectionRecovery::new(&Config::default()),
            ),
        )
        .await
        .expect("send-only heartbeat PING write failure");
        assert_recoverable_connection_error(exit);
        release_tx.send(()).unwrap();
        timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("send-only heartbeat PING test server cleanup")
            .unwrap();
    }

    #[cfg(all(feature = "tracing", feature = "rtds"))]
    #[tokio::test(flavor = "current_thread")]
    async fn rtds_raw_trace_is_explicit_but_warn_and_sticky_fault_are_bounded() {
        // 隔离 tracing 的全局调用点缓存，避免其他无订阅者连接测试影响 TRACE 捕获。
        const CHILD: &str = "SDK_RTDS_RAW_TRACE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ws::connection::tests::rtds_raw_trace_is_explicit_but_warn_and_sticky_fault_are_bounded",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        use tracing_subscriber::layer::SubscriberExt as _;

        use crate::rtds::RtdsMessage;
        use crate::rtds::subscription::SimpleParser;

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
        assert!(diagnostic.contains("top_level=object"));
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
        let mut signals = ConnectionSignals { state_tx, fault_tx };
        let result = ConnectionManager::<TestMessage, TestParser>::handle_connection(
            socket,
            &mut outbound,
            &broadcast_tx,
            Config::default(),
            &TestParser,
            &mut signals,
            &mut ConnectionRecovery::new(&Config::default()),
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
