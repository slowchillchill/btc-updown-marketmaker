use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use polymarket_client_sdk_v2::clob::ws::interest::InterestTracker;
use polymarket_client_sdk_v2::clob::ws::subscription::SubscriptionManager as MarketSubscriptionManager;
use polymarket_client_sdk_v2::clob::ws::types::response::WsMessage;
use polymarket_client_sdk_v2::rtds::subscription::{
    SimpleParser, SubscriptionManager as RtdsSubscriptionManager,
};
use polymarket_client_sdk_v2::rtds::{RtdsMessage, Subscription};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config;
use polymarket_client_sdk_v2::ws::connection::{
    ConnectionFault, ConnectionManager, ConnectionState,
};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;

type MarketConnection = ConnectionManager<WsMessage, Arc<InterestTracker>>;
type RtdsConnection = ConnectionManager<RtdsMessage, SimpleParser>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdkStreamError {
    Connection(String),
    Subscription(String),
    SubscriptionActor(String),
    Parser(String),
    Lagged(u64),
    TerminalDisconnected,
    ReceiverClosed,
    Timeout(&'static str),
}

impl fmt::Display for SdkStreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(message) => write!(formatter, "connection error: {message}"),
            Self::Subscription(message) => write!(formatter, "subscription error: {message}"),
            Self::SubscriptionActor(message) => {
                write!(formatter, "subscription actor error: {message}")
            }
            Self::Parser(message) => write!(formatter, "parser error: {message}"),
            Self::Lagged(count) => write!(formatter, "receiver lagged by {count} messages"),
            Self::TerminalDisconnected => formatter.write_str("terminal disconnected"),
            Self::ReceiverClosed => formatter.write_str("receiver closed"),
            Self::Timeout(operation) => write!(formatter, "timeout while {operation}"),
        }
    }
}

impl Error for SdkStreamError {}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DesiredMarketAssets {
    revision: u64,
    assets: BTreeSet<U256>,
}

#[derive(Clone, Default)]
struct DesiredRtdsSubscription {
    revision: u64,
    subscription: Option<Subscription>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RtdsAppliedSubscription {
    pub(crate) generation: u64,
    pub(crate) revision: u64,
}

struct RtdsFreshnessFence {
    initial_generation: u64,
    applied_generation: Option<u64>,
    ineligible_remaining: usize,
}

impl RtdsFreshnessFence {
    const fn new(initial_generation: u64) -> Self {
        Self {
            initial_generation,
            applied_generation: None,
            ineligible_remaining: 0,
        }
    }

    fn observe_applied(
        &mut self,
        applied: RtdsAppliedSubscription,
        queued_messages: usize,
    ) -> Result<bool, SdkStreamError> {
        match self.applied_generation {
            None => {
                if applied.generation < self.initial_generation {
                    return Err(SdkStreamError::SubscriptionActor(format!(
                        "RTDS first applied generation {} precedes initial generation {}",
                        applied.generation, self.initial_generation
                    )));
                }
                self.applied_generation = Some(applied.generation);
                if applied.generation > self.initial_generation {
                    self.ineligible_remaining = queued_messages;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Some(generation) if applied.generation < generation => {
                Err(SdkStreamError::SubscriptionActor(format!(
                    "RTDS applied generation regressed from {generation} to {}",
                    applied.generation
                )))
            }
            Some(generation) if applied.generation == generation => Ok(false),
            Some(_) => {
                self.applied_generation = Some(applied.generation);
                self.ineligible_remaining = queued_messages;
                Ok(true)
            }
        }
    }

    fn classify_received(
        &mut self,
        generation_advanced: bool,
        connection_generation: u64,
        connection_is_active: bool,
    ) -> Option<u64> {
        if generation_advanced {
            return None;
        }
        let was_fenced = self.ineligible_remaining > 0;
        if was_fenced {
            self.ineligible_remaining -= 1;
        }
        (!was_fenced
            && connection_is_active
            && self.applied_generation == Some(connection_generation))
        .then_some(connection_generation)
    }
}

pub(crate) struct RtdsRuntimeMessage {
    pub(crate) message: RtdsMessage,
    pub(crate) freshness_generation: Option<u64>,
}

pub(crate) enum RtdsRuntimeEvent {
    Applied(RtdsAppliedSubscription),
    Message(RtdsRuntimeMessage),
}

pub struct MarketSdkRuntime {
    connection: MarketConnection,
    messages: Option<MarketSdkMessageReceiver>,
    states: watch::Receiver<ConnectionState>,
    faults: watch::Receiver<Option<ConnectionFault>>,
    desired_tx: watch::Sender<DesiredMarketAssets>,
    applied_rx: watch::Receiver<DesiredMarketAssets>,
    actor_error_rx: watch::Receiver<Option<SdkStreamError>>,
    actor: JoinHandle<()>,
    next_revision: u64,
}

impl MarketSdkRuntime {
    pub fn new(endpoint: String, config: Config) -> Result<Self, SdkStreamError> {
        let interest = Arc::new(InterestTracker::new());
        let connection = ConnectionManager::new(endpoint, config, Arc::clone(&interest))
            .map_err(|err| SdkStreamError::Connection(err.to_string()))?;
        let states = connection.state_receiver();
        let faults = connection.fault_receiver();

        let initial = DesiredMarketAssets::default();
        let (desired_tx, desired_rx) = watch::channel(initial.clone());
        let (applied_tx, applied_rx) = watch::channel(initial);
        let (actor_error_tx, actor_error_rx) = watch::channel(None::<SdkStreamError>);
        let messages = MarketSdkMessageReceiver {
            connection: connection.clone(),
            messages: connection.subscribe(),
            states: states.clone(),
            faults: faults.clone(),
            actor_error_rx: actor_error_rx.clone(),
            connection_activity_seen: false,
        };
        let actor = tokio::spawn(run_market_subscription_actor(
            connection.clone(),
            interest,
            states.clone(),
            desired_rx,
            applied_tx,
            actor_error_tx,
        ));

        Ok(Self {
            connection,
            messages: Some(messages),
            states,
            faults,
            desired_tx,
            applied_rx,
            actor_error_rx,
            actor,
            next_revision: 0,
        })
    }

    pub fn replace_desired_assets(
        &mut self,
        assets: BTreeSet<U256>,
    ) -> Result<u64, SdkStreamError> {
        self.next_revision = self
            .next_revision
            .checked_add(1)
            .ok_or_else(|| SdkStreamError::SubscriptionActor("revision overflow".to_owned()))?;
        self.desired_tx.send_replace(DesiredMarketAssets {
            revision: self.next_revision,
            assets,
        });
        Ok(self.next_revision)
    }

    pub async fn wait_desired_applied(
        &mut self,
        revision: u64,
        timeout_duration: Duration,
    ) -> Result<(), SdkStreamError> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            if self.applied_rx.borrow().revision >= revision {
                return Ok(());
            }
            if let Some(error) = connection_fault(&self.faults) {
                return Err(error);
            }
            if let Some(error) = self.actor_error_rx.borrow().clone() {
                return Err(error);
            }
            let remaining = remaining(deadline, "applying desired market assets")?;
            tokio::select! {
                changed = self.applied_rx.changed() => {
                    if changed.is_err() {
                        return Err(market_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                changed = self.actor_error_rx.changed() => {
                    if changed.is_err() {
                        return Err(market_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                changed = self.faults.changed() => {
                    if changed.is_err() && self.connection.state() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                changed = self.states.changed() => {
                    changed.map_err(|_| terminal_error(&self.faults))?;
                    if *self.states.borrow_and_update() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                () = sleep(remaining) => {
                    return Err(SdkStreamError::Timeout("applying desired market assets"));
                }
            }
        }
    }

    pub async fn replace_and_wait(
        &mut self,
        assets: BTreeSet<U256>,
        timeout_duration: Duration,
    ) -> Result<(), SdkStreamError> {
        let revision = self.replace_desired_assets(assets)?;
        self.wait_desired_applied(revision, timeout_duration).await
    }

    pub async fn resubscribe(&mut self, timeout_duration: Duration) -> Result<(), SdkStreamError> {
        let assets = self.desired_tx.borrow().assets.clone();
        if assets.is_empty() {
            return Ok(());
        }
        self.replace_and_wait(BTreeSet::new(), timeout_duration)
            .await?;
        self.replace_and_wait(assets, timeout_duration).await
    }

    pub async fn recv(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<Option<WsMessage>, SdkStreamError> {
        self.messages
            .as_mut()
            .ok_or_else(|| {
                SdkStreamError::SubscriptionActor(
                    "market message receiver was already taken".to_owned(),
                )
            })?
            .recv(timeout_duration)
            .await
    }

    pub(crate) fn take_message_receiver(
        &mut self,
    ) -> Result<MarketSdkMessageReceiver, SdkStreamError> {
        self.messages.take().ok_or_else(|| {
            SdkStreamError::SubscriptionActor(
                "market message receiver was already taken".to_owned(),
            )
        })
    }

    #[must_use]
    pub fn connection_state(&self) -> ConnectionState {
        self.connection.state()
    }

    #[must_use]
    pub fn subscription_count(&self) -> usize {
        usize::from(!self.applied_rx.borrow().assets.is_empty())
    }
}

pub(crate) struct MarketSdkMessageReceiver {
    connection: MarketConnection,
    messages: broadcast::Receiver<WsMessage>,
    states: watch::Receiver<ConnectionState>,
    faults: watch::Receiver<Option<ConnectionFault>>,
    actor_error_rx: watch::Receiver<Option<SdkStreamError>>,
    connection_activity_seen: bool,
}

impl MarketSdkMessageReceiver {
    pub(crate) fn pending_len(&self) -> usize {
        self.messages.len()
    }

    pub(crate) fn terminal_snapshot(&mut self) -> Option<SdkStreamError> {
        connection_fault(&self.faults)
            .or_else(|| self.actor_error_rx.borrow().clone())
            .or_else(|| {
                (self.disconnected_after_activity() || self.messages.is_closed())
                    .then(|| terminal_error(&self.faults))
            })
    }

    fn disconnected_after_activity(&mut self) -> bool {
        if self.states.has_changed().unwrap_or(true) {
            self.connection_activity_seen = true;
        }
        let state = *self.states.borrow_and_update();
        if state != ConnectionState::Disconnected {
            self.connection_activity_seen = true;
            return false;
        }
        self.connection_activity_seen
    }

    pub(crate) fn try_recv(&mut self) -> Result<Option<WsMessage>, SdkStreamError> {
        match self.messages.try_recv() {
            Ok(message) => Ok(Some(message)),
            Err(broadcast::error::TryRecvError::Lagged(count)) => {
                Err(SdkStreamError::Lagged(count))
            }
            Err(broadcast::error::TryRecvError::Closed) => Err(terminal_error(&self.faults)),
            Err(broadcast::error::TryRecvError::Empty) => {
                let terminal = connection_fault(&self.faults)
                    .or_else(|| self.actor_error_rx.borrow().clone())
                    .or_else(|| {
                        self.disconnected_after_activity()
                            .then(|| terminal_error(&self.faults))
                    });
                let Some(terminal) = terminal else {
                    return Ok(None);
                };
                match self.messages.try_recv() {
                    Ok(message) => Ok(Some(message)),
                    Err(broadcast::error::TryRecvError::Lagged(count)) => {
                        Err(SdkStreamError::Lagged(count))
                    }
                    Err(
                        broadcast::error::TryRecvError::Closed
                        | broadcast::error::TryRecvError::Empty,
                    ) => Err(terminal),
                }
            }
        }
    }

    pub(crate) async fn recv(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<Option<WsMessage>, SdkStreamError> {
        let deadline = Instant::now() + timeout_duration;
        let mut pending_terminal = None::<SdkStreamError>;
        loop {
            match self.messages.try_recv() {
                Ok(message) => return Ok(Some(message)),
                Err(broadcast::error::TryRecvError::Lagged(count)) => {
                    return Err(SdkStreamError::Lagged(count));
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(connection_fault(&self.faults)
                        .or_else(|| self.actor_error_rx.borrow().clone())
                        .or_else(|| pending_terminal.take())
                        .unwrap_or_else(|| terminal_error(&self.faults)));
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    if let Some(error) = pending_terminal.take() {
                        return Err(connection_fault(&self.faults)
                            .or_else(|| self.actor_error_rx.borrow().clone())
                            .unwrap_or(error));
                    }
                }
            }
            if let Some(error) = connection_fault(&self.faults) {
                pending_terminal = Some(error);
                continue;
            }
            if let Some(error) = self.actor_error_rx.borrow().clone() {
                pending_terminal = Some(error);
                continue;
            }
            if self.disconnected_after_activity() {
                pending_terminal = Some(terminal_error(&self.faults));
                continue;
            }
            let remaining = remaining(deadline, "waiting for market message")?;
            tokio::select! {
                message = self.messages.recv() => {
                    match message {
                        Ok(message) => return Ok(Some(message)),
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            return Err(SdkStreamError::Lagged(count));
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            pending_terminal = Some(terminal_error(&self.faults));
                        }
                    }
                }
                changed = self.faults.changed() => {
                    if changed.is_err() && *self.states.borrow() == ConnectionState::Disconnected {
                        pending_terminal = Some(terminal_error(&self.faults));
                    }
                }
                changed = self.states.changed() => {
                    let state = *self.states.borrow_and_update();
                    if changed.is_ok() {
                        self.connection_activity_seen = true;
                    }
                    if state == ConnectionState::Disconnected
                        && (changed.is_err() || self.connection_activity_seen)
                    {
                        pending_terminal = Some(terminal_error(&self.faults));
                    }
                }
                changed = self.actor_error_rx.changed() => {
                    if changed.is_err() {
                        pending_terminal = Some(market_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                () = sleep(remaining) => return Ok(None),
            }
        }
    }
}

impl Drop for MarketSdkRuntime {
    fn drop(&mut self) {
        self.actor.abort();
    }
}

async fn run_market_subscription_actor(
    connection: MarketConnection,
    interest: Arc<InterestTracker>,
    mut states: watch::Receiver<ConnectionState>,
    mut desired_rx: watch::Receiver<DesiredMarketAssets>,
    applied_tx: watch::Sender<DesiredMarketAssets>,
    actor_error_tx: watch::Sender<Option<SdkStreamError>>,
) {
    let result = async {
        let mut desired = latest_watch_value(&mut desired_rx);
        let mut applied = DesiredMarketAssets::default();
        let mut connected_since = None;
        let mut connected_assets = BTreeSet::new();
        let mut subscriptions = None::<MarketSubscriptionManager>;
        loop {
            if let ConnectionState::Connected { since } = *states.borrow() {
                if connected_since != Some(since) {
                    desired = latest_watch_value(&mut desired_rx);
                    subscriptions = Some(MarketSubscriptionManager::new(
                        connection.clone(),
                        Arc::clone(&interest),
                    ));
                    connected_assets.clear();
                    connected_since = Some(since);
                }
                if connected_assets != desired.assets || applied.revision < desired.revision {
                    apply_market_subscription_diff(
                        subscriptions
                            .as_ref()
                            .expect("connected state has subscription manager"),
                        &connected_assets,
                        &desired.assets,
                    )?;
                    connected_assets.clone_from(&desired.assets);
                    applied = desired.clone();
                    applied_tx.send_replace(applied.clone());
                    continue;
                }
            }

            tokio::select! {
                biased;
                changed = states.changed() => {
                    if changed.is_err() {
                        return Err(SdkStreamError::TerminalDisconnected);
                    }
                    match *states.borrow_and_update() {
                        ConnectionState::Disconnected => {
                            return Err(SdkStreamError::TerminalDisconnected);
                        }
                        ConnectionState::Connected { .. } => {}
                        _ => {
                            subscriptions = None;
                            connected_assets.clear();
                            connected_since = None;
                        }
                    }
                }
                changed = desired_rx.changed() => {
                    if changed.is_err() {
                        return Ok::<(), SdkStreamError>(());
                    }
                    desired = latest_watch_value(&mut desired_rx);
                }
            }
        }
    }
    .await;

    if let Err(err) = result {
        actor_error_tx.send_replace(Some(err));
    }
}

fn apply_market_subscription_diff(
    subscriptions: &MarketSubscriptionManager,
    applied: &BTreeSet<U256>,
    desired: &BTreeSet<U256>,
) -> Result<(), SdkStreamError> {
    let additions = desired.difference(applied).copied().collect::<Vec<_>>();
    if !additions.is_empty() {
        drop(
            subscriptions
                .subscribe_market_with_options(additions, true)
                .map_err(|err| SdkStreamError::Subscription(err.to_string()))?,
        );
    }

    let removals = applied.difference(desired).copied().collect::<Vec<_>>();
    if !removals.is_empty() {
        subscriptions
            .unsubscribe_market(&removals)
            .map_err(|err| SdkStreamError::Subscription(err.to_string()))?;
    }
    Ok(())
}

pub struct RtdsSdkRuntime {
    connection: RtdsConnection,
    messages: broadcast::Receiver<RtdsMessage>,
    states: watch::Receiver<ConnectionState>,
    faults: watch::Receiver<Option<ConnectionFault>>,
    desired_tx: watch::Sender<DesiredRtdsSubscription>,
    applied_rx: watch::Receiver<Option<RtdsAppliedSubscription>>,
    actor_error_rx: watch::Receiver<Option<SdkStreamError>>,
    actor: JoinHandle<()>,
    next_revision: u64,
    freshness_fence: RtdsFreshnessFence,
}

impl RtdsSdkRuntime {
    pub fn new(endpoint: String, config: Config) -> Result<Self, SdkStreamError> {
        let connection = ConnectionManager::new(endpoint, config, SimpleParser::default())
            .map_err(|err| SdkStreamError::Connection(err.to_string()))?;
        let messages = connection.subscribe();
        let states = connection.state_receiver();
        let faults = connection.fault_receiver();
        let (desired_tx, desired_rx) = watch::channel(DesiredRtdsSubscription::default());
        let (applied_tx, applied_rx) = watch::channel(None);
        let (actor_error_tx, actor_error_rx) = watch::channel(None::<SdkStreamError>);
        let actor = tokio::spawn(run_rtds_subscription_actor(
            connection.clone(),
            connection.state_receiver(),
            desired_rx,
            applied_tx,
            actor_error_tx,
        ));
        let initial_generation = connection.connection_generation();
        Ok(Self {
            connection,
            messages,
            states,
            faults,
            desired_tx,
            applied_rx,
            actor_error_rx,
            actor,
            next_revision: 0,
            freshness_fence: RtdsFreshnessFence::new(initial_generation),
        })
    }

    pub async fn wait_connected(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<(), SdkStreamError> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            if self.connection.state().is_connected() {
                return Ok(());
            }
            if let Some(error) = connection_fault(&self.faults) {
                return Err(error);
            }
            if let Some(error) = self.actor_error_rx.borrow().clone() {
                return Err(error);
            }
            let remaining = remaining(deadline, "connecting RTDS")?;
            tokio::select! {
                changed = self.faults.changed() => {
                    if changed.is_err() && self.connection.state() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                changed = self.states.changed() => {
                    changed.map_err(|_| terminal_error(&self.faults))?;
                    if *self.states.borrow_and_update() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                changed = self.actor_error_rx.changed() => {
                    if changed.is_err() {
                        return Err(rtds_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                () = sleep(remaining) => return Err(SdkStreamError::Timeout("connecting RTDS")),
            }
        }
    }

    pub fn replace_subscription(
        &mut self,
        subscription: Subscription,
    ) -> Result<u64, SdkStreamError> {
        self.next_revision = self
            .next_revision
            .checked_add(1)
            .ok_or_else(|| SdkStreamError::SubscriptionActor("revision overflow".to_owned()))?;
        self.desired_tx.send_replace(DesiredRtdsSubscription {
            revision: self.next_revision,
            subscription: Some(subscription),
        });
        Ok(self.next_revision)
    }

    pub async fn wait_subscription_applied(
        &mut self,
        revision: u64,
        timeout_duration: Duration,
    ) -> Result<(), SdkStreamError> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            self.sync_applied_fence()?;
            if self.subscription_is_applied(revision) {
                return Ok(());
            }
            if let Some(error) = connection_fault(&self.faults) {
                return Err(error);
            }
            if let Some(error) = self.actor_error_rx.borrow().clone() {
                return Err(error);
            }
            let remaining = remaining(deadline, "applying RTDS subscription")?;
            tokio::select! {
                changed = self.applied_rx.changed() => {
                    if changed.is_err() {
                        return Err(rtds_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                changed = self.actor_error_rx.changed() => {
                    if changed.is_err() {
                        return Err(rtds_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                changed = self.faults.changed() => {
                    if changed.is_err() && self.connection.state() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                changed = self.states.changed() => {
                    changed.map_err(|_| terminal_error(&self.faults))?;
                    if *self.states.borrow_and_update() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                () = sleep(remaining) => {
                    return Err(SdkStreamError::Timeout("applying RTDS subscription"));
                }
            }
        }
    }

    pub async fn subscribe(
        &mut self,
        subscription: Subscription,
        timeout_duration: Duration,
    ) -> Result<(), SdkStreamError> {
        let revision = self.replace_subscription(subscription)?;
        self.wait_subscription_applied(revision, timeout_duration)
            .await
    }

    #[must_use]
    pub fn connection_generation(&self) -> u64 {
        self.connection.connection_generation()
    }

    #[must_use]
    pub fn subscription_revision(&self) -> u64 {
        self.desired_tx.borrow().revision
    }

    pub async fn recv(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<Option<RtdsMessage>, SdkStreamError> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            match self.recv_event(remaining).await? {
                Some(RtdsRuntimeEvent::Message(message)) => return Ok(Some(message.message)),
                Some(RtdsRuntimeEvent::Applied(_)) => {}
                None => return Ok(None),
            }
        }
    }

    pub(crate) async fn recv_event(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<Option<RtdsRuntimeEvent>, SdkStreamError> {
        self.recv_event_until(Some(Instant::now() + timeout_duration))
            .await
    }

    pub(crate) async fn recv_event_until(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<Option<RtdsRuntimeEvent>, SdkStreamError> {
        loop {
            if let Some(error) = connection_fault(&self.faults) {
                return Err(error);
            }
            if let Some(error) = self.actor_error_rx.borrow().clone() {
                return Err(error);
            }
            if let Some(applied) = self.sync_applied_fence()? {
                return Ok(Some(RtdsRuntimeEvent::Applied(applied)));
            }
            let remaining =
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            if remaining.is_some_and(|remaining| remaining.is_zero()) {
                return Ok(None);
            }
            tokio::select! {
                biased;
                changed = self.applied_rx.changed() => {
                    if changed.is_err() {
                        return Err(rtds_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                    if let Some(applied) = self.sync_applied_fence()? {
                        return Ok(Some(RtdsRuntimeEvent::Applied(applied)));
                    }
                }
                message = self.messages.recv() => {
                    return match message {
                        Ok(message) => {
                            let newly_applied = self.sync_applied_fence()?;
                            let generation_advanced = newly_applied.is_some();
                            let connection_generation = self.connection.connection_generation();
                            let connection_is_active = self.connection.state().is_connected();
                            let freshness_generation = self
                                .freshness_fence
                                .classify_received(
                                    generation_advanced,
                                    connection_generation,
                                    connection_is_active,
                                );
                            Ok(Some(RtdsRuntimeEvent::Message(RtdsRuntimeMessage {
                                message,
                                freshness_generation,
                            })))
                        }
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            Err(SdkStreamError::Lagged(count))
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            Err(terminal_error(&self.faults))
                        }
                    };
                }
                changed = self.faults.changed() => {
                    if changed.is_err() && self.connection.state() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                changed = self.states.changed() => {
                    changed.map_err(|_| terminal_error(&self.faults))?;
                    if *self.states.borrow_and_update() == ConnectionState::Disconnected {
                        return Err(terminal_error(&self.faults));
                    }
                }
                changed = self.actor_error_rx.changed() => {
                    if changed.is_err() {
                        return Err(rtds_actor_exit_error(
                            &self.connection,
                            &self.faults,
                            &self.actor_error_rx,
                        ));
                    }
                }
                () = async {
                    match remaining {
                        Some(remaining) => sleep(remaining).await,
                        None => std::future::pending::<()>().await,
                    }
                } => return Ok(None),
            }
        }
    }

    fn sync_applied_fence(&mut self) -> Result<Option<RtdsAppliedSubscription>, SdkStreamError> {
        let applied = *self.applied_rx.borrow_and_update();
        let Some(applied) = applied else {
            return Ok(None);
        };
        if self
            .freshness_fence
            .observe_applied(applied, self.messages.len())?
        {
            Ok(Some(applied))
        } else {
            Ok(None)
        }
    }

    fn subscription_is_applied(&self, revision: u64) -> bool {
        self.applied_rx.borrow().is_some_and(|applied| {
            applied.revision >= revision
                && applied.generation == self.connection.connection_generation()
                && self.connection.state().is_connected()
        })
    }

    #[must_use]
    pub fn subscription_count(&self) -> usize {
        usize::from(self.desired_tx.borrow().subscription.is_some())
    }
}

impl Drop for RtdsSdkRuntime {
    fn drop(&mut self) {
        self.actor.abort();
    }
}

async fn run_rtds_subscription_actor(
    connection: RtdsConnection,
    mut states: watch::Receiver<ConnectionState>,
    mut desired_rx: watch::Receiver<DesiredRtdsSubscription>,
    applied_tx: watch::Sender<Option<RtdsAppliedSubscription>>,
    actor_error_tx: watch::Sender<Option<SdkStreamError>>,
) {
    let result = async {
        let mut desired = latest_watch_value(&mut desired_rx);
        let mut connected_generation = None;
        let mut generation_applied = false;
        let mut subscriptions = None::<RtdsSubscriptionManager>;

        loop {
            if let ConnectionState::Connected { .. } = *states.borrow() {
                let generation = connection.connection_generation();
                if connected_generation != Some(generation) {
                    desired = latest_watch_value(&mut desired_rx);
                    subscriptions = connection
                        .bind_generation(generation)
                        .map(RtdsSubscriptionManager::new);
                    generation_applied = false;
                    connected_generation = Some(generation);
                }

                if !generation_applied
                    && let Some(subscription) = desired.subscription.clone()
                    && let Some(generation_subscriptions) = subscriptions.as_ref()
                {
                    apply_rtds_subscription(generation_subscriptions, subscription)?;
                    if connection.connection_generation() != generation
                        || !connection.state().is_connected()
                    {
                        subscriptions = None;
                        connected_generation = None;
                        continue;
                    }
                    generation_applied = true;
                    applied_tx.send_replace(Some(RtdsAppliedSubscription {
                        generation,
                        revision: desired.revision,
                    }));
                    continue;
                }
            }

            tokio::select! {
                biased;
                changed = states.changed() => {
                    if changed.is_err() {
                        return Err(SdkStreamError::TerminalDisconnected);
                    }
                    match *states.borrow_and_update() {
                        ConnectionState::Disconnected => {
                            return Err(SdkStreamError::TerminalDisconnected);
                        }
                        ConnectionState::Connected { .. } => {}
                        _ => {
                            subscriptions = None;
                            generation_applied = false;
                            connected_generation = None;
                        }
                    }
                }
                changed = desired_rx.changed() => {
                    if changed.is_err() {
                        return Ok::<(), SdkStreamError>(());
                    }
                    desired = latest_watch_value(&mut desired_rx);
                }
            }
        }
    }
    .await;

    if let Err(err) = result {
        actor_error_tx.send_replace(Some(err));
    }
}

fn apply_rtds_subscription(
    subscriptions: &RtdsSubscriptionManager,
    desired: Subscription,
) -> Result<(), SdkStreamError> {
    drop(
        subscriptions
            .subscribe(desired)
            .map_err(|err| SdkStreamError::Subscription(err.to_string()))?,
    );
    Ok(())
}

fn latest_watch_value<T: Clone>(receiver: &mut watch::Receiver<T>) -> T {
    receiver.borrow_and_update().clone()
}

fn connection_fault(faults: &watch::Receiver<Option<ConnectionFault>>) -> Option<SdkStreamError> {
    match faults.borrow().as_ref() {
        Some(ConnectionFault::Parser(message)) => Some(SdkStreamError::Parser(message.clone())),
        Some(fault) => Some(SdkStreamError::Connection(format!(
            "terminal connection fault: {fault:?}"
        ))),
        None => None,
    }
}

fn market_actor_exit_error(
    connection: &MarketConnection,
    faults: &watch::Receiver<Option<ConnectionFault>>,
    actor_errors: &watch::Receiver<Option<SdkStreamError>>,
) -> SdkStreamError {
    connection_fault(faults)
        .or_else(|| actor_errors.borrow().clone())
        .unwrap_or_else(|| {
            if connection.state() == ConnectionState::Disconnected {
                SdkStreamError::TerminalDisconnected
            } else {
                SdkStreamError::SubscriptionActor("actor stopped".to_owned())
            }
        })
}

fn rtds_actor_exit_error(
    connection: &RtdsConnection,
    faults: &watch::Receiver<Option<ConnectionFault>>,
    actor_errors: &watch::Receiver<Option<SdkStreamError>>,
) -> SdkStreamError {
    connection_fault(faults)
        .or_else(|| actor_errors.borrow().clone())
        .unwrap_or_else(|| {
            if connection.state() == ConnectionState::Disconnected {
                SdkStreamError::TerminalDisconnected
            } else {
                SdkStreamError::SubscriptionActor("RTDS actor stopped".to_owned())
            }
        })
}

fn terminal_error(faults: &watch::Receiver<Option<ConnectionFault>>) -> SdkStreamError {
    connection_fault(faults).unwrap_or(SdkStreamError::TerminalDisconnected)
}

fn remaining(deadline: Instant, operation: &'static str) -> Result<Duration, SdkStreamError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(SdkStreamError::Timeout(operation))
    } else {
        Ok(remaining)
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt as _, StreamExt as _};
    use serde_json::{Value, json};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{WebSocketStream, accept_async};

    use super::*;

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
        .expect("subscription request")
    }

    async fn wait_connected(states: &mut watch::Receiver<ConnectionState>) {
        timeout(Duration::from_secs(2), async {
            loop {
                if states.borrow().is_connected() {
                    return;
                }
                states.changed().await.unwrap();
            }
        })
        .await
        .expect("SDK connection")
    }

    #[test]
    fn rtds_fence_keeps_the_last_queued_message_ineligible_before_decrement() {
        let mut fence = RtdsFreshnessFence::new(0);
        assert!(
            !fence
                .observe_applied(
                    RtdsAppliedSubscription {
                        generation: 0,
                        revision: 1,
                    },
                    0,
                )
                .unwrap()
        );
        assert!(
            fence
                .observe_applied(
                    RtdsAppliedSubscription {
                        generation: 1,
                        revision: 1,
                    },
                    1,
                )
                .unwrap()
        );

        assert_eq!(fence.classify_received(false, 1, true), None);
        assert_eq!(fence.ineligible_remaining, 0);
        assert_eq!(fence.classify_received(false, 1, true), Some(1));
    }

    #[test]
    fn rtds_fence_marks_the_current_message_ineligible_when_ack_is_seen_after_recv() {
        let mut fence = RtdsFreshnessFence::new(0);
        fence
            .observe_applied(
                RtdsAppliedSubscription {
                    generation: 0,
                    revision: 1,
                },
                0,
            )
            .unwrap();

        let generation_advanced = fence
            .observe_applied(
                RtdsAppliedSubscription {
                    generation: 1,
                    revision: 1,
                },
                0,
            )
            .unwrap();
        assert!(generation_advanced);
        assert_eq!(fence.ineligible_remaining, 0);
        assert_eq!(fence.classify_received(generation_advanced, 1, true), None);
        assert_eq!(fence.classify_received(false, 1, true), Some(1));
    }

    #[test]
    fn rtds_unobserved_new_generation_cannot_make_a_message_eligible() {
        let mut fence = RtdsFreshnessFence::new(0);
        fence
            .observe_applied(
                RtdsAppliedSubscription {
                    generation: 0,
                    revision: 1,
                },
                0,
            )
            .unwrap();

        assert_eq!(fence.classify_received(false, 1, true), None);
        assert!(
            fence
                .observe_applied(
                    RtdsAppliedSubscription {
                        generation: 1,
                        revision: 1,
                    },
                    0,
                )
                .unwrap()
        );
    }

    #[test]
    fn rtds_same_generation_revision_does_not_rebuild_the_fence() {
        let mut fence = RtdsFreshnessFence::new(0);
        fence
            .observe_applied(
                RtdsAppliedSubscription {
                    generation: 3,
                    revision: 1,
                },
                0,
            )
            .unwrap();
        assert!(
            !fence
                .observe_applied(
                    RtdsAppliedSubscription {
                        generation: 3,
                        revision: 2,
                    },
                    10,
                )
                .unwrap()
        );
        assert_eq!(fence.ineligible_remaining, 0);
    }

    #[test]
    fn rtds_first_observed_applied_generation_builds_a_fence_when_initial_ack_was_skipped() {
        let mut fence = RtdsFreshnessFence::new(0);

        assert!(
            fence
                .observe_applied(
                    RtdsAppliedSubscription {
                        generation: 1,
                        revision: 1,
                    },
                    1,
                )
                .unwrap()
        );
        assert_eq!(fence.classify_received(false, 1, true), None);
        assert_eq!(fence.classify_received(false, 1, true), Some(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rtds_receive_deadline_returns_none_instead_of_a_fatal_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            _ = next_json_request(&mut socket).await;
            finish_rx.await.unwrap();
        });

        let mut config = Config::default();
        config.heartbeat_interval = Duration::from_secs(30);
        config.heartbeat_timeout = Duration::from_secs(30);
        let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), config).unwrap();
        runtime
            .subscribe(
                Subscription::chainlink_prices(Some("btc/usd".to_owned())),
                Duration::from_secs(2),
            )
            .await
            .unwrap();

        assert!(runtime.recv_event(Duration::ZERO).await.unwrap().is_none());
        assert!(runtime.recv(Duration::ZERO).await.unwrap().is_none());
        finish_tx.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rtds_old_queued_message_is_preserved_but_cannot_recover_new_generation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (old_sent_tx, old_sent_rx) = tokio::sync::oneshot::channel();
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let (replay_seen_tx, replay_seen_rx) = tokio::sync::oneshot::channel();
        let (send_new_tx, send_new_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            _ = next_json_request(&mut socket).await;
            socket
                .send(Message::Text(
                    json!({
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "timestamp": 1,
                        "payload": {"symbol": "btc/usd", "timestamp": 1, "value": "1"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            old_sent_tx.send(()).unwrap();
            close_rx.await.unwrap();
            socket.send(Message::Close(None)).await.unwrap();
            drop(socket);

            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            _ = next_json_request(&mut socket).await;
            replay_seen_tx.send(()).unwrap();
            send_new_rx.await.unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "timestamp": 2,
                        "payload": {"symbol": "btc/usd", "timestamp": 2, "value": "2"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            finish_rx.await.unwrap();
        });

        let mut config = Config::default();
        config.reconnect.initial_backoff = Duration::from_millis(5);
        config.reconnect.max_backoff = Duration::from_millis(10);
        config.reconnect.backoff_multiplier = 1.0;
        config.heartbeat_interval = Duration::from_secs(30);
        config.heartbeat_timeout = Duration::from_secs(30);
        let mut runtime = RtdsSdkRuntime::new(format!("ws://{address}"), config).unwrap();
        runtime
            .subscribe(
                Subscription::chainlink_prices(Some("btc/usd".to_owned())),
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        let revision = runtime.subscription_revision();
        old_sent_rx.await.unwrap();
        timeout(Duration::from_secs(2), async {
            while runtime.messages.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let old_generation = runtime.connection_generation();
        close_tx.send(()).unwrap();
        replay_seen_rx.await.unwrap();
        runtime
            .wait_subscription_applied(revision, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(runtime.connection_generation(), old_generation + 1);
        let Some(RtdsRuntimeEvent::Message(old_message)) =
            runtime.recv_event(Duration::from_secs(2)).await.unwrap()
        else {
            panic!("old queued RTDS message");
        };
        assert_eq!(old_message.message.timestamp, 1);
        assert_eq!(old_message.freshness_generation, None);

        send_new_tx.send(()).unwrap();
        let Some(RtdsRuntimeEvent::Message(new_message)) =
            runtime.recv_event(Duration::from_secs(2)).await.unwrap()
        else {
            panic!("new generation RTDS message");
        };
        assert_eq!(new_message.message.timestamp, 2);
        assert_eq!(new_message.freshness_generation, Some(old_generation + 1));
        finish_tx.send(()).unwrap();
        server.await.unwrap();
    }

    fn synthetic_disconnected_market_receiver() -> (
        MarketSdkRuntime,
        MarketSdkMessageReceiver,
        watch::Sender<ConnectionState>,
        broadcast::Sender<WsMessage>,
    ) {
        let mut runtime = MarketSdkRuntime::new("ws://127.0.0.1:9".to_owned(), Config::default())
            .expect("dormant market runtime");
        let mut receiver = runtime.take_message_receiver().unwrap();
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);
        let (message_tx, message_rx) = broadcast::channel(1);
        receiver.states = state_rx;
        receiver.messages = message_rx;
        (runtime, receiver, state_tx, message_tx)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_receiver_initial_disconnected_is_not_terminal() {
        let (_runtime, mut receiver, _state_tx, _message_tx) =
            synthetic_disconnected_market_receiver();

        assert_eq!(receiver.terminal_snapshot(), None);
        assert!(matches!(receiver.try_recv(), Ok(None)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_receiver_connection_activity_then_disconnected_is_terminal() {
        let (_runtime, mut receiver, state_tx, _message_tx) =
            synthetic_disconnected_market_receiver();
        state_tx.send_replace(ConnectionState::Connecting);
        state_tx.send_replace(ConnectionState::Disconnected);

        assert!(matches!(
            receiver.try_recv(),
            Err(SdkStreamError::TerminalDisconnected)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_message_receiver_can_be_taken_only_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), Config::default())
            .expect("market runtime");

        let receiver = runtime
            .take_message_receiver()
            .expect("first receiver take");
        assert!(matches!(
            runtime.take_message_receiver(),
            Err(SdkStreamError::SubscriptionActor(message))
                if message.contains("already taken")
        ));
        assert!(matches!(
            runtime.recv(Duration::from_millis(1)).await,
            Err(SdkStreamError::SubscriptionActor(message))
                if message.contains("already taken")
        ));
        drop(receiver);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_receiver_drains_valid_messages_before_a_later_parser_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (send_frames_tx, send_frames_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            _ = next_json_request(&mut socket).await;
            send_frames_rx.await.unwrap();
            for timestamp in [1_i64, 2] {
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
            socket
                .send(Message::Text(
                    r#"{"event_type":"book","asset_id":"1""#.into(),
                ))
                .await
                .unwrap();
            sleep(Duration::from_millis(200)).await;
        });

        let mut runtime = MarketSdkRuntime::new(format!("ws://{address}"), Config::default())
            .expect("market runtime");
        runtime
            .replace_and_wait(BTreeSet::from([U256::from(1)]), Duration::from_secs(2))
            .await
            .unwrap();
        let mut receiver = runtime.take_message_receiver().unwrap();
        let waiting = tokio::spawn(async move {
            let first = receiver.recv(Duration::from_secs(1)).await;
            (first, receiver)
        });
        tokio::task::yield_now().await;
        send_frames_tx.send(()).unwrap();

        let (first, mut receiver) = waiting.await.unwrap();
        let mut messages = vec![first.unwrap().unwrap()];
        messages.push(
            receiver
                .recv(Duration::from_secs(1))
                .await
                .unwrap()
                .unwrap(),
        );
        for (message, expected_timestamp) in messages.into_iter().zip([1_i64, 2]) {
            let WsMessage::Book(book) = message else {
                panic!("valid book must precede parser fault");
            };
            assert_eq!(book.timestamp, expected_timestamp);
        }
        assert!(matches!(
            receiver.recv(Duration::from_secs(1)).await,
            Err(SdkStreamError::Parser(_))
        ));
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_actor_refreshes_latest_desired_when_new_epoch_state_wins() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (initial_seen_tx, initial_seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let initial = next_json_request(&mut socket).await;
            initial_seen_tx.send(()).unwrap();
            let reconnect = next_json_request(&mut socket).await;
            assert!(
                timeout(Duration::from_millis(200), next_json_request(&mut socket))
                    .await
                    .is_err(),
                "new market epoch must receive only the latest desired assets"
            );
            vec![initial, reconnect]
        });

        let interest = Arc::new(InterestTracker::new());
        let connection = MarketConnection::new(
            format!("ws://{address}"),
            Config::default(),
            Arc::clone(&interest),
        )
        .unwrap();
        let mut actual_states = connection.state_receiver();
        wait_connected(&mut actual_states).await;

        let first_since = Instant::now();
        let (state_tx, state_rx) =
            watch::channel(ConnectionState::Connected { since: first_since });
        let (desired_tx, desired_rx) = watch::channel(DesiredMarketAssets {
            revision: 1,
            assets: BTreeSet::from([U256::from(1)]),
        });
        let (applied_tx, _applied_rx) = watch::channel(DesiredMarketAssets::default());
        let (actor_error_tx, _actor_error_rx) = watch::channel(None);
        let actor = tokio::spawn(run_market_subscription_actor(
            connection,
            interest,
            state_rx,
            desired_rx,
            applied_tx,
            actor_error_tx,
        ));
        initial_seen_rx.await.unwrap();

        desired_tx.send_replace(DesiredMarketAssets {
            revision: 2,
            assets: BTreeSet::from([U256::from(2)]),
        });
        state_tx.send_replace(ConnectionState::Connected {
            since: first_since + Duration::from_secs(1),
        });

        let requests = server.await.unwrap();
        assert_eq!(requests[0]["operation"], "subscribe");
        assert_eq!(requests[0]["assets_ids"], json!(["1"]));
        assert_eq!(requests[1]["operation"], "subscribe");
        assert_eq!(requests[1]["assets_ids"], json!(["2"]));
        state_tx.send_replace(ConnectionState::Disconnected);
        actor.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rtds_actor_refreshes_latest_desired_when_new_epoch_state_wins() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (initial_seen_tx, initial_seen_rx) = tokio::sync::oneshot::channel();
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let initial = next_json_request(&mut socket).await;
            initial_seen_tx.send(()).unwrap();
            close_rx.await.unwrap();
            socket.send(Message::Close(None)).await.unwrap();
            drop(socket);

            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let reconnect = next_json_request(&mut socket).await;
            assert!(
                timeout(Duration::from_millis(200), next_json_request(&mut socket))
                    .await
                    .is_err(),
                "new RTDS epoch must receive only the latest desired subscription"
            );
            vec![initial, reconnect]
        });

        let connection = RtdsConnection::new(
            format!("ws://{address}"),
            Config::default(),
            SimpleParser::default(),
        )
        .unwrap();
        let mut actual_states = connection.state_receiver();
        wait_connected(&mut actual_states).await;

        let (desired_tx, desired_rx) = watch::channel(DesiredRtdsSubscription {
            revision: 1,
            subscription: Some(Subscription::chainlink_prices(Some("btc/usd-1".to_owned()))),
        });
        let (applied_tx, _applied_rx) = watch::channel(None);
        let (actor_error_tx, _actor_error_rx) = watch::channel(None);
        let actor = tokio::spawn(run_rtds_subscription_actor(
            connection.clone(),
            connection.state_receiver(),
            desired_rx,
            applied_tx,
            actor_error_tx,
        ));
        initial_seen_rx.await.unwrap();

        desired_tx.send_replace(DesiredRtdsSubscription {
            revision: 2,
            subscription: Some(Subscription::chainlink_prices(Some("btc/usd-2".to_owned()))),
        });
        close_tx.send(()).unwrap();

        let requests = server.await.unwrap();
        assert_eq!(requests[0]["action"], "subscribe");
        assert_eq!(
            requests[0]["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd-1"}"#
        );
        assert_eq!(requests[1]["action"], "subscribe");
        assert_eq!(
            requests[1]["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd-2"}"#
        );
        actor.abort();
    }
}
