#![expect(
    clippy::module_name_repetitions,
    reason = "Subscription types deliberately include the module name for clarity"
)]

use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use async_stream::try_stream;
use dashmap::{DashMap, Entry};
use futures::Stream;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use tokio::sync::broadcast::error::RecvError;

use super::error::RtdsError;
use super::types::request::{Subscription, SubscriptionRequest};
use super::types::response::{RtdsMessage, parse_messages};
use crate::Result;
use crate::auth::Credentials;
use crate::ws::ConnectionManager;
use crate::ws::connection::{ConnectionState, recoverable_rtds_network_envelope};

#[non_exhaustive]
#[derive(Clone, Default)]
pub struct SimpleParser;

const RTDS_PARSE_DIAGNOSTIC_MAX_BYTES: usize = 2_048;
const RTDS_PARSE_DIAGNOSTIC_TRUNCATED: &str = "...[truncated]";
const RTDS_PARSE_DIAGNOSTIC_SCALAR_FIELDS: [&str; 8] = [
    "message",
    "status",
    "code",
    "topic",
    "type",
    "connectionId",
    "requestId",
    "error",
];
const RTDS_PARSE_DIAGNOSTIC_TOP_LEVEL_SCALAR_FIELDS: [&str; 9] = [
    "message",
    "status",
    "statusCode",
    "code",
    "topic",
    "type",
    "connectionId",
    "requestId",
    "error",
];
const RTDS_PARSE_DIAGNOSTIC_BODY_SCALAR_FIELDS: [&str; 6] = [
    "message",
    "status",
    "code",
    "error",
    "connectionId",
    "requestId",
];
const RTDS_RECOVERABLE_NETWORK_ERROR: &str = "Network error communicating with endpoint";
const RTDS_MESSAGE_FIELDS: [&str; 4] = ["topic", "type", "timestamp", "payload"];

#[derive(Debug)]
struct RtdsParseDiagnosticError {
    source: crate::error::Error,
    diagnostic: String,
}

impl fmt::Display for RtdsParseDiagnosticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RTDS typed message parse failed; rtds_parse_diagnostic={}",
            self.diagnostic
        )
    }
}

impl std::error::Error for RtdsParseDiagnosticError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

struct BoundedDiagnostic {
    value: String,
    truncated: bool,
}

impl BoundedDiagnostic {
    fn new() -> Self {
        Self {
            value: String::with_capacity(RTDS_PARSE_DIAGNOSTIC_MAX_BYTES),
            truncated: false,
        }
    }

    fn push(&mut self, value: &str) {
        if self.truncated {
            return;
        }

        let remaining = RTDS_PARSE_DIAGNOSTIC_MAX_BYTES - self.value.len();
        if value.len() <= remaining {
            self.value.push_str(value);
            return;
        }

        if remaining < RTDS_PARSE_DIAGNOSTIC_TRUNCATED.len() {
            let mut prefix_end =
                RTDS_PARSE_DIAGNOSTIC_MAX_BYTES - RTDS_PARSE_DIAGNOSTIC_TRUNCATED.len();
            while !self.value.is_char_boundary(prefix_end) {
                prefix_end -= 1;
            }
            self.value.truncate(prefix_end);
        } else {
            let prefix_budget = remaining - RTDS_PARSE_DIAGNOSTIC_TRUNCATED.len();
            let mut prefix_end = prefix_budget.min(value.len());
            while !value.is_char_boundary(prefix_end) {
                prefix_end -= 1;
            }
            let Some(prefix) = value.get(..prefix_end) else {
                unreachable!("prefix_end is adjusted to a UTF-8 boundary");
            };
            self.value.push_str(prefix);
        }
        self.value.push_str(RTDS_PARSE_DIAGNOSTIC_TRUNCATED);
        self.truncated = true;
    }

    fn push_char(&mut self, value: char) {
        let mut bytes = [0; 4];
        self.push(value.encode_utf8(&mut bytes));
    }

    fn is_truncated(&self) -> bool {
        self.truncated
    }

    fn finish(self) -> String {
        self.value
    }
}

fn append_json_string(diagnostic: &mut BoundedDiagnostic, value: &str) {
    diagnostic.push("\"");
    for character in value.chars() {
        if diagnostic.is_truncated() {
            break;
        }
        match character {
            '"' => diagnostic.push("\\\""),
            '\\' => diagnostic.push("\\\\"),
            '\u{08}' => diagnostic.push("\\b"),
            '\u{0c}' => diagnostic.push("\\f"),
            '\n' => diagnostic.push("\\n"),
            '\r' => diagnostic.push("\\r"),
            '\t' => diagnostic.push("\\t"),
            character if character.is_control() => {
                diagnostic.push(&format!("\\u{:04x}", character as u32));
            }
            character => diagnostic.push_char(character),
        }
    }
    diagnostic.push("\"");
}

fn append_scalar(diagnostic: &mut BoundedDiagnostic, value: &Value) -> bool {
    match value {
        Value::Null => diagnostic.push("null"),
        Value::Bool(value) => diagnostic.push(if *value { "true" } else { "false" }),
        Value::Number(value) => diagnostic.push(&value.to_string()),
        Value::String(value) => append_json_string(diagnostic, value),
        Value::Array(_) | Value::Object(_) => return false,
    }
    true
}

fn append_object_diagnostic(
    diagnostic: &mut BoundedDiagnostic,
    object: &Map<String, Value>,
    scalar_fields: &[&str],
    selected_label: &str,
    fields_label: &str,
) {
    diagnostic.push(" ");
    diagnostic.push(selected_label);
    diagnostic.push("={");

    let mut first = true;
    for field in scalar_fields {
        if diagnostic.is_truncated() {
            break;
        }
        let Some(value) = object.get(*field) else {
            continue;
        };
        if matches!(value, Value::Array(_) | Value::Object(_)) {
            continue;
        }
        if !first {
            diagnostic.push(",");
        }
        diagnostic.push(field);
        diagnostic.push("=");
        let appended = append_scalar(diagnostic, value);
        debug_assert!(appended, "non-scalar values are filtered above");
        first = false;
    }
    diagnostic.push("} ");
    diagnostic.push(fields_label);
    diagnostic.push("=[");

    let mut fields: Vec<_> = object.keys().collect();
    fields.sort_unstable();
    for (index, field) in fields.into_iter().enumerate() {
        if diagnostic.is_truncated() {
            break;
        }
        if index > 0 {
            diagnostic.push(",");
        }
        append_json_string(diagnostic, field);
    }
    diagnostic.push("]");
}

fn json_shape(value: &Value) -> &'static str {
    match value {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
    }
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut fields: Vec<_> = object.iter().collect();
            fields.sort_unstable_by_key(|(left, _)| *left);

            let mut canonical = Map::new();
            for (field, value) in fields {
                canonical.insert(field.clone(), canonicalize_json(value));
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => value.clone(),
    }
}

fn append_body_json_diagnostic(diagnostic: &mut BoundedDiagnostic, value: &Value) {
    diagnostic.push(" body_json_top_level=");
    diagnostic.push(json_shape(value));
    if let Value::Object(object) = value {
        append_object_diagnostic(
            diagnostic,
            object,
            &RTDS_PARSE_DIAGNOSTIC_BODY_SCALAR_FIELDS,
            "body_selected",
            "body_fields",
        );
    }
}

fn append_body_diagnostic(diagnostic: &mut BoundedDiagnostic, body: &Value) {
    diagnostic.push(" body_type=");
    diagnostic.push(json_shape(body));

    if let Value::String(value) = body {
        let bytes = value.as_bytes();
        diagnostic.push(" body_decoded_bytes=");
        diagnostic.push(&bytes.len().to_string());
        diagnostic.push(" body_decoded_sha256=");
        diagnostic.push(&format!("{:x}", Sha256::digest(bytes)));

        match serde_json::from_str::<Value>(value) {
            Ok(value) => append_body_json_diagnostic(diagnostic, &value),
            Err(_) => diagnostic.push(" body_json_top_level=invalid_json"),
        }
        return;
    }

    let canonical = serde_json::to_vec(&canonicalize_json(body))
        .expect("serializing a serde_json::Value cannot fail");
    diagnostic.push(" body_canonical_json_bytes=");
    diagnostic.push(&canonical.len().to_string());
    diagnostic.push(" body_canonical_json_sha256=");
    diagnostic.push(&format!("{:x}", Sha256::digest(&canonical)));
    if matches!(body, Value::Object(_)) {
        append_body_json_diagnostic(diagnostic, body);
    }
}

struct ArrayMismatchSeed<'diagnostic> {
    mismatch_index: &'diagnostic mut Option<usize>,
}

impl<'de> DeserializeSeed<'de> for ArrayMismatchSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArrayMismatchVisitor {
            mismatch_index: self.mismatch_index,
        })
    }
}

struct ArrayMismatchVisitor<'diagnostic> {
    mismatch_index: &'diagnostic mut Option<usize>,
}

impl<'de> Visitor<'de> for ArrayMismatchVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of RTDS messages")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut index = 0;
        loop {
            match seq.next_element::<RtdsMessage>() {
                Ok(Some(_)) => index += 1,
                Ok(None) => return Ok(()),
                Err(error) => {
                    *self.mismatch_index = Some(index);
                    return Err(error);
                }
            }
        }
    }
}

fn first_array_contract_mismatch(bytes: &[u8]) -> Option<usize> {
    let mut mismatch_index = None;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let _: std::result::Result<(), serde_json::Error> = ArrayMismatchSeed {
        mismatch_index: &mut mismatch_index,
    }
    .deserialize(&mut deserializer);
    mismatch_index
}

fn build_rtds_parse_diagnostic(bytes: &[u8]) -> String {
    let mut diagnostic = BoundedDiagnostic::new();
    diagnostic.push("frame_bytes=");
    diagnostic.push(&bytes.len().to_string());
    diagnostic.push(" sha256=");
    diagnostic.push(&format!("{:x}", Sha256::digest(bytes)));

    match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::Object(object)) => {
            diagnostic.push(" top_level=object");
            append_object_diagnostic(
                &mut diagnostic,
                &object,
                &RTDS_PARSE_DIAGNOSTIC_TOP_LEVEL_SCALAR_FIELDS,
                "selected",
                "fields",
            );
            if let Some(body) = object.get("body") {
                append_body_diagnostic(&mut diagnostic, body);
            }
        }
        Ok(Value::Array(values)) => {
            diagnostic.push(" top_level=array first_contract_mismatch_index=");
            if let Some(index) = first_array_contract_mismatch(bytes) {
                diagnostic.push(&index.to_string());
                if let Some(Value::Object(object)) = values.get(index) {
                    append_object_diagnostic(
                        &mut diagnostic,
                        object,
                        &RTDS_PARSE_DIAGNOSTIC_SCALAR_FIELDS,
                        "selected",
                        "fields",
                    );
                } else {
                    diagnostic.push(" selected={} fields=[]");
                }
            } else {
                diagnostic.push("none");
            }
        }
        Ok(Value::String(_)) => diagnostic.push(" top_level=string"),
        Ok(Value::Number(_)) => diagnostic.push(" top_level=number"),
        Ok(Value::Bool(_)) => diagnostic.push(" top_level=boolean"),
        Ok(Value::Null) => diagnostic.push(" top_level=null"),
        Err(_) => diagnostic.push(" top_level=invalid_json"),
    }

    diagnostic.finish()
}

fn is_recoverable_network_error_envelope(bytes: &[u8]) -> bool {
    let Ok(Value::Object(object)) = serde_json::from_slice(bytes) else {
        return false;
    };

    matches!(
        object.get("message").and_then(Value::as_str),
        Some(RTDS_RECOVERABLE_NETWORK_ERROR | "Too Many Requests" | "Endpoint request timed out")
    ) && RTDS_MESSAGE_FIELDS
        .iter()
        .all(|field| !object.contains_key(*field))
}

impl crate::ws::traits::MessageParser<RtdsMessage> for SimpleParser {
    fn parse(&self, bytes: &[u8]) -> Result<Vec<RtdsMessage>> {
        parse_messages(bytes).map_err(|source| {
            let diagnostic = build_rtds_parse_diagnostic(bytes);
            if is_recoverable_network_error_envelope(bytes) {
                return recoverable_rtds_network_envelope(source, diagnostic);
            }
            let kind = source.kind();
            crate::error::Error::with_source(kind, RtdsParseDiagnosticError { source, diagnostic })
        })
    }
}

/// Unique identifier for a topic/type subscription combination.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicType {
    /// Topic name (e.g., `crypto_prices`, `comments`)
    pub topic: String,
    /// Message type (e.g., `update`, `comment_created`, `*`)
    pub msg_type: String,
}

impl TopicType {
    /// Create a new topic/type identifier.
    #[must_use]
    pub fn new(topic: String, msg_type: String) -> Self {
        Self { topic, msg_type }
    }
}

/// Information about an active subscription.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// Topic and message type this subscription targets
    pub topic_type: TopicType,
    /// Optional filters for this subscription
    pub filters: Option<String>,
    /// CLOB authentication if required
    pub clob_auth: Option<Credentials>,
    /// When the subscription was created
    pub created_at: Instant,
}

/// Manages active subscriptions and routes messages to subscribers.
pub struct SubscriptionManager {
    connection: ConnectionManager<RtdsMessage, SimpleParser>,
    active_subs: DashMap<String, SubscriptionInfo>,
    /// Subscribed topics with reference counts (for multiplexing)
    subscribed_topics: DashMap<TopicType, usize>,
    last_auth: RwLock<Option<Credentials>>,
}

impl SubscriptionManager {
    /// Create a new subscription manager.
    #[must_use]
    pub fn new(connection: ConnectionManager<RtdsMessage, SimpleParser>) -> Self {
        Self {
            connection,
            active_subs: DashMap::new(),
            subscribed_topics: DashMap::new(),
            last_auth: RwLock::new(None),
        }
    }

    /// Start the reconnection handler that re-subscribes on connection recovery.
    pub fn start_reconnection_handler(self: &Arc<Self>) {
        let this = Arc::clone(self);

        tokio::spawn(async move {
            let mut state_rx = this.connection.state_receiver();
            let mut was_connected = state_rx.borrow().is_connected();

            loop {
                // Wait for next state change
                if state_rx.changed().await.is_err() {
                    // Channel closed, connection manager is gone
                    break;
                }

                let state = *state_rx.borrow_and_update();

                match state {
                    ConnectionState::Connected { .. } => {
                        if was_connected {
                            // Reconnect to subscriptions
                            #[cfg(feature = "tracing")]
                            tracing::debug!("RTDS reconnected, re-establishing subscriptions");
                            this.resubscribe_all();
                        }
                        was_connected = true;
                    }
                    ConnectionState::Disconnected => {
                        // Connection permanently closed
                        break;
                    }
                    _ => {
                        // Other states are no-op
                    }
                }
            }
        });
    }

    /// Re-send subscription requests for all tracked topics.
    fn resubscribe_all(&self) {
        // Get stored auth for re-subscription on reconnect.
        // We can recover from poisoned lock because Option<Credentials> has no inconsistent intermediate state.
        let auth = self
            .last_auth
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();

        let subscriptions: Vec<Subscription> = self
            .active_subs
            .iter()
            .map(|entry| {
                let info = entry.value();
                let mut sub = Subscription {
                    topic: info.topic_type.topic.clone(),
                    msg_type: info.topic_type.msg_type.clone(),
                    filters: info.filters.clone(),
                    clob_auth: None,
                };
                // Apply stored auth if subscription originally had auth
                if info.clob_auth.is_some()
                    && let Some(creds) = &auth
                {
                    sub = sub.with_clob_auth(creds.clone());
                }
                sub
            })
            .collect();

        if subscriptions.is_empty() {
            return;
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(count = subscriptions.len(), "Re-subscribing to RTDS topics");

        let request = SubscriptionRequest::subscribe(subscriptions);
        if let Err(e) = self.connection.send(&request) {
            #[cfg(feature = "tracing")]
            tracing::warn!(%e, "Failed to re-subscribe to RTDS topics");
            #[cfg(not(feature = "tracing"))]
            let _: &crate::error::Error = &e;
        }
    }

    /// Subscribe to a topic with the given configuration.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Subscription is consumed to build SubscriptionInfo"
    )]
    pub fn subscribe(
        &self,
        subscription: Subscription,
    ) -> Result<impl Stream<Item = Result<RtdsMessage>>> {
        let topic_type = TopicType::new(subscription.topic.clone(), subscription.msg_type.clone());

        // Store auth for re-subscription on reconnect.
        // We can recover from poisoned lock because Option<Credentials> has no inconsistent intermediate state.
        if let Some(auth) = &subscription.clob_auth {
            *self
                .last_auth
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Some(auth.clone());
        }

        // Increment refcount or insert new topic with refcount=1
        // Using Entry API to atomically check and update, with send inside the guard
        // to prevent TOCTOU race between refcount check and network send
        match self.subscribed_topics.entry(topic_type.clone()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    topic = %subscription.topic,
                    msg_type = %subscription.msg_type,
                    "RTDS topic already subscribed, multiplexing"
                );
            }
            Entry::Vacant(entry) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    topic = %subscription.topic,
                    msg_type = %subscription.msg_type,
                    "Subscribing to RTDS topic"
                );

                // Send subscribe request while holding the entry lock to prevent
                // a concurrent unsubscribe from racing with us
                let request = SubscriptionRequest::subscribe(vec![subscription.clone()]);
                self.connection.send(&request)?;
                // Only insert after successful send
                entry.insert(1);
            }
        }

        // Register subscription info
        let sub_id = format!("{}:{}", topic_type.topic, topic_type.msg_type);
        self.active_subs.insert(
            sub_id,
            SubscriptionInfo {
                topic_type: topic_type.clone(),
                filters: subscription.filters.clone(),
                clob_auth: subscription.clob_auth.clone(),
                created_at: Instant::now(),
            },
        );

        // Create filtered stream with its own receiver
        let mut rx = self.connection.subscribe();
        let target_topic = topic_type.topic;
        let target_type = topic_type.msg_type;

        Ok(try_stream! {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        // Filter messages by topic and type
                        let matches_topic = msg.topic == target_topic;
                        let matches_type = target_type == "*" || msg.msg_type == target_type;

                        if matches_topic && matches_type {
                            yield msg;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        #[cfg(not(feature = "tracing"))]
                        let _ = n;
                        #[cfg(feature = "tracing")]
                        tracing::warn!("RTDS subscription lagged, missed {n} messages — continuing");
                    }
                    Err(RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
    }

    /// Get information about all active subscriptions.
    #[must_use]
    pub fn active_subscriptions(&self) -> Vec<SubscriptionInfo> {
        self.active_subs
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.active_subs.len()
    }

    /// Unsubscribe from topics.
    ///
    /// This decrements the reference count for each topic. Only sends an unsubscribe
    /// request to the server when the reference count reaches zero (no other streams
    /// are using that topic).
    pub fn unsubscribe(&self, topic_types: &[TopicType]) -> Result<()> {
        if topic_types.is_empty() {
            return Err(RtdsError::SubscriptionFailed(
                "topic_types cannot be empty: at least one topic must be provided for unsubscription"
                    .to_owned(),
            )
            .into());
        }

        // Atomically decrement refcounts and send unsubscribe while holding the entry lock
        // to prevent TOCTOU race between refcount check and network send
        for topic_type in topic_types {
            if let Entry::Occupied(mut entry) = self.subscribed_topics.entry(topic_type.clone()) {
                let refcount = entry.get_mut();
                *refcount = refcount.saturating_sub(1);
                if *refcount == 0 {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        topic = %topic_type.topic,
                        msg_type = %topic_type.msg_type,
                        "Unsubscribing from RTDS topic"
                    );

                    // Send unsubscribe while holding the entry lock to prevent
                    // a concurrent subscribe from racing with us
                    let request = SubscriptionRequest::unsubscribe(vec![Subscription {
                        topic: topic_type.topic.clone(),
                        msg_type: topic_type.msg_type.clone(),
                        filters: None,
                        clob_auth: None,
                    }]);
                    self.connection.send(&request)?;
                    entry.remove();
                }
            }
        }

        // Remove active_subs entries where all topics are now unsubscribed
        self.active_subs
            .retain(|_, info| self.subscribed_topics.contains_key(&info.topic_type));

        Ok(())
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    use crate::ws::traits::MessageParser as _;

    #[test]
    fn control_envelopes_use_exact_messages_and_preserve_target_field_exclusion() {
        // 据已见诊断重建对象，不作为归档原始网络帧。
        for message in [
            RTDS_RECOVERABLE_NETWORK_ERROR,
            "Too Many Requests",
            "Endpoint request timed out",
        ] {
            let bytes = serde_json::to_vec(
                &serde_json::json!({"message": message, "connectionId": "c", "requestId": "r"}),
            )
            .unwrap();
            assert!(parse_messages(&bytes).is_err());
            let error = SimpleParser.parse(&bytes).unwrap_err();
            assert_eq!(error.kind(), crate::error::Kind::WebSocket);
            assert!(error.to_string().contains(message));
        }
        for frame in [
            r#"{"message":"Too Many Requests","payload":null}"#,
            r#"{"message":"Endpoint request timed out "}"#,
            r#"[{"message":"Too Many Requests"}]"#,
        ] {
            let error = SimpleParser.parse(frame.as_bytes()).unwrap_err();
            assert!(
                error.downcast_ref::<RtdsParseDiagnosticError>().is_some(),
                "{frame}"
            );
        }
        let typed = br#"{"message":"Too Many Requests","topic":"crypto_prices_chainlink","type":"update","timestamp":1,"payload":{}}"#;
        assert_eq!(SimpleParser.parse(typed).unwrap().len(), 1);
    }

    const VALID_MESSAGE: &str = r#"{
        "topic":"crypto_prices_chainlink",
        "type":"update",
        "timestamp":1753314064237,
        "payload":{"symbol":"btc/usd","timestamp":1753314064213,"value":123.45}
    }"#;

    #[test]
    fn successful_single_and_array_frames_are_unchanged() {
        let single = SimpleParser.parse(VALID_MESSAGE.as_bytes()).unwrap();
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].topic, "crypto_prices_chainlink");
        assert_eq!(single[0].msg_type, "update");
        assert_eq!(single[0].timestamp, 1_753_314_064_237);

        let array = format!("[{VALID_MESSAGE},{VALID_MESSAGE}]");
        let messages = SimpleParser.parse(array.as_bytes()).unwrap();
        assert_eq!(messages.len(), 2);
        assert!(
            messages
                .iter()
                .all(|message| message.topic == "crypto_prices_chainlink")
        );
    }

    #[test]
    fn server_error_envelope_reports_only_bounded_contract_details() {
        let frame = br#"{
            "message":"Invalid request body",
            "connectionId":"connection-123",
            "requestId":"request-456",
            "status":400,
            "error":"allowed error",
            "payload":{"secret":"PAYLOAD_MUST_NOT_LEAK"},
            "unlisted":"UNLISTED_VALUE_MUST_NOT_LEAK"
        }"#;

        let error = SimpleParser.parse(frame).unwrap_err().to_string();
        assert!(error.contains("RTDS typed message parse failed"));
        assert!(error.contains(&format!("frame_bytes={}", frame.len())));
        assert!(error.contains(&format!("sha256={:x}", Sha256::digest(frame))));
        assert!(error.contains("top_level=object"));
        assert!(error.contains("message=\"Invalid request body\""));
        assert!(error.contains("connectionId=\"connection-123\""));
        assert!(error.contains("requestId=\"request-456\""));
        assert!(error.contains("status=400"));
        assert!(error.contains("error=\"allowed error\""));
        assert!(error.contains("fields=["));
        assert!(error.contains("\"unlisted\""));
        assert!(!error.contains("PAYLOAD_MUST_NOT_LEAK"));
        assert!(!error.contains("UNLISTED_VALUE_MUST_NOT_LEAK"));
    }

    #[test]
    fn operator_display_hides_non_allowlisted_invalid_scalar_but_keeps_typed_source() {
        let invalid_scalar = "INVALID_TIMESTAMP_MUST_NOT_LEAK";
        let nested_secret = "NESTED_PAYLOAD_MUST_NOT_LEAK";
        let frame = serde_json::to_vec(&serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": invalid_scalar,
            "payload": {"secret": nested_secret},
        }))
        .unwrap();

        let diagnosed = SimpleParser.parse(&frame).unwrap_err();
        let display = diagnosed.to_string();
        assert!(display.contains("rtds_parse_diagnostic="));
        assert!(display.contains(&format!("sha256={:x}", Sha256::digest(&frame))));
        assert!(display.contains("topic=\"crypto_prices_chainlink\""));
        assert!(display.contains("type=\"update\""));
        assert!(!display.contains(invalid_scalar));
        assert!(!display.contains(nested_secret));

        let wrapper = diagnosed
            .downcast_ref::<RtdsParseDiagnosticError>()
            .expect("diagnostic error wrapper");
        assert!(wrapper.source.to_string().contains(invalid_scalar));
        assert!(std::error::Error::source(wrapper).is_some());
    }

    #[test]
    fn string_body_reports_only_fingerprint_and_inner_allowlist() {
        let body = r#"{"requestId":"inner-request","payload":{"secret":"INNER_PAYLOAD_MUST_NOT_LEAK"},"message":"Invalid request body","connectionId":"inner-connection","unlisted":"INNER_UNLISTED_MUST_NOT_LEAK","error":"bad request"}"#;
        let frame = serde_json::to_vec(&serde_json::json!({
            "statusCode": 400,
            "body": body,
        }))
        .unwrap();

        let error = SimpleParser.parse(&frame).unwrap_err().to_string();
        assert!(error.contains("selected={statusCode=400}"));
        assert!(error.contains("fields=[\"body\",\"statusCode\"]"));
        assert!(error.contains("body_type=string"));
        assert!(error.contains(&format!("body_decoded_bytes={}", body.len())));
        assert!(error.contains(&format!(
            "body_decoded_sha256={:x}",
            Sha256::digest(body.as_bytes())
        )));
        assert!(error.contains("body_json_top_level=object"));
        assert!(error.contains("message=\"Invalid request body\""));
        assert!(error.contains("error=\"bad request\""));
        assert!(error.contains("connectionId=\"inner-connection\""));
        assert!(error.contains("requestId=\"inner-request\""));
        assert!(error.contains(
            "body_fields=[\"connectionId\",\"error\",\"message\",\"payload\",\"requestId\",\"unlisted\"]"
        ));
        assert!(!error.contains("INNER_PAYLOAD_MUST_NOT_LEAK"));
        assert!(!error.contains("INNER_UNLISTED_MUST_NOT_LEAK"));
    }

    #[test]
    fn non_json_string_body_is_fingerprinted_without_echoing_it() {
        let body = "<html>NON_JSON_BODY_MUST_NOT_LEAK</html>";
        let frame = serde_json::to_vec(&serde_json::json!({
            "statusCode": 502,
            "body": body,
        }))
        .unwrap();

        let diagnostic = build_rtds_parse_diagnostic(&frame);
        assert!(diagnostic.contains("selected={statusCode=502}"));
        assert!(diagnostic.contains("body_type=string"));
        assert!(diagnostic.contains(&format!("body_decoded_bytes={}", body.len())));
        assert!(diagnostic.contains(&format!(
            "body_decoded_sha256={:x}",
            Sha256::digest(body.as_bytes())
        )));
        assert!(diagnostic.contains("body_json_top_level=invalid_json"));
        assert!(!diagnostic.contains("NON_JSON_BODY_MUST_NOT_LEAK"));
    }

    #[test]
    fn object_body_uses_a_canonical_fingerprint_without_nested_values() {
        let frame = br#"{
            "statusCode":500,
            "body":{
                "z":1,
                "payload":{"secret":"OBJECT_BODY_MUST_NOT_LEAK"},
                "message":"Internal error",
                "connectionId":"inner-connection"
            }
        }"#;
        let canonical = br#"{"connectionId":"inner-connection","message":"Internal error","payload":{"secret":"OBJECT_BODY_MUST_NOT_LEAK"},"z":1}"#;

        let diagnostic = build_rtds_parse_diagnostic(frame);
        assert!(diagnostic.contains("body_type=object"));
        assert!(diagnostic.contains(&format!("body_canonical_json_bytes={}", canonical.len())));
        assert!(diagnostic.contains(&format!(
            "body_canonical_json_sha256={:x}",
            Sha256::digest(canonical)
        )));
        assert!(diagnostic.contains("body_json_top_level=object"));
        assert!(diagnostic.contains("message=\"Internal error\""));
        assert!(diagnostic.contains("connectionId=\"inner-connection\""));
        assert!(
            diagnostic.contains("body_fields=[\"connectionId\",\"message\",\"payload\",\"z\"]")
        );
        assert!(!diagnostic.contains("OBJECT_BODY_MUST_NOT_LEAK"));
        assert!(!diagnostic.contains("\"secret\""));
    }

    #[test]
    fn array_reports_the_first_contract_mismatch() {
        let frame = format!(
            "[{VALID_MESSAGE},{{\"message\":\"bad second item\",\"payload\":{{\"secret\":\"ARRAY_PAYLOAD_MUST_NOT_LEAK\"}}}},null]"
        );

        let error = SimpleParser
            .parse(frame.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(error.contains("top_level=array first_contract_mismatch_index=1"));
        assert!(error.contains("message=\"bad second item\""));
        assert!(!error.contains("ARRAY_PAYLOAD_MUST_NOT_LEAK"));
    }

    #[test]
    fn array_index_uses_the_real_typed_decode_for_duplicate_fields() {
        let frame =
            br#"[{"topic":"first","topic":"second","type":"update","timestamp":1,"payload":{}}]"#;

        let error = SimpleParser.parse(frame).unwrap_err().to_string();
        assert!(error.contains("top_level=array first_contract_mismatch_index=0"));
        let diagnosed = SimpleParser.parse(frame).unwrap_err();
        let wrapper = diagnosed
            .downcast_ref::<RtdsParseDiagnosticError>()
            .expect("diagnostic error wrapper");
        assert!(
            wrapper
                .source
                .to_string()
                .contains("duplicate field `topic`")
        );
    }

    #[test]
    fn malformed_json_preserves_original_error_without_echoing_the_frame() {
        let frame = br#"{"message":"MALFORMED_VALUE_MUST_NOT_LEAK""#;
        let error = SimpleParser.parse(frame).unwrap_err().to_string();

        assert!(error.contains("top_level=invalid_json"));
        assert!(!error.contains("MALFORMED_VALUE_MUST_NOT_LEAK"));
        let diagnosed = SimpleParser.parse(frame).unwrap_err();
        let wrapper = diagnosed
            .downcast_ref::<RtdsParseDiagnosticError>()
            .expect("diagnostic error wrapper");
        assert!(wrapper.source.to_string().contains("line 1 column"));
    }

    #[test]
    fn diagnostic_preserves_error_kind_and_source_chain() {
        let frame = br#"{"message":"Invalid request body","payload":{}}"#;
        let original = parse_messages(frame).unwrap_err();
        let diagnosed = SimpleParser.parse(frame).unwrap_err();

        assert_eq!(diagnosed.kind(), original.kind());
        assert!(
            diagnosed
                .to_string()
                .contains("RTDS typed message parse failed")
        );
        assert!(!diagnosed.to_string().contains(&original.to_string()));
        let wrapper = diagnosed
            .downcast_ref::<RtdsParseDiagnosticError>()
            .expect("diagnostic error wrapper");
        assert_eq!(wrapper.source.kind(), original.kind());
        assert_eq!(wrapper.source.to_string(), original.to_string());
        assert!(std::error::Error::source(&wrapper.source).is_some());
    }

    #[test]
    fn selected_scalar_control_characters_are_escaped() {
        let frame = serde_json::to_vec(&serde_json::json!({
            "message": "line one\nline two\t\u{0001}",
            "payload": {},
        }))
        .unwrap();

        let diagnostic = build_rtds_parse_diagnostic(&frame);
        assert!(diagnostic.contains("message=\"line one\\nline two\\t\\u0001\""));
        assert!(!diagnostic.contains('\n'));
        assert!(!diagnostic.contains('\t'));
        assert!(!diagnostic.contains('\u{0001}'));
    }

    #[test]
    fn diagnostic_is_utf8_safe_and_never_exceeds_2048_bytes() {
        let ascii_frame = serde_json::to_vec(&serde_json::json!({
            "message": "x".repeat(4_096),
            "payload": {"secret": "PAYLOAD_MUST_NOT_LEAK"},
        }))
        .unwrap();
        let ascii_diagnostic = build_rtds_parse_diagnostic(&ascii_frame);
        assert_eq!(ascii_diagnostic.len(), RTDS_PARSE_DIAGNOSTIC_MAX_BYTES);
        assert!(ascii_diagnostic.ends_with(RTDS_PARSE_DIAGNOSTIC_TRUNCATED));
        assert!(!ascii_diagnostic.contains("PAYLOAD_MUST_NOT_LEAK"));

        let unicode_frame = serde_json::to_vec(&serde_json::json!({
            "message": "错误".repeat(4_096),
            "payload": {},
        }))
        .unwrap();
        let unicode_diagnostic = build_rtds_parse_diagnostic(&unicode_frame);
        assert!(unicode_diagnostic.len() <= RTDS_PARSE_DIAGNOSTIC_MAX_BYTES);
        assert!(unicode_diagnostic.ends_with(RTDS_PARSE_DIAGNOSTIC_TRUNCATED));

        let inner_body = serde_json::to_string(&serde_json::json!({
            "message": "错误".repeat(4_096),
            "payload": {"secret": "INNER_PAYLOAD_MUST_NOT_LEAK"},
        }))
        .unwrap();
        let inner_frame = serde_json::to_vec(&serde_json::json!({
            "statusCode": 500,
            "body": inner_body,
        }))
        .unwrap();
        let inner_diagnostic = build_rtds_parse_diagnostic(&inner_frame);
        assert!(inner_diagnostic.len() <= RTDS_PARSE_DIAGNOSTIC_MAX_BYTES);
        assert!(inner_diagnostic.ends_with(RTDS_PARSE_DIAGNOSTIC_TRUNCATED));
        assert!(!inner_diagnostic.contains("INNER_PAYLOAD_MUST_NOT_LEAK"));
    }
}
