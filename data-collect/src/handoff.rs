use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

use crate::publish::{PaperLiveEnvironment, Result, paper_live_handoff_boundary};
use crate::source_hourly_layout::{
    BINANCE_AGGTRADE_INVALID_REASON_CODE_MAX_BYTES, BINANCE_AGGTRADE_RECORD_INVALID_INPUT,
    BINANCE_AGGTRADE_RECORD_TRADE, BinanceAggTradeHourlyRow,
};

pub const HANDOFF_FRAME_SCHEMA_VERSION: &str = "data_collect_handoff_v1";
pub const HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION: &str = "data_collect_handoff_source_evidence_v1";
const MAX_SOURCE_SEQUENCE_EPOCH_ID_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffRecordKind {
    Trade,
    InvalidInput,
}

impl HandoffRecordKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "trade",
            Self::InvalidInput => "invalid_input",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffSourceEvidence {
    pub evidence_schema_version: String,
    pub record_kind: HandoffRecordKind,
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub websocket_ingress_sequence: u64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub invalid_reason_code: Option<String>,
}

impl HandoffSourceEvidence {
    pub fn trade(
        source_sequence_epoch_id: impl Into<String>,
        connection_generation: u64,
        websocket_ingress_sequence: u64,
        collector_admission_sequence: u64,
        collector_admission_time_utc_ms: i64,
    ) -> Result<Self> {
        Self::new(
            HandoffRecordKind::Trade,
            source_sequence_epoch_id,
            connection_generation,
            websocket_ingress_sequence,
            collector_admission_sequence,
            collector_admission_time_utc_ms,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn invalid_input(
        source_sequence_epoch_id: impl Into<String>,
        connection_generation: u64,
        websocket_ingress_sequence: u64,
        collector_admission_sequence: u64,
        collector_admission_time_utc_ms: i64,
        invalid_reason_code: impl Into<String>,
    ) -> Result<Self> {
        Self::new(
            HandoffRecordKind::InvalidInput,
            source_sequence_epoch_id,
            connection_generation,
            websocket_ingress_sequence,
            collector_admission_sequence,
            collector_admission_time_utc_ms,
            Some(invalid_reason_code.into()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        record_kind: HandoffRecordKind,
        source_sequence_epoch_id: impl Into<String>,
        connection_generation: u64,
        websocket_ingress_sequence: u64,
        collector_admission_sequence: u64,
        collector_admission_time_utc_ms: i64,
        invalid_reason_code: Option<String>,
    ) -> Result<Self> {
        let evidence = Self {
            evidence_schema_version: HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION.to_owned(),
            record_kind,
            source_sequence_epoch_id: source_sequence_epoch_id.into(),
            connection_generation,
            websocket_ingress_sequence,
            collector_admission_sequence,
            collector_admission_time_utc_ms,
            invalid_reason_code,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn correlation_key(&self) -> (&str, u64) {
        (
            self.source_sequence_epoch_id.as_str(),
            self.collector_admission_sequence,
        )
    }

    pub fn validate(&self) -> Result<()> {
        if self.evidence_schema_version != HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "unsupported source evidence schema version: {}",
                    self.evidence_schema_version
                ),
            )
            .into());
        }
        validate_bounded_nonempty(
            "source_sequence_epoch_id",
            &self.source_sequence_epoch_id,
            MAX_SOURCE_SEQUENCE_EPOCH_ID_BYTES,
        )?;
        if self.collector_admission_sequence == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "collector admission sequence must be positive",
            )
            .into());
        }
        if self.connection_generation == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection generation must be positive",
            )
            .into());
        }
        if self.websocket_ingress_sequence == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "websocket ingress sequence must be positive",
            )
            .into());
        }
        if self.collector_admission_time_utc_ms <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "collector admission timestamp must be positive",
            )
            .into());
        }
        match self.record_kind {
            HandoffRecordKind::Trade => {
                if self.invalid_reason_code.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "trade evidence must not have invalid_reason_code",
                    )
                    .into());
                }
            }
            HandoffRecordKind::InvalidInput => {
                let Some(reason) = self.invalid_reason_code.as_deref() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid input evidence requires invalid_reason_code",
                    )
                    .into());
                };
                validate_invalid_reason_code(reason)?;
            }
        }
        Ok(())
    }
}

impl TryFrom<&BinanceAggTradeHourlyRow> for HandoffSourceEvidence {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(row: &BinanceAggTradeHourlyRow) -> std::result::Result<Self, Self::Error> {
        let validation = row.validate();
        if !validation.is_complete() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "invalid Binance aggTrade hourly row: {}",
                    validation.missing_fields.join(",")
                ),
            )
            .into());
        }
        match row.record_kind.as_str() {
            BINANCE_AGGTRADE_RECORD_TRADE => Self::trade(
                row.source_sequence_epoch_id.clone(),
                row.connection_generation,
                row.websocket_ingress_sequence.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "trade row requires websocket_ingress_sequence",
                    )
                })?,
                row.collector_admission_sequence,
                row.collector_admission_time_utc_ms,
            ),
            BINANCE_AGGTRADE_RECORD_INVALID_INPUT => Self::invalid_input(
                row.source_sequence_epoch_id.clone(),
                row.connection_generation,
                row.websocket_ingress_sequence.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid input row requires websocket_ingress_sequence",
                    )
                })?,
                row.collector_admission_sequence,
                row.collector_admission_time_utc_ms,
                row.invalid_reason_code.as_deref().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid input row requires invalid_reason_code",
                    )
                })?,
            ),
            record_kind => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported Binance aggTrade record kind: {record_kind}"),
            )
            .into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffFrameType {
    ExternalMarketEvent,
    SourceStatus,
    SourceGap,
    ReconnectTrace,
    BackpressureState,
    Heartbeat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffFrame {
    pub frame_schema_version: String,
    pub frame_type: HandoffFrameType,
    /// Handoff transport-local frame sequence. This is not a source ingress or
    /// collector admission coordinate.
    pub event_seq: u64,
    pub source_kind: String,
    pub source_event_type: String,
    pub source_identifier: String,
    pub source_timestamp: Option<String>,
    pub collector_recv_time_utc_ms: i64,
    pub handoff_send_time_utc_ms: i64,
    pub collector_host_id: String,
    pub collector_region: String,
    pub raw_json: Option<String>,
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_evidence: Option<HandoffSourceEvidence>,
}

impl HandoffFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn external_market_event(
        event_seq: u64,
        source_kind: impl Into<String>,
        source_event_type: impl Into<String>,
        source_identifier: impl Into<String>,
        source_timestamp: Option<&str>,
        collector_recv_time_utc_ms: i64,
        handoff_send_time_utc_ms: i64,
        collector_host_id: impl Into<String>,
        collector_region: impl Into<String>,
        raw_json: &str,
    ) -> Result<Self> {
        serde_json::from_str::<serde_json::Value>(raw_json)?;
        let frame = Self {
            frame_schema_version: HANDOFF_FRAME_SCHEMA_VERSION.to_owned(),
            frame_type: HandoffFrameType::ExternalMarketEvent,
            event_seq,
            source_kind: source_kind.into(),
            source_event_type: source_event_type.into(),
            source_identifier: source_identifier.into(),
            source_timestamp: source_timestamp.map(str::to_owned),
            collector_recv_time_utc_ms,
            handoff_send_time_utc_ms,
            collector_host_id: collector_host_id.into(),
            collector_region: collector_region.into(),
            raw_json: Some(raw_json.to_owned()),
            status: None,
            source_evidence: None,
        };
        frame.validate()?;
        Ok(frame)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn source_evidence(
        handoff_frame_seq: u64,
        source_kind: impl Into<String>,
        source_event_type: impl Into<String>,
        source_identifier: impl Into<String>,
        source_timestamp: Option<&str>,
        collector_recv_time_utc_ms: i64,
        handoff_send_time_utc_ms: i64,
        collector_host_id: impl Into<String>,
        collector_region: impl Into<String>,
        raw_json: Option<&str>,
        source_evidence: HandoffSourceEvidence,
    ) -> Result<Self> {
        if let Some(raw_json) = raw_json {
            serde_json::from_str::<serde_json::Value>(raw_json)?;
        }
        let frame = Self {
            frame_schema_version: HANDOFF_FRAME_SCHEMA_VERSION.to_owned(),
            frame_type: HandoffFrameType::ExternalMarketEvent,
            event_seq: handoff_frame_seq,
            source_kind: source_kind.into(),
            source_event_type: source_event_type.into(),
            source_identifier: source_identifier.into(),
            source_timestamp: source_timestamp.map(str::to_owned),
            collector_recv_time_utc_ms,
            handoff_send_time_utc_ms,
            collector_host_id: collector_host_id.into(),
            collector_region: collector_region.into(),
            raw_json: raw_json.map(str::to_owned),
            status: None,
            source_evidence: Some(source_evidence),
        };
        frame.validate()?;
        Ok(frame)
    }

    pub fn status(
        event_seq: u64,
        status: impl Into<String>,
        source_kind: impl Into<String>,
        handoff_send_time_utc_ms: i64,
        collector_host_id: impl Into<String>,
        collector_region: impl Into<String>,
    ) -> Result<Self> {
        let frame = Self {
            frame_schema_version: HANDOFF_FRAME_SCHEMA_VERSION.to_owned(),
            frame_type: HandoffFrameType::SourceStatus,
            event_seq,
            source_kind: source_kind.into(),
            source_event_type: "source_status".to_owned(),
            source_identifier: "source_status".to_owned(),
            source_timestamp: None,
            collector_recv_time_utc_ms: handoff_send_time_utc_ms,
            handoff_send_time_utc_ms,
            collector_host_id: collector_host_id.into(),
            collector_region: collector_region.into(),
            raw_json: None,
            status: Some(status.into()),
            source_evidence: None,
        };
        frame.validate()?;
        Ok(frame)
    }

    pub fn to_json_line(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("{}\n", serde_json::to_string(self)?))
    }

    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("frame_schema_version", self.frame_schema_version.as_str()),
            ("source_kind", self.source_kind.as_str()),
            ("source_event_type", self.source_event_type.as_str()),
            ("source_identifier", self.source_identifier.as_str()),
            ("collector_host_id", self.collector_host_id.as_str()),
            ("collector_region", self.collector_region.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{field} must be non-empty"),
                )
                .into());
            }
        }
        if self.collector_recv_time_utc_ms <= 0 || self.handoff_send_time_utc_ms <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "collector and handoff timestamps must be positive",
            )
            .into());
        }
        if let Some(raw_json) = self.raw_json.as_deref() {
            serde_json::from_str::<serde_json::Value>(raw_json)?;
        }
        if self.frame_type == HandoffFrameType::ExternalMarketEvent
            && self.source_kind == "binance_aggtrade"
            && self.source_evidence.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "binance_aggtrade external market event requires source evidence",
            )
            .into());
        }
        if let Some(source_evidence) = self.source_evidence.as_ref() {
            if self.frame_type != HandoffFrameType::ExternalMarketEvent {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "source evidence requires an external market event frame",
                )
                .into());
            }
            source_evidence.validate()?;
            if self.source_kind != "binance_aggtrade" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "aggTrade source evidence requires source_kind=binance_aggtrade",
                )
                .into());
            }
            match source_evidence.record_kind {
                HandoffRecordKind::Trade => {
                    if self.source_event_type != "aggTrade" || self.raw_json.is_none() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "trade source evidence requires source_event_type=aggTrade and raw_json",
                        )
                        .into());
                    }
                }
                HandoffRecordKind::InvalidInput => {
                    if self.source_event_type != "invalid_input" {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid input source evidence requires source_event_type=invalid_input",
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_bounded_nonempty(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} must contain 1..={max_bytes} bytes"),
        )
        .into());
    }
    Ok(())
}

fn validate_invalid_reason_code(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > BINANCE_AGGTRADE_INVALID_REASON_CODE_MAX_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid_reason_code must contain only lowercase ASCII letters, digits, or underscore",
        )
        .into());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoffRuntimeConfig {
    pub environment: PaperLiveEnvironment,
    pub handoff_root: PathBuf,
    pub socket_path: PathBuf,
    pub frame_schema_version: String,
}

impl HandoffRuntimeConfig {
    pub fn for_environment(dataset_root: &Path, environment: PaperLiveEnvironment) -> Result<Self> {
        let boundary = paper_live_handoff_boundary(dataset_root, environment)?;
        Ok(Self {
            environment,
            socket_path: boundary.artifact_root.join("hot.sock"),
            handoff_root: boundary.artifact_root,
            frame_schema_version: HANDOFF_FRAME_SCHEMA_VERSION.to_owned(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeReceivedFrame {
    pub frame: HandoffFrame,
    pub runtime_receive_time_utc_ms: i64,
}

impl RuntimeReceivedFrame {
    pub const fn runtime_timestamp_field_name() -> &'static str {
        "runtime_receive_time_utc_ms"
    }
}

pub async fn run_single_frame_unix_handoff(
    config: &HandoffRuntimeConfig,
    frame: HandoffFrame,
) -> Result<RuntimeReceivedFrame> {
    fs::create_dir_all(&config.handoff_root)?;
    if config.socket_path.exists() {
        fs::remove_file(&config.socket_path)?;
    }
    let listener = UnixListener::bind(&config.socket_path)?;
    let socket_path = config.socket_path.clone();
    let payload = frame.to_json_line()?;

    let client = tokio::spawn(async move {
        let mut stream = UnixStream::connect(socket_path).await?;
        stream.write_all(payload.as_bytes()).await?;
        stream.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    let (stream, _) = listener.accept().await?;
    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut line).await?;
    client
        .await
        .map_err(|error| io::Error::other(format!("handoff client join failed: {error}")))??;

    let frame = serde_json::from_str::<HandoffFrame>(&line)?;
    frame.validate()?;
    let received = RuntimeReceivedFrame {
        frame,
        runtime_receive_time_utc_ms: now_ms(),
    };
    let _ = fs::remove_file(&config.socket_path);
    Ok(received)
}

pub async fn run_single_frame_unix_server_handoff(
    config: &HandoffRuntimeConfig,
    frame: HandoffFrame,
) -> Result<RuntimeReceivedFrame> {
    fs::create_dir_all(&config.handoff_root)?;
    if config.socket_path.exists() {
        fs::remove_file(&config.socket_path)?;
    }
    let listener = UnixListener::bind(&config.socket_path)?;
    let socket_path = config.socket_path.clone();
    let consumer = tokio::spawn(async move {
        let stream = UnixStream::connect(socket_path).await?;
        let mut line = String::new();
        let mut reader = BufReader::new(stream);
        reader.read_line(&mut line).await?;
        let frame = serde_json::from_str::<HandoffFrame>(&line)?;
        frame.validate()?;
        Ok::<RuntimeReceivedFrame, Box<dyn std::error::Error + Send + Sync>>(RuntimeReceivedFrame {
            frame,
            runtime_receive_time_utc_ms: now_ms(),
        })
    });

    let (mut stream, _) = listener.accept().await?;
    stream.write_all(frame.to_json_line()?.as_bytes()).await?;
    stream.shutdown().await?;
    let received = consumer
        .await
        .map_err(|error| io::Error::other(format!("handoff consumer join failed: {error}")))??;
    let _ = fs::remove_file(&config.socket_path);
    Ok(received)
}

pub async fn run_heartbeat_handoff_server(
    config: &HandoffRuntimeConfig,
    collector_host_id: &str,
    collector_region: &str,
    max_runtime_secs: Option<u64>,
) -> Result<()> {
    fs::create_dir_all(&config.handoff_root)?;
    if config.socket_path.exists() {
        fs::remove_file(&config.socket_path)?;
    }
    let listener = UnixListener::bind(&config.socket_path)?;
    let started = Instant::now();
    let max_runtime = Duration::from_secs(max_runtime_secs.unwrap_or(7 * 24 * 60 * 60));
    let mut event_seq = 1_u64;

    while started.elapsed() < max_runtime {
        match timeout(Duration::from_millis(250), listener.accept()).await {
            Ok(Ok((mut stream, _))) => {
                let frame = HandoffFrame::status(
                    event_seq,
                    "heartbeat",
                    "data_collect_hot_handoff",
                    now_ms(),
                    collector_host_id,
                    collector_region,
                )?;
                event_seq = event_seq.saturating_add(1);
                stream.write_all(frame.to_json_line()?.as_bytes()).await?;
                stream.shutdown().await?;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {}
        }
    }

    let _ = fs::remove_file(&config.socket_path);
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
