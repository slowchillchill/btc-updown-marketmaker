use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

use crate::publish::{PaperLiveEnvironment, Result, paper_live_handoff_boundary};

pub const HANDOFF_FRAME_SCHEMA_VERSION: &str = "data_collect_handoff_v1";

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
        };
        frame.validate()?;
        Ok(frame)
    }

    pub fn to_json_line(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("{}\n", serde_json::to_string(self)?))
    }

    fn validate(&self) -> Result<()> {
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
        Ok(())
    }
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
