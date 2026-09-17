use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::UnixListener;
use tokio::time::timeout;

use crate::deployment::CollectorStartupReceipt;
use crate::publish::{PaperLiveEnvironment, Result, paper_live_handoff_root};
use data_pipeline_core::public_input::{
    DATA_COLLECT_HANDOFF_SCHEMA_V2, DataCollectHandoffSessionV2, DataCollectHandoffV2,
    TypedRawRowV2,
};

/// 只按实际读到的字节增长；长度头不用于预分配或设置新的业务上限。
pub async fn read_handoff_v2<R: AsyncRead + Unpin>(reader: &mut R) -> Result<DataCollectHandoffV2> {
    let body = read_handoff_body(reader).await?;
    Ok(serde_json::from_slice(&body)?)
}

pub(crate) async fn read_handoff_body<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut remaining = reader.read_u64().await?;
    let mut body = Vec::new();
    let mut chunk = [0_u8; 8 * 1024];
    while remaining != 0 {
        let count = remaining.min(chunk.len() as u64) as usize;
        reader.read_exact(&mut chunk[..count]).await?;
        body.extend_from_slice(&chunk[..count]);
        remaining -= count as u64;
    }
    Ok(body)
}

pub(crate) async fn write_handoff_body<W: AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
) -> Result<()> {
    writer.write_u64(body.len() as u64).await?;
    writer.write_all(body).await?;
    Ok(())
}

pub async fn write_handoff_v2<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &DataCollectHandoffV2,
) -> Result<()> {
    let body = serde_json::to_vec(frame)?;
    write_handoff_body(writer, &body).await
}

pub(crate) async fn receive_confirmation<R: AsyncRead + Unpin>(
    reader: &mut R,
    expected: u64,
) -> Result<()> {
    match read_handoff_v2(reader).await? {
        DataCollectHandoffV2::Received { transport_sequence } if transport_sequence == expected => {
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("handoff expected Received for sequence {expected}"),
        )
        .into()),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoffReceiveSummary {
    pub session: DataCollectHandoffSessionV2,
    pub last_confirmed_sequence: u64,
    pub aborted_reason: Option<String>,
}

/// 原单消费者协议的逐帧接收端；控制帧不被转换成业务 EOF。
pub struct HandoffReceiverV2<S> {
    stream: S,
    session: DataCollectHandoffSessionV2,
    confirmed: u64,
}
impl<S: AsyncRead + AsyncWrite + Unpin> HandoffReceiverV2<S> {
    pub async fn start(mut stream: S) -> Result<Self> {
        let DataCollectHandoffV2::Session(session) = read_handoff_v2(&mut stream).await? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "handoff must begin with Session",
            )
            .into());
        };
        if session.frame_schema_version != DATA_COLLECT_HANDOFF_SCHEMA_V2 {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "unsupported handoff schema").into(),
            );
        }
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::Received {
                transport_sequence: 0,
            },
        )
        .await?;
        Ok(Self {
            stream,
            session,
            confirmed: 0,
        })
    }
    pub fn session(&self) -> &DataCollectHandoffSessionV2 {
        &self.session
    }
    pub fn last_confirmed_sequence(&self) -> u64 {
        self.confirmed
    }
    pub async fn next_frame(&mut self) -> Result<DataCollectHandoffV2> {
        let frame = read_handoff_v2(&mut self.stream).await?;
        let invalid = match &frame {
            DataCollectHandoffV2::Message {
                transport_sequence, ..
            } => (self.confirmed.checked_add(1) != Some(*transport_sequence))
                .then_some("handoff business sequence is not consecutive"),
            DataCollectHandoffV2::End {
                last_confirmed_sequence,
            } => (*last_confirmed_sequence != self.confirmed)
                .then_some("handoff End does not match confirmed prefix"),
            DataCollectHandoffV2::Aborted {
                last_confirmed_sequence,
                ..
            } => (*last_confirmed_sequence != self.confirmed)
                .then_some("handoff Aborted does not match confirmed prefix"),
            _ => Some("unexpected handoff control frame"),
        };
        if let Some(reason) = invalid {
            return Err(io::Error::new(io::ErrorKind::InvalidData, reason).into());
        }
        Ok(frame)
    }
    /// 当前对象消费并移交后确认；不表明核心应用或后台稳定。
    pub async fn confirm(&mut self, transport_sequence: u64) -> Result<()> {
        write_handoff_v2(
            &mut self.stream,
            &DataCollectHandoffV2::Received { transport_sequence },
        )
        .await?;
        self.confirmed = transport_sequence;
        Ok(())
    }
}

/// 同步借用消费返回后释放当前对象，再发送原 Received。
pub async fn consume_handoff_v2<S, F>(
    stream: &mut S,
    mut consume: F,
) -> Result<HandoffReceiveSummary>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(&TypedRawRowV2) -> Result<()>,
{
    let mut confirmed = 0;
    let result: Result<HandoffReceiveSummary> = async {
        let mut receiver = HandoffReceiverV2::start(stream).await?;
        loop {
            match receiver.next_frame().await? {
                DataCollectHandoffV2::Message {
                    transport_sequence,
                    row,
                } => {
                    consume(&row)?;
                    drop(row);
                    receiver.confirm(transport_sequence).await?;
                    confirmed = transport_sequence;
                }
                DataCollectHandoffV2::End { .. } => {
                    return Ok(HandoffReceiveSummary {
                        session: receiver.session,
                        last_confirmed_sequence: confirmed,
                        aborted_reason: None,
                    });
                }
                DataCollectHandoffV2::Aborted { reason, .. } => {
                    return Ok(HandoffReceiveSummary {
                        session: receiver.session,
                        last_confirmed_sequence: confirmed,
                        aborted_reason: Some(reason),
                    });
                }
                _ => unreachable!("接收器已核对控制帧"),
            }
        }
    }
    .await;
    result.map_err(|error| {
        io::Error::other(format!(
            "handoff_receive_failed:last_confirmed_sequence={confirmed}:{error}"
        ))
        .into()
    })
}

pub const HANDOFF_FRAME_SCHEMA_VERSION: &str = "data_collect_handoff_v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffFrameType {
    SourceStatus,
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
}

impl HandoffFrame {
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
        let handoff_root = paper_live_handoff_root(dataset_root, environment)?;
        Ok(Self {
            environment,
            socket_path: handoff_root.join("hot.sock"),
            handoff_root,
            frame_schema_version: HANDOFF_FRAME_SCHEMA_VERSION.to_owned(),
        })
    }
}

pub(crate) async fn run_source_handoff_server<F, Fut>(
    config: &HandoffRuntimeConfig,
    receipt: CollectorStartupReceipt,
    max_runtime_secs: Option<u64>,
    run_sources: F,
) -> Result<()>
where
    F: FnOnce(
        tokio::net::UnixStream,
        CollectorStartupReceipt,
        Instant,
        Option<Instant>,
        Option<i64>,
    ) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    fs::create_dir_all(&config.handoff_root)?;
    if config.socket_path.exists() {
        fs::remove_file(&config.socket_path)?;
    }
    let listener = UnixListener::bind(&config.socket_path)?;
    let started = Instant::now();
    let duration = max_runtime_secs.map(Duration::from_secs);
    let deadline = duration.map(|duration| started + duration);
    let deadline_ms = duration.map(|duration| now_ms().saturating_add(duration.as_millis() as i64));
    let result = async {
        let accepted = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), listener.accept()).await,
            None => Ok(listener.accept().await),
        };
        let (mut stream, _) = match accepted {
            Ok(result) => result?,
            Err(_) => return Ok(()),
        };
        let session = DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
            frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.to_owned(),
            environment: config.environment.as_str().to_owned(),
            run_id: receipt.run_id.clone(),
            git_commit: receipt.git_commit.clone(),
            binary_sha256: receipt.binary_sha256.clone(),
            config_digest: receipt.config_digest.clone(),
            collector_host_id: receipt.collector_host_id.clone(),
            collector_region: receipt.collector_region.clone(),
        });
        let confirmed = async {
            write_handoff_v2(&mut stream, &session).await?;
            receive_confirmation(&mut stream, 0).await
        };
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), confirmed)
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "hot session was not confirmed before runtime cutoff",
                    )
                })??,
            None => confirmed.await?,
        }
        drop(session);
        run_sources(stream, receipt, started, deadline, deadline_ms).await
    }
    .await;
    let _ = fs::remove_file(&config.socket_path);
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> DataCollectHandoffV2 {
        DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
            frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.to_owned(),
            environment: "paper".to_owned(),
            run_id: "handoff-test".to_owned(),
            git_commit: "test-commit".to_owned(),
            binary_sha256: "a".repeat(64),
            config_digest: "b".repeat(64),
            collector_host_id: "test-host".to_owned(),
            collector_region: "test-region".to_owned(),
        })
    }

    #[tokio::test]
    async fn malformed_body_and_huge_declared_short_body_are_never_confirmed() {
        for huge in [false, true] {
            let (mut sender, mut receiver) = tokio::io::duplex(128);
            let consumer = tokio::spawn(async move {
                consume_handoff_v2(&mut receiver, |_| panic!("damaged body cannot be consumed"))
                    .await
            });
            write_handoff_v2(&mut sender, &session()).await.unwrap();
            receive_confirmation(&mut sender, 0).await.unwrap();
            if huge {
                sender.write_u64(u64::MAX).await.unwrap();
                sender.write_all(b"{short").await.unwrap();
            } else {
                write_handoff_body(&mut sender, b"{malformed-json}")
                    .await
                    .unwrap();
            }
            sender.shutdown().await.unwrap();
            let error = consumer.await.unwrap().unwrap_err().to_string();
            assert!(!error.is_empty());
            let mut next = [0_u8; 1];
            assert_eq!(
                sender.read(&mut next).await.unwrap(),
                0,
                "no Received after damaged input"
            );
        }
    }

    #[tokio::test]
    async fn confirmation_must_match_the_current_message() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        write_handoff_v2(
            &mut writer,
            &DataCollectHandoffV2::Received {
                transport_sequence: 2,
            },
        )
        .await
        .unwrap();
        assert!(
            receive_confirmation(&mut reader, 1)
                .await
                .unwrap_err()
                .to_string()
                .contains("sequence 1")
        );
    }

    #[tokio::test]
    async fn receiver_does_not_adopt_an_unconfirmed_terminal_prefix() {
        let (mut sender, mut receiver) = tokio::io::duplex(128);
        let task = tokio::spawn(async move { consume_handoff_v2(&mut receiver, |_| Ok(())).await });
        write_handoff_v2(&mut sender, &session()).await.unwrap();
        receive_confirmation(&mut sender, 0).await.unwrap();
        write_handoff_v2(
            &mut sender,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 1,
            },
        )
        .await
        .unwrap();
        let error = task.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("End does not match confirmed prefix"));
        assert!(error.contains("last_confirmed_sequence=0"));
    }

    #[tokio::test]
    async fn receiver_requires_consecutive_business_and_counts_only_written_acknowledgments() {
        use data_pipeline_core::typed_raw::{
            PolymarketOrderbookEvent, PolymarketOrderbookRow, TypedRawRow,
        };
        for sequence in [2, 1] {
            let (mut sender, mut receiver) = tokio::io::duplex(4096);
            let consumed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = std::sync::Arc::clone(&consumed);
            let task = tokio::spawn(async move {
                consume_handoff_v2(&mut receiver, |_| {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                })
                .await
            });
            write_handoff_v2(&mut sender, &session()).await.unwrap();
            receive_confirmation(&mut sender, 0).await.unwrap();
            let row =
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(PolymarketOrderbookRow {
                    timestamp_received: 1783392404600,
                    timestamp: Some(1783392404599),
                    market: "0x0000000000000000000000000000000000000000000000000000000000000002"
                        .to_owned(),
                    slug: None,
                    collector_host_id: "test-host".to_owned(),
                    collector_region: "test-region".to_owned(),
                    source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
                        .to_owned(),
                    schema_version: "polymarket_orderbook_v3".to_owned(),
                    source_sequence_epoch_id: "test-epoch".to_owned(),
                    collector_admission_sequence: 1,
                    collector_admission_time_utc_ms: 1783392404601,
                    event: PolymarketOrderbookEvent::Book {
                        asset_id: "101".to_owned(),
                        bids: Vec::new(),
                        asks: Vec::new(),
                        last_trade_price: None,
                    },
                }));
            write_handoff_v2(
                &mut sender,
                &DataCollectHandoffV2::Message {
                    transport_sequence: sequence,
                    row,
                },
            )
            .await
            .unwrap();
            drop(sender);
            let error = task.await.unwrap().unwrap_err().to_string();
            assert!(error.contains("last_confirmed_sequence=0"));
            if sequence == 2 {
                assert!(error.contains("business sequence is not consecutive"));
                assert_eq!(consumed.load(std::sync::atomic::Ordering::Relaxed), 0);
            } else {
                assert_eq!(consumed.load(std::sync::atomic::Ordering::Relaxed), 1);
            }
        }
    }

    #[tokio::test]
    async fn partial_header_is_read_with_the_same_frame_protocol() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        let expected = DataCollectHandoffV2::Aborted {
            last_confirmed_sequence: 7,
            reason: "actual source failure".to_owned(),
        };
        let body = serde_json::to_vec(&expected).unwrap();
        let task = tokio::spawn(async move {
            for byte in (body.len() as u64).to_be_bytes() {
                writer.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
            for chunk in body.chunks(3) {
                writer.write_all(chunk).await.unwrap();
            }
        });
        assert_eq!(read_handoff_v2(&mut reader).await.unwrap(), expected);
        task.await.unwrap();
    }
}
