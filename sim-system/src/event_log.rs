use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
#[cfg(feature = "measurement")]
use std::sync::Arc;
#[cfg(feature = "measurement")]
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[cfg(feature = "measurement")]
use crate::measurement_probe::{MeasurementContext, MeasurementPhase, MeasurementProbe};
use crate::runtime_v2::{
    BusinessInputV2, BusinessProjectionVersionV1, CancelScenarioHitV1, LoggedBusinessInputV2,
    RunDurabilityRoleV2, RunOpenedV2,
};

pub(crate) const SIM_EVENT_LOG_FILE_NAME_V2: &str = "sim-event-log-v2.jsonl";

pub(crate) fn delete_retained_run_log_v2(path: &Path) -> Result<(), SimEventLogErrorV2> {
    fs::remove_file(path).map_err(|error| io_error("删除已关闭运行的单一事件日志", error))
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
enum SimEventLogLineV2 {
    RunOpened(RunOpenedV2),
    AcceptedInputs(Vec<LoggedBusinessInputV2>),
    AcceptedInputsV3 {
        inputs: Vec<LoggedBusinessInputV2>,
        cancel_scenario_hits: Vec<CancelScenarioHitV1>,
    },
    RunSeal,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
enum SimEventLogLineRefV2<'a> {
    RunOpened(&'a RunOpenedV2),
    AcceptedInputs(&'a [BusinessInputV2]),
    AcceptedInputsV3 {
        inputs: &'a [BusinessInputV2],
        cancel_scenario_hits: &'a [CancelScenarioHitV1],
    },
    RunSeal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimEventLogErrorV2 {
    #[cfg(feature = "shared-execution")]
    Ledger(ledger::LedgerDurableErrorV1),
    Noe(execution_normalization::DurableErrorV1),
    NoeNormalization(crate::run_v2::MemoryNoeErrorV1),
    Io {
        operation: &'static str,
        message: String,
    },
    Encode(String),
    CompleteLineDecode {
        offset: u64,
        message: String,
    },
    InvalidStructure(&'static str),
    GroupTooLarge {
        limit_bytes: usize,
    },
    UnsealedCleanRun,
}

impl std::fmt::Display for SimEventLogErrorV2 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "shared-execution")]
            Self::Ledger(error) => write!(formatter, "Ledger 后台物化失败：{error}"),
            Self::Io { operation, message } => write!(formatter, "{operation}: {message}"),
            Self::NoeNormalization(error) => write!(formatter, "NOE 前台归一化失败：{error}"),
            Self::Noe(error) => write!(formatter, "NOE 后台物化失败：{error}"),
            Self::Encode(message) => write!(formatter, "日志组编码失败：{message}"),
            Self::CompleteLineDecode { offset, message } => {
                write!(formatter, "完整日志行在字节 {offset} 无法解码：{message}")
            }
            Self::InvalidStructure(message) => write!(formatter, "日志结构非法：{message}"),
            Self::GroupTooLarge { limit_bytes } => {
                write!(formatter, "日志组超过 {limit_bytes} 字节硬上限")
            }
            Self::UnsealedCleanRun => write!(formatter, "干净运行没有完整 RunSeal"),
        }
    }
}

impl std::error::Error for SimEventLogErrorV2 {}

/// 固定版本前缀；随后是长度（u64 little endian）和一帧CBOR。
pub(crate) const HISTORICAL_DAY_LOG_MAGIC_V1: &[u8; 8] = b"SIMDAY01";
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
enum SimLogLayoutV1 {
    #[default]
    JsonV2,
    HistoricalDayV1(crate::public_consumption::HistoricalDayLogBindingV1),
}

// 日布局保留原接纳组，不将字段位置视为业务输入序号。
struct DayInputRefV1<'a>(&'a BusinessInputV2);
impl Serialize for DayInputRefV1<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeTuple};
        macro_rules! tuple {
            ($tag:expr, $($field:expr),+ $(,)?) => {{
                let count = 1 + [$(stringify!($field)),+].len();
                let mut tuple = serializer.serialize_tuple(count)?;
                tuple.serialize_element(&$tag)?;
                $(tuple.serialize_element($field)?;)+
                tuple.end()
            }};
        }
        match self.0 {
            BusinessInputV2::PublicObserveTime(time) => {
                tuple!(0u8, &time.drive_position(), &time.logical_time_utc_ms())
            }
            BusinessInputV2::PublicCloseTime(close) => tuple!(
                1u8,
                &close.execution_time.drive_position(),
                &close.execution_time.logical_time_utc_ms(),
                &close.next_group_position,
                &close.closure
            ),
            BusinessInputV2::PlatformPrefixAdvance(prefix) => tuple!(
                2u8,
                &prefix.path,
                &prefix.closed_through_utc_ms,
                &prefix.next_index_position
            ),
            BusinessInputV2::PublicGroup(group) => tuple!(
                3u8,
                &group.execution_time.drive_position(),
                &group.execution_time.logical_time_utc_ms(),
                &group.reference.applied_prefix.next_input_position,
                &group.reference.group_position,
                &group.reference.closed_at_input_position
            ),
            BusinessInputV2::HiddenStepBucket(bucket) => {
                let crate::public_consumption::HiddenBucketReferenceV2::Streaming(reference) =
                    &bucket.reference
                else {
                    return Err(S::Error::custom("历史日布局需要原流式隐藏来源"));
                };
                tuple!(
                    4u8,
                    &reference.source.applied_prefix.next_input_position,
                    &reference.bucket_time_utc_ms,
                    &reference.index_start,
                    &reference.index_end,
                    &reference.closure
                )
            }
            // 原正式业务载荷先沿既有JSON serde语义形成值，避免CBOR非人可读模式改变正式编码。
            other => tuple!(5u8, &serde_json::to_value(other).map_err(S::Error::custom)?),
        }
    }
}
#[derive(Debug)]
enum DayInputRecordV1 {
    Observe(u64, i64),
    Close(
        u64,
        i64,
        u64,
        crate::public_consumption::PublicTimeClosureReasonV1,
    ),
    Prefix(crate::market::MarketEventPathV1, i64, u64),
    Public(u64, i64, u64, u64, u64),
    Hidden(u64, i64, u64, u64, contracts::ReplayIndexClosureV1),
    Full(serde_json::Value),
}
impl<'de> Deserialize<'de> for DayInputRecordV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DayInputRecordV1;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("历史日输入元组")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                macro_rules! field {
                    () => {
                        seq.next_element()?
                            .ok_or_else(|| A::Error::custom("历史日输入元组缺字段"))?
                    };
                }
                let tag: u8 = field!();
                Ok(match tag {
                    0 => DayInputRecordV1::Observe(field!(), field!()),
                    1 => DayInputRecordV1::Close(field!(), field!(), field!(), field!()),
                    2 => DayInputRecordV1::Prefix(field!(), field!(), field!()),
                    3 => DayInputRecordV1::Public(field!(), field!(), field!(), field!(), field!()),
                    4 => DayInputRecordV1::Hidden(field!(), field!(), field!(), field!(), field!()),
                    5 => DayInputRecordV1::Full(field!()),
                    _ => return Err(A::Error::custom("未知历史日输入类型")),
                })
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}
impl DayInputRecordV1 {
    fn into_logged(
        self,
        run_id: &str,
        binding: &crate::public_consumption::HistoricalDayLogBindingV1,
        offset: u64,
    ) -> Result<LoggedBusinessInputV2, SimEventLogErrorV2> {
        use crate::public_consumption::*;
        let time = |drive_position, logical_time_utc_ms| ActorTimeRecordV1 {
            run_id: run_id.into(),
            drive_position,
            logical_time_utc_ms,
        };
        Ok(match self {
            Self::Observe(p, at) => LoggedBusinessInputV2::PublicObserveTime(time(p, at)),
            Self::Close(p, at, next, closure) => {
                LoggedBusinessInputV2::PublicCloseTime(LoggedPublicClosureV1 {
                    execution_time: time(p, at),
                    consumption_sequence_ref: binding.consumption_sequence_ref.clone(),
                    next_group_position: next,
                    closure,
                })
            }
            Self::Prefix(path, at, next) => LoggedBusinessInputV2::PlatformPrefixAdvance(
                crate::runtime_v2::PlatformPrefixAdvanceV2 {
                    path,
                    closed_through_utc_ms: at,
                    next_index_position: next,
                },
            ),
            Self::Public(p, at, next, group, closed) => {
                LoggedBusinessInputV2::DayPublicGroup(LoggedDayPublicGroupV1 {
                    execution_time: time(p, at),
                    next_input_position: next,
                    group_position: group,
                    closed_at_input_position: closed,
                })
            }
            Self::Hidden(applied, at, start, end, closure) => {
                LoggedBusinessInputV2::DayHiddenStepBucket(LoggedDayHiddenBucketV1 {
                    applied_input_position: applied,
                    bucket_time_utc_ms: at,
                    index_start: start,
                    index_end: end,
                    closure,
                })
            }
            Self::Full(value) => serde_json::from_value(value).map_err(|e| {
                SimEventLogErrorV2::CompleteLineDecode {
                    offset,
                    message: e.to_string(),
                }
            })?,
        })
    }
}
fn encode_day_frame<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, SimEventLogErrorV2> {
    let mut buffer = BoundedLineBufferV2::new(limit);
    buffer
        .write_all(&[0; 8])
        .map_err(|_| SimEventLogErrorV2::GroupTooLarge { limit_bytes: limit })?;
    ciborium::ser::into_writer(value, &mut buffer).map_err(|error| match error {
        ciborium::ser::Error::Io(_) => SimEventLogErrorV2::GroupTooLarge { limit_bytes: limit },
        error => SimEventLogErrorV2::Encode(error.to_string()),
    })?;
    let mut bytes = buffer.into_inner();
    let length = (bytes.len() - 8) as u64;
    bytes[..8].copy_from_slice(&length.to_le_bytes());
    #[cfg(test)]
    record_line_encode_for_tests();
    Ok(bytes)
}
enum DayFrameReadV1 {
    End,
    Partial,
    Complete(Vec<u8>),
}
fn read_day_frame(
    reader: &mut impl Read,
    limit: usize,
) -> Result<DayFrameReadV1, SimEventLogErrorV2> {
    let mut prefix = [0; 8];
    let mut count = 0;
    while count < 8 {
        let n = reader
            .read(&mut prefix[count..])
            .map_err(|e| io_error("读取日SIM帧长度", e))?;
        if n == 0 {
            return Ok(if count == 0 {
                DayFrameReadV1::End
            } else {
                DayFrameReadV1::Partial
            });
        }
        count += n;
    }
    let length = u64::from_le_bytes(prefix);
    if length.checked_add(8).is_none_or(|n| n > limit as u64) {
        return Err(SimEventLogErrorV2::GroupTooLarge { limit_bytes: limit });
    }
    let mut bytes = vec![0; length as usize];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(DayFrameReadV1::Complete(bytes)),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(DayFrameReadV1::Partial),
        Err(e) => Err(io_error("读取日SIM帧正文", e)),
    }
}
fn decode_day_frame<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    offset: u64,
) -> Result<T, SimEventLogErrorV2> {
    let mut reader = io::Cursor::new(bytes);
    let value = ciborium::de::from_reader(&mut reader).map_err(|e| {
        SimEventLogErrorV2::CompleteLineDecode {
            offset,
            message: e.to_string(),
        }
    })?;
    if reader.position() != bytes.len() as u64 {
        return Err(SimEventLogErrorV2::CompleteLineDecode {
            offset,
            message: "CBOR帧存在额外正文".into(),
        });
    }
    Ok(value)
}

pub(crate) struct SimEventLogV2 {
    file: File,
    layout: SimLogLayoutV1,
    durability_role: RunDurabilityRoleV2,
    projection_version: BusinessProjectionVersionV1,
    byte_count: u64,
    next_input_sequence: u64,
    max_group_bytes: usize,
    #[cfg(feature = "measurement")]
    measurement_probe: Arc<MeasurementProbe>,
    #[cfg(test)]
    fail_next_write: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SimLogCheckpointV1 {
    byte_count: u64,
    next_input_sequence: u64,
    #[serde(default)]
    layout: SimLogLayoutV1,
}
impl SimEventLogV2 {
    fn checkpoint(&self) -> SimLogCheckpointV1 {
        SimLogCheckpointV1 {
            byte_count: self.byte_count,
            next_input_sequence: self.next_input_sequence,
            layout: self.layout.clone(),
        }
    }
    pub(crate) fn restore_checkpoint(
        path: &Path,
        opened: &RunOpenedV2,
        saved: SimLogCheckpointV1,
        max_group_bytes: usize,
    ) -> Result<Self, SimEventLogErrorV2> {
        use std::io::{Seek, SeekFrom};
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| io_error("打开原 SIM 检查点日志", e))?;
        if file
            .metadata()
            .map_err(|e| io_error("读取原 SIM 日志长度", e))?
            .len()
            < saved.byte_count
        {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "SIM 日志短于完整检查点",
            ));
        }
        file.set_len(saved.byte_count)
            .map_err(|e| io_error("丢弃 SIM 未提交执行尾", e))?;
        file.seek(SeekFrom::Start(saved.byte_count))
            .map_err(|e| io_error("定位 SIM 续写位置", e))?;
        file.sync_data()
            .map_err(|e| io_error("稳定 SIM 检查点尾部恢复", e))?;
        Ok(Self {
            layout: saved.layout,
            file,
            durability_role: opened.durability_role,
            projection_version: opened.projection_version,
            byte_count: saved.byte_count,
            next_input_sequence: saved.next_input_sequence,
            max_group_bytes,
            #[cfg(feature = "measurement")]
            measurement_probe: Arc::new(MeasurementProbe::default()),
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            fail_next_sync: false,
        })
    }

    pub(crate) fn create(
        path: &Path,
        opened: &RunOpenedV2,
        max_group_bytes: usize,
    ) -> Result<Self, SimEventLogErrorV2> {
        Self::create_with_layout(path, opened, max_group_bytes, SimLogLayoutV1::JsonV2)
    }
    pub(crate) fn create_historical_day(
        path: &Path,
        opened: &RunOpenedV2,
        max_group_bytes: usize,
        binding: crate::public_consumption::HistoricalDayLogBindingV1,
    ) -> Result<Self, SimEventLogErrorV2> {
        Self::create_with_layout(
            path,
            opened,
            max_group_bytes,
            SimLogLayoutV1::HistoricalDayV1(binding),
        )
    }
    fn create_with_layout(
        path: &Path,
        opened: &RunOpenedV2,
        max_group_bytes: usize,
        layout: SimLogLayoutV1,
    ) -> Result<Self, SimEventLogErrorV2> {
        #[cfg(feature = "measurement")]
        let measurement_probe = Arc::new(MeasurementProbe::default());
        #[cfg(feature = "measurement")]
        let encode_started = Instant::now();
        let encoded = match &layout {
            SimLogLayoutV1::JsonV2 => {
                encode_line(&SimEventLogLineRefV2::RunOpened(opened), max_group_bytes)?
            }
            SimLogLayoutV1::HistoricalDayV1(binding) => {
                let mut bytes = HISTORICAL_DAY_LOG_MAGIC_V1.to_vec();
                bytes.extend(encode_day_frame(
                    &(
                        serde_json::to_value(opened)
                            .map_err(|e| SimEventLogErrorV2::Encode(e.to_string()))?,
                        binding,
                    ),
                    max_group_bytes,
                )?);
                bytes
            }
        };
        #[cfg(feature = "measurement")]
        measurement_probe.record(
            MeasurementContext::CommitGroup,
            MeasurementPhase::LogEncode,
            elapsed_ns(encode_started),
        );
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error("创建单一事件日志", error))?;
        let mut log = Self {
            file,
            layout,
            durability_role: opened.durability_role,
            projection_version: opened.projection_version,
            byte_count: 0,
            next_input_sequence: 1,
            max_group_bytes,
            #[cfg(feature = "measurement")]
            measurement_probe,
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            fail_next_sync: false,
        };
        #[cfg(feature = "measurement")]
        log.write_encoded(&encoded, MeasurementContext::CommitGroup)?;
        #[cfg(not(feature = "measurement"))]
        log.write_encoded(&encoded)?;
        if opened.durability_role == RunDurabilityRoleV2::RecoverablePaper {
            #[cfg(feature = "measurement")]
            log.sync_data(MeasurementContext::CommitGroup)?;
            #[cfg(not(feature = "measurement"))]
            log.sync_data()?;
        }
        Ok(log)
    }

    pub(crate) fn append_group(
        self,
        inputs: &[BusinessInputV2],
    ) -> Result<(Self, Range<u64>), SimEventLogErrorV2> {
        self.append_group_with_hits(inputs, &[])
    }
    pub(crate) fn append_group_with_hits(
        self,
        inputs: &[BusinessInputV2],
        hits: &[CancelScenarioHitV1],
    ) -> Result<(Self, Range<u64>), SimEventLogErrorV2> {
        let sync = self.durability_role == RunDurabilityRoleV2::RecoverablePaper;
        self.append_group_with_sync(inputs, sync, hits)
    }

    fn append_group_with_sync(
        mut self,
        inputs: &[BusinessInputV2],
        sync: bool,
        hits: &[CancelScenarioHitV1],
    ) -> Result<(Self, Range<u64>), SimEventLogErrorV2> {
        let start = self.next_input_sequence;
        if inputs.is_empty() {
            return Ok((self, start..start));
        }
        let next = start
            .checked_add(inputs.len() as u64)
            .expect("有界事件日志的输入序号不会溢出");
        #[cfg(test)]
        record_group_append_for_tests(inputs.len() as u64);
        #[cfg(feature = "measurement")]
        let encode_started = Instant::now();
        let line = if self.projection_version == BusinessProjectionVersionV1::V4 {
            SimEventLogLineRefV2::AcceptedInputsV3 {
                inputs,
                cancel_scenario_hits: hits,
            }
        } else {
            SimEventLogLineRefV2::AcceptedInputs(inputs)
        };
        let encoded = match &self.layout {
            SimLogLayoutV1::JsonV2 => encode_line(&line, self.max_group_bytes)?,
            SimLogLayoutV1::HistoricalDayV1(_) => encode_day_frame(
                &(inputs.iter().map(DayInputRefV1).collect::<Vec<_>>(), hits),
                self.max_group_bytes,
            )?,
        };
        #[cfg(feature = "measurement")]
        self.measurement_probe.record(
            MeasurementContext::CommitGroup,
            MeasurementPhase::LogEncode,
            elapsed_ns(encode_started),
        );
        #[cfg(feature = "measurement")]
        self.write_encoded(&encoded, MeasurementContext::CommitGroup)?;
        #[cfg(not(feature = "measurement"))]
        self.write_encoded(&encoded)?;
        if sync {
            #[cfg(feature = "measurement")]
            self.sync_data(MeasurementContext::CommitGroup)?;
            #[cfg(not(feature = "measurement"))]
            self.sync_data()?;
        }
        self.next_input_sequence = next;
        Ok((self, start..next))
    }

    pub(crate) fn seal_clean_run(mut self) -> Result<(), SimEventLogErrorV2> {
        if self.durability_role != RunDurabilityRoleV2::CleanRun {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "RecoverablePaper 不使用 RunSeal",
            ));
        }
        #[cfg(feature = "measurement")]
        let encode_started = Instant::now();
        let encoded = encode_line(&SimEventLogLineRefV2::RunSeal, self.max_group_bytes)?;
        #[cfg(feature = "measurement")]
        self.measurement_probe.record(
            MeasurementContext::CleanSeal,
            MeasurementPhase::LogEncode,
            elapsed_ns(encode_started),
        );
        #[cfg(feature = "measurement")]
        self.write_encoded(&encoded, MeasurementContext::CleanSeal)?;
        #[cfg(not(feature = "measurement"))]
        self.write_encoded(&encoded)?;
        #[cfg(feature = "measurement")]
        return self.sync_data(MeasurementContext::CleanSeal);
        #[cfg(not(feature = "measurement"))]
        self.sync_data()
    }

    pub(crate) fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub(crate) fn next_input_sequence(&self) -> u64 {
        self.next_input_sequence
    }

    #[cfg(feature = "measurement")]
    pub(crate) fn measurement_probe(&self) -> Arc<MeasurementProbe> {
        Arc::clone(&self.measurement_probe)
    }

    fn write_encoded(
        &mut self,
        encoded: &[u8],
        #[cfg(feature = "measurement")] context: MeasurementContext,
    ) -> Result<(), SimEventLogErrorV2> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_write) {
            return Err(io_error(
                "追加单一事件日志组",
                io::Error::other("测试注入写入失败"),
            ));
        }
        #[cfg(feature = "measurement")]
        let started = Instant::now();
        self.file
            .write_all(encoded)
            .map_err(|error| io_error("追加单一事件日志组", error))?;
        #[cfg(feature = "measurement")]
        self.measurement_probe
            .record(context, MeasurementPhase::LogWrite, elapsed_ns(started));
        self.byte_count = self
            .byte_count
            .checked_add(encoded.len() as u64)
            .expect("有界事件日志的字节计数不会溢出");
        Ok(())
    }

    fn sync_data(
        &mut self,
        #[cfg(feature = "measurement")] context: MeasurementContext,
    ) -> Result<(), SimEventLogErrorV2> {
        #[cfg(test)]
        record_sync_attempt_for_tests();
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_sync) {
            return Err(io_error(
                "同步单一事件日志",
                io::Error::other("测试注入同步失败"),
            ));
        }
        #[cfg(feature = "measurement")]
        let started = Instant::now();
        self.file
            .sync_data()
            .map_err(|error| io_error("同步单一事件日志", error))?;
        #[cfg(feature = "measurement")]
        self.measurement_probe
            .record(context, MeasurementPhase::LogSync, elapsed_ns(started));
        #[cfg(test)]
        record_sync_for_tests();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_write_failure_for_tests(&mut self) {
        self.fail_next_write = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_sync_failure_for_tests(&mut self) {
        self.fail_next_sync = true;
    }
}

pub(crate) struct SimEventLogRecoveryV2 {
    reader: BufReader<File>,
    layout: SimLogLayoutV1,
    run_id: String,
    pub(crate) cancel_scenario_hits: Vec<CancelScenarioHitV1>,
    durability_role: RunDurabilityRoleV2,
    projection_version: BusinessProjectionVersionV1,
    max_group_bytes: usize,
    last_complete_offset: u64,
    next_input_sequence: u64,
    incomplete_tail: bool,
    saw_seal: bool,
    reached_eof: bool,
}

impl SimEventLogRecoveryV2 {
    pub(crate) fn open(
        path: &Path,
        max_group_bytes: usize,
    ) -> Result<(RunOpenedV2, Self), SimEventLogErrorV2> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error("打开单一事件日志", error))?;
        let mut reader = BufReader::new(file);
        let is_day = reader
            .fill_buf()
            .map_err(|e| io_error("读取SIM格式", e))?
            .starts_with(HISTORICAL_DAY_LOG_MAGIC_V1);
        let (opened, layout, last_complete_offset) = if is_day {
            reader
                .seek(SeekFrom::Start(8))
                .map_err(|e| io_error("读取日SIM头", e))?;
            let DayFrameReadV1::Complete(bytes) = read_day_frame(&mut reader, max_group_bytes)?
            else {
                return Err(SimEventLogErrorV2::InvalidStructure("缺少完整日SIM头"));
            };
            let (value, binding): (
                serde_json::Value,
                crate::public_consumption::HistoricalDayLogBindingV1,
            ) = decode_day_frame(&bytes, 8)?;
            let opened: RunOpenedV2 = serde_json::from_value(value).map_err(|e| {
                SimEventLogErrorV2::CompleteLineDecode {
                    offset: 8,
                    message: e.to_string(),
                }
            })?;
            (
                opened,
                SimLogLayoutV1::HistoricalDayV1(binding),
                16 + bytes.len() as u64,
            )
        } else {
            let mut buffer = Vec::new();
            let first = read_complete_line(&mut reader, &mut buffer, 0, max_group_bytes)?
                .ok_or(SimEventLogErrorV2::InvalidStructure("缺少完整 RunOpened"))?;
            let SimEventLogLineV2::RunOpened(opened) = first else {
                return Err(SimEventLogErrorV2::InvalidStructure(
                    "第一行必须是 RunOpened",
                ));
            };
            (opened, SimLogLayoutV1::JsonV2, buffer.len() as u64)
        };
        let recovery = Self {
            reader,
            layout,
            run_id: opened.run_id.clone(),
            cancel_scenario_hits: Vec::new(),
            durability_role: opened.durability_role,
            projection_version: opened.projection_version,
            max_group_bytes,
            last_complete_offset,
            next_input_sequence: 1,
            incomplete_tail: false,
            saw_seal: false,
            reached_eof: false,
        };
        Ok((opened, recovery))
    }

    #[cfg(test)]
    pub(crate) fn historical_day_binding_for_tests(
        &self,
    ) -> Option<&crate::public_consumption::HistoricalDayLogBindingV1> {
        match &self.layout {
            SimLogLayoutV1::HistoricalDayV1(binding) => Some(binding),
            SimLogLayoutV1::JsonV2 => None,
        }
    }
    pub(crate) fn next_group(
        mut self,
    ) -> Result<(Self, Option<Vec<LoggedBusinessInputV2>>), SimEventLogErrorV2> {
        if self.reached_eof {
            return Ok((self, None));
        }
        if let SimLogLayoutV1::HistoricalDayV1(binding) = &self.layout {
            let bytes = match read_day_frame(&mut self.reader, self.max_group_bytes)? {
                DayFrameReadV1::End => {
                    self.reached_eof = true;
                    return Ok((self, None));
                }
                DayFrameReadV1::Partial => {
                    self.incomplete_tail = true;
                    self.reached_eof = true;
                    return Ok((self, None));
                }
                DayFrameReadV1::Complete(bytes) => bytes,
            };
            let (inputs, hits): (Vec<DayInputRecordV1>, Vec<CancelScenarioHitV1>) =
                decode_day_frame(&bytes, self.last_complete_offset)?;
            let inputs = inputs
                .into_iter()
                .map(|i| i.into_logged(&self.run_id, binding, self.last_complete_offset))
                .collect::<Result<Vec<_>, _>>()?;
            self.cancel_scenario_hits = hits;
            self.last_complete_offset += 8 + bytes.len() as u64;
            self.next_input_sequence += inputs.len() as u64;
            return Ok((self, Some(inputs)));
        }
        loop {
            let offset = self.last_complete_offset;
            let mut buffer = Vec::new();
            let read = read_bounded_line(&mut self.reader, &mut buffer, self.max_group_bytes)?;
            if read == 0 {
                self.reached_eof = true;
                return Ok((self, None));
            }
            if buffer.last() != Some(&b'\n') {
                if self.durability_role == RunDurabilityRoleV2::CleanRun {
                    return Err(SimEventLogErrorV2::UnsealedCleanRun);
                }
                self.incomplete_tail = true;
                self.reached_eof = true;
                return Ok((self, None));
            }
            self.last_complete_offset = self
                .last_complete_offset
                .checked_add(read as u64)
                .expect("有界事件日志的字节偏移不会溢出");
            let line = serde_json::from_slice::<SimEventLogLineV2>(&buffer).map_err(|error| {
                SimEventLogErrorV2::CompleteLineDecode {
                    offset,
                    message: error.to_string(),
                }
            })?;
            match line {
                SimEventLogLineV2::RunOpened(_) => {
                    return Err(SimEventLogErrorV2::InvalidStructure(
                        "RunOpened 只能出现一次",
                    ));
                }
                SimEventLogLineV2::AcceptedInputs(inputs)
                | SimEventLogLineV2::AcceptedInputsV3 { inputs, .. } => {
                    if self.saw_seal {
                        return Err(SimEventLogErrorV2::InvalidStructure(
                            "RunSeal 后不能再有输入",
                        ));
                    }
                    self.next_input_sequence = self
                        .next_input_sequence
                        .checked_add(inputs.len() as u64)
                        .expect("有界事件日志的输入序号不会溢出");
                    return Ok((self, Some(inputs)));
                }
                SimEventLogLineV2::RunSeal => {
                    if self.durability_role != RunDurabilityRoleV2::CleanRun {
                        return Err(SimEventLogErrorV2::InvalidStructure(
                            "RecoverablePaper 不能包含 RunSeal",
                        ));
                    }
                    if self.saw_seal {
                        return Err(SimEventLogErrorV2::InvalidStructure("RunSeal 只能出现一次"));
                    }
                    self.saw_seal = true;
                }
            }
        }
    }

    pub(crate) fn resume_paper(self) -> Result<SimEventLogV2, SimEventLogErrorV2> {
        if self.durability_role != RunDurabilityRoleV2::RecoverablePaper {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "只有 RecoverablePaper 可以续写",
            ));
        }
        if !self.reached_eof {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "续写前必须读到日志末尾",
            ));
        }
        let mut file = self.reader.into_inner();
        if self.incomplete_tail {
            file.set_len(self.last_complete_offset)
                .map_err(|error| io_error("截去事件日志末尾不完整组", error))?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|error| io_error("定位事件日志续写位置", error))?;
        Ok(SimEventLogV2 {
            file,
            layout: self.layout,
            durability_role: self.durability_role,
            projection_version: self.projection_version,
            byte_count: self.last_complete_offset,
            next_input_sequence: self.next_input_sequence,
            max_group_bytes: self.max_group_bytes,
            #[cfg(feature = "measurement")]
            measurement_probe: Arc::new(MeasurementProbe::default()),
            #[cfg(test)]
            fail_next_write: false,
            #[cfg(test)]
            fail_next_sync: false,
        })
    }

    pub(crate) fn finish_sealed_clean_run(self) -> Result<(), SimEventLogErrorV2> {
        if !self.reached_eof {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "封口裁决前必须读到日志末尾",
            ));
        }
        if self.durability_role != RunDurabilityRoleV2::CleanRun || !self.saw_seal {
            return Err(SimEventLogErrorV2::UnsealedCleanRun);
        }
        Ok(())
    }
}

fn encode_line(
    line: &SimEventLogLineRefV2<'_>,
    limit_bytes: usize,
) -> Result<Vec<u8>, SimEventLogErrorV2> {
    let mut buffer = BoundedLineBufferV2::new(limit_bytes);
    serde_json::to_writer(&mut buffer, line)
        .map_err(|error| map_encode_error(error, limit_bytes))?;
    buffer
        .write_all(b"\n")
        .map_err(|_| SimEventLogErrorV2::GroupTooLarge { limit_bytes })?;
    #[cfg(test)]
    record_line_encode_for_tests();
    Ok(buffer.into_inner())
}

#[cfg(feature = "measurement")]
fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn read_complete_line(
    reader: &mut BufReader<File>,
    buffer: &mut Vec<u8>,
    offset: u64,
    max_group_bytes: usize,
) -> Result<Option<SimEventLogLineV2>, SimEventLogErrorV2> {
    let read = read_bounded_line(reader, buffer, max_group_bytes)?;
    if read == 0 || buffer.last() != Some(&b'\n') {
        return Ok(None);
    }
    serde_json::from_slice(buffer).map(Some).map_err(|error| {
        SimEventLogErrorV2::CompleteLineDecode {
            offset,
            message: error.to_string(),
        }
    })
}

fn read_bounded_line(
    reader: &mut BufReader<File>,
    buffer: &mut Vec<u8>,
    max_group_bytes: usize,
) -> Result<usize, SimEventLogErrorV2> {
    let mut bounded = reader.take(max_group_bytes.saturating_add(1) as u64);
    let read = bounded
        .read_until(b'\n', buffer)
        .map_err(|error| io_error("读取单一事件日志", error))?;
    if buffer.len() > max_group_bytes {
        return Err(SimEventLogErrorV2::GroupTooLarge {
            limit_bytes: max_group_bytes,
        });
    }
    Ok(read)
}

struct BoundedLineBufferV2 {
    bytes: Vec<u8>,
    limit_bytes: usize,
}

impl BoundedLineBufferV2 {
    fn new(limit_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit_bytes,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedLineBufferV2 {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(bytes.len()) else {
            return Err(io::Error::other("日志组超过字节硬上限"));
        };
        if next_len > self.limit_bytes {
            return Err(io::Error::other("日志组超过字节硬上限"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn map_encode_error(error: serde_json::Error, limit_bytes: usize) -> SimEventLogErrorV2 {
    if error.is_io() {
        SimEventLogErrorV2::GroupTooLarge { limit_bytes }
    } else {
        SimEventLogErrorV2::Encode(error.to_string())
    }
}

fn io_error(operation: &'static str, error: io::Error) -> SimEventLogErrorV2 {
    SimEventLogErrorV2::Io {
        operation,
        message: error.to_string(),
    }
}

#[cfg(test)]
thread_local! {
    static LINE_ENCODE_COUNT_FOR_TESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SYNC_COUNT_FOR_TESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GROUP_APPEND_COUNT_FOR_TESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static INPUT_ENCODE_COUNT_FOR_TESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SYNC_ATTEMPT_COUNT_FOR_TESTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_line_encode_for_tests() {
    LINE_ENCODE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn record_sync_for_tests() {
    SYNC_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn record_sync_attempt_for_tests() {
    SYNC_ATTEMPT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn record_group_append_for_tests(input_count: u64) {
    GROUP_APPEND_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
    INPUT_ENCODE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + input_count));
}

#[cfg(test)]
pub(crate) fn reset_group_append_counts_for_tests() {
    GROUP_APPEND_COUNT_FOR_TESTS.with(|count| count.set(0));
    INPUT_ENCODE_COUNT_FOR_TESTS.with(|count| count.set(0));
    SYNC_ATTEMPT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn group_append_counts_for_tests() -> (u64, u64) {
    (
        GROUP_APPEND_COUNT_FOR_TESTS.with(std::cell::Cell::get),
        INPUT_ENCODE_COUNT_FOR_TESTS.with(std::cell::Cell::get),
    )
}

#[cfg(test)]
pub(crate) fn sync_attempt_count_for_tests() -> u64 {
    SYNC_ATTEMPT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_event_log_counts_for_tests() {
    LINE_ENCODE_COUNT_FOR_TESTS.with(|count| count.set(0));
    SYNC_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn event_log_counts_for_tests() -> (u64, u64) {
    (
        LINE_ENCODE_COUNT_FOR_TESTS.with(std::cell::Cell::get),
        SYNC_COUNT_FOR_TESTS.with(std::cell::Cell::get),
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use contracts::Sha256Hex;

    use super::*;
    use crate::market::MarketEventPathV1;
    use crate::platform_rules::ImplementationIdentityV1;
    use crate::runtime_v2::PlatformPrefixAdvanceV2;

    const MAX_GROUP_BYTES: usize = 64 * 1024;

    fn sha(byte: char) -> Sha256Hex {
        Sha256Hex(std::iter::repeat_n(byte, 64).collect())
    }

    fn opened(role: RunDurabilityRoleV2) -> RunOpenedV2 {
        RunOpenedV2 {
            projection_version: Default::default(),
            run_id: "run-event-log-v2".to_owned(),
            durability_role: role,
            platform_rule_snapshot_file_sha256: sha('1'),
            trade_finality_policy_file_sha256: sha('2'),
            platform_scenario_file_sha256: sha('3'),
            proxy_latency_profile_file_sha256: sha('4'),
            implementation_identity: ImplementationIdentityV1 {
                git_commit: "a".repeat(40),
                source_identity_sha256: sha('5'),
                cargo_lock_sha256: sha('6'),
            },
            market_event_source_identity_sha256: Some(sha('7')),
        }
    }

    fn input(suffix: &str) -> BusinessInputV2 {
        BusinessInputV2::ActivateSession {
            activation_id: format!("activation-{suffix}"),
        }
    }

    fn read_all_groups(
        mut recovery: SimEventLogRecoveryV2,
    ) -> Result<(SimEventLogRecoveryV2, Vec<Vec<LoggedBusinessInputV2>>), SimEventLogErrorV2> {
        let mut groups = Vec::new();
        loop {
            let (next, group) = recovery.next_group()?;
            recovery = next;
            let Some(group) = group else {
                return Ok((recovery, groups));
            };
            groups.push(group);
        }
    }

    fn day_binding() -> crate::public_consumption::HistoricalDayLogBindingV1 {
        crate::public_consumption::HistoricalDayLogBindingV1 {
            input_stream_id: "original-public".into(),
            consumption_sequence_ref: "original-consumption".into(),
            hidden_input_set_identity_sha256: sha('8'),
            hidden_consumer_stream_id: "original-hidden".into(),
            hidden_ordering_version: "fixture-order-v1".into(),
        }
    }

    #[test]
    fn historical_day_frames_preserve_groups_hits_and_resume_at_physical_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("day.sim");
        let header = opened(RunDurabilityRoleV2::FiniteHistorical);
        let binding = day_binding();
        let log =
            SimEventLogV2::create_historical_day(&path, &header, MAX_GROUP_BYTES, binding.clone())
                .unwrap();
        let prefix = PlatformPrefixAdvanceV2 {
            path: MarketEventPathV1::HiddenPlatform,
            closed_through_utc_ms: 100,
            next_index_position: 0,
        };
        let hits = vec![CancelScenarioHitV1 {
            configuration_input_sequence: 1,
            global_request_id: "request-a".into(),
            platform_order_id: "order-a".into(),
            effective_processing_at_utc_ms: 100,
        }];
        let (log, range) = log
            .append_group_with_hits(
                &[input("a"), BusinessInputV2::PlatformPrefixAdvance(prefix)],
                &hits,
            )
            .unwrap();
        assert_eq!(range, 1..3);
        let saved = log.checkpoint();
        let saved_bytes = saved.byte_count;
        let (log, range) = log.append_group(&[input("uncommitted")]).unwrap();
        assert_eq!(range, 3..4);
        drop(log);
        // 已写完但未发布到完整日点的尾帧不得再次进入经济前缀。
        let saved = serde_json::from_slice(&serde_json::to_vec(&saved).unwrap()).unwrap();
        let log =
            SimEventLogV2::restore_checkpoint(&path, &header, saved, MAX_GROUP_BYTES).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), saved_bytes);
        assert_eq!(log.next_input_sequence(), 3);
        let (log, range) = log.append_group(&[input("resumed")]).unwrap();
        assert_eq!(range, 3..4);
        assert_eq!(log.byte_count, std::fs::metadata(&path).unwrap().len());
        drop(log);
        let (decoded, recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(recovery.historical_day_binding_for_tests(), Some(&binding));
        let (recovery, first) = recovery.next_group().unwrap();
        assert_eq!(
            first.unwrap(),
            vec![
                LoggedBusinessInputV2::ActivateSession {
                    activation_id: "activation-a".into()
                },
                LoggedBusinessInputV2::PlatformPrefixAdvance(prefix)
            ]
        );
        assert_eq!(recovery.cancel_scenario_hits, hits);
        let (recovery, second) = recovery.next_group().unwrap();
        assert_eq!(
            second.unwrap(),
            vec![LoggedBusinessInputV2::ActivateSession {
                activation_id: "activation-resumed".into()
            }]
        );
        assert!(recovery.cancel_scenario_hits.is_empty());
        let (recovery, end) = recovery.next_group().unwrap();
        assert!(end.is_none());
        assert_eq!(recovery.next_input_sequence, 4);
        assert!(!recovery.incomplete_tail);
        // 既有检查点无新增布局字段时，继续原JSON写法。
        let old: SimLogCheckpointV1 =
            serde_json::from_value(serde_json::json!({"byte_count":12,"next_input_sequence":2}))
                .unwrap();
        assert_eq!(old.layout, SimLogLayoutV1::JsonV2);
    }

    #[test]
    fn historical_day_partial_tail_is_incomplete_and_complete_corruption_is_fatal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("day.sim");
        let header = opened(RunDurabilityRoleV2::FiniteHistorical);
        let log =
            SimEventLogV2::create_historical_day(&path, &header, MAX_GROUP_BYTES, day_binding())
                .unwrap();
        let saved = log.checkpoint();
        let (log, _) = log.append_group(&[input("a")]).unwrap();
        let complete_bytes = log.byte_count;
        drop(log);
        // 帧正文被截断：保留此前完整组，明确未完成而非成功结束。
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(complete_bytes - 1)
            .unwrap();
        let (_, recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
        let (recovery, group) = recovery.next_group().unwrap();
        assert!(group.is_none());
        assert!(recovery.incomplete_tail);
        assert_eq!(recovery.last_complete_offset, saved.byte_count);
        // 恢复原完整点后续写；完整损坏帧不能被当成可丢弃的截断尾。
        let log =
            SimEventLogV2::restore_checkpoint(&path, &header, saved, MAX_GROUP_BYTES).unwrap();
        drop(log);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&1u64.to_le_bytes()).unwrap();
        file.write_all(&[0xff]).unwrap();
        drop(file);
        let (_, recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
        assert!(matches!(
            recovery.next_group(),
            Err(SimEventLogErrorV2::CompleteLineDecode { .. })
        ));
        // 上限按实际帧字节计量，不为比较旧JSON而重新物化公共报告。
        assert!(matches!(
            encode_day_frame(&vec![0u8; MAX_GROUP_BYTES], 16),
            Err(SimEventLogErrorV2::GroupTooLarge { limit_bytes: 16 })
        ));
    }

    #[test]
    fn source_digest_old_string_and_absent_use_real_log_codec() {
        for digest in [Some(sha('7')), None] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
            let mut header = opened(RunDurabilityRoleV2::CleanRun);
            header.market_event_source_identity_sha256 = digest.clone();
            SimEventLogV2::create(&path, &header, MAX_GROUP_BYTES)
                .unwrap()
                .seal_clean_run()
                .unwrap();
            let line: serde_json::Value = serde_json::from_str(
                std::fs::read_to_string(&path)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap(),
            )
            .unwrap();
            let encoded = serde_json::to_string(&line).unwrap();
            if let Some(value) = &digest {
                assert!(encoded.contains(&format!(
                    "\"market_event_source_identity_sha256\":\"{}\"",
                    value.0
                )));
            } else {
                assert!(!encoded.contains("market_event_source_identity_sha256"));
            }
            let (decoded, recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
            assert_eq!(decoded, header);
            let (recovery, groups) = read_all_groups(recovery).unwrap();
            assert!(groups.is_empty());
            recovery.finish_sealed_clean_run().unwrap();
        }
    }

    #[test]
    fn paper_open_and_each_group_encode_and_sync_once() {
        reset_event_log_counts_for_tests();
        let directory = tempfile::tempdir().expect("创建日志测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let log = SimEventLogV2::create(
            &path,
            &opened(RunDurabilityRoleV2::RecoverablePaper),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 日志");
        assert_eq!(event_log_counts_for_tests(), (1, 1));
        let (log, range) = log
            .append_group(&[input("a"), input("b")])
            .expect("追加 Paper 组");
        assert_eq!(range, 1..3);
        assert_eq!(log.next_input_sequence(), 3);
        assert_eq!(event_log_counts_for_tests(), (2, 2));
        let wire = std::fs::read_to_string(&path).expect("读取 Paper 日志线格式");
        assert!(
            !wire.contains("session_id"),
            "唯一会话形成后的输入不得重复编码会话路由身份"
        );
    }

    #[test]
    fn necessary_prefix_advance_is_one_bodyless_logged_input() {
        reset_event_log_counts_for_tests();
        let directory = tempfile::tempdir().expect("创建日志测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let log = SimEventLogV2::create(
            &path,
            &opened(RunDurabilityRoleV2::RecoverablePaper),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 日志");
        let advance = PlatformPrefixAdvanceV2 {
            path: MarketEventPathV1::HiddenPlatform,
            closed_through_utc_ms: 100,
            next_index_position: 2,
        };
        let (log, range) = log
            .append_group(&[BusinessInputV2::PlatformPrefixAdvance(advance)])
            .expect("追加必要空前缀");
        assert_eq!(range, 1..2);
        assert_eq!(event_log_counts_for_tests(), (2, 2));
        drop(log);

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开空前缀日志");
        let (_, groups) = read_all_groups(recovery).expect("重放空前缀日志");
        assert_eq!(
            groups,
            vec![vec![LoggedBusinessInputV2::PlatformPrefixAdvance(advance)]]
        );
        let wire = std::fs::read_to_string(&path).expect("读取空前缀线格式");
        let prefix_line = wire.lines().nth(1).expect("空前缀位于第二条日志行");
        assert!(!prefix_line.contains("session_id"));
        assert!(!prefix_line.contains("events"));
        assert!(!prefix_line.contains("sha256"));
    }

    #[test]
    fn clean_run_syncs_only_at_run_seal() {
        reset_event_log_counts_for_tests();
        let directory = tempfile::tempdir().expect("创建日志测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let log = SimEventLogV2::create(
            &path,
            &opened(RunDurabilityRoleV2::CleanRun),
            MAX_GROUP_BYTES,
        )
        .expect("创建 CleanRun 日志");
        let (log, _) = log.append_group(&[input("a")]).expect("追加首组");
        let (log, _) = log.append_group(&[input("b")]).expect("追加次组");
        assert_eq!(event_log_counts_for_tests(), (3, 0));
        log.seal_clean_run().expect("封口 CleanRun");
        assert_eq!(event_log_counts_for_tests(), (4, 1));

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开封口日志");
        let (recovery, groups) = read_all_groups(recovery).expect("重放封口日志");
        assert_eq!(groups.len(), 2);
        recovery
            .finish_sealed_clean_run()
            .expect("完整 RunSeal 允许冷产物恢复");

        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("打开已封口日志")
            .write_all(b"partial")
            .expect("写入封口后断尾");
        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("再次打开封口日志");
        assert!(matches!(
            read_all_groups(recovery),
            Err(SimEventLogErrorV2::UnsealedCleanRun)
        ));
    }

    #[test]
    fn paper_recovery_truncates_only_non_newline_tail_and_continues_same_file() {
        let directory = tempfile::tempdir().expect("创建日志测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let log = SimEventLogV2::create(
            &path,
            &opened(RunDurabilityRoleV2::RecoverablePaper),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 日志");
        let (log, _) = log.append_group(&[input("a")]).expect("追加首组");
        let complete_len = log.byte_count();
        drop(log);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("打开断尾日志")
            .write_all(br#"{"kind":"ACCEPTED_INPUTS""#)
            .expect("写入不完整尾组");

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开断尾日志");
        let (recovery, groups) = read_all_groups(recovery).expect("读取完整前缀");
        assert_eq!(groups.len(), 1);
        let resumed = recovery.resume_paper().expect("截尾并续写同一日志");
        assert_eq!(resumed.byte_count(), complete_len);
        let (resumed, range) = resumed.append_group(&[input("b")]).expect("续写下一组");
        assert_eq!(range, 2..3);
        drop(resumed);

        let (_, recovered_again) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("再次打开日志");
        let (_, groups) = read_all_groups(recovered_again).expect("重放续写日志");
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn newline_terminated_bad_json_is_fatal_and_not_truncated() {
        let directory = tempfile::tempdir().expect("创建日志测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let log = SimEventLogV2::create(
            &path,
            &opened(RunDurabilityRoleV2::RecoverablePaper),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 日志");
        let (log, _) = log.append_group(&[input("a")]).expect("追加首组");
        drop(log);
        let before = std::fs::metadata(&path).expect("读取日志长度").len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("打开损坏日志")
            .write_all(b"{bad-json}\n")
            .expect("写入完整损坏行");
        let damaged = std::fs::metadata(&path).expect("读取损坏长度").len();
        assert!(damaged > before);

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开损坏日志");
        let (recovery, first) = recovery.next_group().expect("读取首个正常组");
        assert!(first.is_some());
        assert!(matches!(
            recovery.next_group(),
            Err(SimEventLogErrorV2::CompleteLineDecode { .. })
        ));
        assert_eq!(
            std::fs::metadata(&path).expect("复核损坏长度").len(),
            damaged
        );
    }

    #[test]
    fn background_acceptance_precedes_encoding_and_explicit_finish_seals_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("background.jsonl");
        let log = SimEventLogV2::create(
            &path,
            &opened(RunDurabilityRoleV2::CleanRun),
            MAX_GROUP_BYTES,
        )
        .unwrap();
        let initial = std::fs::read(&path).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let mut first = true;
        let mut background = BackgroundEventLogV1::start_with_hook(log, 2, move || {
            if first {
                first = false;
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
        });
        background.enqueue(vec![input("a")]).unwrap();
        entered_rx.recv().unwrap();
        background.enqueue(vec![input("b")]).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            initial,
            "后台尚未编码时前台已接受两组"
        );
        release_tx.send(()).unwrap();
        background.finish().unwrap();
        let (_, recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
        let (_, groups) = read_all_groups(recovery).unwrap();
        assert_eq!(groups.len(), 2);
        assert!(
            matches!(&groups[0][0], LoggedBusinessInputV2::ActivateSession { activation_id } if activation_id == "activation-a")
        );
        assert!(
            matches!(&groups[1][0], LoggedBusinessInputV2::ActivateSession { activation_id } if activation_id == "activation-b")
        );
    }

    #[test]
    fn planned_stop_transfers_pending_group_and_preserves_write_error_without_seal() {
        for fail in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("stop.jsonl");
            let mut log = SimEventLogV2::create(
                &path,
                &opened(RunDurabilityRoleV2::CleanRun),
                MAX_GROUP_BYTES,
            )
            .unwrap();
            if fail {
                // 实际只读文件描述符使原 write_all 返回操作系统写入错误。
                log.file = OpenOptions::new().read(true).open(&path).unwrap();
            }
            let mut background = BackgroundEventLogV1::start(log, 1);
            let release = background.pause_for_tests(false);
            background.enqueue(vec![input("a")]).unwrap();
            background.enqueue(vec![input("b")]).unwrap();
            assert!(background.pending.is_some());
            let stopper = std::thread::spawn(move || background.stop_and_drain());
            release.send(()).unwrap();
            let result = stopper.join().unwrap();
            if fail {
                assert!(matches!(result, Err(SimEventLogErrorV2::Io { .. })));
            } else {
                result.unwrap();
                let (_, reader) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
                let (reader, first) = reader.next_group().unwrap();
                let (reader, second) = reader.next_group().unwrap();
                assert!(
                    matches!(&first.unwrap()[0], LoggedBusinessInputV2::ActivateSession { activation_id } if activation_id == "activation-a")
                );
                assert!(
                    matches!(&second.unwrap()[0], LoggedBusinessInputV2::ActivateSession { activation_id } if activation_id == "activation-b")
                );
                let (reader, end) = reader.next_group().unwrap();
                assert!(end.is_none());
                assert!(matches!(
                    reader.finish_sealed_clean_run(),
                    Err(SimEventLogErrorV2::UnsealedCleanRun)
                ));
            }
            assert!(!std::fs::read_to_string(path).unwrap().contains("RunSeal"));
        }
    }

    #[test]
    fn background_disconnect_and_write_failure_never_seal_normal_end() {
        let directory = tempfile::tempdir().unwrap();
        for fail_write in [false, true] {
            let path = directory
                .path()
                .join(format!("incomplete-{fail_write}.jsonl"));
            let mut log = SimEventLogV2::create(
                &path,
                &opened(RunDurabilityRoleV2::CleanRun),
                MAX_GROUP_BYTES,
            )
            .unwrap();
            if fail_write {
                log.inject_write_failure_for_tests();
            }
            let mut background = BackgroundEventLogV1::start(log, 1);
            background.enqueue(vec![input("accepted")]).unwrap();
            if fail_write {
                assert!(matches!(
                    background.finish(),
                    Err(SimEventLogErrorV2::Io { .. })
                ));
            } else {
                let BackgroundEventLogV1 { sender, worker, .. } = background;
                drop(sender);
                assert!(matches!(
                    worker.join().unwrap(),
                    Err(SimEventLogErrorV2::UnsealedCleanRun)
                ));
            }
            let wire = std::fs::read_to_string(&path).unwrap();
            assert!(!wire.contains("RUN_SEAL"));
            let (_, recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
            let (recovery, groups) = read_all_groups(recovery).unwrap();
            assert_eq!(groups.len(), usize::from(!fail_write));
            assert!(matches!(
                recovery.finish_sealed_clean_run(),
                Err(SimEventLogErrorV2::UnsealedCleanRun)
            ));
        }
    }

    #[test]
    fn injected_write_or_sync_failure_consumes_the_writer_result() {
        let directory = tempfile::tempdir().expect("创建日志测试目录");
        let write_path = directory.path().join("write-failure.jsonl");
        let mut write_log = SimEventLogV2::create(
            &write_path,
            &opened(RunDurabilityRoleV2::RecoverablePaper),
            MAX_GROUP_BYTES,
        )
        .expect("创建写入故障日志");
        write_log.inject_write_failure_for_tests();
        assert!(write_log.append_group(&[input("write")]).is_err());

        let sync_path = directory.path().join("sync-failure.jsonl");
        let mut sync_log = SimEventLogV2::create(
            &sync_path,
            &opened(RunDurabilityRoleV2::RecoverablePaper),
            MAX_GROUP_BYTES,
        )
        .expect("创建同步故障日志");
        sync_log.inject_sync_failure_for_tests();
        assert!(sync_log.append_group(&[input("sync")]).is_err());
    }
}

/// 同一日志的唯一后台所有者。容量由有限调用方显式提供；未交接批次仍由前台持有。
#[cfg(feature = "shared-execution")]
type LedgerIntakeV2 = std::sync::Arc<std::sync::Mutex<Vec<ledger::LedgerRecordMaterialV2>>>;
#[cfg(feature = "shared-execution")]
struct LedgerBatchV2 {
    root: std::path::PathBuf,
    materials: Vec<ledger::LedgerRecordMaterialV2>,
}
#[cfg(feature = "shared-execution")]
#[derive(Default)]
struct BackgroundLedgerV2 {
    writer: Option<ledger::LedgerBackgroundWriterV2>,
    pending: std::collections::VecDeque<ledger::LedgerRecordMaterialV2>,
}
#[cfg(feature = "shared-execution")]
impl BackgroundLedgerV2 {
    fn accept(&mut self, batch: Option<LedgerBatchV2>) -> Result<(), SimEventLogErrorV2> {
        if let Some(batch) = batch {
            self.writer
                .get_or_insert_with(|| ledger::LedgerBackgroundWriterV2::new(batch.root));
            self.pending.extend(batch.materials);
        }
        if let Some(writer) = &mut self.writer {
            while let Some(material) = self.pending.front() {
                if !writer.write(material).map_err(ledger_background_error)? {
                    break;
                }
                self.pending.pop_front();
            }
            writer
                .flush_accepted_batch()
                .map_err(ledger_background_error)?;
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<(), SimEventLogErrorV2> {
        self.accept(None)?;
        if !self.pending.is_empty() {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "Ledger 来源尚未稳定，本轮未完成",
            ));
        }
        if let Some(writer) = &mut self.writer {
            writer.finish().map_err(ledger_background_error)?;
        }
        Ok(())
    }
}
#[cfg(feature = "shared-execution")]
fn ledger_background_error(error: ledger::LedgerDurableErrorV1) -> SimEventLogErrorV2 {
    SimEventLogErrorV2::Ledger(error)
}
struct BackgroundGroupV1 {
    inputs: Vec<BusinessInputV2>,
    cancel_scenario_hits: Vec<CancelScenarioHitV1>,
    noe: Vec<crate::run_v2::noe_projection::AcceptedNoeProjectionV1>,
    normalization_error: Option<crate::run_v2::MemoryNoeErrorV1>,
    #[cfg(feature = "shared-execution")]
    ledger: Option<LedgerBatchV2>,
}
struct BackgroundFinishV1 {
    seal_clean_run: bool,
    #[cfg(feature = "shared-execution")]
    ledger: Option<LedgerBatchV2>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct BackgroundCheckpointV1 {
    pub log: SimLogCheckpointV1,
    pub noe: Option<crate::run_v2::noe_projection::BackgroundNoeCheckpointV1>,
    #[cfg(feature = "shared-execution")]
    pub ledger: Option<ledger::LedgerBackgroundCheckpointV1>,
}
enum BackgroundLogCommandV1 {
    Checkpoint(
        BackgroundFinishV1,
        std::sync::mpsc::SyncSender<Result<BackgroundCheckpointV1, SimEventLogErrorV2>>,
    ),

    Group(BackgroundGroupV1),
    Finish(BackgroundFinishV1),
    Stabilize(
        BackgroundFinishV1,
        std::sync::mpsc::SyncSender<Result<(), SimEventLogErrorV2>>,
    ),
    #[cfg(test)]
    Pause(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    ),
    #[cfg(test)]
    PauseWrite(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    ),
    #[cfg(test)]
    FailNextWrite,
}
pub(crate) struct BackgroundEventLogV1 {
    sender: std::sync::mpsc::SyncSender<BackgroundLogCommandV1>,
    pending: Option<BackgroundGroupV1>,
    // 有限宿主在每次尚未接纳的提交前等原交接，不重试复合业务操作。
    finite_recording_wait: bool,
    #[cfg(test)]
    finite_wait_notice: Option<std::sync::mpsc::SyncSender<()>>,
    #[cfg(feature = "shared-execution")]
    ledger: Option<(std::path::PathBuf, LedgerIntakeV2)>,
    failure: std::sync::Arc<std::sync::Mutex<Option<SimEventLogErrorV2>>>,
    worker: std::thread::JoinHandle<Result<(), SimEventLogErrorV2>>,
}
impl BackgroundEventLogV1 {
    pub(crate) fn start(log: SimEventLogV2, capacity: usize) -> Self {
        Self::start_with_hook(log, capacity, || {})
    }
    fn start_with_hook(
        log: SimEventLogV2,
        capacity: usize,
        before_encode: impl FnMut() + Send + 'static,
    ) -> Self {
        Self::start_with_state(
            log,
            capacity,
            None,
            #[cfg(feature = "shared-execution")]
            BackgroundLedgerV2::default(),
            before_encode,
        )
    }
    pub(crate) fn restore(
        log: SimEventLogV2,
        capacity: usize,
        noe: Option<crate::run_v2::noe_projection::BackgroundNoeWriterV1>,
        #[cfg(feature = "shared-execution")] saved_ledger: Option<
            ledger::LedgerBackgroundCheckpointV1,
        >,
    ) -> Result<Self, SimEventLogErrorV2> {
        #[cfg(feature = "shared-execution")]
        let ledger = BackgroundLedgerV2 {
            writer: saved_ledger
                .map(ledger::LedgerBackgroundWriterV2::restore_checkpoint)
                .transpose()
                .map_err(ledger_background_error)?,
            pending: Default::default(),
        };
        Ok(Self::start_with_state(
            log,
            capacity,
            noe,
            #[cfg(feature = "shared-execution")]
            ledger,
            || {},
        ))
    }
    fn start_with_state(
        log: SimEventLogV2,
        capacity: usize,
        initial_noe: Option<crate::run_v2::noe_projection::BackgroundNoeWriterV1>,
        #[cfg(feature = "shared-execution")] initial_ledger: BackgroundLedgerV2,
        mut before_encode: impl FnMut() + Send + 'static,
    ) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<BackgroundLogCommandV1>(capacity);
        let failure = std::sync::Arc::new(std::sync::Mutex::new(None));
        let report = failure.clone();
        let worker = std::thread::spawn(move || {
            let result = (|| {
                let mut log = log;
                let mut noe = initial_noe;
                #[cfg(feature = "shared-execution")]
                let mut ledger = initial_ledger;
                #[cfg(test)]
                let mut write_pause: Option<(
                    std::sync::mpsc::SyncSender<()>,
                    std::sync::mpsc::Receiver<()>,
                )> = None;
                loop {
                    match receiver.recv() {
                        Ok(BackgroundLogCommandV1::Group(group)) => {
                            let mut inputs = group.inputs;
                            before_encode();
                            #[cfg(test)]
                            if let Some((entered, release)) = write_pause.take() {
                                entered.send(()).unwrap();
                                release.recv().unwrap();
                            }
                            if matches!(log.layout, SimLogLayoutV1::JsonV2) {
                                for input in &mut inputs {
                                    if let BusinessInputV2::PublicGroup(group) = input {
                                        group.materialize()?;
                                    }
                                }
                            }
                            log = log
                                .append_group_with_sync(
                                    &inputs,
                                    false,
                                    &group.cancel_scenario_hits,
                                )?
                                .0;
                            for batch in group.noe {
                                crate::run_v2::noe_projection::BackgroundNoeWriterV1::stage(
                                    &mut noe, batch,
                                )
                                .map_err(SimEventLogErrorV2::Noe)?;
                            }
                            #[cfg(feature = "shared-execution")]
                            {
                                ledger.accept(group.ledger)?;
                                if ledger.pending.front().is_some_and(
                                    ledger::LedgerRecordMaterialV2::waiting_for_noe_stability,
                                ) {
                                    #[cfg(feature = "measurement")]
                                    log.sync_data(MeasurementContext::CommitGroup)?;
                                    #[cfg(not(feature = "measurement"))]
                                    log.sync_data()?;
                                    if let Some(noe) = &mut noe {
                                        noe.stabilize().map_err(SimEventLogErrorV2::Noe)?;
                                    }
                                    ledger.accept(None)?;
                                }
                            }
                            if let Some(error) = group.normalization_error {
                                return Err(SimEventLogErrorV2::NoeNormalization(error));
                            }
                        }
                        Ok(BackgroundLogCommandV1::Checkpoint(_finish, acknowledged)) => {
                            let result = (|| {
                                #[cfg(feature = "measurement")]
                                log.sync_data(MeasurementContext::CommitGroup)?;
                                #[cfg(not(feature = "measurement"))]
                                log.sync_data()?;
                                if let Some(noe) = &mut noe {
                                    noe.stabilize().map_err(SimEventLogErrorV2::Noe)?;
                                }
                                #[cfg(feature = "shared-execution")]
                                {
                                    ledger.accept(_finish.ledger)?;
                                    ledger.finish()?;
                                }
                                Ok(BackgroundCheckpointV1 {
                                    log: log.checkpoint(),
                                    noe: noe.as_ref().map(|n| n.checkpoint()),
                                    #[cfg(feature = "shared-execution")]
                                    ledger: ledger.writer.as_ref().map(|w| w.checkpoint()),
                                })
                            })();
                            let _ = acknowledged.send(result.clone());
                            result?;
                        }
                        Ok(BackgroundLogCommandV1::Stabilize(_finish, acknowledged)) => {
                            let result = (|| {
                                #[cfg(feature = "measurement")]
                                log.sync_data(MeasurementContext::CommitGroup)?;
                                #[cfg(not(feature = "measurement"))]
                                log.sync_data()?;
                                if let Some(noe) = &mut noe {
                                    noe.stabilize().map_err(SimEventLogErrorV2::Noe)?;
                                }
                                #[cfg(feature = "shared-execution")]
                                {
                                    ledger.accept(_finish.ledger)?;
                                    ledger.finish()?;
                                }
                                Ok(())
                            })();
                            let _ = acknowledged.send(result.clone());
                            result?;
                        }
                        Ok(BackgroundLogCommandV1::Finish(_finish)) => {
                            #[cfg(feature = "measurement")]
                            log.sync_data(if _finish.seal_clean_run {
                                MeasurementContext::CleanSeal
                            } else {
                                MeasurementContext::CommitGroup
                            })?;
                            #[cfg(not(feature = "measurement"))]
                            log.sync_data()?;
                            if let Some(noe) = &mut noe {
                                noe.stabilize().map_err(SimEventLogErrorV2::Noe)?;
                            }
                            #[cfg(feature = "shared-execution")]
                            {
                                ledger.accept(_finish.ledger)?;
                                ledger.finish()?;
                            }
                            return if _finish.seal_clean_run {
                                log.seal_clean_run()
                            } else {
                                Ok(())
                            };
                        }
                        #[cfg(test)]
                        Ok(BackgroundLogCommandV1::Pause(entered, release)) => {
                            entered.send(()).unwrap();
                            release.recv().unwrap();
                        }
                        #[cfg(test)]
                        Ok(BackgroundLogCommandV1::PauseWrite(entered, release)) => {
                            write_pause = Some((entered, release));
                        }
                        #[cfg(test)]
                        Ok(BackgroundLogCommandV1::FailNextWrite) => {
                            log.inject_write_failure_for_tests()
                        }
                        Err(_) => {
                            return if log.durability_role == RunDurabilityRoleV2::FiniteHistorical {
                                // 退出仅丢弃通道，不稳定尾部、不补 RunSeal。
                                Ok(())
                            } else {
                                Err(SimEventLogErrorV2::UnsealedCleanRun)
                            };
                        }
                    }
                }
            })();
            if let Err(error) = &result {
                *report.lock().unwrap() = Some(error.clone());
            }
            // 先发布原错误，再释放唯一接收端，唤醒等待交接的有限供料方。
            drop(receiver);
            result
        });
        Self {
            sender,
            pending: None,
            finite_recording_wait: false,
            #[cfg(test)]
            finite_wait_notice: None,
            #[cfg(feature = "shared-execution")]
            ledger: None,
            failure,
            worker,
        }
    }
    #[cfg(feature = "shared-execution")]
    pub(crate) fn bind_ledger(&mut self, writer: &mut ledger::LedgerWriterV1, root: &Path) {
        let intake = LedgerIntakeV2::default();
        let sink = intake.clone();
        writer.bind_record_sink_v2(Box::new(move |material| {
            sink.lock().unwrap().push(material)
        }));
        self.ledger = Some((root.to_owned(), intake));
    }
    #[cfg(feature = "shared-execution")]
    pub(crate) fn reconnect_ledger(&mut self, writer: &mut ledger::LedgerWriterV1, root: &Path) {
        let intake = LedgerIntakeV2::default();
        let sink = intake.clone();
        writer.reconnect_record_sink_v2(Box::new(move |material| {
            sink.lock().unwrap().push(material)
        }));
        self.ledger = Some((root.to_owned(), intake));
    }
    pub(crate) fn checkpoint(&mut self) -> Result<BackgroundCheckpointV1, SimEventLogErrorV2> {
        self.wait_for_capacity()?;
        let (acknowledged, completed) = std::sync::mpsc::sync_channel(1);
        let finish = BackgroundFinishV1 {
            seal_clean_run: false,
            #[cfg(feature = "shared-execution")]
            ledger: self.take_ledger(),
        };
        self.sender
            .send(BackgroundLogCommandV1::Checkpoint(finish, acknowledged))
            .map_err(|_| SimEventLogErrorV2::InvalidStructure("检查点后台已停止"))?;
        completed
            .recv()
            .map_err(|_| SimEventLogErrorV2::InvalidStructure("检查点后台未返回稳定位置"))?
    }
    #[cfg(feature = "shared-execution")]
    fn take_ledger(&mut self) -> Option<LedgerBatchV2> {
        self.ledger.as_ref().map(|(root, intake)| LedgerBatchV2 {
            root: root.clone(),
            materials: std::mem::take(&mut *intake.lock().unwrap()),
        })
    }
    #[cfg(test)]
    pub(crate) fn pause_for_tests(&self, fail_next_write: bool) -> std::sync::mpsc::SyncSender<()> {
        if fail_next_write {
            self.sender
                .send(BackgroundLogCommandV1::FailNextWrite)
                .unwrap();
        }
        let (entered, waiting) = std::sync::mpsc::sync_channel(1);
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        self.sender
            .send(BackgroundLogCommandV1::Pause(entered, blocked))
            .unwrap();
        waiting.recv().unwrap();
        release
    }
    #[cfg(test)]
    pub(crate) fn pause_write_for_tests(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.sender
            .send(BackgroundLogCommandV1::FailNextWrite)
            .unwrap();
        let (entered, waiting) = std::sync::mpsc::sync_channel(1);
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        self.sender
            .send(BackgroundLogCommandV1::PauseWrite(entered, blocked))
            .unwrap();
        (waiting, release)
    }
    #[cfg(test)]
    pub(crate) fn abort_and_join_for_tests(self) -> Result<(), SimEventLogErrorV2> {
        drop(self.sender);
        self.worker.join().unwrap()
    }
    #[cfg(test)]
    pub(crate) fn notify_finite_wait_for_tests(&mut self, notice: std::sync::mpsc::SyncSender<()>) {
        self.finite_wait_notice = Some(notice);
    }
    pub(crate) fn enable_finite_recording_wait(&mut self) {
        self.finite_recording_wait = true;
    }
    pub(crate) fn ready(&mut self) -> Result<bool, SimEventLogErrorV2> {
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        if self.finite_recording_wait && self.pending.is_some() {
            #[cfg(test)]
            if let Some(notice) = self.finite_wait_notice.take() {
                let _ = notice.send(());
            }
            self.wait_for_capacity()?;
        } else if let Some(inputs) = self.pending.take() {
            self.enqueue_group(inputs)?;
        }
        Ok(self.pending.is_none())
    }
    /// 仅等待当前唯一待交接组进入原通道，不表示写入或耐久完成。
    pub(crate) fn wait_for_capacity(&mut self) -> Result<(), SimEventLogErrorV2> {
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        if let Some(inputs) = self.pending.take() {
            if let Err(std::sync::mpsc::SendError(BackgroundLogCommandV1::Group(inputs))) =
                self.sender.send(BackgroundLogCommandV1::Group(inputs))
            {
                self.pending = Some(inputs);
                return Err(self.failure.lock().unwrap().clone().unwrap_or(
                    SimEventLogErrorV2::InvalidStructure("后台日志已停止，本轮未完成"),
                ));
            }
        }
        self.failure.lock().unwrap().clone().map_or(Ok(()), Err)
    }
    /// 有限宿主显式稳定已有原材料；不推进业务时间、不停止运行或创建账户快照。
    pub(crate) fn stabilize(&mut self) -> Result<(), SimEventLogErrorV2> {
        self.wait_for_capacity()?;
        let (acknowledged, completed) = std::sync::mpsc::sync_channel(1);
        let finish = BackgroundFinishV1 {
            seal_clean_run: false,
            #[cfg(feature = "shared-execution")]
            ledger: self.take_ledger(),
        };
        self.sender
            .send(BackgroundLogCommandV1::Stabilize(finish, acknowledged))
            .map_err(|_| {
                self.failure
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(SimEventLogErrorV2::InvalidStructure("原记录后台已停止"))
            })?;
        completed.recv().map_err(|_| {
            self.failure
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(SimEventLogErrorV2::InvalidStructure("原记录后台未完成稳定"))
        })?
    }
    pub(crate) fn enqueue(
        &mut self,
        inputs: Vec<BusinessInputV2>,
    ) -> Result<(), SimEventLogErrorV2> {
        self.enqueue_with_noe(inputs, Vec::new(), None, Vec::new())
    }
    pub(crate) fn enqueue_with_noe(
        &mut self,
        inputs: Vec<BusinessInputV2>,
        noe: Vec<crate::run_v2::noe_projection::AcceptedNoeProjectionV1>,
        normalization_error: Option<crate::run_v2::MemoryNoeErrorV1>,
        cancel_scenario_hits: Vec<CancelScenarioHitV1>,
    ) -> Result<(), SimEventLogErrorV2> {
        #[cfg(feature = "shared-execution")]
        let ledger = self.take_ledger();
        self.enqueue_group(BackgroundGroupV1 {
            #[cfg(feature = "shared-execution")]
            ledger,
            inputs,
            cancel_scenario_hits,
            noe,
            normalization_error,
        })
    }
    fn enqueue_group(&mut self, inputs: BackgroundGroupV1) -> Result<(), SimEventLogErrorV2> {
        match self.sender.try_send(BackgroundLogCommandV1::Group(inputs)) {
            Ok(()) => Ok(()),
            Err(std::sync::mpsc::TrySendError::Full(BackgroundLogCommandV1::Group(inputs))) => {
                self.pending = Some(inputs);
                Ok(())
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(BackgroundLogCommandV1::Group(
                inputs,
            ))) => {
                self.pending = Some(inputs);
                Err(self.failure.lock().unwrap().clone().unwrap_or(
                    SimEventLogErrorV2::InvalidStructure("后台日志已停止，本轮未完成"),
                ))
            }
            Err(_) => unreachable!("只在此处移交原输入组"),
        }
    }
    pub(crate) fn finish(self) -> Result<(), SimEventLogErrorV2> {
        self.finish_recording(true)
    }
    pub(crate) fn stop_and_drain(self) -> Result<(), SimEventLogErrorV2> {
        self.finish_recording(false)
    }
    fn finish_recording(mut self, seal_clean_run: bool) -> Result<(), SimEventLogErrorV2> {
        #[cfg(feature = "shared-execution")]
        let materials = self.take_ledger();
        let sent = match self.pending.take() {
            Some(inputs) => self.sender.send(BackgroundLogCommandV1::Group(inputs)),
            None => Ok(()),
        };
        let sent = match sent {
            Ok(()) => self
                .sender
                .send(BackgroundLogCommandV1::Finish(BackgroundFinishV1 {
                    seal_clean_run,
                    #[cfg(feature = "shared-execution")]
                    ledger: materials,
                })),
            Err(error) => Err(error),
        };
        drop(self.sender);
        let result = self
            .worker
            .join()
            .map_err(|_| SimEventLogErrorV2::InvalidStructure("后台日志异常退出，本轮未完成"))?;
        result?;
        sent.map_err(|_| SimEventLogErrorV2::UnsealedCleanRun)
    }
}
