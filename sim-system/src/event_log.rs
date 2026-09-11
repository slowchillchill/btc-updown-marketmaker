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
use crate::runtime_v2::{BusinessInputV2, LoggedBusinessInputV2, RunDurabilityRoleV2, RunOpenedV2};

pub(crate) const SIM_EVENT_LOG_FILE_NAME_V2: &str = "sim-event-log-v2.jsonl";

pub(crate) fn delete_retained_run_log_v2(path: &Path) -> Result<(), SimEventLogErrorV2> {
    fs::remove_file(path).map_err(|error| io_error("删除已关闭运行的单一事件日志", error))
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
enum SimEventLogLineV2 {
    RunOpened(RunOpenedV2),
    AcceptedInputs(Vec<LoggedBusinessInputV2>),
    RunSeal,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
enum SimEventLogLineRefV2<'a> {
    RunOpened(&'a RunOpenedV2),
    AcceptedInputs(&'a [BusinessInputV2]),
    RunSeal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimEventLogErrorV2 {
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
            Self::Io { operation, message } => write!(formatter, "{operation}: {message}"),
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

pub(crate) struct SimEventLogV2 {
    file: File,
    durability_role: RunDurabilityRoleV2,
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

impl SimEventLogV2 {
    pub(crate) fn create(
        path: &Path,
        opened: &RunOpenedV2,
        max_group_bytes: usize,
    ) -> Result<Self, SimEventLogErrorV2> {
        #[cfg(feature = "measurement")]
        let measurement_probe = Arc::new(MeasurementProbe::default());
        #[cfg(feature = "measurement")]
        let encode_started = Instant::now();
        let encoded = encode_line(&SimEventLogLineRefV2::RunOpened(opened), max_group_bytes)?;
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
            durability_role: opened.durability_role,
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
        let sync = self.durability_role == RunDurabilityRoleV2::RecoverablePaper;
        self.append_group_with_sync(inputs, sync)
    }

    fn append_group_with_sync(
        mut self,
        inputs: &[BusinessInputV2],
        sync: bool,
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
        let encoded = encode_line(
            &SimEventLogLineRefV2::AcceptedInputs(inputs),
            self.max_group_bytes,
        )?;
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
    durability_role: RunDurabilityRoleV2,
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
        let mut buffer = Vec::new();
        let first = read_complete_line(&mut reader, &mut buffer, 0, max_group_bytes)?
            .ok_or(SimEventLogErrorV2::InvalidStructure("缺少完整 RunOpened"))?;
        let SimEventLogLineV2::RunOpened(opened) = first else {
            return Err(SimEventLogErrorV2::InvalidStructure(
                "第一行必须是 RunOpened",
            ));
        };
        let last_complete_offset = buffer.len() as u64;
        let recovery = Self {
            reader,
            durability_role: opened.durability_role,
            max_group_bytes,
            last_complete_offset,
            next_input_sequence: 1,
            incomplete_tail: false,
            saw_seal: false,
            reached_eof: false,
        };
        Ok((opened, recovery))
    }

    pub(crate) fn next_group(
        mut self,
    ) -> Result<(Self, Option<Vec<LoggedBusinessInputV2>>), SimEventLogErrorV2> {
        if self.reached_eof {
            return Ok((self, None));
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
                SimEventLogLineV2::AcceptedInputs(inputs) => {
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
            durability_role: self.durability_role,
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
            market_event_source_identity_sha256: sha('7'),
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
enum BackgroundLogCommandV1 {
    Group(Vec<BusinessInputV2>),
    Finish,
    #[cfg(test)]
    Pause(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    ),
    #[cfg(test)]
    FailNextWrite,
}
pub(crate) struct BackgroundEventLogV1 {
    sender: std::sync::mpsc::SyncSender<BackgroundLogCommandV1>,
    pending: Option<Vec<BusinessInputV2>>,
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
        mut before_encode: impl FnMut() + Send + 'static,
    ) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<BackgroundLogCommandV1>(capacity);
        let failure = std::sync::Arc::new(std::sync::Mutex::new(None));
        let report = failure.clone();
        let worker = std::thread::spawn(move || {
            let result = (|| {
                let mut log = log;
                loop {
                    match receiver.recv() {
                        Ok(BackgroundLogCommandV1::Group(mut inputs)) => {
                            before_encode();
                            for input in &mut inputs {
                                if let BusinessInputV2::PublicGroup(group) = input {
                                    group.materialize()?;
                                }
                            }
                            log = log.append_group_with_sync(&inputs, false)?.0;
                        }
                        Ok(BackgroundLogCommandV1::Finish) => return log.seal_clean_run(),
                        #[cfg(test)]
                        Ok(BackgroundLogCommandV1::Pause(entered, release)) => {
                            entered.send(()).unwrap();
                            release.recv().unwrap();
                        }
                        #[cfg(test)]
                        Ok(BackgroundLogCommandV1::FailNextWrite) => {
                            log.inject_write_failure_for_tests()
                        }
                        Err(_) => return Err(SimEventLogErrorV2::UnsealedCleanRun),
                    }
                }
            })();
            if let Err(error) = &result {
                *report.lock().unwrap() = Some(error.clone());
            }
            result
        });
        Self {
            sender,
            pending: None,
            failure,
            worker,
        }
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
    pub(crate) fn abort_and_join_for_tests(self) -> Result<(), SimEventLogErrorV2> {
        drop(self.sender);
        self.worker.join().unwrap()
    }
    pub(crate) fn ready(&mut self) -> Result<bool, SimEventLogErrorV2> {
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        if let Some(inputs) = self.pending.take() {
            self.enqueue(inputs)?;
        }
        Ok(self.pending.is_none())
    }
    pub(crate) fn enqueue(
        &mut self,
        inputs: Vec<BusinessInputV2>,
    ) -> Result<(), SimEventLogErrorV2> {
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
    pub(crate) fn finish(mut self) -> Result<(), SimEventLogErrorV2> {
        let sent = self
            .pending
            .take()
            .map_or(Ok(()), |inputs| {
                self.sender.send(BackgroundLogCommandV1::Group(inputs))
            })
            .and_then(|()| self.sender.send(BackgroundLogCommandV1::Finish));
        drop(self.sender);
        let result = self
            .worker
            .join()
            .map_err(|_| SimEventLogErrorV2::InvalidStructure("后台日志异常退出，本轮未完成"))?;
        result?;
        sent.map_err(|_| SimEventLogErrorV2::UnsealedCleanRun)
    }
}
