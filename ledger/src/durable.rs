use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::time::{Duration, Instant};

use rustix::fs::{RenameFlags, renameat_with};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::core::LedgerCommittedEffectV1;
use crate::projection::{AccountAdmissionPolicyV1, LedgerProjectionV1};
use crate::reservation::{CachedReservationRequestV1, LedgerRequestIdentityV1};

const ACTIVE_FILE_NAME: &str = "ledger-active-v1.jsonl";
const GENESIS_CANDIDATE_FILE_NAME: &str = ".ledger-active-v1.genesis-candidate";
const SEGMENT_PREFIX: &str = "ledger-sealed-v1--";
const SEGMENT_SUFFIX: &str = ".jsonl";
const RECORD_VERSION: u16 = 1;
const RECORD_ENCODING: &str = "ledger-jsonl-v1";
const PROJECTION_CANDIDATE_PREFIX: &str = ".ledger-projection-v1.candidate-";
const PROJECTION_PREFIX: &str = "ledger-projection-v1--";
const PROJECTION_SUFFIX: &str = ".json";
const PROJECTION_VERSION: u16 = 1;
const PROJECTION_ENCODING: &str = "ledger-projection-json-v1";
static PROJECTION_CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerDurableErrorV1 {
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
    Encode(String),
    CompleteRecordDecode {
        path: PathBuf,
        offset: u64,
        message: String,
    },
    InvalidStructure(String),
    SealedSegmentHashMismatch {
        path: PathBuf,
        expected: String,
        found: String,
    },
    ProjectionHashMismatch {
        path: PathBuf,
        expected: String,
        found: String,
    },
    DurabilityUncertain {
        operation: &'static str,
        message: String,
    },
    HandlePoisoned,
}

impl fmt::Display for LedgerDurableErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                message,
            } => write!(formatter, "{operation} {}：{message}", path.display()),
            Self::Encode(message) => write!(formatter, "Ledger 耐久记录编码失败：{message}"),
            Self::CompleteRecordDecode {
                path,
                offset,
                message,
            } => write!(
                formatter,
                "Ledger 完整耐久记录无法解码 {}@{offset}：{message}",
                path.display()
            ),
            Self::InvalidStructure(message) => write!(formatter, "Ledger 耐久结构非法：{message}"),
            Self::SealedSegmentHashMismatch {
                path,
                expected,
                found,
            } => write!(
                formatter,
                "Ledger 封存段 SHA-256 不匹配 {}：预期 {expected}，实际 {found}",
                path.display()
            ),
            Self::ProjectionHashMismatch {
                path,
                expected,
                found,
            } => write!(
                formatter,
                "Ledger 投影 SHA-256 不匹配 {}：预期 {expected}，实际 {found}",
                path.display()
            ),
            Self::DurabilityUncertain { operation, message } => {
                write!(formatter, "Ledger 耐久结果不确定（{operation}）：{message}")
            }
            Self::HandlePoisoned => write!(formatter, "Ledger 耐久句柄已不可继续使用"),
        }
    }
}

impl std::error::Error for LedgerDurableErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerSealedSegmentReferenceV1 {
    pub location: String,
    pub sha256: String,
    pub first_durable_record_seq: u64,
    pub last_durable_record_seq: u64,
    pub first_ledger_entry_seq: Option<u64>,
    pub last_ledger_entry_seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerProjectionReferenceV1 {
    pub location: String,
    pub sha256: String,
    pub ledger_entry_seq: u64,
}

#[derive(Deserialize)]
struct LedgerProjectionContainerV1 {
    version: u16,
    encoding: String,
    projection: LedgerProjectionV1,
}

#[derive(Serialize)]
struct LedgerProjectionContainerRefV1<'a> {
    version: u16,
    encoding: &'static str,
    projection: &'a LedgerProjectionV1,
}

pub fn publish_ledger_projection_v1(
    directory: &Path,
    projection: &LedgerProjectionV1,
) -> Result<LedgerProjectionReferenceV1, LedgerDurableErrorV1> {
    let encoded = serde_json::to_vec(&LedgerProjectionContainerRefV1 {
        version: PROJECTION_VERSION,
        encoding: PROJECTION_ENCODING,
        projection,
    })
    .map_err(|error| LedgerDurableErrorV1::Encode(error.to_string()))?;
    let sha256 = sha256_bytes(&encoded);
    let file_name = projection_file_name(projection.ledger_entry_seq, &sha256);
    let final_path = directory.join(&file_name);
    let (mut candidate, candidate_file_name, candidate_path) =
        create_projection_candidate(directory)?;
    if let Err(error) = candidate
        .write_all(&encoded)
        .and_then(|()| candidate.sync_all())
    {
        drop(candidate);
        return cleanup_projection_candidate(
            directory,
            &candidate_path,
            io_error("写入并同步 Ledger 投影候选", &candidate_path, error),
        );
    }
    drop(candidate);
    let parent = match File::open(directory) {
        Ok(parent) => parent,
        Err(error) => {
            return cleanup_projection_candidate(
                directory,
                &candidate_path,
                io_error("打开 Ledger 投影目录", directory, error),
            );
        }
    };
    if let Err(error) = renameat_with(
        &parent,
        &candidate_file_name,
        &parent,
        &file_name,
        RenameFlags::NOREPLACE,
    ) {
        return cleanup_projection_candidate(
            directory,
            &candidate_path,
            LedgerDurableErrorV1::Io {
                operation: "禁止覆盖地发布 Ledger 投影",
                path: final_path,
                message: error.to_string(),
            },
        );
    }
    sync_directory(directory, "同步 Ledger 投影发布").map_err(|error| {
        LedgerDurableErrorV1::DurabilityUncertain {
            operation: "发布 Ledger 投影",
            message: error.to_string(),
        }
    })?;
    Ok(LedgerProjectionReferenceV1 {
        location: directory.join(file_name).to_string_lossy().into_owned(),
        sha256,
        ledger_entry_seq: projection.ledger_entry_seq,
    })
}

pub fn read_ledger_projection_v1(
    reference: &LedgerProjectionReferenceV1,
) -> Result<LedgerProjectionV1, LedgerDurableErrorV1> {
    let path = Path::new(&reference.location);
    let expected_file_name = projection_file_name(reference.ledger_entry_seq, &reference.sha256);
    if path.file_name().and_then(|value| value.to_str()) != Some(expected_file_name.as_str()) {
        return Err(LedgerDurableErrorV1::InvalidStructure(format!(
            "Ledger 投影引用文件名与序号或 SHA-256 不一致：{}",
            path.display()
        )));
    }
    let encoded = fs::read(path).map_err(|error| io_error("读取 Ledger 投影", path, error))?;
    let found = sha256_bytes(&encoded);
    if found != reference.sha256 {
        return Err(LedgerDurableErrorV1::ProjectionHashMismatch {
            path: path.to_path_buf(),
            expected: reference.sha256.clone(),
            found,
        });
    }
    let container: LedgerProjectionContainerV1 =
        serde_json::from_slice(&encoded).map_err(|error| {
            LedgerDurableErrorV1::InvalidStructure(format!(
                "Ledger 投影容器 {} 无法解码：{error}",
                path.display()
            ))
        })?;
    if container.version != PROJECTION_VERSION || container.encoding != PROJECTION_ENCODING {
        return Err(LedgerDurableErrorV1::InvalidStructure(format!(
            "Ledger 投影容器 {} 的版本或编码不受支持",
            path.display()
        )));
    }
    if container.projection.ledger_entry_seq != reference.ledger_entry_seq {
        return Err(LedgerDurableErrorV1::InvalidStructure(format!(
            "Ledger 投影容器 {} 的序号与引用不一致",
            path.display()
        )));
    }
    Ok(container.projection)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DurablePayloadV1 {
    Genesis {
        policy: AccountAdmissionPolicyV1,
    },
    Transaction(Box<LedgerCommittedEffectV1>),
    ReservationDecision {
        request_identity: LedgerRequestIdentityV1,
        request: CachedReservationRequestV1,
    },
}

impl DurablePayloadV1 {
    fn ledger_entry_seq(&self) -> Option<u64> {
        match self {
            Self::Genesis { .. } => None,
            Self::Transaction(effect) => Some(effect.ledger_entry_seq()),
            Self::ReservationDecision { .. } => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DurableEnvelopeV1 {
    version: u16,
    encoding: String,
    durable_record_seq: u64,
    payload: DurablePayloadV1,
}

#[derive(Serialize)]
struct DurableEnvelopeRefV1<'a> {
    version: u16,
    encoding: &'static str,
    durable_record_seq: u64,
    payload: &'a DurablePayloadV1,
}

#[derive(Default)]
struct SegmentStatsV1 {
    first_record_seq: Option<u64>,
    last_record_seq: Option<u64>,
    first_ledger_entry_seq: Option<u64>,
    last_ledger_entry_seq: Option<u64>,
}

struct RecoveryCursorV1 {
    expected_record_seq: u64,
    expected_ledger_entry_seq: u64,
    previous_record_sha256: Option<[u8; 32]>,
}

impl Default for RecoveryCursorV1 {
    fn default() -> Self {
        Self {
            expected_record_seq: 1,
            expected_ledger_entry_seq: 1,
            previous_record_sha256: None,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RecoveryDurationsV1 {
    pub(crate) sealed_replay: Duration,
    pub(crate) active_replay: Duration,
}

impl SegmentStatsV1 {
    fn observe(&mut self, record_seq: u64, ledger_entry_seq: Option<u64>) {
        self.first_record_seq.get_or_insert(record_seq);
        self.last_record_seq = Some(record_seq);
        if let Some(ledger_entry_seq) = ledger_entry_seq {
            self.first_ledger_entry_seq.get_or_insert(ledger_entry_seq);
            self.last_ledger_entry_seq = Some(ledger_entry_seq);
        }
    }
}

pub(crate) struct DurableLedgerV1 {
    directory: PathBuf,
    active_path: PathBuf,
    active: File,
    byte_count: u64,
    next_record_seq: u64,
    segment_stats: SegmentStatsV1,
    poisoned: bool,
    #[cfg(test)]
    fail_next_append: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

impl DurableLedgerV1 {
    pub(crate) fn create(
        directory: &Path,
        policy: &AccountAdmissionPolicyV1,
    ) -> Result<Self, LedgerDurableErrorV1> {
        Self::create_inner(directory, policy, false)
    }

    fn create_inner(
        directory: &Path,
        policy: &AccountAdmissionPolicyV1,
        fail_after_publish: bool,
    ) -> Result<Self, LedgerDurableErrorV1> {
        match fs::create_dir(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("创建 Ledger 耐久目录", directory, error)),
        }
        let parent_directory = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent_directory, "同步 Ledger 耐久根目录项")?;
        for entry in fs::read_dir(directory)
            .map_err(|error| io_error("读取 Ledger 耐久目录", directory, error))?
        {
            let entry =
                entry.map_err(|error| io_error("读取 Ledger 耐久目录项", directory, error))?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if file_name.starts_with(SEGMENT_PREFIX) && file_name.ends_with(SEGMENT_SUFFIX) {
                return Err(LedgerDurableErrorV1::InvalidStructure(
                    "已有封存段的 Ledger 目录必须使用 open_durable".to_owned(),
                ));
            }
        }
        let active_path = directory.join(ACTIVE_FILE_NAME);
        let candidate_path = directory.join(GENESIS_CANDIDATE_FILE_NAME);
        match fs::remove_file(&candidate_path) {
            Ok(()) => {
                if let Err(error) = sync_directory_io(directory) {
                    return Err(LedgerDurableErrorV1::DurabilityUncertain {
                        operation: "同步清理 Ledger genesis 候选",
                        message: error.to_string(),
                    });
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(LedgerDurableErrorV1::DurabilityUncertain {
                    operation: "清理 Ledger genesis 候选",
                    message: error.to_string(),
                });
            }
        }
        let genesis = DurablePayloadV1::Genesis {
            policy: policy.clone(),
        };
        let encoded = encode_record(1, &genesis)?;
        let mut candidate = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&candidate_path)
            .map_err(|error| io_error("创建 Ledger genesis 候选", &candidate_path, error))?;
        if let Err(error) = candidate
            .write_all(&encoded)
            .and_then(|()| candidate.sync_all())
        {
            drop(candidate);
            return cleanup_genesis_candidate(
                directory,
                &candidate_path,
                io_error("写入并同步 Ledger genesis 候选", &candidate_path, error),
            );
        }
        drop(candidate);
        let parent = match File::open(directory) {
            Ok(parent) => parent,
            Err(error) => {
                return cleanup_genesis_candidate(
                    directory,
                    &candidate_path,
                    io_error("打开 Ledger 耐久目录", directory, error),
                );
            }
        };
        if let Err(error) = renameat_with(
            &parent,
            GENESIS_CANDIDATE_FILE_NAME,
            &parent,
            ACTIVE_FILE_NAME,
            RenameFlags::NOREPLACE,
        ) {
            return cleanup_genesis_candidate(
                directory,
                &candidate_path,
                LedgerDurableErrorV1::Io {
                    operation: "禁止覆盖地发布 Ledger genesis",
                    path: active_path,
                    message: error.to_string(),
                },
            );
        }
        if fail_after_publish {
            return Err(LedgerDurableErrorV1::DurabilityUncertain {
                operation: "测试注入 Ledger genesis 发布后故障",
                message: "完整 genesis 已原子发布，目录同步结果未知".to_owned(),
            });
        }
        sync_directory(directory, "同步 Ledger genesis 发布").map_err(|error| {
            LedgerDurableErrorV1::DurabilityUncertain {
                operation: "发布 Ledger genesis",
                message: error.to_string(),
            }
        })?;
        let mut active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)
            .map_err(|error| LedgerDurableErrorV1::DurabilityUncertain {
                operation: "打开已发布的 Ledger genesis",
                message: error.to_string(),
            })?;
        active.seek(SeekFrom::End(0)).map_err(|error| {
            LedgerDurableErrorV1::DurabilityUncertain {
                operation: "定位已发布的 Ledger genesis",
                message: error.to_string(),
            }
        })?;
        let mut stats = SegmentStatsV1::default();
        stats.observe(1, None);
        Ok(Self::from_active(
            directory,
            active_path,
            active,
            encoded.len() as u64,
            2,
            stats,
        ))
    }

    pub(crate) fn open<F>(directory: &Path, mut apply: F) -> Result<Self, LedgerDurableErrorV1>
    where
        F: FnMut(DurablePayloadV1) -> Result<(), LedgerDurableErrorV1>,
    {
        let mut cursor = RecoveryCursorV1::default();
        recover_sealed_segments(directory, &mut cursor, &mut apply)?;
        recover_active_segment(directory, &mut cursor, &mut apply)
    }

    #[cfg(test)]
    pub(crate) fn open_measured<F>(
        directory: &Path,
        mut apply: F,
    ) -> Result<(Self, RecoveryDurationsV1), LedgerDurableErrorV1>
    where
        F: FnMut(DurablePayloadV1) -> Result<(), LedgerDurableErrorV1>,
    {
        let mut cursor = RecoveryCursorV1::default();
        let sealed_started = Instant::now();
        recover_sealed_segments(directory, &mut cursor, &mut apply)?;
        let sealed_replay = sealed_started.elapsed();
        let active_started = Instant::now();
        let ledger = recover_active_segment(directory, &mut cursor, &mut apply)?;
        Ok((
            ledger,
            RecoveryDurationsV1 {
                sealed_replay,
                active_replay: active_started.elapsed(),
            },
        ))
    }

    #[cfg(test)]
    fn create_with_publish_failure(
        directory: &Path,
        policy: &AccountAdmissionPolicyV1,
    ) -> Result<Self, LedgerDurableErrorV1> {
        Self::create_inner(directory, policy, true)
    }

    fn from_active(
        directory: &Path,
        active_path: PathBuf,
        active: File,
        byte_count: u64,
        next_record_seq: u64,
        segment_stats: SegmentStatsV1,
    ) -> Self {
        Self {
            directory: directory.to_path_buf(),
            active_path,
            active,
            byte_count,
            next_record_seq,
            segment_stats,
            poisoned: false,
            #[cfg(test)]
            fail_next_append: false,
            #[cfg(test)]
            fail_next_sync: false,
        }
    }

    pub(crate) fn append(
        &mut self,
        payload: &DurablePayloadV1,
    ) -> Result<(), LedgerDurableErrorV1> {
        if self.poisoned {
            return Err(LedgerDurableErrorV1::HandlePoisoned);
        }
        let encoded = encode_record(self.next_record_seq, payload)?;
        let previous_len = self.byte_count;
        let next_byte_count = previous_len
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| {
                LedgerDurableErrorV1::InvalidStructure("Ledger 活动段字节位置溢出".to_owned())
            })?;
        let next_record_seq = self
            .next_record_seq
            .checked_add(1)
            .ok_or_else(|| LedgerDurableErrorV1::InvalidStructure("耐久记录序号溢出".to_owned()))?;

        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_append) {
            return Err(io_error(
                "追加 Ledger 活动段",
                &self.active_path,
                std::io::Error::other("测试注入写入失败"),
            ));
        }

        let write_result = self.active.write_all(&encoded).and_then(|()| {
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_sync) {
                return Err(std::io::Error::other("测试注入同步失败"));
            }
            self.active.sync_data()
        });
        if let Err(error) = write_result {
            let rollback = self
                .active
                .set_len(previous_len)
                .and_then(|()| self.active.seek(SeekFrom::Start(previous_len)).map(|_| ()))
                .and_then(|()| self.active.sync_data());
            if let Err(rollback_error) = rollback {
                self.poisoned = true;
                return Err(LedgerDurableErrorV1::DurabilityUncertain {
                    operation: "回滚失败的 Ledger 活动段追加",
                    message: format!("原始错误：{error}；回滚错误：{rollback_error}"),
                });
            }
            return Err(io_error(
                "追加并同步 Ledger 活动段",
                &self.active_path,
                error,
            ));
        }

        self.byte_count = next_byte_count;
        self.segment_stats
            .observe(self.next_record_seq, payload.ledger_entry_seq());
        self.next_record_seq = next_record_seq;
        Ok(())
    }

    pub(crate) fn seal_active_segment(
        &mut self,
    ) -> Result<Option<LedgerSealedSegmentReferenceV1>, LedgerDurableErrorV1> {
        if self.poisoned {
            return Err(LedgerDurableErrorV1::HandlePoisoned);
        }
        let Some(first_record_seq) = self.segment_stats.first_record_seq else {
            return Ok(None);
        };
        self.active
            .sync_all()
            .map_err(|error| io_error("封存前同步 Ledger 活动段", &self.active_path, error))?;
        let sha256 = sha256_file(&self.active_path)?;
        let last_record_seq = self
            .segment_stats
            .last_record_seq
            .expect("非空活动段拥有末记录序号");
        let first_ledger_entry_seq = self.segment_stats.first_ledger_entry_seq;
        let last_ledger_entry_seq = self.segment_stats.last_ledger_entry_seq;
        let file_name = segment_file_name(
            first_record_seq,
            last_record_seq,
            first_ledger_entry_seq,
            last_ledger_entry_seq,
            &sha256,
        );
        let parent = File::open(&self.directory)
            .map_err(|error| io_error("打开 Ledger 耐久目录", &self.directory, error))?;
        if let Err(error) = renameat_with(
            &parent,
            ACTIVE_FILE_NAME,
            &parent,
            &file_name,
            RenameFlags::NOREPLACE,
        ) {
            return Err(LedgerDurableErrorV1::Io {
                operation: "禁止覆盖地封存 Ledger 活动段",
                path: self.directory.join(&file_name),
                message: error.to_string(),
            });
        }
        if let Err(error) = sync_directory(&self.directory, "同步 Ledger 封存段发布") {
            self.poisoned = true;
            return Err(LedgerDurableErrorV1::DurabilityUncertain {
                operation: "发布 Ledger 封存段",
                message: error.to_string(),
            });
        }
        let new_active = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&self.active_path)
        {
            Ok(file) => file,
            Err(error) => {
                self.poisoned = true;
                return Err(LedgerDurableErrorV1::DurabilityUncertain {
                    operation: "封存后创建 Ledger 活动段",
                    message: error.to_string(),
                });
            }
        };
        if let Err(error) = new_active
            .sync_all()
            .and_then(|()| sync_directory_io(&self.directory))
        {
            self.poisoned = true;
            return Err(LedgerDurableErrorV1::DurabilityUncertain {
                operation: "同步封存后的 Ledger 活动段",
                message: error.to_string(),
            });
        }
        self.active = new_active;
        self.byte_count = 0;
        self.segment_stats = SegmentStatsV1::default();
        Ok(Some(LedgerSealedSegmentReferenceV1 {
            location: self
                .directory
                .join(file_name)
                .to_string_lossy()
                .into_owned(),
            sha256,
            first_durable_record_seq: first_record_seq,
            last_durable_record_seq: last_record_seq,
            first_ledger_entry_seq,
            last_ledger_entry_seq,
        }))
    }

    #[cfg(test)]
    pub(crate) fn inject_append_failure(&mut self) {
        self.fail_next_append = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_sync_failure(&mut self) {
        self.fail_next_sync = true;
    }
}

fn recover_sealed_segments(
    directory: &Path,
    cursor: &mut RecoveryCursorV1,
    apply: &mut impl FnMut(DurablePayloadV1) -> Result<(), LedgerDurableErrorV1>,
) -> Result<(), LedgerDurableErrorV1> {
    let mut sealed = Vec::new();
    for entry in fs::read_dir(directory)
        .map_err(|error| io_error("读取 Ledger 耐久目录", directory, error))?
    {
        let entry = entry.map_err(|error| io_error("读取 Ledger 段目录项", directory, error))?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with(SEGMENT_PREFIX) && file_name.ends_with(SEGMENT_SUFFIX) {
            sealed.push(parse_segment_reference(&entry.path(), &file_name)?);
        }
    }
    sealed.sort_by_key(|reference| reference.first_durable_record_seq);

    for reference in &sealed {
        let sealed_path = Path::new(&reference.location);
        let found_hash = sha256_file(sealed_path)?;
        if found_hash != reference.sha256 {
            return Err(LedgerDurableErrorV1::SealedSegmentHashMismatch {
                path: sealed_path.to_path_buf(),
                expected: reference.sha256.clone(),
                found: found_hash,
            });
        }
        let sealed_file = File::open(sealed_path)
            .map_err(|error| io_error("打开 Ledger 封存段", sealed_path, error))?;
        let (stats, _) = decode_segment(
            sealed_path,
            BufReader::new(sealed_file),
            false,
            &mut cursor.expected_record_seq,
            &mut cursor.expected_ledger_entry_seq,
            &mut cursor.previous_record_sha256,
            apply,
        )?;
        validate_segment_reference(reference, &stats)?;
    }
    Ok(())
}

fn recover_active_segment(
    directory: &Path,
    cursor: &mut RecoveryCursorV1,
    apply: &mut impl FnMut(DurablePayloadV1) -> Result<(), LedgerDurableErrorV1>,
) -> Result<DurableLedgerV1, LedgerDurableErrorV1> {
    let active_path = directory.join(ACTIVE_FILE_NAME);
    let mut active_options = OpenOptions::new();
    active_options
        .create(cursor.expected_record_seq > 1)
        .truncate(false)
        .read(true)
        .write(true);
    let mut active = active_options
        .open(&active_path)
        .map_err(|error| io_error("打开 Ledger 活动段", &active_path, error))?;
    active
        .sync_all()
        .map_err(|error| io_error("同步 Ledger 活动段", &active_path, error))?;
    sync_directory(directory, "同步 Ledger 活动段目录项")?;
    let original_len = active
        .metadata()
        .map_err(|error| io_error("读取 Ledger 活动段长度", &active_path, error))?
        .len();
    let reader_file = active
        .try_clone()
        .map_err(|error| io_error("复制 Ledger 活动段读取句柄", &active_path, error))?;
    let (segment_stats, last_complete_unique_offset) = decode_segment(
        &active_path,
        BufReader::new(reader_file),
        true,
        &mut cursor.expected_record_seq,
        &mut cursor.expected_ledger_entry_seq,
        &mut cursor.previous_record_sha256,
        apply,
    )?;
    if last_complete_unique_offset != original_len {
        active
            .set_len(last_complete_unique_offset)
            .and_then(|()| active.sync_all())
            .map_err(|error| io_error("恢复 Ledger 活动段完整边界", &active_path, error))?;
    }
    active
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error("定位 Ledger 活动段末尾", &active_path, error))?;
    Ok(DurableLedgerV1::from_active(
        directory,
        active_path,
        active,
        last_complete_unique_offset,
        cursor.expected_record_seq,
        segment_stats,
    ))
}

fn encode_record(
    durable_record_seq: u64,
    payload: &DurablePayloadV1,
) -> Result<Vec<u8>, LedgerDurableErrorV1> {
    let envelope = DurableEnvelopeRefV1 {
        version: RECORD_VERSION,
        encoding: RECORD_ENCODING,
        durable_record_seq,
        payload,
    };
    let mut encoded = serde_json::to_vec(&envelope)
        .map_err(|error| LedgerDurableErrorV1::Encode(error.to_string()))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn cleanup_genesis_candidate<T>(
    directory: &Path,
    candidate_path: &Path,
    original_error: LedgerDurableErrorV1,
) -> Result<T, LedgerDurableErrorV1> {
    match fs::remove_file(candidate_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(LedgerDurableErrorV1::DurabilityUncertain {
                operation: "清理失败的 Ledger genesis 候选",
                message: format!("原始错误：{original_error}；清理错误：{error}"),
            });
        }
    }
    if let Err(error) = sync_directory_io(directory) {
        return Err(LedgerDurableErrorV1::DurabilityUncertain {
            operation: "同步 Ledger genesis 候选清理",
            message: format!("原始错误：{original_error}；目录同步错误：{error}"),
        });
    }
    Err(original_error)
}

fn cleanup_projection_candidate<T>(
    directory: &Path,
    candidate_path: &Path,
    original_error: LedgerDurableErrorV1,
) -> Result<T, LedgerDurableErrorV1> {
    match fs::remove_file(candidate_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(LedgerDurableErrorV1::DurabilityUncertain {
                operation: "清理失败的 Ledger 投影候选",
                message: format!("原始错误：{original_error}；清理错误：{error}"),
            });
        }
    }
    if let Err(error) = sync_directory_io(directory) {
        return Err(LedgerDurableErrorV1::DurabilityUncertain {
            operation: "同步 Ledger 投影候选清理",
            message: format!("原始错误：{original_error}；目录同步错误：{error}"),
        });
    }
    Err(original_error)
}

fn create_projection_candidate(
    directory: &Path,
) -> Result<(File, String, PathBuf), LedgerDurableErrorV1> {
    loop {
        let sequence = PROJECTION_CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let file_name = format!(
            "{PROJECTION_CANDIDATE_PREFIX}{}-{sequence}",
            std::process::id()
        );
        let path = directory.join(&file_name);
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => return Ok((file, file_name, path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io_error("创建 Ledger 投影候选", &path, error)),
        }
    }
}

fn decode_segment(
    path: &Path,
    mut reader: impl BufRead,
    active: bool,
    expected_record_seq: &mut u64,
    expected_ledger_entry_seq: &mut u64,
    previous_record_sha256: &mut Option<[u8; 32]>,
    apply: &mut impl FnMut(DurablePayloadV1) -> Result<(), LedgerDurableErrorV1>,
) -> Result<(SegmentStatsV1, u64), LedgerDurableErrorV1> {
    let mut stats = SegmentStatsV1::default();
    let mut line = Vec::new();
    let mut offset = 0_u64;
    let mut last_complete_unique_offset = 0_u64;
    let mut duplicate_tail_started = false;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| io_error("读取 Ledger 段", path, error))?;
        if read == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            if active {
                break;
            }
            return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                "封存段 {} 存在不完整末记录",
                path.display()
            )));
        }
        let envelope: DurableEnvelopeV1 = serde_json::from_slice(&line).map_err(|error| {
            LedgerDurableErrorV1::CompleteRecordDecode {
                path: path.to_path_buf(),
                offset,
                message: error.to_string(),
            }
        })?;
        if envelope.version != RECORD_VERSION || envelope.encoding != RECORD_ENCODING {
            return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                "{} 的记录版本或编码不受支持",
                path.display()
            )));
        }
        if active && envelope.durable_record_seq.checked_add(1) == Some(*expected_record_seq) {
            let duplicate_sha256: [u8; 32] = Sha256::digest(&line).into();
            if previous_record_sha256.as_ref() != Some(&duplicate_sha256) {
                return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                    "{} 的重复尾记录内容与已提交记录不同",
                    path.display()
                )));
            }
            duplicate_tail_started = true;
            offset += read as u64;
            continue;
        }
        if duplicate_tail_started {
            return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                "{} 的重复完整记录后仍有新记录",
                path.display()
            )));
        }
        if envelope.durable_record_seq != *expected_record_seq {
            return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                "{} 的耐久记录序号不连续：预期 {}，实际 {}",
                path.display(),
                expected_record_seq,
                envelope.durable_record_seq
            )));
        }
        if let Some(ledger_entry_seq) = envelope.payload.ledger_entry_seq() {
            if ledger_entry_seq != *expected_ledger_entry_seq {
                return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                    "{} 的 Ledger 序号不连续：预期 {}，实际 {}",
                    path.display(),
                    expected_ledger_entry_seq,
                    ledger_entry_seq
                )));
            }
            *expected_ledger_entry_seq =
                expected_ledger_entry_seq.checked_add(1).ok_or_else(|| {
                    LedgerDurableErrorV1::InvalidStructure("Ledger 序号溢出".to_owned())
                })?;
        }
        stats.observe(
            envelope.durable_record_seq,
            envelope.payload.ledger_entry_seq(),
        );
        *expected_record_seq = expected_record_seq
            .checked_add(1)
            .ok_or_else(|| LedgerDurableErrorV1::InvalidStructure("耐久记录序号溢出".to_owned()))?;
        apply(envelope.payload)?;
        *previous_record_sha256 = Some(Sha256::digest(&line).into());
        offset += read as u64;
        last_complete_unique_offset = offset;
    }
    Ok((stats, last_complete_unique_offset))
}

fn segment_file_name(
    first_record: u64,
    last_record: u64,
    first_ledger: Option<u64>,
    last_ledger: Option<u64>,
    sha256: &str,
) -> String {
    format!(
        "{SEGMENT_PREFIX}{first_record}--{last_record}--{}--{}--{sha256}{SEGMENT_SUFFIX}",
        optional_seq(first_ledger),
        optional_seq(last_ledger)
    )
}

fn projection_file_name(ledger_entry_seq: u64, sha256: &str) -> String {
    format!("{PROJECTION_PREFIX}{ledger_entry_seq}--{sha256}{PROJECTION_SUFFIX}")
}

fn optional_seq(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

fn parse_optional_seq(value: &str) -> Result<Option<u64>, LedgerDurableErrorV1> {
    if value == "none" {
        Ok(None)
    } else {
        value.parse::<u64>().map(Some).map_err(|_| {
            LedgerDurableErrorV1::InvalidStructure(format!("封存段可选序号非法：{value}"))
        })
    }
}

fn parse_segment_reference(
    path: &Path,
    file_name: &str,
) -> Result<LedgerSealedSegmentReferenceV1, LedgerDurableErrorV1> {
    let body = file_name
        .strip_prefix(SEGMENT_PREFIX)
        .and_then(|value| value.strip_suffix(SEGMENT_SUFFIX))
        .ok_or_else(|| LedgerDurableErrorV1::InvalidStructure("封存段文件名非法".to_owned()))?;
    let fields = body.split("--").collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(LedgerDurableErrorV1::InvalidStructure(format!(
            "封存段文件名非法：{file_name}"
        )));
    }
    let parse_seq = |value: &str| {
        value
            .parse::<u64>()
            .map_err(|_| LedgerDurableErrorV1::InvalidStructure(format!("封存段序号非法：{value}")))
    };
    Ok(LedgerSealedSegmentReferenceV1 {
        location: path.to_string_lossy().into_owned(),
        sha256: fields[4].to_owned(),
        first_durable_record_seq: parse_seq(fields[0])?,
        last_durable_record_seq: parse_seq(fields[1])?,
        first_ledger_entry_seq: parse_optional_seq(fields[2])?,
        last_ledger_entry_seq: parse_optional_seq(fields[3])?,
    })
}

fn validate_segment_reference(
    reference: &LedgerSealedSegmentReferenceV1,
    stats: &SegmentStatsV1,
) -> Result<(), LedgerDurableErrorV1> {
    if stats.first_record_seq != Some(reference.first_durable_record_seq)
        || stats.last_record_seq != Some(reference.last_durable_record_seq)
        || stats.first_ledger_entry_seq != reference.first_ledger_entry_seq
        || stats.last_ledger_entry_seq != reference.last_ledger_entry_seq
    {
        return Err(LedgerDurableErrorV1::InvalidStructure(format!(
            "封存段 {} 的文件名序号范围与内容不一致",
            reference.location
        )));
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, LedgerDurableErrorV1> {
    let mut file =
        File::open(path).map_err(|error| io_error("打开 Ledger 段以计算 SHA-256", path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error("读取 Ledger 段以计算 SHA-256", path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sync_directory(path: &Path, operation: &'static str) -> Result<(), LedgerDurableErrorV1> {
    sync_directory_io(path).map_err(|error| io_error(operation, path, error))
}

fn sync_directory_io(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

fn io_error(operation: &'static str, path: &Path, error: std::io::Error) -> LedgerDurableErrorV1 {
    LedgerDurableErrorV1::Io {
        operation,
        path: path.to_path_buf(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use contracts::ExactDecimal;
    use tempfile::TempDir;

    use super::*;

    fn policy(value: &str) -> AccountAdmissionPolicyV1 {
        AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal(value.to_owned()),
        }
    }

    #[test]
    fn genesis_publish_failure_leaves_one_complete_recoverable_policy() {
        let parent = TempDir::new().unwrap();
        let directory = parent.path().join("ledger-instance");
        assert!(!directory.exists());
        assert!(matches!(
            DurableLedgerV1::create_with_publish_failure(&directory, &policy("80")),
            Err(LedgerDurableErrorV1::DurabilityUncertain { .. })
        ));
        assert!(!directory.join(GENESIS_CANDIDATE_FILE_NAME).exists());
        assert!(directory.join(ACTIVE_FILE_NAME).exists());
        assert!(DurableLedgerV1::create(&directory, &policy("90")).is_err());

        let mut recovered_policy = None;
        let _ledger = DurableLedgerV1::open(&directory, |payload| {
            if let DurablePayloadV1::Genesis { policy } = payload {
                recovered_policy = Some(policy);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(recovered_policy, Some(policy("80")));
    }

    #[test]
    fn projection_candidates_are_owned_by_the_call_that_created_them() {
        let directory = TempDir::new().unwrap();
        let (mut first, first_name, first_path) =
            create_projection_candidate(directory.path()).unwrap();
        first.write_all(b"first").unwrap();
        first.sync_all().unwrap();

        let (mut second, second_name, second_path) =
            create_projection_candidate(directory.path()).unwrap();
        second.write_all(b"second").unwrap();
        second.sync_all().unwrap();

        assert_ne!(first_name, second_name);
        assert_eq!(fs::read(first_path).unwrap(), b"first");
        assert_eq!(fs::read(second_path).unwrap(), b"second");
    }
}
