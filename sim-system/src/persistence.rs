use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use contracts::Sha256Hex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use rustix::fs::{FlockOperation, RenameFlags, flock, renameat_with};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

const CURRENT_CHECKPOINT_SCHEMA_V1: &str = "sim_local_checkpoint_current_v1";
const CURRENT_POINTER_SCHEMA_V1: &str = "sim_local_checkpoint_pointer_v1";
const EVIDENCE_ENVELOPE_SCHEMA_V1: &str = "sim_local_evidence_envelope_v1";
const CURRENT_FILE_NAME: &str = "CURRENT";
const CHECKPOINT_DIRECTORY_NAME: &str = "checkpoints";
const EVIDENCE_DIRECTORY_NAME: &str = "evidence";
const ARTIFACT_DIRECTORY_NAME: &str = "artifacts";
const LOCK_FILE_NAME: &str = "LOCK";
const TRANSACTION_LOG_DATABASE_NAME_V2: &str = "durability-v2.sqlite3";
const TRANSACTION_LOG_SCHEMA_V2: &str = "sim_local_transaction_log_v2";
const TRANSACTION_RECORD_HASH_DOMAIN_V2: &[u8] = b"SIM_LOCAL_TRANSACTION_RECORD_V2";
const TRANSACTION_RECORD_SCHEMA_V2: &str = "sim_local_transaction_record_v2";
pub(crate) const FINAL_EVIDENCE_DIRECTORY_NAME_V2: &str = "evidence-v2";
const FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2: &str = ".evidence-v2.candidate";
const FINAL_EVIDENCE_ENVELOPE_SCHEMA_V2: &str = "sim_local_evidence_envelope_v2";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurabilityProfileV2 {
    CleanRun,
    RecoverablePaper,
}

impl DurabilityProfileV2 {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CleanRun => "CLEAN_RUN",
            Self::RecoverablePaper => "RECOVERABLE_PAPER",
        }
    }

    fn parse(value: &str) -> Result<Self, PersistenceErrorV1> {
        match value {
            "CLEAN_RUN" => Ok(Self::CleanRun),
            "RECOVERABLE_PAPER" => Ok(Self::RecoverablePaper),
            _ => Err(PersistenceErrorV1::Integrity(format!(
                "不支持的 V2 耐久档位：{value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransactionLogIdentityV2 {
    pub(crate) run_id: String,
    pub(crate) durability_profile: DurabilityProfileV2,
}

impl TransactionLogIdentityV2 {
    pub(crate) fn new(run_id: String, durability_profile: DurabilityProfileV2) -> Self {
        Self {
            run_id,
            durability_profile,
        }
    }
}

pub(crate) struct ImmutableArtifactMemberV1 {
    pub(crate) file_name: &'static str,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DurableResourcePreviewV1 {
    pub(crate) evidence_file_count: u64,
    pub(crate) evidence_bytes: u64,
    pub(crate) checkpoint_file_count: u64,
    pub(crate) checkpoint_bytes: u64,
    pub(crate) artifact_file_count: u64,
    pub(crate) artifact_bytes: u64,
    pub(crate) runtime_root_file_count: u64,
    pub(crate) runtime_root_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DurableCommitResourcePreviewsV1 {
    /// 已排除可识别孤立候选、但仍保留当前正式 `CURRENT` 与检查点的提交前稳态。
    pub(crate) maintenance_ready_before: DurableResourcePreviewV1,
    pub(crate) during_commit_peak: DurableResourcePreviewV1,
    /// `CURRENT` 已发布且旧正式检查点已清理时的稳态；调用方不能据此跳过提交后的真实计量。
    pub(crate) maintenance_succeeded_steady: DurableResourcePreviewV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DurableCommitReceiptV1 {
    pub(crate) sequence: u64,
    pub(crate) evidence_sha256: Sha256Hex,
    pub(crate) checkpoint_sha256: Sha256Hex,
    pub(crate) evidence_path: PathBuf,
    pub(crate) checkpoint_path: PathBuf,
    pub(crate) current_path: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DurableCommitFailureV1 {
    DefinitelyNotCommitted(PersistenceErrorV1),
    RecoveryRequired(PersistenceErrorV1),
}

impl fmt::Display for DurableCommitFailureV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefinitelyNotCommitted(error) => write!(formatter, "{error}"),
            Self::RecoveryRequired(error) => {
                write!(formatter, "本地耐久提交结果必须经重启恢复裁决：{error}")
            }
        }
    }
}

impl std::error::Error for DurableCommitFailureV1 {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredEvidenceJsonV1 {
    pub(crate) sequence: u64,
    pub(crate) content_sha256: Sha256Hex,
    pub(crate) envelope_byte_length: u64,
    pub(crate) json: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredDurabilityJsonV1 {
    pub(crate) through_commit_sequence: u64,
    pub(crate) checkpoint_sha256: Sha256Hex,
    pub(crate) checkpoint_json: Vec<u8>,
    pub(crate) evidence: Vec<RecoveredEvidenceJsonV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredEvidenceV1<Evidence> {
    pub(crate) sequence: u64,
    pub(crate) content_sha256: Sha256Hex,
    pub(crate) envelope_byte_length: u64,
    pub(crate) body: Evidence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredDurabilityV1<Evidence, Checkpoint> {
    pub(crate) through_commit_sequence: u64,
    pub(crate) checkpoint_sha256: Sha256Hex,
    pub(crate) checkpoint: Checkpoint,
    pub(crate) evidence: Vec<RecoveredEvidenceV1<Evidence>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PersistenceErrorV1 {
    Io {
        operation: &'static str,
        message: String,
    },
    Json {
        operation: &'static str,
        message: String,
    },
    Sqlite {
        operation: &'static str,
        message: String,
    },
    Integrity(String),
    SequenceOverflow,
    DurabilityUncertain {
        path: PathBuf,
        message: String,
    },
    InjectedBeforeCheckpointPublish,
    InjectedAfterCheckpointBeforeEvidencePublish,
    InjectedAfterEvidenceCommit,
    #[cfg(test)]
    InjectedBeforeArtifactBundlePublish,
}

impl fmt::Display for PersistenceErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, message } => {
                write!(formatter, "本地耐久存储 {operation} 失败：{message}")
            }
            Self::Json { operation, message } => {
                write!(formatter, "本地耐久存储 {operation} JSON 失败：{message}")
            }
            Self::Sqlite { operation, message } => {
                write!(formatter, "本地耐久存储 {operation} SQLite 失败：{message}")
            }
            Self::Integrity(message) => write!(formatter, "本地耐久存储完整性失败：{message}"),
            Self::SequenceOverflow => write!(formatter, "本地耐久提交序号溢出"),
            Self::DurabilityUncertain { path, message } => write!(
                formatter,
                "本地耐久发布结果不确定（{}）：{message}",
                path.display()
            ),
            Self::InjectedBeforeCheckpointPublish => {
                write!(formatter, "测试在不可变检查点发布前注入失败")
            }
            Self::InjectedAfterCheckpointBeforeEvidencePublish => {
                write!(formatter, "测试在检查点发布后、证据提交前注入失败")
            }
            Self::InjectedAfterEvidenceCommit => {
                write!(formatter, "测试在证据提交后、辅助维护前模拟进程崩溃")
            }
            #[cfg(test)]
            Self::InjectedBeforeArtifactBundlePublish => {
                write!(
                    formatter,
                    "测试在运行产物候选目录同步后、正式发布前注入失败"
                )
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransactionLogCommitReceiptV2 {
    pub(crate) sequence: u64,
    pub(crate) record_sha256: Sha256Hex,
    pub(crate) encoded_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FinalEvidenceExportReceiptV2 {
    pub(crate) relative_path: PathBuf,
    pub(crate) evidence_file_count: u64,
    pub(crate) evidence_bytes: u64,
    pub(crate) evidence_head_sha256: Option<Sha256Hex>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EncodedTransactionRecordV2 {
    record_json: Vec<u8>,
    encoded_bytes: u64,
}

impl EncodedTransactionRecordV2 {
    pub(crate) fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation> {
    pub(crate) sequence: u64,
    pub(crate) record_sha256: Sha256Hex,
    pub(crate) evidence: Evidence,
    pub(crate) recovery_operation: RecoveryOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredApplicationCheckpointV2<Checkpoint> {
    pub(crate) through_sequence: u64,
    pub(crate) transaction_head_sha256: Option<Sha256Hex>,
    pub(crate) checkpoint_sha256: Sha256Hex,
    pub(crate) checkpoint: Checkpoint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredTransactionLogV2<Evidence, RecoveryOperation, Checkpoint> {
    pub(crate) checkpoint: Option<RecoveredApplicationCheckpointV2<Checkpoint>>,
    pub(crate) tail: Vec<RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation>>,
}

pub(crate) struct LocalTransactionLogStoreV2 {
    database_path: PathBuf,
    identity: TransactionLogIdentityV2,
    connection: Connection,
    clean_run_append_open: bool,
    _lock_file: File,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TransactionRecordBodyRefV2<'a, Evidence, RecoveryOperation> {
    schema_version: &'static str,
    evidence: &'a Evidence,
    recovery_operation: &'a RecoveryOperation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactionRecordBodyV2<Evidence, RecoveryOperation> {
    schema_version: String,
    evidence: Evidence,
    recovery_operation: RecoveryOperation,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalEvidenceEnvelopeV2<Evidence> {
    schema_version: String,
    sequence: u64,
    predecessor_evidence_sha256: Option<Sha256Hex>,
    body: Evidence,
}

struct TransactionLogAppendStateV2 {
    last_sequence: u64,
    transaction_head_sha256: Option<Sha256Hex>,
    logical_log_bytes: i64,
}

struct TransactionLogRecoveryHeaderV2<Checkpoint> {
    last_sequence: u64,
    transaction_head_sha256: Option<Sha256Hex>,
    checkpoint: Option<RecoveredApplicationCheckpointV2<Checkpoint>>,
}

impl std::error::Error for PersistenceErrorV1 {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentCheckpointContentV1 {
    schema_version: String,
    through_commit_sequence: u64,
    evidence_head_sha256: Sha256Hex,
    checkpoint: Box<RawValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentCheckpointFileV1 {
    content: CurrentCheckpointContentV1,
    checkpoint_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentCheckpointPointerV1 {
    schema_version: String,
    through_commit_sequence: u64,
    checkpoint_sha256: Sha256Hex,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceEnvelopeV1 {
    schema_version: String,
    sequence: u64,
    predecessor_evidence_sha256: Option<Sha256Hex>,
    body: Box<RawValue>,
}

struct EncodedCommitCandidateV1 {
    current: CurrentCheckpointFileV1,
    evidence_bytes: Vec<u8>,
    checkpoint_bytes: Vec<u8>,
    pointer_bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct EvidenceFileV1 {
    path: PathBuf,
    content_sha256: Sha256Hex,
}

pub(crate) struct LocalDurabilityStoreV1 {
    root: PathBuf,
    checkpoint_directory: PathBuf,
    evidence_directory: PathBuf,
    _lock_file: File,
    current: Option<CurrentCheckpointFileV1>,
    #[cfg(test)]
    commit_fault_once: Option<CommitFaultV1>,
    #[cfg(test)]
    artifact_publish_fault_once: bool,
    #[cfg(test)]
    artifact_publish_after_rename_fault_once: bool,
}

impl LocalDurabilityStoreV1 {
    pub(crate) fn current_resources(&self) -> Result<DurableResourcePreviewV1, PersistenceErrorV1> {
        let (runtime_root_file_count, runtime_root_bytes) =
            directory_regular_file_usage(&self.root)?;
        let (evidence_file_count, evidence_bytes) =
            directory_regular_file_usage(&self.evidence_directory)?;
        let (checkpoint_file_count, checkpoint_bytes) =
            directory_regular_file_usage(&self.checkpoint_directory)?;
        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        let (artifact_file_count, artifact_bytes) = if artifacts.exists() {
            directory_regular_file_usage(&artifacts)?
        } else {
            (0, 0)
        };
        Ok(DurableResourcePreviewV1 {
            evidence_file_count,
            evidence_bytes,
            checkpoint_file_count,
            checkpoint_bytes,
            artifact_file_count,
            artifact_bytes,
            runtime_root_file_count,
            runtime_root_bytes,
        })
    }

    pub(crate) fn preview_commit_resources<Evidence, Checkpoint>(
        &self,
        evidence: &Evidence,
        checkpoint: &Checkpoint,
    ) -> Result<DurableResourcePreviewV1, PersistenceErrorV1>
    where
        Evidence: Serialize,
        Checkpoint: Serialize,
    {
        Ok(self
            .preview_commit_resource_states(evidence, checkpoint)?
            .during_commit_peak)
    }

    pub(crate) fn preview_commit_resource_states<Evidence, Checkpoint>(
        &self,
        evidence: &Evidence,
        checkpoint: &Checkpoint,
    ) -> Result<DurableCommitResourcePreviewsV1, PersistenceErrorV1>
    where
        Evidence: Serialize,
        Checkpoint: Serialize,
    {
        Ok(self
            .preview_commit_resource_states_with_current(evidence, checkpoint)?
            .0)
    }

    /// 预览首提交及其维护成功后紧接的第二次提交峰值；第二候选绑定首候选而非磁盘旧水位。
    pub(crate) fn preview_consecutive_commit_resources<
        FirstEvidence,
        FirstCheckpoint,
        SecondEvidence,
        SecondCheckpoint,
    >(
        &self,
        first_evidence: &FirstEvidence,
        first_checkpoint: &FirstCheckpoint,
        second_evidence: &SecondEvidence,
        second_checkpoint: &SecondCheckpoint,
    ) -> Result<(DurableCommitResourcePreviewsV1, DurableResourcePreviewV1), PersistenceErrorV1>
    where
        FirstEvidence: Serialize,
        FirstCheckpoint: Serialize,
        SecondEvidence: Serialize,
        SecondCheckpoint: Serialize,
    {
        let (first_previews, first_current) =
            self.preview_commit_resource_states_with_current(first_evidence, first_checkpoint)?;
        let second_evidence_json = serde_json::to_vec(second_evidence)
            .map_err(|error| json_error("预编码第二份领域证据正文", error))?;
        let second_checkpoint_json = serde_json::to_vec(second_checkpoint)
            .map_err(|error| json_error("预编码第二份检查点正文", error))?;
        let second = encode_commit_candidate_v1(
            Some(&first_current),
            &second_evidence_json,
            &second_checkpoint_json,
        )?;
        let evidence_files = scan_evidence_files(&self.evidence_directory)?;
        let second_sequence = second.current.content.through_commit_sequence;
        if !evidence_files
            .get(&second_sequence)
            .is_none_or(Vec::is_empty)
        {
            return Err(PersistenceErrorV1::Integrity(format!(
                "待提交证据序号 {second_sequence} 已存在正式文件"
            )));
        }
        let second_during_commit_peak =
            preview_candidate_from_steady_v1(first_previews.maintenance_succeeded_steady, &second)?;
        Ok((first_previews, second_during_commit_peak))
    }

    fn preview_commit_resource_states_with_current<Evidence, Checkpoint>(
        &self,
        evidence: &Evidence,
        checkpoint: &Checkpoint,
    ) -> Result<(DurableCommitResourcePreviewsV1, CurrentCheckpointFileV1), PersistenceErrorV1>
    where
        Evidence: Serialize,
        Checkpoint: Serialize,
    {
        let evidence_json = serde_json::to_vec(evidence)
            .map_err(|error| json_error("预编码领域证据正文", error))?;
        let checkpoint_json = serde_json::to_vec(checkpoint)
            .map_err(|error| json_error("预编码检查点正文", error))?;
        let disk_current = load_latest_checkpoint(&self.checkpoint_directory)?
            .map(|loaded| loaded.checkpoint_file);
        ensure_checkpoint_unchanged(self.current.as_ref(), disk_current.as_ref())?;
        let encoded =
            encode_commit_candidate_v1(self.current.as_ref(), &evidence_json, &checkpoint_json)?;
        let sequence = encoded.current.content.through_commit_sequence;
        let next_evidence_bytes = &encoded.evidence_bytes;
        let next_checkpoint_bytes = &encoded.checkpoint_bytes;
        let next_pointer_bytes = &encoded.pointer_bytes;

        let existing_pointer_bytes = current_pointer_file_bytes(&self.root)?;

        let root_candidates = inspect_known_orphan_candidates(
            &self.root,
            KnownOrphanCandidateKindV1::CurrentPointer,
        )?;
        let evidence_candidates = inspect_known_orphan_candidates(
            &self.evidence_directory,
            KnownOrphanCandidateKindV1::ImmutableJson,
        )?;
        let checkpoint_candidates = inspect_known_orphan_candidates(
            &self.checkpoint_directory,
            KnownOrphanCandidateKindV1::ImmutableJson,
        )?;
        let orphan_candidate_file_count = root_candidates
            .file_count
            .checked_add(evidence_candidates.file_count)
            .and_then(|count| count.checked_add(checkpoint_candidates.file_count))
            .ok_or_else(|| PersistenceErrorV1::Integrity("孤立候选文件数量溢出".to_owned()))?;
        let orphan_candidate_bytes = root_candidates
            .bytes
            .checked_add(evidence_candidates.bytes)
            .and_then(|bytes| bytes.checked_add(checkpoint_candidates.bytes))
            .ok_or_else(|| PersistenceErrorV1::Integrity("孤立候选文件字节溢出".to_owned()))?;

        let (root_file_count, root_bytes) = directory_regular_file_usage(&self.root)?;
        let (evidence_file_count, evidence_bytes) =
            directory_regular_file_usage(&self.evidence_directory)?;
        let (checkpoint_file_count, checkpoint_bytes) =
            directory_regular_file_usage(&self.checkpoint_directory)?;
        let existing_checkpoint_file_count = checkpoint_file_count
            .checked_sub(checkpoint_candidates.file_count)
            .ok_or_else(|| {
                PersistenceErrorV1::Integrity("检查点孤立候选数量超出当前计量".to_owned())
            })?;
        let existing_checkpoint_bytes = checkpoint_bytes
            .checked_sub(checkpoint_candidates.bytes)
            .ok_or_else(|| {
            PersistenceErrorV1::Integrity("检查点孤立候选字节超出当前计量".to_owned())
        })?;
        let evidence_files = scan_evidence_files(&self.evidence_directory)?;
        if !evidence_files.get(&sequence).is_none_or(Vec::is_empty) {
            return Err(PersistenceErrorV1::Integrity(format!(
                "待提交证据序号 {sequence} 已存在正式文件"
            )));
        }
        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        let (artifact_file_count, artifact_bytes) = if artifacts.exists() {
            directory_regular_file_usage(&artifacts)?
        } else {
            (0, 0)
        };
        let candidate_file_count = 3_u64;
        let next_evidence_byte_count = u64::try_from(next_evidence_bytes.len())
            .map_err(|_| PersistenceErrorV1::Integrity("证据候选字节超出 u64".to_owned()))?;
        let next_checkpoint_byte_count = u64::try_from(next_checkpoint_bytes.len())
            .map_err(|_| PersistenceErrorV1::Integrity("检查点候选字节超出 u64".to_owned()))?;
        let next_pointer_byte_count = u64::try_from(next_pointer_bytes.len())
            .map_err(|_| PersistenceErrorV1::Integrity("CURRENT 候选字节超出 u64".to_owned()))?;
        let candidate_bytes = next_evidence_byte_count
            .checked_add(next_checkpoint_byte_count)
            .and_then(|subtotal| subtotal.checked_add(next_pointer_byte_count))
            .ok_or_else(|| PersistenceErrorV1::Integrity("候选耐久字节溢出".to_owned()))?;
        let during_commit_peak = DurableResourcePreviewV1 {
            evidence_file_count: evidence_file_count
                .checked_sub(evidence_candidates.file_count)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("证据孤立候选数量超出当前计量".to_owned())
                })?
                .checked_add(1)
                .ok_or_else(|| PersistenceErrorV1::Integrity("证据文件数量溢出".to_owned()))?,
            evidence_bytes: evidence_bytes
                .checked_sub(evidence_candidates.bytes)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("证据孤立候选字节超出当前计量".to_owned())
                })?
                .checked_add(next_evidence_byte_count)
                .ok_or_else(|| PersistenceErrorV1::Integrity("证据字节溢出".to_owned()))?,
            checkpoint_file_count: checkpoint_file_count
                .checked_sub(checkpoint_candidates.file_count)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("检查点孤立候选数量超出当前计量".to_owned())
                })?
                .checked_add(1)
                .ok_or_else(|| PersistenceErrorV1::Integrity("检查点文件数量溢出".to_owned()))?,
            checkpoint_bytes: checkpoint_bytes
                .checked_sub(checkpoint_candidates.bytes)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("检查点孤立候选字节超出当前计量".to_owned())
                })?
                .checked_add(next_checkpoint_byte_count)
                .ok_or_else(|| PersistenceErrorV1::Integrity("检查点字节溢出".to_owned()))?,
            artifact_file_count,
            artifact_bytes,
            runtime_root_file_count: root_file_count
                .checked_sub(orphan_candidate_file_count)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("孤立候选数量超出运行根计量".to_owned())
                })?
                .checked_add(candidate_file_count)
                .ok_or_else(|| PersistenceErrorV1::Integrity("运行根文件数量溢出".to_owned()))?,
            runtime_root_bytes: root_bytes
                .checked_sub(orphan_candidate_bytes)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("孤立候选字节超出运行根计量".to_owned())
                })?
                .checked_add(candidate_bytes)
                .ok_or_else(|| PersistenceErrorV1::Integrity("运行根字节溢出".to_owned()))?,
        };
        let maintenance_ready_before = DurableResourcePreviewV1 {
            evidence_file_count: evidence_file_count
                .checked_sub(evidence_candidates.file_count)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("证据孤立候选数量超出当前计量".to_owned())
                })?,
            evidence_bytes: evidence_bytes
                .checked_sub(evidence_candidates.bytes)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("证据孤立候选字节超出当前计量".to_owned())
                })?,
            checkpoint_file_count: existing_checkpoint_file_count,
            checkpoint_bytes: existing_checkpoint_bytes,
            artifact_file_count,
            artifact_bytes,
            runtime_root_file_count: root_file_count
                .checked_sub(orphan_candidate_file_count)
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("孤立候选数量超出运行根计量".to_owned())
                })?,
            runtime_root_bytes: root_bytes.checked_sub(orphan_candidate_bytes).ok_or_else(
                || PersistenceErrorV1::Integrity("孤立候选字节超出运行根计量".to_owned()),
            )?,
        };
        let maintenance_succeeded_steady = DurableResourcePreviewV1 {
            evidence_file_count: during_commit_peak.evidence_file_count,
            evidence_bytes: during_commit_peak.evidence_bytes,
            checkpoint_file_count: 1,
            checkpoint_bytes: next_checkpoint_byte_count,
            artifact_file_count,
            artifact_bytes,
            runtime_root_file_count: root_file_count
                .checked_sub(orphan_candidate_file_count)
                .and_then(|count| count.checked_sub(existing_checkpoint_file_count))
                .and_then(|count| count.checked_sub(u64::from(existing_pointer_bytes.is_some())))
                .and_then(|count| count.checked_add(candidate_file_count))
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("提交后运行根文件数量溢出或下溢".to_owned())
                })?,
            runtime_root_bytes: root_bytes
                .checked_sub(orphan_candidate_bytes)
                .and_then(|bytes| bytes.checked_sub(existing_checkpoint_bytes))
                .and_then(|bytes| bytes.checked_sub(existing_pointer_bytes.unwrap_or_default()))
                .and_then(|bytes| bytes.checked_add(next_evidence_byte_count))
                .and_then(|bytes| bytes.checked_add(next_checkpoint_byte_count))
                .and_then(|bytes| bytes.checked_add(next_pointer_byte_count))
                .ok_or_else(|| {
                    PersistenceErrorV1::Integrity("提交后运行根字节溢出或下溢".to_owned())
                })?,
        };
        Ok((
            DurableCommitResourcePreviewsV1 {
                maintenance_ready_before,
                during_commit_peak,
                maintenance_succeeded_steady,
            },
            encoded.current,
        ))
    }

    pub(crate) fn preview_artifact_bundle_resources(
        &self,
        bundle_identity: &Sha256Hex,
        members: &[ImmutableArtifactMemberV1],
    ) -> Result<DurableResourcePreviewV1, PersistenceErrorV1> {
        if validate_sha256_text(&bundle_identity.0).is_err() || members.is_empty() {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物目录身份或成员集合非法".to_owned(),
            ));
        }
        let mut expected_names = std::collections::BTreeSet::new();
        let member_bytes = members.iter().try_fold(0_u64, |total, member| {
            if member.file_name.is_empty()
                || member.file_name.starts_with('.')
                || member.file_name.contains('/')
                || member.file_name.contains('\\')
                || !expected_names.insert(member.file_name)
            {
                return Err(PersistenceErrorV1::Integrity(
                    "运行产物成员文件名非法或重复".to_owned(),
                ));
            }
            let bytes = u64::try_from(member.bytes.len()).map_err(|_| {
                PersistenceErrorV1::Integrity("运行产物成员字节超出 u64".to_owned())
            })?;
            total
                .checked_add(bytes)
                .ok_or_else(|| PersistenceErrorV1::Integrity("运行产物字节溢出".to_owned()))
        })?;
        let member_count = u64::try_from(members.len())
            .map_err(|_| PersistenceErrorV1::Integrity("运行产物成员数量超出 u64".to_owned()))?;
        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        let target = artifacts.join(&bundle_identity.0);
        let artifact_entries = inspect_artifact_directory(&artifacts, bundle_identity)?;
        if artifact_entries.target_exists {
            verify_immutable_artifact_bundle(&target, members)?;
        }
        let candidate_usage = artifact_entries
            .candidate_identity
            .as_ref()
            .map(|identity| {
                validate_and_measure_artifact_candidate(
                    &artifacts.join(format!(".{}.candidate", identity.0)),
                    &expected_names,
                )
            })
            .transpose()?;
        let mut current = self.current_resources()?;
        if let Some((candidate_file_count, candidate_bytes)) = candidate_usage
            && (candidate_file_count > current.artifact_file_count
                || candidate_bytes > current.artifact_bytes)
        {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物候选占用超出当前产物计量".to_owned(),
            ));
        }
        let actual_artifact_file_count = current.artifact_file_count;
        let actual_artifact_bytes = current.artifact_bytes;
        let actual_runtime_root_file_count = current.runtime_root_file_count;
        let actual_runtime_root_bytes = current.runtime_root_bytes;
        let replacement_runtime_root_file_count = current
            .runtime_root_file_count
            .checked_sub(current.artifact_file_count)
            .and_then(|count| count.checked_add(member_count))
            .ok_or_else(|| PersistenceErrorV1::Integrity("运行根文件数量溢出".to_owned()))?;
        let replacement_runtime_root_bytes = current
            .runtime_root_bytes
            .checked_sub(current.artifact_bytes)
            .and_then(|bytes| bytes.checked_add(member_bytes))
            .ok_or_else(|| PersistenceErrorV1::Integrity("运行根字节溢出".to_owned()))?;
        // 候选目录会在正式发布前被清理并由完整包替换，因此不能把二者相加；
        // 但候选已经真实占用过的峰值也不能因清理而绕过调用方的资源硬门。
        current.artifact_file_count = actual_artifact_file_count.max(member_count);
        current.artifact_bytes = actual_artifact_bytes.max(member_bytes);
        current.runtime_root_file_count =
            actual_runtime_root_file_count.max(replacement_runtime_root_file_count);
        current.runtime_root_bytes = actual_runtime_root_bytes.max(replacement_runtime_root_bytes);
        Ok(current)
    }

    pub(crate) fn existing_artifact_bundle_identity(
        &self,
    ) -> Result<Option<Sha256Hex>, PersistenceErrorV1> {
        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        Ok(inspect_artifact_identities(&artifacts)?.formal_identity)
    }

    pub(crate) fn existing_artifact_candidate_identity(
        &self,
    ) -> Result<Option<Sha256Hex>, PersistenceErrorV1> {
        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        Ok(inspect_artifact_identities(&artifacts)?.candidate_identity)
    }

    pub(crate) fn remove_validated_artifact_candidate(
        &mut self,
        bundle_identity: &Sha256Hex,
        members: &[ImmutableArtifactMemberV1],
    ) -> Result<bool, PersistenceErrorV1> {
        if validate_sha256_text(&bundle_identity.0).is_err() || members.is_empty() {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物目录身份或成员集合非法".to_owned(),
            ));
        }
        let expected_names = artifact_member_names(members)?;
        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        let entries = inspect_artifact_directory(&artifacts, bundle_identity)?;
        let Some(candidate_identity) = entries.candidate_identity else {
            return Ok(false);
        };
        if &candidate_identity != bundle_identity {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物候选身份与待清理包身份不一致".to_owned(),
            ));
        }
        let candidate = artifacts.join(format!(".{}.candidate", candidate_identity.0));
        remove_known_artifact_candidate(&candidate, &expected_names)?;
        sync_directory(&artifacts, &artifacts)?;
        Ok(true)
    }

    pub(crate) fn try_new(root: PathBuf) -> Result<Self, PersistenceErrorV1> {
        Self::try_open(root, false)
    }

    pub(crate) fn try_open_recovery(root: PathBuf) -> Result<Self, PersistenceErrorV1> {
        Self::try_open(root, true)
    }

    fn try_open(
        root: PathBuf,
        require_committed_checkpoint: bool,
    ) -> Result<Self, PersistenceErrorV1> {
        fs::create_dir_all(&root).map_err(|error| io_error("创建运行耐久目录", error))?;
        ensure_directory(&root, "运行耐久目录")?;
        let root =
            fs::canonicalize(&root).map_err(|error| io_error("规范化运行耐久目录", error))?;
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(LOCK_FILE_NAME))
            .map_err(|error| io_error("打开运行根排他锁", error))?;
        if !lock_file
            .metadata()
            .map_err(|error| io_error("读取运行根排他锁元数据", error))?
            .is_file()
        {
            return Err(PersistenceErrorV1::Integrity(
                "运行根排他锁不是普通文件".to_owned(),
            ));
        }
        flock(&lock_file, FlockOperation::NonBlockingLockExclusive)
            .map_err(|error| io_error("取得运行根排他锁", error))?;
        let checkpoint_directory = root.join(CHECKPOINT_DIRECTORY_NAME);
        fs::create_dir_all(&checkpoint_directory)
            .map_err(|error| io_error("创建不可变检查点目录", error))?;
        ensure_directory(&checkpoint_directory, "不可变检查点目录")?;
        let evidence_directory = root.join(EVIDENCE_DIRECTORY_NAME);
        fs::create_dir_all(&evidence_directory)
            .map_err(|error| io_error("创建仅追加证据目录", error))?;
        ensure_directory(&evidence_directory, "仅追加证据目录")?;

        let recovered = recover_json_from(&root, &checkpoint_directory, &evidence_directory)?;
        let current_pointer = load_current_pointer(&root)?;
        if recovered.is_none() {
            let has_uncommitted_material = !scan_evidence_files(&evidence_directory)?.is_empty()
                || !scan_checkpoint_files(&checkpoint_directory)?.is_empty()
                || has_orphan_candidates(&root)?
                || has_orphan_candidates(&checkpoint_directory)?
                || has_orphan_candidates(&evidence_directory)?;
            if current_pointer.is_some()
                || (require_committed_checkpoint && has_uncommitted_material)
            {
                return Err(PersistenceErrorV1::Integrity(
                    "缺少已提交检查点，但恢复根仍含 CURRENT、候选、检查点或仅追加证据".to_owned(),
                ));
            }
            if has_uncommitted_material {
                remove_orphan_candidates(&root, KnownOrphanCandidateKindV1::CurrentPointer)?;
                remove_orphan_candidates(
                    &checkpoint_directory,
                    KnownOrphanCandidateKindV1::ImmutableJson,
                )?;
                remove_orphan_candidates(
                    &evidence_directory,
                    KnownOrphanCandidateKindV1::ImmutableJson,
                )?;
                remove_checkpoints_after(&checkpoint_directory, 0)?;
            }
        }
        if let Some(recovered) = &recovered {
            remove_orphan_candidates(&root, KnownOrphanCandidateKindV1::CurrentPointer)?;
            remove_orphan_candidates(
                &checkpoint_directory,
                KnownOrphanCandidateKindV1::ImmutableJson,
            )?;
            remove_orphan_candidates(
                &evidence_directory,
                KnownOrphanCandidateKindV1::ImmutableJson,
            )?;
            remove_checkpoints_after(
                &checkpoint_directory,
                recovered.current_file.content.through_commit_sequence,
            )?;
            remove_old_checkpoints(
                &checkpoint_directory,
                recovered.current_file.content.through_commit_sequence,
            )?;
            let pointer_matches = current_pointer.as_ref().is_some_and(|pointer| {
                pointer.through_commit_sequence
                    == recovered.current_file.content.through_commit_sequence
                    && pointer.checkpoint_sha256 == recovered.current_file.checkpoint_sha256
            });
            if !pointer_matches {
                publish_current_pointer(
                    &root,
                    recovered.current_file.content.through_commit_sequence,
                    &recovered.current_file.checkpoint_sha256,
                )?;
            }
        }
        let current = recovered.map(|recovered| recovered.current_file);
        Ok(Self {
            root,
            checkpoint_directory,
            evidence_directory,
            _lock_file: lock_file,
            current,
            #[cfg(test)]
            commit_fault_once: None,
            #[cfg(test)]
            artifact_publish_fault_once: false,
            #[cfg(test)]
            artifact_publish_after_rename_fault_once: false,
        })
    }

    pub(crate) fn commit<Evidence, Checkpoint>(
        &mut self,
        evidence: &Evidence,
        checkpoint: &Checkpoint,
    ) -> Result<DurableCommitReceiptV1, PersistenceErrorV1>
    where
        Evidence: Serialize,
        Checkpoint: Serialize,
    {
        let evidence_json =
            serde_json::to_vec(evidence).map_err(|error| json_error("编码领域证据正文", error))?;
        let checkpoint_json =
            serde_json::to_vec(checkpoint).map_err(|error| json_error("编码检查点正文", error))?;
        self.commit_json(&evidence_json, &checkpoint_json)
    }

    pub(crate) fn commit_classified<Evidence, Checkpoint>(
        &mut self,
        evidence: &Evidence,
        checkpoint: &Checkpoint,
    ) -> Result<DurableCommitReceiptV1, DurableCommitFailureV1>
    where
        Evidence: Serialize,
        Checkpoint: Serialize,
    {
        let evidence_json = serde_json::to_vec(evidence).map_err(|error| {
            DurableCommitFailureV1::DefinitelyNotCommitted(json_error("编码领域证据正文", error))
        })?;
        let checkpoint_json = serde_json::to_vec(checkpoint).map_err(|error| {
            DurableCommitFailureV1::DefinitelyNotCommitted(json_error("编码检查点正文", error))
        })?;
        self.commit_json_classified(&evidence_json, &checkpoint_json)
    }

    pub(crate) fn commit_json(
        &mut self,
        evidence_json: &[u8],
        checkpoint_json: &[u8],
    ) -> Result<DurableCommitReceiptV1, PersistenceErrorV1> {
        let fault = self.take_commit_fault();
        self.commit_json_with_fault(evidence_json, checkpoint_json, fault)
    }

    fn commit_json_classified(
        &mut self,
        evidence_json: &[u8],
        checkpoint_json: &[u8],
    ) -> Result<DurableCommitReceiptV1, DurableCommitFailureV1> {
        let previous_sequence = self
            .current
            .as_ref()
            .map_or(0, |current| current.content.through_commit_sequence);
        let fault = self.take_commit_fault();
        self.commit_json_with_fault(evidence_json, checkpoint_json, fault)
            .map_err(|error| {
                let checkpoint_was_published = load_latest_checkpoint(&self.checkpoint_directory)
                    .map(|loaded| {
                        loaded.is_some_and(|loaded| {
                            loaded.checkpoint_file.content.through_commit_sequence
                                > previous_sequence
                        })
                    })
                    .unwrap_or(true);
                if checkpoint_was_published {
                    DurableCommitFailureV1::RecoveryRequired(error)
                } else {
                    DurableCommitFailureV1::DefinitelyNotCommitted(error)
                }
            })
    }

    fn take_commit_fault(&mut self) -> CommitFaultV1 {
        #[cfg(test)]
        {
            self.commit_fault_once.take().unwrap_or(CommitFaultV1::None)
        }
        #[cfg(not(test))]
        {
            CommitFaultV1::None
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_before_checkpoint_publish_once(&mut self) {
        self.commit_fault_once = Some(CommitFaultV1::BeforeCheckpointPublish);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_checkpoint_before_evidence_publish_once(&mut self) {
        self.commit_fault_once = Some(CommitFaultV1::AfterCheckpointBeforeEvidencePublish);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_evidence_commit_once(&mut self) {
        self.commit_fault_once = Some(CommitFaultV1::AfterEvidenceCommit);
    }

    #[cfg(test)]
    pub(crate) fn fail_before_artifact_bundle_publish_once(&mut self) {
        self.artifact_publish_fault_once = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_after_artifact_bundle_rename_before_directory_sync_once(&mut self) {
        self.artifact_publish_after_rename_fault_once = true;
    }

    pub(crate) fn publish_immutable_artifact_bundle(
        &mut self,
        bundle_identity: &Sha256Hex,
        members: &[ImmutableArtifactMemberV1],
    ) -> Result<PathBuf, PersistenceErrorV1> {
        if validate_sha256_text(&bundle_identity.0).is_err() || members.is_empty() {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物目录身份或成员集合非法".to_owned(),
            ));
        }
        let mut expected_names = std::collections::BTreeSet::new();
        for member in members {
            if member.file_name.is_empty()
                || member.file_name.starts_with('.')
                || member.file_name.contains('/')
                || member.file_name.contains('\\')
                || !expected_names.insert(member.file_name)
            {
                return Err(PersistenceErrorV1::Integrity(
                    "运行产物成员文件名非法或重复".to_owned(),
                ));
            }
        }

        let artifacts = self.root.join(ARTIFACT_DIRECTORY_NAME);
        fs::create_dir_all(&artifacts).map_err(|error| io_error("创建运行产物目录", error))?;
        ensure_directory(&artifacts, "运行产物目录")?;
        sync_directory(&self.root, &artifacts)?;
        let target = artifacts.join(&bundle_identity.0);
        let artifact_entries = inspect_artifact_directory(&artifacts, bundle_identity)?;
        let existing_candidate = artifact_entries
            .candidate_identity
            .as_ref()
            .map(|identity| artifacts.join(format!(".{}.candidate", identity.0)));
        if artifact_entries.target_exists {
            verify_immutable_artifact_bundle(&target, members)?;
            if let Some(candidate) = &existing_candidate {
                remove_known_artifact_candidate(candidate, &expected_names)?;
            }
            sync_directory(&artifacts, &target)?;
            return Ok(target);
        }

        if let Some(candidate) = &existing_candidate {
            remove_known_artifact_candidate(candidate, &expected_names)?;
        }
        let candidate = artifacts.join(format!(".{}.candidate", bundle_identity.0));
        fs::create_dir(&candidate).map_err(|error| io_error("创建运行产物候选目录", error))?;
        for member in members {
            let path = candidate.join(member.file_name);
            write_synced_new_file(&path, &member.bytes, "写入运行产物候选成员")?;
        }
        sync_directory(&candidate, &candidate)?;

        #[cfg(test)]
        if std::mem::take(&mut self.artifact_publish_fault_once) {
            return Err(PersistenceErrorV1::InjectedBeforeArtifactBundlePublish);
        }

        rename_noreplace(&candidate, &target)?;
        #[cfg(test)]
        if std::mem::take(&mut self.artifact_publish_after_rename_fault_once) {
            return Err(PersistenceErrorV1::DurabilityUncertain {
                path: target,
                message: "测试在运行产物正式重命名后、父目录同步前注入失败".to_owned(),
            });
        }
        sync_directory(&artifacts, &target)?;
        verify_immutable_artifact_bundle(&target, members)?;
        Ok(target)
    }

    pub(crate) fn recover<Evidence, Checkpoint>(
        &self,
    ) -> Result<Option<RecoveredDurabilityV1<Evidence, Checkpoint>>, PersistenceErrorV1>
    where
        Evidence: DeserializeOwned,
        Checkpoint: DeserializeOwned,
    {
        let Some(recovered) = self.recover_json()? else {
            return Ok(None);
        };
        let checkpoint = serde_json::from_slice(&recovered.checkpoint_json)
            .map_err(|error| json_error("解码检查点正文", error))?;
        let evidence = recovered
            .evidence
            .into_iter()
            .map(|member| {
                let body = serde_json::from_slice(&member.json)
                    .map_err(|error| json_error("解码证据正文", error))?;
                Ok(RecoveredEvidenceV1 {
                    sequence: member.sequence,
                    content_sha256: member.content_sha256,
                    envelope_byte_length: member.envelope_byte_length,
                    body,
                })
            })
            .collect::<Result<Vec<_>, PersistenceErrorV1>>()?;
        Ok(Some(RecoveredDurabilityV1 {
            through_commit_sequence: recovered.through_commit_sequence,
            checkpoint_sha256: recovered.checkpoint_sha256,
            checkpoint,
            evidence,
        }))
    }

    pub(crate) fn recover_json(
        &self,
    ) -> Result<Option<RecoveredDurabilityJsonV1>, PersistenceErrorV1> {
        Ok(recover_json_from(
            &self.root,
            &self.checkpoint_directory,
            &self.evidence_directory,
        )?
        .map(|recovered| recovered.public))
    }

    fn commit_json_with_fault(
        &mut self,
        evidence_json: &[u8],
        checkpoint_json: &[u8],
        fault: CommitFaultV1,
    ) -> Result<DurableCommitReceiptV1, PersistenceErrorV1> {
        let domain_evidence = parse_raw_json(evidence_json, "验证领域证据正文")?;
        let checkpoint = parse_raw_json(checkpoint_json, "验证检查点正文")?;

        let disk_current = load_latest_checkpoint(&self.checkpoint_directory)?
            .map(|loaded| loaded.checkpoint_file);
        ensure_checkpoint_unchanged(self.current.as_ref(), disk_current.as_ref())?;

        let previous_sequence = self
            .current
            .as_ref()
            .map_or(0, |current| current.content.through_commit_sequence);
        remove_orphan_candidates(&self.root, KnownOrphanCandidateKindV1::CurrentPointer)?;
        remove_orphan_candidates(
            &self.checkpoint_directory,
            KnownOrphanCandidateKindV1::ImmutableJson,
        )?;
        remove_orphan_candidates(
            &self.evidence_directory,
            KnownOrphanCandidateKindV1::ImmutableJson,
        )?;
        let sequence = previous_sequence
            .checked_add(1)
            .ok_or(PersistenceErrorV1::SequenceOverflow)?;
        let envelope = EvidenceEnvelopeV1 {
            schema_version: EVIDENCE_ENVELOPE_SCHEMA_V1.to_owned(),
            sequence,
            predecessor_evidence_sha256: self
                .current
                .as_ref()
                .map(|current| current.content.evidence_head_sha256.clone()),
            body: domain_evidence,
        };
        let evidence_bytes =
            serde_json::to_vec(&envelope).map_err(|error| json_error("编码证据提交信封", error))?;
        let evidence_sha256 = sha256(&evidence_bytes);
        let evidence_path = self
            .evidence_directory
            .join(evidence_file_name(sequence, &evidence_sha256));
        let evidence_candidate_path = self.evidence_directory.join(format!(
            ".{}.candidate",
            evidence_file_name(sequence, &evidence_sha256)
        ));
        if !scan_evidence_files(&self.evidence_directory)?
            .get(&sequence)
            .is_none_or(Vec::is_empty)
        {
            return Err(PersistenceErrorV1::Integrity(format!(
                "待提交证据序号 {sequence} 已存在正式文件"
            )));
        }
        write_synced_candidate(&evidence_candidate_path, &evidence_bytes)?;

        let content = CurrentCheckpointContentV1 {
            schema_version: CURRENT_CHECKPOINT_SCHEMA_V1.to_owned(),
            through_commit_sequence: sequence,
            evidence_head_sha256: evidence_sha256.clone(),
            checkpoint,
        };
        validate_checkpoint_content(&content)?;
        let checkpoint_sha256 = checkpoint_content_sha256(&content)?;
        let current = CurrentCheckpointFileV1 {
            content,
            checkpoint_sha256: checkpoint_sha256.clone(),
        };
        let checkpoint_bytes =
            serde_json::to_vec(&current).map_err(|error| json_error("编码不可变检查点", error))?;
        let checkpoint_path = self
            .checkpoint_directory
            .join(checkpoint_file_name(sequence, &checkpoint_sha256));
        let checkpoint_candidate_path = self.checkpoint_directory.join(format!(
            ".{}.candidate",
            checkpoint_file_name(sequence, &checkpoint_sha256)
        ));
        write_synced_candidate(&checkpoint_candidate_path, &checkpoint_bytes)?;

        if fault == CommitFaultV1::BeforeCheckpointPublish {
            return Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish);
        }

        rename_noreplace(&checkpoint_candidate_path, &checkpoint_path)?;
        sync_directory(&self.checkpoint_directory, &checkpoint_path)?;

        if fault == CommitFaultV1::AfterCheckpointBeforeEvidencePublish {
            return Err(PersistenceErrorV1::InjectedAfterCheckpointBeforeEvidencePublish);
        }

        rename_noreplace(&evidence_candidate_path, &evidence_path)?;
        sync_directory(&self.evidence_directory, &evidence_path)?;
        self.current = Some(current);
        if fault == CommitFaultV1::AfterEvidenceCommit {
            return Err(PersistenceErrorV1::InjectedAfterEvidenceCommit);
        }

        let current_path = self.root.join(CURRENT_FILE_NAME);
        // 正式证据完成目录同步就是本地事务提交点。CURRENT 与旧检查点回收均可由
        // 检查点和证据链重建；提交点之后不能因这两项辅助维护失败而把已提交事务
        // 向调用方误报为未提交。
        let _ = publish_current_pointer(&self.root, sequence, &checkpoint_sha256);
        let _ = remove_old_checkpoints(&self.checkpoint_directory, sequence);
        Ok(DurableCommitReceiptV1 {
            sequence,
            evidence_sha256,
            checkpoint_sha256,
            evidence_path,
            checkpoint_path,
            current_path,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitFaultV1 {
    None,
    BeforeCheckpointPublish,
    AfterCheckpointBeforeEvidencePublish,
    AfterEvidenceCommit,
}

struct RecoveredWithCurrentV1 {
    current_file: CurrentCheckpointFileV1,
    public: RecoveredDurabilityJsonV1,
}

struct LoadedCheckpointV1 {
    checkpoint_file: CurrentCheckpointFileV1,
}

fn recover_json_from(
    root: &Path,
    checkpoint_directory: &Path,
    evidence_directory: &Path,
) -> Result<Option<RecoveredWithCurrentV1>, PersistenceErrorV1> {
    let pointer = load_current_pointer(root)?;
    let files = scan_evidence_files(evidence_directory)?;
    let Some((&committed_sequence, _)) = files.last_key_value() else {
        if pointer.is_some() {
            return Err(PersistenceErrorV1::Integrity(
                "CURRENT 存在，但没有已提交证据".to_owned(),
            ));
        }
        return Ok(None);
    };
    let mut evidence = Vec::new();
    let mut predecessor_evidence_sha256 = None;
    let mut expected_sequence = 1_u64;
    for (&sequence, members) in &files {
        if sequence != expected_sequence {
            return Err(PersistenceErrorV1::Integrity(format!(
                "证据提交序号不连续：期望 {expected_sequence}，实际 {sequence}"
            )));
        }
        if members.len() != 1 {
            return Err(PersistenceErrorV1::Integrity(format!(
                "证据序号 {} 对应 {} 个不可变文件",
                sequence,
                members.len()
            )));
        }
        let member = &members[0];
        let json = fs::read(&member.path).map_err(|error| io_error("读取证据正文", error))?;
        let computed = sha256(&json);
        if computed != member.content_sha256 {
            return Err(PersistenceErrorV1::Integrity(format!(
                "证据序号 {} 的文件名摘要与正文不符",
                sequence
            )));
        }
        let envelope: EvidenceEnvelopeV1 = serde_json::from_slice(&json)
            .map_err(|error| json_error("解码已恢复证据提交信封", error))?;
        validate_evidence_envelope(&envelope, sequence, predecessor_evidence_sha256.as_ref())?;
        evidence.push(RecoveredEvidenceJsonV1 {
            sequence,
            content_sha256: member.content_sha256.clone(),
            envelope_byte_length: u64::try_from(json.len())
                .map_err(|_| PersistenceErrorV1::Integrity("证据文件字节数超出 u64".to_owned()))?,
            json: envelope.body.get().as_bytes().to_vec(),
        });
        predecessor_evidence_sha256 = Some(member.content_sha256.clone());
        if sequence != committed_sequence {
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(PersistenceErrorV1::SequenceOverflow)?;
        }
    }

    let checkpoint_files = scan_checkpoint_files(checkpoint_directory)?;
    if checkpoint_files.keys().any(|sequence| {
        *sequence > committed_sequence
            && committed_sequence
                .checked_add(1)
                .is_none_or(|next| *sequence != next)
    }) {
        return Err(PersistenceErrorV1::Integrity(
            "存在无法由一次未完成提交产生的超前检查点".to_owned(),
        ));
    }
    let checkpoint_members = checkpoint_files.get(&committed_sequence).ok_or_else(|| {
        PersistenceErrorV1::Integrity(format!(
            "已提交证据水位 {committed_sequence} 缺少同代不可变检查点"
        ))
    })?;
    let current = load_checkpoint_member(committed_sequence, checkpoint_members)?.checkpoint_file;

    if let Some(pointer) = &pointer {
        if pointer.through_commit_sequence > committed_sequence {
            return Err(PersistenceErrorV1::Integrity(
                "CURRENT 序号超过已提交证据水位".to_owned(),
            ));
        }
        if pointer.through_commit_sequence == committed_sequence
            && pointer.checkpoint_sha256 != current.checkpoint_sha256
        {
            return Err(PersistenceErrorV1::Integrity(
                "CURRENT 与同代不可变检查点摘要不符".to_owned(),
            ));
        }
    }

    if predecessor_evidence_sha256.as_ref() != Some(&current.content.evidence_head_sha256) {
        return Err(PersistenceErrorV1::Integrity(
            "检查点绑定的证据头与连续证据链不符".to_owned(),
        ));
    }

    let public = RecoveredDurabilityJsonV1 {
        through_commit_sequence: current.content.through_commit_sequence,
        checkpoint_sha256: current.checkpoint_sha256.clone(),
        checkpoint_json: current.content.checkpoint.get().as_bytes().to_vec(),
        evidence,
    };
    Ok(Some(RecoveredWithCurrentV1 {
        current_file: current,
        public,
    }))
}

fn load_current_pointer(
    root: &Path,
) -> Result<Option<CurrentCheckpointPointerV1>, PersistenceErrorV1> {
    Ok(load_current_pointer_with_bytes(root)?.map(|(pointer, _)| pointer))
}

fn current_pointer_file_bytes(root: &Path) -> Result<Option<u64>, PersistenceErrorV1> {
    Ok(load_current_pointer_with_bytes(root)?.map(|(_, bytes)| bytes))
}

fn load_current_pointer_with_bytes(
    root: &Path,
) -> Result<Option<(CurrentCheckpointPointerV1, u64)>, PersistenceErrorV1> {
    let current_path = root.join(CURRENT_FILE_NAME);
    let metadata = match fs::symlink_metadata(&current_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("读取 CURRENT 检查点元数据", error)),
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(PersistenceErrorV1::Integrity(format!(
            "CURRENT 不是普通文件：{}",
            current_path.display()
        )));
    }
    let current_bytes =
        fs::read(&current_path).map_err(|error| io_error("读取 CURRENT 检查点", error))?;
    let current: CurrentCheckpointPointerV1 = serde_json::from_slice(&current_bytes)
        .map_err(|error| json_error("解码 CURRENT 指针", error))?;
    validate_current_pointer(&current)?;
    let byte_length = u64::try_from(current_bytes.len())
        .map_err(|_| PersistenceErrorV1::Integrity("CURRENT 指针字节超出 u64".to_owned()))?;
    Ok(Some((current, byte_length)))
}

fn validate_current_pointer(
    current: &CurrentCheckpointPointerV1,
) -> Result<(), PersistenceErrorV1> {
    if current.schema_version != CURRENT_POINTER_SCHEMA_V1 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "不支持的 CURRENT 指针版本：{}",
            current.schema_version
        )));
    }
    if current.through_commit_sequence == 0 {
        return Err(PersistenceErrorV1::Integrity(
            "CURRENT 指针序号必须从 1 开始".to_owned(),
        ));
    }
    validate_sha256_text(&current.checkpoint_sha256.0)
}

fn validate_evidence_envelope(
    envelope: &EvidenceEnvelopeV1,
    expected_sequence: u64,
    expected_predecessor: Option<&Sha256Hex>,
) -> Result<(), PersistenceErrorV1> {
    if envelope.schema_version != EVIDENCE_ENVELOPE_SCHEMA_V1 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "不支持的证据提交信封版本：{}",
            envelope.schema_version
        )));
    }
    if envelope.sequence != expected_sequence {
        return Err(PersistenceErrorV1::Integrity(format!(
            "证据提交信封序号不连续：期望 {expected_sequence}，实际 {}",
            envelope.sequence
        )));
    }
    if envelope.predecessor_evidence_sha256.as_ref() != expected_predecessor {
        return Err(PersistenceErrorV1::Integrity(format!(
            "证据序号 {expected_sequence} 的前驱摘要不连续"
        )));
    }
    if let Some(predecessor) = &envelope.predecessor_evidence_sha256 {
        validate_sha256_text(&predecessor.0)?;
    }
    Ok(())
}

fn ensure_checkpoint_unchanged(
    cached: Option<&CurrentCheckpointFileV1>,
    disk: Option<&CurrentCheckpointFileV1>,
) -> Result<(), PersistenceErrorV1> {
    match (cached, disk) {
        (None, None) => Ok(()),
        (Some(cached), Some(disk)) if cached.checkpoint_sha256 == disk.checkpoint_sha256 => Ok(()),
        _ => Err(PersistenceErrorV1::Integrity(
            "最新不可变检查点在本存储句柄打开后发生变化".to_owned(),
        )),
    }
}

fn remove_checkpoints_after(
    checkpoint_directory: &Path,
    through_commit_sequence: u64,
) -> Result<(), PersistenceErrorV1> {
    let files = scan_checkpoint_files(checkpoint_directory)?;
    let mut removed = false;
    for (_, members) in files.range((
        std::ops::Bound::Excluded(through_commit_sequence),
        std::ops::Bound::Unbounded,
    )) {
        for member in members {
            fs::remove_file(&member.path)
                .map_err(|error| io_error("清理未提交检查点孤儿", error))?;
            removed = true;
        }
    }
    if removed {
        sync_directory(checkpoint_directory, checkpoint_directory)?;
    }
    Ok(())
}

fn load_latest_checkpoint(
    checkpoint_directory: &Path,
) -> Result<Option<LoadedCheckpointV1>, PersistenceErrorV1> {
    let files = scan_checkpoint_files(checkpoint_directory)?;
    let Some((&latest_sequence, members)) = files.last_key_value() else {
        return Ok(None);
    };
    load_checkpoint_member(latest_sequence, members).map(Some)
}

fn load_checkpoint_member(
    sequence: u64,
    members: &[EvidenceFileV1],
) -> Result<LoadedCheckpointV1, PersistenceErrorV1> {
    if members.len() != 1 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "不可变检查点序号 {sequence} 对应 {} 个文件",
            members.len()
        )));
    }
    let member = &members[0];
    let bytes = fs::read(&member.path).map_err(|error| io_error("读取不可变检查点", error))?;
    let checkpoint_file: CurrentCheckpointFileV1 =
        serde_json::from_slice(&bytes).map_err(|error| json_error("解码不可变检查点", error))?;
    validate_checkpoint_file(&checkpoint_file)?;
    if checkpoint_file.content.through_commit_sequence != sequence {
        return Err(PersistenceErrorV1::Integrity(
            "不可变检查点文件名序号与正文不符".to_owned(),
        ));
    }
    if checkpoint_file.checkpoint_sha256 != member.content_sha256 {
        return Err(PersistenceErrorV1::Integrity(
            "不可变检查点文件名摘要与正文不符".to_owned(),
        ));
    }
    Ok(LoadedCheckpointV1 { checkpoint_file })
}

fn publish_current_pointer(
    root: &Path,
    sequence: u64,
    checkpoint_sha256: &Sha256Hex,
) -> Result<PathBuf, PersistenceErrorV1> {
    let pointer = CurrentCheckpointPointerV1 {
        schema_version: CURRENT_POINTER_SCHEMA_V1.to_owned(),
        through_commit_sequence: sequence,
        checkpoint_sha256: checkpoint_sha256.clone(),
    };
    let bytes =
        serde_json::to_vec(&pointer).map_err(|error| json_error("编码 CURRENT 指针", error))?;
    let current_path = root.join(CURRENT_FILE_NAME);
    let candidate_path = root.join(format!(
        ".CURRENT-{sequence:020}-{}.candidate",
        checkpoint_sha256.0
    ));
    write_synced_candidate(&candidate_path, &bytes)?;
    replace_current(&candidate_path, &current_path, root)?;
    Ok(current_path)
}

fn remove_old_checkpoints(
    checkpoint_directory: &Path,
    latest_sequence: u64,
) -> Result<(), PersistenceErrorV1> {
    let files = scan_checkpoint_files(checkpoint_directory)?;
    let mut removed = false;
    for (&sequence, members) in &files {
        if sequence >= latest_sequence {
            continue;
        }
        for member in members {
            fs::remove_file(&member.path)
                .map_err(|error| io_error("清理旧不可变检查点正文", error))?;
            removed = true;
        }
    }
    if removed {
        sync_directory(checkpoint_directory, checkpoint_directory)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum KnownOrphanCandidateKindV1 {
    CurrentPointer,
    ImmutableJson,
}

struct KnownOrphanCandidatesV1 {
    paths: Vec<PathBuf>,
    file_count: u64,
    bytes: u64,
}

fn inspect_known_orphan_candidates(
    directory: &Path,
    kind: KnownOrphanCandidateKindV1,
) -> Result<KnownOrphanCandidatesV1, PersistenceErrorV1> {
    let entries = fs::read_dir(directory).map_err(|error| io_error("扫描候选文件", error))?;
    let mut candidates = KnownOrphanCandidatesV1 {
        paths: Vec::new(),
        file_count: 0,
        bytes: 0,
    };
    for entry in entries {
        let entry = entry.map_err(|error| io_error("读取候选目录项", error))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PersistenceErrorV1::Integrity("耐久目录项名称不是 UTF-8".to_owned()))?;
        if !name.starts_with('.') || !name.ends_with(".candidate") {
            continue;
        }
        let immutable_name = match kind {
            KnownOrphanCandidateKindV1::CurrentPointer => name
                .strip_prefix(".CURRENT-")
                .and_then(|name| name.strip_suffix(".candidate"))
                .map(|name| format!("{name}.json")),
            KnownOrphanCandidateKindV1::ImmutableJson => name
                .strip_prefix('.')
                .and_then(|name| name.strip_suffix(".candidate"))
                .map(ToOwned::to_owned),
        }
        .ok_or_else(|| {
            PersistenceErrorV1::Integrity(format!("耐久目录包含未知候选文件：{name}"))
        })?;
        parse_immutable_file_name(&immutable_name, "孤立候选").map_err(|_| {
            PersistenceErrorV1::Integrity(format!("耐久目录包含未知候选文件：{name}"))
        })?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取孤立候选文件类型", error))?;
        if !file_type.is_file() {
            return Err(PersistenceErrorV1::Integrity(format!(
                "耐久目录候选不是普通文件：{name}"
            )));
        }
        candidates.file_count = candidates
            .file_count
            .checked_add(1)
            .ok_or_else(|| PersistenceErrorV1::Integrity("孤立候选文件数量溢出".to_owned()))?;
        candidates.bytes = candidates
            .bytes
            .checked_add(
                entry
                    .metadata()
                    .map_err(|error| io_error("读取孤立候选文件元数据", error))?
                    .len(),
            )
            .ok_or_else(|| PersistenceErrorV1::Integrity("孤立候选文件字节溢出".to_owned()))?;
        candidates.paths.push(entry.path());
    }
    Ok(candidates)
}

fn remove_orphan_candidates(
    directory: &Path,
    kind: KnownOrphanCandidateKindV1,
) -> Result<(), PersistenceErrorV1> {
    let candidates = inspect_known_orphan_candidates(directory, kind)?;
    for path in &candidates.paths {
        fs::remove_file(path).map_err(|error| io_error("清理孤立候选文件", error))?;
    }
    if !candidates.paths.is_empty() {
        sync_directory(directory, directory)?;
    }
    Ok(())
}

fn has_orphan_candidates(directory: &Path) -> Result<bool, PersistenceErrorV1> {
    let entries = fs::read_dir(directory).map_err(|error| io_error("扫描候选文件", error))?;
    for entry in entries {
        let entry = entry.map_err(|error| io_error("读取候选目录项", error))?;
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|name| name.starts_with('.') && name.ends_with(".candidate"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn replace_current(
    candidate: &Path,
    current: &Path,
    root: &Path,
) -> Result<(), PersistenceErrorV1> {
    rename_replace(candidate, current)?;
    sync_directory(root, current)
}

fn directory_regular_file_usage(path: &Path) -> Result<(u64, u64), PersistenceErrorV1> {
    let mut file_count = 0_u64;
    let mut byte_count = 0_u64;
    for entry in fs::read_dir(path).map_err(|error| io_error("枚举资源计量目录", error))? {
        let entry = entry.map_err(|error| io_error("读取资源计量目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取资源计量目录项类型", error))?;
        if file_type.is_symlink() {
            return Err(PersistenceErrorV1::Integrity(format!(
                "资源计量目录包含符号链接：{}",
                entry.path().display()
            )));
        }
        if file_type.is_dir() {
            let (nested_files, nested_bytes) = directory_regular_file_usage(&entry.path())?;
            file_count = file_count
                .checked_add(nested_files)
                .ok_or_else(|| PersistenceErrorV1::Integrity("资源文件数量溢出".to_owned()))?;
            byte_count = byte_count
                .checked_add(nested_bytes)
                .ok_or_else(|| PersistenceErrorV1::Integrity("资源文件字节溢出".to_owned()))?;
        } else if file_type.is_file() {
            file_count = file_count
                .checked_add(1)
                .ok_or_else(|| PersistenceErrorV1::Integrity("资源文件数量溢出".to_owned()))?;
            byte_count = byte_count
                .checked_add(
                    entry
                        .metadata()
                        .map_err(|error| io_error("读取资源计量文件元数据", error))?
                        .len(),
                )
                .ok_or_else(|| PersistenceErrorV1::Integrity("资源文件字节溢出".to_owned()))?;
        } else {
            return Err(PersistenceErrorV1::Integrity(format!(
                "资源计量目录包含非普通文件：{}",
                entry.path().display()
            )));
        }
    }
    Ok((file_count, byte_count))
}

fn write_synced_candidate(path: &Path, bytes: &[u8]) -> Result<(), PersistenceErrorV1> {
    remove_candidate_if_present(path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| io_error("创建候选文件", error))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| io_error("写入并同步候选文件", error))
}

fn write_synced_new_file(
    path: &Path,
    bytes: &[u8],
    operation: &'static str,
) -> Result<(), PersistenceErrorV1> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| io_error(operation, error))?;
    file.write_all(bytes)
        .map_err(|error| io_error(operation, error))?;
    file.sync_all()
        .map_err(|error| PersistenceErrorV1::DurabilityUncertain {
            path: path.to_path_buf(),
            message: error.to_string(),
        })
}

fn artifact_member_names(
    members: &[ImmutableArtifactMemberV1],
) -> Result<std::collections::BTreeSet<&'static str>, PersistenceErrorV1> {
    let mut expected_names = std::collections::BTreeSet::new();
    for member in members {
        if member.file_name.is_empty()
            || member.file_name.starts_with('.')
            || member.file_name.contains('/')
            || member.file_name.contains('\\')
            || !expected_names.insert(member.file_name)
        {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物成员文件名非法或重复".to_owned(),
            ));
        }
    }
    Ok(expected_names)
}

fn verify_immutable_artifact_bundle(
    directory: &Path,
    members: &[ImmutableArtifactMemberV1],
) -> Result<(), PersistenceErrorV1> {
    ensure_directory(directory, "正式运行产物目录")?;
    let expected = members
        .iter()
        .map(|member| (member.file_name, member.bytes.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut actual_names = std::collections::BTreeSet::new();
    for entry in fs::read_dir(directory).map_err(|error| io_error("扫描正式运行产物目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取正式运行产物目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取正式运行产物成员类型", error))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PersistenceErrorV1::Integrity("运行产物文件名不是 UTF-8".to_owned()))?;
        if !file_type.is_file() || !actual_names.insert(name.clone()) {
            return Err(PersistenceErrorV1::Integrity(format!(
                "运行产物目录包含非普通或重复成员：{name}"
            )));
        }
        let expected_bytes = expected.get(name.as_str()).ok_or_else(|| {
            PersistenceErrorV1::Integrity(format!("运行产物目录包含未声明成员：{name}"))
        })?;
        let actual =
            fs::read(entry.path()).map_err(|error| io_error("读取正式运行产物成员", error))?;
        if actual.as_slice() != *expected_bytes {
            return Err(PersistenceErrorV1::Integrity(format!(
                "运行产物正式成员正文冲突：{name}"
            )));
        }
    }
    if actual_names != expected.keys().map(|name| (*name).to_owned()).collect() {
        return Err(PersistenceErrorV1::Integrity(
            "运行产物正式目录缺少声明成员".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ArtifactDirectoryEntriesV1 {
    target_exists: bool,
    candidate_identity: Option<Sha256Hex>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ArtifactIdentitiesV1 {
    formal_identity: Option<Sha256Hex>,
    candidate_identity: Option<Sha256Hex>,
}

fn inspect_artifact_identities(
    artifacts: &Path,
) -> Result<ArtifactIdentitiesV1, PersistenceErrorV1> {
    if !artifacts
        .try_exists()
        .map_err(|error| io_error("检查运行产物根目录", error))?
    {
        return Ok(ArtifactIdentitiesV1::default());
    }
    ensure_directory(artifacts, "运行产物目录")?;
    let mut result = ArtifactIdentitiesV1::default();
    for entry in fs::read_dir(artifacts).map_err(|error| io_error("扫描运行产物目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取运行产物目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取运行产物目录项类型", error))?;
        if !file_type.is_dir() {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物根包含非目录项".to_owned(),
            ));
        }
        let name = entry.file_name().into_string().map_err(|_| {
            PersistenceErrorV1::Integrity("运行产物目录项名称不是 UTF-8".to_owned())
        })?;
        let (raw_identity, identity_slot) = match name
            .strip_prefix('.')
            .and_then(|name| name.strip_suffix(".candidate"))
        {
            Some(identity) => (identity, &mut result.candidate_identity),
            None => (name.as_str(), &mut result.formal_identity),
        };
        validate_sha256_text(raw_identity)?;
        if identity_slot
            .replace(Sha256Hex(raw_identity.to_owned()))
            .is_some()
        {
            return Err(PersistenceErrorV1::Integrity(
                "运行产物根包含多个正式包或多个候选包".to_owned(),
            ));
        }
    }
    if let (Some(formal), Some(candidate)) = (&result.formal_identity, &result.candidate_identity)
        && formal != candidate
    {
        return Err(PersistenceErrorV1::Integrity(
            "运行产物根同时包含不同身份的正式包与候选包".to_owned(),
        ));
    }
    Ok(result)
}

fn inspect_artifact_directory(
    artifacts: &Path,
    bundle_identity: &Sha256Hex,
) -> Result<ArtifactDirectoryEntriesV1, PersistenceErrorV1> {
    if !artifacts
        .try_exists()
        .map_err(|error| io_error("检查运行产物根目录", error))?
    {
        return Ok(ArtifactDirectoryEntriesV1::default());
    }
    ensure_directory(artifacts, "运行产物目录")?;
    let mut result = ArtifactDirectoryEntriesV1::default();
    for entry in fs::read_dir(artifacts).map_err(|error| io_error("扫描运行产物目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取运行产物目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取运行产物目录项类型", error))?;
        let name = entry.file_name().into_string().map_err(|_| {
            PersistenceErrorV1::Integrity("运行产物目录项名称不是 UTF-8".to_owned())
        })?;
        if name == bundle_identity.0 {
            if !file_type.is_dir() {
                return Err(PersistenceErrorV1::Integrity(
                    "同身份正式运行产物不是普通目录".to_owned(),
                ));
            }
            result.target_exists = true;
        } else if let Some(candidate_identity) = name
            .strip_prefix('.')
            .and_then(|name| name.strip_suffix(".candidate"))
        {
            if !file_type.is_dir() {
                return Err(PersistenceErrorV1::Integrity(
                    "运行产物候选不是普通目录".to_owned(),
                ));
            }
            validate_sha256_text(candidate_identity)?;
            if result
                .candidate_identity
                .replace(Sha256Hex(candidate_identity.to_owned()))
                .is_some()
            {
                return Err(PersistenceErrorV1::Integrity(
                    "运行产物根包含多个候选包".to_owned(),
                ));
            }
        } else {
            return Err(PersistenceErrorV1::Integrity(format!(
                "运行产物根包含其他正式包或未知目录项：{name}"
            )));
        }
    }
    if result.target_exists
        && result
            .candidate_identity
            .as_ref()
            .is_some_and(|identity| identity != bundle_identity)
    {
        return Err(PersistenceErrorV1::Integrity(
            "运行产物根同时包含目标正式包与不同身份候选包".to_owned(),
        ));
    }
    Ok(result)
}

fn remove_known_artifact_candidate(
    candidate: &Path,
    expected_names: &std::collections::BTreeSet<&'static str>,
) -> Result<(), PersistenceErrorV1> {
    validate_and_measure_artifact_candidate(candidate, expected_names)?;
    for entry in fs::read_dir(candidate).map_err(|error| io_error("扫描运行产物候选目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取运行产物候选目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取运行产物候选成员类型", error))?;
        let name = entry.file_name().into_string().map_err(|_| {
            PersistenceErrorV1::Integrity("运行产物候选文件名不是 UTF-8".to_owned())
        })?;
        if !file_type.is_file() || !expected_names.contains(name.as_str()) {
            return Err(PersistenceErrorV1::Integrity(format!(
                "运行产物候选目录包含未声明或非普通成员：{name}"
            )));
        }
        fs::remove_file(entry.path()).map_err(|error| io_error("清理运行产物候选成员", error))?;
    }
    fs::remove_dir(candidate).map_err(|error| io_error("清理运行产物候选目录", error))
}

fn validate_and_measure_artifact_candidate(
    candidate: &Path,
    expected_names: &std::collections::BTreeSet<&'static str>,
) -> Result<(u64, u64), PersistenceErrorV1> {
    ensure_directory(candidate, "运行产物候选目录")?;
    let mut file_count = 0_u64;
    let mut byte_count = 0_u64;
    for entry in fs::read_dir(candidate).map_err(|error| io_error("扫描运行产物候选目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取运行产物候选目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取运行产物候选成员类型", error))?;
        let name = entry.file_name().into_string().map_err(|_| {
            PersistenceErrorV1::Integrity("运行产物候选文件名不是 UTF-8".to_owned())
        })?;
        if !file_type.is_file() || !expected_names.contains(name.as_str()) {
            return Err(PersistenceErrorV1::Integrity(format!(
                "运行产物候选目录包含未声明或非普通成员：{name}"
            )));
        }
        file_count = file_count
            .checked_add(1)
            .ok_or_else(|| PersistenceErrorV1::Integrity("运行产物候选文件数量溢出".to_owned()))?;
        byte_count = byte_count
            .checked_add(
                entry
                    .metadata()
                    .map_err(|error| io_error("读取运行产物候选成员元数据", error))?
                    .len(),
            )
            .ok_or_else(|| PersistenceErrorV1::Integrity("运行产物候选字节溢出".to_owned()))?;
    }
    Ok((file_count, byte_count))
}

fn remove_candidate_if_present(path: &Path) -> Result<(), PersistenceErrorV1> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("清理同身份孤立候选文件", error)),
    }
}

fn rename_noreplace(candidate: &Path, target: &Path) -> Result<(), PersistenceErrorV1> {
    let parent_path = same_parent(candidate, target)?;
    let parent = File::open(parent_path).map_err(|error| io_error("打开证据父目录", error))?;
    let candidate_name = candidate
        .file_name()
        .ok_or_else(|| PersistenceErrorV1::Integrity("候选证据文件名缺失".to_owned()))?;
    let target_name = target
        .file_name()
        .ok_or_else(|| PersistenceErrorV1::Integrity("目标证据文件名缺失".to_owned()))?;
    renameat_with(
        &parent,
        candidate_name,
        &parent,
        target_name,
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| io_error("原子发布不可变证据", error))
}

fn rename_replace(candidate: &Path, target: &Path) -> Result<(), PersistenceErrorV1> {
    let parent_path = same_parent(candidate, target)?;
    let parent = File::open(parent_path).map_err(|error| io_error("打开检查点父目录", error))?;
    let candidate_name = candidate
        .file_name()
        .ok_or_else(|| PersistenceErrorV1::Integrity("候选检查点文件名缺失".to_owned()))?;
    let target_name = target
        .file_name()
        .ok_or_else(|| PersistenceErrorV1::Integrity("CURRENT 文件名缺失".to_owned()))?;
    renameat_with(
        &parent,
        candidate_name,
        &parent,
        target_name,
        RenameFlags::empty(),
    )
    .map_err(|error| io_error("原子替换 CURRENT 检查点", error))
}

fn same_parent<'a>(left: &'a Path, right: &Path) -> Result<&'a Path, PersistenceErrorV1> {
    let left_parent = left
        .parent()
        .ok_or_else(|| PersistenceErrorV1::Integrity("候选文件父目录缺失".to_owned()))?;
    if right.parent() != Some(left_parent) {
        return Err(PersistenceErrorV1::Integrity(
            "候选文件与目标文件不在同一目录".to_owned(),
        ));
    }
    Ok(left_parent)
}

fn sync_directory(directory: &Path, published: &Path) -> Result<(), PersistenceErrorV1> {
    let directory_file =
        File::open(directory).map_err(|error| io_error("打开待同步目录", error))?;
    directory_file
        .sync_all()
        .map_err(|error| PersistenceErrorV1::DurabilityUncertain {
            path: published.to_path_buf(),
            message: error.to_string(),
        })
}

fn scan_evidence_files(
    evidence_directory: &Path,
) -> Result<BTreeMap<u64, Vec<EvidenceFileV1>>, PersistenceErrorV1> {
    scan_immutable_json_files(evidence_directory, "证据")
}

fn scan_checkpoint_files(
    checkpoint_directory: &Path,
) -> Result<BTreeMap<u64, Vec<EvidenceFileV1>>, PersistenceErrorV1> {
    scan_immutable_json_files(checkpoint_directory, "不可变检查点")
}

fn scan_immutable_json_files(
    directory: &Path,
    identity: &'static str,
) -> Result<BTreeMap<u64, Vec<EvidenceFileV1>>, PersistenceErrorV1> {
    let mut files: BTreeMap<u64, Vec<EvidenceFileV1>> = BTreeMap::new();
    let entries = fs::read_dir(directory).map_err(|error| io_error("扫描不可变文件目录", error))?;
    for entry in entries {
        let entry = entry.map_err(|error| io_error("读取不可变目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取证据目录项类型", error))?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| PersistenceErrorV1::Integrity("证据文件名不是 UTF-8".to_owned()))?;
        if name.starts_with('.') && name.ends_with(".candidate") {
            continue;
        }
        if !file_type.is_file() {
            return Err(PersistenceErrorV1::Integrity(format!(
                "{identity}目录包含非普通文件：{name}"
            )));
        }
        let (sequence, content_sha256) = parse_immutable_file_name(name, identity)?;
        files.entry(sequence).or_default().push(EvidenceFileV1 {
            path: entry.path(),
            content_sha256,
        });
    }
    Ok(files)
}

fn evidence_file_name(sequence: u64, content_sha256: &Sha256Hex) -> String {
    format!("{sequence:020}-{}.json", content_sha256.0)
}

fn checkpoint_file_name(sequence: u64, content_sha256: &Sha256Hex) -> String {
    format!("{sequence:020}-{}.json", content_sha256.0)
}

fn parse_immutable_file_name(
    name: &str,
    identity: &'static str,
) -> Result<(u64, Sha256Hex), PersistenceErrorV1> {
    let stem = name
        .strip_suffix(".json")
        .ok_or_else(|| PersistenceErrorV1::Integrity(format!("非法{identity}文件名：{name}")))?;
    let (sequence, digest) = stem
        .split_once('-')
        .ok_or_else(|| PersistenceErrorV1::Integrity(format!("非法{identity}文件名：{name}")))?;
    if sequence.len() != 20 || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PersistenceErrorV1::Integrity(format!(
            "非法{identity}序号文件名：{name}"
        )));
    }
    let sequence = sequence
        .parse::<u64>()
        .map_err(|_| PersistenceErrorV1::Integrity(format!("{identity}序号超出 u64：{name}")))?;
    if sequence == 0 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "{identity}序号必须从 1 开始"
        )));
    }
    validate_sha256_text(digest)?;
    Ok((sequence, Sha256Hex(digest.to_owned())))
}

fn validate_checkpoint_file(current: &CurrentCheckpointFileV1) -> Result<(), PersistenceErrorV1> {
    validate_checkpoint_content(&current.content)?;
    validate_sha256_text(&current.checkpoint_sha256.0)?;
    let computed = checkpoint_content_sha256(&current.content)?;
    if current.checkpoint_sha256 != computed {
        return Err(PersistenceErrorV1::Integrity(
            "CURRENT 检查点摘要与完整正文不符".to_owned(),
        ));
    }
    Ok(())
}

fn validate_checkpoint_content(
    content: &CurrentCheckpointContentV1,
) -> Result<(), PersistenceErrorV1> {
    if content.schema_version != CURRENT_CHECKPOINT_SCHEMA_V1 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "不支持的 CURRENT 检查点版本：{}",
            content.schema_version
        )));
    }
    if content.through_commit_sequence == 0 {
        return Err(PersistenceErrorV1::Integrity(
            "不可变检查点序号必须从 1 开始".to_owned(),
        ));
    }
    validate_sha256_text(&content.evidence_head_sha256.0)
}

fn checkpoint_content_sha256(
    content: &CurrentCheckpointContentV1,
) -> Result<Sha256Hex, PersistenceErrorV1> {
    let bytes =
        serde_json::to_vec(content).map_err(|error| json_error("编码检查点摘要正文", error))?;
    Ok(sha256(&bytes))
}

fn encode_commit_candidate_v1(
    previous_current: Option<&CurrentCheckpointFileV1>,
    evidence_json: &[u8],
    checkpoint_json: &[u8],
) -> Result<EncodedCommitCandidateV1, PersistenceErrorV1> {
    let domain_evidence = parse_raw_json(evidence_json, "预验证领域证据正文")?;
    let checkpoint = parse_raw_json(checkpoint_json, "预验证检查点正文")?;
    let previous_sequence =
        previous_current.map_or(0, |current| current.content.through_commit_sequence);
    let sequence = previous_sequence
        .checked_add(1)
        .ok_or(PersistenceErrorV1::SequenceOverflow)?;
    let envelope = EvidenceEnvelopeV1 {
        schema_version: EVIDENCE_ENVELOPE_SCHEMA_V1.to_owned(),
        sequence,
        predecessor_evidence_sha256: previous_current
            .map(|current| current.content.evidence_head_sha256.clone()),
        body: domain_evidence,
    };
    let evidence_bytes =
        serde_json::to_vec(&envelope).map_err(|error| json_error("预编码证据提交信封", error))?;
    let evidence_sha256 = sha256(&evidence_bytes);
    let content = CurrentCheckpointContentV1 {
        schema_version: CURRENT_CHECKPOINT_SCHEMA_V1.to_owned(),
        through_commit_sequence: sequence,
        evidence_head_sha256: evidence_sha256,
        checkpoint,
    };
    validate_checkpoint_content(&content)?;
    let current = CurrentCheckpointFileV1 {
        checkpoint_sha256: checkpoint_content_sha256(&content)?,
        content,
    };
    let checkpoint_bytes =
        serde_json::to_vec(&current).map_err(|error| json_error("预编码不可变检查点", error))?;
    let pointer = CurrentCheckpointPointerV1 {
        schema_version: CURRENT_POINTER_SCHEMA_V1.to_owned(),
        through_commit_sequence: sequence,
        checkpoint_sha256: current.checkpoint_sha256.clone(),
    };
    let pointer_bytes =
        serde_json::to_vec(&pointer).map_err(|error| json_error("预编码 CURRENT 指针", error))?;
    Ok(EncodedCommitCandidateV1 {
        current,
        evidence_bytes,
        checkpoint_bytes,
        pointer_bytes,
    })
}

fn preview_candidate_from_steady_v1(
    steady: DurableResourcePreviewV1,
    candidate: &EncodedCommitCandidateV1,
) -> Result<DurableResourcePreviewV1, PersistenceErrorV1> {
    let evidence_bytes = u64::try_from(candidate.evidence_bytes.len())
        .map_err(|_| PersistenceErrorV1::Integrity("证据候选字节超出 u64".to_owned()))?;
    let checkpoint_bytes = u64::try_from(candidate.checkpoint_bytes.len())
        .map_err(|_| PersistenceErrorV1::Integrity("检查点候选字节超出 u64".to_owned()))?;
    let pointer_bytes = u64::try_from(candidate.pointer_bytes.len())
        .map_err(|_| PersistenceErrorV1::Integrity("CURRENT 候选字节超出 u64".to_owned()))?;
    let runtime_candidate_bytes = evidence_bytes
        .checked_add(checkpoint_bytes)
        .and_then(|subtotal| subtotal.checked_add(pointer_bytes))
        .ok_or_else(|| PersistenceErrorV1::Integrity("候选耐久字节溢出".to_owned()))?;
    Ok(DurableResourcePreviewV1 {
        evidence_file_count: steady
            .evidence_file_count
            .checked_add(1)
            .ok_or_else(|| PersistenceErrorV1::Integrity("证据文件数量溢出".to_owned()))?,
        evidence_bytes: steady
            .evidence_bytes
            .checked_add(evidence_bytes)
            .ok_or_else(|| PersistenceErrorV1::Integrity("证据字节溢出".to_owned()))?,
        checkpoint_file_count: steady
            .checkpoint_file_count
            .checked_add(1)
            .ok_or_else(|| PersistenceErrorV1::Integrity("检查点文件数量溢出".to_owned()))?,
        checkpoint_bytes: steady
            .checkpoint_bytes
            .checked_add(checkpoint_bytes)
            .ok_or_else(|| PersistenceErrorV1::Integrity("检查点字节溢出".to_owned()))?,
        artifact_file_count: steady.artifact_file_count,
        artifact_bytes: steady.artifact_bytes,
        runtime_root_file_count: steady
            .runtime_root_file_count
            .checked_add(3)
            .ok_or_else(|| PersistenceErrorV1::Integrity("运行根文件数量溢出".to_owned()))?,
        runtime_root_bytes: steady
            .runtime_root_bytes
            .checked_add(runtime_candidate_bytes)
            .ok_or_else(|| PersistenceErrorV1::Integrity("运行根字节溢出".to_owned()))?,
    })
}

fn parse_raw_json(
    bytes: &[u8],
    operation: &'static str,
) -> Result<Box<RawValue>, PersistenceErrorV1> {
    serde_json::from_slice(bytes).map_err(|error| json_error(operation, error))
}

fn validate_sha256_text(value: &str) -> Result<(), PersistenceErrorV1> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PersistenceErrorV1::Integrity(format!(
            "非法 SHA-256：{value}"
        )));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

impl LocalTransactionLogStoreV2 {
    pub(crate) fn create_new(
        root: PathBuf,
        identity: TransactionLogIdentityV2,
    ) -> Result<Self, PersistenceErrorV1> {
        validate_transaction_log_run_id_v2(&identity.run_id)?;
        fs::create_dir_all(&root).map_err(|error| io_error("创建 V2 运行耐久目录", error))?;
        ensure_directory(&root, "V2 运行耐久目录")?;
        let root =
            fs::canonicalize(&root).map_err(|error| io_error("规范化 V2 运行耐久目录", error))?;
        reject_legacy_material_v2(&root)?;
        let database_path = root.join(TRANSACTION_LOG_DATABASE_NAME_V2);
        reject_orphan_transaction_log_sidecars_v2(&database_path)?;
        if path_entry_exists_no_follow_v2(&database_path)? {
            return Err(PersistenceErrorV1::Integrity(format!(
                "新的 V2 运行根已经包含事务日志：{}",
                database_path.display()
            )));
        }
        let lock_file = open_exclusive_root_lock_v2(&root)?;
        let connection = open_transaction_log_connection_v2(&database_path)?;
        connection
            .execute_batch(
                "CREATE TABLE run_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    durability_profile TEXT NOT NULL,
                    last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
                    transaction_head_sha256 TEXT,
                    logical_log_bytes INTEGER NOT NULL CHECK (logical_log_bytes >= 0)
                ) STRICT;
                CREATE TABLE transaction_log (
                    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
                    predecessor_sha256 TEXT,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes >= 0)
                ) STRICT;
                CREATE TABLE application_checkpoint (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    through_sequence INTEGER NOT NULL CHECK (through_sequence >= 0),
                    transaction_head_sha256 TEXT,
                    checkpoint_sha256 TEXT NOT NULL,
                    checkpoint_json BLOB NOT NULL
                ) STRICT;",
            )
            .map_err(|error| sqlite_error("创建 V2 事务日志数据模式", error))?;
        connection
            .execute(
                "INSERT INTO run_metadata(
                    singleton, schema_version, run_id, durability_profile, last_sequence,
                    transaction_head_sha256, logical_log_bytes
                ) VALUES (1, ?1, ?2, ?3, 0, NULL, 0)",
                params![
                    TRANSACTION_LOG_SCHEMA_V2,
                    identity.run_id,
                    identity.durability_profile.as_str(),
                ],
            )
            .map_err(|error| sqlite_error("初始化 V2 事务日志元数据", error))?;
        Ok(Self {
            database_path,
            clean_run_append_open: identity.durability_profile == DurabilityProfileV2::CleanRun,
            identity,
            connection,
            _lock_file: lock_file,
        })
    }

    pub(crate) fn open_recovery(root: PathBuf) -> Result<Self, PersistenceErrorV1> {
        ensure_directory(&root, "V2 运行耐久目录")?;
        let root =
            fs::canonicalize(&root).map_err(|error| io_error("规范化 V2 运行耐久目录", error))?;
        reject_legacy_material_v2(&root)?;
        let database_path = root.join(TRANSACTION_LOG_DATABASE_NAME_V2);
        reject_orphan_transaction_log_sidecars_v2(&database_path)?;
        let metadata = fs::symlink_metadata(&database_path)
            .map_err(|error| io_error("读取 V2 事务日志元数据", error))?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 事务日志不是普通文件：{}",
                database_path.display()
            )));
        }
        let lock_file = open_exclusive_root_lock_v2(&root)?;
        let connection = open_transaction_log_connection_v2(&database_path)?;
        let identity = load_transaction_log_identity_v2(&connection)?;
        let quick_check: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(|error| sqlite_error("执行 V2 事务日志 quick_check", error))?;
        if quick_check != "ok" {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 事务日志 quick_check 失败：{quick_check}"
            )));
        }
        Ok(Self {
            database_path,
            identity,
            connection,
            clean_run_append_open: false,
            _lock_file: lock_file,
        })
    }

    pub(crate) fn append_transaction<Evidence, RecoveryOperation>(
        &mut self,
        evidence: &Evidence,
        recovery_operation: &RecoveryOperation,
    ) -> Result<TransactionLogCommitReceiptV2, PersistenceErrorV1>
    where
        Evidence: Serialize,
        RecoveryOperation: Serialize,
    {
        let record = encode_transaction_record_v2(evidence, recovery_operation)?;
        self.append_encoded_transaction(record)
    }

    pub(crate) fn append_encoded_transaction(
        &mut self,
        record: EncodedTransactionRecordV2,
    ) -> Result<TransactionLogCommitReceiptV2, PersistenceErrorV1> {
        self.require_profile(DurabilityProfileV2::RecoverablePaper)?;
        let database_path = self.database_path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("开始 V2 业务事务", error))?;
        let receipt = append_encoded_batch_v2(&transaction, vec![record])?
            .pop()
            .expect("单项 V2 事务批次必须有回执");
        transaction
            .commit()
            .map_err(|error| sqlite_commit_uncertain(&database_path, "提交 V2 业务事务", error))?;
        Ok(receipt)
    }

    pub(crate) fn append_transaction_and_application_checkpoint<
        Evidence,
        RecoveryOperation,
        Checkpoint,
    >(
        &mut self,
        evidence: &Evidence,
        recovery_operation: &RecoveryOperation,
        checkpoint: &Checkpoint,
    ) -> Result<(TransactionLogCommitReceiptV2, Sha256Hex), PersistenceErrorV1>
    where
        Evidence: Serialize,
        RecoveryOperation: Serialize,
        Checkpoint: Serialize,
    {
        let record = encode_transaction_record_v2(evidence, recovery_operation)?;
        self.append_encoded_transaction_and_application_checkpoint(record, checkpoint)
    }

    pub(crate) fn append_encoded_transaction_and_application_checkpoint<Checkpoint>(
        &mut self,
        record: EncodedTransactionRecordV2,
        checkpoint: &Checkpoint,
    ) -> Result<(TransactionLogCommitReceiptV2, Sha256Hex), PersistenceErrorV1>
    where
        Checkpoint: Serialize,
    {
        self.require_profile(DurabilityProfileV2::RecoverablePaper)?;
        let checkpoint_json = serde_json::to_vec(checkpoint)
            .map_err(|error| json_error("编码 V2 同事务应用检查点", error))?;
        let checkpoint_sha256 = sha256(&checkpoint_json);
        let database_path = self.database_path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("开始 V2 业务与检查点事务", error))?;
        let receipt = append_encoded_batch_v2(&transaction, vec![record])?
            .pop()
            .expect("单项 V2 事务与检查点批次必须有回执");
        store_application_checkpoint_v2(
            &transaction,
            receipt.sequence,
            &checkpoint_json,
            &checkpoint_sha256,
        )?;
        transaction.commit().map_err(|error| {
            sqlite_commit_uncertain(&database_path, "提交 V2 业务与检查点事务", error)
        })?;
        Ok((receipt, checkpoint_sha256))
    }

    pub(crate) fn append_clean_run_batch(
        &mut self,
        records: Vec<EncodedTransactionRecordV2>,
    ) -> Result<Vec<TransactionLogCommitReceiptV2>, PersistenceErrorV1> {
        self.require_profile(DurabilityProfileV2::CleanRun)?;
        self.require_clean_run_append_open()?;
        if records.is_empty() {
            return Err(PersistenceErrorV1::Integrity(
                "CleanRun 耐久批次不能为空".to_owned(),
            ));
        }
        let database_path = self.database_path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("开始 CleanRun 耐久批次", error))?;
        ensure_clean_run_not_completed_v2(&transaction)?;
        let receipts = append_encoded_batch_v2(&transaction, records)?;
        transaction.commit().map_err(|error| {
            sqlite_commit_uncertain(&database_path, "提交 CleanRun 耐久批次", error)
        })?;
        Ok(receipts)
    }

    pub(crate) fn append_clean_run_final_batch<Checkpoint>(
        &mut self,
        records: Vec<EncodedTransactionRecordV2>,
        checkpoint: &Checkpoint,
    ) -> Result<(Vec<TransactionLogCommitReceiptV2>, Sha256Hex), PersistenceErrorV1>
    where
        Checkpoint: Serialize,
    {
        self.require_profile(DurabilityProfileV2::CleanRun)?;
        self.require_clean_run_append_open()?;
        if records.is_empty() {
            return Err(PersistenceErrorV1::Integrity(
                "CleanRun 终态批次必须包含完成事实".to_owned(),
            ));
        }
        let checkpoint_json = serde_json::to_vec(checkpoint)
            .map_err(|error| json_error("编码 CleanRun 终态应用检查点", error))?;
        let checkpoint_sha256 = sha256(&checkpoint_json);
        let database_path = self.database_path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("开始 CleanRun 终态耐久批次", error))?;
        ensure_clean_run_not_completed_v2(&transaction)?;
        let receipts = append_encoded_batch_v2(&transaction, records)?;
        let last = receipts.last().expect("非空 CleanRun 终态批次必须有回执");
        store_application_checkpoint_v2(
            &transaction,
            last.sequence,
            &checkpoint_json,
            &checkpoint_sha256,
        )?;
        transaction.commit().map_err(|error| {
            sqlite_commit_uncertain(&database_path, "提交 CleanRun 终态耐久批次", error)
        })?;
        Ok((receipts, checkpoint_sha256))
    }

    pub(crate) fn publish_application_checkpoint<Checkpoint>(
        &mut self,
        through_sequence: u64,
        checkpoint: &Checkpoint,
    ) -> Result<Sha256Hex, PersistenceErrorV1>
    where
        Checkpoint: Serialize,
    {
        self.require_profile(DurabilityProfileV2::RecoverablePaper)?;
        let checkpoint_json = serde_json::to_vec(checkpoint)
            .map_err(|error| json_error("编码 V2 周期应用检查点", error))?;
        let checkpoint_sha256 = sha256(&checkpoint_json);
        let database_path = self.database_path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("开始 V2 应用检查点事务", error))?;
        store_application_checkpoint_v2(
            &transaction,
            through_sequence,
            &checkpoint_json,
            &checkpoint_sha256,
        )?;
        transaction.commit().map_err(|error| {
            sqlite_commit_uncertain(&database_path, "提交 V2 周期应用检查点", error)
        })?;
        Ok(checkpoint_sha256)
    }

    pub(crate) fn recover<Evidence, RecoveryOperation, Checkpoint>(
        &self,
    ) -> Result<
        RecoveredTransactionLogV2<Evidence, RecoveryOperation, Checkpoint>,
        PersistenceErrorV1,
    >
    where
        Evidence: DeserializeOwned,
        RecoveryOperation: DeserializeOwned,
        Checkpoint: DeserializeOwned,
    {
        self.require_profile(DurabilityProfileV2::RecoverablePaper)?;
        let header = load_transaction_log_recovery_header_v2::<Checkpoint>(
            &self.connection,
            &self.identity,
        )?;
        let through_sequence = header
            .checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.through_sequence);
        let predecessor = header
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.transaction_head_sha256.clone());
        let tail = recover_transaction_entries_after_v2(
            &self.connection,
            through_sequence,
            predecessor,
            header.last_sequence,
            header.transaction_head_sha256,
        )?;
        Ok(RecoveredTransactionLogV2 {
            checkpoint: header.checkpoint,
            tail,
        })
    }

    pub(crate) fn visit_completed_clean_run_for_artifacts<
        Evidence,
        RecoveryOperation,
        Checkpoint,
        Visitor,
    >(
        &self,
        visitor: Visitor,
    ) -> Result<Option<RecoveredApplicationCheckpointV2<Checkpoint>>, PersistenceErrorV1>
    where
        Evidence: DeserializeOwned,
        RecoveryOperation: DeserializeOwned,
        Checkpoint: DeserializeOwned,
        Visitor: FnMut(
            RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation>,
        ) -> Result<(), PersistenceErrorV1>,
    {
        self.require_profile(DurabilityProfileV2::CleanRun)?;
        let header = load_transaction_log_recovery_header_v2::<Checkpoint>(
            &self.connection,
            &self.identity,
        )?;
        let Some(checkpoint) = header.checkpoint else {
            return Ok(None);
        };
        if checkpoint.through_sequence != header.last_sequence
            || checkpoint.transaction_head_sha256 != header.transaction_head_sha256
        {
            return Err(PersistenceErrorV1::Integrity(
                "CleanRun 终态检查点未覆盖完整事务日志".to_owned(),
            ));
        }
        visit_transaction_entries_after_v2(
            &self.connection,
            0,
            None,
            header.last_sequence,
            header.transaction_head_sha256,
            visitor,
        )?;
        Ok(Some(checkpoint))
    }

    pub(crate) fn visit_checkpointed_paper_prefix_for_artifacts<
        Evidence,
        RecoveryOperation,
        Checkpoint,
        Visitor,
    >(
        &self,
        visitor: Visitor,
    ) -> Result<RecoveredApplicationCheckpointV2<Checkpoint>, PersistenceErrorV1>
    where
        Evidence: DeserializeOwned,
        RecoveryOperation: DeserializeOwned,
        Checkpoint: DeserializeOwned,
        Visitor: FnMut(
            RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation>,
        ) -> Result<(), PersistenceErrorV1>,
    {
        self.require_profile(DurabilityProfileV2::RecoverablePaper)?;
        let header = load_transaction_log_recovery_header_v2::<Checkpoint>(
            &self.connection,
            &self.identity,
        )?;
        let checkpoint = header.checkpoint.ok_or_else(|| {
            PersistenceErrorV1::Integrity("RecoverablePaper 产物封口要求完整应用检查点".to_owned())
        })?;
        if checkpoint.through_sequence != header.last_sequence
            || checkpoint.transaction_head_sha256 != header.transaction_head_sha256
        {
            return Err(PersistenceErrorV1::Integrity(
                "RecoverablePaper 应用检查点未覆盖完整事务日志".to_owned(),
            ));
        }
        visit_transaction_entries_after_v2(
            &self.connection,
            0,
            None,
            header.last_sequence,
            header.transaction_head_sha256,
            visitor,
        )?;
        Ok(checkpoint)
    }

    pub(crate) fn export_final_evidence_v2<Evidence, RecoveryOperation, Checkpoint, Visitor>(
        &self,
        expected_through_sequence: u64,
        expected_checkpoint: &Checkpoint,
        mut visitor: Visitor,
    ) -> Result<FinalEvidenceExportReceiptV2, PersistenceErrorV1>
    where
        Evidence: DeserializeOwned + Serialize,
        RecoveryOperation: DeserializeOwned,
        Checkpoint: DeserializeOwned + PartialEq,
        Visitor: FnMut(u64, &Sha256Hex, u64, &Evidence) -> Result<(), PersistenceErrorV1>,
    {
        let header = load_transaction_log_recovery_header_v2::<Checkpoint>(
            &self.connection,
            &self.identity,
        )?;
        let checkpoint = header.checkpoint.as_ref().ok_or_else(|| {
            PersistenceErrorV1::Integrity(
                "V2 最终证据导出要求覆盖完整事务日志的应用检查点".to_owned(),
            )
        })?;
        if checkpoint.through_sequence != header.last_sequence
            || checkpoint.transaction_head_sha256 != header.transaction_head_sha256
        {
            return Err(PersistenceErrorV1::Integrity(
                "V2 最终证据导出应用检查点未覆盖完整事务日志".to_owned(),
            ));
        }
        if checkpoint.through_sequence != expected_through_sequence {
            return Err(PersistenceErrorV1::Integrity(
                "V2 最终证据导出的提交序号与调用方封口序号不一致".to_owned(),
            ));
        }
        if checkpoint.checkpoint != *expected_checkpoint {
            return Err(PersistenceErrorV1::Integrity(
                "V2 最终证据导出的应用检查点与调用方封口检查点不一致".to_owned(),
            ));
        }

        let root = self
            .database_path
            .parent()
            .ok_or_else(|| PersistenceErrorV1::Integrity("V2 事务日志缺少运行根目录".to_owned()))?;
        ensure_directory(root, "V2 运行耐久目录")?;
        let target = root.join(FINAL_EVIDENCE_DIRECTORY_NAME_V2);
        let candidate = root.join(FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2);

        if path_entry_exists_no_follow_v2(&target)? {
            ensure_directory(&target, "V2 正式最终证据目录")?;
            if remove_owned_final_evidence_candidate_v2(&candidate)? {
                sync_directory(root, root)?;
            }
            validate_final_evidence_directory_shape_v2(&target, header.last_sequence)?;
            return stream_final_evidence_v2::<Evidence, RecoveryOperation, _>(
                &self.connection,
                header.last_sequence,
                header.transaction_head_sha256,
                |sequence,
                 file_sha256,
                 envelope_byte_length,
                 evidence,
                 envelope_bytes,
                 file_name| {
                    let path = target.join(file_name);
                    let metadata = fs::symlink_metadata(&path)
                        .map_err(|error| io_error("读取 V2 正式证据文件元数据", error))?;
                    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                        return Err(PersistenceErrorV1::Integrity(format!(
                            "V2 正式证据成员不是普通文件：{}",
                            path.display()
                        )));
                    }
                    let actual =
                        fs::read(&path).map_err(|error| io_error("读取 V2 正式证据成员", error))?;
                    let actual_sha256 = sha256(&actual);
                    let actual_byte_length = u64::try_from(actual.len()).map_err(|_| {
                        PersistenceErrorV1::Integrity("V2 正式证据成员字节超出 u64".to_owned())
                    })?;
                    if actual_sha256 != *file_sha256
                        || actual_byte_length != envelope_byte_length
                        || actual != envelope_bytes
                    {
                        return Err(PersistenceErrorV1::Integrity(format!(
                            "V2 正式证据成员正文或文件摘要冲突：{file_name}"
                        )));
                    }
                    visitor(sequence, &actual_sha256, actual_byte_length, evidence)
                },
            );
        }

        if remove_owned_final_evidence_candidate_v2(&candidate)? {
            sync_directory(root, root)?;
        }
        fs::create_dir(&candidate).map_err(|error| io_error("创建 V2 最终证据候选目录", error))?;
        let export = stream_final_evidence_v2::<Evidence, RecoveryOperation, _>(
            &self.connection,
            header.last_sequence,
            header.transaction_head_sha256,
            |sequence, file_sha256, envelope_byte_length, evidence, envelope_bytes, file_name| {
                write_synced_new_file(
                    &candidate.join(file_name),
                    envelope_bytes,
                    "写入 V2 最终证据候选成员",
                )?;
                visitor(sequence, file_sha256, envelope_byte_length, evidence)
            },
        );
        let receipt = match export {
            Ok(receipt) => receipt,
            Err(error) => {
                remove_owned_final_evidence_candidate_v2(&candidate)?;
                sync_directory(root, root)?;
                return Err(error);
            }
        };
        sync_directory(&candidate, &candidate)?;
        rename_noreplace(&candidate, &target)?;
        sync_directory(root, &target)?;
        Ok(receipt)
    }

    pub(crate) fn logical_log_bytes(&self) -> Result<u64, PersistenceErrorV1> {
        let value: i64 = self
            .connection
            .query_row(
                "SELECT logical_log_bytes FROM run_metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|error| sqlite_error("读取 V2 逻辑日志字节", error))?;
        sqlite_sequence_to_u64_v2(value)
    }

    #[cfg(test)]
    pub(crate) fn install_transaction_log_insert_failure_for_tests(
        &self,
    ) -> Result<(), PersistenceErrorV1> {
        self.connection
            .execute_batch(
                "CREATE TEMP TRIGGER sim_test_reject_transaction_log_insert
                 BEFORE INSERT ON transaction_log
                 BEGIN
                   SELECT RAISE(ABORT, 'injected transaction_log insert failure');
                 END;",
            )
            .map_err(|error| sqlite_error("安装 V2 事务记录插入失败测试触发器", error))
    }

    #[cfg(test)]
    pub(crate) fn remove_transaction_log_insert_failure_for_tests(
        &self,
    ) -> Result<(), PersistenceErrorV1> {
        self.connection
            .execute_batch("DROP TRIGGER sim_test_reject_transaction_log_insert;")
            .map_err(|error| sqlite_error("移除 V2 事务记录插入失败测试触发器", error))
    }

    pub(crate) fn identity(&self) -> &TransactionLogIdentityV2 {
        &self.identity
    }

    fn require_profile(&self, expected: DurabilityProfileV2) -> Result<(), PersistenceErrorV1> {
        if self.identity.durability_profile != expected {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 耐久接口档位不符：期望 {}，实际 {}",
                expected.as_str(),
                self.identity.durability_profile.as_str()
            )));
        }
        Ok(())
    }

    fn require_clean_run_append_open(&self) -> Result<(), PersistenceErrorV1> {
        if !self.clean_run_append_open {
            return Err(PersistenceErrorV1::Integrity(
                "通过恢复入口打开的 CleanRun 禁止继续追加业务记录".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn try_checkpoint_wal_after_application_checkpoint(
        &mut self,
    ) -> Result<bool, PersistenceErrorV1> {
        self.require_profile(DurabilityProfileV2::RecoverablePaper)?;
        let application_checkpoint_exists = application_checkpoint_exists_v2(&self.connection)?;
        if !application_checkpoint_exists {
            return Err(PersistenceErrorV1::Integrity(
                "RecoverablePaper WAL 回收必须晚于首个完整应用检查点".to_owned(),
            ));
        }
        try_checkpoint_wal_v2(&self.connection)
    }

    pub(crate) fn try_checkpoint_clean_run_wal_between_batches(
        &mut self,
    ) -> Result<bool, PersistenceErrorV1> {
        self.require_profile(DurabilityProfileV2::CleanRun)?;
        self.require_clean_run_append_open()?;
        if application_checkpoint_exists_v2(&self.connection)? {
            return Err(PersistenceErrorV1::Integrity(
                "CleanRun 终态应用检查点发布后不再执行批次间 WAL 回收".to_owned(),
            ));
        }
        try_checkpoint_wal_v2(&self.connection)
    }

    #[cfg(test)]
    fn database_path(&self) -> &Path {
        &self.database_path
    }
}

fn application_checkpoint_exists_v2(connection: &Connection) -> Result<bool, PersistenceErrorV1> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM application_checkpoint WHERE singleton = 1)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("核对 V2 应用检查点", error))
}

fn try_checkpoint_wal_v2(connection: &Connection) -> Result<bool, PersistenceErrorV1> {
    let (busy, _log_pages, _checkpointed_pages): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| sqlite_error("执行 V2 WAL 检查点与回收", error))?;
    match busy {
        0 => Ok(true),
        1 => Ok(false),
        value => Err(PersistenceErrorV1::Integrity(format!(
            "V2 WAL 检查点返回未知阻塞状态：{value}"
        ))),
    }
}

fn load_transaction_log_recovery_header_v2<Checkpoint>(
    connection: &Connection,
    identity: &TransactionLogIdentityV2,
) -> Result<TransactionLogRecoveryHeaderV2<Checkpoint>, PersistenceErrorV1>
where
    Checkpoint: DeserializeOwned,
{
    validate_transaction_log_schema_v2(connection, identity)?;
    let (last_sequence, metadata_head): (i64, Option<String>) = connection
        .query_row(
            "SELECT last_sequence, transaction_head_sha256
             FROM run_metadata WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_error("读取 V2 恢复元数据", error))?;
    let last_sequence = sqlite_sequence_to_u64_v2(last_sequence)?;
    let metadata_head = metadata_head
        .map(Sha256Hex)
        .map(validate_sha256_v2)
        .transpose()?;
    if (last_sequence == 0) != metadata_head.is_none() {
        return Err(PersistenceErrorV1::Integrity(
            "V2 恢复元数据序号与事务头不一致".to_owned(),
        ));
    }
    let checkpoint_row: Option<(i64, Option<String>, String, Vec<u8>)> = connection
        .query_row(
            "SELECT through_sequence, transaction_head_sha256,
                    checkpoint_sha256, checkpoint_json
             FROM application_checkpoint WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| sqlite_error("读取 V2 周期应用检查点", error))?;
    let checkpoint = checkpoint_row
        .map(|(through_sequence, head, expected_sha256, json)| {
            let through_sequence = sqlite_sequence_to_u64_v2(through_sequence)?;
            if through_sequence > last_sequence {
                return Err(PersistenceErrorV1::Integrity(
                    "V2 应用检查点超过事务日志水位".to_owned(),
                ));
            }
            let transaction_head_sha256 =
                head.map(Sha256Hex).map(validate_sha256_v2).transpose()?;
            if (through_sequence == 0) != transaction_head_sha256.is_none() {
                return Err(PersistenceErrorV1::Integrity(
                    "V2 应用检查点序号与事务头不一致".to_owned(),
                ));
            }
            let actual_transaction_head = if through_sequence == 0 {
                None
            } else {
                connection
                    .query_row(
                        "SELECT record_sha256 FROM transaction_log WHERE sequence = ?1",
                        params![u64_to_sqlite_sequence_v2(through_sequence)?],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| sqlite_error("核对 V2 应用检查点事务头", error))?
                    .map(Sha256Hex)
                    .map(validate_sha256_v2)
                    .transpose()?
            };
            if actual_transaction_head != transaction_head_sha256 {
                return Err(PersistenceErrorV1::Integrity(
                    "V2 应用检查点绑定的事务头与日志正文不一致".to_owned(),
                ));
            }
            let actual_sha256 = sha256(&json);
            if actual_sha256.0 != expected_sha256 {
                return Err(PersistenceErrorV1::Integrity(
                    "V2 应用检查点正文摘要不符".to_owned(),
                ));
            }
            let checkpoint = serde_json::from_slice(&json)
                .map_err(|error| json_error("解码 V2 周期应用检查点", error))?;
            Ok(RecoveredApplicationCheckpointV2 {
                through_sequence,
                transaction_head_sha256,
                checkpoint_sha256: actual_sha256,
                checkpoint,
            })
        })
        .transpose()?;
    Ok(TransactionLogRecoveryHeaderV2 {
        last_sequence,
        transaction_head_sha256: metadata_head,
        checkpoint,
    })
}

fn recover_transaction_entries_after_v2<Evidence, RecoveryOperation>(
    connection: &Connection,
    through_sequence: u64,
    predecessor: Option<Sha256Hex>,
    last_sequence: u64,
    metadata_head: Option<Sha256Hex>,
) -> Result<Vec<RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation>>, PersistenceErrorV1>
where
    Evidence: DeserializeOwned,
    RecoveryOperation: DeserializeOwned,
{
    let mut entries = Vec::new();
    visit_transaction_entries_after_v2(
        connection,
        through_sequence,
        predecessor,
        last_sequence,
        metadata_head,
        |entry| {
            entries.push(entry);
            Ok(())
        },
    )?;
    Ok(entries)
}

fn visit_transaction_entries_after_v2<Evidence, RecoveryOperation, Visitor>(
    connection: &Connection,
    through_sequence: u64,
    mut predecessor: Option<Sha256Hex>,
    last_sequence: u64,
    metadata_head: Option<Sha256Hex>,
    mut visitor: Visitor,
) -> Result<(), PersistenceErrorV1>
where
    Evidence: DeserializeOwned,
    RecoveryOperation: DeserializeOwned,
    Visitor: FnMut(
        RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation>,
    ) -> Result<(), PersistenceErrorV1>,
{
    let mut expected_sequence = through_sequence
        .checked_add(1)
        .ok_or(PersistenceErrorV1::SequenceOverflow)?;
    let mut statement = connection
        .prepare(
            "SELECT sequence, predecessor_sha256, record_sha256, record_json
             FROM transaction_log WHERE sequence > ?1 ORDER BY sequence",
        )
        .map_err(|error| sqlite_error("准备读取 V2 尾日志", error))?;
    let rows = statement
        .query_map(
            params![u64_to_sqlite_sequence_v2(through_sequence)?],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .map_err(|error| sqlite_error("读取 V2 尾日志", error))?;
    for row in rows {
        let (sequence, stored_predecessor, stored_record_sha256, record_json) =
            row.map_err(|error| sqlite_error("解码 V2 尾日志行", error))?;
        let sequence = sqlite_sequence_to_u64_v2(sequence)?;
        if sequence != expected_sequence {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 尾日志序号不连续：期望 {expected_sequence}，实际 {sequence}"
            )));
        }
        let stored_predecessor = stored_predecessor
            .map(Sha256Hex)
            .map(validate_sha256_v2)
            .transpose()?;
        if stored_predecessor != predecessor {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 尾日志前驱摘要不连续：{sequence}"
            )));
        }
        let actual_record_sha256 =
            transaction_record_sha256_v2(sequence, predecessor.as_ref(), &record_json);
        if actual_record_sha256.0 != stored_record_sha256 {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 尾日志记录摘要不符：{sequence}"
            )));
        }
        let body: TransactionRecordBodyV2<Evidence, RecoveryOperation> =
            serde_json::from_slice(&record_json)
                .map_err(|error| json_error("解码 V2 事务正文", error))?;
        if body.schema_version != TRANSACTION_RECORD_SCHEMA_V2 {
            return Err(PersistenceErrorV1::Integrity(format!(
                "不支持的 V2 事务正文版本：{}",
                body.schema_version
            )));
        }
        predecessor = Some(actual_record_sha256.clone());
        visitor(RecoveredTransactionLogEntryV2 {
            sequence,
            record_sha256: actual_record_sha256,
            evidence: body.evidence,
            recovery_operation: body.recovery_operation,
        })?;
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(PersistenceErrorV1::SequenceOverflow)?;
    }
    if expected_sequence.saturating_sub(1) != last_sequence || predecessor != metadata_head {
        return Err(PersistenceErrorV1::Integrity(
            "V2 尾日志末端与运行元数据不一致".to_owned(),
        ));
    }
    Ok(())
}

fn stream_final_evidence_v2<Evidence, RecoveryOperation, Visitor>(
    connection: &Connection,
    last_sequence: u64,
    transaction_head_sha256: Option<Sha256Hex>,
    mut visitor: Visitor,
) -> Result<FinalEvidenceExportReceiptV2, PersistenceErrorV1>
where
    Evidence: DeserializeOwned + Serialize,
    RecoveryOperation: DeserializeOwned,
    Visitor: FnMut(u64, &Sha256Hex, u64, &Evidence, &[u8], &str) -> Result<(), PersistenceErrorV1>,
{
    let mut predecessor_evidence_sha256 = None;
    let mut evidence_file_count = 0_u64;
    let mut evidence_bytes = 0_u64;
    visit_transaction_entries_after_v2(
        connection,
        0,
        None,
        last_sequence,
        transaction_head_sha256,
        |entry: RecoveredTransactionLogEntryV2<Evidence, RecoveryOperation>| {
            let envelope = FinalEvidenceEnvelopeV2 {
                schema_version: FINAL_EVIDENCE_ENVELOPE_SCHEMA_V2.to_owned(),
                sequence: entry.sequence,
                predecessor_evidence_sha256: predecessor_evidence_sha256.clone(),
                body: entry.evidence,
            };
            let envelope_bytes = serde_json::to_vec(&envelope)
                .map_err(|error| json_error("编码 V2 最终证据信封", error))?;
            let envelope_byte_length = u64::try_from(envelope_bytes.len()).map_err(|_| {
                PersistenceErrorV1::Integrity("V2 最终证据成员字节超出 u64".to_owned())
            })?;
            let file_sha256 = sha256(&envelope_bytes);
            let file_name = evidence_file_name(entry.sequence, &file_sha256);
            visitor(
                entry.sequence,
                &file_sha256,
                envelope_byte_length,
                &envelope.body,
                &envelope_bytes,
                &file_name,
            )?;
            evidence_file_count = evidence_file_count.checked_add(1).ok_or_else(|| {
                PersistenceErrorV1::Integrity("V2 最终证据文件数量溢出".to_owned())
            })?;
            evidence_bytes = evidence_bytes
                .checked_add(envelope_byte_length)
                .ok_or_else(|| PersistenceErrorV1::Integrity("V2 最终证据总字节溢出".to_owned()))?;
            predecessor_evidence_sha256 = Some(file_sha256);
            Ok(())
        },
    )?;
    Ok(FinalEvidenceExportReceiptV2 {
        relative_path: PathBuf::from(FINAL_EVIDENCE_DIRECTORY_NAME_V2),
        evidence_file_count,
        evidence_bytes,
        evidence_head_sha256: predecessor_evidence_sha256,
    })
}

fn validate_final_evidence_directory_shape_v2(
    directory: &Path,
    expected_count: u64,
) -> Result<(), PersistenceErrorV1> {
    ensure_directory(directory, "V2 正式最终证据目录")?;
    let mut sequences = std::collections::BTreeSet::new();
    for entry in fs::read_dir(directory).map_err(|error| io_error("扫描 V2 正式证据目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取 V2 正式证据目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取 V2 正式证据成员类型", error))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PersistenceErrorV1::Integrity("V2 正式证据文件名不是 UTF-8".to_owned()))?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 正式证据目录包含非普通成员：{name}"
            )));
        }
        let (sequence, _) = parse_immutable_file_name(&name, "V2 最终证据")?;
        if !sequences.insert(sequence) {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 正式证据目录包含重复序号：{sequence}"
            )));
        }
    }
    if u64::try_from(sequences.len()).ok() != Some(expected_count)
        || (1..=expected_count).any(|sequence| !sequences.contains(&sequence))
    {
        return Err(PersistenceErrorV1::Integrity(
            "V2 正式证据目录文件集合与完整事务日志不一致".to_owned(),
        ));
    }
    Ok(())
}

fn remove_owned_final_evidence_candidate_v2(candidate: &Path) -> Result<bool, PersistenceErrorV1> {
    if candidate.file_name().and_then(|name| name.to_str())
        != Some(FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2)
    {
        return Err(PersistenceErrorV1::Integrity(
            "拒绝清理名称不符的 V2 最终证据候选目录".to_owned(),
        ));
    }
    let metadata = match fs::symlink_metadata(candidate) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io_error("读取 V2 最终证据候选目录元数据", error)),
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(PersistenceErrorV1::Integrity(
            "V2 最终证据候选路径不是普通目录".to_owned(),
        ));
    }
    for entry in
        fs::read_dir(candidate).map_err(|error| io_error("扫描 V2 最终证据候选目录", error))?
    {
        let entry = entry.map_err(|error| io_error("读取 V2 最终证据候选目录项", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error("读取 V2 最终证据候选成员类型", error))?;
        let name = entry.file_name().into_string().map_err(|_| {
            PersistenceErrorV1::Integrity("V2 最终证据候选文件名不是 UTF-8".to_owned())
        })?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 最终证据候选目录包含非普通成员：{name}"
            )));
        }
        parse_immutable_file_name(&name, "V2 最终证据候选")?;
        fs::remove_file(entry.path())
            .map_err(|error| io_error("清理 V2 最终证据候选成员", error))?;
    }
    fs::remove_dir(candidate).map_err(|error| io_error("清理 V2 最终证据候选目录", error))?;
    Ok(true)
}

fn open_exclusive_root_lock_v2(root: &Path) -> Result<File, PersistenceErrorV1> {
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(LOCK_FILE_NAME))
        .map_err(|error| io_error("打开 V2 运行根排他锁", error))?;
    if !lock_file
        .metadata()
        .map_err(|error| io_error("读取 V2 运行根排他锁元数据", error))?
        .is_file()
    {
        return Err(PersistenceErrorV1::Integrity(
            "V2 运行根排他锁不是普通文件".to_owned(),
        ));
    }
    flock(&lock_file, FlockOperation::NonBlockingLockExclusive)
        .map_err(|error| io_error("取得 V2 运行根排他锁", error))?;
    Ok(lock_file)
}

fn reject_legacy_material_v2(root: &Path) -> Result<(), PersistenceErrorV1> {
    for legacy in [
        CURRENT_FILE_NAME,
        CHECKPOINT_DIRECTORY_NAME,
        EVIDENCE_DIRECTORY_NAME,
    ] {
        if path_entry_exists_no_follow_v2(&root.join(legacy))? {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 运行根不能混入 V1 耐久材料：{legacy}"
            )));
        }
    }
    Ok(())
}

fn reject_orphan_transaction_log_sidecars_v2(
    database_path: &Path,
) -> Result<(), PersistenceErrorV1> {
    if path_entry_exists_no_follow_v2(database_path)? {
        return Ok(());
    }
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", database_path.display()));
        if path_entry_exists_no_follow_v2(&sidecar)? {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 主事务日志缺失但残留 SQLite 辅助文件：{}",
                sidecar.display()
            )));
        }
    }
    Ok(())
}

fn path_entry_exists_no_follow_v2(path: &Path) -> Result<bool, PersistenceErrorV1> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("读取 V2 固定路径元数据", error)),
    }
}

fn open_transaction_log_connection_v2(
    database_path: &Path,
) -> Result<Connection, PersistenceErrorV1> {
    let connection =
        Connection::open(database_path).map_err(|error| sqlite_error("打开 V2 事务日志", error))?;
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|error| sqlite_error("启用 V2 WAL", error))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(PersistenceErrorV1::Integrity(format!(
            "V2 事务日志未进入 WAL 模式：{journal_mode}"
        )));
    }
    connection
        .execute_batch(
            "PRAGMA synchronous = FULL;
             PRAGMA wal_autocheckpoint = 0;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 0;",
        )
        .map_err(|error| sqlite_error("固定 V2 SQLite 策略", error))?;
    let synchronous: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(|error| sqlite_error("核对 V2 同步策略", error))?;
    let automatic_checkpoint_pages: i64 = connection
        .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
        .map_err(|error| sqlite_error("核对 V2 自动检查点策略", error))?;
    if synchronous != 2 || automatic_checkpoint_pages != 0 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "V2 SQLite 策略不符：synchronous={synchronous}, wal_autocheckpoint={automatic_checkpoint_pages}"
        )));
    }
    Ok(connection)
}

fn validate_transaction_log_schema_v2(
    connection: &Connection,
    expected_identity: &TransactionLogIdentityV2,
) -> Result<(), PersistenceErrorV1> {
    let identity = load_transaction_log_identity_v2(connection)?;
    if &identity != expected_identity {
        return Err(PersistenceErrorV1::Integrity(format!(
            "V2 事务日志身份发生变化：run_id={}，durability_profile={}",
            identity.run_id,
            identity.durability_profile.as_str()
        )));
    }
    Ok(())
}

fn load_transaction_log_identity_v2(
    connection: &Connection,
) -> Result<TransactionLogIdentityV2, PersistenceErrorV1> {
    let (schema_version, run_id, durability_profile): (String, String, String) = connection
        .query_row(
            "SELECT schema_version, run_id, durability_profile
             FROM run_metadata WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| sqlite_error("读取 V2 数据模式版本", error))?;
    if schema_version != TRANSACTION_LOG_SCHEMA_V2 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "不支持的 V2 事务日志版本：{schema_version}"
        )));
    }
    validate_transaction_log_run_id_v2(&run_id)?;
    let durability_profile = DurabilityProfileV2::parse(&durability_profile)?;
    Ok(TransactionLogIdentityV2 {
        run_id,
        durability_profile,
    })
}

fn validate_transaction_log_run_id_v2(run_id: &str) -> Result<(), PersistenceErrorV1> {
    if run_id.is_empty()
        || run_id.trim() != run_id
        || run_id.len() > crate::session::STABLE_ID_MAX_BYTES_V1
    {
        return Err(PersistenceErrorV1::Integrity(
            "V2 事务日志 run_id 必须是非空、无首尾空白且不超过 256 字节的稳定身份".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn encode_transaction_record_v2<Evidence, RecoveryOperation>(
    evidence: &Evidence,
    recovery_operation: &RecoveryOperation,
) -> Result<EncodedTransactionRecordV2, PersistenceErrorV1>
where
    Evidence: Serialize,
    RecoveryOperation: Serialize,
{
    let record_json = serde_json::to_vec(&TransactionRecordBodyRefV2 {
        schema_version: TRANSACTION_RECORD_SCHEMA_V2,
        evidence,
        recovery_operation,
    })
    .map_err(|error| json_error("编码 V2 事务正文", error))?;
    let encoded_bytes = u64::try_from(record_json.len())
        .map_err(|_| PersistenceErrorV1::Integrity("V2 事务正文字节超出 u64".to_owned()))?;
    Ok(EncodedTransactionRecordV2 {
        record_json,
        encoded_bytes,
    })
}

fn load_transaction_log_append_state_v2(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<TransactionLogAppendStateV2, PersistenceErrorV1> {
    let (schema_version, previous_sequence, predecessor_sha256, logical_log_bytes): (
        String,
        i64,
        Option<String>,
        i64,
    ) = transaction
        .query_row(
            "SELECT schema_version, last_sequence,
                    transaction_head_sha256, logical_log_bytes
             FROM run_metadata WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| sqlite_error("读取 V2 业务事务元数据", error))?;
    if schema_version != TRANSACTION_LOG_SCHEMA_V2 {
        return Err(PersistenceErrorV1::Integrity(format!(
            "不支持的 V2 事务日志版本：{schema_version}"
        )));
    }
    let previous_sequence = sqlite_sequence_to_u64_v2(previous_sequence)?;
    let predecessor_sha256 = predecessor_sha256
        .map(Sha256Hex)
        .map(validate_sha256_v2)
        .transpose()?;
    if (previous_sequence == 0) != predecessor_sha256.is_none() {
        return Err(PersistenceErrorV1::Integrity(
            "V2 事务日志序号与前驱摘要状态不一致".to_owned(),
        ));
    }
    Ok(TransactionLogAppendStateV2 {
        last_sequence: previous_sequence,
        transaction_head_sha256: predecessor_sha256,
        logical_log_bytes,
    })
}

fn ensure_clean_run_not_completed_v2(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), PersistenceErrorV1> {
    let completed: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM application_checkpoint WHERE singleton = 1)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("核对 CleanRun 终态", error))?;
    if completed {
        return Err(PersistenceErrorV1::Integrity(
            "CleanRun 已发布终态应用检查点，禁止继续追加业务记录".to_owned(),
        ));
    }
    Ok(())
}

fn append_encoded_batch_v2(
    transaction: &rusqlite::Transaction<'_>,
    records: Vec<EncodedTransactionRecordV2>,
) -> Result<Vec<TransactionLogCommitReceiptV2>, PersistenceErrorV1> {
    let initial = load_transaction_log_append_state_v2(transaction)?;
    let mut sequence = initial.last_sequence;
    let mut predecessor_sha256 = initial.transaction_head_sha256;
    let mut logical_log_bytes = initial.logical_log_bytes;
    let mut receipts = Vec::with_capacity(records.len());
    let mut insert = transaction
        .prepare(
            "INSERT INTO transaction_log(
                sequence, predecessor_sha256, record_sha256,
                record_json, encoded_bytes
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .map_err(|error| sqlite_error("准备追加 V2 业务事务批次", error))?;
    for record in records {
        sequence = sequence
            .checked_add(1)
            .ok_or(PersistenceErrorV1::SequenceOverflow)?;
        let record_sha256 = transaction_record_sha256_v2(
            sequence,
            predecessor_sha256.as_ref(),
            &record.record_json,
        );
        let encoded_bytes_sql = u64_to_sqlite_quantity_v2(record.encoded_bytes, "V2 事务记录字节")?;
        insert
            .execute(params![
                u64_to_sqlite_sequence_v2(sequence)?,
                predecessor_sha256.as_ref().map(|value| value.0.as_str()),
                record_sha256.0.as_str(),
                record.record_json,
                encoded_bytes_sql,
            ])
            .map_err(|error| sqlite_error("追加 V2 业务事务记录", error))?;
        logical_log_bytes = logical_log_bytes
            .checked_add(encoded_bytes_sql)
            .ok_or_else(|| PersistenceErrorV1::Integrity("V2 逻辑日志字节溢出".to_owned()))?;
        predecessor_sha256 = Some(record_sha256.clone());
        receipts.push(TransactionLogCommitReceiptV2 {
            sequence,
            record_sha256,
            encoded_bytes: record.encoded_bytes,
        });
    }
    drop(insert);
    let updated = transaction
        .execute(
            "UPDATE run_metadata
             SET last_sequence = ?1, transaction_head_sha256 = ?2,
                 logical_log_bytes = ?3
             WHERE singleton = 1 AND last_sequence = ?4",
            params![
                u64_to_sqlite_sequence_v2(sequence)?,
                predecessor_sha256.as_ref().map(|value| value.0.as_str()),
                logical_log_bytes,
                u64_to_sqlite_sequence_v2(initial.last_sequence)?,
            ],
        )
        .map_err(|error| sqlite_error("推进 V2 事务日志元数据", error))?;
    if updated != 1 {
        return Err(PersistenceErrorV1::Integrity(
            "V2 事务日志元数据并发推进冲突".to_owned(),
        ));
    }
    Ok(receipts)
}

fn store_application_checkpoint_v2(
    transaction: &rusqlite::Transaction<'_>,
    through_sequence: u64,
    checkpoint_json: &[u8],
    checkpoint_sha256: &Sha256Hex,
) -> Result<(), PersistenceErrorV1> {
    let last_sequence: i64 = transaction
        .query_row(
            "SELECT last_sequence FROM run_metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("读取 V2 应用检查点水位", error))?;
    let last_sequence = sqlite_sequence_to_u64_v2(last_sequence)?;
    if through_sequence > last_sequence {
        return Err(PersistenceErrorV1::Integrity(format!(
            "V2 应用检查点序号 {through_sequence} 超过事务日志水位 {last_sequence}"
        )));
    }
    let transaction_head_sha256 = transaction_head_at_v2(transaction, through_sequence)?;
    let existing: Option<(i64, String, Vec<u8>)> = transaction
        .query_row(
            "SELECT through_sequence, checkpoint_sha256, checkpoint_json
             FROM application_checkpoint WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| sqlite_error("读取既有 V2 应用检查点", error))?;
    if let Some((existing_sequence, existing_sha256, existing_json)) = existing {
        let existing_sequence = sqlite_sequence_to_u64_v2(existing_sequence)?;
        if through_sequence < existing_sequence {
            return Err(PersistenceErrorV1::Integrity(format!(
                "V2 应用检查点不能从 {existing_sequence} 回退到 {through_sequence}"
            )));
        }
        if through_sequence == existing_sequence {
            if existing_sha256 != checkpoint_sha256.0 || existing_json != checkpoint_json {
                return Err(PersistenceErrorV1::Integrity(
                    "同一 V2 应用检查点序号出现不同正文".to_owned(),
                ));
            }
            return Ok(());
        }
    }
    transaction
        .execute(
            "INSERT INTO application_checkpoint(
                singleton, through_sequence, transaction_head_sha256,
                checkpoint_sha256, checkpoint_json
             ) VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(singleton) DO UPDATE SET
                through_sequence = excluded.through_sequence,
                transaction_head_sha256 = excluded.transaction_head_sha256,
                checkpoint_sha256 = excluded.checkpoint_sha256,
                checkpoint_json = excluded.checkpoint_json",
            params![
                u64_to_sqlite_sequence_v2(through_sequence)?,
                transaction_head_sha256
                    .as_ref()
                    .map(|value| value.0.as_str()),
                checkpoint_sha256.0,
                checkpoint_json,
            ],
        )
        .map_err(|error| sqlite_error("发布 V2 周期应用检查点", error))?;
    Ok(())
}

fn transaction_head_at_v2(
    transaction: &rusqlite::Transaction<'_>,
    sequence: u64,
) -> Result<Option<Sha256Hex>, PersistenceErrorV1> {
    if sequence == 0 {
        return Ok(None);
    }
    let value: Option<String> = transaction
        .query_row(
            "SELECT record_sha256 FROM transaction_log WHERE sequence = ?1",
            params![u64_to_sqlite_sequence_v2(sequence)?],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| sqlite_error("读取 V2 应用检查点事务头", error))?;
    value
        .map(Sha256Hex)
        .map(validate_sha256_v2)
        .transpose()?
        .ok_or_else(|| {
            PersistenceErrorV1::Integrity(format!("V2 应用检查点缺少事务序号 {sequence}"))
        })
        .map(Some)
}

fn transaction_record_sha256_v2(
    sequence: u64,
    predecessor: Option<&Sha256Hex>,
    record_json: &[u8],
) -> Sha256Hex {
    let mut digest = Sha256::new();
    digest.update(TRANSACTION_RECORD_HASH_DOMAIN_V2);
    digest.update(sequence.to_be_bytes());
    match predecessor {
        Some(predecessor) => {
            digest.update([1]);
            digest.update((predecessor.0.len() as u64).to_be_bytes());
            digest.update(predecessor.0.as_bytes());
        }
        None => digest.update([0]),
    }
    digest.update((record_json.len() as u64).to_be_bytes());
    digest.update(record_json);
    Sha256Hex(format!("{:x}", digest.finalize()))
}

fn sqlite_sequence_to_u64_v2(value: i64) -> Result<u64, PersistenceErrorV1> {
    u64::try_from(value)
        .map_err(|_| PersistenceErrorV1::Integrity("V2 SQLite 序号或数量为负".to_owned()))
}

fn u64_to_sqlite_sequence_v2(value: u64) -> Result<i64, PersistenceErrorV1> {
    i64::try_from(value).map_err(|_| PersistenceErrorV1::SequenceOverflow)
}

fn u64_to_sqlite_quantity_v2(
    value: u64,
    identity: &'static str,
) -> Result<i64, PersistenceErrorV1> {
    i64::try_from(value)
        .map_err(|_| PersistenceErrorV1::Integrity(format!("{identity} 超出 SQLite i64")))
}

fn validate_sha256_v2(value: Sha256Hex) -> Result<Sha256Hex, PersistenceErrorV1> {
    validate_sha256_text(&value.0)?;
    Ok(value)
}

fn ensure_directory(path: &Path, identity: &'static str) -> Result<(), PersistenceErrorV1> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("读取耐久目录元数据", error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(PersistenceErrorV1::Integrity(format!(
            "{identity} 不是普通目录：{}",
            path.display()
        )));
    }
    Ok(())
}

fn io_error(operation: &'static str, error: impl fmt::Display) -> PersistenceErrorV1 {
    PersistenceErrorV1::Io {
        operation,
        message: error.to_string(),
    }
}

fn json_error(operation: &'static str, error: impl fmt::Display) -> PersistenceErrorV1 {
    PersistenceErrorV1::Json {
        operation,
        message: error.to_string(),
    }
}

fn sqlite_error(operation: &'static str, error: impl fmt::Display) -> PersistenceErrorV1 {
    PersistenceErrorV1::Sqlite {
        operation,
        message: error.to_string(),
    }
}

fn sqlite_commit_uncertain(
    database_path: &Path,
    operation: &'static str,
    error: impl fmt::Display,
) -> PersistenceErrorV1 {
    PersistenceErrorV1::DurabilityUncertain {
        path: database_path.to_path_buf(),
        message: format!("{operation}：{error}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::Write as _;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvidenceV1 {
        event: String,
        value: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestCheckpointV1 {
        state: String,
        applied: u64,
    }

    fn paper_transaction_log_identity() -> TransactionLogIdentityV2 {
        TransactionLogIdentityV2::new(
            "task42-v2-run".to_owned(),
            DurabilityProfileV2::RecoverablePaper,
        )
    }

    fn store() -> (tempfile::TempDir, LocalDurabilityStoreV1) {
        let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
        let store = LocalDurabilityStoreV1::try_new(directory.path().join("run"))
            .expect("创建本地耐久存储");
        (directory, store)
    }

    #[test]
    fn sqlite_transaction_log_v2_recovers_checkpoint_and_ordered_tail() {
        let directory = tempfile::tempdir().expect("创建 V2 事务日志临时目录");
        let root = directory.path().join("run");
        let mut store =
            LocalTransactionLogStoreV2::create_new(root.clone(), paper_transaction_log_identity())
                .expect("创建 V2 事务日志");
        let journal_mode: String = store
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("读取 WAL 模式");
        let synchronous: i64 = store
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("读取同步策略");
        let automatic_checkpoint_pages: i64 = store
            .connection
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .expect("读取自动 WAL 检查点策略");
        assert!(journal_mode.eq_ignore_ascii_case("wal"));
        assert_eq!(synchronous, 2);
        assert_eq!(automatic_checkpoint_pages, 0);

        let first = store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "registered".to_owned(),
                    value: 1,
                },
                &"REGISTER_REQUEST",
            )
            .expect("追加首笔 V2 事务");
        let before_checkpoint: RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1> =
            store.recover().expect("恢复尚无应用检查点的事务日志");
        assert!(before_checkpoint.checkpoint.is_none());
        assert_eq!(before_checkpoint.tail.len(), 1);
        assert_eq!(before_checkpoint.tail[0].record_sha256, first.record_sha256);

        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "bucket".to_owned(),
                    value: 2,
                },
                &"APPLY_PUBLIC_BUCKET",
            )
            .expect("追加第二笔 V2 事务");
        store
            .publish_application_checkpoint(
                1,
                &TestCheckpointV1 {
                    state: "after-first".to_owned(),
                    applied: 1,
                },
            )
            .expect("发布周期应用检查点");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "advanced".to_owned(),
                    value: 3,
                },
                &"DRIVE_EXECUTION",
            )
            .expect("追加第三笔 V2 事务");
        assert!(store.logical_log_bytes().expect("读取逻辑日志字节") > 0);
        let before_wal_reclaim: RecoveredTransactionLogV2<
            TestEvidenceV1,
            String,
            TestCheckpointV1,
        > = store.recover().expect("WAL 回收前恢复事务前缀");
        let wal_path = PathBuf::from(format!("{}-wal", store.database_path().display()));
        assert!(fs::metadata(&wal_path).expect("读取回收前 WAL").len() > 0);
        let completed = store
            .try_checkpoint_wal_after_application_checkpoint()
            .expect("在应用检查点后执行 WAL 检查点与回收");
        assert!(completed);
        assert_eq!(fs::metadata(&wal_path).expect("读取回收后 WAL").len(), 0);
        let after_wal_reclaim: RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1> =
            store.recover().expect("WAL 回收后恢复同一事务前缀");
        assert_eq!(after_wal_reclaim, before_wal_reclaim);
        assert!(store.database_path().is_file());
        drop(store);

        let reopened =
            LocalTransactionLogStoreV2::open_recovery(root).expect("重新打开 V2 事务日志");
        let recovered: RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1> =
            reopened.recover().expect("从应用检查点恢复有序尾日志");
        let checkpoint = recovered.checkpoint.expect("恢复最近应用检查点");
        assert_eq!(checkpoint.through_sequence, 1);
        assert_eq!(checkpoint.checkpoint.applied, 1);
        assert_eq!(
            recovered
                .tail
                .iter()
                .map(|entry| (entry.sequence, entry.evidence.event.as_str()))
                .collect::<Vec<_>>(),
            vec![(2, "bucket"), (3, "advanced")]
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_visits_paper_prefix_only_after_checkpoint_covers_head() {
        let directory = tempfile::tempdir().expect("创建 V2 产物封口临时目录");
        let root = directory.path().join("run");
        let mut store =
            LocalTransactionLogStoreV2::create_new(root, paper_transaction_log_identity())
                .expect("创建 V2 产物封口事务日志");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "registered".to_owned(),
                    value: 1,
                },
                &"REGISTER_REQUEST",
            )
            .expect("追加 V2 产物封口首项");
        let mut visited_without_checkpoint = Vec::new();
        let missing_checkpoint = store
            .visit_checkpointed_paper_prefix_for_artifacts::<
                TestEvidenceV1,
                String,
                TestCheckpointV1,
                _,
            >(|entry| {
                visited_without_checkpoint.push(entry.sequence);
                Ok(())
            });
        assert!(
            matches!(missing_checkpoint, Err(PersistenceErrorV1::Integrity(message)) if message.contains("要求完整应用检查点"))
        );
        assert!(visited_without_checkpoint.is_empty());

        store
            .publish_application_checkpoint(
                1,
                &TestCheckpointV1 {
                    state: "after-first".to_owned(),
                    applied: 1,
                },
            )
            .expect("发布覆盖首项的应用检查点");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "bucket".to_owned(),
                    value: 2,
                },
                &"APPLY_PUBLIC_BUCKET",
            )
            .expect("追加检查点之后的 V2 尾项");
        let mut prematurely_visited = Vec::new();
        let stale_checkpoint = store
            .visit_checkpointed_paper_prefix_for_artifacts::<
                TestEvidenceV1,
                String,
                TestCheckpointV1,
                _,
            >(|entry| {
                prematurely_visited.push(entry.sequence);
                Ok(())
            });
        assert!(
            matches!(stale_checkpoint, Err(PersistenceErrorV1::Integrity(message)) if message.contains("未覆盖完整事务日志"))
        );
        assert!(prematurely_visited.is_empty());

        store
            .publish_application_checkpoint(
                2,
                &TestCheckpointV1 {
                    state: "sealed".to_owned(),
                    applied: 2,
                },
            )
            .expect("发布覆盖完整日志头的应用检查点");
        let mut completed_entries = Vec::new();
        let checkpoint = store
            .visit_checkpointed_paper_prefix_for_artifacts::<
                TestEvidenceV1,
                String,
                TestCheckpointV1,
                _,
            >(|entry| {
                completed_entries.push((
                    entry.sequence,
                    entry.evidence.event,
                    entry.recovery_operation,
                ));
                Ok(())
            })
            .expect("按序流式访问已封口的 V2 完整前缀");
        assert_eq!(checkpoint.through_sequence, 2);
        assert_eq!(checkpoint.checkpoint.applied, 2);
        assert_eq!(
            completed_entries,
            vec![
                (1, "registered".to_owned(), "REGISTER_REQUEST".to_owned()),
                (2, "bucket".to_owned(), "APPLY_PUBLIC_BUCKET".to_owned()),
            ]
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_final_evidence_export_rejects_uncovered_checkpoint() {
        let directory = tempfile::tempdir().expect("创建 V2 最终证据封口临时目录");
        let root = directory.path().join("run");
        let mut store =
            LocalTransactionLogStoreV2::create_new(root.clone(), paper_transaction_log_identity())
                .expect("创建 V2 最终证据封口事务日志");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "registered".to_owned(),
                    value: 1,
                },
                &"REGISTER_REQUEST",
            )
            .expect("追加检查点内事务");
        let stale_checkpoint = TestCheckpointV1 {
            state: "stale".to_owned(),
            applied: 1,
        };
        store
            .publish_application_checkpoint(1, &stale_checkpoint)
            .expect("发布尚未覆盖后续日志的应用检查点");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "bucket".to_owned(),
                    value: 2,
                },
                &"APPLY_PUBLIC_BUCKET",
            )
            .expect("追加检查点外事务");

        let mut visited = Vec::new();
        let result = store.export_final_evidence_v2::<TestEvidenceV1, String, TestCheckpointV1, _>(
            1,
            &stale_checkpoint,
            |sequence, _, _, _| {
                visited.push(sequence);
                Ok(())
            },
        );
        assert!(
            matches!(result, Err(PersistenceErrorV1::Integrity(message)) if message.contains("未覆盖完整事务日志"))
        );
        assert!(visited.is_empty());
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_DIRECTORY_NAME_V2))
                .expect("核对正式 V2 最终证据目录不存在")
        );
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2))
                .expect("核对候选 V2 最终证据目录不存在")
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_exports_exact_v2_evidence_files_idempotently() {
        let directory = tempfile::tempdir().expect("创建 V2 最终证据导出临时目录");
        let root = directory.path().join("run");
        let mut store =
            LocalTransactionLogStoreV2::create_new(root.clone(), paper_transaction_log_identity())
                .expect("创建 V2 最终证据导出事务日志");
        for (event, recovery_operation, value) in [
            ("registered", "REGISTER_REQUEST", 1),
            ("bucket", "APPLY_PUBLIC_BUCKET", 2),
        ] {
            store
                .append_transaction(
                    &TestEvidenceV1 {
                        event: event.to_owned(),
                        value,
                    },
                    &recovery_operation,
                )
                .expect("追加待导出的 V2 事务");
        }
        let sealed_checkpoint = TestCheckpointV1 {
            state: "sealed".to_owned(),
            applied: 2,
        };
        store
            .publish_application_checkpoint(2, &sealed_checkpoint)
            .expect("发布覆盖完整日志的应用检查点");

        let mismatched_checkpoint = TestCheckpointV1 {
            state: "other".to_owned(),
            applied: 2,
        };
        let mismatch = store
            .export_final_evidence_v2::<TestEvidenceV1, String, TestCheckpointV1, _>(
                2,
                &mismatched_checkpoint,
                |_, _, _, _| Ok(()),
            );
        assert!(
            matches!(mismatch, Err(PersistenceErrorV1::Integrity(message)) if message.contains("调用方封口检查点不一致"))
        );
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_DIRECTORY_NAME_V2))
                .expect("检查点不一致时不发布正式目录")
        );
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2))
                .expect("检查点不一致时不创建候选目录")
        );

        let failed = store.export_final_evidence_v2::<TestEvidenceV1, String, TestCheckpointV1, _>(
            2,
            &sealed_checkpoint,
            |sequence, _, _, _| {
                if sequence == 2 {
                    return Err(PersistenceErrorV1::Integrity(
                        "注入 V2 最终证据访问者失败".to_owned(),
                    ));
                }
                Ok(())
            },
        );
        assert!(
            matches!(failed, Err(PersistenceErrorV1::Integrity(message)) if message.contains("访问者失败"))
        );
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_DIRECTORY_NAME_V2))
                .expect("访问者失败后不发布正式目录")
        );
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2))
                .expect("访问者失败后清理候选目录")
        );

        let mut visited = Vec::new();
        let receipt = store
            .export_final_evidence_v2::<TestEvidenceV1, String, TestCheckpointV1, _>(
                2,
                &sealed_checkpoint,
                |sequence, file_sha256, envelope_byte_length, evidence| {
                    visited.push((
                        sequence,
                        file_sha256.clone(),
                        envelope_byte_length,
                        evidence.clone(),
                    ));
                    Ok(())
                },
            )
            .expect("导出完整 V2 最终证据");
        let target = root.join(FINAL_EVIDENCE_DIRECTORY_NAME_V2);
        let mut exported = BTreeMap::new();
        for entry in fs::read_dir(&target).expect("扫描正式 V2 最终证据目录") {
            let entry = entry.expect("读取正式 V2 最终证据成员");
            exported.insert(
                entry.file_name().into_string().expect("证据文件名是 UTF-8"),
                fs::read(entry.path()).expect("读取正式 V2 最终证据正文"),
            );
        }
        assert_eq!(exported.len(), 2);

        let mut predecessor = None;
        let mut actual_bytes = 0_u64;
        for (
            (name, bytes),
            (expected_sequence, visited_sha256, visited_byte_length, visited_evidence),
        ) in exported.iter().zip(&visited)
        {
            let file_sha256 = sha256(bytes);
            let (sequence, name_sha256) =
                parse_immutable_file_name(name, "测试 V2 最终证据").expect("解析证据文件名");
            assert_eq!(sequence, *expected_sequence);
            assert_eq!(name_sha256, file_sha256);
            assert_eq!(&file_sha256, visited_sha256);
            assert_eq!(
                *visited_byte_length,
                u64::try_from(bytes.len()).expect("测试证据字节适配 u64")
            );
            let value: serde_json::Value =
                serde_json::from_slice(bytes).expect("严格读取 V2 最终证据信封值");
            let keys = value
                .as_object()
                .expect("V2 最终证据信封是对象")
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                keys,
                BTreeSet::from([
                    "body",
                    "predecessor_evidence_sha256",
                    "schema_version",
                    "sequence",
                ])
            );
            assert!(value.get("recovery_operation").is_none());
            let envelope: FinalEvidenceEnvelopeV2<TestEvidenceV1> =
                serde_json::from_slice(bytes).expect("严格解码 V2 最终证据信封");
            assert_eq!(envelope.schema_version, FINAL_EVIDENCE_ENVELOPE_SCHEMA_V2);
            assert_eq!(envelope.sequence, sequence);
            assert_eq!(envelope.predecessor_evidence_sha256, predecessor);
            assert_eq!(&envelope.body, visited_evidence);
            predecessor = Some(file_sha256);
            actual_bytes += u64::try_from(bytes.len()).expect("测试证据字节适配 u64");
        }
        assert_eq!(receipt.relative_path, PathBuf::from("evidence-v2"));
        assert_eq!(receipt.evidence_file_count, 2);
        assert_eq!(receipt.evidence_bytes, actual_bytes);
        assert_eq!(receipt.evidence_head_sha256, predecessor);

        let first_export = exported;
        let mut retry_visited = Vec::new();
        let retry_receipt = store
            .export_final_evidence_v2::<TestEvidenceV1, String, TestCheckpointV1, _>(
                2,
                &sealed_checkpoint,
                |sequence, file_sha256, envelope_byte_length, evidence| {
                    retry_visited.push((
                        sequence,
                        file_sha256.clone(),
                        envelope_byte_length,
                        evidence.clone(),
                    ));
                    Ok(())
                },
            )
            .expect("幂等复核既有 V2 最终证据");
        let mut retry_export = BTreeMap::new();
        for entry in fs::read_dir(&target).expect("重试后扫描正式 V2 最终证据目录") {
            let entry = entry.expect("重试后读取正式 V2 最终证据成员");
            retry_export.insert(
                entry
                    .file_name()
                    .into_string()
                    .expect("重试证据文件名是 UTF-8"),
                fs::read(entry.path()).expect("重试后读取正式 V2 最终证据正文"),
            );
        }
        assert_eq!(retry_receipt, receipt);
        assert_eq!(retry_visited, visited);
        assert_eq!(retry_export, first_export);
        assert!(
            !path_entry_exists_no_follow_v2(&root.join(FINAL_EVIDENCE_CANDIDATE_DIRECTORY_NAME_V2))
                .expect("幂等重试后不存在候选目录")
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_defers_busy_wal_maintenance_without_integrity_failure() {
        let directory = tempfile::tempdir().expect("创建 V2 WAL 竞争临时目录");
        let root = directory.path().join("run");
        let mut store =
            LocalTransactionLogStoreV2::create_new(root, paper_transaction_log_identity())
                .expect("创建 V2 WAL 竞争事务日志");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "first".to_owned(),
                    value: 1,
                },
                &"REGISTER_REQUEST",
            )
            .expect("追加首笔 V2 WAL 竞争事务");
        store
            .publish_application_checkpoint(
                1,
                &TestCheckpointV1 {
                    state: "first".to_owned(),
                    applied: 1,
                },
            )
            .expect("发布 V2 WAL 竞争应用检查点");

        let reader = Connection::open(store.database_path()).expect("打开 V2 WAL 并发读取连接");
        reader.execute_batch("BEGIN").expect("开始 V2 WAL 读取事务");
        let _: i64 = reader
            .query_row("SELECT COUNT(*) FROM transaction_log", [], |row| row.get(0))
            .expect("固定 V2 WAL 读取快照");
        store
            .append_transaction(
                &TestEvidenceV1 {
                    event: "second".to_owned(),
                    value: 2,
                },
                &"APPLY_PUBLIC_BUCKET",
            )
            .expect("在读取快照后追加 V2 WAL 事务");

        assert!(
            !store
                .try_checkpoint_wal_after_application_checkpoint()
                .expect("读取竞争只应延期 WAL 维护")
        );
        reader
            .execute_batch("ROLLBACK")
            .expect("释放 V2 WAL 读取事务");
        assert!(
            store
                .try_checkpoint_wal_after_application_checkpoint()
                .expect("读取者退出后完成 WAL 维护")
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_rejects_modified_record_body() {
        let directory = tempfile::tempdir().expect("创建 V2 损坏测试临时目录");
        let root = directory.path().join("run");
        let database_path;
        {
            let mut store = LocalTransactionLogStoreV2::create_new(
                root.clone(),
                paper_transaction_log_identity(),
            )
            .expect("创建 V2 损坏测试事务日志");
            store
                .append_transaction(
                    &TestEvidenceV1 {
                        event: "registered".to_owned(),
                        value: 1,
                    },
                    &"REGISTER_REQUEST",
                )
                .expect("追加待篡改事务");
            database_path = store.database_path().to_owned();
        }
        let connection = Connection::open(database_path).expect("打开待篡改 V2 数据库");
        connection
            .execute(
                "UPDATE transaction_log SET record_json = ?1 WHERE sequence = 1",
                params![br#"{"tampered":true}"#.as_slice()],
            )
            .expect("篡改 V2 事务正文");
        drop(connection);
        let reopened =
            LocalTransactionLogStoreV2::open_recovery(root).expect("打开已篡改 V2 数据库");
        let result: Result<RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1>, _> =
            reopened.recover();
        assert!(
            matches!(result, Err(PersistenceErrorV1::Integrity(message)) if message.contains("记录摘要不符"))
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_commits_record_and_boundary_checkpoint_atomically() {
        let directory = tempfile::tempdir().expect("创建 V2 原子边界临时目录");
        let root = directory.path().join("run");
        let mut store =
            LocalTransactionLogStoreV2::create_new(root, paper_transaction_log_identity())
                .expect("创建 V2 原子边界事务日志");
        store
            .connection
            .execute_batch(
                "CREATE TEMP TRIGGER reject_boundary_checkpoint
                 BEFORE INSERT ON application_checkpoint
                 BEGIN
                   SELECT RAISE(ABORT, 'injected checkpoint failure');
                 END;",
            )
            .expect("安装应用检查点失败注入");
        let failed = store.append_transaction_and_application_checkpoint(
            &TestEvidenceV1 {
                event: "session-created".to_owned(),
                value: 1,
            },
            &"APPLICATION_CHECKPOINT_BOUNDARY",
            &TestCheckpointV1 {
                state: "created".to_owned(),
                applied: 1,
            },
        );
        assert!(matches!(failed, Err(PersistenceErrorV1::Sqlite { .. })));
        let after_failed_boundary: RecoveredTransactionLogV2<
            TestEvidenceV1,
            String,
            TestCheckpointV1,
        > = store.recover().expect("失败边界后恢复空前缀");
        assert!(after_failed_boundary.checkpoint.is_none());
        assert!(after_failed_boundary.tail.is_empty());
        assert_eq!(store.logical_log_bytes().expect("读取回滚后日志字节"), 0);

        store
            .connection
            .execute_batch("DROP TRIGGER reject_boundary_checkpoint;")
            .expect("移除应用检查点失败注入");
        let encoded = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "session-created".to_owned(),
                value: 1,
            },
            &"APPLICATION_CHECKPOINT_BOUNDARY",
        )
        .expect("预编码低频边界事务正文");
        let encoded_bytes = encoded.encoded_bytes();
        let (receipt, checkpoint_sha256) = store
            .append_encoded_transaction_and_application_checkpoint(
                encoded,
                &TestCheckpointV1 {
                    state: "created".to_owned(),
                    applied: 1,
                },
            )
            .expect("原子提交低频事实与应用检查点");
        assert_eq!(receipt.sequence, 1);
        assert_eq!(receipt.encoded_bytes, encoded_bytes);
        let recovered: RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1> =
            store.recover().expect("恢复原子边界");
        let checkpoint = recovered.checkpoint.expect("恢复边界应用检查点");
        assert_eq!(checkpoint.through_sequence, receipt.sequence);
        assert_eq!(
            checkpoint.transaction_head_sha256,
            Some(receipt.record_sha256)
        );
        assert_eq!(checkpoint.checkpoint_sha256, checkpoint_sha256);
        assert!(recovered.tail.is_empty());
    }

    #[test]
    fn sqlite_transaction_log_v2_binds_identity_and_rejects_wrong_checkpoint_head() {
        let directory = tempfile::tempdir().expect("创建 V2 身份绑定临时目录");
        let root = directory.path().join("run");
        {
            let mut store = LocalTransactionLogStoreV2::create_new(
                root.clone(),
                paper_transaction_log_identity(),
            )
            .expect("创建 V2 身份绑定事务日志");
            store
                .append_transaction(
                    &TestEvidenceV1 {
                        event: "registered".to_owned(),
                        value: 1,
                    },
                    &"REGISTER_REQUEST",
                )
                .expect("追加身份绑定事务");
            store
                .publish_application_checkpoint(
                    1,
                    &TestCheckpointV1 {
                        state: "valid-head".to_owned(),
                        applied: 1,
                    },
                )
                .expect("发布待篡改链头的应用检查点");
            assert_eq!(store.identity(), &paper_transaction_log_identity());
            store
                .connection
                .execute(
                    "UPDATE transaction_log
                     SET record_sha256 = ?1 WHERE sequence = 1",
                    params!["0".repeat(64)],
                )
                .expect("篡改被应用检查点覆盖的事务头");
            let result: Result<
                RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1>,
                _,
            > = store.recover();
            assert!(
                matches!(result, Err(PersistenceErrorV1::Integrity(message)) if message.contains("事务头与日志正文不一致"))
            );
        }
        let connection = Connection::open(root.join(TRANSACTION_LOG_DATABASE_NAME_V2))
            .expect("打开待篡改 V2 身份数据库");
        connection
            .execute(
                "UPDATE run_metadata SET durability_profile = 'UNKNOWN' WHERE singleton = 1",
                [],
            )
            .expect("篡改 V2 耐久档位");
        drop(connection);
        assert!(matches!(
            LocalTransactionLogStoreV2::open_recovery(root),
            Err(PersistenceErrorV1::Integrity(message)) if message.contains("不支持的 V2 耐久档位")
        ));
    }

    #[test]
    fn sqlite_transaction_log_v2_rejects_clean_run_single_record_commits() {
        let directory = tempfile::tempdir().expect("创建 CleanRun V2 临时目录");
        let root = directory.path().join("run");
        let mut store = LocalTransactionLogStoreV2::create_new(
            root.clone(),
            TransactionLogIdentityV2::new(
                "task42-clean-run".to_owned(),
                DurabilityProfileV2::CleanRun,
            ),
        )
        .expect("创建 CleanRun V2 存储");
        let result = store.append_transaction(
            &TestEvidenceV1 {
                event: "hot-transaction".to_owned(),
                value: 1,
            },
            &"REGISTER_REQUEST",
        );
        assert!(
            matches!(result, Err(PersistenceErrorV1::Integrity(message)) if message.contains("接口档位不符"))
        );
        assert_eq!(store.logical_log_bytes().expect("读取 CleanRun 空日志"), 0);

        let first = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "registered".to_owned(),
                value: 1,
            },
            &"REGISTER_REQUEST",
        )
        .expect("预编码 CleanRun 首项");
        let second = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "bucket".to_owned(),
                value: 2,
            },
            &"APPLY_PUBLIC_BUCKET",
        )
        .expect("预编码 CleanRun 第二项");
        assert!(first.encoded_bytes() > 0);
        let receipts = store
            .append_clean_run_batch(vec![first, second])
            .expect("以一个共享提交刷入 CleanRun 批次");
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let generic_recovery: Result<
            RecoveredTransactionLogV2<TestEvidenceV1, String, TestCheckpointV1>,
            _,
        > = store.recover();
        assert!(
            matches!(generic_recovery, Err(PersistenceErrorV1::Integrity(message)) if message.contains("接口档位不符"))
        );
        let mut prematurely_exposed = Vec::new();
        let incomplete = store
            .visit_completed_clean_run_for_artifacts::<TestEvidenceV1, String, TestCheckpointV1, _>(
                |entry| {
                    prematurely_exposed.push(entry.sequence);
                    Ok(())
                },
            )
            .expect("检查未完成 CleanRun");
        assert!(incomplete.is_none());
        assert!(prematurely_exposed.is_empty());

        let completion = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "clean-run-completed".to_owned(),
                value: 3,
            },
            &"APPLICATION_CHECKPOINT_BOUNDARY",
        )
        .expect("预编码 CleanRun 完成事实");
        store
            .append_clean_run_final_batch(
                vec![completion],
                &TestCheckpointV1 {
                    state: "completed".to_owned(),
                    applied: 3,
                },
            )
            .expect("以唯一终态屏障完成 CleanRun");
        let late = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "late-write".to_owned(),
                value: 4,
            },
            &"REGISTER_REQUEST",
        )
        .expect("预编码终态后的非法记录");
        assert!(
            matches!(store.append_clean_run_batch(vec![late]), Err(PersistenceErrorV1::Integrity(message)) if message.contains("禁止继续追加"))
        );
        drop(store);

        let reopened = LocalTransactionLogStoreV2::open_recovery(root)
            .expect("重新打开已完成 CleanRun V2 存储");
        let mut completed_entries = Vec::new();
        let checkpoint = reopened
            .visit_completed_clean_run_for_artifacts::<TestEvidenceV1, String, TestCheckpointV1, _>(
                |entry| {
                    completed_entries.push((entry.sequence, entry.evidence.event));
                    Ok(())
                },
            )
            .expect("恢复已完成 CleanRun")
            .expect("CleanRun 只有完成后才有应用检查点");
        assert_eq!(checkpoint.through_sequence, 3);
        assert_eq!(checkpoint.checkpoint.applied, 3);
        assert_eq!(
            completed_entries,
            vec![
                (1, "registered".to_owned()),
                (2, "bucket".to_owned()),
                (3, "clean-run-completed".to_owned()),
            ]
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_clean_run_wal_reclaim_does_not_make_run_recoverable() {
        let directory = tempfile::tempdir().expect("创建 CleanRun WAL 临时目录");
        let root = directory.path().join("run");
        let mut store = LocalTransactionLogStoreV2::create_new(
            root.clone(),
            TransactionLogIdentityV2::new(
                "task42-clean-run-wal".to_owned(),
                DurabilityProfileV2::CleanRun,
            ),
        )
        .expect("创建 CleanRun WAL 存储");
        let record = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "batched".to_owned(),
                value: 1,
            },
            &"REGISTER_REQUEST",
        )
        .expect("预编码 CleanRun WAL 记录");
        store
            .append_clean_run_batch(vec![record])
            .expect("刷入 CleanRun WAL 批次");
        let wal_path = PathBuf::from(format!("{}-wal", store.database_path().display()));
        assert!(
            fs::metadata(&wal_path)
                .expect("读取回收前 CleanRun WAL")
                .len()
                > 0
        );
        assert!(
            store
                .try_checkpoint_clean_run_wal_between_batches()
                .expect("执行 CleanRun 批次间物理 WAL 回收")
        );
        assert_eq!(
            fs::metadata(&wal_path)
                .expect("读取回收后 CleanRun WAL")
                .len(),
            0
        );
        let mut exposed = Vec::new();
        let completed = store
            .visit_completed_clean_run_for_artifacts::<TestEvidenceV1, String, TestCheckpointV1, _>(
                |entry| {
                    exposed.push(entry.sequence);
                    Ok(())
                },
            )
            .expect("物理 WAL 回收后检查 CleanRun 完成资格");
        assert!(completed.is_none());
        assert!(exposed.is_empty());
        drop(store);

        let mut reopened =
            LocalTransactionLogStoreV2::open_recovery(root).expect("重新打开未完成 CleanRun");
        let late = encode_transaction_record_v2(
            &TestEvidenceV1 {
                event: "illegal-resume".to_owned(),
                value: 2,
            },
            &"REGISTER_REQUEST",
        )
        .expect("预编码非法续跑记录");
        assert!(
            matches!(reopened.append_clean_run_batch(vec![late]), Err(PersistenceErrorV1::Integrity(message)) if message.contains("恢复入口打开"))
        );
    }

    #[test]
    fn sqlite_transaction_log_v2_rejects_orphan_wal_without_main_database() {
        let directory = tempfile::tempdir().expect("创建 V2 孤立 WAL 临时目录");
        let root = directory.path().join("run");
        fs::create_dir_all(&root).expect("创建 V2 孤立 WAL 运行根");
        File::create(root.join(format!("{TRANSACTION_LOG_DATABASE_NAME_V2}-wal")))
            .expect("创建孤立 WAL");
        assert!(matches!(
            LocalTransactionLogStoreV2::create_new(root, paper_transaction_log_identity()),
            Err(PersistenceErrorV1::Integrity(message)) if message.contains("主事务日志缺失")
        ));
    }

    #[test]
    fn sqlite_transaction_log_v2_rejects_dangling_database_symlink() {
        let directory = tempfile::tempdir().expect("创建 V2 符号链接临时目录");
        let root = directory.path().join("run");
        fs::create_dir_all(&root).expect("创建 V2 符号链接运行根");
        std::os::unix::fs::symlink(
            directory.path().join("missing.sqlite3"),
            root.join(TRANSACTION_LOG_DATABASE_NAME_V2),
        )
        .expect("创建悬空 V2 主库符号链接");
        assert!(matches!(
            LocalTransactionLogStoreV2::create_new(root, paper_transaction_log_identity()),
            Err(PersistenceErrorV1::Integrity(message)) if message.contains("已经包含事务日志")
        ));
    }

    #[test]
    fn run_root_allows_only_one_live_store_writer() {
        let (directory, store) = store();
        let run_root = directory.path().join("run");
        assert!(matches!(
            LocalDurabilityStoreV1::try_new(run_root.clone()),
            Err(PersistenceErrorV1::Io {
                operation: "取得运行根排他锁",
                ..
            })
        ));

        drop(store);
        LocalDurabilityStoreV1::try_new(run_root).expect("原写者释放后允许恢复运行根");
    }

    #[test]
    fn run_root_is_bound_to_one_absolute_path() {
        let directory = tempfile::tempdir_in(".").expect("在当前目录创建相对耐久根夹具");
        let store = LocalDurabilityStoreV1::try_new(directory.path().join("run"))
            .expect("以相对路径创建本地耐久存储");
        assert!(store.root.is_absolute());
    }

    #[test]
    fn artifact_publish_retry_after_uncertain_rename_revalidates_the_same_bundle() {
        let (_directory, mut published_store) = store();
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"test"}"#.to_vec(),
        }];
        let identity = sha256(&members[0].bytes);
        published_store.fail_after_artifact_bundle_rename_before_directory_sync_once();
        assert!(matches!(
            published_store.publish_immutable_artifact_bundle(&identity, &members),
            Err(PersistenceErrorV1::DurabilityUncertain { .. })
        ));

        let published = published_store
            .publish_immutable_artifact_bundle(&identity, &members)
            .expect("重试必须重新验证既有正式目录并跨过父目录同步屏障");
        assert_eq!(
            fs::read(published.join("simulation-run-manifest.json")).expect("读取幂等发布正文"),
            members[0].bytes
        );
    }

    #[test]
    fn artifact_resource_preview_is_one_bundle_across_retries_and_candidates() {
        let (_directory, mut published_store) = store();
        let first_members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"first"}"#.to_vec(),
        }];
        let first_identity = sha256(b"first-artifact-resource-preview");
        let first_preview = published_store
            .preview_artifact_bundle_resources(&first_identity, &first_members)
            .expect("预览首次运行产物包");
        published_store
            .publish_immutable_artifact_bundle(&first_identity, &first_members)
            .expect("发布首次运行产物包");
        assert_eq!(
            published_store
                .preview_artifact_bundle_resources(&first_identity, &first_members)
                .expect("重复预览已发布运行产物包"),
            first_preview
        );

        let (_candidate_directory, store) = store();
        let baseline = store.current_resources().expect("读取候选前资源基线");
        let next_members = [
            ImmutableArtifactMemberV1 {
                file_name: "simulation-run-manifest.json",
                bytes: br#"{"schema":"test"}"#.to_vec(),
            },
            ImmutableArtifactMemberV1 {
                file_name: "summary.json",
                bytes: br#"{"outcome":"completed"}"#.to_vec(),
            },
        ];
        let next_identity = sha256(b"next-artifact-resource-preview");
        let candidate = store
            .root
            .join(ARTIFACT_DIRECTORY_NAME)
            .join(format!(".{}.candidate", next_identity.0));
        fs::create_dir_all(&candidate).expect("创建同身份遗留候选目录");
        fs::write(candidate.join(next_members[0].file_name), b"partial")
            .expect("写入部分遗留候选成员");

        let preview = store
            .preview_artifact_bundle_resources(&next_identity, &next_members)
            .expect("预览应替换遗留候选后再计入完整包");
        let member_count = u64::try_from(next_members.len()).expect("成员数量可计量");
        let member_bytes = next_members.iter().try_fold(0_u64, |total, member| {
            total.checked_add(u64::try_from(member.bytes.len()).ok()?)
        });
        let member_bytes = member_bytes.expect("成员字节可计量");
        assert_eq!(
            preview.artifact_file_count,
            baseline.artifact_file_count + member_count
        );
        assert_eq!(
            preview.artifact_bytes,
            baseline.artifact_bytes + member_bytes
        );
        assert_eq!(
            preview.runtime_root_file_count,
            baseline.runtime_root_file_count + member_count
        );
        assert_eq!(
            preview.runtime_root_bytes,
            baseline.runtime_root_bytes + member_bytes
        );

        fs::write(candidate.join("undeclared.json"), b"unexpected").expect("写入未声明候选成员");
        assert!(matches!(
            store.preview_artifact_bundle_resources(&next_identity, &next_members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("未声明或非普通成员")
        ));
    }

    #[test]
    fn partial_artifact_candidate_with_prior_identity_is_rebuilt_as_the_requested_bundle() {
        let (_directory, mut store) = store();
        let baseline = store.current_resources().expect("读取部分候选前资源基线");
        let members = [
            ImmutableArtifactMemberV1 {
                file_name: "simulation-run-manifest.json",
                bytes: br#"{"schema":"requested"}"#.to_vec(),
            },
            ImmutableArtifactMemberV1 {
                file_name: "summary.json",
                bytes: br#"{"outcome":"completed"}"#.to_vec(),
            },
        ];
        let prior_identity = sha256(b"prior-partial-artifact-candidate");
        let requested_identity = sha256(b"requested-artifact-bundle");
        let artifacts = store.root.join(ARTIFACT_DIRECTORY_NAME);
        let prior_candidate = artifacts.join(format!(".{}.candidate", prior_identity.0));
        fs::create_dir_all(&prior_candidate).expect("创建旧身份部分候选目录");
        fs::write(
            prior_candidate.join(members[0].file_name),
            &members[0].bytes,
        )
        .expect("写入旧身份部分合法成员");

        assert_eq!(
            store
                .existing_artifact_bundle_identity()
                .expect("候选目录不构成正式包身份"),
            None
        );
        let preview = store
            .preview_artifact_bundle_resources(&requested_identity, &members)
            .expect("旧身份部分候选应按完整目标包预览");
        let member_count = u64::try_from(members.len()).expect("成员数量可计量");
        let member_bytes = members
            .iter()
            .try_fold(0_u64, |total, member| {
                total.checked_add(u64::try_from(member.bytes.len()).ok()?)
            })
            .expect("成员字节可计量");
        assert_eq!(
            preview.artifact_file_count,
            baseline.artifact_file_count + member_count
        );
        assert_eq!(
            preview.artifact_bytes,
            baseline.artifact_bytes + member_bytes
        );

        let non_regular_member = prior_candidate.join(members[1].file_name);
        fs::create_dir(&non_regular_member).expect("构造旧身份候选的非普通成员");
        assert!(matches!(
            store.preview_artifact_bundle_resources(&requested_identity, &members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("未声明或非普通成员")
        ));
        fs::remove_dir(&non_regular_member).expect("清理候选非普通成员夹具");

        assert!(
            store
                .remove_validated_artifact_candidate(&prior_identity, &members)
                .expect("已验证部分候选必须可由资源失败路径安全清理")
        );
        assert!(!prior_candidate.exists());
        assert!(
            !store
                .remove_validated_artifact_candidate(&requested_identity, &members)
                .expect("候选清理重试必须幂等")
        );

        let published = store
            .publish_immutable_artifact_bundle(&requested_identity, &members)
            .expect("清理旧身份部分候选后应重建完整包");
        assert_eq!(published, artifacts.join(&requested_identity.0));
        assert!(!prior_candidate.exists());
        verify_immutable_artifact_bundle(&published, &members).expect("重建后的正式包必须完整");
        assert!(
            !store
                .remove_validated_artifact_candidate(&requested_identity, &members)
                .expect("清理入口不得删除正式包")
        );
        verify_immutable_artifact_bundle(&published, &members).expect("正式包必须保持不变");

        let unknown = artifacts.join(".unknown.candidate");
        fs::write(&unknown, b"unknown").expect("写入未知产物目录项");
        assert!(
            store
                .remove_validated_artifact_candidate(&requested_identity, &members)
                .is_err()
        );
        assert!(unknown.exists(), "未知项必须保持原状");
        verify_immutable_artifact_bundle(&published, &members).expect("未知项不得影响正式包");
    }

    #[test]
    fn artifact_candidate_cleanup_rejects_a_different_bundle_identity_without_deletion() {
        let (_directory, mut store) = store();
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"candidate-b"}"#.to_vec(),
        }];
        let requested_identity = sha256(b"requested-artifact-bundle-a");
        let candidate_identity = sha256(b"uncommitted-artifact-candidate-b");
        let candidate = store
            .root
            .join(ARTIFACT_DIRECTORY_NAME)
            .join(format!(".{}.candidate", candidate_identity.0));
        fs::create_dir_all(&candidate).expect("创建 B 身份候选目录");
        fs::write(candidate.join(members[0].file_name), &members[0].bytes)
            .expect("写入 B 身份候选成员");

        assert_eq!(
            store
                .existing_artifact_bundle_identity()
                .expect("候选身份不得冒充正式包身份"),
            None
        );
        assert_eq!(
            store
                .existing_artifact_candidate_identity()
                .expect("读取唯一 B 身份候选"),
            Some(candidate_identity.clone())
        );
        assert!(matches!(
            store.remove_validated_artifact_candidate(&requested_identity, &members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("候选身份与待清理包身份不一致")
        ));
        assert!(candidate.is_dir(), "请求 A 不得删除 B 身份候选目录");
        assert_eq!(
            fs::read(candidate.join(members[0].file_name)).expect("读取未删除的 B 候选成员"),
            members[0].bytes
        );

        fs::create_dir(
            store
                .root
                .join(ARTIFACT_DIRECTORY_NAME)
                .join(&requested_identity.0),
        )
        .expect("创建不同 A 身份正式目录");
        assert!(matches!(
            store.existing_artifact_candidate_identity(),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("不同身份的正式包与候选包")
        ));
        assert!(candidate.is_dir(), "正式身份冲突不得删除 B 身份候选目录");
    }

    #[test]
    fn artifact_preview_preserves_larger_candidate_usage_for_the_hard_limit() {
        let (_directory, store) = store();
        let baseline = store.current_resources().expect("读取大候选前资源基线");
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: b"{}".to_vec(),
        }];
        let prior_identity = sha256(b"oversized-prior-artifact-candidate");
        let requested_identity = sha256(b"small-requested-artifact-bundle");
        let candidate = store
            .root
            .join(ARTIFACT_DIRECTORY_NAME)
            .join(format!(".{}.candidate", prior_identity.0));
        fs::create_dir_all(&candidate).expect("创建大候选目录");
        let oversized = vec![b'x'; 4_096];
        fs::write(candidate.join(members[0].file_name), &oversized)
            .expect("写入合法名称的大候选成员");

        let preview = store
            .preview_artifact_bundle_resources(&requested_identity, &members)
            .expect("预览必须保留候选实际峰值");
        assert_eq!(preview.artifact_file_count, 1);
        assert_eq!(preview.artifact_bytes, 4_096);
        assert_eq!(
            preview.runtime_root_bytes,
            baseline.runtime_root_bytes + 4_096
        );

        let mut policy = crate::resource::SimulationResourcePolicyV1::component_golden_v1();
        policy.durable.artifacts.max_bytes = 4_095;
        let mut usage = crate::resource::SimulationResourceUsageV1::default();
        usage.durable.artifacts = crate::resource::ResourceQuantityV1::new(
            preview.artifact_file_count,
            preview.artifact_bytes,
        );
        usage.durable.runtime_root = crate::resource::ResourceQuantityV1::new(
            preview.runtime_root_file_count,
            preview.runtime_root_bytes,
        );
        let limit = policy
            .validate_candidate(&usage)
            .expect_err("候选实际峰值必须在允许清理前命中硬上限")
            .limit_reached()
            .expect("候选超限必须形成强类型事实")
            .clone();
        assert_eq!(limit.scope, crate::resource::ResourceLimitScopeV1::Durable);
        assert_eq!(limit.kind, crate::resource::ResourceLimitKindV1::Artifacts);
        assert_eq!(
            limit.exceeded_dimension,
            crate::resource::ResourceLimitDimensionV1::Bytes
        );
    }

    #[test]
    fn artifact_bundle_rejects_another_formal_identity_without_deleting_it() {
        let (_directory, mut store) = store();
        let first_members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"first"}"#.to_vec(),
        }];
        let first_identity = sha256(b"first-formal-artifact");
        let first_target = store
            .publish_immutable_artifact_bundle(&first_identity, &first_members)
            .expect("发布首个正式运行产物包");
        let second_members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"second"}"#.to_vec(),
        }];
        let second_identity = sha256(b"second-formal-artifact");

        assert!(matches!(
            store.preview_artifact_bundle_resources(&second_identity, &second_members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("其他正式包或未知目录项")
        ));
        assert!(matches!(
            store.publish_immutable_artifact_bundle(&second_identity, &second_members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("其他正式包或未知目录项")
        ));
        assert_eq!(
            fs::read(first_target.join(first_members[0].file_name)).expect("读取保留的首个正式包"),
            first_members[0].bytes
        );
        assert!(
            !store
                .root
                .join(ARTIFACT_DIRECTORY_NAME)
                .join(&second_identity.0)
                .exists()
        );
    }

    #[test]
    fn artifact_publish_retry_rejects_tampered_or_symlinked_formal_target() {
        let (directory, mut published_store) = store();
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"test"}"#.to_vec(),
        }];
        let identity = sha256(&members[0].bytes);
        published_store.fail_after_artifact_bundle_rename_before_directory_sync_once();
        assert!(matches!(
            published_store.publish_immutable_artifact_bundle(&identity, &members),
            Err(PersistenceErrorV1::DurabilityUncertain { .. })
        ));
        let formal_target = directory
            .path()
            .join("run")
            .join(ARTIFACT_DIRECTORY_NAME)
            .join(&identity.0);
        fs::write(
            formal_target.join("simulation-run-manifest.json"),
            br#"{"schema":"tampered"}"#,
        )
        .expect("篡改正式目标正文");
        assert!(matches!(
            published_store.publish_immutable_artifact_bundle(&identity, &members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("正文冲突")
        ));

        let (symlink_directory, mut symlink_store) = store();
        let symlink_identity = sha256(b"symlink-target");
        let external = symlink_directory.path().join("external-artifact");
        fs::create_dir(&external).expect("创建符号链接目标目录");
        fs::write(
            external.join("simulation-run-manifest.json"),
            &members[0].bytes,
        )
        .expect("写入符号链接目标正文");
        fs::create_dir(
            symlink_directory
                .path()
                .join("run")
                .join(ARTIFACT_DIRECTORY_NAME),
        )
        .expect("创建运行产物根目录");
        std::os::unix::fs::symlink(
            &external,
            symlink_directory
                .path()
                .join("run")
                .join(ARTIFACT_DIRECTORY_NAME)
                .join(&symlink_identity.0),
        )
        .expect("构造正式目标符号链接");
        assert!(matches!(
            symlink_store.publish_immutable_artifact_bundle(&symlink_identity, &members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("不是普通目录")
        ));
    }

    #[test]
    fn missing_current_is_rebuilt_from_latest_immutable_checkpoint_without_deleting_evidence() {
        let (directory, mut store) = store();
        store
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 1,
                },
                &TestCheckpointV1 {
                    state: "prepared".to_owned(),
                    applied: 1,
                },
            )
            .expect("建立待截断运行根");
        let run_root = directory.path().join("run");
        let evidence_before = fs::read_dir(run_root.join(EVIDENCE_DIRECTORY_NAME))
            .expect("读取截断前证据")
            .map(|entry| entry.expect("合法证据目录项").file_name())
            .collect::<BTreeSet<_>>();
        drop(store);
        fs::remove_file(run_root.join(CURRENT_FILE_NAME)).expect("模拟 CURRENT 被截断");

        let reopened = LocalDurabilityStoreV1::try_new(run_root.clone())
            .expect("缺失 CURRENT 时必须从最高不可变检查点重建");
        assert_eq!(
            reopened
                .recover::<TestEvidenceV1, TestCheckpointV1>()
                .expect("恢复重建后的 CURRENT")
                .expect("必须存在不可变检查点")
                .through_commit_sequence,
            1
        );
        let evidence_after = fs::read_dir(run_root.join(EVIDENCE_DIRECTORY_NAME))
            .expect("读取失败恢复后的证据")
            .map(|entry| entry.expect("合法证据目录项").file_name())
            .collect::<BTreeSet<_>>();
        assert_eq!(evidence_after, evidence_before);
    }

    #[test]
    fn immutable_evidence_and_current_checkpoint_round_trip() {
        let (_directory, mut store) = store();
        assert!(store.recover_json().expect("读取空耐久存储").is_none());

        let first_evidence = TestEvidenceV1 {
            event: "create".to_owned(),
            value: 1,
        };
        let first_checkpoint = TestCheckpointV1 {
            state: "prepared".to_owned(),
            applied: 1,
        };
        let first = store
            .commit(&first_evidence, &first_checkpoint)
            .expect("提交第一项耐久事务");
        assert_eq!(first.sequence, 1);
        assert!(first.evidence_path.is_file());
        assert!(first.current_path.is_file());

        let second_evidence = TestEvidenceV1 {
            event: "activate".to_owned(),
            value: 2,
        };
        let second_checkpoint = TestCheckpointV1 {
            state: "active".to_owned(),
            applied: 2,
        };
        let second = store
            .commit(&second_evidence, &second_checkpoint)
            .expect("提交第二项耐久事务");
        assert_eq!(second.sequence, 2);
        assert_ne!(first.evidence_sha256, second.evidence_sha256);
        assert!(!first.checkpoint_path.exists());
        assert!(second.checkpoint_path.is_file());

        let recovered = store
            .recover::<TestEvidenceV1, TestCheckpointV1>()
            .expect("恢复强类型耐久状态")
            .expect("CURRENT 必须存在");
        assert_eq!(recovered.through_commit_sequence, 2);
        assert_eq!(recovered.checkpoint_sha256, second.checkpoint_sha256);
        assert_eq!(recovered.checkpoint, second_checkpoint);
        assert_eq!(
            recovered
                .evidence
                .iter()
                .map(|member| member.body.clone())
                .collect::<Vec<_>>(),
            vec![first_evidence, second_evidence]
        );
    }

    #[test]
    fn failure_before_current_publish_keeps_old_checkpoint_and_does_not_reserve_sequence() {
        let (directory, mut store) = store();
        let original_evidence = TestEvidenceV1 {
            event: "create".to_owned(),
            value: 1,
        };
        let original_checkpoint = TestCheckpointV1 {
            state: "prepared".to_owned(),
            applied: 1,
        };
        store
            .commit(&original_evidence, &original_checkpoint)
            .expect("提交原 CURRENT");

        let abandoned_evidence = serde_json::to_vec(&TestEvidenceV1 {
            event: "abandoned-activate".to_owned(),
            value: 2,
        })
        .expect("编码待放弃证据");
        let abandoned_checkpoint = serde_json::to_vec(&TestCheckpointV1 {
            state: "abandoned-active".to_owned(),
            applied: 2,
        })
        .expect("编码待放弃检查点");
        assert_eq!(
            store.commit_json_with_fault(
                &abandoned_evidence,
                &abandoned_checkpoint,
                CommitFaultV1::BeforeCheckpointPublish,
            ),
            Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish)
        );

        let recovered = store
            .recover::<TestEvidenceV1, TestCheckpointV1>()
            .expect("失败后旧 CURRENT 仍可恢复")
            .expect("旧 CURRENT 必须存在");
        assert_eq!(recovered.through_commit_sequence, 1);
        assert_eq!(recovered.checkpoint, original_checkpoint);
        assert_eq!(recovered.evidence.len(), 1);

        let replacement_evidence = TestEvidenceV1 {
            event: "replacement-activate".to_owned(),
            value: 3,
        };
        let replacement_checkpoint = TestCheckpointV1 {
            state: "replacement-active".to_owned(),
            applied: 2,
        };
        let retried = store
            .commit(&replacement_evidence, &replacement_checkpoint)
            .expect("同一句柄必须清理未提交孤儿并允许不同正文");
        assert_eq!(retried.sequence, 2);
        let recovered = store
            .recover::<TestEvidenceV1, TestCheckpointV1>()
            .expect("替代提交后恢复")
            .expect("新 CURRENT 必须存在");
        assert_eq!(recovered.through_commit_sequence, 2);
        assert_eq!(recovered.checkpoint, replacement_checkpoint);
        assert_eq!(recovered.evidence[1].body, replacement_evidence);

        let abandoned_after_restart = serde_json::to_vec(&TestEvidenceV1 {
            event: "abandoned-settlement".to_owned(),
            value: 4,
        })
        .expect("编码重启前待放弃证据");
        let abandoned_checkpoint_after_restart = serde_json::to_vec(&TestCheckpointV1 {
            state: "abandoned-settled".to_owned(),
            applied: 3,
        })
        .expect("编码重启前待放弃检查点");
        assert_eq!(
            store.commit_json_with_fault(
                &abandoned_after_restart,
                &abandoned_checkpoint_after_restart,
                CommitFaultV1::BeforeCheckpointPublish,
            ),
            Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish)
        );
        drop(store);

        let run_root = directory.path().join("run");
        let mut reopened = LocalDurabilityStoreV1::try_new(run_root)
            .expect("重启打开时必须清理 CURRENT 水位之后的孤儿");
        let final_evidence = TestEvidenceV1 {
            event: "replacement-settlement".to_owned(),
            value: 5,
        };
        let final_checkpoint = TestCheckpointV1 {
            state: "settled".to_owned(),
            applied: 3,
        };
        let committed = reopened
            .commit(&final_evidence, &final_checkpoint)
            .expect("重启后必须允许同序号的不同正文");
        assert_eq!(committed.sequence, 3);
        let recovered = reopened
            .recover::<TestEvidenceV1, TestCheckpointV1>()
            .expect("重启替代提交后恢复")
            .expect("最终 CURRENT 必须存在");
        assert_eq!(recovered.checkpoint, final_checkpoint);
        assert_eq!(recovered.evidence[2].body, final_evidence);
    }

    #[test]
    fn commit_resource_preview_replaces_known_orphans_and_rejects_unknown_candidates() {
        let (directory, mut store) = store();
        let evidence = TestEvidenceV1 {
            event: "create".to_owned(),
            value: 1,
        };
        let checkpoint = TestCheckpointV1 {
            state: "prepared".to_owned(),
            applied: 1,
        };
        let baseline = store
            .preview_commit_resources(&evidence, &checkpoint)
            .expect("预览没有孤立候选的首次提交");
        let evidence_json = serde_json::to_vec(&evidence).expect("编码待故障证据");
        let checkpoint_json = serde_json::to_vec(&checkpoint).expect("编码待故障检查点");
        assert_eq!(
            store.commit_json_with_fault(
                &evidence_json,
                &checkpoint_json,
                CommitFaultV1::BeforeCheckpointPublish,
            ),
            Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish)
        );
        assert_eq!(
            store
                .preview_commit_resources(&evidence, &checkpoint)
                .expect("重试预览应先扣除可清理的孤立候选"),
            baseline
        );

        fs::write(directory.path().join("run/.unknown.candidate"), b"unknown")
            .expect("写入未知候选文件");
        assert!(matches!(
            store.preview_commit_resources(&evidence, &checkpoint),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("未知候选文件")
        ));
    }

    #[test]
    fn committed_resource_preview_matches_real_commit_after_cleanup() {
        let (directory, mut store) = store();
        for value in 1..=8 {
            store
                .commit(
                    &TestEvidenceV1 {
                        event: format!("prepare-{value}"),
                        value,
                    },
                    &TestCheckpointV1 {
                        state: format!("prepared-{value}"),
                        applied: value,
                    },
                )
                .expect("建立已有 CURRENT 与旧检查点");
        }
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"retained-artifact"}"#.to_vec(),
        }];
        let artifact_identity = sha256(&members[0].bytes);
        store
            .publish_immutable_artifact_bundle(&artifact_identity, &members)
            .expect("建立提交不得清理的正式运行产物");
        let maintenance_ready_before = store
            .current_resources()
            .expect("读取注入孤立候选前的规范稳态");

        let orphan_evidence = serde_json::to_vec(&TestEvidenceV1 {
            event: "orphan".to_owned(),
            value: 9,
        })
        .expect("编码孤立候选证据");
        let orphan_checkpoint = serde_json::to_vec(&TestCheckpointV1 {
            state: "orphan".to_owned(),
            applied: 9,
        })
        .expect("编码孤立候选检查点");
        assert_eq!(
            store.commit_json_with_fault(
                &orphan_evidence,
                &orphan_checkpoint,
                CommitFaultV1::BeforeCheckpointPublish,
            ),
            Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish)
        );

        let evidence = TestEvidenceV1 {
            event: "activate".to_owned(),
            value: 9,
        };
        let checkpoint = TestCheckpointV1 {
            state: "active".to_owned(),
            applied: 9,
        };
        let following_evidence = TestEvidenceV1 {
            event: "settle".to_owned(),
            value: 10,
        };
        let following_checkpoint = TestCheckpointV1 {
            state: "settled".to_owned(),
            applied: 10,
        };
        let first_encoded = encode_commit_candidate_v1(
            store.current.as_ref(),
            &serde_json::to_vec(&evidence).expect("编码首提交证据"),
            &serde_json::to_vec(&checkpoint).expect("编码首提交检查点"),
        )
        .expect("构造首提交候选");
        let second_encoded = encode_commit_candidate_v1(
            Some(&first_encoded.current),
            &serde_json::to_vec(&following_evidence).expect("编码第二提交证据"),
            &serde_json::to_vec(&following_checkpoint).expect("编码第二提交检查点"),
        )
        .expect("构造第二提交候选");
        let second_envelope: EvidenceEnvelopeV1 =
            serde_json::from_slice(&second_encoded.evidence_bytes).expect("解码第二提交信封");
        assert_eq!(second_envelope.sequence, 10, "第二候选必须使用 sequence+2");
        assert_eq!(
            second_envelope.predecessor_evidence_sha256,
            Some(first_encoded.current.content.evidence_head_sha256.clone()),
            "第二候选必须绑定首候选的证据头"
        );
        let (previews, following_during_commit_peak) = store
            .preview_consecutive_commit_resources(
                &evidence,
                &checkpoint,
                &following_evidence,
                &following_checkpoint,
            )
            .expect("同一候选链应给出首提交状态与紧接的第二提交峰值");
        assert_eq!(
            previews.maintenance_ready_before, maintenance_ready_before,
            "提交前稳态必须排除可清理孤立候选"
        );
        assert!(
            previews.during_commit_peak.runtime_root_file_count
                > previews
                    .maintenance_succeeded_steady
                    .runtime_root_file_count
        );
        assert_eq!(previews.maintenance_succeeded_steady.artifact_file_count, 1);
        assert_eq!(
            previews.maintenance_succeeded_steady.artifact_bytes,
            u64::try_from(members[0].bytes.len()).expect("测试产物字节可表示")
        );

        store
            .commit(&evidence, &checkpoint)
            .expect("真实提交应清理孤立候选并替换旧 CURRENT 与旧检查点");
        assert_eq!(
            previews.maintenance_succeeded_steady,
            store.current_resources().expect("读取真实提交后资源")
        );
        assert_eq!(
            following_during_commit_peak,
            store
                .preview_commit_resources(&following_evidence, &following_checkpoint)
                .expect("真实首提交后预览第二次提交峰值")
        );
        store
            .commit(&following_evidence, &following_checkpoint)
            .expect("完成真实连续第二次提交");
        assert!(directory.path().join("run/CURRENT").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn committed_resource_preview_rejects_non_regular_current_and_orphan_candidate() {
        use std::os::unix::fs::symlink;

        let (current_directory, mut current_store) = store();
        let evidence = TestEvidenceV1 {
            event: "create".to_owned(),
            value: 1,
        };
        let checkpoint = TestCheckpointV1 {
            state: "prepared".to_owned(),
            applied: 1,
        };
        current_store
            .commit(&evidence, &checkpoint)
            .expect("建立真实 CURRENT");
        let current = current_directory.path().join("run/CURRENT");
        let saved_current = current_directory.path().join("saved-current");
        fs::rename(&current, &saved_current).expect("移走真实 CURRENT");
        symlink(&saved_current, &current).expect("建立 CURRENT 符号链接");
        assert!(matches!(
            current_store.preview_consecutive_commit_resources(
                &evidence,
                &checkpoint,
                &evidence,
                &checkpoint,
            ),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("CURRENT 不是普通文件")
        ));

        let (_candidate_directory, mut candidate_store) = store();
        let evidence_json = serde_json::to_vec(&evidence).expect("编码孤立候选证据");
        let checkpoint_json = serde_json::to_vec(&checkpoint).expect("编码孤立候选检查点");
        assert_eq!(
            candidate_store.commit_json_with_fault(
                &evidence_json,
                &checkpoint_json,
                CommitFaultV1::BeforeCheckpointPublish,
            ),
            Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish)
        );
        let candidate = fs::read_dir(&candidate_store.evidence_directory)
            .expect("枚举孤立证据候选")
            .map(|entry| entry.expect("读取孤立证据候选").path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".candidate"))
            })
            .expect("故障必须留下证据候选");
        fs::remove_file(&candidate).expect("移除普通候选文件");
        fs::create_dir(&candidate).expect("以同名目录替代候选");
        assert!(matches!(
            candidate_store.preview_consecutive_commit_resources(
                &evidence,
                &checkpoint,
                &evidence,
                &checkpoint,
            ),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("候选不是普通文件")
        ));
    }

    #[test]
    fn failure_before_first_checkpoint_publish_is_uncommitted_only_for_new_run_retry() {
        let (directory, mut store) = store();
        let run_root = directory.path().join("run");
        let evidence = serde_json::to_vec(&TestEvidenceV1 {
            event: "uncommitted-create".to_owned(),
            value: 1,
        })
        .expect("编码首次未提交证据");
        let checkpoint = serde_json::to_vec(&TestCheckpointV1 {
            state: "uncommitted-prepared".to_owned(),
            applied: 1,
        })
        .expect("编码首次未提交检查点");
        assert_eq!(
            store.commit_json_with_fault(
                &evidence,
                &checkpoint,
                CommitFaultV1::BeforeCheckpointPublish,
            ),
            Err(PersistenceErrorV1::InjectedBeforeCheckpointPublish)
        );
        drop(store);

        assert!(matches!(
            LocalDurabilityStoreV1::try_open_recovery(run_root.clone()),
            Err(PersistenceErrorV1::Integrity(_))
        ));

        let mut reopened = LocalDurabilityStoreV1::try_new(run_root)
            .expect("新运行重试必须清理首个提交点之前的孤儿");
        let receipt = reopened
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 2,
                },
                &TestCheckpointV1 {
                    state: "prepared".to_owned(),
                    applied: 1,
                },
            )
            .expect("新运行重试必须重新使用序号一");
        assert_eq!(receipt.sequence, 1);
    }

    #[test]
    fn evidence_commit_point_distinguishes_uncommitted_checkpoint_from_committed_transaction() {
        let (directory, mut store) = store();
        let first_checkpoint = TestCheckpointV1 {
            state: "prepared".to_owned(),
            applied: 1,
        };
        store
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 1,
                },
                &first_checkpoint,
            )
            .expect("提交基线事务");

        let uncommitted_evidence = serde_json::to_vec(&TestEvidenceV1 {
            event: "uncommitted-activate".to_owned(),
            value: 2,
        })
        .expect("编码未提交证据");
        let uncommitted_checkpoint = serde_json::to_vec(&TestCheckpointV1 {
            state: "uncommitted-active".to_owned(),
            applied: 2,
        })
        .expect("编码超前检查点");
        assert_eq!(
            store.commit_json_with_fault(
                &uncommitted_evidence,
                &uncommitted_checkpoint,
                CommitFaultV1::AfterCheckpointBeforeEvidencePublish,
            ),
            Err(PersistenceErrorV1::InjectedAfterCheckpointBeforeEvidencePublish)
        );
        drop(store);

        let run_root = directory.path().join("run");
        let mut reopened = LocalDurabilityStoreV1::try_new(run_root.clone())
            .expect("证据提交前崩溃必须恢复上一代");
        let recovered = reopened
            .recover::<TestEvidenceV1, TestCheckpointV1>()
            .expect("恢复上一代")
            .expect("基线事务必须存在");
        assert_eq!(recovered.through_commit_sequence, 1);
        assert_eq!(recovered.checkpoint, first_checkpoint);

        let committed_evidence = serde_json::to_vec(&TestEvidenceV1 {
            event: "committed-activate".to_owned(),
            value: 3,
        })
        .expect("编码已提交证据");
        let committed_checkpoint = TestCheckpointV1 {
            state: "active".to_owned(),
            applied: 2,
        };
        assert_eq!(
            reopened.commit_json_with_fault(
                &committed_evidence,
                &serde_json::to_vec(&committed_checkpoint).expect("编码已提交检查点"),
                CommitFaultV1::AfterEvidenceCommit,
            ),
            Err(PersistenceErrorV1::InjectedAfterEvidenceCommit)
        );
        drop(reopened);

        let recovered = LocalDurabilityStoreV1::try_new(run_root)
            .expect("证据提交后崩溃必须恢复新代")
            .recover::<TestEvidenceV1, TestCheckpointV1>()
            .expect("恢复已提交事务")
            .expect("新代必须存在");
        assert_eq!(recovered.through_commit_sequence, 2);
        assert_eq!(recovered.checkpoint, committed_checkpoint);
        assert_eq!(recovered.evidence[1].body.event, "committed-activate");
    }

    #[test]
    fn recovery_rejects_truncation_gap_and_changed_body() {
        let (truncated_directory, mut truncated_store) = store();
        truncated_store
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 1,
                },
                &TestCheckpointV1 {
                    state: "prepared".to_owned(),
                    applied: 1,
                },
            )
            .expect("建立待截断 CURRENT");
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(truncated_directory.path().join("run/CURRENT"))
            .expect("打开 CURRENT")
            .write_all(b"{")
            .expect("截断 CURRENT");
        assert!(truncated_store.recover_json().is_err());

        let (_gap_directory, mut gap_store) = store();
        let first = gap_store
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 1,
                },
                &TestCheckpointV1 {
                    state: "prepared".to_owned(),
                    applied: 1,
                },
            )
            .expect("建立证据序号 1");
        gap_store
            .commit(
                &TestEvidenceV1 {
                    event: "activate".to_owned(),
                    value: 2,
                },
                &TestCheckpointV1 {
                    state: "active".to_owned(),
                    applied: 2,
                },
            )
            .expect("建立证据序号 2");
        fs::remove_file(first.evidence_path).expect("删除证据序号 1 制造前驱缺口");
        assert!(gap_store.recover_json().is_err());

        let (_changed_directory, mut changed_store) = store();
        let committed = changed_store
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 1,
                },
                &TestCheckpointV1 {
                    state: "prepared".to_owned(),
                    applied: 1,
                },
            )
            .expect("建立待修改证据");
        fs::write(
            committed.evidence_path,
            br#"{"event":"tampered","value":1}"#,
        )
        .expect("改写证据正文");
        assert!(changed_store.recover_json().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn recovery_rejects_current_symlink() {
        use std::os::unix::fs::symlink;

        let (directory, mut store) = store();
        store
            .commit(
                &TestEvidenceV1 {
                    event: "create".to_owned(),
                    value: 1,
                },
                &TestCheckpointV1 {
                    state: "prepared".to_owned(),
                    applied: 1,
                },
            )
            .expect("建立真实 CURRENT");
        let current = directory.path().join("run/CURRENT");
        let target = directory.path().join("saved-current");
        fs::rename(&current, &target).expect("移走真实 CURRENT");
        symlink(&target, &current).expect("建立 CURRENT 符号链接");

        assert!(matches!(
            store.recover_json(),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("CURRENT 不是普通文件")
        ));
    }
}
