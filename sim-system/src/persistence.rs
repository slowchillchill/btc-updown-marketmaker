use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use contracts::Sha256Hex;
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

pub(crate) struct ImmutableArtifactMemberV1 {
    pub(crate) file_name: &'static str,
    pub(crate) bytes: Vec<u8>,
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
                remove_orphan_candidates(&root)?;
                remove_orphan_candidates(&checkpoint_directory)?;
                remove_orphan_candidates(&evidence_directory)?;
                remove_checkpoints_after(&checkpoint_directory, 0)?;
            }
        }
        if let Some(recovered) = &recovered {
            remove_orphan_candidates(&root)?;
            remove_orphan_candidates(&checkpoint_directory)?;
            remove_orphan_candidates(&evidence_directory)?;
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
        if target
            .try_exists()
            .map_err(|error| io_error("检查运行产物目录", error))?
        {
            verify_immutable_artifact_bundle(&target, members)?;
            sync_directory(&artifacts, &target)?;
            return Ok(target);
        }

        let candidate = artifacts.join(format!(".{}.candidate", bundle_identity.0));
        if candidate
            .try_exists()
            .map_err(|error| io_error("检查运行产物候选目录", error))?
        {
            remove_known_artifact_candidate(&candidate, &expected_names)?;
        }
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
        remove_orphan_candidates(&self.root)?;
        remove_orphan_candidates(&self.checkpoint_directory)?;
        remove_orphan_candidates(&self.evidence_directory)?;
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
    Ok(Some(current))
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

fn remove_orphan_candidates(directory: &Path) -> Result<(), PersistenceErrorV1> {
    let entries = fs::read_dir(directory).map_err(|error| io_error("扫描候选文件", error))?;
    let mut removed = false;
    for entry in entries {
        let entry = entry.map_err(|error| io_error("读取候选目录项", error))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with('.') && name.ends_with(".candidate") {
            fs::remove_file(entry.path()).map_err(|error| io_error("清理孤立候选文件", error))?;
            removed = true;
        }
    }
    if removed {
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

fn remove_known_artifact_candidate(
    candidate: &Path,
    expected_names: &std::collections::BTreeSet<&'static str>,
) -> Result<(), PersistenceErrorV1> {
    ensure_directory(candidate, "运行产物候选目录")?;
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

    fn store() -> (tempfile::TempDir, LocalDurabilityStoreV1) {
        let directory = tempfile::tempdir().expect("创建任务 4.2 临时目录");
        let store = LocalDurabilityStoreV1::try_new(directory.path().join("run"))
            .expect("创建本地耐久存储");
        (directory, store)
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
        let (_directory, mut store) = store();
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"test"}"#.to_vec(),
        }];
        let identity = sha256(&members[0].bytes);
        store.fail_after_artifact_bundle_rename_before_directory_sync_once();
        assert!(matches!(
            store.publish_immutable_artifact_bundle(&identity, &members),
            Err(PersistenceErrorV1::DurabilityUncertain { .. })
        ));

        let published = store
            .publish_immutable_artifact_bundle(&identity, &members)
            .expect("重试必须重新验证既有正式目录并跨过父目录同步屏障");
        assert_eq!(
            fs::read(published.join("simulation-run-manifest.json")).expect("读取幂等发布正文"),
            members[0].bytes
        );
    }

    #[test]
    fn artifact_publish_retry_rejects_tampered_or_symlinked_formal_target() {
        let (directory, mut store) = store();
        let members = [ImmutableArtifactMemberV1 {
            file_name: "simulation-run-manifest.json",
            bytes: br#"{"schema":"test"}"#.to_vec(),
        }];
        let identity = sha256(&members[0].bytes);
        store.fail_after_artifact_bundle_rename_before_directory_sync_once();
        assert!(matches!(
            store.publish_immutable_artifact_bundle(&identity, &members),
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
            store.publish_immutable_artifact_bundle(&identity, &members),
            Err(PersistenceErrorV1::Integrity(message))
                if message.contains("正文冲突")
        ));

        let symlink_identity = sha256(b"symlink-target");
        let external = directory.path().join("external-artifact");
        fs::create_dir(&external).expect("创建符号链接目标目录");
        fs::write(
            external.join("simulation-run-manifest.json"),
            &members[0].bytes,
        )
        .expect("写入符号链接目标正文");
        std::os::unix::fs::symlink(
            &external,
            directory
                .path()
                .join("run")
                .join(ARTIFACT_DIRECTORY_NAME)
                .join(&symlink_identity.0),
        )
        .expect("构造正式目标符号链接");
        assert!(matches!(
            store.publish_immutable_artifact_bundle(&symlink_identity, &members),
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
