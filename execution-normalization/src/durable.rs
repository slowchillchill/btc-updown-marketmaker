use std::error::Error;
use std::fmt;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use contracts::execution_normalization::{
    DurableSourceReferenceV1, EXECUTION_NORMALIZATION_CONTRACT_V1,
    ExecutionNormalizationContextReferenceV1, ExecutionNormalizationContextV1,
    NoePublishedContainerReferenceV1, NoeResultReferenceV1, NoeSourceCommitBoundaryV1,
    SimV2ContextSourceV1, SimV2SourceWatermarkV1, SourceNeutralAccountIdentityV1,
    SourceNeutralControlResultV1, SourceNeutralControlResultV2,
};
use contracts::{AccountInstanceKeyV1, Sha256Hex};
use rustix::fs::{RenameFlags, renameat_with};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::core::{
    CoreErrorV1, NormalizationBatchInput, NormalizationCore, NormalizationRecord,
    SourceNeutralEntityKeyV1,
};

pub const NOE_CONTEXT_CONTAINER_V1: &str = "noe_context_container_v1";
pub const NOE_COMMIT_CONTAINER_V1: &str = "noe_commit_container_v1";
pub const NOE_CONTAINER_ENCODING_V1: &str = "serde_json_v1";

const CONTEXT_FILE_NAME: &str = "context.json";
const COMMITS_DIRECTORY_NAME: &str = "commits";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ContextPayloadV1 {
    container_version: String,
    encoding: String,
    context: ExecutionNormalizationContextV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ContextContainerV1 {
    payload: ContextPayloadV1,
    boundary_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CommitPayload<T, C> {
    container_version: String,
    encoding: String,
    commit_sequence: u64,
    boundary: NoeSourceCommitBoundaryV1,
    records: Vec<NormalizationRecord<T, C>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CommitContainer<T, C> {
    payload: CommitPayload<T, C>,
    boundary_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableCommit<T, C = SourceNeutralControlResultV1> {
    pub commit_sequence: u64,
    pub boundary: NoeSourceCommitBoundaryV1,
    pub records: Vec<NormalizationRecord<T, C>>,
    pub container_reference: NoePublishedContainerReferenceV1,
}

pub fn sim_v2_context(
    seed: SimV2ContextSeedV1,
    contract_version: &str,
) -> Result<ExecutionNormalizationContextV1, DurableErrorV1> {
    let account_scope = serde_json::to_string(&seed.account_instance_key).map_err(|error| {
        DurableErrorV1::Core(CoreErrorV1::IdentityEncoding {
            message: error.to_string(),
        })
    })?;
    let account_identity = SourceNeutralAccountIdentityV1 {
        source_category: "platform".to_owned(),
        environment: "simulation".to_owned(),
        run_scope: seed.run_id,
        account_scope,
    };
    let source = SimV2ContextSourceV1 {
        account_instance_key: seed.account_instance_key,
        run_opened: seed.run_opened,
        account_opening_evidence_identity: seed.account_opening_evidence_identity,
        account_opening: seed.account_opening,
    };
    let context_reference = derive_sim_v2_context_reference(
        contract_version,
        &account_identity,
        &source,
        &seed.implementation_identity,
    )?;
    Ok(ExecutionNormalizationContextV1 {
        context_reference: context_reference.clone(),
        account_identity,
        source,
        contract_version: contract_version.to_owned(),
        implementation_identity: seed.implementation_identity,
    })
}

impl<
    T: NoeTradePayload<C>,
    C: Clone + std::fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
> DurableCommit<T, C>
{
    pub fn result_reference(&self, record_ordinal: u64) -> Option<NoeResultReferenceV1> {
        (record_ordinal < self.records.len() as u64).then(|| NoeResultReferenceV1 {
            commit_container: self.container_reference.clone(),
            record_ordinal,
        })
    }

    pub fn result_references(&self) -> impl Iterator<Item = NoeResultReferenceV1> + '_ {
        (0..self.records.len() as u64).map(|record_ordinal| NoeResultReferenceV1 {
            commit_container: self.container_reference.clone(),
            record_ordinal,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableCommitOutcomeV1 {
    Committed {
        commit_sequence: u64,
        container_reference: NoePublishedContainerReferenceV1,
    },
    AlreadyDurable {
        source_watermark: SimV2SourceWatermarkV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableErrorV1 {
    Io {
        operation: &'static str,
        path: PathBuf,
        message: String,
    },
    ContextAlreadyExists {
        path: PathBuf,
    },
    ContextNotFound {
        path: PathBuf,
    },
    ContainerDecode {
        path: PathBuf,
        message: String,
    },
    UnsupportedContainerVersion {
        path: PathBuf,
        found: String,
    },
    UnsupportedEncoding {
        path: PathBuf,
        found: String,
    },
    BoundaryDigestMismatch {
        path: PathBuf,
        expected: Sha256Hex,
        found: Sha256Hex,
    },
    ContextReferenceMismatch {
        expected: ExecutionNormalizationContextReferenceV1,
        found: ExecutionNormalizationContextReferenceV1,
    },
    CommitSequenceMismatch {
        expected: u64,
        found: u64,
    },
    SourceWatermarkBoundary {
        expected_run_id: Option<String>,
        expected_sequence: u64,
        found: SimV2SourceWatermarkV1,
    },
    CommitAlreadyExists {
        commit_sequence: u64,
    },
    RecordOrdinalOutOfRange {
        path: PathBuf,
        record_ordinal: u64,
        record_count: u64,
    },
    DurabilityUncertain {
        path: PathBuf,
        message: String,
    },
    Core(CoreErrorV1),
}

impl fmt::Display for DurableErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                message,
            } => write!(
                formatter,
                "耐久操作失败：{operation}，路径 {}：{message}",
                path.display()
            ),
            Self::ContextAlreadyExists { path } => {
                write!(formatter, "NOE 上下文已经存在：{}", path.display())
            }
            Self::ContextNotFound { path } => {
                write!(formatter, "NOE 上下文不存在：{}", path.display())
            }
            Self::ContainerDecode { path, message } => {
                write!(formatter, "NOE 容器无法解码：{}：{message}", path.display())
            }
            Self::UnsupportedContainerVersion { path, found } => write!(
                formatter,
                "NOE 容器版本不受支持：{}：{found}",
                path.display()
            ),
            Self::UnsupportedEncoding { path, found } => write!(
                formatter,
                "NOE 容器编码不受支持：{}：{found}",
                path.display()
            ),
            Self::BoundaryDigestMismatch { path, .. } => {
                write!(formatter, "NOE 容器边界摘要不匹配：{}", path.display())
            }
            Self::ContextReferenceMismatch { expected, found } => write!(
                formatter,
                "NOE 上下文引用不匹配：期望 {}，实际 {}",
                expected.0, found.0
            ),
            Self::CommitSequenceMismatch { expected, found } => write!(
                formatter,
                "NOE 提交序列不连续：期望 {expected}，实际 {found}"
            ),
            Self::SourceWatermarkBoundary {
                expected_run_id,
                expected_sequence,
                found,
            } => write!(
                formatter,
                "NOE 来源水位越界：期望运行 {:?} 序列 {}，实际 {}:{}",
                expected_run_id, expected_sequence, found.run_id, found.projection_batch_sequence
            ),
            Self::CommitAlreadyExists { commit_sequence } => {
                write!(formatter, "NOE 提交文件已经存在：{commit_sequence}")
            }
            Self::RecordOrdinalOutOfRange {
                path,
                record_ordinal,
                record_count,
            } => write!(
                formatter,
                "NOE 结果记录序位越界：{}：序位 {record_ordinal}，记录数 {record_count}",
                path.display()
            ),
            Self::DurabilityUncertain { path, message } => write!(
                formatter,
                "NOE 提交发布后的耐久状态不确定：{}：{message}",
                path.display()
            ),
            Self::Core(error) => write!(formatter, "NOE 核心拒绝批次：{error}"),
        }
    }
}

impl Error for DurableErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Core(error) => Some(error),
            _ => None,
        }
    }
}

impl From<CoreErrorV1> for DurableErrorV1 {
    fn from(error: CoreErrorV1) -> Self {
        Self::Core(error)
    }
}

#[derive(Debug)]
pub struct DurableContextStore<T, C = SourceNeutralControlResultV1> {
    context_directory: PathBuf,
    context: ExecutionNormalizationContextV1,
    context_container_reference: NoePublishedContainerReferenceV1,
    commits: Vec<DurableCommit<T, C>>,
    core: NormalizationCore<T, C>,
    poison: Option<PoisonedStoreV1>,
    #[cfg(test)]
    publish_fault: PublishFaultV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimV2ContextSeedV1 {
    pub run_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub run_opened: DurableSourceReferenceV1,
    pub account_opening: DurableSourceReferenceV1,
    pub account_opening_evidence_identity: String,
    pub implementation_identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedNoeResult<T, C = SourceNeutralControlResultV1> {
    pub context: ExecutionNormalizationContextV1,
    pub boundary: NoeSourceCommitBoundaryV1,
    pub record: NormalizationRecord<T, C>,
}

#[derive(Clone, Debug)]
struct PoisonedStoreV1 {
    path: PathBuf,
    message: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct PublishFaultV1 {
    fail_publish_directory_sync: bool,
    fail_rollback_unlink: bool,
    fail_rollback_directory_sync: bool,
}

impl<
    T: NoeTradePayload<C>,
    C: Clone + std::fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
> DurableContextStore<T, C>
{
    pub fn open_or_create_sim_v2(
        root: impl AsRef<Path>,
        seed: SimV2ContextSeedV1,
    ) -> Result<Self, DurableErrorV1> {
        let root = root.as_ref();
        let context = sim_v2_context(seed, T::CONTRACT_VERSION)?;
        let context_reference = context.context_reference.clone();

        match Self::open(root, &context_reference) {
            Ok(store) => Ok(store),
            Err(DurableErrorV1::ContextNotFound { .. }) => Self::create(root, context),
            Err(error) => Err(error),
        }
    }

    pub fn create(
        root: impl AsRef<Path>,
        context: ExecutionNormalizationContextV1,
    ) -> Result<Self, DurableErrorV1> {
        let (context_directory, context_container_reference) =
            create_context_container::<T, C>(root.as_ref(), &context)?;

        let core = NormalizationCore::new(context.clone());
        Ok(Self {
            context_directory,
            context,
            context_container_reference,
            commits: Vec::new(),
            core,
            poison: None,
            #[cfg(test)]
            publish_fault: PublishFaultV1::default(),
        })
    }

    pub fn open(
        root: impl AsRef<Path>,
        context_reference: &ExecutionNormalizationContextReferenceV1,
    ) -> Result<Self, DurableErrorV1> {
        let context_directory = context_directory(root.as_ref(), context_reference);
        let context_path = context_directory.join(CONTEXT_FILE_NAME);
        if !context_path.exists() {
            return Err(DurableErrorV1::ContextNotFound { path: context_path });
        }
        let context_container: ContextContainerV1 = decode_container(&context_path)?;
        check_container_header(
            &context_path,
            &context_container.payload.container_version,
            T::CONTEXT_VERSION,
            &context_container.payload.encoding,
        )?;
        check_digest(
            &context_path,
            &context_container.payload,
            &context_container.boundary_sha256,
        )?;
        if context_container.payload.context.context_reference != *context_reference {
            return Err(DurableErrorV1::ContextReferenceMismatch {
                expected: context_reference.clone(),
                found: context_container.payload.context.context_reference,
            });
        }

        let commits_directory = context_directory.join(COMMITS_DIRECTORY_NAME);
        discard_unpublished_suffixes(&commits_directory)?;
        let mut paths = published_commit_paths(&commits_directory)?;
        paths.sort();
        let mut commits = Vec::with_capacity(paths.len());
        let mut expected_run_id: Option<String> = None;
        for (expected_commit_sequence, path) in paths.into_iter().enumerate() {
            let expected_commit_sequence = expected_commit_sequence as u64;
            let file_sequence = commit_sequence_from_path(&path)?;
            if file_sequence != expected_commit_sequence {
                return Err(DurableErrorV1::CommitSequenceMismatch {
                    expected: expected_commit_sequence,
                    found: file_sequence,
                });
            }
            let container: CommitContainer<T, C> = decode_container(&path)?;
            check_container_header(
                &path,
                &container.payload.container_version,
                T::COMMIT_VERSION,
                &container.payload.encoding,
            )?;
            check_digest(&path, &container.payload, &container.boundary_sha256)?;
            if container.payload.commit_sequence != expected_commit_sequence {
                return Err(DurableErrorV1::CommitSequenceMismatch {
                    expected: expected_commit_sequence,
                    found: container.payload.commit_sequence,
                });
            }
            if container.payload.boundary.context_reference != *context_reference {
                return Err(DurableErrorV1::ContextReferenceMismatch {
                    expected: context_reference.clone(),
                    found: container.payload.boundary.context_reference,
                });
            }
            let watermark = &container.payload.boundary.source_watermark;
            let expected_watermark_sequence = expected_commit_sequence + 1;
            if expected_run_id
                .as_ref()
                .is_some_and(|run_id| run_id != &watermark.run_id)
                || watermark.projection_batch_sequence != expected_watermark_sequence
            {
                return Err(DurableErrorV1::SourceWatermarkBoundary {
                    expected_run_id,
                    expected_sequence: expected_watermark_sequence,
                    found: watermark.clone(),
                });
            }
            expected_run_id.get_or_insert_with(|| watermark.run_id.clone());
            commits.push(DurableCommit {
                commit_sequence: container.payload.commit_sequence,
                boundary: container.payload.boundary,
                records: container.payload.records,
                container_reference: NoePublishedContainerReferenceV1 {
                    location: path.to_string_lossy().into_owned(),
                    boundary_sha256: container.boundary_sha256,
                },
            });
        }

        let context_container_reference = NoePublishedContainerReferenceV1 {
            location: context_path.to_string_lossy().into_owned(),
            boundary_sha256: context_container.boundary_sha256,
        };
        let context = context_container.payload.context;
        let core = NormalizationCore::restore(
            context.clone(),
            commits.iter().flat_map(|commit| commit.records.iter()),
        );
        Ok(Self {
            context_directory,
            context,
            context_container_reference,
            commits,
            core,
            poison: None,
            #[cfg(test)]
            publish_fault: PublishFaultV1::default(),
        })
    }

    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }

    pub fn context_container_reference(&self) -> &NoePublishedContainerReferenceV1 {
        &self.context_container_reference
    }

    pub fn retained_index_counts(&self) -> crate::core::NormalizationIndexCountsV1 {
        self.core.retained_index_counts()
    }

    pub fn commits(&self) -> &[DurableCommit<T, C>] {
        &self.commits
    }

    pub fn entity_identity(
        &self,
        key: &SourceNeutralEntityKeyV1,
    ) -> Result<contracts::execution_normalization::StableLogicalIdentityV1, DurableErrorV1> {
        self.core.entity_identity(key).map_err(Into::into)
    }

    pub fn durable_through(&self) -> Option<&SimV2SourceWatermarkV1> {
        self.commits
            .last()
            .map(|commit| &commit.boundary.source_watermark)
    }

    pub fn commit_batch(
        &mut self,
        input: NormalizationBatchInput<T, C>,
        durable_available_at_utc_ms: i64,
    ) -> Result<DurableCommitOutcomeV1, DurableErrorV1> {
        if let Some(poison) = &self.poison {
            return Err(DurableErrorV1::DurabilityUncertain {
                path: poison.path.clone(),
                message: format!(
                    "当前耐久句柄已经毒化，必须丢弃后重新打开：{}",
                    poison.message
                ),
            });
        }

        let previous = self.durable_through();
        if let Some(previous) = previous {
            if input.source_watermark.run_id != previous.run_id {
                return Err(DurableErrorV1::SourceWatermarkBoundary {
                    expected_run_id: Some(previous.run_id.clone()),
                    expected_sequence: previous.projection_batch_sequence + 1,
                    found: input.source_watermark,
                });
            }
            if input.source_watermark.projection_batch_sequence
                <= previous.projection_batch_sequence
            {
                return Ok(DurableCommitOutcomeV1::AlreadyDurable {
                    source_watermark: previous.clone(),
                });
            }
        }
        let expected_watermark_sequence =
            previous.map_or(1, |watermark| watermark.projection_batch_sequence + 1);
        if input.source_watermark.projection_batch_sequence != expected_watermark_sequence {
            return Err(DurableErrorV1::SourceWatermarkBoundary {
                expected_run_id: previous.map(|watermark| watermark.run_id.clone()),
                expected_sequence: expected_watermark_sequence,
                found: input.source_watermark,
            });
        }

        let prepared = self.core.prepare_batch(input)?;
        let (batch, state_delta) = prepared.into_parts();
        let commit_sequence = self.commits.len() as u64;
        let payload = CommitPayload {
            container_version: T::COMMIT_VERSION.to_owned(),
            encoding: NOE_CONTAINER_ENCODING_V1.to_owned(),
            commit_sequence,
            boundary: NoeSourceCommitBoundaryV1 {
                context_reference: self.context.context_reference.clone(),
                source_watermark: batch.source_watermark,
                durable_available_at_utc_ms,
            },
            records: batch.records,
        };
        let target_name = commit_file_name(commit_sequence);
        let target_path = self
            .context_directory
            .join(COMMITS_DIRECTORY_NAME)
            .join(&target_name);
        let boundary_sha256 = payload_digest(&payload, &target_path)?;
        let container = CommitContainer {
            payload,
            boundary_sha256: boundary_sha256.clone(),
        };
        let bytes = encode_container(&container, &target_path)?;
        let commits_directory = self.context_directory.join(COMMITS_DIRECTORY_NAME);
        #[cfg(test)]
        let publish_fault = std::mem::take(&mut self.publish_fault);
        #[cfg(not(test))]
        let publish_fault = PublishFaultV1::default();
        if let Err(error) = publish_no_replace(
            &commits_directory,
            &format!(".{target_name}.tmp"),
            &target_name,
            &bytes,
            Some(commit_sequence),
            publish_fault,
        ) {
            if let DurableErrorV1::DurabilityUncertain { path, message } = &error {
                self.poison = Some(PoisonedStoreV1 {
                    path: path.clone(),
                    message: message.clone(),
                });
            }
            return Err(error);
        }

        let container_reference = NoePublishedContainerReferenceV1 {
            location: target_path.to_string_lossy().into_owned(),
            boundary_sha256,
        };
        let commit = DurableCommit {
            commit_sequence,
            boundary: container.payload.boundary,
            records: container.payload.records,
            container_reference: container_reference.clone(),
        };
        self.commits.push(commit);
        self.core.accept_delta(state_delta);
        Ok(DurableCommitOutcomeV1::Committed {
            commit_sequence,
            container_reference,
        })
    }
}

fn create_context_container<
    T: NoeTradePayload<C>,
    C: Clone + std::fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    root: &Path,
    context: &ExecutionNormalizationContextV1,
) -> Result<(PathBuf, NoePublishedContainerReferenceV1), DurableErrorV1> {
    fs::create_dir_all(root).map_err(|error| io_error("创建耐久根", root, error))?;
    let context_directory = context_directory(root, &context.context_reference);
    match fs::create_dir(&context_directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(DurableErrorV1::ContextAlreadyExists {
                path: context_directory,
            });
        }
        Err(error) => {
            return Err(io_error("创建上下文目录", &context_directory, error));
        }
    }
    let commits_directory = context_directory.join(COMMITS_DIRECTORY_NAME);
    fs::create_dir(&commits_directory)
        .map_err(|error| io_error("创建提交目录", &commits_directory, error))?;

    let context_path = context_directory.join(CONTEXT_FILE_NAME);
    let payload = ContextPayloadV1 {
        container_version: T::CONTEXT_VERSION.to_owned(),
        encoding: NOE_CONTAINER_ENCODING_V1.to_owned(),
        context: context.clone(),
    };
    let container = ContextContainerV1 {
        boundary_sha256: payload_digest(&payload, &context_path)?,
        payload,
    };
    let context_container_reference = NoePublishedContainerReferenceV1 {
        location: context_path.to_string_lossy().into_owned(),
        boundary_sha256: container.boundary_sha256.clone(),
    };
    let bytes = encode_container(&container, &context_path)?;
    publish_no_replace(
        &context_directory,
        ".context.json.tmp",
        CONTEXT_FILE_NAME,
        &bytes,
        None,
        PublishFaultV1::default(),
    )?;
    sync_directory(root, "同步耐久根")?;

    Ok((context_directory, context_container_reference))
}

/// 新内存运行的唯一后台写入器：不持有核心，不重做归一化。
/// 原始批与最终容器的候选文件独立，发布失败不能删除唯一原始材料。
pub struct StagedNormalizationWriter<T, C> {
    context_directory: PathBuf,
    context: ExecutionNormalizationContextV1,
    staged: u64,
    published: u64,
    payload: std::marker::PhantomData<(T, C)>,
}

impl<
    T: NoeTradePayload<C>,
    C: Clone + std::fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
> StagedNormalizationWriter<T, C>
{
    pub fn create(
        root: &Path,
        context: ExecutionNormalizationContextV1,
    ) -> Result<Self, DurableErrorV1> {
        let (context_directory, _) = create_context_container::<T, C>(root, &context)?;
        Ok(Self {
            context_directory,
            context,
            staged: 0,
            published: 0,
            payload: std::marker::PhantomData,
        })
    }

    pub fn stage<MT: Serialize, MC: Serialize, I: Serialize, F: Serialize>(
        &mut self,
        batch: &crate::core::NormalizationBatch<MT, MC, I, F>,
    ) -> Result<(), DurableErrorV1> {
        let path = self.stage_path(self.staged);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| io_error("创建归一化批暂存", &path, error))?;
        let mut file = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut file, batch).map_err(|error| {
            if error.is_io() {
                DurableErrorV1::Io {
                    operation: "写入归一化批暂存",
                    path: path.clone(),
                    message: error.to_string(),
                }
            } else {
                DurableErrorV1::ContainerDecode {
                    path: path.clone(),
                    message: error.to_string(),
                }
            }
        })?;
        file.flush()
            .map_err(|error| io_error("写入归一化批暂存", &path, error))?;
        self.staged += 1;
        Ok(())
    }

    fn stage_path(&self, sequence: u64) -> PathBuf {
        self.context_directory
            .join(COMMITS_DIRECTORY_NAME)
            .join(format!(".{sequence:020}.batch.json.tmp"))
    }

    /// 调用方必须先稳定关联的 SIM 输入；时间绑定本次稳定尝试。
    /// 每次只读取一个已归一化批，成功发布后才交付耐久引用并移除原暂存。
    pub fn stabilize(
        &mut self,
        mut now: impl FnMut() -> i64,
        mut published: impl FnMut(&DurableCommit<T, C>),
    ) -> Result<(), DurableErrorV1> {
        while self.published < self.staged {
            let sequence = self.published;
            let stage_path = self.stage_path(sequence);
            let batch: crate::core::NormalizationBatch<T, C> = decode_container(&stage_path)?;
            let payload = CommitPayload {
                container_version: T::COMMIT_VERSION.to_owned(),
                encoding: NOE_CONTAINER_ENCODING_V1.to_owned(),
                commit_sequence: sequence,
                boundary: NoeSourceCommitBoundaryV1 {
                    context_reference: self.context.context_reference.clone(),
                    source_watermark: batch.source_watermark,
                    durable_available_at_utc_ms: now(),
                },
                records: batch.records,
            };
            let target_name = commit_file_name(sequence);
            let directory = self.context_directory.join(COMMITS_DIRECTORY_NAME);
            let target_path = directory.join(&target_name);
            let boundary_sha256 = payload_digest(&payload, &target_path)?;
            let container = CommitContainer {
                payload,
                boundary_sha256: boundary_sha256.clone(),
            };
            let bytes = encode_container(&container, &target_path)?;
            publish_no_replace(
                &directory,
                &format!(".{target_name}.tmp"),
                &target_name,
                &bytes,
                Some(sequence),
                PublishFaultV1::default(),
            )?;
            let commit = DurableCommit {
                commit_sequence: sequence,
                boundary: container.payload.boundary,
                records: container.payload.records,
                container_reference: NoePublishedContainerReferenceV1 {
                    location: target_path.to_string_lossy().into_owned(),
                    boundary_sha256,
                },
            };
            published(&commit);
            self.published += 1;
            fs::remove_file(&stage_path)
                .map_err(|error| io_error("移除已发布归一化批暂存", &stage_path, error))?;
        }
        Ok(())
    }
}

fn read_noe_result<
    T: NoeTradePayload<C>,
    C: Clone + std::fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    reference: &NoeResultReferenceV1,
) -> Result<ResolvedNoeResult<T, C>, DurableErrorV1> {
    let commit_path = PathBuf::from(&reference.commit_container.location);
    let commits_directory = commit_path.parent().filter(|path| {
        path.file_name()
            .is_some_and(|name| name == COMMITS_DIRECTORY_NAME)
    });
    let Some(commits_directory) = commits_directory else {
        return Err(DurableErrorV1::ContainerDecode {
            path: commit_path,
            message: "提交容器不在固定 commits 目录".to_owned(),
        });
    };
    let context_directory = commits_directory
        .parent()
        .expect("固定 commits 目录必有上下文父目录");
    let file_sequence = commit_sequence_from_path(&commit_path)?;
    let container: CommitContainer<T, C> = decode_container(&commit_path)?;
    check_container_header(
        &commit_path,
        &container.payload.container_version,
        T::COMMIT_VERSION,
        &container.payload.encoding,
    )?;
    check_digest(&commit_path, &container.payload, &container.boundary_sha256)?;
    if container.boundary_sha256 != reference.commit_container.boundary_sha256 {
        return Err(DurableErrorV1::BoundaryDigestMismatch {
            path: commit_path,
            expected: reference.commit_container.boundary_sha256.clone(),
            found: container.boundary_sha256,
        });
    }
    if container.payload.commit_sequence != file_sequence {
        return Err(DurableErrorV1::CommitSequenceMismatch {
            expected: file_sequence,
            found: container.payload.commit_sequence,
        });
    }

    let record_count = container.payload.records.len() as u64;
    let record = container
        .payload
        .records
        .get(reference.record_ordinal as usize)
        .cloned()
        .ok_or_else(|| DurableErrorV1::RecordOrdinalOutOfRange {
            path: commit_path.clone(),
            record_ordinal: reference.record_ordinal,
            record_count,
        })?;

    let context_path = context_directory.join(CONTEXT_FILE_NAME);
    let context_container: ContextContainerV1 = decode_container(&context_path)?;
    check_container_header(
        &context_path,
        &context_container.payload.container_version,
        T::CONTEXT_VERSION,
        &context_container.payload.encoding,
    )?;
    check_digest(
        &context_path,
        &context_container.payload,
        &context_container.boundary_sha256,
    )?;
    if context_container.payload.context.context_reference
        != container.payload.boundary.context_reference
    {
        return Err(DurableErrorV1::ContextReferenceMismatch {
            expected: container.payload.boundary.context_reference,
            found: context_container.payload.context.context_reference,
        });
    }
    let derived_context_reference = derive_sim_v2_context_reference(
        &context_container.payload.context.contract_version,
        &context_container.payload.context.account_identity,
        &context_container.payload.context.source,
        &context_container.payload.context.implementation_identity,
    )?;
    if context_container.payload.context.context_reference != derived_context_reference {
        return Err(DurableErrorV1::ContextReferenceMismatch {
            expected: derived_context_reference,
            found: context_container.payload.context.context_reference,
        });
    }

    Ok(ResolvedNoeResult {
        context: context_container.payload.context,
        boundary: container.payload.boundary,
        record,
    })
}

fn derive_sim_v2_context_reference(
    contract_version: &str,
    account_identity: &SourceNeutralAccountIdentityV1,
    source: &SimV2ContextSourceV1,
    implementation_identity: &str,
) -> Result<ExecutionNormalizationContextReferenceV1, DurableErrorV1> {
    let encoded = serde_json::to_vec(&(
        contract_version,
        account_identity,
        source,
        implementation_identity,
    ))
    .map_err(|error| {
        DurableErrorV1::Core(CoreErrorV1::IdentityEncoding {
            message: error.to_string(),
        })
    })?;
    Ok(ExecutionNormalizationContextReferenceV1(format!(
        "noe-context-v1:{}",
        sha256_hex(&encoded)
    )))
}

fn context_directory(
    root: &Path,
    context_reference: &ExecutionNormalizationContextReferenceV1,
) -> PathBuf {
    root.join(format!(
        "context-{}",
        sha256_hex(context_reference.0.as_bytes())
    ))
}

fn commit_file_name(commit_sequence: u64) -> String {
    format!("{commit_sequence:020}.json")
}

fn commit_sequence_from_path(path: &Path) -> Result<u64, DurableErrorV1> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let number = name.strip_suffix(".json").unwrap_or_default();
    if number.len() != 20 {
        return Err(DurableErrorV1::ContainerDecode {
            path: path.to_path_buf(),
            message: "提交文件名不包含固定宽度序列".to_owned(),
        });
    }
    number.parse().map_err(
        |error: std::num::ParseIntError| DurableErrorV1::ContainerDecode {
            path: path.to_path_buf(),
            message: error.to_string(),
        },
    )
}

fn published_commit_paths(directory: &Path) -> Result<Vec<PathBuf>, DurableErrorV1> {
    let entries =
        fs::read_dir(directory).map_err(|error| io_error("读取提交目录", directory, error))?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| io_error("读取提交目录项", directory, error))?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn discard_unpublished_suffixes(directory: &Path) -> Result<(), DurableErrorV1> {
    let entries =
        fs::read_dir(directory).map_err(|error| io_error("读取提交目录", directory, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| io_error("读取提交目录项", directory, error))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') && name.ends_with(".json.tmp") {
            fs::remove_file(entry.path())
                .map_err(|error| io_error("清理未发布提交后缀", &entry.path(), error))?;
        }
    }
    sync_directory(directory, "同步提交后缀恢复")
}

fn publish_no_replace(
    parent: &Path,
    candidate_name: &str,
    target_name: &str,
    bytes: &[u8],
    commit_sequence: Option<u64>,
    fault: PublishFaultV1,
) -> Result<(), DurableErrorV1> {
    let candidate_path = parent.join(candidate_name);
    let target_path = parent.join(target_name);
    let mut candidate = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&candidate_path)
        .map_err(|error| io_error("创建提交候选", &candidate_path, error))?;
    if let Err(error) = candidate
        .write_all(bytes)
        .and_then(|()| candidate.sync_all())
    {
        drop(candidate);
        let _ = fs::remove_file(&candidate_path);
        return Err(io_error("写入并同步提交候选", &candidate_path, error));
    }
    drop(candidate);

    let parent_file =
        File::open(parent).map_err(|error| io_error("打开提交父目录", parent, error))?;
    if let Err(error) = renameat_with(
        &parent_file,
        candidate_name,
        &parent_file,
        target_name,
        RenameFlags::NOREPLACE,
    ) {
        let _ = fs::remove_file(&candidate_path);
        if error == rustix::io::Errno::EXIST
            && let Some(commit_sequence) = commit_sequence
        {
            return Err(DurableErrorV1::CommitAlreadyExists { commit_sequence });
        }
        return Err(DurableErrorV1::Io {
            operation: "禁止覆盖地发布提交",
            path: target_path,
            message: error.to_string(),
        });
    }

    let publish_sync = if fault.fail_publish_directory_sync {
        Err(injected_io_error("发布后目录同步失败"))
    } else {
        parent_file.sync_all()
    };
    if let Err(error) = publish_sync {
        let rollback_unlink = if fault.fail_rollback_unlink {
            Err(injected_io_error("回滚删除最终名失败"))
        } else {
            fs::remove_file(&target_path)
        };
        if let Err(rollback_error) = rollback_unlink {
            return Err(DurableErrorV1::DurabilityUncertain {
                path: target_path,
                message: format!("{error}; 回滚删除失败：{rollback_error}"),
            });
        }
        let rollback_sync = if fault.fail_rollback_directory_sync {
            Err(injected_io_error("回滚后目录同步失败"))
        } else {
            parent_file.sync_all()
        };
        return match rollback_sync {
            Ok(()) => Err(io_error("同步提交父目录", &target_path, error)),
            Err(rollback_error) => Err(DurableErrorV1::DurabilityUncertain {
                path: target_path,
                message: format!("{error}; 回滚后同步失败：{rollback_error}"),
            }),
        };
    }
    Ok(())
}

pub(crate) fn publish_artifact_no_replace(path: &Path, bytes: &[u8]) -> Result<(), DurableErrorV1> {
    let directory = path.parent().ok_or_else(|| DurableErrorV1::Io {
        operation: "解析产物父目录",
        path: path.to_path_buf(),
        message: "路径没有父目录".to_owned(),
    })?;
    fs::create_dir_all(directory).map_err(|error| io_error("创建产物目录", directory, error))?;
    let target_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DurableErrorV1::Io {
            operation: "解析产物文件名",
            path: path.to_path_buf(),
            message: "文件名不是 UTF-8".to_owned(),
        })?;
    publish_no_replace(
        directory,
        &format!(".{target_name}.tmp"),
        target_name,
        bytes,
        None,
        PublishFaultV1::default(),
    )
}

fn injected_io_error(message: &'static str) -> std::io::Error {
    std::io::Error::other(message)
}

fn sync_directory(path: &Path, operation: &'static str) -> Result<(), DurableErrorV1> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error(operation, path, error))
}

fn encode_container<T: Serialize>(
    value: &T,
    path: impl AsRef<Path>,
) -> Result<Vec<u8>, DurableErrorV1> {
    serde_json::to_vec(value).map_err(|error| DurableErrorV1::ContainerDecode {
        path: path.as_ref().to_path_buf(),
        message: error.to_string(),
    })
}

fn decode_container<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, DurableErrorV1> {
    let bytes = fs::read(path).map_err(|error| io_error("读取 NOE 容器", path, error))?;
    serde_json::from_slice(&bytes).map_err(|error| DurableErrorV1::ContainerDecode {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

fn payload_digest<T: Serialize>(
    payload: &T,
    path: impl AsRef<Path>,
) -> Result<Sha256Hex, DurableErrorV1> {
    encode_container(payload, path).map(|bytes| Sha256Hex(sha256_hex(&bytes)))
}

fn check_digest<T: Serialize>(
    path: &Path,
    payload: &T,
    found: &Sha256Hex,
) -> Result<(), DurableErrorV1> {
    let expected = payload_digest(payload, path)?;
    if expected != *found {
        return Err(DurableErrorV1::BoundaryDigestMismatch {
            path: path.to_path_buf(),
            expected,
            found: found.clone(),
        });
    }
    Ok(())
}

fn check_container_header(
    path: &Path,
    found_version: &str,
    expected_version: &str,
    found_encoding: &str,
) -> Result<(), DurableErrorV1> {
    if found_version != expected_version {
        return Err(DurableErrorV1::UnsupportedContainerVersion {
            path: path.to_path_buf(),
            found: found_version.to_owned(),
        });
    }
    if found_encoding != NOE_CONTAINER_ENCODING_V1 {
        return Err(DurableErrorV1::UnsupportedEncoding {
            path: path.to_path_buf(),
            found: found_encoding.to_owned(),
        });
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
    }
    encoded
}

fn io_error(operation: &'static str, path: &Path, error: std::io::Error) -> DurableErrorV1 {
    DurableErrorV1::Io {
        operation,
        path: path.to_path_buf(),
        message: error.to_string(),
    }
}

/// 载荷版本只选择正常记录编码；各版本共用同一核心和耐久算法。
pub trait NoeTradePayload<C = SourceNeutralControlResultV1>:
    Clone + std::fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned
{
    const COMMIT_VERSION: &'static str;
    const CONTEXT_VERSION: &'static str;
    const CONTRACT_VERSION: &'static str;
}
impl NoeTradePayload for contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1 {
    const COMMIT_VERSION: &'static str = NOE_COMMIT_CONTAINER_V1;
    const CONTEXT_VERSION: &'static str = NOE_CONTEXT_CONTAINER_V1;
    const CONTRACT_VERSION: &'static str = EXECUTION_NORMALIZATION_CONTRACT_V1;
}
impl NoeTradePayload for contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2 {
    const COMMIT_VERSION: &'static str = "noe_commit_container_v2";
    const CONTEXT_VERSION: &'static str = "noe_context_container_v2";
    const CONTRACT_VERSION: &'static str = "execution_normalization_v2";
}
impl NoeTradePayload<SourceNeutralControlResultV2>
    for contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2
{
    const COMMIT_VERSION: &'static str = "noe_commit_container_v3";
    const CONTEXT_VERSION: &'static str = "noe_context_container_v3";
    const CONTRACT_VERSION: &'static str = "execution_normalization_v3";
}
pub type DurableCommitV3 = DurableCommit<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    SourceNeutralControlResultV2,
>;
pub type DurableContextStoreV3 = DurableContextStore<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    SourceNeutralControlResultV2,
>;
pub type ResolvedNoeResultV3 = ResolvedNoeResult<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    SourceNeutralControlResultV2,
>;
impl DurableContextStoreV3 {
    pub fn commit_batch_v3(
        &mut self,
        input: crate::NormalizationBatchInputV3,
        durable_available_at_utc_ms: i64,
    ) -> Result<DurableCommitOutcomeV1, DurableErrorV1> {
        self.commit_batch(input, durable_available_at_utc_ms)
    }
}
pub fn read_noe_result_v3(
    reference: &NoeResultReferenceV1,
) -> Result<ResolvedNoeResultV3, DurableErrorV1> {
    read_noe_result(reference)
}
impl NoeTradePayload<contracts::execution_normalization::SourceNeutralControlResultV3>
    for contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2
{
    const COMMIT_VERSION: &'static str = "noe_commit_container_v4";
    const CONTEXT_VERSION: &'static str = "noe_context_container_v4";
    const CONTRACT_VERSION: &'static str = "execution_normalization_v4";
}
pub type DurableCommitV4 = DurableCommit<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV3,
>;
pub type DurableContextStoreV4 = DurableContextStore<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV3,
>;
pub type ResolvedNoeResultV4 = ResolvedNoeResult<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV3,
>;
impl DurableContextStoreV4 {
    pub fn commit_batch_v4(
        &mut self,
        input: crate::NormalizationBatchInputV4,
        durable_available_at_utc_ms: i64,
    ) -> Result<DurableCommitOutcomeV1, DurableErrorV1> {
        self.commit_batch(input, durable_available_at_utc_ms)
    }
}
pub fn read_noe_result_v4(
    reference: &NoeResultReferenceV1,
) -> Result<ResolvedNoeResultV4, DurableErrorV1> {
    read_noe_result(reference)
}

pub type DurableCommitV1 =
    DurableCommit<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1>;
pub type DurableCommitV2 =
    DurableCommit<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;
pub type DurableContextStoreV1 =
    DurableContextStore<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1>;
pub type DurableContextStoreV2 =
    DurableContextStore<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;
pub type ResolvedNoeResultV1 =
    ResolvedNoeResult<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1>;
pub type ResolvedNoeResultV2 =
    ResolvedNoeResult<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;

impl DurableContextStoreV2 {
    pub fn commit_batch_v2(
        &mut self,
        input: crate::NormalizationBatchInputV2,
        durable_available_at_utc_ms: i64,
    ) -> Result<DurableCommitOutcomeV1, DurableErrorV1> {
        self.commit_batch(input, durable_available_at_utc_ms)
    }
}
pub fn read_noe_result_v1(
    reference: &NoeResultReferenceV1,
) -> Result<ResolvedNoeResultV1, DurableErrorV1> {
    read_noe_result(reference)
}
pub fn read_noe_result_v2(
    reference: &NoeResultReferenceV1,
) -> Result<ResolvedNoeResultV2, DurableErrorV1> {
    read_noe_result(reference)
}

#[cfg(test)]
mod tests {
    use crate::NormalizationBatchInputV1;
    use contracts::execution_normalization::{
        DurableSourceReferenceV1, ImmutableSourceReferenceV1, SimV2ContextSourceV1,
        SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1,
        SourceNeutralAccountIdentityV1, SourceNeutralControlResultV1,
        SourceNeutralSubmissionOutcomeV1,
    };
    use contracts::{AccountInstanceKeyV1, PlatformSourceV1, SimCallerRoleV1};
    use tempfile::TempDir;

    use super::*;
    use crate::core::{
        NormalizationRecordV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
        SourceNeutralLogicalKeyV1,
    };

    fn context() -> ExecutionNormalizationContextV1 {
        ExecutionNormalizationContextV1 {
            context_reference: ExecutionNormalizationContextReferenceV1(
                "context-55-sync-fault".to_owned(),
            ),
            account_identity: SourceNeutralAccountIdentityV1 {
                source_category: "platform".to_owned(),
                environment: "simulation".to_owned(),
                run_scope: "run-55-sync-fault".to_owned(),
                account_scope: "account-55-sync-fault".to_owned(),
            },
            source: SimV2ContextSourceV1 {
                account_instance_key: AccountInstanceKeyV1 {
                    run_id: "run-55-sync-fault".to_owned(),
                    caller_role: SimCallerRoleV1::ModelTrain,
                    caller_instance_id: "caller-55-sync-fault".to_owned(),
                    account_id: "account-55-sync-fault".to_owned(),
                    platform_source: PlatformSourceV1::Simulated,
                },
                run_opened: DurableSourceReferenceV1 {
                    source: ImmutableSourceReferenceV1::Location(
                        "artifact://sim/run-55-sync-fault/opened".to_owned(),
                    ),
                },
                account_opening_evidence_identity: "opening-55-sync-fault".to_owned(),
                account_opening: DurableSourceReferenceV1 {
                    source: ImmutableSourceReferenceV1::Location(
                        "artifact://sim/run-55-sync-fault/account-opening".to_owned(),
                    ),
                },
            },
            contract_version: "execution_normalization_v1".to_owned(),
            implementation_identity: "noe-stage-1-sync-fault-test".to_owned(),
        }
    }

    fn batch(sequence: u64) -> NormalizationBatchInputV1 {
        let source_watermark = SimV2SourceWatermarkV1 {
            run_id: "run-55-sync-fault".to_owned(),
            projection_batch_sequence: sequence,
        };
        NormalizationBatchInputV1 {
            source_watermark: source_watermark.clone(),
            evidence: vec![SourceNeutralEvidenceV1 {
                source_reference: SimV2SourceReferenceV1 {
                    context_reference: ExecutionNormalizationContextReferenceV1(
                        "context-55-sync-fault".to_owned(),
                    ),
                    watermark: source_watermark,
                    record: SimV2SourceRecordReferenceV1::DirectResponse {
                        private_feedback_sequence: 100 + sequence,
                        platform_responded_at_utc_ms: 2_000 + sequence as i64,
                        response_received_at_utc_ms: 2_100 + sequence as i64,
                        association: SimV2DirectResponseAssociationV1::SubmissionItem {
                            global_request_id: "request-55-sync-fault".to_owned(),
                            batch_item_ordinal: 0,
                        },
                    },
                },
                source_ordinal: 0,
                logical_key: SourceNeutralLogicalKeyV1::SubmissionItem {
                    global_request_id: "request-55-sync-fault".to_owned(),
                    batch_item_ordinal: 0,
                },
                kind: SourceNeutralEvidenceKindV1::Control {
                    result: SourceNeutralControlResultV1::Submission {
                        outcome: SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
                    },
                },
            }],
        }
    }

    fn final_commit_path(root: &Path) -> PathBuf {
        context_directory(
            root,
            &ExecutionNormalizationContextReferenceV1("context-55-sync-fault".to_owned()),
        )
        .join(COMMITS_DIRECTORY_NAME)
        .join(commit_file_name(0))
    }

    #[test]
    fn directory_sync_failure_with_confirmed_rollback_is_retryable() {
        let temporary = TempDir::new().unwrap();
        let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
        store.publish_fault.fail_publish_directory_sync = true;

        let error = store.commit_batch(batch(1), 3_000).unwrap_err();
        assert!(matches!(error, DurableErrorV1::Io { .. }));
        assert!(store.commits().is_empty());
        assert!(store.durable_through().is_none());
        assert!(!final_commit_path(temporary.path()).exists());

        store.commit_batch(batch(1), 3_100).unwrap();
        assert!(matches!(
            store.commits()[0].records[0],
            NormalizationRecordV1::Evidence { .. }
        ));
    }

    #[test]
    fn failed_rollback_unlink_poisons_handle_and_reopen_adopts_surviving_commit() {
        let temporary = TempDir::new().unwrap();
        let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
        store.publish_fault = PublishFaultV1 {
            fail_publish_directory_sync: true,
            fail_rollback_unlink: true,
            fail_rollback_directory_sync: false,
        };

        let error = store.commit_batch(batch(1), 3_000).unwrap_err();
        assert!(matches!(error, DurableErrorV1::DurabilityUncertain { .. }));
        assert!(store.commits().is_empty());
        assert!(store.durable_through().is_none());
        assert!(final_commit_path(temporary.path()).exists());
        assert!(matches!(
            store.commit_batch(batch(1), 3_100),
            Err(DurableErrorV1::DurabilityUncertain { .. })
        ));
        drop(store);

        let mut reopened = DurableContextStoreV1::open(
            temporary.path(),
            &ExecutionNormalizationContextReferenceV1("context-55-sync-fault".to_owned()),
        )
        .unwrap();
        assert_eq!(
            reopened
                .durable_through()
                .unwrap()
                .projection_batch_sequence,
            1
        );
        reopened.commit_batch(batch(2), 3_200).unwrap();
        assert!(matches!(
            reopened.commits()[1].records[0],
            NormalizationRecordV1::Duplicate { .. }
        ));
    }

    #[test]
    fn failed_rollback_sync_poisons_handle_and_reopen_observes_absent_commit() {
        let temporary = TempDir::new().unwrap();
        let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
        store.publish_fault = PublishFaultV1 {
            fail_publish_directory_sync: true,
            fail_rollback_unlink: false,
            fail_rollback_directory_sync: true,
        };

        let error = store.commit_batch(batch(1), 3_000).unwrap_err();
        assert!(matches!(error, DurableErrorV1::DurabilityUncertain { .. }));
        assert!(store.commits().is_empty());
        assert!(store.durable_through().is_none());
        assert!(!final_commit_path(temporary.path()).exists());
        assert!(matches!(
            store.commit_batch(batch(1), 3_100),
            Err(DurableErrorV1::DurabilityUncertain { .. })
        ));
        drop(store);

        let mut reopened = DurableContextStoreV1::open(
            temporary.path(),
            &ExecutionNormalizationContextReferenceV1("context-55-sync-fault".to_owned()),
        )
        .unwrap();
        assert!(reopened.commits().is_empty());
        assert!(reopened.durable_through().is_none());
        reopened.commit_batch(batch(1), 3_200).unwrap();
        assert!(matches!(
            reopened.commits()[0].records[0],
            NormalizationRecordV1::Evidence { .. }
        ));
    }
    fn memory_batch(
        sequence: u64,
    ) -> crate::core::NormalizationBatchInput<
        contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1,
        SourceNeutralControlResultV1,
        crate::identity::MemoryLogicalIdentityV1,
    > {
        let input = batch(sequence);
        crate::core::NormalizationBatchInput {
            source_watermark: input.source_watermark,
            evidence: input
                .evidence
                .into_iter()
                .map(|evidence| crate::core::SourceNeutralEvidence {
                    source_reference: evidence.source_reference,
                    source_ordinal: evidence.source_ordinal,
                    logical_key: evidence.logical_key,
                    kind: crate::core::SourceNeutralEvidenceKind::Control {
                        result: SourceNeutralControlResultV1::Submission {
                            outcome: SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
                        },
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn memory_normalizes_once_without_hot_identity_encoding_and_publishes_legacy_bytes() {
        use crate::identity::{MemoryEconomicFactReferenceV1, MemoryLogicalIdentityV1};
        type Trade = contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1;
        let old = TempDir::new().unwrap();
        let new = TempDir::new().unwrap();
        let mut store = DurableContextStoreV1::create(old.path(), context()).unwrap();
        for sequence in 1..=2 {
            store.commit_batch(batch(sequence), 3_000).unwrap();
        }
        let mut core = crate::core::NormalizationCore::<
            Trade,
            SourceNeutralControlResultV1,
            MemoryLogicalIdentityV1,
            MemoryEconomicFactReferenceV1,
        >::new(context());
        use crate::identity::NormalizationIdentity;
        let account = std::sync::Arc::clone(core.account_identity());
        let entity_key = SourceNeutralEntityKeyV1::Order {
            source_order_id: "actual-order-id".into(),
        };
        let economic = contracts::execution_normalization::SourceNeutralEconomicIdentityV1::AccountTransition { account_identity: (*account).clone(), account_transition_seq: 1 };
        let expected_entity = contracts::execution_normalization::StableLogicalIdentityV1::entity(
            &account,
            &entity_key,
        )
        .unwrap();
        let expected_fact =
            contracts::execution_normalization::StableLogicalIdentityV1::fact_reference(&economic)
                .unwrap();
        crate::identity::ENCODINGS.with(|count| count.set(0));
        crate::core::NORMALIZATIONS.with(|count| count.set(0));
        let memory_entity = MemoryLogicalIdentityV1::entity(&account, &entity_key).unwrap();
        let memory_fact = MemoryLogicalIdentityV1::fact_reference(&economic).unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &memory_entity.account,
            core.account_identity()
        ));
        let first = core.normalize_and_accept(memory_batch(1)).unwrap();
        let second = core.normalize_and_accept(memory_batch(2)).unwrap();
        assert_eq!(crate::core::NORMALIZATIONS.with(|count| count.get()), 2);
        assert_eq!(crate::identity::ENCODINGS.with(|count| count.get()), 0);
        assert!(matches!(
            second.records[0],
            NormalizationRecord::Duplicate { .. }
        ));
        let new_path = new.path().to_owned();
        std::thread::spawn(move || {
            assert_eq!(
                serde_json::to_vec(&memory_entity).unwrap(),
                serde_json::to_vec(&expected_entity).unwrap()
            );
            assert_eq!(
                serde_json::to_vec(&memory_fact).unwrap(),
                serde_json::to_vec(&expected_fact).unwrap()
            );
            let mut writer =
                StagedNormalizationWriter::<Trade, SourceNeutralControlResultV1>::create(
                    &new_path,
                    context(),
                )
                .unwrap();
            writer.stage(&first).unwrap();
            writer.stage(&second).unwrap();
            assert!(crate::identity::ENCODINGS.with(|count| count.get()) > 0);
            assert_eq!(
                crate::core::NORMALIZATIONS.with(|count| count.get()),
                0,
                "后台不重复归一化"
            );
            writer.stabilize(|| 3_000, |_| {}).unwrap();
        })
        .join()
        .unwrap();
        for sequence in 0..2 {
            let relative = format!("commits/{sequence:020}.json");
            let old_file =
                context_directory(old.path(), &context().context_reference).join(&relative);
            let new_file =
                context_directory(new.path(), &context().context_reference).join(&relative);
            assert_eq!(
                fs::read(old_file).unwrap(),
                fs::read(new_file).unwrap(),
                "固定上下文与稳定尝试时间保留旧字节/identity"
            );
        }
    }

    #[test]
    fn memory_source_order_ties_keep_legacy_string_order() {
        use crate::identity::{MemoryEconomicFactReferenceV1, MemoryLogicalIdentityV1};
        type Trade = contracts::execution_normalization::SourceNeutralTradeLifecycleResultV1;
        let mut old = batch(1);
        let mut memory = memory_batch(1);
        // 同一合法 SourceOrder 的数字字符串序不同于枚举/数字派生序。
        for ordinal in [2, 10] {
            let key = SourceNeutralLogicalKeyV1::SubmissionItem {
                global_request_id: "request-55-sync-fault".into(),
                batch_item_ordinal: ordinal,
            };
            let mut e = old.evidence[0].clone();
            e.logical_key = key.clone();
            old.evidence.push(e);
            let mut e = memory.evidence[0].clone();
            e.logical_key = key;
            memory.evidence.push(e);
        }
        let mut old_core = crate::core::NormalizationCore::<Trade>::new(context());
        let mut memory_core = crate::core::NormalizationCore::<
            Trade,
            SourceNeutralControlResultV1,
            MemoryLogicalIdentityV1,
            MemoryEconomicFactReferenceV1,
        >::new(context());
        assert_eq!(
            serde_json::to_vec(&old_core.normalize_and_accept(old).unwrap()).unwrap(),
            serde_json::to_vec(&memory_core.normalize_and_accept(memory).unwrap()).unwrap()
        );
    }
}
