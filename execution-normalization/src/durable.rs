use std::error::Error;
use std::fmt;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use contracts::Sha256Hex;
use contracts::execution_normalization::{
    ExecutionNormalizationContextReferenceV1, ExecutionNormalizationContextV1,
    NoeSourceCommitBoundaryV1, SimV2SourceWatermarkV1,
};
use rustix::fs::{RenameFlags, renameat_with};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::core::{
    CoreErrorV1, NormalizationBatchInputV1, NormalizationCoreV1, NormalizationRecordV1,
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
struct CommitPayloadV1 {
    container_version: String,
    encoding: String,
    commit_sequence: u64,
    boundary: NoeSourceCommitBoundaryV1,
    records: Vec<NormalizationRecordV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CommitContainerV1 {
    payload: CommitPayloadV1,
    boundary_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableCommitV1 {
    pub commit_sequence: u64,
    pub boundary: NoeSourceCommitBoundaryV1,
    pub records: Vec<NormalizationRecordV1>,
    pub boundary_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableCommitOutcomeV1 {
    Committed {
        commit_sequence: u64,
        boundary_sha256: Sha256Hex,
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
pub struct DurableContextStoreV1 {
    context_directory: PathBuf,
    context: ExecutionNormalizationContextV1,
    commits: Vec<DurableCommitV1>,
}

impl DurableContextStoreV1 {
    pub fn create(
        root: impl AsRef<Path>,
        context: ExecutionNormalizationContextV1,
    ) -> Result<Self, DurableErrorV1> {
        let root = root.as_ref();
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

        let payload = ContextPayloadV1 {
            container_version: NOE_CONTEXT_CONTAINER_V1.to_owned(),
            encoding: NOE_CONTAINER_ENCODING_V1.to_owned(),
            context: context.clone(),
        };
        let container = ContextContainerV1 {
            boundary_sha256: payload_digest(&payload, context_directory.join(CONTEXT_FILE_NAME))?,
            payload,
        };
        let bytes = encode_container(&container, context_directory.join(CONTEXT_FILE_NAME))?;
        publish_no_replace(
            &context_directory,
            ".context.json.tmp",
            CONTEXT_FILE_NAME,
            &bytes,
            None,
        )?;
        sync_directory(root, "同步耐久根")?;

        Ok(Self {
            context_directory,
            context,
            commits: Vec::new(),
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
            NOE_CONTEXT_CONTAINER_V1,
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
            let container: CommitContainerV1 = decode_container(&path)?;
            check_container_header(
                &path,
                &container.payload.container_version,
                NOE_COMMIT_CONTAINER_V1,
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
            commits.push(DurableCommitV1 {
                commit_sequence: container.payload.commit_sequence,
                boundary: container.payload.boundary,
                records: container.payload.records,
                boundary_sha256: container.boundary_sha256,
            });
        }

        Ok(Self {
            context_directory,
            context: context_container.payload.context,
            commits,
        })
    }

    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }

    pub fn commits(&self) -> &[DurableCommitV1] {
        &self.commits
    }

    pub fn durable_through(&self) -> Option<&SimV2SourceWatermarkV1> {
        self.commits
            .last()
            .map(|commit| &commit.boundary.source_watermark)
    }

    pub fn commit_batch(
        &mut self,
        core: &mut NormalizationCoreV1,
        input: NormalizationBatchInputV1,
        durable_available_at_utc_ms: i64,
    ) -> Result<DurableCommitOutcomeV1, DurableErrorV1> {
        if core.context() != &self.context {
            return Err(DurableErrorV1::ContextReferenceMismatch {
                expected: self.context.context_reference.clone(),
                found: core.context().context_reference.clone(),
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

        let prepared = core.prepare_batch(input)?;
        let (batch, state_delta) = prepared.into_parts();
        let commit_sequence = self.commits.len() as u64;
        let payload = CommitPayloadV1 {
            container_version: NOE_COMMIT_CONTAINER_V1.to_owned(),
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
        let container = CommitContainerV1 {
            payload,
            boundary_sha256: boundary_sha256.clone(),
        };
        let bytes = encode_container(&container, &target_path)?;
        let commits_directory = self.context_directory.join(COMMITS_DIRECTORY_NAME);
        publish_no_replace(
            &commits_directory,
            &format!(".{target_name}.tmp"),
            &target_name,
            &bytes,
            Some(commit_sequence),
        )?;

        let commit = DurableCommitV1 {
            commit_sequence,
            boundary: container.payload.boundary,
            records: container.payload.records,
            boundary_sha256: boundary_sha256.clone(),
        };
        self.commits.push(commit);
        core.accept_delta(state_delta);
        Ok(DurableCommitOutcomeV1::Committed {
            commit_sequence,
            boundary_sha256,
        })
    }
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

    if let Err(error) = parent_file.sync_all() {
        let cleanup = fs::remove_file(&target_path).and_then(|()| parent_file.sync_all());
        return match cleanup {
            Ok(()) => Err(io_error("同步提交父目录", &target_path, error)),
            Err(cleanup_error) => Err(DurableErrorV1::DurabilityUncertain {
                path: target_path,
                message: format!("{error}; 清理失败：{cleanup_error}"),
            }),
        };
    }
    Ok(())
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
