//! 规范产物的显式构建、原子发布和按需读取。
use crate::canonical_run::{CanonicalRunConfig, CanonicalRunScope};
use crate::gate_a::{
    GateAArtifacts, GateAError, GateAOutputConfig, build_gate_a_candidate, open_gate_a_artifacts,
};
use crate::{FrozenDirectSourceInputSetV1, SortConfig};
use rustix::fs::{RenameFlags, renameat_with};
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

pub fn gate_a_formal_sort_config() -> SortConfig {
    SortConfig {
        max_run_rows: 8_192,
        max_run_bytes: 64 * 1024 * 1024,
        max_temp_run_files: 512,
        merge_fan_in: 4,
        max_open_files: 5,
        batch_rows: 1_024,
        max_temp_bytes: 1024 * 1024 * 1024,
    }
}

pub fn gate_a_formal_canonical_config() -> CanonicalRunConfig {
    CanonicalRunConfig {
        decode_batch_rows: 8_192,
        max_decode_batch_bytes: 64 * 1024 * 1024,
        max_temporary_row_bytes: 128 * 1024,
        max_epoch_count_per_source: 1,
        max_pending_ws_terminals: 1,
        max_retained_reducer_entries: 160 * 1024,
        canonical_rule_version: "canonical-production-run-v1".to_owned(),
        sort: gate_a_formal_sort_config(),
    }
}

pub fn gate_a_formal_output_config() -> GateAOutputConfig {
    GateAOutputConfig {
        max_artifact_payload_bytes: 3 * 1024 * 1024,
        max_capture_output_bytes: 1024 * 1024 * 1024,
        max_candidate_directory_bytes: 4 * 1024 * 1024 * 1024,
        max_observation_l2_levels: 256,
        sort: gate_a_formal_sort_config(),
    }
}

#[derive(Debug)]
pub enum GateAPublicationError {
    Artifact(GateAError),
    Invalid(&'static str),
    Io(io::Error),
}

impl fmt::Display for GateAPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Artifact(error) => write!(formatter, "门 A 产物错误：{error}"),
            Self::Invalid(message) => write!(formatter, "发布输入无效：{message}"),
            Self::Io(error) => write!(formatter, "门 A 发布 I/O 错误：{error}"),
        }
    }
}

impl std::error::Error for GateAPublicationError {}

impl From<GateAError> for GateAPublicationError {
    fn from(error: GateAError) -> Self {
        Self::Artifact(error)
    }
}

impl From<io::Error> for GateAPublicationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub struct PublishedGateAFinal {
    root: PathBuf,
    artifacts: GateAArtifacts,
}
impl PublishedGateAFinal {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn artifacts(&self) -> &GateAArtifacts {
        &self.artifacts
    }
}

/// 构建失败只清理本次成功创建并拥有的目录。
pub fn build_gate_a_publication(
    input: &FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    stage: &Path,
    config: &CanonicalRunConfig,
    output_config: &GateAOutputConfig,
) -> Result<(), GateAPublicationError> {
    fs::create_dir(stage)?;
    let mut guard = PublicationCandidateGuard {
        path: stage.to_path_buf(),
        retain: false,
    };
    build_gate_a_candidate(
        input,
        scope,
        &stage.join("artifacts"),
        config,
        output_config,
    )?;
    guard.retain = true;
    Ok(())
}

/// 读取定位材料；真实载荷在游标消费时解码，不重建候选或证书。
pub fn open_published_gate_a_final(
    root: &Path,
) -> Result<PublishedGateAFinal, GateAPublicationError> {
    let artifacts = open_gate_a_artifacts(&root.join("artifacts"))?;
    Ok(PublishedGateAFinal {
        root: root.to_path_buf(),
        artifacts,
    })
}

/// 显式既有目录的发布失败不取得其删除权。
pub fn publish_gate_a_candidate(
    stage: &Path,
    target: &Path,
) -> Result<PathBuf, GateAPublicationError> {
    sync_tree(stage)?;
    let stage_parent = File::open(
        stage
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    let target_parent = File::open(
        target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    renameat_with(
        &stage_parent,
        stage
            .file_name()
            .ok_or(GateAPublicationError::Invalid("候选缺少目录名"))?,
        &target_parent,
        target
            .file_name()
            .ok_or(GateAPublicationError::Invalid("输出缺少目录名"))?,
        RenameFlags::NOREPLACE,
    )
    .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))?;
    target_parent.sync_all()?;
    stage_parent.sync_all()?;
    Ok(target.to_path_buf())
}

struct PublicationCandidateGuard {
    path: PathBuf,
    retain: bool,
}
impl Drop for PublicationCandidateGuard {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

pub(crate) fn sync_tree(path: &Path) -> Result<(), GateAPublicationError> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(GateAPublicationError::Invalid("同步发布目录时发现符号链接"));
        }
        if metadata.is_dir() {
            sync_tree(&entry.path())?;
        } else if metadata.is_file() {
            File::open(entry.path())?.sync_all()?;
        } else {
            return Err(GateAPublicationError::Invalid("同步发布目录时发现特殊文件"));
        }
    }
    File::open(path)?.sync_all()?;
    Ok(())
}

fn commit_candidate_no_replace(
    mut guard: PublicationCandidateGuard,
    target_name: &str,
) -> Result<PathBuf, GateAPublicationError> {
    let stage = guard.path.clone();
    let parent = stage
        .parent()
        .ok_or(GateAPublicationError::Invalid("发布候选缺少父目录"))?;
    let stage_name = stage
        .file_name()
        .ok_or(GateAPublicationError::Invalid("发布候选缺少目录名"))?;
    let target = parent.join(target_name);
    let parent_file = File::open(parent)?;
    renameat_with(
        &parent_file,
        stage_name,
        &parent_file,
        target_name,
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))?;
    guard.path = target.clone();
    #[cfg(test)]
    crate::public_recording::faults::at("after_rename")?;
    parent_file.sync_all()?;
    guard.retain = true;
    Ok(target)
}

pub(crate) fn commit_public_segment(
    stage: PathBuf,
    target_name: &str,
) -> Result<PathBuf, GateAPublicationError> {
    commit_candidate_no_replace(
        PublicationCandidateGuard {
            path: stage,
            retain: false,
        },
        target_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate_a::{
        CanonicalEventStoreCursor, tests::build_published_final_fixture_candidate,
    };

    #[test]
    fn published_artifacts_read_legacy_extra_manifest_fields_without_certificates() {
        let fixture = build_published_final_fixture_candidate();
        let root = tempfile::TempDir::new().unwrap();
        let stage = root.path().join("stage");
        fs::create_dir(&stage).unwrap();
        fs::rename(&fixture.candidate_path, stage.join("artifacts")).unwrap();
        let manifest = stage.join("artifacts/manifests/canonical-event-store.json");
        let mut old: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        old["dependencies"] = serde_json::json!([]);
        old["canonical_record_stream_sha256"] = serde_json::json!("8".repeat(64));
        fs::write(&manifest, serde_json::to_vec(&old).unwrap()).unwrap();
        fs::write(
            stage.join("gate-a-receipt.json"),
            b"retired certificate is not a reader input",
        )
        .unwrap();
        let path = publish_gate_a_candidate(&stage, &root.path().join("published")).unwrap();
        let opened = open_published_gate_a_final(&path).unwrap();
        let mut cursor = CanonicalEventStoreCursor::open(
            opened.artifacts().canonical_event_store.manifest_path(),
            4,
            1024 * 1024,
            100,
        )
        .unwrap();
        assert!(!cursor.next_page(100).unwrap().is_empty());
    }

    #[test]
    fn missing_target_parent_leaves_existing_candidate_untouched() {
        let root = tempfile::TempDir::new().unwrap();
        let stage = root.path().join("stage");
        fs::create_dir(&stage).unwrap();
        fs::write(stage.join("user-content"), b"owned by caller").unwrap();
        assert!(publish_gate_a_candidate(&stage, &root.path().join("missing/published")).is_err());
        assert_eq!(
            fs::read(stage.join("user-content")).unwrap(),
            b"owned by caller"
        );
    }
}
