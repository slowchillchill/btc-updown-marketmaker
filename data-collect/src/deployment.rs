use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectorBinaryRole {
    TrainingCold,
    PaperHot,
    LiveHot,
}

impl CollectorBinaryRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TrainingCold => "training_cold",
            Self::PaperHot => "paper_hot",
            Self::LiveHot => "live_hot",
        }
    }

    pub const fn binary_name(self) -> &'static str {
        match self {
            Self::TrainingCold => "data-collect-training-collector",
            Self::PaperHot => "data-collect-paper-hot",
            Self::LiveHot => "data-collect-live-hot",
        }
    }

    pub const fn output_suffix(self) -> &'static str {
        match self {
            Self::TrainingCold => "training/typed_raw",
            Self::PaperHot => "paper/data_collect_handoff",
            Self::LiveHot => "live/data_collect_handoff",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRuntimeConfig {
    pub role: CollectorBinaryRole,
    pub dataset_root: PathBuf,
    pub output_root: PathBuf,
    pub run_id: String,
    pub max_runtime_secs: Option<u64>,
    pub collector_region_override: Option<String>,
    pub config_path: Option<PathBuf>,
}

impl DeploymentRuntimeConfig {
    pub fn for_role(
        role: CollectorBinaryRole,
        dataset_root: PathBuf,
        run_id: impl Into<String>,
    ) -> Result<Self> {
        let dataset_root = normalize_absolute_root(&dataset_root)?;
        let output_root = role
            .output_suffix()
            .split('/')
            .fold(dataset_root.clone(), |path, component| path.join(component));
        let run_id = run_id.into();
        if run_id.trim().is_empty() {
            return Err(invalid_input("run_id must be non-empty").into());
        }
        Ok(Self {
            role,
            dataset_root,
            output_root,
            run_id,
            max_runtime_secs: None,
            collector_region_override: None,
            config_path: None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorStartupReceipt {
    pub binary_role: String,
    pub binary_name: String,
    pub run_id: String,
    pub git_commit: String,
    pub binary_sha256: String,
    pub config_digest: String,
    pub dataset_root_canonical: PathBuf,
    pub output_root: PathBuf,
    pub collector_host_id: String,
    pub collector_region: String,
    pub host_clock_sync_status: String,
}

impl CollectorStartupReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &DeploymentRuntimeConfig,
        git_commit: impl Into<String>,
        binary_sha256: impl Into<String>,
        config_digest: impl Into<String>,
        collector_host_id: impl Into<String>,
        collector_region: impl Into<String>,
        host_clock_sync_status: impl Into<String>,
    ) -> Self {
        Self {
            binary_role: config.role.as_str().to_owned(),
            binary_name: config.role.binary_name().to_owned(),
            run_id: config.run_id.clone(),
            git_commit: git_commit.into(),
            binary_sha256: binary_sha256.into(),
            config_digest: config_digest.into(),
            dataset_root_canonical: config.dataset_root.clone(),
            output_root: config.output_root.clone(),
            collector_host_id: collector_host_id.into(),
            collector_region: collector_region.into(),
            host_clock_sync_status: host_clock_sync_status.into(),
        }
    }
}

fn normalize_absolute_root(path: &Path) -> Result<PathBuf> {
    validate_path("dataset_root", path)?;
    Ok(path.to_path_buf())
}

fn validate_path(field: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(invalid_input(format!("{field} must be absolute: {}", path.display())).into());
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(invalid_input(format!("{field} must not contain parent components")).into());
    }
    Ok(())
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
