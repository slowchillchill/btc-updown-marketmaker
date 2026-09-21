use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaperLiveEnvironment {
    Paper,
    Live,
}

impl PaperLiveEnvironment {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}

impl fmt::Display for PaperLiveEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn paper_live_handoff_root(
    dataset_root: &Path,
    environment: PaperLiveEnvironment,
) -> Result<PathBuf> {
    let dataset_root = validate_absolute_root(dataset_root)?;
    let artifact_root = dataset_root
        .join(environment.as_str())
        .join("data_collect_handoff");
    Ok(artifact_root)
}

fn validate_absolute_root(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("dataset root must be absolute: {}", root.display()),
        )
        .into());
    }
    reject_parent_components("dataset_root", root)?;
    Ok(root.to_path_buf())
}

fn reject_parent_components(field: &str, path: &Path) -> Result<()> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} must not contain parent components"),
        )
        .into());
    }
    Ok(())
}
