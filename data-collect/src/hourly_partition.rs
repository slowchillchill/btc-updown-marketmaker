use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use time::OffsetDateTime;

use crate::publish::Result;

const HOUR_MS: i64 = 3_600_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HourlyPartitionPolicy {
    dataset_root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HourlyPartition {
    pub dataset_root: PathBuf,
    pub source_kind: String,
    pub utc_day: String,
    pub utc_hour: String,
    pub hour_start_utc_ms: i64,
    pub hour_end_utc_ms: i64,
    pub partition_path: PathBuf,
    pub running_path: PathBuf,
    pub final_path: PathBuf,
    pub stem: String,
}

impl HourlyPartitionPolicy {
    pub fn new(dataset_root: &Path) -> Result<Self> {
        Ok(Self {
            dataset_root: validate_absolute_root(dataset_root)?,
        })
    }

    pub fn partition(
        &self,
        source_kind: &str,
        recv_time_utc_ms: i64,
        part: Option<u32>,
    ) -> Result<HourlyPartition> {
        let source_kind = validate_path_component("source_kind", source_kind)?;
        if part == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "part must be greater than zero",
            )
            .into());
        }

        let hour_start_utc_ms = recv_time_utc_ms.div_euclid(HOUR_MS) * HOUR_MS;
        let hour_end_utc_ms = hour_start_utc_ms + HOUR_MS;
        let hour_start = OffsetDateTime::from_unix_timestamp(hour_start_utc_ms.div_euclid(1000))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let utc_day = format!(
            "{:04}-{:02}-{:02}",
            hour_start.year(),
            u8::from(hour_start.month()),
            hour_start.day()
        );
        let utc_hour = format!("{utc_day}T{:02}", hour_start.hour());
        let partition_path = self
            .dataset_root
            .join("training")
            .join("typed_raw")
            .join(format!("utc_day={utc_day}"))
            .join(format!("source_kind={source_kind}"));

        let hour = format!("{:02}", hour_start.hour());
        let stem = match part {
            Some(part) => format!("{source_kind}_{hour}-part-{part}"),
            None => format!("{source_kind}_{hour}"),
        };
        let running_path = partition_path.join(format!("{stem}.parquet.running"));
        let final_path = partition_path.join(format!("{stem}.parquet"));

        Ok(HourlyPartition {
            dataset_root: self.dataset_root.clone(),
            source_kind: source_kind.to_owned(),
            utc_day,
            utc_hour,
            hour_start_utc_ms,
            hour_end_utc_ms,
            partition_path,
            running_path,
            final_path,
            stem,
        })
    }
}

pub fn is_legacy_source_first_partition(path: &Path) -> bool {
    let components: Vec<String> = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str().map(ToOwned::to_owned),
            _ => None,
        })
        .collect();

    components.windows(4).any(|window| {
        window[0] == "training"
            && window[1] == "typed_raw"
            && window[2].starts_with("source_kind=")
            && window[3].starts_with("utc_day=")
    })
}

pub fn ensure_hourly_partition_dir(partition: &HourlyPartition) -> Result<()> {
    fs::create_dir_all(&partition.partition_path)?;
    Ok(())
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

fn validate_path_component<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    if value.trim().is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} is not a safe path component"),
        )
        .into());
    }
    Ok(value)
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
