use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use crate::publish::Result;
use crate::retention::{RetentionDeleteMode, TrainingRetentionPolicy};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionAdminArgs {
    pub dataset_root: PathBuf,
    pub receipt_path: PathBuf,
    pub threshold_bytes: u64,
    pub delete_mode: RetentionDeleteMode,
    pub current_utc_day: String,
    pub allow_enabled_delete_validation_root: bool,
    pub validation_root: Option<PathBuf>,
    pub validate_options_only: bool,
}

pub fn run_from_env() -> Result<()> {
    run_args(std::env::args().skip(1))
}

pub fn run_args<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = parse_args(args)?;
    validate_common_paths(&args)?;
    if args.delete_mode == RetentionDeleteMode::Enabled {
        validate_enabled_delete_scope(&args)?;
    } else if args.validation_root.is_some() {
        return Err(invalid_input(
            "--validation-root is only allowed with --delete-mode enabled",
        ));
    }

    if args.validate_options_only {
        return Ok(());
    }

    let receipt = TrainingRetentionPolicy::new(&args.dataset_root)?
        .with_threshold_bytes(args.threshold_bytes)
        .with_delete_mode(args.delete_mode)
        .with_current_utc_day(args.current_utc_day)
        .enforce(&args.receipt_path)?;
    println!("{}", serde_json::to_string_pretty(&receipt)?);
    Ok(())
}

pub fn parse_args<I, S>(args: I) -> Result<RetentionAdminArgs>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut dataset_root = None;
    let mut receipt_path = None;
    let mut threshold_bytes = None;
    let mut delete_mode = None;
    let mut current_utc_day = None;
    let mut allow_enabled_delete_validation_root = false;
    let mut validation_root = None;
    let mut validate_options_only = false;
    let mut iter = args.into_iter().map(Into::into);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--dataset-root" => dataset_root = Some(PathBuf::from(next_value(&mut iter, &arg)?)),
            "--receipt-path" => receipt_path = Some(PathBuf::from(next_value(&mut iter, &arg)?)),
            "--threshold-bytes" => {
                let value = next_value(&mut iter, &arg)?;
                threshold_bytes = Some(value.parse::<u64>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid --threshold-bytes {value}: {error}"),
                    )
                })?);
            }
            "--delete-mode" => {
                let value = next_value(&mut iter, &arg)?;
                delete_mode = Some(parse_delete_mode(&value)?);
            }
            "--current-utc-day" => current_utc_day = Some(next_value(&mut iter, &arg)?),
            "--allow-enabled-delete-validation-root" => {
                allow_enabled_delete_validation_root = true;
            }
            "--validation-root" => {
                validation_root = Some(PathBuf::from(next_value(&mut iter, &arg)?))
            }
            "--validate-options-only" => validate_options_only = true,
            "--help" | "-h" => return Err(invalid_input(help_text())),
            unknown => {
                return Err(invalid_input(format!(
                    "unknown retention admin argument: {unknown}"
                )));
            }
        }
    }

    let args = RetentionAdminArgs {
        dataset_root: dataset_root.ok_or_else(|| invalid_input("missing --dataset-root"))?,
        receipt_path: receipt_path.ok_or_else(|| invalid_input("missing --receipt-path"))?,
        threshold_bytes: threshold_bytes
            .ok_or_else(|| invalid_input("missing --threshold-bytes"))?,
        delete_mode: delete_mode.ok_or_else(|| invalid_input("missing --delete-mode"))?,
        current_utc_day: current_utc_day
            .ok_or_else(|| invalid_input("missing --current-utc-day"))?,
        allow_enabled_delete_validation_root,
        validation_root,
        validate_options_only,
    };
    validate_utc_day(&args.current_utc_day)?;
    Ok(args)
}

fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| invalid_input(format!("missing value for {flag}")))
}

fn parse_delete_mode(value: &str) -> Result<RetentionDeleteMode> {
    match value {
        "dry-run" | "dry_run" => Ok(RetentionDeleteMode::DryRun),
        "enabled" => Ok(RetentionDeleteMode::Enabled),
        _ => Err(invalid_input(format!(
            "invalid --delete-mode {value}; expected dry-run or enabled"
        ))),
    }
}

fn validate_common_paths(args: &RetentionAdminArgs) -> Result<()> {
    validate_absolute_no_parent("--dataset-root", &args.dataset_root)?;
    validate_absolute_no_parent("--receipt-path", &args.receipt_path)?;
    if let Some(validation_root) = &args.validation_root {
        validate_absolute_no_parent("--validation-root", validation_root)?;
    }
    if args.receipt_path.file_name().is_none() {
        return Err(invalid_input("--receipt-path must include a file name"));
    }
    if path_has_ancestor(&args.receipt_path, "training", "typed_raw") {
        return Err(invalid_input(
            "--receipt-path must not be inside training/typed_raw",
        ));
    }
    reject_receipt_symlink_or_directory(&args.receipt_path)?;
    reject_receipt_parent_symlink_or_typed_raw_escape(args)?;
    Ok(())
}

fn validate_enabled_delete_scope(args: &RetentionAdminArgs) -> Result<()> {
    if !args.allow_enabled_delete_validation_root {
        return Err(invalid_input(
            "delete-mode enabled requires --allow-enabled-delete-validation-root",
        ));
    }
    let validation_root = args
        .validation_root
        .as_ref()
        .ok_or_else(|| invalid_input("delete-mode enabled requires --validation-root"))?;
    let receipt_parent = args
        .receipt_path
        .parent()
        .ok_or_else(|| invalid_input("--receipt-path must have a parent directory"))?;

    reject_symlink_component("--validation-root", validation_root)?;
    reject_symlink_component("--dataset-root", &args.dataset_root)?;
    let validation_root = canonical_dir("--validation-root", validation_root)?;
    let dataset_root = canonical_dir("--dataset-root", &args.dataset_root)?;
    let receipt_parent = canonical_dir("--receipt-path parent", receipt_parent)?;
    let evidence_root = canonical_dir(
        "validation evidence root",
        &validation_root.join("evidence"),
    )?;

    if !dataset_root.starts_with(&validation_root) {
        return Err(invalid_input(format!(
            "enabled delete dataset root must be under validation root: dataset={} validation={}",
            dataset_root.display(),
            validation_root.display()
        )));
    }
    if !receipt_parent.starts_with(&evidence_root) {
        return Err(invalid_input(format!(
            "enabled delete receipt parent must be under validation evidence root: receipt_parent={} evidence={}",
            receipt_parent.display(),
            evidence_root.display()
        )));
    }
    let typed_raw_root = dataset_root.join("training").join("typed_raw");
    if receipt_parent.starts_with(&typed_raw_root) {
        return Err(invalid_input(
            "enabled delete receipt path must not be inside training/typed_raw",
        ));
    }
    verify_receipt_parent_writable(&args.receipt_path)?;
    Ok(())
}

fn canonical_dir(label: &str, path: &Path) -> Result<PathBuf> {
    let canonical = path.canonicalize().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "{label} must exist and be canonicalizable: {}: {error}",
                path.display()
            ),
        )
    })?;
    if !canonical.is_dir() {
        return Err(invalid_input(format!(
            "{label} must be an existing directory: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn validate_absolute_no_parent(label: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(invalid_input(format!(
            "{label} must be absolute: {}",
            path.display()
        )));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(invalid_input(format!(
            "{label} must not contain parent components: {}",
            path.display()
        )));
    }
    Ok(())
}

fn reject_symlink_component(label: &str, path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_) | Component::RootDir) {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid_input(format!(
                    "{label} must not contain symlink component: {}",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn reject_receipt_symlink_or_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_input(format!(
            "--receipt-path must not be a symlink: {}",
            path.display()
        ))),
        Ok(metadata) if metadata.is_dir() => Err(invalid_input(format!(
            "--receipt-path must not be a directory: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn reject_receipt_parent_symlink_or_typed_raw_escape(args: &RetentionAdminArgs) -> Result<()> {
    let receipt_parent = args
        .receipt_path
        .parent()
        .ok_or_else(|| invalid_input("--receipt-path must have a parent directory"))?;
    reject_symlink_component("--receipt-path parent", receipt_parent)?;
    if receipt_parent.exists() && args.dataset_root.exists() {
        let receipt_parent = canonical_dir("--receipt-path parent", receipt_parent)?;
        let dataset_root = canonical_dir("--dataset-root", &args.dataset_root)?;
        let typed_raw_root = dataset_root.join("training").join("typed_raw");
        if typed_raw_root.exists() {
            let typed_raw_root = typed_raw_root.canonicalize()?;
            if receipt_parent.starts_with(&typed_raw_root) {
                return Err(invalid_input(
                    "--receipt-path parent must not resolve inside training/typed_raw",
                ));
            }
        }
    }
    Ok(())
}

fn verify_receipt_parent_writable(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid_input("--receipt-path must have a parent directory"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("retention_policy_receipt.json");
    let probe = parent.join(format!(".{file_name}.write-probe"));
    if fs::symlink_metadata(&probe).is_ok() {
        return Err(invalid_input(format!(
            "receipt write probe path already exists: {}",
            probe.display()
        )));
    }
    fs::write(&probe, b"probe").map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "receipt parent is not writable: {}: {error}",
                parent.display()
            ),
        )
    })?;
    fs::remove_file(&probe)?;
    Ok(())
}

fn path_has_ancestor(path: &Path, first: &str, second: &str) -> bool {
    let mut saw_first = false;
    for component in path.components() {
        let name = component.as_os_str().to_string_lossy();
        if saw_first && name == second {
            return true;
        }
        saw_first = name == first;
    }
    false
}

fn validate_utc_day(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let valid = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, byte)| idx == 4 || idx == 7 || byte.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(invalid_input(format!(
            "--current-utc-day must use YYYY-MM-DD: {value}"
        )))
    }
}

fn invalid_input(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn help_text() -> &'static str {
    "usage: data-collect-retention-admin --dataset-root <abs-path> --receipt-path <abs-path> --threshold-bytes <u64> --delete-mode dry-run|enabled --current-utc-day YYYY-MM-DD [--allow-enabled-delete-validation-root --validation-root <abs-path>] [--validate-options-only]"
}
