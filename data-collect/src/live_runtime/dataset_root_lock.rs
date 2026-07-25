use super::*;

pub(super) fn lock_training_dataset_root(dataset_root: &Path) -> Result<File> {
    let directory = File::open(dataset_root)?;
    directory.try_lock().map_err(|error| {
        invalid_data(format!(
            "operator_intervention_required:training_dataset_root_locked:{}:{error}",
            dataset_root.display()
        ))
    })?;
    Ok(directory)
}
