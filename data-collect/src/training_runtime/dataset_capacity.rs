use super::*;

pub(super) fn validate_training_dataset_paths(dataset_root: &Path) -> io::Result<()> {
    let training_root = dataset_root.join("training");
    let metadata = match fs::symlink_metadata(&training_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "training dataset root must be a real directory: {}",
                training_root.display()
            ),
        ));
    }
    validate_directory_tree(&training_root)
}

fn validate_directory_tree(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "symlink is forbidden in training dataset: {}",
                    entry.path().display()
                ),
            ));
        }
        if file_type.is_dir() {
            validate_directory_tree(&entry.path())?;
        } else if !file_type.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "special file is forbidden in training dataset: {}",
                    entry.path().display()
                ),
            ));
        }
    }
    Ok(())
}
