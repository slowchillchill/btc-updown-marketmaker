use super::*;

pub(super) fn training_dataset_size_bytes(dataset_root: &Path) -> io::Result<u64> {
    let training_root = dataset_root.join("training");
    let metadata = match fs::symlink_metadata(&training_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
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
    directory_tree_size_bytes(&training_root)
}

fn directory_tree_size_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
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
            total = total.saturating_add(directory_tree_size_bytes(&entry.path())?);
        } else if file_type.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "special file is forbidden in training dataset: {}",
                    entry.path().display()
                ),
            ));
        }
    }
    Ok(total)
}
