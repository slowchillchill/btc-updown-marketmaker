use super::*;

#[test]
fn dataset_root_lock_allows_only_one_manifest_owner_process() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let first = lock_training_dataset_root(&dataset_root).unwrap();
    let error = lock_training_dataset_root(&dataset_root).unwrap_err();
    assert!(error.to_string().contains("training_dataset_root_locked"));
    drop(first);
    lock_training_dataset_root(&dataset_root).unwrap();
}
