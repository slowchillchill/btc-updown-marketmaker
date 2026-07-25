use super::*;

#[test]
fn dataset_capacity_scan_counts_regular_files_and_rejects_symlinks() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    let training_root = dataset_root.join("training");
    fs::create_dir_all(training_root.join("typed_raw")).unwrap();
    fs::write(training_root.join("typed_raw/a"), [0_u8; 7]).unwrap();
    assert_eq!(training_dataset_size_bytes(&dataset_root).unwrap(), 7);

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            training_root.join("typed_raw/a"),
            training_root.join("typed_raw/link"),
        )
        .unwrap();
        assert!(training_dataset_size_bytes(&dataset_root).is_err());
    }
}
