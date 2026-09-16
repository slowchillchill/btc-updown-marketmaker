use super::*;

#[test]
fn dataset_path_check_accepts_regular_files_and_rejects_descendant_symlinks() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    let training_root = dataset_root.join("training");
    // 验证原扫描覆盖的非 typed_raw 后代，不能用恢复器的局部检查替代。
    fs::create_dir_all(training_root.join("other/deep")).unwrap();
    fs::write(training_root.join("other/deep/a"), [0_u8; 7]).unwrap();
    validate_training_dataset_paths(&dataset_root).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            training_root.join("other/deep/a"),
            training_root.join("other/deep/link"),
        )
        .unwrap();
        assert!(
            validate_training_dataset_paths(&dataset_root)
                .unwrap_err()
                .to_string()
                .contains("symlink is forbidden")
        );
    }
}

#[cfg(unix)]
#[test]
fn dataset_path_check_rejects_special_files() {
    let tmp = tempdir().unwrap();
    let training_root = tmp.path().join("training/other");
    fs::create_dir_all(&training_root).unwrap();
    let _socket =
        std::os::unix::net::UnixListener::bind(training_root.join("special.sock")).unwrap();
    assert!(
        validate_training_dataset_paths(tmp.path())
            .unwrap_err()
            .to_string()
            .contains("special file is forbidden")
    );
}
