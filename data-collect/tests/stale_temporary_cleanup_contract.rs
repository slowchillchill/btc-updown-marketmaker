use std::fs::{self, File};
use std::io::Read as _;
use std::os::unix::fs::symlink;
use std::process::{Command, Stdio};
use std::sync::Mutex;

use data_collect::daily_manifest::remove_unowned_stale_temporary_files;
use tempfile::tempdir;

// 子进程 exec 前会继承其他线程的 CLOEXEC 文件描述符，隔离各租约场景的持有期。
static HOLDER_SCENARIO: Mutex<()> = Mutex::new(());

#[test]
fn stale_cleanup_removes_only_unowned_temporary_files_inside_typed_raw() {
    let _scenario = HOLDER_SCENARIO.lock().unwrap();
    let tmp = tempdir().unwrap();
    let source = tmp
        .path()
        .join("training/typed_raw/utc_day=2026-07-14/source_kind=binance_aggtrade");
    fs::create_dir_all(&source).unwrap();
    let running = source.join("binance_aggtrade_00.parquet.running");
    let finalized = source.join("binance_aggtrade_00.parquet");
    let manifest_tmp = source.parent().unwrap().join("manifest.json.tmp");
    fs::write(&running, b"partial").unwrap();
    fs::write(&finalized, b"finalized").unwrap();
    fs::write(&manifest_tmp, b"partial manifest").unwrap();

    let removed = remove_unowned_stale_temporary_files(tmp.path()).unwrap();

    assert_eq!(removed.len(), 2);
    assert!(
        removed
            .iter()
            .any(|path| path.ends_with("manifest.json.tmp"))
    );
    assert!(
        removed
            .iter()
            .any(|path| path.ends_with(".parquet.running"))
    );
    assert!(!running.exists());
    assert!(!manifest_tmp.exists());
    assert_eq!(fs::read(finalized).unwrap(), b"finalized");
}

#[test]
fn stale_cleanup_refuses_a_file_held_by_the_current_uid() {
    let _scenario = HOLDER_SCENARIO.lock().unwrap();
    let tmp = tempdir().unwrap();
    let source = tmp
        .path()
        .join("training/typed_raw/utc_day=2026-07-14/source_kind=binance_depth_update");
    fs::create_dir_all(&source).unwrap();
    let running = source.join("binance_depth_update_00.parquet.running");
    fs::write(&running, b"partial").unwrap();
    let open = File::open(&running).unwrap();

    let error = remove_unowned_stale_temporary_files(tmp.path())
        .expect_err("an open running file must fail closed");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert!(running.exists());

    drop(open);
    assert_eq!(
        remove_unowned_stale_temporary_files(tmp.path())
            .unwrap()
            .len(),
        1
    );
    assert!(!running.exists());
}

#[test]
fn stale_cleanup_refuses_a_non_collector_same_uid_process_holder() {
    let _scenario = HOLDER_SCENARIO.lock().unwrap();
    let tmp = tempdir().unwrap();
    let source = tmp
        .path()
        .join("training/typed_raw/utc_day=2026-07-14/source_kind=binance_depth_update");
    fs::create_dir_all(&source).unwrap();
    let running = source.join("binance_depth_update_00.parquet.running");
    fs::write(&running, b"partial").unwrap();
    let mut child = Command::new("sh")
        .args(["-c", "exec 9<\"$1\"; printf ready; read _", "plain-holder"])
        .arg(&running)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = [0_u8; 5];
    child
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut ready)
        .unwrap();
    assert_eq!(&ready, b"ready");

    let cleanup = remove_unowned_stale_temporary_files(tmp.path());
    child.kill().unwrap();
    child.wait().unwrap();
    let error = cleanup.expect_err("a non-collector holder must fail closed");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert!(running.exists());

    assert_eq!(
        remove_unowned_stale_temporary_files(tmp.path())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn stale_cleanup_rejects_symlink_escape_without_touching_target() {
    let _scenario = HOLDER_SCENARIO.lock().unwrap();
    let tmp = tempdir().unwrap();
    let outside = tempdir().unwrap();
    let day = tmp.path().join("training/typed_raw/utc_day=2026-07-14");
    fs::create_dir_all(&day).unwrap();
    let outside_running = outside.path().join("outside.parquet.running");
    fs::write(&outside_running, b"do not remove").unwrap();
    symlink(outside.path(), day.join("source_kind=binance_aggtrade")).unwrap();

    let error = remove_unowned_stale_temporary_files(tmp.path())
        .expect_err("a source directory symlink must fail closed");
    assert!(error.to_string().contains("symlink is forbidden"));
    assert_eq!(fs::read(outside_running).unwrap(), b"do not remove");
}
