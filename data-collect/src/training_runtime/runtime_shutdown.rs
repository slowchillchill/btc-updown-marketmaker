use super::*;

pub(super) struct RuntimeExitWatcher {
    pub(super) join: Option<tokio::task::JoinHandle<()>>,
}

impl RuntimeExitWatcher {
    pub(super) fn new(join: tokio::task::JoinHandle<()>) -> Self {
        Self { join: Some(join) }
    }

    pub(super) async fn abort_and_join(&mut self) {
        let Some(join) = self.join.take() else {
            return;
        };
        join.abort();
        let _ = join.await;
    }
}

impl Drop for RuntimeExitWatcher {
    fn drop(&mut self) {
        if let Some(join) = self.join.as_ref() {
            join.abort();
        }
    }
}

pub(super) async fn abort_manifest_owner_if_running(
    handle: &mut tokio::task::JoinHandle<Result<()>>,
    deadline: Instant,
) -> bool {
    if handle.is_finished() {
        return false;
    }
    handle.abort();
    let remaining = deadline.saturating_duration_since(Instant::now());
    if !remaining.is_zero() {
        let _ = timeout(remaining, handle).await;
    }
    true
}

pub(super) async fn converge_spawned_runtime_resources(
    handles: &mut Vec<SourceTaskHandle>,
    writer_handles: &mut BTreeMap<String, SourceWriterHandle>,
    manifest_owner_handle: &mut tokio::task::JoinHandle<Result<()>>,
    runtime_exit_watcher: &mut RuntimeExitWatcher,
    source_close: Option<&RuntimeCloseIdentity>,
    writer_close: Option<RuntimeCloseIdentity>,
    deadline: Instant,
) {
    runtime_exit_watcher.abort_and_join().await;
    for handle in handles.iter_mut() {
        if handle.is_finished() {
            continue;
        }
        if source_close.is_none_or(|close| handle.abort_with_permit(close).is_err()) {
            handle.abort_without_permit();
        }
    }
    while let Some(handle) = handles.pop() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        if let Some(close) = source_close {
            let _ = timeout(remaining, handle.await_termination(close)).await;
        } else {
            let _ = timeout(remaining, handle.join).await;
        }
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if let Some(close) = writer_close {
        let _ =
            shutdown_writer_workers_with_budget(std::mem::take(writer_handles), close, remaining)
                .await;
    } else {
        let _ = stop_writer_workers_with_budget(std::mem::take(writer_handles), remaining).await;
    }
    manifest_owner_handle.abort();
    let remaining = deadline.saturating_duration_since(Instant::now());
    if !remaining.is_zero() {
        let _ = timeout(remaining, manifest_owner_handle).await;
    }
}

pub(super) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
