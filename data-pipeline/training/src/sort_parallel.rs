//! 同轮合并组的有界调度与同一排序器的共享物理资源。
use super::*;
use std::sync::{Mutex, atomic::AtomicU64, mpsc};
use std::time::Duration;

#[cfg(test)]
thread_local! {
    // 仅当前测试线程创建的排序器使用；首轮前两组同时开始，避免夹具依赖调度速度。
    pub(crate) static TEST_START_BARRIER: std::cell::RefCell<Option<Arc<std::sync::Barrier>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Default)]
pub(super) struct ResourceState {
    pub bytes: u64,
    files: usize,
    pub(super) handles: usize,
    pub(super) workers: usize,
    pub peak_bytes: u64,
    peak_files: usize,
    peak_handles: usize,
    pub(super) peak_workers: usize,
    #[cfg(test)]
    pub completions: Vec<usize>,
}

pub(super) struct SortResources {
    pub state: Mutex<ResourceState>,
    pub max_bytes: u64,
    max_files: usize,
    max_handles: usize,
    pub cancelled: AtomicBool,
    pub processed_rows: AtomicU64,
    failure: Mutex<Option<SortError>>,
    #[cfg(test)]
    pub delays: Mutex<Vec<Duration>>,
    #[cfg(test)]
    start_barrier: Option<Arc<std::sync::Barrier>>,
}
impl SortResources {
    pub fn new(config: &SortConfig, bytes: u64, files: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ResourceState {
                bytes,
                files,
                peak_bytes: bytes,
                peak_files: files,
                ..Default::default()
            }),
            max_bytes: config.max_temp_bytes,
            max_files: config.max_temp_run_files,
            max_handles: config.max_open_files,
            cancelled: AtomicBool::new(false),
            processed_rows: AtomicU64::new(0),
            failure: Mutex::new(None),
            #[cfg(test)]
            delays: Mutex::new(Vec::new()),
            #[cfg(test)]
            start_barrier: TEST_START_BARRIER.with(|barrier| barrier.borrow().clone()),
        })
    }
    pub fn create_part(&self) -> Result<(), SortError> {
        let mut state = self.state.lock().unwrap();
        if state.files >= self.max_files {
            return Err(SortError::TempRunFileLimit {
                needed: state.files + 1,
                limit: self.max_files,
            });
        }
        state.files += 1;
        state.peak_files = state.peak_files.max(state.files);
        Ok(())
    }
    pub fn open_handle(self: &Arc<Self>) -> Result<HandleLease, SortError> {
        let mut state = self.state.lock().unwrap();
        if state.handles >= self.max_handles {
            return Err(SortError::OpenFileLimit {
                needed: state.handles + 1,
                limit: self.max_handles,
            });
        }
        state.handles += 1;
        state.peak_handles = state.peak_handles.max(state.handles);
        Ok(HandleLease(Arc::clone(self)))
    }
    pub fn remove_run(&self, run: &RunInfo) -> Result<(), SortError> {
        for path in &run.run.parts {
            let bytes = fs::metadata(path)?.len();
            fs::remove_file(path)?;
            let mut state = self.state.lock().unwrap();
            state.files -= 1;
            state.bytes -= bytes;
        }
        Ok(())
    }
    pub fn apply(&self, metrics: &mut SortMetrics) {
        let state = self.state.lock().unwrap();
        metrics.peak_temp_bytes = metrics.peak_temp_bytes.max(state.peak_bytes);
        metrics.peak_temp_run_files = metrics.peak_temp_run_files.max(state.peak_files);
        metrics.peak_open_files = metrics.peak_open_files.max(state.peak_handles);
        metrics.peak_merge_workers = metrics.peak_merge_workers.max(state.peak_workers);
        metrics.merge_processed_rows = self.processed_rows.load(AtomicOrdering::Relaxed);
    }
    fn progress(
        &self,
        round: usize,
        completed_groups: usize,
        total_groups: usize,
    ) -> MergeProgress {
        let state = self.state.lock().unwrap();
        MergeProgress {
            round,
            completed_groups,
            total_groups,
            active_workers: state.workers,
            processed_rows: self.processed_rows.load(AtomicOrdering::Relaxed),
            peak_active_workers: state.peak_workers,
            peak_temp_bytes: state.peak_bytes,
            peak_temp_files: state.peak_files,
            peak_open_files: state.peak_handles,
        }
    }
}
pub(super) struct HandleLease(Arc<SortResources>);
impl Drop for HandleLease {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().handles -= 1;
    }
}
struct WorkerLease(Arc<SortResources>);
impl WorkerLease {
    fn start(resources: &Arc<SortResources>) -> Self {
        let mut state = resources.state.lock().unwrap();
        state.workers += 1;
        state.peak_workers = state.peak_workers.max(state.workers);
        Self(Arc::clone(resources))
    }
}
impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().workers -= 1;
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn merge_round(
    runs: Vec<RunInfo>,
    root: &Path,
    round: usize,
    next_id: &mut usize,
    config: &SortConfig,
    resources: &Arc<SortResources>,
    metrics: &mut SortMetrics,
    mut digest: Option<&mut Sha256>,
    split: bool,
    progress: &mut dyn FnMut(MergeProgress),
) -> Result<Vec<RunInfo>, SortError> {
    let groups: Vec<_> = {
        let mut remaining = runs.into_iter();
        let mut groups = Vec::new();
        loop {
            let group: Vec<_> = remaining.by_ref().take(config.merge_fan_in).collect();
            if group.is_empty() {
                break;
            }
            groups.push(group);
        }
        groups
    };
    let total = groups.len();
    let first_id = *next_id;
    *next_id += total;
    // 最终轮只有一个组，摘要只在该组唯一写流内形成。
    let mut output: Vec<Option<RunInfo>> = (0..total).map(|_| None).collect();
    let cap = config
        .merge_workers
        .min(total)
        .min(config.max_open_files / 3);
    let (tx, rx) = mpsc::sync_channel(cap);
    let mut completed = 0;
    let mut active = 0;
    let mut reserved_handles = 0;
    let mut pending = groups.into_iter().enumerate().peekable();
    std::thread::scope(|scope| -> Result<(), SortError> {
        loop {
            while active < cap && !resources.cancelled.load(AtomicOrdering::Relaxed) {
                let Some((_, group)) = pending.peek() else {
                    break;
                };
                if group.len() > 1 && reserved_handles + group.len() + 1 > config.max_open_files {
                    break;
                }
                let (number, group) = pending.next().unwrap();
                if group.len() == 1 {
                    output[number] = group.into_iter().next();
                    completed += 1;
                    continue;
                }
                let handles = group.len() + 1;
                reserved_handles += handles;
                active += 1;
                let tx = tx.clone();
                let resources = Arc::clone(resources);
                let group_digest = if total == 1 { digest.take() } else { None };
                scope.spawn(move || {
                    let worker = WorkerLease::start(&resources);
                    #[cfg(test)]
                    {
                        if round == 1
                            && total >= 2
                            && cap >= 2
                            && number < 2
                            && let Some(barrier) = &resources.start_barrier
                        {
                            barrier.wait();
                        }
                        let delay = resources.delays.lock().unwrap().get(number).copied();
                        if let Some(delay) = delay {
                            std::thread::sleep(delay);
                        }
                    }
                    let mut local = SortMetrics::default();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        merge_group(
                            &group,
                            RunDestination {
                                work_directory: root,
                                pass: round,
                                file_id: first_id + number,
                                resources: Arc::clone(&resources),
                            },
                            config,
                            &mut local,
                            group_digest,
                            split,
                        )
                    }))
                    .unwrap_or_else(|_| {
                        Err(SortError::Io(io::Error::other("合并工作线程异常退出")))
                    });
                    let result = match result {
                        Ok(run) => Ok((run, local)),
                        Err(error) => {
                            let mut failure = resources.failure.lock().unwrap();
                            if failure.is_none() {
                                *failure = Some(error);
                            }
                            resources.cancelled.store(true, AtomicOrdering::Relaxed);
                            Err(())
                        }
                    };
                    drop(worker);
                    // 通道只传分片路径和标量，容量至多本轮实际请求工作上限。
                    tx.send((number, handles, group, result)).unwrap();
                });
            }
            progress(resources.progress(round, completed, total));
            if active == 0 {
                break;
            }
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok((number, handles, group, result)) => {
                    active -= 1;
                    reserved_handles -= handles;
                    #[cfg(test)]
                    resources.state.lock().unwrap().completions.push(number);
                    if let Ok((run, local)) = result {
                        // 输入只有实际删除后才归还磁盘额度。失败时仍收回全部活动工作。
                        for input in &group {
                            if let Err(error) = resources.remove_run(input) {
                                let mut failure = resources.failure.lock().unwrap();
                                if failure.is_none() {
                                    *failure = Some(error);
                                }
                                resources.cancelled.store(true, AtomicOrdering::Relaxed);
                                break;
                            }
                        }
                        metrics.peak_write_buffer_rows = metrics
                            .peak_write_buffer_rows
                            .max(local.peak_write_buffer_rows);
                        metrics.peak_write_buffer_bytes = metrics
                            .peak_write_buffer_bytes
                            .max(local.peak_write_buffer_bytes);
                        metrics.peak_heap_entries =
                            metrics.peak_heap_entries.max(local.peak_heap_entries);
                        output[number] = Some(run);
                        completed += 1;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {} // 只报告进度，等待时间不是拒绝条件。
                Err(mpsc::RecvTimeoutError::Disconnected) => unreachable!("协调者保留发送端"),
            }
        }
        Ok(())
    })?;
    resources.apply(metrics);
    progress(resources.progress(round, completed, total));
    if let Some(error) = resources.failure.lock().unwrap().take() {
        return Err(error);
    }
    Ok(output
        .into_iter()
        .map(|run| run.expect("已完成全部有序组"))
        .collect())
}
