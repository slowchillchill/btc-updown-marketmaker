//! 只读标量监督线程；不持有策略、driver、账户或提交能力。
use std::{
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub phase: &'static str,
    pub applied_inputs: u64,
    pub public_groups: u64,
    pub calculations_started: u64,
    pub calculations_completed: u64,
    pub calculating: bool,
}
impl Default for Snapshot {
    fn default() -> Self {
        Self {
            phase: "加载规则与连接输入",
            applied_inputs: 0,
            public_groups: 0,
            calculations_started: 0,
            calculations_completed: 0,
            calculating: false,
        }
    }
}
pub(crate) struct Monitor {
    snapshot: Arc<Mutex<Snapshot>>,
    stop: mpsc::Sender<()>,
}
impl Monitor {
    pub fn start(interval: Duration) -> Self {
        Self::spawn(interval, |p, elapsed| {
            eprintln!(
                "阶段={} 已应用输入={} 公开组={} 已开始计算={} 已完成计算={} 计算中={} 已耗时={:.3}s 总量=未知 剩余时间=未知",
                p.phase,
                p.applied_inputs,
                p.public_groups,
                p.calculations_started,
                p.calculations_completed,
                p.calculating,
                elapsed.as_secs_f64()
            );
        })
    }
    fn spawn(
        interval: Duration,
        mut report: impl FnMut(Snapshot, Duration) + Send + 'static,
    ) -> Self {
        let snapshot = Arc::new(Mutex::new(Snapshot::default()));
        let observed = snapshot.clone();
        let (stop, stopped) = mpsc::channel();
        // std线程不依赖current_thread运行时轮询；同步记录等待时仍能监督。
        // 仅保留标量，退出不join，不等待下一次长interval，也没有业务能力。
        std::thread::spawn(move || {
            let started = Instant::now();
            loop {
                let p = observed.lock().expect("短时标量更新").clone();
                report(p, started.elapsed());
                match stopped.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
        });
        Self { snapshot, stop }
    }
    pub fn update(&self, snapshot: Snapshot) {
        *self.snapshot.lock().expect("唯一宿主同步更新标量") = snapshot;
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        let _ = self.stop.send(());
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalar_monitor_runs_during_host_sync_wait_and_does_not_join_long_interval() {
        let (reports, seen) = mpsc::channel();
        let monitor = Monitor::spawn(Duration::from_millis(10), move |p, _| {
            reports.send(p.phase).unwrap();
        });
        seen.recv_timeout(Duration::from_secs(2)).unwrap();
        monitor.update(Snapshot {
            phase: "同步记录容量等待",
            ..Snapshot::default()
        });
        // 本宿主线程阻塞在真实标准通道接收，没有轮询tokio运行时。
        loop {
            if seen.recv_timeout(Duration::from_secs(2)).unwrap() == "同步记录容量等待" {
                break;
            }
        }
        drop(monitor);
        let (reports, seen) = mpsc::channel::<()>();
        let monitor = Monitor::spawn(Duration::from_secs(3600), move |_, _| {
            reports.send(()).unwrap();
        });
        seen.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(monitor);
        assert!(matches!(
            seen.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }
}
