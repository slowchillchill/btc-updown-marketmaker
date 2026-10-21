//! 本次入口的首观察物理计时；不进入配置、轨迹或学习身份。
use serde_json::{Value, json};
use shared_run::MarketFrameTimingV1;
use std::{sync::OnceLock, time::Instant};

pub(crate) struct FirstObservation {
    started: Instant,
    event: OnceLock<Value>,
}
impl FirstObservation {
    pub(crate) fn new(started: Instant) -> Self {
        Self {
            started,
            event: OnceLock::new(),
        }
    }
    pub(crate) fn record(
        &self,
        account: &str,
        call: u64,
        at: i64,
        frame: Option<&MarketFrameTimingV1>,
    ) {
        // 常见后续调用只读一次已初始化标志；只在首事件取时、复制紧凑引用和输出。
        self.event.get_or_init(|| {
            let elapsed = self.started.elapsed().as_secs_f64();
            let event = json!({"elapsed_seconds":elapsed,"account_key":account,"call":call,"processing_start_utc_ms":at,"market_frame":frame});
            eprintln!("{}", json!({"event":"first_observation","first_observation":event}));
            event
        });
    }
    pub(crate) fn snapshot(&self) -> Value {
        self.event.get().cloned().unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    #[test]
    fn concurrent_first_arrival_is_once_and_no_arrival_is_null() {
        let timing = Arc::new(FirstObservation::new(Instant::now()));
        assert!(timing.snapshot().is_null());
        let barrier = Arc::new(Barrier::new(3));
        let threads = ["a", "b"].map(|key| {
            let timing = timing.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                timing.record(key, 1, 123, None);
            })
        });
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        let first = timing.snapshot();
        assert!(["a", "b"].contains(&first["account_key"].as_str().unwrap()));
        timing.record("later", 2, 999, None);
        assert_eq!(timing.snapshot(), first);
    }
}
