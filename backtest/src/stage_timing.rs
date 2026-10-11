//! #185 有限测量开关：仅 run/day 边界聚合，运行末输出一次；不参与业务判断。
#[cfg(feature = "stage-timing")]
#[derive(Default, serde::Serialize)]
struct Sample {
    count: u64,
    total_ns: u128,
    max_ns: u128,
}
#[cfg(feature = "stage-timing")]
pub(crate) struct StageTimings {
    label: String,
    started: std::time::Instant,
    samples: std::collections::BTreeMap<&'static str, Sample>,
}
#[cfg(feature = "stage-timing")]
impl StageTimings {
    pub(crate) fn new(label: String) -> Self {
        Self {
            label,
            started: std::time::Instant::now(),
            samples: Default::default(),
        }
    }
    pub(crate) fn record(&mut self, name: &'static str, elapsed: std::time::Duration) {
        let sample = self.samples.entry(name).or_default();
        sample.count += 1;
        let ns = elapsed.as_nanos();
        sample.total_ns += ns;
        sample.max_ns = sample.max_ns.max(ns);
    }
}
#[cfg(feature = "stage-timing")]
impl Drop for StageTimings {
    fn drop(&mut self) {
        eprintln!(
            "stage_timing={}",
            serde_json::json!({"scope":self.label,"scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples})
        );
    }
}
macro_rules! measure_stage {
    ($timings:ident, $name:literal, $expression:expr) => {{
        #[cfg(feature = "stage-timing")]
        let started = std::time::Instant::now();
        let result = $expression;
        #[cfg(feature = "stage-timing")]
        $timings.record($name, started.elapsed());
        result
    }};
}
