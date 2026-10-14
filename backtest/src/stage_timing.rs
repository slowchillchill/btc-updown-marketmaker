//! #185 有限测量开关：原研究整点及运行末输出累计值；不参与业务判断。
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
    pub(crate) fn emit_checkpoint(&self, generation: u64) {
        eprintln!(
            "stage_timing_checkpoint={}",
            serde_json::json!({"scope":self.label,"generation":generation,
                "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples})
        );
    }
    pub(crate) fn emit_hour(&self, boundary_utc_ms: i64) {
        eprintln!(
            "stage_timing_hour={}",
            serde_json::json!({"scope":self.label,"boundary_utc_ms":boundary_utc_ms,
                "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples})
        );
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

#[cfg(all(test, feature = "stage-timing"))]
mod tests {
    use super::*;

    #[test]
    fn hourly_output_keeps_cumulative_counts_and_maximum() {
        let mut timings = StageTimings::new("candidate cumulative-test".into());
        timings.record("calculate", std::time::Duration::from_nanos(3));
        timings.emit_hour(3_600_000);
        timings.record("calculate", std::time::Duration::from_nanos(7));
        timings.emit_hour(7_200_000);
        let sample = &timings.samples["calculate"];
        assert_eq!(sample.count, 2);
        assert_eq!(sample.total_ns, 10);
        assert_eq!(sample.max_ns, 7);
    }
}
