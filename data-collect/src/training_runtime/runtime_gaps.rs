use super::*;

pub(super) const MAX_RECORDED_SOURCE_GAPS: usize = 256;
pub(super) const SOURCE_GAP_OVERFLOW_PREFIX: &str = "source_gap_overflow_count:";

#[derive(Clone, Debug, Default)]
pub(super) struct SourceGapStore {
    pub(super) all: Vec<String>,
    by_utc_day: BTreeMap<String, Vec<String>>,
}

impl SourceGapStore {
    pub(super) fn all_reasons(&self) -> Vec<String> {
        self.all.clone()
    }

    pub(super) fn reasons_for_day(&self, utc_day: &str) -> Vec<String> {
        self.by_utc_day.get(utc_day).cloned().unwrap_or_default()
    }
}

impl std::ops::Deref for SourceGapStore {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.all
    }
}

pub(super) type SharedSourceGaps = Arc<Mutex<SourceGapStore>>;

pub fn is_training_runtime_source_gap_reason(reason: &str) -> bool {
    !reason.starts_with("bounded_queue_backpressure_wait:")
        && !reason.starts_with("writer_queue_backpressure_wait:")
}

pub(super) fn record_gap(gaps: &SharedSourceGaps, gap: String) {
    let utc_day = utc_day_string_from_ms(now_ms()).unwrap_or_else(|| "unknown".to_owned());
    record_gap_for_day(gaps, &utc_day, gap);
}

pub(super) fn record_gap_at(gaps: &SharedSourceGaps, recv_time_utc_ms: i64, gap: String) {
    let utc_day = utc_day_string_from_ms(recv_time_utc_ms).unwrap_or_else(|| "unknown".to_owned());
    record_gap_for_day(gaps, &utc_day, gap);
}

pub(super) fn record_gap_for_day(gaps: &SharedSourceGaps, utc_day: &str, gap: String) {
    if !is_training_runtime_source_gap_reason(&gap) {
        return;
    }
    let mut store = gaps.lock().expect("source gap mutex poisoned");
    push_bounded_gap(&mut store.all, gap.clone());
    push_bounded_gap(store.by_utc_day.entry(utc_day.to_owned()).or_default(), gap);
}

fn push_bounded_gap(gaps: &mut Vec<String>, gap: String) {
    if gaps.iter().any(|existing| existing == &gap) {
        return;
    }
    if gaps.len() < MAX_RECORDED_SOURCE_GAPS {
        gaps.push(gap);
        return;
    }
    if let Some(existing) = gaps
        .iter_mut()
        .find(|item| item.starts_with(SOURCE_GAP_OVERFLOW_PREFIX))
    {
        let next_count = existing
            .trim_start_matches(SOURCE_GAP_OVERFLOW_PREFIX)
            .parse::<u64>()
            .unwrap_or(0)
            .saturating_add(1);
        *existing = format!("{SOURCE_GAP_OVERFLOW_PREFIX}{next_count}");
    } else {
        gaps.pop();
        gaps.push(format!("{SOURCE_GAP_OVERFLOW_PREFIX}2"));
    }
}
