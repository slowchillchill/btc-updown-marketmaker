use super::*;

#[test]
fn source_gaps_are_deduped_and_bounded() {
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    record_gap_for_day(&gaps, "2026-07-07", "same_gap".to_owned());
    record_gap_for_day(&gaps, "2026-07-07", "same_gap".to_owned());
    record_gap_for_day(&gaps, "2026-07-08", "same_gap".to_owned());
    for index in 0..(MAX_RECORDED_SOURCE_GAPS + 5) {
        record_gap(&gaps, format!("unique_gap:{index}"));
    }

    let recorded = gaps.lock().unwrap();
    assert_eq!(recorded.iter().filter(|gap| *gap == "same_gap").count(), 1);
    assert_eq!(recorded.reasons_for_day("2026-07-07"), ["same_gap"]);
    assert_eq!(recorded.reasons_for_day("2026-07-08"), ["same_gap"]);
    assert_eq!(recorded.len(), MAX_RECORDED_SOURCE_GAPS);
    assert!(
        recorded
            .last()
            .is_some_and(|gap| gap.starts_with(SOURCE_GAP_OVERFLOW_PREFIX))
    );
}
