use std::collections::BTreeSet;

use data_collect::dedupe::{
    BinanceIdDecision, BinanceRecentIdWindow, FingerprintDecision, LifecycleFingerprintSets,
    TimedFingerprintWindow,
};

#[test]
fn polymarket_dedupes_only_exact_payloads_within_the_same_lifecycle() {
    let mut sets = LifecycleFingerprintSets::with_limit(2);
    sets.replace_lifecycles(BTreeSet::from(["current".to_owned(), "next".to_owned()]))
        .unwrap();

    assert_eq!(
        sets.observe("current", br#"{"price":"0.1","size":"1"}"#),
        Ok(FingerprintDecision::Unique)
    );
    assert_eq!(
        sets.observe("current", br#"{"price":"0.1","size":"1"}"#),
        Ok(FingerprintDecision::Duplicate)
    );
    assert_eq!(
        sets.observe("next", br#"{"price":"0.1","size":"1"}"#),
        Ok(FingerprintDecision::Unique),
        "the same payload in another lifecycle is not a transport replay"
    );
    assert_eq!(
        sets.observe("current", br#"{"price":"0.10","size":"1"}"#),
        Ok(FingerprintDecision::Unique),
        "numeric text that is only business-equivalent must remain distinct"
    );
    assert!(sets.observe("current", br#"{"price":"0.2"}"#).is_err());
}

#[test]
fn polymarket_rollover_preserves_promoted_next_without_copying_retired_state() {
    let mut sets = LifecycleFingerprintSets::with_limit(4);
    sets.replace_lifecycles(BTreeSet::from(["a".to_owned(), "b".to_owned()]))
        .unwrap();
    assert_eq!(
        sets.observe("b", b"prefetched"),
        Ok(FingerprintDecision::Unique)
    );

    sets.replace_lifecycles(BTreeSet::from(["b".to_owned(), "c".to_owned()]))
        .unwrap();

    assert_eq!(
        sets.observe("b", b"prefetched"),
        Ok(FingerprintDecision::Duplicate),
        "the promoted lifecycle keeps its original set"
    );
    assert!(sets.observe("a", b"retired").is_err());
    assert_eq!(sets.lifecycle_count(), 2);
}

#[test]
fn binance_recent_id_window_distinguishes_duplicate_conflict_and_old_replay() {
    let mut window = BinanceRecentIdWindow::with_limit(3);
    assert_eq!(
        window.observe(100, b"payload-100"),
        BinanceIdDecision::Unique
    );
    assert_eq!(
        window.observe(101, b"payload-101"),
        BinanceIdDecision::Unique
    );
    assert_eq!(
        window.observe(102, b"payload-102"),
        BinanceIdDecision::Unique
    );
    assert_eq!(
        window.observe(101, b"payload-101"),
        BinanceIdDecision::Duplicate
    );
    assert_eq!(
        window.observe(101, b"changed-101"),
        BinanceIdDecision::Conflict
    );
    assert_eq!(
        window.observe(101, b"changed-101"),
        BinanceIdDecision::Duplicate,
        "a repeated copy of an already-preserved conflict payload is still an exact replay"
    );
    assert_eq!(
        window.observe(103, b"payload-103"),
        BinanceIdDecision::Unique
    );
    assert_eq!(
        window.observe(99, b"payload-99"),
        BinanceIdDecision::OutsideWindow,
        "an old replay is preserved but does not displace the recent comparison window"
    );
}

#[test]
fn rtds_duplicate_window_is_anchored_to_first_acceptance() {
    let mut window = TimedFingerprintWindow::with_limits(60_000, 2);
    assert_eq!(
        window.observe(b"payload-a", 1_000),
        Ok(FingerprintDecision::Unique)
    );
    assert_eq!(
        window.observe(b"payload-a", 60_999),
        Ok(FingerprintDecision::Duplicate)
    );
    assert_eq!(
        window.observe(b"payload-a", 61_000),
        Ok(FingerprintDecision::Unique),
        "a duplicate at 59:59 must not extend the original expiry"
    );
    assert_eq!(
        window.observe(b"payload-b", 61_001),
        Ok(FingerprintDecision::Unique)
    );
    assert!(window.observe(b"payload-c", 61_002).is_err());

    let mut restored_out_of_order = TimedFingerprintWindow::with_limits(60_000, 2);
    assert_eq!(
        restored_out_of_order.observe(b"payload", 2_000),
        Ok(FingerprintDecision::Unique)
    );
    assert_eq!(
        restored_out_of_order.observe(b"payload", 1_000),
        Ok(FingerprintDecision::Duplicate)
    );
    assert_eq!(
        restored_out_of_order.observe(b"payload", 61_000),
        Ok(FingerprintDecision::Unique),
        "out-of-order recovery must retain the earliest persisted receive time"
    );
}
