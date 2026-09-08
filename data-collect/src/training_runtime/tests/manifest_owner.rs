use super::*;

pub(super) fn test_manifest_owner(
    dataset_root: &Path,
    utc_day: &str,
) -> (ManifestOwnerState, PathBuf) {
    let identity = test_artifact_identity();
    let day_dir = typed_raw_root(dataset_root).join(format!("utc_day={utc_day}"));
    let manifest = reconcile_open_day_manifest(&day_dir, utc_day, &identity).unwrap();
    let mut owner = ManifestOwnerState::new(dataset_root.to_path_buf(), identity);
    owner
        .insert_reconciled_manifest(utc_day.to_owned(), manifest)
        .unwrap();
    (owner, day_dir)
}

pub(super) fn reactivated_manifest_owner(
    dataset_root: &Path,
    utc_day: &str,
    first_started_utc_ms: i64,
    last_started_utc_ms: i64,
) -> (ManifestOwnerState, PathBuf) {
    let (mut first_owner, day_dir) = test_manifest_owner(dataset_root, utc_day);
    first_owner
        .activate_initial(utc_day, first_started_utc_ms, INITIAL_RAW_DAY_GENERATION)
        .unwrap();
    drop(first_owner);

    let (mut recovered_owner, recovered_day_dir) = test_manifest_owner(dataset_root, utc_day);
    assert_eq!(recovered_day_dir, day_dir);
    recovered_owner
        .activate_initial(utc_day, last_started_utc_ms, INITIAL_RAW_DAY_GENERATION)
        .unwrap();
    (recovered_owner, day_dir)
}

pub(super) fn manifest_coverage_value(day_dir: &Path) -> Value {
    load_day_manifest(day_dir).unwrap().unwrap()["collection_day_coverage"].clone()
}

pub(super) fn manifest_file_digest(day_dir: &Path) -> String {
    sha256_file(&day_dir.join(DAILY_MANIFEST_FILE_NAME)).unwrap()
}

#[test]
fn normal_rollover_waits_for_four_producers_and_seven_writers() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            boundary_utc_ms - 1_000,
            INITIAL_RAW_DAY_GENERATION,
        )
        .unwrap();
    for producer in LOGICAL_PRODUCERS {
        assert!(
            owner
                .record_producer_barrier(
                    close.clone(),
                    (*producer).to_owned(),
                    &[],
                    &BTreeMap::new(),
                )
                .unwrap()
                .is_none()
        );
    }
    for source in &training_source_kinds()[..training_source_kinds().len() - 1] {
        assert!(
            owner
                .record_writer_ack(close.clone(), (*source).to_owned())
                .unwrap()
                .is_none()
        );
    }
    assert!(
        owner
            .commit_rollover_old_day(&close, ManifestRolloverCommitMode::Normal)
            .is_err()
    );
    assert_eq!(
        manifest_state(&load_day_manifest(&day_dir).unwrap().unwrap()).unwrap(),
        "open"
    );
    assert!(
        owner
            .record_writer_ack(
                close.clone(),
                training_source_kinds().last().unwrap().to_string(),
            )
            .unwrap()
            .is_some()
    );
    assert_eq!(
        manifest_state(&load_day_manifest(&day_dir).unwrap().unwrap()).unwrap(),
        "open",
        "ten ready participants still require the main-owned commit decision"
    );
}

#[test]
fn pending_exit_rollover_closes_old_day_without_activating_new_day() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let close = test_rollover_close(1_783_468_800_000);
    let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            close.boundary_utc_ms - 10_000,
            close.generation,
        )
        .unwrap();
    for producer in LOGICAL_PRODUCERS {
        owner
            .record_producer_barrier(close.clone(), (*producer).to_owned(), &[], &BTreeMap::new())
            .unwrap();
    }
    for source in training_source_kinds() {
        owner
            .record_writer_ack(close.clone(), (*source).to_owned())
            .unwrap();
    }

    owner
        .commit_rollover_old_day(
            &close,
            ManifestRolloverCommitMode::PendingExitKnown {
                ended_utc_ms: close.boundary_utc_ms,
            },
        )
        .unwrap();
    owner.prepare_rollover_exit_day(&close).unwrap();

    let old = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&old).unwrap(), "closed");
    assert_eq!(old["collection_day_coverage"]["state"], "ended_known");
    assert_eq!(
        old["collection_day_coverage"]["ended_utc_ms"],
        close.boundary_utc_ms
    );
    let new_day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let new = load_day_manifest(&new_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&new).unwrap(), "open");
    assert_eq!(new["collection_day_coverage"]["state"], "not_started");
    assert_eq!(
        new["collection_day_coverage"]["reactivated_within_day"],
        false
    );
    assert!(owner.active_epoch.is_none());
    assert!(owner.rollover.is_none());
}

#[test]
fn unclean_pending_exit_closes_old_day_without_creating_new_day() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let close = test_rollover_close(1_783_468_800_000);
    let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            close.boundary_utc_ms - 10_000,
            close.generation,
        )
        .unwrap();
    for producer in LOGICAL_PRODUCERS {
        owner
            .record_producer_barrier(close.clone(), (*producer).to_owned(), &[], &BTreeMap::new())
            .unwrap();
    }
    for source in training_source_kinds() {
        owner
            .record_writer_ack(close.clone(), (*source).to_owned())
            .unwrap();
    }

    owner
        .commit_rollover_old_day(&close, ManifestRolloverCommitMode::UncleanOldDay)
        .unwrap();
    owner.finish_unclean_rollover(&close).unwrap();

    let old = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&old).unwrap(), "closed");
    assert_eq!(
        old["collection_day_coverage"]["state"],
        "unclean_end_unknown"
    );
    assert!(old["collection_day_coverage"]["ended_utc_ms"].is_null());
    let new_day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    assert!(load_day_manifest(&new_day_dir).unwrap().is_none());
    assert!(owner.active_epoch.is_none());
    assert!(owner.rollover.is_none());
}

#[test]
fn rollover_rejects_wrong_generation_or_close_id() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let close = test_rollover_close(1_783_468_800_000);
    let (mut owner, _) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            close.boundary_utc_ms - 1_000,
            close.generation,
        )
        .unwrap();

    let mut wrong_generation = close.clone();
    wrong_generation.generation += 1;
    wrong_generation.next_generation += 1;
    assert!(
        owner
            .record_producer_barrier(
                wrong_generation,
                "polymarket".to_owned(),
                &[],
                &BTreeMap::new(),
            )
            .unwrap_err()
            .to_string()
            .contains("active epoch mismatch")
    );

    owner
        .record_producer_barrier(
            close.clone(),
            "polymarket".to_owned(),
            &[],
            &BTreeMap::new(),
        )
        .unwrap();
    let mut wrong_close_id = close.clone();
    wrong_close_id.close_id += 1;
    assert!(
        owner
            .record_writer_ack(wrong_close_id, training_source_kinds()[0].to_owned())
            .unwrap_err()
            .to_string()
            .contains("tuple mismatch")
    );
}

#[test]
fn rejected_duplicate_or_late_producer_barrier_preserves_manifest_digest() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let close = test_rollover_close(1_783_468_800_000);
    let (mut owner, day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            close.boundary_utc_ms - 1_000,
            close.generation,
        )
        .unwrap();

    owner
        .record_producer_barrier(
            close.clone(),
            LOGICAL_PRODUCERS[0].to_owned(),
            &[],
            &BTreeMap::new(),
        )
        .unwrap();
    let rejected_counters = BTreeMap::from([(
        training_source_kinds()[0].to_owned(),
        SourceOperationalCounterDelta {
            dropped_rows: 1,
            ..SourceOperationalCounterDelta::default()
        },
    )]);
    let digest_before_duplicate = manifest_file_digest(&day_dir);
    let duplicate_error = owner
        .record_producer_barrier(
            close.clone(),
            LOGICAL_PRODUCERS[0].to_owned(),
            &["rejected_duplicate_gap".to_owned()],
            &rejected_counters,
        )
        .unwrap_err();
    assert!(
        duplicate_error
            .to_string()
            .contains("duplicate logical producer barrier")
    );
    assert_eq!(
        manifest_file_digest(&day_dir),
        digest_before_duplicate,
        "a rejected duplicate producer barrier must not change the disk manifest"
    );

    for producer in &LOGICAL_PRODUCERS[1..] {
        owner
            .record_producer_barrier(close.clone(), (*producer).to_owned(), &[], &BTreeMap::new())
            .unwrap();
    }
    let mut ready = None;
    for source_kind in training_source_kinds() {
        ready = owner
            .record_writer_ack(close.clone(), (*source_kind).to_owned())
            .unwrap()
            .or(ready);
    }
    assert!(ready.is_some());

    let digest_before_late = manifest_file_digest(&day_dir);
    let late_error = owner
        .record_producer_barrier(
            close,
            LOGICAL_PRODUCERS[0].to_owned(),
            &["rejected_late_gap".to_owned()],
            &rejected_counters,
        )
        .unwrap_err();
    assert!(
        late_error
            .to_string()
            .contains("producer barrier arrived after rollover ready")
    );
    assert_eq!(
        manifest_file_digest(&day_dir),
        digest_before_late,
        "a rejected late producer barrier must not change the disk manifest"
    );
}

#[test]
fn manifest_owner_requires_rendezvous_then_two_normal_rollover_cutpoints() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let day = "2026-07-07";
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, day_dir) = reactivated_manifest_owner(
        &dataset_root,
        day,
        boundary_utc_ms - 2_000,
        boundary_utc_ms - 1_000,
    );
    let active_coverage = manifest_coverage_value(&day_dir);
    assert_eq!(active_coverage["reactivated_within_day"], true);
    owner
        .record_gaps(day, &["sticky_coverage_test_gap".to_owned()])
        .unwrap();
    assert_eq!(manifest_coverage_value(&day_dir), active_coverage);

    let operational = BTreeMap::from([(
        "binance_aggtrade".to_owned(),
        SourceOperationalCounterDelta {
            structural_dedupe_dropped_events: 2,
            lag_events: 1,
            reconnect_events: 3,
            dropped_rows: 0,
        },
    )]);
    owner
        .record_operational_counters(day, &operational)
        .unwrap();
    assert_eq!(manifest_coverage_value(&day_dir), active_coverage);
    let no_operational_counters = BTreeMap::new();
    for producer in LOGICAL_PRODUCERS {
        assert!(
            owner
                .record_producer_barrier(
                    close.clone(),
                    (*producer).to_owned(),
                    &[],
                    &no_operational_counters,
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(manifest_coverage_value(&day_dir), active_coverage);
    }
    for source in &training_source_kinds()[..training_source_kinds().len() - 1] {
        assert!(
            owner
                .record_writer_ack(close.clone(), (*source).to_owned())
                .unwrap()
                .is_none()
        );
        assert_eq!(manifest_coverage_value(&day_dir), active_coverage);
    }
    let open = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&open).unwrap(), "open");

    let ready = owner
        .record_writer_ack(
            close.clone(),
            training_source_kinds().last().unwrap().to_string(),
        )
        .unwrap()
        .expect("ten-participant rendezvous must become ready exactly once");
    assert_eq!(manifest_coverage_value(&day_dir), active_coverage);
    assert_eq!(ready.close, close);
    assert_eq!(
        manifest_state(&load_day_manifest(&day_dir).unwrap().unwrap()).unwrap(),
        "open",
        "rendezvous readiness alone must not close the old day"
    );

    owner
        .commit_rollover_old_day(&close, ManifestRolloverCommitMode::Normal)
        .unwrap();
    let closed = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&closed).unwrap(), "closed");
    assert_eq!(closed["closure_reason"], "utc_day_barrier");
    assert_eq!(closed["collection_day_coverage"]["state"], "ended_known");
    assert_eq!(
        closed["collection_day_coverage"]["reactivated_within_day"],
        true
    );
    assert_eq!(
        closed["collection_day_coverage"]["ended_utc_ms"],
        boundary_utc_ms
    );
    assert_eq!(closed["producer_barriers"].as_array().unwrap().len(), 4);
    assert_eq!(closed["writer_finalize_acks"].as_array().unwrap().len(), 7);
    assert_eq!(closed["source_counters"].as_object().unwrap().len(), 7);
    assert_eq!(
        closed["source_counters"]["binance_aggtrade"]["structural_dedupe_dropped_events"],
        2
    );
    assert_eq!(
        closed["source_counters"]["binance_aggtrade"]["lag_events"],
        1
    );
    assert_eq!(
        closed["source_counters"]["binance_aggtrade"]["reconnect_events"],
        3
    );
    assert_eq!(
        fs::read_dir(&day_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_type().unwrap().is_file())
            .count(),
        1,
        "the UTC day root may contain only its one manifest file"
    );
    let next_day_dir =
        typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    assert!(!next_day_dir.exists());
    owner.activate_rollover_new_day(&close).unwrap();
    let active = load_day_manifest(&next_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&active).unwrap(), "open");
    assert_eq!(active["collection_day_coverage"]["state"], "active");
    assert_eq!(
        active["collection_day_coverage"]["reactivated_within_day"],
        false
    );
    assert_eq!(
        active["collection_day_coverage"]["first_started_utc_ms"],
        boundary_utc_ms
    );
    assert!(
        owner
            .record_gaps(day, &["late_old_day_gap".to_owned()])
            .is_err()
    );
}

#[test]
fn manifest_owner_unclean_rollover_preserves_reactivation() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, day_dir) = reactivated_manifest_owner(
        &dataset_root,
        &close.utc_day,
        boundary_utc_ms - 2_000,
        boundary_utc_ms - 1_000,
    );

    for producer in LOGICAL_PRODUCERS {
        assert!(
            owner
                .record_producer_barrier(
                    close.clone(),
                    (*producer).to_owned(),
                    &[],
                    &BTreeMap::new(),
                )
                .unwrap()
                .is_none()
        );
    }
    for (index, source_kind) in training_source_kinds().iter().enumerate() {
        let ready = owner
            .record_writer_ack(close.clone(), (*source_kind).to_owned())
            .unwrap();
        assert_eq!(ready.is_some(), index + 1 == training_source_kinds().len());
    }
    owner
        .commit_rollover_old_day(&close, ManifestRolloverCommitMode::UncleanOldDay)
        .unwrap();
    owner.finish_unclean_rollover(&close).unwrap();

    let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&manifest).unwrap(), "closed");
    assert_eq!(manifest["closure_reason"], "unclean_rollover");
    assert_eq!(
        manifest["collection_day_coverage"]["state"],
        "unclean_end_unknown"
    );
    assert!(manifest["collection_day_coverage"]["ended_utc_ms"].is_null());
    assert_eq!(
        manifest["collection_day_coverage"]["reactivated_within_day"],
        true
    );
    assert!(
        !typed_raw_root(&dataset_root)
            .join(format!("utc_day={}", close.next_utc_day))
            .exists()
    );
}

#[test]
fn manifest_owner_shutdown_dispositions_keep_reactivation_sticky() {
    let day = "2026-07-07";
    let started_utc_ms = 1_783_392_400_000;
    let ended_utc_ms = started_utc_ms + 1_000;
    for (disposition, expected_state, expected_end) in [
        (
            ManifestShutdownDisposition::EndedKnown { ended_utc_ms },
            "ended_known",
            Some(ended_utc_ms),
        ),
        (
            ManifestShutdownDisposition::UncleanEndUnknown,
            "unclean_end_unknown",
            None,
        ),
    ] {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (mut owner, day_dir) =
            reactivated_manifest_owner(&dataset_root, day, started_utc_ms, started_utc_ms + 500);
        let operational = BTreeMap::from([(
            day.to_owned(),
            BTreeMap::from([(
                "polymarket_market".to_owned(),
                SourceOperationalCounterDelta {
                    dropped_rows: 1,
                    ..SourceOperationalCounterDelta::default()
                },
            )]),
        )]);
        owner
            .shutdown(
                &RuntimeCloseIdentity {
                    utc_day: day.to_owned(),
                    generation: INITIAL_RAW_DAY_GENERATION,
                    close_id: 1,
                },
                &disposition,
                &["operator_shutdown".to_owned()],
                &operational,
            )
            .unwrap();

        let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_eq!(manifest_state(&manifest).unwrap(), "open");
        assert_eq!(manifest["gaps"][0], "operator_shutdown");
        assert_eq!(
            manifest["source_counters"]["polymarket_market"]["dropped_rows"],
            1
        );
        assert!(manifest.get("closure_reason").is_none());
        assert_eq!(manifest["collection_day_coverage"]["state"], expected_state);
        assert_eq!(
            manifest["collection_day_coverage"]["reactivated_within_day"],
            true
        );
        match expected_end {
            Some(expected_end) => assert_eq!(
                manifest["collection_day_coverage"]["ended_utc_ms"],
                expected_end
            ),
            None => assert!(manifest["collection_day_coverage"]["ended_utc_ms"].is_null()),
        }
    }
}

#[test]
fn rejected_shutdown_identity_preserves_manifest_digest() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let day = "2026-07-07";
    let (mut owner, day_dir) = test_manifest_owner(&dataset_root, day);
    let started_utc_ms = 1_783_392_400_000;
    owner
        .activate_initial(day, started_utc_ms, INITIAL_RAW_DAY_GENERATION)
        .unwrap();

    let rejected_operational = BTreeMap::from([(
        day.to_owned(),
        BTreeMap::from([(
            training_source_kinds()[0].to_owned(),
            SourceOperationalCounterDelta {
                dropped_rows: 1,
                ..SourceOperationalCounterDelta::default()
            },
        )]),
    )]);
    let digest_before = manifest_file_digest(&day_dir);
    let error = owner
        .shutdown(
            &RuntimeCloseIdentity {
                utc_day: day.to_owned(),
                generation: INITIAL_RAW_DAY_GENERATION + 1,
                close_id: 1,
            },
            &ManifestShutdownDisposition::EndedKnown {
                ended_utc_ms: started_utc_ms + 1_000,
            },
            &["rejected_shutdown_gap".to_owned()],
            &rejected_operational,
        )
        .unwrap_err();
    assert!(error.to_string().contains("active epoch mismatch"));
    assert_eq!(
        manifest_file_digest(&day_dir),
        digest_before,
        "a rejected shutdown identity must not change the disk manifest"
    );
}
