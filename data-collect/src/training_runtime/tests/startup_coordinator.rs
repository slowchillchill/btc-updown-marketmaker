use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_gate_collects_eleven_ready_before_activation_and_first_source_poll() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let started_utc_ms = 1_783_987_201_000;
    let utc_day = utc_day_string_from_ms(started_utc_ms).unwrap();
    let (owner, day_dir) = test_manifest_owner(&dataset_root, &utc_day);
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
    let manifest_owner_handle =
        tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    let (ready_rx, releases, producer_probes, writer_receivers) = startup_gate_fixture();

    for (polls, _) in &producer_probes {
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    activate_and_release_startup(
        &manifest_tx,
        Arc::clone(&raw_day_gate),
        ready_rx,
        releases,
        utc_day,
        ManifestActivationAckMode::Send,
        None,
        || started_utc_ms,
    )
    .await
    .unwrap();

    for receiver in writer_receivers {
        receiver.await.unwrap();
    }
    for (polls, handle) in producer_probes {
        handle.await.unwrap().unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
    let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest["collection_day_coverage"]["state"], "active");
    assert_eq!(
        manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    manifest_owner_handle.abort();
    let _ = manifest_owner_handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_startup_deadline_keeps_manifest_not_started_and_sources_unpolled() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let started_utc_ms = 1_783_987_201_000;
    let utc_day = utc_day_string_from_ms(started_utc_ms).unwrap();
    let (owner, day_dir) = test_manifest_owner(&dataset_root, &utc_day);
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
    let manifest_owner_handle =
        tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    let (ready_rx, releases, producer_probes, writer_receivers) = startup_gate_fixture();

    let error = activate_and_release_startup(
        &manifest_tx,
        Arc::clone(&raw_day_gate),
        ready_rx,
        releases,
        utc_day,
        ManifestActivationAckMode::Send,
        Some(RuntimeDeadlineCandidate {
            deadline: Instant::now() - Duration::from_millis(1),
            observed_utc_ms: started_utc_ms,
        }),
        || started_utc_ms,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("deadline"));
    for receiver in writer_receivers {
        assert!(receiver.await.is_err());
    }
    for (polls, handle) in producer_probes {
        assert!(handle.await.unwrap().is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest["collection_day_coverage"]["state"], "not_started");
    assert_eq!(
        manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    manifest_owner_handle.abort();
    let _ = manifest_owner_handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_activation_with_dropped_reply_is_not_retried_or_released() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let started_utc_ms = 1_783_987_201_000;
    let utc_day = utc_day_string_from_ms(started_utc_ms).unwrap();
    let (owner, day_dir) = test_manifest_owner(&dataset_root, &utc_day);
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
    let manifest_owner_handle =
        tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    let (ready_rx, releases, producer_probes, writer_receivers) = startup_gate_fixture();

    let error = activate_and_release_startup(
        &manifest_tx,
        Arc::clone(&raw_day_gate),
        ready_rx,
        releases,
        utc_day.clone(),
        ManifestActivationAckMode::Drop,
        None,
        || started_utc_ms,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("activation reply dropped"));

    for receiver in writer_receivers {
        assert!(receiver.await.is_err());
    }
    for (polls, handle) in producer_probes {
        assert!(handle.await.unwrap().is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest["collection_day_coverage"]["state"], "active");
    assert_eq!(
        manifest["collection_day_coverage"]["reactivated_within_day"], false,
        "a dropped activation reply must not cause a second mutation"
    );

    manifest_owner_handle.abort();
    let _ = manifest_owner_handle.await;

    let (mut recovered_owner, _) = test_manifest_owner(&dataset_root, &utc_day);
    recovered_owner
        .activate_initial(&utc_day, started_utc_ms + 1_000, INITIAL_RAW_DAY_GENERATION)
        .unwrap();
    let recovered_manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(
        recovered_manifest["collection_day_coverage"]["state"],
        "active"
    );
    assert_eq!(
        recovered_manifest["collection_day_coverage"]["reactivated_within_day"], true,
        "the next same-day owner activation records reactivation without retrying the dropped reply"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_wait_crossing_utc_day_refuses_activation_and_release() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recovery_started_utc_ms = 1_783_987_201_000;
    let utc_day = utc_day_string_from_ms(recovery_started_utc_ms).unwrap();
    let next_day_started_utc_ms = recovery_started_utc_ms.saturating_add(UTC_DAY_MS);
    assert_ne!(
        utc_day_string_from_ms(next_day_started_utc_ms).as_deref(),
        Some(utc_day.as_str())
    );
    let (owner, day_dir) = test_manifest_owner(&dataset_root, &utc_day);
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
    let manifest_owner_handle =
        tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    let (ready_rx, releases, producer_probes, writer_receivers) = startup_gate_fixture();

    let error = activate_and_release_startup(
        &manifest_tx,
        Arc::clone(&raw_day_gate),
        ready_rx,
        releases,
        utc_day,
        ManifestActivationAckMode::Send,
        None,
        || next_day_started_utc_ms,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("UTC day advanced"));

    for receiver in writer_receivers {
        assert!(receiver.await.is_err());
    }
    for (polls, handle) in producer_probes {
        assert!(handle.await.unwrap().is_err());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(manifest["collection_day_coverage"]["state"], "not_started");
    assert_eq!(
        manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    manifest_owner_handle.abort();
    let _ = manifest_owner_handle.await;
}
