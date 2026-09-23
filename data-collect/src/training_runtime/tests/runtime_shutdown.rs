use super::*;

#[tokio::test]
async fn runtime_exit_watcher_aborts_its_task_when_the_owner_is_dropped() {
    let (task_dropped_tx, task_dropped_rx) = oneshot::channel::<()>();
    let watcher = RuntimeExitWatcher::new(tokio::spawn(async move {
        let _task_dropped_tx = task_dropped_tx;
        std::future::pending::<()>().await;
    }));

    drop(watcher);

    assert!(
        timeout(Duration::from_secs(1), task_dropped_rx)
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn failed_manifest_shutdown_aborts_a_blocked_owner_before_a_late_mutation() {
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
    let raw_day_gate = Arc::new(RawDayGate::new());
    raw_day_gate
        .activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    assert_eq!(
        raw_day_gate
            .close_for_rollover(&close.utc_day, close.boundary_utc_ms)
            .await
            .unwrap(),
        close
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
    for source_kind in &training_source_kinds()[..training_source_kinds().len() - 1] {
        assert!(
            owner
                .record_writer_ack(
                    close.clone(),
                    (*source_kind).to_owned(),
                    Arc::clone(&raw_day_gate)
                )
                .unwrap()
                .is_none()
        );
    }
    let digest_before_final_ack = manifest_file_digest(&day_dir);
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, mut rollover_ready_rx) = mpsc::channel(1);
    rollover_ready_tx
        .try_send(ManifestRolloverReady {
            close: close.clone(),
        })
        .unwrap();
    let mut owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    manifest_tx
        .send(ManifestCommand::WriterDayFinalized {
            close: close.clone(),
            source_kind: training_source_kinds()[training_source_kinds().len() - 1].to_owned(),
            raw_day_gate,
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(1), async {
        while manifest_file_digest(&day_dir) == digest_before_final_ack {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner must persist the complete rendezvous before blocking on ready send");
    let digest_before = manifest_file_digest(&day_dir);

    let (late_reply_tx, late_reply_rx) = oneshot::channel();
    manifest_tx
        .send(ManifestCommand::Shutdown {
            close: RuntimeCloseIdentity::from_rollover_old_day(&close),
            disposition: ManifestShutdownDisposition::EndedKnown {
                ended_utc_ms: close.boundary_utc_ms,
            },
            gaps: vec!["late_manifest_mutation".to_owned()],
            operational_counters: BTreeMap::new(),
            reply: late_reply_tx,
        })
        .await
        .unwrap();

    assert!(
        abort_manifest_owner_if_running(&mut owner_handle, Instant::now() + Duration::from_secs(1))
            .await
    );
    assert!(late_reply_rx.await.is_err());
    assert_eq!(manifest_file_digest(&day_dir), digest_before);
    assert_eq!(rollover_ready_rx.recv().await.unwrap().close, close);
}

#[tokio::test]
async fn normally_joined_manifest_owner_is_not_reclassified_as_aborted() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let day = "2026-07-07";
    let (mut owner, _) = test_manifest_owner(&dataset_root, day);
    let started_utc_ms = 1_783_392_400_000;
    owner
        .activate_initial(day, started_utc_ms, INITIAL_RAW_DAY_GENERATION)
        .unwrap();
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
    let mut owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let (reply_tx, reply_rx) = oneshot::channel();
    manifest_tx
        .send(ManifestCommand::Shutdown {
            close: RuntimeCloseIdentity {
                utc_day: day.to_owned(),
                generation: INITIAL_RAW_DAY_GENERATION,
                close_id: 1,
            },
            disposition: ManifestShutdownDisposition::EndedKnown {
                ended_utc_ms: started_utc_ms + 1_000,
            },
            gaps: Vec::new(),
            operational_counters: BTreeMap::new(),
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    timeout(Duration::from_secs(1), async {
        while !owner_handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(
        !abort_manifest_owner_if_running(
            &mut owner_handle,
            Instant::now() + Duration::from_secs(1),
        )
        .await
    );
    owner_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn post_spawn_failure_converges_all_owned_runtime_resources() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let dataset_lease = Arc::new(lock_training_dataset_root(&dataset_root).unwrap());
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let (mut writers, releases) = start_writer_workers_inner(
        true,
        &dataset_root,
        &test_artifact_identity(),
        &manifest_tx,
        &runtime_stats,
        None,
        None,
        Some(Arc::clone(&dataset_lease)),
        |_| Ok(()),
    )
    .unwrap();
    assert!(releases.is_empty());

    let gate = source_task_supervision::current_day_active_gate().await;
    let source = SourceTaskHandle::spawn_with_dataset_lease(
        "binance_aggtrade",
        std::future::pending::<Result<()>>(),
        SourceTaskObservationCell::default(),
        gate,
        Arc::clone(&dataset_lease),
    );
    let mut sources = vec![source];

    let owner_lease = Arc::clone(&dataset_lease);
    let mut manifest_owner = tokio::spawn(async move {
        let _dataset_lease = owner_lease;
        std::future::pending::<()>().await;
        Ok(())
    });
    let (watcher_dropped_tx, watcher_dropped_rx) = oneshot::channel::<()>();
    let mut watcher = RuntimeExitWatcher::new(tokio::spawn(async move {
        let _watcher_dropped_tx = watcher_dropped_tx;
        std::future::pending::<()>().await;
    }));

    converge_spawned_runtime_resources(
        &mut sources,
        &mut writers,
        &mut manifest_owner,
        &mut watcher,
        None,
        None,
        Instant::now() + Duration::from_secs(2),
    )
    .await;
    drop(dataset_lease);

    assert!(sources.is_empty());
    assert!(writers.is_empty());
    assert!(manifest_owner.is_finished());
    assert!(watcher.join.is_none());
    assert!(watcher_dropped_rx.await.is_err());
    assert!(lock_training_dataset_root(&dataset_root).is_ok());
}
