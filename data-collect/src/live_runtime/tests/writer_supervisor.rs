use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_workers_report_ready_only_after_local_initialization() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("startup-ready-writers");
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (ready_tx, mut ready_rx) = mpsc::channel(STARTUP_READY_CHANNEL_CAPACITY);
    let (writers, releases) = start_writer_workers_gated(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &runtime_stats,
        &ready_tx,
        Arc::new(RawDayGate::new()),
        None,
    )
    .unwrap();
    drop(ready_tx);

    let mut ready = BTreeSet::new();
    for _ in 0..LIVE_SOURCES.len() {
        ready.insert(
            timeout(Duration::from_secs(1), ready_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(
        ready,
        LIVE_SOURCES
            .iter()
            .map(|source_kind| StartupParticipant::writer(source_kind))
            .collect()
    );
    for writer in writers.values() {
        assert!(matches!(
            writer.result_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
    }
    StartupReleaseHandles {
        writers: releases,
        producers: Vec::new(),
    }
    .release()
    .unwrap();
    let outcome = stop_writer_workers_with_budget(writers, Duration::from_secs(1)).await;
    assert!(outcome.all_terminal, "{:?}", outcome.errors);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_startup_stop_runs_writers_until_centralized_exact_shutdown() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("startup-stop-writers");
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (ready_tx, mut ready_rx) = mpsc::channel(STARTUP_READY_CHANNEL_CAPACITY);
    let (writers, releases) = start_writer_workers_gated(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &runtime_stats,
        &ready_tx,
        Arc::new(RawDayGate::new()),
        None,
    )
    .unwrap();
    drop(ready_tx);
    for _ in 0..LIVE_SOURCES.len() {
        timeout(Duration::from_secs(1), ready_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }

    let ended_utc_ms = 1_783_392_401_000;
    let close = PreparedSameDayClose {
        identity: RuntimeCloseIdentity {
            utc_day: utc_day_string_from_ms(ended_utc_ms).unwrap(),
            generation: INITIAL_RAW_DAY_GENERATION,
            close_id: 1,
        },
        ended_utc_ms,
        cause: RuntimeCloseCause::RunDeadline,
        unclean: false,
        unclean_cause: None,
    };
    StartupReleaseHandles {
        writers: releases,
        producers: Vec::new(),
    }
    .stop_for_exit(&PreparedRuntimeExit::SameDay(close.clone()))
    .unwrap();

    let outcome = shutdown_writer_workers_with_budget(
        writers,
        close.identity.clone(),
        Duration::from_secs(1),
    )
    .await;
    assert!(outcome.all_terminal, "{:?}", outcome.errors);
    assert!(outcome.all_close_proven, "{:?}", outcome.errors);
    assert!(
        outcome
            .outputs
            .iter()
            .all(|output| { output.coordinated_close.as_ref() == Some(&close.identity) })
    );
}

#[test]
fn typed_writer_closes_row_groups_at_exact_row_and_logical_byte_limits() {
    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade_00",
        typed_schema_version("binance_aggtrade"),
        "binance_aggtrade",
    )
    .unwrap();
    let row = test_aggtrade_hourly_row(1, 1_783_392_400_001);
    let row_group_rows = (1..=PARQUET_ROW_GROUP_MAX_ROWS)
        .map(|sequence| {
            let mut row = row.clone();
            row.websocket_ingress_sequence = Some(sequence);
            row.collector_admission_sequence = sequence;
            row
        })
        .collect::<Vec<_>>();
    writer
        .write_binance_aggtrade_rows_with_logical_bytes(&row_group_rows, PARQUET_ROW_GROUP_MAX_ROWS)
        .unwrap();
    for offset in 1..=17 {
        let mut next_row = row.clone();
        let sequence = PARQUET_ROW_GROUP_MAX_ROWS + offset;
        next_row.websocket_ingress_sequence = Some(sequence);
        next_row.collector_admission_sequence = sequence;
        writer
            .write_binance_aggtrade_rows_with_logical_bytes(
                std::slice::from_ref(&next_row),
                PARQUET_ROW_GROUP_MAX_BYTES / 16,
            )
            .unwrap();
    }

    let published = writer.finalize().unwrap();
    let reader = SerializedFileReader::new(File::open(published.final_path).unwrap()).unwrap();
    let row_groups = reader.metadata().row_groups();
    assert_eq!(row_groups.len(), 3);
    assert_eq!(row_groups[0].num_rows(), 50_000);
    assert_eq!(row_groups[1].num_rows(), 16);
    assert_eq!(row_groups[2].num_rows(), 1);
    assert!(
        !tmp.path()
            .join("binance_aggtrade_00.parquet.running")
            .exists()
    );
}

#[test]
fn live_segment_finalize_emits_daily_delta_without_segment_manifest() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let naming = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_aggtrade", 1_783_392_404_599, None)
        .unwrap();
    let (mut owner, day_dir) = reactivated_manifest_owner(
        &dataset_root,
        "2026-07-07",
        1_783_392_400_000,
        1_783_392_401_000,
    );
    let active_coverage = manifest_coverage_value(&day_dir);
    let mut segment = ActiveSegment::new(naming.clone(), "binance_aggtrade", 0).unwrap();
    let mut event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
    event.collector_admission = Some(RawEventAdmission {
        sequence: 1,
        time_utc_ms: event.recv_time_utc_ms,
    });

    assert_eq!(segment.writer.write_event(&event).unwrap(), 1);
    segment.observe(&event, 1);
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let finalized = finalize_segment(
        segment,
        "shutdown_finalize",
        &test_runtime_provenance("run"),
        &manifest_tx,
    )
    .unwrap();
    let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
        panic!("expected finalized-file delta");
    };
    assert_eq!(delta.final_path, finalized.parquet_path);
    assert_eq!(delta.utc_day, "2026-07-07");
    assert_eq!(delta.utc_hour, "2026-07-07T02");
    assert_eq!(
        delta.facts["hour_window_utc_ms"]["start"],
        1_783_389_600_000i64
    );
    assert_eq!(
        delta.facts["hour_window_utc_ms"]["end"],
        1_783_393_200_000i64
    );
    assert_eq!(delta.part, 0);
    assert_eq!(
        delta.facts["layout_family"],
        "source_specific_hourly_typed_raw"
    );
    assert_eq!(delta.facts["sort_order"], "collector_admission_order");
    assert_eq!(delta.facts["compression"], "zstd_level_3");
    assert!(delta.facts.get("required_typed_fields_present").is_none());
    assert_eq!(delta.facts["backfill_rows"], 0);
    assert_eq!(delta.event_type_counts["aggTrade"], 1);
    assert!(fs::read_dir(&naming.partition_path).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".manifest.json")
    }));

    owner
        .record_operational_counters(
            "2026-07-07",
            &BTreeMap::from([(
                "binance_aggtrade".to_owned(),
                SourceOperationalCounterDelta {
                    structural_dedupe_dropped_events: 1,
                    ..SourceOperationalCounterDelta::default()
                },
            )]),
        )
        .unwrap();
    assert_eq!(manifest_coverage_value(&day_dir), active_coverage);
    owner.apply_finalized(delta).unwrap();
    assert_eq!(manifest_coverage_value(&day_dir), active_coverage);
    let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
    assert_eq!(
        manifest["source_counters"]["binance_aggtrade"]["written_rows"],
        1
    );
    assert_eq!(
        manifest["source_counters"]["binance_aggtrade"]["backfill_rows"],
        0
    );
    assert_eq!(
        manifest["source_counters"]["binance_aggtrade"]["structural_dedupe_dropped_events"],
        1
    );
    let reconciled =
        reconcile_open_day_manifest(&day_dir, "2026-07-07", &test_daily_manifest_identity())
            .unwrap();
    assert_eq!(reconciled["files"].as_array().unwrap().len(), 1);
    assert_eq!(reconciled["collection_day_coverage"], active_coverage);

    let reader = SerializedFileReader::new(File::open(&finalized.parquet_path).unwrap()).unwrap();
    let sort_order = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|metadata| metadata.iter().find(|entry| entry.key == "sort_order"))
        .and_then(|entry| entry.value.as_deref());
    assert_eq!(sort_order, Some("collector_admission_order"));
    let fields = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect::<Vec<_>>();
    assert!(fields.contains(&"aggregate_trade_id".to_owned()));
    assert!(!fields.contains(&"raw_json".to_owned()));
}

#[test]
fn polymarket_metadata_sort_order_matches_parquet_and_embedded_recovery_facts_by_version() {
    use crate::polymarket_market_metadata_layout::{
        POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION, POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
        POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
    };

    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let naming = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("polymarket_market_metadata", 1_783_392_404_599, None)
        .unwrap();
    let segment = ActiveSegment::new(naming, "polymarket_market_metadata", 0).unwrap();
    let context = SegmentManifestContext::from_segment(&segment);
    let provenance = test_runtime_provenance("sort-order");

    for (index, (schema_version, expected_sort_order)) in [
        (
            POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION,
            "receive_order",
        ),
        (
            POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
            "collector_admission_order",
        ),
        (
            POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
            "collector_admission_order",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let facts = segment_daily_facts(&context, schema_version, "test_finalize", &provenance);
        assert_eq!(facts["sort_order"], expected_sort_order);
        let recovery_metadata = DailyFileRecoveryMetadata {
            source_kind: context.source_kind.clone(),
            utc_hour: context.utc_hour.clone(),
            part: context.hour_part,
            rows: 0,
            schema_version: schema_version.to_owned(),
            first_recv_time_utc_ms: 0,
            last_recv_time_utc_ms: 0,
            source_timestamp_start: None,
            source_timestamp_end: None,
            event_type_counts: BTreeMap::new(),
            facts,
        };
        if schema_version == POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION {
            continue;
        }
        let writer_root = dataset_root.join(format!("version-{index}"));
        fs::create_dir(&writer_root).unwrap();
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            &writer_root,
            "metadata-sort-order",
            schema_version,
            "polymarket_market_metadata",
        )
        .unwrap();
        writer
            .append_key_value_metadata(
                DAILY_FILE_RECOVERY_METADATA_KEY,
                serde_json::to_string(&recovery_metadata).unwrap(),
            )
            .unwrap();
        let published = writer.finalize().unwrap();
        let reader = SerializedFileReader::new(File::open(&published.final_path).unwrap()).unwrap();
        let metadata = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        let parquet_sort_order = metadata
            .iter()
            .find(|entry| entry.key == "sort_order")
            .and_then(|entry| entry.value.as_deref());
        let embedded = metadata
            .iter()
            .find(|entry| entry.key == DAILY_FILE_RECOVERY_METADATA_KEY)
            .and_then(|entry| entry.value.as_deref())
            .map(|payload| serde_json::from_str::<DailyFileRecoveryMetadata>(payload).unwrap())
            .unwrap();
        assert_eq!(parquet_sort_order, Some(expected_sort_order));
        assert_eq!(embedded.schema_version, schema_version);
        assert_eq!(embedded.facts["sort_order"], expected_sort_order);
    }
}

#[test]
fn aggtrade_segment_receive_bounds_allow_admission_order_time_regression() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let later = 1_783_392_404_600;
    let earlier = later - 1;
    let naming = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_aggtrade", later, None)
        .unwrap();
    let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();

    segment.observe(&test_raw_event("binance_aggtrade", later), 1);
    segment.observe(&test_raw_event("binance_aggtrade", earlier), 1);

    let context = SegmentManifestContext::from_segment(&segment);
    assert_eq!(context.segment_start_recv_time_utc_ms, earlier);
    assert_eq!(context.last_recv_time_utc_ms, later);
}

#[test]
fn live_runtime_keeps_same_hour_file_at_former_max_bytes() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let naming = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_aggtrade", 1_783_392_404_599, None)
        .unwrap();
    let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();
    let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
    segment.observe(&event, 1);
    segment.bytes = 128 * 1024 * 1024;

    assert_eq!(
        live_segment_rollover_reason(&segment, "2026-07-07T02"),
        None
    );
}

#[test]
fn live_runtime_rolls_segment_on_utc_hour_boundary() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let naming = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_aggtrade", 1_783_392_404_599, None)
        .unwrap();
    let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();
    let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
    segment.observe(&event, 1);

    assert_eq!(
        live_segment_rollover_reason(&segment, "2026-07-07T03"),
        Some("utc_hour_boundary")
    );
}

#[test]
fn live_runtime_uses_next_part_when_same_hour_file_already_exists() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let policy = HourlyPartitionPolicy::new(&dataset_root).unwrap();
    let base = policy
        .partition("binance_aggtrade", 1_783_392_404_599, None)
        .unwrap();
    fs::create_dir_all(&base.partition_path).unwrap();
    fs::write(&base.final_path, b"existing finalized parquet").unwrap();

    let (next, hour_part) =
        next_available_hourly_partition(&policy, "binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();

    assert_eq!(hour_part, 1);
    assert!(next.final_path.ends_with(
        "training/typed_raw/utc_day=2026-07-07/source_kind=binance_aggtrade/binance_aggtrade_02-part-1.parquet"
    ));
}

#[test]
fn source_writer_output_uses_aggregated_stats_without_per_event_receipts() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("stats-run");
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let queue_rows_for_worker = Arc::clone(&queue_rows);
    let dataset_root_for_worker = dataset_root.clone();
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            provenance,
            "binance_aggtrade".to_owned(),
            rx,
            queue_rows_for_worker,
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    let events = vec![
        InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_599)),
        InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_600)),
    ];
    queue_rows.store(events.len() as u64, Ordering::Relaxed);
    tx.send(WriterCommand::Events(events)).unwrap();
    drop(tx);

    let output = handle.join().unwrap().unwrap();
    assert_eq!(output.source_kind, "binance_aggtrade");
    assert_eq!(output.stats.source_kind, "binance_aggtrade");
    assert_eq!(output.stats.events_received, 2);
    assert_eq!(
        output.stats.events_received,
        output
            .stats
            .events_written
            .saturating_add(output.stats.events_dropped)
    );
    let finalized_rows = output
        .finalized_segments
        .iter()
        .map(|segment| segment.row_count)
        .sum::<u64>();
    assert_eq!(output.stats.typed_rows_written, finalized_rows);
    assert_eq!(
        output.stats.write_events_us.count,
        output.stats.events_received
    );

    let artifact_dir = dataset_root.join("training/runtime_artifacts/stats-run");
    if artifact_dir.exists() {
        assert!(
            std::fs::read_dir(&artifact_dir).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("retention_after_finalize_")),
            "writer finalization must not emit source-specific retention receipts"
        );
    }
}

fn completed_writer_with_close(
    source_kind: &str,
    close: &RuntimeCloseIdentity,
) -> SourceWriterHandle {
    let (tx, _rx) = std::sync::mpsc::sync_channel(1);
    let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    SourceWriterHandle {
        schema_version: typed_schema_version(source_kind).to_owned(),
        tx,
        queue_rows: Arc::new(AtomicU64::new(0)),
        accepting_commands: Arc::new(Mutex::new(true)),
        result_rx,
        completed_result: Some(WriterTaskReport {
            source_kind: source_kind.to_owned(),
            first_observed_utc_ms: now_ms(),
            worker_epoch: Some(RuntimeEpoch {
                utc_day: close.utc_day.clone(),
                generation: close.generation,
            }),
            lifecycle_epoch: Some(RuntimeEpoch {
                utc_day: close.utc_day.clone(),
                generation: close.generation,
            }),
            termination_kind: WriterTerminationKind::CoordinatedClose,
            lifecycle_close: Some(close.clone()),
            lifecycle_record_error: None,
            outcome: WriterTaskOutcome::Returned(Ok(SourceWriterOutput {
                source_kind: source_kind.to_owned(),
                finalized_segments: Vec::new(),
                stats: SourceWriterStats::empty(source_kind.to_owned()),
                coordinated_close: Some(close.clone()),
                observed_utc_ms: now_ms(),
            })),
        }),
        thread: Some(std::thread::spawn(|| {})),
    }
}

fn completed_mandatory_writers(
    close: &RuntimeCloseIdentity,
) -> BTreeMap<String, SourceWriterHandle> {
    LIVE_SOURCES
        .iter()
        .map(|source_kind| {
            (
                (*source_kind).to_owned(),
                completed_writer_with_close(source_kind, close),
            )
        })
        .collect()
}

#[tokio::test]
async fn writer_shutdown_requires_exact_mandatory_keys_and_report_proofs() {
    let close = RuntimeCloseIdentity {
        utc_day: "2026-07-17".to_owned(),
        generation: 3,
        close_id: 42,
    };

    let empty = shutdown_writer_workers_with_budget(
        BTreeMap::new(),
        close.clone(),
        Duration::from_millis(10),
    )
    .await;
    assert!(empty.all_terminal);
    assert!(!empty.all_close_proven);
    assert!(
        empty
            .errors
            .iter()
            .any(|error| error.contains("writer mandatory key mismatch"))
    );

    let mut missing_writers = completed_mandatory_writers(&close);
    let missing_source = LIVE_SOURCES[0];
    missing_writers.remove(missing_source);
    let missing = shutdown_writer_workers_with_budget(
        missing_writers,
        close.clone(),
        Duration::from_millis(10),
    )
    .await;
    assert!(missing.all_terminal);
    assert!(!missing.all_close_proven);
    assert!(
        missing
            .errors
            .iter()
            .any(|error| error.contains("writer mandatory key mismatch")
                && error.contains(missing_source))
    );

    let mut extra_writers = completed_mandatory_writers(&close);
    extra_writers.insert(
        "unexpected_writer".to_owned(),
        completed_writer_with_close("unexpected_writer", &close),
    );
    let extra = shutdown_writer_workers_with_budget(
        extra_writers,
        close.clone(),
        Duration::from_millis(10),
    )
    .await;
    assert!(extra.all_terminal);
    assert!(!extra.all_close_proven);
    assert!(
        extra
            .errors
            .iter()
            .any(|error| error.contains("writer mandatory key mismatch")
                && error.contains("unexpected_writer"))
    );

    let exact = shutdown_writer_workers_with_budget(
        completed_mandatory_writers(&close),
        close.clone(),
        Duration::from_millis(10),
    )
    .await;
    assert!(exact.all_terminal, "{:?}", exact.errors);
    assert!(exact.all_close_proven, "{:?}", exact.errors);
    assert!(exact.errors.is_empty());

    let mut missing_report_writers = completed_mandatory_writers(&close);
    missing_report_writers
        .get_mut(LIVE_SOURCES[0])
        .unwrap()
        .completed_result
        .as_mut()
        .unwrap()
        .outcome = WriterTaskOutcome::MissingReport("report channel dropped".to_owned());
    let missing_report = shutdown_writer_workers_with_budget(
        missing_report_writers,
        close,
        Duration::from_millis(10),
    )
    .await;
    assert!(missing_report.all_terminal);
    assert!(!missing_report.all_close_proven);
    assert!(
        missing_report
            .errors
            .iter()
            .any(|error| error.contains("writer report missing"))
    );
}

#[tokio::test]
async fn matching_epoch_reliable_writer_return_is_a_known_close_proof() {
    let close = RuntimeCloseIdentity {
        utc_day: "2026-07-17".to_owned(),
        generation: 3,
        close_id: 43,
    };
    let mut writers = completed_mandatory_writers(&close);
    let report = writers
        .get_mut("binance_aggtrade")
        .unwrap()
        .completed_result
        .as_mut()
        .unwrap();
    report.worker_epoch = None;
    report.lifecycle_close = None;
    report.termination_kind = WriterTerminationKind::ReliableReturn;
    report.outcome = WriterTaskOutcome::Returned(Err(io::Error::other(
        "writer returned a reliable terminal error",
    )
    .into()));

    let outcome =
        shutdown_writer_workers_with_budget(writers, close.clone(), Duration::from_millis(100))
            .await;

    assert!(outcome.all_terminal, "{:?}", outcome.errors);
    assert!(outcome.all_close_proven, "{:?}", outcome.errors);
    assert!(
        outcome
            .errors
            .iter()
            .any(|error| error.contains("reliable terminal error"))
    );

    let mut wrong_epoch = completed_mandatory_writers(&close);
    let report = wrong_epoch
        .get_mut("binance_aggtrade")
        .unwrap()
        .completed_result
        .as_mut()
        .unwrap();
    report.worker_epoch = None;
    report.lifecycle_close = None;
    report.lifecycle_epoch = Some(RuntimeEpoch {
        utc_day: close.utc_day.clone(),
        generation: close.generation + 1,
    });
    report.termination_kind = WriterTerminationKind::ReliableReturn;
    report.outcome = WriterTaskOutcome::Returned(Err(io::Error::other(
        "writer returned in the wrong epoch",
    )
    .into()));

    let wrong_epoch =
        shutdown_writer_workers_with_budget(wrong_epoch, close, Duration::from_millis(100)).await;
    assert!(wrong_epoch.all_terminal);
    assert!(!wrong_epoch.all_close_proven);
    assert!(
        wrong_epoch
            .errors
            .iter()
            .any(|error| error.contains("reliable-return proof mismatch"))
    );
}

#[tokio::test]
async fn writer_report_is_not_terminal_until_the_os_thread_is_joined() {
    let (tx, _command_rx) = std::sync::mpsc::sync_channel(1);
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let thread_exited = Arc::new(AtomicBool::new(false));
    let thread_exited_after_release = Arc::clone(&thread_exited);
    let thread = std::thread::spawn(move || {
        result_tx
            .send(WriterTaskReport {
                source_kind: "binance_aggtrade".to_owned(),
                first_observed_utc_ms: now_ms(),
                worker_epoch: None,
                lifecycle_epoch: None,
                termination_kind: WriterTerminationKind::ReliableReturn,
                lifecycle_close: None,
                lifecycle_record_error: None,
                outcome: WriterTaskOutcome::Returned(Ok(SourceWriterOutput {
                    source_kind: "binance_aggtrade".to_owned(),
                    finalized_segments: Vec::new(),
                    stats: SourceWriterStats::empty("binance_aggtrade".to_owned()),
                    coordinated_close: None,
                    observed_utc_ms: now_ms(),
                })),
            })
            .unwrap();
        release_rx.recv().unwrap();
        thread_exited_after_release.store(true, Ordering::SeqCst);
    });
    let writers = BTreeMap::from([(
        "binance_aggtrade".to_owned(),
        SourceWriterHandle {
            schema_version: typed_schema_version("binance_aggtrade").to_owned(),
            tx,
            queue_rows: Arc::new(AtomicU64::new(0)),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: Some(thread),
        },
    )]);
    let release = tokio::spawn(async move {
        sleep(Duration::from_millis(25)).await;
        release_tx.send(()).unwrap();
    });

    let outcome = stop_writer_workers_with_budget(writers, Duration::from_secs(1)).await;

    assert!(outcome.all_terminal, "{:?}", outcome.errors);
    assert!(thread_exited.load(Ordering::SeqCst));
    release.await.unwrap();
}

#[test]
fn partial_writer_spawn_failure_joins_started_workers_and_releases_the_lease() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let dataset_lease = Arc::new(lock_training_dataset_root(&dataset_root).unwrap());
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let (ready_tx, _ready_rx) = mpsc::channel(STARTUP_READY_CHANNEL_CAPACITY);

    let result = start_writer_workers_inner(
        &dataset_root,
        &test_runtime_provenance("partial-writer-spawn"),
        &manifest_tx,
        &runtime_stats,
        Some(&ready_tx),
        Some(Arc::new(RawDayGate::new())),
        Some(Arc::clone(&dataset_lease)),
        |index| {
            if index == 2 {
                Err(io::Error::other("injected writer spawn failure"))
            } else {
                Ok(())
            }
        },
    );
    let error = match result {
        Ok(_) => panic!("injected writer spawn failure unexpectedly succeeded"),
        Err(error) => error,
    };
    drop(dataset_lease);

    assert!(error.to_string().contains("injected writer spawn failure"));
    assert!(lock_training_dataset_root(&dataset_root).is_ok());
}

#[tokio::test]
async fn writer_finalize_timeout_returns_error_without_waiting_for_worker() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("hung-writer-run");
    let writers = start_writer_workers_with_hung_result(&dataset_root, &provenance, None).unwrap();

    let outcome = stop_writer_workers_with_budget(writers, Duration::from_millis(10)).await;

    assert!(!outcome.all_terminal);
    assert!(
        outcome
            .errors
            .iter()
            .any(|error| error.contains("writer finalize timeout")),
        "expected timeout error, got {:?}",
        outcome.errors
    );
}

#[tokio::test]
async fn hung_writer_retains_dataset_lease_until_the_process_exits() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("hung-writer-lease");
    let dataset_lease = Arc::new(lock_training_dataset_root(&dataset_root).unwrap());
    let writers = start_writer_workers_with_hung_result(
        &dataset_root,
        &provenance,
        Some(Arc::clone(&dataset_lease)),
    )
    .unwrap();
    drop(dataset_lease);

    let outcome = stop_writer_workers_with_budget(writers, Duration::from_millis(10)).await;

    assert!(!outcome.all_terminal);
    let error = lock_training_dataset_root(&dataset_root).unwrap_err();
    assert!(error.to_string().contains("training_dataset_root_locked"));
}

#[tokio::test]
async fn six_writers_return_the_exact_coordinated_close_identity() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("writer-close-proof");
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let writers =
        start_writer_workers(&dataset_root, &provenance, &manifest_tx, &runtime_stats).unwrap();
    let close = RuntimeCloseIdentity {
        utc_day: utc_day_string_from_ms(now_ms()).unwrap(),
        generation: INITIAL_RAW_DAY_GENERATION,
        close_id: 41,
    };

    let outcome =
        shutdown_writer_workers_with_budget(writers, close.clone(), Duration::from_secs(2)).await;

    assert!(outcome.all_terminal, "{:?}", outcome.errors);
    assert!(outcome.all_close_proven, "{:?}", outcome.errors);
    assert_eq!(outcome.outputs.len(), LIVE_SOURCES.len());
    assert!(outcome.outputs.iter().all(|output| {
        output.coordinated_close.as_ref() == Some(&close) && output.observed_utc_ms > 0
    }));
}

#[tokio::test]
async fn production_writer_report_preserves_reliable_terminal_lifecycle_epoch() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = Arc::new(RawDayGate::new());
    let utc_day = utc_day_string_from_ms(now_ms()).unwrap();
    gate.activate_startup(utc_day.clone(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let (mut writers, releases) = start_writer_workers_inner(
        &dataset_root,
        &test_runtime_provenance("writer-reliable-lifecycle"),
        &manifest_tx,
        &runtime_stats,
        None,
        Some(gate),
        None,
        |_| Ok(()),
    )
    .unwrap();
    assert!(releases.is_empty());
    let writer = writers.remove("binance_aggtrade").unwrap();
    let SourceWriterHandle {
        schema_version: _,
        tx,
        queue_rows: _,
        accepting_commands: _,
        result_rx,
        completed_result,
        thread,
    } = writer;
    assert!(completed_result.is_none());
    drop(tx);
    let report = result_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let thread = thread.unwrap();
    while !thread.is_finished() {
        std::thread::yield_now();
    }
    thread.join().unwrap();

    assert_eq!(
        report.termination_kind,
        WriterTerminationKind::ReliableReturn
    );
    assert_eq!(
        report.lifecycle_epoch,
        Some(RuntimeEpoch {
            utc_day,
            generation: INITIAL_RAW_DAY_GENERATION,
        })
    );
    assert!(report.worker_epoch.is_none());
    assert!(report.lifecycle_close.is_none());
    assert!(report.lifecycle_record_error.is_none());

    let remaining = stop_writer_workers_with_budget(writers, Duration::from_secs(1)).await;
    assert!(remaining.all_terminal, "{:?}", remaining.errors);
}

#[tokio::test]
async fn writer_close_proof_rejects_a_precompleted_or_wrong_identity_result() {
    let (tx, _rx) = std::sync::mpsc::sync_channel(1);
    let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let expected = RuntimeCloseIdentity {
        utc_day: "2026-07-17".to_owned(),
        generation: 3,
        close_id: 9,
    };
    let wrong = RuntimeCloseIdentity {
        close_id: 10,
        ..expected.clone()
    };
    let writers = BTreeMap::from([(
        "binance_aggtrade".to_owned(),
        SourceWriterHandle {
            schema_version: typed_schema_version("binance_aggtrade").to_owned(),
            tx,
            queue_rows: Arc::new(AtomicU64::new(0)),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: Some(WriterTaskReport {
                source_kind: "binance_aggtrade".to_owned(),
                first_observed_utc_ms: now_ms(),
                worker_epoch: Some(RuntimeEpoch {
                    utc_day: wrong.utc_day.clone(),
                    generation: wrong.generation,
                }),
                lifecycle_epoch: Some(RuntimeEpoch {
                    utc_day: wrong.utc_day.clone(),
                    generation: wrong.generation,
                }),
                termination_kind: WriterTerminationKind::CoordinatedClose,
                lifecycle_close: Some(wrong.clone()),
                lifecycle_record_error: None,
                outcome: WriterTaskOutcome::Returned(Ok(SourceWriterOutput {
                    source_kind: "binance_aggtrade".to_owned(),
                    finalized_segments: Vec::new(),
                    stats: SourceWriterStats::empty("binance_aggtrade".to_owned()),
                    coordinated_close: Some(wrong),
                    observed_utc_ms: now_ms(),
                })),
            }),
            thread: Some(std::thread::spawn(|| {})),
        },
    )]);

    let outcome =
        shutdown_writer_workers_with_budget(writers, expected, Duration::from_millis(100)).await;

    assert!(outcome.all_terminal);
    assert!(!outcome.all_close_proven);
    assert!(
        outcome
            .errors
            .iter()
            .any(|error| error.contains("terminal proof mismatch"))
    );
}

#[tokio::test]
async fn writer_stop_collects_late_terminal_state_after_another_writer_failed() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let (failed_tx, _failed_rx) = std::sync::mpsc::sync_channel(1);
    let (failed_result_tx, failed_result_rx) = std::sync::mpsc::sync_channel(1);
    drop(failed_result_tx);
    let (late_tx, _late_rx) = std::sync::mpsc::sync_channel(1);
    let (late_result_tx, late_result_rx) = std::sync::mpsc::sync_channel(1);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let late_stats = Arc::clone(&runtime_stats);
    let late_worker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(25));
        late_stats.record_operational_drop("binance_depth_update", 1_783_392_404_599, 2);
        late_result_tx
            .send(WriterTaskReport {
                source_kind: "binance_depth_update".to_owned(),
                first_observed_utc_ms: now_ms(),
                worker_epoch: None,
                lifecycle_epoch: None,
                termination_kind: WriterTerminationKind::ReliableReturn,
                lifecycle_close: None,
                lifecycle_record_error: None,
                outcome: WriterTaskOutcome::Returned(Ok(SourceWriterOutput {
                    source_kind: "binance_depth_update".to_owned(),
                    finalized_segments: Vec::new(),
                    stats: SourceWriterStats::empty("binance_depth_update".to_owned()),
                    coordinated_close: None,
                    observed_utc_ms: now_ms(),
                })),
            })
            .unwrap();
    });
    let writers = BTreeMap::from([
        (
            "binance_aggtrade".to_owned(),
            SourceWriterHandle {
                schema_version: typed_schema_version("binance_aggtrade").to_owned(),
                tx: failed_tx,
                queue_rows: Arc::new(AtomicU64::new(0)),
                accepting_commands: Arc::new(Mutex::new(true)),
                result_rx: failed_result_rx,
                completed_result: Some(WriterTaskReport {
                    source_kind: "binance_aggtrade".to_owned(),
                    first_observed_utc_ms: now_ms(),
                    worker_epoch: None,
                    lifecycle_epoch: None,
                    termination_kind: WriterTerminationKind::ReliableReturn,
                    lifecycle_close: None,
                    lifecycle_record_error: None,
                    outcome: WriterTaskOutcome::Returned(Err(io::Error::other(
                        "first writer failed",
                    )
                    .into())),
                }),
                thread: Some(std::thread::spawn(|| {})),
            },
        ),
        (
            "binance_depth_update".to_owned(),
            SourceWriterHandle {
                schema_version: typed_schema_version("binance_depth_update").to_owned(),
                tx: late_tx,
                queue_rows: Arc::new(AtomicU64::new(0)),
                accepting_commands: Arc::new(Mutex::new(true)),
                result_rx: late_result_rx,
                completed_result: None,
                thread: Some(late_worker),
            },
        ),
    ]);

    let outcome = stop_writer_workers_with_budget(writers, Duration::from_secs(1)).await;

    assert!(outcome.all_terminal);
    assert_eq!(outcome.outputs.len(), 1);
    assert_eq!(outcome.outputs[0].source_kind, "binance_depth_update");
    assert_eq!(outcome.errors.len(), 1);
    assert!(outcome.errors[0].contains("binance_aggtrade writer failed"));
    let counters = runtime_stats.drain_all_operational();
    assert_eq!(
        counters["2026-07-07"]["binance_depth_update"].dropped_rows,
        2
    );
    let mut owner = ManifestOwnerState::new(dataset_root.clone(), test_daily_manifest_identity());
    owner.ensure_open("2026-07-07").unwrap();
    owner
        .record_operational_counters("2026-07-07", &counters["2026-07-07"])
        .unwrap();
    let manifest = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-07"))
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest["source_counters"]["binance_depth_update"]["dropped_rows"],
        2
    );
}

#[tokio::test]
async fn polymarket_price_change_stats_count_one_canonical_v3_message_row() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("polymarket-stats-run");
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let queue_rows_for_worker = Arc::clone(&queue_rows);
    let dataset_root_for_worker = dataset_root.clone();
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            provenance,
            "polymarket_market".to_owned(),
            rx,
            queue_rows_for_worker,
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    let recv_time_utc_ms = 1_783_392_404_600;
    let event = test_polymarket_price_change_event(recv_time_utc_ms);
    let sdk_message = serde_json::from_str::<WsMessage>(&event.raw_json).unwrap();
    let message = PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
        .unwrap()
        .unwrap()
        .with_slug(Some(event.source_identifier.clone()))
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: recv_time_utc_ms,
            collector_host_id: event.collector_host_id,
            collector_region: event.collector_region,
            source_endpoint: event.source_endpoint,
            source_sequence_epoch_id: "polymarket-stats-source-epoch".to_owned(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: recv_time_utc_ms + 1,
        })
        .unwrap();
    let budget = InflightBudget::new();
    let row_reservation = budget.acquire(1, 0, BUFFER_ACQUIRE_TIMEOUT).await.unwrap();
    let byte_reservation = budget
        .acquire(0, message.owned_bytes(), BUFFER_ACQUIRE_TIMEOUT)
        .await
        .unwrap();
    queue_rows.store(1, Ordering::Relaxed);
    tx.send(WriterCommand::PolymarketOrderbookV3(vec![
        InflightPolymarketOrderbookV3 {
            message,
            raw_day_permit: RawDayPermit {
                utc_day: utc_day_string_from_ms(recv_time_utc_ms).unwrap(),
                generation: INITIAL_RAW_DAY_GENERATION,
            },
            _source_recv_monotonic_ms: 0,
            _row_reservation: row_reservation,
            _byte_reservation: byte_reservation,
        },
    ]))
    .unwrap();
    drop(tx);

    let output = handle.join().unwrap().unwrap();
    assert_eq!(output.stats.events_received, 1);
    assert_eq!(output.stats.events_written, 1);
    assert_eq!(output.stats.typed_rows_written, 1);
    assert_eq!(output.stats.finalized_segment_rows, 1);

    let ManifestCommand::Finalized(delta) = manifest_rx.recv().await.unwrap() else {
        panic!("expected finalized-file delta");
    };
    assert_eq!(delta.rows, 1);
    assert_eq!(delta.event_type_counts["price_change"], 1);
}

#[test]
fn binance_depth_manifest_carries_compact_stitch_evidence() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("depth-stitch-run");
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let queue_rows_for_worker = Arc::clone(&queue_rows);
    let dataset_root_for_worker = dataset_root.clone();
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            provenance,
            "binance_depth_update".to_owned(),
            rx,
            queue_rows_for_worker,
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    let event = test_binance_depth_update_event(1_783_392_404_600);
    queue_rows.store(1, Ordering::Relaxed);
    tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
        event,
    )]))
    .unwrap();
    drop(tx);

    let output = handle.join().unwrap().unwrap();
    assert_eq!(output.stats.typed_rows_written, 1);
    let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
        panic!("expected finalized-file delta");
    };
    assert_eq!(delta.event_type_counts["depth_update"], 1);
    assert_eq!(
        delta.facts["binance_depth_stitch"]["snapshot_last_update_id"],
        100
    );
    assert_eq!(
        delta.facts["binance_depth_stitch"]["stitch_applied_rows"],
        1
    );
    assert_eq!(delta.facts["binance_depth_stitch"]["gap_rows"], 0);
    assert_eq!(delta.facts["binance_depth_stitch"]["resnapshot_count"], 0);
    assert_eq!(delta.facts["binance_depth_stitch"]["local_u_min"], 103);
    assert_eq!(delta.facts["binance_depth_stitch"]["local_u_max"], 103);
    assert_eq!(
        delta.facts["binance_depth_stitch"]["first_update_bridged_snapshot"],
        true
    );
    assert_eq!(
        utc_day_string_from_ms(1_783_392_404_600).as_deref(),
        Some("2026-07-07")
    );
}

#[test]
fn sparse_depth_snapshot_hour_finalize_closes_without_a_later_event() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recv_time_utc_ms = 1_783_392_404_600;
    let partition = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_depth_snapshot", recv_time_utc_ms, None)
        .unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(1));
    let queue_rows_for_worker = Arc::clone(&queue_rows);
    let dataset_root_for_worker = dataset_root.clone();
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            test_runtime_provenance("sparse-snapshot-hour-finalize"),
            "binance_depth_snapshot".to_owned(),
            rx,
            queue_rows_for_worker,
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
        test_binance_depth_snapshot_event(recv_time_utc_ms, "bootstrap"),
    )]))
    .unwrap();
    tx.send(WriterCommand::FinalizeHour {
        utc_hour: partition.utc_hour.clone(),
    })
    .unwrap();
    drop(tx);

    let output = handle.join().unwrap().unwrap();
    assert_eq!(output.finalized_segments.len(), 1);
    let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
        panic!("expected finalized-file delta");
    };
    assert_eq!(delta.rows, 1);
    assert_eq!(delta.part, 0);
    assert_eq!(delta.facts["rollover_reason"], "utc_hour_boundary");
    assert!(partition.final_path.is_file());
    assert!(!partition.running_path.exists());
    assert!(
        !HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_depth_snapshot", recv_time_utc_ms, Some(1))
            .unwrap()
            .final_path
            .exists()
    );
}

#[test]
fn empty_depth_snapshot_hour_finalize_does_not_create_a_file() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recv_time_utc_ms = 1_783_392_404_600;
    let partition = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_depth_snapshot", recv_time_utc_ms, None)
        .unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let dataset_root_for_worker = dataset_root.clone();
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            test_runtime_provenance("empty-snapshot-hour-finalize"),
            "binance_depth_snapshot".to_owned(),
            rx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    tx.send(WriterCommand::FinalizeHour {
        utc_hour: partition.utc_hour,
    })
    .unwrap();
    drop(tx);

    let output = handle.join().unwrap().unwrap();
    assert!(output.finalized_segments.is_empty());
    assert!(manifest_rx.try_recv().is_err());
    assert!(!partition.final_path.exists());
    assert!(!partition.running_path.exists());
}

#[test]
fn same_hour_depth_resnapshot_stays_in_the_primary_hour_file() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recv_time_utc_ms = 1_783_392_404_600;
    let partition = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("binance_depth_snapshot", recv_time_utc_ms, None)
        .unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(2));
    let queue_rows_for_worker = Arc::clone(&queue_rows);
    let dataset_root_for_worker = dataset_root.clone();
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            test_runtime_provenance("same-hour-snapshot"),
            "binance_depth_snapshot".to_owned(),
            rx,
            queue_rows_for_worker,
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    tx.send(WriterCommand::Events(vec![
        InflightRawEvent::for_test(test_binance_depth_snapshot_event(
            recv_time_utc_ms,
            "bootstrap",
        )),
        InflightRawEvent::for_test(test_binance_depth_snapshot_event(
            recv_time_utc_ms + 1_000,
            "stitch_gap:test",
        )),
    ]))
    .unwrap();
    tx.send(WriterCommand::FinalizeHour {
        utc_hour: partition.utc_hour.clone(),
    })
    .unwrap();
    drop(tx);

    let output = handle.join().unwrap().unwrap();
    assert_eq!(output.finalized_segments.len(), 1);
    let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
        panic!("expected finalized-file delta");
    };
    assert_eq!(delta.rows, 2);
    assert_eq!(delta.part, 0);
    assert!(partition.final_path.is_file());
    assert!(!partition.running_path.exists());
    assert!(
        !HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_depth_snapshot", recv_time_utc_ms, Some(1))
            .unwrap()
            .final_path
            .exists()
    );
}

#[test]
fn binance_depth_manifest_counts_resnapshot_trigger_evidence() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let provenance = test_runtime_provenance("depth-resnapshot-run");
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let queue_rows_for_worker = Arc::clone(&queue_rows);
    let dataset_root_for_worker = dataset_root.clone();
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let handle = std::thread::spawn(move || {
        source_writer_worker(
            dataset_root_for_worker,
            provenance,
            "binance_depth_update".to_owned(),
            rx,
            queue_rows_for_worker,
            Arc::new(Mutex::new(true)),
            Arc::new(SourceQueueBackpressureStats::default()),
            manifest_tx,
        )
    });

    let event = test_binance_depth_update_event_with_trigger(
        1_783_392_404_600,
        "stitch_gap:U > local_u + 1",
    );
    queue_rows.store(1, Ordering::Relaxed);
    tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
        event,
    )]))
    .unwrap();
    drop(tx);

    let _output = handle.join().unwrap().unwrap();
    let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
        panic!("expected finalized-file delta");
    };
    assert_eq!(
        delta.facts["binance_depth_stitch"]["stitch_applied_rows"],
        0
    );
    assert_eq!(delta.facts["binance_depth_stitch"]["gap_rows"], 1);
    assert_eq!(delta.facts["binance_depth_stitch"]["resnapshot_count"], 1);
    assert!(
        delta.facts["source_gaps"]
            .as_array()
            .is_some_and(|gaps| !gaps.is_empty())
    );
}

#[test]
fn metadata_manifest_distinguishes_complete_and_unavailable_rows() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let naming = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("polymarket_market_metadata", 1_783_392_300_000, None)
        .unwrap();
    let mut segment = ActiveSegment::new(naming, "polymarket_market_metadata", 0).unwrap();

    let unavailable = test_metadata_raw_event(
        "active_scan",
        json!({
            "event_type": "active_scan",
            "slug": "btc-updown-5m-1783392300",
            "condition_id": "condition-1",
            "asset_ids": ["1", "2"],
            "metadata_unavailable_reason": "market_resolved_not_observed_before_window_end",
            "same_condition_market_verified": true
        }),
    );
    segment.observe(&unavailable, 1);
    let context = SegmentManifestContext::from_segment(&segment);
    assert_eq!(context.metadata_unavailable_rows, 1);
    assert_eq!(context.metadata_complete_rows, 0);

    let complete = test_metadata_raw_event(
        "active_scan",
        json!({
            "event_type": "active_scan",
            "slug": "btc-updown-5m-1783392300",
            "condition_id": "condition-1",
            "asset_ids": ["1", "2"],
            "market_start_utc": 1_783_392_300_000_i64,
            "market_end_utc": 1_783_392_600_000_i64,
            "active": true,
            "accepting_orders": true,
            "market_lifecycle_state": "active",
            "tick_size": "0.01",
            "min_size": "1",
            "fee_schedule_raw": r#"{"fee":"0"}"#,
            "reward_eligibility_raw": r#"{"rewards_min_size":"1"}"#,
            "same_condition_market_verified": true
        }),
    );
    segment.observe(&complete, 1);
    let context = SegmentManifestContext::from_segment(&segment);
    assert_eq!(context.metadata_unavailable_rows, 1);
    assert_eq!(context.metadata_complete_rows, 1);
}

#[tokio::test]
async fn missing_writer_is_a_runtime_error() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let mut writers = BTreeMap::new();
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let runtime_stats = SourceQueueBackpressureStats::default();
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut batch = vec![InflightRawEvent::for_test(test_raw_event(
        "binance_aggtrade",
        1_783_392_404_599,
    ))];

    flush_source_batch(
        &mut writers,
        "binance_aggtrade",
        &mut batch,
        &gaps,
        &runtime_stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .expect_err("a missing writer must stop the runtime");

    let summary = tracker.finish(0);
    assert_eq!(summary.dropped_rows, 1);
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| gap == "writer_missing_for_source:binance_aggtrade")
    );
    let mut owner = ManifestOwnerState::new(dataset_root.clone(), test_daily_manifest_identity());
    owner.ensure_open("2026-07-07").unwrap();
    let counters = runtime_stats.drain_all_operational();
    owner
        .record_operational_counters("2026-07-07", &counters["2026-07-07"])
        .unwrap();
    let manifest = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-07"))
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest["source_counters"]["binance_aggtrade"]["dropped_rows"],
        1
    );
}

#[tokio::test]
async fn disconnected_writer_channel_is_a_runtime_error() {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    drop(rx);
    let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let mut writers = BTreeMap::from([(
        "binance_aggtrade".to_owned(),
        SourceWriterHandle {
            schema_version: typed_schema_version("binance_aggtrade").to_owned(),
            tx,
            queue_rows: Arc::new(AtomicU64::new(0)),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: None,
        },
    )]);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let runtime_stats = SourceQueueBackpressureStats::default();
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut batch = vec![InflightRawEvent::for_test(test_raw_event(
        "binance_aggtrade",
        1_783_392_404_599,
    ))];

    let error = flush_source_batch(
        &mut writers,
        "binance_aggtrade",
        &mut batch,
        &gaps,
        &runtime_stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .expect_err("a disconnected writer must stop the runtime");

    assert!(error.to_string().contains("writer_queue_disconnected"));
    assert_eq!(tracker.finish(0).dropped_rows, 1);
    assert_eq!(
        runtime_stats.drain_all_operational()["2026-07-07"]["binance_aggtrade"].dropped_rows,
        1
    );
}

#[tokio::test]
async fn writer_parser_failure_is_reported_when_workers_stop() {
    let tmp = tempdir().unwrap();
    let provenance = test_runtime_provenance("writer-parser-failure");
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let mut writers =
        start_writer_workers(tmp.path(), &provenance, &manifest_tx, &runtime_stats).unwrap();
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
    event.raw_json = json!({"e": "serverShutdown", "E": 1_783_392_404_599_i64}).to_string();
    let mut batch = vec![InflightRawEvent::for_test(event)];

    flush_source_batch(
        &mut writers,
        "binance_aggtrade",
        &mut batch,
        &gaps,
        &runtime_stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .unwrap();
    let mut runtime_failure = None;
    let (outputs, all_terminal, all_close_proven) = writer_outputs_or_record_failure(
        stop_writer_workers_with_budget(writers, Duration::from_millis(WRITER_FINALIZE_BUDGET_MS))
            .await,
        &gaps,
        &mut runtime_failure,
    );

    assert!(all_terminal);
    assert!(all_close_proven);
    assert_eq!(outputs.len(), LIVE_SOURCES.len() - 1);
    assert!(
        outputs
            .iter()
            .all(|output| output.source_kind != "binance_aggtrade")
    );
    assert!(
        runtime_failure.is_some_and(|error| { error.contains("binance_aggtrade writer failed") })
    );
    assert_eq!(
        runtime_stats.drain_all_operational()["2026-07-07"]["binance_aggtrade"].dropped_rows,
        1
    );
}

#[test]
fn writer_failure_counts_active_current_remaining_and_queued_rows_exactly() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(5));
    let accepting_commands = Arc::new(Mutex::new(true));
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);

    tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
        test_raw_event("binance_aggtrade", 1_783_392_404_599),
    )]))
    .unwrap();
    let mut invalid = test_raw_event("binance_aggtrade", 1_783_392_404_600);
    invalid.raw_json = json!({"e": "serverShutdown"}).to_string();
    tx.send(WriterCommand::Events(vec![
        InflightRawEvent::for_test(invalid),
        InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_601)),
    ]))
    .unwrap();
    tx.send(WriterCommand::Events(vec![
        InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_602)),
        InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_603)),
    ]))
    .unwrap();

    let result = source_writer_worker(
        dataset_root,
        test_runtime_provenance("writer-exact-drop"),
        "binance_aggtrade".to_owned(),
        rx,
        Arc::clone(&queue_rows),
        Arc::clone(&accepting_commands),
        Arc::clone(&runtime_stats),
        manifest_tx,
    );

    assert!(!result.unwrap_err().to_string().is_empty());
    assert_eq!(queue_rows.load(Ordering::Relaxed), 0);
    assert!(!*accepting_commands.lock().unwrap());
    assert_eq!(
        runtime_stats.drain_all_operational()["2026-07-07"]["binance_aggtrade"].dropped_rows,
        5
    );
}

fn test_metadata_raw_event(source_event_type: &str, raw_json: Value) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market_metadata".to_owned(),
        source_event_type: source_event_type.to_owned(),
        source_identifier: raw_json
            .get("slug")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        source_ts: Some("1783392300000".to_owned()),
        recv_time_utc_ms: 1_783_392_300_000,
        collector_host_id: "collector-test".to_owned(),
        collector_region: "local-test".to_owned(),
        source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
        raw_json: raw_json.to_string(),
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market_metadata".to_owned(),
    }
}

fn test_binance_depth_update_event_with_trigger(
    recv_time_utc_ms: i64,
    trigger_reason: &str,
) -> RawEvent {
    test_binance_depth_update_event_inner(recv_time_utc_ms, Some(trigger_reason))
}
