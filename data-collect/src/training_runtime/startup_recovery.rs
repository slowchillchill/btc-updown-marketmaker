use super::*;

pub(super) const BINANCE_AGGTRADE_RECOVERY_IDS: usize = 25_000;

pub(super) fn finalized_parquet_files(source_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(source_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.ends_with(".parquet")
            && !file_name.ends_with("-running.parquet")
            && !file_name.ends_with("-digesting.parquet")
        {
            paths.push(entry.path());
        }
    }
    paths.sort_by_key(|path| finalized_parquet_order_key(path));
    Ok(paths)
}

pub(super) fn finalized_parquet_order_key(path: &Path) -> (String, u32) {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    stem.rsplit_once("-part-").map_or_else(
        || (stem.to_owned(), 0),
        |(base, part)| (base.to_owned(), part.parse::<u32>().unwrap_or(u32::MAX)),
    )
}

fn all_finalized_source_parquet_paths(
    dataset_root: &Path,
    source_kind: &str,
) -> Result<Vec<PathBuf>> {
    let typed_raw = dataset_root.join("training").join("typed_raw");
    let entries = match fs::read_dir(&typed_raw) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir()
            || !entry.file_name().to_string_lossy().starts_with("utc_day=")
        {
            continue;
        }
        paths.extend(finalized_parquet_files(
            &entry.path().join(format!("source_kind={source_kind}")),
        )?);
    }
    paths.sort_by_key(|path| {
        (
            path.parent()
                .map(|parent| parent.to_string_lossy().into_owned())
                .unwrap_or_default(),
            finalized_parquet_order_key(path),
        )
    });
    Ok(paths)
}

pub(super) struct StableStartupRecovery {
    pub(super) utc_day: String,
    pub(super) initial_next_boundary_ms: i64,
    pub(super) binance_aggtrade: BinanceAggTradeRecovery,
    pub(super) polymarket: PolymarketRestartRecovery,
    pub(super) current_manifest: Value,
}

pub(super) fn reconcile_stable_startup_recovery<F>(
    dataset_root: &Path,
    identity: &TrainingArtifactIdentity,
    mut clock_ms: F,
) -> Result<StableStartupRecovery>
where
    F: FnMut() -> i64,
{
    let mut last_clock_ms = None;
    let mut sample_clock = || -> Result<i64> {
        let current_ms = clock_ms();
        if let Some(previous_ms) = last_clock_ms
            && current_ms < previous_ms
        {
            return Err(invalid_data(format!(
                "UTC clock moved backward during startup recovery: previous_ms={previous_ms} current_ms={current_ms}"
            ))
            .into());
        }
        last_clock_ms = Some(current_ms);
        Ok(current_ms)
    };
    loop {
        let recovery_started_ms = sample_clock()?;
        let recovery_day = utc_day_string_from_ms(recovery_started_ms)
            .ok_or_else(|| invalid_data("current UTC day unavailable for Binance recovery"))?;
        let binance_aggtrade =
            restore_binance_aggtrade_recovery(dataset_root, &recovery_day, identity)?;
        let current_manifest = reconcile_open_day_manifest(
            &typed_raw_root(dataset_root).join(format!("utc_day={recovery_day}")),
            &recovery_day,
            identity,
        )?;
        let confirmed_ms = sample_clock()?;
        let confirmed_day = utc_day_string_from_ms(confirmed_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable after startup recovery"))?;
        if confirmed_day < recovery_day {
            return Err(invalid_data(format!(
                "UTC day moved backward during startup recovery: started={recovery_day} confirmed={confirmed_day}"
            ))
            .into());
        }
        if confirmed_day == recovery_day {
            let polymarket = restore_polymarket_resolution_recovery(dataset_root, confirmed_ms)?;
            let recovery_completed_ms = sample_clock()?;
            let recovery_completed_day = utc_day_string_from_ms(recovery_completed_ms)
                .ok_or_else(|| invalid_data("UTC day unavailable after Polymarket recovery"))?;
            if recovery_completed_day < recovery_day {
                return Err(invalid_data(format!(
                    "UTC day moved backward during Polymarket recovery: started={recovery_day} completed={recovery_completed_day}"
                ))
                .into());
            }
            if recovery_completed_day > recovery_day {
                continue;
            }
            return Ok(StableStartupRecovery {
                utc_day: recovery_day,
                initial_next_boundary_ms: recovery_completed_ms
                    .div_euclid(UTC_DAY_MS)
                    .saturating_add(1)
                    .saturating_mul(UTC_DAY_MS),
                binance_aggtrade,
                polymarket,
                current_manifest,
            });
        }
    }
}

pub(super) fn restore_binance_aggtrade_recovery(
    dataset_root: &Path,
    current_day: &str,
    current_identity: &TrainingArtifactIdentity,
) -> Result<BinanceAggTradeRecovery> {
    let days = utc_day_directories(dataset_root)?;
    if let Some((future_day, _)) = days.iter().find(|(day, _)| day.as_str() > current_day) {
        return Err(invalid_data(format!(
            "future UTC day cannot participate in recovery: {future_day}"
        ))
        .into());
    }
    for (day, day_dir) in days.iter().filter(|(day, _)| day.as_str() < current_day) {
        let Some(mut manifest) = load_day_manifest(day_dir)? else {
            if day_has_finalized_parquet(day_dir)? {
                return Err(io::Error::other(format!(
                    "prior UTC day has Parquet but no daily manifest: {day}"
                ))
                .into());
            }
            continue;
        };
        validate_manifest_day(&manifest, day)?;
        let state = manifest_state(&manifest)?;
        validate_manifest_recovery_files(day_dir, &manifest, state == "open")?;
        if state == "open" {
            if manifest_collection_day_coverage(&manifest)?.is_some() {
                add_manifest_operational_counters(
                    day_dir,
                    &mut manifest,
                    &required_source_counter_defaults(),
                )?;
            }
            close_reconciled_day_manifest(day_dir, &mut manifest)?;
        }
    }

    if let Some((day, day_dir)) = days.iter().find(|(day, _)| day == current_day) {
        if let Some(manifest) = load_day_manifest(day_dir)? {
            validate_manifest_day(&manifest, day)?;
            if manifest_state(&manifest)? == "closed" {
                return Err(io::Error::other(format!(
                    "current UTC day manifest is already closed: {day}"
                ))
                .into());
            }
            validate_manifest_recovery_files(day_dir, &manifest, true)?;
        } else {
            validate_unmanifested_recovery_files(day_dir, current_identity)?;
        }
    }

    Ok(BinanceAggTradeRecovery {
        websocket_dedupe: restore_binance_transport_dedupe(dataset_root)?,
    })
}

fn binance_aggtrade_restore_row_is_trade(
    record_kinds: Option<&StringArray>,
    ids: &UInt64Array,
    row: usize,
    path: &Path,
) -> Result<bool> {
    let Some(record_kinds) = record_kinds else {
        if ids.is_null(row) {
            return Err(io::Error::other(format!(
                "legacy Binance aggTrade ID missing in {}",
                path.display()
            ))
            .into());
        }
        return Ok(true);
    };
    if record_kinds.is_null(row) {
        return Err(io::Error::other(format!(
            "Binance aggTrade record kind missing in {}",
            path.display()
        ))
        .into());
    }
    match record_kinds.value(row) {
        "trade" => {
            if ids.is_null(row) {
                return Err(io::Error::other(format!(
                    "Binance aggTrade trade ID missing in {}",
                    path.display()
                ))
                .into());
            }
            Ok(true)
        }
        "invalid_input" => {
            if !ids.is_null(row) {
                return Err(io::Error::other(format!(
                    "Binance aggTrade invalid input has a trade ID in {}",
                    path.display()
                ))
                .into());
            }
            Ok(false)
        }
        kind => Err(io::Error::other(format!(
            "unknown Binance aggTrade record kind {kind:?} in {}",
            path.display()
        ))
        .into()),
    }
}

pub(super) fn restore_binance_transport_dedupe(
    dataset_root: &Path,
) -> Result<BinanceRecentIdWindow> {
    restore_binance_transport_dedupe_with_limit(dataset_root, BINANCE_AGGTRADE_RECOVERY_IDS)
}

pub(super) fn restore_binance_transport_dedupe_with_limit(
    dataset_root: &Path,
    recovery_id_limit: usize,
) -> Result<BinanceRecentIdWindow> {
    if recovery_id_limit == 0 {
        return Err(invalid_data("Binance aggTrade recovery ID limit is zero").into());
    }
    let mut websocket = BinanceRecentIdWindow::with_limit(recovery_id_limit);
    let mut recovery_ids = BTreeSet::new();
    let mut paths = all_finalized_source_parquet_paths(dataset_root, "binance_aggtrade")?;
    paths.reverse();
    'files: for path in paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        if builder.schema().index_of("aggregate_trade_id").is_err()
            || builder
                .schema()
                .index_of("source_native_payload_json")
                .is_err()
        {
            continue;
        }
        if builder.metadata().file_metadata().num_rows() == 0 {
            continue;
        }
        let has_record_kind = builder.schema().index_of("record_kind").is_ok();
        let projection_columns = if has_record_kind {
            vec![
                "aggregate_trade_id",
                "source_native_payload_json",
                "record_kind",
            ]
        } else {
            vec!["aggregate_trade_id", "source_native_payload_json"]
        };
        let projection = ProjectionMask::columns(builder.parquet_schema(), projection_columns);
        for row_group in (0..builder.metadata().num_row_groups()).rev() {
            let row_group_builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
            let mut reader = row_group_builder
                .with_projection(projection.clone())
                .with_row_groups(vec![row_group])
                .with_batch_size(16)
                .build()?;
            for batch in &mut reader {
                let batch = batch?;
                let id_index = batch.schema().index_of("aggregate_trade_id")?;
                let payload_index = batch.schema().index_of("source_native_payload_json")?;
                let ids = batch
                    .column(id_index)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| io::Error::other("Binance restore ID is not UInt64"))?;
                let payloads = batch
                    .column(payload_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| io::Error::other("Binance restore payload is not UTF8"))?;
                let record_kinds = if has_record_kind {
                    Some(
                        batch
                            .column(batch.schema().index_of("record_kind")?)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                io::Error::other("Binance restore record kind is not UTF8")
                            })?,
                    )
                } else {
                    None
                };
                for row in 0..batch.num_rows() {
                    if payloads.is_null(row) {
                        return Err(io::Error::other(format!(
                            "Binance restore payload missing in {}",
                            path.display()
                        ))
                        .into());
                    }
                    if !binance_aggtrade_restore_row_is_trade(record_kinds, ids, row, &path)? {
                        continue;
                    }
                    let persisted_sources = payloads.value(row);
                    if persisted_sources.len() > MAX_TYPED_ROW_BYTES {
                        return Err(io::Error::other(format!(
                            "operator_intervention_required:Binance restore payload exceeds {MAX_TYPED_ROW_BYTES} bytes in {}",
                            path.display()
                        ))
                        .into());
                    }
                    let parsed = serde_json::from_str::<Value>(persisted_sources)?;
                    let websocket_payload = match parsed.get("websocket") {
                        Some(Value::String(payload)) => payload.as_str(),
                        Some(Value::Null) => continue,
                        Some(_) => {
                            return Err(io::Error::other(format!(
                                "Binance restore websocket payload is not UTF8 in {}",
                                path.display()
                            ))
                            .into());
                        }
                        None => persisted_sources,
                    };
                    let native_id = ids.value(row);
                    recovery_ids.insert(native_id);
                    while recovery_ids.len() > recovery_id_limit {
                        let Some(evicted) = recovery_ids.pop_first() else {
                            break;
                        };
                        websocket.remove(evicted);
                    }
                    if !recovery_ids.contains(&native_id) {
                        continue;
                    }
                    let _ = websocket.observe(native_id, websocket_payload.as_bytes());
                }
            }
            if recovery_ids.len() == recovery_id_limit {
                break 'files;
            }
        }
    }
    Ok(websocket)
}
