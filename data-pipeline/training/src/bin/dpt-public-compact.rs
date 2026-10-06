//! B紧凑公共材料入口；既有typed/parquet准入和来源键→GPU run排序→CPU归并→完整factsink。
use data_pipeline_training::{
    compact_public::{CompactPublicPreparationConfigV1, PublicCompactReaderV1, PublicCompactWriterV1, consume_historical_merge_facts_v1, repack_public_compact_v1},
    gpu_initial_sort::GpuInitialRunOrderV1,
    historical_initialization::{MetadataInitializationSelectionV1, select_prefix_initialization_config_with_progress_v1},
    historical_preparation_progress::{HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1},
    public_replay::{HISTORICAL_SOURCE_HEAD_ORDERING_VERSION, prepare_historical_finite_files_with_initial_order_v1},
};
use std::{error::Error, fs::File, path::Path, sync::Arc, time::Instant};

fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() == 4 && args[1] == "--pm-device-columns" {
        let started = Instant::now();
        let mut reader = PublicCompactReaderV1::open_with_progress(Path::new(&args[2]),args[3].parse()?,
            std::time::Duration::from_secs(10),|notice| {
                eprintln!("{}",serde_json::json!({"reader":notice,"elapsed_seconds":started.elapsed().as_secs_f64()}));
            })?;
        let mut last_notice = Instant::now();
        let columns = data_pipeline_training::public_gpu_columns::public_pm_device_columns_v1(&mut reader,|inputs| {
            if last_notice.elapsed().as_secs() >= 10 {
                eprintln!("{}",serde_json::json!({"stage":"public_pm_device_columns","inputs":inputs,
                    "elapsed_seconds":started.elapsed().as_secs_f64()}));
                last_notice = Instant::now();
            }
        })?;
        serde_json::to_writer(std::io::stdout(),&columns)?;
        return Ok(());
    }
    if args.len() == 5 && args[1] == "--repack" {
        let started = Instant::now();
        let mut reader = PublicCompactReaderV1::open_with_progress(Path::new(&args[2]),args[4].parse()?,
            std::time::Duration::from_secs(10), |notice| {
                eprintln!("{}",serde_json::json!({"reader":notice,"elapsed_seconds":started.elapsed().as_secs_f64()}));
            })?;
        let mut writer = PublicCompactWriterV1::create(Path::new(&args[3]),reader.manifest.header.clone())?;
        let mut last_notice = Instant::now();
        repack_public_compact_v1(&mut reader,&mut writer,|processed| {
            if last_notice.elapsed().as_secs() >= 10 {
                eprintln!("{}",serde_json::json!({"stage":"compact_repack","inputs":processed,
                    "elapsed_seconds":started.elapsed().as_secs_f64()}));
                last_notice = Instant::now();
            }
        })?;
        let receipt = writer.publish_with_progress(std::time::Duration::from_secs(10), |notice| {
            eprintln!("{}",serde_json::json!({"publication":notice,"elapsed_seconds":started.elapsed().as_secs_f64()}));
        })?;
        serde_json::to_writer(std::io::stdout(),&receipt)?;
        return Ok(());
    }
    if args.len() == 5 && args[1] == "--initialize" {
        let mut config: CompactPublicPreparationConfigV1 = serde_json::from_reader(File::open(&args[2])?)?;
        let started = Instant::now();
        eprintln!("{}",serde_json::json!({"stage":"select_real_metadata_initialization","preceding_manifest":args[3]}));
        let mut decoded = 0_u64;
        let mut last_notice = Instant::now();
        let mut notice = |event| {
            let stage_changed = matches!(&event, Notice::Stage(_));
            if let Notice::RawBatch { rows, .. } = event { decoded += rows; }
            if stage_changed || last_notice.elapsed().as_secs() >= 10 {
                eprintln!("{}",serde_json::json!({"stage":"metadata_initialization_selection","decoded_rows":decoded,
                    "total_rows":null,"elapsed_seconds":started.elapsed().as_secs_f64()}));
                last_notice = Instant::now();
            }
        };
        let notifier = HistoricalPreparationNotifierV1::new(&mut notice);
        config.initialization = Some(select_prefix_initialization_config_with_progress_v1(&config.files,Path::new(&args[3]),
            config.received_start_utc_ms,config.received_end_utc_ms,config.canonical.decode_batch_rows,
            config.canonical.max_decode_batch_bytes,Some(&notifier))?);
        serde_json::to_writer(std::fs::File::create_new(&args[4])?, &config)?;
        eprintln!("{}",serde_json::json!({"stage":"selected_real_metadata_initialization",
            "markets":config.initialization.as_ref().unwrap().markets.len(),"elapsed_seconds":started.elapsed().as_secs_f64()}));
        return Ok(());
    }
    if args.len() == 4 && args[1] == "--inspect" {
        let started = Instant::now();
        let mut reader = PublicCompactReaderV1::open_with_progress(Path::new(&args[2]), args[3].parse()?,
            std::time::Duration::from_secs(10), |notice| {
                eprintln!("{}",serde_json::json!({"reader":notice,"elapsed_seconds":started.elapsed().as_secs_f64()}));
            })?;
        let mut inputs = 0_u64;
        let mut last_notice = Instant::now();
        while reader.next_packet()?.is_some() {
            inputs += 1;
            if last_notice.elapsed().as_secs() >= 10 {
                eprintln!("{}",serde_json::json!({"stage":"compact_read","inputs":inputs,
                    "total_inputs":reader.manifest.inputs,"elapsed_seconds":started.elapsed().as_secs_f64()}));
                last_notice = Instant::now();
            }
        }
        println!("{}",serde_json::json!({"stage":"compact_read_complete","inputs":inputs,
            "groups":reader.manifest.groups,"facts":reader.manifest.facts,"elapsed_seconds":started.elapsed().as_secs_f64()}));
        return Ok(());
    }
    if args.len() != 4 {
        return Err("用法: dpt-public-compact <finite-config.json> <locked-python> <same-commit-sort_worker.py>".into());
    }
    let value: serde_json::Value = serde_json::from_reader(File::open(&args[1])?)?;
    if value["canonical"]["sort"]["merge_workers"].as_u64().is_none() {
        return Err("实际采用CPU外归并，必须按当前共享配额显式配置canonical.sort.merge_workers".into());
    }
    let config: CompactPublicPreparationConfigV1 = serde_json::from_value(value)?;
    std::fs::create_dir(&config.work_root)?;
    let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&args[2]), Path::new(&args[3]))?);
    let started = Instant::now();
    let mut preparation_stage = "starting".to_owned();
    let mut decoded_rows = 0_u64;
    let mut cached_rows = 0_u64;
    let mut preparation_notice_at = Instant::now();
    let mut notice = |event| {
        let mut stage_changed = false;
        match event {
            Notice::Stage(stage) => {
                preparation_stage = serde_json::to_string(&stage).unwrap();
                stage_changed = true;
            }
            Notice::RawBatch { rows, .. } => decoded_rows += rows,
            Notice::SelectedRow { pass: data_pipeline_training::historical_preparation_progress::HistoricalRawPassV1::SecondSupply, .. } => cached_rows += 1,
            Notice::SourceMergeProgress { index, progress } => {
                if preparation_notice_at.elapsed().as_secs() >= 10 {
                    eprintln!("{}",serde_json::json!({"stage":"cpu_source_merge", "source_index":index,
                        "progress":progress,"elapsed_seconds":started.elapsed().as_secs_f64()}));
                    preparation_notice_at = Instant::now();
                }
            }
            Notice::SourceSortFinished { index, metrics } => {
                eprintln!("{}",serde_json::json!({"stage":"source_sort_finished","source_index":index,"metrics":metrics}));
            }
            _ => {}
        }
        if stage_changed || preparation_notice_at.elapsed().as_secs() >= 10 {
            eprintln!("{}",serde_json::json!({"stage":preparation_stage,"decoded_rows_across_passes":decoded_rows,"selected_rows_second_supply":cached_rows,
                "elapsed_seconds":started.elapsed().as_secs_f64(),"total_inputs":null}));
            preparation_notice_at = Instant::now();
        }
    };
    let notifier = HistoricalPreparationNotifierV1::new(&mut notice);
    let initialization = config.initialization.as_ref().map(|config_initialization| {
        MetadataInitializationSelectionV1::open_with_progress(config_initialization,
            config.canonical.decode_batch_rows, config.canonical.max_decode_batch_bytes, Some(&notifier))
    }).transpose()?;
    let mut merge = prepare_historical_finite_files_with_initial_order_v1(
        &config.work_root.join("merge"), &config.files, &config.canonical,
        config.header.input_stream_id.clone(),
        config.received_start_utc_ms..config.received_end_utc_ms,
        initialization.as_ref(), Some(&notifier), Some(order),
    )?;
    let mut header = config.header.clone();
    header.ordering_version = HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into();
    let mut writer = PublicCompactWriterV1::create(&config.work_root.join("compact-public"), header.clone())?;
    let mut last_notice = Instant::now();
    let _public = consume_historical_merge_facts_v1(&mut merge, header, &mut writer, |processed| {
        if last_notice.elapsed().as_secs() >= 10 {
            eprintln!("{}", serde_json::json!({"stage":"public_compact_apply", "processed_inputs":processed,
                "elapsed_seconds":started.elapsed().as_secs_f64(), "total_inputs":null}));
            last_notice = Instant::now();
        }
        Ok(())
    })?;
    let receipt = writer.publish_with_progress(std::time::Duration::from_secs(10), |notice| {
        eprintln!("{}",serde_json::json!({"publication":notice,"elapsed_seconds":started.elapsed().as_secs_f64()}));
    })?;
    eprintln!("{}", serde_json::json!({"stage":"public_compact_published",
        "elapsed_seconds":started.elapsed().as_secs_f64(), "inputs":receipt.inputs,
        "groups":receipt.groups,"facts":receipt.facts}));
    serde_json::to_writer(std::io::stdout(), &receipt)?;
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("dpt-public-compact: {error}");
        std::process::exit(1);
    }
}
