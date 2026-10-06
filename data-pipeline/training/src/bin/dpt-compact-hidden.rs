//! compact原消息→独立hidden归约→自身原子发布；不依赖旧public recording。
use data_pipeline_training::{
    compact_hidden_material::{CompactHiddenPreparationConfigV1, CompactHiddenMaterialReaderV1,
        prepare_compact_hidden_material_with_interval_v1},
    gpu_initial_sort::GpuInitialRunOrderV1,
    historical_preparation_progress::{HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1},
};
use std::{error::Error, fs::File, path::Path, sync::Arc, time::Instant};

fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args: Vec<_> = std::env::args().collect();
    let interval = if args.get(1).is_some_and(|value| value=="--progress-interval-seconds") {
        let seconds:u64 = args.get(2).ok_or("进度间隔缺少秒数")?.parse()?;
        args.drain(1..3);
        std::time::Duration::from_secs(seconds)
    } else { std::time::Duration::from_secs(10) };
    let started = Instant::now();
    let publication = |stage: &str, member: &str, completed: u64, total: Option<u64>| {
        if interval.is_zero() { return; }
        eprintln!("{}",serde_json::json!({"stage":stage,"member":member,
            "completed":completed,"total":total,"unit":if member=="inputs" {"inputs"} else {"bytes"},"elapsed_seconds":started.elapsed().as_secs_f64()}));
    };
    if args.len()==7 && args[1]=="--inspect" {
        let mut reader = CompactHiddenMaterialReaderV1::open_with_progress_interval(Path::new(&args[2]),Path::new(&args[3]),
            args[4].parse()?,args[5].parse()?,args[6].parse()?,interval,publication)?;
        let mut buckets = 0_u64;
        let mut steps = 0_u64;
        let mut references = 0_u64;
        let mut last = Instant::now();
        while let Some(bucket) = reader.next_bucket()? {
            for reference in &bucket.steps { reader.restore_step(reference)?; }
            for position in &bucket.reference_positions { reader.reference_fact(*position)?; }
            buckets += 1; steps += bucket.steps.len() as u64; references += bucket.reference_positions.len() as u64;
            if !interval.is_zero() && last.elapsed()>=interval {
                eprintln!("{}",serde_json::json!({"stage":"hidden_read","buckets":buckets,"steps":steps,
                    "total_indexed_steps":reader.manifest.indexed_steps,"references":references,
                    "elapsed_seconds":started.elapsed().as_secs_f64()}));
                last = Instant::now();
            }
        }
        println!("{}",serde_json::json!({"stage":"hidden_read_complete","buckets":buckets,"steps":steps,
            "references":references,"source_prefix":reader.manifest.summary.source_prefix,
            "elapsed_seconds":started.elapsed().as_secs_f64()}));
        return Ok(());
    }
    if args.len()!=6 {
        return Err("用法: dpt-compact-hidden [--progress-interval-seconds N (0关闭)] <source-compact> <target> <config.json> <locked-python> <same-commit-sort_worker.py>; --inspect <hidden> <source> <rows> <bytes> <max-packet-bytes>".into());
    }
    let value: serde_json::Value = serde_json::from_reader(File::open(&args[3])?)?;
    if value["sort"]["merge_workers"].as_u64().is_none() {
        return Err("实际采用CPU跨run归并，必须显式设置sort.merge_workers".into());
    }
    let config: CompactHiddenPreparationConfigV1 = serde_json::from_value(value)?;
    let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&args[4]),Path::new(&args[5]))?);
    let mut scanned = 0_u64; let mut supplied = 0_u64; let mut reduced = 0_u64;
    let mut last = Instant::now();
    let mut phase = None;
    let mut callback = |notice| {
        if interval.is_zero() { return; }
        let stage = match notice {
            Notice::Stage(stage)=>Some(stage),
            Notice::HiddenScanRow=>{scanned += 1;None},
            Notice::HiddenMessageSupplied=>{supplied += 1;None},
            Notice::HiddenMessageReduced=>{reduced += 1;None},
            Notice::HiddenMessagesSorted(metrics)|Notice::HiddenIndexSorted(metrics)=>{
                eprintln!("{}",serde_json::json!({"stage":"hidden_sort_complete","metrics":metrics,
                    "elapsed_seconds":started.elapsed().as_secs_f64()}));None
            },
            _=>None,
        };
        if stage.is_some() { phase = stage; }
        if stage.is_some() || last.elapsed()>=interval {
            eprintln!("{}",serde_json::json!({"stage":phase,"scanned_inputs":scanned,
                "supplied_messages":supplied,"reduced_messages":reduced,"elapsed_seconds":started.elapsed().as_secs_f64()}));
            last = Instant::now();
        }
    };
    let notifier = HistoricalPreparationNotifierV1::new(&mut callback);
    let manifest = prepare_compact_hidden_material_with_interval_v1(Path::new(&args[1]),Path::new(&args[2]),
        config.max_packet_bytes,&config.sort,order,config.consumer_stream_id,Some(&notifier),interval,publication)?;
    serde_json::to_writer(std::io::stdout(),&manifest)?;
    Ok(())
}
fn main() {
    if let Err(error) = run() { eprintln!("{error}"); std::process::exit(1); }
}
