//! 连续公共观察装配；配置不包含账户或策略。
use backtest::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::PathBuf,
    sync::Mutex,
    time::{Duration, Instant},
};
fn run() -> Result<(), MaterialError> {
    let mut args = std::env::args().skip(1);
    let (mut config_path, mut output_path, mut stop_path, mut progress_path) =
        (None, None, None, None);
    let (mut interval, mut threads) = (None, None);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" => {
                println!(
                    "连续零账户公共观察导出\n--config <JSON> --output <新JSONL> --progress <进度JSON> --stop-file <停止标志路径>\n--progress-interval-seconds <秒数，0为每次通知> --threads <Rayon线程数>\n创建停止标志后在准备批次/公共输入边界停止。SIGINT/SIGTERM直接终止；不支持自动续跑。"
                );
                return Ok(());
            }
            "--config" => config_path = Some(args.next().ok_or("--config缺值")?),
            "--output" => output_path = Some(args.next().ok_or("--output缺值")?),
            "--progress" => {
                progress_path = Some(PathBuf::from(args.next().ok_or("--progress缺值")?))
            }
            "--stop-file" => stop_path = Some(PathBuf::from(args.next().ok_or("--stop-file缺值")?)),
            "--progress-interval-seconds" => {
                interval = Some(args.next().ok_or("进度间隔缺值")?.parse::<u64>()?)
            }
            "--threads" => threads = Some(args.next().ok_or("线程数缺值")?.parse::<usize>()?),
            _ => return Err(format!("未知参数 {arg}").into()),
        }
    }
    let config: ContinuousPublicExportConfigV1 =
        serde_json::from_reader(File::open(config_path.ok_or("缺少--config")?)?)?;
    let interval = interval.ok_or("缺少进度间隔")?;
    let threads = threads.ok_or("缺少线程数")?;
    let stop_path = stop_path.ok_or("缺少停止标志路径")?;
    let progress_path = progress_path.ok_or("缺少进度路径")?;
    let output = Mutex::new(BufWriter::with_capacity(
        1 << 20,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_path.ok_or("缺少--output")?)?,
    ));
    // 首次阶段变化立即输出；随后按配置间隔更新。
    let previous = Mutex::new((Instant::now(), String::new()));
    let write_error = Mutex::new(None::<String>);
    let mut sink = |value: serde_json::Value| -> Result<(), MaterialError> {
        let mut w = output.lock().unwrap();
        serde_json::to_writer(&mut *w, &value)?;
        w.write_all(b"\n")?;
        Ok(())
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    let mut program = File::open(std::env::current_exe()?)?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = program.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    let program_sha256 = format!("{:x}", hash.finalize());
    sink(
        json!({"record_type":"runtime","program_sha256":program_sha256,"effective_rayon_threads":pool.current_num_threads(),"rayon_threads":threads,"progress_interval_seconds":interval,"stop_file":stop_path,"progress_file":progress_path}),
    )?;
    let result=pool.install(||export_continuous_public_v1(&config,&mut sink,|state|{
        let mut last=previous.lock().unwrap();
        if last.0.elapsed()>=Duration::from_secs(interval)||last.1!=state.stage {
            let update=(||->Result<(),MaterialError>{
                {let mut w=output.lock().unwrap();w.flush()?;w.get_ref().sync_data()?;}
                let tmp=progress_path.with_extension("json.tmp");
                std::fs::write(&tmp,serde_json::to_vec_pretty(&json!({"progress":state,"total_inputs":null,"remaining_seconds":null}))?)?;
                std::fs::rename(tmp,&progress_path)?;Ok(())
            })();
            if let Err(e)=update{*write_error.lock().unwrap()=Some(e.to_string());}
            eprintln!("阶段={} 扫描行={} 输入={} 组={} 耗时={:.3}s 总量/剩余=未知",state.stage,state.scanned_rows,state.processed_inputs,state.applied_groups,state.elapsed_seconds);
            *last=(Instant::now(),state.stage.into());
        }
    },||stop_path.exists()||write_error.lock().unwrap().is_some()));
    {
        let mut w = output.lock().unwrap();
        w.flush()?;
        w.get_ref().sync_data()?;
    }
    if let Some(e) = write_error.into_inner().unwrap() {
        return Err(e.into());
    }
    let state = result?;
    if state.stopped {
        return Err("已停止；可读完整行，继续需重新运行".into());
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("连续公共导出结束：{e}");
        std::process::exit(1)
    }
}
