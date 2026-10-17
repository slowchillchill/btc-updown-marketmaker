//! 研究导出装配：公共 backtest 库不依赖人工策略库。
use backtest::*;
use baseline_mm::blb_research::blb_research_v1;
use serde::Deserialize;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    preparations: Vec<HistoricalPreparationConfigV1>,
}
fn durable_line(file: &mut File, value: &impl serde::Serialize) -> Result<(), MaterialError> {
    // 先完整序列化；进程中断时只把换行完整且已同步的行计入完成数。
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.sync_data()?;
    Ok(())
}
fn run() -> Result<(), MaterialError> {
    let mut args = std::env::args().skip(1);
    let mut check_targets = false;
    let mut config_path = None;
    let mut targets_path = None;
    let mut output_path = None;
    let mut stop_file = None::<PathBuf>;
    let mut interval = 30u64;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" => {
                println!(
                    "有限目标公共观察与BLB导出\n--config <JSON，preparations为原HistoricalPreparationConfigV1数组>\n--targets <原target-cutoffs.jsonl> --output <新JSONL路径>\n--progress-interval-seconds <秒，默认30，0关闭周期输出>\n--check-targets --targets <清单>：只解析并统计清单，不打开行情或创建输出\n--stop-file <尚不存在的停止标志路径；创建后在输入边界停止>\n只创建当前进程及公共读取的内部线程；SIGINT/SIGTERM可直接终止，不承诺续跑。"
                );
                return Ok(());
            }
            "--check-targets" => check_targets = true,
            "--config" => config_path = Some(args.next().ok_or("--config缺值")?),
            "--targets" => targets_path = Some(args.next().ok_or("--targets缺值")?),
            "--output" => output_path = Some(args.next().ok_or("--output缺值")?),
            "--stop-file" => stop_file = Some(args.next().ok_or("--stop-file缺值")?.into()),
            "--progress-interval-seconds" => {
                interval = args.next().ok_or("进度间隔缺值")?.parse()?
            }
            _ => return Err(format!("未知参数 {arg}").into()),
        }
    }
    let targets = read_observation_targets_v1(BufReader::new(File::open(
        targets_path.ok_or("缺少--targets")?,
    )?))?;
    if check_targets {
        let mut accounts = std::collections::BTreeMap::<String, usize>::new();
        let mut streams = std::collections::BTreeMap::<String, usize>::new();
        for target in &targets {
            *accounts
                .entry(
                    target
                        .original
                        .get("account")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .into(),
                )
                .or_default() += 1;
            *streams
                .entry(
                    target
                        .group
                        .as_ref()
                        .map(|g| g.input_stream_id.as_str())
                        .unwrap_or("unknown")
                        .into(),
                )
                .or_default() += 1;
        }
        println!(
            "{}",
            serde_json::json!({"targets":targets.len(),"accounts":accounts,"streams":streams})
        );
        return Ok(());
    }
    let config: Config = serde_json::from_reader(File::open(config_path.ok_or("缺少--config")?)?)?;
    let mut streams = std::collections::BTreeSet::new();
    for p in &config.preparations {
        if !streams.insert(p.header.input_stream_id.clone()) {
            return Err("同一input_stream_id重复配置；不能覆盖原流身份".into());
        }
    }
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path.ok_or("缺少--output")?)?;
    let started = Instant::now();
    let mut completed = 0usize;
    let outcome = (|| -> Result<bool, MaterialError> {
        for preparation in &config.preparations {
            if stop_file.as_ref().is_some_and(|p| p.exists()) {
                return Ok(true);
            }
            let selected = targets
                .iter()
                .filter(|t| {
                    t.group
                        .as_ref()
                        .is_some_and(|g| g.input_stream_id == preparation.header.input_stream_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut last = Instant::now();
            eprintln!(
                "阶段=装配 stream={} 目标={} 总输入=未知 耗时={:.3}s",
                preparation.header.input_stream_id,
                selected.len(),
                started.elapsed().as_secs_f64()
            );
            durable_line(
                &mut output,
                &serde_json::json!({"record_type":"stream_config","effective_header":preparation.effective_header(),"preparation":preparation}),
            )?;
            let result = export_historical_observations_v1(
                preparation,
                &selected,
                |row| {
                    let blb = row.observation.as_ref().map(blb_research_v1);
                    durable_line(
                        &mut output,
                        &serde_json::json!({"record_type":"target","public":row,"blb":blb}),
                    )?;
                    completed += 1;
                    Ok(())
                },
                |p| {
                    if interval > 0 && last.elapsed() >= Duration::from_secs(interval) {
                        eprintln!(
                            "阶段=公共归约 输入={} 总输入=未知 已落盘目标={}/{} 耗时={:.3}s",
                            p.processed_inputs,
                            p.completed_targets,
                            p.total_targets,
                            started.elapsed().as_secs_f64()
                        );
                        last = Instant::now();
                    }
                },
                || stop_file.as_ref().is_some_and(|p| p.exists()),
            )?;
            if result.stopped {
                return Ok(true);
            }
        }
        for target in &targets {
            if target
                .group
                .as_ref()
                .is_some_and(|g| streams.contains(&g.input_stream_id))
            {
                continue;
            }
            if stop_file.as_ref().is_some_and(|p| p.exists()) {
                return Ok(true);
            }
            let row = HistoricalObservationExportRowV1 {
                schema_version: "historical_observation_export_v1",
                target: target.original.clone(),
                matched_group: None,
                observation: None,
                unknown_reason: Some(
                    if target.group.is_none() {
                        "target_group_missing"
                    } else {
                        "input_stream_configuration_missing"
                    }
                    .into(),
                ),
            };
            durable_line(
                &mut output,
                &serde_json::json!({"record_type":"target","public":row,"blb":null}),
            )?;
            completed += 1;
        }
        Ok(false)
    })();
    let status = match &outcome {
        Ok(true) => "stopped",
        Ok(false) => "complete",
        Err(_) => "failed",
    };
    eprintln!(
        "阶段={} 已完整落盘目标={}/{} 耗时={:.3}s；输出末尾未完成行不可使用",
        status,
        completed,
        targets.len(),
        started.elapsed().as_secs_f64()
    );
    let stopped = outcome?;
    durable_line(
        &mut output,
        &serde_json::json!({"record_type":"completion","status":status,"completed_targets":completed,"total_targets":targets.len(),"elapsed_seconds":started.elapsed().as_secs_f64()}),
    )?;
    if stopped {
        return Err("已按停止标志退出；保留完整行，不保证续跑".into());
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("导出失败：{e}");
        std::process::exit(1)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn file_failure_does_not_report_a_completed_line() {
        let mut file = OpenOptions::new().write(true).open("/dev/full").unwrap();
        assert!(durable_line(&mut file, &serde_json::json!({"target":1})).is_err());
    }
}
