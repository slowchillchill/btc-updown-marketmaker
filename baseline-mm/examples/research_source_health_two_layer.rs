//! #185：列式研究的每日 bins 拟合与生产健康分类辅助入口。
use baseline_mm::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[path = "source_health_two_layer/stats.rs"]
mod stats;
#[path = "source_health_two_layer/tests.rs"]
#[cfg(test)]
mod tests;
fn save(path: &Path, v: &impl Serialize) -> Result<()> {
    serde_json::to_writer_pretty(BufWriter::new(File::create(path)?), v)?;
    Ok(())
}
/// Columnar consumers pass only daily sufficient statistics, never raw messages.
fn fit_columnar_histograms(input: &Path, output: &Path) -> Result<()> {
    #[derive(Deserialize)]
    struct Bin {
        day_start: i64,
        group_name: String,
        metric: String,
        lower: u64,
        weight: u64,
    }
    let mut report = stats::DailyDistributions::default();
    for line in BufReader::new(File::open(input)?).lines() {
        let row: Bin = serde_json::from_str(&line?)?;
        let distribution = report
            .entry(row.day_start)
            .or_default()
            .entry(row.group_name)
            .or_default();
        let histogram = match row.metric.as_str() {
            "silence" => &mut distribution.silence_time_ms,
            "event_age" => &mut distribution.event_age_time_ms,
            _ => return Err("unexpected columnar histogram metric".into()),
        };
        *histogram.bins.entry(row.lower).or_default() += row.weight;
    }
    save(output, &stats::candidates(&report)?)
}
/// Small cross-language checks call the exact production health policy.
fn classify_columnar_facts(input: &Path, output: &Path) -> Result<()> {
    #[derive(Deserialize)]
    struct Case {
        facts: Option<SourceHealthFactsV1>,
        limits: SourceHealthLimitsV1,
        at: i64,
    }
    let mut writer = BufWriter::new(File::create(output)?);
    for line in BufReader::new(File::open(input)?).lines() {
        let row: Case = serde_json::from_str(&line?)?;
        serde_json::to_writer(
            &mut writer,
            &source_health_state_v1(row.facts.as_ref(), row.limits, row.at),
        )?;
        writeln!(writer)?;
    }
    writer.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let command = args.first().and_then(|x| x.to_str()).ok_or(
        "usage: fit-columnar-histograms INPUT OUTPUT | classify-columnar-facts INPUT OUTPUT",
    )?;
    let path = |i: usize| -> Result<PathBuf> {
        args.get(i)
            .map(PathBuf::from)
            .ok_or("missing path argument".into())
    };
    match command {
        "fit-columnar-histograms" => fit_columnar_histograms(&path(1)?, &path(2)?),
        "classify-columnar-facts" => classify_columnar_facts(&path(1)?, &path(2)?),
        _ => Err("unknown command".into()),
    }
}
