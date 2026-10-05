//! #185：一次原归约产出紧凑标量；各因果折复用材料，不运行账户或采用正式阈值。
use baseline_mm::*;
use contracts::*;
use data_pipeline_core::public_observation_core::PublicObservationCoreV1;
use data_pipeline_training::{finite_source_progress::*, historical_preparation_progress::*};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[path = "source_health_two_layer/compact.rs"]
mod compact;
#[path = "source_health_two_layer/stats.rs"]
mod stats;
#[path = "source_health_two_layer/tests.rs"]
#[cfg(test)]
mod tests;
fn rss() -> String {
    fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .unwrap_or("VmRSS: unknown")
        .to_owned()
}
fn save(path: &Path, v: &impl Serialize) -> Result<()> {
    serde_json::to_writer_pretty(BufWriter::new(File::create(path)?), v)?;
    Ok(())
}
#[derive(Deserialize)]
struct Config {
    preparation: FiniteSourceProgressConfigV1,
    progress_interval_seconds: u64,
    binance_trade_enabled: bool,
    binance_depth_enabled: bool,
}
struct Selector {
    markets: BTreeMap<String, (MarketWindowV2, Option<BtcSettlementReferenceV1>)>,
    boundaries: BTreeSet<i64>,
    spot: bool,
    mask: u8,
    binance: u8,
}
impl Selector {
    fn mask_at(&self, at: i64) -> u8 {
        let mut mask = 0;
        for (window, reference) in self.markets.values() {
            if at >= window.start_at_utc_ms && at < window.end_at_utc_ms {
                mask |= self.binance;
                if self.spot {
                    mask |= 4;
                } else if *reference == Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds) {
                    mask |= 8;
                } else if *reference != Some(BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds) {
                    mask |= 4;
                }
            }
        }
        mask
    }
    fn emit(&mut self, at: i64, w: &mut compact::Writer) -> Result<()> {
        let mask = self.mask_at(at);
        if mask != self.mask {
            w.row(compact::Row {
                at,
                scope: compact::SELECT,
                received: 0,
                event: 0,
                flags: 0,
                samples: 0,
                selected: mask,
            })?;
            self.mask = mask;
        }
        Ok(())
    }
    fn advance(&mut self, to: i64, w: &mut compact::Writer) -> Result<()> {
        while let Some(at) = self.boundaries.first().copied().filter(|t| *t < to) {
            self.boundaries.remove(&at);
            self.emit(at, w)?;
            self.markets
                .retain(|_, (window, _)| window.end_at_utc_ms > at);
        }
        Ok(())
    }
    fn update(
        &mut self,
        at: i64,
        core: &PublicObservationCoreV1,
        w: &mut compact::Writer,
    ) -> Result<()> {
        self.spot = eligible_chainlink_spot_v1(core.current_chainlink_spot()).is_some();
        for (condition, (_, reference)) in &mut self.markets {
            *reference = core.reference_state(condition).and_then(|r| {
                matches!(
                    r.rule.quality,
                    MarketDataQuality::Fresh | MarketDataQuality::Stale
                )
                .then_some(r.rule.value)
                .flatten()
            });
        }
        self.emit(at, w)
    }
}
fn prepare(config: &Path, output: &Path) -> Result<()> {
    let c: Config = serde_json::from_slice(&fs::read(config)?)?;
    fs::create_dir(output)?;
    let mut w = compact::Writer::new(&output.join("scalars.zst"))?;
    let mut specs = Vec::<compact::Scope>::new();
    let mut ids = BTreeMap::new();
    let mut previous_ids = Vec::<Option<Sha256Hex>>::new();
    let mut windows = BTreeMap::<String, MarketWindowV2>::new();
    let begin = Instant::now();
    let interval = Duration::from_secs(c.progress_interval_seconds);
    let mut last = Instant::now();
    let mut stage_last = Instant::now();
    let mut raw = 0u64;
    let declared_main_rows: u64 = c.preparation.files.iter().map(|f| f.rows).sum();
    let declared_main_files = c.preparation.files.len();
    let mut sort_metrics = Vec::new();
    let mut notice = |n| {
        match n {
            HistoricalPreparationNoticeV1::Stage(s) => eprintln!(
                "stage={s:?} elapsed_s={:.1} raw_rows={raw} {}",
                begin.elapsed().as_secs_f64(),
                rss()
            ),
            HistoricalPreparationNoticeV1::RawBatch { rows, .. } => {
                raw += rows;
            }
            HistoricalPreparationNoticeV1::SourceSortFinished { index, metrics } => {
                sort_metrics.push(json!({"index":index,"metrics":metrics}))
            }
            _ => {}
        }
        if !interval.is_zero() && stage_last.elapsed() >= interval {
            eprintln!(
                "stage=preparation elapsed_s={:.1} raw_rows={raw} declared_main_rows_per_pass={declared_main_rows} declared_main_files_per_pass={declared_main_files} {}",
                begin.elapsed().as_secs_f64(),
                rss()
            );
            stage_last = Instant::now();
        }
    };
    let notifier = HistoricalPreparationNotifierV1::new(&mut notice);
    let mut now = i64::MIN;
    let mut inputs = 0u64;
    let mut initialization = Vec::new();
    let mut day = i64::MIN;
    let mut telemetry = BufWriter::new(File::create(output.join("telemetry.jsonl"))?);
    let mut selector = Selector {
        markets: BTreeMap::new(),
        boundaries: BTreeSet::new(),
        spot: false,
        mask: 0,
        binance: u8::from(c.binance_trade_enabled) | (u8::from(c.binance_depth_enabled) * 2),
    };
    for (source, name) in [
        (SourceStreamKindV1::BinanceAggregateTrade, "BTCUSDT"),
        (SourceStreamKindV1::BinanceDepth, "BTCUSDT"),
        (SourceStreamKindV1::ChainlinkSpot, "btc/usd"),
        (SourceStreamKindV1::ChainlinkTwapSixty, "btc/usd"),
    ] {
        let scope = SourceStreamScopeV1 {
            source,
            condition_id: None,
            asset_or_symbol: name.into(),
        };
        let id = specs.len() as u32;
        ids.insert(scope.clone(), id);
        specs.push(compact::Scope {
            scope,
            window: None,
        });
        previous_ids.push(None);
    }
    let core = prepare_finite_source_progress_v1(
        &c.preparation,
        Some(&notifier),
        |input, applied, binding, current| {
            if let Some(binding) = binding {
                initialization.push(json!({"input_position":input.input_position,"source_message_id":binding.source_message_id}));
            }
            inputs += 1;
            for g in &applied.closed_groups {
                now = now.max(g.visible_at_utc_ms);
                selector.advance(now, &mut w)?;
                for b in &g.market_bindings {
                    windows.insert(b.market.condition_id.0.clone(), b.market.window.clone());
                    for asset in &b.assets {
                        let scope = SourceStreamScopeV1 {
                            source: SourceStreamKindV1::PolymarketBook,
                            condition_id: Some(b.market.condition_id.clone()),
                            asset_or_symbol: asset.token_id.0.clone(),
                        };
                        ids.entry(scope.clone()).or_insert_with(|| {
                            let id = specs.len() as u32;
                            specs.push(compact::Scope {
                                scope,
                                window: Some(b.market.window.clone()),
                            });
                            previous_ids.push(None);
                            id
                        });
                    }
                    if b.market.window.end_at_utc_ms > now {
                        selector.boundaries.insert(b.market.window.end_at_utc_ms);
                        if b.market.window.start_at_utc_ms > now {
                            selector.boundaries.insert(b.market.window.start_at_utc_ms);
                        }
                        selector.markets.insert(
                            b.market.condition_id.0.clone(),
                            (b.market.window.clone(), None),
                        );
                    }
                }
                selector.update(now, current, &mut w)?;
                for p in &g.source_progress_changes {
                    let id = *ids.entry(p.scope.clone()).or_insert_with(|| {
                        let id = specs.len() as u32;
                        specs.push(compact::Scope {
                            scope: p.scope.clone(),
                            window: None,
                        });
                        previous_ids.push(None);
                        id
                    });
                    let new_id = p
                        .last_valid_business_message
                        .as_ref()
                        .map(|e| e.source_message_id.clone());
                    let changed = new_id.is_some() && new_id != previous_ids[id as usize];
                    previous_ids[id as usize] = new_id;
                    w.row(compact::Row::from_progress(now, id, p, changed))?;
                }
            }
            if now != i64::MIN {
                selector.update(now, current, &mut w)?;
            }
            let current_day = now.div_euclid(stats::DAY);
            if current_day != day
                || (!interval.is_zero() && last.elapsed() >= interval)
                || matches!(
                    input.ingress,
                    data_pipeline_core::public_input::DptIngressV1::EndOfInput
                )
            {
                let counts = current.object_counts();
                eprintln!(
                    "stage=source-progress inputs={inputs} total_application_inputs=unknown scalar_records={} elapsed_s={:.1} utc_day_index={current_day} {} core_counts={counts:?}",
                    w.records,
                    begin.elapsed().as_secs_f64(),
                    rss()
                );
                serde_json::to_writer(
                    &mut telemetry,
                    &json!({"inputs":inputs,"records":w.records,"elapsed_seconds":begin.elapsed().as_secs_f64(),"utc_ms":now,"rss":rss(),"core_object_counts":counts}),
                )?;
                telemetry.write_all(b"\n")?;
                day = current_day;
                last = Instant::now();
            }
            Ok(())
        },
    )?;
    selector.advance(c.preparation.received_end_utc_ms, &mut w)?;
    for spec in &mut specs {
        if spec.scope.source == SourceStreamKindV1::PolymarketBook {
            spec.window = spec
                .scope
                .condition_id
                .as_ref()
                .and_then(|c| windows.get(&c.0))
                .cloned();
            if spec.window.is_none() {
                return Err("PM scope missing fixed market window".into());
            }
        }
    }
    let m = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: c.preparation.received_start_utc_ms,
        end: c.preparation.received_end_utc_ms,
        selection: "production-action-dependencies/spot-first-twap60/no-twap30".into(),
        scopes: specs,
        records: w.records,
    };
    w.finish()?;
    telemetry.flush()?;
    save(&output.join("material.json"), &m)?;
    save(
        &output.join("preparation-summary.json"),
        &json!({"inputs":inputs,"initialization":initialization,"records":m.records,"elapsed_seconds":begin.elapsed().as_secs_f64(),"sort_metrics":sort_metrics,"final_core_object_counts":core.object_counts()}),
    )?;
    Ok(())
}
/// 已有 #246 标量 journal 的有界导入，仅验证新编码/消费者，不复算原始归约。
#[derive(Deserialize)]
struct ImportedBinding {
    market: MarketIdentityV2,
}
fn import_legacy(input: &Path, output: &Path) -> Result<()> {
    fs::create_dir(output)?;
    let summary: Value = serde_json::from_slice(&fs::read(input.join("summary.json"))?)?;
    let mut windows = BTreeMap::new();
    for line in BufReader::new(File::open(input.join("market-bindings.jsonl"))?).lines() {
        let b: ImportedBinding = serde_json::from_str(&line?)?;
        windows.insert(b.market.condition_id.0, b.market.window);
    }
    let mut specs = Vec::new();
    for source in summary["sources"].as_array().ok_or("missing sources")? {
        let scope: SourceStreamScopeV1 = serde_json::from_value(source["scope"].clone())?;
        let window = scope
            .condition_id
            .as_ref()
            .and_then(|c| windows.get(&c.0))
            .cloned();
        specs.push(compact::Scope { scope, window });
    }
    let mut w = compact::Writer::new(&output.join("scalars.zst"))?;
    let mut previous = vec![Value::Null; specs.len()];
    let mut start = None;
    let mut end = 0;
    for line in BufReader::new(File::open(input.join("source-changes.jsonl"))?).lines() {
        let v: Value = serde_json::from_str(&line?)?;
        let id = v[0].as_u64().ok_or("bad scope id")? as usize;
        if id >= specs.len() {
            return Err("unknown scope".into());
        }
        let at = v[4].as_i64().ok_or("bad visible time")?;
        if start.is_none() {
            w.row(compact::Row {
                at: at.min(1788912000000),
                scope: compact::SELECT,
                received: 0,
                event: 0,
                flags: 0,
                samples: 0,
                selected: 15,
            })?;
            start = Some(at);
        }
        end = end.max(at);
        let valid = &v[5];
        let mut flags = 1;
        if v[8] == "Unknown" {
            flags |= 32;
        }
        if v[7].is_null() {
            flags |= 64;
        }
        let synchronization: SourceSynchronizationV1 = serde_json::from_value(v[8].clone())?;
        let quality: EventQualityV2 = serde_json::from_value(v[9].clone())?;
        if synchronization == SourceSynchronizationV1::SnapshotRequired
            || quality.resync == ResyncQuality::Required
        {
            flags |= 2;
        }
        if matches!(
            quality.availability,
            MarketDataQuality::Fresh | MarketDataQuality::Stale
        ) {
            flags |= 4;
        }
        if !valid.is_null() {
            flags |= 8;
        }
        if valid[2].is_i64() {
            flags |= 16;
        }
        let samples = u32::from(!valid.is_null() && previous[id] != valid[0]);
        previous[id] = valid[0].clone();
        w.row(compact::Row {
            at,
            scope: id as u32,
            received: valid[1].as_i64().unwrap_or(0),
            event: valid[2].as_i64().unwrap_or(0),
            flags,
            samples,
            selected: 0,
        })?;
    }
    // 窗口来自原准备配置而非末次消息；首尾无消息仍计入。
    let m = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: 1788912000000,
        end: 1788912720000,
        selection: "legacy-consumer-validation/all-common-scopes-not-production-selection".into(),
        scopes: specs,
        records: w.records,
    };
    if start.is_none() || end > m.end {
        return Err("unexpected legacy twelve-minute range".into());
    }
    w.finish()?;
    save(&output.join("material.json"), &m)?;
    Ok(())
}
/// 仅绑定既有日清单，不解码原始行或启动研究。路径由调用者显式提供。
fn configure(plan: &Path, input_root: &Path, work_root: &Path, out: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    let p: Value = serde_json::from_slice(&fs::read(plan)?)?;
    let mut c = p["configuration"].clone();
    let mut files = Vec::new();
    for binding in p["manifests"].as_array().ok_or("missing manifests")? {
        let relative = binding["path"].as_str().ok_or("missing manifest path")?;
        let manifest = input_root.join(relative);
        let bytes = fs::read(&manifest)?;
        if format!("{:x}", Sha256::digest(&bytes))
            != binding["sha256"]
                .as_str()
                .ok_or("missing manifest identity")?
        {
            return Err("manifest differs from audited identity".into());
        }
        let m: Value = serde_json::from_slice(&bytes)?;
        for f in m["files"].as_array().ok_or("missing files")? {
            files.push(json!({"path":manifest.parent().ok_or("missing manifest parent")?.join(f["path"].as_str().ok_or("missing file path")?),"source_kind":f["source_kind"],"schema_version":f["schema_version"],"bytes":f["bytes"],"rows":f["rows"],"sha256":f["sha256"]}));
        }
    }
    c["preparation"]["files"] = Value::Array(files);
    c["preparation"]["work_root"] = json!(work_root.join("prepare"));
    for file in c["preparation"]["metadata_initialization"]["files"]
        .as_array_mut()
        .ok_or("missing original initialization")?
    {
        file["manifest_path"] = json!(
            input_root.join(
                file["manifest_path"]
                    .as_str()
                    .ok_or("missing initialization manifest")?
            )
        );
    }
    let _: Config = serde_json::from_value(c.clone())?;
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out)?;
    serde_json::to_writer_pretty(BufWriter::new(output), &c)?;
    Ok(())
}
fn material(dir: &Path) -> Result<compact::Material> {
    Ok(serde_json::from_slice(&fs::read(
        dir.join("material.json"),
    )?)?)
}
fn study(dir: &Path, out: &Path, progress: u64) -> Result<()> {
    let m = material(dir)?;
    let start = 1788912000000;
    let research_end = start + 15 * stats::DAY;
    let development_end = start + 18 * stats::DAY;
    if m.start != start || m.end != development_end || !m.selection.starts_with("production-action")
    {
        return Err("study requires exact eighteen-day production-selected material".into());
    }
    fs::create_dir(out)?;
    compact::read(&dir.join("scalars.zst"), &m, |_| Ok(()))?;
    for (fold, fit_days) in [3, 6, 9, 12, 15].into_iter().enumerate() {
        let cutoff = start + fit_days * stats::DAY;
        let fit = stats::analyze(
            &dir.join("scalars.zst"),
            &m,
            start,
            cutoff,
            vec![],
            progress,
        )?;
        let candidates = stats::candidates(&fit)?;
        // 先写冻结完整参数，再开始本折未来窗口消费者；不读未来形成候选。
        save(
            &out.join(format!("fold-{fold}-frozen.json")),
            &json!({"fit_start":start,"fit_end":cutoff,"candidates":candidates,"daily_body_distributions":fit.daily}),
        )?;
        let end = if cutoff == research_end {
            development_end
        } else {
            cutoff + 3 * stats::DAY
        };
        let report = stats::analyze(
            &dir.join("scalars.zst"),
            &m,
            cutoff,
            end,
            candidates,
            progress,
        )?;
        save(&out.join(format!("fold-{fold}-validation.json")), &report)?;
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let command=args.first().and_then(|x|x.to_str()).ok_or("usage: configure PLAN INPUT_ROOT WORK_ROOT CONFIG_OUT | prepare CONFIG OUT | import-246 INPUT OUT | analyze MATERIAL HEALTH_TOML OUT | study MATERIAL OUT [PROGRESS_SECONDS]")?;
    let path = |i: usize| -> Result<PathBuf> {
        args.get(i)
            .map(PathBuf::from)
            .ok_or("missing path argument".into())
    };
    match command {
        "configure" => configure(&path(1)?, &path(2)?, &path(3)?, &path(4)?),
        "prepare" => prepare(&path(1)?, &path(2)?),
        "import-246" => import_legacy(&path(1)?, &path(2)?),
        "analyze" => {
            let dir = path(1)?;
            let m = material(&dir)?;
            let config = ComplementaryPairConfigV1::parse_toml(&fs::read_to_string(path(2)?)?)?;
            if config.health.normal_exceeds_max() {
                return Err("normal exceeds max".into());
            }
            let r = stats::analyze(
                &dir.join("scalars.zst"),
                &m,
                m.start,
                m.end,
                vec![stats::Candidate {
                    name: "explicit-engineering".into(),
                    health: config.health,
                }],
                30,
            )?;
            save(&path(3)?, &r)
        }
        "study" => study(
            &path(1)?,
            &path(2)?,
            args.get(3)
                .map(|s| s.to_str().ok_or("bad progress seconds"))
                .transpose()?
                .map(str::parse)
                .transpose()?
                .unwrap_or(30),
        ),
        _ => Err("unknown command".into()),
    }
}
