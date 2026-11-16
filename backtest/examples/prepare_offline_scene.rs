//! 工程验证的冻结场景准备入口；配置在策略运行前固定。
use backtest::{HistoricalPreparationConfigV1, offline_scene_source::*};
use serde::Deserialize;
use sim_system::offline_scene::Parameters;
#[derive(Deserialize)]
struct Config {
    preparation: HistoricalPreparationConfigV1,
    target_start: i64,
    target_end: i64,
    execution_start: i64,
    scenarios: Vec<Scenario>,
    #[serde(default)]
    association_scenarios: Vec<Scenario>,
    output: std::path::PathBuf,
    progress_seconds: u64,
    #[serde(default = "one_worker")]
    scenario_workers: usize,
    #[serde(default = "one_worker")]
    condition_workers: usize,
    #[serde(default)]
    condition_temporary_root: Option<std::path::PathBuf>,
}
fn one_worker() -> usize {
    1
}

#[derive(Deserialize)]
struct Scenario {
    name: String,
    parameters: Parameters,
}
fn main() -> Result<(), backtest::MaterialError> {
    let path = std::env::args().nth(1).ok_or("需要场景准备配置路径")?;
    if path == "metadata-identities" {
        use data_pipeline_core::typed_raw::{
            MetadataSourceEvidenceOrigin, POLYMARKET_MARKET_METADATA_SCHEMA,
            PolymarketMetadataRowKind,
        };
        let file = std::env::args()
            .nth(2)
            .ok_or("需要正式metadata字段JSON数组")?;
        let mut rows: Vec<serde_json::Value> = serde_json::from_slice(&std::fs::read(file)?)?;
        let mut identities = Vec::new();
        for row in &mut rows {
            row["schema_version"] = serde_json::json!(POLYMARKET_MARKET_METADATA_SCHEMA);
            row["metadata_row_kind"] = serde_json::to_value(PolymarketMetadataRowKind::try_from(
                row["metadata_row_kind"]
                    .as_str()
                    .ok_or("metadata类型缺失")?,
            )?)?;
            if let Some(raw) = row["source_evidence_origin"].as_str() {
                row["source_evidence_origin"] =
                    serde_json::to_value(MetadataSourceEvidenceOrigin::try_from(raw)?)?;
            }
            let evidence =
                data_pipeline_core::public_reference::metadata_source_evidence_v3_from_fields(
                    &serde_json::from_value::<data_pipeline_core::MetadataSourceIdentityInputV1>(
                        row.clone(),
                    )?,
                )?;
            identities.push(serde_json::json!({"condition_id":row["condition_id"],"snapshot_source_message_id":evidence.source_message_id}));
        }
        println!("{}", serde_json::to_string_pretty(&identities)?);
        return Ok(());
    }
    if path == "verify-storage" {
        let input = std::path::PathBuf::from(std::env::args().nth(2).ok_or("需要原场景")?);
        let output = std::path::PathBuf::from(std::env::args().nth(3).ok_or("需要新场景路径")?);
        let (material, events, _) = open_scene_material(&input)?;
        let mut writer = SceneStreamWriterV2::create(&output)?;
        let mut count = 0u64;
        for event in events {
            writer.push(event?)?;
            count += 1;
            if count % 4096 == 0 {
                eprintln!("storage convert: events={count}");
            }
        }
        let bytes = writer.finish((*material).clone())?;
        let (_, old, _) = open_scene_material(&input)?;
        let (_, new, _) = open_scene_material(&output)?;
        let mut checked = 0;
        let mut old = old;
        let mut new = new;
        loop {
            match (old.next(), new.next()) {
                (None, None) => break,
                (Some(a), Some(b)) if a.as_ref().ok() == b.as_ref().ok() => {
                    a?;
                    b?;
                    checked += 1;
                }
                _ => return Err("场景存储逐事件不一致".into()),
            }
        }
        println!(
            "{}",
            serde_json::json!({"events":checked,"output_bytes":bytes,"equal":true})
        );
        return Ok(());
    }
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    std::fs::create_dir_all(&config.output)?;
    if let Some(root) = &config.condition_temporary_root {
        std::fs::create_dir_all(root)?;
    }
    let start = std::time::Instant::now();
    let (analysis, markets, market_assets, manifests) = collect_scene_facts_v1(
        &config.preparation,
        config.target_start,
        config.target_end,
        config.progress_seconds,
    )?;
    let scan_seconds = start.elapsed().as_secs_f64();
    eprintln!("场景扫描完成: elapsed_seconds={scan_seconds}");
    let mut association_comparisons = Vec::new();
    for scenario in &config.association_scenarios {
        let started = std::time::Instant::now();
        let report = analysis.association_report(&scenario.parameters);
        let result = serde_json::json!({"scenario":scenario.name,"parameters":scenario.parameters,"report":report,"wall_seconds":started.elapsed().as_secs_f64()});
        eprintln!("关联比较: {result}");
        association_comparisons.push(result);
    }
    if config.scenario_workers == 0 {
        return Err("scenario_workers需要大于零".into());
    }
    let mut names = std::collections::BTreeSet::new();
    for scenario in &config.scenarios {
        if scenario.name.is_empty()
            || !scenario
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            || !names.insert(&scenario.name)
        {
            return Err("场景名称需要唯一且为安全文件名".into());
        }
    }
    let work = std::sync::Mutex::new(config.scenarios.iter());
    let summaries = std::thread::scope(
        |scope| -> Result<Vec<serde_json::Value>, backtest::MaterialError> {
            let workers = (0..config.scenario_workers.min(config.scenarios.len())).map(|_| {
            let work = &work; let analysis = &analysis; let config = &config; let markets = &markets; let manifests = &manifests; let market_assets=&market_assets;
            scope.spawn(move || -> Result<Vec<serde_json::Value>, backtest::MaterialError> {
                let mut summaries = Vec::new();
                loop {
                    let Some(scenario) = work.lock().unwrap().next() else { break; };
        let started = std::time::Instant::now();
        let mut last = std::time::Instant::now();
        let mut phase = String::new();
        let path = config.output.join(format!("{}.scene", scenario.name));
                        if config.condition_workers>1 {
                            let temporary = config.condition_temporary_root.as_ref().map(|root|root.join(&scenario.name));
                            let cost=prepare_condition_parallel_scene_with_temporary_root_v2(analysis,scenario.parameters.clone(),markets,market_assets,manifests,&path,config.execution_start,config.condition_workers,config.progress_seconds,temporary.as_deref())?;
                            let summary=serde_json::json!({"scenario":scenario.name,"parameters":scenario.parameters,"report":cost.report,"events":cost.events,"output_bytes":cost.output_bytes,"material":path.file_name().map(|s|s.to_string_lossy().into_owned()),"execution_start":config.execution_start,"target_start":config.target_start,"target_end":config.target_end,"preparation_and_write_seconds":started.elapsed().as_secs_f64(),"cost":cost});
                            eprintln!("场景完成: {summary}");summaries.push(summary);continue;
                        }

        let mut writer = SceneStreamWriterV2::create(&path)?;
        let mut events = 0u64;
        let scene = analysis.prepare_with_sink(scenario.parameters.clone(),|current,completed,total|{
            let base=current.split(':').next().unwrap_or(current);if base!=phase || completed==total || (config.progress_seconds>0 && last.elapsed().as_secs()>=config.progress_seconds){eprintln!("场景构造: scenario={}, phase={current}, completed={completed}, total={total}, elapsed_seconds={}",scenario.name,started.elapsed().as_secs());phase=base.to_owned();last=std::time::Instant::now();}
        }, |mut event| {
            // Only derived initialization time is clamped. Original facts and
            // their global emission order remain unchanged.
            event.at_ms = event.at_ms.max(config.execution_start);
            events += 1;
            writer.push(event)
        })?;
        let report = scene.report.clone();
        let parameters = scene.parameters.clone();
        let output_bytes = writer.finish(OfflineSceneMaterialV1 {
            scene, markets: markets.clone(), market_assets:market_assets.clone(), source_manifests: manifests.clone(),
        })?;
        let summary = serde_json::json!({"scenario":scenario.name,"parameters":parameters,"report":report,"events":events,"preparation_and_write_seconds":started.elapsed().as_secs_f64(),"output_bytes":output_bytes,"material":path.file_name().map(|s|s.to_string_lossy().into_owned()),"execution_start":config.execution_start,"target_start":config.target_start,"target_end":config.target_end});
        eprintln!("场景完成: {summary}");
        summaries.push(summary);
                }
                Ok(summaries)
            })
        }).collect::<Vec<_>>();
            let mut summaries = Vec::new();
            for worker in workers {
                summaries.extend(worker.join().map_err(|_| "场景准备线程panic")??);
            }
            summaries.sort_by(|a, b| a["scenario"].as_str().cmp(&b["scenario"].as_str()));
            Ok(summaries)
        },
    )?;
    serde_json::to_writer_pretty(
        std::fs::File::create(config.output.join("summary.json"))?,
        &serde_json::json!({"preparation_source_kinds":["polymarket_market","polymarket_market_metadata"],"association_comparisons":association_comparisons,"scenario_workers":config.scenario_workers,"condition_workers":config.condition_workers,"condition_temporary_root":config.condition_temporary_root,"scan_seconds":scan_seconds,"elapsed_seconds":start.elapsed().as_secs_f64(),"markets":markets,"source_manifests":manifests,"scenarios":summaries}),
    )?;
    Ok(())
}
