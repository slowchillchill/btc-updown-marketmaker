//! 三类显式工程/历史来源之一：初始化、单区段、最近重试，不扩大源端负载。
#[allow(dead_code)]
#[path = "read_cost.rs"]
mod probe;
use serde_json::{Value, json};
use visualization::{dto::SourcesConfig, sources::existing::ExistingQuery};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 6 {
        return Err("参数：sources.json run_index document path count".into());
    }
    let config: SourcesConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let selected = config
        .runs
        .into_iter()
        .nth(args[2].parse::<usize>()?)
        .ok_or("来源索引越界")?;
    assert!(selected.existing_source.is_some());
    let (state, mut cost) = probe::measure("initialize", || {
        visualization::api::load_sources(SourcesConfig {
            runs: vec![selected],
        })
        .unwrap()
    });
    cost["snapshot_encoding"] = visualization::api::snapshot_encoding_cost(&state, 0)
        .unwrap()
        .clone();
    println!("{cost}");
    let q = ExistingQuery {
        document: args[3].clone(),
        path: args[4].clone(),
        start: Some(0),
        count: Some(args[5].parse()?),
    };
    let (first, mut cost) = probe::measure("select_fragment", || {
        visualization::api::select_existing(&state, 0, q.clone()).unwrap()
    });
    cost["encoding"] = visualization::api::existing_encoding_cost(&state, 0)?;
    println!("{cost}");
    let (retry, mut cost) = probe::measure("retry_latest", || {
        visualization::api::select_existing(&state, 0, q).unwrap()
    });
    assert_eq!(first, retry);
    assert_eq!(first.as_ptr(), retry.as_ptr());
    let body: Value = serde_json::from_slice(&retry)?;
    cost["response_bytes"] = json!(retry.len());
    cost["same_bytes_allocation"] = json!(true);
    cost["new_source_reads"] = json!(0);
    cost["new_complete_encodings"] = json!(0);
    cost["original_document"] = body["document"].clone();
    println!("{cost}");
    Ok(())
}
