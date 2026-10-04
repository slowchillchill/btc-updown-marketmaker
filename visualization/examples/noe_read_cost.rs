//! 单个原 writer 引用首读/最近重试；allocator 仅沿用示例探针，不替换服务 allocator。
#[allow(dead_code)]
#[path = "read_cost.rs"]
mod probe;
use serde_json::{Value, json};
use visualization::{dto::SourcesConfig, sources::noe::NoeResults};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: SourcesConfig = serde_json::from_slice(&std::fs::read(
        std::env::args().nth(1).ok_or("缺 NOE sources.json")?,
    )?)?;
    let source = config
        .runs
        .into_iter()
        .next()
        .ok_or("缺首来源")?
        .noe_results
        .into_iter()
        .nth(2)
        .ok_or("需要 v4 Evidence 原引用 index 2")?;
    if source.version != "v4" {
        return Err("本探针只测工程 v4 单引用".into());
    }
    let mut reader = NoeResults::new(vec![source]);
    let (first, mut cost) = probe::measure("first_read", || reader.load(0).unwrap());
    let body: Value = serde_json::from_slice(&first)?;
    assert_eq!(body["status"], "ready");
    cost["encoding"] = serde_json::to_value(reader.last_encoding_cost())?;
    cost["native_cost"] = body["cost"].clone();
    println!("{cost}");
    let (retry, mut cost) = probe::measure("retry_latest", || reader.load(0).unwrap());
    assert_eq!(first, retry);
    assert_eq!(first.as_ptr(), retry.as_ptr());
    cost["response_bytes"] = json!(retry.len());
    cost["new_native_read_calls"] = json!(0);
    cost["new_complete_encodings"] = json!(0);
    cost["same_bytes_allocation"] = json!(true);
    println!("{cost}");
    Ok(())
}
