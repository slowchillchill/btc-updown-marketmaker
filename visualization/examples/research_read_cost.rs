//! 单份真实公开研究 JSON，初始化/一窗/最近重试；只在本示例使用分配探针。
#[allow(dead_code)]
#[path = "read_cost.rs"]
mod probe;
use serde_json::json;
use visualization::sources::research::{ResearchDocument, ResearchQuery};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("缺显式研究 JSON 路径")?;
    let (mut reader, cost) = probe::measure("initialize", || {
        ResearchDocument::open(Some(std::path::Path::new(&path)))
    });
    assert_eq!(reader.status()["status"], "ready");
    println!("{cost}");
    let query = ResearchQuery {
        path: "/research/markets/1/frozen_sampling/0/descriptive_windows/4".into(),
        start: None,
        count: None,
    };
    let (first, mut cost) =
        probe::measure("select_window", || reader.select(query.clone()).unwrap());
    cost["encoding"] = reader.last_encoding_cost().clone();
    println!("{cost}");
    let (retry, mut cost) = probe::measure("retry_latest", || reader.select(query).unwrap());
    assert_eq!(first, retry);
    assert_eq!(first.as_ptr(), retry.as_ptr());
    cost["response_bytes"] = json!(retry.len());
    cost["new_source_reads"] = json!(0);
    cost["new_complete_encodings"] = json!(0);
    cost["same_bytes_allocation"] = json!(true);
    println!("{cost}");
    Ok(())
}
