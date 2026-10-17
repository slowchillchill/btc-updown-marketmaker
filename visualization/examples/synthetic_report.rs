//! 复用 backtest 的合法工程夹具；不复制领域 fixture，不启动策略研究。
#[path = "../../backtest/tests/common/default.rs"]
mod fixture_default;
#[path = "../../backtest/tests/common/fixture_material.rs"]
mod fixture_material;
mod common {
    pub(crate) use super::fixture_default::*;
}

use shared_run::{StrategyInputV1, StrategyOutputV1, StrategyV1};
use std::path::PathBuf;
struct EngineeringObserver;
impl StrategyV1 for EngineeringObserver {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        Ok(StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}
#[tokio::main]
async fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("指定新建的合成工程产物目录"));
    // 新目录由该验证生成器拥有；浏览服务本身不写任何运行来源。
    std::fs::create_dir(&root).expect("新目录不能已存在");
    eprintln!("合成工程验证：准备合法历史输入并调用 run_historical_v1；非真实研究");
    let (config, rules) = common::setup(&root);
    let report = backtest::run_historical_v1(&config, rules, &mut EngineeringObserver)
        .await
        .unwrap();
    assert!(matches!(
        report.end_reason,
        backtest::HistoricalEndReasonV1::RangeEnd
    ));
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&config.metadata_path).unwrap()).unwrap();
    // run_historical_v1 已返回的稳定凭据；不通过冷扫描推测最新前缀。
    let prefix_path = root.join("committed-prefix.json");
    std::fs::write(
        &prefix_path,
        serde_json::to_vec_pretty(&metadata["public_prefix"]).unwrap(),
    )
    .unwrap();
    let sources = serde_json::json!({"runs":[{"label":"合成工程验证 · 非策略研究", "report":config.result_path,"metadata":config.metadata_path,
        "public_recording":{"root":config.preparation.work_root.join("public"),"committed_prefix":prefix_path,"page_rows":10,"max_batch_bytes":1048576}}]});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec_pretty(&sources).unwrap(),
    )
    .unwrap();
    eprintln!(
        "合成生产入口读取接线完成：RangeEnd；报告、元数据、已返回前缀已生成。未生成诊断历史。"
    );
}
