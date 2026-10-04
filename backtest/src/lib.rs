//! 单次有限公共历史运行。策略通过 StrategyV1 注入，核心不识别具体经营规则。
#[cfg(test)]
extern crate self as backtest;
mod preparation;
mod preparation_progress;
mod run;
mod statistics;
mod timing;
pub use preparation::*;
pub use preparation_progress::HistoricalPreparationProgressV1;
pub use run::*;
pub use statistics::HistoricalStatisticsV1;
pub use timing::*;
pub type MaterialError = Box<dyn std::error::Error + Send + Sync>;
