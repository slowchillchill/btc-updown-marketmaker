//! 单次有限公共历史运行。策略通过 StrategyV1 注入，核心不识别具体经营规则。
#[cfg(test)]
extern crate self as backtest;
mod preparation;
mod preparation_progress;
mod process_measurement;
mod run;
mod statistics;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/common/mod.rs"]
mod test_common;
mod timing;
pub use preparation::*;
pub use preparation_progress::{
    HistoricalPreparationMeasurementsV1, HistoricalPreparationProgressV1,
};
pub use process_measurement::{
    ProcessCountersV1, process_clock_ticks_per_second_v1, process_measurement_v1,
};
pub use run::*;
pub use statistics::HistoricalStatisticsV1;
pub use timing::*;
pub type MaterialError = Box<dyn std::error::Error + Send + Sync>;
