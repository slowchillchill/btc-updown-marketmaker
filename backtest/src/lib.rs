//! 单次有限公共历史运行。策略通过 StrategyV1 注入，核心不识别具体经营规则。
#[cfg(test)]
extern crate self as backtest;
#[macro_use]
mod stage_timing;
mod day;
mod public_observer;
pub use public_observer::*;
mod entry_market;
pub use entry_market::*;
mod continuous_export;
pub use continuous_export::*;
mod observation_export;
pub use observation_export::*;
mod day_economics;
mod day_state;
mod day_summary;
mod decision_evidence;
mod preparation;
mod preparation_progress;
mod prepared_material;
mod process_measurement;
mod research_summary;
mod run;
pub use day::*;
pub use day_economics::HistoricalDayEconomicsSummaryV1;
pub use day_summary::*;
pub use research_summary::{
    DecisionCountsV1, ResearchHourV1, ResearchSnapshotV1, ResearchSummaryV1,
};
mod statistics;
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod test_common;
mod timing;
pub use preparation::*;
pub use preparation_progress::{
    HistoricalPreparationMeasurementsV1, HistoricalPreparationProgressV1,
};
pub use prepared_material::*;
pub use process_measurement::{
    ProcessCountersV1, process_clock_ticks_per_second_v1, process_measurement_v1,
};
pub use run::*;
pub use statistics::HistoricalStatisticsV1;
pub use timing::*;
pub type MaterialError = Box<dyn std::error::Error + Send + Sync>;
