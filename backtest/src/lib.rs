//! 单次有限公共历史运行。策略通过 StrategyV1 注入，核心不识别具体经营规则。
mod preparation;
mod run;
mod timing;
pub use preparation::*;
pub use run::*;
pub use timing::*;
pub type MaterialError = Box<dyn std::error::Error + Send + Sync>;
