//! 旧研究入口保持 JSON/数值身份，公共数学由中性所有者实现。
pub use data_pipeline_core::blb::*;
#[cfg(test)]
use rust_decimal::Decimal;
#[cfg(test)]
#[path = "blb_research_tests.rs"]
mod tests;
