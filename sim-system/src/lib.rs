mod account;

// 提交撮合与确认式交付驱动器在后续组合根接线前必须保持 crate-private，不能为消除告警扩成第九公开操作。
#[allow(dead_code)]
pub(crate) mod event_log;
#[allow(dead_code)]
pub mod execution;
pub mod finality;
pub mod latency_profile;
#[allow(dead_code)]
pub(crate) mod legacy_reader;
#[allow(dead_code)]
pub(crate) mod market;
pub(crate) mod market_directory;
#[allow(dead_code)]
pub(crate) mod matching;
#[cfg(feature = "measurement")]
mod measurement_probe;

pub mod platform_rules;
pub(crate) mod rebate;
pub mod resource;
#[allow(dead_code)]
pub(crate) mod run_v2;
#[allow(dead_code)]
pub(crate) mod runtime_v2;
pub mod scenario;
// 公开 V2 运行时复用的市场登记 DTO、可见水位和输入封口结果纯叶。
pub(crate) mod session;

#[cfg(test)]
mod task21_tests;
#[cfg(test)]
mod task22_tests;
#[cfg(test)]
mod task23_tests;
