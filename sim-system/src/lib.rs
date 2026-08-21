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

pub mod platform_rules;
pub(crate) mod rebate;
pub mod resource;
#[allow(dead_code)]
pub(crate) mod run_v2;
#[allow(dead_code)]
pub(crate) mod runtime_v2;
pub mod scenario;
// 会话水位、动作释放和响应完成原语只供公共八方法背后的受信驱动器使用。
#[allow(dead_code)]
pub(crate) mod session;

#[cfg(test)]
mod task21_tests;
#[cfg(test)]
mod task22_tests;
#[cfg(test)]
mod task23_tests;
