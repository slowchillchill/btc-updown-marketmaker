mod account;
pub mod artifacts;
// 提交撮合与确认式交付驱动器在后续组合根接线前必须保持 crate-private，不能为消除告警扩成第九公开操作。
#[allow(dead_code)]
pub mod execution;
pub mod finality;
pub mod latency_profile;
#[allow(dead_code)]
pub(crate) mod market;
pub(crate) mod market_directory;
#[allow(dead_code)]
pub(crate) mod matching;
pub(crate) mod persistence;
pub mod platform_rules;
pub(crate) mod rebate;
mod scenario;
// 会话水位、动作释放和响应完成原语只供公共八方法背后的受信驱动器使用。
#[allow(dead_code)]
pub(crate) mod session;

#[cfg(test)]
mod task21_tests;
#[cfg(test)]
mod task22_tests;
#[cfg(test)]
mod task23_tests;
#[cfg(test)]
mod task24_tests;
#[cfg(test)]
mod task303_tests;
#[cfg(test)]
mod task30_tests;
#[cfg(test)]
mod task31_tests;
#[cfg(test)]
mod task32_tests;
#[cfg(test)]
mod task33_tests;
#[cfg(test)]
mod task34_tests;
#[cfg(test)]
mod task35_tests;
#[cfg(test)]
mod task41_tests;
#[cfg(test)]
mod task42_tests;
