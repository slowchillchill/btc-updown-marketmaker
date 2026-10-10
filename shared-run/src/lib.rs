//! 单账户共享运行组合：原意图、NOE领取引用和动作前不可变读取。
mod registry;
pub use registry::*;
mod group;
pub use group::*;
mod opening;
pub use opening::*;

mod quantity;

mod economics;
pub use economics::*;

pub mod paper;

mod strategy;
pub use strategy::*;
