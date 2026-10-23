//! 单账户共享运行组合：原意图、NOE领取引用和动作前不可变读取。
mod registry;
pub use registry::*;
mod group;
pub use group::*;

mod quantity;

mod economics;
pub use economics::*;

pub mod paper;

mod strategy;
pub use strategy::*;

mod necessary;
pub use necessary::{
    ActionAccountReadV2, BorrowedActionAccountV2, FrozenNecessaryActionAccountV2,
    NecessaryActionAccountReadV2, ObservedTradeApplicationV1, OwnedActionAccountV2,
    conservative_nav,
};

mod learning;
pub use learning::*;

pub mod paper_strategy;
