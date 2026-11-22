//! 单账户共享运行组合：原意图、NOE领取引用和动作前不可变读取。
#[macro_use]
mod input_measurement;
#[cfg(feature = "stage-timing")]
pub use input_measurement::{
    InputAssemblyModeWorkV1, InputAssemblyWorkV1, strategy_input_assembly_work_v1,
};
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
mod strategy_records;
pub use strategy_records::*;
mod strategy_visibility;
pub use strategy_visibility::*;

mod necessary;
pub use necessary::{
    ActionAccountReadV2, BorrowedActionAccountV2, FrozenNecessaryActionAccountV2,
    NecessaryActionAccountReadV2, ObservedTradeApplicationV1, OwnedActionAccountV2,
    conservative_nav,
};

mod learning;
pub use learning::*;

pub mod paper_strategy;

#[cfg(test)]
#[path = "../tests/support/observation_scaling.rs"]
mod observation_scaling;

#[cfg(feature = "stage-timing")]
mod work_measurement;
#[cfg(feature = "stage-timing")]
pub use work_measurement::{AccountHistoryVisitsV1, account_history_visits_v1};
