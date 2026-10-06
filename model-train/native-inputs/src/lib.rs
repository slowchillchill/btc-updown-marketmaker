//! model-train 所属现有逐帧原生输入组合；消费公共归约与账本，支持集沿原精确合同。
//! #181 的100ms设备接口与公共GPU后端分工见 docs/100ms-window-inputs.md 及训练设计。
//! 当前模块仍为v7 CPU消费者；不以其存在宣称连续GPU环境或完整活动根恢复已交付。
mod account;
mod support;
pub use support::{NativeActor, encoding_limit, quantity_segment};
mod action;
pub use action::{ActionError, CancelFamily, CancelScoreBlock, SelectedAction};
mod cancel;
pub mod exact;
mod market;
pub use market::{BookLevels, pad_books};
mod packing;
pub use packing::PackedBooks;
pub mod prefix;
mod root;
mod values;
pub use root::{
    AccountInputRow, HiddenKey, HiddenOp, InputRole, NativeActionContext, NativeInputGroup,
    NativeInputRoot,
};
#[derive(Debug)]
pub enum InputError {
    Ledger(ledger::LedgerErrorV1),
    MissingAssociation,
    IncompleteBuyPrefix,
    UnsupportedChoice,
    UnsupportedTick(contracts::economics::EconomicRational),
    Fee(sim_system::buy_cost::MatchingMathErrorV1),
}
impl From<ledger::LedgerErrorV1> for InputError {
    fn from(value: ledger::LedgerErrorV1) -> Self {
        Self::Ledger(value)
    }
}

#[cfg(test)]
mod public_input_tests;

#[cfg(test)]
mod market_tests;

#[cfg(test)]
mod book_tests;

#[cfg(test)]
mod action_tests;

#[cfg(all(test, feature = "continuous-validation"))]
mod continuous_tests;

#[cfg(all(test, feature = "continuous-validation"))]
mod continuous_runtime;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "ort")]
pub mod ort_actor;
