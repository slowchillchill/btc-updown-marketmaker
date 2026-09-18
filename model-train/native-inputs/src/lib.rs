//! model-train 所属原生输入组合；不实现公共归约、账本或动作支持集。
mod account;
mod action;
pub use action::{ActionError, CancelFamily, CancelScoreBlock, SelectedAction};
mod cancel;
pub mod exact;
mod market;
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
mod action_tests;
