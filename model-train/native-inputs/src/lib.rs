//! model-train 所属原生输入组合；不实现公共归约、账本或动作支持集。
mod account;
mod cancel;
pub mod exact;
mod root;
mod values;
pub use root::{HiddenKey, HiddenOp, InputRole, NativeInputGroup, NativeInputRoot};
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
