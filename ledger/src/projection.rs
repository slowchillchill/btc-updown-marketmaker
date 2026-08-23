use contracts::{AccountInstanceKeyV1, ConditionId, ExactDecimal, TokenId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountAdmissionPolicyV1 {
    pub per_market_all_in_commitment_limit_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerOutcomeBalanceV1 {
    pub token_id: TokenId,
    pub balance_shares: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerPlatformConfirmedProjectionV1 {
    pub cash_pusd: ExactDecimal,
    pub available_pusd: ExactDecimal,
    pub principal_encumbrance_pusd: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd: ExactDecimal,
    pub outcome_balances: Vec<LedgerOutcomeBalanceV1>,
    pub maker_rebate_receivable_pusd: ExactDecimal,
    pub taker_rebate_receivable_pusd: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd: ExactDecimal,
    pub position_cost_basis_pusd: ExactDecimal,
    pub fees_pusd: ExactDecimal,
    pub redeemable_receivable_pusd: ExactDecimal,
    pub realized_pnl_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerLocalPendingReservationProjectionV1 {
    pub principal_pusd: ExactDecimal,
    pub immediate_fee_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerMarketProjectionV1 {
    pub condition_id: ConditionId,
    pub principal_encumbrance_pusd: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd: ExactDecimal,
    pub local_pending_principal_pusd: ExactDecimal,
    pub local_pending_immediate_fee_pusd: ExactDecimal,
    pub outcome_balances: Vec<LedgerOutcomeBalanceV1>,
    pub maker_rebate_receivable_pusd: ExactDecimal,
    pub taker_rebate_receivable_pusd: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd: ExactDecimal,
    pub position_cost_basis_pusd: ExactDecimal,
    pub fees_pusd: ExactDecimal,
    pub redeemable_receivable_pusd: ExactDecimal,
    pub realized_pnl_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerProjectionV1 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub ledger_entry_seq: u64,
    pub platform_source_prefix: u64,
    pub ledger_local_prefix: u64,
    pub available_through_utc_ms: i64,
    pub platform_confirmed_projection: LedgerPlatformConfirmedProjectionV1,
    pub local_pending_reservation_projection: LedgerLocalPendingReservationProjectionV1,
    pub market_projections: Vec<LedgerMarketProjectionV1>,
}
