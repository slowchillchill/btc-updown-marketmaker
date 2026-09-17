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
    pub position_cost_basis_pusd: ExactDecimal,
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
    pub platform_confirmation_pending_economic_principal_pusd: ExactDecimal,
    pub platform_confirmation_pending_economic_immediate_fee_pusd: ExactDecimal,
    pub platform_confirmation_pending_economic_count: u64,
    pub open_order_count: u64,
    pub pending_fill_count: u64,
    pub cumulative_accepted_principal_pusd: ExactDecimal,
    pub current_all_in_commitment_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerLocalPendingReservationProjectionV1 {
    pub principal_pusd: ExactDecimal,
    pub immediate_fee_pusd: ExactDecimal,
    pub before_send_principal_pusd: ExactDecimal,
    pub before_send_immediate_fee_pusd: ExactDecimal,
    pub before_send_count: u64,
    pub after_send_unknown_principal_pusd: ExactDecimal,
    pub after_send_unknown_immediate_fee_pusd: ExactDecimal,
    pub after_send_unknown_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerMarketProjectionV1 {
    pub condition_id: ConditionId,
    pub principal_encumbrance_pusd: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd: ExactDecimal,
    pub local_pending_principal_pusd: ExactDecimal,
    pub local_pending_immediate_fee_pusd: ExactDecimal,
    pub local_before_send_principal_pusd: ExactDecimal,
    pub local_before_send_immediate_fee_pusd: ExactDecimal,
    pub local_before_send_count: u64,
    pub local_after_send_unknown_principal_pusd: ExactDecimal,
    pub local_after_send_unknown_immediate_fee_pusd: ExactDecimal,
    pub local_after_send_unknown_count: u64,
    pub platform_confirmation_pending_economic_principal_pusd: ExactDecimal,
    pub platform_confirmation_pending_economic_immediate_fee_pusd: ExactDecimal,
    pub platform_confirmation_pending_economic_count: u64,
    pub open_order_count: u64,
    pub pending_fill_count: u64,
    pub outcome_balances: Vec<LedgerOutcomeBalanceV1>,
    pub maker_rebate_receivable_pusd: ExactDecimal,
    pub taker_rebate_receivable_pusd: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd: ExactDecimal,
    pub position_cost_basis_pusd: ExactDecimal,
    pub fees_pusd: ExactDecimal,
    pub redeemable_receivable_pusd: ExactDecimal,
    pub realized_pnl_pusd: ExactDecimal,
    pub cumulative_accepted_principal_pusd: ExactDecimal,
    pub current_all_in_commitment_pusd: ExactDecimal,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerProjectionV2 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub ledger_entry_seq: u64,
    pub platform_source_prefix: u64,
    pub ledger_local_prefix: u64,
    pub available_through_utc_ms: Option<i64>,
    pub platform_confirmed_projection: LedgerPlatformConfirmedProjectionV1,
    pub local_pending_reservation_projection: LedgerLocalPendingReservationProjectionV1,
    pub market_projections: Vec<LedgerMarketProjectionV1>,
}
