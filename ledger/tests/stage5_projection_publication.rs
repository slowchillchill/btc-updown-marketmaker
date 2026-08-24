use std::fs;
use std::path::Path;

use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformSourceV1, SimCallerRoleV1, TokenId,
};
use ledger::{
    LedgerDurableErrorV1, LedgerLocalPendingReservationProjectionV1, LedgerMarketProjectionV1,
    LedgerOutcomeBalanceV1, LedgerPlatformConfirmedProjectionV1, LedgerProjectionV1,
    publish_ledger_projection_v1, read_ledger_projection_v1,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn decimal(value: &str) -> ExactDecimal {
    ExactDecimal(value.to_owned())
}

fn fixed_nonempty_projection() -> LedgerProjectionV1 {
    let outcome = LedgerOutcomeBalanceV1 {
        token_id: TokenId("stage-5-token".to_owned()),
        balance_shares: decimal("4.00000000"),
        position_cost_basis_pusd: decimal("12.00000000"),
    };
    LedgerProjectionV1 {
        account_instance_key: AccountInstanceKeyV1 {
            run_id: "stage-5-run".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "stage-5-caller".to_owned(),
            account_id: "stage-5-account".to_owned(),
            platform_source: PlatformSourceV1::Simulated,
        },
        ledger_entry_seq: 42,
        platform_source_prefix: 31,
        ledger_local_prefix: 7,
        available_through_utc_ms: 1_798_000_000_123,
        platform_confirmed_projection: LedgerPlatformConfirmedProjectionV1 {
            cash_pusd: decimal("88.00000000"),
            available_pusd: decimal("70.00000000"),
            principal_encumbrance_pusd: decimal("10.00000000"),
            immediate_taker_fee_encumbrance_pusd: decimal("1.00000000"),
            outcome_balances: vec![outcome.clone()],
            maker_rebate_receivable_pusd: decimal("0.50000000"),
            taker_rebate_receivable_pusd: decimal("0.25000000"),
            maker_rebate_locked_remainder_pusd: decimal("0.10000000"),
            taker_rebate_locked_remainder_pusd: decimal("0.05000000"),
            position_cost_basis_pusd: decimal("12.00000000"),
            fees_pusd: decimal("1.00000000"),
            redeemable_receivable_pusd: decimal("0.00000000"),
            realized_pnl_pusd: decimal("2.00000000"),
            platform_confirmation_pending_economic_principal_pusd: decimal("3.00000000"),
            platform_confirmation_pending_economic_immediate_fee_pusd: decimal("0.30000000"),
            platform_confirmation_pending_economic_count: 1,
            open_order_count: 1,
            pending_fill_count: 1,
            cumulative_accepted_principal_pusd: decimal("10.00000000"),
            current_all_in_commitment_pusd: decimal("16.50000000"),
        },
        local_pending_reservation_projection: LedgerLocalPendingReservationProjectionV1 {
            principal_pusd: decimal("2.00000000"),
            immediate_fee_pusd: decimal("0.20000000"),
            before_send_principal_pusd: decimal("0.00000000"),
            before_send_immediate_fee_pusd: decimal("0.00000000"),
            before_send_count: 0,
            after_send_unknown_principal_pusd: decimal("2.00000000"),
            after_send_unknown_immediate_fee_pusd: decimal("0.20000000"),
            after_send_unknown_count: 1,
        },
        market_projections: vec![LedgerMarketProjectionV1 {
            condition_id: ConditionId("stage-5-condition".to_owned()),
            principal_encumbrance_pusd: decimal("10.00000000"),
            immediate_taker_fee_encumbrance_pusd: decimal("1.00000000"),
            local_pending_principal_pusd: decimal("2.00000000"),
            local_pending_immediate_fee_pusd: decimal("0.20000000"),
            local_before_send_principal_pusd: decimal("0.00000000"),
            local_before_send_immediate_fee_pusd: decimal("0.00000000"),
            local_before_send_count: 0,
            local_after_send_unknown_principal_pusd: decimal("2.00000000"),
            local_after_send_unknown_immediate_fee_pusd: decimal("0.20000000"),
            local_after_send_unknown_count: 1,
            platform_confirmation_pending_economic_principal_pusd: decimal("3.00000000"),
            platform_confirmation_pending_economic_immediate_fee_pusd: decimal("0.30000000"),
            platform_confirmation_pending_economic_count: 1,
            open_order_count: 1,
            pending_fill_count: 1,
            outcome_balances: vec![outcome],
            maker_rebate_receivable_pusd: decimal("0.50000000"),
            taker_rebate_receivable_pusd: decimal("0.25000000"),
            maker_rebate_locked_remainder_pusd: decimal("0.10000000"),
            taker_rebate_locked_remainder_pusd: decimal("0.05000000"),
            position_cost_basis_pusd: decimal("12.00000000"),
            fees_pusd: decimal("1.00000000"),
            redeemable_receivable_pusd: decimal("0.00000000"),
            realized_pnl_pusd: decimal("2.00000000"),
            cumulative_accepted_principal_pusd: decimal("10.00000000"),
            current_all_in_commitment_pusd: decimal("16.50000000"),
        }],
    }
}

fn contains_key(value: &Value, expected_key: &str) -> bool {
    match value {
        Value::Object(fields) => {
            fields.contains_key(expected_key)
                || fields
                    .values()
                    .any(|value| contains_key(value, expected_key))
        }
        Value::Array(values) => values.iter().any(|value| contains_key(value, expected_key)),
        _ => false,
    }
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn fixed_nonempty_projection_publishes_and_reads_with_one_complete_byte_identity() {
    let directory = TempDir::new().unwrap();
    let expected = fixed_nonempty_projection();

    let reference = publish_ledger_projection_v1(directory.path(), &expected).unwrap();
    let published_path = Path::new(&reference.location);
    let raw = fs::read(published_path).unwrap();
    let container: Value = serde_json::from_slice(&raw).unwrap();

    assert_eq!(published_path.parent(), Some(directory.path()));
    assert_eq!(reference.ledger_entry_seq, 42);
    assert_eq!(reference.sha256, sha256(&raw));
    assert_eq!(container["version"], 1);
    assert_eq!(container["encoding"], "ledger-projection-json-v1");
    assert!(!contains_key(&container, "sha256"));
    assert_eq!(read_ledger_projection_v1(&reference).unwrap(), expected);
}

#[test]
fn corrupted_projection_bytes_fail_the_outer_sha_boundary() {
    let directory = TempDir::new().unwrap();
    let reference =
        publish_ledger_projection_v1(directory.path(), &fixed_nonempty_projection()).unwrap();
    let mut raw = fs::read(&reference.location).unwrap();
    raw[0] ^= 1;
    fs::write(&reference.location, raw).unwrap();

    assert!(matches!(
        read_ledger_projection_v1(&reference),
        Err(LedgerDurableErrorV1::ProjectionHashMismatch { expected, .. })
            if expected == reference.sha256
    ));
}
