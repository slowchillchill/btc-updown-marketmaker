use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use contracts::PlatformAccountSnapshotV1;
use contracts::{
    ACCOUNT_OPENING_ANCHOR_SCHEMA_V1, AccountInstanceKeyV1, AccountOpeningAnchorV1, ConditionId,
    ExactDecimal, PLATFORM_ACCOUNT_DELTA_SCHEMA_V1, PayoutEntryV1, PlatformAccountBalanceDeltaV1,
    PlatformAccountDeltaPredecessorV1, PlatformAccountDeltaReasonV1, PlatformAccountDeltaV1,
    PlatformOutcomeBalanceDeltaV1, PlatformOutcomeBalanceV1, Sha256Hex, TokenId,
};
use data_pipeline_core::parse_canonical_decimal;
use polymarket_client_sdk_v2::clob::types::Amount;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub const INITIAL_ACCOUNT_STATE_SCHEMA_V1: &str = "initial_account_state_v1";
const INITIAL_ACCOUNT_STATE_HASH_DOMAIN_V1: &str = "SIM_INITIAL_ACCOUNT_STATE_V1";
const PLATFORM_ACCOUNT_SUMMARY_HASH_DOMAIN_V1: &str = "SIM_PLATFORM_ACCOUNT_SUMMARY_V1";
const PLATFORM_OUTCOME_BALANCES_HASH_DOMAIN_V1: &str = "SIM_PLATFORM_ACCOUNT_OUTCOME_BALANCES_V1";
const PLATFORM_ACCOUNT_DELTA_ID_DOMAIN_V1: &str = "SIM_PLATFORM_ACCOUNT_DELTA_ID_V1";
const PLATFORM_ACCOUNT_DELTA_HASH_DOMAIN_V1: &str = "SIM_PLATFORM_ACCOUNT_DELTA_SHA256_V1";
const MICRO_SCALE: u32 = 6;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialAccountStateV1 {
    available_pusd: Decimal,
    sha256: Sha256Hex,
}

impl InitialAccountStateV1 {
    pub fn try_new(available_pusd: Decimal) -> Result<Self, InitialAccountStateErrorV1> {
        if available_pusd.is_sign_negative() {
            return Err(InitialAccountStateErrorV1::NegativeAvailablePusd);
        }
        let available_pusd = Amount::usdc(available_pusd)
            .map_err(|_| InitialAccountStateErrorV1::InvalidPusdPrecision)?
            .as_inner();
        decimal_to_micro_units(available_pusd, "initial available_pusd")
            .map_err(|_| InitialAccountStateErrorV1::UnrepresentableAvailablePusd)?;
        let mut hash = Sha256::new();
        hash.update(INITIAL_ACCOUNT_STATE_HASH_DOMAIN_V1);
        hash.update((INITIAL_ACCOUNT_STATE_SCHEMA_V1.len() as u64).to_be_bytes());
        hash.update(INITIAL_ACCOUNT_STATE_SCHEMA_V1);
        let canonical = available_pusd.normalize().to_string();
        hash.update((canonical.len() as u64).to_be_bytes());
        hash.update(canonical);
        let sha256 = Sha256Hex(format!("{:x}", hash.finalize()));
        Ok(Self {
            available_pusd,
            sha256,
        })
    }

    #[must_use]
    pub fn available_pusd(&self) -> Decimal {
        self.available_pusd
    }

    #[must_use]
    pub fn initial_outcome_balance_shares(&self) -> Decimal {
        Decimal::ZERO
    }

    #[must_use]
    pub fn initial_encumbered_pusd(&self) -> Decimal {
        Decimal::ZERO
    }

    #[must_use]
    pub fn initial_rebate_receivable_pusd(&self) -> Decimal {
        Decimal::ZERO
    }

    #[must_use]
    pub fn sha256(&self) -> &Sha256Hex {
        &self.sha256
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitialAccountStateErrorV1 {
    NegativeAvailablePusd,
    InvalidPusdPrecision,
    UnrepresentableAvailablePusd,
}

impl fmt::Display for InitialAccountStateErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeAvailablePusd => write!(formatter, "初始可用 pUSD 不能为负数"),
            Self::InvalidPusdPrecision => write!(formatter, "初始可用 pUSD 最多保留 6 位小数"),
            Self::UnrepresentableAvailablePusd => {
                write!(formatter, "初始可用 pUSD 超出平台账户定点表示范围")
            }
        }
    }
}

impl std::error::Error for InitialAccountStateErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlatformAccountStateV1 {
    account_instance_key: AccountInstanceKeyV1,
    opening_anchor_id: String,
    opening_anchor_sha256: Sha256Hex,
    cash_pusd_units: u128,
    available_pusd_units: u128,
    principal_encumbrance_pusd_units: u128,
    immediate_taker_fee_encumbrance_pusd_units: u128,
    outcome_balance_units: Arc<BTreeMap<String, u128>>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
    outcome_balances_sha256: Sha256Hex,
    account_summary_sha256: Sha256Hex,
    next_account_transition_seq: u64,
    delta_head: Option<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlatformAccountMovementV1 {
    cash_pusd_units_delta: i128,
    available_pusd_units_delta: i128,
    principal_encumbrance_pusd_units_delta: i128,
    immediate_taker_fee_encumbrance_pusd_units_delta: i128,
    outcome_balance_units_delta: BTreeMap<String, i128>,
    maker_rebate_receivable_pusd_delta: Decimal,
    taker_rebate_receivable_pusd_delta: Decimal,
    maker_rebate_locked_remainder_pusd_delta: Decimal,
    taker_rebate_locked_remainder_pusd_delta: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FillEconomicAmountsV1 {
    pub(crate) base_units: u128,
    pub(crate) quote_units: u128,
    pub(crate) immediate_taker_fee_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRedeemAccountEffectV1 {
    pub(crate) redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    pub(crate) redeemed_pusd: Decimal,
    pub(crate) account_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

impl PlatformAccountMovementV1 {
    fn pusd(
        available_pusd_units_delta: i128,
        principal_encumbrance_pusd_units_delta: i128,
        immediate_taker_fee_encumbrance_pusd_units_delta: i128,
    ) -> Self {
        Self {
            cash_pusd_units_delta: 0,
            available_pusd_units_delta,
            principal_encumbrance_pusd_units_delta,
            immediate_taker_fee_encumbrance_pusd_units_delta,
            outcome_balance_units_delta: BTreeMap::new(),
            maker_rebate_receivable_pusd_delta: Decimal::ZERO,
            taker_rebate_receivable_pusd_delta: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
        }
    }

    fn with_outcome(
        cash_pusd_units_delta: i128,
        available_pusd_units_delta: i128,
        principal_encumbrance_pusd_units_delta: i128,
        immediate_taker_fee_encumbrance_pusd_units_delta: i128,
        token_id: &str,
        outcome_balance_units_delta: i128,
    ) -> Self {
        Self {
            cash_pusd_units_delta,
            available_pusd_units_delta,
            principal_encumbrance_pusd_units_delta,
            immediate_taker_fee_encumbrance_pusd_units_delta,
            outcome_balance_units_delta: BTreeMap::from([(
                token_id.to_owned(),
                outcome_balance_units_delta,
            )]),
            maker_rebate_receivable_pusd_delta: Decimal::ZERO,
            taker_rebate_receivable_pusd_delta: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
        }
    }

    fn rebate(
        cash_pusd_units_delta: i128,
        available_pusd_units_delta: i128,
        maker_rebate_receivable_pusd_delta: Decimal,
        taker_rebate_receivable_pusd_delta: Decimal,
        maker_rebate_locked_remainder_pusd_delta: Decimal,
        taker_rebate_locked_remainder_pusd_delta: Decimal,
    ) -> Self {
        Self {
            cash_pusd_units_delta,
            available_pusd_units_delta,
            principal_encumbrance_pusd_units_delta: 0,
            immediate_taker_fee_encumbrance_pusd_units_delta: 0,
            outcome_balance_units_delta: BTreeMap::new(),
            maker_rebate_receivable_pusd_delta,
            taker_rebate_receivable_pusd_delta,
            maker_rebate_locked_remainder_pusd_delta,
            taker_rebate_locked_remainder_pusd_delta,
        }
    }

    fn public_delta(&self) -> PlatformAccountBalanceDeltaV1 {
        let mut outcome_balance_deltas = self
            .outcome_balance_units_delta
            .iter()
            .map(|(token_id, delta)| PlatformOutcomeBalanceDeltaV1 {
                token_id: TokenId(token_id.clone()),
                balance_shares_delta: exact_signed_micro_units(*delta),
            })
            .collect::<Vec<_>>();
        outcome_balance_deltas
            .sort_by(|left, right| compare_token_ids(&left.token_id.0, &right.token_id.0));
        PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: exact_signed_micro_units(self.cash_pusd_units_delta),
            available_pusd_delta: exact_signed_micro_units(self.available_pusd_units_delta),
            principal_encumbrance_pusd_delta: exact_signed_micro_units(
                self.principal_encumbrance_pusd_units_delta,
            ),
            immediate_taker_fee_encumbrance_pusd_delta: exact_signed_micro_units(
                self.immediate_taker_fee_encumbrance_pusd_units_delta,
            ),
            outcome_balance_deltas,
            maker_rebate_receivable_pusd_delta: exact_decimal(
                self.maker_rebate_receivable_pusd_delta,
            ),
            taker_rebate_receivable_pusd_delta: exact_decimal(
                self.taker_rebate_receivable_pusd_delta,
            ),
            maker_rebate_locked_remainder_pusd_delta: exact_decimal(
                self.maker_rebate_locked_remainder_pusd_delta,
            ),
            taker_rebate_locked_remainder_pusd_delta: exact_decimal(
                self.taker_rebate_locked_remainder_pusd_delta,
            ),
        }
    }
}

#[derive(Serialize)]
struct PlatformAccountSummaryMaterialV1<'a> {
    account_instance_key: &'a AccountInstanceKeyV1,
    cash_pusd: ExactDecimal,
    available_pusd: ExactDecimal,
    principal_encumbrance_pusd: ExactDecimal,
    immediate_taker_fee_encumbrance_pusd: ExactDecimal,
    outcome_balances_sha256: &'a Sha256Hex,
    maker_rebate_receivable_pusd: ExactDecimal,
    taker_rebate_receivable_pusd: ExactDecimal,
    maker_rebate_locked_remainder_pusd: ExactDecimal,
    taker_rebate_locked_remainder_pusd: ExactDecimal,
}

#[derive(Serialize)]
struct PlatformAccountDeltaIdMaterialV1<'a> {
    schema_version: &'static str,
    account_instance_key: &'a AccountInstanceKeyV1,
    account_transition_seq: u64,
    predecessor: &'a PlatformAccountDeltaPredecessorV1,
    reason: PlatformAccountDeltaReasonV1,
    reason_identity: &'a str,
    condition_id: &'a Option<ConditionId>,
    platform_effective_at_utc_ms: i64,
}

#[derive(Serialize)]
struct PlatformAccountDeltaHashMaterialV1<'a> {
    schema_version: &'static str,
    delta_id: &'a str,
    account_instance_key: &'a AccountInstanceKeyV1,
    account_transition_seq: u64,
    predecessor: &'a PlatformAccountDeltaPredecessorV1,
    reason: PlatformAccountDeltaReasonV1,
    reason_identity: &'a str,
    condition_id: &'a Option<ConditionId>,
    platform_effective_at_utc_ms: i64,
    balance_delta: &'a PlatformAccountBalanceDeltaV1,
    account_summary_before_sha256: &'a Sha256Hex,
    account_summary_after_sha256: &'a Sha256Hex,
}

impl PlatformAccountStateV1 {
    pub(crate) fn account_instance_key(&self) -> &AccountInstanceKeyV1 {
        &self.account_instance_key
    }

    pub(crate) fn try_from_opening_anchor(
        anchor: &AccountOpeningAnchorV1,
    ) -> Result<Self, PlatformAccountErrorV1> {
        validate_opening_anchor(anchor)?;
        let initial = &anchor.initial_platform_account;
        let cash_pusd_units = parse_micro_units(&initial.cash_pusd, "initial cash_pusd")?;
        let available_pusd_units =
            parse_micro_units(&initial.available_pusd, "initial available_pusd")?;
        let principal_encumbrance_pusd_units = parse_micro_units(
            &initial.principal_encumbrance_pusd,
            "initial principal_encumbrance_pusd",
        )?;
        let immediate_taker_fee_encumbrance_pusd_units = parse_micro_units(
            &initial.immediate_taker_fee_encumbrance_pusd,
            "initial immediate_taker_fee_encumbrance_pusd",
        )?;
        if cash_pusd_units != available_pusd_units
            || principal_encumbrance_pusd_units != 0
            || immediate_taker_fee_encumbrance_pusd_units != 0
        {
            return Err(PlatformAccountErrorV1::InvalidOpeningAnchor(
                "初始现金、可用资金或占用分桶非法",
            ));
        }
        let mut outcome_balance_units = BTreeMap::new();
        for outcome in &initial.outcome_balances {
            if !is_stable_text(&outcome.token_id.0)
                || outcome_balance_units
                    .insert(
                        outcome.token_id.0.clone(),
                        parse_micro_units(&outcome.balance_shares, "initial outcome balance")?,
                    )
                    .is_some()
            {
                return Err(PlatformAccountErrorV1::InvalidOpeningAnchor(
                    "初始结果代币身份为空或重复",
                ));
            }
        }
        if outcome_balance_units.values().any(|balance| *balance != 0) {
            return Err(PlatformAccountErrorV1::InvalidOpeningAnchor(
                "初始结果代币余额必须为零",
            ));
        }
        let maker_rebate_receivable_pusd = parse_nonnegative_decimal(
            &initial.maker_rebate_receivable_pusd,
            "initial maker rebate receivable",
        )?;
        let taker_rebate_receivable_pusd = parse_nonnegative_decimal(
            &initial.taker_rebate_receivable_pusd,
            "initial taker rebate receivable",
        )?;
        let maker_rebate_locked_remainder_pusd = parse_nonnegative_decimal(
            &initial.maker_rebate_locked_remainder_pusd,
            "initial maker rebate locked remainder",
        )?;
        let taker_rebate_locked_remainder_pusd = parse_nonnegative_decimal(
            &initial.taker_rebate_locked_remainder_pusd,
            "initial taker rebate locked remainder",
        )?;
        if !maker_rebate_receivable_pusd.is_zero()
            || !taker_rebate_receivable_pusd.is_zero()
            || !maker_rebate_locked_remainder_pusd.is_zero()
            || !taker_rebate_locked_remainder_pusd.is_zero()
        {
            return Err(PlatformAccountErrorV1::InvalidOpeningAnchor(
                "初始返佣分桶必须为零",
            ));
        }
        let outcome_balances_sha256 = outcome_balances_sha256_from_units(&outcome_balance_units)?;
        let account_summary_sha256 = platform_account_summary_sha256(
            &anchor.account_instance_key,
            cash_pusd_units,
            available_pusd_units,
            principal_encumbrance_pusd_units,
            immediate_taker_fee_encumbrance_pusd_units,
            &outcome_balances_sha256,
            maker_rebate_receivable_pusd,
            taker_rebate_receivable_pusd,
            maker_rebate_locked_remainder_pusd,
            taker_rebate_locked_remainder_pusd,
        )?;
        Ok(Self {
            account_instance_key: anchor.account_instance_key.clone(),
            opening_anchor_id: anchor.anchor_id.clone(),
            opening_anchor_sha256: anchor.anchor_sha256.clone(),
            cash_pusd_units,
            available_pusd_units,
            principal_encumbrance_pusd_units,
            immediate_taker_fee_encumbrance_pusd_units,
            outcome_balance_units: Arc::new(outcome_balance_units),
            maker_rebate_receivable_pusd,
            taker_rebate_receivable_pusd,
            maker_rebate_locked_remainder_pusd,
            taker_rebate_locked_remainder_pusd,
            outcome_balances_sha256,
            account_summary_sha256,
            next_account_transition_seq: 1,
            delta_head: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> PlatformAccountSnapshotV1 {
        let mut outcome_balances = self
            .outcome_balance_units
            .iter()
            .map(|(token_id, balance)| PlatformOutcomeBalanceV1 {
                token_id: TokenId(token_id.clone()),
                balance_shares: exact_micro_units(*balance),
            })
            .collect::<Vec<_>>();
        outcome_balances
            .sort_by(|left, right| compare_token_ids(&left.token_id.0, &right.token_id.0));
        PlatformAccountSnapshotV1 {
            cash_pusd: exact_micro_units(self.cash_pusd_units),
            available_pusd: exact_micro_units(self.available_pusd_units),
            principal_encumbrance_pusd: exact_micro_units(self.principal_encumbrance_pusd_units),
            immediate_taker_fee_encumbrance_pusd: exact_micro_units(
                self.immediate_taker_fee_encumbrance_pusd_units,
            ),
            outcome_balances,
            maker_rebate_receivable_pusd: exact_decimal(self.maker_rebate_receivable_pusd),
            taker_rebate_receivable_pusd: exact_decimal(self.taker_rebate_receivable_pusd),
            maker_rebate_locked_remainder_pusd: exact_decimal(
                self.maker_rebate_locked_remainder_pusd,
            ),
            taker_rebate_locked_remainder_pusd: exact_decimal(
                self.taker_rebate_locked_remainder_pusd,
            ),
        }
    }

    pub(crate) fn available_pusd_units(&self) -> u128 {
        self.available_pusd_units
    }

    pub(crate) fn available_pusd(&self) -> Decimal {
        decimal_from_micro_units(self.available_pusd_units)
    }

    pub(crate) fn principal_encumbrance_pusd(&self) -> Decimal {
        decimal_from_micro_units(self.principal_encumbrance_pusd_units)
    }

    pub(crate) fn immediate_taker_fee_encumbrance_pusd(&self) -> Decimal {
        decimal_from_micro_units(self.immediate_taker_fee_encumbrance_pusd_units)
    }

    #[cfg(test)]
    pub(crate) fn account_summary_sha256(&self) -> &Sha256Hex {
        &self.account_summary_sha256
    }

    #[cfg(test)]
    pub(crate) fn outcome_balances_sha256(&self) -> &Sha256Hex {
        &self.outcome_balances_sha256
    }

    #[cfg(test)]
    pub(crate) fn shares_outcome_balance_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.outcome_balance_units, &other.outcome_balance_units)
    }

    #[cfg(test)]
    pub(crate) fn next_account_transition_seq(&self) -> u64 {
        self.next_account_transition_seq
    }

    pub(crate) fn apply_order_accepted(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        principal_encumbrance_pusd: Decimal,
        immediate_taker_fee_encumbrance_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        let principal_units = decimal_to_micro_units(
            principal_encumbrance_pusd,
            "order accepted principal encumbrance",
        )?;
        let fee_units = decimal_to_micro_units(
            immediate_taker_fee_encumbrance_pusd,
            "order accepted fee encumbrance",
        )?;
        if principal_units == 0 {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "订单接受本金占用必须为正数",
            ));
        }
        let principal_delta = positive_i128(principal_units)?;
        let fee_delta = positive_i128(fee_units)?;
        let total = principal_delta
            .checked_add(fee_delta)
            .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        self.apply_transition(
            PlatformAccountDeltaReasonV1::OrderAcceptedEncumbranceApplied,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::pusd(-total, principal_delta, fee_delta),
        )
    }

    pub(crate) fn apply_fill_finality_economic(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        token_id: &str,
        platform_effective_at_utc_ms: i64,
        amounts: FillEconomicAmountsV1,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if amounts.base_units == 0 || amounts.quote_units == 0 {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "成交 base 与 quote 必须为正数",
            ));
        }
        if !is_stable_text(token_id) {
            return Err(PlatformAccountErrorV1::InvalidTransitionIdentity);
        }
        let total_units = amounts
            .quote_units
            .checked_add(amounts.immediate_taker_fee_units)
            .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        let base_delta = positive_i128(amounts.base_units)?;
        let quote_delta = positive_i128(amounts.quote_units)?;
        let fee_delta = positive_i128(amounts.immediate_taker_fee_units)?;
        let total_delta = positive_i128(total_units)?;
        self.apply_transition(
            PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::with_outcome(
                -total_delta,
                0,
                -quote_delta,
                -fee_delta,
                token_id,
                base_delta,
            ),
        )
    }

    pub(crate) fn apply_fill_finality_unapplied_failure_release(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        quote_units: u128,
        immediate_taker_fee_units: u128,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if quote_units == 0 {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "未应用失败成交 quote 必须为正数",
            ));
        }
        let total_units = quote_units
            .checked_add(immediate_taker_fee_units)
            .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        let quote_delta = positive_i128(quote_units)?;
        let fee_delta = positive_i128(immediate_taker_fee_units)?;
        let total_delta = positive_i128(total_units)?;
        self.apply_transition(
            PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::pusd(total_delta, -quote_delta, -fee_delta),
        )
    }

    pub(crate) fn apply_fill_finality_compensation(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        token_id: &str,
        platform_effective_at_utc_ms: i64,
        amounts: FillEconomicAmountsV1,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if amounts.base_units == 0 || amounts.quote_units == 0 {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "补偿 base 与 quote 必须为正数",
            ));
        }
        if !is_stable_text(token_id) {
            return Err(PlatformAccountErrorV1::InvalidTransitionIdentity);
        }
        let total_units = amounts
            .quote_units
            .checked_add(amounts.immediate_taker_fee_units)
            .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        let base_delta = positive_i128(amounts.base_units)?;
        let total_delta = positive_i128(total_units)?;
        self.apply_transition(
            PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::with_outcome(
                total_delta,
                total_delta,
                0,
                0,
                token_id,
                -base_delta,
            ),
        )
    }

    pub(crate) fn apply_fak_unspent_release(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV1::FakUnspentEncumbranceReleased,
            reason_identity,
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )
    }

    pub(crate) fn apply_caller_cancel_release(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV1::CallerCancellationEncumbranceReleased,
            reason_identity,
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )
    }

    pub(crate) fn apply_gtd_expiration_release(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV1::GtdExpirationEncumbranceReleased,
            reason_identity,
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )
    }

    pub(crate) fn apply_venue_dust_release(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV1::VenueDustEncumbranceReleased,
            reason_identity,
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )?
        .ok_or(PlatformAccountErrorV1::InvalidAmount(
            "平台小额余量释放必须为正数",
        ))
    }

    pub(crate) fn apply_market_resolution_encumbrance_release(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        released_open_quote_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased,
            reason_identity,
            condition_id,
            platform_effective_at_utc_ms,
            released_open_quote_pusd,
        )?
        .ok_or(PlatformAccountErrorV1::InvalidAmount(
            "市场判定开放订单余量释放必须为正数",
        ))
    }

    pub(crate) fn apply_auto_redeem(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        payout_vector: &[PayoutEntryV1],
    ) -> Result<AutoRedeemAccountEffectV1, PlatformAccountErrorV1> {
        if payout_vector.len() != 2 {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "自动赎回必须包含两个结果代币",
            ));
        }

        let mut redeemed_outcome_balances = Vec::with_capacity(2);
        let mut outcome_balance_units_delta = BTreeMap::new();
        let mut payout_pusd = Decimal::ZERO;
        for entry in payout_vector {
            let token_id = &entry.asset.token_id.0;
            let balance_units = self
                .outcome_balance_units
                .get(token_id)
                .copied()
                .unwrap_or(0);
            let balance_shares = decimal_from_micro_units(balance_units);
            redeemed_outcome_balances.push(PlatformOutcomeBalanceV1 {
                token_id: entry.asset.token_id.clone(),
                balance_shares: exact_micro_units(balance_units),
            });
            if balance_units > 0 {
                outcome_balance_units_delta
                    .insert(token_id.clone(), -positive_i128(balance_units)?);
            }
            let payout_per_share = parse_canonical_decimal(
                &entry.payout_pusd_per_share,
                "auto redeem payout_pusd_per_share",
            )
            .map_err(|_| PlatformAccountErrorV1::InvalidAmount("自动赎回兑付向量"))?;
            let token_payout = balance_shares
                .checked_mul(payout_per_share)
                .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
            payout_pusd = payout_pusd
                .checked_add(token_payout)
                .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        }
        let payout_pusd = payout_pusd.normalize();

        let account_delta = if outcome_balance_units_delta.is_empty() {
            None
        } else {
            let payout_units = decimal_to_micro_units(payout_pusd, "auto redeem payout")?;
            let payout_delta = positive_i128(payout_units)?;
            Some(self.apply_transition(
                PlatformAccountDeltaReasonV1::AutoRedeemApplied,
                reason_identity,
                Some(condition_id),
                platform_effective_at_utc_ms,
                PlatformAccountMovementV1 {
                    cash_pusd_units_delta: payout_delta,
                    available_pusd_units_delta: payout_delta,
                    principal_encumbrance_pusd_units_delta: 0,
                    immediate_taker_fee_encumbrance_pusd_units_delta: 0,
                    outcome_balance_units_delta,
                    maker_rebate_receivable_pusd_delta: Decimal::ZERO,
                    taker_rebate_receivable_pusd_delta: Decimal::ZERO,
                    maker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
                    taker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
                },
            )?)
        };

        Ok(AutoRedeemAccountEffectV1 {
            redeemed_outcome_balances,
            redeemed_pusd: payout_pusd,
            account_delta,
        })
    }

    pub(crate) fn apply_maker_rebate_accrual_finalized(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        accrual_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if accrual_pusd <= Decimal::ZERO {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "maker 返佣应计必须为正数",
            ));
        }
        self.apply_transition(
            PlatformAccountDeltaReasonV1::MakerRebateAccrualFinalized,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::rebate(
                0,
                0,
                accrual_pusd,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        )
    }

    pub(crate) fn apply_taker_rebate_accrual_applied(
        &mut self,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        accrual_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if accrual_pusd <= Decimal::ZERO {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "taker 返佣应计必须为正数",
            ));
        }
        self.apply_transition(
            PlatformAccountDeltaReasonV1::TakerRebateAccrualApplied,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::rebate(
                0,
                0,
                Decimal::ZERO,
                accrual_pusd,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        )
    }

    pub(crate) fn apply_maker_rebate_day_finalized(
        &mut self,
        reason_identity: &str,
        platform_effective_at_utc_ms: i64,
        locked_remainder_pusd: Decimal,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        self.apply_rebate_day_finalized(
            PlatformAccountDeltaReasonV1::MakerRebateDayFinalized,
            reason_identity,
            platform_effective_at_utc_ms,
            locked_remainder_pusd,
            true,
        )
    }

    pub(crate) fn apply_taker_rebate_day_finalized(
        &mut self,
        reason_identity: &str,
        platform_effective_at_utc_ms: i64,
        locked_remainder_pusd: Decimal,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        self.apply_rebate_day_finalized(
            PlatformAccountDeltaReasonV1::TakerRebateDayFinalized,
            reason_identity,
            platform_effective_at_utc_ms,
            locked_remainder_pusd,
            false,
        )
    }

    pub(crate) fn apply_maker_rebate_paid(
        &mut self,
        reason_identity: &str,
        platform_effective_at_utc_ms: i64,
        paid_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        self.apply_rebate_paid(
            PlatformAccountDeltaReasonV1::MakerRebatePaid,
            reason_identity,
            platform_effective_at_utc_ms,
            paid_pusd,
            true,
        )
    }

    pub(crate) fn apply_taker_rebate_paid(
        &mut self,
        reason_identity: &str,
        platform_effective_at_utc_ms: i64,
        paid_pusd: Decimal,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        self.apply_rebate_paid(
            PlatformAccountDeltaReasonV1::TakerRebatePaid,
            reason_identity,
            platform_effective_at_utc_ms,
            paid_pusd,
            false,
        )
    }

    fn apply_rebate_day_finalized(
        &mut self,
        reason: PlatformAccountDeltaReasonV1,
        reason_identity: &str,
        platform_effective_at_utc_ms: i64,
        locked_remainder_pusd: Decimal,
        maker: bool,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        if locked_remainder_pusd.is_sign_negative() {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "返佣日结锁定余数不能为负数",
            ));
        }
        if locked_remainder_pusd.is_zero() {
            return Ok(None);
        }
        let receivable_delta = Decimal::ZERO
            .checked_sub(locked_remainder_pusd)
            .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        let movement = if maker {
            PlatformAccountMovementV1::rebate(
                0,
                0,
                receivable_delta,
                Decimal::ZERO,
                locked_remainder_pusd,
                Decimal::ZERO,
            )
        } else {
            PlatformAccountMovementV1::rebate(
                0,
                0,
                Decimal::ZERO,
                receivable_delta,
                Decimal::ZERO,
                locked_remainder_pusd,
            )
        };
        self.apply_transition(
            reason,
            reason_identity,
            None,
            platform_effective_at_utc_ms,
            movement,
        )
        .map(Some)
    }

    fn apply_rebate_paid(
        &mut self,
        reason: PlatformAccountDeltaReasonV1,
        reason_identity: &str,
        platform_effective_at_utc_ms: i64,
        paid_pusd: Decimal,
        maker: bool,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if paid_pusd <= Decimal::ZERO || paid_pusd.normalize().scale() > 4 {
            return Err(PlatformAccountErrorV1::InvalidAmount(
                "返佣付款必须为正数且最多保留 4 位小数",
            ));
        }
        let paid_units = decimal_to_micro_units(paid_pusd, "rebate payment")?;
        let paid_units_delta = positive_i128(paid_units)?;
        let receivable_delta = Decimal::ZERO
            .checked_sub(paid_pusd)
            .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
        let movement = if maker {
            PlatformAccountMovementV1::rebate(
                paid_units_delta,
                paid_units_delta,
                receivable_delta,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )
        } else {
            PlatformAccountMovementV1::rebate(
                paid_units_delta,
                paid_units_delta,
                Decimal::ZERO,
                receivable_delta,
                Decimal::ZERO,
                Decimal::ZERO,
            )
        };
        self.apply_transition(
            reason,
            reason_identity,
            None,
            platform_effective_at_utc_ms,
            movement,
        )
    }

    fn apply_encumbrance_release(
        &mut self,
        reason: PlatformAccountDeltaReasonV1,
        reason_identity: &str,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<Arc<PlatformAccountDeltaV1>>, PlatformAccountErrorV1> {
        let released_units = decimal_to_micro_units(released_pusd, "encumbrance release")?;
        if released_units == 0 {
            return Ok(None);
        }
        let released_delta = positive_i128(released_units)?;
        self.apply_transition(
            reason,
            reason_identity,
            Some(condition_id),
            platform_effective_at_utc_ms,
            PlatformAccountMovementV1::pusd(released_delta, -released_delta, 0),
        )
        .map(Some)
    }

    fn apply_transition(
        &mut self,
        reason: PlatformAccountDeltaReasonV1,
        reason_identity: &str,
        condition_id: Option<&str>,
        platform_effective_at_utc_ms: i64,
        movement: PlatformAccountMovementV1,
    ) -> Result<Arc<PlatformAccountDeltaV1>, PlatformAccountErrorV1> {
        if !is_stable_text(reason_identity)
            || condition_id.is_some_and(|condition| !is_stable_text(condition))
        {
            return Err(PlatformAccountErrorV1::InvalidTransitionIdentity);
        }
        let condition_id = condition_id.map(|value| ConditionId(value.to_owned()));
        let public_movement = movement.public_delta();
        if let Some(existing) = self
            .delta_head
            .as_ref()
            .map(Arc::clone)
            .filter(|delta| delta.reason_identity == reason_identity)
        {
            if existing.reason == reason
                && existing.condition_id == condition_id
                && existing.platform_effective_at_utc_ms == platform_effective_at_utc_ms
                && existing.balance_delta == public_movement
            {
                return Ok(existing);
            }
            return Err(PlatformAccountErrorV1::TransitionIdentityConflict(
                reason_identity.to_owned(),
            ));
        }

        let next_cash_pusd_units = apply_signed_units(
            self.cash_pusd_units,
            movement.cash_pusd_units_delta,
            "cash_pusd",
        )?;
        let next_available_pusd_units = apply_signed_units(
            self.available_pusd_units,
            movement.available_pusd_units_delta,
            "available_pusd",
        )?;
        let next_principal_encumbrance_pusd_units = apply_signed_units(
            self.principal_encumbrance_pusd_units,
            movement.principal_encumbrance_pusd_units_delta,
            "principal_encumbrance_pusd",
        )?;
        let next_immediate_taker_fee_encumbrance_pusd_units = apply_signed_units(
            self.immediate_taker_fee_encumbrance_pusd_units,
            movement.immediate_taker_fee_encumbrance_pusd_units_delta,
            "immediate_taker_fee_encumbrance_pusd",
        )?;
        let next_outcome_balance_units = if movement.outcome_balance_units_delta.is_empty() {
            None
        } else {
            let mut balances = (*self.outcome_balance_units).clone();
            for (token_id, delta) in &movement.outcome_balance_units_delta {
                let current = balances.get(token_id).copied().unwrap_or(0);
                let next = apply_signed_units(current, *delta, "outcome_balance")?;
                balances.insert(token_id.clone(), next);
            }
            Some(balances)
        };
        let next_maker_rebate_receivable_pusd = checked_nonnegative_decimal_add(
            self.maker_rebate_receivable_pusd,
            movement.maker_rebate_receivable_pusd_delta,
            "maker_rebate_receivable_pusd",
        )?;
        let next_taker_rebate_receivable_pusd = checked_nonnegative_decimal_add(
            self.taker_rebate_receivable_pusd,
            movement.taker_rebate_receivable_pusd_delta,
            "taker_rebate_receivable_pusd",
        )?;
        let next_maker_rebate_locked_remainder_pusd = checked_nonnegative_decimal_add(
            self.maker_rebate_locked_remainder_pusd,
            movement.maker_rebate_locked_remainder_pusd_delta,
            "maker_rebate_locked_remainder_pusd",
        )?;
        let next_taker_rebate_locked_remainder_pusd = checked_nonnegative_decimal_add(
            self.taker_rebate_locked_remainder_pusd,
            movement.taker_rebate_locked_remainder_pusd_delta,
            "taker_rebate_locked_remainder_pusd",
        )?;

        let account_summary_before_sha256 = self.account_summary_sha256.clone();
        let next_outcome_balances_sha256 = match &next_outcome_balance_units {
            Some(balances) => outcome_balances_sha256_from_units(balances)?,
            None => self.outcome_balances_sha256.clone(),
        };
        let account_summary_after_sha256 = platform_account_summary_sha256(
            &self.account_instance_key,
            next_cash_pusd_units,
            next_available_pusd_units,
            next_principal_encumbrance_pusd_units,
            next_immediate_taker_fee_encumbrance_pusd_units,
            &next_outcome_balances_sha256,
            next_maker_rebate_receivable_pusd,
            next_taker_rebate_receivable_pusd,
            next_maker_rebate_locked_remainder_pusd,
            next_taker_rebate_locked_remainder_pusd,
        )?;
        let account_transition_seq = self.next_account_transition_seq;
        let next_account_transition_seq = account_transition_seq
            .checked_add(1)
            .ok_or(PlatformAccountErrorV1::SequenceOverflow)?;
        let predecessor = self.delta_head.as_ref().map_or_else(
            || PlatformAccountDeltaPredecessorV1::OpeningAnchor {
                anchor_id: self.opening_anchor_id.clone(),
                anchor_sha256: self.opening_anchor_sha256.clone(),
            },
            |head| PlatformAccountDeltaPredecessorV1::AccountDelta {
                delta_id: head.delta_id.clone(),
                delta_sha256: head.delta_sha256.clone(),
                account_transition_seq: head.account_transition_seq,
            },
        );
        let id_material = PlatformAccountDeltaIdMaterialV1 {
            schema_version: PLATFORM_ACCOUNT_DELTA_SCHEMA_V1,
            account_instance_key: &self.account_instance_key,
            account_transition_seq,
            predecessor: &predecessor,
            reason,
            reason_identity,
            condition_id: &condition_id,
            platform_effective_at_utc_ms,
        };
        let delta_id = hash_json(PLATFORM_ACCOUNT_DELTA_ID_DOMAIN_V1, &id_material)?.0;
        let hash_material = PlatformAccountDeltaHashMaterialV1 {
            schema_version: PLATFORM_ACCOUNT_DELTA_SCHEMA_V1,
            delta_id: &delta_id,
            account_instance_key: &self.account_instance_key,
            account_transition_seq,
            predecessor: &predecessor,
            reason,
            reason_identity,
            condition_id: &condition_id,
            platform_effective_at_utc_ms,
            balance_delta: &public_movement,
            account_summary_before_sha256: &account_summary_before_sha256,
            account_summary_after_sha256: &account_summary_after_sha256,
        };
        let delta_sha256 = hash_json(PLATFORM_ACCOUNT_DELTA_HASH_DOMAIN_V1, &hash_material)?;
        let delta = Arc::new(PlatformAccountDeltaV1 {
            schema_version: PLATFORM_ACCOUNT_DELTA_SCHEMA_V1.to_owned(),
            delta_id,
            delta_sha256,
            account_instance_key: self.account_instance_key.clone(),
            account_transition_seq,
            predecessor,
            reason,
            reason_identity: reason_identity.to_owned(),
            condition_id,
            platform_effective_at_utc_ms,
            balance_delta: public_movement,
            account_summary_before_sha256,
            account_summary_after_sha256: account_summary_after_sha256.clone(),
        });
        self.cash_pusd_units = next_cash_pusd_units;
        self.available_pusd_units = next_available_pusd_units;
        self.principal_encumbrance_pusd_units = next_principal_encumbrance_pusd_units;
        self.immediate_taker_fee_encumbrance_pusd_units =
            next_immediate_taker_fee_encumbrance_pusd_units;
        if let Some(balances) = next_outcome_balance_units {
            self.outcome_balance_units = Arc::new(balances);
        }
        self.maker_rebate_receivable_pusd = next_maker_rebate_receivable_pusd;
        self.taker_rebate_receivable_pusd = next_taker_rebate_receivable_pusd;
        self.maker_rebate_locked_remainder_pusd = next_maker_rebate_locked_remainder_pusd;
        self.taker_rebate_locked_remainder_pusd = next_taker_rebate_locked_remainder_pusd;
        self.outcome_balances_sha256 = next_outcome_balances_sha256;
        self.account_summary_sha256 = account_summary_after_sha256;
        self.next_account_transition_seq = next_account_transition_seq;
        self.delta_head = Some(Arc::clone(&delta));
        Ok(delta)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PlatformAccountErrorV1 {
    InvalidOpeningAnchor(&'static str),
    InvalidAmount(&'static str),
    InvalidTransitionIdentity,
    TransitionIdentityConflict(String),
    BalanceUnderflow(&'static str),
    ArithmeticOverflow,
    SequenceOverflow,
    Serialization(String),
}

impl fmt::Display for PlatformAccountErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOpeningAnchor(message) => write!(formatter, "平台开户锚点非法：{message}"),
            Self::InvalidAmount(message) => write!(formatter, "平台账户金额非法：{message}"),
            Self::InvalidTransitionIdentity => write!(formatter, "平台账户变化身份非法"),
            Self::TransitionIdentityConflict(identity) => {
                write!(formatter, "平台账户变化身份冲突：{identity}")
            }
            Self::BalanceUnderflow(bucket) => write!(formatter, "平台账户分桶下溢：{bucket}"),
            Self::ArithmeticOverflow => write!(formatter, "平台账户金额运算溢出"),
            Self::SequenceOverflow => write!(formatter, "平台账户变化序号溢出"),
            Self::Serialization(message) => write!(formatter, "平台账户规范序列化失败：{message}"),
        }
    }
}

impl std::error::Error for PlatformAccountErrorV1 {}

fn validate_opening_anchor(anchor: &AccountOpeningAnchorV1) -> Result<(), PlatformAccountErrorV1> {
    if anchor.schema_version != ACCOUNT_OPENING_ANCHOR_SCHEMA_V1
        || !is_stable_text(&anchor.anchor_id)
        || !is_sha256_hex(&anchor.anchor_sha256.0)
        || !is_stable_text(&anchor.session_id)
        || !is_stable_text(&anchor.account_instance_key.run_id)
        || !is_stable_text(&anchor.account_instance_key.caller_instance_id)
        || !is_stable_text(&anchor.account_instance_key.account_id)
    {
        return Err(PlatformAccountErrorV1::InvalidOpeningAnchor(
            "版本或身份非法",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn platform_account_summary_sha256(
    account_instance_key: &AccountInstanceKeyV1,
    cash_pusd_units: u128,
    available_pusd_units: u128,
    principal_encumbrance_pusd_units: u128,
    immediate_taker_fee_encumbrance_pusd_units: u128,
    outcome_balances_sha256: &Sha256Hex,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
) -> Result<Sha256Hex, PlatformAccountErrorV1> {
    hash_json(
        PLATFORM_ACCOUNT_SUMMARY_HASH_DOMAIN_V1,
        &PlatformAccountSummaryMaterialV1 {
            account_instance_key,
            cash_pusd: exact_micro_units(cash_pusd_units),
            available_pusd: exact_micro_units(available_pusd_units),
            principal_encumbrance_pusd: exact_micro_units(principal_encumbrance_pusd_units),
            immediate_taker_fee_encumbrance_pusd: exact_micro_units(
                immediate_taker_fee_encumbrance_pusd_units,
            ),
            outcome_balances_sha256,
            maker_rebate_receivable_pusd: exact_decimal(maker_rebate_receivable_pusd),
            taker_rebate_receivable_pusd: exact_decimal(taker_rebate_receivable_pusd),
            maker_rebate_locked_remainder_pusd: exact_decimal(maker_rebate_locked_remainder_pusd),
            taker_rebate_locked_remainder_pusd: exact_decimal(taker_rebate_locked_remainder_pusd),
        },
    )
}

fn outcome_balances_sha256_from_units(
    balances: &BTreeMap<String, u128>,
) -> Result<Sha256Hex, PlatformAccountErrorV1> {
    let mut canonical = balances
        .iter()
        .map(|(token_id, balance)| PlatformOutcomeBalanceV1 {
            token_id: TokenId(token_id.clone()),
            balance_shares: exact_micro_units(*balance),
        })
        .collect::<Vec<_>>();
    canonical.sort_by(|left, right| compare_token_ids(&left.token_id.0, &right.token_id.0));
    hash_json(PLATFORM_OUTCOME_BALANCES_HASH_DOMAIN_V1, &canonical)
}

fn hash_json<T: Serialize>(domain: &str, value: &T) -> Result<Sha256Hex, PlatformAccountErrorV1> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| PlatformAccountErrorV1::Serialization(error.to_string()))?;
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(Sha256Hex(format!("{:x}", hash.finalize())))
}

fn parse_micro_units(
    value: &ExactDecimal,
    field: &'static str,
) -> Result<u128, PlatformAccountErrorV1> {
    let decimal = parse_canonical_decimal(value, field)
        .map_err(|_| PlatformAccountErrorV1::InvalidOpeningAnchor(field))?;
    decimal_to_micro_units(decimal, field)
}

fn parse_nonnegative_decimal(
    value: &ExactDecimal,
    field: &'static str,
) -> Result<Decimal, PlatformAccountErrorV1> {
    let decimal = parse_canonical_decimal(value, field)
        .map_err(|_| PlatformAccountErrorV1::InvalidOpeningAnchor(field))?;
    if decimal.is_sign_negative() {
        return Err(PlatformAccountErrorV1::InvalidOpeningAnchor(field));
    }
    Ok(decimal)
}

fn decimal_to_micro_units(
    value: Decimal,
    field: &'static str,
) -> Result<u128, PlatformAccountErrorV1> {
    if value.is_sign_negative() {
        return Err(PlatformAccountErrorV1::InvalidAmount(field));
    }
    let normalized = value.normalize();
    if normalized.scale() > MICRO_SCALE {
        return Err(PlatformAccountErrorV1::InvalidAmount(field));
    }
    let mantissa = u128::try_from(normalized.mantissa())
        .map_err(|_| PlatformAccountErrorV1::InvalidAmount(field))?;
    let units = mantissa
        .checked_mul(10_u128.pow(MICRO_SCALE - normalized.scale()))
        .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
    let units_i128 =
        i128::try_from(units).map_err(|_| PlatformAccountErrorV1::ArithmeticOverflow)?;
    Decimal::try_from_i128_with_scale(units_i128, MICRO_SCALE)
        .map_err(|_| PlatformAccountErrorV1::ArithmeticOverflow)?;
    Ok(units)
}

fn positive_i128(value: u128) -> Result<i128, PlatformAccountErrorV1> {
    let value = i128::try_from(value).map_err(|_| PlatformAccountErrorV1::ArithmeticOverflow)?;
    Decimal::try_from_i128_with_scale(value, MICRO_SCALE)
        .map_err(|_| PlatformAccountErrorV1::ArithmeticOverflow)?;
    Ok(value)
}

fn apply_signed_units(
    current: u128,
    delta: i128,
    bucket: &'static str,
) -> Result<u128, PlatformAccountErrorV1> {
    let next = current
        .checked_add_signed(delta)
        .ok_or(PlatformAccountErrorV1::BalanceUnderflow(bucket))?;
    positive_i128(next)?;
    Ok(next)
}

fn checked_nonnegative_decimal_add(
    current: Decimal,
    delta: Decimal,
    bucket: &'static str,
) -> Result<Decimal, PlatformAccountErrorV1> {
    let next = current
        .checked_add(delta)
        .ok_or(PlatformAccountErrorV1::ArithmeticOverflow)?;
    if next.is_sign_negative() {
        return Err(PlatformAccountErrorV1::BalanceUnderflow(bucket));
    }
    Ok(next)
}

fn exact_micro_units(value: u128) -> ExactDecimal {
    ExactDecimal(decimal_from_micro_units(value).normalize().to_string())
}

fn exact_signed_micro_units(value: i128) -> ExactDecimal {
    ExactDecimal(
        Decimal::from_i128_with_scale(value, MICRO_SCALE)
            .normalize()
            .to_string(),
    )
}

fn exact_decimal(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}

fn decimal_from_micro_units(value: u128) -> Decimal {
    Decimal::from_i128_with_scale(
        i128::try_from(value).expect("平台开户金额始终受 rust_decimal 边界约束"),
        MICRO_SCALE,
    )
}

fn compare_token_ids(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn is_stable_text(value: &str) -> bool {
    !value.is_empty() && value.trim() == value
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn funded_account() -> PlatformAccountStateV1 {
        PlatformAccountStateV1::try_from_opening_anchor(&AccountOpeningAnchorV1 {
            schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1.to_owned(),
            anchor_id: "anchor-a".to_owned(),
            anchor_sha256: Sha256Hex("a".repeat(64)),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "run-a".to_owned(),
                caller_role: contracts::SimCallerRoleV1::ModelTrain,
                caller_instance_id: "caller-a".to_owned(),
                account_id: "account-a".to_owned(),
                platform_source: contracts::PlatformSourceV1::Simulated,
            },
            session_id: "session-a".to_owned(),
            initial_scenario_sha256: Sha256Hex("b".repeat(64)),
            initial_market_set_sha256: Sha256Hex("c".repeat(64)),
            initial_account_state_sha256: Sha256Hex("d".repeat(64)),
            initial_platform_account: PlatformAccountSnapshotV1 {
                cash_pusd: ExactDecimal("10".to_owned()),
                available_pusd: ExactDecimal("10".to_owned()),
                principal_encumbrance_pusd: ExactDecimal("0".to_owned()),
                immediate_taker_fee_encumbrance_pusd: ExactDecimal("0".to_owned()),
                outcome_balances: vec![
                    PlatformOutcomeBalanceV1 {
                        token_id: TokenId("token-a".to_owned()),
                        balance_shares: ExactDecimal("0".to_owned()),
                    },
                    PlatformOutcomeBalanceV1 {
                        token_id: TokenId("token-b".to_owned()),
                        balance_shares: ExactDecimal("0".to_owned()),
                    },
                ],
                maker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
            },
        })
        .expect("有效平台开户锚点")
    }

    fn assert_pusd_delta(
        delta: &PlatformAccountDeltaV1,
        cash: &str,
        available: &str,
        principal: &str,
        fee: &str,
    ) {
        assert_eq!(delta.balance_delta.cash_pusd_delta.0, cash);
        assert_eq!(delta.balance_delta.available_pusd_delta.0, available);
        assert_eq!(
            delta.balance_delta.principal_encumbrance_pusd_delta.0,
            principal
        );
        assert_eq!(
            delta
                .balance_delta
                .immediate_taker_fee_encumbrance_pusd_delta
                .0,
            fee
        );
    }

    fn binary_payout_vector() -> Vec<PayoutEntryV1> {
        vec![
            PayoutEntryV1 {
                asset: contracts::AssetIdentityV1 {
                    token_id: TokenId("token-a".to_owned()),
                    outcome: contracts::OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                payout_pusd_per_share: ExactDecimal("1".to_owned()),
            },
            PayoutEntryV1 {
                asset: contracts::AssetIdentityV1 {
                    token_id: TokenId("token-b".to_owned()),
                    outcome: contracts::OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
                payout_pusd_per_share: ExactDecimal("0".to_owned()),
            },
        ]
    }

    fn apply_successful_fill(
        account: &mut PlatformAccountStateV1,
        identity: &str,
        token_id: &str,
        base_units: u128,
        quote_units: u128,
        platform_effective_at_utc_ms: i64,
    ) {
        account
            .apply_order_accepted(
                &format!("accepted-{identity}"),
                "condition-a",
                platform_effective_at_utc_ms,
                decimal_from_micro_units(quote_units),
                Decimal::ZERO,
            )
            .expect("成交前本金占用");
        account
            .apply_fill_finality_economic(
                &format!("fill-{identity}"),
                "condition-a",
                token_id,
                platform_effective_at_utc_ms + 1,
                FillEconomicAmountsV1 {
                    base_units,
                    quote_units,
                    immediate_taker_fee_units: 0,
                },
            )
            .expect("成功成交经济应用");
    }

    #[test]
    fn fill_economic_application_and_compensation_are_exact_inverse_movements() {
        let mut account = funded_account();
        let opening_summary = account.account_summary_sha256().clone();
        let opening_outcome_summary = account.outcome_balances_sha256().clone();
        account
            .apply_order_accepted(
                "accepted-a",
                "condition-a",
                1_000,
                Decimal::new(2, 0),
                Decimal::new(1, 1),
            )
            .expect("本金和即时手续费占用");

        let applied = account
            .apply_fill_finality_economic(
                "fill-a-applied",
                "condition-a",
                "token-a",
                1_001,
                FillEconomicAmountsV1 {
                    base_units: 3_000_000,
                    quote_units: 2_000_000,
                    immediate_taker_fee_units: 100_000,
                },
            )
            .expect("成交经济应用");
        assert_eq!(
            applied.reason,
            PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
        );
        assert_pusd_delta(&applied, "-2.1", "0", "-2", "-0.1");
        assert_eq!(applied.balance_delta.outcome_balance_deltas.len(), 1);
        assert_eq!(
            applied.balance_delta.outcome_balance_deltas[0].token_id.0,
            "token-a"
        );
        assert_eq!(
            applied.balance_delta.outcome_balance_deltas[0]
                .balance_shares_delta
                .0,
            "3"
        );
        let applied_snapshot = account.snapshot();
        assert_eq!(applied_snapshot.cash_pusd.0, "7.9");
        assert_eq!(applied_snapshot.available_pusd.0, "7.9");
        assert_eq!(applied_snapshot.principal_encumbrance_pusd.0, "0");
        assert_eq!(applied_snapshot.immediate_taker_fee_encumbrance_pusd.0, "0");
        assert_eq!(
            applied_snapshot
                .outcome_balances
                .iter()
                .map(|balance| (
                    balance.token_id.0.as_str(),
                    balance.balance_shares.0.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![("token-a", "3"), ("token-b", "0")]
        );

        let compensated = account
            .apply_fill_finality_compensation(
                "fill-a-compensated",
                "condition-a",
                "token-a",
                1_002,
                FillEconomicAmountsV1 {
                    base_units: 3_000_000,
                    quote_units: 2_000_000,
                    immediate_taker_fee_units: 100_000,
                },
            )
            .expect("已应用失败补偿");
        assert_eq!(
            compensated.reason,
            PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
        );
        assert_pusd_delta(&compensated, "2.1", "2.1", "0", "0");
        assert_eq!(
            compensated.balance_delta.outcome_balance_deltas[0]
                .balance_shares_delta
                .0,
            "-3"
        );
        assert_eq!(account.account_summary_sha256(), &opening_summary);
        assert_eq!(account.outcome_balances_sha256(), &opening_outcome_summary);
        assert_eq!(
            account
                .snapshot()
                .outcome_balances
                .iter()
                .map(|balance| (
                    balance.token_id.0.as_str(),
                    balance.balance_shares.0.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![("token-a", "0"), ("token-b", "0")],
            "补偿后仍保留开户锚点中的完整零余额集合"
        );
    }

    #[test]
    fn unapplied_failure_releases_both_encumbrances_and_failed_compensation_is_atomic() {
        let mut account = funded_account();
        let opening_summary = account.account_summary_sha256().clone();
        account
            .apply_order_accepted(
                "accepted-b",
                "condition-a",
                2_000,
                Decimal::new(2, 0),
                Decimal::new(1, 1),
            )
            .expect("本金和即时手续费占用");

        let released = account
            .apply_fill_finality_unapplied_failure_release(
                "fill-b-unapplied-failed",
                "condition-a",
                2_001,
                2_000_000,
                100_000,
            )
            .expect("未应用失败释放");
        assert_eq!(
            released.reason,
            PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased
        );
        assert_pusd_delta(&released, "0", "2.1", "-2", "-0.1");
        assert!(released.balance_delta.outcome_balance_deltas.is_empty());
        assert_eq!(account.account_summary_sha256(), &opening_summary);

        let before_failed_compensation = account.clone();
        assert_eq!(
            account.apply_fill_finality_compensation(
                "fill-without-applied-economics",
                "condition-a",
                "token-a",
                2_002,
                FillEconomicAmountsV1 {
                    base_units: 1_000_000,
                    quote_units: 1_000_000,
                    immediate_taker_fee_units: 0,
                },
            ),
            Err(PlatformAccountErrorV1::BalanceUnderflow("outcome_balance"))
        );
        assert_eq!(account, before_failed_compensation);
    }

    #[test]
    fn venue_dust_release_is_positive_exact_and_adjacent_retry_is_idempotent() {
        let mut account = funded_account();
        account
            .apply_order_accepted(
                "accepted-dust",
                "condition-a",
                3_000,
                Decimal::new(2, 0),
                Decimal::ZERO,
            )
            .expect("小额余量订单本金占用");

        let before_zero = account.clone();
        assert_eq!(
            account.apply_venue_dust_release(
                "dust:source-event-a:order-a",
                "condition-a",
                3_001,
                Decimal::ZERO,
            ),
            Err(PlatformAccountErrorV1::InvalidAmount(
                "平台小额余量释放必须为正数"
            ))
        );
        assert_eq!(account, before_zero);

        let released = account
            .apply_venue_dust_release(
                "dust:source-event-a:order-a",
                "condition-a",
                3_001,
                Decimal::new(25, 2),
            )
            .expect("释放精确小额余量本金占用");
        assert_eq!(
            released.reason,
            PlatformAccountDeltaReasonV1::VenueDustEncumbranceReleased
        );
        assert_eq!(released.reason_identity, "dust:source-event-a:order-a");
        assert_pusd_delta(&released, "0", "0.25", "-0.25", "0");
        assert!(released.balance_delta.outcome_balance_deltas.is_empty());
        let snapshot = account.snapshot();
        assert_eq!(snapshot.cash_pusd.0, "10");
        assert_eq!(snapshot.available_pusd.0, "8.25");
        assert_eq!(snapshot.principal_encumbrance_pusd.0, "1.75");
        assert_eq!(snapshot.immediate_taker_fee_encumbrance_pusd.0, "0");
        let next_sequence = account.next_account_transition_seq();

        let retry = account
            .apply_venue_dust_release(
                "dust:source-event-a:order-a",
                "condition-a",
                3_001,
                Decimal::new(25, 2),
            )
            .expect("相邻物理重试返回原变化");
        assert!(Arc::ptr_eq(&released, &retry));
        assert_eq!(account.next_account_transition_seq(), next_sequence);
        assert_eq!(account.snapshot(), snapshot);
    }

    #[test]
    fn market_resolution_release_moves_positive_open_quote_back_to_available() {
        let mut account = funded_account();
        account
            .apply_order_accepted(
                "accepted-resolution",
                "condition-a",
                3_100,
                Decimal::new(2, 0),
                Decimal::ZERO,
            )
            .expect("市场判定前本金占用");

        let released = account
            .apply_market_resolution_encumbrance_release(
                "resolution:source-event-a:order-a",
                "condition-a",
                3_101,
                Decimal::new(25, 2),
            )
            .expect("市场判定释放开放订单余量");

        assert_eq!(
            released.reason,
            PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased
        );
        assert_pusd_delta(&released, "0", "0.25", "-0.25", "0");
        let snapshot = account.snapshot();
        assert_eq!(snapshot.cash_pusd.0, "10");
        assert_eq!(snapshot.available_pusd.0, "8.25");
        assert_eq!(snapshot.principal_encumbrance_pusd.0, "1.75");
    }

    #[test]
    fn auto_redeem_uses_actual_two_token_balances_and_applies_one_atomic_delta() {
        let mut account = funded_account();
        apply_successful_fill(
            &mut account,
            "winning",
            "token-a",
            3_000_000,
            2_000_000,
            3_200,
        );
        apply_successful_fill(
            &mut account,
            "losing",
            "token-b",
            2_000_000,
            1_000_000,
            3_300,
        );

        let effect = account
            .apply_auto_redeem(
                "auto-redeem:source-event-a",
                "condition-a",
                3_400,
                &binary_payout_vector(),
            )
            .expect("二元结果代币原子赎回");

        assert_eq!(
            effect.redeemed_outcome_balances,
            vec![
                PlatformOutcomeBalanceV1 {
                    token_id: TokenId("token-a".to_owned()),
                    balance_shares: ExactDecimal("3".to_owned()),
                },
                PlatformOutcomeBalanceV1 {
                    token_id: TokenId("token-b".to_owned()),
                    balance_shares: ExactDecimal("2".to_owned()),
                },
            ]
        );
        assert_eq!(effect.redeemed_pusd, Decimal::new(3, 0));
        let delta = effect.account_delta.expect("正持仓产生账户变化");
        assert_eq!(
            delta.reason,
            PlatformAccountDeltaReasonV1::AutoRedeemApplied
        );
        assert_pusd_delta(&delta, "3", "3", "0", "0");
        assert_eq!(delta.balance_delta.outcome_balance_deltas.len(), 2);
        assert_eq!(
            delta
                .balance_delta
                .outcome_balance_deltas
                .iter()
                .map(|balance| (
                    balance.token_id.0.as_str(),
                    balance.balance_shares_delta.0.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![("token-a", "-3"), ("token-b", "-2")]
        );
        let snapshot = account.snapshot();
        assert_eq!(snapshot.cash_pusd.0, "10");
        assert_eq!(snapshot.available_pusd.0, "10");
        assert_eq!(
            snapshot
                .outcome_balances
                .iter()
                .map(|balance| (
                    balance.token_id.0.as_str(),
                    balance.balance_shares.0.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![("token-a", "0"), ("token-b", "0")]
        );
    }

    #[test]
    fn auto_redeem_zero_holdings_returns_full_result_without_account_delta() {
        let mut account = funded_account();
        let before = account.clone();

        let effect = account
            .apply_auto_redeem(
                "auto-redeem:zero",
                "condition-a",
                3_500,
                &binary_payout_vector(),
            )
            .expect("零持仓仍返回完整赎回结果");

        assert_eq!(
            effect.redeemed_outcome_balances,
            vec![
                PlatformOutcomeBalanceV1 {
                    token_id: TokenId("token-a".to_owned()),
                    balance_shares: ExactDecimal("0".to_owned()),
                },
                PlatformOutcomeBalanceV1 {
                    token_id: TokenId("token-b".to_owned()),
                    balance_shares: ExactDecimal("0".to_owned()),
                },
            ]
        );
        assert_eq!(effect.redeemed_pusd, Decimal::ZERO);
        assert_eq!(effect.account_delta, None);
        assert_eq!(account, before);
    }

    #[test]
    fn auto_redeem_losing_only_balance_emits_zero_cash_delta_and_clears_token() {
        let mut account = funded_account();
        apply_successful_fill(
            &mut account,
            "losing-only",
            "token-b",
            2_000_000,
            1_000_000,
            3_600,
        );

        let effect = account
            .apply_auto_redeem(
                "auto-redeem:losing-only",
                "condition-a",
                3_700,
                &binary_payout_vector(),
            )
            .expect("仅未获胜代币仍须清零");

        assert_eq!(effect.redeemed_pusd, Decimal::ZERO);
        let delta = effect.account_delta.expect("正持仓清零必须产生账户变化");
        assert_pusd_delta(&delta, "0", "0", "0", "0");
        assert_eq!(delta.balance_delta.outcome_balance_deltas.len(), 1);
        assert_eq!(
            delta.balance_delta.outcome_balance_deltas[0],
            PlatformOutcomeBalanceDeltaV1 {
                token_id: TokenId("token-b".to_owned()),
                balance_shares_delta: ExactDecimal("-2".to_owned()),
            }
        );
        let snapshot = account.snapshot();
        assert_eq!(snapshot.cash_pusd.0, "9");
        assert_eq!(snapshot.available_pusd.0, "9");
        assert_eq!(
            snapshot
                .outcome_balances
                .iter()
                .find(|balance| balance.token_id.0 == "token-b")
                .expect("未获胜代币余额仍保留为零")
                .balance_shares
                .0,
            "0"
        );
    }

    #[test]
    fn maker_rebate_accrual_day_remainder_and_payment_use_exact_separate_buckets() {
        let mut account = funded_account();
        let accrued = account
            .apply_maker_rebate_accrual_finalized(
                "maker-accrual:condition-a",
                "condition-a",
                4_000,
                Decimal::new(123_456_789, 8),
            )
            .expect("maker 返佣应计允许超过六位小数");
        assert_eq!(
            accrued.reason,
            PlatformAccountDeltaReasonV1::MakerRebateAccrualFinalized
        );
        assert_eq!(
            accrued.condition_id,
            Some(ConditionId("condition-a".into()))
        );
        assert_pusd_delta(&accrued, "0", "0", "0", "0");
        assert_eq!(
            accrued.balance_delta.maker_rebate_receivable_pusd_delta.0,
            "1.23456789"
        );

        let finalized = account
            .apply_maker_rebate_day_finalized("maker-day:2026-08-12", 4_001, Decimal::new(6_789, 8))
            .expect("maker 返佣日结")
            .expect("正截取余数产生账户变化");
        assert_eq!(
            finalized.reason,
            PlatformAccountDeltaReasonV1::MakerRebateDayFinalized
        );
        assert_eq!(finalized.condition_id, None);
        assert_eq!(
            finalized.balance_delta.maker_rebate_receivable_pusd_delta.0,
            "-0.00006789"
        );
        assert_eq!(
            finalized
                .balance_delta
                .maker_rebate_locked_remainder_pusd_delta
                .0,
            "0.00006789"
        );

        let paid = account
            .apply_maker_rebate_paid("maker-paid:2026-08-12", 4_002, Decimal::new(12_345, 4))
            .expect("四位小数 maker 返佣付款");
        assert_eq!(paid.reason, PlatformAccountDeltaReasonV1::MakerRebatePaid);
        assert_eq!(paid.condition_id, None);
        assert_pusd_delta(&paid, "1.2345", "1.2345", "0", "0");
        assert_eq!(
            paid.balance_delta.maker_rebate_receivable_pusd_delta.0,
            "-1.2345"
        );
        let paid_snapshot = account.snapshot();
        assert_eq!(paid_snapshot.cash_pusd.0, "11.2345");
        assert_eq!(paid_snapshot.available_pusd.0, "11.2345");
        assert_eq!(paid_snapshot.maker_rebate_receivable_pusd.0, "0");
        assert_eq!(
            paid_snapshot.maker_rebate_locked_remainder_pusd.0,
            "0.00006789"
        );
        assert_eq!(paid_snapshot.taker_rebate_receivable_pusd.0, "0");
        assert_eq!(paid_snapshot.taker_rebate_locked_remainder_pusd.0, "0");

        let retry = account
            .apply_maker_rebate_paid("maker-paid:2026-08-12", 4_002, Decimal::new(12_345, 4))
            .expect("相邻物理重试返回原付款变化");
        assert!(Arc::ptr_eq(&paid, &retry));
        assert_eq!(account.snapshot(), paid_snapshot);
    }

    #[test]
    fn taker_rebate_below_threshold_remains_receivable_and_does_not_join_later_payment() {
        let mut account = funded_account();
        account
            .apply_taker_rebate_accrual_applied(
                "taker-accrual:trade-a",
                "condition-a",
                5_000,
                Decimal::new(8, 1),
            )
            .expect("首日低于门槛的 taker 应计");
        let below_threshold_snapshot = account.snapshot();
        assert_eq!(
            account
                .apply_taker_rebate_day_finalized("taker-day:2026-08-11", 5_001, Decimal::ZERO,)
                .expect("无截取余数时无需账户变化"),
            None
        );
        assert_eq!(account.snapshot(), below_threshold_snapshot);

        account
            .apply_taker_rebate_accrual_applied(
                "taker-accrual:trade-b",
                "condition-b",
                5_002,
                Decimal::new(100_001, 5),
            )
            .expect("次日达到门槛的 taker 应计");
        account
            .apply_taker_rebate_day_finalized("taker-day:2026-08-12", 5_003, Decimal::new(1, 5))
            .expect("次日 taker 日结")
            .expect("次日有正截取余数");
        account
            .apply_taker_rebate_paid("taker-paid:2026-08-12", 5_004, Decimal::ONE)
            .expect("次日 taker 付款不消费首日低门槛应收");

        let snapshot = account.snapshot();
        assert_eq!(snapshot.cash_pusd.0, "11");
        assert_eq!(snapshot.available_pusd.0, "11");
        assert_eq!(snapshot.maker_rebate_receivable_pusd.0, "0");
        assert_eq!(snapshot.maker_rebate_locked_remainder_pusd.0, "0");
        assert_eq!(snapshot.taker_rebate_receivable_pusd.0, "0.8");
        assert_eq!(snapshot.taker_rebate_locked_remainder_pusd.0, "0.00001");
    }

    #[test]
    fn invalid_rebate_amounts_and_insufficient_receivable_are_atomic() {
        let mut account = funded_account();
        let opening = account.clone();

        assert_eq!(
            account.apply_maker_rebate_accrual_finalized(
                "maker-zero",
                "condition-a",
                6_000,
                Decimal::ZERO,
            ),
            Err(PlatformAccountErrorV1::InvalidAmount(
                "maker 返佣应计必须为正数"
            ))
        );
        assert_eq!(account, opening);
        assert_eq!(
            account.apply_taker_rebate_accrual_applied(
                "taker-negative",
                "condition-a",
                6_001,
                Decimal::NEGATIVE_ONE,
            ),
            Err(PlatformAccountErrorV1::InvalidAmount(
                "taker 返佣应计必须为正数"
            ))
        );
        assert_eq!(account, opening);
        assert_eq!(
            account.apply_maker_rebate_day_finalized(
                "maker-day-negative",
                6_002,
                Decimal::NEGATIVE_ONE,
            ),
            Err(PlatformAccountErrorV1::InvalidAmount(
                "返佣日结锁定余数不能为负数"
            ))
        );
        assert_eq!(account, opening);
        assert_eq!(
            account.apply_taker_rebate_paid(
                "taker-paid-too-precise",
                6_003,
                Decimal::new(100_001, 5),
            ),
            Err(PlatformAccountErrorV1::InvalidAmount(
                "返佣付款必须为正数且最多保留 4 位小数"
            ))
        );
        assert_eq!(account, opening);
        assert_eq!(
            account.apply_maker_rebate_paid("maker-paid-without-receivable", 6_004, Decimal::ONE,),
            Err(PlatformAccountErrorV1::BalanceUnderflow(
                "maker_rebate_receivable_pusd"
            ))
        );
        assert_eq!(account, opening);
    }
}
