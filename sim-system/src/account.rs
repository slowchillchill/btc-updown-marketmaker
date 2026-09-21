use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

#[cfg(test)]
use contracts::Sha256Hex;
use contracts::{
    AccountInstanceKeyV1, AccountOpeningFactV2, ConditionId, ExactDecimal,
    PlatformAccountBalanceDeltaV1, PlatformAccountDeltaReasonV2, PlatformAccountDeltaV2,
    PlatformOutcomeBalanceDeltaV1, PlatformOutcomeBalanceV1, TokenId,
};
use data_pipeline_core::parse_canonical_decimal;
use rust_decimal::Decimal;

const MICRO_SCALE: u32 = 6;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PlatformAccountStateV2 {
    account_instance_key: AccountInstanceKeyV1,
    cash_pusd_units: u128,
    available_pusd_units: u128,
    principal_encumbrance_pusd_units: u128,
    immediate_taker_fee_encumbrance_pusd_units: u128,
    outcome_balance_units: BTreeMap<String, u128>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
    next_account_transition_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlatformAccountMovement {
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

struct AccountTransitionSource<'a> {
    cash_pusd_units: u128,
    available_pusd_units: u128,
    principal_encumbrance_pusd_units: u128,
    immediate_taker_fee_encumbrance_pusd_units: u128,
    outcome_balance_units: &'a BTreeMap<String, u128>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
}

struct AccountTransitionValues {
    cash_pusd_units: u128,
    available_pusd_units: u128,
    principal_encumbrance_pusd_units: u128,
    immediate_taker_fee_encumbrance_pusd_units: u128,
    outcome_balance_updates: Vec<(String, u128)>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FillEconomicAmountsV1 {
    pub(crate) base_units: u128,
    pub(crate) quote_units: u128,
    pub(crate) immediate_taker_fee_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AutoRedeemAccountEffectV2 {
    pub(crate) redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    pub(crate) redeemed_pusd: Decimal,
    pub(crate) account_delta: Option<PlatformAccountDeltaV2>,
}

struct AutoRedeemCalculation {
    redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    redeemed_pusd: Decimal,
    movement: Option<PlatformAccountMovement>,
}

impl PlatformAccountMovement {
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

fn order_accepted_movement(
    principal_encumbrance_pusd: Decimal,
    immediate_taker_fee_encumbrance_pusd: Decimal,
) -> Result<PlatformAccountMovement, PlatformAccountError> {
    let principal_units = decimal_to_micro_units(
        principal_encumbrance_pusd,
        "order accepted principal encumbrance",
    )?;
    let fee_units = decimal_to_micro_units(
        immediate_taker_fee_encumbrance_pusd,
        "order accepted fee encumbrance",
    )?;
    if principal_units == 0 {
        return Err(PlatformAccountError::InvalidAmount(
            "订单接受本金占用必须为正数",
        ));
    }
    let principal_delta = positive_i128(principal_units)?;
    let fee_delta = positive_i128(fee_units)?;
    let total = principal_delta
        .checked_add(fee_delta)
        .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    Ok(PlatformAccountMovement::pusd(
        -total,
        principal_delta,
        fee_delta,
    ))
}

fn require_positive_fill_amounts(
    amounts: FillEconomicAmountsV1,
    error: &'static str,
) -> Result<(), PlatformAccountError> {
    if amounts.base_units == 0 || amounts.quote_units == 0 {
        return Err(PlatformAccountError::InvalidAmount(error));
    }
    Ok(())
}

fn fill_economic_movement(
    token_id: &str,
    amounts: FillEconomicAmountsV1,
) -> Result<PlatformAccountMovement, PlatformAccountError> {
    let total_units = amounts
        .quote_units
        .checked_add(amounts.immediate_taker_fee_units)
        .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    let base_delta = positive_i128(amounts.base_units)?;
    let quote_delta = positive_i128(amounts.quote_units)?;
    let fee_delta = positive_i128(amounts.immediate_taker_fee_units)?;
    let total_delta = positive_i128(total_units)?;
    Ok(PlatformAccountMovement::with_outcome(
        -total_delta,
        0,
        -quote_delta,
        -fee_delta,
        token_id,
        base_delta,
    ))
}

fn fill_unapplied_failure_release_movement(
    quote_units: u128,
    immediate_taker_fee_units: u128,
) -> Result<PlatformAccountMovement, PlatformAccountError> {
    if quote_units == 0 {
        return Err(PlatformAccountError::InvalidAmount(
            "未应用失败成交 quote 必须为正数",
        ));
    }
    let total_units = quote_units
        .checked_add(immediate_taker_fee_units)
        .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    let quote_delta = positive_i128(quote_units)?;
    let fee_delta = positive_i128(immediate_taker_fee_units)?;
    let total_delta = positive_i128(total_units)?;
    Ok(PlatformAccountMovement::pusd(
        total_delta,
        -quote_delta,
        -fee_delta,
    ))
}

fn fill_compensation_movement(
    token_id: &str,
    amounts: FillEconomicAmountsV1,
) -> Result<PlatformAccountMovement, PlatformAccountError> {
    let total_units = amounts
        .quote_units
        .checked_add(amounts.immediate_taker_fee_units)
        .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    let base_delta = positive_i128(amounts.base_units)?;
    let total_delta = positive_i128(total_units)?;
    Ok(PlatformAccountMovement::with_outcome(
        total_delta,
        total_delta,
        0,
        0,
        token_id,
        -base_delta,
    ))
}

fn encumbrance_release_movement(
    released_pusd: Decimal,
) -> Result<Option<PlatformAccountMovement>, PlatformAccountError> {
    let released_units = decimal_to_micro_units(released_pusd, "encumbrance release")?;
    if released_units == 0 {
        return Ok(None);
    }
    let released_delta = positive_i128(released_units)?;
    Ok(Some(PlatformAccountMovement::pusd(
        released_delta,
        -released_delta,
        0,
    )))
}

fn rebate_accrual_movement(
    accrual_pusd: Decimal,
    maker: bool,
) -> Result<PlatformAccountMovement, PlatformAccountError> {
    if accrual_pusd <= Decimal::ZERO {
        return Err(PlatformAccountError::InvalidAmount(if maker {
            "maker 返佣应计必须为正数"
        } else {
            "taker 返佣应计必须为正数"
        }));
    }
    Ok(if maker {
        PlatformAccountMovement::rebate(
            0,
            0,
            accrual_pusd,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        )
    } else {
        PlatformAccountMovement::rebate(
            0,
            0,
            Decimal::ZERO,
            accrual_pusd,
            Decimal::ZERO,
            Decimal::ZERO,
        )
    })
}

fn rebate_day_finalized_movement(
    locked_remainder_pusd: Decimal,
    maker: bool,
) -> Result<Option<PlatformAccountMovement>, PlatformAccountError> {
    if locked_remainder_pusd.is_sign_negative() {
        return Err(PlatformAccountError::InvalidAmount(
            "返佣日结锁定余数不能为负数",
        ));
    }
    if locked_remainder_pusd.is_zero() {
        return Ok(None);
    }
    let receivable_delta = Decimal::ZERO
        .checked_sub(locked_remainder_pusd)
        .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    Ok(Some(if maker {
        PlatformAccountMovement::rebate(
            0,
            0,
            receivable_delta,
            Decimal::ZERO,
            locked_remainder_pusd,
            Decimal::ZERO,
        )
    } else {
        PlatformAccountMovement::rebate(
            0,
            0,
            Decimal::ZERO,
            receivable_delta,
            Decimal::ZERO,
            locked_remainder_pusd,
        )
    }))
}

fn rebate_paid_movement(
    paid_pusd: Decimal,
    maker: bool,
) -> Result<PlatformAccountMovement, PlatformAccountError> {
    if paid_pusd <= Decimal::ZERO || paid_pusd.normalize().scale() > 4 {
        return Err(PlatformAccountError::InvalidAmount(
            "返佣付款必须为正数且最多保留 4 位小数",
        ));
    }
    let paid_units = decimal_to_micro_units(paid_pusd, "rebate payment")?;
    let paid_units_delta = positive_i128(paid_units)?;
    let receivable_delta = Decimal::ZERO
        .checked_sub(paid_pusd)
        .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    Ok(if maker {
        PlatformAccountMovement::rebate(
            paid_units_delta,
            paid_units_delta,
            receivable_delta,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        )
    } else {
        PlatformAccountMovement::rebate(
            paid_units_delta,
            paid_units_delta,
            Decimal::ZERO,
            receivable_delta,
            Decimal::ZERO,
            Decimal::ZERO,
        )
    })
}

fn auto_redeem_calculation<'a, P, I, F>(
    outcome_balance_units: &BTreeMap<String, u128>,
    payout_entries: I,
    mut payout_per_share: F,
) -> Result<AutoRedeemCalculation, PlatformAccountError>
where
    I: IntoIterator<Item = (&'a TokenId, P)>,
    F: FnMut(P) -> Result<Decimal, PlatformAccountError>,
{
    let mut redeemed_outcome_balances = Vec::with_capacity(2);
    let mut outcome_balance_units_delta = BTreeMap::new();
    let mut payout_pusd = Decimal::ZERO;
    for (token, payout) in payout_entries {
        let token_id = &token.0;
        let balance_units = outcome_balance_units.get(token_id).copied().unwrap_or(0);
        let balance_shares = decimal_from_micro_units(balance_units);
        redeemed_outcome_balances.push(PlatformOutcomeBalanceV1 {
            token_id: token.clone(),
            balance_shares: exact_micro_units(balance_units),
        });
        if balance_units > 0 {
            outcome_balance_units_delta.insert(token_id.clone(), -positive_i128(balance_units)?);
        }
        let payout_per_share = payout_per_share(payout)?;
        let token_payout = balance_shares
            .checked_mul(payout_per_share)
            .ok_or(PlatformAccountError::ArithmeticOverflow)?;
        payout_pusd = payout_pusd
            .checked_add(token_payout)
            .ok_or(PlatformAccountError::ArithmeticOverflow)?;
    }
    let payout_pusd = payout_pusd.normalize();
    let movement = if outcome_balance_units_delta.is_empty() {
        None
    } else {
        let payout_units = decimal_to_micro_units(payout_pusd, "auto redeem payout")?;
        let payout_delta = positive_i128(payout_units)?;
        Some(PlatformAccountMovement {
            cash_pusd_units_delta: payout_delta,
            available_pusd_units_delta: payout_delta,
            principal_encumbrance_pusd_units_delta: 0,
            immediate_taker_fee_encumbrance_pusd_units_delta: 0,
            outcome_balance_units_delta,
            maker_rebate_receivable_pusd_delta: Decimal::ZERO,
            taker_rebate_receivable_pusd_delta: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd_delta: Decimal::ZERO,
        })
    };
    Ok(AutoRedeemCalculation {
        redeemed_outcome_balances,
        redeemed_pusd: payout_pusd,
        movement,
    })
}

fn calculate_account_transition(
    source: AccountTransitionSource<'_>,
    movement: &PlatformAccountMovement,
) -> Result<AccountTransitionValues, PlatformAccountErrorV1> {
    let cash_pusd_units = apply_signed_units(
        source.cash_pusd_units,
        movement.cash_pusd_units_delta,
        "cash_pusd",
    )?;
    let available_pusd_units = apply_signed_units(
        source.available_pusd_units,
        movement.available_pusd_units_delta,
        "available_pusd",
    )?;
    let principal_encumbrance_pusd_units = apply_signed_units(
        source.principal_encumbrance_pusd_units,
        movement.principal_encumbrance_pusd_units_delta,
        "principal_encumbrance_pusd",
    )?;
    let immediate_taker_fee_encumbrance_pusd_units = apply_signed_units(
        source.immediate_taker_fee_encumbrance_pusd_units,
        movement.immediate_taker_fee_encumbrance_pusd_units_delta,
        "immediate_taker_fee_encumbrance_pusd",
    )?;
    let mut outcome_balance_updates =
        Vec::with_capacity(movement.outcome_balance_units_delta.len());
    for (token_id, delta) in &movement.outcome_balance_units_delta {
        let current = source
            .outcome_balance_units
            .get(token_id)
            .copied()
            .unwrap_or(0);
        outcome_balance_updates.push((
            token_id.clone(),
            apply_signed_units(current, *delta, "outcome_balance")?,
        ));
    }
    let maker_rebate_receivable_pusd = checked_nonnegative_decimal_add(
        source.maker_rebate_receivable_pusd,
        movement.maker_rebate_receivable_pusd_delta,
        "maker_rebate_receivable_pusd",
    )?;
    let taker_rebate_receivable_pusd = checked_nonnegative_decimal_add(
        source.taker_rebate_receivable_pusd,
        movement.taker_rebate_receivable_pusd_delta,
        "taker_rebate_receivable_pusd",
    )?;
    let maker_rebate_locked_remainder_pusd = checked_nonnegative_decimal_add(
        source.maker_rebate_locked_remainder_pusd,
        movement.maker_rebate_locked_remainder_pusd_delta,
        "maker_rebate_locked_remainder_pusd",
    )?;
    let taker_rebate_locked_remainder_pusd = checked_nonnegative_decimal_add(
        source.taker_rebate_locked_remainder_pusd,
        movement.taker_rebate_locked_remainder_pusd_delta,
        "taker_rebate_locked_remainder_pusd",
    )?;
    Ok(AccountTransitionValues {
        cash_pusd_units,
        available_pusd_units,
        principal_encumbrance_pusd_units,
        immediate_taker_fee_encumbrance_pusd_units,
        outcome_balance_updates,
        maker_rebate_receivable_pusd,
        taker_rebate_receivable_pusd,
        maker_rebate_locked_remainder_pusd,
        taker_rebate_locked_remainder_pusd,
    })
}

fn apply_outcome_balance_updates(
    balances: &mut BTreeMap<String, u128>,
    updates: Vec<(String, u128)>,
) {
    for (token_id, balance) in updates {
        balances.insert(token_id, balance);
    }
}

#[allow(dead_code)]
impl PlatformAccountStateV2 {
    pub(crate) fn try_from_opening_fact(
        fact: &AccountOpeningFactV2,
    ) -> Result<Self, PlatformAccountError> {
        let available_pusd = parse_canonical_decimal(
            &fact.initial_account_state.available_pusd,
            "initial available_pusd",
        )
        .map_err(|_| PlatformAccountError::InvalidInitialAccountState)?;
        let available_pusd_units =
            decimal_to_micro_units(available_pusd, "initial available_pusd")?;
        Ok(Self {
            account_instance_key: fact.account_instance_key.clone(),
            cash_pusd_units: available_pusd_units,
            available_pusd_units,
            principal_encumbrance_pusd_units: 0,
            immediate_taker_fee_encumbrance_pusd_units: 0,
            outcome_balance_units: BTreeMap::new(),
            maker_rebate_receivable_pusd: Decimal::ZERO,
            taker_rebate_receivable_pusd: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd: Decimal::ZERO,
            next_account_transition_seq: 1,
        })
    }

    pub(crate) fn account_instance_key(&self) -> &AccountInstanceKeyV1 {
        &self.account_instance_key
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

    pub(crate) fn next_account_transition_seq(&self) -> u64 {
        self.next_account_transition_seq
    }

    pub(crate) fn has_resolution_payout(&self, payouts: &[(TokenId, Decimal); 2]) -> bool {
        payouts.iter().any(|(token, payout)| {
            *payout > Decimal::ZERO
                && self
                    .outcome_balance_units
                    .get(&token.0)
                    .copied()
                    .unwrap_or(0)
                    > 0
        })
    }

    pub(crate) fn outcome_balance_record_count(&self) -> usize {
        self.outcome_balance_units.len()
    }

    #[cfg(test)]
    pub(crate) fn economic_balances_for_tests(
        &self,
    ) -> (
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Vec<(String, Decimal)>,
    ) {
        (
            decimal_from_micro_units(self.cash_pusd_units),
            decimal_from_micro_units(self.available_pusd_units),
            self.maker_rebate_receivable_pusd,
            self.taker_rebate_receivable_pusd,
            self.maker_rebate_locked_remainder_pusd,
            self.taker_rebate_locked_remainder_pusd,
            self.outcome_balance_units
                .iter()
                .map(|(token_id, units)| (token_id.clone(), decimal_from_micro_units(*units)))
                .collect(),
        )
    }

    pub(crate) fn apply_order_accepted(
        &mut self,
        platform_order_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        principal_encumbrance_pusd: Decimal,
        immediate_taker_fee_encumbrance_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_transition(
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: platform_order_id.to_owned(),
            },
            Some(condition_id),
            platform_effective_at_utc_ms,
            order_accepted_movement(
                principal_encumbrance_pusd,
                immediate_taker_fee_encumbrance_pusd,
            )?,
        )
    }

    pub(crate) fn apply_order_accepted_with_required_available(
        &mut self,
        platform_order_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        required_available_pusd_units: u128,
        principal_encumbrance_pusd: Decimal,
        immediate_taker_fee_encumbrance_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        if self.available_pusd_units < required_available_pusd_units {
            return Err(PlatformAccountError::BalanceUnderflow("available_pusd"));
        }
        self.apply_order_accepted(
            platform_order_id,
            condition_id,
            platform_effective_at_utc_ms,
            principal_encumbrance_pusd,
            immediate_taker_fee_encumbrance_pusd,
        )
    }

    pub(crate) fn apply_fill_finality_economic(
        &mut self,
        trade_id: &str,
        path_ordinal: u32,
        condition_id: &ConditionId,
        token_id: &TokenId,
        platform_effective_at_utc_ms: i64,
        amounts: FillEconomicAmountsV1,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        require_positive_fill_amounts(amounts, "成交 base 与 quote 必须为正数")?;
        self.apply_transition(
            PlatformAccountDeltaReasonV2::FillFinalityEconomicApplied {
                trade_id: trade_id.to_owned(),
                path_ordinal,
            },
            Some(condition_id),
            platform_effective_at_utc_ms,
            fill_economic_movement(&token_id.0, amounts)?,
        )
    }

    pub(crate) fn apply_fill_finality_unapplied_failure_release(
        &mut self,
        trade_id: &str,
        path_ordinal: u32,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        quote_units: u128,
        immediate_taker_fee_units: u128,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_transition(
            PlatformAccountDeltaReasonV2::FillFinalityUnappliedFailureReleased {
                trade_id: trade_id.to_owned(),
                path_ordinal,
            },
            Some(condition_id),
            platform_effective_at_utc_ms,
            fill_unapplied_failure_release_movement(quote_units, immediate_taker_fee_units)?,
        )
    }

    pub(crate) fn apply_fill_finality_compensation(
        &mut self,
        trade_id: &str,
        path_ordinal: u32,
        condition_id: &ConditionId,
        token_id: &TokenId,
        platform_effective_at_utc_ms: i64,
        amounts: FillEconomicAmountsV1,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        require_positive_fill_amounts(amounts, "补偿 base 与 quote 必须为正数")?;
        self.apply_transition(
            PlatformAccountDeltaReasonV2::FillFinalityCompensationApplied {
                trade_id: trade_id.to_owned(),
                path_ordinal,
            },
            Some(condition_id),
            platform_effective_at_utc_ms,
            fill_compensation_movement(&token_id.0, amounts)?,
        )
    }

    pub(crate) fn apply_fak_unspent_release(
        &mut self,
        platform_order_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV2::FakUnspentEncumbranceReleased {
                platform_order_id: platform_order_id.to_owned(),
            },
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )
    }

    pub(crate) fn apply_caller_cancel_release(
        &mut self,
        platform_order_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV2::CallerCancellationEncumbranceReleased {
                platform_order_id: platform_order_id.to_owned(),
            },
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )
    }

    pub(crate) fn apply_gtd_expiration_release(
        &mut self,
        platform_order_id: &str,
        encoded_expires_at_utc_ms: i64,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV2::GtdExpirationEncumbranceReleased {
                platform_order_id: platform_order_id.to_owned(),
                encoded_expires_at_utc_ms,
            },
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )
    }

    pub(crate) fn apply_venue_dust_release(
        &mut self,
        platform_order_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV2::VenueDustEncumbranceReleased {
                platform_order_id: platform_order_id.to_owned(),
            },
            condition_id,
            platform_effective_at_utc_ms,
            released_pusd,
        )?
        .ok_or(PlatformAccountError::InvalidAmount(
            "平台小额余量释放必须为正数",
        ))
    }

    pub(crate) fn apply_market_resolution_encumbrance_release(
        &mut self,
        resolution_event_id: &str,
        platform_order_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        released_open_quote_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_encumbrance_release(
            PlatformAccountDeltaReasonV2::MarketResolutionEncumbranceReleased {
                resolution_event_id: resolution_event_id.to_owned(),
                platform_order_id: platform_order_id.to_owned(),
            },
            condition_id,
            platform_effective_at_utc_ms,
            released_open_quote_pusd,
        )?
        .ok_or(PlatformAccountError::InvalidAmount(
            "市场判定开放订单余量释放必须为正数",
        ))
    }

    pub(crate) fn apply_auto_redeem(
        &mut self,
        resolution_event_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        parsed_binary_payout: &[(TokenId, Decimal); 2],
    ) -> Result<AutoRedeemAccountEffectV2, PlatformAccountError> {
        let AutoRedeemCalculation {
            redeemed_outcome_balances,
            redeemed_pusd,
            movement,
        } = auto_redeem_calculation(
            &self.outcome_balance_units,
            parsed_binary_payout
                .iter()
                .map(|(token_id, payout)| (token_id, *payout)),
            Ok,
        )?;
        let account_delta = movement
            .map(|movement| {
                self.apply_transition(
                    PlatformAccountDeltaReasonV2::AutoRedeemApplied {
                        resolution_event_id: resolution_event_id.to_owned(),
                    },
                    Some(condition_id),
                    platform_effective_at_utc_ms,
                    movement,
                )
            })
            .transpose()?;
        Ok(AutoRedeemAccountEffectV2 {
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
        })
    }

    pub(crate) fn apply_maker_rebate_accrual_finalized(
        &mut self,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        accrual_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_transition(
            PlatformAccountDeltaReasonV2::MakerRebateAccrualFinalized,
            Some(condition_id),
            platform_effective_at_utc_ms,
            rebate_accrual_movement(accrual_pusd, true)?,
        )
    }

    pub(crate) fn apply_taker_rebate_accrual_applied(
        &mut self,
        trade_id: &str,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        accrual_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_transition(
            PlatformAccountDeltaReasonV2::TakerRebateAccrualApplied {
                trade_id: trade_id.to_owned(),
            },
            Some(condition_id),
            platform_effective_at_utc_ms,
            rebate_accrual_movement(accrual_pusd, false)?,
        )
    }

    pub(crate) fn apply_maker_rebate_day_finalized(
        &mut self,
        attribution_day_utc_ms: i64,
        platform_effective_at_utc_ms: i64,
        locked_remainder_pusd: Decimal,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        self.apply_rebate_day_finalized(
            PlatformAccountDeltaReasonV2::MakerRebateDayFinalized {
                attribution_day_utc_ms,
            },
            platform_effective_at_utc_ms,
            locked_remainder_pusd,
            true,
        )
    }

    pub(crate) fn apply_taker_rebate_day_finalized(
        &mut self,
        attribution_day_utc_ms: i64,
        platform_effective_at_utc_ms: i64,
        locked_remainder_pusd: Decimal,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        self.apply_rebate_day_finalized(
            PlatformAccountDeltaReasonV2::TakerRebateDayFinalized {
                attribution_day_utc_ms,
            },
            platform_effective_at_utc_ms,
            locked_remainder_pusd,
            false,
        )
    }

    pub(crate) fn apply_maker_rebate_paid(
        &mut self,
        attribution_day_utc_ms: i64,
        platform_effective_at_utc_ms: i64,
        paid_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_rebate_paid(
            PlatformAccountDeltaReasonV2::MakerRebatePaid {
                attribution_day_utc_ms,
            },
            platform_effective_at_utc_ms,
            paid_pusd,
            true,
        )
    }

    pub(crate) fn apply_taker_rebate_paid(
        &mut self,
        attribution_day_utc_ms: i64,
        platform_effective_at_utc_ms: i64,
        paid_pusd: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_rebate_paid(
            PlatformAccountDeltaReasonV2::TakerRebatePaid {
                attribution_day_utc_ms,
            },
            platform_effective_at_utc_ms,
            paid_pusd,
            false,
        )
    }

    fn apply_encumbrance_release(
        &mut self,
        reason: PlatformAccountDeltaReasonV2,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        released_pusd: Decimal,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        encumbrance_release_movement(released_pusd)?
            .map(|movement| {
                self.apply_transition(
                    reason,
                    Some(condition_id),
                    platform_effective_at_utc_ms,
                    movement,
                )
            })
            .transpose()
    }

    fn apply_rebate_day_finalized(
        &mut self,
        reason: PlatformAccountDeltaReasonV2,
        platform_effective_at_utc_ms: i64,
        locked_remainder_pusd: Decimal,
        maker: bool,
    ) -> Result<Option<PlatformAccountDeltaV2>, PlatformAccountError> {
        rebate_day_finalized_movement(locked_remainder_pusd, maker)?
            .map(|movement| {
                self.apply_transition(reason, None, platform_effective_at_utc_ms, movement)
            })
            .transpose()
    }

    fn apply_rebate_paid(
        &mut self,
        reason: PlatformAccountDeltaReasonV2,
        platform_effective_at_utc_ms: i64,
        paid_pusd: Decimal,
        maker: bool,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        self.apply_transition(
            reason,
            None,
            platform_effective_at_utc_ms,
            rebate_paid_movement(paid_pusd, maker)?,
        )
    }

    pub(crate) fn apply_external_cash(
        &mut self,
        identity: &str,
        delta: Decimal,
        at: i64,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        let units = positive_i128(decimal_to_micro_units(delta.abs(), "external cash")?)?
            * if delta.is_sign_negative() { -1 } else { 1 };
        let mut movement = PlatformAccountMovement::pusd(units, 0, 0);
        movement.cash_pusd_units_delta = units;
        self.apply_transition(
            PlatformAccountDeltaReasonV2::RequestedSettlement(
                contracts::economics::SourceNeutralAccountTransitionKindV2::ExternalCashApplied {
                    cash_identity: identity.into(),
                },
            ),
            None,
            at,
            movement,
        )
    }

    pub(crate) fn apply_settlement_cost(
        &mut self,
        condition: &ConditionId,
        transition: contracts::economics::SourceNeutralAccountTransitionKindV2<String>,
        at: i64,
        paid: Decimal,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        let units = positive_i128(decimal_to_micro_units(paid.abs(), "settlement payment")?)?;
        let units = if paid.is_sign_negative() {
            -units
        } else {
            units
        };
        let mut movement = PlatformAccountMovement::pusd(-units, 0, 0);
        movement.cash_pusd_units_delta = -units;
        self.apply_transition(
            PlatformAccountDeltaReasonV2::RequestedSettlement(transition),
            Some(condition),
            at,
            movement,
        )
    }

    fn apply_transition(
        &mut self,
        reason: PlatformAccountDeltaReasonV2,
        condition_id: Option<&ConditionId>,
        platform_effective_at_utc_ms: i64,
        movement: PlatformAccountMovement,
    ) -> Result<PlatformAccountDeltaV2, PlatformAccountError> {
        let balance_delta = movement.public_delta();
        let next = calculate_account_transition(
            AccountTransitionSource {
                cash_pusd_units: self.cash_pusd_units,
                available_pusd_units: self.available_pusd_units,
                principal_encumbrance_pusd_units: self.principal_encumbrance_pusd_units,
                immediate_taker_fee_encumbrance_pusd_units: self
                    .immediate_taker_fee_encumbrance_pusd_units,
                outcome_balance_units: &self.outcome_balance_units,
                maker_rebate_receivable_pusd: self.maker_rebate_receivable_pusd,
                taker_rebate_receivable_pusd: self.taker_rebate_receivable_pusd,
                maker_rebate_locked_remainder_pusd: self.maker_rebate_locked_remainder_pusd,
                taker_rebate_locked_remainder_pusd: self.taker_rebate_locked_remainder_pusd,
            },
            &movement,
        )?;
        let account_transition_seq = self.next_account_transition_seq;
        let next_account_transition_seq = account_transition_seq
            .checked_add(1)
            .ok_or(PlatformAccountError::SequenceOverflow)?;
        let delta = PlatformAccountDeltaV2 {
            account_instance_key: self.account_instance_key.clone(),
            account_transition_seq,
            reason,
            condition_id: condition_id.cloned(),
            platform_effective_at_utc_ms,
            balance_delta,
        };
        self.cash_pusd_units = next.cash_pusd_units;
        self.available_pusd_units = next.available_pusd_units;
        self.principal_encumbrance_pusd_units = next.principal_encumbrance_pusd_units;
        self.immediate_taker_fee_encumbrance_pusd_units =
            next.immediate_taker_fee_encumbrance_pusd_units;
        apply_outcome_balance_updates(
            &mut self.outcome_balance_units,
            next.outcome_balance_updates,
        );
        self.maker_rebate_receivable_pusd = next.maker_rebate_receivable_pusd;
        self.taker_rebate_receivable_pusd = next.taker_rebate_receivable_pusd;
        self.maker_rebate_locked_remainder_pusd = next.maker_rebate_locked_remainder_pusd;
        self.taker_rebate_locked_remainder_pusd = next.taker_rebate_locked_remainder_pusd;
        self.next_account_transition_seq = next_account_transition_seq;
        Ok(delta)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlatformAccountError {
    InvalidInitialAccountState,
    InvalidOpeningAnchor(&'static str),
    InvalidAmount(&'static str),
    InvalidTransitionIdentity,
    TransitionIdentityConflict(String),
    BalanceUnderflow(&'static str),
    ArithmeticOverflow,
    SequenceOverflow,
    Serialization(String),
    RestoredStateIntegrity(&'static str),
}

pub(crate) type PlatformAccountErrorV1 = PlatformAccountError;

impl fmt::Display for PlatformAccountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInitialAccountState => write!(formatter, "初始平台账户状态非法"),
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
            Self::RestoredStateIntegrity(message) => {
                write!(formatter, "恢复平台账户状态非法：{message}")
            }
        }
    }
}

impl std::error::Error for PlatformAccountError {}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn funded_account_v2() -> PlatformAccountStateV2 {
        PlatformAccountStateV2::try_from_opening_fact(&AccountOpeningFactV2 {
            creation_request_id: "create-a".to_owned(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "run-a".to_owned(),
                caller_role: contracts::SimCallerRoleV1::ModelTrain,
                caller_instance_id: "caller-a".to_owned(),
                account_id: "account-a".to_owned(),
                platform_source: contracts::PlatformSourceV1::Simulated,
            },
            session_id: "session-a".to_owned(),
            initial_platform_scenario_file_sha256: Sha256Hex("b".repeat(64)),
            initial_account_state: contracts::InitialAccountStateV2 {
                available_pusd: ExactDecimal("10".to_owned()),
            },
        })
        .expect("有效 V2 开户事实")
    }

    fn condition_a() -> ConditionId {
        ConditionId("condition-a".to_owned())
    }

    fn token_a() -> TokenId {
        TokenId("token-a".to_owned())
    }

    fn parsed_binary_payout_v2() -> [(TokenId, Decimal); 2] {
        [
            (TokenId("token-a".to_owned()), Decimal::ONE),
            (TokenId("token-b".to_owned()), Decimal::ZERO),
        ]
    }

    #[test]
    fn v2_account_uses_shared_arithmetic_without_delta_hash_material() {
        let mut v2 = funded_account_v2();
        let account_instance_key = v2.account_instance_key().clone();
        let v2_delta = v2
            .apply_order_accepted(
                "order-a",
                &condition_a(),
                100,
                Decimal::new(2, 0),
                Decimal::ZERO,
            )
            .expect("V2 账户变化");

        assert_eq!(v2.account_instance_key(), &account_instance_key);
        assert_eq!(v2.available_pusd(), Decimal::new(8, 0));
        assert_eq!(v2.principal_encumbrance_pusd(), Decimal::new(2, 0));
        assert_eq!(v2.immediate_taker_fee_encumbrance_pusd(), Decimal::ZERO);
        assert_eq!(v2.next_account_transition_seq(), 2);
        assert_eq!(
            v2_delta.balance_delta,
            PlatformAccountBalanceDeltaV1 {
                cash_pusd_delta: ExactDecimal("0".to_owned()),
                available_pusd_delta: ExactDecimal("-2".to_owned()),
                principal_encumbrance_pusd_delta: ExactDecimal("2".to_owned()),
                immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
                outcome_balance_deltas: Vec::new(),
                maker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
            }
        );
        let wire = serde_json::to_string(&v2_delta).expect("编码 V2 账户变化");
        assert!(!wire.contains("sha256"));
        assert!(!wire.contains("predecessor"));
        assert!(!wire.contains("delta_id"));
        assert!(!wire.contains("reason_identity"));
    }

    #[test]
    fn v2_mixed_economic_path_uses_one_structured_delta_sequence() {
        let mut account = funded_account_v2();
        let condition = condition_a();
        let token = token_a();
        let mut deltas = Vec::new();

        deltas.push(
            account
                .apply_order_accepted(
                    "order-unapplied",
                    &condition,
                    100,
                    Decimal::ONE,
                    Decimal::new(1, 1),
                )
                .expect("未应用成交前的资金占用"),
        );
        deltas.push(
            account
                .apply_fill_finality_unapplied_failure_release(
                    "trade-unapplied",
                    1,
                    &condition,
                    101,
                    1_000_000,
                    100_000,
                )
                .expect("未应用成交失败释放"),
        );
        deltas.push(
            account
                .apply_order_accepted(
                    "order-compensation",
                    &condition,
                    102,
                    Decimal::ONE,
                    Decimal::new(1, 1),
                )
                .expect("补偿成交前的资金占用"),
        );
        let compensated_amounts = FillEconomicAmountsV1 {
            base_units: 2_000_000,
            quote_units: 1_000_000,
            immediate_taker_fee_units: 100_000,
        };
        deltas.push(
            account
                .apply_fill_finality_economic(
                    "trade-compensation",
                    2,
                    &condition,
                    &token,
                    103,
                    compensated_amounts,
                )
                .expect("补偿前先应用成交经济结果"),
        );
        deltas.push(
            account
                .apply_fill_finality_compensation(
                    "trade-compensation",
                    2,
                    &condition,
                    &token,
                    104,
                    compensated_amounts,
                )
                .expect("成交补偿"),
        );

        for (order_id, at) in [
            ("order-fak", 105),
            ("order-cancel", 107),
            ("order-gtd", 109),
            ("order-dust", 111),
            ("order-resolution", 113),
        ] {
            deltas.push(
                account
                    .apply_order_accepted(
                        order_id,
                        &condition,
                        at,
                        Decimal::new(2, 1),
                        Decimal::ZERO,
                    )
                    .expect("释放前的订单资金占用"),
            );
            deltas.push(match order_id {
                "order-fak" => account
                    .apply_fak_unspent_release(order_id, &condition, at + 1, Decimal::new(2, 1))
                    .expect("FAK 未花预算释放")
                    .expect("正数释放形成账户变化"),
                "order-cancel" => account
                    .apply_caller_cancel_release(order_id, &condition, at + 1, Decimal::new(2, 1))
                    .expect("调用方撤单释放")
                    .expect("正数释放形成账户变化"),
                "order-gtd" => account
                    .apply_gtd_expiration_release(
                        order_id,
                        777,
                        &condition,
                        at + 1,
                        Decimal::new(2, 1),
                    )
                    .expect("GTD 到期释放")
                    .expect("正数释放形成账户变化"),
                "order-dust" => account
                    .apply_venue_dust_release(order_id, &condition, at + 1, Decimal::new(2, 1))
                    .expect("平台小额余量释放"),
                "order-resolution" => account
                    .apply_market_resolution_encumbrance_release(
                        "resolution-market",
                        order_id,
                        &condition,
                        at + 1,
                        Decimal::new(2, 1),
                    )
                    .expect("市场判定开放订单余量释放"),
                _ => unreachable!("固定测试订单集合"),
            });
        }

        deltas.push(
            account
                .apply_maker_rebate_accrual_finalized(&condition, 115, Decimal::new(2, 1))
                .expect("maker 返佣应计"),
        );
        deltas.push(
            account
                .apply_maker_rebate_day_finalized(0, 116, Decimal::new(5, 2))
                .expect("maker 返佣日结")
                .expect("正余数形成账户变化"),
        );
        deltas.push(
            account
                .apply_maker_rebate_paid(0, 117, Decimal::new(15, 2))
                .expect("maker 返佣付款"),
        );
        deltas.push(
            account
                .apply_taker_rebate_accrual_applied(
                    "trade-rebate",
                    &condition,
                    118,
                    Decimal::new(2, 1),
                )
                .expect("taker 返佣应计"),
        );
        deltas.push(
            account
                .apply_taker_rebate_day_finalized(1, 119, Decimal::new(5, 2))
                .expect("taker 返佣日结")
                .expect("正余数形成账户变化"),
        );
        deltas.push(
            account
                .apply_taker_rebate_paid(1, 120, Decimal::new(15, 2))
                .expect("taker 返佣付款"),
        );

        let expected_reasons = [
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-unapplied".to_owned(),
            },
            PlatformAccountDeltaReasonV2::FillFinalityUnappliedFailureReleased {
                trade_id: "trade-unapplied".to_owned(),
                path_ordinal: 1,
            },
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-compensation".to_owned(),
            },
            PlatformAccountDeltaReasonV2::FillFinalityEconomicApplied {
                trade_id: "trade-compensation".to_owned(),
                path_ordinal: 2,
            },
            PlatformAccountDeltaReasonV2::FillFinalityCompensationApplied {
                trade_id: "trade-compensation".to_owned(),
                path_ordinal: 2,
            },
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-fak".to_owned(),
            },
            PlatformAccountDeltaReasonV2::FakUnspentEncumbranceReleased {
                platform_order_id: "order-fak".to_owned(),
            },
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-cancel".to_owned(),
            },
            PlatformAccountDeltaReasonV2::CallerCancellationEncumbranceReleased {
                platform_order_id: "order-cancel".to_owned(),
            },
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-gtd".to_owned(),
            },
            PlatformAccountDeltaReasonV2::GtdExpirationEncumbranceReleased {
                platform_order_id: "order-gtd".to_owned(),
                encoded_expires_at_utc_ms: 777,
            },
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-dust".to_owned(),
            },
            PlatformAccountDeltaReasonV2::VenueDustEncumbranceReleased {
                platform_order_id: "order-dust".to_owned(),
            },
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id: "order-resolution".to_owned(),
            },
            PlatformAccountDeltaReasonV2::MarketResolutionEncumbranceReleased {
                resolution_event_id: "resolution-market".to_owned(),
                platform_order_id: "order-resolution".to_owned(),
            },
            PlatformAccountDeltaReasonV2::MakerRebateAccrualFinalized,
            PlatformAccountDeltaReasonV2::MakerRebateDayFinalized {
                attribution_day_utc_ms: 0,
            },
            PlatformAccountDeltaReasonV2::MakerRebatePaid {
                attribution_day_utc_ms: 0,
            },
            PlatformAccountDeltaReasonV2::TakerRebateAccrualApplied {
                trade_id: "trade-rebate".to_owned(),
            },
            PlatformAccountDeltaReasonV2::TakerRebateDayFinalized {
                attribution_day_utc_ms: 1,
            },
            PlatformAccountDeltaReasonV2::TakerRebatePaid {
                attribution_day_utc_ms: 1,
            },
        ];
        assert_eq!(deltas.len(), expected_reasons.len());
        for (index, (delta, expected_reason)) in
            deltas.iter().zip(expected_reasons.iter()).enumerate()
        {
            assert_eq!(delta.account_transition_seq, index as u64 + 1);
            assert_eq!(&delta.reason, expected_reason);
        }
        assert_eq!(account.cash_pusd_units, 10_300_000);
        assert_eq!(account.available_pusd_units, 10_300_000);
        assert_eq!(account.principal_encumbrance_pusd_units, 0);
        assert_eq!(account.immediate_taker_fee_encumbrance_pusd_units, 0);
        assert_eq!(account.outcome_balance_units.get("token-a"), Some(&0));
        assert_eq!(account.maker_rebate_receivable_pusd, Decimal::ZERO);
        assert_eq!(account.taker_rebate_receivable_pusd, Decimal::ZERO);
        assert_eq!(
            account.maker_rebate_locked_remainder_pusd,
            Decimal::new(5, 2)
        );
        assert_eq!(
            account.taker_rebate_locked_remainder_pusd,
            Decimal::new(5, 2)
        );
        assert_eq!(account.next_account_transition_seq(), 22);
    }

    #[test]
    fn v2_auto_redeem_positive_and_zero_holdings_share_the_business_calculation() {
        let mut funded = funded_account_v2();
        funded
            .apply_order_accepted(
                "order-winning",
                &condition_a(),
                200,
                Decimal::new(2, 0),
                Decimal::ZERO,
            )
            .expect("V2 赎回前订单资金占用");
        funded
            .apply_fill_finality_economic(
                "trade-winning",
                0,
                &condition_a(),
                &token_a(),
                201,
                FillEconomicAmountsV1 {
                    base_units: 3_000_000,
                    quote_units: 2_000_000,
                    immediate_taker_fee_units: 0,
                },
            )
            .expect("V2 赎回前成交经济应用");

        let positive = funded
            .apply_auto_redeem(
                "resolution-a",
                &condition_a(),
                202,
                &parsed_binary_payout_v2(),
            )
            .expect("V2 正持仓自动赎回");
        assert_eq!(positive.redeemed_pusd, Decimal::new(3, 0));
        assert_eq!(
            positive
                .account_delta
                .expect("正持仓形成 V2 账户变化")
                .reason,
            PlatformAccountDeltaReasonV2::AutoRedeemApplied {
                resolution_event_id: "resolution-a".to_owned(),
            }
        );
        assert_eq!(funded.cash_pusd_units, 11_000_000);
        assert_eq!(funded.available_pusd_units, 11_000_000);
        assert!(
            funded
                .outcome_balance_units
                .values()
                .all(|balance| *balance == 0)
        );
        assert_eq!(funded.next_account_transition_seq(), 4);

        let mut empty = funded_account_v2();
        let before_sequence = empty.next_account_transition_seq();
        let zero = empty
            .apply_auto_redeem(
                "resolution-empty",
                &condition_a(),
                203,
                &parsed_binary_payout_v2(),
            )
            .expect("V2 零持仓自动赎回");
        assert_eq!(zero.redeemed_pusd, Decimal::ZERO);
        assert!(zero.account_delta.is_none());
        assert_eq!(
            zero.redeemed_outcome_balances,
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
        assert_eq!(empty.available_pusd(), Decimal::new(10, 0));
        assert_eq!(empty.next_account_transition_seq(), before_sequence);
    }
}
