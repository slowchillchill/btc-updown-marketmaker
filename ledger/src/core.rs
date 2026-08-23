use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use contracts::execution_normalization::{
    NoeResultReferenceV1, SourceNeutralAccountBalanceDeltaV1, SourceNeutralEconomicDispositionV1,
    SourceNeutralEconomicIdentityV1,
};
use contracts::{AccountInstanceKeyV1, ConditionId, ExactDecimal, TokenId};
use execution_normalization::{DurableErrorV1, NormalizationRecordV1, read_noe_result_v1};
use rust_decimal::Decimal;

use crate::projection::{
    AccountAdmissionPolicyV1, LedgerLocalPendingReservationProjectionV1, LedgerMarketProjectionV1,
    LedgerOutcomeBalanceV1, LedgerPlatformConfirmedProjectionV1, LedgerProjectionV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerApplyOutcomeV1 {
    NewlyApplied {
        ledger_entry_seq: u64,
    },
    AlreadyApplied {
        ledger_entry_seq: u64,
    },
    MissingPlatformPredecessor {
        expected_account_transition_seq: u64,
        found_account_transition_seq: u64,
    },
}

#[derive(Debug)]
pub enum LedgerErrorV1 {
    NoeRead(DurableErrorV1),
    NonEconomicRecord,
    AccountTransitionBeforeOpening,
    DifferentOpeningEvidence {
        existing: String,
        found: String,
    },
    AccountIsolation {
        expected: Box<AccountInstanceKeyV1>,
        found: Box<AccountInstanceKeyV1>,
    },
    DecimalParse {
        field: &'static str,
        value: String,
    },
    ArithmeticOverflow {
        field: &'static str,
    },
    UnbalancedTransaction {
        asset: String,
    },
}

impl fmt::Display for LedgerErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoeRead(error) => write!(formatter, "NOE 结果读取失败：{error}"),
            Self::NonEconomicRecord => write!(formatter, "NOE 结果不是经济事实"),
            Self::AccountTransitionBeforeOpening => write!(formatter, "账户变化早于开户"),
            Self::DifferentOpeningEvidence { existing, found } => write!(
                formatter,
                "账户实例已由不同开户证据建立：已有 {existing}，收到 {found}"
            ),
            Self::AccountIsolation { expected, found } => write!(
                formatter,
                "NOE 结果属于不同账户实例：预期 {expected:?}，收到 {found:?}"
            ),
            Self::DecimalParse { field, value } => {
                write!(formatter, "固定精度字段无法解析：{field}={value}")
            }
            Self::ArithmeticOverflow { field } => write!(formatter, "固定精度运算溢出：{field}"),
            Self::UnbalancedTransaction { asset } => {
                write!(formatter, "Ledger 自有交易不平衡：{asset}")
            }
        }
    }
}

impl Error for LedgerErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NoeRead(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DurableErrorV1> for LedgerErrorV1 {
    fn from(error: DurableErrorV1) -> Self {
        Self::NoeRead(error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NoeReferenceKeyV1 {
    location: String,
    boundary_sha256: String,
    record_ordinal: u64,
}

impl From<&NoeResultReferenceV1> for NoeReferenceKeyV1 {
    fn from(reference: &NoeResultReferenceV1) -> Self {
        Self {
            location: reference.commit_container.location.clone(),
            boundary_sha256: reference.commit_container.boundary_sha256.0.clone(),
            record_ordinal: reference.record_ordinal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum LedgerAssetV1 {
    Pusd,
    Outcome(String),
}

impl LedgerAssetV1 {
    fn label(&self) -> String {
        match self {
            Self::Pusd => "pUSD".to_owned(),
            Self::Outcome(token_id) => format!("outcome:{token_id}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LedgerAccountV1 {
    Cash,
    Available,
    PrincipalEncumbrance(Option<String>),
    ImmediateTakerFeeEncumbrance(Option<String>),
    OutcomeBalance {
        condition_id: Option<String>,
        token_id: String,
    },
    MakerRebateReceivable(Option<String>),
    TakerRebateReceivable(Option<String>),
    MakerRebateLockedRemainder(Option<String>),
    TakerRebateLockedRemainder(Option<String>),
    Balancing,
}

impl LedgerAccountV1 {
    fn supports(&self, asset: &LedgerAssetV1) -> bool {
        match self {
            Self::Cash | Self::Available | Self::Balancing => {
                matches!(asset, LedgerAssetV1::Pusd) || matches!(self, Self::Balancing)
            }
            Self::PrincipalEncumbrance(_)
            | Self::ImmediateTakerFeeEncumbrance(_)
            | Self::MakerRebateReceivable(_)
            | Self::TakerRebateReceivable(_)
            | Self::MakerRebateLockedRemainder(_)
            | Self::TakerRebateLockedRemainder(_) => matches!(asset, LedgerAssetV1::Pusd),
            Self::OutcomeBalance { token_id, .. } => {
                matches!(asset, LedgerAssetV1::Outcome(found) if found == token_id)
            }
        }
    }
}

type LedgerEntryV1 = (LedgerAccountV1, LedgerAssetV1, Decimal);

#[derive(Clone, Debug)]
struct LedgerTransactionV1 {
    ledger_entry_seq: u64,
    source_reference: NoeResultReferenceV1,
    entries: Vec<LedgerEntryV1>,
}

#[derive(Clone, Debug)]
struct OutcomeBalanceStateV1 {
    token_id: TokenId,
    balance: Decimal,
}

#[derive(Clone, Debug)]
struct BalanceStateV1 {
    cash_pusd: Decimal,
    available_pusd: Decimal,
    principal_encumbrance_pusd: Decimal,
    immediate_taker_fee_encumbrance_pusd: Decimal,
    outcome_balances: BTreeMap<String, OutcomeBalanceStateV1>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
}

impl BalanceStateV1 {
    fn opened(available_pusd: Decimal) -> Self {
        Self {
            cash_pusd: available_pusd,
            available_pusd,
            principal_encumbrance_pusd: Decimal::ZERO,
            immediate_taker_fee_encumbrance_pusd: Decimal::ZERO,
            outcome_balances: BTreeMap::new(),
            maker_rebate_receivable_pusd: Decimal::ZERO,
            taker_rebate_receivable_pusd: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd: Decimal::ZERO,
        }
    }

    fn empty() -> Self {
        Self::opened(Decimal::ZERO)
    }
}

#[derive(Clone, Debug)]
struct MarketStateV1 {
    condition_id: ConditionId,
    balances: BalanceStateV1,
}

#[derive(Debug)]
struct OpenedAccountStateV1 {
    account_instance_key: AccountInstanceKeyV1,
    opening_evidence_identity: String,
    platform_source_prefix: u64,
    available_through_utc_ms: i64,
    balances: BalanceStateV1,
    markets: BTreeMap<String, MarketStateV1>,
}

#[derive(Clone, Debug)]
struct ParsedBalanceDeltaV1 {
    cash_pusd: Decimal,
    available_pusd: Decimal,
    principal_encumbrance_pusd: Decimal,
    immediate_taker_fee_encumbrance_pusd: Decimal,
    outcome_balances: BTreeMap<String, OutcomeBalanceStateV1>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
}

#[derive(Debug)]
struct BalanceUpdateV1 {
    cash_pusd: Decimal,
    available_pusd: Decimal,
    principal_encumbrance_pusd: Decimal,
    immediate_taker_fee_encumbrance_pusd: Decimal,
    outcome_balances: BTreeMap<String, OutcomeBalanceStateV1>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
}

pub struct LedgerWriterV1 {
    policy: AccountAdmissionPolicyV1,
    account: Option<OpenedAccountStateV1>,
    transactions: Vec<LedgerTransactionV1>,
    applied_references: BTreeMap<NoeReferenceKeyV1, u64>,
}

impl LedgerWriterV1 {
    pub fn new(policy: AccountAdmissionPolicyV1) -> Self {
        Self {
            policy,
            account: None,
            transactions: Vec::new(),
            applied_references: BTreeMap::new(),
        }
    }

    pub fn admission_policy(&self) -> &AccountAdmissionPolicyV1 {
        &self.policy
    }

    pub fn apply_noe_result(
        &mut self,
        reference: &NoeResultReferenceV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        let reference_key = NoeReferenceKeyV1::from(reference);
        if let Some(ledger_entry_seq) = self.applied_references.get(&reference_key) {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: *ledger_entry_seq,
            });
        }

        let resolved = read_noe_result_v1(reference)?;
        let NormalizationRecordV1::EconomicFact { fact, .. } = &resolved.record else {
            return Err(LedgerErrorV1::NonEconomicRecord);
        };

        match &fact.disposition {
            SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } => self
                .apply_opening(
                    reference,
                    &resolved.context.source.account_instance_key,
                    &resolved.context.source.account_opening_evidence_identity,
                    resolved.boundary.durable_available_at_utc_ms,
                    available_pusd,
                ),
            SourceNeutralEconomicDispositionV1::AccountTransition {
                condition_id,
                balance_delta,
                ..
            } => {
                let SourceNeutralEconomicIdentityV1::AccountTransition {
                    account_transition_seq,
                    ..
                } = &fact.economic_identity
                else {
                    unreachable!("NOE 经济事实保证账户变化身份与正文一致")
                };
                self.apply_transition(
                    reference,
                    &resolved.context.source.account_instance_key,
                    *account_transition_seq,
                    resolved.boundary.durable_available_at_utc_ms,
                    condition_id.as_ref(),
                    balance_delta,
                )
            }
        }
    }

    pub fn projection(&self) -> Option<LedgerProjectionV1> {
        let account = self.account.as_ref()?;
        let platform_confirmed_projection = project_balances(&account.balances);
        let market_projections = account
            .markets
            .values()
            .map(|market| {
                let balances = project_balances(&market.balances);
                LedgerMarketProjectionV1 {
                    condition_id: market.condition_id.clone(),
                    principal_encumbrance_pusd: balances.principal_encumbrance_pusd,
                    immediate_taker_fee_encumbrance_pusd: balances
                        .immediate_taker_fee_encumbrance_pusd,
                    local_pending_principal_pusd: zero_exact(),
                    local_pending_immediate_fee_pusd: zero_exact(),
                    outcome_balances: balances.outcome_balances,
                    maker_rebate_receivable_pusd: balances.maker_rebate_receivable_pusd,
                    taker_rebate_receivable_pusd: balances.taker_rebate_receivable_pusd,
                    maker_rebate_locked_remainder_pusd: balances.maker_rebate_locked_remainder_pusd,
                    taker_rebate_locked_remainder_pusd: balances.taker_rebate_locked_remainder_pusd,
                    position_cost_basis_pusd: zero_exact(),
                    fees_pusd: zero_exact(),
                    redeemable_receivable_pusd: zero_exact(),
                    realized_pnl_pusd: zero_exact(),
                }
            })
            .collect();

        Some(LedgerProjectionV1 {
            account_instance_key: account.account_instance_key.clone(),
            ledger_entry_seq: self.current_ledger_entry_seq(),
            platform_source_prefix: account.platform_source_prefix,
            ledger_local_prefix: 0,
            available_through_utc_ms: account.available_through_utc_ms,
            platform_confirmed_projection,
            local_pending_reservation_projection: LedgerLocalPendingReservationProjectionV1 {
                principal_pusd: zero_exact(),
                immediate_fee_pusd: zero_exact(),
            },
            market_projections,
        })
    }

    fn apply_opening(
        &mut self,
        reference: &NoeResultReferenceV1,
        account_instance_key: &AccountInstanceKeyV1,
        opening_evidence_identity: &str,
        available_through_utc_ms: i64,
        available_pusd: &ExactDecimal,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        if let Some(account) = &self.account {
            if account.account_instance_key != *account_instance_key {
                return Err(LedgerErrorV1::AccountIsolation {
                    expected: Box::new(account.account_instance_key.clone()),
                    found: Box::new(account_instance_key.clone()),
                });
            }
            return Err(LedgerErrorV1::DifferentOpeningEvidence {
                existing: account.opening_evidence_identity.clone(),
                found: opening_evidence_identity.to_owned(),
            });
        }

        let available = parse_decimal(available_pusd, "opening.available_pusd")?;
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let entries = opening_entries(available)?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source_reference: reference.clone(),
            entries,
        };
        ensure_balanced(&transaction.entries)?;

        self.account = Some(OpenedAccountStateV1 {
            account_instance_key: account_instance_key.clone(),
            opening_evidence_identity: opening_evidence_identity.to_owned(),
            platform_source_prefix: 0,
            available_through_utc_ms,
            balances: BalanceStateV1::opened(available),
            markets: BTreeMap::new(),
        });
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    fn apply_transition(
        &mut self,
        reference: &NoeResultReferenceV1,
        account_instance_key: &AccountInstanceKeyV1,
        account_transition_seq: u64,
        available_through_utc_ms: i64,
        condition_id: Option<&ConditionId>,
        balance_delta: &SourceNeutralAccountBalanceDeltaV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        let account = self
            .account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountTransitionBeforeOpening)?;
        if account.account_instance_key != *account_instance_key {
            return Err(LedgerErrorV1::AccountIsolation {
                expected: Box::new(account.account_instance_key.clone()),
                found: Box::new(account_instance_key.clone()),
            });
        }
        let expected_account_transition_seq = account.platform_source_prefix.checked_add(1).ok_or(
            LedgerErrorV1::ArithmeticOverflow {
                field: "account_transition_seq",
            },
        )?;
        if account_transition_seq != expected_account_transition_seq {
            return Ok(LedgerApplyOutcomeV1::MissingPlatformPredecessor {
                expected_account_transition_seq,
                found_account_transition_seq: account_transition_seq,
            });
        }

        let parsed = parse_balance_delta(balance_delta)?;
        let global_update = prepare_balance_update(&account.balances, &parsed, "account")?;
        let market_update = condition_id
            .map(|condition_id| {
                let empty = BalanceStateV1::empty();
                let existing = account
                    .markets
                    .get(&condition_id.0)
                    .map(|market| &market.balances)
                    .unwrap_or(&empty);
                prepare_market_balance_update(existing, &parsed).map(|balances| MarketStateV1 {
                    condition_id: condition_id.clone(),
                    balances: balances.into_state(),
                })
            })
            .transpose()?;

        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let entries = transition_entries(condition_id, &parsed)?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source_reference: reference.clone(),
            entries,
        };
        ensure_balanced(&transaction.entries)?;

        let account = self
            .account
            .as_mut()
            .expect("开户状态在无副作用准备期间保持存在");
        account.balances = global_update.into_state();
        if let Some(market) = market_update {
            account
                .markets
                .insert(market.condition_id.0.clone(), market);
        }
        account.platform_source_prefix = account_transition_seq;
        account.available_through_utc_ms = available_through_utc_ms;
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    fn current_ledger_entry_seq(&self) -> u64 {
        self.transactions
            .last()
            .map(|transaction| transaction.ledger_entry_seq)
            .unwrap_or(0)
    }

    fn next_ledger_entry_seq(&self) -> Result<u64, LedgerErrorV1> {
        self.current_ledger_entry_seq()
            .checked_add(1)
            .ok_or(LedgerErrorV1::ArithmeticOverflow {
                field: "ledger_entry_seq",
            })
    }

    fn commit_transaction(&mut self, transaction: LedgerTransactionV1) {
        self.applied_references.insert(
            NoeReferenceKeyV1::from(&transaction.source_reference),
            transaction.ledger_entry_seq,
        );
        self.transactions.push(transaction);
    }
}

impl BalanceUpdateV1 {
    fn into_state(self) -> BalanceStateV1 {
        BalanceStateV1 {
            cash_pusd: self.cash_pusd,
            available_pusd: self.available_pusd,
            principal_encumbrance_pusd: self.principal_encumbrance_pusd,
            immediate_taker_fee_encumbrance_pusd: self.immediate_taker_fee_encumbrance_pusd,
            outcome_balances: self.outcome_balances,
            maker_rebate_receivable_pusd: self.maker_rebate_receivable_pusd,
            taker_rebate_receivable_pusd: self.taker_rebate_receivable_pusd,
            maker_rebate_locked_remainder_pusd: self.maker_rebate_locked_remainder_pusd,
            taker_rebate_locked_remainder_pusd: self.taker_rebate_locked_remainder_pusd,
        }
    }
}

fn prepare_balance_update(
    current: &BalanceStateV1,
    delta: &ParsedBalanceDeltaV1,
    scope: &'static str,
) -> Result<BalanceUpdateV1, LedgerErrorV1> {
    let mut outcome_balances = current.outcome_balances.clone();
    for (key, outcome_delta) in &delta.outcome_balances {
        let existing = outcome_balances
            .get(key)
            .map(|outcome| outcome.balance)
            .unwrap_or(Decimal::ZERO);
        outcome_balances.insert(
            key.clone(),
            OutcomeBalanceStateV1 {
                token_id: outcome_delta.token_id.clone(),
                balance: checked_add(
                    existing,
                    outcome_delta.balance,
                    "outcome_balance.balance_shares",
                )?,
            },
        );
    }
    Ok(BalanceUpdateV1 {
        cash_pusd: checked_add(current.cash_pusd, delta.cash_pusd, scope)?,
        available_pusd: checked_add(current.available_pusd, delta.available_pusd, scope)?,
        principal_encumbrance_pusd: checked_add(
            current.principal_encumbrance_pusd,
            delta.principal_encumbrance_pusd,
            scope,
        )?,
        immediate_taker_fee_encumbrance_pusd: checked_add(
            current.immediate_taker_fee_encumbrance_pusd,
            delta.immediate_taker_fee_encumbrance_pusd,
            scope,
        )?,
        outcome_balances,
        maker_rebate_receivable_pusd: checked_add(
            current.maker_rebate_receivable_pusd,
            delta.maker_rebate_receivable_pusd,
            scope,
        )?,
        taker_rebate_receivable_pusd: checked_add(
            current.taker_rebate_receivable_pusd,
            delta.taker_rebate_receivable_pusd,
            scope,
        )?,
        maker_rebate_locked_remainder_pusd: checked_add(
            current.maker_rebate_locked_remainder_pusd,
            delta.maker_rebate_locked_remainder_pusd,
            scope,
        )?,
        taker_rebate_locked_remainder_pusd: checked_add(
            current.taker_rebate_locked_remainder_pusd,
            delta.taker_rebate_locked_remainder_pusd,
            scope,
        )?,
    })
}

fn prepare_market_balance_update(
    current: &BalanceStateV1,
    delta: &ParsedBalanceDeltaV1,
) -> Result<BalanceUpdateV1, LedgerErrorV1> {
    let mut market_delta = delta.clone();
    market_delta.cash_pusd = Decimal::ZERO;
    market_delta.available_pusd = Decimal::ZERO;
    prepare_balance_update(current, &market_delta, "market")
}

fn parse_balance_delta(
    delta: &SourceNeutralAccountBalanceDeltaV1,
) -> Result<ParsedBalanceDeltaV1, LedgerErrorV1> {
    let mut outcome_balances: BTreeMap<String, OutcomeBalanceStateV1> = BTreeMap::new();
    for outcome in &delta.outcome_balance_deltas {
        let parsed = parse_decimal(
            &outcome.balance_shares_delta,
            "balance_delta.outcome_balance.balance_shares_delta",
        )?;
        let entry = outcome_balances
            .entry(outcome.token_id.0.clone())
            .or_insert_with(|| OutcomeBalanceStateV1 {
                token_id: outcome.token_id.clone(),
                balance: Decimal::ZERO,
            });
        entry.balance = checked_add(
            entry.balance,
            parsed,
            "balance_delta.outcome_balance.balance_shares_delta",
        )?;
    }
    Ok(ParsedBalanceDeltaV1 {
        cash_pusd: parse_decimal(&delta.cash_pusd_delta, "balance_delta.cash_pusd_delta")?,
        available_pusd: parse_decimal(
            &delta.available_pusd_delta,
            "balance_delta.available_pusd_delta",
        )?,
        principal_encumbrance_pusd: parse_decimal(
            &delta.principal_encumbrance_pusd_delta,
            "balance_delta.principal_encumbrance_pusd_delta",
        )?,
        immediate_taker_fee_encumbrance_pusd: parse_decimal(
            &delta.immediate_taker_fee_encumbrance_pusd_delta,
            "balance_delta.immediate_taker_fee_encumbrance_pusd_delta",
        )?,
        outcome_balances,
        maker_rebate_receivable_pusd: parse_decimal(
            &delta.maker_rebate_receivable_pusd_delta,
            "balance_delta.maker_rebate_receivable_pusd_delta",
        )?,
        taker_rebate_receivable_pusd: parse_decimal(
            &delta.taker_rebate_receivable_pusd_delta,
            "balance_delta.taker_rebate_receivable_pusd_delta",
        )?,
        maker_rebate_locked_remainder_pusd: parse_decimal(
            &delta.maker_rebate_locked_remainder_pusd_delta,
            "balance_delta.maker_rebate_locked_remainder_pusd_delta",
        )?,
        taker_rebate_locked_remainder_pusd: parse_decimal(
            &delta.taker_rebate_locked_remainder_pusd_delta,
            "balance_delta.taker_rebate_locked_remainder_pusd_delta",
        )?,
    })
}

fn transition_entries(
    condition_id: Option<&ConditionId>,
    delta: &ParsedBalanceDeltaV1,
) -> Result<Vec<LedgerEntryV1>, LedgerErrorV1> {
    let condition = condition_id.map(|condition_id| condition_id.0.clone());
    let mut entries = Vec::new();
    for (account, amount, field) in [
        (
            LedgerAccountV1::Cash,
            delta.cash_pusd,
            "balance_delta.cash_pusd_delta",
        ),
        (
            LedgerAccountV1::Available,
            delta.available_pusd,
            "balance_delta.available_pusd_delta",
        ),
        (
            LedgerAccountV1::PrincipalEncumbrance(condition.clone()),
            delta.principal_encumbrance_pusd,
            "balance_delta.principal_encumbrance_pusd_delta",
        ),
        (
            LedgerAccountV1::ImmediateTakerFeeEncumbrance(condition.clone()),
            delta.immediate_taker_fee_encumbrance_pusd,
            "balance_delta.immediate_taker_fee_encumbrance_pusd_delta",
        ),
        (
            LedgerAccountV1::MakerRebateReceivable(condition.clone()),
            delta.maker_rebate_receivable_pusd,
            "balance_delta.maker_rebate_receivable_pusd_delta",
        ),
        (
            LedgerAccountV1::TakerRebateReceivable(condition.clone()),
            delta.taker_rebate_receivable_pusd,
            "balance_delta.taker_rebate_receivable_pusd_delta",
        ),
        (
            LedgerAccountV1::MakerRebateLockedRemainder(condition.clone()),
            delta.maker_rebate_locked_remainder_pusd,
            "balance_delta.maker_rebate_locked_remainder_pusd_delta",
        ),
        (
            LedgerAccountV1::TakerRebateLockedRemainder(condition.clone()),
            delta.taker_rebate_locked_remainder_pusd,
            "balance_delta.taker_rebate_locked_remainder_pusd_delta",
        ),
    ] {
        push_balanced_entry(&mut entries, account, LedgerAssetV1::Pusd, amount, field)?;
    }
    for outcome in delta.outcome_balances.values() {
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::OutcomeBalance {
                condition_id: condition.clone(),
                token_id: outcome.token_id.0.clone(),
            },
            LedgerAssetV1::Outcome(outcome.token_id.0.clone()),
            outcome.balance,
            "balance_delta.outcome_balance.balance_shares_delta",
        )?;
    }
    Ok(entries)
}

fn opening_entries(available_pusd: Decimal) -> Result<Vec<LedgerEntryV1>, LedgerErrorV1> {
    let mut entries = Vec::new();
    push_balanced_entry(
        &mut entries,
        LedgerAccountV1::Cash,
        LedgerAssetV1::Pusd,
        available_pusd,
        "opening.cash_pusd",
    )?;
    push_balanced_entry(
        &mut entries,
        LedgerAccountV1::Available,
        LedgerAssetV1::Pusd,
        available_pusd,
        "opening.available_pusd",
    )?;
    Ok(entries)
}

fn push_balanced_entry(
    entries: &mut Vec<LedgerEntryV1>,
    account: LedgerAccountV1,
    asset: LedgerAssetV1,
    amount: Decimal,
    field: &'static str,
) -> Result<(), LedgerErrorV1> {
    if amount.is_zero() {
        return Ok(());
    }
    let offset = Decimal::ZERO
        .checked_sub(amount)
        .ok_or(LedgerErrorV1::ArithmeticOverflow { field })?;
    entries.push((account, asset.clone(), amount));
    entries.push((LedgerAccountV1::Balancing, asset, offset));
    Ok(())
}

fn ensure_balanced(entries: &[LedgerEntryV1]) -> Result<(), LedgerErrorV1> {
    let mut totals = BTreeMap::<LedgerAssetV1, Decimal>::new();
    for (account, asset, amount) in entries {
        if !account.supports(asset) {
            return Err(LedgerErrorV1::UnbalancedTransaction {
                asset: asset.label(),
            });
        }
        let total = totals.entry(asset.clone()).or_insert(Decimal::ZERO);
        *total = total
            .checked_add(*amount)
            .ok_or(LedgerErrorV1::ArithmeticOverflow {
                field: "ledger_transaction_balance",
            })?;
    }
    if let Some((asset, _)) = totals.iter().find(|(_, total)| !total.is_zero()) {
        return Err(LedgerErrorV1::UnbalancedTransaction {
            asset: asset.label(),
        });
    }
    Ok(())
}

fn parse_decimal(value: &ExactDecimal, field: &'static str) -> Result<Decimal, LedgerErrorV1> {
    Decimal::from_str_exact(&value.0).map_err(|_| LedgerErrorV1::DecimalParse {
        field,
        value: value.0.clone(),
    })
}

fn checked_add(
    left: Decimal,
    right: Decimal,
    field: &'static str,
) -> Result<Decimal, LedgerErrorV1> {
    left.checked_add(right)
        .ok_or(LedgerErrorV1::ArithmeticOverflow { field })
}

fn project_balances(state: &BalanceStateV1) -> LedgerPlatformConfirmedProjectionV1 {
    LedgerPlatformConfirmedProjectionV1 {
        cash_pusd: exact(state.cash_pusd),
        available_pusd: exact(state.available_pusd),
        principal_encumbrance_pusd: exact(state.principal_encumbrance_pusd),
        immediate_taker_fee_encumbrance_pusd: exact(state.immediate_taker_fee_encumbrance_pusd),
        outcome_balances: state
            .outcome_balances
            .values()
            .map(|outcome| LedgerOutcomeBalanceV1 {
                token_id: outcome.token_id.clone(),
                balance_shares: exact(outcome.balance),
            })
            .collect(),
        maker_rebate_receivable_pusd: exact(state.maker_rebate_receivable_pusd),
        taker_rebate_receivable_pusd: exact(state.taker_rebate_receivable_pusd),
        maker_rebate_locked_remainder_pusd: exact(state.maker_rebate_locked_remainder_pusd),
        taker_rebate_locked_remainder_pusd: exact(state.taker_rebate_locked_remainder_pusd),
        position_cost_basis_pusd: zero_exact(),
        fees_pusd: zero_exact(),
        redeemable_receivable_pusd: zero_exact(),
        realized_pnl_pusd: zero_exact(),
    }
}

fn exact(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.to_string())
}

fn zero_exact() -> ExactDecimal {
    ExactDecimal("0".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_owned_balance_boundary_rejects_an_unpaired_posting() {
        let entries = vec![(LedgerAccountV1::Cash, LedgerAssetV1::Pusd, Decimal::ONE)];
        assert!(matches!(
            ensure_balanced(&entries),
            Err(LedgerErrorV1::UnbalancedTransaction { asset }) if asset == "pUSD"
        ));
    }

    #[test]
    fn a_balanced_posting_pair_preserves_the_explicit_amount() {
        const EXPECTED_AMOUNT: &str = "10.12345678";
        let amount = EXPECTED_AMOUNT.parse::<Decimal>().unwrap();
        let mut entries = Vec::new();
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::PrincipalEncumbrance(Some("condition-a".to_owned())),
            LedgerAssetV1::Pusd,
            amount,
            "test",
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].2.to_string(), EXPECTED_AMOUNT);
        assert_eq!(entries[1].2.to_string(), "-10.12345678");
        ensure_balanced(&entries).unwrap();
    }

    #[test]
    fn representative_balance_delta_maps_to_explicit_balanced_entries() {
        const CONDITION: &str = "condition-a";
        const TOKEN: &str = "token-a";
        const PRINCIPAL: &str = "10.00000000";
        const FEE: &str = "0.12345678";
        const SHARES: &str = "20.24691356";
        let delta = ParsedBalanceDeltaV1 {
            cash_pusd: Decimal::ZERO,
            available_pusd: "-10.12345678".parse().unwrap(),
            principal_encumbrance_pusd: PRINCIPAL.parse().unwrap(),
            immediate_taker_fee_encumbrance_pusd: FEE.parse().unwrap(),
            outcome_balances: BTreeMap::from([(
                TOKEN.to_owned(),
                OutcomeBalanceStateV1 {
                    token_id: TokenId(TOKEN.to_owned()),
                    balance: SHARES.parse().unwrap(),
                },
            )]),
            maker_rebate_receivable_pusd: Decimal::ZERO,
            taker_rebate_receivable_pusd: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd: Decimal::ZERO,
        };

        let entries = transition_entries(Some(&ConditionId(CONDITION.to_owned())), &delta).unwrap();
        assert_eq!(
            entries,
            vec![
                (
                    LedgerAccountV1::Available,
                    LedgerAssetV1::Pusd,
                    "-10.12345678".parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "10.12345678".parse().unwrap(),
                ),
                (
                    LedgerAccountV1::PrincipalEncumbrance(Some(CONDITION.to_owned())),
                    LedgerAssetV1::Pusd,
                    PRINCIPAL.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "-10.00000000".parse().unwrap(),
                ),
                (
                    LedgerAccountV1::ImmediateTakerFeeEncumbrance(Some(CONDITION.to_owned())),
                    LedgerAssetV1::Pusd,
                    FEE.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "-0.12345678".parse().unwrap(),
                ),
                (
                    LedgerAccountV1::OutcomeBalance {
                        condition_id: Some(CONDITION.to_owned()),
                        token_id: TOKEN.to_owned(),
                    },
                    LedgerAssetV1::Outcome(TOKEN.to_owned()),
                    SHARES.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Outcome(TOKEN.to_owned()),
                    "-20.24691356".parse().unwrap(),
                ),
            ]
        );
        ensure_balanced(&entries).unwrap();
    }

    #[test]
    fn opening_maps_to_explicit_balanced_entries() {
        const OPENING_PUSD: &str = "2000";
        let entries = opening_entries(OPENING_PUSD.parse().unwrap()).unwrap();
        assert_eq!(
            entries,
            vec![
                (
                    LedgerAccountV1::Cash,
                    LedgerAssetV1::Pusd,
                    OPENING_PUSD.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "-2000".parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Available,
                    LedgerAssetV1::Pusd,
                    OPENING_PUSD.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "-2000".parse().unwrap(),
                ),
            ]
        );
        ensure_balanced(&entries).unwrap();
    }
}
