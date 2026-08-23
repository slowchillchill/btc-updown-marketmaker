use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use contracts::execution_normalization::{
    NoeResultReferenceV1, SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralCancellationOutcomeV1, SourceNeutralControlResultV1,
    SourceNeutralEconomicDispositionV1, SourceNeutralEconomicIdentityV1,
    SourceNeutralOrderLifecycleEventV1, SourceNeutralOrderLifecycleResultV1,
    SourceNeutralOrderQueryResultV1, SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1,
    SourceNeutralSettlementLifecycleResultV1, SourceNeutralSubmissionOutcomeV1,
    SourceNeutralTradeEconomicDispositionV1, SourceNeutralTradeLifecycleResultV1,
};
use contracts::{AccountInstanceKeyV1, ConditionId, ExactDecimal, TokenId};
use execution_normalization::{
    DurableErrorV1, NormalizationRecordV1, SourceNeutralEvidenceKindV1, read_noe_result_v1,
};
use rust_decimal::Decimal;

use crate::lifecycle::{
    AppliedFillIndexV1, LedgerSubmissionItemIdentityV1, LifecycleStateV1, LifecycleTotalsV1,
    OutcomeCostBasisV1, TradeLifecycleIndexV1,
};
use crate::projection::{
    AccountAdmissionPolicyV1, LedgerLocalPendingReservationProjectionV1, LedgerMarketProjectionV1,
    LedgerOutcomeBalanceV1, LedgerPlatformConfirmedProjectionV1, LedgerProjectionV1,
};
use crate::reservation::{
    CachedReservationRequestV1, LedgerLocalEventIdentityV1, LedgerRequestIdentityV1,
    LedgerReservationOutcomeV1, LocalReservationV1, MarketLocalPendingV1, ReservationStateV1,
    ReservationStatusV1, ReserveIntentV1,
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
    AccountNotOpened,
    ReservationRequestNotFound {
        request_identity: LedgerRequestIdentityV1,
    },
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
            Self::AccountNotOpened => write!(formatter, "账户尚未开户"),
            Self::ReservationRequestNotFound { request_identity } => {
                write!(formatter, "本地预留请求不存在：{}", request_identity.0)
            }
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
    LocalPendingPrincipal(String),
    LocalPendingImmediateFee(String),
    PlatformPendingPrincipal(String),
    PlatformPendingImmediateFee(String),
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
    PositionCostBasis(Option<String>),
    OutcomeCostBasis {
        condition_id: Option<String>,
        token_id: String,
    },
    Fees(Option<String>),
    RealizedPnl(Option<String>),
    Balancing,
}

impl LedgerAccountV1 {
    fn supports(&self, asset: &LedgerAssetV1) -> bool {
        match self {
            Self::Cash | Self::Available | Self::Balancing => {
                matches!(asset, LedgerAssetV1::Pusd) || matches!(self, Self::Balancing)
            }
            Self::PrincipalEncumbrance(_)
            | Self::LocalPendingPrincipal(_)
            | Self::LocalPendingImmediateFee(_)
            | Self::PlatformPendingPrincipal(_)
            | Self::PlatformPendingImmediateFee(_)
            | Self::ImmediateTakerFeeEncumbrance(_)
            | Self::MakerRebateReceivable(_)
            | Self::TakerRebateReceivable(_)
            | Self::MakerRebateLockedRemainder(_)
            | Self::TakerRebateLockedRemainder(_)
            | Self::PositionCostBasis(_)
            | Self::Fees(_)
            | Self::RealizedPnl(_) => matches!(asset, LedgerAssetV1::Pusd),
            Self::OutcomeCostBasis { .. } => matches!(asset, LedgerAssetV1::Pusd),
            Self::OutcomeBalance { token_id, .. } => {
                matches!(asset, LedgerAssetV1::Outcome(found) if found == token_id)
            }
        }
    }
}

type LedgerEntryV1 = (LedgerAccountV1, LedgerAssetV1, Decimal);

#[derive(Clone, Debug)]
enum LedgerTransactionSourceV1 {
    Noe(NoeResultReferenceV1),
    LocalReservation(LedgerRequestIdentityV1),
    LocalSubmissionEvent {
        local_event_identity: LedgerLocalEventIdentityV1,
        request_identity: LedgerRequestIdentityV1,
        submission_item_identity: LedgerSubmissionItemIdentityV1,
    },
    LocalReleaseEvent {
        local_event_identity: LedgerLocalEventIdentityV1,
        request_identity: LedgerRequestIdentityV1,
    },
    LocalQueryAssociationEvent {
        local_event_identity: LedgerLocalEventIdentityV1,
        query_global_request_id: String,
        request_identity: LedgerRequestIdentityV1,
    },
}

#[derive(Clone, Debug)]
struct LedgerTransactionV1 {
    ledger_entry_seq: u64,
    source: LedgerTransactionSourceV1,
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
    ledger_local_prefix: u64,
    reservations: ReservationStateV1,
    lifecycle: LifecycleStateV1,
    transactions: Vec<LedgerTransactionV1>,
    applied_references: BTreeMap<NoeReferenceKeyV1, u64>,
}

impl LedgerWriterV1 {
    pub fn new(policy: AccountAdmissionPolicyV1) -> Self {
        Self {
            policy,
            account: None,
            ledger_local_prefix: 0,
            reservations: ReservationStateV1::default(),
            lifecycle: LifecycleStateV1::default(),
            transactions: Vec::new(),
            applied_references: BTreeMap::new(),
        }
    }

    pub fn admission_policy(&self) -> &AccountAdmissionPolicyV1 {
        &self.policy
    }

    pub fn try_reserve_intent(
        &mut self,
        request_identity: &LedgerRequestIdentityV1,
        condition_id: &ConditionId,
        requested_principal_pusd: &ExactDecimal,
        immediate_fee_reserve_pusd: &ExactDecimal,
    ) -> Result<LedgerReservationOutcomeV1, LedgerErrorV1> {
        if let Some(cached) = self.reservations.requests.get(request_identity) {
            return Ok(cached.outcome.clone());
        }

        let account = self
            .account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
        let principal = parse_decimal(
            requested_principal_pusd,
            "reservation.requested_principal_pusd",
        )?;
        let immediate_fee = parse_decimal(
            immediate_fee_reserve_pusd,
            "reservation.immediate_fee_reserve_pusd",
        )?;
        let requested_total = checked_add(principal, immediate_fee, "reservation.requested_total")?;
        let pending_total = checked_add(
            self.reservations.principal_pusd,
            self.reservations.immediate_fee_pusd,
            "reservation.global_pending_total",
        )?;
        let platform_pending_total = checked_add(
            self.lifecycle.totals.platform_pending_principal_pusd,
            self.lifecycle.totals.platform_pending_immediate_fee_pusd,
            "reservation.global_platform_pending_total",
        )?;
        let pending_total = checked_add(
            pending_total,
            platform_pending_total,
            "reservation.global_unapplied_total",
        )?;
        let available_after_pending = checked_sub(
            account.balances.available_pusd,
            pending_total,
            "reservation.global_available_after_pending",
        )?;
        if requested_total > available_after_pending {
            return Ok(self.cache_capacity_outcome(
                request_identity,
                LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds,
            ));
        }

        let platform_market = account.markets.get(&condition_id.0);
        let platform_principal = platform_market
            .map(|market| market.balances.principal_encumbrance_pusd)
            .unwrap_or(Decimal::ZERO);
        let platform_fee = platform_market
            .map(|market| market.balances.immediate_taker_fee_encumbrance_pusd)
            .unwrap_or(Decimal::ZERO);
        let local_market = self.reservations.markets.get(&condition_id.0);
        let lifecycle_market = self.lifecycle.markets.get(&condition_id.0);
        let local_principal = local_market
            .map(|market| market.principal_pusd)
            .unwrap_or(Decimal::ZERO);
        let local_fee = local_market
            .map(|market| market.immediate_fee_pusd)
            .unwrap_or(Decimal::ZERO);
        let platform_commitment = checked_add(
            platform_principal,
            platform_fee,
            "reservation.platform_market_commitment",
        )?;
        let platform_pending_commitment = lifecycle_market
            .map(|market| {
                checked_add(
                    market.totals.platform_pending_principal_pusd,
                    market.totals.platform_pending_immediate_fee_pusd,
                    "reservation.market_platform_pending_commitment",
                )
            })
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let position_commitment = lifecycle_market
            .map(|market| {
                checked_add(
                    market.totals.position_cost_basis_pusd,
                    market.totals.fees_pusd,
                    "reservation.market_position_commitment",
                )
            })
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let platform_commitment = checked_add(
            platform_commitment,
            platform_pending_commitment,
            "reservation.platform_and_pending_market_commitment",
        )?;
        let platform_commitment = checked_add(
            platform_commitment,
            position_commitment,
            "reservation.platform_current_market_commitment",
        )?;
        let local_commitment = checked_add(
            local_principal,
            local_fee,
            "reservation.local_market_commitment",
        )?;
        let current_market_commitment = checked_add(
            platform_commitment,
            local_commitment,
            "reservation.current_market_commitment",
        )?;
        let market_commitment_after_request = checked_add(
            current_market_commitment,
            requested_total,
            "reservation.market_commitment_after_request",
        )?;
        let market_limit = parse_decimal(
            &self.policy.per_market_all_in_commitment_limit_pusd,
            "admission_policy.per_market_all_in_commitment_limit_pusd",
        )?;
        if market_commitment_after_request > market_limit {
            return Ok(self.cache_capacity_outcome(
                request_identity,
                LedgerReservationOutcomeV1::PerMarketLimitExceeded,
            ));
        }

        let next_global_principal = checked_add(
            self.reservations.principal_pusd,
            principal,
            "reservation.global_principal",
        )?;
        let next_global_fee = checked_add(
            self.reservations.immediate_fee_pusd,
            immediate_fee,
            "reservation.global_immediate_fee",
        )?;
        let next_before_send_principal = checked_add(
            self.reservations.before_send_principal_pusd,
            principal,
            "reservation.before_send_principal",
        )?;
        let next_before_send_fee = checked_add(
            self.reservations.before_send_immediate_fee_pusd,
            immediate_fee,
            "reservation.before_send_immediate_fee",
        )?;
        let next_before_send_count = checked_add_u64(
            self.reservations.before_send_count,
            1,
            "reservation.before_send_count",
        )?;
        let next_market_principal =
            checked_add(local_principal, principal, "reservation.market_principal")?;
        let next_market_fee =
            checked_add(local_fee, immediate_fee, "reservation.market_immediate_fee")?;
        let (market_before_send_principal, market_before_send_fee, market_before_send_count) =
            local_market
                .map(|market| {
                    Ok::<_, LedgerErrorV1>((
                        checked_add(
                            market.before_send_principal_pusd,
                            principal,
                            "reservation.market_before_send_principal",
                        )?,
                        checked_add(
                            market.before_send_immediate_fee_pusd,
                            immediate_fee,
                            "reservation.market_before_send_immediate_fee",
                        )?,
                        checked_add_u64(
                            market.before_send_count,
                            1,
                            "reservation.market_before_send_count",
                        )?,
                    ))
                })
                .transpose()?
                .unwrap_or((principal, immediate_fee, 1));
        let (
            market_after_send_unknown_principal,
            market_after_send_unknown_fee,
            market_after_send_unknown_count,
        ) = local_market
            .map(|market| {
                (
                    market.after_send_unknown_principal_pusd,
                    market.after_send_unknown_immediate_fee_pusd,
                    market.after_send_unknown_count,
                )
            })
            .unwrap_or((Decimal::ZERO, Decimal::ZERO, 0));
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let ledger_local_prefix = self.next_ledger_local_prefix()?;
        let entries = local_reservation_entries(condition_id, principal, immediate_fee)?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::LocalReservation(request_identity.clone()),
            entries,
        };
        ensure_balanced(&transaction.entries)?;

        let outcome = LedgerReservationOutcomeV1::Reserved { ledger_entry_seq };
        self.reservations.principal_pusd = next_global_principal;
        self.reservations.immediate_fee_pusd = next_global_fee;
        self.reservations.before_send_principal_pusd = next_before_send_principal;
        self.reservations.before_send_immediate_fee_pusd = next_before_send_fee;
        self.reservations.before_send_count = next_before_send_count;
        self.reservations.markets.insert(
            condition_id.0.clone(),
            MarketLocalPendingV1 {
                condition_id: condition_id.clone(),
                principal_pusd: next_market_principal,
                immediate_fee_pusd: next_market_fee,
                before_send_principal_pusd: market_before_send_principal,
                before_send_immediate_fee_pusd: market_before_send_fee,
                before_send_count: market_before_send_count,
                after_send_unknown_principal_pusd: market_after_send_unknown_principal,
                after_send_unknown_immediate_fee_pusd: market_after_send_unknown_fee,
                after_send_unknown_count: market_after_send_unknown_count,
            },
        );
        self.reservations.requests.insert(
            request_identity.clone(),
            CachedReservationRequestV1 {
                outcome: outcome.clone(),
                reservation: Some(LocalReservationV1 {
                    condition_id: condition_id.clone(),
                    principal_pusd: principal,
                    immediate_fee_pusd: immediate_fee,
                    status: ReservationStatusV1::ReservedBeforeSend,
                    last_applied_ledger_entry_seq: ledger_entry_seq,
                }),
            },
        );
        self.ledger_local_prefix = ledger_local_prefix;
        self.commit_transaction(transaction);
        Ok(outcome)
    }

    pub fn try_reserve_intents(
        &mut self,
        intents: &[ReserveIntentV1],
    ) -> Vec<Result<LedgerReservationOutcomeV1, LedgerErrorV1>> {
        intents
            .iter()
            .map(|intent| {
                self.try_reserve_intent(
                    &intent.request_identity,
                    &intent.condition_id,
                    &intent.requested_principal_pusd,
                    &intent.immediate_fee_reserve_pusd,
                )
            })
            .collect()
    }

    pub fn record_submission_started(
        &mut self,
        local_event_identity: &LedgerLocalEventIdentityV1,
        request_identity: &LedgerRequestIdentityV1,
        submission_item_identity: &LedgerSubmissionItemIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
        if let Some(ledger_entry_seq) = self
            .reservations
            .applied_local_events
            .get(local_event_identity)
        {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: *ledger_entry_seq,
            });
        }
        let reservation = self.reservation_for_event(request_identity)?.clone();
        if reservation.status != ReservationStatusV1::ReservedBeforeSend {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: reservation.last_applied_ledger_entry_seq,
            });
        }

        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let ledger_local_prefix = self.next_ledger_local_prefix()?;
        let mut reservations = self.reservations.clone();
        move_reservation_to_submitted_unknown(
            &mut reservations,
            request_identity,
            ledger_entry_seq,
        )?;
        let mut lifecycle = self.lifecycle.clone();
        lifecycle
            .submission_requests
            .insert(submission_item_identity.clone(), request_identity.clone());
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::LocalSubmissionEvent {
                local_event_identity: local_event_identity.clone(),
                request_identity: request_identity.clone(),
                submission_item_identity: submission_item_identity.clone(),
            },
            entries: Vec::new(),
        };
        ensure_balanced(&transaction.entries)?;

        reservations
            .applied_local_events
            .insert(local_event_identity.clone(), ledger_entry_seq);
        self.reservations = reservations;
        self.lifecycle = lifecycle;
        self.ledger_local_prefix = ledger_local_prefix;
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    pub fn record_order_query_association(
        &mut self,
        local_event_identity: &LedgerLocalEventIdentityV1,
        query_global_request_id: &str,
        request_identity: &LedgerRequestIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
        if let Some(ledger_entry_seq) = self
            .reservations
            .applied_local_events
            .get(local_event_identity)
        {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: *ledger_entry_seq,
            });
        }
        self.reservation_for_event(request_identity)?;

        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let ledger_local_prefix = self.next_ledger_local_prefix()?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::LocalQueryAssociationEvent {
                local_event_identity: local_event_identity.clone(),
                query_global_request_id: query_global_request_id.to_owned(),
                request_identity: request_identity.clone(),
            },
            entries: Vec::new(),
        };
        ensure_balanced(&transaction.entries)?;

        self.lifecycle
            .query_requests
            .insert(query_global_request_id.to_owned(), request_identity.clone());
        self.reservations
            .applied_local_events
            .insert(local_event_identity.clone(), ledger_entry_seq);
        self.ledger_local_prefix = ledger_local_prefix;
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    pub fn release_before_send(
        &mut self,
        local_event_identity: &LedgerLocalEventIdentityV1,
        request_identity: &LedgerRequestIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
        if let Some(ledger_entry_seq) = self
            .reservations
            .applied_local_events
            .get(local_event_identity)
        {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: *ledger_entry_seq,
            });
        }
        let reservation = self.reservation_for_event(request_identity)?.clone();
        if reservation.status != ReservationStatusV1::ReservedBeforeSend {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: reservation.last_applied_ledger_entry_seq,
            });
        }

        let next_global_principal = checked_sub(
            self.reservations.principal_pusd,
            reservation.principal_pusd,
            "reservation.release_global_principal",
        )?;
        let next_global_fee = checked_sub(
            self.reservations.immediate_fee_pusd,
            reservation.immediate_fee_pusd,
            "reservation.release_global_immediate_fee",
        )?;
        let next_before_send_principal = checked_sub(
            self.reservations.before_send_principal_pusd,
            reservation.principal_pusd,
            "reservation.release_before_send_principal",
        )?;
        let next_before_send_fee = checked_sub(
            self.reservations.before_send_immediate_fee_pusd,
            reservation.immediate_fee_pusd,
            "reservation.release_before_send_immediate_fee",
        )?;
        let next_before_send_count = checked_sub_u64(
            self.reservations.before_send_count,
            1,
            "reservation.release_before_send_count",
        )?;
        let market = self
            .reservations
            .markets
            .get(&reservation.condition_id.0)
            .expect("活动预留拥有对应市场聚合");
        let next_market_principal = checked_sub(
            market.principal_pusd,
            reservation.principal_pusd,
            "reservation.release_market_principal",
        )?;
        let next_market_fee = checked_sub(
            market.immediate_fee_pusd,
            reservation.immediate_fee_pusd,
            "reservation.release_market_immediate_fee",
        )?;
        let next_market_before_send_principal = checked_sub(
            market.before_send_principal_pusd,
            reservation.principal_pusd,
            "reservation.release_market_before_send_principal",
        )?;
        let next_market_before_send_fee = checked_sub(
            market.before_send_immediate_fee_pusd,
            reservation.immediate_fee_pusd,
            "reservation.release_market_before_send_immediate_fee",
        )?;
        let next_market_before_send_count = checked_sub_u64(
            market.before_send_count,
            1,
            "reservation.release_market_before_send_count",
        )?;
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let ledger_local_prefix = self.next_ledger_local_prefix()?;
        let released_principal = checked_sub(
            Decimal::ZERO,
            reservation.principal_pusd,
            "reservation.release_principal_entry",
        )?;
        let released_fee = checked_sub(
            Decimal::ZERO,
            reservation.immediate_fee_pusd,
            "reservation.release_immediate_fee_entry",
        )?;
        let entries =
            local_reservation_entries(&reservation.condition_id, released_principal, released_fee)?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::LocalReleaseEvent {
                local_event_identity: local_event_identity.clone(),
                request_identity: request_identity.clone(),
            },
            entries,
        };
        ensure_balanced(&transaction.entries)?;

        self.reservations.principal_pusd = next_global_principal;
        self.reservations.immediate_fee_pusd = next_global_fee;
        self.reservations.before_send_principal_pusd = next_before_send_principal;
        self.reservations.before_send_immediate_fee_pusd = next_before_send_fee;
        self.reservations.before_send_count = next_before_send_count;
        let market = self
            .reservations
            .markets
            .get_mut(&reservation.condition_id.0)
            .expect("活动预留拥有对应市场聚合");
        market.principal_pusd = next_market_principal;
        market.immediate_fee_pusd = next_market_fee;
        market.before_send_principal_pusd = next_market_before_send_principal;
        market.before_send_immediate_fee_pusd = next_market_before_send_fee;
        market.before_send_count = next_market_before_send_count;
        let reservation = self.reservation_for_event_mut(request_identity)?;
        reservation.status = ReservationStatusV1::Released;
        reservation.last_applied_ledger_entry_seq = ledger_entry_seq;
        self.reservations
            .applied_local_events
            .insert(local_event_identity.clone(), ledger_entry_seq);
        self.ledger_local_prefix = ledger_local_prefix;
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
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
        match &resolved.record {
            NormalizationRecordV1::EconomicFact { fact, .. } => match &fact.disposition {
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
                    transition,
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
                        transition,
                        balance_delta,
                        &fact.source_reference.record,
                    )
                }
            },
            NormalizationRecordV1::Evidence {
                source_reference,
                evidence,
                ..
            } => self.apply_lifecycle_evidence(
                reference,
                &resolved.context.source.account_instance_key,
                resolved.boundary.durable_available_at_utc_ms,
                &source_reference.record,
                evidence,
            ),
            NormalizationRecordV1::Pending {
                source_reference, ..
            } if self.account.is_some() => self.apply_pending_result(
                reference,
                &resolved.context.source.account_instance_key,
                resolved.boundary.durable_available_at_utc_ms,
                &source_reference.record,
            ),
            _ => Err(LedgerErrorV1::NonEconomicRecord),
        }
    }

    fn apply_lifecycle_evidence(
        &mut self,
        reference: &NoeResultReferenceV1,
        account_instance_key: &AccountInstanceKeyV1,
        available_through_utc_ms: i64,
        source_record: &SimV2SourceRecordReferenceV1,
        evidence: &SourceNeutralEvidenceKindV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.ensure_noe_account(account_instance_key)?;
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let mut reservations = self.reservations.clone();
        let mut lifecycle = self.lifecycle.clone();
        match evidence {
            SourceNeutralEvidenceKindV1::Control { result } => apply_control_lifecycle(
                &mut reservations,
                &mut lifecycle,
                source_record,
                result,
                ledger_entry_seq,
            )?,
            SourceNeutralEvidenceKindV1::OrderLifecycle { result } => {
                apply_order_lifecycle(&mut lifecycle, result, ledger_entry_seq)?;
            }
            SourceNeutralEvidenceKindV1::TradeLifecycle { result } => {
                apply_trade_lifecycle(&mut lifecycle, result, ledger_entry_seq)?;
            }
            SourceNeutralEvidenceKindV1::SettlementLifecycle { result } => {
                apply_settlement_lifecycle(&mut lifecycle, result);
            }
            _ => return Err(LedgerErrorV1::NonEconomicRecord),
        }

        let entries = lifecycle_change_entries(
            &self.reservations,
            &reservations,
            &self.lifecycle,
            &lifecycle,
        )?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::Noe(reference.clone()),
            entries,
        };
        ensure_balanced(&transaction.entries)?;

        let account = self
            .account
            .as_mut()
            .expect("NOE 生命周期证据应用前账户保持存在");
        account.available_through_utc_ms = account
            .available_through_utc_ms
            .max(available_through_utc_ms);
        self.reservations = reservations;
        self.lifecycle = lifecycle;
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    fn apply_pending_result(
        &mut self,
        reference: &NoeResultReferenceV1,
        account_instance_key: &AccountInstanceKeyV1,
        available_through_utc_ms: i64,
        _source_record: &SimV2SourceRecordReferenceV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.ensure_noe_account(account_instance_key)?;
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::Noe(reference.clone()),
            entries: Vec::new(),
        };
        ensure_balanced(&transaction.entries)?;

        let account = self
            .account
            .as_mut()
            .expect("NOE 待定结果应用前账户保持存在");
        account.available_through_utc_ms = account
            .available_through_utc_ms
            .max(available_through_utc_ms);
        self.commit_transaction(transaction);
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    fn ensure_noe_account(
        &self,
        account_instance_key: &AccountInstanceKeyV1,
    ) -> Result<(), LedgerErrorV1> {
        let account = self
            .account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
        if account.account_instance_key != *account_instance_key {
            return Err(LedgerErrorV1::AccountIsolation {
                expected: Box::new(account.account_instance_key.clone()),
                found: Box::new(account_instance_key.clone()),
            });
        }
        Ok(())
    }

    pub fn projection(&self) -> Option<LedgerProjectionV1> {
        let account = self.account.as_ref()?;
        let platform_confirmed_projection =
            project_balances(&account.balances, &self.lifecycle.totals);
        let mut projected_conditions = BTreeMap::<String, ConditionId>::new();
        for market in account.markets.values() {
            projected_conditions.insert(market.condition_id.0.clone(), market.condition_id.clone());
        }
        for market in self.reservations.markets.values() {
            projected_conditions.insert(market.condition_id.0.clone(), market.condition_id.clone());
        }
        for market in self.lifecycle.markets.values() {
            projected_conditions.insert(market.condition_id.0.clone(), market.condition_id.clone());
        }
        let market_projections = projected_conditions
            .into_values()
            .map(|condition_id| {
                let empty_lifecycle = LifecycleTotalsV1::default();
                let lifecycle = self
                    .lifecycle
                    .markets
                    .get(&condition_id.0)
                    .map(|market| &market.totals)
                    .unwrap_or(&empty_lifecycle);
                let balances = account
                    .markets
                    .get(&condition_id.0)
                    .map(|market| project_balances(&market.balances, lifecycle))
                    .unwrap_or_else(|| project_balances(&BalanceStateV1::empty(), lifecycle));
                let local = self.reservations.markets.get(&condition_id.0);
                let local_principal = local
                    .map(|market| market.principal_pusd)
                    .unwrap_or(Decimal::ZERO);
                let local_fee = local
                    .map(|market| market.immediate_fee_pusd)
                    .unwrap_or(Decimal::ZERO);
                let current_all_in_commitment = checked_add(
                    projection_decimal(&balances.current_all_in_commitment_pusd),
                    checked_add(
                        local_principal,
                        local_fee,
                        "projection.market_local_pending_total",
                    )
                    .expect("已提交逐市场本地预留保持可表示"),
                    "projection.market_current_all_in_commitment",
                )
                .expect("已提交逐市场当前占用保持可表示");
                LedgerMarketProjectionV1 {
                    condition_id,
                    principal_encumbrance_pusd: balances.principal_encumbrance_pusd,
                    immediate_taker_fee_encumbrance_pusd: balances
                        .immediate_taker_fee_encumbrance_pusd,
                    local_pending_principal_pusd: exact(local_principal),
                    local_pending_immediate_fee_pusd: exact(local_fee),
                    local_before_send_principal_pusd: exact(
                        local
                            .map(|market| market.before_send_principal_pusd)
                            .unwrap_or(Decimal::ZERO),
                    ),
                    local_before_send_immediate_fee_pusd: exact(
                        local
                            .map(|market| market.before_send_immediate_fee_pusd)
                            .unwrap_or(Decimal::ZERO),
                    ),
                    local_before_send_count: local
                        .map(|market| market.before_send_count)
                        .unwrap_or(0),
                    local_after_send_unknown_principal_pusd: exact(
                        local
                            .map(|market| market.after_send_unknown_principal_pusd)
                            .unwrap_or(Decimal::ZERO),
                    ),
                    local_after_send_unknown_immediate_fee_pusd: exact(
                        local
                            .map(|market| market.after_send_unknown_immediate_fee_pusd)
                            .unwrap_or(Decimal::ZERO),
                    ),
                    local_after_send_unknown_count: local
                        .map(|market| market.after_send_unknown_count)
                        .unwrap_or(0),
                    platform_confirmation_pending_economic_principal_pusd: balances
                        .platform_confirmation_pending_economic_principal_pusd,
                    platform_confirmation_pending_economic_immediate_fee_pusd: balances
                        .platform_confirmation_pending_economic_immediate_fee_pusd,
                    platform_confirmation_pending_economic_count: balances
                        .platform_confirmation_pending_economic_count,
                    open_order_count: balances.open_order_count,
                    pending_fill_count: balances.pending_fill_count,
                    outcome_balances: balances.outcome_balances,
                    maker_rebate_receivable_pusd: balances.maker_rebate_receivable_pusd,
                    taker_rebate_receivable_pusd: balances.taker_rebate_receivable_pusd,
                    maker_rebate_locked_remainder_pusd: balances.maker_rebate_locked_remainder_pusd,
                    taker_rebate_locked_remainder_pusd: balances.taker_rebate_locked_remainder_pusd,
                    position_cost_basis_pusd: balances.position_cost_basis_pusd,
                    fees_pusd: balances.fees_pusd,
                    redeemable_receivable_pusd: zero_exact(),
                    realized_pnl_pusd: balances.realized_pnl_pusd,
                    cumulative_accepted_principal_pusd: balances.cumulative_accepted_principal_pusd,
                    current_all_in_commitment_pusd: exact(current_all_in_commitment),
                }
            })
            .collect();

        Some(LedgerProjectionV1 {
            account_instance_key: account.account_instance_key.clone(),
            ledger_entry_seq: self.current_ledger_entry_seq(),
            platform_source_prefix: account.platform_source_prefix,
            ledger_local_prefix: self.ledger_local_prefix,
            available_through_utc_ms: account.available_through_utc_ms,
            platform_confirmed_projection,
            local_pending_reservation_projection: LedgerLocalPendingReservationProjectionV1 {
                principal_pusd: exact(self.reservations.principal_pusd),
                immediate_fee_pusd: exact(self.reservations.immediate_fee_pusd),
                before_send_principal_pusd: exact(self.reservations.before_send_principal_pusd),
                before_send_immediate_fee_pusd: exact(
                    self.reservations.before_send_immediate_fee_pusd,
                ),
                before_send_count: self.reservations.before_send_count,
                after_send_unknown_principal_pusd: exact(
                    self.reservations.after_send_unknown_principal_pusd,
                ),
                after_send_unknown_immediate_fee_pusd: exact(
                    self.reservations.after_send_unknown_immediate_fee_pusd,
                ),
                after_send_unknown_count: self.reservations.after_send_unknown_count,
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
            source: LedgerTransactionSourceV1::Noe(reference.clone()),
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

    #[allow(clippy::too_many_arguments)]
    fn apply_transition(
        &mut self,
        reference: &NoeResultReferenceV1,
        account_instance_key: &AccountInstanceKeyV1,
        account_transition_seq: u64,
        available_through_utc_ms: i64,
        condition_id: Option<&ConditionId>,
        transition: &SourceNeutralAccountTransitionKindV1,
        balance_delta: &SourceNeutralAccountBalanceDeltaV1,
        source_record: &SimV2SourceRecordReferenceV1,
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
        let mut reservations = self.reservations.clone();
        let mut lifecycle = self.lifecycle.clone();
        apply_economic_lifecycle(
            &mut reservations,
            &mut lifecycle,
            condition_id,
            transition,
            &parsed,
            source_record,
            ledger_entry_seq,
        )?;
        let mut entries = transition_entries(condition_id, &parsed)?;
        entries.extend(lifecycle_change_entries(
            &self.reservations,
            &reservations,
            &self.lifecycle,
            &lifecycle,
        )?);
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::Noe(reference.clone()),
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
        self.reservations = reservations;
        self.lifecycle = lifecycle;
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

    fn next_ledger_local_prefix(&self) -> Result<u64, LedgerErrorV1> {
        self.ledger_local_prefix
            .checked_add(1)
            .ok_or(LedgerErrorV1::ArithmeticOverflow {
                field: "ledger_local_prefix",
            })
    }

    fn cache_capacity_outcome(
        &mut self,
        request_identity: &LedgerRequestIdentityV1,
        outcome: LedgerReservationOutcomeV1,
    ) -> LedgerReservationOutcomeV1 {
        self.reservations.requests.insert(
            request_identity.clone(),
            CachedReservationRequestV1 {
                outcome: outcome.clone(),
                reservation: None,
            },
        );
        outcome
    }

    fn reservation_for_event(
        &self,
        request_identity: &LedgerRequestIdentityV1,
    ) -> Result<&LocalReservationV1, LedgerErrorV1> {
        self.reservations
            .requests
            .get(request_identity)
            .and_then(|request| request.reservation.as_ref())
            .ok_or_else(|| LedgerErrorV1::ReservationRequestNotFound {
                request_identity: request_identity.clone(),
            })
    }

    fn reservation_for_event_mut(
        &mut self,
        request_identity: &LedgerRequestIdentityV1,
    ) -> Result<&mut LocalReservationV1, LedgerErrorV1> {
        self.reservations
            .requests
            .get_mut(request_identity)
            .and_then(|request| request.reservation.as_mut())
            .ok_or_else(|| LedgerErrorV1::ReservationRequestNotFound {
                request_identity: request_identity.clone(),
            })
    }

    fn commit_transaction(&mut self, transaction: LedgerTransactionV1) {
        match &transaction.source {
            LedgerTransactionSourceV1::Noe(reference) => {
                self.applied_references.insert(
                    NoeReferenceKeyV1::from(reference),
                    transaction.ledger_entry_seq,
                );
            }
            LedgerTransactionSourceV1::LocalReservation(request_identity) => {
                let _ = &request_identity.0;
            }
            LedgerTransactionSourceV1::LocalSubmissionEvent {
                local_event_identity,
                request_identity,
                submission_item_identity,
            } => {
                let _ = (
                    &local_event_identity.0,
                    &request_identity.0,
                    &submission_item_identity.global_request_id,
                    submission_item_identity.batch_item_ordinal,
                );
            }
            LedgerTransactionSourceV1::LocalReleaseEvent {
                local_event_identity,
                request_identity,
            } => {
                let _ = (&local_event_identity.0, &request_identity.0);
            }
            LedgerTransactionSourceV1::LocalQueryAssociationEvent {
                local_event_identity,
                query_global_request_id,
                request_identity,
            } => {
                let _ = (
                    &local_event_identity.0,
                    query_global_request_id,
                    &request_identity.0,
                );
            }
        }
        self.transactions.push(transaction);
    }
}

#[derive(Clone, Debug)]
struct ReservationTransitionDetailsV1 {
    condition_id: ConditionId,
    principal_pusd: Decimal,
}

fn move_reservation_to_submitted_unknown(
    reservations: &mut ReservationStateV1,
    request_identity: &LedgerRequestIdentityV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let reservation = reservations
        .requests
        .get(request_identity)
        .and_then(|request| request.reservation.as_ref())
        .cloned()
        .ok_or_else(|| LedgerErrorV1::ReservationRequestNotFound {
            request_identity: request_identity.clone(),
        })?;
    if reservation.status != ReservationStatusV1::ReservedBeforeSend {
        return Ok(());
    }

    reservations.before_send_principal_pusd = checked_sub(
        reservations.before_send_principal_pusd,
        reservation.principal_pusd,
        "reservation.submit_before_send_principal",
    )?;
    reservations.before_send_immediate_fee_pusd = checked_sub(
        reservations.before_send_immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        "reservation.submit_before_send_immediate_fee",
    )?;
    reservations.before_send_count = checked_sub_u64(
        reservations.before_send_count,
        1,
        "reservation.submit_before_send_count",
    )?;
    reservations.after_send_unknown_principal_pusd = checked_add(
        reservations.after_send_unknown_principal_pusd,
        reservation.principal_pusd,
        "reservation.submit_unknown_principal",
    )?;
    reservations.after_send_unknown_immediate_fee_pusd = checked_add(
        reservations.after_send_unknown_immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        "reservation.submit_unknown_immediate_fee",
    )?;
    reservations.after_send_unknown_count = checked_add_u64(
        reservations.after_send_unknown_count,
        1,
        "reservation.submit_unknown_count",
    )?;
    let market = reservations
        .markets
        .get_mut(&reservation.condition_id.0)
        .expect("活动预留拥有对应市场聚合");
    market.before_send_principal_pusd = checked_sub(
        market.before_send_principal_pusd,
        reservation.principal_pusd,
        "reservation.submit_market_before_send_principal",
    )?;
    market.before_send_immediate_fee_pusd = checked_sub(
        market.before_send_immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        "reservation.submit_market_before_send_immediate_fee",
    )?;
    market.before_send_count = checked_sub_u64(
        market.before_send_count,
        1,
        "reservation.submit_market_before_send_count",
    )?;
    market.after_send_unknown_principal_pusd = checked_add(
        market.after_send_unknown_principal_pusd,
        reservation.principal_pusd,
        "reservation.submit_market_unknown_principal",
    )?;
    market.after_send_unknown_immediate_fee_pusd = checked_add(
        market.after_send_unknown_immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        "reservation.submit_market_unknown_immediate_fee",
    )?;
    market.after_send_unknown_count = checked_add_u64(
        market.after_send_unknown_count,
        1,
        "reservation.submit_market_unknown_count",
    )?;
    let reservation = reservations
        .requests
        .get_mut(request_identity)
        .and_then(|request| request.reservation.as_mut())
        .expect("已读取的活动预留保持存在");
    reservation.status = ReservationStatusV1::SubmittedUnknown;
    reservation.last_applied_ledger_entry_seq = ledger_entry_seq;
    Ok(())
}

fn remove_local_pending_reservation(
    reservations: &mut ReservationStateV1,
    reservation: &LocalReservationV1,
) -> Result<(), LedgerErrorV1> {
    reservations.principal_pusd = checked_sub(
        reservations.principal_pusd,
        reservation.principal_pusd,
        "reservation.consume_global_principal",
    )?;
    reservations.immediate_fee_pusd = checked_sub(
        reservations.immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        "reservation.consume_global_immediate_fee",
    )?;
    let market = reservations
        .markets
        .get_mut(&reservation.condition_id.0)
        .expect("活动预留拥有对应市场聚合");
    market.principal_pusd = checked_sub(
        market.principal_pusd,
        reservation.principal_pusd,
        "reservation.consume_market_principal",
    )?;
    market.immediate_fee_pusd = checked_sub(
        market.immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        "reservation.consume_market_immediate_fee",
    )?;

    match reservation.status {
        ReservationStatusV1::ReservedBeforeSend => {
            reservations.before_send_principal_pusd = checked_sub(
                reservations.before_send_principal_pusd,
                reservation.principal_pusd,
                "reservation.consume_before_send_principal",
            )?;
            reservations.before_send_immediate_fee_pusd = checked_sub(
                reservations.before_send_immediate_fee_pusd,
                reservation.immediate_fee_pusd,
                "reservation.consume_before_send_immediate_fee",
            )?;
            reservations.before_send_count = checked_sub_u64(
                reservations.before_send_count,
                1,
                "reservation.consume_before_send_count",
            )?;
            market.before_send_principal_pusd = checked_sub(
                market.before_send_principal_pusd,
                reservation.principal_pusd,
                "reservation.consume_market_before_send_principal",
            )?;
            market.before_send_immediate_fee_pusd = checked_sub(
                market.before_send_immediate_fee_pusd,
                reservation.immediate_fee_pusd,
                "reservation.consume_market_before_send_immediate_fee",
            )?;
            market.before_send_count = checked_sub_u64(
                market.before_send_count,
                1,
                "reservation.consume_market_before_send_count",
            )?;
        }
        ReservationStatusV1::SubmittedUnknown => {
            reservations.after_send_unknown_principal_pusd = checked_sub(
                reservations.after_send_unknown_principal_pusd,
                reservation.principal_pusd,
                "reservation.consume_unknown_principal",
            )?;
            reservations.after_send_unknown_immediate_fee_pusd = checked_sub(
                reservations.after_send_unknown_immediate_fee_pusd,
                reservation.immediate_fee_pusd,
                "reservation.consume_unknown_immediate_fee",
            )?;
            reservations.after_send_unknown_count = checked_sub_u64(
                reservations.after_send_unknown_count,
                1,
                "reservation.consume_unknown_count",
            )?;
            market.after_send_unknown_principal_pusd = checked_sub(
                market.after_send_unknown_principal_pusd,
                reservation.principal_pusd,
                "reservation.consume_market_unknown_principal",
            )?;
            market.after_send_unknown_immediate_fee_pusd = checked_sub(
                market.after_send_unknown_immediate_fee_pusd,
                reservation.immediate_fee_pusd,
                "reservation.consume_market_unknown_immediate_fee",
            )?;
            market.after_send_unknown_count = checked_sub_u64(
                market.after_send_unknown_count,
                1,
                "reservation.consume_market_unknown_count",
            )?;
        }
        ReservationStatusV1::PlatformConfirmedPendingEconomicFact
        | ReservationStatusV1::Converted
        | ReservationStatusV1::Released => {}
    }
    Ok(())
}

fn change_platform_pending(
    lifecycle: &mut LifecycleStateV1,
    reservation: &LocalReservationV1,
    add: bool,
) -> Result<(), LedgerErrorV1> {
    lifecycle.totals.platform_pending_principal_pusd = change_decimal(
        lifecycle.totals.platform_pending_principal_pusd,
        reservation.principal_pusd,
        add,
        "lifecycle.platform_pending_principal",
    )?;
    lifecycle.totals.platform_pending_immediate_fee_pusd = change_decimal(
        lifecycle.totals.platform_pending_immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        add,
        "lifecycle.platform_pending_immediate_fee",
    )?;
    lifecycle.totals.platform_pending_economic_count = change_u64(
        lifecycle.totals.platform_pending_economic_count,
        1,
        add,
        "lifecycle.platform_pending_economic_count",
    )?;
    let market = lifecycle.market_mut(&reservation.condition_id);
    market.totals.platform_pending_principal_pusd = change_decimal(
        market.totals.platform_pending_principal_pusd,
        reservation.principal_pusd,
        add,
        "lifecycle.market_platform_pending_principal",
    )?;
    market.totals.platform_pending_immediate_fee_pusd = change_decimal(
        market.totals.platform_pending_immediate_fee_pusd,
        reservation.immediate_fee_pusd,
        add,
        "lifecycle.market_platform_pending_immediate_fee",
    )?;
    market.totals.platform_pending_economic_count = change_u64(
        market.totals.platform_pending_economic_count,
        1,
        add,
        "lifecycle.market_platform_pending_economic_count",
    )?;
    Ok(())
}

fn apply_definitive_reservation_result(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    request_identity: &LedgerRequestIdentityV1,
    accepted: bool,
    economic_applied: bool,
    ledger_entry_seq: u64,
) -> Result<Option<ReservationTransitionDetailsV1>, LedgerErrorV1> {
    let Some(reservation) = reservations
        .requests
        .get(request_identity)
        .and_then(|request| request.reservation.as_ref())
        .cloned()
    else {
        return Ok(None);
    };

    let next_status = if accepted {
        match reservation.status {
            ReservationStatusV1::ReservedBeforeSend | ReservationStatusV1::SubmittedUnknown => {
                remove_local_pending_reservation(reservations, &reservation)?;
                if economic_applied {
                    ReservationStatusV1::Converted
                } else {
                    change_platform_pending(lifecycle, &reservation, true)?;
                    ReservationStatusV1::PlatformConfirmedPendingEconomicFact
                }
            }
            ReservationStatusV1::PlatformConfirmedPendingEconomicFact if economic_applied => {
                change_platform_pending(lifecycle, &reservation, false)?;
                ReservationStatusV1::Converted
            }
            ReservationStatusV1::PlatformConfirmedPendingEconomicFact => {
                return Ok(None);
            }
            ReservationStatusV1::Converted | ReservationStatusV1::Released => {
                return Ok(None);
            }
        }
    } else {
        match reservation.status {
            ReservationStatusV1::ReservedBeforeSend | ReservationStatusV1::SubmittedUnknown => {
                remove_local_pending_reservation(reservations, &reservation)?;
                ReservationStatusV1::Released
            }
            ReservationStatusV1::PlatformConfirmedPendingEconomicFact => {
                change_platform_pending(lifecycle, &reservation, false)?;
                ReservationStatusV1::Released
            }
            ReservationStatusV1::Converted | ReservationStatusV1::Released => {
                return Ok(None);
            }
        }
    };

    let current = reservations
        .requests
        .get_mut(request_identity)
        .and_then(|request| request.reservation.as_mut())
        .expect("已读取的预留在确定结果应用期间保持存在");
    current.status = next_status;
    current.last_applied_ledger_entry_seq = ledger_entry_seq;
    Ok(Some(ReservationTransitionDetailsV1 {
        condition_id: reservation.condition_id,
        principal_pusd: reservation.principal_pusd,
    }))
}

fn submission_item_identity(
    source_record: &SimV2SourceRecordReferenceV1,
) -> Option<LedgerSubmissionItemIdentityV1> {
    let SimV2SourceRecordReferenceV1::DirectResponse { association, .. } = source_record else {
        return None;
    };
    let SimV2DirectResponseAssociationV1::SubmissionItem {
        global_request_id,
        batch_item_ordinal,
    } = association
    else {
        return None;
    };
    Some(LedgerSubmissionItemIdentityV1 {
        global_request_id: global_request_id.clone(),
        batch_item_ordinal: *batch_item_ordinal,
    })
}

fn query_global_request_id(source_record: &SimV2SourceRecordReferenceV1) -> Option<&str> {
    let SimV2SourceRecordReferenceV1::DirectResponse { association, .. } = source_record else {
        return None;
    };
    let SimV2DirectResponseAssociationV1::Request { global_request_id } = association else {
        return None;
    };
    Some(global_request_id)
}

fn apply_control_lifecycle(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    source_record: &SimV2SourceRecordReferenceV1,
    result: &SourceNeutralControlResultV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    match result {
        SourceNeutralControlResultV1::Submission { outcome } => {
            let request_identity = submission_item_identity(source_record)
                .and_then(|identity| lifecycle.submission_requests.get(&identity).cloned());
            let Some(request_identity) = request_identity else {
                return Ok(());
            };
            match outcome {
                SourceNeutralSubmissionOutcomeV1::Accepted { order } => accept_order_for_request(
                    reservations,
                    lifecycle,
                    &request_identity,
                    order,
                    ledger_entry_seq,
                )?,
                SourceNeutralSubmissionOutcomeV1::RejectedNoEffect
                | SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect => {
                    apply_definitive_reservation_result(
                        reservations,
                        lifecycle,
                        &request_identity,
                        false,
                        false,
                        ledger_entry_seq,
                    )?;
                }
            }
        }
        SourceNeutralControlResultV1::OrderQuery { result } => {
            let request_identity = query_global_request_id(source_record)
                .and_then(|identity| lifecycle.query_requests.get(identity).cloned());
            let Some(request_identity) = request_identity else {
                return Ok(());
            };
            if let SourceNeutralOrderQueryResultV1::Found { order } = result
                && matches!(
                    order.state,
                    SourceNeutralOrderStateV1::Open | SourceNeutralOrderStateV1::Matched
                )
            {
                accept_order_for_request(
                    reservations,
                    lifecycle,
                    &request_identity,
                    order,
                    ledger_entry_seq,
                )?;
            }
        }
        SourceNeutralControlResultV1::Cancellation {
            order_identity,
            outcome: SourceNeutralCancellationOutcomeV1::Canceled,
        } => {
            set_order_open(lifecycle, &order_identity.0, None, false, ledger_entry_seq)?;
        }
        SourceNeutralControlResultV1::Cancellation { .. }
        | SourceNeutralControlResultV1::OrdersQuery { .. }
        | SourceNeutralControlResultV1::TradesQuery { .. } => {}
    }
    Ok(())
}

fn accept_order_for_request(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    request_identity: &LedgerRequestIdentityV1,
    order: &SourceNeutralOrderSnapshotV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let mut order_index = lifecycle
        .orders
        .remove(&order.order_identity.0)
        .unwrap_or_default();
    let economic_applied = order_index.economic_applied;
    let transition = apply_definitive_reservation_result(
        reservations,
        lifecycle,
        request_identity,
        true,
        economic_applied,
        ledger_entry_seq,
    )?;
    if !order_index.accepted_principal_counted
        && let Some(transition) = transition
    {
        add_cumulative_accepted_principal(
            lifecycle,
            &transition.condition_id,
            transition.principal_pusd,
        )?;
        order_index.accepted_principal_counted = true;
    }
    order_index.condition_id = Some(order.condition_id.clone());
    order_index.request_identity = Some(request_identity.clone());
    order_index.last_ledger_entry_seq = ledger_entry_seq;
    lifecycle
        .orders
        .insert(order.order_identity.0.clone(), order_index);
    set_order_open(
        lifecycle,
        &order.order_identity.0,
        Some(&order.condition_id),
        matches!(order.state, SourceNeutralOrderStateV1::Open),
        ledger_entry_seq,
    )
}

fn add_cumulative_accepted_principal(
    lifecycle: &mut LifecycleStateV1,
    condition_id: &ConditionId,
    principal_pusd: Decimal,
) -> Result<(), LedgerErrorV1> {
    lifecycle.totals.cumulative_accepted_principal_pusd = checked_add(
        lifecycle.totals.cumulative_accepted_principal_pusd,
        principal_pusd,
        "lifecycle.cumulative_accepted_principal",
    )?;
    let market = lifecycle.market_mut(condition_id);
    market.totals.cumulative_accepted_principal_pusd = checked_add(
        market.totals.cumulative_accepted_principal_pusd,
        principal_pusd,
        "lifecycle.market_cumulative_accepted_principal",
    )?;
    Ok(())
}

fn set_order_open(
    lifecycle: &mut LifecycleStateV1,
    order_identity: &str,
    condition_id: Option<&ConditionId>,
    open: bool,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let mut order = lifecycle.orders.remove(order_identity).unwrap_or_default();
    let next_condition = condition_id.cloned().or_else(|| order.condition_id.clone());
    if order.open && (!open || order.condition_id != next_condition) {
        lifecycle.totals.open_order_count = checked_sub_u64(
            lifecycle.totals.open_order_count,
            1,
            "lifecycle.open_order_count",
        )?;
        if let Some(previous_condition) = &order.condition_id {
            let market = lifecycle.market_mut(previous_condition);
            market.totals.open_order_count = checked_sub_u64(
                market.totals.open_order_count,
                1,
                "lifecycle.market_open_order_count",
            )?;
        }
    }
    if open && (!order.open || order.condition_id != next_condition) {
        lifecycle.totals.open_order_count = checked_add_u64(
            lifecycle.totals.open_order_count,
            1,
            "lifecycle.open_order_count",
        )?;
        if let Some(next_condition) = &next_condition {
            let market = lifecycle.market_mut(next_condition);
            market.totals.open_order_count = checked_add_u64(
                market.totals.open_order_count,
                1,
                "lifecycle.market_open_order_count",
            )?;
        }
    }
    order.condition_id = next_condition;
    order.open = open;
    order.last_ledger_entry_seq = ledger_entry_seq;
    lifecycle.orders.insert(order_identity.to_owned(), order);
    Ok(())
}

fn apply_order_lifecycle(
    lifecycle: &mut LifecycleStateV1,
    result: &SourceNeutralOrderLifecycleResultV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    match &result.event {
        SourceNeutralOrderLifecycleEventV1::Accepted { state } => set_order_open(
            lifecycle,
            &result.order_identity.0,
            Some(&result.condition_id),
            matches!(state, SourceNeutralOrderStateV1::Open),
            ledger_entry_seq,
        ),
        SourceNeutralOrderLifecycleEventV1::FakUnspentReleased
        | SourceNeutralOrderLifecycleEventV1::VenueDustTerminal
        | SourceNeutralOrderLifecycleEventV1::MarketResolvedCanceled { .. }
        | SourceNeutralOrderLifecycleEventV1::CallerCanceled
        | SourceNeutralOrderLifecycleEventV1::GtdExpired => set_order_open(
            lifecycle,
            &result.order_identity.0,
            Some(&result.condition_id),
            false,
            ledger_entry_seq,
        ),
    }
}

fn apply_trade_lifecycle(
    lifecycle: &mut LifecycleStateV1,
    result: &SourceNeutralTradeLifecycleResultV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let pending = matches!(
        result.economic_disposition,
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied
            | SourceNeutralTradeEconomicDispositionV1::PendingApplied
    );
    set_trade_pending(
        lifecycle,
        &result.trade_identity.0,
        &result.condition_id,
        pending,
        ledger_entry_seq,
    )
}

fn set_trade_pending(
    lifecycle: &mut LifecycleStateV1,
    trade_identity: &str,
    condition_id: &ConditionId,
    pending: bool,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let previous = lifecycle.trades.remove(trade_identity);
    if let Some(previous) = &previous
        && previous.pending
        && (!pending || previous.condition_id != *condition_id)
    {
        lifecycle.totals.pending_fill_count = checked_sub_u64(
            lifecycle.totals.pending_fill_count,
            1,
            "lifecycle.pending_fill_count",
        )?;
        let market = lifecycle.market_mut(&previous.condition_id);
        market.totals.pending_fill_count = checked_sub_u64(
            market.totals.pending_fill_count,
            1,
            "lifecycle.market_pending_fill_count",
        )?;
    }
    if pending
        && previous
            .as_ref()
            .is_none_or(|previous| !previous.pending || previous.condition_id != *condition_id)
    {
        lifecycle.totals.pending_fill_count = checked_add_u64(
            lifecycle.totals.pending_fill_count,
            1,
            "lifecycle.pending_fill_count",
        )?;
        let market = lifecycle.market_mut(condition_id);
        market.totals.pending_fill_count = checked_add_u64(
            market.totals.pending_fill_count,
            1,
            "lifecycle.market_pending_fill_count",
        )?;
    }
    lifecycle.trades.insert(
        trade_identity.to_owned(),
        TradeLifecycleIndexV1 {
            condition_id: condition_id.clone(),
            pending,
            _last_ledger_entry_seq: ledger_entry_seq,
        },
    );
    Ok(())
}

fn apply_settlement_lifecycle(
    lifecycle: &mut LifecycleStateV1,
    result: &SourceNeutralSettlementLifecycleResultV1,
) {
    let _ = (lifecycle, result);
}

fn apply_economic_lifecycle(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    transition: &SourceNeutralAccountTransitionKindV1,
    delta: &ParsedBalanceDeltaV1,
    source_record: &SimV2SourceRecordReferenceV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    match transition {
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity,
        } => {
            let mut order = lifecycle
                .orders
                .remove(&order_identity.0)
                .unwrap_or_default();
            let request_identity = submission_item_identity(source_record)
                .and_then(|identity| lifecycle.submission_requests.get(&identity).cloned())
                .or_else(|| order.request_identity.clone());
            let transition = request_identity
                .as_ref()
                .map(|request_identity| {
                    apply_definitive_reservation_result(
                        reservations,
                        lifecycle,
                        request_identity,
                        true,
                        true,
                        ledger_entry_seq,
                    )
                })
                .transpose()?
                .flatten();
            if !order.accepted_principal_counted {
                let accepted_condition = transition
                    .as_ref()
                    .map(|transition| &transition.condition_id)
                    .or(condition_id);
                if let Some(accepted_condition) = accepted_condition {
                    let accepted_principal = transition
                        .as_ref()
                        .map(|transition| transition.principal_pusd)
                        .unwrap_or(delta.principal_encumbrance_pusd);
                    add_cumulative_accepted_principal(
                        lifecycle,
                        accepted_condition,
                        accepted_principal,
                    )?;
                    order.accepted_principal_counted = true;
                }
            }
            order.condition_id = condition_id.cloned().or(order.condition_id);
            order.request_identity = request_identity;
            order.economic_applied = true;
            order.last_ledger_entry_seq = ledger_entry_seq;
            lifecycle.orders.insert(order_identity.0.clone(), order);
        }
        SourceNeutralAccountTransitionKindV1::FakUnspentEncumbranceReleased { order_identity }
        | SourceNeutralAccountTransitionKindV1::CallerCancellationEncumbranceReleased {
            order_identity,
        }
        | SourceNeutralAccountTransitionKindV1::GtdExpirationEncumbranceReleased {
            order_identity,
            ..
        }
        | SourceNeutralAccountTransitionKindV1::VenueDustEncumbranceReleased { order_identity }
        | SourceNeutralAccountTransitionKindV1::MarketResolutionEncumbranceReleased {
            order_identity,
            ..
        } => {
            set_order_open(
                lifecycle,
                &order_identity.0,
                condition_id,
                false,
                ledger_entry_seq,
            )?;
        }
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity,
            path_ordinal,
        } => {
            let cost_basis_pusd = checked_sub(
                Decimal::ZERO,
                delta.principal_encumbrance_pusd,
                "lifecycle.fill_cost_basis",
            )?;
            let fee_pusd = checked_sub(
                Decimal::ZERO,
                delta.immediate_taker_fee_encumbrance_pusd,
                "lifecycle.fill_fee",
            )?;
            let token_id = delta
                .outcome_balances
                .values()
                .next()
                .map(|outcome| outcome.token_id.clone());
            if let Some(token_id) = &token_id {
                change_outcome_cost_basis(lifecycle, condition_id, token_id, cost_basis_pusd)?;
            } else {
                change_position_cost_basis(lifecycle, condition_id, cost_basis_pusd)?;
            }
            change_fees(lifecycle, condition_id, fee_pusd)?;
            lifecycle.applied_fills.insert(
                (trade_identity.0.clone(), *path_ordinal),
                AppliedFillIndexV1 {
                    condition_id: condition_id.cloned(),
                    token_id,
                    cost_basis_pusd,
                    fee_pusd,
                    active: true,
                },
            );
            if let Some(condition_id) = condition_id {
                set_trade_pending(
                    lifecycle,
                    &trade_identity.0,
                    condition_id,
                    false,
                    ledger_entry_seq,
                )?;
            }
        }
        SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased {
            trade_identity,
            ..
        } => {
            if let Some(condition_id) = condition_id {
                set_trade_pending(
                    lifecycle,
                    &trade_identity.0,
                    condition_id,
                    false,
                    ledger_entry_seq,
                )?;
            }
        }
        SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity,
            path_ordinal,
        } => {
            let fill_key = (trade_identity.0.clone(), *path_ordinal);
            if let Some(fill) = lifecycle.applied_fills.get(&fill_key).cloned()
                && fill.active
            {
                let reversed_cost = checked_sub(
                    Decimal::ZERO,
                    fill.cost_basis_pusd,
                    "lifecycle.compensation_cost_basis",
                )?;
                let reversed_fee =
                    checked_sub(Decimal::ZERO, fill.fee_pusd, "lifecycle.compensation_fee")?;
                let fill_condition = fill.condition_id.as_ref().or(condition_id);
                if let Some(token_id) = &fill.token_id {
                    change_outcome_cost_basis(lifecycle, fill_condition, token_id, reversed_cost)?;
                } else {
                    change_position_cost_basis(lifecycle, fill_condition, reversed_cost)?;
                }
                change_fees(lifecycle, fill_condition, reversed_fee)?;
                if let Some(fill) = lifecycle.applied_fills.get_mut(&fill_key) {
                    fill.active = false;
                }
            }
            if let Some(condition_id) = condition_id {
                set_trade_pending(
                    lifecycle,
                    &trade_identity.0,
                    condition_id,
                    false,
                    ledger_entry_seq,
                )?;
            }
        }
        SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized => {
            let rebate = checked_add(
                delta.maker_rebate_receivable_pusd,
                delta.maker_rebate_locked_remainder_pusd,
                "lifecycle.maker_rebate_accrual",
            )?;
            change_realized_pnl(lifecycle, condition_id, rebate)?;
        }
        SourceNeutralAccountTransitionKindV1::TakerRebateAccrualApplied { .. } => {
            let rebate = checked_add(
                delta.taker_rebate_receivable_pusd,
                delta.taker_rebate_locked_remainder_pusd,
                "lifecycle.taker_rebate_accrual",
            )?;
            change_realized_pnl(lifecycle, condition_id, rebate)?;
        }
        SourceNeutralAccountTransitionKindV1::MakerRebateDayFinalized { .. }
        | SourceNeutralAccountTransitionKindV1::TakerRebateDayFinalized { .. }
        | SourceNeutralAccountTransitionKindV1::MakerRebatePaid { .. }
        | SourceNeutralAccountTransitionKindV1::TakerRebatePaid { .. } => {}
        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied { .. } => {
            let mut removed_cost_basis = Decimal::ZERO;
            let token_ids: Vec<TokenId> = delta
                .outcome_balances
                .values()
                .map(|outcome| outcome.token_id.clone())
                .collect();
            for token_id in token_ids {
                let cost_basis = lifecycle
                    .totals
                    .outcome_cost_basis_pusd
                    .get(&token_id.0)
                    .map(|cost| cost.cost_basis_pusd)
                    .unwrap_or(Decimal::ZERO);
                removed_cost_basis = checked_add(
                    removed_cost_basis,
                    cost_basis,
                    "lifecycle.redeem_removed_cost_basis",
                )?;
                change_outcome_cost_basis(
                    lifecycle,
                    condition_id,
                    &token_id,
                    checked_sub(
                        Decimal::ZERO,
                        cost_basis,
                        "lifecycle.redeem_token_cost_basis",
                    )?,
                )?;
            }
            change_realized_pnl(
                lifecycle,
                condition_id,
                checked_sub(
                    delta.cash_pusd,
                    removed_cost_basis,
                    "lifecycle.auto_redeem_realized_pnl",
                )?,
            )?;
        }
    }
    Ok(())
}

fn change_position_cost_basis(
    lifecycle: &mut LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    delta: Decimal,
) -> Result<(), LedgerErrorV1> {
    lifecycle.totals.position_cost_basis_pusd = checked_add(
        lifecycle.totals.position_cost_basis_pusd,
        delta,
        "lifecycle.position_cost_basis",
    )?;
    if let Some(condition_id) = condition_id {
        let market = lifecycle.market_mut(condition_id);
        market.totals.position_cost_basis_pusd = checked_add(
            market.totals.position_cost_basis_pusd,
            delta,
            "lifecycle.market_position_cost_basis",
        )?;
    }
    Ok(())
}

fn change_outcome_cost_basis(
    lifecycle: &mut LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    token_id: &TokenId,
    delta: Decimal,
) -> Result<(), LedgerErrorV1> {
    change_position_cost_basis(lifecycle, condition_id, delta)?;
    let current = lifecycle
        .totals
        .outcome_cost_basis_pusd
        .get(&token_id.0)
        .map(|cost| cost.cost_basis_pusd)
        .unwrap_or(Decimal::ZERO);
    lifecycle.totals.outcome_cost_basis_pusd.insert(
        token_id.0.clone(),
        OutcomeCostBasisV1 {
            token_id: token_id.clone(),
            cost_basis_pusd: checked_add(current, delta, "lifecycle.outcome_cost_basis")?,
        },
    );
    if let Some(condition_id) = condition_id {
        let market = lifecycle.market_mut(condition_id);
        let current = market
            .totals
            .outcome_cost_basis_pusd
            .get(&token_id.0)
            .map(|cost| cost.cost_basis_pusd)
            .unwrap_or(Decimal::ZERO);
        market.totals.outcome_cost_basis_pusd.insert(
            token_id.0.clone(),
            OutcomeCostBasisV1 {
                token_id: token_id.clone(),
                cost_basis_pusd: checked_add(
                    current,
                    delta,
                    "lifecycle.market_outcome_cost_basis",
                )?,
            },
        );
    }
    Ok(())
}

fn change_fees(
    lifecycle: &mut LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    delta: Decimal,
) -> Result<(), LedgerErrorV1> {
    lifecycle.totals.fees_pusd = checked_add(lifecycle.totals.fees_pusd, delta, "lifecycle.fees")?;
    if let Some(condition_id) = condition_id {
        let market = lifecycle.market_mut(condition_id);
        market.totals.fees_pusd =
            checked_add(market.totals.fees_pusd, delta, "lifecycle.market_fees")?;
    }
    Ok(())
}

fn change_realized_pnl(
    lifecycle: &mut LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    delta: Decimal,
) -> Result<(), LedgerErrorV1> {
    lifecycle.totals.realized_pnl_pusd = checked_add(
        lifecycle.totals.realized_pnl_pusd,
        delta,
        "lifecycle.realized_pnl",
    )?;
    if let Some(condition_id) = condition_id {
        let market = lifecycle.market_mut(condition_id);
        market.totals.realized_pnl_pusd = checked_add(
            market.totals.realized_pnl_pusd,
            delta,
            "lifecycle.market_realized_pnl",
        )?;
    }
    Ok(())
}

fn lifecycle_change_entries(
    before_reservations: &ReservationStateV1,
    after_reservations: &ReservationStateV1,
    before_lifecycle: &LifecycleStateV1,
    after_lifecycle: &LifecycleStateV1,
) -> Result<Vec<LedgerEntryV1>, LedgerErrorV1> {
    let mut entries = Vec::new();
    let reservation_markets: BTreeSet<&String> = before_reservations
        .markets
        .keys()
        .chain(after_reservations.markets.keys())
        .collect();
    for condition_id in reservation_markets {
        let before = before_reservations.markets.get(condition_id);
        let after = after_reservations.markets.get(condition_id);
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::LocalPendingPrincipal(condition_id.clone()),
            LedgerAssetV1::Pusd,
            checked_sub(
                after
                    .map(|market| market.principal_pusd)
                    .unwrap_or(Decimal::ZERO),
                before
                    .map(|market| market.principal_pusd)
                    .unwrap_or(Decimal::ZERO),
                "lifecycle.local_pending_principal_entry",
            )?,
            "lifecycle.local_pending_principal_entry",
        )?;
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::LocalPendingImmediateFee(condition_id.clone()),
            LedgerAssetV1::Pusd,
            checked_sub(
                after
                    .map(|market| market.immediate_fee_pusd)
                    .unwrap_or(Decimal::ZERO),
                before
                    .map(|market| market.immediate_fee_pusd)
                    .unwrap_or(Decimal::ZERO),
                "lifecycle.local_pending_fee_entry",
            )?,
            "lifecycle.local_pending_fee_entry",
        )?;
    }

    let lifecycle_markets: BTreeSet<&String> = before_lifecycle
        .markets
        .keys()
        .chain(after_lifecycle.markets.keys())
        .collect();
    let mut market_platform_principal_delta = Decimal::ZERO;
    let mut market_platform_fee_delta = Decimal::ZERO;
    let mut market_position_cost_delta = Decimal::ZERO;
    let mut market_fee_delta = Decimal::ZERO;
    let mut market_pnl_delta = Decimal::ZERO;
    for condition_id in lifecycle_markets {
        let before = before_lifecycle
            .markets
            .get(condition_id)
            .map(|market| &market.totals);
        let after = after_lifecycle
            .markets
            .get(condition_id)
            .map(|market| &market.totals);
        let platform_principal_delta = totals_decimal_delta(
            before,
            after,
            |totals| totals.platform_pending_principal_pusd,
            "lifecycle.market_platform_principal_delta",
        )?;
        let platform_fee_delta = totals_decimal_delta(
            before,
            after,
            |totals| totals.platform_pending_immediate_fee_pusd,
            "lifecycle.market_platform_fee_delta",
        )?;
        let position_cost_delta = totals_decimal_delta(
            before,
            after,
            |totals| totals.position_cost_basis_pusd,
            "lifecycle.market_position_cost_delta",
        )?;
        let fee_delta = totals_decimal_delta(
            before,
            after,
            |totals| totals.fees_pusd,
            "lifecycle.market_fee_delta",
        )?;
        let pnl_delta = totals_decimal_delta(
            before,
            after,
            |totals| totals.realized_pnl_pusd,
            "lifecycle.market_pnl_delta",
        )?;
        market_platform_principal_delta = checked_add(
            market_platform_principal_delta,
            platform_principal_delta,
            "lifecycle.market_platform_principal_sum",
        )?;
        market_platform_fee_delta = checked_add(
            market_platform_fee_delta,
            platform_fee_delta,
            "lifecycle.market_platform_fee_sum",
        )?;
        market_position_cost_delta = checked_add(
            market_position_cost_delta,
            position_cost_delta,
            "lifecycle.market_position_cost_sum",
        )?;
        market_fee_delta = checked_add(market_fee_delta, fee_delta, "lifecycle.market_fee_sum")?;
        market_pnl_delta = checked_add(market_pnl_delta, pnl_delta, "lifecycle.market_pnl_sum")?;

        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::PlatformPendingPrincipal(condition_id.clone()),
            LedgerAssetV1::Pusd,
            platform_principal_delta,
            "lifecycle.market_platform_principal_entry",
        )?;
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::PlatformPendingImmediateFee(condition_id.clone()),
            LedgerAssetV1::Pusd,
            platform_fee_delta,
            "lifecycle.market_platform_fee_entry",
        )?;

        let before_outcomes = before.map(|totals| &totals.outcome_cost_basis_pusd);
        let after_outcomes = after.map(|totals| &totals.outcome_cost_basis_pusd);
        let outcome_tokens: BTreeSet<&String> = before_outcomes
            .into_iter()
            .flat_map(|outcomes| outcomes.keys())
            .chain(
                after_outcomes
                    .into_iter()
                    .flat_map(|outcomes| outcomes.keys()),
            )
            .collect();
        let mut outcome_cost_delta = Decimal::ZERO;
        for token_id in outcome_tokens {
            let delta = checked_sub(
                after
                    .and_then(|totals| totals.outcome_cost_basis_pusd.get(token_id))
                    .map(|cost| cost.cost_basis_pusd)
                    .unwrap_or(Decimal::ZERO),
                before
                    .and_then(|totals| totals.outcome_cost_basis_pusd.get(token_id))
                    .map(|cost| cost.cost_basis_pusd)
                    .unwrap_or(Decimal::ZERO),
                "lifecycle.market_outcome_cost_delta",
            )?;
            outcome_cost_delta = checked_add(
                outcome_cost_delta,
                delta,
                "lifecycle.market_outcome_cost_sum",
            )?;
            push_balanced_entry(
                &mut entries,
                LedgerAccountV1::OutcomeCostBasis {
                    condition_id: Some(condition_id.clone()),
                    token_id: token_id.clone(),
                },
                LedgerAssetV1::Pusd,
                delta,
                "lifecycle.market_outcome_cost_entry",
            )?;
        }
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::PositionCostBasis(Some(condition_id.clone())),
            LedgerAssetV1::Pusd,
            checked_sub(
                position_cost_delta,
                outcome_cost_delta,
                "lifecycle.market_unattributed_position_cost_delta",
            )?,
            "lifecycle.market_unattributed_position_cost_entry",
        )?;
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::Fees(Some(condition_id.clone())),
            LedgerAssetV1::Pusd,
            fee_delta,
            "lifecycle.market_fee_entry",
        )?;
        push_balanced_entry(
            &mut entries,
            LedgerAccountV1::RealizedPnl(Some(condition_id.clone())),
            LedgerAssetV1::Pusd,
            pnl_delta,
            "lifecycle.market_pnl_entry",
        )?;
    }

    debug_assert_eq!(
        checked_sub(
            after_lifecycle.totals.platform_pending_principal_pusd,
            before_lifecycle.totals.platform_pending_principal_pusd,
            "lifecycle.global_platform_principal_delta",
        )?,
        market_platform_principal_delta,
    );
    debug_assert_eq!(
        checked_sub(
            after_lifecycle.totals.platform_pending_immediate_fee_pusd,
            before_lifecycle.totals.platform_pending_immediate_fee_pusd,
            "lifecycle.global_platform_fee_delta",
        )?,
        market_platform_fee_delta,
    );

    for (account, global_delta, market_delta, field) in [
        (
            LedgerAccountV1::PositionCostBasis(None),
            checked_sub(
                after_lifecycle.totals.position_cost_basis_pusd,
                before_lifecycle.totals.position_cost_basis_pusd,
                "lifecycle.global_position_cost_delta",
            )?,
            market_position_cost_delta,
            "lifecycle.account_only_position_cost_entry",
        ),
        (
            LedgerAccountV1::Fees(None),
            checked_sub(
                after_lifecycle.totals.fees_pusd,
                before_lifecycle.totals.fees_pusd,
                "lifecycle.global_fee_delta",
            )?,
            market_fee_delta,
            "lifecycle.account_only_fee_entry",
        ),
        (
            LedgerAccountV1::RealizedPnl(None),
            checked_sub(
                after_lifecycle.totals.realized_pnl_pusd,
                before_lifecycle.totals.realized_pnl_pusd,
                "lifecycle.global_pnl_delta",
            )?,
            market_pnl_delta,
            "lifecycle.account_only_pnl_entry",
        ),
    ] {
        let account_only_delta = checked_sub(global_delta, market_delta, field)?;
        push_balanced_entry(
            &mut entries,
            account,
            LedgerAssetV1::Pusd,
            account_only_delta,
            field,
        )?;
    }
    Ok(entries)
}

fn totals_decimal_delta(
    before: Option<&LifecycleTotalsV1>,
    after: Option<&LifecycleTotalsV1>,
    field_value: impl Fn(&LifecycleTotalsV1) -> Decimal,
    field: &'static str,
) -> Result<Decimal, LedgerErrorV1> {
    let after_value = after.map(&field_value).unwrap_or(Decimal::ZERO);
    let before_value = before.map(field_value).unwrap_or(Decimal::ZERO);
    checked_sub(after_value, before_value, field)
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

fn local_reservation_entries(
    condition_id: &ConditionId,
    principal_pusd: Decimal,
    immediate_fee_pusd: Decimal,
) -> Result<Vec<LedgerEntryV1>, LedgerErrorV1> {
    let mut entries = Vec::new();
    push_balanced_entry(
        &mut entries,
        LedgerAccountV1::LocalPendingPrincipal(condition_id.0.clone()),
        LedgerAssetV1::Pusd,
        principal_pusd,
        "reservation.principal_entry",
    )?;
    push_balanced_entry(
        &mut entries,
        LedgerAccountV1::LocalPendingImmediateFee(condition_id.0.clone()),
        LedgerAssetV1::Pusd,
        immediate_fee_pusd,
        "reservation.immediate_fee_entry",
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

fn checked_sub(
    left: Decimal,
    right: Decimal,
    field: &'static str,
) -> Result<Decimal, LedgerErrorV1> {
    left.checked_sub(right)
        .ok_or(LedgerErrorV1::ArithmeticOverflow { field })
}

fn checked_add_u64(left: u64, right: u64, field: &'static str) -> Result<u64, LedgerErrorV1> {
    left.checked_add(right)
        .ok_or(LedgerErrorV1::ArithmeticOverflow { field })
}

fn checked_sub_u64(left: u64, right: u64, field: &'static str) -> Result<u64, LedgerErrorV1> {
    left.checked_sub(right)
        .ok_or(LedgerErrorV1::ArithmeticOverflow { field })
}

fn change_decimal(
    current: Decimal,
    amount: Decimal,
    add: bool,
    field: &'static str,
) -> Result<Decimal, LedgerErrorV1> {
    if add {
        checked_add(current, amount, field)
    } else {
        checked_sub(current, amount, field)
    }
}

fn change_u64(
    current: u64,
    amount: u64,
    add: bool,
    field: &'static str,
) -> Result<u64, LedgerErrorV1> {
    if add {
        checked_add_u64(current, amount, field)
    } else {
        checked_sub_u64(current, amount, field)
    }
}

fn project_balances(
    state: &BalanceStateV1,
    lifecycle: &LifecycleTotalsV1,
) -> LedgerPlatformConfirmedProjectionV1 {
    let available_pusd = if lifecycle.platform_pending_principal_pusd.is_zero()
        && lifecycle.platform_pending_immediate_fee_pusd.is_zero()
    {
        state.available_pusd
    } else {
        checked_sub(
            checked_sub(
                state.available_pusd,
                lifecycle.platform_pending_principal_pusd,
                "projection.available_pending_principal",
            )
            .expect("平台待经济事实本金保持可投影"),
            lifecycle.platform_pending_immediate_fee_pusd,
            "projection.available_pending_fee",
        )
        .expect("平台待经济事实费用保持可投影")
    };
    let principal_encumbrance_pusd = if lifecycle.platform_pending_principal_pusd.is_zero() {
        state.principal_encumbrance_pusd
    } else {
        checked_add(
            state.principal_encumbrance_pusd,
            lifecycle.platform_pending_principal_pusd,
            "projection.principal_with_pending",
        )
        .expect("平台本金总占用保持可投影")
    };
    let immediate_taker_fee_encumbrance_pusd =
        if lifecycle.platform_pending_immediate_fee_pusd.is_zero() {
            state.immediate_taker_fee_encumbrance_pusd
        } else {
            checked_add(
                state.immediate_taker_fee_encumbrance_pusd,
                lifecycle.platform_pending_immediate_fee_pusd,
                "projection.fee_with_pending",
            )
            .expect("平台费用总占用保持可投影")
        };
    let current_all_in_commitment_pusd = checked_add(
        checked_add(
            principal_encumbrance_pusd,
            immediate_taker_fee_encumbrance_pusd,
            "projection.platform_encumbrance_total",
        )
        .expect("平台占用合计保持可投影"),
        checked_add(
            lifecycle.position_cost_basis_pusd,
            lifecycle.fees_pusd,
            "projection.position_and_fee_total",
        )
        .expect("持仓成本与费用保持可投影"),
        "projection.current_all_in_commitment",
    )
    .expect("当前含手续费占用保持可投影");

    let outcome_balances = state
        .outcome_balances
        .values()
        .map(|outcome| {
            let cost_basis = lifecycle.outcome_cost_basis_pusd.get(&outcome.token_id.0);
            LedgerOutcomeBalanceV1 {
                token_id: cost_basis
                    .map(|cost| cost.token_id.clone())
                    .unwrap_or_else(|| outcome.token_id.clone()),
                balance_shares: exact(outcome.balance),
                position_cost_basis_pusd: exact(
                    cost_basis
                        .map(|cost| cost.cost_basis_pusd)
                        .unwrap_or(Decimal::ZERO),
                ),
            }
        })
        .collect();

    LedgerPlatformConfirmedProjectionV1 {
        cash_pusd: exact(state.cash_pusd),
        available_pusd: exact(available_pusd),
        principal_encumbrance_pusd: exact(principal_encumbrance_pusd),
        immediate_taker_fee_encumbrance_pusd: exact(immediate_taker_fee_encumbrance_pusd),
        outcome_balances,
        maker_rebate_receivable_pusd: exact(state.maker_rebate_receivable_pusd),
        taker_rebate_receivable_pusd: exact(state.taker_rebate_receivable_pusd),
        maker_rebate_locked_remainder_pusd: exact(state.maker_rebate_locked_remainder_pusd),
        taker_rebate_locked_remainder_pusd: exact(state.taker_rebate_locked_remainder_pusd),
        position_cost_basis_pusd: exact(lifecycle.position_cost_basis_pusd),
        fees_pusd: exact(lifecycle.fees_pusd),
        redeemable_receivable_pusd: zero_exact(),
        realized_pnl_pusd: exact(lifecycle.realized_pnl_pusd),
        platform_confirmation_pending_economic_principal_pusd: exact(
            lifecycle.platform_pending_principal_pusd,
        ),
        platform_confirmation_pending_economic_immediate_fee_pusd: exact(
            lifecycle.platform_pending_immediate_fee_pusd,
        ),
        platform_confirmation_pending_economic_count: lifecycle.platform_pending_economic_count,
        open_order_count: lifecycle.open_order_count,
        pending_fill_count: lifecycle.pending_fill_count,
        cumulative_accepted_principal_pusd: exact(lifecycle.cumulative_accepted_principal_pusd),
        current_all_in_commitment_pusd: exact(current_all_in_commitment_pusd),
    }
}

fn projection_decimal(value: &ExactDecimal) -> Decimal {
    Decimal::from_str_exact(&value.0).expect("Ledger 自有投影小数由已解析 Decimal 构造")
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

    #[test]
    fn local_reservation_and_release_entries_balance_the_explicit_amounts() {
        const CONDITION: &str = "condition-local";
        const PRINCIPAL: &str = "123.45678901";
        const FEE: &str = "0.10000000";
        let condition_id = ConditionId(CONDITION.to_owned());
        let reserved = local_reservation_entries(
            &condition_id,
            PRINCIPAL.parse().unwrap(),
            FEE.parse().unwrap(),
        )
        .unwrap();
        assert_eq!(
            reserved,
            vec![
                (
                    LedgerAccountV1::LocalPendingPrincipal(CONDITION.to_owned()),
                    LedgerAssetV1::Pusd,
                    PRINCIPAL.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "-123.45678901".parse().unwrap(),
                ),
                (
                    LedgerAccountV1::LocalPendingImmediateFee(CONDITION.to_owned()),
                    LedgerAssetV1::Pusd,
                    FEE.parse().unwrap(),
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    "-0.10000000".parse().unwrap(),
                ),
            ]
        );
        ensure_balanced(&reserved).unwrap();

        let released = local_reservation_entries(
            &condition_id,
            "-123.45678901".parse().unwrap(),
            "-0.10000000".parse().unwrap(),
        )
        .unwrap();
        ensure_balanced(&released).unwrap();
    }

    #[test]
    fn lifecycle_changes_map_to_asset_balanced_transactions() {
        const CONDITION: &str = "condition-lifecycle";
        const TOKEN: &str = "token-lifecycle";
        let before = LifecycleStateV1::default();
        let mut after = LifecycleStateV1::default();
        after.totals.platform_pending_principal_pusd = "10".parse().unwrap();
        after.totals.platform_pending_immediate_fee_pusd = "0.5".parse().unwrap();
        after.totals.position_cost_basis_pusd = "4".parse().unwrap();
        after.totals.fees_pusd = "0.2".parse().unwrap();
        after.totals.realized_pnl_pusd = "-1".parse().unwrap();
        after.totals.outcome_cost_basis_pusd.insert(
            TOKEN.to_owned(),
            OutcomeCostBasisV1 {
                token_id: TokenId(TOKEN.to_owned()),
                cost_basis_pusd: "4".parse().unwrap(),
            },
        );
        let totals = after.totals.clone();
        let market = after.market_mut(&ConditionId(CONDITION.to_owned()));
        market.totals = totals;

        let entries = lifecycle_change_entries(
            &ReservationStateV1::default(),
            &ReservationStateV1::default(),
            &before,
            &after,
        )
        .unwrap();
        assert!(!entries.is_empty());
        ensure_balanced(&entries).unwrap();
    }

    #[test]
    fn noe_transaction_source_owns_one_result_reference() {
        let reference = NoeResultReferenceV1 {
            commit_container:
                contracts::execution_normalization::NoePublishedContainerReferenceV1 {
                    location: "ledger-test-noe.json".to_owned(),
                    boundary_sha256: contracts::Sha256Hex("a".repeat(64)),
                },
            record_ordinal: 7,
        };
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq: 1,
            source: LedgerTransactionSourceV1::Noe(reference.clone()),
            entries: Vec::new(),
        };
        ensure_balanced(&transaction.entries).unwrap();
        match transaction.source {
            LedgerTransactionSourceV1::Noe(found) => assert_eq!(found, reference),
            _ => panic!("NOE 交易必须只拥有结果引用来源"),
        }
    }
}
