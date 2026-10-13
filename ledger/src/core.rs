use crate::identity::{LedgerIdentity, LedgerIdentityKeyV1};
use execution_normalization::{
    NoeConsumedRecordRefV1, NoeConsumedRecordReferenceV1, NoeConsumptionVisibilityV1,
    NoeRecordPositionV1,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::path::Path;

use contracts::execution_normalization::{
    NoeResultReferenceV1, SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralCancellationOutcomeV1, SourceNeutralEconomicDispositionV1,
    SourceNeutralEconomicIdentityV1, SourceNeutralOrderLifecycleEventV1,
    SourceNeutralOrderLifecycleResultV1, SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1,
    SourceNeutralSubmissionOutcomeV1, SourceNeutralTradeEconomicDispositionV1,
    SourceNeutralTradeLifecycleResultV1,
};
use contracts::{AccountInstanceKeyV1, ConditionId, ExactDecimal, TokenId};
use execution_normalization::{DurableErrorV1, NormalizationRecordV1, read_noe_result_v1};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::durable::RecoveryDurationsV1;
use crate::durable::{
    DurableLedgerV1, DurablePayloadV1, LedgerDurableErrorV1, LedgerSealedSegmentReferenceV1,
};
use crate::lifecycle::{
    AppliedFillIndexV1, LedgerSubmissionItemIdentityV1, LifecycleStateV1, LifecycleTotalsV1,
    MarketLifecycleStateV1, OutcomeCostBasisV1, TradeLifecycleIndexV1,
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

pub(super) enum LifecycleQuantityV1 {
    SubmissionOpen(ExactDecimal),
    MakerMatch(ExactDecimal),
}

type LedgerControl<I = contracts::execution_normalization::StableLogicalIdentityV1> =
    contracts::execution_normalization::SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV1<I>,
        SourceNeutralOrderSnapshotV1<I>,
        I,
    >;
type LedgerEvidence<I = contracts::execution_normalization::StableLogicalIdentityV1> =
    execution_normalization::SourceNeutralEvidenceKind<
        SourceNeutralTradeLifecycleResultV1<I>,
        LedgerControl<I>,
        I,
    >;
use contracts::execution_normalization::SourceNeutralOrderQueryResult;
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
    Durable(LedgerDurableErrorV1),
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
}

impl fmt::Display for LedgerErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoeRead(error) => write!(formatter, "NOE 结果读取失败：{error}"),
            Self::Durable(error) => write!(formatter, "Ledger 耐久写入失败：{error}"),
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
        }
    }
}

impl Error for LedgerErrorV1 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NoeRead(error) => Some(error),
            Self::Durable(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DurableErrorV1> for LedgerErrorV1 {
    fn from(error: DurableErrorV1) -> Self {
        Self::NoeRead(error)
    }
}

impl From<LedgerDurableErrorV1> for LedgerErrorV1 {
    fn from(error: LedgerDurableErrorV1) -> Self {
        Self::Durable(error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct NoeReferenceKeyV1 {
    location: String,
    record_ordinal: u64,
}

impl From<&NoeResultReferenceV1> for NoeReferenceKeyV1 {
    fn from(reference: &NoeResultReferenceV1) -> Self {
        Self {
            location: reference.commit_container.location.clone(),
            record_ordinal: reference.record_ordinal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum LedgerAssetV1 {
    Pusd,
    Outcome(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    SettlementFeeLiability,
    ActualTakerFeeLiability,
    Balancing,
}

type LedgerEntryV1 = (LedgerAccountV1, LedgerAssetV1, Decimal);

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LedgerTransactionSourceV1 {
    #[serde(skip)]
    Memory(NoeConsumedRecordRefV1),
    #[serde(skip)]
    MemoryV2(execution_normalization::NoeConsumedRecordRefV2),
    NoeConsumed(NoeConsumedRecordReferenceV1),
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LedgerTransactionV1 {
    ledger_entry_seq: u64,
    source: LedgerTransactionSourceV1,
    entries: Vec<LedgerEntryV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OutcomeBalanceStateV1 {
    token_id: TokenId,
    balance: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MarketStateV1 {
    condition_id: ConditionId,
    balances: BalanceStateV1,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpenedAccountStateV1 {
    account_instance_key: AccountInstanceKeyV1,
    opening_evidence_identity: String,
    platform_source_prefix: u64,
    available_through_utc_ms: Option<i64>,
    balances: BalanceStateV1,
    markets: BTreeMap<String, MarketStateV1>,
}

#[derive(Debug)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BalanceUpdateV1 {
    cash_pusd: Decimal,
    available_pusd: Decimal,
    principal_encumbrance_pusd: Decimal,
    immediate_taker_fee_encumbrance_pusd: Decimal,
    outcome_balance_updates: BTreeMap<String, OutcomeBalanceStateV1>,
    maker_rebate_receivable_pusd: Decimal,
    taker_rebate_receivable_pusd: Decimal,
    maker_rebate_locked_remainder_pusd: Decimal,
    taker_rebate_locked_remainder_pusd: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MarketBalanceCommittedPatchV1 {
    condition_id: ConditionId,
    update: BalanceUpdateV1,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum AccountCommittedPatchV1 {
    Open(OpenedAccountStateV1),
    Update {
        available_through_utc_ms: Option<i64>,
        platform_source_prefix: Option<u64>,
        global_balance_update: Option<BalanceUpdateV1>,
        market_balance_update: Option<MarketBalanceCommittedPatchV1>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LedgerCommittedEffectV1 {
    transaction: LedgerTransactionV1,
    account: Option<AccountCommittedPatchV1>,
    reservations: Option<ReservationStateV1>,
    lifecycle: Option<LifecycleStateV1>,
    ledger_local_prefix: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    funds: Option<self::funds::FundsPatchV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    economics: Option<self::economics::EconomicsPatch>,
}

impl LedgerCommittedEffectV1 {
    pub(crate) fn ledger_entry_seq(&self) -> u64 {
        self.transaction.ledger_entry_seq
    }
}

#[derive(Serialize, Deserialize)]
enum AdmissionPolicy {
    Legacy(AccountAdmissionPolicyV1),
    DualCash(crate::projection::AccountAdmissionPolicyV2),
}
#[derive(Serialize, Deserialize)]
pub struct LedgerWriterV1 {
    policy: AdmissionPolicy,
    account: Option<OpenedAccountStateV1>,
    ledger_local_prefix: u64,
    reservations: ReservationStateV1,
    lifecycle: LifecycleStateV1,
    current_ledger_entry_seq: u64,
    applied_references: BTreeMap<NoeReferenceKeyV1, u64>,
    #[serde(skip)]
    durable: Option<DurableLedgerV1>,
    applied_memory: BTreeMap<NoeRecordPositionV1, u64>,
    #[serde(skip)]
    record_sink: Option<Box<dyn FnMut(self::memory::LedgerRecordMaterialV2)>>,
    funds: self::funds::FundsStateV1,
    economics: self::economics::EconomicsState,
}
impl LedgerWriterV1 {
    /// 原已提交内存经济前缀；公共关联、资金及 NOE 应用共用原序号。
    pub fn applied_entry_sequence(&self) -> u64 {
        self.current_ledger_entry_seq
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct LedgerHotIndexCountsV1 {
    pub(crate) account_markets: usize,
    pub(crate) account_global_outcome_balances: usize,
    pub(crate) account_market_outcome_balances: usize,
    pub(crate) reservation_requests: usize,
    pub(crate) reservation_applied_local_events: usize,
    pub(crate) reservation_markets: usize,
    pub(crate) lifecycle_submission_requests: usize,
    pub(crate) lifecycle_query_requests: usize,
    pub(crate) lifecycle_orders: usize,
    pub(crate) lifecycle_trades: usize,
    pub(crate) lifecycle_fills: usize,
    pub(crate) lifecycle_global_outcome_costs: usize,
    pub(crate) lifecycle_markets: usize,
    pub(crate) lifecycle_market_outcome_costs: usize,
    pub(crate) applied_noe_references: usize,
}

#[cfg(test)]
impl LedgerHotIndexCountsV1 {
    pub(crate) fn add_assign(&mut self, other: Self) {
        self.account_markets += other.account_markets;
        self.account_global_outcome_balances += other.account_global_outcome_balances;
        self.account_market_outcome_balances += other.account_market_outcome_balances;
        self.reservation_requests += other.reservation_requests;
        self.reservation_applied_local_events += other.reservation_applied_local_events;
        self.reservation_markets += other.reservation_markets;
        self.lifecycle_submission_requests += other.lifecycle_submission_requests;
        self.lifecycle_query_requests += other.lifecycle_query_requests;
        self.lifecycle_orders += other.lifecycle_orders;
        self.lifecycle_trades += other.lifecycle_trades;
        self.lifecycle_fills += other.lifecycle_fills;
        self.lifecycle_global_outcome_costs += other.lifecycle_global_outcome_costs;
        self.lifecycle_markets += other.lifecycle_markets;
        self.lifecycle_market_outcome_costs += other.lifecycle_market_outcome_costs;
        self.applied_noe_references += other.applied_noe_references;
    }
}

impl LedgerWriterV1 {
    pub fn new(policy: AccountAdmissionPolicyV1) -> Self {
        Self::with_policy(AdmissionPolicy::Legacy(policy))
    }
    pub fn new_with_admission_v2(policy: crate::projection::AccountAdmissionPolicyV2) -> Self {
        Self::with_policy(AdmissionPolicy::DualCash(policy))
    }
    fn with_policy(policy: AdmissionPolicy) -> Self {
        Self {
            policy,
            account: None,
            ledger_local_prefix: 0,
            reservations: ReservationStateV1::default(),
            lifecycle: LifecycleStateV1::default(),
            current_ledger_entry_seq: 0,
            applied_references: BTreeMap::new(),
            durable: None,
            applied_memory: BTreeMap::new(),
            record_sink: None,
            funds: self::funds::FundsStateV1::default(),
            economics: self::economics::EconomicsState::default(),
        }
    }

    pub fn create_durable(
        policy: AccountAdmissionPolicyV1,
        directory: impl AsRef<Path>,
    ) -> Result<Self, LedgerErrorV1> {
        let mut writer = Self::new(policy);
        writer.durable = Some(DurableLedgerV1::create(
            directory.as_ref(),
            writer.admission_policy(),
        )?);
        Ok(writer)
    }

    pub fn open_durable(directory: impl AsRef<Path>) -> Result<Self, LedgerErrorV1> {
        let mut recovered = None;
        let mut positions = execution_normalization::NoeRecoveryPositionReader::default();
        let durable = DurableLedgerV1::open(directory.as_ref(), |payload| {
            Self::apply_recovery_payload(&mut recovered, payload, &mut positions)
        })?;
        Self::finish_recovery(recovered, durable)
    }

    #[cfg(test)]
    pub(crate) fn open_durable_measured(
        directory: impl AsRef<Path>,
    ) -> Result<(Self, RecoveryDurationsV1), LedgerErrorV1> {
        let mut recovered = None;
        let mut positions = execution_normalization::NoeRecoveryPositionReader::default();
        let (durable, durations) = DurableLedgerV1::open_measured(directory.as_ref(), |payload| {
            Self::apply_recovery_payload(&mut recovered, payload, &mut positions)
        })?;
        Ok((Self::finish_recovery(recovered, durable)?, durations))
    }

    fn apply_recovery_payload(
        recovered: &mut Option<Self>,
        payload: DurablePayloadV1,
        positions: &mut execution_normalization::NoeRecoveryPositionReader,
    ) -> Result<(), LedgerDurableErrorV1> {
        match payload {
            DurablePayloadV1::Genesis { policy } => {
                if recovered.is_some() {
                    return Err(LedgerDurableErrorV1::InvalidStructure(
                        "Ledger 耐久历史包含重复 genesis".to_owned(),
                    ));
                }
                *recovered = Some(Self::new(policy));
                Ok(())
            }
            DurablePayloadV1::GenesisV2 { policy } => {
                if recovered.is_some() {
                    return Err(LedgerDurableErrorV1::InvalidStructure(
                        "Ledger 重复 genesis".into(),
                    ));
                }
                *recovered = Some(Self::new_with_admission_v2(policy));
                Ok(())
            }
            payload => recovered
                .as_mut()
                .ok_or_else(|| {
                    LedgerDurableErrorV1::InvalidStructure(
                        "Ledger 耐久历史在 genesis 前包含业务记录".to_owned(),
                    )
                })?
                .apply_recovered_payload(payload, positions),
        }
    }

    fn finish_recovery(
        recovered: Option<Self>,
        durable: DurableLedgerV1,
    ) -> Result<Self, LedgerErrorV1> {
        let mut writer = recovered.ok_or_else(|| {
            LedgerDurableErrorV1::InvalidStructure("Ledger 耐久历史缺少 genesis".to_owned())
        })?;
        writer.durable = Some(durable);
        Ok(writer)
    }

    pub fn seal_active_segment(
        &mut self,
    ) -> Result<Option<LedgerSealedSegmentReferenceV1>, LedgerErrorV1> {
        self.durable
            .as_mut()
            .ok_or_else(|| {
                LedgerDurableErrorV1::InvalidStructure(
                    "纯内存 Ledger 没有可封存的活动段".to_owned(),
                )
            })?
            .seal_active_segment()
            .map_err(Into::into)
    }

    /// 旧 V1 实例的政策；V2 调用者使用 admission_policy_v2。
    pub fn admission_policy(&self) -> &AccountAdmissionPolicyV1 {
        match &self.policy {
            AdmissionPolicy::Legacy(policy) => policy,
            AdmissionPolicy::DualCash(_) => panic!("V2 实例没有 V1 市场额度政策"),
        }
    }
    pub fn admission_policy_v2(&self) -> Option<crate::projection::AccountAdmissionPolicyV2> {
        match self.policy {
            AdmissionPolicy::DualCash(policy) => Some(policy),
            AdmissionPolicy::Legacy(_) => None,
        }
    }
    fn genesis(&self) -> DurablePayloadV1 {
        match &self.policy {
            AdmissionPolicy::Legacy(policy) => DurablePayloadV1::Genesis {
                policy: policy.clone(),
            },
            AdmissionPolicy::DualCash(policy) => DurablePayloadV1::GenesisV2 { policy: *policy },
        }
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

        self.account
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
        self.reserve_parsed_intent(request_identity, condition_id, principal, immediate_fee)
    }

    fn reserve_parsed_intent(
        &mut self,
        request_identity: &LedgerRequestIdentityV1,
        condition_id: &ConditionId,
        principal: Decimal,
        immediate_fee: Decimal,
    ) -> Result<LedgerReservationOutcomeV1, LedgerErrorV1> {
        let account = self
            .account
            .as_ref()
            .ok_or(LedgerErrorV1::AccountNotOpened)?;
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
            return self.cache_capacity_outcome(
                request_identity,
                LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds,
            );
        }

        if matches!(self.policy, AdmissionPolicy::DualCash(_)) {
            let total_reserved = account.balances.principal_encumbrance_pusd
                + account.balances.immediate_taker_fee_encumbrance_pusd
                + pending_total;
            let maker_available = self.economics.cash.maker_cash - total_reserved;
            if requested_total > maker_available {
                return self.cache_capacity_outcome(
                    request_identity,
                    LedgerReservationOutcomeV1::InsufficientMakerAvailableFunds,
                );
            }
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
        if let AdmissionPolicy::Legacy(policy) = &self.policy {
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
                &policy.per_market_all_in_commitment_limit_pusd,
                "admission_policy.per_market_all_in_commitment_limit_pusd",
            )?;
            if market_commitment_after_request > market_limit {
                return self.cache_capacity_outcome(
                    request_identity,
                    LedgerReservationOutcomeV1::PerMarketLimitExceeded,
                );
            }
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

        let outcome = LedgerReservationOutcomeV1::Reserved { ledger_entry_seq };
        let mut reservations = ReservationStateV1 {
            principal_pusd: next_global_principal,
            immediate_fee_pusd: next_global_fee,
            before_send_principal_pusd: next_before_send_principal,
            before_send_immediate_fee_pusd: next_before_send_fee,
            before_send_count: next_before_send_count,
            after_send_unknown_principal_pusd: self.reservations.after_send_unknown_principal_pusd,
            after_send_unknown_immediate_fee_pusd: self
                .reservations
                .after_send_unknown_immediate_fee_pusd,
            after_send_unknown_count: self.reservations.after_send_unknown_count,
            ..ReservationStateV1::default()
        };
        reservations.markets.insert(
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
        reservations.insert_request(
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
        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: None,
            reservations: Some(reservations),
            lifecycle: None,
            ledger_local_prefix: Some(ledger_local_prefix),
        })?;
        Ok(outcome)
    }

    /// 同一动作的各身份先按共同现金精确求和；不产生“首腿已占用、次腿资金不足”的局部预留。
    /// 双现金宿主使用此入口；既有单身份粘滞决定和原记录机制保持。
    pub fn try_reserve_action_v2(
        &mut self,
        intents: &[ReserveIntentV1],
    ) -> Result<Vec<LedgerReservationOutcomeV1>, LedgerErrorV1> {
        let (actual, maker, _) = self.cash_balances()?;
        let mut total = Decimal::ZERO;
        let mut parsed = BTreeMap::new();
        for intent in intents {
            if self
                .reservations
                .requests
                .contains_key(&intent.request_identity)
                || parsed.contains_key(&intent.request_identity)
            {
                continue;
            }
            let principal = parse_decimal(&intent.requested_principal_pusd, "action.principal")?;
            let fee = parse_decimal(&intent.immediate_fee_reserve_pusd, "action.immediate_fee")?;
            parsed.insert(intent.request_identity.clone(), (principal, fee));
            total = checked_add(
                total,
                checked_add(principal, fee, "action.item_total")?,
                "action.total",
            )?;
        }
        let unavailable = if total > actual {
            Some(LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds)
        } else if matches!(self.policy, AdmissionPolicy::DualCash(_)) && total > maker {
            Some(LedgerReservationOutcomeV1::InsufficientMakerAvailableFunds)
        } else {
            None
        };
        if let Some(outcome) = unavailable {
            return intents
                .iter()
                .map(|intent| {
                    if let Some(cached) = self.reservations.requests.get(&intent.request_identity) {
                        Ok(cached.outcome.clone())
                    } else {
                        self.cache_capacity_outcome(&intent.request_identity, outcome.clone())
                    }
                })
                .collect();
        }
        intents
            .iter()
            .map(|intent| {
                if let Some(cached) = self.reservations.requests.get(&intent.request_identity) {
                    return Ok(cached.outcome.clone());
                }
                let (principal, fee) = parsed[&intent.request_identity];
                self.reserve_parsed_intent(
                    &intent.request_identity,
                    &intent.condition_id,
                    principal,
                    fee,
                )
            })
            .collect()
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
        let mut keys = SparseLedgerStateKeysV1::default();
        keys.add_request(&self.reservations, request_identity);
        let mut reservations = reservation_state_slice(&self.reservations, &keys);
        move_reservation_to_submitted_unknown(
            &mut reservations,
            request_identity,
            ledger_entry_seq,
        )?;
        reservations
            .applied_local_events
            .insert(local_event_identity.clone(), ledger_entry_seq);
        let mut lifecycle = lifecycle_state_slice(&self.lifecycle, &keys);
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

        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: None,
            reservations: Some(reservations),
            lifecycle: Some(lifecycle),
            ledger_local_prefix: Some(ledger_local_prefix),
        })?;
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
        let mut reservations =
            reservation_state_slice(&self.reservations, &SparseLedgerStateKeysV1::default());
        reservations
            .applied_local_events
            .insert(local_event_identity.clone(), ledger_entry_seq);
        let mut lifecycle =
            lifecycle_state_slice(&self.lifecycle, &SparseLedgerStateKeysV1::default());
        lifecycle
            .query_requests
            .insert(query_global_request_id.to_owned(), request_identity.clone());
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: LedgerTransactionSourceV1::LocalQueryAssociationEvent {
                local_event_identity: local_event_identity.clone(),
                query_global_request_id: query_global_request_id.to_owned(),
                request_identity: request_identity.clone(),
            },
            entries: Vec::new(),
        };

        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: None,
            reservations: Some(reservations),
            lifecycle: Some(lifecycle),
            ledger_local_prefix: Some(ledger_local_prefix),
        })?;
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    pub fn release_before_send(
        &mut self,
        local_event_identity: &LedgerLocalEventIdentityV1,
        request_identity: &LedgerRequestIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.release_local_unsent(local_event_identity, request_identity, false)
    }

    /// 调用方已确定尚未调用发送；允许释放仅完成本地发送登记的预留。
    /// 不能用于发送错误或平台结果未知的请求，后者继续沿真实反馈闭合。
    pub fn release_known_unsent(
        &mut self,
        local_event_identity: &LedgerLocalEventIdentityV1,
        request_identity: &LedgerRequestIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.release_local_unsent(local_event_identity, request_identity, true)
    }

    fn release_local_unsent(
        &mut self,
        local_event_identity: &LedgerLocalEventIdentityV1,
        request_identity: &LedgerRequestIdentityV1,
        registered_but_unsent: bool,
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
        if reservation.status != ReservationStatusV1::ReservedBeforeSend
            && !(registered_but_unsent
                && reservation.status == ReservationStatusV1::SubmittedUnknown)
        {
            return Ok(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: reservation.last_applied_ledger_entry_seq,
            });
        }

        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let ledger_local_prefix = self.next_ledger_local_prefix()?;
        let mut keys = SparseLedgerStateKeysV1::default();
        keys.add_request(&self.reservations, request_identity);
        let mut reservations = reservation_state_slice(&self.reservations, &keys);
        remove_local_pending_reservation(&mut reservations, &reservation)?;
        let prepared_reservation = reservations
            .requests
            .get_mut(request_identity)
            .and_then(|request| request.reservation.as_mut())
            .expect("已读取的发送前预留保持存在");
        prepared_reservation.status = ReservationStatusV1::Released;
        prepared_reservation.last_applied_ledger_entry_seq = ledger_entry_seq;
        reservations
            .applied_local_events
            .insert(local_event_identity.clone(), ledger_entry_seq);
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

        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: None,
            reservations: Some(reservations),
            lifecycle: None,
            ledger_local_prefix: Some(ledger_local_prefix),
        })?;
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

        let source = LedgerTransactionSourceV1::Noe(reference.clone());
        let resolved = read_noe_result_v1(reference)?;
        match &resolved.record {
            NormalizationRecordV1::EconomicFact { fact, .. } => match &fact.disposition {
                SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } => self
                    .apply_opening(
                        &source,
                        &resolved.context.source.account_instance_key,
                        &resolved.context.source.account_opening_evidence_identity,
                        Some(resolved.boundary.durable_available_at_utc_ms),
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
                        &source,
                        &resolved.context.source.account_instance_key,
                        *account_transition_seq,
                        Some(resolved.boundary.durable_available_at_utc_ms),
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
                &source,
                &resolved.context.source.account_instance_key,
                Some(resolved.boundary.durable_available_at_utc_ms),
                &source_reference.record,
                evidence,
                None,
            ),
            NormalizationRecordV1::Pending { .. } if self.account.is_some() => self
                .apply_pending_result(
                    &source,
                    &resolved.context.source.account_instance_key,
                    Some(resolved.boundary.durable_available_at_utc_ms),
                ),
            _ => Err(LedgerErrorV1::NonEconomicRecord),
        }
    }

    fn apply_lifecycle_evidence<I: LedgerIdentity>(
        &mut self,
        reference: &LedgerTransactionSourceV1,
        account_instance_key: &AccountInstanceKeyV1,
        available_through_utc_ms: Option<i64>,
        source_record: &SimV2SourceRecordReferenceV1,
        evidence: &LedgerEvidence<I>,
        quantity: Option<LifecycleQuantityV1>,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.ensure_noe_account(account_instance_key)?;
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let keys =
            lifecycle_evidence_keys(&self.reservations, &self.lifecycle, source_record, evidence);
        let before_reservations = reservation_balance_slice(&self.reservations, &keys);
        let mut reservations = reservation_state_slice(&self.reservations, &keys);
        let before_lifecycle = lifecycle_balance_slice(&self.lifecycle, &keys);
        let mut lifecycle = lifecycle_state_slice(&self.lifecycle, &keys);
        match evidence {
            LedgerEvidence::Control { result } => apply_control_lifecycle(
                &mut reservations,
                &mut lifecycle,
                source_record,
                result,
                ledger_entry_seq,
            )?,
            LedgerEvidence::OrderLifecycle { result } => {
                apply_order_lifecycle(&mut lifecycle, result, ledger_entry_seq)?;
            }
            LedgerEvidence::TradeLifecycle { result } => {
                apply_trade_lifecycle(&mut lifecycle, result, ledger_entry_seq)?;
            }
            LedgerEvidence::SettlementLifecycle { .. } => {}
            _ => return Err(LedgerErrorV1::NonEconomicRecord),
        }

        if let Some(quantity) = quantity {
            apply_lifecycle_quantity(&mut lifecycle, evidence, quantity, ledger_entry_seq)?;
        }

        let entries = lifecycle_change_entries(
            &before_reservations,
            &reservations,
            &before_lifecycle,
            &lifecycle,
        )?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: reference.clone(),
            entries,
        };

        let next_available_through_utc_ms = self
            .account
            .as_ref()
            .expect("NOE 生命周期证据应用前账户保持存在")
            .available_through_utc_ms
            .max(available_through_utc_ms);
        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: Some(AccountCommittedPatchV1::Update {
                available_through_utc_ms: next_available_through_utc_ms,
                platform_source_prefix: None,
                global_balance_update: None,
                market_balance_update: None,
            }),
            reservations: Some(reservations),
            lifecycle: Some(lifecycle),
            ledger_local_prefix: None,
        })?;
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    fn apply_pending_result(
        &mut self,
        reference: &LedgerTransactionSourceV1,
        account_instance_key: &AccountInstanceKeyV1,
        available_through_utc_ms: Option<i64>,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.ensure_noe_account(account_instance_key)?;
        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: reference.clone(),
            entries: Vec::new(),
        };

        let next_available_through_utc_ms = self
            .account
            .as_ref()
            .expect("NOE 待定结果应用前账户保持存在")
            .available_through_utc_ms
            .max(available_through_utc_ms);
        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: Some(AccountCommittedPatchV1::Update {
                available_through_utc_ms: next_available_through_utc_ms,
                platform_source_prefix: None,
                global_balance_update: None,
                market_balance_update: None,
            }),
            reservations: None,
            lifecycle: None,
            ledger_local_prefix: None,
        })?;
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
        let platform_confirmed_projection = project_balances(
            &account.balances,
            &self.lifecycle.totals,
            &self.lifecycle.outcome_cost_basis_pusd,
        );
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
                let lifecycle_market = self.lifecycle.markets.get(&condition_id.0);
                let lifecycle = lifecycle_market
                    .map(|market| &market.totals)
                    .unwrap_or(&empty_lifecycle);
                let empty_outcome_costs = BTreeMap::new();
                let outcome_costs = lifecycle_market
                    .map(|market| &market.outcome_cost_basis_pusd)
                    .unwrap_or(&empty_outcome_costs);
                let balances = account
                    .markets
                    .get(&condition_id.0)
                    .map(|market| project_balances(&market.balances, lifecycle, outcome_costs))
                    .unwrap_or_else(|| {
                        project_balances(&BalanceStateV1::empty(), lifecycle, outcome_costs)
                    });
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
            available_through_utc_ms: account.available_through_utc_ms.unwrap_or_default(),
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
        reference: &LedgerTransactionSourceV1,
        account_instance_key: &AccountInstanceKeyV1,
        opening_evidence_identity: &str,
        available_through_utc_ms: Option<i64>,
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
            source: reference.clone(),
            entries,
        };

        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: Some(AccountCommittedPatchV1::Open(OpenedAccountStateV1 {
                account_instance_key: account_instance_key.clone(),
                opening_evidence_identity: opening_evidence_identity.to_owned(),
                platform_source_prefix: 0,
                available_through_utc_ms,
                balances: BalanceStateV1::opened(available),
                markets: BTreeMap::new(),
            })),
            reservations: None,
            lifecycle: None,
            ledger_local_prefix: None,
        })?;
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_transition<I: LedgerIdentity>(
        &mut self,
        reference: &LedgerTransactionSourceV1,
        account_instance_key: &AccountInstanceKeyV1,
        account_transition_seq: u64,
        available_through_utc_ms: Option<i64>,
        condition_id: Option<&ConditionId>,
        transition: &SourceNeutralAccountTransitionKindV1<I>,
        balance_delta: &SourceNeutralAccountBalanceDeltaV1,
        source_record: &SimV2SourceRecordReferenceV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.apply_transition_common(
            reference,
            account_instance_key,
            account_transition_seq,
            available_through_utc_ms,
            condition_id,
            Some(transition),
            balance_delta,
            source_record,
        )
    }

    fn apply_transition_common<I: LedgerIdentity>(
        &mut self,
        reference: &LedgerTransactionSourceV1,
        account_instance_key: &AccountInstanceKeyV1,
        account_transition_seq: u64,
        available_through_utc_ms: Option<i64>,
        condition_id: Option<&ConditionId>,
        transition: Option<&SourceNeutralAccountTransitionKindV1<I>>,
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
                prepare_market_balance_update(existing, &parsed)
            })
            .transpose()?;

        let ledger_entry_seq = self.next_ledger_entry_seq()?;
        let keys = transition
            .map(|transition| {
                economic_transition_keys(
                    &self.reservations,
                    &self.lifecycle,
                    condition_id,
                    transition,
                    &parsed,
                    source_record,
                )
            })
            .unwrap_or_default();
        let before_reservations = reservation_balance_slice(&self.reservations, &keys);
        let mut reservations = reservation_state_slice(&self.reservations, &keys);
        let before_lifecycle = lifecycle_balance_slice(&self.lifecycle, &keys);
        let mut lifecycle = lifecycle_state_slice(&self.lifecycle, &keys);
        if let Some(transition) = transition {
            apply_economic_lifecycle(
                &mut reservations,
                &mut lifecycle,
                condition_id,
                transition,
                &parsed,
                source_record,
                ledger_entry_seq,
            )?;
        }
        let mut entries = transition_entries(condition_id, &parsed)?;
        entries.extend(lifecycle_change_entries(
            &before_reservations,
            &reservations,
            &before_lifecycle,
            &lifecycle,
        )?);
        let transaction = LedgerTransactionV1 {
            ledger_entry_seq,
            source: reference.clone(),
            entries,
        };

        let market_balance_update =
            condition_id
                .zip(market_update)
                .map(|(condition_id, update)| MarketBalanceCommittedPatchV1 {
                    condition_id: condition_id.clone(),
                    update,
                });
        self.commit_effect(LedgerCommittedEffectV1 {
            funds: None,
            economics: None,
            transaction,
            account: Some(AccountCommittedPatchV1::Update {
                available_through_utc_ms,
                platform_source_prefix: Some(account_transition_seq),
                global_balance_update: Some(global_update),
                market_balance_update,
            }),
            reservations: Some(reservations),
            lifecycle: Some(lifecycle),
            ledger_local_prefix: None,
        })?;
        Ok(LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq })
    }

    fn current_ledger_entry_seq(&self) -> u64 {
        self.current_ledger_entry_seq
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
    ) -> Result<LedgerReservationOutcomeV1, LedgerErrorV1> {
        let request = CachedReservationRequestV1 {
            outcome: outcome.clone(),
            reservation: None,
        };
        let payload = DurablePayloadV1::ReservationDecision {
            request_identity: request_identity.clone(),
            request,
        };
        if let Some(durable) = &mut self.durable {
            durable.append(&payload)?;
        }
        if let Some(sink) = &mut self.record_sink {
            sink(self::memory::LedgerRecordMaterialV2::new(payload.clone()));
        }
        let DurablePayloadV1::ReservationDecision {
            request_identity,
            request,
        } = payload
        else {
            unreachable!("刚构造的容量决定保持其耐久记录类型")
        };
        self.reservations.insert_request(request_identity, request);
        Ok(outcome)
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

    fn commit_effect(&mut self, mut effect: LedgerCommittedEffectV1) -> Result<(), LedgerErrorV1> {
        let balance_delta = match &effect.transaction.source {
            LedgerTransactionSourceV1::Memory(source) => memory::liability_delta(source),
            LedgerTransactionSourceV1::MemoryV2(source) => memory::liability_delta(source),
            _ => None,
        };
        if let Some(balance_delta) = balance_delta {
            for (account, amount) in [
                (
                    LedgerAccountV1::SettlementFeeLiability,
                    &balance_delta.settlement_fee_liability_pusd_delta,
                ),
                (
                    LedgerAccountV1::ActualTakerFeeLiability,
                    &balance_delta.actual_taker_fee_liability_pusd_delta,
                ),
            ] {
                let amount = parse_decimal(amount, "economic liability")?;
                if !amount.is_zero() {
                    effect
                        .transaction
                        .entries
                        .push((account, LedgerAssetV1::Pusd, amount));
                    effect.transaction.entries.push((
                        LedgerAccountV1::Balancing,
                        LedgerAssetV1::Pusd,
                        -amount,
                    ));
                }
            }
        }
        effect.funds = self.funds.prepare(&effect)?;
        effect.economics = self.economics.prepare(&effect)?;
        let payload = DurablePayloadV1::Transaction(Box::new(effect));
        if let Some(durable) = &mut self.durable {
            durable.append(&payload)?;
        }
        let DurablePayloadV1::Transaction(effect) = payload else {
            unreachable!("刚构造的 Ledger 交易保持其耐久记录类型")
        };
        if let Some(sink) = &mut self.record_sink {
            sink(self::memory::LedgerRecordMaterialV2::new(
                DurablePayloadV1::Transaction(effect.clone()),
            ));
        }
        self.apply_committed_effect(*effect);
        Ok(())
    }

    fn apply_recovered_payload(
        &mut self,
        payload: DurablePayloadV1,
        positions: &mut execution_normalization::NoeRecoveryPositionReader,
    ) -> Result<(), LedgerDurableErrorV1> {
        match payload {
            DurablePayloadV1::Genesis { .. } | DurablePayloadV1::GenesisV2 { .. } => {
                Err(LedgerDurableErrorV1::InvalidStructure(
                    "Ledger 耐久历史包含非首条 genesis".to_owned(),
                ))
            }
            DurablePayloadV1::ReservationDecision {
                request_identity,
                request,
            } => {
                self.reservations.insert_request(request_identity, request);
                Ok(())
            }
            DurablePayloadV1::Transaction(effect) => {
                let expected = self
                    .current_ledger_entry_seq
                    .checked_add(1)
                    .ok_or_else(|| {
                        LedgerDurableErrorV1::InvalidStructure("Ledger 序号溢出".to_owned())
                    })?;
                if effect.ledger_entry_seq() != expected {
                    return Err(LedgerDurableErrorV1::InvalidStructure(format!(
                        "恢复的 Ledger 序号不连续：预期 {expected}，实际 {}",
                        effect.ledger_entry_seq()
                    )));
                }
                if let LedgerTransactionSourceV1::NoeConsumed(source) = &effect.transaction.source {
                    let position = positions
                        .read(source.result())
                        .map_err(|e| LedgerDurableErrorV1::InvalidStructure(e.to_string()))?;
                    self.applied_memory
                        .insert(position, effect.ledger_entry_seq());
                }
                self.apply_committed_effect(*effect);
                Ok(())
            }
        }
    }

    fn apply_committed_effect(&mut self, effect: LedgerCommittedEffectV1) {
        if let Some(patch) = effect.economics {
            self.economics.commit(patch);
        }
        if let Some(patch) = effect.funds {
            self.funds.commit(patch);
        }
        if let Some(account_patch) = effect.account {
            match account_patch {
                AccountCommittedPatchV1::Open(account) => self.account = Some(account),
                AccountCommittedPatchV1::Update {
                    available_through_utc_ms,
                    platform_source_prefix,
                    global_balance_update,
                    market_balance_update,
                } => {
                    let account = self.account.as_mut().expect("已准备的账户补丁拥有开户前驱");
                    if let Some(update) = global_balance_update {
                        update.commit(&mut account.balances);
                    }
                    if let Some(market_patch) = market_balance_update {
                        let market = account
                            .markets
                            .entry(market_patch.condition_id.0.clone())
                            .or_insert_with(|| MarketStateV1 {
                                condition_id: market_patch.condition_id,
                                balances: BalanceStateV1::empty(),
                            });
                        market_patch.update.commit(&mut market.balances);
                    }
                    if let Some(platform_source_prefix) = platform_source_prefix {
                        account.platform_source_prefix = platform_source_prefix;
                    }
                    account.available_through_utc_ms = available_through_utc_ms;
                }
            }
        }
        if let Some(reservations) = effect.reservations {
            commit_reservation_slice(&mut self.reservations, reservations);
        }
        if let Some(lifecycle) = effect.lifecycle {
            commit_lifecycle_slice(&mut self.lifecycle, lifecycle);
        }
        if let Some(ledger_local_prefix) = effect.ledger_local_prefix {
            self.ledger_local_prefix = ledger_local_prefix;
        }
        match &effect.transaction.source {
            LedgerTransactionSourceV1::Memory(record) => {
                self.applied_memory
                    .insert(record.position(), effect.transaction.ledger_entry_seq);
            }
            LedgerTransactionSourceV1::MemoryV2(record) => {
                self.applied_memory
                    .insert(record.position(), effect.transaction.ledger_entry_seq);
            }
            LedgerTransactionSourceV1::NoeConsumed(_) => {}
            LedgerTransactionSourceV1::Noe(reference) => {
                self.applied_references.insert(
                    NoeReferenceKeyV1::from(reference),
                    effect.transaction.ledger_entry_seq,
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
        self.current_ledger_entry_seq = effect.transaction.ledger_entry_seq;
    }

    #[cfg(test)]
    fn inject_durable_append_failure(&mut self) {
        self.durable
            .as_mut()
            .expect("测试 Ledger 使用耐久句柄")
            .inject_append_failure();
    }

    #[cfg(test)]
    fn inject_durable_sync_failure(&mut self) {
        self.durable
            .as_mut()
            .expect("测试 Ledger 使用耐久句柄")
            .inject_sync_failure();
    }

    #[cfg(test)]
    pub(crate) fn baseline_hot_index_counts(&self) -> LedgerHotIndexCountsV1 {
        let account = self.account.as_ref();
        LedgerHotIndexCountsV1 {
            account_markets: account.map_or(0, |account| account.markets.len()),
            account_global_outcome_balances: account
                .map_or(0, |account| account.balances.outcome_balances.len()),
            account_market_outcome_balances: account.map_or(0, |account| {
                account
                    .markets
                    .values()
                    .map(|market| market.balances.outcome_balances.len())
                    .sum()
            }),
            reservation_requests: self.reservations.requests.len(),
            reservation_applied_local_events: self.reservations.applied_local_events.len(),
            reservation_markets: self.reservations.markets.len(),
            lifecycle_submission_requests: self.lifecycle.submission_requests.len(),
            lifecycle_query_requests: self.lifecycle.query_requests.len(),
            lifecycle_orders: self.lifecycle.orders.len(),
            lifecycle_trades: self.lifecycle.trades.len(),
            lifecycle_fills: self.lifecycle.applied_fills.len(),
            lifecycle_global_outcome_costs: self.lifecycle.outcome_cost_basis_pusd.len(),
            lifecycle_markets: self.lifecycle.markets.len(),
            lifecycle_market_outcome_costs: self
                .lifecycle
                .markets
                .values()
                .map(|market| market.outcome_cost_basis_pusd.len())
                .sum(),
            applied_noe_references: self.applied_references.len(),
        }
    }
}

#[derive(Clone, Debug)]
struct ReservationTransitionDetailsV1 {
    condition_id: ConditionId,
    principal_pusd: Decimal,
}

#[derive(Default)]
struct SparseLedgerStateKeysV1 {
    requests: BTreeSet<LedgerRequestIdentityV1>,
    conditions: BTreeSet<String>,
    submission_items: BTreeSet<LedgerSubmissionItemIdentityV1>,
    query_requests: BTreeSet<String>,
    orders: BTreeSet<LedgerIdentityKeyV1>,
    trades: BTreeSet<LedgerIdentityKeyV1>,
    fills: BTreeSet<(LedgerIdentityKeyV1, u32)>,
    outcome_tokens: BTreeSet<String>,
}

impl SparseLedgerStateKeysV1 {
    fn add_condition(&mut self, condition_id: &ConditionId) {
        self.conditions.insert(condition_id.0.clone());
    }

    fn add_request(
        &mut self,
        reservations: &ReservationStateV1,
        request_identity: &LedgerRequestIdentityV1,
    ) {
        self.requests.insert(request_identity.clone());
        if let Some(reservation) = reservations
            .requests
            .get(request_identity)
            .and_then(|request| request.reservation.as_ref())
        {
            self.add_condition(&reservation.condition_id);
        }
    }

    fn add_order(&mut self, lifecycle: &LifecycleStateV1, order_identity: &LedgerIdentityKeyV1) {
        self.orders.insert(order_identity.to_owned());
        if let Some(condition_id) = lifecycle
            .orders
            .get(order_identity)
            .and_then(|order| order.condition_id.as_ref())
        {
            self.add_condition(condition_id);
        }
    }

    fn add_trade(&mut self, lifecycle: &LifecycleStateV1, trade_identity: &LedgerIdentityKeyV1) {
        self.trades.insert(trade_identity.to_owned());
        if let Some(trade) = lifecycle.trades.get(trade_identity) {
            self.add_condition(&trade.condition_id);
        }
    }

    fn add_fill(
        &mut self,
        lifecycle: &LifecycleStateV1,
        trade_identity: &LedgerIdentityKeyV1,
        path_ordinal: u32,
    ) {
        let key = (trade_identity.to_owned(), path_ordinal);
        self.fills.insert(key.clone());
        if let Some(fill) = lifecycle.applied_fills.get(&key) {
            if let Some(condition_id) = &fill.condition_id {
                self.add_condition(condition_id);
            }
            if let Some(token_id) = &fill.token_id {
                self.outcome_tokens.insert(token_id.0.clone());
            }
        }
    }
}

// 原密封最终性每个trade只产生一次经济应用；FAILED的ordinal是失败步骤，
// 不是applied_fills中保留的经济应用步骤。沿原trade身份找尚未撤回的应用项，
// 同时供稀疏候选和实际补偿使用，保持原持久键及两种ordinal各自的含义。
fn active_applied_fill_key(
    lifecycle: &LifecycleStateV1,
    trade_identity: &LedgerIdentityKeyV1,
) -> Option<(LedgerIdentityKeyV1, u32)> {
    lifecycle
        .applied_fills
        .range((trade_identity.clone(), 0)..=(trade_identity.clone(), u32::MAX))
        .find(|(_, fill)| fill.active)
        .map(|(key, _)| key.clone())
}

fn reservation_state_slice(
    source: &ReservationStateV1,
    keys: &SparseLedgerStateKeysV1,
) -> ReservationStateV1 {
    let mut slice = reservation_balance_slice(source, keys);
    for request_identity in &keys.requests {
        if let Some(request) = source.requests.get(request_identity) {
            slice
                .requests
                .insert(request_identity.clone(), request.clone());
        }
    }
    slice
}

fn reservation_balance_slice(
    source: &ReservationStateV1,
    keys: &SparseLedgerStateKeysV1,
) -> ReservationStateV1 {
    let mut slice = ReservationStateV1 {
        principal_pusd: source.principal_pusd,
        immediate_fee_pusd: source.immediate_fee_pusd,
        before_send_principal_pusd: source.before_send_principal_pusd,
        before_send_immediate_fee_pusd: source.before_send_immediate_fee_pusd,
        before_send_count: source.before_send_count,
        after_send_unknown_principal_pusd: source.after_send_unknown_principal_pusd,
        after_send_unknown_immediate_fee_pusd: source.after_send_unknown_immediate_fee_pusd,
        after_send_unknown_count: source.after_send_unknown_count,
        ..ReservationStateV1::default()
    };
    for condition_id in &keys.conditions {
        if let Some(market) = source.markets.get(condition_id) {
            slice.markets.insert(condition_id.clone(), market.clone());
        }
    }
    slice
}

fn lifecycle_state_slice(
    source: &LifecycleStateV1,
    keys: &SparseLedgerStateKeysV1,
) -> LifecycleStateV1 {
    let mut slice = lifecycle_balance_slice(source, keys);
    for submission_item in &keys.submission_items {
        if let Some(request_identity) = source.submission_requests.get(submission_item) {
            slice
                .submission_requests
                .insert(submission_item.clone(), request_identity.clone());
        }
    }
    for query_request in &keys.query_requests {
        if let Some(request_identity) = source.query_requests.get(query_request) {
            slice
                .query_requests
                .insert(query_request.clone(), request_identity.clone());
        }
    }
    for order_identity in &keys.orders {
        if let Some(order) = source.orders.get(order_identity) {
            slice.orders.insert(order_identity.clone(), order.clone());
        }
    }
    for trade_identity in &keys.trades {
        if let Some(trade) = source.trades.get(trade_identity) {
            slice.trades.insert(trade_identity.clone(), trade.clone());
        }
    }
    for fill_key in &keys.fills {
        if let Some(fill) = source.applied_fills.get(fill_key) {
            slice.applied_fills.insert(fill_key.clone(), fill.clone());
        }
    }
    for token_id in &keys.outcome_tokens {
        if let Some(cost_basis) = source.outcome_cost_basis_pusd.get(token_id) {
            slice
                .outcome_cost_basis_pusd
                .insert(token_id.clone(), cost_basis.clone());
        }
    }
    slice
}

fn lifecycle_balance_slice(
    source: &LifecycleStateV1,
    keys: &SparseLedgerStateKeysV1,
) -> LifecycleStateV1 {
    let mut slice = LifecycleStateV1 {
        totals: source.totals.clone(),
        ..LifecycleStateV1::default()
    };
    for condition_id in &keys.conditions {
        if let Some(market) = source.markets.get(condition_id) {
            let outcome_cost_basis_pusd = keys
                .outcome_tokens
                .iter()
                .filter_map(|token_id| {
                    market
                        .outcome_cost_basis_pusd
                        .get(token_id)
                        .map(|cost_basis| (token_id.clone(), cost_basis.clone()))
                })
                .collect();
            slice.markets.insert(
                condition_id.clone(),
                MarketLifecycleStateV1 {
                    condition_id: market.condition_id.clone(),
                    totals: market.totals.clone(),
                    outcome_cost_basis_pusd,
                },
            );
        }
    }
    slice
}

fn commit_reservation_slice(target: &mut ReservationStateV1, slice: ReservationStateV1) {
    target.principal_pusd = slice.principal_pusd;
    target.immediate_fee_pusd = slice.immediate_fee_pusd;
    target.before_send_principal_pusd = slice.before_send_principal_pusd;
    target.before_send_immediate_fee_pusd = slice.before_send_immediate_fee_pusd;
    target.before_send_count = slice.before_send_count;
    target.after_send_unknown_principal_pusd = slice.after_send_unknown_principal_pusd;
    target.after_send_unknown_immediate_fee_pusd = slice.after_send_unknown_immediate_fee_pusd;
    target.after_send_unknown_count = slice.after_send_unknown_count;
    for (id, request) in slice.requests {
        target.insert_request(id, request);
    }
    target
        .applied_local_events
        .extend(slice.applied_local_events);
    target.markets.extend(slice.markets);
}

fn commit_lifecycle_slice(target: &mut LifecycleStateV1, slice: LifecycleStateV1) {
    target.totals = slice.totals;
    target.submission_requests.extend(slice.submission_requests);
    target.query_requests.extend(slice.query_requests);
    target.orders.extend(slice.orders);
    target.trades.extend(slice.trades);
    target.applied_fills.extend(slice.applied_fills);
    target
        .outcome_cost_basis_pusd
        .extend(slice.outcome_cost_basis_pusd);
    for (condition_id, market_slice) in slice.markets {
        if let Some(market) = target.markets.get_mut(&condition_id) {
            market.condition_id = market_slice.condition_id;
            market.totals = market_slice.totals;
            market
                .outcome_cost_basis_pusd
                .extend(market_slice.outcome_cost_basis_pusd);
        } else {
            target.markets.insert(condition_id, market_slice);
        }
    }
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

fn lifecycle_evidence_keys<I: LedgerIdentity>(
    reservations: &ReservationStateV1,
    lifecycle: &LifecycleStateV1,
    source_record: &SimV2SourceRecordReferenceV1,
    evidence: &LedgerEvidence<I>,
) -> SparseLedgerStateKeysV1 {
    let mut keys = SparseLedgerStateKeysV1::default();
    match evidence {
        LedgerEvidence::Control { result } => match result {
            LedgerControl::CancellationRequestCompleted { result } => match *result {},
            LedgerControl::Submission { outcome } => {
                if let Some(submission_item) = submission_item_identity(source_record) {
                    if let Some(request_identity) =
                        lifecycle.submission_requests.get(&submission_item)
                    {
                        keys.add_request(reservations, request_identity);
                    }
                    keys.submission_items.insert(submission_item);
                }
                if let SourceNeutralSubmissionOutcomeV1::Accepted { order } = outcome {
                    keys.add_order(lifecycle, &order.order_identity.ledger_key());
                    keys.add_condition(&order.condition_id);
                }
            }
            LedgerControl::OrderQuery { result } => {
                if let Some(query_request) = query_global_request_id(source_record) {
                    if let Some(request_identity) = lifecycle.query_requests.get(query_request) {
                        keys.add_request(reservations, request_identity);
                    }
                    keys.query_requests.insert(query_request.to_owned());
                }
                if let SourceNeutralOrderQueryResult::Found { order } = result {
                    keys.add_order(lifecycle, &order.order_identity.ledger_key());
                    keys.add_condition(&order.condition_id);
                }
            }
            LedgerControl::Cancellation { order_identity, .. } => {
                keys.add_order(lifecycle, &order_identity.ledger_key());
            }
            LedgerControl::OrdersQuery { .. } | LedgerControl::TradesQuery { .. } => {}
        },
        LedgerEvidence::OrderLifecycle { result } => {
            keys.add_order(lifecycle, &result.order_identity.ledger_key());
            keys.add_condition(&result.condition_id);
        }
        LedgerEvidence::TradeLifecycle { result } => {
            keys.add_trade(lifecycle, &result.trade_identity.ledger_key());
            keys.add_order(lifecycle, &result.order_identity.ledger_key());
            keys.add_condition(&result.condition_id);
        }
        _ => {}
    }
    keys
}

fn economic_transition_keys<I: LedgerIdentity>(
    reservations: &ReservationStateV1,
    lifecycle: &LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    transition: &SourceNeutralAccountTransitionKindV1<I>,
    delta: &ParsedBalanceDeltaV1,
    source_record: &SimV2SourceRecordReferenceV1,
) -> SparseLedgerStateKeysV1 {
    let mut keys = SparseLedgerStateKeysV1::default();
    if let Some(condition_id) = condition_id {
        keys.add_condition(condition_id);
    }
    match transition {
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity,
        } => {
            keys.add_order(lifecycle, &order_identity.ledger_key());
            if let Some(submission_item) = submission_item_identity(source_record) {
                if let Some(request_identity) = lifecycle.submission_requests.get(&submission_item)
                {
                    keys.add_request(reservations, request_identity);
                }
                keys.submission_items.insert(submission_item);
            }
            if let Some(request_identity) = lifecycle
                .orders
                .get(&order_identity.ledger_key())
                .and_then(|order| order.request_identity.as_ref())
            {
                keys.add_request(reservations, request_identity);
            }
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
        } => keys.add_order(lifecycle, &order_identity.ledger_key()),
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity,
            path_ordinal,
        } => keys.add_fill(lifecycle, &trade_identity.ledger_key(), *path_ordinal),
        SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity,
            ..
        } => {
            if let Some((identity, application_ordinal)) =
                active_applied_fill_key(lifecycle, &trade_identity.ledger_key())
            {
                keys.add_fill(lifecycle, &identity, application_ordinal);
            }
        }
        SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased { .. }
        | SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized
        | SourceNeutralAccountTransitionKindV1::TakerRebateAccrualApplied { .. }
        | SourceNeutralAccountTransitionKindV1::MakerRebateDayFinalized { .. }
        | SourceNeutralAccountTransitionKindV1::TakerRebateDayFinalized { .. }
        | SourceNeutralAccountTransitionKindV1::MakerRebatePaid { .. }
        | SourceNeutralAccountTransitionKindV1::TakerRebatePaid { .. }
        | SourceNeutralAccountTransitionKindV1::AutoRedeemApplied { .. } => {}
    }
    keys.outcome_tokens.extend(
        delta
            .outcome_balances
            .values()
            .map(|outcome| outcome.token_id.0.clone()),
    );
    keys
}

fn apply_control_lifecycle<I: LedgerIdentity>(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    source_record: &SimV2SourceRecordReferenceV1,
    result: &LedgerControl<I>,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    match result {
        LedgerControl::CancellationRequestCompleted { result } => match *result {},
        LedgerControl::Submission { outcome } => {
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
        LedgerControl::OrderQuery { result } => {
            let request_identity = query_global_request_id(source_record)
                .and_then(|identity| lifecycle.query_requests.get(identity).cloned());
            let Some(request_identity) = request_identity else {
                return Ok(());
            };
            if let SourceNeutralOrderQueryResult::Found { order } = result
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
        LedgerControl::Cancellation {
            order_identity,
            outcome: SourceNeutralCancellationOutcomeV1::Canceled,
        } => {
            set_order_open(
                lifecycle,
                &order_identity.ledger_key(),
                None,
                false,
                ledger_entry_seq,
            )?;
        }
        LedgerControl::Cancellation { .. }
        | LedgerControl::OrdersQuery { .. }
        | LedgerControl::TradesQuery { .. } => {}
    }
    Ok(())
}

fn accept_order_for_request<I: LedgerIdentity>(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    request_identity: &LedgerRequestIdentityV1,
    order: &SourceNeutralOrderSnapshotV1<I>,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let mut order_index = lifecycle
        .orders
        .remove(&order.order_identity.ledger_key())
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
        .insert(order.order_identity.ledger_key().clone(), order_index);
    set_order_open(
        lifecycle,
        &order.order_identity.ledger_key(),
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
    order_identity: &LedgerIdentityKeyV1,
    condition_id: Option<&ConditionId>,
    open: bool,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let mut order = lifecycle.orders.remove(order_identity).unwrap_or_default();
    let next_condition = condition_id.cloned().or_else(|| order.condition_id.clone());
    let open = open
        && !order.closed
        && order
            .submission_open_base_quantity_shares
            .is_none_or(|initial| initial > order.maker_matched_base_quantity_shares);
    order.closed |= !open;
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

fn apply_order_lifecycle<I: LedgerIdentity>(
    lifecycle: &mut LifecycleStateV1,
    result: &SourceNeutralOrderLifecycleResultV1<I>,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    match &result.event {
        SourceNeutralOrderLifecycleEventV1::Accepted { state } => set_order_open(
            lifecycle,
            &result.order_identity.ledger_key(),
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
            &result.order_identity.ledger_key(),
            Some(&result.condition_id),
            false,
            ledger_entry_seq,
        ),
    }
}

fn apply_lifecycle_quantity<I: LedgerIdentity>(
    lifecycle: &mut LifecycleStateV1,
    evidence: &LedgerEvidence<I>,
    quantity: LifecycleQuantityV1,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let (order_identity, condition_id, open) = match (evidence, quantity) {
        (
            LedgerEvidence::Control {
                result:
                    LedgerControl::Submission {
                        outcome: SourceNeutralSubmissionOutcomeV1::Accepted { order },
                    },
            },
            LifecycleQuantityV1::SubmissionOpen(open),
        ) => {
            let initial = parse_decimal(&open, "lifecycle.submission_open_quantity")?;
            let index = lifecycle
                .orders
                .entry(order.order_identity.ledger_key())
                .or_default();
            // 原提交计量固定不变，后续查询快照不替代此数量基点。
            index
                .submission_open_base_quantity_shares
                .get_or_insert(initial);
            (
                &order.order_identity,
                &order.condition_id,
                matches!(order.state, SourceNeutralOrderStateV1::Open),
            )
        }
        (LedgerEvidence::TradeLifecycle { result }, LifecycleQuantityV1::MakerMatch(quantity)) => {
            let trade = lifecycle
                .trades
                .get_mut(&result.trade_identity.ledger_key())
                .expect("同一候选已归约成交生命周期");
            if trade.open_quantity_counted {
                return Ok(());
            }
            let quantity = parse_decimal(&quantity, "lifecycle.maker_match_quantity")?;
            trade.open_quantity_counted = true;
            let order = lifecycle
                .orders
                .entry(result.order_identity.ledger_key())
                .or_default();
            order.maker_matched_base_quantity_shares = checked_add(
                order.maker_matched_base_quantity_shares,
                quantity,
                "lifecycle.maker_match_quantity",
            )?;
            // 提交响应到达前只保留已耗减数量，不制造初始余量或关闭未知订单。
            if order.submission_open_base_quantity_shares.is_none() {
                return Ok(());
            }
            (&result.order_identity, &result.condition_id, order.open)
        }
        _ => unreachable!("数量适配与原生命周期种类一致"),
    };
    set_order_open(
        lifecycle,
        &order_identity.ledger_key(),
        Some(condition_id),
        open,
        ledger_entry_seq,
    )
}

fn apply_trade_lifecycle<I: LedgerIdentity>(
    lifecycle: &mut LifecycleStateV1,
    result: &SourceNeutralTradeLifecycleResultV1<I>,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let pending = matches!(
        result.economic_disposition,
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied
            | SourceNeutralTradeEconomicDispositionV1::PendingApplied
    );
    set_trade_pending(
        lifecycle,
        &result.trade_identity.ledger_key(),
        &result.condition_id,
        pending,
        ledger_entry_seq,
    )
}

fn set_trade_pending(
    lifecycle: &mut LifecycleStateV1,
    trade_identity: &LedgerIdentityKeyV1,
    condition_id: &ConditionId,
    pending: bool,
    ledger_entry_seq: u64,
) -> Result<(), LedgerErrorV1> {
    let previous = lifecycle.trades.remove(trade_identity);
    // 已有非 pending 成交就是终态；较早观察仍消费，但不得回开计数。
    let pending = pending && previous.as_ref().is_none_or(|previous| previous.pending);
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
            open_quantity_counted: previous
                .as_ref()
                .is_some_and(|index| index.open_quantity_counted),
            _last_ledger_entry_seq: ledger_entry_seq,
        },
    );
    Ok(())
}

fn apply_economic_lifecycle<I: LedgerIdentity>(
    reservations: &mut ReservationStateV1,
    lifecycle: &mut LifecycleStateV1,
    condition_id: Option<&ConditionId>,
    transition: &SourceNeutralAccountTransitionKindV1<I>,
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
                .remove(&order_identity.ledger_key())
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
            lifecycle
                .orders
                .insert(order_identity.ledger_key().clone(), order);
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
                &order_identity.ledger_key(),
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
                (trade_identity.ledger_key().clone(), *path_ordinal),
                AppliedFillIndexV1 {
                    condition_id: condition_id.cloned(),
                    token_id,
                    cost_basis_pusd,
                    fee_pusd,
                    active: true,
                },
            );
        }
        SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased { .. } => {}
        SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity,
            ..
        } => {
            if let Some(fill_key) = active_applied_fill_key(lifecycle, &trade_identity.ledger_key())
                && let Some(fill) = lifecycle.applied_fills.get(&fill_key).cloned()
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
        .outcome_cost_basis_pusd
        .get(&token_id.0)
        .map(|cost| cost.cost_basis_pusd)
        .unwrap_or(Decimal::ZERO);
    lifecycle.outcome_cost_basis_pusd.insert(
        token_id.0.clone(),
        OutcomeCostBasisV1 {
            token_id: token_id.clone(),
            cost_basis_pusd: checked_add(current, delta, "lifecycle.outcome_cost_basis")?,
        },
    );
    if let Some(condition_id) = condition_id {
        let market = lifecycle.market_mut(condition_id);
        let current = market
            .outcome_cost_basis_pusd
            .get(&token_id.0)
            .map(|cost| cost.cost_basis_pusd)
            .unwrap_or(Decimal::ZERO);
        market.outcome_cost_basis_pusd.insert(
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
    let mut market_position_cost_delta = Decimal::ZERO;
    let mut market_fee_delta = Decimal::ZERO;
    let mut market_pnl_delta = Decimal::ZERO;
    for condition_id in lifecycle_markets {
        let before_market = before_lifecycle.markets.get(condition_id);
        let after_market = after_lifecycle.markets.get(condition_id);
        let before = before_market.map(|market| &market.totals);
        let after = after_market.map(|market| &market.totals);
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

        let before_outcomes = before_market.map(|market| &market.outcome_cost_basis_pusd);
        let after_outcomes = after_market.map(|market| &market.outcome_cost_basis_pusd);
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
                after_market
                    .and_then(|market| market.outcome_cost_basis_pusd.get(token_id))
                    .map(|cost| cost.cost_basis_pusd)
                    .unwrap_or(Decimal::ZERO),
                before_market
                    .and_then(|market| market.outcome_cost_basis_pusd.get(token_id))
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
    fn commit(self, current: &mut BalanceStateV1) {
        current.cash_pusd = self.cash_pusd;
        current.available_pusd = self.available_pusd;
        current.principal_encumbrance_pusd = self.principal_encumbrance_pusd;
        current.immediate_taker_fee_encumbrance_pusd = self.immediate_taker_fee_encumbrance_pusd;
        current
            .outcome_balances
            .extend(self.outcome_balance_updates);
        current.maker_rebate_receivable_pusd = self.maker_rebate_receivable_pusd;
        current.taker_rebate_receivable_pusd = self.taker_rebate_receivable_pusd;
        current.maker_rebate_locked_remainder_pusd = self.maker_rebate_locked_remainder_pusd;
        current.taker_rebate_locked_remainder_pusd = self.taker_rebate_locked_remainder_pusd;
    }
}

fn prepare_balance_update(
    current: &BalanceStateV1,
    delta: &ParsedBalanceDeltaV1,
    scope: &'static str,
) -> Result<BalanceUpdateV1, LedgerErrorV1> {
    prepare_scoped_balance_update(current, delta, delta.cash_pusd, delta.available_pusd, scope)
}

fn prepare_scoped_balance_update(
    current: &BalanceStateV1,
    delta: &ParsedBalanceDeltaV1,
    cash_delta: Decimal,
    available_delta: Decimal,
    scope: &'static str,
) -> Result<BalanceUpdateV1, LedgerErrorV1> {
    let mut outcome_balance_updates = BTreeMap::new();
    for (key, outcome_delta) in &delta.outcome_balances {
        let existing = current
            .outcome_balances
            .get(key)
            .map(|outcome| outcome.balance)
            .unwrap_or(Decimal::ZERO);
        outcome_balance_updates.insert(
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
        cash_pusd: checked_add(current.cash_pusd, cash_delta, scope)?,
        available_pusd: checked_add(current.available_pusd, available_delta, scope)?,
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
        outcome_balance_updates,
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
    prepare_scoped_balance_update(current, delta, Decimal::ZERO, Decimal::ZERO, "market")
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
    outcome_cost_basis_pusd: &BTreeMap<String, OutcomeCostBasisV1>,
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
            let cost_basis = outcome_cost_basis_pusd.get(&outcome.token_id.0);
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
    }

    #[test]
    fn sparse_balance_updates_preserve_untouched_outcome_tokens() {
        const TOKEN_A: &str = "token-a";
        const TOKEN_B: &str = "token-b";
        let mut balances = BalanceStateV1::opened("10".parse().unwrap());
        balances.outcome_balances.insert(
            TOKEN_A.to_owned(),
            OutcomeBalanceStateV1 {
                token_id: TokenId(TOKEN_A.to_owned()),
                balance: "3".parse().unwrap(),
            },
        );
        let token_b_delta = ParsedBalanceDeltaV1 {
            cash_pusd: Decimal::ZERO,
            available_pusd: "-1".parse().unwrap(),
            principal_encumbrance_pusd: Decimal::ZERO,
            immediate_taker_fee_encumbrance_pusd: Decimal::ZERO,
            outcome_balances: BTreeMap::from([(
                TOKEN_B.to_owned(),
                OutcomeBalanceStateV1 {
                    token_id: TokenId(TOKEN_B.to_owned()),
                    balance: "2".parse().unwrap(),
                },
            )]),
            maker_rebate_receivable_pusd: Decimal::ZERO,
            taker_rebate_receivable_pusd: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd: Decimal::ZERO,
        };

        let token_b_update = prepare_balance_update(&balances, &token_b_delta, "test").unwrap();
        assert_eq!(
            token_b_update
                .outcome_balance_updates
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec![TOKEN_B]
        );
        token_b_update.commit(&mut balances);
        assert_eq!(balances.outcome_balances.len(), 2);
        assert_eq!(
            balances.outcome_balances[TOKEN_A].balance,
            "3".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            balances.outcome_balances[TOKEN_B].balance,
            "2".parse::<Decimal>().unwrap()
        );
        assert_eq!(balances.available_pusd, "9".parse::<Decimal>().unwrap());

        let scalar_only_delta = ParsedBalanceDeltaV1 {
            cash_pusd: "1".parse().unwrap(),
            available_pusd: Decimal::ZERO,
            principal_encumbrance_pusd: Decimal::ZERO,
            immediate_taker_fee_encumbrance_pusd: Decimal::ZERO,
            outcome_balances: BTreeMap::new(),
            maker_rebate_receivable_pusd: Decimal::ZERO,
            taker_rebate_receivable_pusd: Decimal::ZERO,
            maker_rebate_locked_remainder_pusd: Decimal::ZERO,
            taker_rebate_locked_remainder_pusd: Decimal::ZERO,
        };
        let scalar_update = prepare_balance_update(&balances, &scalar_only_delta, "test").unwrap();
        assert!(scalar_update.outcome_balance_updates.is_empty());
        scalar_update.commit(&mut balances);
        assert_eq!(balances.outcome_balances.len(), 2);
        assert_eq!(
            balances.outcome_balances[TOKEN_A].balance,
            "3".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            balances.outcome_balances[TOKEN_B].balance,
            "2".parse::<Decimal>().unwrap()
        );
        assert_eq!(balances.cash_pusd, "11".parse::<Decimal>().unwrap());
        assert_eq!(balances.available_pusd, "9".parse::<Decimal>().unwrap());
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

        let released = local_reservation_entries(
            &condition_id,
            "-123.45678901".parse().unwrap(),
            "-0.10000000".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(
            released,
            vec![
                (
                    LedgerAccountV1::LocalPendingPrincipal(CONDITION.to_owned()),
                    LedgerAssetV1::Pusd,
                    "-123.45678901".parse().unwrap()
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    PRINCIPAL.parse().unwrap()
                ),
                (
                    LedgerAccountV1::LocalPendingImmediateFee(CONDITION.to_owned()),
                    LedgerAssetV1::Pusd,
                    "-0.10000000".parse().unwrap()
                ),
                (
                    LedgerAccountV1::Balancing,
                    LedgerAssetV1::Pusd,
                    FEE.parse().unwrap()
                ),
            ]
        );
    }

    #[test]
    fn failed_durable_append_does_not_advance_memory_or_recovery() {
        let temporary = tempfile::TempDir::new().unwrap();
        let policy = AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal("100".to_owned()),
        };
        let reference = NoeResultReferenceV1 {
            commit_container:
                contracts::execution_normalization::NoePublishedContainerReferenceV1 {
                    location: "deleted-noe-container.json".to_owned(),
                },
            record_ordinal: 0,
        };
        let account_instance_key = AccountInstanceKeyV1 {
            run_id: "durable-failure".to_owned(),
            caller_role: contracts::SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-durable-failure".to_owned(),
            account_id: "account-durable-failure".to_owned(),
            platform_source: contracts::PlatformSourceV1::Simulated,
        };
        let mut writer = LedgerWriterV1::create_durable(policy, temporary.path()).unwrap();
        writer.inject_durable_append_failure();
        assert!(matches!(
            writer.apply_opening(
                &LedgerTransactionSourceV1::Noe(reference.clone()),
                &account_instance_key,
                "opening-durable-failure",
                Some(1),
                &ExactDecimal("10".to_owned()),
            ),
            Err(LedgerErrorV1::Durable(LedgerDurableErrorV1::Io { .. }))
        ));
        assert_eq!(writer.projection(), None);

        writer.inject_durable_sync_failure();
        assert!(matches!(
            writer.apply_opening(
                &LedgerTransactionSourceV1::Noe(reference.clone()),
                &account_instance_key,
                "opening-durable-failure",
                Some(1),
                &ExactDecimal("10".to_owned()),
            ),
            Err(LedgerErrorV1::Durable(LedgerDurableErrorV1::Io { .. }))
        ));
        assert_eq!(writer.projection(), None);
        drop(writer);

        let recovered = LedgerWriterV1::open_durable(temporary.path()).unwrap();
        assert_eq!(recovered.projection(), None);
        assert_eq!(
            recovered
                .admission_policy()
                .per_market_all_in_commitment_limit_pusd,
            ExactDecimal("100".to_owned())
        );
    }

    #[test]
    fn failed_durable_capacity_decision_is_not_cached_or_returned() {
        let temporary = tempfile::TempDir::new().unwrap();
        let policy = AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal("100".to_owned()),
        };
        let reference = NoeResultReferenceV1 {
            commit_container:
                contracts::execution_normalization::NoePublishedContainerReferenceV1 {
                    location: "capacity-opening.json".to_owned(),
                },
            record_ordinal: 0,
        };
        let account_instance_key = AccountInstanceKeyV1 {
            run_id: "durable-capacity".to_owned(),
            caller_role: contracts::SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-durable-capacity".to_owned(),
            account_id: "account-durable-capacity".to_owned(),
            platform_source: contracts::PlatformSourceV1::Simulated,
        };
        let request_identity = LedgerRequestIdentityV1("rejected-write-failure".to_owned());
        let mut writer = LedgerWriterV1::create_durable(policy, temporary.path()).unwrap();
        writer
            .apply_opening(
                &LedgerTransactionSourceV1::Noe(reference.clone()),
                &account_instance_key,
                "opening-durable-capacity",
                Some(1),
                &ExactDecimal("10".to_owned()),
            )
            .unwrap();
        writer.inject_durable_append_failure();
        assert!(matches!(
            writer.try_reserve_intent(
                &request_identity,
                &ConditionId("condition-capacity".to_owned()),
                &ExactDecimal("20".to_owned()),
                &ExactDecimal("0".to_owned()),
            ),
            Err(LedgerErrorV1::Durable(LedgerDurableErrorV1::Io { .. }))
        ));
        assert!(!writer.reservations.requests.contains_key(&request_identity));
        assert_eq!(writer.projection().unwrap().ledger_entry_seq, 1);
        drop(writer);

        let recovered = LedgerWriterV1::open_durable(temporary.path()).unwrap();
        assert!(
            !recovered
                .reservations
                .requests
                .contains_key(&request_identity)
        );
        assert_eq!(recovered.projection().unwrap().ledger_entry_seq, 1);
    }
}

#[path = "memory.rs"]
pub(crate) mod memory;

#[path = "funds.rs"]
pub(crate) mod funds;

#[path = "economics.rs"]
pub(crate) mod economics;
