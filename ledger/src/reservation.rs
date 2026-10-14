use std::collections::{BTreeMap, BTreeSet};

use contracts::{ConditionId, ExactDecimal};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LedgerRequestIdentityV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LedgerLocalEventIdentityV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReserveIntentV1 {
    pub request_identity: LedgerRequestIdentityV1,
    pub condition_id: ConditionId,
    pub requested_principal_pusd: ExactDecimal,
    pub immediate_fee_reserve_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerReservationOutcomeV1 {
    Reserved {
        ledger_entry_seq: u64,
    },
    InsufficientGlobalAvailableFunds,
    /// 旧双现金耐久决定的解释值；当前准入不产生此结果。
    InsufficientMakerAvailableFunds,
    PerMarketLimitExceeded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationStatusV1 {
    ReservedBeforeSend,
    SubmittedUnknown,
    PlatformConfirmedPendingEconomicFact,
    Converted,
    Released,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LocalReservationV1 {
    pub(crate) condition_id: ConditionId,
    pub(crate) principal_pusd: Decimal,
    pub(crate) immediate_fee_pusd: Decimal,
    pub(crate) status: ReservationStatusV1,
    pub(crate) last_applied_ledger_entry_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CachedReservationRequestV1 {
    pub(crate) outcome: LedgerReservationOutcomeV1,
    pub(crate) reservation: Option<LocalReservationV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MarketLocalPendingV1 {
    pub(crate) condition_id: ConditionId,
    pub(crate) principal_pusd: Decimal,
    pub(crate) immediate_fee_pusd: Decimal,
    pub(crate) before_send_principal_pusd: Decimal,
    pub(crate) before_send_immediate_fee_pusd: Decimal,
    pub(crate) before_send_count: u64,
    pub(crate) after_send_unknown_principal_pusd: Decimal,
    pub(crate) after_send_unknown_immediate_fee_pusd: Decimal,
    pub(crate) after_send_unknown_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ReservationStateV1 {
    pub(crate) requests: BTreeMap<LedgerRequestIdentityV1, CachedReservationRequestV1>,
    #[serde(default)]
    pub(crate) active_requests: Option<BTreeSet<LedgerRequestIdentityV1>>,
    pub(crate) applied_local_events: BTreeMap<LedgerLocalEventIdentityV1, u64>,
    pub(crate) principal_pusd: Decimal,
    pub(crate) immediate_fee_pusd: Decimal,
    pub(crate) before_send_principal_pusd: Decimal,
    pub(crate) before_send_immediate_fee_pusd: Decimal,
    pub(crate) before_send_count: u64,
    pub(crate) after_send_unknown_principal_pusd: Decimal,
    pub(crate) after_send_unknown_immediate_fee_pusd: Decimal,
    pub(crate) after_send_unknown_count: u64,
    pub(crate) markets: BTreeMap<String, MarketLocalPendingV1>,
}

impl Default for ReservationStateV1 {
    fn default() -> Self {
        Self {
            requests: BTreeMap::new(),
            active_requests: Some(BTreeSet::new()),
            applied_local_events: BTreeMap::new(),
            principal_pusd: Decimal::ZERO,
            immediate_fee_pusd: Decimal::ZERO,
            before_send_principal_pusd: Decimal::ZERO,
            before_send_immediate_fee_pusd: Decimal::ZERO,
            before_send_count: 0,
            after_send_unknown_principal_pusd: Decimal::ZERO,
            after_send_unknown_immediate_fee_pusd: Decimal::ZERO,
            after_send_unknown_count: 0,
            markets: BTreeMap::new(),
        }
    }
}

impl ReservationStateV1 {
    pub(crate) fn insert_request(
        &mut self,
        id: LedgerRequestIdentityV1,
        request: CachedReservationRequestV1,
    ) {
        if let Some(active) = &mut self.active_requests {
            if request.reservation.as_ref().is_some_and(|r| {
                !matches!(
                    r.status,
                    ReservationStatusV1::Converted | ReservationStatusV1::Released
                )
            }) {
                active.insert(id.clone());
            } else {
                active.remove(&id);
            }
        }
        self.requests.insert(id, request);
    }
}
