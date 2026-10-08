#[cfg(feature = "measurement")]
use crate::matching::{order_projection_owned_bytes_v2, trade_projection_owned_bytes_v2};
use crate::public_consumption::*;
use crate::session::PublicVisibilityCursorV1;
pub use noe_projection::{
    CancellationEvidenceProjection, MeasuredCancellationEvidence, MeasuredOrderEvidence,
    MemoryNoeObservation, MemoryNoeObservationV4, MemoryNoeProjectionPort,
    MemoryNoeProjectionPortV4, MemoryOrderEvidence, SimV2NoeProjectionPortV4,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "measurement")]
use std::time::Instant;

use contracts::{
    AccountInstanceKeyV1, AccountOpeningFactV2, CancelInputItemRefV2, CancelNotCanceledReasonV2,
    ConditionId, DirectResponseMetadataV2, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    ExecutionMethodV2, MarketEnrollmentV2, OrderDirectResponseV2, OrderLifecycleFeedbackV2,
    OrderProjectionV2, OrderQueryResultV2, OrdersDirectResponseV2, OrdersPageV2,
    OrdersQueryResultV2, PlatformSourceV1, PostOrderDirectResponseV2, PostOrdersDirectResponseV2,
    QueryCursorV2, QueryOutOfRetentionV2, SettlementLifecycleFeedbackV2, Sha256Hex,
    SimCallerRoleV1, SimulatedOrderKnownStatusV2, SubmissionEvidenceUnknownReasonV2,
    SubmissionItemOutcomeV2, SubmissionRejectedReasonV2, SubmissionResponseItemV2,
    TradeLifecycleFeedbackV2, TradeProjectionV2, TradesDirectResponseV2, TradesPageV2,
    TradesQueryResultV2,
};

use crate::account::{PlatformAccountError, PlatformAccountStateV2};
use crate::event_log::{
    BackgroundEventLogV1, SimEventLogErrorV2, SimEventLogRecoveryV2, SimEventLogV2,
};
use crate::execution::{
    CancelOrderItemV1, OrdersFilterV1, PostOrderItemV1, SIM_QUERY_PAGE_SIZE_V1, SimBuyOrderKindV1,
    TradesFilterV1, limit_buy_encoding, market_buy_encoding, normalize_gtd_expiration,
    strict_millisecond_interval,
};
use crate::finality::TradeFinalityPolicyV2;
use crate::latency_profile::{DelaySampleDomainV1, EmpiricalDelayProfile};
use crate::market::{
    MarketBuyAdmissionV2, MarketBuyEvidenceUnknownV2, MarketBuyRejectionV2, MarketEventErrorV1,
    MarketEventPathV1, MarketStateStoreV2, market_path_drives_execution_for,
    normalize_market_event_v2,
};
use crate::market_directory::{SessionMarketDirectoryErrorV1, SessionMarketDirectoryV1};
use crate::matching::{
    BuyOrderActionV2, BuyOrderDispositionV2, BuyOrderRejectedReasonV2, BuyOrderTermsV2,
    CallerCancelDispositionV2, ExecutionMarketEventEffectsV2, ExecutionStateV2,
    LifecycleFeedbackEffectV2, MatchingStateErrorV1, micro_units_v1,
};
#[cfg(feature = "measurement")]
use crate::measurement_probe::{
    MeasurementContext, MeasurementObservation, MeasurementPhase, MeasurementProbe,
    MeasurementResourceSnapshot,
};
use crate::platform_rules::{PlatformRuleSnapshotV3, SealedProcessRuleSnapshotV3};
#[cfg(feature = "measurement")]
use crate::resource::ResourceQuantityV1;
use crate::runtime_v2::{
    AcceptedClosedMarketBucketV2, BusinessInputV2, BusinessRequestV2,
    DurableMarketBucketReferenceV2, LoggedBusinessInputV2, PlatformPrefixAdvanceV2,
    RetentionAdvanceV2, RunDurabilityRoleV2, RunOpenedV2, SessionSpecV3, TrustedDecisionCutoffV2,
};
#[cfg(any(feature = "measurement", test))]
use crate::scenario::PlatformMarketV1;
use crate::scenario::SealedPlatformScenarioV1;
use crate::session::MarketEnrollmentRequestV1;

pub(crate) mod noe_projection;
pub use noe_projection::{
    AcceptedNoeProjectionV1, MemoryNoeBatch, MemoryNoeErrorV1, MemoryNoeFeedback,
    MemoryNoeObservationV2, MemoryNoeObservationV3, MemoryNoeProjectionPortV2,
    MemoryNoeProjectionPortV3, MemoryNoeRecord,
};
pub use noe_projection::{
    SimV2NoeObservationV1, SimV2NoeProjectionPortV1, SimV2NoeProjectionPortV2,
    SimV2NoeProjectionPortV3,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateSessionDispositionV2 {
    NewPrepared,
    ExistingPrepared,
}

#[derive(Debug)]
pub struct CreateSessionControlResultV2 {
    pub disposition: CreateSessionDispositionV2,
    pub opening_fact: Arc<AccountOpeningFactV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionInitializationControlResultV2 {
    Activated,
    ExistingActive,
    FailedClosed,
    ExistingFailedClosed,
}

#[derive(Debug, PartialEq, Eq)]
enum SessionInitializationV2 {
    PreparedLedgerInit,
    Active { activation_id: String },
    InitializationFailedClosed { failure_id: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MutationActionKeyV2 {
    effective_processing_at_utc_ms: i64,
    input_sequence: u64,
    batch_item_ordinal: u16,
}

#[derive(Debug)]
enum MutationActionV2 {
    PostItem {
        global_request_id: String,
        item: PostOrderItemV1,
        normalized_gtd_expires_at_utc_ms: Option<Option<i64>>,
    },
    CancelTarget {
        global_request_id: String,
        target_ordinal: u64,
        platform_order_id: String,
        input_item_refs: Vec<CancelInputItemRefV2>,
    },
    CancelMarket {
        global_request_id: String,
        condition_id: ConditionId,
    },
    CancelAll {
        global_request_id: String,
    },
    OrderQuery {
        global_request_id: String,
        platform_order_id: String,
    },
    OrdersQuery {
        global_request_id: String,
        filter: OrdersFilterV1,
        cursor: Option<QueryCursorV2>,
    },
    TradesQuery {
        global_request_id: String,
        filter: TradesFilterV1,
        cursor: Option<QueryCursorV2>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MutationMethodV2 {
    PostOrder,
    PostOrders,
    CancelOrder,
    CancelOrders,
    CancelMarketOrders,
    CancelAllOrders,
    Order,
    Orders,
    Trades,
}

#[derive(Debug)]
struct CancelTargetRegistrationV2 {
    target_ordinal: u64,
    action_ordinal: u16,
    platform_order_id: String,
    input_item_refs: Vec<CancelInputItemRefV2>,
}

enum NewMutationItemsV2 {
    Post {
        method: MutationMethodV2,
        items: Vec<(u16, PostOrderItemV1)>,
    },
    Cancel {
        method: MutationMethodV2,
        targets: Vec<CancelTargetRegistrationV2>,
    },
    CancelMarket {
        condition_id: ConditionId,
    },
    CancelAll,
    OrderQuery {
        platform_order_id: String,
    },
    OrdersQuery {
        filter: OrdersFilterV1,
        cursor: Option<QueryCursorV2>,
    },
    TradesQuery {
        filter: TradesFilterV1,
        cursor: Option<QueryCursorV2>,
    },
}

type CancelTargetBusinessEffectV3 =
    CancelTargetBusinessEffect<contracts::CancelNotCanceledReasonV3>;
type MutationActionBusinessEffectV3 =
    MutationActionBusinessEffect<contracts::CancelNotCanceledReasonV3>;
type DirectResponsePayloadDraftV3 =
    DirectResponsePayloadDraft<contracts::CancelNotCanceledReasonV3>;
type DirectResponseDraftV3 = DirectResponseDraft<contracts::CancelNotCanceledReasonV3>;
type StoredDirectResponseV3 = StoredDirectResponse<contracts::CancelNotCanceledReasonV3>;
pub type BusinessDirectResponseV2 = BusinessDirectResponse<CancelNotCanceledReasonV2>;
pub type BusinessDirectResponseV3 = BusinessDirectResponse<contracts::CancelNotCanceledReasonV3>;
type CancelTargetBusinessEffect<R> = contracts::CancelTargetResponse<R>;

#[derive(Debug, PartialEq, Eq)]
enum MutationActionBusinessEffect<R> {
    Post(Arc<PostOrderItemBusinessEffectV2>),
    Cancel(Vec<Arc<CancelTargetBusinessEffect<R>>>),
    Query(Arc<QueryActionBusinessEffectV2>),
}

#[derive(Debug, PartialEq, Eq)]
enum QueryActionResultV2 {
    Order(OrderQueryResultV2),
    Orders(OrdersQueryResultV2),
    Trades(TradesQueryResultV2),
}

#[derive(Debug, PartialEq, Eq)]
struct QueryActionBusinessEffectV2 {
    effective_processing_at_utc_ms: i64,
    result: QueryActionResultV2,
}

#[derive(Debug, PartialEq, Eq)]
enum DirectResponsePayloadDraft<R> {
    PostOrder(Arc<PostOrderItemBusinessEffectV2>),
    PostOrders(Vec<Arc<PostOrderItemBusinessEffectV2>>),
    CancelOrder(Arc<CancelTargetBusinessEffect<R>>),
    CancelOrders(Vec<Arc<CancelTargetBusinessEffect<R>>>),
    CancelMarketOrders(Vec<Arc<CancelTargetBusinessEffect<R>>>),
    CancelAllOrders(Vec<Arc<CancelTargetBusinessEffect<R>>>),
    Order(Arc<QueryActionBusinessEffectV2>),
    Orders(Arc<QueryActionBusinessEffectV2>),
    Trades(Arc<QueryActionBusinessEffectV2>),
}

#[derive(Debug, PartialEq, Eq)]
struct DirectResponseDraft<R> {
    global_request_id: String,
    caller_correlation_id: Option<String>,
    method: MutationMethodV2,
    platform_responded_at_utc_ms: i64,
    response_received_at_utc_ms: i64,
    payload: DirectResponsePayloadDraft<R>,
}

#[derive(Debug, PartialEq, Eq)]
enum StoredDirectResponse<R> {
    Draft(DirectResponseDraft<R>),
    Final(BusinessDirectResponse<R>),
}

#[cfg(test)]
impl<R> std::ops::Deref for StoredDirectResponse<R> {
    type Target = DirectResponseDraft<R>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Draft(response) => response,
            Self::Final(_) => panic!("最终直接响应不再暴露内部草稿"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BusinessDirectResponse<R> {
    PostOrder(PostOrderDirectResponseV2),
    PostOrders(PostOrdersDirectResponseV2),
    CancelOrder(contracts::CancelOrderDirectResponse<R>),
    CancelOrders(contracts::CancelOrdersDirectResponse<R>),
    CancelMarketOrders(contracts::CancelMarketOrdersDirectResponse<R>),
    CancelAllOrders(contracts::CancelAllOrdersDirectResponse<R>),
    Order(OrderDirectResponseV2),
    Orders(OrdersDirectResponseV2),
    Trades(TradesDirectResponseV2),
}

#[derive(Debug, PartialEq, Eq)]
pub struct BusinessProjectionBatch<T, R = CancelNotCanceledReasonV2> {
    pub initial_markets: Vec<contracts::economics::SourceNeutralInitialMarketV1>,
    pub run_id: String,
    pub projection_batch_sequence: u64,
    pub account_opening: Option<Arc<AccountOpeningFactV2>>,
    pub market_enrollment: Option<MarketEnrollmentV2>,
    direct_responses: Vec<Arc<StoredDirectResponse<R>>>,
    pub order_lifecycle: Vec<OrderLifecycleFeedbackV2>,
    pub trade_lifecycle: Vec<T>,
    pub settlement_lifecycle: Vec<SettlementLifecycleFeedbackV2>,
}

impl<T, R> BusinessProjectionBatch<T, R> {
    pub fn direct_responses(&self) -> impl Iterator<Item = &BusinessDirectResponse<R>> {
        self.direct_responses
            .iter()
            .filter_map(|response| match response.as_ref() {
                StoredDirectResponse::Final(response) => Some(response),
                StoredDirectResponse::Draft(_) => None,
            })
    }
}

pub type BusinessProjectionBatchV2 = BusinessProjectionBatch<TradeLifecycleFeedbackV2>;
pub type BusinessProjectionBatchV3 = BusinessProjectionBatch<contracts::TradeLifecycleFeedbackV3>;
pub type BusinessProjectionBatchV4 = BusinessProjectionBatch<
    contracts::TradeLifecycleFeedbackV3,
    contracts::CancelNotCanceledReasonV3,
>;
#[derive(Clone)]
pub enum BusinessProjectionBatchPayloadV1 {
    V2(Arc<BusinessProjectionBatchV2>),
    V3(Arc<BusinessProjectionBatchV3>),
    V4(Arc<BusinessProjectionBatchV4>),
}
impl BusinessProjectionBatchPayloadV1 {
    fn sequence(&self) -> u64 {
        match self {
            Self::V2(b) => b.projection_batch_sequence,
            Self::V3(b) => b.projection_batch_sequence,
            Self::V4(b) => b.projection_batch_sequence,
        }
    }
}
impl BusinessProjectionBatchV4 {
    fn into_payload(
        self,
        version: crate::runtime_v2::BusinessProjectionVersionV1,
    ) -> BusinessProjectionBatchPayloadV1 {
        match version {
            crate::runtime_v2::BusinessProjectionVersionV1::V4 => {
                BusinessProjectionBatchPayloadV1::V4(Arc::new(self))
            }
            crate::runtime_v2::BusinessProjectionVersionV1::V3 => {
                BusinessProjectionBatchPayloadV1::V3(Arc::new(BusinessProjectionBatchV3 {
                    initial_markets: self.initial_markets,
                    run_id: self.run_id,
                    projection_batch_sequence: self.projection_batch_sequence,
                    account_opening: self.account_opening,
                    market_enrollment: self.market_enrollment,
                    direct_responses: legacy_direct_responses(self.direct_responses),
                    order_lifecycle: self.order_lifecycle,
                    trade_lifecycle: self.trade_lifecycle,
                    settlement_lifecycle: self.settlement_lifecycle,
                }))
            }
            crate::runtime_v2::BusinessProjectionVersionV1::V2 => {
                BusinessProjectionBatchPayloadV1::V2(Arc::new(BusinessProjectionBatchV2 {
                    initial_markets: self.initial_markets,
                    run_id: self.run_id,
                    projection_batch_sequence: self.projection_batch_sequence,
                    account_opening: self.account_opening,
                    market_enrollment: self.market_enrollment,
                    direct_responses: legacy_direct_responses(self.direct_responses),
                    order_lifecycle: self.order_lifecycle,
                    trade_lifecycle: self
                        .trade_lifecycle
                        .into_iter()
                        .map(contracts::TradeLifecycleFeedbackV3::into_v2)
                        .collect(),
                    settlement_lifecycle: self.settlement_lifecycle,
                }))
            }
        }
    }
}
// 旧根不能装载撤单场景；这里只编码旧根原有的两类原因。
fn legacy_direct_responses(
    responses: Vec<Arc<StoredDirectResponseV3>>,
) -> Vec<Arc<StoredDirectResponse<CancelNotCanceledReasonV2>>> {
    fn target(source: &contracts::CancelTargetResponseV3) -> contracts::CancelTargetResponseV2 {
        contracts::CancelTargetResponse {
            target_ordinal: source.target_ordinal,
            platform_order_id: source.platform_order_id.clone(),
            input_item_refs: source.input_item_refs.clone(),
            effective_processing_at_utc_ms: source.effective_processing_at_utc_ms,
            outcome: match &source.outcome {
                contracts::CancelTargetOutcome::Canceled(value) => {
                    contracts::CancelTargetOutcome::Canceled(value.clone())
                }
                contracts::CancelTargetOutcome::EvidenceUnknownNoEffect => {
                    contracts::CancelTargetOutcome::EvidenceUnknownNoEffect
                }
                contracts::CancelTargetOutcome::NotCanceledNoEffect(reason) => {
                    contracts::CancelTargetOutcome::NotCanceledNoEffect(match reason {
                        contracts::CancelNotCanceledReasonV3::NotFoundOrNotOwned => {
                            CancelNotCanceledReasonV2::NotFoundOrNotOwned
                        }
                        contracts::CancelNotCanceledReasonV3::NoCancellableRemainder => {
                            CancelNotCanceledReasonV2::NoCancellableRemainder
                        }
                        contracts::CancelNotCanceledReasonV3::TemporaryFailureNoEffect => {
                            unreachable!("旧根没有撤单场景输入")
                        }
                    })
                }
            },
        }
    }
    responses
        .into_iter()
        .map(|source| {
            let StoredDirectResponse::Final(response) = source.as_ref() else {
                unreachable!("待交付投影批次只能持有最终直接响应")
            };
            let response = match response {
                BusinessDirectResponse::PostOrder(r) => {
                    BusinessDirectResponse::PostOrder(r.clone())
                }
                BusinessDirectResponse::PostOrders(r) => {
                    BusinessDirectResponse::PostOrders(r.clone())
                }
                BusinessDirectResponse::Order(r) => BusinessDirectResponse::Order(r.clone()),
                BusinessDirectResponse::Orders(r) => BusinessDirectResponse::Orders(r.clone()),
                BusinessDirectResponse::Trades(r) => BusinessDirectResponse::Trades(r.clone()),
                BusinessDirectResponse::CancelOrder(r) => {
                    BusinessDirectResponse::CancelOrder(contracts::CancelOrderDirectResponse {
                        metadata: r.metadata.clone(),
                        target: target(&r.target),
                    })
                }
                BusinessDirectResponse::CancelOrders(r) => {
                    BusinessDirectResponse::CancelOrders(contracts::CancelOrdersDirectResponse {
                        metadata: r.metadata.clone(),
                        targets: r.targets.iter().map(target).collect(),
                    })
                }
                BusinessDirectResponse::CancelMarketOrders(r) => {
                    BusinessDirectResponse::CancelMarketOrders(
                        contracts::CancelMarketOrdersDirectResponse {
                            metadata: r.metadata.clone(),
                            targets: r.targets.iter().map(target).collect(),
                        },
                    )
                }
                BusinessDirectResponse::CancelAllOrders(r) => {
                    BusinessDirectResponse::CancelAllOrders(
                        contracts::CancelAllOrdersDirectResponse {
                            metadata: r.metadata.clone(),
                            targets: r.targets.iter().map(target).collect(),
                        },
                    )
                }
            };
            Arc::new(StoredDirectResponse::Final(response))
        })
        .collect()
}
pub trait BusinessProjectionPortV2 {
    fn uses_background_normalization(&self) -> bool {
        false
    }
    fn accept_for_background(
        &mut self,
        _: Arc<BusinessProjectionBatchPayloadV1>,
    ) -> Result<noe_projection::AcceptedNoeProjectionV1, noe_projection::MemoryNoeErrorV1> {
        Err(noe_projection::MemoryNoeErrorV1::InvalidProjection(
            "端口不支持后台归一化材料",
        ))
    }
    fn close_receive_prefix(&mut self, _at: i64) {}
    fn observe_public_time(&mut self, _time: &TrustedActorExecutionTimeV1) {}

    fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool;

    fn try_accept_payload(&mut self, batch: Arc<BusinessProjectionBatchPayloadV1>) -> bool {
        match batch.as_ref() {
            BusinessProjectionBatchPayloadV1::V2(batch) => self.try_accept(Arc::clone(batch)),
            BusinessProjectionBatchPayloadV1::V3(_) | BusinessProjectionBatchPayloadV1::V4(_) => {
                false
            }
        }
    }
    fn durable_through(&self, run_id: &str) -> u64;
    fn actual_delivered_private_feedback(&self, _run_id: &str) -> Option<u64> {
        None
    }
}

struct PendingProjectionBatchV2 {
    batch: Arc<BusinessProjectionBatchPayloadV1>,

    accepted: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MutationActionTransitionEffectsV2 {
    business: MutationActionBusinessEffectV3,
    lifecycle: Vec<LifecycleFeedbackEffectV2>,
    completed_response: Option<Arc<StoredDirectResponseV3>>,
}

#[derive(Debug)]
struct MutationRequestRecordV2 {
    method: MutationMethodV2,
    caller_correlation_id: Option<String>,
    input_sequence: u64,
    request_sent_at_utc_ms: i64,
    platform_arrived_at_utc_ms: i64,
    inbound_delay_ms: u64,
    scheduled_action_count: u64,
    remaining_action_count: u64,
    post_results: Vec<Arc<PostOrderItemBusinessEffectV2>>,
    cancel_results: Vec<Arc<CancelTargetBusinessEffectV3>>,
    query_result: Option<Arc<QueryActionBusinessEffectV2>>,
    completed_response: Option<Arc<StoredDirectResponseV3>>,
}

#[derive(Debug, Default)]
struct MutationRequestStateV2 {
    requests_by_id: BTreeMap<String, MutationRequestRecordV2>,
    pending_actions: BTreeMap<MutationActionKeyV2, MutationActionV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OrdersQueryKeyV2 {
    condition_id: Option<String>,
    token_id: Option<String>,
}

impl From<&OrdersFilterV1> for OrdersQueryKeyV2 {
    fn from(filter: &OrdersFilterV1) -> Self {
        Self {
            condition_id: filter
                .condition_id
                .as_ref()
                .map(|condition_id| condition_id.0.clone()),
            token_id: filter.token_id.as_ref().map(|token_id| token_id.0.clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TradesQueryKeyV2 {
    trade_id: Option<String>,
    condition_id: Option<String>,
    token_id: Option<String>,
    after_utc_ms: Option<i64>,
    before_utc_ms: Option<i64>,
}

impl From<&TradesFilterV1> for TradesQueryKeyV2 {
    fn from(filter: &TradesFilterV1) -> Self {
        Self {
            trade_id: filter.trade_id.clone(),
            condition_id: filter
                .condition_id
                .as_ref()
                .map(|condition_id| condition_id.0.clone()),
            token_id: filter.token_id.as_ref().map(|token_id| token_id.0.clone()),
            after_utc_ms: filter.after_utc_ms,
            before_utc_ms: filter.before_utc_ms,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct QueryStateV2 {
    current_revision: u64,
    earliest_retained_root_sequence: u64,
    order_history_retained_from_utc_ms: Option<i64>,
    trade_history_retained_from_utc_ms: Option<i64>,
    order_roots: BTreeMap<(u64, OrdersQueryKeyV2), Vec<OrderProjectionV2>>,
    trade_roots: BTreeMap<(u64, TradesQueryKeyV2), Vec<TradeProjectionV2>>,
}

impl QueryStateV2 {
    fn advance_revision(&mut self) {
        self.current_revision = self
            .current_revision
            .checked_add(1)
            .expect("V2 查询可观察修订不得溢出");
    }

    fn advance_retention(&mut self, advance: RetentionAdvanceV2) -> Result<(), SimRunActorErrorV2> {
        let advances_optional_floor = |current: Option<i64>, next: Option<i64>| match current {
            None => true,
            Some(current) => next.is_some_and(|next| next >= current),
        };
        if advance.earliest_retained_root_sequence < self.earliest_retained_root_sequence
            || advance.earliest_retained_root_sequence > self.current_revision
            || !advances_optional_floor(
                self.order_history_retained_from_utc_ms,
                advance.order_history_retained_from_utc_ms,
            )
            || !advances_optional_floor(
                self.trade_history_retained_from_utc_ms,
                advance.trade_history_retained_from_utc_ms,
            )
        {
            return Err(SimRunActorErrorV2::InvalidRetentionAdvance);
        }

        while self
            .order_roots
            .first_key_value()
            .is_some_and(|((root_sequence, _), _)| {
                *root_sequence < advance.earliest_retained_root_sequence
            })
        {
            self.order_roots.pop_first();
        }
        while self
            .trade_roots
            .first_key_value()
            .is_some_and(|((root_sequence, _), _)| {
                *root_sequence < advance.earliest_retained_root_sequence
            })
        {
            self.trade_roots.pop_first();
        }
        self.earliest_retained_root_sequence = advance.earliest_retained_root_sequence;
        self.order_history_retained_from_utc_ms = advance.order_history_retained_from_utc_ms;
        self.trade_history_retained_from_utc_ms = advance.trade_history_retained_from_utc_ms;
        Ok(())
    }
}

#[cfg(feature = "measurement")]
fn orders_query_key_owned_bytes_v2(key: &OrdersQueryKeyV2) -> u64 {
    (key.condition_id.as_ref().map_or(0, String::len)
        + key.token_id.as_ref().map_or(0, String::len)) as u64
}

#[cfg(feature = "measurement")]
fn trades_query_key_owned_bytes_v2(key: &TradesQueryKeyV2) -> u64 {
    (key.trade_id.as_ref().map_or(0, String::len)
        + key.condition_id.as_ref().map_or(0, String::len)
        + key.token_id.as_ref().map_or(0, String::len)) as u64
}

fn orders_page_from_root_v2(
    root_sequence: u64,
    rows: &[OrderProjectionV2],
    next_offset: u64,
) -> OrdersQueryResultV2 {
    let Ok(start) = usize::try_from(next_offset) else {
        return OrdersQueryResultV2::InvalidCursor;
    };
    if start > rows.len() {
        return OrdersQueryResultV2::InvalidCursor;
    }
    let end = start.saturating_add(SIM_QUERY_PAGE_SIZE_V1).min(rows.len());
    let next_cursor = (end < rows.len()).then_some(QueryCursorV2 {
        root_sequence,
        next_offset: u64::try_from(end).expect("V2 查询根行数必须可表示为 u64"),
    });
    OrdersQueryResultV2::Page(OrdersPageV2 {
        root_sequence,
        items: rows[start..end].to_vec(),
        next_cursor,
    })
}

fn trades_page_from_root_v2(
    root_sequence: u64,
    rows: &[TradeProjectionV2],
    next_offset: u64,
) -> TradesQueryResultV2 {
    let Ok(start) = usize::try_from(next_offset) else {
        return TradesQueryResultV2::InvalidCursor;
    };
    if start > rows.len() {
        return TradesQueryResultV2::InvalidCursor;
    }
    let end = start.saturating_add(SIM_QUERY_PAGE_SIZE_V1).min(rows.len());
    let next_cursor = (end < rows.len()).then_some(QueryCursorV2 {
        root_sequence,
        next_offset: u64::try_from(end).expect("V2 查询根行数必须可表示为 u64"),
    });
    TradesQueryResultV2::Page(TradesPageV2 {
        root_sequence,
        items: rows[start..end].to_vec(),
        next_cursor,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationRequestReceiptV2 {
    input_sequence: u64,
    platform_arrived_at_utc_ms: i64,
    scheduled_action_count: u64,
}

impl MutationRequestReceiptV2 {
    pub fn input_sequence(&self) -> u64 {
        self.input_sequence
    }

    pub fn platform_arrived_at_utc_ms(&self) -> i64 {
        self.platform_arrived_at_utc_ms
    }

    pub fn scheduled_action_count(&self) -> u64 {
        self.scheduled_action_count
    }
}

type PostOrderItemBusinessEffectV2 = SubmissionResponseItemV2;
type PostOrderItemOutcomeV2 = SubmissionItemOutcomeV2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LogicalPathFrontierV2 {
    closed_through_utc_ms: Option<i64>,
    next_index_position: u64,
    next_event_at_utc_ms: Option<i64>,
    sealed: bool,
}

#[derive(Debug)]
struct UniqueSessionStateV2 {
    opening_fact: Arc<AccountOpeningFactV2>,
    response_bridge_binding_id: String,
    session_seed: Sha256Hex,
    initialization: SessionInitializationV2,
    market_directory: SessionMarketDirectoryV1,
    public_markets: MarketStateStoreV2,
    hidden_platform_markets: Option<MarketStateStoreV2>,
    execution_state: ExecutionStateV2,
    market_enrollment: MarketEnrollmentStateV2,
    mutation_requests: MutationRequestStateV2,
    query_state: QueryStateV2,
    next_private_feedback_sequence: u64,
    next_projection_batch_sequence: u64,
    hidden_platform_frontier: LogicalPathFrontierV2,
    public_visibility_frontier: LogicalPathFrontierV2,
}

#[derive(Debug, Default)]
struct MarketEnrollmentStateV2 {
    records: Vec<MarketEnrollmentV2>,
    records_by_request_id: BTreeMap<String, usize>,
    business_input_started: bool,
    paper_candidate_condition_ids: BTreeSet<String>,
    paper_candidate_token_ids: BTreeSet<String>,
}

#[derive(Debug)]
pub(crate) struct BusinessReducerStateV2 {
    session: Option<UniqueSessionStateV2>,

    pm_materials: Vec<DeferredPublicPmReductionV1>,
    public_pm_evidence: BTreeMap<
        String,
        data_pipeline_core::public_observation_core::PmOwnedStateV1<
            data_pipeline_core::pm_reduction::PreparedPmEvidenceV1<contracts::SourceEvidenceRefV3>,
        >,
    >,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SessionMarketEventEffectsV2 {
    execution: Vec<ExecutionMarketEventEffectsV2>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LogicalDriveOutputV2 {
    // 本次 lifecycle 前缀中补入的旧返佣数量及实际处理时刻，仅活到投影物化。
    late_rebate_feedback: Option<(usize, i64)>,
    lifecycle: Vec<LifecycleFeedbackEffectV2>,
    mutations: Vec<MutationActionTransitionEffectsV2>,
}

enum MarketEventSliceReductionErrorV2 {
    Unaccepted {
        state: BusinessReducerStateV2,
        error: SimRunActorErrorV2,
    },
    Fatal(SimRunActorErrorV2),
}

impl MarketEnrollmentStateV2 {
    fn successful(&self, request_id: &str) -> Option<&MarketEnrollmentV2> {
        self.records_by_request_id
            .get(request_id)
            .and_then(|index| self.records.get(*index))
    }
}

impl UniqueSessionStateV2 {
    fn recording_closed_through(&self) -> Option<i64> {
        match self.opening_fact.account_instance_key.caller_role {
            SimCallerRoleV1::Paper => self.public_visibility_frontier.closed_through_utc_ms,
            SimCallerRoleV1::ModelTrain
            | SimCallerRoleV1::OfflineEvaluation
            | SimCallerRoleV1::HistoricalBacktest => {
                self.hidden_platform_frontier.closed_through_utc_ms
            }
        }
    }

    fn enroll_market(
        &mut self,
        request: &MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV2, SimRunActorErrorV2> {
        if !matches!(self.initialization, SessionInitializationV2::Active { .. }) {
            return Err(SimRunActorErrorV2::SessionNotActive);
        }
        if !is_stable_text(&request.enrollment_request_id) {
            return Err(SimRunActorErrorV2::InvalidMarketEnrollment(
                "enrollment_request_id 为空或含首尾空白",
            ));
        }
        request
            .market
            .validate_for_admission()
            .map_err(|_| SimRunActorErrorV2::InvalidMarketEnrollment("市场身份或结果身份非法"))?;
        let caller_role = self.opening_fact.account_instance_key.caller_role;
        if matches!(
            caller_role,
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation
        ) && self.market_enrollment.business_input_started
        {
            return Err(SimRunActorErrorV2::MarketEnrollmentFrozenAfterBusinessInput);
        }
        if caller_role == SimCallerRoleV1::Paper {
            let condition_id = &request.market.market().condition_id.0;
            if self
                .market_enrollment
                .paper_candidate_condition_ids
                .contains(condition_id)
            {
                return Err(SimRunActorErrorV2::MarketEnrollmentCandidateAlreadyUsed(
                    condition_id.clone(),
                ));
            }
            if let Some(asset) = request.market.assets().iter().find(|asset| {
                self.market_enrollment
                    .paper_candidate_token_ids
                    .contains(&asset.token_id.0)
            }) {
                return Err(SimRunActorErrorV2::MarketEnrollmentCandidateAlreadyUsed(
                    asset.token_id.0.clone(),
                ));
            }
        }

        let enrollment_sequence = u64::try_from(self.market_enrollment.records.len())
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or(SimRunActorErrorV2::MarketEnrollmentSequenceOverflow)?;
        let enrollment = MarketEnrollmentV2 {
            enrollment_request_id: request.enrollment_request_id.clone(),
            enrollment_sequence,
            market: request.market.market().clone(),
            assets: request.market.assets().clone(),
        };

        self.market_directory
            .try_insert(request.market.clone())
            .map_err(SimRunActorErrorV2::MarketEnrollmentDirectory)?;
        self.public_markets.commit_admitted_market(&request.market);
        if let Some(hidden) = self.hidden_platform_markets.as_mut() {
            hidden.commit_admitted_market(&request.market);
        }
        self.execution_state.commit_admitted_market(&request.market);
        let index = self.market_enrollment.records.len();
        self.market_enrollment.records.push(enrollment.clone());
        self.market_enrollment
            .records_by_request_id
            .insert(request.enrollment_request_id.clone(), index);
        Ok(enrollment)
    }

    fn map_execution_state<R>(
        self,
        apply: impl FnOnce(ExecutionStateV2) -> Result<(ExecutionStateV2, R), MatchingStateErrorV1>,
    ) -> Result<(Self, R), MatchingStateErrorV1> {
        let Self {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        } = self;
        let (execution_state, result) = apply(execution_state)?;
        Ok((
            Self {
                opening_fact,
                response_bridge_binding_id,
                session_seed,
                initialization,
                market_directory,
                public_markets,
                hidden_platform_markets,
                execution_state,
                market_enrollment,
                mutation_requests,
                query_state,
                next_private_feedback_sequence,
                next_projection_batch_sequence,
                hidden_platform_frontier,
                public_visibility_frontier,
            },
            result,
        ))
    }

    fn apply_buy_order(
        self,
        input: BuyOrderActionV2,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<(Self, BuyOrderDispositionV2), MatchingStateErrorV1> {
        self.map_execution_state(|execution_state| {
            execution_state.apply_buy_order(input, rules, finality_policy)
        })
    }

    fn apply_caller_cancel(
        self,
        global_request_id: Option<&str>,
        platform_order_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(Self, CallerCancelDispositionV2), MatchingStateErrorV1> {
        self.map_execution_state(|execution_state| {
            execution_state.apply_caller_cancel_for_request(
                global_request_id,
                platform_order_id,
                platform_effective_at_utc_ms,
            )
        })
    }
}

pub(crate) enum BusinessReducerInputV2<'a> {
    CreateSession(&'a SessionSpecV3),
    ActivateSession(&'a str),
    FailSessionInitialization(&'a str),
    EnrollMarket(&'a MarketEnrollmentRequestV1),
    RegisterNewRequest {
        cutoff: &'a TrustedDecisionCutoffV2,
        request: &'a BusinessRequestV2,
        input_sequence: u64,
        latency_profile: &'a EmpiricalDelayProfile,
        rules: &'a PlatformRuleSnapshotV3,
    },
    AdvanceNextMutationActionAt {
        platform_effective_at_utc_ms: i64,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    PlatformPrefixAdvance {
        advance: PlatformPrefixAdvanceV2,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    ClosedMarketBucket {
        bucket: AcceptedClosedMarketBucketV2,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    HiddenStepBucket {
        bucket: &'a AcceptedHiddenStepBucketV1,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    PublicGroup {
        group: &'a AcceptedPublicGroupV1,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    PublicObserveTime {
        time: &'a TrustedActorExecutionTimeV1,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    PublicCloseTime {
        closure: &'a TrustedPublicTimeClosureV1,
        rules: &'a PlatformRuleSnapshotV3,
        finality_policy: &'a TradeFinalityPolicyV2,
    },
    RetentionAdvance(RetentionAdvanceV2),
}

pub(crate) enum BusinessReducerOutputV2 {
    CreateSession(CreateSessionControlResultV2),
    SessionInitialization(SessionInitializationControlResultV2),
    MarketEnrollment(MarketEnrollmentV2),
    RequestRegistration(MutationRequestReceiptV2),
    MutationAction(Option<MutationActionTransitionEffectsV2>),
    LogicalDrive(LogicalDriveOutputV2),
    RetentionAdvanced,
}

fn execution_method_v2(method: MutationMethodV2) -> ExecutionMethodV2 {
    match method {
        MutationMethodV2::PostOrder => ExecutionMethodV2::PostOrder,
        MutationMethodV2::PostOrders => ExecutionMethodV2::PostOrders,
        MutationMethodV2::CancelOrder => ExecutionMethodV2::CancelOrder,
        MutationMethodV2::CancelOrders => ExecutionMethodV2::CancelOrders,
        MutationMethodV2::CancelMarketOrders => ExecutionMethodV2::CancelMarketOrders,
        MutationMethodV2::CancelAllOrders => ExecutionMethodV2::CancelAllOrders,
        MutationMethodV2::Order => ExecutionMethodV2::Order,
        MutationMethodV2::Orders => ExecutionMethodV2::Orders,
        MutationMethodV2::Trades => ExecutionMethodV2::Trades,
    }
}

fn finalize_direct_response_v2(
    response: &DirectResponseDraftV3,
    private_feedback_sequence: u64,
) -> BusinessDirectResponseV3 {
    let metadata = DirectResponseMetadataV2 {
        global_request_id: response.global_request_id.clone(),
        caller_correlation_id: response.caller_correlation_id.clone(),
        method: execution_method_v2(response.method),
        platform_responded_at_utc_ms: response.platform_responded_at_utc_ms,
        response_received_at_utc_ms: response.response_received_at_utc_ms,
        private_feedback_sequence,
    };
    match &response.payload {
        DirectResponsePayloadDraftV3::PostOrder(item) => {
            BusinessDirectResponse::PostOrder(PostOrderDirectResponseV2 {
                metadata,
                item: item.as_ref().clone(),
            })
        }
        DirectResponsePayloadDraftV3::PostOrders(items) => {
            BusinessDirectResponse::PostOrders(PostOrdersDirectResponseV2 {
                metadata,
                items: items.iter().map(|item| item.as_ref().clone()).collect(),
            })
        }
        DirectResponsePayloadDraftV3::CancelOrder(target) => {
            BusinessDirectResponse::CancelOrder(contracts::CancelOrderDirectResponse {
                metadata,
                target: target.as_ref().clone(),
            })
        }
        DirectResponsePayloadDraftV3::CancelOrders(targets) => {
            BusinessDirectResponse::CancelOrders(contracts::CancelOrdersDirectResponse {
                metadata,
                targets: targets
                    .iter()
                    .map(|target| target.as_ref().clone())
                    .collect(),
            })
        }
        DirectResponsePayloadDraftV3::CancelMarketOrders(targets) => {
            BusinessDirectResponse::CancelMarketOrders(
                contracts::CancelMarketOrdersDirectResponse {
                    metadata,
                    targets: targets
                        .iter()
                        .map(|target| target.as_ref().clone())
                        .collect(),
                },
            )
        }
        DirectResponsePayloadDraftV3::CancelAllOrders(targets) => {
            BusinessDirectResponse::CancelAllOrders(contracts::CancelAllOrdersDirectResponse {
                metadata,
                targets: targets
                    .iter()
                    .map(|target| target.as_ref().clone())
                    .collect(),
            })
        }
        DirectResponsePayloadDraftV3::Order(result) => {
            let QueryActionResultV2::Order(result) = &result.result else {
                unreachable!("Order 响应草稿只携带单订单查询结果")
            };
            BusinessDirectResponse::Order(OrderDirectResponseV2 {
                metadata,
                result: result.clone(),
            })
        }
        DirectResponsePayloadDraftV3::Orders(result) => {
            let QueryActionResultV2::Orders(result) = &result.result else {
                unreachable!("Orders 响应草稿只携带订单列表查询结果")
            };
            BusinessDirectResponse::Orders(OrdersDirectResponseV2 {
                metadata,
                result: result.clone(),
            })
        }
        DirectResponsePayloadDraftV3::Trades(result) => {
            let QueryActionResultV2::Trades(result) = &result.result else {
                unreachable!("Trades 响应草稿只携带成交列表查询结果")
            };
            BusinessDirectResponse::Trades(TradesDirectResponseV2 {
                metadata,
                result: result.clone(),
            })
        }
    }
}

fn finalize_lifecycle_feedback_v2(
    effect: LifecycleFeedbackEffectV2,
    account_instance_key: &AccountInstanceKeyV1,
    session_seed: &Sha256Hex,
    latency_profile: &EmpiricalDelayProfile,
    private_feedback_sequence: u64,
    processed_at_utc_ms: Option<i64>,
) -> Result<FinalizedLifecycleFeedbackV2, SimRunActorErrorV2> {
    let platform_event_at_utc_ms = match &effect {
        LifecycleFeedbackEffectV2::Order(effect) => effect.platform_event_at_utc_ms,
        LifecycleFeedbackEffectV2::Trade(effect) => effect.platform_event_at_utc_ms,
        LifecycleFeedbackEffectV2::Settlement(effect) => effect.platform_event_at_utc_ms,
    };
    let sent_at_utc_ms = match &effect {
        LifecycleFeedbackEffectV2::Settlement(effect) => effect
            .repeated_at_utc_ms
            .unwrap_or(platform_event_at_utc_ms),
        _ => platform_event_at_utc_ms,
    };
    let stable_sequence = private_feedback_sequence.to_string();
    let delay_ms = latency_profile.sample_delay_ms(
        session_seed.0.as_bytes(),
        &stable_sequence,
        DelaySampleDomainV1::LifecycleFeedbackInbound,
    );
    let metadata = contracts::LifecycleFeedbackMetadataV2 {
        account_instance_key: account_instance_key.clone(),
        platform_event_at_utc_ms,
        feedback_received_at_utc_ms: checked_add_delay_v2(
            processed_at_utc_ms.unwrap_or(sent_at_utc_ms),
            delay_ms,
        )?,
        private_feedback_sequence,
    };
    Ok(match effect {
        LifecycleFeedbackEffectV2::Order(effect) => {
            FinalizedLifecycleFeedbackV2::Order(OrderLifecycleFeedbackV2 {
                metadata,
                platform_order_id: effect.platform_order_id,
                condition_id: effect.condition_id,
                token_id: effect.token_id,
                event: effect.event,
            })
        }
        LifecycleFeedbackEffectV2::Trade(effect) => {
            FinalizedLifecycleFeedbackV2::Trade(contracts::TradeLifecycleFeedbackV3 {
                maker_fee_equivalent_pusd: effect.maker_fee_equivalent_pusd,
                fill_sequence: effect.fill_sequence,
                matched_at_utc_ms: effect.matched_at_utc_ms,
                feedback: TradeLifecycleFeedbackV2 {
                    metadata,
                    trade_id: effect.trade_id,
                    platform_order_id: effect.platform_order_id,
                    condition_id: effect.condition_id,
                    token_id: effect.token_id,
                    status: effect.status,
                    economic_disposition: effect.economic_disposition,
                    liquidity_role: effect.liquidity_role,
                    source_price_pusd_per_share: effect.source_price_pusd_per_share,
                    base_quantity_shares: effect.base_quantity_shares,
                    quote_amount_pusd: effect.quote_amount_pusd,
                    immediate_taker_fee_pusd: effect.immediate_taker_fee_pusd,
                    economic_application_account_transition_seq: effect
                        .economic_application_account_transition_seq,
                    account_delta: effect.account_delta,
                },
            })
        }
        LifecycleFeedbackEffectV2::Settlement(effect) => {
            FinalizedLifecycleFeedbackV2::Settlement(SettlementLifecycleFeedbackV2 {
                metadata,
                event: effect.event,
            })
        }
    })
}

enum FinalizedLifecycleFeedbackV2 {
    Order(OrderLifecycleFeedbackV2),
    Trade(contracts::TradeLifecycleFeedbackV3),
    Settlement(SettlementLifecycleFeedbackV2),
}

enum ProjectionDraftV2 {
    Lifecycle(LifecycleFeedbackEffectV2, Option<i64>),
    Direct(Arc<StoredDirectResponseV3>),
}

pub(crate) enum CommittedBusinessOutputV2 {
    CreateSession(CreateSessionControlResultV2),
    SessionInitialization(SessionInitializationControlResultV2),
    MarketEnrollment(MarketEnrollmentV2),
    RequestRegistration(MutationRequestReceiptV2),
    Driven,
}

fn finalize_projection_batch_v2(
    run_id: &str,
    mut state: BusinessReducerStateV2,
    output: BusinessReducerOutputV2,
    latency_profile: &EmpiricalDelayProfile,
) -> Result<
    (
        BusinessReducerStateV2,
        CommittedBusinessOutputV2,
        Option<BusinessProjectionBatchV4>,
    ),
    SimRunActorErrorV2,
> {
    let mut account_opening = None;
    let mut initial_markets = Vec::new();
    let mut market_enrollment = None;
    let mut drafts = Vec::new();
    let committed = match output {
        BusinessReducerOutputV2::CreateSession(result) => {
            account_opening = Some(Arc::clone(&result.opening_fact));
            initial_markets = state
                .session
                .as_ref()
                .expect("实际开户已创建会话")
                .market_directory
                .markets()
                .map(|m| contracts::economics::SourceNeutralInitialMarketV1 {
                    market: m.market().clone(),
                    assets: m.assets().clone(),
                })
                .collect();
            CommittedBusinessOutputV2::CreateSession(result)
        }
        BusinessReducerOutputV2::MarketEnrollment(result) => {
            market_enrollment = Some(result.clone());
            CommittedBusinessOutputV2::MarketEnrollment(result)
        }
        BusinessReducerOutputV2::MutationAction(Some(effect)) => {
            drafts.extend(
                effect
                    .lifecycle
                    .into_iter()
                    .map(|effect| ProjectionDraftV2::Lifecycle(effect, None)),
            );
            if let Some(response) = effect.completed_response {
                drafts.push(ProjectionDraftV2::Direct(response));
            }
            CommittedBusinessOutputV2::Driven
        }
        BusinessReducerOutputV2::LogicalDrive(output) => {
            drafts.extend(
                output
                    .lifecycle
                    .into_iter()
                    .enumerate()
                    .map(|(index, effect)| {
                        let processed_at = output
                            .late_rebate_feedback
                            .filter(|(count, _)| index < *count)
                            .map(|(_, at)| at);
                        ProjectionDraftV2::Lifecycle(effect, processed_at)
                    }),
            );
            for mutation in output.mutations {
                drafts.extend(
                    mutation
                        .lifecycle
                        .into_iter()
                        .map(|effect| ProjectionDraftV2::Lifecycle(effect, None)),
                );
                if let Some(response) = mutation.completed_response {
                    drafts.push(ProjectionDraftV2::Direct(response));
                }
            }
            CommittedBusinessOutputV2::Driven
        }
        BusinessReducerOutputV2::SessionInitialization(result) => {
            CommittedBusinessOutputV2::SessionInitialization(result)
        }
        BusinessReducerOutputV2::RequestRegistration(result) => {
            CommittedBusinessOutputV2::RequestRegistration(result)
        }
        BusinessReducerOutputV2::MutationAction(None) => CommittedBusinessOutputV2::Driven,
        BusinessReducerOutputV2::RetentionAdvanced => CommittedBusinessOutputV2::Driven,
    };
    if account_opening.is_none() && market_enrollment.is_none() && drafts.is_empty() {
        return Ok((state, committed, None));
    }
    let session = state
        .session
        .as_mut()
        .expect("形成业务投影时唯一会话必须存在");
    let projection_batch_sequence = session.next_projection_batch_sequence;
    session.next_projection_batch_sequence = projection_batch_sequence
        .checked_add(1)
        .expect("有界投影批次序号不得溢出");
    let mut next_private_feedback_sequence = session.next_private_feedback_sequence;
    let mut direct_responses = Vec::new();
    let mut order_lifecycle = Vec::new();
    let mut trade_lifecycle = Vec::new();
    let mut settlement_lifecycle = Vec::new();
    for draft in drafts {
        match draft {
            ProjectionDraftV2::Lifecycle(effect, processed_at) => {
                let finalized = finalize_lifecycle_feedback_v2(
                    effect,
                    &session.opening_fact.account_instance_key,
                    &session.session_seed,
                    latency_profile,
                    next_private_feedback_sequence,
                    processed_at,
                )?;
                match finalized {
                    FinalizedLifecycleFeedbackV2::Order(feedback) => {
                        order_lifecycle.push(feedback);
                    }
                    FinalizedLifecycleFeedbackV2::Trade(feedback) => {
                        trade_lifecycle.push(feedback);
                    }
                    FinalizedLifecycleFeedbackV2::Settlement(feedback) => {
                        settlement_lifecycle.push(feedback);
                    }
                }
            }
            ProjectionDraftV2::Direct(response) => {
                let StoredDirectResponseV3::Draft(response_draft) = response.as_ref() else {
                    unreachable!("单次归约只会产生尚未投影的直接响应草稿")
                };
                let global_request_id = response_draft.global_request_id.clone();
                let response = Arc::new(StoredDirectResponseV3::Final(
                    finalize_direct_response_v2(response_draft, next_private_feedback_sequence),
                ));
                session
                    .mutation_requests
                    .requests_by_id
                    .get_mut(&global_request_id)
                    .expect("直接响应对应的请求必须仍由唯一会话持有")
                    .completed_response = Some(Arc::clone(&response));
                direct_responses.push(response);
            }
        }
        next_private_feedback_sequence = next_private_feedback_sequence
            .checked_add(1)
            .expect("有界私有反馈序号不得溢出");
    }
    session.next_private_feedback_sequence = next_private_feedback_sequence;
    #[cfg(test)]
    record_projection_batch_construction_for_tests();
    Ok((
        state,
        committed,
        Some(BusinessProjectionBatchV4 {
            initial_markets,
            run_id: run_id.to_owned(),
            projection_batch_sequence,
            account_opening,
            market_enrollment,
            direct_responses,
            order_lifecycle,
            trade_lifecycle,
            settlement_lifecycle,
        }),
    ))
}

#[derive(Debug)]
pub(crate) enum BusinessReductionErrorV2 {
    Unaccepted {
        state: BusinessReducerStateV2,
        error: SimRunActorErrorV2,
    },
    Fatal(SimRunActorErrorV2),
}

pub(crate) type BusinessReductionResultV2 =
    Result<(BusinessReducerStateV2, BusinessReducerOutputV2), BusinessReductionErrorV2>;

pub(crate) struct BusinessGroupCommitV2 {
    pub(crate) outputs: Vec<CommittedBusinessOutputV2>,
    pub(crate) unaccepted: Option<SimRunActorErrorV2>,
}

fn reduce_actor_input_v2(
    opened: &RunOpenedV2,
    initial_market_directory: &SessionMarketDirectoryV1,
    state: BusinessReducerStateV2,
    input: &BusinessInputV2,
    input_sequence: u64,
    latency_profile: &EmpiricalDelayProfile,
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
) -> BusinessReductionResultV2 {
    match input {
        BusinessInputV2::CancelScenario(
            crate::runtime_v2::CancelScenarioOperationV1::Configure(scenario),
        ) => {
            let mut state = state;
            if opened.projection_version != crate::runtime_v2::BusinessProjectionVersionV1::V4 {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::CompletionNotReady("撤单场景只适用于V4根"),
                ));
            }
            let Some(session) = state.session.as_mut() else {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::CompletionNotReady("撤单场景需要已开户会话"),
                ));
            };
            if scenario.targets.iter().any(|target| {
                session
                    .mutation_requests
                    .requests_by_id
                    .contains_key(&target.global_request_id)
            }) {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::CompletionNotReady("撤单场景必须在指定请求登记前配置"),
                ));
            }

            session
                .execution_state
                .configure_cancel_scenario(scenario, input_sequence);

            Ok((
                state,
                BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2::default()),
            ))
        }
        BusinessInputV2::Settlement(operation) => {
            let mut state = state;
            let Some(session) = state.session.as_mut() else {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::CompletionNotReady("结算需要已开户会话"),
                ));
            };

            let mut operation = operation.clone();
            use crate::matching::settlement::SettlementOperationV1 as Op;
            let arrival = match &mut operation {
                Op::Request {
                    request,
                    requested_at_utc_ms,
                } => Some((&request.identity.resolution_identity, requested_at_utc_ms)),
                Op::Query {
                    identity,
                    requested_at_utc_ms,
                } => Some((&identity.resolution_identity, requested_at_utc_ms)),
                Op::Configure(_) | Op::ExternalCash { .. } => None,
            };
            if let Some((identity, at)) = arrival {
                *at = checked_add_delay_v2(
                    *at,
                    latency_profile.sample_delay_ms(
                        session.session_seed.0.as_bytes(),
                        identity,
                        DelaySampleDomainV1::RequestOutbound,
                    ),
                )
                .map_err(BusinessReductionErrorV2::Fatal)?;
            }
            let lifecycle = session
                .execution_state
                .settlement_operation(&operation)
                .map_err(|error| {
                    BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
                })?;

            Ok((
                state,
                BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {
                    lifecycle,
                    late_rebate_feedback: None,
                    mutations: Vec::new(),
                }),
            ))
        }
        BusinessInputV2::HiddenStepBucket(bucket) => drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::HiddenStepBucket {
                bucket,
                rules,
                finality_policy,
            },
        ),
        BusinessInputV2::PublicGroup(group) => drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PublicGroup {
                group,
                rules,
                finality_policy,
            },
        ),
        BusinessInputV2::PublicCloseTime(closure) => drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PublicCloseTime {
                closure,
                rules,
                finality_policy,
            },
        ),
        BusinessInputV2::PublicObserveTime(time) => drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PublicObserveTime {
                time,
                rules,
                finality_policy,
            },
        ),
        BusinessInputV2::PublicRequest {
            cutoff,
            send_time,
            request,
        } => {
            let legacy = TrustedDecisionCutoffV2 {
                logical_time_utc_ms: send_time
                    .as_ref()
                    .unwrap_or(&cutoff.decision_time)
                    .logical_time_utc_ms(),
                public_visibility: PublicVisibilityCursorV1::default(),
                private_feedback_sequence: cutoff.private_feedback_sequence,
            };
            reduce_business_input_v2(
                opened,
                initial_market_directory,
                state,
                BusinessReducerInputV2::RegisterNewRequest {
                    cutoff: &legacy,
                    request,
                    input_sequence,
                    latency_profile,
                    rules,
                },
            )
        }
        BusinessInputV2::CreateSession(spec) => reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::CreateSession(spec),
        ),
        BusinessInputV2::ActivateSession { activation_id } => reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::ActivateSession(activation_id),
        ),
        BusinessInputV2::FailSessionInitialization { failure_id } => reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::FailSessionInitialization(failure_id),
        ),
        BusinessInputV2::EnrollMarket(request) => reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::EnrollMarket(request),
        ),
        BusinessInputV2::Request { cutoff, request } => reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::RegisterNewRequest {
                cutoff,
                request,
                input_sequence,
                latency_profile,
                rules,
            },
        ),
        BusinessInputV2::PlatformPrefixAdvance(advance) => drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PlatformPrefixAdvance {
                advance: *advance,
                rules,
                finality_policy,
            },
        ),
        BusinessInputV2::RetentionAdvance(advance) => reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::RetentionAdvance(*advance),
        ),
        BusinessInputV2::ClosedMarketBucket(bucket) => drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: bucket.clone(),
                rules,
                finality_policy,
            },
        ),
    }
}

#[cfg(feature = "measurement")]
fn projection_resource_quantity_v2(batch: &BusinessProjectionBatchPayloadV1) -> ResourceQuantityV1 {
    match batch {
        BusinessProjectionBatchPayloadV1::V2(batch) => {
            projection_resource_quantity(batch, trade_lifecycle_owned_bytes_v2)
        }
        BusinessProjectionBatchPayloadV1::V4(batch) => {
            projection_resource_quantity(batch, trade_lifecycle_owned_bytes_v3)
        }
        BusinessProjectionBatchPayloadV1::V3(batch) => {
            projection_resource_quantity(batch, trade_lifecycle_owned_bytes_v3)
        }
    }
}
#[cfg(feature = "measurement")]
fn projection_resource_quantity<T, R>(
    batch: &BusinessProjectionBatch<T, R>,
    trade_bytes: impl Fn(&T) -> u64,
) -> ResourceQuantityV1 {
    let records = 1_u64
        + u64::from(batch.account_opening.is_some())
        + u64::from(batch.market_enrollment.is_some())
        + batch.direct_responses.len() as u64
        + batch.order_lifecycle.len() as u64
        + batch.trade_lifecycle.len() as u64
        + batch.settlement_lifecycle.len() as u64;
    let bytes = batch.run_id.len() as u64
        + batch.market_enrollment.as_ref().map_or(0, |enrollment| {
            enrollment.enrollment_request_id.len() as u64
                + enrollment.market.condition_id.0.len() as u64
                + enrollment.market.clob_market_id.0.len() as u64
                + enrollment
                    .assets
                    .iter()
                    .map(|asset| (asset.token_id.0.len() + asset.outcome.label.len()) as u64)
                    .sum::<u64>()
        })
        + batch
            .direct_responses
            .iter()
            .map(|response| match response.as_ref() {
                StoredDirectResponse::Final(response) => direct_response_owned_bytes_v2(response),
                StoredDirectResponse::Draft(_) => {
                    unreachable!("待交付投影批次只能持有最终直接响应")
                }
            })
            .sum::<u64>()
        + batch
            .order_lifecycle
            .iter()
            .map(order_lifecycle_owned_bytes_v2)
            .sum::<u64>()
        + batch.trade_lifecycle.iter().map(trade_bytes).sum::<u64>()
        + batch
            .settlement_lifecycle
            .iter()
            .map(settlement_lifecycle_owned_bytes_v2)
            .sum::<u64>();
    ResourceQuantityV1::new(records, bytes)
}

#[cfg(feature = "measurement")]
fn account_key_owned_bytes_v2(key: &AccountInstanceKeyV1) -> u64 {
    (key.run_id.len() + key.caller_instance_id.len() + key.account_id.len()) as u64
}

#[cfg(feature = "measurement")]
pub(crate) fn settlement_request_owned_bytes_v1(
    id: &contracts::economics::SettlementRequestIdV1<String>,
) -> u64 {
    account_key_owned_bytes_v2(&id.account)
        + (id.condition_id.0.len() + id.resolution_identity.len() + id.policy_version.len()) as u64
}

#[cfg(feature = "measurement")]
fn settlement_transition_owned_bytes_v1(
    transition: &contracts::economics::SourceNeutralAccountTransitionKindV2<String>,
) -> u64 {
    use contracts::economics::SourceNeutralAccountTransitionKindV2 as K;
    match transition {
        K::ResolutionRedeemApplied {
            resolution_identity,
            settlement_request_id,
        } => {
            resolution_identity.len() as u64
                + settlement_request_owned_bytes_v1(settlement_request_id)
        }
        K::SettlementCostRecognized {
            settlement_request_id,
            cost_identity,
        }
        | K::SettlementCostPaid {
            settlement_request_id,
            cost_identity,
        } => cost_identity.len() as u64 + settlement_request_owned_bytes_v1(settlement_request_id),
        K::ExternalCashApplied { cash_identity } => cash_identity.len() as u64,
        K::Existing(transition) => {
            use contracts::execution_normalization::SourceNeutralAccountTransitionKindV1 as Old;
            match transition {
                Old::OrderAcceptedEncumbranceApplied { order_identity }
                | Old::FakUnspentEncumbranceReleased { order_identity }
                | Old::CallerCancellationEncumbranceReleased { order_identity }
                | Old::VenueDustEncumbranceReleased { order_identity }
                | Old::GtdExpirationEncumbranceReleased { order_identity, .. } => {
                    order_identity.len() as u64
                }
                Old::MarketResolutionEncumbranceReleased {
                    resolution_identity,
                    order_identity,
                } => (resolution_identity.len() + order_identity.len()) as u64,
                Old::FillFinalityEconomicApplied { trade_identity, .. }
                | Old::FillFinalityUnappliedFailureReleased { trade_identity, .. }
                | Old::FillFinalityCompensationApplied { trade_identity, .. }
                | Old::TakerRebateAccrualApplied { trade_identity } => trade_identity.len() as u64,
                Old::AutoRedeemApplied {
                    resolution_identity,
                } => resolution_identity.len() as u64,
                _ => 0,
            }
        }
    }
}

#[cfg(feature = "measurement")]
fn account_delta_owned_bytes_v2(delta: &contracts::PlatformAccountDeltaV2) -> u64 {
    let reason = match &delta.reason {
        contracts::PlatformAccountDeltaReasonV2::RequestedSettlement(transition) => {
            settlement_transition_owned_bytes_v1(transition)
        }
        contracts::PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
            platform_order_id,
        }
        | contracts::PlatformAccountDeltaReasonV2::FakUnspentEncumbranceReleased {
            platform_order_id,
        }
        | contracts::PlatformAccountDeltaReasonV2::CallerCancellationEncumbranceReleased {
            platform_order_id,
        }
        | contracts::PlatformAccountDeltaReasonV2::GtdExpirationEncumbranceReleased {
            platform_order_id,
            ..
        }
        | contracts::PlatformAccountDeltaReasonV2::VenueDustEncumbranceReleased {
            platform_order_id,
        } => platform_order_id.len() as u64,
        contracts::PlatformAccountDeltaReasonV2::MarketResolutionEncumbranceReleased {
            resolution_event_id,
            platform_order_id,
        } => (resolution_event_id.len() + platform_order_id.len()) as u64,
        contracts::PlatformAccountDeltaReasonV2::FillFinalityEconomicApplied {
            trade_id, ..
        }
        | contracts::PlatformAccountDeltaReasonV2::FillFinalityUnappliedFailureReleased {
            trade_id,
            ..
        }
        | contracts::PlatformAccountDeltaReasonV2::FillFinalityCompensationApplied {
            trade_id,
            ..
        }
        | contracts::PlatformAccountDeltaReasonV2::TakerRebateAccrualApplied { trade_id } => {
            trade_id.len() as u64
        }
        contracts::PlatformAccountDeltaReasonV2::AutoRedeemApplied {
            resolution_event_id,
        } => resolution_event_id.len() as u64,
        contracts::PlatformAccountDeltaReasonV2::MakerRebateAccrualFinalized
        | contracts::PlatformAccountDeltaReasonV2::MakerRebateDayFinalized { .. }
        | contracts::PlatformAccountDeltaReasonV2::TakerRebateDayFinalized { .. }
        | contracts::PlatformAccountDeltaReasonV2::MakerRebatePaid { .. }
        | contracts::PlatformAccountDeltaReasonV2::TakerRebatePaid { .. } => 0,
    };
    let balance = &delta.balance_delta;
    account_key_owned_bytes_v2(&delta.account_instance_key)
        + reason
        + delta
            .condition_id
            .as_ref()
            .map_or(0, |condition_id| condition_id.0.len() as u64)
        + [
            &balance.cash_pusd_delta.0,
            &balance.available_pusd_delta.0,
            &balance.principal_encumbrance_pusd_delta.0,
            &balance.immediate_taker_fee_encumbrance_pusd_delta.0,
            &balance.maker_rebate_receivable_pusd_delta.0,
            &balance.taker_rebate_receivable_pusd_delta.0,
            &balance.maker_rebate_locked_remainder_pusd_delta.0,
            &balance.taker_rebate_locked_remainder_pusd_delta.0,
        ]
        .into_iter()
        .map(|value| value.len() as u64)
        .sum::<u64>()
        + balance
            .outcome_balance_deltas
            .iter()
            .map(|outcome| (outcome.token_id.0.len() + outcome.balance_shares_delta.0.len()) as u64)
            .sum::<u64>()
}

#[cfg(feature = "measurement")]
fn direct_metadata_owned_bytes_v2(metadata: &DirectResponseMetadataV2) -> u64 {
    (metadata.global_request_id.len()
        + metadata
            .caller_correlation_id
            .as_ref()
            .map_or(0, String::len)) as u64
}

#[cfg(feature = "measurement")]
fn submission_item_owned_bytes_v2(item: &SubmissionResponseItemV2) -> u64 {
    let outcome = match &item.outcome {
        SubmissionItemOutcomeV2::Accepted(accepted) => {
            accepted.platform_order_id.len() as u64
                + accepted.platform_order_status.raw.len() as u64
                + accepted
                    .fills
                    .iter()
                    .map(|fill| {
                        (fill.trade_id.len()
                            + fill.source_price_pusd_per_share.0.len()
                            + fill.base_quantity_shares.0.len()
                            + fill.quote_amount_pusd.0.len()) as u64
                    })
                    .sum::<u64>()
                + match &accepted.measurement {
                    contracts::SubmissionMeasurementV2::Base {
                        accepted_base_quantity_shares,
                        matched_base_quantity_shares,
                        open_base_quantity_shares,
                        venue_dust_closed_base_quantity_shares,
                        order_type_terminated_base_quantity_shares,
                        accepted_quote_amount_pusd,
                        matched_quote_amount_pusd,
                        open_encumbered_quote_amount_pusd,
                        venue_dust_released_quote_amount_pusd,
                        released_price_improvement_quote_amount_pusd,
                        immediate_taker_fee_encumbered_pusd,
                    } => [
                        accepted_base_quantity_shares,
                        matched_base_quantity_shares,
                        open_base_quantity_shares,
                        venue_dust_closed_base_quantity_shares,
                        order_type_terminated_base_quantity_shares,
                        accepted_quote_amount_pusd,
                        matched_quote_amount_pusd,
                        open_encumbered_quote_amount_pusd,
                        venue_dust_released_quote_amount_pusd,
                        released_price_improvement_quote_amount_pusd,
                        immediate_taker_fee_encumbered_pusd,
                    ]
                    .into_iter()
                    .map(|value| value.0.len() as u64)
                    .sum::<u64>(),
                    contracts::SubmissionMeasurementV2::QuoteBudget {
                        accepted_quote_budget_pusd,
                        used_quote_amount_pusd,
                        released_or_unspent_quote_amount_pusd,
                        actual_matched_base_quantity_shares,
                        sdk_taker_amount_audit_base_quantity_shares,
                        immediate_taker_fee_encumbered_pusd,
                    } => [
                        accepted_quote_budget_pusd,
                        used_quote_amount_pusd,
                        released_or_unspent_quote_amount_pusd,
                        actual_matched_base_quantity_shares,
                        sdk_taker_amount_audit_base_quantity_shares,
                        immediate_taker_fee_encumbered_pusd,
                    ]
                    .into_iter()
                    .map(|value| value.0.len() as u64)
                    .sum::<u64>(),
                }
        }
        SubmissionItemOutcomeV2::RejectedNoEffect(_)
        | SubmissionItemOutcomeV2::EvidenceUnknownNoEffect(_) => 0,
    };
    (item.item_id.len() + item.condition_id.0.len() + item.token_id.0.len()) as u64 + outcome
}

#[cfg(feature = "measurement")]
fn cancel_target_owned_bytes_v2<R>(target: &contracts::CancelTargetResponse<R>) -> u64 {
    let outcome = match &target.outcome {
        contracts::CancelTargetOutcome::Canceled(measurement) => {
            (measurement.canceled_base_quantity_shares.0.len()
                + measurement.released_quote_amount_pusd.0.len()) as u64
        }
        contracts::CancelTargetOutcome::NotCanceledNoEffect(_)
        | contracts::CancelTargetOutcome::EvidenceUnknownNoEffect => 0,
    };
    target.platform_order_id.len() as u64
        + target
            .input_item_refs
            .iter()
            .map(|item| item.item_id.len() as u64)
            .sum::<u64>()
        + outcome
}

#[cfg(feature = "measurement")]
fn order_result_owned_bytes_v2(result: &OrderQueryResultV2) -> u64 {
    match result {
        OrderQueryResultV2::Found(order) => order_projection_owned_bytes_v2(order),
        OrderQueryResultV2::NotFound | OrderQueryResultV2::OutOfRetention(_) => 0,
    }
}

#[cfg(feature = "measurement")]
fn orders_result_owned_bytes_v2(result: &OrdersQueryResultV2) -> u64 {
    match result {
        OrdersQueryResultV2::Page(page) => {
            page.items.iter().map(order_projection_owned_bytes_v2).sum()
        }
        OrdersQueryResultV2::InvalidCursor | OrdersQueryResultV2::OutOfRetention(_) => 0,
    }
}

#[cfg(feature = "measurement")]
fn trades_result_owned_bytes_v2(result: &TradesQueryResultV2) -> u64 {
    match result {
        TradesQueryResultV2::Page(page) => {
            page.items.iter().map(trade_projection_owned_bytes_v2).sum()
        }
        TradesQueryResultV2::InvalidCursor | TradesQueryResultV2::OutOfRetention(_) => 0,
    }
}

#[cfg(feature = "measurement")]
fn direct_response_owned_bytes_v2<R>(response: &BusinessDirectResponse<R>) -> u64 {
    match response {
        BusinessDirectResponse::PostOrder(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + submission_item_owned_bytes_v2(&response.item)
        }
        BusinessDirectResponse::PostOrders(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + response
                    .items
                    .iter()
                    .map(submission_item_owned_bytes_v2)
                    .sum::<u64>()
        }
        BusinessDirectResponse::CancelOrder(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + cancel_target_owned_bytes_v2(&response.target)
        }
        BusinessDirectResponse::CancelOrders(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + response
                    .targets
                    .iter()
                    .map(cancel_target_owned_bytes_v2)
                    .sum::<u64>()
        }
        BusinessDirectResponse::CancelMarketOrders(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + response
                    .targets
                    .iter()
                    .map(cancel_target_owned_bytes_v2)
                    .sum::<u64>()
        }
        BusinessDirectResponse::CancelAllOrders(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + response
                    .targets
                    .iter()
                    .map(cancel_target_owned_bytes_v2)
                    .sum::<u64>()
        }
        BusinessDirectResponse::Order(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + order_result_owned_bytes_v2(&response.result)
        }
        BusinessDirectResponse::Orders(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + orders_result_owned_bytes_v2(&response.result)
        }
        BusinessDirectResponse::Trades(response) => {
            direct_metadata_owned_bytes_v2(&response.metadata)
                + trades_result_owned_bytes_v2(&response.result)
        }
    }
}

#[cfg(feature = "measurement")]
fn lifecycle_metadata_owned_bytes_v2(metadata: &contracts::LifecycleFeedbackMetadataV2) -> u64 {
    account_key_owned_bytes_v2(&metadata.account_instance_key)
}

#[cfg(feature = "measurement")]
fn optional_account_delta_owned_bytes_v2(delta: Option<&contracts::PlatformAccountDeltaV2>) -> u64 {
    delta.map_or(0, account_delta_owned_bytes_v2)
}

#[cfg(feature = "measurement")]
fn order_lifecycle_owned_bytes_v2(feedback: &OrderLifecycleFeedbackV2) -> u64 {
    let event = match &feedback.event {
        contracts::OrderLifecycleEventV2::OrderAccepted {
            status,
            account_delta,
        } => status.raw.len() as u64 + account_delta_owned_bytes_v2(account_delta),
        contracts::OrderLifecycleEventV2::FakUnspentReleased {
            released_quote_amount_pusd,
            account_delta,
        } => {
            released_quote_amount_pusd.0.len() as u64 + account_delta_owned_bytes_v2(account_delta)
        }
        contracts::OrderLifecycleEventV2::VenueDustTerminal {
            closed_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } => {
            (closed_base_quantity_shares.0.len() + released_quote_amount_pusd.0.len()) as u64
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
        contracts::OrderLifecycleEventV2::MarketResolvedCanceled {
            resolution_event_id,
            canceled_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } => {
            (resolution_event_id.len()
                + canceled_base_quantity_shares.0.len()
                + released_quote_amount_pusd.0.len()) as u64
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
        contracts::OrderLifecycleEventV2::CallerCanceled {
            canceled_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } => {
            (canceled_base_quantity_shares.0.len() + released_quote_amount_pusd.0.len()) as u64
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
        contracts::OrderLifecycleEventV2::GtdExpired {
            expired_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
            ..
        } => {
            (expired_base_quantity_shares.0.len() + released_quote_amount_pusd.0.len()) as u64
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
    };
    lifecycle_metadata_owned_bytes_v2(&feedback.metadata)
        + (feedback.platform_order_id.len()
            + feedback.condition_id.0.len()
            + feedback.token_id.0.len()) as u64
        + event
}

#[cfg(feature = "measurement")]
fn trade_lifecycle_owned_bytes_v2(feedback: &TradeLifecycleFeedbackV2) -> u64 {
    lifecycle_metadata_owned_bytes_v2(&feedback.metadata)
        + (feedback.trade_id.len()
            + feedback.platform_order_id.len()
            + feedback.condition_id.0.len()
            + feedback.token_id.0.len()
            + feedback.status.raw.len()
            + feedback.source_price_pusd_per_share.0.len()
            + feedback.base_quantity_shares.0.len()
            + feedback.quote_amount_pusd.0.len()
            + feedback.immediate_taker_fee_pusd.0.len()) as u64
        + optional_account_delta_owned_bytes_v2(feedback.account_delta.as_ref())
}

#[cfg(feature = "measurement")]
fn trade_lifecycle_owned_bytes_v3(trade: &contracts::TradeLifecycleFeedbackV3) -> u64 {
    trade_lifecycle_owned_bytes_v2(&trade.feedback)
        + trade
            .maker_fee_equivalent_pusd
            .as_ref()
            .map_or(0, |fee| fee.0.len() as u64)
}

#[cfg(feature = "measurement")]
fn settlement_lifecycle_owned_bytes_v2(feedback: &SettlementLifecycleFeedbackV2) -> u64 {
    lifecycle_metadata_owned_bytes_v2(&feedback.metadata)
        + settlement_event_owned_bytes_v2(&feedback.event)
}
#[cfg(feature = "measurement")]
pub(crate) fn settlement_event_owned_bytes_v2(
    event: &contracts::SettlementLifecycleEventV2,
) -> u64 {
    use contracts::SettlementLifecycleEventV2 as Event;
    match event {
        Event::ExternalCash {
            fact,
            account_delta,
        } => {
            (fact.identity.len()
                + fact.signed_delta_pusd.0.len()
                + fact.correction_of.as_ref().map_or(0, String::len)) as u64
                + account_delta_owned_bytes_v2(account_delta)
        }
        Event::RequestedSettlement {
            result,
            account_delta,
            settlement_fee_liability_pusd_delta,
        } => {
            use contracts::economics::RequestedSettlementResultV1 as R;
            let result_bytes = match result {
                R::Request(fact) => settlement_request_owned_bytes_v1(&fact.settlement_request_id),
                R::Cost(fact) => {
                    settlement_request_owned_bytes_v1(&fact.settlement_request_id)
                        + (fact.cost_identity.len()
                            + fact.recognized_pusd.0.len()
                            + fact.paid_pusd.0.len()
                            + fact.correction_of.as_ref().map_or(0, String::len))
                            as u64
                }
            };
            result_bytes
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
                + settlement_fee_liability_pusd_delta.0.len() as u64
        }
        Event::ResolvedPosition {
            condition_id,
            resolution_event_id,
            resolution,
        } => {
            let contracts::MarketResolutionTerminalV3::Resolved {
                winning_asset_id,
                winning_outcome,
                payout_vector,
                mapping_rule_version,
            } = resolution;
            (condition_id.0.len()
                + resolution_event_id.len()
                + winning_asset_id.len()
                + winning_outcome.len()
                + mapping_rule_version.len()) as u64
                + payout_vector
                    .iter()
                    .map(|p| {
                        (p.asset.token_id.0.len()
                            + p.asset.outcome.label.len()
                            + p.payout_pusd_per_share.0.len()) as u64
                    })
                    .sum::<u64>()
        }
        Event::MakerRebateAccrualFinalized {
            condition_id,
            contributions,
            accrual_pusd,
            account_delta,
            ..
        } => {
            condition_id.0.len() as u64
                + accrual_pusd.0.len() as u64
                + contributions
                    .iter()
                    .map(|contribution| {
                        (contribution.trade_id.len()
                            + contribution.base_quantity_shares.0.len()
                            + contribution.price_pusd_per_share.0.len()
                            + contribution.maker_fee_equivalent_pusd.0.len()
                            + contribution.rebate_rate.0.len()
                            + contribution.rebate_contribution_pusd.0.len())
                            as u64
                    })
                    .sum::<u64>()
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
        Event::TakerRebateAccrualApplied {
            trade_id,
            condition_id,
            actual_taker_fee_pusd,
            rebate_rate,
            accrual_pusd,
            account_delta,
            ..
        } => {
            (trade_id.len()
                + condition_id.0.len()
                + actual_taker_fee_pusd.0.len()
                + rebate_rate.0.len()
                + accrual_pusd.0.len()) as u64
                + account_delta_owned_bytes_v2(account_delta)
        }
        Event::MakerRebateDayFinalized {
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
            ..
        }
        | Event::TakerRebateDayFinalized {
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
            ..
        } => {
            (accrual_pusd.0.len() + payable_pusd.0.len() + locked_remainder_pusd.0.len()) as u64
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
        Event::MakerRebatePaid {
            paid_pusd,
            account_delta,
            ..
        }
        | Event::TakerRebatePaid {
            paid_pusd,
            account_delta,
            ..
        } => paid_pusd.0.len() as u64 + account_delta_owned_bytes_v2(account_delta),
        Event::AutoRedeemApplied {
            condition_id,
            resolution_event_id,
            resolution,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
            ..
        } => {
            let contracts::MarketResolutionTerminalV3::Resolved {
                winning_asset_id,
                winning_outcome,
                payout_vector,
                mapping_rule_version,
            } = resolution;
            (condition_id.0.len()
                + resolution_event_id.len()
                + winning_asset_id.len()
                + winning_outcome.len()
                + mapping_rule_version.len()
                + redeemed_pusd.0.len()) as u64
                + payout_vector
                    .iter()
                    .map(|entry| {
                        (entry.asset.token_id.0.len()
                            + entry.asset.outcome.label.len()
                            + entry.payout_pusd_per_share.0.len()) as u64
                    })
                    .sum::<u64>()
                + redeemed_outcome_balances
                    .iter()
                    .map(|balance| {
                        (balance.token_id.0.len() + balance.balance_shares.0.len()) as u64
                    })
                    .sum::<u64>()
                + optional_account_delta_owned_bytes_v2(account_delta.as_ref())
        }
    }
}

/// 当前请求在原事务路径的效果；不描述此前请求或公开时间推进的效果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestEffectV1 {
    /// 请求构造／注册未成功；此调用没有登记或调度当前请求。
    NoRequestEffect,
    /// 原注册归约已接纳，后续提交失败不能保证没有请求效果。
    MayHaveRequestEffect,
}
#[derive(Debug)]
pub struct RequestFailureV1 {
    pub error: SimRunActorErrorV2,
    pub effect: RequestEffectV1,
}
impl RequestFailureV1 {
    pub fn before_request_effect(error: SimRunActorErrorV2) -> Self {
        Self {
            error,
            effect: RequestEffectV1::NoRequestEffect,
        }
    }
}

#[derive(Debug)]
pub struct UnpublishedRunArtifactCandidateV2 {
    run_id: String,
    log_path: PathBuf,
    accepted_input_count: u64,
    #[cfg(feature = "measurement")]
    measurement_observations: Vec<MeasurementObservation>,
}

impl UnpublishedRunArtifactCandidateV2 {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn accepted_input_count(&self) -> u64 {
        self.accepted_input_count
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn delete_after_retention(&self) -> Result<(), SimRunActorErrorV2> {
        crate::event_log::delete_retained_run_log_v2(&self.log_path)
            .map_err(SimRunActorErrorV2::Log)
    }

    #[cfg(feature = "measurement")]
    pub fn measurement_observations(&self) -> &[MeasurementObservation] {
        &self.measurement_observations
    }
}

pub(crate) struct SimRunActorV2 {
    opened: RunOpenedV2,
    _rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
    _finality_policy: Arc<TradeFinalityPolicyV2>,
    scenario: Arc<SealedPlatformScenarioV1>,
    _latency_profile: Arc<EmpiricalDelayProfile>,
    log_path: PathBuf,
    log: Option<SimEventLogV2>,
    // 仅未确认稳定的登记市场截止；成功稳定后删除，不保存历史市场清单。
    unstable_market_ends: BTreeSet<i64>,
    background_log: Option<BackgroundEventLogV1>,
    applied_input_sequence: u64,
    state: Option<BusinessReducerStateV2>,
    projection_port: Box<dyn BusinessProjectionPortV2>,
    pending_projections: VecDeque<PendingProjectionBatchV2>,

    #[cfg(feature = "measurement")]
    measurement_probe: Arc<MeasurementProbe>,
}

pub struct SimExecution {
    actor: SimRunActorV2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NextRequiredLogicalDriveV2 {
    pub path: MarketEventPathV1,
    pub required_at_utc_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequiredLogicalDriveScheduleV2 {
    gtd_at_utc_ms: Option<i64>,
    finality_at_utc_ms: Option<i64>,
    rebate_at_utc_ms: Option<i64>,
    settlement_at_utc_ms: Option<i64>,
    mutation_at_utc_ms: Option<i64>,
}

impl RequiredLogicalDriveScheduleV2 {
    fn earliest_at_utc_ms(self) -> Option<i64> {
        [
            self.gtd_at_utc_ms,
            self.finality_at_utc_ms,
            self.rebate_at_utc_ms,
            self.settlement_at_utc_ms,
            self.mutation_at_utc_ms,
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SimRunActorErrorV2 {
    Closed,
    RecordingBackpressured,
    PublicConsumptionSource(String),
    InvalidRunId,
    RuleSnapshotIdentityMismatch,
    FinalityPolicyIdentityMismatch,
    ScenarioIdentityMismatch,
    LatencyProfileIdentityMismatch,
    InvalidSessionSpec(&'static str),
    SessionAlreadyCreated,
    SessionNotCreated,
    SessionNotActive,
    InitializationAlreadyDecided,
    InvalidMarketEnrollment(&'static str),
    MarketEnrollmentFrozenAfterBusinessInput,
    MarketEnrollmentCandidateAlreadyUsed(String),
    MarketEnrollmentDirectory(SessionMarketDirectoryErrorV1),
    MarketEnrollmentSequenceOverflow,
    InvalidBusinessRequest(&'static str),
    InvalidRetentionAdvance,
    RequestTimeOverflow,
    MissingCurrentTick(String),
    HiddenPlatformReplayForbidden,
    MarketEvent(MarketEventErrorV1),
    Matching(MatchingStateErrorV1),
    Account(PlatformAccountError),
    Log(SimEventLogErrorV2),
    Noe(noe_projection::MemoryNoeErrorV1),
    ProjectionBackpressured,
    CompletionNotReady(&'static str),
}

impl std::fmt::Display for SimRunActorErrorV2 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PublicConsumptionSource(message) => {
                write!(formatter, "公共消费来源未完成：{message}")
            }
            Self::RecordingBackpressured => write!(formatter, "记录材料尚待交接，暂停供料"),
            Self::Noe(error) => write!(formatter, "{error}"),
            Self::Closed => write!(formatter, "运行根已经关闭"),
            Self::InvalidRunId => write!(formatter, "运行身份非法"),
            Self::RuleSnapshotIdentityMismatch => write!(formatter, "运行根规则文件身份不一致"),
            Self::FinalityPolicyIdentityMismatch => {
                write!(formatter, "运行根最终性策略文件身份不一致")
            }
            Self::ScenarioIdentityMismatch => write!(formatter, "运行根场景文件身份不一致"),
            Self::LatencyProfileIdentityMismatch => {
                write!(formatter, "运行根代理时延配置档身份不一致")
            }
            Self::InvalidSessionSpec(field) => write!(formatter, "会话创建字段非法：{field}"),
            Self::SessionAlreadyCreated => write!(formatter, "运行根已经拥有唯一会话"),
            Self::SessionNotCreated => write!(formatter, "运行根尚未创建会话"),
            Self::SessionNotActive => write!(formatter, "唯一会话尚未激活"),
            Self::InitializationAlreadyDecided => write!(formatter, "会话初始化已经形成相反终态"),
            Self::InvalidMarketEnrollment(message) => {
                write!(formatter, "市场登记控制请求非法：{message}")
            }
            Self::MarketEnrollmentFrozenAfterBusinessInput => {
                write!(
                    formatter,
                    "训练或离线评估会话已经接纳业务输入，市场目录已冻结"
                )
            }
            Self::MarketEnrollmentCandidateAlreadyUsed(identity) => {
                write!(formatter, "候选市场身份已经被业务请求使用：{identity}")
            }
            Self::MarketEnrollmentDirectory(error) => {
                write!(formatter, "市场登记目录冲突：{error}")
            }
            Self::MarketEnrollmentSequenceOverflow => write!(formatter, "市场登记序号溢出"),
            Self::InvalidBusinessRequest(message) => write!(formatter, "业务请求非法：{message}"),
            Self::InvalidRetentionAdvance => {
                write!(formatter, "保留水位必须单调且不得越过当前查询修订")
            }
            Self::RequestTimeOverflow => write!(formatter, "请求逻辑时点溢出"),
            Self::MissingCurrentTick(token_id) => {
                write!(formatter, "活动资产缺少当前 tick：{token_id}")
            }
            Self::HiddenPlatformReplayForbidden => write!(formatter, "Paper 会话禁止隐藏平台回放"),
            Self::MarketEvent(error) => write!(formatter, "市场事件失败：{error}"),
            Self::Matching(error) => write!(formatter, "执行状态失败：{error:?}"),
            Self::Account(error) => write!(formatter, "平台账户初始化失败：{error}"),
            Self::Log(error) => write!(formatter, "单一事件日志失败：{error}"),
            Self::ProjectionBackpressured => {
                write!(formatter, "唯一投影接收端口暂时不能接纳当前批次")
            }
            Self::CompletionNotReady(message) => write!(formatter, "运行完成门尚未满足：{message}"),
        }
    }
}

impl std::error::Error for SimRunActorErrorV2 {}

fn validate_run_dependencies_v2(
    opened: &RunOpenedV2,
    rule_snapshot: &SealedProcessRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
    scenario: &SealedPlatformScenarioV1,
    latency_profile: &EmpiricalDelayProfile,
) -> Result<(), SimRunActorErrorV2> {
    if !is_stable_text(&opened.run_id) {
        return Err(SimRunActorErrorV2::InvalidRunId);
    }
    if rule_snapshot.file_sha256() != &opened.platform_rule_snapshot_file_sha256 {
        return Err(SimRunActorErrorV2::RuleSnapshotIdentityMismatch);
    }
    if finality_policy.policy_id() != &opened.trade_finality_policy_file_sha256 {
        return Err(SimRunActorErrorV2::FinalityPolicyIdentityMismatch);
    }
    if scenario.file_sha256() != &opened.platform_scenario_file_sha256 {
        return Err(SimRunActorErrorV2::ScenarioIdentityMismatch);
    }
    if latency_profile.file_sha256() != &opened.proxy_latency_profile_file_sha256 {
        return Err(SimRunActorErrorV2::LatencyProfileIdentityMismatch);
    }
    Ok(())
}

struct ReplayedEventLogV2 {
    opened: RunOpenedV2,
    state: BusinessReducerStateV2,
    recovery: SimEventLogRecoveryV2,
    pending_projections: VecDeque<PendingProjectionBatchV2>,

    accepted_input_count: u64,
}

fn materialize_logged_input_v2<F>(
    input: LoggedBusinessInputV2,
    read_bucket: &mut F,
) -> Result<BusinessInputV2, SimRunActorErrorV2>
where
    F: FnMut(
        &DurableMarketBucketReferenceV2,
    ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
{
    Ok(match input {
        LoggedBusinessInputV2::CancelScenario(operation) => {
            BusinessInputV2::CancelScenario(operation)
        }
        LoggedBusinessInputV2::Settlement(operation) => BusinessInputV2::Settlement(operation),
        LoggedBusinessInputV2::HiddenStepBucket(_) => {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "隐藏桶恢复需要原记录读取器",
            ));
        }
        LoggedBusinessInputV2::PublicGroup(_) => {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "公共组恢复需要原记录读取器",
            ));
        }
        LoggedBusinessInputV2::PublicObserveTime(time) => {
            BusinessInputV2::PublicObserveTime(TrustedActorExecutionTimeV1(time))
        }
        LoggedBusinessInputV2::PublicRequest {
            cutoff,
            send_time,
            request,
        } => BusinessInputV2::PublicRequest {
            cutoff: cutoff.into_trusted(),
            send_time: send_time.map(TrustedActorExecutionTimeV1),
            request,
        },
        LoggedBusinessInputV2::PublicCloseTime(closure) => {
            BusinessInputV2::PublicCloseTime(closure.into_trusted())
        }
        LoggedBusinessInputV2::CreateSession(spec) => BusinessInputV2::CreateSession(spec),
        LoggedBusinessInputV2::ActivateSession { activation_id } => {
            BusinessInputV2::ActivateSession { activation_id }
        }
        LoggedBusinessInputV2::FailSessionInitialization { failure_id } => {
            BusinessInputV2::FailSessionInitialization { failure_id }
        }
        LoggedBusinessInputV2::EnrollMarket(request) => BusinessInputV2::EnrollMarket(request),
        LoggedBusinessInputV2::Request { cutoff, request } => {
            BusinessInputV2::Request { cutoff, request }
        }
        LoggedBusinessInputV2::PlatformPrefixAdvance(advance) => {
            BusinessInputV2::PlatformPrefixAdvance(advance)
        }
        LoggedBusinessInputV2::RetentionAdvance(advance) => {
            BusinessInputV2::RetentionAdvance(advance)
        }
        LoggedBusinessInputV2::ClosedMarketBucket(bucket) => {
            let events = read_bucket(&bucket.reference)?;
            BusinessInputV2::ClosedMarketBucket(AcceptedClosedMarketBucketV2 {
                reference: bucket.reference,
                events,
            })
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn replay_event_log_v2<F>(
    path: &Path,
    rule_snapshot: &SealedProcessRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
    scenario: &SealedPlatformScenarioV1,
    latency_profile: &EmpiricalDelayProfile,
    max_group_bytes: usize,
    projection_port: Option<&dyn BusinessProjectionPortV2>,
    mut read_bucket: F,
) -> Result<ReplayedEventLogV2, SimRunActorErrorV2>
where
    F: FnMut(
        &DurableMarketBucketReferenceV2,
    ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
{
    let (opened, mut recovery) =
        SimEventLogRecoveryV2::open(path, max_group_bytes).map_err(SimRunActorErrorV2::Log)?;
    validate_run_dependencies_v2(
        &opened,
        rule_snapshot,
        finality_policy,
        scenario,
        latency_profile,
    )?;
    let durable_through = projection_port.map(|port| port.durable_through(&opened.run_id));
    let mut state = BusinessReducerStateV2 {
        session: None,
        public_pm_evidence: BTreeMap::new(),
        pm_materials: Vec::new(),
    };
    let mut pending_projections = VecDeque::new();

    let mut input_sequence = 1_u64;
    loop {
        let (next, group) = recovery.next_group().map_err(SimRunActorErrorV2::Log)?;
        recovery = next;
        let Some(group) = group else {
            break;
        };
        for logged in group {
            let input = materialize_logged_input_v2(logged, &mut read_bucket)?;
            let reduced = reduce_actor_input_v2(
                &opened,
                scenario.initial_market_directory(),
                state,
                &input,
                input_sequence,
                latency_profile,
                rule_snapshot.snapshot(),
                finality_policy,
            );
            let (next_state, output) = match reduced {
                Ok(result) => result,
                Err(BusinessReductionErrorV2::Unaccepted { error, .. })
                | Err(BusinessReductionErrorV2::Fatal(error)) => return Err(error),
            };
            let (next_state, _, projection) =
                finalize_projection_batch_v2(&opened.run_id, next_state, output, latency_profile)?;
            state = next_state;
            state.pm_materials.clear();
            if let Some(session) = &mut state.session {
                session.execution_state.cancel_scenario_hits.clear();
            }
            if let Some(projection) = projection
                && durable_through
                    .is_some_and(|watermark| projection.projection_batch_sequence > watermark)
            {
                let projection = projection.into_payload(opened.projection_version);

                pending_projections.push_back(PendingProjectionBatchV2 {
                    batch: Arc::new(projection),

                    accepted: false,
                });
            }
            input_sequence = input_sequence
                .checked_add(1)
                .expect("有界恢复输入序号不得溢出");
        }
    }
    Ok(ReplayedEventLogV2 {
        opened,
        state,
        recovery,
        pending_projections,

        accepted_input_count: input_sequence - 1,
    })
}

impl SimExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn create_new(
        path: &Path,
        opened: RunOpenedV2,
        rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        projection_port: Box<dyn BusinessProjectionPortV2>,
        max_group_bytes: usize,
    ) -> Result<Self, SimRunActorErrorV2> {
        SimRunActorV2::create_new(
            path,
            opened,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            projection_port,
            max_group_bytes,
        )
        .map(|actor| Self { actor })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn recover_paper<F>(
        path: &Path,
        rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        projection_port: Box<dyn BusinessProjectionPortV2>,
        max_group_bytes: usize,
        read_bucket: F,
    ) -> Result<Self, SimRunActorErrorV2>
    where
        F: FnMut(
            &DurableMarketBucketReferenceV2,
        ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
    {
        #[cfg(feature = "measurement")]
        let recovery_started = Instant::now();
        let actor = SimRunActorV2::recover_paper(
            path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            projection_port,
            max_group_bytes,
            read_bucket,
        )?;
        #[cfg(feature = "measurement")]
        actor.measurement_probe.record(
            MeasurementContext::Recovery,
            MeasurementPhase::RecoveryReplay,
            elapsed_ns_v2(recovery_started),
        );
        Ok(Self { actor })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn recover_sealed_clean_run<F>(
        path: &Path,
        rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        max_group_bytes: usize,
        read_bucket: F,
    ) -> Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2>
    where
        F: FnMut(
            &DurableMarketBucketReferenceV2,
        ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
    {
        SimRunActorV2::recover_sealed_clean_run(
            path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            max_group_bytes,
            read_bucket,
        )
    }

    pub fn create_session(
        &mut self,
        spec: SessionSpecV3,
    ) -> Result<CreateSessionControlResultV2, SimRunActorErrorV2> {
        self.actor.create_session(spec)
    }

    pub fn activate_session(
        &mut self,
        activation_id: String,
    ) -> Result<SessionInitializationControlResultV2, SimRunActorErrorV2> {
        self.actor.activate_session(activation_id)
    }

    pub fn fail_session_initialization(
        &mut self,
        failure_id: String,
    ) -> Result<SessionInitializationControlResultV2, SimRunActorErrorV2> {
        self.actor.fail_session_initialization(failure_id)
    }

    pub fn enroll_market(
        &mut self,
        request: MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV2, SimRunActorErrorV2> {
        self.actor.enroll_market(request)
    }

    pub fn post_order(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::PostOrderRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::PostOrder(request))
    }

    pub fn post_orders(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::PostOrdersRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::PostOrders(request))
    }

    pub fn cancel_order(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::CancelOrderRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::CancelOrder(request))
    }

    pub fn cancel_orders(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::CancelOrdersRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::CancelOrders(request))
    }

    pub fn cancel_market_orders(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::CancelMarketOrdersRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::CancelMarketOrders(request))
    }

    pub fn cancel_all_orders(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::CancelAllOrdersRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::CancelAllOrders(request))
    }

    pub fn order(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::OrderRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::Order(request))
    }

    pub fn orders(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::OrdersRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::Orders(request))
    }

    pub fn trades(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: crate::runtime_v2::TradesRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.actor
            .register_request(cutoff, BusinessRequestV2::Trades(request))
    }

    pub fn drive_platform_prefix(
        &mut self,
        advance: PlatformPrefixAdvanceV2,
    ) -> Result<(), SimRunActorErrorV2> {
        self.actor.drive_platform_prefix(advance)
    }

    pub fn next_required_logical_drive(&self) -> Option<NextRequiredLogicalDriveV2> {
        self.actor.next_required_logical_drive()
    }

    pub fn drive_closed_market_bucket(
        &mut self,
        bucket: AcceptedClosedMarketBucketV2,
    ) -> Result<(), SimRunActorErrorV2> {
        self.actor.drive_closed_market_bucket(bucket)
    }

    pub fn advance_retention(
        &mut self,
        advance: RetentionAdvanceV2,
    ) -> Result<(), SimRunActorErrorV2> {
        self.actor.advance_retention(advance)
    }

    pub fn complete_clean_run(
        self,
    ) -> Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2> {
        self.actor.complete_clean_run()
    }

    #[cfg(feature = "measurement")]
    pub fn drain_measurement_observations(&self) -> Vec<MeasurementObservation> {
        self.actor.measurement_probe.drain()
    }

    #[cfg(feature = "measurement")]
    pub fn measurement_resource_snapshot(&self) -> MeasurementResourceSnapshot {
        self.actor.measurement_resource_snapshot()
    }

    #[cfg(test)]
    fn inject_log_write_failure_for_tests(&mut self) {
        self.actor.inject_log_write_failure_for_tests();
    }

    #[cfg(test)]
    fn inject_log_sync_failure_for_tests(&mut self) {
        self.actor.inject_log_sync_failure_for_tests();
    }
}

impl SimRunActorV2 {
    fn next_required_logical_drive(&self) -> Option<NextRequiredLogicalDriveV2> {
        next_required_logical_drive_v2(self.state.as_ref()?, self._rule_snapshot.snapshot())
    }

    pub(crate) fn create_new(
        path: &Path,
        opened: RunOpenedV2,
        rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        projection_port: Box<dyn BusinessProjectionPortV2>,
        max_group_bytes: usize,
    ) -> Result<Self, SimRunActorErrorV2> {
        validate_run_dependencies_v2(
            &opened,
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        )?;
        let log = SimEventLogV2::create(path, &opened, max_group_bytes)
            .map_err(SimRunActorErrorV2::Log)?;
        #[cfg(feature = "measurement")]
        let measurement_probe = log.measurement_probe();
        Ok(Self {
            opened,
            _rule_snapshot: rule_snapshot,
            _finality_policy: finality_policy,
            scenario,
            _latency_profile: latency_profile,
            log_path: path.to_path_buf(),
            log: Some(log),
            background_log: None,
            unstable_market_ends: BTreeSet::new(),
            applied_input_sequence: 1,
            state: Some(BusinessReducerStateV2 {
                session: None,
                public_pm_evidence: BTreeMap::new(),
                pm_materials: Vec::new(),
            }),
            projection_port,
            pending_projections: VecDeque::new(),

            #[cfg(feature = "measurement")]
            measurement_probe,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_paper<F>(
        path: &Path,
        rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        projection_port: Box<dyn BusinessProjectionPortV2>,
        max_group_bytes: usize,
        read_bucket: F,
    ) -> Result<Self, SimRunActorErrorV2>
    where
        F: FnMut(
            &DurableMarketBucketReferenceV2,
        ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
    {
        let replayed = replay_event_log_v2(
            path,
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
            max_group_bytes,
            Some(projection_port.as_ref()),
            read_bucket,
        )?;
        if replayed.opened.durability_role != RunDurabilityRoleV2::RecoverablePaper {
            return Err(SimRunActorErrorV2::Log(
                SimEventLogErrorV2::InvalidStructure("只有 RecoverablePaper 可以恢复业务"),
            ));
        }
        let log = replayed
            .recovery
            .resume_paper()
            .map_err(SimRunActorErrorV2::Log)?;
        #[cfg(feature = "measurement")]
        let measurement_probe = log.measurement_probe();
        let mut actor = Self {
            opened: replayed.opened,
            _rule_snapshot: rule_snapshot,
            _finality_policy: finality_policy,
            scenario,
            _latency_profile: latency_profile,
            log_path: path.to_path_buf(),
            log: Some(log),
            background_log: None,
            unstable_market_ends: BTreeSet::new(),
            applied_input_sequence: 1,
            state: Some(replayed.state),
            projection_port,
            pending_projections: replayed.pending_projections,

            #[cfg(feature = "measurement")]
            measurement_probe,
        };
        actor.deliver_pending_projections();
        Ok(actor)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_sealed_clean_run<F>(
        path: &Path,
        rule_snapshot: Arc<SealedProcessRuleSnapshotV3>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        max_group_bytes: usize,
        read_bucket: F,
    ) -> Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2>
    where
        F: FnMut(
            &DurableMarketBucketReferenceV2,
        ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>,
    {
        #[cfg(feature = "measurement")]
        let recovery_started = Instant::now();
        let replayed = replay_event_log_v2(
            path,
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
            max_group_bytes,
            None,
            read_bucket,
        )?;
        if replayed.opened.durability_role != RunDurabilityRoleV2::CleanRun {
            return Err(SimRunActorErrorV2::Log(
                SimEventLogErrorV2::InvalidStructure("只有 CleanRun 可以形成冷产物候选"),
            ));
        }
        replayed
            .recovery
            .finish_sealed_clean_run()
            .map_err(SimRunActorErrorV2::Log)?;
        Ok(UnpublishedRunArtifactCandidateV2 {
            run_id: replayed.opened.run_id,
            log_path: path.to_path_buf(),
            accepted_input_count: replayed.accepted_input_count,
            #[cfg(feature = "measurement")]
            measurement_observations: vec![MeasurementObservation {
                context: MeasurementContext::Recovery,
                phase: MeasurementPhase::RecoveryReplay,
                elapsed_ns: elapsed_ns_v2(recovery_started),
            }],
        })
    }

    pub(crate) fn complete_clean_run(
        mut self,
    ) -> Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2> {
        if self.opened.durability_role != RunDurabilityRoleV2::CleanRun {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "RecoverablePaper 不使用 RunSeal",
            ));
        }
        self.deliver_pending_projections();
        if !self.pending_projections.is_empty() {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "仍有未被消费端确认的投影批次",
            ));
        }
        let state = self.state.as_ref().ok_or(SimRunActorErrorV2::Closed)?;
        let session = state
            .session
            .as_ref()
            .ok_or(SimRunActorErrorV2::CompletionNotReady("唯一会话尚未创建"))?;
        let needs_business_completion = match session.initialization {
            SessionInitializationV2::PreparedLedgerInit => {
                return Err(SimRunActorErrorV2::CompletionNotReady(
                    "会话初始化决定尚未完成",
                ));
            }
            SessionInitializationV2::Active { .. } => true,
            SessionInitializationV2::InitializationFailedClosed { .. } => false,
        };
        let input_exhausted = match session.opening_fact.account_instance_key.caller_role {
            SimCallerRoleV1::Paper => session.public_visibility_frontier.sealed,
            SimCallerRoleV1::ModelTrain
            | SimCallerRoleV1::OfflineEvaluation
            | SimCallerRoleV1::HistoricalBacktest => {
                session.hidden_platform_frontier.sealed && session.public_visibility_frontier.sealed
            }
        };
        if needs_business_completion && !input_exhausted {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "市场输入路径尚未全部成功结束",
            ));
        }
        if needs_business_completion
            && (next_scheduled_obligation_at_v2(state, self._rule_snapshot.snapshot()).is_some()
                || !session.mutation_requests.pending_actions.is_empty()
                || session
                    .mutation_requests
                    .requests_by_id
                    .values()
                    .any(|request| request.remaining_action_count != 0))
        {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "仍有已经登记但尚未闭合的业务义务",
            ));
        }
        if needs_business_completion
            && (!session.public_markets.all_qualified_resolutions_terminal()
                || session
                    .hidden_platform_markets
                    .as_ref()
                    .is_some_and(|markets| !markets.all_qualified_resolutions_terminal()))
        {
            return Err(SimRunActorErrorV2::CompletionNotReady(
                "合格市场判定尚未完成既有因果闭合",
            ));
        }
        let accepted_input_count = if let Some(background) = self.background_log.take() {
            background.finish().map_err(SimRunActorErrorV2::Log)?;
            self.applied_input_sequence - 1
        } else {
            let log = self.log.take().ok_or(SimRunActorErrorV2::Closed)?;
            let accepted_input_count = log.next_input_sequence() - 1;
            log.seal_clean_run().map_err(SimRunActorErrorV2::Log)?;
            accepted_input_count
        };
        Ok(UnpublishedRunArtifactCandidateV2 {
            run_id: self.opened.run_id,
            log_path: self.log_path,
            accepted_input_count,
            #[cfg(feature = "measurement")]
            measurement_observations: self.measurement_probe.drain(),
        })
    }

    pub(crate) fn create_session(
        &mut self,
        spec: SessionSpecV3,
    ) -> Result<CreateSessionControlResultV2, SimRunActorErrorV2> {
        let state = self.state.as_ref().ok_or(SimRunActorErrorV2::Closed)?;
        if let Some(session) = state.session.as_ref() {
            if session.opening_fact.creation_request_id == spec.creation_request_id {
                return Ok(CreateSessionControlResultV2 {
                    disposition: CreateSessionDispositionV2::ExistingPrepared,
                    opening_fact: Arc::clone(&session.opening_fact),
                });
            }
        }

        let mut committed = self.commit_group(vec![BusinessInputV2::CreateSession(spec)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::CreateSession(result)) = committed.outputs.pop() else {
            unreachable!("CreateSession 归约只能返回创建结果")
        };
        Ok(result)
    }

    pub(crate) fn activate_session(
        &mut self,
        activation_id: String,
    ) -> Result<SessionInitializationControlResultV2, SimRunActorErrorV2> {
        let state = self.state.as_ref().ok_or(SimRunActorErrorV2::Closed)?;
        if let Some(UniqueSessionStateV2 {
            initialization:
                SessionInitializationV2::Active {
                    activation_id: existing,
                },
            ..
        }) = state.session.as_ref()
        {
            if *existing == activation_id {
                return Ok(SessionInitializationControlResultV2::ExistingActive);
            }
        }

        let mut committed =
            self.commit_group(vec![BusinessInputV2::ActivateSession { activation_id }])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::SessionInitialization(result)) =
            committed.outputs.pop()
        else {
            unreachable!("ActivateSession 归约只能返回初始化结果")
        };
        Ok(result)
    }

    pub(crate) fn fail_session_initialization(
        &mut self,
        failure_id: String,
    ) -> Result<SessionInitializationControlResultV2, SimRunActorErrorV2> {
        let state = self.state.as_ref().ok_or(SimRunActorErrorV2::Closed)?;
        if let Some(UniqueSessionStateV2 {
            initialization:
                SessionInitializationV2::InitializationFailedClosed {
                    failure_id: existing,
                },
            ..
        }) = state.session.as_ref()
        {
            if *existing == failure_id {
                return Ok(SessionInitializationControlResultV2::ExistingFailedClosed);
            }
        }

        let mut committed =
            self.commit_group(vec![BusinessInputV2::FailSessionInitialization {
                failure_id,
            }])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::SessionInitialization(result)) =
            committed.outputs.pop()
        else {
            unreachable!("FailSessionInitialization 归约只能返回初始化结果")
        };
        Ok(result)
    }

    pub(crate) fn enroll_market(
        &mut self,
        request: MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV2, SimRunActorErrorV2> {
        let state = self.state.as_ref().ok_or(SimRunActorErrorV2::Closed)?;
        if let Some(existing) = state.session.as_ref().and_then(|session| {
            session
                .market_enrollment
                .successful(&request.enrollment_request_id)
        }) {
            return Ok(existing.clone());
        }

        let mut committed = self.commit_group(vec![BusinessInputV2::EnrollMarket(request)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::MarketEnrollment(result)) = committed.outputs.pop()
        else {
            unreachable!("EnrollMarket 归约只能返回市场登记结果")
        };
        Ok(result)
    }

    pub(crate) fn register_request(
        &mut self,
        cutoff: TrustedDecisionCutoffV2,
        request: BusinessRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        #[cfg(feature = "measurement")]
        let public_started = Instant::now();
        if let Some(existing) = existing_mutation_request_receipt_v2(
            self.state.as_ref().ok_or(SimRunActorErrorV2::Closed)?,
            &request.context().global_request_id,
        ) {
            #[cfg(feature = "measurement")]
            self.measurement_probe.record(
                MeasurementContext::PublicRequest,
                MeasurementPhase::PublicEndToEnd,
                elapsed_ns_v2(public_started),
            );
            return Ok(existing);
        }
        let mut committed =
            self.commit_group(vec![BusinessInputV2::Request { cutoff, request }])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::RequestRegistration(result)) = committed.outputs.pop()
        else {
            unreachable!("Request 归约只能返回首次接收回执")
        };
        #[cfg(feature = "measurement")]
        self.measurement_probe.record(
            MeasurementContext::PublicRequest,
            MeasurementPhase::PublicEndToEnd,
            elapsed_ns_v2(public_started),
        );
        Ok(result)
    }

    pub(crate) fn drive_platform_prefix(
        &mut self,
        advance: PlatformPrefixAdvanceV2,
    ) -> Result<(), SimRunActorErrorV2> {
        let mut committed =
            self.commit_group(vec![BusinessInputV2::PlatformPrefixAdvance(advance)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::Driven) = committed.outputs.pop() else {
            unreachable!("平台前缀归约只能返回驱动完成")
        };
        Ok(())
    }

    pub(crate) fn drive_closed_market_bucket(
        &mut self,
        bucket: AcceptedClosedMarketBucketV2,
    ) -> Result<(), SimRunActorErrorV2> {
        let mut committed = self.commit_group(vec![BusinessInputV2::ClosedMarketBucket(bucket)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::Driven) = committed.outputs.pop() else {
            unreachable!("已封口市场桶归约只能返回驱动完成")
        };
        Ok(())
    }

    pub(crate) fn advance_retention(
        &mut self,
        advance: RetentionAdvanceV2,
    ) -> Result<(), SimRunActorErrorV2> {
        let mut committed = self.commit_group(vec![BusinessInputV2::RetentionAdvance(advance)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let Some(CommittedBusinessOutputV2::Driven) = committed.outputs.pop() else {
            unreachable!("保留水位归约只能返回推进完成")
        };
        Ok(())
    }

    pub(crate) fn commit_group(
        &mut self,
        inputs: Vec<BusinessInputV2>,
    ) -> Result<BusinessGroupCommitV2, SimRunActorErrorV2> {
        self.commit_group_with_request_effect(inputs)
            .map_err(|failure| failure.error)
    }

    fn commit_group_with_request_effect(
        &mut self,
        inputs: Vec<BusinessInputV2>,
    ) -> Result<BusinessGroupCommitV2, RequestFailureV1> {
        let mut request_effect = RequestEffectV1::NoRequestEffect;
        self.commit_group_inner(inputs, &mut request_effect)
            .map_err(|error| RequestFailureV1 {
                error,
                effect: request_effect,
            })
    }

    fn commit_group_inner(
        &mut self,
        mut inputs: Vec<BusinessInputV2>,
        request_effect: &mut RequestEffectV1,
    ) -> Result<BusinessGroupCommitV2, SimRunActorErrorV2> {
        self.deliver_pending_projections();
        if self
            .pending_projections
            .iter()
            .any(|pending| !pending.accepted)
        {
            return Err(SimRunActorErrorV2::ProjectionBackpressured);
        }
        let start_sequence = if let Some(background) = &mut self.background_log {
            if !background.ready().map_err(SimRunActorErrorV2::Log)? {
                return Err(SimRunActorErrorV2::RecordingBackpressured);
            }
            self.applied_input_sequence
        } else {
            self.log
                .as_ref()
                .ok_or(SimRunActorErrorV2::Closed)?
                .next_input_sequence()
        };
        let old_closed = self
            .state
            .as_ref()
            .and_then(|s| s.session.as_ref())
            .and_then(|s| s.recording_closed_through());
        let mut state = self.take_state()?;
        let mut outputs = Vec::with_capacity(inputs.len());
        let mut projections = Vec::new();
        let mut unaccepted = None;
        for (index, input) in inputs.iter_mut().enumerate() {
            let input_sequence = start_sequence
                .checked_add(index as u64)
                .expect("有界输入组序号不得溢出");
            #[cfg(feature = "measurement")]
            let reducer_started = Instant::now();
            let reduced = reduce_actor_input_v2(
                &self.opened,
                self.scenario.initial_market_directory(),
                state,
                input,
                input_sequence,
                &self._latency_profile,
                self._rule_snapshot.snapshot(),
                &self._finality_policy,
            );
            #[cfg(feature = "measurement")]
            self.measurement_probe.record(
                MeasurementContext::CommitGroup,
                MeasurementPhase::Reducer,
                elapsed_ns_v2(reducer_started),
            );
            let (mut next_state, output) = match reduced {
                Ok(result) => result,
                Err(BusinessReductionErrorV2::Unaccepted {
                    state: returned,
                    error,
                }) => {
                    state = returned;
                    unaccepted = Some(error);
                    break;
                }
                Err(BusinessReductionErrorV2::Fatal(error)) => return Err(error),
            };
            if matches!(
                input,
                BusinessInputV2::PublicRequest { .. } | BusinessInputV2::Request { .. }
            ) {
                *request_effect = RequestEffectV1::MayHaveRequestEffect;
            }
            if let BusinessInputV2::PublicGroup(group) = input {
                group.pm_materials = std::mem::take(&mut next_state.pm_materials);
            }
            #[cfg(feature = "measurement")]
            let projection_started = Instant::now();
            let (next_state, output, projection) = finalize_projection_batch_v2(
                &self.opened.run_id,
                next_state,
                output,
                &self._latency_profile,
            )?;
            #[cfg(feature = "measurement")]
            self.measurement_probe.record(
                MeasurementContext::CommitGroup,
                MeasurementPhase::ProjectionFinalize,
                elapsed_ns_v2(projection_started),
            );
            state = next_state;
            outputs.push(output);
            if let Some(projection) = projection {
                projections.push(projection);
            }
        }
        if outputs.is_empty() {
            self.state = Some(state);
            return Ok(BusinessGroupCommitV2 {
                outputs,
                unaccepted,
            });
        }
        let accepted_len = outputs.len();
        let cancel_scenario_hits = state
            .session
            .as_mut()
            .map(|session| std::mem::take(&mut session.execution_state.cancel_scenario_hits))
            .unwrap_or_default();
        if let Some(background) = &mut self.background_log {
            self.state = Some(state);
            self.applied_input_sequence += accepted_len as u64;
            if let Some(session) = self.state.as_ref().and_then(|s| s.session.as_ref()) {
                for input in &inputs[..accepted_len] {
                    match input {
                        BusinessInputV2::CreateSession(_) => self.unstable_market_ends.extend(
                            session
                                .market_directory
                                .markets()
                                .map(|m| m.market().window.end_at_utc_ms),
                        ),
                        BusinessInputV2::EnrollMarket(request) => {
                            self.unstable_market_ends
                                .insert(request.market.market().window.end_at_utc_ms);
                        }
                        _ => {}
                    }
                }
            }
            let new_closed = self
                .state
                .as_ref()
                .and_then(|s| s.session.as_ref())
                .and_then(|s| s.recording_closed_through());
            let mut accepted = inputs;
            accepted.truncate(accepted_len);
            let mut normalized = Vec::new();
            let mut normalization_error = None;
            if self.projection_port.uses_background_normalization() {
                for projection in projections.drain(..) {
                    match self.projection_port.accept_for_background(Arc::new(
                        projection.into_payload(self.opened.projection_version),
                    )) {
                        Ok(batch) => normalized.push(batch),
                        Err(error) => {
                            normalization_error = Some(error);
                            break;
                        }
                    }
                }
            }
            // 现有平台前缀闭合后，非负出站时延保证此接收时间之前不再产生遗漏反馈。
            // Paper 使用公共平台路径；训练/离线使用隐藏平台路径，与是否存在同毫秒公共组无关。
            if let Some(session) = self.state.as_ref().and_then(|state| state.session.as_ref()) {
                let frontier = match session.opening_fact.account_instance_key.caller_role {
                    SimCallerRoleV1::Paper => &session.public_visibility_frontier,
                    SimCallerRoleV1::ModelTrain
                    | SimCallerRoleV1::OfflineEvaluation
                    | SimCallerRoleV1::HistoricalBacktest => &session.hidden_platform_frontier,
                };
                if let Some(at) = frontier.closed_through_utc_ms {
                    self.projection_port.close_receive_prefix(at);
                }
            }
            background
                .enqueue_with_noe(
                    accepted,
                    normalized,
                    normalization_error.clone(),
                    cancel_scenario_hits,
                )
                .map_err(SimRunActorErrorV2::Log)?;
            if let Some(error) = normalization_error {
                return Err(SimRunActorErrorV2::Noe(error));
            }
            if new_closed != old_closed
                && let Some(at) = new_closed.filter(|at| {
                    self.unstable_market_ends
                        .first()
                        .is_some_and(|end| end <= at)
                })
            {
                background.stabilize().map_err(SimRunActorErrorV2::Log)?;
                self.unstable_market_ends.retain(|end| *end > at);
            }
        } else {
            let log = self.log.take().ok_or(SimRunActorErrorV2::Closed)?;
            let (log, _) = log
                .append_group_with_hits(&inputs[..accepted_len], &cancel_scenario_hits)
                .map_err(SimRunActorErrorV2::Log)?;
            self.log = Some(log);
            self.state = Some(state);
        }
        #[cfg(test)]
        record_group_state_install_for_tests();
        #[cfg(feature = "measurement")]
        let had_projections = !projections.is_empty();
        for projection in projections {
            let projection = projection.into_payload(self.opened.projection_version);

            self.pending_projections
                .push_back(PendingProjectionBatchV2 {
                    batch: Arc::new(projection),

                    accepted: false,
                });
        }
        #[cfg(feature = "measurement")]
        let delivery_started = had_projections.then(Instant::now);
        self.deliver_pending_projections();
        #[cfg(feature = "measurement")]
        if let Some(started) = delivery_started {
            self.measurement_probe.record(
                MeasurementContext::CommitGroup,
                MeasurementPhase::ProjectionDelivery,
                elapsed_ns_v2(started),
            );
        }
        Ok(BusinessGroupCommitV2 {
            outputs,
            unaccepted,
        })
    }

    fn deliver_pending_projections(&mut self) {
        let durable_through = self.projection_port.durable_through(&self.opened.run_id);
        while self
            .pending_projections
            .front()
            .is_some_and(|pending| pending.accepted && pending.batch.sequence() <= durable_through)
        {
            self.pending_projections
                .pop_front()
                .expect("已确认投影前缀存在");
        }
        for pending in &mut self.pending_projections {
            if pending.accepted {
                continue;
            }
            if !self
                .projection_port
                .try_accept_payload(Arc::clone(&pending.batch))
            {
                break;
            }
            pending.accepted = true;
        }
        let durable_through = self.projection_port.durable_through(&self.opened.run_id);
        while self
            .pending_projections
            .front()
            .is_some_and(|pending| pending.accepted && pending.batch.sequence() <= durable_through)
        {
            self.pending_projections
                .pop_front()
                .expect("已确认投影前缀存在");
        }
    }

    #[cfg(feature = "measurement")]
    fn measurement_resource_snapshot(&self) -> MeasurementResourceSnapshot {
        let business = self
            .state
            .as_ref()
            .map_or(ResourceQuantityV1::default(), |state| {
                business_resource_snapshot_v2(state)
            });
        let (log_byte_count, next_input_sequence) = self
            .log
            .as_ref()
            .map_or((0, 0), |log| (log.byte_count(), log.next_input_sequence()));
        MeasurementResourceSnapshot::new(
            business,
            self.pending_projections.iter().fold(
                ResourceQuantityV1::default(),
                |mut total, pending| {
                    total.include(projection_resource_quantity_v2(&pending.batch));
                    total
                },
            ),
            log_byte_count,
            next_input_sequence,
        )
    }

    fn take_state(&mut self) -> Result<BusinessReducerStateV2, SimRunActorErrorV2> {
        if self.log.is_none() && self.background_log.is_none() {
            return Err(SimRunActorErrorV2::Closed);
        }
        self.state.take().ok_or(SimRunActorErrorV2::Closed)
    }

    #[cfg(test)]
    fn next_input_sequence_for_tests(&self) -> Option<u64> {
        self.log.as_ref().map(SimEventLogV2::next_input_sequence)
    }

    #[cfg(test)]
    fn inject_log_write_failure_for_tests(&mut self) {
        self.log
            .as_mut()
            .expect("活动演员必须持有日志")
            .inject_write_failure_for_tests();
    }

    #[cfg(test)]
    fn inject_log_sync_failure_for_tests(&mut self) {
        self.log
            .as_mut()
            .expect("活动演员必须持有日志")
            .inject_sync_failure_for_tests();
    }
}

fn checked_add_delay_v2(base_utc_ms: i64, delay_ms: u64) -> Result<i64, SimRunActorErrorV2> {
    base_utc_ms
        .checked_add(i64::try_from(delay_ms).map_err(|_| SimRunActorErrorV2::RequestTimeOverflow)?)
        .ok_or(SimRunActorErrorV2::RequestTimeOverflow)
}

#[cfg(feature = "measurement")]
fn elapsed_ns_v2(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn validate_request_context_v2(request: &BusinessRequestV2) -> Result<(), SimRunActorErrorV2> {
    let context = request.context();
    if !is_stable_text(&context.global_request_id) {
        return Err(SimRunActorErrorV2::InvalidBusinessRequest(
            "global_request_id 为空或含首尾空白",
        ));
    }
    if context
        .caller_correlation_id
        .as_deref()
        .is_some_and(|identity| !is_stable_text(identity))
    {
        return Err(SimRunActorErrorV2::InvalidBusinessRequest(
            "caller_correlation_id 为空或含首尾空白",
        ));
    }
    Ok(())
}

fn validate_post_item_v2(item: &PostOrderItemV1) -> Result<(), SimRunActorErrorV2> {
    if !is_stable_text(&item.item_id)
        || !is_stable_text(&item.order.condition_id.0)
        || !is_stable_text(&item.order.token_id.0)
    {
        return Err(SimRunActorErrorV2::InvalidBusinessRequest(
            "提交项、市场或资产身份为空或含首尾空白",
        ));
    }
    let positive = |first: rust_decimal::Decimal, second: rust_decimal::Decimal| {
        first > rust_decimal::Decimal::ZERO && second > rust_decimal::Decimal::ZERO
    };
    let valid = match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            ..
        } => positive(*limit_price_pusd_per_share, *size_shares),
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            expires_at_utc_ms,
            ..
        } => positive(*limit_price_pusd_per_share, *size_shares) && *expires_at_utc_ms > 0,
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        }
        | SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => positive(*spend_pusd, *worst_price_pusd_per_share),
    };
    if !valid {
        return Err(SimRunActorErrorV2::InvalidBusinessRequest(
            "订单金额、份额、价格及 GTD 到期时点必须为正值",
        ));
    }
    Ok(())
}

fn validate_platform_order_id_v2(platform_order_id: &str) -> bool {
    let Some(sequence) = platform_order_id.strip_prefix("sim-") else {
        return false;
    };
    let Ok(sequence_number) = sequence.parse::<u64>() else {
        return false;
    };
    sequence_number > 0 && sequence_number.to_string() == sequence
}

fn validate_cancel_item_v2(item: &CancelOrderItemV1) -> Result<(), SimRunActorErrorV2> {
    if !is_stable_text(&item.item_id) || !validate_platform_order_id_v2(&item.platform_order_id) {
        return Err(SimRunActorErrorV2::InvalidBusinessRequest(
            "撤单项身份或 V2 平台订单身份非法",
        ));
    }
    Ok(())
}

#[cfg(feature = "measurement")]
fn post_action_owned_bytes_v2(global_request_id: &str, item: &PostOrderItemV1) -> u64 {
    (global_request_id.len()
        + item.item_id.len()
        + item.order.condition_id.0.len()
        + item.order.token_id.0.len()) as u64
}

#[cfg(feature = "measurement")]
fn submission_measurement_owned_bytes_v2(measurement: &contracts::SubmissionMeasurementV2) -> u64 {
    match measurement {
        contracts::SubmissionMeasurementV2::Base {
            accepted_base_quantity_shares,
            matched_base_quantity_shares,
            open_base_quantity_shares,
            venue_dust_closed_base_quantity_shares,
            order_type_terminated_base_quantity_shares,
            accepted_quote_amount_pusd,
            matched_quote_amount_pusd,
            open_encumbered_quote_amount_pusd,
            venue_dust_released_quote_amount_pusd,
            released_price_improvement_quote_amount_pusd,
            immediate_taker_fee_encumbered_pusd,
        } => [
            accepted_base_quantity_shares,
            matched_base_quantity_shares,
            open_base_quantity_shares,
            venue_dust_closed_base_quantity_shares,
            order_type_terminated_base_quantity_shares,
            accepted_quote_amount_pusd,
            matched_quote_amount_pusd,
            open_encumbered_quote_amount_pusd,
            venue_dust_released_quote_amount_pusd,
            released_price_improvement_quote_amount_pusd,
            immediate_taker_fee_encumbered_pusd,
        ]
        .into_iter()
        .map(|value| value.0.len() as u64)
        .sum(),
        contracts::SubmissionMeasurementV2::QuoteBudget {
            accepted_quote_budget_pusd,
            used_quote_amount_pusd,
            released_or_unspent_quote_amount_pusd,
            actual_matched_base_quantity_shares,
            sdk_taker_amount_audit_base_quantity_shares,
            immediate_taker_fee_encumbered_pusd,
        } => [
            accepted_quote_budget_pusd,
            used_quote_amount_pusd,
            released_or_unspent_quote_amount_pusd,
            actual_matched_base_quantity_shares,
            sdk_taker_amount_audit_base_quantity_shares,
            immediate_taker_fee_encumbered_pusd,
        ]
        .into_iter()
        .map(|value| value.0.len() as u64)
        .sum(),
    }
}

#[cfg(feature = "measurement")]
fn cancel_action_owned_bytes_v2(
    global_request_id: &str,
    platform_order_id: &str,
    input_item_refs: &[CancelInputItemRefV2],
) -> u64 {
    (global_request_id.len()
        + platform_order_id.len()
        + input_item_refs
            .iter()
            .map(|reference| reference.item_id.len())
            .sum::<usize>()) as u64
}

#[cfg(feature = "measurement")]
fn cancel_market_action_owned_bytes_v2(global_request_id: &str, condition_id: &ConditionId) -> u64 {
    (global_request_id.len() + condition_id.0.len()) as u64
}

#[cfg(feature = "measurement")]
fn cancel_result_owned_bytes_v2<R>(effect: &CancelTargetBusinessEffect<R>) -> u64 {
    let outcome_bytes = match &effect.outcome {
        contracts::CancelTargetOutcome::Canceled(canceled) => {
            canceled.canceled_base_quantity_shares.0.len()
                + canceled.released_quote_amount_pusd.0.len()
        }
        contracts::CancelTargetOutcome::NotCanceledNoEffect(_)
        | contracts::CancelTargetOutcome::EvidenceUnknownNoEffect => 0,
    };
    (effect.platform_order_id.len()
        + effect
            .input_item_refs
            .iter()
            .map(|reference| reference.item_id.len())
            .sum::<usize>()
        + outcome_bytes) as u64
}

#[cfg(feature = "measurement")]
fn order_query_action_owned_bytes_v2(global_request_id: &str, platform_order_id: &str) -> u64 {
    (global_request_id.len() + platform_order_id.len()) as u64
}

#[cfg(feature = "measurement")]
fn orders_query_action_owned_bytes_v2(global_request_id: &str, filter: &OrdersFilterV1) -> u64 {
    (global_request_id.len()
        + filter
            .condition_id
            .as_ref()
            .map_or(0, |condition_id| condition_id.0.len())
        + filter
            .token_id
            .as_ref()
            .map_or(0, |token_id| token_id.0.len())) as u64
}

#[cfg(feature = "measurement")]
fn trades_query_action_owned_bytes_v2(global_request_id: &str, filter: &TradesFilterV1) -> u64 {
    (global_request_id.len()
        + filter.trade_id.as_ref().map_or(0, String::len)
        + filter
            .condition_id
            .as_ref()
            .map_or(0, |condition_id| condition_id.0.len())
        + filter
            .token_id
            .as_ref()
            .map_or(0, |token_id| token_id.0.len())) as u64
}

fn existing_mutation_request_receipt_v2(
    state: &BusinessReducerStateV2,
    global_request_id: &str,
) -> Option<MutationRequestReceiptV2> {
    state.session.as_ref().and_then(|session| {
        session
            .mutation_requests
            .requests_by_id
            .get(global_request_id)
            .map(|existing| MutationRequestReceiptV2 {
                input_sequence: existing.input_sequence,
                platform_arrived_at_utc_ms: existing.platform_arrived_at_utc_ms,
                scheduled_action_count: existing.scheduled_action_count,
            })
    })
}

#[allow(clippy::result_large_err)]
fn register_new_request_v2(
    mut state: BusinessReducerStateV2,
    cutoff: &TrustedDecisionCutoffV2,
    request: &BusinessRequestV2,
    input_sequence: u64,
    latency_profile: &EmpiricalDelayProfile,
    rules: &PlatformRuleSnapshotV3,
) -> Result<
    (BusinessReducerStateV2, MutationRequestReceiptV2),
    (BusinessReducerStateV2, SimRunActorErrorV2),
> {
    let context = request.context();
    if let Err(error) = validate_request_context_v2(request) {
        return Err((state, error));
    }
    let Some(session) = state.session.as_ref() else {
        return Err((state, SimRunActorErrorV2::SessionNotCreated));
    };
    if !matches!(
        session.initialization,
        SessionInitializationV2::Active { .. }
    ) {
        return Err((state, SimRunActorErrorV2::SessionNotActive));
    }

    let items = match request {
        BusinessRequestV2::PostOrder(request) => {
            if let Err(error) = validate_post_item_v2(&request.item) {
                return Err((state, error));
            }
            NewMutationItemsV2::Post {
                method: MutationMethodV2::PostOrder,
                items: vec![(0_u16, request.item.clone())],
            }
        }
        BusinessRequestV2::PostOrders(request) => {
            if request.items.is_empty() {
                return Err((
                    state,
                    SimRunActorErrorV2::InvalidBusinessRequest("批量提交不能为空"),
                ));
            }
            let mut ordinals = BTreeSet::new();
            for item in &request.items {
                if !ordinals.insert(item.batch_item_ordinal) {
                    return Err((
                        state,
                        SimRunActorErrorV2::InvalidBusinessRequest("批量提交 ordinal 重复"),
                    ));
                }
                if let Err(error) = validate_post_item_v2(&item.item) {
                    return Err((state, error));
                }
            }
            let mut items = request
                .items
                .iter()
                .map(|item| (item.batch_item_ordinal, item.item.clone()))
                .collect::<Vec<_>>();
            items.sort_by_key(|(ordinal, _)| *ordinal);
            NewMutationItemsV2::Post {
                method: MutationMethodV2::PostOrders,
                items,
            }
        }
        BusinessRequestV2::CancelOrder(request) => {
            if let Err(error) = validate_cancel_item_v2(&request.item) {
                return Err((state, error));
            }
            NewMutationItemsV2::Cancel {
                method: MutationMethodV2::CancelOrder,
                targets: vec![CancelTargetRegistrationV2 {
                    target_ordinal: 0,
                    action_ordinal: 0,
                    platform_order_id: request.item.platform_order_id.clone(),
                    input_item_refs: vec![CancelInputItemRefV2 {
                        item_id: request.item.item_id.clone(),
                        batch_item_ordinal: 0,
                    }],
                }],
            }
        }
        BusinessRequestV2::CancelOrders(request) => {
            if request.items.is_empty() {
                return Err((
                    state,
                    SimRunActorErrorV2::InvalidBusinessRequest("批量撤单不能为空"),
                ));
            }
            let mut ordinals = BTreeSet::new();
            let mut target_indexes: BTreeMap<String, usize> = BTreeMap::new();
            let mut targets: Vec<CancelTargetRegistrationV2> = Vec::new();
            for item in &request.items {
                if !ordinals.insert(item.batch_item_ordinal) {
                    return Err((
                        state,
                        SimRunActorErrorV2::InvalidBusinessRequest("批量撤单 ordinal 重复"),
                    ));
                }
                if let Err(error) = validate_cancel_item_v2(&item.item) {
                    return Err((state, error));
                }
                if let Some(index) = target_indexes.get(&item.item.platform_order_id).copied() {
                    targets[index].action_ordinal =
                        targets[index].action_ordinal.min(item.batch_item_ordinal);
                    targets[index].input_item_refs.push(CancelInputItemRefV2 {
                        item_id: item.item.item_id.clone(),
                        batch_item_ordinal: item.batch_item_ordinal,
                    });
                } else {
                    let index = targets.len();
                    let target_ordinal = index as u64;
                    target_indexes.insert(item.item.platform_order_id.clone(), index);
                    targets.push(CancelTargetRegistrationV2 {
                        target_ordinal,
                        action_ordinal: item.batch_item_ordinal,
                        platform_order_id: item.item.platform_order_id.clone(),
                        input_item_refs: vec![CancelInputItemRefV2 {
                            item_id: item.item.item_id.clone(),
                            batch_item_ordinal: item.batch_item_ordinal,
                        }],
                    });
                }
            }
            NewMutationItemsV2::Cancel {
                method: MutationMethodV2::CancelOrders,
                targets,
            }
        }
        BusinessRequestV2::CancelMarketOrders(request) => NewMutationItemsV2::CancelMarket {
            condition_id: request.condition_id.clone(),
        },
        BusinessRequestV2::CancelAllOrders(_) => NewMutationItemsV2::CancelAll,
        BusinessRequestV2::Order(request) => {
            if !validate_platform_order_id_v2(&request.platform_order_id) {
                return Err((
                    state,
                    SimRunActorErrorV2::InvalidBusinessRequest("V2 平台订单身份非法"),
                ));
            }
            NewMutationItemsV2::OrderQuery {
                platform_order_id: request.platform_order_id.clone(),
            }
        }
        BusinessRequestV2::Orders(request) => {
            if request
                .filter
                .condition_id
                .as_ref()
                .is_some_and(|condition_id| !is_stable_text(&condition_id.0))
                || request
                    .filter
                    .token_id
                    .as_ref()
                    .is_some_and(|token_id| !is_stable_text(&token_id.0))
            {
                return Err((
                    state,
                    SimRunActorErrorV2::InvalidBusinessRequest("订单列表过滤身份非法"),
                ));
            }
            NewMutationItemsV2::OrdersQuery {
                filter: request.filter.clone(),
                cursor: request.cursor,
            }
        }
        BusinessRequestV2::Trades(request) => {
            if request
                .filter
                .trade_id
                .as_ref()
                .is_some_and(|trade_id| !is_stable_text(trade_id))
                || request
                    .filter
                    .condition_id
                    .as_ref()
                    .is_some_and(|condition_id| !is_stable_text(&condition_id.0))
                || request
                    .filter
                    .token_id
                    .as_ref()
                    .is_some_and(|token_id| !is_stable_text(&token_id.0))
                || request
                    .filter
                    .after_utc_ms
                    .into_iter()
                    .chain(request.filter.before_utc_ms)
                    .any(|value| value < 0)
                || matches!(
                    (request.filter.after_utc_ms, request.filter.before_utc_ms),
                    (Some(after), Some(before)) if after > before
                )
            {
                return Err((
                    state,
                    SimRunActorErrorV2::InvalidBusinessRequest("成交列表过滤条件非法"),
                ));
            }
            NewMutationItemsV2::TradesQuery {
                filter: request.filter.clone(),
                cursor: request.cursor,
            }
        }
    };

    let outbound_delay_ms = latency_profile.sample_delay_ms(
        session.session_seed.0.as_bytes(),
        &context.global_request_id,
        DelaySampleDomainV1::RequestOutbound,
    );
    let inbound_delay_ms = latency_profile.sample_delay_ms(
        session.session_seed.0.as_bytes(),
        &context.global_request_id,
        DelaySampleDomainV1::DirectResponseInbound,
    );
    let platform_arrived_at_utc_ms =
        match checked_add_delay_v2(cutoff.logical_time_utc_ms, outbound_delay_ms) {
            Ok(at) => at,
            Err(error) => return Err((state, error)),
        };
    let mut pending = Vec::new();

    let method = match items {
        NewMutationItemsV2::Post { method, items } => {
            pending.reserve(items.len());
            for (ordinal, item) in items {
                let processing_delay_ms =
                    if matches!(
                        item.order.kind,
                        SimBuyOrderKindV1::FokMarket { .. } | SimBuyOrderKindV1::FakMarket { .. }
                    ) && rules.synchronized_platform_rules.taker_order_delay_enabled
                    {
                        rules.documented_platform_rules.taker_delay.value.delay_ms
                    } else {
                        0
                    };
                let effective_processing_at_utc_ms =
                    match checked_add_delay_v2(platform_arrived_at_utc_ms, processing_delay_ms) {
                        Ok(at) => at,
                        Err(error) => return Err((state, error)),
                    };
                let normalized_gtd_expires_at_utc_ms = match item.order.kind {
                    SimBuyOrderKindV1::GtdLimit {
                        expires_at_utc_ms, ..
                    } => {
                        let timing = &rules.documented_platform_rules.gtd_timing.value;
                        match normalize_gtd_expiration(
                            expires_at_utc_ms,
                            effective_processing_at_utc_ms,
                            timing.minimum_lead_seconds,
                            timing.safety_offset_seconds,
                        ) {
                            Ok(normalized) => Some(normalized),
                            Err(()) => {
                                return Err((state, SimRunActorErrorV2::RequestTimeOverflow));
                            }
                        }
                    }
                    SimBuyOrderKindV1::GtcLimit { .. }
                    | SimBuyOrderKindV1::FokMarket { .. }
                    | SimBuyOrderKindV1::FakMarket { .. } => None,
                };

                pending.push((
                    MutationActionKeyV2 {
                        effective_processing_at_utc_ms,
                        input_sequence,
                        batch_item_ordinal: ordinal,
                    },
                    MutationActionV2::PostItem {
                        global_request_id: context.global_request_id.clone(),
                        item,
                        normalized_gtd_expires_at_utc_ms,
                    },
                ));
            }
            method
        }
        NewMutationItemsV2::Cancel { method, targets } => {
            pending.reserve(targets.len());
            for target in targets {
                pending.push((
                    MutationActionKeyV2 {
                        effective_processing_at_utc_ms: platform_arrived_at_utc_ms,
                        input_sequence,
                        batch_item_ordinal: target.action_ordinal,
                    },
                    MutationActionV2::CancelTarget {
                        global_request_id: context.global_request_id.clone(),
                        target_ordinal: target.target_ordinal,
                        platform_order_id: target.platform_order_id,
                        input_item_refs: target.input_item_refs,
                    },
                ));
            }
            method
        }
        NewMutationItemsV2::CancelMarket { condition_id } => {
            pending.push((
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: platform_arrived_at_utc_ms,
                    input_sequence,
                    batch_item_ordinal: 0,
                },
                MutationActionV2::CancelMarket {
                    global_request_id: context.global_request_id.clone(),
                    condition_id,
                },
            ));
            MutationMethodV2::CancelMarketOrders
        }
        NewMutationItemsV2::CancelAll => {
            pending.push((
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: platform_arrived_at_utc_ms,
                    input_sequence,
                    batch_item_ordinal: 0,
                },
                MutationActionV2::CancelAll {
                    global_request_id: context.global_request_id.clone(),
                },
            ));
            MutationMethodV2::CancelAllOrders
        }
        NewMutationItemsV2::OrderQuery { platform_order_id } => {
            pending.push((
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: platform_arrived_at_utc_ms,
                    input_sequence,
                    batch_item_ordinal: 0,
                },
                MutationActionV2::OrderQuery {
                    global_request_id: context.global_request_id.clone(),
                    platform_order_id,
                },
            ));
            MutationMethodV2::Order
        }
        NewMutationItemsV2::OrdersQuery { filter, cursor } => {
            pending.push((
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: platform_arrived_at_utc_ms,
                    input_sequence,
                    batch_item_ordinal: 0,
                },
                MutationActionV2::OrdersQuery {
                    global_request_id: context.global_request_id.clone(),
                    filter,
                    cursor,
                },
            ));
            MutationMethodV2::Orders
        }
        NewMutationItemsV2::TradesQuery { filter, cursor } => {
            pending.push((
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: platform_arrived_at_utc_ms,
                    input_sequence,
                    batch_item_ordinal: 0,
                },
                MutationActionV2::TradesQuery {
                    global_request_id: context.global_request_id.clone(),
                    filter,
                    cursor,
                },
            ));
            MutationMethodV2::Trades
        }
    };

    let Some(session) = state.session.as_ref() else {
        unreachable!("刚确认存在的唯一会话必须仍存在")
    };
    let mut condition_fences = Vec::new();
    let mut token_fences = Vec::new();
    if session.opening_fact.account_instance_key.caller_role == SimCallerRoleV1::Paper {
        for (_, action) in &pending {
            let MutationActionV2::PostItem { item, .. } = action else {
                continue;
            };
            if session
                .market_directory
                .market(&item.order.condition_id.0)
                .is_none()
                && !session
                    .market_enrollment
                    .paper_candidate_condition_ids
                    .contains(&item.order.condition_id.0)
                && !condition_fences.contains(&item.order.condition_id.0)
            {
                condition_fences.push(item.order.condition_id.0.clone());
            }
        }
        for (_, action) in &pending {
            let filter = match action {
                MutationActionV2::OrdersQuery { filter, .. } => {
                    Some((filter.condition_id.as_ref(), filter.token_id.as_ref()))
                }
                MutationActionV2::TradesQuery { filter, .. } => {
                    Some((filter.condition_id.as_ref(), filter.token_id.as_ref()))
                }
                MutationActionV2::PostItem { .. }
                | MutationActionV2::CancelTarget { .. }
                | MutationActionV2::CancelMarket { .. }
                | MutationActionV2::CancelAll { .. }
                | MutationActionV2::OrderQuery { .. } => None,
            };
            let Some((condition_id, token_id)) = filter else {
                continue;
            };
            if let Some(condition_id) = condition_id {
                if session.market_directory.market(&condition_id.0).is_none()
                    && !session
                        .market_enrollment
                        .paper_candidate_condition_ids
                        .contains(&condition_id.0)
                    && !condition_fences.contains(&condition_id.0)
                {
                    condition_fences.push(condition_id.0.clone());
                }
            } else if let Some(token_id) = token_id
                && !session.market_directory.contains_token(&token_id.0)
                && !session
                    .market_enrollment
                    .paper_candidate_token_ids
                    .contains(&token_id.0)
                && !token_fences.contains(&token_id.0)
            {
                token_fences.push(token_id.0.clone());
            }
        }
    }

    let session = state
        .session
        .as_mut()
        .expect("刚确认存在的唯一会话必须仍存在");
    for condition_id in condition_fences {
        session
            .market_enrollment
            .paper_candidate_condition_ids
            .insert(condition_id);
    }
    for token_id in token_fences {
        session
            .market_enrollment
            .paper_candidate_token_ids
            .insert(token_id);
    }
    let scheduled_action_count =
        u64::try_from(pending.len()).expect("V2 唯一 u16 ordinal 保证动作数量可表示为 u64");
    for (key, action) in pending {
        assert!(
            session
                .mutation_requests
                .pending_actions
                .insert(key, action)
                .is_none(),
            "日志输入序号和批量 ordinal 必须唯一标识 V2 动作"
        );
    }
    let (post_result_capacity, cancel_result_capacity) = match method {
        MutationMethodV2::PostOrder | MutationMethodV2::PostOrders => {
            (scheduled_action_count as usize, 0)
        }
        MutationMethodV2::CancelOrder
        | MutationMethodV2::CancelOrders
        | MutationMethodV2::CancelMarketOrders
        | MutationMethodV2::CancelAllOrders => (0, scheduled_action_count as usize),
        MutationMethodV2::Order | MutationMethodV2::Orders | MutationMethodV2::Trades => (0, 0),
    };
    session.mutation_requests.requests_by_id.insert(
        context.global_request_id.clone(),
        MutationRequestRecordV2 {
            method,
            caller_correlation_id: context.caller_correlation_id.clone(),
            input_sequence,
            request_sent_at_utc_ms: cutoff.logical_time_utc_ms,
            platform_arrived_at_utc_ms,
            inbound_delay_ms,
            scheduled_action_count,
            remaining_action_count: scheduled_action_count,
            post_results: Vec::with_capacity(post_result_capacity),
            cancel_results: Vec::with_capacity(cancel_result_capacity),
            query_result: None,
            completed_response: None,
        },
    );
    session.market_enrollment.business_input_started = true;

    Ok((
        state,
        MutationRequestReceiptV2 {
            input_sequence,
            platform_arrived_at_utc_ms,
            scheduled_action_count,
        },
    ))
}

fn map_market_rejection_v2(reason: MarketBuyRejectionV2) -> SubmissionRejectedReasonV2 {
    match reason {
        MarketBuyRejectionV2::MarketNotInScenario => {
            SubmissionRejectedReasonV2::MarketNotInScenario
        }
        MarketBuyRejectionV2::AssetNotInMarket => SubmissionRejectedReasonV2::AssetNotInMarket,
        MarketBuyRejectionV2::EndAtReached => SubmissionRejectedReasonV2::EndAtReached,
        MarketBuyRejectionV2::ExplicitTradingStateVeto => {
            SubmissionRejectedReasonV2::ExplicitTradingStateVeto
        }
    }
}

fn map_market_evidence_v2(reason: MarketBuyEvidenceUnknownV2) -> SubmissionEvidenceUnknownReasonV2 {
    match reason {
        MarketBuyEvidenceUnknownV2::Preloading => SubmissionEvidenceUnknownReasonV2::Preloading,
        MarketBuyEvidenceUnknownV2::TargetAssetNotFresh(quality) => {
            SubmissionEvidenceUnknownReasonV2::TargetAssetNotFresh(quality)
        }
    }
}

fn map_buy_rejection_v2(reason: BuyOrderRejectedReasonV2) -> SubmissionRejectedReasonV2 {
    match reason {
        BuyOrderRejectedReasonV2::PostOnlyWouldTake => {
            SubmissionRejectedReasonV2::PostOnlyWouldTake
        }
        BuyOrderRejectedReasonV2::BelowMarketableLimitBuyMinimumPrincipal => {
            SubmissionRejectedReasonV2::BelowMarketableLimitBuyMinimumPrincipal
        }
        BuyOrderRejectedReasonV2::GtdExpirationTooSoon => {
            SubmissionRejectedReasonV2::GtdExpirationTooSoon
        }
        BuyOrderRejectedReasonV2::InsufficientAvailablePusd => {
            SubmissionRejectedReasonV2::InsufficientAvailablePusd
        }
        BuyOrderRejectedReasonV2::FokNotFullyFilled => {
            SubmissionRejectedReasonV2::FokNotFullyFilled
        }
        BuyOrderRejectedReasonV2::FakNoMatch => SubmissionRejectedReasonV2::FakNoMatch,
    }
}

fn post_terms_v2(
    item: &PostOrderItemV1,
    normalized_gtd_expires_at_utc_ms: Option<Option<i64>>,
    current_tick_size_pusd_per_share: rust_decimal::Decimal,
    rules: &PlatformRuleSnapshotV3,
) -> Result<Result<BuyOrderTermsV2, SubmissionRejectedReasonV2>, SimRunActorErrorV2> {
    let minimum_shares = rules
        .synchronized_platform_rules
        .minimum_order_size_shares
        .value();
    let terms = match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => {
            let Some(encoding) = limit_buy_encoding(
                *limit_price_pusd_per_share,
                *size_shares,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if encoding.taker_amount < minimum_shares {
                return Ok(Err(SubmissionRejectedReasonV2::BelowMinimumShares));
            }
            BuyOrderTermsV2::Gtc {
                limit_price_pusd_per_share: encoding.price,
                accepted_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
                accepted_quote_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
                post_only: *post_only,
            }
        }
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            ..
        } => {
            let Some(encoding) = limit_buy_encoding(
                *limit_price_pusd_per_share,
                *size_shares,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if encoding.taker_amount < minimum_shares {
                return Ok(Err(SubmissionRejectedReasonV2::BelowMinimumShares));
            }
            BuyOrderTermsV2::Gtd {
                limit_price_pusd_per_share: encoding.price,
                accepted_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
                accepted_quote_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
                post_only: *post_only,
                expires_at_utc_ms: normalized_gtd_expires_at_utc_ms.flatten(),
            }
        }
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            let Some(encoding) = market_buy_encoding(
                *worst_price_pusd_per_share,
                *spend_pusd,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if *spend_pusd < crate::platform_rules::MARKET_BUY_MINIMUM_PRINCIPAL_PUSD {
                return Ok(Err(
                    SubmissionRejectedReasonV2::BelowMarketBuyMinimumPrincipal,
                ));
            }
            BuyOrderTermsV2::Fok {
                worst_price_pusd_per_share: encoding.price,
                accepted_quote_budget_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
                sdk_taker_amount_audit_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
            }
        }
        SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            let Some(encoding) = market_buy_encoding(
                *worst_price_pusd_per_share,
                *spend_pusd,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if *spend_pusd < crate::platform_rules::MARKET_BUY_MINIMUM_PRINCIPAL_PUSD {
                return Ok(Err(
                    SubmissionRejectedReasonV2::BelowMarketBuyMinimumPrincipal,
                ));
            }
            BuyOrderTermsV2::Fak {
                worst_price_pusd_per_share: encoding.price,
                accepted_quote_budget_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
                sdk_taker_amount_audit_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)
                    .map_err(SimRunActorErrorV2::Matching)?,
            }
        }
    };
    Ok(Ok(terms))
}

fn apply_post_action_v2(
    session: UniqueSessionStateV2,
    key: MutationActionKeyV2,
    global_request_id: String,
    item: PostOrderItemV1,
    normalized_gtd_expires_at_utc_ms: Option<Option<i64>>,
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<
    (
        UniqueSessionStateV2,
        PostOrderItemBusinessEffectV2,
        Vec<LifecycleFeedbackEffectV2>,
    ),
    SimRunActorErrorV2,
> {
    let outcome = match session.market_directory.market(&item.order.condition_id.0) {
        None => PostOrderItemOutcomeV2::RejectedNoEffect(
            SubmissionRejectedReasonV2::MarketNotInScenario,
        ),
        Some(member) => {
            let caller_role = session.opening_fact.account_instance_key.caller_role;
            let role_markets = match caller_role {
                SimCallerRoleV1::ModelTrain
                | SimCallerRoleV1::OfflineEvaluation
                | SimCallerRoleV1::HistoricalBacktest => session
                    .hidden_platform_markets
                    .as_ref()
                    .expect("训练和离线评估会话必须持有隐藏平台状态"),
                SimCallerRoleV1::Paper => &session.public_markets,
            };
            match role_markets.buy_admission(
                &item.order.condition_id.0,
                &item.order.token_id.0,
                key.effective_processing_at_utc_ms,
                member.market().window.end_at_utc_ms,
            ) {
                MarketBuyAdmissionV2::RejectedNoEffect(reason) => {
                    PostOrderItemOutcomeV2::RejectedNoEffect(map_market_rejection_v2(reason))
                }
                MarketBuyAdmissionV2::EvidenceUnknownNoEffect(reason) => {
                    PostOrderItemOutcomeV2::EvidenceUnknownNoEffect(map_market_evidence_v2(reason))
                }
                MarketBuyAdmissionV2::MissingCurrentTick => {
                    return Err(SimRunActorErrorV2::MissingCurrentTick(
                        item.order.token_id.0.clone(),
                    ));
                }
                MarketBuyAdmissionV2::Ready {
                    current_tick_size_pusd_per_share,
                } => match post_terms_v2(
                    &item,
                    normalized_gtd_expires_at_utc_ms,
                    current_tick_size_pusd_per_share,
                    rules,
                )? {
                    Err(reason) => PostOrderItemOutcomeV2::RejectedNoEffect(reason),
                    Ok(terms) => {
                        let input = BuyOrderActionV2 {
                            global_request_id,
                            batch_item_ordinal: key.batch_item_ordinal,
                            item_id: item.item_id.clone(),
                            condition_id: item.order.condition_id.clone(),
                            token_id: item.order.token_id.clone(),
                            effective_processing_at_utc_ms: key.effective_processing_at_utc_ms,
                            terms,
                        };
                        let (session, disposition) = session
                            .apply_buy_order(input, rules, finality_policy)
                            .map_err(SimRunActorErrorV2::Matching)?;

                        let (outcome, lifecycle) = match disposition {
                            BuyOrderDispositionV2::Accepted(accepted) => {
                                let (accepted, lifecycle) = accepted
                                    .into_output_effects(
                                        item.order.condition_id.clone(),
                                        item.order.token_id.clone(),
                                        key.effective_processing_at_utc_ms,
                                    )
                                    .map_err(SimRunActorErrorV2::Matching)?;

                                (PostOrderItemOutcomeV2::Accepted(accepted), lifecycle)
                            }
                            BuyOrderDispositionV2::RejectedNoEffect(reason) => (
                                PostOrderItemOutcomeV2::RejectedNoEffect(map_buy_rejection_v2(
                                    reason,
                                )),
                                Vec::new(),
                            ),
                        };
                        return Ok((
                            session,
                            PostOrderItemBusinessEffectV2 {
                                item_id: item.item_id,
                                batch_item_ordinal: key.batch_item_ordinal,
                                condition_id: item.order.condition_id,
                                token_id: item.order.token_id,
                                effective_processing_at_utc_ms: key.effective_processing_at_utc_ms,
                                outcome,
                            },
                            lifecycle,
                        ));
                    }
                },
            }
        }
    };
    Ok((
        session,
        PostOrderItemBusinessEffectV2 {
            item_id: item.item_id,
            batch_item_ordinal: key.batch_item_ordinal,
            condition_id: item.order.condition_id,
            token_id: item.order.token_id,
            effective_processing_at_utc_ms: key.effective_processing_at_utc_ms,
            outcome,
        },
        Vec::new(),
    ))
}

fn apply_cancel_target_action_v2(
    session: UniqueSessionStateV2,
    global_request_id: Option<&str>,
    target_ordinal: u64,
    platform_order_id: String,
    input_item_refs: Vec<CancelInputItemRefV2>,
    platform_effective_at_utc_ms: i64,
) -> Result<
    (
        UniqueSessionStateV2,
        CancelTargetBusinessEffectV3,
        Vec<LifecycleFeedbackEffectV2>,
    ),
    SimRunActorErrorV2,
> {
    let (session, disposition) = session
        .apply_caller_cancel(
            global_request_id,
            &platform_order_id,
            platform_effective_at_utc_ms,
        )
        .map_err(SimRunActorErrorV2::Matching)?;

    let (outcome, lifecycle) = match disposition {
        CallerCancelDispositionV2::TemporaryFailureNoEffect => (
            contracts::CancelTargetOutcome::NotCanceledNoEffect(
                contracts::CancelNotCanceledReasonV3::TemporaryFailureNoEffect,
            ),
            Vec::new(),
        ),
        CallerCancelDispositionV2::NotFound => (
            contracts::CancelTargetOutcome::NotCanceledNoEffect(
                contracts::CancelNotCanceledReasonV3::NotFoundOrNotOwned,
            ),
            Vec::new(),
        ),
        CallerCancelDispositionV2::NoCancellableRemainder => (
            contracts::CancelTargetOutcome::NotCanceledNoEffect(
                contracts::CancelNotCanceledReasonV3::NoCancellableRemainder,
            ),
            Vec::new(),
        ),
        CallerCancelDispositionV2::Canceled(effect) => {
            let (measurement, lifecycle) = effect
                .into_output_effects(platform_order_id.clone(), platform_effective_at_utc_ms)
                .map_err(SimRunActorErrorV2::Matching)?;

            (
                contracts::CancelTargetOutcome::Canceled(measurement),
                vec![lifecycle],
            )
        }
    };
    Ok((
        session,
        CancelTargetBusinessEffectV3 {
            target_ordinal,
            platform_order_id,
            input_item_refs,
            effective_processing_at_utc_ms: platform_effective_at_utc_ms,
            outcome,
        },
        lifecycle,
    ))
}

fn apply_cancel_targets_action_v2(
    mut session: UniqueSessionStateV2,
    global_request_id: Option<&str>,
    target_ids: Vec<String>,
    platform_effective_at_utc_ms: i64,
) -> Result<
    (
        UniqueSessionStateV2,
        Vec<CancelTargetBusinessEffectV3>,
        Vec<LifecycleFeedbackEffectV2>,
    ),
    SimRunActorErrorV2,
> {
    let mut effects = Vec::with_capacity(target_ids.len());
    let mut lifecycle = Vec::with_capacity(target_ids.len());

    for (target_ordinal, platform_order_id) in target_ids.into_iter().enumerate() {
        let (next_session, effect, mut target_lifecycle) = apply_cancel_target_action_v2(
            session,
            global_request_id,
            target_ordinal as u64,
            platform_order_id,
            Vec::new(),
            platform_effective_at_utc_ms,
        )?;
        session = next_session;

        effects.push(effect);
        lifecycle.append(&mut target_lifecycle);
    }
    Ok((session, effects, lifecycle))
}

fn apply_cancel_all_action_v2(
    session: UniqueSessionStateV2,
    platform_effective_at_utc_ms: i64,
) -> Result<
    (
        UniqueSessionStateV2,
        Vec<CancelTargetBusinessEffectV3>,
        Vec<LifecycleFeedbackEffectV2>,
    ),
    SimRunActorErrorV2,
> {
    let target_ids = session.execution_state.active_platform_order_ids();
    apply_cancel_targets_action_v2(session, None, target_ids, platform_effective_at_utc_ms)
}

fn apply_cancel_market_action_v2(
    session: UniqueSessionStateV2,
    global_request_id: &str,
    condition_id: &ConditionId,
    platform_effective_at_utc_ms: i64,
) -> Result<
    (
        UniqueSessionStateV2,
        Vec<CancelTargetBusinessEffectV3>,
        Vec<LifecycleFeedbackEffectV2>,
    ),
    SimRunActorErrorV2,
> {
    let target_ids = session
        .execution_state
        .active_platform_order_ids_for_condition(condition_id);
    apply_cancel_targets_action_v2(
        session,
        Some(global_request_id),
        target_ids,
        platform_effective_at_utc_ms,
    )
}

fn apply_order_query_action_v2(
    session: UniqueSessionStateV2,
    platform_order_id: String,
    platform_effective_at_utc_ms: i64,
) -> Result<(UniqueSessionStateV2, QueryActionBusinessEffectV2), SimRunActorErrorV2> {
    let result = match session
        .execution_state
        .order_query_projection_v2(&platform_order_id)
        .map_err(SimRunActorErrorV2::Matching)?
    {
        Some((order, true, _)) => OrderQueryResultV2::Found(Box::new(order)),
        Some((order, false, last_updated_at_utc_ms))
            if session
                .query_state
                .order_history_retained_from_utc_ms
                .is_none_or(|retained_from| last_updated_at_utc_ms >= retained_from) =>
        {
            OrderQueryResultV2::Found(Box::new(order))
        }
        Some((_, false, _)) => {
            let retained_from_utc_ms = session
                .query_state
                .order_history_retained_from_utc_ms
                .expect("V2 终态订单越过历史水位时必须存在保留水位");
            OrderQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::OrderHistory {
                retained_from_utc_ms,
            })
        }
        None => match session.query_state.order_history_retained_from_utc_ms {
            Some(retained_from_utc_ms) => {
                OrderQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::OrderHistory {
                    retained_from_utc_ms,
                })
            }
            None => OrderQueryResultV2::NotFound,
        },
    };
    Ok((
        session,
        QueryActionBusinessEffectV2 {
            effective_processing_at_utc_ms: platform_effective_at_utc_ms,
            result: QueryActionResultV2::Order(result),
        },
    ))
}

fn apply_orders_query_action_v2(
    mut session: UniqueSessionStateV2,
    filter: OrdersFilterV1,
    cursor: Option<QueryCursorV2>,
    platform_effective_at_utc_ms: i64,
) -> Result<(UniqueSessionStateV2, QueryActionBusinessEffectV2), SimRunActorErrorV2> {
    let key = OrdersQueryKeyV2::from(&filter);

    let result = if let Some(cursor) = cursor {
        if cursor.root_sequence < session.query_state.earliest_retained_root_sequence {
            OrdersQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::RootSnapshot {
                requested_root_sequence: cursor.root_sequence,
                earliest_retained_root_sequence: session
                    .query_state
                    .earliest_retained_root_sequence,
            })
        } else {
            match session
                .query_state
                .order_roots
                .get(&(cursor.root_sequence, key))
            {
                Some(root) => {
                    orders_page_from_root_v2(cursor.root_sequence, root, cursor.next_offset)
                }
                None => OrdersQueryResultV2::InvalidCursor,
            }
        }
    } else {
        let root_sequence = session.query_state.current_revision;
        let root_key = (root_sequence, key.clone());
        if !session.query_state.order_roots.contains_key(&root_key) {
            let mut rows = session
                .execution_state
                .order_projections_v2()
                .map_err(SimRunActorErrorV2::Matching)?
                .into_iter()
                .filter(|order| {
                    order.status.known == Some(SimulatedOrderKnownStatusV2::Live)
                        && filter
                            .condition_id
                            .as_ref()
                            .is_none_or(|condition_id| order.condition_id == *condition_id)
                        && filter
                            .token_id
                            .as_ref()
                            .is_none_or(|token_id| order.token_id == *token_id)
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                (left.created_at_utc_ms, &left.platform_order_id)
                    .cmp(&(right.created_at_utc_ms, &right.platform_order_id))
            });

            session
                .query_state
                .order_roots
                .insert(root_key.clone(), rows);
        }
        let root = session
            .query_state
            .order_roots
            .get(&root_key)
            .expect("V2 订单查询首页必须物化或复用精确根");
        orders_page_from_root_v2(root_sequence, root, 0)
    };
    Ok((
        session,
        QueryActionBusinessEffectV2 {
            effective_processing_at_utc_ms: platform_effective_at_utc_ms,
            result: QueryActionResultV2::Orders(result),
        },
    ))
}

fn apply_trades_query_action_v2(
    mut session: UniqueSessionStateV2,
    filter: TradesFilterV1,
    cursor: Option<QueryCursorV2>,
    platform_effective_at_utc_ms: i64,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<(UniqueSessionStateV2, QueryActionBusinessEffectV2), SimRunActorErrorV2> {
    let key = TradesQueryKeyV2::from(&filter);

    let result = if let Some(cursor) = cursor {
        if cursor.root_sequence < session.query_state.earliest_retained_root_sequence {
            TradesQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::RootSnapshot {
                requested_root_sequence: cursor.root_sequence,
                earliest_retained_root_sequence: session
                    .query_state
                    .earliest_retained_root_sequence,
            })
        } else {
            match session
                .query_state
                .trade_roots
                .get(&(cursor.root_sequence, key))
            {
                Some(root) => {
                    trades_page_from_root_v2(cursor.root_sequence, root, cursor.next_offset)
                }
                None => TradesQueryResultV2::InvalidCursor,
            }
        }
    } else {
        let root_sequence = session.query_state.current_revision;
        let root_key = (root_sequence, key.clone());
        if !session.query_state.trade_roots.contains_key(&root_key) {
            let all_trades = session
                .execution_state
                .trade_projections_v2(finality_policy)
                .map_err(SimRunActorErrorV2::Matching)?;
            let exact_trade_is_retained = filter.trade_id.as_ref().is_some_and(|trade_id| {
                all_trades.iter().any(|trade| {
                    trade.trade_id == *trade_id
                        && session
                            .query_state
                            .trade_history_retained_from_utc_ms
                            .is_none_or(|retained_from| trade.matched_at_utc_ms >= retained_from)
                })
            });
            let normalized_interval = strict_millisecond_interval(&filter);
            let empty_open_interval = normalized_interval.is_none();
            let history_is_incomplete = session
                .query_state
                .trade_history_retained_from_utc_ms
                .is_some_and(|retained_from| {
                    !exact_trade_is_retained
                        && normalized_interval
                            .is_some_and(|(first_inclusive, _)| first_inclusive < retained_from)
                });
            if history_is_incomplete && !empty_open_interval {
                let retained_from_utc_ms = session
                    .query_state
                    .trade_history_retained_from_utc_ms
                    .expect("V2 成交历史不完整必须存在保留水位");
                return Ok((
                    session,
                    QueryActionBusinessEffectV2 {
                        effective_processing_at_utc_ms: platform_effective_at_utc_ms,
                        result: QueryActionResultV2::Trades(TradesQueryResultV2::OutOfRetention(
                            QueryOutOfRetentionV2::TradeHistory {
                                retained_from_utc_ms,
                            },
                        )),
                    },
                ));
            }
            let mut rows = if empty_open_interval {
                Vec::new()
            } else {
                all_trades
                    .into_iter()
                    .filter(|trade| {
                        filter
                            .trade_id
                            .as_ref()
                            .is_none_or(|trade_id| trade.trade_id == *trade_id)
                            && filter
                                .condition_id
                                .as_ref()
                                .is_none_or(|condition_id| trade.condition_id == *condition_id)
                            && filter
                                .token_id
                                .as_ref()
                                .is_none_or(|token_id| trade.token_id == *token_id)
                            && filter
                                .after_utc_ms
                                .is_none_or(|after| trade.matched_at_utc_ms > after)
                            && filter
                                .before_utc_ms
                                .is_none_or(|before| trade.matched_at_utc_ms < before)
                    })
                    .collect::<Vec<_>>()
            };
            rows.sort_by(|left, right| {
                (left.matched_at_utc_ms, &left.trade_id)
                    .cmp(&(right.matched_at_utc_ms, &right.trade_id))
            });

            session
                .query_state
                .trade_roots
                .insert(root_key.clone(), rows);
        }
        let root = session
            .query_state
            .trade_roots
            .get(&root_key)
            .expect("V2 成交查询首页必须物化或复用精确根");
        trades_page_from_root_v2(root_sequence, root, 0)
    };
    Ok((
        session,
        QueryActionBusinessEffectV2 {
            effective_processing_at_utc_ms: platform_effective_at_utc_ms,
            result: QueryActionResultV2::Trades(result),
        },
    ))
}

fn seal_completed_response_v2(
    global_request_id: &str,
    request: &mut MutationRequestRecordV2,
) -> Result<Arc<StoredDirectResponseV3>, SimRunActorErrorV2> {
    debug_assert_eq!(request.remaining_action_count, 0);
    debug_assert!(request.completed_response.is_none());
    request
        .post_results
        .sort_by_key(|result| result.batch_item_ordinal);
    request
        .cancel_results
        .sort_by_key(|result| result.target_ordinal);

    let (platform_responded_at_utc_ms, payload) = match request.method {
        MutationMethodV2::PostOrder => {
            let item = request
                .post_results
                .pop()
                .expect("V2 单笔提交完成时必须恰有一个结果");
            (
                item.effective_processing_at_utc_ms,
                DirectResponsePayloadDraftV3::PostOrder(item),
            )
        }
        MutationMethodV2::PostOrders => {
            let items = std::mem::take(&mut request.post_results);
            let responded_at = items
                .iter()
                .map(|item| item.effective_processing_at_utc_ms)
                .max()
                .expect("V2 批量提交完成时必须至少有一个结果");
            (
                responded_at,
                DirectResponsePayloadDraftV3::PostOrders(items),
            )
        }
        MutationMethodV2::CancelOrder => {
            let target = request
                .cancel_results
                .pop()
                .expect("V2 单笔撤单完成时必须恰有一个目标结果");
            (
                target.effective_processing_at_utc_ms,
                DirectResponsePayloadDraftV3::CancelOrder(target),
            )
        }
        MutationMethodV2::CancelOrders => {
            let targets = std::mem::take(&mut request.cancel_results);
            let responded_at = targets
                .iter()
                .map(|target| target.effective_processing_at_utc_ms)
                .max()
                .expect("V2 批量撤单完成时必须至少有一个目标结果");
            (
                responded_at,
                DirectResponsePayloadDraftV3::CancelOrders(targets),
            )
        }
        MutationMethodV2::CancelMarketOrders => {
            let targets = std::mem::take(&mut request.cancel_results);
            let responded_at = targets
                .iter()
                .map(|target| target.effective_processing_at_utc_ms)
                .max()
                .unwrap_or(request.platform_arrived_at_utc_ms);
            (
                responded_at,
                DirectResponsePayloadDraftV3::CancelMarketOrders(targets),
            )
        }
        MutationMethodV2::CancelAllOrders => {
            let targets = std::mem::take(&mut request.cancel_results);
            let responded_at = targets
                .iter()
                .map(|target| target.effective_processing_at_utc_ms)
                .max()
                .unwrap_or(request.platform_arrived_at_utc_ms);
            (
                responded_at,
                DirectResponsePayloadDraftV3::CancelAllOrders(targets),
            )
        }
        MutationMethodV2::Order => {
            let result = request
                .query_result
                .take()
                .expect("V2 单订单查询完成时必须恰有一个结果");
            (
                result.effective_processing_at_utc_ms,
                DirectResponsePayloadDraftV3::Order(result),
            )
        }
        MutationMethodV2::Orders => {
            let result = request
                .query_result
                .take()
                .expect("V2 订单列表查询完成时必须恰有一个结果");
            (
                result.effective_processing_at_utc_ms,
                DirectResponsePayloadDraftV3::Orders(result),
            )
        }
        MutationMethodV2::Trades => {
            let result = request
                .query_result
                .take()
                .expect("V2 成交列表查询完成时必须恰有一个结果");
            (
                result.effective_processing_at_utc_ms,
                DirectResponsePayloadDraftV3::Trades(result),
            )
        }
    };
    let response_received_at_utc_ms =
        checked_add_delay_v2(platform_responded_at_utc_ms, request.inbound_delay_ms)?;
    let response = Arc::new(StoredDirectResponseV3::Draft(DirectResponseDraftV3 {
        global_request_id: global_request_id.to_owned(),
        caller_correlation_id: request.caller_correlation_id.clone(),
        method: request.method,
        platform_responded_at_utc_ms,
        response_received_at_utc_ms,
        payload,
    }));
    request.completed_response = Some(Arc::clone(&response));
    Ok(response)
}

#[allow(clippy::result_large_err)]
fn advance_next_mutation_action_at_v2(
    mut state: BusinessReducerStateV2,
    platform_effective_at_utc_ms: i64,
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<
    (
        BusinessReducerStateV2,
        Option<MutationActionTransitionEffectsV2>,
    ),
    BusinessReductionErrorV2,
> {
    let Some(mut session) = state.session.take() else {
        return Err(unaccepted_business_input_v2(
            state,
            SimRunActorErrorV2::SessionNotCreated,
        ));
    };
    let Some(first_key) = session
        .mutation_requests
        .pending_actions
        .first_key_value()
        .map(|(key, _)| *key)
    else {
        state.session = Some(session);
        return Ok((state, None));
    };
    if first_key.effective_processing_at_utc_ms > platform_effective_at_utc_ms {
        state.session = Some(session);
        return Ok((state, None));
    }
    if first_key.effective_processing_at_utc_ms < platform_effective_at_utc_ms {
        return Err(BusinessReductionErrorV2::Fatal(
            SimRunActorErrorV2::Matching(MatchingStateErrorV1::LogicalTimeRegression),
        ));
    }
    let (key, action) = session
        .mutation_requests
        .pending_actions
        .pop_first()
        .expect("刚读取的 V2 逻辑动作必须仍是首项");
    let (global_request_id, mut session, effect, lifecycle) = match action {
        MutationActionV2::PostItem {
            global_request_id,
            item,
            normalized_gtd_expires_at_utc_ms,
        } => {
            let (session, effect, lifecycle) = apply_post_action_v2(
                session,
                key,
                global_request_id.clone(),
                item,
                normalized_gtd_expires_at_utc_ms,
                rules,
                finality_policy,
            )
            .map_err(BusinessReductionErrorV2::Fatal)?;
            let effect = Arc::new(effect);
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Post(effect),
                lifecycle,
            )
        }
        MutationActionV2::CancelTarget {
            global_request_id,
            target_ordinal,
            platform_order_id,
            input_item_refs,
        } => {
            let (session, effect, lifecycle) = apply_cancel_target_action_v2(
                session,
                Some(&global_request_id),
                target_ordinal,
                platform_order_id,
                input_item_refs,
                key.effective_processing_at_utc_ms,
            )
            .map_err(BusinessReductionErrorV2::Fatal)?;
            let effect = Arc::new(effect);
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Cancel(vec![effect]),
                lifecycle,
            )
        }
        MutationActionV2::CancelMarket {
            global_request_id,
            condition_id,
        } => {
            let (session, effects, lifecycle) = apply_cancel_market_action_v2(
                session,
                &global_request_id,
                &condition_id,
                key.effective_processing_at_utc_ms,
            )
            .map_err(BusinessReductionErrorV2::Fatal)?;
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Cancel(effects.into_iter().map(Arc::new).collect()),
                lifecycle,
            )
        }
        MutationActionV2::CancelAll { global_request_id } => {
            let (session, effects, lifecycle) =
                apply_cancel_all_action_v2(session, key.effective_processing_at_utc_ms)
                    .map_err(BusinessReductionErrorV2::Fatal)?;
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Cancel(effects.into_iter().map(Arc::new).collect()),
                lifecycle,
            )
        }
        MutationActionV2::OrderQuery {
            global_request_id,
            platform_order_id,
        } => {
            let (session, effect) = apply_order_query_action_v2(
                session,
                platform_order_id,
                key.effective_processing_at_utc_ms,
            )
            .map_err(BusinessReductionErrorV2::Fatal)?;
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Query(Arc::new(effect)),
                Vec::new(),
            )
        }
        MutationActionV2::OrdersQuery {
            global_request_id,
            filter,
            cursor,
        } => {
            let (session, effect) = apply_orders_query_action_v2(
                session,
                filter,
                cursor,
                key.effective_processing_at_utc_ms,
            )
            .map_err(BusinessReductionErrorV2::Fatal)?;
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Query(Arc::new(effect)),
                Vec::new(),
            )
        }
        MutationActionV2::TradesQuery {
            global_request_id,
            filter,
            cursor,
        } => {
            let (session, effect) = apply_trades_query_action_v2(
                session,
                filter,
                cursor,
                key.effective_processing_at_utc_ms,
                finality_policy,
            )
            .map_err(BusinessReductionErrorV2::Fatal)?;
            (
                global_request_id,
                session,
                MutationActionBusinessEffectV3::Query(Arc::new(effect)),
                Vec::new(),
            )
        }
    };

    let query_projection_changed = match &effect {
        MutationActionBusinessEffectV3::Post(effect) => {
            matches!(&effect.outcome, PostOrderItemOutcomeV2::Accepted(_))
        }
        MutationActionBusinessEffectV3::Cancel(effects) => effects
            .iter()
            .any(|effect| matches!(&effect.outcome, contracts::CancelTargetOutcome::Canceled(_))),
        MutationActionBusinessEffectV3::Query(_) => false,
    };
    if query_projection_changed {
        session.query_state.advance_revision();
    }
    let request = session
        .mutation_requests
        .requests_by_id
        .get_mut(&global_request_id)
        .expect("V2 待处理动作必须仍归属于唯一首次请求记录");
    request.remaining_action_count = request
        .remaining_action_count
        .checked_sub(1)
        .expect("V2 待处理动作数量必须保持正值");
    match &effect {
        MutationActionBusinessEffectV3::Post(effect) => {
            request.post_results.push(Arc::clone(effect));
        }
        MutationActionBusinessEffectV3::Cancel(effects) => {
            for effect in effects {
                request.cancel_results.push(Arc::clone(effect));
            }
        }
        MutationActionBusinessEffectV3::Query(effect) => {
            request.query_result = Some(Arc::clone(effect));
        }
    }
    let completed_response = if request.remaining_action_count == 0 {
        Some(
            seal_completed_response_v2(&global_request_id, request)
                .map_err(BusinessReductionErrorV2::Fatal)?,
        )
    } else {
        None
    };

    state.session = Some(session);
    Ok((
        state,
        Some(MutationActionTransitionEffectsV2 {
            business: effect,
            lifecycle,
            completed_response,
        }),
    ))
}

fn unaccepted_business_input_v2(
    state: BusinessReducerStateV2,
    error: SimRunActorErrorV2,
) -> BusinessReductionErrorV2 {
    BusinessReductionErrorV2::Unaccepted { state, error }
}

fn next_scheduled_obligation_at_v2(
    state: &BusinessReducerStateV2,
    rules: &PlatformRuleSnapshotV3,
) -> Option<i64> {
    let session = state.session.as_ref()?;
    [
        session.execution_state.next_gtd_deadline_at_utc_ms(rules),
        session.execution_state.next_scheduled_finality_at_utc_ms(),
        session.execution_state.next_rebate_clock_at_utc_ms(),
        session.execution_state.next_settlement_clock_at_utc_ms(),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn next_required_logical_drive_v2(
    state: &BusinessReducerStateV2,
    rules: &PlatformRuleSnapshotV3,
) -> Option<NextRequiredLogicalDriveV2> {
    let session = state.session.as_ref()?;
    if !matches!(
        session.initialization,
        SessionInitializationV2::Active { .. }
    ) {
        return None;
    }
    let schedule = RequiredLogicalDriveScheduleV2 {
        gtd_at_utc_ms: session.execution_state.next_gtd_deadline_at_utc_ms(rules),
        finality_at_utc_ms: session.execution_state.next_scheduled_finality_at_utc_ms(),
        rebate_at_utc_ms: session.execution_state.next_rebate_clock_at_utc_ms(),
        settlement_at_utc_ms: session.execution_state.next_settlement_clock_at_utc_ms(),
        mutation_at_utc_ms: session
            .mutation_requests
            .pending_actions
            .first_key_value()
            .map(|(key, _)| key.effective_processing_at_utc_ms),
    };
    schedule
        .earliest_at_utc_ms()
        .map(|required_at_utc_ms| NextRequiredLogicalDriveV2 {
            path: match session.opening_fact.account_instance_key.caller_role {
                SimCallerRoleV1::ModelTrain
                | SimCallerRoleV1::OfflineEvaluation
                | SimCallerRoleV1::HistoricalBacktest => MarketEventPathV1::HiddenPlatform,
                SimCallerRoleV1::Paper => MarketEventPathV1::PublicVisibility,
            },
            required_at_utc_ms,
        })
}

#[allow(clippy::result_large_err)]
fn drain_scheduled_obligations_at_v2(
    mut state: BusinessReducerStateV2,
    logical_at_utc_ms: i64,
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
    output: &mut LogicalDriveOutputV2,
) -> Result<BusinessReducerStateV2, BusinessReductionErrorV2> {
    if let Some(session) = state.session.as_mut() {
        if session.execution_state.next_settlement_clock_at_utc_ms() == Some(logical_at_utc_ms) {
            let effects = session
                .execution_state
                .advance_settlement_at(logical_at_utc_ms)
                .map_err(|error| {
                    BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
                })?;
            for effect in &effects {
                if let LifecycleFeedbackEffectV2::Settlement(
                    crate::matching::SettlementLifecycleEffectV2 {
                        event:
                            contracts::SettlementLifecycleEventV2::RequestedSettlement {
                                result:
                                    contracts::economics::RequestedSettlementResultV1::Request(request),
                                ..
                            },
                        ..
                    },
                ) = effect
                {
                    if request.outcome
                        == contracts::economics::SettlementRequestOutcomeV1::FinalSuccess
                    {
                        let condition = &request.settlement_request_id.condition_id.0;
                        if let Some(terminal) = session
                            .execution_state
                            .redeemed_market_resolution(condition)
                        {
                            let _ = session
                                .public_markets
                                .mark_resolution_terminal_if_qualified(condition, terminal);
                            if let Some(hidden) = &mut session.hidden_platform_markets {
                                let _ = hidden
                                    .mark_resolution_terminal_if_qualified(condition, terminal);
                            }
                        }
                    }
                }
            }

            output.lifecycle.extend(effects);
        }
    }
    while state
        .session
        .as_ref()
        .and_then(|session| session.execution_state.next_gtd_deadline_at_utc_ms(rules))
        == Some(logical_at_utc_ms)
    {
        let session = state.session.take().expect("逻辑驱动已确认唯一会话");
        let (session, effects) = session
            .map_execution_state(|execution| {
                execution.advance_next_gtd_deadline_at(logical_at_utc_ms, rules)
            })
            .map_err(|error| {
                BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
            })?;

        output
            .lifecycle
            .extend(effects.into_lifecycle_feedback(rules).map_err(|error| {
                BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
            })?);
        state.session = Some(session);
    }
    while state
        .session
        .as_ref()
        .and_then(|session| session.execution_state.next_scheduled_finality_at_utc_ms())
        == Some(logical_at_utc_ms)
    {
        let session = state.session.take().expect("逻辑驱动已确认唯一会话");
        let (mut session, effects) = session
            .map_execution_state(|execution| {
                execution.advance_next_scheduled_finality_at(
                    logical_at_utc_ms,
                    finality_policy,
                    rules,
                )
            })
            .map_err(|error| {
                BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
            })?;

        if effects.transition.is_some() {
            session.query_state.advance_revision();
        }
        if let Some(transition) = effects.transition.as_ref() {
            let condition = &transition.trade_finality.condition_id.0;
            if let Some(terminal) = session
                .execution_state
                .redeemed_market_resolution(condition)
            {
                let _ = session
                    .public_markets
                    .mark_resolution_terminal_if_qualified(condition, terminal);
                if let Some(hidden) = &mut session.hidden_platform_markets {
                    let _ = hidden.mark_resolution_terminal_if_qualified(condition, terminal);
                }
            }
            for settlement in &transition.settlement {
                if let crate::matching::EconomicLifecycleEffectV2::AutoRedeem(redeem) = settlement {
                    let _ = session
                        .public_markets
                        .mark_resolution_terminal_if_qualified(
                            &redeem.condition_id.0,
                            &redeem.terminal,
                        );
                    if let Some(hidden) = session.hidden_platform_markets.as_mut() {
                        let _ = hidden.mark_resolution_terminal_if_qualified(
                            &redeem.condition_id.0,
                            &redeem.terminal,
                        );
                    }
                }
            }
        }
        output
            .lifecycle
            .extend(effects.into_lifecycle_feedback().map_err(|error| {
                BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
            })?);
        state.session = Some(session);
    }
    while state
        .session
        .as_ref()
        .and_then(|session| session.execution_state.next_rebate_clock_at_utc_ms())
        == Some(logical_at_utc_ms)
    {
        state = advance_rebate_due_v2(state, logical_at_utc_ms, output)
            .map_err(BusinessReductionErrorV2::Fatal)?;
    }
    Ok(state)
}

fn advance_rebate_due_v2(
    mut state: BusinessReducerStateV2,
    due: i64,
    output: &mut LogicalDriveOutputV2,
) -> Result<BusinessReducerStateV2, SimRunActorErrorV2> {
    let session = state.session.take().expect("逻辑驱动已确认唯一会话");
    let (session, effects) = session
        .map_execution_state(|execution| execution.advance_next_rebate_clock_at(due))
        .map_err(SimRunActorErrorV2::Matching)?;

    output.lifecycle.extend(
        effects
            .into_lifecycle_feedback()
            .map_err(SimRunActorErrorV2::Matching)?,
    );
    state.session = Some(session);
    Ok(state)
}

#[allow(clippy::result_large_err)]
fn advance_role_time_at_v2(
    mut state: BusinessReducerStateV2,
    path: MarketEventPathV1,
    drives_execution: bool,
    logical_at_utc_ms: i64,
) -> Result<BusinessReducerStateV2, BusinessReductionErrorV2> {
    let session = state.session.as_mut().expect("逻辑驱动已确认唯一会话");
    match path {
        MarketEventPathV1::HiddenPlatform => session
            .hidden_platform_markets
            .as_mut()
            .expect("隐藏执行路径必须持有隐藏市场状态")
            .advance_time(&session.market_directory, logical_at_utc_ms),
        MarketEventPathV1::PublicVisibility => session
            .public_markets
            .advance_time(&session.market_directory, logical_at_utc_ms),
    }
    .map_err(|error| BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(error)))?;
    if drives_execution {
        session
            .execution_state
            .advance_market_time(&session.market_directory, logical_at_utc_ms)
            .map_err(|error| {
                BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
            })?;
    }

    Ok(state)
}

#[allow(clippy::result_large_err)]
pub(crate) fn drive_business_input_at_v2(
    mut state: BusinessReducerStateV2,
    input: BusinessReducerInputV2<'_>,
) -> BusinessReductionResultV2 {
    #[cfg(test)]
    record_reducer_call_for_tests();
    let (
        path,
        logical_at_utc_ms,
        next_index_position,
        start_index_position,
        bucket,
        rules,
        finality_policy,
        public_group,
        public_closure,
        hidden_bucket,
        public_observation,
    ) = match input {
        BusinessReducerInputV2::PlatformPrefixAdvance {
            advance,
            rules,
            finality_policy,
        } => (
            advance.path,
            advance.closed_through_utc_ms,
            advance.next_index_position,
            None,
            None,
            rules,
            finality_policy,
            None,
            None,
            None,
            None,
        ),
        BusinessReducerInputV2::ClosedMarketBucket {
            bucket,
            rules,
            finality_policy,
        } => {
            let reference = &bucket.reference;
            (
                reference.path,
                reference.bucket_at_utc_ms,
                reference.next_index_position,
                Some(reference.start_index_position),
                Some(bucket),
                rules,
                finality_policy,
                None,
                None,
                None,
                None,
            )
        }
        BusinessReducerInputV2::HiddenStepBucket {
            bucket,
            rules,
            finality_policy,
        } => (
            MarketEventPathV1::HiddenPlatform,
            bucket.reference.bucket_time_utc_ms,
            bucket.reference.index_end,
            Some(bucket.reference.index_start),
            None,
            rules,
            finality_policy,
            None,
            None,
            Some(bucket),
            None,
        ),
        BusinessReducerInputV2::PublicGroup {
            group,
            rules,
            finality_policy,
        } => (
            MarketEventPathV1::PublicVisibility,
            group.execution_time.logical_time_utc_ms(),
            group.reference.group_position + 1,
            Some(group.reference.group_position),
            None,
            rules,
            finality_policy,
            Some(group),
            None,
            None,
            None,
        ),
        BusinessReducerInputV2::PublicObserveTime {
            time,
            rules,
            finality_policy,
        } => (
            MarketEventPathV1::PublicVisibility,
            time.logical_time_utc_ms(),
            state.session.as_ref().map_or(0, |session| {
                session.public_visibility_frontier.next_index_position
            }),
            None,
            None,
            rules,
            finality_policy,
            None,
            None,
            None,
            Some(time),
        ),
        BusinessReducerInputV2::PublicCloseTime {
            closure,
            rules,
            finality_policy,
        } => (
            MarketEventPathV1::PublicVisibility,
            closure.execution_time.logical_time_utc_ms(),
            closure.next_group_position,
            None,
            None,
            rules,
            finality_policy,
            None,
            Some(closure),
            None,
            None,
        ),
        _ => {
            return Err(BusinessReductionErrorV2::Fatal(
                SimRunActorErrorV2::MarketEvent(MarketEventErrorV1::InvalidBucket(
                    "逻辑驱动器只消费平台前缀或已封口市场桶",
                )),
            ));
        }
    };
    let (caller_role, frontier, first_due) = {
        let Some(session) = state.session.as_ref() else {
            return Err(unaccepted_business_input_v2(
                state,
                SimRunActorErrorV2::SessionNotCreated,
            ));
        };
        if !matches!(
            session.initialization,
            SessionInitializationV2::Active { .. }
        ) {
            return Err(unaccepted_business_input_v2(
                state,
                SimRunActorErrorV2::SessionNotActive,
            ));
        }
        let frontier = match path {
            MarketEventPathV1::HiddenPlatform => session.hidden_platform_frontier,
            MarketEventPathV1::PublicVisibility => session.public_visibility_frontier,
        };
        let first_due = next_required_logical_drive_v2(&state, rules)
            .map(|required| required.required_at_utc_ms);
        (
            session.opening_fact.account_instance_key.caller_role,
            frontier,
            first_due,
        )
    };
    if path == MarketEventPathV1::HiddenPlatform && caller_role == SimCallerRoleV1::Paper {
        return Err(unaccepted_business_input_v2(
            state,
            SimRunActorErrorV2::HiddenPlatformReplayForbidden,
        ));
    }
    let drives_execution = market_path_drives_execution_for(caller_role, path);
    let finite_obligation = caller_role == SimCallerRoleV1::HistoricalBacktest
        && hidden_bucket.is_none()
        && bucket.is_none()
        && public_group.is_none()
        && public_observation.is_none()
        && public_closure.is_none();
    let common_invalid = (frontier.sealed && !finite_obligation)
        || frontier
            .closed_through_utc_ms
            .is_some_and(|closed| logical_at_utc_ms <= closed)
        || next_index_position < frontier.next_index_position;
    let closes_time = public_group.is_none() && public_observation.is_none();
    // 有限历史源已封口或已给出下一完整桶：宿主可在已证明空档中闭合明确范围尾部。
    let finite_closed_gap = finite_obligation
        && path == MarketEventPathV1::HiddenPlatform
        && (frontier.sealed
            || frontier
                .next_event_at_utc_ms
                .is_some_and(|next| next > logical_at_utc_ms));
    let payload_invalid = if public_group.is_some() {
        start_index_position.is_some_and(|start| start < frontier.next_index_position)
    } else if let Some(hidden) = hidden_bucket {
        start_index_position.is_some_and(|start| start < frontier.next_index_position)
            || next_index_position < hidden.reference.index_start
            || frontier
                .next_event_at_utc_ms
                .is_some_and(|expected| logical_at_utc_ms != expected)
            || matches!(hidden.reference.closure, contracts::ReplayIndexClosureV1::NextGlobalEventAt(next_at) if next_at <= logical_at_utc_ms)
    } else if public_closure.is_some() || public_observation.is_some() {
        false
    } else if let Some(bucket) = &bucket {
        start_index_position.is_some_and(|start| start < frontier.next_index_position)
            || next_index_position < bucket.reference.start_index_position
            || frontier
                .next_event_at_utc_ms
                .is_some_and(|expected| logical_at_utc_ms != expected)
            || matches!(bucket.reference.closure, crate::market::MillisecondBucketClosureV1::NextEventAt(next_at) if next_at <= logical_at_utc_ms)
    } else {
        !drives_execution
            || (!finite_closed_gap && first_due != Some(logical_at_utc_ms))
            || frontier
                .next_event_at_utc_ms
                .is_some_and(|next_at| logical_at_utc_ms >= next_at)
    };
    if common_invalid || payload_invalid {
        let error = if bucket.is_none()
            && (!drives_execution || first_due != Some(logical_at_utc_ms))
        {
            MarketEventErrorV1::InvalidBucket("无正文前缀只在当前执行路径确有最早到期义务时受理")
        } else {
            MarketEventErrorV1::BucketIdentityConflict
        };
        return Err(unaccepted_business_input_v2(
            state,
            SimRunActorErrorV2::MarketEvent(error),
        ));
    }

    if let Some(group) = public_group {
        if let Err(error) = validate_public_group_membership_v1(
            &state.session.as_ref().unwrap().market_directory,
            group,
        ) {
            return Err(unaccepted_business_input_v2(
                state,
                SimRunActorErrorV2::MarketEvent(error),
            ));
        }
    }
    if let Some(bucket) = hidden_bucket {
        let directory = &state.session.as_ref().unwrap().market_directory;
        for record in bucket.records.iter() {
            let result = match record {
                HiddenExecutionRecordV1::MarketFact(fact) => validate_membership_v1(
                    directory,
                    &fact.market,
                    fact_membership_v1(&fact.payload),
                ),
                HiddenExecutionRecordV1::Step { market, step } => {
                    use data_pipeline_core::pm_reduction::PmStepChangeV1 as S;
                    let assets = match &step.change {
                        S::InitialSnapshot { asset, .. } => MembershipAssetsV1::One(asset),
                        S::LevelChange { transition, .. } => {
                            MembershipAssetsV1::One(&transition.asset)
                        }
                        S::BestBidAsk { quote, .. } => MembershipAssetsV1::One(&quote.asset),
                        S::Tick { tick, .. } => MembershipAssetsV1::One(&tick.asset),
                        S::PublicTrade { trade } => MembershipAssetsV1::One(&trade.asset),
                        S::Quality { boundary } => match &boundary.scope {
                            contracts::QualityScopeV1::Asset(a) => MembershipAssetsV1::One(a),
                            _ => MembershipAssetsV1::None,
                        },
                        _ => continue,
                    };
                    validate_membership_v1(directory, market, assets)
                }
                HiddenExecutionRecordV1::Reference(_)
                | HiddenExecutionRecordV1::BinanceDepthReference { .. } => Ok(()),
            };
            if let Err(error) = result {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::MarketEvent(error),
                ));
            }
        }
    }
    let mut output = LogicalDriveOutputV2::default();
    // 仅 PAPER 公共组与在线关闭补入旧返佣。迟入以 logical_at 判定，
    // 实际处理 E 仅影响这些新反馈的 received，不改变经济 due 或普通 timer。
    let late_rebate_processed_at = if caller_role == SimCallerRoleV1::Paper {
        if public_group.is_some() {
            Some(logical_at_utc_ms)
        } else {
            public_closure.and_then(|closure| match closure.closure {
                PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(now) => Some(now),
                _ => None,
            })
        }
    } else {
        None
    };
    if let Some(processed_at) = late_rebate_processed_at {
        while let Some(due) = state
            .session
            .as_ref()
            .and_then(|session| session.execution_state.next_rebate_clock_at_utc_ms())
            .filter(|due| *due < logical_at_utc_ms)
        {
            state = advance_rebate_due_v2(state, due, &mut output)
                .map_err(BusinessReductionErrorV2::Fatal)?;
        }
        output.late_rebate_feedback = Some((output.lifecycle.len(), processed_at));
    }
    if drives_execution && public_observation.is_none() {
        state = drain_scheduled_obligations_at_v2(
            state,
            logical_at_utc_ms,
            rules,
            finality_policy,
            &mut output,
        )?;
    }
    state = advance_role_time_at_v2(state, path, drives_execution, logical_at_utc_ms)?;
    if let Some(bucket) = &bucket {
        let (next, effects) = reduce_market_event_slice_v2(
            state,
            path,
            logical_at_utc_ms,
            bucket.events.as_ref(),
            rules,
            finality_policy,
        )
        .map_err(|error| match error {
            MarketEventSliceReductionErrorV2::Unaccepted { state, error } => {
                BusinessReductionErrorV2::Unaccepted { state, error }
            }
            MarketEventSliceReductionErrorV2::Fatal(error) => {
                BusinessReductionErrorV2::Fatal(error)
            }
        })?;
        state = next;
        for execution in effects.execution {
            output
                .lifecycle
                .extend(execution.into_lifecycle_feedback().map_err(|error| {
                    BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
                })?);
        }
        if bucket.reference.closure == crate::market::MillisecondBucketClosureV1::EndOfInput {
            let session = state.session.as_ref().expect("闭桶归约后唯一会话存在");
            let role_markets = match path {
                MarketEventPathV1::HiddenPlatform => session
                    .hidden_platform_markets
                    .as_ref()
                    .expect("隐藏路径只属于训练或离线评估会话"),
                MarketEventPathV1::PublicVisibility => &session.public_markets,
            };
            role_markets.validate_input_complete().map_err(|error| {
                BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(error))
            })?;
        }
    }
    if let Some(hidden) = hidden_bucket {
        let (next, effects) = reduce_hidden_step_bucket_v1(state, hidden, rules, finality_policy)?;
        state = next;
        for execution in effects.execution {
            output.lifecycle.extend(
                execution.into_lifecycle_feedback().map_err(|e| {
                    BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(e))
                })?,
            );
        }
        if hidden.reference.closure == contracts::ReplayIndexClosureV1::EndOfInput
            && caller_role != SimCallerRoleV1::HistoricalBacktest
        {
            state
                .session
                .as_ref()
                .unwrap()
                .hidden_platform_markets
                .as_ref()
                .unwrap()
                .validate_input_complete()
                .map_err(|e| BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(e)))?;
        }
    }
    if let Some(group) = public_group {
        let (next, effects) = reduce_public_group_v1(state, group, rules, finality_policy)?;
        state = next;
        for execution in effects.execution {
            output.lifecycle.extend(
                execution.into_lifecycle_feedback().map_err(|e| {
                    BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(e))
                })?,
            );
        }
    }
    if public_closure.is_some_and(|c| c.closure == PublicTimeClosureReasonV1::EndOfInput)
        && caller_role != SimCallerRoleV1::HistoricalBacktest
    {
        state
            .session
            .as_ref()
            .unwrap()
            .public_markets
            .validate_input_complete()
            .map_err(|e| BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(e)))?;
    }
    if drives_execution && closes_time {
        while state.session.as_ref().and_then(|session| {
            session
                .mutation_requests
                .pending_actions
                .first_key_value()
                .map(|(key, _)| key.effective_processing_at_utc_ms)
        }) == Some(logical_at_utc_ms)
        {
            let (next, effect) = advance_next_mutation_action_at_v2(
                state,
                logical_at_utc_ms,
                rules,
                finality_policy,
            )?;
            state = next;
            if let Some(effect) = effect {
                output.mutations.push(effect);
            }
        }
    }
    let successful_execution_eof = drives_execution
        && (bucket.as_ref().is_some_and(|bucket| {
            bucket.reference.closure == crate::market::MillisecondBucketClosureV1::EndOfInput
        }) || public_closure
            .is_some_and(|c| c.closure == PublicTimeClosureReasonV1::EndOfInput));
    let successful_execution_eof = successful_execution_eof
        || (drives_execution
            && hidden_bucket.is_some_and(|b| {
                b.reference.closure == contracts::ReplayIndexClosureV1::EndOfInput
            }));
    if successful_execution_eof && caller_role != SimCallerRoleV1::HistoricalBacktest {
        while let Some(next_at) = next_scheduled_obligation_at_v2(&state, rules) {
            state = drain_scheduled_obligations_at_v2(
                state,
                next_at,
                rules,
                finality_policy,
                &mut output,
            )?;
            state = advance_role_time_at_v2(state, path, true, next_at)?;
        }
    }
    let session = state.session.as_mut().expect("逻辑驱动已确认唯一会话");
    let frontier = match path {
        MarketEventPathV1::HiddenPlatform => &mut session.hidden_platform_frontier,
        MarketEventPathV1::PublicVisibility => &mut session.public_visibility_frontier,
    };
    if closes_time {
        frontier.closed_through_utc_ms = Some(logical_at_utc_ms);
    }
    frontier.next_index_position = next_index_position;
    if let Some(bucket) = bucket {
        match bucket.reference.closure {
            crate::market::MillisecondBucketClosureV1::NextEventAt(next_at) => {
                frontier.next_event_at_utc_ms = Some(next_at);
            }
            crate::market::MillisecondBucketClosureV1::EndOfInput => {
                frontier.next_event_at_utc_ms = None;
                frontier.sealed = true;
            }
        }
    }
    if let Some(hidden) = hidden_bucket {
        match hidden.reference.closure {
            contracts::ReplayIndexClosureV1::NextGlobalEventAt(at) => {
                frontier.next_event_at_utc_ms = Some(at)
            }
            contracts::ReplayIndexClosureV1::EndOfInput => {
                frontier.next_event_at_utc_ms = None;
                frontier.sealed = true;
            }
        }
    }
    if let Some(closure) = public_closure {
        match closure.closure {
            PublicTimeClosureReasonV1::NextGlobalEventAt(at) => {
                frontier.next_event_at_utc_ms = Some(at)
            }
            PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(_) => {
                frontier.next_event_at_utc_ms = None;
            }
            PublicTimeClosureReasonV1::EndOfInput => {
                frontier.next_event_at_utc_ms = None;
                frontier.sealed = true;
            }
        }
    }
    Ok((state, BusinessReducerOutputV2::LogicalDrive(output)))
}

#[allow(clippy::result_large_err)]
fn reduce_business_input_v2(
    opened: &RunOpenedV2,
    initial_market_directory: &SessionMarketDirectoryV1,
    mut state: BusinessReducerStateV2,
    input: BusinessReducerInputV2<'_>,
) -> BusinessReductionResultV2 {
    #[cfg(test)]
    record_reducer_call_for_tests();
    let output = match input {
        BusinessReducerInputV2::CreateSession(spec) => {
            if state.session.is_some() {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::SessionAlreadyCreated,
                ));
            }
            if let Err(error) = validate_session_spec(spec, &opened.run_id) {
                return Err(unaccepted_business_input_v2(state, error));
            }
            let account_instance_key = AccountInstanceKeyV1 {
                run_id: opened.run_id.clone(),
                caller_role: spec.caller_role,
                caller_instance_id: spec.caller_instance_id.clone(),
                account_id: spec.account_id.clone(),
                platform_source: PlatformSourceV1::Simulated,
            };
            let opening_fact = Arc::new(AccountOpeningFactV2 {
                creation_request_id: spec.creation_request_id.clone(),
                account_instance_key,
                session_id: spec.session_id.clone(),
                initial_platform_scenario_file_sha256: opened.platform_scenario_file_sha256.clone(),
                initial_account_state: spec.initial_account_state.clone(),
            });
            let account = match PlatformAccountStateV2::try_from_opening_fact(&opening_fact) {
                Ok(account) => account,
                Err(error) => {
                    return Err(unaccepted_business_input_v2(
                        state,
                        SimRunActorErrorV2::Account(error),
                    ));
                }
            };
            let market_directory = initial_market_directory.clone();
            let public_markets = MarketStateStoreV2::new(&market_directory);
            let hidden_platform_markets = match spec.caller_role {
                SimCallerRoleV1::ModelTrain
                | SimCallerRoleV1::OfflineEvaluation
                | SimCallerRoleV1::HistoricalBacktest => {
                    Some(MarketStateStoreV2::new(&market_directory))
                }
                SimCallerRoleV1::Paper => None,
            };
            let execution_state = ExecutionStateV2::new(&market_directory, account);

            state.session = Some(UniqueSessionStateV2 {
                opening_fact: Arc::clone(&opening_fact),
                response_bridge_binding_id: spec.response_bridge_binding_id.clone(),
                session_seed: spec.session_seed.clone(),
                initialization: SessionInitializationV2::PreparedLedgerInit,
                market_directory,
                public_markets,
                hidden_platform_markets,
                execution_state,
                market_enrollment: MarketEnrollmentStateV2::default(),
                mutation_requests: MutationRequestStateV2::default(),
                query_state: QueryStateV2::default(),
                next_private_feedback_sequence: 1,
                next_projection_batch_sequence: 1,
                hidden_platform_frontier: LogicalPathFrontierV2::default(),
                public_visibility_frontier: LogicalPathFrontierV2::default(),
            });

            BusinessReducerOutputV2::CreateSession(CreateSessionControlResultV2 {
                disposition: CreateSessionDispositionV2::NewPrepared,
                opening_fact,
            })
        }
        BusinessReducerInputV2::ActivateSession(activation_id) => {
            let Some(session) = state.session.as_mut() else {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::SessionNotCreated,
                ));
            };
            match &session.initialization {
                SessionInitializationV2::Active { .. }
                | SessionInitializationV2::InitializationFailedClosed { .. } => {
                    return Err(unaccepted_business_input_v2(
                        state,
                        SimRunActorErrorV2::InitializationAlreadyDecided,
                    ));
                }
                SessionInitializationV2::PreparedLedgerInit => {}
            }
            if !is_stable_text(activation_id) {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::InvalidSessionSpec("activation_id"),
                ));
            }
            session.initialization = SessionInitializationV2::Active {
                activation_id: activation_id.to_owned(),
            };

            BusinessReducerOutputV2::SessionInitialization(
                SessionInitializationControlResultV2::Activated,
            )
        }
        BusinessReducerInputV2::FailSessionInitialization(failure_id) => {
            let Some(session) = state.session.as_mut() else {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::SessionNotCreated,
                ));
            };
            match &session.initialization {
                SessionInitializationV2::Active { .. }
                | SessionInitializationV2::InitializationFailedClosed { .. } => {
                    return Err(unaccepted_business_input_v2(
                        state,
                        SimRunActorErrorV2::InitializationAlreadyDecided,
                    ));
                }
                SessionInitializationV2::PreparedLedgerInit => {}
            }
            if !is_stable_text(failure_id) {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::InvalidSessionSpec("failure_id"),
                ));
            }
            session.initialization = SessionInitializationV2::InitializationFailedClosed {
                failure_id: failure_id.to_owned(),
            };

            BusinessReducerOutputV2::SessionInitialization(
                SessionInitializationControlResultV2::FailedClosed,
            )
        }
        BusinessReducerInputV2::EnrollMarket(request) => {
            let Some(session) = state.session.as_mut() else {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::SessionNotCreated,
                ));
            };
            let enrollment = match session.enroll_market(request) {
                Ok(enrollment) => enrollment,
                Err(error) => return Err(unaccepted_business_input_v2(state, error)),
            };

            BusinessReducerOutputV2::MarketEnrollment(enrollment)
        }
        BusinessReducerInputV2::RegisterNewRequest {
            cutoff,
            request,
            input_sequence,
            latency_profile,
            rules,
        } => {
            let (reduced, receipt) = register_new_request_v2(
                state,
                cutoff,
                request,
                input_sequence,
                latency_profile,
                rules,
            )
            .map_err(|(state, error)| unaccepted_business_input_v2(state, error))?;
            state = reduced;
            BusinessReducerOutputV2::RequestRegistration(receipt)
        }
        BusinessReducerInputV2::AdvanceNextMutationActionAt {
            platform_effective_at_utc_ms,
            rules,
            finality_policy,
        } => {
            let (reduced, effect) = advance_next_mutation_action_at_v2(
                state,
                platform_effective_at_utc_ms,
                rules,
                finality_policy,
            )?;
            state = reduced;
            BusinessReducerOutputV2::MutationAction(effect)
        }
        BusinessReducerInputV2::RetentionAdvance(advance) => {
            let Some(session) = state.session.as_mut() else {
                return Err(unaccepted_business_input_v2(
                    state,
                    SimRunActorErrorV2::SessionNotCreated,
                ));
            };
            match session.query_state.advance_retention(advance) {
                Ok(()) => (),
                Err(error) => return Err(unaccepted_business_input_v2(state, error)),
            };

            BusinessReducerOutputV2::RetentionAdvanced
        }
        logical @ (BusinessReducerInputV2::PlatformPrefixAdvance { .. }
        | BusinessReducerInputV2::ClosedMarketBucket { .. }
        | BusinessReducerInputV2::HiddenStepBucket { .. }
        | BusinessReducerInputV2::PublicGroup { .. }
        | BusinessReducerInputV2::PublicCloseTime { .. }
        | BusinessReducerInputV2::PublicObserveTime { .. }) => {
            return drive_business_input_at_v2(state, logical);
        }
    };
    Ok((state, output))
}

#[allow(clippy::result_large_err)]
fn reduce_market_event_slice_v2<I: crate::market::MarketEventIdentityV1, E>(
    state: BusinessReducerStateV2,
    path: MarketEventPathV1,
    platform_effective_at_utc_ms: i64,
    events: &[ExecutionMarketEventV3<I, E>],
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<(BusinessReducerStateV2, SessionMarketEventEffectsV2), MarketEventSliceReductionErrorV2>
{
    let prechecked = (|| {
        let session = state
            .session
            .as_ref()
            .ok_or(SimRunActorErrorV2::SessionNotCreated)?;
        if !matches!(
            session.initialization,
            SessionInitializationV2::Active { .. }
        ) {
            return Err(SimRunActorErrorV2::SessionNotActive);
        }
        let caller_role = session.opening_fact.account_instance_key.caller_role;
        if caller_role == SimCallerRoleV1::Paper && path == MarketEventPathV1::HiddenPlatform {
            return Err(SimRunActorErrorV2::HiddenPlatformReplayForbidden);
        }
        events
            .iter()
            .map(|event| {
                validate_event_membership_v2(&session.market_directory, event)
                    .map_err(SimRunActorErrorV2::MarketEvent)?;
                normalize_market_event_v2(event).map_err(SimRunActorErrorV2::MarketEvent)
            })
            .collect::<Result<Vec<_>, _>>()
    })();
    let normalized = match prechecked {
        Ok(normalized) => normalized,
        Err(error) => {
            return Err(MarketEventSliceReductionErrorV2::Unaccepted { state, error });
        }
    };

    let keys: Vec<_> = events
        .iter()
        .map(|event| event.event_id.identity_key())
        .collect();
    let headers: Vec<_> = events
        .iter()
        .zip(&keys)
        .map(|(event, identity)| crate::market::MarketEventHeaderV1 {
            identity,
            market: &event.market,
            time: &event.time,
            quality: &event.quality,
        })
        .collect();
    reduce_normalized_market_slice_v1(
        state,
        path,
        platform_effective_at_utc_ms,
        &headers,
        &normalized,
        rules,
        finality_policy,
    )
}

fn reduce_normalized_market_slice_v1(
    mut state: BusinessReducerStateV2,
    path: MarketEventPathV1,
    platform_effective_at_utc_ms: i64,
    events: &[crate::market::MarketEventHeaderV1<'_>],
    normalized: &[crate::market::NormalizedMarketEventV2<'_>],
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<(BusinessReducerStateV2, SessionMarketEventEffectsV2), MarketEventSliceReductionErrorV2>
{
    let session = state
        .session
        .as_mut()
        .expect("市场事件预检已经确认唯一会话存在");
    let caller_role = session.opening_fact.account_instance_key.caller_role;
    let drives_execution = market_path_drives_execution_for(caller_role, path);
    let mut effects = SessionMarketEventEffectsV2::default();

    let mut query_projection_changed = false;
    for (event, normalized) in events.iter().zip(normalized) {
        let end_at_utc_ms = session
            .market_directory
            .market(&event.market.condition_id.0)
            .expect("市场事件预检已经确认目录成员存在")
            .market()
            .window
            .end_at_utc_ms;
        let market_application = {
            let role_markets = match path {
                MarketEventPathV1::HiddenPlatform => session
                    .hidden_platform_markets
                    .as_mut()
                    .expect("只有训练或离线评估会话可以通过隐藏路径预检"),
                MarketEventPathV1::PublicVisibility => &mut session.public_markets,
            };
            role_markets
                .apply_admitted_header(
                    event,
                    normalized,
                    platform_effective_at_utc_ms,
                    end_at_utc_ms,
                )
                .map_err(|error| {
                    MarketEventSliceReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(error))
                })?
        };

        if drives_execution {
            let mut execution = session
                .execution_state
                .apply_market_event(
                    &event.market.condition_id.0,
                    normalized,
                    platform_effective_at_utc_ms,
                    end_at_utc_ms,
                    rules,
                    finality_policy,
                )
                .map_err(|error| {
                    MarketEventSliceReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(error))
                })?;
            if let Some(reason_code) = market_application.resolution_invalidation_reason {
                session
                    .execution_state
                    .invalidate_qualified_resolution(&event.market.condition_id.0, reason_code);
            }
            if let Some(qualified_resolution) = market_application.qualified_resolution {
                execution.merge(
                    session
                        .execution_state
                        .apply_qualified_resolution(
                            &event.market.condition_id.0,
                            &qualified_resolution,
                            platform_effective_at_utc_ms,
                            finality_policy,
                        )
                        .map_err(|error| {
                            MarketEventSliceReductionErrorV2::Fatal(SimRunActorErrorV2::Matching(
                                error,
                            ))
                        })?,
                );
            }

            query_projection_changed |= execution.query_projection_changed();
            effects.execution.push(execution);
        }
        let UniqueSessionStateV2 {
            execution_state,
            public_markets,
            hidden_platform_markets,
            ..
        } = session;
        if let Some(redeemed_resolution) =
            execution_state.redeemed_market_resolution(&event.market.condition_id.0)
        {
            let role_markets = match path {
                MarketEventPathV1::HiddenPlatform => hidden_platform_markets
                    .as_mut()
                    .expect("只有训练或离线评估会话可以通过隐藏路径预检"),
                MarketEventPathV1::PublicVisibility => public_markets,
            };
            let _ = role_markets.mark_resolution_terminal_if_qualified(
                &event.market.condition_id.0,
                redeemed_resolution,
            );
        }
    }

    session.market_enrollment.business_input_started = true;
    if query_projection_changed {
        session.query_state.advance_revision();
    }
    Ok((state, effects))
}

enum MembershipAssetsV1<'a> {
    None,
    One(&'a contracts::AssetIdentityV1),
    Exact(&'a [contracts::AssetIdentityV1]),
    Terminal(&'a contracts::MarketResolutionTerminalV3),
}
fn validate_membership_v1(
    directory: &SessionMarketDirectoryV1,
    market: &contracts::MarketIdentityV2,
    assets: MembershipAssetsV1<'_>,
) -> Result<(), MarketEventErrorV1> {
    let member = directory
        .market(&market.condition_id.0)
        .ok_or_else(|| MarketEventErrorV1::UnknownMarket(market.condition_id.0.clone()))?;
    if member.market() != market {
        return Err(MarketEventErrorV1::MarketIdentityConflict(
            market.condition_id.0.clone(),
        ));
    }
    match assets {
        MembershipAssetsV1::None => {}
        MembershipAssetsV1::One(asset) => {
            if !member.assets().contains(asset) {
                return Err(MarketEventErrorV1::UnknownAsset(asset.token_id.0.clone()));
            }
        }
        MembershipAssetsV1::Exact(assets) => {
            if assets != member.assets().as_slice() {
                return Err(MarketEventErrorV1::ResolutionAssetConflict);
            }
        }
        MembershipAssetsV1::Terminal(contracts::MarketResolutionTerminalV3::Resolved {
            payout_vector,
            ..
        }) => {
            if payout_vector.len() != member.assets().len()
                || !payout_vector
                    .iter()
                    .map(|p| &p.asset)
                    .eq(member.assets().iter())
            {
                return Err(MarketEventErrorV1::ResolutionAssetConflict);
            }
        }
    }
    Ok(())
}
fn fact_membership_v1(payload: &contracts::CanonicalMarketFactPayloadV1) -> MembershipAssetsV1<'_> {
    use contracts::CanonicalMarketFactPayloadV1 as F;
    match payload {
        F::FixedIdentity { assets } | F::MarketLifecycleSnapshot { assets, .. } => {
            MembershipAssetsV1::Exact(assets)
        }
        F::InitialTick { asset, .. } => MembershipAssetsV1::One(asset),
        F::LifecycleTransition(t) => MembershipAssetsV1::Exact(&t.assets),
        F::Terminal(t) => MembershipAssetsV1::Terminal(t),
        F::QualityBoundary(b) => match &b.scope {
            contracts::QualityScopeV1::Asset(a) => MembershipAssetsV1::One(a),
            _ => MembershipAssetsV1::None,
        },
        F::TradingState(_) => MembershipAssetsV1::None,
    }
}
fn validate_event_membership_v2<I, E>(
    directory: &SessionMarketDirectoryV1,
    event: &ExecutionMarketEventV3<I, E>,
) -> Result<(), MarketEventErrorV1> {
    use ExecutionMarketEventPayloadV3 as F;
    let assets = match &event.payload {
        F::MarketLifecycleTransition(t) => MembershipAssetsV1::Exact(&t.assets),
        F::InitialL2Snapshot(v) => MembershipAssetsV1::One(&v.asset),
        F::BestBidAsk(v) => MembershipAssetsV1::One(&v.asset),
        F::TickSizeChange(v) => MembershipAssetsV1::One(&v.asset),
        F::PublicTrade(v) => MembershipAssetsV1::One(&v.asset),
        F::LevelExecutionTransition(v) => MembershipAssetsV1::One(&v.asset),
        F::QualityBoundary(b) => match &b.scope {
            contracts::QualityScopeV1::Asset(a) => MembershipAssetsV1::One(a),
            _ => MembershipAssetsV1::None,
        },
        F::MarketResolutionTerminal(t) => MembershipAssetsV1::Terminal(t),
        F::MarketTradingState(_) => MembershipAssetsV1::None,
    };
    validate_membership_v1(directory, &event.market, assets)
}
fn validate_public_group_membership_v1(
    directory: &SessionMarketDirectoryV1,
    group: &AcceptedPublicGroupV1,
) -> Result<(), MarketEventErrorV1> {
    use contracts::{
        CanonicalBookFactV1 as B, CanonicalPmPayloadV1 as P, PublicBusinessPayloadV1 as F,
    };
    for fact in group.prepared.group.records.iter() {
        match &fact.payload {
            F::MarketFact { market, payload } => {
                validate_membership_v1(directory, market, fact_membership_v1(payload))?
            }
            F::PolymarketMessage(message) => {
                validate_membership_v1(directory, &message.market, MembershipAssetsV1::None)?;
                match &message.payload {
                    P::Book {
                        fact: B::FullBook { asset, .. },
                    }
                    | P::BestBidAsk { asset, .. }
                    | P::TickSizeChange { asset, .. }
                    | P::PublicTrade { asset, .. } => validate_membership_v1(
                        directory,
                        &message.market,
                        MembershipAssetsV1::One(asset),
                    )?,
                    P::Book {
                        fact: B::AbsoluteLevelChanges { changes },
                    } => {
                        for change in changes {
                            validate_membership_v1(
                                directory,
                                &message.market,
                                MembershipAssetsV1::One(&change.asset),
                            )?;
                        }
                    }
                    P::NewMarket { .. } => {}
                }
            }
            F::ReferenceFact(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static REDUCER_CALL_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static PROJECTION_BATCH_CONSTRUCTION_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static GROUP_STATE_INSTALL_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_reducer_call_for_tests() {
    REDUCER_CALL_COUNT.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
fn reset_reducer_call_count_for_tests() {
    REDUCER_CALL_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn reducer_call_count_for_tests() -> u64 {
    REDUCER_CALL_COUNT.with(std::cell::Cell::get)
}

#[cfg(test)]
fn record_projection_batch_construction_for_tests() {
    PROJECTION_BATCH_CONSTRUCTION_COUNT.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
fn record_group_state_install_for_tests() {
    GROUP_STATE_INSTALL_COUNT.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
fn reset_group_commit_counts_for_tests() {
    reset_reducer_call_count_for_tests();
    PROJECTION_BATCH_CONSTRUCTION_COUNT.with(|count| count.set(0));
    GROUP_STATE_INSTALL_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn group_commit_counts_for_tests() -> (u64, u64, u64) {
    (
        reducer_call_count_for_tests(),
        PROJECTION_BATCH_CONSTRUCTION_COUNT.with(std::cell::Cell::get),
        GROUP_STATE_INSTALL_COUNT.with(std::cell::Cell::get),
    )
}

fn validate_session_spec(
    spec: &SessionSpecV3,
    expected_run_id: &str,
) -> Result<(), SimRunActorErrorV2> {
    for (field, value) in [
        ("run_id", spec.run_id.as_str()),
        ("session_id", spec.session_id.as_str()),
        ("account_id", spec.account_id.as_str()),
        ("caller_instance_id", spec.caller_instance_id.as_str()),
        ("creation_request_id", spec.creation_request_id.as_str()),
        (
            "response_bridge_binding_id",
            spec.response_bridge_binding_id.as_str(),
        ),
    ] {
        if !is_stable_text(value) {
            return Err(SimRunActorErrorV2::InvalidSessionSpec(field));
        }
    }
    if spec.run_id != expected_run_id {
        return Err(SimRunActorErrorV2::InvalidSessionSpec("run_id"));
    }
    Ok(())
}

fn is_stable_text(value: &str) -> bool {
    !value.is_empty() && value.trim() == value
}

#[cfg(feature = "measurement")]
fn market_payload_bytes(market: &PlatformMarketV1) -> u64 {
    let identity = market.market();
    (identity.condition_id.0.len()
        + identity.clob_market_id.0.len()
        + market
            .assets()
            .iter()
            .map(|asset| asset.token_id.0.len() + asset.outcome.label.len())
            .sum::<usize>()) as u64
}

#[cfg(feature = "measurement")]
fn post_result_resources_v2(effect: &PostOrderItemBusinessEffectV2) -> ResourceQuantityV1 {
    let mut resources = ResourceQuantityV1::default();
    let mut owned_bytes =
        (effect.item_id.len() + effect.condition_id.0.len() + effect.token_id.0.len()) as u64;
    if let PostOrderItemOutcomeV2::Accepted(accepted) = &effect.outcome {
        owned_bytes += (accepted.platform_order_id.len() + accepted.platform_order_status.raw.len())
            as u64
            + submission_measurement_owned_bytes_v2(&accepted.measurement);
        for fill in &accepted.fills {
            resources.add_record(
                (fill.trade_id.len()
                    + fill.source_price_pusd_per_share.0.len()
                    + fill.base_quantity_shares.0.len()
                    + fill.quote_amount_pusd.0.len()) as u64,
            );
        }
        resources.add_records(accepted.account_transition_sequences.len() as u64);
    }
    resources.add_record(owned_bytes);
    resources
}

#[cfg(feature = "measurement")]
fn query_result_resources_v2(result: &QueryActionResultV2) -> ResourceQuantityV1 {
    let mut resources = ResourceQuantityV1::default();
    resources.add_record(0);
    match result {
        QueryActionResultV2::Order(OrderQueryResultV2::Found(order)) => {
            resources.add_record(order_projection_owned_bytes_v2(order));
        }
        QueryActionResultV2::Orders(OrdersQueryResultV2::Page(page)) => {
            for order in &page.items {
                resources.add_record(order_projection_owned_bytes_v2(order));
            }
        }
        QueryActionResultV2::Trades(TradesQueryResultV2::Page(page)) => {
            for trade in &page.items {
                resources.add_record(trade_projection_owned_bytes_v2(trade));
            }
        }
        QueryActionResultV2::Order(
            OrderQueryResultV2::NotFound | OrderQueryResultV2::OutOfRetention(_),
        )
        | QueryActionResultV2::Orders(
            OrdersQueryResultV2::InvalidCursor | OrdersQueryResultV2::OutOfRetention(_),
        )
        | QueryActionResultV2::Trades(
            TradesQueryResultV2::InvalidCursor | TradesQueryResultV2::OutOfRetention(_),
        ) => {}
    }
    resources
}

#[cfg(feature = "measurement")]
fn direct_response_draft_owned_bytes_v2<R>(draft: &DirectResponseDraft<R>) -> u64 {
    (draft.global_request_id.len() + draft.caller_correlation_id.as_ref().map_or(0, String::len))
        as u64
        + match &draft.payload {
            DirectResponsePayloadDraft::PostOrder(item) => post_result_resources_v2(item).bytes,
            DirectResponsePayloadDraft::PostOrders(items) => items
                .iter()
                .map(|item| post_result_resources_v2(item).bytes)
                .sum(),
            DirectResponsePayloadDraft::CancelOrder(item) => cancel_result_owned_bytes_v2(item),
            DirectResponsePayloadDraft::CancelOrders(items)
            | DirectResponsePayloadDraft::CancelMarketOrders(items)
            | DirectResponsePayloadDraft::CancelAllOrders(items) => items
                .iter()
                .map(|item| cancel_result_owned_bytes_v2(item))
                .sum(),
            DirectResponsePayloadDraft::Order(item)
            | DirectResponsePayloadDraft::Orders(item)
            | DirectResponsePayloadDraft::Trades(item) => {
                query_result_resources_v2(&item.result).bytes
            }
        }
}

#[cfg(feature = "measurement")]
fn business_resource_snapshot_v2(state: &BusinessReducerStateV2) -> ResourceQuantityV1 {
    let mut total = ResourceQuantityV1::default();
    let Some(session) = &state.session else {
        return total;
    };
    let opening = &session.opening_fact;
    total.add_record(
        account_key_owned_bytes_v2(&opening.account_instance_key)
            + (opening.creation_request_id.len()
                + opening.session_id.len()
                + opening.initial_platform_scenario_file_sha256.0.len()
                + opening.initial_account_state.available_pusd.0.len()
                + session.response_bridge_binding_id.len()
                + session.session_seed.0.len()) as u64,
    );
    total.bytes += match &session.initialization {
        SessionInitializationV2::PreparedLedgerInit => 0,
        SessionInitializationV2::Active { activation_id } => activation_id.len() as u64,
        SessionInitializationV2::InitializationFailedClosed { failure_id } => {
            failure_id.len() as u64
        }
    };
    for market in session.market_directory.markets() {
        total.add_record(market_payload_bytes(market));
    }
    total.include(session.public_markets.measurement_resources());
    if let Some(hidden) = &session.hidden_platform_markets {
        total.include(hidden.measurement_resources());
    }
    total.include(session.execution_state.measurement_resources());
    for record in &session.market_enrollment.records {
        total.add_record(
            (record.enrollment_request_id.len()
                + record.market.condition_id.0.len()
                + record.market.clob_market_id.0.len()
                + record
                    .assets
                    .iter()
                    .map(|a| a.token_id.0.len() + a.outcome.label.len())
                    .sum::<usize>()) as u64,
        );
    }
    for id in session
        .market_enrollment
        .records_by_request_id
        .keys()
        .chain(
            session
                .market_enrollment
                .paper_candidate_condition_ids
                .iter(),
        )
        .chain(session.market_enrollment.paper_candidate_token_ids.iter())
    {
        total.add_record(id.len() as u64);
    }
    for (id, request) in &session.mutation_requests.requests_by_id {
        total.add_record(
            (id.len()
                + request
                    .caller_correlation_id
                    .as_ref()
                    .map_or(0, String::len)) as u64,
        );
        for post in &request.post_results {
            total.include(post_result_resources_v2(post));
        }
        for cancel in &request.cancel_results {
            total.add_record(cancel_result_owned_bytes_v2(cancel));
        }
        if let Some(query) = &request.query_result {
            total.include(query_result_resources_v2(&query.result));
        }
        if let Some(response) = &request.completed_response {
            match response.as_ref() {
                StoredDirectResponse::Final(response) => {
                    total.add_record(direct_response_owned_bytes_v2(response))
                }
                StoredDirectResponse::Draft(draft) => {
                    total.add_record(direct_response_draft_owned_bytes_v2(draft))
                }
            }
        }
    }
    for action in session.mutation_requests.pending_actions.values() {
        let bytes = match action {
            MutationActionV2::PostItem {
                global_request_id,
                item,
                ..
            } => post_action_owned_bytes_v2(global_request_id, item),
            MutationActionV2::CancelTarget {
                global_request_id,
                platform_order_id,
                input_item_refs,
                ..
            } => {
                cancel_action_owned_bytes_v2(global_request_id, platform_order_id, input_item_refs)
            }
            MutationActionV2::CancelMarket {
                global_request_id,
                condition_id,
            } => cancel_market_action_owned_bytes_v2(global_request_id, condition_id),
            MutationActionV2::CancelAll { global_request_id } => global_request_id.len() as u64,
            MutationActionV2::OrderQuery {
                global_request_id,
                platform_order_id,
            } => order_query_action_owned_bytes_v2(global_request_id, platform_order_id),
            MutationActionV2::OrdersQuery {
                global_request_id,
                filter,
                ..
            } => orders_query_action_owned_bytes_v2(global_request_id, filter),
            MutationActionV2::TradesQuery {
                global_request_id,
                filter,
                ..
            } => trades_query_action_owned_bytes_v2(global_request_id, filter),
        };
        total.add_record(bytes);
    }
    for ((_, key), root) in &session.query_state.order_roots {
        total.add_record(orders_query_key_owned_bytes_v2(key));
        for row in root {
            total.add_record(order_projection_owned_bytes_v2(row));
        }
    }
    for ((_, key), root) in &session.query_state.trade_roots {
        total.add_record(trades_query_key_owned_bytes_v2(key));
        for row in root {
            total.add_record(trade_projection_owned_bytes_v2(row));
        }
    }
    total
}

#[cfg(test)]
mod tests {
    include!("limit_cost_submission_tests.rs");
    use std::cell::RefCell;
    use std::fs;
    use std::io::Write;
    use std::rc::Rc;

    use contracts::{
        AssetIdentityV1, BookSide, ClobMarketId, ConditionId, ExactDecimal, InitialAccountStateV2,
        LevelExecutionBasis, LevelExecutionTransitionV3, MarketIdentityV2, MarketWindowV2,
        OutcomeIdentityV1, PublicTradeIdSourceV1, QueueResultQuality, ResyncQuality,
        SimCallerRoleV1, SourceAttributionQuality, TokenId,
    };
    use data_pipeline_training::{
        ClosedExecutionMarketIndexBucketV1, GateASimulationReplayAdapter, ReplayIndexClosureV1,
        gate_a_formal_output_config, open_published_gate_a_final,
    };
    use rust_decimal::Decimal;

    use super::*;

    use crate::event_log::{SIM_EVENT_LOG_FILE_NAME_V2, SimEventLogRecoveryV2};
    use crate::finality::successful_policy_v2_for_tests;
    use crate::latency_profile::load_embedded_proxy_latency_profile;
    use crate::market::MillisecondBucketClosureV1;
    use crate::matching::BuyOrderAcceptedV2;
    use crate::platform_rules::{
        CurrentProcessSnapshotRequestV1, ImplementationIdentityV1, ReferenceMarketSpecV1,
        ReferenceOutcomeSpecV1, query_and_seal_current_process_snapshot_v3,
        tests::successful_snapshot_v3_for_sim_tests,
    };
    use crate::runtime_v2::{
        CancelAllOrdersRequestV2, CancelMarketOrdersRequestV2, CancelOrderBatchItemV2,
        CancelOrderRequestV2, CancelOrdersRequestV2, LoggedBusinessInputV2, OrderRequestV2,
        OrdersRequestV2, PostOrderBatchItemV2, PostOrderRequestV2, PostOrdersRequestV2,
        RequestContextV2, RunDurabilityRoleV2, TradesRequestV2,
    };
    use crate::scenario::{PLATFORM_SCENARIO_SCHEMA_V1, SimulationScenarioArtifactV1};
    use crate::session::PublicVisibilityCursorV1;

    mod noe_resource_tests {
        include!("noe_resource_tests.rs");
    }

    const MAX_GROUP_BYTES: usize = 64 * 1024;

    #[derive(Default)]
    struct AcceptingProjectionPortV2 {
        durable_through: u64,
    }

    impl BusinessProjectionPortV2 for AcceptingProjectionPortV2 {
        fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
            self.durable_through = batch.projection_batch_sequence;
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.durable_through
        }
    }

    fn accepting_projection_port() -> Box<dyn BusinessProjectionPortV2> {
        Box::<AcceptingProjectionPortV2>::default()
    }

    #[derive(Default)]
    struct ControllableProjectionPortStateV2 {
        accept_limit: usize,
        durable_through: u64,
        accepted: Vec<Arc<BusinessProjectionBatchV2>>,
    }

    struct ControllableProjectionPortV2 {
        state: Rc<RefCell<ControllableProjectionPortStateV2>>,
    }

    impl BusinessProjectionPortV2 for ControllableProjectionPortV2 {
        fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
            let mut state = self.state.borrow_mut();
            if state.accepted.len() >= state.accept_limit {
                return false;
            }
            state.accepted.push(batch);
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.state.borrow().durable_through
        }
    }

    #[derive(Default)]
    struct ControllableProjectionPortStateV4 {
        accept_limit: usize,
        durable_through: u64,
        accepted: Vec<Arc<BusinessProjectionBatchV4>>,
    }

    struct ControllableProjectionPortV4 {
        state: Rc<RefCell<ControllableProjectionPortStateV4>>,
    }

    impl BusinessProjectionPortV2 for ControllableProjectionPortV4 {
        fn try_accept(&mut self, _: Arc<BusinessProjectionBatchV2>) -> bool {
            false
        }
        fn try_accept_payload(&mut self, payload: Arc<BusinessProjectionBatchPayloadV1>) -> bool {
            let BusinessProjectionBatchPayloadV1::V4(batch) = payload.as_ref() else {
                return false;
            };
            let mut state = self.state.borrow_mut();
            if state.accepted.len() >= state.accept_limit {
                return false;
            }
            state.accepted.push(batch.clone());
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.state.borrow().durable_through
        }
    }

    #[cfg(feature = "measurement")]
    #[derive(Default)]
    struct CapturingProjectionPortStateV2 {
        durable_through: u64,
        accepted: Vec<Arc<BusinessProjectionBatchV2>>,
    }

    #[cfg(feature = "measurement")]
    struct CapturingProjectionPortV2 {
        state: Rc<RefCell<CapturingProjectionPortStateV2>>,
    }

    #[cfg(feature = "measurement")]
    impl BusinessProjectionPortV2 for CapturingProjectionPortV2 {
        fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
            let mut state = self.state.borrow_mut();
            state.durable_through = batch.projection_batch_sequence;
            state.accepted.push(batch);
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.state.borrow().durable_through
        }
    }

    #[cfg(feature = "measurement")]
    fn direct_response_request_id(response: &BusinessDirectResponseV2) -> &str {
        match response {
            BusinessDirectResponseV2::PostOrder(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::PostOrders(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::CancelOrder(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::CancelOrders(response) => {
                &response.metadata.global_request_id
            }
            BusinessDirectResponseV2::CancelMarketOrders(response) => {
                &response.metadata.global_request_id
            }
            BusinessDirectResponseV2::CancelAllOrders(response) => {
                &response.metadata.global_request_id
            }
            BusinessDirectResponseV2::Order(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::Orders(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::Trades(response) => &response.metadata.global_request_id,
        }
    }

    #[cfg(feature = "measurement")]
    fn captured_direct_response_json(
        state: &Rc<RefCell<CapturingProjectionPortStateV2>>,
        request_id: &str,
    ) -> serde_json::Value {
        let state = state.borrow();
        let response = state
            .accepted
            .iter()
            .rev()
            .flat_map(|batch| batch.direct_responses())
            .find(|response| direct_response_request_id(response) == request_id)
            .unwrap_or_else(|| panic!("未捕获公开请求响应：{request_id}"));
        match response {
            BusinessDirectResponseV2::PostOrder(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::PostOrders(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::CancelOrder(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::CancelOrders(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::CancelMarketOrders(response) => {
                serde_json::to_value(response)
            }
            BusinessDirectResponseV2::CancelAllOrders(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::Order(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::Orders(response) => serde_json::to_value(response),
            BusinessDirectResponseV2::Trades(response) => serde_json::to_value(response),
        }
        .expect("编码公开请求响应")
    }

    #[cfg(feature = "measurement")]
    fn drive_public_until_response(
        execution: &mut SimExecution,
        state: &Rc<RefCell<CapturingProjectionPortStateV2>>,
        request_id: &str,
        next_index_position: u64,
    ) {
        for _ in 0..16 {
            if state
                .borrow()
                .accepted
                .iter()
                .flat_map(|batch| batch.direct_responses())
                .any(|response| direct_response_request_id(response) == request_id)
            {
                return;
            }
            let required = execution
                .next_required_logical_drive()
                .unwrap_or_else(|| panic!("请求 {request_id} 尚未完成却无必要逻辑推进"));
            execution
                .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position,
                })
                .unwrap_or_else(|error| panic!("推进请求 {request_id} 失败：{error}"));
        }
        panic!("请求 {request_id} 在固定推进次数内未完成");
    }

    fn sha(byte: char) -> Sha256Hex {
        Sha256Hex(std::iter::repeat_n(byte, 64).collect())
    }

    fn sealed_scenario() -> Arc<SealedPlatformScenarioV1> {
        let scenario = crate::scenario::scenario_for_sim_tests();
        let artifact = SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            markets: scenario.market_set().markets().to_vec(),
        };
        let bytes = serde_json::to_vec(&artifact).expect("编码场景文件测试夹具");
        Arc::new(SealedPlatformScenarioV1::decode_and_seal(&bytes).expect("解码并密封场景文件"))
    }

    fn sealed_scenario_for_markets(
        markets: Vec<PlatformMarketV1>,
    ) -> Arc<SealedPlatformScenarioV1> {
        let scenario = crate::scenario::PlatformScenarioV1::try_new(markets)
            .expect("构造多市场逻辑驱动测试场景");
        let artifact = SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            markets: scenario.market_set().markets().to_vec(),
        };
        let bytes = serde_json::to_vec(&artifact).expect("编码多市场逻辑驱动测试场景");
        Arc::new(
            SealedPlatformScenarioV1::decode_and_seal(&bytes)
                .expect("解码并密封多市场逻辑驱动测试场景"),
        )
    }

    fn opened(
        run_id: &str,
        rule_snapshot: &SealedProcessRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
        scenario: &SealedPlatformScenarioV1,
        latency_profile: &EmpiricalDelayProfile,
    ) -> RunOpenedV2 {
        RunOpenedV2 {
            projection_version: Default::default(),
            run_id: run_id.to_owned(),
            durability_role: RunDurabilityRoleV2::CleanRun,
            platform_rule_snapshot_file_sha256: rule_snapshot.file_sha256().clone(),
            trade_finality_policy_file_sha256: finality_policy.policy_id().clone(),
            platform_scenario_file_sha256: scenario.file_sha256().clone(),
            proxy_latency_profile_file_sha256: latency_profile.file_sha256().clone(),
            implementation_identity: ImplementationIdentityV1 {
                git_commit: "a".repeat(40),
                source_identity_sha256: sha('5'),
                cargo_lock_sha256: sha('6'),
            },
            market_event_source_identity_sha256: Some(sha('7')),
        }
    }

    fn spec(run_id: &str, creation_request_id: &str) -> SessionSpecV3 {
        SessionSpecV3 {
            run_id: run_id.to_owned(),
            session_id: "session-control-v2".to_owned(),
            account_id: "account-control-v2".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-control-v2".to_owned(),
            creation_request_id: creation_request_id.to_owned(),
            response_bridge_binding_id: "bridge-control-v2".to_owned(),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("10".to_owned()),
            },
            session_seed: sha('8'),
        }
    }

    fn market(
        condition_digit: char,
        clob_market_id: &str,
        first_token_id: &str,
        second_token_id: &str,
    ) -> crate::scenario::PlatformMarketV1 {
        crate::scenario::PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", condition_digit.to_string().repeat(64))),
                clob_market_id: ClobMarketId(clob_market_id.to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 2_000,
                    end_at_utc_ms: 3_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(first_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(second_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("构造追加市场测试正文")
    }

    fn market_with_window(
        condition_digit: char,
        clob_market_id: &str,
        first_token_id: &str,
        second_token_id: &str,
        start_at_utc_ms: i64,
        end_at_utc_ms: i64,
    ) -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", condition_digit.to_string().repeat(64))),
                clob_market_id: ClobMarketId(clob_market_id.to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms,
                    end_at_utc_ms,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(first_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(second_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("构造带窗口的逻辑驱动测试市场")
    }

    async fn active_market_actor(
        role: SimCallerRoleV1,
        run_id: &str,
    ) -> (tempfile::TempDir, SimRunActorV2) {
        active_market_actor_with_policy(role, run_id, successful_policy_v2_for_tests()).await
    }

    async fn active_market_actor_with_policy(
        role: SimCallerRoleV1,
        run_id: &str,
        finality_policy: Arc<TradeFinalityPolicyV2>,
    ) -> (tempfile::TempDir, SimRunActorV2) {
        let directory = tempfile::tempdir().expect("创建市场事件测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut run_opened = opened(
            run_id,
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        if role == SimCallerRoleV1::Paper {
            run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        }
        let mut actor = SimRunActorV2::create_new(
            &path,
            run_opened,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建市场事件测试运行根");
        let mut session_spec = spec(run_id, &format!("create-{run_id}"));
        session_spec.caller_role = role;
        actor
            .create_session(session_spec)
            .expect("创建市场事件测试会话");
        actor
            .activate_session(format!("activate-{run_id}"))
            .expect("激活市场事件测试会话");
        (directory, actor)
    }

    struct TrainingReplayTestProducer<'a> {
        actor: &'a mut SimRunActorV2,
    }

    impl TrainingReplayTestProducer<'_> {
        fn push(
            &mut self,
            path: MarketEventPathV1,
            at_utc_ms: i64,
            start_index_position: u64,
            next_index_position: u64,
            next_event_at_utc_ms: i64,
            events: Arc<[ExecutionMarketEventV3]>,
        ) {
            self.actor
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: DurableMarketBucketReferenceV2 {
                        path,
                        bucket_at_utc_ms: at_utc_ms,
                        start_index_position,
                        next_index_position,
                        closure: MillisecondBucketClosureV1::NextEventAt(next_event_at_utc_ms),
                    },
                    events,
                })
                .unwrap_or_else(|error| panic!("训练测试生产器驱动失败：{error}"));
        }

        fn advance_required(
            &mut self,
            hidden_next_index_position: u64,
            public_next_index_position: u64,
        ) {
            let required = self
                .actor
                .next_required_logical_drive()
                .expect("训练私有响应尚未完成时必须有必要推进");
            let next_index_position = match required.path {
                MarketEventPathV1::HiddenPlatform => hidden_next_index_position,
                MarketEventPathV1::PublicVisibility => public_next_index_position,
            };
            self.actor
                .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position,
                })
                .unwrap_or_else(|error| panic!("训练必要前缀推进失败：{error}"));
        }
    }

    struct PublicVisibilityTestBucket {
        at_utc_ms: i64,
        start_index_position: u64,
        next_index_position: u64,
        next_event_at_utc_ms: i64,
        events: Arc<[ExecutionMarketEventV3]>,
    }

    struct PaperVisibilityReplayTestProducer<'a> {
        actor: &'a mut SimRunActorV2,
    }

    impl PaperVisibilityReplayTestProducer<'_> {
        fn push(&mut self, bucket: PublicVisibilityTestBucket) {
            self.actor
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::PublicVisibility,
                        bucket_at_utc_ms: bucket.at_utc_ms,
                        start_index_position: bucket.start_index_position,
                        next_index_position: bucket.next_index_position,
                        closure: MillisecondBucketClosureV1::NextEventAt(
                            bucket.next_event_at_utc_ms,
                        ),
                    },
                    events: bucket.events,
                })
                .unwrap_or_else(|error| panic!("Paper 可见性测试生产器驱动失败：{error}"));
        }

        fn advance_required(&mut self, next_index_position: u64) {
            let required = self
                .actor
                .next_required_logical_drive()
                .expect("Paper 私有响应尚未完成时必须有必要推进");
            assert_eq!(required.path, MarketEventPathV1::PublicVisibility);
            self.actor
                .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position,
                })
                .unwrap_or_else(|error| panic!("Paper 必要可见性前缀推进失败：{error}"));
        }
    }

    fn completed_post_response<'a>(
        actor: &'a SimRunActorV2,
        request_id: &str,
    ) -> Option<&'a PostOrderDirectResponseV2> {
        let response = actor
            .state
            .as_ref()?
            .session
            .as_ref()?
            .mutation_requests
            .requests_by_id
            .get(request_id)?
            .completed_response
            .as_ref()?;
        match response.as_ref() {
            StoredDirectResponse::Final(BusinessDirectResponse::PostOrder(response)) => {
                Some(response)
            }
            _ => None,
        }
    }

    fn public_replay_witness(
        actor: &SimRunActorV2,
        condition_id: &str,
    ) -> (Decimal, LogicalPathFrontierV2) {
        let session = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("测试生产器唯一会话存在");
        (
            session
                .public_markets
                .external_quantity_for_tests(
                    condition_id,
                    "1",
                    BookSide::Bid,
                    "0.4".parse().expect("固定公开见证价格"),
                )
                .expect("固定公开见证档位存在"),
            session.public_visibility_frontier,
        )
    }

    async fn active_market_actor_with_scenario_and_policy(
        run_id: &str,
        scenario: Arc<SealedPlatformScenarioV1>,
        finality_policy: Arc<TradeFinalityPolicyV2>,
    ) -> (tempfile::TempDir, SimRunActorV2) {
        let directory = tempfile::tempdir().expect("创建同刻逻辑驱动测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let run_opened = opened(
            run_id,
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        let mut actor = SimRunActorV2::create_new(
            &path,
            run_opened,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建同刻逻辑驱动测试运行根");
        actor
            .create_session(spec(run_id, &format!("create-{run_id}")))
            .expect("创建同刻逻辑驱动测试会话");
        actor
            .activate_session(format!("activate-{run_id}"))
            .expect("激活同刻逻辑驱动测试会话");
        (directory, actor)
    }

    fn matched_not_broadcasted_policy_v2_for_tests() -> Arc<TradeFinalityPolicyV2> {
        Arc::new(
            TradeFinalityPolicyV2::decode_and_seal(
                br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED_NOT_BROADCASTED","offset_ms":0},{"state":"MATCHED","offset_ms":10000},{"state":"MINED","offset_ms":10010},{"state":"CONFIRMED","offset_ms":10020}],"economic_application_ordinal":2},"failure_overrides":[]}"#,
            )
            .expect("MNB 查询测试最终性策略必须合法"),
        )
    }

    fn decoded_initial_market_events(quantity: &str) -> Vec<ExecutionMarketEventV3> {
        [
            crate::task21_tests::initial_snapshot_event("1", 0, "Up", 1_100, 1_110, 1, quantity),
            crate::task21_tests::initial_snapshot_event("2", 1, "Down", 1_100, 1_110, 2, quantity),
        ]
        .into_iter()
        .map(|event| {
            let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
                .expect("编码真实初始市场事件");
            data_pipeline_core::decode_execution_market_event_v3(&bytes)
                .expect("从真实边界解码初始市场事件")
        })
        .collect()
    }

    fn decoded_maker_initial_events() -> Vec<ExecutionMarketEventV3> {
        let mut first =
            crate::task21_tests::initial_snapshot_event("1", 0, "Up", 1_100, 1_110, 1, "2");
        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut first.payload else {
            unreachable!("maker 测试首项必须是初始 L2")
        };
        snapshot.asks[0].price_pusd_per_share = ExactDecimal("0.2".to_owned());
        snapshot.asks[0].quantity_shares = ExactDecimal("1.000001".to_owned());
        first.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&first)
            .expect("重建 maker 初始事件身份");
        let second =
            crate::task21_tests::initial_snapshot_event("2", 1, "Down", 1_100, 1_110, 2, "2");
        [first, second]
            .into_iter()
            .map(|event| {
                let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
                    .expect("编码 maker 初始市场事件");
                data_pipeline_core::decode_execution_market_event_v3(&bytes)
                    .expect("从真实边界解码 maker 初始市场事件")
            })
            .collect()
    }

    fn decoded_initial_event_for_market(
        market: &PlatformMarketV1,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        let asset = &market.assets()[0];
        let mut event = crate::task21_tests::initial_snapshot_event(
            &asset.token_id.0,
            asset.outcome.index,
            &asset.outcome.label,
            source_at_utc_ms,
            received_at_utc_ms,
            sequence,
            "2",
        );
        event.market = market.market().clone();
        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut event.payload else {
            unreachable!("初始市场事件测试夹具必须是初始 L2")
        };
        snapshot.asset = asset.clone();
        event.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&event)
            .expect("重建追加市场事件身份");
        let bytes =
            data_pipeline_core::encode_execution_market_event_v3(&event).expect("编码追加市场事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码追加市场事件")
    }

    fn decoded_initial_events_for_market(
        market: &PlatformMarketV1,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        first_sequence: u64,
    ) -> Vec<ExecutionMarketEventV3> {
        decoded_initial_events_for_market_with_quantity(
            market,
            source_at_utc_ms,
            received_at_utc_ms,
            first_sequence,
            "2",
        )
    }

    fn decoded_initial_events_for_market_with_quantity(
        market: &PlatformMarketV1,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        first_sequence: u64,
        quantity: &str,
    ) -> Vec<ExecutionMarketEventV3> {
        market
            .assets()
            .iter()
            .enumerate()
            .map(|(index, asset)| {
                let mut event = crate::task21_tests::initial_snapshot_event(
                    &asset.token_id.0,
                    asset.outcome.index,
                    &asset.outcome.label,
                    source_at_utc_ms,
                    received_at_utc_ms,
                    first_sequence + u64::try_from(index).expect("测试资产序号可表示"),
                    quantity,
                );
                event.market = market.market().clone();
                let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut event.payload
                else {
                    unreachable!("初始市场事件测试夹具必须是初始 L2")
                };
                snapshot.asset = asset.clone();
                event.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&event)
                    .expect("重建双市场初始事件身份");
                let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
                    .expect("编码双市场初始事件");
                data_pipeline_core::decode_execution_market_event_v3(&bytes)
                    .expect("从真实边界解码双市场初始事件")
            })
            .collect()
    }

    fn decoded_resolution_event_for_market(
        market: &PlatformMarketV1,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        let winning_asset = &market.assets()[0];
        let mut event = crate::task21_tests::sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
                contracts::MarketResolutionTerminalV3::Resolved {
                    winning_asset_id: winning_asset.token_id.0.clone(),
                    winning_outcome: winning_asset.outcome.label.clone(),
                    payout_vector: market
                        .assets()
                        .iter()
                        .enumerate()
                        .map(|(index, asset)| contracts::PayoutEntryV1 {
                            asset: asset.clone(),
                            payout_pusd_per_share: ExactDecimal(if index == 0 {
                                "1".to_owned()
                            } else {
                                "0".to_owned()
                            }),
                        })
                        .collect(),
                    mapping_rule_version: "run-v2-eof-resolution-v1".to_owned(),
                },
            ),
            crate::task21_tests::orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            crate::task21_tests::fresh_quality(),
        );
        event.market = market.market().clone();
        event.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&event)
            .expect("重建双市场判定事件身份");
        let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
            .expect("编码双市场判定事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码双市场判定事件")
    }

    fn decoded_level_market_event(
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
        before: &str,
        after: &str,
    ) -> ExecutionMarketEventV3 {
        let event = crate::task21_tests::level_transition_event(
            source_at_utc_ms,
            received_at_utc_ms,
            sequence,
            before,
            after,
            "0",
            &(before
                .parse::<rust_decimal::Decimal>()
                .expect("测试 before")
                - after.parse::<rust_decimal::Decimal>().expect("测试 after"))
            .to_string(),
        );
        let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
            .expect("编码真实档位市场事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码档位市场事件")
    }

    fn decoded_maker_level_event(
        side: BookSide,
        price: &str,
        before: &str,
        after: &str,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        let before_decimal = before
            .parse::<rust_decimal::Decimal>()
            .expect("maker 测试 before");
        let after_decimal = after
            .parse::<rust_decimal::Decimal>()
            .expect("maker 测试 after");
        let (added, reduced) = if after_decimal >= before_decimal {
            (after_decimal - before_decimal, rust_decimal::Decimal::ZERO)
        } else {
            (rust_decimal::Decimal::ZERO, before_decimal - after_decimal)
        };
        let event = crate::task21_tests::sealed_event(
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
                causal_group_id: Sha256Hex(format!("{:064x}", sequence + 100)),
                market: crate::task21_tests::market(),
                asset: crate::task21_tests::asset("1", 0, "Up"),
                side,
                price_pusd_per_share: ExactDecimal(price.to_owned()),
                quantity_before_shares: ExactDecimal(before.to_owned()),
                quantity_after_shares: ExactDecimal(after.to_owned()),
                attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
                already_covered_quantity_shares: ExactDecimal("0".to_owned()),
                net_added_quantity_shares: ExactDecimal(added.to_string()),
                net_reduced_quantity_shares: ExactDecimal(reduced.to_string()),
                queue_consumption_quantity_shares: ExactDecimal(reduced.to_string()),
                basis: LevelExecutionBasis::LevelDeltaInference,
                platform_effective_at_utc_ms: source_at_utc_ms,
                visible_at_utc_ms: received_at_utc_ms,
                source_attribution_quality: SourceAttributionQuality::Unattributed,
                queue_result_quality: QueueResultQuality::DeterministicInferred,
            }),
            crate::task21_tests::orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            crate::task21_tests::fresh_quality(),
        );
        let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
            .expect("编码 maker 档位市场事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码 maker 档位市场事件")
    }

    fn decoded_maker_level_event_for_market(
        market: &PlatformMarketV1,
        price: &str,
        before: &str,
        after: &str,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        let mut event = decoded_maker_level_event(
            BookSide::Bid,
            price,
            before,
            after,
            source_at_utc_ms,
            received_at_utc_ms,
            sequence,
        );
        event.market = market.market().clone();
        let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) =
            &mut event.payload
        else {
            unreachable!("maker 档位测试夹具必须是档位执行转换")
        };
        transition.market = market.market().clone();
        transition.asset = market.assets()[0].clone();
        event.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&event)
            .expect("重建多市场 maker 档位事件身份");
        let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
            .expect("编码多市场 maker 档位事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码多市场 maker 档位事件")
    }

    fn decoded_resolution_market_event(
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        let scenario = crate::scenario::scenario_for_sim_tests();
        let member = &scenario.market_set().markets()[0];
        let winning_asset = &member.assets()[0];
        let mut event = crate::task21_tests::sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
                contracts::MarketResolutionTerminalV3::Resolved {
                    winning_asset_id: winning_asset.token_id.0.clone(),
                    winning_outcome: winning_asset.outcome.label.clone(),
                    payout_vector: member
                        .assets()
                        .iter()
                        .enumerate()
                        .map(|(index, asset)| contracts::PayoutEntryV1 {
                            asset: asset.clone(),
                            payout_pusd_per_share: ExactDecimal(if index == 0 {
                                "1".to_owned()
                            } else {
                                "0".to_owned()
                            }),
                        })
                        .collect(),
                    mapping_rule_version: "run-v2-resolution-v1".to_owned(),
                },
            ),
            crate::task21_tests::orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            crate::task21_tests::fresh_quality(),
        );
        event.market = member.market().clone();
        event.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&event)
            .expect("重建 V2 判定事件身份");
        let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
            .expect("编码 V2 判定市场事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码 V2 判定市场事件")
    }

    fn apply_buy_to_candidate(
        mut state: BusinessReducerStateV2,
        input: BuyOrderActionV2,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> (BusinessReducerStateV2, BuyOrderAcceptedV2) {
        let UniqueSessionStateV2 {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            mut query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        } = state.session.take().expect("BUY 候选必须拥有唯一会话");
        let (execution_state, disposition) = execution_state
            .apply_buy_order(input, rules, finality_policy)
            .expect("真实 V2 BUY 叶必须形成候选");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("maker 会话轨迹的 GTC 必须被接受")
        };

        query_state.advance_revision();
        state.session = Some(UniqueSessionStateV2 {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        });
        (state, accepted)
    }

    fn advance_rebate_clock_to_candidate(
        mut state: BusinessReducerStateV2,
        platform_effective_at_utc_ms: i64,
    ) -> (
        BusinessReducerStateV2,
        crate::matching::RebateClockAdvanceEffectsV2,
    ) {
        let UniqueSessionStateV2 {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        } = state.session.take().expect("返佣时钟候选必须拥有唯一会话");
        let (execution_state, effects) = execution_state
            .advance_next_rebate_clock_at(platform_effective_at_utc_ms)
            .expect("返佣时钟候选必须成功");

        state.session = Some(UniqueSessionStateV2 {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        });
        (state, effects)
    }

    fn advance_finality_to_candidate(
        mut state: BusinessReducerStateV2,
        platform_event_at_utc_ms: i64,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> (
        BusinessReducerStateV2,
        crate::matching::ScheduledFinalityAdvanceEffectsV2,
    ) {
        let UniqueSessionStateV2 {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            mut query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        } = state.session.take().expect("最终性候选必须拥有唯一会话");
        let (execution_state, effects) = execution_state
            .advance_next_scheduled_finality_at(platform_event_at_utc_ms, finality_policy, rules)
            .expect("最终性候选必须成功");

        if effects.transition.is_some() {
            query_state.advance_revision();
        }
        state.session = Some(UniqueSessionStateV2 {
            opening_fact,
            response_bridge_binding_id,
            session_seed,
            initialization,
            market_directory,
            public_markets,
            hidden_platform_markets,
            execution_state,
            market_enrollment,
            mutation_requests,
            query_state,
            next_private_feedback_sequence,
            next_projection_batch_sequence,
            hidden_platform_frontier,
            public_visibility_frontier,
        });
        (state, effects)
    }

    fn request_cutoff(at_utc_ms: i64) -> TrustedDecisionCutoffV2 {
        TrustedDecisionCutoffV2 {
            logical_time_utc_ms: at_utc_ms,
            public_visibility: PublicVisibilityCursorV1::default(),
            private_feedback_sequence: 0,
        }
    }

    fn encoded_gtd_expiration_for_lead_for_tests(
        state: &BusinessReducerStateV2,
        latency_profile: &EmpiricalDelayProfile,
        global_request_id: &str,
        decision_at_utc_ms: i64,
        lead_seconds: i64,
    ) -> i64 {
        let session = state.session.as_ref().expect("GTD 测试必须已有唯一会话");
        let outbound_delay_ms = latency_profile.sample_delay_ms(
            session.session_seed.0.as_bytes(),
            global_request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let processing_at_utc_ms = decision_at_utc_ms
            .checked_add(i64::try_from(outbound_delay_ms).expect("测试时延可表示"))
            .expect("GTD 测试处理时点可表示");
        let processing_second = processing_at_utc_ms.div_euclid(1_000)
            + i64::from(processing_at_utc_ms.rem_euclid(1_000) != 0);
        processing_second
            .checked_add(lead_seconds)
            .and_then(|second| second.checked_mul(1_000))
            .expect("GTD 测试到期时点可表示")
    }

    fn post_item(
        item_id: &str,
        condition_id: &ConditionId,
        token_id: &TokenId,
        kind: SimBuyOrderKindV1,
    ) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: crate::execution::SimBuyOrderV1 {
                condition_id: condition_id.clone(),
                token_id: token_id.clone(),
                kind,
            },
        }
    }

    fn register_request_candidate(
        opened: &RunOpenedV2,
        initial_market_directory: &SessionMarketDirectoryV1,
        state: BusinessReducerStateV2,
        cutoff: &TrustedDecisionCutoffV2,
        request: &BusinessRequestV2,
        input_sequence: u64,
        latency_profile: &EmpiricalDelayProfile,
        rules: &PlatformRuleSnapshotV3,
    ) -> (BusinessReducerStateV2, MutationRequestReceiptV2) {
        match reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::RegisterNewRequest {
                cutoff,
                request,
                input_sequence,
                latency_profile,
                rules,
            },
        ) {
            Ok((state, BusinessReducerOutputV2::RequestRegistration(receipt))) => (state, receipt),
            Ok(_) => panic!("请求登记归约必须返回请求回执"),
            Err(BusinessReductionErrorV2::Unaccepted { error, .. })
            | Err(BusinessReductionErrorV2::Fatal(error)) => {
                panic!("请求登记候选失败：{error}")
            }
        }
    }

    fn advance_mutation_candidate(
        opened: &RunOpenedV2,
        initial_market_directory: &SessionMarketDirectoryV1,
        state: BusinessReducerStateV2,
        platform_effective_at_utc_ms: i64,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> (
        BusinessReducerStateV2,
        Option<MutationActionBusinessEffectV3>,
    ) {
        let (state, effect) = advance_mutation_candidate_with_outputs(
            opened,
            initial_market_directory,
            state,
            platform_effective_at_utc_ms,
            rules,
            finality_policy,
        );
        (state, effect.map(|effect| effect.business))
    }

    fn advance_mutation_candidate_with_outputs(
        opened: &RunOpenedV2,
        initial_market_directory: &SessionMarketDirectoryV1,
        state: BusinessReducerStateV2,
        platform_effective_at_utc_ms: i64,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> (
        BusinessReducerStateV2,
        Option<MutationActionTransitionEffectsV2>,
    ) {
        match reduce_business_input_v2(
            opened,
            initial_market_directory,
            state,
            BusinessReducerInputV2::AdvanceNextMutationActionAt {
                platform_effective_at_utc_ms,
                rules,
                finality_policy,
            },
        ) {
            Ok((state, BusinessReducerOutputV2::MutationAction(effect))) => (state, effect),
            Ok(_) => panic!("逻辑动作归约必须返回动作效果"),
            Err(BusinessReductionErrorV2::Unaccepted { error, .. })
            | Err(BusinessReductionErrorV2::Fatal(error)) => {
                panic!("逻辑动作候选失败：{error}")
            }
        }
    }

    async fn assert_necessary_prefix_reduces_once_without_body_and_sparse_gap_emits_nothing() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-prefix-driver-v2").await;
        let initial = decoded_initial_market_events("10");
        let condition_id = initial[0].market.condition_id.clone();
        let state = actor.state.take().expect("必要前缀测试初始状态");
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 1_100,
                        start_index_position: 0,
                        next_index_position: 2,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(2_000),
                    },
                    events: Arc::from(initial),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("必要前缀测试初始正文桶必须成功");
        let (state, accepted) = apply_buy_to_candidate(
            state,
            BuyOrderActionV2 {
                global_request_id: "prefix-gtd-request".to_owned(),
                batch_item_ordinal: 0,
                item_id: "prefix-gtd-item".to_owned(),
                condition_id,
                token_id: TokenId("1".to_owned()),
                effective_processing_at_utc_ms: 1_200,
                terms: BuyOrderTermsV2::Gtd {
                    limit_price_pusd_per_share: "0.3".parse().expect("GTD 测试价格"),
                    accepted_base_units: 1_000_000,
                    accepted_quote_units: 300_000,
                    post_only: false,
                    expires_at_utc_ms: Some(2_000),
                },
            },
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(accepted.platform_order_id, "sim-1");

        let frontier_before_same_time_prefix = state
            .session
            .as_ref()
            .expect("同刻前缀反例前会话存在")
            .hidden_platform_frontier;
        let error = match drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PlatformPrefixAdvance {
                advance: PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    closed_through_utc_ms: 2_000,
                    next_index_position: 2,
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        ) {
            Ok(_) => panic!("已知 NextEventAt(t) 时同刻空前缀必须拒绝"),
            Err(error) => error,
        };
        let BusinessReductionErrorV2::Unaccepted { state, .. } = error else {
            panic!("同刻空前缀必须是零安装的未受理输入")
        };
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("同刻前缀拒绝后会话存在")
                .hidden_platform_frontier,
            frontier_before_same_time_prefix
        );

        reset_reducer_call_count_for_tests();
        let (state, output) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 2_000,
                        start_index_position: 2,
                        next_index_position: 3,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(3_000),
                    },
                    events: Arc::from(vec![decoded_level_market_event(
                        2_000, 2_010, 3, "2", "1.5",
                    )]),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("拒绝同刻空前缀后已知 t 正文必须仍可受理");
        assert_eq!(reducer_call_count_for_tests(), 1);
        let BusinessReducerOutputV2::LogicalDrive(output) = output else {
            panic!("必要前缀必须返回逻辑驱动效果")
        };
        assert!(matches!(
            output.lifecycle.as_slice(),
            [
                LifecycleFeedbackEffectV2::Order(_),
                LifecycleFeedbackEffectV2::Settlement(_)
            ]
        ));
        assert!(output.mutations.is_empty());

        let frontier_before = state
            .session
            .as_ref()
            .expect("必要前缀后会话存在")
            .hidden_platform_frontier;
        let error = match drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PlatformPrefixAdvance {
                advance: PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    closed_through_utc_ms: 2_500,
                    next_index_position: 9,
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        ) {
            Ok(_) => panic!("普通稀疏缺口不得制造无正文业务输入"),
            Err(error) => error,
        };
        let BusinessReductionErrorV2::Unaccepted { state, .. } = error else {
            panic!("普通稀疏缺口必须是零生效的未受理输入")
        };
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("稀疏缺口拒绝后会话仍存在")
                .hidden_platform_frontier,
            frontier_before
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn necessary_prefix_reduces_once_without_body_and_sparse_gap_emits_nothing() {
        assert_necessary_prefix_reduces_once_without_body_and_sparse_gap_emits_nothing().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn logical_driver_keeps_non_execution_public_visibility_separate() {
        let run_id = "run-public-visibility-isolation-v2";
        let request_id = "public-visibility-gtd";
        let mutation_request_id = "public-visibility-mutation";
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let session_seed = sha('8');
        let outbound_delay = |request_id: &str| {
            i64::try_from(latency_profile.sample_delay_ms(
                session_seed.0.as_bytes(),
                request_id,
                DelaySampleDomainV1::RequestOutbound,
            ))
            .expect("路径隔离测试出站时延可表示")
        };
        let decision_at = 10_000;
        let processing_at = decision_at + outbound_delay(request_id);
        let target_at = (processing_at.div_euclid(1_000)
            + i64::from(processing_at.rem_euclid(1_000) != 0)
            + 240)
            * 1_000;

        let replay_market = market_with_window('d', "123", "1", "2", 0, target_at + 300_000);
        let scenario = sealed_scenario_for_markets(vec![replay_market]);
        let (_directory, mut actor) = active_market_actor_with_scenario_and_policy(
            run_id,
            scenario,
            successful_policy_v2_for_tests(),
        )
        .await;
        let market = actor
            .scenario
            .initial_market_directory()
            .markets()
            .next()
            .expect("路径隔离测试初始市场存在")
            .clone();
        let initial_events =
            decoded_initial_events_for_market_with_quantity(&market, 1_100, target_at, 1, "2");
        let state = actor.state.take().expect("路径隔离测试初始状态");
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 1_100,
                        start_index_position: 0,
                        next_index_position: 2,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(target_at),
                    },
                    events: Arc::from(initial_events.clone()),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("路径隔离测试隐藏初始正文必须成功");
        let request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "public-visibility-gtd-item",
                &market.market().condition_id,
                &market.assets()[0].token_id,
                SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: "0.2".parse().expect("路径隔离 GTD 价格"),
                    size_shares: "5".parse().expect("路径隔离 GTD 数量"),
                    post_only: false,
                    expires_at_utc_ms: target_at + 999,
                },
            ),
        });
        let (state, receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(decision_at),
            &request,
            1,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (state, mutation) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let Some(MutationActionBusinessEffectV3::Post(effect)) = mutation else {
            panic!("路径隔离 GTD 必须形成提交效果")
        };
        assert!(
            matches!(effect.outcome, PostOrderItemOutcomeV2::Accepted(_)),
            "路径隔离 GTD 必须被接受，实际结果：{:?}",
            effect.outcome
        );
        let mutation_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: mutation_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "public-visibility-mutation-item",
                &market.market().condition_id,
                &market.assets()[0].token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.1".parse().expect("路径隔离 mutation 价格"),
                    size_shares: "5".parse().expect("路径隔离 mutation 数量"),
                    post_only: false,
                },
            ),
        });
        let mutation_decision_at = target_at - outbound_delay(mutation_request_id);
        let (state, mutation_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(mutation_decision_at),
            &mutation_request,
            2,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        assert_eq!(mutation_receipt.platform_arrived_at_utc_ms, target_at);
        let session = state.session.as_ref().expect("路径隔离 GTD 后会话存在");
        assert!(
            session
                .execution_state
                .has_gtd_deadline_for_tests(target_at, "sim-1")
        );
        let account_sequence_before = session.execution_state.next_account_transition_seq();
        let query_revision_before = session.query_state.current_revision;
        let pending_actions_before = session.mutation_requests.pending_actions.len();

        let (state, public_output) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::PublicVisibility,
                        bucket_at_utc_ms: target_at,
                        start_index_position: 0,
                        next_index_position: 2,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(
                            target_at + 1_000,
                        ),
                    },
                    events: Arc::from(initial_events),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("训练公开可见正文必须由完整 driver 成功受理");
        let BusinessReducerOutputV2::LogicalDrive(public_output) = public_output else {
            panic!("公开可见正文必须返回逻辑驱动效果")
        };
        assert!(public_output.lifecycle.is_empty());
        assert!(public_output.mutations.is_empty());
        let session = state.session.as_ref().expect("公开可见正文后会话存在");
        let price = "0.4".parse().expect("路径隔离公开盘口价格");
        assert_eq!(
            session.public_markets.external_quantity_for_tests(
                &market.market().condition_id.0,
                &market.assets()[0].token_id.0,
                BookSide::Bid,
                price,
            ),
            Some("2".parse().expect("路径隔离公开盘口数量"))
        );
        assert_eq!(
            session.public_visibility_frontier.closed_through_utc_ms,
            Some(target_at)
        );
        assert!(
            session
                .execution_state
                .has_gtd_deadline_for_tests(target_at, "sim-1")
        );
        assert_eq!(
            session.execution_state.next_account_transition_seq(),
            account_sequence_before
        );
        assert_eq!(session.query_state.current_revision, query_revision_before);
        assert_eq!(
            session.mutation_requests.pending_actions.len(),
            pending_actions_before
        );

        let (state, hidden_output) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: target_at,
                        start_index_position: 2,
                        next_index_position: 3,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(
                            target_at + 1_000,
                        ),
                    },
                    events: Arc::from(vec![decoded_maker_level_event_for_market(
                        &market,
                        "0.4",
                        "2",
                        "1.5",
                        target_at,
                        target_at + 10,
                        3,
                    )]),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("同一训练角色隐藏正文仍必须驱动执行");
        let BusinessReducerOutputV2::LogicalDrive(hidden_output) = hidden_output else {
            panic!("隐藏正文必须返回逻辑驱动效果")
        };
        assert!(hidden_output.lifecycle.iter().any(|effect| matches!(
            effect,
            LifecycleFeedbackEffectV2::Order(order)
                if matches!(order.event, contracts::OrderLifecycleEventV2::GtdExpired { .. })
        )));
        assert_eq!(hidden_output.mutations.len(), 1);
        assert!(
            !state
                .session
                .as_ref()
                .expect("隐藏正文后会话存在")
                .execution_state
                .has_gtd_deadline_for_tests(target_at, "sim-1")
        );

        let (_paper_directory, mut paper) =
            active_market_actor(SimCallerRoleV1::Paper, "run-paper-public-driver-v2").await;
        let paper_events = decoded_initial_market_events("2");
        let paper_condition_id = paper_events[0].market.condition_id.0.clone();
        let paper_state = paper.state.take().expect("Paper 公开正文前状态");
        let (paper_state, _) = drive_business_input_at_v2(
            paper_state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::PublicVisibility,
                        bucket_at_utc_ms: 1_110,
                        start_index_position: 0,
                        next_index_position: 2,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(2_000),
                    },
                    events: Arc::from(paper_events),
                },
                rules: paper._rule_snapshot.snapshot(),
                finality_policy: paper._finality_policy.as_ref(),
            },
        )
        .expect("Paper 公开正文仍必须由完整 driver 驱动执行");
        assert_eq!(
            paper_state
                .session
                .as_ref()
                .expect("Paper 公开正文后会话存在")
                .execution_state
                .external_queue_base_units_for_tests(
                    &paper_condition_id,
                    "1",
                    BookSide::Bid,
                    price,
                ),
            2_000_000
        );
    }

    async fn prepared_dual_market_eof_candidate(
        run_id: &str,
    ) -> (SimRunActorV2, BusinessReducerStateV2, Vec<PlatformMarketV1>) {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, run_id).await;
        let initial_market = actor
            .scenario
            .initial_market_directory()
            .markets()
            .next()
            .expect("双市场 EOF 测试初始市场存在")
            .clone();
        let added_market = market('e', "456", "301", "302");
        actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: format!("enroll-{run_id}"),
                market: added_market.clone(),
            })
            .expect("双市场 EOF 测试登记第二市场");
        let mut initial_events =
            decoded_initial_events_for_market(&initial_market, 1_100, 1_110, 1);
        initial_events.extend(decoded_initial_events_for_market(
            &added_market,
            1_100,
            1_110,
            3,
        ));
        let state = actor.state.take().expect("双市场 EOF 初始闭桶前状态");
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 1_100,
                        start_index_position: 0,
                        next_index_position: 4,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(4_000),
                    },
                    events: Arc::from(initial_events),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("双市场初始正文桶必须整体成功");
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PlatformPrefixAdvance {
                advance: PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    closed_through_utc_ms: 2_000,
                    next_index_position: 4,
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("初始市场 end_at 必要前缀必须成功");
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PlatformPrefixAdvance {
                advance: PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    closed_through_utc_ms: 3_000,
                    next_index_position: 4,
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("追加市场 end_at 必要前缀必须成功");
        (actor, state, vec![initial_market, added_market])
    }

    async fn assert_multi_market_eof_is_whole_bucket_zero_or_full_effect() {
        let (failed_actor, failed_state, failed_markets) =
            prepared_dual_market_eof_candidate("run-eof-failed-v2").await;
        let failed_events = Arc::from(vec![decoded_resolution_event_for_market(
            &failed_markets[0],
            4_000,
            4_100,
            5,
        )]);
        let failed = drive_business_input_at_v2(
            failed_state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 4_000,
                        start_index_position: 4,
                        next_index_position: 5,
                        closure: crate::market::MillisecondBucketClosureV1::EndOfInput,
                    },
                    events: Arc::clone(&failed_events),
                },
                rules: failed_actor._rule_snapshot.snapshot(),
                finality_policy: failed_actor._finality_policy.as_ref(),
            },
        );
        assert!(matches!(
            failed,
            Err(BusinessReductionErrorV2::Fatal(
                SimRunActorErrorV2::MarketEvent(
                    MarketEventErrorV1::MissingQualifiedMarketTerminal(_)
                )
            ))
        ));
        assert_eq!(Arc::strong_count(&failed_events), 1);

        let (success_actor, success_state, success_markets) =
            prepared_dual_market_eof_candidate("run-eof-success-v2").await;
        let events = Arc::from(vec![
            decoded_resolution_event_for_market(&success_markets[0], 4_000, 4_100, 5),
            decoded_resolution_event_for_market(&success_markets[1], 4_000, 4_100, 6),
        ]);
        let (state, output) = drive_business_input_at_v2(
            success_state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 4_000,
                        start_index_position: 4,
                        next_index_position: 6,
                        closure: crate::market::MillisecondBucketClosureV1::EndOfInput,
                    },
                    events,
                },
                rules: success_actor._rule_snapshot.snapshot(),
                finality_policy: success_actor._finality_policy.as_ref(),
            },
        )
        .expect("完整双市场 EOF 必须整体成功");
        let BusinessReducerOutputV2::LogicalDrive(output) = output else {
            panic!("双市场 EOF 必须返回逻辑驱动效果")
        };
        assert_eq!(output.lifecycle.len(), 4);
        let session = state.session.as_ref().expect("双市场 EOF 后会话存在");
        assert!(session.hidden_platform_frontier.sealed);
        for market in success_markets {
            assert_eq!(
                session
                    .hidden_platform_markets
                    .as_ref()
                    .expect("训练会话拥有隐藏市场状态")
                    .lifecycle_phase_for_tests(&market.market().condition_id.0),
                Some(crate::market::MarketLifecyclePhaseV1::ResolutionTerminal)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_market_eof_is_whole_bucket_zero_or_full_effect() {
        assert_multi_market_eof_is_whole_bucket_zero_or_full_effect().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn successful_execution_eof_drains_only_already_scheduled_obligations() {
        let finality_policy = Arc::new(
            TradeFinalityPolicyV2::decode_and_seal(
                br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":4000},{"state":"CONFIRMED","offset_ms":4010}],"economic_application_ordinal":1},"failure_overrides":[]}"#,
            )
            .expect("EOF 后排空测试最终性策略必须合法"),
        );
        let (_directory, mut actor) = active_market_actor_with_policy(
            SimCallerRoleV1::ModelTrain,
            "run-eof-drains-scheduled-v2",
            finality_policy,
        )
        .await;
        let initial = decoded_maker_initial_events();
        let condition_id = initial[0].market.condition_id.clone();
        let state = actor.state.take().expect("EOF 后排空测试初始状态");
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 1_100,
                        start_index_position: 0,
                        next_index_position: 2,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(3_000),
                    },
                    events: Arc::from(initial),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("EOF 后排空测试初始正文必须成功");
        let (state, accepted) = apply_buy_to_candidate(
            state,
            BuyOrderActionV2 {
                global_request_id: "eof-scheduled-fill-request".to_owned(),
                batch_item_ordinal: 0,
                item_id: "eof-scheduled-fill-item".to_owned(),
                condition_id,
                token_id: TokenId("1".to_owned()),
                effective_processing_at_utc_ms: 1_200,
                terms: BuyOrderTermsV2::Gtc {
                    limit_price_pusd_per_share: "0.3".parse().expect("EOF 后排空测试价格"),
                    accepted_base_units: 4_000_000,
                    accepted_quote_units: 1_200_000,
                    post_only: false,
                },
            },
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(accepted.initial_finality_count_for_tests(), 1);
        let (state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::PlatformPrefixAdvance {
                advance: PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    closed_through_utc_ms: 2_000,
                    next_index_position: 2,
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("EOF 前既有 maker 时钟必须由必要空前缀闭合");
        let events = Arc::from(vec![decoded_resolution_market_event(3_000, 3_100, 3)]);
        let (state, output) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 3_000,
                        start_index_position: 2,
                        next_index_position: 3,
                        closure: crate::market::MillisecondBucketClosureV1::EndOfInput,
                    },
                    events,
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("成功 EOF 必须排空此前已经排定的有限业务义务");
        let BusinessReducerOutputV2::LogicalDrive(output) = output else {
            panic!("EOF 后排空必须返回唯一逻辑驱动效果")
        };
        let session = state.session.as_ref().expect("EOF 后排空会话存在");
        assert!(session.hidden_platform_frontier.sealed);
        assert_eq!(
            session
                .execution_state
                .next_gtd_deadline_at_utc_ms(actor._rule_snapshot.snapshot()),
            None
        );
        assert_eq!(
            session.execution_state.next_scheduled_finality_at_utc_ms(),
            None
        );
        assert_eq!(session.execution_state.next_rebate_clock_at_utc_ms(), None);
        assert!(output.lifecycle.iter().any(|effect| matches!(
            effect,
            LifecycleFeedbackEffectV2::Trade(trade)
                if trade.platform_event_at_utc_ms > 3_000
                    && trade.status.known
                        == Some(contracts::SimulatedTradeKnownStatusV2::Confirmed)
        )));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn logical_driver_drains_five_same_time_classes_in_fixed_order() {
        let run_id = "run-five-way-collision-v2";
        let gtd_request_id = "five-way-gtd";
        let fok_request_id = "five-way-prior-fok";
        let maker_request_id = "five-way-dpt-maker";
        let mutation_request_id = "five-way-mutation";
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let session_seed = sha('8');
        let outbound_delay = |request_id: &str| {
            i64::try_from(latency_profile.sample_delay_ms(
                session_seed.0.as_bytes(),
                request_id,
                DelaySampleDomainV1::RequestOutbound,
            ))
            .expect("同刻测试出站时延可表示")
        };
        let gtd_decision_at = 10_000;
        let gtd_processing_at = gtd_decision_at + outbound_delay(gtd_request_id);
        let target_at = (gtd_processing_at.div_euclid(1_000)
            + i64::from(gtd_processing_at.rem_euclid(1_000) != 0)
            + 240)
            * 1_000;
        let fok_effective_at = 20_000;
        let finality_offset = target_at - fok_effective_at;
        let finality_policy = Arc::new(
            TradeFinalityPolicyV2::decode_and_seal(
                format!(
                    "{{\"schema_version\":\"trade_finality_policy_v2\",\"normal_path\":{{\"steps\":[{{\"state\":\"MATCHED\",\"offset_ms\":0}},{{\"state\":\"MINED\",\"offset_ms\":{}}},{{\"state\":\"CONFIRMED\",\"offset_ms\":{finality_offset}}}],\"economic_application_ordinal\":1}},\"failure_overrides\":[]}}",
                    finality_offset - 1,
                )
                .as_bytes(),
            )
            .expect("同刻测试最终性策略必须合法"),
        );
        let previous_market = market_with_window('d', "455", "201", "202", 0, target_at);
        let collision_market = market_with_window('e', "456", "301", "302", 0, target_at + 300_000);
        let scenario =
            sealed_scenario_for_markets(vec![previous_market.clone(), collision_market.clone()]);
        let (_directory, mut actor) = active_market_actor_with_scenario_and_policy(
            run_id,
            scenario,
            Arc::clone(&finality_policy),
        )
        .await;
        let mut initial_events = decoded_initial_events_for_market_with_quantity(
            &previous_market,
            1_100,
            1_110,
            1,
            "10",
        );
        initial_events.extend(decoded_initial_events_for_market_with_quantity(
            &collision_market,
            1_100,
            1_110,
            3,
            "10",
        ));
        let state = actor.state.take().expect("同刻碰撞初始状态");
        let (mut state, _) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 1_100,
                        start_index_position: 0,
                        next_index_position: 4,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(target_at),
                    },
                    events: Arc::from(initial_events),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("同刻碰撞初始正文桶必须成功");

        let register_post_at = |state,
                                request_id: &str,
                                item_id: &str,
                                kind: SimBuyOrderKindV1,
                                input_sequence: u64,
                                desired_arrival_at: i64| {
            let request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                context: RequestContextV2 {
                    global_request_id: request_id.to_owned(),
                    caller_correlation_id: None,
                },
                item: post_item(
                    item_id,
                    &collision_market.market().condition_id,
                    &collision_market.assets()[0].token_id,
                    kind,
                ),
            });
            let decision_at = desired_arrival_at - outbound_delay(request_id);
            let (state, receipt) = register_request_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                &request_cutoff(decision_at),
                &request,
                input_sequence,
                actor._latency_profile.as_ref(),
                actor._rule_snapshot.snapshot(),
            );
            assert_eq!(receipt.platform_arrived_at_utc_ms, desired_arrival_at);
            (state, receipt)
        };

        let taker_delay = i64::try_from(
            actor
                ._rule_snapshot
                .snapshot()
                .documented_platform_rules
                .taker_delay
                .value
                .delay_ms,
        )
        .expect("同刻测试 taker 延迟可表示");
        let (next, fok_receipt) = register_post_at(
            state,
            fok_request_id,
            "five-way-prior-fok-item",
            SimBuyOrderKindV1::FokMarket {
                spend_pusd: "3".parse().expect("同刻 FOK 预算"),
                worst_price_pusd_per_share: "0.6".parse().expect("同刻 FOK 最差价"),
            },
            10,
            fok_effective_at - taker_delay,
        );
        let (next, fok_transition) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            fok_effective_at,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert_eq!(fok_receipt.scheduled_action_count, 1);
        let Some(fok_transition) = fok_transition else {
            panic!("先前 FOK 必须形成真实 mutation 效果")
        };
        let MutationActionBusinessEffectV3::Post(fok_effect) = fok_transition.business else {
            panic!("先前 FOK mutation 必须是提交效果")
        };
        assert!(
            matches!(fok_effect.outcome, PostOrderItemOutcomeV2::Accepted(_)),
            "先前 FOK 必须成交，实际结果：{:?}",
            fok_effect.outcome
        );
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("先前 FOK 后会话存在")
                .execution_state
                .next_scheduled_finality_at_utc_ms(),
            Some(target_at - 1)
        );
        let (next, mined) = advance_finality_to_candidate(
            state,
            target_at - 1,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert!(matches!(
            mined.transition.as_ref(),
            Some(transition)
                if transition.trade_finality.finality_state
                    == crate::finality::TradeFinalityStateV1::Mined
        ));
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("MINED 后会话存在")
                .execution_state
                .next_scheduled_finality_at_utc_ms(),
            Some(target_at)
        );

        let (next, maker_receipt) = register_post_at(
            state,
            maker_request_id,
            "five-way-dpt-maker-item",
            SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: "0.3".parse().expect("同刻 maker 价格"),
                size_shares: "5".parse().expect("同刻 maker 数量"),
                post_only: false,
            },
            11,
            30_000,
        );
        let (next, maker_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            maker_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert!(matches!(
            maker_effect,
            Some(MutationActionBusinessEffectV3::Post(effect))
                if matches!(effect.outcome, PostOrderItemOutcomeV2::Accepted(_))
        ));

        let (next, gtd_receipt) = register_post_at(
            state,
            gtd_request_id,
            "five-way-gtd-item",
            SimBuyOrderKindV1::GtdLimit {
                limit_price_pusd_per_share: "0.2".parse().expect("同刻 GTD 价格"),
                size_shares: "5".parse().expect("同刻 GTD 数量"),
                post_only: false,
                expires_at_utc_ms: target_at + 999,
            },
            12,
            gtd_processing_at,
        );
        let (next, gtd_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            gtd_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert!(matches!(
            gtd_effect,
            Some(MutationActionBusinessEffectV3::Post(effect))
                if matches!(effect.outcome, PostOrderItemOutcomeV2::Accepted(_))
        ));
        assert!(
            state
                .session
                .as_ref()
                .expect("GTD 准备后会话存在")
                .execution_state
                .has_gtd_deadline_for_tests(target_at, "sim-3")
        );

        let (state, mutation_receipt) = register_post_at(
            state,
            mutation_request_id,
            "five-way-mutation-item",
            SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: "0.1".parse().expect("同刻 mutation 价格"),
                size_shares: "5".parse().expect("同刻 mutation 数量"),
                post_only: false,
            },
            13,
            target_at,
        );
        assert_eq!(mutation_receipt.platform_arrived_at_utc_ms, target_at);

        let dpt_events: Arc<[ExecutionMarketEventV3]> = Arc::from(vec![
            decoded_maker_level_event_for_market(
                &collision_market,
                "0.3",
                "0",
                "5",
                target_at,
                target_at + 10,
                5,
            ),
            decoded_maker_level_event_for_market(
                &collision_market,
                "0.3",
                "5",
                "0",
                target_at,
                target_at + 11,
                6,
            ),
        ]);
        let (state, output) = drive_business_input_at_v2(
            state,
            BusinessReducerInputV2::ClosedMarketBucket {
                bucket: AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: target_at,
                        start_index_position: 4,
                        next_index_position: 6,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(
                            target_at + 1_000,
                        ),
                    },
                    events: Arc::clone(&dpt_events),
                },
                rules: actor._rule_snapshot.snapshot(),
                finality_policy: actor._finality_policy.as_ref(),
            },
        )
        .expect("五类同刻正文必须由一个逻辑循环排空");
        assert_eq!(Arc::strong_count(&dpt_events), 1);
        let BusinessReducerOutputV2::LogicalDrive(output) = output else {
            panic!("五类同刻正文必须返回逻辑驱动效果")
        };
        let [
            LifecycleFeedbackEffectV2::Order(gtd),
            LifecycleFeedbackEffectV2::Trade(finality),
            LifecycleFeedbackEffectV2::Settlement(taker_accrual),
            LifecycleFeedbackEffectV2::Settlement(maker_market),
            LifecycleFeedbackEffectV2::Trade(dpt),
        ] = output.lifecycle.as_slice()
        else {
            panic!("五类同刻必须按 GTD、最终性及因果返佣、成熟返佣、DPT 排空")
        };
        let contracts::OrderLifecycleEventV2::GtdExpired {
            account_delta: Some(gtd_delta),
            ..
        } = &gtd.event
        else {
            panic!("首个同刻效果必须是带释放账户变化的 GTD 到期")
        };
        assert_eq!(gtd.platform_event_at_utc_ms, target_at);
        assert_eq!(finality.platform_event_at_utc_ms, target_at);
        assert_eq!(
            finality.status.known,
            Some(contracts::SimulatedTradeKnownStatusV2::Confirmed)
        );
        assert!(finality.account_delta.is_none());
        let contracts::SettlementLifecycleEventV2::TakerRebateAccrualApplied {
            account_delta: taker_delta,
            ..
        } = &taker_accrual.event
        else {
            panic!("最终性必须先原位排空 taker 返佣应计")
        };
        assert!(matches!(
            &maker_market.event,
            contracts::SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                condition_id,
                status: contracts::MakerRebateAccrualStatusV2::ZeroNoEligibleMakerFill,
                ..
            } if condition_id == &previous_market.market().condition_id
        ));
        assert_eq!(dpt.platform_event_at_utc_ms, target_at);
        assert_eq!(
            dpt.liquidity_role,
            contracts::TradeLiquidityRoleV2::MakerInferred
        );
        assert!(
            dpt.account_delta.is_none(),
            "DPT maker 成交等待后续最终性应用经济变化"
        );
        assert_eq!(
            [
                gtd_delta.account_transition_seq,
                taker_delta.account_transition_seq,
            ],
            [
                gtd_delta.account_transition_seq,
                gtd_delta.account_transition_seq + 1,
            ],
            "实际账户序号必须证明 GTD 先于最终性解锁的因果返佣"
        );
        let [mutation] = output.mutations.as_slice() else {
            panic!("DPT 后必须只处理同刻登记的唯一订单 mutation")
        };
        let MutationActionBusinessEffectV3::Post(mutation_effect) = &mutation.business else {
            panic!("同刻最后一个稳定身份必须是订单 mutation")
        };
        assert!(matches!(
            mutation_effect.outcome,
            PostOrderItemOutcomeV2::Accepted(_)
        ));
        let [LifecycleFeedbackEffectV2::Order(mutation_order)] = mutation.lifecycle.as_slice()
        else {
            panic!("同刻订单 mutation 必须形成唯一订单接受效果")
        };
        let contracts::OrderLifecycleEventV2::OrderAccepted {
            account_delta: mutation_delta,
            ..
        } = &mutation_order.event
        else {
            panic!("同刻 mutation 必须是订单接受")
        };
        assert_eq!(mutation_order.platform_event_at_utc_ms, target_at);
        assert_eq!(
            mutation_delta.account_transition_seq,
            taker_delta.account_transition_seq + 1,
            "mutation 的账户序号必须证明它晚于同刻因果返佣和 DPT 稳定身份"
        );
        let session = state.session.as_ref().expect("五类同刻后会话存在");
        assert_eq!(
            session.execution_state.next_account_transition_seq(),
            mutation_delta.account_transition_seq + 1
        );
        assert!(
            session
                .mutation_requests
                .requests_by_id
                .get(mutation_request_id)
                .and_then(|request| request.completed_response.as_ref())
                .is_some()
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn register_and_advance_query_candidate(
        opened: &RunOpenedV2,
        initial_market_directory: &SessionMarketDirectoryV1,
        state: BusinessReducerStateV2,
        request: &BusinessRequestV2,
        input_sequence: u64,
        decision_at_utc_ms: i64,
        latency_profile: &EmpiricalDelayProfile,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> (
        BusinessReducerStateV2,
        MutationRequestReceiptV2,
        Arc<QueryActionBusinessEffectV2>,
    ) {
        let cutoff = request_cutoff(decision_at_utc_ms);
        let (state, receipt) = register_request_candidate(
            opened,
            initial_market_directory,
            state,
            &cutoff,
            request,
            input_sequence,
            latency_profile,
            rules,
        );
        let (state, effect) = advance_mutation_candidate(
            opened,
            initial_market_directory,
            state,
            receipt.platform_arrived_at_utc_ms,
            rules,
            finality_policy,
        );
        let Some(MutationActionBusinessEffectV3::Query(effect)) = effect else {
            panic!("查询请求必须形成唯一查询业务效果")
        };
        (state, receipt, effect)
    }

    #[allow(clippy::too_many_arguments)]
    fn register_and_drain_post_candidate(
        opened: &RunOpenedV2,
        initial_market_directory: &SessionMarketDirectoryV1,
        state: BusinessReducerStateV2,
        request: &BusinessRequestV2,
        input_sequence: u64,
        decision_at_utc_ms: i64,
        latency_profile: &EmpiricalDelayProfile,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> (
        BusinessReducerStateV2,
        MutationRequestReceiptV2,
        Vec<Arc<PostOrderItemBusinessEffectV2>>,
        Vec<LifecycleFeedbackEffectV2>,
    ) {
        let cutoff = request_cutoff(decision_at_utc_ms);
        let (mut state, receipt) = register_request_candidate(
            opened,
            initial_market_directory,
            state,
            &cutoff,
            request,
            input_sequence,
            latency_profile,
            rules,
        );
        let mut effects = Vec::new();
        let mut lifecycle = Vec::new();
        for _ in 0..receipt.scheduled_action_count {
            let next_at = state
                .session
                .as_ref()
                .and_then(|session| {
                    session
                        .mutation_requests
                        .pending_actions
                        .first_key_value()
                        .map(|(key, _)| key.effective_processing_at_utc_ms)
                })
                .expect("已登记提交请求必须仍有下一项动作");
            let (next, transition) = advance_mutation_candidate_with_outputs(
                opened,
                initial_market_directory,
                state,
                next_at,
                rules,
                finality_policy,
            );
            let Some(transition) = transition else {
                panic!("提交请求必须形成动作转换效果")
            };
            let MutationActionBusinessEffectV3::Post(effect) = transition.business else {
                panic!("提交请求必须只形成提交业务效果")
            };
            state = next;
            effects.push(effect);
            lifecycle.extend(transition.lifecycle);
        }
        (state, receipt, effects, lifecycle)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trades_query_exposes_mnb_at_fill_creation_and_freezes_old_root() {
        let finality_policy = matched_not_broadcasted_policy_v2_for_tests();
        let (_directory, mut actor) = active_market_actor_with_policy(
            SimCallerRoleV1::ModelTrain,
            "run-query-mnb-v2",
            Arc::clone(&finality_policy),
        )
        .await;
        let initial = decoded_initial_market_events("10");
        let condition_id = initial[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());
        let state = actor.state.take().expect("MNB 查询初始盘口前状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("MNB 查询初始盘口必须成功"));
        let post_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-fill-request".to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "mnb-fill-item",
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::FokMarket {
                    spend_pusd: "3".parse().expect("MNB FOK 预算"),
                    worst_price_pusd_per_share: "0.6".parse().expect("MNB FOK 最差价"),
                },
            ),
        });
        let (state, _, post_effects, post_lifecycle) = register_and_drain_post_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &post_request,
            29,
            1_200,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        let PostOrderItemOutcomeV2::Accepted(accepted) = &post_effects[0].outcome else {
            panic!("MNB FOK 必须通过真实请求入口形成成交")
        };
        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(accepted.fills.len(), 1);
        let [
            LifecycleFeedbackEffectV2::Order(_),
            LifecycleFeedbackEffectV2::Trade(initial_trade),
        ] = post_lifecycle.as_slice()
        else {
            panic!("MNB FOK 必须形成接受和成交首态效果")
        };
        assert_eq!(initial_trade.fill_sequence, 1);
        assert_eq!(
            initial_trade.matched_at_utc_ms,
            initial_trade.platform_event_at_utc_ms
        );
        assert_eq!(
            initial_trade.status.known,
            Some(contracts::SimulatedTradeKnownStatusV2::MatchedNotBroadcasted)
        );
        assert_eq!(initial_trade.path_ordinal, 0);
        assert!(initial_trade.account_delta.is_none());
        let fill_effective_at = post_effects[0].effective_processing_at_utc_ms;

        let first_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-first".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: None,
        });
        let (state, first_receipt, first_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &first_request,
            30,
            fill_effective_at + 1,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(first_page)) =
            &first_effect.result
        else {
            panic!("MNB 成交首次查询必须返回页面")
        };
        assert_eq!(first_page.root_sequence, 1);
        assert_eq!(first_page.items.len(), 1);
        assert_eq!(first_page.items[0].trade_id, "sim-1");
        assert_eq!(first_page.items[0].status.raw, "MATCHED_NOT_BROADCASTED");
        assert_eq!(
            first_page.items[0].status.known,
            Some(contracts::SimulatedTradeKnownStatusV2::MatchedNotBroadcasted)
        );
        assert!(first_page.next_cursor.is_none());
        let first_root_sequence = first_page.root_sequence;
        let first_response = Arc::clone(
            state
                .session
                .as_ref()
                .expect("MNB 首次查询后会话存在")
                .mutation_requests
                .requests_by_id["mnb-query-first"]
                .completed_response
                .as_ref()
                .expect("MNB 首次查询必须冻结响应"),
        );
        let DirectResponsePayloadDraftV3::Trades(first_response_result) = &first_response.payload
        else {
            panic!("成交列表查询必须密封成方法特定响应草稿")
        };
        assert!(Arc::ptr_eq(first_response_result, &first_effect));
        let next_finality_at = state
            .session
            .as_ref()
            .and_then(|session| session.execution_state.next_scheduled_finality_at_utc_ms())
            .expect("MNB 成交必须排定下一最终性状态");
        assert!(first_receipt.platform_arrived_at_utc_ms < next_finality_at);
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("MNB 首次查询后会话存在")
                .query_state
                .current_revision,
            first_root_sequence
        );

        let (state, finality_effects) = advance_finality_to_candidate(
            state,
            next_finality_at,
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        assert!(finality_effects.transition.is_some());
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("MNB 最终性推进后会话存在")
                .query_state
                .current_revision,
            first_root_sequence + 1
        );

        let old_root_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-old-root".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: first_root_sequence,
                next_offset: 0,
            }),
        });
        let (state, _, old_root_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &old_root_request,
            31,
            next_finality_at + 1,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(old_page)) =
            &old_root_effect.result
        else {
            panic!("MNB 旧根续读必须仍返回冻结页面")
        };
        assert_eq!(old_page.root_sequence, first_root_sequence);
        assert_eq!(old_page.items.len(), 1);
        assert_eq!(
            old_page.items[0].status.known,
            Some(contracts::SimulatedTradeKnownStatusV2::MatchedNotBroadcasted)
        );

        let current_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-current".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: None,
        });
        let (state, _, current_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &current_request,
            32,
            next_finality_at + 2,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(current_page)) =
            &current_effect.result
        else {
            panic!("MNB 最终性推进后的新查询必须返回新根")
        };
        assert_eq!(current_page.root_sequence, first_root_sequence + 1);
        assert_eq!(current_page.items.len(), 1);
        assert_eq!(current_page.items[0].status.raw, "MATCHED");
        assert_eq!(
            current_page.items[0].status.known,
            Some(contracts::SimulatedTradeKnownStatusV2::Matched)
        );
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("MNB 查询轨迹终态会话存在")
                .query_state
                .trade_roots
                .len(),
            2,
            "纯查询复用旧根，最终性变化后只新增一个新修订根"
        );

        let mut state = state;
        state
            .session
            .as_mut()
            .expect("根保留水位测试必须拥有会话")
            .query_state
            .earliest_retained_root_sequence = first_root_sequence + 1;
        assert_eq!(
            existing_mutation_request_receipt_v2(&state, "mnb-query-first"),
            Some(first_receipt),
            "保留水位推进后同一请求身份仍必须返回首次回执"
        );
        assert!(Arc::ptr_eq(
            state
                .session
                .as_ref()
                .expect("根保留水位推进后会话存在")
                .mutation_requests
                .requests_by_id["mnb-query-first"]
                .completed_response
                .as_ref()
                .expect("保留水位推进后首次响应仍存在"),
            &first_response,
        ));

        let expired_root_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-expired-root".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: first_root_sequence,
                next_offset: 0,
            }),
        });
        let (state, _, expired_root_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &expired_root_request,
            33,
            next_finality_at + 3,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        assert_eq!(
            expired_root_effect.result,
            QueryActionResultV2::Trades(TradesQueryResultV2::OutOfRetention(
                QueryOutOfRetentionV2::RootSnapshot {
                    requested_root_sequence: first_root_sequence,
                    earliest_retained_root_sequence: first_root_sequence + 1,
                }
            ))
        );

        let missing_filter_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-missing-filter-root".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1 {
                token_id: Some(TokenId("missing-token".to_owned())),
                ..TradesFilterV1::default()
            },
            cursor: Some(QueryCursorV2 {
                root_sequence: first_root_sequence + 1,
                next_offset: 0,
            }),
        });
        let (state, _, missing_filter_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &missing_filter_request,
            34,
            next_finality_at + 4,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        assert_eq!(
            missing_filter_effect.result,
            QueryActionResultV2::Trades(TradesQueryResultV2::InvalidCursor)
        );

        let terminal_boundary_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-terminal-boundary".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: first_root_sequence + 1,
                next_offset: 1,
            }),
        });
        let (state, _, terminal_boundary_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &terminal_boundary_request,
            35,
            next_finality_at + 5,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(terminal_page)) =
            &terminal_boundary_effect.result
        else {
            panic!("偏移恰等于根长度必须形成合法空终页")
        };
        assert_eq!(terminal_page.root_sequence, first_root_sequence + 1);
        assert!(terminal_page.items.is_empty());
        assert!(terminal_page.next_cursor.is_none());

        let beyond_boundary_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-beyond-boundary".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: first_root_sequence + 1,
                next_offset: 2,
            }),
        });
        let (mut state, _, beyond_boundary_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &beyond_boundary_request,
            36,
            next_finality_at + 6,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        assert_eq!(
            beyond_boundary_effect.result,
            QueryActionResultV2::Trades(TradesQueryResultV2::InvalidCursor)
        );

        let missing_order_request = BusinessRequestV2::Order(OrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-missing-order".to_owned(),
                caller_correlation_id: None,
            },
            platform_order_id: "sim-999".to_owned(),
        });
        let (next, _, missing_order_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &missing_order_request,
            37,
            next_finality_at + 7,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        state = next;
        assert_eq!(
            missing_order_effect.result,
            QueryActionResultV2::Order(OrderQueryResultV2::NotFound)
        );

        let query_state = &mut state
            .session
            .as_mut()
            .expect("成交历史水位测试必须拥有会话")
            .query_state;
        query_state.trade_history_retained_from_utc_ms = Some(fill_effective_at);
        query_state.advance_revision();
        let retained_interval_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-retained-interval".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1 {
                after_utc_ms: Some(fill_effective_at - 1),
                before_utc_ms: Some(fill_effective_at + 2),
                ..TradesFilterV1::default()
            },
            cursor: None,
        });
        let (next, _, retained_interval_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &retained_interval_request,
            38,
            next_finality_at + 8,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        state = next;
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(retained_interval_page)) =
            &retained_interval_effect.result
        else {
            panic!("首个包含毫秒等于成交历史水位时必须返回完整页面")
        };
        assert_eq!(retained_interval_page.items.len(), 1);
        assert_eq!(retained_interval_page.items[0].trade_id, "sim-1");

        let empty_interval_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-empty-discrete-interval".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1 {
                after_utc_ms: Some(fill_effective_at - 1),
                before_utc_ms: Some(fill_effective_at),
                ..TradesFilterV1::default()
            },
            cursor: None,
        });
        let (next, _, empty_interval_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &empty_interval_request,
            39,
            next_finality_at + 9,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        state = next;
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(empty_interval_page)) =
            &empty_interval_effect.result
        else {
            panic!("无离散毫秒落入的严格开区间必须返回合法空页")
        };
        assert!(empty_interval_page.items.is_empty());

        let retained_exact_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-retained-exact-trade".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1 {
                trade_id: Some("sim-1".to_owned()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        });
        let (next, _, retained_exact_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &retained_exact_request,
            40,
            next_finality_at + 10,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        state = next;
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(retained_exact_page)) =
            &retained_exact_effect.result
        else {
            panic!("仍在成交历史水位内的精确成交必须形成正向页面")
        };
        assert_eq!(retained_exact_page.items.len(), 1);

        let session = state
            .session
            .as_mut()
            .expect("旧终态事实水位测试必须拥有会话");
        session.query_state.trade_history_retained_from_utc_ms = Some(fill_effective_at + 1);
        session.query_state.order_history_retained_from_utc_ms = Some(fill_effective_at + 1);
        session.query_state.advance_revision();

        let expired_exact_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-expired-exact-trade".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1 {
                trade_id: Some("sim-1".to_owned()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        });
        let (next, _, expired_exact_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &expired_exact_request,
            41,
            next_finality_at + 11,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        state = next;
        assert_eq!(
            expired_exact_effect.result,
            QueryActionResultV2::Trades(TradesQueryResultV2::OutOfRetention(
                QueryOutOfRetentionV2::TradeHistory {
                    retained_from_utc_ms: fill_effective_at + 1,
                }
            ))
        );

        let expired_order_request = BusinessRequestV2::Order(OrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "mnb-query-expired-terminal-order".to_owned(),
                caller_correlation_id: None,
            },
            platform_order_id: "sim-1".to_owned(),
        });
        let (_, _, expired_order_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &expired_order_request,
            42,
            next_finality_at + 12,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        assert_eq!(
            expired_order_effect.result,
            QueryActionResultV2::Order(OrderQueryResultV2::OutOfRetention(
                QueryOutOfRetentionV2::OrderHistory {
                    retained_from_utc_ms: fill_effective_at + 1,
                }
            ))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_roots_share_revision_keep_filters_and_freeze_pages() {
        let finality_policy = matched_not_broadcasted_policy_v2_for_tests();
        let (_directory, mut actor) = active_market_actor_with_policy(
            SimCallerRoleV1::ModelTrain,
            "run-query-roots-v2",
            Arc::clone(&finality_policy),
        )
        .await;
        let quantity_opening = Arc::clone(
            &actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .opening_fact,
        );
        let initial = decoded_initial_market_events("0");
        let condition_id = initial[0].market.condition_id.clone();
        let first_token_id = TokenId("1".to_owned());
        let second_token_id = TokenId("2".to_owned());
        let state = actor.state.take().expect("查询根初始盘口前状态");
        let (mut state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("查询根初始盘口必须成功"));

        let mut post_items = Vec::with_capacity(102);
        post_items.push(PostOrderBatchItemV2 {
            batch_item_ordinal: 0,
            item: post_item(
                "query-root-item-0",
                &condition_id,
                &first_token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.6".parse().expect("查询根首单价格"),
                    size_shares: "6".parse().expect("查询根首单数量"),
                    post_only: false,
                },
            ),
        });
        for index in 1..=100_u16 {
            post_items.push(PostOrderBatchItemV2 {
                batch_item_ordinal: index,
                item: post_item(
                    &format!("query-root-item-{index}"),
                    &condition_id,
                    &first_token_id,
                    SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share: "0.01".parse().expect("查询根挂单价格"),
                        size_shares: "5".parse().expect("查询根挂单数量"),
                        post_only: false,
                    },
                ),
            });
        }
        post_items.push(PostOrderBatchItemV2 {
            batch_item_ordinal: 101,
            item: post_item(
                "query-root-item-101",
                &condition_id,
                &second_token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.01".parse().expect("查询根第二资产挂单价格"),
                    size_shares: "5".parse().expect("查询根第二资产挂单数量"),
                    post_only: false,
                },
            ),
        });
        for (batch_index, chunk) in post_items.chunks(15).enumerate() {
            let global_request_id = format!("query-root-post-batch-{batch_index}");
            let outbound_delay_ms = actor._latency_profile.sample_delay_ms(
                state
                    .session
                    .as_ref()
                    .expect("查询根提交批次前会话存在")
                    .session_seed
                    .0
                    .as_bytes(),
                &global_request_id,
                DelaySampleDomainV1::RequestOutbound,
            );
            let target_arrival_at = 1_300 + batch_index as i64;
            let decision_at =
                target_arrival_at - i64::try_from(outbound_delay_ms).expect("查询根出站时延可表示");
            let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
                context: RequestContextV2 {
                    global_request_id,
                    caller_correlation_id: None,
                },
                items: chunk.to_vec(),
            });
            let (next, receipt, effects, _) = register_and_drain_post_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                &request,
                200 + batch_index as u64,
                decision_at,
                actor._latency_profile.as_ref(),
                actor._rule_snapshot.snapshot(),
                finality_policy.as_ref(),
            );
            assert_eq!(receipt.platform_arrived_at_utc_ms, target_arrival_at);
            assert_eq!(effects.len(), chunk.len());
            assert!(
                effects
                    .iter()
                    .all(|effect| matches!(&effect.outcome, PostOrderItemOutcomeV2::Accepted(_)))
            );
            state = next;
        }
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("查询根挂单后会话存在")
                .query_state
                .current_revision,
            102
        );

        let single_request = BusinessRequestV2::Order(OrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-single".to_owned(),
                caller_correlation_id: None,
            },
            platform_order_id: "sim-1".to_owned(),
        });
        let (next, _, single_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &single_request,
            300,
            1_400,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;

        let QueryActionResultV2::Order(OrderQueryResultV2::Found(single_order)) =
            &single_effect.result
        else {
            panic!("活动订单单笔查询必须返回 Found")
        };

        assert_eq!(single_order.platform_order_id, "sim-1");
        assert_eq!(
            single_order.status.known,
            Some(SimulatedOrderKnownStatusV2::Live)
        );
        let contracts::OrderQuantityProjectionV2::Base {
            active_unfilled_base_quantity_shares,
            ..
        } = &single_order.quantities
        else {
            panic!("活动 GTC 必须使用 base 查询计量")
        };
        assert_eq!(active_unfilled_base_quantity_shares.0, "1");
        let single_response = state
            .session
            .as_ref()
            .expect("单笔订单查询后唯一会话存在")
            .mutation_requests
            .requests_by_id["query-root-single"]
            .completed_response
            .as_ref()
            .expect("单笔订单查询必须密封首次直接响应");
        let DirectResponsePayloadDraftV3::Order(single_response_result) = &single_response.payload
        else {
            panic!("单笔订单查询必须密封成方法特定响应草稿")
        };
        assert!(Arc::ptr_eq(single_response_result, &single_effect));

        let all_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-all".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: None,
        });
        let (next, _, all_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &all_request,
            301,
            1_401,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;

        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(all_page)) = &all_effect.result
        else {
            panic!("全量活动订单首页必须返回页面")
        };
        let projected = noe_projection::project_real_query_for_tests(
            &_directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
            &actor.opened,
            &quantity_opening,
            &all_effect.result,
        );
        assert_eq!(projected["Page"]["root_sequence"], all_page.root_sequence);
        assert_eq!(
            projected["Page"]["next_cursor"],
            serde_json::to_value(all_page.next_cursor).unwrap()
        );
        assert!(
            projected["Page"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|o| o.get("quantities").is_some())
        );

        assert_eq!(all_page.root_sequence, 102);
        assert_eq!(all_page.items.len(), 100);
        assert_eq!(
            all_page.next_cursor,
            Some(QueryCursorV2 {
                root_sequence: 102,
                next_offset: 100,
            })
        );

        let first_token_filter = OrdersFilterV1 {
            condition_id: Some(condition_id.clone()),
            token_id: Some(first_token_id.clone()),
        };

        let first_token_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-first-token".to_owned(),
                caller_correlation_id: None,
            },
            filter: first_token_filter.clone(),
            cursor: None,
        });
        let (next, _, first_token_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &first_token_request,
            302,
            1_402,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;

        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(first_token_page)) =
            &first_token_effect.result
        else {
            panic!("资产过滤活动订单首页必须返回页面")
        };

        assert_eq!(first_token_page.root_sequence, 102);
        assert_eq!(first_token_page.items.len(), 100);
        assert_eq!(first_token_page.next_cursor, all_page.next_cursor);

        let trade_filter = TradesFilterV1 {
            trade_id: Some("sim-1".to_owned()),
            condition_id: Some(condition_id.clone()),
            token_id: Some(first_token_id.clone()),
            after_utc_ms: Some(1_299),
            before_utc_ms: Some(1_301),
        };

        let trade_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-trade".to_owned(),
                caller_correlation_id: None,
            },
            filter: trade_filter.clone(),
            cursor: None,
        });
        let (next, _, trade_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &trade_request,
            303,
            1_402,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            finality_policy.as_ref(),
        );
        state = next;

        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(trade_page)) =
            &trade_effect.result
        else {
            panic!("同修订成交复合根必须返回页面")
        };

        assert_eq!(trade_page.root_sequence, 102);
        assert_eq!(trade_page.items.len(), 1);
        assert_eq!(trade_page.items[0].trade_id, "sim-1");
        assert_eq!(
            trade_page.items[0].status.known,
            Some(contracts::SimulatedTradeKnownStatusV2::MatchedNotBroadcasted)
        );

        let query_state = &state
            .session
            .as_ref()
            .expect("双过滤根后会话存在")
            .query_state;
        assert_eq!(query_state.current_revision, 102);
        assert_eq!(query_state.order_roots.len(), 2);
        assert_eq!(query_state.trade_roots.len(), 1);
        assert!(
            query_state
                .trade_roots
                .contains_key(&(102, TradesQueryKeyV2::from(&trade_filter)))
        );

        let repeated_all_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-all-repeat".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: None,
        });
        let (next, _, repeated_all_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &repeated_all_request,
            304,
            1_403,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;

        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(repeated_all_page)) =
            &repeated_all_effect.result
        else {
            panic!("同修订同过滤首页必须复用页面")
        };
        let projected = noe_projection::project_real_query_for_tests(
            &_directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
            &actor.opened,
            &quantity_opening,
            &repeated_all_effect.result,
        );
        assert_eq!(
            projected["Page"]["root_sequence"],
            repeated_all_page.root_sequence
        );
        assert_eq!(
            projected["Page"]["next_cursor"],
            serde_json::to_value(repeated_all_page.next_cursor).unwrap()
        );
        assert!(
            projected["Page"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|o| o.get("quantities").is_some())
        );

        assert_eq!(repeated_all_page, all_page);
        let query_state = &state
            .session
            .as_ref()
            .expect("重复首页后会话存在")
            .query_state;
        assert_eq!(query_state.current_revision, 102);
        assert_eq!(query_state.order_roots.len(), 2);

        let cancel_request = BusinessRequestV2::CancelOrder(CancelOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-cancel".to_owned(),
                caller_correlation_id: None,
            },
            item: CancelOrderItemV1 {
                item_id: "query-root-cancel-item".to_owned(),
                platform_order_id: "sim-1".to_owned(),
            },
        });
        let cancel_cutoff = request_cutoff(1_404);
        let (next, cancel_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &cancel_cutoff,
            &cancel_request,
            305,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, cancel_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            cancel_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let Some(MutationActionBusinessEffectV3::Cancel(cancel_effects)) = cancel_effect else {
            panic!("查询根撤单必须形成撤单效果")
        };
        assert!(matches!(
            cancel_effects[0].outcome,
            contracts::CancelTargetOutcome::Canceled(_)
        ));
        state = next;
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("撤单后会话存在")
                .query_state
                .current_revision,
            103
        );

        let all_continuation = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-all-continuation".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: all_page.next_cursor,
        });
        let (next, _, all_continuation_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &all_continuation,
            306,
            1_405,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(all_tail)) =
            &all_continuation_effect.result
        else {
            panic!("全量旧根续页必须成功")
        };
        assert_eq!(all_tail.root_sequence, 102);
        assert_eq!(all_tail.items.len(), 2);

        let first_token_continuation = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-first-token-continuation".to_owned(),
                caller_correlation_id: None,
            },
            filter: first_token_filter,
            cursor: first_token_page.next_cursor,
        });
        let (next, _, first_token_continuation_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &first_token_continuation,
            307,
            1_406,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(first_token_tail)) =
            &first_token_continuation_effect.result
        else {
            panic!("资产过滤旧根续页必须成功")
        };
        assert_eq!(first_token_tail.root_sequence, 102);
        assert_eq!(first_token_tail.items.len(), 1);

        let current_all_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-current-all".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: None,
        });
        let (mut state, _, current_all_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &current_all_request,
            308,
            1_407,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(current_all_page)) =
            &current_all_effect.result
        else {
            panic!("撤单后的新订单根必须成功")
        };
        let projected = noe_projection::project_real_query_for_tests(
            &_directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
            &actor.opened,
            &quantity_opening,
            &current_all_effect.result,
        );
        assert_eq!(
            projected["Page"]["root_sequence"],
            current_all_page.root_sequence
        );
        assert_eq!(
            projected["Page"]["next_cursor"],
            serde_json::to_value(current_all_page.next_cursor).unwrap()
        );
        assert!(
            projected["Page"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|o| o.get("quantities").is_some())
        );

        assert_eq!(current_all_page.root_sequence, 103);
        assert_eq!(current_all_page.items.len(), 100);
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("查询根测试终态会话存在")
                .query_state
                .current_revision,
            103,
            "查询和续页本身不得推进可观察修订"
        );

        state
            .session
            .as_mut()
            .expect("订单根水位测试必须拥有会话")
            .query_state
            .earliest_retained_root_sequence = 103;
        let expired_orders_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-orders-expired".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: 102,
                next_offset: 100,
            }),
        });
        let (next, _, expired_orders_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &expired_orders_request,
            309,
            1_408,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert_eq!(
            expired_orders_effect.result,
            QueryActionResultV2::Orders(OrdersQueryResultV2::OutOfRetention(
                QueryOutOfRetentionV2::RootSnapshot {
                    requested_root_sequence: 102,
                    earliest_retained_root_sequence: 103,
                }
            )),
            "订单列表必须先按逻辑根水位裁决，即使旧根尚未物理删除"
        );

        let missing_orders_filter = OrdersFilterV1 {
            condition_id: Some(condition_id),
            token_id: Some(second_token_id),
        };
        let missing_orders_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-orders-missing".to_owned(),
                caller_correlation_id: None,
            },
            filter: missing_orders_filter,
            cursor: Some(QueryCursorV2 {
                root_sequence: 103,
                next_offset: 0,
            }),
        });
        let (next, _, missing_orders_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &missing_orders_request,
            310,
            1_409,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert_eq!(
            missing_orders_effect.result,
            QueryActionResultV2::Orders(OrdersQueryResultV2::InvalidCursor)
        );

        let terminal_orders_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-orders-terminal".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: 103,
                next_offset: 101,
            }),
        });
        let (next, _, terminal_orders_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &terminal_orders_request,
            311,
            1_410,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(terminal_orders_page)) =
            &terminal_orders_effect.result
        else {
            panic!("订单列表偏移恰等于根长度必须形成合法空终页")
        };
        assert!(terminal_orders_page.items.is_empty());
        assert!(terminal_orders_page.next_cursor.is_none());

        let beyond_orders_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-root-orders-beyond".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: Some(QueryCursorV2 {
                root_sequence: 103,
                next_offset: 102,
            }),
        });
        let (_, _, beyond_orders_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &beyond_orders_request,
            312,
            1_411,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        for effect in [
            &expired_orders_effect,
            &missing_orders_effect,
            &beyond_orders_effect,
        ] {
            let projected = noe_projection::project_real_query_for_tests(
                &_directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
                &actor.opened,
                &quantity_opening,
                &effect.result,
            );
            let QueryActionResultV2::Orders(source) = &effect.result else {
                unreachable!()
            };
            assert_eq!(
                projected,
                serde_json::to_value(source).unwrap(),
                "无数量分支不能生成数量或改写保留范围"
            );
        }
        assert_eq!(
            beyond_orders_effect.result,
            QueryActionResultV2::Orders(OrdersQueryResultV2::InvalidCursor)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_maker_stays_private_until_first_positive_maker_fill_changes_query_root() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-query-maker-v2").await;
        let quantity_opening = Arc::clone(
            &actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .opening_fact,
        );
        let initial = decoded_initial_market_events("0");
        let condition_id = initial[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());
        let state = actor.state.take().expect("maker 查询初始盘口前状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("maker 查询初始盘口必须成功"));

        let post_request_id = "query-maker-post";
        let outbound_delay_ms = actor._latency_profile.sample_delay_ms(
            state
                .session
                .as_ref()
                .expect("maker 查询提交前会话存在")
                .session_seed
                .0
                .as_bytes(),
            post_request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let post_decision_at =
            1_200 - i64::try_from(outbound_delay_ms).expect("maker 查询出站时延可表示");
        let post_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: post_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "query-maker-item",
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("maker 查询限价"),
                    size_shares: "5".parse().expect("maker 查询数量"),
                    post_only: false,
                },
            ),
        });
        let (state, receipt, post_effects, _) = register_and_drain_post_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &post_request,
            320,
            post_decision_at,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(receipt.platform_arrived_at_utc_ms, 1_200);
        assert!(matches!(
            &post_effects[0].outcome,
            PostOrderItemOutcomeV2::Accepted(_)
        ));
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("maker 查询挂单后会话存在")
                .query_state
                .current_revision,
            1
        );

        let before_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-maker-before".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: None,
        });
        let (state, _, before_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &before_request,
            321,
            1_220,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(before_page)) =
            &before_effect.result
        else {
            panic!("maker 穿越前活动订单查询必须返回页面")
        };
        assert_eq!(before_page.root_sequence, 1);
        assert_eq!(before_page.items.len(), 1);
        let contracts::OrderQuantityProjectionV2::Base {
            matched_base_quantity_shares,
            active_unfilled_base_quantity_shares,
            ..
        } = &before_page.items[0].quantities
        else {
            panic!("maker 查询订单必须使用 base 计量")
        };
        assert_eq!(matched_base_quantity_shares.0, "0");
        assert_eq!(active_unfilled_base_quantity_shares.0, "5");
        let frozen_before_page = before_page.clone();

        let first_zero_quote_cycle = [
            decoded_maker_level_event(BookSide::Bid, "0.3", "0", "0.000001", 1_300, 1_301, 320),
            decoded_maker_level_event(BookSide::Bid, "0.3", "0.000001", "0", 1_300, 1_302, 321),
        ];
        let (state, zero_effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_300,
            &first_zero_quote_cycle,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("首次零对价 maker 穿越必须成功"));
        assert!(
            zero_effects
                .execution
                .iter()
                .all(|effect| !effect.query_projection_changed()),
            "私有 pending 分桶变化不得伪造公开查询变化"
        );
        let session = state.session.as_ref().expect("零对价 maker 穿越后会话存在");
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests("sim-1"),
            Some((0, 1, 4_999_999, 0, 1_500_000))
        );
        assert_eq!(session.query_state.current_revision, 1);

        let repeated_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-maker-after-zero".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: None,
        });
        let (mut state, _, repeated_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &repeated_request,
            322,
            1_320,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(repeated_page)) =
            &repeated_effect.result
        else {
            panic!("零对价 maker 后活动订单查询必须成功")
        };
        assert_eq!(repeated_page, &frozen_before_page);
        let projected = noe_projection::project_real_query_for_tests(
            &_directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
            &actor.opened,
            &quantity_opening,
            &repeated_effect.result,
        );
        assert_eq!(
            projected["Page"]["items"][0]["quantities"]["Base"]["active_unfilled_base_quantity_shares"],
            "5"
        );
        assert_eq!(projected["Page"]["root_sequence"], 1);

        let session = state.session.as_ref().expect("零对价 maker 查询后会话存在");
        assert_eq!(session.query_state.current_revision, 1);
        assert_eq!(session.query_state.order_roots.len(), 1);

        state
            .session
            .as_mut()
            .expect("活动订单历史水位测试必须拥有会话")
            .query_state
            .order_history_retained_from_utc_ms = Some(1_301);
        let current_order_request = BusinessRequestV2::Order(OrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-maker-current-order".to_owned(),
                caller_correlation_id: None,
            },
            platform_order_id: "sim-1".to_owned(),
        });
        let (mut state, _, current_order_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &current_order_request,
            323,
            1_330,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let QueryActionResultV2::Order(OrderQueryResultV2::Found(current_order)) =
            &current_order_effect.result
        else {
            panic!("零对价 maker 后单笔订单必须仍可查")
        };
        let contracts::OrderQuantityProjectionV2::Base {
            matched_base_quantity_shares,
            active_unfilled_base_quantity_shares,
            ..
        } = &current_order.quantities
        else {
            panic!("零对价 maker 后订单必须使用 base 计量")
        };
        assert_eq!(matched_base_quantity_shares.0, "0");
        assert_eq!(active_unfilled_base_quantity_shares.0, "5");
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("活动订单历史水位测试后会话存在")
                .query_state
                .order_history_retained_from_utc_ms,
            Some(1_301),
            "活动订单必须无条件可查，且查询不得改写历史水位"
        );

        for cycle in 1..4_u64 {
            let source_at_utc_ms = 1_300 + i64::try_from(cycle).expect("maker 循环可表示") * 100;
            let sequence = 322 + cycle * 2;
            let events = [
                decoded_maker_level_event(
                    BookSide::Bid,
                    "0.3",
                    "0",
                    "0.000001",
                    source_at_utc_ms,
                    source_at_utc_ms + 1,
                    sequence,
                ),
                decoded_maker_level_event(
                    BookSide::Bid,
                    "0.3",
                    "0.000001",
                    "0",
                    source_at_utc_ms,
                    source_at_utc_ms + 2,
                    sequence + 1,
                ),
            ];
            let (next, effects) = reduce_market_event_slice_v2(
                state,
                MarketEventPathV1::HiddenPlatform,
                source_at_utc_ms,
                &events,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            )
            .unwrap_or_else(|_| panic!("后续 maker 穿越必须成功"));
            if cycle < 3 {
                assert!(
                    effects
                        .execution
                        .iter()
                        .all(|effect| !effect.query_projection_changed())
                );
            } else {
                assert!(
                    effects
                        .execution
                        .iter()
                        .any(ExecutionMarketEventEffectsV2::query_projection_changed)
                );
                assert_eq!(
                    effects
                        .execution
                        .iter()
                        .flat_map(ExecutionMarketEventEffectsV2::initial_finality_for_tests)
                        .map(|effect| effect.fill_sequence)
                        .collect::<Vec<_>>(),
                    vec![1]
                );
            }
            state = next;
        }

        let session = state
            .session
            .as_ref()
            .expect("首次正对价 maker 成交后会话存在");
        assert_eq!(session.query_state.current_revision, 2);
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests("sim-1"),
            Some((4, 0, 4_999_996, 1, 1_499_999))
        );
        assert_eq!(session.execution_state.fill_shape_for_tests(), (1, 2));

        let new_order_request = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-maker-positive-order".to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1::default(),
            cursor: None,
        });
        let (state, _, new_order_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &new_order_request,
            324,
            1_650,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(new_order_page)) =
            &new_order_effect.result
        else {
            panic!("首次正对价 maker 后必须形成新订单根")
        };
        assert_eq!(new_order_page.root_sequence, 2);
        assert_eq!(new_order_page.items.len(), 1);
        let contracts::OrderQuantityProjectionV2::Base {
            matched_base_quantity_shares,
            active_unfilled_base_quantity_shares,
            matched_quote_amount_pusd,
            ..
        } = &new_order_page.items[0].quantities
        else {
            panic!("首次正对价 maker 后订单必须使用 base 计量")
        };
        assert_eq!(matched_base_quantity_shares.0, "0.000004");
        assert_eq!(active_unfilled_base_quantity_shares.0, "4.999996");
        assert_eq!(matched_quote_amount_pusd.0, "0.000001");

        let trade_request = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: "query-maker-positive-trade".to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1::default(),
            cursor: None,
        });
        let (state, _, trade_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &trade_request,
            325,
            1_660,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(trade_page)) =
            &trade_effect.result
        else {
            panic!("首次正对价 maker 成交必须进入 trades")
        };
        assert_eq!(trade_page.root_sequence, 2);
        assert_eq!(trade_page.items.len(), 1);
        assert_eq!(trade_page.items[0].trade_id, "sim-1");
        assert_eq!(
            trade_page.items[0].liquidity_role,
            contracts::TradeLiquidityRoleV2::MakerInferred
        );
        assert_eq!(trade_page.items[0].base_quantity_shares.0, "0.000004");
        assert_eq!(trade_page.items[0].quote_amount_pusd.0, "0.000001");
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("maker 查询终态会话存在")
                .query_state
                .current_revision,
            2,
            "查询本身不得推进公开修订"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn d0007_market_buy_minimum_is_original_principal_not_five_shares_or_partial_fill() {
        for (case, spend, depth, market) in [
            ("fok-one", "1", "100", 0),
            ("fak-one", "1", "100", 1),
            ("fok-below", ".99", "100", 0),
            ("fak-below", ".99", "100", 1),
            ("fak-partial", "1", "1", 1),
            ("gtc-two", "1", "100", 2),
            ("gtd-two", "1", "100", 3),
        ] {
            let (_directory, mut actor) =
                active_market_actor(SimCallerRoleV1::ModelTrain, case).await;
            let initial = decoded_initial_market_events(depth)
                .into_iter()
                .map(|mut event| {
                    let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book) = &mut event.payload
                    else {
                        unreachable!()
                    };
                    book.asks[0].price_pusd_per_share = ExactDecimal("0.5".into());
                    book.asks[0].quantity_shares = ExactDecimal(depth.into());
                    event.event_id =
                        data_pipeline_core::derive_execution_market_event_v3_id(&event).unwrap();
                    let bytes =
                        data_pipeline_core::encode_execution_market_event_v3(&event).unwrap();
                    data_pipeline_core::decode_execution_market_event_v3(&bytes).unwrap()
                })
                .collect::<Vec<_>>();
            let condition = initial[0].market.condition_id.clone();
            let token = TokenId("1".into());
            let (state, _) = reduce_market_event_slice_v2(
                actor.state.take().unwrap(),
                MarketEventPathV1::HiddenPlatform,
                1100,
                &initial,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            )
            .unwrap_or_else(|_| panic!("valid original book"));
            let before = state
                .session
                .as_ref()
                .unwrap()
                .execution_state
                .account_balances_for_tests();
            let kind = match market {
                0 => SimBuyOrderKindV1::FokMarket {
                    spend_pusd: spend.parse().unwrap(),
                    worst_price_pusd_per_share: ".5".parse().unwrap(),
                },
                1 => SimBuyOrderKindV1::FakMarket {
                    spend_pusd: spend.parse().unwrap(),
                    worst_price_pusd_per_share: ".5".parse().unwrap(),
                },
                2 => SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: ".5".parse().unwrap(),
                    size_shares: 2.into(),
                    post_only: false,
                },
                _ => SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: ".5".parse().unwrap(),
                    size_shares: 2.into(),
                    post_only: false,
                    expires_at_utc_ms: 300_000,
                },
            };
            let request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                context: RequestContextV2 {
                    global_request_id: case.into(),
                    caller_correlation_id: None,
                },
                item: post_item(case, &condition, &token, kind),
            });
            let (state, _, effects, lifecycle) = register_and_drain_post_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                &request,
                123,
                1200,
                actor._latency_profile.as_ref(),
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            );
            assert_eq!(effects.len(), 1);
            if spend == ".99" || market >= 2 {
                assert!(
                    matches!(&effects[0].outcome, PostOrderItemOutcomeV2::RejectedNoEffect(reason) if *reason==if market>=2 {SubmissionRejectedReasonV2::BelowMinimumShares}else{SubmissionRejectedReasonV2::BelowMarketBuyMinimumPrincipal})
                );
                assert!(lifecycle.is_empty());
                assert_eq!(
                    state
                        .session
                        .as_ref()
                        .unwrap()
                        .execution_state
                        .account_balances_for_tests(),
                    before
                );
                assert_eq!(
                    state
                        .session
                        .as_ref()
                        .unwrap()
                        .execution_state
                        .fill_shape_for_tests()
                        .0,
                    0
                );
            } else {
                let PostOrderItemOutcomeV2::Accepted(accepted) = &effects[0].outcome else {
                    panic!("{case}: {:?}", effects[0].outcome)
                };
                let contracts::SubmissionMeasurementV2::QuoteBudget {
                    accepted_quote_budget_pusd,
                    used_quote_amount_pusd,
                    released_or_unspent_quote_amount_pusd,
                    actual_matched_base_quantity_shares,
                    sdk_taker_amount_audit_base_quantity_shares,
                    ..
                } = &accepted.measurement
                else {
                    panic!()
                };
                assert_eq!(accepted_quote_budget_pusd.0, "1");
                assert_eq!(sdk_taker_amount_audit_base_quantity_shares.0, "2");
                assert_eq!(
                    used_quote_amount_pusd.0,
                    if depth == "1" { "0.5" } else { "1" }
                );
                assert_eq!(
                    actual_matched_base_quantity_shares.0,
                    if depth == "1" { "1" } else { "2.000001" }
                );
                assert_eq!(
                    released_or_unspent_quote_amount_pusd.0,
                    if depth == "1" { "0.5" } else { "0" }
                );
                assert!(!accepted.fills.is_empty());
                if depth == "1" {
                    assert!(lifecycle.iter().any(|event|matches!(event,LifecycleFeedbackEffectV2::Order(order) if matches!(&order.event, contracts::OrderLifecycleEventV2::FakUnspentReleased{released_quote_amount_pusd,..} if released_quote_amount_pusd.0=="0.5"))));
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn large_batches_register_all_items_and_preserve_cancel_input_refs() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-large-batches").await;
        let condition_id = decoded_initial_market_events("10")[0]
            .market
            .condition_id
            .clone();
        let post = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "large-post".to_owned(),
                caller_correlation_id: None,
            },
            items: (0..16_u16)
                .map(|ordinal| PostOrderBatchItemV2 {
                    batch_item_ordinal: ordinal,
                    item: post_item(
                        &format!("post-{ordinal}"),
                        &condition_id,
                        &TokenId("1".to_owned()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.3".parse().unwrap(),
                            size_shares: "5".parse().unwrap(),
                            post_only: false,
                        },
                    ),
                })
                .collect(),
        });
        let state = actor.state.take().unwrap();
        let (state, receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_200),
            &post,
            11,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        assert_eq!(receipt.scheduled_action_count, 16);
        let actions = &state
            .session
            .as_ref()
            .unwrap()
            .mutation_requests
            .pending_actions;
        assert_eq!(actions.len(), 16);
        let cancel = BusinessRequestV2::CancelOrders(CancelOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "large-cancel".to_owned(),
                caller_correlation_id: None,
            },
            items: (0..1001_u16)
                .map(|ordinal| CancelOrderBatchItemV2 {
                    batch_item_ordinal: ordinal,
                    item: CancelOrderItemV1 {
                        item_id: format!("cancel-{ordinal}"),
                        platform_order_id: "sim-1".to_owned(),
                    },
                })
                .collect(),
        });
        let (mut state, receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_200),
            &cancel,
            12,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        assert_eq!(receipt.scheduled_action_count, 1);
        // 只执行 16 个提交动作和一个聚合后的撤单动作。
        let mut completed = None;
        let mut posted = false;
        for _ in 0..17 {
            let at = state
                .session
                .as_ref()
                .unwrap()
                .mutation_requests
                .pending_actions
                .first_key_value()
                .unwrap()
                .0
                .effective_processing_at_utc_ms;
            let (next, transition) = advance_mutation_candidate_with_outputs(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                at,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            );
            state = next;
            if let Some(response) = transition.and_then(|t| t.completed_response) {
                if let DirectResponsePayloadDraftV3::PostOrders(items) = &response.payload {
                    assert_eq!(
                        items
                            .iter()
                            .map(|item| item.batch_item_ordinal)
                            .collect::<Vec<_>>(),
                        (0..16).collect::<Vec<_>>()
                    );
                    posted = true;
                }
                if let DirectResponsePayloadDraftV3::CancelOrders(targets) = &response.payload {
                    assert_eq!(targets.len(), 1);
                    assert_eq!(targets[0].input_item_refs.len(), 1001);
                    assert_eq!(
                        targets[0]
                            .input_item_refs
                            .last()
                            .unwrap()
                            .batch_item_ordinal,
                        1000
                    );
                    completed = Some(response);
                }
            }
        }
        assert!(completed.is_some());
        assert!(posted);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mutation_requests_freeze_first_post_results_and_deduplicate_cancel_targets() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-mutation-v2").await;
        let initial = decoded_initial_market_events("10");
        let condition_id = initial[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());
        let state = actor.state.take().expect("请求测试初始盘口前状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("请求测试初始盘口必须成功"));

        let global_request_id = "post-batch-v2";
        let caller_correlation_id = "post-correlation";
        let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: global_request_id.to_owned(),
                caller_correlation_id: Some(caller_correlation_id.to_owned()),
            },
            items: vec![
                PostOrderBatchItemV2 {
                    batch_item_ordinal: 7,
                    item: post_item(
                        "duplicate-item",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.3".parse().expect("GTC 价格"),
                            size_shares: "5".parse().expect("GTC 数量"),
                            post_only: false,
                        },
                    ),
                },
                PostOrderBatchItemV2 {
                    batch_item_ordinal: 1,
                    item: post_item(
                        "fok-item",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::FokMarket {
                            spend_pusd: "3.6".parse().expect("FOK 预算"),
                            worst_price_pusd_per_share: "0.6".parse().expect("FOK 最差价"),
                        },
                    ),
                },
                PostOrderBatchItemV2 {
                    batch_item_ordinal: 3,
                    item: post_item(
                        "duplicate-item",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::FakMarket {
                            spend_pusd: "3.6".parse().expect("FAK 预算"),
                            worst_price_pusd_per_share: "0.6".parse().expect("FAK 最差价"),
                        },
                    ),
                },
            ],
        });
        let cutoff = request_cutoff(1_200);

        let (mut state, receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &cutoff,
            &request,
            11,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        assert_eq!(receipt.scheduled_action_count, 3);

        let delayed_at = receipt
            .platform_arrived_at_utc_ms
            .checked_add(150)
            .expect("测试暂缓时点可表示");
        let keys = state
            .session
            .as_ref()
            .expect("请求登记后唯一会话存在")
            .mutation_requests
            .pending_actions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: receipt.platform_arrived_at_utc_ms,
                    input_sequence: 11,
                    batch_item_ordinal: 7,
                },
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: delayed_at,
                    input_sequence: 11,
                    batch_item_ordinal: 1,
                },
                MutationActionKeyV2 {
                    effective_processing_at_utc_ms: delayed_at,
                    input_sequence: 11,
                    batch_item_ordinal: 3,
                },
            ]
        );

        let (next, first_transition) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(first_transition) = first_transition else {
            panic!("首个普通 GTC 动作必须形成提交结果")
        };
        assert!(first_transition.completed_response.is_none());
        let [LifecycleFeedbackEffectV2::Order(order_accepted)] =
            first_transition.lifecycle.as_slice()
        else {
            panic!("首个 GTC 必须只形成订单接受生命周期效果")
        };
        let contracts::OrderLifecycleEventV2::OrderAccepted {
            status,
            account_delta,
        } = &order_accepted.event
        else {
            panic!("首个 GTC 生命周期效果必须是订单接受")
        };
        assert_eq!(order_accepted.platform_order_id, "sim-1");
        assert_eq!(order_accepted.condition_id, condition_id);
        assert_eq!(order_accepted.token_id, token_id);
        assert_eq!(status.raw, "LIVE");
        assert_eq!(status.known, Some(SimulatedOrderKnownStatusV2::Live));
        assert_eq!(account_delta.account_transition_seq, 1);
        assert!(matches!(
            &account_delta.reason,
            contracts::PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                platform_order_id
            } if platform_order_id == "sim-1"
        ));
        let MutationActionBusinessEffectV3::Post(gtc_effect) = first_transition.business else {
            panic!("首个普通 GTC 动作必须形成提交结果")
        };
        let PostOrderItemOutcomeV2::Accepted(gtc_accepted) = &gtc_effect.outcome else {
            panic!("普通 GTC 必须接受")
        };
        assert_eq!(gtc_accepted.platform_order_id, "sim-1");
        assert_eq!(gtc_accepted.platform_order_status.raw, "LIVE");
        assert_eq!(
            gtc_accepted.platform_order_status.known,
            Some(SimulatedOrderKnownStatusV2::Live)
        );
        assert_eq!(gtc_accepted.terminal_reason, None);
        assert!(gtc_accepted.fills.is_empty());
        assert_eq!(gtc_accepted.account_transition_sequences, vec![1]);
        let contracts::SubmissionMeasurementV2::Base {
            accepted_base_quantity_shares,
            matched_base_quantity_shares,
            open_base_quantity_shares,
            venue_dust_closed_base_quantity_shares,
            order_type_terminated_base_quantity_shares,
            accepted_quote_amount_pusd,
            matched_quote_amount_pusd,
            open_encumbered_quote_amount_pusd,
            venue_dust_released_quote_amount_pusd,
            released_price_improvement_quote_amount_pusd,
            immediate_taker_fee_encumbered_pusd,
        } = &gtc_accepted.measurement
        else {
            panic!("普通 GTC 必须冻结 base 计量")
        };
        assert_eq!(accepted_base_quantity_shares.0, "5");
        assert_eq!(matched_base_quantity_shares.0, "0");
        assert_eq!(open_base_quantity_shares.0, "5");
        assert_eq!(venue_dust_closed_base_quantity_shares.0, "0");
        assert_eq!(order_type_terminated_base_quantity_shares.0, "0");
        assert_eq!(accepted_quote_amount_pusd.0, "1.5");
        assert_eq!(matched_quote_amount_pusd.0, "0");
        assert_eq!(open_encumbered_quote_amount_pusd.0, "1.5");
        assert_eq!(venue_dust_released_quote_amount_pusd.0, "0");
        assert_eq!(released_price_improvement_quote_amount_pusd.0, "0");
        assert_eq!(immediate_taker_fee_encumbered_pusd.0, "0");

        let frozen_gtc = Arc::clone(&gtc_effect);

        let (next, second_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            delayed_at,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(MutationActionBusinessEffectV3::Post(fok_effect)) = second_effect else {
            panic!("第二个 FOK 动作必须形成提交结果")
        };
        assert!(matches!(
            &fok_effect.outcome,
            PostOrderItemOutcomeV2::RejectedNoEffect(SubmissionRejectedReasonV2::FokNotFullyFilled)
        ));
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("FOK 后唯一会话存在")
                .mutation_requests
                .requests_by_id[global_request_id]
                .post_results
                .iter()
                .map(|result| result.batch_item_ordinal)
                .collect::<Vec<_>>(),
            vec![7, 1],
            "请求完成前不为尚不可交付的结果反复排序"
        );

        let (next, third_transition) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            delayed_at,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(third_transition) = third_transition else {
            panic!("第三个 FAK 动作必须形成提交结果")
        };
        let third_response = Arc::clone(
            third_transition
                .completed_response
                .as_ref()
                .expect("批量最后一项必须一次密封直接响应"),
        );
        assert_eq!(third_transition.lifecycle.len(), 3);
        let LifecycleFeedbackEffectV2::Order(fak_accepted_lifecycle) =
            &third_transition.lifecycle[0]
        else {
            panic!("FAK 首个生命周期效果必须是订单接受")
        };
        assert!(matches!(
            &fak_accepted_lifecycle.event,
            contracts::OrderLifecycleEventV2::OrderAccepted { status, account_delta }
                if status.known == Some(SimulatedOrderKnownStatusV2::Matched)
                    && account_delta.account_transition_seq == 2
        ));
        let LifecycleFeedbackEffectV2::Order(fak_release_lifecycle) =
            &third_transition.lifecycle[1]
        else {
            panic!("FAK 第二个生命周期效果必须是未花预算释放")
        };
        assert!(matches!(
            &fak_release_lifecycle.event,
            contracts::OrderLifecycleEventV2::FakUnspentReleased {
                released_quote_amount_pusd,
                account_delta,
            } if released_quote_amount_pusd.0 == "0.6"
                && account_delta.account_transition_seq == 3
        ));
        let LifecycleFeedbackEffectV2::Trade(initial_trade_lifecycle) =
            &third_transition.lifecycle[2]
        else {
            panic!("FAK 第三个生命周期效果必须是成交首态")
        };
        assert_eq!(initial_trade_lifecycle.trade_id, "sim-1");
        assert_eq!(initial_trade_lifecycle.platform_order_id, "sim-2");
        assert_eq!(initial_trade_lifecycle.path_ordinal, 0);
        assert_eq!(initial_trade_lifecycle.status.raw, "MATCHED");
        assert!(initial_trade_lifecycle.account_delta.is_none());
        let MutationActionBusinessEffectV3::Post(fak_effect) = third_transition.business else {
            panic!("第三个 FAK 动作必须形成提交结果")
        };
        let PostOrderItemOutcomeV2::Accepted(fak_accepted) = &fak_effect.outcome else {
            panic!("有正成交的 FAK 必须接受")
        };
        assert_eq!(fak_accepted.platform_order_id, "sim-2");
        assert_eq!(fak_accepted.platform_order_status.raw, "MATCHED");
        assert_eq!(
            fak_accepted.platform_order_status.known,
            Some(SimulatedOrderKnownStatusV2::Matched)
        );
        assert_eq!(
            fak_accepted.terminal_reason,
            Some(contracts::SimOrderTerminalReasonV2::FakCompleted)
        );
        assert_eq!(fak_accepted.account_transition_sequences, vec![2, 3]);
        assert_eq!(fak_accepted.fills.len(), 1);
        assert_eq!(fak_accepted.fills[0].trade_id, "sim-1");
        assert_eq!(fak_accepted.fills[0].source_price_pusd_per_share.0, "0.6");
        assert_eq!(fak_accepted.fills[0].base_quantity_shares.0, "5");
        assert_eq!(fak_accepted.fills[0].quote_amount_pusd.0, "3");
        let contracts::SubmissionMeasurementV2::QuoteBudget {
            accepted_quote_budget_pusd,
            used_quote_amount_pusd,
            released_or_unspent_quote_amount_pusd,
            actual_matched_base_quantity_shares,
            sdk_taker_amount_audit_base_quantity_shares,
            immediate_taker_fee_encumbered_pusd,
        } = &fak_accepted.measurement
        else {
            panic!("FAK 必须冻结 quote-budget 计量")
        };
        assert_eq!(accepted_quote_budget_pusd.0, "3.6");
        assert_eq!(used_quote_amount_pusd.0, "3");
        assert_eq!(released_or_unspent_quote_amount_pusd.0, "0.6");
        assert_eq!(actual_matched_base_quantity_shares.0, "5");
        assert_eq!(sdk_taker_amount_audit_base_quantity_shares.0, "6");
        assert_eq!(immediate_taker_fee_encumbered_pusd.0, "0.084");

        let request_record = &state
            .session
            .as_ref()
            .expect("提交批量完成后唯一会话存在")
            .mutation_requests
            .requests_by_id[global_request_id];
        assert_eq!(request_record.remaining_action_count, 0);
        assert!(request_record.post_results.is_empty());
        assert!(Arc::ptr_eq(
            request_record
                .completed_response
                .as_ref()
                .expect("完成请求必须保存首次直接响应"),
            &third_response,
        ));
        let DirectResponsePayloadDraftV3::PostOrders(items) = &third_response.payload else {
            panic!("批量提交必须密封成方法特定响应草稿")
        };
        assert_eq!(
            items
                .iter()
                .map(|result| result.batch_item_ordinal)
                .collect::<Vec<_>>(),
            vec![1, 3, 7]
        );
        assert!(Arc::ptr_eq(&items[2], &frozen_gtc));
        assert_eq!(third_response.platform_responded_at_utc_ms, delayed_at);
        assert_eq!(
            third_response.response_received_at_utc_ms,
            delayed_at + request_record.inbound_delay_ms as i64
        );

        let altered_retry = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: global_request_id.to_owned(),
                caller_correlation_id: Some("later-different-correlation".to_owned()),
            },
            items: Vec::new(),
        });

        assert_eq!(
            existing_mutation_request_receipt_v2(
                &state,
                &altered_retry.context().global_request_id
            ),
            Some(receipt),
            "物理重投必须在验证后来正文前返回首次回执"
        );

        let cancel_request_id = "cancel-batch-v2";
        let cancel_request = BusinessRequestV2::CancelOrders(CancelOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: cancel_request_id.to_owned(),
                caller_correlation_id: None,
            },
            items: vec![
                CancelOrderBatchItemV2 {
                    batch_item_ordinal: 2,
                    item: CancelOrderItemV1 {
                        item_id: "ref-a".to_owned(),
                        platform_order_id: "sim-1".to_owned(),
                    },
                },
                CancelOrderBatchItemV2 {
                    batch_item_ordinal: 1,
                    item: CancelOrderItemV1 {
                        item_id: "ref-b".to_owned(),
                        platform_order_id: "sim-999".to_owned(),
                    },
                },
                CancelOrderBatchItemV2 {
                    batch_item_ordinal: 0,
                    item: CancelOrderItemV1 {
                        item_id: "ref-c".to_owned(),
                        platform_order_id: "sim-1".to_owned(),
                    },
                },
            ],
        });

        let (next, cancel_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_500),
            &cancel_request,
            12,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        state = next;
        assert_eq!(cancel_receipt.scheduled_action_count, 2);

        let (next, cancel_first) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            cancel_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(cancel_first) = cancel_first else {
            panic!("首个批撤动作必须形成撤单结果")
        };
        assert!(cancel_first.completed_response.is_none());
        let [LifecycleFeedbackEffectV2::Order(cancel_lifecycle)] =
            cancel_first.lifecycle.as_slice()
        else {
            panic!("成功撤单必须只形成一条订单生命周期效果")
        };
        assert_eq!(cancel_lifecycle.platform_order_id, "sim-1");
        assert_eq!(cancel_lifecycle.condition_id, condition_id);
        assert_eq!(cancel_lifecycle.token_id, token_id);
        let contracts::OrderLifecycleEventV2::CallerCanceled {
            canceled_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } = &cancel_lifecycle.event
        else {
            panic!("成功撤单必须形成调用方撤单生命周期效果")
        };
        assert_eq!(canceled_base_quantity_shares.0, "5");
        assert_eq!(released_quote_amount_pusd.0, "1.5");
        assert_eq!(
            account_delta
                .as_ref()
                .expect("撤单释放开放本金必须形成账户变化")
                .account_transition_seq,
            4
        );
        let MutationActionBusinessEffectV3::Cancel(first_targets) = cancel_first.business else {
            panic!("首个批撤动作必须形成撤单结果")
        };
        assert_eq!(first_targets.len(), 1);
        let first_target = &first_targets[0];
        assert_eq!(first_target.target_ordinal, 0);
        assert_eq!(first_target.platform_order_id, "sim-1");
        assert_eq!(
            first_target
                .input_item_refs
                .iter()
                .map(|reference| (reference.item_id.as_str(), reference.batch_item_ordinal))
                .collect::<Vec<_>>(),
            vec![("ref-a", 2), ("ref-c", 0)]
        );
        let contracts::CancelTargetOutcome::Canceled(canceled) = &first_target.outcome else {
            panic!("sim-1 必须成功撤销开放余量")
        };
        assert_eq!(canceled.canceled_base_quantity_shares.0, "5");
        assert_eq!(canceled.released_quote_amount_pusd.0, "1.5");
        assert_eq!(canceled.account_transition_seq, Some(4));

        let (next, cancel_second) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            cancel_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(cancel_second) = cancel_second else {
            panic!("第二个批撤动作必须形成撤单结果")
        };
        assert!(cancel_second.lifecycle.is_empty());
        let cancel_response = Arc::clone(
            cancel_second
                .completed_response
                .as_ref()
                .expect("批撤最后一项必须一次密封直接响应"),
        );
        let MutationActionBusinessEffectV3::Cancel(second_targets) = cancel_second.business else {
            panic!("第二个批撤动作必须形成撤单结果")
        };
        assert_eq!(second_targets.len(), 1);
        assert_eq!(second_targets[0].platform_order_id, "sim-999");
        assert!(matches!(
            &second_targets[0].outcome,
            contracts::CancelTargetOutcome::NotCanceledNoEffect(
                contracts::CancelNotCanceledReasonV3::NotFoundOrNotOwned
            )
        ));

        let cancel_record = &state
            .session
            .as_ref()
            .expect("批撤完成后唯一会话存在")
            .mutation_requests
            .requests_by_id[cancel_request_id];
        assert_eq!(cancel_record.remaining_action_count, 0);
        assert!(cancel_record.cancel_results.is_empty());
        assert!(Arc::ptr_eq(
            cancel_record
                .completed_response
                .as_ref()
                .expect("完成批撤必须保存首次直接响应"),
            &cancel_response,
        ));
        let DirectResponsePayloadDraftV3::CancelOrders(targets) = &cancel_response.payload else {
            panic!("批撤必须密封成方法特定响应草稿")
        };
        assert_eq!(
            targets
                .iter()
                .map(|result| (result.target_ordinal, result.platform_order_id.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "sim-1"), (1, "sim-999")]
        );

        let PostOrderItemOutcomeV2::Accepted(frozen) = &frozen_gtc.outcome else {
            unreachable!("此前已确认 GTC 被接受")
        };
        assert_eq!(frozen.platform_order_status.raw, "LIVE");
        let contracts::SubmissionMeasurementV2::Base {
            matched_base_quantity_shares,
            open_base_quantity_shares,
            open_encumbered_quote_amount_pusd,
            ..
        } = &frozen.measurement
        else {
            unreachable!("此前已确认 GTC 使用 base 计量")
        };
        assert_eq!(matched_base_quantity_shares.0, "0");
        assert_eq!(open_base_quantity_shares.0, "5");
        assert_eq!(open_encumbered_quote_amount_pusd.0, "1.5");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn single_mutation_methods_preserve_gtd_priority_and_cancel_all_snapshot() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-single-mutations-v2").await;
        let initial = decoded_initial_market_events("10");
        let condition_id = initial[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());
        let state = actor.state.take().expect("单方法请求测试初始状态");
        let (mut state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("单方法请求测试初始盘口必须成功"));

        let equal_request_id = "post-single-gtd-equal";
        let equal_cutoff_at = 1_200;
        let encoded_equal_expiration = encoded_gtd_expiration_for_lead_for_tests(
            &state,
            actor._latency_profile.as_ref(),
            equal_request_id,
            equal_cutoff_at,
            240,
        );
        let equal_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: equal_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "single-gtd-equal-item",
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("GTD 价格"),
                    size_shares: "5".parse().expect("GTD 数量"),
                    post_only: false,
                    expires_at_utc_ms: encoded_equal_expiration + 999,
                },
            ),
        });
        let (next, equal_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(equal_cutoff_at),
            &equal_request,
            20,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, equal_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            equal_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(MutationActionBusinessEffectV3::Post(equal_effect)) = equal_effect else {
            panic!("240 秒等号 GTD 必须形成单笔提交结果")
        };
        let PostOrderItemOutcomeV2::Accepted(equal_accepted) = &equal_effect.outcome else {
            panic!("240 秒等号 GTD 必须通过")
        };
        assert_eq!(equal_accepted.platform_order_id, "sim-1");
        let equal_response = state
            .session
            .as_ref()
            .expect("单笔提交后唯一会话存在")
            .mutation_requests
            .requests_by_id[equal_request_id]
            .completed_response
            .as_ref()
            .expect("单笔提交必须密封首次直接响应");
        let DirectResponsePayloadDraftV3::PostOrder(equal_response_item) = &equal_response.payload
        else {
            panic!("单笔提交必须密封成方法特定响应草稿")
        };
        assert!(Arc::ptr_eq(equal_response_item, &equal_effect));
        assert!(
            state
                .session
                .as_ref()
                .expect("GTD 接受后唯一会话存在")
                .execution_state
                .has_gtd_deadline_for_tests(encoded_equal_expiration, "sim-1"),
            "实际截止必须使用 SDK 秒级编码后的毫秒值"
        );

        let cancel_one_request = BusinessRequestV2::CancelOrder(CancelOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "cancel-single-gtd".to_owned(),
                caller_correlation_id: None,
            },
            item: CancelOrderItemV1 {
                item_id: "cancel-single-gtd-item".to_owned(),
                platform_order_id: "sim-1".to_owned(),
            },
        });
        let (next, cancel_one_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_250),
            &cancel_one_request,
            21,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, cancel_one_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            cancel_one_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(MutationActionBusinessEffectV3::Cancel(cancel_one_effects)) = cancel_one_effect
        else {
            panic!("单笔撤单必须形成目标结果")
        };
        assert_eq!(cancel_one_effects.len(), 1);
        assert!(matches!(
            &cancel_one_effects[0].outcome,
            contracts::CancelTargetOutcome::Canceled(_)
        ));
        let cancel_one_response = state
            .session
            .as_ref()
            .expect("单笔撤单后唯一会话存在")
            .mutation_requests
            .requests_by_id["cancel-single-gtd"]
            .completed_response
            .as_ref()
            .expect("单笔撤单必须密封首次直接响应");
        let DirectResponsePayloadDraftV3::CancelOrder(cancel_one_response_target) =
            &cancel_one_response.payload
        else {
            panic!("单笔撤单必须密封成方法特定响应草稿")
        };
        assert!(Arc::ptr_eq(
            cancel_one_response_target,
            &cancel_one_effects[0]
        ));

        let too_soon_request_id = "post-single-gtd-too-soon";
        let too_soon_cutoff_at = 1_300;
        let too_soon_expiration = encoded_gtd_expiration_for_lead_for_tests(
            &state,
            actor._latency_profile.as_ref(),
            too_soon_request_id,
            too_soon_cutoff_at,
            239,
        );
        let too_soon_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: too_soon_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "single-gtd-too-soon-item",
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("GTD 价格"),
                    size_shares: "5".parse().expect("GTD 数量"),
                    post_only: false,
                    expires_at_utc_ms: too_soon_expiration,
                },
            ),
        });
        let (next, too_soon_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(too_soon_cutoff_at),
            &too_soon_request,
            22,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, too_soon_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            too_soon_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(MutationActionBusinessEffectV3::Post(too_soon_effect)) = too_soon_effect else {
            panic!("239 秒 GTD 必须形成单笔提交结果")
        };
        assert!(matches!(
            &too_soon_effect.outcome,
            PostOrderItemOutcomeV2::RejectedNoEffect(
                SubmissionRejectedReasonV2::GtdExpirationTooSoon
            )
        ));

        let post_only_request_id = "post-single-gtd-post-only";
        let post_only_cutoff_at = 1_350;
        let post_only_expiration = encoded_gtd_expiration_for_lead_for_tests(
            &state,
            actor._latency_profile.as_ref(),
            post_only_request_id,
            post_only_cutoff_at,
            239,
        );
        let post_only_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: post_only_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "single-gtd-post-only-item",
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: "0.6".parse().expect("GTD 可成交价格"),
                    size_shares: "5".parse().expect("GTD 数量"),
                    post_only: true,
                    expires_at_utc_ms: post_only_expiration,
                },
            ),
        });
        let (next, post_only_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(post_only_cutoff_at),
            &post_only_request,
            23,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, post_only_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            post_only_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(MutationActionBusinessEffectV3::Post(post_only_effect)) = post_only_effect else {
            panic!("post-only GTD 必须形成单笔提交结果")
        };
        assert!(matches!(
            &post_only_effect.outcome,
            PostOrderItemOutcomeV2::RejectedNoEffect(SubmissionRejectedReasonV2::PostOnlyWouldTake)
        ));

        let resting_batch = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "post-before-cancel-all".to_owned(),
                caller_correlation_id: None,
            },
            items: vec![
                PostOrderBatchItemV2 {
                    batch_item_ordinal: 1,
                    item: post_item(
                        "cancel-all-second",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.3".parse().expect("GTC 价格"),
                            size_shares: "5".parse().expect("GTC 数量"),
                            post_only: false,
                        },
                    ),
                },
                PostOrderBatchItemV2 {
                    batch_item_ordinal: 0,
                    item: post_item(
                        "cancel-all-first",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.3".parse().expect("GTC 价格"),
                            size_shares: "5".parse().expect("GTC 数量"),
                            post_only: false,
                        },
                    ),
                },
            ],
        });
        let (next, resting_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_400),
            &resting_batch,
            24,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, _) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            resting_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let (next, _) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            resting_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("全撤前唯一会话存在")
                .execution_state
                .active_platform_order_ids(),
            vec!["sim-2".to_owned(), "sim-3".to_owned()]
        );

        let pending_events = [
            decoded_maker_level_event(BookSide::Bid, "0.3", "0", "0.000001", 1_420, 1_421, 70),
            decoded_maker_level_event(BookSide::Bid, "0.3", "0.000001", "0", 1_420, 1_421, 71),
        ];
        let (next, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_420,
            &pending_events,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("真实档位减量必须形成 pending maker 候选"));
        state = next;
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("pending maker 后唯一会话存在")
                .execution_state
                .fill_shape_for_tests(),
            (0, 1),
            "零 quote maker 不得伪造成交或消耗 fill_sequence"
        );

        let cancel_all_request_id = "cancel-all-snapshot";
        let cancel_all_request = BusinessRequestV2::CancelAllOrders(CancelAllOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: cancel_all_request_id.to_owned(),
                caller_correlation_id: None,
            },
        });

        let (next, cancel_all_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_450),
            &cancel_all_request,
            25,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, cancel_all_effect) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            cancel_all_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(cancel_all_effect) = cancel_all_effect else {
            panic!("全撤必须形成目标结果")
        };
        assert_eq!(cancel_all_effect.lifecycle.len(), 2);
        for (index, lifecycle) in cancel_all_effect.lifecycle.iter().enumerate() {
            let LifecycleFeedbackEffectV2::Order(lifecycle) = lifecycle else {
                panic!("全撤只能形成订单生命周期效果")
            };
            let contracts::OrderLifecycleEventV2::CallerCanceled {
                canceled_base_quantity_shares,
                released_quote_amount_pusd,
                account_delta,
            } = &lifecycle.event
            else {
                panic!("全撤目标必须形成调用方撤单生命周期效果")
            };
            assert_eq!(lifecycle.platform_order_id, format!("sim-{}", index + 2));
            assert_eq!(lifecycle.condition_id, condition_id);
            assert_eq!(lifecycle.token_id, token_id);
            assert_eq!(canceled_base_quantity_shares.0, "5");
            assert_eq!(released_quote_amount_pusd.0, "1.5");
            assert_eq!(
                account_delta
                    .as_ref()
                    .expect("全撤必须释放开放本金")
                    .account_transition_seq,
                index as u64 + 5
            );
        }
        let cancel_all_response = Arc::clone(
            cancel_all_effect
                .completed_response
                .as_ref()
                .expect("全撤单一动作必须一次密封直接响应"),
        );
        let MutationActionBusinessEffectV3::Cancel(cancel_all_effects) = cancel_all_effect.business
        else {
            panic!("全撤必须形成目标结果")
        };
        assert_eq!(
            cancel_all_effects
                .iter()
                .map(|effect| effect.platform_order_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sim-2", "sim-3"]
        );
        assert!(
            cancel_all_effects.iter().all(|effect| matches!(
                &effect.outcome,
                contracts::CancelTargetOutcome::Canceled(_)
            ))
        );
        let contracts::CancelTargetOutcome::Canceled(first_canceled) =
            &cancel_all_effects[0].outcome
        else {
            unreachable!("首个全撤目标已经确认成功")
        };
        let contracts::CancelTargetOutcome::Canceled(second_canceled) =
            &cancel_all_effects[1].outcome
        else {
            unreachable!("第二个全撤目标已经确认成功")
        };
        assert_eq!(first_canceled.canceled_base_quantity_shares.0, "5");
        assert_eq!(first_canceled.released_quote_amount_pusd.0, "1.5");
        assert_eq!(first_canceled.account_transition_seq, Some(5));
        assert_eq!(second_canceled.canceled_base_quantity_shares.0, "5");
        assert_eq!(second_canceled.released_quote_amount_pusd.0, "1.5");
        assert_eq!(second_canceled.account_transition_seq, Some(6));
        assert_eq!(
            state
                .session
                .as_ref()
                .expect("全撤后唯一会话存在")
                .execution_state
                .external_queue_base_units_for_tests(
                    &condition_id.0,
                    &token_id.0,
                    BookSide::Bid,
                    "0.3".parse().expect("全撤测试价格"),
                ),
            1,
            "全撤不得删除真实档位减量留下的外部队尾段"
        );

        let later_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "post-after-cancel-all".to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "later-order",
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("GTC 价格"),
                    size_shares: "5".parse().expect("GTC 数量"),
                    post_only: false,
                },
            ),
        });
        let (next, later_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_500),
            &later_request,
            26,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (next, later_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            later_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let Some(MutationActionBusinessEffectV3::Post(later_effect)) = later_effect else {
            panic!("全撤后的新单必须形成提交结果")
        };
        let PostOrderItemOutcomeV2::Accepted(later_accepted) = &later_effect.outcome else {
            panic!("全撤后的新单必须独立接受")
        };
        assert_eq!(later_accepted.platform_order_id, "sim-4");
        let session = state.session.as_ref().expect("全撤冻结测试唯一会话存在");
        assert_eq!(
            session.execution_state.active_platform_order_ids(),
            vec!["sim-4".to_owned()]
        );
        assert_eq!(
            match &session.mutation_requests.requests_by_id[cancel_all_request_id]
                .completed_response
                .as_ref()
                .expect("全撤完成后必须保存首次直接响应")
                .payload
            {
                DirectResponsePayloadDraftV3::CancelAllOrders(results) => results,
                _ => panic!("全撤必须保存方法特定响应草稿"),
            }
            .iter()
            .map(|result| result.platform_order_id.as_str())
            .collect::<Vec<_>>(),
            vec!["sim-2", "sim-3"],
            "全撤首次目标集不得顺带包含后来订单"
        );
        assert!(Arc::ptr_eq(
            session.mutation_requests.requests_by_id[cancel_all_request_id]
                .completed_response
                .as_ref()
                .expect("全撤完成后必须保存首次直接响应"),
            &cancel_all_response,
        ));

        let altered_retry = BusinessRequestV2::CancelAllOrders(CancelAllOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: cancel_all_request_id.to_owned(),
                caller_correlation_id: Some("later-correlation".to_owned()),
            },
        });
        assert_eq!(
            existing_mutation_request_receipt_v2(
                &state,
                &altered_retry.context().global_request_id
            ),
            Some(cancel_all_receipt)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancel_market_orders_filters_active_targets_and_keeps_other_market_reserved() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-cancel-market-v2").await;
        let other_market = market_with_window('2', "456", "3", "4", 1_000, 3_000);
        actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-cancel-market-other".to_owned(),
                market: other_market.clone(),
            })
            .expect("登记另一市场");

        let target_events = decoded_initial_market_events("10");
        let target_condition_id = target_events[0].market.condition_id.clone();
        let target_token_id = TokenId("1".to_owned());
        let other_events =
            decoded_initial_events_for_market_with_quantity(&other_market, 1_100, 1_110, 10, "10");
        let state = actor.state.take().expect("市场撤单测试初始状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &target_events,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("目标市场初始盘口必须成功"));
        let (mut state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &other_events,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("另一市场初始盘口必须成功"));

        let resting_order =
            |batch_item_ordinal, item_id, condition_id: &ConditionId, token_id: &TokenId| {
                PostOrderBatchItemV2 {
                    batch_item_ordinal,
                    item: post_item(
                        item_id,
                        condition_id,
                        token_id,
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.3".parse().expect("固定测试价格"),
                            size_shares: "5".parse().expect("固定测试数量"),
                            post_only: false,
                        },
                    ),
                }
            };
        let post_request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "post-before-cancel-market".to_owned(),
                caller_correlation_id: None,
            },
            items: vec![
                resting_order(0, "target-first", &target_condition_id, &target_token_id),
                resting_order(1, "target-second", &target_condition_id, &target_token_id),
                resting_order(
                    2,
                    "other-market",
                    &other_market.market().condition_id,
                    &other_market.assets()[0].token_id,
                ),
            ],
        });
        let (next, post_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_200),
            &post_request,
            20,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        state = next;
        for _ in 0..3 {
            let (next, effect) = advance_mutation_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                post_receipt.platform_arrived_at_utc_ms,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            );
            state = next;
            assert!(matches!(
                effect,
                Some(MutationActionBusinessEffectV3::Post(_))
            ));
        }
        let request_id = "cancel-target-market";
        let cancel_request = BusinessRequestV2::CancelMarketOrders(CancelMarketOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: request_id.to_owned(),
                caller_correlation_id: None,
            },
            condition_id: target_condition_id.clone(),
        });

        let (next, receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_300),
            &cancel_request,
            21,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        state = next;
        assert_eq!(receipt.scheduled_action_count, 1);

        let (next, effect) = advance_mutation_candidate_with_outputs(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        state = next;
        let effect = effect.expect("市场撤单必须形成目标结果");
        assert_eq!(effect.lifecycle.len(), 2);
        let MutationActionBusinessEffectV3::Cancel(targets) = effect.business else {
            panic!("市场撤单必须形成撤单业务结果")
        };
        assert_eq!(
            targets
                .iter()
                .map(|target| (target.target_ordinal, target.platform_order_id.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "sim-1"), (1, "sim-2")]
        );
        assert!(
            targets.iter().all(|target| matches!(
                target.outcome,
                contracts::CancelTargetOutcome::Canceled(_)
            ))
        );

        let session = state.session.as_ref().expect("市场撤单后唯一会话存在");
        assert_eq!(
            session.execution_state.active_platform_order_ids(),
            vec!["sim-3".to_owned()],
            "另一市场活动订单必须保持开放"
        );
        let (other_order, active, _) = session
            .execution_state
            .order_query_projection_v2("sim-3")
            .expect("读取另一市场订单")
            .expect("另一市场订单必须保留");
        assert!(active);
        assert_eq!(other_order.condition_id, other_market.market().condition_id);
        let contracts::OrderQuantityProjectionV2::Base {
            active_unfilled_base_quantity_shares,
            open_encumbered_quote_amount_pusd,
            ..
        } = other_order.quantities
        else {
            panic!("另一市场 GTC 必须使用 base 计量")
        };
        assert_eq!(active_unfilled_base_quantity_shares.0, "5");
        assert_eq!(open_encumbered_quote_amount_pusd.0, "1.5");
        let response = session.mutation_requests.requests_by_id[request_id]
            .completed_response
            .as_ref()
            .expect("市场撤单必须一次密封直接响应");
        let BusinessDirectResponse::CancelMarketOrders(response) =
            finalize_direct_response_v2(response, 1)
        else {
            panic!("市场撤单必须保留方法特定响应身份")
        };
        assert_eq!(
            response.metadata.method,
            ExecutionMethodV2::CancelMarketOrders
        );
        assert_eq!(response.targets.len(), 2);

        let empty_request = BusinessRequestV2::CancelMarketOrders(CancelMarketOrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: "cancel-missing-market".to_owned(),
                caller_correlation_id: None,
            },
            condition_id: ConditionId("missing-market".to_owned()),
        });
        let (next, empty_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(1_400),
            &empty_request,
            22,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        let (state, empty_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            empty_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let Some(MutationActionBusinessEffectV3::Cancel(empty_targets)) = empty_effect else {
            panic!("无匹配市场撤单必须形成空成功结果")
        };
        assert!(empty_targets.is_empty());
        let session = state.session.as_ref().expect("空市场撤单后唯一会话存在");
        let response = session.mutation_requests.requests_by_id["cancel-missing-market"]
            .completed_response
            .as_ref()
            .expect("无匹配市场撤单必须密封直接响应");
        assert!(matches!(
            &response.payload,
            DirectResponsePayloadDraftV3::CancelMarketOrders(targets) if targets.is_empty()
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn training_request_candidate_freezes_later_market_enrollment() {
        for (role, suffix) in [
            (SimCallerRoleV1::ModelTrain, "train"),
            (SimCallerRoleV1::OfflineEvaluation, "offline"),
        ] {
            let run_id = format!("run-{suffix}-request-freeze-v2");
            let (_directory, mut actor) = active_market_actor(role, &run_id).await;
            let initial_market = actor
                .scenario
                .initial_market_directory()
                .markets()
                .next()
                .expect("场景至少有一个初始市场");
            let request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                context: RequestContextV2 {
                    global_request_id: format!("{suffix}-request-freeze"),
                    caller_correlation_id: None,
                },
                item: post_item(
                    &format!("{suffix}-request-freeze-item"),
                    &initial_market.market().condition_id,
                    &initial_market.assets()[0].token_id,
                    SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share: "0.3".parse().expect("训练测试价格"),
                        size_shares: "5".parse().expect("训练测试数量"),
                        post_only: false,
                    },
                ),
            });
            let state = actor.state.take().expect("训练请求冻结测试初始状态");
            let (state, _) = register_request_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                &request_cutoff(1_200),
                &request,
                29,
                actor._latency_profile.as_ref(),
                actor._rule_snapshot.snapshot(),
            );
            assert!(
                state
                    .session
                    .as_ref()
                    .expect("训练请求候选必须持有唯一会话")
                    .market_enrollment
                    .business_input_started
            );

            let later_enrollment = MarketEnrollmentRequestV1 {
                enrollment_request_id: format!("enroll-after-{suffix}-request"),
                market: market('d', "890", "102", "103"),
            };
            let state = match reduce_business_input_v2(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                BusinessReducerInputV2::EnrollMarket(&later_enrollment),
            ) {
                Err(BusinessReductionErrorV2::Unaccepted { state, error }) => {
                    assert_eq!(
                        error,
                        SimRunActorErrorV2::MarketEnrollmentFrozenAfterBusinessInput
                    );
                    state
                }
                Ok(_) | Err(BusinessReductionErrorV2::Fatal(_)) => {
                    panic!("训练或离线请求候选形成后必须冻结市场登记")
                }
            };

            assert_eq!(
                state
                    .session
                    .as_ref()
                    .expect("登记拒绝后唯一会话仍存在")
                    .mutation_requests
                    .pending_actions
                    .len(),
                1,
                "登记拒绝不得破坏已经形成的请求候选"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_post_fences_only_an_unknown_explicit_condition() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-paper-post-fence-v2").await;
        let future_token_id = TokenId("99".to_owned());
        let fenced_market = market('b', "456", &future_token_id.0, "100");
        let fenced_condition_id = fenced_market.market().condition_id.clone();
        let unknown_condition_request_id = "paper-post-unknown-condition";
        let unknown_condition_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: unknown_condition_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "unknown-condition-item",
                &fenced_condition_id,
                &future_token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("Paper 测试价格"),
                    size_shares: "5".parse().expect("Paper 测试数量"),
                    post_only: false,
                },
            ),
        });
        let state = actor.state.take().expect("Paper 围栏测试初始状态");

        let outbound_delay_ms = actor._latency_profile.sample_delay_ms(
            state
                .session
                .as_ref()
                .expect("Paper 围栏测试唯一会话")
                .session_seed
                .0
                .as_bytes(),
            unknown_condition_request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let unknown_condition_cutoff =
            1_500 - i64::try_from(outbound_delay_ms).expect("Paper 测试出站时延可表示");
        let (next, unknown_condition_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(unknown_condition_cutoff),
            &unknown_condition_request,
            30,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        assert_eq!(unknown_condition_receipt.platform_arrived_at_utc_ms, 1_500);

        let session = next.session.as_ref().expect("未知市场请求登记后唯一会话");
        assert!(
            session
                .market_enrollment
                .paper_candidate_condition_ids
                .contains(&fenced_condition_id.0)
        );
        assert!(
            session
                .market_enrollment
                .paper_candidate_token_ids
                .is_empty(),
            "显式携带 condition_id 的下单不得另行冻结 token"
        );
        let (next, unknown_condition_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            unknown_condition_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let Some(MutationActionBusinessEffectV3::Post(unknown_condition_effect)) =
            unknown_condition_effect
        else {
            panic!("未知市场下单必须形成正常业务结果")
        };
        assert!(matches!(
            &unknown_condition_effect.outcome,
            PostOrderItemOutcomeV2::RejectedNoEffect(
                SubmissionRejectedReasonV2::MarketNotInScenario
            )
        ));

        let fenced_enrollment = MarketEnrollmentRequestV1 {
            enrollment_request_id: "enroll-fenced-condition".to_owned(),
            market: fenced_market,
        };
        let state = match reduce_business_input_v2(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            BusinessReducerInputV2::EnrollMarket(&fenced_enrollment),
        ) {
            Err(BusinessReductionErrorV2::Unaccepted { state, error }) => {
                assert_eq!(
                    error,
                    SimRunActorErrorV2::MarketEnrollmentCandidateAlreadyUsed(
                        fenced_condition_id.0.clone()
                    )
                );
                state
            }
            Ok(_) | Err(BusinessReductionErrorV2::Fatal(_)) => {
                panic!("Paper 已使用的未知 condition 必须阻止后来登记")
            }
        };

        let known_condition_id = actor
            .scenario
            .initial_market_directory()
            .markets()
            .next()
            .expect("场景至少有一个初始市场")
            .market()
            .condition_id
            .clone();
        let unknown_token_request_id = "paper-post-known-condition-unknown-token";
        let unknown_token_request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: unknown_token_request_id.to_owned(),
                caller_correlation_id: None,
            },
            item: post_item(
                "unknown-token-item",
                &known_condition_id,
                &future_token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("Paper 测试价格"),
                    size_shares: "5".parse().expect("Paper 测试数量"),
                    post_only: false,
                },
            ),
        });
        let outbound_delay_ms = actor._latency_profile.sample_delay_ms(
            state
                .session
                .as_ref()
                .expect("未知 token 请求前唯一会话")
                .session_seed
                .0
                .as_bytes(),
            unknown_token_request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let unknown_token_cutoff =
            1_500 - i64::try_from(outbound_delay_ms).expect("Paper 测试出站时延可表示");
        let (next, unknown_token_receipt) = register_request_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &request_cutoff(unknown_token_cutoff),
            &unknown_token_request,
            31,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
        );
        assert_eq!(unknown_token_receipt.platform_arrived_at_utc_ms, 1_500);
        assert!(
            next.session
                .as_ref()
                .expect("未知 token 请求登记后唯一会话")
                .market_enrollment
                .paper_candidate_token_ids
                .is_empty(),
            "已知 condition 下的未知 token 拒绝不得阻止其他市场以后使用该 token"
        );
        let (next, unknown_token_effect) = advance_mutation_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            unknown_token_receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        let Some(MutationActionBusinessEffectV3::Post(unknown_token_effect)) = unknown_token_effect
        else {
            panic!("未知 token 下单必须形成正常业务结果")
        };
        assert!(matches!(
            &unknown_token_effect.outcome,
            PostOrderItemOutcomeV2::RejectedNoEffect(SubmissionRejectedReasonV2::AssetNotInMarket)
        ));

        let reusable_token_market = market('c', "789", &future_token_id.0, "101");
        let reusable_token_enrollment = MarketEnrollmentRequestV1 {
            enrollment_request_id: "enroll-reusable-token".to_owned(),
            market: reusable_token_market.clone(),
        };
        let (state, output) = reduce_business_input_v2(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            next,
            BusinessReducerInputV2::EnrollMarket(&reusable_token_enrollment),
        )
        .unwrap_or_else(|_| panic!("未被 token-only 查询冻结的 token 必须仍可用于市场登记"));
        let BusinessReducerOutputV2::MarketEnrollment(enrollment) = output else {
            panic!("成功登记必须返回市场登记结果")
        };
        assert_eq!(enrollment.market, *reusable_token_market.market());
        assert!(
            state
                .session
                .as_ref()
                .expect("登记复用 token 后唯一会话")
                .market_directory
                .market(&reusable_token_market.market().condition_id.0)
                .is_some()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_queries_fence_one_filter_identity_and_retry_reuses_first_request() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-paper-query-fence-v2").await;
        let condition_filter_market = market('d', "901", "201", "202");
        let condition_filter_id = condition_filter_market.market().condition_id.clone();
        let condition_filter_token = TokenId("201".to_owned());
        let condition_query_id = "paper-query-condition";
        let condition_query = BusinessRequestV2::Orders(OrdersRequestV2 {
            context: RequestContextV2 {
                global_request_id: condition_query_id.to_owned(),
                caller_correlation_id: None,
            },
            filter: OrdersFilterV1 {
                condition_id: Some(condition_filter_id.clone()),
                token_id: Some(condition_filter_token.clone()),
            },
            cursor: None,
        });
        let state = actor.state.take().expect("Paper 查询围栏初始状态");

        let outbound_delay_ms = actor._latency_profile.sample_delay_ms(
            state
                .session
                .as_ref()
                .expect("Paper condition 查询前会话存在")
                .session_seed
                .0
                .as_bytes(),
            condition_query_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let decision_at =
            1_500 - i64::try_from(outbound_delay_ms).expect("Paper 查询出站时延可表示");
        let (state, condition_receipt, condition_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &condition_query,
            40,
            decision_at,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(condition_receipt.platform_arrived_at_utc_ms, 1_500);
        let QueryActionResultV2::Orders(OrdersQueryResultV2::Page(condition_page)) =
            &condition_effect.result
        else {
            panic!("未知 condition 的 Paper 订单查询必须形成合法空页")
        };
        assert!(condition_page.items.is_empty());

        let session = state
            .session
            .as_ref()
            .expect("Paper condition 查询后会话存在");
        assert!(
            session
                .market_enrollment
                .paper_candidate_condition_ids
                .contains(&condition_filter_id.0)
        );
        assert!(
            session
                .market_enrollment
                .paper_candidate_token_ids
                .is_empty(),
            "显式 condition 过滤必须优先且不得另建 token 围栏"
        );
        let first_response = Arc::clone(
            session.mutation_requests.requests_by_id[condition_query_id]
                .completed_response
                .as_ref()
                .expect("首次查询响应必须冻结在统一请求记录"),
        );
        let DirectResponsePayloadDraftV3::Orders(first_result) = &first_response.payload else {
            panic!("订单列表查询必须密封成方法特定响应草稿")
        };
        assert!(Arc::ptr_eq(first_result, &condition_effect));

        let retry_token_id = TokenId("203".to_owned());
        let altered_retry = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: condition_query_id.to_owned(),
                caller_correlation_id: Some("later-correlation".to_owned()),
            },
            filter: TradesFilterV1 {
                token_id: Some(retry_token_id.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        });

        let session_before_retry = state.session.as_ref().expect("物理重投前会话存在");
        let request_count_before_retry =
            session_before_retry.mutation_requests.requests_by_id.len();
        let root_count_before_retry = session_before_retry.query_state.order_roots.len()
            + session_before_retry.query_state.trade_roots.len();
        assert_eq!(
            existing_mutation_request_receipt_v2(
                &state,
                &altered_retry.context().global_request_id
            ),
            Some(condition_receipt),
            "同一全局请求身份跨方法重投必须在后来正文前返回首次回执"
        );
        let session_after_retry = state.session.as_ref().expect("物理重投后会话存在");

        assert_eq!(
            session_after_retry.mutation_requests.requests_by_id.len(),
            request_count_before_retry
        );
        assert!(
            session_after_retry
                .mutation_requests
                .pending_actions
                .is_empty()
        );
        assert_eq!(
            session_after_retry.query_state.order_roots.len()
                + session_after_retry.query_state.trade_roots.len(),
            root_count_before_retry
        );
        assert!(Arc::ptr_eq(
            session_after_retry.mutation_requests.requests_by_id[condition_query_id]
                .completed_response
                .as_ref()
                .expect("物理重投后首次响应仍存在"),
            &first_response
        ));
        assert!(
            !session_after_retry
                .market_enrollment
                .paper_candidate_token_ids
                .contains(&retry_token_id.0),
            "后来重投正文不得建立第二围栏"
        );

        let retry_token_market = market('e', "902", &retry_token_id.0, "204");
        let retry_token_enrollment = MarketEnrollmentRequestV1 {
            enrollment_request_id: "enroll-retry-token".to_owned(),
            market: retry_token_market.clone(),
        };
        let (state, output) = reduce_business_input_v2(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            BusinessReducerInputV2::EnrollMarket(&retry_token_enrollment),
        )
        .unwrap_or_else(|_| panic!("重投正文中的 token 未被采用，必须仍可用于登记"));
        let BusinessReducerOutputV2::MarketEnrollment(enrollment) = output else {
            panic!("重投 token 市场登记必须成功")
        };
        assert_eq!(enrollment.market, *retry_token_market.market());

        let token_only_id = "paper-query-token-only";
        let token_only = TokenId("205".to_owned());
        let token_only_query = BusinessRequestV2::Trades(TradesRequestV2 {
            context: RequestContextV2 {
                global_request_id: token_only_id.to_owned(),
                caller_correlation_id: None,
            },
            filter: TradesFilterV1 {
                token_id: Some(token_only.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        });

        let outbound_delay_ms = actor._latency_profile.sample_delay_ms(
            state
                .session
                .as_ref()
                .expect("Paper token 查询前会话存在")
                .session_seed
                .0
                .as_bytes(),
            token_only_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let decision_at =
            1_600 - i64::try_from(outbound_delay_ms).expect("Paper token 查询时延可表示");
        let (state, token_receipt, token_effect) = register_and_advance_query_candidate(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            &token_only_query,
            41,
            decision_at,
            actor._latency_profile.as_ref(),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(token_receipt.platform_arrived_at_utc_ms, 1_600);
        let QueryActionResultV2::Trades(TradesQueryResultV2::Page(token_page)) =
            &token_effect.result
        else {
            panic!("未知 token-only Paper 成交查询必须形成合法空页")
        };
        assert!(token_page.items.is_empty());

        let session = state.session.as_ref().expect("Paper token 查询后会话存在");
        assert!(
            session
                .market_enrollment
                .paper_candidate_token_ids
                .contains(&token_only.0)
        );
        assert_eq!(
            session
                .market_enrollment
                .paper_candidate_condition_ids
                .len(),
            1
        );

        let fenced_token_market = market('f', "903", &token_only.0, "206");
        let fenced_token_enrollment = MarketEnrollmentRequestV1 {
            enrollment_request_id: "enroll-fenced-query-token".to_owned(),
            market: fenced_token_market,
        };
        match reduce_business_input_v2(
            &actor.opened,
            actor.scenario.initial_market_directory(),
            state,
            BusinessReducerInputV2::EnrollMarket(&fenced_token_enrollment),
        ) {
            Err(BusinessReductionErrorV2::Unaccepted { error, .. }) => assert_eq!(
                error,
                SimRunActorErrorV2::MarketEnrollmentCandidateAlreadyUsed(token_only.0)
            ),
            Ok(_) | Err(BusinessReductionErrorV2::Fatal(_)) => {
                panic!("Paper token-only 查询必须阻止后来登记复用该 token")
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_root_has_one_control_state_and_physical_retries_do_not_relog() {
        let directory = tempfile::tempdir().expect("创建演员测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut actor = SimRunActorV2::create_new(
            &path,
            opened(
                "run-control-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建唯一运行根演员");

        let created = actor
            .create_session(spec("run-control-v2", "create-a"))
            .expect("创建唯一会话");
        assert_eq!(created.disposition, CreateSessionDispositionV2::NewPrepared);
        assert_eq!(actor.next_input_sequence_for_tests(), Some(2));

        let mut retry = spec("run-control-v2", "create-a");
        retry.run_id = "later-invalid-run".to_owned();
        retry.response_bridge_binding_id.clear();
        let existing = actor.create_session(retry).expect("物理重试返回首次结果");
        assert_eq!(
            existing.disposition,
            CreateSessionDispositionV2::ExistingPrepared
        );
        assert!(Arc::ptr_eq(&created.opening_fact, &existing.opening_fact));
        assert_eq!(actor.next_input_sequence_for_tests(), Some(2));

        assert_eq!(
            actor.activate_session("activate-a".to_owned()),
            Ok(SessionInitializationControlResultV2::Activated)
        );
        assert_eq!(actor.next_input_sequence_for_tests(), Some(3));
        assert_eq!(
            actor.activate_session("activate-a".to_owned()),
            Ok(SessionInitializationControlResultV2::ExistingActive)
        );
        assert_eq!(actor.next_input_sequence_for_tests(), Some(3));
        assert_eq!(
            actor
                .create_session(spec("run-control-v2", "create-b"))
                .expect_err("第二条会话轨迹必须被拒绝"),
            SimRunActorErrorV2::SessionAlreadyCreated
        );
        assert_eq!(
            actor.fail_session_initialization("failure-a".to_owned()),
            Err(SimRunActorErrorV2::InitializationAlreadyDecided)
        );
        assert_eq!(actor.next_input_sequence_for_tests(), Some(3));

        let failure_path = directory.path().join("failure.jsonl");
        let mut failed_closed = SimRunActorV2::create_new(
            &failure_path,
            opened(
                "run-failure-control-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建失败关闭测试根");
        failed_closed
            .create_session(spec("run-failure-control-v2", "create-failure"))
            .expect("创建待失败关闭会话");
        assert_eq!(
            failed_closed.fail_session_initialization("failure-a".to_owned()),
            Ok(SessionInitializationControlResultV2::FailedClosed)
        );
        assert_eq!(failed_closed.next_input_sequence_for_tests(), Some(3));
        assert_eq!(
            failed_closed.fail_session_initialization("failure-a".to_owned()),
            Ok(SessionInitializationControlResultV2::ExistingFailedClosed)
        );
        assert_eq!(failed_closed.next_input_sequence_for_tests(), Some(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unique_session_owns_complete_role_specific_business_state() {
        let directory = tempfile::tempdir().expect("创建完整会话状态测试目录");
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");

        for (index, role, expected_hidden_markets) in [
            (0, SimCallerRoleV1::ModelTrain, 1),
            (1, SimCallerRoleV1::OfflineEvaluation, 1),
            (2, SimCallerRoleV1::Paper, 0),
        ] {
            let run_id = format!("run-complete-state-{index}");
            let path = directory
                .path()
                .join(format!("complete-state-{index}.jsonl"));
            let mut run_opened = opened(
                &run_id,
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            );
            if role == SimCallerRoleV1::Paper {
                run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
            }
            let mut actor = SimRunActorV2::create_new(
                &path,
                run_opened,
                Arc::clone(&rule_snapshot),
                Arc::clone(&finality_policy),
                Arc::clone(&scenario),
                Arc::clone(&latency_profile),
                accepting_projection_port(),
                MAX_GROUP_BYTES,
            )
            .expect("创建角色测试运行根");
            let mut session_spec = spec(&run_id, &format!("create-{index}"));
            session_spec.caller_role = role;
            let created = actor
                .create_session(session_spec)
                .expect("创建完整唯一会话状态");

            let reducer_state = actor.state.as_ref().expect("活动根持有业务归约状态");
            let session = reducer_state.session.as_ref().expect("唯一会话已经创建");
            assert_eq!(session.market_directory.markets().len(), 1);
            assert_eq!(session.public_markets.market_count(), 1);
            assert_eq!(
                session
                    .hidden_platform_markets
                    .as_ref()
                    .map_or(0, MarketStateStoreV2::market_count),
                expected_hidden_markets
            );
            assert_eq!(
                session.execution_state.market_shape_for_tests(),
                (1, 2, 1, 2)
            );
            assert_eq!(session.execution_state.next_account_transition_seq(), 1);
            assert_eq!(
                session.execution_state.account_instance_key(),
                &created.opening_fact.account_instance_key
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_event_slice_routes_each_role_without_double_execution() {
        let events = decoded_initial_market_events("2");
        let condition_id = &events[0].market.condition_id.0;
        let price = "0.4".parse().expect("测试价格");

        for (index, role) in [
            SimCallerRoleV1::ModelTrain,
            SimCallerRoleV1::OfflineEvaluation,
        ]
        .into_iter()
        .enumerate()
        {
            let (_directory, mut actor) =
                active_market_actor(role, &format!("run-market-role-{index}")).await;

            let state = actor.state.take().expect("活动根持有候选前状态");
            let (state, hidden_effects) = reduce_market_event_slice_v2(
                state,
                MarketEventPathV1::HiddenPlatform,
                1_100,
                &events,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            )
            .unwrap_or_else(|_| panic!("训练或离线隐藏路径必须成功"));
            actor.state = Some(state);

            let session = actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("隐藏路径后唯一会话存在");
            assert_eq!(hidden_effects.execution.len(), 2);

            assert_eq!(
                session
                    .hidden_platform_markets
                    .as_ref()
                    .expect("训练或离线会话拥有隐藏状态")
                    .external_quantity_for_tests(condition_id, "1", BookSide::Bid, price),
                Some("2".parse().expect("测试数量"))
            );
            assert_eq!(
                session.public_markets.external_quantity_for_tests(
                    condition_id,
                    "1",
                    BookSide::Bid,
                    price,
                ),
                Some(rust_decimal::Decimal::ZERO)
            );
            assert_eq!(
                session.execution_state.external_queue_base_units_for_tests(
                    condition_id,
                    "1",
                    BookSide::Bid,
                    price,
                ),
                2_000_000
            );

            let state = actor.state.take().expect("公开候选前状态");
            let (state, public_effects) = reduce_market_event_slice_v2(
                state,
                MarketEventPathV1::PublicVisibility,
                1_110,
                &events,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            )
            .unwrap_or_else(|_| panic!("训练或离线公开路径必须成功"));
            actor.state = Some(state);
            let session = actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("公开路径后唯一会话存在");
            assert!(public_effects.execution.is_empty());

            assert_eq!(
                session.public_markets.external_quantity_for_tests(
                    condition_id,
                    "1",
                    BookSide::Bid,
                    price,
                ),
                Some("2".parse().expect("测试数量"))
            );
            assert_eq!(
                session.execution_state.external_queue_base_units_for_tests(
                    condition_id,
                    "1",
                    BookSide::Bid,
                    price,
                ),
                2_000_000,
                "公开可见路径不能把同一证据再撮合一次"
            );
        }

        let (_directory, mut paper) =
            active_market_actor(SimCallerRoleV1::Paper, "run-market-role-paper").await;

        let state = paper.state.take().expect("Paper 隐藏候选前状态");
        let state = match reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &events,
            paper._rule_snapshot.snapshot(),
            paper._finality_policy.as_ref(),
        ) {
            Err(MarketEventSliceReductionErrorV2::Unaccepted { state, error }) => {
                assert_eq!(error, SimRunActorErrorV2::HiddenPlatformReplayForbidden);
                state
            }
            Ok(_) | Err(MarketEventSliceReductionErrorV2::Fatal(_)) => {
                panic!("Paper 隐藏路径必须在状态应用前拒绝")
            }
        };
        paper.state = Some(state);
        let paper_state = paper.state.as_ref().expect("Paper 拒绝后状态仍可用");
        let paper_session = paper_state.session.as_ref().expect("Paper 唯一会话存在");

        assert!(!paper_session.market_enrollment.business_input_started);
        assert_eq!(
            paper_session.public_markets.external_quantity_for_tests(
                condition_id,
                "1",
                BookSide::Bid,
                price,
            ),
            Some(rust_decimal::Decimal::ZERO)
        );

        let state = paper.state.take().expect("Paper 公开候选前状态");
        let (state, effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::PublicVisibility,
            1_110,
            &events,
            paper._rule_snapshot.snapshot(),
            paper._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("Paper 公开路径必须成功"));
        paper.state = Some(state);
        let session = paper
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("Paper 公开路径后唯一会话存在");
        assert_eq!(effects.execution.len(), 2);

        assert_eq!(
            session.public_markets.external_quantity_for_tests(
                condition_id,
                "1",
                BookSide::Bid,
                price,
            ),
            Some("2".parse().expect("测试数量"))
        );
        assert_eq!(
            session.execution_state.external_queue_base_units_for_tests(
                condition_id,
                "1",
                BookSide::Bid,
                price,
            ),
            2_000_000
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn training_and_paper_producers_share_only_the_public_prefix() {
        let (_training_directory, mut training) = active_market_actor(
            SimCallerRoleV1::OfflineEvaluation,
            "run-training-producer-public-prefix",
        )
        .await;
        let (_paper_directory, mut paper) =
            active_market_actor(SimCallerRoleV1::Paper, "run-paper-producer-public-prefix").await;
        let initial: Arc<[ExecutionMarketEventV3]> = Arc::from(decoded_initial_market_events("2"));
        let condition_id = initial[0].market.condition_id.0.clone();
        {
            let mut producer = TrainingReplayTestProducer {
                actor: &mut training,
            };
            producer.push(
                MarketEventPathV1::HiddenPlatform,
                1_100,
                0,
                2,
                1_200,
                Arc::clone(&initial),
            );
            assert_eq!(
                public_replay_witness(producer.actor, &condition_id).0,
                Decimal::ZERO,
                "训练隐藏首桶不得提前改变公开市场"
            );
            producer.push(
                MarketEventPathV1::PublicVisibility,
                1_110,
                0,
                2,
                2_000,
                Arc::clone(&initial),
            );
        }
        {
            let mut producer = PaperVisibilityReplayTestProducer { actor: &mut paper };
            producer.push(PublicVisibilityTestBucket {
                at_utc_ms: 1_110,
                start_index_position: 0,
                next_index_position: 2,
                next_event_at_utc_ms: 2_000,
                events: Arc::clone(&initial),
            });
        }
        let initial_training_public = public_replay_witness(&training, &condition_id);
        let initial_paper_public = public_replay_witness(&paper, &condition_id);
        assert_eq!(initial_training_public, initial_paper_public);
        assert_eq!(initial_training_public.0, "2".parse::<Decimal>().unwrap());
        assert_eq!(initial_training_public.1.next_index_position, 2);

        let update: Arc<[ExecutionMarketEventV3]> =
            Arc::from(vec![decoded_level_market_event(1_200, 2_000, 3, "2", "1")]);

        let paper_input_before_hidden = paper.next_input_sequence_for_tests();
        let paper_public_before_hidden = public_replay_witness(&paper, &condition_id);
        let paper_log_before_hidden = fs::metadata(&paper.log_path)
            .expect("Paper 隐藏拒绝前日志存在")
            .len();
        assert_eq!(
            paper
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        bucket_at_utc_ms: 1_200,
                        start_index_position: 2,
                        next_index_position: 3,
                        closure: MillisecondBucketClosureV1::NextEventAt(2_500),
                    },
                    events: Arc::clone(&update),
                })
                .expect_err("Paper 隐藏路径必须拒绝"),
            SimRunActorErrorV2::HiddenPlatformReplayForbidden
        );

        assert_eq!(
            paper.next_input_sequence_for_tests(),
            paper_input_before_hidden
        );
        assert_eq!(
            fs::metadata(&paper.log_path)
                .expect("Paper 隐藏拒绝后日志存在")
                .len(),
            paper_log_before_hidden
        );
        assert_eq!(
            public_replay_witness(&paper, &condition_id),
            paper_public_before_hidden
        );

        {
            let mut producer = TrainingReplayTestProducer {
                actor: &mut training,
            };
            producer.push(
                MarketEventPathV1::HiddenPlatform,
                1_200,
                2,
                3,
                2_500,
                Arc::clone(&update),
            );
        }
        let training_session = training
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("训练隐藏更新后会话存在");
        assert_eq!(
            training_session
                .hidden_platform_markets
                .as_ref()
                .expect("训练生产器持有隐藏状态")
                .external_quantity_for_tests(
                    &condition_id,
                    "1",
                    BookSide::Bid,
                    "0.4".parse().unwrap(),
                ),
            Some("1".parse().unwrap())
        );
        assert_eq!(
            public_replay_witness(&training, &condition_id).0,
            "2".parse::<Decimal>().unwrap(),
            "隐藏更新后公开市场必须保持旧值"
        );

        for (actor, request_id) in [
            (&mut training, "training-private-before-public"),
            (&mut paper, "paper-private-before-public"),
        ] {
            let session_seed = actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("私有响应测试会话存在")
                .session_seed
                .clone();
            let outbound_delay = i64::try_from(actor._latency_profile.sample_delay_ms(
                session_seed.0.as_bytes(),
                request_id,
                DelaySampleDomainV1::RequestOutbound,
            ))
            .expect("私有响应测试时延可表示");
            actor
                .register_request(
                    request_cutoff(1_500 - outbound_delay),
                    BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                        context: RequestContextV2 {
                            global_request_id: request_id.to_owned(),
                            caller_correlation_id: Some("public-prefix-only".to_owned()),
                        },
                        item: post_item(
                            &format!("{request_id}-item"),
                            &ConditionId(condition_id.clone()),
                            &TokenId("1".to_owned()),
                            SimBuyOrderKindV1::GtcLimit {
                                limit_price_pusd_per_share: "0.4".parse().unwrap(),
                                size_shares: actor
                                    ._rule_snapshot
                                    .synchronized_rules()
                                    .minimum_order_size_shares
                                    .value(),
                                post_only: true,
                            },
                        ),
                    }),
                )
                .unwrap_or_else(|error| panic!("登记私有响应测试请求失败：{error}"));
        }
        for _ in 0..16 {
            if completed_post_response(&training, "training-private-before-public").is_some() {
                break;
            }
            let mut producer = TrainingReplayTestProducer {
                actor: &mut training,
            };
            assert!(
                producer
                    .actor
                    .next_required_logical_drive()
                    .expect("训练响应必要推进存在")
                    .required_at_utc_ms
                    < 2_000
            );
            producer.advance_required(3, 2);
        }
        for _ in 0..16 {
            if completed_post_response(&paper, "paper-private-before-public").is_some() {
                break;
            }
            let mut producer = PaperVisibilityReplayTestProducer { actor: &mut paper };
            assert!(
                producer
                    .actor
                    .next_required_logical_drive()
                    .expect("Paper 响应必要推进存在")
                    .required_at_utc_ms
                    < 2_000
            );
            producer.advance_required(2);
        }
        let training_response =
            completed_post_response(&training, "training-private-before-public")
                .expect("训练私有响应必须在公开更新前形成");
        let paper_response = completed_post_response(&paper, "paper-private-before-public")
            .expect("Paper 私有响应必须在公开更新前形成");
        assert!(training_response.metadata.response_received_at_utc_ms < 2_000);
        assert!(paper_response.metadata.response_received_at_utc_ms < 2_000);
        for witness in [
            public_replay_witness(&training, &condition_id),
            public_replay_witness(&paper, &condition_id),
        ] {
            assert_eq!(witness.0, "2".parse::<Decimal>().unwrap());
            assert_eq!(witness.1.next_index_position, 2);
            assert!(witness.1.closed_through_utc_ms.unwrap_or_default() < 2_000);
        }

        {
            let mut producer = TrainingReplayTestProducer {
                actor: &mut training,
            };
            producer.push(
                MarketEventPathV1::PublicVisibility,
                2_000,
                2,
                3,
                2_500,
                Arc::clone(&update),
            );
        }
        {
            let mut producer = PaperVisibilityReplayTestProducer { actor: &mut paper };
            producer.push(PublicVisibilityTestBucket {
                at_utc_ms: 2_000,
                start_index_position: 2,
                next_index_position: 3,
                next_event_at_utc_ms: 2_500,
                events: update,
            });
        }
        let training_public = public_replay_witness(&training, &condition_id);
        let paper_public = public_replay_witness(&paper, &condition_id);
        assert_eq!(training_public, paper_public);
        assert_eq!(
            training_public.0,
            Decimal::ZERO,
            "公开更新与此前私有订单执行合并后必须得到双方相同的新公开状态"
        );
        assert_eq!(training_public.1.next_index_position, 3);
        assert_eq!(training_public.1.closed_through_utc_ms, Some(2_000));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn end_at_releases_exact_and_execution_books_and_ignores_later_book_events() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-market-end-at").await;
        let initial = decoded_initial_market_events("2");
        let condition_id = initial[0].market.condition_id.0.clone();
        let bid_price = "0.4".parse().expect("测试买价");
        let ask_price = "0.6".parse().expect("测试卖价");

        let state = actor.state.take().expect("初始盘口候选前状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("end_at 测试初始盘口必须成功"));
        actor.state = Some(state);

        let at_end = [decoded_level_market_event(2_000, 2_100, 3, "2", "1")];
        let state = actor.state.take().expect("end_at 候选前状态");
        let (state, effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            2_000,
            &at_end,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("end_at 必须只释放两层外部撮合状态"));
        actor.state = Some(state);
        assert_eq!(effects.execution.len(), 1);

        let reducer_state = actor.state.as_ref().expect("end_at 后状态存在");
        let session = reducer_state
            .session
            .as_ref()
            .expect("end_at 后唯一会话存在");
        let hidden = session
            .hidden_platform_markets
            .as_ref()
            .expect("训练会话拥有隐藏市场状态");
        assert_eq!(hidden.external_level_count_for_tests(&condition_id), 0);
        for (token_id, price, side) in [
            ("1", bid_price, BookSide::Bid),
            ("1", ask_price, BookSide::Ask),
            ("2", bid_price, BookSide::Bid),
            ("2", ask_price, BookSide::Ask),
        ] {
            assert_eq!(
                session.execution_state.external_queue_base_units_for_tests(
                    &condition_id,
                    token_id,
                    side,
                    price,
                ),
                0
            );
        }

        let after_end = [decoded_level_market_event(2_001, 2_101, 4, "1", "0.5")];
        let state = actor.state.take().expect("盘后候选前状态");
        let (state, effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            2_001,
            &after_end,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("盘后外部盘口事件必须无效果消费"));
        actor.state = Some(state);
        assert_eq!(effects.execution.len(), 1);
        let reducer_state = actor.state.as_ref().expect("盘后无效果后状态存在");
        let session = reducer_state.session.as_ref().expect("盘后唯一会话存在");
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .external_level_count_for_tests(&condition_id),
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn redeemed_resolution_marks_both_role_states_and_terminal_books_stay_empty() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-resolution-role-terminal").await;
        let initial = decoded_initial_market_events("2");
        let condition_id = initial[0].market.condition_id.0.clone();

        for (path, platform_effective_at_utc_ms) in [
            (MarketEventPathV1::HiddenPlatform, 1_100),
            (MarketEventPathV1::PublicVisibility, 1_110),
        ] {
            let state = actor.state.take().expect("角色初始盘口候选前状态");
            let (state, _) = reduce_market_event_slice_v2(
                state,
                path,
                platform_effective_at_utc_ms,
                &initial,
                actor._rule_snapshot.snapshot(),
                actor._finality_policy.as_ref(),
            )
            .unwrap_or_else(|_| panic!("隐藏与公开角色初始盘口必须成功"));
            actor.state = Some(state);
        }

        let state = actor.state.take().expect("maker 市场结束候选前状态");
        let (state, maker_end) = advance_rebate_clock_to_candidate(state, 2_000);
        actor.state = Some(state);
        assert!(matches!(
            maker_end.settlement.as_slice(),
            [crate::matching::EconomicLifecycleEffectV2::MakerMarketFinalized(_)]
        ));
        assert!(maker_end.account_deltas.is_empty());

        let resolution = decoded_resolution_market_event(4_000, 4_100, 3);
        let terminal = match &resolution.payload {
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) => terminal.clone(),
            _ => unreachable!("判定测试夹具必须是判定终态事件"),
        };
        let state = actor.state.take().expect("隐藏判定候选前状态");
        let (state, hidden_effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            4_000,
            std::slice::from_ref(&resolution),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("隐藏合格判定必须完成零余额赎回"));
        actor.state = Some(state);
        assert_eq!(hidden_effects.execution.len(), 1);
        let [hidden_execution] = hidden_effects.execution.as_slice() else {
            unreachable!("隐藏合格判定必须形成一个执行效果")
        };
        assert!(matches!(
            hidden_execution.settlement_for_tests(),
            [crate::matching::EconomicLifecycleEffectV2::ResolvedPosition { .. }, crate::matching::EconomicLifecycleEffectV2::AutoRedeem(
                crate::matching::AutoRedeemEffectV2 {
                    redeemed_pusd,
                    account_transition_seq: None,
                    ..
                }
            )] if *redeemed_pusd == rust_decimal::Decimal::ZERO
        ));
        assert_eq!(hidden_execution.account_delta_count_for_tests(), 0);

        let reducer_state = actor.state.as_ref().expect("隐藏判定后状态存在");
        let session = reducer_state.session.as_ref().expect("隐藏判定后会话存在");
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .lifecycle_phase_for_tests(&condition_id),
            Some(crate::market::MarketLifecyclePhaseV1::ResolutionTerminal)
        );
        assert_eq!(
            session
                .public_markets
                .lifecycle_phase_for_tests(&condition_id),
            Some(crate::market::MarketLifecyclePhaseV1::Active)
        );
        assert_eq!(
            session
                .execution_state
                .redeemed_market_resolution(&condition_id),
            Some(&terminal)
        );
        assert_eq!(session.execution_state.next_account_transition_seq(), 1);

        let state = actor.state.take().expect("公开判定候选前状态");
        let (state, public_effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::PublicVisibility,
            4_100,
            std::slice::from_ref(&resolution),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("公开路径必须同步既有赎回终态"));
        actor.state = Some(state);
        assert!(public_effects.execution.is_empty());

        let reducer_state = actor.state.as_ref().expect("公开判定后状态存在");
        let session = reducer_state.session.as_ref().expect("公开判定后会话存在");
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .lifecycle_phase_for_tests(&condition_id),
            Some(crate::market::MarketLifecyclePhaseV1::ResolutionTerminal)
        );
        assert_eq!(
            session
                .public_markets
                .lifecycle_phase_for_tests(&condition_id),
            Some(crate::market::MarketLifecyclePhaseV1::ResolutionTerminal)
        );
        assert_eq!(session.execution_state.next_account_transition_seq(), 1);

        let state = actor.state.take().expect("公开判定重到候选前状态");
        let (state, repeated) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::PublicVisibility,
            4_100,
            std::slice::from_ref(&resolution),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("公开判定重到必须是规范无效果"));
        actor.state = Some(state);
        assert!(repeated.execution.is_empty());

        let scenario = crate::scenario::scenario_for_sim_tests();
        let member = &scenario.market_set().markets()[0];
        let late_initial = decoded_initial_event_for_market(member, 4_200, 4_210, 4);
        let state = actor.state.take().expect("隐藏终态初始盘口候选前状态");
        let (state, hidden_terminal_effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            4_200,
            std::slice::from_ref(&late_initial),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("隐藏终态后的初始盘口必须无效果"));
        actor.state = Some(state);
        assert_eq!(
            hidden_terminal_effects.execution.as_slice(),
            &[ExecutionMarketEventEffectsV2::default()]
        );

        let late_level = decoded_maker_level_event(BookSide::Bid, "0.3", "9", "8", 4_200, 4_210, 5);
        let state = actor.state.take().expect("公开终态档位候选前状态");
        let (state, public_terminal_effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::PublicVisibility,
            4_210,
            std::slice::from_ref(&late_level),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("公开终态后的错误前驱档位必须无效果"));
        actor.state = Some(state);
        assert!(public_terminal_effects.execution.is_empty());

        let reducer_state = actor.state.as_ref().expect("终态盘口后状态存在");
        let session = reducer_state.session.as_ref().expect("终态盘口后会话存在");

        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .external_level_count_for_tests(&condition_id),
            0
        );
        assert_eq!(
            session
                .public_markets
                .external_level_count_for_tests(&condition_id),
            0
        );
        assert_eq!(
            session.execution_state.external_queue_base_units_for_tests(
                &condition_id,
                "1",
                BookSide::Bid,
                "0.3".parse().expect("终态盘口测试价格"),
            ),
            0
        );
        assert_eq!(session.execution_state.next_account_transition_seq(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_session_candidate_routes_real_buy_debt_and_first_positive_maker_fill_once() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-market-maker").await;
        let initial = decoded_maker_initial_events();
        let condition_id = initial[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());

        let state = actor.state.take().expect("maker 初始盘口候选前状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("maker 初始盘口必须成功"));

        let (state, accepted) = apply_buy_to_candidate(
            state,
            BuyOrderActionV2 {
                global_request_id: "run-v2-maker-request".to_owned(),
                batch_item_ordinal: 0,
                item_id: "run-v2-maker-item".to_owned(),
                condition_id: condition_id.clone(),
                token_id: token_id.clone(),
                effective_processing_at_utc_ms: 1_200,
                terms: BuyOrderTermsV2::Gtc {
                    limit_price_pusd_per_share: "0.3".parse().expect("maker 限价"),
                    accepted_base_units: 4_000_000,
                    accepted_quote_units: 1_200_000,
                    post_only: false,
                },
            },
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(accepted.initial_finality_count_for_tests(), 1);
        assert_eq!(accepted.account_delta_count_for_tests(), 1);

        actor.state = Some(state);

        let session = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("BUY 后唯一会话存在");
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests("sim-1"),
            Some((1_000_001, 0, 2_999_999, 200_000, 900_000))
        );
        assert_eq!(session.execution_state.fill_shape_for_tests(), (1, 2));
        assert_eq!(
            session.execution_state.account_overlay_quantity_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                "0.3".parse().expect("maker 叠加价位"),
            ),
            "2.999999".parse().expect("maker 叠加数量")
        );
        let balances_before_maker = session.execution_state.account_balances_for_tests();
        assert_eq!(balances_before_maker.1, "1.1".parse().expect("本金占用"));

        let mut maker_events = vec![decoded_maker_level_event(
            BookSide::Ask,
            "0.2",
            "1.000001",
            "0",
            1_300,
            1_310,
            10,
        )];
        for cycle in 0..3_u64 {
            maker_events.push(decoded_maker_level_event(
                BookSide::Bid,
                "0.3",
                "0",
                "0.000001",
                1_300,
                1_311 + cycle as i64 * 2,
                11 + cycle * 2,
            ));
            maker_events.push(decoded_maker_level_event(
                BookSide::Bid,
                "0.3",
                "0.000001",
                "0",
                1_300,
                1_312 + cycle as i64 * 2,
                12 + cycle * 2,
            ));
        }

        let state = actor.state.take().expect("maker 切片候选前状态");
        let (state, effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_300,
            &maker_events,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("同一真实解码 maker 切片必须成功"));
        actor.state = Some(state);

        assert_eq!(effects.execution.len(), maker_events.len());
        assert_eq!(
            effects
                .execution
                .iter()
                .map(ExecutionMarketEventEffectsV2::initial_finality_count_for_tests)
                .sum::<usize>(),
            1
        );
        assert_eq!(
            effects
                .execution
                .iter()
                .flat_map(ExecutionMarketEventEffectsV2::initial_finality_for_tests)
                .map(|effect| effect.fill_sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        let lifecycle = effects
            .execution
            .into_iter()
            .map(ExecutionMarketEventEffectsV2::into_lifecycle_feedback)
            .collect::<Result<Vec<_>, _>>()
            .expect("maker 事件切片必须形成可交付生命周期效果")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let [LifecycleFeedbackEffectV2::Trade(maker_trade)] = lifecycle.as_slice() else {
            panic!("首次正对价 maker 穿越只能形成一个成交首态效果")
        };
        assert_eq!(maker_trade.trade_id, "sim-2");
        assert_eq!(maker_trade.fill_sequence, 2);
        assert_eq!(maker_trade.matched_at_utc_ms, 1_300);
        assert_eq!(
            maker_trade.liquidity_role,
            contracts::TradeLiquidityRoleV2::MakerInferred
        );
        assert_eq!(maker_trade.base_quantity_shares.0, "0.000003");
        assert_eq!(maker_trade.quote_amount_pusd.0, "0.000001");
        assert!(maker_trade.account_delta.is_none());

        let reducer_state = actor.state.as_ref().expect("maker 切片后状态存在");

        let session = reducer_state
            .session
            .as_ref()
            .expect("maker 切片后唯一会话存在");
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .external_level_count_for_tests(&condition_id.0),
            3
        );
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .external_quantity_for_tests(
                    &condition_id.0,
                    &token_id.0,
                    BookSide::Ask,
                    "0.2".parse().expect("taker 债务价位"),
                ),
            Some(rust_decimal::Decimal::ZERO)
        );
        assert_eq!(
            session.execution_state.external_queue_base_units_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Ask,
                "0.2".parse().expect("taker 债务价位"),
            ),
            0
        );
        assert_eq!(
            session.execution_state.external_queue_base_units_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                "0.3".parse().expect("maker 价位"),
            ),
            3
        );
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests("sim-1"),
            Some((1_000_004, 0, 2_999_996, 200_001, 899_999))
        );
        assert_eq!(session.execution_state.fill_shape_for_tests(), (2, 3));
        assert_eq!(
            session.execution_state.account_overlay_quantity_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                "0.3".parse().expect("maker 叠加价位"),
            ),
            "2.999996".parse().expect("maker 叠加数量")
        );
        assert_eq!(
            session.execution_state.account_balances_for_tests(),
            balances_before_maker,
            "ordinal 0 只形成最终性首态，尚未到经济应用 ordinal"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_session_maker_level_depletion_terminalizes_real_venue_dust() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-market-maker-dust").await;
        let initial = decoded_initial_market_events("10");
        let condition_id = initial[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());
        let price = "0.4".parse().expect("maker 小额余量价位");

        let state = actor
            .state
            .take()
            .expect("maker 小额余量初始盘口候选前状态");
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_100,
            &initial,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("maker 小额余量初始盘口必须成功"));
        let better_ask =
            decoded_maker_level_event(BookSide::Ask, "0.3", "0", "4.999", 1_101, 1_111, 3);
        let (state, _) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_101,
            std::slice::from_ref(&better_ask),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("真实价优 ask 必须先进入同一会话候选"));

        let global_request_id = "run-v2-maker-dust-request".to_owned();
        let item_id = "run-v2-maker-dust-item".to_owned();
        let expires_at_utc_ms = 300_000;
        let (state, accepted) = apply_buy_to_candidate(
            state,
            BuyOrderActionV2 {
                global_request_id: global_request_id.clone(),
                batch_item_ordinal: 0,
                item_id: item_id.clone(),
                condition_id: condition_id.clone(),
                token_id: token_id.clone(),
                effective_processing_at_utc_ms: 1_200,
                terms: BuyOrderTermsV2::Gtd {
                    limit_price_pusd_per_share: price,
                    accepted_base_units: 5_000_000,
                    accepted_quote_units: 2_000_000,
                    post_only: false,
                    expires_at_utc_ms: Some(expires_at_utc_ms),
                },
            },
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        );
        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(accepted.account_delta_count_for_tests(), 1);

        actor.state = Some(state);

        let session = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("maker 小额余量 BUY 后唯一会话存在");
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests("sim-1"),
            Some((4_999_000, 0, 1_000, 1_499_700, 400))
        );
        assert_eq!(
            session.execution_state.base_order_dust_for_tests("sim-1"),
            Some((0, 0, false))
        );
        assert!(
            session
                .execution_state
                .has_gtd_deadline_for_tests(expires_at_utc_ms, "sim-1")
        );
        assert_eq!(
            session.execution_state.account_overlay_quantity_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                price,
            ),
            "0.001".parse().expect("maker 小额余量叠加数量")
        );
        let balances_before_dust = session.execution_state.account_balances_for_tests();
        let fee_before_dust = session
            .execution_state
            .immediate_taker_fee_encumbrance_for_tests();
        assert_eq!(
            balances_before_dust.1,
            "1.5001".parse().expect("dust 前本金占用")
        );

        let event = decoded_maker_level_event(BookSide::Bid, "0.4", "10", "0", 1_300, 1_310, 20);
        let state = actor.state.take().expect("maker 小额余量档位候选前状态");
        let (state, effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            1_300,
            std::slice::from_ref(&event),
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("maker 档位减量必须原子关闭平台小额余量"));
        actor.state = Some(state);

        assert_eq!(effects.execution.len(), 1);
        assert_eq!(
            effects.execution[0].venue_dust_order_id_for_tests(),
            Some("sim-1")
        );
        assert_eq!(effects.execution[0].account_delta_count_for_tests(), 1);
        assert_eq!(effects.execution[0].initial_finality_count_for_tests(), 0);
        let lifecycle = effects
            .execution
            .into_iter()
            .next()
            .expect("maker 小额余量必须形成一个执行效果")
            .into_lifecycle_feedback()
            .expect("maker 小额余量必须形成订单生命周期效果");
        let [LifecycleFeedbackEffectV2::Order(dust_lifecycle)] = lifecycle.as_slice() else {
            panic!("maker 小额余量只能形成一个订单终态效果")
        };
        let contracts::OrderLifecycleEventV2::VenueDustTerminal {
            closed_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } = &dust_lifecycle.event
        else {
            panic!("maker 小额余量必须冻结完整订单终态事实")
        };
        assert_eq!(closed_base_quantity_shares.0, "0.001");
        assert_eq!(released_quote_amount_pusd.0, "0.0004");
        assert!(account_delta.is_some());

        let reducer_state = actor.state.as_ref().expect("maker 小额余量关闭后状态存在");

        let session = reducer_state
            .session
            .as_ref()
            .expect("maker 小额余量关闭后唯一会话存在");
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .external_quantity_for_tests(&condition_id.0, &token_id.0, BookSide::Bid, price,),
            Some(rust_decimal::Decimal::ZERO)
        );
        assert_eq!(
            session.execution_state.external_queue_base_units_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                price,
            ),
            0
        );
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests("sim-1"),
            Some((4_999_000, 0, 0, 1_499_700, 0))
        );
        assert_eq!(
            session.execution_state.base_order_dust_for_tests("sim-1"),
            Some((1_000, 400, true))
        );
        assert!(
            !session
                .execution_state
                .has_gtd_deadline_for_tests(expires_at_utc_ms, "sim-1")
        );
        assert_eq!(
            session.execution_state.account_overlay_quantity_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                price,
            ),
            rust_decimal::Decimal::ZERO
        );
        let balances_after_dust = session.execution_state.account_balances_for_tests();
        assert_eq!(
            balances_after_dust.0,
            balances_before_dust.0
                + "0.0004"
                    .parse::<rust_decimal::Decimal>()
                    .expect("dust 释放金额")
        );
        assert_eq!(
            balances_after_dust.1,
            "1.4997".parse().expect("dust 后本金占用")
        );
        assert_eq!(
            session
                .execution_state
                .immediate_taker_fee_encumbrance_for_tests(),
            fee_before_dust,
            "maker 小额余量只能释放开放本金，不能提前释放 taker 手续费"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unknown_market_slice_is_atomic_and_retries_after_real_enrollment() {
        let (_directory, mut actor) =
            active_market_actor(SimCallerRoleV1::ModelTrain, "run-market-retry").await;
        let added_market = market('2', "456", "3", "4");
        let events = vec![
            decoded_initial_market_events("2").remove(0),
            decoded_initial_event_for_market(&added_market, 2_100, 2_110, 20),
        ];
        let initial_condition_id = events[0].market.condition_id.0.clone();
        let added_condition_id = added_market.market().condition_id.0.clone();
        let price = "0.4".parse().expect("测试价格");

        let before_sequence = actor.next_input_sequence_for_tests();

        let state = actor.state.take().expect("未登记候选前状态");
        let state = match reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            2_100,
            &events,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        ) {
            Err(MarketEventSliceReductionErrorV2::Unaccepted { state, error }) => {
                assert_eq!(
                    error,
                    SimRunActorErrorV2::MarketEvent(MarketEventErrorV1::UnknownMarket(
                        added_condition_id.clone(),
                    ))
                );
                state
            }
            Ok(_) | Err(MarketEventSliceReductionErrorV2::Fatal(_)) => {
                panic!("切片中的未登记市场必须让整个候选零生效")
            }
        };
        actor.state = Some(state);
        let reducer_state = actor.state.as_ref().expect("未登记失败后状态仍可用");
        let session = reducer_state.session.as_ref().expect("唯一会话存在");

        assert!(!session.market_enrollment.business_input_started);
        assert_eq!(actor.next_input_sequence_for_tests(), before_sequence);
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏状态")
                .external_quantity_for_tests(&initial_condition_id, "1", BookSide::Bid, price),
            Some(rust_decimal::Decimal::ZERO),
            "切片第二项失败时第一项也不能留下状态"
        );

        actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-before-retry".to_owned(),
                market: added_market.clone(),
            })
            .expect("未登记失败不冻结训练目录");
        let sequence_after_enrollment = actor.next_input_sequence_for_tests();
        let state = actor.state.take().expect("登记后重试候选前状态");
        let (state, effects) = reduce_market_event_slice_v2(
            state,
            MarketEventPathV1::HiddenPlatform,
            2_100,
            &events,
            actor._rule_snapshot.snapshot(),
            actor._finality_policy.as_ref(),
        )
        .unwrap_or_else(|_| panic!("登记后同一已解码切片必须成功"));
        actor.state = Some(state);
        assert_eq!(effects.execution.len(), 2);
        assert_eq!(
            actor.next_input_sequence_for_tests(),
            sequence_after_enrollment
        );
        let session = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("重试成功后唯一会话存在");
        assert!(session.market_enrollment.business_input_started);
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏状态")
                .external_quantity_for_tests(&added_condition_id, "3", BookSide::Bid, price),
            Some("2".parse().expect("测试数量"))
        );
        assert_eq!(
            actor.enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "frozen-after-market-input".to_owned(),
                market: market('3', "789", "5", "6"),
            }),
            Err(SimRunActorErrorV2::MarketEnrollmentFrozenAfterBusinessInput)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enrollment_extends_every_owned_container_once_and_reuses_the_success() {
        let directory = tempfile::tempdir().expect("创建市场登记测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut actor = SimRunActorV2::create_new(
            &path,
            opened(
                "run-enrollment-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建市场登记运行根");
        actor
            .create_session(spec("run-enrollment-v2", "create-enrollment"))
            .expect("创建市场登记会话");
        let added_market = market('2', "456", "3", "4");
        let enrollment_request_id = "r".repeat(257);

        assert_eq!(
            actor.enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: enrollment_request_id.clone(),
                market: added_market.clone(),
            }),
            Err(SimRunActorErrorV2::SessionNotActive)
        );
        assert_eq!(actor.next_input_sequence_for_tests(), Some(2));

        actor
            .activate_session("activate-enrollment".to_owned())
            .expect("激活市场登记会话");

        reset_reducer_call_count_for_tests();
        let enrolled = actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: enrollment_request_id.clone(),
                market: added_market.clone(),
            })
            .expect("登记第二个市场");
        assert_eq!(reducer_call_count_for_tests(), 1);
        assert_eq!(enrolled.enrollment_sequence, 1);
        assert_eq!(enrolled.market, *added_market.market());
        assert_eq!(enrolled.assets, *added_market.assets());
        assert_eq!(actor.next_input_sequence_for_tests(), Some(4));

        let reducer_state = actor.state.as_ref().expect("活动根持有业务状态");
        let session = reducer_state.session.as_ref().expect("唯一会话存在");
        assert_eq!(session.market_directory.markets().len(), 2);
        assert!(
            session
                .public_markets
                .contains_market(&added_market.market().condition_id.0)
        );
        assert!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .contains_market(&added_market.market().condition_id.0)
        );
        assert_eq!(
            session.execution_state.market_shape_for_tests(),
            (2, 4, 2, 4)
        );
        assert_eq!(session.execution_state.next_account_transition_seq(), 1);
        assert_eq!(session.market_enrollment.records.len(), 1);
        assert_eq!(session.market_enrollment.records_by_request_id.len(), 1);

        let retried = actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id,
                market: market('3', "789", "5", "6"),
            })
            .expect("成功登记的物理重投返回原结果");
        assert_eq!(retried, enrolled);
        assert_eq!(reducer_call_count_for_tests(), 1);
        assert_eq!(actor.next_input_sequence_for_tests(), Some(4));
        let session = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("物理重投后唯一会话仍存在");
        assert_eq!(session.market_enrollment.records.len(), 1);
        assert_eq!(session.market_enrollment.records_by_request_id.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_enrollment_extends_public_and_execution_state_without_hidden_state() {
        let directory = tempfile::tempdir().expect("创建 Paper 市场登记测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut paper_opened = opened(
            "run-paper-enrollment-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        paper_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut actor = SimRunActorV2::create_new(
            &path,
            paper_opened,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 市场登记运行根");
        let mut paper_spec = spec("run-paper-enrollment-v2", "create-paper-enrollment");
        paper_spec.caller_role = SimCallerRoleV1::Paper;
        actor
            .create_session(paper_spec)
            .expect("创建 Paper 唯一会话");
        actor
            .activate_session("activate-paper-enrollment".to_owned())
            .expect("激活 Paper 唯一会话");

        let added_market = market('2', "456", "3", "4");
        actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-paper-market-2".to_owned(),
                market: added_market.clone(),
            })
            .expect("Paper 追加第二个市场");

        let reducer_state = actor.state.as_ref().expect("Paper 根持有业务状态");
        let session = reducer_state.session.as_ref().expect("Paper 唯一会话存在");
        assert_eq!(session.market_directory.markets().len(), 2);
        assert_eq!(session.public_markets.market_count(), 2);
        assert!(session.hidden_platform_markets.is_none());
        assert_eq!(
            session.execution_state.market_shape_for_tests(),
            (2, 4, 2, 4)
        );
        assert_eq!(session.market_enrollment.records.len(), 1);
        assert_eq!(session.market_enrollment.records_by_request_id.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_enrollment_changes_no_container_and_does_not_bind_the_request_id() {
        let directory = tempfile::tempdir().expect("创建登记失败原子性测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut actor = SimRunActorV2::create_new(
            &path,
            opened(
                "run-enrollment-rejection-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建登记失败测试根");
        actor
            .create_session(spec(
                "run-enrollment-rejection-v2",
                "create-enrollment-rejection",
            ))
            .expect("创建登记失败测试会话");
        actor
            .activate_session("activate-enrollment-rejection".to_owned())
            .expect("激活登记失败测试会话");

        let before_sequence = actor.next_input_sequence_for_tests();
        assert!(matches!(
            actor.enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "reusable-after-rejection".to_owned(),
                market: market('2', "456", "1", "4"),
            }),
            Err(SimRunActorErrorV2::MarketEnrollmentDirectory(_))
        ));

        let reducer_state = actor.state.as_ref().expect("失败后仍有原业务状态");
        let session = reducer_state
            .session
            .as_ref()
            .expect("失败后唯一会话仍存在");

        assert_eq!(session.market_directory.markets().len(), 1);
        assert_eq!(session.public_markets.market_count(), 1);
        assert_eq!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话拥有隐藏市场状态")
                .market_count(),
            1
        );
        assert_eq!(
            session.execution_state.market_shape_for_tests(),
            (1, 2, 1, 2)
        );
        assert_eq!(session.execution_state.next_account_transition_seq(), 1);
        assert!(session.market_enrollment.records.is_empty());
        assert!(session.market_enrollment.records_by_request_id.is_empty());
        assert_eq!(actor.next_input_sequence_for_tests(), before_sequence);

        let accepted = actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "reusable-after-rejection".to_owned(),
                market: market('2', "456", "3", "4"),
            })
            .expect("未受理失败不永久占用登记身份");
        assert_eq!(accepted.enrollment_sequence, 1);
        assert_eq!(actor.next_input_sequence_for_tests(), Some(4));
    }

    #[tokio::test(flavor = "current_thread")]
    #[cfg(feature = "measurement")]
    async fn measurement_probe_observes_public_commit_and_current_resources() {
        let directory = tempfile::tempdir().expect("创建测量探针测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut execution = SimExecution::create_new(
            &path,
            opened(
                "run-measurement-probe-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建测量探针测试根");
        execution.drain_measurement_observations();

        execution
            .create_session(spec("run-measurement-probe-v2", "create-measurement-probe"))
            .expect("公开创建测量探针测试会话");

        let observations = execution.drain_measurement_observations();
        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.phase)
                .collect::<Vec<_>>(),
            vec![
                crate::measurement_probe::MeasurementPhase::Reducer,
                crate::measurement_probe::MeasurementPhase::ProjectionFinalize,
                crate::measurement_probe::MeasurementPhase::LogEncode,
                crate::measurement_probe::MeasurementPhase::LogWrite,
                crate::measurement_probe::MeasurementPhase::ProjectionDelivery,
            ]
        );
        assert!(observations.iter().all(|observation| {
            observation.context == crate::measurement_probe::MeasurementContext::CommitGroup
        }));
        let resources = execution.measurement_resource_snapshot();
        assert!(resources.business.records > 0);
        assert_eq!(resources.pending_projection, ResourceQuantityV1::default());
        assert_eq!(
            resources.log_byte_count,
            std::fs::metadata(path).unwrap().len()
        );
        assert_eq!(resources.next_input_sequence, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    #[cfg(feature = "measurement")]
    async fn measurement_probe_keeps_clean_seal_separate_from_recovery() {
        let directory = tempfile::tempdir().expect("创建测量封口测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut execution = SimExecution::create_new(
            &path,
            opened(
                "run-measurement-seal-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建测量封口根");
        execution
            .create_session(spec("run-measurement-seal-v2", "create-measurement-seal"))
            .expect("创建测量封口会话");
        execution
            .fail_session_initialization("failed-measurement-seal".to_owned())
            .expect("关闭测量封口会话");
        execution.drain_measurement_observations();

        let candidate = execution.complete_clean_run().expect("形成测量冷候选");
        let observations = candidate.measurement_observations();
        assert!(observations.iter().any(|observation| {
            observation.context == crate::measurement_probe::MeasurementContext::CleanSeal
                && observation.phase == crate::measurement_probe::MeasurementPhase::LogSync
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_facade_routes_control_enrollment_and_all_nine_methods_to_one_actor() {
        let directory = tempfile::tempdir().expect("创建公开综合轨迹目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut execution = SimExecution::create_new(
            &path,
            opened(
                "run-public-golden-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建公开综合轨迹根");
        let session_spec = spec("run-public-golden-v2", "create-public-golden");
        assert_eq!(
            execution
                .create_session(session_spec.clone())
                .expect("公开创建会话")
                .disposition,
            CreateSessionDispositionV2::NewPrepared
        );
        assert_eq!(
            execution
                .create_session(session_spec)
                .expect("公开物理重试创建")
                .disposition,
            CreateSessionDispositionV2::ExistingPrepared
        );
        execution
            .activate_session("activate-public-golden".to_owned())
            .expect("公开激活会话");
        let enrolled_market = market('2', "456", "3", "4");
        execution
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-public-golden".to_owned(),
                market: enrolled_market.clone(),
            })
            .expect("公开登记市场");

        let condition_id = enrolled_market.market().condition_id.clone();
        let token_id = enrolled_market.assets()[0].token_id.clone();
        let context = |id: &str| RequestContextV2 {
            global_request_id: id.to_owned(),
            caller_correlation_id: Some(format!("correlation-{id}")),
        };
        let cutoff = request_cutoff(1_200);
        let post = |id: &str| {
            post_item(
                id,
                &condition_id,
                &token_id,
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.3".parse().expect("公开轨迹价格"),
                    size_shares: "5".parse().expect("公开轨迹数量"),
                    post_only: false,
                },
            )
        };
        let receipts = [
            execution
                .post_order(
                    cutoff.clone(),
                    PostOrderRequestV2 {
                        context: context("public-post-order"),
                        item: post("public-post-order-item"),
                    },
                )
                .expect("公开 post_order"),
            execution
                .post_orders(
                    cutoff.clone(),
                    PostOrdersRequestV2 {
                        context: context("public-post-orders"),
                        items: vec![PostOrderBatchItemV2 {
                            batch_item_ordinal: 0,
                            item: post("public-post-orders-item"),
                        }],
                    },
                )
                .expect("公开 post_orders"),
            execution
                .cancel_order(
                    cutoff.clone(),
                    CancelOrderRequestV2 {
                        context: context("public-cancel-order"),
                        item: CancelOrderItemV1 {
                            item_id: "public-cancel-order-item".to_owned(),
                            platform_order_id: "sim-1".to_owned(),
                        },
                    },
                )
                .expect("公开 cancel_order"),
            execution
                .cancel_orders(
                    cutoff.clone(),
                    CancelOrdersRequestV2 {
                        context: context("public-cancel-orders"),
                        items: vec![CancelOrderBatchItemV2 {
                            batch_item_ordinal: 0,
                            item: CancelOrderItemV1 {
                                item_id: "public-cancel-orders-item".to_owned(),
                                platform_order_id: "sim-2".to_owned(),
                            },
                        }],
                    },
                )
                .expect("公开 cancel_orders"),
            execution
                .cancel_market_orders(
                    cutoff.clone(),
                    CancelMarketOrdersRequestV2 {
                        context: context("public-cancel-market-orders"),
                        condition_id: condition_id.clone(),
                    },
                )
                .expect("公开 cancel_market_orders"),
            execution
                .cancel_all_orders(
                    cutoff.clone(),
                    CancelAllOrdersRequestV2 {
                        context: context("public-cancel-all-orders"),
                    },
                )
                .expect("公开 cancel_all_orders"),
            execution
                .order(
                    cutoff.clone(),
                    OrderRequestV2 {
                        context: context("public-order"),
                        platform_order_id: "sim-1".to_owned(),
                    },
                )
                .expect("公开 order"),
            execution
                .orders(
                    cutoff.clone(),
                    OrdersRequestV2 {
                        context: context("public-orders"),
                        filter: OrdersFilterV1::default(),
                        cursor: None,
                    },
                )
                .expect("公开 orders"),
            execution
                .trades(
                    cutoff.clone(),
                    TradesRequestV2 {
                        context: context("public-trades"),
                        filter: TradesFilterV1::default(),
                        cursor: None,
                    },
                )
                .expect("公开 trades"),
        ];
        assert_eq!(
            receipts
                .iter()
                .map(MutationRequestReceiptV2::input_sequence)
                .collect::<Vec<_>>(),
            (4..=12).collect::<Vec<_>>()
        );
        assert!(
            receipts
                .iter()
                .all(|receipt| receipt.scheduled_action_count() == 1)
        );
        assert_eq!(
            execution
                .cancel_market_orders(
                    cutoff,
                    CancelMarketOrdersRequestV2 {
                        context: context("public-cancel-market-orders"),
                        condition_id: ConditionId("different-on-retry".to_owned()),
                    },
                )
                .expect("公开 cancel_market_orders 物理重投"),
            receipts[4],
            "同一全局请求身份重投不得登记第二个市场撤单动作"
        );

        drop(execution);
        let (_, mut recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开公开综合轨迹日志");
        let mut groups = 0_u64;
        loop {
            let (next, group) = recovery.next_group().expect("读取公开综合轨迹组");
            recovery = next;
            if group.is_none() {
                break;
            }
            groups += 1;
        }
        assert_eq!(groups, 12, "公开入口必须各自只追加一个完整组");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enrollment_log_failure_publishes_no_candidate_and_leaves_no_complete_input() {
        let directory = tempfile::tempdir().expect("创建登记日志失败测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut actor = SimRunActorV2::create_new(
            &path,
            opened(
                "run-enrollment-log-failure-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建登记日志失败测试根");
        actor
            .create_session(spec(
                "run-enrollment-log-failure-v2",
                "create-enrollment-log-failure",
            ))
            .expect("创建登记日志失败测试会话");
        actor
            .activate_session("activate-enrollment-log-failure".to_owned())
            .expect("激活登记日志失败测试会话");

        actor.inject_log_write_failure_for_tests();
        assert!(matches!(
            actor.enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-log-failure".to_owned(),
                market: market('2', "456", "3", "4"),
            }),
            Err(SimRunActorErrorV2::Log(_))
        ));
        assert_eq!(
            actor
                .enroll_market(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "enroll-log-failure".to_owned(),
                    market: market('2', "456", "3", "4"),
                })
                .expect_err("日志失败后的运行根必须关闭"),
            SimRunActorErrorV2::Closed
        );
        drop(actor);

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开登记失败日志");
        let (recovery, first) = recovery.next_group().expect("读取创建组");
        assert!(matches!(
            first.as_deref(),
            Some([LoggedBusinessInputV2::CreateSession(_)])
        ));
        let (recovery, second) = recovery.next_group().expect("读取激活组");
        assert!(matches!(
            second.as_deref(),
            Some([LoggedBusinessInputV2::ActivateSession { .. }])
        ));
        let (_, third) = recovery.next_group().expect("读取登记失败后的日志末尾");
        assert!(third.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn log_failure_drops_detached_control_state_and_closes_only_that_root() {
        let directory = tempfile::tempdir().expect("创建演员测试目录");
        let failed_path = directory.path().join("failed.jsonl");
        let healthy_path = directory.path().join("healthy.jsonl");
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut failed = SimRunActorV2::create_new(
            &failed_path,
            opened(
                "run-failed-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建待故障演员");
        let mut healthy = SimRunActorV2::create_new(
            &healthy_path,
            opened(
                "run-healthy-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建独立演员");

        reset_group_commit_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        failed.inject_log_write_failure_for_tests();
        assert!(matches!(
            failed.commit_group(vec![
                BusinessInputV2::CreateSession(spec("run-failed-v2", "failed-create")),
                BusinessInputV2::ActivateSession {
                    activation_id: "failed-activate".to_owned(),
                },
            ]),
            Err(SimRunActorErrorV2::Log(_))
        ));
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 0));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 2));
        assert_eq!(
            failed
                .create_session(spec("run-failed-v2", "failed-retry"))
                .expect_err("日志失败后的运行根必须保持关闭"),
            SimRunActorErrorV2::Closed
        );

        assert_eq!(
            healthy
                .create_session(spec("run-healthy-v2", "healthy-create"))
                .expect("另一根继续创建")
                .disposition,
            CreateSessionDispositionV2::NewPrepared
        );
        assert_eq!(healthy.next_input_sequence_for_tests(), Some(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_write_failure_reduces_once_installs_nothing_and_closes_the_root() {
        let directory = tempfile::tempdir().expect("创建公开写失败测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut execution = SimExecution::create_new(
            &path,
            opened(
                "run-public-write-failure-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建公开写失败根");

        reset_group_commit_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        execution.inject_log_write_failure_for_tests();
        assert!(matches!(
            execution.create_session(spec(
                "run-public-write-failure-v2",
                "create-public-write-failure",
            )),
            Err(SimRunActorErrorV2::Log(_))
        ));
        assert_eq!(group_commit_counts_for_tests(), (1, 1, 0));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 1));
        assert_eq!(
            execution
                .create_session(spec(
                    "run-public-write-failure-v2",
                    "retry-after-public-write-failure",
                ))
                .expect_err("公开日志写失败后运行根必须关闭"),
            SimRunActorErrorV2::Closed
        );
        drop(execution);

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开公开写失败日志");
        let (_, first) = recovery.next_group().expect("读取公开写失败日志末尾");
        assert!(first.is_none(), "公开写失败不得留下完整受理输入");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_paper_sync_unknown_recovers_actual_group_and_reattaches_the_unique_session() {
        let directory = tempfile::tempdir().expect("创建公开同步结果未知测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut paper_opened = opened(
            "run-public-sync-unknown-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        paper_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut execution = SimExecution::create_new(
            &path,
            paper_opened,
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建公开 Paper 根");

        reset_group_commit_counts_for_tests();
        crate::event_log::reset_event_log_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        execution.inject_log_sync_failure_for_tests();
        let session_spec = spec("run-public-sync-unknown-v2", "create-public-sync-unknown");
        assert!(matches!(
            execution.create_session(session_spec.clone()),
            Err(SimRunActorErrorV2::Log(_))
        ));
        assert_eq!(group_commit_counts_for_tests(), (1, 1, 0));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 1));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (1, 0));
        assert_eq!(crate::event_log::sync_attempt_count_for_tests(), 1);
        assert_eq!(
            execution
                .create_session(session_spec.clone())
                .expect_err("同步结果未知后当前公开根必须关闭"),
            SimRunActorErrorV2::Closed
        );
        drop(execution);

        let mut recovered = SimExecution::recover_paper(
            &path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
            |_| panic!("公开创建组恢复不得读取市场桶"),
        )
        .expect("公开恢复按实际完整组裁决");
        assert_eq!(
            recovered
                .create_session(session_spec)
                .expect("公开重附着稳定会话")
                .disposition,
            CreateSessionDispositionV2::ExistingPrepared
        );
        recovered
            .activate_session("activate-after-public-recovery".to_owned())
            .expect("公开恢复后继续唯一演员");
        assert_eq!(
            recovered
                .create_session(spec(
                    "run-public-sync-unknown-v2",
                    "conflicting-public-attachment",
                ))
                .expect_err("冲突重附着必须由同一演员拒绝"),
            SimRunActorErrorV2::SessionAlreadyCreated
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_sync_failure_closes_live_root_and_recovery_keeps_the_complete_group() {
        let directory = tempfile::tempdir().expect("创建同步结果未知测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut paper_opened = opened(
            "run-sync-unknown-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        paper_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut actor = SimRunActorV2::create_new(
            &path,
            paper_opened,
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 演员");

        reset_group_commit_counts_for_tests();
        crate::event_log::reset_event_log_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        actor.inject_log_sync_failure_for_tests();
        assert!(matches!(
            actor.commit_group(vec![
                BusinessInputV2::CreateSession(spec("run-sync-unknown-v2", "create-sync-unknown",)),
                BusinessInputV2::ActivateSession {
                    activation_id: "activate-sync-unknown".to_owned(),
                },
            ]),
            Err(SimRunActorErrorV2::Log(_))
        ));
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 0));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 2));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (1, 0));
        assert_eq!(crate::event_log::sync_attempt_count_for_tests(), 1);
        assert!(actor.pending_projections.is_empty());
        assert_eq!(
            actor
                .create_session(spec("run-sync-unknown-v2", "create-sync-unknown"))
                .expect_err("同步结果未知后当前运行根必须关闭"),
            SimRunActorErrorV2::Closed
        );
        drop(actor);

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).expect("打开结果未知日志");
        let (recovery, group) = recovery.next_group().expect("读取完整日志组");
        let group = group.expect("同步报错前完整写入的组仍由恢复裁决");
        assert_eq!(group.len(), 2);
        assert!(matches!(
            &group[0],
            LoggedBusinessInputV2::CreateSession(spec)
                if spec.creation_request_id == "create-sync-unknown"
        ));
        assert!(matches!(
            &group[1],
            LoggedBusinessInputV2::ActivateSession { activation_id }
                if activation_id == "activate-sync-unknown"
        ));
        let (recovery, end) = recovery.next_group().expect("读取日志末尾");
        assert!(end.is_none());
        let resumed = recovery.resume_paper().expect("从完整组后续写");
        assert_eq!(resumed.next_input_sequence(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_group_commits_once_and_projection_backpressure_preserves_the_suffix() {
        let directory = tempfile::tempdir().expect("创建投影组测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let port_state = Rc::new(RefCell::new(ControllableProjectionPortStateV2 {
            accept_limit: usize::MAX,
            ..ControllableProjectionPortStateV2::default()
        }));
        let mut run_opened = opened(
            "run-projection-group-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut actor = SimRunActorV2::create_new(
            &path,
            run_opened,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            Box::new(ControllableProjectionPortV2 {
                state: Rc::clone(&port_state),
            }),
            MAX_GROUP_BYTES,
        )
        .expect("创建投影组演员");
        let mut session_spec = spec("run-projection-group-v2", "create-projection-group");
        session_spec.caller_role = SimCallerRoleV1::Paper;
        actor.create_session(session_spec).expect("创建投影组会话");
        actor
            .activate_session("activate-projection-group".to_owned())
            .expect("激活投影组会话");
        port_state.borrow_mut().durable_through = 1;
        actor.deliver_pending_projections();
        assert!(actor.pending_projections.is_empty());

        let already_accepted = port_state.borrow().accepted.len();
        port_state.borrow_mut().accept_limit = already_accepted + 1;
        reset_group_commit_counts_for_tests();
        crate::event_log::reset_event_log_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        let committed = actor
            .commit_group(vec![
                BusinessInputV2::EnrollMarket(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "projection-enroll-2".to_owned(),
                    market: market('2', "456", "3", "4"),
                }),
                BusinessInputV2::EnrollMarket(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "projection-enroll-3".to_owned(),
                    market: market('3', "789", "5", "6"),
                }),
            ])
            .expect("两个登记输入必须形成一个耐久组");
        assert!(committed.unaccepted.is_none());
        assert_eq!(committed.outputs.len(), 2);
        assert_eq!(group_commit_counts_for_tests(), (2, 2, 1));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 2));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (1, 1));
        assert_eq!(actor.pending_projections.len(), 2);
        assert!(actor.pending_projections[0].accepted);
        assert!(!actor.pending_projections[1].accepted);

        assert_eq!(
            public_consumption_tests::finite_feeding::feed(
                &mut actor,
                |actor| actor.enroll_market(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "projection-enroll-blocked".to_owned(),
                    market: market('4', "987", "7", "8"),
                }),
                |_| panic!("投影未接纳不得进入日志容量等待")
            ),
            Err(SimRunActorErrorV2::ProjectionBackpressured)
        );
        assert_eq!(group_commit_counts_for_tests(), (2, 2, 1));

        port_state.borrow_mut().accept_limit = usize::MAX;
        actor.deliver_pending_projections();
        actor.deliver_pending_projections();
        assert_eq!(port_state.borrow().accepted.len(), already_accepted + 2);
        assert!(
            actor
                .pending_projections
                .iter()
                .all(|pending| pending.accepted)
        );
        assert_eq!(group_commit_counts_for_tests(), (2, 2, 1));

        port_state.borrow_mut().durable_through = 3;
        actor.deliver_pending_projections();
        assert!(actor.pending_projections.is_empty());

        assert_eq!(group_commit_counts_for_tests(), (2, 2, 1));

        let input_sequence_before_prefix = actor.next_input_sequence_for_tests().expect("活动日志");
        reset_group_commit_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        let prefix = actor
            .commit_group(vec![
                BusinessInputV2::EnrollMarket(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "projection-prefix-enroll-4".to_owned(),
                    market: market('4', "987", "7", "8"),
                }),
                BusinessInputV2::EnrollMarket(MarketEnrollmentRequestV1 {
                    enrollment_request_id: "projection-rejected-enroll-5".to_owned(),
                    market: market('5', "654", "7", "10"),
                }),
            ])
            .expect("非致命未受理错误必须提交此前成功前缀");
        assert_eq!(prefix.outputs.len(), 1);
        assert!(matches!(
            prefix.unaccepted,
            Some(SimRunActorErrorV2::MarketEnrollmentDirectory(_))
        ));
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 1));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 1));
        assert_eq!(
            actor.next_input_sequence_for_tests(),
            Some(input_sequence_before_prefix + 1)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_request_and_bucket_form_one_projection_with_continuous_private_sequences() {
        let directory = tempfile::tempdir().expect("创建请求投影测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let port_state = Rc::new(RefCell::new(ControllableProjectionPortStateV4 {
            accept_limit: usize::MAX,
            ..ControllableProjectionPortStateV4::default()
        }));
        let mut run_opened = opened(
            "run-request-projection-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        run_opened.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V4;
        run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut actor = SimRunActorV2::create_new(
            &path,
            run_opened,
            rule_snapshot,
            finality_policy,
            scenario,
            Arc::clone(&latency_profile),
            Box::new(ControllableProjectionPortV4 {
                state: Rc::clone(&port_state),
            }),
            MAX_GROUP_BYTES,
        )
        .expect("创建请求投影演员");
        let mut session_spec = spec("run-request-projection-v2", "create-request-projection");
        session_spec.caller_role = SimCallerRoleV1::Paper;
        actor
            .create_session(session_spec)
            .expect("创建请求投影会话");
        actor
            .activate_session("activate-request-projection".to_owned())
            .expect("激活请求投影会话");
        port_state.borrow_mut().durable_through = 1;
        actor.deliver_pending_projections();

        let request_id = "projection-post-order";
        let target_arrival_at_utc_ms = 1_100;
        let session_seed = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("请求投影会话存在")
            .session_seed
            .clone();
        let outbound_delay_ms = latency_profile.sample_delay_ms(
            session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let cutoff_at_utc_ms = target_arrival_at_utc_ms
            - i64::try_from(outbound_delay_ms).expect("测试出站时延可表示");
        let initial_events = decoded_initial_market_events("10");
        reset_group_commit_counts_for_tests();
        crate::event_log::reset_event_log_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        let committed = actor
            .commit_group(vec![
                BusinessInputV2::Request {
                    cutoff: request_cutoff(cutoff_at_utc_ms),
                    request: BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                        context: RequestContextV2 {
                            global_request_id: request_id.to_owned(),
                            caller_correlation_id: Some("projection-correlation".to_owned()),
                        },
                        item: post_item(
                            "projection-post-item",
                            &ConditionId(format!("0x{}", "1".repeat(64))),
                            &TokenId("1".to_owned()),
                            SimBuyOrderKindV1::GtcLimit {
                                limit_price_pusd_per_share: "0.1".parse().expect("测试价格"),
                                size_shares: "20".parse().expect("测试数量"),
                                post_only: true,
                            },
                        ),
                    }),
                },
                BusinessInputV2::ClosedMarketBucket(AcceptedClosedMarketBucketV2 {
                    reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                        path: MarketEventPathV1::PublicVisibility,
                        bucket_at_utc_ms: target_arrival_at_utc_ms,
                        start_index_position: 0,
                        next_index_position: initial_events.len() as u64,
                        closure: crate::market::MillisecondBucketClosureV1::NextEventAt(1_200),
                    },
                    events: Arc::from(initial_events),
                }),
            ])
            .expect("请求与同刻桶必须形成一个耐久组");
        assert!(committed.unaccepted.is_none());
        assert_eq!(committed.outputs.len(), 2);
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 1));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 2));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (1, 1));

        actor
            .register_request(
                request_cutoff(cutoff_at_utc_ms + 10_000),
                BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: Some("later-body-is-ignored".to_owned()),
                    },
                    item: post_item(
                        "later-item-is-ignored",
                        &ConditionId(format!("0x{}", "1".repeat(64))),
                        &TokenId("1".to_owned()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.2".parse().expect("重投测试价格"),
                            size_shares: "10".parse().expect("重投测试数量"),
                            post_only: false,
                        },
                    ),
                }),
            )
            .expect("物理重投必须直接返回首次接收回执");
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 1));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 2));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (1, 1));

        let port = port_state.borrow();
        let batch = port.accepted.last().expect("请求投影批次已被接纳");
        assert_eq!(batch.projection_batch_sequence, 2);
        assert_eq!(
            batch.order_lifecycle.len(),
            1,
            "投影正文：{:?}",
            batch.direct_responses
        );
        assert!(batch.trade_lifecycle.is_empty());
        assert!(batch.settlement_lifecycle.is_empty());
        assert_eq!(batch.direct_responses.len(), 1);
        assert_eq!(
            batch.order_lifecycle[0].metadata.private_feedback_sequence,
            1
        );
        let StoredDirectResponse::Final(BusinessDirectResponse::PostOrder(response)) =
            batch.direct_responses[0].as_ref()
        else {
            panic!("请求投影必须形成 PostOrder 方法特定响应")
        };
        assert_eq!(response.metadata.private_feedback_sequence, 2);
        assert_eq!(response.metadata.global_request_id, request_id);
        assert_eq!(response.item.item_id, "projection-post-item");
        let stored_response = actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .and_then(|session| session.mutation_requests.requests_by_id.get(request_id))
            .and_then(|request| request.completed_response.as_ref())
            .expect("唯一会话必须保留首次最终响应");
        assert!(Arc::ptr_eq(stored_response, &batch.direct_responses[0]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_v4_post_order_cancel_failure_and_original_log_recovery() {
        let directory = tempfile::tempdir().expect("创建请求投影测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let port_state = Rc::new(RefCell::new(ControllableProjectionPortStateV4 {
            accept_limit: usize::MAX,
            ..ControllableProjectionPortStateV4::default()
        }));
        let mut run_opened = opened(
            "run-request-projection-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        run_opened.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V4;
        run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut execution = SimExecution::create_new(
            &path,
            run_opened,
            rule_snapshot,
            finality_policy,
            scenario,
            Arc::clone(&latency_profile),
            Box::new(ControllableProjectionPortV4 {
                state: Rc::clone(&port_state),
            }),
            MAX_GROUP_BYTES,
        )
        .expect("创建请求投影演员");
        let mut session_spec = spec("run-request-projection-v2", "create-request-projection");
        session_spec.caller_role = SimCallerRoleV1::Paper;
        execution
            .create_session(session_spec)
            .expect("创建请求投影会话");
        execution
            .activate_session("activate-request-projection".to_owned())
            .expect("激活请求投影会话");
        port_state.borrow_mut().durable_through = 1;
        execution.actor.deliver_pending_projections();

        let request_id = "projection-post-order";
        let target_arrival_at_utc_ms = 1_100;
        let session_seed = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("请求投影会话存在")
            .session_seed
            .clone();
        let outbound_delay_ms = latency_profile.sample_delay_ms(
            session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let cutoff_at_utc_ms = target_arrival_at_utc_ms
            - i64::try_from(outbound_delay_ms).expect("测试出站时延可表示");
        let initial_events = decoded_initial_market_events("10");
        reset_group_commit_counts_for_tests();
        crate::event_log::reset_event_log_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        let receipt = execution
            .post_order(
                request_cutoff(cutoff_at_utc_ms),
                PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: Some("projection-correlation".to_owned()),
                    },
                    item: post_item(
                        "projection-post-item",
                        &ConditionId(format!("0x{}", "1".repeat(64))),
                        &TokenId("1".to_owned()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.1".parse().expect("测试价格"),
                            size_shares: "20".parse().expect("测试数量"),
                            post_only: true,
                        },
                    ),
                },
            )
            .expect("公开 post_order 必须返回登记回执");
        assert_eq!(receipt.scheduled_action_count(), 1);
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: target_arrival_at_utc_ms,
                    start_index_position: 0,
                    next_index_position: initial_events.len() as u64,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(10_000),
                },
                events: Arc::from(initial_events),
            })
            .expect("公开正文桶必须形成方法特定结果");
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 2));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (2, 2));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (2, 2));

        execution
            .post_order(
                request_cutoff(cutoff_at_utc_ms + 10_000),
                PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: Some("later-body-is-ignored".to_owned()),
                    },
                    item: post_item(
                        "later-item-is-ignored",
                        &ConditionId(format!("0x{}", "1".repeat(64))),
                        &TokenId("1".to_owned()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.2".parse().expect("重投测试价格"),
                            size_shares: "10".parse().expect("重投测试数量"),
                            post_only: false,
                        },
                    ),
                },
            )
            .expect("公开物理重投必须直接返回首次接收回执");
        assert_eq!(group_commit_counts_for_tests(), (2, 1, 2));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (2, 2));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (2, 2));

        let port = port_state.borrow();
        let batch = port.accepted.last().expect("请求投影批次已被接纳");
        assert_eq!(batch.projection_batch_sequence, 2);
        assert_eq!(
            batch.order_lifecycle.len(),
            1,
            "投影正文：{:?}",
            batch.direct_responses
        );
        assert!(batch.trade_lifecycle.is_empty());
        assert!(batch.settlement_lifecycle.is_empty());
        assert_eq!(batch.direct_responses.len(), 1);
        assert_eq!(
            batch.order_lifecycle[0].metadata.private_feedback_sequence,
            1
        );
        let StoredDirectResponse::Final(BusinessDirectResponse::PostOrder(response)) =
            batch.direct_responses[0].as_ref()
        else {
            panic!("请求投影必须形成 PostOrder 方法特定响应")
        };
        assert_eq!(response.metadata.private_feedback_sequence, 2);
        assert_eq!(response.metadata.global_request_id, request_id);
        assert_eq!(response.item.item_id, "projection-post-item");
        let stored_response = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .and_then(|session| session.mutation_requests.requests_by_id.get(request_id))
            .and_then(|request| request.completed_response.as_ref())
            .expect("唯一会话必须保留首次最终响应");
        assert!(Arc::ptr_eq(stored_response, &batch.direct_responses[0]));
        drop(port);
        execution
            .actor
            .commit_group(vec![BusinessInputV2::CancelScenario(
                crate::runtime_v2::CancelScenarioOperationV1::Configure(
                    crate::runtime_v2::CancelFailureScenarioV1 {
                        targets: vec![crate::runtime_v2::CancelFailureTargetV1 {
                            global_request_id: "finite-failure".into(),
                            platform_order_id: "sim-1".into(),
                        }],
                    },
                ),
            )])
            .unwrap();
        let replay = |path: &Path, actor: &SimRunActorV2| {
            replay_event_log_v2(
                path,
                &actor._rule_snapshot,
                &actor._finality_policy,
                &actor.scenario,
                &actor._latency_profile,
                MAX_GROUP_BYTES,
                None,
                |reference| {
                    assert_eq!(reference.bucket_at_utc_ms, 1_100);
                    Ok(Arc::from(decoded_initial_market_events("10")))
                },
            )
            .unwrap()
        };
        let before_failure = replay(&path, &execution.actor);
        let request = crate::runtime_v2::CancelOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "finite-failure".into(),
                caller_correlation_id: None,
            },
            item: CancelOrderItemV1 {
                item_id: "failure-item".into(),
                platform_order_id: "sim-1".into(),
            },
        };
        let receipt = execution
            .cancel_order(request_cutoff(1_200), request.clone())
            .unwrap();
        loop {
            if execution
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .mutation_requests
                .requests_by_id["finite-failure"]
                .completed_response
                .is_some()
            {
                break;
            }
            let required = execution.next_required_logical_drive().unwrap();
            execution
                .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position: 2,
                })
                .unwrap();
        }
        let before_duplicate = execution.actor.next_input_sequence_for_tests();
        assert_eq!(
            execution
                .cancel_order(request_cutoff(1_300), request.clone())
                .unwrap(),
            receipt
        );
        assert_eq!(
            execution.actor.next_input_sequence_for_tests(),
            before_duplicate
        );
        let live = execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert_eq!(
            live.execution_state,
            before_failure
                .state
                .session
                .as_ref()
                .unwrap()
                .execution_state,
            "暂失败及重复请求不得改变订单、资金、队列或成交状态"
        );
        let result = live.mutation_requests.requests_by_id["finite-failure"]
            .completed_response
            .as_ref()
            .unwrap();
        let StoredDirectResponse::Final(BusinessDirectResponse::CancelOrder(response)) =
            result.as_ref()
        else {
            panic!("真实撤单最终响应")
        };
        assert_eq!(
            response.target.outcome,
            contracts::CancelTargetOutcome::NotCanceledNoEffect(
                contracts::CancelNotCanceledReasonV3::TemporaryFailureNoEffect
            )
        );
        let text = fs::read_to_string(&path).unwrap();
        let mut lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let hits: Vec<_> = lines
            .iter()
            .filter_map(|l| l.pointer("/body/cancel_scenario_hits"))
            .flat_map(|h| h.as_array().unwrap())
            .cloned()
            .collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["global_request_id"], "finite-failure");
        assert_eq!(hits[0]["platform_order_id"], "sim-1");
        assert_eq!(
            hits[0]["effective_processing_at_utc_ms"],
            response.target.effective_processing_at_utc_ms
        );
        // 原恢复入口从输入重新执行；命中诊断材料从副本移除也必须得到同一结果。
        let without_hits = directory.path().join("without-hits.jsonl");
        for line in &mut lines {
            if let Some(hits) = line.pointer_mut("/body/cancel_scenario_hits") {
                *hits = serde_json::json!([]);
            }
        }
        fs::write(
            &without_hits,
            lines.iter().map(|l| format!("{l}\n")).collect::<String>(),
        )
        .unwrap();
        let result = Arc::clone(result);
        let expected_next = execution.actor.next_input_sequence_for_tests();
        let rules = Arc::clone(&execution.actor._rule_snapshot);
        let policy = Arc::clone(&execution.actor._finality_policy);
        let scenario = Arc::clone(&execution.actor.scenario);
        let latency = Arc::clone(&execution.actor._latency_profile);
        let live_state = execution.actor.state.take().unwrap();
        drop(execution);
        let live = live_state.session.as_ref().unwrap();
        for source in [&path, &without_hits] {
            let capture = Rc::new(RefCell::new(ControllableProjectionPortStateV4 {
                accept_limit: usize::MAX,
                ..ControllableProjectionPortStateV4::default()
            }));
            let mut restored = SimExecution::recover_paper(
                source,
                rules.clone(),
                policy.clone(),
                scenario.clone(),
                latency.clone(),
                Box::new(ControllableProjectionPortV4 {
                    state: capture.clone(),
                }),
                MAX_GROUP_BYTES,
                |reference| {
                    assert_eq!(reference.bucket_at_utc_ms, 1_100);
                    Ok(Arc::from(decoded_initial_market_events("10")))
                },
            )
            .unwrap();
            assert_eq!(
                restored.actor.opened.projection_version,
                crate::runtime_v2::BusinessProjectionVersionV1::V4
            );
            assert_eq!(
                restored.actor.next_input_sequence_for_tests(),
                expected_next
            );
            assert_eq!(
                capture.borrow().accepted,
                port_state.borrow().accepted,
                "V4原投影逐字段重建"
            );
            assert_eq!(
                restored
                    .cancel_order(request_cutoff(1_300), request.clone())
                    .unwrap(),
                receipt
            );
            assert_eq!(
                restored.actor.next_input_sequence_for_tests(),
                expected_next,
                "恢复后重复请求不追加输入"
            );
            let restored = restored
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap();
            assert_eq!(restored.execution_state, live.execution_state);
            assert_eq!(
                restored.mutation_requests.requests_by_id["finite-failure"]
                    .completed_response
                    .as_ref()
                    .unwrap(),
                &result,
                "原请求/目标/处理与收到时刻、暂失败原因必须原样恢复"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_paper_necessary_prefix_logs_and_projects_once_without_market_body() {
        let (directory, actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-empty-prefix-group-v2").await;
        let mut execution = SimExecution { actor };
        let initial_events = decoded_initial_market_events("2");
        let condition_id = initial_events[0].market.condition_id.clone();
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 1_100,
                    start_index_position: 0,
                    next_index_position: 2,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(1_200),
                },
                events: Arc::from(initial_events),
            })
            .expect("Paper 初始公开桶必须受理");

        let request_id = "empty-prefix-gtd-request";
        let platform_arrived_at_utc_ms = 1_200;
        let session_seed = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("空前缀测试会话存在")
            .session_seed
            .clone();
        let outbound_delay_ms = execution.actor._latency_profile.sample_delay_ms(
            session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let cutoff_at_utc_ms = platform_arrived_at_utc_ms
            - i64::try_from(outbound_delay_ms).expect("测试出站时延可表示");
        execution
            .post_order(
                request_cutoff(cutoff_at_utc_ms),
                PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: None,
                    },
                    item: post_item(
                        "empty-prefix-gtd-item",
                        &condition_id,
                        &TokenId("1".to_owned()),
                        SimBuyOrderKindV1::GtdLimit {
                            limit_price_pusd_per_share: "0.1".parse().expect("空前缀 GTD 价格"),
                            size_shares: "20".parse().expect("空前缀 GTD 数量"),
                            post_only: true,
                            expires_at_utc_ms: 300_999,
                        },
                    ),
                },
            )
            .expect("通过公开 post_order 登记空前缀 GTD 请求");
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: crate::runtime_v2::DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: platform_arrived_at_utc_ms,
                    start_index_position: 2,
                    next_index_position: 3,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(400_000),
                },
                events: Arc::from(vec![decoded_level_market_event(
                    platform_arrived_at_utc_ms,
                    platform_arrived_at_utc_ms + 10,
                    3,
                    "2",
                    "1.5",
                )]),
            })
            .expect("请求到达时正文桶必须形成 GTD 订单");
        let session = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("正文桶处理后唯一会话存在");
        assert_eq!(
            session
                .execution_state
                .next_gtd_deadline_at_utc_ms(execution.actor._rule_snapshot.snapshot()),
            Some(300_000),
            "原规则快照保留编码E到期"
        );
        let first_due_at_utc_ms = [
            session
                .execution_state
                .next_gtd_deadline_at_utc_ms(execution.actor._rule_snapshot.snapshot()),
            session.execution_state.next_scheduled_finality_at_utc_ms(),
            session.execution_state.next_rebate_clock_at_utc_ms(),
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("正文桶处理后必须排定到期义务");

        reset_group_commit_counts_for_tests();
        crate::event_log::reset_event_log_counts_for_tests();
        crate::event_log::reset_group_append_counts_for_tests();
        execution
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: MarketEventPathV1::PublicVisibility,
                closed_through_utc_ms: first_due_at_utc_ms,
                next_index_position: 3,
            })
            .expect("到期时必要空前缀必须受理");
        assert_eq!(group_commit_counts_for_tests(), (1, 1, 1));
        assert_eq!(crate::event_log::group_append_counts_for_tests(), (1, 1));
        assert_eq!(crate::event_log::event_log_counts_for_tests(), (1, 1));
        assert_eq!(crate::event_log::sync_attempt_count_for_tests(), 1);
        // 旧规则日志仍按已绑定的240秒准入和E截止重建，不自动切换当前版本。
        let rules = execution.actor._rule_snapshot.clone();
        let policy = execution.actor._finality_policy.clone();
        let scenario = execution.actor.scenario.clone();
        let latency = execution.actor._latency_profile.clone();
        let expected = execution.actor.state.take().unwrap();
        drop(execution);
        let recovered = SimExecution::recover_paper(
            &directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
            rules,
            policy,
            scenario,
            latency,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
            |reference| {
                Ok(Arc::from(match reference.bucket_at_utc_ms {
                    1_100 => decoded_initial_market_events("2"),
                    1_200 => vec![decoded_level_market_event(1_200, 1_210, 3, "2", "1.5")],
                    _ => panic!("未录入的市场桶"),
                }))
            },
        )
        .unwrap();
        assert_eq!(
            recovered
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .execution_state,
            expected.session.as_ref().unwrap().execution_state
        );
    }

    #[test]
    fn next_required_schedule_selects_the_earliest_of_all_four_categories() {
        let schedule = RequiredLogicalDriveScheduleV2 {
            gtd_at_utc_ms: Some(1_400),
            finality_at_utc_ms: Some(1_300),
            rebate_at_utc_ms: Some(1_500),
            settlement_at_utc_ms: None,
            mutation_at_utc_ms: Some(1_200),
        };

        assert_eq!(schedule.earliest_at_utc_ms(), Some(1_200));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_sparse_request_exposes_earliest_drive_and_completes_before_next_bucket() {
        let (_directory, actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-sparse-request-prefix-v2").await;
        let mut execution = SimExecution { actor };
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 1_100,
                    start_index_position: 0,
                    next_index_position: 2,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(2_000),
                },
                events: Arc::from(decoded_initial_market_events("2")),
            })
            .expect("Paper 初始公开桶必须受理");

        let request_id = "sparse-orders-request";
        let platform_arrived_at_utc_ms = 1_200;
        let outbound_delay_ms = execution.actor._latency_profile.sample_delay_ms(
            execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("稀疏请求测试会话存在")
                .session_seed
                .0
                .as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        execution
            .orders(
                request_cutoff(
                    platform_arrived_at_utc_ms
                        - i64::try_from(outbound_delay_ms).expect("测试出站时延可表示"),
                ),
                OrdersRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: None,
                    },
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("公开稀疏查询必须先登记 pending mutation");

        let required = execution
            .next_required_logical_drive()
            .expect("pending mutation 必须形成下一必要逻辑推进");
        let NextRequiredLogicalDriveV2 {
            path,
            required_at_utc_ms,
        } = required;
        assert_eq!(path, MarketEventPathV1::PublicVisibility);
        assert_eq!(required_at_utc_ms, platform_arrived_at_utc_ms);

        execution
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path,
                closed_through_utc_ms: required_at_utc_ms,
                next_index_position: 2,
            })
            .expect("早于下一真实桶的 pending mutation 必须由必要空前缀闭合");
        assert!(
            execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .and_then(|session| session.mutation_requests.requests_by_id.get(request_id))
                .and_then(|request| request.completed_response.as_ref())
                .is_some(),
            "稀疏 Request + prefix 必须形成方法特定响应"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_request_due_at_next_bucket_requires_the_real_bucket() {
        let (_directory, actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-request-bucket-tie-v2").await;
        let mut execution = SimExecution { actor };
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 1_100,
                    start_index_position: 0,
                    next_index_position: 2,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(1_200),
                },
                events: Arc::from(decoded_initial_market_events("2")),
            })
            .expect("Paper 初始公开桶必须受理");
        let request_id = "request-bucket-tie";
        let outbound_delay_ms = execution.actor._latency_profile.sample_delay_ms(
            execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("同刻测试会话存在")
                .session_seed
                .0
                .as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        execution
            .orders(
                request_cutoff(
                    1_200 - i64::try_from(outbound_delay_ms).expect("测试出站时延可表示"),
                ),
                OrdersRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: None,
                    },
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("登记与下一真实桶同刻的请求");
        let required = execution
            .next_required_logical_drive()
            .expect("同刻 pending mutation 仍可观察");
        assert_eq!(required.required_at_utc_ms, 1_200);
        assert!(
            execution
                .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: 1_200,
                    next_index_position: 2,
                })
                .is_err(),
            "同刻真实桶必须优先，空前缀不得越过"
        );
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 1_200,
                    start_index_position: 2,
                    next_index_position: 3,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(2_000),
                },
                events: Arc::from(vec![decoded_level_market_event(
                    1_200, 1_210, 3, "2", "1.5",
                )]),
            })
            .expect("同刻真实桶必须处理 pending mutation");
        assert!(
            execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .and_then(|session| session.mutation_requests.requests_by_id.get(request_id))
                .and_then(|request| request.completed_response.as_ref())
                .is_some(),
            "同刻真实桶必须形成方法特定响应"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_sparse_request_prefix_recovers_the_same_result_and_next_drive() {
        let (directory, actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-sparse-prefix-recovery-v2").await;
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let mut execution = SimExecution { actor };
        let initial_events: Arc<[ExecutionMarketEventV3]> =
            Arc::from(decoded_initial_market_events("2"));
        let reference = DurableMarketBucketReferenceV2 {
            path: MarketEventPathV1::PublicVisibility,
            bucket_at_utc_ms: 1_100,
            start_index_position: 0,
            next_index_position: 2,
            closure: crate::market::MillisecondBucketClosureV1::NextEventAt(2_000),
        };
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: reference.clone(),
                events: Arc::clone(&initial_events),
            })
            .expect("Paper 恢复代表初始桶必须受理");
        let request_id = "sparse-prefix-recovery-request";
        let outbound_delay_ms = execution.actor._latency_profile.sample_delay_ms(
            execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("恢复代表会话存在")
                .session_seed
                .0
                .as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        execution
            .orders(
                request_cutoff(
                    1_200 - i64::try_from(outbound_delay_ms).expect("测试出站时延可表示"),
                ),
                OrdersRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: request_id.to_owned(),
                        caller_correlation_id: None,
                    },
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("登记恢复代表稀疏查询");
        let required = execution
            .next_required_logical_drive()
            .expect("恢复代表 pending mutation 可观察");
        execution
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: required.path,
                closed_through_utc_ms: required.required_at_utc_ms,
                next_index_position: 2,
            })
            .expect("恢复代表必要空前缀必须受理");
        let live_result = Arc::clone(
            execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .and_then(|session| session.mutation_requests.requests_by_id.get(request_id))
                .and_then(|request| request.completed_response.as_ref())
                .expect("live 稀疏查询结果存在"),
        );
        let live_next = execution.next_required_logical_drive();
        let rule_snapshot = Arc::clone(&execution.actor._rule_snapshot);
        let finality_policy = Arc::clone(&execution.actor._finality_policy);
        let scenario = Arc::clone(&execution.actor.scenario);
        let latency_profile = Arc::clone(&execution.actor._latency_profile);
        drop(execution);

        let recovered = SimExecution::recover_paper(
            &path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
            move |logged_reference| {
                assert_eq!(logged_reference, &reference);
                Ok(Arc::clone(&initial_events))
            },
        )
        .expect("Request + prefix 必须从同一日志恢复");
        let recovered_result = recovered
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .and_then(|session| session.mutation_requests.requests_by_id.get(request_id))
            .and_then(|request| request.completed_response.as_ref())
            .expect("恢复后的稀疏查询结果存在");
        assert_eq!(recovered_result.as_ref(), live_result.as_ref());
        assert_eq!(recovered.next_required_logical_drive(), live_next);
    }

    #[cfg(feature = "measurement")]
    #[tokio::test(flavor = "current_thread")]
    async fn public_paper_retention_releases_history_and_cold_recovery_preserves_semantics() {
        let directory = tempfile::tempdir().expect("创建公开 retention 代表目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let test_market = market_with_window('1', "123", "1", "2", 1_000, 100_000);
        let scenario = sealed_scenario_for_markets(vec![test_market.clone()]);
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let live_capture = Rc::new(RefCell::new(CapturingProjectionPortStateV2::default()));
        let mut run_opened = opened(
            "run-public-retention-recovery-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut execution = SimExecution::create_new(
            &path,
            run_opened,
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            Box::new(CapturingProjectionPortV2 {
                state: Rc::clone(&live_capture),
            }),
            MAX_GROUP_BYTES,
        )
        .expect("创建公开 Paper retention 代表根");
        let mut session_spec = spec(
            "run-public-retention-recovery-v2",
            "create-public-retention-recovery",
        );
        session_spec.caller_role = SimCallerRoleV1::Paper;
        execution
            .create_session(session_spec)
            .expect("创建公开 Paper retention 会话");
        execution
            .activate_session("activate-public-retention-recovery".to_owned())
            .expect("激活公开 Paper retention 会话");

        let initial_events: Arc<[ExecutionMarketEventV3]> = Arc::from(
            decoded_initial_events_for_market_with_quantity(&test_market, 1_100, 1_110, 1, "10"),
        );
        let condition_id = initial_events[0].market.condition_id.clone();
        let token_id = TokenId("1".to_owned());
        let bucket_reference = DurableMarketBucketReferenceV2 {
            path: MarketEventPathV1::PublicVisibility,
            bucket_at_utc_ms: 1_100,
            start_index_position: 0,
            next_index_position: 2,
            closure: crate::market::MillisecondBucketClosureV1::NextEventAt(200_000),
        };
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: bucket_reference.clone(),
                events: Arc::clone(&initial_events),
            })
            .expect("公开 retention 代表初始盘口桶必须受理");
        let context = |request_id: &str| RequestContextV2 {
            global_request_id: request_id.to_owned(),
            caller_correlation_id: None,
        };

        execution
            .post_order(
                request_cutoff(10_000),
                PostOrderRequestV2 {
                    context: context("retention-first-fok"),
                    item: post_item(
                        "retention-first-fok-item",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::FokMarket {
                            spend_pusd: "3".parse().expect("首个 FOK 预算"),
                            worst_price_pusd_per_share: "0.6".parse().expect("首个 FOK 最差价"),
                        },
                    ),
                },
            )
            .expect("登记首个公开 FOK");
        drive_public_until_response(&mut execution, &live_capture, "retention-first-fok", 2);
        let first_fok: PostOrderDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-first-fok"),
        )
        .expect("解码首个公开 FOK 响应");
        let SubmissionItemOutcomeV2::Accepted(first_accepted) = &first_fok.item.outcome else {
            panic!("首个公开 FOK 必须 accepted terminal")
        };
        assert_eq!(first_accepted.platform_order_id, "sim-1");
        assert_eq!(first_accepted.fills.len(), 1);
        let first_trade_id = first_accepted.fills[0].trade_id.clone();

        execution
            .orders(
                request_cutoff(20_000),
                OrdersRequestV2 {
                    context: context("retention-old-orders-root"),
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("登记旧 orders 根查询");
        drive_public_until_response(
            &mut execution,
            &live_capture,
            "retention-old-orders-root",
            2,
        );
        execution
            .trades(
                request_cutoff(30_000),
                TradesRequestV2 {
                    context: context("retention-old-trades-root"),
                    filter: TradesFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("登记旧 trades 根查询");
        drive_public_until_response(
            &mut execution,
            &live_capture,
            "retention-old-trades-root",
            2,
        );
        let old_orders: OrdersDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-old-orders-root"),
        )
        .expect("解码旧 orders 根响应");
        let old_trades: TradesDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-old-trades-root"),
        )
        .expect("解码旧 trades 根响应");
        let (
            OrdersQueryResultV2::Page(old_orders_page),
            TradesQueryResultV2::Page(old_trades_page),
        ) = (&old_orders.result, &old_trades.result)
        else {
            panic!("公开 retention 代表必须形成旧 orders/trades Page")
        };
        assert_eq!(old_orders_page.root_sequence, old_trades_page.root_sequence);
        let old_root_sequence = old_orders_page.root_sequence;

        execution
            .post_order(
                request_cutoff(40_000),
                PostOrderRequestV2 {
                    context: context("retention-current-order"),
                    item: post_item(
                        "retention-current-order-item",
                        &condition_id,
                        &token_id,
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.3".parse().expect("当前 GTC 价格"),
                            size_shares: "5".parse().expect("当前 GTC 数量"),
                            post_only: false,
                        },
                    ),
                },
            )
            .expect("登记当前公开订单");
        drive_public_until_response(&mut execution, &live_capture, "retention-current-order", 2);
        let current_order: PostOrderDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-current-order"),
        )
        .expect("解码当前公开订单响应");
        let SubmissionItemOutcomeV2::Accepted(current_accepted) = &current_order.item.outcome
        else {
            panic!("当前公开订单必须 accepted")
        };
        assert_eq!(current_accepted.platform_order_id, "sim-2");
        assert!(current_accepted.fills.is_empty());
        let retained_from_utc_ms = current_order.item.effective_processing_at_utc_ms;

        execution
            .orders(
                request_cutoff(50_000),
                OrdersRequestV2 {
                    context: context("retention-current-orders-root"),
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("登记当前 orders 根查询");
        drive_public_until_response(
            &mut execution,
            &live_capture,
            "retention-current-orders-root",
            2,
        );
        let current_orders: OrdersDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-current-orders-root"),
        )
        .expect("解码当前 orders 根响应");
        let OrdersQueryResultV2::Page(current_orders_page) = &current_orders.result else {
            panic!("公开 retention 代表必须形成当前 orders Page")
        };
        assert!(current_orders_page.root_sequence > old_root_sequence);

        let resources_before = execution.measurement_resource_snapshot();
        execution
            .advance_retention(RetentionAdvanceV2 {
                earliest_retained_root_sequence: old_root_sequence + 1,
                order_history_retained_from_utc_ms: Some(retained_from_utc_ms),
                trade_history_retained_from_utc_ms: Some(retained_from_utc_ms),
            })
            .expect("公开 retention 推进必须受理");
        let resources_after = execution.measurement_resource_snapshot();
        assert!(resources_after.business.records < resources_before.business.records);
        assert!(resources_after.business.bytes < resources_before.business.bytes);
        assert_eq!(
            resources_after.next_input_sequence,
            resources_before.next_input_sequence + 1,
            "一次公开 retention 只能新增一项耐久输入"
        );

        execution
            .orders(
                request_cutoff(60_000),
                OrdersRequestV2 {
                    context: context("retention-expired-root"),
                    filter: OrdersFilterV1::default(),
                    cursor: Some(QueryCursorV2 {
                        root_sequence: old_root_sequence,
                        next_offset: 0,
                    }),
                },
            )
            .expect("登记旧 root 续读");
        execution
            .order(
                request_cutoff(70_000),
                OrderRequestV2 {
                    context: context("retention-expired-order"),
                    platform_order_id: "sim-1".to_owned(),
                },
            )
            .expect("登记 cutoff 前 order 查询");
        execution
            .trades(
                request_cutoff(80_000),
                TradesRequestV2 {
                    context: context("retention-expired-trade"),
                    filter: TradesFilterV1 {
                        trade_id: Some(first_trade_id),
                        ..TradesFilterV1::default()
                    },
                    cursor: None,
                },
            )
            .expect("登记 cutoff 前 trade 查询");
        execution
            .orders(
                request_cutoff(90_000),
                OrdersRequestV2 {
                    context: context("retention-current-range"),
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                },
            )
            .expect("登记 retention 后当前范围查询");
        for request_id in [
            "retention-expired-root",
            "retention-expired-order",
            "retention-expired-trade",
            "retention-current-range",
        ] {
            drive_public_until_response(&mut execution, &live_capture, request_id, 2);
        }

        let expired_root: OrdersDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-expired-root"),
        )
        .expect("解码旧 root 响应");
        assert_eq!(
            expired_root.result,
            OrdersQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::RootSnapshot {
                requested_root_sequence: old_root_sequence,
                earliest_retained_root_sequence: old_root_sequence + 1,
            })
        );
        let expired_order: OrderDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-expired-order"),
        )
        .expect("解码过期 order 响应");
        assert_eq!(
            expired_order.result,
            OrderQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::OrderHistory {
                retained_from_utc_ms,
            })
        );
        let expired_trade: TradesDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-expired-trade"),
        )
        .expect("解码过期 trade 响应");
        assert_eq!(
            expired_trade.result,
            TradesQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::TradeHistory {
                retained_from_utc_ms,
            })
        );
        let current_range: OrdersDirectResponseV2 = serde_json::from_value(
            captured_direct_response_json(&live_capture, "retention-current-range"),
        )
        .expect("解码当前范围响应");
        assert!(matches!(current_range.result, OrdersQueryResultV2::Page(_)));

        let live_semantics = [
            "retention-expired-root",
            "retention-expired-order",
            "retention-expired-trade",
            "retention-current-range",
        ]
        .map(|request_id| captured_direct_response_json(&live_capture, request_id));
        let live_resources = execution.measurement_resource_snapshot();
        drop(execution);

        let recovery_capture = Rc::new(RefCell::new(CapturingProjectionPortStateV2::default()));
        let recovered = SimExecution::recover_paper(
            &path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            Box::new(CapturingProjectionPortV2 {
                state: Rc::clone(&recovery_capture),
            }),
            MAX_GROUP_BYTES,
            move |reference| {
                assert_eq!(reference, &bucket_reference);
                Ok(Arc::clone(&initial_events))
            },
        )
        .expect("从同一 Paper 日志冷恢复 retention 代表");
        assert_eq!(recovered.measurement_resource_snapshot(), live_resources);
        let recovery_semantics = [
            "retention-expired-root",
            "retention-expired-order",
            "retention-expired-trade",
            "retention-current-range",
        ]
        .map(|request_id| captured_direct_response_json(&recovery_capture, request_id));
        assert_eq!(recovery_semantics, live_semantics);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_recovery_truncates_tail_replays_bucket_once_and_resends_above_watermark() {
        let directory = tempfile::tempdir().expect("创建 Paper 恢复测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let initial_events: Arc<[ExecutionMarketEventV3]> =
            Arc::from(decoded_initial_market_events("2"));
        let reference = DurableMarketBucketReferenceV2 {
            path: MarketEventPathV1::PublicVisibility,
            bucket_at_utc_ms: 1_110,
            start_index_position: 0,
            next_index_position: 2,
            closure: crate::market::MillisecondBucketClosureV1::NextEventAt(1_200),
        };
        let initial_port_state = Rc::new(RefCell::new(ControllableProjectionPortStateV2 {
            accept_limit: usize::MAX,
            ..ControllableProjectionPortStateV2::default()
        }));
        let mut run_opened = opened(
            "run-paper-recovery-v2",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
        let mut actor = SimRunActorV2::create_new(
            &path,
            run_opened,
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            Box::new(ControllableProjectionPortV2 {
                state: Rc::clone(&initial_port_state),
            }),
            MAX_GROUP_BYTES,
        )
        .expect("创建 Paper 恢复测试运行根");
        let mut session_spec = spec("run-paper-recovery-v2", "create-paper-recovery");
        session_spec.caller_role = SimCallerRoleV1::Paper;
        actor.create_session(session_spec).expect("创建 Paper 会话");
        actor
            .activate_session("activate-paper-recovery".to_owned())
            .expect("激活 Paper 会话");
        actor
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-paper-recovery".to_owned(),
                market: market('2', "456", "3", "4"),
            })
            .expect("登记恢复测试市场");
        actor
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: reference.clone(),
                events: Arc::clone(&initial_events),
            })
            .expect("提交恢复测试市场桶");
        assert_eq!(actor.next_input_sequence_for_tests(), Some(5));
        drop(actor);

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("打开 Paper 日志追加不完整尾部")
            .write_all(br#"{"type":"InputGroup"#)
            .expect("写入不完整尾部");

        let recovery_port_state = Rc::new(RefCell::new(ControllableProjectionPortStateV2 {
            accept_limit: usize::MAX,
            durable_through: 1,
            ..ControllableProjectionPortStateV2::default()
        }));
        let bucket_read_count = Rc::new(RefCell::new(0_usize));
        let mut recovered = SimRunActorV2::recover_paper(
            &path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            Box::new(ControllableProjectionPortV2 {
                state: Rc::clone(&recovery_port_state),
            }),
            MAX_GROUP_BYTES,
            {
                let bucket_read_count = Rc::clone(&bucket_read_count);
                move |logged_reference| {
                    assert_eq!(logged_reference, &reference);
                    *bucket_read_count.borrow_mut() += 1;
                    Ok(Arc::clone(&initial_events))
                }
            },
        )
        .expect("恢复 Paper 运行根");
        assert_eq!(*bucket_read_count.borrow(), 1);
        assert_eq!(recovered.next_input_sequence_for_tests(), Some(5));
        {
            let port = recovery_port_state.borrow();
            assert_eq!(
                port.accepted
                    .iter()
                    .map(|batch| batch.projection_batch_sequence)
                    .collect::<Vec<_>>(),
                vec![2]
            );
        }
        recovered
            .enroll_market(MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-paper-recovery-after-restart".to_owned(),
                market: market('3', "789", "5", "6"),
            })
            .expect("恢复后必须从下一输入序号继续");
        assert_eq!(recovered.next_input_sequence_for_tests(), Some(6));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retention_advance_releases_query_roots_and_replays_the_same_watermarks() {
        let (directory, mut actor) =
            active_market_actor(SimCallerRoleV1::Paper, "run-retention-release-v2").await;
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);

        let query_id = "retention-empty-orders";
        let query_arrival_at_utc_ms = 1_100;
        let query_delay_ms = actor._latency_profile.sample_delay_ms(
            actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("保留测试会话存在")
                .session_seed
                .0
                .as_bytes(),
            query_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        actor
            .register_request(
                request_cutoff(
                    query_arrival_at_utc_ms
                        - i64::try_from(query_delay_ms).expect("查询出站时延可表示"),
                ),
                BusinessRequestV2::Orders(OrdersRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: query_id.to_owned(),
                        caller_correlation_id: None,
                    },
                    filter: OrdersFilterV1::default(),
                    cursor: None,
                }),
            )
            .expect("登记保留测试空订单查询");
        let initial_events: Arc<[ExecutionMarketEventV3]> =
            Arc::from(decoded_initial_market_events("2"));
        let bucket_reference = DurableMarketBucketReferenceV2 {
            path: MarketEventPathV1::PublicVisibility,
            bucket_at_utc_ms: 1_100,
            start_index_position: 0,
            next_index_position: 2,
            closure: crate::market::MillisecondBucketClosureV1::NextEventAt(1_200),
        };
        actor
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: bucket_reference.clone(),
                events: Arc::clone(&initial_events),
            })
            .expect("首个真实公开桶同时形成修订零查询根");

        let post_id = "retention-revision-post";
        let post_arrival_at_utc_ms = 1_200;
        let post_delay_ms = actor._latency_profile.sample_delay_ms(
            actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .expect("保留测试会话存在")
                .session_seed
                .0
                .as_bytes(),
            post_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        actor
            .register_request(
                request_cutoff(
                    post_arrival_at_utc_ms
                        - i64::try_from(post_delay_ms).expect("提交出站时延可表示"),
                ),
                BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: post_id.to_owned(),
                        caller_correlation_id: None,
                    },
                    item: post_item(
                        "retention-revision-item",
                        &initial_events[0].market.condition_id,
                        &TokenId("1".to_owned()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.1".parse().expect("保留测试价格"),
                            size_shares: "20".parse().expect("保留测试数量"),
                            post_only: true,
                        },
                    ),
                }),
            )
            .expect("登记推进查询修订的订单");
        let post_events: Arc<[ExecutionMarketEventV3]> =
            Arc::from(vec![decoded_level_market_event(
                post_arrival_at_utc_ms,
                post_arrival_at_utc_ms + 10,
                3,
                "2",
                "1.5",
            )]);
        let post_bucket_reference = DurableMarketBucketReferenceV2 {
            path: MarketEventPathV1::PublicVisibility,
            bucket_at_utc_ms: post_arrival_at_utc_ms,
            start_index_position: 2,
            next_index_position: 3,
            closure: crate::market::MillisecondBucketClosureV1::NextEventAt(2_000),
        };
        actor
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: post_bucket_reference.clone(),
                events: Arc::clone(&post_events),
            })
            .expect("真实正文桶接纳订单并推进查询修订");

        let session = actor.state.as_ref().unwrap().session.as_ref().unwrap();
        assert!(
            session
                .query_state
                .order_roots
                .contains_key(&(0, OrdersQueryKeyV2::from(&OrdersFilterV1::default())))
        );
        let revision = session.query_state.current_revision;
        assert_eq!(revision, 1);
        actor
            .advance_retention(RetentionAdvanceV2 {
                earliest_retained_root_sequence: revision,
                order_history_retained_from_utc_ms: Some(1_000),
                trade_history_retained_from_utc_ms: Some(1_000),
            })
            .expect("单调保留水位推进必须受理");
        let state = actor.state.as_ref().expect("保留推进后状态存在");
        let session = state.session.as_ref().expect("保留推进后会话存在");
        assert!(session.query_state.order_roots.is_empty());

        let rule_snapshot = Arc::clone(&actor._rule_snapshot);
        let finality_policy = Arc::clone(&actor._finality_policy);
        let scenario = Arc::clone(&actor.scenario);
        let latency_profile = Arc::clone(&actor._latency_profile);
        drop(actor);

        let recovered = SimRunActorV2::recover_paper(
            &path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
            move |reference| {
                if reference == &bucket_reference {
                    Ok(Arc::clone(&initial_events))
                } else if reference == &post_bucket_reference {
                    Ok(Arc::clone(&post_events))
                } else {
                    panic!("恢复只应读取两条已记录市场桶引用：{reference:?}")
                }
            },
        )
        .expect("恢复带保留推进的 Paper 运行根");
        let recovered_state = recovered.state.as_ref().expect("恢复后状态存在");
        let recovered_session = recovered_state.session.as_ref().expect("恢复后会话存在");
        assert_eq!(recovered_session.query_state.current_revision, revision);
        assert_eq!(
            recovered_session
                .query_state
                .earliest_retained_root_sequence,
            revision
        );
        assert_eq!(
            recovered_session
                .query_state
                .order_history_retained_from_utc_ms,
            Some(1_000)
        );
        assert_eq!(
            recovered_session
                .query_state
                .trade_history_retained_from_utc_ms,
            Some(1_000)
        );
        assert!(recovered_session.query_state.order_roots.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oversized_group_keeps_the_existing_typed_error_and_no_partial_business_result() {
        let directory = tempfile::tempdir().expect("创建组上限测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let max_group_bytes = 1_024;
        let mut actor = SimRunActorV2::create_new(
            &path,
            opened(
                "run-group-limit-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            max_group_bytes,
        )
        .expect("创建组上限运行根");
        actor
            .create_session(spec("run-group-limit-v2", "create-group-limit"))
            .expect("组上限测试先形成唯一会话");
        assert_eq!(
            actor
                .activate_session("x".repeat(max_group_bytes * 2))
                .expect_err("超大控制组必须由既有日志上限拒绝"),
            SimRunActorErrorV2::Log(SimEventLogErrorV2::GroupTooLarge {
                limit_bytes: max_group_bytes,
            })
        );
        drop(actor);

        let (_, recovery) =
            SimEventLogRecoveryV2::open(&path, max_group_bytes).expect("打开组上限日志");
        let (recovery, first) = recovery.next_group().expect("读取唯一已受理组");
        assert!(matches!(
            first.as_deref(),
            Some([LoggedBusinessInputV2::CreateSession(_)])
        ));
        let (_, end) = recovery.next_group().expect("超大组没有形成完整日志输入");
        assert!(end.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_clean_run_drives_body_eof_retention_and_seals_to_cold_candidate() {
        let directory = tempfile::tempdir().expect("创建 CleanRun 完成测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let port_state = Rc::new(RefCell::new(ControllableProjectionPortStateV2 {
            accept_limit: usize::MAX,
            ..ControllableProjectionPortStateV2::default()
        }));
        let mut execution = SimExecution::create_new(
            &path,
            opened(
                "run-clean-candidate-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            Box::new(ControllableProjectionPortV2 {
                state: Rc::clone(&port_state),
            }),
            MAX_GROUP_BYTES,
        )
        .expect("创建 CleanRun 完成测试运行根");
        execution
            .create_session(spec("run-clean-candidate-v2", "create-clean-candidate"))
            .expect("创建 CleanRun 会话");
        execution
            .activate_session("activate-clean-candidate".to_owned())
            .expect("激活 CleanRun 会话");

        let initial_events: Arc<[ExecutionMarketEventV3]> =
            Arc::from(decoded_initial_market_events("2"));
        let condition_id = initial_events[0].market.condition_id.0.clone();
        let resolution_events: Arc<[ExecutionMarketEventV3]> =
            Arc::from(vec![decoded_resolution_market_event(4_000, 4_100, 3)]);
        let buckets = [
            (
                DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    bucket_at_utc_ms: 1_100,
                    start_index_position: 0,
                    next_index_position: 2,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(4_000),
                },
                Arc::clone(&initial_events),
            ),
            (
                DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 1_110,
                    start_index_position: 0,
                    next_index_position: 2,
                    closure: crate::market::MillisecondBucketClosureV1::NextEventAt(4_100),
                },
                initial_events,
            ),
            (
                DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    bucket_at_utc_ms: 4_000,
                    start_index_position: 2,
                    next_index_position: 3,
                    closure: crate::market::MillisecondBucketClosureV1::EndOfInput,
                },
                Arc::clone(&resolution_events),
            ),
            (
                DurableMarketBucketReferenceV2 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 4_100,
                    start_index_position: 2,
                    next_index_position: 3,
                    closure: crate::market::MillisecondBucketClosureV1::EndOfInput,
                },
                resolution_events,
            ),
        ];
        for (reference, events) in &buckets[..2] {
            execution
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: reference.clone(),
                    events: Arc::clone(events),
                })
                .expect("驱动 CleanRun 双路径市场桶");
        }
        execution
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: MarketEventPathV1::HiddenPlatform,
                closed_through_utc_ms: 2_000,
                next_index_position: 2,
            })
            .expect("EOF 前必须先闭合既有 maker 时钟");
        for (reference, events) in &buckets[2..] {
            execution
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: reference.clone(),
                    events: Arc::clone(events),
                })
                .expect("驱动 CleanRun 双路径 EOF 桶");
        }
        let session = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .expect("完成前保留唯一会话");
        assert!(
            session.public_markets.all_qualified_resolutions_terminal(),
            "公开路径阶段：{:?}，隐藏路径阶段：{:?}",
            session
                .public_markets
                .lifecycle_phase_for_tests(&condition_id),
            session
                .hidden_platform_markets
                .as_ref()
                .and_then(|markets| markets.lifecycle_phase_for_tests(&condition_id))
        );
        assert!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话保留隐藏市场状态")
                .all_qualified_resolutions_terminal(),
            "隐藏路径阶段：{:?}",
            session
                .hidden_platform_markets
                .as_ref()
                .and_then(|markets| markets.lifecycle_phase_for_tests(&condition_id))
        );
        let retention_revision = session.query_state.current_revision;
        execution
            .advance_retention(RetentionAdvanceV2 {
                earliest_retained_root_sequence: retention_revision,
                order_history_retained_from_utc_ms: Some(4_100),
                trade_history_retained_from_utc_ms: Some(4_100),
            })
            .expect("关闭前沿同一日志推进既有保留水位");

        let projection_sequences = port_state
            .borrow()
            .accepted
            .iter()
            .map(|batch| batch.projection_batch_sequence)
            .collect::<Vec<_>>();
        assert!(!projection_sequences.is_empty());
        assert_eq!(
            projection_sequences,
            (1..=projection_sequences.len() as u64).collect::<Vec<_>>(),
            "公开正文、EOF 与保留入口必须连续交付同一投影水位"
        );
        port_state.borrow_mut().durable_through = *projection_sequences
            .last()
            .expect("公开 CleanRun 至少交付一个投影批次");

        let session = execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert!(session.hidden_platform_frontier.sealed);
        assert!(session.public_visibility_frontier.sealed);
        assert!(session.public_markets.all_qualified_resolutions_terminal());
        assert_eq!(
            session.query_state.earliest_retained_root_sequence,
            retention_revision
        );
        assert_eq!(
            session.query_state.order_history_retained_from_utc_ms,
            Some(4_100)
        );
        assert_eq!(
            session.query_state.trade_history_retained_from_utc_ms,
            Some(4_100)
        );
        assert!(
            session
                .hidden_platform_markets
                .as_ref()
                .expect("训练会话保留隐藏市场状态")
                .all_qualified_resolutions_terminal()
        );
        let candidate = execution
            .complete_clean_run()
            .expect("CleanRun 必须形成未发布冷产物候选");
        assert_eq!(candidate.run_id(), "run-clean-candidate-v2");
        assert_eq!(candidate.log_path, path);
        assert_eq!(candidate.accepted_input_count, 8);
        let recovered = SimExecution::recover_sealed_clean_run(
            &path,
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            MAX_GROUP_BYTES,
            |reference| {
                buckets
                    .iter()
                    .find(|(stored, _)| stored == reference)
                    .map(|(_, events)| Arc::clone(events))
                    .ok_or(SimRunActorErrorV2::CompletionNotReady(
                        "恢复找不到耐久市场桶",
                    ))
            },
        )
        .expect("显式恢复读取实际封口日志和市场桶");
        assert_eq!(recovered.run_id(), "run-clean-candidate-v2");
        assert_eq!(recovered.accepted_input_count(), 8);
        candidate
            .delete_after_retention()
            .unwrap_or_else(|error| panic!("保留期结束后必须删除精确运行文件：{error}"));
        assert!(!path.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_failed_initialization_clean_run_seals_to_a_cold_candidate() {
        let directory = tempfile::tempdir().expect("创建公开初始化失败封口目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut execution = SimExecution::create_new(
            &path,
            opened(
                "run-public-failed-init-seal-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建公开初始化失败封口根");
        execution
            .create_session(spec(
                "run-public-failed-init-seal-v2",
                "create-public-failed-init-seal",
            ))
            .expect("公开创建待初始化会话");
        execution
            .fail_session_initialization("public-ledger-rejected".to_owned())
            .expect("公开记录稳定初始化失败决定");

        let candidate = execution
            .complete_clean_run()
            .expect("公开初始化失败终态必须允许冷封口");
        assert_eq!(candidate.run_id(), "run-public-failed-init-seal-v2");
        assert_eq!(candidate.accepted_input_count(), 2);
        assert_eq!(candidate.log_path(), path.as_path());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_initialization_clean_run_seals_without_market_input() {
        let directory = tempfile::tempdir().expect("创建初始化失败封口测试目录");
        let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut actor = SimRunActorV2::create_new(
            &path,
            opened(
                "run-failed-init-seal-v2",
                &rule_snapshot,
                &finality_policy,
                &scenario,
                &latency_profile,
            ),
            rule_snapshot,
            finality_policy,
            scenario,
            latency_profile,
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建初始化失败封口测试运行根");
        actor
            .create_session(spec("run-failed-init-seal-v2", "create-failed-init-seal"))
            .expect("创建待初始化会话");
        actor
            .fail_session_initialization("ledger-rejected".to_owned())
            .expect("记录稳定初始化失败决定");

        assert!(matches!(
            actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap()
                .initialization,
            SessionInitializationV2::InitializationFailedClosed { .. }
        ));
        let candidate = actor
            .complete_clean_run()
            .expect("初始化失败终态必须允许冷封口");
        assert_eq!(candidate.accepted_input_count, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_root_binds_all_cold_file_identities_before_log_creation() {
        let directory = tempfile::tempdir().expect("创建运行根绑定测试目录");
        let rule_snapshot = successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = successful_policy_v2_for_tests();
        let scenario = sealed_scenario();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");

        let rule_path = directory.path().join("rule-mismatch.jsonl");
        let mut rule_mismatch = opened(
            "run-rule-mismatch",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        rule_mismatch.platform_rule_snapshot_file_sha256 = sha('a');
        assert!(matches!(
            SimRunActorV2::create_new(
                &rule_path,
                rule_mismatch,
                Arc::clone(&rule_snapshot),
                Arc::clone(&finality_policy),
                Arc::clone(&scenario),
                Arc::clone(&latency_profile),
                accepting_projection_port(),
                MAX_GROUP_BYTES,
            ),
            Err(SimRunActorErrorV2::RuleSnapshotIdentityMismatch)
        ));
        assert!(!rule_path.exists());

        let finality_path = directory.path().join("finality-mismatch.jsonl");
        let mut finality_mismatch = opened(
            "run-finality-mismatch",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        finality_mismatch.trade_finality_policy_file_sha256 = sha('b');
        assert!(matches!(
            SimRunActorV2::create_new(
                &finality_path,
                finality_mismatch,
                Arc::clone(&rule_snapshot),
                Arc::clone(&finality_policy),
                Arc::clone(&scenario),
                Arc::clone(&latency_profile),
                accepting_projection_port(),
                MAX_GROUP_BYTES,
            ),
            Err(SimRunActorErrorV2::FinalityPolicyIdentityMismatch)
        ));
        assert!(!finality_path.exists());

        let scenario_path = directory.path().join("scenario-mismatch.jsonl");
        let mut scenario_mismatch = opened(
            "run-scenario-mismatch",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        scenario_mismatch.platform_scenario_file_sha256 = sha('c');
        assert!(matches!(
            SimRunActorV2::create_new(
                &scenario_path,
                scenario_mismatch,
                Arc::clone(&rule_snapshot),
                Arc::clone(&finality_policy),
                Arc::clone(&scenario),
                Arc::clone(&latency_profile),
                accepting_projection_port(),
                MAX_GROUP_BYTES,
            ),
            Err(SimRunActorErrorV2::ScenarioIdentityMismatch)
        ));
        assert!(!scenario_path.exists());

        let latency_path = directory.path().join("latency-mismatch.jsonl");
        let mut latency_mismatch = opened(
            "run-latency-mismatch",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        latency_mismatch.proxy_latency_profile_file_sha256 = sha('d');
        assert!(matches!(
            SimRunActorV2::create_new(
                &latency_path,
                latency_mismatch,
                rule_snapshot,
                finality_policy,
                scenario,
                latency_profile,
                accepting_projection_port(),
                MAX_GROUP_BYTES,
            ),
            Err(SimRunActorErrorV2::LatencyProfileIdentityMismatch)
        ));
        assert!(!latency_path.exists());
    }

    const REAL_GATE_A_FINAL: &str = "/home/chillchill/sim-system/.local-artifacts/data-pipeline-training/gate-a/bc2fdeddd1ea9a6c931823be498591ccaf32dfad61ed969f86c92b16ae3cc574";
    const REAL_CONDITION_ID: &str =
        "0xd4346bcf419856fa3261d3c0705597d5c909ead020b9f1a0de468092704fc65f";
    const REAL_UP_TOKEN_ID: &str =
        "45253098770861975037434792771412818336165758089047645530470012231811628219325";
    const REAL_DOWN_TOKEN_ID: &str =
        "13103176462561566402126211408478811357460751704260150146723274072578196298541";
    const REAL_TRADE_EVENT_ID: &str =
        "0ba3871e7c0b1b6356f2e70f679ff250a4fe1a7c343289711ea05cd06c69f830";

    #[derive(Default)]
    struct RealGateAVerticalCoverage {
        initial_tokens: [BTreeSet<String>; 2],
        tick_tokens: [BTreeSet<String>; 2],
        trade_count: [usize; 2],
        book_resync_count: [usize; 2],
        resync_boundary_count: [usize; 2],
        trading_state_count: [usize; 2],
        resolution_count: [usize; 2],
        first_initial_bid: Option<(String, Decimal)>,
        evidence_seen: [bool; 4],
        golden_initial: Vec<ExecutionMarketEventV3>,
        golden_resync: Option<ExecutionMarketEventV3>,
        golden_resolution: Option<ExecutionMarketEventV3>,
    }

    impl RealGateAVerticalCoverage {
        fn observe(
            &mut self,
            path: MarketEventPathV1,
            bucket: &ClosedExecutionMarketIndexBucketV1,
        ) {
            let index = usize::from(path == MarketEventPathV1::PublicVisibility);
            for event in bucket.events().iter() {
                assert_eq!(event.market.condition_id.0, REAL_CONDITION_ID);
                match path {
                    MarketEventPathV1::HiddenPlatform => assert_eq!(
                        event.time.source_event_at_utc_ms,
                        Some(bucket.index_at_utc_ms())
                    ),
                    MarketEventPathV1::PublicVisibility => {
                        assert_eq!(event.time.received_at_utc_ms, bucket.index_at_utc_ms())
                    }
                }
                if event.quality.resync == ResyncQuality::ResyncBoundary {
                    self.resync_boundary_count[index] += 1;
                }
                match &event.payload {
                    ExecutionMarketEventPayloadV3::MarketTradingState(_) => {
                        self.trading_state_count[index] += 1
                    }
                    ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                        self.initial_tokens[index].insert(snapshot.asset.token_id.0.clone());
                        if path == MarketEventPathV1::HiddenPlatform
                            && self.first_initial_bid.is_none()
                        {
                            self.first_initial_bid = snapshot.bids.first().map(|level| {
                                (
                                    snapshot.asset.token_id.0.clone(),
                                    level
                                        .price_pusd_per_share
                                        .0
                                        .parse()
                                        .expect("真实初始盘口价格必须可解析"),
                                )
                            });
                        }
                        self.evidence_seen[0] |= event.event_id.0
                            == "88595520314550562534ca587f82b31caf82c651025e5d7e652d5695c4e70628";
                        if path == MarketEventPathV1::HiddenPlatform
                            && matches!(
                                event.event_id.0.as_str(),
                                "3e3208011d6de4a8eb363cd546f9f035ad1da6f03ed32c0ceb3b383d5ab802fe"
                                    | "88595520314550562534ca587f82b31caf82c651025e5d7e652d5695c4e70628"
                            )
                        {
                            self.golden_initial.push(event.clone());
                        }
                    }
                    ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
                        self.tick_tokens[index].insert(change.asset.token_id.0.clone());
                        self.evidence_seen[1] |= event.event_id.0
                            == "b9893ed474c9c6fc688eadaf1626c45ba0f380508bd327a18d5f0588e007c338";
                    }
                    ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
                        assert!(!trade.trade_id.is_empty());
                        assert_eq!(
                            trade.trade_id_source,
                            PublicTradeIdSourceV1::DerivedSourceEvidence
                        );
                        self.trade_count[index] += 1;
                        if event.event_id.0 == REAL_TRADE_EVENT_ID {
                            assert_eq!(
                                trade.fee_rate_bps.as_ref().map(|fee| fee.0.as_str()),
                                Some("0")
                            );
                            self.evidence_seen[2] = true;
                        }
                    }
                    ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                        if transition.basis == LevelExecutionBasis::BookResyncDeltaInference =>
                    {
                        self.book_resync_count[index] += 1;
                        if path == MarketEventPathV1::HiddenPlatform
                            && event.event_id.0
                                == "c114c118d06b40f1bbca4a383fca55cf5b08ae8fffa8d083dd57e65e7389a355"
                        {
                            self.golden_resync = Some(event.clone());
                        }
                    }
                    ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => {
                        self.resolution_count[index] += 1;
                        self.evidence_seen[3] |= event.event_id.0
                            == "2e70d5e3f9c369d8ea6b585843b594b5b166b9a7ff0902ade7405635fdd000c3";
                        if path == MarketEventPathV1::HiddenPlatform {
                            self.golden_resolution = Some(event.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn real_gate_a_market() -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(REAL_CONDITION_ID.to_owned()),
                clob_market_id: ClobMarketId("3278040".to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 1_785_715_500_000,
                    end_at_utc_ms: 1_785_715_800_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(REAL_UP_TOKEN_ID.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(REAL_DOWN_TOKEN_ID.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("构造唯一代表市场")
    }

    fn real_gate_a_reference_market() -> ReferenceMarketSpecV1 {
        ReferenceMarketSpecV1 {
            condition_id: ConditionId(REAL_CONDITION_ID.to_owned()),
            clob_market_id: ClobMarketId("3278040".to_owned()),
            slug: "btc-updown-5m-1785715500".to_owned(),
            outcomes: vec![
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(REAL_UP_TOKEN_ID.to_owned()),
                    outcome: "Up".to_owned(),
                },
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(REAL_DOWN_TOKEN_ID.to_owned()),
                    outcome: "Down".to_owned(),
                },
            ],
        }
    }

    fn real_gate_a_bucket(
        path: MarketEventPathV1,
        bucket: &ClosedExecutionMarketIndexBucketV1,
    ) -> AcceptedClosedMarketBucketV2 {
        AcceptedClosedMarketBucketV2 {
            reference: DurableMarketBucketReferenceV2 {
                path,
                bucket_at_utc_ms: bucket.index_at_utc_ms(),
                start_index_position: bucket.start_logical_position(),
                next_index_position: bucket.next_logical_position(),
                closure: match bucket.closure() {
                    ReplayIndexClosureV1::NextGlobalEventAt(at) => {
                        crate::market::MillisecondBucketClosureV1::NextEventAt(at)
                    }
                    ReplayIndexClosureV1::EndOfInput => {
                        crate::market::MillisecondBucketClosureV1::EndOfInput
                    }
                },
            },
            events: Arc::clone(bucket.events()),
        }
    }

    fn real_gate_a_state_summary(execution: &SimExecution) -> String {
        let Some(state) = execution.actor.state.as_ref() else {
            return "actor_state=absent".to_owned();
        };
        let Some(session) = state.session.as_ref() else {
            return "actor_state=present,session=absent".to_owned();
        };
        format!(
            "public_phase={:?},hidden_phase={:?},public_levels={},hidden_levels={},execution_shape={:?},public_frontier={:?},hidden_frontier={:?},private_sequence={}",
            session
                .public_markets
                .lifecycle_phase_for_tests(REAL_CONDITION_ID),
            session
                .hidden_platform_markets
                .as_ref()
                .and_then(|markets| markets.lifecycle_phase_for_tests(REAL_CONDITION_ID)),
            session
                .public_markets
                .external_level_count_for_tests(REAL_CONDITION_ID),
            session
                .hidden_platform_markets
                .as_ref()
                .map_or(0, |markets| markets
                    .external_level_count_for_tests(REAL_CONDITION_ID)),
            session.execution_state.market_shape_for_tests(),
            session.public_visibility_frontier,
            session.hidden_platform_frontier,
            session.next_private_feedback_sequence,
        )
    }

    fn assert_real_gate_a_quantity(
        execution: &SimExecution,
        ordinal: u64,
        path: MarketEventPathV1,
        event_id: &str,
        at_utc_ms: i64,
        expected: (Decimal, Decimal, u128),
    ) {
        let (expected_public, expected_hidden, expected_queue) = expected;
        let session = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .unwrap();
        let price = Decimal::new(1, 2);
        let public = session.public_markets.external_quantity_for_tests(
            REAL_CONDITION_ID,
            REAL_UP_TOKEN_ID,
            BookSide::Bid,
            price,
        );
        let hidden = session
            .hidden_platform_markets
            .as_ref()
            .and_then(|markets| {
                markets.external_quantity_for_tests(
                    REAL_CONDITION_ID,
                    REAL_UP_TOKEN_ID,
                    BookSide::Bid,
                    price,
                )
            });
        let queue = session.execution_state.external_queue_base_units_for_tests(
            REAL_CONDITION_ID,
            REAL_UP_TOKEN_ID,
            BookSide::Bid,
            price,
        );
        if public != Some(expected_public)
            || hidden != Some(expected_hidden)
            || queue != expected_queue
        {
            panic!(
                "独立金标首分歧：ordinal={ordinal},path={path:?},event_id={event_id},time={at_utc_ms},expected=({expected_public},{expected_hidden},{expected_queue}),actual=({public:?},{hidden:?},{queue}),state={}",
                real_gate_a_state_summary(execution),
            );
        }
    }

    fn drive_real_gate_a_independent_golden(
        golden: &mut SimExecution,
        mut initial: Vec<ExecutionMarketEventV3>,
        resync: ExecutionMarketEventV3,
        resolution: ExecutionMarketEventV3,
    ) {
        initial.sort_by_key(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                snapshot.asset.token_id.0.clone()
            }
            _ => unreachable!(),
        });
        assert_eq!(
            initial
                .iter()
                .map(|event| event.event_id.0.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "3e3208011d6de4a8eb363cd546f9f035ad1da6f03ed32c0ceb3b383d5ab802fe",
                "88595520314550562534ca587f82b31caf82c651025e5d7e652d5695c4e70628",
            ])
        );
        let up_initial = initial
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot)
                    if snapshot.asset.token_id.0 == REAL_UP_TOKEN_ID =>
                {
                    Some(snapshot)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            (
                initial[0].time.source_event_at_utc_ms,
                initial[0].time.received_at_utc_ms,
                up_initial.initial_tick_size_pusd_per_share.0.as_str(),
                up_initial
                    .bids
                    .iter()
                    .find(|level| level.price_pusd_per_share.0 == "0.01")
                    .unwrap()
                    .quantity_shares
                    .0
                    .as_str(),
            ),
            (
                Some(1_785_715_201_804),
                1_785_715_201_871,
                "0.01",
                "100040.33",
            )
        );
        let ExecutionMarketEventPayloadV3::LevelExecutionTransition(real_resync) = &resync.payload
        else {
            unreachable!()
        };
        assert_eq!(
            (
                resync.event_id.0.as_str(),
                resync.time.source_event_at_utc_ms,
                resync.time.received_at_utc_ms,
                real_resync.asset.token_id.0.as_str(),
                real_resync.price_pusd_per_share.0.as_str(),
                real_resync.quantity_before_shares.0.as_str(),
                real_resync.quantity_after_shares.0.as_str(),
                real_resync.basis,
                resync.quality.resync,
            ),
            (
                "c114c118d06b40f1bbca4a383fca55cf5b08ae8fffa8d083dd57e65e7389a355",
                Some(1_785_715_387_835),
                1_785_715_387_843,
                REAL_UP_TOKEN_ID,
                "0.01",
                "99213.87",
                "99193.87",
                LevelExecutionBasis::BookResyncDeltaInference,
                ResyncQuality::ResyncBoundary,
            )
        );
        assert_eq!(
            (
                resolution.event_id.0.as_str(),
                resolution.time.source_event_at_utc_ms,
                resolution.time.received_at_utc_ms,
            ),
            (
                "2e70d5e3f9c369d8ea6b585843b594b5b166b9a7ff0902ade7405635fdd000c3",
                Some(1_785_715_888_402),
                1_785_715_888_411,
            )
        );
        let mut bridge = resync.clone();
        bridge.quality.resync = ResyncQuality::NotRequired;
        let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) =
            &mut bridge.payload
        else {
            unreachable!()
        };
        transition.causal_group_id = sha('4');
        transition.quantity_before_shares = ExactDecimal("100040.33".to_owned());
        transition.quantity_after_shares = real_resync.quantity_before_shares.clone();
        transition.attributed_trade_quantity_shares = ExactDecimal("0".to_owned());
        transition.already_covered_quantity_shares = ExactDecimal("0".to_owned());
        transition.net_added_quantity_shares = ExactDecimal("0".to_owned());
        transition.net_reduced_quantity_shares = ExactDecimal("826.46".to_owned());
        transition.queue_consumption_quantity_shares = ExactDecimal("826.46".to_owned());
        transition.basis = LevelExecutionBasis::LevelDeltaInference;
        bridge.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&bridge)
            .expect("重建独立金标桥接事件身份");
        let bridge = data_pipeline_core::decode_execution_market_event_v3(
            &data_pipeline_core::encode_execution_market_event_v3(&bridge)
                .expect("编码独立金标桥接事件"),
        )
        .expect("从真实边界解码独立金标桥接事件");
        let initial: Arc<[ExecutionMarketEventV3]> = Arc::from(initial);
        let resync: Arc<[ExecutionMarketEventV3]> = Arc::from(vec![bridge, resync]);
        let resolution: Arc<[ExecutionMarketEventV3]> = Arc::from(vec![resolution]);
        let buckets = [
            (
                MarketEventPathV1::HiddenPlatform,
                1_785_715_201_804,
                0,
                2,
                crate::market::MillisecondBucketClosureV1::NextEventAt(1_785_715_387_835),
                Arc::clone(&initial),
            ),
            (
                MarketEventPathV1::PublicVisibility,
                1_785_715_201_871,
                0,
                2,
                crate::market::MillisecondBucketClosureV1::NextEventAt(1_785_715_387_843),
                initial,
            ),
            (
                MarketEventPathV1::HiddenPlatform,
                1_785_715_387_835,
                2,
                4,
                crate::market::MillisecondBucketClosureV1::NextEventAt(1_785_715_888_402),
                Arc::clone(&resync),
            ),
            (
                MarketEventPathV1::PublicVisibility,
                1_785_715_387_843,
                2,
                4,
                crate::market::MillisecondBucketClosureV1::NextEventAt(1_785_715_888_411),
                resync,
            ),
        ];
        for (ordinal, (path, at, start, next, closure, events)) in buckets.into_iter().enumerate() {
            let event_id = events[0].event_id.0.clone();
            golden
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: DurableMarketBucketReferenceV2 {
                        path,
                        bucket_at_utc_ms: at,
                        start_index_position: start,
                        next_index_position: next,
                        closure,
                    },
                    events,
                })
                .unwrap_or_else(|error| {
                    panic!(
                        "独立金标驱动失败：ordinal={},path={path:?},event_id={event_id},time={at},error={error}",
                        ordinal + 1
                    )
                });
            let (public, hidden, queue) = match ordinal {
                0 => (Decimal::ZERO, Decimal::new(10_004_033, 2), 100_040_330_000),
                1 => (
                    Decimal::new(10_004_033, 2),
                    Decimal::new(10_004_033, 2),
                    100_040_330_000,
                ),
                2 => (
                    Decimal::new(10_004_033, 2),
                    Decimal::new(9_919_387, 2),
                    99_193_870_000,
                ),
                3 => (
                    Decimal::new(9_919_387, 2),
                    Decimal::new(9_919_387, 2),
                    99_193_870_000,
                ),
                _ => unreachable!(),
            };
            assert_real_gate_a_quantity(
                golden,
                u64::try_from(ordinal + 1).unwrap(),
                path,
                &event_id,
                at,
                (public, hidden, queue),
            );
        }
        let required = golden
            .next_required_logical_drive()
            .expect("独立金标必须暴露 market end 必要义务");
        assert_eq!(required.path, MarketEventPathV1::HiddenPlatform);
        assert_eq!(required.required_at_utc_ms, 1_785_715_800_000);
        golden
            .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: required.path,
                closed_through_utc_ms: required.required_at_utc_ms,
                next_index_position: 4,
            })
            .expect("独立金标推进唯一必要空前缀");
        for (path, at) in [
            (MarketEventPathV1::HiddenPlatform, 1_785_715_888_402),
            (MarketEventPathV1::PublicVisibility, 1_785_715_888_411),
        ] {
            golden
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: DurableMarketBucketReferenceV2 {
                        path,
                        bucket_at_utc_ms: at,
                        start_index_position: 4,
                        next_index_position: 5,
                        closure: crate::market::MillisecondBucketClosureV1::EndOfInput,
                    },
                    events: Arc::clone(&resolution),
                })
                .unwrap_or_else(|error| panic!("独立金标 resolution 失败：{error}"));
        }
        let session = golden
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert!(session.public_markets.all_qualified_resolutions_terminal());
        assert!(
            session
                .hidden_platform_markets
                .as_ref()
                .unwrap()
                .all_qualified_resolutions_terminal()
        );
    }

    fn real_gate_a_post_response<'a>(
        execution: &'a SimExecution,
        request_id: &str,
    ) -> Option<&'a PostOrderDirectResponseV2> {
        let response = execution
            .actor
            .state
            .as_ref()?
            .session
            .as_ref()?
            .mutation_requests
            .requests_by_id
            .get(request_id)?
            .completed_response
            .as_ref()?;
        match response.as_ref() {
            StoredDirectResponse::Final(BusinessDirectResponse::PostOrder(response)) => {
                Some(response)
            }
            _ => None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires frozen Gate A V2 final and official CURRENT_QUERY endpoints"]
    async fn frozen_gate_a_representative_market_matches_golden_dual_time_execution_vertical() {
        let directory = tempfile::tempdir().expect("创建真实纵向临时目录");
        let implementation_identity = ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: sha('5'),
            cargo_lock_sha256: sha('6'),
        };
        let rule_snapshot =
            query_and_seal_current_process_snapshot_v3(CurrentProcessSnapshotRequestV1 {
                reference_market: real_gate_a_reference_market(),
                target_path: directory.path().join("current-process-rules.json"),
                implementation_identity: implementation_identity.clone(),
            })
            .await
            .expect("唯一 CURRENT_QUERY 必须密封进程规则快照");
        assert_eq!(
            rule_snapshot.access_mode(),
            crate::platform_rules::SnapshotAccessMode::CurrentQuery
        );

        let final_gate_a = open_published_gate_a_final(Path::new(REAL_GATE_A_FINAL))
            .expect("只读打开冻结 Gate A 产物");
        let output = gate_a_formal_output_config();
        let mut adapter = GateASimulationReplayAdapter::open(
            &final_gate_a,
            output.sort.batch_rows,
            output.sort.max_run_bytes,
            output.sort.batch_rows,
        )
        .expect("用正式输出配置打开联合适配器");
        assert_eq!(adapter.max_page_rows(), output.sort.batch_rows);
        let canonical_manifest = adapter.canonical_event_store_manifest_sha256().clone();
        let event_time_manifest = adapter.event_time_index_manifest_sha256().clone();
        let visibility_manifest = adapter.visibility_index_manifest_sha256().clone();

        let scenario = sealed_scenario_for_markets(vec![real_gate_a_market()]);
        let finality_policy = successful_policy_v2_for_tests();
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let mut run_opened = opened(
            "gate-a-real-vertical",
            &rule_snapshot,
            &finality_policy,
            &scenario,
            &latency_profile,
        );
        run_opened.implementation_identity = implementation_identity;
        run_opened.market_event_source_identity_sha256 = Some(canonical_manifest.clone());
        assert_eq!(
            run_opened.platform_rule_snapshot_file_sha256,
            *rule_snapshot.file_sha256()
        );
        let mut golden_opened = run_opened.clone();
        golden_opened.market_event_source_identity_sha256 = Some(sha('9'));
        assert_ne!(
            run_opened.market_event_source_identity_sha256,
            golden_opened.market_event_source_identity_sha256
        );
        let mut real = SimExecution::create_new(
            &directory.path().join("real.jsonl"),
            run_opened,
            Arc::clone(&rule_snapshot),
            Arc::clone(&finality_policy),
            Arc::clone(&scenario),
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建真实回放执行根");
        let mut golden = SimExecution::create_new(
            &directory.path().join("golden.jsonl"),
            golden_opened,
            Arc::clone(&rule_snapshot),
            finality_policy,
            scenario,
            Arc::clone(&latency_profile),
            accepting_projection_port(),
            MAX_GROUP_BYTES,
        )
        .expect("创建同语义金标执行根");
        assert!(Arc::ptr_eq(
            &real.actor._rule_snapshot,
            &golden.actor._rule_snapshot
        ));
        for execution in [&mut real, &mut golden] {
            let mut session = spec("gate-a-real-vertical", "create-gate-a-real-vertical");
            session.initial_account_state.available_pusd = ExactDecimal("1000".to_owned());
            execution.create_session(session).expect("创建真实纵向会话");
            execution
                .activate_session("activate-gate-a-real-vertical".to_owned())
                .expect("激活真实纵向会话");
        }
        let mut platform = adapter.next_platform_bucket().expect("读取平台首桶");
        let mut visibility = adapter.next_visibility_bucket().expect("读取可见性首桶");
        let mut platform_done = platform.is_none();
        let mut visibility_done = visibility.is_none();
        let mut platform_eof = false;
        let mut visibility_eof = false;
        let mut ordinal = 0_u64;
        let mut coverage = RealGateAVerticalCoverage::default();
        let mut independent_trading_state = false;
        let mut queue_observed = false;
        let mut order_id = None;
        let minimum_order_size = rule_snapshot
            .synchronized_rules()
            .minimum_order_size_shares
            .value();
        const REQUEST_ID: &str = "gate-a-real-vertical-post";

        while platform.is_some() || visibility.is_some() {
            let next_bucket_at = match (&platform, &visibility) {
                (Some(left), Some(right)) => left.index_at_utc_ms().min(right.index_at_utc_ms()),
                (Some(bucket), None) | (None, Some(bucket)) => bucket.index_at_utc_ms(),
                (None, None) => unreachable!(),
            };
            let real_required = real.next_required_logical_drive();
            if let Some(required) = real_required
                && required.required_at_utc_ms < next_bucket_at
            {
                let next_position = match required.path {
                    MarketEventPathV1::HiddenPlatform => platform.as_ref().map_or_else(
                        || adapter.platform_next_logical_position(),
                        ClosedExecutionMarketIndexBucketV1::start_logical_position,
                    ),
                    MarketEventPathV1::PublicVisibility => visibility.as_ref().map_or_else(
                        || adapter.visibility_next_logical_position(),
                        ClosedExecutionMarketIndexBucketV1::start_logical_position,
                    ),
                };
                let advance = PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position: next_position,
                };
                real.drive_platform_prefix(advance)
                    .expect("真实根推进必要空前缀");
                ordinal += 1;
                continue;
            }

            let path = match (&platform, &visibility) {
                (Some(left), Some(right)) if left.index_at_utc_ms() <= right.index_at_utc_ms() => {
                    MarketEventPathV1::HiddenPlatform
                }
                (Some(_), None) => MarketEventPathV1::HiddenPlatform,
                _ => MarketEventPathV1::PublicVisibility,
            };
            let bucket = match path {
                MarketEventPathV1::HiddenPlatform => platform.take().unwrap(),
                MarketEventPathV1::PublicVisibility => visibility.take().unwrap(),
            };
            coverage.observe(path, &bucket);
            let is_order_bucket = path == MarketEventPathV1::HiddenPlatform
                && bucket
                    .events()
                    .iter()
                    .any(|event| event.event_id.0 == REAL_TRADE_EVENT_ID);
            let is_representative_resync = bucket.events().iter().any(|event| {
                event.event_id.0
                    == "c114c118d06b40f1bbca4a383fca55cf5b08ae8fffa8d083dd57e65e7389a355"
            });
            if is_order_bucket {
                let outbound_delay = i64::try_from(latency_profile.sample_delay_ms(
                    sha('8').0.as_bytes(),
                    REQUEST_ID,
                    DelaySampleDomainV1::RequestOutbound,
                ))
                .expect("请求时延可表示");
                assert_eq!(outbound_delay, 11);
                let receipt = real
                    .post_order(
                        request_cutoff(bucket.index_at_utc_ms() - outbound_delay),
                        PostOrderRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: REQUEST_ID.to_owned(),
                                caller_correlation_id: Some("gate-a-real-vertical".to_owned()),
                            },
                            item: post_item(
                                "gate-a-real-vertical-item",
                                &ConditionId(REAL_CONDITION_ID.to_owned()),
                                &TokenId(REAL_UP_TOKEN_ID.to_owned()),
                                SimBuyOrderKindV1::GtcLimit {
                                    limit_price_pusd_per_share: "0.01".parse().unwrap(),
                                    size_shares: minimum_order_size,
                                    post_only: false,
                                },
                            ),
                        },
                    )
                    .expect("真实代表订单登记");
                assert_eq!(receipt.platform_arrived_at_utc_ms(), 1_785_715_202_580);
            }

            let accepted = real_gate_a_bucket(path, &bucket);
            assert!(Arc::ptr_eq(&accepted.events, bucket.events()));
            real.drive_closed_market_bucket(accepted).unwrap_or_else(|error| {
                let event_ids = bucket
                    .events()
                    .iter()
                    .map(|event| event.event_id.0.as_str())
                    .collect::<Vec<_>>();
                panic!(
                    "真实回放首分歧：ordinal={},path={path:?},event_ids={event_ids:?},time={},error={error},state={}",
                    ordinal + 1,
                    bucket.index_at_utc_ms(),
                    real_gate_a_state_summary(&real),
                )
            });
            ordinal += 1;

            if path == MarketEventPathV1::PublicVisibility && is_representative_resync {
                assert_real_gate_a_quantity(
                    &real,
                    ordinal,
                    path,
                    "c114c118d06b40f1bbca4a383fca55cf5b08ae8fffa8d083dd57e65e7389a355",
                    bucket.index_at_utc_ms(),
                    (
                        Decimal::new(9_919_387, 2),
                        Decimal::new(9_919_387, 2),
                        99_193_870_000,
                    ),
                );
            }

            if path == MarketEventPathV1::HiddenPlatform {
                let session = real.actor.state.as_ref().unwrap().session.as_ref().unwrap();
                let hidden = session.hidden_platform_markets.as_ref().unwrap();
                independent_trading_state |= hidden != &session.public_markets;
                if let Some((token_id, price)) = &coverage.first_initial_bid {
                    queue_observed |= session.execution_state.external_queue_base_units_for_tests(
                        REAL_CONDITION_ID,
                        token_id,
                        BookSide::Bid,
                        *price,
                    ) > 0;
                }
                if is_order_bucket {
                    let real_response = real_gate_a_post_response(&real, REQUEST_ID)
                        .expect("真实代表订单必须形成私有响应");
                    let SubmissionItemOutcomeV2::Accepted(accepted) = &real_response.item.outcome
                    else {
                        panic!("真实订单必须被有效接纳：{:?}", real_response.item.outcome);
                    };
                    assert_eq!(
                        real_response.metadata.response_received_at_utc_ms,
                        1_785_715_202_588
                    );
                    let public_closed = session
                        .public_visibility_frontier
                        .closed_through_utc_ms
                        .unwrap_or(i64::MIN);
                    assert!(public_closed < 1_785_715_202_623);
                    order_id = Some(accepted.platform_order_id.clone());
                }
            }

            match (path, bucket.closure()) {
                (MarketEventPathV1::HiddenPlatform, ReplayIndexClosureV1::EndOfInput) => {
                    platform_eof = true
                }
                (MarketEventPathV1::PublicVisibility, ReplayIndexClosureV1::EndOfInput) => {
                    visibility_eof = true
                }
                _ => {}
            }
            match path {
                MarketEventPathV1::HiddenPlatform if !platform_done => {
                    platform = adapter.next_platform_bucket().expect("读取平台下一桶");
                    platform_done = platform.is_none();
                }
                MarketEventPathV1::PublicVisibility if !visibility_done => {
                    visibility = adapter.next_visibility_bucket().expect("读取可见性下一桶");
                    visibility_done = visibility.is_none();
                }
                _ => {}
            }
        }

        drive_real_gate_a_independent_golden(
            &mut golden,
            std::mem::take(&mut coverage.golden_initial),
            coverage.golden_resync.take().expect("真实代表 resync 存在"),
            coverage
                .golden_resolution
                .take()
                .expect("真实代表 resolution 存在"),
        );
        assert!(platform_done && visibility_done && platform_eof && visibility_eof);
        let expected_tokens =
            BTreeSet::from([REAL_UP_TOKEN_ID.to_owned(), REAL_DOWN_TOKEN_ID.to_owned()]);
        assert_eq!(
            coverage.initial_tokens,
            [expected_tokens.clone(), expected_tokens.clone()]
        );
        assert_eq!(
            coverage.tick_tokens,
            [expected_tokens.clone(), expected_tokens]
        );
        assert_eq!(coverage.trade_count, [1_918, 1_918]);
        assert_eq!(coverage.book_resync_count, [362, 362]);
        assert_eq!(coverage.resync_boundary_count, [362, 362]);
        assert_eq!(coverage.trading_state_count, [0, 1]);
        assert_eq!(coverage.resolution_count, [1, 1]);
        assert!(coverage.evidence_seen.iter().all(|seen| *seen));
        assert!(independent_trading_state && queue_observed);
        let order_id = order_id.expect("真实路径必须完成一个有效订单响应");
        let session = real.actor.state.as_ref().unwrap().session.as_ref().unwrap();
        assert_eq!(
            session
                .execution_state
                .base_order_quantities_for_tests(&order_id)
                .map(|state| state.2),
            Some(0),
            "市场 resolution 后有效订单必须走完生命周期"
        );
        assert!(session.public_markets.all_qualified_resolutions_terminal());
        assert!(
            session
                .hidden_platform_markets
                .as_ref()
                .unwrap()
                .all_qualified_resolutions_terminal()
        );
        eprintln!(
            "Gate A real vertical PASS: store={},event_time={},visibility={},buckets={ordinal},trades={},book_resync={},resync_boundaries={}",
            canonical_manifest.0,
            event_time_manifest.0,
            visibility_manifest.0,
            coverage.trade_count[0],
            coverage.book_resync_count[0],
            coverage.resync_boundary_count[0],
        );
    }
    struct TradeFactsCapturePort {
        inner: Box<dyn BusinessProjectionPortV2>,
        accepted: Rc<RefCell<Vec<Arc<BusinessProjectionBatchPayloadV1>>>>,
        reject_once: bool,
    }
    impl BusinessProjectionPortV2 for TradeFactsCapturePort {
        fn try_accept(&mut self, _: Arc<BusinessProjectionBatchV2>) -> bool {
            panic!("实际待交付路径必须调用版本载荷入口")
        }
        fn try_accept_payload(&mut self, payload: Arc<BusinessProjectionBatchPayloadV1>) -> bool {
            if self.reject_once {
                self.reject_once = false;
                return false;
            }
            if !self.inner.try_accept_payload(Arc::clone(&payload)) {
                return false;
            }
            self.accepted.borrow_mut().push(payload);
            true
        }
        fn durable_through(&self, run_id: &str) -> u64 {
            self.inner.durable_through(run_id)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trade_facts_v3_public_delivery_durable_rebuild_and_legacy_equivalence() {
        trade_facts_with_order_evidence(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn order_quantity_v3_public_delivery_durable_rebuild_and_legacy_equivalence() {
        trade_facts_with_order_evidence(true).await;
    }

    async fn trade_facts_with_order_evidence(measured: bool) {
        use crate::runtime_v2::BusinessProjectionVersionV1;
        use contracts::execution_normalization::{
            DurableSourceReferenceV1, ImmutableSourceReferenceV1,
        };
        use execution_normalization::{NormalizationRecordV2, SourceNeutralEvidenceKindV2};
        let read_result = |reference: &contracts::execution_normalization::NoeResultReferenceV1| {
            if measured {
                let new = execution_normalization::read_noe_result_v3(reference).unwrap();
                execution_normalization::ResolvedNoeResultV2 {
                    context: new.context,
                    boundary: new.boundary,
                    record: serde_json::from_value(serde_json::to_value(new.record).unwrap())
                        .unwrap(),
                }
            } else {
                execution_normalization::read_noe_result_v2(reference).unwrap()
            }
        };
        let mut legacy_feedback = Vec::new();
        let mut legacy_identities = Vec::new();
        let mut legacy_economics = Vec::new();
        for version in [
            BusinessProjectionVersionV1::V2,
            BusinessProjectionVersionV1::V3,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2);
            let root = directory.path().join("noe");
            let rules = successful_snapshot_v3_for_sim_tests().await;
            let session_spec = {
                let mut value = spec("trade-facts", "create-trade-facts");
                value.caller_role = SimCallerRoleV1::Paper;
                if measured {
                    value.initial_account_state.available_pusd = ExactDecimal("2000".into());
                }
                value
            };
            let account_key = contracts::AccountInstanceKeyV1 {
                run_id: session_spec.run_id.clone(),
                caller_role: session_spec.caller_role,
                caller_instance_id: session_spec.caller_instance_id.clone(),
                account_id: session_spec.account_id.clone(),
                platform_source: contracts::PlatformSourceV1::Simulated,
            };
            let policy_bytes = serde_json::to_vec(&serde_json::json!({
                "schema_version": "trade_finality_policy_v2",
                "normal_path": {"steps": [
                    {"state":"MATCHED_NOT_BROADCASTED","offset_ms":0},
                    {"state":"MATCHED","offset_ms":10},
                    {"state":"MINED","offset_ms":20},
                    {"state":"CONFIRMED","offset_ms":30}], "economic_application_ordinal":2},
                "failure_overrides":[{"selector":{"selector_domain":"FILL","account_instance_key":account_key,"fill_sequence":2},
                    "path":{"steps":[{"state":"MATCHED_NOT_BROADCASTED","offset_ms":0},{"state":"MATCHED","offset_ms":10},{"state":"FAILED","offset_ms":20}],"economic_application_ordinal":null}}]
            })).unwrap();
            let policy = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&policy_bytes).unwrap());
            let market = market_with_window('1', "123", "1", "2", 1_000, 100_000);
            let scenario = sealed_scenario_for_markets(vec![market.clone()]);
            let latency = load_embedded_proxy_latency_profile().unwrap();
            let mut run_opened = opened("trade-facts", &rules, &policy, &scenario, &latency);
            run_opened.durability_role = RunDurabilityRoleV2::RecoverablePaper;
            run_opened.projection_version = version;
            let evidence = DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(path.to_string_lossy().into_owned()),
            };
            let (inner, observation): (Box<dyn BusinessProjectionPortV2>, _) = match version {
                BusinessProjectionVersionV1::V2 => {
                    let port = SimV2NoeProjectionPortV1::new(
                        &root,
                        run_opened.clone(),
                        evidence.clone(),
                        evidence.clone(),
                        "opening".into(),
                        "trade-facts-test".into(),
                    );
                    let observation = port.observation();
                    (Box::new(port), observation)
                }
                BusinessProjectionVersionV1::V3 => {
                    if measured {
                        let port = SimV2NoeProjectionPortV3::new(
                            &root,
                            run_opened.clone(),
                            evidence.clone(),
                            evidence.clone(),
                            "opening".into(),
                            "trade-facts-test".into(),
                        );
                        let observation = port.observation();
                        (Box::new(port), observation)
                    } else {
                        let port = SimV2NoeProjectionPortV2::new(
                            &root,
                            run_opened.clone(),
                            evidence.clone(),
                            evidence.clone(),
                            "opening".into(),
                            "trade-facts-test".into(),
                        );
                        let observation = port.observation();
                        (Box::new(port), observation)
                    }
                }
                BusinessProjectionVersionV1::V4 => unreachable!("此测试逐项验证原 V2/V3 兼容"),
            };
            let accepted = Rc::new(RefCell::new(Vec::new()));
            let mut execution = SimExecution::create_new(
                &path,
                run_opened.clone(),
                Arc::clone(&rules),
                Arc::clone(&policy),
                Arc::clone(&scenario),
                Arc::clone(&latency),
                Box::new(TradeFactsCapturePort {
                    inner,
                    accepted: Rc::clone(&accepted),
                    reject_once: true,
                }),
                MAX_GROUP_BYTES,
            )
            .unwrap();
            execution.create_session(session_spec).unwrap();
            assert!(
                !execution.actor.pending_projections.is_empty(),
                "端口拒收必须保留原待交付批次"
            );
            execution.activate_session("activate".into()).unwrap();
            let mut events =
                decoded_initial_events_for_market_with_quantity(&market, 1_100, 1_110, 1, "5");
            if let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) =
                &mut events[0].payload
            {
                snapshot.asks[0].price_pusd_per_share = ExactDecimal("0.5".into());
                let mut second = snapshot.asks[0].clone();
                second.price_pusd_per_share = ExactDecimal("0.6".into());
                if measured {
                    second.quantity_shares = ExactDecimal("5.005".into());
                }
                snapshot.asks.push(second);
            } else {
                panic!("真实初始盘口");
            }
            if measured {
                if let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) =
                    &mut events[1].payload
                {
                    snapshot.asks[0].price_pusd_per_share = ExactDecimal("0.5".into());
                    snapshot.asks[0].quantity_shares = ExactDecimal("10".into());
                }
                events[1].event_id =
                    data_pipeline_core::derive_execution_market_event_v3_id(&events[1]).unwrap();
            }
            events[0].event_id =
                data_pipeline_core::derive_execution_market_event_v3_id(&events[0]).unwrap();
            let events: Arc<[ExecutionMarketEventV3]> = Arc::from(
                events
                    .into_iter()
                    .map(|event| {
                        data_pipeline_core::decode_execution_market_event_v3(
                            &data_pipeline_core::encode_execution_market_event_v3(&event).unwrap(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
            let reference = DurableMarketBucketReferenceV2 {
                path: MarketEventPathV1::PublicVisibility,
                bucket_at_utc_ms: 1_100,
                start_index_position: 0,
                next_index_position: 2,
                closure: MillisecondBucketClosureV1::NextEventAt(200_000),
            };
            execution
                .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                    reference: reference.clone(),
                    events: Arc::clone(&events),
                })
                .unwrap();
            execution
                .post_order(
                    request_cutoff(10_000),
                    PostOrderRequestV2 {
                        context: RequestContextV2 {
                            global_request_id: "two-fills".into(),
                            caller_correlation_id: None,
                        },
                        item: post_item(
                            "two-fills-item",
                            &market.market().condition_id,
                            &TokenId("1".into()),
                            SimBuyOrderKindV1::GtcLimit {
                                limit_price_pusd_per_share: "0.6".parse().unwrap(),
                                size_shares: if measured { "10.01" } else { "10" }.parse().unwrap(),
                                post_only: false,
                            },
                        ),
                    },
                )
                .unwrap();
            for _ in 0..32 {
                let Some(required) = execution.next_required_logical_drive() else {
                    break;
                };
                if required.required_at_utc_ms > 20_000 {
                    break;
                }
                execution
                    .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                        path: required.path,
                        closed_through_utc_ms: required.required_at_utc_ms,
                        next_index_position: 2,
                    })
                    .unwrap();
            }
            if measured {
                execution
                    .post_order(
                        request_cutoff(15_000),
                        PostOrderRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: "improved-budget".into(),
                                caller_correlation_id: None,
                            },
                            item: post_item(
                                "improved-fak",
                                &market.market().condition_id,
                                &TokenId("2".into()),
                                SimBuyOrderKindV1::FakMarket {
                                    spend_pusd: "3".parse().unwrap(),
                                    worst_price_pusd_per_share: "0.6".parse().unwrap(),
                                },
                            ),
                        },
                    )
                    .unwrap();
                for _ in 0..32 {
                    let Some(required) = execution.next_required_logical_drive() else {
                        break;
                    };
                    if required.required_at_utc_ms > 20_000 {
                        break;
                    }
                    execution
                        .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                            path: required.path,
                            closed_through_utc_ms: required.required_at_utc_ms,
                            next_index_position: 2,
                        })
                        .unwrap();
                }
                execution
                    .post_orders(
                        request_cutoff(20_000),
                        PostOrdersRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: "quantity-batch".into(),
                                caller_correlation_id: None,
                            },
                            items: vec![
                                PostOrderBatchItemV2 {
                                    batch_item_ordinal: 0,
                                    item: post_item(
                                        "rejected-fak",
                                        &market.market().condition_id,
                                        &TokenId("2".into()),
                                        SimBuyOrderKindV1::FakMarket {
                                            spend_pusd: "3".parse().unwrap(),
                                            worst_price_pusd_per_share: "0.6".parse().unwrap(),
                                        },
                                    ),
                                },
                                PostOrderBatchItemV2 {
                                    batch_item_ordinal: 1,
                                    item: post_item(
                                        "partial-base",
                                        &market.market().condition_id,
                                        &TokenId("2".into()),
                                        SimBuyOrderKindV1::GtcLimit {
                                            size_shares: "7".parse().unwrap(),
                                            limit_price_pusd_per_share: "0.6".parse().unwrap(),
                                            post_only: false,
                                        },
                                    ),
                                },
                                PostOrderBatchItemV2 {
                                    batch_item_ordinal: 2,
                                    item: post_item(
                                        "zero-matched",
                                        &market.market().condition_id,
                                        &TokenId("2".into()),
                                        SimBuyOrderKindV1::GtcLimit {
                                            size_shares: "7".parse().unwrap(),
                                            limit_price_pusd_per_share: "0.6".parse().unwrap(),
                                            post_only: false,
                                        },
                                    ),
                                },
                            ],
                        },
                    )
                    .unwrap();
                for _ in 0..32 {
                    let Some(required) = execution.next_required_logical_drive() else {
                        break;
                    };
                    if required.required_at_utc_ms > 25_000 {
                        break;
                    }
                    execution
                        .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                            path: required.path,
                            closed_through_utc_ms: required.required_at_utc_ms,
                            next_index_position: 2,
                        })
                        .unwrap();
                }
                execution
                    .order(
                        request_cutoff(25_000),
                        OrderRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: "quantity-order".into(),
                                caller_correlation_id: None,
                            },
                            platform_order_id: "sim-3".into(),
                        },
                    )
                    .unwrap();
                execution
                    .orders(
                        request_cutoff(25_000),
                        OrdersRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: "quantity-orders".into(),
                                caller_correlation_id: None,
                            },
                            filter: OrdersFilterV1::default(),
                            cursor: None,
                        },
                    )
                    .unwrap();
                for _ in 0..16 {
                    let Some(required) = execution.next_required_logical_drive() else {
                        break;
                    };
                    if required.required_at_utc_ms > 30_000 {
                        break;
                    }
                    execution
                        .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                            path: required.path,
                            closed_through_utc_ms: required.required_at_utc_ms,
                            next_index_position: 2,
                        })
                        .unwrap();
                }
            }
            assert!(execution.actor.pending_projections.is_empty());

            let mut feedback = Vec::new();
            let mut new_feedback = Vec::new();
            for (index, payload) in accepted.borrow().iter().enumerate() {
                assert_eq!(payload.sequence(), index as u64 + 1);
                match payload.as_ref() {
                    BusinessProjectionBatchPayloadV1::V4(_) => unreachable!("原版本兼容夹具"),
                    BusinessProjectionBatchPayloadV1::V2(batch) => {
                        feedback.extend(batch.trade_lifecycle.clone())
                    }
                    BusinessProjectionBatchPayloadV1::V3(batch) => {
                        new_feedback.extend(batch.trade_lifecycle.clone());
                        feedback.extend(
                            batch
                                .trade_lifecycle
                                .iter()
                                .cloned()
                                .map(contracts::TradeLifecycleFeedbackV3::into_v2),
                        );
                    }
                }
            }
            assert_eq!(
                feedback.len(),
                if measured { 15 } else { 7 },
                "两笔同刻成交分别成功／失败的全部生命周期"
            );
            if version == BusinessProjectionVersionV1::V2 {
                legacy_feedback = feedback;
                for reference in observation.result_references() {
                    let record = execution_normalization::read_noe_result_v1(&reference)
                        .unwrap()
                        .record;
                    legacy_identities.push((
                        record.logical_identity().clone(),
                        record.source_order().clone(),
                    ));
                    if let Some(fact) = record.economic_fact() {
                        legacy_economics.push((
                            fact.fact_reference.clone(),
                            fact.logical_identity.clone(),
                            fact.economic_identity.clone(),
                            contracts::economics::SourceNeutralEconomicDispositionV2::from(
                                fact.disposition.clone(),
                            ),
                            fact.source_reference.watermark.clone(),
                            fact.source_reference.record.clone(),
                        ));
                    }
                }
                continue;
            }
            assert_eq!(
                feedback, legacy_feedback,
                "新载荷不能改变旧字段、序号、时间或经济事实"
            );
            let first: Vec<_> = new_feedback
                .iter()
                .filter(|f| {
                    f.status.known
                        == Some(contracts::SimulatedTradeKnownStatusV2::MatchedNotBroadcasted)
                })
                .collect();
            assert_eq!(first.len(), if measured { 4 } else { 2 });
            assert_eq!(first[0].matched_at_utc_ms, first[1].matched_at_utc_ms);
            assert_eq!((first[0].fill_sequence, first[1].fill_sequence), (1, 2));
            assert!(
                new_feedback
                    .iter()
                    .any(|f| f.economic_application_account_transition_seq.is_some())
            );
            assert!(
                new_feedback
                    .iter()
                    .any(|f| f.economic_application_account_transition_seq.is_none())
            );
            for current in &new_feedback {
                let initial = first
                    .iter()
                    .find(|f| f.trade_id == current.trade_id)
                    .unwrap();
                assert_eq!(current.fill_sequence, initial.fill_sequence);
                assert_eq!(current.matched_at_utc_ms, initial.matched_at_utc_ms);
            }
            let mut identities = Vec::new();
            let mut economics = Vec::new();
            let mut matched_records = 0;
            let mut first_resolved = None;
            for result_ref in observation.result_references() {
                let result = read_result(&result_ref);
                first_resolved.get_or_insert_with(|| result.clone());
                identities.push((
                    result.record.logical_identity().clone(),
                    result.record.source_order().clone(),
                ));
                // 旧 Economic 来源仍产生 V1；V3 成交扩展不改写其经济载荷版本。
                if let NormalizationRecordV2::EconomicFact { fact, .. } = &result.record {
                    economics.push((
                        fact.fact_reference.clone(),
                        fact.logical_identity.clone(),
                        fact.economic_identity.clone(),
                        contracts::economics::SourceNeutralEconomicDispositionV2::from(
                            fact.disposition.clone(),
                        ),
                        fact.source_reference.watermark.clone(),
                        fact.source_reference.record.clone(),
                    ));
                }
                if let NormalizationRecordV2::EconomicFactV2 { fact, .. } = &result.record {
                    economics.push((
                        fact.fact_reference.clone(),
                        fact.logical_identity.clone(),
                        fact.economic_identity.clone(),
                        fact.disposition.clone(),
                        fact.source_reference.watermark.clone(),
                        fact.source_reference.record.clone(),
                    ));
                }
                if let NormalizationRecordV2::Evidence {
                    source_reference,
                    evidence: SourceNeutralEvidenceKindV2::TradeLifecycleV3 { result },
                    ..
                } = result.record
                {
                    let result = result.result;
                    let contracts::execution_normalization::SimV2SourceRecordReferenceV1::TradeLifecycle { private_feedback_sequence, platform_event_at_utc_ms, feedback_received_at_utc_ms } = source_reference.record else { panic!("成交直接来源"); };
                    let original = new_feedback
                        .iter()
                        .find(|f| f.metadata.private_feedback_sequence == private_feedback_sequence)
                        .unwrap();
                    assert_eq!(result.fill_sequence, original.fill_sequence);
                    assert_eq!(result.matched_at_utc_ms, original.matched_at_utc_ms);
                    assert_eq!(
                        result.source_price_pusd_per_share,
                        original.source_price_pusd_per_share
                    );
                    assert_eq!(result.base_quantity_shares, original.base_quantity_shares);
                    assert_eq!(result.quote_amount_pusd, original.quote_amount_pusd);
                    assert_eq!(
                        result.immediate_taker_fee_pusd,
                        original.immediate_taker_fee_pusd
                    );
                    assert_eq!(result.liquidity_role, original.liquidity_role);
                    assert_eq!(
                        result.economic_application_account_transition_seq,
                        original.economic_application_account_transition_seq
                    );
                    assert_eq!(
                        platform_event_at_utc_ms,
                        original.metadata.platform_event_at_utc_ms
                    );
                    assert_eq!(
                        feedback_received_at_utc_ms,
                        original.metadata.feedback_received_at_utc_ms
                    );
                    matched_records += 1;
                }
            }
            assert_eq!(matched_records, if measured { 15 } else { 7 });
            assert_eq!(identities, legacy_identities);
            assert_eq!(economics, legacy_economics);
            let resolved = first_resolved.unwrap();
            drop(execution);
            if measured {
                noe_projection::assert_order_controls_v3(
                    &root,
                    &resolved.context.context_reference,
                    &accepted.borrow(),
                );
            } else {
                let restored = execution_normalization::DurableContextStoreV2::open(
                    &root,
                    &resolved.context.context_reference,
                )
                .unwrap();
                assert_eq!(
                    restored
                        .commits()
                        .iter()
                        .map(|c| c.records.len())
                        .sum::<usize>(),
                    observation.result_references().len()
                );
            }
            // 从同一事件日志重建到一个空的正常NOE根，验证保存的运行版本和完整新字段。
            let rebuilt_root = directory.path().join("rebuilt-noe");
            let (port, rebuilt_observation): (Box<dyn BusinessProjectionPortV2>, _) = if measured {
                let port = SimV2NoeProjectionPortV3::new(
                    &rebuilt_root,
                    run_opened,
                    evidence.clone(),
                    evidence,
                    "opening".into(),
                    "trade-facts-test".into(),
                );
                let observation = port.observation();
                (Box::new(port), observation)
            } else {
                let port = SimV2NoeProjectionPortV2::new(
                    &rebuilt_root,
                    run_opened,
                    evidence.clone(),
                    evidence,
                    "opening".into(),
                    "trade-facts-test".into(),
                );
                let rebuilt_observation = port.observation();
                (Box::new(port), rebuilt_observation)
            };
            let recovered = SimExecution::recover_paper(
                &path,
                rules,
                policy,
                scenario,
                latency,
                port,
                MAX_GROUP_BYTES,
                |r| {
                    assert_eq!(r, &reference);
                    Ok(Arc::clone(&events))
                },
            )
            .unwrap();
            assert_eq!(
                recovered.actor.opened.projection_version,
                BusinessProjectionVersionV1::V3
            );
            let rebuilt: Vec<_> = rebuilt_observation
                .result_references()
                .iter()
                .map(|r| read_result(r).record)
                .collect();
            let original: Vec<_> = observation
                .result_references()
                .iter()
                .map(|r| read_result(r).record)
                .collect();
            assert_eq!(rebuilt, original);
            if measured {
                let original_full: Vec<_> = observation
                    .result_references()
                    .iter()
                    .map(|r| {
                        execution_normalization::read_noe_result_v3(r)
                            .unwrap()
                            .record
                    })
                    .collect();
                let rebuilt_full: Vec<_> = rebuilt_observation
                    .result_references()
                    .iter()
                    .map(|r| {
                        execution_normalization::read_noe_result_v3(r)
                            .unwrap()
                            .record
                    })
                    .collect();
                assert_eq!(
                    rebuilt_full, original_full,
                    "新控制数量与批项时间必须逐字段原样重建"
                );
            }
            let batch = accepted
                .borrow()
                .iter()
                .find_map(|payload| match payload.as_ref() {
                    BusinessProjectionBatchPayloadV1::V3(batch)
                        if batch
                            .trade_lifecycle
                            .iter()
                            .any(|f| f.account_delta.is_some()) =>
                    {
                        Some(Arc::clone(batch))
                    }
                    _ => None,
                })
                .unwrap();
            drop(recovered);
            if !measured {
                noe_projection::assert_v3_rebuilt_evidence_semantics(
                    &rebuilt_root,
                    &resolved.context.context_reference,
                    &batch,
                );
            } else {
                noe_projection::assert_order_controls_v3(
                    &rebuilt_root,
                    &resolved.context.context_reference,
                    &accepted.borrow(),
                );
            }
        }
    }
    mod public_consumption_tests {
        include!("public_consumption_tests.rs");
    }
}

fn reduce_public_group_v1(
    mut state: BusinessReducerStateV2,
    group: &AcceptedPublicGroupV1,
    rules: &PlatformRuleSnapshotV3,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<(BusinessReducerStateV2, SessionMarketEventEffectsV2), BusinessReductionErrorV2> {
    use crate::market::MarketEventIdentityV1;
    use data_pipeline_core::pm_reduction::*;
    let fail = |error| BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(error));
    let adapt = |error| match error {
        MarketEventSliceReductionErrorV2::Unaccepted { state, error } => {
            BusinessReductionErrorV2::Unaccepted { state, error }
        }
        MarketEventSliceReductionErrorV2::Fatal(error) => BusinessReductionErrorV2::Fatal(error),
    };
    let at = group.execution_time.logical_time_utc_ms();
    let mut effects = SessionMarketEventEffectsV2::default();
    for context in &group.prepared.pm_contexts {
        state
            .public_pm_evidence
            .entry(context.market.condition_id.0.clone())
            .or_insert_with(|| {
                data_pipeline_core::public_observation_core::PmOwnedStateV1::new_evidence_only(
                    context.clone(),
                )
            });
    }
    for fact in group.prepared.group.records.iter() {
        match &fact.payload {
            contracts::PublicBusinessPayloadV1::PolymarketMessage(message) => {
                let (position, prepared) = group
                    .prepared
                    .pm_inputs
                    .iter()
                    .find(|(_, p)| p.message().event_id == message.event_id)
                    .expect("公共供料包含当前规范PM的原准备材料");
                let evidence = &state.public_pm_evidence[&message.market.condition_id.0];
                let read = SimPmReadV1 {
                    evidence,
                    markets: &state.session.as_ref().unwrap().public_markets,
                };
                let reduction = compute_pm_message_steps_v1(
                    evidence.context(),
                    &read,
                    prepared,
                    &format!("{}:public", group.execution_time.run_id()),
                    *position,
                    true,
                )
                .map_err(|error| {
                    fail(MarketEventErrorV1::Decode {
                        index: 0,
                        message: error.to_string(),
                    })
                })?;
                for step in &reduction.steps {
                    let l2 = matches!(
                        step.change,
                        PmStepChangeV1::InitialSnapshot { .. } | PmStepChangeV1::LevelChange { .. }
                    );
                    let applies = !l2
                        || state
                            .session
                            .as_ref()
                            .unwrap()
                            .public_markets
                            .applies_pm_l2(
                                &message.market.condition_id.0,
                                at,
                                message.market.window.end_at_utc_ms,
                            );
                    if let Some(normalized) =
                        crate::market::normalize_pm_step_v1(step).map_err(fail)?
                    {
                        let identity =
                            PublicExecutionIdentityV1::Step(step.derived_id.clone()).identity_key();
                        let header = crate::market::MarketEventHeaderV1 {
                            identity: &identity,
                            market: &message.market,
                            time: &step.dependency_time,
                            quality: &step.quality,
                        };
                        let (next, applied) = reduce_normalized_market_slice_v1(
                            state,
                            MarketEventPathV1::PublicVisibility,
                            at,
                            &[header],
                            &[normalized],
                            rules,
                            finality_policy,
                        )
                        .map_err(adapt)?;
                        state = next;
                        effects.execution.extend(applied.execution);
                    }
                    if applies {
                        state
                            .public_pm_evidence
                            .get_mut(&message.market.condition_id.0)
                            .unwrap()
                            .apply_step(step);
                    }
                }
                state
                    .pm_materials
                    .push(DeferredPublicPmReductionV1::capture(reduction));
            }
            contracts::PublicBusinessPayloadV1::MarketFact { market, payload } => {
                if let Some(normalized) =
                    crate::market::normalize_public_market_fact_v1(payload, &fact.quality)
                        .map_err(fail)?
                {
                    let identity =
                        PublicExecutionIdentityV1::Fact(fact.position.clone()).identity_key();
                    let header = crate::market::MarketEventHeaderV1 {
                        identity: &identity,
                        market,
                        time: &fact.dependency_time,
                        quality: &fact.quality,
                    };
                    let (next, applied) = reduce_normalized_market_slice_v1(
                        state,
                        MarketEventPathV1::PublicVisibility,
                        at,
                        &[header],
                        &[normalized],
                        rules,
                        finality_policy,
                    )
                    .map_err(adapt)?;
                    state = next;
                    effects.execution.extend(applied.execution);
                }
            }
            contracts::PublicBusinessPayloadV1::ReferenceFact(_) => {}
        }
    }
    Ok((state, effects))
}

/// 持有唯一执行器的新消费接口；构造时移交日志所有权，不暴露旧同步驱动。
pub struct PublicSimExecutionV1 {
    actor: SimRunActorV2,
}
impl PublicSimExecutionV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn create_new(
        path: &Path,
        opened: PublicRunOpenedV1,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        finality: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency: Arc<EmpiricalDelayProfile>,
        projection: Box<dyn PublicProjectionPortV1>,
        max_group_bytes: usize,
        finite_in_flight_groups: usize,
    ) -> Result<Self, SimRunActorErrorV2> {
        Self::create_new_with_role(
            path,
            opened,
            rules,
            finality,
            scenario,
            latency,
            projection,
            max_group_bytes,
            finite_in_flight_groups,
            RunDurabilityRoleV2::CleanRun,
        )
    }
    /// 真实有限历史运行记录；不能调用 complete 冒充完整经济终局。
    #[allow(clippy::too_many_arguments)]
    pub fn create_new_historical(
        path: &Path,
        opened: PublicRunOpenedV1,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        finality: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency: Arc<EmpiricalDelayProfile>,
        projection: Box<dyn PublicProjectionPortV1>,
        max_group_bytes: usize,
        finite_in_flight_groups: usize,
    ) -> Result<Self, SimRunActorErrorV2> {
        Self::create_new_with_role(
            path,
            opened,
            rules,
            finality,
            scenario,
            latency,
            projection,
            max_group_bytes,
            finite_in_flight_groups,
            RunDurabilityRoleV2::FiniteHistorical,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn create_new_with_role(
        path: &Path,
        opened: PublicRunOpenedV1,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        finality: Arc<TradeFinalityPolicyV2>,
        scenario: Arc<SealedPlatformScenarioV1>,
        latency: Arc<EmpiricalDelayProfile>,
        projection: Box<dyn PublicProjectionPortV1>,
        max_group_bytes: usize,
        finite_in_flight_groups: usize,
        role: RunDurabilityRoleV2,
    ) -> Result<Self, SimRunActorErrorV2> {
        let mut record = opened.into_record();
        record.durability_role = role;
        let mut actor = SimRunActorV2::create_new(
            path,
            record,
            rules,
            finality,
            scenario,
            latency,
            Box::new(PublicProjectionAdapterV1(projection)),
            max_group_bytes,
        )?;
        let log = actor.log.take().expect("新建运行唯一日志");
        actor.background_log = Some(BackgroundEventLogV1::start(log, finite_in_flight_groups));
        Ok(Self { actor })
    }
    pub fn configure_cancel_failure_v1(
        &mut self,
        scenario: crate::runtime_v2::CancelFailureScenarioV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed = self
            .actor
            .commit_group(vec![BusinessInputV2::CancelScenario(
                crate::runtime_v2::CancelScenarioOperationV1::Configure(scenario),
            )])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }
    /// 有限模拟场景沿原输入记录持久化；真实业务不从脚本直接写 Ledger。
    pub fn configure_resolution_redeem_v1(
        &mut self,
        scenario: crate::ResolutionRedeemScenarioV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed = self.actor.commit_group(vec![BusinessInputV2::Settlement(
            crate::matching::settlement::SettlementOperationV1::Configure(scenario),
        )])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }

    /// 在首次 Ledger 应用前，将原材料接入本运行的唯一后台所有者。
    #[cfg(feature = "shared-execution")]
    pub fn bind_ledger_recording_v2(
        &mut self,
        ledger: &mut ledger::LedgerWriterV1,
        root: &Path,
    ) -> Result<(), SimRunActorErrorV2> {
        self.actor
            .background_log
            .as_mut()
            .ok_or(SimRunActorErrorV2::Closed)?
            .bind_ledger(ledger, root);
        Ok(())
    }
    /// 有限宿主显式暂停供料时使用；业务接纳本身保持非阻塞。
    pub fn wait_for_recording_capacity(&mut self) -> Result<(), SimRunActorErrorV2> {
        self.actor
            .background_log
            .as_mut()
            .ok_or(SimRunActorErrorV2::Closed)?
            .wait_for_capacity()
            .map_err(SimRunActorErrorV2::Log)
    }
    pub fn create_session(
        &mut self,
        spec: SessionSpecV3,
    ) -> Result<CreateSessionControlResultV2, SimRunActorErrorV2> {
        self.actor.create_session(spec)
    }
    pub fn activate_session(
        &mut self,
        activation_id: String,
    ) -> Result<SessionInitializationControlResultV2, SimRunActorErrorV2> {
        self.actor.activate_session(activation_id)
    }
    pub fn fail_session_initialization(
        &mut self,
        failure_id: String,
    ) -> Result<SessionInitializationControlResultV2, SimRunActorErrorV2> {
        self.actor.fail_session_initialization(failure_id)
    }
    pub fn enroll_market(
        &mut self,
        request: MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV2, SimRunActorErrorV2> {
        self.actor.enroll_market(request)
    }
    /// 有限宿主在原后台稳定已接纳输入、NOE和Ledger材料，不生成账户快照。
    pub fn stabilize_recording(&mut self) -> Result<(), SimRunActorErrorV2> {
        self.actor
            .background_log
            .as_mut()
            .ok_or(SimRunActorErrorV2::Closed)?
            .stabilize()
            .map_err(SimRunActorErrorV2::Log)
    }
    /// 排空已经接纳的记录，不推进业务时间、不领取后续反馈，也不生成 RunSeal。
    pub fn stop_and_drain(mut self) -> Result<(), SimRunActorErrorV2> {
        self.actor
            .background_log
            .take()
            .ok_or(SimRunActorErrorV2::Closed)?
            .stop_and_drain()
            .map_err(SimRunActorErrorV2::Log)
    }
    pub fn complete(self) -> Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2> {
        self.actor.complete_clean_run()
    }
    fn apply_public_group(
        &mut self,
        prepared: Arc<PreparedPublicGroupV1>,
        time: TrustedActorExecutionTimeV1,
    ) -> Result<PublicGroupAppliedReceiptV1, SimRunActorErrorV2> {
        let reference = prepared.reference.clone();
        let sequence = self.actor.applied_input_sequence;
        let accepted = AcceptedPublicGroupV1 {
            reference: reference.clone(),
            execution_time: time.clone(),
            prepared,
            pm_materials: Vec::new(),
            pm_step_bindings: Vec::new(),
            pm_reports: Vec::new(),
        };
        let committed = self
            .actor
            .commit_group(vec![BusinessInputV2::PublicGroup(accepted)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        let session = self
            .actor
            .state
            .as_ref()
            .and_then(|s| s.session.as_ref())
            .expect("成功应用公开组的唯一会话");
        Ok(PublicGroupAppliedReceiptV1 {
            session_id: session.opening_fact.session_id.clone(),
            group: reference,
            actor_input_sequence: sequence,
            execution_time: time,
        })
    }
    fn decision_cutoff(
        &self,
        receipt: PublicGroupAppliedReceiptV1,
    ) -> TrustedGroupDecisionCutoffV1 {
        let delivered = self
            .actor
            .projection_port
            .actual_delivered_private_feedback(&self.actor.opened.run_id)
            .expect("新构造边界必有真实反馈交付能力");
        TrustedGroupDecisionCutoffV1 {
            decision_time: receipt.execution_time.clone(),
            receipt,
            private_feedback_sequence: delivered,
        }
    }
    fn request(
        &mut self,
        cutoff: TrustedGroupDecisionCutoffV1,
        send_time: Option<TrustedActorExecutionTimeV1>,
        request: BusinessRequestV2,
    ) -> Result<MutationRequestReceiptV2, RequestFailureV1> {
        if let Some(existing) = existing_mutation_request_receipt_v2(
            self.actor.state.as_ref().ok_or_else(|| {
                RequestFailureV1::before_request_effect(SimRunActorErrorV2::Closed)
            })?,
            &request.context().global_request_id,
        ) {
            return Ok(existing);
        }
        let mut committed =
            self.actor
                .commit_group_with_request_effect(vec![BusinessInputV2::PublicRequest {
                    cutoff,
                    send_time,
                    request,
                }])?;
        if let Some(error) = committed.unaccepted {
            return Err(RequestFailureV1::before_request_effect(error));
        }
        let Some(CommittedBusinessOutputV2::RequestRegistration(result)) = committed.outputs.pop()
        else {
            unreachable!("请求归约结果")
        };
        Ok(result)
    }
    pub fn hidden_bucket(
        &mut self,
        bucket: AcceptedHiddenStepBucketV1,
    ) -> Result<(), SimRunActorErrorV2> {
        while let Some(required) = self.actor.next_required_logical_drive() {
            if required.path != MarketEventPathV1::HiddenPlatform
                || required.required_at_utc_ms >= bucket.reference.bucket_time_utc_ms
            {
                break;
            }
            let next_index_position = self
                .actor
                .state
                .as_ref()
                .and_then(|s| s.session.as_ref())
                .expect("到期义务具有会话")
                .hidden_platform_frontier
                .next_index_position;
            self.actor.drive_platform_prefix(PlatformPrefixAdvanceV2 {
                path: MarketEventPathV1::HiddenPlatform,
                closed_through_utc_ms: required.required_at_utc_ms,
                next_index_position,
            })?;
        }
        let committed = self
            .actor
            .commit_group(vec![BusinessInputV2::HiddenStepBucket(bucket)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }
    fn observe_time(
        &mut self,
        time: TrustedActorExecutionTimeV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed = self
            .actor
            .commit_group(vec![BusinessInputV2::PublicObserveTime(time.clone())])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        self.actor.projection_port.observe_public_time(&time);
        Ok(())
    }
    fn close_public_time(
        &mut self,
        closure: TrustedPublicTimeClosureV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed = self
            .actor
            .commit_group(vec![BusinessInputV2::PublicCloseTime(closure)])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }
}

/// 有限与持续入口共用的唯一消费状态；不拥有来源或独立业务状态。
pub struct PublicConsumptionStateV1 {
    execution: PublicSimExecutionV1,
    consumption_sequence_ref: String,
    last_receipt: Option<PublicGroupAppliedReceiptV1>,
    last_cutoff: Option<TrustedGroupDecisionCutoffV1>,
    last_prepared: Option<Arc<PreparedPublicGroupV1>>,
    rules: BTreeMap<String, Arc<contracts::PublicTradingRuleContextV1>>,
    close_pending: bool,
    drive_position: u64,
}
impl PublicConsumptionStateV1 {
    fn new(execution: PublicSimExecutionV1, consumption_sequence_ref: String) -> Self {
        let rules = execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .into_iter()
            .flat_map(|session| session.market_directory.markets())
            .map(|market| {
                (
                    market.market().condition_id.0.clone(),
                    Arc::new(
                        execution
                            .actor
                            ._rule_snapshot
                            .public_rule_context(market.market().clone()),
                    ),
                )
            })
            .collect();
        Self {
            execution,
            consumption_sequence_ref,
            last_receipt: None,
            last_cutoff: None,
            last_prepared: None,
            rules,
            close_pending: false,
            drive_position: 0,
        }
    }
    /// 查询原成功登记映射，不另存宿主市场集合。
    pub fn has_enrolled_market(&self, condition: &contracts::ConditionId) -> bool {
        self.rules.contains_key(&condition.0)
    }
    /// 沿原会话登记规则扩展市场；重复请求以原成功结果确定规则映射。
    pub fn enroll_market(
        &mut self,
        request: MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV2, SimRunActorErrorV2> {
        let enrolled = self.execution.enroll_market(request)?;
        self.rules
            .entry(enrolled.market.condition_id.0.clone())
            .or_insert_with(|| {
                Arc::new(
                    self.execution
                        .actor
                        ._rule_snapshot
                        .public_rule_context(enrolled.market.clone()),
                )
            });
        Ok(enrolled)
    }
    fn time(&self, at: i64) -> TrustedActorExecutionTimeV1 {
        TrustedActorExecutionTimeV1(ActorTimeRecordV1 {
            run_id: self.execution.actor.opened.run_id.clone(),
            drive_position: self.drive_position,
            logical_time_utc_ms: at,
        })
    }
    pub fn wait_for_recording_capacity(&mut self) -> Result<(), SimRunActorErrorV2> {
        self.execution.wait_for_recording_capacity()
    }
    pub fn stabilize_recording(&mut self) -> Result<(), SimRunActorErrorV2> {
        self.execution.stabilize_recording()
    }
    fn apply_group(
        &mut self,
        prepared: Arc<PreparedPublicGroupV1>,
        at: i64,
    ) -> Result<(PublicGroupAppliedReceiptV1, TrustedGroupDecisionCutoffV1), SimRunActorErrorV2>
    {
        let time = self.time(at);
        let receipt = self
            .execution
            .apply_public_group(Arc::clone(&prepared), time)?;
        self.execution
            .actor
            .projection_port
            .observe_public_time(receipt.execution_time());
        self.drive_position += 1;
        self.close_pending = true;
        self.last_receipt = Some(receipt.clone());
        let cutoff = self.execution.decision_cutoff(receipt.clone());
        self.last_cutoff = Some(cutoff.clone());
        self.last_prepared = Some(prepared);
        Ok((receipt, cutoff))
    }
    fn close_at(
        &mut self,
        at: i64,
        reason: PublicTimeClosureReasonV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let closure = TrustedPublicTimeClosureV1 {
            execution_time: self.time(at),
            consumption_sequence_ref: self.consumption_sequence_ref.clone(),
            next_group_position: self
                .last_receipt
                .as_ref()
                .map_or(0, |r| r.group.group_position + 1),
            closure: reason,
        };
        self.execution.close_public_time(closure)?;
        self.drive_position += 1;
        if self
            .last_receipt
            .as_ref()
            .is_some_and(|r| r.execution_time.logical_time_utc_ms() <= at)
        {
            self.close_pending = false;
        }
        Ok(())
    }
    /// 原义务的下一逻辑时点，宿主与事实、观察和完成事件比较。
    pub fn next_required_logical_drive(&self) -> Option<NextRequiredLogicalDriveV2> {
        self.execution.actor.next_required_logical_drive()
    }
    fn advance_before(
        &mut self,
        at: i64,
        reason: PublicTimeClosureReasonV1,
    ) -> Result<(), SimRunActorErrorV2> {
        while let Some(required) = self
            .next_required_logical_drive()
            .filter(|required| required.required_at_utc_ms < at)
        {
            match required.path {
                MarketEventPathV1::PublicVisibility => {
                    let frontier = self
                        .execution
                        .actor
                        .state
                        .as_ref()
                        .and_then(|state| state.session.as_ref())
                        .and_then(|session| {
                            session.public_visibility_frontier.closed_through_utc_ms
                        });
                    let close = std::iter::once(required.required_at_utc_ms)
                        .chain(frontier.map(|closed| closed + 1))
                        .chain(
                            self.last_receipt
                                .as_ref()
                                .map(|receipt| receipt.execution_time.logical_time_utc_ms()),
                        )
                        .max()
                        .unwrap();
                    if close >= at {
                        break;
                    }
                    self.close_at(close, reason.clone())?
                }
                MarketEventPathV1::HiddenPlatform => {
                    let position = self
                        .execution
                        .actor
                        .state
                        .as_ref()
                        .and_then(|state| state.session.as_ref())
                        .unwrap()
                        .hidden_platform_frontier
                        .next_index_position;
                    self.execution
                        .actor
                        .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                            path: required.path,
                            closed_through_utc_ms: required.required_at_utc_ms,
                            next_index_position: position,
                        })?;
                }
            }
        }
        if let Some(before) = at.checked_sub(1) {
            let needs_close = self
                .execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .is_some_and(|session| {
                    session
                        .public_visibility_frontier
                        .closed_through_utc_ms
                        .is_none_or(|closed| closed < before)
                });
            if needs_close {
                self.close_at(before, reason)?;
            }
        }
        Ok(())
    }
    fn observe_at(
        &mut self,
        at: i64,
    ) -> Result<Option<TrustedGroupDecisionCutoffV1>, SimRunActorErrorV2> {
        let time = self.time(at);
        self.execution.observe_time(time.clone())?;
        self.drive_position += 1;
        if let Some(receipt) = self.last_receipt.clone() {
            let mut cutoff = self.execution.decision_cutoff(receipt);
            cutoff.decision_time = time;
            self.last_cutoff = Some(cutoff);
        }
        Ok(self.observation_cutoff())
    }
    /// 当前受信观察时间与实际领取反馈前缀，不应用事实或执行请求。
    pub fn observation_cutoff(&self) -> Option<TrustedGroupDecisionCutoffV1> {
        let mut cutoff = self.execution.decision_cutoff(self.last_receipt.clone()?);
        cutoff.decision_time = self.last_cutoff.as_ref()?.decision_time.clone();
        Some(cutoff)
    }
    fn validate_observation(
        &self,
        cutoff: &TrustedGroupDecisionCutoffV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let valid = cutoff.decision_time.run_id() == self.execution.actor.opened.run_id
            && self.last_receipt.as_ref().is_some_and(|receipt| {
                receipt.session_id == cutoff.receipt.session_id
                    && cutoff.receipt.group.group_position <= receipt.group.group_position
            });
        if valid {
            Ok(())
        } else {
            Err(SimRunActorErrorV2::InvalidBusinessRequest(
                "观察截止不属于本消费运行已交付前缀",
            ))
        }
    }
    #[cfg(feature = "measurement")]
    pub fn measurement_resources(&self) -> PublicConsumptionResourceSnapshotV1 {
        let resources = self.execution.actor.measurement_resource_snapshot();
        let mut public_pm_evidence =
            data_pipeline_core::public_observation_core::PmEvidenceObjectCountsV1::default();
        if let Some(state) = &self.execution.actor.state {
            for evidence in state.public_pm_evidence.values() {
                let counts = evidence.evidence_object_counts();
                public_pm_evidence.markets += counts.markets;
                public_pm_evidence.assets += counts.assets;
                public_pm_evidence.levels += counts.levels;
                public_pm_evidence.absences += counts.absences;
                public_pm_evidence.selected += counts.selected;
            }
        }
        PublicConsumptionResourceSnapshotV1 {
            public_pm_evidence,
            business: resources.business,
            pending_projection: resources.pending_projection,
            applied_input_sequence: self.execution.actor.applied_input_sequence,
        }
    }
    #[cfg(feature = "measurement")]
    pub fn drain_measurement_observations(&self) -> Vec<MeasurementObservation> {
        self.execution.actor.measurement_probe.drain()
    }
    pub fn frames(&self) -> Vec<PublicObservationFrameV1<'_>> {
        let Some(cutoff) = self.last_cutoff.as_ref() else {
            return Vec::new();
        };
        self.frames_with_cutoff(cutoff)
    }
    /// 观察宿主领取反馈后，读取最新实际交付截止；旧 frames 保留组快照语义。
    pub fn frames_at_observation(&self) -> Vec<PublicObservationFrameV1<'_>> {
        let Some(cutoff) = self.observation_cutoff() else {
            return Vec::new();
        };
        self.frames_with_cutoff(&cutoff)
    }
    fn frames_with_cutoff(
        &self,
        cutoff: &TrustedGroupDecisionCutoffV1,
    ) -> Vec<PublicObservationFrameV1<'_>> {
        let Some(prepared) = &self.last_prepared else {
            return Vec::new();
        };
        prepared
            .group
            .observations
            .iter()
            .map(|observation| PublicObservationFrameV1 {
                observation,
                rule_context: Arc::clone(
                    self.rules
                        .get(&observation.market.condition_id.0)
                        .expect("组市场已登记"),
                ),
                receipt: self.last_receipt.as_ref().unwrap().clone(),
                decision_cutoff: cutoff.clone(),
            })
            .collect()
    }
    pub fn decision(&mut self) -> Option<PublicGroupDecisionV1<'_>> {
        let cutoff = self.execution.decision_cutoff(self.last_receipt.clone()?);
        Some(PublicGroupDecisionV1 {
            driver: self,
            cutoff,
            send_time: None,
            paper_clock: None,
        })
    }
    pub fn hidden_bucket(
        &mut self,
        bucket: AcceptedHiddenStepBucketV1,
    ) -> Result<(), SimRunActorErrorV2> {
        self.execution.hidden_bucket(bucket)
    }
}
/// 原序有限供料；迭代结束仅使用原有限关闭语义。
pub struct FinitePublicConsumptionV1<
    I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>,
> {
    state: PublicConsumptionStateV1,
    source: std::iter::Peekable<I>,
    source_failure: Option<String>,
}
impl<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>> std::ops::Deref
    for FinitePublicConsumptionV1<I>
{
    type Target = PublicConsumptionStateV1;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>> std::ops::DerefMut
    for FinitePublicConsumptionV1<I>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
impl<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>
    FinitePublicConsumptionV1<I>
{
    pub fn new(
        execution: PublicSimExecutionV1,
        source: I,
        consumption_sequence_ref: String,
    ) -> Self {
        Self {
            state: PublicConsumptionStateV1::new(execution, consumption_sequence_ref),
            source: source.peekable(),
            source_failure: None,
        }
    }
    pub fn wait_for_recording_capacity(&mut self) -> Result<(), SimRunActorErrorV2> {
        self.state.wait_for_recording_capacity()
    }
    /// 只查看下一真实供料时刻，不应用或关闭组。
    pub fn next_group_at(&mut self) -> Result<Option<i64>, SimRunActorErrorV2> {
        if let Some(message) = &self.source_failure {
            return Err(SimRunActorErrorV2::PublicConsumptionSource(message.clone()));
        }
        let next_time = match self.source.peek() {
            Some(Ok(item)) => Some(item.delivered_at_utc_ms),
            Some(Err(error)) => {
                let message = error.to_string();
                self.source_failure = Some(message.clone());
                return Err(SimRunActorErrorV2::PublicConsumptionSource(message));
            }
            None => None,
        };
        Ok(next_time)
    }
    pub fn advance_before(&mut self, at: i64) -> Result<(), SimRunActorErrorV2> {
        if self.next_group_at()?.is_some_and(|next| next < at) {
            return Err(SimRunActorErrorV2::InvalidBusinessRequest(
                "推进前必须处理严格更早的公共输入",
            ));
        }
        self.state
            .advance_before(at, PublicTimeClosureReasonV1::NextGlobalEventAt(at))?;
        // 完整隐藏桶证明其下一桶之前没有其他隐藏行情；有限历史宿主据此关闭空档。
        if let Some(before) = at.checked_sub(1) {
            let session = self
                .execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .ok_or(SimRunActorErrorV2::SessionNotCreated)?;
            let frontier = &session.hidden_platform_frontier;
            if session.opening_fact.account_instance_key.caller_role
                == SimCallerRoleV1::HistoricalBacktest
                && frontier
                    .closed_through_utc_ms
                    .is_some_and(|closed| closed < before)
                && (frontier.sealed || frontier.next_event_at_utc_ms.is_some_and(|next| next >= at))
            {
                let next_index_position = frontier.next_index_position;
                self.execution
                    .actor
                    .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                        path: MarketEventPathV1::HiddenPlatform,
                        closed_through_utc_ms: before,
                        next_index_position,
                    })?;
            }
        }
        Ok(())
    }
    /// 静默观察复用已有合法材料，严格更早的义务先推进，at仍开放。
    pub fn observe_at(
        &mut self,
        at: i64,
    ) -> Result<Option<TrustedGroupDecisionCutoffV1>, SimRunActorErrorV2> {
        self.advance_before(at)?;
        self.state.observe_at(at)
    }
    /// 从原冻结观察取得当前发送能力，不重新选择意图。
    pub fn send_decision_at(
        &mut self,
        cutoff: TrustedGroupDecisionCutoffV1,
        at: i64,
    ) -> Result<PublicGroupDecisionV1<'_>, SimRunActorErrorV2> {
        self.state.validate_observation(&cutoff)?;
        if at < cutoff.decision_time.logical_time_utc_ms() {
            return Err(SimRunActorErrorV2::InvalidBusinessRequest(
                "发送不能早于原观察",
            ));
        }
        self.observe_at(at)?;
        let send_time = self
            .state
            .last_cutoff
            .as_ref()
            .unwrap()
            .decision_time
            .clone();
        Ok(PublicGroupDecisionV1 {
            driver: &mut self.state,
            cutoff,
            send_time: Some(send_time),
            paper_clock: None,
        })
    }
    pub fn next_group(
        &mut self,
    ) -> Result<
        Option<(PublicGroupAppliedReceiptV1, TrustedGroupDecisionCutoffV1)>,
        SimRunActorErrorV2,
    > {
        let next_time = self.next_group_at()?;
        if self.close_pending {
            let at = self
                .last_receipt
                .as_ref()
                .expect("待关闭组已有回执")
                .execution_time
                .logical_time_utc_ms();
            let already_closed = self
                .execution
                .actor
                .state
                .as_ref()
                .and_then(|s| s.session.as_ref())
                .and_then(|s| s.public_visibility_frontier.closed_through_utc_ms)
                .is_some_and(|closed| closed >= at);
            if next_time != Some(at) && !already_closed {
                self.state.close_at(
                    at,
                    next_time.map_or(
                        PublicTimeClosureReasonV1::EndOfInput,
                        PublicTimeClosureReasonV1::NextGlobalEventAt,
                    ),
                )?;
            }
        }
        let Some(at) = next_time else {
            return Ok(None);
        };
        while let Some(required) = self.execution.actor.next_required_logical_drive() {
            if required.path != MarketEventPathV1::PublicVisibility
                || required.required_at_utc_ms >= at
            {
                break;
            }
            self.state.close_at(
                required.required_at_utc_ms,
                PublicTimeClosureReasonV1::NextGlobalEventAt(at),
            )?;
        }
        // 真实下一公开组证明此前空档；仅闭合到 t-1。
        // t 本身保持开放，同毫秒后续组和请求仍按原顺序处理。
        if let Some(before) = at.checked_sub(1) {
            let close_gap = self
                .execution
                .actor
                .state
                .as_ref()
                .and_then(|state| state.session.as_ref())
                .is_some_and(|session| {
                    session.opening_fact.account_instance_key.caller_role == SimCallerRoleV1::Paper
                        && session
                            .public_visibility_frontier
                            .closed_through_utc_ms
                            .is_none_or(|closed| closed < before)
                });
            if close_gap {
                self.state
                    .close_at(before, PublicTimeClosureReasonV1::NextGlobalEventAt(at))?;
            }
        }
        let prepared = Arc::clone(
            &self
                .source
                .peek()
                .expect("下一项已观察")
                .as_ref()
                .expect("成功项")
                .prepared,
        );
        let result = self.state.apply_group(prepared, at)?;
        self.source.next();
        Ok(Some(result))
    }
    /// 有限宿主在明确范围尾部结束公开供料；不生成市场终态或 CleanRun。
    pub fn finish_input_at(&mut self, at: i64) -> Result<(), SimRunActorErrorV2> {
        self.advance_before(at)?;
        let session = self
            .execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .ok_or(SimRunActorErrorV2::SessionNotCreated)?;
        if session.opening_fact.account_instance_key.caller_role
            == SimCallerRoleV1::HistoricalBacktest
            && session
                .hidden_platform_frontier
                .closed_through_utc_ms
                .is_none_or(|closed| closed < at)
        {
            let next_index_position = session.hidden_platform_frontier.next_index_position;
            self.execution
                .actor
                .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                    path: MarketEventPathV1::HiddenPlatform,
                    closed_through_utc_ms: at,
                    next_index_position,
                })?;
        }
        self.state.observe_at(at)?;
        self.state
            .close_at(at, PublicTimeClosureReasonV1::EndOfInput)
    }
    /// 登记下一真实组所需的身份，仍不消费组。
    pub fn next_prepared_group(
        &mut self,
    ) -> Result<Option<Arc<PreparedPublicGroupV1>>, SimRunActorErrorV2> {
        self.next_group_at()?;
        Ok(self
            .source
            .peek()
            .map(|item| Arc::clone(&item.as_ref().expect("已核实成功来源").prepared)))
    }
    pub fn settlement_simulation_read(
        &self,
        condition: &contracts::ConditionId,
    ) -> Option<crate::SettlementSimulationReadV1> {
        self.execution
            .actor
            .state
            .as_ref()?
            .session
            .as_ref()?
            .execution_state
            .settlement_simulation_read(condition)
    }
    /// 停止读取来源，沿同一后台排空已接纳材料；不消费迭代器剩余项。
    pub fn stop_and_drain(self) -> Result<(), SimRunActorErrorV2> {
        self.state.execution.stop_and_drain()
    }
    pub fn complete(self) -> Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2> {
        self.state.execution.complete()
    }
}

/// 一个运行只建立一次 UTC 锚与单调起点；来源时间不参与执行时钟。
struct PaperExecutionClockV1 {
    utc_anchor_ms: i64,
    started: std::time::Instant,
    #[cfg(test)]
    controlled: Option<Arc<std::sync::atomic::AtomicI64>>,
}
impl PaperExecutionClockV1 {
    fn new() -> Self {
        let started = std::time::Instant::now();
        let utc_anchor_ms = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        {
            Ok(value) => value.as_millis() as i64,
            Err(value) => -(value.duration().as_millis() as i64),
        };
        Self {
            utc_anchor_ms,
            started,
            #[cfg(test)]
            controlled: None,
        }
    }
    fn now_ms(&self) -> i64 {
        #[cfg(test)]
        if let Some(clock) = &self.controlled {
            return clock.load(std::sync::atomic::Ordering::SeqCst);
        }
        self.utc_anchor_ms + self.started.elapsed().as_millis() as i64
    }
}

/// 持续公开消费入口。宿主串行交付真实组，并按下一唤醒时间调用空闲推进。
/// 供料和空闲推进共用原执行器；同步记录容量等待后重新读取实际执行时刻。
pub struct ContinuousPublicConsumptionV1 {
    state: PublicConsumptionStateV1,
    clock: PaperExecutionClockV1,
}
impl std::ops::Deref for ContinuousPublicConsumptionV1 {
    type Target = PublicConsumptionStateV1;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl std::ops::DerefMut for ContinuousPublicConsumptionV1 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
impl ContinuousPublicConsumptionV1 {
    pub fn new(execution: PublicSimExecutionV1, consumption_sequence_ref: String) -> Self {
        Self {
            state: PublicConsumptionStateV1::new(execution, consumption_sequence_ref),
            clock: PaperExecutionClockV1::new(),
        }
    }
    fn required_at(&self) -> Option<i64> {
        self.state
            .execution
            .actor
            .next_required_logical_drive()
            .filter(|required| required.path == MarketEventPathV1::PublicVisibility)
            .map(|required| required.required_at_utc_ms)
    }
    fn required_close_at(&self) -> Option<i64> {
        let due = self.required_at()?;
        let frontier = self
            .state
            .execution
            .actor
            .state
            .as_ref()
            .and_then(|state| state.session.as_ref())
            .and_then(|session| session.public_visibility_frontier.closed_through_utc_ms);
        let last = self
            .state
            .last_receipt
            .as_ref()
            .map(|receipt| receipt.execution_time.logical_time_utc_ms());
        Some(
            std::iter::once(due)
                .chain(frontier.map(|at| at + 1))
                .chain(last)
                .max()
                .unwrap(),
        )
    }
    fn close_before(&self, now: i64, supplying: bool) -> Option<i64> {
        let due = self.required_close_at().filter(|at| *at < now);
        if !supplying && due.is_none() {
            return None;
        }
        let pending = self
            .state
            .last_receipt
            .as_ref()
            .filter(|_| self.state.close_pending)
            .map(|receipt| receipt.execution_time.logical_time_utc_ms())
            .filter(|at| *at < now);
        pending.into_iter().chain(due).min()
    }
    /// 在串行应用点分配时间，不接受调用方指定的每组执行时间。
    pub fn apply_group(
        &mut self,
        prepared: Arc<PreparedPublicGroupV1>,
    ) -> Result<(PublicGroupAppliedReceiptV1, TrustedGroupDecisionCutoffV1), SimRunActorErrorV2>
    {
        loop {
            self.state.wait_for_recording_capacity()?;
            let now = self.clock.now_ms();
            if let Some(at) = self.close_before(now, true) {
                match self
                    .state
                    .close_at(at, PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(now))
                {
                    Ok(()) | Err(SimRunActorErrorV2::RecordingBackpressured) => continue,
                    Err(error) => return Err(error),
                }
            }
            match self.state.apply_group(Arc::clone(&prepared), now) {
                Err(SimRunActorErrorV2::RecordingBackpressured) => continue,
                result => return result,
            }
        }
    }
    /// 无新行情时只推进真实到期义务，不生成 frame 或公开可见时间。
    pub fn advance_due(&mut self) -> Result<(), SimRunActorErrorV2> {
        while self.close_before(self.clock.now_ms(), false).is_some() {
            self.state.wait_for_recording_capacity()?;
            let now = self.clock.now_ms();
            let Some(at) = self.close_before(now, false) else {
                break;
            };
            match self
                .state
                .close_at(at, PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(now))
            {
                Ok(()) | Err(SimRunActorErrorV2::RecordingBackpressured) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    /// 静默观察使用本运行UTC锚和单调时钟，不合成公共来源。
    pub fn observe(&mut self) -> Result<Option<TrustedGroupDecisionCutoffV1>, SimRunActorErrorV2> {
        loop {
            self.state.wait_for_recording_capacity()?;
            let now = self.clock.now_ms();
            match self.state.advance_before(
                now,
                PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(now),
            ) {
                Err(SimRunActorErrorV2::RecordingBackpressured) => continue,
                Err(error) => return Err(error),
                Ok(()) => {}
            }
            match self.state.observe_at(now) {
                Err(SimRunActorErrorV2::RecordingBackpressured) => continue,
                result => return result,
            }
        }
    }
    /// 只借用串行发送边界，真正Bridge请求点重新读取单调时钟。
    pub fn send_decision(
        &mut self,
        cutoff: TrustedGroupDecisionCutoffV1,
    ) -> Result<PublicGroupDecisionV1<'_>, SimRunActorErrorV2> {
        self.state.validate_observation(&cutoff)?;
        Ok(PublicGroupDecisionV1 {
            driver: &mut self.state,
            cutoff,
            send_time: None,
            paper_clock: Some(&self.clock),
        })
    }
    /// 原下一义务所在毫秒严格结束后的唤醒；无义务时不要求定时唤醒。
    pub fn next_wake_after(&self) -> Option<std::time::Duration> {
        self.required_close_at().map(|at| {
            let delay = (i128::from(at) + 1 - i128::from(self.clock.now_ms())).max(0);
            std::time::Duration::from_millis(delay as u64)
        })
    }
    /// 不推进未来义务，沿同一后台排空已接纳材料。
    pub fn stop_and_drain(self) -> Result<(), SimRunActorErrorV2> {
        self.state.execution.stop_and_drain()
    }
}

fn reduce_hidden_step_bucket_v1(
    mut state: BusinessReducerStateV2,
    bucket: &AcceptedHiddenStepBucketV1,
    rules: &PlatformRuleSnapshotV3,
    finality: &TradeFinalityPolicyV2,
) -> Result<(BusinessReducerStateV2, SessionMarketEventEffectsV2), BusinessReductionErrorV2> {
    let mut effects = SessionMarketEventEffectsV2::default();
    let fail = |error| BusinessReductionErrorV2::Fatal(SimRunActorErrorV2::MarketEvent(error));
    for record in bucket.records.iter() {
        let (normalized, market, identity, time, quality) = match record {
            HiddenExecutionRecordV1::Step { market, step } => (
                crate::market::normalize_pm_step_v1(step).map_err(fail)?,
                market,
                &step.derived_id.0,
                &step.dependency_time,
                &step.quality,
            ),
            HiddenExecutionRecordV1::MarketFact(fact) => (
                crate::market::normalize_public_market_fact_v1(&fact.payload, &fact.quality)
                    .map_err(fail)?,
                &fact.market,
                &fact.event_id.0,
                &fact.dependency_time,
                &fact.quality,
            ),
            HiddenExecutionRecordV1::Reference(_)
            | HiddenExecutionRecordV1::BinanceDepthReference { .. } => continue,
        };
        let Some(normalized) = normalized else {
            continue;
        };
        let header = crate::market::MarketEventHeaderV1 {
            identity,
            market,
            time,
            quality,
        };
        let (next, applied) = reduce_normalized_market_slice_v1(
            state,
            MarketEventPathV1::HiddenPlatform,
            bucket.reference.bucket_time_utc_ms,
            &[header],
            &[normalized],
            rules,
            finality,
        )
        .map_err(|error| match error {
            MarketEventSliceReductionErrorV2::Unaccepted { state, error } => {
                BusinessReductionErrorV2::Unaccepted { state, error }
            }
            MarketEventSliceReductionErrorV2::Fatal(error) => {
                BusinessReductionErrorV2::Fatal(error)
            }
        })?;
        state = next;
        effects.execution.extend(applied.execution);
    }
    Ok((state, effects))
}

/// 组决定持有同一驱动器的独占借用；业务调用方不能替换其冻结截止或跨运行投递。
pub struct PublicGroupDecisionV1<'a> {
    driver: &'a mut PublicConsumptionStateV1,
    cutoff: TrustedGroupDecisionCutoffV1,
    send_time: Option<TrustedActorExecutionTimeV1>,
    paper_clock: Option<&'a PaperExecutionClockV1>,
}
impl PublicGroupDecisionV1<'_> {
    pub fn cutoff(&self) -> &TrustedGroupDecisionCutoffV1 {
        &self.cutoff
    }
    /// 与原冻结观察分开：实际请求发送采用的执行时刻。
    pub fn sent_at_utc_ms(&self) -> i64 {
        self.send_time
            .as_ref()
            .unwrap_or(&self.cutoff.decision_time)
            .logical_time_utc_ms()
    }
    pub fn frames(&self) -> Vec<PublicObservationFrameV1<'_>> {
        self.driver.frames_with_cutoff(&self.cutoff)
    }
    /// 资格检查借用当前实际观察；原 frames 继续绑定冻结决定。
    pub fn frames_at_observation(&self) -> Vec<PublicObservationFrameV1<'_>> {
        self.driver.frames_at_observation()
    }
    /// 显式已批准的模拟外部现金事实，不是模型动作。
    pub fn apply_external_cash_fact_v1(
        &mut self,
        fact: contracts::economics::ExternalCashFactV1<String>,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed =
            self.driver
                .execution
                .actor
                .commit_group(vec![BusinessInputV2::Settlement(
                    crate::matching::settlement::SettlementOperationV1::ExternalCash {
                        fact,
                        at_utc_ms: self.cutoff.decision_time.logical_time_utc_ms(),
                    },
                )])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }
    pub fn request_resolution_redeem_v1(
        &mut self,
        request: contracts::economics::SettlementRequestV1,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed =
            self.driver
                .execution
                .actor
                .commit_group(vec![BusinessInputV2::Settlement(
                    crate::matching::settlement::SettlementOperationV1::Request {
                        request,
                        requested_at_utc_ms: self.cutoff.decision_time.logical_time_utc_ms(),
                    },
                )])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }
    /// 查询只请求后端核对；结果仍按原反馈时延进入 NOE，不直接泄漏隐藏状态。
    pub fn query_resolution_redeem_v1(
        &mut self,
        identity: contracts::economics::SettlementRequestIdV1<String>,
    ) -> Result<(), SimRunActorErrorV2> {
        let committed =
            self.driver
                .execution
                .actor
                .commit_group(vec![BusinessInputV2::Settlement(
                    crate::matching::settlement::SettlementOperationV1::Query {
                        identity,
                        requested_at_utc_ms: self.cutoff.decision_time.logical_time_utc_ms(),
                    },
                )])?;
        if let Some(error) = committed.unaccepted {
            return Err(error);
        }
        Ok(())
    }
    pub fn request(
        &mut self,
        request: BusinessRequestV2,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        self.request_with_effect(request)
            .map_err(|failure| failure.error)
    }
    /// 沿原注册事务返回当前请求效果；时间推进失败不代表当前请求已登记。
    pub fn request_with_effect(
        &mut self,
        request: BusinessRequestV2,
    ) -> Result<MutationRequestReceiptV2, RequestFailureV1> {
        if let Some(clock) = self.paper_clock {
            if let Some(existing) = existing_mutation_request_receipt_v2(
                self.driver.execution.actor.state.as_ref().ok_or_else(|| {
                    RequestFailureV1::before_request_effect(SimRunActorErrorV2::Closed)
                })?,
                &request.context().global_request_id,
            ) {
                return Ok(existing);
            }
            let now = clock.now_ms();
            self.driver
                .advance_before(
                    now,
                    PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(now),
                )
                .map_err(RequestFailureV1::before_request_effect)?;
            self.driver
                .observe_at(now)
                .map_err(RequestFailureV1::before_request_effect)?;
            self.send_time = Some(
                self.driver
                    .last_cutoff
                    .as_ref()
                    .unwrap()
                    .decision_time
                    .clone(),
            );
        }
        self.driver
            .execution
            .request(self.cutoff.clone(), self.send_time.clone(), request)
    }
}

#[cfg(test)]
type StoredDirectResponseV2 = StoredDirectResponse<CancelNotCanceledReasonV2>;
