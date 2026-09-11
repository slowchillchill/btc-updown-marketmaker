use std::sync::Arc;

use contracts::{
    ConditionId, ExecutionMarketEventV3, InitialAccountStateV2, QueryCursorV2, Sha256Hex,
    SimCallerRoleV1,
};
use serde::{Deserialize, Serialize};

use crate::execution::{CancelOrderItemV1, OrdersFilterV1, PostOrderItemV1, TradesFilterV1};
use crate::market::{MarketEventPathV1, MillisecondBucketClosureV1};
use crate::platform_rules::ImplementationIdentityV1;
use crate::session::{MarketEnrollmentRequestV1, PublicVisibilityCursorV1};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunDurabilityRoleV2 {
    CleanRun,
    RecoverablePaper,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BusinessProjectionVersionV1 {
    #[default]
    V2,
    V3,
}
impl BusinessProjectionVersionV1 {
    pub fn is_v2(&self) -> bool {
        *self == Self::V2
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOpenedV2 {
    #[serde(default, skip_serializing_if = "BusinessProjectionVersionV1::is_v2")]
    pub projection_version: BusinessProjectionVersionV1,
    pub run_id: String,
    pub durability_role: RunDurabilityRoleV2,
    pub platform_rule_snapshot_file_sha256: Sha256Hex,
    pub trade_finality_policy_file_sha256: Sha256Hex,
    pub platform_scenario_file_sha256: Sha256Hex,
    pub proxy_latency_profile_file_sha256: Sha256Hex,
    pub implementation_identity: ImplementationIdentityV1,
    pub market_event_source_identity_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSpecV3 {
    pub run_id: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_role: SimCallerRoleV1,
    pub caller_instance_id: String,
    pub creation_request_id: String,
    pub response_bridge_binding_id: String,
    pub initial_account_state: InitialAccountStateV2,
    pub session_seed: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContextV2 {
    pub global_request_id: String,
    pub caller_correlation_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedDecisionCutoffV2 {
    pub logical_time_utc_ms: i64,
    pub public_visibility: PublicVisibilityCursorV1,
    pub private_feedback_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderRequestV2 {
    pub context: RequestContextV2,
    pub item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderBatchItemV2 {
    pub batch_item_ordinal: u16,
    pub item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrdersRequestV2 {
    pub context: RequestContextV2,
    pub items: Vec<PostOrderBatchItemV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderRequestV2 {
    pub context: RequestContextV2,
    pub item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderBatchItemV2 {
    pub batch_item_ordinal: u16,
    pub item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrdersRequestV2 {
    pub context: RequestContextV2,
    pub items: Vec<CancelOrderBatchItemV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelMarketOrdersRequestV2 {
    pub context: RequestContextV2,
    pub condition_id: ConditionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAllOrdersRequestV2 {
    pub context: RequestContextV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRequestV2 {
    pub context: RequestContextV2,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersRequestV2 {
    pub context: RequestContextV2,
    pub filter: OrdersFilterV1,
    pub cursor: Option<QueryCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesRequestV2 {
    pub context: RequestContextV2,
    pub filter: TradesFilterV1,
    pub cursor: Option<QueryCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "method",
    content = "request",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum BusinessRequestV2 {
    PostOrder(PostOrderRequestV2),
    PostOrders(PostOrdersRequestV2),
    CancelOrder(CancelOrderRequestV2),
    CancelOrders(CancelOrdersRequestV2),
    CancelMarketOrders(CancelMarketOrdersRequestV2),
    CancelAllOrders(CancelAllOrdersRequestV2),
    Order(OrderRequestV2),
    Orders(OrdersRequestV2),
    Trades(TradesRequestV2),
}

impl BusinessRequestV2 {
    pub fn context(&self) -> &RequestContextV2 {
        match self {
            Self::PostOrder(request) => &request.context,
            Self::PostOrders(request) => &request.context,
            Self::CancelOrder(request) => &request.context,
            Self::CancelOrders(request) => &request.context,
            Self::CancelMarketOrders(request) => &request.context,
            Self::CancelAllOrders(request) => &request.context,
            Self::Order(request) => &request.context,
            Self::Orders(request) => &request.context,
            Self::Trades(request) => &request.context,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableMarketBucketReferenceV2 {
    pub path: MarketEventPathV1,
    pub bucket_at_utc_ms: i64,
    pub start_index_position: u64,
    pub next_index_position: u64,
    pub closure: MillisecondBucketClosureV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AcceptedClosedMarketBucketV2 {
    pub reference: DurableMarketBucketReferenceV2,
    #[serde(skip_serializing)]
    pub events: Arc<[ExecutionMarketEventV3]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedClosedMarketBucketV2 {
    pub(crate) reference: DurableMarketBucketReferenceV2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformPrefixAdvanceV2 {
    pub path: MarketEventPathV1,
    pub closed_through_utc_ms: i64,
    pub next_index_position: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionAdvanceV2 {
    pub earliest_retained_root_sequence: u64,
    pub order_history_retained_from_utc_ms: Option<i64>,
    pub trade_history_retained_from_utc_ms: Option<i64>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "input", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum BusinessInputV2 {
    CreateSession(SessionSpecV3),
    ActivateSession {
        activation_id: String,
    },
    FailSessionInitialization {
        failure_id: String,
    },
    EnrollMarket(MarketEnrollmentRequestV1),
    Request {
        cutoff: TrustedDecisionCutoffV2,
        request: BusinessRequestV2,
    },
    PlatformPrefixAdvance(PlatformPrefixAdvanceV2),
    RetentionAdvance(RetentionAdvanceV2),
    ClosedMarketBucket(AcceptedClosedMarketBucketV2),
    PublicGroup(crate::public_consumption::AcceptedPublicGroupV1),
    HiddenStepBucket(crate::public_consumption::AcceptedHiddenStepBucketV1),
    PublicRequest {
        cutoff: crate::public_consumption::TrustedGroupDecisionCutoffV1,
        request: BusinessRequestV2,
    },
    PublicCloseTime(crate::public_consumption::TrustedPublicTimeClosureV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "input", content = "body", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum LoggedBusinessInputV2 {
    CreateSession(SessionSpecV3),
    ActivateSession {
        activation_id: String,
    },
    FailSessionInitialization {
        failure_id: String,
    },
    EnrollMarket(MarketEnrollmentRequestV1),
    Request {
        cutoff: TrustedDecisionCutoffV2,
        request: BusinessRequestV2,
    },
    PlatformPrefixAdvance(PlatformPrefixAdvanceV2),
    RetentionAdvance(RetentionAdvanceV2),
    ClosedMarketBucket(LoggedClosedMarketBucketV2),
    PublicGroup(crate::public_consumption::LoggedPublicGroupV1),
    HiddenStepBucket(crate::public_consumption::LoggedHiddenStepBucketV1),
    PublicRequest {
        cutoff: crate::public_consumption::LoggedGroupCutoffV1,
        request: BusinessRequestV2,
    },
    PublicCloseTime(crate::public_consumption::LoggedPublicClosureV1),
}
