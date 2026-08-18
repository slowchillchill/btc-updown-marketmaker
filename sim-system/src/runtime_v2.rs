use std::sync::Arc;

use contracts::{
    ExecutionMarketEventV3, InitialAccountStateV2, QueryCursorV2, Sha256Hex, SimCallerRoleV1,
};
use serde::{Deserialize, Serialize};

use crate::execution::{CancelOrderItemV1, OrdersFilterV1, PostOrderItemV1, TradesFilterV1};
use crate::market::{MarketEventPathV1, MillisecondBucketClosureV1};
use crate::platform_rules::ImplementationIdentityV1;
use crate::session::{MarketEnrollmentRequestV1, PublicVisibilityCursorV1};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum RunDurabilityRoleV2 {
    CleanRun,
    RecoverablePaper,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunOpenedV2 {
    pub(crate) run_id: String,
    pub(crate) durability_role: RunDurabilityRoleV2,
    pub(crate) platform_rule_snapshot_file_sha256: Sha256Hex,
    pub(crate) trade_finality_policy_file_sha256: Sha256Hex,
    pub(crate) platform_scenario_file_sha256: Sha256Hex,
    pub(crate) proxy_latency_profile_file_sha256: Sha256Hex,
    pub(crate) implementation_identity: ImplementationIdentityV1,
    pub(crate) market_event_source_identity_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionSpecV3 {
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) account_id: String,
    pub(crate) caller_role: SimCallerRoleV1,
    pub(crate) caller_instance_id: String,
    pub(crate) creation_request_id: String,
    pub(crate) response_bridge_binding_id: String,
    pub(crate) initial_account_state: InitialAccountStateV2,
    pub(crate) session_seed: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RequestContextV2 {
    pub(crate) global_request_id: String,
    pub(crate) caller_correlation_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TrustedDecisionCutoffV2 {
    pub(crate) logical_time_utc_ms: i64,
    pub(crate) public_visibility: PublicVisibilityCursorV1,
    pub(crate) private_feedback_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostOrderRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostOrderBatchItemV2 {
    pub(crate) batch_item_ordinal: u16,
    pub(crate) item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostOrdersRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) items: Vec<PostOrderBatchItemV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CancelOrderRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CancelOrderBatchItemV2 {
    pub(crate) batch_item_ordinal: u16,
    pub(crate) item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CancelOrdersRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) items: Vec<CancelOrderBatchItemV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CancelAllOrdersRequestV2 {
    pub(crate) context: RequestContextV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrderRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) platform_order_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrdersRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) filter: OrdersFilterV1,
    pub(crate) cursor: Option<QueryCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TradesRequestV2 {
    pub(crate) context: RequestContextV2,
    pub(crate) filter: TradesFilterV1,
    pub(crate) cursor: Option<QueryCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "method",
    content = "request",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub(crate) enum BusinessRequestV2 {
    PostOrder(PostOrderRequestV2),
    PostOrders(PostOrdersRequestV2),
    CancelOrder(CancelOrderRequestV2),
    CancelOrders(CancelOrdersRequestV2),
    CancelAllOrders(CancelAllOrdersRequestV2),
    Order(OrderRequestV2),
    Orders(OrdersRequestV2),
    Trades(TradesRequestV2),
}

impl BusinessRequestV2 {
    pub(crate) fn context(&self) -> &RequestContextV2 {
        match self {
            Self::PostOrder(request) => &request.context,
            Self::PostOrders(request) => &request.context,
            Self::CancelOrder(request) => &request.context,
            Self::CancelOrders(request) => &request.context,
            Self::CancelAllOrders(request) => &request.context,
            Self::Order(request) => &request.context,
            Self::Orders(request) => &request.context,
            Self::Trades(request) => &request.context,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableMarketBucketReferenceV2 {
    pub(crate) path: MarketEventPathV1,
    pub(crate) bucket_at_utc_ms: i64,
    pub(crate) start_index_position: u64,
    pub(crate) next_index_position: u64,
    pub(crate) closure: MillisecondBucketClosureV1,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AcceptedClosedMarketBucketV2 {
    pub(crate) reference: DurableMarketBucketReferenceV2,
    #[serde(skip_serializing)]
    pub(crate) events: Arc<[ExecutionMarketEventV3]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LoggedClosedMarketBucketV2 {
    pub(crate) reference: DurableMarketBucketReferenceV2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PlatformPrefixAdvanceV2 {
    pub(crate) path: MarketEventPathV1,
    pub(crate) closed_through_utc_ms: i64,
    pub(crate) next_index_position: u64,
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
    ClosedMarketBucket(AcceptedClosedMarketBucketV2),
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
    ClosedMarketBucket(LoggedClosedMarketBucketV2),
}
