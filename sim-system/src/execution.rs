use polymarket_client_sdk_v2::clob::order_builder::{
    BuyOrderEncoding, encode_limit_buy_order, encode_market_buy_order,
};
pub use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use crate::account::PlatformAccountError;
pub use crate::event_log::SimEventLogErrorV2;
pub use crate::market::{MarketEventErrorV1, MarketEventPathV1, MillisecondBucketClosureV1};
pub use crate::market_directory::SessionMarketDirectoryErrorV1;
pub use crate::matching::{MatchingMathErrorV1, MatchingStateErrorV1};
pub use crate::run_v2::{
    BusinessDirectResponseV2, BusinessProjectionBatchV2, BusinessProjectionPortV2,
    CreateSessionControlResultV2, CreateSessionDispositionV2, MutationRequestReceiptV2,
    SessionInitializationControlResultV2, SimExecution, SimRunActorErrorV2,
    UnpublishedRunArtifactCandidateV2,
};
pub use crate::runtime_v2::{
    AcceptedClosedMarketBucketV2, BusinessRequestV2, CancelAllOrdersRequestV2,
    CancelOrderBatchItemV2, CancelOrderRequestV2, CancelOrdersRequestV2,
    DurableMarketBucketReferenceV2, OrderRequestV2, OrdersRequestV2, PlatformPrefixAdvanceV2,
    PostOrderBatchItemV2, PostOrderRequestV2, PostOrdersRequestV2, RequestContextV2,
    RetentionAdvanceV2, RunDurabilityRoleV2, RunOpenedV2, SessionSpecV3, TradesRequestV2,
    TrustedDecisionCutoffV2,
};
pub use crate::session::{MarketEnrollmentRequestV1, PublicVisibilityCursorV1, SessionError};
pub use contracts::{ConditionId, TokenId};

pub const SIM_QUERY_PAGE_SIZE_V1: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimBuyOrderKindV1 {
    GtcLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
    },
    GtdLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
        expires_at_utc_ms: i64,
    },
    FokMarket {
        spend_pusd: Decimal,
        worst_price_pusd_per_share: Decimal,
    },
    FakMarket {
        spend_pusd: Decimal,
        worst_price_pusd_per_share: Decimal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimBuyOrderV1 {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub kind: SimBuyOrderKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderItemV1 {
    pub item_id: String,
    pub order: SimBuyOrderV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderItemV1 {
    pub item_id: String,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersFilterV1 {
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesFilterV1 {
    pub trade_id: Option<String>,
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
    pub after_utc_ms: Option<i64>,
    pub before_utc_ms: Option<i64>,
}

pub(crate) fn strict_millisecond_interval(filter: &TradesFilterV1) -> Option<(i64, i64)> {
    let first_inclusive = match filter.after_utc_ms {
        Some(after) => after.checked_add(1)?,
        None => 0,
    };
    let last_inclusive = match filter.before_utc_ms {
        Some(before) => before.checked_sub(1)?,
        None => i64::MAX,
    };
    (first_inclusive <= last_inclusive).then_some((first_inclusive, last_inclusive))
}

pub(crate) fn limit_buy_encoding(
    price: Decimal,
    size: Decimal,
    minimum_tick_size: Decimal,
) -> Option<BuyOrderEncoding> {
    encode_limit_buy_order(price, size, minimum_tick_size).ok()
}

pub(crate) fn market_buy_encoding(
    price: Decimal,
    spend: Decimal,
    minimum_tick_size: Decimal,
) -> Option<BuyOrderEncoding> {
    encode_market_buy_order(price, spend, minimum_tick_size).ok()
}

pub(crate) fn normalize_gtd_expiration(
    expires_at_utc_ms: i64,
    effective_processing_at_utc_ms: i64,
    minimum_lead_seconds: u64,
    safety_offset_seconds: u64,
) -> Result<Option<i64>, ()> {
    let encoded_expires_sec = expires_at_utc_ms.div_euclid(1_000);
    let processing_sec = effective_processing_at_utc_ms
        .div_euclid(1_000)
        .checked_add(i64::from(
            effective_processing_at_utc_ms.rem_euclid(1_000) != 0,
        ))
        .ok_or(())?;
    let lead = minimum_lead_seconds
        .checked_add(safety_offset_seconds)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(())?;
    let minimum_expires_sec = processing_sec.checked_add(lead).ok_or(())?;
    if encoded_expires_sec < minimum_expires_sec {
        return Ok(None);
    }
    encoded_expires_sec.checked_mul(1_000).map(Some).ok_or(())
}
