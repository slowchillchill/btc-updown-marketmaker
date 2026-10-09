use rust_decimal::Decimal;

use crate::{ConditionId, TokenId};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BuyLegIntentV1 {
    pub token_id: TokenId,
    pub order: BuyOrderIntentV1,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ModelIntentV1 {
    SubmitSingleBuy {
        condition_id: ConditionId,
        order: BuyLegIntentV1,
    },
    SubmitPairedBuy {
        condition_id: ConditionId,
        orders: [BuyLegIntentV1; 2],
    },
    CancelOrder {
        platform_order_id: String,
    },
    CancelOrders {
        platform_order_ids: Vec<String>,
    },
    CancelMarketOrders {
        condition_id: ConditionId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BuyOrderIntentV1 {
    GtcLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
    },
    GtdLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
        expires_at_utc_sec: i64,
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
