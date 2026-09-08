//! 完整公开盘口消息。来源身份、原成员位置和本次输入位置相互独立。
use serde::{Deserialize, Serialize};

use crate::*;

pub const CANONICAL_PM_MESSAGE_SCHEMA_V1: &str = "canonical_pm_message_v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalInputRecordRefV1 {
    pub input_stream_id: String,
    pub input_position: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CanonicalPmSideV1 {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalBbaFactV1 {
    pub best_bid_pusd_per_share: Option<ExactDecimal>,
    pub best_ask_pusd_per_share: Option<ExactDecimal>,
    pub spread_pusd_per_share: Option<ExactDecimal>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalAbsoluteLevelChangeV1 {
    pub asset: AssetIdentityV1,
    pub side: CanonicalPmSideV1,
    pub price_pusd_per_share: ExactDecimal,
    pub quantity_shares: Option<ExactDecimal>,
    /// None 为未提供；Some 且两侧为空为原消息明确提供的空报价。
    pub best_bid_ask: Option<CanonicalBbaFactV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalBookFactV1 {
    FullBook {
        asset: AssetIdentityV1,
        bids: Vec<PriceLevelV1>,
        asks: Vec<PriceLevelV1>,
        last_trade_price_pusd_per_share: Option<ExactDecimal>,
    },
    AbsoluteLevelChanges {
        changes: Vec<CanonicalAbsoluteLevelChangeV1>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalPmEventDescriptionV1 {
    pub id: String,
    pub ticker: Option<String>,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalPmPayloadV1 {
    Book {
        fact: CanonicalBookFactV1,
    },
    BestBidAsk {
        asset: AssetIdentityV1,
        quote: CanonicalBbaFactV1,
    },
    TickSizeChange {
        asset: AssetIdentityV1,
        old_tick_size_pusd_per_share: Option<ExactDecimal>,
        new_tick_size_pusd_per_share: ExactDecimal,
    },
    PublicTrade {
        asset: AssetIdentityV1,
        price_pusd_per_share: ExactDecimal,
        quantity_shares: Option<ExactDecimal>,
        side: Option<CanonicalPmSideV1>,
        fee_rate_bps: Option<ExactDecimal>,
    },
    NewMarket {
        clob_market_id: String,
        question: Option<String>,
        description: Option<String>,
        asset_ids: Vec<String>,
        outcomes: Vec<String>,
        event_message: Option<CanonicalPmEventDescriptionV1>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalPmMessageV1 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub market: MarketIdentityV2,
    pub group_id: Sha256Hex,
    pub lineage: EventLineageV3,
    pub time: EventTimeV2,
    pub input_record: CanonicalInputRecordRefV1,
    pub payload: CanonicalPmPayloadV1,
}
