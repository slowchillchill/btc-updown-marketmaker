//! 新版公共输入的类型边界；这里只携带事实，不维护观测或市场归约状态。

use std::collections::BTreeMap;

use contracts::MarketAnchorSourceV1;
use serde::{Deserialize, Serialize};

use crate::typed_raw::{
    PolymarketMarketMetadataRow, PolymarketRtdsChainlinkBtcUsdRow, TypedRawRow,
};

pub const POLYMARKET_MARKET_METADATA_SCHEMA_V6: &str = "polymarket_market_metadata_v6";
pub const POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1: &str = "polymarket_rtds_chainlink_twap_v1";
pub const HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1: &str = "historical_price_anchor_repair_v1";
pub const HISTORICAL_PRICE_ANCHOR_REPAIR_MANIFEST_SCHEMA_V1: &str =
    "historical_price_anchor_repair_manifest_v1";

pub const DATA_COLLECT_HANDOFF_SCHEMA_V2: &str = "data_collect_handoff_v2";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataCollectHandoffSessionV2 {
    pub frame_schema_version: String,
    pub environment: String,
    pub run_id: String,
    pub git_commit: String,
    pub binary_sha256: String,
    pub config_digest: String,
    pub collector_host_id: String,
    pub collector_region: String,
}

/// 传输确认只表示当前对象消费并释放，不表示 DPT 应用或持久提交。
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataCollectHandoffV2 {
    Session(DataCollectHandoffSessionV2),
    Message {
        transport_sequence: u64,
        row: TypedRawRowV2,
    },
    Received {
        transport_sequence: u64,
    },
    End {
        last_confirmed_sequence: u64,
    },
    Aborted {
        last_confirmed_sequence: u64,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypedRawRowV2 {
    Existing(TypedRawRow),
    PolymarketMarketMetadataV6(Box<PolymarketMarketMetadataV6Input>),
    PolymarketRtdsChainlinkTwap(PolymarketRtdsChainlinkTwapInput),
    HistoricalPriceAnchorRepairBatch(HistoricalPriceAnchorRepairBatchInput),
}

/// `Existing` 复用 V5 的业务表达；外层 V6 版本保留真实持久格式归属。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolymarketMarketMetadataV6Input {
    Existing(PolymarketMarketMetadataRow),
    AnchorRule(MarketAnchorRuleUpdateInput),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorOriginV1 {
    GammaEventBySlug,
    WebsiteCryptoPrice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorQueryResultV1 {
    Observed,
    HttpError,
    DecodeError,
    TransportError,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnchorRuleUpdateInput {
    pub schema_version: String,
    pub timestamp_received: i64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub metadata_origin: MarketAnchorOriginV1,
    pub collector_host_id: String,
    pub collector_region: String,
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub asset_ids: String,
    pub market_start_utc: i64,
    pub market_end_utc: i64,
    pub fixed_identity_verified_at_utc: i64,
    pub market_lifecycle_state: String,
    pub market_runtime_role: String,
    pub source_endpoint: Option<String>,
    pub metadata_fetch_start_utc: i64,
    pub anchor_query_source_epoch_id: String,
    pub anchor_query_ordinal: u64,
    pub anchor_query_result: MarketAnchorQueryResultV1,
    pub anchor_event_id: Option<String>,
    pub price_to_beat_decimal: Option<String>,
    pub price_to_beat_source_field: Option<String>,
    pub anchor_source_computed_at_utc: Option<i64>,
    pub crypto_market_config_id: Option<String>,
    pub crypto_market_config_nested_id: Option<String>,
    pub crypto_market_config_asset: Option<String>,
    pub crypto_market_config_duration: Option<String>,
    pub twap_enabled: Option<bool>,
    pub twap_lookback_seconds: Option<u32>,
    pub resolution_source: Option<String>,
    pub http_request_url: Option<String>,
    pub http_response_url: Option<String>,
    pub http_status_code: Option<u16>,
    pub http_received_at_utc: Option<i64>,
    pub http_body_encoding: Option<String>,
    pub http_body: Option<String>,
    pub http_body_sha256: Option<String>,
}

/// `common` 只复用同名原字段；它不是一条可以按现货 v4 验证的行情。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketRtdsChainlinkTwapInput {
    pub common: PolymarketRtdsChainlinkBtcUsdRow,
    pub full_accuracy_value: String,
    pub window_s: u64,
    pub source_sequence_epoch_id: String,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPriceAnchorRepairRowV1 {
    pub schema_version: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub slug: String,
    pub asset_ids_json: String,
    pub market_start_at_utc_ms: i64,
    pub market_end_at_utc_ms: i64,
    pub anchor_source: MarketAnchorSourceV1,
    pub price_to_beat_raw: String,
    pub response_group_id: String,
    pub response_member_index: u32,
    pub response_received_at_utc_ms: i64,
    pub response_sha256: String,
    pub source_value_path: String,
    pub source_event_at_utc_ms: Option<i64>,
    pub historical_identity_ready_at_utc_ms: i64,
    pub historical_identity_evidence_ids_json: String,
    pub repair_policy_version: String,
}

/// 原 HTTP 实体的采集证据；旧 curl 未记录的状态和最终 URL 保持空值。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponseEvidenceV1 {
    pub origin: String,
    pub request_url: String,
    pub received_at_utc: String,
    pub received_at_utc_ms: i64,
    pub receipt_observation: String,
    pub response_body_sha256: String,
    pub response_body_path: String,
    pub original_response_file: String,
    pub http_status_code: Option<u16>,
    pub response_url: Option<String>,
}

/// 物理行定位只用于找回原身份证据，不能作为来源消息身份或采集代际。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalIdentityRowEvidenceV1 {
    pub manifest_path: String,
    pub manifest_sha256: String,
    pub parquet_path: String,
    pub parquet_sha256: String,
    pub row_index: u64,
    pub schema_version: String,
    pub metadata_row_kind: String,
    pub row_received_at_utc_ms: i64,
    pub fixed_identity_verified_at_utc_ms: i64,
    pub source_evidence_origin: Option<String>,
    pub source_endpoint: Option<String>,
    pub source_evidence_received_at_utc_ms: Option<i64>,
    pub source_evidence_raw_sha256: Option<String>,
    pub collector_admission_sequence: Option<u64>,
    pub collector_admission_time_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPriceAnchorRepairBatchInput {
    pub response_group_id: String,
    pub response: HttpResponseEvidenceV1,
    pub rows: Vec<HistoricalPriceAnchorRepairRowV1>,
    pub identity_evidence: BTreeMap<String, HistoricalIdentityRowEvidenceV1>,
}
