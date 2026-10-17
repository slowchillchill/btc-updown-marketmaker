use data_pipeline_core::{public_input::*, typed_raw::*};

pub fn reference_after(at: i64, sequence: u64) -> PolymarketRtdsChainlinkTwapInput {
    PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: at,
            message_timestamp: at - 1,
            payload_timestamp: at - 2,
            age_ms: Some(2),
            symbol: "btc/usd".into(),
            value: "64950.01".into(),
            topic: "crypto_prices_twap_thirty".into(),
            message_type: "update".into(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".into(),
            source_native_payload_json: "{\"window_s\":30}".into(),
            collector_host_id: "host".into(),
            collector_region: "eu-west-1".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".into(),
        },
        full_accuracy_value: "64950014218511440012345".into(),
        window_s: 30,
        source_sequence_epoch_id: "rtds-epoch".into(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: at + 1,
    }
}
