//! 合法合成来源，复用公共归约器的真实构造合同。
use data_pipeline_core::typed_raw::*;
pub fn level(price: &str, quantity: &str) -> DepthLevel {
    DepthLevel {
        price: price.to_owned(),
        quantity: quantity.to_owned(),
    }
}
pub fn snapshot(
    epoch: &str,
    last_update_id: u64,
    admission_sequence: u64,
) -> BinanceDepthSnapshotRow {
    BinanceDepthSnapshotRow {
        timestamp_received: 2_000 + admission_sequence as i64,
        snapshot_fetch_start_utc: 900,
        last_update_id,
        bids: vec![level("64000", "0.5"), level("64100", "0.7")],
        asks: vec![level("64300", "0.3"), level("64200", "0.4")],
        symbol: "BTCUSDT".to_owned(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
            .to_owned(),
        snapshot_status: "complete".to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: admission_sequence,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 2_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: "binance_depth_snapshot_v3".to_owned(),
    }
}
pub fn trade(aggregate_trade_id: u64, admission_sequence: u64, price: &str) -> BinanceAggTradeRow {
    BinanceAggTradeRow {
        timestamp_received: 3_000 + admission_sequence as i64,
        rest_backfill_timestamp_received: None,
        ingest_source: "websocket".to_owned(),
        rest_was_best_price_match: None,
        source_native_payload_json: format!(
            r#"{{"e":"aggTrade","E":2800,"T":2700,"s":"BTCUSDT","a":{aggregate_trade_id},"p":"{price}"}}"#
        ),
        source_sequence_epoch_id: "aggtrade-epoch-1".to_owned(),
        connection_generation: 1,
        websocket_ingress_sequence: admission_sequence,
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 3_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".to_owned(),
        schema_version: "binance_aggtrade_v7".to_owned(),
        record: BinanceAggTradeRecord::Trade {
            event_type: "aggTrade".to_owned(),
            event_time: 2_800,
            trade_time: 2_700,
            symbol: "BTCUSDT".to_owned(),
            aggregate_trade_id,
            price: price.to_owned(),
            quantity: "0.01".to_owned(),
            first_trade_id: aggregate_trade_id * 10,
            last_trade_id: aggregate_trade_id * 10 + 1,
            buyer_is_maker: false,
            stream: "btcusdt@aggTrade".to_owned(),
            ws_ignore: false,
        },
    }
}
pub fn twap_input(
    window_s: u64,
) -> data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
    use data_pipeline_core::typed_raw::{PolymarketRtdsChainlinkBtcUsdRow, RtdsFreshnessStatus};
    data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: 2000,
            message_timestamp: 1999,
            payload_timestamp: 1998,
            age_ms: Some(2),
            symbol: "btc/usd".to_owned(),
            value: "64950.01".to_owned(),
            topic: if window_s == 30 {
                "crypto_prices_twap_thirty"
            } else {
                "crypto_prices_twap_sixty"
            }
            .to_owned(),
            message_type: "update".to_owned(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".to_owned(),
            source_native_payload_json: format!("{{\"window_s\":{window_s}}}"),
            collector_host_id: "host".to_owned(),
            collector_region: "hk".to_owned(),
            source_endpoint: "wss://ws-live-data.polymarket.com".to_owned(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".to_owned(),
        },
        full_accuracy_value: "64950014218511440012345".to_owned(),
        window_s,
        source_sequence_epoch_id: "rtds-epoch".to_owned(),
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: 2001,
    }
}
