//! Bounded fixture built by the real collector constructor and Parquet writer.
use data_collect::polymarket_orderbook_v3::*;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{fs::File, path::Path};
fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: generate_health_projection_fixture OUTPUT ROWS".into());
    }
    if args[2] == "metadata" {
        return write_metadata(&args[1]);
    }
    let rows: u64 = args[2].parse()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(Path::new(&args[1]))?,
        polymarket_orderbook_v3_schema(),
        Some(properties),
    )?;
    for begin in (0..rows).step_by(1024) {
        let messages = (begin..rows.min(begin + 1024))
            .map(|i| {
                let base = 1_788_912_000_000_i64;
                let received = base + i as i64 / 2;
                let asset = (i % 2 + 1).to_string();
                let generation = if i < rows / 2 { 1 } else { 2 };
                let epoch_first = if generation == 1 { 0 } else { rows / 2 };
                let event = if i < epoch_first + 2 {
                    PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
                        asset_id: asset.clone(),
                        bids: vec![],
                        asks: vec![],
                        last_trade_price: None,
                    })
                } else {
                    PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
                        price_changes: vec![PolymarketPriceChangeEntryV3 {
                            asset_id: asset,
                            price: "0.5".into(),
                            size: Some("2".into()),
                            side: "BUY".into(),
                            best_bid: None,
                            best_ask: None,
                        }],
                    })
                };
                PolymarketOrderbookV3Message {
                    envelope: PolymarketOrderbookV3Envelope {
                        timestamp_received: received,
                        timestamp: Some(base),
                        market: format!("0x{}", "11".repeat(32)),
                        slug: None,
                        collector_host_id: "fixture-collector".into(),
                        collector_region: "test".into(),
                        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
                            .into(),
                        schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
                        source_sequence_epoch_id: format!("fixture-epoch-{generation}"),
                        collector_admission_sequence: i - epoch_first + 1,
                        collector_admission_time_utc_ms: received,
                    },
                    event,
                }
            })
            .collect::<Vec<_>>();
        writer.write(&polymarket_orderbook_v3_messages_to_batch(&messages)?)?;
    }
    writer.close()?;
    Ok(())
}

fn write_metadata(path: &str) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use data_collect::polymarket_market_metadata_layout::{
        PolymarketMarketMetadataRow, PolymarketMarketMetadataV5Row,
    };
    use data_collect::polymarket_market_metadata_v6_layout::{
        PolymarketMarketMetadataV6Row, polymarket_market_metadata_v6_rows_to_batch,
        polymarket_market_metadata_v6_schema,
    };
    use sha2::{Digest, Sha256};
    let base = 1_788_912_000_000_i64;
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    let row: PolymarketMarketMetadataRow = serde_json::from_value(serde_json::json!({
        "metadata_row_kind":"metadata_snapshot","timestamp_received":base-1,
        "collector_admission_sequence":1,"collector_admission_time_utc_ms":base,
        "metadata_origin":"gamma_markets_discovery","collector_host_id":"fixture-collector",
        "collector_region":"test","schema_version":"polymarket_market_metadata_v5","slug":"btc-updown-5m-fixture",
        "condition_id":format!("0x{}","11".repeat(32)),"clob_market_id":"123","asset_ids":"[\"1\",\"2\"]",
        "market_start_utc":base,"market_end_utc":base+300000,"fixed_identity_verified_at_utc":base-2,
        "market_lifecycle_state":"prefetched","market_runtime_role":"next",
        "source_endpoint":"https://gamma-api.polymarket.com/markets","source_evidence_origin":"gamma_markets_discovery",
        "source_evidence_event_type":"gamma_markets_discovery_response","source_evidence_received_at_utc":base-1,
        "row_evidence_validation_completed_at_utc":base,"source_evidence_payload_json":payload,
        "source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())),"metadata_fetch_start_utc":base-10,
        "closed":false,"outcomes_raw":"[\"UP\",\"DOWN\"]","outcome_prices_raw":"[\"0.5\",\"0.5\"]","active":true,"accepting_orders":true
    }))?;
    let batch = polymarket_market_metadata_v6_rows_to_batch(
        polymarket_market_metadata_v6_schema(),
        &[PolymarketMarketMetadataV6Row::Existing(
            PolymarketMarketMetadataV5Row::from_common(row, None, None),
        )],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
