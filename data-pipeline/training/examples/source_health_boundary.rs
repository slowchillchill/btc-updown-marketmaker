//! Small untrusted boundaries only: original initialization selection, duplicate
//! identity subsets, and the shared settlement-rule mapping. Never a full replay.
use contracts::BtcSettlementReferenceV1;
use data_pipeline_core::{
    public_input::{
        MarketAnchorOriginV1, MarketAnchorQueryResultV1, PolymarketMarketMetadataV6Input,
        TypedRawRowV2,
    },
    public_reference::{observed_settlement_reference_v1, source_evidence_v3_from_typed_raw_v2},
    typed_raw::TypedRawRow,
};
use data_pipeline_training::{
    historical_initialization::{
        MetadataInitializationConfigV1, MetadataInitializationSelectionV1,
    },
    public_input::{PublicFiniteFileV1, open_finite_public_input_v1},
};
use serde_json::json;
use std::{
    fs::File,
    io::{BufWriter, Write},
};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("rules") => {
            let rules = [false, true].into_iter().flat_map(|enabled| [0, 30, 60].into_iter().filter_map(move |window| {
                observed_settlement_reference_v1(MarketAnchorOriginV1::GammaEventBySlug, MarketAnchorQueryResultV1::Observed, Some(enabled), Some(window)).map(|reference| {
                    let code = match reference {
                        BtcSettlementReferenceV1::ChainlinkSpot => 4,
                        BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => 0,
                        BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => 8,
                    };
                    json!({"enabled": enabled, "window": window, "reference": code})
                })
            })).collect::<Vec<_>>();
            println!("{}", serde_json::to_string(&rules)?);
        }
        Some("initialization") if args.len() == 4 => {
            let config: MetadataInitializationConfigV1 = serde_json::from_reader(File::open(&args[2])?)?;
            let selection = MetadataInitializationSelectionV1::open(&config, 1024, 64 * 1024 * 1024)?;
            let mut output = BufWriter::new(File::create(&args[3])?);
            for row in selection.read(1024, 64 * 1024 * 1024) {
                let (row, _) = row?;
                let metadata = match &row {
                    TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row)) => row,
                    TypedRawRowV2::PolymarketMarketMetadataV6(row) => match row.as_ref() {
                        PolymarketMarketMetadataV6Input::Existing(row) => row,
                        _ => continue,
                    },
                    _ => continue,
                };
                let assets: Vec<String> = serde_json::from_str(&metadata.asset_ids)?;
                writeln!(output, "{}", json!({"condition_id": metadata.condition_id, "assets": assets,
                    "window_start": metadata.market_start_utc, "window_end": metadata.market_end_utc,
                    "received": metadata.timestamp_received, "admitted": metadata.collector_admission_time_utc_ms,
                    "sequence": metadata.collector_admission_sequence}))?;
            }
            output.flush()?;
        }
        Some("identities") if args.len() == 4 => {
            let file: PublicFiniteFileV1 = serde_json::from_reader(File::open(&args[2])?)?;
            let mut output = BufWriter::new(File::create(&args[3])?);
            let mut index = 0_u64;
            for batch in open_finite_public_input_v1(&file, 1024, 64 * 1024 * 1024)? {
                for row in batch? {
                    let source = source_evidence_v3_from_typed_raw_v2(&row)?;
                    let business = match &row {
                        TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade)) =>
                            Some(data_pipeline_core::prepare_binance_aggregate_trade_payload(trade)?),
                        _ => None,
                    };
                    writeln!(output, "{}", json!({"index": index, "identity": source.source_message_id, "business": business}))?;
                    index += 1;
                }
            }
            output.flush()?;
        }
        _ => return Err("usage: source_health_boundary rules | initialization CONFIG OUTPUT | identities FILE_FACTS OUTPUT".into()),
    }
    Ok(())
}
