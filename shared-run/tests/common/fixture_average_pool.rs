use super::fixture_clock::TradingTimes;
use super::fixture_input::{
    DOWN_TOKEN_ID, UP_TOKEN_ID, book, core, metadata_snapshot_row, pm, record, row,
};
use super::fixture_recording::{apply, config, end};
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use std::path::Path;

/// 合法原 GTC 各5份；真实盘口正增量及耗用只使其中1/2份成交，其余单独撤销。
pub fn recorded_average_pool(
    root: &Path,
    fractional: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_scaled_average_pool(root, fractional, 1)
}

pub fn recorded_scaled_average_pool(
    root: &Path,
    fractional: bool,
    scale: u32,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut metadata = metadata_snapshot_row();
    metadata.market_end_utc = 320_000;
    let first = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata.clone(),
        ))),
    );
    metadata.timestamp_received = 20_000;
    metadata.collector_admission_sequence = 2;
    metadata.collector_admission_time_utc_ms = 20_001;
    metadata.metadata_fetch_start_utc = Some(19_990);
    metadata.source_evidence_received_at_utc = Some(20_000);
    metadata.row_evidence_validation_completed_at_utc = Some(20_001);
    metadata.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    metadata.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let active = record(
        1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
    );
    let books = [UP_TOKEN_ID, DOWN_TOKEN_ID].map(|token| {
        let mut book = book("100");
        if let PolymarketOrderbookEvent::Book {
            asset_id,
            bids,
            asks,
            ..
        } = &mut book
        {
            *asset_id = token.into();
            bids[0].price = "0.01".into();
            asks[0].price = "0.99".into();
        }
        book
    });
    let mut inputs = vec![
        first,
        active,
        pm(2, 1, 21_000, books[0].clone()),
        pm(3, 2, 21_000, books[1].clone()),
    ];
    let mut times = vec![10_000, 22_000, 22_000, 22_000];
    for (at, visible, token, price, size) in [
        (25_000, 26_000, UP_TOKEN_ID, "0.1", "1"),
        (25_002, 28_000, UP_TOKEN_ID, "0.1", "0"),
        (
            35_000,
            36_000,
            UP_TOKEN_ID,
            if fractional { "0.45" } else { "0.9" },
            if fractional { "2" } else { "1" },
        ),
        (
            35_002,
            38_000,
            UP_TOKEN_ID,
            if fractional { "0.45" } else { "0.9" },
            "0",
        ),
        (45_000, 46_000, DOWN_TOKEN_ID, "0.2", "1"),
        (45_002, 48_000, DOWN_TOKEN_ID, "0.2", "0"),
    ] {
        let position = inputs.len() as u64;
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                position - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: vec![PolymarketPriceChangeEntry {
                        asset_id: token.into(),
                        price: price.into(),
                        size: Some((size.parse::<u32>().unwrap() * scale).to_string()),
                        side: PolymarketSide::Buy,
                        best_bid: None,
                        best_ask: None,
                    }],
                },
            ))),
        ));
        times.push(visible);
    }
    for at in [50_000, 55_000, 60_000] {
        let position = inputs.len() as u64;
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                position - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.1".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ));
        times.push(at + 1_000);
    }
    inputs.push(end(inputs.len() as u64));
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}
