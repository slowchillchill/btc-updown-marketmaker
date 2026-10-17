use super::fixture_clock::TradingTimes;
use super::fixture_input::{
    DOWN_TOKEN_ID, UP_TOKEN_ID, book, core, metadata_snapshot_row, record, row,
};
use super::fixture_market_identity::OTHER_CONDITION_ID;
use super::fixture_recording::{apply, config};
use super::fixture_reference::reference_after;
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use std::path::Path;

pub fn recorded_equal_start_markets(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_two_market_rebate(root, false, false)
}

pub fn recorded_rebate_contributions(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_two_market_rebate(root, true, false)
}

pub(super) fn recorded_two_market_rebate(
    root: &Path,
    rebate: bool,
    staggered: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut rows = Vec::new();
    let mut times = Vec::new();
    for active in [false, true] {
        for high in [true, false] {
            let mut metadata = metadata_snapshot_row();
            if rebate {
                metadata.market_end_utc = 320_000;
            }
            if staggered && !high {
                metadata.market_end_utc = 40_000;
            }
            metadata.collector_admission_sequence = rows.len() as u64 + 1;
            if high {
                metadata.condition_id = OTHER_CONDITION_ID.into();
                metadata.clob_market_id = "3195577".into();
                metadata.asset_ids = "[\"21\",\"4\"]".into();
            }
            if active {
                metadata.timestamp_received = 20_000;
                metadata.collector_admission_time_utc_ms = 20_001;
                metadata.metadata_fetch_start_utc = Some(19_990);
                metadata.source_evidence_received_at_utc = Some(20_000);
                metadata.row_evidence_validation_completed_at_utc = Some(20_001);
                metadata.market_lifecycle_state = MetadataMarketLifecycleState::Active;
                metadata.market_runtime_role = MetadataMarketRuntimeRole::Current;
            }
            rows.push(TypedRawRowV2::Existing(
                TypedRawRow::PolymarketMarketMetadata(Box::new(metadata)),
            ));
            times.push(if active { 22_000 } else { 10_000 });
        }
    }
    let mut sequence = 0;
    for (high, token) in [
        (true, "21"),
        (true, "4"),
        (false, UP_TOKEN_ID),
        (false, DOWN_TOKEN_ID),
    ] {
        sequence += 1;
        let mut event = book("100");
        if let PolymarketOrderbookEvent::Book { asset_id, bids, .. } = &mut event {
            *asset_id = token.into();
            if rebate {
                bids[0].price = "0.01".into();
            }
        }
        let mut r = row("epoch-two", sequence, 21_000, 21_001, event);
        if high {
            r.market = OTHER_CONDITION_ID.into();
        }
        rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
        times.push(22_000);
    }
    for at in [25_000, 27_000] {
        for high in [true, false] {
            sequence += 1;
            let mut r = row(
                "epoch-two",
                sequence,
                at,
                at + 1,
                if rebate {
                    PolymarketOrderbookEvent::PriceChange {
                        price_changes: vec![PolymarketPriceChangeEntry {
                            asset_id: if high { "21" } else { UP_TOKEN_ID }.into(),
                            price: "0.5".into(),
                            size: Some(
                                if at == 27_000 {
                                    "0"
                                } else if high {
                                    "142.868571"
                                } else {
                                    "142.862857"
                                }
                                .into(),
                            ),
                            side: PolymarketSide::Buy,
                            best_bid: None,
                            best_ask: None,
                        }],
                    }
                } else {
                    PolymarketOrderbookEvent::LastTradePrice {
                        asset_id: if high { "21" } else { UP_TOKEN_ID }.into(),
                        price: "0.6".into(),
                        size: None,
                        side: None,
                        fee_rate_bps: None,
                    }
                },
            );
            if high {
                r.market = OTHER_CONDITION_ID.into();
            }
            rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
            times.push(at + 1000);
        }
    }
    if staggered {
        for (seq, at) in [(1, 31_000), (2, 41_000), (3, 51_000)] {
            rows.push(TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(
                at, seq,
            )));
            times.push(at + 1000);
        }
    }
    if rebate {
        for (seq, at) in [
            (1, 35_000),
            (2, 89_099_000),
            (3, 89_101_000),
            (4, 89_110_000),
        ] {
            rows.push(TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(
                at, seq,
            )));
            times.push(at + 1000);
        }
    }
    for (position, row) in rows.into_iter().enumerate() {
        let input = record(position as u64, row);
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}
