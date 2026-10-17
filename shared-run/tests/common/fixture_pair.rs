use super::fixture_input::{
    DOWN_TOKEN_ID, SNAPSHOT_RECEIVED_AT, UP_TOKEN_ID, book, core, market, meta,
    metadata_snapshot_row, pm, record, row,
};
use super::fixture_recording::{apply, config, end};
use super::fixture_terminal::{
    TERMINAL_ADMITTED_TIME_MS, terminal_metadata_row, terminal_orderbook_row,
};
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use sim_system::{execution::*, public_consumption::*};
use std::path::Path;

pub fn recorded_memory_complete(root: &Path) -> PublicObservationCoreV1 {
    recorded_pair(root, false)
}

pub fn recorded_pair(root: &Path, reject_second: bool) -> PublicObservationCoreV1 {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    if reject_second && let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut down {
        bids[0].price = "0.2".into();
        asks[0].price = "0.3".into();
    }
    let mut active = metadata_snapshot_row();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let active = record(
        1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
    );
    let mut inputs = vec![
        meta(0),
        active,
        pm(2, 1, 21_000, book("100")),
        pm(3, 2, 21_000, down),
        pm(
            4,
            3,
            21_001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![PolymarketPriceChangeEntry {
                    asset_id: UP_TOKEN_ID.to_owned(),
                    price: "0.5".to_owned(),
                    size: Some("80".to_owned()),
                    side: PolymarketSide::Buy,
                    best_bid: None,
                    best_ask: None,
                }],
            },
        ),
        record(
            5,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                4,
                25_000,
                25_001,
                book("80"),
            ))),
        ),
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
        record(
            7,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        end(8),
    ];
    for input in &mut inputs[6..] {
        input.input_position += 1;
    }
    inputs.insert(
        6,
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                5,
                27_000,
                27_001,
                book("80"),
            ))),
        ),
    );
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    core
}

pub struct MemoryRecordedTimes;
impl PublicConsumptionTimeSourceV1 for MemoryRecordedTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(match group.group_position {
            0 => SNAPSHOT_RECEIVED_AT,
            1..=4 => 22_000,
            5 => 26_000,
            6 => 28_000,
            _ => TERMINAL_ADMITTED_TIME_MS,
        })
    }
}
