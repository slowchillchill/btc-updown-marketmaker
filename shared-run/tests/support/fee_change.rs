use super::fixture_clock::TradingTimes;
use super::fixture_input::{
    DOWN_TOKEN_ID, UP_TOKEN_ID, book, core, market, meta, metadata_snapshot_row, pm, record, row,
};
use super::fixture_recording::{apply, config, end};
use super::fixture_reference::reference_after;
use super::fixture_terminal::{terminal_metadata_row, terminal_orderbook_row};
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use std::path::Path;

/// 决定时 .9，提交后、平台处理前由真实 Book 更新为 .1；原请求仍保留 .9。
pub fn recorded_market_fee_change(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut active = metadata_snapshot_row();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut initial = book("1000");
    if let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut initial {
        bids[0].price = "0.05".into();
        asks[0].price = "0.9".into();
        asks[0].size = "1000".into();
    }
    let mut down = initial.clone();
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let mut improved = initial.clone();
    if let PolymarketOrderbookEvent::Book { asks, .. } = &mut improved {
        asks[0].price = "0.1".into();
        asks[0].size = "900".into();
    }
    let mut inputs = vec![
        meta(0),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        pm(2, 1, 21_000, initial),
        pm(3, 2, 21_000, down),
        record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a", 3, 22_001, 22_002, improved,
            ))),
        ),
    ];
    let mut times = vec![10_000, 22_000, 22_000, 22_000, 22_002];
    for at in [25_000, 27_000, 29_000] {
        let p = inputs.len() as u64;
        inputs.push(record(
            p,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                p - 1,
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
    let p = inputs.len() as u64;
    inputs.push(record(
        p,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            terminal_metadata_row(&market()),
        ))),
    ));
    times.push(31_003);
    inputs.push(record(
        p + 1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
            &market(),
        ))),
    ));
    times.push(31_003);
    // 等待原有代理私有回执，保留判定未赎回的真实后继状态。
    for sequence in 1..=8 {
        let at = 35_000 + sequence as i64 * 5_000;
        inputs.push(record(
            inputs.len() as u64,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(at, sequence)),
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
