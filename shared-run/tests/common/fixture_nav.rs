use super::fixture_clock::TradingTimes;
use super::fixture_input::{
    DOWN_TOKEN_ID, UP_TOKEN_ID, book, core, metadata_snapshot_row, record, row,
};
use super::fixture_market_identity::OTHER_CONDITION_ID;
use super::fixture_recording::{apply, config, end};
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use std::path::Path;

/// #183：两个持仓市场、独立报价/空盘、闭市后未判定及真实换代缺盘。
pub fn recorded_nav_marks(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_nav_marks_with_book_quantity(root, "1")
}

pub fn recorded_nav_marks_with_book_quantity(
    root: &Path,
    quantity: &str,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut rows = vec![];
    let mut times = vec![];
    for active in [false, true] {
        for other in [false, true] {
            let mut m = metadata_snapshot_row();
            m.market_end_utc = if other { 90_000 } else { 60_000 };
            if other {
                m.condition_id = OTHER_CONDITION_ID.into();
                m.clob_market_id = "3195577".into();
                m.asset_ids = "[\"21\",\"4\"]".into();
            }
            m.collector_admission_sequence = rows.len() as u64 + 1;
            if active {
                m.timestamp_received = 20_000;
                m.collector_admission_time_utc_ms = 20_001;
                m.metadata_fetch_start_utc = Some(19_990);
                m.source_evidence_received_at_utc = Some(20_000);
                m.row_evidence_validation_completed_at_utc = Some(20_001);
                m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
                m.market_runtime_role = MetadataMarketRuntimeRole::Current;
            }
            rows.push(TypedRawRowV2::Existing(
                TypedRawRow::PolymarketMarketMetadata(Box::new(m)),
            ));
            times.push(if active { 22_000 } else { 10_000 });
        }
    }
    for (i, token) in [UP_TOKEN_ID, DOWN_TOKEN_ID, "21", "4"].iter().enumerate() {
        let mut event = book(quantity);
        if let PolymarketOrderbookEvent::Book { asset_id, bids, .. } = &mut event {
            *asset_id = (*token).into();
            bids[0].price = if *token == UP_TOKEN_ID { "0.5" } else { "0.4" }.into();
        }
        let mut r = row("epoch-a", i as u64 + 1, 21_000, 21_001, event);
        if i >= 2 {
            r.market = OTHER_CONDITION_ID.into();
        }
        rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
        times.push(22_000);
    }
    for (i, at, bid) in [
        (5, 35_000, Some("0.4")),
        (6, 37_000, Some("0.5")),
        (7, 39_000, None),
        (8, 41_000, Some("0.5")),
    ] {
        rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
            row(
                "epoch-a",
                if i == 5 { i } else { i + 1 },
                at,
                at + 1,
                PolymarketOrderbookEvent::BestBidAsk {
                    asset_id: UP_TOKEN_ID.into(),
                    best_bid: bid.map(str::to_owned),
                    best_ask: Some("0.6".into()),
                    spread: None,
                },
            ),
        )));
        times.push(at + 1000);
        if i == 5 {
            // Review真实失败：此时10孤腿；删除无关高档不应把旧0.5提升覆盖直接0.4。
            rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
                row(
                    "epoch-a",
                    6,
                    36_500,
                    36_501,
                    PolymarketOrderbookEvent::PriceChange {
                        price_changes: vec![PolymarketPriceChangeEntry {
                            asset_id: UP_TOKEN_ID.into(),
                            price: "0.9".into(),
                            size: Some("0".into()),
                            side: PolymarketSide::Buy,
                            best_bid: None,
                            best_ask: None,
                        }],
                    },
                ),
            )));
            times.push(37_000);
        }
    }
    // 关闭已可见，正式判定尚不可见；有效报价仍用于旧市场持仓。
    let mut closed = metadata_snapshot_row();
    closed.market_end_utc = 60_000;
    closed.timestamp_received = 65_000;
    closed.collector_admission_sequence = 5;
    closed.collector_admission_time_utc_ms = 65_001;
    closed.metadata_fetch_start_utc = Some(64_990);
    closed.source_evidence_received_at_utc = Some(65_000);
    closed.row_evidence_validation_completed_at_utc = Some(65_001);
    closed.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    closed.market_runtime_role = MetadataMarketRuntimeRole::Current;
    closed.closed = Some(true);
    rows.push(TypedRawRowV2::Existing(
        TypedRawRow::PolymarketMarketMetadata(Box::new(closed)),
    ));
    times.push(66_000);
    // 第二市场没有直接BBA，真实换代使它的完整L2不可用；第一市场的BBA保持。
    let mut r = row(
        "epoch-b",
        1,
        70_000,
        70_001,
        PolymarketOrderbookEvent::LastTradePrice {
            asset_id: "21".into(),
            price: "0.6".into(),
            size: None,
            side: None,
            fee_rate_bps: None,
        },
    );
    r.market = OTHER_CONDITION_ID.into();
    rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
    times.push(71_000);
    for (position, row) in rows.into_iter().enumerate() {
        let input = record(position as u64, row);
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    let input = end(times.len() as u64);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}
