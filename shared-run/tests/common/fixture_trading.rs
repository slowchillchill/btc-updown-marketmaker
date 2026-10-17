use super::fixture_clock::TradingTimes;
use super::fixture_input::{
    DOWN_TOKEN_ID, UP_TOKEN_ID, book, core, market, metadata_snapshot_row, pm, record, row,
};
use super::fixture_recording::{apply, config, end};
use super::fixture_reference::reference_after;
use super::fixture_terminal::{
    shift_terminal_times, shift_window_times, terminal_metadata_row, terminal_orderbook_row,
};
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use std::path::Path;

pub fn recorded_trading(
    root: &Path,
    initial_taker: &str,
    maker: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_maker(
        root,
        initial_taker,
        if maker { Some(("0.6", "3")) } else { None },
    )
}

pub fn recorded_trading_with_maker(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_window(root, initial_taker, maker, 60_000, false)
}

pub fn recorded_trading_window(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
    end_at: i64,
    after_terminal: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_tail(
        root,
        initial_taker,
        maker,
        end_at,
        if after_terminal { 2 } else { 0 },
        "100",
    )
}

pub(super) fn recorded_trading_with_tail(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
    end_at: i64,
    tail: u64,
    down_initial: &str,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_probes(root, initial_taker, maker, end_at, tail, down_initial, &[])
}

#[allow(clippy::too_many_arguments)]
pub fn recorded_trading_with_probes(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
    end_at: i64,
    tail: u64,
    down_initial: &str,
    extra_probes: &[i64],
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut metadata = metadata_snapshot_row();
    metadata.market_end_utc = end_at;
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
    let mut up = book("100");
    if let PolymarketOrderbookEvent::Book { asks, .. } = &mut up {
        asks[0].size = initial_taker.into();
        asks.push(PolymarketPriceLevel {
            price: "0.7".into(),
            size: "80".into(),
        });
    }
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, asks, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
        asks[0].size = down_initial.into();
    }
    let mut inputs = vec![first, active, pm(2, 1, 21_000, up), pm(3, 2, 21_000, down)];
    let mut times = vec![10_000, 22_000, 22_000, 22_000];
    // 先消耗原taker债务，再以真实价位新增/删除形成唯一maker3。
    let changes = if let Some((_, size)) = maker {
        vec![
            (25_000, 26_000, PolymarketSide::Sell, "0"),
            (27_000, 28_000, PolymarketSide::Buy, size),
            (27_002, 28_000, PolymarketSide::Buy, "0"),
        ]
    } else {
        vec![
            (25_000, 26_000, PolymarketSide::Sell, initial_taker),
            (27_000, 28_000, PolymarketSide::Sell, initial_taker),
        ]
    };
    for (at, visible, side, size) in changes {
        let p = inputs.len() as u64;
        inputs.push(record(
            p,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                p - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: vec![PolymarketPriceChangeEntry {
                        asset_id: UP_TOKEN_ID.into(),
                        price: if matches!(side, PolymarketSide::Buy) {
                            maker.unwrap().0.into()
                        } else {
                            "0.6".into()
                        },
                        size: Some(size.into()),
                        side,
                        best_bid: None,
                        best_ask: None,
                    }],
                },
            ))),
        ));
        times.push(visible);
    }
    let mut probes = vec![29_000, 35_000, 45_000, 55_000];
    if end_at > 100_000 {
        probes.extend([295_000, 305_000, 315_000, 395_000]);
    }
    if !extra_probes.is_empty() {
        probes.extend(extra_probes);
        probes.sort_unstable();
        probes.dedup();
    }
    // 这些探针属于终态前的交易窗口；320s窗口不能先交付396s，
    // 再回退到321.003s终态。更长窗口仍保留395s探针。
    for at in probes.into_iter().filter(|at| *at < end_at) {
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
                    price: "0.6".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ));
        times.push(at + 1_000);
    }
    let mut market = market();
    market.window.end_at_utc_ms = end_at;
    let mut terminal = terminal_metadata_row(&market);
    terminal.timestamp_received = 61_001;
    terminal.collector_admission_time_utc_ms = 61_003;
    terminal.ws_platform_timestamp_raw = Some("61000".into());
    terminal.source_evidence_received_at_utc = Some(61_001);
    terminal.row_evidence_validation_completed_at_utc = Some(61_003);
    // 完整终态字段复用原夹具，仅整体平移结束后的所有原时间。
    let mut value = serde_json::to_value(&terminal).unwrap();
    shift_terminal_times(&mut value);
    if end_at != 60_000 {
        shift_window_times(&mut value, end_at - 60_000);
    }
    terminal = serde_json::from_value(value).unwrap();
    let p = inputs.len() as u64;
    inputs.push(record(
        p,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(terminal))),
    ));
    times.push(end_at + 1_003);
    let mut ws = terminal_orderbook_row(&market);
    ws.timestamp = Some(end_at + 1_000);
    ws.timestamp_received = end_at + 1_001;
    ws.collector_admission_time_utc_ms = end_at + 1_002;
    let p = inputs.len() as u64;
    inputs.push(record(
        p,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
    ));
    times.push(end_at + 1_003);
    if tail > 0 {
        for sequence in 1..=tail {
            let at = end_at + sequence as i64 * 5_000;
            let p = inputs.len() as u64;
            inputs.push(record(
                p,
                TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(at, sequence)),
            ));
            times.push(at + 1_000);
        }
    }
    inputs.push(end(inputs.len() as u64));
    let mut group_times = Vec::new();
    for input in inputs {
        let result = apply(&mut core, &input);
        // 终态metadata与对应WS可合为一个真实组；消费时序按已关闭组，
        // 不能把每条输入位置直接当作group_position导致尾部时间错位。
        for group in &result.applied.closed_groups {
            group_times.push(times[group.closed_at_input_position as usize]);
        }
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(group_times))
}
