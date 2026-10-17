use super::fixture_clock::TradingTimes;
use super::fixture_execution::execution_with_evidence;
use super::fixture_input::{
    DOWN_TOKEN_ID, MARKET_END_AT, book, core, market, metadata_snapshot_row, pm, record,
};
use super::fixture_recording::{apply, config, end};
use super::fixture_terminal::{
    TERMINAL_ADMITTED_TIME_MS, terminal_metadata_row, terminal_orderbook_row,
};
use super::fixture_terminal::{shift_terminal_times, shift_window_times};
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use sim_system::{execution::*, platform_rules::*};
use std::{path::Path, sync::Arc};

/// #318：两个真实价格片段；有限公开轮分别领取经济应用与成功/失败反馈。
pub fn recorded_repair_lifecycle(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_repair_lifecycle_prices(root, "0.5", [("0.6", "5"), ("0.72", "5")], false)
}

pub fn recorded_repair_lifecycle_prices(
    root: &Path,
    up_price: &str,
    down_levels: [(&str, &str); 2],
    sibling: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    // 新sibling专用：初始book关闭点22001，两段2*29697+250+20+1，
    // 结束再留8秒。原四组合的来源窗口与消费安排保持不变。
    let end_at = if sibling {
        22_001 + 2 * 59_665 + 8_000
    } else {
        MARKET_END_AT
    };
    let mut initial = metadata_snapshot_row();
    initial.market_end_utc = end_at;
    let mut active = initial.clone();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut up = book("10");
    if let PolymarketOrderbookEvent::Book { asks, .. } = &mut up {
        asks[0].price = up_price.into();
        asks[0].size = "10".into();
    }
    let mut down = book("10");
    if let PolymarketOrderbookEvent::Book {
        asset_id,
        bids,
        asks,
        ..
    } = &mut down
    {
        *asset_id = DOWN_TOKEN_ID.into();
        // 新低价夹具维持不交叉；原.6/.72夹具仍使用原.5买档。
        bids[0].price = rust_decimal::Decimal::new(5, 1)
            .min(
                down_levels[0].0.parse::<rust_decimal::Decimal>().unwrap()
                    - rust_decimal::Decimal::new(1, 2),
            )
            .to_string();
        *asks = down_levels
            .map(|(price, size)| PolymarketPriceLevel {
                price: price.into(),
                size: size.into(),
            })
            .into();
    }
    let mut inputs = vec![
        record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(initial))),
        ),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        pm(2, 1, 21_000, up.clone()),
        pm(3, 2, 21_000, down),
    ];
    let mut times = vec![10_000, 22_000, 22_000, 22_000];
    let probes: Vec<i64> = if sibling {
        vec![22_000, 81_665, 141_330]
    } else {
        (22_250..=28_000).step_by(250).collect()
    };
    for (n, at) in probes.into_iter().enumerate() {
        inputs.push(pm(n as u64 + 4, n as u64 + 3, at, up.clone()));
        times.push(at + 1);
    }
    let mut market = market();
    market.window.end_at_utc_ms = end_at;
    let mut terminal = terminal_metadata_row(&market);
    let mut ws = terminal_orderbook_row(&market);
    if sibling {
        // 沿recorded_trading_with_probes的原终局整体平移，不只改消费时钟。
        terminal.timestamp_received = 61_001;
        terminal.collector_admission_time_utc_ms = 61_003;
        terminal.ws_platform_timestamp_raw = Some("61000".into());
        terminal.source_evidence_received_at_utc = Some(61_001);
        terminal.row_evidence_validation_completed_at_utc = Some(61_003);
        let mut value = serde_json::to_value(&terminal).unwrap();
        shift_terminal_times(&mut value);
        shift_window_times(&mut value, end_at - 60_000);
        terminal = serde_json::from_value(value).unwrap();
        ws.timestamp = Some(end_at + 1_000);
        ws.timestamp_received = end_at + 1_001;
        ws.collector_admission_time_utc_ms = end_at + 1_002;
    }
    for row in [
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(terminal))),
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
    ] {
        inputs.push(record(inputs.len() as u64, row));
        times.push(if sibling {
            end_at + 1_003
        } else {
            TERMINAL_ADMITTED_TIME_MS
        });
    }
    inputs.push(end(inputs.len() as u64));
    let mut group_times = Vec::new();
    for input in inputs {
        let output = apply(&mut core, &input);
        if sibling {
            for group in &output.applied.closed_groups {
                group_times.push(times[group.closed_at_input_position as usize]);
            }
        }
        writer.record(input, output).unwrap();
    }
    writer.stabilize().unwrap();
    (
        core,
        TradingTimes(if sibling { group_times } else { times }),
    )
}

pub fn execution_with_fee_policy_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    fee: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_evidence::<MeasuredCancellationEvidence>(
        path,
        core,
        policy,
        None,
        "e".repeat(64),
        "2000",
        "shared-combination",
        BusinessProjectionVersionV1::V4,
        None,
        Some(fee),
        SimCallerRoleV1::Paper,
    )
}
