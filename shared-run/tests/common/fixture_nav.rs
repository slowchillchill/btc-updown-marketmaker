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

/// #378：同一当前市场，固定旧责任，仅改变已见无责任市场或实际责任集合。
pub struct ObservationScalingMaterial {
    pub core: PublicObservationCoreV1,
    pub times: TradingTimes,
    pub initial_groups: usize,
    pub before_updates: usize,
    pub update_groups: Vec<u64>,
    pub old_markets: Vec<(String, String)>,
    pub current_market: String,
}

pub fn recorded_observation_scaling(root: &Path, seen: usize) -> ObservationScalingMaterial {
    let mut core = core();
    let mut writer =
        PublicRecordingWriterV1::create_historical(root, core.header(), config()).unwrap();
    let current_market = OTHER_CONDITION_ID.to_owned();
    let old_markets: Vec<_> = (0..seen - 1)
        .map(|i| {
            (
                if i == 0 {
                    super::fixture_input::CONDITION_ID.to_owned()
                } else {
                    format!("0x{:064x}", 1000 + i)
                },
                if i == 0 {
                    UP_TOKEN_ID.to_owned()
                } else {
                    (1000 + i * 2).to_string()
                },
            )
        })
        .collect();
    let market = |i: usize| {
        if i == seen - 1 {
            (
                current_market.clone(),
                "3195577".to_owned(),
                ["21".to_owned(), "4".to_owned()],
            )
        } else {
            (
                old_markets[i].0.clone(),
                if i == 0 {
                    "3195576".to_owned()
                } else {
                    (3195577 + i).to_string()
                },
                [
                    old_markets[i].1.clone(),
                    if i == 0 {
                        DOWN_TOKEN_ID.to_owned()
                    } else {
                        (1001 + i * 2).to_string()
                    },
                ],
            )
        }
    };
    let mut times = Vec::new();
    let mut position = 0;
    let mut push = |core: &mut PublicObservationCoreV1, raw: TypedRawRowV2, at: i64| {
        let input = record(position, raw);
        position += 1;
        let applied = apply(core, &input);
        assert_eq!(
            applied.applied.closed_groups.len(),
            1,
            "每个已声明更新必须真实形成一组"
        );
        let group = applied.applied.closed_groups[0].group_position;
        times.push(at);
        writer.record(input, applied).unwrap();
        group
    };
    for active in [false, true] {
        for i in 0..seen {
            let (id, clob, tokens) = market(i);
            let mut m = metadata_snapshot_row();
            m.condition_id = id;
            m.clob_market_id = clob;
            m.asset_ids = serde_json::to_string(&tokens).unwrap();
            m.market_end_utc = if i == seen - 1 { 90_000 } else { 60_000 };
            m.collector_admission_sequence = (usize::from(active) * seen + i + 1) as u64;
            if active {
                m.timestamp_received = 20_000;
                m.collector_admission_time_utc_ms = 20_001;
                m.metadata_fetch_start_utc = Some(19_990);
                m.source_evidence_received_at_utc = Some(20_000);
                m.row_evidence_validation_completed_at_utc = Some(20_001);
                m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
                m.market_runtime_role = MetadataMarketRuntimeRole::Current;
            }
            push(
                &mut core,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(m))),
                if active { 22_000 } else { 10_002 },
            );
        }
    }
    let mut pm_sequence = 0;
    for i in 0..seen {
        let (id, _, tokens) = market(i);
        for token in tokens {
            pm_sequence += 1;
            let mut event = book("10");
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut event {
                *asset_id = token;
            }
            let mut r = row("epoch-a", pm_sequence, 21_000, 21_001, event);
            r.market = id.clone();
            push(
                &mut core,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)),
                22_000,
            );
        }
    }
    let initial_groups = seen * 4;
    for i in 0..seen - 1 {
        let (id, clob, tokens) = market(i);
        let mut m = metadata_snapshot_row();
        m.condition_id = id;
        m.clob_market_id = clob;
        m.asset_ids = serde_json::to_string(&tokens).unwrap();
        m.market_end_utc = 60_000;
        m.timestamp_received = 65_000;
        m.collector_admission_sequence = (2 * seen + i + 1) as u64;
        m.collector_admission_time_utc_ms = 65_001;
        m.metadata_fetch_start_utc = Some(64_990);
        m.source_evidence_received_at_utc = Some(65_000);
        m.row_evidence_validation_completed_at_utc = Some(65_001);
        m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
        m.market_runtime_role = MetadataMarketRuntimeRole::Current;
        m.closed = Some(true);
        push(
            &mut core,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(m))),
            66_000,
        );
    }
    let before_updates = initial_groups + seen - 1;
    let mut update_groups = Vec::new();
    for n in 0..512 {
        let at = 70_000 + n * 2;
        pm_sequence += 1;
        let event = PolymarketOrderbookEvent::BestBidAsk {
            asset_id: "21".into(),
            best_bid: Some(if n % 2 == 0 { "0.40" } else { "0.41" }.into()),
            best_ask: Some("0.6".into()),
            spread: None,
        };
        let mut r = row("epoch-a", pm_sequence, at, at + 1, event);
        r.market = current_market.clone();
        update_groups.push(push(
            &mut core,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)),
            at + 1000,
        ));
    }
    drop(push);
    let input = end(position);
    let applied = apply(&mut core, &input);
    times.extend(std::iter::repeat_n(
        72_024,
        applied.applied.closed_groups.len(),
    ));
    writer.record(input, applied).unwrap();
    writer.stabilize().unwrap();
    assert_eq!(update_groups.len(), 512);
    ObservationScalingMaterial {
        core,
        times: TradingTimes(times),
        initial_groups,
        before_updates,
        update_groups,
        old_markets,
        current_market,
    }
}
