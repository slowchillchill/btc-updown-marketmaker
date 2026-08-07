use contracts::*;
use data_pipeline_core::reduction::{
    PolymarketAssetContextV1, PolymarketMarketContextV1, PolymarketOrderbookInputV1,
    PolymarketReducer, ReductionDispositionV1, ReductionEvidenceRoleV1, ReductionOperationKindV1,
};
use data_pipeline_core::source_evidence_from_typed_raw;
use data_pipeline_core::typed_raw::{
    POLYMARKET_ORDERBOOK_SCHEMA, PolymarketOrderbookEvent, PolymarketOrderbookRow,
    PolymarketPriceChangeEntry, PolymarketPriceLevel, PolymarketSide, TypedRawRow,
};

const UP_TOKEN_ID: &str = "123";
const DOWN_TOKEN_ID: &str = "456";

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
}

fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("3195576".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 500,
            end_at_utc_ms: 300_500,
        },
    }
}

fn asset(token_id: &str, index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token_id.to_owned()),
        outcome: OutcomeIdentityV1 {
            index,
            label: label.to_owned(),
        },
    }
}

fn up_asset() -> AssetIdentityV1 {
    asset(UP_TOKEN_ID, 0, "UP")
}

fn market_context() -> PolymarketMarketContextV1 {
    PolymarketMarketContextV1 {
        market: market(),
        assets: vec![
            PolymarketAssetContextV1 {
                asset: up_asset(),
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            },
            PolymarketAssetContextV1 {
                asset: asset(DOWN_TOKEN_ID, 1, "DOWN"),
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            },
        ],
        identity_source_evidence: vec![SourceEvidenceRefV2 {
            source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
            source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                sha256: sha('a'),
            }),
            source_event_at_utc_ms: None,
            received_at_utc_ms: 900,
            collector_admission: None,
            input_manifest_sha256: sha('b'),
            input_file_sha256: sha('c'),
        }],
    }
}

fn reducer() -> PolymarketReducer {
    PolymarketReducer::new(market_context(), "polymarket-reducer-v1").unwrap()
}

fn row(
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
    event: PolymarketOrderbookEvent,
) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: received_at_utc_ms,
        timestamp: Some(source_event_at_utc_ms),
        market: format!("0x{}", "1".repeat(64)),
        slug: Some("btc-updown-5m".to_owned()),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA.to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: received_at_utc_ms + 1,
        event,
    }
}

fn input(row: PolymarketOrderbookRow) -> PolymarketOrderbookInputV1 {
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(row.clone()),
        &sha('d'),
        &sha('e'),
    )
    .unwrap();
    PolymarketOrderbookInputV1 { row, evidence }
}

fn book_row(
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
    best_bid_quantity: &str,
) -> PolymarketOrderbookRow {
    row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        PolymarketOrderbookEvent::Book {
            asset_id: UP_TOKEN_ID.to_owned(),
            bids: vec![
                PolymarketPriceLevel {
                    price: "0.50".to_owned(),
                    size: "100".to_owned(),
                },
                PolymarketPriceLevel {
                    price: "0.52".to_owned(),
                    size: best_bid_quantity.to_owned(),
                },
            ],
            asks: vec![
                PolymarketPriceLevel {
                    price: "0.55".to_owned(),
                    size: "100".to_owned(),
                },
                PolymarketPriceLevel {
                    price: "0.53".to_owned(),
                    size: "260".to_owned(),
                },
            ],
            last_trade_price: Some("0.51".to_owned()),
        },
    )
}

fn book_row_for_asset(
    asset_id: &str,
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
    best_bid_quantity: &str,
) -> PolymarketOrderbookRow {
    let mut row = book_row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        best_bid_quantity,
    );
    let PolymarketOrderbookEvent::Book {
        asset_id: row_asset_id,
        ..
    } = &mut row.event
    else {
        unreachable!();
    };
    *row_asset_id = asset_id.to_owned();
    row
}

fn price_change_row(
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
    absolute_quantity: &str,
) -> PolymarketOrderbookRow {
    row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![PolymarketPriceChangeEntry {
                asset_id: UP_TOKEN_ID.to_owned(),
                price: "0.52".to_owned(),
                size: Some(absolute_quantity.to_owned()),
                side: PolymarketSide::Buy,
                best_bid: Some("0.52".to_owned()),
                best_ask: Some("0.53".to_owned()),
            }],
        },
    )
}

fn price_change_without_bba_row(
    asset_id: &str,
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
    absolute_quantity: &str,
) -> PolymarketOrderbookRow {
    row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![PolymarketPriceChangeEntry {
                asset_id: asset_id.to_owned(),
                price: "0.52".to_owned(),
                size: Some(absolute_quantity.to_owned()),
                side: PolymarketSide::Buy,
                best_bid: None,
                best_ask: None,
            }],
        },
    )
}

fn best_bid_ask_row(
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
) -> PolymarketOrderbookRow {
    row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id: UP_TOKEN_ID.to_owned(),
            best_bid: Some("0.52".to_owned()),
            best_ask: Some("0.53".to_owned()),
            spread: Some("0.01".to_owned()),
        },
    )
}

fn empty_bid_book_row(
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
) -> PolymarketOrderbookRow {
    row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        PolymarketOrderbookEvent::Book {
            asset_id: DOWN_TOKEN_ID.to_owned(),
            bids: vec![],
            asks: vec![PolymarketPriceLevel {
                price: "0.01".to_owned(),
                size: "100".to_owned(),
            }],
            last_trade_price: None,
        },
    )
}

fn empty_ask_book_row(
    epoch: &str,
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    received_at_utc_ms: i64,
) -> PolymarketOrderbookRow {
    row(
        epoch,
        admission_sequence,
        source_event_at_utc_ms,
        received_at_utc_ms,
        PolymarketOrderbookEvent::Book {
            asset_id: UP_TOKEN_ID.to_owned(),
            bids: vec![PolymarketPriceLevel {
                price: "0.99".to_owned(),
                size: "100".to_owned(),
            }],
            asks: vec![],
            last_trade_price: None,
        },
    )
}

fn tick_size_change_row(
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
    old_tick_size: &str,
    new_tick_size: &str,
) -> PolymarketOrderbookRow {
    row(
        "epoch-1",
        admission_sequence,
        source_event_at_utc_ms,
        source_event_at_utc_ms + 10,
        PolymarketOrderbookEvent::TickSizeChange {
            asset_id: UP_TOKEN_ID.to_owned(),
            old_tick_size: Some(old_tick_size.to_owned()),
            new_tick_size: new_tick_size.to_owned(),
        },
    )
}

fn public_trade_row(
    admission_sequence: u64,
    source_event_at_utc_ms: i64,
) -> PolymarketOrderbookRow {
    row(
        "epoch-1",
        admission_sequence,
        source_event_at_utc_ms,
        source_event_at_utc_ms + 10,
        PolymarketOrderbookEvent::LastTradePrice {
            asset_id: UP_TOKEN_ID.to_owned(),
            price: "0.52".to_owned(),
            size: Some("50".to_owned()),
            side: Some(PolymarketSide::Sell),
            fee_rate_bps: Some("0".to_owned()),
        },
    )
}

fn new_market_row(admission_sequence: u64) -> PolymarketOrderbookRow {
    row(
        "epoch-1",
        admission_sequence,
        950,
        960,
        PolymarketOrderbookEvent::NewMarket {
            clob_market_id: market().clob_market_id.0,
            question: Some("Bitcoin Up or Down".to_owned()),
            description: None,
            asset_ids: vec![UP_TOKEN_ID.to_owned(), DOWN_TOKEN_ID.to_owned()],
            outcomes: vec!["UP".to_owned(), "DOWN".to_owned()],
            event_message: None,
        },
    )
}

fn level_pairs(levels: &[PriceLevelV1]) -> Vec<(&str, &str)> {
    levels
        .iter()
        .map(|level| {
            (
                level.price_pusd_per_share.0.as_str(),
                level.quantity_shares.0.as_str(),
            )
        })
        .collect()
}

fn latest_orderbook_admission_sequence(event: &ExecutionMarketEventV3) -> u64 {
    event
        .lineage
        .source_evidence
        .iter()
        .filter(|source| source.source_kind == CanonicalSourceKind::PolymarketOrderbook)
        .filter_map(|source| source.collector_admission.as_ref())
        .map(|coordinate| coordinate.collector_admission_sequence)
        .max()
        .expect("归约事件必须保留直接使用的 orderbook 接纳证据")
}

#[test]
fn new_market_confirms_fixed_identity_without_inventing_an_active_transition() {
    let mut reducer = reducer();

    let batch = reducer
        .reduce_orderbook_group(vec![input(new_market_row(299))])
        .unwrap();

    assert!(batch.events.is_empty());
    assert_eq!(batch.reports.len(), 1);
    assert_eq!(
        batch.reports[0].operation,
        ReductionOperationKindV1::Confirm
    );
    assert_eq!(
        batch.reports[0].reason_code,
        "POLYMARKET_NEW_MARKET_IDENTITY_CONFIRMED"
    );
    assert!(matches!(
        &batch.reports[0].disposition,
        ReductionDispositionV1::Suppressed { operation_evidence }
            if operation_evidence.iter().any(|evidence| {
                evidence.role == ReductionEvidenceRoleV1::Input
                    && evidence.source_evidence.source_kind
                        == CanonicalSourceKind::PolymarketOrderbook
            })
    ));
}

#[test]
fn new_market_requires_the_exact_asset_set_and_accepts_trim_equivalent_labels() {
    let mut trim_equivalent = new_market_row(299);
    let PolymarketOrderbookEvent::NewMarket { outcomes, .. } = &mut trim_equivalent.event else {
        unreachable!("new_market fixture must retain its event kind");
    };
    *outcomes = vec![" UP ".to_owned(), " down ".to_owned()];

    let accepted = reducer()
        .reduce_orderbook_group(vec![input(trim_equivalent)])
        .unwrap();

    assert!(accepted.events.is_empty());
    assert_eq!(accepted.reports.len(), 1);

    let mut duplicate_asset = new_market_row(300);
    let PolymarketOrderbookEvent::NewMarket {
        asset_ids,
        outcomes,
        ..
    } = &mut duplicate_asset.event
    else {
        unreachable!("new_market fixture must retain its event kind");
    };
    *asset_ids = vec![UP_TOKEN_ID.to_owned(), UP_TOKEN_ID.to_owned()];
    *outcomes = vec!["UP".to_owned(), "UP".to_owned()];

    assert!(
        reducer()
            .reduce_orderbook_group(vec![input(duplicate_asset)])
            .is_err()
    );
}

#[test]
fn first_book_emits_one_initial_snapshot_with_canonical_levels_and_initial_tick() {
    let mut reducer = reducer();
    let batch = reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();

    let (event, snapshot) = batch
        .events
        .iter()
        .find_map(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => Some((event, snapshot)),
            _ => None,
        })
        .expect("首个完整 book 必须形成初始 L2 快照");

    assert_eq!(snapshot.asset, up_asset());
    assert_eq!(
        snapshot.initial_tick_size_pusd_per_share,
        ExactDecimal("0.01".to_owned())
    );
    assert_eq!(snapshot.source_sequence_epoch_id, "epoch-1");
    assert_eq!(snapshot.source_cursor, 300);
    assert_eq!(
        level_pairs(&snapshot.bids),
        vec![("0.52", "250"), ("0.5", "100")]
    );
    assert_eq!(
        level_pairs(&snapshot.asks),
        vec![("0.53", "260"), ("0.55", "100")]
    );
    assert_eq!(event.time.source_event_at_utc_ms, Some(1_000));
    assert_eq!(event.time.received_at_utc_ms, 1_010);
    assert!(
        event
            .lineage
            .source_evidence
            .iter()
            .any(|source| { source.source_kind == CanonicalSourceKind::PolymarketMarketMetadata })
    );
    assert!(event.lineage.source_evidence.iter().any(|source| {
        source.source_kind == CanonicalSourceKind::PolymarketOrderbook
            && source
                .collector_admission
                .as_ref()
                .is_some_and(|coordinate| coordinate.collector_admission_sequence == 300)
    }));
}

#[test]
fn later_full_books_confirm_identical_state_and_delete_missing_levels_with_conservation() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();

    let confirmation = reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 301, 1_001, 1_011, "250"))])
        .unwrap();
    assert!(confirmation.events.is_empty());
    assert_eq!(confirmation.reports.len(), 1);
    assert_eq!(
        confirmation.reports[0].operation,
        ReductionOperationKindV1::Confirm
    );
    assert_eq!(
        confirmation.reports[0].reason_code,
        "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION"
    );
    assert!(matches!(
        &confirmation.reports[0].disposition,
        ReductionDispositionV1::Suppressed { operation_evidence }
            if !operation_evidence.is_empty()
    ));

    let mut level_removed = book_row("epoch-1", 302, 1_002, 1_012, "250");
    let PolymarketOrderbookEvent::Book { bids, .. } = &mut level_removed.event else {
        unreachable!();
    };
    bids.retain(|level| level.price != "0.52");

    let deletion = reducer
        .reduce_orderbook_group(vec![input(level_removed)])
        .unwrap();
    let transition = deletion
        .events
        .iter()
        .find_map(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                if transition.side == BookSide::Bid
                    && transition.price_pusd_per_share.0 == "0.52" =>
            {
                Some((event, transition))
            }
            _ => None,
        })
        .expect("后续完整 book 缺少旧档位时必须形成删除转换");
    assert_eq!(transition.1.quantity_before_shares.0, "250");
    assert_eq!(transition.1.quantity_after_shares.0, "0");
    assert_eq!(
        transition.1.basis,
        LevelExecutionBasis::BookResyncDeltaInference
    );
    assert_eq!(transition.0.quality.resync, ResyncQuality::ResyncBoundary);

    let report = deletion
        .reports
        .iter()
        .find(|report| report.reason_code == "POLYMARKET_BOOK_RESYNC_DELTA")
        .expect("完整 book 删除档位必须形成归约报告");
    assert_eq!(report.operation, ReductionOperationKindV1::Delete);
    assert_eq!(
        report.quantity_conservation,
        Some(data_pipeline_core::reduction::QuantityConservationV1 {
            quantity_before: ExactDecimal("250".to_owned()),
            quantity_after: ExactDecimal("0".to_owned()),
            quantity_added: ExactDecimal("0".to_owned()),
            quantity_reduced: ExactDecimal("250".to_owned()),
        })
    );
    assert!(matches!(
        &report.disposition,
        ReductionDispositionV1::Produced { result_event_id }
            if result_event_id == &transition.0.event_id
    ));
}

#[test]
fn same_source_millisecond_uses_admission_order_and_bba_does_not_change_quantity() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();

    let batch = reducer
        .reduce_orderbook_group(vec![
            input(price_change_row("epoch-1", 304, 1_100, 1_114, "110")),
            input(best_bid_ask_row("epoch-1", 302, 1_100, 1_112)),
            input(price_change_row("epoch-1", 301, 1_100, 1_111, "210")),
            input(price_change_row("epoch-1", 303, 1_100, 1_113, "160")),
        ])
        .unwrap();

    let transitions = batch
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                if transition.asset.token_id.0 == UP_TOKEN_ID
                    && transition.side == BookSide::Bid
                    && transition.price_pusd_per_share.0 == "0.52" =>
            {
                Some((event, transition))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        transitions
            .iter()
            .map(|(_, transition)| (
                transition.quantity_before_shares.0.as_str(),
                transition.quantity_after_shares.0.as_str(),
            ))
            .collect::<Vec<_>>(),
        vec![("250", "210"), ("210", "160"), ("160", "110")]
    );
    assert_eq!(
        transitions
            .iter()
            .map(|(event, _)| latest_orderbook_admission_sequence(event))
            .collect::<Vec<_>>(),
        vec![301, 303, 304]
    );
    assert!(
        batch
            .events
            .iter()
            .any(|event| matches!(&event.payload, ExecutionMarketEventPayloadV3::BestBidAsk(_)))
    );
}

#[test]
fn different_source_milliseconds_use_event_time_even_when_admission_order_is_reversed() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();

    let batch = reducer
        .reduce_orderbook_group(vec![
            input(price_change_row("epoch-1", 301, 1_100, 1_110, "210")),
            input(price_change_row("epoch-1", 302, 1_099, 1_111, "200")),
        ])
        .unwrap();
    let transitions = batch
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                if transition.asset.token_id.0 == UP_TOKEN_ID
                    && transition.side == BookSide::Bid
                    && transition.price_pusd_per_share.0 == "0.52" =>
            {
                Some((event, transition))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        transitions
            .iter()
            .map(|(event, transition)| (
                event.time.source_event_at_utc_ms,
                transition.quantity_before_shares.0.as_str(),
                transition.quantity_after_shares.0.as_str(),
            ))
            .collect::<Vec<_>>(),
        vec![(Some(1_099), "250", "200"), (Some(1_100), "200", "210")]
    );
}

#[test]
fn bba_merge_and_overwrite_keep_the_direct_state_evidence() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();

    let first = reducer
        .reduce_orderbook_group(vec![input(best_bid_ask_row("epoch-1", 301, 1_100, 1_110))])
        .unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.reports.len(), 1);
    assert_eq!(first.reports[0].operation, ReductionOperationKindV1::Merge);

    let mut next_row = best_bid_ask_row("epoch-1", 302, 1_200, 1_210);
    let PolymarketOrderbookEvent::BestBidAsk {
        best_bid,
        best_ask,
        spread,
        ..
    } = &mut next_row.event
    else {
        unreachable!();
    };
    *best_bid = Some("0.51".to_owned());
    *best_ask = Some("0.54".to_owned());
    *spread = Some("0.03".to_owned());
    let second = reducer
        .reduce_orderbook_group(vec![input(next_row)])
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.reports.len(), 1);
    assert_eq!(
        second.reports[0].operation,
        ReductionOperationKindV1::Overwrite
    );
    let event = &second.events[0];
    for expected_sequence in [301, 302] {
        assert!(event.lineage.source_evidence.iter().any(|evidence| {
            evidence
                .collector_admission
                .as_ref()
                .is_some_and(|coordinate| {
                    coordinate.collector_admission_sequence == expected_sequence
                })
        }));
    }
}

#[test]
fn empty_book_boundary_quotes_remain_raw_evidence_and_map_to_absent_bba_sides() {
    let mut price_change_reducer = reducer();
    price_change_reducer
        .reduce_orderbook_group(vec![input(empty_bid_book_row(
            "epoch-1", 300, 1_000, 1_010,
        ))])
        .unwrap();
    let price_change = row(
        "epoch-1",
        301,
        1_100,
        1_110,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![PolymarketPriceChangeEntry {
                asset_id: DOWN_TOKEN_ID.to_owned(),
                price: "0.01".to_owned(),
                size: Some("90".to_owned()),
                side: PolymarketSide::Sell,
                best_bid: Some("0".to_owned()),
                best_ask: Some("0.01".to_owned()),
            }],
        },
    );
    let preserved_price_change = price_change.clone();
    let price_change_batch = price_change_reducer
        .reduce_orderbook_group(vec![input(price_change)])
        .unwrap();
    assert_eq!(
        preserved_price_change.event,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![PolymarketPriceChangeEntry {
                asset_id: DOWN_TOKEN_ID.to_owned(),
                price: "0.01".to_owned(),
                size: Some("90".to_owned()),
                side: PolymarketSide::Sell,
                best_bid: Some("0".to_owned()),
                best_ask: Some("0.01".to_owned()),
            }],
        }
    );
    assert!(price_change_batch.events.iter().any(|event| matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::BestBidAsk(best)
            if best.best_bid_pusd_per_share.is_none()
                && best.best_ask_pusd_per_share.as_ref().is_some_and(|ask| ask.0 == "0.01")
    )));

    let mut standalone_reducer = reducer();
    standalone_reducer
        .reduce_orderbook_group(vec![input(empty_ask_book_row(
            "epoch-1", 300, 1_000, 1_010,
        ))])
        .unwrap();
    let standalone = row(
        "epoch-1",
        301,
        1_100,
        1_110,
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id: UP_TOKEN_ID.to_owned(),
            best_bid: Some("0.99".to_owned()),
            best_ask: Some("1".to_owned()),
            spread: Some("0.01".to_owned()),
        },
    );
    let preserved_standalone = standalone.clone();
    let standalone_batch = standalone_reducer
        .reduce_orderbook_group(vec![input(standalone)])
        .unwrap();
    assert_eq!(
        preserved_standalone.event,
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id: UP_TOKEN_ID.to_owned(),
            best_bid: Some("0.99".to_owned()),
            best_ask: Some("1".to_owned()),
            spread: Some("0.01".to_owned()),
        }
    );
    assert!(standalone_batch.events.iter().any(|event| matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::BestBidAsk(best)
            if best.best_bid_pusd_per_share.as_ref().is_some_and(|bid| bid.0 == "0.99")
                && best.best_ask_pusd_per_share.is_none()
    )));
}

#[test]
fn tick_change_must_match_the_asset_current_tick() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();

    let batch = reducer
        .reduce_orderbook_group(vec![input(tick_size_change_row(
            301, 1_100, "0.01", "0.001",
        ))])
        .unwrap();
    let (first_event, change) = batch
        .events
        .iter()
        .find_map(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::TickSizeChange(change) => Some((event, change)),
            _ => None,
        })
        .expect("合法 tick_size_change 必须形成规范事件");
    assert_eq!(
        change
            .old_tick_size_pusd_per_share
            .as_ref()
            .map(|value| value.0.as_str()),
        Some("0.01")
    );
    assert_eq!(change.new_tick_size_pusd_per_share.0, "0.001");
    let first_change_evidence = first_event
        .lineage
        .source_evidence
        .iter()
        .find(|evidence| {
            evidence
                .collector_admission
                .as_ref()
                .is_some_and(|coordinate| coordinate.collector_admission_sequence == 301)
        })
        .expect("首次动态 tick 证据必须进入事件谱系")
        .clone();

    let replayed = reducer
        .reduce_orderbook_group(vec![input(tick_size_change_row(
            302, 1_100, "0.01", "0.001",
        ))])
        .expect("相同动态 tick 重投必须被幂等确认");
    assert!(
        replayed.events.is_empty(),
        "相同动态 tick 重投不得重复产生事件"
    );
    assert!(replayed.reports.iter().any(|report| {
        report.operation == ReductionOperationKindV1::Confirm
            && report.reason_code == "POLYMARKET_TICK_CONFIRMATION"
            && matches!(
                report.disposition,
                ReductionDispositionV1::Suppressed { .. }
            )
    }));

    let second = reducer
        .reduce_orderbook_group(vec![input(tick_size_change_row(
            303, 1_200, "0.001", "0.0001",
        ))])
        .unwrap();
    let second_event = second
        .events
        .iter()
        .find(|event| {
            matches!(
                &event.payload,
                ExecutionMarketEventPayloadV3::TickSizeChange(_)
            )
        })
        .expect("第二次合法 tick_size_change 必须形成规范事件");
    assert!(
        second_event
            .lineage
            .source_evidence
            .contains(&first_change_evidence),
        "第二次 tick 变化必须保留其当前 tick 的直接来源证据"
    );

    let conflicting = reducer.reduce_orderbook_group(vec![input(tick_size_change_row(
        304, 1_300, "0.01", "0.00001",
    ))]);
    assert!(
        conflicting.is_err(),
        "消息声明的 old_tick_size 与当前状态不符时必须拒绝归约"
    );
}

#[test]
fn collector_admission_sequence_cannot_repeat_with_a_different_source_time_in_one_epoch() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();
    let repeated_coordinate = price_change_row("epoch-1", 300, 1_001, 1_011, "200");

    assert!(
        reducer
            .reduce_orderbook_group(vec![input(repeated_coordinate)])
            .is_err()
    );
}

#[test]
fn empty_price_change_is_explicit_no_output_instead_of_disappearing() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();
    let empty_change = row(
        "epoch-1",
        301,
        1_001,
        1_011,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![PolymarketPriceChangeEntry {
                asset_id: UP_TOKEN_ID.to_owned(),
                price: "0.52".to_owned(),
                size: None,
                side: PolymarketSide::Buy,
                best_bid: None,
                best_ask: None,
            }],
        },
    );

    let batch = reducer
        .reduce_orderbook_group(vec![input(empty_change)])
        .unwrap();
    assert!(batch.events.is_empty());
    assert_eq!(batch.reports.len(), 1);
    assert_eq!(batch.reports[0].operation, ReductionOperationKindV1::Reject);
    assert!(matches!(
        &batch.reports[0].disposition,
        ReductionDispositionV1::NoOutput { operation_evidence }
            if operation_evidence.len() == 1
    ));
}

#[test]
fn identical_message_retry_preserves_both_operation_roles() {
    let mut reducer = reducer();
    let repeated = input(book_row("epoch-1", 300, 1_000, 1_010, "250"));
    let batch = reducer
        .reduce_orderbook_group(vec![repeated.clone(), repeated])
        .unwrap();
    let report = batch
        .reports
        .iter()
        .find(|report| report.reason_code == "POLYMARKET_IDENTICAL_MESSAGE_RETRY")
        .expect("identical retry must produce a suppression report");
    let ReductionDispositionV1::Suppressed { operation_evidence } = &report.disposition else {
        panic!("identical retry must be suppressed");
    };
    assert_eq!(operation_evidence.len(), 2);
    assert!(
        operation_evidence
            .iter()
            .any(|evidence| { evidence.role == ReductionEvidenceRoleV1::MatchedExisting })
    );
    assert!(
        operation_evidence
            .iter()
            .any(|evidence| { evidence.role == ReductionEvidenceRoleV1::IncomingDuplicate })
    );
}

#[test]
fn new_epoch_marks_only_fresh_l2_missing_and_other_components_cannot_restore_it() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();
    reducer
        .reduce_orderbook_group(vec![input(best_bid_ask_row("epoch-1", 301, 1_100, 1_110))])
        .unwrap();

    let invalidated = reducer
        .reduce_orderbook_group(vec![input(price_change_without_bba_row(
            UP_TOKEN_ID,
            "epoch-2",
            1,
            2_000,
            2_010,
            "70",
        ))])
        .expect("新 epoch 完整 book 前的绝对价位更新必须局部拒绝，不能中止归约");
    assert_eq!(invalidated.events.len(), 1);
    let missing_event = &invalidated.events[0];
    assert_eq!(missing_event.time.source_event_at_utc_ms, Some(2_000));
    assert_eq!(missing_event.time.received_at_utc_ms, 2_010);
    assert_eq!(latest_orderbook_admission_sequence(missing_event), 1);
    assert_eq!(
        missing_event.quality.availability,
        MarketDataQuality::Missing
    );
    assert_eq!(missing_event.quality.resync, ResyncQuality::Required);
    let ExecutionMarketEventPayloadV3::QualityBoundary(missing) = &missing_event.payload else {
        panic!("epoch 切换必须为原本 Fresh 的 L2 生成资产范围质量边界");
    };
    assert_eq!(missing.scope, QualityScopeV1::Asset(up_asset()));
    assert_eq!(missing.previous_quality, MarketDataQuality::Fresh);
    assert_eq!(missing.current_quality, MarketDataQuality::Missing);
    assert_eq!(missing.reason_code, "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED");
    assert_eq!(invalidated.reports.len(), 2);
    assert_eq!(
        invalidated.reports[0].reason_code,
        "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED"
    );
    assert_eq!(
        invalidated.reports[0].operation,
        ReductionOperationKindV1::Reject
    );
    assert!(matches!(
        &invalidated.reports[0].disposition,
        ReductionDispositionV1::Produced { result_event_id }
            if result_event_id == &missing_event.event_id
    ));
    assert_eq!(
        invalidated.reports[1].reason_code,
        "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED"
    );
    assert!(matches!(
        &invalidated.reports[1].disposition,
        ReductionDispositionV1::NoOutput { operation_evidence }
            if operation_evidence.len() == 1
    ));
    assert!(invalidated.events.iter().all(|event| !matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset),
            ..
        }) if asset.token_id.0 == DOWN_TOKEN_ID
    )));

    let bba = reducer
        .reduce_orderbook_group(vec![input(best_bid_ask_row("epoch-2", 2, 2_001, 2_011))])
        .unwrap();
    assert!(
        bba.events
            .iter()
            .any(|event| matches!(&event.payload, ExecutionMarketEventPayloadV3::BestBidAsk(_)))
    );

    let mut tick = tick_size_change_row(3, 2_002, "0.01", "0.001");
    tick.source_sequence_epoch_id = "epoch-2".to_owned();
    let tick = reducer.reduce_orderbook_group(vec![input(tick)]).unwrap();
    assert!(tick.events.iter().any(|event| matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::TickSizeChange(_)
    )));

    let mut trade = public_trade_row(4, 2_003);
    trade.source_sequence_epoch_id = "epoch-2".to_owned();
    let trade = reducer.reduce_orderbook_group(vec![input(trade)]).unwrap();
    assert!(trade.events.iter().any(|event| matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::PublicTrade(_)
    )));

    let still_missing = reducer
        .reduce_orderbook_group(vec![input(price_change_without_bba_row(
            UP_TOKEN_ID,
            "epoch-2",
            5,
            2_004,
            2_014,
            "60",
        ))])
        .unwrap();
    assert!(still_missing.events.is_empty());
    assert_eq!(still_missing.reports.len(), 1);
    assert_eq!(
        still_missing.reports[0].reason_code,
        "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED"
    );

    let next_epoch_still_missing = reducer
        .reduce_orderbook_group(vec![input(price_change_without_bba_row(
            UP_TOKEN_ID,
            "epoch-3",
            1,
            3_000,
            3_010,
            "50",
        ))])
        .unwrap();
    assert!(
        next_epoch_still_missing.events.is_empty(),
        "已经 Missing 的 L2 在再次切换 epoch 时不得制造 Missing 到 Missing 边界"
    );
    assert_eq!(next_epoch_still_missing.reports.len(), 1);
    assert_eq!(
        next_epoch_still_missing.reports[0].reason_code,
        "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED"
    );
}

#[test]
fn new_epoch_book_rebuilds_from_empty_then_recovers_only_that_asset() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();
    reducer
        .reduce_orderbook_group(vec![input(book_row_for_asset(
            DOWN_TOKEN_ID,
            "epoch-1",
            301,
            1_001,
            1_011,
            "250",
        ))])
        .unwrap();

    let rebuilt = reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-2", 1, 2_000, 2_010, "250"))])
        .unwrap();
    assert!(rebuilt.events.iter().all(|event| !matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(_)
    )));
    assert_eq!(rebuilt.events.len(), 7);
    assert_eq!(rebuilt.reports.len(), 7);

    for (index, asset_id) in [UP_TOKEN_ID, DOWN_TOKEN_ID].iter().enumerate() {
        let event = &rebuilt.events[index];
        let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload else {
            panic!("新 epoch 必须先把每个原本 Fresh 的资产 L2 标成 Missing");
        };
        assert_eq!(boundary.previous_quality, MarketDataQuality::Fresh);
        assert_eq!(boundary.current_quality, MarketDataQuality::Missing);
        assert!(matches!(
            &boundary.scope,
            QualityScopeV1::Asset(asset) if asset.token_id.0 == *asset_id
        ));
        assert!(matches!(
            &rebuilt.reports[index].disposition,
            ReductionDispositionV1::Produced { result_event_id }
                if result_event_id == &event.event_id
        ));
    }

    let transitions = rebuilt
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.payload {
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                Some((index, event, transition))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(transitions.len(), 4);
    for (index, event, transition) in &transitions {
        assert_eq!(transition.asset.token_id.0, UP_TOKEN_ID);
        assert_eq!(transition.quantity_before_shares.0, "0");
        assert_ne!(transition.quantity_after_shares.0, "0");
        assert_eq!(
            transition.basis,
            LevelExecutionBasis::BookResyncDeltaInference
        );
        assert_eq!(event.quality.availability, MarketDataQuality::Fresh);
        assert_eq!(event.quality.resync, ResyncQuality::ResyncBoundary);
        let report = &rebuilt.reports[*index];
        assert_eq!(report.operation, ReductionOperationKindV1::Merge);
        assert_eq!(report.reason_code, "POLYMARKET_BOOK_RESYNC_DELTA");
        let conservation = report
            .quantity_conservation
            .as_ref()
            .expect("每个重建价位都必须记录数量守恒");
        assert_eq!(conservation.quantity_before.0, "0");
        assert_eq!(
            conservation.quantity_after,
            transition.quantity_after_shares
        );
        assert_eq!(
            conservation.quantity_added,
            transition.quantity_after_shares
        );
        assert_eq!(conservation.quantity_reduced.0, "0");
        assert!(matches!(
            &report.disposition,
            ReductionDispositionV1::Produced { result_event_id }
                if result_event_id == &event.event_id
        ));
    }
    assert!(
        rebuilt
            .reports
            .iter()
            .all(|report| { report.operation != ReductionOperationKindV1::Delete })
    );

    let recovery_index = rebuilt.events.len() - 1;
    assert!(
        transitions
            .iter()
            .all(|(index, _, _)| *index < recovery_index)
    );
    let recovery_event = &rebuilt.events[recovery_index];
    assert_eq!(
        recovery_event.quality.availability,
        MarketDataQuality::Fresh
    );
    assert_eq!(recovery_event.quality.resync, ResyncQuality::ResyncBoundary);
    let ExecutionMarketEventPayloadV3::QualityBoundary(recovery) = &recovery_event.payload else {
        panic!("全部重建价位之后必须形成 Missing 到 Fresh 的恢复边界");
    };
    assert_eq!(recovery.scope, QualityScopeV1::Asset(up_asset()));
    assert_eq!(recovery.previous_quality, MarketDataQuality::Missing);
    assert_eq!(recovery.current_quality, MarketDataQuality::Fresh);
    assert_eq!(
        recovery.reason_code,
        "POLYMARKET_L2_EPOCH_BASELINE_RESTORED"
    );
    assert_eq!(
        rebuilt.reports[recovery_index].reason_code,
        "POLYMARKET_L2_EPOCH_BASELINE_RESTORED"
    );
    assert!(matches!(
        &rebuilt.reports[recovery_index].disposition,
        ReductionDispositionV1::Produced { result_event_id }
            if result_event_id == &recovery_event.event_id
    ));

    let down_still_missing = reducer
        .reduce_orderbook_group(vec![input(price_change_without_bba_row(
            DOWN_TOKEN_ID,
            "epoch-2",
            2,
            2_001,
            2_011,
            "80",
        ))])
        .unwrap();
    assert!(down_still_missing.events.is_empty());
    assert_eq!(down_still_missing.reports.len(), 1);
    assert_eq!(
        down_still_missing.reports[0].reason_code,
        "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED"
    );

    let same_epoch_confirmation = reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-2", 3, 2_002, 2_012, "250"))])
        .unwrap();
    assert!(same_epoch_confirmation.events.is_empty());
    assert_eq!(same_epoch_confirmation.reports.len(), 1);
    assert_eq!(
        same_epoch_confirmation.reports[0].reason_code,
        "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION"
    );
}

#[test]
fn empty_book_still_restores_missing_l2_after_an_epoch_switch() {
    let mut reducer = reducer();
    reducer
        .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
        .unwrap();
    let mut empty = book_row("epoch-2", 1, 2_000, 2_010, "250");
    let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut empty.event else {
        unreachable!();
    };
    bids.clear();
    asks.clear();

    let batch = reducer.reduce_orderbook_group(vec![input(empty)]).unwrap();
    assert_eq!(batch.events.len(), 2);
    assert_eq!(batch.reports.len(), 2);
    assert!(batch.events.iter().all(|event| !matches!(
        &event.payload,
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(_)
            | ExecutionMarketEventPayloadV3::LevelExecutionTransition(_)
    )));
    let ExecutionMarketEventPayloadV3::QualityBoundary(missing) = &batch.events[0].payload else {
        panic!("空 book 重建前仍必须先产生 Fresh 到 Missing 边界");
    };
    assert_eq!(missing.current_quality, MarketDataQuality::Missing);
    let ExecutionMarketEventPayloadV3::QualityBoundary(recovered) = &batch.events[1].payload else {
        panic!("空 book 本身也是完整基准，必须产生 Missing 到 Fresh 边界");
    };
    assert_eq!(recovered.previous_quality, MarketDataQuality::Missing);
    assert_eq!(recovered.current_quality, MarketDataQuality::Fresh);
    assert_eq!(
        batch.events[1].quality.resync,
        ResyncQuality::ResyncBoundary
    );
}

#[test]
fn public_trade_and_level_delta_never_claim_exact_shared_causality_or_double_reduce() {
    for trade_first in [true, false] {
        let mut reducer = reducer();
        reducer
            .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
            .unwrap();

        let trade = input(public_trade_row(
            if trade_first { 301 } else { 302 },
            if trade_first { 1_100 } else { 1_101 },
        ));
        let change = input(price_change_row(
            "epoch-1",
            if trade_first { 302 } else { 301 },
            if trade_first { 1_101 } else { 1_100 },
            if trade_first { 1_111 } else { 1_110 },
            "200",
        ));
        let batch = reducer
            .reduce_orderbook_group(if trade_first {
                vec![trade, change]
            } else {
                vec![change, trade]
            })
            .unwrap();

        assert_eq!(
            batch
                .events
                .iter()
                .filter(|event| matches!(
                    &event.payload,
                    ExecutionMarketEventPayloadV3::PublicTrade(_)
                ))
                .count(),
            1
        );
        let transition = batch
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                    if transition.price_pusd_per_share.0 == "0.52" =>
                {
                    Some(transition)
                }
                _ => None,
            })
            .expect("盘口绝对数量变化必须独立形成一次转换");
        assert_eq!(transition.quantity_before_shares.0, "250");
        assert_eq!(transition.quantity_after_shares.0, "200");
        assert_eq!(transition.queue_consumption_quantity_shares.0, "50");
        assert_eq!(transition.attributed_trade_quantity_shares.0, "0");
        assert_eq!(transition.already_covered_quantity_shares.0, "0");
        assert_eq!(
            transition.source_attribution_quality,
            SourceAttributionQuality::Unattributed
        );
        assert_eq!(
            transition.queue_result_quality,
            QueueResultQuality::DeterministicInferred
        );
    }
}

#[test]
fn public_trade_and_later_full_book_never_claim_exact_shared_causality_or_double_reduce() {
    for trade_first in [true, false] {
        let mut reducer = reducer();
        reducer
            .reduce_orderbook_group(vec![input(book_row("epoch-1", 300, 1_000, 1_010, "250"))])
            .unwrap();

        let trade = input(public_trade_row(
            if trade_first { 301 } else { 302 },
            if trade_first { 1_100 } else { 1_101 },
        ));
        let book = input(book_row(
            "epoch-1",
            if trade_first { 302 } else { 301 },
            if trade_first { 1_101 } else { 1_100 },
            if trade_first { 1_111 } else { 1_110 },
            "200",
        ));
        let batch = reducer
            .reduce_orderbook_group(if trade_first {
                vec![trade, book]
            } else {
                vec![book, trade]
            })
            .unwrap();

        assert_eq!(
            batch
                .events
                .iter()
                .filter(|event| matches!(
                    &event.payload,
                    ExecutionMarketEventPayloadV3::PublicTrade(_)
                ))
                .count(),
            1
        );
        let transition = batch
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                    if transition.price_pusd_per_share.0 == "0.52" =>
                {
                    Some(transition)
                }
                _ => None,
            })
            .expect("后续完整盘口的绝对数量变化必须独立形成一次转换");
        assert_eq!(transition.quantity_before_shares.0, "250");
        assert_eq!(transition.quantity_after_shares.0, "200");
        assert_eq!(transition.queue_consumption_quantity_shares.0, "50");
        assert_eq!(transition.attributed_trade_quantity_shares.0, "0");
        assert_eq!(transition.already_covered_quantity_shares.0, "0");
        assert_eq!(
            transition.source_attribution_quality,
            SourceAttributionQuality::Unattributed
        );
        assert_eq!(
            transition.queue_result_quality,
            QueueResultQuality::DeterministicInferred
        );
    }
}
