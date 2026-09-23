// 仅用于已有本地归档 ignored 测试。
const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
use std::path::Path;

use contracts::{
    AssetIdentityV1, BookSide, CanonicalEventRecordV1, CanonicalSourceKind, ConditionId,
    ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, KnownMarketLifecycleState,
    LevelExecutionBasis, MappedMarketLifecycleStateV1, MarketIdentityV2,
    MarketResolutionTerminalV3, MarketWindowV2, OutcomeIdentityV1, Sha256Hex, TokenId,
};
use data_pipeline_core::reduction::{
    PolymarketAssetContextV1, PolymarketMarketContextV1, PolymarketMetadataInputV1,
    PolymarketMetadataReducer, PolymarketOrderbookInputV1, PolymarketReducer,
    PolymarketTerminalMerger, ReductionDispositionV1, ReductionEvidenceRoleV1,
    ReductionOperationKindV1, ReductionReportEntryV1,
};
use data_pipeline_core::typed_raw::{
    BinanceAggTradeRecord, MetadataResolutionFollowupStatus, MetadataSourceEvidenceOrigin,
    PolymarketMetadataRowKind, PolymarketOrderbookEvent, PolymarketOrderbookRow, PolymarketSide,
    RtdsFreshnessStatus, TypedRawRow, extract_metadata_initial_tick, map_metadata_outcomes,
};
use data_pipeline_core::{
    project_record_event_time_index_entry, project_record_visibility_index_entry,
    source_evidence_from_typed_raw,
};
use data_pipeline_training::typed_raw::{
    open_validated_typed_raw_parquet, read_first_validated_typed_raw_batch,
};
use data_pipeline_training::validate_direct_source_daily_manifest_admission;
use serde::Serialize;
use sha2::{Digest, Sha256};

fn assert_frozen_source_evidence_mapping(
    row: &TypedRawRow,
    manifest_sha256: &Sha256Hex,
    file_sha256: &Sha256Hex,
) {
    let evidence = source_evidence_from_typed_raw(row, manifest_sha256, file_sha256)
        .expect("validated real typed_raw row must map to source evidence");
    let (source_kind, source_time, received_time) = match row {
        TypedRawRow::PolymarketOrderbook(row) => (
            CanonicalSourceKind::PolymarketOrderbook,
            row.timestamp,
            row.timestamp_received,
        ),
        TypedRawRow::PolymarketMarketMetadata(row) => (
            CanonicalSourceKind::PolymarketMarketMetadata,
            match row.source_evidence_origin {
                Some(MetadataSourceEvidenceOrigin::MarketResolvedWs) => {
                    row.ws_platform_timestamp_utc
                }
                _ => None,
            },
            row.timestamp_received,
        ),
        TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => (
            CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
            (row.freshness_status != RtdsFreshnessStatus::InvalidTimestamp)
                .then_some(row.payload_timestamp),
            row.timestamp_received,
        ),
        TypedRawRow::BinanceDepthSnapshot(row) => (
            CanonicalSourceKind::BinanceDepthSnapshot,
            None,
            row.timestamp_received,
        ),
        TypedRawRow::BinanceDepthUpdate(row) => (
            CanonicalSourceKind::BinanceDepthUpdate,
            Some(row.event_time),
            row.timestamp_received,
        ),
        TypedRawRow::BinanceAggTrade(row) => (
            CanonicalSourceKind::BinanceAggtrade,
            match &row.record {
                BinanceAggTradeRecord::Trade { trade_time, .. } => Some(*trade_time),
                BinanceAggTradeRecord::InvalidInput { .. } => None,
            },
            row.timestamp_received,
        ),
    };
    assert_eq!(evidence.source_kind, source_kind);
    assert_eq!(evidence.source_event_at_utc_ms, source_time);
    assert_eq!(evidence.received_at_utc_ms, received_time);
    assert_eq!(evidence.input_manifest_sha256, *manifest_sha256);
    assert_eq!(evidence.input_file_sha256, *file_sha256);
    assert!(evidence.source_native_identity.is_some() || evidence.collector_admission.is_some());
    if matches!(
        row,
        TypedRawRow::PolymarketMarketMetadata(_) | TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_)
    ) {
        assert_eq!(evidence.collector_admission, None);
    }
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-04 dataset"]
fn real_six_current_schemas_decode_through_the_validated_parquet_boundary() {
    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json");
    let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
        .expect("2026-08-04 real input must pass complete admission");
    let manifest_sha256 = Sha256Hex(admitted.manifest_sha256().to_owned());
    let expected = [
        ("polymarket_market", "polymarket_orderbook_v3"),
        (
            "polymarket_market_metadata",
            "polymarket_market_metadata_v5",
        ),
        (
            "polymarket_rtds_chainlink_btc_usd",
            "polymarket_rtds_chainlink_btc_usd_v4",
        ),
        ("binance_depth_snapshot", "binance_depth_snapshot_v3"),
        ("binance_depth_update", "binance_depth_update_v4"),
        ("binance_aggtrade", "binance_aggtrade_v7"),
    ];

    for (source_kind, schema_version) in expected {
        if source_kind == "polymarket_market_metadata" {
            let mut decoded_rows = 0_usize;
            for file in admitted
                .files()
                .iter()
                .filter(|file| file.source_kind() == source_kind)
            {
                assert_eq!(file.schema_version(), schema_version);
                let file_sha256 = Sha256Hex(file.sha256().to_owned());
                for batch in open_validated_typed_raw_parquet(file, 64)
                    .unwrap_or_else(|error| panic!("{source_kind} real file open failed: {error}"))
                {
                    let rows = batch.unwrap_or_else(|error| {
                        panic!("{source_kind} real full-day decode failed: {error}")
                    });
                    assert!(
                        rows.iter()
                            .all(|row| row.source_kind().as_str() == source_kind)
                    );
                    for row in &rows {
                        assert_frozen_source_evidence_mapping(row, &manifest_sha256, &file_sha256);
                    }
                    decoded_rows += rows.len();
                }
            }
            assert!(decoded_rows > 0, "{source_kind} full day must not be empty");
            continue;
        }

        let file = admitted
            .files()
            .iter()
            .find(|file| file.source_kind() == source_kind)
            .unwrap_or_else(|| panic!("missing admitted real file for {source_kind}"));
        assert_eq!(file.schema_version(), schema_version);
        let file_sha256 = Sha256Hex(file.sha256().to_owned());

        let rows = read_first_validated_typed_raw_batch(file, 64)
            .unwrap_or_else(|error| panic!("{source_kind} real decode failed: {error}"));
        assert!(
            !rows.is_empty(),
            "{source_kind} first batch must not be empty"
        );
        assert!(
            rows.iter()
                .all(|row| row.source_kind().as_str() == source_kind),
            "{source_kind} decoded row kind mismatch"
        );
        for row in &rows {
            assert_frozen_source_evidence_mapping(row, &manifest_sha256, &file_sha256);
        }
    }
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-04 dataset"]
fn real_same_source_millisecond_orderbook_messages_preserve_captured_order() {
    const FILE_SHA256: &str = "9e4c76c5e66df075e3a1d56ab32e5b5346cb819ea0f5db050da6d9ca34bf9746";
    const SOURCE_TIME_MS: i64 = 1_785_805_200_427;
    const MARKET: &str = "0x0273e1adec113acd16e85a26a82599f114e19c48e8ee0db22910d95535bc4037";
    const EPOCH: &str = "61e04849dd5494a454df50a3a46ca48fa2dfa85fcadd8885e2ebfa39895942d1";
    const ASSET: &str =
        "43645622479853717098868934274362793054298038835382952916817061855337420640678";

    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json");
    let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
        .expect("2026-08-04 real input must pass complete admission");
    let file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == FILE_SHA256)
        .expect("fixed real orderbook file must be admitted");

    let read_transitions = |batch_size| {
        let mut transitions = Vec::new();
        for batch in open_validated_typed_raw_parquet(file, batch_size)
            .expect("fixed real orderbook file must open")
        {
            for decoded in batch.expect("fixed real orderbook batch must decode") {
                let TypedRawRow::PolymarketOrderbook(row) = decoded else {
                    panic!("fixed orderbook file decoded as another source kind");
                };
                if row.timestamp != Some(SOURCE_TIME_MS)
                    || row.market != MARKET
                    || row.source_sequence_epoch_id != EPOCH
                {
                    continue;
                }
                let PolymarketOrderbookEvent::PriceChange { price_changes } = row.event else {
                    continue;
                };
                let Some(change) = price_changes.iter().find(|change| {
                    change.asset_id == ASSET
                        && change.price == "0.52"
                        && change.side == PolymarketSide::Buy
                }) else {
                    continue;
                };
                transitions.push((
                    row.collector_admission_sequence,
                    row.timestamp_received,
                    change.size.clone(),
                    price_changes
                        .iter()
                        .map(|change| change.asset_id.clone())
                        .collect::<Vec<_>>(),
                ));
                if row.collector_admission_sequence == 140_535_305 {
                    return transitions;
                }
            }
        }
        transitions
    };

    let expected = vec![
        (
            140_535_301,
            1_785_805_200_435,
            Some("210".to_owned()),
            vec![
                ASSET.to_owned(),
                "78511619933201429830171676522042853588141772677959714883557571797218199722184"
                    .to_owned(),
            ],
        ),
        (
            140_535_304,
            1_785_805_200_436,
            Some("160".to_owned()),
            vec![
                ASSET.to_owned(),
                "78511619933201429830171676522042853588141772677959714883557571797218199722184"
                    .to_owned(),
            ],
        ),
        (
            140_535_305,
            1_785_805_200_436,
            Some("110".to_owned()),
            vec![
                ASSET.to_owned(),
                "78511619933201429830171676522042853588141772677959714883557571797218199722184"
                    .to_owned(),
            ],
        ),
    ];
    assert_eq!(read_transitions(1), expected);
    assert_eq!(read_transitions(8_192), expected);
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-04 dataset"]
fn real_same_source_millisecond_reducer_is_invariant_to_read_batching() {
    const ORDERBOOK_FILE_SHA256: &str =
        "9e4c76c5e66df075e3a1d56ab32e5b5346cb819ea0f5db050da6d9ca34bf9746";
    const METADATA_FILE_SHA256: &str =
        "c3cabb79787469a2e649cde74878f63ac1ba5b86b08077acd70a272638b137a6";
    const SOURCE_TIME_MS: i64 = 1_785_805_200_427;
    const MARKET: &str = "0x0273e1adec113acd16e85a26a82599f114e19c48e8ee0db22910d95535bc4037";
    const EPOCH: &str = "61e04849dd5494a454df50a3a46ca48fa2dfa85fcadd8885e2ebfa39895942d1";
    const UP_ASSET: &str =
        "43645622479853717098868934274362793054298038835382952916817061855337420640678";
    const DOWN_ASSET: &str =
        "78511619933201429830171676522042853588141772677959714883557571797218199722184";
    const BOOK_SEQUENCES: [u64; 2] = [140_535_165, 140_535_166];
    const PRICE_CHANGE_FIRST_SEQUENCE: u64 = 140_535_301;
    const PRICE_CHANGE_LAST_SEQUENCE: u64 = 140_535_305;
    type PriceChangeEntrySignature = (
        String,
        String,
        Option<String>,
        PolymarketSide,
        Option<String>,
        Option<String>,
    );
    type PriceChangeMessageSignature = (u64, Vec<PriceChangeEntrySignature>);

    fn semantic_digest<T: Serialize>(value: &T) -> String {
        Sha256::digest(serde_json::to_vec(value).expect("semantic summary must serialize"))
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn target_level_transitions(events: &[ExecutionMarketEventV3]) -> Vec<(u64, String, String)> {
        events
            .iter()
            .filter_map(|event| {
                let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) =
                    &event.payload
                else {
                    return None;
                };
                if transition.asset.token_id.0 != UP_ASSET
                    || transition.side != BookSide::Bid
                    || transition.price_pusd_per_share.0 != "0.52"
                    || transition.basis != LevelExecutionBasis::LevelDeltaInference
                {
                    return None;
                }
                let trigger_sequence = event
                    .lineage
                    .source_evidence
                    .iter()
                    .filter(|evidence| {
                        evidence.source_kind == CanonicalSourceKind::PolymarketOrderbook
                    })
                    .filter_map(|evidence| evidence.collector_admission.as_ref())
                    .map(|coordinate| coordinate.collector_admission_sequence)
                    .max()
                    .expect("target transition must retain its orderbook admission evidence");
                if !(PRICE_CHANGE_FIRST_SEQUENCE..=PRICE_CHANGE_LAST_SEQUENCE)
                    .contains(&trigger_sequence)
                {
                    return None;
                }
                Some((
                    trigger_sequence,
                    transition.quantity_before_shares.0.clone(),
                    transition.quantity_after_shares.0.clone(),
                ))
            })
            .collect()
    }

    fn price_change_signature(rows: &[PolymarketOrderbookRow]) -> Vec<PriceChangeMessageSignature> {
        rows.iter()
            .map(|row| {
                let PolymarketOrderbookEvent::PriceChange { price_changes } = &row.event else {
                    panic!("fixed seq301..305 rows must remain price_change messages");
                };
                (
                    row.collector_admission_sequence,
                    price_changes
                        .iter()
                        .map(|change| {
                            (
                                change.asset_id.clone(),
                                change.price.clone(),
                                change.size.clone(),
                                change.side,
                                change.best_bid.clone(),
                                change.best_ask.clone(),
                            )
                        })
                        .collect(),
                )
            })
            .collect()
    }

    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json");
    let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
        .expect("2026-08-04 real input must pass complete admission");
    let manifest_sha256 = Sha256Hex(admitted.manifest_sha256().to_owned());
    let orderbook_file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == ORDERBOOK_FILE_SHA256)
        .expect("fixed real orderbook file must be admitted");
    let metadata_file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == METADATA_FILE_SHA256)
        .expect("fixed real metadata file must be admitted");
    let metadata_file_sha256 = Sha256Hex(metadata_file.sha256().to_owned());

    let (metadata_row, identity_evidence) = 'metadata: {
        for batch in open_validated_typed_raw_parquet(metadata_file, 64)
            .expect("fixed real metadata file must open")
        {
            for decoded in batch.expect("fixed real metadata batch must decode") {
                let TypedRawRow::PolymarketMarketMetadata(row) = &decoded else {
                    panic!("fixed metadata file decoded as another source kind");
                };
                if row.condition_id == MARKET
                    && row.metadata_row_kind == PolymarketMetadataRowKind::MetadataSnapshot
                {
                    let evidence = source_evidence_from_typed_raw(
                        &decoded,
                        &manifest_sha256,
                        &metadata_file_sha256,
                    )
                    .expect("fixed real metadata row must map to identity evidence");
                    break 'metadata (row.clone(), evidence);
                }
            }
        }
        panic!("fixed market metadata snapshot is missing");
    };
    assert_eq!(metadata_row.clob_market_id, "3296857");
    assert_eq!(metadata_row.market_start_utc, 1_785_805_200_000);
    assert_eq!(metadata_row.market_end_utc, 1_785_805_500_000);
    assert_eq!(
        metadata_row.asset_ids,
        format!(r#"["{UP_ASSET}","{DOWN_ASSET}"]"#)
    );
    let tick = extract_metadata_initial_tick(
        metadata_row
            .source_evidence_payload_json
            .as_deref()
            .expect("fixed metadata snapshot must retain its source payload"),
    )
    .expect("fixed metadata tick evidence must decode")
    .expect("fixed metadata snapshot must contain an initial tick");
    assert_eq!(tick.exact, ExactDecimal("0.01".to_owned()));
    let outcomes = map_metadata_outcomes(
        &metadata_row.asset_ids,
        metadata_row.outcomes_raw.as_deref(),
        metadata_row.outcome_prices_raw.as_deref(),
    )
    .expect("fixed metadata identity mapping must decode");
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| (
                outcome.asset_id.as_str(),
                outcome
                    .outcome
                    .as_deref()
                    .expect("outcome label must exist"),
            ))
            .collect::<Vec<_>>(),
        vec![(UP_ASSET, "Up"), (DOWN_ASSET, "Down")]
    );

    let context = PolymarketMarketContextV1 {
        market: MarketIdentityV2 {
            condition_id: ConditionId(metadata_row.condition_id.clone()),
            clob_market_id: contracts::ClobMarketId(metadata_row.clob_market_id.clone()),
            window: MarketWindowV2 {
                start_at_utc_ms: metadata_row.market_start_utc,
                end_at_utc_ms: metadata_row.market_end_utc,
            },
        },
        assets: outcomes
            .into_iter()
            .enumerate()
            .map(|(index, outcome)| PolymarketAssetContextV1 {
                asset: AssetIdentityV1 {
                    token_id: TokenId(outcome.asset_id),
                    outcome: OutcomeIdentityV1 {
                        index: index as u32,
                        label: outcome.outcome.expect("fixed outcome label must exist"),
                    },
                },
                initial_tick_size_pusd_per_share: tick.exact.clone(),
            })
            .collect(),
        identity_source_evidence: vec![identity_evidence],
    };
    let orderbook_file_sha256 = Sha256Hex(orderbook_file.sha256().to_owned());

    let run = |read_batch_size: usize| {
        let mut reducer = PolymarketReducer::new(context.clone(), "polymarket-reducer-v1")
            .expect("fixed real market context must initialize the reducer");
        let mut events = Vec::<ExecutionMarketEventV3>::new();
        let mut reports = Vec::<ReductionReportEntryV1>::new();
        let mut baseline_books = Vec::<PolymarketOrderbookRow>::new();
        let mut target_rows = Vec::<PolymarketOrderbookRow>::new();
        let mut saw_last_target = false;

        for batch in open_validated_typed_raw_parquet(orderbook_file, read_batch_size)
            .expect("fixed real orderbook file must open")
        {
            let mut inputs = Vec::new();
            for decoded in batch.expect("fixed real orderbook batch must decode") {
                let TypedRawRow::PolymarketOrderbook(row) = decoded else {
                    panic!("fixed orderbook file decoded as another source kind");
                };
                if row.market != MARKET || row.source_sequence_epoch_id != EPOCH {
                    continue;
                }
                let is_replay_row = (BOOK_SEQUENCES[0]..=PRICE_CHANGE_LAST_SEQUENCE)
                    .contains(&row.collector_admission_sequence);
                let is_baseline = BOOK_SEQUENCES.contains(&row.collector_admission_sequence)
                    && matches!(&row.event, PolymarketOrderbookEvent::Book { .. });
                let is_target = row.timestamp == Some(SOURCE_TIME_MS)
                    && (PRICE_CHANGE_FIRST_SEQUENCE..=PRICE_CHANGE_LAST_SEQUENCE)
                        .contains(&row.collector_admission_sequence)
                    && matches!(&row.event, PolymarketOrderbookEvent::PriceChange { .. });
                if !is_replay_row {
                    continue;
                }
                if is_baseline {
                    baseline_books.push(row.clone());
                }
                if is_target {
                    saw_last_target |=
                        row.collector_admission_sequence == PRICE_CHANGE_LAST_SEQUENCE;
                    target_rows.push(row.clone());
                }
                let typed = TypedRawRow::PolymarketOrderbook(row.clone());
                let evidence = source_evidence_from_typed_raw(
                    &typed,
                    &manifest_sha256,
                    &orderbook_file_sha256,
                )
                .expect("fixed real orderbook row must map to source evidence");
                inputs.push(PolymarketOrderbookInputV1 { row, evidence });
            }
            if !inputs.is_empty() {
                let batch = reducer
                    .reduce_orderbook_group(inputs)
                    .expect("fixed real baseline and seq301..305 must reduce");
                events.extend(batch.events);
                reports.extend(batch.reports);
            }
            if saw_last_target {
                break;
            }
        }

        assert_eq!(
            baseline_books
                .iter()
                .map(|row| row.collector_admission_sequence)
                .collect::<Vec<_>>(),
            BOOK_SEQUENCES
        );
        assert!(baseline_books.iter().all(|row| {
            row.timestamp == Some(1_785_805_200_271)
                && matches!(
                    &row.event,
                    PolymarketOrderbookEvent::Book { bids, asks, .. }
                        if !bids.is_empty() && !asks.is_empty()
                )
        }));
        assert!(baseline_books.iter().any(|row| matches!(
            &row.event,
            PolymarketOrderbookEvent::Book {
                asset_id,
                bids,
                ..
            } if asset_id == UP_ASSET
                && bids.iter().any(|level| level.price == "0.52" && level.size == "280")
        )));
        assert_eq!(
            target_rows
                .iter()
                .map(|row| row.collector_admission_sequence)
                .collect::<Vec<_>>(),
            (PRICE_CHANGE_FIRST_SEQUENCE..=PRICE_CHANGE_LAST_SEQUENCE).collect::<Vec<_>>()
        );
        (events, reports, baseline_books, target_rows)
    };

    let small_batches = run(1);
    let large_batches = run(8_192);
    assert_eq!(small_batches.2, large_batches.2);
    assert_eq!(small_batches.3, large_batches.3);

    let expected_price_changes = vec![
        (
            140_535_301,
            vec![
                (
                    UP_ASSET.to_owned(),
                    "0.52".to_owned(),
                    Some("210".to_owned()),
                    PolymarketSide::Buy,
                    Some("0.52".to_owned()),
                    Some("0.53".to_owned()),
                ),
                (
                    DOWN_ASSET.to_owned(),
                    "0.48".to_owned(),
                    Some("210".to_owned()),
                    PolymarketSide::Sell,
                    Some("0.47".to_owned()),
                    Some("0.48".to_owned()),
                ),
            ],
        ),
        (
            140_535_302,
            vec![
                (
                    UP_ASSET.to_owned(),
                    "0.01".to_owned(),
                    Some("99410.59".to_owned()),
                    PolymarketSide::Buy,
                    Some("0.52".to_owned()),
                    Some("0.53".to_owned()),
                ),
                (
                    DOWN_ASSET.to_owned(),
                    "0.99".to_owned(),
                    Some("99410.59".to_owned()),
                    PolymarketSide::Sell,
                    Some("0.47".to_owned()),
                    Some("0.48".to_owned()),
                ),
            ],
        ),
        (
            140_535_303,
            vec![
                (
                    DOWN_ASSET.to_owned(),
                    "0.46".to_owned(),
                    Some("84.39".to_owned()),
                    PolymarketSide::Buy,
                    Some("0.47".to_owned()),
                    Some("0.48".to_owned()),
                ),
                (
                    UP_ASSET.to_owned(),
                    "0.54".to_owned(),
                    Some("84.39".to_owned()),
                    PolymarketSide::Sell,
                    Some("0.52".to_owned()),
                    Some("0.53".to_owned()),
                ),
            ],
        ),
        (
            140_535_304,
            vec![
                (
                    UP_ASSET.to_owned(),
                    "0.52".to_owned(),
                    Some("160".to_owned()),
                    PolymarketSide::Buy,
                    Some("0.52".to_owned()),
                    Some("0.53".to_owned()),
                ),
                (
                    DOWN_ASSET.to_owned(),
                    "0.48".to_owned(),
                    Some("160".to_owned()),
                    PolymarketSide::Sell,
                    Some("0.47".to_owned()),
                    Some("0.48".to_owned()),
                ),
            ],
        ),
        (
            140_535_305,
            vec![
                (
                    UP_ASSET.to_owned(),
                    "0.52".to_owned(),
                    Some("110".to_owned()),
                    PolymarketSide::Buy,
                    Some("0.52".to_owned()),
                    Some("0.53".to_owned()),
                ),
                (
                    DOWN_ASSET.to_owned(),
                    "0.48".to_owned(),
                    Some("110".to_owned()),
                    PolymarketSide::Sell,
                    Some("0.47".to_owned()),
                    Some("0.48".to_owned()),
                ),
            ],
        ),
    ];
    assert_eq!(
        price_change_signature(&small_batches.3),
        expected_price_changes
    );

    let expected_transitions = vec![
        (140_535_301, "260".to_owned(), "210".to_owned()),
        (140_535_304, "210".to_owned(), "160".to_owned()),
        (140_535_305, "160".to_owned(), "110".to_owned()),
    ];
    assert_eq!(
        target_level_transitions(&small_batches.0),
        expected_transitions
    );
    assert_eq!(
        target_level_transitions(&large_batches.0),
        expected_transitions
    );
    assert_eq!(
        semantic_digest(&small_batches.0),
        semantic_digest(&large_batches.0)
    );
    assert_eq!(
        semantic_digest(&small_batches.1),
        semantic_digest(&large_batches.1)
    );
    assert_eq!(small_batches.0, large_batches.0);
    assert_eq!(small_batches.1, large_batches.1);
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-03 and 2026-08-04 datasets"]
fn real_ws_terminal_metadata_pair_merges_once() {
    const CONTEXT_FILE_SHA256: &str =
        "aeb170577980936b737eaf791efbd0ae0f54ecd9473e9fa0ba76de2377f61f3f";
    const METADATA_FILE_SHA256: &str =
        "c3cabb79787469a2e649cde74878f63ac1ba5b86b08077acd70a272638b137a6";
    const ORDERBOOK_FILE_SHA256: &str =
        "ff24c1ffaeb8ab0e0f5e56ca2b7c55f7a9b5924db3d54a8f73b0be1be145ccb4";
    const MARKET: &str = "0x81f50a6d58f3da8906ffb5ee36e845733d3c9f84a9bbe6c67b27a50cfd6b082b";
    const CLOB_MARKET_ID: &str = "3296526";
    const EPOCH: &str = "61e04849dd5494a454df50a3a46ca48fa2dfa85fcadd8885e2ebfa39895942d1";
    const CLAIMANT_SEQUENCE: u64 = 138_794_672;
    const SOURCE_TIME_MS: i64 = 1_785_801_687_572;
    const RECEIVED_TIME_MS: i64 = 1_785_801_687_582;
    const UP_ASSET: &str =
        "72450649409583869117851001123870857724846138585089546340877708395109783362247";
    const DOWN_ASSET: &str =
        "46315352323998127784849921227498306058634684732866689381276334771330039999326";

    let context_manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-03/typed_raw/utc_day=2026-08-03/manifest.json");
    let context_admitted = validate_direct_source_daily_manifest_admission(&context_manifest_path)
        .expect("2026-08-03 real context input must pass complete admission");
    let context_manifest_sha256 = Sha256Hex(context_admitted.manifest_sha256().to_owned());
    let context_file = context_admitted
        .files()
        .iter()
        .find(|file| file.sha256() == CONTEXT_FILE_SHA256)
        .expect("fixed prior-day real metadata file must be admitted");
    let context_file_sha256 = Sha256Hex(context_file.sha256().to_owned());

    let (context_row, identity_evidence) = 'context: {
        for batch in open_validated_typed_raw_parquet(context_file, 64)
            .expect("fixed prior-day real metadata file must open")
        {
            for decoded in batch.expect("fixed prior-day real metadata batch must decode") {
                let TypedRawRow::PolymarketMarketMetadata(row) = &decoded else {
                    panic!("fixed metadata file decoded as another source kind");
                };
                if row.condition_id == MARKET
                    && row.metadata_row_kind == PolymarketMetadataRowKind::MetadataSnapshot
                {
                    let evidence = source_evidence_from_typed_raw(
                        &decoded,
                        &context_manifest_sha256,
                        &context_file_sha256,
                    )
                    .expect("fixed real metadata snapshot must map to identity evidence");
                    break 'context (row.as_ref().clone(), evidence);
                }
            }
        }
        panic!("fixed prior-day real metadata snapshot is missing");
    };
    assert_eq!(context_row.clob_market_id, CLOB_MARKET_ID);
    assert_eq!(context_row.market_start_utc, 1_785_801_300_000);
    assert_eq!(context_row.market_end_utc, 1_785_801_600_000);
    let tick = extract_metadata_initial_tick(
        context_row
            .source_evidence_payload_json
            .as_deref()
            .expect("fixed real metadata snapshot must retain its source payload"),
    )
    .expect("fixed real metadata tick evidence must decode")
    .expect("fixed real metadata snapshot must contain an initial tick");
    let outcomes = map_metadata_outcomes(
        &context_row.asset_ids,
        context_row.outcomes_raw.as_deref(),
        context_row.outcome_prices_raw.as_deref(),
    )
    .expect("fixed real metadata assets and outcomes must decode");
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| (outcome.asset_id.as_str(), outcome.outcome.as_deref()))
            .collect::<Vec<_>>(),
        vec![(UP_ASSET, Some("Up")), (DOWN_ASSET, Some("Down"))]
    );

    let terminal_manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json");
    let terminal_admitted =
        validate_direct_source_daily_manifest_admission(&terminal_manifest_path)
            .expect("2026-08-04 real terminal input must pass complete admission");
    let terminal_manifest_sha256 = Sha256Hex(terminal_admitted.manifest_sha256().to_owned());
    let metadata_file = terminal_admitted
        .files()
        .iter()
        .find(|file| file.sha256() == METADATA_FILE_SHA256)
        .expect("fixed real terminal metadata file must be admitted");
    let orderbook_file = terminal_admitted
        .files()
        .iter()
        .find(|file| file.sha256() == ORDERBOOK_FILE_SHA256)
        .expect("fixed real terminal orderbook file must be admitted");
    let metadata_file_sha256 = Sha256Hex(metadata_file.sha256().to_owned());
    let orderbook_file_sha256 = Sha256Hex(orderbook_file.sha256().to_owned());

    let (metadata_row, metadata_evidence) = 'metadata: {
        for batch in open_validated_typed_raw_parquet(metadata_file, 64)
            .expect("fixed real terminal metadata file must open")
        {
            for decoded in batch.expect("fixed real terminal metadata batch must decode") {
                let TypedRawRow::PolymarketMarketMetadata(row) = &decoded else {
                    panic!("fixed terminal metadata file decoded as another source kind");
                };
                if row.condition_id == MARKET
                    && row.metadata_row_kind == PolymarketMetadataRowKind::ResolutionTerminalWs
                    && row.ws_claimant_source_sequence_epoch_id.as_deref() == Some(EPOCH)
                    && row.ws_claimant_collector_admission_sequence == Some(CLAIMANT_SEQUENCE)
                {
                    let evidence = source_evidence_from_typed_raw(
                        &decoded,
                        &terminal_manifest_sha256,
                        &metadata_file_sha256,
                    )
                    .expect("fixed real terminal metadata row must map to source evidence");
                    break 'metadata (row.as_ref().clone(), evidence);
                }
            }
        }
        panic!("fixed real terminal metadata claimant is missing");
    };

    let (orderbook_row, orderbook_evidence) = 'orderbook: {
        for batch in open_validated_typed_raw_parquet(orderbook_file, 8_192)
            .expect("fixed real terminal orderbook file must open")
        {
            for decoded in batch.expect("fixed real terminal orderbook batch must decode") {
                let TypedRawRow::PolymarketOrderbook(row) = &decoded else {
                    panic!("fixed terminal orderbook file decoded as another source kind");
                };
                if row.market == MARKET
                    && row.source_sequence_epoch_id == EPOCH
                    && row.collector_admission_sequence == CLAIMANT_SEQUENCE
                    && matches!(row.event, PolymarketOrderbookEvent::MarketResolved { .. })
                {
                    let evidence = source_evidence_from_typed_raw(
                        &decoded,
                        &terminal_manifest_sha256,
                        &orderbook_file_sha256,
                    )
                    .expect("fixed real terminal orderbook row must map to source evidence");
                    break 'orderbook (row.clone(), evidence);
                }
            }
        }
        panic!("fixed real terminal orderbook claimant is missing");
    };

    assert_eq!(metadata_row.clob_market_id, CLOB_MARKET_ID);
    assert_eq!(metadata_row.ws_platform_timestamp_utc, Some(SOURCE_TIME_MS));
    assert_eq!(metadata_row.timestamp_received, RECEIVED_TIME_MS);
    assert_eq!(metadata_row.winning_asset_id.as_deref(), Some(UP_ASSET));
    assert!(metadata_row.outcomes_raw.is_none());
    assert!(metadata_row.outcome_prices_raw.is_none());
    assert_eq!(orderbook_row.timestamp, Some(SOURCE_TIME_MS));
    assert_eq!(orderbook_row.timestamp_received, RECEIVED_TIME_MS);
    let PolymarketOrderbookEvent::MarketResolved {
        clob_market_id,
        asset_ids,
        outcomes: terminal_outcomes,
        winning_asset_id,
        winning_outcome,
        ..
    } = &orderbook_row.event
    else {
        unreachable!("fixed claimant was selected as market_resolved");
    };
    assert_eq!(clob_market_id, CLOB_MARKET_ID);
    assert_eq!(asset_ids, &[UP_ASSET.to_owned(), DOWN_ASSET.to_owned()]);
    assert!(terminal_outcomes.is_empty());
    assert_eq!(winning_asset_id, UP_ASSET);
    assert_eq!(winning_outcome, "Up");

    let context = PolymarketMarketContextV1 {
        market: MarketIdentityV2 {
            condition_id: ConditionId(context_row.condition_id.clone()),
            clob_market_id: contracts::ClobMarketId(context_row.clob_market_id.clone()),
            window: MarketWindowV2 {
                start_at_utc_ms: context_row.market_start_utc,
                end_at_utc_ms: context_row.market_end_utc,
            },
        },
        assets: outcomes
            .into_iter()
            .enumerate()
            .map(|(index, outcome)| PolymarketAssetContextV1 {
                asset: AssetIdentityV1 {
                    token_id: TokenId(outcome.asset_id),
                    outcome: OutcomeIdentityV1 {
                        index: index as u32,
                        label: outcome
                            .outcome
                            .expect("fixed real outcome label must exist"),
                    },
                },
                initial_tick_size_pusd_per_share: tick.exact.clone(),
            })
            .collect(),
        identity_source_evidence: vec![identity_evidence.clone()],
    };
    assert_eq!(context.market.condition_id.0, metadata_row.condition_id);
    assert_eq!(
        context.market.window.start_at_utc_ms,
        metadata_row.market_start_utc
    );
    assert_eq!(
        context.market.window.end_at_utc_ms,
        metadata_row.market_end_utc
    );

    let mut merger =
        PolymarketTerminalMerger::new(context, "polymarket-terminal-real-regression-v1")
            .expect("fixed real market context must initialize the terminal merger");
    let metadata_input = PolymarketMetadataInputV1 {
        row: metadata_row,
        evidence: metadata_evidence.clone(),
    };
    let orderbook_input = PolymarketOrderbookInputV1 {
        row: orderbook_row,
        evidence: orderbook_evidence.clone(),
    };

    let first = merger
        .merge_ws_terminal(metadata_input.clone(), orderbook_input.clone())
        .expect("matching real WS terminal pair must merge");
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.reports.len(), 2);
    let event = &first.events[0];
    let ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
        MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            winning_outcome,
            payout_vector,
            mapping_rule_version,
        },
    ) = &event.payload
    else {
        panic!("matching real WS terminal pair must produce one resolved event");
    };
    assert_eq!(winning_asset_id, UP_ASSET);
    assert_eq!(winning_outcome, "Up");
    assert_eq!(
        mapping_rule_version,
        "polymarket-binary-winning-asset-one-hot-v1"
    );
    assert_eq!(
        payout_vector
            .iter()
            .map(|entry| (
                entry.asset.token_id.0.as_str(),
                entry.asset.outcome.index,
                entry.asset.outcome.label.as_str(),
                entry.payout_pusd_per_share.0.as_str(),
            ))
            .collect::<Vec<_>>(),
        vec![(DOWN_ASSET, 1, "Down", "0"), (UP_ASSET, 0, "Up", "1")]
    );
    assert_eq!(event.time.source_event_at_utc_ms, Some(SOURCE_TIME_MS));
    assert_eq!(event.time.received_at_utc_ms, RECEIVED_TIME_MS);
    assert_eq!(event.lineage.source_evidence.len(), 3);
    assert!(event.lineage.source_evidence.contains(&metadata_evidence));
    assert!(event.lineage.source_evidence.contains(&orderbook_evidence));
    assert!(event.lineage.source_evidence.contains(&identity_evidence));
    assert_eq!(first.reports[0].operation, ReductionOperationKindV1::Merge);
    assert!(matches!(
        &first.reports[0].disposition,
        ReductionDispositionV1::Produced { result_event_id }
            if result_event_id == &event.event_id
    ));
    data_pipeline_core::validate_execution_market_event_v3(event)
        .expect("real resolved event must satisfy the canonical contract");
    let ExecutionMarketEventPayloadV3::MarketLifecycleTransition(retired) =
        &first.events[1].payload
    else {
        panic!("qualified real WS terminal must retire the market");
    };
    assert_eq!(
        retired.previous_state,
        Some(MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Active,
        })
    );
    assert_eq!(
        retired.current_state,
        MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Retired,
        }
    );

    let duplicate = merger
        .merge_ws_terminal(metadata_input, orderbook_input)
        .expect("identical real WS terminal replay must be handled");
    assert!(duplicate.events.is_empty());
    assert_eq!(duplicate.reports.len(), 1);
    assert_eq!(
        duplicate.reports[0].operation,
        ReductionOperationKindV1::Suppress
    );
    assert_eq!(
        duplicate.reports[0].reason_code,
        "POLYMARKET_TERMINAL_IDENTICAL_DUPLICATE"
    );
    assert!(matches!(
        &duplicate.reports[0].disposition,
        ReductionDispositionV1::Suppressed { operation_evidence }
            if operation_evidence.len() == 5
                && operation_evidence.iter().any(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::Context
                        && evidence.source_evidence == identity_evidence
                })
                && operation_evidence.iter().any(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::MatchedExisting
                        && evidence.source_evidence == metadata_evidence
                })
                && operation_evidence.iter().any(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::MatchedExisting
                        && evidence.source_evidence == orderbook_evidence
                })
                && operation_evidence.iter().any(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::IncomingDuplicate
                        && evidence.source_evidence == metadata_evidence
                })
                && operation_evidence.iter().any(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::IncomingDuplicate
                        && evidence.source_evidence == orderbook_evidence
                })
    ));
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-04 dataset"]
fn real_gamma_terminal_is_visible_but_has_no_event_time_projection() {
    const FILE_SHA256: &str = "b6df4934fd53d66e90f9f4659953afb384fcb11dc475541cc185edc43b599e30";
    const SLUG: &str = "btc-updown-5m-1785830700";
    const CONDITION_ID: &str = "0x1cac37ac291097d2f37f0c802c3f1bbd7069682ff899ce2dff1e070a1ae349e1";
    const RECEIVED_AT: i64 = 1_785_831_360_020;
    const MARKET_END_AT: i64 = 1_785_831_000_000;
    const DOWN_ASSET: &str =
        "57340114296652696790348720787886408225965681507497145905165038453726151847705";

    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json");
    let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
        .expect("2026-08-04 real Gamma input must pass complete admission");
    let manifest_sha256 = Sha256Hex(admitted.manifest_sha256().to_owned());
    let file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == FILE_SHA256)
        .expect("fixed real Gamma metadata file must be admitted");
    let file_sha256 = Sha256Hex(file.sha256().to_owned());
    let mut snapshot = None;
    let mut terminal = None;
    for batch in open_validated_typed_raw_parquet(file, 64)
        .expect("fixed real Gamma metadata file must open")
    {
        for decoded in batch.expect("fixed real Gamma metadata batch must decode") {
            let TypedRawRow::PolymarketMarketMetadata(row) = &decoded else {
                panic!("fixed Gamma metadata file decoded as another source kind");
            };
            if row.slug != SLUG {
                continue;
            }
            let evidence = source_evidence_from_typed_raw(&decoded, &manifest_sha256, &file_sha256)
                .expect("fixed real Gamma row must map to source evidence");
            match row.metadata_row_kind {
                PolymarketMetadataRowKind::MetadataSnapshot if snapshot.is_none() => {
                    snapshot = Some(PolymarketMetadataInputV1 {
                        row: row.as_ref().clone(),
                        evidence,
                    });
                }
                PolymarketMetadataRowKind::ResolutionPollAttempt
                    if row.timestamp_received == RECEIVED_AT =>
                {
                    terminal = Some(PolymarketMetadataInputV1 {
                        row: row.as_ref().clone(),
                        evidence,
                    });
                }
                _ => {}
            }
        }
    }
    let snapshot = snapshot.expect("fixed real Gamma identity snapshot is missing");
    let terminal = terminal.expect("fixed real Gamma terminal fallback is missing");
    assert_eq!(snapshot.row.condition_id, CONDITION_ID);
    assert_eq!(terminal.row.market_end_utc, MARKET_END_AT);
    assert_eq!(terminal.row.closed_time_utc, Some(MARKET_END_AT + 18_000));
    assert_eq!(terminal.evidence.source_event_at_utc_ms, None);

    let (_, context, initial) = PolymarketMetadataReducer::from_identity_snapshot(
        snapshot,
        "polymarket-metadata-real-regression-v1",
    )
    .expect("real Gamma snapshot must build the production market context");
    assert_eq!(context.market.window.end_at_utc_ms, MARKET_END_AT);
    assert_eq!(initial.events.len(), 2);
    let mut merger =
        PolymarketTerminalMerger::new(context, "polymarket-terminal-real-regression-v1")
            .expect("real Gamma market context must initialize the terminal merger");

    let batch = merger
        .merge_gamma_terminal(terminal)
        .expect("real Gamma fallback must reduce without inventing platform time");

    assert_eq!(batch.events.len(), 3);
    assert_eq!(batch.reports.len(), 3);
    let ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
        MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            winning_outcome,
            ..
        },
    ) = &batch.events[0].payload
    else {
        panic!("real Gamma fallback must preserve the public result");
    };
    assert_eq!(winning_asset_id, DOWN_ASSET);
    assert_eq!(winning_outcome, "Down");
    for event in &batch.events {
        assert_eq!(event.time.source_event_at_utc_ms, None);
        assert_eq!(event.time.received_at_utc_ms, RECEIVED_AT);
        let record = CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(event.clone()));
        assert_eq!(
            project_record_event_time_index_entry(&record).unwrap(),
            None
        );
        assert_eq!(
            project_record_visibility_index_entry(&record)
                .unwrap()
                .received_at_utc_ms,
            RECEIVED_AT
        );
    }
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-05 dataset"]
fn real_waiting_exhaustion_summary_remains_audit_only() {
    const CONTEXT_FILE_SHA256: &str =
        "b1dbc1462b24856a6402eed60ecd6d1b4625c19f148070e4847d847276d85746";
    const SUMMARY_FILE_SHA256: &str =
        "8718d8af818172214abf5a77762f90fe3d8fb4905d4218917e7c162704cdfefe";
    const SLUG: &str = "btc-updown-5m-1785941700";
    const RECEIVED_AT: i64 = 1_785_942_480_031;

    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-05/typed_raw/utc_day=2026-08-05/manifest.json");
    let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
        .expect("2026-08-05 real waiting-summary input must pass complete admission");
    let manifest_sha256 = Sha256Hex(admitted.manifest_sha256().to_owned());
    let context_file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == CONTEXT_FILE_SHA256)
        .expect("fixed real context metadata file must be admitted");
    let summary_file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == SUMMARY_FILE_SHA256)
        .expect("fixed real waiting-summary metadata file must be admitted");
    let context_file_sha256 = Sha256Hex(context_file.sha256().to_owned());
    let summary_file_sha256 = Sha256Hex(summary_file.sha256().to_owned());

    let snapshot = 'snapshot: {
        for batch in open_validated_typed_raw_parquet(context_file, 64)
            .expect("fixed real context metadata file must open")
        {
            for decoded in batch.expect("fixed real context metadata batch must decode") {
                let TypedRawRow::PolymarketMarketMetadata(row) = &decoded else {
                    panic!("fixed context metadata file decoded as another source kind");
                };
                if row.slug == SLUG
                    && row.metadata_row_kind == PolymarketMetadataRowKind::MetadataSnapshot
                {
                    let evidence = source_evidence_from_typed_raw(
                        &decoded,
                        &manifest_sha256,
                        &context_file_sha256,
                    )
                    .expect("fixed real identity snapshot must map to source evidence");
                    break 'snapshot PolymarketMetadataInputV1 {
                        row: row.as_ref().clone(),
                        evidence,
                    };
                }
            }
        }
        panic!("fixed real identity snapshot is missing");
    };

    let summary = 'summary: {
        for batch in open_validated_typed_raw_parquet(summary_file, 64)
            .expect("fixed real waiting-summary metadata file must open")
        {
            for decoded in batch.expect("fixed real waiting-summary metadata batch must decode") {
                let TypedRawRow::PolymarketMarketMetadata(row) = &decoded else {
                    panic!("fixed waiting-summary file decoded as another source kind");
                };
                if row.slug == SLUG
                    && row.metadata_row_kind == PolymarketMetadataRowKind::ResolutionFollowupSummary
                {
                    let evidence = source_evidence_from_typed_raw(
                        &decoded,
                        &manifest_sha256,
                        &summary_file_sha256,
                    )
                    .expect("fixed real waiting summary must map to source evidence");
                    break 'summary PolymarketMetadataInputV1 {
                        row: row.as_ref().clone(),
                        evidence,
                    };
                }
            }
        }
        panic!("fixed real waiting summary is missing");
    };

    assert_eq!(summary.row.timestamp_received, RECEIVED_AT);
    assert_eq!(
        summary.row.resolution_followup_status,
        Some(MetadataResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal)
    );
    assert_eq!(summary.row.resolution_poll_attempts_recorded, Some(6));
    assert_eq!(summary.row.resolution_valid_response_count, Some(6));
    assert_eq!(summary.row.resolution_transport_unavailable_count, Some(0));
    assert_eq!(summary.row.resolution_missed_slot_count, Some(0));
    assert_eq!(summary.evidence.source_event_at_utc_ms, None);

    let summary_evidence = summary.evidence.clone();
    let (mut reducer, context, _) = PolymarketMetadataReducer::from_identity_snapshot(
        snapshot,
        "polymarket-metadata-real-regression-v1",
    )
    .expect("real identity snapshot must build the production market context");
    assert_eq!(context.market.condition_id.0, summary.row.condition_id);
    assert_eq!(context.market.clob_market_id.0, summary.row.clob_market_id);
    assert_eq!(
        context.market.window.start_at_utc_ms,
        summary.row.market_start_utc
    );
    assert_eq!(
        context.market.window.end_at_utc_ms,
        summary.row.market_end_utc
    );

    let batch = reducer
        .reduce_metadata(summary)
        .expect("real waiting summary must reduce to an audit-only rejection");

    assert!(batch.events.is_empty());
    assert_eq!(batch.reports.len(), 1);
    assert_eq!(batch.reports[0].operation, ReductionOperationKindV1::Reject);
    assert_eq!(
        batch.reports[0].reason_code,
        "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_AFTER_VALID_NONTERMINAL"
    );
    let ReductionDispositionV1::NoOutput { operation_evidence } = &batch.reports[0].disposition
    else {
        panic!("real waiting summary must not produce or suppress a canonical event");
    };
    assert!(operation_evidence.iter().any(|evidence| {
        evidence.source_evidence == summary_evidence
            && evidence.source_evidence.source_event_at_utc_ms.is_none()
    }));
}
