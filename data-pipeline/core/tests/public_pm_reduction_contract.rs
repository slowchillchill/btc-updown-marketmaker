use contracts::*;
use data_pipeline_core::pm_message::*;
use data_pipeline_core::pm_reduction::*;
use data_pipeline_core::public_reference::source_evidence_v3_from_existing;
use data_pipeline_core::reduction::{
    PolymarketAssetContextV1, PolymarketMarketContextV1, PolymarketOrderbookInputV1,
    PolymarketReducer, ReductionOperationKindV1,
};
use data_pipeline_core::source_evidence_from_typed_raw;
use data_pipeline_core::typed_raw::{
    POLYMARKET_ORDERBOOK_SCHEMA, PolymarketOrderbookEvent, PolymarketOrderbookRow,
    PolymarketPriceChangeEntry, PolymarketPriceLevel, PolymarketSide, TypedRawRow,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::str::FromStr;

type LevelKey = (String, bool, Decimal);

#[derive(Clone, Debug)]
struct ReadState<E = SourceEvidenceRefV3> {
    context: PmMarketContextV1<E>,
    epoch: Option<String>,
    assets: BTreeMap<String, PmAssetStateV1<E>>,
    quantities: BTreeMap<LevelKey, Decimal>,
    levels: BTreeMap<LevelKey, PmEvidencePairV1<E>>,
    absences: BTreeMap<LevelKey, PmEvidencePairV1<E>>,
    selected: BTreeMap<LevelKey, PmSelectedLevelV1<E>>,
}

fn key(token: &str, side: BookSide, price: Decimal) -> LevelKey {
    (token.to_owned(), side == BookSide::Ask, price)
}
fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

impl<E: PmEvidenceV1> ReadState<E> {
    fn new(context: PmMarketContextV1<E>) -> Self {
        let evidence = context.identity_source_evidence[0].clone();
        let assets = context
            .assets
            .iter()
            .map(|value| {
                (
                    value.asset.token_id.0.clone(),
                    PmAssetStateV1 {
                        has_initial_snapshot: false,
                        book_ready: false,
                        tick_size: value.initial_tick_size_pusd_per_share.clone(),
                        tick_direct_evidence: vec![evidence.clone()],
                        tick_frontier: evidence.clone(),
                        absence_baseline: None,
                        selection_baseline: None,
                        bba: None,
                        bba_reanchor_frontier: None,
                    },
                )
            })
            .collect();
        Self {
            context,
            epoch: None,
            assets,
            quantities: BTreeMap::new(),
            levels: BTreeMap::new(),
            absences: BTreeMap::new(),
            selected: BTreeMap::new(),
        }
    }
    fn apply<I>(&mut self, result: &PmMessageReductionV1<'_, E, I>) {
        for step in &result.steps {
            match &step.change {
                PmStepChangeV1::Epoch {
                    source_sequence_epoch_id,
                } => self.epoch = Some(source_sequence_epoch_id.clone()),
                PmStepChangeV1::ResetAsset {
                    asset,
                    absence_baseline,
                    bba_reanchor_frontier,
                } => {
                    let token = &asset.token_id.0;
                    self.quantities.retain(|(id, ..), _| id != token);
                    self.levels.retain(|(id, ..), _| id != token);
                    self.absences.retain(|(id, ..), _| id != token);
                    self.selected.retain(|(id, ..), _| id != token);
                    let state = self.assets.get_mut(token).unwrap();
                    state.book_ready = false;
                    state.absence_baseline = absence_baseline.clone();
                    state.selection_baseline = None;
                    state.bba = None;
                    state.bba_reanchor_frontier = bba_reanchor_frontier.clone();
                }
                PmStepChangeV1::InitialSnapshot {
                    asset,
                    bids,
                    asks,
                    evidence,
                    ..
                } => {
                    let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                    state.has_initial_snapshot = true;
                    state.book_ready = true;
                    for (side, values) in [
                        (BookSide::Bid, bids.as_ref()),
                        (BookSide::Ask, asks.as_ref()),
                    ] {
                        for value in values {
                            let key =
                                key(&asset.token_id.0, side, d(&value.price_pusd_per_share.0));
                            let quantity = d(&value.quantity_shares.0);
                            if !quantity.is_zero() {
                                self.quantities.insert(key.clone(), quantity);
                            }
                            self.levels.insert(key, evidence.clone());
                        }
                    }
                }
                PmStepChangeV1::LevelChange {
                    transition,
                    evidence,
                } => {
                    let key = key(
                        &transition.asset.token_id.0,
                        transition.side,
                        d(&transition.price_pusd_per_share.0),
                    );
                    assert_eq!(
                        self.quantities.get(&key).copied().unwrap_or(Decimal::ZERO),
                        d(&transition.quantity_before_shares.0)
                    );
                    let after = d(&transition.quantity_after_shares.0);
                    if after.is_zero() {
                        self.quantities.remove(&key);
                        self.levels.remove(&key);
                        self.absences.insert(key, evidence.clone());
                    } else {
                        self.quantities.insert(key.clone(), after);
                        self.absences.remove(&key);
                        self.levels.insert(key, evidence.clone());
                    }
                }
                PmStepChangeV1::LevelEvidenceOnly {
                    asset,
                    side,
                    price,
                    evidence,
                } => {
                    self.levels
                        .insert(key(&asset.token_id.0, *side, d(&price.0)), evidence.clone());
                }
                PmStepChangeV1::Selection {
                    asset,
                    side,
                    price,
                    selected,
                    ..
                } => {
                    self.selected
                        .insert(key(&asset.token_id.0, *side, d(&price.0)), selected.clone());
                }
                PmStepChangeV1::AbsenceBaseline {
                    asset,
                    evidence,
                    selected,
                } => {
                    let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                    state.absence_baseline = Some(evidence.clone());
                    state.selection_baseline = selected.clone();
                }
                PmStepChangeV1::BookReady { asset } => {
                    self.assets.get_mut(&asset.token_id.0).unwrap().book_ready = true
                }
                PmStepChangeV1::BestBidAsk { quote, evidence } => {
                    let state = self.assets.get_mut(&quote.asset.token_id.0).unwrap();
                    state.bba = Some(PmBbaStateV1 {
                        best_bid: quote.best_bid_pusd_per_share.clone(),
                        best_ask: quote.best_ask_pusd_per_share.clone(),
                        evidence: evidence.clone(),
                    });
                    state.bba_reanchor_frontier = None;
                }
                PmStepChangeV1::Tick { tick, evidence } => {
                    let state = self.assets.get_mut(&tick.asset.token_id.0).unwrap();
                    state.tick_size = tick.new_tick_size_pusd_per_share.clone();
                    state.tick_direct_evidence = vec![evidence.direct.clone()];
                    state.tick_frontier = evidence.frontier.clone();
                }
                PmStepChangeV1::PublicConfirmation { .. }
                | PmStepChangeV1::PublicTrade { .. }
                | PmStepChangeV1::Quality { .. } => {}
            }
        }
    }
}

impl<E: PmEvidenceV1> PmStateReadV1<E> for ReadState<E> {
    fn market(&self) -> &MarketIdentityV2 {
        &self.context.market
    }
    fn current_epoch(&self) -> Option<&str> {
        self.epoch.as_deref()
    }
    fn asset_state(&self, token: &str) -> Option<PmAssetReadV1<'_, E>> {
        self.assets.get(token).map(PmAssetStateV1::as_read)
    }
    fn quantity(&self, token: &str, side: BookSide, price: Decimal) -> Decimal {
        self.quantities
            .get(&key(token, side, price))
            .copied()
            .unwrap_or(Decimal::ZERO)
    }
    fn level_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PmEvidenceReadV1<'_, E>> {
        self.levels
            .get(&key(token, side, price))
            .map(PmEvidencePairV1::as_read)
    }
    fn absence_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PmEvidenceReadV1<'_, E>> {
        self.absences
            .get(&key(token, side, price))
            .map(PmEvidencePairV1::as_read)
    }
    fn selected_level(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<&PmSelectedLevelV1<E>> {
        self.selected.get(&key(token, side, price))
    }
    fn level_keys<'a>(
        &'a self,
        token: &'a str,
        side: BookSide,
    ) -> Box<dyn Iterator<Item = Decimal> + 'a> {
        Box::new(
            self.levels
                .keys()
                .chain(self.absences.keys())
                .chain(self.selected.keys())
                .filter(move |(id, ask, _)| id == token && *ask == (side == BookSide::Ask))
                .map(|(_, _, price)| *price),
        )
    }
}

impl ReadState {
    fn run<'m>(&mut self, message: &'m CanonicalPmMessageV1) -> PmMessageReductionV1<'m> {
        let parsed = parse_canonical_pm_message(message).unwrap();
        let result = reduce_pm_message(&self.context, self, &parsed).unwrap();
        self.apply(&result);
        result
    }
}

fn public_context() -> PmMarketContextV1 {
    let context = context();
    PmMarketContextV1 {
        market: context.market,
        assets: context.assets,
        identity_source_evidence: context
            .identity_source_evidence
            .iter()
            .map(source_evidence_v3_from_existing)
            .collect::<Result<_, _>>()
            .unwrap(),
        canonical_rule_version: "public-pm-v1".to_owned(),
    }
}
fn message(context: &PmMarketContextV1, row: &PolymarketOrderbookRow) -> CanonicalPmMessageV1 {
    canonical_pm_message_from_typed_raw(
        context,
        row,
        CanonicalInputRecordRefV1 {
            input_stream_id: "test-original-stream".to_owned(),
            input_position: row.collector_admission_sequence,
        },
    )
    .unwrap()
}
fn update(
    context: &PmMarketContextV1,
    seq: u64,
    source: i64,
    quantity: &str,
) -> CanonicalPmMessageV1 {
    message(
        context,
        &row(
            "epoch-a",
            seq,
            source,
            2_000 + seq as i64,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some(quantity))],
            },
        ),
    )
}
fn transitions<'a>(result: &'a PmMessageReductionV1<'_>) -> Vec<&'a LevelExecutionTransitionV3> {
    result
        .steps
        .iter()
        .filter_map(|step| match &step.change {
            PmStepChangeV1::LevelChange { transition, .. } => Some(transition),
            _ => None,
        })
        .collect()
}

fn sha(value: char) -> Sha256Hex {
    Sha256Hex(value.to_string().repeat(64))
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

fn asset(token: &str, index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token.to_owned()),
        outcome: OutcomeIdentityV1 {
            index,
            label: label.to_owned(),
        },
    }
}

fn context() -> PolymarketMarketContextV1 {
    PolymarketMarketContextV1 {
        market: market(),
        assets: vec![
            PolymarketAssetContextV1 {
                asset: asset("123", 0, "UP"),
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            },
            PolymarketAssetContextV1 {
                asset: asset("456", 1, "DOWN"),
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

fn row(
    epoch: &str,
    sequence: u64,
    source_time: i64,
    received_time: i64,
    event: PolymarketOrderbookEvent,
) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: received_time,
        timestamp: Some(source_time),
        market: market().condition_id.0,
        slug: Some("btc-updown-5m".to_owned()),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA.to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: received_time + 1,
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

fn book(quantity: &str) -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::Book {
        asset_id: "123".to_owned(),
        bids: vec![PolymarketPriceLevel {
            price: "0.5".to_owned(),
            size: quantity.to_owned(),
        }],
        asks: vec![PolymarketPriceLevel {
            price: "0.6".to_owned(),
            size: "80".to_owned(),
        }],
        last_trade_price: Some("0.55".to_owned()),
    }
}

fn change(quantity: Option<&str>) -> PolymarketPriceChangeEntry {
    PolymarketPriceChangeEntry {
        asset_id: "123".to_owned(),
        price: "0.5".to_owned(),
        size: quantity.map(str::to_owned),
        side: PolymarketSide::Buy,
        best_bid: None,
        best_ask: None,
    }
}

#[test]
fn legacy_complete_events_and_reports_keep_the_frozen_baseline() {
    let mut reducer = PolymarketReducer::new(context(), "pm-shared-baseline-v1").unwrap();
    let rows = [
        row("epoch-a", 1, 1_000, 1_500, book("100")),
        row(
            "epoch-a",
            2,
            1_100,
            1_400,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80")), change(Some("100"))],
            },
        ),
        row(
            "epoch-a",
            3,
            1_200,
            1_300,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: "123".to_owned(),
                best_bid: Some("0".to_owned()),
                best_ask: Some("1".to_owned()),
                spread: None,
            },
        ),
        row(
            "epoch-a",
            4,
            1_300,
            1_600,
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: "123".to_owned(),
                old_tick_size: Some("0.02".to_owned()),
                new_tick_size: "0.01".to_owned(),
            },
        ),
        row("epoch-a", 5, 1_400, 1_700, book("100")),
        row("epoch-b", 1, 800, 1_900, book("90")),
        row("epoch-c", 1, 700, 2_000, book("90")),
    ];
    let mut material = Vec::new();
    for row in rows {
        let batch = reducer.reduce_orderbook_group(vec![input(row)]).unwrap();
        material.push(serde_json::json!({
            "events": batch.events,
            "reports": batch.reports,
            "retained_state_entries": reducer.retained_state_entries(),
        }));
    }
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&material).unwrap())
    );
    assert_eq!(
        digest,
        "c785da762241eaedd9af60c03e7c8ef7f7bfc11a0605286139f32fa6144d5f64"
    );
}

#[test]
fn legacy_raw_full_book_still_rejects_explicit_zero_quantity() {
    let value = row("epoch-a", 1, 1_000, 1_100, book("0"));
    assert!(value.validate().is_err());
}

#[test]
fn legacy_source_time_reversal_is_not_the_new_public_selection_entry() {
    let mut reducer = PolymarketReducer::new(context(), "pm-shared-baseline-v1").unwrap();
    reducer
        .reduce_orderbook_group(vec![input(row(
            "epoch-a",
            81_856,
            1_605,
            1_660,
            book("4557.61"),
        ))])
        .unwrap();
    let result = reducer.reduce_orderbook_group(vec![input(row(
        "epoch-a",
        81_860,
        1_433,
        1_660,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("4362.61"))],
        },
    ))]);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("source-time groups must not move backwards")
    );
}

#[test]
fn ordered_members_keep_intermediate_quantities_and_evidence_without_netting() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_000, 1_500, book("100")));
    state.run(&first);
    let value = message(
        &context,
        &row(
            "epoch-a",
            2,
            1_100,
            1_400,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80")), change(Some("100"))],
            },
        ),
    );
    let result = state.run(&value);
    let changes = transitions(&result);
    assert_eq!(
        changes
            .iter()
            .map(|v| (&v.quantity_before_shares.0, &v.quantity_after_shares.0))
            .collect::<Vec<_>>(),
        vec![
            (&"100".to_owned(), &"80".to_owned()),
            (&"80".to_owned(), &"100".to_owned())
        ]
    );
    let steps = result
        .steps
        .iter()
        .filter(|v| matches!(v.change, PmStepChangeV1::LevelChange { .. }))
        .collect::<Vec<_>>();
    assert_eq!(steps[0].member_index, Some(0));
    assert_eq!(steps[1].member_index, Some(1));
    assert_eq!(
        steps[1].prestate.as_ref().unwrap().direct.source_message_id,
        value.lineage.source_evidence[0].source_message_id
    );
    assert_eq!(steps[1].dependency_time.source_event_at_utc_ms, Some(1_100));
    assert_eq!(steps[1].dependency_time.received_at_utc_ms, 1_500);
    assert_eq!(result.touched_level_entries, 1);
    assert_ne!(steps[0].derived_id, steps[1].derived_id);
    assert_ne!(steps[0].derived_id, value.event_id);
}

#[test]
fn newer_confirmation_blocks_older_change_without_replacing_inference_prestate() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_000, 1_500, book("100")));
    state.run(&first);
    let confirm = update(&context, 2, 1_300, "100");
    let confirm_result = state.run(&confirm);
    assert!(transitions(&confirm_result).is_empty());
    let selected = state
        .selected_level("123", BookSide::Bid, d("0.5"))
        .unwrap()
        .clone();
    assert_eq!(selected.confirmation, confirm.lineage.source_evidence[0]);
    assert_eq!(
        state
            .level_evidence("123", BookSide::Bid, d("0.5"))
            .unwrap()
            .to_owned()
            .direct,
        first.lineage.source_evidence[0]
    );
    let older = update(&context, 3, 1_200, "80");
    let result = state.run(&older);
    assert!(result.steps.is_empty());
    assert_eq!(state.quantity("123", BookSide::Bid, d("0.5")), d("100"));
    assert_eq!(
        state
            .selected_level("123", BookSide::Bid, d("0.5"))
            .unwrap(),
        &selected
    );
    let later = update(&context, 4, 1_400, "70");
    let result = state.run(&later);
    let transition = result
        .steps
        .iter()
        .find(|step| matches!(step.change, PmStepChangeV1::LevelChange { .. }))
        .unwrap();
    assert_eq!(
        transition.prestate.as_ref().unwrap().direct,
        first.lineage.source_evidence[0]
    );
    assert!(
        !transition
            .dependencies
            .contains(&confirm.lineage.source_evidence[0])
    );
}

#[test]
fn full_book_selects_each_price_and_absence_independently() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_000, 1_100, book("100")));
    state.run(&first);
    state.run(&update(&context, 2, 1_500, "120"));
    let older_book = message(
        &context,
        &row(
            "epoch-a",
            3,
            1_200,
            1_600,
            PolymarketOrderbookEvent::Book {
                asset_id: "123".to_owned(),
                bids: vec![
                    PolymarketPriceLevel {
                        price: "0.5".to_owned(),
                        size: "90".to_owned(),
                    },
                    PolymarketPriceLevel {
                        price: "0.4".to_owned(),
                        size: "20".to_owned(),
                    },
                ],
                asks: vec![],
                last_trade_price: None,
            },
        ),
    );
    let result = state.run(&older_book);
    assert_eq!(state.quantity("123", BookSide::Bid, d("0.5")), d("120"));
    assert_eq!(state.quantity("123", BookSide::Bid, d("0.4")), d("20"));
    assert_eq!(
        state.quantity("123", BookSide::Ask, d("0.6")),
        Decimal::ZERO
    );
    assert_eq!(
        result.touched_level_entries, 0,
        "完整快照只借用输入和前序，不能复制全簿到消息覆盖"
    );
    let empty = message(
        &context,
        &row(
            "epoch-a",
            4,
            1_700,
            1_800,
            PolymarketOrderbookEvent::Book {
                asset_id: "123".to_owned(),
                bids: vec![],
                asks: vec![],
                last_trade_price: None,
            },
        ),
    );
    state.run(&empty);
    state.run(&update(&context, 5, 1_600, "200"));
    assert_eq!(
        state.quantity("123", BookSide::Bid, d("0.5")),
        Decimal::ZERO
    );
    let mut unseen = change(Some("10"));
    unseen.price = "0.3".to_owned();
    let unseen = message(
        &context,
        &row(
            "epoch-a",
            6,
            1_600,
            1_900,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![unseen],
            },
        ),
    );
    state.run(&unseen);
    assert_eq!(
        state.quantity("123", BookSide::Bid, d("0.3")),
        Decimal::ZERO
    );
    assert!(
        state
            .selected_level("123", BookSide::Bid, d("0.3"))
            .is_none()
    );
}

#[test]
fn wholly_older_full_book_does_not_emit_a_current_confirmation() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_500, 1_600, book("100")));
    state.run(&first);
    let older = message(&context, &row("epoch-a", 2, 1_200, 1_700, book("90")));
    let result = state.run(&older);
    assert!(
        result.steps.is_empty(),
        "整条较旧全簿不得生成当前盘口确认或就绪写入"
    );
    assert!(result.reports.is_empty());
    assert_eq!(state.quantity("123", BookSide::Bid, d("0.5")), d("100"));
}

#[test]
fn empty_full_book_confirmation_follows_the_selected_absence_version() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let empty = || PolymarketOrderbookEvent::Book {
        asset_id: "123".to_owned(),
        bids: vec![],
        asks: vec![],
        last_trade_price: None,
    };
    let first_row = row("epoch-a", 1, 1_500, 1_600, empty());
    first_row.validate().unwrap();
    let first = message(&context, &first_row);
    state.run(&first);
    let original_absence = state.assets["123"].absence_baseline.clone();
    let original_selection = state.assets["123"].selection_baseline.clone();

    let older_row = row("epoch-a", 2, 1_200, 1_700, empty());
    older_row.validate().unwrap();
    let older = message(&context, &older_row);
    let result = state.run(&older);
    assert!(result.steps.is_empty());
    assert!(
        result.reports.is_empty(),
        "没有价位键的较旧全簿也不能确认较新的缺价基准"
    );
    assert_eq!(state.assets["123"].absence_baseline, original_absence);
    assert_eq!(state.assets["123"].selection_baseline, original_selection);

    for (sequence, source_time, received_time) in [(3, 1_600, 1_800), (4, 1_600, 1_900)] {
        let next_row = row("epoch-a", sequence, source_time, received_time, empty());
        next_row.validate().unwrap();
        let next = message(&context, &next_row);
        let result = state.run(&next);
        assert_eq!(result.reports.len(), 1);
        assert_eq!(
            result.reports[0].operation,
            ReductionOperationKindV1::Confirm
        );
        assert_eq!(
            result.reports[0].reason_code,
            "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION"
        );
        assert_eq!(
            state.assets["123"]
                .absence_baseline
                .as_ref()
                .unwrap()
                .direct,
            next.lineage.source_evidence[0]
        );
        assert_eq!(
            state.assets["123"]
                .selection_baseline
                .as_ref()
                .unwrap()
                .confirmation,
            next.lineage.source_evidence[0]
        );
    }
}

#[test]
fn legacy_empty_full_book_keeps_its_confirmation() {
    let mut reducer = PolymarketReducer::new(context(), "pm-shared-baseline-v1").unwrap();
    for (sequence, source_time) in [(1, 1_500), (2, 1_600), (3, 1_600)] {
        let empty = row(
            "epoch-a",
            sequence,
            source_time,
            1_600 + sequence as i64,
            PolymarketOrderbookEvent::Book {
                asset_id: "123".to_owned(),
                bids: vec![],
                asks: vec![],
                last_trade_price: None,
            },
        );
        empty.validate().unwrap();
        let result = reducer.reduce_orderbook_group(vec![input(empty)]).unwrap();
        if sequence > 1 {
            assert!(result.events.is_empty());
            assert_eq!(result.reports.len(), 1);
            assert_eq!(
                result.reports[0].operation,
                ReductionOperationKindV1::Confirm
            );
            assert_eq!(
                result.reports[0].reason_code,
                "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION"
            );
        }
    }
}

#[test]
fn canonical_zero_presence_and_snapshot_absence_do_not_replace_inference_metadata() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_000, 1_100, book("100")));
    state.run(&first);
    let source_message = update(&context, 2, 1_200, "100");
    let zero = build_canonical_pm_message(
        &context,
        source_message.lineage.source_evidence[0].clone(),
        source_message.input_record.clone(),
        CanonicalPmPayloadV1::Book {
            fact: CanonicalBookFactV1::FullBook {
                asset: asset("123", 0, "UP"),
                bids: vec![PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                    quantity_shares: ExactDecimal("0".to_owned()),
                }],
                asks: vec![],
                last_trade_price_pusd_per_share: None,
            },
        },
    )
    .unwrap();
    let result = state.run(&zero);
    let metadata = state
        .level_evidence("123", BookSide::Bid, d("0.4"))
        .unwrap()
        .to_owned();
    assert_eq!(metadata.direct, zero.lineage.source_evidence[0]);
    assert_eq!(metadata.frontier, metadata.direct);
    assert!(
        state
            .selected_level("123", BookSide::Bid, d("0.4"))
            .unwrap()
            .present
    );
    assert!(
        result
            .steps
            .iter()
            .any(|step| matches!(step.change, PmStepChangeV1::LevelEvidenceOnly { .. }))
    );
    assert!(
        !transitions(&result)
            .iter()
            .any(|change| change.price_pusd_per_share.0 == "0.4")
    );
    let empty = message(
        &context,
        &row(
            "epoch-a",
            3,
            1_400,
            1_500,
            PolymarketOrderbookEvent::Book {
                asset_id: "123".to_owned(),
                bids: vec![],
                asks: vec![],
                last_trade_price: None,
            },
        ),
    );
    state.run(&empty);
    assert_eq!(
        state
            .level_evidence("123", BookSide::Bid, d("0.4"))
            .unwrap()
            .to_owned(),
        metadata
    );
    assert!(
        !state
            .selected_level("123", BookSide::Bid, d("0.4"))
            .unwrap()
            .present
    );
    let mut stale = change(Some("9"));
    stale.price = "0.4".to_owned();
    let stale = message(
        &context,
        &row(
            "epoch-a",
            4,
            1_300,
            1_600,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![stale],
            },
        ),
    );
    assert!(transitions(&state.run(&stale)).is_empty());
    assert_eq!(
        state.quantity("123", BookSide::Bid, d("0.4")),
        Decimal::ZERO
    );
}

#[test]
fn bba_presence_missing_size_tick_priority_and_atomic_error_keep_old_meanings() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let mut with_bba = change(Some("40"));
    with_bba.best_bid = Some("0".to_owned());
    with_bba.best_ask = Some("1".to_owned());
    let value = message(
        &context,
        &row(
            "epoch-a",
            1,
            1_000,
            1_100,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![with_bba],
            },
        ),
    );
    let CanonicalPmPayloadV1::Book {
        fact: CanonicalBookFactV1::AbsoluteLevelChanges { changes },
    } = &value.payload
    else {
        panic!()
    };
    assert_eq!(
        changes[0]
            .best_bid_ask
            .as_ref()
            .unwrap()
            .best_bid_pusd_per_share,
        None
    );
    assert_eq!(
        changes[0]
            .best_bid_ask
            .as_ref()
            .unwrap()
            .best_ask_pusd_per_share,
        None
    );
    let result = state.run(&value);
    assert_eq!(
        result.reports[0].reason_code,
        "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED"
    );
    assert!(state.assets["123"].bba.is_some());
    assert!(!state.assets["123"].book_ready);
    let missing = message(
        &context,
        &row(
            "epoch-a",
            2,
            1_100,
            1_200,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(None)],
            },
        ),
    );
    assert_eq!(
        state.run(&missing).reports[0].reason_code,
        "POLYMARKET_PRICE_CHANGE_WITHOUT_STATE"
    );
    let tick = message(
        &context,
        &row(
            "epoch-a",
            3,
            1_200,
            1_300,
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: "123".to_owned(),
                old_tick_size: Some("0.02".to_owned()),
                new_tick_size: "0.01".to_owned(),
            },
        ),
    );
    assert_eq!(
        state.run(&tick).reports[0].reason_code,
        "POLYMARKET_TICK_CONFIRMATION"
    );
    let first = message(&context, &row("epoch-a", 4, 1_300, 1_400, book("100")));
    state.run(&first);
    let mut unknown = change(None);
    unknown.side = PolymarketSide::Unknown;
    let bad = message(
        &context,
        &row(
            "epoch-a",
            5,
            1_400,
            1_500,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80")), unknown],
            },
        ),
    );
    let before = format!("{state:?}");
    assert!(
        reduce_pm_message(&context, &state, &parse_canonical_pm_message(&bad).unwrap())
            .unwrap_err()
            .to_string()
            .contains("unknown side")
    );
    assert_eq!(format!("{state:?}"), before);
}

#[test]
fn epoch_reset_preserves_initialization_and_only_ready_l2_emits_required() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_000, 1_100, book("100")));
    state.run(&first);
    let make_tick = |epoch, seq| {
        message(
            &context,
            &row(
                epoch,
                seq,
                800,
                1_400 + seq as i64,
                PolymarketOrderbookEvent::TickSizeChange {
                    asset_id: "123".to_owned(),
                    old_tick_size: None,
                    new_tick_size: "0.01".to_owned(),
                },
            ),
        )
    };
    let boundary = make_tick("epoch-b", 1);
    let result = state.run(&boundary);
    assert_eq!(
        result
            .reports
            .iter()
            .filter(|report| report.reason_code == "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED")
            .count(),
        1
    );
    assert!(state.assets["123"].has_initial_snapshot);
    assert!(!state.assets["123"].book_ready);
    let again = make_tick("epoch-c", 1);
    let result = state.run(&again);
    assert!(
        !result
            .reports
            .iter()
            .any(|report| report.reason_code == "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED")
    );
    let empty = message(
        &context,
        &row(
            "epoch-c",
            2,
            700,
            1_800,
            PolymarketOrderbookEvent::Book {
                asset_id: "123".to_owned(),
                bids: vec![],
                asks: vec![],
                last_trade_price: None,
            },
        ),
    );
    let result = state.run(&empty);
    assert!(
        !result
            .steps
            .iter()
            .any(|step| matches!(step.change, PmStepChangeV1::InitialSnapshot { .. }))
    );
    assert!(
        result
            .reports
            .iter()
            .any(|report| report.reason_code == "POLYMARKET_L2_EPOCH_BASELINE_RESTORED")
    );
    assert!(state.assets["123"].book_ready);
}

#[test]
fn canonical_identity_ignores_current_input_position_and_derived_identity_uses_prestate() {
    let context = public_context();
    let row = row(
        "epoch-a",
        2,
        1_200,
        1_400,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("70"))],
        },
    );
    let value = message(&context, &row);
    let moved = canonical_pm_message_from_typed_raw(
        &context,
        &row,
        CanonicalInputRecordRefV1 {
            input_stream_id: "another-shard".to_owned(),
            input_position: 700,
        },
    )
    .unwrap();
    assert_eq!(value.event_id, moved.event_id);
    assert_eq!(value.group_id, moved.group_id);
    assert_eq!(value.lineage, moved.lineage);
    let mut left = ReadState::new(context.clone());
    let mut right = left.clone();
    let first = message(
        &context,
        &self::row("epoch-a", 1, 1_000, 1_100, book("100")),
    );
    left.run(&first);
    let other = message(&context, &self::row("epoch-a", 1, 1_000, 1_100, book("90")));
    right.run(&other);
    let a = left.run(&value);
    let b = right.run(&value);
    assert_ne!(a.steps[0].derived_id, b.steps[0].derived_id);
    assert_eq!(transitions(&a)[0].quantity_before_shares.0, "100");
    assert_eq!(transitions(&b)[0].quantity_before_shares.0, "90");
    assert_eq!(value.event_id, moved.event_id);
}

#[test]
fn retained_real_81856_then_81860_keeps_full_messages_and_has_no_195_reduction() {
    // 只建立这两条消息所需的合法前序，不冒称完整历史前缀重放。
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/public_pm_source_time_reversal.json")).unwrap();
    assert_eq!(
        fixture["source_file_sha256"],
        "7e544a8d2811ef0b597dbc1df73fad490a7e9b2ced76b52c8ed5340e6641f945"
    );
    let identity = &fixture["identity"];
    let context = PmMarketContextV1 {
        market: MarketIdentityV2 {
            condition_id: ConditionId(identity["condition_id"].as_str().unwrap().to_owned()),
            clob_market_id: ClobMarketId(identity["clob_market_id"].as_str().unwrap().to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: identity["market_start_utc"].as_i64().unwrap(),
                end_at_utc_ms: identity["market_end_utc"].as_i64().unwrap(),
            },
        },
        assets: identity["asset_ids"]
            .as_array()
            .unwrap()
            .iter()
            .zip(identity["outcomes"].as_array().unwrap())
            .enumerate()
            .map(|(index, (token, label))| PolymarketAssetContextV1 {
                asset: asset(
                    token.as_str().unwrap(),
                    index as u32,
                    label.as_str().unwrap(),
                ),
                initial_tick_size_pusd_per_share: ExactDecimal(
                    identity["initial_tick"].as_str().unwrap().to_owned(),
                ),
            })
            .collect(),
        identity_source_evidence: vec![SourceEvidenceRefV3 {
            source_kind: CanonicalSourceKindV2::PolymarketMarketMetadata,
            source_message_id: Sha256Hex(
                identity["source_evidence_raw_sha256"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            ),
            source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                sha256: Sha256Hex(
                    identity["source_evidence_raw_sha256"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                ),
            }),
            source_event_at_utc_ms: None,
            received_at_utc_ms: identity["source_evidence_received_at_utc"]
                .as_i64()
                .unwrap(),
            collector_admission: None,
        }],
        canonical_rule_version: "public-pm-v1".to_owned(),
    };
    let rows: Vec<PolymarketOrderbookRow> =
        serde_json::from_value(fixture["rows"].clone()).unwrap();
    for row in &rows {
        row.validate().unwrap();
    }
    let messages = rows
        .iter()
        .map(|row| message(&context, row))
        .collect::<Vec<_>>();
    let mut state = ReadState::new(context.clone());
    let first = state.run(&messages[0]);
    let CanonicalPmPayloadV1::Book {
        fact: CanonicalBookFactV1::FullBook { bids, asks, .. },
    } = &messages[0].payload
    else {
        panic!()
    };
    assert_eq!((bids.len(), asks.len()), (70, 29));
    let borrowed = first
        .steps
        .iter()
        .find_map(|step| match &step.change {
            PmStepChangeV1::InitialSnapshot { bids, .. } => Some(bids.as_ref()),
            _ => None,
        })
        .unwrap();
    assert_eq!(borrowed.as_ptr(), bids.as_ptr());
    let token = context.assets[0].asset.token_id.0.clone();
    let result = state.run(&messages[1]);
    assert_eq!(
        state.quantity(&token, BookSide::Bid, d("0.01")),
        d("4557.61")
    );
    assert!(transitions(&result).is_empty());
    assert_eq!(
        messages[0].time.source_event_at_utc_ms,
        Some(1_786_163_145_605)
    );
    assert_eq!(
        messages[1].time.source_event_at_utc_ms,
        Some(1_786_163_145_433)
    );
    assert_eq!(
        messages[0].time.received_at_utc_ms,
        messages[1].time.received_at_utc_ms
    );
    assert_ne!(messages[0].group_id, messages[1].group_id);
    let CanonicalPmPayloadV1::Book {
        fact: CanonicalBookFactV1::AbsoluteLevelChanges { changes },
    } = &messages[1].payload
    else {
        panic!()
    };
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].quantity_shares.as_ref().unwrap().0, "4362.61");
    assert!(result.steps.iter().any(|step| matches!(&step.change, PmStepChangeV1::BestBidAsk { quote, .. } if quote.asset.token_id.0 == token)));
    assert_eq!(first.touched_level_entries, 0);
    assert_eq!(result.touched_level_entries, 0);
    let metadata_bytes = serde_json::to_vec(&(
        state.levels.iter().collect::<Vec<_>>(),
        state.absences.iter().collect::<Vec<_>>(),
        state.selected.iter().collect::<Vec<_>>(),
    ))
    .unwrap()
    .len();
    println!(
        "real_99_level_sample quantities={} quantity_payload_bytes={} level_evidence={} absences={} selections={} metadata_serialized_bytes={} snapshot_steps={} snapshot_serialized_bytes={} later_steps={} later_serialized_bytes={} overlay_entries={}",
        state.quantities.len(),
        state.quantities.len() * std::mem::size_of::<Decimal>(),
        state.levels.len(),
        state.absences.len(),
        state.selected.len(),
        metadata_bytes,
        first.steps.len(),
        serde_json::to_vec(&first).unwrap().len(),
        result.steps.len(),
        serde_json::to_vec(&result).unwrap().len(),
        result.touched_level_entries
    );
}

#[test]
fn absent_predecessor_role_trade_no_output_and_same_millisecond_messages_stay_distinct() {
    let context = public_context();
    let mut state = ReadState::new(context.clone());
    let first = message(&context, &row("epoch-a", 1, 1_000, 1_500, book("100")));
    state.run(&first);
    let mut new_level = change(Some("20"));
    new_level.price = "0.4".to_owned();
    let value = message(
        &context,
        &row(
            "epoch-a",
            2,
            1_200,
            1_300,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![new_level],
            },
        ),
    );
    let result = state.run(&value);
    let level = result
        .steps
        .iter()
        .find(|step| matches!(step.change, PmStepChangeV1::LevelChange { .. }))
        .unwrap();
    assert!(level.prestate.is_none());
    assert_eq!(
        level.absence.as_ref().unwrap().direct,
        first.lineage.source_evidence[0]
    );
    for (seq, quantity, direction, expected) in [
        (
            3,
            None,
            Some(PolymarketSide::Buy),
            "POLYMARKET_PUBLIC_TRADE_INCOMPLETE",
        ),
        (
            4,
            Some("10"),
            Some(PolymarketSide::Unknown),
            "POLYMARKET_PUBLIC_TRADE_UNKNOWN_SIDE",
        ),
    ] {
        let trade = message(
            &context,
            &row(
                "epoch-a",
                seq,
                1_200,
                1_400,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: "123".to_owned(),
                    price: "0.5".to_owned(),
                    size: quantity.map(str::to_owned),
                    side: direction,
                    fee_rate_bps: Some("1000".to_owned()),
                },
            ),
        );
        let result = state.run(&trade);
        assert_eq!(result.reports[0].reason_code, expected);
        assert!(transitions(&result).is_empty());
        assert_ne!(trade.group_id, value.group_id);
    }
    let second = update(&context, 5, 1_200, "90");
    let result = state.run(&second);
    assert_eq!(transitions(&result)[0].quantity_after_shares.0, "90");
    let later = update(&context, 6, 1_200, "80");
    let result = state.run(&later);
    assert_eq!(transitions(&result)[0].quantity_before_shares.0, "90");
    assert_ne!(later.group_id, second.group_id);
}

#[test]
fn prepared_steps_keep_complete_positions_and_original_prestate_after_later_application() {
    let original_context = public_context();
    let mut prepared_context = PmMarketContextV1 {
        market: original_context.market.clone(),
        assets: original_context.assets.clone(),
        canonical_rule_version: original_context.canonical_rule_version.clone(),
        identity_source_evidence: original_context
            .identity_source_evidence
            .iter()
            .cloned()
            .map(|source| PreparedPmEvidenceV1::prepare(source, None).unwrap())
            .collect(),
    };
    let rows = [
        row("epoch-a", 1, 1_000, 1_500, book("100")),
        row(
            "epoch-a",
            2,
            1_100,
            1_400,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80")), change(Some("100"))],
            },
        ),
        row("epoch-a", 3, 1_200, 1_300, book("150")),
    ];
    let messages = rows
        .iter()
        .map(|row| PreparedPmMessageV1::prepare(message(&original_context, row)).unwrap())
        .collect::<Vec<_>>();
    let mut state = ReadState::new(prepared_context.clone());
    let initial = compute_pm_message_steps_v1(
        &prepared_context,
        &state,
        &messages[0],
        "public-consumer",
        10,
        true,
    )
    .unwrap();
    // 原域字面材料独立于准备/归约/物化算法；保留字段顺序及十进制原规范。
    let original_book_material = br#"{"asset":{"token_id":"123","outcome":{"index":0,"label":"UP"}},"bids":[{"price_pusd_per_share":"0.5","quantity_shares":"100"}],"asks":[{"price_pusd_per_share":"0.6","quantity_shares":"80"}]}"#;
    let mut original_hash = Sha256::new();
    original_hash.update(b"data-pipeline/polymarket-book-digest/v1\0");
    original_hash.update((original_book_material.len() as u64).to_be_bytes());
    original_hash.update(original_book_material);
    let original_digest = format!("{:x}", original_hash.finalize());
    assert_eq!(
        original_digest,
        "07b1fc7567ca36ddbc82f45705cc1d414fb6510432efd8ca03967c3155eea4fb"
    );
    assert!(initial.steps.iter().any(|step| matches!(&step.change, PmStepChangeV1::InitialSnapshot { book_digest_sha256, .. } if book_digest_sha256.0 == original_digest)));
    state.apply(&initial);
    let middle = compute_pm_message_steps_v1(
        &prepared_context,
        &state,
        &messages[1],
        "public-consumer",
        11,
        true,
    )
    .unwrap();
    let changes = middle
        .steps
        .iter()
        .filter_map(|step| match &step.change {
            PmStepChangeV1::LevelChange { transition, .. } => Some((
                transition.quantity_before_shares.0.as_str(),
                transition.quantity_after_shares.0.as_str(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(changes, vec![("100", "80"), ("80", "100")]);
    assert!(
        middle.steps.iter().all(
            |step| step.derived_id.consumer_stream_id == "public-consumer"
                && step.derived_id.canonical_record_position == 11
                && step.derived_id.member_index == step.member_index
                && step.derived_id.step_index == step.step_index
        )
    );
    state.apply(&middle);
    let later = compute_pm_message_steps_v1(
        &prepared_context,
        &state,
        &messages[2],
        "public-consumer",
        12,
        true,
    )
    .unwrap();
    state.apply(&later);
    assert_eq!(
        state.quantities[&key("123", BookSide::Bid, d("0.5"))],
        d("150")
    );
    prepared_context.canonical_rule_version = "later-rule-does-not-rewrite-old-result".to_owned();
    let materialized = materialize_pm_step_identity_v1(middle).unwrap();
    let mut old = ReadState::new(original_context.clone());
    old.run(messages[0].message());
    let expected = old.run(messages[1].message());
    assert_eq!(
        serde_json::to_value(&materialized.reduction).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    for (binding, step) in materialized
        .bindings
        .iter()
        .zip(&materialized.reduction.steps)
    {
        assert_eq!(binding.position.canonical_record_position, 11);
        assert_eq!(binding.derived_id.0.0, step.derived_id.0);
    }
    assert!(
        materialized
            .reduction
            .steps
            .iter()
            .any(|step| step.direct_time.received_at_utc_ms == 1400
                && step.dependency_time.received_at_utc_ms == 1500)
    );
}

#[test]
fn prepared_ingress_supplies_all_nonterminal_pm_kinds_and_shared_source_identity() {
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2;
    let context = public_context();
    let events = [
        book("100"),
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("80"))],
        },
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id: "123".to_owned(),
            best_bid: None,
            best_ask: None,
            spread: None,
        },
        PolymarketOrderbookEvent::TickSizeChange {
            asset_id: "123".to_owned(),
            old_tick_size: Some("0.01".to_owned()),
            new_tick_size: "0.02".to_owned(),
        },
        PolymarketOrderbookEvent::LastTradePrice {
            asset_id: "123".to_owned(),
            price: "0.5".to_owned(),
            size: Some("1".to_owned()),
            side: Some(PolymarketSide::Buy),
            fee_rate_bps: None,
        },
        PolymarketOrderbookEvent::NewMarket {
            clob_market_id: context.market.clob_market_id.0.clone(),
            question: None,
            description: None,
            asset_ids: vec!["123".to_owned(), "456".to_owned()],
            outcomes: vec!["UP".to_owned(), "DOWN".to_owned()],
            event_message: None,
        },
    ];
    for (position, event) in events.into_iter().enumerate() {
        let row = TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch",
            position as u64 + 1,
            1100,
            1200,
            event,
        )));
        let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        let source = source_evidence.clone();
        let input = RecordedIngressV1 {
            input_stream_id: "pm-input".to_owned(),
            input_position: position as u64,
            ingress: DptIngressV1::Message {
                row,
                source_evidence,
            },
            dependency_record_refs: vec![],
        };
        let prepared = prepare_ingress_v1(
            &input,
            Some(&context),
            &Default::default(),
            &[],
            "public-pm-v1",
        )
        .unwrap();
        let PreparedCanonicalInputV1::PolymarketMessage(message) = &prepared.current else {
            panic!("pm")
        };
        assert_eq!(message.message().lineage.source_evidence, vec![source]);
        assert_eq!(
            prepared.prepared_group_id.as_ref(),
            Some(&message.message().group_id)
        );
        assert!(std::sync::Arc::ptr_eq(
            prepared.pm_source_identity.as_ref().unwrap(),
            &message.evidence().shared_source_identity()
        ));
        assert_ne!(
            prepared.prepared_group_id.as_ref().unwrap(),
            &prepared
                .pm_source_identity
                .as_ref()
                .unwrap()
                .causal_group_id
        );
    }
}
