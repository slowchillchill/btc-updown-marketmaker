use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::{
    AssetIdentityV1, BookSide, CanonicalEventRecordV1, EventQualityV2, ExactDecimal,
    ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialL2SnapshotV2,
    LevelExecutionBasis, LevelExecutionTransitionV3, MarketDataQuality, OutcomeIdentityV1,
    PriceLevelV1, QualityBoundaryV2, QualityScopeV1, QueueResultQuality, ResyncQuality, Sha256Hex,
    SourceAttributionQuality, TickSizeChangeV2, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::execution::{
    BaseSubmissionMeasurementV1, InitialAccountStateV1, PlatformMarketV1, PlatformScenarioV1,
    PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1, PostOrdersDirectResponseV1,
    PostOrdersRequestV1, QuoteBudgetSubmissionMeasurementV1, SimBuyOrderKindV1, SimBuyOrderV1,
    SimCallerRoleV1, SimExecutionContractErrorV1, SimExecutionProcessV1, SimExecutionSessionV1,
    SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1, SubmissionEffectDispositionV1,
    SubmissionEvidenceUnknownReasonV1, SubmissionItemOutcomeV1, SubmissionMeasurementV1,
    SubmissionPlatformOrderStatusV1, SubmissionRejectedReasonV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, SubmissionTerminalReasonV1,
};
use crate::latency_profile::load_embedded_proxy_latency_profile;
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MillisecondBucketClosureV1,
};
use crate::matching::{
    SimLiquidityRoleV1, cumulative_segment_quote_units_v1, decimal_from_micro_units_v1,
    maximum_base_for_limit_order_segment_v1, maximum_base_for_quote_budget_v1, micro_units_v1,
    segment_quote_units_v1, stable_fill_id, taker_fee_pusd_v1,
};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{RequestContextV1, RequestRegistrationDispositionV1};
use crate::task21_tests::{
    asset, hidden_bucket, initial_snapshot_event, market, orderbook_evidence, scenario,
    sealed_event, session_spec,
};
use crate::task22_tests::{
    asset as long_asset, context_for_arrival as long_context_for_arrival,
    hidden_bucket as long_hidden_bucket, load_initial_market as load_long_initial_market,
    market as long_market, orderbook_evidence as long_orderbook_evidence,
    scenario as long_scenario, sealed_event as long_sealed_event,
    session_spec as long_session_spec,
};

const REAL_PUBLIC_TRADE_RECORD: &[u8] =
    include_bytes!("../tests/fixtures/task23_real_public_trade.json");
const REAL_LEVEL_DELTA_RECORD: &[u8] =
    include_bytes!("../tests/fixtures/task23_real_level_delta.json");
const REAL_SUPPRESSED_BOOK_WITNESS: &[u8] =
    include_bytes!("../tests/fixtures/task23_real_suppressed_book_witness.json");

fn decimal(value: &str) -> Decimal {
    value.parse().expect("测试十进制")
}

fn fixture_bytes_without_trailing_newline(bytes: &'static [u8]) -> &'static [u8] {
    bytes.strip_suffix(b"\n").unwrap_or(bytes)
}

fn real_execution_event(bytes: &'static [u8], expected_sha256: &str) -> ExecutionMarketEventV3 {
    let bytes = fixture_bytes_without_trailing_newline(bytes);
    assert_eq!(format!("{:x}", Sha256::digest(bytes)), expected_sha256);
    let record: CanonicalEventRecordV1 = serde_json::from_slice(bytes).expect("真实规范记录解码");
    let CanonicalEventRecordV1::ExecutionMarketEvent(event) = record else {
        panic!("真实夹具必须是 ExecutionMarketEventV3");
    };
    encode_execution_market_event_v3(&event).expect("真实事件通过 DPT 唯一校验入口");
    *event
}

fn real_sample_initial_snapshot(
    template: &ExecutionMarketEventV3,
    snapshot_asset: AssetIdentityV1,
    source_cursor: u64,
    best_bid: &str,
    best_ask: &str,
    ask_quantity: &str,
) -> ExecutionMarketEventV3 {
    let mut event = template.clone();
    event.event_id = contracts::CanonicalEventId("0".repeat(64));
    event.time.source_event_at_utc_ms = Some(1_785_715_201_804);
    event.time.received_at_utc_ms = 1_785_715_201_871;
    event.lineage.canonical_rule_version = "task23-real-adjacent-setup-v1".to_owned();
    event.lineage.source_evidence = vec![orderbook_evidence(
        1_785_715_201_804,
        1_785_715_201_871,
        source_cursor,
    )];
    event.quality = EventQualityV2 {
        availability: MarketDataQuality::Fresh,
        resync: ResyncQuality::NotRequired,
    };
    event.payload = ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
        asset: snapshot_asset,
        initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
        source_sequence_epoch_id: "task23-real-adjacent-setup".to_owned(),
        source_cursor,
        book_digest_sha256: Sha256Hex(if source_cursor == 1 {
            "a".repeat(64)
        } else {
            "b".repeat(64)
        }),
        bids: vec![PriceLevelV1 {
            price_pusd_per_share: ExactDecimal(best_bid.to_owned()),
            quantity_shares: ExactDecimal("1".to_owned()),
        }],
        asks: vec![PriceLevelV1 {
            price_pusd_per_share: ExactDecimal(best_ask.to_owned()),
            quantity_shares: ExactDecimal(ask_quantity.to_owned()),
        }],
    });
    event.event_id = derive_execution_market_event_v3_id(&event).expect("真实样本初始化事件身份");
    event
}

fn expected_pending_evidence_sha256(
    previous: Option<&str>,
    platform_order_id: &str,
    matched_base_units: u128,
    source_event_id: &str,
    base_units: u128,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"SIM_PENDING_MAKER_DEPLETION_V1\0");
    hash.update((platform_order_id.len() as u64).to_be_bytes());
    hash.update(platform_order_id.as_bytes());
    hash.update(matched_base_units.to_be_bytes());
    match previous {
        Some(previous) => {
            hash.update([1]);
            hash.update(previous.as_bytes());
        }
        None => hash.update([0]),
    }
    hash.update((source_event_id.len() as u64).to_be_bytes());
    hash.update(source_event_id.as_bytes());
    hash.update(base_units.to_be_bytes());
    format!("{:x}", hash.finalize())
}

fn quote_measurement(response: &PostOrderDirectResponseV1) -> &QuoteBudgetSubmissionMeasurementV1 {
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("response item must be accepted");
    };
    let SubmissionMeasurementV1::QuoteBudget(measurement) = &accepted.measurement else {
        panic!("response item must use quote budget measurement");
    };
    measurement
}

fn base_measurement(response: &PostOrderDirectResponseV1) -> &BaseSubmissionMeasurementV1 {
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("response item must be accepted");
    };
    let SubmissionMeasurementV1::Base(measurement) = &accepted.measurement else {
        panic!("response item must use base measurement");
    };
    measurement
}

fn accepting_capture_bridge(
    captured: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
) -> SubmissionResponseBridgeV1 {
    SubmissionResponseBridgeV1::new(
        move |response| {
            captured
                .lock()
                .expect("capture mutex")
                .push(Arc::clone(&response));
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
    )
}

fn accepting_batch_capture_bridge(
    captured: Arc<Mutex<Vec<Arc<PostOrdersDirectResponseV1>>>>,
) -> SubmissionResponseBridgeV1 {
    SubmissionResponseBridgeV1::new(
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        move |response| {
            captured
                .lock()
                .expect("batch capture mutex")
                .push(Arc::clone(&response));
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
    )
}

async fn ready_session(
    id: &str,
    available_pusd: Decimal,
    capacity: usize,
    bridge: SubmissionResponseBridgeV1,
) -> SimExecutionSessionV1 {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("profile");
    let scenario = scenario();
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("task23 initial account");
    spec.response_outbox_capacity = NonZeroUsize::new(capacity).expect("positive capacity");
    let mut session = SimExecutionProcessV1::new(rules, profile, scenario)
        .create_session(spec, bridge)
        .expect("task23 session");
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("task23 initial bucket");
    session
}

async fn ready_long_session(
    id: &str,
    available_pusd: Decimal,
    bridge: SubmissionResponseBridgeV1,
) -> (
    SimExecutionSessionV1,
    Arc<crate::latency_profile::EmpiricalDelayProfile>,
) {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("profile");
    let scenario = long_scenario();
    let mut spec = long_session_spec(&scenario, SimCallerRoleV1::ModelTrain, id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("task23 long initial account");
    spec.response_outbox_capacity = NonZeroUsize::new(1).expect("positive capacity");
    let mut session = SimExecutionProcessV1::new(rules, Arc::clone(&profile), scenario)
        .create_session(spec, bridge)
        .expect("task23 long session");
    load_long_initial_market(&mut session, "0.00001", "0.6", "5");
    (session, profile)
}

fn request_context(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
) -> RequestContextV1 {
    let spec = session.session_spec();
    RequestContextV1 {
        execution_contract_version: spec.execution_contract_version.clone(),
        session_id: spec.session_id.clone(),
        account_id: spec.account_id.clone(),
        caller_role: spec.caller_role,
        caller_instance_id: spec.caller_instance_id.clone(),
        global_request_id: request_id.to_owned(),
        caller_correlation_id: Some(format!("correlation-{request_id}")),
        caller_sequence,
        request_sent_at_utc_ms,
        model_visible_cursor: session.model_visible_cursor(),
    }
}

fn fak_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
    spend: &str,
    worst_price: &str,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: request_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market().condition_id,
                token_id: asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: decimal(spend),
                    worst_price_pusd_per_share: decimal(worst_price),
                },
            },
        },
    }
}

fn limit_item(item_id: &str, price: &str, size: &str, post_only: bool) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: market().condition_id,
            token_id: asset("1", 0, "Up").token_id,
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: decimal(price),
                size_shares: decimal(size),
                post_only,
            },
        },
    }
}

fn market_item(item_id: &str, fok: bool, spend: &str, worst_price: &str) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: market().condition_id,
            token_id: asset("1", 0, "Up").token_id,
            kind: if fok {
                SimBuyOrderKindV1::FokMarket {
                    spend_pusd: decimal(spend),
                    worst_price_pusd_per_share: decimal(worst_price),
                }
            } else {
                SimBuyOrderKindV1::FakMarket {
                    spend_pusd: decimal(spend),
                    worst_price_pusd_per_share: decimal(worst_price),
                }
            },
        },
    }
}

fn long_gtd_request(
    session: &SimExecutionSessionV1,
    profile: &crate::latency_profile::EmpiricalDelayProfile,
    request_id: &str,
    desired_arrival_at_utc_ms: i64,
    requested_expiry_at_utc_ms: i64,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: long_context_for_arrival(
            session,
            profile,
            request_id,
            1,
            desired_arrival_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: long_market().condition_id,
                token_id: long_asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: decimal("0.00001"),
                    size_shares: decimal("5"),
                    post_only: true,
                    expires_at_utc_ms: requested_expiry_at_utc_ms,
                },
            },
        },
    }
}

fn long_limit_item(item_id: &str, price: &str, size: &str, post_only: bool) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: long_market().condition_id,
            token_id: long_asset("1", 0, "Up").token_id,
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: decimal(price),
                size_shares: decimal(size),
                post_only,
            },
        },
    }
}

fn gtc_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
    price: &str,
    size: &str,
    post_only: bool,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: request_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: limit_item(&format!("item-{request_id}"), price, size, post_only),
    }
}

fn post_orders_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
    items: Vec<PostOrderItemV1>,
) -> PostOrdersRequestV1 {
    PostOrdersRequestV1 {
        context: request_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        items,
    }
}

fn ask_transition(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    price: &str,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    level_transition(
        BookSide::Ask,
        source_at,
        received_at,
        sequence,
        price,
        before,
        after,
    )
}

pub(crate) fn bid_transition(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    price: &str,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    level_transition(
        BookSide::Bid,
        source_at,
        received_at,
        sequence,
        price,
        before,
        after,
    )
}

fn tick_transition(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    old: &str,
    new: &str,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
            asset: asset("1", 0, "Up"),
            old_tick_size_pusd_per_share: Some(ExactDecimal(old.to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal(new.to_owned()),
        }),
        orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
    )
}

fn target_gap_event(source_at: i64, received_at: i64, sequence: u64) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset("1", 0, "Up")),
            previous_quality: MarketDataQuality::Fresh,
            current_quality: MarketDataQuality::Missing,
            reason_code: "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED".to_owned(),
        }),
        orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Missing,
            resync: ResyncQuality::Required,
        },
    )
}

fn target_recovered_event(
    source_at: i64,
    received_at: i64,
    sequence: u64,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset("1", 0, "Up")),
            previous_quality: MarketDataQuality::Missing,
            current_quality: MarketDataQuality::Fresh,
            reason_code: "POLYMARKET_L2_EPOCH_BASELINE_RESTORED".to_owned(),
        }),
        orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}

fn level_transition(
    side: BookSide,
    source_at: i64,
    received_at: i64,
    sequence: u64,
    price: &str,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    let before_decimal = decimal(before);
    let after_decimal = decimal(after);
    let added = (after_decimal - before_decimal).max(Decimal::ZERO);
    let reduced = (before_decimal - after_decimal).max(Decimal::ZERO);
    sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: contracts::Sha256Hex("8".repeat(64)),
            market: market(),
            asset: asset("1", 0, "Up"),
            side,
            price_pusd_per_share: ExactDecimal(price.to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(added.normalize().to_string()),
            net_reduced_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            queue_consumption_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at,
            visible_at_utc_ms: received_at,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}

fn long_bid_transition(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    let before_decimal = decimal(before);
    let after_decimal = decimal(after);
    let added = (after_decimal - before_decimal).max(Decimal::ZERO);
    let reduced = (before_decimal - after_decimal).max(Decimal::ZERO);
    long_sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: contracts::Sha256Hex("7".repeat(64)),
            market: long_market(),
            asset: long_asset("1", 0, "Up"),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.00001".to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(added.normalize().to_string()),
            net_reduced_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            queue_consumption_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at,
            visible_at_utc_ms: received_at,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        long_orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}

pub(crate) fn long_ask_transition(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    price: &str,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    let before_decimal = decimal(before);
    let after_decimal = decimal(after);
    let added = (after_decimal - before_decimal).max(Decimal::ZERO);
    let reduced = (before_decimal - after_decimal).max(Decimal::ZERO);
    long_sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: contracts::Sha256Hex("6".repeat(64)),
            market: long_market(),
            asset: long_asset("1", 0, "Up"),
            side: BookSide::Ask,
            price_pusd_per_share: ExactDecimal(price.to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(added.normalize().to_string()),
            net_reduced_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            queue_consumption_quantity_shares: ExactDecimal(reduced.normalize().to_string()),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at,
            visible_at_utc_ms: received_at,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        long_orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}

fn close_platform_at(session: &mut SimExecutionSessionV1, at: i64, position: u64) {
    session
        .apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            at,
            position,
            position,
            MillisecondBucketClosureV1::NextEventAt(at + 1),
            Vec::new(),
        ))
        .expect("close platform prefix at action time");
}

fn long_hidden_bucket_with_next_event_at(
    at: i64,
    start: u64,
    next: u64,
    next_event_at: i64,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    ClosedMarketEventBucketV1::hidden(
        at,
        start,
        next,
        MillisecondBucketClosureV1::NextEventAt(next_event_at),
        events
            .iter()
            .map(|event| encode_execution_market_event_v3(event).expect("规范事件编码"))
            .collect::<Vec<_>>(),
    )
}

pub(crate) async fn session_with_one_pending_maker_depletion(
    id: &str,
) -> (
    SimExecutionSessionV1,
    String,
    String,
    ClosedMarketEventBucketV1,
    i64,
) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    session_with_one_pending_maker_depletion_with_bridge(
        id,
        Arc::clone(&captured),
        accepting_capture_bridge(captured),
        100,
    )
    .await
}

pub(crate) async fn session_with_one_pending_maker_depletion_with_bridge(
    id: &str,
    captured: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    bridge: SubmissionResponseBridgeV1,
    hidden_receive_offset_ms: i64,
) -> (
    SimExecutionSessionV1,
    String,
    String,
    ClosedMarketEventBucketV1,
    i64,
) {
    let mut session = ready_session(id, decimal("10"), 1, bridge).await;
    let tick = tick_transition(1_101, 1_201, 3, "0.01", "0.00001");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[tick]))
        .expect("dynamic tick bucket");
    let registration = session
        .post_order(gtc_request(&session, id, 1, 1_200, "0.00001", "5", true))
        .expect("post-only GTC registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 3);
    session
        .drive_next_execution_action_through(effective)
        .expect("post-only GTC processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("post-only GTC response delivery");
    let platform_order_id = {
        let responses = captured.lock().expect("capture mutex");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("post-only GTC must be accepted");
        };
        accepted.simulated_platform_order_id.clone()
    };

    let first_addition = bid_transition(
        effective + 1,
        effective + hidden_receive_offset_ms + 1,
        4,
        "0.00001",
        "0",
        "0.000001",
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 1, 4, &[first_addition]))
        .expect("one micro external segment joins behind the account order");
    let first_depletion = bid_transition(
        effective + 2,
        effective + hidden_receive_offset_ms + 2,
        5,
        "0.00001",
        "0.000001",
        "0",
    );
    let first_depletion_event_id = first_depletion.event_id.0.clone();
    let first_depletion_bucket = hidden_bucket(effective + 2, 5, &[first_depletion]);
    session
        .apply_closed_market_event_bucket(first_depletion_bucket.clone())
        .expect("zero-quote maker depletion must remain pending");
    (
        session,
        platform_order_id,
        first_depletion_event_id,
        first_depletion_bucket,
        effective,
    )
}

#[test]
fn initial_account_rejects_negative_and_overprecision_before_session_creation() {
    assert!(InitialAccountStateV1::try_new(decimal("-0.000001")).is_err());
    assert!(InitialAccountStateV1::try_new(decimal("1.0000001")).is_err());

    let account = InitialAccountStateV1::try_new(decimal("10.000000")).expect("合法初始账户");
    assert_eq!(account.available_pusd(), decimal("10"));
    assert_eq!(account.initial_outcome_balance_shares(), Decimal::ZERO);
    assert_eq!(account.initial_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(account.initial_rebate_receivable_pusd(), Decimal::ZERO);
    assert_eq!(account.sha256().0.len(), 64);
}

#[test]
fn endpoint_quantization_does_not_lose_submicro_transitions() {
    let first_before = micro_units_v1(decimal("0.0000006")).expect("量化旧端点");
    let first_after = micro_units_v1(decimal("0.0000011")).expect("量化新端点");
    assert_eq!(first_after - first_before, 1);
    assert_eq!(micro_units_v1(first_after_decimal_delta()).unwrap(), 0);

    let second_after = micro_units_v1(decimal("0.0000006")).expect("量化第二端点");
    let third_after = micro_units_v1(decimal("0.0000001")).expect("量化第三端点");
    assert_eq!(first_after - second_after, 1);
    assert_eq!(second_after - third_after, 0);
    assert_eq!(third_after, 0);
}

fn first_after_decimal_delta() -> Decimal {
    decimal("0.0000011") - decimal("0.0000006")
}

#[test]
fn synthetic_segment_uses_cumulative_floor_and_maximal_quote_inverse() {
    assert_eq!(segment_quote_units_v1(7, decimal("0.4285715")).unwrap(), 3);
    assert_eq!(cumulative_segment_quote_units_v1(7, 3, 0).unwrap(), 0);

    let first = maximum_base_for_quote_budget_v1(7, 3, 0, 1)
        .expect("首笔 quote 预算反解")
        .expect("首笔必须可成交");
    assert_eq!(first, (4, 1));

    let second = maximum_base_for_quote_budget_v1(7, 3, 4, 2)
        .expect("第二笔 quote 预算反解")
        .expect("第二笔必须可成交");
    assert_eq!(second, (3, 2));
    assert_eq!(cumulative_segment_quote_units_v1(7, 3, 7).unwrap(), 3);
}

#[test]
fn synthetic_segment_wide_integer_math_and_zero_quote_are_safe() {
    let base = ((1_u128 << 96) - 1)
        .checked_mul(1_000_000)
        .expect("合法 Decimal 微单位适配 u128");
    let quote = base / 2;
    let fill = maximum_base_for_quote_budget_v1(base, quote, base - 4, 1)
        .expect("宽整数反解")
        .expect("宽整数边界必须可成交");
    assert_eq!(fill, (3, 1));

    assert_eq!(
        maximum_base_for_quote_budget_v1(7, 0, 0, 1).expect("零 quote 不得除零"),
        None
    );
    assert!(maximum_base_for_quote_budget_v1(0, 1, 0, 1).is_err());
    assert!(maximum_base_for_quote_budget_v1(7, 3, 8, 1).is_err());
    assert!(decimal_from_micro_units_v1(u128::MAX).is_err());
    let decimal_max_micro = u128::try_from(Decimal::MAX.mantissa())
        .expect("Decimal::MAX 为正")
        .checked_mul(1_000_000)
        .expect("Decimal::MAX 微单位适配 u128");
    assert_eq!(
        decimal_from_micro_units_v1(decimal_max_micro).expect("尾零应安全降标"),
        Decimal::MAX
    );
    assert!(decimal_from_micro_units_v1(1_u128 << 96).is_err());
}

#[test]
fn limit_quote_envelope_handles_both_slopes_and_wide_integer_boundaries() {
    let wide = 10_u128.pow(30);
    let cases = [
        (
            (100_000, 33_333, 0, 100_000, 5_010_000, 1_669_983, 0, 0),
            Some((99_999, 33_332)),
        ),
        (
            (4_000_000, 3_000_000, 2, 2, 5_000_000, 4_000_000, 4, 3),
            Some((1, 1)),
        ),
        (
            (
                4 * wide,
                3 * wide,
                2 * wide + 2,
                2,
                8 * wide,
                6 * wide,
                3 * wide + 3,
                9 * wide / 4 + 2,
            ),
            Some((1, 1)),
        ),
    ];
    for (input, expected) in cases {
        let (
            external_base,
            external_quote,
            consumed,
            maximum,
            order_base,
            order_quote,
            matched,
            used,
        ) = input;
        assert_eq!(
            maximum_base_for_limit_order_segment_v1(
                external_base,
                external_quote,
                consumed,
                maximum,
                order_base,
                order_quote,
                matched,
                used,
            )
            .expect("合法双 quote 包络"),
            expected
        );
    }
}

#[test]
fn taker_fee_sums_exact_price_levels_before_rounding_once() {
    let fee = taker_fee_pusd_v1(
        &[(238, decimal("0.4")), (238, decimal("0.6"))],
        decimal("0.07"),
        1,
    )
    .expect("手续费计算");
    assert_eq!(fee, decimal("0.00001"));

    let below_half =
        taker_fee_pusd_v1(&[(200, decimal("0.5"))], decimal("0.07"), 1).expect("小额手续费计算");
    assert_eq!(below_half, Decimal::ZERO);

    let midpoint =
        taker_fee_pusd_v1(&[(1, decimal("0.5"))], decimal("20"), 1).expect("精确中点手续费计算");
    assert_eq!(midpoint, decimal("0.00001"));

    let huge_exponent = taker_fee_pusd_v1(&[(u128::MAX, decimal("0.5"))], decimal("1"), u32::MAX)
        .expect("巨大指数应以严格上界快速归零");
    assert_eq!(huge_exponent, Decimal::ZERO);

    let high_magnitude =
        taker_fee_pusd_v1(&[(10_u128.pow(34), decimal("0.5"))], decimal("0.07"), 48)
            .expect("高数量与高指数仍须精确舍入");
    assert_eq!(high_magnitude, decimal("0.00884"));
}

#[tokio::test(flavor = "current_thread")]
async fn fak_uses_quote_budget_and_keeps_sdk_base_as_audit_only() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-fak",
        decimal("10"),
        2,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let better_ask = ask_transition(1_101, 1_201, 3, "0.4", "0", "10");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[better_ask]))
        .expect("better ask bucket");
    let request = fak_request(&session, "fak-improvement", 1, 1_200, "3", "0.6");
    let registration = session.post_order(request).expect("FAK registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 3);
    assert_eq!(
        session
            .drive_next_execution_action_through(effective)
            .expect("FAK processing"),
        SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true
        }
    );
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    assert_eq!(
        session
            .drive_direct_response_delivery_through(received_at)
            .expect("FAK delivery"),
        SubmissionDeliveryDispositionV1::Delivered
    );

    let responses = captured.lock().expect("capture mutex");
    assert_eq!(responses.len(), 1);
    let measurement = quote_measurement(&responses[0]);
    assert_eq!(measurement.accepted_quote_budget_pusd, decimal("3"));
    assert_eq!(measurement.used_quote_amount_pusd, decimal("3"));
    assert_eq!(
        measurement.released_or_unspent_quote_amount_pusd,
        Decimal::ZERO
    );
    assert_eq!(
        measurement.actual_matched_base_quantity_shares,
        decimal("7.500002")
    );
    assert_eq!(measurement.sdk_taker_amount_audit_shares, decimal("5"));
    assert_eq!(
        measurement.immediate_taker_fee_encumbered_pusd,
        decimal("0.126")
    );
    assert_eq!(session.available_pusd(), decimal("6.874"));
    assert_eq!(session.principal_encumbered_pusd(), decimal("3"));
    assert_eq!(
        session.immediate_taker_fee_encumbered_pusd(),
        decimal("0.126")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn partial_fak_is_matched_and_releases_only_the_unspent_quote() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-partial-fak",
        decimal("10"),
        1,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let remove_initial_ask = ask_transition(1_101, 1_201, 3, "0.6", "5", "0");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[remove_initial_ask]))
        .expect("remove initial ask");
    let two_pusd_ask = ask_transition(1_102, 1_202, 4, "0.4", "0", "5");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_102, 4, &[two_pusd_ask]))
        .expect("add exact two-pUSD ask");
    let registration = session
        .post_order(fak_request(&session, "partial-fak", 1, 1_200, "3", "0.6"))
        .expect("partial FAK registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 4);
    session
        .drive_next_execution_action_through(effective)
        .expect("partial FAK processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("partial FAK delivery");

    let responses = captured.lock().expect("capture mutex");
    let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
        panic!("positive partial FAK must be accepted");
    };
    assert_eq!(
        accepted.platform_order_status,
        SubmissionPlatformOrderStatusV1::Matched
    );
    assert_eq!(
        accepted.terminal_reason,
        Some(SubmissionTerminalReasonV1::FakCompleted)
    );
    let SubmissionMeasurementV1::QuoteBudget(measurement) = &accepted.measurement else {
        panic!("FAK must use quote-budget measurement");
    };
    assert_eq!(measurement.used_quote_amount_pusd, decimal("2"));
    assert_eq!(
        measurement.released_or_unspent_quote_amount_pusd,
        decimal("1")
    );
    assert_eq!(
        measurement.actual_matched_base_quantity_shares,
        decimal("5")
    );
    assert_eq!(session.simulated_order_count(), 1);
    assert_eq!(session.available_pusd(), decimal("7.916"));
    assert_eq!(session.principal_encumbered_pusd(), decimal("2"));
    assert_eq!(
        session.immediate_taker_fee_encumbered_pusd(),
        decimal("0.084")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn exactly_filled_fok_is_accepted_and_matched() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-full-fok",
        decimal("10"),
        1,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let request = PostOrderRequestV1 {
        context: request_context(&session, "full-fok", 1, 1_200),
        idempotency_key: "idem-full-fok".to_owned(),
        item: market_item("full-fok-item", true, "3", "0.6"),
    };
    let registration = session.post_order(request).expect("full FOK registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("full FOK processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("full FOK delivery");

    let responses = captured.lock().expect("capture mutex");
    let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
        panic!("exactly filled FOK must be accepted");
    };
    assert_eq!(
        accepted.platform_order_status,
        SubmissionPlatformOrderStatusV1::Matched
    );
    assert_eq!(accepted.terminal_reason, None);
    let SubmissionMeasurementV1::QuoteBudget(measurement) = &accepted.measurement else {
        panic!("FOK must use quote-budget measurement");
    };
    assert_eq!(measurement.used_quote_amount_pusd, decimal("3"));
    assert_eq!(
        measurement.released_or_unspent_quote_amount_pusd,
        Decimal::ZERO
    );
    assert_eq!(
        measurement.actual_matched_base_quantity_shares,
        decimal("5")
    );
    assert_eq!(session.simulated_order_count(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn outbox_capacity_and_physical_retry_preserve_the_registration_receipt() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-capacity",
        decimal("10"),
        1,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let first = fak_request(&session, "capacity-first", 1, 1_200, "3", "0.6");
    let first_registration = session
        .post_order(first.clone())
        .expect("first registration");
    let second = fak_request(&session, "capacity-second", 2, 1_500, "3", "0.6");
    assert!(matches!(
        session.post_order(second.clone()),
        Err(crate::session::SessionError::ResponseOutboxFull)
    ));
    let retry_before = session
        .post_order(first.clone())
        .expect("retry before processing");
    assert_eq!(
        retry_before.receipt.disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        retry_before.receipt.transit,
        first_registration.receipt.transit
    );

    let effective = first_registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("first processing");
    let retry_after_response = session
        .post_order(first.clone())
        .expect("retry after response");
    assert_eq!(
        retry_after_response.receipt.transit, first_registration.receipt.transit,
        "响应完成不得改写物理重投登记回执"
    );
    let received_at = effective
        + i64::try_from(first_registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("first delivery");
    let retry_after_confirmation = session.post_order(first).expect("retry after confirmation");
    assert_eq!(
        retry_after_confirmation.receipt.transit,
        first_registration.receipt.transit
    );
    session
        .post_order(second)
        .expect("confirmed response must release exactly one outbox slot");
}

#[tokio::test(flavor = "current_thread")]
async fn gtc_price_improvement_and_later_maker_fill_keep_one_quote_conservation_path() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-gtc-maker",
        decimal("10"),
        2,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let better_ask = ask_transition(1_101, 1_201, 3, "0.4", "0", "2");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[better_ask]))
        .expect("better ask bucket");
    let request = gtc_request(&session, "gtc-maker", 1, 1_200, "0.6", "10", false);
    let registration = session.post_order(request).expect("GTC registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 3);
    session
        .drive_next_execution_action_through(effective)
        .expect("GTC processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("GTC delivery");

    let responses = captured.lock().expect("capture mutex");
    let response = responses.first().expect("one GTC response");
    let measurement = base_measurement(response);
    assert_eq!(measurement.accepted_base_quantity_shares, decimal("10"));
    assert_eq!(measurement.matched_base_quantity_shares, decimal("7"));
    assert_eq!(measurement.open_base_quantity_shares, decimal("3"));
    assert_eq!(measurement.accepted_quote_amount_pusd, decimal("6"));
    assert_eq!(measurement.matched_quote_amount_pusd, decimal("3.8"));
    assert_eq!(
        measurement.open_encumbered_quote_amount_pusd,
        decimal("1.8")
    );
    assert_eq!(
        measurement.released_price_improvement_quote_amount_pusd,
        decimal("0.4")
    );
    assert_eq!(
        measurement.immediate_taker_fee_encumbered_pusd,
        decimal("0.1176")
    );
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("GTC must be accepted");
    };
    let platform_order_id = accepted.simulated_platform_order_id.clone();
    drop(responses);

    let queued_behind_account = bid_transition(effective + 1, effective + 101, 4, "0.6", "0", "1");
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 1, 4, &[queued_behind_account]))
        .expect("external bid queued behind account order");
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.6"),
        ),
        decimal("3"),
        "后方外部段不得越过账户订单"
    );
    let crosses_account = bid_transition(effective + 2, effective + 102, 5, "0.6", "1", "0");
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 2, 5, &[crosses_account]))
        .expect("DPT reduction crosses account order");
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.6"),
        ),
        decimal("2")
    );
    let order = session
        .simulated_order(&platform_order_id)
        .expect("GTC order state");
    assert_eq!(order.matched_base_units, 8_000_000);
    assert_eq!(order.open_base_units, 2_000_000);
    assert_eq!(order.matched_quote_units, 4_400_000);
    assert_eq!(order.open_quote_units, 1_200_000);
    assert_eq!(order.released_quote_units, 400_000);
    assert_eq!(
        order.matched_quote_units + order.open_quote_units + order.released_quote_units,
        order.accepted_quote_units
    );
    assert_eq!(session.simulated_fill_count(&platform_order_id), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn zero_quote_maker_depletion_waits_and_then_forms_one_positive_quote_fill() {
    let (mut session, platform_order_id, first_depletion_event_id, pending_bucket, effective) =
        session_with_one_pending_maker_depletion("task23-pending-maker-depletion").await;
    let pending = session
        .simulated_order(&platform_order_id)
        .expect("pending maker order state");
    assert_eq!(pending.matched_base_units, 0);
    assert_eq!(pending.pending_maker_depletion_base_units, 1);
    assert_eq!(pending.open_base_units, 4_999_999);
    assert_eq!(pending.matched_quote_units, 0);
    assert_eq!(pending.open_quote_units, 50);
    let pending_evidence = pending
        .pending_maker_depletion_evidence
        .as_ref()
        .expect("zero-quote depletion must retain bounded evidence");
    assert_eq!(pending_evidence.event_count, 1);
    assert_eq!(
        pending_evidence.first_source_event_id,
        first_depletion_event_id
    );
    assert_eq!(
        pending_evidence.last_source_event_id,
        first_depletion_event_id
    );
    let first_evidence_sha256 =
        expected_pending_evidence_sha256(None, &platform_order_id, 0, &first_depletion_event_id, 1);
    assert_eq!(pending_evidence.sha256.0, first_evidence_sha256);
    let pending_before_retry = pending.clone();
    assert_eq!(session.simulated_fill_count(&platform_order_id), 0);
    assert_eq!(session.principal_encumbered_pusd(), decimal("0.00005"));
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.00001"),
        ),
        decimal("4.999999")
    );
    assert_eq!(
        session
            .apply_closed_market_event_bucket(pending_bucket)
            .expect("pending bucket physical retry"),
        MarketBucketApplyDispositionV1::PhysicalRetry
    );
    assert_eq!(
        session.simulated_order(&platform_order_id),
        Some(&pending_before_retry)
    );
    assert_eq!(session.simulated_fill_count(&platform_order_id), 0);

    let second_addition = bid_transition(
        effective + 3,
        effective + 103,
        6,
        "0.00001",
        "0",
        "0.099999",
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 3, 6, &[second_addition]))
        .expect("additional external segment joins behind the account order");
    let second_depletion = bid_transition(
        effective + 4,
        effective + 104,
        7,
        "0.00001",
        "0.099999",
        "0",
    );
    let second_depletion_event_id = second_depletion.event_id.0.clone();
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 4, 7, &[second_depletion]))
        .expect("cumulative positive-quote maker depletion must form one fill");
    let filled = session
        .simulated_order(&platform_order_id)
        .expect("maker order after pending amount becomes executable");
    assert_eq!(filled.matched_base_units, 100_000);
    assert_eq!(filled.pending_maker_depletion_base_units, 0);
    assert_eq!(filled.open_base_units, 4_900_000);
    assert_eq!(filled.matched_quote_units, 1);
    assert_eq!(filled.open_quote_units, 49);
    assert!(filled.pending_maker_depletion_evidence.is_none());
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);
    let expected_evidence_sha256 = expected_pending_evidence_sha256(
        Some(&first_evidence_sha256),
        &platform_order_id,
        0,
        &second_depletion_event_id,
        99_999,
    );
    let inferred_fill = session
        .simulated_fill(&platform_order_id)
        .expect("one inferred maker fill");
    assert_eq!(
        inferred_fill.liquidity_role,
        SimLiquidityRoleV1::MakerInferred
    );
    assert_eq!(inferred_fill.price_pusd_per_share, decimal("0.00001"));
    assert_eq!(inferred_fill.base_units, 100_000);
    assert_eq!(inferred_fill.quote_units, 1);
    assert_eq!(inferred_fill.source_identity, expected_evidence_sha256);
    let fill_evidence = inferred_fill
        .maker_depletion_evidence
        .as_ref()
        .expect("inferred maker fill must retain bounded depletion evidence");
    assert_eq!(fill_evidence.sha256.0, expected_evidence_sha256);
    assert_eq!(fill_evidence.event_count, 2);
    assert_eq!(
        fill_evidence.first_source_event_id,
        first_depletion_event_id
    );
    assert_eq!(
        fill_evidence.last_source_event_id,
        second_depletion_event_id
    );
    assert_ne!(inferred_fill.source_identity, first_depletion_event_id);
    assert_ne!(inferred_fill.source_identity, second_depletion_event_id);
    assert_eq!(
        inferred_fill.trade_id,
        stable_fill_id(
            "MAKER_INFERRED",
            &platform_order_id,
            &expected_evidence_sha256,
            0,
        )
    );
    assert_eq!(session.principal_encumbered_pusd(), decimal("0.00005"));
    assert_eq!(
        filled.matched_base_units
            + filled.pending_maker_depletion_base_units
            + filled.open_base_units,
        filled.accepted_base_units.expect("GTC accepted base")
    );
    assert_eq!(
        filled.matched_quote_units + filled.open_quote_units + filled.released_quote_units,
        filled.accepted_quote_units
    );
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.00001"),
        ),
        decimal("4.9")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn end_at_preserves_pending_maker_depletion_and_blocks_later_fill() {
    let (mut session, platform_order_id, _, _, _) =
        session_with_one_pending_maker_depletion("task23-pending-through-end").await;
    let pending_before_end = session
        .simulated_order(&platform_order_id)
        .expect("pending maker order before end_at")
        .clone();
    let available_before_end = session.available_pusd();
    let principal_before_end = session.principal_encumbered_pusd();
    let end_at = market().window.end_at_utc_ms;

    let post_end_addition = bid_transition(end_at, end_at + 100, 6, "0.00001", "0", "0.099999");
    session
        .apply_closed_market_event_bucket(hidden_bucket(end_at, 6, &[post_end_addition]))
        .expect("end_at must close account matching before applying the event");
    let post_end_depletion =
        bid_transition(end_at + 1, end_at + 101, 7, "0.00001", "0.099999", "0");
    session
        .apply_closed_market_event_bucket(hidden_bucket(end_at + 1, 7, &[post_end_depletion]))
        .expect("post-end external depletion must not cross the account order");

    assert_eq!(
        session.simulated_order(&platform_order_id),
        Some(&pending_before_end)
    );
    assert_eq!(session.simulated_fill_count(&platform_order_id), 0);
    assert_eq!(session.available_pusd(), available_before_end);
    assert_eq!(session.principal_encumbered_pusd(), principal_before_end);
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.00001"),
        ),
        decimal("4.999999")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn gtd_deadline_without_market_event_terminalizes_pending_and_releases_once() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (mut session, profile) = ready_long_session(
        "task23-gtd-pending-expiry",
        decimal("10"),
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let registration = session
        .post_order(long_gtd_request(
            &session,
            &profile,
            "gtd-pending-expiry",
            150_000,
            391_999,
        ))
        .expect("GTD registration");
    assert_eq!(registration.item.effective_processing_at_utc_ms, 150_000);
    session
        .apply_closed_market_event_bucket(long_hidden_bucket(150_000, 2, 2, &[]))
        .expect("close platform at GTD processing time");
    session
        .drive_next_execution_action_through(150_000)
        .expect("create GTD order");
    let response_received_at = 150_000
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(response_received_at)
        .expect("deliver GTD submission response");
    let platform_order_id = {
        let responses = captured.lock().expect("capture mutex");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("GTD must be accepted");
        };
        accepted.simulated_platform_order_id.clone()
    };
    assert_eq!(session.next_order_deadline_at_utc_ms(), Some(391_000));

    let addition = long_bid_transition(150_001, 160_001, 3, "0", "0.000001");
    session
        .apply_closed_market_event_bucket(long_hidden_bucket(150_001, 2, 3, &[addition]))
        .expect("one micro joins behind GTD");
    let depletion = long_bid_transition(150_002, 160_002, 4, "0.000001", "0");
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            150_002,
            3,
            4,
            391_001,
            &[depletion],
        ))
        .expect("one micro becomes pending before GTD expiry");
    let pending = session
        .simulated_order(&platform_order_id)
        .expect("pending GTD")
        .clone();
    assert_eq!(pending.pending_maker_depletion_base_units, 1);
    assert_eq!(pending.open_base_units, 4_999_999);
    assert_eq!(pending.open_quote_units, 50);
    assert_eq!(session.principal_encumbered_pusd(), decimal("0.00005"));

    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            390_999,
            4,
            4,
            391_001,
            &[],
        ))
        .expect("empty closed prefix before deadline");
    assert_eq!(session.simulated_order(&platform_order_id), Some(&pending));
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            391_000,
            4,
            4,
            391_001,
            &[],
        ))
        .expect("empty closed prefix at deadline");
    let expired = session
        .simulated_order(&platform_order_id)
        .expect("expired GTD")
        .clone();
    assert_eq!(expired.matched_base_units, 0);
    assert_eq!(expired.pending_maker_depletion_base_units, 0);
    assert_eq!(expired.open_base_units, 0);
    assert_eq!(expired.expired_base_units, 5_000_000);
    assert_eq!(expired.matched_quote_units, 0);
    assert_eq!(expired.open_quote_units, 0);
    assert_eq!(expired.released_quote_units, 0);
    assert_eq!(expired.expired_released_quote_units, 50);
    assert_eq!(expired.expired_at_utc_ms, Some(391_000));
    assert!(expired.gtd_expiration_identity.is_some());
    assert_eq!(
        expired.terminalized_pending_maker_depletion_evidence,
        pending.pending_maker_depletion_evidence
    );
    assert_eq!(session.simulated_fill_count(&platform_order_id), 0);
    assert_eq!(session.available_pusd(), decimal("10"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        session.account_overlay_quantity(
            &long_market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.00001"),
        ),
        Decimal::ZERO
    );
    assert_eq!(session.next_order_deadline_at_utc_ms(), None);
    assert_eq!(
        session.drive_next_execution_action_through(390_999),
        Err(crate::execution::SubmissionProcessingErrorV1::Matching(
            crate::matching::MatchingStateErrorV1::LogicalTimeRegression
        ))
    );

    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            391_000,
            4,
            4,
            391_001,
            &[],
        ))
        .expect("physical retry of the expiry prefix");
    assert_eq!(session.simulated_order(&platform_order_id), Some(&expired));
    assert_eq!(session.available_pusd(), decimal("10"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn gtd_after_end_releases_only_open_principal_and_preserves_taker_fee() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (mut session, profile) = ready_long_session(
        "task23-gtd-after-end-with-fee",
        decimal("10"),
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let request = PostOrderRequestV1 {
        context: long_context_for_arrival(&session, &profile, "gtd-after-end-with-fee", 1, 150_000),
        idempotency_key: "idem-gtd-after-end-with-fee".to_owned(),
        item: PostOrderItemV1 {
            item_id: "item-gtd-after-end-with-fee".to_owned(),
            order: SimBuyOrderV1 {
                condition_id: long_market().condition_id,
                token_id: long_asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: decimal("0.6"),
                    size_shares: decimal("10"),
                    post_only: false,
                    expires_at_utc_ms: 501_999,
                },
            },
        },
    };
    let registration = session.post_order(request).expect("GTD fee registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("partially match GTD before end");
    let response_received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(response_received_at)
        .expect("deliver partial GTD response");
    let (platform_order_id, open_quote) = {
        let responses = captured.lock().expect("capture mutex");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("partially matched GTD must be accepted");
        };
        let SubmissionMeasurementV1::Base(measurement) = &accepted.measurement else {
            panic!("GTD must use base measurement");
        };
        assert_eq!(measurement.matched_base_quantity_shares, decimal("5"));
        assert_eq!(measurement.open_base_quantity_shares, decimal("5"));
        assert_eq!(measurement.matched_quote_amount_pusd, decimal("3"));
        assert_eq!(measurement.open_encumbered_quote_amount_pusd, decimal("3"));
        assert!(measurement.immediate_taker_fee_encumbered_pusd > Decimal::ZERO);
        (
            accepted.simulated_platform_order_id.clone(),
            measurement.open_encumbered_quote_amount_pusd,
        )
    };
    let available_before_expiry = session.available_pusd();
    let principal_before_expiry = session.principal_encumbered_pusd();
    let fee_before_expiry = session.immediate_taker_fee_encumbered_pusd();
    assert!(fee_before_expiry > Decimal::ZERO);

    let end_at = long_market().window.end_at_utc_ms;
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            end_at,
            2,
            2,
            501_000,
            &[],
        ))
        .expect("end_at closes matching without terminalizing GTD");
    let before_expiry = session
        .simulated_order(&platform_order_id)
        .expect("GTD remains active after end_at")
        .clone();
    assert_eq!(before_expiry.open_base_units, 5_000_000);
    assert_eq!(before_expiry.expired_base_units, 0);

    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            501_000,
            2,
            2,
            501_001,
            &[],
        ))
        .expect("trusted empty prefix triggers GTD after end_at");
    let expired = session
        .simulated_order(&platform_order_id)
        .expect("GTD terminalized at encoded expiry");
    assert_eq!(expired.matched_base_units, 5_000_000);
    assert_eq!(expired.expired_base_units, 5_000_000);
    assert_eq!(expired.open_base_units, 0);
    assert_eq!(expired.matched_quote_units, 3_000_000);
    assert_eq!(expired.expired_released_quote_units, 3_000_000);
    assert_eq!(expired.open_quote_units, 0);
    assert_eq!(expired.expired_at_utc_ms, Some(501_000));
    assert_eq!(
        session.available_pusd(),
        available_before_expiry + open_quote
    );
    assert_eq!(
        session.principal_encumbered_pusd(),
        principal_before_expiry - open_quote
    );
    assert_eq!(
        session.immediate_taker_fee_encumbered_pusd(),
        fee_before_expiry
    );
}

#[tokio::test(flavor = "current_thread")]
async fn gtd_expiry_equality_precedes_same_millisecond_maker_depletion() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (mut session, profile) = ready_long_session(
        "task23-gtd-expiry-tie",
        decimal("10"),
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let registration = session
        .post_order(long_gtd_request(
            &session,
            &profile,
            "gtd-expiry-tie",
            150_000,
            391_999,
        ))
        .expect("GTD registration");
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            150_000,
            2,
            2,
            390_999,
            &[],
        ))
        .expect("close platform at GTD processing time");
    session
        .drive_next_execution_action_through(150_000)
        .expect("create GTD order");
    let response_received_at = 150_000
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(response_received_at)
        .expect("deliver GTD submission response");
    let platform_order_id = {
        let responses = captured.lock().expect("capture mutex");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("GTD must be accepted");
        };
        accepted.simulated_platform_order_id.clone()
    };

    let before_expiry_add = long_bid_transition(390_999, 400_999, 3, "0", "0.1");
    let before_expiry_reduce = long_bid_transition(390_999, 400_999, 4, "0.1", "0");
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            390_999,
            2,
            4,
            391_000,
            &[before_expiry_add, before_expiry_reduce],
        ))
        .expect("expiry minus one maker depletion");
    let before_expiry = session
        .simulated_order(&platform_order_id)
        .expect("live GTD before expiry");
    assert_eq!(before_expiry.matched_base_units, 100_000);
    assert_eq!(before_expiry.open_base_units, 4_900_000);
    assert_eq!(before_expiry.matched_quote_units, 1);
    assert_eq!(before_expiry.open_quote_units, 49);
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);
    assert_eq!(session.available_pusd(), decimal("9.99995"));
    assert_eq!(session.principal_encumbered_pusd(), decimal("0.00005"));

    let at_expiry_add = long_bid_transition(391_000, 401_000, 5, "0", "0.1");
    let at_expiry_reduce = long_bid_transition(391_000, 401_000, 6, "0.1", "0");
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            391_000,
            4,
            6,
            391_001,
            &[at_expiry_add, at_expiry_reduce],
        ))
        .expect("expiry equality closes GTD before market events");
    let expired = session
        .simulated_order(&platform_order_id)
        .expect("expired GTD");
    assert_eq!(expired.matched_base_units, 100_000);
    assert_eq!(expired.pending_maker_depletion_base_units, 0);
    assert_eq!(expired.open_base_units, 0);
    assert_eq!(expired.expired_base_units, 4_900_000);
    assert_eq!(expired.matched_quote_units, 1);
    assert_eq!(expired.open_quote_units, 0);
    assert_eq!(expired.expired_released_quote_units, 49);
    assert_eq!(expired.expired_at_utc_ms, Some(391_000));
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);
    assert_eq!(session.available_pusd(), decimal("9.999999"));
    assert_eq!(session.principal_encumbered_pusd(), decimal("0.000001"));
    assert_eq!(
        session.account_overlay_quantity(
            &long_market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.00001"),
        ),
        Decimal::ZERO
    );
}

#[tokio::test(flavor = "current_thread")]
async fn limit_base_matching_respects_the_original_order_quote_envelope() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-limit-envelope",
        decimal("10"),
        1,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let tick = tick_transition(1_101, 1_201, 3, "0.01", "0.00001");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[tick]))
        .expect("dynamic tick bucket");
    let ask = ask_transition(1_102, 1_202, 4, "0.33333", "0", "0.1");
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_102, 4, &[ask]))
        .expect("rounding-sensitive ask bucket");
    let request = gtc_request(
        &session,
        "limit-envelope",
        1,
        1_200,
        "0.33333",
        "5.01",
        false,
    );
    let registration = session.post_order(request).expect("limit registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 4);
    session
        .drive_next_execution_action_through(effective)
        .expect("rounding-sensitive limit processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("limit response delivery");
    let responses = captured.lock().expect("capture mutex");
    let measurement = base_measurement(&responses[0]);
    assert_eq!(measurement.accepted_base_quantity_shares, decimal("5.01"));
    assert_eq!(
        measurement.matched_base_quantity_shares,
        decimal("0.099999")
    );
    assert_eq!(measurement.matched_quote_amount_pusd, decimal("0.033332"));
    assert_eq!(measurement.open_base_quantity_shares, decimal("4.910001"));
    assert_eq!(
        measurement.matched_quote_amount_pusd
            + measurement.open_encumbered_quote_amount_pusd
            + measurement.released_price_improvement_quote_amount_pusd,
        measurement.accepted_quote_amount_pusd
    );
}

#[tokio::test(flavor = "current_thread")]
async fn partial_dpt_reduction_retains_taker_depletion_debt_until_fully_observed() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-taker-debt",
        decimal("10"),
        1,
        accepting_capture_bridge(captured),
    )
    .await;
    let registration = session
        .post_order(fak_request(&session, "taker-debt", 1, 1_200, "3", "0.6"))
        .expect("FAK registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("FAK consumes the full external ask");

    let first = ask_transition(effective + 1, effective + 101, 3, "0.6", "5", "2");
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 1, 3, &[first]))
        .expect("partial DPT echo consumes only part of taker debt");
    let second = ask_transition(effective + 2, effective + 102, 4, "0.6", "2", "0");
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective + 2, 4, &[second]))
        .expect("remaining DPT echo consumes the retained debt");
    assert_eq!(session.platform_event_index_position(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn funding_gate_rolls_back_matching_and_exact_principal_plus_fee_passes() {
    let rejected_batches = Arc::new(Mutex::new(Vec::new()));
    let mut rejected = ready_session(
        "task23-funding-reject",
        decimal("3.125999"),
        1,
        accepting_batch_capture_bridge(Arc::clone(&rejected_batches)),
    )
    .await;
    let better_ask = ask_transition(1_101, 1_201, 3, "0.4", "0", "10");
    rejected
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[better_ask]))
        .expect("better ask bucket");
    let request = post_orders_request(
        &rejected,
        "funding-batch",
        1,
        1_200,
        vec![
            market_item("needs-one-more-micro", false, "3", "0.6"),
            market_item("uses-untouched-queue", false, "2", "0.4"),
        ],
    );
    let registration = rejected.post_orders(request).expect("batch registration");
    let effective = registration.items[0].effective_processing_at_utc_ms;
    assert_eq!(
        registration.items[1].effective_processing_at_utc_ms,
        effective
    );
    close_platform_at(&mut rejected, effective, 3);
    rejected
        .drive_next_execution_action_through(effective)
        .expect("first funding item");
    assert_eq!(rejected.available_pusd(), decimal("3.125999"));
    rejected
        .drive_next_execution_action_through(effective)
        .expect("second funding item");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    rejected
        .drive_direct_response_delivery_through(received_at)
        .expect("batch response");
    {
        let batches = rejected_batches.lock().expect("batch capture mutex");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].items.len(), 2);
        assert!(matches!(
            batches[0].items[0].outcome,
            SubmissionItemOutcomeV1::RejectedNoEffect(
                SubmissionRejectedReasonV1::InsufficientAvailablePusd
            )
        ));
        let SubmissionItemOutcomeV1::Accepted(second) = &batches[0].items[1].outcome else {
            panic!("second item must use the queue discarded by the rejected preflight");
        };
        let SubmissionMeasurementV1::QuoteBudget(second_measurement) = &second.measurement else {
            panic!("second item must use quote-budget measurement");
        };
        assert_eq!(
            second_measurement.actual_matched_base_quantity_shares,
            decimal("5.000002")
        );
        assert_eq!(second_measurement.used_quote_amount_pusd, decimal("2"));
    }
    assert_eq!(rejected.available_pusd(), decimal("1.041999"));

    let accepted_responses = Arc::new(Mutex::new(Vec::new()));
    let mut accepted = ready_session(
        "task23-funding-equal",
        decimal("3.126"),
        1,
        accepting_capture_bridge(Arc::clone(&accepted_responses)),
    )
    .await;
    let better_ask = ask_transition(1_101, 1_201, 3, "0.4", "0", "10");
    accepted
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[better_ask]))
        .expect("better ask bucket");
    let registration = accepted
        .post_order(fak_request(
            &accepted,
            "funding-equal",
            1,
            1_200,
            "3",
            "0.6",
        ))
        .expect("equal funding registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut accepted, effective, 3);
    accepted
        .drive_next_execution_action_through(effective)
        .expect("equal funding processing");
    assert_eq!(accepted.available_pusd(), Decimal::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn delayed_batch_processing_keeps_original_response_ordinals_and_one_delivery() {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-batch-order",
        decimal("4"),
        1,
        accepting_batch_capture_bridge(Arc::clone(&batches)),
    )
    .await;
    let request = post_orders_request(
        &session,
        "batch-order",
        1,
        1_200,
        vec![
            market_item("delayed-fak", false, "3", "0.6"),
            limit_item("ordinary-gtc", "0.4", "5", true),
        ],
    );
    let registration = session
        .post_orders(request)
        .expect("mixed batch registration");
    let delayed_at = registration.items[0].effective_processing_at_utc_ms;
    let ordinary_at = registration.items[1].effective_processing_at_utc_ms;
    assert_eq!(delayed_at - ordinary_at, 250);
    close_platform_at(&mut session, ordinary_at, 2);
    assert_eq!(
        session
            .drive_next_execution_action_through(ordinary_at)
            .expect("ordinary item first"),
        SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: false
        }
    );
    assert!(batches.lock().expect("batch capture mutex").is_empty());
    assert_eq!(session.available_pusd(), decimal("2"));
    close_platform_at(&mut session, delayed_at, 2);
    assert_eq!(
        session
            .drive_next_execution_action_through(delayed_at)
            .expect("delayed item second"),
        SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true
        }
    );
    assert!(batches.lock().expect("batch capture mutex").is_empty());
    let received_at = delayed_at
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("mixed batch delivery");
    let responses = batches.lock().expect("batch capture mutex");
    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0]
            .items
            .iter()
            .map(|item| item.batch_item_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(matches!(
        responses[0].items[0].outcome,
        SubmissionItemOutcomeV1::RejectedNoEffect(
            SubmissionRejectedReasonV1::InsufficientAvailablePusd
        )
    ));
    assert!(matches!(
        responses[0].items[1].outcome,
        SubmissionItemOutcomeV1::Accepted(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn zero_fill_fok_and_fak_are_rejected_without_consuming_liquidity_or_funds() {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-fok-rollback",
        decimal("10"),
        1,
        accepting_batch_capture_bridge(Arc::clone(&batches)),
    )
    .await;
    let request = post_orders_request(
        &session,
        "fok-rollback",
        1,
        1_200,
        vec![
            market_item("fok-one-micro-short", true, "3.000001", "0.6"),
            market_item("fak-after-fok", false, "3", "0.6"),
            market_item("fak-with-empty-book", false, "3", "0.6"),
        ],
    );
    let registration = session
        .post_orders(request)
        .expect("FOK/FAK batch registration");
    let effective = registration.items[0].effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("FOK processing");
    assert_eq!(session.available_pusd(), decimal("10"));
    session
        .drive_next_execution_action_through(effective)
        .expect("FAK processing");
    let available_after_match = session.available_pusd();
    let principal_after_match = session.principal_encumbered_pusd();
    let fee_after_match = session.immediate_taker_fee_encumbered_pusd();
    session
        .drive_next_execution_action_through(effective)
        .expect("zero-match FAK processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("FOK/FAK delivery");
    let responses = batches.lock().expect("batch capture mutex");
    assert_eq!(responses[0].items.len(), 3);
    assert!(matches!(
        responses[0].items[0].outcome,
        SubmissionItemOutcomeV1::RejectedNoEffect(SubmissionRejectedReasonV1::FokNotFullyFilled)
    ));
    let SubmissionItemOutcomeV1::Accepted(fak) = &responses[0].items[1].outcome else {
        panic!("following FAK must use untouched liquidity");
    };
    let SubmissionMeasurementV1::QuoteBudget(fak_measurement) = &fak.measurement else {
        panic!("FAK must use quote budget");
    };
    assert_eq!(
        fak.platform_order_status,
        SubmissionPlatformOrderStatusV1::Matched
    );
    assert_eq!(fak_measurement.used_quote_amount_pusd, decimal("3"));
    assert_eq!(
        fak_measurement.actual_matched_base_quantity_shares,
        decimal("5")
    );
    assert_eq!(
        fak.terminal_reason,
        Some(SubmissionTerminalReasonV1::FakCompleted)
    );
    assert!(matches!(
        responses[0].items[2].outcome,
        SubmissionItemOutcomeV1::RejectedNoEffect(SubmissionRejectedReasonV1::FakNoMatch)
    ));
    assert_eq!(session.simulated_order_count(), 1);
    assert_eq!(session.available_pusd(), available_after_match);
    assert_eq!(session.principal_encumbered_pusd(), principal_after_match);
    assert_eq!(
        session.immediate_taker_fee_encumbered_pusd(),
        fee_after_match
    );
}

#[tokio::test(flavor = "current_thread")]
async fn zero_quote_external_residue_is_transparent_to_post_only_and_formal_matching() {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let (mut session, profile) = ready_long_session(
        "task23-zero-quote-transparent",
        decimal("10"),
        accepting_batch_capture_bridge(Arc::clone(&batches)),
    )
    .await;
    let request = PostOrdersRequestV1 {
        context: long_context_for_arrival(&session, &profile, "zero-quote-transparent", 1, 150_000),
        idempotency_key: "idem-zero-quote-transparent".to_owned(),
        items: vec![
            long_limit_item("post-only-sees-executable-ask", "0.5", "5", true),
            long_limit_item("ordinary-consumes-executable-ask", "0.5", "5", false),
        ],
    };
    let registration = session.post_orders(request).expect("batch registration");
    let effective = registration.items[0].effective_processing_at_utc_ms;
    assert_eq!(
        registration.items[1].effective_processing_at_utc_ms,
        effective
    );
    let changes = [
        long_ask_transition(120_000, 130_000, 3, "0.00001", "0", "0.000001"),
        long_ask_transition(120_000, 130_000, 4, "0.4", "0", "5"),
    ];
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            120_000, 2, 4, effective, &changes,
        ))
        .expect("add zero-quote residue and executable ask");
    close_platform_at(&mut session, effective, 4);
    session
        .drive_next_execution_action_through(effective)
        .expect("post-only preflight");
    session
        .drive_next_execution_action_through(effective)
        .expect("ordinary formal matching");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("batch delivery");

    let responses = batches.lock().expect("batch capture mutex");
    assert_eq!(responses.len(), 1);
    assert!(matches!(
        responses[0].items[0].outcome,
        SubmissionItemOutcomeV1::RejectedNoEffect(SubmissionRejectedReasonV1::PostOnlyWouldTake)
    ));
    let SubmissionItemOutcomeV1::Accepted(ordinary) = &responses[0].items[1].outcome else {
        panic!("ordinary order must consume the executable ask behind zero-quote residue");
    };
    assert_eq!(
        ordinary.platform_order_status,
        SubmissionPlatformOrderStatusV1::Matched
    );
    assert_eq!(ordinary.fills.len(), 1);
    assert_eq!(
        ordinary.fills[0].source_price_pusd_per_share,
        decimal("0.4")
    );
    assert_eq!(ordinary.fills[0].base_quantity_shares, decimal("5"));
    assert_eq!(ordinary.fills[0].quote_amount_pusd, decimal("2"));
    let SubmissionMeasurementV1::Base(measurement) = &ordinary.measurement else {
        panic!("ordinary limit order must use base measurement");
    };
    assert_eq!(measurement.matched_base_quantity_shares, decimal("5"));
    assert_eq!(measurement.open_base_quantity_shares, Decimal::ZERO);
    assert_eq!(measurement.matched_quote_amount_pusd, decimal("2"));
    let platform_order_id = ordinary.simulated_platform_order_id.clone();
    drop(responses);

    let remove_zero_quote_residue = long_ask_transition(
        effective + 1,
        effective + 101,
        5,
        "0.00001",
        "0.000001",
        "0",
    );
    assert_eq!(
        session
            .apply_closed_market_event_bucket(long_hidden_bucket(
                effective + 1,
                4,
                5,
                &[remove_zero_quote_residue],
            ))
            .expect("retained zero-quote residue remains available to DPT depletion"),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn real_adjacent_trade_level_delta_and_suppressed_book_apply_queue_reduction_once() {
    let trade = real_execution_event(
        REAL_PUBLIC_TRADE_RECORD,
        "ad744d302d40a1e5432561ecf5dbb50b02f4c4d1ed070cf069655eeb3e542e9e",
    );
    let level_delta = real_execution_event(
        REAL_LEVEL_DELTA_RECORD,
        "8f5591af9e10deeeb590fbc06d696c23fa2d0add7b396a666109e8d2ee8bf353",
    );
    assert_eq!(trade.market, level_delta.market);
    let ExecutionMarketEventPayloadV3::PublicTrade(public_trade) = &trade.payload else {
        panic!("第一条真实事件必须是 public_trade");
    };
    let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) = &level_delta.payload
    else {
        panic!("第二条真实事件必须是 level_execution_transition");
    };
    assert_eq!(public_trade.asset, transition.asset);
    assert_eq!(public_trade.price_pusd_per_share.0, "0.89");
    assert_eq!(public_trade.quantity_shares.0, "5");
    assert_eq!(transition.price_pusd_per_share.0, "0.89");
    assert_eq!(transition.basis, LevelExecutionBasis::LevelDeltaInference);
    assert_eq!(
        transition.source_attribution_quality,
        SourceAttributionQuality::Unattributed
    );
    assert_eq!(
        trade.time.received_at_utc_ms,
        level_delta.time.received_at_utc_ms
    );
    assert_eq!(
        trade
            .lineage
            .source_evidence
            .last()
            .and_then(|evidence| evidence.collector_admission.as_ref())
            .map(|coordinate| coordinate.collector_admission_sequence),
        Some(99_360_519)
    );
    assert_eq!(
        level_delta
            .lineage
            .source_evidence
            .last()
            .and_then(|evidence| evidence.collector_admission.as_ref())
            .map(|coordinate| coordinate.collector_admission_sequence),
        Some(99_360_520)
    );
    let trade_at = trade.time.source_event_at_utc_ms.expect("真实成交源时点");
    let delta_at = level_delta
        .time
        .source_event_at_utc_ms
        .expect("真实价位变化源时点");

    let down_asset = transition.asset.clone();
    let up_asset = AssetIdentityV1 {
        token_id: TokenId(
            "45253098770861975037434792771412818336165758089047645530470012231811628219325"
                .to_owned(),
        ),
        outcome: OutcomeIdentityV1 {
            index: 0,
            label: "Up".to_owned(),
        },
    };
    let platform_market = PlatformMarketV1::try_new(
        level_delta.market.clone(),
        [up_asset.clone(), down_asset.clone()],
    )
    .expect("真实样本场景市场");
    let scenario =
        Arc::new(PlatformScenarioV1::try_new(vec![platform_market]).expect("真实样本场景"));
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("profile");
    let mut spec = session_spec(
        &scenario,
        SimCallerRoleV1::ModelTrain,
        "task23-real-adjacent",
    );
    spec.initial_account_state =
        InitialAccountStateV1::try_new(decimal("1000")).expect("真实样本初始账户");
    spec.response_outbox_capacity = NonZeroUsize::new(1).expect("positive capacity");
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = SimExecutionProcessV1::new(
        Arc::clone(&rules),
        Arc::clone(&profile),
        Arc::clone(&scenario),
    )
    .create_session(spec, accepting_capture_bridge(Arc::clone(&captured)))
    .expect("真实样本会话");
    let initial = [
        real_sample_initial_snapshot(&level_delta, down_asset.clone(), 1, "0.1", "0.89", "234.21"),
        real_sample_initial_snapshot(&level_delta, up_asset, 2, "0.4", "0.6", "1"),
    ];
    session
        .apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            1_785_715_201_804,
            0,
            2,
            MillisecondBucketClosureV1::NextEventAt(
                trade.time.source_event_at_utc_ms.expect("真实成交源时点"),
            ),
            initial
                .iter()
                .map(|event| encode_execution_market_event_v3(event).expect("初始化事件编码"))
                .collect::<Vec<_>>(),
        ))
        .expect("真实样本初始化");

    let total_base_units = micro_units_v1(decimal("234.21")).expect("真实价位 base 微单位");
    let total_quote_units =
        segment_quote_units_v1(total_base_units, decimal("0.89")).expect("真实价位 quote 微单位");
    let total_quote =
        decimal_from_micro_units_v1(total_quote_units).expect("真实完整价位 quote 十进制");

    let probe_captured = Arc::new(Mutex::new(Vec::new()));
    let mut probe_spec = session_spec(
        &scenario,
        SimCallerRoleV1::ModelTrain,
        "task23-real-public-trade-queue-probe",
    );
    probe_spec.initial_account_state =
        InitialAccountStateV1::try_new(decimal("1000")).expect("真实探针初始账户");
    probe_spec.response_outbox_capacity = NonZeroUsize::new(1).expect("positive capacity");
    let mut probe = SimExecutionProcessV1::new(rules, profile, Arc::clone(&scenario))
        .create_session(
            probe_spec,
            accepting_capture_bridge(Arc::clone(&probe_captured)),
        )
        .expect("真实 public_trade 队列探针会话");
    probe
        .apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            1_785_715_201_804,
            0,
            2,
            MillisecondBucketClosureV1::NextEventAt(trade_at),
            initial
                .iter()
                .map(|event| encode_execution_market_event_v3(event).expect("探针初始化事件编码"))
                .collect::<Vec<_>>(),
        ))
        .expect("真实 public_trade 队列探针初始化");
    let probe_registration = probe
        .post_order(PostOrderRequestV1 {
            context: request_context(&probe, "real-public-trade-queue-probe", 1, trade_at + 1),
            idempotency_key: "idem-real-public-trade-queue-probe".to_owned(),
            item: PostOrderItemV1 {
                item_id: "item-real-public-trade-queue-probe".to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: level_delta.market.condition_id.clone(),
                    token_id: down_asset.token_id.clone(),
                    kind: SimBuyOrderKindV1::FokMarket {
                        spend_pusd: total_quote,
                        worst_price_pusd_per_share: decimal("0.89"),
                    },
                },
            },
        })
        .expect("真实 public_trade 队列探针 FOK 登记");
    let probe_effective = probe_registration.item.effective_processing_at_utc_ms;
    probe
        .apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            trade_at,
            2,
            3,
            MillisecondBucketClosureV1::NextEventAt(probe_effective),
            vec![encode_execution_market_event_v3(&trade).expect("探针真实成交事件编码")],
        ))
        .expect("真实 public_trade 队列探针成交桶");
    close_platform_at(&mut probe, probe_effective, 3);
    probe
        .drive_next_execution_action_through(probe_effective)
        .expect("真实 public_trade 后完整队列探针撮合");
    let probe_received_at = probe_effective
        + i64::try_from(probe_registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    probe
        .drive_direct_response_delivery_through(probe_received_at)
        .expect("真实 public_trade 队列探针响应");
    let probe_responses = probe_captured.lock().expect("探针响应锁");
    let probe_measurement = quote_measurement(&probe_responses[0]);
    assert_eq!(probe_measurement.used_quote_amount_pusd, total_quote);
    assert_eq!(
        probe_measurement.actual_matched_base_quantity_shares,
        decimal("234.21")
    );

    let consumed_base_units = micro_units_v1(decimal("5")).expect("真实减量微单位");
    let consumed_quote_units =
        cumulative_segment_quote_units_v1(total_base_units, total_quote_units, consumed_base_units)
            .expect("真实减量累计 quote");
    let remaining_quote_units = total_quote_units
        .checked_sub(consumed_quote_units)
        .expect("真实剩余 quote");
    let remaining_quote =
        decimal_from_micro_units_v1(remaining_quote_units).expect("真实剩余 quote 十进制");
    let request = PostOrderRequestV1 {
        context: request_context(&session, "real-adjacent-fak", 1, 1_785_715_695_521),
        idempotency_key: "idem-real-adjacent-fak".to_owned(),
        item: PostOrderItemV1 {
            item_id: "item-real-adjacent-fak".to_owned(),
            order: SimBuyOrderV1 {
                condition_id: level_delta.market.condition_id.clone(),
                token_id: down_asset.token_id.clone(),
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: remaining_quote,
                    worst_price_pusd_per_share: decimal("0.89"),
                },
            },
        },
    };
    let registration = session.post_order(request).expect("真实样本 FAK 登记");
    let effective = registration.item.effective_processing_at_utc_ms;

    session
        .apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            trade_at,
            2,
            3,
            MillisecondBucketClosureV1::NextEventAt(delta_at),
            vec![encode_execution_market_event_v3(&trade).expect("真实成交事件编码")],
        ))
        .expect("真实 public_trade 桶");
    assert_eq!(
        session.platform_derived_quantity(
            &level_delta.market.condition_id.0,
            &down_asset.token_id.0,
            BookSide::Ask,
            decimal("0.89"),
        ),
        Some(decimal("234.21"))
    );
    let delta_bucket = ClosedMarketEventBucketV1::hidden(
        delta_at,
        3,
        4,
        MillisecondBucketClosureV1::NextEventAt(effective),
        vec![encode_execution_market_event_v3(&level_delta).expect("真实价位事件编码")],
    );
    assert_eq!(
        session
            .apply_closed_market_event_bucket(delta_bucket.clone())
            .expect("真实 LevelDelta 桶"),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        session
            .apply_closed_market_event_bucket(delta_bucket)
            .expect("真实 LevelDelta 物理重投"),
        MarketBucketApplyDispositionV1::PhysicalRetry
    );
    assert_eq!(
        session.platform_derived_quantity(
            &level_delta.market.condition_id.0,
            &down_asset.token_id.0,
            BookSide::Ask,
            decimal("0.89"),
        ),
        Some(decimal("229.21"))
    );

    let witness: serde_json::Value = serde_json::from_slice(
        fixture_bytes_without_trailing_newline(REAL_SUPPRESSED_BOOK_WITNESS),
    )
    .expect("真实抑制见证解码");
    assert_eq!(witness["operation"], "CONFIRM");
    assert_eq!(
        witness["reason_code"],
        "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION"
    );
    assert_eq!(witness["disposition"], "SUPPRESSED");
    assert_eq!(witness["collector_admission_sequence"], 99_360_522);
    assert_eq!(witness["emitted_execution_market_event_count"], 0);
    assert_eq!(
        witness["source_reduction_report_payload_sha256"],
        "d88ec8dc555137cb94504ec5b0b9be3cc3aad4759350486327633aaaf2e06e7b"
    );

    close_platform_at(&mut session, effective, 4);
    session
        .drive_next_execution_action_through(effective)
        .expect("真实样本 FAK 撮合");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms)
            .expect("test delay fits i64");
    session
        .drive_direct_response_delivery_through(received_at)
        .expect("真实样本响应交付");
    let responses = captured.lock().expect("capture mutex");
    let measurement = quote_measurement(&responses[0]);
    assert_eq!(measurement.used_quote_amount_pusd, remaining_quote);
    assert_eq!(
        measurement.actual_matched_base_quantity_shares,
        decimal("229.21")
    );
    assert_eq!(
        session.simulated_fill_count(match &responses[0].item.outcome {
            SubmissionItemOutcomeV1::Accepted(accepted) => {
                &accepted.simulated_platform_order_id
            }
            _ => panic!("真实样本 FAK 必须成交"),
        }),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mutation_idempotency_reuses_one_business_effect_across_logical_request_ids() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-mutation-idempotency",
        decimal("10"),
        2,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let first_request = gtc_request(&session, "mutation-first", 1, 1_200, "0.4", "5", true);
    let mut second_request = first_request.clone();
    second_request.context = request_context(&session, "mutation-second", 2, 1_201);

    let first = session
        .post_order(first_request)
        .expect("first logical mutation registration");
    let second = session
        .post_order(second_request)
        .expect("same mutation under a new logical request registration");
    let first_effective = first.item.effective_processing_at_utc_ms;
    let second_effective = second.item.effective_processing_at_utc_ms;
    let first_received_at = first_effective
        + i64::try_from(first.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    let second_received_at = second_effective
        + i64::try_from(second.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    let first_processing_at = first
        .item
        .effective_processing_at_utc_ms
        .min(second.item.effective_processing_at_utc_ms);
    let final_processing_at = first
        .item
        .effective_processing_at_utc_ms
        .max(second.item.effective_processing_at_utc_ms);
    close_platform_at(&mut session, first_processing_at, 2);
    session
        .drive_next_execution_action_through(first_processing_at)
        .expect("first mutation processing");
    if final_processing_at > first_processing_at {
        close_platform_at(&mut session, final_processing_at, 2);
    }
    session
        .drive_next_execution_action_through(final_processing_at)
        .expect("idempotent logical retry processing");

    let delivery_through = [first, second]
        .iter()
        .map(|registration| {
            registration.item.effective_processing_at_utc_ms
                + i64::try_from(registration.receipt.transit.inbound_delay_ms)
                    .expect("test delay fits i64")
        })
        .max()
        .expect("two registrations");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("first idempotent response delivery");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("second idempotent response delivery");

    let responses = captured.lock().expect("capture mutex");
    assert_eq!(responses.len(), 2);
    let SubmissionItemOutcomeV1::Accepted(first_outcome) = &responses[0].item.outcome else {
        panic!("first mutation must be accepted");
    };
    let SubmissionItemOutcomeV1::Accepted(second_outcome) = &responses[1].item.outcome else {
        panic!("idempotent logical retry must reuse the accepted outcome");
    };
    assert_eq!(first_outcome, second_outcome);
    assert_ne!(
        responses[0].metadata.global_request_id,
        responses[1].metadata.global_request_id
    );
    assert_ne!(
        responses[0].metadata.delivery_id,
        responses[1].metadata.delivery_id
    );
    assert_eq!(
        responses[0].item.effective_processing_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].item.effective_processing_at_utc_ms,
        second_effective
    );
    assert_eq!(
        responses[0].item.submission_effect_identity,
        responses[1].item.submission_effect_identity
    );
    assert_eq!(
        responses[0].item.submission_effect_formed_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].item.submission_effect_formed_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[0].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::NewEffect
    );
    assert_eq!(
        responses[1].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::ExistingEffect
    );
    assert_eq!(
        responses[0].metadata.platform_responded_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].metadata.platform_responded_at_utc_ms,
        second_effective
    );
    assert_eq!(
        responses[0].metadata.response_received_at_utc_ms,
        first_received_at
    );
    assert_eq!(
        responses[1].metadata.response_received_at_utc_ms,
        second_received_at
    );
    assert_eq!(session.available_pusd(), decimal("8"));
    assert_eq!(session.principal_encumbered_pusd(), decimal("2"));
    assert_eq!(session.simulated_order_count(), 1);
    assert_eq!(
        session.simulated_fill_count(&first_outcome.simulated_platform_order_id),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mutation_idempotency_freezes_rejection_after_market_state_improves() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-rejected-mutation-idempotency",
        decimal("10"),
        2,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let first_request = gtc_request(
        &session,
        "rejected-mutation-first",
        1,
        1_200,
        "0.6",
        "5",
        true,
    );
    let mut second_request = first_request.clone();
    second_request.context = request_context(&session, "rejected-mutation-second", 2, 12_000);
    let first = session
        .post_order(first_request)
        .expect("first rejected mutation registration");
    let second = session
        .post_order(second_request)
        .expect("second rejected mutation registration");
    let first_effective = first.item.effective_processing_at_utc_ms;
    let second_effective = second.item.effective_processing_at_utc_ms;
    let first_received_at = first_effective
        + i64::try_from(first.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    let second_received_at = second_effective
        + i64::try_from(second.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    assert!(first.item.effective_processing_at_utc_ms < second.item.effective_processing_at_utc_ms);
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            first.item.effective_processing_at_utc_ms,
            2,
            2,
            second.item.effective_processing_at_utc_ms,
            &[],
        ))
        .expect("close first action with the next market event boundary");
    session
        .drive_next_execution_action_through(first.item.effective_processing_at_utc_ms)
        .expect("first post-only rejection");

    let ask_removed = ask_transition(
        second.item.effective_processing_at_utc_ms,
        second.item.effective_processing_at_utc_ms + 100,
        3,
        "0.6",
        "5",
        "0",
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(
            second.item.effective_processing_at_utc_ms,
            3,
            &[ask_removed],
        ))
        .expect("remove the crossing ask before the second logical request");
    session
        .drive_next_execution_action_through(second.item.effective_processing_at_utc_ms)
        .expect("idempotent rejection reuse");

    let delivery_through = [first, second]
        .iter()
        .map(|registration| {
            registration.item.effective_processing_at_utc_ms
                + i64::try_from(registration.receipt.transit.inbound_delay_ms)
                    .expect("test delay fits i64")
        })
        .max()
        .expect("two registrations");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("first rejection response delivery");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("second rejection response delivery");

    let responses = captured.lock().expect("capture mutex");
    assert_eq!(responses.len(), 2);
    for response in responses.iter() {
        assert!(matches!(
            response.item.outcome,
            SubmissionItemOutcomeV1::RejectedNoEffect(
                SubmissionRejectedReasonV1::PostOnlyWouldTake
            )
        ));
    }
    assert_eq!(
        responses[0].item.effective_processing_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].item.effective_processing_at_utc_ms,
        second_effective
    );
    assert_eq!(
        responses[0].item.submission_effect_identity,
        responses[1].item.submission_effect_identity
    );
    assert_eq!(
        responses[0].item.submission_effect_formed_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].item.submission_effect_formed_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[0].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::NewEffect
    );
    assert_eq!(
        responses[1].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::ExistingEffect
    );
    assert_eq!(
        responses[0].metadata.response_received_at_utc_ms,
        first_received_at
    );
    assert_eq!(
        responses[1].metadata.response_received_at_utc_ms,
        second_received_at
    );
    assert_eq!(session.simulated_order_count(), 0);
    assert_eq!(session.available_pusd(), decimal("10"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn mutation_idempotency_freezes_evidence_unknown_after_resync() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-evidence-mutation-idempotency",
        decimal("10"),
        2,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let gap = target_gap_event(1_101, 1_201, 3);
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[gap]))
        .expect("target asset gap");
    let first_request = gtc_request(
        &session,
        "evidence-mutation-first",
        1,
        1_300,
        "0.4",
        "5",
        true,
    );
    let mut second_request = first_request.clone();
    second_request.context = request_context(&session, "evidence-mutation-second", 2, 12_000);
    let first = session
        .post_order(first_request)
        .expect("first evidence mutation registration");
    let second = session
        .post_order(second_request)
        .expect("second evidence mutation registration");
    let first_effective = first.item.effective_processing_at_utc_ms;
    let second_effective = second.item.effective_processing_at_utc_ms;
    let first_received_at = first_effective
        + i64::try_from(first.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    let second_received_at = second_effective
        + i64::try_from(second.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    assert!(first.item.effective_processing_at_utc_ms < second.item.effective_processing_at_utc_ms);
    session
        .apply_closed_market_event_bucket(long_hidden_bucket_with_next_event_at(
            first.item.effective_processing_at_utc_ms,
            3,
            3,
            second.item.effective_processing_at_utc_ms,
            &[],
        ))
        .expect("close first evidence action");
    session
        .drive_next_execution_action_through(first.item.effective_processing_at_utc_ms)
        .expect("first evidence-unknown result");

    let rebuilt_ask = ask_transition(
        second.item.effective_processing_at_utc_ms,
        second.item.effective_processing_at_utc_ms + 100,
        4,
        "0.6",
        "0",
        "5",
    );
    let recovered = target_recovered_event(
        second.item.effective_processing_at_utc_ms,
        second.item.effective_processing_at_utc_ms + 100,
        5,
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(
            second.item.effective_processing_at_utc_ms,
            5,
            &[rebuilt_ask, recovered],
        ))
        .expect("target asset resync before second logical request");
    session
        .drive_next_execution_action_through(second.item.effective_processing_at_utc_ms)
        .expect("idempotent evidence result reuse");

    let delivery_through = [first, second]
        .iter()
        .map(|registration| {
            registration.item.effective_processing_at_utc_ms
                + i64::try_from(registration.receipt.transit.inbound_delay_ms)
                    .expect("test delay fits i64")
        })
        .max()
        .expect("two registrations");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("first evidence response delivery");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("second evidence response delivery");

    let responses = captured.lock().expect("capture mutex");
    assert_eq!(responses.len(), 2);
    for response in responses.iter() {
        assert!(matches!(
            response.item.outcome,
            SubmissionItemOutcomeV1::EvidenceUnknownNoEffect(
                SubmissionEvidenceUnknownReasonV1::TargetAssetNotFresh(MarketDataQuality::Missing)
            )
        ));
    }
    assert_eq!(
        responses[0].item.effective_processing_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].item.effective_processing_at_utc_ms,
        second_effective
    );
    assert_eq!(
        responses[0].item.submission_effect_identity,
        responses[1].item.submission_effect_identity
    );
    assert_eq!(
        responses[0].item.submission_effect_formed_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[1].item.submission_effect_formed_at_utc_ms,
        first_effective
    );
    assert_eq!(
        responses[0].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::NewEffect
    );
    assert_eq!(
        responses[1].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::ExistingEffect
    );
    assert_eq!(
        responses[0].metadata.response_received_at_utc_ms,
        first_received_at
    );
    assert_eq!(
        responses[1].metadata.response_received_at_utc_ms,
        second_received_at
    );
    assert_eq!(session.simulated_order_count(), 0);
    assert_eq!(session.available_pusd(), decimal("10"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn batch_mutation_idempotency_reuses_interleaved_item_effects_in_original_ordinal_order() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-batch-mutation-idempotency",
        decimal("5.084"),
        2,
        accepting_batch_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let first_request = post_orders_request(
        &session,
        "batch-mutation-first",
        1,
        1_200,
        vec![
            market_item("shared-delayed-fak", false, "3", "0.6"),
            limit_item("shared-ordinary-gtc", "0.4", "5", true),
        ],
    );
    let mut second_request = first_request.clone();
    second_request.context = request_context(&session, "batch-mutation-second", 2, 1_201);
    let first = session
        .post_orders(first_request)
        .expect("first batch mutation registration");
    let second = session
        .post_orders(second_request)
        .expect("second batch mutation registration");
    let first_request_id = first.receipt.global_request_id.clone();
    let second_request_id = second.receipt.global_request_id.clone();
    let first_effective = first
        .items
        .iter()
        .map(|item| item.effective_processing_at_utc_ms)
        .collect::<Vec<_>>();
    let second_effective = second
        .items
        .iter()
        .map(|item| item.effective_processing_at_utc_ms)
        .collect::<Vec<_>>();
    let first_received_at = *first_effective.iter().max().expect("first batch times")
        + i64::try_from(first.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    let second_received_at = *second_effective.iter().max().expect("second batch times")
        + i64::try_from(second.receipt.transit.inbound_delay_ms).expect("test delay fits i64");
    let mut action_times = first
        .items
        .iter()
        .chain(second.items.iter())
        .map(|item| item.effective_processing_at_utc_ms)
        .collect::<Vec<_>>();
    action_times.sort_unstable();
    assert_eq!(action_times.len(), 4);
    for action_at in action_times {
        close_platform_at(&mut session, action_at, 2);
        session
            .drive_next_execution_action_through(action_at)
            .expect("interleaved batch item processing");
    }

    let delivery_through = [first, second]
        .iter()
        .map(|registration| {
            registration
                .items
                .iter()
                .map(|item| item.effective_processing_at_utc_ms)
                .max()
                .expect("non-empty batch")
                + i64::try_from(registration.receipt.transit.inbound_delay_ms)
                    .expect("test delay fits i64")
        })
        .max()
        .expect("two registrations");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("first batch response delivery");
    session
        .drive_direct_response_delivery_through(delivery_through)
        .expect("second batch response delivery");

    let responses = captured.lock().expect("batch capture mutex");
    assert_eq!(responses.len(), 2);
    let first_response = responses
        .iter()
        .find(|response| response.metadata.global_request_id == first_request_id)
        .expect("first batch response");
    let second_response = responses
        .iter()
        .find(|response| response.metadata.global_request_id == second_request_id)
        .expect("second batch response");
    assert_ne!(
        first_response.metadata.global_request_id,
        second_response.metadata.global_request_id
    );
    assert_ne!(
        first_response.metadata.delivery_id,
        second_response.metadata.delivery_id
    );
    assert_eq!(first_response.items.len(), 2);
    assert_eq!(
        first_response
            .items
            .iter()
            .map(|item| &item.outcome)
            .collect::<Vec<_>>(),
        second_response
            .items
            .iter()
            .map(|item| &item.outcome)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first_response
            .items
            .iter()
            .map(|item| item.batch_item_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        second_response
            .items
            .iter()
            .map(|item| item.batch_item_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_ne!(
        first_response.items[0].effective_processing_at_utc_ms,
        second_response.items[0].effective_processing_at_utc_ms
    );
    for ordinal in 0..2 {
        let (expected_formed_at, first_disposition, second_disposition) =
            if first_effective[ordinal] <= second_effective[ordinal] {
                (
                    first_effective[ordinal],
                    SubmissionEffectDispositionV1::NewEffect,
                    SubmissionEffectDispositionV1::ExistingEffect,
                )
            } else {
                (
                    second_effective[ordinal],
                    SubmissionEffectDispositionV1::ExistingEffect,
                    SubmissionEffectDispositionV1::NewEffect,
                )
            };
        assert_eq!(
            first_response.items[ordinal].effective_processing_at_utc_ms,
            first_effective[ordinal]
        );
        assert_eq!(
            second_response.items[ordinal].effective_processing_at_utc_ms,
            second_effective[ordinal]
        );
        assert_eq!(
            first_response.items[ordinal].submission_effect_identity,
            second_response.items[ordinal].submission_effect_identity
        );
        assert_eq!(
            first_response.items[ordinal].submission_effect_formed_at_utc_ms,
            expected_formed_at
        );
        assert_eq!(
            second_response.items[ordinal].submission_effect_formed_at_utc_ms,
            expected_formed_at
        );
        assert_eq!(
            first_response.items[ordinal].submission_effect_disposition,
            first_disposition
        );
        assert_eq!(
            second_response.items[ordinal].submission_effect_disposition,
            second_disposition
        );
    }
    assert_eq!(
        first_response.metadata.platform_responded_at_utc_ms,
        *first_effective.iter().max().expect("first batch times")
    );
    assert_eq!(
        second_response.metadata.platform_responded_at_utc_ms,
        *second_effective.iter().max().expect("second batch times")
    );
    assert_eq!(
        first_response.metadata.response_received_at_utc_ms,
        first_received_at
    );
    assert_eq!(
        second_response.metadata.response_received_at_utc_ms,
        second_received_at
    );
    assert_eq!(session.simulated_order_count(), 2);
    assert_eq!(session.available_pusd(), Decimal::ZERO);
    assert_eq!(session.principal_encumbered_pusd(), decimal("5"));
    assert_eq!(
        session.immediate_taker_fee_encumbered_pusd(),
        decimal("0.084")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn full_retry_reuses_the_same_arc_before_advancing_past_an_unfinished_request() {
    let seen = Arc::new(Mutex::new(Vec::<Arc<PostOrderDirectResponseV1>>::new()));
    let bridge_seen = Arc::clone(&seen);
    let bridge = SubmissionResponseBridgeV1::new(
        move |response| {
            let mut seen = bridge_seen.lock().expect("Full bridge mutex");
            seen.push(Arc::clone(&response));
            if seen.len() == 1 {
                SubmissionResponseAcceptanceV1::Full
            } else {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            }
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
    );
    let mut session = ready_session("task23-full-retry", decimal("10"), 2, bridge).await;
    let first_request = gtc_request(&session, "full-first", 1, 1_200, "0.4", "5", true);
    let first = session
        .post_order(first_request.clone())
        .expect("first registration");
    let first_received = first.item.effective_processing_at_utc_ms
        + i64::try_from(first.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    let second_request = gtc_request(
        &session,
        "full-second",
        2,
        first_received + 1,
        "0.4",
        "5",
        true,
    );
    let second = session
        .post_order(second_request.clone())
        .expect("second registration");
    assert!(second.item.effective_processing_at_utc_ms > first.item.effective_processing_at_utc_ms);
    close_platform_at(&mut session, first.item.effective_processing_at_utc_ms, 2);
    session
        .drive_next_execution_action_through(first.item.effective_processing_at_utc_ms)
        .expect("first processing");
    assert_eq!(
        session
            .drive_direct_response_delivery_through(first_received)
            .expect("first delivery attempt"),
        SubmissionDeliveryDispositionV1::Full
    );
    assert_eq!(session.model_visible_cursor().private_feedback_sequence, 0);
    let available_after_business = session.available_pusd();

    let second_received = second.item.effective_processing_at_utc_ms
        + i64::try_from(second.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    assert_eq!(
        session
            .drive_direct_response_delivery_through(second_received)
            .expect("Full retry must precede the unfinished second response barrier"),
        SubmissionDeliveryDispositionV1::Delivered
    );
    assert_eq!(session.model_visible_cursor().private_feedback_sequence, 1);
    assert_eq!(session.available_pusd(), available_after_business);
    let seen = seen.lock().expect("Full bridge mutex");
    assert_eq!(seen.len(), 2);
    assert!(Arc::ptr_eq(&seen[0], &seen[1]));
    assert_eq!(seen[0].metadata, seen[1].metadata);
    drop(seen);

    close_platform_at(&mut session, second.item.effective_processing_at_utc_ms, 2);
    session
        .drive_next_execution_action_through(second.item.effective_processing_at_utc_ms)
        .expect("previously registered second request remains processable");
    assert_eq!(session.available_pusd(), decimal("6"));
    let retry = session
        .post_order(second_request)
        .expect("second physical retry after first Full");
    assert_eq!(retry.receipt.transit, second.receipt.transit);
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_confirmation_closes_only_new_ingress_and_keeps_existing_work() {
    let bridge = SubmissionResponseBridgeV1::new(
        |response| {
            let mut wrong = response.metadata.delivery_confirmation();
            wrong.payload_sha256 = Sha256Hex("0".repeat(64));
            SubmissionResponseAcceptanceV1::Accepted(wrong)
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
    );
    let mut session = ready_session("task23-wrong-confirm", decimal("10"), 2, bridge).await;
    let first_request = gtc_request(&session, "wrong-first", 1, 1_200, "0.4", "5", true);
    let first = session
        .post_order(first_request.clone())
        .expect("first registration");
    let second_request = gtc_request(
        &session,
        "wrong-second",
        2,
        first.item.effective_processing_at_utc_ms + 1,
        "0.4",
        "5",
        true,
    );
    let second = session
        .post_order(second_request.clone())
        .expect("second registration");
    close_platform_at(&mut session, first.item.effective_processing_at_utc_ms, 2);
    session
        .drive_next_execution_action_through(first.item.effective_processing_at_utc_ms)
        .expect("first processing");
    let received_at = first.item.effective_processing_at_utc_ms
        + i64::try_from(first.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    assert!(matches!(
        session.drive_direct_response_delivery_through(received_at),
        Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict)
    ));
    assert_eq!(session.model_visible_cursor().private_feedback_sequence, 0);
    assert_eq!(
        session
            .post_order(first_request)
            .expect("physical retry remains available")
            .receipt
            .transit,
        first.receipt.transit
    );
    let new_request = gtc_request(
        &session,
        "wrong-new",
        3,
        second.item.effective_processing_at_utc_ms + 1,
        "0.4",
        "5",
        true,
    );
    assert!(matches!(
        session.post_order(new_request),
        Err(SimExecutionContractErrorV1::ResponseBridgeClosed)
    ));

    close_platform_at(&mut session, second.item.effective_processing_at_utc_ms, 2);
    session
        .drive_next_execution_action_through(second.item.effective_processing_at_utc_ms)
        .expect("existing second request must still process");
    assert_eq!(session.available_pusd(), decimal("6"));
    assert_eq!(
        session
            .post_order(second_request)
            .expect("existing second physical retry")
            .receipt
            .transit,
        second.receipt.transit
    );
}

#[tokio::test(flavor = "current_thread")]
async fn disconnected_bridge_retains_the_response_and_registration_receipt() {
    let bridge = SubmissionResponseBridgeV1::new(
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
        |_| SubmissionResponseAcceptanceV1::Disconnected,
    );
    let mut session = ready_session("task23-disconnected", decimal("10"), 1, bridge).await;
    let request = gtc_request(&session, "disconnected", 1, 1_200, "0.4", "5", true);
    let registration = session
        .post_order(request.clone())
        .expect("disconnected registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("disconnected business processing");
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    assert_eq!(
        session
            .drive_direct_response_delivery_through(received_at)
            .expect("disconnected delivery"),
        SubmissionDeliveryDispositionV1::Closed
    );
    assert_eq!(session.model_visible_cursor().private_feedback_sequence, 0);
    assert_eq!(session.available_pusd(), decimal("8"));
    assert_eq!(
        session
            .post_order(request)
            .expect("physical retry survives disconnect")
            .receipt
            .transit,
        registration.receipt.transit
    );
    let new_request = gtc_request(&session, "after-disconnect", 2, 1_300, "0.4", "5", true);
    assert!(matches!(
        session.post_order(new_request),
        Err(SimExecutionContractErrorV1::ResponseBridgeClosed)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn private_schedule_cannot_cross_an_unformed_direct_response() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = ready_session(
        "task23-private-barrier",
        decimal("10"),
        1,
        accepting_capture_bridge(Arc::clone(&captured)),
    )
    .await;
    let request = gtc_request(&session, "private-barrier", 1, 1_200, "0.4", "5", true);
    let registration = session.post_order(request).expect("barrier registration");
    let effective = registration.item.effective_processing_at_utc_ms;
    let received_at = effective
        + i64::try_from(registration.receipt.transit.inbound_delay_ms).expect("delay fits i64");
    assert!(matches!(
        session.drive_direct_response_delivery_through(received_at),
        Err(SimExecutionContractErrorV1::PrivateFactScheduleCrossesExpectedResponse(id))
            if id == "private-barrier"
    ));
    assert_eq!(session.model_visible_cursor().private_feedback_sequence, 0);
    assert_eq!(session.available_pusd(), decimal("10"));
    close_platform_at(&mut session, effective, 2);
    session
        .drive_next_execution_action_through(effective)
        .expect("barrier failure must leave action processable");
    assert_eq!(
        session
            .drive_direct_response_delivery_through(received_at)
            .expect("delivery after response formation"),
        SubmissionDeliveryDispositionV1::Delivered
    );
    assert_eq!(session.model_visible_cursor().private_feedback_sequence, 1);
}
