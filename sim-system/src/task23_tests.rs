use contracts::{
    BookSide, EventQualityV2, ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    LevelExecutionBasis, LevelExecutionTransitionV3, MarketDataQuality, QueueResultQuality,
    ResyncQuality, SourceAttributionQuality,
};
use rust_decimal::Decimal;

use crate::task21_tests::{asset, market, orderbook_evidence, sealed_event};

pub(crate) fn bid_transition(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    price: &str,
    before: &str,
    after: &str,
) -> ExecutionMarketEventV3 {
    let before_decimal: Decimal = before.parse().expect("test decimal");
    let after_decimal: Decimal = after.parse().expect("test decimal");
    let added = (after_decimal - before_decimal).max(Decimal::ZERO);
    let reduced = (before_decimal - after_decimal).max(Decimal::ZERO);
    sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: contracts::Sha256Hex("8".repeat(64)),
            market: market(),
            asset: asset("1", 0, "Up"),
            side: BookSide::Bid,
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
