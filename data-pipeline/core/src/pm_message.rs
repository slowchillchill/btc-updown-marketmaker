//! 原类型化消息到完整规范事实；不拆分成员，不读取物理文件。
use contracts::*;

use crate::pm_reduction::PmMarketContextV1;
use crate::typed_raw::{
    self, PolymarketOrderbookEvent as Raw, PolymarketOrderbookRow, PolymarketSide,
};
use crate::{ContractError, hash_domain_bytes, invalid, stable_json, typed_raw_contract_error};

pub struct ParsedCanonicalPmMessageV1<'a> {
    pub message: &'a CanonicalPmMessageV1,
    pub source: &'a SourceEvidenceRefV3,
    pub source_at_utc_ms: i64,
    pub admission: &'a CollectorAdmissionCoordinateV1,
}

pub fn parse_canonical_pm_message(
    message: &CanonicalPmMessageV1,
) -> Result<ParsedCanonicalPmMessageV1<'_>, ContractError> {
    let [source] = message.lineage.source_evidence.as_slice() else {
        return Err(invalid(
            "source_evidence",
            "one Polymarket message requires its original source evidence",
        ));
    };
    Ok(ParsedCanonicalPmMessageV1 {
        message,
        source,
        source_at_utc_ms: source.source_event_at_utc_ms.ok_or_else(|| {
            invalid(
                "source_event_at_utc_ms",
                "Polymarket orderbook evidence requires source event time",
            )
        })?,
        admission: source.collector_admission.as_ref().ok_or_else(|| {
            invalid(
                "collector_admission",
                "Polymarket orderbook evidence requires its admission coordinate",
            )
        })?,
    })
}

/// 调用方提供本次逻辑输入位置；它不参与事实或原消息组 ID。
/// 本入口允许规范 FullBook 的显式零量；旧原始解析入口仍要求正量。
pub fn build_canonical_pm_message(
    context: &PmMarketContextV1,
    source: SourceEvidenceRefV3,
    input_record: CanonicalInputRecordRefV1,
    payload: CanonicalPmPayloadV1,
) -> Result<CanonicalPmMessageV1, ContractError> {
    let group_id = hash_domain_bytes(
        b"data-pipeline/public-pm-message-group/v1\0",
        &stable_json(&source.source_message_id)?,
    );
    let event_id = CanonicalEventId(
        hash_domain_bytes(
            b"data-pipeline/canonical-pm-message/v1\0",
            &stable_json(&(
                &context.market,
                &context.canonical_rule_version,
                &source,
                &payload,
            ))?,
        )
        .0,
    );
    Ok(CanonicalPmMessageV1 {
        schema_version: CANONICAL_PM_MESSAGE_SCHEMA_V1.to_owned(),
        event_id,
        market: context.market.clone(),
        group_id,
        time: EventTimeV2 {
            source_event_at_utc_ms: source.source_event_at_utc_ms,
            received_at_utc_ms: source.received_at_utc_ms,
        },
        lineage: EventLineageV3 {
            canonical_rule_version: context.canonical_rule_version.clone(),
            source_evidence: vec![source],
        },
        input_record,
        payload,
    })
}

pub fn canonical_pm_message_from_typed_raw(
    context: &PmMarketContextV1,
    row: &PolymarketOrderbookRow,
    input_record: CanonicalInputRecordRefV1,
) -> Result<CanonicalPmMessageV1, ContractError> {
    row.validate().map_err(typed_raw_contract_error)?;
    if row.market != context.market.condition_id.0 {
        return Err(invalid(
            "row.market",
            "does not match the registered condition ID",
        ));
    }
    let admission = CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: row.source_sequence_epoch_id.clone(),
        collector_admission_sequence: row.collector_admission_sequence,
        admitted_at_utc_ms: row.collector_admission_time_utc_ms,
    };
    let source_kind = CanonicalSourceKindV2::PolymarketOrderbook;
    let native: Option<SourceNativeIdentityV1> = None;
    let source = SourceEvidenceRefV3 {
        source_message_id: hash_domain_bytes(
            b"data-pipeline/source-message/v1\0",
            &stable_json(&(
                source_kind,
                &native,
                row.timestamp,
                row.timestamp_received,
                Some(&admission),
            ))?,
        ),
        source_kind,
        source_native_identity: native,
        source_event_at_utc_ms: row.timestamp,
        received_at_utc_ms: row.timestamp_received,
        collector_admission: Some(admission),
    };
    let payload = payload_from_typed_raw(row, &context.assets)?;
    build_canonical_pm_message(context, source, input_record, payload)
}

fn decimal(value: &str, field: &str) -> Result<ExactDecimal, ContractError> {
    typed_raw::normalize_decimal_text(value, field).map_err(typed_raw_contract_error)
}

fn optional(value: &Option<String>, field: &str) -> Result<Option<ExactDecimal>, ContractError> {
    value
        .as_deref()
        .map(|value| decimal(value, field))
        .transpose()
}

fn side(value: PolymarketSide) -> CanonicalPmSideV1 {
    match value {
        PolymarketSide::Buy => CanonicalPmSideV1::Buy,
        PolymarketSide::Sell => CanonicalPmSideV1::Sell,
        PolymarketSide::Unknown => CanonicalPmSideV1::Unknown,
    }
}

fn quote(
    bid: &Option<String>,
    ask: &Option<String>,
    spread: &Option<String>,
) -> Result<CanonicalBbaFactV1, ContractError> {
    Ok(CanonicalBbaFactV1 {
        best_bid_pusd_per_share: optional(bid, "best_bid")?.filter(|value| value.0 != "0"),
        best_ask_pusd_per_share: optional(ask, "best_ask")?.filter(|value| value.0 != "1"),
        spread_pusd_per_share: optional(spread, "spread")?,
    })
}

pub(crate) fn payload_from_typed_raw(
    row: &PolymarketOrderbookRow,
    assets: &[crate::reduction::PolymarketAssetContextV1],
) -> Result<CanonicalPmPayloadV1, ContractError> {
    let asset = |id: &str| {
        assets
            .iter()
            .find(|value| value.asset.token_id.0 == id)
            .map(|value| value.asset.clone())
            .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))
    };
    Ok(match &row.event {
        Raw::Book {
            asset_id,
            bids,
            asks,
            last_trade_price,
        } => {
            let levels = |values: &[typed_raw::PolymarketPriceLevel]| {
                values
                    .iter()
                    .map(|value| {
                        Ok(PriceLevelV1 {
                            price_pusd_per_share: decimal(&value.price, "book.price")?,
                            quantity_shares: decimal(&value.size, "book.size")?,
                        })
                    })
                    .collect::<Result<Vec<_>, ContractError>>()
            };
            CanonicalPmPayloadV1::Book {
                fact: CanonicalBookFactV1::FullBook {
                    asset: asset(asset_id)?,
                    bids: levels(bids)?,
                    asks: levels(asks)?,
                    last_trade_price_pusd_per_share: optional(
                        last_trade_price,
                        "last_trade_price",
                    )?,
                },
            }
        }
        Raw::PriceChange { price_changes } => CanonicalPmPayloadV1::Book {
            fact: CanonicalBookFactV1::AbsoluteLevelChanges {
                changes: price_changes
                    .iter()
                    .map(|value| {
                        Ok(CanonicalAbsoluteLevelChangeV1 {
                            asset: asset(&value.asset_id)?,
                            side: side(value.side),
                            price_pusd_per_share: decimal(&value.price, "price_change.price")?,
                            quantity_shares: optional(&value.size, "price_change.size")?,
                            best_bid_ask: if value.best_bid.is_some() || value.best_ask.is_some() {
                                Some(quote(&value.best_bid, &value.best_ask, &None)?)
                            } else {
                                None
                            },
                        })
                    })
                    .collect::<Result<Vec<_>, ContractError>>()?,
            },
        },
        Raw::BestBidAsk {
            asset_id,
            best_bid,
            best_ask,
            spread,
        } => CanonicalPmPayloadV1::BestBidAsk {
            asset: asset(asset_id)?,
            quote: quote(best_bid, best_ask, spread)?,
        },
        Raw::TickSizeChange {
            asset_id,
            old_tick_size,
            new_tick_size,
        } => CanonicalPmPayloadV1::TickSizeChange {
            asset: asset(asset_id)?,
            old_tick_size_pusd_per_share: optional(old_tick_size, "old_tick_size")?,
            new_tick_size_pusd_per_share: decimal(new_tick_size, "new_tick_size")?,
        },
        Raw::LastTradePrice {
            asset_id,
            price,
            size,
            side: direction,
            fee_rate_bps,
        } => CanonicalPmPayloadV1::PublicTrade {
            asset: asset(asset_id)?,
            price_pusd_per_share: decimal(price, "last_trade_price.price")?,
            quantity_shares: optional(size, "last_trade_price.size")?,
            side: direction.map(side),
            fee_rate_bps: optional(fee_rate_bps, "fee_rate_bps")?,
        },
        Raw::NewMarket {
            clob_market_id,
            question,
            description,
            asset_ids,
            outcomes,
            event_message,
        } => CanonicalPmPayloadV1::NewMarket {
            clob_market_id: clob_market_id.clone(),
            question: question.clone(),
            description: description.clone(),
            asset_ids: asset_ids.clone(),
            outcomes: outcomes.clone(),
            event_message: event_message
                .as_ref()
                .map(|value| CanonicalPmEventDescriptionV1 {
                    id: value.id.clone(),
                    ticker: value.ticker.clone(),
                    slug: value.slug.clone(),
                    title: value.title.clone(),
                    description: value.description.clone(),
                }),
        },
        Raw::MarketResolved { .. } => {
            return Err(invalid(
                "event",
                "market_resolved WebSocket rows require the qualified terminal merger",
            ));
        }
    })
}
