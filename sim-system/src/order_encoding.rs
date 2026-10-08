//! 原CPU接单与GPU控制面共用的纯SDK编码；不选择pending或撮合、分配身份。
use crate::execution::{PostOrderItemV1, SimBuyOrderKindV1, limit_buy_encoding, market_buy_encoding};
use crate::matching::{BuyOrderTermsV2, MatchingStateErrorV1, micro_units_v1};
use contracts::SubmissionRejectedReasonV2;

pub(crate) fn post_terms_v2(
    item: &PostOrderItemV1,
    normalized_gtd_expires_at_utc_ms: Option<Option<i64>>,
    current_tick_size_pusd_per_share: rust_decimal::Decimal,
    minimum_shares: rust_decimal::Decimal,
) -> Result<Result<BuyOrderTermsV2, SubmissionRejectedReasonV2>, MatchingStateErrorV1> {
    let terms = match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => {
            let Some(encoding) = limit_buy_encoding(
                *limit_price_pusd_per_share,
                *size_shares,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if encoding.taker_amount < minimum_shares {
                return Ok(Err(SubmissionRejectedReasonV2::BelowMinimumShares));
            }
            BuyOrderTermsV2::Gtc {
                limit_price_pusd_per_share: encoding.price,
                accepted_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
                accepted_quote_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
                post_only: *post_only,
            }
        }
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            ..
        } => {
            let Some(encoding) = limit_buy_encoding(
                *limit_price_pusd_per_share,
                *size_shares,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if encoding.taker_amount < minimum_shares {
                return Ok(Err(SubmissionRejectedReasonV2::BelowMinimumShares));
            }
            BuyOrderTermsV2::Gtd {
                limit_price_pusd_per_share: encoding.price,
                accepted_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
                accepted_quote_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
                post_only: *post_only,
                expires_at_utc_ms: normalized_gtd_expires_at_utc_ms.flatten(),
            }
        }
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            let Some(encoding) = market_buy_encoding(
                *worst_price_pusd_per_share,
                *spend_pusd,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if *spend_pusd < crate::platform_rules::MARKET_BUY_MINIMUM_PRINCIPAL_PUSD {
                return Ok(Err(
                    SubmissionRejectedReasonV2::BelowMarketBuyMinimumPrincipal,
                ));
            }
            BuyOrderTermsV2::Fok {
                worst_price_pusd_per_share: encoding.price,
                accepted_quote_budget_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
                sdk_taker_amount_audit_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
            }
        }
        SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            let Some(encoding) = market_buy_encoding(
                *worst_price_pusd_per_share,
                *spend_pusd,
                current_tick_size_pusd_per_share,
            ) else {
                return Ok(Err(SubmissionRejectedReasonV2::OrderEncodingRejected));
            };
            if *spend_pusd < crate::platform_rules::MARKET_BUY_MINIMUM_PRINCIPAL_PUSD {
                return Ok(Err(
                    SubmissionRejectedReasonV2::BelowMarketBuyMinimumPrincipal,
                ));
            }
            BuyOrderTermsV2::Fak {
                worst_price_pusd_per_share: encoding.price,
                accepted_quote_budget_units: micro_units_v1(encoding.maker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
                sdk_taker_amount_audit_base_units: micro_units_v1(encoding.taker_amount)
                    .map_err(MatchingStateErrorV1::from)?,
            }
        }
    };
    Ok(Ok(terms))
}

/// 批次边界编码原请求，ticks必须取各项实际处理时hidden状态。
/// 输出保持原terms和RejectedReason，不把SDK audit base当QuoteBudget目标。
pub fn post_terms_batch_v2(input: &str) -> Result<String, String> {
    #[derive(serde::Deserialize)]
    struct Entry {
        item: PostOrderItemV1,
        tick: rust_decimal::Decimal,
        normalized_gtd: Option<Option<i64>>,
    }
    #[derive(serde::Deserialize)]
    struct Batch {
        entries: Vec<Entry>,
        minimum_shares: rust_decimal::Decimal,
    }
    let batch: Batch = serde_json::from_str(input).map_err(|e|e.to_string())?;
    let output=batch.entries.iter().map(|entry|post_terms_v2(&entry.item,entry.normalized_gtd,
        entry.tick,batch.minimum_shares)).collect::<Result<Vec<_>,_>>().map_err(|e|format!("{e:?}"))?;
    serde_json::to_string(&output).map_err(|e|e.to_string())
}
