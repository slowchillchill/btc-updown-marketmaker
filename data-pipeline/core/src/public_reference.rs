//! 对已采集参考事实做纯规范化；不维护市场状态或触发模型。

use contracts::{
    BtcSettlementReferenceV1, CANONICAL_REFERENCE_EVENT_SCHEMA_V2, CanonicalEventId,
    CanonicalReferenceEventPayloadV2, CanonicalReferenceEventV2, CanonicalSourceKindV2,
    ChainlinkBtcUsdTwapReferenceV1, CollectorAdmissionCoordinateV1, EventLineageV3, EventQualityV2,
    EventTimeV2, ExactDecimal, MarketAnchorSourceV1, MarketDataQuality, MarketIdentityV2,
    MarketPriceAnchorUpdateV1, MarketPriceAnchorV1, MarketSettlementReferenceUpdateV1,
    PriceAnchorApplicationV1, ResyncQuality, RtdsObservedAnomalyClassificationV1,
    RtdsObservedAnomalyV1, Sha256Hex, SourceEvidenceRefV2, SourceEvidenceRefV3,
    SourceNativeIdentityV1,
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::public_input::{
    HistoricalPriceAnchorRepairBatchInput, HttpResponseEvidenceV1, MarketAnchorOriginV1,
    MarketAnchorQueryResultV1, MarketAnchorRuleUpdateInput, PolymarketRtdsChainlinkTwapInput,
};
use crate::typed_raw::{RtdsFreshnessStatus, TypedRawError};
use crate::{ContractError, hash_domain_bytes, stable_json};

fn decimal_error(field: &str, reason: impl ToString) -> TypedRawError {
    TypedRawError::InvalidField {
        field: field.to_owned(),
        reason: reason.to_string(),
    }
}

/// 原词法可含指数；有效整数和 scale 仍受现有 Decimal 精确范围约束。
pub fn normalize_price_decimal_lexical_v1(
    raw: &str,
    field: &str,
) -> Result<ExactDecimal, TypedRawError> {
    normalize_decimal_parts(raw, 0, field)
}

/// 将原 E18 整数文本精确移位，不使用消息里的显示价格。
pub fn normalize_e18_price_decimal_v1(
    raw: &str,
    field: &str,
) -> Result<ExactDecimal, TypedRawError> {
    let unsigned = raw.strip_prefix('-').unwrap_or(raw);
    if unsigned.is_empty() || !unsigned.bytes().all(|value| value.is_ascii_digit()) {
        return Err(decimal_error(field, "必须为 E18 十进制整数文本"));
    }
    normalize_decimal_parts(raw, 18, field)
}

fn normalize_decimal_parts(
    raw: &str,
    additional_scale: i128,
    field: &str,
) -> Result<ExactDecimal, TypedRawError> {
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, raw),
    };
    let mut exponent_parts = unsigned.split(['e', 'E']);
    let mantissa = exponent_parts.next().unwrap_or_default();
    let exponent = exponent_parts.next();
    if exponent_parts.next().is_some() {
        return Err(decimal_error(field, "十进制文本只能包含一个指数"));
    }
    let mut parts = mantissa.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|value| value.is_ascii_digit())
        || fraction.is_some_and(|value| {
            value.is_empty() || !value.bytes().all(|value| value.is_ascii_digit())
        })
    {
        return Err(decimal_error(field, "必须包含十进制数字和可选指数"));
    }
    if let Some(exponent) = exponent {
        let unsigned = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
        if unsigned.is_empty() || !unsigned.bytes().all(|value| value.is_ascii_digit()) {
            return Err(decimal_error(field, "指数必须为整数文本"));
        }
    }
    let digits = format!("{integer}{}", fraction.unwrap_or_default());
    let nonzero = digits.trim_start_matches('0');
    if nonzero.is_empty() {
        return Ok(ExactDecimal("0".to_owned()));
    }
    let significant = nonzero.trim_end_matches('0');
    let trailing_zeroes = nonzero.len() - significant.len();
    let exponent = exponent
        .map(str::parse::<i128>)
        .transpose()
        .map_err(|error| decimal_error(field, error))?
        .unwrap_or(0);
    let scale = i128::try_from(fraction.map_or(0, str::len))
        .ok()
        .and_then(|scale| scale.checked_add(additional_scale))
        .and_then(|scale| scale.checked_sub(exponent))
        .and_then(|scale| scale.checked_sub(trailing_zeroes as i128))
        .ok_or_else(|| decimal_error(field, "十进制 scale 超出支持范围"))?;
    // 先确定 Decimal 的固有表示范围，再展开指数；不按指数分配无界字符串。
    if significant.len() > 29 || scale > 28 || scale < -28 {
        return Err(decimal_error(field, "数值超出 Decimal 的精确表示范围"));
    }
    let mut canonical = String::new();
    if negative {
        canonical.push('-');
    }
    if scale <= 0 {
        let zeros = usize::try_from(-scale).unwrap();
        if significant.len() + zeros > 29 {
            return Err(decimal_error(field, "数值超出 Decimal 的精确表示范围"));
        }
        canonical.push_str(significant);
        canonical.extend(std::iter::repeat_n('0', zeros));
    } else {
        let scale = usize::try_from(scale).unwrap();
        if scale >= significant.len() {
            canonical.push_str("0.");
            canonical.extend(std::iter::repeat_n('0', scale - significant.len()));
            canonical.push_str(significant);
        } else {
            let split = significant.len() - scale;
            canonical.push_str(&significant[..split]);
            canonical.push('.');
            canonical.push_str(&significant[split..]);
        }
    }
    let value = Decimal::from_str_exact(&canonical).map_err(|error| decimal_error(field, error))?;
    Ok(ExactDecimal(value.normalize().to_string()))
}

fn finalize_reference(
    source: &SourceEvidenceRefV3,
    canonical_rule_version: &str,
    availability: MarketDataQuality,
    payload: CanonicalReferenceEventPayloadV2,
) -> Result<CanonicalReferenceEventV2, ContractError> {
    #[derive(Serialize)]
    struct Identity<'a> {
        schema_version: &'a str,
        time: &'a EventTimeV2,
        lineage: &'a EventLineageV3,
        quality: &'a EventQualityV2,
        payload: &'a CanonicalReferenceEventPayloadV2,
    }
    let time = EventTimeV2 {
        source_event_at_utc_ms: source.source_event_at_utc_ms,
        received_at_utc_ms: source.received_at_utc_ms,
    };
    let lineage = EventLineageV3 {
        canonical_rule_version: canonical_rule_version.to_owned(),
        source_evidence: vec![source.clone()],
    };
    let quality = EventQualityV2 {
        availability,
        resync: ResyncQuality::NotRequired,
    };
    let identity = Identity {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V2,
        time: &time,
        lineage: &lineage,
        quality: &quality,
        payload: &payload,
    };
    let event_id = CanonicalEventId(
        hash_domain_bytes(
            b"data-pipeline/canonical-event-id/reference-event/v2\0",
            &stable_json(&identity)?,
        )
        .0,
    );
    Ok(CanonicalReferenceEventV2 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V2.to_owned(),
        event_id,
        time,
        lineage,
        quality,
        payload,
    })
}

/// 一份实际响应可以产生锚点及规则两个成员；封组仍由唯一共享核心负责。
pub fn canonicalize_market_reference_response_v1(
    row: &MarketAnchorRuleUpdateInput,
    market: &MarketIdentityV2,
    source: &SourceEvidenceRefV3,
    canonical_rule_version: &str,
) -> Result<Vec<CanonicalReferenceEventV2>, ContractError> {
    if row.anchor_query_result != MarketAnchorQueryResultV1::Observed {
        return Ok(Vec::new());
    }
    let mut events = Vec::new();
    let anchor_source = match row.price_to_beat_source_field.as_deref() {
        Some("eventMetadata.priceToBeat") => {
            Some(MarketAnchorSourceV1::GammaEventMetadataPriceToBeat)
        }
        Some("openPrice") => Some(MarketAnchorSourceV1::OfficialCryptoPriceOpenPrice),
        _ => None,
    };
    if let (Some(raw), Some(anchor_source)) = (&row.price_to_beat_decimal, anchor_source) {
        let price_usd_per_btc = normalize_price_decimal_lexical_v1(raw, "price_to_beat_decimal")
            .map_err(crate::typed_raw_contract_error)?;
        events.push(finalize_reference(
            source,
            canonical_rule_version,
            MarketDataQuality::Fresh,
            CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(MarketPriceAnchorUpdateV1 {
                market: market.clone(),
                anchor: MarketPriceAnchorV1 {
                    price_usd_per_btc,
                    source: anchor_source,
                    application: PriceAnchorApplicationV1::Observed,
                },
            }),
        )?);
    }
    // 官网参数只是请求依据，不能成为官网发布的规则。
    let reference = if row.metadata_origin == MarketAnchorOriginV1::GammaEventBySlug {
        match (row.twap_enabled, row.twap_lookback_seconds) {
            (Some(false), _) => Some(BtcSettlementReferenceV1::ChainlinkSpot),
            (Some(true), Some(30)) => Some(BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds),
            (Some(true), Some(60)) => Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds),
            _ => None,
        }
    } else {
        None
    };
    if let Some(reference) = reference {
        events.push(finalize_reference(
            source,
            canonical_rule_version,
            MarketDataQuality::Fresh,
            CanonicalReferenceEventPayloadV2::MarketSettlementReferenceUpdate(
                MarketSettlementReferenceUpdateV1 {
                    market: market.clone(),
                    reference,
                },
            ),
        )?);
    }
    Ok(events)
}

pub fn canonicalize_chainlink_twap_v1(
    row: &PolymarketRtdsChainlinkTwapInput,
    source: &SourceEvidenceRefV3,
    canonical_rule_version: &str,
) -> Result<CanonicalReferenceEventV2, ContractError> {
    let common = &row.common;
    let anomaly = match common.freshness_status {
        RtdsFreshnessStatus::InvalidType => Some(RtdsObservedAnomalyClassificationV1::InvalidType),
        RtdsFreshnessStatus::InvalidTopic => {
            Some(RtdsObservedAnomalyClassificationV1::InvalidTopic)
        }
        RtdsFreshnessStatus::InvalidSymbol => {
            Some(RtdsObservedAnomalyClassificationV1::InvalidSymbol)
        }
        RtdsFreshnessStatus::InvalidTimestamp => {
            Some(RtdsObservedAnomalyClassificationV1::InvalidTimestamp)
        }
        RtdsFreshnessStatus::Fresh | RtdsFreshnessStatus::Stale => None,
    };
    let (availability, payload) = if let Some(classification) = anomaly {
        (
            MarketDataQuality::Invalid,
            CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(RtdsObservedAnomalyV1 {
                classification,
                observed_topic: common.topic.clone(),
                observed_message_type: common.message_type.clone(),
                observed_symbol: common.symbol.clone(),
                observed_value_raw: common.value.clone(),
                observed_message_timestamp_raw: common.message_timestamp,
                observed_payload_timestamp_raw: common.payload_timestamp,
            }),
        )
    } else {
        let price_usd_per_btc =
            normalize_e18_price_decimal_v1(&row.full_accuracy_value, "full_accuracy_value")
                .map_err(crate::typed_raw_contract_error)?;
        (
            if common.freshness_status == RtdsFreshnessStatus::Fresh {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::Stale
            },
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(ChainlinkBtcUsdTwapReferenceV1 {
                symbol: common.symbol.clone(),
                window_s: row.window_s,
                price_usd_per_btc,
                payload_timestamp_utc_ms: common.payload_timestamp,
                message_timestamp_utc_ms: common.message_timestamp,
            }),
        )
    };
    finalize_reference(source, canonical_rule_version, availability, payload)
}

/// 旧事实使用已有语义来源映射；物理摘要在新版来源信封中退出。
pub fn source_evidence_v3_from_existing(
    source: &SourceEvidenceRefV2,
) -> Result<SourceEvidenceRefV3, ContractError> {
    use contracts::CanonicalSourceKind as Old;
    let source_kind = match source.source_kind {
        Old::BinanceAggtrade => CanonicalSourceKindV2::BinanceAggtrade,
        Old::BinanceDepthSnapshot => CanonicalSourceKindV2::BinanceDepthSnapshot,
        Old::BinanceDepthUpdate => CanonicalSourceKindV2::BinanceDepthUpdate,
        Old::PolymarketMarketMetadata => CanonicalSourceKindV2::PolymarketMarketMetadata,
        Old::PolymarketOrderbook => CanonicalSourceKindV2::PolymarketOrderbook,
        Old::PolymarketRtdsChainlinkBtcUsd => CanonicalSourceKindV2::PolymarketRtdsChainlinkBtcUsd,
    };
    let identity = (
        source_kind,
        &source.source_native_identity,
        source.source_event_at_utc_ms,
        source.received_at_utc_ms,
        &source.collector_admission,
    );
    Ok(SourceEvidenceRefV3 {
        source_kind,
        source_message_id: hash_domain_bytes(
            b"data-pipeline/source-message/v1\0",
            &stable_json(&identity)?,
        ),
        source_native_identity: source.source_native_identity.clone(),
        source_event_at_utc_ms: source.source_event_at_utc_ms,
        received_at_utc_ms: source.received_at_utc_ms,
        collector_admission: source.collector_admission.clone(),
    })
}

pub fn historical_response_source_evidence_v3(
    response: &HttpResponseEvidenceV1,
) -> Result<SourceEvidenceRefV3, ContractError> {
    let identity = [
        "historical_price_anchor_http_response_v1",
        &response.origin,
        &response.request_url,
        &response.received_at_utc,
        &response.response_body_sha256,
    ];
    use sha2::{Digest, Sha256};
    Ok(SourceEvidenceRefV3 {
        source_kind: CanonicalSourceKindV2::PolymarketMarketMetadata,
        source_message_id: Sha256Hex(format!("{:x}", Sha256::digest(stable_json(&identity)?))),
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: Sha256Hex(response.response_body_sha256.clone()),
        }),
        source_event_at_utc_ms: None,
        received_at_utc_ms: response.received_at_utc_ms,
        collector_admission: None,
    })
}

/// 新查询使用实际响应与实际准入坐标；没有完整响应的失败仍保留为零成员输入。
pub fn market_anchor_source_evidence_v3(
    row: &MarketAnchorRuleUpdateInput,
) -> Result<SourceEvidenceRefV3, ContractError> {
    let admission = CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: row.anchor_query_source_epoch_id.clone(),
        collector_admission_sequence: row.collector_admission_sequence,
        admitted_at_utc_ms: row.collector_admission_time_utc_ms,
    };
    let native = row.http_body_sha256.as_ref().map(|digest| {
        SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: Sha256Hex(digest.clone()),
        }
    });
    Ok(SourceEvidenceRefV3 {
        source_kind: CanonicalSourceKindV2::PolymarketMarketMetadata,
        source_message_id: hash_domain_bytes(
            b"data-pipeline/source-message/market-anchor/v1\0",
            &stable_json(&(
                row.metadata_origin,
                &row.http_request_url,
                &native,
                row.timestamp_received,
                row.anchor_query_ordinal,
                &admission,
            ))?,
        ),
        source_native_identity: native,
        source_event_at_utc_ms: row.anchor_source_computed_at_utc,
        received_at_utc_ms: row.timestamp_received,
        collector_admission: Some(admission),
    })
}

/// 有限批次的纯规范化；同一响应的所有成员共享一个真实来源。
pub fn canonicalize_historical_price_anchor_batch_v1(
    batch: &HistoricalPriceAnchorRepairBatchInput,
    identity_sources: &std::collections::BTreeMap<String, SourceEvidenceRefV3>,
    canonical_rule_version: &str,
) -> Result<Vec<CanonicalReferenceEventV2>, ContractError> {
    let source = historical_response_source_evidence_v3(&batch.response)?;
    let mut events = Vec::with_capacity(batch.rows.len());
    for row in &batch.rows {
        let ids: Vec<String> = serde_json::from_str(&row.historical_identity_evidence_ids_json)
            .map_err(|error| {
                crate::typed_raw_contract_error(decimal_error(
                    "historical_identity_evidence_ids_json",
                    error,
                ))
            })?;
        let evidence = ids
            .iter()
            .map(|id| {
                identity_sources.get(id).cloned().ok_or_else(|| {
                    crate::typed_raw_contract_error(decimal_error(
                        "historical_identity_evidence_ids_json",
                        format!("缺少原身份来源 {id}"),
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        events.push(finalize_reference(
            &source,
            canonical_rule_version,
            MarketDataQuality::Fresh,
            CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(MarketPriceAnchorUpdateV1 {
                market: MarketIdentityV2 {
                    condition_id: contracts::ConditionId(row.condition_id.clone()),
                    clob_market_id: contracts::ClobMarketId(row.clob_market_id.clone()),
                    window: contracts::MarketWindowV2 {
                        start_at_utc_ms: row.market_start_at_utc_ms,
                        end_at_utc_ms: row.market_end_at_utc_ms,
                    },
                },
                anchor: MarketPriceAnchorV1 {
                    price_usd_per_btc: normalize_price_decimal_lexical_v1(
                        &row.price_to_beat_raw,
                        "price_to_beat_raw",
                    )
                    .map_err(crate::typed_raw_contract_error)?,
                    source: row.anchor_source,
                    application: PriceAnchorApplicationV1::HistoricalRepair {
                        policy_version: row.repair_policy_version.clone(),
                        historical_identity_ready_at_utc_ms: row
                            .historical_identity_ready_at_utc_ms,
                        effective_from_utc_ms: row
                            .market_start_at_utc_ms
                            .max(row.historical_identity_ready_at_utc_ms),
                        historical_identity_evidence: evidence,
                    },
                },
            }),
        )?);
    }
    Ok(events)
}

pub fn twap_source_evidence_v3(
    row: &PolymarketRtdsChainlinkTwapInput,
) -> Result<SourceEvidenceRefV3, ContractError> {
    let common = &row.common;
    let admission = CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: row.source_sequence_epoch_id.clone(),
        collector_admission_sequence: row.collector_admission_sequence,
        admitted_at_utc_ms: row.collector_admission_time_utc_ms,
    };
    let native = hash_domain_bytes(
        b"data-pipeline/source-evidence/rtds-native-payload/v1\0",
        common.source_native_payload_json.as_bytes(),
    );
    Ok(SourceEvidenceRefV3 {
        source_kind: CanonicalSourceKindV2::PolymarketRtdsChainlinkTwap,
        source_message_id: hash_domain_bytes(
            b"data-pipeline/source-message/rtds-twap/v1\0",
            &stable_json(&(&native, common.timestamp_received, &admission))?,
        ),
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: native,
        }),
        source_event_at_utc_ms: (common.freshness_status != RtdsFreshnessStatus::InvalidTimestamp)
            .then_some(common.payload_timestamp),
        received_at_utc_ms: common.timestamp_received,
        collector_admission: Some(admission),
    })
}
