//! 新版公共输入的类型边界；这里只携带事实，不维护观测或市场归约状态。

use std::collections::BTreeMap;

use contracts::MarketAnchorSourceV1;
use serde::{Deserialize, Serialize};

use crate::typed_raw::{
    PolymarketMarketMetadataRow, PolymarketRtdsChainlinkBtcUsdRow, TypedRawRow,
};

pub const POLYMARKET_MARKET_METADATA_SCHEMA_V6: &str = "polymarket_market_metadata_v6";
pub const POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1: &str = "polymarket_rtds_chainlink_twap_v1";
pub const HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1: &str = "historical_price_anchor_repair_v1";
pub const HISTORICAL_PRICE_ANCHOR_REPAIR_MANIFEST_SCHEMA_V1: &str =
    "historical_price_anchor_repair_manifest_v1";

pub const DATA_COLLECT_HANDOFF_SCHEMA_V2: &str = "data_collect_handoff_v2";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataCollectHandoffSessionV2 {
    pub frame_schema_version: String,
    pub environment: String,
    pub run_id: String,
    pub git_commit: String,
    pub binary_sha256: String,
    pub config_digest: String,
    pub collector_host_id: String,
    pub collector_region: String,
}

/// 传输确认只表示当前对象消费并释放，不表示 DPT 应用或持久提交。
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataCollectHandoffV2 {
    Session(DataCollectHandoffSessionV2),
    Message {
        transport_sequence: u64,
        row: TypedRawRowV2,
    },
    Received {
        transport_sequence: u64,
    },
    End {
        last_confirmed_sequence: u64,
    },
    Aborted {
        last_confirmed_sequence: u64,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypedRawRowV2 {
    Existing(TypedRawRow),
    PolymarketMarketMetadataV6(Box<PolymarketMarketMetadataV6Input>),
    PolymarketRtdsChainlinkTwap(PolymarketRtdsChainlinkTwapInput),
    HistoricalPriceAnchorRepairBatch(HistoricalPriceAnchorRepairBatchInput),
}

/// `Existing` 复用 V5 的业务表达；外层 V6 版本保留真实持久格式归属。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolymarketMarketMetadataV6Input {
    Existing(PolymarketMarketMetadataRow),
    AnchorRule(MarketAnchorRuleUpdateInput),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorOriginV1 {
    GammaEventBySlug,
    WebsiteCryptoPrice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorQueryResultV1 {
    Observed,
    HttpError,
    DecodeError,
    TransportError,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnchorRuleUpdateInput {
    pub schema_version: String,
    pub timestamp_received: i64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub metadata_origin: MarketAnchorOriginV1,
    pub collector_host_id: String,
    pub collector_region: String,
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub asset_ids: String,
    pub market_start_utc: i64,
    pub market_end_utc: i64,
    pub fixed_identity_verified_at_utc: i64,
    pub market_lifecycle_state: String,
    pub market_runtime_role: String,
    pub source_endpoint: Option<String>,
    pub metadata_fetch_start_utc: i64,
    pub anchor_query_source_epoch_id: String,
    pub anchor_query_ordinal: u64,
    pub anchor_query_result: MarketAnchorQueryResultV1,
    pub anchor_event_id: Option<String>,
    pub price_to_beat_decimal: Option<String>,
    pub price_to_beat_source_field: Option<String>,
    pub anchor_source_computed_at_utc: Option<i64>,
    pub crypto_market_config_id: Option<String>,
    pub crypto_market_config_nested_id: Option<String>,
    pub crypto_market_config_asset: Option<String>,
    pub crypto_market_config_duration: Option<String>,
    pub twap_enabled: Option<bool>,
    pub twap_lookback_seconds: Option<u32>,
    pub resolution_source: Option<String>,
    pub http_request_url: Option<String>,
    pub http_response_url: Option<String>,
    pub http_status_code: Option<u16>,
    pub http_received_at_utc: Option<i64>,
    pub http_body_encoding: Option<String>,
    pub http_body: Option<String>,
    pub http_body_sha256: Option<String>,
}

/// `common` 只复用同名原字段；它不是一条可以按现货 v4 验证的行情。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketRtdsChainlinkTwapInput {
    pub common: PolymarketRtdsChainlinkBtcUsdRow,
    pub full_accuracy_value: String,
    pub window_s: u64,
    pub source_sequence_epoch_id: String,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPriceAnchorRepairRowV1 {
    pub schema_version: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub slug: String,
    pub asset_ids_json: String,
    pub market_start_at_utc_ms: i64,
    pub market_end_at_utc_ms: i64,
    pub anchor_source: MarketAnchorSourceV1,
    pub price_to_beat_raw: String,
    pub response_group_id: String,
    pub response_member_index: u32,
    pub response_received_at_utc_ms: i64,
    pub response_sha256: String,
    pub source_value_path: String,
    pub source_event_at_utc_ms: Option<i64>,
    pub historical_identity_ready_at_utc_ms: i64,
    pub historical_identity_evidence_ids_json: String,
    pub repair_policy_version: String,
}

/// 原 HTTP 实体的采集证据；旧 curl 未记录的状态和最终 URL 保持空值。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponseEvidenceV1 {
    pub origin: String,
    pub request_url: String,
    pub received_at_utc: String,
    pub received_at_utc_ms: i64,
    pub receipt_observation: String,
    pub response_body_sha256: String,
    pub response_body_path: String,
    pub original_response_file: String,
    pub http_status_code: Option<u16>,
    pub response_url: Option<String>,
}

/// 物理行定位只用于找回原身份证据，不能作为来源消息身份或采集代际。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalIdentityRowEvidenceV1 {
    pub manifest_path: String,
    pub manifest_sha256: String,
    pub parquet_path: String,
    pub parquet_sha256: String,
    pub row_index: u64,
    pub schema_version: String,
    pub metadata_row_kind: String,
    pub row_received_at_utc_ms: i64,
    pub fixed_identity_verified_at_utc_ms: i64,
    pub source_evidence_origin: Option<String>,
    pub source_endpoint: Option<String>,
    pub source_evidence_received_at_utc_ms: Option<i64>,
    pub source_evidence_raw_sha256: Option<String>,
    pub collector_admission_sequence: Option<u64>,
    pub collector_admission_time_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPriceAnchorRepairBatchInput {
    pub response_group_id: String,
    pub response: HttpResponseEvidenceV1,
    pub rows: Vec<HistoricalPriceAnchorRepairRowV1>,
    pub identity_evidence: BTreeMap<String, HistoricalIdentityRowEvidenceV1>,
}

/// 唯一接纳序列的记录表达；准备材料不成为第二份耐久输入。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DptIngressV1 {
    Message {
        row: TypedRawRowV2,
        source_evidence: contracts::SourceEvidenceRefV3,
    },
    EndOfInput,
    Aborted {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedIngressV1 {
    pub input_stream_id: String,
    pub input_position: u64,
    pub ingress: DptIngressV1,
    pub dependency_record_refs: Vec<contracts::CanonicalInputRecordRefV1>,
}

/// 已完成原 JSON 映射的元数据；真实前序比较仍由原市场归约器完成。
pub struct PreparedMetadataV1<'a> {
    pub row: &'a PolymarketMarketMetadataRow,
    pub source: crate::pm_reduction::PreparedPmEvidenceV1<contracts::SourceEvidenceRefV3>,
    pub outcomes: Vec<crate::typed_raw::MetadataOutcomeEvidence>,
    pub initial_tick: Option<crate::typed_raw::MetadataInitialTickEvidence>,
}

/// 有状态深度、成交和终态只准备输入，不能在此伪造已经归约的事件。
pub enum PreparedCanonicalInputV1<'a> {
    PolymarketMessage(crate::pm_reduction::PreparedPmMessageV1),
    PolymarketWsTerminal {
        row: &'a crate::typed_raw::PolymarketOrderbookRow,
        source: crate::pm_reduction::PreparedPmEvidenceV1<contracts::SourceEvidenceRefV3>,
    },
    Metadata(PreparedMetadataV1<'a>),
    AnchorRule {
        input: &'a MarketAnchorRuleUpdateInput,
        facts: Vec<contracts::CanonicalReferenceEventV2>,
    },
    BinanceDepthSnapshot {
        row: &'a crate::typed_raw::BinanceDepthSnapshotRow,
        source: crate::pm_reduction::PreparedPmEvidenceV1<contracts::SourceEvidenceRefV3>,
    },
    BinanceDepthUpdate {
        row: &'a crate::typed_raw::BinanceDepthUpdateRow,
        source: crate::pm_reduction::PreparedPmEvidenceV1<contracts::SourceEvidenceRefV3>,
    },
    BinanceAggregateTrade {
        row: &'a crate::typed_raw::BinanceAggTradeRow,
        source: crate::pm_reduction::PreparedPmEvidenceV1<contracts::SourceEvidenceRefV3>,
        payload: Option<contracts::BinanceAggregateTradeReferenceV1>,
    },
    ChainlinkSpot(contracts::CanonicalReferenceEventV2),
    ChainlinkTwap(contracts::CanonicalReferenceEventV2),
    HistoricalAnchorRepair {
        batch: &'a HistoricalPriceAnchorRepairBatchInput,
        facts: Vec<contracts::CanonicalReferenceEventV2>,
    },
    EndOfInput,
    Aborted {
        reason: &'a str,
    },
}

/// 借用唯一原输入；小量坐标/引用供本次调用使用，不复制原消息。
#[derive(Serialize)]
pub struct RecordedIngressViewV1<'a> {
    pub input_stream_id: String,
    pub input_position: u64,
    pub ingress: &'a DptIngressV1,
    pub dependency_record_refs: Vec<contracts::CanonicalInputRecordRefV1>,
}
pub struct PreparedIngressV1<'a> {
    pub recorded: RecordedIngressViewV1<'a>,
    pub current: PreparedCanonicalInputV1<'a>,
    pub prepared_group_id: Option<contracts::Sha256Hex>,
    pub dependencies: std::borrow::Cow<'a, [contracts::CanonicalReferenceEventV2]>,
    pub reference_selection: Option<contracts::PreparedReferenceSelectionV1>,
    pub reference_selection_market: Option<contracts::MarketIdentityV2>,
    pub pm_source_identity: Option<std::sync::Arc<crate::pm_reduction::PreparedPmSourceIdentityV1>>,
}

/// 来源所属方调用；此处可以编码，返回后业务入口只消费已有类型和规范键。
pub fn prepare_ingress_v1<'a>(
    recorded: &'a RecordedIngressV1,
    pm_context: Option<&crate::pm_reduction::PmMarketContextV1>,
    identity_sources: &BTreeMap<String, contracts::SourceEvidenceRefV3>,
    dependencies: &'a [contracts::CanonicalReferenceEventV2],
    canonical_rule_version: &str,
) -> Result<PreparedIngressV1<'a>, crate::ContractError> {
    prepare_ingress_view_v1(
        RecordedIngressViewV1 {
            input_stream_id: recorded.input_stream_id.clone(),
            input_position: recorded.input_position,
            ingress: &recorded.ingress,
            dependency_record_refs: recorded.dependency_record_refs.clone(),
        },
        pm_context,
        identity_sources,
        dependencies,
        canonical_rule_version,
    )
}
fn prepare_ingress_view_v1<'a>(
    recorded: RecordedIngressViewV1<'a>,
    pm_context: Option<&crate::pm_reduction::PmMarketContextV1>,
    identity_sources: &BTreeMap<String, contracts::SourceEvidenceRefV3>,
    dependencies: &'a [contracts::CanonicalReferenceEventV2],
    canonical_rule_version: &str,
) -> Result<PreparedIngressV1<'a>, crate::ContractError> {
    use crate::pm_reduction::{PreparedPmEvidenceV1, PreparedPmMessageV1};
    use crate::public_reference::*;
    use crate::typed_raw::{BinanceAggTradeRecord, PolymarketOrderbookEvent};
    use contracts::*;
    let (row, source) = match &recorded.ingress {
        DptIngressV1::Message {
            row,
            source_evidence,
        } => (row, source_evidence),
        DptIngressV1::EndOfInput => {
            return Ok(PreparedIngressV1 {
                recorded,
                current: PreparedCanonicalInputV1::EndOfInput,
                prepared_group_id: None,
                dependencies: std::borrow::Cow::Borrowed(dependencies),
                reference_selection: None,
                reference_selection_market: None,
                pm_source_identity: None,
            });
        }
        DptIngressV1::Aborted { reason } => {
            return Ok(PreparedIngressV1 {
                recorded,
                current: PreparedCanonicalInputV1::Aborted { reason },
                prepared_group_id: None,
                dependencies: std::borrow::Cow::Borrowed(dependencies),
                reference_selection: None,
                reference_selection_market: None,
                pm_source_identity: None,
            });
        }
    };
    let prepared_source = || PreparedPmEvidenceV1::prepare(source.clone(), None);
    let mut prepared_group_id = Some(source.source_message_id.clone());
    let metadata = |row: &'a PolymarketMarketMetadataRow| -> Result<PreparedCanonicalInputV1<'a>, crate::ContractError> {
        row.validate().map_err(crate::typed_raw_contract_error)?;
        let outcomes = crate::typed_raw::map_metadata_outcomes(&row.asset_ids, row.outcomes_raw.as_deref(), row.outcome_prices_raw.as_deref()).map_err(crate::typed_raw_contract_error)?;
        let initial_tick = if row.metadata_row_kind == crate::typed_raw::PolymarketMetadataRowKind::MetadataSnapshot {
            row.source_evidence_payload_json.as_deref().map(crate::typed_raw::extract_metadata_initial_tick).transpose().map_err(crate::typed_raw_contract_error)?.flatten()
        } else { None };
        Ok(PreparedCanonicalInputV1::Metadata(PreparedMetadataV1 { row, source: prepared_source()?, outcomes, initial_tick }))
    };
    let current = match row {
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) => {
            row.validate().map_err(crate::typed_raw_contract_error)?;
            if matches!(row.event, PolymarketOrderbookEvent::MarketResolved { .. }) {
                prepared_group_id = Some(crate::hash_domain_bytes(
                    b"data-pipeline/public-pm-message-group/v1\0",
                    &crate::stable_json(&source.source_message_id)?,
                ));
                PreparedCanonicalInputV1::PolymarketWsTerminal {
                    row,
                    source: prepared_source()?,
                }
            } else {
                let context = pm_context.ok_or_else(|| {
                    crate::invalid("pm_context", "规范 PM 消息需要原固定身份上下文")
                })?;
                crate::pm_message::require_pm_market(context, row)?;
                let payload = crate::pm_message::payload_from_typed_raw(row, &context.assets)?;
                let message = crate::pm_message::build_canonical_pm_message(
                    context,
                    source.clone(),
                    CanonicalInputRecordRefV1 {
                        input_stream_id: recorded.input_stream_id.clone(),
                        input_position: recorded.input_position,
                    },
                    payload,
                )?;
                prepared_group_id = Some(message.group_id.clone());
                PreparedCanonicalInputV1::PolymarketMessage(PreparedPmMessageV1::prepare(message)?)
            }
        }
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row)) => metadata(row)?,
        TypedRawRowV2::PolymarketMarketMetadataV6(row) => match row.as_ref() {
            PolymarketMarketMetadataV6Input::Existing(row) => metadata(row)?,
            PolymarketMarketMetadataV6Input::AnchorRule(input) => {
                let market = MarketIdentityV2 {
                    condition_id: ConditionId(input.condition_id.clone()),
                    clob_market_id: ClobMarketId(input.clob_market_id.clone()),
                    window: MarketWindowV2 {
                        start_at_utc_ms: input.market_start_utc,
                        end_at_utc_ms: input.market_end_utc,
                    },
                };
                PreparedCanonicalInputV1::AnchorRule {
                    input,
                    facts: canonicalize_market_reference_response_v1(
                        input,
                        &market,
                        source,
                        canonical_rule_version,
                    )?,
                }
            }
        },
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(row)) => {
            row.validate().map_err(crate::typed_raw_contract_error)?;
            PreparedCanonicalInputV1::BinanceDepthSnapshot {
                row,
                source: prepared_source()?,
            }
        }
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(row)) => {
            row.validate().map_err(crate::typed_raw_contract_error)?;
            PreparedCanonicalInputV1::BinanceDepthUpdate {
                row,
                source: prepared_source()?,
            }
        }
        TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(row)) => {
            row.validate().map_err(crate::typed_raw_contract_error)?;
            let payload = match &row.record {
                BinanceAggTradeRecord::Trade { .. } => {
                    Some(crate::prepare_binance_aggregate_trade_payload(row)?)
                }
                BinanceAggTradeRecord::InvalidInput { .. } => None,
            };
            PreparedCanonicalInputV1::BinanceAggregateTrade {
                row,
                source: prepared_source()?,
                payload,
            }
        }
        TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row)) => {
            row.validate().map_err(crate::typed_raw_contract_error)?;
            PreparedCanonicalInputV1::ChainlinkSpot(canonicalize_chainlink_spot_v1(
                row,
                source,
                canonical_rule_version,
            )?)
        }
        TypedRawRowV2::PolymarketRtdsChainlinkTwap(row) => PreparedCanonicalInputV1::ChainlinkTwap(
            canonicalize_chainlink_twap_v1(row, source, canonical_rule_version)?,
        ),
        TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch) => {
            PreparedCanonicalInputV1::HistoricalAnchorRepair {
                batch,
                facts: canonicalize_historical_price_anchor_batch_v1(
                    batch,
                    identity_sources,
                    canonical_rule_version,
                )?,
            }
        }
    };
    let pm_source_identity = match &current {
        PreparedCanonicalInputV1::PolymarketMessage(message) => {
            Some(message.evidence().shared_source_identity())
        }
        _ => None,
    };
    Ok(PreparedIngressV1 {
        recorded,
        current,
        prepared_group_id,
        dependencies: std::borrow::Cow::Borrowed(dependencies),
        reference_selection: None,
        reference_selection_market: None,
        pm_source_identity,
    })
}

/// 来源准备借用唯一核心的当前身份，不构造第二份市场状态。
pub fn prepare_public_ingress_v1<'a>(
    core: &crate::public_observation_core::PublicObservationCoreV1,
    recorded: &'a RecordedIngressV1,
    identity_sources: &BTreeMap<String, contracts::SourceEvidenceRefV3>,
    dependencies: &'a [contracts::CanonicalReferenceEventV2],
) -> Result<PreparedIngressV1<'a>, crate::ContractError> {
    let context = match &recorded.ingress {
        DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
            ..
        } => core.pm_context(&row.market),
        _ => None,
    };
    prepare_ingress_v1(
        recorded,
        context.as_ref(),
        identity_sources,
        dependencies,
        &core.header().canonical_rule_version,
    )
}

/// 输入所属方只按纯函数游标供页；目标市场由真实调用上下文确定。
/// 返回引用随当前 RecordedIngress 保存，重放仍从同一记录恢复，不保存页副本。
pub fn prepare_temporary_reference_for_market_v1(
    core: &crate::public_observation_core::PublicObservationCoreV1,
    input: &mut PreparedIngressV1<'_>,
    market: &contracts::MarketIdentityV2,
    mut read_page: impl FnMut(
        crate::public_reference::TemporaryReferenceCursorV1,
    ) -> Result<
        (
            Vec<crate::public_reference::TemporaryReferenceCandidateV1>,
            crate::public_reference::TemporaryReferenceCursorV1,
            bool,
        ),
        crate::ContractError,
    >,
) -> Result<(), crate::ContractError> {
    use crate::public_reference::*;
    use contracts::*;
    let prior = core.reference_state(&market.condition_id.0);
    let mut reference = prior.and_then(|state| state.rule.value);
    let mut official = prior.and_then(|state| state.official.value.as_ref());
    let mut applicability = prior
        .and_then(|state| state.changed_rule_record.clone())
        .map_or(
            TemporaryReferenceApplicabilityV1::OpeningWindow,
            TemporaryReferenceApplicabilityV1::FromInput,
        );
    let facts: &[CanonicalReferenceEventV2] = match &input.current {
        PreparedCanonicalInputV1::AnchorRule { facts, .. }
        | PreparedCanonicalInputV1::HistoricalAnchorRepair { facts, .. } => facts,
        _ => &[],
    };
    for fact in facts {
        match &fact.payload {
            CanonicalReferenceEventPayloadV2::MarketSettlementReferenceUpdate(update)
                if &update.market == market =>
            {
                if reference.is_some_and(|old| old != update.reference) {
                    // 这是真实配置记录的坐标，只证明从此观察到该配置，不声称平台生效时刻。
                    applicability =
                        TemporaryReferenceApplicabilityV1::FromInput(CanonicalInputRecordRefV1 {
                            input_stream_id: input.recorded.input_stream_id.clone(),
                            input_position: input.recorded.input_position,
                        });
                }
                reference = Some(update.reference);
            }
            CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(update)
                if &update.market == market =>
            {
                official = Some(&update.anchor)
            }
            _ => {}
        }
    }
    let mut cursor = TemporaryReferenceCursorV1::default();
    let mut page = Vec::new();
    let mut next_cursor = cursor;
    let mut exhausted = false;
    loop {
        match select_temporary_reference_v1(
            TemporaryReferenceStateV1 {
                market,
                official,
                temporary: prior.and_then(|state| state.temporary.as_ref()),
                reference,
                applicability: applicability.clone(),
            },
            input.recorded.input_position,
            cursor,
            TemporaryReferencePageV1 {
                candidates: &page,
                next_cursor,
                exhausted,
            },
        ) {
            TemporaryReferenceSelectionResultV1::NeedNextPage { cursor: next } => {
                cursor = next;
                (page, next_cursor, exhausted) = read_page(cursor)?;
            }
            TemporaryReferenceSelectionResultV1::Prepared(selection) => {
                if let Some(selected) = &selection.selected_record_ref {
                    if selected.input_position < input.recorded.input_position {
                        if !input.recorded.dependency_record_refs.contains(selected) {
                            input.recorded.dependency_record_refs.push(selected.clone());
                        }
                        if let Some(candidate) = page
                            .iter()
                            .find(|candidate| &candidate.record_ref == selected)
                        {
                            input
                                .dependencies
                                .to_mut()
                                .push(candidate.canonical_fact.clone());
                        }
                    }
                }
                input.reference_selection_market = Some(market.clone());
                input.reference_selection = Some(selection);
                return Ok(());
            }
        }
    }
}

/// 配齐同一输入的原序引用及必要材料后才返回；调用者随后立即同步 push。
pub fn prepare_public_ingress_with_reference_v1<'a>(
    core: &crate::public_observation_core::PublicObservationCoreV1,
    recorded: &'a mut RecordedIngressV1,
    identity_sources: &BTreeMap<String, contracts::SourceEvidenceRefV3>,
    dependencies: &'a [contracts::CanonicalReferenceEventV2],
    market: &contracts::MarketIdentityV2,
    read_page: impl FnMut(
        crate::public_reference::TemporaryReferenceCursorV1,
    ) -> Result<
        (
            Vec<crate::public_reference::TemporaryReferenceCandidateV1>,
            crate::public_reference::TemporaryReferenceCursorV1,
            bool,
        ),
        crate::ContractError,
    >,
) -> Result<PreparedIngressV1<'a>, crate::ContractError> {
    let context = match &recorded.ingress {
        DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
            ..
        } => core.pm_context(&row.market),
        _ => None,
    };
    let view = RecordedIngressViewV1 {
        input_stream_id: recorded.input_stream_id.clone(),
        input_position: recorded.input_position,
        ingress: &recorded.ingress,
        dependency_record_refs: recorded.dependency_record_refs.clone(),
    };
    let mut prepared = prepare_ingress_view_v1(
        view,
        context.as_ref(),
        identity_sources,
        dependencies,
        &core.header().canonical_rule_version,
    )?;
    prepare_temporary_reference_for_market_v1(core, &mut prepared, market, read_page)?;
    recorded.dependency_record_refs = prepared.recorded.dependency_record_refs.clone();
    Ok(prepared)
}
