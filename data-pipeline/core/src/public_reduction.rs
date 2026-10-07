//! 将同次业务结果绑定公共逻辑位置；后台物化不读取任何可变归约状态。
use contracts::*;
use serde::{Deserialize, Serialize};

use crate::pm_reduction::PreparedPmEvidenceV1;
use crate::reduction::{
    PendingBusinessIdentityV1, PolymarketReductionBatch, ReductionDispositionV1,
    ReductionOperationEvidenceV1, ReductionReportEntryV1, ReferenceReductionOutcomeV1,
};
use crate::{ContractError, hash_domain_bytes, stable_json};

pub type PreparedPublicEvidenceV1 = PreparedPmEvidenceV1<SourceEvidenceRefV3>;

/// 当前真实关闭输入的调用材料；来源记录与当前关闭位置不互相替代。
#[derive(Clone, Debug)]
pub struct PublicReductionContextV1 {
    pub input_stream_id: String,
    pub closed_at_input_position: u64,
    pub first_fact_ordinal: u32,
    pub first_report_ordinal: u32,
    pub group_id: Sha256Hex,
    pub source_refs: Vec<CanonicalInputRecordRefV1>,
    pub direct_sources: Vec<SourceEvidenceRefV3>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicReductionReportV1<I = PublicFactPositionV1> {
    pub position: PublicReportPositionV1,
    pub report: ReductionReportEntryV1<SourceEvidenceRefV3, I>,
}

#[derive(Clone, Debug)]
pub struct PublicReductionBatchV1 {
    pub facts: std::sync::Arc<[PublicBusinessFactV1]>,
    pub reports: Vec<PublicReductionReportV1>,
    group_id: Sha256Hex,
    direct_source_message_ids: Vec<Sha256Hex>,
    prepared_reference_ids: Vec<PublicFactBindingV1>,
}

pub struct MaterializedPublicReductionV1 {
    pub records: Vec<CanonicalEventRecordV2>,
    pub bindings: Vec<PublicFactBindingV1>,
    pub reports: Vec<PublicReductionReportV1<CanonicalEventId>>,
}

fn position(context: &PublicReductionContextV1, ordinal: usize) -> PublicFactPositionV1 {
    PublicFactPositionV1 {
        input_stream_id: context.input_stream_id.clone(),
        closed_at_input_position: context.closed_at_input_position,
        fact_ordinal: context.first_fact_ordinal + ordinal as u32,
    }
}

fn source_time(sources: &[SourceEvidenceRefV3]) -> EventTimeV2 {
    EventTimeV2 {
        source_event_at_utc_ms: sources
            .iter()
            .filter_map(|s| s.source_event_at_utc_ms)
            .max(),
        received_at_utc_ms: sources
            .iter()
            .map(|s| s.received_at_utc_ms)
            .max()
            .expect("业务事件有直接来源"),
    }
}

fn fact(
    context: &PublicReductionContextV1,
    ordinal: usize,
    payload: PublicBusinessPayloadV1,
    lineage: EventLineageV2<PreparedPublicEvidenceV1>,
    dependency_time: EventTimeV2,
    quality: EventQualityV2,
) -> PublicBusinessFactV1 {
    PublicBusinessFactV1 {
        position: position(context, ordinal),
        direct_sources: context.direct_sources.clone(),
        payload,
        source_refs: context.source_refs.clone(),
        lineage: EventLineageV3 {
            canonical_rule_version: lineage.canonical_rule_version,
            source_evidence: lineage
                .source_evidence
                .into_iter()
                .map(|s| s.source().clone())
                .collect(),
        },
        direct_time: source_time(&context.direct_sources),
        dependency_time,
        quality,
    }
}

fn map_report<I, J>(
    report: ReductionReportEntryV1<PreparedPublicEvidenceV1, I>,
    produced: impl FnOnce(I) -> J,
) -> ReductionReportEntryV1<SourceEvidenceRefV3, J> {
    let operations = |values: Vec<ReductionOperationEvidenceV1<PreparedPublicEvidenceV1>>| {
        values
            .into_iter()
            .map(|value| ReductionOperationEvidenceV1 {
                role: value.role,
                source_evidence: value.source_evidence.source().clone(),
            })
            .collect()
    };
    ReductionReportEntryV1 {
        schema_version: "public_reduction_report_v1".to_owned(),
        canonical_rule_version: report.canonical_rule_version,
        scope: report.scope,
        operation: report.operation,
        reason_code: report.reason_code,
        quantity_conservation: report.quantity_conservation,
        disposition: match report.disposition {
            ReductionDispositionV1::Produced { result_event_id } => {
                ReductionDispositionV1::Produced {
                    result_event_id: produced(result_event_id),
                }
            }
            ReductionDispositionV1::NoOutput { operation_evidence } => {
                ReductionDispositionV1::NoOutput {
                    operation_evidence: operations(operation_evidence),
                }
            }
            ReductionDispositionV1::Suppressed { operation_evidence } => {
                ReductionDispositionV1::Suppressed {
                    operation_evidence: operations(operation_evidence),
                }
            }
        },
    }
}

fn finish(
    context: PublicReductionContextV1,
    facts: Vec<PublicBusinessFactV1>,
    reports: Vec<ReductionReportEntryV1<PreparedPublicEvidenceV1, PendingBusinessIdentityV1>>,
    identities: Vec<PendingBusinessIdentityV1>,
) -> PublicReductionBatchV1 {
    let reports = reports
        .into_iter()
        .enumerate()
        .map(|(ordinal, report)| PublicReductionReportV1 {
            position: PublicReportPositionV1 {
                input_stream_id: context.input_stream_id.clone(),
                closed_at_input_position: context.closed_at_input_position,
                report_ordinal: context.first_report_ordinal + ordinal as u32,
            },
            report: map_report(report, |identity| {
                position(
                    &context,
                    identities
                        .iter()
                        .position(|candidate| candidate == &identity)
                        .expect("Produced 引用同次归约产生的事件"),
                )
            }),
        })
        .collect();
    PublicReductionBatchV1 {
        facts: facts.into(),
        reports,
        group_id: context.group_id,
        direct_source_message_ids: context
            .direct_sources
            .into_iter()
            .map(|s| s.source_message_id)
            .collect(),
        prepared_reference_ids: Vec::new(),
    }
}

/// 同次实际前序已经归约完毕；这里只分配稳定位置，不再次计算业务。
pub fn bind_reference_reduction_v1(
    outcome: ReferenceReductionOutcomeV1<PreparedPublicEvidenceV1, PendingBusinessIdentityV1>,
    context: PublicReductionContextV1,
) -> PublicReductionBatchV1 {
    let mut identities = Vec::new();
    let mut facts = Vec::new();
    if let Some(event) = outcome.event {
        identities.push(event.event_id);
        let payload = match event.payload {
            CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(value) => {
                CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(value)
            }
            CanonicalReferenceEventPayloadV1::BinanceDepthState(value) => {
                CanonicalReferenceEventPayloadV2::BinanceDepthState(value)
            }
            CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(value) => {
                CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(value)
            }
        };
        facts.push(fact(
            &context,
            0,
            PublicBusinessPayloadV1::ReferenceFact(payload),
            event.lineage,
            event.time,
            event.quality,
        ));
    }
    finish(context, facts, vec![outcome.report], identities)
}

pub fn bind_market_reduction_v1(
    batch: PolymarketReductionBatch<PreparedPublicEvidenceV1, PendingBusinessIdentityV1>,
    context: PublicReductionContextV1,
) -> PublicReductionBatchV1 {
    bind_market_reduction_with_prefix(batch, context, Vec::new())
}

fn bind_market_reduction_with_prefix(
    batch: PolymarketReductionBatch<PreparedPublicEvidenceV1, PendingBusinessIdentityV1>,
    context: PublicReductionContextV1,
    mut facts: Vec<PublicBusinessFactV1>,
) -> PublicReductionBatchV1 {
    let snapshot_ids = batch
        .reports
        .iter()
        .filter(|report| report.reason_code == "POLYMARKET_METADATA_LIFECYCLE_SNAPSHOT")
        .filter_map(|report| match &report.disposition {
            ReductionDispositionV1::Produced { result_event_id } => Some(result_event_id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut identities = Vec::with_capacity(batch.events.len());
    facts.extend(
        batch
            .events
            .into_iter()
            .enumerate()
            .map(|(ordinal, event)| {
                let lifecycle_snapshot = snapshot_ids
                    .iter()
                    .any(|identity| identity == &event.event_id);
                identities.push(event.event_id);
                let payload = match event.payload {
                    ExecutionMarketEventPayloadV3::MarketLifecycleTransition(value)
                        if lifecycle_snapshot =>
                    {
                        CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot {
                            current_state: value.current_state,
                            assets: value.assets,
                        }
                    }
                    ExecutionMarketEventPayloadV3::MarketLifecycleTransition(value) => {
                        CanonicalMarketFactPayloadV1::LifecycleTransition(value)
                    }
                    ExecutionMarketEventPayloadV3::MarketTradingState(value) => {
                        CanonicalMarketFactPayloadV1::TradingState(value)
                    }
                    ExecutionMarketEventPayloadV3::MarketResolutionTerminal(value) => {
                        CanonicalMarketFactPayloadV1::Terminal(value)
                    }
                    ExecutionMarketEventPayloadV3::QualityBoundary(value) => {
                        CanonicalMarketFactPayloadV1::QualityBoundary(value)
                    }
                    _ => unreachable!("已准备市场归约只由元数据和终态的唯一业务方法产生"),
                };
                fact(
                    &context,
                    ordinal,
                    PublicBusinessPayloadV1::MarketFact {
                        market: event.market,
                        payload,
                    },
                    event.lineage,
                    event.time,
                    event.quality,
                )
            }),
    );
    finish(context, facts, batch.reports, identities)
}

/// 原MarketFact纯物化配方；冷材料复用原组及直接来源顺序，不读取归约状态。
pub fn materialize_market_fact_v1(
    group_id: &Sha256Hex,
    direct_source_message_ids: &[Sha256Hex],
    fact: &PublicBusinessFactV1,
) -> Result<CanonicalMarketFactV1, ContractError> {
    let PublicBusinessPayloadV1::MarketFact {market, payload} = &fact.payload else {
        return Err(ContractError::InvalidField {field: "market_fact.payload".into(),
            reason: "MarketFact物化需原市场事实载荷".into()});
    };
    #[derive(Serialize)]
    struct Identity<'a> {
        schema_version: &'a str,
        market: &'a MarketIdentityV2,
        group_id: &'a Sha256Hex,
        direct_source_message_ids: &'a [Sha256Hex],
        lineage: &'a EventLineageV3,
        direct_time: &'a EventTimeV2,
        dependency_time: &'a EventTimeV2,
        quality: &'a EventQualityV2,
        payload: &'a CanonicalMarketFactPayloadV1,
    }
    let id = CanonicalEventId(
        hash_domain_bytes(
            b"data-pipeline/canonical-event-id/market-fact/v1\0",
            &stable_json(&Identity {
                schema_version: CANONICAL_MARKET_FACT_SCHEMA_V1,
                market,
                group_id,
                direct_source_message_ids,
                lineage: &fact.lineage,
                direct_time: &fact.direct_time,
                dependency_time: &fact.dependency_time,
                quality: &fact.quality,
                payload,
            })?,
        )
        .0,
    );
    let event = CanonicalMarketFactV1 {
        schema_version: CANONICAL_MARKET_FACT_SCHEMA_V1.to_owned(),
        event_id: id.clone(),
        market: market.clone(),
        group_id: group_id.clone(),
        source_refs: fact.source_refs.clone(),
        direct_source_message_ids: direct_source_message_ids.to_vec(),
        lineage: fact.lineage.clone(),
        direct_time: fact.direct_time.clone(),
        dependency_time: fact.dependency_time.clone(),
        quality: fact.quality.clone(),
        payload: payload.clone(),
    };
    Ok(event)
}

/// 后台唯一事实流调用。所有规则、前序证据及载荷都来自传入结果。
pub fn materialize_public_reduction_v1(
    batch: PublicReductionBatchV1,
) -> Result<MaterializedPublicReductionV1, ContractError> {
    let mut records = Vec::with_capacity(batch.facts.len());
    let mut bindings = Vec::with_capacity(batch.facts.len());
    for fact in batch.facts.iter() {
        let (record, event_id) = match &fact.payload {
            PublicBusinessPayloadV1::PolymarketMessage(message) => {
                let id = message.event_id.clone();
                (
                    CanonicalEventRecordV2::PolymarketMessage(message.clone()),
                    id,
                )
            }
            PublicBusinessPayloadV1::ReferenceFact(payload) => {
                let event = if let Some(binding) = batch
                    .prepared_reference_ids
                    .iter()
                    .find(|binding| binding.position == fact.position)
                {
                    CanonicalReferenceEventV2 {
                        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V2.to_owned(),
                        event_id: binding.event_id.clone(),
                        time: fact.dependency_time.clone(),
                        lineage: fact.lineage.clone(),
                        quality: fact.quality.clone(),
                        payload: payload.clone(),
                    }
                } else {
                    crate::public_reference::materialize_reference_v2(
                        fact.lineage.clone(),
                        fact.dependency_time.clone(),
                        fact.quality.clone(),
                        payload.clone(),
                    )?
                };
                let id = event.event_id.clone();
                (CanonicalEventRecordV2::ReferenceEvent(event), id)
            }
            PublicBusinessPayloadV1::MarketFact { .. } => {
                let event = materialize_market_fact_v1(
                    &batch.group_id, &batch.direct_source_message_ids, fact)?;
                let id = event.event_id.clone();
                (CanonicalEventRecordV2::MarketFact(event), id)
            }
        };
        bindings.push(PublicFactBindingV1 {
            position: fact.position.clone(),
            event_id,
        });
        records.push(record);
    }
    let reports = batch
        .reports
        .into_iter()
        .map(|report| {
            let value = report.report;
            let disposition = match value.disposition {
                ReductionDispositionV1::Produced { result_event_id } => {
                    ReductionDispositionV1::Produced {
                        result_event_id: bindings
                            .iter()
                            .find(|binding| binding.position == result_event_id)
                            .expect("Produced 已绑定同批事实位置")
                            .event_id
                            .clone(),
                    }
                }
                ReductionDispositionV1::NoOutput { operation_evidence } => {
                    ReductionDispositionV1::NoOutput { operation_evidence }
                }
                ReductionDispositionV1::Suppressed { operation_evidence } => {
                    ReductionDispositionV1::Suppressed { operation_evidence }
                }
            };
            PublicReductionReportV1 {
                position: report.position,
                report: ReductionReportEntryV1 {
                    schema_version: value.schema_version,
                    canonical_rule_version: value.canonical_rule_version,
                    scope: value.scope,
                    operation: value.operation,
                    reason_code: value.reason_code,
                    quantity_conservation: value.quantity_conservation,
                    disposition,
                },
            }
        })
        .collect();
    Ok(MaterializedPublicReductionV1 {
        records,
        bindings,
        reports,
    })
}

/// 首次身份与初始 tick 直接使用同次元数据计算返回的固定上下文。
pub fn bind_initial_market_reduction_v1(
    market_context: &crate::reduction::PolymarketMarketContextV1<PreparedPublicEvidenceV1>,
    batch: PolymarketReductionBatch<PreparedPublicEvidenceV1, PendingBusinessIdentityV1>,
    mut context: PublicReductionContextV1,
) -> PublicReductionBatchV1 {
    let first = batch.events.first().expect("首次元数据总有生命周期事实");
    let lineage = EventLineageV2 {
        canonical_rule_version: first.lineage.canonical_rule_version.clone(),
        source_evidence: market_context.identity_source_evidence.clone(),
    };
    let dependency_time = source_time(
        &lineage
            .source_evidence
            .iter()
            .map(|s| s.source().clone())
            .collect::<Vec<_>>(),
    );
    let quality = first.quality.clone();
    let mut facts = vec![fact(
        &context,
        0,
        PublicBusinessPayloadV1::MarketFact {
            market: market_context.market.clone(),
            payload: CanonicalMarketFactPayloadV1::FixedIdentity {
                assets: market_context
                    .assets
                    .iter()
                    .map(|asset| asset.asset.clone())
                    .collect(),
            },
        },
        lineage.clone(),
        dependency_time.clone(),
        quality.clone(),
    )];
    for (ordinal, asset) in market_context.assets.iter().enumerate() {
        facts.push(fact(
            &context,
            ordinal + 1,
            PublicBusinessPayloadV1::MarketFact {
                market: market_context.market.clone(),
                payload: CanonicalMarketFactPayloadV1::InitialTick {
                    asset: asset.asset.clone(),
                    tick_size_pusd_per_share: asset.initial_tick_size_pusd_per_share.clone(),
                },
            },
            lineage.clone(),
            dependency_time.clone(),
            quality.clone(),
        ));
    }
    context.first_fact_ordinal += facts.len() as u32;
    bind_market_reduction_with_prefix(batch, context, facts)
}

/// 已规范的来源事实绑定原 ID，不在后台重算来源身份。
pub fn bind_prepared_reference_facts_v1(
    events: Vec<CanonicalReferenceEventV2>,
    context: PublicReductionContextV1,
) -> PublicReductionBatchV1 {
    let mut prepared_reference_ids = Vec::with_capacity(events.len());
    let direct_time = source_time(&context.direct_sources);
    let facts: Vec<_> = events
        .into_iter()
        .enumerate()
        .map(|(ordinal, event)| {
            let position = position(&context, ordinal);
            prepared_reference_ids.push(PublicFactBindingV1 {
                position: position.clone(),
                event_id: event.event_id,
            });
            PublicBusinessFactV1 {
                position,
                direct_sources: context.direct_sources.clone(),
                payload: PublicBusinessPayloadV1::ReferenceFact(event.payload),
                source_refs: context.source_refs.clone(),
                lineage: event.lineage,
                direct_time: direct_time.clone(),
                dependency_time: event.time,
                quality: event.quality,
            }
        })
        .collect();
    PublicReductionBatchV1 {
        facts: facts.into(),
        reports: Vec::new(),
        group_id: context.group_id,
        direct_source_message_ids: context
            .direct_sources
            .into_iter()
            .map(|source| source.source_message_id)
            .collect(),
        prepared_reference_ids,
    }
}
