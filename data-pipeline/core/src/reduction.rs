use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use contracts::economics::EconomicRational;
use contracts::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::typed_raw::{
    self, BinanceAggTradeRecord, BinanceAggTradeRow, BinanceDepthSnapshotRow,
    BinanceDepthStitchVerdict, BinanceDepthUpdateRow, DepthLevel, PolymarketMarketMetadataRow,
    PolymarketOrderbookEvent, PolymarketOrderbookRow, TypedRawRow,
};
use crate::{
    ContractError, derive_execution_market_event_v3_id, derive_reference_event_v1_id,
    hash_domain_bytes, invalid, parse_canonical_decimal, semantic_source_evidence,
    source_evidence_from_typed_raw, stable_json, typed_raw_contract_error, validate_event_id,
    validate_market, validate_source_evidence,
};

pub const REDUCTION_REPORT_SCHEMA_V1: &str = "canonical_reduction_report_v1";

const POLYMARKET_TERMINAL_MAPPING_RULE_V1: &str = "polymarket-binary-winning-asset-one-hot-v1";
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReductionOperationKindV1 {
    Merge,
    Overwrite,
    Delete,
    Confirm,
    Reject,
    Suppress,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantityConservationV1 {
    pub quantity_before: ExactDecimal,
    pub quantity_after: ExactDecimal,
    pub quantity_added: ExactDecimal,
    pub quantity_reduced: ExactDecimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReductionEvidenceRoleV1 {
    Context,
    Input,
    MatchedExisting,
    IncomingDuplicate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionOperationEvidenceV1<E = SourceEvidenceRefV2> {
    pub role: ReductionEvidenceRoleV1,
    pub source_evidence: E,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "disposition",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum ReductionDispositionV1<E = SourceEvidenceRefV2, I = CanonicalEventId> {
    Produced {
        result_event_id: I,
    },
    NoOutput {
        operation_evidence: Vec<ReductionOperationEvidenceV1<E>>,
    },
    Suppressed {
        operation_evidence: Vec<ReductionOperationEvidenceV1<E>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionReportEntryV1<E = SourceEvidenceRefV2, I = CanonicalEventId> {
    pub schema_version: String,
    pub canonical_rule_version: String,
    pub scope: String,
    pub operation: ReductionOperationKindV1,
    pub reason_code: String,
    pub quantity_conservation: Option<QuantityConservationV1>,
    pub disposition: ReductionDispositionV1<E, I>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceReductionOutcomeV1<E = SourceEvidenceRefV2, I = CanonicalEventId> {
    pub event: Option<CanonicalReferenceEventV1<I, E>>,
    pub report: ReductionReportEntryV1<E, I>,
}

pub fn build_chainlink_reference_outcome_v1(
    row: &typed_raw::PolymarketRtdsChainlinkBtcUsdRow,
    input_manifest_sha256: &Sha256Hex,
    input_file_sha256: &Sha256Hex,
    canonical_rule_version: &str,
) -> Result<ReferenceReductionOutcomeV1, ContractError> {
    let event = crate::build_chainlink_reference_event_v1(
        row,
        input_manifest_sha256,
        input_file_sha256,
        canonical_rule_version,
    )?;
    let report = produced_report::<SourceEvidenceRefV2>(
        canonical_rule_version,
        format!("chainlink_reference:{}", row.symbol),
        ReductionOperationKindV1::Merge,
        "CHAINLINK_REFERENCE_UPDATE",
        None,
        event.event_id.clone(),
    )?;
    Ok(ReferenceReductionOutcomeV1 {
        event: Some(event),
        report,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketReductionBatch<E = SourceEvidenceRefV2, I = CanonicalEventId> {
    pub events: Vec<ExecutionMarketEventV3<I, E>>,
    pub reports: Vec<ReductionReportEntryV1<E, I>>,
}

fn exact_from_decimal(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}

pub(crate) fn canonicalize_source_evidence(
    evidence: Vec<SourceEvidenceRefV2>,
) -> Result<Vec<SourceEvidenceRefV2>, ContractError> {
    let mut keyed = evidence
        .into_iter()
        .map(|item| {
            Ok((
                stable_json(&semantic_source_evidence(&item))?,
                stable_json(&item)?,
                item,
            ))
        })
        .collect::<Result<Vec<_>, ContractError>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    keyed.dedup_by(|left, right| left.0 == right.0);
    let canonical = keyed
        .into_iter()
        .map(|(_, _, item)| item)
        .collect::<Vec<_>>();
    validate_source_evidence(&canonical)?;
    Ok(canonical)
}

fn visibility_frontier_evidence<E: crate::pm_reduction::PmEvidenceV1>(
    evidence: Vec<E>,
) -> Result<E, ContractError> {
    canonicalize_reduction_evidence::<E>(evidence)?
        .into_iter()
        .max_by_key(|item| item.received_time())
        .ok_or_else(|| invalid("source_evidence", "visibility frontier requires evidence"))
}

fn canonicalize_operation_evidence(
    evidence: Vec<ReductionOperationEvidenceV1>,
) -> Result<Vec<ReductionOperationEvidenceV1>, ContractError> {
    let mut keyed = evidence
        .into_iter()
        .map(|item| Ok((stable_json(&item)?, item)))
        .collect::<Result<Vec<_>, ContractError>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    keyed.dedup_by(|left, right| left.0 == right.0);
    let canonical = keyed.into_iter().map(|(_, item)| item).collect::<Vec<_>>();
    validate_operation_evidence(&canonical)?;
    Ok(canonical)
}

fn validate_operation_evidence(
    evidence: &[ReductionOperationEvidenceV1],
) -> Result<(), ContractError> {
    if evidence.is_empty() {
        return Err(invalid("operation_evidence", "must not be empty"));
    }
    let mut previous: Option<Vec<u8>> = None;
    for (index, item) in evidence.iter().enumerate() {
        validate_source_evidence(std::slice::from_ref(&item.source_evidence))?;
        let encoded = stable_json(item)?;
        if previous.as_ref().is_some_and(|value| value >= &encoded) {
            return Err(ContractError::NonCanonicalOrder {
                field: format!("operation_evidence[{index}]"),
            });
        }
        previous = Some(encoded);
    }
    Ok(())
}

fn event_time_from_evidence(evidence: &[SourceEvidenceRefV2]) -> EventTimeV2 {
    EventTimeV2 {
        source_event_at_utc_ms: evidence
            .iter()
            .filter_map(|item| item.source_event_at_utc_ms)
            .max(),
        received_at_utc_ms: evidence
            .iter()
            .map(|item| item.received_at_utc_ms)
            .max()
            .expect("canonical evidence is non-empty"),
    }
}

fn finalize_execution_event_legacy(
    market: MarketIdentityV2,
    canonical_rule_version: &str,
    evidence: Vec<SourceEvidenceRefV2>,
    quality: EventQualityV2,
    payload: ExecutionMarketEventPayloadV3,
) -> Result<ExecutionMarketEventV3, ContractError> {
    let evidence = canonicalize_source_evidence(evidence)?;
    let mut event = ExecutionMarketEventV3 {
        schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        event_id: CanonicalEventId("0".repeat(64)),
        market,
        time: event_time_from_evidence(&evidence),
        lineage: EventLineageV2 {
            canonical_rule_version: canonical_rule_version.to_owned(),
            source_evidence: evidence,
        },
        quality,
        payload,
    };
    event.event_id = derive_execution_market_event_v3_id(&event)?;
    Ok(event)
}

fn finalize_reference_event_legacy(
    canonical_rule_version: &str,
    evidence: Vec<SourceEvidenceRefV2>,
    quality: EventQualityV2,
    payload: CanonicalReferenceEventPayloadV1,
) -> Result<CanonicalReferenceEventV1, ContractError> {
    let evidence = canonicalize_source_evidence(evidence)?;
    let mut event = CanonicalReferenceEventV1 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        event_id: CanonicalEventId("0".repeat(64)),
        time: event_time_from_evidence(&evidence),
        lineage: EventLineageV2 {
            canonical_rule_version: canonical_rule_version.to_owned(),
            source_evidence: evidence,
        },
        quality,
        payload,
    };
    event.event_id = derive_reference_event_v1_id(&event)?;
    Ok(event)
}

fn validate_quantity_conservation(summary: &QuantityConservationV1) -> Result<(), ContractError> {
    let before = parse_canonical_decimal(&summary.quantity_before, "quantity_before")?;
    let after = parse_canonical_decimal(&summary.quantity_after, "quantity_after")?;
    let added = parse_canonical_decimal(&summary.quantity_added, "quantity_added")?;
    let reduced = parse_canonical_decimal(&summary.quantity_reduced, "quantity_reduced")?;
    if [before, after, added, reduced]
        .iter()
        .any(Decimal::is_sign_negative)
    {
        return Err(invalid(
            "quantity_conservation",
            "quantities must be non-negative",
        ));
    }
    let before_after_added = before
        .checked_add(added)
        .ok_or_else(|| invalid("quantity_conservation", "decimal addition overflow"))?;
    let after_after_reduced = after
        .checked_add(reduced)
        .ok_or_else(|| invalid("quantity_conservation", "decimal addition overflow"))?;
    if before_after_added != after_after_reduced {
        return Err(invalid(
            "quantity_conservation",
            "before + added must equal after + reduced",
        ));
    }
    Ok(())
}

pub fn validate_reduction_report_entry_v1(
    report: &ReductionReportEntryV1,
) -> Result<(), ContractError> {
    validate_reduction_report::<SourceEvidenceRefV2>(report)
}

fn validate_reduction_report<E: ReductionEvidenceV1>(
    report: &ReductionReportEntryV1<E, E::Identity>,
) -> Result<(), ContractError> {
    if report.schema_version != REDUCTION_REPORT_SCHEMA_V1 {
        return Err(crate::ContractError::UnsupportedSchema {
            contract: "canonical reduction report",
            actual: report.schema_version.clone(),
        });
    }
    if report.canonical_rule_version.trim().is_empty()
        || report.scope.trim().is_empty()
        || report.reason_code.trim().is_empty()
    {
        return Err(invalid(
            "reduction_report",
            "rule version, scope, and reason code must not be empty",
        ));
    }
    if let Some(summary) = &report.quantity_conservation {
        validate_quantity_conservation(summary)?;
    }
    match (&report.operation, &report.disposition) {
        (
            ReductionOperationKindV1::Merge
            | ReductionOperationKindV1::Overwrite
            | ReductionOperationKindV1::Delete
            | ReductionOperationKindV1::Reject,
            ReductionDispositionV1::Produced { result_event_id },
        ) => {
            E::validate_identity(result_event_id)?;
        }
        (
            ReductionOperationKindV1::Reject,
            ReductionDispositionV1::NoOutput { operation_evidence },
        )
        | (
            ReductionOperationKindV1::Confirm | ReductionOperationKindV1::Suppress,
            ReductionDispositionV1::Suppressed { operation_evidence },
        ) => {
            E::validate_operations(operation_evidence)?;
        }
        _ => {
            return Err(invalid(
                "reduction_report",
                "operation and disposition form an impossible combination",
            ));
        }
    }
    Ok(())
}

pub fn encode_reduction_report_entry_v1(
    report: &ReductionReportEntryV1,
) -> Result<Vec<u8>, ContractError> {
    stable_json(report)
}

pub fn decode_reduction_report_entry_v1(
    bytes: &[u8],
) -> Result<ReductionReportEntryV1, ContractError> {
    let report =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_reduction_report_entry_v1(&report)?;
    Ok(report)
}

fn produced_report<E: ReductionEvidenceV1>(
    canonical_rule_version: &str,
    scope: String,
    operation: ReductionOperationKindV1,
    reason_code: &str,
    quantity_conservation: Option<QuantityConservationV1>,
    event_id: E::Identity,
) -> Result<ReductionReportEntryV1<E, E::Identity>, ContractError> {
    let report = ReductionReportEntryV1 {
        schema_version: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
        canonical_rule_version: canonical_rule_version.to_owned(),
        scope,
        operation,
        reason_code: reason_code.to_owned(),
        quantity_conservation,
        disposition: ReductionDispositionV1::Produced {
            result_event_id: event_id,
        },
    };
    Ok(report)
}

fn non_event_report<E: ReductionEvidenceV1>(
    canonical_rule_version: &str,
    scope: String,
    operation: ReductionOperationKindV1,
    reason_code: &str,
    evidence: Vec<E>,
    suppressed: bool,
) -> Result<ReductionReportEntryV1<E, E::Identity>, ContractError> {
    let evidence = E::canonicalize_operations(
        evidence
            .into_iter()
            .map(|source_evidence| ReductionOperationEvidenceV1 {
                role: ReductionEvidenceRoleV1::Input,
                source_evidence,
            })
            .collect(),
    )?;
    let disposition = if suppressed {
        ReductionDispositionV1::Suppressed {
            operation_evidence: evidence,
        }
    } else {
        ReductionDispositionV1::NoOutput {
            operation_evidence: evidence,
        }
    };
    let report = ReductionReportEntryV1 {
        schema_version: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
        canonical_rule_version: canonical_rule_version.to_owned(),
        scope,
        operation,
        reason_code: reason_code.to_owned(),
        quantity_conservation: None,
        disposition,
    };
    Ok(report)
}

pub(crate) fn duplicate_suppressed_report<E: ReductionEvidenceV1>(
    canonical_rule_version: &str,
    scope: String,
    reason_code: &str,
    context: Vec<E>,
    matched_existing: Vec<E>,
    incoming_duplicate: Vec<E>,
) -> Result<ReductionReportEntryV1<E, E::Identity>, ContractError> {
    let mut evidence = context
        .into_iter()
        .map(|source_evidence| ReductionOperationEvidenceV1 {
            role: ReductionEvidenceRoleV1::Context,
            source_evidence,
        })
        .collect::<Vec<_>>();
    evidence.extend(matched_existing.into_iter().map(|source_evidence| {
        ReductionOperationEvidenceV1 {
            role: ReductionEvidenceRoleV1::MatchedExisting,
            source_evidence,
        }
    }));
    evidence.extend(incoming_duplicate.into_iter().map(|source_evidence| {
        ReductionOperationEvidenceV1 {
            role: ReductionEvidenceRoleV1::IncomingDuplicate,
            source_evidence,
        }
    }));
    let report = ReductionReportEntryV1 {
        schema_version: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
        canonical_rule_version: canonical_rule_version.to_owned(),
        scope,
        operation: ReductionOperationKindV1::Suppress,
        reason_code: reason_code.to_owned(),
        quantity_conservation: None,
        disposition: ReductionDispositionV1::Suppressed {
            operation_evidence: E::canonicalize_operations(evidence)?,
        },
    };
    Ok(report)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReferenceLevelState<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    price: ExactDecimal,
    quantity: ExactDecimal,
    direct_evidence: E,
}

#[derive(Clone, Debug)]
struct BinanceDepthBookState<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    snapshot_last_update_id: u64,
    local_u: Option<u64>,
    bids: BTreeMap<Decimal, ReferenceLevelState<E>>,
    asks: BTreeMap<Decimal, ReferenceLevelState<E>>,
    deleted_bids: BTreeMap<Decimal, E>,
    deleted_asks: BTreeMap<Decimal, E>,
    snapshot_evidence: E,
    anchor_evidence: E,
    visibility_frontier_evidence: E,
}

#[derive(Clone, Debug)]
struct BinanceDepthEpochState<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    last_admission_sequence: Option<u64>,
    book: Option<BinanceDepthBookState<E>>,
    awaiting_after_confirmed_gap: bool,
    epoch_switch_prior_book: Option<BinanceDepthBookState<E>>,
    epoch_switch_missing_emitted: bool,
}

#[derive(Clone, Debug)]
pub struct BinanceDepthReducer<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    canonical_rule_version: String,
    epochs: BTreeMap<String, BinanceDepthEpochState<E>>,
    current_epoch: Option<String>,
    seen_epochs: BTreeSet<String>,
}

impl<E: ReductionEvidenceV1> BinanceDepthReducer<E> {
    pub fn new(canonical_rule_version: impl Into<String>) -> Result<Self, ContractError> {
        let canonical_rule_version = canonical_rule_version.into();
        if canonical_rule_version.trim().is_empty() {
            return Err(invalid("canonical_rule_version", "must not be empty"));
        }
        Ok(Self {
            canonical_rule_version,
            epochs: BTreeMap::new(),
            current_epoch: None,
            seen_epochs: BTreeSet::new(),
        })
    }

    /// 只投影唯一现有数量表；公共核心可持有当前不可变派生值，不反向更新数量。
    pub(crate) fn current_depth_value(&self) -> Option<BinanceDepthStateReferenceV1> {
        self.current_epoch
            .as_ref()
            .and_then(|epoch| self.epochs.get(epoch))
            .and_then(|state| state.book.as_ref())
            .map(binance_depth_payload)
    }

    /// 返回会随输入增长、并由生产编排层实施硬上限的保留集合条目数。
    pub fn retained_state_entries(&self) -> usize {
        self.epochs.values().fold(
            self.epochs.len().saturating_add(self.seen_epochs.len()),
            |total, state| {
                state
                    .book
                    .iter()
                    .chain(state.epoch_switch_prior_book.iter())
                    .fold(total, |total, book| {
                        total
                            .saturating_add(book.bids.len())
                            .saturating_add(book.asks.len())
                            .saturating_add(book.deleted_bids.len())
                            .saturating_add(book.deleted_asks.len())
                    })
            },
        )
    }

    /// 完整公共归约与薄路径共用数量状态和接纳步骤。
    pub fn compute_snapshot(
        &mut self,
        row: &BinanceDepthSnapshotRow,
        evidence: E,
    ) -> Result<ReferenceReductionOutcomeV1<E, E::Identity>, ContractError> {
        self.compute_snapshot_projection(row, evidence, false)
            .map(|outcome| outcome.expect("完整路径提供公共归约结果"))
    }
    pub fn compute_update(
        &mut self,
        row: &BinanceDepthUpdateRow,
        evidence: E,
    ) -> Result<ReferenceReductionOutcomeV1<E, E::Identity>, ContractError> {
        self.compute_update_projection(row, evidence, false)
            .map(|outcome| outcome.expect("完整路径提供公共归约结果"))
    }
    /// 研究薄入口复用已验证行和接纳边界，不投影完整事件或盘口谱系。
    pub fn apply_quantity_snapshot(
        &mut self,
        row: &BinanceDepthSnapshotRow,
        evidence: E,
    ) -> Result<(), ContractError> {
        self.compute_snapshot_projection(row, evidence, true)
            .map(|_| ())
    }
    pub fn apply_quantity_update(
        &mut self,
        row: &BinanceDepthUpdateRow,
        evidence: E,
    ) -> Result<(), ContractError> {
        self.compute_update_projection(row, evidence, true)
            .map(|_| ())
    }
    /// 仅在实际采样时精确求和；缺口或新代际尚无快照时返回 None。
    pub fn current_quantity_totals(
        &self,
    ) -> Result<Option<(ExactRationalV1, ExactRationalV1)>, ContractError> {
        let Some(book) = self
            .current_epoch
            .as_ref()
            .and_then(|e| self.epochs.get(e))
            .and_then(|s| s.book.as_ref())
        else {
            return Ok(None);
        };
        let total = |levels: &BTreeMap<Decimal, ReferenceLevelState<E>>| {
            levels
                .values()
                .try_fold(EconomicRational::zero(), |sum, level| {
                    Ok::<_, ContractError>(sum.add(&EconomicRational::from_decimal(
                        parse_canonical_decimal(&level.quantity, "quantity")?,
                    )))
                })
                .map(|sum| sum.public())
        };
        Ok(Some((total(&book.bids)?, total(&book.asks)?)))
    }

    fn compute_snapshot_projection(
        &mut self,
        row: &BinanceDepthSnapshotRow,
        evidence: E,
        quantity_only: bool,
    ) -> Result<Option<ReferenceReductionOutcomeV1<E, E::Identity>>, ContractError> {
        self.begin_epoch(&row.source_sequence_epoch_id)?;
        let state = self
            .epochs
            .entry(row.source_sequence_epoch_id.clone())
            .or_default();
        require_next_admission(
            state.last_admission_sequence,
            row.collector_admission_sequence,
            "binance_depth_snapshot.collector_admission_sequence",
        )?;
        state.last_admission_sequence = Some(row.collector_admission_sequence);

        let bids = reference_level_map(&row.bids, &evidence, "bids")?;
        let asks = reference_level_map(&row.asks, &evidence, "asks")?;
        let prior_evidence = state
            .book
            .as_ref()
            .filter(|_| !quantity_only)
            .map(binance_depth_evidence)
            .transpose()?
            .unwrap_or_default();
        let same_state = state.book.as_ref().is_some_and(|current| {
            current.snapshot_last_update_id == row.last_update_id
                && current.local_u.is_none()
                && same_reference_levels(&current.bids, &bids)
                && same_reference_levels(&current.asks, &asks)
        });
        let resync =
            if state.awaiting_after_confirmed_gap || state.epoch_switch_prior_book.is_some() {
                ResyncQuality::ResyncBoundary
            } else {
                ResyncQuality::NotRequired
            };

        let scope = if quantity_only {
            String::new()
        } else {
            format!("binance_depth:{}", row.source_sequence_epoch_id)
        };
        if same_state {
            if quantity_only {
                return Ok(None);
            }
            let mut confirmation_evidence = prior_evidence;
            confirmation_evidence.push(evidence);
            return Ok(Some(ReferenceReductionOutcomeV1 {
                event: None,
                report: non_event_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Confirm,
                    "BINANCE_DEPTH_IDENTICAL_SNAPSHOT",
                    confirmation_evidence,
                    true,
                )?,
            }));
        }
        let book = BinanceDepthBookState {
            snapshot_last_update_id: row.last_update_id,
            local_u: None,
            bids,
            asks,
            deleted_bids: BTreeMap::new(),
            deleted_asks: BTreeMap::new(),
            snapshot_evidence: evidence.clone(),
            anchor_evidence: evidence.clone(),
            visibility_frontier_evidence: evidence,
        };
        if quantity_only {
            state.install_book(book);
            return Ok(None);
        }
        let event = finalize_binance_depth_event(
            &self.canonical_rule_version,
            &book,
            MarketDataQuality::Fresh,
            resync,
            Vec::new(),
        )?;
        let report = produced_report::<E>(
            &self.canonical_rule_version,
            scope,
            ReductionOperationKindV1::Merge,
            "BINANCE_DEPTH_SNAPSHOT_BASELINE",
            None,
            event.event_id.clone(),
        )?;
        state.install_book(book);
        Ok(Some(ReferenceReductionOutcomeV1 {
            event: Some(event),
            report,
        }))
    }

    fn compute_update_projection(
        &mut self,
        row: &BinanceDepthUpdateRow,
        evidence: E,
        quantity_only: bool,
    ) -> Result<Option<ReferenceReductionOutcomeV1<E, E::Identity>>, ContractError> {
        self.begin_epoch(&row.source_sequence_epoch_id)?;
        let state = self
            .epochs
            .entry(row.source_sequence_epoch_id.clone())
            .or_default();
        require_next_admission(
            state.last_admission_sequence,
            row.collector_admission_sequence,
            "binance_depth_update.collector_admission_sequence",
        )?;
        let expected = expected_depth_verdict(state.book.as_ref(), row)?;
        if expected != row.stitch_verdict {
            return Err(invalid(
                "binance_depth_update.stitch_verdict",
                "does not match the reducer's structured snapshot and update-ID state",
            ));
        }
        state.last_admission_sequence = Some(row.collector_admission_sequence);
        let scope = if quantity_only {
            String::new()
        } else {
            format!("binance_depth:{}", row.source_sequence_epoch_id)
        };

        match row.stitch_verdict {
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable => {
                if quantity_only {
                    return Ok(None);
                }
                if !state.epoch_switch_missing_emitted
                    && let Some(previous_book) = state.epoch_switch_prior_book.as_ref()
                {
                    let event = finalize_binance_depth_event(
                        &self.canonical_rule_version,
                        previous_book,
                        MarketDataQuality::Missing,
                        ResyncQuality::Required,
                        vec![evidence.clone()],
                    )?;
                    let report = produced_report::<E>(
                        &self.canonical_rule_version,
                        scope,
                        ReductionOperationKindV1::Reject,
                        "BINANCE_DEPTH_EPOCH_SNAPSHOT_REQUIRED",
                        None,
                        event.event_id.clone(),
                    )?;
                    state.epoch_switch_missing_emitted = true;
                    return Ok(Some(ReferenceReductionOutcomeV1 {
                        event: Some(event),
                        report,
                    }));
                }
                Ok(Some(ReferenceReductionOutcomeV1 {
                    event: None,
                    report: non_event_report(
                        &self.canonical_rule_version,
                        scope,
                        ReductionOperationKindV1::Reject,
                        "BINANCE_DEPTH_SNAPSHOT_UNAVAILABLE",
                        vec![evidence],
                        false,
                    )?,
                }))
            }
            BinanceDepthStitchVerdict::NotAppliedStaleDuplicate => {
                if quantity_only {
                    return Ok(None);
                }
                let matched_evidence = binance_depth_evidence(
                    state.book.as_ref().expect("stale verdict requires a book"),
                )?;
                Ok(Some(ReferenceReductionOutcomeV1 {
                    event: None,
                    report: duplicate_suppressed_report(
                        &self.canonical_rule_version,
                        scope,
                        "BINANCE_DEPTH_STALE_DUPLICATE",
                        Vec::new(),
                        matched_evidence,
                        vec![evidence],
                    )?,
                }))
            }
            BinanceDepthStitchVerdict::NotAppliedConfirmedHole => {
                if quantity_only {
                    state.confirm_gap();
                    return Ok(None);
                }
                let book = state.book.as_ref().expect("hole verdict requires a book");
                let event = finalize_binance_depth_event(
                    &self.canonical_rule_version,
                    book,
                    MarketDataQuality::ConfirmedGap,
                    ResyncQuality::Required,
                    vec![evidence.clone()],
                )?;
                let report = produced_report::<E>(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Reject,
                    "BINANCE_DEPTH_CONFIRMED_HOLE",
                    None,
                    event.event_id.clone(),
                )?;
                state.confirm_gap();
                Ok(Some(ReferenceReductionOutcomeV1 {
                    event: Some(event),
                    report,
                }))
            }
            BinanceDepthStitchVerdict::Applied => {
                let book = state
                    .book
                    .as_mut()
                    .expect("expected verdict requires a book");
                let previous_frontier = book.visibility_frontier_evidence.clone();
                let bid_report = apply_reference_level_updates(
                    &mut book.bids,
                    &mut book.deleted_bids,
                    &row.bids,
                    &evidence,
                    "bids",
                    !quantity_only,
                )?;
                let ask_report = apply_reference_level_updates(
                    &mut book.asks,
                    &mut book.deleted_asks,
                    &row.asks,
                    &evidence,
                    "asks",
                    !quantity_only,
                )?;
                // 薄入口无报告消费者；只维护共同权威映射及可见性状态。
                let report = match (bid_report, ask_report) {
                    (
                        Some((bid_summary, mut prior_evidence)),
                        Some((ask_summary, ask_prior_evidence)),
                    ) => {
                        prior_evidence.extend(ask_prior_evidence);
                        Some((bid_summary.combine(ask_summary)?, prior_evidence))
                    }
                    (None, None) => None,
                    _ => unreachable!("两侧采用相同报告模式"),
                };
                book.local_u = Some(row.final_update_id);
                book.anchor_evidence = evidence.clone();
                book.visibility_frontier_evidence =
                    visibility_frontier_evidence(vec![previous_frontier, evidence])?;
                let Some((summary, prior_evidence)) = report else {
                    return Ok(None);
                };
                let event = finalize_binance_depth_event(
                    &self.canonical_rule_version,
                    book,
                    MarketDataQuality::Fresh,
                    ResyncQuality::NotRequired,
                    prior_evidence,
                )?;
                let report = produced_report::<E>(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Overwrite,
                    "BINANCE_DEPTH_APPLIED_UPDATE",
                    Some(summary.into_report()),
                    event.event_id.clone(),
                )?;
                Ok(Some(ReferenceReductionOutcomeV1 {
                    event: Some(event),
                    report,
                }))
            }
        }
    }

    fn begin_epoch(&mut self, epoch: &str) -> Result<(), ContractError> {
        if self.current_epoch.as_deref() == Some(epoch) {
            return Ok(());
        }
        if self.seen_epochs.contains(epoch) {
            return Err(invalid(
                "source_sequence_epoch_id",
                "a completed Binance depth epoch cannot resume after another epoch began",
            ));
        }
        let (prior_book, prior_awaiting_after_confirmed_gap, prior_epoch_switch_missing_emitted) =
            self.current_epoch
                .as_ref()
                .and_then(|current| self.epochs.remove(current))
                .map(|state| {
                    (
                        state.book.or(state.epoch_switch_prior_book),
                        state.awaiting_after_confirmed_gap,
                        state.epoch_switch_missing_emitted,
                    )
                })
                .unwrap_or((None, false, false));
        self.current_epoch = Some(epoch.to_owned());
        self.seen_epochs.insert(epoch.to_owned());
        let state = self.epochs.entry(epoch.to_owned()).or_default();
        state.awaiting_after_confirmed_gap = prior_awaiting_after_confirmed_gap;
        state.epoch_switch_prior_book = prior_book;
        state.epoch_switch_missing_emitted = prior_epoch_switch_missing_emitted;
        Ok(())
    }
}

fn require_next_admission(
    previous: Option<u64>,
    current: u64,
    field: &str,
) -> Result<(), ContractError> {
    if previous.is_some_and(|value| current <= value) {
        return Err(invalid(
            field,
            "must be strictly increasing within one epoch",
        ));
    }
    Ok(())
}

fn expected_depth_verdict<E: ReductionEvidenceV1>(
    book: Option<&BinanceDepthBookState<E>>,
    row: &BinanceDepthUpdateRow,
) -> Result<BinanceDepthStitchVerdict, ContractError> {
    let Some(book) = book else {
        return Ok(BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable);
    };
    if row.snapshot_last_update_id != Some(book.snapshot_last_update_id)
        || row.local_u_before != book.local_u
    {
        return Err(invalid(
            "binance_depth_update.stitch_coordinates",
            "do not match the reducer's current epoch state",
        ));
    }
    let previous = book.local_u.unwrap_or(book.snapshot_last_update_id);
    let required = previous
        .checked_add(1)
        .ok_or_else(|| invalid("binance_depth_update", "update ID overflow"))?;
    if row.final_update_id <= previous {
        return Ok(BinanceDepthStitchVerdict::NotAppliedStaleDuplicate);
    }
    if book.local_u.is_none() {
        return Ok(
            if row.first_update_id <= required && required <= row.final_update_id {
                BinanceDepthStitchVerdict::Applied
            } else {
                BinanceDepthStitchVerdict::NotAppliedConfirmedHole
            },
        );
    }
    Ok(if row.first_update_id > required {
        BinanceDepthStitchVerdict::NotAppliedConfirmedHole
    } else {
        BinanceDepthStitchVerdict::Applied
    })
}

fn reference_level_map<E: ReductionEvidenceV1>(
    levels: &[DepthLevel],
    evidence: &E,
    field: &str,
) -> Result<BTreeMap<Decimal, ReferenceLevelState<E>>, ContractError> {
    let mut mapped = BTreeMap::new();
    for (index, level) in levels.iter().enumerate() {
        let price =
            typed_raw::normalize_decimal_text(&level.price, &format!("{field}[{index}].price"))
                .map_err(typed_raw_contract_error)?;
        let quantity = typed_raw::normalize_decimal_text(
            &level.quantity,
            &format!("{field}[{index}].quantity"),
        )
        .map_err(typed_raw_contract_error)?;
        let key = parse_canonical_decimal(&price, &format!("{field}[{index}].price"))?;
        if mapped
            .insert(
                key,
                ReferenceLevelState {
                    price,
                    quantity,
                    direct_evidence: evidence.clone(),
                },
            )
            .is_some()
        {
            return Err(invalid(field, "contains a duplicate price level"));
        }
    }
    Ok(mapped)
}

fn same_reference_levels<E: ReductionEvidenceV1>(
    left: &BTreeMap<Decimal, ReferenceLevelState<E>>,
    right: &BTreeMap<Decimal, ReferenceLevelState<E>>,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_price, left), (right_price, right))| {
                left_price == right_price && left.quantity == right.quantity
            })
}

#[derive(Default)]
struct QuantityTotals {
    before: Decimal,
    after: Decimal,
    added: Decimal,
    reduced: Decimal,
}
impl QuantityTotals {
    fn combine(self, other: Self) -> Result<Self, ContractError> {
        let sum = |left: Decimal, right: Decimal, field: &str| {
            left.checked_add(right)
                .ok_or_else(|| invalid(field, "decimal addition overflow"))
        };
        Ok(Self {
            before: sum(self.before, other.before, "quantity_before")?,
            after: sum(self.after, other.after, "quantity_after")?,
            added: sum(self.added, other.added, "quantity_added")?,
            reduced: sum(self.reduced, other.reduced, "quantity_reduced")?,
        })
    }
    fn into_report(self) -> QuantityConservationV1 {
        QuantityConservationV1 {
            quantity_before: exact_from_decimal(self.before),
            quantity_after: exact_from_decimal(self.after),
            quantity_added: exact_from_decimal(self.added),
            quantity_reduced: exact_from_decimal(self.reduced),
        }
    }
}

fn apply_reference_level_updates<E: ReductionEvidenceV1>(
    levels: &mut BTreeMap<Decimal, ReferenceLevelState<E>>,
    deleted_levels: &mut BTreeMap<Decimal, E>,
    updates: &[DepthLevel],
    evidence: &E,
    field: &str,
    collect_report: bool,
) -> Result<Option<(QuantityTotals, Vec<E>)>, ContractError> {
    let mut seen = BTreeSet::new();
    let mut report = collect_report.then(|| (QuantityTotals::default(), Vec::new()));
    for (index, update) in updates.iter().enumerate() {
        let price =
            typed_raw::normalize_decimal_text(&update.price, &format!("{field}[{index}].price"))
                .map_err(typed_raw_contract_error)?;
        let quantity = typed_raw::normalize_decimal_text(
            &update.quantity,
            &format!("{field}[{index}].quantity"),
        )
        .map_err(typed_raw_contract_error)?;
        let key = parse_canonical_decimal(&price, &format!("{field}[{index}].price"))?;
        if !seen.insert(key) {
            return Err(invalid(field, "contains a duplicate update price"));
        }
        let previous = levels.get(&key);
        let before = previous
            .map(|level| parse_canonical_decimal(&level.quantity, field))
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let after = parse_canonical_decimal(&quantity, field)?;
        if let Some((summary, prior_evidence)) = report.as_mut() {
            if let Some(previous) = previous {
                prior_evidence.push(previous.direct_evidence.clone());
            } else if let Some(previous_deletion) = deleted_levels.get(&key) {
                prior_evidence.push(previous_deletion.clone());
            }
            let (added, reduced) = match after.cmp(&before) {
                Ordering::Greater => (after - before, Decimal::ZERO),
                Ordering::Less => (Decimal::ZERO, before - after),
                Ordering::Equal => (Decimal::ZERO, Decimal::ZERO),
            };
            *summary = std::mem::take(summary).combine(QuantityTotals {
                before,
                after,
                added,
                reduced,
            })?;
        }
        if after.is_zero() {
            levels.remove(&key);
            if !before.is_zero() || deleted_levels.contains_key(&key) {
                deleted_levels.insert(key, evidence.clone());
            }
        } else {
            deleted_levels.remove(&key);
            levels.insert(
                key,
                ReferenceLevelState {
                    price,
                    quantity,
                    direct_evidence: evidence.clone(),
                },
            );
        }
    }
    Ok(report)
}

fn finalize_binance_depth_event<E: ReductionEvidenceV1>(
    canonical_rule_version: &str,
    book: &BinanceDepthBookState<E>,
    availability: MarketDataQuality,
    resync: ResyncQuality,
    additional_evidence: Vec<E>,
) -> Result<CanonicalReferenceEventV1<E::Identity, E>, ContractError> {
    let mut evidence = binance_depth_evidence(book)?;
    evidence.extend(additional_evidence);
    finalize_reference_event(
        canonical_rule_version,
        evidence,
        EventQualityV2 {
            availability,
            resync,
        },
        CanonicalReferenceEventPayloadV1::BinanceDepthState(binance_depth_payload(book)),
    )
}

fn binance_depth_payload<E: ReductionEvidenceV1>(
    book: &BinanceDepthBookState<E>,
) -> BinanceDepthStateReferenceV1 {
    let last_update_id = book.local_u.unwrap_or(book.snapshot_last_update_id);
    let bids = book
        .bids
        .values()
        .rev()
        .map(|level| ReferencePriceLevelV1 {
            price_usdt_per_btc: level.price.clone(),
            quantity_btc: level.quantity.clone(),
        })
        .collect();
    let asks = book
        .asks
        .values()
        .map(|level| ReferencePriceLevelV1 {
            price_usdt_per_btc: level.price.clone(),
            quantity_btc: level.quantity.clone(),
        })
        .collect();
    BinanceDepthStateReferenceV1 {
        symbol: "BTCUSDT".to_owned(),
        last_update_id,
        bids,
        asks,
    }
}

fn binance_depth_evidence<E: ReductionEvidenceV1>(
    book: &BinanceDepthBookState<E>,
) -> Result<Vec<E>, ContractError> {
    canonicalize_reduction_evidence::<E>(
        book.bids
            .values()
            .chain(book.asks.values())
            .map(|level| level.direct_evidence.clone())
            .chain(book.deleted_bids.values().cloned())
            .chain(book.deleted_asks.values().cloned())
            .chain([
                book.snapshot_evidence.clone(),
                book.anchor_evidence.clone(),
                book.visibility_frontier_evidence.clone(),
            ])
            .collect(),
    )
}

#[derive(Clone, Debug)]
pub struct BinanceAggTradeReducer<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    canonical_rule_version: String,
    current_trade: Option<(u64, i64, BinanceAggregateTradeReferenceV1, E)>,
}

impl<E: ReductionEvidenceV1> BinanceAggTradeReducer<E> {
    pub fn new(canonical_rule_version: impl Into<String>) -> Result<Self, ContractError> {
        let canonical_rule_version = canonical_rule_version.into();
        if canonical_rule_version.trim().is_empty() {
            return Err(invalid("canonical_rule_version", "must not be empty"));
        }
        Ok(Self {
            canonical_rule_version,
            current_trade: None,
        })
    }

    pub fn compute(
        &mut self,
        row: &BinanceAggTradeRow,
        evidence: E,
        prepared_payload: Option<&BinanceAggregateTradeReferenceV1>,
    ) -> Result<ReferenceReductionOutcomeV1<E, E::Identity>, ContractError> {
        let BinanceAggTradeRecord::Trade {
            aggregate_trade_id, ..
        } = &row.record
        else {
            return Ok(ReferenceReductionOutcomeV1 {
                event: None,
                report: non_event_report(
                    &self.canonical_rule_version,
                    "binance_aggtrade:invalid".to_owned(),
                    ReductionOperationKindV1::Reject,
                    "BINANCE_AGGTRADE_INVALID_INPUT",
                    vec![evidence],
                    false,
                )?,
            });
        };
        let payload = prepared_payload.expect("已准备的 Trade 保留规范成交载荷");
        let candidate = finalize_reference_event(
            &self.canonical_rule_version,
            vec![evidence.clone()],
            EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
            CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(payload.clone()),
        )?;
        let trade_time = candidate
            .time
            .source_event_at_utc_ms
            .expect("validated aggregate trade has source time");
        if let Some((current_id, current_trade_time, current_payload, current_evidence)) =
            &self.current_trade
        {
            match aggregate_trade_id.cmp(current_id) {
                Ordering::Less => {
                    return Err(invalid(
                        "aggregate_trade_id",
                        "grouped aggregate trades must not move backwards",
                    ));
                }
                Ordering::Equal
                    if current_trade_time != &trade_time || current_payload != payload =>
                {
                    return Err(invalid(
                        "aggregate_trade_id",
                        "the same native aggregate trade ID has conflicting payloads",
                    ));
                }
                Ordering::Equal => {
                    return Ok(ReferenceReductionOutcomeV1 {
                        event: None,
                        report: duplicate_suppressed_report(
                            &self.canonical_rule_version,
                            format!("binance_aggtrade:{aggregate_trade_id}"),
                            "BINANCE_AGGTRADE_IDENTICAL_DUPLICATE",
                            Vec::new(),
                            vec![current_evidence.clone()],
                            vec![evidence],
                        )?,
                    });
                }
                Ordering::Greater => {}
            }
        }
        self.current_trade = Some((*aggregate_trade_id, trade_time, payload.clone(), evidence));
        let report = produced_report::<E>(
            &self.canonical_rule_version,
            format!("binance_aggtrade:{aggregate_trade_id}"),
            ReductionOperationKindV1::Merge,
            "BINANCE_AGGTRADE_ACCEPTED",
            None,
            candidate.event_id.clone(),
        )?;
        Ok(ReferenceReductionOutcomeV1 {
            event: Some(candidate),
            report,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketAssetContextV1 {
    pub asset: AssetIdentityV1,
    pub initial_tick_size_pusd_per_share: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketContextV1<E = SourceEvidenceRefV2> {
    pub market: MarketIdentityV2,
    pub assets: Vec<PolymarketAssetContextV1>,
    pub identity_source_evidence: Vec<E>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookInputV1<E = SourceEvidenceRefV2> {
    pub row: PolymarketOrderbookRow,
    pub evidence: E,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMetadataInputV1<E = SourceEvidenceRefV2> {
    pub row: PolymarketMarketMetadataRow,
    pub evidence: E,
}

#[derive(Clone, Debug)]
pub struct PolymarketMetadataReducer<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    market: MarketIdentityV2,
    assets: Vec<AssetIdentityV1>,
    identity_source_evidence: Vec<E>,
    canonical_rule_version: String,
    current_lifecycle: KnownMarketLifecycleState,
    last_lifecycle_transition: Option<(KnownMarketLifecycleState, KnownMarketLifecycleState)>,
    current_trading_state: Option<MarketTradingStateV2>,
}

impl<E: ReductionEvidenceV1> PolymarketMetadataReducer<E> {
    pub fn compute_identity_snapshot(
        input: PolymarketMetadataInputV1<E>,
        outcomes: Vec<typed_raw::MetadataOutcomeEvidence>,
        initial_tick: Option<typed_raw::MetadataInitialTickEvidence>,
        canonical_rule_version: impl Into<String>,
    ) -> Result<
        (
            Self,
            PolymarketMarketContextV1<E>,
            PolymarketReductionBatch<E, E::Identity>,
        ),
        ContractError,
    > {
        let canonical_rule_version = canonical_rule_version.into();
        if canonical_rule_version.trim().is_empty() {
            return Err(invalid("canonical_rule_version", "must not be empty"));
        }
        validate_identity_snapshot(&input)?;

        let initial_tick = initial_tick.ok_or_else(|| {
            invalid(
                "source_evidence_payload_json.orderPriceMinTickSize",
                "is required to build the market context",
            )
        })?;
        let market = MarketIdentityV2 {
            condition_id: ConditionId(input.row.condition_id.clone()),
            clob_market_id: ClobMarketId(input.row.clob_market_id.clone()),
            window: MarketWindowV2 {
                start_at_utc_ms: input.row.market_start_utc,
                end_at_utc_ms: input.row.market_end_utc,
            },
        };
        validate_market(&market, "market")?;

        let mut asset_contexts = outcomes
            .into_iter()
            .enumerate()
            .map(|(index, outcome)| {
                let label = outcome.outcome.ok_or_else(|| {
                    invalid(
                        format!("outcomes_raw[{index}]"),
                        "is required to build the market context",
                    )
                })?;
                Ok(PolymarketAssetContextV1 {
                    asset: AssetIdentityV1 {
                        token_id: TokenId(outcome.asset_id),
                        outcome: OutcomeIdentityV1 {
                            index: u32::try_from(index).map_err(|_| {
                                invalid("outcomes_raw", "outcome index exceeds u32")
                            })?,
                            label,
                        },
                    },
                    initial_tick_size_pusd_per_share: initial_tick.exact.clone(),
                })
            })
            .collect::<Result<Vec<_>, ContractError>>()?;
        asset_contexts.sort_by(|left, right| {
            compare_decimal_ids(&left.asset.token_id.0, &right.asset.token_id.0)
        });
        let assets = asset_contexts
            .iter()
            .map(|context| context.asset.clone())
            .collect::<Vec<_>>();
        let current_lifecycle = map_metadata_lifecycle(input.row.market_lifecycle_state);
        let current_trading_state = metadata_trading_state(&input.row);
        let identity_source_evidence = vec![input.evidence.clone()];
        let context = PolymarketMarketContextV1 {
            market: market.clone(),
            assets: asset_contexts,
            identity_source_evidence: identity_source_evidence.clone(),
        };
        let mut reducer = Self {
            market,
            assets,
            identity_source_evidence,
            canonical_rule_version,
            current_lifecycle,
            last_lifecycle_transition: None,
            current_trading_state: current_trading_state.clone(),
        };
        let mut batch = PolymarketReductionBatch {
            events: Vec::new(),
            reports: Vec::new(),
        };
        reducer.append_lifecycle_event(
            None,
            current_lifecycle,
            &input.evidence,
            ReductionOperationKindV1::Merge,
            "POLYMARKET_METADATA_INITIAL_LIFECYCLE",
            &mut batch,
        )?;
        if let Some(trading_state) = current_trading_state {
            reducer.append_trading_state_event(
                trading_state,
                &input.evidence,
                ReductionOperationKindV1::Merge,
                "POLYMARKET_METADATA_INITIAL_TRADING_STATE",
                &mut batch,
            )?;
        }
        Ok((reducer, context, batch))
    }

    pub const fn market(&self) -> &MarketIdentityV2 {
        &self.market
    }

    pub fn compute_metadata(
        &mut self,
        input: PolymarketMetadataInputV1<E>,
        outcomes: &[typed_raw::MetadataOutcomeEvidence],
        public_snapshot: bool,
    ) -> Result<PolymarketReductionBatch<E, E::Identity>, ContractError> {
        validate_prepared_metadata_identity(&input.row, &self.market, &self.assets, outcomes)?;
        if input.row.metadata_row_kind
            == typed_raw::PolymarketMetadataRowKind::ResolutionFollowupSummary
        {
            let reason_code = match input.row.resolution_followup_status {
                Some(
                    typed_raw::MetadataResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal,
                ) => "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_AFTER_VALID_NONTERMINAL",
                Some(
                    typed_raw::MetadataResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse,
                ) => "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_WITHOUT_VALID_RESPONSE",
                _ => unreachable!("validated followup summary has a budget-exhausted status"),
            };
            let mut evidence = self.identity_source_evidence.clone();
            evidence.push(input.evidence);
            return Ok(PolymarketReductionBatch {
                events: Vec::new(),
                reports: vec![non_event_report(
                    &self.canonical_rule_version,
                    format!("polymarket_market:{}", self.market.condition_id.0),
                    ReductionOperationKindV1::Reject,
                    reason_code,
                    evidence,
                    false,
                )?],
            });
        }
        if input.row.market_lifecycle_transition
            == Some(typed_raw::MetadataMarketLifecycleTransition::ActiveToRetired)
        {
            return Err(invalid(
                "market_lifecycle_transition",
                "active_to_retired must be emitted only by the qualified terminal merger",
            ));
        }

        let mut batch = PolymarketReductionBatch {
            events: Vec::new(),
            reports: Vec::new(),
        };
        if let Some((previous, current)) =
            mapped_metadata_transition(input.row.market_lifecycle_transition)
        {
            if map_metadata_lifecycle(input.row.market_lifecycle_state) != current {
                return Err(invalid(
                    "market_lifecycle_state",
                    "does not match market_lifecycle_transition",
                ));
            }
            if self.current_lifecycle == current {
                if self.last_lifecycle_transition != Some((previous, current)) {
                    return Err(invalid(
                        "market_lifecycle_transition",
                        "duplicate current state has a different transition preimage",
                    ));
                }
            } else {
                if self.current_lifecycle != previous {
                    return Err(invalid(
                        "market_lifecycle_transition",
                        "previous state does not match the reducer state",
                    ));
                }
                self.append_lifecycle_event(
                    Some(previous),
                    current,
                    &input.evidence,
                    ReductionOperationKindV1::Overwrite,
                    "POLYMARKET_METADATA_LIFECYCLE_TRANSITION",
                    &mut batch,
                )?;
                self.current_lifecycle = current;
                self.last_lifecycle_transition = Some((previous, current));
            }
        } else if map_metadata_lifecycle(input.row.market_lifecycle_state) != self.current_lifecycle
        {
            if public_snapshot
                && input.row.metadata_row_kind
                    == typed_raw::PolymarketMetadataRowKind::MetadataSnapshot
                && self.current_lifecycle == KnownMarketLifecycleState::Prefetched
                && map_metadata_lifecycle(input.row.market_lifecycle_state)
                    == KnownMarketLifecycleState::Active
            {
                self.append_lifecycle_event(
                    None,
                    KnownMarketLifecycleState::Active,
                    &input.evidence,
                    ReductionOperationKindV1::Overwrite,
                    "POLYMARKET_METADATA_LIFECYCLE_SNAPSHOT",
                    &mut batch,
                )?;
                self.current_lifecycle = KnownMarketLifecycleState::Active;
                self.last_lifecycle_transition = None;
            } else {
                return Err(invalid(
                    "market_lifecycle_state",
                    "changed without an explicit lifecycle transition",
                ));
            }
        }

        if let Some(trading_state) = metadata_trading_state(&input.row)
            && self.current_trading_state.as_ref() != Some(&trading_state)
        {
            self.append_trading_state_event(
                trading_state.clone(),
                &input.evidence,
                ReductionOperationKindV1::Overwrite,
                "POLYMARKET_METADATA_TRADING_STATE_CHANGED",
                &mut batch,
            )?;
            self.current_trading_state = Some(trading_state);
        }

        if batch.events.is_empty() {
            let mut evidence = self.identity_source_evidence.clone();
            evidence.push(input.evidence);
            batch.reports.push(non_event_report(
                &self.canonical_rule_version,
                format!("polymarket_market:{}", self.market.condition_id.0),
                ReductionOperationKindV1::Confirm,
                "POLYMARKET_METADATA_STATE_UNCHANGED",
                evidence,
                true,
            )?);
        }
        Ok(batch)
    }

    fn append_lifecycle_event(
        &mut self,
        previous_state: Option<KnownMarketLifecycleState>,
        current_state: KnownMarketLifecycleState,
        input_evidence: &E,
        operation: ReductionOperationKindV1,
        reason_code: &str,
        batch: &mut PolymarketReductionBatch<E, E::Identity>,
    ) -> Result<(), ContractError> {
        let mut evidence = self.identity_source_evidence.clone();
        evidence.push(input_evidence.clone());
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence,
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::MarketLifecycleTransition(MarketLifecycleTransitionV2 {
                previous_state: previous_state
                    .map(|value| MappedMarketLifecycleStateV1::Known { value }),
                current_state: MappedMarketLifecycleStateV1::Known {
                    value: current_state,
                },
                assets: self.assets.clone(),
            }),
        )?;
        batch.reports.push(produced_report::<E>(
            &self.canonical_rule_version,
            format!("polymarket_market:{}", self.market.condition_id.0),
            operation,
            reason_code,
            None,
            event.event_id.clone(),
        )?);
        batch.events.push(event);
        Ok(())
    }

    fn append_trading_state_event(
        &mut self,
        trading_state: MarketTradingStateV2,
        input_evidence: &E,
        operation: ReductionOperationKindV1,
        reason_code: &str,
        batch: &mut PolymarketReductionBatch<E, E::Identity>,
    ) -> Result<(), ContractError> {
        let mut evidence = self.identity_source_evidence.clone();
        evidence.push(input_evidence.clone());
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence,
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::MarketTradingState(trading_state),
        )?;
        batch.reports.push(produced_report::<E>(
            &self.canonical_rule_version,
            format!("polymarket_market:{}", self.market.condition_id.0),
            operation,
            reason_code,
            None,
            event.event_id.clone(),
        )?);
        batch.events.push(event);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PolymarketTerminalMerger<E: ReductionEvidenceV1 = SourceEvidenceRefV2> {
    market: MarketIdentityV2,
    assets: Vec<AssetIdentityV1>,
    identity_source_evidence: Vec<E>,
    canonical_rule_version: String,
    emitted_terminal: Option<MarketResolutionTerminalV3>,
    emitted_terminal_evidence: Vec<E>,
    quarantined: bool,
}

impl<E: ReductionEvidenceV1> PolymarketTerminalMerger<E> {
    pub fn new(
        context: PolymarketMarketContextV1<E>,
        canonical_rule_version: impl Into<String>,
    ) -> Result<Self, ContractError> {
        let (market, canonical_rule_version, contexts, identity_source_evidence) =
            prepare_polymarket_context(context, canonical_rule_version.into())?;
        let mut assets = contexts
            .into_iter()
            .map(|context| context.asset)
            .collect::<Vec<_>>();
        assets.sort_by(|left, right| compare_decimal_ids(&left.token_id.0, &right.token_id.0));
        Ok(Self {
            market,
            assets,
            identity_source_evidence,
            canonical_rule_version,
            emitted_terminal: None,
            emitted_terminal_evidence: Vec::new(),
            quarantined: false,
        })
    }

    pub fn compute_ws_terminal(
        &mut self,
        metadata: PolymarketMetadataInputV1<E>,
        metadata_outcomes: &[typed_raw::MetadataOutcomeEvidence],
        orderbook: PolymarketOrderbookInputV1<E>,
    ) -> Result<PolymarketReductionBatch<E, E::Identity>, ContractError> {
        let terminal_evidence = vec![metadata.evidence.clone(), orderbook.evidence.clone()];
        let mut evidence = self.identity_source_evidence.clone();
        evidence.extend(terminal_evidence.clone());
        let scope = format!("polymarket_market:{}", self.market.condition_id.0);

        if self.quarantined {
            return Ok(PolymarketReductionBatch {
                events: Vec::new(),
                reports: vec![non_event_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Reject,
                    "POLYMARKET_TERMINAL_MARKET_ALREADY_QUARANTINED",
                    evidence,
                    false,
                )?],
            });
        }

        if let Some(reason_code) = terminal_conflict_reason(
            &self.market,
            &self.assets,
            &metadata.row,
            &orderbook.row,
            metadata_outcomes,
        )? {
            let previous_quality = if self.emitted_terminal.is_some() {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::UnknownNotObserved
            };
            let mut conflict_evidence = evidence;
            if self.emitted_terminal.is_some() {
                conflict_evidence.extend(self.emitted_terminal_evidence.clone());
            }
            let event = finalize_execution_event(
                self.market.clone(),
                &self.canonical_rule_version,
                conflict_evidence,
                EventQualityV2 {
                    availability: MarketDataQuality::Invalid,
                    resync: ResyncQuality::NotRequired,
                },
                ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                    scope: QualityScopeV1::Market,
                    previous_quality,
                    current_quality: MarketDataQuality::Invalid,
                    reason_code: reason_code.to_owned(),
                }),
            )?;
            let report = produced_report::<E>(
                &self.canonical_rule_version,
                scope,
                ReductionOperationKindV1::Reject,
                reason_code,
                None,
                event.event_id.clone(),
            )?;
            self.quarantined = true;
            return Ok(PolymarketReductionBatch {
                events: vec![event],
                reports: vec![report],
            });
        }

        let winning_asset_id = metadata
            .row
            .winning_asset_id
            .as_deref()
            .expect("validated WS terminal has a winner");
        let winning_asset = self
            .assets
            .iter()
            .find(|asset| asset.token_id.0 == winning_asset_id)
            .expect("conflict validation matched the winner to context");
        let payout_vector = self
            .assets
            .iter()
            .cloned()
            .map(|asset| PayoutEntryV1 {
                payout_pusd_per_share: ExactDecimal(
                    if asset.token_id.0 == winning_asset_id {
                        "1"
                    } else {
                        "0"
                    }
                    .to_owned(),
                ),
                asset,
            })
            .collect();
        let terminal = MarketResolutionTerminalV3::Resolved {
            winning_asset_id: winning_asset_id.to_owned(),
            winning_outcome: winning_asset.outcome.label.clone(),
            payout_vector,
            mapping_rule_version: POLYMARKET_TERMINAL_MAPPING_RULE_V1.to_owned(),
        };
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence.clone(),
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal.clone()),
        )?;

        if let Some(previous) = &self.emitted_terminal {
            if previous != &terminal {
                let mut conflict_evidence = evidence;
                conflict_evidence.extend(self.emitted_terminal_evidence.clone());
                let conflict = finalize_execution_event(
                    self.market.clone(),
                    &self.canonical_rule_version,
                    conflict_evidence,
                    EventQualityV2 {
                        availability: MarketDataQuality::Invalid,
                        resync: ResyncQuality::NotRequired,
                    },
                    ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                        scope: QualityScopeV1::Market,
                        previous_quality: MarketDataQuality::Fresh,
                        current_quality: MarketDataQuality::Invalid,
                        reason_code: "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT".to_owned(),
                    }),
                )?;
                let report = produced_report::<E>(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Reject,
                    "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT",
                    None,
                    conflict.event_id.clone(),
                )?;
                self.quarantined = true;
                return Ok(PolymarketReductionBatch {
                    events: vec![conflict],
                    reports: vec![report],
                });
            }
            return Ok(PolymarketReductionBatch {
                events: Vec::new(),
                reports: vec![duplicate_suppressed_report(
                    &self.canonical_rule_version,
                    scope,
                    "POLYMARKET_TERMINAL_IDENTICAL_DUPLICATE",
                    self.identity_source_evidence.clone(),
                    self.emitted_terminal_evidence.clone(),
                    terminal_evidence,
                )?],
            });
        }

        self.emitted_terminal = Some(terminal);
        self.emitted_terminal_evidence = terminal_evidence.clone();
        let report = produced_report::<E>(
            &self.canonical_rule_version,
            scope,
            ReductionOperationKindV1::Merge,
            "POLYMARKET_TERMINAL_WS_METADATA_MERGE",
            None,
            event.event_id.clone(),
        )?;
        let lifecycle_event = qualified_terminal_lifecycle_event(
            &self.market,
            &self.assets,
            &self.canonical_rule_version,
            evidence_for_terminal_state(&self.identity_source_evidence, &terminal_evidence),
        )?;
        let lifecycle_report = produced_report::<E>(
            &self.canonical_rule_version,
            format!("polymarket_market:{}", self.market.condition_id.0),
            ReductionOperationKindV1::Overwrite,
            "POLYMARKET_TERMINAL_ACTIVE_TO_RETIRED",
            None,
            lifecycle_event.event_id.clone(),
        )?;
        Ok(PolymarketReductionBatch {
            events: vec![event, lifecycle_event],
            reports: vec![report, lifecycle_report],
        })
    }

    pub fn compute_gamma_terminal(
        &mut self,
        metadata: PolymarketMetadataInputV1<E>,
        metadata_outcomes: &[typed_raw::MetadataOutcomeEvidence],
    ) -> Result<PolymarketReductionBatch<E, E::Identity>, ContractError> {
        let terminal_evidence = vec![metadata.evidence.clone()];
        let mut evidence = self.identity_source_evidence.clone();
        evidence.extend(terminal_evidence.clone());
        let scope = format!("polymarket_market:{}", self.market.condition_id.0);

        if self.quarantined {
            return Ok(PolymarketReductionBatch {
                events: Vec::new(),
                reports: vec![non_event_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Reject,
                    "POLYMARKET_TERMINAL_MARKET_ALREADY_QUARANTINED",
                    evidence,
                    false,
                )?],
            });
        }

        if let Some(reason_code) = gamma_terminal_conflict_reason(
            &self.market,
            &self.assets,
            &metadata,
            metadata_outcomes,
        )? {
            let previous_quality = if self.emitted_terminal.is_some() {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::UnknownNotObserved
            };
            let mut conflict_evidence = evidence;
            if self.emitted_terminal.is_some() {
                conflict_evidence.extend(self.emitted_terminal_evidence.clone());
            }
            let event = finalize_execution_event(
                self.market.clone(),
                &self.canonical_rule_version,
                conflict_evidence,
                EventQualityV2 {
                    availability: MarketDataQuality::Invalid,
                    resync: ResyncQuality::NotRequired,
                },
                ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                    scope: QualityScopeV1::Market,
                    previous_quality,
                    current_quality: MarketDataQuality::Invalid,
                    reason_code: reason_code.to_owned(),
                }),
            )?;
            let report = produced_report::<E>(
                &self.canonical_rule_version,
                scope,
                ReductionOperationKindV1::Reject,
                reason_code,
                None,
                event.event_id.clone(),
            )?;
            self.quarantined = true;
            return Ok(PolymarketReductionBatch {
                events: vec![event],
                reports: vec![report],
            });
        }

        let winning_asset_id = metadata
            .row
            .winning_asset_id
            .as_deref()
            .expect("validated Gamma terminal has a winner");
        let winning_asset = self
            .assets
            .iter()
            .find(|asset| asset.token_id.0 == winning_asset_id)
            .expect("Gamma terminal validation matched the winner to context");
        let terminal = MarketResolutionTerminalV3::Resolved {
            winning_asset_id: winning_asset_id.to_owned(),
            winning_outcome: winning_asset.outcome.label.clone(),
            payout_vector: self
                .assets
                .iter()
                .cloned()
                .map(|asset| PayoutEntryV1 {
                    payout_pusd_per_share: ExactDecimal(
                        if asset.token_id.0 == winning_asset_id {
                            "1"
                        } else {
                            "0"
                        }
                        .to_owned(),
                    ),
                    asset,
                })
                .collect(),
            mapping_rule_version: POLYMARKET_TERMINAL_MAPPING_RULE_V1.to_owned(),
        };
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence.clone(),
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal.clone()),
        )?;

        if let Some(previous) = &self.emitted_terminal {
            if previous != &terminal {
                let mut conflict_evidence = evidence;
                conflict_evidence.extend(self.emitted_terminal_evidence.clone());
                let conflict = finalize_execution_event(
                    self.market.clone(),
                    &self.canonical_rule_version,
                    conflict_evidence,
                    EventQualityV2 {
                        availability: MarketDataQuality::Invalid,
                        resync: ResyncQuality::NotRequired,
                    },
                    ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                        scope: QualityScopeV1::Market,
                        previous_quality: MarketDataQuality::Fresh,
                        current_quality: MarketDataQuality::Invalid,
                        reason_code: "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT".to_owned(),
                    }),
                )?;
                let report = produced_report::<E>(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Reject,
                    "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT",
                    None,
                    conflict.event_id.clone(),
                )?;
                self.quarantined = true;
                return Ok(PolymarketReductionBatch {
                    events: vec![conflict],
                    reports: vec![report],
                });
            }
            return Ok(PolymarketReductionBatch {
                events: Vec::new(),
                reports: vec![duplicate_suppressed_report(
                    &self.canonical_rule_version,
                    scope,
                    "POLYMARKET_TERMINAL_IDENTICAL_DUPLICATE",
                    self.identity_source_evidence.clone(),
                    self.emitted_terminal_evidence.clone(),
                    terminal_evidence,
                )?],
            });
        }

        self.emitted_terminal = Some(terminal);
        self.emitted_terminal_evidence = terminal_evidence.clone();
        let report = produced_report::<E>(
            &self.canonical_rule_version,
            scope,
            ReductionOperationKindV1::Merge,
            "POLYMARKET_TERMINAL_GAMMA_VISIBLE_ONLY_NO_SOURCE_TIME",
            None,
            event.event_id.clone(),
        )?;
        let state_evidence =
            evidence_for_terminal_state(&self.identity_source_evidence, &terminal_evidence);
        let lifecycle_event = qualified_terminal_lifecycle_event(
            &self.market,
            &self.assets,
            &self.canonical_rule_version,
            state_evidence.clone(),
        )?;
        let lifecycle_report = produced_report::<E>(
            &self.canonical_rule_version,
            format!("polymarket_market:{}", self.market.condition_id.0),
            ReductionOperationKindV1::Overwrite,
            "POLYMARKET_TERMINAL_ACTIVE_TO_RETIRED",
            None,
            lifecycle_event.event_id.clone(),
        )?;
        let trading_state = metadata_trading_state(&metadata.row)
            .expect("qualified Gamma terminal carries independent trading state fields");
        let trading_event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            state_evidence,
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::MarketTradingState(trading_state),
        )?;
        let trading_report = produced_report::<E>(
            &self.canonical_rule_version,
            format!("polymarket_market:{}", self.market.condition_id.0),
            ReductionOperationKindV1::Overwrite,
            "POLYMARKET_TERMINAL_TRADING_STATE",
            None,
            trading_event.event_id.clone(),
        )?;
        Ok(PolymarketReductionBatch {
            events: vec![event, lifecycle_event, trading_event],
            reports: vec![report, lifecycle_report, trading_report],
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolymarketLevelState {
    price: ExactDecimal,
    quantity: ExactDecimal,
    direct_evidence: SourceEvidenceRefV2,
    visibility_frontier_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolymarketAbsenceState {
    direct_evidence: SourceEvidenceRefV2,
    visibility_frontier_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolymarketBbaState {
    best_bid: Option<ExactDecimal>,
    best_ask: Option<ExactDecimal>,
    direct_evidence: SourceEvidenceRefV2,
    visibility_frontier_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug)]
struct PolymarketAssetState {
    context: PolymarketAssetContextV1,
    has_initial_snapshot: bool,
    book_ready: bool,
    tick_size: ExactDecimal,
    tick_direct_evidence: Vec<SourceEvidenceRefV2>,
    tick_visibility_frontier_evidence: SourceEvidenceRefV2,
    bids: BTreeMap<Decimal, PolymarketLevelState>,
    asks: BTreeMap<Decimal, PolymarketLevelState>,
    bid_absences: BTreeMap<Decimal, PolymarketAbsenceState>,
    ask_absences: BTreeMap<Decimal, PolymarketAbsenceState>,
    l2_absence_baseline: Option<PolymarketAbsenceState>,
    bba: Option<PolymarketBbaState>,
    bba_reanchor_frontier_evidence: Option<SourceEvidenceRefV2>,
}

#[derive(Clone, Debug)]
pub struct PolymarketReducer {
    market: MarketIdentityV2,
    canonical_rule_version: String,
    identity_source_evidence: Vec<SourceEvidenceRefV2>,
    assets: BTreeMap<String, PolymarketAssetState>,
    current_epoch: Option<String>,
    seen_epochs: BTreeSet<String>,
    last_sort_key: Option<(i64, u64)>,
    seen_admission_sequences: BTreeSet<u64>,
    last_row: Option<PolymarketOrderbookRow>,
    last_evidence: Option<SourceEvidenceRefV2>,
}

impl PolymarketReducer {
    pub fn new(
        context: PolymarketMarketContextV1,
        canonical_rule_version: impl Into<String>,
    ) -> Result<Self, ContractError> {
        let (market, canonical_rule_version, contexts, identity_source_evidence) =
            prepare_polymarket_context(context, canonical_rule_version.into())?;
        let identity_visibility_frontier =
            visibility_frontier_evidence(identity_source_evidence.clone())?;
        let assets = contexts
            .into_iter()
            .map(|asset_context| {
                (
                    asset_context.asset.token_id.0.clone(),
                    PolymarketAssetState {
                        tick_size: asset_context.initial_tick_size_pusd_per_share.clone(),
                        tick_direct_evidence: identity_source_evidence.clone(),
                        tick_visibility_frontier_evidence: identity_visibility_frontier.clone(),
                        context: asset_context,
                        has_initial_snapshot: false,
                        book_ready: false,
                        bids: BTreeMap::new(),
                        asks: BTreeMap::new(),
                        bid_absences: BTreeMap::new(),
                        ask_absences: BTreeMap::new(),
                        l2_absence_baseline: None,
                        bba: None,
                        bba_reanchor_frontier_evidence: None,
                    },
                )
            })
            .collect();
        Ok(Self {
            market,
            canonical_rule_version,
            identity_source_evidence,
            assets,
            current_epoch: None,
            seen_epochs: BTreeSet::new(),
            last_sort_key: None,
            seen_admission_sequences: BTreeSet::new(),
            last_row: None,
            last_evidence: None,
        })
    }

    /// 返回会随输入增长、并由生产编排层实施硬上限的保留集合条目数。
    pub fn retained_state_entries(&self) -> usize {
        self.assets.values().fold(
            self.seen_epochs
                .len()
                .saturating_add(self.seen_admission_sequences.len()),
            |total, state| {
                total
                    .saturating_add(state.bids.len())
                    .saturating_add(state.asks.len())
                    .saturating_add(state.bid_absences.len())
                    .saturating_add(state.ask_absences.len())
                    .saturating_add(usize::from(state.l2_absence_baseline.is_some()))
                    .saturating_add(usize::from(state.bba_reanchor_frontier_evidence.is_some()))
            },
        )
    }

    pub fn reduce_orderbook_group(
        &mut self,
        mut inputs: Vec<PolymarketOrderbookInputV1>,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        if inputs.is_empty() {
            return Err(invalid("inputs", "must not be empty"));
        }
        for input in &inputs {
            validate_polymarket_input(input, &self.market)?;
        }
        let epoch = inputs[0].row.source_sequence_epoch_id.clone();
        if inputs
            .iter()
            .any(|input| input.row.source_sequence_epoch_id != epoch)
        {
            return Err(invalid(
                "inputs.source_sequence_epoch_id",
                "one reduction group cannot compare admission coordinates across epochs",
            ));
        }
        inputs.sort_by(|left, right| {
            polymarket_sort_key(&left.row).cmp(&polymarket_sort_key(&right.row))
        });

        let mut batch = PolymarketReductionBatch {
            events: Vec::new(),
            reports: Vec::new(),
        };
        if self.current_epoch.as_deref() != Some(epoch.as_str()) {
            if self.seen_epochs.contains(&epoch) {
                return Err(invalid(
                    "source_sequence_epoch_id",
                    "a completed epoch cannot be resumed after another epoch began",
                ));
            }
            // 代际排序/去重仍属旧外围；断开与恢复的证据计算复用共同组件。
            let context = self.common_context();
            let trigger = &inputs[0].evidence;
            let reset = crate::pm_reduction::reduce_epoch(
                &context,
                self,
                crate::pm_reduction::PmMessageInput {
                    fact_id: CanonicalEventId(
                        hash_domain_bytes(
                            b"data-pipeline/canonical-pm-legacy-epoch/v1\0",
                            &stable_json(&semantic_source_evidence(trigger))?,
                        )
                        .0,
                    ),
                    payload: None,
                    source: trigger,
                    source_at: inputs[0].row.timestamp.expect("validated source time"),
                    admission: trigger
                        .collector_admission
                        .as_ref()
                        .expect("validated admission"),
                },
            )?;
            self.current_epoch = Some(epoch.clone());
            self.seen_epochs.insert(epoch);
            self.last_sort_key = None;
            self.seen_admission_sequences.clear();
            self.last_row = None;
            self.last_evidence = None;
            self.apply_common_reduction(reset, &mut batch)?;
        }

        for input in inputs {
            let key = polymarket_sort_key(&input.row);
            if let Some(previous) = self.last_sort_key {
                if key < previous {
                    return Err(invalid(
                        "inputs",
                        "source-time groups must not move backwards within one epoch",
                    ));
                }
                if key == previous {
                    if self.last_row.as_ref() != Some(&input.row) {
                        return Err(invalid(
                            "collector_admission_sequence",
                            "the same epoch, source time, and admission key has conflicting messages",
                        ));
                    }
                    batch.reports.push(duplicate_suppressed_report(
                        &self.canonical_rule_version,
                        polymarket_message_scope(&input.row),
                        "POLYMARKET_IDENTICAL_MESSAGE_RETRY",
                        Vec::new(),
                        vec![
                            self.last_evidence
                                .as_ref()
                                .expect("duplicate key follows prior evidence")
                                .clone(),
                        ],
                        vec![input.evidence],
                    )?);
                    continue;
                }
            }
            if !self
                .seen_admission_sequences
                .insert(input.row.collector_admission_sequence)
            {
                return Err(invalid(
                    "collector_admission_sequence",
                    "must identify only one Polymarket message within one source epoch",
                ));
            }
            self.last_sort_key = Some(key);
            self.last_row = Some(input.row.clone());
            self.last_evidence = Some(input.evidence.clone());
            self.apply_polymarket_input(input, &mut batch)?;
        }
        Ok(batch)
    }

    fn common_context(&self) -> crate::pm_reduction::PmMarketContextV1<SourceEvidenceRefV2> {
        crate::pm_reduction::PmMarketContextV1 {
            market: self.market.clone(),
            assets: self
                .assets
                .values()
                .map(|state| state.context.clone())
                .collect(),
            identity_source_evidence: self.identity_source_evidence.clone(),
            canonical_rule_version: self.canonical_rule_version.clone(),
        }
    }

    fn apply_polymarket_input(
        &mut self,
        input: PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        use crate::pm_reduction::*;
        let context = self.common_context();
        let payload = crate::pm_message::payload_from_typed_raw(&input.row, &context.assets)?;
        let admission = input
            .evidence
            .collector_admission
            .as_ref()
            .expect("validated admission");
        let message = PmMessageInput {
            fact_id: CanonicalEventId(
                hash_domain_bytes(
                    b"data-pipeline/canonical-pm-legacy-message/v1\0",
                    &stable_json(&(&semantic_source_evidence(&input.evidence), &payload))?,
                )
                .0,
            ),
            payload: Some(&payload),
            source: &input.evidence,
            source_at: input.row.timestamp.expect("validated source time"),
            admission,
        };
        let reduction = reduce_message(&context, self, message, false)?;
        self.apply_common_reduction(reduction, batch)
    }

    fn apply_common_reduction(
        &mut self,
        reduction: crate::pm_reduction::PmMessageReductionV1<'_, SourceEvidenceRefV2>,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        use crate::pm_reduction::{PmReductionDispositionV2, PmStepChangeV1};
        let mut event_ids = BTreeMap::new();
        for step in reduction.steps {
            let payload = match step.change {
                PmStepChangeV1::InitialSnapshot {
                    asset,
                    initial_tick_size_pusd_per_share,
                    source_sequence_epoch_id,
                    source_cursor,
                    book_digest_sha256,
                    bids,
                    asks,
                    evidence,
                } => {
                    let state = self
                        .assets
                        .get_mut(&asset.token_id.0)
                        .expect("resolved asset");
                    state.has_initial_snapshot = true;
                    state.book_ready = true;
                    for (target, levels) in [
                        (&mut state.bids, bids.as_ref()),
                        (&mut state.asks, asks.as_ref()),
                    ] {
                        target.clear();
                        for level in levels {
                            target.insert(
                                parse_canonical_decimal(&level.price_pusd_per_share, "price")?,
                                PolymarketLevelState {
                                    price: level.price_pusd_per_share.clone(),
                                    quantity: level.quantity_shares.clone(),
                                    direct_evidence: evidence.direct.clone(),
                                    visibility_frontier_evidence: evidence.frontier.clone(),
                                },
                            );
                        }
                    }
                    Some(ExecutionMarketEventPayloadV3::InitialL2Snapshot(
                        InitialL2SnapshotV2 {
                            asset,
                            initial_tick_size_pusd_per_share,
                            source_sequence_epoch_id,
                            source_cursor,
                            book_digest_sha256,
                            bids: polymarket_levels(&state.bids, true),
                            asks: polymarket_levels(&state.asks, false),
                        },
                    ))
                }
                PmStepChangeV1::LevelChange {
                    transition,
                    evidence,
                } => {
                    let state = self
                        .assets
                        .get_mut(&transition.asset.token_id.0)
                        .expect("resolved asset");
                    let (levels, absences) = match transition.side {
                        BookSide::Bid => (&mut state.bids, &mut state.bid_absences),
                        BookSide::Ask => (&mut state.asks, &mut state.ask_absences),
                    };
                    let key = parse_canonical_decimal(&transition.price_pusd_per_share, "price")?;
                    if parse_canonical_decimal(&transition.quantity_after_shares, "quantity_after")?
                        .is_zero()
                    {
                        levels.remove(&key);
                        absences.insert(
                            key,
                            PolymarketAbsenceState {
                                direct_evidence: evidence.direct,
                                visibility_frontier_evidence: evidence.frontier,
                            },
                        );
                    } else {
                        absences.remove(&key);
                        levels.insert(
                            key,
                            PolymarketLevelState {
                                price: transition.price_pusd_per_share.clone(),
                                quantity: transition.quantity_after_shares.clone(),
                                direct_evidence: evidence.direct,
                                visibility_frontier_evidence: evidence.frontier,
                            },
                        );
                    }
                    Some(ExecutionMarketEventPayloadV3::LevelExecutionTransition(
                        transition,
                    ))
                }
                PmStepChangeV1::LevelEvidenceOnly {
                    asset,
                    side,
                    price,
                    evidence,
                } => {
                    let state = self
                        .assets
                        .get_mut(&asset.token_id.0)
                        .expect("resolved asset");
                    let levels = match side {
                        BookSide::Bid => &mut state.bids,
                        BookSide::Ask => &mut state.asks,
                    };
                    levels.insert(
                        parse_canonical_decimal(&price, "price")?,
                        PolymarketLevelState {
                            price,
                            quantity: ExactDecimal("0".to_owned()),
                            direct_evidence: evidence.direct,
                            visibility_frontier_evidence: evidence.frontier,
                        },
                    );
                    None
                }
                PmStepChangeV1::AbsenceBaseline {
                    asset, evidence, ..
                } => {
                    self.assets
                        .get_mut(&asset.token_id.0)
                        .expect("resolved asset")
                        .l2_absence_baseline = Some(PolymarketAbsenceState {
                        direct_evidence: evidence.direct,
                        visibility_frontier_evidence: evidence.frontier,
                    });
                    None
                }
                PmStepChangeV1::BookReady { asset } => {
                    self.assets
                        .get_mut(&asset.token_id.0)
                        .expect("resolved asset")
                        .book_ready = true;
                    None
                }
                PmStepChangeV1::BestBidAsk { quote, evidence } => {
                    let state = self
                        .assets
                        .get_mut(&quote.asset.token_id.0)
                        .expect("resolved asset");
                    state.bba = Some(PolymarketBbaState {
                        best_bid: quote.best_bid_pusd_per_share.clone(),
                        best_ask: quote.best_ask_pusd_per_share.clone(),
                        direct_evidence: evidence.direct,
                        visibility_frontier_evidence: evidence.frontier,
                    });
                    state.bba_reanchor_frontier_evidence = None;
                    Some(ExecutionMarketEventPayloadV3::BestBidAsk(quote))
                }
                PmStepChangeV1::Tick { tick, evidence } => {
                    let state = self
                        .assets
                        .get_mut(&tick.asset.token_id.0)
                        .expect("resolved asset");
                    state.tick_size = tick.new_tick_size_pusd_per_share.clone();
                    state.tick_direct_evidence = vec![evidence.direct];
                    state.tick_visibility_frontier_evidence = evidence.frontier;
                    Some(ExecutionMarketEventPayloadV3::TickSizeChange(tick))
                }
                PmStepChangeV1::PublicTrade { trade } => {
                    Some(ExecutionMarketEventPayloadV3::PublicTrade(trade))
                }
                PmStepChangeV1::Quality { boundary } => {
                    Some(ExecutionMarketEventPayloadV3::QualityBoundary(boundary))
                }
                PmStepChangeV1::ResetAsset {
                    asset,
                    absence_baseline,
                    bba_reanchor_frontier,
                } => {
                    let state = self
                        .assets
                        .get_mut(&asset.token_id.0)
                        .expect("resolved asset");
                    state.book_ready = false;
                    state.bids.clear();
                    state.asks.clear();
                    state.bid_absences.clear();
                    state.ask_absences.clear();
                    state.bba = None;
                    state.l2_absence_baseline =
                        absence_baseline.map(|value| PolymarketAbsenceState {
                            direct_evidence: value.direct,
                            visibility_frontier_evidence: value.frontier,
                        });
                    state.bba_reanchor_frontier_evidence = bba_reanchor_frontier;
                    None
                }
                PmStepChangeV1::Epoch { .. }
                | PmStepChangeV1::Selection { .. }
                | PmStepChangeV1::PublicConfirmation { .. } => None,
            };
            if let Some(payload) = payload {
                let event = finalize_execution_event(
                    self.market.clone(),
                    &self.canonical_rule_version,
                    step.dependencies,
                    step.quality,
                    payload,
                )?;
                event_ids.insert(step.derived_id.0, event.event_id.clone());
                batch.events.push(event);
            }
        }
        for report in reduction.reports {
            batch.reports.push(match report.disposition {
                PmReductionDispositionV2::Produced { derived_id } => {
                    produced_report::<SourceEvidenceRefV2>(
                        &self.canonical_rule_version,
                        report.scope,
                        report.operation,
                        &report.reason_code,
                        report.quantity_conservation,
                        event_ids
                            .remove(&derived_id.0)
                            .expect("produced report points to its execution step"),
                    )?
                }
                PmReductionDispositionV2::NoOutput { operation_evidence } => non_event_report(
                    &self.canonical_rule_version,
                    report.scope,
                    report.operation,
                    &report.reason_code,
                    operation_evidence
                        .into_iter()
                        .map(|value| value.source_evidence)
                        .collect(),
                    false,
                )?,
                PmReductionDispositionV2::Suppressed { operation_evidence } => non_event_report(
                    &self.canonical_rule_version,
                    report.scope,
                    report.operation,
                    &report.reason_code,
                    operation_evidence
                        .into_iter()
                        .map(|value| value.source_evidence)
                        .collect(),
                    true,
                )?,
            });
        }
        Ok(())
    }
}

impl crate::pm_reduction::PmStateReadV1<SourceEvidenceRefV2> for PolymarketReducer {
    fn market(&self) -> &MarketIdentityV2 {
        &self.market
    }
    fn current_epoch(&self) -> Option<&str> {
        self.current_epoch.as_deref()
    }
    fn asset_state(
        &self,
        token: &str,
    ) -> Option<crate::pm_reduction::PmAssetReadV1<'_, SourceEvidenceRefV2>> {
        use crate::pm_reduction::*;
        let state = self.assets.get(token)?;
        Some(PmAssetReadV1 {
            has_initial_snapshot: state.has_initial_snapshot,
            book_ready: state.book_ready,
            tick_size: &state.tick_size,
            tick_direct_evidence: &state.tick_direct_evidence,
            tick_frontier: &state.tick_visibility_frontier_evidence,
            absence_baseline: state
                .l2_absence_baseline
                .as_ref()
                .map(|value| PmEvidenceReadV1 {
                    direct: &value.direct_evidence,
                    frontier: &value.visibility_frontier_evidence,
                }),
            selection_baseline: None,
            bba: state.bba.as_ref().map(|value| PmBbaReadV1 {
                best_bid: &value.best_bid,
                best_ask: &value.best_ask,
                evidence: PmEvidenceReadV1 {
                    direct: &value.direct_evidence,
                    frontier: &value.visibility_frontier_evidence,
                },
            }),
            bba_reanchor_frontier: state.bba_reanchor_frontier_evidence.as_ref(),
        })
    }
    fn quantity(&self, token: &str, side: BookSide, price: Decimal) -> Decimal {
        self.assets
            .get(token)
            .and_then(|state| {
                match side {
                    BookSide::Bid => &state.bids,
                    BookSide::Ask => &state.asks,
                }
                .get(&price)
            })
            .map(|value| {
                parse_canonical_decimal(&value.quantity, "quantity")
                    .expect("retained canonical quantity")
            })
            .unwrap_or(Decimal::ZERO)
    }
    fn level_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<crate::pm_reduction::PmEvidenceReadV1<'_, SourceEvidenceRefV2>> {
        let state = self.assets.get(token)?;
        let value = match side {
            BookSide::Bid => &state.bids,
            BookSide::Ask => &state.asks,
        }
        .get(&price)?;
        Some(crate::pm_reduction::PmEvidenceReadV1 {
            direct: &value.direct_evidence,
            frontier: &value.visibility_frontier_evidence,
        })
    }
    fn absence_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<crate::pm_reduction::PmEvidenceReadV1<'_, SourceEvidenceRefV2>> {
        let state = self.assets.get(token)?;
        let value = match side {
            BookSide::Bid => &state.bid_absences,
            BookSide::Ask => &state.ask_absences,
        }
        .get(&price)?;
        Some(crate::pm_reduction::PmEvidenceReadV1 {
            direct: &value.direct_evidence,
            frontier: &value.visibility_frontier_evidence,
        })
    }
    fn selected_level(
        &self,
        _: &str,
        _: BookSide,
        _: Decimal,
    ) -> Option<&crate::pm_reduction::PmSelectedLevelV1<SourceEvidenceRefV2>> {
        None
    }
    fn level_keys<'a>(
        &'a self,
        token: &'a str,
        side: BookSide,
    ) -> Box<dyn Iterator<Item = Decimal> + 'a> {
        match self.assets.get(token) {
            Some(state) => {
                let (levels, absences) = match side {
                    BookSide::Bid => (&state.bids, &state.bid_absences),
                    BookSide::Ask => (&state.asks, &state.ask_absences),
                };
                Box::new(levels.keys().chain(absences.keys()).copied())
            }
            None => Box::new(std::iter::empty()),
        }
    }
}

fn validate_polymarket_input(
    input: &PolymarketOrderbookInputV1,
    market: &MarketIdentityV2,
) -> Result<(), ContractError> {
    input.row.validate().map_err(typed_raw_contract_error)?;
    validate_source_evidence(std::slice::from_ref(&input.evidence))?;
    if input.row.market != market.condition_id.0 {
        return Err(invalid(
            "row.market",
            "does not match the registered condition ID",
        ));
    }
    let expected_admission = input
        .evidence
        .collector_admission
        .as_ref()
        .ok_or_else(|| invalid("evidence.collector_admission", "is required"))?;
    if input.evidence.source_kind != CanonicalSourceKind::PolymarketOrderbook
        || input.evidence.source_native_identity.is_some()
        || input.evidence.source_event_at_utc_ms != input.row.timestamp
        || input.evidence.received_at_utc_ms != input.row.timestamp_received
        || expected_admission.source_sequence_epoch_id != input.row.source_sequence_epoch_id
        || expected_admission.collector_admission_sequence != input.row.collector_admission_sequence
        || expected_admission.admitted_at_utc_ms != input.row.collector_admission_time_utc_ms
    {
        return Err(invalid(
            "evidence",
            "does not match the orderbook row's frozen source-time and admission mapping",
        ));
    }
    Ok(())
}

fn validate_polymarket_metadata_input(
    input: &PolymarketMetadataInputV1,
) -> Result<(), ContractError> {
    input.row.validate().map_err(typed_raw_contract_error)?;
    let expected = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(input.row.clone())),
        &input.evidence.input_manifest_sha256,
        &input.evidence.input_file_sha256,
    )?;
    if input.evidence != expected {
        return Err(invalid(
            "evidence",
            "does not match the metadata row's frozen source mapping",
        ));
    }
    Ok(())
}

fn validate_identity_snapshot<E: crate::pm_reduction::PmEvidenceV1>(
    input: &PolymarketMetadataInputV1<E>,
) -> Result<(), ContractError> {
    use typed_raw::{
        MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin, PolymarketMetadataOrigin,
        PolymarketMetadataRowKind,
    };

    let source_pair_matches = matches!(
        (
            input.row.metadata_origin,
            input.row.source_evidence_origin,
            input.row.source_evidence_event_type,
        ),
        (
            PolymarketMetadataOrigin::GammaMarketsDiscovery,
            Some(MetadataSourceEvidenceOrigin::GammaMarketsDiscovery),
            Some(MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse),
        ) | (
            PolymarketMetadataOrigin::GammaBySlug,
            Some(MetadataSourceEvidenceOrigin::GammaBySlug),
            Some(MetadataSourceEvidenceEventType::GammaMarketBySlugResponse),
        )
    );
    if input.row.metadata_row_kind != PolymarketMetadataRowKind::MetadataSnapshot
        || !source_pair_matches
        || input.row.source_evidence_payload_json.is_none()
        || input.row.outcomes_raw.is_none()
        || input.evidence.source_time().is_some()
    {
        return Err(invalid(
            "metadata",
            "market context requires a complete Gamma identity snapshot without fabricated source time",
        ));
    }
    Ok(())
}

fn validate_prepared_metadata_identity(
    row: &PolymarketMarketMetadataRow,
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
    outcomes: &[typed_raw::MetadataOutcomeEvidence],
) -> Result<(), ContractError> {
    if row.condition_id != market.condition_id.0
        || row.clob_market_id != market.clob_market_id.0
        || row.market_start_utc != market.window.start_at_utc_ms
        || row.market_end_utc != market.window.end_at_utc_ms
    {
        return Err(invalid(
            "metadata",
            "fixed market identity changed within one reducer",
        ));
    }
    if outcomes.len() != assets.len()
        || outcomes.iter().enumerate().any(|(index, outcome)| {
            assets.iter().all(|asset| {
                asset.token_id.0 != outcome.asset_id
                    || asset.outcome.index != u32::try_from(index).unwrap_or(u32::MAX)
                    || outcome.outcome.as_deref().is_some_and(|label| {
                        !label
                            .trim()
                            .eq_ignore_ascii_case(asset.outcome.label.trim())
                    })
            })
        })
    {
        return Err(invalid(
            "metadata.asset_ids",
            "fixed asset identity changed within one reducer",
        ));
    }
    Ok(())
}

const fn map_metadata_lifecycle(
    state: typed_raw::MetadataMarketLifecycleState,
) -> KnownMarketLifecycleState {
    match state {
        typed_raw::MetadataMarketLifecycleState::Candidate => KnownMarketLifecycleState::Candidate,
        typed_raw::MetadataMarketLifecycleState::Prefetched => {
            KnownMarketLifecycleState::Prefetched
        }
        typed_raw::MetadataMarketLifecycleState::Active => KnownMarketLifecycleState::Active,
        typed_raw::MetadataMarketLifecycleState::Retired => KnownMarketLifecycleState::Retired,
    }
}

const fn mapped_metadata_transition(
    transition: Option<typed_raw::MetadataMarketLifecycleTransition>,
) -> Option<(KnownMarketLifecycleState, KnownMarketLifecycleState)> {
    match transition {
        Some(typed_raw::MetadataMarketLifecycleTransition::PrefetchedToActive) => Some((
            KnownMarketLifecycleState::Prefetched,
            KnownMarketLifecycleState::Active,
        )),
        Some(typed_raw::MetadataMarketLifecycleTransition::CandidateToActive) => Some((
            KnownMarketLifecycleState::Candidate,
            KnownMarketLifecycleState::Active,
        )),
        Some(typed_raw::MetadataMarketLifecycleTransition::ActiveToRetired) => None,
        None => None,
    }
}

fn metadata_trading_state(row: &PolymarketMarketMetadataRow) -> Option<MarketTradingStateV2> {
    (row.active.is_some() || row.closed.is_some() || row.accepting_orders.is_some()).then_some(
        MarketTradingStateV2 {
            active: row.active,
            closed: row.closed,
            accepting_orders: row.accepting_orders,
        },
    )
}

fn evidence_for_terminal_state<E: ReductionEvidenceV1>(
    identity_evidence: &[E],
    terminal_evidence: &[E],
) -> Vec<E> {
    identity_evidence
        .iter()
        .chain(terminal_evidence)
        .cloned()
        .collect()
}

fn qualified_terminal_lifecycle_event<E: ReductionEvidenceV1>(
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
    canonical_rule_version: &str,
    evidence: Vec<E>,
) -> Result<ExecutionMarketEventV3<E::Identity, E>, ContractError> {
    finalize_execution_event(
        market.clone(),
        canonical_rule_version,
        evidence,
        fresh_quality(ResyncQuality::NotRequired),
        ExecutionMarketEventPayloadV3::MarketLifecycleTransition(MarketLifecycleTransitionV2 {
            previous_state: Some(MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            }),
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Retired,
            },
            assets: assets.to_vec(),
        }),
    )
}

fn validate_polymarket_orderbook_evidence(
    input: &PolymarketOrderbookInputV1,
) -> Result<(), ContractError> {
    input.row.validate().map_err(typed_raw_contract_error)?;
    let expected = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(input.row.clone()),
        &input.evidence.input_manifest_sha256,
        &input.evidence.input_file_sha256,
    )?;
    if input.evidence != expected {
        return Err(invalid(
            "evidence",
            "does not match the orderbook row's frozen source-time and admission mapping",
        ));
    }
    Ok(())
}

fn terminal_conflict_reason(
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
    metadata: &PolymarketMarketMetadataRow,
    orderbook: &PolymarketOrderbookRow,
    metadata_outcomes: &[typed_raw::MetadataOutcomeEvidence],
) -> Result<Option<&'static str>, ContractError> {
    use typed_raw::{
        MetadataMarketLifecycleState, MetadataMarketLifecycleTransition, MetadataMarketRuntimeRole,
        MetadataMarketRuntimeRoleTransition, MetadataResolutionEvidenceSource,
        MetadataResolutionFollowupStatus, MetadataSourceEvidenceChannel,
        MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin, MetadataTerminalClaimOwner,
        PolymarketMetadataOrigin, PolymarketMetadataRowKind,
    };

    if metadata.metadata_row_kind != PolymarketMetadataRowKind::ResolutionTerminalWs
        || metadata.metadata_origin != PolymarketMetadataOrigin::MarketResolvedWs
        || metadata.source_evidence_origin != Some(MetadataSourceEvidenceOrigin::MarketResolvedWs)
        || metadata.source_evidence_event_type
            != Some(MetadataSourceEvidenceEventType::MarketResolved)
        || metadata.source_evidence_channel != Some(MetadataSourceEvidenceChannel::Market)
        || metadata.resolution_evidence_source
            != Some(MetadataResolutionEvidenceSource::MarketResolvedWs)
        || metadata.terminal_claim_owner != Some(MetadataTerminalClaimOwner::MarketResolvedWs)
        || metadata.resolution_followup_status != Some(MetadataResolutionFollowupStatus::TerminalWs)
        || metadata.market_lifecycle_state != MetadataMarketLifecycleState::Retired
        || metadata.market_lifecycle_transition
            != Some(MetadataMarketLifecycleTransition::ActiveToRetired)
        || metadata.market_runtime_role != MetadataMarketRuntimeRole::None
        || metadata.market_runtime_role_transition
            != Some(MetadataMarketRuntimeRoleTransition::ResolutionPendingToNone)
        || metadata.closed == Some(false)
        || metadata.accepting_orders == Some(true)
    {
        return Err(invalid(
            "metadata",
            "does not represent a qualified WebSocket terminal claim",
        ));
    }

    let PolymarketOrderbookEvent::MarketResolved {
        clob_market_id,
        asset_ids,
        outcomes,
        winning_asset_id,
        winning_outcome,
        ..
    } = &orderbook.event
    else {
        return Err(invalid(
            "orderbook.event",
            "must be a market_resolved WebSocket event",
        ));
    };

    if metadata.ws_claimant_source_sequence_epoch_id.as_deref()
        != Some(orderbook.source_sequence_epoch_id.as_str())
        || metadata.ws_claimant_collector_admission_sequence
            != Some(orderbook.collector_admission_sequence)
    {
        return Ok(Some("POLYMARKET_TERMINAL_CLAIMANT_CONFLICT"));
    }
    if metadata.ws_platform_timestamp_utc != orderbook.timestamp
        || metadata
            .ws_platform_timestamp_raw
            .as_deref()
            .and_then(|value| value.trim().parse::<i64>().ok())
            != metadata.ws_platform_timestamp_utc
    {
        return Ok(Some("POLYMARKET_TERMINAL_SOURCE_TIME_CONFLICT"));
    }
    if metadata.source_evidence_received_at_utc != Some(orderbook.timestamp_received) {
        return Ok(Some("POLYMARKET_TERMINAL_RECEIVE_TIME_CONFLICT"));
    }
    if metadata.condition_id != market.condition_id.0
        || orderbook.market != market.condition_id.0
        || metadata.clob_market_id != market.clob_market_id.0
        || clob_market_id != &market.clob_market_id.0
        || metadata.market_start_utc != market.window.start_at_utc_ms
        || metadata.market_end_utc != market.window.end_at_utc_ms
    {
        return Ok(Some("POLYMARKET_TERMINAL_MARKET_IDENTITY_CONFLICT"));
    }
    if orderbook
        .slug
        .as_deref()
        .is_some_and(|slug| slug != metadata.slug)
    {
        return Ok(Some("POLYMARKET_TERMINAL_MARKET_IDENTITY_CONFLICT"));
    }

    if metadata_outcomes.len() != assets.len()
        || metadata_outcomes
            .iter()
            .enumerate()
            .any(|(index, outcome)| {
                assets.iter().all(|asset| {
                    asset.token_id.0 != outcome.asset_id
                        || asset.outcome.index != u32::try_from(index).unwrap_or(u32::MAX)
                        || outcome.outcome.as_deref().is_some_and(|label| {
                            !label
                                .trim()
                                .eq_ignore_ascii_case(asset.outcome.label.trim())
                        })
                })
            })
        || !same_decimal_id_set(
            asset_ids.iter().map(String::as_str),
            assets.iter().map(|asset| asset.token_id.0.as_str()),
        )
    {
        return Ok(Some("POLYMARKET_TERMINAL_ASSET_IDENTITY_CONFLICT"));
    }
    if !outcomes.is_empty()
        && (outcomes.len() != asset_ids.len()
            || asset_ids.iter().zip(outcomes).any(|(token_id, label)| {
                assets.iter().all(|asset| {
                    asset.token_id.0 != *token_id
                        || !asset
                            .outcome
                            .label
                            .trim()
                            .eq_ignore_ascii_case(label.trim())
                })
            }))
    {
        return Ok(Some("POLYMARKET_TERMINAL_ASSET_IDENTITY_CONFLICT"));
    }

    let Some(metadata_winner_id) = metadata.winning_asset_id.as_deref() else {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    };
    let Some(metadata_winner_outcome) = metadata.winning_outcome.as_deref() else {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    };
    let Some(context_winner) = assets
        .iter()
        .find(|asset| asset.token_id.0 == metadata_winner_id)
    else {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    };
    if winning_asset_id != metadata_winner_id
        || !winning_outcome
            .trim()
            .eq_ignore_ascii_case(metadata_winner_outcome.trim())
        || !context_winner
            .outcome
            .label
            .trim()
            .eq_ignore_ascii_case(metadata_winner_outcome.trim())
    {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    }

    if metadata.outcome_prices_raw.is_some() {
        let one_hot_matches = metadata_outcomes.iter().all(|outcome| {
            outcome.price.as_ref().is_some_and(|price| {
                price.exact.0
                    == if outcome.asset_id == metadata_winner_id {
                        "1"
                    } else {
                        "0"
                    }
            })
        });
        if !one_hot_matches {
            return Ok(Some("POLYMARKET_TERMINAL_PAYOUT_EVIDENCE_CONFLICT"));
        }
    }
    Ok(None)
}

fn gamma_terminal_conflict_reason<E: ReductionEvidenceV1>(
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
    metadata: &PolymarketMetadataInputV1<E>,
    metadata_outcomes: &[typed_raw::MetadataOutcomeEvidence],
) -> Result<Option<&'static str>, ContractError> {
    use typed_raw::{
        MetadataMarketLifecycleState, MetadataMarketLifecycleTransition, MetadataMarketRuntimeRole,
        MetadataMarketRuntimeRoleTransition, MetadataResolutionEvidenceSource,
        MetadataResolutionFollowupStatus, MetadataResolutionPollResult,
        MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin, MetadataTerminalClaimOwner,
        PolymarketMetadataOrigin, PolymarketMetadataRowKind,
    };

    let row = &metadata.row;
    if row.metadata_row_kind != PolymarketMetadataRowKind::ResolutionPollAttempt
        || row.metadata_origin != PolymarketMetadataOrigin::GammaBySlug
        || row.source_evidence_origin != Some(MetadataSourceEvidenceOrigin::GammaBySlug)
        || row.source_evidence_event_type
            != Some(MetadataSourceEvidenceEventType::GammaMarketBySlugResponse)
        || row.source_evidence_channel.is_some()
        || row.resolution_evidence_source != Some(MetadataResolutionEvidenceSource::GammaBySlug)
        || row.terminal_claim_owner != Some(MetadataTerminalClaimOwner::GammaBySlug)
        || row.resolution_poll_result != Some(MetadataResolutionPollResult::Terminal)
        || row.resolution_followup_status
            != Some(MetadataResolutionFollowupStatus::TerminalGammaFallback)
        || row.resolution_integrity_error_class.is_some()
        || row.closed != Some(true)
        || row.accepting_orders != Some(false)
        || row
            .closed_time_utc
            .is_none_or(|closed| closed < row.market_end_utc)
        || row.closed_time_check_failure_reason.is_some()
        || !row
            .uma_resolution_status
            .as_deref()
            .is_some_and(|status| status.trim().eq_ignore_ascii_case("resolved"))
        || row.market_lifecycle_state != MetadataMarketLifecycleState::Retired
        || row.market_lifecycle_transition
            != Some(MetadataMarketLifecycleTransition::ActiveToRetired)
        || row.market_runtime_role != MetadataMarketRuntimeRole::None
        || row.market_runtime_role_transition
            != Some(MetadataMarketRuntimeRoleTransition::ResolutionPendingToNone)
        || row.source_evidence_received_at_utc != Some(row.timestamp_received)
        || metadata.evidence.source_time().is_some()
    {
        return Err(invalid(
            "metadata",
            "does not represent a qualified Gamma terminal fallback",
        ));
    }

    if row.condition_id != market.condition_id.0
        || row.clob_market_id != market.clob_market_id.0
        || row.market_start_utc != market.window.start_at_utc_ms
        || row.market_end_utc != market.window.end_at_utc_ms
    {
        return Ok(Some("POLYMARKET_TERMINAL_MARKET_IDENTITY_CONFLICT"));
    }

    if metadata_outcomes.len() != assets.len()
        || metadata_outcomes
            .iter()
            .enumerate()
            .any(|(index, outcome)| {
                assets.iter().all(|asset| {
                    asset.token_id.0 != outcome.asset_id
                        || asset.outcome.index != u32::try_from(index).unwrap_or(u32::MAX)
                        || outcome.outcome.as_deref().is_some_and(|label| {
                            !label
                                .trim()
                                .eq_ignore_ascii_case(asset.outcome.label.trim())
                        })
                })
            })
    {
        return Ok(Some("POLYMARKET_TERMINAL_ASSET_IDENTITY_CONFLICT"));
    }

    let Some(winning_asset_id) = row.winning_asset_id.as_deref() else {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    };
    let Some(winning_outcome) = row.winning_outcome.as_deref() else {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    };
    let Some(context_winner) = assets
        .iter()
        .find(|asset| asset.token_id.0 == winning_asset_id)
    else {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    };
    if !context_winner
        .outcome
        .label
        .trim()
        .eq_ignore_ascii_case(winning_outcome.trim())
    {
        return Ok(Some("POLYMARKET_TERMINAL_WINNER_CONFLICT"));
    }

    if metadata_outcomes.iter().any(|outcome| {
        outcome.price.as_ref().is_none_or(|price| {
            price.exact.0
                != if outcome.asset_id == winning_asset_id {
                    "1"
                } else {
                    "0"
                }
        })
    }) {
        return Ok(Some("POLYMARKET_TERMINAL_PAYOUT_EVIDENCE_CONFLICT"));
    }
    Ok(None)
}

pub(crate) fn same_decimal_id_set<'a>(
    left: impl Iterator<Item = &'a str>,
    right: impl Iterator<Item = &'a str>,
) -> bool {
    let mut left = left.collect::<Vec<_>>();
    let mut right = right.collect::<Vec<_>>();
    left.sort_by(|a, b| compare_decimal_ids(a, b));
    right.sort_by(|a, b| compare_decimal_ids(a, b));
    left == right
}

fn compare_decimal_ids(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn polymarket_sort_key(row: &PolymarketOrderbookRow) -> (i64, u64) {
    (
        row.timestamp
            .expect("validated Polymarket evidence requires source time"),
        row.collector_admission_sequence,
    )
}

fn polymarket_levels(
    levels: &BTreeMap<Decimal, PolymarketLevelState>,
    descending: bool,
) -> Vec<PriceLevelV1> {
    let map = |level: &PolymarketLevelState| PriceLevelV1 {
        price_pusd_per_share: level.price.clone(),
        quantity_shares: level.quantity.clone(),
    };
    if descending {
        levels.values().rev().map(map).collect()
    } else {
        levels.values().map(map).collect()
    }
}

fn fresh_quality(resync: ResyncQuality) -> EventQualityV2 {
    EventQualityV2 {
        availability: MarketDataQuality::Fresh,
        resync,
    }
}

fn polymarket_message_scope(row: &PolymarketOrderbookRow) -> String {
    format!(
        "polymarket_message:{}:{}:{}",
        row.market, row.source_sequence_epoch_id, row.collector_admission_sequence
    )
}

/// 仅支持既有 V2 和已准备的 V2/V3 证据，不改变业务归约分支。
pub trait ReductionEvidenceV1: crate::pm_reduction::PmEvidenceV1 {
    type Identity: Clone + std::fmt::Debug + PartialEq + Eq;
    fn execution_identity(
        event: &ExecutionMarketEventV3<(), Self>,
    ) -> Result<Self::Identity, ContractError>;
    fn reference_identity(
        event: &CanonicalReferenceEventV1<(), Self>,
    ) -> Result<Self::Identity, ContractError>;
    fn canonicalize_operations(
        values: Vec<ReductionOperationEvidenceV1<Self>>,
    ) -> Result<Vec<ReductionOperationEvidenceV1<Self>>, ContractError>;
    fn validate_operations(
        values: &[ReductionOperationEvidenceV1<Self>],
    ) -> Result<(), ContractError>;
    fn validate_identity(identity: &Self::Identity) -> Result<(), ContractError>;
}

impl ReductionEvidenceV1 for SourceEvidenceRefV2 {
    type Identity = CanonicalEventId;
    fn execution_identity(
        event: &ExecutionMarketEventV3<(), Self>,
    ) -> Result<Self::Identity, ContractError> {
        derive_execution_market_event_v3_id(event)
    }

    fn reference_identity(
        event: &CanonicalReferenceEventV1<(), Self>,
    ) -> Result<Self::Identity, ContractError> {
        derive_reference_event_v1_id(event)
    }
    fn canonicalize_operations(
        values: Vec<ReductionOperationEvidenceV1<Self>>,
    ) -> Result<Vec<ReductionOperationEvidenceV1<Self>>, ContractError> {
        canonicalize_operation_evidence(values)
    }
    fn validate_operations(
        values: &[ReductionOperationEvidenceV1<Self>],
    ) -> Result<(), ContractError> {
        validate_operation_evidence(values)
    }
    fn validate_identity(identity: &Self::Identity) -> Result<(), ContractError> {
        validate_event_id(identity, "result_event_id")
    }
}

/// 只在同次结果内关联事实与报告。材料由事件本身唯一持有，不复制完整载荷。
#[derive(Clone, Debug)]
pub struct PendingBusinessIdentityV1(std::sync::Arc<()>);
impl PartialEq for PendingBusinessIdentityV1 {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for PendingBusinessIdentityV1 {}

impl<E: crate::pm_reduction::PmEvidenceV1> ReductionEvidenceV1
    for crate::pm_reduction::PreparedPmEvidenceV1<E>
{
    type Identity = PendingBusinessIdentityV1;
    fn execution_identity(
        _: &ExecutionMarketEventV3<(), Self>,
    ) -> Result<Self::Identity, ContractError> {
        Ok(PendingBusinessIdentityV1(std::sync::Arc::new(())))
    }
    fn reference_identity(
        _: &CanonicalReferenceEventV1<(), Self>,
    ) -> Result<Self::Identity, ContractError> {
        Ok(PendingBusinessIdentityV1(std::sync::Arc::new(())))
    }
    fn canonicalize_operations(
        mut values: Vec<ReductionOperationEvidenceV1<Self>>,
    ) -> Result<Vec<ReductionOperationEvidenceV1<Self>>, ContractError> {
        values.sort_by(|a, b| {
            a.source_evidence
                .operation_key(a.role)
                .cmp(b.source_evidence.operation_key(b.role))
        });
        values.dedup_by(|a, b| {
            a.source_evidence.operation_key(a.role) == b.source_evidence.operation_key(b.role)
        });
        Self::validate_operations(&values)?;
        Ok(values)
    }
    fn validate_operations(
        values: &[ReductionOperationEvidenceV1<Self>],
    ) -> Result<(), ContractError> {
        if values.is_empty() {
            return Err(invalid("operation_evidence", "must not be empty"));
        }
        Ok(())
    }
    fn validate_identity(_: &Self::Identity) -> Result<(), ContractError> {
        Ok(())
    }
}

fn finalize_execution_event<E: ReductionEvidenceV1>(
    market: MarketIdentityV2,
    canonical_rule_version: &str,
    evidence: Vec<E>,
    quality: EventQualityV2,
    payload: ExecutionMarketEventPayloadV3,
) -> Result<ExecutionMarketEventV3<E::Identity, E>, ContractError> {
    let evidence = canonicalize_reduction_evidence::<E>(evidence)?;
    let time = event_time_from_prepared_evidence(&evidence);
    let material = ExecutionMarketEventV3 {
        schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        event_id: (),
        market,
        time,
        lineage: EventLineageV2 {
            canonical_rule_version: canonical_rule_version.to_owned(),
            source_evidence: evidence,
        },
        quality,
        payload,
    };
    Ok(ExecutionMarketEventV3 {
        event_id: E::execution_identity(&material)?,
        schema_version: material.schema_version,
        market: material.market,
        time: material.time,
        lineage: material.lineage,
        quality: material.quality,
        payload: material.payload,
    })
}

fn finalize_reference_event<E: ReductionEvidenceV1>(
    canonical_rule_version: &str,
    evidence: Vec<E>,
    quality: EventQualityV2,
    payload: CanonicalReferenceEventPayloadV1,
) -> Result<CanonicalReferenceEventV1<E::Identity, E>, ContractError> {
    let evidence = canonicalize_reduction_evidence::<E>(evidence)?;
    let time = event_time_from_prepared_evidence(&evidence);
    let material = CanonicalReferenceEventV1 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        event_id: (),
        time,
        lineage: EventLineageV2 {
            canonical_rule_version: canonical_rule_version.to_owned(),
            source_evidence: evidence,
        },
        quality,
        payload,
    };
    Ok(CanonicalReferenceEventV1 {
        event_id: E::reference_identity(&material)?,
        schema_version: material.schema_version,
        time: material.time,
        lineage: material.lineage,
        quality: material.quality,
        payload: material.payload,
    })
}

fn event_time_from_prepared_evidence<E: crate::pm_reduction::PmEvidenceV1>(
    evidence: &[E],
) -> EventTimeV2 {
    EventTimeV2 {
        source_event_at_utc_ms: evidence.iter().filter_map(|e| e.source_time()).max(),
        received_at_utc_ms: evidence
            .iter()
            .map(|e| e.received_time())
            .max()
            .expect("canonical evidence is non-empty"),
    }
}

impl<E: ReductionEvidenceV1> BinanceDepthEpochState<E> {
    fn install_book(&mut self, book: BinanceDepthBookState<E>) {
        self.book = Some(book);
        self.awaiting_after_confirmed_gap = false;
        self.epoch_switch_prior_book = None;
        self.epoch_switch_missing_emitted = false;
    }
    fn confirm_gap(&mut self) {
        self.book = None;
        self.awaiting_after_confirmed_gap = true;
    }
}

impl<E: ReductionEvidenceV1> Default for BinanceDepthEpochState<E> {
    fn default() -> Self {
        Self {
            last_admission_sequence: None,
            book: None,
            awaiting_after_confirmed_gap: false,
            epoch_switch_prior_book: None,
            epoch_switch_missing_emitted: false,
        }
    }
}

impl BinanceDepthReducer<SourceEvidenceRefV2> {
    pub fn apply_snapshot(
        &mut self,
        row: &BinanceDepthSnapshotRow,
        input_manifest_sha256: &Sha256Hex,
        input_file_sha256: &Sha256Hex,
    ) -> Result<ReferenceReductionOutcomeV1, ContractError> {
        row.validate().map_err(typed_raw_contract_error)?;
        self.begin_epoch(&row.source_sequence_epoch_id)?;
        let evidence = source_evidence_from_typed_raw(
            &TypedRawRow::BinanceDepthSnapshot(row.clone()),
            input_manifest_sha256,
            input_file_sha256,
        )?;
        self.compute_snapshot(row, evidence)
    }
    pub fn apply_update(
        &mut self,
        row: &BinanceDepthUpdateRow,
        input_manifest_sha256: &Sha256Hex,
        input_file_sha256: &Sha256Hex,
    ) -> Result<ReferenceReductionOutcomeV1, ContractError> {
        row.validate().map_err(typed_raw_contract_error)?;
        self.begin_epoch(&row.source_sequence_epoch_id)?;
        let evidence = source_evidence_from_typed_raw(
            &TypedRawRow::BinanceDepthUpdate(row.clone()),
            input_manifest_sha256,
            input_file_sha256,
        )?;
        self.compute_update(row, evidence)
    }
}

impl BinanceAggTradeReducer<SourceEvidenceRefV2> {
    pub fn apply(
        &mut self,
        row: &BinanceAggTradeRow,
        input_manifest_sha256: &Sha256Hex,
        input_file_sha256: &Sha256Hex,
    ) -> Result<ReferenceReductionOutcomeV1, ContractError> {
        row.validate().map_err(typed_raw_contract_error)?;
        let evidence = source_evidence_from_typed_raw(
            &TypedRawRow::BinanceAggTrade(row.clone()),
            input_manifest_sha256,
            input_file_sha256,
        )?;
        let payload = if matches!(row.record, BinanceAggTradeRecord::Trade { .. }) {
            Some(crate::prepare_binance_aggregate_trade_payload(row)?)
        } else {
            None
        };
        self.compute(row, evidence, payload.as_ref())
    }
}

impl PolymarketMetadataReducer<SourceEvidenceRefV2> {
    pub fn from_identity_snapshot(
        input: PolymarketMetadataInputV1,
        canonical_rule_version: impl Into<String>,
    ) -> Result<(Self, PolymarketMarketContextV1, PolymarketReductionBatch), ContractError> {
        validate_polymarket_metadata_input(&input)?;
        let canonical_rule_version = canonical_rule_version.into();
        if canonical_rule_version.trim().is_empty() {
            return Err(invalid("canonical_rule_version", "must not be empty"));
        }
        validate_identity_snapshot(&input)?;
        let outcomes = input
            .row
            .outcome_evidence()
            .map_err(typed_raw_contract_error)?;
        let initial_tick = typed_raw::extract_metadata_initial_tick(
            input
                .row
                .source_evidence_payload_json
                .as_deref()
                .expect("validated identity snapshot retains the Gamma payload"),
        )
        .map_err(typed_raw_contract_error)?;
        Self::compute_identity_snapshot(input, outcomes, initial_tick, canonical_rule_version)
    }
    pub fn reduce_metadata(
        &mut self,
        input: PolymarketMetadataInputV1,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        validate_polymarket_metadata_input(&input)?;
        let outcomes = input
            .row
            .outcome_evidence()
            .map_err(typed_raw_contract_error)?;
        self.compute_metadata(input, &outcomes, false)
    }
}

fn prepare_polymarket_context<E: ReductionEvidenceV1>(
    context: PolymarketMarketContextV1<E>,
    canonical_rule_version: String,
) -> Result<
    (
        MarketIdentityV2,
        String,
        Vec<PolymarketAssetContextV1>,
        Vec<E>,
    ),
    ContractError,
> {
    if canonical_rule_version.trim().is_empty() {
        return Err(invalid("canonical_rule_version", "must not be empty"));
    }
    validate_market(&context.market, "market")?;
    if context.assets.len() != 2 {
        return Err(invalid(
            "assets",
            "btc-updown market context requires exactly two assets",
        ));
    }
    let identity_source_evidence =
        canonicalize_reduction_evidence::<E>(context.identity_source_evidence)?;
    if identity_source_evidence
        .iter()
        .any(|evidence| !evidence.is_metadata_identity())
    {
        return Err(invalid(
            "identity_source_evidence",
            "market identity and initial tick context requires non-WS metadata evidence without a fabricated platform time or admission coordinate",
        ));
    }

    let mut outcome_indices = BTreeSet::new();
    let mut assets = BTreeMap::new();
    for asset_context in context.assets {
        crate::validate_asset(&asset_context.asset, "assets.asset")?;
        if !outcome_indices.insert(asset_context.asset.outcome.index) {
            return Err(invalid("assets", "outcome indices must be unique"));
        }
        let tick = parse_canonical_decimal(
            &asset_context.initial_tick_size_pusd_per_share,
            "assets.initial_tick_size_pusd_per_share",
        )?;
        if tick <= Decimal::ZERO {
            return Err(invalid(
                "assets.initial_tick_size_pusd_per_share",
                "must be positive",
            ));
        }
        let token_id = asset_context.asset.token_id.0.clone();
        if assets.insert(token_id, asset_context).is_some() {
            return Err(invalid("assets", "token IDs must be unique"));
        }
    }
    Ok((
        context.market,
        canonical_rule_version,
        assets.into_values().collect(),
        identity_source_evidence,
    ))
}

impl PolymarketTerminalMerger<SourceEvidenceRefV2> {
    pub fn merge_ws_terminal(
        &mut self,
        metadata: PolymarketMetadataInputV1,
        orderbook: PolymarketOrderbookInputV1,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        validate_polymarket_metadata_input(&metadata)?;
        validate_polymarket_orderbook_evidence(&orderbook)?;
        let outcomes = metadata
            .row
            .outcome_evidence()
            .map_err(typed_raw_contract_error)?;
        self.compute_ws_terminal(metadata, &outcomes, orderbook)
    }
    pub fn merge_gamma_terminal(
        &mut self,
        metadata: PolymarketMetadataInputV1,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        validate_polymarket_metadata_input(&metadata)?;
        let outcomes = metadata
            .row
            .outcome_evidence()
            .map_err(typed_raw_contract_error)?;
        self.compute_gamma_terminal(metadata, &outcomes)
    }
}

/// V2 有限隐藏准备也可以延后物化，保留原物理绑定和原身份域。
pub fn materialize_legacy_reference_reduction_v1(
    outcome: ReferenceReductionOutcomeV1<
        crate::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV2>,
        PendingBusinessIdentityV1,
    >,
) -> Result<ReferenceReductionOutcomeV1, ContractError> {
    let event = outcome
        .event
        .map(|event| {
            finalize_reference_event_legacy(
                &event.lineage.canonical_rule_version,
                event
                    .lineage
                    .source_evidence
                    .into_iter()
                    .map(|source| source.source().clone())
                    .collect(),
                event.quality,
                event.payload,
            )
        })
        .transpose()?;
    let report = materialize_legacy_report(outcome.report, |_| {
        event
            .as_ref()
            .expect("Produced 有同次事件")
            .event_id
            .clone()
    })?;
    Ok(ReferenceReductionOutcomeV1 { event, report })
}

pub fn materialize_legacy_market_reduction_v1(
    batch: PolymarketReductionBatch<
        crate::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV2>,
        PendingBusinessIdentityV1,
    >,
) -> Result<PolymarketReductionBatch, ContractError> {
    let mut identities = Vec::with_capacity(batch.events.len());
    let events = batch
        .events
        .into_iter()
        .map(|event| {
            identities.push(event.event_id);
            finalize_execution_event_legacy(
                event.market,
                &event.lineage.canonical_rule_version,
                event
                    .lineage
                    .source_evidence
                    .into_iter()
                    .map(|source| source.source().clone())
                    .collect(),
                event.quality,
                event.payload,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let reports = batch
        .reports
        .into_iter()
        .map(|report| {
            materialize_legacy_report(report, |identity| {
                events[identities
                    .iter()
                    .position(|candidate| candidate == &identity)
                    .expect("Produced 引用同次事件")]
                .event_id
                .clone()
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PolymarketReductionBatch { events, reports })
}

fn materialize_legacy_report(
    report: ReductionReportEntryV1<
        crate::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV2>,
        PendingBusinessIdentityV1,
    >,
    identity: impl FnOnce(PendingBusinessIdentityV1) -> CanonicalEventId,
) -> Result<ReductionReportEntryV1, ContractError> {
    let operations = |items: Vec<
        ReductionOperationEvidenceV1<
            crate::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV2>,
        >,
    >| {
        items
            .into_iter()
            .map(|item| ReductionOperationEvidenceV1 {
                role: item.role,
                source_evidence: item.source_evidence.source().clone(),
            })
            .collect()
    };
    let report = ReductionReportEntryV1 {
        schema_version: report.schema_version,
        canonical_rule_version: report.canonical_rule_version,
        scope: report.scope,
        operation: report.operation,
        reason_code: report.reason_code,
        quantity_conservation: report.quantity_conservation,
        disposition: match report.disposition {
            ReductionDispositionV1::Produced { result_event_id } => {
                ReductionDispositionV1::Produced {
                    result_event_id: identity(result_event_id),
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
    };
    Ok(report)
}

#[cfg(test)]
mod prepared_evidence_tests {
    use super::*;
    use crate::pm_reduction::{PmEvidenceV1, PreparedPmEvidenceV1};

    #[test]
    fn prepared_keys_keep_v2_physical_tie_and_independent_time_maxima_without_encoding() {
        let mut first = SourceEvidenceRefV2 {
            source_kind: CanonicalSourceKind::PolymarketOrderbook,
            source_native_identity: None,
            source_event_at_utc_ms: Some(300),
            received_at_utc_ms: 200,
            collector_admission: Some(CollectorAdmissionCoordinateV1 {
                source_sequence_epoch_id: "epoch".to_owned(),
                collector_admission_sequence: 1,
                admitted_at_utc_ms: 201,
            }),
            input_manifest_sha256: Sha256Hex("a".repeat(64)),
            input_file_sha256: Sha256Hex("a".repeat(64)),
        };
        let preferred = PreparedPmEvidenceV1::prepare(first.clone(), None).unwrap();
        first.input_file_sha256 = Sha256Hex("b".repeat(64));
        let duplicate = PreparedPmEvidenceV1::prepare(first.clone(), None).unwrap();
        first.source_event_at_utc_ms = Some(100);
        first
            .collector_admission
            .as_mut()
            .unwrap()
            .collector_admission_sequence = 2;
        let other = PreparedPmEvidenceV1::prepare(first, None).unwrap();
        let original = canonicalize_source_evidence(vec![
            other.source().clone(),
            duplicate.source().clone(),
            preferred.source().clone(),
        ])
        .unwrap();
        crate::without_identity_encoding(|| {
            let actual =
                PreparedPmEvidenceV1::canonicalize(vec![other, duplicate, preferred]).unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|source| source.source().clone())
                    .collect::<Vec<_>>(),
                original
            );
            assert_eq!(
                actual
                    .iter()
                    .find(|source| source.source_time() == Some(300))
                    .unwrap()
                    .source()
                    .input_file_sha256
                    .0,
                "a".repeat(64)
            );
            let frontier = visibility_frontier_evidence(actual.clone()).unwrap();
            assert_eq!(frontier.source(), original.last().unwrap());
            let time = event_time_from_prepared_evidence(&actual);
            assert_eq!(time.source_event_at_utc_ms, Some(300));
            assert_eq!(time.received_at_utc_ms, 200);
            assert_eq!(
                time.received_at_utc_ms - time.source_event_at_utc_ms.unwrap(),
                -100
            );
            let report = duplicate_suppressed_report(
                "rule",
                "scope".to_owned(),
                "duplicate",
                vec![],
                vec![actual[0].clone()],
                vec![actual[1].clone()],
            )
            .unwrap();
            assert!(
                matches!(report.disposition, ReductionDispositionV1::Suppressed { operation_evidence } if operation_evidence.len() == 2)
            );
        });
    }

    #[test]
    fn prepared_depth_produced_and_confirmation_reports_do_not_encode() {
        let row = BinanceDepthSnapshotRow {
            timestamp_received: 2001,
            snapshot_fetch_start_utc: 900,
            last_update_id: 10,
            bids: vec![DepthLevel {
                price: "64000".to_owned(),
                quantity: "0.5".to_owned(),
            }],
            asks: vec![DepthLevel {
                price: "65000".to_owned(),
                quantity: "0.2".to_owned(),
            }],
            symbol: "BTCUSDT".to_owned(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .to_owned(),
            snapshot_status: "complete".to_owned(),
            source_sequence_epoch_id: "epoch".to_owned(),
            connection_generation: 1,
            depth_ingress_sequence: 1,
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: 2101,
            collector_host_id: "collector".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            schema_version: "binance_depth_snapshot_v3".to_owned(),
        };
        let prepared = |row: &BinanceDepthSnapshotRow| {
            let source = crate::public_reference::source_evidence_v3_from_typed_raw_v2(
                &crate::public_input::TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(
                    row.clone(),
                )),
            )
            .unwrap();
            PreparedPmEvidenceV1::prepare(source, None).unwrap()
        };
        let first = prepared(&row);
        let mut next = row.clone();
        next.collector_admission_sequence = 2;
        next.depth_ingress_sequence = 2;
        next.timestamp_received += 1;
        next.collector_admission_time_utc_ms += 1;
        let second = prepared(&next);
        let mut reducer = BinanceDepthReducer::new("rule").unwrap();
        crate::without_identity_encoding(|| {
            let first = reducer.compute_snapshot(&row, first).unwrap();
            assert!(first.event.is_some());
            let second = reducer.compute_snapshot(&next, second).unwrap();
            assert!(second.event.is_none());
            assert_eq!(
                second.report.reason_code,
                "BINANCE_DEPTH_IDENTICAL_SNAPSHOT"
            );
        });
    }
}

fn canonicalize_reduction_evidence<E: crate::pm_reduction::PmEvidenceV1>(
    values: Vec<E>,
) -> Result<Vec<E>, ContractError> {
    let values = E::canonicalize(values)?;
    crate::require_source_evidence(&values)?;
    Ok(values)
}
