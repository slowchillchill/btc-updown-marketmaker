use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use contracts::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::typed_raw::{
    self, BinanceAggTradeRecord, BinanceAggTradeRow, BinanceDepthSnapshotRow,
    BinanceDepthStitchVerdict, BinanceDepthUpdateRow, DepthLevel, PolymarketMarketMetadataRow,
    PolymarketOrderbookEvent, PolymarketOrderbookRow, PolymarketPriceChangeEntry,
    PolymarketPriceLevel, PolymarketSide, TypedRawRow,
};
use crate::{
    ContractError, build_binance_aggregate_trade_reference_event_v1,
    derive_execution_market_event_v3_id, derive_reference_event_v1_id, hash_domain_bytes, invalid,
    parse_canonical_decimal, semantic_source_evidence, source_evidence_from_typed_raw, stable_json,
    typed_raw_contract_error, validate_event_id, validate_execution_market_event_v3,
    validate_market, validate_reference_event_v1, validate_source_evidence,
};

pub const REDUCTION_REPORT_SCHEMA_V1: &str = "canonical_reduction_report_v1";

const POLYMARKET_BOOK_DIGEST_DOMAIN_V1: &[u8] = b"data-pipeline/polymarket-book-digest/v1\0";
const POLYMARKET_CAUSAL_GROUP_DOMAIN_V1: &[u8] = b"data-pipeline/polymarket-causal-group/v1\0";
const POLYMARKET_PUBLIC_TRADE_ID_DOMAIN_V1: &[u8] =
    b"data-pipeline/polymarket-public-trade-id/v1\0";
const POLYMARKET_TERMINAL_MAPPING_RULE_V1: &str = "polymarket-binary-winning-asset-one-hot-v1";
const POLYMARKET_L2_EPOCH_BASELINE_REQUIRED: &str = "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED";
const POLYMARKET_L2_EPOCH_BASELINE_RESTORED: &str = "POLYMARKET_L2_EPOCH_BASELINE_RESTORED";
const POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED: &str =
    "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED";

fn normalize_polymarket_boundary_quote(
    value: Option<&str>,
    field: &str,
    empty_sentinel: Decimal,
) -> Result<Option<ExactDecimal>, ContractError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let normalized =
        typed_raw::normalize_decimal_text(value, field).map_err(typed_raw_contract_error)?;
    if parse_canonical_decimal(&normalized, field)? == empty_sentinel {
        Ok(None)
    } else {
        Ok(Some(normalized))
    }
}

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
pub struct ReductionOperationEvidenceV1 {
    pub role: ReductionEvidenceRoleV1,
    pub source_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "disposition",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum ReductionDispositionV1 {
    Produced {
        result_event_id: CanonicalEventId,
    },
    NoOutput {
        operation_evidence: Vec<ReductionOperationEvidenceV1>,
    },
    Suppressed {
        operation_evidence: Vec<ReductionOperationEvidenceV1>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionReportEntryV1 {
    pub schema_version: String,
    pub canonical_rule_version: String,
    pub scope: String,
    pub operation: ReductionOperationKindV1,
    pub reason_code: String,
    pub quantity_conservation: Option<QuantityConservationV1>,
    pub disposition: ReductionDispositionV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceReductionOutcomeV1 {
    pub event: Option<CanonicalReferenceEventV1>,
    pub report: ReductionReportEntryV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketReductionBatch {
    pub events: Vec<ExecutionMarketEventV3>,
    pub reports: Vec<ReductionReportEntryV1>,
}

fn exact_from_decimal(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}

fn canonicalize_source_evidence(
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

fn finalize_execution_event(
    market: MarketIdentityV2,
    canonical_rule_version: &str,
    evidence: Vec<SourceEvidenceRefV2>,
    quality: EventQualityV2,
    payload: ExecutionMarketEventPayloadV3,
) -> Result<ExecutionMarketEventV3, ContractError> {
    if canonical_rule_version.trim().is_empty() {
        return Err(invalid("canonical_rule_version", "must not be empty"));
    }
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
    validate_execution_market_event_v3(&event)?;
    Ok(event)
}

fn finalize_reference_event(
    canonical_rule_version: &str,
    evidence: Vec<SourceEvidenceRefV2>,
    quality: EventQualityV2,
    payload: CanonicalReferenceEventPayloadV1,
) -> Result<CanonicalReferenceEventV1, ContractError> {
    if canonical_rule_version.trim().is_empty() {
        return Err(invalid("canonical_rule_version", "must not be empty"));
    }
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
    validate_reference_event_v1(&event)?;
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
            validate_event_id(result_event_id, "result_event_id")?;
        }
        (
            ReductionOperationKindV1::Reject,
            ReductionDispositionV1::NoOutput { operation_evidence },
        )
        | (
            ReductionOperationKindV1::Confirm | ReductionOperationKindV1::Suppress,
            ReductionDispositionV1::Suppressed { operation_evidence },
        ) => {
            validate_operation_evidence(operation_evidence)?;
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
    validate_reduction_report_entry_v1(report)?;
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

fn produced_report(
    canonical_rule_version: &str,
    scope: String,
    operation: ReductionOperationKindV1,
    reason_code: &str,
    quantity_conservation: Option<QuantityConservationV1>,
    event_id: CanonicalEventId,
) -> Result<ReductionReportEntryV1, ContractError> {
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
    validate_reduction_report_entry_v1(&report)?;
    Ok(report)
}

fn non_event_report(
    canonical_rule_version: &str,
    scope: String,
    operation: ReductionOperationKindV1,
    reason_code: &str,
    evidence: Vec<SourceEvidenceRefV2>,
    suppressed: bool,
) -> Result<ReductionReportEntryV1, ContractError> {
    let evidence = canonicalize_operation_evidence(
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
    validate_reduction_report_entry_v1(&report)?;
    Ok(report)
}

fn duplicate_suppressed_report(
    canonical_rule_version: &str,
    scope: String,
    reason_code: &str,
    context: Vec<SourceEvidenceRefV2>,
    matched_existing: Vec<SourceEvidenceRefV2>,
    incoming_duplicate: Vec<SourceEvidenceRefV2>,
) -> Result<ReductionReportEntryV1, ContractError> {
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
            operation_evidence: canonicalize_operation_evidence(evidence)?,
        },
    };
    validate_reduction_report_entry_v1(&report)?;
    Ok(report)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReferenceLevelState {
    price: ExactDecimal,
    quantity: ExactDecimal,
    direct_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug)]
struct BinanceDepthBookState {
    snapshot_last_update_id: u64,
    local_u: Option<u64>,
    bids: BTreeMap<Decimal, ReferenceLevelState>,
    asks: BTreeMap<Decimal, ReferenceLevelState>,
    deleted_bids: BTreeMap<Decimal, SourceEvidenceRefV2>,
    deleted_asks: BTreeMap<Decimal, SourceEvidenceRefV2>,
    snapshot_evidence: SourceEvidenceRefV2,
    anchor_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug, Default)]
struct BinanceDepthEpochState {
    last_admission_sequence: Option<u64>,
    book: Option<BinanceDepthBookState>,
    awaiting_after_confirmed_gap: bool,
    epoch_switch_prior_book: Option<BinanceDepthBookState>,
    epoch_switch_missing_emitted: bool,
}

#[derive(Clone, Debug)]
pub struct BinanceDepthReducer {
    canonical_rule_version: String,
    epochs: BTreeMap<String, BinanceDepthEpochState>,
    current_epoch: Option<String>,
    seen_epochs: BTreeSet<String>,
}

impl BinanceDepthReducer {
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
        state.book = Some(BinanceDepthBookState {
            snapshot_last_update_id: row.last_update_id,
            local_u: None,
            bids,
            asks,
            deleted_bids: BTreeMap::new(),
            deleted_asks: BTreeMap::new(),
            snapshot_evidence: evidence.clone(),
            anchor_evidence: evidence.clone(),
        });
        state.awaiting_after_confirmed_gap = false;
        state.epoch_switch_prior_book = None;
        state.epoch_switch_missing_emitted = false;

        let scope = format!("binance_depth:{}", row.source_sequence_epoch_id);
        if same_state {
            let mut confirmation_evidence = prior_evidence;
            confirmation_evidence.push(evidence);
            return Ok(ReferenceReductionOutcomeV1 {
                event: None,
                report: non_event_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Confirm,
                    "BINANCE_DEPTH_IDENTICAL_SNAPSHOT",
                    confirmation_evidence,
                    true,
                )?,
            });
        }
        let event = finalize_binance_depth_event(
            &self.canonical_rule_version,
            state.book.as_ref().expect("snapshot established a book"),
            MarketDataQuality::Fresh,
            resync,
            Vec::new(),
        )?;
        let report = produced_report(
            &self.canonical_rule_version,
            scope,
            ReductionOperationKindV1::Merge,
            "BINANCE_DEPTH_SNAPSHOT_BASELINE",
            None,
            event.event_id.clone(),
        )?;
        Ok(ReferenceReductionOutcomeV1 {
            event: Some(event),
            report,
        })
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
        let scope = format!("binance_depth:{}", row.source_sequence_epoch_id);

        match row.stitch_verdict {
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable => {
                if !state.epoch_switch_missing_emitted
                    && let Some(previous_book) = state.epoch_switch_prior_book.as_ref()
                {
                    let event = finalize_binance_depth_event(
                        &self.canonical_rule_version,
                        previous_book,
                        MarketDataQuality::Missing,
                        ResyncQuality::Required,
                        vec![evidence],
                    )?;
                    let report = produced_report(
                        &self.canonical_rule_version,
                        scope,
                        ReductionOperationKindV1::Reject,
                        "BINANCE_DEPTH_EPOCH_SNAPSHOT_REQUIRED",
                        None,
                        event.event_id.clone(),
                    )?;
                    state.epoch_switch_missing_emitted = true;
                    return Ok(ReferenceReductionOutcomeV1 {
                        event: Some(event),
                        report,
                    });
                }
                Ok(ReferenceReductionOutcomeV1 {
                    event: None,
                    report: non_event_report(
                        &self.canonical_rule_version,
                        scope,
                        ReductionOperationKindV1::Reject,
                        "BINANCE_DEPTH_SNAPSHOT_UNAVAILABLE",
                        vec![evidence],
                        false,
                    )?,
                })
            }
            BinanceDepthStitchVerdict::NotAppliedStaleDuplicate => {
                let matched_evidence = binance_depth_evidence(
                    state.book.as_ref().expect("stale verdict requires a book"),
                )?;
                Ok(ReferenceReductionOutcomeV1 {
                    event: None,
                    report: duplicate_suppressed_report(
                        &self.canonical_rule_version,
                        scope,
                        "BINANCE_DEPTH_STALE_DUPLICATE",
                        Vec::new(),
                        matched_evidence,
                        vec![evidence],
                    )?,
                })
            }
            BinanceDepthStitchVerdict::NotAppliedConfirmedHole => {
                let book = state.book.as_ref().expect("hole verdict requires a book");
                let event = finalize_binance_depth_event(
                    &self.canonical_rule_version,
                    book,
                    MarketDataQuality::ConfirmedGap,
                    ResyncQuality::Required,
                    vec![evidence],
                )?;
                let report = produced_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Reject,
                    "BINANCE_DEPTH_CONFIRMED_HOLE",
                    None,
                    event.event_id.clone(),
                )?;
                state.book = None;
                state.awaiting_after_confirmed_gap = true;
                Ok(ReferenceReductionOutcomeV1 {
                    event: Some(event),
                    report,
                })
            }
            BinanceDepthStitchVerdict::Applied => {
                let book = state
                    .book
                    .as_mut()
                    .expect("expected verdict requires a book");
                let (bid_summary, mut prior_evidence) = apply_reference_level_updates(
                    &mut book.bids,
                    &mut book.deleted_bids,
                    &row.bids,
                    &evidence,
                    "bids",
                )?;
                let (ask_summary, ask_prior_evidence) = apply_reference_level_updates(
                    &mut book.asks,
                    &mut book.deleted_asks,
                    &row.asks,
                    &evidence,
                    "asks",
                )?;
                prior_evidence.extend(ask_prior_evidence);
                let summary = bid_summary.combine(ask_summary)?;
                book.local_u = Some(row.final_update_id);
                book.anchor_evidence = evidence;
                let event = finalize_binance_depth_event(
                    &self.canonical_rule_version,
                    book,
                    MarketDataQuality::Fresh,
                    ResyncQuality::NotRequired,
                    prior_evidence,
                )?;
                let report = produced_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Overwrite,
                    "BINANCE_DEPTH_APPLIED_UPDATE",
                    Some(summary),
                    event.event_id.clone(),
                )?;
                Ok(ReferenceReductionOutcomeV1 {
                    event: Some(event),
                    report,
                })
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
                .and_then(|current| self.epochs.get(current))
                .map(|state| {
                    (
                        state
                            .book
                            .clone()
                            .or_else(|| state.epoch_switch_prior_book.clone()),
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

fn expected_depth_verdict(
    book: Option<&BinanceDepthBookState>,
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

fn reference_level_map(
    levels: &[DepthLevel],
    evidence: &SourceEvidenceRefV2,
    field: &str,
) -> Result<BTreeMap<Decimal, ReferenceLevelState>, ContractError> {
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

fn same_reference_levels(
    left: &BTreeMap<Decimal, ReferenceLevelState>,
    right: &BTreeMap<Decimal, ReferenceLevelState>,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_price, left), (right_price, right))| {
                left_price == right_price && left.quantity == right.quantity
            })
}

impl QuantityConservationV1 {
    fn zero() -> Self {
        Self {
            quantity_before: ExactDecimal("0".to_owned()),
            quantity_after: ExactDecimal("0".to_owned()),
            quantity_added: ExactDecimal("0".to_owned()),
            quantity_reduced: ExactDecimal("0".to_owned()),
        }
    }

    fn combine(self, other: Self) -> Result<Self, ContractError> {
        let sum = |left: &ExactDecimal, right: &ExactDecimal, field: &str| {
            parse_canonical_decimal(left, field)?
                .checked_add(parse_canonical_decimal(right, field)?)
                .map(exact_from_decimal)
                .ok_or_else(|| invalid(field, "decimal addition overflow"))
        };
        let combined = Self {
            quantity_before: sum(
                &self.quantity_before,
                &other.quantity_before,
                "quantity_before",
            )?,
            quantity_after: sum(
                &self.quantity_after,
                &other.quantity_after,
                "quantity_after",
            )?,
            quantity_added: sum(
                &self.quantity_added,
                &other.quantity_added,
                "quantity_added",
            )?,
            quantity_reduced: sum(
                &self.quantity_reduced,
                &other.quantity_reduced,
                "quantity_reduced",
            )?,
        };
        validate_quantity_conservation(&combined)?;
        Ok(combined)
    }
}

fn apply_reference_level_updates(
    levels: &mut BTreeMap<Decimal, ReferenceLevelState>,
    deleted_levels: &mut BTreeMap<Decimal, SourceEvidenceRefV2>,
    updates: &[DepthLevel],
    evidence: &SourceEvidenceRefV2,
    field: &str,
) -> Result<(QuantityConservationV1, Vec<SourceEvidenceRefV2>), ContractError> {
    let mut seen = BTreeSet::new();
    let mut summary = QuantityConservationV1::zero();
    let mut prior_evidence = Vec::new();
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
        if let Some(previous) = previous {
            prior_evidence.push(previous.direct_evidence.clone());
        } else if let Some(previous_deletion) = deleted_levels.get(&key) {
            prior_evidence.push(previous_deletion.clone());
        }
        let after = parse_canonical_decimal(&quantity, field)?;
        let (added, reduced) = match after.cmp(&before) {
            Ordering::Greater => (after - before, Decimal::ZERO),
            Ordering::Less => (Decimal::ZERO, before - after),
            Ordering::Equal => (Decimal::ZERO, Decimal::ZERO),
        };
        summary = summary.combine(QuantityConservationV1 {
            quantity_before: exact_from_decimal(before),
            quantity_after: exact_from_decimal(after),
            quantity_added: exact_from_decimal(added),
            quantity_reduced: exact_from_decimal(reduced),
        })?;
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
    Ok((summary, prior_evidence))
}

fn finalize_binance_depth_event(
    canonical_rule_version: &str,
    book: &BinanceDepthBookState,
    availability: MarketDataQuality,
    resync: ResyncQuality,
    additional_evidence: Vec<SourceEvidenceRefV2>,
) -> Result<CanonicalReferenceEventV1, ContractError> {
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
    let mut evidence = binance_depth_evidence(book)?;
    evidence.extend(additional_evidence);
    finalize_reference_event(
        canonical_rule_version,
        evidence,
        EventQualityV2 {
            availability,
            resync,
        },
        CanonicalReferenceEventPayloadV1::BinanceDepthState(BinanceDepthStateReferenceV1 {
            symbol: "BTCUSDT".to_owned(),
            last_update_id,
            bids,
            asks,
        }),
    )
}

fn binance_depth_evidence(
    book: &BinanceDepthBookState,
) -> Result<Vec<SourceEvidenceRefV2>, ContractError> {
    canonicalize_source_evidence(
        book.bids
            .values()
            .chain(book.asks.values())
            .map(|level| level.direct_evidence.clone())
            .chain(book.deleted_bids.values().cloned())
            .chain(book.deleted_asks.values().cloned())
            .chain([book.snapshot_evidence.clone(), book.anchor_evidence.clone()])
            .collect(),
    )
}

#[derive(Clone, Debug)]
pub struct BinanceAggTradeReducer {
    canonical_rule_version: String,
    current_trade: Option<(
        u64,
        i64,
        BinanceAggregateTradeReferenceV1,
        SourceEvidenceRefV2,
    )>,
}

impl BinanceAggTradeReducer {
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
        let candidate = build_binance_aggregate_trade_reference_event_v1(
            row,
            input_manifest_sha256,
            input_file_sha256,
            &self.canonical_rule_version,
        )?;
        let CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(payload) = &candidate.payload
        else {
            unreachable!("aggregate-trade builder returned another payload")
        };
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
        let report = produced_report(
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
pub struct PolymarketMarketContextV1 {
    pub market: MarketIdentityV2,
    pub assets: Vec<PolymarketAssetContextV1>,
    pub identity_source_evidence: Vec<SourceEvidenceRefV2>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookInputV1 {
    pub row: PolymarketOrderbookRow,
    pub evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMetadataInputV1 {
    pub row: PolymarketMarketMetadataRow,
    pub evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug)]
pub struct PolymarketMetadataReducer {
    market: MarketIdentityV2,
    assets: Vec<AssetIdentityV1>,
    identity_source_evidence: Vec<SourceEvidenceRefV2>,
    canonical_rule_version: String,
    current_lifecycle: KnownMarketLifecycleState,
    last_lifecycle_transition: Option<(KnownMarketLifecycleState, KnownMarketLifecycleState)>,
    current_trading_state: Option<MarketTradingStateV2>,
}

impl PolymarketMetadataReducer {
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

        let outcomes = typed_raw::map_metadata_outcomes(
            &input.row.asset_ids,
            input.row.outcomes_raw.as_deref(),
            input.row.outcome_prices_raw.as_deref(),
        )
        .map_err(typed_raw_contract_error)?;
        let payload = input
            .row
            .source_evidence_payload_json
            .as_deref()
            .expect("validated identity snapshot retains the Gamma payload");
        let initial_tick = typed_raw::extract_metadata_initial_tick(payload)
            .map_err(typed_raw_contract_error)?
            .ok_or_else(|| {
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

    pub fn reduce_metadata(
        &mut self,
        input: PolymarketMetadataInputV1,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        validate_polymarket_metadata_input(&input)?;
        validate_metadata_identity(&input.row, &self.market, &self.assets)?;
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
            return Err(invalid(
                "market_lifecycle_state",
                "changed without an explicit lifecycle transition",
            ));
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
        input_evidence: &SourceEvidenceRefV2,
        operation: ReductionOperationKindV1,
        reason_code: &str,
        batch: &mut PolymarketReductionBatch,
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
        batch.reports.push(produced_report(
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
        input_evidence: &SourceEvidenceRefV2,
        operation: ReductionOperationKindV1,
        reason_code: &str,
        batch: &mut PolymarketReductionBatch,
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
        batch.reports.push(produced_report(
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
pub struct PolymarketTerminalMerger {
    market: MarketIdentityV2,
    assets: Vec<AssetIdentityV1>,
    identity_source_evidence: Vec<SourceEvidenceRefV2>,
    canonical_rule_version: String,
    emitted_terminal: Option<MarketResolutionTerminalV3>,
    emitted_terminal_evidence: Vec<SourceEvidenceRefV2>,
    quarantined: bool,
}

impl PolymarketTerminalMerger {
    pub fn new(
        context: PolymarketMarketContextV1,
        canonical_rule_version: impl Into<String>,
    ) -> Result<Self, ContractError> {
        let reducer = PolymarketReducer::new(context, canonical_rule_version)?;
        let identity_source_evidence = reducer.identity_source_evidence.clone();
        let mut assets = reducer
            .assets
            .into_values()
            .map(|state| state.context.asset)
            .collect::<Vec<_>>();
        assets.sort_by(|left, right| compare_decimal_ids(&left.token_id.0, &right.token_id.0));
        Ok(Self {
            market: reducer.market,
            assets,
            identity_source_evidence,
            canonical_rule_version: reducer.canonical_rule_version,
            emitted_terminal: None,
            emitted_terminal_evidence: Vec::new(),
            quarantined: false,
        })
    }

    pub fn merge_ws_terminal(
        &mut self,
        metadata: PolymarketMetadataInputV1,
        orderbook: PolymarketOrderbookInputV1,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        validate_polymarket_metadata_input(&metadata)?;
        validate_polymarket_orderbook_evidence(&orderbook)?;
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

        if let Some(reason_code) =
            terminal_conflict_reason(&self.market, &self.assets, &metadata.row, &orderbook.row)?
        {
            let previous_quality = if self.emitted_terminal.is_some() {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::UnknownNotObserved
            };
            let event = finalize_execution_event(
                self.market.clone(),
                &self.canonical_rule_version,
                evidence,
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
            let report = produced_report(
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
                let conflict = finalize_execution_event(
                    self.market.clone(),
                    &self.canonical_rule_version,
                    evidence,
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
                let report = produced_report(
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
        let report = produced_report(
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
        let lifecycle_report = produced_report(
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

    pub fn merge_gamma_terminal(
        &mut self,
        metadata: PolymarketMetadataInputV1,
    ) -> Result<PolymarketReductionBatch, ContractError> {
        validate_polymarket_metadata_input(&metadata)?;
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

        if let Some(reason_code) =
            gamma_terminal_conflict_reason(&self.market, &self.assets, &metadata)?
        {
            let previous_quality = if self.emitted_terminal.is_some() {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::UnknownNotObserved
            };
            let event = finalize_execution_event(
                self.market.clone(),
                &self.canonical_rule_version,
                evidence,
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
            let report = produced_report(
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
                let conflict = finalize_execution_event(
                    self.market.clone(),
                    &self.canonical_rule_version,
                    evidence,
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
                let report = produced_report(
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
        let report = produced_report(
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
        let lifecycle_report = produced_report(
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
        let trading_report = produced_report(
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolymarketBbaState {
    best_bid: Option<ExactDecimal>,
    best_ask: Option<ExactDecimal>,
    direct_evidence: SourceEvidenceRefV2,
}

#[derive(Clone, Debug)]
struct PolymarketAssetState {
    context: PolymarketAssetContextV1,
    has_initial_snapshot: bool,
    book_ready: bool,
    tick_size: ExactDecimal,
    tick_direct_evidence: Vec<SourceEvidenceRefV2>,
    bids: BTreeMap<Decimal, PolymarketLevelState>,
    asks: BTreeMap<Decimal, PolymarketLevelState>,
    bba: Option<PolymarketBbaState>,
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
        let canonical_rule_version = canonical_rule_version.into();
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
            canonicalize_source_evidence(context.identity_source_evidence)?;
        if identity_source_evidence.iter().any(|evidence| {
            evidence.source_kind != CanonicalSourceKind::PolymarketMarketMetadata
                || evidence.source_event_at_utc_ms.is_some()
                || !matches!(
                    evidence.source_native_identity,
                    Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { .. })
                )
                || evidence.collector_admission.is_some()
        }) {
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
            if assets
                .insert(
                    token_id,
                    PolymarketAssetState {
                        tick_size: asset_context.initial_tick_size_pusd_per_share.clone(),
                        tick_direct_evidence: identity_source_evidence.clone(),
                        context: asset_context,
                        has_initial_snapshot: false,
                        book_ready: false,
                        bids: BTreeMap::new(),
                        asks: BTreeMap::new(),
                        bba: None,
                    },
                )
                .is_some()
            {
                return Err(invalid("assets", "token IDs must be unique"));
            }
        }
        Ok(Self {
            market: context.market,
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
            let switching_epoch = self.current_epoch.is_some();
            self.current_epoch = Some(epoch.clone());
            self.seen_epochs.insert(epoch);
            self.last_sort_key = None;
            self.seen_admission_sequences.clear();
            self.last_row = None;
            self.last_evidence = None;
            let mut invalidated_assets = Vec::new();
            for state in self.assets.values_mut() {
                if switching_epoch && state.book_ready {
                    invalidated_assets.push(state.context.asset.clone());
                }
                state.book_ready = false;
                state.bids.clear();
                state.asks.clear();
                state.bba = None;
            }
            for asset in invalidated_assets {
                append_polymarket_quality_boundary(
                    &self.market,
                    &self.canonical_rule_version,
                    &self.identity_source_evidence,
                    &asset,
                    MarketDataQuality::Fresh,
                    MarketDataQuality::Missing,
                    ResyncQuality::Required,
                    POLYMARKET_L2_EPOCH_BASELINE_REQUIRED,
                    ReductionOperationKindV1::Reject,
                    &inputs[0].evidence,
                    &mut batch,
                )?;
            }
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

    fn apply_polymarket_input(
        &mut self,
        input: PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        match &input.row.event {
            PolymarketOrderbookEvent::Book {
                asset_id,
                bids,
                asks,
                ..
            } => self.apply_polymarket_book(asset_id, bids, asks, &input, batch),
            PolymarketOrderbookEvent::PriceChange { price_changes } => {
                for change in price_changes {
                    self.apply_polymarket_price_change(change, &input, batch)?;
                }
                Ok(())
            }
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id,
                best_bid,
                best_ask,
                spread,
            } => self.apply_polymarket_bba(
                asset_id,
                best_bid.as_deref(),
                best_ask.as_deref(),
                spread.as_deref(),
                &input,
                batch,
            ),
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id,
                old_tick_size,
                new_tick_size,
            } => self.apply_polymarket_tick(
                asset_id,
                old_tick_size.as_deref(),
                new_tick_size,
                &input,
                batch,
            ),
            PolymarketOrderbookEvent::LastTradePrice {
                asset_id,
                price,
                size,
                side,
                fee_rate_bps,
            } => self.apply_polymarket_trade(
                asset_id,
                price,
                size.as_deref(),
                *side,
                fee_rate_bps.as_deref(),
                &input,
                batch,
            ),
            PolymarketOrderbookEvent::NewMarket {
                clob_market_id,
                asset_ids,
                outcomes,
                ..
            } => {
                self.confirm_new_market_identity(clob_market_id, asset_ids, outcomes, &input, batch)
            }
            PolymarketOrderbookEvent::MarketResolved { .. } => Err(invalid(
                "event",
                "market_resolved WebSocket rows require the qualified terminal merger",
            )),
        }
    }

    fn confirm_new_market_identity(
        &self,
        clob_market_id: &str,
        asset_ids: &[String],
        outcomes: &[String],
        input: &PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        if clob_market_id != self.market.clob_market_id.0
            || asset_ids.len() != self.assets.len()
            || !same_decimal_id_set(
                asset_ids.iter().map(String::as_str),
                self.assets.keys().map(String::as_str),
            )
            || asset_ids.iter().zip(outcomes).any(|(token_id, outcome)| {
                self.assets.get(token_id).is_none_or(|state| {
                    !state
                        .context
                        .asset
                        .outcome
                        .label
                        .trim()
                        .eq_ignore_ascii_case(outcome.trim())
                })
            })
        {
            return Err(invalid(
                "event.new_market",
                "does not match the registered fixed market identity",
            ));
        }
        let mut evidence = self.identity_source_evidence.clone();
        evidence.push(input.evidence.clone());
        batch.reports.push(non_event_report(
            &self.canonical_rule_version,
            polymarket_message_scope(&input.row),
            ReductionOperationKindV1::Confirm,
            "POLYMARKET_NEW_MARKET_IDENTITY_CONFIRMED",
            evidence,
            true,
        )?);
        Ok(())
    }

    fn apply_polymarket_book(
        &mut self,
        asset_id: &str,
        bids: &[PolymarketPriceLevel],
        asks: &[PolymarketPriceLevel],
        input: &PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        let incoming_bids = polymarket_level_map(bids, &input.evidence, "bids")?;
        let incoming_asks = polymarket_level_map(asks, &input.evidence, "asks")?;
        let state = self
            .assets
            .get_mut(asset_id)
            .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
        let scope = polymarket_asset_scope(&self.market, &state.context.asset);
        if !state.book_ready && !state.has_initial_snapshot {
            state.book_ready = true;
            state.has_initial_snapshot = true;
            state.bids = incoming_bids;
            state.asks = incoming_asks;
            let mut evidence = self.identity_source_evidence.clone();
            evidence.push(input.evidence.clone());
            let digest = polymarket_book_digest(&state.context.asset, &state.bids, &state.asks)?;
            let event = finalize_execution_event(
                self.market.clone(),
                &self.canonical_rule_version,
                evidence,
                fresh_quality(ResyncQuality::NotRequired),
                ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
                    asset: state.context.asset.clone(),
                    initial_tick_size_pusd_per_share: state.tick_size.clone(),
                    source_sequence_epoch_id: input.row.source_sequence_epoch_id.clone(),
                    source_cursor: input.row.collector_admission_sequence,
                    book_digest_sha256: digest,
                    bids: polymarket_levels(&state.bids, true),
                    asks: polymarket_levels(&state.asks, false),
                }),
            )?;
            batch.reports.push(produced_report(
                &self.canonical_rule_version,
                scope,
                ReductionOperationKindV1::Merge,
                "POLYMARKET_INITIAL_BOOK_BASELINE",
                None,
                event.event_id.clone(),
            )?);
            batch.events.push(event);
            return Ok(());
        }

        let recovering_from_epoch_switch = !state.book_ready && state.has_initial_snapshot;
        let previous_bids = std::mem::take(&mut state.bids);
        let previous_asks = std::mem::take(&mut state.asks);
        state.book_ready = true;
        append_polymarket_book_transitions(
            &self.market,
            &self.canonical_rule_version,
            &self.identity_source_evidence,
            &state.context.asset,
            BookSide::Bid,
            &previous_bids,
            &incoming_bids,
            input,
            batch,
        )?;
        append_polymarket_book_transitions(
            &self.market,
            &self.canonical_rule_version,
            &self.identity_source_evidence,
            &state.context.asset,
            BookSide::Ask,
            &previous_asks,
            &incoming_asks,
            input,
            batch,
        )?;
        let no_change = same_polymarket_levels(&previous_bids, &incoming_bids)
            && same_polymarket_levels(&previous_asks, &incoming_asks);
        state.bids = incoming_bids;
        state.asks = incoming_asks;
        if recovering_from_epoch_switch {
            append_polymarket_quality_boundary(
                &self.market,
                &self.canonical_rule_version,
                &self.identity_source_evidence,
                &state.context.asset,
                MarketDataQuality::Missing,
                MarketDataQuality::Fresh,
                ResyncQuality::ResyncBoundary,
                POLYMARKET_L2_EPOCH_BASELINE_RESTORED,
                ReductionOperationKindV1::Overwrite,
                &input.evidence,
                batch,
            )?;
        } else if no_change {
            let mut evidence = self.identity_source_evidence.clone();
            evidence.push(input.evidence.clone());
            evidence.extend(
                previous_bids
                    .values()
                    .chain(previous_asks.values())
                    .map(|level| level.direct_evidence.clone()),
            );
            batch.reports.push(non_event_report(
                &self.canonical_rule_version,
                scope,
                ReductionOperationKindV1::Confirm,
                "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION",
                evidence,
                true,
            )?);
        }
        Ok(())
    }

    fn apply_polymarket_price_change(
        &mut self,
        change: &PolymarketPriceChangeEntry,
        input: &PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        let side = match change.side {
            PolymarketSide::Buy => BookSide::Bid,
            PolymarketSide::Sell => BookSide::Ask,
            PolymarketSide::Unknown => {
                return Err(invalid(
                    "price_change.side",
                    "unknown side cannot be mapped to a book side",
                ));
            }
        };
        if change.size.is_some() {
            let state = self
                .assets
                .get(&change.asset_id)
                .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
            if !state.book_ready {
                batch.reports.push(non_event_report(
                    &self.canonical_rule_version,
                    polymarket_asset_scope(&self.market, &state.context.asset),
                    ReductionOperationKindV1::Reject,
                    POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED,
                    vec![input.evidence.clone()],
                    false,
                )?);
                if change.best_bid.is_some() || change.best_ask.is_some() {
                    self.apply_polymarket_bba(
                        &change.asset_id,
                        change.best_bid.as_deref(),
                        change.best_ask.as_deref(),
                        None,
                        input,
                        batch,
                    )?;
                }
                return Ok(());
            }
        }
        if let Some(size) = &change.size {
            let state = self
                .assets
                .get_mut(&change.asset_id)
                .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
            let price = typed_raw::normalize_decimal_text(&change.price, "price_change.price")
                .map_err(typed_raw_contract_error)?;
            let quantity = typed_raw::normalize_decimal_text(size, "price_change.size")
                .map_err(typed_raw_contract_error)?;
            let key = parse_canonical_decimal(&price, "price_change.price")?;
            let levels = match side {
                BookSide::Bid => &mut state.bids,
                BookSide::Ask => &mut state.asks,
            };
            let previous = levels.get(&key).cloned();
            let before = previous
                .as_ref()
                .map(|level| parse_canonical_decimal(&level.quantity, "quantity_before"))
                .transpose()?
                .unwrap_or(Decimal::ZERO);
            let after = parse_canonical_decimal(&quantity, "quantity_after")?;
            let scope = polymarket_level_scope(&self.market, &state.context.asset, side, &price);
            let mut evidence = self.identity_source_evidence.clone();
            evidence.push(input.evidence.clone());
            if let Some(previous) = &previous {
                evidence.push(previous.direct_evidence.clone());
            }
            if before == after {
                batch.reports.push(non_event_report(
                    &self.canonical_rule_version,
                    scope,
                    ReductionOperationKindV1::Confirm,
                    "POLYMARKET_ABSOLUTE_LEVEL_CONFIRMATION",
                    evidence,
                    true,
                )?);
            } else {
                let event = polymarket_level_transition_event(
                    &self.market,
                    &self.canonical_rule_version,
                    &state.context.asset,
                    side,
                    price.clone(),
                    before,
                    after,
                    LevelExecutionBasis::LevelDeltaInference,
                    ResyncQuality::NotRequired,
                    &input.evidence,
                    evidence,
                )?;
                let summary = level_quantity_summary(before, after);
                let operation = if after.is_zero() {
                    ReductionOperationKindV1::Delete
                } else if before.is_zero() {
                    ReductionOperationKindV1::Merge
                } else {
                    ReductionOperationKindV1::Overwrite
                };
                batch.reports.push(produced_report(
                    &self.canonical_rule_version,
                    scope,
                    operation,
                    "POLYMARKET_ABSOLUTE_LEVEL_UPDATE",
                    Some(summary),
                    event.event_id.clone(),
                )?);
                batch.events.push(event);
            }
            if after.is_zero() {
                levels.remove(&key);
            } else {
                levels.insert(
                    key,
                    PolymarketLevelState {
                        price,
                        quantity,
                        direct_evidence: input.evidence.clone(),
                    },
                );
            }
        }
        if change.best_bid.is_some() || change.best_ask.is_some() {
            self.apply_polymarket_bba(
                &change.asset_id,
                change.best_bid.as_deref(),
                change.best_ask.as_deref(),
                None,
                input,
                batch,
            )?;
        } else if change.size.is_none() {
            let state = self
                .assets
                .get(&change.asset_id)
                .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
            batch.reports.push(non_event_report(
                &self.canonical_rule_version,
                polymarket_asset_scope(&self.market, &state.context.asset),
                ReductionOperationKindV1::Reject,
                "POLYMARKET_PRICE_CHANGE_WITHOUT_STATE",
                vec![input.evidence.clone()],
                false,
            )?);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_polymarket_bba(
        &mut self,
        asset_id: &str,
        best_bid: Option<&str>,
        best_ask: Option<&str>,
        spread: Option<&str>,
        input: &PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        let best_bid = normalize_polymarket_boundary_quote(best_bid, "best_bid", Decimal::ZERO)?;
        let best_ask = normalize_polymarket_boundary_quote(best_ask, "best_ask", Decimal::ONE)?;
        if let (Some(bid), Some(ask)) = (&best_bid, &best_ask) {
            let bid = parse_canonical_decimal(bid, "best_bid")?;
            let ask = parse_canonical_decimal(ask, "best_ask")?;
            if bid > ask {
                return Err(invalid("best_bid_ask", "best bid must not exceed best ask"));
            }
            if let Some(spread) = spread {
                let spread = typed_raw::normalize_decimal_text(spread, "spread")
                    .map_err(typed_raw_contract_error)?;
                if parse_canonical_decimal(&spread, "spread")? != ask - bid {
                    return Err(invalid(
                        "spread",
                        "must equal best ask minus best bid when all values are present",
                    ));
                }
            }
        }
        let state = self
            .assets
            .get_mut(asset_id)
            .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
        let scope = polymarket_asset_scope(&self.market, &state.context.asset);
        if state
            .bba
            .as_ref()
            .is_some_and(|current| current.best_bid == best_bid && current.best_ask == best_ask)
        {
            let mut evidence = self.identity_source_evidence.clone();
            evidence.push(input.evidence.clone());
            evidence.push(
                state
                    .bba
                    .as_ref()
                    .expect("matched BBA state")
                    .direct_evidence
                    .clone(),
            );
            batch.reports.push(non_event_report(
                &self.canonical_rule_version,
                scope,
                ReductionOperationKindV1::Confirm,
                "POLYMARKET_BBA_CONFIRMATION",
                evidence,
                true,
            )?);
            return Ok(());
        }
        let previous = state.bba.clone();
        state.bba = Some(PolymarketBbaState {
            best_bid: best_bid.clone(),
            best_ask: best_ask.clone(),
            direct_evidence: input.evidence.clone(),
        });
        let mut evidence = self.identity_source_evidence.clone();
        evidence.push(input.evidence.clone());
        if let Some(previous) = &previous {
            evidence.push(previous.direct_evidence.clone());
        }
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence,
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                asset: state.context.asset.clone(),
                best_bid_pusd_per_share: best_bid,
                best_ask_pusd_per_share: best_ask,
            }),
        )?;
        batch.reports.push(produced_report(
            &self.canonical_rule_version,
            scope,
            if previous.is_some() {
                ReductionOperationKindV1::Overwrite
            } else {
                ReductionOperationKindV1::Merge
            },
            "POLYMARKET_BBA_UPDATE",
            None,
            event.event_id.clone(),
        )?);
        batch.events.push(event);
        Ok(())
    }

    fn apply_polymarket_tick(
        &mut self,
        asset_id: &str,
        old_tick_size: Option<&str>,
        new_tick_size: &str,
        input: &PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        let old_tick = old_tick_size
            .map(|value| typed_raw::normalize_decimal_text(value, "old_tick_size"))
            .transpose()
            .map_err(typed_raw_contract_error)?;
        let new_tick = typed_raw::normalize_decimal_text(new_tick_size, "new_tick_size")
            .map_err(typed_raw_contract_error)?;
        let state = self
            .assets
            .get_mut(asset_id)
            .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
        let scope = polymarket_asset_scope(&self.market, &state.context.asset);
        if new_tick == state.tick_size {
            let mut evidence = state.tick_direct_evidence.clone();
            evidence.push(input.evidence.clone());
            batch.reports.push(non_event_report(
                &self.canonical_rule_version,
                scope,
                ReductionOperationKindV1::Confirm,
                "POLYMARKET_TICK_CONFIRMATION",
                evidence,
                true,
            )?);
            return Ok(());
        }
        if old_tick
            .as_ref()
            .is_some_and(|declared| declared != &state.tick_size)
        {
            return Err(invalid(
                "old_tick_size",
                "does not match the current asset tick",
            ));
        }
        let mut evidence = self.identity_source_evidence.clone();
        evidence.extend(state.tick_direct_evidence.clone());
        evidence.push(input.evidence.clone());
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence,
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
                asset: state.context.asset.clone(),
                old_tick_size_pusd_per_share: old_tick,
                new_tick_size_pusd_per_share: new_tick.clone(),
            }),
        )?;
        state.tick_size = new_tick;
        state.tick_direct_evidence = vec![input.evidence.clone()];
        batch.reports.push(produced_report(
            &self.canonical_rule_version,
            scope,
            ReductionOperationKindV1::Overwrite,
            "POLYMARKET_TICK_UPDATE",
            None,
            event.event_id.clone(),
        )?);
        batch.events.push(event);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_polymarket_trade(
        &mut self,
        asset_id: &str,
        price: &str,
        size: Option<&str>,
        side: Option<PolymarketSide>,
        fee_rate_bps: Option<&str>,
        input: &PolymarketOrderbookInputV1,
        batch: &mut PolymarketReductionBatch,
    ) -> Result<(), ContractError> {
        let state = self
            .assets
            .get(asset_id)
            .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
        let (Some(size), Some(side)) = (size, side) else {
            batch.reports.push(non_event_report(
                &self.canonical_rule_version,
                polymarket_asset_scope(&self.market, &state.context.asset),
                ReductionOperationKindV1::Reject,
                "POLYMARKET_PUBLIC_TRADE_INCOMPLETE",
                vec![input.evidence.clone()],
                false,
            )?);
            return Ok(());
        };
        let aggressor_side = match side {
            PolymarketSide::Buy => TradeSide::Buy,
            PolymarketSide::Sell => TradeSide::Sell,
            PolymarketSide::Unknown => {
                batch.reports.push(non_event_report(
                    &self.canonical_rule_version,
                    polymarket_asset_scope(&self.market, &state.context.asset),
                    ReductionOperationKindV1::Reject,
                    "POLYMARKET_PUBLIC_TRADE_UNKNOWN_SIDE",
                    vec![input.evidence.clone()],
                    false,
                )?);
                return Ok(());
            }
        };
        let price = typed_raw::normalize_decimal_text(price, "last_trade_price.price")
            .map_err(typed_raw_contract_error)?;
        let quantity = typed_raw::normalize_decimal_text(size, "last_trade_price.size")
            .map_err(typed_raw_contract_error)?;
        if parse_canonical_decimal(&quantity, "last_trade_price.size")?.is_zero() {
            return Err(invalid(
                "last_trade_price.size",
                "a public trade quantity must be positive",
            ));
        }
        let fee_rate_bps = fee_rate_bps
            .map(|value| typed_raw::normalize_decimal_text(value, "fee_rate_bps"))
            .transpose()
            .map_err(typed_raw_contract_error)?;
        let trade_id =
            polymarket_source_derived_id(POLYMARKET_PUBLIC_TRADE_ID_DOMAIN_V1, &input.evidence)?;
        let mut evidence = self.identity_source_evidence.clone();
        evidence.push(input.evidence.clone());
        let event = finalize_execution_event(
            self.market.clone(),
            &self.canonical_rule_version,
            evidence,
            fresh_quality(ResyncQuality::NotRequired),
            ExecutionMarketEventPayloadV3::PublicTrade(PublicTradeV2 {
                asset: state.context.asset.clone(),
                trade_id: trade_id.0,
                trade_id_source: PublicTradeIdSourceV1::DerivedSourceEvidence,
                price_pusd_per_share: price,
                quantity_shares: quantity,
                aggressor_side,
                fee_rate_bps,
            }),
        )?;
        batch.reports.push(produced_report(
            &self.canonical_rule_version,
            polymarket_asset_scope(&self.market, &state.context.asset),
            ReductionOperationKindV1::Merge,
            "POLYMARKET_PUBLIC_TRADE_ACCEPTED",
            None,
            event.event_id.clone(),
        )?);
        batch.events.push(event);
        Ok(())
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

fn validate_identity_snapshot(input: &PolymarketMetadataInputV1) -> Result<(), ContractError> {
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
        || input.evidence.source_event_at_utc_ms.is_some()
    {
        return Err(invalid(
            "metadata",
            "market context requires a complete Gamma identity snapshot without fabricated source time",
        ));
    }
    Ok(())
}

fn validate_metadata_identity(
    row: &PolymarketMarketMetadataRow,
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
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
    let outcomes = typed_raw::map_metadata_outcomes(
        &row.asset_ids,
        row.outcomes_raw.as_deref(),
        row.outcome_prices_raw.as_deref(),
    )
    .map_err(typed_raw_contract_error)?;
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

fn evidence_for_terminal_state(
    identity_evidence: &[SourceEvidenceRefV2],
    terminal_evidence: &[SourceEvidenceRefV2],
) -> Vec<SourceEvidenceRefV2> {
    identity_evidence
        .iter()
        .chain(terminal_evidence)
        .cloned()
        .collect()
}

fn qualified_terminal_lifecycle_event(
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
    canonical_rule_version: &str,
    evidence: Vec<SourceEvidenceRefV2>,
) -> Result<ExecutionMarketEventV3, ContractError> {
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
        || metadata.terminal_predicate_passed != Some(true)
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
    if metadata.source_evidence_received_at_utc != Some(orderbook.timestamp_received)
        || metadata.timestamp_received != orderbook.timestamp_received
    {
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

    let metadata_outcomes = typed_raw::map_metadata_outcomes(
        &metadata.asset_ids,
        metadata.outcomes_raw.as_deref(),
        metadata.outcome_prices_raw.as_deref(),
    )
    .map_err(typed_raw_contract_error)?;
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

fn gamma_terminal_conflict_reason(
    market: &MarketIdentityV2,
    assets: &[AssetIdentityV1],
    metadata: &PolymarketMetadataInputV1,
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
        || row.terminal_predicate_passed != Some(true)
        || row.resolution_integrity_error_class.is_some()
        || row.closed != Some(true)
        || row.accepting_orders != Some(false)
        || row.closed_time_check_passed != Some(true)
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
        || metadata.evidence.source_event_at_utc_ms.is_some()
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

    let metadata_outcomes = typed_raw::map_metadata_outcomes(
        &row.asset_ids,
        row.outcomes_raw.as_deref(),
        row.outcome_prices_raw.as_deref(),
    )
    .map_err(typed_raw_contract_error)?;
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

fn same_decimal_id_set<'a>(
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

fn polymarket_level_map(
    levels: &[PolymarketPriceLevel],
    evidence: &SourceEvidenceRefV2,
    field: &str,
) -> Result<BTreeMap<Decimal, PolymarketLevelState>, ContractError> {
    let mut mapped = BTreeMap::new();
    for (index, level) in levels.iter().enumerate() {
        let price =
            typed_raw::normalize_decimal_text(&level.price, &format!("{field}[{index}].price"))
                .map_err(typed_raw_contract_error)?;
        let quantity =
            typed_raw::normalize_decimal_text(&level.size, &format!("{field}[{index}].size"))
                .map_err(typed_raw_contract_error)?;
        let key = parse_canonical_decimal(&price, &format!("{field}[{index}].price"))?;
        if mapped
            .insert(
                key,
                PolymarketLevelState {
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

fn same_polymarket_levels(
    left: &BTreeMap<Decimal, PolymarketLevelState>,
    right: &BTreeMap<Decimal, PolymarketLevelState>,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_price, left), (right_price, right))| {
                left_price == right_price && left.quantity == right.quantity
            })
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

#[derive(Serialize)]
struct PolymarketBookDigestMaterialV1<'a> {
    asset: &'a AssetIdentityV1,
    bids: Vec<PriceLevelV1>,
    asks: Vec<PriceLevelV1>,
}

fn polymarket_book_digest(
    asset: &AssetIdentityV1,
    bids: &BTreeMap<Decimal, PolymarketLevelState>,
    asks: &BTreeMap<Decimal, PolymarketLevelState>,
) -> Result<Sha256Hex, ContractError> {
    let material = PolymarketBookDigestMaterialV1 {
        asset,
        bids: polymarket_levels(bids, true),
        asks: polymarket_levels(asks, false),
    };
    Ok(hash_domain_bytes(
        POLYMARKET_BOOK_DIGEST_DOMAIN_V1,
        &stable_json(&material)?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn append_polymarket_book_transitions(
    market: &MarketIdentityV2,
    canonical_rule_version: &str,
    identity_source_evidence: &[SourceEvidenceRefV2],
    asset: &AssetIdentityV1,
    side: BookSide,
    previous: &BTreeMap<Decimal, PolymarketLevelState>,
    current: &BTreeMap<Decimal, PolymarketLevelState>,
    input: &PolymarketOrderbookInputV1,
    batch: &mut PolymarketReductionBatch,
) -> Result<(), ContractError> {
    let mut keys = previous
        .keys()
        .chain(current.keys())
        .copied()
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    if side == BookSide::Bid {
        keys.reverse();
    }
    for key in keys {
        let before_state = previous.get(&key);
        let after_state = current.get(&key);
        let before = before_state
            .map(|level| parse_canonical_decimal(&level.quantity, "quantity_before"))
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let after = after_state
            .map(|level| parse_canonical_decimal(&level.quantity, "quantity_after"))
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        if before == after {
            continue;
        }
        let price = after_state
            .or(before_state)
            .expect("union key belongs to at least one book")
            .price
            .clone();
        let mut evidence = identity_source_evidence.to_vec();
        evidence.push(input.evidence.clone());
        if let Some(previous) = before_state {
            evidence.push(previous.direct_evidence.clone());
        }
        let event = polymarket_level_transition_event(
            market,
            canonical_rule_version,
            asset,
            side,
            price.clone(),
            before,
            after,
            LevelExecutionBasis::BookResyncDeltaInference,
            ResyncQuality::ResyncBoundary,
            &input.evidence,
            evidence,
        )?;
        let operation = if after.is_zero() {
            ReductionOperationKindV1::Delete
        } else if before.is_zero() {
            ReductionOperationKindV1::Merge
        } else {
            ReductionOperationKindV1::Overwrite
        };
        batch.reports.push(produced_report(
            canonical_rule_version,
            polymarket_level_scope(market, asset, side, &price),
            operation,
            "POLYMARKET_BOOK_RESYNC_DELTA",
            Some(level_quantity_summary(before, after)),
            event.event_id.clone(),
        )?);
        batch.events.push(event);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn polymarket_level_transition_event(
    market: &MarketIdentityV2,
    canonical_rule_version: &str,
    asset: &AssetIdentityV1,
    side: BookSide,
    price: ExactDecimal,
    before: Decimal,
    after: Decimal,
    basis: LevelExecutionBasis,
    resync: ResyncQuality,
    trigger_evidence: &SourceEvidenceRefV2,
    evidence: Vec<SourceEvidenceRefV2>,
) -> Result<ExecutionMarketEventV3, ContractError> {
    let evidence = canonicalize_source_evidence(evidence)?;
    let time = event_time_from_evidence(&evidence);
    let source_time = time.source_event_at_utc_ms.ok_or_else(|| {
        invalid(
            "source_event_at_utc_ms",
            "Polymarket level transition requires platform source time",
        )
    })?;
    let (added, reduced) = match after.cmp(&before) {
        Ordering::Greater => (after - before, Decimal::ZERO),
        Ordering::Less => (Decimal::ZERO, before - after),
        Ordering::Equal => (Decimal::ZERO, Decimal::ZERO),
    };
    let causal_group_id =
        polymarket_source_derived_id(POLYMARKET_CAUSAL_GROUP_DOMAIN_V1, trigger_evidence)?;
    finalize_execution_event(
        market.clone(),
        canonical_rule_version,
        evidence,
        fresh_quality(resync),
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id,
            market: market.clone(),
            asset: asset.clone(),
            side,
            price_pusd_per_share: price,
            quantity_before_shares: exact_from_decimal(before),
            quantity_after_shares: exact_from_decimal(after),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: exact_from_decimal(added),
            net_reduced_quantity_shares: exact_from_decimal(reduced),
            queue_consumption_quantity_shares: exact_from_decimal(reduced),
            basis,
            platform_effective_at_utc_ms: source_time,
            visible_at_utc_ms: time.received_at_utc_ms,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
    )
}

fn level_quantity_summary(before: Decimal, after: Decimal) -> QuantityConservationV1 {
    let (added, reduced) = match after.cmp(&before) {
        Ordering::Greater => (after - before, Decimal::ZERO),
        Ordering::Less => (Decimal::ZERO, before - after),
        Ordering::Equal => (Decimal::ZERO, Decimal::ZERO),
    };
    QuantityConservationV1 {
        quantity_before: exact_from_decimal(before),
        quantity_after: exact_from_decimal(after),
        quantity_added: exact_from_decimal(added),
        quantity_reduced: exact_from_decimal(reduced),
    }
}

fn polymarket_source_derived_id(
    domain: &[u8],
    evidence: &SourceEvidenceRefV2,
) -> Result<Sha256Hex, ContractError> {
    Ok(hash_domain_bytes(
        domain,
        &stable_json(&semantic_source_evidence(evidence))?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn append_polymarket_quality_boundary(
    market: &MarketIdentityV2,
    canonical_rule_version: &str,
    identity_source_evidence: &[SourceEvidenceRefV2],
    asset: &AssetIdentityV1,
    previous_quality: MarketDataQuality,
    current_quality: MarketDataQuality,
    resync: ResyncQuality,
    reason_code: &str,
    operation: ReductionOperationKindV1,
    trigger_evidence: &SourceEvidenceRefV2,
    batch: &mut PolymarketReductionBatch,
) -> Result<(), ContractError> {
    let mut evidence = identity_source_evidence.to_vec();
    evidence.push(trigger_evidence.clone());
    let event = finalize_execution_event(
        market.clone(),
        canonical_rule_version,
        evidence,
        EventQualityV2 {
            availability: current_quality,
            resync,
        },
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset.clone()),
            previous_quality,
            current_quality,
            reason_code: reason_code.to_owned(),
        }),
    )?;
    batch.reports.push(produced_report(
        canonical_rule_version,
        polymarket_asset_scope(market, asset),
        operation,
        reason_code,
        None,
        event.event_id.clone(),
    )?);
    batch.events.push(event);
    Ok(())
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

fn polymarket_asset_scope(market: &MarketIdentityV2, asset: &AssetIdentityV1) -> String {
    format!(
        "polymarket_asset:{}:{}",
        market.condition_id.0, asset.token_id.0
    )
}

fn polymarket_level_scope(
    market: &MarketIdentityV2,
    asset: &AssetIdentityV1,
    side: BookSide,
    price: &ExactDecimal,
) -> String {
    let side = match side {
        BookSide::Bid => "BID",
        BookSide::Ask => "ASK",
    };
    format!(
        "polymarket_level:{}:{}:{side}:{}",
        market.condition_id.0, asset.token_id.0, price.0
    )
}
