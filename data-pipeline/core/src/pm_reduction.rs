//! 共用纯盘口计算。数量由调用方拥有；这里只借用前序，并覆盖本消息触及的键。
use std::collections::{BTreeMap, BTreeSet};

use contracts::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::pm_message::ParsedCanonicalPmMessageV1;
use crate::reduction::{
    PolymarketAssetContextV1, QuantityConservationV1, ReductionEvidenceRoleV1,
    ReductionOperationKindV1,
};
use crate::{ContractError, hash_domain_bytes, invalid, parse_canonical_decimal, stable_json};

mod sealed {
    pub trait Evidence {}
    pub trait StepIndex {}
}

/// 泛型只用于真实 V2 旧包装与 V3 公共入口，不制造物理来源标识。
pub trait PmEvidenceV1:
    sealed::Evidence + Clone + std::fmt::Debug + PartialEq + Eq + Serialize
{
    fn source_time(&self) -> Option<i64>;
    fn is_metadata_identity(&self) -> bool;
    fn received_time(&self) -> i64;
    fn semantic_bytes(&self) -> Result<Vec<u8>, ContractError>;
    fn canonicalize(values: Vec<Self>) -> Result<Vec<Self>, ContractError>;
    fn source_digest(&self, domain: &[u8]) -> Result<Sha256Hex, ContractError> {
        Ok(hash_domain_bytes(domain, &self.semantic_bytes()?))
    }
    fn prepared_book_digest(
        &self,
        asset: &AssetIdentityV1,
        bids: &BTreeMap<Decimal, &PriceLevelV1>,
        asks: &BTreeMap<Decimal, &PriceLevelV1>,
    ) -> Result<Sha256Hex, ContractError> {
        book_digest(asset, bids, asks)
    }
}

impl sealed::Evidence for SourceEvidenceRefV2 {}
impl PmEvidenceV1 for SourceEvidenceRefV2 {
    fn is_metadata_identity(&self) -> bool {
        self.source_kind == CanonicalSourceKind::PolymarketMarketMetadata
            && self.source_event_at_utc_ms.is_none()
            && matches!(
                self.source_native_identity,
                Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { .. })
            )
            && self.collector_admission.is_none()
    }

    fn source_time(&self) -> Option<i64> {
        self.source_event_at_utc_ms
    }
    fn received_time(&self) -> i64 {
        self.received_at_utc_ms
    }
    fn semantic_bytes(&self) -> Result<Vec<u8>, ContractError> {
        stable_json(&crate::semantic_source_evidence(self))
    }
    fn canonicalize(values: Vec<Self>) -> Result<Vec<Self>, ContractError> {
        crate::reduction::canonicalize_source_evidence(values)
    }
}
impl sealed::Evidence for SourceEvidenceRefV3 {}
impl PmEvidenceV1 for SourceEvidenceRefV3 {
    fn is_metadata_identity(&self) -> bool {
        self.source_kind == CanonicalSourceKindV2::PolymarketMarketMetadata
            && self.source_event_at_utc_ms.is_none()
            && matches!(
                self.source_native_identity,
                Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { .. })
            )
            && self.collector_admission.is_none()
    }

    fn source_time(&self) -> Option<i64> {
        self.source_event_at_utc_ms
    }
    fn received_time(&self) -> i64 {
        self.received_at_utc_ms
    }
    fn semantic_bytes(&self) -> Result<Vec<u8>, ContractError> {
        stable_json(self)
    }
    fn canonicalize(values: Vec<Self>) -> Result<Vec<Self>, ContractError> {
        let mut keyed = values
            .into_iter()
            .map(|value| Ok((stable_json(&value)?, value)))
            .collect::<Result<Vec<_>, ContractError>>()?;
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        keyed.dedup_by(|a, b| a.0 == b.0);
        Ok(keyed.into_iter().map(|(_, value)| value).collect())
    }
}

#[derive(Clone, Debug)]
pub struct PmMarketContextV1<E = SourceEvidenceRefV3> {
    pub market: MarketIdentityV2,
    pub assets: Vec<PolymarketAssetContextV1>,
    pub identity_source_evidence: Vec<E>,
    pub canonical_rule_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmEvidencePairV1<E = SourceEvidenceRefV3> {
    pub direct: E,
    pub frontier: E,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmL2SelectionVersionV1 {
    pub source_sequence_epoch_id: String,
    pub source_at_utc_ms: i64,
    pub collector_admission_sequence: u64,
    /// None 表示完整快照；Some 是原 price_changes 下标。
    pub member_index: Option<u32>,
}

impl PmL2SelectionVersionV1 {
    fn is_at_least(&self, previous: &Self) -> bool {
        self.source_sequence_epoch_id == previous.source_sequence_epoch_id
            && (
                self.source_at_utc_ms,
                self.collector_admission_sequence,
                self.member_index,
            ) >= (
                previous.source_at_utc_ms,
                previous.collector_admission_sequence,
                previous.member_index,
            )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmSelectedLevelV1<E = SourceEvidenceRefV3> {
    pub version: PmL2SelectionVersionV1,
    /// FullBook 的显式零量仍存在；删除或快照缺价为 false。
    pub present: bool,
    pub confirmation: E,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PmBbaStateV1<E = SourceEvidenceRefV3> {
    pub best_bid: Option<ExactDecimal>,
    pub best_ask: Option<ExactDecimal>,
    pub evidence: PmEvidencePairV1<E>,
}

/// 小量状态元数据；数量和价位键通过 PmStateReadV1 借用查询。
#[derive(Clone, Debug)]
pub struct PmAssetStateV1<E = SourceEvidenceRefV3> {
    pub has_initial_snapshot: bool,
    pub book_ready: bool,
    pub tick_size: ExactDecimal,
    pub tick_direct_evidence: Vec<E>,
    pub tick_frontier: E,
    pub absence_baseline: Option<PmEvidencePairV1<E>>,
    pub selection_baseline: Option<PmSelectedLevelV1<E>>,
    pub bba: Option<PmBbaStateV1<E>>,
    pub bba_reanchor_frontier: Option<E>,
}

#[derive(Clone, Copy, Debug)]
pub struct PmEvidenceReadV1<'a, E = SourceEvidenceRefV3> {
    pub direct: &'a E,
    pub frontier: &'a E,
}

impl<E: Clone> PmEvidenceReadV1<'_, E> {
    pub fn to_owned(&self) -> PmEvidencePairV1<E> {
        PmEvidencePairV1 {
            direct: self.direct.clone(),
            frontier: self.frontier.clone(),
        }
    }
}

impl<E> PmEvidencePairV1<E> {
    pub fn as_read(&self) -> PmEvidenceReadV1<'_, E> {
        PmEvidenceReadV1 {
            direct: &self.direct,
            frontier: &self.frontier,
        }
    }
}

pub struct PmBbaReadV1<'a, E = SourceEvidenceRefV3> {
    pub best_bid: &'a Option<ExactDecimal>,
    pub best_ask: &'a Option<ExactDecimal>,
    pub evidence: PmEvidenceReadV1<'a, E>,
}

pub struct PmAssetReadV1<'a, E = SourceEvidenceRefV3> {
    pub has_initial_snapshot: bool,
    pub book_ready: bool,
    pub tick_size: &'a ExactDecimal,
    pub tick_direct_evidence: &'a [E],
    pub tick_frontier: &'a E,
    pub absence_baseline: Option<PmEvidenceReadV1<'a, E>>,
    pub selection_baseline: Option<&'a PmSelectedLevelV1<E>>,
    pub bba: Option<PmBbaReadV1<'a, E>>,
    pub bba_reanchor_frontier: Option<&'a E>,
}

impl<E: Clone> PmAssetReadV1<'_, E> {
    fn to_owned(&self) -> PmAssetStateV1<E> {
        PmAssetStateV1 {
            has_initial_snapshot: self.has_initial_snapshot,
            book_ready: self.book_ready,
            tick_size: self.tick_size.clone(),
            tick_direct_evidence: self.tick_direct_evidence.to_vec(),
            tick_frontier: self.tick_frontier.clone(),
            absence_baseline: self
                .absence_baseline
                .as_ref()
                .map(PmEvidenceReadV1::to_owned),
            selection_baseline: self.selection_baseline.cloned(),
            bba: self.bba.as_ref().map(|value| PmBbaStateV1 {
                best_bid: value.best_bid.clone(),
                best_ask: value.best_ask.clone(),
                evidence: value.evidence.to_owned(),
            }),
            bba_reanchor_frontier: self.bba_reanchor_frontier.cloned(),
        }
    }
}

impl<E> PmAssetStateV1<E> {
    pub fn as_read(&self) -> PmAssetReadV1<'_, E> {
        PmAssetReadV1 {
            has_initial_snapshot: self.has_initial_snapshot,
            book_ready: self.book_ready,
            tick_size: &self.tick_size,
            tick_direct_evidence: &self.tick_direct_evidence,
            tick_frontier: &self.tick_frontier,
            absence_baseline: self
                .absence_baseline
                .as_ref()
                .map(PmEvidencePairV1::as_read),
            selection_baseline: self.selection_baseline.as_ref(),
            bba: self.bba.as_ref().map(|value| PmBbaReadV1 {
                best_bid: &value.best_bid,
                best_ask: &value.best_ask,
                evidence: value.evidence.as_read(),
            }),
            bba_reanchor_frontier: self.bba_reanchor_frontier.as_ref(),
        }
    }
}

pub trait PmStateReadV1<E = SourceEvidenceRefV3> {
    fn market(&self) -> &MarketIdentityV2;
    fn current_epoch(&self) -> Option<&str>;
    fn asset_state(&self, token: &str) -> Option<PmAssetReadV1<'_, E>>;
    /// 缺键沿既有数量簿约定返回零；存在性另查归约元数据。
    fn quantity(&self, token: &str, side: BookSide, price: Decimal) -> Decimal;
    fn level_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PmEvidenceReadV1<'_, E>>;
    fn absence_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PmEvidenceReadV1<'_, E>>;
    fn selected_level(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<&PmSelectedLevelV1<E>>;
    /// 遍历价位、明确缺价及选择元数据的键；不能只遍历非零数量。
    fn level_keys<'a>(
        &'a self,
        token: &'a str,
        side: BookSide,
    ) -> Box<dyn Iterator<Item = Decimal> + 'a>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PmConfirmationComponentV1 {
    BestBidAsk,
    Tick,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PmStepChangeV1<'m, E = SourceEvidenceRefV3> {
    Epoch {
        source_sequence_epoch_id: String,
    },
    ResetAsset {
        asset: AssetIdentityV1,
        absence_baseline: Option<PmEvidencePairV1<E>>,
        bba_reanchor_frontier: Option<E>,
    },
    InitialSnapshot {
        asset: AssetIdentityV1,
        initial_tick_size_pusd_per_share: ExactDecimal,
        source_sequence_epoch_id: String,
        source_cursor: u64,
        book_digest_sha256: Sha256Hex,
        bids: std::borrow::Cow<'m, [PriceLevelV1]>,
        asks: std::borrow::Cow<'m, [PriceLevelV1]>,
        evidence: PmEvidencePairV1<E>,
    },
    LevelChange {
        transition: LevelExecutionTransitionV3,
        evidence: PmEvidencePairV1<E>,
    },
    LevelEvidenceOnly {
        asset: AssetIdentityV1,
        side: BookSide,
        price: ExactDecimal,
        evidence: PmEvidencePairV1<E>,
    },
    Selection {
        asset: AssetIdentityV1,
        side: BookSide,
        price: ExactDecimal,
        selected: PmSelectedLevelV1<E>,
        previous: Option<PmSelectedLevelV1<E>>,
        absence_baseline: Option<PmSelectedLevelV1<E>>,
    },
    AbsenceBaseline {
        asset: AssetIdentityV1,
        evidence: PmEvidencePairV1<E>,
        selected: Option<PmSelectedLevelV1<E>>,
    },
    BookReady {
        asset: AssetIdentityV1,
    },
    BestBidAsk {
        quote: BestBidAskV2,
        evidence: PmEvidencePairV1<E>,
    },
    Tick {
        tick: TickSizeChangeV2,
        evidence: PmEvidencePairV1<E>,
    },
    PublicConfirmation {
        asset: AssetIdentityV1,
        kind: PmConfirmationComponentV1,
    },
    PublicTrade {
        trade: PublicTradeV2,
    },
    Quality {
        boundary: QualityBoundaryV2,
    },
}

impl<E> PmStepChangeV1<'_, E> {
    fn into_owned(self) -> PmStepChangeV1<'static, E> {
        match self {
            Self::Epoch {
                source_sequence_epoch_id,
            } => PmStepChangeV1::Epoch {
                source_sequence_epoch_id,
            },
            Self::ResetAsset {
                asset,
                absence_baseline,
                bba_reanchor_frontier,
            } => PmStepChangeV1::ResetAsset {
                asset,
                absence_baseline,
                bba_reanchor_frontier,
            },
            Self::InitialSnapshot {
                asset,
                initial_tick_size_pusd_per_share,
                source_sequence_epoch_id,
                source_cursor,
                book_digest_sha256,
                bids,
                asks,
                evidence,
            } => PmStepChangeV1::InitialSnapshot {
                asset,
                initial_tick_size_pusd_per_share,
                source_sequence_epoch_id,
                source_cursor,
                book_digest_sha256,
                bids: std::borrow::Cow::Owned(bids.into_owned()),
                asks: std::borrow::Cow::Owned(asks.into_owned()),
                evidence,
            },
            Self::LevelChange {
                transition,
                evidence,
            } => PmStepChangeV1::LevelChange {
                transition,
                evidence,
            },
            Self::LevelEvidenceOnly {
                asset,
                side,
                price,
                evidence,
            } => PmStepChangeV1::LevelEvidenceOnly {
                asset,
                side,
                price,
                evidence,
            },
            Self::Selection {
                asset,
                side,
                price,
                selected,
                previous,
                absence_baseline,
            } => PmStepChangeV1::Selection {
                asset,
                side,
                price,
                selected,
                previous,
                absence_baseline,
            },
            Self::AbsenceBaseline {
                asset,
                evidence,
                selected,
            } => PmStepChangeV1::AbsenceBaseline {
                asset,
                evidence,
                selected,
            },
            Self::BookReady { asset } => PmStepChangeV1::BookReady { asset },
            Self::BestBidAsk { quote, evidence } => PmStepChangeV1::BestBidAsk { quote, evidence },
            Self::Tick { tick, evidence } => PmStepChangeV1::Tick { tick, evidence },
            Self::PublicConfirmation { asset, kind } => {
                PmStepChangeV1::PublicConfirmation { asset, kind }
            }
            Self::PublicTrade { trade } => PmStepChangeV1::PublicTrade { trade },
            Self::Quality { boundary } => PmStepChangeV1::Quality { boundary },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmReductionStepV1<'m, E = SourceEvidenceRefV3, I = CanonicalEventId> {
    pub canonical_fact_id: CanonicalEventId,
    pub member_index: Option<u32>,
    pub step_index: u32,
    pub derived_id: I,
    pub change: PmStepChangeV1<'m, E>,
    pub direct: E,
    pub prestate: Option<PmEvidencePairV1<E>>,
    pub absence: Option<PmEvidencePairV1<E>>,
    pub dependencies: Vec<E>,
    pub direct_time: EventTimeV2,
    pub dependency_time: EventTimeV2,
    pub quality: EventQualityV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmOperationEvidenceV2<E = SourceEvidenceRefV3> {
    pub role: ReductionEvidenceRoleV1,
    pub source_evidence: E,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PmReductionDispositionV2<E = SourceEvidenceRefV3, I = CanonicalEventId> {
    Produced {
        derived_id: I,
    },
    NoOutput {
        operation_evidence: Vec<PmOperationEvidenceV2<E>>,
    },
    Suppressed {
        operation_evidence: Vec<PmOperationEvidenceV2<E>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmReductionReportV2<E = SourceEvidenceRefV3, I = CanonicalEventId> {
    pub canonical_fact_id: CanonicalEventId,
    pub member_index: Option<u32>,
    pub canonical_rule_version: String,
    pub scope: String,
    pub operation: ReductionOperationKindV1,
    pub reason_code: String,
    pub quantity_conservation: Option<QuantityConservationV1>,
    pub disposition: PmReductionDispositionV2<E, I>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PmMessageReductionV1<'m, E = SourceEvidenceRefV3, I = CanonicalEventId> {
    pub steps: Vec<PmReductionStepV1<'m, E, I>>,
    pub reports: Vec<PmReductionReportV2<E, I>>,
    /// 实际消息内覆盖峰值；只计触及的价位，不施加新资源门槛。
    pub touched_level_entries: usize,
    #[serde(skip)]
    materialization_market: MarketIdentityV2,
    #[serde(skip)]
    materialization_rule_version: String,
}

pub(crate) struct PmMessageInput<'m, E> {
    pub fact_id: CanonicalEventId,
    pub payload: Option<&'m CanonicalPmPayloadV1>,
    pub source: &'m E,
    pub source_at: i64,
    pub admission: &'m CollectorAdmissionCoordinateV1,
}

#[derive(Clone)]
struct WorkingLevel<E> {
    quantity: Decimal,
    level: Option<PmEvidencePairV1<E>>,
    absence: Option<PmEvidencePairV1<E>>,
    selection: Option<PmSelectedLevelV1<E>>,
}

struct Worker<'a, 'm, E, S> {
    context: &'a PmMarketContextV1<E>,
    previous: &'a S,
    input: PmMessageInput<'m, E>,
    public_selection: bool,
    reset: bool,
    member: Option<u32>,
    assets: BTreeMap<String, PmAssetStateV1<E>>,
    // bool 为 ask，避免引入 BookSide 全局排序语义。
    overlay: BTreeMap<(String, bool, Decimal), WorkingLevel<E>>,
    result: PmMessageReductionV1<'m, E, u32>,
}

fn exact(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}
fn pair<E: Clone>(direct: &E, frontier: E) -> PmEvidencePairV1<E> {
    PmEvidencePairV1 {
        direct: direct.clone(),
        frontier,
    }
}
fn time<E: PmEvidenceV1>(evidence: &[E]) -> EventTimeV2 {
    EventTimeV2 {
        source_event_at_utc_ms: evidence.iter().filter_map(E::source_time).max(),
        received_at_utc_ms: evidence
            .iter()
            .map(E::received_time)
            .max()
            .expect("message dependencies contain direct evidence"),
    }
}
fn frontier<E: PmEvidenceV1>(evidence: Vec<E>) -> Result<E, ContractError> {
    E::canonicalize(evidence)?
        .into_iter()
        .max_by_key(E::received_time)
        .ok_or_else(|| invalid("source_evidence", "visibility frontier requires evidence"))
}
fn fresh(resync: ResyncQuality) -> EventQualityV2 {
    EventQualityV2 {
        availability: MarketDataQuality::Fresh,
        resync,
    }
}
fn quantity_summary(before: Decimal, after: Decimal) -> QuantityConservationV1 {
    QuantityConservationV1 {
        quantity_before: exact(before),
        quantity_after: exact(after),
        quantity_added: exact((after - before).max(Decimal::ZERO)),
        quantity_reduced: exact((before - after).max(Decimal::ZERO)),
    }
}

pub fn reduce_pm_message<'m>(
    context: &PmMarketContextV1,
    previous: &impl PmStateReadV1,
    message: &ParsedCanonicalPmMessageV1<'m>,
) -> Result<PmMessageReductionV1<'m>, ContractError> {
    reduce_message(
        context,
        previous,
        PmMessageInput {
            fact_id: message.message.event_id.clone(),
            payload: Some(&message.message.payload),
            source: message.source,
            source_at: message.source_at_utc_ms,
            admission: message.admission,
        },
        true,
    )
}

pub(crate) fn reduce_epoch<'m, E: PmEvidenceV1, S: PmStateReadV1<E>>(
    context: &PmMarketContextV1<E>,
    previous: &S,
    input: PmMessageInput<'m, E>,
) -> Result<PmMessageReductionV1<'m, E>, ContractError> {
    let mut worker = Worker {
        context,
        previous,
        input,
        public_selection: false,
        reset: false,
        member: None,
        assets: BTreeMap::new(),
        overlay: BTreeMap::new(),
        result: PmMessageReductionV1 {
            steps: Vec::new(),
            reports: Vec::new(),
            touched_level_entries: 0,
            materialization_market: context.market.clone(),
            materialization_rule_version: context.canonical_rule_version.clone(),
        },
    };
    worker.epoch()?;
    materialize_pm_reduction(worker.result)
}

pub(crate) fn reduce_message<'m, E: PmEvidenceV1, S: PmStateReadV1<E>>(
    context: &PmMarketContextV1<E>,
    previous: &S,
    input: PmMessageInput<'m, E>,
    public_selection: bool,
) -> Result<PmMessageReductionV1<'m, E>, ContractError> {
    materialize_pm_reduction(compute_message(
        context,
        previous,
        input,
        public_selection,
        public_selection,
    )?)
}

fn compute_message<'m, E: PmEvidenceV1, S: PmStateReadV1<E>>(
    context: &PmMarketContextV1<E>,
    previous: &S,
    input: PmMessageInput<'m, E>,
    public_selection: bool,
    manage_epoch: bool,
) -> Result<PmMessageReductionV1<'m, E, u32>, ContractError> {
    let mut worker = Worker {
        context,
        previous,
        input,
        public_selection,
        reset: false,
        member: None,
        assets: BTreeMap::new(),
        overlay: BTreeMap::new(),
        result: PmMessageReductionV1 {
            steps: Vec::new(),
            reports: Vec::new(),
            touched_level_entries: 0,
            materialization_market: context.market.clone(),
            materialization_rule_version: context.canonical_rule_version.clone(),
        },
    };
    if manage_epoch
        && previous.current_epoch()
            != Some(worker.input.admission.source_sequence_epoch_id.as_str())
    {
        worker.epoch()?;
    }
    worker.apply()?;
    worker.result.touched_level_entries = worker.overlay.len();
    Ok(worker.result)
}

impl<'a, 'm, E: PmEvidenceV1, S: PmStateReadV1<E>> Worker<'a, 'm, E, S> {
    fn asset(&mut self, asset: &AssetIdentityV1) -> Result<PmAssetStateV1<E>, ContractError> {
        if let Some(state) = self.assets.get(&asset.token_id.0) {
            return Ok(state.clone());
        }
        let state = self
            .previous
            .asset_state(&asset.token_id.0)
            .map(|value| value.to_owned())
            .ok_or_else(|| invalid("asset_id", "is not part of the registered market"))?;
        self.assets.insert(asset.token_id.0.clone(), state.clone());
        Ok(state)
    }
    fn scope(&self, asset: &AssetIdentityV1) -> String {
        format!(
            "polymarket_asset:{}:{}",
            self.context.market.condition_id.0, asset.token_id.0
        )
    }
    fn level_scope(&self, asset: &AssetIdentityV1, side: BookSide, price: &ExactDecimal) -> String {
        format!(
            "polymarket_level:{}:{}:{}:{}",
            self.context.market.condition_id.0,
            asset.token_id.0,
            if side == BookSide::Bid { "BID" } else { "ASK" },
            price.0
        )
    }
    fn dependencies(&self, previous: Option<&PmEvidencePairV1<E>>) -> Vec<E> {
        let mut values = self.context.identity_source_evidence.clone();
        values.push(self.input.source.clone());
        if let Some(previous) = previous {
            values.extend([previous.direct.clone(), previous.frontier.clone()]);
        }
        values
    }
    fn version(&self) -> PmL2SelectionVersionV1 {
        PmL2SelectionVersionV1 {
            source_sequence_epoch_id: self.input.admission.source_sequence_epoch_id.clone(),
            source_at_utc_ms: self.input.source_at,
            collector_admission_sequence: self.input.admission.collector_admission_sequence,
            member_index: self.member,
        }
    }
    fn selected(&self, present: bool) -> PmSelectedLevelV1<E> {
        PmSelectedLevelV1 {
            version: self.version(),
            present,
            confirmation: self.input.source.clone(),
        }
    }
    fn accepts(&self, state: &PmAssetStateV1<E>, level: &WorkingLevel<E>) -> bool {
        !self.public_selection
            || level
                .selection
                .iter()
                .chain(state.selection_baseline.iter())
                .all(|previous| self.version().is_at_least(&previous.version))
    }
    fn level(&self, asset: &AssetIdentityV1, side: BookSide, key: Decimal) -> WorkingLevel<E> {
        self.overlay
            .get(&(asset.token_id.0.clone(), side == BookSide::Ask, key))
            .cloned()
            .unwrap_or_else(|| {
                if self.reset {
                    WorkingLevel {
                        quantity: Decimal::ZERO,
                        level: None,
                        absence: None,
                        selection: None,
                    }
                } else {
                    WorkingLevel {
                        quantity: self.previous.quantity(&asset.token_id.0, side, key),
                        level: self
                            .previous
                            .level_evidence(&asset.token_id.0, side, key)
                            .map(|value| value.to_owned()),
                        absence: self
                            .previous
                            .absence_evidence(&asset.token_id.0, side, key)
                            .map(|value| value.to_owned()),
                        selection: self
                            .previous
                            .selected_level(&asset.token_id.0, side, key)
                            .cloned(),
                    }
                }
            })
    }
    fn save_level(
        &mut self,
        asset: &AssetIdentityV1,
        side: BookSide,
        key: Decimal,
        value: WorkingLevel<E>,
    ) {
        self.overlay.insert(
            (asset.token_id.0.clone(), side == BookSide::Ask, key),
            value,
        );
    }
    fn step(
        &mut self,
        change: PmStepChangeV1<'m, E>,
        prestate: Option<PmEvidencePairV1<E>>,
        dependencies: Vec<E>,
        quality: EventQualityV2,
    ) -> Result<u32, ContractError> {
        self.step_with_absence(change, prestate, None, dependencies, quality)
    }
    fn step_with_absence(
        &mut self,
        change: PmStepChangeV1<'m, E>,
        prestate: Option<PmEvidencePairV1<E>>,
        absence: Option<PmEvidencePairV1<E>>,
        dependencies: Vec<E>,
        quality: EventQualityV2,
    ) -> Result<u32, ContractError> {
        let dependencies = E::canonicalize(dependencies)?;
        let dependency_time = time(&dependencies);
        let step_index = self.result.steps.len() as u32;
        let derived_id = step_index;
        self.result.steps.push(PmReductionStepV1 {
            canonical_fact_id: self.input.fact_id.clone(),
            member_index: self.member,
            step_index,
            derived_id: derived_id.clone(),
            change,
            direct: self.input.source.clone(),
            prestate,
            absence,
            dependencies,
            direct_time: EventTimeV2 {
                source_event_at_utc_ms: self.input.source.source_time(),
                received_at_utc_ms: self.input.source.received_time(),
            },
            dependency_time,
            quality,
        });
        Ok(derived_id)
    }
    fn report(
        &mut self,
        scope: String,
        operation: ReductionOperationKindV1,
        reason: &str,
        quantity: Option<QuantityConservationV1>,
        disposition: PmReductionDispositionV2<E, u32>,
    ) {
        self.result.reports.push(PmReductionReportV2 {
            canonical_fact_id: self.input.fact_id.clone(),
            member_index: self.member,
            canonical_rule_version: self.context.canonical_rule_version.clone(),
            scope,
            operation,
            reason_code: reason.to_owned(),
            quantity_conservation: quantity,
            disposition,
        });
    }
    fn no_event(&mut self, scope: String, reason: &str, evidence: Vec<E>, suppressed: bool) {
        let evidence = evidence
            .into_iter()
            .map(|source_evidence| PmOperationEvidenceV2 {
                role: ReductionEvidenceRoleV1::Input,
                source_evidence,
            })
            .collect();
        self.report(
            scope,
            if suppressed {
                ReductionOperationKindV1::Confirm
            } else {
                ReductionOperationKindV1::Reject
            },
            reason,
            None,
            if suppressed {
                PmReductionDispositionV2::Suppressed {
                    operation_evidence: evidence,
                }
            } else {
                PmReductionDispositionV2::NoOutput {
                    operation_evidence: evidence,
                }
            },
        );
    }
    fn confirmation(
        &mut self,
        asset: &AssetIdentityV1,
        kind: PmConfirmationComponentV1,
        evidence: Vec<E>,
    ) -> Result<(), ContractError> {
        if self.public_selection {
            self.step(
                PmStepChangeV1::PublicConfirmation {
                    asset: asset.clone(),
                    kind,
                },
                None,
                evidence,
                fresh(ResyncQuality::NotRequired),
            )?;
        }
        Ok(())
    }
    fn select_level(
        &mut self,
        asset: &AssetIdentityV1,
        side: BookSide,
        key: Decimal,
        present: bool,
        mut level: WorkingLevel<E>,
        save: bool,
    ) -> Result<(), ContractError> {
        if self.public_selection {
            let selected = self.selected(present);
            let baseline = self
                .assets
                .get(&asset.token_id.0)
                .and_then(|state| state.selection_baseline.clone());
            let dependencies = std::iter::once(self.input.source.clone())
                .chain(
                    level
                        .selection
                        .iter()
                        .chain(baseline.iter())
                        .map(|value| value.confirmation.clone()),
                )
                .collect();
            self.step(
                PmStepChangeV1::Selection {
                    asset: asset.clone(),
                    side,
                    price: exact(key),
                    selected: selected.clone(),
                    previous: level.selection.clone(),
                    absence_baseline: baseline,
                },
                None,
                dependencies,
                fresh(ResyncQuality::NotRequired),
            )?;
            level.selection = Some(selected);
        }
        if save {
            self.save_level(asset, side, key, level);
        }
        Ok(())
    }

    fn epoch(&mut self) -> Result<(), ContractError> {
        let switching = self.previous.current_epoch().is_some();
        self.step(
            PmStepChangeV1::Epoch {
                source_sequence_epoch_id: self.input.admission.source_sequence_epoch_id.clone(),
            },
            None,
            vec![self.input.source.clone()],
            fresh(ResyncQuality::NotRequired),
        )?;
        // 与旧 BTreeMap 资产外围保持相同的确定顺序。
        let mut assets = self
            .context
            .assets
            .iter()
            .map(|item| &item.asset)
            .collect::<Vec<_>>();
        assets.sort_by(|a, b| a.token_id.0.cmp(&b.token_id.0));
        for asset in assets {
            let mut state = self.asset(asset)?;
            if switching && let Some(current) = &state.bba {
                state.bba_reanchor_frontier = Some(frontier(
                    state
                        .bba_reanchor_frontier
                        .iter()
                        .cloned()
                        .chain([current.evidence.frontier.clone()])
                        .collect(),
                )?);
            }
            let was_ready = state.book_ready;
            if switching && was_ready {
                let mut evidence = vec![self.input.source.clone()];
                for side in [BookSide::Bid, BookSide::Ask] {
                    for key in self.previous.level_keys(&asset.token_id.0, side) {
                        if let Some(value) =
                            self.previous.level_evidence(&asset.token_id.0, side, key)
                        {
                            evidence.push(value.frontier.clone());
                        }
                        if let Some(value) =
                            self.previous.absence_evidence(&asset.token_id.0, side, key)
                        {
                            evidence.push(value.frontier.clone());
                        }
                    }
                }
                evidence.extend(
                    state
                        .absence_baseline
                        .iter()
                        .map(|value| value.frontier.clone()),
                );
                state.absence_baseline = Some(pair(self.input.source, frontier(evidence)?));
            } else if switching && let Some(previous) = state.absence_baseline.take() {
                state.absence_baseline = Some(pair(
                    self.input.source,
                    frontier(vec![previous.frontier, self.input.source.clone()])?,
                ));
            }
            self.step(
                PmStepChangeV1::ResetAsset {
                    asset: asset.clone(),
                    absence_baseline: state.absence_baseline.clone(),
                    bba_reanchor_frontier: state.bba_reanchor_frontier.clone(),
                },
                None,
                vec![self.input.source.clone()],
                fresh(ResyncQuality::NotRequired),
            )?;
            if switching && was_ready {
                let reset = state
                    .absence_baseline
                    .as_ref()
                    .expect("ready state reset has baseline");
                self.quality(
                    asset,
                    MarketDataQuality::Fresh,
                    MarketDataQuality::Missing,
                    ResyncQuality::Required,
                    "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED",
                    ReductionOperationKindV1::Reject,
                    vec![reset.frontier.clone()],
                )?;
            }
            state.book_ready = false;
            state.bba = None;
            state.selection_baseline = None;
            self.assets.insert(asset.token_id.0.clone(), state);
        }
        self.reset = true;
        Ok(())
    }

    fn quality(
        &mut self,
        asset: &AssetIdentityV1,
        previous: MarketDataQuality,
        current: MarketDataQuality,
        resync: ResyncQuality,
        reason: &str,
        operation: ReductionOperationKindV1,
        causal: Vec<E>,
    ) -> Result<(), ContractError> {
        let mut evidence = self.dependencies(None);
        evidence.extend(causal);
        let derived_id = self.step(
            PmStepChangeV1::Quality {
                boundary: QualityBoundaryV2 {
                    scope: QualityScopeV1::Asset(asset.clone()),
                    previous_quality: previous,
                    current_quality: current,
                    reason_code: reason.to_owned(),
                },
            },
            None,
            evidence,
            EventQualityV2 {
                availability: current,
                resync,
            },
        )?;
        self.report(
            self.scope(asset),
            operation,
            reason,
            None,
            PmReductionDispositionV2::Produced { derived_id },
        );
        Ok(())
    }

    fn apply(&mut self) -> Result<(), ContractError> {
        match self
            .input
            .payload
            .expect("message reduction has its canonical payload")
        {
            CanonicalPmPayloadV1::Book {
                fact:
                    CanonicalBookFactV1::FullBook {
                        asset, bids, asks, ..
                    },
            } => self.book(asset, bids, asks),
            CanonicalPmPayloadV1::Book {
                fact: CanonicalBookFactV1::AbsoluteLevelChanges { changes },
            } => {
                for (index, change) in changes.iter().enumerate() {
                    self.member = Some(index as u32);
                    self.price_change(change)?;
                }
                Ok(())
            }
            CanonicalPmPayloadV1::BestBidAsk { asset, quote } => self.bba(asset, quote),
            CanonicalPmPayloadV1::TickSizeChange {
                asset,
                old_tick_size_pusd_per_share,
                new_tick_size_pusd_per_share,
            } => self.tick(
                asset,
                old_tick_size_pusd_per_share,
                new_tick_size_pusd_per_share,
            ),
            CanonicalPmPayloadV1::PublicTrade {
                asset,
                price_pusd_per_share,
                quantity_shares,
                side,
                fee_rate_bps,
            } => self.trade(
                asset,
                price_pusd_per_share,
                quantity_shares,
                *side,
                fee_rate_bps,
            ),
            CanonicalPmPayloadV1::NewMarket {
                clob_market_id,
                asset_ids,
                outcomes,
                ..
            } => {
                let expected = &self.context.assets;
                if clob_market_id != &self.context.market.clob_market_id.0
                    || asset_ids.len() != expected.len()
                    || !crate::reduction::same_decimal_id_set(
                        asset_ids.iter().map(String::as_str),
                        expected.iter().map(|value| value.asset.token_id.0.as_str()),
                    )
                    || asset_ids.iter().zip(outcomes).any(|(token, outcome)| {
                        expected
                            .iter()
                            .find(|value| &value.asset.token_id.0 == token)
                            .is_none_or(|value| {
                                !value
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
                self.no_event(
                    format!(
                        "polymarket_message:{}:{}:{}",
                        self.context.market.condition_id.0,
                        self.input.admission.source_sequence_epoch_id,
                        self.input.admission.collector_admission_sequence
                    ),
                    "POLYMARKET_NEW_MARKET_IDENTITY_CONFIRMED",
                    self.dependencies(None),
                    true,
                );
                Ok(())
            }
        }
    }

    fn book(
        &mut self,
        asset: &AssetIdentityV1,
        bids: &'m [PriceLevelV1],
        asks: &'m [PriceLevelV1],
    ) -> Result<(), ContractError> {
        let incoming_bids = input_levels(bids, "bids")?;
        let incoming_asks = input_levels(asks, "asks")?;
        let mut state = self.asset(asset)?;
        if !state.book_ready && !state.has_initial_snapshot {
            let mut evidence = self.dependencies(None);
            evidence.extend(state.tick_direct_evidence.clone());
            evidence.push(state.tick_frontier.clone());
            let baseline = pair(self.input.source, frontier(evidence.clone())?);
            let derived_id = self.step(
                PmStepChangeV1::InitialSnapshot {
                    asset: asset.clone(),
                    initial_tick_size_pusd_per_share: state.tick_size.clone(),
                    source_sequence_epoch_id: self.input.admission.source_sequence_epoch_id.clone(),
                    source_cursor: self.input.admission.collector_admission_sequence,
                    book_digest_sha256: self.input.source.prepared_book_digest(
                        asset,
                        &incoming_bids,
                        &incoming_asks,
                    )?,
                    bids: bids.into(),
                    asks: asks.into(),
                    evidence: baseline.clone(),
                },
                None,
                evidence,
                fresh(ResyncQuality::NotRequired),
            )?;
            self.report(
                self.scope(asset),
                ReductionOperationKindV1::Merge,
                "POLYMARKET_INITIAL_BOOK_BASELINE",
                None,
                PmReductionDispositionV2::Produced { derived_id },
            );
            // 首次快照步骤借用原数组，前序数量簿没有被复制。
            if self.public_selection {
                for (side, levels) in [
                    (BookSide::Bid, &incoming_bids),
                    (BookSide::Ask, &incoming_asks),
                ] {
                    for key in levels.keys() {
                        self.step(
                            PmStepChangeV1::Selection {
                                asset: asset.clone(),
                                side,
                                price: exact(*key),
                                selected: self.selected(true),
                                previous: None,
                                absence_baseline: None,
                            },
                            None,
                            vec![self.input.source.clone()],
                            fresh(ResyncQuality::NotRequired),
                        )?;
                    }
                }
            }
            self.step(
                PmStepChangeV1::AbsenceBaseline {
                    asset: asset.clone(),
                    evidence: baseline,
                    selected: self.public_selection.then(|| self.selected(false)),
                },
                None,
                vec![self.input.source.clone()],
                fresh(ResyncQuality::NotRequired),
            )?;
            return Ok(());
        }
        let recovering = !state.book_ready && state.has_initial_snapshot;
        let baseline = state
            .absence_baseline
            .clone()
            .ok_or_else(|| invalid("book", "post-initial book requires an L2 absence baseline"))?;
        let mut identical = true;
        let mut confirmation_evidence = self.dependencies(None);
        for (side, incoming) in [
            (BookSide::Bid, incoming_bids),
            (BookSide::Ask, incoming_asks),
        ] {
            let mut keys: BTreeSet<Decimal> = incoming.keys().copied().collect();
            if !self.reset {
                keys.extend(self.previous.level_keys(&asset.token_id.0, side));
            }
            let mut keys = keys.into_iter().collect::<Vec<_>>();
            if side == BookSide::Bid {
                keys.reverse();
            }
            for key in keys {
                let mut previous = self.level(asset, side, key);
                if let Some(level) = &previous.level {
                    confirmation_evidence.extend([level.direct.clone(), level.frontier.clone()]);
                }
                let incoming_level = incoming.get(&key).copied();
                if !self.public_selection && previous.level.is_none() && incoming_level.is_none() {
                    continue;
                }
                if !self.accepts(&state, &previous) {
                    identical = false;
                    continue;
                }
                let before = previous.quantity;
                let after = incoming_level
                    .map(|value| parse_canonical_decimal(&value.quantity_shares, "quantity_after"))
                    .transpose()?
                    .unwrap_or(Decimal::ZERO);
                let predecessor = previous
                    .level
                    .clone()
                    .or(previous.absence.clone())
                    .unwrap_or_else(|| baseline.clone());
                if before == after {
                    // 旧推导前像保持；新显式零键只保留原输入的直接证据和前沿。
                    if previous.level.is_none() && incoming_level.is_some() {
                        let input_evidence = pair(self.input.source, self.input.source.clone());
                        self.step_with_absence(
                            PmStepChangeV1::LevelEvidenceOnly {
                                asset: asset.clone(),
                                side,
                                price: exact(key),
                                evidence: input_evidence.clone(),
                            },
                            None,
                            Some(predecessor),
                            vec![self.input.source.clone()],
                            fresh(ResyncQuality::ResyncBoundary),
                        )?;
                        previous.level = Some(input_evidence);
                        identical = false;
                    }
                } else {
                    let evidence = self.dependencies(Some(&predecessor));
                    let next = pair(self.input.source, frontier(evidence.clone())?);
                    self.transition(
                        asset,
                        side,
                        key,
                        before,
                        after,
                        LevelExecutionBasis::BookResyncDeltaInference,
                        ResyncQuality::ResyncBoundary,
                        previous.level.is_none(),
                        predecessor,
                        next.clone(),
                        evidence,
                        "POLYMARKET_BOOK_RESYNC_DELTA",
                    )?;
                    previous.quantity = after;
                    previous.level = if after.is_zero() {
                        None
                    } else {
                        Some(next.clone())
                    };
                    previous.absence = if after.is_zero() { Some(next) } else { None };
                    identical = false;
                }
                self.select_level(asset, side, key, incoming_level.is_some(), previous, false)?;
            }
        }
        // 较旧全簿可以更新旧价位，但不能降低已经选中的全簿缺价依据。
        if !self.public_selection
            || state
                .selection_baseline
                .as_ref()
                .is_none_or(|previous| self.version().is_at_least(&previous.version))
        {
            let next = pair(
                self.input.source,
                frontier(vec![baseline.frontier.clone(), self.input.source.clone()])?,
            );
            self.step_with_absence(
                PmStepChangeV1::AbsenceBaseline {
                    asset: asset.clone(),
                    evidence: next.clone(),
                    selected: self.public_selection.then(|| self.selected(false)),
                },
                None,
                Some(baseline.clone()),
                vec![baseline.frontier.clone(), self.input.source.clone()],
                fresh(ResyncQuality::ResyncBoundary),
            )?;
            state.absence_baseline = Some(next);
        } else {
            // 空全簿没有逐价位比较，也须沿缺价版本取消较旧确认。
            identical = false;
        }
        if recovering || !self.public_selection {
            self.step(
                PmStepChangeV1::BookReady {
                    asset: asset.clone(),
                },
                None,
                vec![self.input.source.clone()],
                fresh(ResyncQuality::ResyncBoundary),
            )?;
        }
        if recovering {
            self.quality(
                asset,
                MarketDataQuality::Missing,
                MarketDataQuality::Fresh,
                ResyncQuality::ResyncBoundary,
                "POLYMARKET_L2_EPOCH_BASELINE_RESTORED",
                ReductionOperationKindV1::Overwrite,
                vec![baseline.direct, baseline.frontier],
            )?;
        } else if identical {
            self.no_event(
                self.scope(asset),
                "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION",
                confirmation_evidence,
                true,
            );
        }
        Ok(())
    }

    fn transition(
        &mut self,
        asset: &AssetIdentityV1,
        side: BookSide,
        key: Decimal,
        before: Decimal,
        after: Decimal,
        basis: LevelExecutionBasis,
        resync: ResyncQuality,
        was_absent: bool,
        predecessor: PmEvidencePairV1<E>,
        next: PmEvidencePairV1<E>,
        evidence: Vec<E>,
        reason: &str,
    ) -> Result<(), ContractError> {
        let dependency_time = time(&evidence);
        let source_time = dependency_time.source_event_at_utc_ms.ok_or_else(|| {
            invalid(
                "source_event_at_utc_ms",
                "Polymarket level transition requires platform source time",
            )
        })?;
        let summary = quantity_summary(before, after);
        let causal_group_id = self
            .input
            .source
            .source_digest(b"data-pipeline/polymarket-causal-group/v1\0")?;
        let derived_id = self.step_with_absence(
            PmStepChangeV1::LevelChange {
                transition: LevelExecutionTransitionV3 {
                    causal_group_id,
                    market: self.context.market.clone(),
                    asset: asset.clone(),
                    side,
                    price_pusd_per_share: exact(key),
                    quantity_before_shares: summary.quantity_before.clone(),
                    quantity_after_shares: summary.quantity_after.clone(),
                    attributed_trade_quantity_shares: exact(Decimal::ZERO),
                    already_covered_quantity_shares: exact(Decimal::ZERO),
                    net_added_quantity_shares: summary.quantity_added.clone(),
                    net_reduced_quantity_shares: summary.quantity_reduced.clone(),
                    queue_consumption_quantity_shares: summary.quantity_reduced.clone(),
                    basis,
                    platform_effective_at_utc_ms: source_time,
                    visible_at_utc_ms: dependency_time.received_at_utc_ms,
                    source_attribution_quality: SourceAttributionQuality::Unattributed,
                    queue_result_quality: QueueResultQuality::DeterministicInferred,
                },
                evidence: next,
            },
            (!was_absent).then(|| predecessor.clone()),
            was_absent.then_some(predecessor),
            evidence,
            fresh(resync),
        )?;
        let operation = if after.is_zero() {
            ReductionOperationKindV1::Delete
        } else if before.is_zero() {
            ReductionOperationKindV1::Merge
        } else {
            ReductionOperationKindV1::Overwrite
        };
        self.report(
            self.level_scope(asset, side, &exact(key)),
            operation,
            reason,
            Some(summary),
            PmReductionDispositionV2::Produced { derived_id },
        );
        Ok(())
    }

    fn price_change(
        &mut self,
        change: &CanonicalAbsoluteLevelChangeV1,
    ) -> Result<(), ContractError> {
        let side = match change.side {
            CanonicalPmSideV1::Buy => BookSide::Bid,
            CanonicalPmSideV1::Sell => BookSide::Ask,
            CanonicalPmSideV1::Unknown => {
                return Err(invalid(
                    "price_change.side",
                    "unknown side cannot be mapped to a book side",
                ));
            }
        };
        let asset = &change.asset;
        if let Some(quantity) = &change.quantity_shares {
            let state = self.asset(asset)?;
            if !state.book_ready {
                self.no_event(
                    self.scope(asset),
                    "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED",
                    vec![self.input.source.clone()],
                    false,
                );
                if let Some(quote) = &change.best_bid_ask {
                    self.bba(asset, quote)?;
                }
                return Ok(());
            }
            let key = parse_canonical_decimal(&change.price_pusd_per_share, "price_change.price")?;
            let baseline = state.absence_baseline.clone().ok_or_else(|| {
                invalid(
                    "price_change",
                    "book-ready state requires an L2 absence baseline",
                )
            })?;
            let mut previous = self.level(asset, side, key);
            if self.accepts(&state, &previous) {
                let before = previous.quantity;
                let after = parse_canonical_decimal(quantity, "quantity_after")?;
                let predecessor = previous
                    .level
                    .clone()
                    .or(previous.absence.clone())
                    .unwrap_or(baseline);
                let evidence = self.dependencies(Some(&predecessor));
                if before == after {
                    self.no_event(
                        self.level_scope(asset, side, &change.price_pusd_per_share),
                        "POLYMARKET_ABSOLUTE_LEVEL_CONFIRMATION",
                        evidence,
                        true,
                    );
                } else {
                    let next = pair(self.input.source, frontier(evidence.clone())?);
                    self.transition(
                        asset,
                        side,
                        key,
                        before,
                        after,
                        LevelExecutionBasis::LevelDeltaInference,
                        ResyncQuality::NotRequired,
                        previous.level.is_none(),
                        predecessor,
                        next.clone(),
                        evidence,
                        "POLYMARKET_ABSOLUTE_LEVEL_UPDATE",
                    )?;
                    previous.quantity = after;
                    previous.level = if after.is_zero() {
                        None
                    } else {
                        Some(next.clone())
                    };
                    previous.absence = if after.is_zero() { Some(next) } else { None };
                }
                self.select_level(asset, side, key, !after.is_zero(), previous, true)?;
            }
        }
        if let Some(quote) = &change.best_bid_ask {
            self.bba(asset, quote)?;
        } else if change.quantity_shares.is_none() {
            self.asset(asset)?;
            self.no_event(
                self.scope(asset),
                "POLYMARKET_PRICE_CHANGE_WITHOUT_STATE",
                vec![self.input.source.clone()],
                false,
            );
        }
        Ok(())
    }

    fn bba(
        &mut self,
        asset: &AssetIdentityV1,
        quote: &CanonicalBbaFactV1,
    ) -> Result<(), ContractError> {
        let bid = quote
            .best_bid_pusd_per_share
            .clone()
            .filter(|value| value.0 != "0");
        let ask = quote
            .best_ask_pusd_per_share
            .clone()
            .filter(|value| value.0 != "1");
        if let (Some(bid), Some(ask)) = (&bid, &ask) {
            let bid = parse_canonical_decimal(bid, "best_bid")?;
            let ask = parse_canonical_decimal(ask, "best_ask")?;
            if bid > ask {
                return Err(invalid("best_bid_ask", "best bid must not exceed best ask"));
            }
            if let Some(spread) = &quote.spread_pusd_per_share
                && parse_canonical_decimal(spread, "spread")? != ask - bid
            {
                return Err(invalid(
                    "spread",
                    "must equal best ask minus best bid when all values are present",
                ));
            }
        }
        let mut state = self.asset(asset)?;
        let mut evidence = self.dependencies(state.bba.as_ref().map(|value| &value.evidence));
        if state
            .bba
            .as_ref()
            .is_some_and(|value| value.best_bid == bid && value.best_ask == ask)
        {
            self.no_event(
                self.scope(asset),
                "POLYMARKET_BBA_CONFIRMATION",
                evidence.clone(),
                true,
            );
            return self.confirmation(asset, PmConfirmationComponentV1::BestBidAsk, evidence);
        }
        if state.bba.is_none() {
            evidence.extend(state.bba_reanchor_frontier.iter().cloned());
        }
        let next = pair(self.input.source, frontier(evidence.clone())?);
        let derived_id = self.step(
            PmStepChangeV1::BestBidAsk {
                quote: BestBidAskV2 {
                    asset: asset.clone(),
                    best_bid_pusd_per_share: bid.clone(),
                    best_ask_pusd_per_share: ask.clone(),
                },
                evidence: next.clone(),
            },
            state.bba.as_ref().map(|value| value.evidence.clone()),
            evidence,
            fresh(ResyncQuality::NotRequired),
        )?;
        self.report(
            self.scope(asset),
            if state.bba.is_some() {
                ReductionOperationKindV1::Overwrite
            } else {
                ReductionOperationKindV1::Merge
            },
            "POLYMARKET_BBA_UPDATE",
            None,
            PmReductionDispositionV2::Produced { derived_id },
        );
        state.bba = Some(PmBbaStateV1 {
            best_bid: bid,
            best_ask: ask,
            evidence: next,
        });
        state.bba_reanchor_frontier = None;
        self.assets.insert(asset.token_id.0.clone(), state);
        Ok(())
    }

    fn tick(
        &mut self,
        asset: &AssetIdentityV1,
        old: &Option<ExactDecimal>,
        new: &ExactDecimal,
    ) -> Result<(), ContractError> {
        let mut state = self.asset(asset)?;
        let mut evidence = state.tick_direct_evidence.clone();
        evidence.extend([state.tick_frontier.clone(), self.input.source.clone()]);
        if new == &state.tick_size {
            self.no_event(
                self.scope(asset),
                "POLYMARKET_TICK_CONFIRMATION",
                evidence.clone(),
                true,
            );
            return self.confirmation(asset, PmConfirmationComponentV1::Tick, evidence);
        }
        if old.as_ref().is_some_and(|value| value != &state.tick_size) {
            return Err(invalid(
                "old_tick_size",
                "does not match the current asset tick",
            ));
        }
        evidence.extend(self.context.identity_source_evidence.clone());
        let next = pair(self.input.source, frontier(evidence.clone())?);
        let derived_id = self.step(
            PmStepChangeV1::Tick {
                tick: TickSizeChangeV2 {
                    asset: asset.clone(),
                    old_tick_size_pusd_per_share: old.clone(),
                    new_tick_size_pusd_per_share: new.clone(),
                },
                evidence: next.clone(),
            },
            None,
            evidence,
            fresh(ResyncQuality::NotRequired),
        )?;
        self.report(
            self.scope(asset),
            ReductionOperationKindV1::Overwrite,
            "POLYMARKET_TICK_UPDATE",
            None,
            PmReductionDispositionV2::Produced { derived_id },
        );
        state.tick_size = new.clone();
        state.tick_direct_evidence = vec![next.direct];
        state.tick_frontier = next.frontier;
        self.assets.insert(asset.token_id.0.clone(), state);
        Ok(())
    }

    fn trade(
        &mut self,
        asset: &AssetIdentityV1,
        price: &ExactDecimal,
        quantity: &Option<ExactDecimal>,
        side: Option<CanonicalPmSideV1>,
        fee: &Option<ExactDecimal>,
    ) -> Result<(), ContractError> {
        self.asset(asset)?;
        let (Some(quantity), Some(side)) = (quantity, side) else {
            self.no_event(
                self.scope(asset),
                "POLYMARKET_PUBLIC_TRADE_INCOMPLETE",
                vec![self.input.source.clone()],
                false,
            );
            return Ok(());
        };
        let side = match side {
            CanonicalPmSideV1::Buy => TradeSide::Buy,
            CanonicalPmSideV1::Sell => TradeSide::Sell,
            CanonicalPmSideV1::Unknown => {
                self.no_event(
                    self.scope(asset),
                    "POLYMARKET_PUBLIC_TRADE_UNKNOWN_SIDE",
                    vec![self.input.source.clone()],
                    false,
                );
                return Ok(());
            }
        };
        if parse_canonical_decimal(quantity, "last_trade_price.size")?.is_zero() {
            return Err(invalid(
                "last_trade_price.size",
                "a public trade quantity must be positive",
            ));
        }
        let trade_id = self
            .input
            .source
            .source_digest(b"data-pipeline/polymarket-public-trade-id/v1\0")?;
        let derived_id = self.step(
            PmStepChangeV1::PublicTrade {
                trade: PublicTradeV2 {
                    asset: asset.clone(),
                    trade_id: trade_id.0,
                    trade_id_source: PublicTradeIdSourceV1::DerivedSourceEvidence,
                    price_pusd_per_share: price.clone(),
                    quantity_shares: quantity.clone(),
                    aggressor_side: side,
                    fee_rate_bps: fee.clone(),
                },
            },
            None,
            self.dependencies(None),
            fresh(ResyncQuality::NotRequired),
        )?;
        self.report(
            self.scope(asset),
            ReductionOperationKindV1::Merge,
            "POLYMARKET_PUBLIC_TRADE_ACCEPTED",
            None,
            PmReductionDispositionV2::Produced { derived_id },
        );
        Ok(())
    }
}

fn input_levels<'a>(
    levels: &'a [PriceLevelV1],
    field: &str,
) -> Result<BTreeMap<Decimal, &'a PriceLevelV1>, ContractError> {
    let mut mapped = BTreeMap::new();
    for level in levels {
        let price = parse_canonical_decimal(&level.price_pusd_per_share, field)?;
        if mapped.insert(price, level).is_some() {
            return Err(invalid(field, "contains a duplicate price level"));
        }
    }
    Ok(mapped)
}

fn book_digest(
    asset: &AssetIdentityV1,
    bids: &BTreeMap<Decimal, &PriceLevelV1>,
    asks: &BTreeMap<Decimal, &PriceLevelV1>,
) -> Result<Sha256Hex, ContractError> {
    #[derive(Serialize)]
    struct Material<'a> {
        asset: &'a AssetIdentityV1,
        bids: Vec<&'a PriceLevelV1>,
        asks: Vec<&'a PriceLevelV1>,
    }
    let material = Material {
        asset,
        bids: bids.values().rev().copied().collect(),
        asks: asks.values().copied().collect(),
    };
    Ok(hash_domain_bytes(
        b"data-pipeline/polymarket-book-digest/v1\0",
        &stable_json(&material)?,
    ))
}

/// 只编码已经计算完成的不可变步骤；不读取或推进调用方状态。
pub fn materialize_pm_reduction<'m, E: PmEvidenceV1, I: PmStepIndexV1>(
    reduction: PmMessageReductionV1<'m, E, I>,
) -> Result<PmMessageReductionV1<'m, E>, ContractError> {
    let mut ids = Vec::with_capacity(reduction.steps.len());
    let mut steps = Vec::with_capacity(reduction.steps.len());
    for step in reduction.steps {
        let derived_id = CanonicalEventId(
            hash_domain_bytes(
                b"data-pipeline/pm-derived-step/v1\0",
                &stable_json(&(
                    &reduction.materialization_rule_version,
                    &reduction.materialization_market,
                    &step.canonical_fact_id,
                    step.member_index,
                    step.step_index,
                    &step.change,
                    &step.prestate,
                    &step.absence,
                    &step.dependencies,
                    &step.quality,
                ))?,
            )
            .0,
        );
        ids.push(derived_id.clone());
        steps.push(PmReductionStepV1 {
            canonical_fact_id: step.canonical_fact_id,
            member_index: step.member_index,
            step_index: step.step_index,
            derived_id,
            change: step.change,
            direct: step.direct,
            prestate: step.prestate,
            absence: step.absence,
            dependencies: step.dependencies,
            direct_time: step.direct_time,
            dependency_time: step.dependency_time,
            quality: step.quality,
        });
    }
    let reports = reduction
        .reports
        .into_iter()
        .map(|report| PmReductionReportV2 {
            canonical_fact_id: report.canonical_fact_id,
            member_index: report.member_index,
            canonical_rule_version: report.canonical_rule_version,
            scope: report.scope,
            operation: report.operation,
            reason_code: report.reason_code,
            quantity_conservation: report.quantity_conservation,
            disposition: match report.disposition {
                PmReductionDispositionV2::Produced { derived_id } => {
                    PmReductionDispositionV2::Produced {
                        derived_id: ids[derived_id.step_index() as usize].clone(),
                    }
                }
                PmReductionDispositionV2::NoOutput { operation_evidence } => {
                    PmReductionDispositionV2::NoOutput { operation_evidence }
                }
                PmReductionDispositionV2::Suppressed { operation_evidence } => {
                    PmReductionDispositionV2::Suppressed { operation_evidence }
                }
            },
        })
        .collect();
    Ok(PmMessageReductionV1 {
        steps,
        reports,
        touched_level_entries: reduction.touched_level_entries,
        materialization_market: reduction.materialization_market,
        materialization_rule_version: reduction.materialization_rule_version,
    })
}

/// 后台来源准备保存原规范字节，热计算只比较已有键。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct PreparedPmEvidenceV1<E> {
    value: E,
    #[serde(skip)]
    physical_key: Vec<u8>,
    #[serde(skip)]
    operation_keys: Vec<Vec<u8>>,
    #[serde(skip)]
    source_identity: std::sync::Arc<PreparedPmSourceIdentityV1>,
}
impl<E: PmEvidenceV1> PreparedPmEvidenceV1<E> {
    pub fn prepare(
        value: E,
        payload: Option<&CanonicalPmPayloadV1>,
    ) -> Result<Self, ContractError> {
        // 沿旧入口的单项证据检查；组合的规范化直接使用原键。
        E::canonicalize(vec![value.clone()])?;
        let semantic_key = value.semantic_bytes()?;
        let physical_key = stable_json(&value)?;
        let operation_keys = [
            ReductionEvidenceRoleV1::Context,
            ReductionEvidenceRoleV1::Input,
            ReductionEvidenceRoleV1::MatchedExisting,
            ReductionEvidenceRoleV1::IncomingDuplicate,
        ]
        .into_iter()
        .map(|role| {
            stable_json(&crate::reduction::ReductionOperationEvidenceV1 {
                role,
                source_evidence: value.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
        let mut book_digests = BTreeMap::new();
        if let Some(CanonicalPmPayloadV1::Book {
            fact:
                CanonicalBookFactV1::FullBook {
                    asset, bids, asks, ..
                },
        }) = payload
        {
            book_digests.insert(
                asset.token_id.0.clone(),
                book_digest(
                    asset,
                    &input_levels(bids, "bids")?,
                    &input_levels(asks, "asks")?,
                )?,
            );
        }
        Ok(Self {
            source_identity: std::sync::Arc::new(PreparedPmSourceIdentityV1 {
                causal_group_id: hash_domain_bytes(
                    b"data-pipeline/polymarket-causal-group/v1\0",
                    &semantic_key,
                ),
                public_trade_id: hash_domain_bytes(
                    b"data-pipeline/polymarket-public-trade-id/v1\0",
                    &semantic_key,
                ),
                evidence_semantic_key: semantic_key,
                book_digests_by_member: book_digests,
            }),
            value,
            physical_key,
            operation_keys,
        })
    }

    pub fn source(&self) -> &E {
        &self.value
    }
    pub fn source_identity(&self) -> &PreparedPmSourceIdentityV1 {
        &self.source_identity
    }
    pub fn shared_source_identity(&self) -> std::sync::Arc<PreparedPmSourceIdentityV1> {
        self.source_identity.clone()
    }
    pub(crate) fn operation_key(&self, role: ReductionEvidenceRoleV1) -> &[u8] {
        &self.operation_keys[match role {
            ReductionEvidenceRoleV1::Context => 0,
            ReductionEvidenceRoleV1::Input => 1,
            ReductionEvidenceRoleV1::MatchedExisting => 2,
            ReductionEvidenceRoleV1::IncomingDuplicate => 3,
        }]
    }
}
impl<E: PmEvidenceV1> sealed::Evidence for PreparedPmEvidenceV1<E> {}
impl<E: PmEvidenceV1> PmEvidenceV1 for PreparedPmEvidenceV1<E> {
    fn is_metadata_identity(&self) -> bool {
        self.value.is_metadata_identity()
    }
    fn source_time(&self) -> Option<i64> {
        self.value.source_time()
    }
    fn received_time(&self) -> i64 {
        self.value.received_time()
    }
    fn semantic_bytes(&self) -> Result<Vec<u8>, ContractError> {
        Ok(self.source_identity.evidence_semantic_key.clone())
    }
    fn canonicalize(mut values: Vec<Self>) -> Result<Vec<Self>, ContractError> {
        values.sort_by(|a, b| {
            a.source_identity
                .evidence_semantic_key
                .cmp(&b.source_identity.evidence_semantic_key)
                .then_with(|| a.physical_key.cmp(&b.physical_key))
        });
        values.dedup_by(|a, b| {
            a.source_identity.evidence_semantic_key == b.source_identity.evidence_semantic_key
        });
        Ok(values)
    }
    fn source_digest(&self, domain: &[u8]) -> Result<Sha256Hex, ContractError> {
        Ok(match domain {
            b"data-pipeline/polymarket-causal-group/v1\0" => {
                self.source_identity.causal_group_id.clone()
            }
            b"data-pipeline/polymarket-public-trade-id/v1\0" => {
                self.source_identity.public_trade_id.clone()
            }
            _ => unreachable!("共同业务算法只有两类来源摘要"),
        })
    }
    fn prepared_book_digest(
        &self,
        asset: &AssetIdentityV1,
        _: &BTreeMap<Decimal, &PriceLevelV1>,
        _: &BTreeMap<Decimal, &PriceLevelV1>,
    ) -> Result<Sha256Hex, ContractError> {
        Ok(self.source_identity.book_digests_by_member[&asset.token_id.0].clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedPmSourceIdentityV1 {
    pub causal_group_id: Sha256Hex,
    pub public_trade_id: Sha256Hex,
    pub evidence_semantic_key: Vec<u8>,
    pub book_digests_by_member: BTreeMap<String, Sha256Hex>,
}

pub struct PreparedPmMessageV1 {
    message: CanonicalPmMessageV1,
    evidence: PreparedPmEvidenceV1<SourceEvidenceRefV3>,
}
impl PreparedPmMessageV1 {
    pub fn prepare(message: CanonicalPmMessageV1) -> Result<Self, ContractError> {
        let parsed = crate::pm_message::parse_canonical_pm_message(&message)?;
        let evidence =
            PreparedPmEvidenceV1::prepare(parsed.source.clone(), Some(&message.payload))?;
        Ok(Self { message, evidence })
    }
    pub fn message(&self) -> &CanonicalPmMessageV1 {
        &self.message
    }
    pub fn evidence(&self) -> &PreparedPmEvidenceV1<SourceEvidenceRefV3> {
        &self.evidence
    }
}

/// 只消费已经准备的来源与调用方前序；报告和步骤引用同一完整逻辑位置。
pub fn compute_pm_message_steps_v1<'m>(
    context: &PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>,
    previous: &impl PmStateReadV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>,
    message: &'m PreparedPmMessageV1,
    consumer_stream_id: &str,
    canonical_record_position: u64,
    public_selection: bool,
) -> Result<
    PmMessageReductionV1<'m, PreparedPmEvidenceV1<SourceEvidenceRefV3>, PmDerivedStepPositionV1>,
    ContractError,
> {
    let result = compute_message(
        context,
        previous,
        PmMessageInput {
            fact_id: message.message.event_id.clone(),
            payload: Some(&message.message.payload),
            source: &message.evidence,
            source_at: message
                .evidence
                .value
                .source_event_at_utc_ms
                .expect("准备已取得来源时间"),
            admission: message
                .evidence
                .value
                .collector_admission
                .as_ref()
                .expect("准备已取得准入坐标"),
        },
        public_selection,
        true,
    )?;
    let positions: Vec<_> = result
        .steps
        .iter()
        .map(|step| PmDerivedStepPositionV1 {
            consumer_stream_id: consumer_stream_id.to_owned(),
            canonical_record_position,
            member_index: step.member_index,
            step_index: step.step_index,
        })
        .collect();
    let steps = result
        .steps
        .into_iter()
        .map(|step| PmReductionStepV1 {
            derived_id: positions[step.step_index as usize].clone(),
            canonical_fact_id: step.canonical_fact_id,
            member_index: step.member_index,
            step_index: step.step_index,
            change: step.change,
            direct: step.direct,
            prestate: step.prestate,
            absence: step.absence,
            dependencies: step.dependencies,
            direct_time: step.direct_time,
            dependency_time: step.dependency_time,
            quality: step.quality,
        })
        .collect();
    let reports = result
        .reports
        .into_iter()
        .map(|report| PmReductionReportV2 {
            canonical_fact_id: report.canonical_fact_id,
            member_index: report.member_index,
            canonical_rule_version: report.canonical_rule_version,
            scope: report.scope,
            operation: report.operation,
            reason_code: report.reason_code,
            quantity_conservation: report.quantity_conservation,
            disposition: match report.disposition {
                PmReductionDispositionV2::Produced { derived_id } => {
                    PmReductionDispositionV2::Produced {
                        derived_id: positions[derived_id as usize].clone(),
                    }
                }
                PmReductionDispositionV2::NoOutput { operation_evidence } => {
                    PmReductionDispositionV2::NoOutput { operation_evidence }
                }
                PmReductionDispositionV2::Suppressed { operation_evidence } => {
                    PmReductionDispositionV2::Suppressed { operation_evidence }
                }
            },
        })
        .collect();
    Ok(PmMessageReductionV1 {
        steps,
        reports,
        touched_level_entries: result.touched_level_entries,
        materialization_market: result.materialization_market,
        materialization_rule_version: result.materialization_rule_version,
    })
}

/// 仅支持内部消息序号与已签发的完整公共步骤位置。
pub trait PmStepIndexV1: sealed::StepIndex {
    fn step_index(&self) -> u32;
}
impl sealed::StepIndex for u32 {}
impl PmStepIndexV1 for u32 {
    fn step_index(&self) -> u32 {
        *self
    }
}
impl sealed::StepIndex for PmDerivedStepPositionV1 {}
impl PmStepIndexV1 for PmDerivedStepPositionV1 {
    fn step_index(&self) -> u32 {
        self.step_index
    }
}

pub type PmLogicalStepV1<'m> =
    PmReductionStepV1<'m, PreparedPmEvidenceV1<SourceEvidenceRefV3>, PmDerivedStepPositionV1>;

pub struct MaterializedPmReductionV1<'m> {
    pub reduction: PmMessageReductionV1<'m, PreparedPmEvidenceV1<SourceEvidenceRefV3>>,
    pub bindings: Vec<PmDerivedStepBindingV1>,
}

/// 后台物化已捕获的步骤材料，同时保存公共位置到独立派生身份的绑定。
pub fn materialize_pm_step_identity_v1<'m>(
    reduction: PmMessageReductionV1<
        'm,
        PreparedPmEvidenceV1<SourceEvidenceRefV3>,
        PmDerivedStepPositionV1,
    >,
) -> Result<MaterializedPmReductionV1<'m>, ContractError> {
    let positions = reduction
        .steps
        .iter()
        .map(|step| step.derived_id.clone())
        .collect::<Vec<_>>();
    let reduction = materialize_pm_reduction(reduction)?;
    let bindings = positions
        .into_iter()
        .zip(&reduction.steps)
        .map(|(position, step)| PmDerivedStepBindingV1 {
            position,
            derived_id: DerivedPmStepIdV1(Sha256Hex(step.derived_id.0.clone())),
        })
        .collect();
    Ok(MaterializedPmReductionV1 {
        reduction,
        bindings,
    })
}

#[cfg(test)]
mod no_encoding_tests {
    use super::*;
    type E = PreparedPmEvidenceV1<SourceEvidenceRefV3>;
    struct InitialState {
        market: MarketIdentityV2,
        asset: PmAssetStateV1<E>,
    }
    impl PmStateReadV1<E> for InitialState {
        fn market(&self) -> &MarketIdentityV2 {
            &self.market
        }
        fn current_epoch(&self) -> Option<&str> {
            None
        }
        fn asset_state(&self, _: &str) -> Option<PmAssetReadV1<'_, E>> {
            Some(self.asset.as_read())
        }
        fn quantity(&self, _: &str, _: BookSide, _: Decimal) -> Decimal {
            Decimal::ZERO
        }
        fn level_evidence(
            &self,
            _: &str,
            _: BookSide,
            _: Decimal,
        ) -> Option<PmEvidenceReadV1<'_, E>> {
            None
        }
        fn absence_evidence(
            &self,
            _: &str,
            _: BookSide,
            _: Decimal,
        ) -> Option<PmEvidenceReadV1<'_, E>> {
            None
        }
        fn selected_level(
            &self,
            _: &str,
            _: BookSide,
            _: Decimal,
        ) -> Option<&PmSelectedLevelV1<E>> {
            None
        }
        fn level_keys<'a>(
            &'a self,
            _: &'a str,
            _: BookSide,
        ) -> Box<dyn Iterator<Item = Decimal> + 'a> {
            Box::new(std::iter::empty())
        }
    }
    #[test]
    fn prepared_full_book_steps_and_initial_digest_do_not_encode() {
        let raw_identity = SourceEvidenceRefV2 {
            source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
            source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                sha256: Sha256Hex("a".repeat(64)),
            }),
            source_event_at_utc_ms: None,
            received_at_utc_ms: 800,
            collector_admission: None,
            input_manifest_sha256: Sha256Hex("b".repeat(64)),
            input_file_sha256: Sha256Hex("c".repeat(64)),
        };
        let identity =
            crate::public_reference::source_evidence_v3_from_existing(&raw_identity).unwrap();
        let market = MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            clob_market_id: ClobMarketId("123".to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: 1000,
                end_at_utc_ms: 301000,
            },
        };
        let assets = ["123", "456"]
            .into_iter()
            .enumerate()
            .map(|(index, token)| PolymarketAssetContextV1 {
                asset: AssetIdentityV1 {
                    token_id: TokenId(token.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: index as u32,
                        label: if index == 0 { "UP" } else { "DOWN" }.to_owned(),
                    },
                },
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            })
            .collect::<Vec<_>>();
        let raw_context = PmMarketContextV1 {
            market: market.clone(),
            assets: assets.clone(),
            identity_source_evidence: vec![identity.clone()],
            canonical_rule_version: "rule".to_owned(),
        };
        let source =
            crate::public_reference::source_evidence_v3_from_existing(&SourceEvidenceRefV2 {
                source_kind: CanonicalSourceKind::PolymarketOrderbook,
                source_native_identity: None,
                source_event_at_utc_ms: Some(1100),
                received_at_utc_ms: 1000,
                collector_admission: Some(CollectorAdmissionCoordinateV1 {
                    source_sequence_epoch_id: "epoch".to_owned(),
                    collector_admission_sequence: 1,
                    admitted_at_utc_ms: 1001,
                }),
                input_manifest_sha256: Sha256Hex("b".repeat(64)),
                input_file_sha256: Sha256Hex("c".repeat(64)),
            })
            .unwrap();
        let message = crate::pm_message::build_canonical_pm_message(
            &raw_context,
            source,
            CanonicalInputRecordRefV1 {
                input_stream_id: "input".to_owned(),
                input_position: 0,
            },
            CanonicalPmPayloadV1::Book {
                fact: CanonicalBookFactV1::FullBook {
                    asset: assets[0].asset.clone(),
                    bids: vec![PriceLevelV1 {
                        price_pusd_per_share: ExactDecimal("0.5".to_owned()),
                        quantity_shares: ExactDecimal("1".to_owned()),
                    }],
                    asks: vec![PriceLevelV1 {
                        price_pusd_per_share: ExactDecimal("0.6".to_owned()),
                        quantity_shares: ExactDecimal("2".to_owned()),
                    }],
                    last_trade_price_pusd_per_share: None,
                },
            },
        )
        .unwrap();
        let message = PreparedPmMessageV1::prepare(message).unwrap();
        let evidence = E::prepare(identity, None).unwrap();
        let context = PmMarketContextV1 {
            market: market.clone(),
            assets,
            identity_source_evidence: vec![evidence.clone()],
            canonical_rule_version: "rule".to_owned(),
        };
        let state = InitialState {
            market,
            asset: PmAssetStateV1 {
                has_initial_snapshot: false,
                book_ready: false,
                tick_size: ExactDecimal("0.01".to_owned()),
                tick_direct_evidence: vec![evidence.clone()],
                tick_frontier: evidence,
                absence_baseline: None,
                selection_baseline: None,
                bba: None,
                bba_reanchor_frontier: None,
            },
        };
        for public_selection in [false, true] {
            crate::without_identity_encoding(|| {
                let result = compute_pm_message_steps_v1(
                    &context,
                    &state,
                    &message,
                    "consumer",
                    0,
                    public_selection,
                )
                .unwrap()
                .into_owned();
                assert!(result.steps.iter().any(|step| matches!(&step.change, PmStepChangeV1::InitialSnapshot { bids, asks, .. } if bids.len() == 1 && asks.len() == 1)));
                assert!(
                    result
                        .steps
                        .iter()
                        .all(|step| step.derived_id.consumer_stream_id == "consumer")
                );
                assert!(
                    result
                        .steps
                        .iter()
                        .any(|step| step.direct_time.received_at_utc_ms
                            - step.direct_time.source_event_at_utc_ms.unwrap()
                            == -100)
                );
            });
        }
    }
}

/// 交接当前不可变结果；只持有仍借用的当前快照成员，不编码或再归约。
impl<E, I> PmMessageReductionV1<'_, E, I> {
    pub fn into_owned(self) -> PmMessageReductionV1<'static, E, I> {
        PmMessageReductionV1 {
            steps: self
                .steps
                .into_iter()
                .map(|s| PmReductionStepV1 {
                    canonical_fact_id: s.canonical_fact_id,
                    member_index: s.member_index,
                    step_index: s.step_index,
                    derived_id: s.derived_id,
                    change: s.change.into_owned(),
                    direct: s.direct,
                    prestate: s.prestate,
                    absence: s.absence,
                    dependencies: s.dependencies,
                    direct_time: s.direct_time,
                    dependency_time: s.dependency_time,
                    quality: s.quality,
                })
                .collect(),
            reports: self.reports,
            touched_level_entries: self.touched_level_entries,
            materialization_market: self.materialization_market,
            materialization_rule_version: self.materialization_rule_version,
        }
    }
}
