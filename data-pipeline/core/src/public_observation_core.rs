//! 同步公开核心：唯一业务状态、真实封组与逐事实投影，无 I/O 或身份编码。
use crate::pm_reduction::*;
use crate::public_input::*;
use crate::public_reduction::*;
use crate::reduction::*;
use crate::typed_raw::*;
use crate::{ContractError, invalid};
use contracts::*;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

type LevelKey = (String, bool, Decimal);

/// 按需读取的证据对象数量，不是字节数；不包含字符串容量或临时归约材料。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PmEvidenceObjectCountsV1 {
    pub markets: usize,
    pub assets: usize,
    pub levels: usize,
    pub absences: usize,
    pub selected: usize,
}

#[derive(Clone, Debug)]
pub struct PmOwnedStateV1<E = SourceEvidenceRefV3> {
    context: PmMarketContextV1<E>,
    epoch: Option<String>,
    assets: BTreeMap<String, PmAssetStateV1<E>>,
    quantities: Option<BTreeMap<LevelKey, Decimal>>,
    levels: BTreeMap<LevelKey, PmEvidencePairV1<E>>,
    absences: BTreeMap<LevelKey, PmEvidencePairV1<E>>,
    selected: BTreeMap<LevelKey, PmSelectedLevelV1<E>>,
}

fn key(token: &str, side: BookSide, price: Decimal) -> LevelKey {
    (token.to_owned(), side == BookSide::Ask, price)
}
fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

impl<E: PmEvidenceV1> PmOwnedStateV1<E> {
    pub fn evidence_object_counts(&self) -> PmEvidenceObjectCountsV1 {
        PmEvidenceObjectCountsV1 {
            markets: 1,
            assets: self.assets.len(),
            levels: self.levels.len(),
            absences: self.absences.len(),
            selected: self.selected.len(),
        }
    }
    pub fn context(&self) -> &PmMarketContextV1<E> {
        &self.context
    }
    pub fn new(context: PmMarketContextV1<E>) -> Self {
        let evidence = context.identity_source_evidence[0].clone();
        let assets = context
            .assets
            .iter()
            .map(|value| {
                (
                    value.asset.token_id.0.clone(),
                    PmAssetStateV1 {
                        has_initial_snapshot: false,
                        book_ready: false,
                        tick_size: value.initial_tick_size_pusd_per_share.clone(),
                        tick_direct_evidence: vec![evidence.clone()],
                        tick_frontier: evidence.clone(),
                        absence_baseline: None,
                        selection_baseline: None,
                        bba: None,
                        bba_reanchor_frontier: None,
                    },
                )
            })
            .collect();
        Self {
            context,
            epoch: None,
            assets,
            quantities: Some(BTreeMap::new()),
            levels: BTreeMap::new(),
            absences: BTreeMap::new(),
            selected: BTreeMap::new(),
        }
    }
    /// 模拟器仅持有原归约证据，数量通过其已有数量簿借用。
    pub fn new_evidence_only(context: PmMarketContextV1<E>) -> Self {
        let mut state = Self::new(context);
        state.quantities = None;
        state
    }
    pub fn apply<I>(&mut self, result: &PmMessageReductionV1<'_, E, I>) {
        for step in &result.steps {
            self.apply_step(step);
        }
    }
    pub fn apply_step<I>(&mut self, step: &PmReductionStepV1<'_, E, I>) {
        match &step.change {
            PmStepChangeV1::Epoch {
                source_sequence_epoch_id,
            } => self.epoch = Some(source_sequence_epoch_id.clone()),
            PmStepChangeV1::ResetAsset {
                asset,
                absence_baseline,
                bba_reanchor_frontier,
            } => {
                let token = &asset.token_id.0;
                if let Some(quantities) = &mut self.quantities {
                    quantities.retain(|(id, ..), _| id != token);
                }
                self.levels.retain(|(id, ..), _| id != token);
                self.absences.retain(|(id, ..), _| id != token);
                self.selected.retain(|(id, ..), _| id != token);
                let state = self.assets.get_mut(token).unwrap();
                state.book_ready = false;
                state.absence_baseline = absence_baseline.clone();
                state.selection_baseline = None;
                state.bba = None;
                state.bba_reanchor_frontier = bba_reanchor_frontier.clone();
            }
            PmStepChangeV1::InitialSnapshot {
                asset,
                bids,
                asks,
                evidence,
                ..
            } => {
                let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                state.has_initial_snapshot = true;
                state.book_ready = true;
                for (side, values) in [
                    (BookSide::Bid, bids.as_ref()),
                    (BookSide::Ask, asks.as_ref()),
                ] {
                    for value in values {
                        let key = key(&asset.token_id.0, side, d(&value.price_pusd_per_share.0));
                        let quantity = d(&value.quantity_shares.0);
                        if !quantity.is_zero() {
                            if let Some(quantities) = &mut self.quantities {
                                quantities.insert(key.clone(), quantity);
                            }
                        }
                        self.levels.insert(key, evidence.clone());
                    }
                }
            }
            PmStepChangeV1::LevelChange {
                transition,
                evidence,
            } => {
                let key = key(
                    &transition.asset.token_id.0,
                    transition.side,
                    d(&transition.price_pusd_per_share.0),
                );
                let after = d(&transition.quantity_after_shares.0);
                if after.is_zero() {
                    if let Some(quantities) = &mut self.quantities {
                        quantities.remove(&key);
                    }
                    self.levels.remove(&key);
                    self.absences.insert(key, evidence.clone());
                } else {
                    if let Some(quantities) = &mut self.quantities {
                        quantities.insert(key.clone(), after);
                    }
                    self.absences.remove(&key);
                    self.levels.insert(key, evidence.clone());
                }
            }
            PmStepChangeV1::LevelEvidenceOnly {
                asset,
                side,
                price,
                evidence,
            } => {
                self.levels
                    .insert(key(&asset.token_id.0, *side, d(&price.0)), evidence.clone());
            }
            PmStepChangeV1::Selection {
                asset,
                side,
                price,
                selected,
                ..
            } => {
                self.selected
                    .insert(key(&asset.token_id.0, *side, d(&price.0)), selected.clone());
            }
            PmStepChangeV1::AbsenceBaseline {
                asset,
                evidence,
                selected,
            } => {
                let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                state.absence_baseline = Some(evidence.clone());
                state.selection_baseline = selected.clone();
            }
            PmStepChangeV1::BookReady { asset } => {
                self.assets.get_mut(&asset.token_id.0).unwrap().book_ready = true
            }
            PmStepChangeV1::BestBidAsk { quote, evidence } => {
                let state = self.assets.get_mut(&quote.asset.token_id.0).unwrap();
                state.bba = Some(PmBbaStateV1 {
                    best_bid: quote.best_bid_pusd_per_share.clone(),
                    best_ask: quote.best_ask_pusd_per_share.clone(),
                    evidence: evidence.clone(),
                });
                state.bba_reanchor_frontier = None;
            }
            PmStepChangeV1::Tick { tick, evidence } => {
                let state = self.assets.get_mut(&tick.asset.token_id.0).unwrap();
                state.tick_size = tick.new_tick_size_pusd_per_share.clone();
                state.tick_direct_evidence = vec![evidence.direct.clone()];
                state.tick_frontier = evidence.frontier.clone();
            }
            PmStepChangeV1::PublicConfirmation { .. }
            | PmStepChangeV1::PublicTrade { .. }
            | PmStepChangeV1::Quality { .. } => {}
        }
    }
}

impl<E: PmEvidenceV1> PmStateReadV1<E> for PmOwnedStateV1<E> {
    fn market(&self) -> &MarketIdentityV2 {
        &self.context.market
    }
    fn current_epoch(&self) -> Option<&str> {
        self.epoch.as_deref()
    }
    fn asset_state(&self, token: &str) -> Option<PmAssetReadV1<'_, E>> {
        self.assets.get(token).map(PmAssetStateV1::as_read)
    }
    fn quantity(&self, token: &str, side: BookSide, price: Decimal) -> Decimal {
        self.quantities
            .as_ref()
            .and_then(|quantities| quantities.get(&key(token, side, price)))
            .copied()
            .unwrap_or(Decimal::ZERO)
    }
    fn level_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PmEvidenceReadV1<'_, E>> {
        self.levels
            .get(&key(token, side, price))
            .map(PmEvidencePairV1::as_read)
    }
    fn absence_evidence(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PmEvidenceReadV1<'_, E>> {
        self.absences
            .get(&key(token, side, price))
            .map(PmEvidencePairV1::as_read)
    }
    fn selected_level(
        &self,
        token: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<&PmSelectedLevelV1<E>> {
        self.selected.get(&key(token, side, price))
    }
    fn level_keys<'a>(
        &'a self,
        token: &'a str,
        side: BookSide,
    ) -> Box<dyn Iterator<Item = Decimal> + 'a> {
        Box::new(
            self.levels
                .keys()
                .chain(self.absences.keys())
                .chain(self.selected.keys())
                .filter(move |(id, ask, _)| id == token && *ask == (side == BookSide::Ask))
                .map(|(_, _, price)| *price),
        )
    }
}

type E = PreparedPublicEvidenceV1;
pub type PublicPmReductionV1<'a> = PmMessageReductionV1<'a, E, PmDerivedStepPositionV1>;

/// 组事实位置和同次 PM 步骤/报告、其他业务物化材料一一关联。
pub struct PublicAppliedIngressV1<'a> {
    pub applied: AppliedIngressV1,
    pub pm_reductions: Vec<(PublicFactPositionV1, PublicPmReductionV1<'a>)>,
    pub reductions: Vec<PublicReductionBatchV1>,
}
/// 完整事实消费；保留原归约、撮合步骤和合法前缀，不展开模型观察或全部盘口。
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublicFactGroupV1 {
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub member_input_positions: Vec<u64>,
    pub closed_at_input_position: u64,
    pub visible_at_utc_ms: i64,
    pub records: Arc<[PublicBusinessFactV1]>,
    pub source_progress_changes: Vec<SourceStreamProgressV1>,
}
pub struct PublicFactAppliedIngressV1<'a> {
    pub applied_prefix: AppliedIngressPrefixV1,
    pub groups: Vec<PublicFactGroupV1>,
    pub pm_reductions: Vec<(PublicFactPositionV1, PublicPmReductionV1<'a>)>,
    pub reductions: Vec<PublicReductionBatchV1>,
}
#[derive(Clone, Copy)]
enum PublicOutputModeV1 {
    Observations,
    SourceProgress,
    Facts,
}
/// 研究专用事实增量；没有完整观察、撮合材料或持久前缀承诺。
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PublicSourceProgressGroupV1 {
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub member_input_positions: Vec<u64>,
    pub closed_at_input_position: u64,
    pub visible_at_utc_ms: i64,
    /// 仅本组变更的来源；代际导致的其它资产同步变化也在此交付。
    pub source_progress_changes: Vec<SourceStreamProgressV1>,
    /// 仅首次建立的固定市场身份与资产绑定，窗口复用原市场类型。
    pub market_bindings: Vec<PublicSourceProgressMarketBindingV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PublicSourceProgressMarketBindingV1 {
    pub market: MarketIdentityV2,
    pub assets: Vec<AssetIdentityV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PublicSourceProgressAppliedV1 {
    pub applied_prefix: AppliedIngressPrefixV1,
    pub closed_groups: Vec<PublicSourceProgressGroupV1>,
}
struct CoreApplicationV1<'a> {
    applied: AppliedIngressV1,
    pm_reductions: Vec<(PublicFactPositionV1, PublicPmReductionV1<'a>)>,
    reductions: Vec<PublicReductionBatchV1>,
    compact_groups: Option<Vec<PublicSourceProgressGroupV1>>,
    fact_groups: Option<Vec<PublicFactGroupV1>>,
    changed_scopes: BTreeSet<SourceStreamScopeV1>,
    closed_fact_count: u32,
}

/// 原公共组来源可见时间；接收/采集准入与事实依赖取最大值。
/// 此纯原语不选择消费宿主执行时间，也不维护全局单调时钟。
pub fn public_group_visible_at_utc_ms(
    ingress: &DptIngressV1,
    records: &[PublicBusinessFactV1],
) -> i64 {
    let input_available = match ingress {
        DptIngressV1::Message {
            source_evidence, ..
        } => source_evidence
            .collector_admission
            .as_ref()
            .map_or(source_evidence.received_at_utc_ms, |c| {
                c.admitted_at_utc_ms.max(source_evidence.received_at_utc_ms)
            }),
        _ => i64::MIN,
    };
    records
        .iter()
        .map(|fact| fact.dependency_time.received_at_utc_ms)
        .chain(std::iter::once(input_available))
        .max()
        .unwrap()
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DptCoreErrorV1 {
    Contract(ContractError),
    MissingIdentitySnapshot,
    MetadataBeforeIdentitySnapshot,
    DuplicateWsTerminalClaim {
        epoch: String,
        collector_admission_sequence: u64,
    },
    PendingWsTerminalLimit {
        limit: usize,
    },
    UnpairedWsTerminalMetadata {
        count: usize,
    },
    Aborted {
        reason: String,
    },
}
impl From<ContractError> for DptCoreErrorV1 {
    fn from(value: ContractError) -> Self {
        Self::Contract(value)
    }
}
impl std::fmt::Display for DptCoreErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DptCoreErrorV1 {}

#[derive(Default)]
struct AssetProjection {
    tick: ObservedFactV1<ExactDecimal>,
    bba: ObservedFactV1<CanonicalBbaFactV1>,
    valuation_bba: ObservedFactV1<Option<ExactDecimal>>,
    valuation_bba_version: Option<crate::pm_reduction::PmL2SelectionVersionV1>,
    trade: ObservedFactV1<PublicTradeV2>,
    book: ObservedFactV1<()>,
}
struct MarketState {
    metadata: PolymarketMetadataReducer<E>,
    terminal: PolymarketTerminalMerger<E>,
    pm: PmOwnedStateV1<E>,
    assets: BTreeMap<String, AssetProjection>,
    level_anchors: BTreeMap<LevelKey, ObservedFactV1<()>>,
    seen_epochs: BTreeSet<String>,
    seen_messages: BTreeSet<u64>,
    last_message: Option<(u64, Sha256Hex, E)>,
    lifecycle: ObservedFactV1<MappedMarketLifecycleStateV1>,
    active: ObservedFactV1<bool>,
    closed: ObservedFactV1<bool>,
    accepting_orders: ObservedFactV1<bool>,
    resolution: ObservedFactV1<MarketResolutionTerminalV3>,
}
#[derive(Default)]
pub struct PublicMarketReferenceStateV1 {
    pub official: ObservedFactV1<MarketPriceAnchorV1>,
    pub rule: ObservedFactV1<BtcSettlementReferenceV1>,
    pub temporary: Option<TemporaryTwapReferenceV1>,
    pub(crate) changed_rule_record: Option<CanonicalInputRecordRefV1>,
}
#[derive(Default)]
struct GlobalReferences {
    spot: ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
    thirty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    sixty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    depth: ObservedFactV1<()>,
    trade: ObservedFactV1<BinanceAggregateTradeReferenceV1>,
}
struct PendingMetadata {
    input: PolymarketMetadataInputV1<E>,
    outcomes: Vec<MetadataOutcomeEvidence>,
    position: u64,
}
struct PendingWs {
    input: PolymarketOrderbookInputV1<E>,
    group_id: Sha256Hex,
    position: u64,
}
struct PendingTerminal {
    first_position: u64,
    metadata: Option<PendingMetadata>,
    ws: Option<PendingWs>,
}

/// 只读有限验收口径：条目数不是分配容量、编码字节或 RSS。
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PublicCoreObjectCountsV1 {
    pub markets: usize,
    pub assets: usize,
    pub pm_quantity_keys: usize,
    pub pm_level_evidence_keys: usize,
    pub pm_absence_keys: usize,
    pub pm_selection_keys: usize,
    pub public_level_anchor_keys: usize,
    pub seen_epoch_entries: usize,
    pub seen_message_entries: usize,
    pub pending_terminal_keys: usize,
    pub pending_metadata_sides: usize,
    pub pending_ws_sides: usize,
    pub reference_market_states: usize,
    pub fixed_temporary_references: usize,
    pub original_retained_state_entries: usize,
    pub binance_depth_retained_entries: usize,
}

pub struct PublicObservationCoreV1 {
    header: PublicInputStreamHeaderV1,
    next_input_position: u64,
    next_group_position: u64,
    next_canonical_position: u64,
    markets: BTreeMap<String, MarketState>,
    references: BTreeMap<String, PublicMarketReferenceStateV1>,
    global: GlobalReferences,
    binance_depth: BinanceDepthReducer<E>,
    // 只读派生的当前值；唯一数量归约器及事实仍是权威。旧组自行持有旧 Arc。
    current_depth_projection: Option<Arc<ObservedFactV1<BinanceDepthStateReferenceV1>>>,
    binance_trade: BinanceAggTradeReducer<E>,
    pending: BTreeMap<(String, u64), PendingTerminal>,
    source_progress_enabled: bool,
    source_progress: BTreeMap<SourceStreamScopeV1, SourceStreamProgressV1>,
}

fn observed<T>(
    value: T,
    fact: &PublicBusinessFactV1,
    group_id: &Sha256Hex,
    field: &str,
) -> ObservedFactV1<T> {
    ObservedFactV1 {
        value: matches!(
            fact.quality.availability,
            MarketDataQuality::Fresh | MarketDataQuality::Stale
        )
        .then_some(value),
        quality: fact.quality.availability,
        direct_anchors: fact
            .direct_sources
            .iter()
            .map(|source| DirectObservationAnchorV1 {
                field_path: field.to_owned(),
                position: fact.position.clone(),
                group_id: group_id.clone(),
                source_message_id: source.source_message_id.clone(),
                source_event_at_utc_ms: source.source_event_at_utc_ms,
                received_at_utc_ms: source.received_at_utc_ms,
            })
            .collect(),
        dependency_frontier: Some(AppliedIngressPrefixV1 {
            input_stream_id: fact.position.input_stream_id.clone(),
            next_input_position: fact.position.closed_at_input_position + 1,
        }),
    }
}
fn component<T>(
    value: Option<T>,
    fact: &PublicBusinessFactV1,
    group: &Sha256Hex,
    field: &str,
) -> ObservedFactV1<T> {
    value.map_or_else(ObservedFactV1::default, |v| observed(v, fact, group, field))
}

impl PublicObservationCoreV1 {
    pub fn new(header: PublicInputStreamHeaderV1) -> Result<Self, DptCoreErrorV1> {
        Ok(Self {
            binance_depth: BinanceDepthReducer::new(&header.canonical_rule_version)?,
            current_depth_projection: None,
            binance_trade: BinanceAggTradeReducer::new(&header.canonical_rule_version)?,
            header,
            next_input_position: 0,
            next_group_position: 0,
            next_canonical_position: 0,
            markets: BTreeMap::new(),
            references: BTreeMap::new(),
            global: GlobalReferences::default(),
            pending: BTreeMap::new(),
            source_progress_enabled: false,
            source_progress: BTreeMap::new(),
        })
    }
    /// 在消费开始前显式选用；原关闭组与旧消费者默认行为保持。
    pub fn enable_source_progress(&mut self) {
        self.source_progress_enabled = true;
    }
    /// 已应用输入后的准确流当前进展；零市场也可读取，不是历史组快照。
    pub fn current_source_progress(
        &self,
        scope: &SourceStreamScopeV1,
    ) -> Option<&SourceStreamProgressV1> {
        self.source_progress.get(scope)
    }
    pub fn header(&self) -> &PublicInputStreamHeaderV1 {
        &self.header
    }
    /// 已应用输入后的当前完整现价；不是任意历史组的快照。
    pub fn current_chainlink_spot(&self) -> &ObservedFactV1<ChainlinkBtcUsdReferenceV1> {
        &self.global.spot
    }
    pub fn applied_prefix(&self) -> AppliedIngressPrefixV1 {
        AppliedIngressPrefixV1 {
            input_stream_id: self.header.input_stream_id.clone(),
            next_input_position: self.next_input_position,
        }
    }
    pub fn object_counts(&self) -> PublicCoreObjectCountsV1 {
        PublicCoreObjectCountsV1 {
            markets: self.markets.len(),
            assets: self.markets.values().map(|m| m.pm.assets.len()).sum(),
            pm_quantity_keys: self
                .markets
                .values()
                .map(|m| m.pm.quantities.as_ref().map_or(0, BTreeMap::len))
                .sum(),
            pm_level_evidence_keys: self.markets.values().map(|m| m.pm.levels.len()).sum(),
            pm_absence_keys: self.markets.values().map(|m| m.pm.absences.len()).sum(),
            pm_selection_keys: self.markets.values().map(|m| m.pm.selected.len()).sum(),
            public_level_anchor_keys: self.markets.values().map(|m| m.level_anchors.len()).sum(),
            seen_epoch_entries: self.markets.values().map(|m| m.seen_epochs.len()).sum(),
            seen_message_entries: self.markets.values().map(|m| m.seen_messages.len()).sum(),
            pending_terminal_keys: self.pending.len(),
            pending_metadata_sides: self
                .pending
                .values()
                .filter(|p| p.metadata.is_some())
                .count(),
            pending_ws_sides: self.pending.values().filter(|p| p.ws.is_some()).count(),
            reference_market_states: self.references.len(),
            fixed_temporary_references: self
                .references
                .values()
                .filter(|r| r.temporary.is_some())
                .count(),
            original_retained_state_entries: self.retained_state_entries(),
            binance_depth_retained_entries: self.binance_depth.retained_state_entries(),
        }
    }
    pub fn pending_ws_terminal_count(&self) -> usize {
        self.pending.len()
    }
    pub fn reference_state(&self, condition: &str) -> Option<&PublicMarketReferenceStateV1> {
        self.references.get(condition)
    }
    /// 仅投影已知固定身份；来源准备可借此构造规范消息。
    pub fn fixed_markets(&self) -> impl Iterator<Item = MarketIdentityV2> + '_ {
        self.markets.values().map(|m| m.pm.context.market.clone())
    }
    /// 原规范事实位置；供同次材料消费者按组内事实原序绑定。
    pub fn next_canonical_position(&self) -> u64 {
        self.next_canonical_position
    }
    /// 来源准备的同一证据材料，只投影固定上下文，不复制数量状态。
    pub fn prepared_pm_context(
        &self,
        condition: &str,
    ) -> Option<PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>> {
        self.markets.get(condition).map(|m| m.pm.context.clone())
    }
    pub fn pm_context(&self, condition: &str) -> Option<PmMarketContextV1> {
        self.markets.get(condition).map(|m| PmMarketContextV1 {
            market: m.pm.context.market.clone(),
            assets: m.pm.context.assets.clone(),
            identity_source_evidence: m
                .pm
                .context
                .identity_source_evidence
                .iter()
                .map(|e| e.source().clone())
                .collect(),
            canonical_rule_version: self.header.canonical_rule_version.clone(),
        })
    }
    pub fn retained_state_entries(&self) -> usize {
        self.markets.values().fold(
            self.pending
                .len()
                .saturating_add(self.binance_depth.retained_state_entries()),
            |total, m| {
                total
                    .saturating_add(m.seen_epochs.len())
                    .saturating_add(m.seen_messages.len())
                    .saturating_add(m.pm.levels.len())
                    .saturating_add(m.pm.absences.len())
                    .saturating_add(
                        m.pm.assets
                            .values()
                            .map(|a| {
                                usize::from(a.absence_baseline.is_some())
                                    + usize::from(a.bba_reanchor_frontier.is_some())
                            })
                            .sum::<usize>(),
                    )
            },
        )
    }
    fn context(
        &self,
        input: &PreparedIngressV1<'_>,
        fact_ordinal: u32,
        report_ordinal: u32,
    ) -> PublicReductionContextV1 {
        let sources = match &input.recorded.ingress {
            DptIngressV1::Message {
                source_evidence, ..
            } => vec![source_evidence.clone()],
            _ => vec![],
        };
        let mut refs = vec![CanonicalInputRecordRefV1 {
            input_stream_id: self.header.input_stream_id.clone(),
            input_position: self.next_input_position,
        }];
        refs.extend(input.recorded.dependency_record_refs.clone());
        PublicReductionContextV1 {
            input_stream_id: self.header.input_stream_id.clone(),
            closed_at_input_position: self.next_input_position,
            first_fact_ordinal: fact_ordinal,
            first_report_ordinal: report_ordinal,
            group_id: input
                .prepared_group_id
                .clone()
                .expect("业务输入已准备组身份"),
            source_refs: refs,
            direct_sources: sources,
        }
    }
    /// 输入所属方串行调用；返回的 PM 材料只借用本次输入，不借用核心状态。
    pub fn push<'a>(
        &mut self,
        input: &'a PreparedIngressV1<'_>,
    ) -> Result<PublicAppliedIngressV1<'a>, DptCoreErrorV1> {
        let result = self.push_application(input, PublicOutputModeV1::Observations)?;
        Ok(PublicAppliedIngressV1 {
            applied: result.applied,
            pm_reductions: result.pm_reductions,
            reductions: result.reductions,
        })
    }
    /// 新材料调用完整公共归约的事实输出；不调用研究型来源进展入口。
    pub fn push_facts<'a>(
        &mut self,
        input: &'a PreparedIngressV1<'_>,
    ) -> Result<PublicFactAppliedIngressV1<'a>, DptCoreErrorV1> {
        self.enable_source_progress();
        let result = self.push_application(input, PublicOutputModeV1::Facts)?;
        Ok(PublicFactAppliedIngressV1 {
            applied_prefix: result.applied.applied_prefix,
            groups: result.fact_groups.unwrap(),
            pm_reductions: result.pm_reductions,
            reductions: result.reductions,
        })
    }
    /// 显式研究消费：沿同一归约封组，省去全市场观察及全来源集合复制。
    /// 此入口自动选用来源进展；返回类型不能交给完整录制或SIM。
    pub fn push_source_progress(
        &mut self,
        input: &PreparedIngressV1<'_>,
    ) -> Result<PublicSourceProgressAppliedV1, DptCoreErrorV1> {
        self.enable_source_progress();
        let result = self.push_application(input, PublicOutputModeV1::SourceProgress)?;
        Ok(PublicSourceProgressAppliedV1 {
            applied_prefix: result.applied.applied_prefix,
            closed_groups: result.compact_groups.unwrap(),
        })
    }
    fn push_application<'a>(
        &mut self,
        input: &'a PreparedIngressV1<'_>,
        mode: PublicOutputModeV1,
    ) -> Result<CoreApplicationV1<'a>, DptCoreErrorV1> {
        if input.recorded.input_stream_id != self.header.input_stream_id
            || input.recorded.input_position != self.next_input_position
        {
            return Err(ContractError::InvalidField {
                field: "recorded_ingress".into(),
                reason: "输入必须属于同一流并从零连续递增".into(),
            }
            .into());
        }
        let mut result = CoreApplicationV1 {
            applied: AppliedIngressV1 {
                applied_prefix: self.applied_prefix(),
                closed_groups: vec![],
            },
            pm_reductions: vec![],
            reductions: vec![],
            compact_groups: matches!(mode, PublicOutputModeV1::SourceProgress).then(Vec::new),
            fact_groups: matches!(mode, PublicOutputModeV1::Facts).then(Vec::new),
            changed_scopes: BTreeSet::new(),
            closed_fact_count: 0,
        };
        let context = match input.current {
            PreparedCanonicalInputV1::EndOfInput | PreparedCanonicalInputV1::Aborted { .. } => None,
            _ => input
                .prepared_group_id
                .as_ref()
                .map(|_| self.context(input, 0, 0)),
        };
        match &input.current {
            PreparedCanonicalInputV1::EndOfInput => {
                let missing_sources = self
                    .pending
                    .values()
                    .filter(|pair| pair.metadata.is_some())
                    .count();
                if missing_sources != 0 {
                    return Err(DptCoreErrorV1::UnpairedWsTerminalMetadata {
                        count: missing_sources,
                    });
                }
            }
            PreparedCanonicalInputV1::Aborted { reason } => {
                return Err(DptCoreErrorV1::Aborted {
                    reason: (*reason).to_owned(),
                });
            }
            PreparedCanonicalInputV1::BinanceDepthSnapshot { row, source } => {
                // compute 可改状态后返回无事件或错误，不能仅按 Produced 失效。
                self.current_depth_projection = None;
                let batch = bind_reference_reduction_v1(
                    self.binance_depth.compute_snapshot(row, source.clone())?,
                    context.unwrap(),
                );
                self.close_batch(batch, vec![self.next_input_position], input, &mut result);
            }
            PreparedCanonicalInputV1::BinanceDepthUpdate { row, source } => {
                // compute 可改状态后返回无事件或错误，不能仅按 Produced 失效。
                self.current_depth_projection = None;
                let batch = bind_reference_reduction_v1(
                    self.binance_depth.compute_update(row, source.clone())?,
                    context.unwrap(),
                );
                self.close_batch(batch, vec![self.next_input_position], input, &mut result);
            }
            PreparedCanonicalInputV1::BinanceAggregateTrade {
                row,
                source,
                payload,
            } => {
                let batch = bind_reference_reduction_v1(
                    self.binance_trade
                        .compute(row, source.clone(), payload.as_ref())?,
                    context.unwrap(),
                );
                self.close_batch(batch, vec![self.next_input_position], input, &mut result);
            }
            PreparedCanonicalInputV1::ChainlinkSpot(event)
            | PreparedCanonicalInputV1::ChainlinkTwap(event) => {
                let batch = bind_prepared_reference_facts_v1(vec![event.clone()], context.unwrap());
                self.close_batch(batch, vec![self.next_input_position], input, &mut result);
            }
            PreparedCanonicalInputV1::AnchorRule { facts, .. }
            | PreparedCanonicalInputV1::HistoricalAnchorRepair { facts, .. } => {
                let batch = bind_prepared_reference_facts_v1(facts.clone(), context.unwrap());
                self.close_batch(batch, vec![self.next_input_position], input, &mut result);
            }
            PreparedCanonicalInputV1::Metadata(prepared) => {
                let row = prepared.row;
                let metadata = PolymarketMetadataInputV1 {
                    row: row.clone(),
                    evidence: prepared.source.clone(),
                };
                if row.metadata_row_kind == PolymarketMetadataRowKind::ResolutionTerminalWs {
                    let key = (
                        row.ws_claimant_source_sequence_epoch_id
                            .clone()
                            .expect("已验证声明代际"),
                        row.ws_claimant_collector_admission_sequence
                            .expect("已验证声明序号"),
                    );
                    self.insert_pending(&key, true)?;
                    self.pending.get_mut(&key).unwrap().metadata = Some(PendingMetadata {
                        input: metadata,
                        outcomes: prepared.outcomes.clone(),
                        position: self.next_input_position,
                    });
                } else if !self.markets.contains_key(&row.condition_id) {
                    if row.metadata_row_kind != PolymarketMetadataRowKind::MetadataSnapshot {
                        return Err(DptCoreErrorV1::MetadataBeforeIdentitySnapshot);
                    }
                    let (metadata_reducer, market_context, initial) =
                        PolymarketMetadataReducer::compute_identity_snapshot(
                            metadata,
                            prepared.outcomes.clone(),
                            prepared.initial_tick.clone(),
                            &self.header.canonical_rule_version,
                        )?;
                    let terminal = PolymarketTerminalMerger::new(
                        market_context.clone(),
                        &self.header.canonical_rule_version,
                    )?;
                    let pm = PmOwnedStateV1::new(PmMarketContextV1 {
                        market: market_context.market.clone(),
                        assets: market_context.assets.clone(),
                        identity_source_evidence: market_context.identity_source_evidence.clone(),
                        canonical_rule_version: self.header.canonical_rule_version.clone(),
                    });
                    let assets = market_context
                        .assets
                        .iter()
                        .map(|a| (a.asset.token_id.0.clone(), AssetProjection::default()))
                        .collect();
                    let batch = bind_initial_market_reduction_v1(
                        &market_context,
                        initial,
                        context.unwrap(),
                    );
                    self.markets.insert(
                        row.condition_id.clone(),
                        MarketState {
                            metadata: metadata_reducer,
                            terminal,
                            pm,
                            assets,
                            level_anchors: BTreeMap::new(),
                            seen_epochs: BTreeSet::new(),
                            seen_messages: BTreeSet::new(),
                            last_message: None,
                            lifecycle: ObservedFactV1::default(),
                            active: ObservedFactV1::default(),
                            closed: ObservedFactV1::default(),
                            accepting_orders: ObservedFactV1::default(),
                            resolution: ObservedFactV1::default(),
                        },
                    );
                    self.close_batch(batch, vec![self.next_input_position], input, &mut result);
                } else {
                    let market = self.markets.get_mut(&row.condition_id).unwrap();
                    let gamma = row.metadata_row_kind
                        == PolymarketMetadataRowKind::ResolutionPollAttempt
                        && row.resolution_poll_result
                            == Some(MetadataResolutionPollResult::Terminal)
                        && row.resolution_evidence_source
                            == Some(MetadataResolutionEvidenceSource::GammaBySlug);
                    let batch = if gamma {
                        market
                            .terminal
                            .compute_gamma_terminal(metadata, &prepared.outcomes)?
                    } else {
                        market
                            .metadata
                            .compute_metadata(metadata, &prepared.outcomes, true)?
                    };
                    let batch = bind_market_reduction_v1(batch, context.unwrap());
                    self.close_batch(batch, vec![self.next_input_position], input, &mut result);
                }
                self.drain_terminals(input, &mut result)?;
            }
            PreparedCanonicalInputV1::PolymarketWsTerminal { row, source } => {
                let key = (
                    row.source_sequence_epoch_id.clone(),
                    row.collector_admission_sequence,
                );
                self.insert_pending(&key, false)?;
                self.pending.get_mut(&key).unwrap().ws = Some(PendingWs {
                    input: PolymarketOrderbookInputV1 {
                        row: (*row).clone(),
                        evidence: source.clone(),
                    },
                    group_id: input.prepared_group_id.clone().unwrap(),
                    position: self.next_input_position,
                });
                self.drain_terminals(input, &mut result)?;
            }
            PreparedCanonicalInputV1::PolymarketMessage(message) => {
                let msg = message.message();
                let market = self
                    .markets
                    .get_mut(&msg.market.condition_id.0)
                    .ok_or(DptCoreErrorV1::MissingIdentitySnapshot)?;
                let admission = message
                    .evidence()
                    .source()
                    .collector_admission
                    .as_ref()
                    .expect("已准备准入坐标");
                if market.pm.epoch.as_deref() != Some(&admission.source_sequence_epoch_id) {
                    if market
                        .seen_epochs
                        .contains(&admission.source_sequence_epoch_id)
                    {
                        return Err(invalid(
                            "source_sequence_epoch_id",
                            "a completed epoch cannot be resumed after another epoch began",
                        )
                        .into());
                    }
                    market.seen_messages.clear();
                    market.last_message = None;
                }
                if let Some((_, identity, previous)) = market
                    .last_message
                    .as_ref()
                    .filter(|(sequence, ..)| *sequence == admission.collector_admission_sequence)
                {
                    if identity != &message.evidence().source().source_message_id {
                        return Err(invalid("collector_admission_sequence", "one admission sequence cannot identify different messages within an epoch").into());
                    }
                    let report = duplicate_suppressed_report::<E>(
                        &self.header.canonical_rule_version,
                        format!(
                            "polymarket_message:{}:{}:{}",
                            msg.market.condition_id.0,
                            admission.source_sequence_epoch_id,
                            admission.collector_admission_sequence
                        ),
                        "POLYMARKET_IDENTICAL_MESSAGE_RETRY",
                        vec![],
                        vec![previous.clone()],
                        vec![message.evidence().clone()],
                    )?;
                    result.reductions.push(bind_market_reduction_v1(
                        PolymarketReductionBatch {
                            events: vec![],
                            reports: vec![report],
                        },
                        context.unwrap(),
                    ));
                } else {
                    if market
                        .seen_messages
                        .contains(&admission.collector_admission_sequence)
                    {
                        return Err(invalid(
                            "collector_admission_sequence",
                            "must identify only one Polymarket message within one source epoch",
                        )
                        .into());
                    }
                    let reduction = compute_pm_message_steps_v1(
                        &market.pm.context,
                        &market.pm,
                        message,
                        &self.header.input_stream_id,
                        self.next_canonical_position,
                        true,
                    )?;
                    let fact = PublicBusinessFactV1 {
                        position: PublicFactPositionV1 {
                            input_stream_id: self.header.input_stream_id.clone(),
                            closed_at_input_position: self.next_input_position,
                            fact_ordinal: 0,
                        },
                        direct_sources: vec![message.evidence().source().clone()],
                        payload: PublicBusinessPayloadV1::PolymarketMessage(msg.clone()),
                        source_refs: std::iter::once(msg.input_record.clone())
                            .chain(input.recorded.dependency_record_refs.iter().cloned())
                            .collect(),
                        lineage: msg.lineage.clone(),
                        direct_time: msg.time.clone(),
                        dependency_time: msg.time.clone(),
                        quality: EventQualityV2 {
                            availability: MarketDataQuality::Fresh,
                            resync: ResyncQuality::NotRequired,
                        },
                    };
                    let source_epoch_changed =
                        market.pm.epoch.as_deref() != Some(&admission.source_sequence_epoch_id);
                    market.pm.apply(&reduction);
                    market.apply_pm_projection(&reduction, &fact, &msg.group_id, &msg.payload);
                    market
                        .seen_epochs
                        .insert(admission.source_sequence_epoch_id.clone());
                    market
                        .seen_messages
                        .insert(admission.collector_admission_sequence);
                    market.last_message = Some((
                        admission.collector_admission_sequence,
                        message.evidence().source().source_message_id.clone(),
                        message.evidence().clone(),
                    ));
                    result
                        .pm_reductions
                        .push((fact.position.clone(), reduction));
                    self.commit_selection(input);
                    self.update_pm_source_progress(
                        &fact,
                        source_epoch_changed,
                        &mut result.changed_scopes,
                    );
                    self.close_group(
                        msg.group_id.clone(),
                        vec![self.next_input_position],
                        vec![fact].into(),
                        input.recorded.ingress,
                        &mut result,
                    );
                }
            }
        }
        self.next_input_position += 1;
        result.applied.applied_prefix = self.applied_prefix();
        Ok(result)
    }
    fn insert_pending(
        &mut self,
        key: &(String, u64),
        metadata_side: bool,
    ) -> Result<(), DptCoreErrorV1> {
        if self.pending.get(key).is_some_and(|p| {
            if metadata_side {
                p.metadata.is_some()
            } else {
                p.ws.is_some()
            }
        }) {
            return Err(DptCoreErrorV1::DuplicateWsTerminalClaim {
                epoch: key.0.clone(),
                collector_admission_sequence: key.1,
            });
        }
        if !self.pending.contains_key(key)
            && self.pending.len() >= self.header.max_pending_ws_terminals
        {
            return Err(DptCoreErrorV1::PendingWsTerminalLimit {
                limit: self.header.max_pending_ws_terminals,
            });
        }
        self.pending.entry(key.clone()).or_insert(PendingTerminal {
            first_position: self.next_input_position,
            metadata: None,
            ws: None,
        });
        Ok(())
    }
    fn drain_terminals(
        &mut self,
        input: &PreparedIngressV1<'_>,
        output: &mut CoreApplicationV1<'_>,
    ) -> Result<(), DptCoreErrorV1> {
        let mut ready: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(key, pending)| {
                let metadata = pending.metadata.as_ref()?;
                pending.ws.as_ref()?;
                self.markets
                    .contains_key(&metadata.input.row.condition_id)
                    .then_some((pending.first_position, key.clone()))
            })
            .collect();
        ready.sort_by_key(|(position, _)| *position);
        for (_, key) in ready {
            let pending = self.pending.remove(&key).unwrap();
            let metadata = pending.metadata.unwrap();
            let ws = pending.ws.unwrap();
            let mut members = vec![metadata.position, ws.position];
            members.sort_unstable();
            if !members.contains(&self.next_input_position) {
                members.push(self.next_input_position);
            }
            let context = PublicReductionContextV1 {
                input_stream_id: self.header.input_stream_id.clone(),
                closed_at_input_position: self.next_input_position,
                first_fact_ordinal: output.closed_fact_count,
                first_report_ordinal: output
                    .reductions
                    .iter()
                    .map(|b| b.reports.len() as u32)
                    .sum(),
                group_id: ws.group_id.clone(),
                source_refs: members
                    .iter()
                    .map(|position| CanonicalInputRecordRefV1 {
                        input_stream_id: self.header.input_stream_id.clone(),
                        input_position: *position,
                    })
                    .collect(),
                direct_sources: vec![
                    metadata.input.evidence.source().clone(),
                    ws.input.evidence.source().clone(),
                ],
            };
            let market = self
                .markets
                .get_mut(&metadata.input.row.condition_id)
                .unwrap();
            let batch = market.terminal.compute_ws_terminal(
                metadata.input,
                &metadata.outcomes,
                ws.input,
            )?;
            let batch = bind_market_reduction_v1(batch, context);
            self.close_batch_with_group(batch, ws.group_id, members, input, output);
        }
        Ok(())
    }
    fn close_batch(
        &mut self,
        batch: PublicReductionBatchV1,
        members: Vec<u64>,
        input: &PreparedIngressV1<'_>,
        output: &mut CoreApplicationV1<'_>,
    ) {
        self.close_batch_with_group(
            batch,
            input.prepared_group_id.clone().unwrap(),
            members,
            input,
            output,
        );
    }
    // 原进展事实参数加本次增量出口，不为参数个数另建第二状态对象。
    #[allow(clippy::too_many_arguments)]
    fn put_source_progress(
        &mut self,
        scope: SourceStreamScopeV1,
        evidence: &SourceEvidenceRefV3,
        generation: Option<u64>,
        valid: bool,
        snapshot: bool,
        quality: EventQualityV2,
        changed: &mut BTreeSet<SourceStreamScopeV1>,
    ) -> bool {
        if !self.source_progress_enabled {
            return false;
        }
        let prior = self.source_progress.get(&scope);
        if prior.is_some_and(|old| {
            old.last_observed_message.source_message_id == evidence.source_message_id
        }) {
            return false;
        }
        let generation_changed = prior.is_some_and(|old| old.connection_generation != generation);
        let mut synchronization = prior.map_or(SourceSynchronizationV1::Unknown, |old| {
            old.synchronization.clone()
        });
        if scope.source == SourceStreamKindV1::BinanceDepth {
            if generation_changed || quality.resync == ResyncQuality::Required {
                synchronization = SourceSynchronizationV1::SnapshotRequired;
            }
            if snapshot {
                synchronization = SourceSynchronizationV1::SnapshotObserved {
                    source_message_id: evidence.source_message_id.clone(),
                };
            }
        }
        // 被抑制的重放不刷新有效业务时间，但真实代次变化仍是可见事实。
        if !valid && !generation_changed && prior.is_some_and(|old| old.quality == quality) {
            return false;
        }
        let last_valid_business_message = if valid {
            Some(evidence.clone())
        } else {
            prior.and_then(|old| old.last_valid_business_message.clone())
        };
        changed.insert(scope.clone());
        self.source_progress.insert(
            scope.clone(),
            SourceStreamProgressV1 {
                scope,
                last_valid_business_message,
                last_observed_message: evidence.clone(),
                connection_generation: generation,
                synchronization,
                quality,
            },
        );
        true
    }
    fn update_source_progress(
        &mut self,
        input: &PreparedIngressV1<'_>,
        batch: &PublicReductionBatchV1,
        changed: &mut BTreeSet<SourceStreamScopeV1>,
    ) -> bool {
        if !self.source_progress_enabled {
            return false;
        }
        let produced_or_confirmed = !batch.facts.is_empty()
            || batch
                .reports
                .iter()
                .any(|r| r.report.operation == ReductionOperationKindV1::Confirm);
        let fresh = EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        };
        let quality = batch.facts.last().map_or(fresh, |f| f.quality.clone());
        let (kind, symbol, evidence, generation, valid, snapshot) = match &input.current {
            PreparedCanonicalInputV1::BinanceDepthSnapshot { row, source } => (
                SourceStreamKindV1::BinanceDepth,
                row.symbol.clone(),
                source.source(),
                Some(row.connection_generation),
                produced_or_confirmed,
                true,
            ),
            PreparedCanonicalInputV1::BinanceDepthUpdate { row, source } => {
                let quality = if row.stitch_verdict
                    == BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable
                {
                    EventQualityV2 {
                        availability: MarketDataQuality::Missing,
                        resync: ResyncQuality::Required,
                    }
                } else {
                    quality
                };
                return self.put_source_progress(
                    SourceStreamScopeV1 {
                        source: SourceStreamKindV1::BinanceDepth,
                        condition_id: None,
                        asset_or_symbol: row.symbol.clone(),
                    },
                    source.source(),
                    Some(row.connection_generation),
                    row.stitch_verdict == BinanceDepthStitchVerdict::Applied,
                    false,
                    quality,
                    changed,
                );
            }
            PreparedCanonicalInputV1::BinanceAggregateTrade {
                row,
                source,
                payload,
            } => {
                let Some(payload) = payload else {
                    return false;
                };
                (
                    SourceStreamKindV1::BinanceAggregateTrade,
                    payload.symbol.clone(),
                    source.source(),
                    Some(row.connection_generation),
                    produced_or_confirmed,
                    false,
                )
            }
            PreparedCanonicalInputV1::ChainlinkSpot(event) => {
                let CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(value) = &event.payload
                else {
                    return false;
                };
                (
                    SourceStreamKindV1::ChainlinkSpot,
                    value.symbol.clone(),
                    &event.lineage.source_evidence[0],
                    None,
                    true,
                    false,
                )
            }
            PreparedCanonicalInputV1::ChainlinkTwap(event) => {
                let CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(value) = &event.payload
                else {
                    return false;
                };
                let kind = if value.window_s == 30 {
                    SourceStreamKindV1::ChainlinkTwapThirty
                } else {
                    SourceStreamKindV1::ChainlinkTwapSixty
                };
                (
                    kind,
                    value.symbol.clone(),
                    &event.lineage.source_evidence[0],
                    None,
                    true,
                    false,
                )
            }
            _ => return false,
        };
        self.put_source_progress(
            SourceStreamScopeV1 {
                source: kind,
                condition_id: None,
                asset_or_symbol: symbol,
            },
            evidence,
            generation,
            valid,
            snapshot,
            quality,
            changed,
        )
    }
    fn update_pm_source_progress(
        &mut self,
        fact: &PublicBusinessFactV1,
        epoch_changed: bool,
        changed: &mut BTreeSet<SourceStreamScopeV1>,
    ) {
        if !self.source_progress_enabled {
            return;
        }
        let PublicBusinessPayloadV1::PolymarketMessage(message) = &fact.payload else {
            return;
        };
        let (assets, snapshot): (Vec<_>, bool) = match &message.payload {
            CanonicalPmPayloadV1::Book {
                fact: CanonicalBookFactV1::FullBook { asset, .. },
            } => (vec![asset], true),
            CanonicalPmPayloadV1::Book {
                fact: CanonicalBookFactV1::AbsoluteLevelChanges { changes },
            } => (changes.iter().map(|c| &c.asset).collect(), false),
            CanonicalPmPayloadV1::BestBidAsk { asset, .. }
            | CanonicalPmPayloadV1::TickSizeChange { asset, .. }
            | CanonicalPmPayloadV1::PublicTrade { asset, .. } => (vec![asset], false),
            _ => (vec![], false),
        };
        if epoch_changed {
            // 与原归约器的本市场代际变化一致；每次代际只重置一次。
            for progress in self.source_progress.values_mut().filter(|p| {
                p.scope.source == SourceStreamKindV1::PolymarketBook
                    && p.scope.condition_id.as_ref() == Some(&message.market.condition_id)
            }) {
                changed.insert(progress.scope.clone());
                progress.synchronization = SourceSynchronizationV1::SnapshotRequired;
                // 当前消息涉及的资产随后更新有效进展，不能提前触发重复抑制。
                if !assets
                    .iter()
                    .any(|asset| asset.token_id.0 == progress.scope.asset_or_symbol)
                {
                    progress.last_observed_message = fact.direct_sources[0].clone();
                }
            }
        }
        for asset in assets {
            let scope = SourceStreamScopeV1 {
                source: SourceStreamKindV1::PolymarketBook,
                condition_id: Some(message.market.condition_id.clone()),
                asset_or_symbol: asset.token_id.0.clone(),
            };
            self.put_source_progress(
                scope.clone(),
                &fact.direct_sources[0],
                None,
                true,
                false,
                fact.quality.clone(),
                changed,
            );
            if snapshot {
                self.source_progress
                    .get_mut(&scope)
                    .unwrap()
                    .synchronization = SourceSynchronizationV1::SnapshotObserved {
                    source_message_id: fact.direct_sources[0].source_message_id.clone(),
                };
            }
        }
    }
    fn close_batch_with_group(
        &mut self,
        batch: PublicReductionBatchV1,
        group: Sha256Hex,
        members: Vec<u64>,
        input: &PreparedIngressV1<'_>,
        output: &mut CoreApplicationV1<'_>,
    ) {
        let progress_changed =
            self.update_source_progress(input, &batch, &mut output.changed_scopes);
        for fact in batch.facts.iter() {
            self.apply_fact(fact, &group);
        }
        self.commit_selection(input);
        if !batch.facts.is_empty() || progress_changed {
            self.close_group(
                group,
                members,
                batch.facts.clone(),
                input.recorded.ingress,
                output,
            );
        }
        output.reductions.push(batch);
    }
    fn commit_selection(&mut self, input: &PreparedIngressV1<'_>) {
        if let (Some(market), Some(selection)) = (
            &input.reference_selection_market,
            &input.reference_selection,
        ) {
            if let (Some(value), Some(anchor)) =
                (&selection.selected_value, &selection.direct_anchor)
            {
                self.references
                    .entry(market.condition_id.0.clone())
                    .or_default()
                    .temporary = Some(TemporaryTwapReferenceV1 {
                    value: value.clone(),
                    selected_position: anchor.position.clone(),
                    direct_anchor: anchor.clone(),
                    dependency_frontier: AppliedIngressPrefixV1 {
                        input_stream_id: self.header.input_stream_id.clone(),
                        next_input_position: selection.dependency_input_position + 1,
                    },
                });
            }
        }
    }
    fn close_group(
        &mut self,
        group_id: Sha256Hex,
        members: Vec<u64>,
        records: Arc<[PublicBusinessFactV1]>,
        ingress: &DptIngressV1,
        output: &mut CoreApplicationV1<'_>,
    ) {
        self.next_canonical_position += records.len() as u64;
        output.closed_fact_count += records.len() as u32;
        if let Some(groups) = &mut output.fact_groups {
            groups.push(PublicFactGroupV1 {
                visible_at_utc_ms: public_group_visible_at_utc_ms(ingress, &records),
                group_id,
                group_position: self.next_group_position,
                member_input_positions: members,
                closed_at_input_position: self.next_input_position,
                records,
                source_progress_changes: std::mem::take(&mut output.changed_scopes)
                    .into_iter()
                    .map(|scope| self.source_progress[&scope].clone())
                    .collect(),
            });
            self.next_group_position += 1;
            return;
        }
        if let Some(groups) = &mut output.compact_groups {
            groups.push(PublicSourceProgressGroupV1 {
                group_id,
                group_position: self.next_group_position,
                member_input_positions: members,
                closed_at_input_position: self.next_input_position,
                visible_at_utc_ms: public_group_visible_at_utc_ms(ingress, &records),
                source_progress_changes: std::mem::take(&mut output.changed_scopes)
                    .into_iter()
                    .map(|scope| self.source_progress[&scope].clone())
                    .collect(),
                market_bindings: records
                    .iter()
                    .filter_map(|fact| match &fact.payload {
                        PublicBusinessPayloadV1::MarketFact {
                            market,
                            payload: CanonicalMarketFactPayloadV1::FixedIdentity { assets },
                        } => Some(PublicSourceProgressMarketBindingV1 {
                            market: market.clone(),
                            assets: assets.clone(),
                        }),
                        _ => None,
                    })
                    .collect(),
            });
            self.next_group_position += 1;
            return;
        }
        output.changed_scopes.clear();
        let depth = Arc::clone(self.current_depth_projection.get_or_insert_with(|| {
            Arc::new(ObservedFactV1 {
                value: if self.global.depth.value.is_some() {
                    self.binance_depth.current_depth_value()
                } else {
                    None
                },
                quality: self.global.depth.quality,
                direct_anchors: self.global.depth.direct_anchors.clone(),
                dependency_frontier: self.global.depth.dependency_frontier.clone(),
            })
        }));
        let progress: Arc<[SourceStreamProgressV1]> =
            self.source_progress.values().cloned().collect();
        let observations = self
            .markets
            .iter()
            .map(|(id, state)| {
                let mut observation = state.observation(
                    &group_id,
                    self.next_group_position,
                    AppliedIngressPrefixV1 {
                        input_stream_id: self.header.input_stream_id.clone(),
                        next_input_position: self.next_input_position + 1,
                    },
                    &records,
                    self.references.get(id),
                    &self.global,
                    &depth,
                    &self.header.observation_version,
                );
                observation.source_progress = Arc::clone(&progress);
                observation
            })
            .collect();
        output
            .applied
            .closed_groups
            .push(Arc::new(ClosedBusinessGroupV1 {
                group_id,
                group_position: self.next_group_position,
                member_input_positions: members,
                closed_at_input_position: self.next_input_position,
                records,
                observations,
            }));
        self.next_group_position += 1;
    }
    fn apply_fact(&mut self, fact: &PublicBusinessFactV1, group: &Sha256Hex) {
        match &fact.payload {
            PublicBusinessPayloadV1::PolymarketMessage(_) => {}
            PublicBusinessPayloadV1::ReferenceFact(payload) => match payload {
                CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(value) => {
                    self.global.spot = observed(value.clone(), fact, group, "chainlink_spot")
                }
                CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(value) => {
                    match value.window_s {
                        30 => {
                            self.global.thirty =
                                observed(value.clone(), fact, group, "chainlink_twap_thirty")
                        }
                        60 => {
                            self.global.sixty =
                                observed(value.clone(), fact, group, "chainlink_twap_sixty")
                        }
                        _ => unreachable!("来源准备已保留有效 TWAP 窗口"),
                    }
                }
                CanonicalReferenceEventPayloadV2::BinanceDepthState(_) => {
                    self.current_depth_projection = None;
                    self.global.depth = observed((), fact, group, "binance_depth")
                }
                CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(value) => {
                    self.global.trade = observed(value.clone(), fact, group, "binance_trade")
                }
                CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(value) => {
                    self.references
                        .entry(value.market.condition_id.0.clone())
                        .or_default()
                        .official = observed(value.anchor.clone(), fact, group, "official_anchor")
                }
                CanonicalReferenceEventPayloadV2::MarketSettlementReferenceUpdate(value) => {
                    let state = self
                        .references
                        .entry(value.market.condition_id.0.clone())
                        .or_default();
                    if state.rule.value.is_some_and(|old| old != value.reference) {
                        state.changed_rule_record = fact.source_refs.first().cloned();
                    }
                    state.rule = observed(value.reference, fact, group, "settlement_reference");
                }
                CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(_) => {}
            },
            PublicBusinessPayloadV1::MarketFact { market, payload } => {
                let Some(state) = self.markets.get_mut(&market.condition_id.0) else {
                    return;
                };
                match payload {
                    CanonicalMarketFactPayloadV1::FixedIdentity { .. } => {}
                    CanonicalMarketFactPayloadV1::InitialTick {
                        asset,
                        tick_size_pusd_per_share,
                    } => {
                        state.assets.get_mut(&asset.token_id.0).unwrap().tick =
                            observed(tick_size_pusd_per_share.clone(), fact, group, "tick")
                    }
                    CanonicalMarketFactPayloadV1::LifecycleTransition(value) => {
                        state.lifecycle =
                            observed(value.current_state.clone(), fact, group, "lifecycle")
                    }
                    CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot {
                        current_state, ..
                    } => {
                        state.lifecycle = observed(current_state.clone(), fact, group, "lifecycle")
                    }
                    CanonicalMarketFactPayloadV1::TradingState(value) => {
                        state.active = component(value.active, fact, group, "active");
                        state.closed = component(value.closed, fact, group, "closed");
                        state.accepting_orders =
                            component(value.accepting_orders, fact, group, "accepting_orders");
                    }
                    CanonicalMarketFactPayloadV1::Terminal(value) => {
                        state.resolution = observed(value.clone(), fact, group, "terminal")
                    }
                    CanonicalMarketFactPayloadV1::QualityBoundary(value) => {
                        state.resolution.quality = value.current_quality;
                        if !matches!(
                            value.current_quality,
                            MarketDataQuality::Fresh | MarketDataQuality::Stale
                        ) {
                            state.resolution.value = None;
                        }
                    }
                }
            }
        }
    }
}

impl MarketState {
    fn apply_pm_projection(
        &mut self,
        reduction: &PublicPmReductionV1<'_>,
        fact: &PublicBusinessFactV1,
        group: &Sha256Hex,
        payload: &CanonicalPmPayloadV1,
    ) {
        for step in &reduction.steps {
            match &step.change {
                PmStepChangeV1::ResetAsset { asset, .. } => {
                    self.level_anchors
                        .retain(|(token, ..), _| token != &asset.token_id.0);
                    let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                    state.book = ObservedFactV1 {
                        value: None,
                        // 重置不新增质量事实；真实缺失沿共同算法的 Quality 步骤更新。
                        quality: state.book.quality,
                        ..ObservedFactV1::default()
                    };
                    // BBA 模型值保留原最后事实；执行路径的重锚由所属执行器处理。
                }
                PmStepChangeV1::InitialSnapshot {
                    asset, bids, asks, ..
                } => {
                    self.assets.get_mut(&asset.token_id.0).unwrap().book =
                        observed_pm((), step, fact, group, "l2");
                    for (side, levels) in [
                        (BookSide::Bid, bids.as_ref()),
                        (BookSide::Ask, asks.as_ref()),
                    ] {
                        for level in levels {
                            self.level_anchors.insert(
                                key(&asset.token_id.0, side, d(&level.price_pusd_per_share.0)),
                                observed_pm(
                                    (),
                                    step,
                                    fact,
                                    group,
                                    &level_field_path(asset, side, &level.price_pusd_per_share),
                                ),
                            );
                        }
                    }
                }
                PmStepChangeV1::Selection {
                    asset, side, price, ..
                } => {
                    self.level_anchors.insert(
                        key(&asset.token_id.0, *side, d(&price.0)),
                        observed_pm(
                            (),
                            step,
                            fact,
                            group,
                            &level_field_path(asset, *side, price),
                        ),
                    );
                }
                PmStepChangeV1::AbsenceBaseline {
                    asset, selected, ..
                } => {
                    if selected.is_some() {
                        self.assets.get_mut(&asset.token_id.0).unwrap().book =
                            observed_pm((), step, fact, group, "l2.absence");
                    }
                }
                PmStepChangeV1::BookReady { asset } => {
                    self.assets.get_mut(&asset.token_id.0).unwrap().book.quality =
                        MarketDataQuality::Fresh
                }
                PmStepChangeV1::BestBidAsk { quote, .. } => {
                    let direct_quote =
                        direct_bba(payload, step.member_index).unwrap_or(CanonicalBbaFactV1 {
                            best_bid_pusd_per_share: quote.best_bid_pusd_per_share.clone(),
                            best_ask_pusd_per_share: quote.best_ask_pusd_per_share.clone(),
                            spread_pusd_per_share: None,
                        });
                    self.assets.get_mut(&quote.asset.token_id.0).unwrap().bba =
                        observed_pm(direct_quote, step, fact, group, "best_bid_ask");
                }
                PmStepChangeV1::Tick { tick, .. } => {
                    self.assets.get_mut(&tick.asset.token_id.0).unwrap().tick = observed_pm(
                        tick.new_tick_size_pusd_per_share.clone(),
                        step,
                        fact,
                        group,
                        "tick",
                    )
                }
                PmStepChangeV1::PublicConfirmation { asset, kind } => {
                    let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                    match kind {
                        PmConfirmationComponentV1::Tick => {
                            if let Some(value) = state.tick.value.clone() {
                                state.tick = observed_pm(value, step, fact, group, "tick");
                            }
                        }
                        PmConfirmationComponentV1::BestBidAsk => {
                            if let Some(value) = direct_bba(payload, step.member_index)
                                .or_else(|| state.bba.value.clone())
                            {
                                state.bba = observed_pm(value, step, fact, group, "best_bid_ask");
                            }
                        }
                    }
                }
                PmStepChangeV1::PublicTrade { trade } => {
                    self.assets.get_mut(&trade.asset.token_id.0).unwrap().trade =
                        observed_pm(trade.clone(), step, fact, group, "public_trade")
                }
                PmStepChangeV1::Quality { boundary } => {
                    if let QualityScopeV1::Asset(asset) = &boundary.scope {
                        if let Some(asset) = self.assets.get_mut(&asset.token_id.0) {
                            asset.book.quality = boundary.current_quality;
                        }
                    }
                }
                _ => {}
            }
            // 直接BBA的最新来源投影只服务派生读取；原模型BBA字段仍保留到达语义。
            let asset = match &step.change {
                PmStepChangeV1::BestBidAsk { quote, .. } => Some(&quote.asset),
                PmStepChangeV1::PublicConfirmation {
                    asset,
                    kind: PmConfirmationComponentV1::BestBidAsk,
                } => Some(asset),
                _ => None,
            };
            if let Some(asset) = asset {
                let source = &fact.direct_sources[0];
                let admission = source.collector_admission.as_ref().expect("PM 准入坐标");
                let version = crate::pm_reduction::PmL2SelectionVersionV1 {
                    source_sequence_epoch_id: admission.source_sequence_epoch_id.clone(),
                    source_at_utc_ms: source
                        .source_event_at_utc_ms
                        .expect("PM 准备已取得来源时间"),
                    collector_admission_sequence: admission.collector_admission_sequence,
                    member_index: step.member_index,
                };
                let state = self.assets.get_mut(&asset.token_id.0).unwrap();
                if state.valuation_bba_version.as_ref().is_none_or(|old| {
                    old.source_sequence_epoch_id != version.source_sequence_epoch_id
                        || version.is_at_least(old)
                }) {
                    let bid = state
                        .bba
                        .value
                        .as_ref()
                        .and_then(|q| q.best_bid_pusd_per_share.clone());
                    state.valuation_bba = observed_pm(bid, step, fact, group, "valuation_best_bid");
                    state.valuation_bba_version = Some(version);
                }
            }
        }
    }
    /// 同截止只读派生；下方数量更新不刷新首档版本，L2缺失不使独立BBA失效。
    fn valuation_bid(
        &self,
        token: &str,
        bids: &ObservedFactV1<Vec<PriceLevelV1>>,
    ) -> ObservedFactV1<Option<ExactDecimal>> {
        let state = &self.assets[token];
        if bids.quality != MarketDataQuality::Fresh || bids.value.is_none() {
            return state.valuation_bba.clone();
        }
        let baseline = self.pm.assets[token].selection_baseline.as_ref();
        let selected_at = |price: &ExactDecimal| {
            self.pm
                .selected
                .get(&key(token, BookSide::Bid, d(&price.0)))
                .into_iter()
                .chain(baseline)
                .max_by_key(|s| {
                    (
                        s.version.source_at_utc_ms,
                        s.version.collector_admission_sequence,
                        s.version.member_index,
                    )
                })
        };
        let direct = state
            .valuation_bba_version
            .as_ref()
            .filter(|v| self.pm.epoch.as_ref() == Some(&v.source_sequence_epoch_id));
        let bid = state.valuation_bba.value.as_ref().and_then(|p| p.as_ref());
        // 点位删除只撤销该直接买价，不能抹除BBA排除了旧更高价的证据。
        let quote_removed = direct.is_some_and(|v| {
            bid.is_some_and(|p| {
                selected_at(p).is_some_and(|s| s.version != *v && s.version.is_at_least(v))
                    && self.pm.quantity(token, BookSide::Bid, d(&p.0)).is_zero()
            })
        });
        let best = bids
            .value
            .as_ref()
            .unwrap()
            .iter()
            .filter(|l| d(&l.quantity_shares.0) > Decimal::ZERO)
            .find(|l| {
                direct.is_none_or(|v| {
                    let newer = selected_at(&l.price_pusd_per_share)
                        .is_some_and(|s| !v.is_at_least(&s.version));
                    match bid {
                        Some(p) if d(&l.price_pusd_per_share.0) <= d(&p.0) => {
                            quote_removed || (d(&l.price_pusd_per_share.0) == d(&p.0) && newer)
                        }
                        _ => newer,
                    }
                })
            })
            .map(|l| l.price_pusd_per_share.clone());
        if direct.is_some() && best.is_none() && !quote_removed {
            return state.valuation_bba.clone();
        }
        ObservedFactV1 {
            value: Some(best),
            quality: bids.quality,
            direct_anchors: bids.direct_anchors.clone(),
            dependency_frontier: bids.dependency_frontier.clone(),
        }
    }
    fn observation(
        &self,
        group: &Sha256Hex,
        group_position: u64,
        prefix: AppliedIngressPrefixV1,
        events: &Arc<[PublicBusinessFactV1]>,
        reference: Option<&PublicMarketReferenceStateV1>,
        global: &GlobalReferences,
        depth: &Arc<ObservedFactV1<BinanceDepthStateReferenceV1>>,
        version: &str,
    ) -> MarketBusinessObservationV1 {
        let unknown = PublicMarketReferenceStateV1::default();
        let reference = reference.unwrap_or(&unknown);
        let assets = self
            .pm
            .context
            .assets
            .iter()
            .map(|context| {
                let token = &context.asset.token_id.0;
                let state = &self.assets[token];
                let side = |side: BookSide| {
                    if !self.pm.assets[token].book_ready {
                        return ObservedFactV1 {
                            quality: state.book.quality,
                            ..ObservedFactV1::default()
                        };
                    }
                    let mut levels: Vec<_> = self
                        .pm
                        .quantities
                        .iter()
                        .flat_map(|quantities| quantities.iter())
                        .filter(|((id, ask, _), _)| id == token && *ask == (side == BookSide::Ask))
                        .map(|((_, _, price), quantity)| PriceLevelV1 {
                            price_pusd_per_share: ExactDecimal(price.normalize().to_string()),
                            quantity_shares: ExactDecimal(quantity.normalize().to_string()),
                        })
                        .collect();
                    if side == BookSide::Bid {
                        levels.reverse();
                    }
                    let mut anchors = Vec::new();
                    let mut frontier = state.book.dependency_frontier.clone();
                    for ((id, ask, _), evidence) in &self.level_anchors {
                        if id == token && *ask == (side == BookSide::Ask) {
                            for anchor in &evidence.direct_anchors {
                                if !anchors.contains(anchor) {
                                    anchors.push(anchor.clone());
                                }
                            }
                            if evidence.dependency_frontier.as_ref().is_some_and(|value| {
                                frontier.as_ref().is_none_or(|old| {
                                    value.next_input_position > old.next_input_position
                                })
                            }) {
                                frontier = evidence.dependency_frontier.clone();
                            }
                        }
                    }
                    if anchors.is_empty() {
                        anchors = state.book.direct_anchors.clone();
                    }
                    ObservedFactV1 {
                        value: Some(levels),
                        quality: state.book.quality,
                        direct_anchors: anchors,
                        dependency_frontier: frontier,
                    }
                };
                let bids = side(BookSide::Bid);
                let valuation_best_bid = self.valuation_bid(token, &bids);
                AssetBusinessObservationV1 {
                    asset: context.asset.clone(),
                    bids,
                    asks: side(BookSide::Ask),
                    best_bid_ask: state.bba.clone(),
                    valuation_best_bid,
                    tick: state.tick.clone(),
                    public_trade: state.trade.clone(),
                }
            })
            .collect();
        let effective_reference = if reference.official.value.is_some() {
            EffectiveMarketReferenceV1::OfficialAnchor(reference.official.clone())
        } else if let Some(value) = &reference.temporary {
            EffectiveMarketReferenceV1::TemporaryTwap(value.clone())
        } else {
            EffectiveMarketReferenceV1::Missing
        };
        MarketBusinessObservationV1 {
            schema_version: version.to_owned(),
            market: self.pm.context.market.clone(),
            group_id: group.clone(),
            group_position,
            visible_prefix: prefix,
            assets,
            lifecycle: self.lifecycle.clone(),
            active: self.active.clone(),
            closed: self.closed.clone(),
            accepting_orders: self.accepting_orders.clone(),
            terminal: self.resolution.clone(),
            official_anchor: reference.official.clone(),
            settlement_reference: reference.rule.clone(),
            temporary_reference: reference.temporary.clone(),
            effective_reference,
            chainlink_spot: global.spot.clone(),
            chainlink_twap_thirty: global.thirty.clone(),
            chainlink_twap_sixty: global.sixty.clone(),
            binance_depth: Arc::clone(depth),
            binance_trade: global.trade.clone(),
            group_events: Arc::clone(events),
            source_progress: Arc::from([]),
        }
    }
}

fn observed_pm<T>(
    value: T,
    step: &PmLogicalStepV1<'_>,
    fact: &PublicBusinessFactV1,
    group: &Sha256Hex,
    field: &str,
) -> ObservedFactV1<T> {
    let source = step.direct.source();
    ObservedFactV1 {
        value: Some(value),
        quality: step.quality.availability,
        direct_anchors: vec![DirectObservationAnchorV1 {
            field_path: field.to_owned(),
            position: fact.position.clone(),
            group_id: group.clone(),
            source_message_id: source.source_message_id.clone(),
            source_event_at_utc_ms: source.source_event_at_utc_ms,
            received_at_utc_ms: source.received_at_utc_ms,
        }],
        dependency_frontier: Some(AppliedIngressPrefixV1 {
            input_stream_id: fact.position.input_stream_id.clone(),
            next_input_position: fact.position.closed_at_input_position + 1,
        }),
    }
}

#[cfg(test)]
#[path = "public_observation_core_tests.rs"]
mod tests;

fn direct_bba(payload: &CanonicalPmPayloadV1, member: Option<u32>) -> Option<CanonicalBbaFactV1> {
    match payload {
        CanonicalPmPayloadV1::BestBidAsk { quote, .. } => Some(quote.clone()),
        CanonicalPmPayloadV1::Book {
            fact: CanonicalBookFactV1::AbsoluteLevelChanges { changes },
        } => member
            .and_then(|i| changes.get(i as usize))
            .and_then(|change| change.best_bid_ask.clone()),
        _ => None,
    }
}

fn level_field_path(asset: &AssetIdentityV1, side: BookSide, price: &ExactDecimal) -> String {
    format!(
        "assets[{}].{}[{}]",
        asset.token_id.0,
        if side == BookSide::Bid {
            "bids"
        } else {
            "asks"
        },
        price.0
    )
}

impl PublicAppliedIngressV1<'_> {
    pub fn into_owned(self) -> PublicAppliedIngressV1<'static> {
        PublicAppliedIngressV1 {
            applied: self.applied,
            reductions: self.reductions,
            pm_reductions: self
                .pm_reductions
                .into_iter()
                .map(|(p, r)| (p, r.into_owned()))
                .collect(),
        }
    }
}
