//! 完整历史约束场景。输入为公共归约后的规范 BUY 坐标，数量使用微 shares。
//! 原接收时间和顺序保留在输入中；内部时间是显式模型假设，不进入行情观察。
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

/// Reuse the public execution owner's six-decimal quantity conversion. It
/// truncates sub-micro fractions and avoids an intermediate Decimal multiply.
pub fn scene_micro_units(
    value: rust_decimal::Decimal,
) -> Result<u128, Box<dyn std::error::Error + Send + Sync>> {
    Ok(crate::matching::micro_units_v1(value)?)
}

pub const MODEL_VERSION: &str = "LOCAL_PROBABILISTIC_SHARED_FIFO_V2";
pub const FRAGMENT_MODEL_VERSION: &str = "LOCAL_PROBABILISTIC_SHARED_FIFO_V1";
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Level {
    pub condition: String,
    pub buy_token: String,
    pub price_micro: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FactKind {
    Initial(u128),
    Add(u128),
    Reduce(u128),
    Trade(u128),
    /// 快照/恢复是独立参考修正，不能冒充普通撤单。
    Recovery {
        before: u128,
        after: u128,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fact {
    pub sequence: u64,
    pub received_at_ms: i64,
    pub source_at_ms: Option<i64>,
    pub level: Level,
    pub kind: FactKind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Allocation {
    Nearest,
    DistanceWeighted,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cancellation {
    Front,
    Back,
    /// 旧逐碎片候选；保留失败身份与可复现算法，不作为新主情景。
    Proportional,
    SegmentProportionalV2,
    Concentrated,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InternalTime {
    Received,
    SourceWhenPresent,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Parameters {
    pub association_ms: u64,
    pub allocation: Allocation,
    pub cancellation: Cancellation,
    pub internal_time: InternalTime,
    pub seed: u64,
    /// 仅用于错位关联对照，不移动实际需求时间。
    pub association_trade_shift_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slice {
    pub segment: u64,
    pub start: u128,
    pub end: u128,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Add {
        segment: u64,
        quantity: u128,
        implicit: bool,
    },
    Cancel {
        targets: Vec<Slice>,
        recovery: bool,
    },
    Demand {
        quantity: u128,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub at_ms: i64,
    pub source_sequence: u64,
    pub level: Level,
    pub kind: EventKind,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparationReport {
    pub trade_quantity: u128,
    pub reduction_quantity: u128,
    pub associated_quantity: u128,
    pub unexplained_trade: u128,
    pub cancellation_quantity: u128,
    pub implicit_quantity: u128,
    pub recovery_quantity: u128,
    pub unavailable_cancellation_quantity: u128,
    pub candidate_edges: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedScene {
    pub model_version: String,
    pub parameters: Parameters,
    pub events: Vec<Event>,
    pub report: PreparationReport,
}
// 无跨事件可变随机游标：策略拆单、ID、线程排程均不改变外部场景。
fn random(seed: u64, event: u64, salt: u64) -> u64 {
    let mut x = seed ^ event.wrapping_mul(0x9e3779b97f4a7c15) ^ salt;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn time(f: &Fact, p: &Parameters) -> i64 {
    match p.internal_time {
        InternalTime::Received => f.received_at_ms,
        InternalTime::SourceWhenPresent => f.source_at_ms.unwrap_or(f.received_at_ms),
    }
}
/// 关联分析复用同一份事实索引；不同种子仅重做容量分配和场景构造。
#[derive(Clone, Debug)]
pub struct Analysis {
    facts: Vec<Fact>,
    levels: BTreeMap<Level, Vec<usize>>,
}
impl Analysis {
    pub fn new(facts: Vec<Fact>) -> Self {
        let mut levels = BTreeMap::<Level, Vec<usize>>::new();
        for (i, f) in facts.iter().enumerate() {
            levels.entry(f.level.clone()).or_default().push(i);
        }
        for indices in levels.values_mut() {
            indices.sort_by_key(|i| (facts[*i].received_at_ms, facts[*i].sequence));
        }
        Self { facts, levels }
    }
    pub fn prepare(&self, p: Parameters) -> PreparedScene {
        self.prepare_with_progress(p, |_, _, _| {})
    }
    pub fn prepare_with_progress(
        &self,
        p: Parameters,
        progress: impl FnMut(&str, usize, usize),
    ) -> PreparedScene {
        let mut events = Vec::new();
        let mut scene = self
            .prepare_with_sink(p, progress, |event| {
                events.push(event);
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
        scene.events = events;
        scene
    }
    /// Compare local association rules without constructing unused queue scenes.
    pub fn association_report(&self, p: &Parameters) -> PreparationReport {
        self.associate(p, &mut |_, _, _| {}).1
    }
    fn associate(
        &self,
        p: &Parameters,
        progress: &mut impl FnMut(&str, usize, usize),
    ) -> (Vec<u128>, PreparationReport) {
        progress("association", 0, self.facts.len());
        let mut processed = 0;
        let mut report = PreparationReport::default();
        let mut explained = vec![0u128; self.facts.len()];
        let mut remaining = vec![0u128; self.facts.len()];
        for (i, f) in self.facts.iter().enumerate() {
            match f.kind {
                FactKind::Reduce(q) => {
                    remaining[i] = q;
                    report.reduction_quantity += q;
                }
                FactKind::Trade(q) => report.trade_quantity += q,
                _ => {}
            }
        }
        // 每笔需求和每条减量只有一份容量；局部窗口通过二分定位，不做全日笛卡尔积。
        for indices in self.levels.values() {
            for &i in indices {
                processed += 1;
                if processed % 2048 == 0 {
                    progress("association", processed, self.facts.len());
                }
                let FactKind::Trade(mut budget) = self.facts[i].kind else {
                    continue;
                };
                let center = self.facts[i]
                    .received_at_ms
                    .saturating_add(p.association_trade_shift_ms);
                let width = i64::try_from(p.association_ms).unwrap_or(i64::MAX);
                let lo = indices.partition_point(|j| {
                    self.facts[*j].received_at_ms < center.saturating_sub(width)
                });
                let hi = indices.partition_point(|j| {
                    self.facts[*j].received_at_ms <= center.saturating_add(width)
                });
                let mut candidates: Vec<_> = indices[lo..hi]
                    .iter()
                    .copied()
                    .filter(|j| remaining[*j] > 0)
                    .collect();
                report.candidate_edges += candidates.len() as u64;
                candidates.sort_by(|a, b| {
                    let score = |j: usize| {
                        let distance = self.facts[j].received_at_ms.abs_diff(center);
                        match p.allocation {
                            Allocation::Nearest => distance as f64,
                            Allocation::DistanceWeighted => {
                                let u =
                                    (random(p.seed, self.facts[i].sequence, self.facts[j].sequence)
                                        as f64
                                        + 1.)
                                        / (u64::MAX as f64 + 2.);
                                -u.ln() * (distance as f64 + 1.)
                            }
                        }
                    };
                    score(*a)
                        .total_cmp(&score(*b))
                        .then_with(|| self.facts[*a].sequence.cmp(&self.facts[*b].sequence))
                });
                for j in candidates {
                    let q = budget.min(remaining[j]);
                    remaining[j] -= q;
                    budget -= q;
                    explained[j] += q;
                    report.associated_quantity += q;
                    if budget == 0 {
                        break;
                    }
                }
                report.unexplained_trade += budget;
            }
        }
        progress("association", self.facts.len(), self.facts.len());
        (explained, report)
    }
    /// Plan global Add identities using only level quantities, before any
    /// condition worker samples coordinates. Facts and association arrays stay shared.
    pub fn condition_plan(&self, p: Parameters) -> ConditionPreparationPlan<'_> {
        self.condition_plan_with_progress(p, |_, _, _| {})
    }
    pub fn condition_plan_with_progress(
        &self,
        p: Parameters,
        mut progress: impl FnMut(&str, usize, usize),
    ) -> ConditionPreparationPlan<'_> {
        let (explained, report) = self.associate(&p, &mut progress);
        progress("quantity_sort", 0, self.facts.len());
        let mut ordered: Vec<_> = (0..self.facts.len()).collect();
        ordered.sort_by_key(|i| (time(&self.facts[*i], &p), self.facts[*i].sequence));
        let mut quantities = BTreeMap::<Level, u128>::new();
        let mut segments = vec![None; self.facts.len()];
        let mut ranks = vec![0; self.facts.len()];
        let mut groups = BTreeMap::<String, Vec<usize>>::new();
        let mut next_segment = 0;
        progress("quantity_plan", 0, self.facts.len());
        for (rank, &i) in ordered.iter().enumerate() {
            if rank % 16384 == 0 {
                progress("quantity_plan", rank, self.facts.len());
            }
            ranks[i] = rank;
            let fact = &self.facts[i];
            groups
                .entry(fact.level.condition.clone())
                .or_default()
                .push(i);
            let mut add = |q: u128, quantities: &mut BTreeMap<Level, u128>| {
                if q > 0 {
                    next_segment += 1;
                    segments[i] = Some((next_segment, q));
                    *quantities.entry(fact.level.clone()).or_default() += q;
                }
            };
            match fact.kind {
                FactKind::Initial(q) | FactKind::Add(q) => add(q, &mut quantities),
                FactKind::Trade(q) => {
                    let last = Level {
                        price_micro: 1_000_000,
                        ..fact.level.clone()
                    };
                    let available: u128 = quantities
                        .range(fact.level.clone()..=last.clone())
                        .map(|(_, q)| *q)
                        .sum();
                    if available < q {
                        add(q - available, &mut quantities);
                    }
                    let mut demand = q;
                    for (_, live) in quantities.range_mut(fact.level.clone()..=last).rev() {
                        let used = demand.min(*live);
                        *live -= used;
                        demand -= used;
                        if demand == 0 {
                            break;
                        }
                    }
                    assert_eq!(demand, 0);
                }
                FactKind::Reduce(q) => {
                    let live = quantities.entry(fact.level.clone()).or_default();
                    *live -= (*live).min(q - explained[i]);
                }
                FactKind::Recovery { before, after } => {
                    if after > before {
                        add(after - before, &mut quantities);
                    } else {
                        let live = quantities.entry(fact.level.clone()).or_default();
                        *live -= (*live).min(before - after);
                    }
                }
            }
        }
        progress("quantity_plan", self.facts.len(), self.facts.len());
        ConditionPreparationPlan {
            analysis: self,
            parameters: p,
            explained,
            report,
            segments,
            ranks,
            groups: groups.into_iter().collect(),
        }
    }
    /// The sink receives events in the same global order as the in-memory path.
    /// Only reference state with no remaining internal-time fact is released.
    pub fn prepare_with_sink<E>(
        &self,
        p: Parameters,
        mut progress: impl FnMut(&str, usize, usize),
        mut sink: impl FnMut(Event) -> Result<(), E>,
    ) -> Result<PreparedScene, E> {
        let (explained, report) = self.associate(&p, &mut progress);
        let mut ordered: Vec<_> = (0..self.facts.len()).collect();
        ordered.sort_by_key(|i| (time(&self.facts[*i], &p), self.facts[*i].sequence));
        self.prepare_ordered(
            p,
            &ordered,
            &explained,
            report,
            None,
            progress,
            |_, event| sink(event),
        )
    }
    fn prepare_ordered<E>(
        &self,
        p: Parameters,
        ordered: &[usize],
        explained: &[u128],
        mut report: PreparationReport,
        segment_plan: Option<&[Option<(u64, u128)>]>,
        mut progress: impl FnMut(&str, usize, usize),
        mut sink: impl FnMut(usize, Event) -> Result<(), E>,
    ) -> Result<PreparedScene, E> {
        progress("scenario", 0, ordered.len());
        let mut reference = SharedQueue::default();
        let mut remaining_conditions = BTreeMap::<&str, usize>::new();
        for &i in ordered {
            let fact = &self.facts[i];
            *remaining_conditions
                .entry(&fact.level.condition)
                .or_default() += 1;
        }
        let mut next_segment = 0;
        for (completed, &i) in ordered.iter().enumerate() {
            if completed % 64 == 0 {
                let fragments = reference
                    .levels
                    .get(&self.facts[i].level)
                    .into_iter()
                    .flatten()
                    .map(|entry| match entry {
                        Entry::External { ranges, .. } => ranges.len(),
                        Entry::Own { .. } => 0,
                    })
                    .sum::<usize>();
                progress(
                    &format!("scenario:level_fragments={fragments}"),
                    completed,
                    ordered.len(),
                );
            }
            let f = &self.facts[i];
            let at = time(f, &p);
            let mut emit = |kind: EventKind, reference: &mut SharedQueue| {
                let e = Event {
                    at_ms: at,
                    source_sequence: f.sequence,
                    level: f.level.clone(),
                    kind,
                };
                reference.apply(&e);
                sink(i, e)
            };
            let mut add = |q: u128, implicit: bool, reference: &mut SharedQueue| {
                if q > 0 {
                    if let Some(plan) = segment_plan {
                        let (id, quantity) = plan[i].expect("数量预计划缺少正量Add");
                        assert_eq!(q, quantity, "数量预计划和参考队列缺口必须一致");
                        next_segment = id;
                    } else {
                        next_segment += 1;
                    }
                    emit(
                        EventKind::Add {
                            segment: next_segment,
                            quantity: q,
                            implicit,
                        },
                        reference,
                    )?;
                }
                Ok::<(), E>(())
            };
            match f.kind {
                FactKind::Initial(q) | FactKind::Add(q) => add(q, false, &mut reference)?,
                FactKind::Trade(q) => {
                    let available = reference.demand_external_quantity(&f.level);
                    if available < q {
                        report.implicit_quantity += q - available;
                        add(q - available, true, &mut reference)?;
                    }
                    emit(EventKind::Demand { quantity: q }, &mut reference)?;
                }
                FactKind::Reduce(q) => {
                    let cancellation = q - explained[i];
                    report.cancellation_quantity += cancellation;
                    let targets = reference.select_cancellation(
                        &f.level,
                        cancellation,
                        p.cancellation,
                        p.seed,
                        f.sequence,
                    );
                    report.unavailable_cancellation_quantity +=
                        cancellation - targets.iter().map(|t| t.end - t.start).sum::<u128>();
                    emit(
                        EventKind::Cancel {
                            targets,
                            recovery: false,
                        },
                        &mut reference,
                    )?;
                }
                FactKind::Recovery { before, after } => {
                    report.recovery_quantity += before.abs_diff(after);
                    if after > before {
                        add(after - before, false, &mut reference)?
                    } else {
                        let targets = reference.select_cancellation(
                            &f.level,
                            before - after,
                            p.cancellation,
                            p.seed,
                            f.sequence,
                        );
                        emit(
                            EventKind::Cancel {
                                targets,
                                recovery: true,
                            },
                            &mut reference,
                        )?;
                    }
                }
            }
            let left = remaining_conditions
                .get_mut(f.level.condition.as_str())
                .unwrap();
            *left -= 1;
            if *left == 0 {
                reference.discard_condition(&f.level.condition);
            }
        }
        progress("scenario", ordered.len(), ordered.len());
        Ok(PreparedScene {
            model_version: if p.cancellation == Cancellation::Proportional {
                FRAGMENT_MODEL_VERSION
            } else {
                MODEL_VERSION
            }
            .into(),
            parameters: p,
            events: Vec::new(),
            report,
        })
    }
}
/// A condition contains both complementary tokens and all its price levels.
/// Workers own only their queue; the original facts and global plan are borrowed.
pub struct ConditionPreparationPlan<'a> {
    analysis: &'a Analysis,
    parameters: Parameters,
    explained: Vec<u128>,
    report: PreparationReport,
    segments: Vec<Option<(u64, u128)>>,
    ranks: Vec<usize>,
    groups: Vec<(String, Vec<usize>)>,
}
impl ConditionPreparationPlan<'_> {
    pub fn condition_count(&self) -> usize {
        self.groups.len()
    }
    pub fn condition_name(&self, index: usize) -> &str {
        &self.groups[index].0
    }
    pub fn prepare_condition<E>(
        &self,
        index: usize,
        progress: impl FnMut(&str, usize, usize),
        mut sink: impl FnMut(usize, Event) -> Result<(), E>,
    ) -> Result<PreparedScene, E> {
        self.analysis.prepare_ordered(
            self.parameters.clone(),
            &self.groups[index].1,
            &self.explained,
            PreparationReport::default(),
            Some(&self.segments),
            progress,
            |i, event| sink(self.ranks[i], event),
        )
    }
    pub fn combine_reports(
        &self,
        reports: impl IntoIterator<Item = PreparationReport>,
    ) -> PreparationReport {
        let mut combined = self.report.clone();
        for report in reports {
            combined.cancellation_quantity += report.cancellation_quantity;
            combined.implicit_quantity += report.implicit_quantity;
            combined.recovery_quantity += report.recovery_quantity;
            combined.unavailable_cancellation_quantity += report.unavailable_cancellation_quantity;
        }
        combined
    }
}

// 存储块只加速排名定位和局部差集，不改变原段或存活区间的边界。
const RANGE_BLOCK_SIZE: usize = 4096;
#[derive(Clone, Debug)]
struct RangeBlock {
    ranges: Vec<(u128, u128)>,
    live: u128,
}
#[derive(Clone, Debug, Default)]
struct LiveRanges {
    blocks: VecDeque<RangeBlock>,
}
impl PartialEq for LiveRanges {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for LiveRanges {}
impl Serialize for LiveRanges {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(self.len()))?;
        for r in self.iter() {
            seq.serialize_element(r)?;
        }
        seq.end()
    }
}
impl<'de> Deserialize<'de> for LiveRanges {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Vec::<(u128, u128)>::deserialize(d)?;
        if v.iter().any(|(a, b)| a >= b) {
            return Err(serde::de::Error::custom("存活区间必须为正长度"));
        }
        Ok(Self::from_vec(v))
    }
}
impl LiveRanges {
    fn from_vec(v: Vec<(u128, u128)>) -> Self {
        let mut blocks = VecDeque::new();
        for ranges in v.chunks(RANGE_BLOCK_SIZE) {
            let live = ranges.iter().map(|(a, b)| b - a).sum();
            blocks.push_back(RangeBlock {
                ranges: ranges.to_vec(),
                live,
            });
        }
        Self { blocks }
    }
    fn iter(&self) -> impl Iterator<Item = &(u128, u128)> {
        self.blocks.iter().flat_map(|b| b.ranges.iter())
    }
    fn len(&self) -> usize {
        self.blocks.iter().map(|b| b.ranges.len()).sum()
    }
    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
    fn live_quantity(&self) -> u128 {
        self.blocks.iter().map(|b| b.live).sum()
    }
    fn push_block(out: &mut VecDeque<RangeBlock>, block: RangeBlock) {
        if block.ranges.is_empty() {
            return;
        }
        if let Some(last) = out.back_mut() {
            if last.ranges.len() + block.ranges.len() <= RANGE_BLOCK_SIZE {
                last.live += block.live;
                last.ranges.extend(block.ranges);
                return;
            }
        }
        out.push_back(block);
    }
    fn subtract(&mut self, cuts: &mut [(u128, u128)]) {
        if cuts.is_empty() {
            return;
        }
        cuts.sort_unstable();
        let mut count = 0;
        for i in 0..cuts.len() {
            let cut = cuts[i];
            if count > 0 && cut.0 <= cuts[count - 1].1 {
                cuts[count - 1].1 = cuts[count - 1].1.max(cut.1);
            } else {
                cuts[count] = cut;
                count += 1;
            }
        }
        let cuts = &cuts[..count];
        let mut out = VecDeque::new();
        let mut at = 0;
        for mut block in std::mem::take(&mut self.blocks) {
            let start = block.ranges[0].0;
            let end = block.ranges.last().unwrap().1;
            while at < cuts.len() && cuts[at].1 <= start {
                at += 1;
            }
            if at < cuts.len() && cuts[at].0 < end {
                let last = at + cuts[at..].partition_point(|cut| cut.0 < end);
                subtract_sorted_ranges(&mut block.ranges, &cuts[at..last]);
                block.live = block.ranges.iter().map(|(a, b)| b - a).sum();
            }
            Self::push_block(&mut out, block);
        }
        self.blocks = out;
    }
    fn consume_prefix(&mut self, mut budget: u128) -> u128 {
        let original = budget;
        while budget > 0 {
            let Some(front) = self.blocks.front_mut() else {
                break;
            };
            if budget >= front.live {
                budget -= front.live;
                self.blocks.pop_front();
                continue;
            }
            for (a, b) in &mut front.ranges {
                let n = budget.min(*b - *a);
                *a += n;
                budget -= n;
                front.live -= n;
                if budget == 0 {
                    break;
                }
            }
            front.ranges.retain(|(a, b)| a < b);
        }
        original - budget
    }
    fn rank_targets(&self, segment: u64, start: u128, end: u128, out: &mut Vec<Slice>) {
        let mut rank = 0;
        for block in &self.blocks {
            if rank + block.live <= start {
                rank += block.live;
                continue;
            }
            for &(a, b) in &block.ranges {
                let len = b - a;
                let left = start.max(rank);
                let right = end.min(rank + len);
                if left < right {
                    out.push(Slice {
                        segment,
                        start: a + left - rank,
                        end: a + right - rank,
                    });
                }
                rank += len;
                if rank >= end {
                    return;
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Entry {
    External {
        segment: u64,
        total: u128,
        ranges: LiveRanges,
    },
    Own {
        order: String,
        quantity: u128,
    },
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SharedQueue {
    #[serde(with = "level_entries")]
    levels: BTreeMap<Level, VecDeque<Entry>>,
}
mod level_entries {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &BTreeMap<Level, VecDeque<Entry>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Level, VecDeque<Entry>>, D::Error> {
        let values = Vec::<(Level, VecDeque<Entry>)>::deserialize(deserializer)?;
        let mut result = BTreeMap::new();
        for (level, queue) in values {
            if result.insert(level, queue).is_some() {
                return Err(serde::de::Error::custom("队列含重复价位"));
            }
        }
        Ok(result)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fill {
    pub order: String,
    pub quantity: u128,
    pub level: Level,
}
#[cfg(test)]
fn remove_ranges(ranges: &mut Vec<(u128, u128)>, cuts: &mut [(u128, u128)]) {
    cuts.sort_unstable();
    subtract_sorted_ranges(ranges, cuts)
}
fn subtract_sorted_ranges(ranges: &mut Vec<(u128, u128)>, cuts: &[(u128, u128)]) {
    let mut next = Vec::with_capacity(ranges.len());
    let mut cut = 0;
    for &(a, b) in ranges.iter() {
        let mut cursor = a;
        while cut < cuts.len() && cuts[cut].1 <= cursor {
            cut += 1;
        }
        let mut index = cut;
        while index < cuts.len() && cuts[index].0 < b {
            let (start, end) = cuts[index];
            if cursor < start {
                next.push((cursor, start.min(b)));
            }
            cursor = cursor.max(end).min(b);
            if cursor == b {
                break;
            }
            index += 1;
        }
        if cursor < b {
            next.push((cursor, b));
        }
    }
    *ranges = next;
}

impl SharedQueue {
    pub(crate) fn external_levels<'a>(
        &'a self,
        condition: &str,
        buy_token: &str,
    ) -> impl Iterator<Item = (&'a Level, u128)> + use<'a> {
        let first = Level {
            condition: condition.into(),
            buy_token: buy_token.into(),
            price_micro: 0,
        };
        let last = Level {
            price_micro: 1_000_000,
            ..first.clone()
        };
        self.levels
            .range(first..=last)
            .rev()
            .map(|(level, _)| (level, self.external_quantity(level)))
            .filter(|(_, q)| *q > 0)
    }
    pub(crate) fn taker_targets(&self, level: &Level, mut quantity: u128) -> Vec<Slice> {
        let mut segments = self
            .levels
            .get(level)
            .into_iter()
            .flatten()
            .filter_map(|entry| match entry {
                Entry::External {
                    segment, ranges, ..
                } => Some((*segment, ranges)),
                Entry::Own { .. } => None,
            })
            .collect::<Vec<_>>();
        segments.sort_by_key(|(segment, _)| *segment);
        let mut out = Vec::new();
        for (segment, ranges) in segments {
            for &(a, b) in ranges.iter() {
                if quantity == 0 {
                    return out;
                }
                let n = quantity.min(b - a);
                out.push(Slice {
                    segment,
                    start: a,
                    end: a + n,
                });
                quantity -= n;
            }
        }
        debug_assert_eq!(quantity, 0);
        out
    }
    #[cfg(test)]
    pub(crate) fn external_ranges<'a>(
        &'a self,
        condition: &str,
        buy_token: &str,
    ) -> impl Iterator<Item = (&'a Level, u64, u128, u128, u128)> + use<'a> {
        let first = Level {
            condition: condition.into(),
            buy_token: buy_token.into(),
            price_micro: 0,
        };
        let last = Level {
            price_micro: 1_000_000,
            ..first.clone()
        };
        self.levels
            .range(first..=last)
            .rev()
            .flat_map(|(level, queue)| {
                // Segment IDs need not follow insertion order for this reader.
                // Sort only borrowed segment references, never flattened fragments.
                let mut segments = queue
                    .iter()
                    .filter_map(|entry| match entry {
                        Entry::External {
                            segment,
                            total,
                            ranges,
                        } => Some((*segment, *total, ranges)),
                        Entry::Own { .. } => None,
                    })
                    .collect::<Vec<_>>();
                segments.sort_by_key(|(segment, _, _)| *segment);
                segments
                    .into_iter()
                    .flat_map(move |(segment, total, ranges)| {
                        ranges
                            .iter()
                            .map(move |(a, b)| (level, segment, total, *a, *b))
                    })
            })
    }
    pub(crate) fn consume_targets(&mut self, level: &Level, targets: &[Slice]) {
        self.apply(&Event {
            at_ms: 0,
            source_sequence: 0,
            level: level.clone(),
            kind: EventKind::Cancel {
                targets: targets.to_vec(),
                recovery: false,
            },
        });
    }
    fn demand_levels(&self, level: &Level) -> impl DoubleEndedIterator<Item = &Level> {
        self.levels
            .range(
                level.clone()..=Level {
                    price_micro: 1_000_000,
                    ..level.clone()
                },
            )
            .map(|(l, _)| l)
    }
    fn demand_external_quantity(&self, level: &Level) -> u128 {
        self.demand_levels(level)
            .map(|l| self.external_quantity(l))
            .sum()
    }
    pub(crate) fn discard_condition(&mut self, condition: &str) {
        self.levels.retain(|level, _| level.condition != condition);
    }
    pub fn external_quantity(&self, l: &Level) -> u128 {
        self.levels
            .get(l)
            .into_iter()
            .flatten()
            .map(|e| match e {
                Entry::External { ranges, .. } => ranges.live_quantity(),
                _ => 0,
            })
            .sum()
    }
    pub fn insert(&mut self, l: Level, order: String, quantity: u128) {
        self.levels
            .entry(l)
            .or_default()
            .push_back(Entry::Own { order, quantity });
    }
    pub(crate) fn venue_dust_candidate(&self, level: &Level) -> Option<&str> {
        let q = self.levels.get(level)?;
        if q.len() != 1 {
            return None;
        }
        match q.front()? {
            Entry::Own { order, quantity } if *quantity > 0 => Some(order),
            _ => None,
        }
    }
    pub(crate) fn cancel_at(&mut self, level: &Level, order: &str) -> u128 {
        let mut removed = 0;
        if let Some(q) = self.levels.get_mut(level) {
            q.retain(|e| {
                if let Entry::Own {
                    order: id,
                    quantity,
                } = e
                {
                    if id == order {
                        removed += quantity;
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            });
        }
        removed
    }
    #[cfg(test)]
    pub fn cancel(&mut self, order: &str) -> u128 {
        let mut removed = 0;
        for q in self.levels.values_mut() {
            q.retain(|e| {
                if let Entry::Own {
                    order: id,
                    quantity,
                } = e
                {
                    if id == order {
                        removed += quantity;
                        return false;
                    }
                }
                true
            });
        }
        removed
    }
    pub(crate) fn apply(&mut self, e: &Event) -> Vec<Fill> {
        match &e.kind {
            EventKind::Add {
                segment, quantity, ..
            } => {
                if *quantity == 0 {
                    return Vec::new();
                }
                self.levels
                    .entry(e.level.clone())
                    .or_default()
                    .push_back(Entry::External {
                        segment: *segment,
                        total: *quantity,
                        ranges: LiveRanges::from_vec(vec![(0, *quantity)]),
                    });
                Vec::new()
            }
            EventKind::Cancel { targets, .. } => {
                let mut by_segment = BTreeMap::<u64, Vec<(u128, u128)>>::new();
                for t in targets {
                    if t.start == t.end {
                        continue;
                    }
                    by_segment
                        .entry(t.segment)
                        .or_default()
                        .push((t.start, t.end));
                }
                if let Some(q) = self.levels.get_mut(&e.level) {
                    for entry in q.iter_mut() {
                        if let Entry::External {
                            segment, ranges, ..
                        } = entry
                        {
                            if let Some(cuts) = by_segment.get_mut(segment) {
                                ranges.subtract(cuts);
                            }
                        }
                    }
                    q.retain(|e| match e {
                        Entry::External { ranges, .. } => !ranges.is_empty(),
                        Entry::Own { quantity, .. } => *quantity > 0,
                    });
                }
                Vec::new()
            }
            EventKind::Demand { quantity } => {
                let mut budget = *quantity;
                let mut fills = Vec::new();
                let levels: Vec<_> = self.demand_levels(&e.level).cloned().collect();
                for l in levels.into_iter().rev() {
                    let (used, new) = self.consume(&l, budget, false);
                    budget -= used;
                    fills.extend(new);
                    if budget == 0 {
                        break;
                    }
                }
                fills
            }
        }
    }
    // 主动单只消费外部区间；Maker需求与其共用同一份区间，不恢复历史深度。
    #[cfg(test)]
    pub fn take(&mut self, l: &Level, quantity: u128) -> u128 {
        self.consume(l, quantity, true).0
    }
    fn consume(&mut self, l: &Level, mut budget: u128, external_only: bool) -> (u128, Vec<Fill>) {
        let requested = budget;
        let mut fills = Vec::new();
        if let Some(q) = self.levels.get_mut(l) {
            for e in q.iter_mut() {
                if budget == 0 {
                    break;
                }
                match e {
                    Entry::External { ranges, .. } => {
                        budget -= ranges.consume_prefix(budget);
                    }
                    Entry::Own { order, quantity } if !external_only => {
                        let n = budget.min(*quantity);
                        *quantity -= n;
                        budget -= n;
                        if n > 0 {
                            fills.push(Fill {
                                order: order.clone(),
                                quantity: n,
                                level: l.clone(),
                            })
                        }
                    }
                    _ => {}
                }
            }
            q.retain(|e| match e {
                Entry::External { ranges, .. } => !ranges.is_empty(),
                Entry::Own { quantity, .. } => *quantity > 0,
            });
        }
        (requested - budget, fills)
    }
    fn select_cancellation(
        &self,
        l: &Level,
        mut budget: u128,
        model: Cancellation,
        seed: u64,
        event: u64,
    ) -> Vec<Slice> {
        if model == Cancellation::SegmentProportionalV2 {
            let total = self.external_quantity(l);
            if total == 0 {
                return Vec::new();
            }
            let target = budget.min(total);
            let phase =
                (((random(seed, event, 0) as u128) << 64) | random(seed, event, 1) as u128) % total;
            let endpoint = |x: u128| {
                ((BigUint::from(x) * BigUint::from(target) + BigUint::from(phase))
                    / BigUint::from(total))
                .to_u128()
                .expect("取消份额不超过总量")
            };
            let mut cumulative = 0;
            let mut targets = Vec::new();
            for entry in self.levels.get(l).into_iter().flatten() {
                let Entry::External {
                    segment, ranges, ..
                } = entry
                else {
                    continue;
                };
                let live = ranges.live_quantity();
                let n = endpoint(cumulative + live) - endpoint(cumulative);
                cumulative += live;
                if n == 0 {
                    continue;
                }
                let offset = (((random(seed, event, *segment) as u128) << 64)
                    | random(seed, event, segment.wrapping_add(1)) as u128)
                    % live;
                let first = n.min(live - offset);
                // 在存活数量的排名坐标上只抽一个环形窗口，再映射回原外部段坐标。
                for (start, end) in [(offset, offset + first), (0, n - first)] {
                    if start == end {
                        continue;
                    }
                    ranges.rank_targets(*segment, start, end, &mut targets);
                }
            }
            return targets;
        }
        let mut intervals: Vec<_> = self
            .levels
            .get(l)
            .into_iter()
            .flatten()
            .flat_map(|e| match e {
                Entry::External {
                    segment, ranges, ..
                } => ranges
                    .iter()
                    .map(|(a, b)| Slice {
                        segment: *segment,
                        start: *a,
                        end: *b,
                    })
                    .collect(),
                _ => Vec::new(),
            })
            .collect();
        match model {
            Cancellation::SegmentProportionalV2 => unreachable!("整段规则已处理"),
            Cancellation::Front => {}
            Cancellation::Back => intervals.reverse(),
            Cancellation::Concentrated => intervals.sort_by_key(|s| random(seed, event, s.segment)),
            Cancellation::Proportional => {
                let total: u128 = intervals.iter().map(|s| s.end - s.start).sum();
                if total == 0 {
                    return Vec::new();
                }
                let target = budget.min(total);
                let phase = (((random(seed, event, 0) as u128) << 64)
                    | random(seed, event, 1) as u128)
                    % total;
                let endpoint = |x: u128| {
                    ((BigUint::from(x) * BigUint::from(target) + BigUint::from(phase))
                        / BigUint::from(total))
                    .to_u128()
                    .expect("取消份额不超过总量")
                };
                let mut cumulative = 0;
                let mut targets = Vec::new();
                for s in intervals {
                    let len = s.end - s.start;
                    let n = endpoint(cumulative + len) - endpoint(cumulative);
                    cumulative += len;
                    if n == 0 {
                        continue;
                    }
                    let offset = (((random(seed, event, s.segment) as u128) << 64)
                        | random(seed, event, s.segment.wrapping_add(1)) as u128)
                        % len;
                    let start = s.start + offset;
                    let first = n.min(s.end - start);
                    targets.push(Slice {
                        segment: s.segment,
                        start,
                        end: start + first,
                    });
                    if first < n {
                        targets.push(Slice {
                            segment: s.segment,
                            start: s.start,
                            end: s.start + n - first,
                        });
                    }
                }
                return targets;
            }
        }
        let mut targets = Vec::new();
        for s in intervals {
            if budget == 0 {
                break;
            }
            let n = budget.min(s.end - s.start);
            budget -= n;
            targets.push(if model == Cancellation::Back {
                Slice {
                    start: s.end - n,
                    ..s
                }
            } else {
                Slice {
                    end: s.start + n,
                    ..s
                }
            });
        }
        targets
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// 一个账户持有一个游标；场景可由多个候选共享，队列和账户不可共享。
    #[derive(Debug)]
    pub struct SceneExecution {
        scene: std::sync::Arc<PreparedScene>,
        next: usize,
        pub(crate) queue: SharedQueue,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct TimedFill {
        pub at_ms: i64,
        pub fill: Fill,
    }
    impl SceneExecution {
        pub fn new(scene: std::sync::Arc<PreparedScene>) -> Self {
            Self {
                scene,
                next: 0,
                queue: SharedQueue::default(),
            }
        }
        pub fn advance_through(&mut self, at_ms: i64) -> Vec<TimedFill> {
            let mut fills = Vec::new();
            while let Some(e) = self
                .scene
                .events
                .get(self.next)
                .filter(|e| e.at_ms <= at_ms)
            {
                fills.extend(self.queue.apply(e).into_iter().map(|fill| TimedFill {
                    at_ms: e.at_ms,
                    fill,
                }));
                self.next += 1;
            }
            fills
        }
        pub fn insert(&mut self, level: Level, order: String, quantity: u128) {
            self.queue.insert(level, order, quantity)
        }
        pub fn cancel(&mut self, order: &str) -> u128 {
            self.queue.cancel(order)
        }
    }

    fn l() -> Level {
        Level {
            condition: "c".into(),
            buy_token: "yes".into(),
            price_micro: 400_000,
        }
    }
    fn e(kind: EventKind) -> Event {
        Event {
            at_ms: 0,
            source_sequence: 0,
            level: l(),
            kind,
        }
    }
    fn add(q: &mut SharedQueue, id: u64, n: u128) {
        q.apply(&e(EventKind::Add {
            segment: id,
            quantity: n,
            implicit: false,
        }));
    }
    #[test]
    fn back_partial_cut_uses_tail_and_does_not_retarget_a_consumed_prefix() {
        let mut reference = SharedQueue::default();
        add(&mut reference, 1, 10);
        let front = reference.select_cancellation(&l(), 4, Cancellation::Front, 7, 0);
        let back = reference.select_cancellation(&l(), 4, Cancellation::Back, 7, 0);
        assert_eq!(
            front,
            vec![Slice {
                segment: 1,
                start: 0,
                end: 4
            }]
        );
        assert_eq!(
            back,
            vec![Slice {
                segment: 1,
                start: 6,
                end: 10
            }]
        );
        let mut actual = SharedQueue::default();
        add(&mut actual, 1, 10);
        actual.insert(l(), "own".into(), 3);
        let taker = actual.taker_targets(&l(), 4);
        actual.consume_targets(&l(), &taker);
        actual.apply(&e(EventKind::Cancel {
            targets: back,
            recovery: false,
        }));
        // 10 - 4 Taker - 4 fixed tail = 2 external; demand 5 fills Own 3.
        let fills = actual.apply(&e(EventKind::Demand { quantity: 5 }));
        assert_eq!(fills.iter().map(|f| f.quantity).sum::<u128>(), 3);
    }

    #[test]
    fn shared_demand_and_split_order_invariance() {
        let mut a = SharedQueue::default();
        add(&mut a, 1, 5);
        a.insert(l(), "a".into(), 10);
        a.insert(l(), "b".into(), 10);
        let mut b = SharedQueue::default();
        add(&mut b, 1, 5);
        b.insert(l(), "single".into(), 20);
        let f = a.apply(&e(EventKind::Demand { quantity: 20 }));
        assert_eq!(f.iter().map(|x| x.quantity).collect::<Vec<_>>(), [10, 5]);
        assert_eq!(
            b.apply(&e(EventKind::Demand { quantity: 20 }))[0].quantity,
            15
        );
        assert_eq!(a.cancel("b"), 5);
        assert!(a.apply(&e(EventKind::Demand { quantity: 100 })).is_empty());
    }
    #[test]
    fn condition_preplanning_preserves_every_event_and_global_segment_identity() {
        let mut facts = Vec::new();
        for n in 0..480 {
            let level = Level {
                condition: format!("condition-{}", n % 12),
                buy_token: if n % 24 < 12 { "yes" } else { "no" }.into(),
                price_micro: 300_000 + (n % 5) * 10_000,
            };
            let kind = match n % 9 {
                0 => FactKind::Initial(0),
                1 => FactKind::Add(40),
                2 => FactKind::Trade(23),
                3 => FactKind::Reduce(35),
                4 => FactKind::Recovery {
                    before: 100,
                    after: 80,
                },
                5 => FactKind::Trade(0),
                6 => FactKind::Recovery {
                    before: 0,
                    after: 20,
                },
                7 => FactKind::Initial(15),
                _ => FactKind::Trade(30),
            };
            facts.push(Fact {
                sequence: u64::from(n),
                received_at_ms: i64::from(n) * 10,
                source_at_ms: Some(i64::from(n / 24) * 5 + i64::from(n % 7)),
                level,
                kind,
            });
        }
        let analysis = Analysis::new(facts);
        for internal_time in [InternalTime::Received, InternalTime::SourceWhenPresent] {
            for allocation in [Allocation::Nearest, Allocation::DistanceWeighted] {
                for cancellation in [
                    Cancellation::Front,
                    Cancellation::Back,
                    Cancellation::Concentrated,
                    Cancellation::Proportional,
                    Cancellation::SegmentProportionalV2,
                ] {
                    let p = Parameters {
                        association_ms: 250,
                        allocation,
                        cancellation,
                        internal_time,
                        seed: 7,
                        association_trade_shift_ms: 0,
                    };
                    let serial = analysis.prepare(p.clone());
                    let plan = analysis.condition_plan(p);
                    let results = std::thread::scope(|scope| {
                        let plan = &plan;
                        (0..plan.condition_count())
                            .map(|index| {
                                scope.spawn(move || {
                                    let mut events = Vec::new();
                                    let scene = plan
                                        .prepare_condition(
                                            index,
                                            |_, _, _| {},
                                            |rank, event| {
                                                events.push((rank, event));
                                                Ok::<_, ()>(())
                                            },
                                        )
                                        .unwrap();
                                    (events, scene.report)
                                })
                            })
                            .collect::<Vec<_>>()
                            .into_iter()
                            .map(|h| h.join().unwrap())
                            .collect::<Vec<_>>()
                    });
                    let mut events = Vec::new();
                    let mut reports = Vec::new();
                    for (stream, report) in results {
                        events.extend(stream);
                        reports.push(report);
                    }
                    events.sort_by_key(|(rank, _)| *rank);
                    assert_eq!(
                        serial.events,
                        events
                            .into_iter()
                            .map(|(_, event)| event)
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(serial.report, plan.combine_reports(reports));
                }
            }
        }
    }
    #[test]
    fn taker_borrowed_segments_keep_the_old_flattened_sort_and_batch_difference() {
        let mut q = SharedQueue::default();
        let mut higher = l();
        higher.price_micro += 1;
        q.apply(&Event {
            level: higher.clone(),
            ..e(EventKind::Add {
                segment: 8,
                quantity: 100_000,
                implicit: false,
            })
        });
        add(&mut q, 10, 30_000);
        q.insert(l(), "own".into(), 100);
        add(&mut q, 2, 20);
        let ranges = LiveRanges::from_vec((0..15_000).map(|i| (i * 2, i * 2 + 1)).collect());
        if let Entry::External { ranges: stored, .. } =
            q.levels.get_mut(&l()).unwrap().front_mut().unwrap()
        {
            *stored = ranges;
        }
        let mut expected = Vec::new();
        for (level, queue) in &q.levels {
            for entry in queue {
                if let Entry::External {
                    segment,
                    total,
                    ranges,
                } = entry
                {
                    for &(a, b) in ranges.iter() {
                        expected.push((level.clone(), *segment, *total, a, b));
                    }
                }
            }
        }
        expected
            .sort_by_key(|(l, segment, _, a, _)| (std::cmp::Reverse(l.price_micro), *segment, *a));
        let actual = q
            .external_ranges("c", "yes")
            .map(|(l, s, t, a, b)| (l.clone(), s, t, a, b))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(q.external_ranges("c", "yes").take(1).count(), 1);
        let targets = vec![
            Slice {
                segment: 10,
                start: 1,
                end: 100,
            },
            Slice {
                segment: 10,
                start: 101,
                end: 1234,
            },
            Slice {
                segment: 2,
                start: 1,
                end: 10,
            },
        ];
        let mut single = q.clone();
        for target in &targets {
            single.consume_targets(&l(), std::slice::from_ref(target));
        }
        q.consume_targets(&l(), &targets);
        assert_eq!(q, single);
        assert_eq!(q.cancel("own"), 100);
    }
    #[test]
    fn zero_scene_changes_leave_quantity_and_interval_boundaries_unchanged() {
        let mut queue = SharedQueue::default();
        add(&mut queue, 0, 10);
        let before = queue.clone();
        queue.apply(&e(EventKind::Add {
            segment: 1,
            quantity: 0,
            implicit: false,
        }));
        queue.apply(&e(EventKind::Demand { quantity: 0 }));
        queue.apply(&e(EventKind::Cancel {
            targets: vec![Slice {
                segment: 0,
                start: 5,
                end: 5,
            }],
            recovery: false,
        }));
        assert_eq!(queue, before);
    }
    #[test]
    fn scene_quantity_conversion_uses_execution_precision_without_decimal_multiply_overflow() {
        let largest = rust_decimal::Decimal::MAX;
        assert_eq!(
            scene_micro_units(largest).unwrap(),
            u128::try_from(largest.mantissa()).unwrap() * 1_000_000
        );
        assert_eq!(
            scene_micro_units("1.1234567".parse().unwrap()).unwrap(),
            1_123_456
        );
    }
    #[test]
    fn implicit_supply_uses_the_same_price_priority_budget_as_demand() {
        let low = l();
        let mut high = low.clone();
        high.price_micro += 1;
        for (available, missing) in [(20, 0), (7, 3)] {
            let analysis = Analysis::new(vec![
                Fact {
                    sequence: 0,
                    received_at_ms: 0,
                    source_at_ms: None,
                    level: high.clone(),
                    kind: FactKind::Initial(available),
                },
                Fact {
                    sequence: 1,
                    received_at_ms: 1,
                    source_at_ms: None,
                    level: low.clone(),
                    kind: FactKind::Trade(10),
                },
            ]);
            let scene = analysis.prepare(Parameters {
                association_ms: 0,
                allocation: Allocation::Nearest,
                cancellation: Cancellation::SegmentProportionalV2,
                internal_time: InternalTime::Received,
                seed: 7,
                association_trade_shift_ms: 0,
            });
            assert_eq!(scene.report.implicit_quantity, missing);
            let mut q = SharedQueue::default();
            for event in &scene.events {
                q.apply(event);
            }
            assert_eq!(q.external_quantity(&high), available.saturating_sub(10));
            assert_eq!(q.external_quantity(&low), 0);
        }
    }
    #[test]
    fn streaming_sink_preserves_global_internal_order_and_segment_ids() {
        let mut other = l();
        other.condition = "other".into();
        let facts = vec![
            Fact {
                sequence: 0,
                received_at_ms: 100,
                source_at_ms: Some(20),
                level: l(),
                kind: FactKind::Initial(100),
            },
            Fact {
                sequence: 1,
                received_at_ms: 101,
                source_at_ms: Some(30),
                level: other,
                kind: FactKind::Add(20),
            },
            Fact {
                sequence: 2,
                received_at_ms: 102,
                source_at_ms: Some(10),
                level: l(),
                kind: FactKind::Trade(10),
            },
            Fact {
                sequence: 3,
                received_at_ms: 103,
                source_at_ms: Some(40),
                level: l(),
                kind: FactKind::Reduce(20),
            },
        ];
        let p = Parameters {
            association_ms: 0,
            allocation: Allocation::Nearest,
            cancellation: Cancellation::SegmentProportionalV2,
            internal_time: InternalTime::SourceWhenPresent,
            seed: 7,
            association_trade_shift_ms: 0,
        };
        let analysis = Analysis::new(facts);
        let old = analysis.prepare(p.clone());
        let mut events = Vec::new();
        let streamed = analysis
            .prepare_with_sink(
                p,
                |_, _, _| {},
                |e| {
                    events.push(e);
                    Ok::<_, ()>(())
                },
            )
            .unwrap();
        assert_eq!(old.events, events);
        assert_eq!(old.report, streamed.report);
        assert_eq!(
            events.iter().map(|e| e.source_sequence).collect::<Vec<_>>(),
            vec![2, 2, 0, 1, 3]
        );
        assert!(matches!(
            events[0].kind,
            EventKind::Add {
                segment: 1,
                implicit: true,
                ..
            }
        ));
        assert!(matches!(events[2].kind, EventKind::Add { segment: 2, .. }));
        assert!(matches!(events[3].kind, EventKind::Add { segment: 3, .. }));
        assert!(streamed.events.is_empty());
    }
    #[test]
    #[ignore = "explicit existing frozen real-scene equivalence input via OFFLINE_SCENE_EQUIVALENCE_INPUT"]
    fn frozen_real_segment_targets_match_storage_blocks() {
        #[derive(Deserialize)]
        struct Frozen {
            parameters: Parameters,
            levels: Vec<Level>,
            events: Vec<(i64, u64, usize, EventKind)>,
        }
        let path = std::env::var("OFFLINE_SCENE_EQUIVALENCE_INPUT")
            .expect("explicit real artifact required");
        let scene: Frozen =
            serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(path).unwrap()))
                .unwrap();
        let mut reference = SharedQueue::default();
        let start = std::time::Instant::now();
        for (i, (at_ms, source_sequence, level, kind)) in scene.events.into_iter().enumerate() {
            let event = Event {
                at_ms,
                source_sequence,
                level: scene.levels[level].clone(),
                kind,
            };
            if let EventKind::Cancel { targets, .. } = &event.kind {
                let quantity = targets.iter().map(|s| s.end - s.start).sum();
                let actual = reference.select_cancellation(
                    &event.level,
                    quantity,
                    scene.parameters.cancellation,
                    scene.parameters.seed,
                    event.source_sequence,
                );
                assert_eq!(targets, &actual, "event {i}");
            }
            reference.apply(&event);
            if i % 4096 == 0 {
                eprintln!(
                    "frozen target equivalence: event={i}, elapsed_seconds={}",
                    start.elapsed().as_secs_f64()
                );
            }
        }
    }
    #[test]
    fn cancellation_does_not_retarget_consumed_ranges() {
        let mut q = SharedQueue::default();
        add(&mut q, 1, 60);
        add(&mut q, 2, 40);
        let cancel = e(EventKind::Cancel {
            targets: vec![Slice {
                segment: 1,
                start: 0,
                end: 60,
            }],
            recovery: false,
        });
        assert_eq!(q.take(&l(), 40), 40);
        q.apply(&cancel);
        assert_eq!(q.external_quantity(&l()), 40);
        assert_eq!(q.take(&l(), 40), 40);
        assert_eq!(q.take(&l(), 40), 0);
    }
    #[test]
    fn own_intervention_keeps_reference_external_quantity() {
        let mut q = SharedQueue::default();
        q.insert(l(), "own".into(), 30);
        add(&mut q, 1, 70);
        assert_eq!(
            q.apply(&e(EventKind::Demand { quantity: 30 }))[0].quantity,
            30
        );
        assert_eq!(q.external_quantity(&l()), 70);
        q.apply(&e(EventKind::Cancel {
            targets: vec![Slice {
                segment: 1,
                start: 30,
                end: 70,
            }],
            recovery: true,
        }));
        assert_eq!(q.external_quantity(&l()), 30);
    }
    #[test]
    fn association_has_global_capacity_and_trade_is_not_spread() {
        let facts = [
            (100, FactKind::Initial(100)),
            (110, FactKind::Reduce(60)),
            (120, FactKind::Reduce(40)),
            (150, FactKind::Trade(30)),
            (151, FactKind::Trade(30)),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (t, k))| Fact {
            sequence: i as u64,
            received_at_ms: t,
            source_at_ms: None,
            level: l(),
            kind: k,
        })
        .collect();
        let p = Parameters {
            association_ms: 100,
            allocation: Allocation::Nearest,
            cancellation: Cancellation::Front,
            internal_time: InternalTime::Received,
            seed: 7,
            association_trade_shift_ms: 0,
        };
        let analysis = Analysis::new(facts);
        let s = analysis.prepare(p.clone());
        assert_eq!(s.report.associated_quantity, 60);
        assert_eq!(s.report.cancellation_quantity, 40);
        assert_eq!(
            s.events
                .iter()
                .filter(|x| matches!(x.kind, EventKind::Demand { .. }))
                .count(),
            2
        );
        assert_eq!(s.events, analysis.prepare(p).events);
        let mut q = SharedQueue::default();
        for e in &s.events {
            q.apply(e);
        }
        assert_eq!(q.external_quantity(&l()), 0);
    }

    #[test]
    fn proportional_targets_conserve_volume_and_ignore_own_ids() {
        for seed in 0..32 {
            let mut q = SharedQueue::default();
            add(&mut q, 1, 20);
            q.insert(l(), "a".into(), 100);
            add(&mut q, 2, 80);
            let targets = q.select_cancellation(&l(), 35, Cancellation::Proportional, seed, 3);
            assert_eq!(targets.iter().map(|s| s.end - s.start).sum::<u128>(), 35);
            q.apply(&e(EventKind::Cancel {
                targets,
                recovery: false,
            }));
            assert_eq!(q.external_quantity(&l()), 65);
            assert_eq!(q.cancel("a"), 100);
        }
    }
    #[test]
    fn cursor_never_reissues_demand_and_cancel_race_keeps_actual_time() {
        let scene = std::sync::Arc::new(PreparedScene {
            model_version: MODEL_VERSION.into(),
            parameters: Parameters {
                association_ms: 50,
                allocation: Allocation::Nearest,
                cancellation: Cancellation::Front,
                internal_time: InternalTime::Received,
                seed: 1,
                association_trade_shift_ms: 0,
            },
            report: PreparationReport::default(),
            events: vec![Event {
                at_ms: 10,
                ..e(EventKind::Demand { quantity: 15 })
            }],
        });
        let mut a = SceneExecution::new(scene.clone());
        a.insert(l(), "a".into(), 20);
        assert!(a.advance_through(9).is_empty());
        let fills = a.advance_through(10);
        assert_eq!(fills[0].fill.quantity, 15);
        assert_eq!(fills[0].at_ms, 10);
        assert!(a.advance_through(100).is_empty());
        assert_eq!(a.cancel("a"), 5);
        let mut b = SceneExecution::new(scene);
        b.insert(l(), "b".into(), 20);
        assert_eq!(b.cancel("b"), 20);
        assert!(b.advance_through(10).is_empty());
    }

    #[test]
    fn canceled_external_segments_do_not_accumulate_or_retarget() {
        let mut queue = SharedQueue::default();
        for id in 0..20_000 {
            add(&mut queue, id, 100);
            queue.apply(&e(EventKind::Cancel {
                targets: vec![Slice {
                    segment: id,
                    start: 0,
                    end: 100,
                }],
                recovery: false,
            }));
            assert!(queue.levels[&l()].is_empty());
        }
        add(&mut queue, 30_000, 80);
        queue.apply(&e(EventKind::Cancel {
            targets: vec![Slice {
                segment: 1,
                start: 0,
                end: 100,
            }],
            recovery: false,
        }));
        assert_eq!(queue.external_quantity(&l()), 80);
    }

    #[test]
    fn batch_cancellation_is_exact_interval_difference() {
        let mut ranges = vec![(0, 10), (20, 30), (40, 50)];
        remove_ranges(&mut ranges, &mut [(23, 27), (2, 8), (7, 22), (26, 44)]);
        assert_eq!(ranges, vec![(0, 2), (22, 23), (44, 50)]);
        let mut many = (0..20_000).map(|i| (i * 3, i * 3 + 2)).collect::<Vec<_>>();
        let mut cuts = (0..20_000)
            .map(|i| (i * 3 + 1, i * 3 + 2))
            .collect::<Vec<_>>();
        remove_ranges(&mut many, &mut cuts);
        assert_eq!(many.len(), 20_000);
        assert!(many.iter().all(|(a, b)| b - a == 1));
    }

    #[test]
    fn batched_cancel_preserves_legacy_fragment_sampler_exactly() {
        let mut new = SharedQueue::default();
        let mut old = SharedQueue::default();
        for id in 0..96u64 {
            if id % 8 == 0 {
                add(&mut new, id, 100);
                add(&mut old, id, 100);
            }
            let targets = old.select_cancellation(&l(), 7, Cancellation::Proportional, 13, id);
            new.apply(&e(EventKind::Cancel {
                targets: targets.clone(),
                recovery: false,
            }));
            for entry in old.levels.get_mut(&l()).unwrap().iter_mut() {
                if let Entry::External {
                    segment, ranges, ..
                } = entry
                {
                    for t in targets.iter().filter(|t| t.segment == *segment) {
                        let mut next = Vec::new();
                        for &(a, b) in ranges.iter() {
                            if a < t.start {
                                let end = b.min(t.start);
                                if a < end {
                                    next.push((a, end));
                                }
                            }
                            if b > t.end {
                                let start = a.max(t.end);
                                if start < b {
                                    next.push((start, b));
                                }
                            }
                        }
                        *ranges = LiveRanges::from_vec(next);
                    }
                }
            }
            old.levels.get_mut(&l()).unwrap().retain(|e| match e {
                Entry::External { ranges, .. } => !ranges.is_empty(),
                Entry::Own { quantity, .. } => *quantity > 0,
            });
            assert_eq!(new, old, "event {id}");
        }
    }

    #[test]
    fn segment_sampler_changes_joint_distribution_and_keeps_exact_total() {
        let mut q = SharedQueue::default();
        add(&mut q, 1, 12);
        q.apply(&e(EventKind::Cancel {
            targets: vec![Slice {
                segment: 1,
                start: 4,
                end: 8,
            }],
            recovery: false,
        }));
        let mut saw_whole_fragment = false;
        for seed in 0..64 {
            let old = q.select_cancellation(&l(), 4, Cancellation::Proportional, seed, 3);
            let new = q.select_cancellation(&l(), 4, Cancellation::SegmentProportionalV2, seed, 3);
            let in_first = |targets: &[Slice]| {
                targets
                    .iter()
                    .map(|s| s.end.min(4).saturating_sub(s.start))
                    .sum::<u128>()
            };
            assert_eq!(in_first(&old), 2);
            assert_eq!(new.iter().map(|s| s.end - s.start).sum::<u128>(), 4);
            if [0, 4].contains(&in_first(&new)) {
                saw_whole_fragment = true;
            }
            let mut actual = q.clone();
            actual.apply(&e(EventKind::Cancel {
                targets: new.clone(),
                recovery: false,
            }));
            assert_eq!(actual.external_quantity(&l()), 4);
            actual.apply(&e(EventKind::Cancel {
                targets: new,
                recovery: false,
            }));
            assert_eq!(actual.external_quantity(&l()), 4, "同一目标不转扣");
        }
        assert!(
            saw_whole_fragment,
            "整段环形切口与旧每碎片固定份额的联合分布不同"
        );
    }
    #[test]
    fn segment_sampler_has_two_cut_edge_bound_and_is_independent_of_own_splitting() {
        let mut q = SharedQueue::default();
        add(&mut q, 1, 1_000_000);
        for event in 0..1000 {
            let before = q.levels[&l()]
                .iter()
                .map(|e| match e {
                    Entry::External { ranges, .. } => ranges.len(),
                    _ => 0,
                })
                .sum::<usize>();
            let targets =
                q.select_cancellation(&l(), 31, Cancellation::SegmentProportionalV2, 7, event);
            assert_eq!(targets.iter().map(|s| s.end - s.start).sum::<u128>(), 31);
            q.apply(&e(EventKind::Cancel {
                targets,
                recovery: false,
            }));
            let after = q.levels[&l()]
                .iter()
                .map(|e| match e {
                    Entry::External { ranges, .. } => ranges.len(),
                    _ => 0,
                })
                .sum::<usize>();
            assert!(after <= before + 2);
            assert!(after <= 1 + 2 * (event as usize + 1));
        }
        assert_eq!(q.external_quantity(&l()), 969_000);
        let mut single = SharedQueue::default();
        let mut split = SharedQueue::default();
        add(&mut single, 1, 5);
        add(&mut split, 1, 5);
        single.insert(l(), "one".into(), 20);
        split.insert(l(), "a".into(), 10);
        split.insert(l(), "b".into(), 10);
        add(&mut single, 2, 40);
        add(&mut split, 2, 40);
        let a = single.select_cancellation(&l(), 17, Cancellation::SegmentProportionalV2, 7, 9);
        let b = split.select_cancellation(&l(), 17, Cancellation::SegmentProportionalV2, 7, 9);
        assert_eq!(a, b);
        assert_eq!(a.iter().map(|s| s.end - s.start).sum::<u128>(), 17);
        assert!(
            single
                .apply(&e(EventKind::Cancel {
                    targets: a,
                    recovery: false
                }))
                .is_empty()
        );
        assert!(
            split
                .apply(&e(EventKind::Cancel {
                    targets: b,
                    recovery: false
                }))
                .is_empty()
        );
        let f1 = single.apply(&e(EventKind::Demand { quantity: 20 }));
        let f2 = split.apply(&e(EventKind::Demand { quantity: 20 }));
        assert_eq!(
            f1.iter().map(|f| f.quantity).sum::<u128>(),
            f2.iter().map(|f| f.quantity).sum::<u128>()
        );
    }

    #[test]
    fn range_storage_blocks_preserve_rank_targets_and_cached_live_quantity() {
        let original = (0..25_000u128)
            .map(|i| (i * 3, i * 3 + 2))
            .collect::<Vec<_>>();
        let mut blocked = LiveRanges::from_vec(original.clone());
        for (start, end) in [(0, 1), (8191, 8200), (9000, 25_000), (49_999, 50_000)] {
            let mut expected = Vec::new();
            let mut rank = 0;
            for &(a, b) in &original {
                let lo = start.max(rank);
                let hi = end.min(rank + b - a);
                if lo < hi {
                    expected.push(Slice {
                        segment: 9,
                        start: a + lo - rank,
                        end: a + hi - rank,
                    });
                }
                rank += b - a;
            }
            let mut got = Vec::new();
            blocked.rank_targets(9, start, end, &mut got);
            assert_eq!(got, expected);
        }
        let cuts = vec![
            (9, 31),
            (24_001, 49_999),
            (60_000, 61_000),
            (24_000, 24_009),
        ];
        let mut expected = original;
        remove_ranges(&mut expected, &mut cuts.clone());
        blocked.subtract(&mut cuts.clone());
        assert_eq!(blocked.iter().copied().collect::<Vec<_>>(), expected);
        assert_eq!(
            blocked.live_quantity(),
            expected.iter().map(|(a, b)| b - a).sum()
        );
        let consumed = blocked.consume_prefix(10_111);
        let mut budget = 10_111;
        for (a, b) in &mut expected {
            let n = budget.min(*b - *a);
            *a += n;
            budget -= n;
        }
        expected.retain(|(a, b)| a < b);
        assert_eq!(consumed, 10_111 - budget);
        assert_eq!(blocked.iter().copied().collect::<Vec<_>>(), expected);
        assert_eq!(
            blocked.live_quantity(),
            expected.iter().map(|(a, b)| b - a).sum()
        );
        let encoded = serde_json::to_vec(&blocked).unwrap();
        assert_eq!(encoded, serde_json::to_vec(&expected).unwrap());
        assert_eq!(
            serde_json::from_slice::<LiveRanges>(&encoded).unwrap(),
            blocked
        );
    }
}
