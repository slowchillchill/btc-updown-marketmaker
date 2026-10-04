//! 仅测试构建的有限重放诊断；默认关闭，不提供生产接口。
use super::*;
use std::{
    cell::{Cell, RefCell},
    time::{Duration, Instant},
};
const PRIMARY: [&str; 6] = [
    "main_drain",
    "identity",
    "dependencies",
    "prepare",
    "core_push",
    "into_owned",
];
const RETAIN_SCANS: usize = 128;
const RETAIN_SLOW: usize = 16;
thread_local! { static STATE: RefCell<Option<Report>> = const { RefCell::new(None) }; }
thread_local! { static DECODE_ROLE: Cell<Option<&'static str>> = const { Cell::new(None) }; }
#[derive(Default, Serialize)]
struct DecodeCounts {
    metadata_loads: u64,
    metadata_load_ns: u128,
    row_group_open_ns: u128,
    successful_batch_decode_and_rows_ns: u128,
    max_parquet_metadata_estimate_bytes: usize,
    row_groups_opened: u64,
    batches: u64,
    rows: u64,
    max_sort_row_resident_bytes: usize,
    max_arrow_batch_bytes: usize,
}
#[derive(Default, Serialize)]
struct ProgressCounts {
    checks: u64,
    emissions: u64,
    emission_ns: u128,
    applied_inputs: u64,
    applied_groups: u64,
    index_validated_rows: u64,
    original_candidate_validated_rows: u64,
    dependency_validated_rows: u64,
    index_len: usize,
    index_capacity: usize,
    index_item_size_bytes: usize,
}
#[derive(Default, Serialize)]
pub(crate) struct Times {
    calls: u64,
    completed: u64,
    ns: u128,
    max_ns: u128,
    rows: BTreeMap<&'static str, u64>,
}
impl Times {
    fn add(&mut self, ns: u128, completed: bool) {
        self.calls += 1;
        self.completed += u64::from(completed);
        self.ns += ns;
        self.max_ns = self.max_ns.max(ns);
    }
}
#[derive(Default, Clone, Serialize)]
pub(crate) struct Context {
    expected_input_position: u64,
    input_position: Option<u64>,
    kind: String,
    target_condition: Option<String>,
    historical_member_conditions: Vec<String>,
    source_message_id: Option<String>,
    dependency_refs: Vec<CanonicalInputRecordRefV1>,
}
#[derive(Default, Serialize)]
struct Entry {
    context: Context,
    outcome: String,
    total_ns: u128,
    primary_ns: BTreeMap<&'static str, u128>,
    nested_ns: BTreeMap<&'static str, u128>,
    residual_ns: i128,
    failed_spans: Vec<&'static str>,
}
#[derive(Default, Serialize)]
pub(crate) struct Scan {
    kind: &'static str,
    context: Context,
    cutoff: Option<u64>,
    cursor_before: Option<u64>,
    cursor_after: Option<u64>,
    open_attempted: bool,
    open_succeeded: bool,
    completed: bool,
    exhausted: Option<bool>,
    rows: BTreeMap<&'static str, u64>,
    twap_encountered: u64,
    first_twap_input: Option<u64>,
    last_twap_input: Option<u64>,
    matched_records: u64,
    returned: Option<usize>,
    ns: u128,
}
#[derive(Default, Serialize)]
struct ScanTotals {
    invocations: u64,
    open_attempts: u64,
    open_successes: u64,
    completed: u64,
    rows: u64,
    row_kinds: BTreeMap<&'static str, u64>,
    twap_encountered: u64,
    matched_records: u64,
    returned: u64,
    ns: u128,
}
impl ScanTotals {
    fn add(&mut self, d: &Scan) {
        self.invocations += 1;
        self.open_attempts += u64::from(d.open_attempted);
        self.open_successes += u64::from(d.open_succeeded);
        self.completed += u64::from(d.completed);
        self.rows += d.rows.values().sum::<u64>();
        for (kind, count) in &d.rows {
            *self.row_kinds.entry(kind).or_default() += count;
        }
        self.twap_encountered += d.twap_encountered;
        self.matched_records += d.matched_records;
        self.returned += d.returned.unwrap_or(0) as u64;
        self.ns += d.ns;
    }
}
#[derive(Serialize)]
struct PositionScans {
    context: Context,
    kinds: BTreeMap<&'static str, ScanTotals>,
}
#[derive(Default, Serialize)]
pub(crate) struct Report {
    entry_calls: u64,
    returned_inputs: u64,
    eof_returns: u64,
    failed_entries: u64,
    entry_total_ns: u128,
    residual_total_ns: i128,
    negative_residual_entries: u64,
    primary: BTreeMap<&'static str, Times>,
    nested: BTreeMap<&'static str, Times>,
    by_return_kind: BTreeMap<String, Times>,
    scan_totals: BTreeMap<&'static str, ScanTotals>,
    scan_by_input: BTreeMap<u64, PositionScans>,
    scans_without_input_context: u64,
    scans: Vec<Scan>,
    scans_omitted: u64,
    slowest_entries: Vec<Entry>,
    slow_entries_omitted: u64,
    last_entry: Option<Entry>,
    #[serde(skip)]
    current: Option<Entry>,
    decode_by_role: BTreeMap<&'static str, DecodeCounts>,
    progress: ProgressCounts,
    candidate_pages: Vec<serde_json::Value>,
    candidate_page_evidence_ns: u128,
    dependency_outputs: Vec<serde_json::Value>,
    dependency_evidence_ns: u128,
    #[serde(skip)]
    original_dependencies: bool,
    #[serde(skip)]
    original_candidates: bool,
    #[serde(skip)]
    capture_pages: bool,
    #[serde(skip)]
    progress_every: Duration,
    #[serde(skip)]
    began: Option<Instant>,
    #[serde(skip)]
    last_progress: Option<Instant>,
    #[serde(skip)]
    known_total_inputs: Option<u64>,
}
pub(crate) struct Session;
impl Session {
    pub(crate) fn enable() -> Self {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            assert!(s.is_none(), "诊断会话不可嵌套");
            *s = Some(Report::default());
        });
        Self
    }
    pub(crate) fn configure(&self, original: bool, interval: u64, pages: bool) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            r.original_candidates = original;
            r.capture_pages = pages;
            r.progress_every = Duration::from_secs(interval);
            r.began = Some(Instant::now());
            r.last_progress = r.began;
        });
    }
    pub(crate) fn dependency_mode(&self, original: bool) {
        STATE.with(|s| s.borrow_mut().as_mut().unwrap().original_dependencies = original);
    }
    pub(crate) fn finish(self) -> Report {
        STATE.with(|s| s.borrow_mut().take().unwrap())
    }
}
pub(crate) fn original_candidates() -> bool {
    STATE.with(|s| s.borrow().as_ref().is_some_and(|r| r.original_candidates))
}
pub(crate) fn original_dependencies() -> bool {
    STATE.with(|s| s.borrow().as_ref().is_some_and(|r| r.original_dependencies))
}
pub(crate) fn dependency_evidence(
    refs: &[CanonicalInputRecordRefV1],
    values: &[CanonicalReferenceEventV2],
) {
    if refs.is_empty() {
        return;
    }
    STATE.with(|s| {
        if let Some(r) = s.borrow_mut().as_mut().filter(|r| r.capture_pages) {
            let at = Instant::now();
            r.dependency_outputs.push(serde_json::json!({"context":r.current.as_ref().map(|e|&e.context),"requested_refs":refs,"values":values}));
            r.dependency_evidence_ns += at.elapsed().as_nanos();
        }
    });
}
pub(crate) fn known_total(prefix: Option<&CommittedIngressPrefixV1>) {
    STATE.with(|s| {
        if let Some(r) = s.borrow_mut().as_mut() {
            r.known_total_inputs = prefix
                .filter(|p| p.normal_end)
                .map(|p| p.next_input_position);
        }
    });
}
pub(crate) fn applied_work(inputs: u64, groups: u64) {
    STATE.with(|s| {
        if let Some(r) = s.borrow_mut().as_mut() {
            r.progress.applied_inputs = inputs;
            r.progress.applied_groups = groups;
        }
    });
    progress("applied", false);
}
pub(crate) fn progress(stage: &'static str, force: bool) {
    STATE.with(|s| {
        let mut s=s.borrow_mut(); let Some(r)=s.as_mut() else{return};
        let Some(began)=r.began else{return};
        if r.progress_every.is_zero() {return;}
        r.progress.checks+=1;
        let now=Instant::now();
        if !force && now.duration_since(r.last_progress.unwrap())<r.progress_every {return;}
        r.last_progress=Some(now);
        let emission=Instant::now();
        let row=serde_json::json!({"stage":stage,"elapsed_seconds":began.elapsed().as_secs_f64(),
            "applied_inputs":r.progress.applied_inputs,"applied_input_total":r.known_total_inputs,
            "applied_input_ratio_only":r.known_total_inputs.map(|n|r.progress.applied_inputs as f64/n as f64),
            "applied_groups":r.progress.applied_groups,"index_validated_rows":r.progress.index_validated_rows,
            "index_items":r.progress.index_len,"index_items_total":null,
            "original_candidate_validated_rows":r.progress.original_candidate_validated_rows,
            "dependency_validated_rows":r.progress.dependency_validated_rows,
            "location_decode":r.decode_by_role.get("candidate_location"),"eta":null,
            "note":"读取/预索引/应用分别计数；比例仅为已应用Input，不是整任务完成比例；未知总量/ETA不推断"});
        eprintln!("DPT_REPLAY_PROGRESS {row}");
        r.progress.emissions+=1;r.progress.emission_ns+=emission.elapsed().as_nanos();
    });
}
pub(crate) fn index_size(len: usize, capacity: usize, size: usize) {
    STATE.with(|s| {
        if let Some(r) = s.borrow_mut().as_mut() {
            r.progress.index_len = len;
            r.progress.index_capacity = capacity;
            r.progress.index_item_size_bytes = size;
        }
    });
}
pub(crate) struct DecodeScope(Option<&'static str>);
impl DecodeScope {
    pub(crate) fn new(role: &'static str) -> Self {
        let enabled = STATE.with(|s| s.borrow().is_some());
        Self(DECODE_ROLE.replace(enabled.then_some(role)))
    }
}
impl Drop for DecodeScope {
    fn drop(&mut self) {
        DECODE_ROLE.set(self.0);
    }
}
pub(crate) fn decode_enabled() -> bool {
    DECODE_ROLE.get().is_some()
}
fn decode_update(update: impl FnOnce(&mut DecodeCounts)) {
    if let Some(role) = DECODE_ROLE.get() {
        STATE.with(|s| {
            if let Some(r) = s.borrow_mut().as_mut() {
                update(r.decode_by_role.entry(role).or_default());
            }
        });
    }
}
pub(crate) fn decode_clock() -> Option<Instant> {
    decode_enabled().then(Instant::now)
}
pub(crate) fn decoded_metadata(bytes: usize, ns: u128) {
    decode_update(|d| {
        d.metadata_loads += 1;
        d.metadata_load_ns += ns;
        d.max_parquet_metadata_estimate_bytes = d.max_parquet_metadata_estimate_bytes.max(bytes);
    });
}
pub(crate) fn decoded_row_group(ns: u128) {
    decode_update(|d| {
        d.row_groups_opened += 1;
        d.row_group_open_ns += ns;
    });
}
pub(crate) fn decoded_batch(rows: usize, resident: usize, arrow: usize, ns: u128) {
    decode_update(|d| {
        d.batches += 1;
        d.successful_batch_decode_and_rows_ns += ns;
        d.rows += rows as u64;
        d.max_sort_row_resident_bytes = d.max_sort_row_resident_bytes.max(resident);
        d.max_arrow_batch_bytes = d.max_arrow_batch_bytes.max(arrow);
    });
    progress(DECODE_ROLE.get().unwrap_or("decode"), false);
}
pub(crate) fn candidate_page_evidence(
    values: &[TemporaryReferenceCandidateV1],
    before: TemporaryReferenceCursorV1,
    after: TemporaryReferenceCursorV1,
    cutoff: u64,
    exhausted: bool,
) {
    STATE.with(|s|{let mut s=s.borrow_mut();let Some(r)=s.as_mut().filter(|r|r.capture_pages) else{return};let at=Instant::now();
        r.candidate_pages.push(serde_json::json!({"context":r.current.as_ref().map(|e|&e.context),"cursor_before":before.next_input_position,"cursor_after":after.next_input_position,"cutoff":cutoff,"exhausted":exhausted,
            "candidates":values.iter().map(|v|serde_json::json!({"canonical_fact":v.canonical_fact,"record_ref":v.record_ref,"anchor":v.anchor})).collect::<Vec<_>>() }));r.candidate_page_evidence_ns+=at.elapsed().as_nanos();});
}
pub(crate) fn candidate_statistics(replay: &PublicReplayV1) -> serde_json::Value {
    replay
        .candidates
        .as_ref()
        .map(|c| c.statistics())
        .unwrap_or(serde_json::Value::Null)
}
pub(crate) fn release_candidates(replay: &mut PublicReplayV1) {
    drop(replay.candidates.take());
}
impl Drop for Session {
    fn drop(&mut self) {
        STATE.with(|s| {
            s.borrow_mut().take();
        });
    }
}
fn started() -> Option<Instant> {
    STATE.with(|s| s.borrow().as_ref().map(|_| Instant::now()))
}
pub(crate) fn kind(input: &RecordedIngressV1) -> &'static str {
    match &input.ingress {
        DptIngressV1::EndOfInput => "end_of_input",
        DptIngressV1::Aborted { .. } => "aborted",
        DptIngressV1::Message { row, .. } => match row {
            TypedRawRowV2::Existing(row) => match row {
                TypedRawRow::PolymarketOrderbook(_) => "polymarket_orderbook",
                TypedRawRow::PolymarketMarketMetadata(_) => "metadata_existing",
                TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_) => "chainlink_spot",
                TypedRawRow::BinanceDepthSnapshot(_) => "binance_depth_snapshot",
                TypedRawRow::BinanceDepthUpdate(_) => "binance_depth_update",
                TypedRawRow::BinanceAggTrade(_) => "binance_agg_trade",
            },
            TypedRawRowV2::PolymarketMarketMetadataV6(row) => match row.as_ref() {
                PolymarketMarketMetadataV6Input::Existing(_) => "metadata_v6_existing",
                PolymarketMarketMetadataV6Input::AnchorRule(_) => "anchor_rule",
            },
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(_) => "chainlink_twap",
            TypedRawRowV2::HistoricalPriceAnchorRepairBatch(_) => "historical_anchor_repair",
        },
    }
}
fn row_kind(row: &PublicStoredRowV1) -> &'static str {
    match row {
        PublicStoredRowV1::Input(_) => "input",
        PublicStoredRowV1::Canonical { .. } => "canonical",
        PublicStoredRowV1::Report { .. } => "report",
        PublicStoredRowV1::PmReport { .. } => "pm_report",
        PublicStoredRowV1::PmStepBinding(_) => "pm_step_binding",
        PublicStoredRowV1::Group(_) => "group",
        PublicStoredRowV1::Observation(_) => "observation",
        PublicStoredRowV1::SourceBinding(_) => "source_binding",
    }
}
pub(crate) struct EntryGuard(Option<Instant>);
impl EntryGuard {
    pub(crate) fn new(expected: u64) -> Self {
        let start = started();
        if start.is_some() {
            STATE.with(|s| {
                s.borrow_mut().as_mut().unwrap().current = Some(Entry {
                    context: Context {
                        expected_input_position: expected,
                        kind: "unresolved".into(),
                        ..Context::default()
                    },
                    outcome: "error".into(),
                    ..Entry::default()
                })
            });
        }
        Self(start)
    }
    pub(crate) fn bind(&self, input: &RecordedIngressV1, target: Option<&MarketIdentityV2>) {
        if self.0.is_none() {
            return;
        }
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let entry = s.as_mut().unwrap().current.as_mut().unwrap();
            entry.context.input_position = Some(input.input_position);
            entry.context.kind = kind(input).into();
            entry.context.target_condition = target.map(|m| m.condition_id.0.clone());
            entry.context.dependency_refs = input.dependency_record_refs.clone();
            if let DptIngressV1::Message {
                row,
                source_evidence,
            } = &input.ingress
            {
                entry.context.source_message_id = Some(source_evidence.source_message_id.0.clone());
                if let TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch) = row {
                    entry.context.historical_member_conditions =
                        batch.rows.iter().map(|r| r.condition_id.clone()).collect();
                }
            }
        });
    }
    pub(crate) fn returned(&self, eof: bool) {
        if self.0.is_some() {
            STATE.with(|s| {
                s.borrow_mut()
                    .as_mut()
                    .unwrap()
                    .current
                    .as_mut()
                    .unwrap()
                    .outcome = if eof { "eof" } else { "input" }.into()
            });
        }
    }
}
impl Drop for EntryGuard {
    fn drop(&mut self) {
        let Some(start) = self.0 else {
            return;
        };
        let ns = start.elapsed().as_nanos();
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            let mut e = r.current.take().unwrap();
            e.total_ns = ns;
            e.residual_ns = ns as i128 - e.primary_ns.values().sum::<u128>() as i128;
            r.entry_calls += 1;
            r.entry_total_ns += ns;
            r.residual_total_ns += e.residual_ns;
            r.negative_residual_entries += u64::from(e.residual_ns < 0);
            match e.outcome.as_str() {
                "input" => r.returned_inputs += 1,
                "eof" => r.eof_returns += 1,
                _ => r.failed_entries += 1,
            };
            let k = if e.outcome == "eof" {
                "eof".into()
            } else {
                e.context.kind.clone()
            };
            r.by_return_kind
                .entry(k)
                .or_default()
                .add(ns, e.outcome != "error");
            r.last_entry = Some(e);
            let e = r.last_entry.as_ref().unwrap();
            if r.slowest_entries.len() < RETAIN_SLOW
                || ns > r.slowest_entries.last().unwrap().total_ns
            {
                // 只克隆至多16个小型定位项，不复制业务载荷。
                let v = Entry {
                    context: e.context.clone(),
                    outcome: e.outcome.clone(),
                    total_ns: e.total_ns,
                    primary_ns: e.primary_ns.clone(),
                    nested_ns: e.nested_ns.clone(),
                    residual_ns: e.residual_ns,
                    failed_spans: e.failed_spans.clone(),
                };
                r.slowest_entries.push(v);
                r.slowest_entries
                    .sort_by_key(|e| std::cmp::Reverse(e.total_ns));
                r.slowest_entries.truncate(RETAIN_SLOW);
            }
            r.slow_entries_omitted = r.entry_calls.saturating_sub(r.slowest_entries.len() as u64);
        });
    }
}
pub(crate) struct Span {
    start: Option<Instant>,
    kind: &'static str,
    completed: bool,
    rows: BTreeMap<&'static str, u64>,
}
impl Span {
    pub(crate) fn new(kind: &'static str) -> Self {
        progress(kind, false);
        Self {
            start: started(),
            kind,
            completed: false,
            rows: BTreeMap::new(),
        }
    }
    pub(crate) fn row(&mut self, row: &PublicStoredRowV1) {
        if self.start.is_some() {
            *self.rows.entry(row_kind(row)).or_default() += 1;
        }
    }
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        let ns = start.elapsed().as_nanos();
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            let t = r.primary.entry(self.kind).or_default();
            t.add(ns, self.completed);
            for (k, n) in &self.rows {
                *t.rows.entry(k).or_default() += n;
            }
            if let Some(e) = r.current.as_mut() {
                *e.primary_ns.entry(self.kind).or_default() += ns;
                if !self.completed {
                    e.failed_spans.push(self.kind);
                }
            }
        });
    }
}
pub(crate) struct ScanGuard {
    start: Option<Instant>,
    data: Option<Scan>,
}
impl ScanGuard {
    pub(crate) fn new(
        kind: &'static str,
        cursor: Option<u64>,
        cutoff: Option<u64>,
        opens: bool,
    ) -> Self {
        let start = started();
        let data = start.map(|_| {
            STATE.with(|s| Scan {
                kind,
                context: s
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .current
                    .as_ref()
                    .map(|e| e.context.clone())
                    .unwrap_or_default(),
                cursor_before: cursor,
                cutoff,
                open_attempted: opens,
                ..Scan::default()
            })
        });
        Self { start, data }
    }
    pub(crate) fn opened(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.open_succeeded = true;
        }
    }
    pub(crate) fn row(&mut self, row: &PublicStoredRowV1) {
        if let Some(d) = self.data.as_mut() {
            STATE.with(|s| {
                if let Some(r) = s.borrow_mut().as_mut() {
                    match d.kind {
                        "candidate_index_validation" => r.progress.index_validated_rows += 1,
                        "candidate_page" if r.original_candidates => {
                            r.progress.original_candidate_validated_rows += 1
                        }
                        "dependency_scan" | "dependency_index_validation" => {
                            r.progress.dependency_validated_rows += 1
                        }
                        _ => {}
                    }
                }
            });
            *d.rows.entry(row_kind(row)).or_default() += 1;
            if let PublicStoredRowV1::Canonical {
                binding,
                record: CanonicalEventRecordV2::ReferenceEvent(event),
                ..
            } = row
                && matches!(
                    event.payload,
                    CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_)
                )
            {
                d.twap_encountered += 1;
                d.first_twap_input
                    .get_or_insert(binding.position.closed_at_input_position);
                d.last_twap_input = Some(binding.position.closed_at_input_position);
            }
        }
        progress(self.data.as_ref().map_or("scan", |d| d.kind), false);
    }
    pub(crate) fn matched(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.matched_records += 1;
        }
    }
    pub(crate) fn complete(
        &mut self,
        returned: usize,
        cursor: Option<u64>,
        exhausted: Option<bool>,
    ) {
        if let Some(d) = self.data.as_mut() {
            d.completed = true;
            d.returned = Some(returned);
            d.cursor_after = cursor;
            d.exhausted = exhausted;
        }
    }
}
impl Drop for ScanGuard {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        let ns = start.elapsed().as_nanos();
        let mut d = self.data.take().unwrap();
        d.ns = ns;
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            let r = s.as_mut().unwrap();
            r.scan_totals.entry(d.kind).or_default().add(&d);
            if let Some(position) = d.context.input_position {
                r.scan_by_input
                    .entry(position)
                    .or_insert_with(|| PositionScans {
                        context: d.context.clone(),
                        kinds: BTreeMap::new(),
                    })
                    .kinds
                    .entry(d.kind)
                    .or_default()
                    .add(&d);
            } else {
                r.scans_without_input_context += 1;
            }
            if matches!(d.kind, "candidate_open" | "candidate_page") {
                r.nested.entry(d.kind).or_default().add(ns, d.completed);
                if let Some(e) = r.current.as_mut() {
                    *e.nested_ns.entry(d.kind).or_default() += ns;
                    if !d.completed {
                        e.failed_spans.push(d.kind);
                    }
                }
            }
            if r.scans.len() < RETAIN_SCANS {
                r.scans.push(d);
            } else {
                r.scans_omitted += 1;
            }
        });
    }
}
pub(crate) fn report_value(report: Report) -> serde_json::Value {
    let prepare_ns = report.primary.get("prepare").map_or(0, |t| t.ns);
    let candidate_ns = report.nested.values().map(|t| t.ns).sum::<u128>();
    let mut value = serde_json::to_value(report).unwrap();
    value["prepare_remaining_ns"] = serde_json::json!(prepare_ns as i128 - candidate_ns as i128);
    value["retention"] = serde_json::json!({"scan_descriptors":"first 128 actual scan/open/page invocations; total by kind and omitted count retained","scan_by_input":"all actually scanning Input positions with per-kind counts/rows/time; no per-row payload","slow_entries":"16 slowest whole entry calls; total and omitted count retained"});
    value["time_accounting"] = serde_json::json!({"primary":PRIMARY,"residual":"signed entry total minus non-overlapping primary spans; no zero clamp","nested":"candidate_open/candidate_page are contained in prepare and must not be added to primary; scan elapsed is a subset, not extra primary time","overhead":"clock checks and local counting are inside this test diagnosis; bookkeeping at scope destruction falls within enclosing span or residual; overhead not independently isolated","twap_encountered":"original candidate_page counts decoded TWAP before filtering; indexed candidate_index_validation counts validated TWAP and indexed candidate_page counts selected body rows only; cached index visits have no full body decode","decode_cost":"metadata/row-group/successful batch spans are contained in candidate page or validation/open calls, not additional primary time; batch includes Arrow decode and SortRow materialization plus test size accounting; failed loads/batches stay in enclosing original-error spans","size_accounting":"Parquet memory_size is its estimated allocation including shared-pointer double counts, not RSS; Arrow and SortRow maximum refer to one successful batch, not process or all-reader sum","dependency_evidence":"nonempty saved-ref requests retain all complete returned Reference events, including empty result; projection time is inside dependencies; validation/locating scan spans are nested there, not additive primary time", "candidate_evidence":"all successful pages retain complete canonical fact/anchor/ref and cursor/cutoff; projection time is inside prepare_remaining, not independent production latency; progress emission cost is inside its caller and separately reported, never added again"});
    value
}
pub(crate) fn reader_prefix(replay: &PublicReplayV1) -> Option<CommittedIngressPrefixV1> {
    replay.reader.prefix.clone()
}

#[cfg(test)]
mod progress_tests {
    use super::*;
    #[test]
    fn progress_default_is_disabled_and_known_total_only_comes_from_normal_prefix() {
        let session = Session::enable();
        progress("disabled", true);
        STATE.with(|s| assert_eq!(s.borrow().as_ref().unwrap().progress.emissions, 0));
        session.configure(false, 30, false);
        known_total(None);
        applied_work(2, 1);
        progress("candidate_index_validation", true);
        STATE.with(|s| {
            let s = s.borrow();
            let r = s.as_ref().unwrap();
            assert_eq!(r.known_total_inputs, None);
            assert_eq!(r.progress.applied_inputs, 2);
            assert_eq!(r.progress.emissions, 1);
        });
        let prefix = CommittedIngressPrefixV1 {
            input_stream_id: "test".into(),
            next_input_position: 10,
            next_canonical_position: 0,
            next_group_position: 0,
            next_observation_position: 0,
            next_public_index_position: 0,
            stable_storage_ref: "stable-00000000000000000000".into(),
            normal_end: false,
        };
        known_total(Some(&prefix));
        STATE.with(|s| assert_eq!(s.borrow().as_ref().unwrap().known_total_inputs, None));
        let prefix = CommittedIngressPrefixV1 {
            normal_end: true,
            ..prefix
        };
        known_total(Some(&prefix));
        STATE.with(|s| assert_eq!(s.borrow().as_ref().unwrap().known_total_inputs, Some(10)));
        let r = session.finish();
        assert_eq!(r.progress.applied_groups, 1);
    }
}
