//! #185：仅重放既有public recording；单流政策复用生产，候选只作同折描述。
use baseline_mm::*;
use contracts::*;
use data_pipeline_core::public_input::DptIngressV1;
use data_pipeline_training::public_replay::PublicReplayV1;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const DAY: i64 = 86_400_000;
const PERCENTILES: [usize; 4] = [50, 90, 95, 99];
#[derive(Serialize, Deserialize)]
struct Config {
    public_root: PathBuf,
    prefix: CommittedIngressPrefixV1,
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
    expected_capture_bytes: u64,
    expected_capture_sha256: String,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    #[serde(default = "default_progress_interval")]
    progress_interval_seconds: u64,
    selected_source_identity_sha256: String,
    selection_evidence: Value,
    implementation_identity: Value,
}
fn default_progress_interval() -> u64 {
    30
}
#[derive(Default, Clone, Serialize)]
struct Counts {
    applied_inputs: u64,
    end_inputs: u64,
    closed_groups: u64,
    reference_facts: u64,
    reference_anomalies: BTreeMap<String, u64>,
    observations: u64,
    progress_updates: u64,
    progress_unchanged: u64,
    valid_message_updates: u64,
    same_value_valid_updates: u64,
    same_millisecond_received: u64,
    negative_received_intervals: u64,
    missing_event: u64,
    event_after_receive: u64,
    receive_after_visible_frontier: u64,
    cross_midnight_received_intervals: u64,
    connection_generation_unknown: u64,
    synchronization_unknown: u64,
}
#[derive(Default)]
struct Series {
    values: Vec<u64>,
    missing: u64,
    negative: u64,
    inconsistent: u64,
}
impl Series {
    fn add(&mut self, value: Option<i128>, consistent: bool) {
        match value {
            None => self.missing += 1,
            Some(v) if v < 0 => self.negative += 1,
            Some(_) if !consistent => self.inconsistent += 1,
            Some(v) => self.values.push(v as u64),
        }
    }
    fn summary(&mut self) -> Value {
        self.values.sort_unstable();
        json!({"valid_nonnegative_samples":self.values.len(),"missing":self.missing,
            "negative":self.negative,"inconsistent_excluded":self.inconsistent,
            "quantiles_ms":quantiles(&self.values),"maximum_ms":self.values.last()})
    }
}
fn quantiles(sorted: &[u64]) -> BTreeMap<String, Option<u64>> {
    PERCENTILES
        .into_iter()
        .map(|p| {
            let value = (!sorted.is_empty()).then(|| sorted[(p * sorted.len()).div_ceil(100) - 1]);
            (format!("p{p}"), value)
        })
        .collect()
}
#[derive(Clone, Debug, Serialize)]
struct Segment {
    visible_at_utc_ms: i64,
    facts: Option<SourceHealthFactsV1>,
}
#[derive(Default, Debug, Serialize)]
struct Coverage {
    window_ms: u64,
    healthy_ms: u64,
    first_failure_ms: BTreeMap<String, u64>,
    receive_silence_condition_ms: u64,
    event_age_condition_ms: u64,
    timing_condition_union_ms: u64,
    timing_condition_overlap_ms: u64,
    event_time_missing_evidence_ms: u64,
}
fn milliseconds(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
fn conditions(
    f: Option<&SourceHealthFactsV1>,
    at: i64,
    silence: i64,
    delay: i64,
) -> (bool, bool, bool) {
    let Some((r, e)) = f.and_then(|f| f.last_valid_message_times) else {
        return (false, false, false);
    };
    let timeline = r <= at && e.is_none_or(|e| e <= r);
    (
        timeline && at >= r.saturating_add(silence),
        timeline && e.is_some_and(|e| at >= e.saturating_add(delay)),
        e.is_none(),
    )
}
impl Coverage {
    fn include(
        &mut self,
        f: Option<&SourceHealthFactsV1>,
        from: i64,
        to: i64,
        silence: i64,
        delay: i64,
    ) {
        let span = (to - from) as u64;
        self.window_ms += span;
        match source_health_failure_v1(f, silence, delay, from) {
            Some(failure) => {
                *self
                    .first_failure_ms
                    .entry(format!("{failure:?}"))
                    .or_default() += span
            }
            None => self.healthy_ms += span,
        }
        let (receive, event, missing) = conditions(f, from, silence, delay);
        self.receive_silence_condition_ms += u64::from(receive) * span;
        self.event_age_condition_ms += u64::from(event) * span;
        self.timing_condition_union_ms += u64::from(receive || event) * span;
        self.timing_condition_overlap_ms += u64::from(receive && event) * span;
        self.event_time_missing_evidence_ms += u64::from(missing) * span;
    }
    fn check(&self) -> Result<()> {
        if self.healthy_ms + self.first_failure_ms.values().sum::<u64>() != self.window_ms
            || self.receive_silence_condition_ms + self.event_age_condition_ms
                != self.timing_condition_union_ms + self.timing_condition_overlap_ms
        {
            return Err("区间分类或联合时长对账失败".into());
        }
        Ok(())
    }
}
fn coverage(
    segments: &[Segment],
    start: i64,
    end: i64,
    silence: u64,
    delay: u64,
) -> Result<(Coverage, BTreeMap<i64, Coverage>)> {
    let (silence, delay) = (milliseconds(silence), milliseconds(delay));
    let mut overall = Coverage::default();
    let mut days = BTreeMap::<i64, Coverage>::new();
    for (index, segment) in segments.iter().enumerate() {
        let from = segment.visible_at_utc_ms.max(start);
        let to = segments
            .get(index + 1)
            .map_or(end, |s| s.visible_at_utc_ms)
            .min(end);
        if from >= to {
            continue;
        }
        let mut boundaries = vec![from, to];
        if let Some((r, e)) = segment
            .facts
            .as_ref()
            .and_then(|f| f.last_valid_message_times)
        {
            boundaries.extend([r, r.saturating_add(silence)]);
            if let Some(e) = e {
                boundaries.push(e.saturating_add(delay));
            }
        }
        let mut midnight = (from.div_euclid(DAY) + 1) * DAY;
        while midnight < to {
            boundaries.push(midnight);
            midnight += DAY;
        }
        boundaries.retain(|t| *t >= from && *t <= to);
        boundaries.sort_unstable();
        boundaries.dedup();
        for times in boundaries.windows(2) {
            overall.include(segment.facts.as_ref(), times[0], times[1], silence, delay);
            days.entry(times[0].div_euclid(DAY) * DAY)
                .or_default()
                .include(segment.facts.as_ref(), times[0], times[1], silence, delay);
        }
    }
    overall.check()?;
    for day in days.values() {
        day.check()?;
    }
    if overall.window_ms != (end - start) as u64
        || days.values().map(|d| d.window_ms).sum::<u64>() != overall.window_ms
    {
        return Err("UTC窗口交集时长对账失败".into());
    }
    Ok((overall, days))
}
fn hash(path: &Path) -> Result<String> {
    let mut reader = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
fn replay(c: &Config, output: &Path) -> Result<Value> {
    if c.received_start_utc_ms >= c.received_end_utc_ms || !c.prefix.normal_end {
        return Err("研究窗口或发布正常前缀无效".into());
    }
    let capture = c
        .public_root
        .join(&c.prefix.stable_storage_ref)
        .join("captured.parquet");
    if fs::metadata(&capture)?.len() != c.expected_capture_bytes
        || hash(&capture)? != c.expected_capture_sha256
    {
        return Err("原捕获长度/摘要不符；拒绝回放".into());
    }
    let mut replay = PublicReplayV1::open_committed(
        &c.public_root,
        c.prefix.clone(),
        c.read_page_rows,
        c.max_read_batch_bytes,
    )?;
    replay.core.enable_source_progress();
    replay.set_progress(
        Duration::from_secs(c.progress_interval_seconds),
        std::io::stderr(),
    );
    let scope = SourceStreamScopeV1 {
        source: SourceStreamKindV1::ChainlinkSpot,
        condition_id: None,
        asset_or_symbol: "btc/usd".into(),
    };
    let (mut total, mut daily) = (Counts::default(), BTreeMap::<i64, Counts>::new());
    let (mut receive, mut lag, mut age) = (Series::default(), Series::default(), Series::default());
    let mut segments = vec![Segment {
        visible_at_utc_ms: c.received_start_utc_ms,
        facts: None,
    }];
    let mut now = i64::MIN;
    let mut previous_progress: Option<SourceStreamProgressV1> = None;
    let mut previous_value = None;
    let mut previous_update: Option<(i64, Value)> = None;
    let (mut first, mut last) = (None, None);
    let (mut longest, mut crossing) = (Vec::<(u64, Value)>::new(), Vec::<Value>::new());
    let (mut end_seen, mut last_progress_print) = (false, Instant::now());
    let began = Instant::now();
    while let Some((input, applied, _)) = replay.next_prepared_applied()? {
        if end_seen {
            return Err("正常End后仍有输入".into());
        }
        let mut increment = Counts {
            applied_inputs: 1,
            ..Counts::default()
        };
        for group in &applied.applied.closed_groups {
            now = now.max(
                sim_system::public_consumption::original_public_group_visible_at_utc_ms(
                    &input, group,
                ),
            );
            increment.closed_groups += 1;
            increment.observations += group.observations.len() as u64;
            for fact in group.records.iter() {
                if let PublicBusinessPayloadV1::ReferenceFact(reference) = &fact.payload {
                    increment.reference_facts += 1;
                    if let CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(a) = reference {
                        *increment
                            .reference_anomalies
                            .entry(format!("{:?}", a.classification))
                            .or_default() += 1;
                    }
                }
            }
        }
        let progress = replay.core.current_source_progress(&scope);
        if matches!(input.ingress, DptIngressV1::EndOfInput) {
            increment.end_inputs = 1;
            end_seen = true;
        } else {
            let changed = progress != previous_progress.as_ref();
            increment.progress_updates = u64::from(changed);
            increment.progress_unchanged = u64::from(!changed);
            if let Some(p) = progress {
                increment.connection_generation_unknown =
                    u64::from(p.connection_generation.is_none());
                increment.synchronization_unknown =
                    u64::from(p.synchronization == SourceSynchronizationV1::Unknown);
                let prior_id = previous_progress
                    .as_ref()
                    .and_then(|p| p.last_valid_business_message.as_ref())
                    .map(|e| &e.source_message_id);
                if let Some(e) = p
                    .last_valid_business_message
                    .as_ref()
                    .filter(|e| Some(&e.source_message_id) != prior_id)
                {
                    increment.valid_message_updates = 1;
                    let value = replay
                        .core
                        .current_chainlink_spot()
                        .value
                        .as_ref()
                        .map(|p| p.price_usd_per_btc.clone());
                    increment.same_value_valid_updates =
                        u64::from(previous_value.is_some() && value == previous_value);
                    previous_value = value;
                    increment.missing_event = u64::from(e.source_event_at_utc_ms.is_none());
                    increment.event_after_receive = u64::from(
                        e.source_event_at_utc_ms
                            .is_some_and(|t| t > e.received_at_utc_ms),
                    );
                    increment.receive_after_visible_frontier =
                        u64::from(e.received_at_utc_ms > now);
                    let evidence = json!({"input_position":input.input_position,"source_message_id":e.source_message_id,
                        "received_at_utc_ms":e.received_at_utc_ms,"source_event_at_utc_ms":e.source_event_at_utc_ms,"visible_frontier_utc_ms":now,
                        "observed_message_timestamp_utc_ms":replay.core.current_chainlink_spot().value.as_ref().map(|v|v.message_timestamp_utc_ms)});
                    first.get_or_insert_with(|| evidence.clone());
                    last = Some(evidence.clone());
                    let consistent = e.received_at_utc_ms <= now
                        && e.source_event_at_utc_ms
                            .is_none_or(|t| t <= e.received_at_utc_ms);
                    lag.add(
                        e.source_event_at_utc_ms
                            .map(|t| i128::from(e.received_at_utc_ms) - i128::from(t)),
                        true,
                    );
                    age.add(
                        e.source_event_at_utc_ms
                            .map(|t| i128::from(now) - i128::from(t)),
                        consistent,
                    );
                    if let Some((r, left)) = &previous_update {
                        let gap = i128::from(e.received_at_utc_ms) - i128::from(*r);
                        increment.same_millisecond_received = u64::from(gap == 0);
                        increment.negative_received_intervals = u64::from(gap < 0);
                        receive.add(Some(gap), true);
                        let pair = json!({"left":left,"right":evidence,"received_interval_ms":gap});
                        if r.div_euclid(DAY) != e.received_at_utc_ms.div_euclid(DAY) {
                            increment.cross_midnight_received_intervals = 1;
                            crossing.push(pair.clone());
                        }
                        if gap >= 0 {
                            longest.push((gap as u64, pair));
                            longest.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                            longest.truncate(10);
                        }
                    }
                    previous_update = Some((e.received_at_utc_ms, evidence));
                }
            }
        }
        add_counts(&mut total, &increment);
        if now != i64::MIN {
            add_counts(
                daily.entry(now.div_euclid(DAY) * DAY).or_default(),
                &increment,
            );
        }
        if !end_seen && now != i64::MIN {
            let facts = progress.map(SourceHealthFactsV1::from);
            let at = now.clamp(c.received_start_utc_ms, c.received_end_utc_ms);
            let current = segments.last_mut().expect("初始未知区间");
            if current.visible_at_utc_ms == at {
                current.facts = facts;
            } else if current.facts != facts {
                segments.push(Segment {
                    visible_at_utc_ms: at,
                    facts,
                });
            }
        }
        previous_progress = progress.cloned();
        if c.progress_interval_seconds > 0
            && last_progress_print.elapsed() >= Duration::from_secs(c.progress_interval_seconds)
        {
            eprintln!(
                "{}",
                json!({"stage":"source_health_replay","applied_inputs":total.applied_inputs,"total_inputs":c.prefix.next_input_position,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
            last_progress_print = Instant::now();
        }
    }
    if !end_seen
        || total.end_inputs != 1
        || total.applied_inputs != c.prefix.next_input_position
        || total.closed_groups != c.prefix.next_group_position
        || total.observations != c.prefix.next_observation_position
        || replay.core.applied_prefix().next_input_position != c.prefix.next_input_position
    {
        return Err("End/EOF/已应用输入或发布前缀对账失败".into());
    }
    let (receive_summary, lag_summary, age_summary) =
        (receive.summary(), lag.summary(), age.summary());
    let mut silences: Vec<_> = quantiles(&receive.values)
        .values()
        .flatten()
        .copied()
        .collect();
    silences.sort_unstable();
    silences.dedup();
    let mut delays: Vec<_> = quantiles(&age.values).values().flatten().copied().collect();
    delays.sort_unstable();
    delays.dedup();
    let mut candidates = Vec::new();
    for silence in silences {
        for delay in &delays {
            let (overall, days) = coverage(
                &segments,
                c.received_start_utc_ms,
                c.received_end_utc_ms,
                silence,
                *delay,
            )?;
            candidates.push(json!({"receive_silence_ms":silence,"event_delay_ms":delay,
            "receive_interval_at_or_above":receive.values.iter().filter(|v|**v>=silence).count(),"receive_interval_denominator":receive.values.len(),
            "receive_interval_exceed_fraction":(!receive.values.is_empty()).then(||receive.values.iter().filter(|v|**v>=silence).count() as f64/receive.values.len() as f64),
            "event_age_at_or_above":age.values.iter().filter(|v|**v>=*delay).count(),"event_age_denominator":age.values.len(),
            "event_age_exceed_fraction":(!age.values.is_empty()).then(||age.values.iter().filter(|v|**v>=*delay).count() as f64/age.values.len() as f64),
            "overall":overall,"daily_by_utc_start_ms":days}));
        }
    }
    let mut writer = BufWriter::new(fs::File::create(output.join("segments.jsonl"))?);
    for segment in &segments {
        serde_json::to_writer(&mut writer, segment)?;
        writeln!(writer)?;
    }
    writer.flush()?;
    let last_receive = previous_update.map(|p| p.0);
    Ok(
        json!({"capture_verified_before_open":{"bytes":c.expected_capture_bytes,"sha256":c.expected_capture_sha256},"prefix":c.prefix,"actual_applied_prefix":replay.core.applied_prefix(),"normal_end":end_seen,"eof":true,
        "counts":total,"daily_counts_by_visible_utc_start_ms":daily,
        "receive_interval":receive_summary,"receive_to_source_event_lag":lag_summary,"visible_frontier_to_event_age":age_summary,
        "first_valid_update":first,"last_valid_update":last,"last_visible_frontier_utc_ms":now,
        "unconfirmed_tail_after_last_receive_ms":last_receive.map(|t|c.received_end_utc_ms.saturating_sub(t).max(0)),
        "cross_midnight_intervals":crossing,"longest_received_intervals":longest,"segments":segments.len(),
        "quantile_method":"排序非负有效样本；秩ceil(p*n)，1起算；p50/p90/p95/p99；去重笛卡尔积最多16对；零不加1；最大值仅尾部诊断",
        "time_method":"固定半开UTC窗口；原group visible的单调max；输入应用后读取，不用sigma接纳点；无消息区间依据最近公共进展，End只封口；UTC交集分桶，跨日不重置",
        "condition_method":"静默/事件条件仅用已有消息且recv<=frontier、event缺失或event<=recv；缺event年龄不可计算，不当零；独立条件可重叠，union不重复；first_failure沿共享生产优先级互斥对账",
        "limitations":"同折Chainlink来源描述；连接/同步缺证据保持未知；未运行市场/策略/账户/收益/六源；文本gaps不推精确断连；末次确认后的延伸是原最后进展的条件分析，不是新确认或交易可用证明",
        "candidates":candidates}),
    )
}
fn add_counts(a: &mut Counts, b: &Counts) {
    macro_rules! add {($($f:ident),*)=>{$(a.$f+=b.$f;)*};}
    add!(
        applied_inputs,
        end_inputs,
        closed_groups,
        reference_facts,
        observations,
        progress_updates,
        progress_unchanged,
        valid_message_updates,
        same_value_valid_updates,
        same_millisecond_received,
        negative_received_intervals,
        missing_event,
        event_after_receive,
        receive_after_visible_frontier,
        cross_midnight_received_intervals,
        connection_generation_unknown,
        synchronization_unknown
    );
    for (key, value) in &b.reference_anomalies {
        *a.reference_anomalies.entry(key.clone()).or_default() += value;
    }
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err(
            "用法 research_source_health CONFIG OUTPUT_DIRECTORY（仅既有public回放）".into(),
        );
    }
    let c: Config = serde_json::from_slice(&fs::read(&args[0])?)?;
    let output = PathBuf::from(&args[1]);
    fs::create_dir(&output)?;
    let before = fs::read_to_string("/proc/self/io").ok();
    let began = Instant::now();
    let result = replay(&c, &output);
    let value = json!({"effective_config":c,"elapsed_seconds":began.elapsed().as_secs_f64(),
        "process_io_before":before,"process_io_after":fs::read_to_string("/proc/self/io").ok(),
        "status":if result.is_ok(){"completed"}else{"failed"},"error":result.as_ref().err().map(ToString::to_string),"result":result.as_ref().ok()});
    let mut writer = BufWriter::new(fs::File::create(output.join("result.private.json"))?);
    serde_json::to_writer_pretty(&mut writer, &value)?;
    writer.flush()?;
    result.map(|_| ())
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../shared-run/tests/common/mod.rs"]
mod common;
#[cfg(test)]
mod tests {
    use super::*;
    use data_pipeline_core::{public_input::*, typed_raw::*};
    use data_pipeline_training::public_recording::PublicRecordingWriterV1;
    fn facts(received: i64, event: Option<i64>) -> Option<SourceHealthFactsV1> {
        Some(SourceHealthFactsV1 {
            synchronization_required: false,
            availability: MarketDataQuality::Fresh,
            last_valid_message_times: Some((received, event)),
        })
    }
    fn segment(at: i64, r: i64, event: Option<i64>) -> Segment {
        Segment {
            visible_at_utc_ms: at,
            facts: facts(r, event),
        }
    }
    #[test]
    fn unknown_event_yields_to_silence_at_original_boundary_and_zero_is_immediate() {
        let segments = vec![
            Segment {
                visible_at_utc_ms: 0,
                facts: None,
            },
            segment(2, 2, None),
        ];
        let (c, _) = coverage(&segments, 0, 12, 4, 8).unwrap();
        assert_eq!(c.window_ms, 12);
        assert_eq!(c.healthy_ms, 0);
        assert_eq!(c.first_failure_ms["UnknownBusinessProgress"], 2);
        assert_eq!(c.first_failure_ms["UnknownEventTime"], 4);
        assert_eq!(c.first_failure_ms["ReceiveSilence"], 6);
        assert_eq!(c.event_time_missing_evidence_ms, 10);
        assert_eq!(c.receive_silence_condition_ms, 6);
        assert_eq!(
            source_health_failure_v1(facts(2, Some(2)).as_ref(), 0, 8, 2),
            Some(SourceHealthFailureV1::ReceiveSilence)
        );
        let future = vec![segment(0, 2, Some(1))];
        let (c, _) = coverage(&future, 0, 6, 3, 10).unwrap();
        assert_eq!(c.first_failure_ms["TimeInconsistent"], 2);
        assert_eq!(c.healthy_ms, 3);
        assert_eq!(c.first_failure_ms["ReceiveSilence"], 1);
        assert_eq!(
            source_health_failure_v1(facts(2, Some(3)).as_ref(), 4, 8, 10),
            Some(SourceHealthFailureV1::TimeInconsistent)
        );
    }
    #[test]
    fn independent_timing_union_and_production_first_failure_do_not_double_count() {
        let (c, _) = coverage(
            &[segment(0, 0, Some(-4)), segment(10, 10, Some(10))],
            0,
            20,
            6,
            8,
        )
        .unwrap();
        assert_eq!(
            (
                c.receive_silence_condition_ms,
                c.event_age_condition_ms,
                c.timing_condition_overlap_ms,
                c.timing_condition_union_ms
            ),
            (8, 8, 6, 10)
        );
        assert_eq!(c.healthy_ms, 10);
        assert_eq!(c.first_failure_ms["EventDelay"], 2);
        assert_eq!(c.first_failure_ms["ReceiveSilence"], 8);
        assert_eq!(quantiles(&[0, 1, 2, 10])["p50"], Some(1));
        assert_eq!(quantiles(&[0, 1, 2, 10])["p99"], Some(10));
    }
    #[test]
    fn midnight_window_intersection_preserves_unknown_head_and_unconfirmed_tail() {
        let m = DAY;
        let (c, d) = coverage(
            &[
                Segment {
                    visible_at_utc_ms: m - 8,
                    facts: None,
                },
                segment(m - 5, m - 5, Some(m - 6)),
                segment(m + 4, m + 4, Some(m + 3)),
            ],
            m - 8,
            m + 12,
            4,
            7,
        )
        .unwrap();
        assert_eq!(
            (
                c.window_ms,
                c.healthy_ms,
                c.first_failure_ms["UnknownBusinessProgress"],
                c.first_failure_ms["ReceiveSilence"]
            ),
            (20, 8, 3, 9)
        );
        assert_eq!(
            (
                c.receive_silence_condition_ms,
                c.event_age_condition_ms,
                c.timing_condition_union_ms
            ),
            (9, 5, 9)
        );
        assert_eq!(
            (
                d[&0].window_ms,
                d[&0].healthy_ms,
                d[&0].first_failure_ms["ReceiveSilence"]
            ),
            (8, 4, 1)
        );
        assert_eq!(
            (
                d[&DAY].window_ms,
                d[&DAY].healthy_ms,
                d[&DAY].first_failure_ms["ReceiveSilence"]
            ),
            (12, 4, 8)
        );
    }
    fn spot(received: i64, event: i64) -> PolymarketRtdsChainlinkBtcUsdRow {
        PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: received,
            message_timestamp: received,
            payload_timestamp: event,
            age_ms: Some(received - event),
            symbol: "btc/usd".into(),
            value: "100000".into(),
            topic: "crypto_prices_chainlink".into(),
            message_type: "update".into(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".into(),
            source_native_payload_json: "{}".into(),
            collector_host_id: "synthetic".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into(),
        }
    }
    fn recorded(root: &Path) -> Config {
        let mut core = common::core(); // 原writer未启用health，证明旧record兼容。
        let mut writer =
            PublicRecordingWriterV1::create(root, core.header(), common::config()).unwrap();
        let rows: Vec<_> = [(10, 9), (10, 8), (20, 19), (20, 19)]
            .into_iter()
            .map(|(r, e)| {
                TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot(r, e)))
            })
            .collect();
        for (i, row) in rows.into_iter().enumerate() {
            let input = common::record(i as u64, row);
            let applied = core
                .push(&prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap())
                .unwrap()
                .into_owned();
            writer.record(input, applied).unwrap();
        }
        let mut twap = spot(25, 24);
        twap.topic = "crypto_prices_twap_thirty".into();
        twap.schema_version = POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1.into();
        let input = common::record(
            4,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(PolymarketRtdsChainlinkTwapInput {
                common: twap,
                full_accuracy_value: "100000000000000000000000".into(),
                window_s: 30,
                source_sequence_epoch_id: "twap".into(),
                collector_admission_sequence: 1,
                collector_admission_time_utc_ms: 25,
            }),
        );
        let applied = core
            .push(&prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap())
            .unwrap()
            .into_owned();
        writer.record(input, applied).unwrap();
        let input = RecordedIngressV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position: 5,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        };
        let applied = core
            .push(&prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap())
            .unwrap()
            .into_owned();
        writer.record(input, applied).unwrap();
        let prefix = writer.stabilize().unwrap().unwrap();
        let capture = root
            .join(&prefix.stable_storage_ref)
            .join("captured.parquet");
        Config {
            public_root: root.into(),
            prefix,
            received_start_utc_ms: 0,
            received_end_utc_ms: 30,
            expected_capture_bytes: fs::metadata(&capture).unwrap().len(),
            expected_capture_sha256: hash(&capture).unwrap(),
            read_page_rows: 2,
            max_read_batch_bytes: 1 << 20,
            progress_interval_seconds: 0,
            selected_source_identity_sha256: "synthetic".into(),
            selection_evidence: json!([]),
            implementation_identity: json!("synthetic"),
        }
    }
    #[test]
    fn zero_market_old_record_counts_same_value_same_millisecond_but_not_duplicate_or_twap() {
        let temp = tempfile::tempdir().unwrap();
        let c = recorded(&temp.path().join("public"));
        let r = replay(&c, temp.path()).unwrap();
        assert_eq!(r["counts"]["observations"], 0);
        assert_eq!(r["counts"]["valid_message_updates"], 3);
        assert_eq!(r["counts"]["same_value_valid_updates"], 2);
        assert_eq!(r["counts"]["same_millisecond_received"], 1);
        assert_eq!(r["counts"]["progress_unchanged"], 2);
        assert_eq!(r["counts"]["end_inputs"], 1);
        assert_eq!(r["receive_interval"]["valid_nonnegative_samples"], 2);
        assert_eq!(r["receive_interval"]["quantiles_ms"]["p50"], 0);
        assert_eq!(r["unconfirmed_tail_after_last_receive_ms"], 10);
    }
    #[test]
    fn original_reader_rejects_reachable_corrupt_footer() {
        let temp = tempfile::tempdir().unwrap();
        let mut c = recorded(&temp.path().join("public"));
        let path = c
            .public_root
            .join(&c.prefix.stable_storage_ref)
            .join("captured.parquet");
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() = 0;
        fs::write(&path, bytes).unwrap();
        c.expected_capture_sha256 = hash(&path).unwrap();
        assert!(replay(&c, temp.path()).is_err());
    }
}
