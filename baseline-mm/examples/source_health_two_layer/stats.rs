//! 有限直方图累计；不保存全部消息样本或时间区间。
use super::*;
use compact::{Material, Row, SELECT};
pub const DAY: i64 = 86_400_000;
fn bucket(v: u64) -> (u64, u64) {
    if v >= DAY as u64 {
        let days = v / (DAY as u64);
        let lower = (DAY as u64) * (1u64 << (63 - days.leading_zeros()));
        (lower, lower)
    } else {
        let width = if v < 60_000 { 100 } else { 1000 };
        (v / width * width, width)
    }
}
#[derive(Default, Clone, Serialize)]
pub struct Histogram {
    pub bins: BTreeMap<u64, u64>,
    pub weight: u64,
}
impl Histogram {
    fn point(&mut self, v: u64, n: u64) {
        let b = bucket(v).0;
        *self.bins.entry(b).or_default() += n;
        self.weight += n;
    }
    fn ramp(&mut self, origin: i64, mut a: i64, b: i64) {
        while a < b {
            let v = (a - origin) as u64;
            let (key, width) = bucket(v);
            let dt = (b - a).min((key + width - v) as i64) as u64;
            *self.bins.entry(key).or_default() += dt;
            self.weight += dt;
            a += dt as i64;
        }
    }
    pub fn quantile(&self, p: u64, upper: Option<u64>) -> Option<u64> {
        let total: u64 = self
            .bins
            .iter()
            .filter(|(k, _)| upper.is_none_or(|u| **k <= u))
            .map(|(_, v)| v)
            .sum();
        if total == 0 {
            return None;
        }
        let target = (total * p).div_ceil(100).max(1);
        let mut n = 0;
        for (k, v) in &self.bins {
            if upper.is_some_and(|u| *k > u) {
                break;
            }
            n += v;
            if n >= target {
                return Some(k.saturating_add(bucket(*k).1));
            }
        }
        None
    }
    pub fn body_normal(&self) -> Option<u64> {
        let q1 = self.quantile(25, None)?;
        let q3 = self.quantile(75, None)?;
        let fence = q3.saturating_add((q3 - q1) * 3 / 2);
        self.quantile(90, Some(fence))
    }
}
#[derive(Default, Serialize)]
pub struct Distribution {
    pub silence_time_ms: Histogram,
    pub event_age_time_ms: Histogram,
    pub arrival_delay_samples: Histogram,
    pub exclusions_ms: BTreeMap<String, u64>,
    pub unknown_context_ms: BTreeMap<String, u64>,
    pub excluded_arrival_samples: BTreeMap<String, u64>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub name: String,
    pub health: SourceHealthConfigV1,
}
#[derive(Default, Serialize)]
pub struct StateTotals {
    pub duration_ms: BTreeMap<String, u64>,
    pub transitions: u64,
    pub exceeded_episodes: u64,
    pub longest_exceeded_ms: u64,
    pub exceeded_episode_duration_ms: Histogram,
    pub unfinished_exceeded_at_end: u64,
}
#[derive(Default, Serialize)]
pub struct Report {
    pub start: i64,
    pub end: i64,
    pub groups: BTreeMap<String, Distribution>,
    pub source_distributions: BTreeMap<String, Distribution>,
    pub daily: BTreeMap<i64, BTreeMap<String, Distribution>>,
    pub candidate_states: BTreeMap<String, BTreeMap<String, StateTotals>>,
    pub candidates: Vec<Candidate>,
}
#[derive(Default)]
struct StateRun {
    label: String,
    exceeded_ms: u64,
}
struct Current {
    at: i64,
    facts: Option<SourceHealthFactsV1>,
    flags: u8,
    runs: Vec<StateRun>,
}
pub fn group(k: SourceStreamKindV1) -> &'static str {
    match k {
        SourceStreamKindV1::PolymarketBook => "polymarket",
        SourceStreamKindV1::BinanceAggregateTrade => "binance_trade",
        SourceStreamKindV1::BinanceDepth => "binance_depth",
        _ => "chainlink",
    }
}
fn selected(k: SourceStreamKindV1, mask: u8) -> bool {
    match k {
        SourceStreamKindV1::PolymarketBook => true,
        SourceStreamKindV1::BinanceAggregateTrade => mask & 1 != 0,
        SourceStreamKindV1::BinanceDepth => mask & 2 != 0,
        SourceStreamKindV1::ChainlinkSpot => mask & 4 != 0,
        SourceStreamKindV1::ChainlinkTwapSixty => mask & 8 != 0,
        _ => false,
    }
}
fn independent(f: Option<&SourceHealthFactsV1>, at: i64) -> Option<SourceHealthFailureV1> {
    source_health_failure_v1(f, i64::MAX, i64::MAX, at)
}
fn label(s: &SourceHealthStateV1) -> String {
    match s {
        SourceHealthStateV1::Normal => "normal".into(),
        SourceHealthStateV1::Elevated => "elevated".into(),
        SourceHealthStateV1::Exceeded(_) => "exceeded".into(),
        SourceHealthStateV1::Unavailable(f) => format!("unavailable/{f:?}"),
    }
}
fn state_interval(
    totals: &mut StateTotals,
    run: &mut StateRun,
    f: Option<&SourceHealthFactsV1>,
    h: SourceHealthLimitsV1,
    a: i64,
    b: i64,
) {
    let mut cuts = vec![a, b];
    if let Some((recv, event)) = f.and_then(|f| f.last_valid_message_times) {
        cuts.push(recv);
        cuts.extend([
            recv.saturating_add(h.silence_normal_ms),
            recv.saturating_add(h.silence_max_ms),
        ]);
        if let Some(e) = event {
            cuts.extend([
                e.saturating_add(h.event_normal_ms),
                e.saturating_add(h.event_max_ms),
            ]);
        }
    }
    cuts.retain(|t| *t >= a && *t <= b);
    cuts.sort_unstable();
    cuts.dedup();
    for w in cuts.windows(2) {
        let name = label(&source_health_state_v1(f, h, w[0]));
        let dt = (w[1] - w[0]) as u64;
        if name != run.label {
            if !run.label.is_empty() {
                totals.transitions += 1;
            }
            if run.label == "exceeded" {
                totals.longest_exceeded_ms = totals.longest_exceeded_ms.max(run.exceeded_ms);
                totals
                    .exceeded_episode_duration_ms
                    .point(run.exceeded_ms, 1);
                run.exceeded_ms = 0;
            }
            if name == "exceeded" {
                totals.exceeded_episodes += 1;
            }
            run.label = name.clone();
        }
        *totals.duration_ms.entry(name.clone()).or_default() += dt;
        if name == "exceeded" {
            run.exceeded_ms += dt;
        }
    }
}
fn add_distribution(
    d: &mut Distribution,
    f: Option<&SourceHealthFactsV1>,
    flags: u8,
    a: i64,
    b: i64,
) {
    if let Some((received, _)) = f.and_then(|f| f.last_valid_message_times)
        && received > a
        && received < b
    {
        add_distribution(d, f, flags, a, received);
        add_distribution(d, f, flags, received, b);
        return;
    }
    for (bit, label) in [
        (32, "synchronization_unknown"),
        (64, "connection_generation_unknown"),
    ] {
        if flags & bit != 0 {
            *d.unknown_context_ms.entry(label.into()).or_default() += (b - a) as u64;
        }
    }
    if let Some(reason) = independent(f, a) {
        *d.exclusions_ms.entry(format!("{reason:?}")).or_default() += (b - a) as u64;
        return;
    }
    let (recv, event) = f.unwrap().last_valid_message_times.unwrap();
    d.silence_time_ms.ramp(recv, a, b);
    d.event_age_time_ms.ramp(event.unwrap(), a, b);
}
struct Runner<'a> {
    m: &'a Material,
    current: Vec<Current>,
    common_ids: Vec<usize>,
    mask: u8,
    report: Report,
}
impl Runner<'_> {
    fn interval(&mut self, id: usize, to: i64) {
        let spec = &self.m.scopes[id];
        let k = spec.scope.source;
        let c = &mut self.current[id];
        let previous = c.at;
        c.at = to;
        if k == SourceStreamKindV1::ChainlinkTwapThirty {
            return;
        }
        let w = spec.window.as_ref();
        let mut a = previous
            .max(self.report.start)
            .max(w.map_or(i64::MIN, |w| w.start_at_utc_ms));
        let b = to
            .min(self.report.end)
            .min(w.map_or(i64::MAX, |w| w.end_at_utc_ms));
        let g = group(k).to_owned();
        while a < b {
            let day = a.div_euclid(DAY) * DAY;
            let end = b.min(day + DAY);
            add_distribution(
                self.report
                    .source_distributions
                    .entry(format!("{k:?}"))
                    .or_default(),
                c.facts.as_ref(),
                c.flags,
                a,
                end,
            );
            if !selected(k, self.mask) {
                a = end;
                continue;
            }
            add_distribution(
                self.report.groups.entry(g.clone()).or_default(),
                c.facts.as_ref(),
                c.flags,
                a,
                end,
            );
            add_distribution(
                self.report
                    .daily
                    .entry(day)
                    .or_default()
                    .entry(g.clone())
                    .or_default(),
                c.facts.as_ref(),
                c.flags,
                a,
                end,
            );
            for (i, candidate) in self.report.candidates.iter().enumerate() {
                let t = self
                    .report
                    .candidate_states
                    .entry(candidate.name.clone())
                    .or_default()
                    .entry(g.clone())
                    .or_default();
                state_interval(
                    t,
                    &mut c.runs[i],
                    c.facts.as_ref(),
                    candidate.health.limits(k),
                    a,
                    end,
                );
            }
            a = end;
        }
    }
    fn row(&mut self, r: Row) -> Result<()> {
        if r.scope == SELECT {
            // 只有共同来源，固定最多四项；不逐消息扫描全部市场。
            let ids = self.common_ids.clone();
            for i in ids {
                self.interval(i, r.at);
                let k = self.m.scopes[i].scope.source;
                if selected(k, self.mask) && !selected(k, r.selected) {
                    for (j, run) in self.current[i].runs.iter_mut().enumerate() {
                        if run.label == "exceeded" {
                            let t = self
                                .report
                                .candidate_states
                                .entry(self.report.candidates[j].name.clone())
                                .or_default()
                                .entry(group(k).into())
                                .or_default();
                            t.longest_exceeded_ms = t.longest_exceeded_ms.max(run.exceeded_ms);
                            t.exceeded_episode_duration_ms.point(run.exceeded_ms, 1);
                            t.unfinished_exceeded_at_end += 1;
                        }
                        run.label.clear();
                        run.exceeded_ms = 0;
                    }
                }
            }
            self.mask = r.selected;
            return Ok(());
        }
        let id = r.scope as usize;
        self.interval(id, r.at);
        self.current[id].facts = r.facts();
        self.current[id].flags = r.flags;
        let spec = &self.m.scopes[id];
        let in_window = spec
            .window
            .as_ref()
            .is_none_or(|w| r.at >= w.start_at_utc_ms && r.at < w.end_at_utc_ms);
        if r.samples > 0
            && r.at >= self.report.start
            && r.at < self.report.end
            && in_window
            && spec.scope.source != SourceStreamKindV1::ChainlinkTwapThirty
        {
            let g = group(spec.scope.source).to_owned();
            let day = r.at.div_euclid(DAY) * DAY;
            let f = r.facts();
            let failure = independent(f.as_ref(), r.at);
            let own = self
                .report
                .source_distributions
                .entry(format!("{:?}", spec.scope.source))
                .or_default();
            if let Some(reason) = &failure {
                *own.excluded_arrival_samples
                    .entry(format!("{reason:?}"))
                    .or_default() += r.samples as u64;
            } else {
                own.arrival_delay_samples
                    .point((r.received - r.event) as u64, r.samples as u64);
            }
            if !selected(spec.scope.source, self.mask) {
                return Ok(());
            }
            for d in [
                self.report.groups.entry(g.clone()).or_default(),
                self.report
                    .daily
                    .entry(day)
                    .or_default()
                    .entry(g)
                    .or_default(),
            ] {
                if let Some(reason) = &failure {
                    *d.excluded_arrival_samples
                        .entry(format!("{reason:?}"))
                        .or_default() += r.samples as u64;
                } else {
                    d.arrival_delay_samples
                        .point((r.received - r.event) as u64, r.samples as u64);
                }
            }
        }
        Ok(())
    }
}
pub fn analyze(
    path: &Path,
    m: &Material,
    start: i64,
    end: i64,
    candidates: Vec<Candidate>,
    progress_seconds: u64,
) -> Result<Report> {
    let mut runner = Runner {
        m,
        common_ids: m
            .scopes
            .iter()
            .enumerate()
            .filter(|(_, s)| s.scope.source != SourceStreamKindV1::PolymarketBook)
            .map(|(i, _)| i)
            .collect(),
        current: m
            .scopes
            .iter()
            .map(|_| Current {
                at: m.start,
                facts: None,
                flags: 0,
                runs: (0..candidates.len()).map(|_| StateRun::default()).collect(),
            })
            .collect(),
        mask: 0,
        report: Report {
            start,
            end,
            candidates,
            ..Default::default()
        },
    };
    let begin = Instant::now();
    let mut last = Instant::now();
    let mut n = 0u64;
    compact::read_until(path, m, Some(end), |r| {
        n += 1;
        runner.row(r)?;
        if progress_seconds > 0 && last.elapsed().as_secs() >= progress_seconds {
            eprintln!(
                "stage=scalar-analysis records={n}/{} elapsed_s={:.1} range=[{start},{end}) rss={}",
                m.records,
                begin.elapsed().as_secs_f64(),
                rss()
            );
            last = Instant::now();
        }
        Ok(())
    })?;
    for i in 0..runner.current.len() {
        runner.interval(i, end);
    }
    for (id, c) in runner.current.iter().enumerate() {
        for (i, run) in c.runs.iter().enumerate() {
            if run.label == "exceeded" {
                let t = runner
                    .report
                    .candidate_states
                    .entry(runner.report.candidates[i].name.clone())
                    .or_default()
                    .entry(group(m.scopes[id].scope.source).into())
                    .or_default();
                t.longest_exceeded_ms = t.longest_exceeded_ms.max(run.exceeded_ms);
                t.exceeded_episode_duration_ms.point(run.exceeded_ms, 1);
                t.unfinished_exceeded_at_end += 1;
            }
        }
    }
    Ok(runner.report)
}
pub fn candidates(fit: &Report) -> Result<Vec<Candidate>> {
    let mut bounds = BTreeMap::new();
    for g in ["polymarket", "chainlink", "binance_trade", "binance_depth"] {
        let mut s = Vec::new();
        let mut a = Vec::new();
        for groups in fit.daily.values() {
            if let Some(d) = groups.get(g)
                && let (Some(x), Some(y)) = (
                    d.silence_time_ms.body_normal(),
                    d.event_age_time_ms.body_normal(),
                )
            {
                s.push(x);
                a.push(y);
            }
        }
        if s.is_empty() {
            return Err(format!("fit group {g} has no eligible daily body distribution").into());
        }
        s.sort_unstable();
        a.sort_unstable();
        bounds.insert(g, (s[(s.len() - 1) / 2], a[(a.len() - 1) / 2]));
    }
    let mut candidates = Vec::new();
    for multiplier in [2u64, 4, 8] {
        let mut h: SourceHealthConfigV1 = SourceHealthConfigV1 {
            polymarket_silence_normal_ms: 0,
            polymarket_event_delay_normal_ms: 0,
            polymarket_silence_ms: 0,
            polymarket_event_delay_ms: 0,
            chainlink_silence_normal_ms: 0,
            chainlink_event_delay_normal_ms: 0,
            chainlink_silence_ms: 0,
            chainlink_event_delay_ms: 0,
            binance_trade_silence_normal_ms: 0,
            binance_trade_event_delay_normal_ms: 0,
            binance_trade_silence_ms: 0,
            binance_trade_event_delay_ms: 0,
            binance_depth_silence_normal_ms: 0,
            binance_depth_event_delay_normal_ms: 0,
            binance_depth_silence_ms: 0,
            binance_depth_event_delay_ms: 0,
        };
        macro_rules! set {
            ($g:literal,$sn:ident,$an:ident,$sm:ident,$am:ident) => {
                let (s, a) = bounds[$g];
                h.$sn = s;
                h.$an = a;
                h.$sm = s.saturating_mul(multiplier);
                h.$am = a.saturating_mul(multiplier);
            };
        }
        set!(
            "polymarket",
            polymarket_silence_normal_ms,
            polymarket_event_delay_normal_ms,
            polymarket_silence_ms,
            polymarket_event_delay_ms
        );
        set!(
            "chainlink",
            chainlink_silence_normal_ms,
            chainlink_event_delay_normal_ms,
            chainlink_silence_ms,
            chainlink_event_delay_ms
        );
        set!(
            "binance_trade",
            binance_trade_silence_normal_ms,
            binance_trade_event_delay_normal_ms,
            binance_trade_silence_ms,
            binance_trade_event_delay_ms
        );
        set!(
            "binance_depth",
            binance_depth_silence_normal_ms,
            binance_depth_event_delay_normal_ms,
            binance_depth_silence_ms,
            binance_depth_event_delay_ms
        );
        candidates.push(Candidate {
            name: format!("daily-body90-max{multiplier}x"),
            health: h,
        });
    }
    Ok(candidates)
}
