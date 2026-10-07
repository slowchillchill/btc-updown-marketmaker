//! 有限直方图累计；不保存全部消息样本或时间区间。
use super::*;
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
}
impl Histogram {
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
#[derive(Default)]
pub struct Distribution {
    pub silence_time_ms: Histogram,
    pub event_age_time_ms: Histogram,
}
pub type DailyDistributions = BTreeMap<i64, BTreeMap<String, Distribution>>;
#[derive(Serialize)]
pub struct Candidate {
    pub name: String,
    pub health: SourceHealthConfigV1,
}
pub fn candidates(fit: &DailyDistributions) -> Result<Vec<Candidate>> {
    let mut bounds = BTreeMap::new();
    for g in ["polymarket", "chainlink", "binance_trade", "binance_depth"] {
        let mut s = Vec::new();
        let mut a = Vec::new();
        for groups in fit.values() {
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
