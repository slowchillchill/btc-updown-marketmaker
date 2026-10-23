use contracts::*;
use serde::{Deserialize, Serialize};

/// 毫秒阈值全部显式指定；工程示例不是正式交易默认。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHealthConfigV1 {
    pub polymarket_silence_normal_ms: u64,
    pub polymarket_silence_ms: u64,
    pub polymarket_event_delay_normal_ms: u64,
    pub polymarket_event_delay_ms: u64,
    pub chainlink_silence_normal_ms: u64,
    pub chainlink_silence_ms: u64,
    pub chainlink_event_delay_normal_ms: u64,
    pub chainlink_event_delay_ms: u64,
    pub binance_trade_silence_normal_ms: u64,
    pub binance_trade_silence_ms: u64,
    pub binance_trade_event_delay_normal_ms: u64,
    pub binance_trade_event_delay_ms: u64,
    pub binance_depth_silence_normal_ms: u64,
    pub binance_depth_silence_ms: u64,
    pub binance_depth_event_delay_normal_ms: u64,
    pub binance_depth_event_delay_ms: u64,
}
impl SourceHealthConfigV1 {
    /// 原八项仍为 max；normal 与 max 相等或为零均合法。
    pub fn limits(&self, source: contracts::SourceStreamKindV1) -> SourceHealthLimitsV1 {
        use contracts::SourceStreamKindV1 as K;
        let (sn, an, sm, am) = match source {
            K::PolymarketBook => (
                self.polymarket_silence_normal_ms,
                self.polymarket_event_delay_normal_ms,
                self.polymarket_silence_ms,
                self.polymarket_event_delay_ms,
            ),
            K::BinanceAggregateTrade => (
                self.binance_trade_silence_normal_ms,
                self.binance_trade_event_delay_normal_ms,
                self.binance_trade_silence_ms,
                self.binance_trade_event_delay_ms,
            ),
            K::BinanceDepth => (
                self.binance_depth_silence_normal_ms,
                self.binance_depth_event_delay_normal_ms,
                self.binance_depth_silence_ms,
                self.binance_depth_event_delay_ms,
            ),
            _ => (
                self.chainlink_silence_normal_ms,
                self.chainlink_event_delay_normal_ms,
                self.chainlink_silence_ms,
                self.chainlink_event_delay_ms,
            ),
        };
        let ms = |v| i64::try_from(v).unwrap_or(i64::MAX);
        SourceHealthLimitsV1 {
            silence_normal_ms: ms(sn),
            event_normal_ms: ms(an),
            silence_max_ms: ms(sm),
            event_max_ms: ms(am),
        }
    }
    pub fn normal_exceeds_max(&self) -> bool {
        [
            (
                self.polymarket_silence_normal_ms,
                self.polymarket_silence_ms,
            ),
            (
                self.polymarket_event_delay_normal_ms,
                self.polymarket_event_delay_ms,
            ),
            (self.chainlink_silence_normal_ms, self.chainlink_silence_ms),
            (
                self.chainlink_event_delay_normal_ms,
                self.chainlink_event_delay_ms,
            ),
            (
                self.binance_trade_silence_normal_ms,
                self.binance_trade_silence_ms,
            ),
            (
                self.binance_trade_event_delay_normal_ms,
                self.binance_trade_event_delay_ms,
            ),
            (
                self.binance_depth_silence_normal_ms,
                self.binance_depth_silence_ms,
            ),
            (
                self.binance_depth_event_delay_normal_ms,
                self.binance_depth_event_delay_ms,
            ),
        ]
        .iter()
        .any(|(normal, max)| normal > max)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthLimitsV1 {
    pub silence_normal_ms: i64,
    pub event_normal_ms: i64,
    pub silence_max_ms: i64,
    pub event_max_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealthFailureV1 {
    UnknownBusinessProgress,
    ReceiveSilence,
    EventDelay,
    UnknownEventTime,
    TimeInconsistent,
    SynchronizationRequired,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthIssueV1 {
    pub scope: SourceStreamScopeV1,
    pub failure: SourceHealthFailureV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealthStateV1 {
    Normal,
    Elevated,
    Exceeded(SourceHealthFailureV1),
    Unavailable(SourceHealthFailureV1),
}
impl SourceHealthStateV1 {
    pub fn failure(&self) -> Option<SourceHealthFailureV1> {
        match self {
            Self::Exceeded(f) | Self::Unavailable(f) => Some(f.clone()),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthStatusV1 {
    pub scope: SourceStreamScopeV1,
    pub state: SourceHealthStateV1,
}

/// 原来源进展的必要标量；不保存载荷，不新增公共质量或健康状态。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthFactsV1 {
    pub synchronization_required: bool,
    pub availability: MarketDataQuality,
    pub last_valid_message_times: Option<(i64, Option<i64>)>,
}
impl From<&SourceStreamProgressV1> for SourceHealthFactsV1 {
    fn from(p: &SourceStreamProgressV1) -> Self {
        Self {
            synchronization_required: p.synchronization
                == SourceSynchronizationV1::SnapshotRequired
                || p.quality.resync == ResyncQuality::Required,
            availability: p.quality.availability,
            last_valid_message_times: p
                .last_valid_business_message
                .as_ref()
                .map(|e| (e.received_at_utc_ms, e.source_event_at_utc_ms)),
        }
    }
}
/// 参数为原策略已转换的毫秒阈值；顺序、饱和加法及>=边界保持。
pub fn source_health_failure_v1(
    facts: Option<&SourceHealthFactsV1>,
    silence_ms: i64,
    event_delay_ms: i64,
    at: i64,
) -> Option<SourceHealthFailureV1> {
    use SourceHealthFailureV1 as F;
    let Some(f) = facts else {
        return Some(F::UnknownBusinessProgress);
    };
    if f.synchronization_required {
        return Some(F::SynchronizationRequired);
    }
    if !matches!(
        f.availability,
        MarketDataQuality::Fresh | MarketDataQuality::Stale
    ) {
        return Some(F::Unavailable);
    }
    let Some((received, event)) = f.last_valid_message_times else {
        return Some(F::UnknownBusinessProgress);
    };
    if received > at || event.is_some_and(|e| e > received) {
        Some(F::TimeInconsistent)
    } else if at >= received.saturating_add(silence_ms) {
        Some(F::ReceiveSilence)
    } else if let Some(e) = event {
        (at >= e.saturating_add(event_delay_ms)).then_some(F::EventDelay)
    } else {
        Some(F::UnknownEventTime)
    }
}

/// 与研究共用的三态；独立未知/质量异常不混入年龄分布。
pub fn source_health_state_v1(
    facts: Option<&SourceHealthFactsV1>,
    limits: SourceHealthLimitsV1,
    at: i64,
) -> SourceHealthStateV1 {
    // 必要事实未知/独立异常先单列；它们不是已知 S/A 的超限样本。
    if let Some(failure) = source_health_failure_v1(facts, i64::MAX, i64::MAX, at) {
        return SourceHealthStateV1::Unavailable(failure);
    }
    match source_health_failure_v1(facts, limits.silence_max_ms, limits.event_max_ms, at) {
        Some(f @ (SourceHealthFailureV1::ReceiveSilence | SourceHealthFailureV1::EventDelay)) => {
            SourceHealthStateV1::Exceeded(f)
        }
        Some(f) => SourceHealthStateV1::Unavailable(f),
        None => {
            let (received, event) = facts.unwrap().last_valid_message_times.unwrap();
            if at >= received.saturating_add(limits.silence_normal_ms)
                || at >= event.unwrap().saturating_add(limits.event_normal_ms)
            {
                SourceHealthStateV1::Elevated
            } else {
                SourceHealthStateV1::Normal
            }
        }
    }
}
