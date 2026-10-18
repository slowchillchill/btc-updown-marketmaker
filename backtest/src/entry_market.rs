use crate::MaterialError;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryMarketScopeV1 {
    #[serde(default)]
    pub start_offset_ms: u64,
    #[serde(default)]
    pub end_margin_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveEntryMarketScopeV1 {
    pub policy: EntryMarketScopeV1,
    pub input_start_utc_ms: i64,
    pub input_end_utc_ms: i64,
    pub market_start_inclusive_utc_ms: i64,
    pub market_start_exclusive_utc_ms: i64,
}
impl EntryMarketScopeV1 {
    pub fn expand(
        &self,
        start: i64,
        end: i64,
    ) -> Result<EffectiveEntryMarketScopeV1, MaterialError> {
        let lower = start
            .checked_add(i64::try_from(self.start_offset_ms)?)
            .ok_or("开仓范围起点溢出")?;
        let upper = end
            .checked_sub(i64::try_from(self.end_margin_ms)?)
            .ok_or("开仓范围终点溢出")?;
        // 空范围自然不允许新配对；不增加最短经营时长或自动移动端点。
        Ok(EffectiveEntryMarketScopeV1 {
            policy: self.clone(),
            input_start_utc_ms: start,
            input_end_utc_ms: end,
            market_start_inclusive_utc_ms: lower,
            market_start_exclusive_utc_ms: upper,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn declared_endpoints_not_first_message_and_no_clock_alignment() {
        let p = EntryMarketScopeV1 {
            start_offset_ms: 300_000,
            end_margin_ms: 0,
        };
        let r = p.expand(1_234, 14_401_234).unwrap();
        assert_eq!(r.market_start_inclusive_utc_ms, 301_234);
        assert_eq!(r.market_start_exclusive_utc_ms, 14_401_234);
        assert!(
            r.market_start_inclusive_utc_ms <= 14_101_234
                && 14_101_234 < r.market_start_exclusive_utc_ms
        );
        assert_eq!(
            EntryMarketScopeV1::default()
                .expand(1234, 5678)
                .unwrap()
                .market_start_inclusive_utc_ms,
            1234
        );
        assert!(p.expand(i64::MAX - 1, i64::MAX).is_err());
    }
}
