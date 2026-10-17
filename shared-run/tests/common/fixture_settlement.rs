use super::fixture_clock::TradingTimes;
use super::fixture_trading::recorded_trading_with_tail;
use data_pipeline_core::public_observation_core::*;
use std::path::Path;

pub fn recorded_settlement(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    // 覆盖显式 2Δ 顺序的 S3 最后成功及费用支付，不改变任何原代理延迟。
    recorded_trading_with_tail(root, "5", None, 60_000, 80, "100")
}
