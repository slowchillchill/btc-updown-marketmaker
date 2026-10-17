use super::fixture_clock::TradingTimes;
use super::fixture_trading::recorded_trading_with_tail;
use data_pipeline_core::public_observation_core::*;
use std::path::Path;

pub fn recorded_settlement_pending_loser(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_tail(root, "6", None, 60_000, 40, "6")
}
