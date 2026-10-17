use super::fixture_clock::TradingTimes;
use super::fixture_multi_market::recorded_two_market_rebate;
use data_pipeline_core::public_observation_core::*;
use std::path::Path;

pub fn recorded_staggered_market_ends(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_two_market_rebate(root, false, true)
}
