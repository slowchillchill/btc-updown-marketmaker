#[path = "fixture_material.rs"]
mod fixture_material;
use backtest::*;
use data_collect::{
    polymarket_market_metadata_layout::PolymarketMarketMetadataV5Row,
    polymarket_orderbook_v3::PolymarketOrderbookV3Message,
};
pub(crate) use fixture_material::*;
use sim_system::platform_rules::*;
use std::{path::Path, sync::Arc};
pub(crate) fn setup_with_terminal(
    root: &Path,
    metadata: PolymarketMarketMetadataV5Row,
    pm: PolymarketOrderbookV3Message,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    setup_day_with_terminal(root, BASE, "2026-08-24", Some((metadata, pm)))
}
