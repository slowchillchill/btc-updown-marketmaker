use super::fixture_material::*;
use backtest::*;
use sim_system::platform_rules::*;
use std::{path::Path, sync::Arc};
pub(crate) fn setup(root: &Path) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    setup_day_with_terminal(root, BASE, "2026-08-24", None)
}
