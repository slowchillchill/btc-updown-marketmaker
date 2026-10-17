use super::fixture_material::*;
use backtest::*;
use sim_system::platform_rules::*;
use std::{path::Path, sync::Arc};
pub(crate) fn setup_day(
    root: &Path,
    base: i64,
    utc_day: &str,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    setup_day_with_terminal(root, base, utc_day, None)
}
