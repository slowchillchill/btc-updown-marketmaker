use super::fixture_execution::execution_with_evidence;
use super::fixture_policy::SUCCESS_POLICY;
use contracts::*;
use data_pipeline_core::public_observation_core::*;
use sim_system::{execution::*, platform_rules::*};
use std::{path::Path, sync::Arc};

pub fn execution_with_fee_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    cash: &str,
    fee_rate: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_evidence::<MeasuredCancellationEvidence>(
        path,
        core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        cash,
        "shared-combination",
        BusinessProjectionVersionV1::V4,
        None,
        Some(fee_rate),
        SimCallerRoleV1::Paper,
    )
}
