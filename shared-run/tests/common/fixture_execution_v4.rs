use super::fixture_execution::execution_with_evidence;
use contracts::*;
use data_pipeline_core::public_observation_core::*;
use sim_system::{execution::*, platform_rules::*};
use std::{path::Path, sync::Arc};

pub fn execution_with_identity_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
    run_id: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_evidence::<MeasuredCancellationEvidence>(
        path,
        core,
        policy,
        delay,
        seed,
        cash,
        run_id,
        BusinessProjectionVersionV1::V4,
        None,
        None,
        SimCallerRoleV1::Paper,
    )
}

pub fn execution_with_rules_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_identity_v4(path, core, policy, delay, seed, cash, "shared-combination")
}
