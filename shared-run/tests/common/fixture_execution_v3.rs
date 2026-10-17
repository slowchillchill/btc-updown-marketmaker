use super::fixture_execution::execution_with_evidence;
use contracts::*;
use data_pipeline_core::public_observation_core::*;
use sim_system::execution::*;
use std::path::Path;

pub fn execution(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    execution_with_seed(path, core, policy, None, "e".repeat(64))
}

pub fn execution_with_seed(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    execution_with_seed_and_cash(path, core, policy, delay, seed, "100")
}

pub fn execution_with_seed_and_cash(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    let (run, observation, _) = execution_with_evidence::<MeasuredOrderEvidence>(
        path,
        core,
        policy,
        delay,
        seed,
        cash,
        "shared-combination",
        BusinessProjectionVersionV1::V3,
        None,
        None,
        SimCallerRoleV1::Paper,
    );
    (run, observation)
}
