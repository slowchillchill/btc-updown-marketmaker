use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::*,
};
use data_pipeline_training::public_recording::*;
use sha2::{Digest, Sha256};
use sim_system::{
    execution::*, finality::*, latency_profile::*, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use super::fixture_execution::{execution_with_evidence};
use super::fixture_input::{core};
use super::fixture_policy::{SUCCESS_POLICY};

pub fn execution_with_minimum_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    cash: &str,
    minimum_shares: &str,
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
        Some(minimum_shares),
        None,
        SimCallerRoleV1::Paper,
    )
}
