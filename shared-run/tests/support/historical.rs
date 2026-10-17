use super::fixture_execution::execution_with_evidence;
use super::fixture_policy::SUCCESS_POLICY;
use contracts::*;
use data_pipeline_core::public_observation_core::*;
use sim_system::{execution::*, platform_rules::*};
use std::{path::Path, sync::Arc};

/// 复用原记录、规则、NOE与账户构造，仅选择真实有限历史角色。
pub fn execution_historical_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    seed: String,
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
        seed,
        "100",
        "shared-combination",
        BusinessProjectionVersionV1::V4,
        None,
        None,
        SimCallerRoleV1::HistoricalBacktest,
    )
}
