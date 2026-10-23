use contracts::*;
use data_pipeline_core::public_observation_core::*;
use sim_system::{
    execution::*, finality::*, latency_profile::*, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{path::Path, sync::Arc};
pub(super) fn execution(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    rules: Arc<SealedProcessRuleSnapshotV3>,
    cash: &str,
    run_id: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    type P = MeasuredCancellationEvidence;
    let caller_role = SimCallerRoleV1::HistoricalBacktest;
    let seed = "e".repeat(64);
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(policy).unwrap());
    let scenario = PlatformScenarioV1::try_new(
        core.fixed_markets()
            .map(|market| {
                let c = core.pm_context(&market.condition_id.0).unwrap();
                PlatformMarketV1::try_new(
                    c.market,
                    c.assets
                        .into_iter()
                        .map(|a| a.asset)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        markets: scenario.market_set().markets().to_vec(),
    };
    let scenario = Arc::new(
        SealedPlatformScenarioV1::decode_and_seal(&serde_json::to_vec(&artifact).unwrap()).unwrap(),
    );
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        latency_sampler_version: "xoshiro256plusplus_jump_multiply_high_empirical_rank_v2".into(),
        run_id: run_id.into(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        },
        market_event_source_identity_sha256: Some(Sha256Hex("d".repeat(64))),
    };
    let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
        source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
            path.display().to_string(),
        ),
    };
    let port = MemoryNoeProjectionPort::<P>::new(
        path.parent().unwrap().join("noe"),
        opened.clone(),
        reference.clone(),
        reference,
        "create".into(),
        "shared-test".into(),
    );
    let observation = port.observation();
    let spec = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id.clone(),
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let create: fn(_, _, _, _, _, _, _, _, _) -> _ =
        if caller_role == SimCallerRoleV1::HistoricalBacktest {
            PublicSimExecutionV1::create_new_historical
        } else {
            PublicSimExecutionV1::create_new
        };
    let mut run = create(
        path,
        spec,
        rules.clone(),
        finality,
        scenario,
        latency,
        Box::new(port),
        2 << 20,
        128,
    )
    .unwrap();
    run.create_session(SessionSpecV3 {
        run_id: opened.run_id,
        session_id: "shared-session".into(),
        account_id: "shared-account".into(),
        caller_role,
        caller_instance_id: "shared-caller".into(),
        creation_request_id: "create".into(),
        response_bridge_binding_id: "shared-bridge".into(),
        initial_account_state: InitialAccountStateV2 {
            available_pusd: ExactDecimal(cash.into()),
        },
        session_seed: Sha256Hex(seed),
    })
    .unwrap();
    run.activate_session("activate".into()).unwrap();
    (run, observation, rules)
}
