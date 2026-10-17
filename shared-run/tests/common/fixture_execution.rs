use contracts::*;
use data_pipeline_core::public_observation_core::*;
use sha2::{Digest, Sha256};
use sim_system::{
    execution::*, finality::*, latency_profile::*, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{path::Path, sync::Arc};

// 同一真实夹具仅显式区分源投影版本；不再包装一份运行配置。
#[allow(clippy::too_many_arguments)]
pub(super) fn execution_with_evidence<P: MemoryOrderEvidence + 'static>(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
    run_id: &str,
    projection_version: BusinessProjectionVersionV1,
    minimum_shares: Option<&str>,
    fee_rate: Option<&str>,
    caller_role: SimCallerRoleV1,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservation<P>,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = Path::new(file!());
    // workspace 编译的 file! 带包目录；独立 #[path] 消费则相对调用包。
    let source = manifest.join(
        source
            .strip_prefix(manifest.file_name().unwrap())
            .unwrap_or(source),
    );
    let original = source
        .parent()
        .unwrap()
        .join("../../../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let (rule_path, rule_sha) = if delay.is_some() || minimum_shares.is_some() || fee_rate.is_some()
    {
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&original).unwrap()).unwrap();
        if let Some(rate) = fee_rate {
            value["synchronized_platform_rules"]["fee_schedule"]["rate"] = serde_json::json!(rate);
        }
        if let Some(delay) = delay {
            value["documented_platform_rules"]["taker_delay"]["value"]["delay_ms"] =
                serde_json::json!(delay);
        }
        if let Some(minimum) = minimum_shares {
            value["synchronized_platform_rules"]["minimum_order_size_shares"] =
                serde_json::json!(minimum);
        }
        if minimum_shares.is_some() {
            // 新实验显式建立新规则产物，保留原历史快照字节。
            let timing = &mut value["documented_platform_rules"]["gtd_timing"];
            timing["value"]["minimum_lead_seconds"] = serde_json::json!(120);
            timing["evidence"]["category"] = serde_json::json!("OFFICIAL_PLATFORM_RULE");
            timing["evidence"]["source"] =
                serde_json::json!("https://docs.polymarket.com/trading/place-orders#limit-orders");
            timing["evidence"]["version"] = serde_json::json!("POLYMARKET_GTD_EFFECTIVE_EXPIRY_V1");
            timing["evidence"]["reviewed_on_utc_date"] = serde_json::json!("2026-09-26");
            timing["evidence"]["scope"] =
                serde_json::json!("声明截止至少提前180秒，实际截止提前60秒");
            timing["evidence"]["limitation"] =
                serde_json::json!("SIM保留整秒编码、毫秒精确时长比较，不断言服务器时钟精度");
            timing["evidence"]["failure_semantics"] =
                serde_json::json!("编码截止小于ceil(P_ms/1000)+120+60时拒单，等号接受");
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        let output = path.parent().unwrap().join("rules-contract-150.json");
        std::fs::write(&output, &bytes).unwrap();
        (output, format!("{:x}", Sha256::digest(&bytes)))
    } else {
        (
            original,
            "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
        )
    };
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: rule_path,
        expected_file_sha256: Sha256Hex(rule_sha),
        replay_causal_start_utc_ms: i64::MAX,
    })
    .unwrap();
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
        projection_version,
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
