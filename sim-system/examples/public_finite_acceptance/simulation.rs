//! 显式有限消费组件；不设生产容量或构造交易请求。
use super::recorded_times::RecordedReceivedTimes;
use super::{Config, encoded_digest, write_line};
use contracts::{
    Sha256Hex,
    execution_normalization::{DurableSourceReferenceV1, ImmutableSourceReferenceV1},
};
use data_pipeline_training::public_recording::RecordingResult;
use serde::Deserialize;
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2,
    latency_profile::load_embedded_proxy_latency_profile, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const PROJECTION_VERSION: BusinessProjectionVersionV1 = BusinessProjectionVersionV1::V3;

#[derive(Deserialize)]
struct SimulationConfig {
    data: Config,
    reference_market: ReferenceMarketSpecV1,
    implementation_identity: ImplementationIdentityV1,
    finality_path: PathBuf,
    session: SessionSpecV3,
    finite_in_flight_groups: usize,
    max_group_bytes: usize,
    recording_file_sha256: Sha256Hex,
}
// 本有限入口不发送业务请求，因此实际已交付私有响应前缀始终为零。
// 经济投影仍由已有 NOE 端口消费/持久确认，不伪造耐久确认。
struct Projection(SimV2NoeProjectionPortV2);
impl BusinessProjectionPortV2 for Projection {
    fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
        self.0.try_accept(batch)
    }
    fn try_accept_payload(&mut self, batch: Arc<BusinessProjectionBatchPayloadV1>) -> bool {
        self.0.try_accept_payload(batch)
    }
    fn durable_through(&self, run: &str) -> u64 {
        self.0.durable_through(run)
    }
}
impl PublicProjectionPortV1 for Projection {
    fn delivered_private_feedback(&self, _: &str) -> u64 {
        0
    }
}
type Source = RecordedPublicGroupSourceV1<RecordedReceivedTimes>;
fn source(root: &Path, config: &Config) -> RecordingResult<Source> {
    let rows = config.canonical.decode_batch_rows;
    let bytes = config.canonical.max_decode_batch_bytes;
    Ok(Source::open(
        root,
        rows,
        bytes,
        RecordedReceivedTimes::open(root, rows, bytes)?,
    )?)
}
struct Materials(Source);
impl PublicConsumptionMaterialReaderV1 for Materials {
    fn public_group(
        &mut self,
        reference: &contracts::PublicGroupReferenceV1,
    ) -> Result<Arc<PreparedPublicGroupV1>, SimRunActorErrorV2> {
        for item in self.0.by_ref() {
            let item = item?;
            if item.prepared.reference.group_position == reference.group_position {
                return Ok(item.prepared);
            }
        }
        Err(SimRunActorErrorV2::PublicConsumptionSource(
            "原记录组缺失".into(),
        ))
    }
    fn hidden_bucket(
        &mut self,
        _: &contracts::ClosedHiddenStepBucketV1,
    ) -> Result<Arc<[HiddenExecutionRecordV1]>, SimRunActorErrorV2> {
        Err(SimRunActorErrorV2::PublicConsumptionSource(
            "此公开有限入口没有隐藏桶供料".into(),
        ))
    }
}
pub fn run(config_path: &Path, out: &Path, recording: &Path) -> RecordingResult<()> {
    let config: SimulationConfig = serde_json::from_reader(File::open(config_path)?)?;
    fs::create_dir(out)?;
    fs::copy(config_path, out.join("configuration.json"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rules = runtime.block_on(query_and_seal_current_process_snapshot_v3(
        CurrentProcessSnapshotRequestV1 {
            reference_market: config.reference_market,
            target_path: out.join("process-rule-snapshot.json"),
            implementation_identity: config.implementation_identity.clone(),
        },
    ))?;
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&fs::read(
        &config.finality_path,
    )?)?);
    let mut input = data_pipeline_training::public_recording::PublicRecordingReaderV1::recover(
        recording,
        config.data.canonical.decode_batch_rows,
        config.data.canonical.max_decode_batch_bytes,
    )?;
    let mut markets = Vec::new();
    while let Some(row) = input.next_row()? {
        if let data_pipeline_training::public_recording::PublicStoredRowV1::Canonical {
            record: contracts::CanonicalEventRecordV2::MarketFact(fact),
            ..
        } = row
        {
            if let contracts::CanonicalMarketFactPayloadV1::FixedIdentity { assets } = fact.payload
            {
                markets.push(PlatformMarketV1::try_new(
                    fact.market,
                    assets.try_into().map_err(|_| "固定身份不是两腿")?,
                )?);
            }
        }
    }
    drop(input);
    let scenario = PlatformScenarioV1::try_new(markets)?;
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        market_set_sha256: scenario.market_set_sha256().clone(),
        markets: scenario.market_set().markets().to_vec(),
    };
    let bytes = serde_json::to_vec_pretty(&artifact)?;
    fs::write(out.join("scenario.json"), &bytes)?;
    let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(&bytes)?);
    let latency = load_embedded_proxy_latency_profile()?;
    let opened = RunOpenedV2 {
        projection_version: PROJECTION_VERSION,
        run_id: config.session.run_id.clone(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: config.implementation_identity,
        market_event_source_identity_sha256: config.recording_file_sha256,
    };
    let log = out.join("sim.jsonl");
    let evidence = |position| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!("{}#input={position}", log.display())),
    };
    let projection = Projection(SimV2NoeProjectionPortV2::new(
        out.join("noe"),
        opened.clone(),
        evidence(0),
        evidence(1),
        config.session.creation_request_id.clone(),
        "issue88-finite-consumption".into(),
    ));
    let mut execution = PublicSimExecutionV1::create_new(
        &log,
        PublicRunOpenedV1 {
            projection_version: opened.projection_version,
            run_id: opened.run_id,
            platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
            trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
            platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
            proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
            implementation_identity: opened.implementation_identity,
            market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
        },
        rules,
        finality,
        scenario,
        latency,
        Box::new(projection),
        config.max_group_bytes,
        config.finite_in_flight_groups,
    )?;
    execution.create_session(config.session)?;
    execution.activate_session("issue88-finite-activate".into())?;
    let mut driver = FinitePublicConsumptionV1::new(
        execution,
        source(recording, &config.data)?,
        "issue88-r3-closed-input-received".into(),
    );
    let mut receipts = BufWriter::new(File::create(out.join("receipts.jsonl"))?);
    let mut metrics = BufWriter::new(File::create(out.join("checkpoints.jsonl"))?);
    let mut phases = BufWriter::new(File::create(out.join("phases.jsonl"))?);
    let mut groups = 0;
    loop {
        let next = driver.next_group();
        for phase in driver.drain_measurement_observations() {
            write_line(&mut phases, &phase)?;
        }
        let Some((receipt, cutoff)) = next? else {
            break;
        };
        groups += 1;
        let frames = encoded_digest(&driver.frames())?;
        write_line(
            &mut receipts,
            &serde_json::json!({"receipt": receipt, "cutoff": cutoff, "frames": frames}),
        )?;
        if config
            .data
            .checkpoints
            .contains(&(receipt.group().closed_at_input_position + 1))
        {
            write_line(
                &mut metrics,
                &serde_json::json!({"input_count": receipt.group().closed_at_input_position + 1, "groups": groups, "resources": driver.measurement_resources(), "unmeasured": ["background_log_bytes", "background_stable_sequence", "allocator_capacity", "heap_bytes", "cgroup_memory_peak"]}),
            )?;
        }
    }
    receipts.flush()?;
    metrics.flush()?;
    phases.flush()?;
    serde_json::to_writer_pretty(
        File::create(out.join("input-completion.json"))?,
        &serde_json::json!({"groups": groups, "input_time_path_ended": true}),
    )?;
    let candidate = driver.complete(&mut Materials(source(recording, &config.data)?))?;
    serde_json::to_writer_pretty(
        File::create(out.join("completion.json"))?,
        &serde_json::json!({"groups": groups, "clean_sealed_log": candidate.log_path()}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::*;

    #[test]
    fn finite_projection_accepts_real_account_opening_and_persists_it() {
        let directory = tempfile::tempdir().unwrap();
        // 只读复用已有合法规则夹具；正式入口仍在会话前真实查询当前规则。
        let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/platform-rule-snapshot-v3-gate-b.json"),
            expected_file_sha256: Sha256Hex(
                "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
            ),
            replay_causal_start_utc_ms: 1_787_400_000_000,
        })
        .unwrap();
        let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":1},"failure_overrides":[]}"#).unwrap());
        let market = PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
                clob_market_id: ClobMarketId("3195576".into()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 20_000,
                    end_at_utc_ms: 30_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId("20".into()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".into(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId("3".into()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".into(),
                    },
                },
            ],
        )
        .unwrap();
        let scenario = PlatformScenarioV1::try_new(vec![market]).unwrap();
        let scenario = Arc::new(
            SealedPlatformScenarioV1::decode_and_seal(
                &serde_json::to_vec(&SimulationScenarioArtifactV1 {
                    schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
                    scenario_sha256: scenario.scenario_sha256().clone(),
                    market_set_sha256: scenario.market_set_sha256().clone(),
                    markets: scenario.market_set().markets().to_vec(),
                })
                .unwrap(),
            )
            .unwrap(),
        );
        let latency = load_embedded_proxy_latency_profile().unwrap();
        let opened = RunOpenedV2 {
            projection_version: PROJECTION_VERSION,
            run_id: "finite-projection-test".into(),
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
            market_event_source_identity_sha256: Sha256Hex("d".repeat(64)),
        };
        let reference = DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Location(
                directory.path().join("sim.jsonl").display().to_string(),
            ),
        };
        let projection = SimV2NoeProjectionPortV2::new(
            directory.path().join("noe"),
            opened.clone(),
            reference.clone(),
            reference,
            "opening".into(),
            "finite-test".into(),
        );
        let observation = projection.observation();
        let mut execution = PublicSimExecutionV1::create_new(
            &directory.path().join("sim.jsonl"),
            PublicRunOpenedV1 {
                projection_version: opened.projection_version,
                run_id: opened.run_id.clone(),
                platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
                trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
                platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
                proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
                implementation_identity: opened.implementation_identity,
                market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
            },
            rules,
            finality,
            scenario,
            latency,
            Box::new(Projection(projection)),
            262144,
            4,
        )
        .unwrap();
        execution
            .create_session(SessionSpecV3 {
                run_id: opened.run_id,
                session_id: "session".into(),
                account_id: "account".into(),
                caller_role: SimCallerRoleV1::Paper,
                caller_instance_id: "finite-test".into(),
                creation_request_id: "opening".into(),
                response_bridge_binding_id: "response".into(),
                initial_account_state: InitialAccountStateV2 {
                    available_pusd: ExactDecimal("10".into()),
                },
                session_seed: Sha256Hex("e".repeat(64)),
            })
            .unwrap();
        execution.activate_session("activate".into()).unwrap();
        assert_eq!(observation.result_references().len(), 1);
        assert!(directory.path().join("noe").exists());
    }
}
