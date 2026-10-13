//! 显式有限消费组件；不设生产容量或构造交易请求。
#[path = "feeding.rs"]
mod feeding;
use super::recorded_times::RecordedReceivedTimes;
use super::{Config, encoded_digest, write_line};
use contracts::{
    Sha256Hex,
    execution_normalization::{DurableSourceReferenceV1, ImmutableSourceReferenceV1},
};
use data_pipeline_training::{
    hidden_preparation::HiddenPreparationV1, public_recording::RecordingResult,
};
use feeding::feed;
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
    #[serde(default)]
    hidden_bucket_checkpoints: Vec<u64>,
}
// 无业务请求的有限入口不会实际领取私有反馈，交付前缀保持零。
// NOE 接纳交给真实内存核心，耐久由同一 SIM 后台所有者在 Finish 发布。
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
pub fn run(
    config_path: &Path,
    out: &Path,
    recording: &Path,
    hidden_reference: Option<&Path>,
) -> RecordingResult<()> {
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
        market_event_source_identity_sha256: Some(config.recording_file_sha256),
    };
    let log = out.join("sim.jsonl");
    let evidence = |position| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!("{}#input={position}", log.display())),
    };
    let projection = MemoryNoeProjectionPortV2::new(
        out.join("noe"),
        opened.clone(),
        evidence(0),
        evidence(1),
        config.session.creation_request_id.clone(),
        "issue88-finite-consumption".into(),
    );
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
    feed(
        &mut execution,
        |execution| execution.create_session(config.session.clone()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )?;
    feed(
        &mut execution,
        |execution| execution.activate_session("issue88-finite-activate".into()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )?;
    if let Some(reference) = hidden_reference {
        return run_hidden(
            execution,
            &config.data,
            &config.hidden_bucket_checkpoints,
            out,
            recording,
            reference,
        );
    }
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
        let next = feed(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        );
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
    let candidate = driver.complete()?;
    serde_json::to_writer_pretty(
        File::create(out.join("completion.json"))?,
        &serde_json::json!({"groups": groups, "clean_sealed_log": candidate.log_path()}),
    )?;
    Ok(())
}

// 只验证既有隐藏步骤被真实执行器按原前量接纳；不构造公开时序或完整运行封口。
fn run_hidden(
    execution: PublicSimExecutionV1,
    config: &Config,
    checkpoints: &[u64],
    out: &Path,
    recording: &Path,
    reference_path: &Path,
) -> RecordingResult<()> {
    let prepared: HiddenPreparationV1 = serde_json::from_reader(File::open(reference_path)?)?;
    fs::copy(reference_path, out.join("hidden-reference.json"))?;
    let mut buckets = RecordedHiddenBucketSourceV1::open(
        &prepared,
        recording,
        config.output.sort.batch_rows,
        config.canonical.max_decode_batch_bytes,
    )?;
    // 空公开迭代器仅复用已有隐藏调用和只读计量；从不调用 next_group/complete。
    let mut driver = FinitePublicConsumptionV1::new(
        execution,
        std::iter::empty(),
        "issue88-hidden-mechanism-without-public-consumption".into(),
    );
    let mut applied = BufWriter::new(File::create(out.join("hidden-applied.jsonl"))?);
    let mut metrics = BufWriter::new(File::create(out.join("checkpoints.jsonl"))?);
    let mut phases = BufWriter::new(File::create(out.join("phases.jsonl"))?);
    let (mut count, mut members, mut peak_members, mut peak_encoded_bytes) = (0_u64, 0, 0, 0);
    let mut last_reference = None;
    while let Some(bucket) = buckets.next_bucket()? {
        let reference = bucket.reference().clone();
        let member_count = bucket.records().len();
        // 克隆只共享原桶 Arc；实际前量校对成功返回后才进行独立验收编码。
        feed(
            &mut driver,
            |driver| driver.hidden_bucket(bucket.clone()),
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )?;
        let material = encoded_digest(&bucket.records())?;
        count += 1;
        members += member_count;
        peak_members = peak_members.max(member_count);
        peak_encoded_bytes = peak_encoded_bytes.max(material.1);
        write_line(
            &mut applied,
            &serde_json::json!({"applied_bucket": count, "bucket": reference, "material": material}),
        )?;
        for phase in driver.drain_measurement_observations() {
            write_line(&mut phases, &phase)?;
        }
        if checkpoints.contains(&count) {
            write_line(
                &mut metrics,
                &serde_json::json!({"applied_buckets": count, "applied_index_members": members,
                    "resources": driver.measurement_resources(), "read_metrics": buckets.read_metrics(),
                    "unmeasured": ["background_log_bytes", "background_stable_sequence", "allocator_capacity", "heap_bytes", "cgroup_memory_peak"]}),
            )?;
        }
        last_reference = Some(reference);
    }
    applied.flush()?;
    metrics.flush()?;
    phases.flush()?;
    serde_json::to_writer_pretty(
        File::create(out.join("hidden-input-completion.json"))?,
        &serde_json::json!({"applied_buckets": count, "applied_index_members": members,
            "last_applied_bucket": last_reference, "hidden_source_exhausted": true,
            "peak_bucket_index_members": peak_members, "peak_bucket_encoded_bytes": peak_encoded_bytes,
            "resources": driver.measurement_resources(), "read_metrics": buckets.read_metrics(),
            "unexercised": ["public_consumption", "clean_run_completion", "background_completion", "durable_completion"],
            "unmeasured": ["background_log_bytes", "background_stable_sequence", "allocator_capacity", "heap_bytes", "cgroup_memory_peak", "file_open_count", "row_group_reads"]}),
    )?;
    // 最后一桶接纳只证明已应用；原后台与 CleanRun 完成门保持，不把未封口日志冒作稳定产物。
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::*;
    use data_pipeline_core::typed_raw::{BinanceAggTradeRecord, BinanceAggTradeRow};

    fn offline_execution(directory: &Path, role: SimCallerRoleV1) -> PublicSimExecutionV1 {
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
            market_event_source_identity_sha256: Some(Sha256Hex("d".repeat(64))),
        };
        let reference = DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Location(
                directory.join("sim.jsonl").display().to_string(),
            ),
        };
        let projection = MemoryNoeProjectionPortV2::new(
            directory.join("noe"),
            opened.clone(),
            reference.clone(),
            reference,
            "opening".into(),
            "finite-test".into(),
        );
        let observation = projection.observation();
        let mut execution = PublicSimExecutionV1::create_new(
            &directory.join("sim.jsonl"),
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
            Box::new(projection),
            262144,
            4,
        )
        .unwrap();
        execution
            .create_session(SessionSpecV3 {
                run_id: opened.run_id,
                session_id: "session".into(),
                account_id: "account".into(),
                caller_role: role,
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
        assert_eq!(observation.accepted_through(), 1);
        assert!(
            observation
                .result_references()
                .expect("读取已接纳结果引用")
                .is_empty(),
            "Finish 前没有耐久引用"
        );
        execution
    }

    #[test]
    fn finite_projection_accepts_real_account_opening_without_premature_durability() {
        let directory = tempfile::tempdir().unwrap();
        let _execution = offline_execution(directory.path(), SimCallerRoleV1::Paper);
    }

    #[test]
    fn hidden_entry_records_applied_prefix_and_preserves_original_failures() {
        use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
        use data_pipeline_training::{hidden_preparation::prepare_hidden_v1, public_recording::*};
        let directory = tempfile::tempdir().unwrap();
        let recording = directory.path().join("recording");
        let header = PublicInputStreamHeaderV1 {
            input_stream_id: "core-test".into(),
            canonical_rule_version: "rule-v1".into(),
            observation_version: "public-observation-v1".into(),
            ordering_version: "original-input-v1".into(),
            input_scope: "constructed-finite".into(),
            max_pending_ws_terminals: 2,
            initial_state: PublicInitialStateV1::Empty,
        };
        let mut output = data_pipeline_training::gate_a_formal_output_config();
        output.sort.batch_rows = 2;
        output.sort.max_run_rows = 4;
        output.sort.max_run_bytes = 1 << 20;
        output.max_artifact_payload_bytes = 1 << 19;
        let mut core = PublicObservationCoreV1::new(header.clone()).unwrap();
        let mut writer =
            PublicRecordingWriterV1::create(&recording, &header, output.clone()).unwrap();
        // 复用原Binance原域对照的小成交夹具，经真实核心/记录/隐藏准备生成非空参考桶。
        let inputs = (1..=2)
            .map(|sequence| {
                let row = TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(
                    sequence, sequence, "64000",
                )));
                let source_evidence =
                    data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(
                        &row,
                    )
                    .unwrap();
                DptIngressV1::Message {
                    row,
                    source_evidence,
                }
            })
            .chain(std::iter::once(DptIngressV1::EndOfInput));
        for (position, ingress) in inputs.enumerate() {
            let input = RecordedIngressV1 {
                input_stream_id: header.input_stream_id.clone(),
                input_position: position as u64,
                ingress,
                dependency_record_refs: vec![],
            };
            let prepared =
                prepare_public_ingress_v1(&core, &input, &std::collections::BTreeMap::new(), &[])
                    .unwrap();
            let result = core.push(&prepared).unwrap().into_owned();
            writer.record(input, result).unwrap();
        }
        writer.stabilize().unwrap();
        drop(writer);
        let hidden = prepare_hidden_v1(
            core,
            PublicRecordingReaderV1::recover(&recording, 1, 1 << 20).unwrap(),
            &directory.path().join("hidden"),
            &output,
            "hidden-test".into(),
        )
        .unwrap();
        let reference = directory.path().join("reference.json");
        fs::write(&reference, serde_json::to_vec(&hidden).unwrap()).unwrap();
        let config = Config {
            files: vec![],
            manifest_sha256: Sha256Hex("a".repeat(64)),
            received_start: 0,
            received_end: 4000,
            condition: None,
            omit_initial_books_epoch: None,
            canonical: data_pipeline_training::gate_a_formal_canonical_config(),
            output,
            header,
            checkpoints: vec![],
            binance_witness_positions: vec![],
        };
        let mut source = RecordedHiddenBucketSourceV1::open(
            &hidden,
            &recording,
            2,
            config.canonical.max_decode_batch_bytes,
        )
        .unwrap();
        let bucket = source.next_bucket().unwrap().unwrap();
        assert!(!bucket.records().is_empty());
        assert!(matches!(
            &bucket.records()[0],
            HiddenExecutionRecordV1::Reference(_)
        ));
        let material = encoded_digest(&bucket.records()).unwrap();
        assert!(!source.next_bucket().unwrap().unwrap().records().is_empty());
        assert!(source.next_bucket().unwrap().is_none());
        for role in [SimCallerRoleV1::ModelTrain, SimCallerRoleV1::Paper] {
            let out = directory.path().join(format!("{role:?}"));
            fs::create_dir(&out).unwrap();
            let execution = offline_execution(&out, role);
            let result = run_hidden(execution, &config, &[1], &out, &recording, &reference);
            if role == SimCallerRoleV1::ModelTrain {
                // 首桶已应用；末桶结束时仍须遵守原市场初始加载门。
                assert!(format!("{:?}", result.unwrap_err()).contains("IncompleteInitialLoad"));
                let applied: serde_json::Value = serde_json::from_str(
                    &fs::read_to_string(out.join("hidden-applied.jsonl")).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    applied["bucket"],
                    serde_json::to_value(bucket.reference()).unwrap()
                );
                assert_eq!(
                    applied["material"],
                    serde_json::to_value(material.clone()).unwrap()
                );
            } else {
                assert!(
                    format!("{:?}", result.unwrap_err()).contains("HiddenPlatformReplayForbidden")
                );
                assert!(!out.join("hidden-input-completion.json").exists());
                assert!(
                    fs::read(out.join("hidden-applied.jsonl"))
                        .unwrap()
                        .is_empty()
                );
            }
            assert!(!out.join("hidden-input-completion.json").exists());
            assert!(!out.join("completion.json").exists());
        }
    }

    fn trade(aggregate_trade_id: u64, admission_sequence: u64, price: &str) -> BinanceAggTradeRow {
        let event_time = 2800 + admission_sequence as i64;
        let trade_time = 2700 + admission_sequence as i64;
        BinanceAggTradeRow {
            timestamp_received: 3_000 + admission_sequence as i64,
            rest_backfill_timestamp_received: None,
            ingest_source: "websocket".to_owned(),
            rest_was_best_price_match: None,
            source_native_payload_json: format!(
                r#"{{"e":"aggTrade","E":{event_time},"T":{trade_time},"s":"BTCUSDT","a":{aggregate_trade_id},"p":"{price}"}}"#
            ),
            source_sequence_epoch_id: "aggtrade-epoch-1".to_owned(),
            connection_generation: 1,
            websocket_ingress_sequence: admission_sequence,
            rest_response_ingress_sequence: None,
            rest_response_item_index: None,
            collector_admission_sequence: admission_sequence,
            collector_admission_time_utc_ms: 3_100 + admission_sequence as i64,
            collector_host_id: "collector-1".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".to_owned(),
            schema_version: "binance_aggtrade_v7".to_owned(),
            record: BinanceAggTradeRecord::Trade {
                event_type: "aggTrade".to_owned(),
                event_time,
                trade_time,
                symbol: "BTCUSDT".to_owned(),
                aggregate_trade_id,
                price: price.to_owned(),
                quantity: "0.01".to_owned(),
                first_trade_id: aggregate_trade_id * 10,
                last_trade_id: aggregate_trade_id * 10 + 1,
                buyer_is_maker: false,
                stream: "btcusdt@aggTrade".to_owned(),
                ws_ignore: false,
            },
        }
    }
}
