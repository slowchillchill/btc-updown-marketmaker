//! 仅显式执行的 MS-REAL-001 真实发布核查；复用既有计时、资源和公开构建入口。

use super::*;
use crate::canonical_run::SourceRowCounts;
use crate::gate_a::{
    CanonicalEventStoreCursor, EventTimeIndexCursor, ReductionReportCursor, VisibilityIndexCursor,
};
use crate::gate_a_publication::tests::immutable_file_snapshot;
use crate::{
    ValidatedPublishedDptObservationV1, build_dpt_observation_measured_build_state,
    build_dpt_observation_quality_report, build_dpt_observation_resource_report_from_state,
    build_dpt_observation_test_evidence_report, decode_dpt_observation_measured_build_state,
    encode_dpt_observation_measured_build_state, encode_dpt_observation_quality_report,
    finalize_and_publish_dpt_observation_candidate, open_published_dpt_observation,
    open_published_gate_a_final,
};
use contracts::{CanonicalEventRecordV1, MarketDataQuality, ObservationComponentId};
use serde_json::json;

const CURRENT_WORKSPACE: &str = "/home/chillchill/btc-updown-marketmaker";
const RUN_PARENT: &str = "/home/chillchill/btc-updown-marketmaker/.local-artifacts/data-pipeline-training/missing-source-support/ms-real-001";
const MANIFEST: &str =
    "/home/chillchill/new-datasets/2026-08-08/typed_raw/utc_day=2026-08-08/manifest.json";
const MANIFEST_SHA256: &str = "1fa227a4b7bc0da2dd1eec0ad7501dbb4f7c96fd43a29b01194f6c34c91156b0";
const CONDITION: &str = "0xfdf68d373e45f59d9bb1ebc776b3a45eaaaf5f6845b52d9f51dae9bb90394169";
const START: i64 = 1_786_162_857_624;
const BUILD_CHILD: &str = "dpt_gate_a_cli::dpt_observation_real_tests::ms_real_001_build_child";

struct RealContext {
    run_root: PathBuf,
    code_tree: PathBuf,
    git_commit: String,
}

fn context() -> AnyResult<RealContext> {
    let run_root = PathBuf::from(env::var("DPT_MS_RUN_ROOT")?);
    let code_tree = PathBuf::from(env::var("DPT_MS_CODE_TREE")?);
    let git_commit = env::var(EXPECTED_GIT_COMMIT_ENV)?;
    assert_eq!(env::current_dir()?, Path::new(CURRENT_WORKSPACE));
    assert_eq!(run_root.parent(), Some(Path::new(RUN_PARENT)));
    assert_eq!(fs::canonicalize(&run_root)?, run_root);
    require_unchanged_git_checkout_at(&code_tree, &git_commit)?;
    Ok(RealContext {
        run_root,
        code_tree,
        git_commit,
    })
}

#[test]
#[ignore = "仅按 #100 批准的固定原始输入、预编译二进制和真实隔离资源环境显式执行"]
fn ms_real_001_build_child() -> AnyResult<()> {
    let context = context()?;
    let manifest = PathBuf::from(MANIFEST);
    assert_eq!(sha256_value(&fs::read(&manifest)?).0, MANIFEST_SHA256);
    println!("完整日清单准入开始：{MANIFEST}");
    let input = freeze_direct_source_input_set(&[manifest])?;
    println!(
        "完整准入通过：input_set={} input_manifests={}",
        input.input_set_identity_sha256(),
        input.input_manifest_set_sha256()
    );
    let stage = context.run_root.join(".observation.candidate");
    let artifacts = stage.join("artifacts");
    let scope = CanonicalRunScope::new(CONDITION, START, START + 1)?;
    let output_config = gate_a_formal_output_config();
    let mut candidate = build_gate_a_candidate(
        &input,
        &scope,
        &artifacts,
        &gate_a_formal_canonical_config(),
        &output_config,
    )?;
    assert_eq!(
        candidate.canonical_metrics.selected_rows,
        SourceRowCounts {
            polymarket_market_metadata: 1,
            ..SourceRowCounts::default()
        }
    );
    let validated = validate_gate_a_candidate(
        &artifacts,
        &context.run_root.join("candidate-validation"),
        &output_config,
    )?;
    candidate
        .resource_metrics
        .peak_formal_work_directory_bytes_bound =
        formal_work_directory_peak(&candidate, &validated)?;
    assert_eq!(validated.event_time_index.manifest().file.rows, 0);
    assert_eq!(validated.peak_observation_l2_levels, 0);
    let quality = build_dpt_observation_quality_report(&input, &candidate, &validated)?;
    let quality_bytes = encode_dpt_observation_quality_report(&quality)?;
    write_new_synced(
        &stage.join("reports/quality-isolation-report.json"),
        &quality_bytes,
    )?;
    let state = build_dpt_observation_measured_build_state(
        &candidate,
        sha256_value(&quality_bytes),
        regular_directory_bytes(&artifacts)?,
        context.git_commit.clone(),
    )?;
    input.verify_unchanged()?;
    assert_eq!(sha256_value(&fs::read(MANIFEST)?).0, MANIFEST_SHA256);
    require_unchanged_git_checkout_at(&context.code_tree, &context.git_commit)?;
    write_new_synced(
        &context.run_root.join("measured-build-state.json"),
        &encode_dpt_observation_measured_build_state(&state)?,
    )?;
    println!(
        "构建与结束完整准入复核通过：{}",
        serde_json::to_string(
            &json!({"selected_rows": quality.selected_source_rows, "artifact_rows": quality.artifact_rows, "scope": quality.scope, "input_set_identity_sha256": quality.input_set_identity_sha256, "input_manifest_set_sha256": quality.input_manifest_set_sha256})
        )?
    );
    Ok(())
}

#[test]
#[ignore = "仅按 #100 使用新鲜 dpt-gate-a 用户服务、bwrap、2GiB 且零交换空间显式执行"]
fn ms_real_001_measured_publication() -> AnyResult<()> {
    let context = context()?;
    let network = require_formal_network_and_environment_isolation()?;
    let cgroup = CgroupFacts::current()?;
    cgroup.require_formal_limits(network.supervisor_process_id)?;
    let stage = context.run_root.join(".observation.candidate");
    fs::create_dir(&stage)?;
    fs::create_dir(stage.join("reports"))?;
    let filesystem = publication_filesystem_facts_at(&context.run_root, &stage)?;
    let executable = env::current_exe()?;
    let arguments = [
        BUILD_CHILD,
        "--exact",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]
    .map(OsString::from);
    let time_path = context.run_root.join("time-v.txt");
    let mut command = timed_child_command(
        Path::new(CURRENT_WORKSPACE),
        &executable,
        &arguments,
        &time_path,
        &context.git_commit,
    );
    let argv = std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    write_new_synced(
        &context.run_root.join("timed-command.json"),
        &serde_json::to_vec(
            &json!({"argv": argv, "working_directory": CURRENT_WORKSPACE, "git_commit": context.git_commit}),
        )?,
    )?;
    let measured = measure_child(&mut command, &cgroup, network.supervisor_process_id)?;
    let child_output = combined_output(&measured.output);
    write_new_synced(
        &context.run_root.join("build-child-output.txt"),
        child_output.as_bytes(),
    )?;
    println!("{child_output}");
    if !measured.output.status.success() {
        return Err("固定真实案例构建失败；保留候选及原始输出，不签发成功".into());
    }
    require_unchanged_git_checkout_at(&context.code_tree, &context.git_commit)?;
    let filesystem_after = publication_filesystem_facts_at(&context.run_root, &stage)?;
    assert_eq!(filesystem.filesystem_type, filesystem_after.filesystem_type);
    assert_eq!(
        filesystem.publication_root_device_id,
        filesystem_after.publication_root_device_id
    );
    assert_eq!(
        filesystem.candidate_device_id,
        filesystem_after.candidate_device_id
    );
    let state = decode_dpt_observation_measured_build_state(&read_regular_bounded(
        &context.run_root.join("measured-build-state.json"),
        4 * 1024 * 1024,
    )?)?;
    assert_eq!(state.git_commit, context.git_commit);
    assert_eq!(
        fs::canonicalize(&state.candidate_path)?,
        fs::canonicalize(stage.join("artifacts"))?
    );
    let time_output = String::from_utf8(read_regular_bounded(&time_path, 1024 * 1024)?)?;
    let external = measured_resource_facts(&measured, &cgroup, filesystem, network, time_output)?;
    let resource =
        build_dpt_observation_resource_report_from_state(&state, collect_host_facts()?, external)?;
    write_new_synced(
        &context.run_root.join("measured-resource-report.json"),
        &encode_resource_high_water_report(&resource)?,
    )?;
    assert!(
        resource.evidence.passed,
        "真实资源未通过原冻结界限：{resource:#?}"
    );
    write_new_synced(
        &stage.join("reports/resource-evidence.json"),
        &encode_resource_high_water_report(&resource)?,
    )?;
    let commands_path = PathBuf::from(env::var("DPT_MS_TEST_COMMANDS")?);
    assert!(commands_path.starts_with(&context.run_root));
    let commands: Vec<GateATestCommandEvidenceV1> =
        serde_json::from_slice(&read_regular_bounded(&commands_path, 16 * 1024 * 1024)?)?;
    let tests = build_dpt_observation_test_evidence_report(
        context.git_commit.clone(),
        state.artifact_manifests.clone(),
        commands,
    )?;
    let published = finalize_and_publish_dpt_observation_candidate(&stage, &tests)?;
    let before = immutable_file_snapshot(&published.root);
    let opened = open_published_dpt_observation(
        &published.root,
        &context.run_root.join("published-validation"),
    )?;
    assert_eq!(opened.receipt(), &published.receipt);
    assert!(!opened.receipt().training_dataset_ready);
    assert_eq!(opened.run_manifest().git_commit, context.git_commit);
    assert_eq!(
        opened.producer_pipeline_core_identity_sha256(),
        &pipeline_core_identity_sha256()
    );
    assert_eq!(
        opened.reader_pipeline_core_identity_sha256(),
        &pipeline_core_identity_sha256()
    );
    assert!(
        open_published_gate_a_final(
            &published.root,
            &context.run_root.join("wrong-purpose-validation")
        )
        .is_err()
    );
    read_all_published_facts(&opened, &context.run_root)?;
    assert_eq!(before, immutable_file_snapshot(&published.root));
    write_new_synced(
        &context.run_root.join("published-file-sha256.json"),
        &serde_json::to_vec(&before)?,
    )?;
    require_unchanged_git_checkout_at(&context.code_tree, &context.git_commit)?;
    println!(
        "真实发布及只读重开通过：root={} receipt={} rss_kib={} cgroup_peak_bytes={}",
        published.root.display(),
        published.receipt.receipt_id.0,
        resource.external.maximum_resident_set_size_kib,
        resource.external.memory_peak_bytes
    );
    Ok(())
}

fn read_all_published_facts(
    opened: &ValidatedPublishedDptObservationV1,
    run_root: &Path,
) -> AnyResult<()> {
    let candidate = opened.candidate();
    let mut events = CanonicalEventStoreCursor::open(
        candidate.canonical_event_store.manifest_path(),
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut event_index = EventTimeIndexCursor::open(
        candidate.event_time_index.manifest_path(),
        None,
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut visibility = VisibilityIndexCursor::open(
        candidate.visibility_index.manifest_path(),
        None,
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut observations = MarketObservationCursor::open(
        candidate.market_observations.manifest_path(),
        None,
        None,
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut reductions = ReductionReportCursor::open(
        candidate.reduction_reports.manifest_path(),
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut facts = BTreeMap::new();
    let mut records = Vec::new();
    loop {
        let page = events.next_page(1)?;
        if page.is_empty() {
            break;
        }
        for record in page {
            let time = match &record {
                CanonicalEventRecordV1::ExecutionMarketEvent(event) => &event.time,
                CanonicalEventRecordV1::ReferenceEvent(event) => &event.time,
            };
            assert_eq!(time.received_at_utc_ms, START);
            assert_eq!(time.source_event_at_utc_ms, None);
            records.push(serde_json::to_value(record)?);
        }
    }
    assert_eq!(
        records.len() as u64,
        candidate.canonical_event_store.manifest().file.rows
    );
    facts.insert("canonical_records", records);
    assert!(event_index.next_page(1)?.is_empty());
    facts.insert("event_time_index", Vec::new());
    let mut rows = Vec::new();
    loop {
        let page = visibility.next_page(1)?;
        if page.is_empty() {
            break;
        }
        rows.extend(
            page.into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    assert_eq!(
        rows.len() as u64,
        candidate.visibility_index.manifest().file.rows
    );
    facts.insert("visibility_index", rows);
    let mut rows = Vec::new();
    loop {
        let page = observations.next_page(1)?;
        if page.is_empty() {
            break;
        }
        for row in page {
            assert_eq!(row.visibility_cutoff.received_at_utc_ms, START);
            for id in [
                ObservationComponentId::PolymarketL2,
                ObservationComponentId::BestBidAsk,
                ObservationComponentId::ChainlinkBtcUsd,
                ObservationComponentId::BinanceBtcUsdtDepth,
                ObservationComponentId::BinanceBtcUsdtTrade,
            ] {
                let state = row
                    .component_states
                    .iter()
                    .find(|state| state.component == id)
                    .unwrap();
                assert_eq!(state.quality, MarketDataQuality::UnknownNotObserved);
                assert_eq!(state.last_visible_event_id, None);
                assert_eq!(state.received_at_utc_ms, None);
                assert_eq!(state.source_event_at_utc_ms, None);
                assert!(row.missing_mask.contains(&id));
            }
            rows.push(serde_json::to_value(row)?);
        }
    }
    assert_eq!(
        rows.len() as u64,
        candidate.market_observations.manifest().file.rows
    );
    facts.insert("observations", rows);
    let mut rows = Vec::new();
    loop {
        let page = reductions.next_page(1)?;
        if page.is_empty() {
            break;
        }
        rows.extend(
            page.into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    assert_eq!(
        rows.len() as u64,
        candidate.reduction_reports.manifest().file.rows
    );
    facts.insert("reduction_reports", rows);
    write_new_synced(
        &run_root.join("published-readback-facts.json"),
        &serde_json::to_vec(&facts)?,
    )?;
    Ok(())
}
