use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::{self, Command, Output};

use crate::canonical_run::{CanonicalRunDigests, CanonicalRunInputFile, CanonicalRunScope};
use crate::freeze_direct_source_input_set;
use crate::gate_a::{
    GATE_A_CONTROL_FILE_BYTES_BOUND, GateACandidate, MarketObservationCursor,
    ValidatedGateACandidate, build_gate_a_candidate, pipeline_core_identity_sha256,
    validate_gate_a_candidate, workspace_pipeline_core_identity_sha256,
};
use crate::gate_a_publication::{
    GATE_A_CGROUP_MEMORY_MAX_BYTES, GATE_A_WORKSPACE_TEST_ARGV, GATE_A_WORKSPACE_TEST_EVIDENCE_IDS,
    GateACgroupMemoryEventsV1, GateAExternalResourceFactsV1, GateAHostFactsV1,
    GateATestCommandEvidenceV1, build_measured_build_state, build_quality_isolation_report,
    build_resource_high_water_report_from_state, build_test_evidence_report,
    decode_measured_build_state, decode_resource_high_water_report, encode_measured_build_state,
    encode_quality_isolation_report, encode_resource_high_water_report,
    finalize_and_publish_gate_a_candidate, gate_a_formal_canonical_config,
    gate_a_formal_output_config, gate_a_perturbation_configs, regular_directory_bytes,
};
use contracts::{MarketObservationCausalShellV3, Sha256Hex};
use serde::Serialize;
use sha2::{Digest, Sha256};

const WORKSPACE: &str = "/home/chillchill/sim-system";
const PUBLICATION_ROOT: &str =
    "/home/chillchill/sim-system/.local-artifacts/data-pipeline-training/gate-a";
const BUILD_STATE_FILE: &str = ".measured-build-state.json";
const PHYSICAL_COMMAND_FILE: &str = ".physical-perturbation-command.json";
const TIME_V_FILE: &str = ".time-v.txt";
const EXPECTED_GIT_COMMIT_ENV: &str = "DPT_GATE_A_EXPECTED_GIT_COMMIT";

type AnyResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug)]
struct BuildArgs {
    manifest: PathBuf,
    condition_id: String,
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
    candidate_dir: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CandidateDeterminismSummary {
    input_set_identity_sha256: Sha256Hex,
    input_manifest_set_sha256: Sha256Hex,
    scope: CanonicalRunScope,
    selected_files: Vec<CanonicalRunInputFile>,
    canonical_digests: CanonicalRunDigests,
    recomputed_stream_digests: CanonicalRunDigests,
    artifact_facts: [(u64, Sha256Hex); 4],
    observation_rows: u64,
    observation_semantics_sha256: Sha256Hex,
}

#[derive(Clone, Debug)]
struct PublicationFilesystemFacts {
    filesystem_type: String,
    publication_root_device_id: u64,
    candidate_device_id: u64,
}

#[derive(Clone, Debug)]
struct NetworkIsolationFacts {
    network_namespace: String,
    interfaces: Vec<String>,
    supervisor_process_id: u32,
    supervisor_name: String,
    supervisor_argv: Vec<String>,
}

#[derive(Serialize)]
struct ObservationDeterminismMaterial<'a> {
    schema_version: &'a str,
    market: &'a contracts::MarketIdentityV2,
    canonical_event_record_contract_version: &'a str,
    execution_event_contract_version: &'a str,
    reference_event_contract_version: &'a str,
    input_set_identity_sha256: &'a Sha256Hex,
    contributing_event_ids: &'a [contracts::CanonicalEventId],
    visibility_cutoff: &'a contracts::ObservationVisibilityCutoffV2,
    market_state_digest_sha256: &'a Sha256Hex,
    component_states: &'a [contracts::ObservationComponentStateV2],
    missing_mask: &'a [contracts::ObservationComponentId],
}

struct ExactCandidateCleanup {
    path: PathBuf,
    retain: bool,
}

impl ExactCandidateCleanup {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            retain: false,
        }
    }
}

impl Drop for ExactCandidateCleanup {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

pub(crate) fn main() {
    if let Err(error) = run() {
        eprintln!("dpt-gate-a 失败：{error}");
        std::process::exit(1);
    }
}

fn run() -> AnyResult<()> {
    let mut args = env::args_os();
    let _program = args.next();
    let command = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or("缺少命令；只支持 measured-build 或 publish")?;
    let remaining = args.collect::<Vec<_>>();
    match command.as_str() {
        "measured-build" => run_measured_build(parse_build_args(&remaining)?),
        "publish" => run_publish(parse_publish_path(&remaining)?),
        "__build-child" => run_build_child(parse_build_args(&remaining)?),
        _ => Err(format!("未知命令：{command}").into()),
    }
}

fn parse_build_args(arguments: &[OsString]) -> AnyResult<BuildArgs> {
    let flags = parse_exact_flags(
        arguments,
        &[
            "--manifest",
            "--condition-id",
            "--received-start-utc-ms",
            "--received-end-utc-ms",
            "--candidate-dir",
        ],
    )?;
    let args = BuildArgs {
        manifest: PathBuf::from(required_flag(&flags, "--manifest")?),
        condition_id: required_flag(&flags, "--condition-id")?.to_owned(),
        received_start_utc_ms: required_flag(&flags, "--received-start-utc-ms")?.parse()?,
        received_end_utc_ms: required_flag(&flags, "--received-end-utc-ms")?.parse()?,
        candidate_dir: PathBuf::from(required_flag(&flags, "--candidate-dir")?),
    };
    validate_candidate_path(&args.candidate_dir)?;
    CanonicalRunScope::new(
        args.condition_id.clone(),
        args.received_start_utc_ms,
        args.received_end_utc_ms,
    )?;
    Ok(args)
}

fn parse_publish_path(arguments: &[OsString]) -> AnyResult<PathBuf> {
    let flags = parse_exact_flags(arguments, &["--candidate-dir"])?;
    let path = PathBuf::from(required_flag(&flags, "--candidate-dir")?);
    validate_candidate_path(&path)?;
    Ok(path)
}

fn parse_exact_flags(
    arguments: &[OsString],
    allowed: &[&str],
) -> AnyResult<BTreeMap<String, String>> {
    if !arguments.len().is_multiple_of(2) {
        return Err("参数必须由固定的 --flag value 对组成".into());
    }
    let mut parsed = BTreeMap::new();
    for pair in arguments.chunks_exact(2) {
        let flag = pair[0].to_str().ok_or("参数名必须是 UTF-8")?;
        if !allowed.contains(&flag) {
            return Err(format!("不支持的参数：{flag}").into());
        }
        let value = pair[1].to_str().ok_or("参数值必须是 UTF-8")?;
        if value.is_empty() || parsed.insert(flag.to_owned(), value.to_owned()).is_some() {
            return Err(format!("参数为空或重复：{flag}").into());
        }
    }
    if parsed.len() != allowed.len() {
        return Err("缺少固定参数".into());
    }
    Ok(parsed)
}

fn required_flag<'a>(flags: &'a BTreeMap<String, String>, key: &str) -> AnyResult<&'a str> {
    flags
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| format!("缺少参数：{key}").into())
}

fn validate_candidate_path(path: &Path) -> AnyResult<()> {
    if !path.is_absolute()
        || path.parent() != Some(Path::new(PUBLICATION_ROOT))
        || path
            .file_name()
            .and_then(|value| value.to_str())
            .is_none_or(|name| !name.starts_with('.') || !name.ends_with(".candidate"))
        || path
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err("candidate-dir 必须是冻结发布根下的隐藏直接子目录".into());
    }
    Ok(())
}

fn run_measured_build(args: BuildArgs) -> AnyResult<()> {
    let git_commit = require_clean_git_checkout()?;
    let network = require_formal_network_and_environment_isolation()?;
    let cgroup = CgroupFacts::current()?;
    cgroup.require_formal_limits(network.supervisor_process_id)?;
    fs::create_dir_all(PUBLICATION_ROOT)?;
    fs::create_dir(&args.candidate_dir)?;
    let mut cleanup = ExactCandidateCleanup::new(args.candidate_dir.clone());
    fs::create_dir(args.candidate_dir.join("reports"))?;
    let filesystem = publication_filesystem_facts(&args.candidate_dir)?;

    let events_before = cgroup.memory_events()?;
    let process_ids_before = cgroup.process_ids()?;
    let current_exe = env::current_exe()?;
    let time_path = args.candidate_dir.join(TIME_V_FILE);
    let child_arguments = child_arguments(&args);
    let mut command = Command::new("/usr/bin/time");
    command
        .current_dir(WORKSPACE)
        .env("LC_ALL", "C")
        .env(EXPECTED_GIT_COMMIT_ENV, &git_commit)
        .arg("-v")
        .arg("-o")
        .arg(&time_path)
        .arg(&current_exe)
        .args(&child_arguments);
    let output = command.output()?;
    let events_after = cgroup.memory_events()?;
    let process_ids_after = cgroup.process_ids()?;
    cgroup.require_formal_process_tree(&process_ids_after, network.supervisor_process_id)?;
    require_unchanged_git_checkout(&git_commit)?;
    let filesystem_after = publication_filesystem_facts(&args.candidate_dir)?;
    if filesystem_after.filesystem_type != filesystem.filesystem_type
        || filesystem_after.publication_root_device_id != filesystem.publication_root_device_id
        || filesystem_after.candidate_device_id != filesystem.candidate_device_id
    {
        return Err("计时构建期间发布文件系统身份发生变化".into());
    }
    let mut combined_output = combined_output(&output);
    if !output.status.success() {
        eprint!("{combined_output}");
        return Err("计时构建子进程失败".into());
    }

    let state_bytes =
        read_regular_bounded(&args.candidate_dir.join(BUILD_STATE_FILE), 4 * 1024 * 1024)?;
    let state = decode_measured_build_state(&state_bytes)?;
    if state.git_commit != git_commit {
        return Err("计时构建状态未绑定构建开始时冻结的 Git 提交".into());
    }
    let expected_artifacts = fs::canonicalize(args.candidate_dir.join("artifacts"))?;
    if fs::canonicalize(&state.candidate_path)? != expected_artifacts {
        return Err("计时构建状态未绑定本次 artifacts 目录".into());
    }
    let time_v_output = String::from_utf8(read_regular_bounded(&time_path, 1024 * 1024)?)?;
    let exit_code = output.status.code().ok_or("计时构建没有普通退出码")?;
    let external = GateAExternalResourceFactsV1 {
        build_exit_code: exit_code,
        maximum_resident_set_size_kib: parse_time_value(
            &time_v_output,
            "Maximum resident set size (kbytes):",
        )?,
        publication_filesystem_type: filesystem.filesystem_type,
        publication_root_device_id: filesystem.publication_root_device_id,
        candidate_device_id: filesystem.candidate_device_id,
        cgroup_v2_relative_path: cgroup.relative_path.clone(),
        cgroup_process_ids_before: process_ids_before,
        cgroup_process_ids_after: process_ids_after,
        memory_peak_bytes: cgroup.read_number("memory.peak")?,
        memory_max_bytes: cgroup.read_number("memory.max")?,
        memory_swap_peak_bytes: cgroup.read_number("memory.swap.peak")?,
        memory_swap_max_bytes: cgroup.read_number("memory.swap.max")?,
        memory_events_before: events_before,
        memory_events_after: events_after,
        network_namespace: network.network_namespace,
        network_interfaces: network.interfaces,
        network_isolation_supervisor_pid: network.supervisor_process_id,
        network_isolation_supervisor_name: network.supervisor_name,
        network_isolation_supervisor_argv: network.supervisor_argv,
        time_v_output_sha256: sha256_value(time_v_output.as_bytes()),
        time_v_output,
    };
    let resource =
        build_resource_high_water_report_from_state(&state, collect_host_facts()?, external)?;
    if !resource.evidence.passed {
        return Err("正式资源高水位超过冻结门".into());
    }
    combined_output
        .push_str("\nruntime_network_isolation=PASS filesystem=ext4 cgroup_isolation=PASS\n");
    let resource_bytes = encode_resource_high_water_report(&resource)?;
    write_new_synced(
        &args.candidate_dir.join("reports/resource-evidence.json"),
        &resource_bytes,
    )?;

    let mut recorded_argv = vec![
        "/usr/bin/time".to_owned(),
        "-v".to_owned(),
        "-o".to_owned(),
        time_path.to_string_lossy().into_owned(),
        current_exe.to_string_lossy().into_owned(),
    ];
    recorded_argv.extend(
        child_arguments
            .iter()
            .map(|value| value.to_string_lossy().into_owned()),
    );
    let physical_command = GateATestCommandEvidenceV1 {
        argv: recorded_argv,
        working_directory: WORKSPACE.to_owned(),
        exit_code,
        combined_output_sha256: sha256_value(combined_output.as_bytes()),
        combined_output,
        covered_evidence_ids: vec![
            "test/offline-boundary".to_owned(),
            "test/physical-perturbation".to_owned(),
        ],
    };
    write_new_synced(
        &args.candidate_dir.join(PHYSICAL_COMMAND_FILE),
        &serde_json::to_vec(&physical_command)?,
    )?;
    fs::remove_file(time_path)?;
    sync_directory(&args.candidate_dir.join("reports"))?;
    sync_directory(&args.candidate_dir)?;
    cleanup.retain = true;
    println!(
        "measured_build=PASS candidate={} rss_kib={} cgroup_peak_bytes={}",
        args.candidate_dir.display(),
        resource.external.maximum_resident_set_size_kib,
        resource.external.memory_peak_bytes
    );
    Ok(())
}

fn run_build_child(args: BuildArgs) -> AnyResult<()> {
    let git_commit =
        env::var(EXPECTED_GIT_COMMIT_ENV).map_err(|_| "计时构建子进程缺少冻结 Git 提交")?;
    require_unchanged_git_checkout(&git_commit)?;
    let input = freeze_direct_source_input_set(std::slice::from_ref(&args.manifest))?;
    let scope = CanonicalRunScope::new(
        args.condition_id,
        args.received_start_utc_ms,
        args.received_end_utc_ms,
    )?;
    let (perturbation_config, perturbation_output) = gate_a_perturbation_configs();
    let perturbation_path = args.candidate_dir.join(".perturbation-artifacts");
    let perturbation = build_gate_a_candidate(
        &input,
        &scope,
        &perturbation_path,
        &perturbation_config,
        &perturbation_output,
    )?;
    let perturbation_validated = validate_gate_a_candidate(
        &perturbation_path,
        &args.candidate_dir.join(".perturbation-validation"),
        &perturbation_output,
    )?;
    let perturbation_peak_formal_work_directory_bytes_bound =
        formal_work_directory_peak(&perturbation, &perturbation_validated)?;
    let perturbation_summary = candidate_determinism_summary(
        &perturbation,
        &perturbation_validated,
        &perturbation_output,
    )?;
    fs::remove_dir_all(&perturbation_path)?;

    let primary_config = gate_a_formal_canonical_config();
    let primary_output = gate_a_formal_output_config();
    let artifacts = args.candidate_dir.join("artifacts");
    let mut primary =
        build_gate_a_candidate(&input, &scope, &artifacts, &primary_config, &primary_output)?;
    let primary_validated = validate_gate_a_candidate(
        &artifacts,
        &args.candidate_dir.join(".primary-validation"),
        &primary_output,
    )?;
    let primary_peak_formal_work_directory_bytes_bound =
        formal_work_directory_peak(&primary, &primary_validated)?;
    primary
        .resource_metrics
        .peak_formal_work_directory_bytes_bound =
        perturbation_peak_formal_work_directory_bytes_bound
            .max(primary_peak_formal_work_directory_bytes_bound);
    let primary_summary =
        candidate_determinism_summary(&primary, &primary_validated, &primary_output)?;
    require_same_logical_candidate(&primary_summary, &perturbation_summary)?;

    let quality = build_quality_isolation_report(&primary, &primary_validated)?;
    let quality_bytes = encode_quality_isolation_report(&quality)?;
    let quality_sha256 = sha256_value(&quality_bytes);
    write_new_synced(
        &args
            .candidate_dir
            .join("reports/quality-isolation-report.json"),
        &quality_bytes,
    )?;
    let artifact_directory_bytes = regular_directory_bytes(&artifacts)?;
    require_unchanged_git_checkout(&git_commit)?;
    let state = build_measured_build_state(
        &primary,
        quality_sha256,
        artifact_directory_bytes,
        git_commit,
    )?;
    write_new_synced(
        &args.candidate_dir.join(BUILD_STATE_FILE),
        &encode_measured_build_state(&state)?,
    )?;
    println!(
        "physical_perturbation=PASS canonical_records={} observations={}",
        primary.canonical_event_store.manifest.file.rows,
        primary.market_observations.manifest.file.rows
    );
    Ok(())
}

fn run_publish(candidate_dir: PathBuf) -> AnyResult<()> {
    let state_bytes = read_regular_bounded(&candidate_dir.join(BUILD_STATE_FILE), 4 * 1024 * 1024)?;
    let state = decode_measured_build_state(&state_bytes)?;
    let git_commit = require_clean_git_checkout()?;
    if git_commit != state.git_commit {
        return Err("发布提交与计时构建冻结提交不一致".into());
    }
    let expected_artifacts = fs::canonicalize(candidate_dir.join("artifacts"))?;
    if fs::canonicalize(&state.candidate_path)? != expected_artifacts {
        return Err("发布候选不是计时构建状态绑定的 artifacts 目录".into());
    }
    let network = require_formal_network_and_environment_isolation()?;
    let cgroup = CgroupFacts::current()?;
    cgroup.require_formal_limits(network.supervisor_process_id)?;
    let filesystem = publication_filesystem_facts(&candidate_dir)?;
    let resource_bytes = read_regular_bounded(
        &candidate_dir.join("reports/resource-evidence.json"),
        4 * 1024 * 1024,
    )?;
    let resource = decode_resource_high_water_report(&resource_bytes)?;
    if resource.external.publication_filesystem_type != filesystem.filesystem_type
        || resource.external.publication_root_device_id != filesystem.publication_root_device_id
        || resource.external.candidate_device_id != filesystem.candidate_device_id
    {
        return Err("发布文件系统不是计时构建资源报告绑定的 ext4 设备".into());
    }
    let mut cleanup = ExactCandidateCleanup::new(candidate_dir.clone());
    let physical_bytes =
        read_regular_bounded(&candidate_dir.join(PHYSICAL_COMMAND_FILE), 4 * 1024 * 1024)?;
    let physical: GateATestCommandEvidenceV1 = serde_json::from_slice(&physical_bytes)?;

    let output = Command::new("cargo")
        .current_dir(WORKSPACE)
        .args(&GATE_A_WORKSPACE_TEST_ARGV[1..])
        .output()?;
    cgroup.require_formal_process_tree(&cgroup.process_ids()?, network.supervisor_process_id)?;
    require_unchanged_git_checkout(&git_commit)?;
    let workspace_output = combined_output(&output);
    let workspace_command = GateATestCommandEvidenceV1 {
        argv: GATE_A_WORKSPACE_TEST_ARGV
            .into_iter()
            .map(str::to_owned)
            .collect(),
        working_directory: WORKSPACE.to_owned(),
        exit_code: output.status.code().ok_or("cargo test 没有普通退出码")?,
        combined_output_sha256: sha256_value(workspace_output.as_bytes()),
        combined_output: workspace_output,
        covered_evidence_ids: GATE_A_WORKSPACE_TEST_EVIDENCE_IDS
            .into_iter()
            .map(str::to_owned)
            .collect(),
    };
    let artifact_manifests = read_quality_artifact_bindings(&candidate_dir)?;
    let tests = build_test_evidence_report(
        git_commit.clone(),
        artifact_manifests,
        vec![physical, workspace_command],
    )?;
    require_unchanged_git_checkout(&git_commit)?;
    fs::remove_file(candidate_dir.join(PHYSICAL_COMMAND_FILE))?;
    fs::remove_file(candidate_dir.join(BUILD_STATE_FILE))?;
    publication_filesystem_facts(&candidate_dir)?;
    require_unchanged_git_checkout(&git_commit)?;
    let published = finalize_and_publish_gate_a_candidate(&candidate_dir, &tests)?;
    cleanup.retain = true;
    println!(
        "gate_a_publication=PASS receipt_id={} root={}",
        published.receipt.receipt_id.0,
        published.root.display()
    );
    Ok(())
}

fn child_arguments(args: &BuildArgs) -> Vec<OsString> {
    vec![
        OsString::from("__build-child"),
        OsString::from("--manifest"),
        args.manifest.as_os_str().to_owned(),
        OsString::from("--condition-id"),
        OsString::from(&args.condition_id),
        OsString::from("--received-start-utc-ms"),
        OsString::from(args.received_start_utc_ms.to_string()),
        OsString::from("--received-end-utc-ms"),
        OsString::from(args.received_end_utc_ms.to_string()),
        OsString::from("--candidate-dir"),
        args.candidate_dir.as_os_str().to_owned(),
    ]
}

fn candidate_determinism_summary(
    primary: &GateACandidate,
    validated: &ValidatedGateACandidate,
    output: &crate::gate_a::GateAOutputConfig,
) -> AnyResult<CandidateDeterminismSummary> {
    let artifact_fact = |artifact: &crate::gate_a::GateAArtifactDescriptor| {
        (
            artifact.manifest.file.rows,
            artifact.manifest.file.logical_sha256.clone(),
        )
    };
    let (observation_rows, observation_semantics_sha256) =
        observation_semantics_digest(primary, output)?;
    Ok(CandidateDeterminismSummary {
        input_set_identity_sha256: primary.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: primary.input_manifest_set_sha256.clone(),
        scope: primary.scope.clone(),
        selected_files: primary.selected_files.clone(),
        canonical_digests: primary.canonical_digests.clone(),
        recomputed_stream_digests: validated.recomputed_stream_digests.clone(),
        artifact_facts: [
            artifact_fact(&primary.canonical_event_store),
            artifact_fact(&primary.event_time_index),
            artifact_fact(&primary.visibility_index),
            artifact_fact(&primary.reduction_reports),
        ],
        observation_rows,
        observation_semantics_sha256,
    })
}

fn formal_work_directory_peak(
    candidate: &GateACandidate,
    validated: &ValidatedGateACandidate,
) -> AnyResult<u64> {
    let build_bound = candidate
        .resource_metrics
        .peak_candidate_directory_bytes_bound
        .checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND)
        .ok_or("正式工作目录峰值上界溢出")?;
    Ok(build_bound.max(validated.peak_formal_work_directory_bytes_bound))
}

fn observation_semantics_digest(
    candidate: &GateACandidate,
    output: &crate::gate_a::GateAOutputConfig,
) -> AnyResult<(u64, Sha256Hex)> {
    let mut cursor = MarketObservationCursor::open(
        &candidate.market_observations.manifest_path,
        None,
        None,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        output.sort.batch_rows,
    )?;
    let mut hasher = Sha256::new();
    hasher.update(b"data-pipeline/training/observation-determinism/v1\0");
    let mut rows = 0u64;
    loop {
        let page = cursor.next_page(output.sort.batch_rows)?;
        if page.is_empty() {
            break;
        }
        for observation in &page {
            let bytes = observation_determinism_bytes(observation)?;
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
            rows = rows.checked_add(1).ok_or("Observation 行数溢出")?;
        }
    }
    if rows != candidate.market_observations.manifest.file.rows {
        return Err("Observation 语义摘要行数与 manifest 不一致".into());
    }
    Ok((rows, Sha256Hex(format!("{:x}", hasher.finalize()))))
}

fn observation_determinism_bytes(
    observation: &MarketObservationCausalShellV3,
) -> AnyResult<Vec<u8>> {
    Ok(serde_json::to_vec(&ObservationDeterminismMaterial {
        schema_version: &observation.schema_version,
        market: &observation.market,
        canonical_event_record_contract_version: &observation
            .lineage
            .canonical_event_record_contract_version,
        execution_event_contract_version: &observation.lineage.execution_event_contract_version,
        reference_event_contract_version: &observation.lineage.reference_event_contract_version,
        input_set_identity_sha256: &observation.lineage.input_set_identity_sha256,
        contributing_event_ids: &observation.lineage.contributing_event_ids,
        visibility_cutoff: &observation.visibility_cutoff,
        market_state_digest_sha256: &observation.market_state_digest_sha256,
        component_states: &observation.component_states,
        missing_mask: &observation.missing_mask,
    })?)
}

fn require_same_logical_candidate(
    primary: &CandidateDeterminismSummary,
    perturbation: &CandidateDeterminismSummary,
) -> AnyResult<()> {
    if primary != perturbation {
        return Err("主配置与物理扰动配置的逻辑产物不一致".into());
    }
    Ok(())
}

fn read_quality_artifact_bindings(
    candidate_dir: &Path,
) -> AnyResult<crate::gate_a_publication::GateAArtifactManifestBindingsV1> {
    let bytes = read_regular_bounded(
        &candidate_dir.join("reports/quality-isolation-report.json"),
        4 * 1024 * 1024,
    )?;
    Ok(crate::gate_a_publication::decode_quality_isolation_report(&bytes)?.artifact_manifests)
}

fn require_clean_git_checkout() -> AnyResult<String> {
    let status = git_output(&["status", "--porcelain", "--untracked-files=all"])?;
    if !status.is_empty() {
        return Err("正式门 A 命令要求当前 Git 工作树干净".into());
    }
    if workspace_pipeline_core_identity_sha256(Path::new(WORKSPACE))?
        != pipeline_core_identity_sha256()
    {
        return Err("正式门 A 命令所用二进制不是由当前工作树的管道核心构建".into());
    }
    let commit = git_output(&["rev-parse", "HEAD"])?;
    if commit.len() != 40
        || !commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("当前 Git 提交不是 40 位小写十六进制身份".into());
    }
    Ok(commit)
}

fn require_unchanged_git_checkout(expected_commit: &str) -> AnyResult<()> {
    let current_commit = require_clean_git_checkout()?;
    if current_commit != expected_commit {
        return Err("正式门 A 运行期间 Git 提交发生变化".into());
    }
    Ok(())
}

fn git_output(arguments: &[&str]) -> AnyResult<String> {
    let output = Command::new("git")
        .current_dir(WORKSPACE)
        .args(arguments)
        .output()?;
    if !output.status.success() {
        return Err(format!("git 命令失败：{}", combined_output(&output)).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn combined_output(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn sha256_value(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> AnyResult<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_regular_bounded(path: &Path, max_bytes: u64) -> AnyResult<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max_bytes {
        return Err(format!("控制文件不是上限内普通文件：{}", path.display()).into());
    }
    Ok(fs::read(path)?)
}

fn sync_directory(path: &Path) -> AnyResult<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn parse_time_value(output: &str, prefix: &str) -> AnyResult<u64> {
    output
        .lines()
        .find_map(|line| line.trim().strip_prefix(prefix))
        .ok_or_else(|| format!("/usr/bin/time 缺少字段：{prefix}").into())
        .and_then(|value| value.trim().parse().map_err(Into::into))
}

fn publication_filesystem_facts(candidate_dir: &Path) -> AnyResult<PublicationFilesystemFacts> {
    let publication_root = Path::new(PUBLICATION_ROOT);
    let root_metadata = fs::symlink_metadata(publication_root)?;
    let candidate_metadata = fs::symlink_metadata(candidate_dir)?;
    if root_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
        || candidate_metadata.file_type().is_symlink()
        || !candidate_metadata.is_dir()
    {
        return Err("发布根和候选根必须是普通目录".into());
    }
    let root_filesystem = filesystem_type(publication_root)?;
    let candidate_filesystem = filesystem_type(candidate_dir)?;
    if root_filesystem != "ext4"
        || candidate_filesystem != root_filesystem
        || root_metadata.dev() != candidate_metadata.dev()
    {
        return Err("正式发布根与候选目录必须位于同一 ext4 文件系统".into());
    }
    Ok(PublicationFilesystemFacts {
        filesystem_type: root_filesystem,
        publication_root_device_id: root_metadata.dev(),
        candidate_device_id: candidate_metadata.dev(),
    })
}

fn filesystem_type(path: &Path) -> AnyResult<String> {
    let output = Command::new("/usr/bin/findmnt")
        .args(["--noheadings", "--output", "FSTYPE", "--target"])
        .arg(path)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "findmnt 无法识别 {}：{}",
            path.display(),
            combined_output(&output)
        )
        .into());
    }
    let filesystem = String::from_utf8(output.stdout)?.trim().to_owned();
    if filesystem.is_empty() || filesystem.split_whitespace().count() != 1 {
        return Err(format!("文件系统类型输出无效：{}", path.display()).into());
    }
    Ok(filesystem)
}

fn require_formal_network_and_environment_isolation() -> AnyResult<NetworkIsolationFacts> {
    for (name, _) in env::vars_os() {
        let name = name.to_string_lossy().to_ascii_uppercase();
        let forbidden = name.contains("PROXY")
            || name.contains("PRIVATE_KEY")
            || name.contains("WALLET")
            || name.ends_with("_TOKEN")
            || name.ends_with("_SECRET")
            || name.ends_with("_API_KEY")
            || matches!(
                name.as_str(),
                "AWS_ACCESS_KEY_ID" | "AWS_SECRET_ACCESS_KEY" | "SSH_AUTH_SOCK"
            );
        if forbidden {
            return Err(format!("正式离线命令环境仍含代理、钱包或凭据变量：{name}").into());
        }
    }

    let network_namespace = fs::read_link("/proc/self/ns/net")?
        .to_string_lossy()
        .into_owned();
    let interfaces = parse_network_interfaces(&fs::read_to_string("/proc/net/dev")?)?;
    if interfaces != ["lo"] {
        return Err("正式命令必须运行在只含 lo 的独立网络命名空间".into());
    }
    let (supervisor_process_id, supervisor_name, supervisor_argv) = network_isolation_supervisor()?;
    Ok(NetworkIsolationFacts {
        network_namespace,
        interfaces,
        supervisor_process_id,
        supervisor_name,
        supervisor_argv,
    })
}

fn network_isolation_supervisor() -> AnyResult<(u32, String, Vec<String>)> {
    let status = fs::read_to_string("/proc/self/status")?;
    let supervisor_process_id = status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:"))
        .ok_or("/proc/self/status 缺少 PPid")?
        .trim()
        .parse::<u32>()?;
    if supervisor_process_id == 0 {
        return Err("网络隔离监督进程 PID 无效".into());
    }
    let supervisor_status = fs::read_to_string(format!("/proc/{supervisor_process_id}/status"))?;
    let supervisor_name = supervisor_status
        .lines()
        .find_map(|line| line.strip_prefix("Name:"))
        .ok_or("网络隔离监督进程 status 缺少 Name")?
        .trim()
        .to_owned();
    let cmdline = fs::read(format!("/proc/{supervisor_process_id}/cmdline"))?;
    if cmdline.is_empty() || cmdline.len() > 64 * 1024 || *cmdline.last().unwrap() != 0 {
        return Err("网络隔离监督进程 cmdline 无效".into());
    }
    let supervisor_argv = cmdline[..cmdline.len() - 1]
        .split(|byte| *byte == 0)
        .map(|argument| {
            if argument.is_empty() {
                return Err("网络隔离监督进程参数为空".into());
            }
            Ok(std::str::from_utf8(argument)?.to_owned())
        })
        .collect::<AnyResult<Vec<_>>>()?;
    if supervisor_name != "bwrap" || !is_required_bwrap_argv(&supervisor_argv) {
        return Err("正式命令必须由固定 bubblewrap 网络隔离监督进程启动".into());
    }
    Ok((supervisor_process_id, supervisor_name, supervisor_argv))
}

fn is_required_bwrap_argv(argv: &[String]) -> bool {
    let required_prefix = [
        "/usr/bin/bwrap",
        "--unshare-user",
        "--uid",
        "0",
        "--gid",
        "0",
        "--unshare-net",
    ];
    argv.len() > required_prefix.len()
        && argv
            .iter()
            .zip(required_prefix)
            .all(|(actual, expected)| actual == expected)
        && !argv.iter().any(|argument| argument == "--share-net")
        && argv.windows(2).any(|pair| pair == ["--proc", "/proc"])
}

fn parse_network_interfaces(content: &str) -> AnyResult<Vec<String>> {
    let mut lines = content.lines();
    if lines.next().is_none() || lines.next().is_none() {
        return Err("/proc/net/dev 缺少固定表头".into());
    }
    let mut interfaces = lines
        .map(|line| {
            let (name, _) = line.split_once(':').ok_or("/proc/net/dev 接口行缺少冒号")?;
            let name = name.trim();
            if name.is_empty() {
                return Err("/proc/net/dev 接口名为空".into());
            }
            Ok(name.to_owned())
        })
        .collect::<AnyResult<Vec<_>>>()?;
    interfaces.sort();
    if interfaces.is_empty() || interfaces.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("/proc/net/dev 接口集合为空或重复".into());
    }
    Ok(interfaces)
}

#[derive(Clone, Debug)]
struct CgroupFacts {
    relative_path: String,
    directory: PathBuf,
}

impl CgroupFacts {
    fn current() -> AnyResult<Self> {
        let content = fs::read_to_string("/proc/self/cgroup")?;
        let relative_path = content
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .ok_or("当前进程不在统一 cgroup v2 层级")?
            .to_owned();
        let relative = Path::new(relative_path.trim_start_matches('/'));
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err("当前 cgroup v2 路径不是安全叶路径".into());
        }
        Ok(Self {
            directory: Path::new("/sys/fs/cgroup").join(relative),
            relative_path,
        })
    }

    fn require_formal_limits(&self, supervisor_process_id: u32) -> AnyResult<()> {
        let leaf = Path::new(&self.relative_path)
            .file_name()
            .and_then(|name| name.to_str());
        if self.relative_path == "/"
            || leaf
                .is_none_or(|name| !name.starts_with("dpt-gate-a-") || !name.ends_with(".service"))
            || self.read_number("memory.max")? != GATE_A_CGROUP_MEMORY_MAX_BYTES
            || self.read_number("memory.swap.max")? != 0
        {
            return Err(
                "正式命令必须运行在 memory.max=1GiB、memory.swap.max=0 的独立 cgroup 中".into(),
            );
        }
        let process_ids = self.process_ids()?;
        self.require_formal_process_tree(&process_ids, supervisor_process_id)?;
        if self.memory_events()?
            != (GateACgroupMemoryEventsV1 {
                low: 0,
                high: 0,
                max: 0,
                oom: 0,
                oom_kill: 0,
                oom_group_kill: 0,
            })
        {
            return Err("正式命令必须运行在未复用、无既有内存事件的 cgroup 中".into());
        }
        Ok(())
    }

    fn process_ids(&self) -> AnyResult<Vec<u32>> {
        let mut process_ids = fs::read_to_string(self.directory.join("cgroup.procs"))?
            .lines()
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()?;
        process_ids.sort_unstable();
        process_ids.dedup();
        Ok(process_ids)
    }

    fn require_formal_process_tree(
        &self,
        process_ids: &[u32],
        supervisor_process_id: u32,
    ) -> AnyResult<()> {
        let mut expected = vec![process::id(), supervisor_process_id];
        expected.sort_unstable();
        expected.dedup();
        if expected.len() != 2 || process_ids != expected {
            return Err("正式 cgroup 必须只包含网络隔离监督进程和当前门 A 进程".into());
        }
        Ok(())
    }

    fn read_number(&self, name: &str) -> AnyResult<u64> {
        let value = fs::read_to_string(self.directory.join(name))?;
        if value.trim() == "max" {
            return Err(format!("cgroup {name} 不能是 max").into());
        }
        Ok(value.trim().parse()?)
    }

    fn memory_events(&self) -> AnyResult<GateACgroupMemoryEventsV1> {
        let content = fs::read_to_string(self.directory.join("memory.events"))?;
        let mut values = BTreeMap::new();
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            let key = parts.next().ok_or("memory.events 缺少键")?;
            let value = parts.next().ok_or("memory.events 缺少值")?.parse()?;
            if parts.next().is_some() || values.insert(key, value).is_some() {
                return Err("memory.events 结构无效".into());
            }
        }
        let get = |key: &str| -> AnyResult<u64> {
            values
                .get(key)
                .copied()
                .ok_or_else(|| format!("memory.events 缺少 {key}").into())
        };
        Ok(GateACgroupMemoryEventsV1 {
            low: get("low")?,
            high: get("high")?,
            max: get("max")?,
            oom: get("oom")?,
            oom_kill: get("oom_kill")?,
            oom_group_kill: get("oom_group_kill")?,
        })
    }
}

fn collect_host_facts() -> AnyResult<GateAHostFactsV1> {
    let os_release = fs::read_to_string("/etc/os-release")?;
    let operating_system = os_release
        .lines()
        .find_map(|line| line.strip_prefix("PRETTY_NAME="))
        .map(|value| value.trim_matches('"').to_owned())
        .ok_or("/etc/os-release 缺少 PRETTY_NAME")?;
    let cpuinfo = fs::read_to_string("/proc/cpuinfo")?;
    let cpu_model = cpuinfo
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.trim() == "model name")
        })
        .map(|(_, value)| value.trim().to_owned())
        .ok_or("/proc/cpuinfo 缺少 model name")?;
    let meminfo = fs::read_to_string("/proc/meminfo")?;
    let physical_memory_kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.split_whitespace().next())
        .ok_or("/proc/meminfo 缺少 MemTotal")?
        .parse()?;
    Ok(GateAHostFactsV1 {
        operating_system,
        architecture: env::consts::ARCH.to_owned(),
        cpu_model,
        logical_cpu_count: u32::try_from(std::thread::available_parallelism()?.get())?,
        physical_memory_bytes: physical_memory_kib
            .checked_mul(1024)
            .ok_or("物理内存字节数溢出")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{
        ClobMarketId, ConditionId, MarketIdentityV2, MarketWindowV2, ObservationLineageV3,
        ObservationVisibilityCutoffV2,
    };

    fn observation(visibility_manifest: char) -> MarketObservationCausalShellV3 {
        MarketObservationCausalShellV3 {
            schema_version: "market_observation_causal_shell_v3".to_owned(),
            observation_id: Sha256Hex(visibility_manifest.to_string().repeat(64)),
            market: MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
                clob_market_id: ClobMarketId("market-1".to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 1_000,
                    end_at_utc_ms: 2_000,
                },
            },
            lineage: ObservationLineageV3 {
                canonical_event_record_contract_version: "canonical_event_record_v1".to_owned(),
                execution_event_contract_version: "execution_market_event_v3".to_owned(),
                reference_event_contract_version: "canonical_reference_event_v1".to_owned(),
                input_set_identity_sha256: Sha256Hex("2".repeat(64)),
                visibility_index_manifest_sha256: Sha256Hex(
                    visibility_manifest.to_string().repeat(64),
                ),
                contributing_event_ids: Vec::new(),
            },
            visibility_cutoff: ObservationVisibilityCutoffV2 {
                received_at_utc_ms: 1_500,
            },
            market_state_digest_sha256: Sha256Hex("3".repeat(64)),
            component_states: Vec::new(),
            missing_mask: Vec::new(),
            canonical_hash_sha256: Sha256Hex(visibility_manifest.to_string().repeat(64)),
        }
    }

    #[test]
    fn observation_determinism_ignores_only_physical_manifest_derived_identity() {
        let primary = observation('a');
        let mut perturbation = observation('b');
        assert_eq!(
            observation_determinism_bytes(&primary).unwrap(),
            observation_determinism_bytes(&perturbation).unwrap()
        );

        perturbation.visibility_cutoff.received_at_utc_ms += 1;
        assert_ne!(
            observation_determinism_bytes(&primary).unwrap(),
            observation_determinism_bytes(&perturbation).unwrap()
        );
    }

    #[test]
    fn proc_network_interface_parser_uses_the_namespace_aware_view() {
        let loopback_only = "Inter-| Receive | Transmit\n face |bytes |bytes\n    lo: 0 0\n";
        assert_eq!(parse_network_interfaces(loopback_only).unwrap(), ["lo"]);
        assert!(parse_network_interfaces("Inter-| Receive\n face |bytes\ninvalid").is_err());
    }

    #[test]
    fn bwrap_isolation_prefix_is_exact() {
        let argv = [
            "/usr/bin/bwrap",
            "--unshare-user",
            "--uid",
            "0",
            "--gid",
            "0",
            "--unshare-net",
            "--proc",
            "/proc",
            "/bin/true",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        assert!(is_required_bwrap_argv(&argv));
        let mut without_network_isolation = argv;
        without_network_isolation.insert(7, "--share-net".to_owned());
        assert!(!is_required_bwrap_argv(&without_network_isolation));
    }
}
