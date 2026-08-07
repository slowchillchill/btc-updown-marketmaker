use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};

use contracts::{MarketObservationCausalShellV3, Sha256Hex};
use data_pipeline_training::canonical_run::{
    CanonicalRunDigests, CanonicalRunInputFile, CanonicalRunScope,
};
use data_pipeline_training::freeze_direct_source_input_set;
use data_pipeline_training::gate_a::{
    GATE_A_CONTROL_FILE_BYTES_BOUND, GateACandidate, MarketObservationCursor,
    ValidatedGateACandidate, build_gate_a_candidate, pipeline_core_identity_sha256,
    validate_gate_a_candidate, workspace_pipeline_core_identity_sha256,
};
use data_pipeline_training::gate_a_publication::{
    GATE_A_CGROUP_MEMORY_MAX_BYTES, GATE_A_WORKSPACE_TEST_ARGV, GATE_A_WORKSPACE_TEST_EVIDENCE_IDS,
    GateACgroupMemoryEventsV1, GateAExternalResourceFactsV1, GateAHostFactsV1,
    GateATestCommandEvidenceV1, build_measured_build_state, build_quality_isolation_report,
    build_resource_high_water_report_from_state, build_test_evidence_report,
    decode_measured_build_state, encode_measured_build_state, encode_quality_isolation_report,
    encode_resource_high_water_report, finalize_and_publish_gate_a_candidate,
    gate_a_formal_canonical_config, gate_a_formal_output_config, gate_a_perturbation_configs,
    regular_directory_bytes,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const WORKSPACE: &str = "/home/chillchill/sim-system";
const PUBLICATION_ROOT: &str =
    "/home/chillchill/sim-system/.local-artifacts/data-pipeline-training/gate-a";
const BUILD_STATE_FILE: &str = ".measured-build-state.json";
const PHYSICAL_COMMAND_FILE: &str = ".physical-perturbation-command.json";
const TIME_V_FILE: &str = ".time-v.txt";

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

fn main() {
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
    require_clean_git_checkout()?;
    let cgroup = CgroupFacts::current()?;
    cgroup.require_formal_limits()?;
    fs::create_dir_all(PUBLICATION_ROOT)?;
    fs::create_dir(&args.candidate_dir)?;
    let mut cleanup = ExactCandidateCleanup::new(args.candidate_dir.clone());
    fs::create_dir(args.candidate_dir.join("reports"))?;

    let events_before = cgroup.memory_events()?;
    let current_exe = env::current_exe()?;
    let time_path = args.candidate_dir.join(TIME_V_FILE);
    let child_arguments = child_arguments(&args);
    let mut command = Command::new("/usr/bin/time");
    command
        .current_dir(WORKSPACE)
        .env("LC_ALL", "C")
        .arg("-v")
        .arg("-o")
        .arg(&time_path)
        .arg(&current_exe)
        .args(&child_arguments);
    let output = command.output()?;
    let events_after = cgroup.memory_events()?;
    let combined_output = combined_output(&output);
    if !output.status.success() {
        eprint!("{combined_output}");
        return Err("计时构建子进程失败".into());
    }

    let state_bytes =
        read_regular_bounded(&args.candidate_dir.join(BUILD_STATE_FILE), 4 * 1024 * 1024)?;
    let state = decode_measured_build_state(&state_bytes)?;
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
        cgroup_v2_relative_path: cgroup.relative_path.clone(),
        memory_peak_bytes: cgroup.read_number("memory.peak")?,
        memory_max_bytes: cgroup.read_number("memory.max")?,
        memory_swap_peak_bytes: cgroup.read_number("memory.swap.peak")?,
        memory_swap_max_bytes: cgroup.read_number("memory.swap.max")?,
        memory_events_before: events_before,
        memory_events_after: events_after,
        time_v_output_sha256: sha256_value(time_v_output.as_bytes()),
        time_v_output,
    };
    let resource =
        build_resource_high_water_report_from_state(&state, collect_host_facts()?, external)?;
    if !resource.evidence.passed {
        return Err("正式资源高水位超过冻结门".into());
    }
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
        covered_evidence_ids: vec!["test/physical-perturbation".to_owned()],
    };
    write_new_synced(
        &args.candidate_dir.join(PHYSICAL_COMMAND_FILE),
        &serde_json::to_vec(&physical_command)?,
    )?;
    fs::remove_file(time_path)?;
    fs::remove_file(args.candidate_dir.join(BUILD_STATE_FILE))?;
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
    let state = build_measured_build_state(&primary, quality_sha256, artifact_directory_bytes)?;
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
    require_clean_git_checkout()?;
    let cgroup = CgroupFacts::current()?;
    cgroup.require_formal_limits()?;
    let mut cleanup = ExactCandidateCleanup::new(candidate_dir.clone());
    let physical_bytes =
        read_regular_bounded(&candidate_dir.join(PHYSICAL_COMMAND_FILE), 4 * 1024 * 1024)?;
    let physical: GateATestCommandEvidenceV1 = serde_json::from_slice(&physical_bytes)?;

    let output = Command::new("cargo")
        .current_dir(WORKSPACE)
        .args(&GATE_A_WORKSPACE_TEST_ARGV[1..])
        .output()?;
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
    let git_commit = git_output(&["rev-parse", "HEAD"])?;
    let artifact_manifests = read_quality_artifact_bindings(&candidate_dir)?;
    let tests = build_test_evidence_report(
        git_commit,
        artifact_manifests,
        vec![physical, workspace_command],
    )?;
    fs::remove_file(candidate_dir.join(PHYSICAL_COMMAND_FILE))?;
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
    output: &data_pipeline_training::gate_a::GateAOutputConfig,
) -> AnyResult<CandidateDeterminismSummary> {
    let artifact_fact = |artifact: &data_pipeline_training::gate_a::GateAArtifactDescriptor| {
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
    output: &data_pipeline_training::gate_a::GateAOutputConfig,
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
) -> AnyResult<data_pipeline_training::gate_a_publication::GateAArtifactManifestBindingsV1> {
    let bytes = read_regular_bounded(
        &candidate_dir.join("reports/quality-isolation-report.json"),
        4 * 1024 * 1024,
    )?;
    Ok(
        data_pipeline_training::gate_a_publication::decode_quality_isolation_report(&bytes)?
            .artifact_manifests,
    )
}

fn require_clean_git_checkout() -> AnyResult<()> {
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

    fn require_formal_limits(&self) -> AnyResult<()> {
        if self.relative_path == "/"
            || self.read_number("memory.max")? != GATE_A_CGROUP_MEMORY_MAX_BYTES
            || self.read_number("memory.swap.max")? != 0
        {
            return Err(
                "正式命令必须运行在 memory.max=1GiB、memory.swap.max=0 的独立 cgroup 中".into(),
            );
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
}
