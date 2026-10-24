use contracts::execution_normalization::*;
use contracts::{AccountInstanceKeyV1, ExactDecimal, PlatformSourceV1, SimCallerRoleV1};
use execution_normalization::*;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

const RUN: &str = "synthetic-noe-prefix";
const CASH: &str = "12345678901234567890.123456789";
type Trade = SourceNeutralTradeLifecycleResultV2;
type Control = SourceNeutralControlResultV3;

// 使用具名证据根时，失败也保留正式输入、stdout/stderr 及配置；没有该根时仅用临时夹具。
fn root(name: &str) -> PathBuf {
    let path = if let Some(parent) = std::env::var_os("NOE_PREFIX_TEST_OUTPUT_ROOT") {
        PathBuf::from(parent).join(name)
    } else {
        tempfile::tempdir().unwrap().keep()
    };
    fs::create_dir_all(&path).unwrap();
    path
}

struct Fixture {
    directory: PathBuf,
    expected: Vec<Value>,
    context: ExecutionNormalizationContextV1,
    commits: Vec<PublishedNoeCommitV1>,
}

fn fixture(root: &Path, count: u64, split: u64) -> Fixture {
    let source = |name| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!("artifact://{RUN}/{name}")),
    };
    let context = sim_v2_context(
        SimV2ContextSeedV1 {
            run_id: RUN.into(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: RUN.into(),
                caller_role: SimCallerRoleV1::HistoricalBacktest,
                caller_instance_id: "fixture".into(),
                account_id: "synthetic-account".into(),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: source("opened"),
            account_opening: source("opening"),
            account_opening_evidence_identity: "opening".into(),
            implementation_identity: "read-noe-prefix-fixture".into(),
        },
        "execution_normalization_v4",
    )
    .unwrap();
    let mut core = NormalizationCore::<Trade, Control>::new(context.clone());
    let mut writer =
        StagedNormalizationWriter::<Trade, Control>::create_batched(root, context.clone()).unwrap();
    let mut expected = Vec::new();
    let mut commits = Vec::new();
    for seq in 1..=count {
        let request = format!("request-{seq}");
        let evidence = if seq == 1 {
            SourceNeutralEvidence {
                source_record: SimV2SourceRecordReferenceV1::AccountOpening,
                source_ordinal: 0,
                logical_key: SourceNeutralLogicalKeyV1::AccountOpening {
                    opening_evidence_identity: "opening".into(),
                },
                kind: SourceNeutralEvidenceKind::Economic {
                    evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                        opening_evidence_identity: "opening".into(),
                        available_pusd: ExactDecimal(CASH.into()),
                    },
                },
            }
        } else {
            SourceNeutralEvidence {
                source_record: SimV2SourceRecordReferenceV1::DirectResponse {
                    private_feedback_sequence: 100 + seq,
                    platform_responded_at_utc_ms: 2_000 + seq as i64,
                    response_received_at_utc_ms: 2_100 + seq as i64,
                    association: SimV2DirectResponseAssociationV1::SubmissionItem {
                        global_request_id: request.clone(),
                        batch_item_ordinal: 0,
                    },
                },
                source_ordinal: 0,
                logical_key: SourceNeutralLogicalKeyV1::SubmissionItem {
                    global_request_id: request,
                    batch_item_ordinal: 0,
                },
                kind: SourceNeutralEvidenceKind::Control {
                    result: SourceNeutralControlResultV3::Submission {
                        outcome: SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
                    },
                },
            }
        };
        let batch = core
            .normalize_and_accept(NormalizationBatchInput {
                source_watermark: SimV2SourceWatermarkV1 {
                    run_id: RUN.into(),
                    projection_batch_sequence: seq,
                },
                evidence: vec![evidence],
            })
            .unwrap();
        assert_eq!(batch.records.len(), 1);
        expected.push(serde_json::to_value(&batch.records[0]).unwrap());
        writer.stage(&batch).unwrap();
        if seq == split || seq == count {
            writer
                .stabilize(|| 4_000 + seq as i64, |commit| commits.push(commit.clone()))
                .unwrap();
        }
    }
    let directory = writer.context_directory().to_path_buf();
    drop(writer);
    fs::write(
        root.join("fixture.json"),
        serde_json::to_vec_pretty(&json!({
            "synthetic": true, "context_directory_display": directory.to_string_lossy(), "published_prefix": count,
            "records": expected.len(), "segments": 2, "opening_cash": CASH,
            "construction": "NormalizationCore -> create_batched -> stage -> stabilize",
        }))
        .unwrap(),
    )
    .unwrap();
    Fixture {
        directory,
        expected,
        context,
        commits,
    }
}

fn invoke(root: &Path, directory: &Path, prefix: u64, label: &str) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_read_noe_prefix"))
        .arg(directory)
        .arg(prefix.to_string())
        .output()
        .unwrap();
    fs::write(root.join(format!("{label}.stdout.jsonl")), &output.stdout).unwrap();
    fs::write(root.join(format!("{label}.stderr.txt")), &output.stderr).unwrap();
    fs::write(
        root.join(format!("{label}.exit.txt")),
        output.status.code().unwrap().to_string(),
    )
    .unwrap();
    output
}

fn lines(output: &Output) -> Vec<Value> {
    output
        .stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

#[test]
fn cli_preserves_records_and_prefix_and_reports_partial_failure() {
    let root = root("contract");
    let fixture = fixture(&root, 5, 3);
    let tail = fixture.directory.join("commits/.unpublished.tmp");
    fs::write(&tail, b"unpublished tail is not a record").unwrap();
    let zero = invoke(&root, &fixture.directory, 0, "zero");
    assert_eq!(zero.status.code(), Some(0));
    assert!(zero.stderr.is_empty());
    assert_eq!(
        lines(&zero),
        vec![
            json!({"kind":"begin","format":"noe-prefix-read-v1","published_prefix":0}),
            json!({"kind":"complete","stats":{"commits":0,"records":0,"segment_opens":0,"pages_read":0}}),
        ]
    );
    let partial = invoke(&root, &fixture.directory, 4, "partial");
    assert_eq!(partial.status.code(), Some(0));
    assert!(partial.stderr.is_empty());
    let actual = lines(&partial);
    assert_eq!(actual.len(), 6);
    assert_eq!(
        actual[0],
        json!({"kind":"begin","format":"noe-prefix-read-v1","published_prefix":4})
    );
    for seq in 0..4 {
        assert_eq!(
            actual[seq + 1],
            json!({
                "kind":"record", "reference": NoeResultReferenceV1 {
                    commit_container: fixture.commits[seq].container_reference.clone(), record_ordinal: 0,
                },
                "position": NoeRecordPositionV1 {
                    context_reference: fixture.context.context_reference.clone(),
                    source_watermark: SimV2SourceWatermarkV1 { run_id: RUN.into(), projection_batch_sequence: seq as u64 + 1 },
                    record_ordinal: 0,
                },
                "record": fixture.expected[seq],
            })
        );
    }
    assert_eq!(
        actual[1]["record"]["EconomicFact"]["fact"]["disposition"]["AccountOpening"]["available_pusd"],
        CASH
    );
    assert_eq!(
        actual[2]["record"]["Evidence"]["source_reference"]["record"]["DirectResponse"]["private_feedback_sequence"],
        102
    );
    assert_eq!(
        actual[2]["record"]["Evidence"]["source_reference"]["record"]["DirectResponse"]["association"]
            ["SubmissionItem"]["global_request_id"],
        "request-2"
    );
    assert_eq!(
        actual[5],
        json!({"kind":"complete","stats":{"commits":4,"records":4,"segment_opens":2,"pages_read":2}})
    );

    let too_large = invoke(&root, &fixture.directory, 6, "too-large");
    assert_eq!(too_large.status.code(), Some(1));
    assert_eq!(lines(&too_large).len(), 6); // begin + 五条部分事实，无 complete。
    assert!(
        lines(&too_large)
            .iter()
            .all(|line| line["kind"] != "complete")
    );
    assert!(String::from_utf8_lossy(&too_large.stderr).contains("6"));
    assert!(String::from_utf8_lossy(&too_large.stderr).contains("5"));

    let last_segment = fixture
        .directory
        .join("commits/segment-00000000000000000003.parquet");
    fs::write(&last_segment, b"broken later segment").unwrap();
    let before_damage = invoke(&root, &fixture.directory, 3, "before-damage");
    assert_eq!(before_damage.status.code(), Some(0));
    let damaged = invoke(&root, &fixture.directory, 4, "damaged");
    assert_eq!(damaged.status.code(), Some(1));
    assert_eq!(lines(&damaged).len(), 4);
    assert!(
        lines(&damaged)
            .iter()
            .all(|line| line["kind"] != "complete")
    );
    assert!(!damaged.stderr.is_empty());
    assert_eq!(fs::read(last_segment).unwrap(), b"broken later segment");
    assert_eq!(fs::read(tail).unwrap(), b"unpublished tail is not a record");
}

#[test]
fn cli_uses_os_path_and_reports_stdout_and_argument_errors() {
    use std::os::unix::ffi::OsStringExt;
    let root = root("output-error");
    let non_utf8 = root.join(std::ffi::OsString::from_vec(b"context-\xff".to_vec()));
    let fixture = fixture(&non_utf8, 5, 3);
    // 不解析记录路径为 UTF-8；明确零前缀同样使用原 OS 目录。
    assert_eq!(
        invoke(&root, &fixture.directory, 0, "os-path")
            .status
            .code(),
        Some(0)
    );
    let invalid = Command::new(env!("CARGO_BIN_EXE_read_noe_prefix"))
        .arg(&fixture.directory)
        .arg("-1")
        .output()
        .unwrap();
    fs::write(root.join("invalid.stdout"), &invalid.stdout).unwrap();
    fs::write(root.join("invalid.stderr"), &invalid.stderr).unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert!(invalid.stdout.is_empty());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("usage:"));
    let missing = fixture.directory.join("missing-context");
    for (directory, label) in [
        (&fixture.directory, "flush-failure"),
        (&missing, "read-and-flush-failure"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_read_noe_prefix"))
            .arg(directory)
            .arg("0")
            .stdout(Stdio::from(
                fs::OpenOptions::new()
                    .write(true)
                    .open("/dev/full")
                    .unwrap(),
            ))
            .output()
            .unwrap();
        fs::write(root.join(format!("{label}.stderr")), &output.stderr).unwrap();
        fs::write(
            root.join(format!("{label}.exit")),
            output.status.code().unwrap().to_string(),
        )
        .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("flush stdout JSONL"));
        if label == "read-and-flush-failure" {
            assert!(error.contains("additional output failure:"));
            assert!(error.contains("missing-context"));
        }
    }
}

/// 单独计量正式合成构造，留下供 release CLI 成本复用的输入；不包含 CLI 读取。
#[test]
fn publish_finite_synthetic_fixture() {
    let root = root("finite");
    let fixture = fixture(&root, 256, 128);
    assert_eq!(fixture.commits.len(), 256);
    assert_eq!(fixture.expected.len(), 256);
}
