use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use data_pipeline_training::canonical_run::CanonicalRunScope;
use data_pipeline_training::freeze_direct_source_input_set;
use data_pipeline_training::gate_a::{
    PlatformReplayCursor, VisibilityReplayCursor, build_gate_a_candidate,
    pipeline_core_identity_sha256, validate_gate_a_candidate,
};
use data_pipeline_training::{
    gate_a_formal_canonical_config, gate_a_formal_output_config, open_published_gate_a_final,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
const CONDITION_ID: &str = "0xd4346bcf419856fa3261d3c0705597d5c909ead020b9f1a0de468092704fc65f";
const RECEIVED_START_UTC_MS: i64 = 1_785_715_201_837;
const RECEIVED_END_UTC_MS: i64 = 1_785_715_888_412;

fn manifest_path() -> PathBuf {
    Path::new(DATASET_LINEAGE_ROOT).join("2026-08-03/typed_raw/utc_day=2026-08-03/manifest.json")
}

fn immutable_tree_snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    fn visit(root: &Path, path: &Path, snapshot: &mut BTreeMap<PathBuf, (u64, String)>) {
        let mut entries = fs::read_dir(path)
            .expect("冻结 final 目录必须可读")
            .collect::<Result<Vec<_>, _>>()
            .expect("冻结 final 目录项必须可读");
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).expect("冻结 final 元数据必须可读");
            assert!(!metadata.file_type().is_symlink());
            if metadata.is_dir() {
                visit(root, &path, snapshot);
            } else {
                assert!(metadata.is_file());
                let bytes = fs::read(&path).expect("冻结 final 文件必须可读");
                snapshot.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    (metadata.len(), format!("{:x}", Sha256::digest(&bytes))),
                );
            }
        }
    }

    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot);
    snapshot
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-03 dataset"]
fn real_complete_single_market_gate_a_candidate_is_closed_and_replayable() {
    let input =
        freeze_direct_source_input_set(&[manifest_path()]).expect("真实每日清单必须完成冻结准入");
    let scope = CanonicalRunScope::new(CONDITION_ID, RECEIVED_START_UTC_MS, RECEIVED_END_UTC_MS)
        .expect("正式候选范围必须有效");
    let root = TempDir::new().expect("创建门 A 真实候选临时根");
    let candidate_path = root.path().join("candidate");
    let validation_path = root.path().join("validation");
    let canonical_config = gate_a_formal_canonical_config();
    let output_config = gate_a_formal_output_config();

    let candidate = build_gate_a_candidate(
        &input,
        &scope,
        &candidate_path,
        &canonical_config,
        &output_config,
    )
    .expect("真实完整单市场候选必须成功构建");
    let validated = validate_gate_a_candidate(&candidate_path, &validation_path, &output_config)
        .expect("真实完整单市场候选必须通过闭包复核");
    let mut platform_replay = PlatformReplayCursor::open(
        &validated,
        None,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
        256,
    )
    .expect("平台回放读取器必须绑定已复核候选");
    let replay_page = platform_replay
        .next_page(256)
        .expect("平台回放首个有界页必须可解析");
    assert!(!replay_page.is_empty());
    let replay_times = replay_page
        .iter()
        .map(|record| match record {
            contracts::CanonicalEventRecordV1::ExecutionMarketEvent(event) => event
                .time
                .source_event_at_utc_ms
                .expect("事件时间索引不得包含无平台时间事件"),
            contracts::CanonicalEventRecordV1::ReferenceEvent(event) => event
                .time
                .source_event_at_utc_ms
                .expect("事件时间索引不得包含无平台时间事件"),
        })
        .collect::<Vec<_>>();
    assert!(replay_times.windows(2).all(|pair| pair[0] <= pair[1]));

    eprintln!(
        "task_2_5_gate_a={{scope:{:?},files:{:?},canonical_config:{:?},output_config:{:?},digests:{:?},canonical_metrics:{:?},resource_metrics:{:?},artifacts:[{:?},{:?},{:?},{:?},{:?}],validated_digests:{:?},validated_peak_l2:{}}}",
        candidate.scope,
        candidate.selected_files,
        candidate.config,
        candidate.output_config,
        candidate.canonical_digests,
        candidate.canonical_metrics,
        candidate.resource_metrics,
        candidate.canonical_event_store,
        candidate.event_time_index,
        candidate.visibility_index,
        candidate.market_observations,
        candidate.reduction_reports,
        validated.recomputed_stream_digests,
        validated.peak_observation_l2_levels,
    );
    assert_eq!(
        candidate.canonical_digests,
        validated.recomputed_stream_digests
    );
    assert_eq!(
        candidate.resource_metrics.peak_observation_l2_levels,
        validated.peak_observation_l2_levels
    );
    assert_eq!(
        candidate.canonical_event_store.manifest.file.rows,
        candidate.visibility_index.manifest.file.rows
    );
    assert!(candidate.event_time_index.manifest.file.rows > 0);
    assert!(candidate.market_observations.manifest.file.rows > 0);
    assert!(candidate.reduction_reports.manifest.file.rows > 0);
}

#[test]
#[ignore = "requires the frozen published Gate A V2 final at its approved absolute path"]
fn frozen_gate_a_v2_final_opens_with_current_reader_and_both_replays() {
    let root = Path::new(
        "/home/chillchill/sim-system/.local-artifacts/data-pipeline-training/gate-a/\
bc2fdeddd1ea9a6c931823be498591ccaf32dfad61ed969f86c92b16ae3cc574",
    );
    let before = immutable_tree_snapshot(root);
    assert!(!root.join(".final-validation").exists());
    let validation_root = TempDir::new().expect("创建 final 外部验证临时根");
    let validation_path = validation_root.path().join("validation");

    let opened = open_published_gate_a_final(root, &validation_path)
        .expect("冻结 Gate A V2 final 必须可由当前 reader 只读打开");
    assert_eq!(
        opened.receipt().receipt_id.0,
        "bc2fdeddd1ea9a6c931823be498591ccaf32dfad61ed969f86c92b16ae3cc574"
    );
    assert_eq!(
        opened.receipt().run_manifest_sha256.0,
        "ceccb9bfbbee0ead000ae284515a870cedf6ae4d13180ca60ef247c1d69a9696"
    );
    assert_eq!(
        opened.producer_pipeline_core_identity_sha256().0,
        "023d8a3d5a5fdcd741b617e70e9ab2a357d72ce4663b87cd3410b14447d546b5"
    );
    assert_eq!(
        opened.reader_pipeline_core_identity_sha256(),
        &pipeline_core_identity_sha256()
    );
    assert_ne!(
        opened.producer_pipeline_core_identity_sha256(),
        opened.reader_pipeline_core_identity_sha256()
    );
    assert_eq!(
        &opened.run_manifest().pipeline_core_identity_sha256,
        opened.producer_pipeline_core_identity_sha256()
    );

    let output = &opened.run_manifest().output_config;
    let mut platform = PlatformReplayCursor::open(
        opened.candidate(),
        None,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        8,
    )
    .expect("平台回放必须绑定冻结 candidate");
    let mut visibility = VisibilityReplayCursor::open(
        opened.candidate(),
        None,
        output.sort.batch_rows,
        output.sort.max_run_bytes,
        8,
    )
    .expect("模型可见回放必须绑定冻结 candidate");
    assert_eq!(
        platform.canonical_event_store_manifest_sha256(),
        visibility.canonical_event_store_manifest_sha256()
    );
    assert!(
        !platform
            .next_page(8)
            .expect("平台回放小页必须可读")
            .is_empty()
    );
    assert!(
        !visibility
            .next_page(8)
            .expect("模型可见回放小页必须可读")
            .is_empty()
    );

    assert!(!validation_path.exists(), "外部验证临时目录必须清理");
    assert!(!root.join(".final-validation").exists());
    assert_eq!(
        immutable_tree_snapshot(root),
        before,
        "冻结 final 不得发生变化"
    );
}
