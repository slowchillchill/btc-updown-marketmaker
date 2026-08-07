use std::path::{Path, PathBuf};

use data_pipeline_training::canonical_run::CanonicalRunScope;
use data_pipeline_training::freeze_direct_source_input_set;
use data_pipeline_training::gate_a::{
    PlatformReplayCursor, build_gate_a_candidate, validate_gate_a_candidate,
};
use data_pipeline_training::gate_a_publication::{
    gate_a_formal_canonical_config, gate_a_formal_output_config,
};
use tempfile::TempDir;

const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
const CONDITION_ID: &str = "0xd4346bcf419856fa3261d3c0705597d5c909ead020b9f1a0de468092704fc65f";
const RECEIVED_START_UTC_MS: i64 = 1_785_715_201_837;
const RECEIVED_END_UTC_MS: i64 = 1_785_715_888_412;

fn manifest_path() -> PathBuf {
    Path::new(DATASET_LINEAGE_ROOT).join("2026-08-03/typed_raw/utc_day=2026-08-03/manifest.json")
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
