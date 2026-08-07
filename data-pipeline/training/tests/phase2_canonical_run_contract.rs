use std::path::{Path, PathBuf};

use data_pipeline_training::canonical_run::{
    CanonicalRunConfig, CanonicalRunError, CanonicalRunScope, reduce_frozen_input_to_staging,
};
use data_pipeline_training::typed_raw::TypedRawReadError;
use data_pipeline_training::{SortConfig, freeze_direct_source_input_set};
use tempfile::TempDir;

const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
const CONDITION_ID: &str = "0x242b1d2421714f5f93a1e30239e732bca05ced7c86393b7b204a32eb0f615bee";
const RECEIVED_START_UTC_MS: i64 = 1_785_867_275_000;
const RECEIVED_END_UTC_MS: i64 = 1_785_867_421_000;

fn manifest_path() -> PathBuf {
    Path::new(DATASET_LINEAGE_ROOT).join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json")
}

fn config(
    decode_batch_rows: usize,
    max_run_rows: usize,
    merge_fan_in: usize,
    batch_rows: usize,
) -> CanonicalRunConfig {
    CanonicalRunConfig {
        decode_batch_rows,
        max_decode_batch_bytes: 64 * 1024 * 1024,
        max_temporary_row_bytes: 128 * 1024,
        max_epoch_count_per_source: 1,
        max_pending_ws_terminals: 1,
        max_retained_reducer_entries: 32 * 1024,
        canonical_rule_version: "canonical-production-run-v1".to_owned(),
        sort: SortConfig {
            max_run_rows,
            max_run_bytes: 32 * 1024 * 1024,
            max_temp_run_files: 8,
            merge_fan_in,
            max_open_files: merge_fan_in + 1,
            batch_rows,
            max_temp_bytes: 4 * 1024 * 1024,
        },
    }
}

#[test]
#[ignore = "requires complete admission of the local finalized 2026-08-04 dataset"]
fn real_scoped_six_source_run_uses_one_deterministic_production_path() {
    let input =
        freeze_direct_source_input_set(&[manifest_path()]).expect("真实每日清单必须完成冻结准入");
    let scope = CanonicalRunScope::new(CONDITION_ID, RECEIVED_START_UTC_MS, RECEIVED_END_UTC_MS)
        .expect("真实试验范围必须有效");
    let first_root = TempDir::new().expect("创建第一次运行临时根");
    let second_root = TempDir::new().expect("创建第二次运行临时根");

    let first = reduce_frozen_input_to_staging(
        &input,
        &scope,
        first_root.path(),
        &config(8_192, 2_048, 4, 1_024),
    )
    .expect("第一次真实生产路径运行必须成功");
    let second = reduce_frozen_input_to_staging(
        &input,
        &scope,
        second_root.path(),
        &config(4_096, 4_096, 4, 2_048),
    )
    .expect("批次和排序分区扰动后的真实生产路径运行必须成功");

    eprintln!(
        "task_2_4_first={{scope:{:?},input_set:{},manifest_set:{},files:{:?},config:{:?},digests:{:?},metrics:{:?}}}",
        first.scope,
        first.input_set_identity_sha256,
        first.input_manifest_set_sha256,
        first.selected_files,
        first.config,
        first.digests,
        first.metrics,
    );
    eprintln!(
        "task_2_4_second={{config:{:?},digests:{:?},metrics:{:?}}}",
        second.config, second.digests, second.metrics,
    );

    assert_eq!(first.scope, scope);
    assert_eq!(
        first.input_set_identity_sha256,
        second.input_set_identity_sha256
    );
    assert_eq!(
        first.input_manifest_set_sha256,
        second.input_manifest_set_sha256
    );
    assert_eq!(first.selected_files, second.selected_files);
    assert_eq!(
        first.temporary_sort_logical_sha256,
        second.temporary_sort_logical_sha256
    );
    assert_eq!(first.digests, second.digests);
    assert_eq!(first.metrics.selected_rows, second.metrics.selected_rows);
    assert_eq!(
        first.metrics.canonical_records,
        second.metrics.canonical_records
    );
    assert_eq!(
        first.metrics.execution_market_events,
        second.metrics.execution_market_events
    );
    assert_eq!(
        first.metrics.reference_events,
        second.metrics.reference_events
    );
    assert_eq!(
        first.metrics.event_time_projections,
        second.metrics.event_time_projections
    );
    assert_eq!(
        first.metrics.visibility_projections,
        second.metrics.visibility_projections
    );
    assert_eq!(
        first.metrics.reduction_reports,
        second.metrics.reduction_reports
    );

    let counts = &first.metrics.selected_rows;
    assert_eq!(counts.polymarket_orderbook, 3_935);
    assert_eq!(counts.polymarket_market_metadata, 3);
    assert!(counts.polymarket_rtds_chainlink_btc_usd > 0);
    assert_eq!(counts.binance_depth_snapshot, 1);
    assert!(counts.binance_depth_update > 0);
    assert!(counts.binance_aggtrade > 0);
    assert!(first.metrics.canonical_records > 0);
    assert_eq!(
        first.metrics.visibility_projections,
        first.metrics.canonical_records
    );
    assert!(first.metrics.event_time_projections < first.metrics.canonical_records);
    for run in [&first, &second] {
        assert!(run.metrics.peak_decode_batch_rows > 0);
        assert!(run.metrics.peak_decode_batch_rows <= run.config.decode_batch_rows);
        assert!(run.metrics.peak_decode_batch_bytes > 0);
        assert!(run.metrics.peak_decode_batch_bytes <= run.config.max_decode_batch_bytes);
        assert!(run.metrics.peak_temporary_row_bytes <= run.config.max_temporary_row_bytes);
        assert!(run.metrics.polymarket_epoch_count <= run.config.max_epoch_count_per_source);
        assert!(run.metrics.binance_depth_epoch_count <= run.config.max_epoch_count_per_source);
        assert!(run.metrics.peak_pending_ws_terminals <= run.config.max_pending_ws_terminals);
        assert!(run.metrics.peak_retained_reducer_entries > 0);
        assert!(
            run.metrics.peak_retained_reducer_entries <= run.config.max_retained_reducer_entries
        );
        assert_eq!(run.metrics.peak_open_input_files, 1);
        assert_eq!(run.metrics.peak_business_queue_rows, 1);
        assert_eq!(run.metrics.peak_hot_markets, 1);
        assert_eq!(run.metrics.parallelism, 1);
        assert!(run.metrics.sort.peak_run_rows <= run.config.sort.max_run_rows);
        assert!(run.metrics.sort.peak_run_bytes <= run.config.sort.max_run_bytes);
        assert!(run.metrics.sort.peak_temp_run_files <= run.config.sort.max_temp_run_files);
        assert!(run.metrics.sort.peak_open_files <= run.config.sort.max_open_files);
        assert!(run.metrics.sort.peak_heap_entries <= run.config.sort.merge_fan_in);
        assert!(run.metrics.sort.peak_write_buffer_rows <= run.config.sort.batch_rows);
        assert!(run.metrics.sort.peak_temp_bytes <= run.config.sort.max_temp_bytes);
    }
    assert!(first.temporary_sorted_input_path.is_file());
    assert!(second.temporary_sorted_input_path.is_file());
    assert_ne!(
        first.metrics.sort.initial_runs,
        second.metrics.sort.initial_runs
    );

    let failure_root = TempDir::new().expect("创建超限运行临时根");
    let mut over_limit = config(8_192, 2_048, 4, 1_024);
    over_limit.max_decode_batch_bytes = 1;
    let error = reduce_frozen_input_to_staging(&input, &scope, failure_root.path(), &over_limit)
        .expect_err("真实 Arrow 批次超过冻结字节上限时必须失败");
    assert!(matches!(
        error,
        CanonicalRunError::TypedRaw(TypedRawReadError::DecodeBatchBytesLimit { limit: 1, .. })
    ));
    assert_eq!(
        std::fs::read_dir(failure_root.path())
            .expect("读取失败运行临时根")
            .count(),
        0,
        "超限失败不得留下半成功暂存目录",
    );
}
