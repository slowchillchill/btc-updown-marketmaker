use std::path::Path;

// 此本地归档仅供具名 ignored 测试使用，不是生产输入资格。
const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
use data_pipeline_training::{
    validate_direct_source_daily_manifest_admission, validate_direct_source_daily_manifest_header,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct DailyManifestQualityFacts {
    state: String,
    gaps: Vec<String>,
    collection_day_coverage: CollectionDayCoverageFacts,
}

#[derive(Deserialize)]
struct CollectionDayCoverageFacts {
    reactivated_within_day: bool,
}

fn read_daily_manifest_quality_facts(utc_day: &str) -> DailyManifestQualityFacts {
    let manifest_path = Path::new(DATASET_LINEAGE_ROOT).join(format!(
        "{utc_day}/typed_raw/utc_day={utc_day}/manifest.json"
    ));
    let bytes = std::fs::read(&manifest_path).expect("真实每日清单必须可读取");
    serde_json::from_slice(&bytes).expect("真实每日清单必须包含质量事实")
}

#[test]
#[ignore = "requires the local finalized 2026-07-30 dataset"]
fn real_2026_07_30_daily_manifest_header_is_validated() {
    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-07-30/typed_raw/utc_day=2026-07-30/manifest.json");

    let header = validate_direct_source_daily_manifest_header(&manifest_path).unwrap();

    assert_eq!(header.schema_version(), "data_collect_daily_manifest_v3");
    assert_eq!(header.state(), "closed");
    assert_eq!(header.utc_day(), "2026-07-30");
    assert_eq!(header.closure_reason(), "utc_day_barrier");
    assert_eq!(
        header.coverage_schema_version(),
        "data_collect_collection_day_coverage_v1"
    );
    assert_eq!(header.coverage_state(), "ended_known");
    assert_eq!(header.first_started_utc_ms(), Some(1_785_369_600_000));
    assert_eq!(header.last_started_utc_ms(), Some(1_785_369_600_000));
    assert_eq!(header.ended_utc_ms(), Some(1_785_456_000_000));
    assert!(!header.reactivated_within_day());
}

#[test]
#[ignore = "requires the local finalized 2026-07-30 and 2026-07-31 datasets"]
fn real_daily_manifest_quality_facts_keep_closed_separate_from_gaps_and_reactivation() {
    let normal_day = read_daily_manifest_quality_facts("2026-07-30");
    let reactivated_day = read_daily_manifest_quality_facts("2026-07-31");

    assert_eq!(normal_day.state, "closed");
    assert!(normal_day.gaps.is_empty());
    assert!(!normal_day.collection_day_coverage.reactivated_within_day);

    assert_eq!(reactivated_day.state, "closed");
    assert_eq!(reactivated_day.gaps.len(), 6);
    assert!(
        reactivated_day
            .collection_day_coverage
            .reactivated_within_day
    );
}

#[test]
#[ignore = "requires the local finalized 2026-07-30 through 2026-08-02 datasets"]
fn real_four_day_daily_manifests_pass_complete_admission() {
    let expected_days = [
        ("2026-07-30", 121_u64, 43_741_059_u64, 2_658_237_003_u64),
        ("2026-07-31", 127_u64, 42_259_888_u64, 2_604_557_207_u64),
        ("2026-08-01", 121_u64, 31_336_754_u64, 1_892_535_675_u64),
        ("2026-08-02", 122_u64, 33_718_545_u64, 2_047_572_809_u64),
    ];
    let mut total_files = 0;
    let mut total_rows = 0;
    let mut total_bytes = 0;

    for (utc_day, expected_files, expected_rows, expected_bytes) in expected_days {
        let manifest_path = Path::new(DATASET_LINEAGE_ROOT).join(format!(
            "{utc_day}/typed_raw/utc_day={utc_day}/manifest.json"
        ));
        let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
            .expect("四日真实清单必须通过完整准入");

        assert_eq!(admitted.utc_day(), utc_day);
        assert_eq!(
            admitted.manifest_path(),
            manifest_path.canonicalize().unwrap()
        );
        assert!(!admitted.files().is_empty());

        let files = admitted.files();
        let rows = files.iter().map(|file| file.rows()).sum::<u64>();
        let bytes = files.iter().map(|file| file.bytes()).sum::<u64>();
        assert_eq!(files.len() as u64, expected_files);
        assert_eq!(rows, expected_rows);
        assert_eq!(bytes, expected_bytes);

        total_files += files.len() as u64;
        total_rows += rows;
        total_bytes += bytes;
    }

    assert_eq!(total_files, 491);
    assert_eq!(total_rows, 151_056_246);
    assert_eq!(total_bytes, 9_202_902_694);
}
