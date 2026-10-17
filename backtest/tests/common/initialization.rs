use backtest::*;
use contracts::*;
/// 工程夹具显式选择所需Snapshot；不增加区间外行情。
pub(crate) fn initialize_at(config: &mut HistoricalRunConfigV1, start: i64, conditions: &[&str]) {
    use data_pipeline_core::{public_input::*, public_reference::*};
    use data_pipeline_training::{historical_initialization::*, public_input::*};
    let manifest_path = config.preparation.scope.manifest_paths[0].clone();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let file = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|file| file["source_kind"] == "polymarket_market_metadata")
        .unwrap();
    let relative_path = std::path::PathBuf::from(file["path"].as_str().unwrap());
    let facts = PublicFiniteFileV1 {
        path: manifest_path.parent().unwrap().join(&relative_path),
        source_kind: file["source_kind"].as_str().unwrap().into(),
        schema_version: file["schema_version"].as_str().unwrap().into(),
        bytes: file["bytes"].as_u64().unwrap(),
        rows: file["rows"].as_u64().unwrap(),
        sha256: file["sha256"].as_str().unwrap().into(),
    };
    let mut markets = vec![];
    for batch in open_finite_public_input_v1(&facts, 1, 1 << 20).unwrap() {
        for row in batch.unwrap() {
            if let TypedRawRowV2::Existing(
                data_pipeline_core::typed_raw::TypedRawRow::PolymarketMarketMetadata(metadata),
            ) = &row
                && conditions.contains(&metadata.condition_id.as_str())
            {
                markets.push(MetadataInitializationMarketV1 {
                    condition_id: ConditionId(metadata.condition_id.clone()),
                    snapshot_source_message_id: source_evidence_v3_from_typed_raw_v2(&row)
                        .unwrap()
                        .source_message_id,
                });
            }
        }
    }
    config.preparation.scope.received_start_utc_ms = start;
    config.preparation.metadata_initialization = Some(MetadataInitializationConfigV1 {
        files: vec![MetadataInitializationFileSelectionV1 {
            manifest_path,
            relative_paths: vec![relative_path],
        }],
        markets,
        received_start_utc_ms: start,
        received_end_utc_ms: config.preparation.scope.received_end_utc_ms,
        purpose: "受控工程初始化验证".into(),
    });
}
