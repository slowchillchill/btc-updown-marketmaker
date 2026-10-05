//! 经生产typed读取器定位原Snapshot身份；只输出材料事实，不创建初始化政策。
use data_pipeline_core::{
    public_input::{PolymarketMarketMetadataV6Input, TypedRawRowV2},
    public_reference::source_evidence_v3_from_typed_raw_v2,
    typed_raw::{PolymarketMetadataRowKind, TypedRawRow},
};
use data_pipeline_training::public_input::{PublicFiniteFileV1, open_finite_public_input_v1};
use std::path::{Component, Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        return Err(
            "用法：inspect_metadata_snapshots <manifest> <before_utc_ms> <relative_file...>".into(),
        );
    }
    let manifest = PathBuf::from(&args[0]).canonicalize()?;
    let before: i64 = args[1].parse()?;
    let document: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest)?)?;
    let files = document["files"].as_array().ok_or("清单缺files")?;
    for relative in &args[2..] {
        if !Path::new(relative)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
        {
            return Err("仅接受显式日清单中的相对文件路径".into());
        }
        let facts = files
            .iter()
            .find(|file| file["path"] == *relative)
            .ok_or("文件不在日清单")?;
        if facts["source_kind"] != "polymarket_market_metadata" {
            return Err("只接受市场元数据文件".into());
        }
        let file = PublicFiniteFileV1 {
            path: manifest.parent().ok_or("清单缺父目录")?.join(relative),
            source_kind: facts["source_kind"].as_str().ok_or("缺source_kind")?.into(),
            schema_version: facts["schema_version"]
                .as_str()
                .ok_or("缺schema_version")?
                .into(),
            bytes: facts["bytes"].as_u64().ok_or("缺bytes")?,
            rows: facts["rows"].as_u64().ok_or("缺rows")?,
            sha256: facts["sha256"].as_str().ok_or("缺sha256")?.into(),
        };
        let mut count = 0;
        for batch in open_finite_public_input_v1(&file, 256, 64 * 1024 * 1024)? {
            for row in batch? {
                let metadata = match &row {
                    TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(value)) => {
                        Some(value.as_ref())
                    }
                    TypedRawRowV2::PolymarketMarketMetadataV6(value) => match value.as_ref() {
                        PolymarketMarketMetadataV6Input::Existing(value) => Some(value),
                        _ => None,
                    },
                    _ => None,
                };
                let Some(metadata) = metadata else { continue };
                if metadata.metadata_row_kind != PolymarketMetadataRowKind::MetadataSnapshot {
                    continue;
                }
                let source = source_evidence_v3_from_typed_raw_v2(&row)?;
                if source.received_at_utc_ms >= before {
                    continue;
                }
                println!(
                    "{}",
                    serde_json::json!({
                        "schema": "metadata_snapshot_location_v1", "utc_day":document["utc_day"],
                        "relative_path":relative,"file_sha256":file.sha256,
                        "condition_id":metadata.condition_id,
                        "snapshot_source_message_id":source.source_message_id,
                        "received_at_utc_ms":source.received_at_utc_ms,
                        "collector_admission_time_utc_ms":metadata.collector_admission_time_utc_ms,
                        "collector_admission_sequence":metadata.collector_admission_sequence,
                    })
                );
                count += 1;
            }
        }
        eprintln!("元数据文件核验完成：{relative}，起点前Snapshot {count}个");
    }
    Ok(())
}
