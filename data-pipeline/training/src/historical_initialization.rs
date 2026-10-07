//! 显式市场元数据前序选择；文件事实来自原清单，不签发全日准入。
use crate::historical_preparation_progress::{
    HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1,
    HistoricalRawPassV1 as Pass, notify,
};
use crate::{public_input::*, public_recording::RecordingResult};
use contracts::*;
use data_pipeline_core::{
    public_input::*,
    public_reference::source_evidence_v3_from_typed_raw_v2,
    typed_raw::{PolymarketMarketMetadataRow, PolymarketMetadataRowKind, TypedRawRow},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

pub const HISTORICAL_INITIALIZATION_SOURCE_IDENTITY_VERSION: &str =
    "historical-metadata-initialization-source-v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataInitializationFileSelectionV1 {
    pub manifest_path: PathBuf,
    pub relative_paths: Vec<PathBuf>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataInitializationMarketV1 {
    pub condition_id: ConditionId,
    pub snapshot_source_message_id: Sha256Hex,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataInitializationConfigV1 {
    pub files: Vec<MetadataInitializationFileSelectionV1>,
    pub markets: Vec<MetadataInitializationMarketV1>,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    pub purpose: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataInitializationFileFactsV1 {
    pub manifest_sha256: Sha256Hex,
    pub relative_path: PathBuf,
    pub file: PublicFiniteFileV1,
}
#[derive(Deserialize)]
struct Manifest {
    files: Vec<ManifestFile>,
}
#[derive(Deserialize)]
struct ManifestFile {
    path: PathBuf,
    source_kind: String,
    schema_version: String,
    bytes: u64,
    rows: u64,
    sha256: String,
}
type MetadataOrder = (i64, u64, String);
fn existing(row: &TypedRawRowV2) -> Option<&PolymarketMarketMetadataRow> {
    match row {
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row)) => Some(row),
        TypedRawRowV2::PolymarketMarketMetadataV6(row) => match row.as_ref() {
            PolymarketMarketMetadataV6Input::Existing(row) => Some(row),
            _ => None,
        },
        _ => None,
    }
}
/// 元数据无连接 epoch；初始化与直接研究共用准入时刻、序号、公共来源身份排序。
pub fn metadata_admission_order_v1(
    admitted: i64,
    sequence: u64,
    id: &Sha256Hex,
) -> (i64, u64, String) {
    (admitted, sequence, id.0.clone())
}
fn order(row: &PolymarketMarketMetadataRow, id: &Sha256Hex) -> MetadataOrder {
    metadata_admission_order_v1(
        row.collector_admission_time_utc_ms,
        row.collector_admission_sequence,
        id,
    )
}
/// 只保存文件事实及目标Snapshot坐标，不缓存元数据载荷或市场状态。
pub struct MetadataInitializationSelectionV1 {
    pub files: Vec<MetadataInitializationFileFactsV1>,
    starts: BTreeMap<String, MetadataOrder>,
    received_start: i64,
}
impl MetadataInitializationSelectionV1 {
    pub fn open(
        config: &MetadataInitializationConfigV1,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        Self::open_with_progress(config, rows, bytes, None)
    }
    pub fn open_with_progress(
        config: &MetadataInitializationConfigV1,
        rows: usize,
        bytes: usize,
        progress: Option<&HistoricalPreparationNotifierV1<'_>>,
    ) -> RecordingResult<Self> {
        let mut files = Vec::new();
        let mut seen = BTreeSet::new();
        for selection in &config.files {
            let manifest_path = std::fs::canonicalize(&selection.manifest_path)?;
            let raw = crate::read_daily_manifest(&manifest_path)?;
            let manifest: Manifest = serde_json::from_slice(&raw)?;
            let manifest_sha256 = Sha256Hex(format!("{:x}", Sha256::digest(&raw)));
            for relative in &selection.relative_paths {
                let facts = manifest
                    .files
                    .iter()
                    .find(|file| &file.path == relative)
                    .ok_or("初始化文件不在指定原清单中")?;
                if facts.source_kind != "polymarket_market_metadata" {
                    return Err("初始化只接受市场元数据文件".into());
                }
                let path = std::fs::canonicalize(manifest_path.parent().unwrap().join(relative))?;
                if !seen.insert(path.clone()) {
                    open_finite_public_input_v1(
                        &PublicFiniteFileV1 {
                            path: path.clone(),
                            source_kind: facts.source_kind.clone(),
                            schema_version: facts.schema_version.clone(),
                            bytes: facts.bytes,
                            rows: facts.rows,
                            sha256: facts.sha256.clone(),
                        },
                        rows,
                        bytes,
                    )?;
                    continue;
                }
                files.push(MetadataInitializationFileFactsV1 {
                    manifest_sha256: manifest_sha256.clone(),
                    relative_path: relative.clone(),
                    file: PublicFiniteFileV1 {
                        path,
                        source_kind: facts.source_kind.clone(),
                        schema_version: facts.schema_version.clone(),
                        bytes: facts.bytes,
                        rows: facts.rows,
                        sha256: facts.sha256.clone(),
                    },
                });
            }
        }
        let mut starts = BTreeMap::new();
        let mut found = BTreeSet::new();
        for file in &files {
            for batch in open_finite_public_input_v1(&file.file, rows, bytes)? {
                let batch = batch?;
                notify(
                    progress,
                    Notice::RawBatch {
                        pass: Pass::InitializationSelection,
                        initialization: true,
                        rows: batch.len() as u64,
                    },
                );
                for row in batch {
                    let Some(metadata) = existing(&row) else {
                        continue;
                    };
                    let source = source_evidence_v3_from_typed_raw_v2(&row)?;
                    for target in &config.markets {
                        if source.source_message_id != target.snapshot_source_message_id {
                            continue;
                        }
                        if metadata.condition_id != target.condition_id.0
                            || metadata.metadata_row_kind
                                != PolymarketMetadataRowKind::MetadataSnapshot
                            || source.received_at_utc_ms >= config.received_start_utc_ms
                        {
                            return Err("指定初始化Snapshot与市场、类型或研究起点不匹配".into());
                        }
                        let key = order(metadata, &source.source_message_id);
                        starts
                            .entry(target.condition_id.0.clone())
                            .and_modify(|prior| {
                                if key < *prior {
                                    *prior = key.clone();
                                }
                            })
                            .or_insert(key);
                        found.insert((
                            target.condition_id.0.clone(),
                            target.snapshot_source_message_id.0.clone(),
                        ));
                    }
                }
            }
            notify(
                progress,
                Notice::RawFileComplete {
                    pass: Pass::InitializationSelection,
                    initialization: true,
                },
            );
        }
        if config.markets.iter().any(|target| {
            !found.contains(&(
                target.condition_id.0.clone(),
                target.snapshot_source_message_id.0.clone(),
            ))
        }) {
            return Err("指定初始化Snapshot未在所选文件中找到".into());
        }
        Ok(Self {
            files,
            starts,
            received_start: config.received_start_utc_ms,
        })
    }
    pub fn read(
        &self,
        rows: usize,
        bytes: usize,
    ) -> impl Iterator<Item = RecordingResult<(TypedRawRowV2, Option<SourceArtifactBindingV1>)>> + '_
    {
        self.read_with_progress(rows, bytes, None, Pass::SecondSupply)
    }
    pub fn read_with_progress<'a>(
        &'a self,
        rows: usize,
        bytes: usize,
        progress: Option<&'a HistoricalPreparationNotifierV1<'_>>,
        pass: Pass,
    ) -> impl Iterator<Item = RecordingResult<(TypedRawRowV2, Option<SourceArtifactBindingV1>)>> + 'a
    {
        self.files.iter().flat_map(move |file| {
            let stream: Box<dyn Iterator<Item = RecordingResult<TypedRawRowV2>>> =
                match open_finite_public_input_v1(&file.file, rows, bytes) {
                    Ok(stream) => Box::new(stream.flat_map(move |batch| match batch {
                        Ok(rows) => {
                            notify(
                                progress,
                                Notice::RawBatch {
                                    pass,
                                    initialization: true,
                                    rows: rows.len() as u64,
                                },
                            );
                            rows.into_iter().map(Ok).collect::<Vec<_>>()
                        }
                        Err(error) => vec![Err(error.into())],
                    })),
                    Err(error) => Box::new(std::iter::once(Err(error.into()))),
                };
            stream
                .filter_map(move |row| {
                    let selected = (|| -> RecordingResult<Option<_>> {
                        let row = row?;
                        let Some(metadata) = existing(&row) else {
                            return Ok(None);
                        };
                        let Some(start) = self.starts.get(&metadata.condition_id) else {
                            return Ok(None);
                        };
                        let source = source_evidence_v3_from_typed_raw_v2(&row)?;
                        if source.received_at_utc_ms >= self.received_start
                            || order(metadata, &source.source_message_id) < *start
                        {
                            return Ok(None);
                        }
                        let binding = SourceArtifactBindingV1 {
                            source_message_id: source.source_message_id,
                            manifest_sha256: file.manifest_sha256.clone(),
                            file_sha256: Sha256Hex(file.file.sha256.clone()),
                            relative_path: file.relative_path.to_string_lossy().into_owned(),
                        };
                        notify(
                            progress,
                            Notice::SelectedRow {
                                pass,
                                initialization: true,
                            },
                        );
                        Ok(Some((row, Some(binding))))
                    })();
                    match selected {
                        Ok(None) => None,
                        Ok(Some(row)) => Some(Ok(row)),
                        Err(error) => Some(Err(error)),
                    }
                })
                .chain(
                    std::iter::once_with(move || {
                        notify(
                            progress,
                            Notice::RawFileComplete {
                                pass,
                                initialization: true,
                            },
                        );
                        None
                    })
                    .flatten(),
                )
        })
    }
    /// v1组合身份只编码原日集合、真实文件事实和显式选择；物理根路径不参与。
    pub fn source_identity(
        &self,
        daily_identity: &str,
        config: &MetadataInitializationConfigV1,
    ) -> RecordingResult<Sha256Hex> {
        let mut files = self
            .files
            .iter()
            .map(|file| {
                (
                    &file.manifest_sha256,
                    &file.relative_path,
                    &file.file.source_kind,
                    &file.file.schema_version,
                    file.file.bytes,
                    file.file.rows,
                    &file.file.sha256,
                )
            })
            .collect::<Vec<_>>();
        files.sort();
        let mut markets = config
            .markets
            .iter()
            .map(|market| (&market.condition_id.0, &market.snapshot_source_message_id.0))
            .collect::<Vec<_>>();
        markets.sort();
        markets.dedup();
        let encoded = serde_json::to_vec(&(
            daily_identity,
            files,
            markets,
            config.received_start_utc_ms,
            config.received_end_utc_ms,
            &config.purpose,
        ))?;
        let mut hash = Sha256::new();
        hash.update(HISTORICAL_INITIALIZATION_SOURCE_IDENTITY_VERSION.as_bytes());
        hash.update([0]);
        hash.update(encoded);
        Ok(Sha256Hex(format!("{:x}", hash.finalize())))
    }
}
