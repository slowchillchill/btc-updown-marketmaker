use super::*;
pub(super) const FORMAT: &str = "noe_arrow_parquet_v1";
pub(super) const SEGMENT_FORMAT: &str = "noe_arrow_parquet_segment_v1";
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct SegmentRange {
    pub file_name: String,
    pub row_start: u64,
    pub record_count: u64,
    pub source_watermark: SimV2SourceWatermarkV1,
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    physical_format: String,
    container_version: String,
    commit_sequence: u64,
    context_reference: ExecutionNormalizationContextReferenceV1,
    durable_available_at_utc_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment: Option<SegmentRange>,
}
#[derive(Serialize, Deserialize)]
struct EvidenceBody<T0, T1, T2, T3> {
    logical_identity: T0,
    source_order: T1,
    source_reference: T2,
    evidence: T3,
}
#[derive(Serialize, Deserialize)]
struct EconomicFactBody<T0, T1> {
    source_order: T0,
    fact: T1,
}
#[derive(Serialize, Deserialize)]
struct DuplicateBody<T0, T1, T2, T3> {
    logical_identity: T0,
    source_order: T1,
    source_reference: T2,
    original_source_reference: T3,
}
#[derive(Serialize, Deserialize)]
struct EconomicAliasBody<T0, T1, T2, T3, T4, T5, T6> {
    logical_identity: T0,
    source_order: T1,
    source_reference: T2,
    decision: T3,
    economic_identity: T4,
    primary_logical_identity: T5,
    fact_reference: T6,
}
#[derive(Serialize, Deserialize)]
struct PendingBody<T0, T1, T2, T3, T4> {
    logical_identity: T0,
    source_order: T1,
    source_reference: T2,
    evidence: T3,
    scope: T4,
}
#[derive(Serialize, Deserialize)]
struct ConflictBody<T0, T1, T2, T3, T4> {
    logical_identity: T0,
    source_order: T1,
    source_reference: T2,
    scope: T3,
    conflicting_evidence: T4,
}

fn parquet_error(path: &Path, error: impl fmt::Display) -> DurableErrorV1 {
    DurableErrorV1::ContainerDecode {
        path: path.to_owned(),
        message: error.to_string(),
    }
}
pub(super) fn write_batch<MT: Serialize, MC: Serialize, I: Serialize, F: Serialize>(
    path: &Path,
    source_watermark: &SimV2SourceWatermarkV1,
    records: &[NormalizationRecord<MT, MC, I, F>],
) -> Result<(), DurableErrorV1> {
    let rows = encode_rows(path, records)?;
    let boundary = serde_json::to_string(source_watermark).map_err(|e| parquet_error(path, e))?;
    crate::record_file::write(path, FORMAT, &boundary, &rows)
        .map_err(|e| io_error("写入归一化 Parquet 批", path, e))
}
pub(super) fn encode_rows<MT: Serialize, MC: Serialize, I: Serialize, F: Serialize>(
    path: &Path,
    records: &[NormalizationRecord<MT, MC, I, F>],
) -> Result<Vec<crate::record_file::Row>, DurableErrorV1> {
    records
        .iter()
        .enumerate()
        .map(|(ordinal, record)| {
            let (kind, body) = match record {
                NormalizationRecord::Evidence {
                    logical_identity,
                    source_order,
                    source_reference,
                    evidence,
                } => (
                    "Evidence",
                    serde_json::to_vec(&EvidenceBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        evidence,
                    }),
                ),
                NormalizationRecord::EconomicFact { source_order, fact } => (
                    "EconomicFact",
                    serde_json::to_vec(&EconomicFactBody { source_order, fact }),
                ),
                NormalizationRecord::EconomicFactV2 { source_order, fact } => (
                    "EconomicFactV2",
                    serde_json::to_vec(&EconomicFactBody { source_order, fact }),
                ),
                NormalizationRecord::Duplicate {
                    logical_identity,
                    source_order,
                    source_reference,
                    original_source_reference,
                } => (
                    "Duplicate",
                    serde_json::to_vec(&DuplicateBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        original_source_reference,
                    }),
                ),
                NormalizationRecord::EconomicAlias {
                    logical_identity,
                    source_order,
                    source_reference,
                    decision,
                    economic_identity,
                    primary_logical_identity,
                    fact_reference,
                } => (
                    "EconomicAlias",
                    serde_json::to_vec(&EconomicAliasBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        decision,
                        economic_identity,
                        primary_logical_identity,
                        fact_reference,
                    }),
                ),
                NormalizationRecord::Pending {
                    logical_identity,
                    source_order,
                    source_reference,
                    evidence,
                    scope,
                } => (
                    "Pending",
                    serde_json::to_vec(&PendingBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        evidence,
                        scope,
                    }),
                ),
                NormalizationRecord::Conflict {
                    logical_identity,
                    source_order,
                    source_reference,
                    scope,
                    conflicting_evidence,
                } => (
                    "Conflict",
                    serde_json::to_vec(&ConflictBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        scope,
                        conflicting_evidence,
                    }),
                ),
            };
            Ok((
                ordinal as u64,
                kind.to_owned(),
                body.map_err(|e| parquet_error(path, e))?,
            ))
        })
        .collect::<Result<Vec<_>, DurableErrorV1>>()
}
pub(super) fn read_batch<
    T: NoeTradePayload<C>,
    C: Clone + fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    path: &Path,
) -> Result<crate::core::NormalizationBatch<T, C>, DurableErrorV1> {
    let (boundary, rows) =
        crate::record_file::read(path, FORMAT).map_err(|e| parquet_error(path, e))?;
    let source_watermark = serde_json::from_str(&boundary).map_err(|e| parquet_error(path, e))?;
    let records = decode_rows::<T, C>(path, rows)?;
    Ok(crate::core::NormalizationBatch {
        source_watermark,
        records,
    })
}
pub(super) fn decode_rows<
    T: NoeTradePayload<C>,
    C: Clone + fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    path: &Path,
    rows: Vec<crate::record_file::Row>,
) -> Result<Vec<NormalizationRecord<T, C>>, DurableErrorV1> {
    rows.into_iter()
        .enumerate()
        .map(|(expected, (seq, kind, body))| {
            if seq != expected as u64 {
                return Err(parquet_error(path, "记录 ordinal 不连续"));
            }
            match kind.as_str() {
                "Evidence" => {
                    let EvidenceBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        evidence,
                    } = serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::Evidence {
                        logical_identity,
                        source_order,
                        source_reference,
                        evidence,
                    })
                }
                "EconomicFact" => {
                    let EconomicFactBody { source_order, fact } =
                        serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::EconomicFact { source_order, fact })
                }
                "EconomicFactV2" => {
                    let EconomicFactBody { source_order, fact } =
                        serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::EconomicFactV2 { source_order, fact })
                }
                "Duplicate" => {
                    let DuplicateBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        original_source_reference,
                    } = serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::Duplicate {
                        logical_identity,
                        source_order,
                        source_reference,
                        original_source_reference,
                    })
                }
                "EconomicAlias" => {
                    let EconomicAliasBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        decision,
                        economic_identity,
                        primary_logical_identity,
                        fact_reference,
                    } = serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::EconomicAlias {
                        logical_identity,
                        source_order,
                        source_reference,
                        decision,
                        economic_identity,
                        primary_logical_identity,
                        fact_reference,
                    })
                }
                "Pending" => {
                    let PendingBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        evidence,
                        scope,
                    } = serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::Pending {
                        logical_identity,
                        source_order,
                        source_reference,
                        evidence,
                        scope,
                    })
                }
                "Conflict" => {
                    let ConflictBody {
                        logical_identity,
                        source_order,
                        source_reference,
                        scope,
                        conflicting_evidence,
                    } = serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
                    Ok(NormalizationRecord::Conflict {
                        logical_identity,
                        source_order,
                        source_reference,
                        scope,
                        conflicting_evidence,
                    })
                }
                _ => Err(parquet_error(path, format!("未知 NOE 记录变体：{kind}"))),
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

fn manifest_path(path: &Path) -> PathBuf {
    path.with_extension("manifest.json")
}
pub(super) fn read_commit<
    T: NoeTradePayload<C>,
    C: Clone + fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    path: &Path,
) -> Result<CommitContainer<T, C>, DurableErrorV1> {
    let manifest: Manifest = decode_container(&manifest_path(path))?;
    if manifest.physical_format != FORMAT && manifest.physical_format != SEGMENT_FORMAT {
        return Err(parquet_error(path, "不支持的 NOE 物理格式"));
    }
    let batch = if let Some(segment) = &manifest.segment {
        let physical = segment_path(path, segment)?;
        let rows = crate::record_file::read_range(
            &physical,
            SEGMENT_FORMAT,
            segment.row_start,
            segment.record_count,
        )
        .map_err(|e| parquet_error(&physical, e))?;
        crate::core::NormalizationBatch {
            source_watermark: segment.source_watermark.clone(),
            records: decode_rows::<T, C>(path, rows)?,
        }
    } else {
        read_batch(path)?
    };
    Ok(CommitContainer {
        payload: CommitPayload {
            container_version: manifest.container_version,
            encoding: NOE_CONTAINER_ENCODING_V1.into(),
            commit_sequence: manifest.commit_sequence,
            boundary: NoeSourceCommitBoundaryV1 {
                context_reference: manifest.context_reference,
                source_watermark: batch.source_watermark,
                durable_available_at_utc_ms: manifest.durable_available_at_utc_ms,
            },
            records: batch.records,
        },
    })
}
pub(super) fn read_position(
    path: &Path,
    expected_version: &str,
) -> Result<(NoeSourceCommitBoundaryV1, u64), DurableErrorV1> {
    let manifest: Manifest = decode_container(&manifest_path(path))?;
    if manifest.physical_format != FORMAT && manifest.physical_format != SEGMENT_FORMAT {
        return Err(parquet_error(path, "不支持的 NOE 物理格式"));
    }
    check_container_header(
        path,
        &manifest.container_version,
        expected_version,
        NOE_CONTAINER_ENCODING_V1,
    )?;
    let file_sequence = commit_sequence_from_path(path)?;
    if manifest.commit_sequence != file_sequence {
        return Err(DurableErrorV1::CommitSequenceMismatch {
            expected: file_sequence,
            found: manifest.commit_sequence,
        });
    }
    let (source_watermark, rows) = if let Some(segment) = &manifest.segment {
        let physical = segment_path(path, segment)?;
        let (_, total) = crate::record_file::read_metadata(&physical, SEGMENT_FORMAT)
            .map_err(|e| parquet_error(&physical, e))?;
        if segment
            .row_start
            .checked_add(segment.record_count)
            .is_none_or(|end| end > total)
        {
            return Err(parquet_error(path, "逻辑批范围超出共享段"));
        }
        (segment.source_watermark.clone(), segment.record_count)
    } else {
        let (watermark, rows) =
            crate::record_file::read_metadata(path, FORMAT).map_err(|e| parquet_error(path, e))?;
        (
            serde_json::from_str(&watermark).map_err(|e| parquet_error(path, e))?,
            rows,
        )
    };
    Ok((
        NoeSourceCommitBoundaryV1 {
            context_reference: manifest.context_reference,
            source_watermark,
            durable_available_at_utc_ms: manifest.durable_available_at_utc_ms,
        },
        rows,
    ))
}

pub(super) fn publish(
    stage: &Path,
    target: &Path,
    container_version: &str,
    commit_sequence: u64,
    boundary: &NoeSourceCommitBoundaryV1,
    fault: PublishFaultV1,
) -> Result<(), DurableErrorV1> {
    let directory = target.parent().expect("提交目录");
    let parent =
        File::open(directory).map_err(|e| io_error("打开 Parquet 提交目录", directory, e))?;
    rustix::fs::renameat_with(
        &parent,
        stage.file_name().unwrap(),
        &parent,
        target.file_name().unwrap(),
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|e| {
        if e == rustix::io::Errno::EXIST {
            DurableErrorV1::CommitAlreadyExists {
                commit_sequence: commit_sequence,
            }
        } else {
            io_error("发布归一化 Parquet", target, e.into())
        }
    })?;
    parent
        .sync_all()
        .map_err(|e| io_error("同步 Parquet 目录", directory, e))?;
    let manifest = Manifest {
        physical_format: FORMAT.into(),
        container_version: container_version.to_owned(),
        commit_sequence: commit_sequence,
        context_reference: boundary.context_reference.clone(),
        durable_available_at_utc_ms: boundary.durable_available_at_utc_ms,
        segment: None,
    };
    let path = manifest_path(target);
    let name = path.file_name().unwrap().to_str().unwrap();
    publish_no_replace(
        directory,
        &format!(".{name}.tmp"),
        name,
        &encode_container(&manifest, &path)?,
        Some(manifest.commit_sequence),
        fault,
    )
}

fn segment_path(logical: &Path, segment: &SegmentRange) -> Result<PathBuf, DurableErrorV1> {
    let name = Path::new(&segment.file_name);
    if name.components().count() != 1
        || !segment.file_name.starts_with("segment-")
        || !segment.file_name.ends_with(".parquet")
    {
        return Err(parquet_error(logical, "共享段文件名无效"));
    }
    Ok(logical.parent().expect("提交目录").join(name))
}
pub(super) fn publish_segment_manifest(
    target: &Path,
    version: &str,
    sequence: u64,
    boundary: &NoeSourceCommitBoundaryV1,
    segment: SegmentRange,
    fault: PublishFaultV1,
) -> Result<(), DurableErrorV1> {
    let manifest = Manifest {
        physical_format: SEGMENT_FORMAT.into(),
        container_version: version.into(),
        commit_sequence: sequence,
        context_reference: boundary.context_reference.clone(),
        durable_available_at_utc_ms: boundary.durable_available_at_utc_ms,
        segment: Some(segment),
    };
    let path = manifest_path(target);
    let name = path.file_name().unwrap().to_str().unwrap();
    publish_no_replace(
        target.parent().unwrap(),
        &format!(".{name}.tmp"),
        name,
        &encode_container(&manifest, &path)?,
        Some(sequence),
        fault,
    )
}
/// 截尾只回收不被保留逻辑提交引用的、明确归此 owner 的整段。
pub(super) fn discard_unreferenced_segments(directory: &Path) -> Result<(), DurableErrorV1> {
    let mut referenced = std::collections::BTreeSet::new();
    for logical in published_commit_paths(directory)? {
        if logical.extension().is_some_and(|e| e == "parquet") {
            let manifest: Manifest = decode_container(&manifest_path(&logical))?;
            if let Some(segment) = manifest.segment {
                referenced.insert(segment.file_name);
            }
        }
    }
    for entry in fs::read_dir(directory).map_err(|e| io_error("读取共享段尾", directory, e))?
    {
        let entry = entry.map_err(|e| io_error("读取共享段项", directory, e))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let number = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".parquet"));
        if number.is_some_and(|s| s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()))
            && !referenced.contains(name.as_ref())
        {
            fs::remove_file(entry.path())
                .map_err(|e| io_error("回收未引用NOE共享段", &entry.path(), e))?;
        }
    }
    Ok(())
}

pub(super) fn read_record<
    T: NoeTradePayload<C>,
    C: Clone + fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    path: &Path,
    ordinal: u64,
) -> Result<(CommitContainer<T, C>, u64), DurableErrorV1> {
    let manifest: Manifest = decode_container(&manifest_path(path))?;
    let Some(segment) = &manifest.segment else {
        let mut container = read_commit(path)?;
        let count = container.payload.records.len() as u64;
        let record = container
            .payload
            .records
            .into_iter()
            .nth(ordinal as usize)
            .ok_or_else(|| DurableErrorV1::RecordOrdinalOutOfRange {
                path: path.to_owned(),
                record_ordinal: ordinal,
                record_count: count,
            })?;
        container.payload.records = vec![record];
        return Ok((container, count));
    };
    if ordinal >= segment.record_count {
        return Err(DurableErrorV1::RecordOrdinalOutOfRange {
            path: path.to_owned(),
            record_ordinal: ordinal,
            record_count: segment.record_count,
        });
    }
    if manifest.physical_format != SEGMENT_FORMAT {
        return Err(parquet_error(path, "不支持的NOE物理格式"));
    }
    let physical = segment_path(path, segment)?;
    let row = segment
        .row_start
        .checked_add(ordinal)
        .ok_or_else(|| parquet_error(path, "NOE记录范围溢出"))?;
    let mut rows = crate::record_file::read_range(&physical, SEGMENT_FORMAT, row, 1)
        .map_err(|e| parquet_error(&physical, e))?;
    if rows.len() != 1 || rows[0].0 != ordinal {
        return Err(parquet_error(path, "记录 ordinal 不连续"));
    }
    rows[0].0 = 0; // 单行解码沿原变体校验；逻辑 ordinal 留在真实引用中。
    let records = decode_rows::<T, C>(path, rows)?;
    Ok((
        CommitContainer {
            payload: CommitPayload {
                container_version: manifest.container_version,
                encoding: NOE_CONTAINER_ENCODING_V1.into(),
                commit_sequence: manifest.commit_sequence,
                boundary: NoeSourceCommitBoundaryV1 {
                    context_reference: manifest.context_reference,
                    source_watermark: segment.source_watermark.clone(),
                    durable_available_at_utc_ms: manifest.durable_available_at_utc_ms,
                },
                records,
            },
        },
        segment.record_count,
    ))
}

#[derive(Default)]
pub(super) struct HistoryReader {
    segment: Option<(PathBuf, crate::record_file::SequentialReader)>,
}
impl HistoryReader {
    pub fn read<
        T: NoeTradePayload<C>,
        C: Clone + fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
    >(
        &mut self,
        path: &Path,
    ) -> Result<CommitContainer<T, C>, DurableErrorV1> {
        let manifest: Manifest = decode_container(&manifest_path(path))?;
        let Some(segment) = &manifest.segment else {
            return read_commit(path);
        };
        if manifest.physical_format != SEGMENT_FORMAT {
            return Err(parquet_error(path, "不支持的NOE物理格式"));
        }
        let physical = segment_path(path, segment)?;
        if self
            .segment
            .as_ref()
            .is_none_or(|(p, r)| *p != physical || r.position != segment.row_start)
        {
            let reader = crate::record_file::SequentialReader::open(
                &physical,
                SEGMENT_FORMAT,
                segment.row_start,
            )
            .map_err(|e| parquet_error(&physical, e))?;
            self.segment = Some((physical.clone(), reader));
        }
        let rows = self
            .segment
            .as_mut()
            .unwrap()
            .1
            .take(segment.record_count)
            .map_err(|e| parquet_error(&physical, e))?;
        let records = decode_rows::<T, C>(path, rows)?;
        Ok(CommitContainer {
            payload: CommitPayload {
                container_version: manifest.container_version,
                encoding: NOE_CONTAINER_ENCODING_V1.into(),
                commit_sequence: manifest.commit_sequence,
                boundary: NoeSourceCommitBoundaryV1 {
                    context_reference: manifest.context_reference,
                    source_watermark: segment.source_watermark.clone(),
                    durable_available_at_utc_ms: manifest.durable_available_at_utc_ms,
                },
                records,
            },
        })
    }
}
