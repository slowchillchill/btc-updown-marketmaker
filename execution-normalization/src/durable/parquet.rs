use super::*;
pub(super) const FORMAT: &str = "noe_arrow_parquet_v1";
#[derive(Serialize, Deserialize)]
struct Manifest {
    physical_format: String,
    container_version: String,
    commit_sequence: u64,
    context_reference: ExecutionNormalizationContextReferenceV1,
    durable_available_at_utc_ms: i64,
    boundary_sha256: contracts::Sha256Hex,
}
fn parquet_error(path: &Path, error: impl fmt::Display) -> DurableErrorV1 {
    DurableErrorV1::ContainerDecode {
        path: path.to_owned(),
        message: error.to_string(),
    }
}
pub(super) fn write_batch<MT: Serialize, MC: Serialize, I: Serialize, F: Serialize>(
    path: &Path,
    batch: &crate::core::NormalizationBatch<MT, MC, I, F>,
) -> Result<(), DurableErrorV1> {
    let rows = batch
        .records
        .iter()
        .enumerate()
        .map(|(ordinal, record)| {
            let value = serde_json::to_value(record).map_err(|e| parquet_error(path, e))?;
            let (kind, body) = value
                .as_object()
                .and_then(|v| v.iter().next())
                .expect("原外部标记记录");
            Ok((
                ordinal as u64,
                kind.clone(),
                serde_json::to_vec(body).map_err(|e| parquet_error(path, e))?,
            ))
        })
        .collect::<Result<Vec<_>, DurableErrorV1>>()?;
    let boundary =
        serde_json::to_string(&batch.source_watermark).map_err(|e| parquet_error(path, e))?;
    crate::record_file::write(path, FORMAT, &boundary, &rows)
        .map_err(|e| io_error("写入归一化 Parquet 批", path, e))
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
    let records = rows
        .into_iter()
        .enumerate()
        .map(|(expected, (seq, kind, body))| {
            if seq != expected as u64 {
                return Err(parquet_error(path, "记录 ordinal 不连续"));
            }
            let body: serde_json::Value =
                serde_json::from_slice(&body).map_err(|e| parquet_error(path, e))?;
            serde_json::from_value(serde_json::Value::Object(serde_json::Map::from_iter([(
                kind, body,
            )])))
            .map_err(|e| parquet_error(path, e))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(crate::core::NormalizationBatch {
        source_watermark,
        records,
    })
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
    if manifest.physical_format != FORMAT {
        return Err(parquet_error(path, "不支持的 NOE 物理格式"));
    }
    let batch = read_batch(path)?;
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
        boundary_sha256: manifest.boundary_sha256,
    })
}
pub(super) fn publish<
    T: NoeTradePayload<C>,
    C: Clone + fmt::Debug + Eq + Serialize + serde::de::DeserializeOwned,
>(
    stage: &Path,
    target: &Path,
    container: &CommitContainer<T, C>,
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
                commit_sequence: container.payload.commit_sequence,
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
        container_version: container.payload.container_version.clone(),
        commit_sequence: container.payload.commit_sequence,
        context_reference: container.payload.boundary.context_reference.clone(),
        durable_available_at_utc_ms: container.payload.boundary.durable_available_at_utc_ms,
        boundary_sha256: container.boundary_sha256.clone(),
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
