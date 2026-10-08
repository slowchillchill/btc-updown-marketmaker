//! 有限布局试验，只由 cfg(test) 父模块编译；不成为生产准入或恢复入口。
use super::*;
use crate::PartCursor;
use serde_json::{Value, json};
use std::{
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    time::Instant,
};

#[derive(Serialize, Deserialize)]
struct LayoutConfig {
    original_root: PathBuf,
    candidate_root: PathBuf,
    recording: GateAOutputConfig,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    purpose: String,
    #[serde(default)]
    payload_page_8m_zstd9: bool,
}
#[derive(Default, Serialize)]
struct PayloadStats {
    rows: u64,
    payload_bytes: u64,
    max_payload_bytes: usize,
    // 互斥上界桶，最后一桶无上界；只计载荷长度，不保留业务对象。
    length_buckets: BTreeMap<&'static str, u64>,
}
impl PayloadStats {
    fn add(&mut self, bytes: usize) {
        self.rows += 1;
        self.payload_bytes += bytes as u64;
        self.max_payload_bytes = self.max_payload_bytes.max(bytes);
        let bucket = match bytes {
            0..=1024 => "0..1024",
            1025..=4096 => "1025..4096",
            4097..=16384 => "4097..16384",
            16385..=65536 => "16385..65536",
            65537..=262144 => "65537..262144",
            262145..=1048576 => "262145..1048576",
            _ => ">1048576",
        };
        *self.length_buckets.entry(bucket).or_default() += 1;
    }
}
fn kind(row: &PublicStoredRowV1) -> &'static str {
    match row {
        PublicStoredRowV1::Input(_) => "Input",
        PublicStoredRowV1::Canonical { .. } => "Canonical",
        PublicStoredRowV1::BinanceDepthReference { .. } => "binance_depth_reference",
        PublicStoredRowV1::Report { .. } => "Report",
        PublicStoredRowV1::PmReport { .. } => "PmReport",
        PublicStoredRowV1::PmStepBinding(_) => "PmStepBinding",
        PublicStoredRowV1::GroupObservationRange(_) => "GroupObservationRange",
        PublicStoredRowV1::Group(_) => "Group",
        PublicStoredRowV1::Observation(_) => "Observation",
        PublicStoredRowV1::SourceBinding(_) => "SourceBinding",
    }
}
fn canonical_subtype(row: &PublicStoredRowV1) -> Option<&'static str> {
    let PublicStoredRowV1::Canonical { record, .. } = row else {
        return None;
    };
    Some(match record {
        CanonicalEventRecordV2::PolymarketMessage(_) => "PolymarketMessage",
        CanonicalEventRecordV2::MarketFact(_) => "MarketFact",
        CanonicalEventRecordV2::ReferenceEvent(event) => match &event.payload {
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(_) => {
                "ReferenceEvent.ChainlinkBtcUsd"
            }
            CanonicalReferenceEventPayloadV2::BinanceDepthState(_) => {
                "ReferenceEvent.BinanceDepthState"
            }
            CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(_) => {
                "ReferenceEvent.BinanceAggregateTrade"
            }
            CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(_) => {
                "ReferenceEvent.MarketPriceAnchorUpdate"
            }
            CanonicalReferenceEventPayloadV2::MarketSettlementReferenceUpdate(_) => {
                "ReferenceEvent.MarketSettlementReferenceUpdate"
            }
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_) => {
                "ReferenceEvent.ChainlinkBtcUsdTwap"
            }
            CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(_) => {
                "ReferenceEvent.RtdsObservedAnomaly"
            }
        },
    })
}
fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidData, message.into()).into()
}
#[derive(Default, Serialize)]
struct ColumnLayout {
    compressed_bytes: i64,
    uncompressed_bytes: i64,
    codecs: std::collections::BTreeSet<String>,
    encodings: std::collections::BTreeSet<String>,
}
fn file_layout(paths: &[PathBuf]) -> RecordingResult<Value> {
    // footer/文件元数据读取，不为容量统计遍历正文。
    let mut logical_file_bytes = 0u64;
    let mut allocated_file_bytes = 0u64;
    let mut footer_bytes = 0u64;
    let mut row_groups = 0usize;
    let mut rows = 0i64;
    let mut compressed = 0i64;
    let mut uncompressed = 0i64;
    let mut columns = BTreeMap::<String, ColumnLayout>::new();
    for path in paths {
        let mut file = File::open(path)?;
        let stat = file.metadata()?;
        logical_file_bytes += stat.len();
        allocated_file_bytes += stat.blocks() * 512;
        file.seek(SeekFrom::End(-8))?;
        let mut tail = [0; 8];
        file.read_exact(&mut tail)?;
        footer_bytes += u32::from_le_bytes(tail[..4].try_into()?) as u64;
        let metadata = crate::ArrowReaderMetadata::load(&file, Default::default())?;
        let groups = metadata.metadata().row_groups();
        row_groups += groups.len();
        rows += metadata.metadata().file_metadata().num_rows();
        for g in groups {
            for col in g.columns() {
                compressed += col.compressed_size();
                uncompressed += col.uncompressed_size();
                let entry = columns.entry(col.column_path().string()).or_default();
                entry.compressed_bytes += col.compressed_size();
                entry.uncompressed_bytes += col.uncompressed_size();
                // footer只保存codec，不保存压缩级别；不能将getter恢复的默认级别当实际级别。
                entry.codecs.insert(
                    format!("{:?}", col.compression())
                        .split('(')
                        .next()
                        .unwrap()
                        .to_owned(),
                );
                entry
                    .encodings
                    .extend(col.encodings().map(|e| format!("{e:?}")));
            }
        }
    }
    Ok(
        json!({"physical_files":paths.len(),"logical_file_bytes":logical_file_bytes,"allocated_file_bytes":allocated_file_bytes,
        "footer_bytes":footer_bytes,"row_groups":row_groups,
        "rows":rows,
        "compressed_column_bytes":compressed,"uncompressed_column_bytes":uncompressed,"columns":columns}),
    )
}
fn copy_descriptor(original: &Path, candidate: &Path, name: &str) -> RecordingResult<()> {
    fs::copy(original.join(name), candidate.join(name))?;
    File::open(candidate.join(name))?.sync_all()?;
    Ok(())
}
#[derive(Default, Serialize)]
struct Position {
    segment: Option<String>,
    row: u64,
}
fn recode(c: &LayoutConfig) -> (Value, RecordingResult<()>) {
    let started = Instant::now();
    let mut rows = 0u64;
    let mut kinds = BTreeMap::<&'static str, PayloadStats>::new();
    let mut canonical_subtypes = BTreeMap::<&'static str, PayloadStats>::new();
    let mut segments = vec![];
    let mut current = Position::default();
    let mut prefix = None;
    let result = (|| -> RecordingResult<()> {
        let reader = PublicRecordingReaderV1::recover(
            &c.original_root,
            c.read_page_rows,
            c.max_read_batch_bytes,
        )?;
        prefix = reader.prefix.clone();
        // 新根必须独立，create_dir 的原 OS 错误保留；不覆盖历史材料。
        fs::create_dir(&c.candidate_root)?;
        copy_descriptor(&c.original_root, &c.candidate_root, "stream.json")?;
        File::open(&c.candidate_root)?.sync_all()?;
        let quota = Arc::new(Mutex::new(0));
        for (ordinal, (path, committed)) in reader.stable_segments().into_iter().enumerate() {
            let name = &committed.stable_storage_ref;
            let stage = c.candidate_root.join(format!("pending-{ordinal:020}"));
            current.segment = Some(name.clone());
            current.row = 0;
            let mut writer = if c.payload_page_8m_zstd9 {
                SequentialCaptureWriter::create_payload_page_trial(
                    "public_recording_v1",
                    &stage,
                    quota.clone(),
                    &c.recording,
                )?
            } else {
                SequentialCaptureWriter::create_segment(
                    "public_recording_v1",
                    &stage,
                    quota.clone(),
                    &c.recording,
                )?
            };
            writer.defer_batch_sync();
            let mut cursor = PartCursor::open(
                &segment_run(&path)?,
                c.read_page_rows,
                c.max_read_batch_bytes,
            )?;
            let mut count = 0;
            while let Some(row) = cursor.next_row()? {
                current.row = count;
                let decoded: PublicStoredRowV1 = serde_json::from_slice(row.payload())?;
                kinds
                    .entry(kind(&decoded))
                    .or_default()
                    .add(row.payload().len());
                if let Some(subtype) = canonical_subtype(&decoded) {
                    canonical_subtypes
                        .entry(subtype)
                        .or_default()
                        .add(row.payload().len());
                }
                drop(decoded);
                writer.push(count, row.sort_key_bytes().to_vec(), row.payload())?;
                count += 1;
                rows += 1;
            }
            let (peak_rows, peak_bytes) = writer.write_buffer_peaks();
            writer.close_segment()?;
            // 原成功关闭之后才写稳定凭据，再沿原 sync/publish 责任发布。
            fs::copy(path.join("prefix.json"), stage.join("prefix.json"))?;
            sync_tree(&stage)?;
            commit_public_segment(stage, name)?;
            let layout = file_layout(&segment_run(&c.candidate_root.join(name))?.parts)?;
            segments.push(json!({"stable_storage_ref":name,"rows":count,
                "writer_peak_write_buffer_rows":peak_rows,"writer_peak_write_buffer_bytes":peak_bytes,"layout":layout}));
        }
        if c.original_root
            .join("historical-initialization.json")
            .exists()
        {
            copy_descriptor(
                &c.original_root,
                &c.candidate_root,
                "historical-initialization.json",
            )?;
            File::open(&c.candidate_root)?.sync_all()?;
        }
        Ok(())
    })();
    (
        json!({"schema":"public_recording_layout_recode_v1","effective_config":c,
        "status":if result.is_ok(){"success"}else{"error"},"original_error":result.as_ref().err().map(ToString::to_string),
        "current_position":current,"source_declared_prefix":prefix,"rows_written":rows,"payload_by_kind":kinds,"canonical_payload_by_subtype":canonical_subtypes,
        "segments":segments,"wall_seconds":started.elapsed().as_secs_f64(),
        "measurement_note":"classification decodes and releases only the current original row; Canonical subtype counts whole original stored-row payload length, not reserialized event/lineage; original key/payload bytes passed to writer without reserialization; footer-only layout metadata; peak is original capacity-counted row buffer, excludes Arrow/compressor/reader/process RSS"}),
        result,
    )
}
fn check_eof_prefix(
    reader: &mut PublicRecordingReaderV1,
    expected: &CommittedIngressPrefixV1,
) -> RecordingResult<()> {
    // 与原 next_row 的分片 EOF 完全相同，仅复用其最小收口，不另造计数模型。
    reader.observed.stable_storage_ref = expected.stable_storage_ref.clone();
    if reader.observed != *expected {
        return Err(invalid("稳定分片内容与前缀计数不符"));
    }
    Ok(())
}
fn compare(c: &LayoutConfig) -> (Value, RecordingResult<()>) {
    let started = Instant::now();
    let mut rows = 0u64;
    let mut segments = vec![];
    let mut current = Position::default();
    let mut original_prefix = None;
    let mut candidate_prefix = None;
    let result = (|| -> RecordingResult<()> {
        let mut original = PublicRecordingReaderV1::recover(
            &c.original_root,
            c.read_page_rows,
            c.max_read_batch_bytes,
        )?;
        let mut candidate = PublicRecordingReaderV1::recover(
            &c.candidate_root,
            c.read_page_rows,
            c.max_read_batch_bytes,
        )?;
        original_prefix = original.prefix.clone();
        candidate_prefix = candidate.prefix.clone();
        if original.header != candidate.header || original.prefix != candidate.prefix {
            return Err(invalid("布局对照 header/prefix 不相等"));
        }
        for name in ["stream.json", "historical-initialization.json"] {
            let old = c.original_root.join(name);
            let new = c.candidate_root.join(name);
            if old.exists() != new.exists() || (old.exists() && fs::read(old)? != fs::read(new)?) {
                return Err(invalid(format!("布局对照描述文件字节不相等：{name}")));
            }
        }
        let old_segments = original.stable_segments();
        let new_segments = candidate.stable_segments();
        if old_segments.len() != new_segments.len() {
            return Err(invalid("布局对照稳定分片数量不相等"));
        }
        for ((old, old_prefix), (new, new_prefix)) in old_segments.iter().zip(&new_segments) {
            if old_prefix != new_prefix
                || fs::read(old.join("prefix.json"))? != fs::read(new.join("prefix.json"))?
            {
                return Err(invalid("布局对照分片前缀字节不相等"));
            }
            let mut a =
                PartCursor::open(&segment_run(old)?, c.read_page_rows, c.max_read_batch_bytes)?;
            let mut b =
                PartCursor::open(&segment_run(new)?, c.read_page_rows, c.max_read_batch_bytes)?;
            let mut count = 0u64;
            current.segment = Some(old_prefix.stable_storage_ref.clone());
            loop {
                current.row = count;
                match (a.next_row()?, b.next_row()?) {
                    (Some(a), Some(b)) => {
                        if a.sort_key_bytes() != b.sort_key_bytes() {
                            return Err(invalid("布局对照 sort_key 字节不相等"));
                        }
                        if a.payload() != b.payload() {
                            return Err(invalid("布局对照 payload 字节不相等"));
                        }
                        let decoded: PublicStoredRowV1 = serde_json::from_slice(a.payload())?;
                        original.observe(&decoded)?;
                        candidate.observe(&decoded)?;
                        rows += 1;
                        count += 1;
                    }
                    (None, None) => break,
                    _ => return Err(invalid("布局对照单边 EOF，记录数量不相等")),
                }
            }
            check_eof_prefix(&mut original, old_prefix)?;
            check_eof_prefix(&mut candidate, new_prefix)?;
            segments.push(json!({"stable_storage_ref":old_prefix.stable_storage_ref,"rows":count,"double_eof":true,"complete_prefix_equal":true}));
        }
        Ok(())
    })();
    (
        json!({"schema":"public_recording_layout_compare_v1","effective_config":c,
        "status":if result.is_ok(){"success"}else{"error"},"original_error":result.as_ref().err().map(ToString::to_string),
        "current_position":current,"rows_compared":rows,"segments":segments,
        "original_prefix":original_prefix,"candidate_prefix":candidate_prefix,
        "double_eof":result.is_ok(),"wall_seconds":started.elapsed().as_secs_f64(),
        "measurement_note":"RunCursor original/new raw key+payload comparison to each double EOF; same traversal calls original PublicRecordingReader::observe and original EOF prefix closure. Does not traverse PublicRecordingReader::next_row I/O/error path; subsequent original replay tests actual reader consumption. No whole-row stream retained."}),
        result,
    )
}
fn persist(report: &Value, result: RecordingResult<()>, path: &Path) -> RecordingResult<()> {
    let written = (|| -> RecordingResult<()> {
        fs::write(path, serde_json::to_vec_pretty(report)?)?;
        Ok(())
    })();
    match (result, written) {
        (Err(original), Err(writing)) => {
            eprintln!("布局报告写入另有错误：{writing}");
            Err(original)
        }
        (Err(original), Ok(())) => Err(original),
        (Ok(()), Err(writing)) => Err(writing),
        (Ok(()), Ok(())) => Ok(()),
    }
}
fn configured() -> RecordingResult<(LayoutConfig, PathBuf)> {
    let path = std::env::var("DPT_RECORDING_LAYOUT_CONFIG")?;
    let report = std::env::var("DPT_RECORDING_LAYOUT_REPORT")?.into();
    Ok((serde_json::from_slice(&fs::read(path)?)?, report))
}
#[test]
#[ignore = "仅显式批准的既有十二分钟材料布局重编码；普通回归不运行"]
fn existing_twelve_minute_recode_4096() -> RecordingResult<()> {
    let (c, output) = configured()?;
    let (report, result) = recode(&c);
    persist(&report, result, &output)
}
#[test]
#[ignore = "仅显式批准的原新十二分钟材料字节/前缀比较；普通回归不运行"]
fn existing_twelve_minute_compare_bytes_and_prefix() -> RecordingResult<()> {
    let (c, output) = configured()?;
    let (report, result) = compare(&c);
    persist(&report, result, &output)
}
fn fixture(c: &LayoutConfig) {
    let mut core = core();
    let mut writer =
        PublicRecordingWriterV1::create(&c.original_root, core.header(), config()).unwrap();
    for input in [meta(0), end(1)] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
        writer.stabilize().unwrap();
    }
    fs::write(
        c.original_root.join("historical-initialization.json"),
        b"{\"fixture\":true}\n",
    )
    .unwrap();
}
fn fixture_config(root: &Path) -> LayoutConfig {
    let mut recording = config();
    recording.sort.batch_rows = 4096;
    LayoutConfig {
        original_root: root.join("original"),
        candidate_root: root.join("candidate"),
        recording,
        read_page_rows: 1,
        max_read_batch_bytes: 1 << 20,
        purpose: "原接口有效小夹具与原字节对照".into(),
        payload_page_8m_zstd9: false,
    }
}
#[test]
fn valid_original_construction_preserves_bytes_segments_and_complete_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let mut c = fixture_config(temp.path());
    c.payload_page_8m_zstd9 = true;
    fixture(&c);
    let (report, result) = recode(&c);
    result.unwrap();
    assert_eq!(report["segments"].as_array().unwrap().len(), 2);
    let canonical = &report["payload_by_kind"]["Canonical"];
    let parts = report["canonical_payload_by_subtype"].as_object().unwrap();
    assert_eq!(
        parts
            .values()
            .map(|v| v["rows"].as_u64().unwrap())
            .sum::<u64>(),
        canonical["rows"].as_u64().unwrap()
    );
    assert_eq!(
        parts
            .values()
            .map(|v| v["payload_bytes"].as_u64().unwrap())
            .sum::<u64>(),
        canonical["payload_bytes"].as_u64().unwrap()
    );
    let (compared, result) = compare(&c);
    result.unwrap();
    assert_eq!(compared["double_eof"], true);
    assert!(compared["original_prefix"]["normal_end"].as_bool().unwrap());
    let mut reader = PublicRecordingReaderV1::recover(&c.candidate_root, 1, 1 << 20).unwrap();
    let mut count = 0;
    while reader.next_row().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(compared["rows_compared"], count);
}
#[test]
fn missing_row_and_same_length_payload_change_are_precise_errors() {
    for omit in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let c = fixture_config(temp.path());
        fixture(&c);
        recode(&c).1.unwrap();
        let reader = PublicRecordingReaderV1::recover(&c.candidate_root, 1, 1 << 20).unwrap();
        let (segment, _) = &reader.stable_segments()[0];
        let path = segment.join("captured.parquet");
        let mut cursor = RunCursor::open(&path, 1, 1 << 20).unwrap();
        // 仅小夹具破坏准备，真实试验不保留第二流水。
        let mut rows = vec![];
        while let Some(row) = cursor.next_row().unwrap() {
            rows.push(row);
        }
        drop(cursor);
        let replacement = temp.path().join("replacement");
        let mut writer = SequentialCaptureWriter::create_segment(
            "public_recording_v1",
            &replacement,
            Arc::new(Mutex::new(0)),
            &c.recording,
        )
        .unwrap();
        writer.defer_batch_sync();
        let limit = rows.len() - usize::from(omit);
        for (n, row) in rows.iter().take(limit).enumerate() {
            let mut bytes = row.payload().to_vec();
            if !omit && n == 0 {
                bytes[0] = b'[';
            }
            writer
                .push(n as u64, row.sort_key_bytes().to_vec(), &bytes)
                .unwrap();
        }
        let rewritten = writer.close_segment().unwrap();
        fs::rename(rewritten, path).unwrap();
        let (report, result) = compare(&c);
        let error = result.unwrap_err().to_string();
        assert_eq!(
            error,
            if omit {
                "布局对照单边 EOF，记录数量不相等"
            } else {
                "布局对照 payload 字节不相等"
            }
        );
        assert_eq!(report["status"], "error");
        assert_eq!(report["double_eof"], false);
        assert!(report["current_position"]["row"].is_u64());
    }
}
