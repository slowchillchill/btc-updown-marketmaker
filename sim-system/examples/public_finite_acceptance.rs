//! 阶段5显式有限验收；配置与输入身份由运行前冻结，结果不是生产运行承诺。
#[path = "public_finite_acceptance/recorded_times.rs"]
mod recorded_times;
#[path = "public_finite_acceptance/simulation.rs"]
mod simulation;
#[path = "public_finite_acceptance/terminal_order.rs"]
mod terminal_order;
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*,
    public_reference::source_evidence_v3_from_typed_raw_v2, typed_raw::*,
};
use data_pipeline_training::{
    canonical_run::CanonicalRunConfig, gate_a::GateAOutputConfig, public_input::*,
    public_recording::*, public_replay::*, typed_raw::TypedRawParquetStream,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    files: Vec<PublicFiniteFileV1>,
    manifest_sha256: Sha256Hex,
    received_start: i64,
    received_end: i64,
    condition: Option<String>,
    omit_initial_books_epoch: Option<String>,
    canonical: CanonicalRunConfig,
    output: GateAOutputConfig,
    header: PublicInputStreamHeaderV1,
    checkpoints: Vec<u64>,
}
#[derive(Default, Serialize)]
struct DecodeMetrics {
    batches: u64,
    physical_rows: u64,
    peak_arrow_rows: usize,
    peak_arrow_bytes: usize,
    peak_decoded_vec_capacity: usize,
}
struct Rows<'a> {
    config: &'a Config,
    metrics: std::rc::Rc<std::cell::RefCell<DecodeMetrics>>,
    files: std::slice::Iter<'a, PublicFiniteFileV1>,
    stream: Option<TypedRawParquetStream<TypedRawRowV2>>,
    current_file: Option<&'a PublicFiniteFileV1>,
    bindings: Option<&'a std::cell::RefCell<PublicRecordingWriterV1>>,
    pending: std::vec::IntoIter<TypedRawRowV2>,
}
impl<'a> Rows<'a> {
    fn new(
        config: &'a Config,
        metrics: std::rc::Rc<std::cell::RefCell<DecodeMetrics>>,
        bindings: Option<&'a std::cell::RefCell<PublicRecordingWriterV1>>,
    ) -> Self {
        Self {
            config,
            metrics,
            files: config.files.iter(),
            stream: None,
            current_file: None,
            bindings,
            pending: Vec::new().into_iter(),
        }
    }
    fn selected(&self, row: &TypedRawRowV2) -> bool {
        if let TypedRawRowV2::Existing(row) = row {
            if let Some(condition) = &self.config.condition {
                let actual = match row {
                    TypedRawRow::PolymarketOrderbook(r) => &r.market,
                    TypedRawRow::PolymarketMarketMetadata(r) => &r.condition_id,
                    _ => return false,
                };
                if actual != condition {
                    return false;
                }
            }
            if let TypedRawRow::PolymarketOrderbook(r) = row {
                if self.config.omit_initial_books_epoch.as_ref()
                    == Some(&r.source_sequence_epoch_id)
                    && matches!(r.collector_admission_sequence, 1 | 2)
                    && matches!(r.event, PolymarketOrderbookEvent::Book { .. })
                {
                    return false;
                }
            }
        }
        true
    }
}
impl Iterator for Rows<'_> {
    type Item = RecordingResult<TypedRawRowV2>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.next() {
                if !self.selected(&row) {
                    continue;
                }
                let source = match source_evidence_v3_from_typed_raw_v2(&row) {
                    Ok(source) => source,
                    Err(error) => return Some(Err(error.into())),
                };
                if source.received_at_utc_ms < self.config.received_start
                    || source.received_at_utc_ms >= self.config.received_end
                {
                    continue;
                }
                if let Some(writer) = self.bindings {
                    let file = self.current_file.expect("当前行来自已打开文件");
                    let relative_path = format!(
                        "{}/{}",
                        file.path
                            .parent()
                            .unwrap()
                            .file_name()
                            .unwrap()
                            .to_string_lossy(),
                        file.path.file_name().unwrap().to_string_lossy()
                    );
                    if let Err(error) =
                        writer
                            .borrow_mut()
                            .record_source_binding(SourceArtifactBindingV1 {
                                source_message_id: source.source_message_id,
                                manifest_sha256: self.config.manifest_sha256.clone(),
                                file_sha256: Sha256Hex(file.sha256.clone()),
                                relative_path,
                            })
                    {
                        return Some(Err(error));
                    }
                }
                return Some(Ok(row));
            }
            if let Some(stream) = &mut self.stream {
                match stream.next() {
                    Some(Ok(batch)) => {
                        let mut metrics = self.metrics.borrow_mut();
                        metrics.batches += 1;
                        metrics.physical_rows += stream.last_physical_batch_rows() as u64;
                        metrics.peak_arrow_rows = metrics
                            .peak_arrow_rows
                            .max(stream.last_physical_batch_rows());
                        metrics.peak_arrow_bytes = metrics
                            .peak_arrow_bytes
                            .max(stream.last_physical_batch_bytes());
                        metrics.peak_decoded_vec_capacity =
                            metrics.peak_decoded_vec_capacity.max(batch.capacity());
                        self.pending = batch.into_iter();
                        continue;
                    }
                    Some(Err(error)) => return Some(Err(error.into())),
                    None => {}
                }
            }
            let file = self.files.next()?;
            self.current_file = Some(file);
            match open_finite_public_input_v1(
                file,
                self.config.canonical.decode_batch_rows,
                self.config.canonical.max_decode_batch_bytes,
            ) {
                Ok(stream) => self.stream = Some(stream),
                Err(error) => return Some(Err(error.into())),
            }
        }
    }
}
fn write_line(writer: &mut impl Write, value: &impl Serialize) -> RecordingResult<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}
struct DigestWriter {
    hash: sha2::Sha256,
    bytes: u64,
}
impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        use sha2::Digest;
        self.hash.update(bytes);
        self.bytes += bytes.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded_digest(value: &impl Serialize) -> RecordingResult<(String, u64)> {
    use sha2::Digest;
    let mut writer = DigestWriter {
        hash: sha2::Sha256::new(),
        bytes: 0,
    };
    serde_json::to_writer(&mut writer, value)?;
    Ok((format!("{:x}", writer.hash.finalize()), writer.bytes))
}
fn measure_result(
    result: &PublicAppliedIngressV1<'_>,
    writer: &mut impl Write,
) -> RecordingResult<()> {
    // 验收编码位于业务返回之后；逐输入完整结果流有界写出，不积累历史对象。
    let reports: Vec<_> = result.reductions.iter().map(|b| &b.reports).collect();
    let (digest, bytes) = encoded_digest(&(&result.applied, &result.pm_reductions, reports))?;
    let groups = result
        .applied
        .closed_groups
        .iter()
        .map(encoded_digest)
        .collect::<RecordingResult<Vec<_>>>()?;
    write_line(
        writer,
        &serde_json::json!({"complete_input_result_sha256": digest, "encoded_bytes": bytes, "ordered_groups": groups}),
    )
}
// 窄代际样本只保留本代最初14条的实际公开观测，供独立窗口断言。
fn measure_pm_boundary(
    input: &RecordedIngressViewV1<'_>,
    result: &PublicAppliedIngressV1<'_>,
    writer: &mut impl Write,
) -> RecordingResult<()> {
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
        ..
    } = &input.ingress
    else {
        return Ok(());
    };
    if row.collector_admission_sequence > 14 {
        return Ok(());
    }
    for group in &result.applied.closed_groups {
        for observed in &group.observations {
            write_line(
                writer,
                &serde_json::json!({
                    "input_position": input.input_position,
                    "epoch": row.source_sequence_epoch_id,
                    "source_sequence": row.collector_admission_sequence,
                    "source_event": row.event,
                    "visible_prefix": observed.visible_prefix,
                    "assets": observed.assets,
                    "lifecycle": observed.lifecycle,
                }),
            )?;
        }
    }
    Ok(())
}
// 快照和紧接快照编号的更新只作有限机制见证，全部输入仍完整归约。
fn measure_binance_boundary(
    input: &RecordedIngressViewV1<'_>,
    result: &PublicAppliedIngressV1<'_>,
    writer: &mut impl Write,
) -> RecordingResult<()> {
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(row),
        ..
    } = &input.ingress
    else {
        return Ok(());
    };
    let boundary = match row {
        TypedRawRow::BinanceDepthSnapshot(_) => true,
        TypedRawRow::BinanceDepthUpdate(row) => {
            row.local_u_before.is_some() && row.local_u_before == row.snapshot_last_update_id
        }
        _ => false,
    };
    if boundary {
        let reports: Vec<_> = result.reductions.iter().map(|r| &r.reports).collect();
        write_line(
            writer,
            &serde_json::json!({"input": input, "applied": result.applied, "reports": reports}),
        )?;
    }
    Ok(())
}
fn run() -> RecordingResult<()> {
    let args: Vec<_> = std::env::args().collect();
    if !(args.len() == 5 && args[1] != "simulate-hidden")
        && !(args.len() == 4 && args[1] == "core")
        && !(args.len() == 6 && args[1] == "simulate-hidden")
    {
        return Err("用法: public_finite_acceptance <core|record|replay|hidden|hidden-probe|simulate|simulate-hidden|terminal-order> <config.json> <output> [recording-root] [hidden-reference.json]（core无需记录目录，simulate-hidden使用既有隐藏准备引用）".into());
    }
    if args[1] == "simulate" || args[1] == "simulate-hidden" {
        return simulation::run(
            std::path::Path::new(&args[2]),
            std::path::Path::new(&args[3]),
            std::path::Path::new(&args[4]),
            args.get(5).map(std::path::Path::new),
        );
    }
    let config: Config = serde_json::from_reader(File::open(&args[2])?)?;
    let out = PathBuf::from(&args[3]);
    fs::create_dir(&out)?;
    serde_json::to_writer_pretty(File::create(out.join("configuration.json"))?, &config)?;
    let mut results = BufWriter::new(File::create(out.join("results.jsonl"))?);
    let mut metrics = BufWriter::new(File::create(out.join("checkpoints.jsonl"))?);
    let mut boundary = BufWriter::new(File::create(out.join("pm-boundary.jsonl"))?);
    let mut binance_boundary = BufWriter::new(File::create(out.join("binance-boundary.jsonl"))?);
    let recording = args.get(4).map(PathBuf::from).unwrap_or_default();
    if args[1] == "terminal-order" {
        terminal_order::run(&config, &out, &recording)?;
    } else if args[1] == "record" || args[1] == "core" {
        let decode = std::rc::Rc::new(std::cell::RefCell::new(DecodeMetrics::default()));
        let writer = if args[1] == "record" {
            Some(std::cell::RefCell::new(PublicRecordingWriterV1::create(
                &recording,
                &config.header,
                config.output.clone(),
            )?))
        } else {
            None
        };
        let writer_ref = writer.as_ref();
        let mut first_read = true;
        let mut merge = prepare_historical_rows_v1(
            &out.join("source-order"),
            &config.canonical,
            config.header.input_stream_id.clone(),
            || {
                let bindings = if first_read { writer_ref } else { None };
                first_read = false;
                Ok(Rows::new(&config, decode.clone(), bindings))
            },
        )?;
        serde_json::to_writer_pretty(
            File::create(out.join("source-sort-metrics.json"))?,
            merge.sort_metrics(),
        )?;
        serde_json::to_writer_pretty(
            File::create(out.join("source-decode-metrics.json"))?,
            &*decode.borrow(),
        )?;
        let mut core = PublicObservationCoreV1::new(config.header.clone())?;
        let mut writer = writer.map(std::cell::RefCell::into_inner);
        let mut count = 0;
        let mut observations = 0_u64;
        while let Some(mut input) = merge.next_input()? {
            let prepared = merge.prepare_input(&core, &mut input)?;
            let result = core
                .push(&prepared)
                .map_err(|e| format!("input_position={}: {e}", prepared.recorded.input_position))?;
            measure_result(&result, &mut results)?;
            measure_pm_boundary(&prepared.recorded, &result, &mut boundary)?;
            measure_binance_boundary(&prepared.recorded, &result, &mut binance_boundary)?;
            count += 1;
            observations += result
                .applied
                .closed_groups
                .iter()
                .map(|g| g.observations.len() as u64)
                .sum::<u64>();
            if config.checkpoints.contains(&count) {
                write_line(
                    &mut metrics,
                    &serde_json::json!({"input_count": count, "core": core.object_counts(), "groups_this_input": result.applied.closed_groups.len(), "input_encoded_bytes": serde_json::to_vec(&prepared.recorded)?.len(), "applied_encoded_bytes": serde_json::to_vec(&result.applied)?.len(), "unmeasured": ["allocator_capacity", "heap_bytes", "cgroup_memory_peak"]}),
                )?;
            }
            if let Some(writer) = &mut writer {
                let result = result.into_owned();
                drop(prepared);
                writer.record(input, result)?;
            }
        }
        let end = RecordedIngressV1 {
            input_stream_id: config.header.input_stream_id.clone(),
            input_position: count,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        };
        let prepared = prepare_public_ingress_v1(&core, &end, &BTreeMap::new(), &[])?;
        let result = core.push(&prepared)?;
        measure_result(&result, &mut results)?;
        let completion = if let Some(writer) = &mut writer {
            let result = result.into_owned();
            writer.record(end, result)?;
            serde_json::json!({"messages": count, "committed": writer.stabilize()?})
        } else {
            serde_json::json!({"messages": count, "applied": core.applied_prefix(), "observations": observations,
                "unexercised": ["recording", "source_binding_persistence", "max_capture_output_bytes", "max_artifact_payload_bytes", "max_candidate_directory_bytes"]})
        };
        serde_json::to_writer_pretty(File::create(out.join("completion.json"))?, &completion)?;
    } else if args[1] == "replay" {
        let mut replay = PublicReplayV1::open(
            &recording,
            config.canonical.decode_batch_rows,
            config.canonical.max_decode_batch_bytes,
        )?;
        let mut count = 0;
        while let Some((input, result)) = replay.next_applied()? {
            measure_result(&result, &mut results)?;
            let view = RecordedIngressViewV1 {
                input_stream_id: input.input_stream_id.clone(),
                input_position: input.input_position,
                ingress: &input.ingress,
                dependency_record_refs: input.dependency_record_refs.clone(),
            };
            measure_pm_boundary(&view, &result, &mut boundary)?;
            measure_binance_boundary(&view, &result, &mut binance_boundary)?;
            if matches!(input.ingress, DptIngressV1::Message { .. }) {
                count += 1;
            }
            if config.checkpoints.contains(&count)
                && !matches!(input.ingress, DptIngressV1::EndOfInput)
            {
                write_line(
                    &mut metrics,
                    &serde_json::json!({"input_count": count, "core": replay.core.object_counts(), "groups_this_input": result.applied.closed_groups.len(), "input_encoded_bytes": serde_json::to_vec(&input)?.len(), "applied_encoded_bytes": serde_json::to_vec(&result.applied)?.len(), "unmeasured": ["allocator_capacity", "heap_bytes", "cgroup_memory_peak"]}),
                )?;
            }
        }
        serde_json::to_writer_pretty(
            File::create(out.join("completion.json"))?,
            &serde_json::json!({"messages": count}),
        )?;
    } else if args[1] == "hidden" || args[1] == "hidden-probe" {
        use data_pipeline_training::hidden_preparation::*;
        let mut replay = PublicReplayV1::open(
            &recording,
            config.canonical.decode_batch_rows,
            config.canonical.max_decode_batch_bytes,
        )?;
        while replay.next_applied()?.is_some() {}
        let reader = PublicRecordingReaderV1::recover(
            &recording,
            config.output.sort.batch_rows,
            config.canonical.max_decode_batch_bytes,
        )?;
        let prepared = prepare_hidden_v1(
            replay.into_core(),
            reader,
            &out.join("hidden"),
            &config.output,
            format!("{}:hidden", config.header.input_stream_id),
        )?;
        serde_json::to_writer_pretty(File::create(out.join("hidden-reference.json"))?, &prepared)?;
        if args[1] == "hidden-probe" {
            let mut reader = HiddenMaterialReaderV1::open(
                &prepared,
                &recording,
                config.output.sort.batch_rows,
                config.canonical.max_decode_batch_bytes,
            )?;
            // 仅诊断原读取器最初两页，不签发完整消费凭据。
            for _ in 0..2 {
                if reader.next_page()?.is_empty() {
                    break;
                }
                write_line(&mut metrics, reader.metrics())?;
                metrics.flush()?;
            }
            serde_json::to_writer_pretty(
                File::create(out.join("probe.json"))?,
                &serde_json::json!({"read_metrics": reader.metrics(), "unmeasured": ["file_open_count", "row_group_reads"]}),
            )?;
            return Ok(());
        }
        let mut buckets = sim_system::public_consumption::RecordedHiddenBucketSourceV1::open(
            &prepared,
            &recording,
            config.output.sort.batch_rows,
            config.canonical.max_decode_batch_bytes,
        )?;
        let mut count = 0_u64;
        let mut peak_members = 0_usize;
        let mut peak_encoded_bytes = 0;
        while let Some(bucket) = buckets.next_bucket()? {
            peak_members = peak_members.max(bucket.records().len());
            count += 1;
            let material = encoded_digest(&bucket.records())?;
            peak_encoded_bytes = peak_encoded_bytes.max(material.1);
            write_line(
                &mut results,
                &serde_json::json!({"bucket": bucket.reference(), "material": material}),
            )?;
        }
        serde_json::to_writer_pretty(
            File::create(out.join("completion.json"))?,
            &serde_json::json!({"complete_buckets": count, "peak_bucket_index_members": peak_members, "peak_bucket_encoded_bytes": peak_encoded_bytes, "read_metrics": buckets.read_metrics(), "unmeasured": ["file_open_count", "row_group_reads"]}),
        )?;
    } else {
        return Err(
            "入口只接受 core/record/replay/hidden/hidden-probe/simulate/simulate-hidden/terminal-order".into(),
        );
    }
    results.flush()?;
    metrics.flush()?;
    boundary.flush()?;
    binance_boundary.flush()?;
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("finite_acceptance_failed: {error}");
        std::process::exit(1);
    }
}
