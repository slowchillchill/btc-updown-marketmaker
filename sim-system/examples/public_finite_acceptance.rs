//! 阶段5显式有限验收；配置与输入身份由运行前冻结，结果不是生产运行承诺。
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
    pending: std::vec::IntoIter<TypedRawRowV2>,
}
impl<'a> Rows<'a> {
    fn new(config: &'a Config, metrics: std::rc::Rc<std::cell::RefCell<DecodeMetrics>>) -> Self {
        Self {
            config,
            metrics,
            files: config.files.iter(),
            stream: None,
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
fn run() -> RecordingResult<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("用法: public_finite_acceptance <record|replay> <config.json> <output> <recording-root>".into());
    }
    let config: Config = serde_json::from_reader(File::open(&args[2])?)?;
    let out = PathBuf::from(&args[3]);
    fs::create_dir(&out)?;
    serde_json::to_writer_pretty(File::create(out.join("configuration.json"))?, &config)?;
    let mut results = BufWriter::new(File::create(out.join("results.jsonl"))?);
    let mut metrics = BufWriter::new(File::create(out.join("checkpoints.jsonl"))?);
    let recording = PathBuf::from(&args[4]);
    if args[1] == "record" {
        let decode = std::rc::Rc::new(std::cell::RefCell::new(DecodeMetrics::default()));
        let mut merge = prepare_historical_rows_v1(
            &out.join("source-order"),
            &config.canonical,
            config.header.input_stream_id.clone(),
            || Ok(Rows::new(&config, decode.clone())),
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
        let mut writer =
            PublicRecordingWriterV1::create(&recording, &config.header, config.output.clone())?;
        let mut count = 0;
        while let Some(input) = merge.next_input()? {
            let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[])?;
            let result = core
                .push(&prepared)
                .map_err(|e| format!("input_position={}: {e}", input.input_position))?;
            measure_result(&result, &mut results)?;
            count += 1;
            if config.checkpoints.contains(&count) {
                write_line(
                    &mut metrics,
                    &serde_json::json!({"input_count": count, "core": core.object_counts(), "groups_this_input": result.applied.closed_groups.len(), "input_encoded_bytes": serde_json::to_vec(&input)?.len(), "applied_encoded_bytes": serde_json::to_vec(&result.applied)?.len(), "unmeasured": ["allocator_capacity", "heap_bytes", "cgroup_memory_peak"]}),
                )?;
            }
            let result = result.into_owned();
            writer.record(input, result)?;
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
        let result = result.into_owned();
        writer.record(end, result)?;
        let prefix = writer.stabilize()?;
        serde_json::to_writer_pretty(
            File::create(out.join("completion.json"))?,
            &serde_json::json!({"messages": count, "committed": prefix}),
        )?;
    } else if args[1] == "replay" {
        let mut replay = PublicReplayV1::open(
            &recording,
            config.canonical.decode_batch_rows,
            config.canonical.max_decode_batch_bytes,
        )?;
        let mut count = 0;
        while let Some((input, result)) = replay.next_applied()? {
            measure_result(&result, &mut results)?;
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
    } else {
        return Err("入口只接受 record/replay".into());
    }
    results.flush()?;
    metrics.flush()?;
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("finite_acceptance_failed: {error}");
        std::process::exit(1);
    }
}
