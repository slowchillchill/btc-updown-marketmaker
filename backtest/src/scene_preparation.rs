//! 市场线程只序列化一次最终事件；单路 rank 堆转送原字节，zstd 可并行压缩。
use super::*;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

/// 只统计下游实际接受的压缩字节。必须在 encoder.finish 和缓冲 flush 后取摘要。
pub(super) struct HashingWriter<W> {
    inner: W,
    digest: Sha256,
    pub(super) peak_process_threads: Option<usize>,
    last_thread_sample: Option<Instant>,
}
impl<W> HashingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            peak_process_threads: None,
            last_thread_sample: None,
        }
    }
    pub fn finish(self) -> String {
        format!("{:x}", self.digest.finalize())
    }
}
impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let accepted = self.inner.write(bytes)?;
        self.digest.update(&bytes[..accepted]);
        if self
            .last_thread_sample
            .is_none_or(|at| at.elapsed().as_secs() >= 1)
        {
            if let Some(threads) = process_threads() {
                self.peak_process_threads =
                    Some(self.peak_process_threads.unwrap_or(0).max(threads));
            }
            self.last_thread_sample = Some(Instant::now());
        }
        Ok(accepted)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn process_threads() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(|entries| entries.filter_map(Result::ok).count())
}

pub(super) fn publish_manifest(
    path: &Path,
    manifest: &StreamManifestV2,
) -> Result<(), MaterialError> {
    // 文件级收尾失败不发布半份新清单。调用方继续为每次准备提供独立输出目录。
    let pending = path.with_extension("scene-manifest.pending");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)?;
    let mut writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, manifest)?;
    writer.flush()?;
    drop(writer);
    std::fs::rename(pending, path)?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct ScenePreparationCostV3 {
    #[serde(flatten)]
    pub cost: ScenePreparationCostV2,
    /// 合流阶段总工人预算：1同步；N>1为一个保序线程和N−1后台压缩线程。
    pub merge_workers: usize,
    pub ordering_workers: usize,
    /// 请求的后台工人数；小卷可由zstd同步完成，不能据此推断实际启动数。
    pub configured_compression_background_threads: usize,
    pub merge_process_threads_before: Option<usize>,
    pub peak_process_threads_during_payload_writes: Option<usize>,
    pub event_serializations: u64,
    pub merge_event_deserializations: u64,
    pub temporary_read_passes: usize,
}
struct ConditionOutput {
    path: PathBuf,
    count: u64,
    report: PreparationReport,
    max_event_bytes: u64,
}
#[derive(Clone, Copy, Debug)]
struct Head {
    rank: u64,
    len: u64,
    at_ms: i64,
}
fn write_ranked(
    mut sink: impl Write,
    rank: usize,
    event: &Event,
    bytes: &mut Vec<u8>,
) -> Result<u64, MaterialError> {
    bytes.clear();
    serde_json::to_writer(&mut *bytes, event)?;
    bytes.push(b'\n');
    let len = u64::try_from(bytes.len())?;
    sink.write_all(&u64::try_from(rank)?.to_le_bytes())?;
    sink.write_all(&len.to_le_bytes())?;
    sink.write_all(&event.at_ms.to_le_bytes())?;
    sink.write_all(bytes)?;
    Ok(len)
}
struct RankedReader<R> {
    source: R,
    max_event_bytes: u64,
    last_rank: Option<u64>,
}
impl<R: Read> RankedReader<R> {
    fn next_head(&mut self) -> Result<Option<Head>, MaterialError> {
        let mut rank = [0; 8];
        if self.source.read(&mut rank[..1])? == 0 {
            return Ok(None);
        }
        self.source.read_exact(&mut rank[1..])?;
        let mut len = [0; 8];
        let mut at = [0; 8];
        self.source.read_exact(&mut len)?;
        self.source.read_exact(&mut at)?;
        let head = Head {
            rank: u64::from_le_bytes(rank),
            len: u64::from_le_bytes(len),
            at_ms: i64::from_le_bytes(at),
        };
        if head.len == 0
            || head.len > self.max_event_bytes
            || self.last_rank.is_some_and(|rank| head.rank < rank)
        {
            return Err("临时场景头长度或rank顺序不符".into());
        }
        self.last_rank = Some(head.rank);
        Ok(Some(head))
    }
}

fn merge(
    outputs: &[ConditionOutput],
    path: &Path,
    workers: usize,
    expected_events: u64,
    progress_seconds: u64,
    material: OfflineSceneMaterialV1,
) -> Result<(u64, Option<usize>), MaterialError> {
    type Reader = RankedReader<
        std::io::BufReader<zstd::stream::read::Decoder<'static, std::io::BufReader<std::fs::File>>>,
    >;
    let started = Instant::now();
    let mut last = started;
    let mut readers = Vec::<Reader>::new();
    let mut heads = Vec::new();
    let mut heap = std::collections::BinaryHeap::new();
    for output in outputs {
        let decoder = zstd::stream::read::Decoder::new(std::fs::File::open(&output.path)?)?;
        let mut reader = RankedReader {
            source: std::io::BufReader::new(decoder),
            max_event_bytes: output.max_event_bytes,
            last_rank: None,
        };
        let head = reader.next_head()?;
        if let Some(head) = head {
            heap.push(std::cmp::Reverse((head.rank, readers.len())));
        }
        readers.push(reader);
        heads.push(head);
    }
    let mut writer = SceneStreamWriterV2::create_with_merge_workers(path, workers)?;
    let mut events = 0;
    while let Some(std::cmp::Reverse((_rank, index))) = heap.pop() {
        let head = heads[index].take().expect("堆仅引用存在的头");
        if writer.last_at.is_some_and(|at| head.at_ms < at) {
            return Err("临时场景时间不递增".into());
        }
        // 每个流仅顺序读一次；只保留固定大小拷贝缓冲，不解析或暂存完整事件。
        let copied = std::io::copy(
            &mut readers[index].source.by_ref().take(head.len),
            &mut writer.encoder,
        )?;
        if copied != head.len {
            return Err("临时场景事件字节提前结束".into());
        }
        writer.last_at = Some(head.at_ms);
        writer.events += 1;
        events += 1;
        heads[index] = readers[index].next_head()?;
        if let Some(head) = heads[index] {
            heap.push(std::cmp::Reverse((head.rank, index)));
        }
        if progress_seconds > 0 && last.elapsed().as_secs() >= progress_seconds {
            eprintln!(
                "场景保序字节合流: completed={events}, total={expected_events}, merge_worker_budget={workers}, elapsed_seconds={:.3}",
                started.elapsed().as_secs_f64()
            );
            last = Instant::now();
        }
    }
    if events != expected_events {
        return Err("临时场景合流事件数不符".into());
    }
    writer.finish_with_thread_sample(material)
}

/// 市场构造并行度独立于合流阶段总工人预算。一次顺序合流，没有按工人重扫事实或全量队列。
/// merge_workers=1 在合流线程同步压缩；N>1向zstd请求N−1后台线程。
/// 小卷可能同步完成；实际进程线程数由合流前与载荷写出时的样本分别记录。
/// 旧 v2 入口沿用此实现且固定 merge_workers=1。
pub fn prepare_condition_parallel_scene_with_options_v3(
    analysis: &Analysis,
    parameters: Parameters,
    markets: &BTreeMap<String, MarketIdentityV2>,
    market_assets: &BTreeMap<String, [AssetIdentityV1; 2]>,
    source_manifests: &[(String, String)],
    path: &Path,
    execution_start: i64,
    condition_workers: usize,
    merge_workers: usize,
    progress_seconds: u64,
    condition_temporary_root: Option<&Path>,
) -> Result<ScenePreparationCostV3, MaterialError> {
    if condition_workers == 0 || merge_workers == 0 || merge_workers > u32::MAX as usize {
        return Err("场景工人数需要大于零且压缩工人数可表示为u32".into());
    }
    if let Some(directory) = condition_temporary_root {
        std::fs::create_dir(directory)?;
    }
    let started = Instant::now();
    let mut last_plan = started;
    let mut phase = String::new();
    eprintln!("场景数量预计划: start, total_conditions={}", markets.len());
    let plan = analysis.condition_plan_with_progress(parameters.clone(), |current, completed, total| {
        if current != phase || completed == total || (progress_seconds > 0 && last_plan.elapsed().as_secs() >= progress_seconds) {
            eprintln!("场景数量预计划: phase={current}, completed={completed}, total={total}, elapsed_seconds={:.3}", started.elapsed().as_secs_f64()); phase = current.into(); last_plan = Instant::now();
        }
    });
    let quantity_plan_seconds = started.elapsed().as_secs_f64();
    let pending = Mutex::new(0..plan.condition_count());
    let completed = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let outputs = std::thread::scope(|scope| -> Result<Vec<ConditionOutput>, MaterialError> {
        let mut handles = Vec::new();
        for _ in 0..condition_workers.min(plan.condition_count()) {
            let plan = &plan;
            let pending = &pending;
            let completed = &completed;
            let failed = &failed;
            handles.push(scope.spawn(move || -> Result<Vec<ConditionOutput>, MaterialError> {
                let result = (|| {
                    let mut outputs = Vec::new(); let mut bytes = Vec::new();
                    while !failed.load(Ordering::Relaxed) {
                        let Some(index) = pending.lock().map_err(|_| "市场工作分配锁损坏")?.next() else { break; };
                        let temp = match condition_temporary_root { Some(dir) => dir.join(format!("condition-{index}.ranked.zst")), None => path.with_extension(format!("condition-{index}.ranked.zst")) };
                        let mut encoder = zstd::stream::write::Encoder::new(std::io::BufWriter::new(std::fs::File::create(&temp)?), 3)?; encoder.include_checksum(true)?;
                        let mut count = 0; let mut max_event_bytes = 0; let mut last = Instant::now();
                        let scene = plan.prepare_condition(index, |phase, done, total| {
                            if done == total || (progress_seconds > 0 && last.elapsed().as_secs() >= progress_seconds) {
                                eprintln!("场景市场构造: condition={}, phase={phase}, completed={done}, total={total}, elapsed_seconds={:.3}", plan.condition_name(index), started.elapsed().as_secs_f64()); last = Instant::now();
                            }
                        }, |rank, mut event| -> Result<(), MaterialError> {
                            event.at_ms = event.at_ms.max(execution_start);
                            max_event_bytes = max_event_bytes.max(write_ranked(&mut encoder, rank, &event, &mut bytes)?); count += 1; Ok(())
                        })?;
                        encoder.finish()?.flush()?;
                        outputs.push(ConditionOutput { path: temp, count, report: scene.report, max_event_bytes });
                        let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                        eprintln!("场景市场完成: completed={done}, total={}, elapsed_seconds={:.3}", plan.condition_count(), started.elapsed().as_secs_f64());
                    }
                    Ok(outputs)
                })();
                if result.is_err() { failed.store(true, Ordering::Relaxed); } result
            }));
        }
        // 任何失败都先回收所有实际工作线程；不遗留分离后台写入者。
        let mut outputs = Vec::new();
        let mut error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(items)) => outputs.extend(items),
                Ok(Err(cause)) => {
                    failed.store(true, Ordering::Relaxed);
                    if error.is_none() {
                        error = Some(cause);
                    }
                }
                Err(_) => {
                    failed.store(true, Ordering::Relaxed);
                    if error.is_none() {
                        error = Some("市场构造线程panic".into());
                    }
                }
            }
        }
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(outputs)
        }
    })?;
    let construction_seconds = started.elapsed().as_secs_f64() - quantity_plan_seconds;
    let report = plan.combine_reports(outputs.iter().map(|output| output.report.clone()));
    let expected_events = outputs.iter().map(|output| output.count).sum();
    let temporary_logical_bytes = outputs
        .iter()
        .map(|output| std::fs::metadata(&output.path).map(|m| m.len()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum();
    let merge_started = Instant::now();
    eprintln!(
        "场景保序字节合流: events={expected_events}, temporary_bytes={temporary_logical_bytes}, merge_worker_budget={merge_workers}, ordering_workers=1"
    );
    let merge_process_threads_before = process_threads();
    let (output_bytes, peak_process_threads_during_payload_writes) = merge(
        &outputs,
        path,
        merge_workers,
        expected_events,
        progress_seconds,
        OfflineSceneMaterialV1 {
            scene: PreparedScene {
                model_version: if parameters.cancellation == Cancellation::Proportional {
                    FRAGMENT_MODEL_VERSION
                } else {
                    MODEL_VERSION
                }
                .into(),
                parameters,
                events: Vec::new(),
                report: report.clone(),
            },
            markets: markets.clone(),
            market_assets: market_assets.clone(),
            source_manifests: source_manifests.to_vec(),
        },
    )?;
    for output in outputs {
        std::fs::remove_file(output.path)?;
    }
    if let Some(directory) = condition_temporary_root {
        std::fs::remove_dir(directory)?;
    }
    Ok(ScenePreparationCostV3 {
        cost: ScenePreparationCostV2 {
            events: expected_events,
            output_bytes,
            quantity_plan_seconds,
            construction_seconds,
            merge_seconds: merge_started.elapsed().as_secs_f64(),
            temporary_logical_bytes,
            condition_workers,
            report,
        },
        merge_workers,
        ordering_workers: 1,
        configured_compression_background_threads: merge_workers - 1,
        merge_process_threads_before,
        peak_process_threads_during_payload_writes,
        event_serializations: expected_events,
        merge_event_deserializations: 0,
        temporary_read_passes: 1,
    })
}

#[cfg(test)]
#[path = "scene_preparation_tests.rs"]
mod tests;
