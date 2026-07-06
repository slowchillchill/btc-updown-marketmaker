use std::fs;
use std::time::Instant;

use data_collect::backpressure::{
    BackpressureConfig, BackpressureMonitor, QueueSample, bounded_event_queue,
};
use data_collect::resource::{MetricUnavailable, ResourceSample, ResourceWatermark};
use data_collect::{ParquetSegmentWriter, RawEvent};
use tokio::sync::mpsc::error::TryRecvError;

const CHUNK_ROWS: u64 = 5_000;

fn main() {
    run_case("polymarket_p95_window", 302_000);
    run_case("polymarket_max_window", 433_000);
}

fn run_case(case_name: &str, total_rows: u64) {
    let root = std::env::temp_dir().join(format!(
        "data-collect-throughput-probe-{}-{case_name}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create throughput probe root");

    let mut monitor = BackpressureMonitor::new(
        "four_source_synthetic",
        BackpressureConfig::new(
            QueueSample {
                rows: 2_000,
                bytes: 1_000_000,
                oldest_event_age_ms: 250,
                writer_lag_ms: 50,
            },
            QueueSample {
                rows: 25_000,
                bytes: 25_000_000,
                oldest_event_age_ms: 2_000,
                writer_lag_ms: 500,
            },
            3,
        ),
    );

    let started = Instant::now();
    let mut writer = ParquetSegmentWriter::create(
        &root,
        &format!("{case_name}-typed-raw"),
        "parquet_raw_layout_v1",
    )
    .expect("create parquet writer");
    let (tx, mut rx) =
        bounded_event_queue::<Vec<RawEvent>>(1).expect("create bounded throughput queue");

    let mut written = 0_u64;
    let mut chunk_events_per_sec = Vec::new();
    let mut flush_latency_ms = Vec::new();
    while written < total_rows {
        let take = CHUNK_ROWS.min(total_rows - written);
        let events: Vec<RawEvent> = (written..written + take).map(synthetic_event).collect();
        let bytes = events.iter().map(estimated_event_bytes).sum();
        tx.try_send(events)
            .expect("bounded queue accepts drained chunk");
        let events = match rx.try_recv() {
            Ok(events) => events,
            Err(TryRecvError::Empty) => panic!("bounded queue unexpectedly empty"),
            Err(TryRecvError::Disconnected) => panic!("bounded queue disconnected"),
        };

        let write_started = Instant::now();
        writer
            .write_events(&events)
            .expect("write synthetic events");
        let writer_lag_ms = write_started.elapsed().as_millis() as u64;
        let write_elapsed_secs = write_started.elapsed().as_secs_f64().max(0.001);
        chunk_events_per_sec.push(take as f64 / write_elapsed_secs);
        flush_latency_ms.push(writer_lag_ms);

        monitor.observe(QueueSample {
            rows: take,
            bytes,
            oldest_event_age_ms: writer_lag_ms,
            writer_lag_ms,
        });

        written += take;
    }

    let finalize_started = Instant::now();
    let published = writer.finalize();
    let finalize_latency_ms = finalize_started.elapsed().as_millis() as u64;
    let finalize_failures = u8::from(published.is_err());
    let published = published.expect("finalize throughput probe");
    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let events_per_sec = total_rows as f64 / elapsed_secs;
    let high = monitor.high_watermark();
    let event_rate_stats = F64Stats::from_values(&chunk_events_per_sec);
    let flush_stats = U64Stats::from_values(&flush_latency_ms);
    let resource_watermark = synthetic_resource_watermark(high, flush_stats.max);
    let unavailable_metrics = resource_watermark
        .unavailable
        .iter()
        .map(|metric| metric.metric.as_str())
        .collect::<Vec<_>>()
        .join(",");

    println!(
        "case={} rows={} elapsed_ms={} events_per_sec_total={:.2} events_per_sec_p50={:.2} events_per_sec_p95={:.2} events_per_sec_max={:.2} flush_latency_ms_p50={} flush_latency_ms_p95={} flush_latency_ms_max={} finalize_latency_ms={} finalize_failures={} writer_queue_rows_high={} writer_queue_bytes_high={} queue_age_ms_high={} writer_lag_ms_high={} rss_bytes={} resource_unavailable={} final_path={} sha256={}",
        case_name,
        published.row_count,
        elapsed.as_millis(),
        events_per_sec,
        event_rate_stats.p50,
        event_rate_stats.p95,
        event_rate_stats.max,
        flush_stats.p50,
        flush_stats.p95,
        flush_stats.max,
        finalize_latency_ms,
        finalize_failures,
        high.rows,
        high.bytes,
        high.oldest_event_age_ms,
        high.writer_lag_ms,
        resource_watermark.rss_bytes.unwrap_or(0),
        unavailable_metrics,
        published.final_path.display(),
        published.sha256
    );

    fs::remove_dir_all(&root).expect("remove throughput probe root");
}

struct F64Stats {
    p50: f64,
    p95: f64,
    max: f64,
}

impl F64Stats {
    fn from_values(values: &[f64]) -> Self {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        Self {
            p50: percentile_f64(&sorted, 50),
            p95: percentile_f64(&sorted, 95),
            max: sorted.last().copied().unwrap_or_default(),
        }
    }
}

struct U64Stats {
    p50: u64,
    p95: u64,
    max: u64,
}

impl U64Stats {
    fn from_values(values: &[u64]) -> Self {
        let mut sorted = values.to_vec();
        sorted.sort();
        Self {
            p50: percentile_u64(&sorted, 50),
            p95: percentile_u64(&sorted, 95),
            max: sorted.last().copied().unwrap_or_default(),
        }
    }
}

fn percentile_f64(sorted: &[f64], percentile: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = percentile_index(sorted.len(), percentile);
    sorted[index]
}

fn percentile_u64(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = percentile_index(sorted.len(), percentile);
    sorted[index]
}

fn percentile_index(len: usize, percentile: usize) -> usize {
    let percentile = percentile.min(100);
    let rank = (len * percentile).div_ceil(100);
    rank.saturating_sub(1).min(len - 1)
}

fn synthetic_resource_watermark(high: QueueSample, flush_latency_ms: u64) -> ResourceWatermark {
    ResourceSample {
        rss_bytes: read_rss_bytes(),
        cpu_percent: None,
        disk_free_bytes: None,
        artifact_dir_bytes: None,
        journal_log_bytes: None,
        journal_limit_status: None,
        open_files: None,
        queue_bytes: Some(high.bytes),
        queue_age_ms: Some(high.oldest_event_age_ms),
        parquet_writer_memory_estimate_bytes: None,
        cgroup_memory_current_bytes: None,
        cgroup_memory_stat: None,
        cgroup_memory_events: None,
        psi_cpu: None,
        psi_memory: None,
        psi_io: None,
        stage_latency_ms: Some(flush_latency_ms),
        ttl_ms: None,
        unavailable: vec![
            MetricUnavailable {
                metric: "cpu_percent".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "disk_free_bytes".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "artifact_dir_bytes".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "journal_log_bytes".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "journal_limit_status".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "open_files".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "parquet_writer_memory_estimate_bytes".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "cgroup_memory_current_bytes".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "cgroup_memory_stat".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "cgroup_memory_events".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "psi_cpu".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "psi_memory".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "psi_io".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
            MetricUnavailable {
                metric: "ttl_ms".to_owned(),
                reason: "not measured by synthetic throughput probe".to_owned(),
            },
        ],
    }
    .into_watermark()
}

fn read_rss_bytes() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(rss_pages * 4096)
}

fn synthetic_event(seq: u64) -> RawEvent {
    let source_kind = match seq % 4 {
        0 => "polymarket_market",
        1 => "binance_depth_update",
        2 => "binance_aggtrade",
        _ => "polymarket_rtds_chainlink_btc_usd",
    };

    RawEvent {
        source_kind: source_kind.to_owned(),
        source_event_type: match source_kind {
            "polymarket_market" => "price_change",
            "binance_depth_update" => "depthUpdate",
            "binance_aggtrade" => "aggTrade",
            _ => "update",
        }
        .to_owned(),
        source_identifier: format!("{source_kind}:{seq}"),
        source_ts: Some((1_766_789_400_000 + seq as i64).to_string()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        source_endpoint: format!("synthetic://{source_kind}"),
        raw_json: format!(r#"{{"source":"{source_kind}","seq":{seq}}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind.to_owned(),
    }
}

fn estimated_event_bytes(event: &RawEvent) -> u64 {
    [
        event.source_kind.as_str(),
        event.source_event_type.as_str(),
        event.source_identifier.as_str(),
        event.source_ts.as_deref().unwrap_or_default(),
        event.collector_host_id.as_str(),
        event.collector_region.as_str(),
        event.source_endpoint.as_str(),
        event.raw_json.as_str(),
        event.parquet_raw_source_kind.as_str(),
    ]
    .iter()
    .map(|value| value.len() as u64)
    .sum::<u64>()
        + std::mem::size_of::<RawEvent>() as u64
}
