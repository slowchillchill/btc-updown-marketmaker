use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use data_collect::backpressure::bounded_event_queue;
use data_collect::latency::{
    DurableWriteBatch, FlushPolicy, FlushReason, HotPathReceipt, LatencyFanout, LatencyStats,
    QueueDurationSummary, QueueDurationTracker, SequencedRawEvent, TimedRawEvent,
    WriteBatchReceipt,
};
use data_collect::market_discovery::{
    BtcUpdownDiscoveryPair, BtcUpdownMarketCandidate, build_market_subscription_request_json,
    discovery_windows, select_btc_updown_pair, sha256_hex,
};
use data_collect::{ParquetSegmentWriter, RawEvent, writer::InstrumentedPublishedSegment};
use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::{ChannelType, Client as MarketWsClient};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::rtds::{
    Client as RtdsClient, Subscription, SubscriptionRequest as RtdsSubscriptionRequest,
};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use data_collect::collector_identity::resolve_collector_region;

const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";
const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
const BINANCE_DEPTH_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms";
const BINANCE_AGGTRADE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const BINANCE_DEPTH_SNAPSHOT: &str =
    "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000";

const DEFAULT_DURATION_SECS: u64 = 45 * 60;
const DEFAULT_DISCOVERY_WAIT_SECS: u64 = 90;
const DEFAULT_QUEUE_CAPACITY: usize = 25_000;
const DEFAULT_FLUSH_ROWS: usize = 5_000;
const DEFAULT_HOT_QUEUE_CAPACITY: usize = 25_000;
const DEFAULT_MAX_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_MIN_WINDOWS: usize = 6;
const DEFAULT_MIN_ROLLOVER_EDGES: usize = 1;
const BINANCE_HTTP_CONNECT_TIMEOUT_SECS: u64 = 5;
const BINANCE_HTTP_TOTAL_TIMEOUT_SECS: u64 = 15;
const WS_CONNECT_TIMEOUT_SECS: u64 = 20;
const RTDS_FRESHNESS_TIMEOUT_SECS: u64 = 60;
const SOURCE_SEND_TIMEOUT_MS: u64 = 2_000;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("real_source_alignment_probe_error={err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = ProbeConfig::from_env_and_args()?;
    let run_started = Instant::now();
    fs::create_dir_all(&config.artifact_dir)?;
    validate_artifact_root(&config)?;

    let run_id = env_or_default(
        "REAL_SOURCE_RUN_ID",
        format!("phase0-real-source-{}-{}", now_ms(), std::process::id()),
    );
    let collector_host_id = collector_host_id();
    let collector_region = resolve_collector_region().await;
    let source_gaps = Arc::new(Mutex::new(Vec::<String>::new()));
    let rollover_edges = Arc::new(Mutex::new(Vec::<RolloverEdge>::new()));

    let gamma = GammaClient::default();
    let initial_discovery = discover_pair_with_wait(&gamma, config.discovery_wait_secs).await?;
    let collector_identity = CollectorIdentity {
        host_id: collector_host_id.clone(),
        region: collector_region.clone(),
    };
    let asset_ids = pair_asset_ids(&initial_discovery);
    let market_subscription_json = build_market_subscription_request_json(&asset_ids)?;
    let market_subscription_json_sha256 = sha256_hex(market_subscription_json.as_bytes());
    let rtds_subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let rtds_subscription_json =
        serde_json::to_string(&RtdsSubscriptionRequest::subscribe(vec![rtds_subscription]))?;
    let rtds_subscription_json_sha256 = sha256_hex(rtds_subscription_json.as_bytes());

    let discovery_receipt_path = config.artifact_dir.join("initial_discovery_receipt.json");
    fs::write(
        &discovery_receipt_path,
        serde_json::to_vec_pretty(&json!({
            "run_id": run_id,
            "recv_time_utc_ms": now_ms(),
            "collector_host_id": collector_host_id,
            "collector_region": collector_region,
            "initial_pair": initial_discovery,
            "polymarket_subscription_json": market_subscription_json,
            "polymarket_subscription_json_sha256": market_subscription_json_sha256,
            "polymarket_subscription_evidence_scope": {
                "sdk_api_used_for_custom_feature_business_messages": "Client::subscribe_best_bid_ask",
                "sdk_source_fact": "polymarket_client_sdk_v2 0.7.0 Client::subscribe_best_bid_ask calls subscribe_market_with_options(asset_ids, true)",
                "orderbook_api_used": "Client::subscribe_orderbook",
                "actual_outbound_frame_captured_by_probe": false,
                "actual_outbound_frame_unavailable_reason": "official SDK does not expose sent-frame receipt in this probe; Phase 1 adapter must capture request evidence or record unavailable reason"
            },
            "rtds_subscription_json": rtds_subscription_json,
            "rtds_subscription_json_sha256": rtds_subscription_json_sha256,
        }))?,
    )?;

    let (event_tx, mut event_rx) = bounded_event_queue::<TimedRawEvent>(config.queue_capacity)?;
    let (hot_tx, mut hot_rx) = mpsc::channel::<HotHandoffEvent>(config.hot_queue_capacity);
    let hot_receipts = Arc::new(Mutex::new(Vec::<HotPathReceipt>::new()));
    let hot_receipts_for_task = Arc::clone(&hot_receipts);
    let hot_consumer_handle = tokio::spawn(async move {
        while let Some(event) = hot_rx.recv().await {
            let receipt = event
                .sequenced
                .hot_path_receipt(event.hot_send_time_utc_ms, now_ms());
            hot_receipts_for_task
                .lock()
                .expect("hot receipt mutex poisoned")
                .push(receipt);
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    let (chunk_tx, chunk_rx) = std::sync::mpsc::sync_channel::<WriteChunk>(8);
    let writer_queue_rows = Arc::new(AtomicU64::new(0));
    let writer_queue_bytes = Arc::new(AtomicU64::new(0));
    let writer_queue_rows_for_worker = Arc::clone(&writer_queue_rows);
    let writer_queue_bytes_for_worker = Arc::clone(&writer_queue_bytes);
    let writer_dir = config.artifact_dir.join("typed_raw");
    let writer_handle = tokio::task::spawn_blocking(move || {
        let mut writer = ParquetSegmentWriter::create(
            &writer_dir,
            "real-source-alignment-typed-raw",
            "parquet_raw_layout_v1",
        )?;
        let mut receipts = Vec::new();
        while let Ok(chunk) = chunk_rx.recv() {
            writer_queue_rows_for_worker.fetch_sub(chunk.rows, Ordering::Relaxed);
            writer_queue_bytes_for_worker.fetch_sub(chunk.bytes_estimate, Ordering::Relaxed);
            let writer_dequeue_time_utc_ms = now_ms();
            let raw_events = chunk
                .events
                .iter()
                .map(|event| event.event.clone())
                .collect::<Vec<_>>();
            let write_started = Instant::now();
            let write_start_time_utc_ms = now_ms();
            writer.write_events(&raw_events)?;
            let write_done_time_utc_ms = now_ms();
            let write_events_us = write_started.elapsed().as_micros() as u64;
            let receipt = DurableWriteBatch::new(
                chunk.batch_seq,
                chunk.events,
                chunk.flush_reason,
                chunk.batch_send_time_utc_ms,
                writer_dequeue_time_utc_ms,
                write_start_time_utc_ms,
                write_done_time_utc_ms,
                write_events_us,
            )
            .receipt();
            receipts.push(receipt);
        }
        let published = writer.finalize_with_timing()?;
        Ok::<_, Box<dyn Error + Send + Sync>>(WriterOutput {
            published,
            write_receipts: receipts,
        })
    });

    let deadline = run_started + Duration::from_secs(config.duration_secs);
    let deadline_ms = now_ms() + (config.duration_secs * 1000) as i64;
    let handles = vec![
        tokio::spawn(run_polymarket_source(
            event_tx.clone(),
            Arc::clone(&source_gaps),
            Arc::clone(&rollover_edges),
            collector_identity.clone(),
            deadline_ms,
            config.discovery_wait_secs,
            run_started,
        )),
        tokio::spawn(run_rtds_source(
            event_tx.clone(),
            Arc::clone(&source_gaps),
            collector_identity.clone(),
            deadline,
            run_started,
        )),
        tokio::spawn(run_binance_depth_source(
            event_tx.clone(),
            Arc::clone(&source_gaps),
            collector_identity.clone(),
            deadline,
            run_started,
        )),
        tokio::spawn(run_binance_ws_source(
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: BINANCE_AGGTRADE_WS.to_owned(),
                freshness_timeout_secs: 60,
            },
            event_tx.clone(),
            Arc::clone(&source_gaps),
            collector_identity.clone(),
            deadline,
            run_started,
        )),
    ];
    drop(event_tx);

    let mut coverage = BTreeMap::<String, CoverageStats>::new();
    let mut fanout = LatencyFanout::new(1);
    let flush_policy = FlushPolicy {
        flush_rows: config.flush_rows,
        max_flush_interval_ms: Some(config.max_flush_interval_ms),
    };
    let mut batch = Vec::with_capacity(config.flush_rows);
    let mut batch_seq = 0_u64;
    let mut batch_oldest_monotonic_ms = None::<u64>;
    let mut source_queue_tracker = QueueDurationTracker::new("source_event_queue");
    let mut hot_queue_tracker = QueueDurationTracker::new("hot_handoff_queue");
    let mut writer_queue_tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut queue_samples = Vec::<QueueSampleReceipt>::new();
    let mut next_queue_sample = run_started;
    let mut average_event_bytes = 512_u64;
    let mut received_rows = 0_u64;

    while Instant::now() < deadline {
        match timeout(Duration::from_millis(250), event_rx.recv()).await {
            Ok(Some(timed_event)) => {
                received_rows = received_rows.saturating_add(1);
                let collector_dequeue_time_utc_ms = now_ms();
                let collector_dequeue_monotonic_ms = monotonic_ms(run_started);
                let sequenced = fanout.assign_sequence(
                    timed_event,
                    collector_dequeue_time_utc_ms,
                    collector_dequeue_monotonic_ms,
                );
                update_coverage(&mut coverage, &sequenced.event);
                average_event_bytes = rolling_average_bytes(
                    average_event_bytes,
                    estimated_sequenced_event_bytes(&sequenced),
                    received_rows,
                );
                if config.hot_path_probe_enabled {
                    send_hot_handoff(
                        &hot_tx,
                        &source_gaps,
                        &mut hot_queue_tracker,
                        sequenced.clone(),
                    )
                    .await;
                }
                if batch.is_empty() {
                    batch_oldest_monotonic_ms = Some(sequenced.source_recv_monotonic_ms);
                }
                batch.push(sequenced);
                if let Some(reason) = flush_policy.reason(
                    batch.len(),
                    batch_oldest_monotonic_ms,
                    monotonic_ms(run_started),
                ) {
                    send_chunk(
                        WriterChunkSendContext {
                            chunk_tx: &chunk_tx,
                            writer_queue_rows: &writer_queue_rows,
                            writer_queue_bytes: &writer_queue_bytes,
                            writer_queue_tracker: &mut writer_queue_tracker,
                            run_started,
                        },
                        &mut batch,
                        &mut batch_seq,
                        reason,
                    )
                    .await?;
                    batch_oldest_monotonic_ms = None;
                }
            }
            Ok(None) => break,
            Err(_) => {
                if let Some(reason) = flush_policy.reason(
                    batch.len(),
                    batch_oldest_monotonic_ms,
                    monotonic_ms(run_started),
                ) {
                    send_chunk(
                        WriterChunkSendContext {
                            chunk_tx: &chunk_tx,
                            writer_queue_rows: &writer_queue_rows,
                            writer_queue_bytes: &writer_queue_bytes,
                            writer_queue_tracker: &mut writer_queue_tracker,
                            run_started,
                        },
                        &mut batch,
                        &mut batch_seq,
                        reason,
                    )
                    .await?;
                    batch_oldest_monotonic_ms = None;
                }
            }
        }
        if Instant::now() >= next_queue_sample {
            record_queue_sample(
                &mut queue_samples,
                QueueSampleContext {
                    run_started,
                    event_rx_len: event_rx.len() as u64,
                    hot_tx: &hot_tx,
                    hot_queue_capacity: config.hot_queue_capacity,
                    writer_queue_rows: &writer_queue_rows,
                    writer_queue_bytes: &writer_queue_bytes,
                    average_event_bytes,
                    source_queue_tracker: &mut source_queue_tracker,
                    hot_queue_tracker: &mut hot_queue_tracker,
                    writer_queue_tracker: &mut writer_queue_tracker,
                },
            );
            next_queue_sample += Duration::from_secs(1);
        }
    }

    for handle in handles {
        match timeout(Duration::from_secs(15), handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => record_gap(&source_gaps, format!("source_task_error:{err}")),
            Ok(Err(err)) => record_gap(&source_gaps, format!("source_task_join_error:{err}")),
            Err(_) => record_gap(&source_gaps, "source_task_join_timeout".to_owned()),
        }
    }

    while let Ok(timed_event) = event_rx.try_recv() {
        received_rows = received_rows.saturating_add(1);
        let collector_dequeue_time_utc_ms = now_ms();
        let collector_dequeue_monotonic_ms = monotonic_ms(run_started);
        let sequenced = fanout.assign_sequence(
            timed_event,
            collector_dequeue_time_utc_ms,
            collector_dequeue_monotonic_ms,
        );
        update_coverage(&mut coverage, &sequenced.event);
        average_event_bytes = rolling_average_bytes(
            average_event_bytes,
            estimated_sequenced_event_bytes(&sequenced),
            received_rows,
        );
        if config.hot_path_probe_enabled {
            send_hot_handoff(
                &hot_tx,
                &source_gaps,
                &mut hot_queue_tracker,
                sequenced.clone(),
            )
            .await;
        }
        if batch.is_empty() {
            batch_oldest_monotonic_ms = Some(sequenced.source_recv_monotonic_ms);
        }
        batch.push(sequenced);
        if let Some(reason) = flush_policy.reason(
            batch.len(),
            batch_oldest_monotonic_ms,
            monotonic_ms(run_started),
        ) {
            send_chunk(
                WriterChunkSendContext {
                    chunk_tx: &chunk_tx,
                    writer_queue_rows: &writer_queue_rows,
                    writer_queue_bytes: &writer_queue_bytes,
                    writer_queue_tracker: &mut writer_queue_tracker,
                    run_started,
                },
                &mut batch,
                &mut batch_seq,
                reason,
            )
            .await?;
            batch_oldest_monotonic_ms = None;
        }
    }
    send_chunk(
        WriterChunkSendContext {
            chunk_tx: &chunk_tx,
            writer_queue_rows: &writer_queue_rows,
            writer_queue_bytes: &writer_queue_bytes,
            writer_queue_tracker: &mut writer_queue_tracker,
            run_started,
        },
        &mut batch,
        &mut batch_seq,
        FlushReason::Shutdown,
    )
    .await?;
    drop(chunk_tx);
    drop(hot_tx);

    let writer_output = writer_handle
        .await
        .map_err(|err| format!("writer task join failed: {err}"))??;
    hot_consumer_handle
        .await
        .map_err(|err| format!("hot consumer task join failed: {err}"))??;
    let published = writer_output.published.segment.clone();
    let finalize_timing = writer_output.published.timing.clone();
    let write_receipts = writer_output.write_receipts;
    let hot_receipts = hot_receipts
        .lock()
        .expect("hot receipt mutex poisoned")
        .clone();
    let final_queue_monotonic_ms = monotonic_ms(run_started);
    let source_queue_summary = source_queue_tracker.finish(final_queue_monotonic_ms);
    let hot_queue_summary = hot_queue_tracker.finish(final_queue_monotonic_ms);
    let writer_queue_summary = writer_queue_tracker.finish(final_queue_monotonic_ms);

    let mut gaps = source_gaps
        .lock()
        .expect("source gap mutex poisoned")
        .clone();
    let rollover_evidence = build_rollover_receipts(
        &rollover_edges
            .lock()
            .expect("rollover edge mutex poisoned")
            .clone(),
        &coverage,
    );
    for source in expected_sources() {
        if coverage.get(*source).is_none_or(|stats| stats.rows == 0) {
            gaps.push(format!("missing_source_coverage:{source}"));
        }
    }

    let polymarket_windows = coverage
        .get("polymarket_market")
        .map(|stats| stats.market_windows.len())
        .unwrap_or_default();
    if polymarket_windows < config.min_market_windows {
        gaps.push(format!(
            "insufficient_polymarket_market_windows: observed={polymarket_windows} required={}",
            config.min_market_windows
        ));
    }
    let valid_rollover_edges = rollover_evidence
        .iter()
        .filter(|edge| {
            edge.prefetched_promoted_to_current
                && edge.previous_current_retired_by_time
                && edge.windows_contiguous
                && edge.new_current_first_recv_time_utc_ms.is_some()
        })
        .count();
    if valid_rollover_edges < config.min_rollover_edges {
        gaps.push(format!(
            "insufficient_rollover_edges: observed={valid_rollover_edges} required={}",
            config.min_rollover_edges
        ));
    }

    let coverage_receipts = coverage
        .iter()
        .map(|(source_kind, stats)| stats.to_receipt(source_kind))
        .collect::<Vec<_>>();
    let latency_summary = build_latency_summary(
        &hot_receipts,
        &write_receipts,
        &finalize_timing,
        source_queue_summary.clone(),
        hot_queue_summary.clone(),
        writer_queue_summary.clone(),
    );
    let resource_watermark = json!({
        "rss_bytes": read_rss_bytes(),
        "artifact_dir_bytes": dir_size_bytes(&config.artifact_dir).ok(),
        "queue_rows_high": source_queue_summary.rows_high.max(hot_queue_summary.rows_high).max(writer_queue_summary.rows_high),
        "queue_bytes_high": source_queue_summary.bytes_high.max(hot_queue_summary.bytes_high).max(writer_queue_summary.bytes_high),
        "source_event_queue": source_queue_summary,
        "hot_handoff_queue": hot_queue_summary,
        "writer_chunk_queue": writer_queue_summary,
        "received_rows": received_rows,
        "unavailable": [
            {"metric":"cpu_percent","reason":"not measured by phase0 real source probe"},
            {"metric":"disk_free_bytes","reason":"not measured by phase0 real source probe"},
            {"metric":"journal_log_bytes","reason":"not measured by phase0 real source probe"},
            {"metric":"psi","reason":"not measured by phase0 real source probe"}
        ]
    });
    let verdict = if gaps.is_empty() {
        "PASS"
    } else {
        "SOURCE_GAP"
    };

    let coverage_path = config.artifact_dir.join("source_coverage.json");
    fs::write(
        &coverage_path,
        serde_json::to_vec_pretty(&coverage_receipts)?,
    )?;
    let gaps_path = config.artifact_dir.join("source_gaps.json");
    fs::write(&gaps_path, serde_json::to_vec_pretty(&gaps)?)?;
    let rollover_path = config.artifact_dir.join("rollover_edges.json");
    fs::write(
        &rollover_path,
        serde_json::to_vec_pretty(&rollover_evidence)?,
    )?;
    let latency_summary_path = config.artifact_dir.join("latency_summary.json");
    fs::write(
        &latency_summary_path,
        serde_json::to_vec_pretty(&latency_summary)?,
    )?;
    let hot_path_receipts_path = config.artifact_dir.join("hot_path_receipts.jsonl");
    write_jsonl(&hot_path_receipts_path, &hot_receipts)?;
    let write_batch_receipts_path = config.artifact_dir.join("write_batch_receipts.jsonl");
    write_jsonl(&write_batch_receipts_path, &write_receipts)?;
    let queue_samples_path = config.artifact_dir.join("queue_samples.jsonl");
    write_jsonl(&queue_samples_path, &queue_samples)?;
    let resource_path = config.artifact_dir.join("resource_watermark.json");
    fs::write(
        &resource_path,
        serde_json::to_vec_pretty(&resource_watermark)?,
    )?;
    let manifest = json!({
        "run_id": run_id,
        "verdict": verdict,
        "artifact_path": published.final_path,
        "row_count": published.row_count,
        "sha256": published.sha256,
        "schema_version": published.schema_version,
        "collector_host_id": collector_host_id,
        "collector_region": collector_region,
        "source_coverage": coverage_receipts,
        "source_gaps": gaps,
        "rollover_edges": rollover_evidence,
        "latency_summary": latency_summary,
        "hot_path_probe_enabled": config.hot_path_probe_enabled,
        "hot_path_receipts_path": hot_path_receipts_path,
        "write_batch_receipts_path": write_batch_receipts_path,
        "queue_samples_path": queue_samples_path,
        "resource_watermark": resource_watermark,
        "finalize_timing": finalize_timing,
        "polymarket_subscription_json_sha256": market_subscription_json_sha256,
        "rtds_subscription_json_sha256": rtds_subscription_json_sha256,
        "dataset_root": config.dataset_root,
        "artifact_dir": config.artifact_dir,
        "recv_time_utc_ms": now_ms()
    });
    let manifest_path = config.artifact_dir.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    println!("real_source_manifest={}", manifest_path.display());
    println!("real_source_verdict={verdict}");
    println!("real_source_rows={}", published.row_count);

    if verdict == "PASS" {
        Ok(())
    } else {
        Err(format!(
            "real source alignment probe has gaps; manifest={}",
            manifest_path.display()
        )
        .into())
    }
}

async fn discover_pair_with_wait(
    gamma: &GammaClient,
    wait_secs: u64,
) -> Result<BtcUpdownDiscoveryPair, Box<dyn Error + Send + Sync>> {
    let started_ms = now_ms();
    let final_error = loop {
        let query_ms = now_ms();
        let windows = discovery_windows(query_ms);
        let current = fetch_exact_slug_candidates(gamma, &windows.current.slug).await?;
        let next = fetch_exact_slug_candidates(gamma, &windows.next.slug).await?;
        match select_btc_updown_pair(query_ms, &current, &next) {
            Ok(pair) => return Ok(pair),
            Err(err) => {
                let last_error = format!("{err:?}");
                if now_ms().saturating_sub(started_ms) >= (wait_secs * 1000) as i64 {
                    break last_error;
                }
                sleep(Duration::from_secs(10)).await;
            }
        }
    };
    Err(format!(
        "btc-updown-5m discovery failed after wait_secs={wait_secs}: {}",
        final_error
    )
    .into())
}

async fn fetch_exact_slug_candidates(
    gamma: &GammaClient,
    slug: &str,
) -> Result<Vec<BtcUpdownMarketCandidate>, Box<dyn Error + Send + Sync>> {
    let mut request = MarketsRequest::default();
    request.slug = vec![slug.to_owned()];
    request.closed = Some(false);
    request.limit = Some(10);
    let markets = gamma.markets(&request).await?;
    Ok(markets
        .iter()
        .map(BtcUpdownMarketCandidate::from_sdk_market)
        .collect())
}

async fn run_polymarket_source(
    tx: mpsc::Sender<TimedRawEvent>,
    gaps: Arc<Mutex<Vec<String>>>,
    rollover_edges: Arc<Mutex<Vec<RolloverEdge>>>,
    collector_identity: CollectorIdentity,
    deadline_ms: i64,
    discovery_wait_secs: u64,
    run_started: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let gamma = GammaClient::default();
    let mut previous_pair = None::<BtcUpdownDiscoveryPair>;
    while now_ms() < deadline_ms {
        let pair = match discover_pair_with_wait(&gamma, discovery_wait_secs).await {
            Ok(pair) => pair,
            Err(err) => {
                record_gap(&gaps, format!("polymarket_discovery_source_gap:{err}"));
                sleep(Duration::from_secs(10)).await;
                continue;
            }
        };
        if let Some(previous) = &previous_pair {
            rollover_edges
                .lock()
                .expect("rollover edge mutex poisoned")
                .push(RolloverEdge::from_pairs(previous, &pair));
        }
        previous_pair = Some(pair.clone());
        let segment_end_ms = (pair.current.end_ts_s * 1000 + 5_000).min(deadline_ms);
        collect_polymarket_pair(
            &pair,
            &tx,
            &gaps,
            &collector_identity.host_id,
            &collector_identity.region,
            segment_end_ms,
            run_started,
        )
        .await?;
    }
    Ok(())
}

async fn collect_polymarket_pair(
    pair: &BtcUpdownDiscoveryPair,
    tx: &mpsc::Sender<TimedRawEvent>,
    gaps: &Arc<Mutex<Vec<String>>>,
    collector_host_id: &str,
    collector_region: &str,
    segment_end_ms: i64,
    run_started: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let assets = pair_asset_ids(pair)
        .iter()
        .map(|asset| U256::from_str(asset))
        .collect::<Result<Vec<_>, _>>()?;
    let asset_to_slug = pair_asset_to_slug(pair);
    let client = MarketWsClient::new(MARKET_WS_ENDPOINT, bounded_ws_config())?;
    let mut bba_stream = Box::pin(client.subscribe_best_bid_ask(assets.clone())?);
    let mut book_stream = Box::pin(client.subscribe_orderbook(assets)?);
    let collector_identity = CollectorIdentity {
        host_id: collector_host_id.to_owned(),
        region: collector_region.to_owned(),
    };
    wait_market_connected(&client, Duration::from_secs(20)).await?;

    let switch_event = RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "market_discovery_switch".to_owned(),
        source_identifier: format!("{}:{}", pair.current.slug, pair.next.slug),
        source_ts: Some(now_ms().to_string()),
        recv_time_utc_ms: now_ms(),
        collector_admission: None,
        collector_host_id: collector_host_id.to_owned(),
        collector_region: collector_region.to_owned(),
        source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
        raw_json: serde_json::to_string(&json!({
            "event_type": "market_discovery_switch",
            "current": pair.current,
            "next": pair.next,
            "connection_state": format!("{:?}", client.connection_state(ChannelType::Market)),
            "subscription_count": client.subscription_count()
        }))?,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    };
    send_event(tx, gaps, run_started, switch_event).await;

    while now_ms() < segment_end_ms {
        tokio::select! {
            maybe_bba = bba_stream.next() => {
                match maybe_bba {
                    Some(Ok(event)) => {
                        let asset_id = event.asset_id.to_string();
                        let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                        let raw = json!({
                            "event_type": "best_bid_ask",
                            "market": event.market.to_string(),
                            "asset_id": asset_id,
                            "slug": slug,
                            "best_bid": event.best_bid.to_string(),
                            "best_ask": event.best_ask.to_string(),
                            "spread": event.spread.to_string(),
                            "timestamp": event.timestamp
                        });
                        send_event(tx, gaps, run_started, raw_event(
                            "polymarket_market",
                            "best_bid_ask",
                            &format!("{}:{}", slug, event.asset_id),
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            &collector_identity,
                            raw,
                        )).await;
                    }
                    Some(Err(err)) => record_gap(gaps, format!("polymarket_bba_stream_error:{err}")),
                    None => {
                        record_gap(gaps, "polymarket_bba_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_book = book_stream.next() => {
                match maybe_book {
                    Some(Ok(event)) => {
                        let asset_id = event.asset_id.to_string();
                        let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                        let raw = json!({
                            "event_type": "book",
                            "market": event.market.to_string(),
                            "asset_id": asset_id,
                            "slug": slug,
                            "timestamp": event.timestamp,
                            "bids_len": event.bids.len(),
                            "asks_len": event.asks.len(),
                            "hash": event.hash
                        });
                        send_event(tx, gaps, run_started, raw_event(
                            "polymarket_market",
                            "book",
                            &format!("{}:{}", slug, event.asset_id),
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            &collector_identity,
                            raw,
                        )).await;
                    }
                    Some(Err(err)) => record_gap(gaps, format!("polymarket_book_stream_error:{err}")),
                    None => {
                        record_gap(gaps, "polymarket_book_stream_closed".to_owned());
                        break;
                    }
                }
            }
            () = sleep(Duration::from_millis(250)) => {}
        }
    }
    Ok(())
}

async fn run_rtds_source(
    tx: mpsc::Sender<TimedRawEvent>,
    gaps: Arc<Mutex<Vec<String>>>,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = RtdsClient::new(RTDS_WS_ENDPOINT, bounded_ws_config())?;
    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let mut stream = Box::pin(client.subscribe_raw(subscription)?);
    wait_rtds_connected(&client, Duration::from_secs(20)).await?;
    while Instant::now() < deadline {
        match timeout(
            Duration::from_secs(RTDS_FRESHNESS_TIMEOUT_SECS),
            stream.next(),
        )
        .await
        {
            Ok(Some(Ok(message))) => {
                let price = message.as_chainlink_price();
                let source_ts = price
                    .as_ref()
                    .map(|payload| payload.timestamp)
                    .unwrap_or(message.timestamp);
                let raw = json!({
                    "topic": message.topic,
                    "type": message.msg_type,
                    "message_timestamp": message.timestamp,
                    "payload_timestamp": price.as_ref().map(|payload| payload.timestamp),
                    "symbol": price.as_ref().map(|payload| payload.symbol.clone()),
                    "value": price.as_ref().map(|payload| payload.value.to_string()),
                    "payload": message.payload
                });
                send_event(
                    &tx,
                    &gaps,
                    run_started,
                    raw_event(
                        "polymarket_rtds_chainlink_btc_usd",
                        "update",
                        "btc/usd",
                        Some(source_ts.to_string()),
                        RTDS_WS_ENDPOINT,
                        &collector_identity,
                        raw,
                    ),
                )
                .await;
            }
            Ok(Some(Err(err))) => record_gap(&gaps, format!("rtds_stream_error:{err}")),
            Ok(None) => {
                record_gap(&gaps, "rtds_stream_closed".to_owned());
                break;
            }
            Err(_) => record_gap(&gaps, "rtds_business_message_timeout".to_owned()),
        }
    }
    Ok(())
}

async fn run_binance_depth_source(
    tx: mpsc::Sender<TimedRawEvent>,
    gaps: Arc<Mutex<Vec<String>>>,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(BINANCE_HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(BINANCE_HTTP_TOTAL_TIMEOUT_SECS))
        .build()?;
    match http_client.get(BINANCE_DEPTH_SNAPSHOT).send().await {
        Ok(response) => {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if status.is_success() && !text.trim().is_empty() {
                send_event(
                    &tx,
                    &gaps,
                    run_started,
                    raw_event(
                        "binance_depth",
                        "snapshot",
                        "BTCUSDT",
                        None,
                        BINANCE_DEPTH_SNAPSHOT,
                        &collector_identity,
                        json!({"endpoint": BINANCE_DEPTH_SNAPSHOT, "raw": text}),
                    ),
                )
                .await;
            } else {
                record_gap(&gaps, format!("binance_depth_snapshot_bad_status:{status}"));
            }
        }
        Err(err) => record_gap(&gaps, format!("binance_depth_snapshot_error:{err}")),
    }

    run_binance_ws_source(
        BinanceWsSpec {
            source_kind: "binance_depth".to_owned(),
            default_event_type: "depthUpdate".to_owned(),
            url: BINANCE_DEPTH_WS.to_owned(),
            freshness_timeout_secs: 30,
        },
        tx,
        gaps,
        collector_identity,
        deadline,
        run_started,
    )
    .await
}

async fn run_binance_ws_source(
    spec: BinanceWsSpec,
    tx: mpsc::Sender<TimedRawEvent>,
    gaps: Arc<Mutex<Vec<String>>>,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut stream, _) = match timeout(
        Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
        connect_async(&spec.url),
    )
    .await
    {
        Ok(Ok(connected)) => connected,
        Ok(Err(err)) => {
            return Err(format!("{}_ws_connect_error:{err}", spec.source_kind).into());
        }
        Err(_) => return Err(format!("{}_ws_connect_timeout", spec.source_kind).into()),
    };
    while Instant::now() < deadline {
        match timeout(
            Duration::from_secs(spec.freshness_timeout_secs),
            stream.next(),
        )
        .await
        {
            Ok(Some(Ok(Message::Text(text)))) => {
                let raw_json = text.to_string();
                let value = serde_json::from_str::<Value>(&raw_json).unwrap_or(Value::Null);
                let event_type = value
                    .get("e")
                    .and_then(Value::as_str)
                    .unwrap_or(&spec.default_event_type);
                let source_ts = value
                    .get("E")
                    .or_else(|| value.get("T"))
                    .and_then(Value::as_i64)
                    .map(|ts| ts.to_string());
                send_event(
                    &tx,
                    &gaps,
                    run_started,
                    RawEvent {
                        source_kind: spec.source_kind.clone(),
                        source_event_type: event_type.to_owned(),
                        source_identifier: "BTCUSDT".to_owned(),
                        source_ts,
                        recv_time_utc_ms: now_ms(),
                        collector_admission: None,
                        collector_host_id: collector_identity.host_id.clone(),
                        collector_region: collector_identity.region.clone(),
                        source_endpoint: spec.url.clone(),
                        raw_json,
                        parquet_raw_layout_version: 1,
                        parquet_raw_source_kind: spec.source_kind.clone(),
                    },
                )
                .await;
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = stream.send(Message::Pong(payload)).await;
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                record_gap(&gaps, format!("{}_ws_closed:{frame:?}", spec.source_kind));
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => record_gap(&gaps, format!("{}_ws_error:{err}", spec.source_kind)),
            Ok(None) => {
                record_gap(&gaps, format!("{}_ws_stream_ended", spec.source_kind));
                break;
            }
            Err(_) => record_gap(
                &gaps,
                format!("{}_business_message_timeout", spec.source_kind),
            ),
        }
    }
    Ok(())
}

async fn wait_market_connected(
    client: &MarketWsClient,
    timeout_duration: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(timeout_duration, async {
        loop {
            if matches!(
                client.connection_state(ChannelType::Market),
                ConnectionState::Connected { .. }
            ) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "market ws did not connect in time")?;
    Ok(())
}

async fn wait_rtds_connected(
    client: &RtdsClient,
    timeout_duration: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(timeout_duration, async {
        loop {
            if matches!(client.connection_state(), ConnectionState::Connected { .. }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "rtds ws did not connect in time")?;
    Ok(())
}

fn bounded_ws_config() -> WsConfig {
    let mut config = WsConfig::default();
    config.reconnect.max_attempts = Some(3);
    config.reconnect.max_backoff = Duration::from_secs(5);
    config.heartbeat_interval = Duration::from_secs(10);
    config.heartbeat_timeout = Duration::from_secs(20);
    config
}

async fn send_event(
    tx: &mpsc::Sender<TimedRawEvent>,
    gaps: &Arc<Mutex<Vec<String>>>,
    run_started: Instant,
    event: RawEvent,
) {
    let source_kind = event.source_kind.clone();
    let timed_event = TimedRawEvent::new(event, monotonic_ms(run_started));
    match tx.try_send(timed_event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(timed_event)) => {
            record_gap(
                gaps,
                format!("bounded_queue_backpressure_wait:{source_kind}"),
            );
            match timeout(
                Duration::from_millis(SOURCE_SEND_TIMEOUT_MS),
                tx.send(timed_event),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => record_gap(gaps, format!("bounded_queue_send_failed:{err}")),
                Err(_) => record_gap(
                    gaps,
                    format!("bounded_queue_backpressure_timeout:{source_kind}"),
                ),
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            record_gap(gaps, format!("bounded_queue_send_closed:{source_kind}"));
        }
    }
}

async fn send_chunk(
    context: WriterChunkSendContext<'_>,
    batch: &mut Vec<SequencedRawEvent>,
    batch_seq: &mut u64,
    flush_reason: FlushReason,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if batch.is_empty() {
        return Ok(());
    }
    *batch_seq = (*batch_seq).saturating_add(1);
    let rows = batch.len() as u64;
    let bytes_estimate = batch.iter().map(estimated_sequenced_event_bytes).sum();
    let mut chunk = WriteChunk {
        batch_seq: *batch_seq,
        events: std::mem::take(batch),
        flush_reason,
        batch_send_time_utc_ms: now_ms(),
        rows,
        bytes_estimate,
    };
    loop {
        context.writer_queue_rows.fetch_add(rows, Ordering::Relaxed);
        context
            .writer_queue_bytes
            .fetch_add(bytes_estimate, Ordering::Relaxed);
        match context.chunk_tx.try_send(chunk) {
            Ok(()) => {
                context.writer_queue_tracker.observe(
                    monotonic_ms(context.run_started),
                    context.writer_queue_rows.load(Ordering::Relaxed),
                    context.writer_queue_bytes.load(Ordering::Relaxed),
                );
                return Ok(());
            }
            Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                context.writer_queue_rows.fetch_sub(rows, Ordering::Relaxed);
                context
                    .writer_queue_bytes
                    .fetch_sub(bytes_estimate, Ordering::Relaxed);
                context.writer_queue_tracker.record_backpressure_wait();
                chunk = returned;
                sleep(Duration::from_millis(50)).await;
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                context.writer_queue_rows.fetch_sub(rows, Ordering::Relaxed);
                context
                    .writer_queue_bytes
                    .fetch_sub(bytes_estimate, Ordering::Relaxed);
                return Err("writer chunk channel disconnected".into());
            }
        }
    }
}

async fn send_hot_handoff(
    hot_tx: &mpsc::Sender<HotHandoffEvent>,
    gaps: &Arc<Mutex<Vec<String>>>,
    hot_queue_tracker: &mut QueueDurationTracker,
    sequenced: SequencedRawEvent,
) {
    let source_kind = sequenced.event.source_kind.clone();
    let event = HotHandoffEvent {
        sequenced,
        hot_send_time_utc_ms: now_ms(),
    };
    match hot_tx.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(event)) => {
            hot_queue_tracker.record_backpressure_wait();
            record_gap(
                gaps,
                format!("hot_handoff_queue_backpressure_wait:{source_kind}"),
            );
            match timeout(
                Duration::from_millis(SOURCE_SEND_TIMEOUT_MS),
                hot_tx.send(event),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => record_gap(gaps, format!("hot_handoff_send_failed:{err}")),
                Err(_) => {
                    hot_queue_tracker.record_backpressure_timeout();
                    hot_queue_tracker.record_drop(1);
                    record_gap(
                        gaps,
                        format!("hot_handoff_queue_backpressure_timeout:{source_kind}"),
                    );
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            hot_queue_tracker.record_drop(1);
            record_gap(gaps, format!("hot_handoff_send_closed:{source_kind}"));
        }
    }
}

fn record_queue_sample(samples: &mut Vec<QueueSampleReceipt>, context: QueueSampleContext<'_>) {
    let sample_monotonic_ms = monotonic_ms(context.run_started);
    let source_event_queue_bytes = context
        .event_rx_len
        .saturating_mul(context.average_event_bytes);
    let hot_handoff_queue_rows = context
        .hot_queue_capacity
        .saturating_sub(context.hot_tx.capacity()) as u64;
    let hot_handoff_queue_bytes =
        hot_handoff_queue_rows.saturating_mul(context.average_event_bytes);
    let writer_chunk_queue_rows = context.writer_queue_rows.load(Ordering::Relaxed);
    let writer_chunk_queue_bytes = context.writer_queue_bytes.load(Ordering::Relaxed);

    context.source_queue_tracker.observe(
        sample_monotonic_ms,
        context.event_rx_len,
        source_event_queue_bytes,
    );
    context.hot_queue_tracker.observe(
        sample_monotonic_ms,
        hot_handoff_queue_rows,
        hot_handoff_queue_bytes,
    );
    context.writer_queue_tracker.observe(
        sample_monotonic_ms,
        writer_chunk_queue_rows,
        writer_chunk_queue_bytes,
    );

    samples.push(QueueSampleReceipt {
        sample_time_utc_ms: now_ms(),
        sample_monotonic_ms,
        source_event_queue_rows: context.event_rx_len,
        source_event_queue_bytes,
        hot_handoff_queue_rows,
        hot_handoff_queue_bytes,
        writer_chunk_queue_rows,
        writer_chunk_queue_bytes,
        source_event_queue: context.source_queue_tracker.snapshot(sample_monotonic_ms),
        hot_handoff_queue: context.hot_queue_tracker.snapshot(sample_monotonic_ms),
        writer_chunk_queue: context.writer_queue_tracker.snapshot(sample_monotonic_ms),
    });
}

fn build_latency_summary(
    hot_receipts: &[HotPathReceipt],
    write_receipts: &[WriteBatchReceipt],
    finalize_timing: &data_collect::writer::FinalizeTiming,
    source_queue_summary: QueueDurationSummary,
    hot_queue_summary: QueueDurationSummary,
    writer_queue_summary: QueueDurationSummary,
) -> LatencySummary {
    let source_recv_to_hot_delivery_ms = hot_receipts
        .iter()
        .map(|receipt| receipt.source_recv_to_hot_delivery_ms)
        .collect::<Vec<_>>();
    let source_recv_to_collector_dequeue_ms = hot_receipts
        .iter()
        .map(|receipt| receipt.source_recv_to_collector_dequeue_ms)
        .collect::<Vec<_>>();
    let collector_dequeue_to_hot_send_ms = hot_receipts
        .iter()
        .map(|receipt| {
            receipt
                .hot_send_time_utc_ms
                .saturating_sub(receipt.collector_dequeue_time_utc_ms)
                .max(0) as u64
        })
        .collect::<Vec<_>>();
    let hot_queue_wait_ms = hot_receipts
        .iter()
        .map(|receipt| receipt.hot_queue_wait_ms)
        .collect::<Vec<_>>();
    let source_recv_to_write_done_ms = write_receipts
        .iter()
        .flat_map(|receipt| receipt.source_recv_to_write_done_ms.iter().copied())
        .collect::<Vec<_>>();
    let batch_wait_ms = write_receipts
        .iter()
        .map(|receipt| receipt.batch_wait_ms)
        .collect::<Vec<_>>();
    let writer_channel_wait_ms = write_receipts
        .iter()
        .map(|receipt| receipt.writer_channel_wait_ms)
        .collect::<Vec<_>>();
    let write_events_us = write_receipts
        .iter()
        .map(|receipt| receipt.write_events_us)
        .collect::<Vec<_>>();
    let hot_path_drop_count = hot_queue_summary.dropped_rows;
    let backpressure_wait_count = source_queue_summary
        .backpressure_wait_count
        .saturating_add(hot_queue_summary.backpressure_wait_count)
        .saturating_add(writer_queue_summary.backpressure_wait_count);
    let backpressure_timeout_count = source_queue_summary
        .backpressure_timeout_count
        .saturating_add(hot_queue_summary.backpressure_timeout_count)
        .saturating_add(writer_queue_summary.backpressure_timeout_count);
    let dropped_rows = source_queue_summary
        .dropped_rows
        .saturating_add(hot_queue_summary.dropped_rows)
        .saturating_add(writer_queue_summary.dropped_rows);
    let writer_backlog_monotonic_growth_observed =
        writer_queue_summary.sustained_congestion_observed(3, 5_000);

    LatencySummary {
        source_recv_to_hot_delivery_ms: LatencyStats::from_values(&source_recv_to_hot_delivery_ms),
        source_recv_to_write_done_ms: LatencyStats::from_values(&source_recv_to_write_done_ms),
        source_recv_to_collector_dequeue_ms: LatencyStats::from_values(
            &source_recv_to_collector_dequeue_ms,
        ),
        collector_dequeue_to_hot_send_ms: LatencyStats::from_values(
            &collector_dequeue_to_hot_send_ms,
        ),
        hot_queue_wait_ms: LatencyStats::from_values(&hot_queue_wait_ms),
        batch_wait_ms: LatencyStats::from_values(&batch_wait_ms),
        writer_channel_wait_ms: LatencyStats::from_values(&writer_channel_wait_ms),
        write_events_us: LatencyStats::from_values(&write_events_us),
        finalize_total_us: finalize_timing.finalize_total_us,
        finalize_close_us: finalize_timing.finalize_close_us,
        sha256_us: finalize_timing.sha256_us,
        atomic_rename_us: finalize_timing.atomic_rename_us,
        running_to_digesting_rename_us: finalize_timing.running_to_digesting_rename_us,
        digesting_to_final_rename_us: finalize_timing.digesting_to_final_rename_us,
        source_event_queue: source_queue_summary,
        hot_handoff_queue: hot_queue_summary,
        writer_chunk_queue: writer_queue_summary,
        backpressure_wait_count,
        backpressure_timeout_count,
        dropped_rows,
        hot_path_drop_count,
        writer_backlog_monotonic_growth_observed,
    }
}

fn write_jsonl<T: Serialize>(path: &Path, items: &[T]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut file = File::create(path)?;
    for item in items {
        serde_json::to_writer(&mut file, item)?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

fn raw_event(
    source_kind: &str,
    source_event_type: &str,
    source_identifier: &str,
    source_ts: Option<String>,
    source_endpoint: &str,
    collector_identity: &CollectorIdentity,
    raw: Value,
) -> RawEvent {
    RawEvent {
        source_kind: source_kind.to_owned(),
        source_event_type: source_event_type.to_owned(),
        source_identifier: source_identifier.to_owned(),
        source_ts,
        recv_time_utc_ms: now_ms(),
        collector_admission: None,
        collector_host_id: collector_identity.host_id.clone(),
        collector_region: collector_identity.region.clone(),
        source_endpoint: source_endpoint.to_owned(),
        raw_json: raw.to_string(),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind.to_owned(),
    }
}

fn pair_asset_ids(pair: &BtcUpdownDiscoveryPair) -> Vec<String> {
    vec![
        pair.current.up_asset_id.clone(),
        pair.current.down_asset_id.clone(),
        pair.next.up_asset_id.clone(),
        pair.next.down_asset_id.clone(),
    ]
}

fn pair_asset_to_slug(pair: &BtcUpdownDiscoveryPair) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert(pair.current.up_asset_id.clone(), pair.current.slug.clone());
    map.insert(
        pair.current.down_asset_id.clone(),
        pair.current.slug.clone(),
    );
    map.insert(pair.next.up_asset_id.clone(), pair.next.slug.clone());
    map.insert(pair.next.down_asset_id.clone(), pair.next.slug.clone());
    map
}

fn update_coverage(coverage: &mut BTreeMap<String, CoverageStats>, event: &RawEvent) {
    let stats = coverage.entry(event.source_kind.clone()).or_default();
    stats.rows = stats.rows.saturating_add(1);
    *stats
        .event_type_counts
        .entry(event.source_event_type.clone())
        .or_default() += 1;
    stats.first_recv_time_utc_ms = stats
        .first_recv_time_utc_ms
        .map_or(Some(event.recv_time_utc_ms), |value| {
            Some(value.min(event.recv_time_utc_ms))
        });
    stats.last_recv_time_utc_ms = stats
        .last_recv_time_utc_ms
        .map_or(Some(event.recv_time_utc_ms), |value| {
            Some(value.max(event.recv_time_utc_ms))
        });
    if event.source_kind == "polymarket_market"
        && let Some((slug, _)) = event.source_identifier.split_once(':')
        && slug.starts_with("btc-updown-5m-")
    {
        stats.market_windows.insert(slug.to_owned());
        stats
            .market_window_first_recv_time_utc_ms
            .entry(slug.to_owned())
            .and_modify(|value| *value = (*value).min(event.recv_time_utc_ms))
            .or_insert(event.recv_time_utc_ms);
    }
}

#[derive(Default)]
struct CoverageStats {
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    rows: u64,
    market_windows: BTreeSet<String>,
    market_window_first_recv_time_utc_ms: BTreeMap<String, i64>,
    event_type_counts: BTreeMap<String, u64>,
}

impl CoverageStats {
    fn to_receipt(&self, source_kind: &str) -> CoverageReceipt {
        CoverageReceipt {
            source_kind: source_kind.to_owned(),
            first_recv_time_utc_ms: self.first_recv_time_utc_ms,
            last_recv_time_utc_ms: self.last_recv_time_utc_ms,
            rows: self.rows,
            market_windows: self.market_windows.iter().cloned().collect(),
            market_window_first_recv_time_utc_ms: self.market_window_first_recv_time_utc_ms.clone(),
            event_type_counts: self.event_type_counts.clone(),
        }
    }
}

#[derive(Serialize)]
struct CoverageReceipt {
    source_kind: String,
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    rows: u64,
    market_windows: Vec<String>,
    market_window_first_recv_time_utc_ms: BTreeMap<String, i64>,
    event_type_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Serialize)]
struct RolloverEdge {
    edge_recv_time_utc_ms: i64,
    previous_current_slug: String,
    previous_next_slug: String,
    new_current_slug: String,
    new_next_slug: String,
    previous_current_end_ts_s: i64,
    previous_next_start_ts_s: i64,
    new_current_start_ts_s: i64,
}

impl RolloverEdge {
    fn from_pairs(previous: &BtcUpdownDiscoveryPair, next: &BtcUpdownDiscoveryPair) -> Self {
        Self {
            edge_recv_time_utc_ms: now_ms(),
            previous_current_slug: previous.current.slug.clone(),
            previous_next_slug: previous.next.slug.clone(),
            new_current_slug: next.current.slug.clone(),
            new_next_slug: next.next.slug.clone(),
            previous_current_end_ts_s: previous.current.end_ts_s,
            previous_next_start_ts_s: previous.next.start_ts_s,
            new_current_start_ts_s: next.current.start_ts_s,
        }
    }
}

#[derive(Serialize)]
struct RolloverEvidenceReceipt {
    edge_recv_time_utc_ms: i64,
    previous_current_slug: String,
    previous_next_slug: String,
    new_current_slug: String,
    new_next_slug: String,
    prefetched_promoted_to_current: bool,
    previous_current_retired_by_time: bool,
    windows_contiguous: bool,
    new_current_first_recv_time_utc_ms: Option<i64>,
    previous_next_first_recv_time_utc_ms: Option<i64>,
}

fn build_rollover_receipts(
    edges: &[RolloverEdge],
    coverage: &BTreeMap<String, CoverageStats>,
) -> Vec<RolloverEvidenceReceipt> {
    let market_coverage = coverage.get("polymarket_market");
    edges
        .iter()
        .map(|edge| {
            let new_current_first_recv_time_utc_ms = market_coverage
                .and_then(|stats| {
                    stats
                        .market_window_first_recv_time_utc_ms
                        .get(&edge.new_current_slug)
                })
                .copied();
            let previous_next_first_recv_time_utc_ms = market_coverage
                .and_then(|stats| {
                    stats
                        .market_window_first_recv_time_utc_ms
                        .get(&edge.previous_next_slug)
                })
                .copied();
            RolloverEvidenceReceipt {
                edge_recv_time_utc_ms: edge.edge_recv_time_utc_ms,
                previous_current_slug: edge.previous_current_slug.clone(),
                previous_next_slug: edge.previous_next_slug.clone(),
                new_current_slug: edge.new_current_slug.clone(),
                new_next_slug: edge.new_next_slug.clone(),
                prefetched_promoted_to_current: edge.previous_next_slug == edge.new_current_slug,
                previous_current_retired_by_time: edge.edge_recv_time_utc_ms
                    >= edge.previous_current_end_ts_s * 1000,
                windows_contiguous: edge.previous_current_end_ts_s == edge.previous_next_start_ts_s
                    && edge.previous_next_start_ts_s == edge.new_current_start_ts_s,
                new_current_first_recv_time_utc_ms,
                previous_next_first_recv_time_utc_ms,
            }
        })
        .collect()
}

#[derive(Clone)]
struct ProbeConfig {
    artifact_dir: PathBuf,
    dataset_root: Option<PathBuf>,
    duration_secs: u64,
    discovery_wait_secs: u64,
    queue_capacity: usize,
    flush_rows: usize,
    hot_queue_capacity: usize,
    max_flush_interval_ms: u64,
    hot_path_probe_enabled: bool,
    min_market_windows: usize,
    min_rollover_edges: usize,
}

#[derive(Clone)]
struct CollectorIdentity {
    host_id: String,
    region: String,
}

struct BinanceWsSpec {
    source_kind: String,
    default_event_type: String,
    url: String,
    freshness_timeout_secs: u64,
}

struct WriterOutput {
    published: InstrumentedPublishedSegment,
    write_receipts: Vec<WriteBatchReceipt>,
}

struct WriteChunk {
    batch_seq: u64,
    events: Vec<SequencedRawEvent>,
    flush_reason: FlushReason,
    batch_send_time_utc_ms: i64,
    rows: u64,
    bytes_estimate: u64,
}

struct HotHandoffEvent {
    sequenced: SequencedRawEvent,
    hot_send_time_utc_ms: i64,
}

struct QueueSampleContext<'a> {
    run_started: Instant,
    event_rx_len: u64,
    hot_tx: &'a mpsc::Sender<HotHandoffEvent>,
    hot_queue_capacity: usize,
    writer_queue_rows: &'a AtomicU64,
    writer_queue_bytes: &'a AtomicU64,
    average_event_bytes: u64,
    source_queue_tracker: &'a mut QueueDurationTracker,
    hot_queue_tracker: &'a mut QueueDurationTracker,
    writer_queue_tracker: &'a mut QueueDurationTracker,
}

struct WriterChunkSendContext<'a> {
    chunk_tx: &'a std::sync::mpsc::SyncSender<WriteChunk>,
    writer_queue_rows: &'a AtomicU64,
    writer_queue_bytes: &'a AtomicU64,
    writer_queue_tracker: &'a mut QueueDurationTracker,
    run_started: Instant,
}

#[derive(Serialize)]
struct QueueSampleReceipt {
    sample_time_utc_ms: i64,
    sample_monotonic_ms: u64,
    source_event_queue_rows: u64,
    source_event_queue_bytes: u64,
    hot_handoff_queue_rows: u64,
    hot_handoff_queue_bytes: u64,
    writer_chunk_queue_rows: u64,
    writer_chunk_queue_bytes: u64,
    source_event_queue: QueueDurationSummary,
    hot_handoff_queue: QueueDurationSummary,
    writer_chunk_queue: QueueDurationSummary,
}

#[derive(Serialize)]
struct LatencySummary {
    source_recv_to_hot_delivery_ms: LatencyStats,
    source_recv_to_write_done_ms: LatencyStats,
    source_recv_to_collector_dequeue_ms: LatencyStats,
    collector_dequeue_to_hot_send_ms: LatencyStats,
    hot_queue_wait_ms: LatencyStats,
    batch_wait_ms: LatencyStats,
    writer_channel_wait_ms: LatencyStats,
    write_events_us: LatencyStats,
    finalize_total_us: u64,
    finalize_close_us: u64,
    sha256_us: u64,
    atomic_rename_us: u64,
    running_to_digesting_rename_us: u64,
    digesting_to_final_rename_us: u64,
    source_event_queue: QueueDurationSummary,
    hot_handoff_queue: QueueDurationSummary,
    writer_chunk_queue: QueueDurationSummary,
    backpressure_wait_count: u64,
    backpressure_timeout_count: u64,
    dropped_rows: u64,
    hot_path_drop_count: u64,
    writer_backlog_monotonic_growth_observed: bool,
}

impl ProbeConfig {
    fn from_env_and_args() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Ok(Self {
            artifact_dir: artifact_dir()?,
            dataset_root: std::env::var("DATASET_ROOT").ok().map(PathBuf::from),
            duration_secs: env_u64_alias(
                "PHASE0_REAL_SOURCE_DURATION_SECS",
                "REAL_SOURCE_RUN_SECS",
                DEFAULT_DURATION_SECS,
            ),
            discovery_wait_secs: env_u64(
                "PHASE0_REAL_SOURCE_DISCOVERY_WAIT_SECS",
                DEFAULT_DISCOVERY_WAIT_SECS,
            ),
            queue_capacity: env_usize("PHASE0_REAL_SOURCE_QUEUE_CAPACITY", DEFAULT_QUEUE_CAPACITY),
            flush_rows: env_usize("PHASE0_REAL_SOURCE_FLUSH_ROWS", DEFAULT_FLUSH_ROWS),
            hot_queue_capacity: env_usize(
                "PHASE0_REAL_SOURCE_HOT_QUEUE_CAPACITY",
                DEFAULT_HOT_QUEUE_CAPACITY,
            ),
            max_flush_interval_ms: env_u64(
                "PHASE0_REAL_SOURCE_MAX_FLUSH_INTERVAL_MS",
                DEFAULT_MAX_FLUSH_INTERVAL_MS,
            ),
            hot_path_probe_enabled: env_bool("PHASE0_REAL_SOURCE_ENABLE_HOT_PATH_PROBE", true),
            min_market_windows: env_usize("PHASE0_REAL_SOURCE_MIN_WINDOWS", DEFAULT_MIN_WINDOWS),
            min_rollover_edges: env_usize(
                "PHASE0_REAL_SOURCE_MIN_ROLLOVER_EDGES",
                DEFAULT_MIN_ROLLOVER_EDGES,
            ),
        })
    }
}

fn validate_artifact_root(config: &ProbeConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    let Some(dataset_root) = &config.dataset_root else {
        return Ok(());
    };
    fs::create_dir_all(dataset_root)?;
    fs::create_dir_all(&config.artifact_dir)?;
    let root = fs::canonicalize(dataset_root)?;
    let artifact = fs::canonicalize(&config.artifact_dir)?;
    if !artifact.starts_with(&root) {
        return Err(format!(
            "artifact dir {} is outside DATASET_ROOT {}",
            artifact.display(),
            root.display()
        )
        .into());
    }
    Ok(())
}

fn artifact_dir() -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--artifact-dir" {
            let path = args
                .next()
                .ok_or("--artifact-dir requires a following path")?;
            return Ok(PathBuf::from(path));
        }
    }
    if let Ok(path) = std::env::var("REAL_SOURCE_ARTIFACT_DIR")
        && !path.trim().is_empty()
    {
        return Ok(PathBuf::from(path));
    }
    if let Ok(root) = std::env::var("DATASET_ROOT") {
        let run_id = env_or_default(
            "REAL_SOURCE_RUN_ID",
            format!("phase0-real-source-{}-{}", now_ms(), std::process::id()),
        );
        return Ok(PathBuf::from(root).join("phase0-validation").join(run_id));
    }
    std::env::current_dir()
        .map(|cwd| cwd.join("real-source-alignment-artifacts"))
        .map_err(Into::into)
}

fn expected_sources() -> &'static [&'static str] {
    &[
        "polymarket_market",
        "binance_depth",
        "binance_aggtrade",
        "polymarket_rtds_chainlink_btc_usd",
    ]
}

fn env_or_default(key: &str, default: String) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64_alias(primary: &str, secondary: &str, default: u64) -> u64 {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(secondary).ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn collector_host_id() -> String {
    if let Ok(host_id) = std::env::var("COLLECTOR_HOST_ID")
        && !host_id.trim().is_empty()
    {
        return host_id;
    }
    fs::read_to_string(Path::new("/etc/hostname"))
        .map(|value| value.trim().to_owned())
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn record_gap(gaps: &Arc<Mutex<Vec<String>>>, gap: String) {
    gaps.lock().expect("source gap mutex poisoned").push(gap);
}

fn rolling_average_bytes(previous_average: u64, current_value: u64, observed_rows: u64) -> u64 {
    if observed_rows <= 1 {
        return current_value.max(1);
    }
    let prior_weight = observed_rows.saturating_sub(1);
    previous_average
        .saturating_mul(prior_weight)
        .saturating_add(current_value)
        / observed_rows
}

fn estimated_sequenced_event_bytes(event: &SequencedRawEvent) -> u64 {
    estimated_event_bytes(&event.event)
}

fn estimated_event_bytes(event: &RawEvent) -> u64 {
    event.source_kind.len() as u64
        + event.source_event_type.len() as u64
        + event.source_identifier.len() as u64
        + event.source_ts.as_deref().unwrap_or_default().len() as u64
        + event.collector_host_id.len() as u64
        + event.collector_region.len() as u64
        + event.source_endpoint.len() as u64
        + event.raw_json.len() as u64
        + event.parquet_raw_source_kind.len() as u64
}

fn read_rss_bytes() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(rss_pages * 4096)
}

fn dir_size_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(dir_size_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn monotonic_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}
