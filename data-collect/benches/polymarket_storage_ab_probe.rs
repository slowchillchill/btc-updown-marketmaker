use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use data_collect::polymarket_orderbook_layout::{
    PolymarketOrderbookContext, PolymarketOrderbookRow,
};
use data_collect::polymarket_orderbook_v3::{
    POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketOrderbookV3Admission,
    PolymarketOrderbookV3Event, PolymarketOrderbookV3Message, PolymarketOrderbookV3Projection,
};
use data_collect::writer::{
    PARQUET_ROW_GROUP_MAX_BYTES, PARQUET_ROW_GROUP_MAX_ROWS,
    PolymarketOrderbookV3WriteSequenceState, TypedParquetSegmentWriter,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use polymarket_client_sdk_v2::clob::ws::WsMessage;
use polymarket_client_sdk_v2::clob::ws::interest::MessageInterest;
use polymarket_client_sdk_v2::clob::ws::types::response::parse_if_interested;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FIXTURE_VERSION: &str = "polymarket_storage_ab_fixture_v1";
const MARKET: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
const SLUG: &str = "btc-updown-5m-1783392300";
const BASE_TIME_MS: i64 = 1_783_392_404_600;
const ZSTD_LEVEL: i32 = 3;
const SCAN_BATCH_ROWS: usize = 65_536;

const PRICE_CHANGE_MESSAGES: usize = 7_800;
const BOOK_MESSAGES: usize = 151;
const BEST_BID_ASK_MESSAGES: usize = 145;
const LAST_TRADE_PRICE_MESSAGES: usize = 80;
const TICK_SIZE_CHANGE_MESSAGES: usize = 8;
const NEW_MARKET_MESSAGES: usize = 4;
const MARKET_RESOLVED_MESSAGES: usize = 4;
const LOGICAL_MESSAGES: usize = PRICE_CHANGE_MESSAGES
    + BOOK_MESSAGES
    + BEST_BID_ASK_MESSAGES
    + LAST_TRADE_PRICE_MESSAGES
    + TICK_SIZE_CHANGE_MESSAGES
    + NEW_MARKET_MESSAGES
    + MARKET_RESOLVED_MESSAGES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageCase {
    V2,
    V3,
}

impl StorageCase {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "v2" => Ok(Self::V2),
            "v3" => Ok(Self::V3),
            _ => Err(invalid_input(format!("unknown storage case: {value}"))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::V2 => "v2",
            Self::V3 => "v3",
        }
    }

    fn schema_version(self) -> &'static str {
        match self {
            Self::V2 => "polymarket_orderbook_v2",
            Self::V3 => POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        }
    }
}

#[derive(Clone)]
struct LogicalDelivery {
    logical_delivery_id: u64,
    event_type: &'static str,
    raw_sdk_json: String,
    slug: String,
    matched_assets: BTreeSet<String>,
    platform_duplicate_group: Option<&'static str>,
}

#[derive(Clone, Copy)]
struct RetryAttempt {
    source_delivery_id: u64,
}

#[derive(Clone, Copy)]
struct ProcessUsage {
    user_us: u64,
    system_us: u64,
    peak_rss_kib: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ColumnBytes {
    chunks: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct WriteReport {
    fixture_digest_sha256: String,
    logical_messages: u64,
    persisted_rows: u64,
    event_rows: BTreeMap<String, u64>,
    schema_version: String,
    compression: String,
    row_group_max_rows: u64,
    row_group_max_logical_bytes: u64,
    row_groups: u64,
    column_chunks: BTreeMap<String, ColumnBytes>,
    finalized_bytes: u64,
    finalized_sha256: String,
    write_finalize_wall_us: u64,
    finalize_wall_us: u64,
    process_user_cpu_us: u64,
    process_system_cpu_us: u64,
    logical_messages_per_second: f64,
    peak_rss_kib: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ScanReport {
    scanned_rows: u64,
    cache_drop_advice: String,
    scan_wall_us: u64,
    scan_user_cpu_us: u64,
    scan_system_cpu_us: u64,
    scan_rows_per_second: f64,
    peak_rss_kib: u64,
}

#[derive(Serialize)]
struct FixtureReport {
    version: &'static str,
    logical_messages: u64,
    expected_v2_rows: u64,
    retained_price_change_entries: u64,
    variant_messages: BTreeMap<String, u64>,
    identical_platform_deliveries: u64,
    distinct_platform_admission_keys: u64,
    internal_retry_source_delivery_id: u64,
}

#[derive(Serialize)]
struct EnvironmentReport {
    os: &'static str,
    architecture: &'static str,
    kernel_release: String,
    cpu_model: String,
    logical_cpus: usize,
    memory_total_kib: u64,
    rustc: String,
    cargo: String,
}

#[derive(Serialize)]
struct ComparisonReport {
    status: &'static str,
    fixture: FixtureReport,
    fixture_digest_sha256: String,
    environment: EnvironmentReport,
    v2: WriteReport,
    v2_scan: ScanReport,
    v3: WriteReport,
    v3_scan: ScanReport,
    finalized_bytes_reduction: u64,
    finalized_bytes_reduction_percent: f64,
    scan_semantics: &'static str,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("polymarket_storage_ab_probe failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some("--write-case") => {
            let case = StorageCase::parse(required_arg(&args, 2, "case")?)?;
            let root = PathBuf::from(required_arg(&args, 3, "root")?);
            println!("{}", serde_json::to_string(&write_case(case, &root)?)?);
            Ok(())
        }
        Some("--scan-case") => {
            let case = StorageCase::parse(required_arg(&args, 2, "case")?)?;
            let path = PathBuf::from(required_arg(&args, 3, "parquet path")?);
            println!("{}", serde_json::to_string(&scan_case(case, &path)?)?);
            Ok(())
        }
        None | Some("--bench") => run_controller(),
        Some(other) => Err(invalid_input(format!("unknown probe mode: {other}"))),
    }
}

fn run_controller() -> Result<()> {
    let (deliveries, retry) = logical_fixture()?;
    let fixture = validate_fixture(&deliveries, retry)?;
    let expected_digest = fixture_digest(&deliveries, retry);
    drop(deliveries);

    let root = probe_root()?;
    let executable = env::current_exe()?;
    let v2 =
        child_json::<WriteReport>(&executable, &["--write-case", "v2", required_utf8(&root)?])?;
    let v3 =
        child_json::<WriteReport>(&executable, &["--write-case", "v3", required_utf8(&root)?])?;
    let v2_path = case_path(&root, StorageCase::V2);
    let v3_path = case_path(&root, StorageCase::V3);
    let v2_scan = child_json::<ScanReport>(
        &executable,
        &["--scan-case", "v2", required_utf8(&v2_path)?],
    )?;
    let v3_scan = child_json::<ScanReport>(
        &executable,
        &["--scan-case", "v3", required_utf8(&v3_path)?],
    )?;

    require(
        v2.fixture_digest_sha256 == expected_digest,
        "v2 fixture digest drift",
    )?;
    require(
        v3.fixture_digest_sha256 == expected_digest,
        "v3 fixture digest drift",
    )?;
    require(
        v2.logical_messages == fixture.logical_messages,
        "v2 logical message drift",
    )?;
    require(
        v3.logical_messages == fixture.logical_messages,
        "v3 logical message drift",
    )?;
    require(
        v2.persisted_rows == fixture.expected_v2_rows,
        "v2 fan-out row drift",
    )?;
    require(
        v3.persisted_rows == fixture.logical_messages,
        "v3 message-to-row drift",
    )?;
    require(
        v2.persisted_rows > v3.persisted_rows,
        "v2 fixture did not fan out",
    )?;
    require(
        v2.schema_version == StorageCase::V2.schema_version()
            && v3.schema_version == StorageCase::V3.schema_version(),
        "schema version drift",
    )?;
    require(
        v2.compression == "zstd_level_3" && v3.compression == "zstd_level_3",
        "compression policy drift",
    )?;
    require(
        v2.row_group_max_rows == v3.row_group_max_rows
            && v2.row_group_max_logical_bytes == v3.row_group_max_logical_bytes,
        "row-group policy drift",
    )?;
    require(
        column_bytes(&v2, "source_native_payload_json") > 0 && column_bytes(&v2, "hash") > 0,
        "v2 payload/hash column evidence missing",
    )?;
    require(
        !has_field(&v3, "source_native_payload_json")
            && !has_field(&v3, "hash")
            && !has_field(&v3, "transaction_hash"),
        "v3 contains an excluded field",
    )?;
    require(
        v3.finalized_bytes < v2.finalized_bytes,
        "v3 is not smaller than v2",
    )?;
    require(
        v2_scan.scanned_rows == v2.persisted_rows && v3_scan.scanned_rows == v3.persisted_rows,
        "full-column scan row count drift",
    )?;

    let reduction = v2.finalized_bytes - v3.finalized_bytes;
    let report = ComparisonReport {
        status: "PASS",
        fixture,
        fixture_digest_sha256: expected_digest,
        environment: environment_report(),
        finalized_bytes_reduction: reduction,
        finalized_bytes_reduction_percent: percent(reduction, v2.finalized_bytes),
        v2,
        v2_scan,
        v3,
        v3_scan,
        scan_semantics: "separate-process all-column scan after POSIX_FADV_DONTNEED request; the advisory does not prove physical cold-cache I/O",
    };
    println!("{}", serde_json::to_string_pretty(&report)?);

    if env::var_os("DATA_COLLECT_KEEP_STORAGE_AB_PROBE").is_none() {
        fs::remove_dir_all(&root)?;
    }
    Ok(())
}

fn write_case(case: StorageCase, root: &Path) -> Result<WriteReport> {
    let (deliveries, retry) = logical_fixture()?;
    let digest = fixture_digest(&deliveries, retry);
    let logical_messages = u64::try_from(deliveries.len())?;
    let case_root = root.join(case.name());
    fs::create_dir(&case_root)?;

    let before = process_usage()?;
    let started = Instant::now();
    let mut writer = TypedParquetSegmentWriter::create_polymarket_orderbook_with_zstd_level(
        &case_root,
        "fixture",
        case.schema_version(),
        ZSTD_LEVEL,
    )?;
    let event_rows = match case {
        StorageCase::V2 => {
            let rows = legacy_rows(&deliveries)?;
            let counts = count_legacy_rows(&rows);
            writer.write_polymarket_orderbook_rows(&rows)?;
            counts
        }
        StorageCase::V3 => {
            let messages = v3_messages(&deliveries)?;
            let counts = count_v3_messages(&messages);
            let mut sequence = PolymarketOrderbookV3WriteSequenceState::default();
            writer.write_polymarket_orderbook_v3_messages(&messages, &mut sequence)?;
            let retry = messages
                .iter()
                .find(|message| {
                    message.envelope.collector_admission_sequence == retry.source_delivery_id
                })
                .ok_or_else(|| invalid_data("v3 retry source delivery is missing"))?
                .clone();
            require(
                writer
                    .write_polymarket_orderbook_v3_messages(
                        std::slice::from_ref(&retry),
                        &mut sequence,
                    )
                    .is_err(),
                "internal retry unexpectedly created a second admission",
            )?;
            counts
        }
    };
    let finalize_started = Instant::now();
    let published = writer.finalize()?;
    let finalize_wall_us = elapsed_us(finalize_started);
    let wall_us = elapsed_us(started);
    let after = process_usage()?;
    let (row_groups, columns, compression, footer_rows) = parquet_facts(&published.final_path)?;
    require(
        footer_rows == published.row_count,
        "writer/footer row count drift",
    )?;

    Ok(WriteReport {
        fixture_digest_sha256: digest,
        logical_messages,
        persisted_rows: published.row_count,
        event_rows,
        schema_version: published.schema_version,
        compression,
        row_group_max_rows: PARQUET_ROW_GROUP_MAX_ROWS,
        row_group_max_logical_bytes: PARQUET_ROW_GROUP_MAX_BYTES,
        row_groups,
        column_chunks: columns,
        finalized_bytes: fs::metadata(&published.final_path)?.len(),
        finalized_sha256: published.sha256,
        write_finalize_wall_us: wall_us,
        finalize_wall_us,
        process_user_cpu_us: after.user_us.saturating_sub(before.user_us),
        process_system_cpu_us: after.system_us.saturating_sub(before.system_us),
        logical_messages_per_second: rate(logical_messages, wall_us),
        peak_rss_kib: after.peak_rss_kib,
    })
}

fn scan_case(_case: StorageCase, path: &Path) -> Result<ScanReport> {
    let file = File::open(path)?;
    // The kernel may ignore this request. The report deliberately describes it as advisory.
    let advice_code =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    drop(file);
    let before = process_usage()?;
    let started = Instant::now();
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let mut rows = 0_u64;
    for batch in builder.with_batch_size(SCAN_BATCH_ROWS).build()? {
        rows = rows.saturating_add(u64::try_from(batch?.num_rows())?);
    }
    let wall_us = elapsed_us(started);
    let after = process_usage()?;
    Ok(ScanReport {
        scanned_rows: rows,
        cache_drop_advice: if advice_code == 0 {
            "requested_successfully".to_owned()
        } else {
            format!("request_failed_errno_{advice_code}")
        },
        scan_wall_us: wall_us,
        scan_user_cpu_us: after.user_us.saturating_sub(before.user_us),
        scan_system_cpu_us: after.system_us.saturating_sub(before.system_us),
        scan_rows_per_second: rate(rows, wall_us),
        peak_rss_kib: after.peak_rss_kib,
    })
}

fn logical_fixture() -> Result<(Vec<LogicalDelivery>, RetryAttempt)> {
    let mut deliveries = Vec::with_capacity(LOGICAL_MESSAGES);
    for index in 0..PRICE_CHANGE_MESSAGES {
        deliveries.push(price_change_delivery(index)?);
    }
    for index in 0..BOOK_MESSAGES {
        deliveries.push(book_delivery(index)?);
    }
    for index in 0..BEST_BID_ASK_MESSAGES {
        deliveries.push(best_bid_ask_delivery(index)?);
    }
    for index in 0..LAST_TRADE_PRICE_MESSAGES {
        deliveries.push(last_trade_price_delivery(index)?);
    }
    for index in 0..TICK_SIZE_CHANGE_MESSAGES {
        deliveries.push(tick_size_change_delivery(index)?);
    }
    for index in 0..NEW_MARKET_MESSAGES {
        deliveries.push(new_market_delivery(index)?);
    }
    for index in 0..MARKET_RESOLVED_MESSAGES {
        deliveries.push(market_resolved_delivery(index)?);
    }
    for (index, delivery) in deliveries.iter_mut().enumerate() {
        delivery.logical_delivery_id = u64::try_from(index + 1)?;
    }
    require(deliveries.len() == LOGICAL_MESSAGES, "fixture length drift")?;
    Ok((
        deliveries,
        RetryAttempt {
            source_delivery_id: 1,
        },
    ))
}

fn price_change_delivery(index: usize) -> Result<LogicalDelivery> {
    let entry_count = 1 + index % 3;
    let asset_ids = ["101", "102", "999"];
    let changes = (0..entry_count)
        .map(|entry| {
            json!({
                "asset_id": asset_ids[entry],
                "price": format!("0.{:04}", 4_000 + (index + entry) % 1_000),
                "size": format!("{}", 10 + (index + entry) % 90),
                "side": if entry % 2 == 0 { "BUY" } else { "SELL" },
                "hash": format!("price-change-hash-{index:06}-{entry}"),
                "best_bid": "0.49",
                "best_ask": "0.51"
            })
        })
        .collect::<Vec<_>>();
    delivery(
        "price_change",
        json!({
            "event_type": "price_change",
            "market": MARKET,
            "timestamp": (BASE_TIME_MS + i64::try_from(index)?).to_string(),
            "price_changes": changes
        }),
        ["101", "102"],
        None,
    )
}

fn book_delivery(index: usize) -> Result<LogicalDelivery> {
    let duplicate = index >= BOOK_MESSAGES - 2;
    let value_index = if duplicate { BOOK_MESSAGES - 2 } else { index };
    delivery(
        "book",
        json!({
            "event_type": "book",
            "market": MARKET,
            "asset_id": "101",
            "timestamp": (BASE_TIME_MS + 20_000 + i64::try_from(value_index)?).to_string(),
            "bids": [
                {"price": "0.49", "size": format!("{}", 20 + value_index % 50)},
                {"price": "0.48", "size": format!("{}", 10 + value_index % 25)}
            ],
            "asks": [
                {"price": "0.51", "size": format!("{}", 15 + value_index % 40)},
                {"price": "0.52", "size": format!("{}", 8 + value_index % 20)}
            ],
            "last_trade_price": "0.50",
            "hash": format!("book-hash-{value_index:06}")
        }),
        ["101"],
        duplicate.then_some("identical-book-platform-delivery"),
    )
}

fn best_bid_ask_delivery(index: usize) -> Result<LogicalDelivery> {
    delivery(
        "best_bid_ask",
        json!({
            "event_type": "best_bid_ask",
            "market": MARKET,
            "asset_id": "101",
            "best_bid": format!("0.{:04}", 4_800 + index % 100),
            "best_ask": format!("0.{:04}", 5_100 + index % 100),
            "spread": "0.03",
            "timestamp": (BASE_TIME_MS + 30_000 + i64::try_from(index)?).to_string()
        }),
        ["101"],
        None,
    )
}

fn last_trade_price_delivery(index: usize) -> Result<LogicalDelivery> {
    delivery(
        "last_trade_price",
        json!({
            "event_type": "last_trade_price",
            "market": MARKET,
            "asset_id": "101",
            "price": format!("0.{:04}", 4_900 + index % 100),
            "side": if index.is_multiple_of(2) { "BUY" } else { "SELL" },
            "size": format!("{}", 5 + index % 30),
            "fee_rate_bps": "0",
            "timestamp": (BASE_TIME_MS + 40_000 + i64::try_from(index)?).to_string()
        }),
        ["101"],
        None,
    )
}

fn tick_size_change_delivery(index: usize) -> Result<LogicalDelivery> {
    delivery(
        "tick_size_change",
        json!({
            "event_type": "tick_size_change",
            "market": MARKET,
            "asset_id": "101",
            "old_tick_size": "0.01",
            "new_tick_size": if index.is_multiple_of(2) { "0.001" } else { "0.0001" },
            "timestamp": (BASE_TIME_MS + 50_000 + i64::try_from(index)?).to_string()
        }),
        ["101"],
        None,
    )
}

fn new_market_delivery(index: usize) -> Result<LogicalDelivery> {
    delivery(
        "new_market",
        json!({
            "event_type": "new_market",
            "id": format!("clob-market-{index}"),
            "question": format!("Will BTC finish up in fixture {index}?"),
            "market": MARKET,
            "slug": SLUG,
            "description": "deterministic storage fixture",
            "assets_ids": ["101", "102"],
            "outcomes": ["Up", "Down"],
            "event_message": {
                "id": format!("event-{index}"),
                "ticker": "BTC",
                "slug": "btc-updown",
                "title": "BTC Up or Down",
                "description": "five minute fixture"
            },
            "timestamp": (BASE_TIME_MS + 60_000 + i64::try_from(index)?).to_string()
        }),
        ["101", "102"],
        None,
    )
}

fn market_resolved_delivery(index: usize) -> Result<LogicalDelivery> {
    delivery(
        "market_resolved",
        json!({
            "event_type": "market_resolved",
            "id": format!("clob-market-{index}"),
            "question": format!("Will BTC finish up in fixture {index}?"),
            "market": MARKET,
            "slug": SLUG,
            "description": "deterministic storage fixture",
            "assets_ids": ["101", "102"],
            "outcomes": ["Up", "Down"],
            "winning_asset_id": if index.is_multiple_of(2) { "101" } else { "102" },
            "winning_outcome": if index.is_multiple_of(2) { "Up" } else { "Down" },
            "event_message": {
                "id": format!("event-{index}"),
                "ticker": "BTC",
                "slug": "btc-updown",
                "title": "BTC Up or Down",
                "description": "five minute fixture"
            },
            "timestamp": (BASE_TIME_MS + 70_000 + i64::try_from(index)?).to_string()
        }),
        ["101", "102"],
        None,
    )
}

fn delivery<const N: usize>(
    event_type: &'static str,
    value: serde_json::Value,
    matched_assets: [&str; N],
    platform_duplicate_group: Option<&'static str>,
) -> Result<LogicalDelivery> {
    Ok(LogicalDelivery {
        logical_delivery_id: 0,
        event_type,
        raw_sdk_json: serde_json::to_string(&value)?,
        slug: SLUG.to_owned(),
        matched_assets: matched_assets.into_iter().map(ToOwned::to_owned).collect(),
        platform_duplicate_group,
    })
}

fn validate_fixture(deliveries: &[LogicalDelivery], retry: RetryAttempt) -> Result<FixtureReport> {
    let mut variants = BTreeMap::<String, u64>::new();
    let mut retained_price_change_entries = 0_u64;
    let mut duplicate_raw = None::<&str>;
    let mut duplicate_delivery_ids = BTreeSet::<u64>::new();
    for delivery in deliveries {
        *variants.entry(delivery.event_type.to_owned()).or_default() += 1;
        let message = parse_delivery(delivery)?;
        if let WsMessage::PriceChange(event) = &message {
            retained_price_change_entries =
                retained_price_change_entries.saturating_add(u64::try_from(
                    event
                        .price_changes
                        .iter()
                        .filter(|change| {
                            delivery
                                .matched_assets
                                .contains(&change.asset_id.to_string())
                        })
                        .count(),
                )?);
        }
        if delivery.platform_duplicate_group.is_some() {
            duplicate_delivery_ids.insert(delivery.logical_delivery_id);
            if let Some(expected) = duplicate_raw {
                require(
                    expected == delivery.raw_sdk_json,
                    "platform duplicate payload drift",
                )?;
            } else {
                duplicate_raw = Some(&delivery.raw_sdk_json);
            }
        }
    }
    for required_variant in [
        "book",
        "price_change",
        "tick_size_change",
        "last_trade_price",
        "best_bid_ask",
        "new_market",
        "market_resolved",
    ] {
        require(
            variants.get(required_variant).copied().unwrap_or(0) > 0,
            "variant missing",
        )?;
    }
    let duplicate_deliveries = u64::try_from(duplicate_delivery_ids.len())?;
    require(duplicate_deliveries == 2, "duplicate fixture count drift")?;
    require(
        deliveries
            .iter()
            .filter(|delivery| delivery.logical_delivery_id == retry.source_delivery_id)
            .count()
            == 1,
        "retry source delivery is not unique",
    )?;
    let non_price_rows = u64::try_from(deliveries.len() - PRICE_CHANGE_MESSAGES)?;
    Ok(FixtureReport {
        version: FIXTURE_VERSION,
        logical_messages: u64::try_from(deliveries.len())?,
        expected_v2_rows: retained_price_change_entries.saturating_add(non_price_rows),
        retained_price_change_entries,
        variant_messages: variants,
        identical_platform_deliveries: duplicate_deliveries,
        distinct_platform_admission_keys: u64::try_from(duplicate_delivery_ids.len())?,
        internal_retry_source_delivery_id: retry.source_delivery_id,
    })
}

fn v3_messages(deliveries: &[LogicalDelivery]) -> Result<Vec<PolymarketOrderbookV3Message>> {
    deliveries
        .iter()
        .map(|delivery| {
            let message = parse_delivery(delivery)?;
            let mut projection =
                PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&message)?
                    .ok_or_else(|| invalid_data("fixture variant is not projectable"))?;
            if let PolymarketOrderbookV3Event::PriceChange(price_change) = &mut projection.event {
                price_change
                    .price_changes
                    .retain(|change| delivery.matched_assets.contains(&change.asset_id));
            }
            projection.with_slug(Some(delivery.slug.clone())).admit(
                PolymarketOrderbookV3Admission {
                    timestamp_received: BASE_TIME_MS + i64::try_from(delivery.logical_delivery_id)?,
                    collector_host_id: "storage-probe-host".to_owned(),
                    collector_region: "local-dev".to_owned(),
                    source_endpoint: "synthetic://polymarket-market".to_owned(),
                    source_sequence_epoch_id: "storage-probe-epoch-v1".to_owned(),
                    collector_admission_sequence: delivery.logical_delivery_id,
                    collector_admission_time_utc_ms: BASE_TIME_MS
                        + i64::try_from(delivery.logical_delivery_id)?
                        + 1,
                },
            )
        })
        .collect()
}

fn legacy_rows(deliveries: &[LogicalDelivery]) -> Result<Vec<PolymarketOrderbookRow>> {
    let mut rows = Vec::new();
    for delivery in deliveries {
        let message = parse_delivery(delivery)?;
        let context = legacy_context(delivery);
        let mut projected = match message {
            WsMessage::Book(event) => {
                require_target(&delivery.matched_assets, &event.asset_id.to_string())?;
                vec![PolymarketOrderbookRow::from_sdk_book(&event, context).map_err(invalid_data)?]
            }
            WsMessage::PriceChange(mut event) => {
                event.price_changes.retain(|change| {
                    delivery
                        .matched_assets
                        .contains(&change.asset_id.to_string())
                });
                PolymarketOrderbookRow::from_sdk_price_change(&event, context)
                    .map_err(invalid_data)?
            }
            WsMessage::TickSizeChange(event) => {
                require_target(&delivery.matched_assets, &event.asset_id.to_string())?;
                vec![PolymarketOrderbookRow::tick_size_change(
                    &event.market.to_string(),
                    &event.asset_id.to_string(),
                    event
                        .timestamp
                        .ok_or_else(|| invalid_data("tick timestamp missing"))?,
                    &event
                        .old_tick_size
                        .ok_or_else(|| invalid_data("old tick size missing"))?
                        .to_string(),
                    &event.new_tick_size.to_string(),
                    context,
                )]
            }
            WsMessage::LastTradePrice(event) => {
                require_target(&delivery.matched_assets, &event.asset_id.to_string())?;
                vec![PolymarketOrderbookRow::last_trade_price(
                    &event.market.to_string(),
                    &event.asset_id.to_string(),
                    event
                        .timestamp
                        .ok_or_else(|| invalid_data("trade timestamp missing"))?,
                    &event.price.to_string(),
                    event
                        .fee_rate_bps
                        .as_ref()
                        .map(ToString::to_string)
                        .as_deref(),
                    event.side.as_ref().map(ToString::to_string).as_deref(),
                    event.size.as_ref().map(ToString::to_string).as_deref(),
                    None,
                    context,
                )]
            }
            WsMessage::BestBidAsk(event) => {
                require_target(&delivery.matched_assets, &event.asset_id.to_string())?;
                let mut row = legacy_base(
                    "best_bid_ask",
                    event.market.to_string(),
                    Some(event.asset_id.to_string()),
                    event.timestamp,
                    context,
                );
                row.best_bid = event.best_bid.map(|value| value.to_string());
                row.best_ask = event.best_ask.map(|value| value.to_string());
                vec![row]
            }
            WsMessage::NewMarket(event) => {
                require_any_target(
                    &delivery.matched_assets,
                    event.asset_ids.iter().map(ToString::to_string),
                )?;
                let mut row = legacy_base(
                    "new_market",
                    event.market.to_string(),
                    None,
                    event.timestamp,
                    context,
                );
                row.subscribed_asset_ids_for_market = Some(serde_json::to_string(
                    &event
                        .asset_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )?);
                vec![row]
            }
            WsMessage::MarketResolved(event) => {
                let asset_ids = event
                    .asset_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                require_any_target(&delivery.matched_assets, asset_ids.iter().cloned())?;
                vec![PolymarketOrderbookRow::market_resolved(
                    &event.market.to_string(),
                    event
                        .timestamp
                        .ok_or_else(|| invalid_data("resolved timestamp missing"))?,
                    &event
                        .winning_asset_id
                        .ok_or_else(|| invalid_data("winning asset missing"))?
                        .to_string(),
                    event
                        .winning_outcome
                        .as_deref()
                        .ok_or_else(|| invalid_data("winning outcome missing"))?,
                    None,
                    &asset_ids,
                    context,
                )]
            }
            _ => return Err(invalid_data("unexpected fixture SDK variant")),
        };
        for row in &projected {
            require(
                row.validate().is_complete(),
                "legacy fixture row failed v2 validation",
            )?;
        }
        rows.append(&mut projected);
    }
    Ok(rows)
}

fn legacy_context(delivery: &LogicalDelivery) -> PolymarketOrderbookContext {
    PolymarketOrderbookContext {
        timestamp_received_ms: BASE_TIME_MS
            + i64::try_from(delivery.logical_delivery_id).expect("fixture id fits i64"),
        slug: Some(delivery.slug.clone()),
        source_native_payload_json: delivery.raw_sdk_json.clone(),
        collector_host_id: "storage-probe-host".to_owned(),
        collector_region: "local-dev".to_owned(),
        source_endpoint: "synthetic://polymarket-market".to_owned(),
        schema_version: "polymarket_orderbook_v2".to_owned(),
    }
}

fn legacy_base(
    event_type: &str,
    market: String,
    asset_id: Option<String>,
    timestamp: Option<i64>,
    context: PolymarketOrderbookContext,
) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: context.timestamp_received_ms,
        timestamp,
        market,
        event_type: event_type.to_owned(),
        source_native_payload_json: context.source_native_payload_json,
        asset_id,
        bids: None,
        asks: None,
        price: None,
        size: None,
        side: None,
        best_bid: None,
        best_ask: None,
        hash: None,
        fee_rate_bps: None,
        transaction_hash: None,
        old_tick_size: None,
        new_tick_size: None,
        winning_asset_id: None,
        winning_outcome: None,
        close_reason_if_present: None,
        subscribed_asset_ids_for_market: None,
        resolved_event_asset_ids: None,
        slug: context.slug,
        collector_host_id: context.collector_host_id,
        collector_region: context.collector_region,
        source_endpoint: context.source_endpoint,
        schema_version: context.schema_version,
    }
}

fn parse_delivery(delivery: &LogicalDelivery) -> Result<WsMessage> {
    let mut parsed = parse_if_interested(delivery.raw_sdk_json.as_bytes(), &MessageInterest::ALL)?;
    require(
        parsed.len() == 1,
        "fixture must decode to exactly one SDK message",
    )?;
    Ok(parsed.pop().expect("length checked"))
}

fn fixture_digest(deliveries: &[LogicalDelivery], retry: RetryAttempt) -> String {
    let mut digest = Sha256::new();
    digest.update(FIXTURE_VERSION.as_bytes());
    for delivery in deliveries {
        digest.update(delivery.logical_delivery_id.to_le_bytes());
        digest.update(
            u64::try_from(delivery.raw_sdk_json.len())
                .unwrap()
                .to_le_bytes(),
        );
        digest.update(delivery.raw_sdk_json.as_bytes());
        digest.update(delivery.slug.as_bytes());
        for asset_id in &delivery.matched_assets {
            digest.update(asset_id.as_bytes());
            digest.update([0]);
        }
    }
    digest.update(b"internal_retry_source_delivery_id");
    digest.update(retry.source_delivery_id.to_le_bytes());
    format!("{:x}", digest.finalize())
}

fn count_legacy_rows(rows: &[PolymarketOrderbookRow]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.event_type.clone()).or_default() += 1;
    }
    counts
}

fn count_v3_messages(messages: &[PolymarketOrderbookV3Message]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for message in messages {
        *counts.entry(message.event_type().to_owned()).or_default() += 1;
    }
    counts
}

fn parquet_facts(path: &Path) -> Result<(u64, BTreeMap<String, ColumnBytes>, String, u64)> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let metadata = reader.metadata();
    let mut columns = BTreeMap::<String, ColumnBytes>::new();
    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let entry = columns
                .entry(column.column_path().string())
                .or_insert(ColumnBytes {
                    chunks: 0,
                    compressed_bytes: 0,
                    uncompressed_bytes: 0,
                });
            entry.chunks = entry.chunks.saturating_add(1);
            entry.compressed_bytes = entry
                .compressed_bytes
                .saturating_add(u64::try_from(column.compressed_size())?);
            entry.uncompressed_bytes = entry
                .uncompressed_bytes
                .saturating_add(u64::try_from(column.uncompressed_size())?);
        }
    }
    let compression = metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|values| {
            values
                .iter()
                .find(|value| value.key == "compression")
                .and_then(|value| value.value.clone())
        })
        .ok_or_else(|| invalid_data("compression metadata missing"))?;
    Ok((
        u64::try_from(metadata.num_row_groups())?,
        columns,
        compression,
        u64::try_from(metadata.file_metadata().num_rows())?,
    ))
}

fn process_usage() -> Result<ProcessUsage> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage on a zero return code.
    let code = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if code != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: the successful getrusage call initialized the value.
    let usage = unsafe { usage.assume_init() };
    Ok(ProcessUsage {
        user_us: timeval_us(usage.ru_utime),
        system_us: timeval_us(usage.ru_stime),
        peak_rss_kib: u64::try_from(usage.ru_maxrss).unwrap_or(0),
    })
}

fn timeval_us(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(value.tv_usec).unwrap_or(0))
}

fn child_json<T>(executable: &Path, args: &[&str]) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let output = Command::new(executable).args(args).output()?;
    require_child_success(&output)?;
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn require_child_success(output: &Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "probe child failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    ))
    .into())
}

fn environment_report() -> EnvironmentReport {
    EnvironmentReport {
        os: env::consts::OS,
        architecture: env::consts::ARCH,
        kernel_release: read_trimmed("/proc/sys/kernel/osrelease"),
        cpu_model: fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|content| {
                content.lines().find_map(|line| {
                    line.strip_prefix("model name")
                        .and_then(|rest| rest.split_once(':'))
                        .map(|(_, value)| value.trim().to_owned())
                })
            })
            .unwrap_or_else(|| "unavailable".to_owned()),
        logical_cpus: std::thread::available_parallelism().map_or(0, usize::from),
        memory_total_kib: fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|content| {
                content.lines().find_map(|line| {
                    line.strip_prefix("MemTotal:")
                        .and_then(|value| value.split_whitespace().next())
                        .and_then(|value| value.parse().ok())
                })
            })
            .unwrap_or(0),
        rustc: command_version("rustc"),
        cargo: command_version("cargo"),
    }
}

fn command_version(command: &str) -> String {
    Command::new(command)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn read_trimmed(path: &str) -> String {
    fs::read_to_string(path)
        .map(|value| value.trim().to_owned())
        .unwrap_or_else(|_| "unavailable".to_owned())
}

fn probe_root() -> Result<PathBuf> {
    let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/tmp");
    fs::create_dir_all(&parent)?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = parent.join(format!(
        "data-collect-polymarket-storage-ab-probe-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&root)?;
    Ok(root)
}

fn case_path(root: &Path, case: StorageCase) -> PathBuf {
    root.join(case.name()).join("fixture.parquet")
}

fn required_arg<'a>(args: &'a [String], index: usize, name: &str) -> Result<&'a str> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| invalid_input(format!("missing {name} argument")))
}

fn required_utf8(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| invalid_input("probe path is not UTF-8"))
}

fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid_data(message))
    }
}

fn require_target(targets: &BTreeSet<String>, asset_id: &str) -> Result<()> {
    require(
        targets.contains(asset_id),
        "fixture asset is not target-admitted",
    )
}

fn require_any_target<I>(targets: &BTreeSet<String>, asset_ids: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    require(
        asset_ids
            .into_iter()
            .any(|asset_id| targets.contains(&asset_id)),
        "fixture market has no target-admitted asset",
    )
}

fn has_field(report: &WriteReport, field: &str) -> bool {
    report
        .column_chunks
        .keys()
        .any(|path| path.split('.').any(|component| component == field))
}

fn column_bytes(report: &WriteReport, field: &str) -> u64 {
    report
        .column_chunks
        .iter()
        .filter(|(path, _)| path.split('.').any(|component| component == field))
        .map(|(_, bytes)| bytes.compressed_bytes)
        .sum()
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn rate(units: u64, elapsed_us: u64) -> f64 {
    if elapsed_us == 0 {
        return 0.0;
    }
    units as f64 * 1_000_000.0 / elapsed_us as f64
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 * 100.0 / denominator as f64
}

fn invalid_input(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn invalid_data(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}
