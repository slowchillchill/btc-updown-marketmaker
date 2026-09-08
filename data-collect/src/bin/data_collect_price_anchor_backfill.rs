//! 固定来源日期的官方锚点补查与独立补充产物；不运行 DPT 或改写原采集日。

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use data_collect::sha256_file;
use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use polymarket_client_sdk_v2::gamma::Client;
use polymarket_client_sdk_v2::gamma::types::request::WebsiteCryptoPriceRequest;
use polymarket_client_sdk_v2::gamma::types::response::{
    Event, Market, ResponseCapture, WebsiteCryptoPriceResponse,
};
use polymarket_client_sdk_v2::types::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep_until, timeout};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const SCHEMA: &str = "historical_price_anchor_repair_v1";
const POLICY: &str = "price_anchor_historical_repair_v1";
const PARQUET_NAME: &str = "historical_price_anchor_repair_v1.parquet";
const QUERY_INTERVAL: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug)]
struct Config {
    action: String,
    evidence_root: PathBuf,
    dataset_root: PathBuf,
    output_dir: PathBuf,
    additional_queries: Option<PathBuf>,
    build_commit: String,
    build_state: String,
}

impl Config {
    fn from_args() -> Result<Self> {
        let mut args = std::env::args().skip(1);
        let action = args
            .next()
            .ok_or_else(|| invalid("需要 build 或 query-remaining 子命令"))?;
        if action != "build" && action != "query-remaining" {
            return Err(invalid("子命令须为 build 或 query-remaining"));
        }
        let mut values = BTreeMap::new();
        while let Some(key) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| invalid(format!("{key} 缺少参数值")))?;
            match key.as_str() {
                "--evidence-root"
                | "--dataset-root"
                | "--output-dir"
                | "--additional-queries"
                | "--build-commit"
                | "--build-state" => {
                    values.insert(key, value);
                }
                _ => return Err(invalid(format!("未知参数 {key}"))),
            }
        }
        let mut required = |name: &str| {
            values
                .remove(name)
                .ok_or_else(|| invalid(format!("缺少 {name}")))
        };
        let evidence_root = PathBuf::from(required("--evidence-root")?);
        let dataset_root = PathBuf::from(required("--dataset-root")?);
        let output_dir = PathBuf::from(required("--output-dir")?);
        let build_commit = required("--build-commit")?;
        let build_state = required("--build-state")?;
        Ok(Self {
            action,
            evidence_root,
            dataset_root,
            output_dir,
            build_commit,
            build_state,
            additional_queries: values.remove("--additional-queries").map(PathBuf::from),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileBinding {
    path: String,
    sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Generator {
    build_commit: String,
    binary_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceDateSet {
    first_utc_day: String,
    last_utc_day: String,
    member_slugs: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestBinding {
    path: String,
    sha256: String,
    utc_day: String,
    role: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MetadataFileBinding {
    path: String,
    sha256: String,
    rows: u64,
    bytes: u64,
    manifest_path: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OutputBinding {
    path: String,
    sha256: String,
    bytes: u64,
    rows: u64,
    schema_version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HttpResponseEvidenceV1 {
    origin: String,
    request_url: String,
    received_at_utc: String,
    received_at_utc_ms: i64,
    receipt_observation: String,
    response_body_sha256: String,
    response_body_path: String,
    original_response_file: String,
    http_status_code: Option<u16>,
    response_url: Option<String>,
}

impl HttpResponseEvidenceV1 {
    fn id(&self) -> Result<String> {
        Ok(hash_bytes(&serde_json::to_vec(&[
            "historical_price_anchor_http_response_v1",
            &self.origin,
            &self.request_url,
            &self.received_at_utc,
            &self.response_body_sha256,
        ])?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HistoricalIdentityRowEvidenceV1 {
    manifest_path: String,
    manifest_sha256: String,
    parquet_path: String,
    parquet_sha256: String,
    row_index: u64,
    schema_version: String,
    metadata_row_kind: String,
    row_received_at_utc_ms: i64,
    fixed_identity_verified_at_utc_ms: i64,
    source_evidence_origin: Option<String>,
    source_endpoint: Option<String>,
    source_evidence_received_at_utc_ms: Option<i64>,
    source_evidence_raw_sha256: Option<String>,
    collector_admission_sequence: Option<u64>,
    collector_admission_time_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WebsiteQueryAttemptV1 {
    slug: String,
    condition_id: String,
    clob_market_id: String,
    parameters_json: String,
    attempt_started_at_utc: String,
    attempt_completed_at_utc: String,
    result: String,
    response_group_id: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryRun {
    schema_version: String,
    generator: Generator,
    build_state: String,
    input_evidence_files: Vec<FileBinding>,
    planned_slugs: Vec<String>,
    unattempted_slugs: Vec<String>,
    stopped_on_http_429: bool,
    response_groups: BTreeMap<String, HttpResponseEvidenceV1>,
    network_attempts: Vec<WebsiteQueryAttemptV1>,
}

#[derive(Debug, Serialize)]
struct Remaining {
    slug: String,
    condition_id: String,
    clob_market_id: String,
    reason: String,
    investigation_response_group_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RepairManifest {
    schema_version: String,
    repair_policy_version: String,
    created_at_utc: String,
    generator: Generator,
    source_date_sets: BTreeMap<String, SourceDateSet>,
    input_manifests: Vec<ManifestBinding>,
    input_metadata_files: Vec<MetadataFileBinding>,
    input_evidence_files: Vec<FileBinding>,
    response_groups: BTreeMap<String, HttpResponseEvidenceV1>,
    identity_evidence: BTreeMap<String, HistoricalIdentityRowEvidenceV1>,
    outputs: Vec<OutputBinding>,
    remaining: Vec<Remaining>,
    network_attempts: Vec<WebsiteQueryAttemptV1>,
}

#[derive(Debug, Deserialize)]
struct IdentityFields {
    condition_id: String,
    clob_market_id: String,
    asset_ids: String,
    market_start_utc: String,
    market_end_utc: String,
}
#[derive(Debug, Deserialize)]
struct LocalIdentity {
    identity: IdentityFields,
    first_fixed_identity_verified_at_utc: String,
}
#[derive(Clone, Debug)]
struct Identity {
    condition_id: String,
    clob_market_id: String,
    asset_ids_json: String,
    start: i64,
    end: i64,
    ready: i64,
}
#[derive(Debug, Deserialize)]
struct LegacyQuery {
    url: String,
    received_at: String,
    response_file: String,
    sha256: String,
    endpoint: Option<String>,
    slug: Option<String>,
}
#[derive(Debug, Deserialize)]
struct UiSelection {
    slug: String,
    url: String,
    received_at: String,
    response_file: String,
    sha256: String,
}
#[derive(Debug, Deserialize)]
struct Combined {
    ui_confirmed_values: Vec<UiSelection>,
}
#[derive(Debug, Deserialize)]
struct UiStatus {
    received_at: String,
    response_file: String,
    sha256: String,
    http_status: String,
    body: Option<String>,
}
#[derive(Debug, Deserialize)]
struct Inventory {
    per_day: BTreeMap<String, InventoryDay>,
}
#[derive(Debug, Deserialize)]
struct InventoryDay {
    group: String,
}
#[derive(Debug, Deserialize)]
struct DailyManifest {
    utc_day: String,
    files: Vec<DailyFile>,
}
#[derive(Debug, Deserialize)]
struct DailyFile {
    path: String,
    sha256: String,
    rows: u64,
    bytes: u64,
    source_kind: String,
    utc_hour: String,
}

#[derive(Clone, Debug)]
struct SelectedAnchor {
    source: String,
    raw: String,
    group: String,
    member: u32,
    value_path: String,
}
struct Sources {
    identities: BTreeMap<String, Identity>,
    date_sets: BTreeMap<String, SourceDateSet>,
    inventory: Inventory,
    evidence_files: Vec<FileBinding>,
    responses: BTreeMap<String, HttpResponseEvidenceV1>,
    anchors: BTreeMap<String, SelectedAnchor>,
    website_requests: BTreeMap<String, WebsiteCryptoPriceRequest>,
    outside_history: BTreeSet<String>,
    investigations: BTreeMap<String, Vec<String>>,
    network_attempts: Vec<WebsiteQueryAttemptV1>,
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}
fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn time_ms(raw: &str) -> Result<i64> {
    Ok(DateTime::parse_from_rfc3339(raw)?.timestamp_millis())
}
fn now() -> String {
    Utc::now().to_rfc3339()
}
fn outside_observed_history_window(market_start_ms: i64, limit_observed_at_ms: i64) -> bool {
    market_start_ms < limit_observed_at_ms - 30 * 24 * 60 * 60 * 1000
}
fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path)?)
        .map_err(|e| invalid(format!("{}: {e}", path.display())))
}
fn verified_bytes(path: &Path, expected: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path)?;
    if hash_bytes(&bytes) != expected {
        return Err(invalid(format!("{}: 原字节 SHA 不匹配", path.display())));
    }
    Ok(bytes)
}
fn decode<T: DeserializeOwned>(bytes: &[u8], path: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| invalid(format!("SDK 原字节解析失败 {path}: {error}")))
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn write_json_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_new(path, &bytes)
}
fn bind(path: &Path) -> Result<FileBinding> {
    Ok(FileBinding {
        path: path_text(path),
        sha256: sha256_file(path)?,
    })
}
fn generator(config: &Config) -> Result<Generator> {
    Ok(Generator {
        build_commit: config.build_commit.clone(),
        binary_sha256: sha256_file(&std::env::current_exe()?)?,
    })
}

fn legacy_response(
    query: &LegacyQuery,
    origin: &str,
    status: Option<u16>,
) -> Result<HttpResponseEvidenceV1> {
    Ok(HttpResponseEvidenceV1 {
        origin: origin.to_owned(),
        request_url: query.url.clone(),
        received_at_utc: query.received_at.clone(),
        received_at_utc_ms: time_ms(&query.received_at)?,
        receipt_observation: "curl_completed".to_owned(),
        response_body_sha256: query.sha256.clone(),
        response_body_path: format!("responses/{}.json", query.sha256),
        original_response_file: query.response_file.clone(),
        http_status_code: status,
        response_url: None,
    })
}

fn market_for_identity<'a>(
    event: &'a Event,
    slug: &str,
    identity: &Identity,
) -> Result<&'a Market> {
    if event.slug.as_deref() != Some(slug) {
        return Err(invalid(format!("{slug}: Gamma 事件身份不符")));
    }
    let market = event
        .markets
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|market| market.id == identity.clob_market_id)
        .ok_or_else(|| invalid(format!("{slug}: Gamma 缺少关联市场")))?;
    match_market(market, slug, identity)?;
    Ok(market)
}
fn match_market(market: &Market, slug: &str, identity: &Identity) -> Result<()> {
    let ids = market
        .clob_token_ids
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let expected: Vec<String> = serde_json::from_str(&identity.asset_ids_json)?;
    if market.slug.as_deref() != Some(slug)
        || market.id != identity.clob_market_id
        || market.condition_id.map(|id| format!("{id:#x}")).as_deref()
            != Some(&identity.condition_id)
        || ids != expected
    {
        return Err(invalid(format!(
            "{slug}: Gamma 市场/condition/有序资产与历史身份不符"
        )));
    }
    Ok(())
}

fn load_sources(config: &Config) -> Result<Sources> {
    let root = &config.evidence_root;
    let diagnostics: Vec<UiStatus> = read_json(&root.join("ui-diagnostic-summary.json"))?;
    let history_limit_observed_at = diagnostics
        .iter()
        .find(|row| {
            row.http_status == "400"
                && row.body.as_deref()
                    == Some(r#"{"error":"Timestamp too old for Chainlink API (max 30 days)"}"#)
        })
        .map(|row| time_ms(&row.received_at))
        .transpose()?;
    let evidence_names = [
        "fixed-market-local-identities.json",
        "fixed-date-market-slugs.json",
        "fixed-date-inventory.json",
        "fixed-market-probe-queries.json",
        "missing-detail-probe-records.json",
        "response-file-hashes.json",
        "combined-anchor-evidence-summary.json",
        "official-ui-bounded-results.json",
        "official-ui-slow-results.json",
        "ui-diagnostic-summary.json",
    ];
    let evidence_files = evidence_names
        .iter()
        .map(|name| bind(&root.join(name)))
        .collect::<Result<Vec<_>>>()?;
    let local: BTreeMap<String, LocalIdentity> = read_json(&root.join(evidence_names[0]))?;
    let identities = local
        .into_iter()
        .map(|(slug, local)| {
            let value = local.identity;
            Ok((
                slug,
                Identity {
                    condition_id: value.condition_id,
                    clob_market_id: value.clob_market_id,
                    asset_ids_json: value.asset_ids,
                    start: time_ms(&value.market_start_utc)?,
                    end: time_ms(&value.market_end_utc)?,
                    ready: time_ms(&local.first_fixed_identity_verified_at_utc)?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let members: BTreeMap<String, Vec<String>> = read_json(&root.join(evidence_names[1]))?;
    let inventory: Inventory = read_json(&root.join(evidence_names[2]))?;
    let date_sets = members
        .into_iter()
        .map(|(name, member_slugs)| {
            let days = inventory
                .per_day
                .iter()
                .filter(|(_, day)| day.group == name)
                .map(|(day, _)| day.clone())
                .collect::<Vec<_>>();
            let first = days
                .first()
                .ok_or_else(|| invalid(format!("{name}: 缺少来源日期")))?
                .clone();
            Ok((
                name,
                SourceDateSet {
                    first_utc_day: first,
                    last_utc_day: days.last().unwrap().clone(),
                    member_slugs,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut sources = Sources {
        identities,
        date_sets,
        inventory,
        evidence_files,
        responses: BTreeMap::new(),
        anchors: BTreeMap::new(),
        website_requests: BTreeMap::new(),
        outside_history: BTreeSet::new(),
        investigations: BTreeMap::new(),
        network_attempts: Vec::new(),
    };
    let queries: Vec<LegacyQuery> = read_json(&root.join("fixed-market-probe-queries.json"))?;
    for query in queries {
        let bytes = verified_bytes(Path::new(&query.response_file), &query.sha256)?;
        let events: Vec<Event> = decode(&bytes, &query.response_file)?;
        let response = legacy_response(&query, "gamma_events", None)?;
        let group = response.id()?;
        for (member, event) in events.iter().enumerate() {
            let slug = event
                .slug
                .as_deref()
                .ok_or_else(|| invalid("Gamma 事件缺少 slug"))?;
            let identity = sources
                .identities
                .get(slug)
                .ok_or_else(|| invalid(format!("{slug}: 不在冻结来源身份中")))?;
            let market = market_for_identity(event, slug, identity)?;
            if market.resolution_source.as_deref()
                == Some("https://data.chain.link/streams/btc-usd")
                && history_limit_observed_at.is_some_and(|observed_at| {
                    outside_observed_history_window(identity.start, observed_at)
                })
            {
                sources.outside_history.insert(slug.to_owned());
            }
            sources
                .investigations
                .entry(slug.to_owned())
                .or_default()
                .push(group.clone());
            if let Some(raw) = event
                .event_metadata
                .as_ref()
                .and_then(|m| m.price_to_beat.as_ref())
            {
                sources.anchors.insert(
                    slug.to_owned(),
                    SelectedAnchor {
                        source: "gamma_event_metadata_price_to_beat".to_owned(),
                        raw: raw.0.clone(),
                        group: group.clone(),
                        member: member.try_into()?,
                        value_path: format!("/{member}/eventMetadata/priceToBeat"),
                    },
                );
            }
            if let Some(rule) = market.crypto_market_config.as_ref()
                && rule.asset.as_deref() == Some("btc")
                && rule.duration.as_deref() == Some("5m")
                && rule.twap_enabled == Some(true)
                && rule.twap_lookback_seconds == Some(60)
            {
                sources.website_requests.insert(
                    slug.to_owned(),
                    WebsiteCryptoPriceRequest::builder()
                        .symbol("BTC")
                        .variant("fiveminute")
                        .twap_enabled(true)
                        .twap_lookback_seconds(60)
                        .event_start_time(
                            DateTime::from_timestamp_millis(identity.start)
                                .ok_or_else(|| invalid("市场开始时间超出 SDK 范围"))?,
                        )
                        .end_date(
                            DateTime::from_timestamp_millis(identity.end)
                                .ok_or_else(|| invalid("市场结束时间超出 SDK 范围"))?,
                        )
                        .build(),
                );
            }
        }
        sources.responses.insert(group, response);
    }
    let details: Vec<LegacyQuery> = read_json(&root.join("missing-detail-probe-records.json"))?;
    for query in details {
        let bytes = verified_bytes(Path::new(&query.response_file), &query.sha256)?;
        let slug = query
            .slug
            .as_deref()
            .ok_or_else(|| invalid("Gamma 详情元记录缺少 slug"))?;
        let identity = sources
            .identities
            .get(slug)
            .ok_or_else(|| invalid("Gamma 详情没有原身份"))?;
        let origin = if query.endpoint.as_deref() == Some("events") {
            let event: Event = decode(&bytes, &query.response_file)?;
            market_for_identity(&event, slug, identity)?;
            "gamma_event_by_slug"
        } else {
            let market: Market = decode(&bytes, &query.response_file)?;
            match_market(&market, slug, identity)?;
            "gamma_market_by_slug"
        };
        let response = legacy_response(&query, origin, None)?;
        let group = response.id()?;
        sources
            .investigations
            .entry(slug.to_owned())
            .or_default()
            .push(group.clone());
        sources.responses.insert(group, response);
    }
    let mut statuses = BTreeMap::new();
    for name in [
        "official-ui-bounded-results.json",
        "official-ui-slow-results.json",
        "ui-diagnostic-summary.json",
    ] {
        let rows: Vec<UiStatus> = read_json(&root.join(name))?;
        for row in rows {
            statuses.insert(
                (row.response_file, row.sha256, row.received_at),
                row.http_status.parse::<u16>()?,
            );
        }
    }
    let combined: Combined = read_json(&root.join("combined-anchor-evidence-summary.json"))?;
    for ui in combined.ui_confirmed_values {
        let bytes = verified_bytes(Path::new(&ui.response_file), &ui.sha256)?;
        let prices: WebsiteCryptoPriceResponse = decode(&bytes, &ui.response_file)?;
        let status = statuses
            .get(&(
                ui.response_file.clone(),
                ui.sha256.clone(),
                ui.received_at.clone(),
            ))
            .copied();
        if status != Some(200) {
            return Err(invalid(format!(
                "{}: 没有对应官网实际成功状态证据",
                ui.slug
            )));
        }
        let response = legacy_response(
            &LegacyQuery {
                url: ui.url,
                received_at: ui.received_at,
                response_file: ui.response_file,
                sha256: ui.sha256,
                endpoint: None,
                slug: Some(ui.slug.clone()),
            },
            "website_crypto_price",
            status,
        )?;
        let group = response.id()?;
        if let Some(raw) = prices.open_price {
            sources
                .anchors
                .entry(ui.slug.clone())
                .or_insert(SelectedAnchor {
                    source: "official_crypto_price_open_price".to_owned(),
                    raw: raw.0,
                    group: group.clone(),
                    member: 0,
                    value_path: "/openPrice".to_owned(),
                });
        }
        sources
            .investigations
            .entry(ui.slug)
            .or_default()
            .push(group.clone());
        sources.responses.insert(group, response);
    }
    if let Some(path) = &config.additional_queries {
        import_queries(&mut sources, path)?;
    }
    Ok(sources)
}

fn import_queries(sources: &mut Sources, path: &Path) -> Result<()> {
    let run: QueryRun = read_json(path)?;
    sources.evidence_files.push(bind(path)?);
    for (group_id, response) in run.response_groups {
        verified_bytes(
            Path::new(&response.original_response_file),
            &response.response_body_sha256,
        )?;
        if response.id()? != group_id {
            return Err(invalid("新查询的响应组身份与原响应证据不符"));
        }
        sources.responses.insert(group_id, response);
    }
    for attempt in run.network_attempts {
        let identity = sources
            .identities
            .get(&attempt.slug)
            .ok_or_else(|| invalid("新查询没有原市场身份"))?;
        if identity.condition_id != attempt.condition_id
            || identity.clob_market_id != attempt.clob_market_id
        {
            return Err(invalid(format!("{}: 新查询身份与原历史不符", attempt.slug)));
        }
        if let Some(group_id) = &attempt.response_group_id {
            let response = sources
                .responses
                .get(group_id)
                .ok_or_else(|| invalid("新查询缺少原响应组"))?;
            sources
                .investigations
                .entry(attempt.slug.clone())
                .or_default()
                .push(group_id.clone());
            if attempt.result == "observed" && response.http_status_code == Some(200) {
                let bytes = verified_bytes(
                    Path::new(&response.original_response_file),
                    &response.response_body_sha256,
                )?;
                let prices: WebsiteCryptoPriceResponse =
                    decode(&bytes, &response.original_response_file)?;
                if let Some(raw) = prices.open_price {
                    sources
                        .anchors
                        .entry(attempt.slug.clone())
                        .or_insert(SelectedAnchor {
                            source: "official_crypto_price_open_price".to_owned(),
                            raw: raw.0,
                            group: group_id.clone(),
                            member: 0,
                            value_path: "/openPrice".to_owned(),
                        });
                }
            }
        }
        sources.network_attempts.push(attempt);
    }
    Ok(())
}

const IDENTITY_COLUMNS: &[&str] = &[
    "slug",
    "condition_id",
    "clob_market_id",
    "asset_ids",
    "market_start_utc",
    "market_end_utc",
    "fixed_identity_verified_at_utc",
    "schema_version",
    "metadata_row_kind",
    "timestamp_received",
    "source_evidence_origin",
    "source_endpoint",
    "source_evidence_received_at_utc",
    "source_evidence_raw_sha256",
    "collector_admission_sequence",
    "collector_admission_time_utc_ms",
];

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid(format!("原 Parquet 列缺失或类型不符: {name}")))
}
fn text_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let array = column::<StringArray>(batch, name)?;
    Ok((!array.is_null(row)).then(|| array.value(row).to_owned()))
}
fn required_text(batch: &RecordBatch, name: &str, row: usize) -> Result<String> {
    text_at(batch, name, row)?.ok_or_else(|| invalid(format!("原 Parquet 必填列为空: {name}")))
}
fn timestamp_at(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<i64>> {
    let array = column::<TimestampMillisecondArray>(batch, name)?;
    Ok((!array.is_null(row)).then(|| array.value(row)))
}
fn required_time(batch: &RecordBatch, name: &str, row: usize) -> Result<i64> {
    timestamp_at(batch, name, row)?
        .ok_or_else(|| invalid(format!("原 Parquet 必填时间为空: {name}")))
}

struct IdentityInputs {
    manifests: Vec<ManifestBinding>,
    files: Vec<MetadataFileBinding>,
    evidence: BTreeMap<String, HistoricalIdentityRowEvidenceV1>,
    evidence_ids: BTreeMap<String, String>,
}

fn load_identity_inputs(config: &Config, sources: &Sources) -> Result<IdentityInputs> {
    let mut days: BTreeMap<String, BTreeSet<String>> = sources
        .inventory
        .per_day
        .keys()
        .map(|day| (day.clone(), BTreeSet::new()))
        .collect();
    for identity in sources.identities.values() {
        let time = DateTime::from_timestamp_millis(identity.ready)
            .ok_or_else(|| invalid("身份就绪时间超出范围"))?;
        let day = time.format("%Y-%m-%d").to_string();
        if !sources.inventory.per_day.contains_key(&day) {
            days.entry(day)
                .or_default()
                .insert(time.format("%Y-%m-%dT%H").to_string());
        }
    }
    let mut output = IdentityInputs {
        manifests: Vec::new(),
        files: Vec::new(),
        evidence: BTreeMap::new(),
        evidence_ids: BTreeMap::new(),
    };
    let mut selected: BTreeMap<String, HistoricalIdentityRowEvidenceV1> = BTreeMap::new();
    for (day, context_hours) in days {
        let manifest_path = config
            .dataset_root
            .join(&day)
            .join("typed_raw")
            .join(format!("utc_day={day}"))
            .join("manifest.json");
        let manifest_sha256 = sha256_file(&manifest_path)?;
        let manifest: DailyManifest = read_json(&manifest_path)?;
        output.manifests.push(ManifestBinding {
            path: path_text(&manifest_path),
            sha256: manifest_sha256.clone(),
            utc_day: manifest.utc_day,
            role: if context_hours.is_empty() {
                "fixed_source_date"
            } else {
                "historical_identity_context"
            }
            .to_owned(),
        });
        for file in manifest.files.into_iter().filter(|f| {
            f.source_kind == "polymarket_market_metadata"
                && (context_hours.is_empty() || context_hours.contains(&f.utc_hour))
        }) {
            let parquet_path = manifest_path.parent().unwrap().join(&file.path);
            let actual_hash = sha256_file(&parquet_path)?;
            if actual_hash != file.sha256 {
                return Err(invalid(format!(
                    "{}: 原每日清单摘要不符",
                    parquet_path.display()
                )));
            }
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&parquet_path)?)?;
            let projection =
                ProjectionMask::columns(builder.parquet_schema(), IDENTITY_COLUMNS.iter().copied());
            let reader = builder
                .with_projection(projection)
                .with_batch_size(1024)
                .build()?;
            let mut offset = 0_u64;
            for batch in reader {
                let batch = batch?;
                let slugs = column::<StringArray>(&batch, "slug")?;
                for row in 0..batch.num_rows() {
                    let Some(expected) = sources.identities.get(slugs.value(row)) else {
                        continue;
                    };
                    let actual_assets: Vec<String> =
                        serde_json::from_str(&required_text(&batch, "asset_ids", row)?)?;
                    let expected_assets: Vec<String> =
                        serde_json::from_str(&expected.asset_ids_json)?;
                    if required_text(&batch, "condition_id", row)? != expected.condition_id
                        || required_text(&batch, "clob_market_id", row)? != expected.clob_market_id
                        || actual_assets != expected_assets
                        || required_time(&batch, "market_start_utc", row)? != expected.start
                        || required_time(&batch, "market_end_utc", row)? != expected.end
                        || required_time(&batch, "fixed_identity_verified_at_utc", row)?
                            != expected.ready
                    {
                        return Err(invalid(format!(
                            "{} 行 {}: 原身份索引与原 Parquet 不符",
                            parquet_path.display(),
                            offset + row as u64
                        )));
                    }
                    let admission = column::<UInt64Array>(&batch, "collector_admission_sequence")?;
                    let evidence = HistoricalIdentityRowEvidenceV1 {
                        manifest_path: path_text(&manifest_path),
                        manifest_sha256: manifest_sha256.clone(),
                        parquet_path: path_text(&parquet_path),
                        parquet_sha256: actual_hash.clone(),
                        row_index: offset + row as u64,
                        schema_version: required_text(&batch, "schema_version", row)?,
                        metadata_row_kind: required_text(&batch, "metadata_row_kind", row)?,
                        row_received_at_utc_ms: required_time(&batch, "timestamp_received", row)?,
                        fixed_identity_verified_at_utc_ms: expected.ready,
                        source_evidence_origin: text_at(&batch, "source_evidence_origin", row)?,
                        source_endpoint: text_at(&batch, "source_endpoint", row)?,
                        source_evidence_received_at_utc_ms: timestamp_at(
                            &batch,
                            "source_evidence_received_at_utc",
                            row,
                        )?,
                        source_evidence_raw_sha256: text_at(
                            &batch,
                            "source_evidence_raw_sha256",
                            row,
                        )?,
                        collector_admission_sequence: (!admission.is_null(row))
                            .then(|| admission.value(row)),
                        collector_admission_time_utc_ms: timestamp_at(
                            &batch,
                            "collector_admission_time_utc_ms",
                            row,
                        )?,
                    };
                    let rank = |e: &HistoricalIdentityRowEvidenceV1| {
                        (
                            e.metadata_row_kind != "metadata_snapshot",
                            e.row_received_at_utc_ms,
                        )
                    };
                    let slug = slugs.value(row).to_owned();
                    if selected
                        .get(&slug)
                        .is_none_or(|old| rank(&evidence) < rank(old))
                    {
                        selected.insert(slug, evidence);
                    }
                }
                offset += batch.num_rows() as u64;
            }
            if offset != file.rows || fs::metadata(&parquet_path)?.len() != file.bytes {
                return Err(invalid("原 Parquet 实际行数/字节与清单不符"));
            }
            output.files.push(MetadataFileBinding {
                path: path_text(&parquet_path),
                sha256: actual_hash,
                rows: offset,
                bytes: file.bytes,
                manifest_path: path_text(&manifest_path),
            });
        }
    }
    for (index, slug) in sources.identities.keys().enumerate() {
        let evidence = selected
            .remove(slug)
            .ok_or_else(|| invalid(format!("{slug}: 没有原 Parquet 身份就绪依据")))?;
        let id = format!("identity-{index:05}");
        output.evidence_ids.insert(slug.clone(), id.clone());
        output.evidence.insert(id, evidence);
    }
    Ok(output)
}

#[derive(Clone, Debug)]
struct RepairRow {
    condition_id: String,
    clob_market_id: String,
    slug: String,
    asset_ids_json: String,
    market_start_at_utc_ms: i64,
    market_end_at_utc_ms: i64,
    anchor_source: String,
    price_to_beat_raw: String,
    response_group_id: String,
    response_member_index: u32,
    response_received_at_utc_ms: i64,
    response_sha256: String,
    source_value_path: String,
    historical_identity_ready_at_utc_ms: i64,
    historical_identity_evidence_ids_json: String,
}

fn sort_rows(rows: &mut [RepairRow]) {
    rows.sort_by(|a, b| {
        a.response_group_id
            .cmp(&b.response_group_id)
            .then(a.response_member_index.cmp(&b.response_member_index))
    });
}

fn repair_batch(rows: &[RepairRow]) -> Result<RecordBatch> {
    let timestamp = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let names = [
        "schema_version",
        "condition_id",
        "clob_market_id",
        "slug",
        "asset_ids_json",
        "market_start_at_utc_ms",
        "market_end_at_utc_ms",
        "anchor_source",
        "price_to_beat_raw",
        "response_group_id",
        "response_member_index",
        "response_received_at_utc_ms",
        "response_sha256",
        "source_value_path",
        "source_event_at_utc_ms",
        "historical_identity_ready_at_utc_ms",
        "historical_identity_evidence_ids_json",
        "repair_policy_version",
    ];
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                Field::new(
                    *name,
                    if [5, 6, 11, 14, 15].contains(&i) {
                        timestamp.clone()
                    } else if i == 10 {
                        DataType::UInt32
                    } else {
                        DataType::Utf8
                    },
                    i == 14,
                )
            })
            .collect::<Vec<_>>(),
    ));
    macro_rules! strings {
        ($field:ident) => {
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.$field.as_str()),
            )) as ArrayRef
        };
    }
    macro_rules! times {
        ($field:ident) => {
            Arc::new(
                TimestampMillisecondArray::from_iter_values(rows.iter().map(|r| r.$field))
                    .with_timezone("UTC"),
            ) as ArrayRef
        };
    }
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![SCHEMA; rows.len()])),
            strings!(condition_id),
            strings!(clob_market_id),
            strings!(slug),
            strings!(asset_ids_json),
            times!(market_start_at_utc_ms),
            times!(market_end_at_utc_ms),
            strings!(anchor_source),
            strings!(price_to_beat_raw),
            strings!(response_group_id),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.response_member_index),
            )),
            times!(response_received_at_utc_ms),
            strings!(response_sha256),
            strings!(source_value_path),
            Arc::new(TimestampMillisecondArray::from(vec![None; rows.len()]).with_timezone("UTC")),
            times!(historical_identity_ready_at_utc_ms),
            strings!(historical_identity_evidence_ids_json),
            Arc::new(StringArray::from(vec![POLICY; rows.len()])),
        ],
    )?)
}

fn write_parquet(path: &Path, rows: &[RepairRow]) -> Result<()> {
    let batch = repair_batch(rows)?;
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let sync = file.try_clone()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    sync.sync_all()?;
    Ok(())
}

fn copy_responses(
    output: &Path,
    responses: &BTreeMap<String, HttpResponseEvidenceV1>,
) -> Result<()> {
    fs::create_dir(output.join("responses"))?;
    let mut copied = BTreeSet::new();
    for response in responses.values() {
        if copied.insert(response.response_body_sha256.clone()) {
            let bytes = verified_bytes(
                Path::new(&response.original_response_file),
                &response.response_body_sha256,
            )?;
            write_new(&output.join(&response.response_body_path), &bytes)?;
        }
    }
    Ok(())
}

fn build(config: &Config, sources: Sources) -> Result<()> {
    let identity_inputs = load_identity_inputs(config, &sources)?;
    let mut rows = Vec::with_capacity(sources.anchors.len());
    for (slug, anchor) in &sources.anchors {
        let identity = sources
            .identities
            .get(slug)
            .ok_or_else(|| invalid("锚点缺少原身份"))?;
        let response = &sources.responses[&anchor.group];
        rows.push(RepairRow {
            condition_id: identity.condition_id.clone(),
            clob_market_id: identity.clob_market_id.clone(),
            slug: slug.clone(),
            asset_ids_json: identity.asset_ids_json.clone(),
            market_start_at_utc_ms: identity.start,
            market_end_at_utc_ms: identity.end,
            anchor_source: anchor.source.clone(),
            price_to_beat_raw: anchor.raw.clone(),
            response_group_id: anchor.group.clone(),
            response_member_index: anchor.member,
            response_received_at_utc_ms: response.received_at_utc_ms,
            response_sha256: response.response_body_sha256.clone(),
            source_value_path: anchor.value_path.clone(),
            historical_identity_ready_at_utc_ms: identity.ready,
            historical_identity_evidence_ids_json: serde_json::to_string(&[
                &identity_inputs.evidence_ids[slug]
            ])?,
        });
    }
    sort_rows(&mut rows);
    let mut remaining = Vec::new();
    for (slug, identity) in &sources.identities {
        if sources.anchors.contains_key(slug) {
            continue;
        }
        let last = sources
            .network_attempts
            .iter()
            .rev()
            .find(|attempt| &attempt.slug == slug);
        let reason = if let Some(attempt) = last {
            match attempt.result.as_str() {
                "observed" => "website_missing_open_price",
                "http_error" => "website_http_error",
                "decode_error" => "website_decode_error",
                "transport_error" => "website_transport_error",
                "timeout" => "website_timeout",
                _ => return Err(invalid("新查询结果类型不受该版本支持")),
            }
        } else if sources.outside_history.contains(slug) {
            "outside_observed_website_history_window"
        } else {
            "pending_website_query"
        };
        remaining.push(Remaining {
            slug: slug.clone(),
            condition_id: identity.condition_id.clone(),
            clob_market_id: identity.clob_market_id.clone(),
            reason: reason.to_owned(),
            investigation_response_group_ids: sources
                .investigations
                .get(slug)
                .cloned()
                .unwrap_or_default(),
        });
    }
    fs::create_dir(&config.output_dir)?;
    copy_responses(&config.output_dir, &sources.responses)?;
    let parquet_path = config.output_dir.join(PARQUET_NAME);
    write_parquet(&parquet_path, &rows)?;
    let output = OutputBinding {
        path: PARQUET_NAME.to_owned(),
        sha256: sha256_file(&parquet_path)?,
        bytes: fs::metadata(&parquet_path)?.len(),
        rows: rows.len() as u64,
        schema_version: SCHEMA.to_owned(),
    };
    let report = serde_json::json!({
        "scope": "固定来源日期锚点补充产物，尚未接入 DPT 或模型", "build_state": config.build_state,
        "build_commit_is_baseline": config.build_state != "committed", "rows": rows.len(), "remaining": remaining.len(),
        "selected_response_groups": rows.iter().map(|r| &r.response_group_id).collect::<BTreeSet<_>>().len(),
        "at_market_start": rows.iter().filter(|r| r.historical_identity_ready_at_utc_ms <= r.market_start_at_utc_ms).count(),
        "at_original_identity_ready": rows.iter().filter(|r| r.historical_identity_ready_at_utc_ms > r.market_start_at_utc_ms).count(),
        "groups": sources.date_sets.iter().map(|(name,set)| (name.clone(), serde_json::json!({"identities":set.member_slugs.len(),
            "with_anchor":set.member_slugs.iter().filter(|s| sources.anchors.contains_key(*s)).count()}))).collect::<BTreeMap<_,_>>(),
    });
    write_json_new(&config.output_dir.join("build-report.json"), &report)?;
    let manifest = RepairManifest {
        schema_version: "historical_price_anchor_repair_manifest_v1".to_owned(),
        repair_policy_version: POLICY.to_owned(),
        created_at_utc: now(),
        generator: generator(config)?,
        source_date_sets: sources.date_sets,
        input_manifests: identity_inputs.manifests,
        input_metadata_files: identity_inputs.files,
        input_evidence_files: sources.evidence_files,
        response_groups: sources.responses,
        identity_evidence: identity_inputs.evidence,
        outputs: vec![output],
        remaining,
        network_attempts: sources.network_attempts,
    };
    write_json_new(&config.output_dir.join("manifest.json"), &manifest)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn persist_capture<T>(
    capture: ResponseCapture<T>,
    output: &Path,
) -> Result<(HttpResponseEvidenceV1, polymarket_client_sdk_v2::Result<T>)> {
    let sha256 = hash_bytes(&capture.raw_body);
    let relative = format!("responses/{sha256}.json");
    let file = output.join(&relative);
    if !file.exists() {
        write_new(&file, &capture.raw_body)?;
    }
    Ok((
        HttpResponseEvidenceV1 {
            origin: "website_crypto_price".to_owned(),
            request_url: capture.request_url.to_string(),
            received_at_utc: capture.received_at_utc.to_rfc3339(),
            received_at_utc_ms: capture.received_at_utc.timestamp_millis(),
            receipt_observation: "sdk_body_received".to_owned(),
            response_body_sha256: sha256,
            response_body_path: relative,
            original_response_file: path_text(&file),
            http_status_code: Some(capture.status_code.as_u16()),
            response_url: Some(capture.response_url.to_string()),
        },
        capture.result,
    ))
}

async fn query_remaining(config: &Config, sources: Sources) -> Result<()> {
    let planned = sources
        .website_requests
        .keys()
        .filter(|s| !sources.anchors.contains_key(*s))
        .cloned()
        .collect::<Vec<_>>();
    fs::create_dir(&config.output_dir)?;
    fs::create_dir(config.output_dir.join("responses"))?;
    let mut journal = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output_dir.join("attempts.jsonl"))?;
    let mut run = QueryRun {
        schema_version: "historical_price_anchor_website_query_v1".to_owned(),
        generator: generator(config)?,
        build_state: config.build_state.clone(),
        input_evidence_files: sources.evidence_files,
        planned_slugs: planned.clone(),
        unattempted_slugs: planned.clone(),
        stopped_on_http_429: false,
        response_groups: BTreeMap::new(),
        network_attempts: Vec::new(),
    };
    let client = Client::default();
    let mut next = Instant::now();
    for slug in planned {
        sleep_until(next).await;
        next = Instant::now() + QUERY_INTERVAL;
        let identity = &sources.identities[&slug];
        let request = &sources.website_requests[&slug];
        let mut attempt = WebsiteQueryAttemptV1 {
            slug: slug.clone(),
            condition_id: identity.condition_id.clone(),
            clob_market_id: identity.clob_market_id.clone(),
            parameters_json: serde_json::to_string(request)?,
            attempt_started_at_utc: now(),
            attempt_completed_at_utc: String::new(),
            result: String::new(),
            response_group_id: None,
            error: None,
        };
        let mut response_evidence = None;
        let result = timeout(QUERY_TIMEOUT, client.website_crypto_price_capture(request)).await;
        attempt.attempt_completed_at_utc = now();
        match result {
            Err(_) => {
                attempt.result = "timeout".to_owned();
                attempt.error = Some("SDK 官网请求超过 20 秒".to_owned());
            }
            Ok(Err(error)) => {
                attempt.result = "transport_error".to_owned();
                attempt.error = Some(error.to_string());
            }
            Ok(Ok(capture)) => {
                let status = capture.status_code;
                let (response, decoded) = persist_capture(capture, &config.output_dir)?;
                attempt.result = if !status.is_success() {
                    "http_error"
                } else if decoded.is_err() {
                    "decode_error"
                } else {
                    "observed"
                }
                .to_owned();
                attempt.error = decoded.err().map(|error| error.to_string());
                let id = response.id()?;
                attempt.response_group_id = Some(id.clone());
                run.stopped_on_http_429 = status.as_u16() == 429;
                run.response_groups.insert(id, response.clone());
                response_evidence = Some(response);
            }
        }
        serde_json::to_writer(
            &mut journal,
            &serde_json::json!({"attempt": &attempt, "response": response_evidence}),
        )?;
        journal.write_all(b"\n")?;
        journal.sync_data()?;
        println!("{} {}", slug, attempt.result);
        run.network_attempts.push(attempt);
        run.unattempted_slugs.remove(0);
        if run.stopped_on_http_429 {
            break;
        }
    }
    write_json_new(&config.output_dir.join("query-results.json"), &run)?;
    println!(
        "已完成 {} 项，未执行 {} 项，遇 429 停止={}",
        run.network_attempts.len(),
        run.unattempted_slugs.len(),
        run.stopped_on_http_429
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = async {
        let config = Config::from_args()?;
        let sources = load_sources(&config)?;
        eprintln!(
            "已从 SDK 原字节解析取得 {} 个锚点、{} 个身份",
            sources.anchors.len(),
            sources.identities.len()
        );
        if config.action == "build" {
            build(&config, sources)
        } else {
            query_remaining(&config, sources).await
        }
    }
    .await;
    if let Err(error) = result {
        eprintln!("price_anchor_backfill_error={error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_bytes_keep_exact_anchor_and_exclude_final_price() {
        let bytes = br#"[{"id":"1","eventMetadata":{"priceToBeat":64950.014218511440000000000001,"finalPrice":99999}},{"id":"2","eventMetadata":{"priceToBeat":"7.100e-18","finalPrice":88888}}]"#;
        let events: Vec<Event> = decode(bytes, "test").unwrap();
        assert_eq!(
            events[0]
                .event_metadata
                .as_ref()
                .unwrap()
                .price_to_beat
                .as_ref()
                .unwrap()
                .0,
            "64950.014218511440000000000001"
        );
        assert_eq!(
            events[1]
                .event_metadata
                .as_ref()
                .unwrap()
                .price_to_beat
                .as_ref()
                .unwrap()
                .0,
            "7.100e-18"
        );
        let website: WebsiteCryptoPriceResponse =
            decode(br#"{"openPrice":77188.89475116669,"closePrice":1}"#, "test").unwrap();
        assert_eq!(website.open_price.unwrap().0, "77188.89475116669");
    }

    #[test]
    fn legacy_receipt_preserves_unknown_http_facts_and_group_identity() {
        let query = LegacyQuery {
            url: "https://gamma-api.polymarket.com/events?slug=a&slug=b".to_owned(),
            received_at: "2026-09-08T03:55:08.963464+00:00".to_owned(),
            response_file: "/original/a.json".to_owned(),
            sha256: "abcd".to_owned(),
            endpoint: None,
            slug: None,
        };
        let original = legacy_response(&query, "gamma_events", None).unwrap();
        assert_eq!(original.http_status_code, None);
        assert_eq!(original.response_url, None);
        assert_eq!(original.received_at_utc_ms, 1788839708963);
        let mut moved = original.clone();
        moved.original_response_file = "/copied/b.json".to_owned();
        moved.response_body_path = "elsewhere.json".to_owned();
        assert_eq!(original.id().unwrap(), moved.id().unwrap());
        moved.received_at_utc = "2026-09-08T03:55:08.963465+00:00".to_owned();
        assert_ne!(original.id().unwrap(), moved.id().unwrap());
    }

    fn repair_row(member: u32) -> RepairRow {
        RepairRow {
            condition_id: "condition".to_owned(),
            clob_market_id: "market".to_owned(),
            slug: format!("market-{member}"),
            asset_ids_json: "[\"1\",\"2\"]".to_owned(),
            market_start_at_utc_ms: 1000,
            market_end_at_utc_ms: 301000,
            anchor_source: "gamma_event_metadata_price_to_beat".to_owned(),
            price_to_beat_raw: "64950.014218511440000000000001".to_owned(),
            response_group_id: "same-response".to_owned(),
            response_member_index: member,
            response_received_at_utc_ms: 90000000,
            response_sha256: "body-sha".to_owned(),
            source_value_path: format!("/{member}/eventMetadata/priceToBeat"),
            historical_identity_ready_at_utc_ms: 57500,
            historical_identity_evidence_ids_json: "[\"identity-0\"]".to_owned(),
        }
    }

    #[test]
    fn supplement_parquet_keeps_numeric_members_and_two_time_domains() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("anchors.parquet");
        let mut rows = vec![repair_row(10), repair_row(2)];
        sort_rows(&mut rows);
        write_parquet(&file, &rows).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&file).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 18);
        assert_eq!(
            column::<UInt32Array>(&batch, "response_member_index")
                .unwrap()
                .values()
                .as_ref(),
            &[2, 10]
        );
        assert_eq!(
            required_text(&batch, "price_to_beat_raw", 0).unwrap(),
            rows[0].price_to_beat_raw
        );
        assert_eq!(
            required_time(&batch, "response_received_at_utc_ms", 0).unwrap(),
            90000000
        );
        assert_eq!(
            required_time(&batch, "historical_identity_ready_at_utc_ms", 0).unwrap(),
            57500
        );
        assert!(
            column::<TimestampMillisecondArray>(&batch, "source_event_at_utc_ms")
                .unwrap()
                .is_null(0)
        );
        assert!(write_parquet(&file, &rows).is_err());
    }

    #[test]
    fn changed_response_bytes_do_not_pass_original_digest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("response.json");
        fs::write(&path, b"{\"openPrice\":2}").unwrap();
        assert!(verified_bytes(&path, &hash_bytes(b"{\"openPrice\":1}")).is_err());
    }

    #[test]
    fn history_reason_uses_the_frozen_limit_observation_time() {
        let observed = time_ms("2000-02-01T00:00:00Z").unwrap();
        assert!(!outside_observed_history_window(
            time_ms("2000-01-31T00:00:00Z").unwrap(),
            observed
        ));
        assert!(outside_observed_history_window(
            time_ms("1999-12-01T00:00:00Z").unwrap(),
            observed
        ));
    }

    #[test]
    fn failed_sdk_response_keeps_actual_body_status_and_receipt() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("responses")).unwrap();
        let body = b"{\"error\":\"rate limited\"}".to_vec();
        let capture = ResponseCapture::<WebsiteCryptoPriceResponse>::builder()
            .result(Err(polymarket_client_sdk_v2::error::Error::status(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                reqwest::Method::GET,
                "/api/crypto/crypto-price".to_owned(),
                "rate limited",
            )))
            .raw_body(body.clone())
            .request_url(
                "https://polymarket.com/api/crypto/crypto-price?symbol=BTC"
                    .parse()
                    .unwrap(),
            )
            .response_url(
                "https://polymarket.com/api/crypto/crypto-price?symbol=BTC"
                    .parse()
                    .unwrap(),
            )
            .received_at_utc(
                DateTime::parse_from_rfc3339("2026-09-08T06:00:00.123456Z")
                    .unwrap()
                    .with_timezone(&Utc),
            )
            .status_code(reqwest::StatusCode::TOO_MANY_REQUESTS)
            .build();
        let (evidence, result) = persist_capture(capture, dir.path()).unwrap();
        assert!(result.is_err());
        assert_eq!(evidence.http_status_code, Some(429));
        assert_eq!(evidence.received_at_utc, "2026-09-08T06:00:00.123456+00:00");
        assert_eq!(fs::read(evidence.original_response_file).unwrap(), body);
    }
}
