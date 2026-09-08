//! 已签发来源范围的官方锚点补查与独立补充产物；不运行 DPT 或改写原采集日。

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
use data_collect::{daily_manifest::write_json_atomic, sha256_file};
use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use polymarket_client_sdk_v2::gamma::Client;
use polymarket_client_sdk_v2::gamma::types::request::{
    EventBySlugRequest, MarketsRequest, WebsiteCryptoPriceRequest,
};
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
const LEGACY_CONFLICT: &str = "btc-updown-5m-1785196800";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    FixedDates,
    RemainingV5,
}

#[derive(Debug)]
struct Config {
    action: String,
    scope: Scope,
    evidence_root: PathBuf,
    dataset_root: PathBuf,
    output_dir: PathBuf,
    additional_queries: Vec<PathBuf>,
    build_commit: String,
    build_state: String,
}

impl Config {
    fn from_args() -> Result<Self> {
        Self::parse(std::env::args().skip(1))
    }

    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self> {
        let action = args
            .next()
            .ok_or_else(|| invalid("需要 build 或 query-remaining 子命令"))?;
        if action != "build" && action != "query-remaining" {
            return Err(invalid("子命令须为 build 或 query-remaining"));
        }
        let mut values = BTreeMap::new();
        let mut additional_queries = Vec::new();
        while let Some(key) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| invalid(format!("{key} 缺少参数值")))?;
            match key.as_str() {
                "--additional-queries" => additional_queries.push(PathBuf::from(value)),
                "--scope" | "--evidence-root" | "--dataset-root" | "--output-dir"
                | "--build-commit" | "--build-state" => {
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
        let scope = match values.remove("--scope").as_deref().unwrap_or("fixed-dates") {
            "fixed-dates" => Scope::FixedDates,
            "remaining-v5" => Scope::RemainingV5,
            value => return Err(invalid(format!("未知来源范围 {value}"))),
        };
        if scope == Scope::FixedDates && additional_queries.len() > 1 {
            additional_queries = additional_queries.into_iter().rev().take(1).collect();
        }
        Ok(Self {
            action,
            scope,
            evidence_root,
            dataset_root,
            output_dir,
            build_commit,
            build_state,
            additional_queries,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct QueryMember {
    slug: String,
    condition_id: String,
    clob_market_id: String,
    asset_ids_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemainingQueryAttemptV1 {
    request_ordinal: u64,
    origin: String,
    members: Vec<QueryMember>,
    parameters_json: String,
    attempt_started_at_utc: String,
    attempt_completed_at_utc: Option<String>,
    result: String,
    response_group_id: Option<String>,
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum NetworkAttempt {
    Website(WebsiteQueryAttemptV1),
    Remaining(RemainingQueryAttemptV1),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemainingPlannedRequest {
    origin: String,
    members: Vec<QueryMember>,
    parameters_json: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemainingQueryRunV1 {
    schema_version: String,
    generator: Generator,
    build_state: String,
    input_evidence_files: Vec<FileBinding>,
    response_groups: BTreeMap<String, HttpResponseEvidenceV1>,
    network_attempts: Vec<RemainingQueryAttemptV1>,
    stopped_on_http_429: bool,
    unattempted_requests: Vec<RemainingPlannedRequest>,
    in_flight_request: Option<RemainingQueryAttemptV1>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemainingCompletion {
    attempt: RemainingQueryAttemptV1,
    response: Option<HttpResponseEvidenceV1>,
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
    network_attempts: Vec<NetworkAttempt>,
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
    ready: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RemainingLocalIdentity {
    identity: IdentityFields,
    ready_values: Vec<String>,
}
#[derive(Debug, Deserialize)]
struct RemainingDateEvidence {
    days: BTreeMap<String, RemainingDayEvidence>,
}
#[derive(Debug, Deserialize)]
struct RemainingDayEvidence {
    manifest_path: String,
    manifest_sha256: String,
}

#[derive(Debug, Serialize)]
struct OfflineDecodeAdoption {
    response_group_id: String,
    original_sdk_error: String,
    adopted_members: usize,
}

#[derive(Default)]
struct RemainingV5State {
    manifests: Vec<ManifestBinding>,
    completed: BTreeSet<(String, String)>,
    returned_markets: BTreeSet<String>,
    history_limit_observed_at: Option<i64>,
    spot_history_members: BTreeSet<String>,
    offline_decode_adoptions: Vec<OfflineDecodeAdoption>,
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
    network_attempts: Vec<NetworkAttempt>,
    remaining_v5: Option<RemainingV5State>,
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
    if config.scope == Scope::RemainingV5 {
        return load_remaining_sources(config);
    }
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
                    ready: Some(time_ms(&local.first_fixed_identity_verified_at_utc)?),
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
        remaining_v5: None,
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
    if let Some(path) = config.additional_queries.last() {
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
        sources
            .network_attempts
            .push(NetworkAttempt::Website(attempt));
    }
    Ok(())
}

fn load_remaining_sources(config: &Config) -> Result<Sources> {
    let local_path = config
        .evidence_root
        .join("remaining-market-local-identities.json");
    let dates_path = config.evidence_root.join("remaining-date-evidence.json");
    let local: BTreeMap<String, RemainingLocalIdentity> = read_json(&local_path)?;
    let dates: RemainingDateEvidence = read_json(&dates_path)?;
    let identities = local
        .into_iter()
        .map(|(slug, local)| {
            let ready = if slug == LEGACY_CONFLICT {
                None
            } else {
                let [ready] = local.ready_values.as_slice() else {
                    return Err(invalid(format!("{slug}: 原身份就绪证据不一致")));
                };
                Some(time_ms(ready)?)
            };
            let value = local.identity;
            Ok((
                slug,
                Identity {
                    condition_id: value.condition_id,
                    clob_market_id: value.clob_market_id,
                    asset_ids_json: value.asset_ids,
                    start: time_ms(&value.market_start_utc)?,
                    end: time_ms(&value.market_end_utc)?,
                    ready,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut sources = Sources {
        date_sets: BTreeMap::from([(
            "unassigned".to_owned(),
            SourceDateSet {
                first_utc_day: "2026-07-27".to_owned(),
                last_utc_day: "2026-09-07".to_owned(),
                member_slugs: identities.keys().cloned().collect(),
            },
        )]),
        identities,
        inventory: Inventory {
            per_day: BTreeMap::new(),
        },
        evidence_files: vec![bind(&local_path)?, bind(&dates_path)?],
        responses: BTreeMap::new(),
        anchors: BTreeMap::new(),
        website_requests: BTreeMap::new(),
        outside_history: BTreeSet::new(),
        investigations: BTreeMap::new(),
        network_attempts: Vec::new(),
        remaining_v5: Some(RemainingV5State {
            manifests: dates
                .days
                .into_iter()
                .map(|(day, d)| ManifestBinding {
                    path: d.manifest_path,
                    sha256: d.manifest_sha256,
                    utc_day: day,
                    role: "unassigned_source_date".to_owned(),
                })
                .collect(),
            ..RemainingV5State::default()
        }),
    };
    let notes = config.evidence_root.join("remaining-backfill-scope.md");
    if notes.exists() {
        sources.evidence_files.push(bind(&notes)?);
    }
    for input in &config.additional_queries {
        if input.is_dir() && input.join("selected-identities.json").exists() {
            import_first_markets_probe(&mut sources, input)?;
        } else if input
            .file_name()
            .is_some_and(|name| name == "ui-diagnostic-summary.json")
        {
            sources.evidence_files.push(bind(input)?);
            let rows: Vec<UiStatus> = read_json(input)?;
            for row in rows {
                if row.http_status == "400"
                    && row.body.as_deref()
                        == Some(r#"{"error":"Timestamp too old for Chainlink API (max 30 days)"}"#)
                {
                    verified_bytes(Path::new(&row.response_file), &row.sha256)?;
                    sources
                        .evidence_files
                        .push(bind(Path::new(&row.response_file))?);
                    sources
                        .remaining_v5
                        .as_mut()
                        .unwrap()
                        .history_limit_observed_at = Some(time_ms(&row.received_at)?);
                    break;
                }
            }
        } else {
            import_remaining_queries(&mut sources, input)?;
        }
    }
    refresh_history_limit(&mut sources);
    Ok(sources)
}

fn query_member(slug: &str, identity: &Identity) -> QueryMember {
    QueryMember {
        slug: slug.to_owned(),
        condition_id: identity.condition_id.clone(),
        clob_market_id: identity.clob_market_id.clone(),
        asset_ids_json: identity.asset_ids_json.clone(),
    }
}

fn website_request_for_identity(
    market: &Market,
    identity: &Identity,
) -> Option<WebsiteCryptoPriceRequest> {
    let config = market.crypto_market_config.as_ref()?;
    let symbol = config.asset.as_ref()?.to_ascii_uppercase();
    let variant = match config.duration.as_deref()? {
        "5m" => "fiveminute",
        _ => return None,
    };
    let twap = config.twap_enabled?;
    let window = if twap {
        config.twap_lookback_seconds?
    } else {
        0
    };
    Some(
        WebsiteCryptoPriceRequest::builder()
            .symbol(symbol)
            .variant(variant)
            .twap_enabled(twap)
            .twap_lookback_seconds(window)
            .event_start_time(DateTime::from_timestamp_millis(identity.start)?)
            .end_date(DateTime::from_timestamp_millis(identity.end)?)
            .build(),
    )
}

fn note_market(sources: &mut Sources, slug: &str, market: &Market) {
    let state = sources.remaining_v5.as_mut().unwrap();
    state.returned_markets.insert(slug.to_owned());
    if market.resolution_source.as_deref() == Some("https://data.chain.link/streams/btc-usd") {
        state.spot_history_members.insert(slug.to_owned());
    }
    if let Some(request) = website_request_for_identity(market, &sources.identities[slug]) {
        sources.website_requests.insert(slug.to_owned(), request);
    }
}

fn refresh_history_limit(sources: &mut Sources) {
    let state = sources.remaining_v5.as_ref().unwrap();
    if let Some(at) = state.history_limit_observed_at {
        for slug in &state.spot_history_members {
            if outside_observed_history_window(sources.identities[slug].start, at) {
                sources.outside_history.insert(slug.clone());
            }
        }
    }
}

fn select_gamma_anchor(
    sources: &mut Sources,
    slug: &str,
    raw: &str,
    group: &str,
    member: u32,
    value_path: String,
) {
    if sources.identities[slug].ready.is_none() {
        return;
    }
    if sources
        .anchors
        .get(slug)
        .is_none_or(|anchor| anchor.source == "official_crypto_price_open_price")
    {
        sources.anchors.insert(
            slug.to_owned(),
            SelectedAnchor {
                source: "gamma_event_metadata_price_to_beat".to_owned(),
                raw: raw.to_owned(),
                group: group.to_owned(),
                member,
                value_path,
            },
        );
    }
}

enum RemainingDecoded {
    Markets(Vec<Market>),
    Event(Box<Event>),
    Website(WebsiteCryptoPriceResponse),
}

fn adopt_remaining_response(
    sources: &mut Sources,
    attempt: &RemainingQueryAttemptV1,
    group: &str,
    decoded: RemainingDecoded,
) -> Result<()> {
    match decoded {
        RemainingDecoded::Markets(markets) => {
            for (i, market) in markets.iter().enumerate() {
                let slug = market
                    .slug
                    .as_deref()
                    .ok_or_else(|| invalid("Gamma 市场缺少 slug"))?;
                let identity = sources
                    .identities
                    .get(slug)
                    .ok_or_else(|| invalid("Gamma 市场没有原身份"))?;
                match_market(market, slug, identity)?;
                note_market(sources, slug, market);
                // 外层 i 与实际关联事件 j 都来自原响应，不能重排或假定 events[0]。
                if let Some((j, event)) = market
                    .events
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .find(|(_, event)| event.slug.as_deref() == Some(slug))
                    && let Some(raw) = event
                        .event_metadata
                        .as_ref()
                        .and_then(|m| m.price_to_beat.as_ref())
                {
                    select_gamma_anchor(
                        sources,
                        slug,
                        &raw.0,
                        group,
                        i.try_into()?,
                        format!("/{i}/events/{j}/eventMetadata/priceToBeat"),
                    );
                }
            }
        }
        RemainingDecoded::Event(event) => {
            let slug = &attempt.members[0].slug;
            let market = market_for_identity(&event, slug, &sources.identities[slug])?;
            note_market(sources, slug, market);
            if let Some(raw) = event
                .event_metadata
                .as_ref()
                .and_then(|m| m.price_to_beat.as_ref())
            {
                select_gamma_anchor(
                    sources,
                    slug,
                    &raw.0,
                    group,
                    0,
                    "/eventMetadata/priceToBeat".to_owned(),
                );
            }
        }
        RemainingDecoded::Website(prices) => {
            let slug = &attempt.members[0].slug;
            if sources.identities[slug].ready.is_some()
                && let Some(raw) = prices.open_price
            {
                sources
                    .anchors
                    .entry(slug.clone())
                    .or_insert(SelectedAnchor {
                        source: "official_crypto_price_open_price".to_owned(),
                        raw: raw.0,
                        group: group.to_owned(),
                        member: 0,
                        value_path: "/openPrice".to_owned(),
                    });
            }
        }
    }
    refresh_history_limit(sources);
    Ok(())
}

fn decode_remaining(origin: &str, bytes: &[u8], path: &str) -> Result<RemainingDecoded> {
    match origin {
        "gamma_markets" => Ok(RemainingDecoded::Markets(decode(bytes, path)?)),
        "gamma_event_by_slug" => Ok(RemainingDecoded::Event(Box::new(decode(bytes, path)?))),
        "website_crypto_price" => Ok(RemainingDecoded::Website(decode(bytes, path)?)),
        _ => Err(invalid(format!("本范围不支持响应来源 {origin}"))),
    }
}

fn import_remaining_attempt(
    sources: &mut Sources,
    attempt: RemainingQueryAttemptV1,
    response: Option<HttpResponseEvidenceV1>,
    adopt_old_decode_error: bool,
) -> Result<()> {
    for member in &attempt.members {
        let identity = sources
            .identities
            .get(&member.slug)
            .ok_or_else(|| invalid("新查询没有原身份"))?;
        if identity.condition_id != member.condition_id
            || identity.clob_market_id != member.clob_market_id
            || serde_json::from_str::<Vec<String>>(&identity.asset_ids_json)?
                != serde_json::from_str::<Vec<String>>(&member.asset_ids_json)?
        {
            return Err(invalid(format!("{}: 查询身份与原历史不符", member.slug)));
        }
    }
    if let Some(response) = response {
        let id = response.id()?;
        if attempt.response_group_id.as_deref() != Some(&id) {
            return Err(invalid("查询的响应组身份与原响应证据不符"));
        }
        let bytes = verified_bytes(
            Path::new(&response.original_response_file),
            &response.response_body_sha256,
        )?;
        if sources.network_attempts.iter().any(|old| {
            matches!(old, NetworkAttempt::Remaining(old)
            if old.response_group_id.as_ref() == Some(&id))
        }) {
            return Ok(());
        }
        for member in &attempt.members {
            sources
                .investigations
                .entry(member.slug.clone())
                .or_default()
                .push(id.clone());
        }
        if response.http_status_code == Some(200)
            && (attempt.result == "observed" || adopt_old_decode_error)
        {
            let before = sources.anchors.len();
            let decoded =
                decode_remaining(&attempt.origin, &bytes, &response.original_response_file)?;
            adopt_remaining_response(sources, &attempt, &id, decoded)?;
            if adopt_old_decode_error && let Some(error) = &attempt.error {
                sources
                    .remaining_v5
                    .as_mut()
                    .unwrap()
                    .offline_decode_adoptions
                    .push(OfflineDecodeAdoption {
                        response_group_id: id.clone(),
                        original_sdk_error: error.clone(),
                        adopted_members: sources.anchors.len() - before,
                    });
            }
        }
        sources.responses.insert(id, response);
    } else if attempt.response_group_id.is_some() {
        return Err(invalid("新查询缺少原响应组"));
    }
    if attempt.attempt_completed_at_utc.is_some() {
        for member in &attempt.members {
            sources
                .remaining_v5
                .as_mut()
                .unwrap()
                .completed
                .insert((attempt.origin.clone(), member.slug.clone()));
        }
    }
    sources
        .network_attempts
        .push(NetworkAttempt::Remaining(attempt));
    Ok(())
}

#[derive(Deserialize)]
struct ProbeIdentity {
    slug: String,
    identity: IdentityFields,
}
#[derive(Deserialize)]
struct ProbePrepared {
    request_parameters: Box<serde_json::value::RawValue>,
}
#[derive(Deserialize)]
struct ProbeAttempt {
    decode_result: String,
    decode_error: Option<String>,
    http_status_code: u16,
    receipt_observation: String,
    received_at_utc: String,
    received_at_utc_ms: i64,
    request_url: String,
    response_url: String,
    response_body_path: String,
    response_body_sha256: String,
    started_at_unix_ns: String,
}
#[derive(Deserialize)]
struct ProbeRunEvidence {
    completed_at_utc: String,
}

fn import_first_markets_probe(sources: &mut Sources, root: &Path) -> Result<()> {
    let selected: Vec<ProbeIdentity> = read_json(&root.join("selected-identities.json"))?;
    for name in [
        "selected-identities.json",
        "evidence-index.json",
        "build-provenance.json",
        "compile-evidence.json",
        "compile-controls-evidence.json",
        "run-evidence.json",
        "run-closed-true-evidence.json",
    ] {
        sources.evidence_files.push(bind(&root.join(name))?);
    }
    let members = selected
        .into_iter()
        .map(|item| QueryMember {
            slug: item.slug,
            condition_id: item.identity.condition_id,
            clob_market_id: item.identity.clob_market_id,
            asset_ids_json: item.identity.asset_ids,
        })
        .collect::<Vec<_>>();
    for (ordinal, (directory, run_name)) in [
        ("capture", "run-evidence.json"),
        ("capture-closed-true", "run-closed-true-evidence.json"),
    ]
    .into_iter()
    .enumerate()
    {
        let dir = root.join(directory);
        let prepared: ProbePrepared = read_json(&dir.join("prepared-request.json"))?;
        let old: ProbeAttempt = read_json(&dir.join("attempt.json"))?;
        let run: ProbeRunEvidence = read_json(&root.join(run_name))?;
        sources
            .evidence_files
            .push(bind(&dir.join("prepared-request.json"))?);
        sources
            .evidence_files
            .push(bind(&dir.join("attempt.json"))?);
        let ns: i128 = old.started_at_unix_ns.parse()?;
        let started = DateTime::<Utc>::from_timestamp(
            (ns / 1_000_000_000).try_into()?,
            (ns % 1_000_000_000).try_into()?,
        )
        .ok_or_else(|| invalid("原请求开始时间超出范围"))?;
        let response = HttpResponseEvidenceV1 {
            origin: "gamma_markets".to_owned(),
            request_url: old.request_url,
            received_at_utc: old.received_at_utc,
            received_at_utc_ms: old.received_at_utc_ms,
            receipt_observation: old.receipt_observation,
            response_body_path: format!("responses/{}.json", old.response_body_sha256),
            response_body_sha256: old.response_body_sha256,
            original_response_file: path_text(&dir.join(old.response_body_path)),
            http_status_code: Some(old.http_status_code),
            response_url: Some(old.response_url),
        };
        let attempt = RemainingQueryAttemptV1 {
            request_ordinal: ordinal as u64,
            origin: "gamma_markets".to_owned(),
            members: members.clone(),
            parameters_json: prepared.request_parameters.get().to_owned(),
            attempt_started_at_utc: started.to_rfc3339(),
            attempt_completed_at_utc: Some(run.completed_at_utc),
            result: if old.decode_result == "ok" {
                "observed"
            } else {
                "decode_error"
            }
            .to_owned(),
            response_group_id: Some(response.id()?),
            error: old.decode_error,
        };
        import_remaining_attempt(
            sources,
            attempt,
            Some(response),
            directory == "capture-closed-true",
        )?;
    }
    Ok(())
}

fn read_remaining_run(path: &Path) -> Result<RemainingQueryRunV1> {
    let mut run: RemainingQueryRunV1 = read_json(path)?;
    let journal_path = path.parent().unwrap().join("attempts.jsonl");
    if journal_path.exists() {
        let records = serde_json::Deserializer::from_reader(File::open(journal_path)?)
            .into_iter::<RemainingCompletion>();
        for record in records {
            let record = match record {
                Ok(record) => record,
                Err(error) if error.is_eof() => break,
                Err(error) => return Err(error.into()),
            };
            if run
                .network_attempts
                .iter()
                .any(|old| old.request_ordinal == record.attempt.request_ordinal)
            {
                continue;
            }
            if let Some(response) = record.response {
                run.stopped_on_http_429 |= response.http_status_code == Some(429);
                run.response_groups.insert(response.id()?, response);
            }
            run.network_attempts.push(record.attempt);
        }
    }
    if run.in_flight_request.as_ref().is_some_and(|pending| {
        run.network_attempts
            .iter()
            .any(|done| done.request_ordinal == pending.request_ordinal)
    }) {
        run.in_flight_request = None;
    }
    Ok(run)
}

fn import_remaining_queries(sources: &mut Sources, input: &Path) -> Result<()> {
    let path = if input.is_dir() {
        input.join("query-results.json")
    } else {
        input.to_owned()
    };
    let binding = bind(&path)?;
    if sources
        .evidence_files
        .iter()
        .any(|old| old.path == binding.path && old.sha256 == binding.sha256)
    {
        return Ok(());
    }
    let run = read_remaining_run(&path)?;
    sources.evidence_files.push(binding);
    let journal = path.parent().unwrap().join("attempts.jsonl");
    if journal.exists() {
        sources.evidence_files.push(bind(&journal)?);
    }
    for attempt in run
        .network_attempts
        .into_iter()
        .chain(run.in_flight_request)
    {
        let response = attempt
            .response_group_id
            .as_ref()
            .and_then(|id| run.response_groups.get(id))
            .cloned();
        import_remaining_attempt(sources, attempt, response, false)?;
    }
    Ok(())
}

fn markets_request(members: &[QueryMember]) -> MarketsRequest {
    MarketsRequest::builder()
        .limit(50)
        .id(members.iter().map(|m| m.clob_market_id.clone()).collect())
        .closed(true)
        .build()
}

fn remaining_plan(sources: &Sources) -> Result<Vec<RemainingPlannedRequest>> {
    let state = sources.remaining_v5.as_ref().unwrap();
    let completed = |origin: &str, slug: &str| {
        state
            .completed
            .contains(&(origin.to_owned(), slug.to_owned()))
    };
    let mut output = Vec::new();
    let members = sources
        .identities
        .iter()
        .filter(|(slug, _)| !completed("gamma_markets", slug))
        .map(|(slug, identity)| query_member(slug, identity))
        .collect::<Vec<_>>();
    for batch in members.chunks(50) {
        output.push(RemainingPlannedRequest {
            origin: "gamma_markets".to_owned(),
            members: batch.to_vec(),
            parameters_json: serde_json::to_string(&markets_request(batch))?,
        });
    }
    for (slug, identity) in &sources.identities {
        if identity.ready.is_none()
            || sources.anchors.contains_key(slug)
            || !completed("gamma_markets", slug)
        {
            continue;
        }
        if !completed("gamma_event_by_slug", slug) {
            let request = EventBySlugRequest::builder().slug(slug).build();
            output.push(RemainingPlannedRequest {
                origin: "gamma_event_by_slug".to_owned(),
                members: vec![query_member(slug, identity)],
                parameters_json: serde_json::to_string(&request)?,
            });
        }
    }
    for (slug, request) in &sources.website_requests {
        if sources.identities[slug].ready.is_some()
            && !sources.anchors.contains_key(slug)
            && completed("gamma_event_by_slug", slug)
            && !completed("website_crypto_price", slug)
            && !sources.outside_history.contains(slug)
        {
            output.push(RemainingPlannedRequest {
                origin: "website_crypto_price".to_owned(),
                members: vec![query_member(slug, &sources.identities[slug])],
                parameters_json: serde_json::to_string(request)?,
            });
        }
    }
    Ok(output)
}

fn remaining_v5_reason(sources: &Sources, slug: &str) -> String {
    if slug == LEGACY_CONFLICT {
        return "legacy_fixed_identity_conflict".to_owned();
    }
    let state = sources.remaining_v5.as_ref().unwrap();
    let last = sources
        .network_attempts
        .iter()
        .rev()
        .find_map(|attempt| match attempt {
            NetworkAttempt::Remaining(attempt)
                if attempt.members.iter().any(|m| m.slug == slug) =>
            {
                Some(attempt)
            }
            _ => None,
        });
    if let Some(attempt) = last {
        if attempt.result == "in_flight" {
            return format!("{}_completion_unknown", attempt.origin);
        }
        let adopted_offline = state.offline_decode_adoptions.iter().any(|adoption| {
            attempt.response_group_id.as_deref() == Some(&adoption.response_group_id)
        });
        if attempt.result != "observed" && !adopted_offline {
            return format!("{}_{}", attempt.origin, attempt.result);
        }
        if attempt.origin == "website_crypto_price" {
            return "website_missing_open_price".to_owned();
        }
    }
    if state
        .completed
        .contains(&("gamma_event_by_slug".to_owned(), slug.to_owned()))
    {
        if sources.outside_history.contains(slug) {
            return "outside_observed_website_history_window".to_owned();
        }
        return if sources.website_requests.contains_key(slug) {
            "pending_website_query"
        } else {
            "missing_authoritative_website_config"
        }
        .to_owned();
    }
    if state
        .completed
        .contains(&("gamma_markets".to_owned(), slug.to_owned()))
    {
        return if state.returned_markets.contains(slug) {
            "pending_gamma_event_detail"
        } else {
            "gamma_market_not_returned"
        }
        .to_owned();
    }
    "pending_gamma_markets".to_owned()
}

enum RemainingCapture {
    Markets(ResponseCapture<Vec<Market>>),
    Event(Box<ResponseCapture<Event>>),
    Website(ResponseCapture<WebsiteCryptoPriceResponse>),
}

async fn capture_remaining(
    client: &Client,
    planned: &RemainingPlannedRequest,
    sources: &Sources,
) -> polymarket_client_sdk_v2::Result<RemainingCapture> {
    match planned.origin.as_str() {
        "gamma_markets" => client
            .markets_capture(&markets_request(&planned.members))
            .await
            .map(RemainingCapture::Markets),
        "gamma_event_by_slug" => client
            .event_by_slug_capture(
                &EventBySlugRequest::builder()
                    .slug(&planned.members[0].slug)
                    .build(),
            )
            .await
            .map(|capture| RemainingCapture::Event(Box::new(capture))),
        _ => client
            .website_crypto_price_capture(&sources.website_requests[&planned.members[0].slug])
            .await
            .map(RemainingCapture::Website),
    }
}

fn persist_remaining_capture(
    capture: RemainingCapture,
    output: &Path,
) -> Result<(
    HttpResponseEvidenceV1,
    polymarket_client_sdk_v2::Result<RemainingDecoded>,
)> {
    let (mut response, result, origin) = match capture {
        RemainingCapture::Markets(capture) => {
            let (response, result) = persist_capture(capture, output)?;
            (
                response,
                result.map(RemainingDecoded::Markets),
                "gamma_markets",
            )
        }
        RemainingCapture::Event(capture) => {
            let (response, result) = persist_capture(*capture, output)?;
            (
                response,
                result.map(|event| RemainingDecoded::Event(Box::new(event))),
                "gamma_event_by_slug",
            )
        }
        RemainingCapture::Website(capture) => {
            let (response, result) = persist_capture(capture, output)?;
            (
                response,
                result.map(RemainingDecoded::Website),
                "website_crypto_price",
            )
        }
    };
    response.origin = origin.to_owned();
    Ok((response, result))
}

fn complete_remaining_attempt(
    run: &mut RemainingQueryRunV1,
    journal: &mut File,
    attempt: RemainingQueryAttemptV1,
    response: Option<HttpResponseEvidenceV1>,
) -> Result<()> {
    serde_json::to_writer(
        &mut *journal,
        &RemainingCompletion {
            attempt: attempt.clone(),
            response: response.clone(),
        },
    )?;
    journal.write_all(b"\n")?;
    journal.sync_data()?;
    if let Some(response) = response {
        run.stopped_on_http_429 = response.http_status_code == Some(429);
        run.response_groups.insert(response.id()?, response);
    }
    run.network_attempts.push(attempt);
    run.in_flight_request = None;
    Ok(())
}

async fn query_remaining_v5(config: &Config, mut sources: Sources) -> Result<()> {
    fs::create_dir(&config.output_dir)?;
    fs::create_dir(config.output_dir.join("responses"))?;
    let state_path = config.output_dir.join("query-results.json");
    let mut journal = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output_dir.join("attempts.jsonl"))?;
    let mut run = RemainingQueryRunV1 {
        schema_version: "historical_price_anchor_remaining_query_v1".to_owned(),
        generator: generator(config)?,
        build_state: config.build_state.clone(),
        input_evidence_files: sources.evidence_files.clone(),
        response_groups: BTreeMap::new(),
        network_attempts: Vec::new(),
        stopped_on_http_429: false,
        unattempted_requests: remaining_plan(&sources)?,
        in_flight_request: None,
    };
    write_json_atomic(&state_path, &run)?;
    let client = Client::default();
    let mut next = Instant::now();
    while !run.unattempted_requests.is_empty() && !run.stopped_on_http_429 {
        sleep_until(next).await;
        let planned = run.unattempted_requests.remove(0);
        let mut attempt = RemainingQueryAttemptV1 {
            request_ordinal: run.network_attempts.len() as u64,
            origin: planned.origin.clone(),
            members: planned.members.clone(),
            parameters_json: planned.parameters_json.clone(),
            attempt_started_at_utc: now(),
            attempt_completed_at_utc: None,
            result: "in_flight".to_owned(),
            response_group_id: None,
            error: None,
        };
        run.in_flight_request = Some(attempt.clone());
        write_json_atomic(&state_path, &run)?;
        next = Instant::now() + QUERY_INTERVAL;
        let captured = timeout(
            QUERY_TIMEOUT,
            capture_remaining(&client, &planned, &sources),
        )
        .await;
        attempt.attempt_completed_at_utc = Some(now());
        let mut response = None;
        let mut decoded = None;
        match captured {
            Err(_) => {
                attempt.result = "timeout".to_owned();
                attempt.error = Some("SDK 请求超过 20 秒".to_owned());
            }
            Ok(Err(error)) => {
                attempt.result = "transport_error".to_owned();
                attempt.error = Some(error.to_string());
            }
            Ok(Ok(capture)) => {
                let (evidence, result) = persist_remaining_capture(capture, &config.output_dir)?;
                attempt.response_group_id = Some(evidence.id()?);
                attempt.result = if !evidence
                    .http_status_code
                    .is_some_and(|s| (200..300).contains(&s))
                {
                    "http_error"
                } else if result.is_err() {
                    "decode_error"
                } else {
                    "observed"
                }
                .to_owned();
                match result {
                    Ok(value) => decoded = Some(value),
                    Err(error) => attempt.error = Some(error.to_string()),
                }
                response = Some(evidence);
            }
        }
        complete_remaining_attempt(&mut run, &mut journal, attempt.clone(), response.clone())?;
        // 完整结果先持久化；即使身份采用失败，下一次显式导入仍能恢复这次响应。
        write_json_atomic(&state_path, &run)?;
        if let Some(value) = decoded {
            adopt_remaining_response(
                &mut sources,
                &attempt,
                attempt.response_group_id.as_ref().unwrap(),
                value,
            )?;
        }
        // 已由同一 SDK 解析的响应不再重复解码，只登记本次完成事实。
        if let Some(evidence) = response {
            let group = evidence.id()?;
            for member in &attempt.members {
                sources
                    .investigations
                    .entry(member.slug.clone())
                    .or_default()
                    .push(group.clone());
            }
            sources.responses.insert(group, evidence);
        }
        for member in &attempt.members {
            sources
                .remaining_v5
                .as_mut()
                .unwrap()
                .completed
                .insert((attempt.origin.clone(), member.slug.clone()));
        }
        println!(
            "{} {} {}",
            attempt.request_ordinal, attempt.origin, attempt.result
        );
        sources
            .network_attempts
            .push(NetworkAttempt::Remaining(attempt));
        run.unattempted_requests = remaining_plan(&sources)?;
        write_json_atomic(&state_path, &run)?;
    }
    println!(
        "已完成 {} 个请求，未执行 {} 个请求，遇 429 停止={}",
        run.network_attempts.len(),
        run.unattempted_requests.len(),
        run.stopped_on_http_429
    );
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
    let plans = if let Some(state) = &sources.remaining_v5 {
        state
            .manifests
            .iter()
            .cloned()
            .map(|m| (m, BTreeSet::new()))
            .collect::<Vec<_>>()
    } else {
        let mut days: BTreeMap<String, BTreeSet<String>> = sources
            .inventory
            .per_day
            .keys()
            .map(|day| (day.clone(), BTreeSet::new()))
            .collect();
        for ready in sources
            .identities
            .values()
            .filter_map(|identity| identity.ready)
        {
            let time = DateTime::from_timestamp_millis(ready)
                .ok_or_else(|| invalid("身份就绪时间超出范围"))?;
            let day = time.format("%Y-%m-%d").to_string();
            if !sources.inventory.per_day.contains_key(&day) {
                days.entry(day)
                    .or_default()
                    .insert(time.format("%Y-%m-%dT%H").to_string());
            }
        }
        days.into_iter()
            .map(|(day, hours)| {
                let path = config
                    .dataset_root
                    .join(&day)
                    .join("typed_raw")
                    .join(format!("utc_day={day}"))
                    .join("manifest.json");
                Ok((
                    ManifestBinding {
                        path: path_text(&path),
                        sha256: sha256_file(&path)?,
                        utc_day: day,
                        role: if hours.is_empty() {
                            "fixed_source_date"
                        } else {
                            "historical_identity_context"
                        }
                        .to_owned(),
                    },
                    hours,
                ))
            })
            .collect::<Result<Vec<_>>>()?
    };
    let mut output = IdentityInputs {
        manifests: Vec::new(),
        files: Vec::new(),
        evidence: BTreeMap::new(),
        evidence_ids: BTreeMap::new(),
    };
    let mut selected: BTreeMap<String, HistoricalIdentityRowEvidenceV1> = BTreeMap::new();
    for (binding, context_hours) in plans {
        let manifest_path = PathBuf::from(&binding.path);
        let manifest_sha256 = binding.sha256;
        let manifest: DailyManifest =
            serde_json::from_slice(&verified_bytes(&manifest_path, &manifest_sha256)?)?;
        output.manifests.push(ManifestBinding {
            path: path_text(&manifest_path),
            sha256: manifest_sha256.clone(),
            utc_day: manifest.utc_day,
            role: binding.role,
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
                    let Some(ready) = expected.ready else {
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
                        || required_time(&batch, "fixed_identity_verified_at_utc", row)? != ready
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
                        fixed_identity_verified_at_utc_ms: ready,
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
    for (index, (slug, _)) in sources
        .identities
        .iter()
        .filter(|(_, identity)| identity.ready.is_some())
        .enumerate()
    {
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
        let Some(ready) = identity.ready else {
            continue;
        };
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
            historical_identity_ready_at_utc_ms: ready,
            historical_identity_evidence_ids_json: serde_json::to_string(&[
                &identity_inputs.evidence_ids[slug]
            ])?,
        });
    }
    sort_rows(&mut rows);
    let mut remaining = Vec::new();
    for (slug, identity) in &sources.identities {
        if sources.anchors.contains_key(slug) && identity.ready.is_some() {
            continue;
        }
        let last = sources
            .network_attempts
            .iter()
            .rev()
            .filter_map(|attempt| match attempt {
                NetworkAttempt::Website(attempt) => Some(attempt),
                _ => None,
            })
            .find(|attempt| &attempt.slug == slug);
        let reason = if sources.remaining_v5.is_some() {
            remaining_v5_reason(&sources, slug)
        } else if let Some(attempt) = last {
            match attempt.result.as_str() {
                "observed" => "website_missing_open_price",
                "http_error" => "website_http_error",
                "decode_error" => "website_decode_error",
                "transport_error" => "website_transport_error",
                "timeout" => "website_timeout",
                _ => return Err(invalid("新查询结果类型不受该版本支持")),
            }
            .to_owned()
        } else if sources.outside_history.contains(slug) {
            "outside_observed_website_history_window".to_owned()
        } else {
            "pending_website_query".to_owned()
        };
        remaining.push(Remaining {
            slug: slug.clone(),
            condition_id: identity.condition_id.clone(),
            clob_market_id: identity.clob_market_id.clone(),
            reason,
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
    let mut report = serde_json::json!({
        "scope": "固定来源日期锚点补充产物，尚未接入 DPT 或模型", "build_state": config.build_state,
        "build_commit_is_baseline": config.build_state != "committed", "rows": rows.len(), "remaining": remaining.len(),
        "selected_response_groups": rows.iter().map(|r| &r.response_group_id).collect::<BTreeSet<_>>().len(),
        "at_market_start": rows.iter().filter(|r| r.historical_identity_ready_at_utc_ms <= r.market_start_at_utc_ms).count(),
        "at_original_identity_ready": rows.iter().filter(|r| r.historical_identity_ready_at_utc_ms > r.market_start_at_utc_ms).count(),
        "groups": sources.date_sets.iter().map(|(name,set)| (name.clone(), serde_json::json!({"identities":set.member_slugs.len(),
            "with_anchor":set.member_slugs.iter().filter(|s| sources.anchors.contains_key(*s)).count()}))).collect::<BTreeMap<_,_>>(),
    });
    if let Some(state) = &sources.remaining_v5 {
        report["scope"] = "额外 V5 独立锚点补充，日期归属为 unassigned，尚未接入 DPT 或模型".into();
        report["offline_decode_adoptions"] = serde_json::to_value(&state.offline_decode_adoptions)?;
    }
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
            if config.scope == Scope::RemainingV5 {
                query_remaining_v5(&config, sources).await
            } else {
                query_remaining(&config, sources).await
            }
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
    fn remaining_scope_flag_keeps_the_existing_command_shape() {
        let args = [
            "build",
            "--scope",
            "remaining-v5",
            "--evidence-root",
            "evidence",
            "--dataset-root",
            "datasets",
            "--output-dir",
            "output",
            "--build-commit",
            "test",
            "--build-state",
            "uncommitted",
            "--additional-queries",
            "first",
            "--additional-queries",
            "second",
        ];
        let config = Config::parse(args.into_iter().map(str::to_owned)).unwrap();
        assert_eq!(config.scope, Scope::RemainingV5);
        assert_eq!(
            config.additional_queries,
            [PathBuf::from("first"), PathBuf::from("second")]
        );
        let old_args = args[..1]
            .iter()
            .chain(args[3..].iter())
            .map(|s| (*s).to_owned());
        let old = Config::parse(old_args).unwrap();
        assert_eq!(old.scope, Scope::FixedDates);
        assert_eq!(old.additional_queries, [PathBuf::from("second")]);
        let explicit = args.iter().map(|s| {
            if *s == "remaining-v5" {
                "fixed-dates"
            } else {
                *s
            }
            .to_owned()
        });
        assert_eq!(
            Config::parse(explicit).unwrap().additional_queries,
            old.additional_queries
        );
    }

    const FIRST: &str = "btc-updown-5m-1785181200";
    const SECOND: &str = "btc-updown-5m-1785304800";

    fn remaining_fixture(root: &Path) -> Sources {
        let mut identities = BTreeMap::new();
        for (index, (slug, start, ready)) in [
            (
                FIRST,
                "2026-07-27T19:40:00Z",
                vec!["2026-07-27 19:40:57.836000+00:00"],
            ),
            (
                SECOND,
                "2026-07-29T06:00:00Z",
                vec!["2026-07-29 06:01:30.995000+00:00"],
            ),
            (
                LEGACY_CONFLICT,
                "2026-07-28T00:00:00Z",
                vec!["2026-07-27T23:59:00Z", "2026-07-28T00:01:00Z"],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            identities.insert(slug, serde_json::json!({"identity": {
                "condition_id": format!("0x{:064x}", index + 1), "clob_market_id": (index + 1).to_string(),
                "asset_ids": serde_json::to_string(&[(index * 2 + 1).to_string(), (index * 2 + 2).to_string()]).unwrap(),
                "market_start_utc": start,
                "market_end_utc": DateTime::from_timestamp_millis(time_ms(start).unwrap() + 300_000).unwrap().to_rfc3339(),
            }, "ready_values": ready}));
        }
        fs::write(
            root.join("remaining-market-local-identities.json"),
            serde_json::to_vec(&identities).unwrap(),
        )
        .unwrap();
        fs::write(root.join("remaining-date-evidence.json"), br#"{"days":{"2026-07-27":{"manifest_path":"/original/2026-07-27-deploy-ec92bd3999cc/typed_raw/utc_day=2026-07-27/manifest.json","manifest_sha256":"original-manifest"}}}"#).unwrap();
        load_remaining_sources(&Config {
            action: "build".to_owned(),
            scope: Scope::RemainingV5,
            evidence_root: root.to_owned(),
            dataset_root: root.join("not-the-original-root"),
            output_dir: root.join("output"),
            additional_queries: Vec::new(),
            build_commit: "test".to_owned(),
            build_state: "test".to_owned(),
        })
        .unwrap()
    }

    fn remaining_attempt(sources: &Sources, origin: &str) -> RemainingQueryAttemptV1 {
        let members = sources
            .identities
            .iter()
            .map(|(slug, id)| query_member(slug, id))
            .collect::<Vec<_>>();
        RemainingQueryAttemptV1 {
            request_ordinal: 0,
            origin: origin.to_owned(),
            parameters_json: serde_json::to_string(&markets_request(&members)).unwrap(),
            members,
            attempt_started_at_utc: "2026-09-08T07:00:00Z".to_owned(),
            attempt_completed_at_utc: None,
            result: "in_flight".to_owned(),
            response_group_id: None,
            error: None,
        }
    }

    fn test_response(
        root: &Path,
        origin: &str,
        body: &[u8],
        status: u16,
    ) -> HttpResponseEvidenceV1 {
        let body_path = root.join(format!("{}.json", hash_bytes(body)));
        fs::write(&body_path, body).unwrap();
        HttpResponseEvidenceV1 {
            origin: origin.to_owned(),
            request_url: "https://gamma-api.polymarket.com/markets?closed=true".to_owned(),
            received_at_utc: "2026-09-08T07:00:01Z".to_owned(),
            received_at_utc_ms: time_ms("2026-09-08T07:00:01Z").unwrap(),
            receipt_observation: "sdk_body_received".to_owned(),
            response_body_path: format!("responses/{}.json", hash_bytes(body)),
            response_body_sha256: hash_bytes(body),
            original_response_file: path_text(&body_path),
            http_status_code: Some(status),
            response_url: Some("https://gamma-api.polymarket.com/markets?closed=true".to_owned()),
        }
    }

    fn test_query_run(attempt: RemainingQueryAttemptV1) -> RemainingQueryRunV1 {
        RemainingQueryRunV1 {
            schema_version: "historical_price_anchor_remaining_query_v1".to_owned(),
            generator: Generator {
                build_commit: "test".to_owned(),
                binary_sha256: "test".to_owned(),
            },
            build_state: "test".to_owned(),
            input_evidence_files: Vec::new(),
            response_groups: BTreeMap::new(),
            network_attempts: Vec::new(),
            stopped_on_http_429: false,
            unattempted_requests: Vec::new(),
            in_flight_request: Some(attempt),
        }
    }

    #[test]
    fn recovered_completion_retains_429_and_ignores_only_the_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let sources = remaining_fixture(dir.path());
        let mut attempt = remaining_attempt(&sources, "gamma_markets");
        let mut run = test_query_run(attempt.clone());
        let state_path = dir.path().join("query-results.json");
        write_json_atomic(&state_path, &run).unwrap();
        let response = test_response(
            dir.path(),
            "gamma_markets",
            br#"{"error":"rate limited"}"#,
            429,
        );
        attempt.result = "http_error".to_owned();
        attempt.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
        attempt.response_group_id = Some(response.id().unwrap());
        let mut journal = File::create(dir.path().join("attempts.jsonl")).unwrap();
        complete_remaining_attempt(&mut run, &mut journal, attempt, Some(response)).unwrap();
        assert!(run.stopped_on_http_429);
        journal
            .write_all(br#"{"attempt":{"request_ordinal":1"#)
            .unwrap();
        journal.sync_all().unwrap();
        let original = fs::read(dir.path().join("attempts.jsonl")).unwrap();
        let recovered = read_remaining_run(&state_path).unwrap();
        assert_eq!(recovered.network_attempts.len(), 1);
        assert_eq!(recovered.response_groups.len(), 1);
        assert!(recovered.in_flight_request.is_none());
        assert!(
            recovered.stopped_on_http_429,
            "完整 429 日志必须恢复整个网络段停止事实"
        );
        assert_eq!(
            fs::read(dir.path().join("attempts.jsonl")).unwrap(),
            original
        );
    }

    #[test]
    fn offline_adoption_keeps_the_old_decode_error_but_missing_members_advance_to_detail() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = remaining_fixture(dir.path());
        let id = &sources.identities[FIRST];
        let body = format!(
            r#"[{{"id":"{}","slug":"{FIRST}","conditionId":"{}","clobTokenIds":{},"events":[{{"id":"1","slug":"{FIRST}","eventMetadata":{{"finalPrice":99}}}}]}}]"#,
            id.clob_market_id,
            id.condition_id,
            serde_json::to_string(&id.asset_ids_json).unwrap()
        );
        let response = test_response(dir.path(), "gamma_markets", body.as_bytes(), 200);
        let mut attempt = remaining_attempt(&sources, "gamma_markets");
        attempt.result = "decode_error".to_owned();
        attempt.error = Some("original feature-specific SDK error".to_owned());
        attempt.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
        attempt.response_group_id = Some(response.id().unwrap());
        import_remaining_attempt(&mut sources, attempt, Some(response), true).unwrap();
        assert!(sources.anchors.is_empty(), "finalPrice 不能成为锚点");
        let NetworkAttempt::Remaining(original) = &sources.network_attempts[0] else {
            panic!()
        };
        assert_eq!(original.result, "decode_error");
        assert_eq!(
            original.error.as_deref(),
            Some("original feature-specific SDK error")
        );
        assert_eq!(
            remaining_v5_reason(&sources, FIRST),
            "pending_gamma_event_detail"
        );
        assert_eq!(
            remaining_v5_reason(&sources, LEGACY_CONFLICT),
            "legacy_fixed_identity_conflict"
        );
        let plan = remaining_plan(&sources).unwrap();
        assert_eq!(plan.len(), 2);
        assert!(
            plan.iter()
                .all(|request| request.origin == "gamma_event_by_slug")
        );
    }

    fn market_body(sources: &Sources, slug: &str, events: &str) -> String {
        let id = &sources.identities[slug];
        format!(
            r#"{{"id":"{}","slug":"{slug}","conditionId":"{}","clobTokenIds":{},"events":{events}}}"#,
            id.clob_market_id,
            id.condition_id,
            serde_json::to_string(&id.asset_ids_json).unwrap()
        )
    }

    #[test]
    fn markets_keep_original_member_and_event_indices_and_exclude_the_legacy_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = remaining_fixture(dir.path());
        let first = market_body(
            &sources,
            FIRST,
            &format!(
                r#"[{{"id":"8","slug":"another-event","eventMetadata":{{"priceToBeat":999}}}},{{"id":"9","slug":"{FIRST}","eventMetadata":{{"priceToBeat":64950.014218511440000000000001,"finalPrice":1}}}}]"#
            ),
        );
        let second = market_body(
            &sources,
            SECOND,
            &format!(
                r#"[{{"id":"10","slug":"{SECOND}","eventMetadata":{{"priceToBeat":"7.100e-18"}}}}]"#
            ),
        );
        let conflict = market_body(
            &sources,
            LEGACY_CONFLICT,
            &format!(
                r#"[{{"id":"11","slug":"{LEGACY_CONFLICT}","eventMetadata":{{"priceToBeat":2}}}}]"#
            ),
        );
        let body = format!("[{second},{conflict},{first}]");
        let response = test_response(dir.path(), "gamma_markets", body.as_bytes(), 200);
        let group = response.id().unwrap();
        let mut attempt = remaining_attempt(&sources, "gamma_markets");
        attempt.result = "observed".to_owned();
        attempt.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
        attempt.response_group_id = Some(group.clone());
        import_remaining_attempt(&mut sources, attempt.clone(), Some(response.clone()), false)
            .unwrap();
        import_remaining_attempt(&mut sources, attempt, Some(response), false).unwrap();
        assert_eq!(
            sources.network_attempts.len(),
            1,
            "重复导入同一实际组不造新请求"
        );
        assert_eq!(sources.responses.len(), 1);
        assert_eq!(sources.anchors.len(), 2);
        assert_eq!(sources.anchors[FIRST].raw, "64950.014218511440000000000001");
        assert_eq!(
            sources.anchors[FIRST].value_path,
            "/2/events/1/eventMetadata/priceToBeat"
        );
        assert_eq!(sources.anchors[FIRST].member, 2);
        assert_eq!(sources.anchors[SECOND].raw, "7.100e-18");
        assert_eq!(sources.anchors[SECOND].member, 0);
        assert!(sources.anchors.values().all(|anchor| anchor.group == group));
        assert!(sources.identities[LEGACY_CONFLICT].ready.is_none());
        assert!(!sources.anchors.contains_key(LEGACY_CONFLICT));
        assert_eq!(
            sources.identities[FIRST].ready,
            Some(time_ms("2026-07-27T19:40:57.836Z").unwrap())
        );
        assert_eq!(
            sources.identities[SECOND].ready,
            Some(time_ms("2026-07-29T06:01:30.995Z").unwrap())
        );
        assert_eq!(
            sources.date_sets.keys().collect::<Vec<_>>(),
            [&"unassigned".to_owned()]
        );
        assert_eq!(sources.date_sets["unassigned"].member_slugs.len(), 3);
        let binding = &sources.remaining_v5.as_ref().unwrap().manifests[0];
        assert_eq!(
            binding.path,
            "/original/2026-07-27-deploy-ec92bd3999cc/typed_raw/utc_day=2026-07-27/manifest.json"
        );
        assert_eq!(binding.role, "unassigned_source_date");
        assert!(remaining_plan(&sources).unwrap().is_empty());
    }

    #[test]
    fn explicit_resume_distinguishes_completed_batches_from_unknown_in_flight_requests() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = remaining_fixture(dir.path());
        let initial = remaining_plan(&sources).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&initial[0].parameters_json).unwrap(),
            serde_json::json!({"limit":50,"id":["1","3","2"],"closed":true})
        );
        let mut batch = remaining_attempt(&sources, "gamma_markets");
        batch.result = "observed".to_owned();
        batch.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
        let response = test_response(dir.path(), "gamma_markets", b"[]", 200);
        batch.response_group_id = Some(response.id().unwrap());
        let first_dir = dir.path().join("first-run");
        fs::create_dir(&first_dir).unwrap();
        let mut completed_run = test_query_run(batch.clone());
        let mut journal = File::create(first_dir.join("attempts.jsonl")).unwrap();
        write_json_atomic(&first_dir.join("query-results.json"), &completed_run).unwrap();
        complete_remaining_attempt(&mut completed_run, &mut journal, batch, Some(response))
            .unwrap();
        import_remaining_queries(&mut sources, &first_dir).unwrap();
        let plan = remaining_plan(&sources).unwrap();
        assert_eq!(plan.len(), 2);
        assert!(
            plan.iter()
                .all(|request| request.origin == "gamma_event_by_slug")
        );

        let mut pending = remaining_attempt(&sources, "gamma_event_by_slug");
        pending.members.retain(|member| member.slug == FIRST);
        pending.parameters_json = "{}".to_owned();
        let second_dir = dir.path().join("second-run");
        fs::create_dir(&second_dir).unwrap();
        let mut unknown_run = test_query_run(pending.clone());
        unknown_run.unattempted_requests = plan
            .into_iter()
            .filter(|request| request.members[0].slug == SECOND)
            .collect();
        write_json_atomic(&second_dir.join("query-results.json"), &unknown_run).unwrap();
        fs::write(
            second_dir.join("attempts.jsonl"),
            br#"{"attempt":{"request_ordinal":0"#,
        )
        .unwrap();
        import_remaining_queries(&mut sources, &second_dir).unwrap();
        assert_eq!(
            sources.network_attempts.len(),
            2,
            "不同目录的 ordinal=0 是不同尝试"
        );
        assert_eq!(
            remaining_v5_reason(&sources, FIRST),
            "gamma_event_by_slug_completion_unknown"
        );
        assert_eq!(
            remaining_v5_reason(&sources, SECOND),
            "gamma_market_not_returned"
        );
        assert_eq!(
            remaining_plan(&sources).unwrap().len(),
            2,
            "显式新段可安排未完成详情，不能重发完成批次"
        );
        let recovered = read_remaining_run(&second_dir.join("query-results.json")).unwrap();
        assert!(recovered.network_attempts.is_empty());
        assert!(recovered.in_flight_request.is_some());
        assert_eq!(recovered.unattempted_requests.len(), 1);

        pending.result = "transport_error".to_owned();
        pending.attempt_completed_at_utc = Some("2026-09-08T07:00:03Z".to_owned());
        pending.error = Some("connection closed".to_owned());
        import_remaining_attempt(&mut sources, pending, None, false).unwrap();
        let plan = remaining_plan(&sources).unwrap();
        assert_eq!(plan.len(), 1, "已完成错误也不自动重发");
        assert_eq!(plan[0].members[0].slug, SECOND);
        assert_eq!(
            remaining_v5_reason(&sources, FIRST),
            "gamma_event_by_slug_transport_error"
        );
        fs::write(second_dir.join("attempts.jsonl"), b"not-json\n{}\n").unwrap();
        assert!(read_remaining_run(&second_dir.join("query-results.json")).is_err());
    }

    #[test]
    fn gamma_anchor_precedes_website_value_in_either_import_order() {
        for website_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut sources = remaining_fixture(dir.path());
            let mut website = remaining_attempt(&sources, "website_crypto_price");
            website.members.retain(|member| member.slug == FIRST);
            website.result = "observed".to_owned();
            website.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
            let website_response = test_response(
                dir.path(),
                "website_crypto_price",
                br#"{"openPrice":1.001}"#,
                200,
            );
            website.response_group_id = Some(website_response.id().unwrap());
            let market = market_body(
                &sources,
                FIRST,
                &format!(
                    r#"[{{"id":"1","slug":"{FIRST}","eventMetadata":{{"priceToBeat":"2.002"}}}}]"#
                ),
            );
            let mut gamma = remaining_attempt(&sources, "gamma_markets");
            gamma.result = "observed".to_owned();
            gamma.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
            let gamma_response = test_response(
                dir.path(),
                "gamma_markets",
                format!("[{market}]").as_bytes(),
                200,
            );
            let gamma_group = gamma_response.id().unwrap();
            gamma.response_group_id = Some(gamma_group.clone());
            let mut inputs = vec![(gamma, gamma_response), (website, website_response)];
            if website_first {
                inputs.reverse();
            }
            for (attempt, response) in inputs {
                import_remaining_attempt(&mut sources, attempt, Some(response), false).unwrap();
            }
            assert_eq!(sources.anchors[FIRST].raw, "2.002");
            assert_eq!(sources.anchors[FIRST].group, gamma_group);
            assert_eq!(sources.responses.len(), 2, "未采用的官网原响应仍留存");
        }
    }

    #[test]
    fn website_fallback_uses_actual_config_and_waits_for_the_completed_gamma_detail() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = remaining_fixture(dir.path());
        let id = &sources.identities[FIRST];
        for (twap, window) in [(true, 30), (true, 60), (false, 0)] {
            let body = format!(
                r#"{{"id":"1","cryptoMarketConfig":{{"asset":"btc","duration":"5m","twapEnabled":{twap},"twapLookbackSeconds":{window}}}}}"#
            );
            let market: Market = decode(body.as_bytes(), "test").unwrap();
            let request = website_request_for_identity(&market, id).unwrap();
            assert_eq!(request.symbol, "BTC");
            assert_eq!(request.variant, "fiveminute");
            assert_eq!(request.twap_enabled, twap);
            assert_eq!(request.twap_lookback_seconds, window);
            assert_eq!(request.event_start_time.timestamp_millis(), id.start);
            assert_eq!(request.end_date.timestamp_millis(), id.end);
        }
        let absent: Market = decode(br#"{"id":"1"}"#, "test").unwrap();
        assert!(website_request_for_identity(&absent, id).is_none());
        let mut batch = remaining_attempt(&sources, "gamma_markets");
        batch.result = "observed".to_owned();
        batch.attempt_completed_at_utc = Some("2026-09-08T07:00:02Z".to_owned());
        let response = test_response(dir.path(), "gamma_markets", b"[]", 200);
        batch.response_group_id = Some(response.id().unwrap());
        import_remaining_attempt(&mut sources, batch, Some(response), false).unwrap();
        let market = market_body(&sources, FIRST, "[]");
        let market = market.strip_suffix('}').unwrap();
        let body = format!(
            r#"{{"id":"1","slug":"{FIRST}","markets":[{market},"resolutionSource":"https://data.chain.link/streams/btc-usd","cryptoMarketConfig":{{"asset":"btc","duration":"5m","twapEnabled":false}}}}]}}"#
        );
        let mut detail = remaining_attempt(&sources, "gamma_event_by_slug");
        detail.members.retain(|member| member.slug == FIRST);
        detail.result = "observed".to_owned();
        detail.attempt_completed_at_utc = Some("2026-09-08T07:00:04Z".to_owned());
        let response = test_response(dir.path(), "gamma_event_by_slug", body.as_bytes(), 200);
        detail.response_group_id = Some(response.id().unwrap());
        import_remaining_attempt(&mut sources, detail, Some(response), false).unwrap();
        assert!(
            remaining_plan(&sources)
                .unwrap()
                .iter()
                .any(|request| request.origin == "website_crypto_price"
                    && request.members[0].slug == FIRST)
        );
        sources
            .remaining_v5
            .as_mut()
            .unwrap()
            .history_limit_observed_at = Some(time_ms("2026-09-08T04:00:00Z").unwrap());
        refresh_history_limit(&mut sources);
        assert_eq!(
            remaining_v5_reason(&sources, FIRST),
            "outside_observed_website_history_window"
        );
        assert!(
            !remaining_plan(&sources)
                .unwrap()
                .iter()
                .any(|request| request.origin == "website_crypto_price")
        );
    }

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
