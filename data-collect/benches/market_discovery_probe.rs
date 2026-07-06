use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data_collect::collector_identity::resolve_collector_region;
use data_collect::market_discovery::{
    BtcUpdownDiscoveryPair, BtcUpdownMarketCandidate, DiscoverySelectionError, MarketWindow,
    build_market_subscription_request_json, discovery_windows, select_btc_updown_pair, sha256_hex,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use serde::Serialize;
use tokio::time::sleep;

const DEFAULT_NEXT_WAIT_SECS: u64 = 90;
const DEFAULT_POLL_INTERVAL_SECS: u64 = 10;
const GAMMA_MARKETS_ENDPOINT: &str = "gamma.markets";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("market_discovery_probe_error={err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let artifact_dir = artifact_dir()?;
    fs::create_dir_all(&artifact_dir)?;

    let run_id = env_or_default(
        "DISCOVERY_RUN_ID",
        format!(
            "phase0-market-discovery-{}-{}",
            now_ms(),
            std::process::id()
        ),
    );
    let collector_host_id = collector_host_id();
    let collector_region = resolve_collector_region().await;
    let clock_sync_status = env_or_default("CLOCK_SYNC_STATUS", "not_checked_by_probe".to_owned());
    let source_config_digest =
        sha256_hex(b"gamma_exact_slug:btc-updown-5m:current_next:closed_false:limit_10");
    let max_wait = env_u64("MARKET_DISCOVERY_NEXT_WAIT_SECS", DEFAULT_NEXT_WAIT_SECS);
    let poll_interval = env_u64(
        "MARKET_DISCOVERY_POLL_INTERVAL_SECS",
        DEFAULT_POLL_INTERVAL_SECS,
    );

    let gamma = GammaClient::default();
    let started_ms = now_ms();
    let mut attempts = Vec::new();
    let mut final_pair = None;
    let mut final_error = None;

    loop {
        let attempt_started_ms = now_ms();
        let windows = discovery_windows(attempt_started_ms);
        let current_query = fetch_exact_slug(&gamma, &windows.current).await;
        let next_query = fetch_exact_slug(&gamma, &windows.next).await;

        let current_candidates = current_query
            .markets
            .iter()
            .map(BtcUpdownMarketCandidate::from_sdk_market)
            .collect::<Vec<_>>();
        let next_candidates = next_query
            .markets
            .iter()
            .map(BtcUpdownMarketCandidate::from_sdk_market)
            .collect::<Vec<_>>();
        let selection = if current_query.error.is_none() && next_query.error.is_none() {
            select_btc_updown_pair(attempt_started_ms, &current_candidates, &next_candidates)
        } else {
            Err(DiscoverySelectionError::MissingCurrentCandidate {
                expected_slug: windows.current.slug.clone(),
            })
        };

        match selection {
            Ok(pair) => {
                attempts.push(DiscoveryAttempt {
                    attempt_started_utc_ms: attempt_started_ms,
                    attempt_recv_time_utc_ms: now_ms(),
                    windows,
                    current_query,
                    next_query,
                    selection_error: None,
                });
                final_pair = Some(pair);
                break;
            }
            Err(err) => {
                let source_gap = format!("{err:?}");
                attempts.push(DiscoveryAttempt {
                    attempt_started_utc_ms: attempt_started_ms,
                    attempt_recv_time_utc_ms: now_ms(),
                    windows,
                    current_query,
                    next_query,
                    selection_error: Some(source_gap.clone()),
                });
                final_error = Some(source_gap);
            }
        }

        if now_ms().saturating_sub(started_ms) >= (max_wait * 1000) as i64 {
            break;
        }
        sleep(Duration::from_secs(poll_interval)).await;
    }

    let subscription_json = final_pair
        .as_ref()
        .map(|pair| {
            build_market_subscription_request_json(&[
                pair.current.up_asset_id.clone(),
                pair.current.down_asset_id.clone(),
                pair.next.up_asset_id.clone(),
                pair.next.down_asset_id.clone(),
            ])
        })
        .transpose()?;
    let subscription_json_sha256 = subscription_json
        .as_deref()
        .map(|json| sha256_hex(json.as_bytes()));

    let receipt = DiscoveryReceipt {
        discovery_run_id: run_id,
        target: "polymarket_btc_updown_5m".to_owned(),
        collector_host_id,
        collector_region,
        clock_sync_status,
        source_config_digest,
        fetch_start_utc_ms: started_ms,
        recv_time_utc_ms: now_ms(),
        endpoint: GAMMA_MARKETS_ENDPOINT.to_owned(),
        attempts,
        pair: final_pair.clone(),
        subscription_json,
        subscription_json_sha256,
        source_gaps: final_error.iter().cloned().collect(),
        verdict: if final_pair.is_some() {
            "PASS".to_owned()
        } else {
            "SOURCE_GAP".to_owned()
        },
    };

    let receipt_path = artifact_dir.join("market_discovery_receipt.json");
    fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
    println!("market_discovery_receipt={}", receipt_path.display());
    println!("market_discovery_verdict={}", receipt.verdict);

    if receipt.verdict == "PASS" {
        Ok(())
    } else {
        Err(format!(
            "btc-updown-5m discovery did not produce current and next markets; receipt={}",
            receipt_path.display()
        )
        .into())
    }
}

async fn fetch_exact_slug(gamma: &GammaClient, window: &MarketWindow) -> QueryReceipt {
    let fetch_start_utc_ms = now_ms();
    let mut request = MarketsRequest::default();
    request.slug = vec![window.slug.clone()];
    request.closed = Some(false);
    request.limit = Some(10);

    let query_params_json =
        serde_json::to_string(&request).unwrap_or_else(|err| format!("serialize_error:{err}"));

    match gamma.markets(&request).await {
        Ok(markets) => {
            let raw_markets = markets
                .iter()
                .map(CandidateRawEvidence::from_sdk_market)
                .collect::<Vec<_>>();
            let raw_json = serde_json::to_vec(&raw_markets).unwrap_or_default();
            let query_params_sha256 = sha256_hex(query_params_json.as_bytes());
            QueryReceipt {
                expected_slug: window.slug.clone(),
                fetch_start_utc_ms,
                recv_time_utc_ms: now_ms(),
                endpoint: GAMMA_MARKETS_ENDPOINT.to_owned(),
                query_params_json,
                query_params_sha256,
                raw_json_sha256: sha256_hex(&raw_json),
                row_count: markets.len(),
                markets,
                raw_markets,
                error: None,
            }
        }
        Err(err) => {
            let query_params_sha256 = sha256_hex(query_params_json.as_bytes());
            QueryReceipt {
                expected_slug: window.slug.clone(),
                fetch_start_utc_ms,
                recv_time_utc_ms: now_ms(),
                endpoint: GAMMA_MARKETS_ENDPOINT.to_owned(),
                query_params_json,
                query_params_sha256,
                raw_json_sha256: sha256_hex(b"[]"),
                row_count: 0,
                markets: Vec::new(),
                raw_markets: Vec::new(),
                error: Some(err.to_string()),
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct DiscoveryReceipt {
    discovery_run_id: String,
    target: String,
    collector_host_id: String,
    collector_region: String,
    clock_sync_status: String,
    source_config_digest: String,
    fetch_start_utc_ms: i64,
    recv_time_utc_ms: i64,
    endpoint: String,
    attempts: Vec<DiscoveryAttempt>,
    pair: Option<BtcUpdownDiscoveryPair>,
    subscription_json: Option<String>,
    subscription_json_sha256: Option<String>,
    source_gaps: Vec<String>,
    verdict: String,
}

#[derive(Debug, Serialize)]
struct DiscoveryAttempt {
    attempt_started_utc_ms: i64,
    attempt_recv_time_utc_ms: i64,
    windows: data_collect::market_discovery::DiscoveryWindows,
    current_query: QueryReceipt,
    next_query: QueryReceipt,
    selection_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryReceipt {
    expected_slug: String,
    fetch_start_utc_ms: i64,
    recv_time_utc_ms: i64,
    endpoint: String,
    query_params_json: String,
    query_params_sha256: String,
    raw_json_sha256: String,
    row_count: usize,
    #[serde(skip_serializing)]
    markets: Vec<SdkGammaMarket>,
    raw_markets: Vec<CandidateRawEvidence>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct CandidateRawEvidence {
    market_id: String,
    slug: Option<String>,
    condition_id: Option<String>,
    clob_token_ids: Option<Vec<String>>,
    active: Option<bool>,
    accepting_orders: Option<bool>,
    start_ts_s: Option<i64>,
    end_ts_s: Option<i64>,
    raw_json_sha256: String,
    raw_json: String,
}

impl CandidateRawEvidence {
    fn from_sdk_market(market: &SdkGammaMarket) -> Self {
        let raw_json =
            serde_json::to_string(market).unwrap_or_else(|err| format!("serialize_error:{err}"));
        Self {
            market_id: market.id.clone(),
            slug: market.slug.clone(),
            condition_id: market
                .condition_id
                .as_ref()
                .map(|condition_id| format!("{condition_id:#x}")),
            clob_token_ids: market
                .clob_token_ids
                .as_ref()
                .map(|ids| ids.iter().map(ToString::to_string).collect()),
            active: market.active,
            accepting_orders: market.accepting_orders,
            start_ts_s: market.start_date.map(|dt| dt.timestamp()),
            end_ts_s: market.end_date.map(|dt| dt.timestamp()),
            raw_json_sha256: sha256_hex(raw_json.as_bytes()),
            raw_json,
        }
    }
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
    std::env::var("PHASE0_DISCOVERY_ARTIFACT_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::current_dir().map(|cwd| cwd.join("market-discovery-artifacts")))
        .map_err(Into::into)
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
