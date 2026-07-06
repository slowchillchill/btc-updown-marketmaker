use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt as _;
use polymarket_client_sdk_v2::clob::ws::{ChannelType, Client as MarketWsClient};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::rtds::{Client as RtdsClient, Subscription, SubscriptionRequest};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config;
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use tokio::time::{sleep, timeout};

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 20;
const DEFAULT_EVENT_TIMEOUT_SECS: u64 = 20;
const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";
const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("websocket_smoke_probe_error={err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let connect_timeout = env_duration(
        "WS_SMOKE_CONNECT_TIMEOUT_SECS",
        DEFAULT_CONNECT_TIMEOUT_SECS,
    );
    let event_timeout = env_duration("WS_SMOKE_EVENT_TIMEOUT_SECS", DEFAULT_EVENT_TIMEOUT_SECS);

    println!("probe=websocket_smoke_probe");
    println!("sdk_crate=polymarket_client_sdk_v2");
    println!("sdk_version=0.6.0");
    println!("recv_time_utc_ms={}", now_ms());

    let asset = discover_market_asset().await?;
    println!(
        "market_metadata=OK market_id={} slug={} active={} accepting_orders={} asset_id={}",
        asset.market_id,
        asset.slug.as_deref().unwrap_or("unknown"),
        asset
            .active
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        asset
            .accepting_orders
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        asset.asset_id
    );

    smoke_market_ws(asset.asset_id, connect_timeout, event_timeout).await?;
    smoke_rtds_ws(connect_timeout, event_timeout).await?;

    println!("overall_websocket_smoke=PASS");
    Ok(())
}

async fn discover_market_asset() -> Result<MarketAsset, Box<dyn Error>> {
    let gamma = GammaClient::default();
    let markets = gamma
        .markets(&MarketsRequest::builder().closed(false).limit(50).build())
        .await?;

    let selected = markets
        .iter()
        .filter(|market| {
            market
                .clob_token_ids
                .as_ref()
                .is_some_and(|ids| !ids.is_empty())
        })
        .find(|market| {
            let slug = market.slug.as_deref().unwrap_or_default().to_lowercase();
            let question = market
                .question
                .as_deref()
                .unwrap_or_default()
                .to_lowercase();
            slug.contains("btc") || slug.contains("bitcoin") || question.contains("bitcoin")
        })
        .or_else(|| {
            markets.iter().find(|market| {
                market
                    .clob_token_ids
                    .as_ref()
                    .is_some_and(|ids| !ids.is_empty())
            })
        })
        .ok_or("gamma returned no open market with clob_token_ids")?;

    let asset_id = selected
        .clob_token_ids
        .as_ref()
        .and_then(|ids| ids.first().copied())
        .ok_or("selected market has no clob token ids")?;

    Ok(MarketAsset {
        market_id: selected.id.clone(),
        slug: selected.slug.clone(),
        active: selected.active,
        accepting_orders: selected.accepting_orders,
        asset_id,
    })
}

async fn smoke_market_ws(
    asset_id: U256,
    connect_timeout: Duration,
    event_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let request = polymarket_client_sdk_v2::clob::ws::SubscriptionRequest::market(vec![asset_id])
        .with_custom_features(true);
    println!(
        "market_subscription_json={}",
        serde_json::to_string(&request)?
    );

    let client = MarketWsClient::new(MARKET_WS_ENDPOINT, bounded_ws_config())?;
    let mut stream = Box::pin(client.subscribe_orderbook(vec![asset_id])?);

    wait_market_connected(&client, connect_timeout).await?;
    println!(
        "market_ws_connection_state={:?}",
        client.connection_state(ChannelType::Market)
    );
    println!(
        "market_ws_subscription_count={}",
        client.subscription_count()
    );

    match timeout(event_timeout, stream.next()).await {
        Ok(Some(Ok(book))) => {
            println!(
                "market_ws_business_message=PASS event_type=book asset_id={} source_ts={} bids={} asks={} recv_time_utc_ms={}",
                book.asset_id,
                book.timestamp,
                book.bids.len(),
                book.asks.len(),
                now_ms()
            );
        }
        Ok(Some(Err(err))) => return Err(format!("market ws stream error: {err}").into()),
        Ok(None) => return Err("market ws stream closed before message".into()),
        Err(_) => {
            println!(
                "market_ws_business_message=NO_BUSINESS_MESSAGE timeout_ms={}",
                event_timeout.as_millis()
            );
        }
    }

    println!("market_ws_smoke=PASS");
    Ok(())
}

async fn smoke_rtds_ws(
    connect_timeout: Duration,
    event_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let request = SubscriptionRequest::subscribe(vec![subscription]);
    println!(
        "rtds_subscription_json={}",
        serde_json::to_string(&request)?
    );

    let client = RtdsClient::new(RTDS_WS_ENDPOINT, bounded_ws_config())?;
    let mut stream = Box::pin(client.subscribe_chainlink_prices(Some("btc/usd".to_owned()))?);

    wait_rtds_connected(&client, connect_timeout).await?;
    println!("rtds_ws_connection_state={:?}", client.connection_state());
    println!("rtds_ws_subscription_count={}", client.subscription_count());

    match timeout(event_timeout, stream.next()).await {
        Ok(Some(Ok(price))) => {
            println!(
                "rtds_ws_business_message=PASS topic=crypto_prices_chainlink symbol={} source_ts={} value={} recv_time_utc_ms={}",
                price.symbol,
                price.timestamp,
                price.value,
                now_ms()
            );
        }
        Ok(Some(Err(err))) => return Err(format!("rtds ws stream error: {err}").into()),
        Ok(None) => return Err("rtds ws stream closed before message".into()),
        Err(_) => {
            println!(
                "rtds_ws_business_message=NO_BUSINESS_MESSAGE timeout_ms={}",
                event_timeout.as_millis()
            );
        }
    }

    println!("rtds_ws_smoke=PASS");
    Ok(())
}

async fn wait_market_connected(
    client: &MarketWsClient,
    timeout_duration: Duration,
) -> Result<(), Box<dyn Error>> {
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
    .map_err(|_| {
        format!(
            "market ws did not reach connected state within {}ms",
            timeout_duration.as_millis()
        )
    })?;
    Ok(())
}

async fn wait_rtds_connected(
    client: &RtdsClient,
    timeout_duration: Duration,
) -> Result<(), Box<dyn Error>> {
    timeout(timeout_duration, async {
        loop {
            if matches!(client.connection_state(), ConnectionState::Connected { .. }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "rtds ws did not reach connected state within {}ms",
            timeout_duration.as_millis()
        )
    })?;
    Ok(())
}

fn bounded_ws_config() -> Config {
    let mut config = Config::default();
    config.reconnect.max_attempts = Some(1);
    config.reconnect.max_backoff = Duration::from_secs(2);
    config.heartbeat_interval = Duration::from_secs(5);
    config.heartbeat_timeout = Duration::from_secs(10);
    config
}

fn env_duration(key: &str, default_secs: u64) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or_else(|| Duration::from_secs(default_secs), Duration::from_secs)
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

struct MarketAsset {
    market_id: String,
    slug: Option<String>,
    active: Option<bool>,
    accepting_orders: Option<bool>,
    asset_id: U256,
}
