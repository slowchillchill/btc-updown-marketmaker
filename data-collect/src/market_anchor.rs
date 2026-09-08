use base64::Engine as _;
use polymarket_client_sdk_v2::gamma::types::request::WebsiteCryptoPriceRequest;
use polymarket_client_sdk_v2::gamma::types::response::{
    Event, Market, ResponseCapture, WebsiteCryptoPriceResponse,
};
use polymarket_client_sdk_v2::types::DateTime;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnchorIdentity {
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub asset_ids: String,
    pub market_start_utc: i64,
    pub market_end_utc: i64,
    pub fixed_identity_verified_at_utc: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorOrigin {
    GammaEventBySlug,
    WebsiteCryptoPrice,
}

impl MarketAnchorOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GammaEventBySlug => "gamma_event_by_slug",
            Self::WebsiteCryptoPrice => "website_crypto_price",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorQueryResult {
    Observed,
    HttpError,
    DecodeError,
    TransportError,
    Timeout,
}

/// 一份实际响应中的锚点与规则事实；空锚点表示本次未提供，不删除先前值。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnchorObservation {
    pub anchor_query_result: MarketAnchorQueryResult,
    pub anchor_event_id: Option<String>,
    pub price_to_beat_decimal: Option<String>,
    pub price_to_beat_source_field: Option<String>,
    pub anchor_source_computed_at_utc: Option<i64>,
    pub crypto_market_config_id: Option<String>,
    pub crypto_market_config_nested_id: Option<String>,
    pub crypto_market_config_asset: Option<String>,
    pub crypto_market_config_duration: Option<String>,
    pub twap_enabled: Option<bool>,
    pub twap_lookback_seconds: Option<u32>,
    pub resolution_source: Option<String>,
    pub http_request_url: Option<String>,
    pub http_response_url: Option<String>,
    pub http_status_code: Option<u16>,
    pub http_received_at_utc: Option<i64>,
    pub http_body_encoding: Option<String>,
    pub http_body: Option<String>,
    pub http_body_sha256: Option<String>,
}

impl MarketAnchorObservation {
    pub fn unavailable(result: MarketAnchorQueryResult) -> Self {
        Self {
            anchor_query_result: result,
            anchor_event_id: None,
            price_to_beat_decimal: None,
            price_to_beat_source_field: None,
            anchor_source_computed_at_utc: None,
            crypto_market_config_id: None,
            crypto_market_config_nested_id: None,
            crypto_market_config_asset: None,
            crypto_market_config_duration: None,
            twap_enabled: None,
            twap_lookback_seconds: None,
            resolution_source: None,
            http_request_url: None,
            http_response_url: None,
            http_status_code: None,
            http_received_at_utc: None,
            http_body_encoding: None,
            http_body: None,
            http_body_sha256: None,
        }
    }

    fn capture<T>(capture: ResponseCapture<T>) -> (Self, polymarket_client_sdk_v2::Result<T>) {
        let mut observation = Self::unavailable(if !capture.status_code.is_success() {
            MarketAnchorQueryResult::HttpError
        } else if capture.result.is_err() {
            MarketAnchorQueryResult::DecodeError
        } else {
            MarketAnchorQueryResult::Observed
        });
        observation.http_request_url = Some(capture.request_url.to_string());
        observation.http_response_url = Some(capture.response_url.to_string());
        observation.http_status_code = Some(capture.status_code.as_u16());
        observation.http_received_at_utc = Some(capture.received_at_utc.timestamp_millis());
        observation.http_body_sha256 = Some(format!("{:x}", Sha256::digest(&capture.raw_body)));
        let (encoding, body) = match String::from_utf8(capture.raw_body) {
            Ok(body) => ("utf8", body),
            Err(error) => (
                "base64",
                base64::engine::general_purpose::STANDARD.encode(error.as_bytes()),
            ),
        };
        observation.http_body_encoding = Some(encoding.to_owned());
        observation.http_body = Some(body);
        (observation, capture.result)
    }

    pub fn from_gamma_capture(
        capture: ResponseCapture<Event>,
        identity: &MarketAnchorIdentity,
    ) -> (Self, Option<WebsiteCryptoPriceRequest>) {
        let (mut observation, result) = Self::capture(capture);
        let Ok(event) = result else {
            return (observation, None);
        };
        observation.anchor_event_id = Some(event.id.clone());
        let market = event.markets.as_deref().and_then(|markets| {
            markets
                .iter()
                .find(|market| gamma_market_matches(market, identity))
        });
        let Some(market) = market.filter(|_| event.slug.as_deref() == Some(&identity.slug)) else {
            observation.anchor_query_result = MarketAnchorQueryResult::DecodeError;
            return (observation, None);
        };
        observation.price_to_beat_decimal = event
            .event_metadata
            .as_ref()
            .and_then(|metadata| metadata.price_to_beat.as_ref())
            .map(|price| price.0.clone());
        if observation.price_to_beat_decimal.is_some() {
            observation.price_to_beat_source_field = Some("eventMetadata.priceToBeat".to_owned());
        }
        observation.crypto_market_config_id = market.crypto_market_config_id.clone();
        observation.resolution_source = market.resolution_source.clone();
        if let Some(config) = &market.crypto_market_config {
            observation.crypto_market_config_nested_id = config.id.clone();
            observation.crypto_market_config_asset = config.asset.clone();
            observation.crypto_market_config_duration = config.duration.clone();
            observation.twap_enabled = config.twap_enabled;
            observation.twap_lookback_seconds = config.twap_lookback_seconds;
        }
        let fallback = observation
            .price_to_beat_decimal
            .is_none()
            .then(|| website_request(market, identity))
            .flatten();
        (observation, fallback)
    }

    pub fn from_website_capture(capture: ResponseCapture<WebsiteCryptoPriceResponse>) -> Self {
        let (mut observation, result) = Self::capture(capture);
        if let Ok(prices) = result {
            observation.price_to_beat_decimal = prices.open_price.map(|price| price.0);
            if observation.price_to_beat_decimal.is_some() {
                observation.price_to_beat_source_field = Some("openPrice".to_owned());
            }
        }
        observation
    }
}

fn gamma_market_matches(market: &Market, identity: &MarketAnchorIdentity) -> bool {
    if market.id != identity.clob_market_id
        || market.slug.as_deref() != Some(&identity.slug)
        || market.condition_id.map(|id| format!("{id:#x}")).as_deref()
            != Some(&identity.condition_id)
    {
        return false;
    }
    let Some(ids) = &market.clob_token_ids else {
        return false;
    };
    let Some(outcomes) = &market.outcomes else {
        return false;
    };
    let Ok(expected) = serde_json::from_str::<[String; 2]>(&identity.asset_ids) else {
        return false;
    };
    if ids.len() != 2 || outcomes.len() != 2 {
        return false;
    }
    let mut ordered = [None, None];
    for (id, outcome) in ids.iter().zip(outcomes) {
        let index = match outcome.trim().to_ascii_lowercase().as_str() {
            "up" => 0,
            "down" => 1,
            _ => return false,
        };
        ordered[index] = Some(id.to_string());
    }
    ordered == expected.map(Some)
}

fn website_request(
    market: &Market,
    identity: &MarketAnchorIdentity,
) -> Option<WebsiteCryptoPriceRequest> {
    let config = market.crypto_market_config.as_ref()?;
    let symbol = config.asset.as_ref()?.to_ascii_uppercase();
    let variant = match config.duration.as_deref()? {
        "5m" => "fiveminute",
        _ => return None,
    };
    let twap_enabled = config.twap_enabled?;
    let window = if twap_enabled {
        config.twap_lookback_seconds?
    } else {
        0
    };
    Some(
        WebsiteCryptoPriceRequest::builder()
            .symbol(symbol)
            .event_start_time(DateTime::from_timestamp_millis(identity.market_start_utc)?)
            .variant(variant)
            .end_date(DateTime::from_timestamp_millis(identity.market_end_utc)?)
            .twap_enabled(twap_enabled)
            .twap_lookback_seconds(window)
            .build(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_metadata_format_can_persist_independent_anchor_responses() {
        assert_eq!(
            crate::writer::TypedParquetSegmentWriter::current_schema_version(
                "polymarket_market_metadata",
            )
            .unwrap(),
            "polymarket_market_metadata_v6",
        );
    }

    fn identity() -> MarketAnchorIdentity {
        MarketAnchorIdentity {
            slug: "btc-updown-5m-1786162800".to_owned(),
            condition_id: format!("0x{:064x}", 2),
            clob_market_id: "123".to_owned(),
            asset_ids: "[\"101\",\"102\"]".to_owned(),
            market_start_utc: 1786162800000,
            market_end_utc: 1786163100000,
            fixed_identity_verified_at_utc: 1786162857625,
        }
    }

    fn capture<T: serde::de::DeserializeOwned>(body: &[u8]) -> ResponseCapture<T> {
        ResponseCapture::builder()
            .result(Ok(serde_json::from_slice(body).unwrap()))
            .raw_body(body.to_vec())
            .request_url(
                "https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786162800"
                    .parse()
                    .unwrap(),
            )
            .response_url(
                "https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786162800"
                    .parse()
                    .unwrap(),
            )
            .received_at_utc(DateTime::from_timestamp_millis(1786162858000).unwrap())
            .status_code(reqwest::StatusCode::OK)
            .build()
    }

    fn event_body(price: &str) -> String {
        format!(
            r#"{{"id":"789","slug":"btc-updown-5m-1786162800","eventMetadata":{{"priceToBeat":{price},"finalPrice":64943.19479955845}},"markets":[{{"id":"123","slug":"btc-updown-5m-1786162800","conditionId":"0x{:064x}","clobTokenIds":"[\"101\",\"102\"]","outcomes":"[\"Up\",\"Down\"]","cryptoMarketConfig":{{"id":"btc-5m-twap-30","asset":"btc","duration":"5m","twapEnabled":true,"twapLookbackSeconds":30}}}}]}}"#,
            2
        )
    }

    #[test]
    fn gamma_keeps_decimal_lexical_and_exact_http_body() {
        let body = event_body("64950.0142185114400000001");
        let (observation, fallback) =
            MarketAnchorObservation::from_gamma_capture(capture(body.as_bytes()), &identity());
        assert_eq!(
            observation.price_to_beat_decimal.as_deref(),
            Some("64950.0142185114400000001")
        );
        assert_eq!(observation.http_body.as_deref(), Some(body.as_str()));
        assert_eq!(
            observation.http_body_sha256,
            Some(format!("{:x}", Sha256::digest(body.as_bytes())))
        );
        assert_eq!(observation.twap_lookback_seconds, Some(30));
        assert_eq!(observation.anchor_source_computed_at_utc, None);
        assert!(fallback.is_none());
    }

    #[test]
    fn missing_gamma_anchor_has_rule_and_explicit_website_parameters() {
        let (observation, fallback) = MarketAnchorObservation::from_gamma_capture(
            capture(event_body("null").as_bytes()),
            &identity(),
        );
        assert_eq!(
            observation.anchor_query_result,
            MarketAnchorQueryResult::Observed
        );
        assert_eq!(observation.price_to_beat_decimal, None);
        let fallback = fallback.unwrap();
        assert_eq!(fallback.symbol, "BTC");
        assert_eq!(fallback.variant, "fiveminute");
        assert!(fallback.twap_enabled);
        assert_eq!(fallback.twap_lookback_seconds, 30);
        assert_eq!(
            fallback.event_start_time.timestamp_millis(),
            identity().market_start_utc
        );
        assert_eq!(
            fallback.end_date.timestamp_millis(),
            identity().market_end_utc
        );
    }

    #[test]
    fn another_market_response_cannot_publish_an_anchor_for_requested_identity() {
        let body = event_body("64950.01").replace("\"id\":\"123\"", "\"id\":\"456\"");
        let (observation, fallback) =
            MarketAnchorObservation::from_gamma_capture(capture(body.as_bytes()), &identity());
        assert_eq!(
            observation.anchor_query_result,
            MarketAnchorQueryResult::DecodeError
        );
        assert_eq!(observation.price_to_beat_decimal, None);
        assert_eq!(observation.http_body.as_deref(), Some(body.as_str()));
        assert!(fallback.is_none());
    }

    #[test]
    fn website_only_open_price_is_an_anchor_and_has_no_new_rule() {
        let body = br#"{"openPrice":64950.0142185114400000001,"closePrice":64943.19479955845,"timestamp":123,"cached":true}"#;
        let observation = MarketAnchorObservation::from_website_capture(capture(body));
        assert_eq!(
            observation.price_to_beat_decimal.as_deref(),
            Some("64950.0142185114400000001")
        );
        assert_eq!(
            observation.price_to_beat_source_field.as_deref(),
            Some("openPrice")
        );
        assert_eq!(observation.twap_enabled, None);
        assert_eq!(observation.anchor_source_computed_at_utc, None);
    }

    #[test]
    fn malformed_http_body_is_still_byte_exact_evidence() {
        let mut response: ResponseCapture<Event> = capture(event_body("null").as_bytes());
        response.raw_body = vec![0xff, 0, b'a'];
        response.status_code = reqwest::StatusCode::TOO_MANY_REQUESTS;
        response.result = Err(polymarket_client_sdk_v2::error::Error::validation(
            "rate limited",
        ));
        let (observation, fallback) =
            MarketAnchorObservation::from_gamma_capture(response, &identity());
        assert_eq!(
            observation.anchor_query_result,
            MarketAnchorQueryResult::HttpError
        );
        assert_eq!(observation.http_body_encoding.as_deref(), Some("base64"));
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(observation.http_body.unwrap())
                .unwrap(),
            [0xff, 0, b'a']
        );
        assert!(fallback.is_none());
    }
}
