//! Client for the Polymarket Gamma API.
//!
//! This module provides an HTTP client for interacting with the Polymarket Gamma API,
//! which offers endpoints for querying events, markets, tags, series, comments, and more.
//!
//! # Example
//!
//! ```no_run
//! use polymarket_client_sdk_v2::gamma::{Client, types::request::EventsRequest};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::default();
//!
//! // List active events
//! let request = EventsRequest::builder()
//!     .active(true)
//!     .limit(10)
//!     .build();
//!
//! let events = client.events(&request).await?;
//! for event in events {
//!     println!("{}: {:?}", event.id, event.title);
//! }
//! # Ok(())
//! # }
//! ```

use std::future::Future;

use async_stream::try_stream;
use chrono::Utc;
use futures::Stream;
use reqwest::{
    Client as ReqwestClient, Method, Request, StatusCode,
    header::{HeaderMap, HeaderValue},
};
use serde::Serialize;
use serde::de::DeserializeOwned;
#[cfg(feature = "tracing")]
use tracing::warn;
use url::Url;

use super::types::request::{
    CommentsByIdRequest, CommentsByUserAddressRequest, CommentsRequest, EventByIdRequest,
    EventBySlugRequest, EventTagsRequest, EventsRequest, MarketByIdRequest, MarketBySlugRequest,
    MarketTagsRequest, MarketsRequest, PublicProfileRequest, RelatedTagsByIdRequest,
    RelatedTagsBySlugRequest, SearchRequest, SeriesByIdRequest, SeriesListRequest, TagByIdRequest,
    TagBySlugRequest, TagsRequest, TeamsRequest, WebsiteCryptoPriceRequest,
};
use super::types::response::{
    Comment, Event, HealthResponse, Market, PublicProfile, RelatedTag, ResponseCapture,
    SearchResults, Series, SportsMarketTypesResponse, SportsMetadata, Tag, Team,
    WebsiteCryptoPriceResponse,
};
use crate::error::Error;
use crate::{Result, ToQueryParams as _};

const MAX_LIMIT: i32 = 500;
const WEBSITE_CRYPTO_PRICE_URL: &str = "https://polymarket.com/api/crypto/crypto-price";

/// HTTP client for the Polymarket Gamma API.
///
/// Provides methods for querying events, markets, tags, series, comments,
/// profiles, and search functionality.
///
/// # API Base URL
///
/// The default API endpoint is `https://gamma-api.polymarket.com`.
///
/// # Example
///
/// ```no_run
/// use polymarket_client_sdk_v2::gamma::Client;
///
/// // Create client with default endpoint
/// let client = Client::default();
///
/// // Or with a custom endpoint
/// let client = Client::new("https://custom-api.example.com").unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    host: Url,
    client: ReqwestClient,
}

impl Default for Client {
    fn default() -> Self {
        Client::new("https://gamma-api.polymarket.com")
            .expect("Client with default endpoint should succeed")
    }
}

impl Client {
    /// Creates a new Gamma API client with a custom host URL.
    ///
    /// # Arguments
    ///
    /// * `host` - The base URL for the Gamma API.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or the HTTP client cannot be created.
    pub fn new(host: &str) -> Result<Client> {
        let mut headers = HeaderMap::new();

        headers.insert("User-Agent", HeaderValue::from_static("rs_clob_client"));
        headers.insert("Accept", HeaderValue::from_static("*/*"));
        headers.insert("Connection", HeaderValue::from_static("keep-alive"));
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        let client = ReqwestClient::builder().default_headers(headers).build()?;

        Ok(Self {
            host: Url::parse(host)?,
            client,
        })
    }

    /// Returns the base URL of the API.
    #[must_use]
    pub fn host(&self) -> &Url {
        &self.host
    }

    async fn get<Req: Serialize, Res: DeserializeOwned + Serialize>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Res> {
        let query = req.query_params(None);
        let request = self
            .client
            .request(Method::GET, format!("{}{path}{query}", self.host))
            .build()?;
        crate::request(&self.client, request, None).await
    }

    fn build_capture_request<Req: Serialize>(&self, url: &str, request: &Req) -> Result<Request> {
        Ok(self
            .client
            .request(Method::GET, format!("{url}{}", request.query_params(None)))
            .build()?)
    }

    async fn capture_request<Res: DeserializeOwned>(
        &self,
        request: Request,
    ) -> Result<ResponseCapture<Res>> {
        let method = request.method().clone();
        let path = request.url().path().to_owned();
        let request_url = request.url().clone();
        let response = self.client.execute(request).await?;
        let response_url = response.url().clone();
        let status_code = response.status();
        let error_headers = (!status_code.is_success()).then(|| response.headers().clone());
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(_) if !status_code.is_success() => {
                // 兼容原请求在错误正文接收失败时保留 HTTP 状态和空错误正文。
                return Err(Error::status(status_code, method, path, ""));
            }
            Err(error) => return Err(error.into()),
        };
        let received_at_utc = Utc::now();

        let result = if let Some(headers) = error_headers {
            // 复用 reqwest 的字符集与 BOM 解码，只处理已在内存中的同一实体。
            let mut text_response = http::Response::new(body.clone());
            *text_response.headers_mut() = headers;
            let message = reqwest::Response::from(text_response)
                .text()
                .await
                .unwrap_or_default();
            Err(Error::status(status_code, method, path, message))
        } else {
            match crate::serde_helpers::deserialize_slice_with_warnings::<Option<Res>>(&body) {
                Ok(Some(value)) => Ok(value),
                Ok(None) => Err(Error::status(
                    StatusCode::NOT_FOUND,
                    method,
                    path,
                    "Unable to find requested resource",
                )),
                Err(error) => Err(error),
            }
        };
        let raw_body = body.into();

        Ok(ResponseCapture {
            result,
            raw_body,
            request_url,
            response_url,
            received_at_utc,
            status_code,
        })
    }

    async fn get_capture<Req: Serialize, Res: DeserializeOwned>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<ResponseCapture<Res>> {
        let request = self.build_capture_request(&format!("{}{path}", self.host), request)?;
        self.capture_request(request).await
    }

    /// Performs a health check on the Gamma API.
    ///
    /// Returns "OK" when the API is healthy and operational. Use this for monitoring
    /// and verifying the API's availability.
    ///
    /// # Errors
    ///
    /// Returns an error if the API is unreachable or returns a non-200 status code.
    pub async fn status(&self) -> Result<HealthResponse> {
        let request = self
            .client
            .request(Method::GET, format!("{}status", self.host))
            .build()?;

        let response = self.client.execute(request).await?;
        let status_code = response.status();

        if !status_code.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(Error::status(
                status_code,
                Method::GET,
                "status".to_owned(),
                message,
            ));
        }

        Ok(response.text().await?)
    }

    /// Retrieves a list of sports teams with optional filtering.
    ///
    /// Returns teams participating in sports markets. Use filters to narrow results
    /// by sport type, league, or other criteria.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn teams(&self, request: &TeamsRequest) -> Result<Vec<Team>> {
        self.get("teams", request).await
    }

    /// Retrieves metadata for all supported sports.
    ///
    /// Returns information about sports categories available on Polymarket,
    /// including sports like NFL, NBA, MLB, etc. Useful for discovering
    /// what sports markets are available.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn sports(&self) -> Result<Vec<SportsMetadata>> {
        self.get("sports", &()).await
    }

    /// Retrieves valid market types for sports.
    ///
    /// Returns the different types of sports markets available (e.g., moneyline,
    /// spread, over/under). Use this to understand what formats are supported.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn sports_market_types(&self) -> Result<SportsMarketTypesResponse> {
        self.get("sports/market-types", &()).await
    }

    /// Retrieves a list of tags with optional filtering.
    ///
    /// Tags categorize markets and events (e.g., "Politics", "Crypto", "Sports").
    /// Use filters to search for specific tag types or categories.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn tags(&self, request: &TagsRequest) -> Result<Vec<Tag>> {
        self.get("tags", request).await
    }

    /// Retrieves a single tag by its unique ID.
    ///
    /// Returns detailed information about a specific tag including its name,
    /// description, and associated markets.
    ///
    /// # Errors
    ///
    /// Returns an error if the tag ID is invalid or the request fails.
    pub async fn tag_by_id(&self, request: &TagByIdRequest) -> Result<Tag> {
        self.get(&format!("tags/{}", request.id), request).await
    }

    /// Retrieves a single tag by its URL-friendly slug.
    ///
    /// Returns the same information as [`Self::tag_by_id`] but uses a human-readable
    /// slug identifier instead of a numeric ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the slug is invalid or the request fails.
    pub async fn tag_by_slug(&self, request: &TagBySlugRequest) -> Result<Tag> {
        self.get(&format!("tags/slug/{}", request.slug), request)
            .await
    }

    /// Retrieves related tag relationships for a tag by ID.
    ///
    /// Returns tags that are semantically related to the specified tag, including
    /// the relationship type (e.g., parent, child, related). Useful for discovering
    /// related markets and topics.
    ///
    /// # Errors
    ///
    /// Returns an error if the tag ID is invalid or the request fails.
    pub async fn related_tags_by_id(
        &self,
        request: &RelatedTagsByIdRequest,
    ) -> Result<Vec<RelatedTag>> {
        self.get(&format!("tags/{}/related-tags", request.id), request)
            .await
    }

    /// Retrieves related tag relationships for a tag by slug.
    ///
    /// Same as [`Self::related_tags_by_id`] but uses a slug identifier instead of an ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the slug is invalid or the request fails.
    pub async fn related_tags_by_slug(
        &self,
        request: &RelatedTagsBySlugRequest,
    ) -> Result<Vec<RelatedTag>> {
        self.get(&format!("tags/slug/{}/related-tags", request.slug), request)
            .await
    }

    /// Retrieves tags that are related to a specified tag by ID.
    ///
    /// Returns the actual tag objects (not just relationships) for tags related to
    /// the specified tag. This provides full tag details for related topics.
    ///
    /// # Errors
    ///
    /// Returns an error if the tag ID is invalid or the request fails.
    pub async fn tags_related_to_tag_by_id(
        &self,
        request: &RelatedTagsByIdRequest,
    ) -> Result<Vec<Tag>> {
        self.get(&format!("tags/{}/related-tags/tags", request.id), request)
            .await
    }

    /// Retrieves tags that are related to a specified tag by slug.
    ///
    /// Same as [`Self::tags_related_to_tag_by_id`] but uses a slug identifier instead of an ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the slug is invalid or the request fails.
    pub async fn tags_related_to_tag_by_slug(
        &self,
        request: &RelatedTagsBySlugRequest,
    ) -> Result<Vec<Tag>> {
        self.get(
            &format!("tags/slug/{}/related-tags/tags", request.slug),
            request,
        )
        .await
    }

    /// Retrieves a list of events with optional filtering.
    ///
    /// Events are collections of related markets (e.g., "2024 Presidential Election").
    /// Use filters to search by tags, active status, or other criteria.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn events(&self, request: &EventsRequest) -> Result<Vec<Event>> {
        self.get("events", request).await
    }

    /// Retrieves a single event by its unique ID.
    ///
    /// Returns detailed information about an event including its markets,
    /// description, and associated tags.
    ///
    /// # Errors
    ///
    /// Returns an error if the event ID is invalid or the request fails.
    pub async fn event_by_id(&self, request: &EventByIdRequest) -> Result<Event> {
        self.get(&format!("events/{}", request.id), request).await
    }

    /// Retrieves a single event by its URL-friendly slug.
    ///
    /// Returns the same information as [`Self::event_by_id`] but uses a slug
    /// identifier instead of a numeric ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the slug is invalid or the request fails.
    pub async fn event_by_slug(&self, request: &EventBySlugRequest) -> Result<Event> {
        self.event_by_slug_capture(request).await?.result
    }

    /// 取得事件并保留原响应证据，新价格字段从原 JSON 字节直接解析。
    ///
    /// HTTP 错误、空体、无效 JSON 和顶层 `null` 保留在返回值的 `result` 中。
    pub async fn event_by_slug_capture(
        &self,
        request: &EventBySlugRequest,
    ) -> Result<ResponseCapture<Event>> {
        self.get_capture(&format!("events/slug/{}", request.slug), request)
            .await
    }

    /// 查询固定官网价格端点，复用本客户端连接池，不受 Gamma 自定义主机影响。
    ///
    /// 参数必须来自目标市场；来源优先级、重试和历史生效语义由调用方处理。
    pub async fn website_crypto_price_capture(
        &self,
        request: &WebsiteCryptoPriceRequest,
    ) -> Result<ResponseCapture<WebsiteCryptoPriceResponse>> {
        let request = self.build_capture_request(WEBSITE_CRYPTO_PRICE_URL, request)?;
        self.capture_request(request).await
    }

    /// Retrieves all tags associated with an event.
    ///
    /// Returns the categorization tags for a specific event, helping understand
    /// the event's topics and categories.
    ///
    /// # Errors
    ///
    /// Returns an error if the event ID is invalid or the request fails.
    pub async fn event_tags(&self, request: &EventTagsRequest) -> Result<Vec<Tag>> {
        self.get(&format!("events/{}/tags", request.id), request)
            .await
    }

    /// Retrieves a list of prediction markets with optional filtering.
    ///
    /// Markets are the core trading instruments on Polymarket. Use filters to search
    /// by tags, events, active status, or CLOB token IDs. Returns market details
    /// including current prices, volume, and outcome information.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn markets(&self, request: &MarketsRequest) -> Result<Vec<Market>> {
        self.markets_capture(request).await?.result
    }

    /// 取得市场发现响应及原字节；保留其中嵌套事件的新价格词法。
    pub async fn markets_capture(
        &self,
        request: &MarketsRequest,
    ) -> Result<ResponseCapture<Vec<Market>>> {
        self.get_capture("markets", request).await
    }

    /// Retrieves a single market by its unique ID.
    ///
    /// Returns detailed information about a specific market including outcomes,
    /// current prices, volume, and resolution details.
    ///
    /// # Errors
    ///
    /// Returns an error if the market ID is invalid or the request fails.
    pub async fn market_by_id(&self, request: &MarketByIdRequest) -> Result<Market> {
        self.get(&format!("markets/{}", request.id), request).await
    }

    /// Retrieves a single market by its URL-friendly slug.
    ///
    /// Returns the same information as [`Self::market_by_id`] but uses a slug
    /// identifier instead of a numeric ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the slug is invalid or the request fails.
    pub async fn market_by_slug(&self, request: &MarketBySlugRequest) -> Result<Market> {
        self.market_by_slug_capture(request).await?.result
    }

    /// 取得单市场响应及原字节，兼容原有终态查询调用方式。
    pub async fn market_by_slug_capture(
        &self,
        request: &MarketBySlugRequest,
    ) -> Result<ResponseCapture<Market>> {
        self.get_capture(&format!("markets/slug/{}", request.slug), request)
            .await
    }

    /// Retrieves all tags associated with a market.
    ///
    /// Returns the categorization tags for a specific market, helping understand
    /// the market's topics and categories.
    ///
    /// # Errors
    ///
    /// Returns an error if the market ID is invalid or the request fails.
    pub async fn market_tags(&self, request: &MarketTagsRequest) -> Result<Vec<Tag>> {
        self.get(&format!("markets/{}/tags", request.id), request)
            .await
    }

    /// Retrieves a list of market series with optional filtering.
    ///
    /// Series are groups of related markets that follow a pattern (e.g., weekly
    /// sports outcomes, monthly economic indicators). Useful for tracking recurring
    /// predictions over time.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn series(&self, request: &SeriesListRequest) -> Result<Vec<Series>> {
        self.get("series", request).await
    }

    /// Retrieves a single series by its unique ID.
    ///
    /// Returns detailed information about a series including all markets in the series
    /// and their resolution history.
    ///
    /// # Errors
    ///
    /// Returns an error if the series ID is invalid or the request fails.
    pub async fn series_by_id(&self, request: &SeriesByIdRequest) -> Result<Series> {
        self.get(&format!("series/{}", request.id), request).await
    }

    /// Retrieves a list of user comments with optional filtering.
    ///
    /// Comments are user-generated discussions and analysis on markets and events.
    /// Use filters to search by market, event, or other criteria.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn comments(&self, request: &CommentsRequest) -> Result<Vec<Comment>> {
        self.get("comments", request).await
    }

    /// Retrieves comments by their unique comment ID.
    ///
    /// Returns comments with the specified ID, including nested replies and
    /// associated metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the comment ID is invalid or the request fails.
    pub async fn comments_by_id(&self, request: &CommentsByIdRequest) -> Result<Vec<Comment>> {
        self.get(&format!("comments/{}", request.id), request).await
    }

    /// Retrieves all comments authored by a specific wallet address.
    ///
    /// Returns comments posted by a particular user, useful for viewing a user's
    /// contribution history and market analysis.
    ///
    /// # Errors
    ///
    /// Returns an error if the address is invalid or the request fails.
    pub async fn comments_by_user_address(
        &self,
        request: &CommentsByUserAddressRequest,
    ) -> Result<Vec<Comment>> {
        self.get(
            &format!("comments/user_address/{}", request.user_address),
            request,
        )
        .await
    }

    /// Retrieves a public trading profile for a wallet address.
    ///
    /// Returns public statistics about a trader including their trading history,
    /// win rate, and other performance metrics. Only publicly visible information
    /// is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the address is invalid or the request fails.
    pub async fn public_profile(&self, request: &PublicProfileRequest) -> Result<PublicProfile> {
        self.get("public-profile", request).await
    }

    /// Searches across markets, events, and user profiles.
    ///
    /// Performs a text search to find markets, events, or users matching the query.
    /// Useful for discovery and finding specific content across the platform.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the search query is invalid.
    pub async fn search(&self, request: &SearchRequest) -> Result<SearchResults> {
        self.get("public-search", request).await
    }

    /// Returns a stream of results using offset-based pagination.
    ///
    /// This method repeatedly invokes the provided closure `call`, which takes the
    /// client and pagination parameters (limit and offset) to fetch data. Each page
    /// of results is flattened into individual items in the stream.
    ///
    /// The stream continues fetching pages until:
    /// - An empty page is returned, or
    /// - A page with fewer items than the requested limit is returned (indicating the last page)
    ///
    /// # Arguments
    ///
    /// * `call` - A closure that takes `&Client`, `limit: i32`, and `offset: i32`,
    ///   returning a future that resolves to a `Result<Vec<Data>>`
    /// * `limit` - The number of items to fetch per page (default: 100)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use polymarket_client_sdk_v2::gamma::{Client, types::request::EventsRequest};
    /// use tokio::pin;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::default();
    ///
    /// // Stream all active events
    /// let mut stream = client.stream_data(
    ///     |client, limit, offset| {
    ///         let request = EventsRequest::builder()
    ///             .active(true)
    ///             .limit(limit)
    ///             .offset(offset)
    ///             .build();
    ///         async move { client.events(&request).await }
    ///     },
    ///     100, // page size
    /// );
    ///
    /// pin!(stream);
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(event) => println!("Event: {}", event.id),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_data<'client, Call, Fut, Data>(
        &'client self,
        call: Call,
        limit: i32,
    ) -> impl Stream<Item = Result<Data>> + 'client
    where
        Call: Fn(&'client Client, i32, i32) -> Fut + 'client,
        Fut: Future<Output = Result<Vec<Data>>> + 'client,
        Data: 'client,
    {
        let limit = if limit > MAX_LIMIT {
            #[cfg(feature = "tracing")]
            warn!(
                "Supplied {limit} limit, Gamma only allows for maximum {MAX_LIMIT} responses per call, defaulting to {MAX_LIMIT}"
            );

            MAX_LIMIT
        } else {
            limit
        };

        try_stream! {
            let mut offset = 0;

            loop {
                let data = call(self, limit, offset).await?;

                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_possible_wrap,
                    reason = "We shouldn't ever truncate/wrap since we'll never return that many records in one call")
                ]
                let count = data.len() as i32;

                for item in data {
                    yield item;
                }

                // Stop if we received fewer items than requested (last page)
                if count < limit {
                    break;
                }

                offset += count;
            }
        }
    }
}

#[cfg(test)]
mod price_capture_tests {
    use httpmock::{Method::GET, MockServer};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[tokio::test]
    async fn incomplete_error_body_keeps_legacy_http_status() -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await?;
                let mut request = [0_u8; 1024];
                socket.read(&mut request).await?;
                socket
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 10\r\nConnection: close\r\n\r\nx")
                    .await?;
                socket.shutdown().await?;
            }
            Ok::<(), std::io::Error>(())
        });
        let client = Client::new(&format!("http://{address}"))?;
        let request = EventsRequest::default();
        let old_error = client
            .get::<_, Event>("incomplete", &request)
            .await
            .unwrap_err();
        let new_error = client
            .get_capture::<_, Event>("incomplete", &request)
            .await
            .unwrap_err();
        for error in [old_error, new_error] {
            let status = error.downcast_ref::<crate::error::Status>().unwrap();
            assert_eq!(status.status_code, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(status.message, "");
        }
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn capture_error_text_matches_existing_sdk_charset_behavior() -> anyhow::Result<()> {
        let server = MockServer::start();
        let client = Client::new(&server.base_url())?;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/events/charset");
            then.status(400)
                .header("content-type", "text/plain; charset=windows-1252")
                .body([0x80]);
        });
        let request = EventsRequest::default();
        let legacy_error = client
            .get::<_, Event>("events/charset", &request)
            .await
            .unwrap_err();
        let capture = client
            .get_capture::<_, Event>("events/charset", &request)
            .await?;
        assert_eq!(capture.raw_body, [0x80]);
        let captured_error = capture.result.unwrap_err();
        let old_status = legacy_error.downcast_ref::<crate::error::Status>().unwrap();
        let new_status = captured_error
            .downcast_ref::<crate::error::Status>()
            .unwrap();
        assert_eq!(old_status.message, "€");
        assert_eq!(new_status.message, old_status.message);
        mock.assert_calls(2);
        Ok(())
    }

    #[tokio::test]
    async fn website_price_uses_fixed_endpoint_and_explicit_market_parameters() -> anyhow::Result<()>
    {
        let server = MockServer::start();
        let client = Client::new(&server.base_url())?;
        let start = "2026-08-21T19:35:00Z".parse::<chrono::DateTime<Utc>>()?;
        let end = "2026-08-21T19:40:00Z".parse::<chrono::DateTime<Utc>>()?;
        let body = r#"{"openPrice":77188.89475116669,"closePrice":77204.30914551041,"timestamp":1788841107993,"completed":true,"incomplete":false,"cached":true}"#;

        for (enabled, window) in [(false, 0), (true, 30), (true, 60)] {
            let input = WebsiteCryptoPriceRequest::builder()
                .symbol("BTC")
                .event_start_time(start)
                .variant("fiveminute")
                .end_date(end)
                .twap_enabled(enabled)
                .twap_lookback_seconds(window)
                .build();
            let mut request = client.build_capture_request(WEBSITE_CRYPTO_PRICE_URL, &input)?;
            assert_eq!(request.url().scheme(), "https");
            assert_eq!(request.url().host_str(), Some("polymarket.com"));
            assert_eq!(request.url().path(), "/api/crypto/crypto-price");
            let mock = server.mock(|when, then| {
                when.method(GET)
                    .path("/api/crypto/crypto-price")
                    .query_param("symbol", "BTC")
                    .query_param("eventStartTime", "2026-08-21T19:35:00Z")
                    .query_param("variant", "fiveminute")
                    .query_param("endDate", "2026-08-21T19:40:00Z")
                    .query_param("twapEnabled", enabled.to_string())
                    .query_param("twapLookbackSeconds", window.to_string());
                then.status(200).body(body);
            });

            // 仅测试将相同请求送入本机服务，生产接口没有可配置后备主机。
            let mut local_url = Url::parse(&server.base_url())?;
            local_url.set_path(request.url().path());
            local_url.set_query(request.url().query());
            *request.url_mut() = local_url;
            let capture: ResponseCapture<WebsiteCryptoPriceResponse> =
                client.capture_request(request).await?;
            assert_eq!(capture.raw_body, body.as_bytes());
            let prices = capture.result?;
            assert_eq!(prices.open_price.unwrap().0, "77188.89475116669");
            assert_eq!(prices.close_price.unwrap().0, "77204.30914551041");
            assert_eq!(prices.timestamp, Some(1_788_841_107_993));
            assert_eq!(prices.completed, Some(true));
            assert_eq!(prices.incomplete, Some(false));
            assert_eq!(prices.cached, Some(true));
            mock.assert();
        }
        Ok(())
    }
}
