use std::future::Future;
use std::pin::Pin;
use std::str::FromStr as _;

use contracts::{BuyLegIntentV1, BuyOrderIntentV1, ModelIntentV1};
use polymarket_client_sdk_v2::auth::Kind;
use polymarket_client_sdk_v2::auth::Signer;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::Client;
use polymarket_client_sdk_v2::clob::types::request::CancelMarketOrderRequest;
use polymarket_client_sdk_v2::clob::types::response::{CancelOrdersResponse, PostOrderResponse};
use polymarket_client_sdk_v2::clob::types::{Amount, OrderType, Side, SignedOrder};
use polymarket_client_sdk_v2::error::Error;
use polymarket_client_sdk_v2::types::{B256, DateTime, U256, Utc};

use crate::{BridgeBackendV1, private};

pub type LiveIntentCallContextV1 = ();
pub type LiveIntentErrorV1 = Error;

#[derive(Debug, PartialEq)]
pub enum LiveIntentImmediateSuccessV1 {
    PostOrder(PostOrderResponse),
    PostOrders(Vec<PostOrderResponse>),
    Cancellation(CancelOrdersResponse),
}

pub struct LiveAdapterV1<'live, S: Signer, K: Kind> {
    client: &'live Client<Authenticated<K>>,
    signer: &'live S,
}

impl<'live, S: Signer, K: Kind> LiveAdapterV1<'live, S, K> {
    pub(crate) const fn bind(client: &'live Client<Authenticated<K>>, signer: &'live S) -> Self {
        Self { client, signer }
    }

    async fn signed_buy(&self, leg: &BuyLegIntentV1) -> Result<SignedOrder, Error> {
        let token_id = U256::from_str(&leg.token_id.0)?;
        let signable = match &leg.order {
            BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share,
                size_shares,
                post_only,
            } => {
                self.client
                    .limit_order()
                    .token_id(token_id)
                    .price(*limit_price_pusd_per_share)
                    .size(*size_shares)
                    .side(Side::Buy)
                    .order_type(OrderType::GTC)
                    .post_only(*post_only)
                    .build()
                    .await?
            }
            BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share,
                size_shares,
                post_only,
                expires_at_utc_sec,
            } => {
                let expiration = DateTime::<Utc>::from_timestamp(*expires_at_utc_sec, 0)
                    .ok_or_else(|| Error::validation("GTD 到期 Unix 秒不能表示为 SDK DateTime"))?;
                self.client
                    .limit_order()
                    .token_id(token_id)
                    .price(*limit_price_pusd_per_share)
                    .size(*size_shares)
                    .side(Side::Buy)
                    .order_type(OrderType::GTD)
                    .post_only(*post_only)
                    .expiration(expiration)
                    .build()
                    .await?
            }
            BuyOrderIntentV1::FokMarket {
                spend_pusd,
                worst_price_pusd_per_share,
            } => {
                self.client
                    .market_order()
                    .token_id(token_id)
                    .amount(Amount::usdc(*spend_pusd)?)
                    .price(*worst_price_pusd_per_share)
                    .side(Side::Buy)
                    .order_type(OrderType::FOK)
                    .build()
                    .await?
            }
            BuyOrderIntentV1::FakMarket {
                spend_pusd,
                worst_price_pusd_per_share,
            } => {
                self.client
                    .market_order()
                    .token_id(token_id)
                    .amount(Amount::usdc(*spend_pusd)?)
                    .price(*worst_price_pusd_per_share)
                    .side(Side::Buy)
                    .order_type(OrderType::FAK)
                    .build()
                    .await?
            }
        };
        self.client.sign(self.signer, signable).await
    }

    async fn dispatch_intent(
        &mut self,
        intent: &ModelIntentV1,
    ) -> Result<LiveIntentImmediateSuccessV1, Error> {
        match intent {
            ModelIntentV1::SubmitSingleBuy { order, .. } => {
                let signed = self.signed_buy(order).await?;
                self.client
                    .post_order(signed)
                    .await
                    .map(LiveIntentImmediateSuccessV1::PostOrder)
            }
            ModelIntentV1::SubmitPairedBuy { orders, .. } => {
                let first = self.signed_buy(&orders[0]).await?;
                let second = self.signed_buy(&orders[1]).await?;
                self.client
                    .post_orders(vec![first, second])
                    .await
                    .map(LiveIntentImmediateSuccessV1::PostOrders)
            }
            ModelIntentV1::CancelOrder { platform_order_id } => self
                .client
                .cancel_order(platform_order_id)
                .await
                .map(LiveIntentImmediateSuccessV1::Cancellation),
            ModelIntentV1::CancelOrders { platform_order_ids } => {
                let order_ids = platform_order_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                self.client
                    .cancel_orders(&order_ids)
                    .await
                    .map(LiveIntentImmediateSuccessV1::Cancellation)
            }
            ModelIntentV1::CancelMarketOrders { condition_id } => {
                let request = CancelMarketOrderRequest::builder()
                    .market(B256::from_str(&condition_id.0).map_err(|error| {
                        Error::with_source(polymarket_client_sdk_v2::error::Kind::Internal, error)
                    })?)
                    .build();
                self.client
                    .cancel_market_orders(&request)
                    .await
                    .map(LiveIntentImmediateSuccessV1::Cancellation)
            }
        }
    }
}

impl<'live, S: Signer, K: Kind> private::Sealed for LiveAdapterV1<'live, S, K> {
    type DispatchFuture<'a>
        = Pin<Box<dyn Future<Output = Result<LiveIntentImmediateSuccessV1, Error>> + 'a>>
    where
        Self: 'a;

    fn dispatch<'a, Dispatch>(
        &'a mut self,
        _context: <Self as BridgeBackendV1>::CallContext,
        intent: &'a ModelIntentV1,
    ) -> Self::DispatchFuture<'a>
    where
        'live: 'a,
        Dispatch: private::DispatchKey,
    {
        Box::pin(self.dispatch_intent(intent))
    }
}

impl<'live, S: Signer, K: Kind> BridgeBackendV1 for LiveAdapterV1<'live, S, K> {
    type CallContext = ();
    type ImmediateSuccess = LiveIntentImmediateSuccessV1;
    type Error = Error;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;
    use std::time::Duration;

    use contracts::{BuyLegIntentV1, BuyOrderIntentV1, ConditionId, ModelIntentV1, TokenId};
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::state::Authenticated;
    use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer as _, Uuid};
    use polymarket_client_sdk_v2::clob::types::TickSize;
    use polymarket_client_sdk_v2::clob::{Client, Config};
    use polymarket_client_sdk_v2::error::{Kind as ErrorKind, Method, Status, StatusCode};
    use polymarket_client_sdk_v2::types::{B256, Decimal, U256};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    use crate::{IntentBridge, LiveIntentImmediateSuccessV1};

    const TEST_PRIVATE_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const TEST_PASSPHRASE: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CONDITION_ID: &str = "0x0000000000000000000000000000000000000000000000000000000000000088";
    const BUILDER_CODE: &str = "0x00000000000000000000000000000000000000000000000000000000000000b8";
    const VERSION_RESPONSE: &str = r#"{"version":2}"#;
    const SINGLE_RESPONSE: &str = r#"{
        "errorMsg": null,
        "makingAmount": "5.5",
        "takingAmount": "10",
        "orderID": "single-accepted",
        "status": "live",
        "success": true,
        "transactionHashes": [],
        "tradeIds": []
    }"#;
    const BATCH_RESPONSE: &str = r#"[
        {
            "errorMsg": null,
            "makingAmount": "5.5",
            "takingAmount": "10",
            "orderID": "first-accepted",
            "status": "live",
            "success": true,
            "transactionHashes": [],
            "tradeIds": []
        },
        {
            "errorMsg": "second rejected",
            "makingAmount": "0",
            "takingAmount": "0",
            "orderID": "second-rejected",
            "status": "unmatched",
            "success": false,
            "transactionHashes": [],
            "tradeIds": []
        }
    ]"#;
    const CANCELLATION_RESPONSE: &str = r#"{
        "canceled": ["order-88"],
        "notCanceled": {}
    }"#;
    const MIXED_CANCELLATION_RESPONSE: &str = r#"{
        "canceled": ["duplicate-88"],
        "notCanceled": {"middle-88":"already matched"}
    }"#;

    #[derive(Debug)]
    struct PlannedResponse {
        status: &'static str,
        body: &'static str,
    }

    #[derive(Debug)]
    struct CapturedRequest {
        method: String,
        path: String,
        body: Value,
    }

    async fn read_request(stream: &mut TcpStream) -> CapturedRequest {
        let mut bytes = Vec::new();
        let (header_end, content_length) = loop {
            let mut chunk = [0_u8; 4096];
            let count = stream.read(&mut chunk).await.expect("应读取回环 SDK 请求");
            assert_ne!(count, 0, "SDK 请求不应在头完整前结束");
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let header_end = header_end + 4;
                let headers =
                    std::str::from_utf8(&bytes[..header_end]).expect("SDK 请求头应为 UTF-8");
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length").then(|| {
                            value
                                .trim()
                                .parse::<usize>()
                                .expect("Content-Length 应合法")
                        })
                    })
                    .unwrap_or(0);
                if bytes.len() >= header_end + content_length {
                    break (header_end, content_length);
                }
            }
        };
        let request_line = std::str::from_utf8(&bytes[..header_end])
            .expect("SDK 请求头应为 UTF-8")
            .lines()
            .next()
            .expect("SDK 请求应有请求行");
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().expect("请求行应有方法").to_owned();
        let path = request_parts.next().expect("请求行应有路径").to_owned();
        let body = if content_length == 0 {
            Value::Null
        } else {
            serde_json::from_slice(&bytes[header_end..header_end + content_length])
                .expect("SDK 请求体应为 JSON")
        };
        CapturedRequest { method, path, body }
    }

    async fn spawn_server(
        responses: Vec<PlannedResponse>,
    ) -> (String, JoinHandle<Vec<CapturedRequest>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("应绑定回环端口");
        let address = listener.local_addr().expect("应取得回环地址");
        let task = tokio::spawn(async move {
            let mut captured = Vec::new();
            for planned in responses {
                let (mut stream, _) = listener.accept().await.expect("应接收 SDK 请求");
                captured.push(read_request(&mut stream).await);
                let wire = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    planned.status,
                    planned.body.len(),
                    planned.body,
                );
                stream
                    .write_all(wire.as_bytes())
                    .await
                    .expect("应写入回环响应");
            }
            if let Ok(Ok((mut stream, _))) =
                timeout(Duration::from_millis(100), listener.accept()).await
            {
                captured.push(read_request(&mut stream).await);
                stream
                    .write_all(
                        b"HTTP/1.1 500 Unexpected Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .expect("应拒绝未预期请求");
            }
            captured
        });
        (format!("http://{address}"), task)
    }

    macro_rules! test_signer {
        ($chain_id:expr) => {{
            LocalSigner::from_str(TEST_PRIVATE_KEY)
                .expect("测试私钥应合法")
                .with_chain_id(Some($chain_id))
        }};
    }

    async fn authenticated_client<S: polymarket_client_sdk_v2::auth::Signer>(
        host: &str,
        signer: &S,
        builder_code: Option<B256>,
    ) -> Client<Authenticated<Normal>> {
        let config = match builder_code {
            Some(builder_code) => Config::builder().builder_code(builder_code).build(),
            None => Config::default(),
        };
        Client::new(host, config)
            .expect("回环地址应创建 Client")
            .authentication_builder(signer)
            .credentials(Credentials::new(
                Uuid::nil(),
                TEST_SECRET.to_owned(),
                TEST_PASSPHRASE.to_owned(),
            ))
            .salt_generator(|| 88)
            .authenticate()
            .await
            .expect("固定测试凭据应离线认证")
    }

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("测试 Decimal 应合法")
    }

    fn token(value: &str) -> TokenId {
        TokenId(value.to_owned())
    }

    fn condition() -> ConditionId {
        ConditionId(CONDITION_ID.to_owned())
    }

    fn cache_order_rules(client: &Client<Authenticated<Normal>>, token_ids: &[&str]) {
        for token_id in token_ids {
            let token_id = U256::from_str(token_id).expect("测试 TokenId 应合法");
            client.set_tick_size(token_id, TickSize::Hundredth);
            client.set_neg_risk(token_id, false);
        }
    }

    async fn finish_server(task: JoinHandle<Vec<CapturedRequest>>) -> Vec<CapturedRequest> {
        timeout(Duration::from_secs(2), task)
            .await
            .expect("回环服务不应超时")
            .expect("回环服务任务不应失败")
    }

    #[tokio::test]
    async fn paired_limit_buy_preserves_fields_order_and_single_batch_submission() {
        let (host, server) = spawn_server(vec![
            PlannedResponse {
                status: "200 OK",
                body: VERSION_RESPONSE,
            },
            PlannedResponse {
                status: "200 OK",
                body: BATCH_RESPONSE,
            },
        ])
        .await;
        let signer = test_signer!(POLYGON);
        let builder_code = B256::from_str(BUILDER_CODE).expect("测试 builder code 应合法");
        let client = authenticated_client(&host, &signer, Some(builder_code)).await;
        cache_order_rules(&client, &["11", "22"]);
        let intent = ModelIntentV1::SubmitPairedBuy {
            condition_id: condition(),
            orders: [
                BuyLegIntentV1 {
                    token_id: token("11"),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: decimal("0.55"),
                        size_shares: decimal("10"),
                        post_only: true,
                    },
                },
                BuyLegIntentV1 {
                    token_id: token("22"),
                    order: BuyOrderIntentV1::GtdLimit {
                        limit_price_pusd_per_share: decimal("0.62"),
                        size_shares: decimal("3.5"),
                        post_only: false,
                        expires_at_utc_sec: 1_900_000_000,
                    },
                },
            ],
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let result = bridge.execute((), &intent).await.expect("双腿提交应成功");

        let LiveIntentImmediateSuccessV1::PostOrders(responses) = result else {
            panic!("应返回批订单即时响应");
        };
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].order_id, "first-accepted");
        assert_eq!(responses[1].error_msg.as_deref(), Some("second rejected"));
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 2, "只应读取一次版本并提交一次批订单");
        assert_eq!(
            (captured[0].method.as_str(), captured[0].path.as_str()),
            ("GET", "/version")
        );
        assert_eq!(
            (captured[1].method.as_str(), captured[1].path.as_str()),
            ("POST", "/orders")
        );
        let orders = captured[1].body.as_array().expect("批请求体应为数组");
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0]["orderType"], "GTC");
        assert_eq!(orders[0]["postOnly"], true);
        assert!(orders[0].get("deferExec").is_none());
        assert_eq!(orders[0]["order"]["tokenId"], "11");
        assert_eq!(orders[0]["order"]["makerAmount"], "5500000");
        assert_eq!(orders[0]["order"]["takerAmount"], "10000000");
        assert_eq!(orders[0]["order"]["side"], "BUY");
        assert_eq!(orders[0]["order"]["expiration"], "0");
        assert_eq!(orders[0]["order"]["builder"], BUILDER_CODE);
        assert_eq!(orders[1]["orderType"], "GTD");
        assert_eq!(orders[1]["postOnly"], false);
        assert!(orders[1].get("deferExec").is_none());
        assert_eq!(orders[1]["order"]["tokenId"], "22");
        assert_eq!(orders[1]["order"]["makerAmount"], "2170000");
        assert_eq!(orders[1]["order"]["takerAmount"], "3500000");
        assert_eq!(orders[1]["order"]["side"], "BUY");
        assert_eq!(orders[1]["order"]["expiration"], "1900000000");
        assert_eq!(orders[1]["order"]["builder"], BUILDER_CODE);
    }

    #[tokio::test]
    async fn fok_market_buy_uses_frozen_spend_worst_price_and_one_submission() {
        let (host, server) = spawn_server(vec![
            PlannedResponse {
                status: "200 OK",
                body: VERSION_RESPONSE,
            },
            PlannedResponse {
                status: "200 OK",
                body: SINGLE_RESPONSE,
            },
        ])
        .await;
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        cache_order_rules(&client, &["33"]);
        let intent = ModelIntentV1::SubmitSingleBuy {
            condition_id: condition(),
            order: BuyLegIntentV1 {
                token_id: token("33"),
                order: BuyOrderIntentV1::FokMarket {
                    spend_pusd: decimal("7.2"),
                    worst_price_pusd_per_share: decimal("0.40"),
                },
            },
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let result = bridge.execute((), &intent).await.expect("FOK 提交应成功");

        let LiveIntentImmediateSuccessV1::PostOrder(response) = result else {
            panic!("应返回单订单即时响应");
        };
        assert_eq!(response.order_id, "single-accepted");
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 2, "只应读取一次版本并提交一次单订单");
        let body = &captured[1].body;
        assert_eq!(
            (captured[1].method.as_str(), captured[1].path.as_str()),
            ("POST", "/order")
        );
        assert_eq!(body["orderType"], "FOK");
        assert!(body.get("postOnly").is_none());
        assert!(body.get("deferExec").is_none());
        assert_eq!(body["order"]["tokenId"], "33");
        assert_eq!(body["order"]["makerAmount"], "7200000");
        assert_eq!(body["order"]["takerAmount"], "18000000");
        assert_eq!(body["order"]["side"], "BUY");
    }

    #[tokio::test]
    async fn fak_market_buy_propagates_sdk_http_error_unchanged() {
        let (host, server) = spawn_server(vec![
            PlannedResponse {
                status: "200 OK",
                body: VERSION_RESPONSE,
            },
            PlannedResponse {
                status: "503 Service Unavailable",
                body: r#"{"failure":"sdk-http-88"}"#,
            },
        ])
        .await;
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        cache_order_rules(&client, &["44"]);
        let intent = ModelIntentV1::SubmitSingleBuy {
            condition_id: condition(),
            order: BuyLegIntentV1 {
                token_id: token("44"),
                order: BuyOrderIntentV1::FakMarket {
                    spend_pusd: decimal("6"),
                    worst_price_pusd_per_share: decimal("0.25"),
                },
            },
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let error = bridge
            .execute((), &intent)
            .await
            .expect_err("503 应原样返回 SDK 错误");

        assert_eq!(error.kind(), ErrorKind::Status);
        let status = error
            .downcast_ref::<Status>()
            .expect("应保留 SDK Status 详情");
        assert_eq!(status.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status.method, Method::POST);
        assert_eq!(status.path, "/order");
        assert_eq!(status.message, r#"{"failure":"sdk-http-88"}"#);
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 2);
        let body = &captured[1].body;
        assert_eq!(body["orderType"], "FAK");
        assert_eq!(body["order"]["tokenId"], "44");
        assert_eq!(body["order"]["makerAmount"], "6000000");
        assert_eq!(body["order"]["takerAmount"], "24000000");
        assert_eq!(body["order"]["side"], "BUY");
    }

    #[tokio::test]
    async fn buy_build_failure_makes_zero_http_requests() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("应绑定回环端口");
        let host = format!("http://{}", listener.local_addr().expect("应取得回环地址"));
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        cache_order_rules(&client, &["55"]);
        let intent = ModelIntentV1::SubmitSingleBuy {
            condition_id: condition(),
            order: BuyLegIntentV1 {
                token_id: token("55"),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: decimal("0.555"),
                    size_shares: decimal("2"),
                    post_only: false,
                },
            },
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let error = bridge
            .execute((), &intent)
            .await
            .expect_err("未对齐价格应由 SDK 拒绝");

        assert_eq!(error.kind(), ErrorKind::Validation);
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "构建失败不应产生 HTTP 请求",
        );
    }

    #[tokio::test]
    async fn buy_sign_failure_makes_zero_submission_requests() {
        let (host, server) = spawn_server(vec![PlannedResponse {
            status: "200 OK",
            body: VERSION_RESPONSE,
        }])
        .await;
        let client_signer = test_signer!(POLYGON);
        let unsupported_signer = test_signer!(1);
        let client = authenticated_client(&host, &client_signer, None).await;
        cache_order_rules(&client, &["66"]);
        let intent = ModelIntentV1::SubmitSingleBuy {
            condition_id: condition(),
            order: BuyLegIntentV1 {
                token_id: token("66"),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: decimal("0.60"),
                    size_shares: decimal("2"),
                    post_only: false,
                },
            },
        };
        let mut bridge = IntentBridge::bind_live(&client, &unsupported_signer);

        let error = bridge
            .execute((), &intent)
            .await
            .expect_err("不支持的签名链应失败");

        assert_eq!(error.kind(), ErrorKind::Internal);
        let captured = finish_server(server).await;
        assert_eq!(
            captured.len(),
            1,
            "签名失败前只能有 SDK 版本读取，提交必须为零"
        );
        assert_eq!(captured[0].path, "/version");
    }

    #[tokio::test]
    async fn paired_second_build_failure_makes_zero_submission_requests() {
        let (host, server) = spawn_server(vec![PlannedResponse {
            status: "200 OK",
            body: VERSION_RESPONSE,
        }])
        .await;
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        cache_order_rules(&client, &["71", "72"]);
        let intent = ModelIntentV1::SubmitPairedBuy {
            condition_id: condition(),
            orders: [
                BuyLegIntentV1 {
                    token_id: token("71"),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: decimal("0.51"),
                        size_shares: decimal("2"),
                        post_only: false,
                    },
                },
                BuyLegIntentV1 {
                    token_id: token("72"),
                    order: BuyOrderIntentV1::GtdLimit {
                        limit_price_pusd_per_share: decimal("0.527"),
                        size_shares: decimal("3"),
                        post_only: true,
                        expires_at_utc_sec: 1_900_000_000,
                    },
                },
            ],
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let error = bridge
            .execute((), &intent)
            .await
            .expect_err("第二腿构建应失败");

        assert_eq!(error.kind(), ErrorKind::Validation);
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 1, "双腿全部成功前不得提交");
        assert_eq!(captured[0].path, "/version");
    }

    #[tokio::test]
    async fn cancel_order_calls_only_the_single_order_method() {
        let (host, server) = spawn_server(vec![PlannedResponse {
            status: "200 OK",
            body: CANCELLATION_RESPONSE,
        }])
        .await;
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        let intent = ModelIntentV1::CancelOrder {
            platform_order_id: "order-88".to_owned(),
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let result = bridge.execute((), &intent).await.expect("单撤应成功");

        let LiveIntentImmediateSuccessV1::Cancellation(response) = result else {
            panic!("应返回撤单即时响应");
        };
        assert_eq!(response.canceled, vec!["order-88"]);
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 1);
        assert_eq!(
            (captured[0].method.as_str(), captured[0].path.as_str()),
            ("DELETE", "/order")
        );
        assert_eq!(captured[0].body, json!({"orderID":"order-88"}));
    }

    #[tokio::test]
    async fn cancel_orders_preserves_duplicates_order_and_mixed_response() {
        let (host, server) = spawn_server(vec![PlannedResponse {
            status: "200 OK",
            body: MIXED_CANCELLATION_RESPONSE,
        }])
        .await;
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        let intent = ModelIntentV1::CancelOrders {
            platform_order_ids: vec![
                "duplicate-88".to_owned(),
                "middle-88".to_owned(),
                "duplicate-88".to_owned(),
            ],
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let result = bridge
            .execute((), &intent)
            .await
            .expect("批撤应返回混合响应");

        let LiveIntentImmediateSuccessV1::Cancellation(response) = result else {
            panic!("应返回撤单即时响应");
        };
        assert_eq!(response.canceled, vec!["duplicate-88"]);
        assert_eq!(
            response.not_canceled.get("middle-88").map(String::as_str),
            Some("already matched"),
        );
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 1);
        assert_eq!(
            (captured[0].method.as_str(), captured[0].path.as_str()),
            ("DELETE", "/orders")
        );
        assert_eq!(
            captured[0].body,
            json!(["duplicate-88", "middle-88", "duplicate-88"]),
        );
    }

    #[tokio::test]
    async fn cancel_market_orders_sets_only_market_scope() {
        let (host, server) = spawn_server(vec![PlannedResponse {
            status: "200 OK",
            body: CANCELLATION_RESPONSE,
        }])
        .await;
        let signer = test_signer!(POLYGON);
        let client = authenticated_client(&host, &signer, None).await;
        let intent = ModelIntentV1::CancelMarketOrders {
            condition_id: condition(),
        };
        let mut bridge = IntentBridge::bind_live(&client, &signer);

        let result = bridge.execute((), &intent).await.expect("市场撤单应成功");

        assert!(matches!(
            result,
            LiveIntentImmediateSuccessV1::Cancellation(_)
        ));
        let captured = finish_server(server).await;
        assert_eq!(captured.len(), 1);
        assert_eq!(
            (captured[0].method.as_str(), captured[0].path.as_str()),
            ("DELETE", "/cancel-market-orders"),
        );
        assert_eq!(
            captured[0].body,
            json!({"market": CONDITION_ID, "asset_id": null}),
            "SDK 请求只设置 market；asset_id 必须保持未设置",
        );
    }
}
