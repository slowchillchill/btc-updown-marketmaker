# Polymarket Rust SDK Gap Report

事项编号：`data-collect-demand`

对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

对应计划文档：`/home/chillchill/btc-updown-marketmaker/plan/data-collect-plan.md`

核对时间：`2026-07-06`

## 1. Verdict

`PASS`

官方 Polymarket Rust SDK 覆盖 Phase 0 后续技术测试所需的 Polymarket 能力，但正式采集实现不得只使用高层 typed stream 直接落盘。为满足 `raw_json`、source gap、subscription evidence 和 lag/reconnect trace 需求，Polymarket 接入必须使用官方 SDK 暴露的低层 WS 基础设施和 request/response 类型，构建采集证据 parser；这仍属于 SDK-first，不是 raw REST、manual signing、自建 SDK 或 shadow implementation。

本报告不授权任何 SDK 旁路。

## 2. Evidence

| 证据 | 结论 |
| --- | --- |
| 官方 Rust SDK 仓库：`https://github.com/Polymarket/rs-clob-client-v2`，本次核对 clone commit `3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`，`Cargo.toml` package `polymarket_client_sdk_v2` version `0.6.0` | SDK feature 包含 `clob`、`ws`、`rtds`、`gamma`；README 说明 `ws` 支持实时 orderbook/price/user stream，`rtds` 支持 Binance/Chainlink crypto prices，`gamma` 支持 discovery/metadata。 |
| 官方 Market Channel 文档：`https://docs.polymarket.com/market-data/websocket/market-channel` | market channel endpoint 是 `wss://ws-subscriptions-clob.polymarket.com/ws/market`；订阅字段包含 `assets_ids`、`type: "market"`、`custom_feature_enabled: true`；消息类型包含 `book`、`price_change`、`tick_size_change`、`last_trade_price`、`best_bid_ask`、`new_market`、`market_resolved`。 |
| 官方 RTDS 文档：`https://docs.polymarket.com/market-data/websocket/rtds` | RTDS endpoint 是 `wss://ws-live-data.polymarket.com`；Chainlink 订阅 topic 是 `crypto_prices_chainlink`，`type: "*"`，symbol filter 使用 slash 格式，例如 `btc/usd`。 |
| SDK source `src/clob/ws/types/request.rs` | `SubscriptionRequest` 序列化 `type: market`、`assets_ids`，并提供 `with_custom_features(true)` 写入 `custom_feature_enabled`。 |
| SDK source `src/clob/ws/types/response.rs` | `WsMessage` 覆盖 `Book`、`PriceChange`、`TickSizeChange`、`LastTradePrice`、`BestBidAsk`、`NewMarket`、`MarketResolved`；`parse_if_interested()` 基于 raw bytes 解析。 |
| SDK source `src/clob/ws/client.rs` / `src/clob/ws/subscription.rs` | 高层 client 支持 orderbook、price、tick size、best bid/ask、new markets、market resolutions；`subscribe_market_with_options(..., true)` 会启用 custom features 并在重连时重发。 |
| SDK source `src/rtds/types/request.rs` / `src/rtds/types/response.rs` / `src/rtds/client.rs` | `Subscription::chainlink_prices(Some("btc/usd"))` 生成 `crypto_prices_chainlink` + `type: "*"` + escaped JSON filter；`RtdsMessage` 和 `ChainlinkPrice` 保留 topic/type/message timestamp/payload timestamp/symbol/value。 |
| SDK source `src/gamma/client.rs` / `src/gamma/types/request.rs` / `src/gamma/types/response.rs` | Gamma client 支持 event/market/list/search/by slug/by id；market response 覆盖 condition id、slug、start/end、active、accepting orders、tick/min size、fee/reward、CLOB token ids 等 metadata。 |
| SDK source `src/ws/connection.rs` / `src/ws/traits.rs` | `ConnectionManager`、`MessageParser`、`send()`、`subscribe()`、`state_receiver()` 是 public SDK API；可用官方 SDK 的低层连接、request 类型和 typed response 类型承载 raw evidence parser。 |

## 3. Coverage Matrix

| 需求点 | SDK 覆盖判断 | Phase 0 后续要求 |
| --- | --- | --- |
| market channel 订阅 | 覆盖。SDK request 支持 `assets_ids`、`type: market`、`custom_feature_enabled`。 | 技术 probe 必须把实际 subscription JSON 或 digest 写入 evidence。 |
| `book` / `price_change` / `tick_size_change` / `last_trade_price` | 覆盖。SDK typed response 覆盖这些事件。 | 每条事件仍必须补充 `recv_time_utc`、collector host/region、raw_json。 |
| `best_bid_ask` / `new_market` / `market_resolved` | 覆盖。SDK custom feature stream 和 typed response 已覆盖。 | 后续 probe 必须证明 `custom_feature_enabled: true` 已发送，并记录首个业务事件或 gap。 |
| market discovery / metadata | 覆盖。Gamma 和 CLOB market endpoints 可组合得到 slug、condition/token/asset id、active/acceptingOrders、tick/min size、fee/reward。 | 对 `btc-updown-5m` 这类目标市场的 slug/query 选择必须在后续 source adapter 中写成可验证 discovery evidence。 |
| RTDS Chainlink BTC/USD | 覆盖。SDK RTDS 支持 `crypto_prices_chainlink`，symbol filter 可写 `btc/usd`。 | 后续 probe 必须记录 subscription raw JSON/digest、RTDS message timestamp、payload timestamp 和 stale/gap。 |
| raw_json 原始证据 | 高层 typed stream 不直接保留 raw_json；SDK 低层 `MessageParser` 可从 raw bytes 同时产出 raw_json + typed event。 | Polymarket source adapter 不得只消费高层 typed stream 落盘；必须用官方 SDK 低层 WS API 或等价 SDK 内公共 API保留 raw_json。 |
| lag / source gap 证据 | 高层 typed stream 对 broadcast lag 主要通过 tracing/内部处理；低层 `ConnectionManager::subscribe()` 暴露 receiver，可显式处理 lag/closed/state。 | source health/gap/reconnect trace 必须由 data-collect 自身记录，不得只依赖 SDK 内部日志。 |
| reconnect / backoff | SDK 有 reconnect/backoff 和 heartbeat；但 data-collect 仍需业务 freshness、有限重试策略和 source gap 语义。 | Phase 0 网络测试不得把 SDK 连接 state 或 PONG 单独当作 healthy。 |

## 4. SDK-First Boundary For Implementation

允许：

- 使用 `polymarket_client_sdk_v2` 官方 crate，启用 `clob`、`ws`、`rtds`、`gamma`、`tracing` 等所需 feature。
- 使用 SDK 高层 client 做能力 smoke test、metadata 查询或非持久化探针。
- 使用 SDK public 低层 WS 基础设施：`ws::ConnectionManager`、`ws::MessageParser`、`clob::ws::SubscriptionRequest`、`clob::ws::types::response::WsMessage`、`rtds::Subscription`、`rtds::RtdsMessage`，以保留 raw payload、typed event、recv_time 和 gap evidence。

禁止，除非未来单独得到用户批准并形成需求/计划/changelog 决策记录：

- 绕过 SDK 直接手写 Polymarket raw REST 或 raw WebSocket 协议作为主实现。
- manual signing、手写认证、复制 SDK 内部签名逻辑或建立自建 SDK。
- 因 SDK 高层 stream 不保留 raw_json，就放弃 raw_json、source gap 或 reconnect trace 需求。
- 把 Polymarket SDK 内部 reconnect/heartbeat 成功等同于业务数据 fresh。

## 5. Remaining Risks

1. 当前 verdict 只说明 SDK 覆盖 Phase 0 后续技术测试入口；真实 `btc-updown-5m` discovery、订阅确认、首条业务消息、静默 freshness 和 RTDS `btc/usd` live 数据仍要在 Phase 0 probe 中验证。
2. SDK 高层 stream 可读性更好，但证据边界不够；正式实现如果为了简单直接使用高层 stream 落 parquet，会遗漏 `raw_json` 和 source gap，必须在审查中拦截。
3. SDK reconnect 默认可无限重试；data-collect 需要外层按需求定义有限重试、backoff ceiling、jitter、circuit breaker、gap 和 recovery verdict。
4. 如果后续发现 SDK public 低层 API 在 crates.io 发布版本和 GitHub HEAD 不一致，必须停下并记录 crate 版本差异；不得临时切换 raw bypass。

## 6. Phase 0 Decision

Polymarket SDK 能力核对通过，可以进入 Phase 0 后续 writer/publish、throughput/backpressure、network recovery、ResourceWatermark 和服务器角色验证。

推进条件：

- 后续 Rust 依赖必须锁定 `polymarket_client_sdk_v2` 版本和 feature，并在 manifest 记录 source config digest。
- Polymarket source probe 必须先写测试证明 raw payload、typed event、`recv_time_utc`、subscription evidence、lag/gap 和 business freshness 都可记录。
- 若任何测试需要使用 SDK 未公开能力或修改 SDK 源码，必须回到用户决策。
