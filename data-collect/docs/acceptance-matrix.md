# data-collect Acceptance Matrix

对应需求：`demand/data-collect-demand.md`

本矩阵只覆盖 `./data-collect/` 的外部市场源采集、证据、发布和消费者边界；不把 `data-pipeline`、训练标签/特征、paper/live 交易运行逻辑或 SDK 旁路实现纳入 `data-collect`。

| # | 需求验收点 | 覆盖证据 |
|---|---|---|
| 1 | `data-collect` 职责边界只到 raw evidence、`recv_time_utc`、typed raw parquet、coverage、health/gap/reconnect、manifest、atomic rename、consumer boundary、网络韧性、ResourceWatermark | `event_envelope_contract`、`source_format_contract`、`consumer_format_compatibility_contract`、`training_consumer_boundary_contract`、`paper_live_boundary_contract`、Phase 6 非范围扫描 |
| 2 | 每条源事件保留 `recv_time_utc`；Polymarket 同时保留平台 `timestamp` 与 collector host/region | `event_envelope_contract`、`source_format_contract`、`parquet_writer_contract`、0.6.12 collector region smoke |
| 3 | 非爱尔兰 collector 延迟不冒充 paper/live 爱尔兰 runtime receive latency | `paper_live_boundary_contract`、`real_source_latency_contract`、0.6.11 hot/durable latency evidence |
| 4 | cold start、restart、rollover 覆盖 current active 与 `T-30s` next preload，并记录切换证据 | `market_discovery_probe_contract`、`source_scheduler_contract`、`e2e_collect_contract`、0.6.9/0.6.10/targeted rollover evidence；restart 复用 stateless cold-start discovery / resubscribe 合同 |
| 5 | `new_market`、metadata、condition id、token/asset id、start/end、lifecycle state 可追溯 | `polymarket_contract`、`source_format_contract`、0.6.9 discovery receipts |
| 6 | market discovery 证明 exact slug + active scan / metadata snapshot，slug-only 不完整 | `market_discovery_probe_contract`、`polymarket_contract` |
| 7 | Polymarket、Binance depth、Binance aggTrade、RTDS Chainlink 四源均有 coverage、health、gap、reconnect trace 与 typed fields | `source_format_contract`、`binance_contract`、`rtds_contract`、`e2e_collect_contract`、0.6.10/0.6.11 remote runs |
| 8 | `market_resolved` / retired predicate 通过事件路径或 fallback metadata 路径证明 | `polymarket_contract`、`source_format_contract` |
| 9 | `best_bid_ask`、`new_market`、`market_resolved` 证明 `custom_feature_enabled: true`、订阅确认和 SDK 支持状态 | `market_discovery_probe_contract`、`polymarket_contract`、0.6.10/targeted rollover evidence |
| 10 | Binance live 源来自官方 WS/REST，保留 aggTrade/depth/snapshot 原始字段并可审计 stitch predicate | `binance_contract`、`source_format_contract` |
| 11 | parquet running/temp segment、close/finalize 后 atomic rename，消费者只发现 finalized；metadata 含 layout version 与 source kind | `parquet_writer_contract`、`writer_publish_contract`、`finalized_discovery_contract`、`e2e_collect_contract` |
| 12 | manifest 包含路径、row count、sha256、schema/code/binary/config digest、verdict、coverage/gap、evidence refs、time window、ResourceWatermark、queue、flush、host/region、required files | `manifest_contract`、`finalized_discovery_contract`、`e2e_collect_contract` |
| 13 | paper/live append-only log、rotation/live artifacts、order evidence、NOE/ledger、execution feedback 由其他模块承接 | `paper_live_boundary_contract`、`partition_publish_contract`、Phase 6 非范围扫描 |
| 14 | training cold path 与 paper/live hot path 接入边界可验证 | `training_consumer_boundary_contract`、`paper_live_boundary_contract`、`consumer_format_compatibility_contract`、0.6.11 hot/durable latency evidence；当前只证明 `data-collect` hot-path/handoff 边界与 fake handoff 技术路径，真实 paper/live host runtime receive timestamp 由 paper/live runtime artifact 提供 |
| 15 | 网络异常、空响应、业务静默、ping/pong 正常但 stale、rate limit、shutdown、snapshot 缺失、source stale 写入 health/gap/reconnect | `health_gap_contract`、`network_recovery_contract`、`reconnect_trace_contract`、`binance_contract`、`rtds_contract` |
| 16 | 恢复策略有限重试、backoff、jitter、max attempts/elapsed/ceiling、按源独立熔断，重连后重新验证订阅、snapshot、freshness、coverage、RTDS/Binance 状态 | `reconnect_trace_contract`、`network_recovery_contract`、`rtds_contract`、`binance_contract` |
| 17 | reconnect trace 记录 attempt、sleep、错误类别、最终状态和 open/half-open/recovered/exhausted | `reconnect_trace_contract`、`network_recovery_contract` |
| 18 | Rust async/non-blocking：主循环不做阻塞 I/O/CPU；blocking work 隔离；bounded queue 与 backpressure | `parquet_writer_contract`、`bounded_queue_contract`、`backpressure_contract`、`resource_watermark_contract` |
| 19 | 性能验收覆盖 p95/max、2k-5k events/s、逐源吞吐/burst/drop/gap、writer queue、flush latency、ResourceWatermark、hot/durable latency、hardware tier | `throughput_probe`、`real_source_latency_contract`、0.6.10/0.6.11/0.6.12 remote evidence、Phase 6 final bench |
| 20 | Phase 0 技术验收真实验证 `btc-updown-5m` discovery、rollover、四源真实流、UTC window 对齐、hot/durable latency | `data-collect/docs/phase0-technical-validation.md` 0.6.9-0.6.12 与 12.11 |
| 21 | ResourceWatermark 硬阈值包含 threshold policy receipt 与保护动作结果 | `resource_watermark_contract`、`manifest_contract`、`writer_publish_contract`、`finalized_discovery_contract` |
| 22 | archive/upload/listing/gzip 不属于当前验收 | `demand/data-collect-demand.md` 1.2、`plan/data-collect-plan.md` 决策门、Phase 6 非范围扫描 |
| 23 | Polymarket 采集路径先有官方 Rust SDK 能力核对；SDK 缺口需用户同意 | `data-collect/docs/polymarket-sdk-gap-report.md`、`data-collect/docs/phase0-technical-validation.md` §6、`polymarket_contract`、`rtds_contract`、`market_discovery_probe_contract`；Phase 0 SDK-first WebSocket smoke 只作为 reachability evidence，actual outbound frame 未捕获时正式 adapter 必须补 request evidence 或 unavailable reason |
| 24 | 文档、计划或实现不把 data-pipeline labels/features/tick_stream/split/T-30 样本、paper/live 交易逻辑或 SDK 旁路写进 `data-collect` | `consumer_format_compatibility_contract`、`training_consumer_boundary_contract`、`paper_live_boundary_contract`、Phase 6 非范围扫描 |

## Phase 6 Verification Commands

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --test e2e_collect_contract -- --nocapture
rg -n "labels|features|tick_stream|canonical split|purged|embargo|reward|PnL|NAV|ledger|NormalizedOrderEvent|submit|cancel|manual signing|shadow implementation" data-collect/src data-collect/tests data-collect/docs
cargo fmt --all -- --check
git diff --check
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests --quiet
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo bench -p data-collect --bench throughput_probe -- --sample-size 20
```
