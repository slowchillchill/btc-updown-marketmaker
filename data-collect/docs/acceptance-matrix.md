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
| 18 | Rust async/non-blocking：主循环不做阻塞 I/O/CPU；blocking work 隔离；bounded queue 与 backpressure | `parquet_writer_contract`、`bounded_queue_contract`、`backpressure_contract`、`resource_watermark_contract`、Phase 7 live runtime tests；可恢复 wait 进入 ResourceWatermark，timeout/drop 进入 source gap 或 dropped_rows |
| 19 | 性能验收覆盖 p95/max、2k-5k events/s、逐源吞吐/burst/drop/gap、writer queue、flush latency、ResourceWatermark、hot/durable latency、hardware tier | `throughput_probe`、`real_source_latency_contract`、0.6.10/0.6.11/0.6.12 remote evidence、Phase 6 final bench、Phase 7 已通过 5 分钟 smoke 与 2 小时 systemd gate；Step 10.4 产物收敛后二小时 gate 待重跑 |
| 20 | Phase 0 技术验收真实验证 `btc-updown-5m` discovery、rollover、四源真实流、UTC window 对齐、hot/durable latency | `data-collect/docs/phase0-technical-validation.md` 0.6.9-0.6.12 与 12.11 |
| 21 | ResourceWatermark 硬阈值包含 threshold policy receipt 与保护动作结果 | `resource_watermark_contract`、`manifest_contract`、`writer_publish_contract`、`finalized_discovery_contract` |
| 22 | archive/upload/listing/gzip 不属于当前验收 | `demand/data-collect-demand.md` 1.2、`plan/data-collect-plan.md` 决策门、Phase 6 非范围扫描 |
| 23 | Polymarket 采集路径先有官方 Rust SDK 能力核对；SDK 缺口需用户同意 | `data-collect/docs/polymarket-sdk-gap-report.md`、`data-collect/docs/phase0-technical-validation.md` §6、`polymarket_contract`、`rtds_contract`、`market_discovery_probe_contract`；Phase 0 SDK-first WebSocket smoke 只作为 reachability evidence，actual outbound frame 未捕获时正式 adapter 必须补 request evidence 或 unavailable reason |
| 24 | 文档、计划或实现不把 data-pipeline labels/features/tick_stream/split/T-30 样本、paper/live 交易逻辑或 SDK 旁路写进 `data-collect` | `consumer_format_compatibility_contract`、`training_consumer_boundary_contract`、`paper_live_boundary_contract`、Phase 6 非范围扫描 |
| 25 | 训练、paper、live 三运行形态能独立导出 release 二进制、sha256 和运行说明；训练有部署 README，paper/live 有 hot path 接入文档 | `deployment_binary_contract`、`deploy_readme_contract`、`hot_path_handoff_contract`、`data-collect/scripts/export-release-artifacts.sh`、`data-collect/docs/training-collector-deploy-readme.md`、`data-collect/docs/paper-live-hot-path-integration.md`、Phase 7 release export；paper/live 当前阶段只验证 frame contract、heartbeat/status server 和 fake consumer，不声明已完成真实 paper/live 外部市场源长跑 |
| 26 | 训练 cold mirror 按 `source_kind + recv_time_utc UTC day` 分区，每个 source/day 可包含多个 finalized segment；manifest 证明 recv window、source timestamp range、market/window、soft rollover reason、row count 和 sha256 | `causal_time_contract`、`segment_rollover_contract`、`training_collector_runtime_contract`、Phase 7 15 分钟 smoke 和已通过 2 小时 systemd run 的 finalized segment / `ManifestDraft` complete sidecar；Step 10.4 新产物口径待远端重验 |
| 27 | `max_rows`、`max_bytes`、`max_segment_duration`、UTC day boundary 和 shutdown finalize 只触发 soft rollover 或保全发布；正常行情放大不因 soft rollover 自动停机 | `segment_rollover_contract`、`training_collector_runtime_contract`、Phase 7 已通过 2 小时 systemd run：33 个 finalized segment、service stop 后 `Result=success`、source gaps `[]`、partial_count `0`；Step 10.4 shutdown 修复后待重跑 |
| 28 | 训练样本因果可见性使用采集服务器 `recv_time_utc` / availability time；paper/live 因果可见性使用对应服务器 runtime receive timestamp；source/platform timestamp 不替代 receive/available time | `causal_time_contract`、`hot_path_handoff_contract`、`paper_live_boundary_contract`、`data-collect/docs/paper-live-hot-path-integration.md` |
| 29 | paper/live 二进制 hot path 接入文档明确 IPC/local stream/bounded handoff 协议、event seq、timestamp namespace、backpressure/status、runtime receive timestamp 和非范围边界 | `hot_path_handoff_contract`、`data-collect/docs/paper-live-hot-path-integration.md`、Phase 7 paper/live 非部署 gate；当前阶段不把 heartbeat/status handoff server 冒充为已验收真实 `external_market_event` runtime |
| 30 | PMXT-inspired Polymarket orderbook hourly layout 保留完整 `bids` / `asks`，`book` 不得缩水为 `bids_len` / `asks_len`，并覆盖 `price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` typed fields 或 unavailable reason | `pmxt_orderbook_layout_contract`、`polymarket_metadata_layout_contract`、`parquet_writer_contract`、`polymarket_contract`、PMXT sample comparison、manual-review complete market artifact |
| 31 | Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 均使用 source-specific hourly typed schema，不能只依赖 opaque `source_kind/raw_json` 宽表 | `source_hourly_layout_contract`、`binance_depth_stitch_contract`、`parquet_writer_contract`、`source_format_contract`、manual-review source coverage |
| 32 | 新训练产物使用 day-first hourly logical partition：`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH[--part=N].parquet`；legacy source-first 路径只读兼容且显式标记，no mixed publish | `hourly_partition_contract`、`partition_publish_contract`、`finalized_discovery_contract`、`training_collector_runtime_contract`、remote cross-hour systemd run |
| 33 | running/digesting/tmp/`.rsync-partial` 不进入 finalized discovery；manual-review shutdown 可 finalize 当前 hourly part，并记录 `manual_review_complete_market` 与 shutdown rollover reason | `hourly_partition_contract`、`manual_review_complete_market_contract`、`finalized_discovery_contract`、remote manual-review complete market artifact |
| 34 | 50GB training retention 以 `datasets/training` 为阈值，默认 dry-run/no-op policy receipt；真实删除只限最老 finalized complete UTC day，不删 current/running/incomplete/manual-inspection/run manifests/startup receipts | `training_retention_contract`、`resource_watermark_contract`、`manifest_contract`、remote small-threshold retention validation |
| 35 | PMXT-like hourly parquet 属于训练 cold path；paper/live hot path 继续消费实时 handoff，不等待 finalized parquet；本计划不生成 labels、features、tick_stream、replay context 或训练切分 | `paper_live_boundary_contract`、`training_consumer_boundary_contract`、`consumer_format_compatibility_contract`、`data-collect/docs/paper-live-hot-path-integration.md` |

## Phase 6 Verification Commands

```bash
CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test e2e_collect_contract -- --nocapture
rg -n "labels|features|tick_stream|canonical split|purged|embargo|reward|PnL|NAV|ledger|NormalizedOrderEvent|submit|cancel|manual signing|shadow implementation" data-collect/src data-collect/tests data-collect/docs
cargo fmt --all -- --check
git diff --check
CARGO_TARGET_DIR=target/data-collect-ci cargo clippy -p data-collect --all-targets -- -D warnings
CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --lib --tests --quiet
CARGO_TARGET_DIR=target/data-collect-ci cargo bench -p data-collect --bench throughput_probe -- --sample-size 20
```

## Phase 7 Verification Commands

```bash
cargo fmt --all -- --check
git diff --check
CARGO_TARGET_DIR=target/data-collect-ci cargo clippy -p data-collect --all-targets -- -D warnings
CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --lib --tests --quiet
CARGO_TARGET_DIR=target/data-collect-ci cargo build -p data-collect --release \
  --bin data-collect-training-collector \
  --bin data-collect-paper-hot \
  --bin data-collect-live-hot
data-collect/scripts/export-release-artifacts.sh
```
