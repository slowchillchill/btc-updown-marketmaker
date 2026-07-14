# data-collect Acceptance Matrix

对应需求：`demand/data-collect-demand.md`；当前可靠性修复覆盖：`demand/data-collect-reliability-repair-demand.md`

本矩阵只覆盖 `./data-collect/` 的外部市场源采集、证据、发布和消费者边界；不把 `data-pipeline`、训练标签/特征、paper/live 交易运行逻辑或 SDK 旁路实现纳入 `data-collect`。

| # | 需求验收点 | 覆盖证据 |
|---|---|---|
| 1 | `data-collect` 职责边界只到 raw evidence、`recv_time_utc`、typed raw parquet、每日采集事实 manifest、atomic rename、consumer boundary 与网络韧性；训练采集生产路径不生成 ResourceWatermark/receipt/sidecar | `event_envelope_contract`、`source_format_contract`、`consumer_format_compatibility_contract`、`training_consumer_boundary_contract`、`paper_live_boundary_contract`、`training_collector_runtime_contract` |
| 2 | 每条源事件保留 `recv_time_utc`；Polymarket 同时保留平台 `timestamp` 与 collector host/region | `event_envelope_contract`、`source_format_contract`、`parquet_writer_contract`、0.6.12 collector region smoke |
| 3 | 非爱尔兰 collector 延迟不冒充 paper/live 爱尔兰 runtime receive latency | `paper_live_boundary_contract`、`real_source_latency_contract`、0.6.11 hot/durable latency evidence |
| 4 | cold start、restart、rollover 覆盖 current active 与 `T-30s` next preload，并记录切换证据 | `market_discovery_probe_contract`、`source_scheduler_contract`、`e2e_collect_contract`、0.6.9/0.6.10/targeted rollover evidence；restart 复用 stateless cold-start discovery / resubscribe 合同 |
| 5 | `new_market`、metadata、condition id、token/asset id、start/end、lifecycle state 可追溯 | `polymarket_contract`、`source_format_contract`、0.6.9 discovery receipts |
| 6 | market discovery 证明 exact slug + active scan / metadata snapshot，slug-only 不完整 | `market_discovery_probe_contract`、`polymarket_contract` |
| 7 | Polymarket、Binance depth、Binance aggTrade、RTDS Chainlink 四源均有 typed fields；训练采集生产路径把 coverage/health 事实、紧凑 gap 与 reconnect 计数收敛到每日 manifest，详细错误只进 journald | `source_format_contract`、`binance_contract`、`rtds_contract`、`daily_manifest`/`live_runtime` 单元测试、0.6.10/0.6.11 remote runs |
| 8 | `market_resolved` / retired predicate 通过事件路径或 fallback metadata 路径证明 | `polymarket_contract`、`source_format_contract` |
| 9 | `best_bid_ask`、`new_market`、`market_resolved` 证明 `custom_feature_enabled: true`、订阅确认和 SDK 支持状态 | `market_discovery_probe_contract`、`polymarket_contract`、0.6.10/targeted rollover evidence |
| 10 | Binance live 源来自官方 WS/REST，保留 aggTrade/depth/snapshot 原始字段并可审计 stitch predicate | `binance_contract`、`source_format_contract` |
| 11 | parquet running/temp segment、close/finalize 后 atomic rename，消费者只发现 finalized；metadata 含 layout version 与 source kind | `parquet_writer_contract`、`writer_publish_contract`、`finalized_discovery_contract`、`e2e_collect_contract` |
| 12 | 每 UTC 日只有一个 manifest owner 和一个 `manifest.json`；记录 finalized 文件事实、逐 source 首末接收时间、写入/结构去重/backfill/lag/reconnect/drop 计数、紧凑 gap、Binance 连续性与 build/config/host/region identity，不含训练 verdict 或 receipt 大字段 | `daily_manifest` 单元测试、`binance_aggtrade_continuity_contract`、`stale_temporary_cleanup_contract`、`training_collector_runtime_contract` |
| 13 | paper/live append-only log、rotation/live artifacts、order evidence、NOE/ledger、execution feedback 由其他模块承接 | `paper_live_boundary_contract`、`partition_publish_contract`、Phase 6 非范围扫描 |
| 14 | training cold path 与 paper/live hot path 接入边界可验证 | `training_consumer_boundary_contract`、`paper_live_boundary_contract`、`consumer_format_compatibility_contract`、0.6.11 hot/durable latency evidence；当前只证明 `data-collect` hot-path/handoff 边界与 fake handoff 技术路径，真实 paper/live host runtime receive timestamp 由 paper/live runtime artifact 提供 |
| 15 | 网络异常、空响应、业务静默、ping/pong 正常但 stale、rate limit、shutdown、snapshot 缺失与 source stale 必须进入有界恢复，并在每日 manifest 留下 gap/计数或以进程错误进入 journald | `health_gap_contract`、`network_recovery_contract`、`reconnect_trace_contract`、`binance_contract`、`rtds_contract`、`live_runtime` 恢复测试 |
| 16 | 恢复策略有限重试、backoff、jitter、max attempts/elapsed/ceiling、按源独立熔断，重连后重新验证订阅、snapshot、freshness、coverage、RTDS/Binance 状态 | `reconnect_trace_contract`、`network_recovery_contract`、`rtds_contract`、`binance_contract` |
| 17 | 恢复算法覆盖 attempt、sleep、错误类别、最终状态与 recovered/exhausted；训练采集生产中只持久化每日 reconnect 计数/紧凑 gap，不生成 reconnect trace sidecar | `reconnect_trace_contract`、`network_recovery_contract`、`daily_manifest`/`live_runtime` 单元测试 |
| 18 | Rust async/non-blocking：主循环不做阻塞 I/O/CPU；blocking work 隔离；bounded queue 与 backpressure；可恢复 wait 不冒充 gap，timeout/drop 进入每日 gap 或 `dropped_rows` | `parquet_writer_contract`、`bounded_queue_contract`、`backpressure_contract`、`live_runtime` queue/inflight 测试 |
| 19 | 性能验收覆盖 2k-5k events/s、四源吞吐、全局 row/byte 预算、单行 1MiB、row group 50,000 rows/16MiB、bounded queue 与 backpressure | `source_hourly_layout_probe`、`throughput_probe`、`inflight`/writer 单元测试；新 collector 的远端 gate 与 26 小时长期验收在实现独立审查后执行 |
| 20 | Phase 0 技术验收真实验证 `btc-updown-5m` discovery、rollover、四源真实流、UTC window 对齐、hot/durable latency | `data-collect/docs/phase0-technical-validation.md` 0.6.9-0.6.12 与 12.11 |
| 21 | `datasets/training` 达到 50GB 时复用协调停止，finalize 后退出 75；不生成 ResourceWatermark/retention receipt，systemd 对 75/76 不重启 | `training_collector_runtime_contract`、仓库唯一 systemd unit、`live_runtime` capacity tests |
| 22 | archive/upload/listing/gzip 不属于当前验收 | `demand/data-collect-demand.md` 1.2、`plan/data-collect-plan.md` 决策门、Phase 6 非范围扫描 |
| 23 | Polymarket 采集路径先有官方 Rust SDK 能力核对；SDK 缺口需用户同意 | `data-collect/docs/polymarket-sdk-gap-report.md`、`data-collect/docs/phase0-technical-validation.md` §6、`polymarket_contract`、`rtds_contract`、`market_discovery_probe_contract`；Phase 0 SDK-first WebSocket smoke 只作为 reachability evidence，actual outbound frame 未捕获时正式 adapter 必须补 request evidence 或 unavailable reason |
| 24 | 文档、计划或实现不把 data-pipeline labels/features/tick_stream/split/T-30 样本、paper/live 交易逻辑或 SDK 旁路写进 `data-collect` | `consumer_format_compatibility_contract`、`training_consumer_boundary_contract`、`paper_live_boundary_contract`、Phase 6 非范围扫描 |
| 25 | 训练、paper、live 三角色保持独立源码 binary；训练部署只构建和部署一个固定路径 binary 与一个 unit，不再导出多 binary bundle；paper/live 仍可分别单独构建和部署 | `deployment_binary_contract`、`deploy_readme_contract`、`hot_path_handoff_contract`、训练部署 README、paper/live hot path 接入文档 |
| 26 | 训练 cold mirror 按 UTC day/source/hour 分区；正常每 source/hour 一个 finalized Parquet，同小时重启碰撞才使用 `part-N`；daily manifest 记录行数、字节数、SHA 与时间范围 | `hourly_partition_contract`、`parquet_writer_contract`、`training_collector_runtime_contract`、daily manifest tests |
| 27 | 50,000 rows/16MiB 只结束 Parquet row group，不创建新 part；UTC hour、同小时重启碰撞和协调 shutdown 分别遵守文件/finalize 合同 | `source_hourly_layout_contract`、`source_hourly_layout_probe`、`hourly_partition_contract`、`training_collector_runtime_contract` |
| 28 | 训练样本因果可见性使用采集服务器 `recv_time_utc` / availability time；paper/live 因果可见性使用对应服务器 runtime receive timestamp；source/platform timestamp 不替代 receive/available time | `causal_time_contract`、`hot_path_handoff_contract`、`paper_live_boundary_contract`、`data-collect/docs/paper-live-hot-path-integration.md` |
| 29 | paper/live 二进制 hot path 接入文档明确 IPC/local stream/bounded handoff 协议、event seq、timestamp namespace、backpressure/status、runtime receive timestamp 和非范围边界 | `hot_path_handoff_contract`、`data-collect/docs/paper-live-hot-path-integration.md`、Phase 7 paper/live 非部署 gate；当前阶段不把 heartbeat/status handoff server 冒充为已验收真实 `external_market_event` runtime |
| 30 | PMXT-inspired Polymarket orderbook hourly layout 保留完整 `bids` / `asks`，`book` 不得缩水为 `bids_len` / `asks_len`，并覆盖 `price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` typed fields或明确 unavailable reason | `pmxt_orderbook_layout_contract`、`polymarket_metadata_layout_contract`、`parquet_writer_contract`、`polymarket_contract` |
| 31 | Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 均使用 source-specific hourly typed schema，不能只依赖 opaque `source_kind/raw_json` 宽表 | `source_hourly_layout_contract`、`binance_depth_stitch_contract`、`parquet_writer_contract`、`source_format_contract`、manual-review source coverage |
| 32 | 新训练产物使用 day-first hourly logical partition：`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_HH[-part-N].parquet`；legacy source-first 路径只读兼容且显式标记，no mixed publish | `hourly_partition_contract`、`partition_publish_contract`、`finalized_discovery_contract`、`training_collector_runtime_contract`、remote cross-hour systemd run |
| 33 | `*.running`、`manifest.json.tmp` 与 transfer partial 不进入 finalized discovery；协调 shutdown finalize 当前小时文件，但当前 UTC 日仍保持 open | `hourly_partition_contract`、`finalized_discovery_contract`、`stale_temporary_cleanup_contract`、daily manifest tests |
| 34 | collector 不做 retention 或自动删除；`datasets/training` 达到 50GB 时完成 drain/finalize/open-manifest 更新并退出 75，等待独立归档运维 | `training_collector_runtime_contract`、capacity scan tests、仓库唯一 systemd unit |
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
```
