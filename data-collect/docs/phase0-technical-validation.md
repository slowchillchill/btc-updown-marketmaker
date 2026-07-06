# data-collect Phase 0 Technical Validation

事项编号：`data-collect-demand`

对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

对应计划文档：`/home/chillchill/btc-updown-marketmaker/plan/data-collect-plan.md`

## 0. 执行授权与边界

- 执行授权：用户已在 2026-07-06 明确批准执行 `plan/data-collect-plan.md`。
- 服务器测试授权：本地开发与测试通过后，允许使用数据采集服务器 `ubuntu@3.255.156.183` 做测试验证。
- 数据采集服务器清理授权：为新采集程序测试验证腾出磁盘空间时，允许终止数据采集服务器上当前运行的采集程序，并允许删除该服务器上的旧采集程序和旧采集数据。
- 当前阶段：Phase 0 技术测试与性能验证已完成。0.6.11 hot path / parquet path 远端真实四源 60 分钟补证已完成；0.6.12 已在阶段 0 内修复并验证 `collector_region=unknown` 缺口；0.7.2 远端 verdict 已重新汇总为 `PASS_TO_PHASE1_REVIEW`。0.7.3 用户审查 gate 已按用户要求由 4 个独立子代理完成，综合结论为 `ACCEPT_WITH_NOTES`；阶段收尾 commit/push 已完成。后续可从阶段 1 开始，但仍不得创建超出计划的正式 collector runtime、不得部署、不得接入 paper/live。
- Polymarket 约束：Polymarket 相关实现必须官方 Rust SDK-first；若 SDK 能力不足，必须先形成缺口报告并获得用户同意，不得自行启用 raw REST、manual signing、自建 SDK 或 shadow implementation。
- 本地网络约束：用户已说明本地电脑网络环境不能直接访问 Polymarket；后续如需在本地电脑测试或采集 Polymarket live-source 数据，必须使用本地已配置的香港 VPN 线路。当前 Phase 0 synthetic writer/throughput/network/resource 合同测试没有发起本地 Polymarket live-source 采集；若未来本地直连失败，必须先记录 VPN/线路状态，不得直接归因于 SDK 或采集程序。

## 1. 路径与主机口径

| 项目 | 当前口径 |
| --- | --- |
| 本地开发/文档/代码/构建/cargo/bench 仓库 | `/home/chillchill/btc-updown-marketmaker` |
| 远端服务器源码/测试工作目录写法 | `~/btc-updown-marketmaker` |
| `ubuntu` 用户服务器上 `~` 的常见展开 | `/home/ubuntu`；不得误作本地路径 |
| AWS live-trade 服务器运行产物根 | `/home/ubuntu/live-trade` |
| 数据采集服务器 | `ssh ubuntu@3.255.156.183` |
| AWS live-trade 服务器 | `ssh ubuntu@108.130.74.143` |

## 2. 阶段 0 验证矩阵

| 验证域 | 必须覆盖 | 当前状态 |
| --- | --- | --- |
| Polymarket Rust SDK 能力 | market channel、RTDS、market discovery、metadata、`custom_feature_enabled`、`new_market`、`market_resolved` | `PASS`；见 `data-collect/docs/polymarket-sdk-gap-report.md` |
| 四源采集 | Polymarket `btc-updown-5m` market channel、Binance `BTCUSDT` depth、Binance `BTCUSDT` aggTrade、RTDS Chainlink `btc/usd` | `PASS_FOR_PHASE0_REMOTE_PROBE`；0.6.9 cold-start discovery 与 0.6.10 60 分钟真实四源长跑通过，见 12.8 |
| 时间语义 | `recv_time_utc`、source timestamp、collector host/region、host clock sync status | writer 合同已覆盖 `recv_time_utc`、source timestamp、collector host/region；host clock sync status 待真实 runtime receipt |
| raw evidence | parseable `raw_json`、source kind、source event type、source identifier、typed raw parquet | writer probe 已通过 |
| coverage/health | source coverage、source health、source gap、reconnect trace、manifest | `PASS_FOR_PHASE0_REMOTE_PROBE`；0.6.10 manifest/source_coverage/source_gaps 已记录，source_gaps 为空 |
| 性能 | Polymarket p95/max 事件量、四源合并 `2k-5k events/s`、writer queue、flush latency、hot path / durable path latency | synthetic throughput probe 已通过；0.6.10 真实四源 60 分钟长跑采集 `177573` 行；0.6.11 远端 hot/durable 补证采集 `181091` 行，hot p95 `1ms`，durable write-done p95 `1004ms`，本地 `write_events_us` p95 `382us` |
| writer/publish | temp/running parquet、close/finalize、sha256、manifest、required_files[]、atomic rename、finalize timing | writer/publish probe 已通过；0.6.11 finalized parquet sha256 `e78c6712eb23a2f02bc0425b319ca027ffefb25c2249d6562b0ed51068f37e21`，`finalize_total_us=480767`，`atomic_rename_us=75` |
| 网络异常 | 连接失败、断链、server shutdown、错误返回、空 payload、业务静默、ping/pong 正常但 stale、rate limit、snapshot 缺失、source stale | network recovery 合同已通过；真实源恢复待 adapter/远端验证 |
| ResourceWatermark | RSS、CPU、disk free、artifact dir bytes、journal/log bytes、open files、queue bytes/age、writer memory estimate、cgroup memory、PSI、anon/file/slab、stage latency/TTL、unavailable reason | ResourceWatermark 合同与只读服务器资源核对已通过；0.6.11 sidecar 采样 `717` 条，RSS 峰值 `142856192` bytes，CPU 稳定段约 `6-7%`，open files 峰值 `24`，disk free 最低 `57630863360` bytes，CPU/IO/memory PSI full avg10 无非零采样；生产级 runtime manifest 仍需补齐完整采样 |
| 服务器角色 | 数据采集服务器 hardware tier；live-trade 小主机只做低影响运行边界核对 | 0.6 只读核对已完成 |

## 3. 运行根与 artifact 根 receipt

本节包含 Phase 0 receipt。用户已决定最终生产数据根路径为各运行服务器本机 `~/btc-updown-marketmaker/datasets/`；其中 `~` 按当前服务器用户解析。Phase 0 仍必须在服务器验证时记录 canonical path、owner/group/mode、sha256/manifest 证据和用户审查 verdict。

| 字段 | 当前值 | 状态 |
| --- | --- | --- |
| `local_dev_repo` | `/home/chillchill/btc-updown-marketmaker` | 已确认 |
| `local_phase0_report` | `data-collect/docs/phase0-technical-validation.md` | 已创建 |
| `data_collect_run_root` | Phase 0 旧 synthetic/contract 临时测试根：服务器 `~/btc-updown-marketmaker/data-collect/phase0-runs`，解析为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs`；真实四源长跑使用 `~/btc-updown-marketmaker/datasets/phase0-validation/<run_id>` 作为验证 artifact 根 | 已确认；0.6.10 已在真实源验证 artifact 根通过 |
| `data_collect_artifact_root` | Phase 0 真实源验证 artifact 根：数据采集服务器 `~/btc-updown-marketmaker/datasets/phase0-validation/<run_id>`；不得冒充正式 UTC day 生产分区 | 已由用户批准根路径；0.6.10 run dir `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T021650Z` 通过；0.6.11 run dir `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-20260706T092424Z` 通过 |
| `training_utc_day_partition_root` | 数据服务器本机 `~/btc-updown-marketmaker/datasets/` 下按 UTC day 分区；具体子目录、owner/group/mode 和 manifest receipt 需在写入前验证 | 已由用户决定根路径，非当前阻塞项 |
| `paper_live_handoff_artifact_root` | paper/live 交易服务器本机 `~/btc-updown-marketmaker/datasets/`；只承载 handoff mirror/coverage/manifest，不替代 paper/live append-only log 或交易 runtime artifact | 已由用户决定根路径，非当前阻塞项 |
| `allowed_reader_roles` | Phase 0 仅本地操作者通过 SSH 以服务器 `ubuntu` 用户读取测试证据；未授权 paper/live/训练消费者读取该临时根 | 已确认，仅限 Phase 0 |
| `allowed_writer_roles` | Phase 0 仅本地操作者通过 SSH 以服务器 `ubuntu` 用户写入测试证据；正式 collector writer 角色待后续阶段定义 | 已确认，仅限 Phase 0 |
| `fail_closed_policy` | 路径越界、权限不匹配、owner/group 不符合 receipt、解析到 `/home/ubuntu/live-trade` 非授权子路径时 fail closed | 已定义 |
| `user_review_verdict` | 0.7.2 已重新汇总为 `PASS_TO_PHASE1_REVIEW`；0.7.3 用户审查 gate 已按用户要求由 4 个独立子代理完成，综合结论 `ACCEPT_WITH_NOTES`；阶段收尾 commit/push 已完成 | 已接受，阶段收尾完成 |

## 4. 只读环境核对

执行时间：`2026-07-05T21:07:52Z`

### 4.1 文档状态与初始文件状态

```bash
git status --short -- demand/data-collect-demand.md plan/data-collect-plan.md changelog/data-collect-changelog.md
find data-collect -maxdepth 3 -type f -printf '%p\n' | sort
```

结果：

- `git status --short -- ...` 无输出，三份 data-collect 文档在基线核对时相对 HEAD clean。
- `find data-collect ...` 无输出，`data-collect/` 尚无实现文件。

### 4.2 本地主机 hardware tier

```text
hostname: chillchill-AMD128G
kernel: Linux chillchill-AMD128G 7.0.0-27-generic #27-Ubuntu SMP PREEMPT_DYNAMIC Thu Jun 18 19:13:49 UTC 2026 x86_64 GNU/Linux
cpu_threads: 32
memory: 115Gi total, 108Gi available at baseline
swap: 8.0Gi total, 0B used at baseline
filesystem: /dev/nvme0n1p2 1.9T total, 1.7T used, 84G available, 96% used
ulimit_nofile: 524288
rustc: 1.95.0 (59807616e 2026-04-14)
cargo: 1.95.0 (f2d3ce0bd 2026-03-21)
```

本地磁盘已用 96%，Phase 0 本地 bench 和 artifact 必须控制写入量，避免把磁盘压力误判为 collector 技术路径问题。

### 4.3 本地磁盘水位更新

用户已在 2026-07-06 告知本地磁盘已释放空间。复核时间：`2026-07-05T21:18:18Z`。

```text
filesystem: /dev/nvme0n1p2 1.9T total, 457G used, 1.3T available, 26% used
available_bytes: 1421072707584
```

当前本地磁盘不再是 Phase 0 bench 的主要限制，但 ResourceWatermark 仍必须记录 disk free、artifact dir bytes 和相关 unavailable reason。

## 5. Phase 0 当前 verdict

local probe verdict：`LOCAL_PROBE_PASS_FOR_SYNTHETIC_CONTRACTS`

remote Phase 0 verdict：`PASS_TO_PHASE1_REVIEW`；旧 generic WebSocket smoke 形成的 `PASS_TO_PHASE1_REVIEW` 曾被撤销，0.6.9/0.6.10/targeted rollover 曾重新打开 durable mirror / 四源 coverage 维度，0.6.11 补齐 hot path / parquet path 延迟证据，0.6.12 已修复并验证 `collector_region=unknown` 缺口。

overall Phase 0 status：`PHASE0_COMPLETE_READY_FOR_PHASE1`；远端技术验证已满足进入阶段 1 的技术条件，0.7.3 用户审查 gate 已按用户要求由 4 个独立子代理完成，阶段收尾 commit/push 已完成。

含义：本地 SDK 能力核对、writer/publish、synthetic throughput/backpressure、network recovery contract、ResourceWatermark contract 和 clippy 验证已经通过；数据采集服务器已完成清理 receipt、5-10 分钟 smoke、45 分钟 synthetic throughput / ResourceWatermark 预检、HTTP reachability smoke、0.6.9 真实 `btc-updown-5m` cold-start discovery、0.6.10 真实四源 60 分钟长跑、审查后 15 分钟 targeted rollover 补证、0.6.11 真实四源 60 分钟 hot path / parquet path 延迟补证，以及 0.6.12 collector region 自动识别补证。旧 generic WebSocket smoke 仍只作为 reachability evidence，不再作为真实源通过依据。

已完成的用户审查 gate 结论和剩余边界：

1. 用户已要求派生 subagents 替其完成用户审查 gate；四个独立审查均为 `PASS_WITH_NOTES` / `ACCEPT_WITH_NOTES`，未发现 Phase 0 阻塞项。
2. `PASS_TO_PHASE1_REVIEW` 是 Phase 0 技术前置组合证据：0.6.7 覆盖 synthetic throughput / ResourceWatermark 预检，0.6.10 覆盖真实四源长跑，targeted rollover run 覆盖显式切换边界，0.6.11 覆盖 hot path / durable parquet path 隔离和延迟，0.6.12 覆盖 collector region 自动识别；不得把 Phase 0 probe 当成完整生产 collector、完整生产 ResourceWatermark、完整 data-pipeline、paper/live 交易逻辑、NOE/ledger 或 execution feedback ingestion。
3. 0.6.10 与 targeted rollover run 的 ResourceWatermark 仍有 CPU/disk free/journal/PSI 等指标以 unavailable reason 记录；0.6.11 sidecar 已补齐 CPU/disk/PSI 采样，0.6.12 已补齐 collector region 证据。正式 collector 阶段必须补齐 host clock sync status 和生产级 ResourceWatermark manifest，不得把 Phase 0 probe 的缩水运行水位当成生产完整水位。
4. Phase 0 已证明官方 SDK `subscribe_best_bid_ask` custom feature 业务路径和 `subscribe_orderbook` book 路径，但未捕获实际 outbound frame；正式 adapter 必须捕获 request evidence，或在 manifest 中写明 unavailable reason。Phase 0 也不等于已完整覆盖 `price_change`、`last_trade_price`、`tick_size_change`、`new_market`、`market_resolved` 的生产事件合同。
5. 后续实现必须继续 SDK-first，不能因为 Phase 0 通过就跳过 `btc-updown-5m` discovery、raw evidence、freshness、source gap 和 reconnect trace 合同。

## 6. Polymarket Rust SDK 能力核对

核对产物：`data-collect/docs/polymarket-sdk-gap-report.md`

SDK gap verdict：`PASS`

核对结论：

1. 官方 Rust SDK `polymarket_client_sdk_v2` 覆盖 Phase 0 后续技术测试所需入口：market channel、`custom_feature_enabled`、`book`、`price_change`、`tick_size_change`、`last_trade_price`、`best_bid_ask`、`new_market`、`market_resolved`、Gamma/CLOB metadata、RTDS Chainlink `btc/usd`。
2. 高层 typed stream 不能单独满足 `raw_json`、source gap、subscription evidence 和 lag/reconnect trace 需求；后续 Polymarket adapter 必须使用官方 SDK 暴露的低层 WS 基础设施、request 类型和 typed response 类型构建采集证据 parser。
3. 该低层接法仍属于 SDK-first；本阶段没有授权 raw REST、manual signing、自建 SDK 或 shadow implementation。
4. 后续网络技术测试不得把 SDK reconnect、connection state 或 PONG 成功单独当作业务数据 fresh；必须继续验证 business freshness、source stale、source gap 和 recovery verdict。

## 7. Writer / Publish 技术测试

核对产物：

- `Cargo.toml`
- `Cargo.lock`
- `data-collect/Cargo.toml`
- `data-collect/src/lib.rs`
- `data-collect/src/writer.rs`
- `data-collect/src/manifest.rs`
- `data-collect/tests/writer_publish_contract.rs`
- `data-collect/benches/parquet_writer_probe.rs`

当前 writer/publish verdict：`PASS_FOR_PHASE0_PROBE`

验证命令均以本地仓库 `/home/chillchill/btc-updown-marketmaker` 为 workdir，并设置 `CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target`，避免在仓库内生成 `target/`。

### 7.1 TDD RED

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect writer_running_file_is_not_discoverable -- --nocapture
```

结果：失败于缺少 `ManifestDraft`、`RawEvent`、`discover_finalized_parquet`、`sha256_file` 和 `writer::ParquetSegmentWriter` 等 API，符合“功能未实现”的 RED 预期。

### 7.2 合同测试

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect writer_running_file_is_not_discoverable -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect writer_refuses_to_truncate_existing_running_segment -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect writer_finalize_atomic_rename_publishes_only_finalized -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect writer_segment_can_run_on_blocking_pool -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect manifest_requires_row_count_sha256_schema_source_coverage -- --nocapture
```

结果：五条测试均通过。

覆盖事实：

1. `*-running.parquet`、`*-digesting.parquet`、`*.tmp`、`.rsync-partial` 不进入 finalized discovery。
2. `ParquetSegmentWriter::create()` 不会 truncate 已存在的 `*-running.parquet`，旧 partial evidence 会保留为需恢复/标记 incomplete 的边界。
3. `ParquetSegmentWriter::finalize()` 在 ArrowWriter close 后先把 running 文件 rename 为不可发现的 `*-digesting.parquet`，sha256 成功后再 atomic rename 为 finalized `.parquet`。
4. finalized artifact 记录 row count、sha256 和 schema version；sha256 由 digesting 文件计算后随 finalized artifact 返回。
5. `write_segment_on_blocking_pool()` 使用 Tokio blocking pool 承载 writer create/write/finalize，Phase 0 证明文件写入可隔离出 async 主事件循环；后续正式 writer service 仍需在 runtime 阶段接入 bounded queue 和 backpressure。
6. manifest completeness 判定要求 artifact path、row count、sha256、schema version、code version、collector binary sha256、source config digest、source coverage、完整 ResourceWatermark、writer queue high watermark、flush latency、collector host/region 和 `required_files[]`；缺 sha256、source coverage、完整 ResourceWatermark、required file role/path 时为 incomplete。

### 7.3 Writer Probe Bench

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo bench -p data-collect --bench parquet_writer_probe
```

结果：

```text
rows=10000 elapsed_ms=3 final_path=/tmp/data-collect-parquet-writer-probe-107364/phase0-writer-probe.parquet sha256=e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1
```

说明：该 bench 是本机 release probe 的单次输出，仅证明 writer path 可运行和产生 finalized artifact，不等同于后续 `2k-5k events/s` 四源合并吞吐验收。吞吐、queue、flush latency 分布和 ResourceWatermark 仍在后续 Phase 0 任务中验证。

## 8. Throughput / Backpressure 技术测试

核对产物：

- `data-collect/src/backpressure.rs`
- `data-collect/tests/backpressure_contract.rs`
- `data-collect/benches/throughput_probe.rs`

当前 throughput/backpressure verdict：`PASS_FOR_PHASE0_SYNTHETIC_PROBE`

### 8.1 TDD RED

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect bounded_queue_records_backpressure_observation_without_source_gap -- --nocapture
```

结果：失败于缺少 `data_collect::backpressure`，符合“backpressure 功能未实现”的 RED 预期。

### 8.2 Bounded Queue / Backpressure 合同测试

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect bounded_queue_records_backpressure_observation_without_source_gap -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect queue_watermarks_include_rows_bytes_age_writer_lag -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect bounded_queue_rejects_zero_capacity_without_panic -- --nocapture
```

结果：三条测试均通过。

覆盖事实：

1. `tokio::sync::mpsc::channel(capacity)` 用作 bounded event queue；`try_send` 在队列满时返回 full error，不静默丢事件。
2. 可恢复 queue full 和 bounded wait success 只记录 backpressure observation，verdict 为 `SoftDegraded`，运行动作是 `Continue`，不是 `ProcessFatal`；send timeout / drop 才记录 source gap。
3. Queue watermark 显式包含 rows、bytes、oldest event age 和 writer lag。
4. zero capacity queue config 返回可记录错误，不 panic，避免配置错误造成不可解释的进程级失败。
5. soft watermark 只降级并继续；连续 hard watermark 达到配置次数后才给出 `PauseIntake`，且仍不标记 `process_fatal`。

### 8.3 Throughput Probe

计划中的 bench 命令已收窄为只运行目标 bench，避免 `cargo bench` 同时执行其他 bench target：

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo bench -p data-collect --bench throughput_probe -- --sample-size 20
```

结果：

```text
case=polymarket_p95_window rows=302000 elapsed_ms=157 events_per_sec_total=1913010.85 events_per_sec_p50=5000000.00 events_per_sec_p95=5000000.00 events_per_sec_max=5000000.00 flush_latency_ms_p50=0 flush_latency_ms_p95=1 flush_latency_ms_max=2 finalize_latency_ms=24 finalize_failures=0 writer_queue_rows_high=5000 writer_queue_bytes_high=1866250 queue_age_ms_high=2 writer_lag_ms_high=2 rss_bytes=38207488 resource_unavailable=cpu_percent,disk_free_bytes,artifact_dir_bytes,journal_log_bytes,journal_limit_status,open_files,parquet_writer_memory_estimate_bytes,cgroup_memory_current_bytes,cgroup_memory_stat,cgroup_memory_events,psi_cpu,psi_memory,psi_io,ttl_ms final_path=/tmp/data-collect-throughput-probe-107416-polymarket_p95_window/polymarket_p95_window-typed-raw.parquet sha256=ac699795a832515e49c6082ff48ebc255dd9650ec770bd043c37c910f6ff289d
case=polymarket_max_window rows=433000 elapsed_ms=208 events_per_sec_total=2074725.70 events_per_sec_p50=5000000.00 events_per_sec_p95=5000000.00 events_per_sec_max=5000000.00 flush_latency_ms_p50=0 flush_latency_ms_p95=1 flush_latency_ms_max=2 finalize_latency_ms=35 finalize_failures=0 writer_queue_rows_high=5000 writer_queue_bytes_high=1866250 queue_age_ms_high=2 writer_lag_ms_high=2 rss_bytes=56500224 resource_unavailable=cpu_percent,disk_free_bytes,artifact_dir_bytes,journal_log_bytes,journal_limit_status,open_files,parquet_writer_memory_estimate_bytes,cgroup_memory_current_bytes,cgroup_memory_stat,cgroup_memory_events,psi_cpu,psi_memory,psi_io,ttl_ms final_path=/tmp/data-collect-throughput-probe-107416-polymarket_max_window/polymarket_max_window-typed-raw.parquet sha256=110e41d52b05a6fa8cea81c04146ec172c4d888ead13815f4196476d3d915b28
```

说明：这是本地合成四源事件 + Tokio bounded queue `try_send`/`try_recv` + parquet writer 的 release probe，证明本地 writer/queue 技术路径高于 `2k-5k events/s` 工程基线，并记录 events/s 分位数、flush latency 分位数、finalize latency/failure、RSS 和未测量 ResourceWatermark 字段。它不替代真实 Polymarket/Binance/RTDS 网络源、数据采集服务器 hardware tier、完整运行时 ResourceWatermark 或长时间 soak 验证。

## 9. Network Recovery / Freshness 技术测试

核对产物：

- `data-collect/src/health.rs`
- `data-collect/tests/network_recovery_contract.rs`

当前 network recovery verdict：`PASS_FOR_PHASE0_CONTRACT`

### 9.1 TDD RED

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect retry_uses_bounded_backoff_jitter_and_records_trace -- --nocapture
```

结果：失败于缺少 `data_collect::health`，符合“network recovery / freshness 功能未实现”的 RED 预期。

### 9.2 异常覆盖

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --test network_recovery_contract -- --nocapture
```

结果：5 条测试全部通过，包括：

1. `network_failures_map_to_health_gap_or_reconnect_trace`
2. `retry_uses_bounded_backoff_jitter_and_records_trace`
3. `source_circuit_breakers_are_independent`
4. `pong_without_business_data_marks_stale_not_healthy`
5. `reconnect_requires_subscription_snapshot_and_fresh_business_data`

异常映射覆盖连接失败、断链、server shutdown、订阅/HTTP/WS 错误、空 payload、空 array、空 snapshot、长时间无业务消息、PONG 但业务静默、rate limit、连接数/订阅数限制、snapshot 缺失/过旧、update id gap、source stale、source gap 和 coverage drop；每类都进入 source health、source gap 或 reconnect trace。

### 9.3 恢复策略指定测试

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect retry_uses_bounded_backoff_jitter_and_records_trace -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect source_circuit_breakers_are_independent -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect pong_without_business_data_marks_stale_not_healthy -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect reconnect_requires_subscription_snapshot_and_fresh_business_data -- --nocapture
```

结果：四条指定测试均通过。

覆盖事实：

1. retry trace 记录 source、failure、attempt、sleep、cumulative elapsed、jitter、retry exhausted；backoff 有 max attempts、max elapsed 和 max backoff ceiling，jitter 按 source/attempt/seed 分散，避免同 policy 源同步重试。
2. 每个 source 的 circuit state 独立；状态覆盖 `Degraded`、`Open`、`HalfOpen`、`Recovered`、`Exhausted`，单源状态不会改变其他 source。
3. PONG 存在但没有业务消息时，在 freshness timeout 内保持 `Reconnecting`；超过 timeout 后 verdict 为 `Stale`，reason 为 `pong_without_business_data`，不会标记 healthy。
4. 重连后必须重新满足 subscription ack、snapshot 和 fresh business message；只有连接或 snapshot 不会恢复 healthy。

## 10. Hardware Tier / ResourceWatermark 技术测试

核对产物：

- `data-collect/docs/hardware-tier-validation.md`
- `data-collect/src/resource.rs`
- `data-collect/tests/resource_watermark_contract.rs`

当前 hardware/resource verdict：`PASS_FOR_PHASE0_CONTRACT_WITH_REMOTE_DISK_LIMIT`

### 10.1 数据采集服务器 hardware tier

执行命令：

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 ubuntu@3.255.156.183 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
```

结果摘要：

- CPU：`2` vCPU。
- Memory：`1.9Gi total`，约 `1.4Gi available`，无 swap。
- Root disk：`58G total`，`52G used`，`5.8G available`，`90% used`。
- `ulimit -n`：`1024`。
- PSI：cpu/memory/io 可读。

影响：数据采集服务器硬件 tier 与 live 小主机同级，且根盘剩余空间低。后续远端验证前需要按用户授权清理旧采集程序/旧采集数据或明确测试 artifact 根；本次只读核对未终止进程、未删除数据。

### 10.2 live-trade 服务器运行边界

执行命令：

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 ubuntu@108.130.74.143 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
```

结果摘要：

- CPU：`2` vCPU。
- Memory：`1.9Gi total`，约 `1.4Gi available`，无 swap。
- Root disk：`58G total`，`11G used`，`47G available`，`19% used`。
- `ulimit -n`：`1024`。
- PSI：cpu/memory/io 可读。

影响：live-trade 服务器确认仍是小主机运行边界，不是构建机；本次未在该服务器运行任何 Rust 编译、cargo 测试、训练式重处理、历史全量 parquet 扫描或归档压缩。

### 10.3 ResourceWatermark 合同测试

```bash
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect resource_watermark_records_required_metrics_or_unavailable_reason -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect stateless_hard_threshold_records_observation_without_stop_action -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect resource_hard_threshold_requires_consecutive_samples_before_stop_action -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect soft_threshold_records_degraded_without_process_fatal -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect resource_policy_evaluates_non_disk_watermarks -- --nocapture
```

结果：五条测试均通过。

覆盖事实：

1. ResourceWatermark schema 覆盖 RSS、CPU、disk free、artifact dir bytes、journal/log bytes、journal 限额状态、open files、queue bytes、queue age、parquet writer memory estimate、cgroup memory current/stat/events、PSI、anon/file/slab、stage latency、TTL 和 unavailable reason。
2. 指标缺失时必须写 `MetricUnavailable`，不能静默丢失。
3. soft/hard threshold 除 disk、queue、stage latency 外，也评估 RSS、CPU、artifact dir bytes、journal/log bytes、open files、queue age、parquet writer memory estimate、cgroup current/stat/events、PSI 和 TTL。
4. soft threshold 只记录 degraded/backpressure/recovery observation，不标记 `process_fatal`。
5. hard threshold 记录 `StopNewIntake`、`FinalizeOpenSegments`、`WriteIncompleteManifest` 等保全动作，仍不直接等同于进程级 fatal；只有后续无法 finalize/manifest 或路径越界等条件才会进入 `ProcessFatal`。

## 11. Phase 0 Summary Verdict

Phase 0 本地 probe 当前结论：`LOCAL_PROBE_PASS_FOR_SYNTHETIC_CONTRACTS`

Phase 0 远端执行当前结论：`PASS_TO_PHASE1_REVIEW`；旧 generic WebSocket smoke 形成的 `PASS_TO_PHASE1_REVIEW` 曾被撤销，0.6.9/0.6.10 真实源证据和 targeted rollover 补证曾重新打开 durable mirror / 四源 coverage 维度，0.6.11 已补齐 hot path / parquet path 延迟证据，0.6.12 已补齐 collector region 自动识别证据。

Phase 0 整体当前状态：`PHASE0_COMPLETE_READY_FOR_PHASE1`；0.7.3 用户审查 gate 与阶段收尾 commit/push 均已完成。

技术 probe 状态：

| 验证域 | Verdict |
| --- | --- |
| Polymarket Rust SDK 能力 | `PASS` |
| writer / publish / atomic rename / manifest completeness | `PASS_FOR_PHASE0_PROBE` |
| synthetic throughput / bounded queue / backpressure | `PASS_FOR_PHASE0_SYNTHETIC_PROBE` |
| network recovery / freshness contract | `PASS_FOR_PHASE0_CONTRACT` |
| hardware tier / ResourceWatermark contract | `PASS_FOR_PHASE0_CONTRACT_WITH_REMOTE_DISK_LIMIT` |
| Rust lint / clippy | `PASS` after installing `cargo-clippy`; `cargo clippy -p data-collect --all-targets -- -D warnings` passed |
| 数据采集服务器清理和临时测试根 | `PASS`; 清理后根盘 `/dev/root` 为 `58G total / 3.1G used / 55G available / 6% used` |
| 远端 writer smoke | `PASS`; 16 轮，每轮 `rows=10000`，sha256 `e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1` |
| 远端 45 分钟 synthetic throughput / ResourceWatermark | `PASS`; 415 轮，`finalize_failures=0`，无 PSI 压力，无 tmp 残留 |
| 远端 HTTP reachability smoke | `PASS`; Binance depth/aggTrade、Polymarket Gamma/CLOB 均 `http_code=200` 且 `empty_response=false` |
| 远端 SDK-first WebSocket reachability smoke | `PASS_BUT_INSUFFICIENT`; generic market channel 与 RTDS 均 Connected、subscription_count=1，且收到业务消息，但没有证明 `btc-updown-5m` active market discovery、rollover 或 custom feature 实际订阅证据 |
| 真实 `btc-updown-5m` discovery / rollover / 四源时间线对齐长跑 | `PASS_FOR_PHASE0_REMOTE_PROBE`; 0.6.9 cold-start discovery `PASS`，0.6.10 60 分钟真实四源长跑 `PASS` |
| hot path / durable parquet path 延迟补证 | `PASS`; 0.6.11 60 分钟真实四源补证 `probe_exit=0`，hot p95 `1ms`，durable write-done p95 `1004ms`，本地 `write_events_us` p95 `382us` |
| collector region 自动识别补证 | `PASS`; 0.6.12 短 smoke 在未设置 `COLLECTOR_REGION` 时通过 AWS IMDS 写出 `collector_region=eu-west-1` |

Phase 1 进入前约束：

1. 远端技术 gate 已达到 `PASS_TO_PHASE1_REVIEW`，计划 0.7.3 已按用户要求由 4 个独立子代理审查通过，阶段收尾 commit/push 已完成。
2. Phase 0 probe 已验证真实 discovery、rollover、四源 coverage、hot path / durable path 隔离、finalized parquet、manifest 和 sha256；正式 collector 阶段仍需补齐生产级 source adapter、网络恢复、ResourceWatermark 采样和持续发布机制。
3. 0.6.12 已修复 collector region 自动识别；ResourceWatermark JSON 仍不是生产完整采样，不得在后续阶段把该 probe 当成生产完整监控实现。

Phase 1 执行提醒：

1. 后续从阶段 1 的计划任务开始执行。
2. paper/live 服务器的 `~/btc-updown-marketmaker/datasets/` 只在后续 handoff 阶段验证，不在 Phase 0 冒充运行期 append-only log。

## 12. Phase 0 Remote Validation Execution

### 12.1 Cleanup Receipt

执行窗口：`2026-07-05T22:48:03Z` 到 `2026-07-05T22:48:23Z`，目标服务器 `ubuntu@3.255.156.183`。

清理前证据：

- receipt：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/receipts/cleanup-pre-20260705T224803Z.txt`
- 旧采集进程：PID `124397`，运行 `80-13:59:11`，命令为 `./poly-tx_v1.1.5 ws-series --series-prefix btc-updown-5m ...`
- 旧程序与数据目录：`/home/ubuntu/ron/collect-orderbook`，`du -sh` 为 `50G`
- 清理前根盘：`/dev/root 58G total / 53G used / 4.4G available / 93% used`

清理动作与结果：

- receipt：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/receipts/cleanup-action-20260705T224823Z.txt`
- 仅终止确认属于旧采集程序的 PID `124397`；TERM 后 `post_alive=no`
- 仅删除已记录的旧采集目录 `/home/ubuntu/ron/collect-orderbook`
- 未触碰 `~/wallets` 或 `/home/ubuntu/live-trade`
- 清理后根盘：`/dev/root 58G total / 3.1G used / 55G available / 6% used`

### 12.2 Local Build And Remote Upload

本地构建 release bench binary 后上传到数据采集服务器，远端未运行 cargo 编译命令。

| binary | sha256 | 远端路径 |
| --- | --- | --- |
| `parquet_writer_probe` | `629d2d221819206db25251e26e22e50d8c442311b272efd148b5068365e9687c` | `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/bin/parquet_writer_probe` |
| `throughput_probe` | `ab17ffd3b609db03d5ec5f061643fbcca89f1300e3f7382f0600665c8c49c105` | `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/bin/throughput_probe` |
| `websocket_smoke_probe` | `841fc12f5771675ea4624b414614a76c5b1fd1d8125a1423ccc64dbb2ce1c1fc` | `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/bin/websocket_smoke_probe` |

本地 sha256 与远端 sha256 一致；远端 `file` 显示上传的 probe 均为 x86-64 Linux executable。数据采集服务器未运行 cargo 编译命令。

### 12.3 Remote Smoke Test

执行窗口：`2026-07-05T22:49:50Z` 到 `2026-07-05T22:57:52Z`。

HTTP smoke：

| endpoint | http_code | size_download | time_total | empty_response |
| --- | --- | --- | --- | --- |
| Binance depth smoke | `200` | `366` | `0.245352` | `false` |
| Binance aggTrade smoke | `200` | `122` | `0.240192` | `false` |
| Polymarket Gamma smoke | `200` | `6687` | `0.041811` | `false` |
| Polymarket CLOB smoke | `200` | `1823307` | `0.196489` | `false` |

Writer smoke：

- `writer_log_count=16`
- 每轮 `rows=10000`
- 每轮 sha256 为 `e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1`
- 每轮 `elapsed_ms` 约 `13-14`
- 结束时根盘仍为 `6%` 使用；内存约 `1.9Gi total / 1.4Gi available`，无 swap

### 12.4 Remote 45 Minute Throughput And ResourceWatermark Acceptance

run dir：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-remote-20260705T225836Z`

执行窗口：`2026-07-05T22:58:36Z` 到 `2026-07-05T23:43:42Z`，总计 `415` 轮 probe，`du_run_dir=1.9M`。

| case | count | eps_min | eps_avg | eps_max | finalize_failures | rss_max | flush_p95_max | finalize_latency_max | queue_rows_max | queue_bytes_max | queue_age_max | writer_lag_max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `polymarket_p95_window` | `415` | `200941.84` | `484201.34` | `502871.99` | `0` | `15663104` | `26` | `808` | `5000` | `1866250` | `74` | `74` |
| `polymarket_max_window` | `415` | `272352.19` | `504273.28` | `521463.79` | `0` | `56786944` | `16` | `643` | `5000` | `1866250` | `70` | `70` |

资源结论：

- 吞吐全程显著高于 `2k-5k events/s` 工程基线。
- `finalize_failures=0`，未发现 finalized partial artifact。
- CPU/memory/io PSI `avg10=0.00 avg60=0.00 avg300=0.00`。
- sampler 尾部内存约 `1.3Gi available`，无 swap。
- `tmp` 文件数为 `0`。
- 根盘结束仍为 `/dev/root 58G total / 3.1G used / 55G available / 6% used`。

2-4 小时加强 soak 未触发：45 分钟验收没有出现 RSS 持续增长、磁盘下降不可解释、PSI 偏高、writer latency 持续恶化、hard threshold 接近或 source/network smoke 不稳定。

### 12.5 Remote Network Reachability Smoke

run dir：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-network-20260705T234452Z`

HTTP reachability：

| endpoint | http_code | size_download | time_total | remote_ip | curl_exit | empty_response |
| --- | --- | --- | --- | --- | --- | --- |
| Binance aggTrade | `200` | `123` | `0.638602` | `54.230.153.225` | `0` | `false` |
| Binance depth | `200` | `366` | `0.241885` | `54.230.153.225` | `0` | `false` |
| Polymarket CLOB | `200` | `1823307` | `0.076771` | `2606:4700:440d::6812:22cd` | `0` | `false` |
| Polymarket Gamma | `200` | `6694` | `0.042197` | `2606:4700:4408::ac40:9933` | `0` | `false` |

所有 HTTP `.err` 文件大小为 `0`。

WebSocket reachability：

- 远端未安装 `websocat` 或 `wscat`。
- 已记录 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-network-20260705T234452Z/logs/websocket-tool-unavailable.txt`，内容为 `TOOL_UNAVAILABLE websocket_client`。
- 该命令行工具缺口已由后续官方 Polymarket Rust SDK-first smoke probe 覆盖；不得把该历史 `TOOL_UNAVAILABLE` 继续当作当前远端 verdict 限制。

SDK-first WebSocket smoke：

- run dir：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-sdk-ws-smoke-20260706T000336Z`
- probe binary：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/bin/websocket_smoke_probe`
- sha256：`841fc12f5771675ea4624b414614a76c5b1fd1d8125a1423ccc64dbb2ce1c1fc`
- 执行时间：`2026-07-06T00:03:36Z` 到 `2026-07-06T00:03:38Z`
- `probe_exit=0`
- SDK：`polymarket_client_sdk_v2` version `0.6.0`，feature 包含 `clob`、`gamma`、`rtds`、`ws`；其 `ws`/`rtds` feature 通过 crate 依赖使用 `tokio-tungstenite`，不是安装 `websocat`/`wscat`。

SDK-first WebSocket smoke 输出摘要：

| source | connection_state | subscription_count | business_message | evidence |
| --- | --- | --- | --- | --- |
| Polymarket market channel | `Connected` | `1` | `PASS` | `event_type=book`，`bids=48`，`asks=41`，`source_ts=1783296215780` |
| Polymarket RTDS Chainlink BTC/USD | `Connected` | `1` | `PASS` | `topic=crypto_prices_chainlink`，`symbol=btc/usd`，`source_ts=1783296217000` |

market subscription evidence limitation：

```json
{"type":"market","operation":"subscribe","markets":[],"assets_ids":["105267568073659068217311993901927962476298440625043565106676088842803600775810"],"initial_dump":true,"custom_feature_enabled":true}
```

该 JSON 是 probe 本地构造并打印的 `SubscriptionRequest`，不是抓取到的 SDK 实际发送帧；当前 `websocket_smoke_probe` 实际调用 `subscribe_orderbook(...)`，因此不能把该 JSON 当成 `custom_feature_enabled=true` 已真实发送的证据。

RTDS subscription evidence：

```json
{"action":"subscribe","subscriptions":[{"topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}]}
```

说明：本次 market channel smoke 为 SDK-first reachability 证明，使用 Gamma metadata 获取一个当前 open market 的 CLOB token id 后订阅 market channel；其 discovery 逻辑只优先匹配任意 `btc` / `bitcoin` slug 或 question，并在未命中时 fallback 到任意 open CLOB market。它不等同于 `btc-updown-5m` current active market discovery、next market prefetched、rollover 或 source adapter 完成。

### 12.6 Remote Post-Run State

收尾只读核对结果：

- 没有残留 `poly-tx`、`data-collect`、`collector`、`throughput_probe` 或 `parquet_writer_probe` 进程。
- 远端 Phase 0 run root 大小：`21M`。
- 根盘：`/dev/root 58G total / 3.1G used / 55G available / 6% used`。
- Phase 0 临时根和关键子目录 owner/group 均为 `ubuntu:ubuntu`，mode 均为 `775`。

### 12.7 Remote Verdict Superseded

原远端 verdict：`PASS_TO_PHASE1_REVIEW`，已撤销。

撤销后临时远端 verdict：`BLOCKED_NEEDS_DECISION`。

撤销理由：

1. 清理 receipt、binary 上传、writer smoke、45 分钟 synthetic throughput / ResourceWatermark、HTTP reachability 仍是有效证据，但只证明容量预检和 endpoint reachability。
2. 官方 Polymarket Rust SDK-first WebSocket smoke 使用 generic open market token，不证明 `btc-updown-5m` active market discovery、next market prefetched 或 rollover。
3. Binance 和 RTDS 没有以真实长跑方式接入同一事件管线，也没有证明与 Polymarket market window 的 coverage 对齐。
4. 阶段 0 必须补做真实 `btc-updown-5m` discovery / rollover / 四源时间线对齐长跑，重新汇总远端 verdict。

### 12.8 Remote Real Source Discovery And Alignment

0.6.9 cold-start discovery：

- run dir：`/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-market-discovery-20260706T011058Z`
- receipt：`artifacts/market_discovery_receipt.json`
- binary sha256：`3240d55af3000564805f39548008820c41e9847d385a0ba4942a4680794ba5d6`
- verdict：`PASS`
- current：`btc-updown-5m-1783300200`，condition `0x12cc794b04ca1175bdb82f891296f0cf1520eac4f7ffcfad3c1e747b1f3b51ab`
- next：`btc-updown-5m-1783300500`，condition `0x010c092bd2c411af2434f8b4a0c101e9fbcdc33c9d7080c885769d6d28e5af10`
- source_gaps：`[]`

0.6.10 第一轮真实四源长跑：

- run dir：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T011412Z`
- binary sha256：`dda322a9b62dfc90330c503acd360ba9ced11711e17951f50e69cd0c635b20a6`
- row_count：`180161`
- finalized parquet sha256：`0e9e3c43bc2804b057541ba9865604c34d7200c2fbeb0fc06d582a7ee223af5b`
- verdict：`SOURCE_GAP`
- 失败原因：`binance_aggtrade_business_message_timeout` 5 次；该轮仍证明四源接入、180k 行真实吞吐和 14 个 Polymarket 5 分钟窗口，但 5 秒 aggTrade freshness timeout 被判定为过度校验，不作为通过依据。

修复：

- 将 Binance WS freshness 从固定 `5s` 改为按源配置：depth `30s`、aggTrade `60s`；RTDS freshness `60s`。这是需求语义上的修复：记录“长时间无业务消息”，而不是把短暂低频交易间隔当 source gap。
- 新版 `real_source_alignment_probe` binary sha256：`9a233dbb3fda705a3954b013b262543ad220791f31e08dd2343d8c352d2d7cb9`。
- 30 秒短诊断 `phase0-real-source-short2-20260706T021557Z` 通过：`row_count=1618`，source_gaps `[]`。

0.6.10 第二轮真实四源 60 分钟长跑：

- run dir：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T021650Z`
- artifact root：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T021650Z/artifacts`
- binary sha256：`9a233dbb3fda705a3954b013b262543ad220791f31e08dd2343d8c352d2d7cb9`
- verdict：`PASS`
- probe_exit：`0`
- row_count：`177573`
- finalized parquet：`artifacts/typed_raw/real-source-alignment-typed-raw.parquet`
- finalized parquet sha256：`f561a27b39191d1636a12f61386a7f02089ab4e8bf3f408582f9bab1707ec775`
- manifest sha256：`45440a6595584546633bffb617c4cf05475980df11c8254fffaa0b5f42f9a60d`
- source_gaps sha256：`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`
- artifact dir bytes：`81570811`；`du -sh` 为 `78M`
- 数据服务器根盘结束：`/dev/root 58G total / 3.3G used / 54G available / 6% used`

Source coverage：

| source_kind | rows | first_recv_time_utc_ms | last_recv_time_utc_ms | market_windows |
| --- | ---: | ---: | ---: | --- |
| `polymarket_market` | `115074` | `1783304210363` | `1783307810336` | `14` 个：`btc-updown-5m-1783304100` 到 `btc-updown-5m-1783308000` |
| `binance_depth` | `35991` | `1783304210455` | `1783307810317` | n/a |
| `binance_aggtrade` | `23211` | `1783304211369` | `1783307810948` | n/a |
| `polymarket_rtds_chainlink_btc_usd` | `3297` | `1783304213000` | `1783307811129` | n/a |

ResourceWatermark receipt：

- `rss_bytes=77373440`
- `queue_rows_high=94`
- `queue_bytes_high=3568670`
- `received_rows=177573`
- CPU、disk free、journal/log、PSI 在 Phase 0 probe 中以 unavailable reason 记录；正式 collector 阶段必须补齐真实采样。

审查后 targeted rollover 补证：

- 子代理审查指出第二轮 60 分钟长跑只有 `14` 个 window 聚合证据，缺显式 `previous.next == new.current` rollover edge；该反馈有效。
- 首次补证 run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-rollover-edge-20260706T033425Z` 使用过严 exact start 校验，失败于 `WindowBoundsMismatch`：Gamma exact slug 返回的 `candidate_start_ts_s` 早于 slug 目标窗口。修复后策略为：slug/end 必须匹配，`candidate_start_ts_s <= logical start` 可接受但必须保留原始 candidate start/end。
- 修复后 targeted run：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-rollover-edge-20260706T033741Z`
- binary sha256：`f49fdb369a8f4f39c83921a85afb562314138c495d0af18e4a0737c9d92eec52`
- verdict：`PASS`
- probe_exit：`0`
- row_count：`39570`
- finalized parquet sha256：`21bc3066abcb9d7b301b1a3125a7c2a86cf8e9cfb9a3ce9e929e65214da19405`
- manifest sha256：`6b9480164db171810eb37109984653cb829f09aaa32de64e0104b651ef653dd8`
- rollover_edges sha256：`7d405c5f4336b1f5a15cb3479801ca94796a5bb1d83be74c170bbf73c4d31a89`
- source_gaps：`[]`，sha256 `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`
- rollover_edges：`3` 条，均满足 `prefetched_promoted_to_current=true`、`previous_current_retired_by_time=true`、`windows_contiguous=true`。边界分别为 `1783308900 -> 1783309200`、`1783309200 -> 1783309500`、`1783309500 -> 1783309800`。
- targeted source coverage：Polymarket market `24763` 行，覆盖 `5` 个 window（`btc-updown-5m-1783308900` 到 `btc-updown-5m-1783310100`），事件类型 `best_bid_ask=13287`、`book=11472`、`market_discovery_switch=4`；Binance depth `8991` 行；Binance aggTrade `4935` 行；RTDS Chainlink `881` 行。
- ResourceWatermark：`rss_bytes=33030144`、`queue_rows_high=61`、`queue_bytes_high=3224409`、`artifact_dir_bytes=17981890`；CPU/disk free/journal/PSI 仍以 unavailable reason 记录。
- initial discovery 保留平台 metadata 差异：current logical start `1783308900`，`candidate_start_ts_s=1783222978`；next logical start `1783309200`，`candidate_start_ts_s=1783223253`；两者 candidate end 均与 logical end 匹配。
- subscription evidence scope：Phase 0 记录 SDK API 事实，`Client::subscribe_best_bid_ask` 在 SDK `0.6.0` 中调用 `subscribe_market_with_options(asset_ids, true)`；该 probe 未捕获 actual outbound frame，正式 adapter 必须补齐 request evidence 或写明 unavailable reason。
- clock sync scope：Phase 0 记录 collector host/region 与 `recv_time_utc` 证据，但未把主机 clock sync 状态作为已完成项；正式 runtime receipt 必须补齐 clock sync status 或 unavailable reason。

远端 Phase 0 技术 verdict（0.6.10 旧结论）：

- 0.6.10 旧远端 verdict：`PASS_TO_PHASE1_REVIEW`
- 当前 overall status：已由 12.11 用户审查 gate 和阶段收尾更新为 `PHASE0_COMPLETE_READY_FOR_PHASE1`。
- 撤销/重新打开原因：0.6.10 未证明 paper/live 实时 hot path、`source_recv_to_hot_delivery_ms`、`source_recv_to_write_done_ms`、queue high 持续时间和 parquet writer 与 hot path 隔离。
- 0.6.11 远端真实四源 60 分钟 hot path / parquet path 延迟补证已完成，并已重新执行 0.7.2；0.7.3 用户审查 gate 和阶段收尾 commit/push 已由 12.11 完成。

### 12.9 Local Hot Path / Parquet Path Latency Implementation

0.6.11 本地实现与验证：

- 新增 `data-collect/src/latency.rs`，包含 `TimedRawEvent`、collector/fanout `event_seq` 分配、hot path receipt、durable write batch receipt、flush policy、queue duration tracker 和 latency stats。
- 修改 `data-collect/src/writer.rs`，在现有 `ParquetSegmentWriter` 上新增 `finalize_with_timing()`，记录 close、running->digesting rename、sha256、digesting->final rename、atomic rename 和 total finalize timing；未复制 shadow parquet writer。
- 新增 `data-collect/tests/real_source_latency_contract.rs`，覆盖 hot/durable `event_seq` 对齐、hot receipt 不依赖 parquet、durable batch 写入延迟映射、1000ms time flush、queue high 持续时间、writer finalize timing。
- 修改 `data-collect/benches/real_source_alignment_probe.rs`：source task 发送 `TimedRawEvent`，collector 分配全局 `event_seq`；同一事件先送 fake realtime bounded handoff，再进入 durable parquet batching path；writer worker 返回 `write_batch_receipts` 和 instrumented finalize timing；probe 写出 `latency_summary.json`、`hot_path_receipts.jsonl`、`write_batch_receipts.jsonl`、`queue_samples.jsonl`。
- 时间单位口径：网络/业务路径延迟、batch wait、queue wait 继续使用毫秒字段；本地 `write_events`、finalize close、sha256、atomic rename 和 total finalize 使用微秒字段（`*_us`），避免把低于 1ms 的本地写入耗时舍入为 `0ms` 或 `1ms`。

本地验证命令均通过：

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings
CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo build -p data-collect --release --bench real_source_alignment_probe
git diff --check
```

测试结果：

- data-collect tests：`26` 条通过。
- `real_source_latency_contract`：`5` 条通过。
- release bench build：`real_source_alignment_probe` 通过。

本节只证明本地代码与合同测试通过；远端 60 分钟真实四源 hot path / parquet path 延迟补证仍待执行，不能据此进入阶段 1。

0.6.11 远端执行状态更新：

- 180 秒短 smoke run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-smoke-20260706T090412Z` 曾通过并生成 hot/durable receipt，但该 binary 使用旧的本地写入毫秒字段；其网络/业务路径毫秒指标可作为 smoke 证据，本地写入耗时字段已被新 `*_us` 口径取代。
- 60 分钟正式 run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-20260706T090834Z` 已按用户要求暂停；probe exit 为 `143`，只保留为中断记录，不作为阶段 0 验收证据。
- 单位修正后的 120 秒短 smoke run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-units-smoke-20260706T091716Z` 通过，binary sha256 `4e066002d33c0ec8d285b9d5e86549611052a669d3c2354d4103195e7c2a5aff`，`probe_exit=0`，manifest verdict `PASS`，row_count `5778`，source_gaps `[]`。
- 单位 smoke source coverage：Polymarket market `4037` 行并覆盖 `2` 个 window；Binance depth `1189` 行；Binance aggTrade `432` 行；RTDS Chainlink `120` 行。
- 单位 smoke 字段核对：`latency_summary.json` 不再包含 `write_events_ms`、`finalize_total_ms`、`finalize_close_ms`、`sha256_ms`、`atomic_rename_ms`；`write_batch_receipts.jsonl` 不再包含 `write_events_ms`。
- 单位 smoke 本地写入/发布耗时：`write_events_us` count `113`，p50 `119us`，p95 `626us`，p99 `1139us`，max `2318us`；`finalize_total_us=15264`，`finalize_close_us=2665`，`sha256_us=12526`，`atomic_rename_us=69`。
- 单位 smoke 路径/等待延迟仍为毫秒：`source_recv_to_hot_delivery_ms` p50/p95/p99/max 为 `0/1/1/263ms`；`source_recv_to_write_done_ms` 为 `569/1007/1061/1098ms`；`batch_wait_ms` 为 `1017/1097/1098/1098ms`；`writer_channel_wait_ms` 为 `0/1/1/1ms`。

0.6.11 远端 60 分钟正式补证结果：

- run dir：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-20260706T092424Z`
- binary sha256：`4e066002d33c0ec8d285b9d5e86549611052a669d3c2354d4103195e7c2a5aff`
- `probe_exit=0`，manifest verdict `PASS`，row_count `181091`，schema `parquet_raw_layout_v1`，source_gaps `[]`
- finalized parquet：`artifacts/typed_raw/real-source-alignment-typed-raw.parquet`，大小 `75003773` bytes，sha256 `e78c6712eb23a2f02bc0425b319ca027ffefb25c2249d6562b0ed51068f37e21`
- source coverage：Polymarket market `117578` 行，覆盖 `14` 个 `btc-updown-5m` market window，事件类型 `best_bid_ask=63768`、`book=53797`、`market_discovery_switch=13`；Binance depth `35989` 行，事件类型 `depthUpdate=35988`、`snapshot=1`；Binance aggTrade `24003` 行；RTDS Chainlink `3521` 行。
- UTC 5 分钟 recv-time bucket 对齐：按 `recv_time_utc_ms` 归入 `13` 个 UTC bucket，四源均有事件，missing source bucket 数量为 `0`；首尾 bucket 为部分窗口。
- rollover：`rollover_edges.json` 写出 `12` 条 edge，均满足 `prefetched_promoted_to_current=true`、`previous_current_retired_by_time=true`、`windows_contiguous=true`。
- hot/durable 对齐：`hot_path_receipts.jsonl` 为 `181091` 行，`event_seq` 从 `1` 到 `181091` 连续；`write_batch_receipts.jsonl` 为 `3354` 个 batch，覆盖同一连续 `event_seq`，flush reason 为 `time=3353`、`shutdown=1`；hot receipt 行数、write rows 和 manifest row_count 三者一致。
- finalized-only：`typed_raw` 目录只剩 `real-source-alignment-typed-raw.parquet`，无 running/digesting/tmp parquet 残留；`logs/file-sha256.tsv` 已记录 parquet sha256。
- latency summary：`source_recv_to_hot_delivery_ms` p50/p95/p99/max 为 `0/1/1/191ms`；`source_recv_to_write_done_ms` 为 `533/1004/1061/1102ms`；`source_recv_to_collector_dequeue_ms` 为 `0/0/1/191ms`；`collector_dequeue_to_hot_send_ms` 为 `0/0/0/3ms`；`hot_queue_wait_ms` 为 `0/0/1/63ms`；`batch_wait_ms` 为 `1011/1090/1099/1101ms`；`writer_channel_wait_ms` 为 `0/0/1/6ms`。
- local write/finalize timing：`write_events_us` count `3354`，p50 `112us`，p95 `382us`，p99 `1122us`，max `26476us`；`finalize_total_us=480767`、`finalize_close_us=63033`、`sha256_us=417658`、`atomic_rename_us=75`、`running_to_digesting_rename_us=36`、`digesting_to_final_rename_us=39`。
- queue/resource：source event queue rows high `15`，超过 `10` rows 累计 `6102ms`，最长连续 `1031ms`；hot handoff queue rows high `1`；writer chunk queue rows high `472`，超过 `100` rows 累计 `177118ms`，最长连续超过 `100` rows `2035ms`，未超过 `1000` rows。`backpressure_wait_count=0`、`backpressure_timeout_count=0`、`dropped_rows=0`、`hot_path_drop_count=0`、`writer_backlog_monotonic_growth_observed=false`。
- sidecar ResourceWatermark：`logs/resource_samples.tsv` 共 `717` 条采样，时间范围 `2026-07-06T09:24:24Z` 到 `2026-07-06T10:24:34Z`；RSS 峰值 `142856192` bytes；CPU 首个启动采样峰值 `100%`，稳定段约 `6-7%`；open files 峰值 `24`；disk free 最低 `57630863360` bytes；CPU/IO/memory PSI full avg10 无非零采样。
- 单位字段核对：`latency_summary.json` 和 `write_batch_receipts.jsonl` 不包含旧 `write_events_ms`、`finalize_total_ms`、`finalize_close_ms`、`sha256_ms`、`atomic_rename_ms` 字段；包含 `write_events_us`、`finalize_total_us`、`finalize_close_us`、`sha256_us`、`atomic_rename_us`。

0.6.11 结论：

- hot path 与 durable parquet path 已被同一真实四源 60 分钟 run 验证为隔离：hot p95 为 `1ms`，不等待 parquet batch、write、sha256 或 finalize；durable write-done p95 约 `1004ms` 主要来自 `PHASE0_REAL_SOURCE_MAX_FLUSH_INTERVAL_MS=1000` 的 time flush 等待，本地写入 p95 为 `382us`。
- 当前 60 分钟 run 没有显示写入拥堵：writer chunk queue high `472` 行，但无 drop、无 backpressure wait/timeout、无 monotonic backlog，run 结束已 drain 并完成 finalized-only publish。
- 0.6.10 的 `queue_rows_high=94`、targeted rollover run 的 `queue_rows_high=61` 和 0.6.11 的 writer chunk queue high `472` 都是真实流量下的 bounded queue observation；45 分钟 synthetic throughput 的 `writer_queue_rows_high=5000` 是容量预检的合成压力输入，不是同一种真实运行拥堵指标。
- 旧限制：0.6.11 manifest 内 `collector_host_id=ip-172-26-10-0`，但 `collector_region=unknown`；该 collector region 缺口已由 12.10 的 0.6.12 smoke 修复并验证。ResourceWatermark JSON 内部仍把 CPU、disk free、journal/log、PSI 标为 Phase 0 probe 未测，虽然本次 sidecar 已在 `logs/resource_samples.tsv` 补齐 CPU/disk/PSI 等 Linux 指标；正式 collector runtime 仍必须补齐 clock sync status 和完整生产级 ResourceWatermark manifest。

远端 Phase 0 技术 verdict（0.7.2 重新汇总）：

- 当前 verdict：已由 0.6.12 补证更新为 `PASS_TO_PHASE1_REVIEW`，见 12.10。
- 可以进入用户审查的证据：0.6.9 cold-start discovery、0.6.10 真实四源 60 分钟长跑、targeted rollover 补证、0.6.11 hot/durable path 60 分钟补证和 0.6.12 collector region 自动识别补证均已完成。
- 后续硬要求：正式 collector 阶段必须补齐 host clock sync status、生产级 ResourceWatermark runtime manifest、actual Polymarket request evidence，以及完整生产 source health / reconnect trace。
- 0.7.3 用户审查 gate 已由 12.11 完成；阶段收尾 commit/push 已完成。

### 12.10 Collector Region Evidence Fix

用户指出 `collector_region=unknown` 不应留到阶段 1。阶段 0 已完成修复与补证。

代码修复：

- 新增 `data-collect/src/collector_identity.rs`：`COLLECTOR_REGION` env override 优先；env 缺失时读取 AWS IMDS placement region；两者都不可用时才返回 `unknown`。
- `market_discovery_probe` 与 `real_source_alignment_probe` 已统一调用 `resolve_collector_region()`。
- 新增 `data-collect/tests/collector_identity_contract.rs`。RED 阶段失败于缺少 `data_collect::collector_identity`；GREEN 后 `collector_region_prefers_explicit_env_value`、`collector_region_uses_imds_when_env_is_missing`、`collector_region_uses_imds_when_env_is_blank`、`collector_region_is_unknown_only_when_no_source_is_available` 4 条合同测试通过；`data-collect` 本地 `--lib --tests` 当前共 `30` 条测试通过。

远端短 smoke：

- run dir：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-region-smoke-20260706T104540Z`
- binary sha256：`8459e763ff12921fc478a34c79ced1872235035690213991da408ebb24554b60`
- 执行口径：显式 `env -u COLLECTOR_REGION`，验证自动 IMDS fallback。
- `probe_exit=0`，manifest verdict `PASS`，row_count `4430`，source_gaps `[]`。
- manifest：`collector_host_id=ip-172-26-10-0`，`collector_region=eu-west-1`，parquet sha256 `ecf7347aa3b47b3342e83a76008efe9583c4d02e3b45d115da69379a831dda5f`。
- initial discovery receipt：`collector_host_id=ip-172-26-10-0`，`collector_region=eu-west-1`。
- coverage：Polymarket market `2568` 行并覆盖 `2` 个 market window；Binance depth `1189` 行；Binance aggTrade `554` 行；RTDS Chainlink `119` 行。

结论：`collector_region=unknown` 已在阶段 0 修复并测试通过。0.6.11 的 60 分钟性能 run 不因该字段问题重跑；0.6.12 是针对 collector identity evidence 的真实源短 smoke 补证。远端 Phase 0 技术 verdict 更新为 `PASS_TO_PHASE1_REVIEW`；0.7.3 用户审查 gate 和阶段收尾结论见 12.11。

### 12.11 User Review Gate Delegated Subagent Review

用户要求“派生 subagents 替我完成用户审查 gate”。本节记录 0.7.3 gate 的独立审查结果。

审查代理与结论：

- 需求/计划/报告一致性审查：`PASS_WITH_NOTES`，gate recommendation `ACCEPT_WITH_NOTES`；确认 `PASS_TO_PHASE1_REVIEW` 由 demand 与 plan 支持，0.6.12 collector region 已一致更新，剩余 request evidence、clock sync、生产 ResourceWatermark 属于 Phase 1+。
- 代码/测试/SDK-first 证据审查：`PASS_WITH_NOTES`，gate recommendation `ACCEPT_WITH_NOTES`；确认 Polymarket 路径使用官方 Rust SDK，未发现 raw REST/manual signing/self-SDK 旁路；本地 `cargo fmt --all -- --check`、`cargo test -p data-collect --lib --tests -- --nocapture`、`cargo clippy -p data-collect --all-targets -- -D warnings`、release bench build 和 `git diff --check` 均通过。
- 远端性能/资源/验收证据审查：`PASS_WITH_NOTES`，gate recommendation `ACCEPT_WITH_NOTES`；只读核对 0.6.10、targeted rollover、0.6.11 和 0.6.12 artifact，确认 `probe_exit=0`、manifest `PASS`、source_gaps `[]`、event_seq 连续、hot/durable 对齐、PSI 无压力、0.6.12 写出 `collector_region=eu-west-1`。
- 范围纪律/过度设计/路径规则审查：`PASS_WITH_NOTES`，gate recommendation `ACCEPT_WITH_NOTES`；确认远端 `~/btc-updown-marketmaker` 路径口径可接受、live-trade 小主机未被当构建机、可恢复 backpressure/source gap 与 fatal 状态已分级、data-collect 未越界到 data-pipeline 或 paper/live 交易逻辑。

综合 verdict：`ACCEPT_WITH_NOTES`。

阶段收尾：已创建并推送 Phase 0 收尾提交。

Phase 0 当前状态：`PHASE0_COMPLETE_READY_FOR_PHASE1`。

剩余注意项不阻塞 Phase 0：正式 collector 阶段仍需补齐 host clock sync status、生产级 ResourceWatermark runtime manifest、actual Polymarket outbound request evidence 或明确 unavailable reason、完整 source health/reconnect trace，以及 paper/live 主机 runtime receive timestamp 证据。

### 12.12 Phase 6 Local Delivery Cross-check

Phase 6 新增本地交付核对，不改变 Phase 0 远端技术 verdict：

- `data-collect/tests/e2e_collect_contract.rs` 用本地 synthetic 四源写入验证 typed raw parquet、manifest sidecar、health/gap/reconnect evidence、finalized-only discovery、consumer boundary declarations，以及 current active / planned next preload coverage 对齐。
- `data-collect/docs/acceptance-matrix.md` 将 `demand/data-collect-demand.md` 第九节 24 个验收标准逐条映射到合同测试、Phase 0 远端 artifact 或 Phase 6 扫描。
- Phase 6 本地 e2e contract 不替代 0.6.10 / 0.6.11 的真实四源远端长跑；它用于防止交付阶段遗漏 manifest、finalized-only、消费者边界或验收矩阵。
