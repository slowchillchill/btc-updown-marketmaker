# data-collect changelog

事项编号：`data-collect-demand`。

> 对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

> 对应计划文档：`/home/chillchill/btc-updown-marketmaker/plan/data-collect-plan.md`

## 2026-07-06

### 初始模块计划

- 根据 `demand/data-collect-demand.md` 创建模块计划 `plan/data-collect-plan.md`。
- 计划范围限定为 `./data-collect/` 的外部市场源采集证据模块；不授权代码执行、部署、paper、live、真实下单、撤单或资金操作。
- 按用户补充要求，把“技术测试与性能验证”设为阶段 0 硬 gate：正式开发前必须先验证 Polymarket SDK 能力、四源采集、parquet writer、atomic rename、bounded queue/backpressure、ResourceWatermark，以及数据采集服务器和交易服务器的硬件/运行边界。
- 阶段 0 未通过或未经用户审查时，不进入正式模块开发；如果发现 SDK 缺口、性能不可达、live 小主机资源不匹配或需求边界冲突，必须形成事实、影响、可选项和推荐选项，再交由用户决定。
- 计划明确 AWS 爱尔兰 live-trade 小主机只作为运行目标边界核对对象，不承担 Rust 编译、cargo test/check/fmt、训练式重处理、历史全量 parquet 扫描、压缩归档批处理或 archive/upload/listing/gzip。
- 计划保留 `data-collect` 非范围：不生成 labels、features、tick_stream、训练切分、T-30 样本加工、paper/live append-only log、order evidence、NOE/ledger、execution feedback ingestion 或交易逻辑；Polymarket SDK 缺口方案必须先获用户同意。

### 审查反馈修复

- 根据 subagent 审查结果修复 `plan/data-collect-plan.md`：明确当前仓库只落文档，后续代码编写、本地构建、`cargo` 测试和 bench 的目标路径；该条后续被证明混淆了本地路径与服务器路径，见“路径口径再修复”。
- 在计划中补充阶段推进通用 gate：每个阶段完成后必须自审、更新 changelog、派生独立审查、修复并复审、commit/push 后才能进入下一阶段；阶段 0 还必须经用户审查通过。
- 在当时计划中补充 `data_collect_run_root`、`data_collect_artifact_root`、训练 UTC day 分区根、paper/live handoff artifact 根和权限边界 receipt 要求；最终生产根路径后来由用户决定为各服务器本机 `~/btc-updown-marketmaker/datasets/`。
- 补充四源 typed raw 数据格式合同，明确 Polymarket market channel、Polymarket market metadata / `new_market`、Binance depth update/snapshot、Binance aggTrade、Polymarket RTDS Chainlink BTC/USD 的必备 typed 字段、raw evidence 和消费者边界。
- 补充 data-pipeline、训练、paper、live 三类消费者的格式兼容性检查：`data-pipeline` 可消费 typed raw mirror，训练必须经 data-pipeline 生成 labels/features/tick_stream/split 后消费，paper/live 只能消费外部市场源证据和 coverage，不能用 `data-collect` 替代 runtime append-only log、NOE/ledger 或交易逻辑。
- 补充 poly-tx 问题修复计划项：辅助源必须独立于首个 Polymarket worker 启动并证明四源 T-30s preload coverage；discovery / asset_ids miss 过期后必须记录 miss、`discovery_miss_rate`、跳过过期 target 或按当前时间重算目标，不能卡住后续 market。
- 将需求覆盖索引从主题级改为 `demand/data-collect-demand.md` 第九节 1-22 的逐项映射，并补充数据格式与消费者兼容性检查表。

### 过度硬失败风险修复

- 根据用户对“过度校验导致采集程序频繁失败”的质疑，修复 `plan/data-collect-plan.md` 的运行时失败语义：新增 `ProcessFatal`、`ArtifactIncomplete`、`SourceDegraded`、`BackpressureDegraded`、`PhaseGateBlocked` 五级校验结果，明确 `source gap`、`stale`、`coverage drop`、单个 artifact incomplete 和可恢复 backpressure 是质量/覆盖信号，不得默认触发 collector 进程退出。
- 收窄 manifest、Polymarket discovery、atomic publish、bounded queue、ResourceWatermark 和方案不一致决策门中的硬失败措辞：coverage 不完整应写入 gap/incomplete 并限制消费者使用，其他 source/window 继续采集；只有路径越界、artifact 根不可写、partial artifact 被发布、SDK-first 被绕过、持续 hard threshold 且无法保全已采数据等条件才进入 fail closed。
- 为 bounded queue 和 ResourceWatermark 增加 soft/hard threshold、连续采样窗口、hysteresis、暂停/恢复动作记录要求，避免短暂资源或队列尖峰造成频繁停采。
- 修正 `PASS_WITH_LIMITS` 表述：它不再自动允许进入正式开发，必须列出限制和风险，并经用户明确批准后才能进入阶段 1。
- 根据复审 minor 反馈继续收窄 `BackpressureDegraded` 表格措辞：soft watermark 只记录 backpressure、降级、恢复观察和 gap；暂停新市场或单源 intake 只能发生在 hard threshold 连续越线且无法恢复之后。
- 修正计划文档自身验证命令：用 `awk` 直接检查 trailing whitespace，避免 `git diff --no-index --check /dev/null ...` 因正常文件差异返回非零而在 `set -e` 下被误判为失败。

### 路径口径再修复

- 根据用户指出的问题，修复 `plan/data-collect-plan.md` 中把远端服务器路径 `/home/ubuntu/btc-updown-marketmaker` 误写成本地开发、代码编写、本地构建、`cargo` 测试和 bench 目标路径的错误。
- 修复后的口径：本地开发、文档落盘、代码编写、本地构建、`cargo` 测试和 bench 均在 `/home/chillchill/btc-updown-marketmaker`；各远端服务器源码/工作目录统一写作 `~/btc-updown-marketmaker`。
- 保留 AWS live-trade 服务器不是构建机的限制；`/home/ubuntu/live-trade` 仍只表示 live 服务器上的运行产物根，不代表本地开发路径。

### Phase 0 执行启动

- 用户已在 2026-07-06 明确批准执行 `plan/data-collect-plan.md`；本地开发与测试通过后，授权使用数据采集服务器 `ubuntu@3.255.156.183` 做测试验证。
- 用户授权在需要腾出磁盘空间用于新采集程序测试验证时，终止数据采集服务器当前运行的采集程序，并删除该服务器上的旧采集程序和旧采集数据。
- 创建 Phase 0 技术验证报告 `data-collect/docs/phase0-technical-validation.md`，记录验证矩阵、路径与主机口径、运行根和 artifact 根 receipt 初版、本地只读环境核对和当时的 `IN_PROGRESS` verdict。
- 本地基线显示 `/home/chillchill/btc-updown-marketmaker` 所在文件系统已使用 96%；Phase 0 本地 bench 和 artifact 必须控制写入量，并把磁盘水位纳入 ResourceWatermark 证据。
- 用户随后释放本地磁盘空间；`2026-07-05T21:18:18Z` 复核显示该文件系统已降至 26% 使用、约 1.3T 可用。Phase 0 报告保留原始基线，并新增当前磁盘水位更新。

### Phase 0 Polymarket SDK 能力核对

- 创建 `data-collect/docs/polymarket-sdk-gap-report.md`，核对官方 Rust SDK `polymarket_client_sdk_v2`（GitHub clone commit `3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`，package version `0.6.0`）、官方 Market Channel 文档、官方 RTDS 文档和 SDK 源码。
- SDK gap verdict 为 `PASS`：SDK 覆盖 market channel、`custom_feature_enabled`、`book`、`price_change`、`tick_size_change`、`last_trade_price`、`best_bid_ask`、`new_market`、`market_resolved`、Gamma/CLOB metadata 和 RTDS Chainlink `btc/usd`。
- 记录实现约束：高层 typed stream 不能单独满足 `raw_json`、source gap、subscription evidence 和 lag/reconnect trace；后续 Polymarket adapter 必须使用官方 SDK 暴露的低层 WS 基础设施、request 类型和 typed response 类型构建采集证据 parser。
- 明确本次核对未授权 raw REST、manual signing、自建 SDK 或 shadow implementation；若后续测试需要 SDK 未公开能力或修改 SDK 源码，必须回到用户决策。
- 更新 `data-collect/docs/phase0-technical-validation.md` 的 SDK 能力状态和 Phase 0 当时阻塞项；当时 Phase 0 仍为 `IN_PROGRESS`，尚未完成 writer/publish、throughput/backpressure、network recovery、ResourceWatermark 和服务器角色验证。

### Phase 0 writer / publish 技术测试

- 创建最小 Rust workspace 和 `data-collect` crate：`Cargo.toml`、`Cargo.lock`、`data-collect/Cargo.toml`、`data-collect/src/lib.rs`、`data-collect/src/writer.rs`、`data-collect/src/manifest.rs`、`data-collect/tests/writer_publish_contract.rs`、`data-collect/benches/parquet_writer_probe.rs`。
- 按 TDD 执行 RED：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect writer_running_file_is_not_discoverable -- --nocapture` 先失败于缺少 writer/manifest/discovery API，确认测试覆盖未实现功能。
- 实现最小 typed raw parquet writer：合成事件字段覆盖 `source_kind`、`source_event_type`、`source_identifier`、`source_ts`、`recv_time_utc`、collector host/region、`raw_json`、`parquet_raw_layout_version`、`parquet_raw_source_kind`。
- 验证 running/finalize/atomic rename：`writer_running_file_is_not_discoverable` 和 `writer_finalize_atomic_rename_publishes_only_finalized` 均通过；`*-running.parquet`、`*.tmp`、`.rsync-partial` 不进入 finalized discovery，close 后通过 `fs::rename` 发布 finalized `.parquet` 并计算 sha256。
- 验证 manifest 完整性：`manifest_requires_row_count_sha256_schema_source_coverage` 通过；缺 sha256 或 source coverage 时 verdict 为 incomplete。
- 运行 writer probe bench：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo bench -p data-collect --bench parquet_writer_probe` 通过，单次输出 `rows=10000 elapsed_ms=3 final_path=/tmp/data-collect-parquet-writer-probe-91703/phase0-writer-probe.parquet sha256=e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1`。
- 更新 `data-collect/docs/phase0-technical-validation.md` 和 `plan/data-collect-plan.md`：Phase 0 writer/publish 当前 verdict 为 `PASS_FOR_PHASE0_PROBE`；该 bench 不等同于后续四源合并 `2k-5k events/s` 吞吐验收。

### Phase 0 throughput / backpressure 技术测试

- 创建 `data-collect/src/backpressure.rs`、`data-collect/tests/backpressure_contract.rs` 和 `data-collect/benches/throughput_probe.rs`。
- 按 TDD 执行 RED：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect bounded_queue_records_backpressure_and_source_gap -- --nocapture` 先失败于缺少 `data_collect::backpressure` 模块，确认测试覆盖未实现功能。
- 实现最小 Tokio bounded queue/backpressure 合同：`tokio::sync::mpsc::channel(capacity)`、queue full source gap、rows/bytes/oldest event age/writer lag watermark、soft/hard degradation、连续 hard 才 `PauseIntake`，且不把 backpressure 标为 `ProcessFatal`。
- 验证命令通过：`bounded_queue_records_backpressure_and_source_gap` 与 `queue_watermarks_include_rows_bytes_age_writer_lag` 均通过。
- 修正计划中的 bench 命令：原 `cargo bench -p data-collect throughput_probe -- --sample-size 20` 会混跑其他 bench target；改为 `cargo bench -p data-collect --bench throughput_probe -- --sample-size 20`。
- 本地 synthetic throughput probe 通过：`polymarket_p95_window` 302000 rows，约 1,852,329.93 events/s；`polymarket_max_window` 433000 rows，约 2,127,162.69 events/s；两者 writer queue high watermark 均为 5000 rows / 1866250 bytes，writer lag high watermark 2ms。
- 更新 `data-collect/docs/phase0-technical-validation.md` 和 `plan/data-collect-plan.md`：throughput/backpressure 当前 verdict 为 `PASS_FOR_PHASE0_SYNTHETIC_PROBE`；该结果不替代真实网络源、数据采集服务器 hardware tier、ResourceWatermark 或 soak 验证。

### Phase 0 network recovery / freshness 技术测试

- 创建 `data-collect/src/health.rs` 和 `data-collect/tests/network_recovery_contract.rs`。
- 按 TDD 执行 RED：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect retry_uses_bounded_backoff_jitter_and_records_trace -- --nocapture` 先失败于缺少 `data_collect::health` 模块，确认测试覆盖未实现功能。
- 实现最小 network recovery/freshness 合同：网络异常枚举、health/gap/reconnect trace 映射、有限 retry trace、backoff ceiling、deterministic jitter、按源独立 circuit state、PONG 与业务 freshness 分离、重连 gate。
- 完整测试文件通过：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --test network_recovery_contract -- --nocapture`，5 条测试全部通过。
- 指定恢复策略测试均通过：`retry_uses_bounded_backoff_jitter_and_records_trace`、`source_circuit_breakers_are_independent`、`pong_without_business_data_marks_stale_not_healthy`、`reconnect_requires_subscription_snapshot_and_fresh_business_data`。
- 更新 `data-collect/docs/phase0-technical-validation.md` 和 `plan/data-collect-plan.md`：network recovery 当前 verdict 为 `PASS_FOR_PHASE0_CONTRACT`；该合同仍不替代真实 Polymarket/Binance/RTDS live-source recovery 和 long-run freshness 验证。

### Phase 0 hardware tier / ResourceWatermark 技术测试

- 执行只读 SSH 核对数据采集服务器 `ubuntu@3.255.156.183` 和 live-trade 服务器 `ubuntu@108.130.74.143` 的 kernel、CPU、内存、磁盘、`ulimit -n` 和 PSI；本次未启动采集、未终止进程、未删除数据、未在 live 小主机运行 cargo 或 Rust 编译。
- 创建 `data-collect/docs/hardware-tier-validation.md`。数据采集服务器为 `2` vCPU、`1.9Gi` 内存、无 swap、`ulimit -n=1024`，根盘 `58G total / 52G used / 5.8G available / 90% used`；后续远端验证前需要按用户授权清理旧采集程序/旧采集数据或确认测试 artifact 根。
- live-trade 服务器为 `2` vCPU、`1.9Gi` 内存、无 swap、`ulimit -n=1024`，根盘 `58G total / 11G used / 47G available / 19% used`；确认仍是运行边界小主机，不是构建机。
- 创建 `data-collect/src/resource.rs` 和 `data-collect/tests/resource_watermark_contract.rs`；按 TDD 执行 RED 后实现 ResourceWatermark schema 与 soft/hard threshold verdict。
- 三条 ResourceWatermark 指定测试均通过：`resource_watermark_records_required_metrics_or_unavailable_reason`、`hard_threshold_records_degrade_stop_and_finalize_actions`、`soft_threshold_records_degraded_without_process_fatal`。
- 更新 `data-collect/docs/phase0-technical-validation.md` 和 `plan/data-collect-plan.md`：hardware/resource 当前 verdict 为 `PASS_FOR_PHASE0_CONTRACT_WITH_REMOTE_DISK_LIMIT`；Phase 0 仍需用户审查运行根、artifact 根、训练 UTC day 分区根和 paper/live handoff 根。

### Phase 0 verdict 汇总

- 当时更新 `data-collect/docs/phase0-technical-validation.md` 的 verdict 为 `BLOCKED_NEEDS_DECISION`，并补充 Phase 0 Summary Verdict。
- 技术 probe 结果：Polymarket SDK 能力 `PASS`；writer/publish `PASS_FOR_PHASE0_PROBE`；synthetic throughput/backpressure `PASS_FOR_PHASE0_SYNTHETIC_PROBE`；network recovery `PASS_FOR_PHASE0_CONTRACT`；hardware/resource `PASS_FOR_PHASE0_CONTRACT_WITH_REMOTE_DISK_LIMIT`。
- 未自动进入 Phase 1 的原因：Phase 0 必须经用户审查；最终训练 UTC day 分区根、paper/live handoff artifact 根和数据采集服务器 artifact 根仍未确认；数据采集服务器根盘当时约 `5.8G` 可用，远端验证前需要执行已授权清理或确认测试 artifact 根。
- 更新 `plan/data-collect-plan.md`：当时勾选步骤 0.7.1；后续远端验收 gate 补充后，远端 verdict 为步骤 0.7.2，用户审查 gate 为步骤 0.7.3，均保持未完成。

### 本地 Polymarket 网络约束补充

- 用户补充说明：本地电脑网络环境不能直接访问 Polymarket；后续如需在本地电脑测试或采集 Polymarket 数据，必须使用本地已配置的香港 VPN 线路。
- 已同步更新 `demand/data-collect-demand.md`、`plan/data-collect-plan.md` 和 `data-collect/docs/phase0-technical-validation.md`；本地直连 Polymarket 失败不得直接归因于 SDK、采集程序或 source stale，必须先记录 VPN/线路状态、endpoint、错误类别和重试证据。
- 当前 Phase 0 已完成的 writer、throughput、network recovery 和 ResourceWatermark 测试均为 synthetic/contract probe，没有发起本地 Polymarket live-source 采集，因此该约束不改变既有测试结果。

### Phase 0 fresh verification

- 根据新增本地 Polymarket 网络约束后的文档状态，重新运行本地验证：`cargo fmt --all --check`、`git diff --check`、data-collect 13 条合同测试、writer bench 和 throughput bench 均通过。
- 最新 writer bench 输出：10000 rows，3ms，finalized parquet sha256 `e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1`。
- 最新 throughput bench 输出：p95 window 302000 rows，约 2,007,033.79 events/s；max window 433000 rows，约 2,078,031.19 events/s；两者 queue high watermark 均为 5000 rows / 1866250 bytes。

### Phase 0 review fix

- 独立路径/范围审查发现 `plan/data-collect-plan.md` 的最终验证清单仍残留旧 throughput bench 命令；已统一修正为 `cargo bench -p data-collect --bench throughput_probe -- --sample-size 20`，避免误混跑其他 bench target。

### Phase 0 FAIL review remediation

- 两个独立审查指出当前 Phase 0 合同证据不足：manifest 使用缩水版 ResourceWatermark、network recovery 缺 `empty array` / `max elapsed` / `HalfOpen` / `Exhausted`、throughput bench 缺分位数与 finalize/resource 证据、writer publish 在 sha256 前暴露 finalized path、bounded queue 零容量会 panic。
- 已修复 writer publish 合同：`*-running.parquet` 不会被 create truncate；finalize 路径改为 running -> digesting -> final，`*-digesting.parquet` 不进入 finalized discovery；manifest required file 需要非空 role/path。
- 已补充 writer blocking-pool 边界：`write_segment_on_blocking_pool()` 使用 Tokio blocking pool 承载 create/write/finalize，Phase 0 证明文件写入可隔离出 async 主事件循环；正式 writer service 仍留在后续 runtime 阶段实现。
- 已修复 ResourceWatermark 合同：manifest 现在使用 `resource.rs` 中完整 ResourceWatermark；manifest completeness 要求每个资源指标有值或 unavailable reason；ResourcePolicy soft/hard threshold 覆盖 RSS、CPU、artifact dir、journal/log、open files、queue age、parquet writer memory、cgroup、PSI 和 TTL。
- 已修复 network recovery 合同：异常枚举加入 `EmptyArray`；retry trace artifact 显式记录 `max_elapsed_ms`、`jitter_seed` 和 `cumulative_elapsed_ms`；source-specific jitter 使用 source / attempt / seed 分散 sleep；source circuit state 覆盖 `HalfOpen` 与 `Exhausted`；PONG 无业务消息在 freshness timeout 内保持 `Reconnecting`，超时后才标记 stale。
- 已修复 bounded queue 合同：零容量配置返回错误，不 panic，避免配置错误变成不可解释的频繁进程失败。
- 已增强 throughput probe：经过 Tokio bounded queue `try_send`/`try_recv`，成功路径输出 events/s p50/p95/max、flush latency p50/p95/max、finalize latency、`finalize_failures=0`、RSS 和未测量 ResourceWatermark 字段；finalize 失败路径仍会 panic，不把失败路径结构化报告记为已完成。
- 修复后重新验证：`cargo fmt --all --check`、`cargo test -p data-collect -- --nocapture` 当前为 17 条合同测试通过；writer bench 10000 rows / 3ms；throughput bench p95 window 302000 rows 约 1,913,010.85 events/s，max window 433000 rows 约 2,074,725.70 events/s。
- 用户授权安装后，已通过 `rustup component add clippy` 安装 `cargo-clippy`。首次 clippy 检查发现 `data-collect/src/health.rs` 中一处 `collapsible_if`，已按 clippy 建议做最小修复；随后 `cargo clippy -p data-collect --all-targets -- -D warnings` 通过。

### 成熟工具优先规则复核

- 根据新增全局规则复核 `data-collect-demand.md`、`data-collect-plan.md`、Phase 0 实现和测试：当前技术路径优先复用官方 Polymarket Rust SDK、Binance 官方 WS/REST、Tokio、Apache Arrow Rust parquet writer、RustCrypto `sha2`、`tempfile` 和 clippy；未发现手写 SDK 旁路、parquet 格式、async runtime、HTTP/WebSocket 协议栈或加密哈希实现。
- 补充 `plan/data-collect-plan.md` 的成熟工具与自研边界：Phase 0 的 `health.rs` retry/circuit state 是合同 probe；Phase 4 正式实现 retry/backoff/jitter/circuit breaker 前必须先评估成熟 Rust crate 或 SDK 现有能力，若仍需自研状态机，必须记录缺口、取舍和验证方式。

### Phase 0 远端验收 gate 补充

- 根据用户同意的建议，补充 `plan/data-collect-plan.md` 阶段 0：清理数据采集服务器磁盘和确认临时测试根只是远端验证前置条件，不能直接进入阶段 1。
- 在任务 0.6 新增远端执行 gate：清理后 receipt、本地构建并上传 probe binary、远端 5-10 分钟 smoke、远端 30-60 分钟 synthetic throughput / ResourceWatermark 验收跑、真实网络 reachability smoke，以及资源边缘时可选 2-4 小时加强 soak。
- 明确数据采集服务器不把 Rust 编译计入采集能力验证：probe binary 在本地构建后上传，远端测试采集运行路径、writer publish、bounded queue、ResourceWatermark、磁盘/内存/CPU/PSI 和 endpoint reachability。
- 调整阶段 0 verdict：本地 probe 只能给出 `LOCAL_PROBE_*`；进入阶段 1 前还需要远端 `PASS_TO_PHASE1_REVIEW`，或用户明确接受 `PASS_WITH_LIMITS_NEEDS_USER_APPROVAL`。

### Phase 0 远端验收 gate 审查修复

- 派生三个 subagents 审查补充后的计划：一致性与过度设计审查为 `PASS_WITH_NOTES`，远端命令可执行性审查为 `FAIL`。
- 根据可执行性审查修复 `plan/data-collect-plan.md`：本地 release bench binary 使用临时 target dir 构建、筛选 executable file、重命名为稳定的 `parquet_writer_probe` / `throughput_probe` 后上传，避免 Cargo `release/deps` 中 `.d` 文件或旧 hash 被误选。
- 为远端 writer smoke、30-60 分钟验收跑和可选 2-4 小时 soak 中的 probe 执行增加 `timeout`，避免单次 probe 卡住导致计划时长失效。
- 为 HTTP reachability 增加统一 `probe_http()` 模板，记录 `http_code`、download size、time、remote IP、curl exit code 和 empty response，避免把 HTTP 4xx/5xx 或空响应伪装为成功。
- 为真实网络 reachability smoke 补充可执行命令模板：Binance depth/aggTrade、Polymarket Gamma、Polymarket CLOB metadata、WebSocket 工具检测，以及 Polymarket market channel / RTDS WebSocket handshake 或 `TOOL_UNAVAILABLE` 证据；未测项不得写成通过。
- 更新 `data-collect/docs/phase0-technical-validation.md`：显式写入本地 probe verdict 为 `LOCAL_PROBE_PASS`，整体 Phase 0 verdict 仍为 `BLOCKED_NEEDS_DECISION`。
- 第二轮复审发现 RTDS smoke payload 中 `filters` 误写为裸 `btc/usd`；已按需求修正为可追溯 `filters: "{\"symbol\":\"btc/usd\"}"` 的 subscription JSON，避免错误 payload 被误判为远端 RTDS 网络失败。
- 根据复审 notes 补充 HTTP reachability 的 `empty_response=false` 记录，并为可选 soak 增加 `SOAK_SECONDS` 2-4 小时硬 cap，避免外部环境变量把条件 soak 拉长为过度测试。

### Phase 0 远端验收执行

- 按用户授权在数据采集服务器 `ubuntu@3.255.156.183` 执行清理前 receipt、旧采集程序终止、旧数据目录删除和清理后 receipt。旧进程为 PID `124397`，命令为 `./poly-tx_v1.1.5 ws-series --series-prefix btc-updown-5m ...`；旧目录 `/home/ubuntu/ron/collect-orderbook` 大小约 `50G`；根盘从 `93%` 使用降至 `6%` 使用。
- Phase 0 临时测试根确认为 `~/btc-updown-marketmaker/data-collect/phase0-runs`，解析为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs`；owner/group 为 `ubuntu:ubuntu`，mode 为 `775`。该路径只作为 Phase 0 临时 run/artifact 根，不是最终训练或 paper/live 生产数据根。
- 本地构建 release bench binary 后上传，远端未运行 cargo 编译命令。`parquet_writer_probe` sha256 为 `629d2d221819206db25251e26e22e50d8c442311b272efd148b5068365e9687c`；`throughput_probe` sha256 为 `ab17ffd3b609db03d5ec5f061643fbcca89f1300e3f7382f0600665c8c49c105`；本地和远端 sha256 一致。
- 远端 5-10 分钟 smoke 于 `2026-07-05T22:49:50Z` 到 `2026-07-05T22:57:52Z` 完成。Binance depth/aggTrade、Polymarket Gamma/CLOB HTTP smoke 均为 `http_code=200` 且 `empty_response=false`；writer smoke 共 16 轮，每轮 `rows=10000`，sha256 为 `e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1`。
- 远端 45 分钟 synthetic throughput / ResourceWatermark 验收 run dir 为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-remote-20260705T225836Z`，共 415 轮。`polymarket_p95_window` eps_min/avg/max 为 `200941.84 / 484201.34 / 502871.99`；`polymarket_max_window` eps_min/avg/max 为 `272352.19 / 504273.28 / 521463.79`；两者 `finalize_failures=0`，PSI 无压力，`tmp` 文件数为 `0`，未触发 2-4 小时加强 soak 条件。
- 远端真实网络 reachability smoke run dir 为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-network-20260705T234452Z`。Binance depth/aggTrade、Polymarket Gamma/CLOB 均 `http_code=200` 且非空，错误文件大小为 `0`；远端没有 `websocat`/`wscat`，已记录 `TOOL_UNAVAILABLE websocket_client`，未把 Polymarket market channel / RTDS WebSocket handshake 写成通过。
- 远端收尾核对显示没有残留 `poly-tx`、`data-collect`、`collector`、`throughput_probe` 或 `parquet_writer_probe` 进程；Phase 0 run root 大小约 `21M`；根盘保持 `6%` 使用。
- 更新 `data-collect/docs/phase0-technical-validation.md` 和 `plan/data-collect-plan.md`：步骤 0.6.4-0.6.8 与 0.7.2 已完成；当时远端 verdict 为 `PASS_WITH_LIMITS_NEEDS_USER_APPROVAL`，原因是命令行 WebSocket 工具不可用；该限制已由后续 SDK-first WebSocket smoke 补测覆盖，阶段 0 仍未经用户审查通过不进入阶段 1。

### Phase 0 SDK-first WebSocket smoke 补测

- 根据用户对 `websocat/wscat` 的质疑，确认 `websocat`/`wscat` 只是命令行 smoke 工具，不是 Rust 标准库或正式实现依赖；更正确的补救是使用官方 Polymarket Rust SDK 编写最小 WebSocket smoke probe。
- 新增 `data-collect/benches/websocket_smoke_probe.rs` 和 bench target `websocket_smoke_probe`；新增依赖 `polymarket_client_sdk_v2 = 0.6.0`，features 为 `clob`、`gamma`、`rtds`、`ws`，并直接依赖 `futures` 与 `serde_json`。SDK 的 `ws`/`rtds` feature 通过 crate 依赖使用 `tokio-tungstenite`，没有安装 `websocat`/`wscat`。
- 本地构建通过：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo build -p data-collect --release --bench websocket_smoke_probe`。本地不运行 Polymarket live-source 连接，避免受本地非 VPN 网络环境干扰。
- 上传到数据采集服务器的 binary 路径为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/bin/websocket_smoke_probe`，sha256 为 `841fc12f5771675ea4624b414614a76c5b1fd1d8125a1423ccc64dbb2ce1c1fc`；远端未运行 cargo 编译命令。
- 远端 run dir 为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-sdk-ws-smoke-20260706T000336Z`，执行时间 `2026-07-06T00:03:36Z` 到 `2026-07-06T00:03:38Z`，`probe_exit=0`。
- market channel smoke：通过 SDK Gamma metadata 选取当前 open market token id，subscription JSON 包含 `type=market`、`operation=subscribe`、`assets_ids=[105267568073659068217311993901927962476298440625043565106676088842803600775810]`、`initial_dump=true`、`custom_feature_enabled=true`；market WS 达到 `Connected`、`subscription_count=1`，收到 `book` 业务消息，`bids=48`、`asks=41`。
- RTDS smoke：subscription JSON 为 `crypto_prices_chainlink` + `type=*` + escaped filter `{"symbol":"btc/usd"}`；RTDS WS 达到 `Connected`、`subscription_count=1`，收到 `crypto_prices_chainlink` 业务消息，`symbol=btc/usd`。
- 更新 `data-collect/docs/phase0-technical-validation.md` 和 `plan/data-collect-plan.md`：远端 technical verdict 从 `PASS_WITH_LIMITS_NEEDS_USER_APPROVAL` 改为 `PASS_TO_PHASE1_REVIEW`；0.7.3 用户审查 gate 仍未完成，未经用户审查通过不进入阶段 1。

### Phase 0 verdict 撤销与真实多源验证边界修复

- 用户指出阶段 0 存在严重边界错误：`websocket_smoke_probe` 的 market discovery 只通过 Gamma `closed(false)` 取 open market，优先匹配任意 `btc` / `bitcoin` slug 或 question，未命中时 fallback 到任意 open CLOB market；该逻辑不能证明当前 `btc-updown-5m` active market、prefetched next market 或 market rollover 正确。
- 复核后确认：远端 45 分钟验收是 synthetic throughput / ResourceWatermark，不是真实 Polymarket orderbook 长跑；Binance 只做 REST depth/aggTrade HTTP reachability，未验证官方 depth WS + snapshot stitch 或 aggTrade WS；RTDS 只做连接和单条 `btc/usd` 业务消息 smoke，未验证长时间 freshness 或与 Polymarket/Binance 时间线对齐。
- 复核后确认：`websocket_smoke_probe` 打印的 `custom_feature_enabled=true` subscription JSON 是本地构造的 `SubscriptionRequest`，实际订阅调用为 SDK `subscribe_orderbook(...)`；该证据不能作为 custom feature 实际发送帧的证明。
- 更新 `demand/data-collect-demand.md`：增加阶段 0 技术验收必须真实覆盖 `btc-updown-5m` 冷启动 discovery、current active、prefetched next、market rollover、Polymarket / Binance depth / Binance aggTrade / RTDS Chainlink 四源同时间线长跑；generic open market smoke、HTTP reachability、单条业务消息或 synthetic throughput 不得替代。
- 当时更新 `plan/data-collect-plan.md`：阶段 0 前置成功标准新增真实 active market discovery、四源长跑与时间线对齐；新增步骤 0.6.9 `btc-updown-5m` 冷启动 discovery 验证和 0.6.10 真实四源长时间采集 / rollover / 时间线对齐验收；步骤 0.7.2 改回未完成，原 `PASS_TO_PHASE1_REVIEW` 撤销，阶段 0 verdict 改为 `BLOCKED_NEEDS_DECISION`。
- 当时更新 `data-collect/docs/phase0-technical-validation.md`：remote verdict 改为 `BLOCKED_NEEDS_DECISION`，overall status 改为 `PHASE0_REOPENED_REAL_SOURCE_VALIDATION_REQUIRED`；保留旧 writer/synthetic/reachability 证据为容量预检和 reachability evidence，但明确其不足以证明阶段 0 技术架构通过真实数据验证。
- 同步修正计划最终验证命令：避免 `cargo test -p data-collect --all-targets` 默认执行 `harness=false` live-source bench，改为 `cargo test -p data-collect --lib --tests -- --nocapture`；live-source probe 必须作为显式 bench/远端验收命令运行。

### Phase 0 执行前复审修复与生产根路径决策

- 用户决定最终生产根路径放在数据服务器和 paper/live 交易服务器各自本机 `~/btc-updown-marketmaker/datasets/`；已同步到 `demand/data-collect-demand.md`、`plan/data-collect-plan.md` 和 `data-collect/docs/phase0-technical-validation.md`。该路径只承载 `data-collect` 外部市场源 mirror、coverage、manifest 或 handoff artifact，不替代 paper/live append-only event log、NOE/ledger 或交易 runtime artifact。
- 修复 Phase 0 gate 口径：最终生产根路径已获用户批准，不再以根路径未决作为 `BLOCKED_NEEDS_DECISION` 原因；当前 Phase 0 阻塞项仍是真实 `btc-updown-5m` discovery、rollover、四源长跑和时间线对齐未完成。
- 修复 `plan/data-collect-plan.md` 的 0.6.7 旧表述：45-60 分钟 synthetic throughput / ResourceWatermark 只作为容量预检，不再单独决定是否进入阶段 1。
- 修复 0.6.9 和 0.6.10 命令模板：新增本地 `set -euo pipefail`、远端 fail-closed bash wrapper、目录创建、binary sha256、run/artifact root、probe exit code、file-list 和 file sha256 收集，避免 `timeout ... | tee ...` 把 probe 失败伪装成成功。
- 补充 0.6.9 discovery receipt 字段：`discovery_run_id`、endpoint/query params 或 config digest、metadata raw evidence/digest、`fetch_start_utc`、`recv_time_utc`、snapshot freshness、current/next same condition/market proof，并禁止 generic `btc` / `bitcoin` open market fallback。
- 补充 0.6.10 最小 source loop / output schema 和证据字段：真实发出的 Polymarket subscription JSON/digest 必须证明 `custom_feature_enabled: true` 和 real current/prefetched asset ids；RTDS 必须记录 message timestamp、payload timestamp、symbol、value、source topic 和 `recv_time_utc`；四源必须进入同一 bounded queue 并输出 finalized parquet、manifest、coverage、health/gap/reconnect、ResourceWatermark 和 sha256。
- 修复需求验收覆盖索引：第九节现为 23 条验收标准，新增第 19 条映射到 0.6.9、0.6.10 和 0.7.2，后续编号顺延。

### Phase 0 真实源 discovery 与四源长跑执行

- 新增 `data-collect/src/market_discovery.rs` 和 `data-collect/tests/market_discovery_probe_contract.rs`，使用 exact slug `btc-updown-5m-{unix_5m_start}` 验证 current/next market；测试明确拒绝 generic BTC/Bitcoin open market fallback，并要求 `custom_feature_enabled=true` subscription JSON 绑定真实 asset ids。
- 新增 `data-collect/benches/market_discovery_probe.rs` 和 `data-collect/benches/real_source_alignment_probe.rs`；新增 bench target `market_discovery_probe`、`real_source_alignment_probe`；新增直接依赖 `reqwest` 与 `tokio-tungstenite` 用于 Binance 官方 HTTP/WS，Polymarket market channel 和 RTDS 仍使用官方 Rust SDK-first。
- 本地验证通过：`cargo fmt --all -- --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo build -p data-collect --release --bench market_discovery_probe --bench real_source_alignment_probe`。
- 0.6.9 远端 cold-start discovery 通过：run dir `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-market-discovery-20260706T011058Z`，binary sha256 `3240d55af3000564805f39548008820c41e9847d385a0ba4942a4680794ba5d6`，receipt verdict `PASS`，current `btc-updown-5m-1783300200`，next `btc-updown-5m-1783300500`，source_gaps `[]`。
- 0.6.10 第一轮真实四源 60 分钟长跑未通过但保留为有效诊断证据：run dir `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T011412Z`，row_count `180161`，finalized parquet sha256 `0e9e3c43bc2804b057541ba9865604c34d7200c2fbeb0fc06d582a7ee223af5b`，四源均有 coverage，Polymarket 覆盖 `14` 个 window，但 verdict 为 `SOURCE_GAP`，原因是 `binance_aggtrade_business_message_timeout` 5 次。
- 复核判定第一轮失败是 Phase 0 probe 的过度 freshness 校验：aggTrade 用 `5s` 无业务消息即记 source gap，不符合需求中的“长时间无业务消息”语义。已将 Binance depth freshness 调整为 `30s`，aggTrade freshness 调整为 `60s`，RTDS freshness 保持 `60s`；新版 `real_source_alignment_probe` binary sha256 为 `9a233dbb3fda705a3954b013b262543ad220791f31e08dd2343d8c352d2d7cb9`。
- 修复后 30 秒短诊断通过：run dir `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-short2-20260706T021557Z`，row_count `1618`，source_gaps `[]`。
- 0.6.10 第二轮真实四源 60 分钟长跑通过：run dir `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T021650Z`，`probe_exit=0`，manifest verdict `PASS`，row_count `177573`，finalized parquet sha256 `f561a27b39191d1636a12f61386a7f02089ab4e8bf3f408582f9bab1707ec775`，manifest sha256 `45440a6595584546633bffb617c4cf05475980df11c8254fffaa0b5f42f9a60d`，source_gaps `[]`。
- 第二轮 source coverage：Polymarket market `115074` 行并覆盖 `14` 个 `btc-updown-5m` window（`1783304100` 到 `1783308000`）；Binance depth `35991` 行；Binance aggTrade `23211` 行；RTDS Chainlink `3297` 行。
- 第二轮 ResourceWatermark receipt：`rss_bytes=77373440`、`queue_rows_high=94`、`queue_bytes_high=3568670`、`artifact_dir_bytes=81570811`；CPU、disk free、journal/log、PSI 在 Phase 0 probe 中以 unavailable reason 记录，正式 collector 阶段仍必须补齐真实采样。
- 更新 `plan/data-collect-plan.md` 和 `data-collect/docs/phase0-technical-validation.md`：0.6.9、0.6.10、0.7.2 标记完成；远端技术 verdict 重新汇总为 `PASS_TO_PHASE1_REVIEW`；overall status 为 `PHASE0_REMOTE_PASS_PENDING_USER_REVIEW`。0.7.3 用户审查 gate 仍未完成，未经用户审查不得进入阶段 1。

### Phase 0 审查后 rollover / evidence 小修

- 三个独立 subagent 完成 Phase 0 审查，均未发现 Critical blocker；共同指出需要把 rollover edge 和 subscription evidence 口径写得更精确，避免把 window 数量或本地构造 JSON 当成完整证据。
- 修复 `data-collect/benches/real_source_alignment_probe.rs`：新增 `rollover_edges.json` 和 manifest `rollover_edges`，记录 `previous_next_slug == new_current_slug`、`previous_current_retired_by_time`、`windows_contiguous`、每个 promoted current 的 first business recv time；source coverage 增加 `event_type_counts` 和 `market_window_first_recv_time_utc_ms`。
- 修复 `data-collect/src/market_discovery.rs` 与合同测试：拒绝 generic BTC fallback 不变；保留平台返回的 `candidate_start_ts_s` / `candidate_end_ts_s`。首次补证 run 发现 Gamma exact slug 的 `candidate_start_ts_s` 可早于 slug 目标窗口，因此修复为 slug/end 必须匹配、candidate start 允许早于 logical start 并保留原始值，而不是过度校验 exact start。
- 修复真实源 probe 网络边界：Binance REST snapshot 使用显式 connect/total timeout；Binance WS connect 使用显式 timeout；source send 改为 `try_send` + bounded wait，队列满时记录 backpressure，不再无限等待。
- 修复测试健壮性：`market_discovery_probe_contract` 的 subscription JSON 校验改为解析 `serde_json::Value` 后断言字段，不再依赖字符串顺序。
- 本地验证通过：`cargo fmt --all -- --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture`（21 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings`、`git diff --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo build -p data-collect --release --bench market_discovery_probe --bench real_source_alignment_probe`。
- 首次 targeted rollover 补证 run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-rollover-edge-20260706T033425Z` 失败：`probe_exit=1`，原因是 exact start 过度校验导致 `WindowBoundsMismatch`。该结果保留为修复依据。
- 修复后 targeted rollover run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-rollover-edge-20260706T033741Z` 通过：binary sha256 `f49fdb369a8f4f39c83921a85afb562314138c495d0af18e4a0737c9d92eec52`，`probe_exit=0`，manifest verdict `PASS`，row_count `39570`，finalized parquet sha256 `21bc3066abcb9d7b301b1a3125a7c2a86cf8e9cfb9a3ce9e929e65214da19405`，manifest sha256 `6b9480164db171810eb37109984653cb829f09aaa32de64e0104b651ef653dd8`，rollover_edges sha256 `7d405c5f4336b1f5a15cb3479801ca94796a5bb1d83be74c170bbf73c4d31a89`，source_gaps `[]`。
- targeted run 写出 `3` 条显式 rollover edge，全部满足 `prefetched_promoted_to_current=true`、`previous_current_retired_by_time=true`、`windows_contiguous=true`；Polymarket 覆盖 `5` 个 window，事件类型为 `best_bid_ask=13287`、`book=11472`、`market_discovery_switch=4`。Phase 0 仍只证明技术路径和业务消息，不宣称已完整实现 `price_change`、`last_trade_price`、`tick_size_change`、`new_market`、`market_resolved` 的生产事件合同。
- subscription evidence 口径修正：Phase 0 记录官方 SDK `Client::subscribe_best_bid_ask` 在 `polymarket_client_sdk_v2 0.6.0` 中调用 `subscribe_market_with_options(asset_ids, true)`，并记录业务消息；本 probe 未捕获 actual outbound frame，正式 adapter 阶段必须补齐 request evidence 或在 manifest 中写明 unavailable reason。
- 第二轮复审无 Critical/Important；根据 Minor 反馈在 `plan/data-collect-plan.md` 中补充：短诊断 run 必须显式设置 `PHASE0_REAL_SOURCE_MIN_ROLLOVER_EDGES=0`，避免未跨 market 边界的短 smoke 被误判为正式 gate 失败；Phase 4 正式实现必须把可恢复 backpressure observation 与阻塞 verdict 的 source gap 分开，只有 send timeout、drop、持续 hard threshold 或无法保全 segment 时才阻塞 complete verdict。

### Phase 0 hot path / parquet path 延迟补证计划

- 用户指出训练和 paper/live 对 `data-collect` 的消费路径不能等同：训练消费历史 finalized parquet，可以慢一些但必须正确；paper/live 需要实时 handoff，不能等待 parquet batch、finalize 或 manifest。
- 复核后确认 0.6.10 真实四源长跑只证明 durable parquet mirror、source coverage 和 rollover 证据，不足以证明 paper/live 实时 hot path、`source_recv_to_hot_delivery_ms`、`source_recv_to_write_done_ms`、queue high 持续时间或 parquet writer 与实时 handoff 的隔离。
- 根据用户同意，修正 `demand/data-collect-demand.md`：明确训练走 cold path，消费 finalized typed raw parquet + manifest + `data-pipeline` 输出；paper/live 走 hot path，消费 realtime handoff 或等价低延迟事件流；finalized parquet 不能作为 paper/live 热路径唯一事实来源。
- 更新 `plan/data-collect-plan.md`：新增步骤 0.6.11，要求远端真实四源 60 分钟 hot path / parquet path 延迟补证；probe 使用 fake realtime consumer，不接入真实 paper/live，不生成 paper/live append-only event log、rotation artifact、NOE/ledger、order evidence、execution feedback 或交易逻辑。
- 同步更新 `plan/data-collect-plan.md` 的任务 5.2、验收矩阵和消费者兼容性表：需求第九节验收标准由 23 条变为 24 条，新增 hot/cold path 接入边界映射到 0.6.11、5.1、5.2 和 6.2。
- 0.7.2 远端 Phase 0 verdict 重新打开为 `PHASE0_REOPENED_HOT_PATH_LATENCY_VALIDATION_REQUIRED`；0.6.11 完成并写入报告前，不得把旧的 durable mirror / 四源 coverage `PASS_TO_PHASE1_REVIEW` 作为当前进入阶段 1 的依据。

### Phase 0 hot path / parquet path 计划复审修正

- 根据独立复审反馈修正 `plan/data-collect-plan.md` 的 0.6.11：`event_seq` 改为在 collector/fanout 处分配，source task 只生成带 source receive 时间的 `TimedRawEvent`，避免多源并发任务各自分配序号造成对齐证据不可靠。
- 明确新增 `data-collect/src/latency.rs` 承载可单测的 fanout、receipt、latency summary 和 queue duration helper；`real_source_alignment_probe.rs` 只做真实源编排与 artifact 写出，避免核心延迟计算藏在 bench 私有逻辑里导致 integration test 无法覆盖。
- 明确在现有 `data-collect/src/writer.rs` 增加 instrumented finalize API，记录 close、sha256、atomic rename 和 total finalize 耗时；不得在 probe 中复制一套 shadow parquet writer。
- 收窄远端 sidecar 和 PASS 条件：Linux 指标缺失必须写 `unavailable:<reason>`；单个采样点缺失不直接阻塞，整段关键指标不可读才不能给 `PASS`；四源要求 coverage verdict，不要求每个源在每个 5 分钟窗口必然有业务事件。
- 修正远端命令模板：上传 binary sha256 后远端执行 `sha256sum -c`，probe 启动时显式传入 `DATASET_ROOT` 与 `REAL_SOURCE_RUN_ID`，避免 artifact 根和 run id 只存在于 shell 变量而未进入 probe。

### Phase 0 hot path / parquet path 本地实现

- 按 TDD 新增 `data-collect/tests/real_source_latency_contract.rs`；RED 阶段失败于缺少 `data_collect::latency` 和 `ParquetSegmentWriter::finalize_with_timing()`，确认测试覆盖的是尚未实现的 0.6.11 契约。
- 新增 `data-collect/src/latency.rs`：`TimedRawEvent`、collector/fanout `event_seq` 分配、hot path receipt、durable write batch receipt、`FlushPolicy`、queue duration tracker、latency stats。
- 修改 `data-collect/src/writer.rs`：在现有 running -> digesting -> final 发布路径上新增 `finalize_with_timing()`，记录 close、sha256、atomic rename 和 total finalize timing；未引入 shadow parquet writer。
- 修改 `data-collect/benches/real_source_alignment_probe.rs`：source task 发送 `TimedRawEvent`，collector 分配全局 `event_seq`；同一事件先进入 fake realtime bounded handoff，再进入 durable parquet batching path；writer worker 返回 `write_batch_receipts` 和 instrumented finalize timing；新增 `latency_summary.json`、`hot_path_receipts.jsonl`、`write_batch_receipts.jsonl`、`queue_samples.jsonl`。
- 修复实现过程中的两个问题：一次 writer patch 曾错误插入大量 noop 方法，已立即删除并复核文件规模；writer queue depth 计数曾存在“发送成功后加、writer 先减”的竞态，已改为发送前加、失败回滚，避免 queue high watermark 被污染。
- 修复 0.6.11 远端命令模板：`.sha256` 文件改为在 upload dir 内用 basename 生成，避免 `sha256sum -c` 在远端读取本地 `/tmp/...` 绝对路径而失败。
- 本地验证通过：`cargo fmt --all -- --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture`（26 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo build -p data-collect --release --bench real_source_alignment_probe`、`git diff --check`。
- 当前状态：0.6.11 本地实现与合同验证完成；远端数据采集服务器 60 分钟真实四源 hot path / parquet path 延迟补证尚未执行，Phase 0 不得进入阶段 1。

### Phase 0 latency unit 修正

- 用户指出本地写入耗时不应使用毫秒字段；复核后确认短 smoke 中 `write_events_ms p99=1` 只能说明本地写入小于或约等于 1ms 的粗粒度观测，不适合回答“本地计算/写入耗时是多少”。
- 修正单位口径：网络/业务路径延迟、batch wait、queue wait 继续使用毫秒；本地 `write_events`、finalize close、sha256、atomic rename 和 total finalize 改用微秒字段 `*_us`。未改动 `source_recv_to_hot_delivery_ms`、`source_recv_to_write_done_ms`、`batch_wait_ms` 等需求指定的路径延迟字段。
- 代码修正：`data-collect/src/writer.rs` 的 `FinalizeTiming` 改为 `finalize_total_us`、`finalize_close_us`、`sha256_us`、`atomic_rename_us` 等字段，并用 `Instant::elapsed().as_micros()` 采样；`data-collect/src/latency.rs` 和 `real_source_alignment_probe.rs` 将 `write_events_ms` 改为 `write_events_us`。
- 文档修正：`plan/data-collect-plan.md` 和 `data-collect/docs/phase0-technical-validation.md` 已同步 `*_us` 字段口径；旧 180 秒 smoke 的本地写入毫秒字段不再作为本地写入耗时解释依据。
- 远端状态：用户要求暂停时，60 分钟正式 run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-20260706T090834Z` 已终止，probe exit 为 `143`，并写入 `logs/paused-by-user.txt`；该 run 不作为阶段 0 验收证据。
- 重新上传单位修正后的 `real_source_alignment_probe`，远端 `sha256sum -c` 通过，binary sha256 为 `4e066002d33c0ec8d285b9d5e86549611052a669d3c2354d4103195e7c2a5aff`。
- 单位修正后的 120 秒短 smoke run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-units-smoke-20260706T091716Z` 通过：`probe_exit=0`，manifest verdict `PASS`，row_count `5778`，source_gaps `[]`。字段核对显示 `latency_summary.json` 和 `write_batch_receipts.jsonl` 不再包含旧 `write_events_ms/finalize_*_ms` 字段。
- 单位 smoke 结果：本地写入 `write_events_us` count `113`，p50 `119us`，p95 `626us`，p99 `1139us`，max `2318us`；`finalize_total_us=15264`、`finalize_close_us=2665`、`sha256_us=12526`、`atomic_rename_us=69`。网络/业务路径仍以毫秒报告：`source_recv_to_hot_delivery_ms` p95 `1ms`，`source_recv_to_write_done_ms` p95 `1007ms`，`batch_wait_ms` p95 `1097ms`。

### Phase 0 hot path / parquet path 正式远端补证

- 用户批准执行 60 分钟正式测试后，已在数据采集服务器执行 0.6.11 远端真实四源 hot path / parquet path 延迟补证。
- 正式 run dir 为 `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-20260706T092424Z`，binary sha256 `4e066002d33c0ec8d285b9d5e86549611052a669d3c2354d4103195e7c2a5aff`，`probe_exit=0`，manifest verdict `PASS`，row_count `181091`，finalized parquet sha256 `e78c6712eb23a2f02bc0425b319ca027ffefb25c2249d6562b0ed51068f37e21`，source_gaps `[]`。
- source coverage：Polymarket market `117578` 行，覆盖 `14` 个 `btc-updown-5m` market window；Binance depth `35989` 行；Binance aggTrade `24003` 行；RTDS Chainlink `3521` 行。按 `recv_time_utc_ms` 归入 `13` 个 UTC 5 分钟 bucket 时，四源均有事件，missing source bucket 为 `0`。
- rollover evidence：`rollover_edges.json` 写出 `12` 条 edge，均满足 `prefetched_promoted_to_current=true`、`previous_current_retired_by_time=true`、`windows_contiguous=true`。
- hot/durable 对齐：`hot_path_receipts.jsonl` 为 `181091` 行，`event_seq` 从 `1` 到 `181091` 连续；`write_batch_receipts.jsonl` 为 `3354` 个 batch，覆盖同一连续 `event_seq`，flush reason 为 `time=3353`、`shutdown=1`；finalized 目录只剩 `real-source-alignment-typed-raw.parquet`，无 running/digesting/tmp parquet 残留。
- 延迟结果：`source_recv_to_hot_delivery_ms` p50/p95/p99/max 为 `0/1/1/191ms`；`source_recv_to_write_done_ms` 为 `533/1004/1061/1102ms`；`batch_wait_ms` 为 `1011/1090/1099/1101ms`；`writer_channel_wait_ms` 为 `0/0/1/6ms`；本地 `write_events_us` 为 `112/382/1122/26476us`；`finalize_total_us=480767`、`finalize_close_us=63033`、`sha256_us=417658`、`atomic_rename_us=75`。
- queue/resource 结果：source event queue rows high `15`，hot handoff queue rows high `1`，writer chunk queue rows high `472`，无 backpressure wait/timeout/drop，`writer_backlog_monotonic_growth_observed=false`。sidecar 采样 `717` 条，RSS 峰值 `142856192` bytes，CPU 首个启动采样峰值 `100%`、稳定段约 `6-7%`，open files 峰值 `24`，disk free 最低 `57630863360` bytes，CPU/IO/memory PSI full avg10 无非零采样。
- 结论：0.6.11 已证明 hot path 不等待 parquet batch、write、sha256 或 finalize；durable write-done p95 约 `1004ms` 主要来自 `PHASE0_REAL_SOURCE_MAX_FLUSH_INTERVAL_MS=1000` 的 time flush 等待，本地写入 p95 为 `382us`。
- 限制：manifest 记录 `collector_host_id=ip-172-26-10-0`，但 `collector_region=unknown`；ResourceWatermark JSON 内部仍把 CPU、disk free、journal/log、PSI 标为 probe 未测，虽然本次 sidecar 已补齐这些 Linux 采样。该限制不推翻 hot/durable path 性能结论，但正式 collector runtime 必须补齐 region、clock sync status 和完整 ResourceWatermark manifest。
- 已更新 `plan/data-collect-plan.md` 和 `data-collect/docs/phase0-technical-validation.md`：0.6.11 与 0.7.2 标记完成，远端 Phase 0 verdict 为 `PASS_WITH_LIMITS_NEEDS_USER_APPROVAL`；0.7.3 用户审查 gate 仍未完成，未经用户明确审查通过不得进入阶段 1。

### Phase 0 collector region 修复与补证

- 用户要求不要把 `collector_region=unknown` 留到阶段 1；已在阶段 0 内修复并验证。
- 根因：Phase 0 probe 只读取 `COLLECTOR_REGION` 环境变量，未设置时默认 `unknown`；服务器实际 AWS IMDS placement region 已核对为 `eu-west-1`，availability zone 为 `eu-west-1a`。
- 新增 `data-collect/src/collector_identity.rs` 和 `data-collect/tests/collector_identity_contract.rs`：region 解析顺序为非空 `COLLECTOR_REGION` env override 优先，否则读取 AWS IMDS placement region，仍不可用时才写 `unknown`。
- `market_discovery_probe` 与 `real_source_alignment_probe` 已统一调用 `resolve_collector_region()`，避免后续 Phase 0 probe 再出现 bench 私有默认 `unknown`。
- TDD 证据：RED 阶段 `cargo test -p data-collect --test collector_identity_contract -- --nocapture` 失败于缺少 `data_collect::collector_identity`；GREEN 后 `collector_region_prefers_explicit_env_value`、`collector_region_uses_imds_when_env_is_missing`、`collector_region_uses_imds_when_env_is_blank`、`collector_region_is_unknown_only_when_no_source_is_available` 均通过。
- 本地验证通过：`cargo test -p data-collect --lib --tests -- --nocapture` 当前为 `30` 条测试通过；`cargo clippy -p data-collect --all-targets -- -D warnings` 通过；`cargo build -p data-collect --release --bench market_discovery_probe --bench real_source_alignment_probe` 通过。
- 远端真实源短 smoke 通过：run dir `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-region-smoke-20260706T104540Z`，binary sha256 `8459e763ff12921fc478a34c79ced1872235035690213991da408ebb24554b60`，执行时显式 `env -u COLLECTOR_REGION`，`probe_exit=0`，manifest verdict `PASS`，row_count `4430`，source_gaps `[]`。
- smoke manifest 和 initial discovery receipt 均写出 `collector_host_id=ip-172-26-10-0`、`collector_region=eu-west-1`；finalized parquet sha256 `ecf7347aa3b47b3342e83a76008efe9583c4d02e3b45d115da69379a831dda5f`。
- 已更新 `plan/data-collect-plan.md` 与 `data-collect/docs/phase0-technical-validation.md`：新增 0.6.12，移除 `collector_region=unknown` 作为当前限制项；远端 Phase 0 技术 verdict 更新为 `PASS_TO_PHASE1_REVIEW`。0.7.3 用户审查 gate 仍未完成，未经用户明确审查通过不得进入阶段 1。

### Phase 0 用户审查 gate 完成

- 用户要求“派生 subagents 替我完成用户审查 gate”。已派生 4 个独立子代理分别审查需求/计划/报告一致性、代码/测试/SDK-first 证据、远端性能/资源/验收证据、范围纪律/过度设计/路径规则。
- 四个审查结论均为 `PASS_WITH_NOTES`，gate recommendation 均为 `ACCEPT_WITH_NOTES`，未发现 Critical 或阻塞 Important 问题；综合 verdict 为 `ACCEPT_WITH_NOTES`。
- 已更新 `plan/data-collect-plan.md`：0.7.3 用户审查 gate 标记完成，Phase 0 当前状态为 `PHASE0_COMPLETE_READY_FOR_PHASE1`。
- 已更新 `data-collect/docs/phase0-technical-validation.md`：新增 12.11 用户审查 gate 记录，并把当前状态从 `PHASE0_REMOTE_PASS_PENDING_USER_REVIEW` 更新为 `PHASE0_COMPLETE_READY_FOR_PHASE1`。
- 保留阶段收尾约束：进入阶段 1 前仍必须完成 commit/push；不得把 Phase 0 probe 当成生产 collector，正式 collector 阶段仍需补齐 host clock sync status、生产级 ResourceWatermark runtime manifest、actual Polymarket outbound request evidence 或明确 unavailable reason、完整 source health/reconnect trace，以及 paper/live 主机 runtime receive timestamp 证据。

### Phase 0 阶段收尾 commit/push

- 已按阶段推进规则创建并推送 Phase 0 收尾提交。
- `plan/data-collect-plan.md` 和 `data-collect/docs/phase0-technical-validation.md` 已同步为 `PHASE0_COMPLETE_READY_FOR_PHASE1`。
- 未纳入本次提交的未跟踪资料包括 `TimesFM与流式增量模型讨论结论.md`、`mistake-log/V2/`、`mistake-log/never-full-load/`、`mistake-log/online-feedback/`、`mistake-log/sdk-live-trade/`；这些不属于本次 data-collect Phase 0 收尾范围。

### Phase 1 合同与 schema 冻结实现

- 按 TDD 为阶段 1 新增合同测试；RED 阶段失败于缺少 `data_collect::event`、`data_collect::source_format`、manifest 扩展字段和 health/gap 正式合同类型，确认测试覆盖尚未实现的阶段 1 内容。
- 新增 `data-collect/src/event.rs` 和 `data-collect/tests/event_envelope_contract.rs`：固定 `source_kind`、`source_event_type`、`source_identifier`、source timestamp / no-source-timestamp evidence、`recv_time_utc`、collector host/region、host clock sync、raw_json / equivalent reference、`parquet_raw_layout_version` 和 `parquet_raw_source_kind` 合同。
- 新增 `data-collect/src/source_format.rs`、`data-collect/tests/source_format_contract.rs` 和 `data-collect/tests/consumer_format_compatibility_contract.rs`：固定 Polymarket market / metadata、Binance depth update / snapshot、Binance aggTrade、RTDS Chainlink BTC/USD 的 typed raw 字段和消费者边界；确认 `data-collect` 不生成 labels/features/tick_stream/split、paper/live append-only log、NOE/ledger 或交易决策。
- 扩展 Phase 0 既有 `data-collect/src/manifest.rs`，新增 health/gap/reconnect artifact reference、start/end、coverage window、manifest source gap、coverage incomplete reason 和 failure class；incomplete manifest 归类为 `ArtifactIncomplete`，不会默认升级为 `ProcessFatal`。
- 扩展 Phase 0 既有 `data-collect/src/health.rs`，新增需求级异常清单、`SourceHealthEvent`、`CoverageVerdict` 和 `RuntimeFailureClass`；writer queue/backpressure/resource watermark 进入 `BackpressureDegraded`，source gap/stale 进入 `SourceDegraded`，均不默认触发进程级失败。
- 修正计划文档后续阶段文件动作口径：Phase 0 已创建的 `manifest.rs`、`health.rs`、`writer.rs`、`resource.rs` 在后续阶段按“修改/扩展”执行，不再写成从零“创建”，避免执行阶段误判真实文件状态。
- 阶段 1 第一轮审查结果：1 个 subagent `FAIL`、2 个 subagents `PASS_WITH_NOTES`。成立问题包括 Binance depth direct stream 被错误要求 combined wrapper、manifest source coverage 有效性校验不足、`parquet_raw_layout_version` 在 envelope 与 writer 间类型不一致、event envelope 缺 `source_endpoint`、`market_resolved` typed schema 风险，以及 paper/live hot path 语义不够显式。
- 已修复审查问题：`EventEnvelope` 新增 `source_endpoint`，`parquet_raw_layout_version` 改为与 writer schema 一致的 numeric version；Binance depth update 新增必填 official `stream_name`，`combined_stream` 只在使用 combined wrapper 时保留；manifest 校验每条 source coverage 的 source kind、row count 和 recv window；新增最小 `PolymarketMarketResolutionRaw` 合同固定 `market_resolved` retired event predicate 字段；paper/live consumer boundary 新增 `requires_realtime_handoff`。
- 复审 notes 继续收敛：`PolymarketMarketResolutionRaw` 显式要求 `source_event_type == "market_resolved"`；`writer::RawEvent` 和 parquet writer schema 新增 `source_endpoint` 字段，避免 event envelope 与 durable typed raw mirror 分叉；`combined_stream` 若存在但仅为空白也判为 incomplete。
- 阶段 1 新增测试通过：`cargo test -p data-collect --test event_envelope_contract -- --nocapture`（5 条）、`cargo test -p data-collect --test source_format_contract -- --nocapture`（6 条）、`cargo test -p data-collect --test consumer_format_compatibility_contract -- --nocapture`（4 条）、`cargo test -p data-collect --test manifest_contract -- --nocapture`（3 条）、`cargo test -p data-collect --test health_gap_contract -- --nocapture`（4 条）。
- 阶段 1 最终复审结果：需求/计划一致性审查为 `PASS_WITH_NOTES` 且无 Critical/Important；Rust 代码/测试质量审查从第一轮 `FAIL` 修复后转为 `PASS`；schema/消费者边界审查从 `PASS_WITH_NOTES` 修复后转为 `PASS`。最终无阻塞 commit/push gate 的问题。
