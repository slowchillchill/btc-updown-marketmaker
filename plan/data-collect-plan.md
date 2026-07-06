# data-collect Implementation Plan / 共用数据采集模块计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `./data-collect/` 建立只负责外部市场源采集证据的 Rust async/non-blocking 模块，为训练、paper trade 和 live trade 提供 finalized typed raw parquet、coverage、health/gap、reconnect trace、manifest 和 ResourceWatermark 证据。

**Architecture:** 先用技术测试与性能验证证明 SDK 能力、四源采集、parquet 连续写入、atomic rename、bounded queue/backpressure 和服务器资源水位可靠；验证通过后再进入正式模块开发。正式模块按 source adapter、event envelope、writer/publisher、health/gap、resource watermark 和 manifest contract 分层，禁止把 data-pipeline、paper/live 交易逻辑或 SDK 旁路实现写入 `data-collect`。

**Tech Stack:** Rust、Tokio 或等价 async runtime、官方 Polymarket Rust SDK-first、Binance 官方 WS/REST、Apache Arrow Rust parquet writer、bounded channel、background/blocking pool、sha256 manifest。

---

> 对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

> 对应总需求文档：`/home/chillchill/btc-updown-marketmaker/demand/rebuild-core-demand.md`

> 对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/data-collect-changelog.md`

> 事项编号：`data-collect-demand`

> 状态：用户已在 2026-07-06 批准执行本文档；执行仍限定为 data-collect 本地开发、测试和经授权的数据采集服务器验证，不授权 paper、live、真实下单、撤单或资金操作。

## 零、路径执行口径

1. 当前本地开发、文档落盘、代码编写、本地构建、`cargo` 测试和 bench 的仓库是 `/home/chillchill/btc-updown-marketmaker`；本文档、对应需求和 changelog 也在该仓库维护。
2. 各远端服务器上的源码/工作目录统一写作 `~/btc-updown-marketmaker`；在 `ubuntu` 用户服务器上该路径通常解析为 `/home/ubuntu/btc-updown-marketmaker`，但计划、命令和 changelog 不得把展开后的远端路径误写成本地开发路径。
3. 下文所有 `data-collect/...` 代码、测试、bench 和运行文档路径，除非明确写成 `demand/`、`plan/`、`changelog/`，均表示本地 `/home/chillchill/btc-updown-marketmaker/data-collect/...` 下的实现路径；远端验证时对应服务器 `~/btc-updown-marketmaker/data-collect/...`。
4. 下文命令块中的 `cargo ...` 为可读性省略了工作目录；本地执行时必须以 `/home/chillchill/btc-updown-marketmaker` 为 workdir。只有在明确进入数据采集服务器验证时，才允许在该服务器 `~/btc-updown-marketmaker` 下执行；不得在 AWS live-trade 小主机上运行。
5. AWS 爱尔兰 live-trade 服务器只允许低影响运行边界核对和已构建二进制运行；不得在该服务器运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`、训练式重处理、历史全量 parquet 扫描或归档压缩。
6. 生产数据根路径由用户决定为各运行服务器本机 `~/btc-updown-marketmaker/datasets/`：数据服务器的训练 raw mirror、paper/live 交易服务器的 handoff mirror/coverage/manifest 均使用该 server-local 根路径；其中 `~` 按当前服务器用户解析，不得写成本地开发机路径，也不得把 paper/live append-only event log、NOE/ledger 或交易 runtime artifact 混入 `data-collect` 范围。
7. 阶段 0 必须产出 `data_collect_run_root`、`data_collect_artifact_root`、训练 UTC day 分区根、paper/live handoff artifact 根和权限边界 receipt；最终生产根路径已由用户批准，Phase 0 仍需验证 canonical path、owner/group/mode、路径越界 fail-closed 和 artifact evidence，但不得仅因最终根路径未决而阻塞。
8. 所有运行根和 artifact 根必须是绝对路径，必须解析在用户批准的根目录内；路径越界、权限不匹配、owner/group 不符合 receipt、或解析到 `/home/ubuntu/live-trade` 非授权子路径时必须 fail closed。
9. 本地电脑网络环境不能直接访问 Polymarket；如果后续需要在本地电脑测试或采集 Polymarket live-source 数据，必须使用用户已配置的香港 VPN 线路，并记录 VPN/线路状态、endpoint、错误类别和重试证据；本地直连失败不得直接归因于 SDK、采集程序或 source stale。

## 一、计划定位

本计划是 `data-collect-demand` 的模块计划文档，范围限定为本地仓库 `/home/chillchill/btc-updown-marketmaker/data-collect/` 中的 `./data-collect/` 模块；远端验证时同一相对路径对应服务器 `~/btc-updown-marketmaker/data-collect/`。

它负责回答：

1. 在正式开发前，如何先验证 Polymarket SDK 能力、四源采集、parquet 写入、atomic rename、bounded queue/backpressure 和服务器资源水位是否可靠。
2. 验证通过后，`data-collect` 模块应按什么合同、文件结构、阶段和 gate 实现。
3. 每个阶段如何验证满足 `demand/data-collect-demand.md` 的验收标准。
4. 发现 SDK 缺口、性能不可达、live 小主机资源不匹配或边界冲突时，如何停止并回到用户决策。

它不负责：

1. 生成 `data-pipeline` 的 labels、features、tick_stream、训练切分或 T-30 样本加工。
2. 实现 paper/live 交易决策、下单、撤单、风控、订单生命周期归一化、NOE/ledger 或 execution feedback ingestion。
3. 绕过官方 Polymarket Rust SDK；SDK 不覆盖的能力必须先形成缺口说明并获得用户同意。
4. 在 AWS 爱尔兰 live-trade 小主机上编译 Rust、运行训练式重处理、历史全量 parquet 扫描、压缩归档批处理或 archive/upload/listing/gzip。

### 1.1 成熟工具与自研边界

1. 技术路径优先复用成熟工具和库：官方 Polymarket Rust SDK、Binance 官方 WS/REST 协议、Tokio async runtime / bounded channel / blocking pool、Apache Arrow Rust parquet writer、RustCrypto `sha2`、`tempfile` 测试夹具，以及后续 source adapter 需要的成熟 HTTP/WebSocket client crate。
2. `data-collect` 自研部分只允许承载项目特有证据合同：event envelope、source coverage、source health/gap/reconnect trace 语义、manifest completeness、ResourceWatermark policy、消费者边界声明和路径 fail-closed 规则。
3. 不得自研或绕过已有成熟基础设施：不得手写 Polymarket SDK 旁路、manual signing、raw REST/WS 主实现、parquet 格式、async runtime、HTTP/WebSocket 协议栈、加密哈希实现、archive/upload/listing/gzip。
4. Phase 4 正式实现 retry/backoff/jitter/circuit breaker 前，必须先评估成熟 Rust crate 或 SDK 现有能力；若仍需自研状态机，必须在 changelog 记录现有库缺口、取舍和验证方式。Phase 0 当前 `health.rs` 只作为合同 probe，不等同于最终生产 retry 框架。

## 二、前置成功标准

正式开发前必须先通过阶段 0。阶段 0 不通过时，不进入阶段 1-6。

阶段 0 通过标准：

1. 官方 Polymarket Rust SDK 能力核对完成，明确 market channel、RTDS、market discovery、metadata 是否覆盖需求；缺口已经形成报告并获得用户批准，或不存在阻塞缺口。
2. 技术测试必须真实发现当前 `btc-updown-5m` active market 和 prefetched next market，跨至少一个 market rollover 验证 current -> retired、prefetched -> active 的切换边界；不得用 generic BTC/Bitcoin open market 或任意 CLOB market 代替。
3. 技术测试必须用真实 Polymarket market channel、Binance `BTCUSDT` depth WS + REST snapshot、Binance `BTCUSDT` aggTrade WS、RTDS Chainlink `btc/usd` 四源同时运行，并按同一 UTC window 记录 coverage、first business event、source timestamp、`recv_time_utc`、source gap/stale 和逐源吞吐。
4. 技术测试证明四源合并 `2k-5k events/s` 的解析、排队、写入、health/gap 统计路径在目标 hardware tier 上可达到需求基线，或明确给出达不到时的真实瓶颈和备选方案；synthetic throughput 只能作为容量预检，不再单独构成阶段 0 通过证据。
5. 技术测试证明 parquet running/temp segment、close/finalize、sha256、manifest、required_files[] 和 atomic rename 发布路径可恢复、可校验、不会被消费者读取 partial artifact。
6. 技术测试证明长时间无业务消息、ping/pong 正常但数据静默、rate limit、server shutdown、snapshot 缺失、source stale、writer queue 堵塞和资源水位越线都会写入 source health、source gap 或 reconnect trace，并按“运行时校验分级与失败动作”处理；普通 gap/stale 不得默认触发 collector 进程退出。
7. 数据采集服务器和交易服务器分别记录 hardware tier；live 交易小主机只做运行边界评估，不承担 Rust 编译、训练式重处理、历史全量 parquet 扫描或归档压缩。
8. ResourceWatermark 技术测试覆盖 RSS、CPU、disk free、artifact dir bytes、journal/log bytes、journal 限额状态、open files、queue bytes、queue age、parquet writer memory estimate、cgroup memory、PSI、anon/file/slab、stage latency / TTL 和 unavailable reason。
9. 阶段 0 产物写入后续 changelog，包含命令、commit 或源码快照、artifact 路径、sha256、硬件 tier、通过/失败 verdict、剩余风险和用户决策点。

## 三、候选文件结构

以下文件结构是后续正式开发的计划边界，均相对本地仓库 `/home/chillchill/btc-updown-marketmaker`；远端验证时均相对服务器 `~/btc-updown-marketmaker`。执行时如果真实 Rust workspace 与本计划不一致，必须按“方案不一致决策门”停下来记录事实、影响和选项。

| 路径 | 责任 |
| --- | --- |
| `data-collect/Cargo.toml` | `data-collect` Rust crate 依赖、feature、binary 定义 |
| `data-collect/src/main.rs` | collector binary 入口，只做配置加载、runtime 启动、任务编排和 shutdown |
| `data-collect/src/config.rs` | source endpoint、market target、queue 水位、timeout、retry、resource threshold 和 artifact root 配置 |
| `data-collect/src/event.rs` | `recv_time_utc`、source timestamp、collector host/region、raw_json、source kind、event type 的统一 envelope |
| `data-collect/src/sources/polymarket.rs` | Polymarket market channel、market discovery、metadata、new_market、retired predicate 和 SDK 缺口接入边界 |
| `data-collect/src/sources/binance.rs` | Binance `BTCUSDT` depth、snapshot stitch、aggTrade 和官方 WS/REST source integrity |
| `data-collect/src/sources/rtds.rs` | Polymarket RTDS Chainlink `btc/usd` subscription、freshness 和 stale/gap 记录 |
| `data-collect/src/health.rs` | source health、source gap、reconnect trace、freshness、circuit breaker 和 retry state |
| `data-collect/src/writer.rs` | typed raw parquet writer、running/temp segment、flush、close/finalize 和 writer queue 指标 |
| `data-collect/src/publish.rs` | finalized artifact atomic rename、required_files[]、sha256 和 incomplete verdict |
| `data-collect/src/resource.rs` | ResourceWatermark 采样、threshold policy receipt、硬阈值动作和 unavailable reason |
| `data-collect/src/manifest.rs` | run manifest schema、source coverage、schema version、code version、collector binary sha256 和 validation verdict |
| `data-collect/tests/` | contract、writer、manifest、network recovery、resource watermark 和 boundary 测试 |
| `data-collect/benches/` | 阶段 0 和性能验收复用的吞吐、writer、resource microbench |
| `data-collect/docs/phase0-technical-validation.md` | 阶段 0 技术测试报告；正式开发前必须先生成 |

## 数据格式合同

`data-collect` 的输出格式是 typed raw parquet + manifest + health/gap/reconnect artifact。它只提供外部市场源事实证据，不直接生成 `data-pipeline` 的 labels、features、tick_stream、训练切分，也不生成 paper/live 的 append-only event log、order evidence、NOE/ledger 或交易决策。

### 共用 event envelope

每条源事件写入 typed raw parquet 前必须先满足共用 envelope：

| 字段 | 要求 |
| --- | --- |
| `source_kind` | 枚举：`polymarket_market`、`polymarket_market_metadata`、`binance_depth_update`、`binance_depth_snapshot`、`binance_aggtrade`、`polymarket_rtds_chainlink_btc_usd` |
| `source_event_type` | 保留平台事件类型；无事件类型的 snapshot 必须写 snapshot 类型 |
| `source_identifier` | stream/topic/market/condition/token/asset id 的可追溯标识 |
| `source_ts` | source 原始 timestamp；若源无 exchange timestamp，必须写 no-source-timestamp 证据和 snapshot boundary |
| `recv_time_utc` | 采集进程收到业务消息的 UTC 时间，不覆盖 source timestamp |
| `collector_host_id` / `collector_region` | 延迟统计和跨区域对照必填 |
| `host_clock_sync_status` | clock sync 状态或 unavailable reason |
| `source_endpoint` | WS/REST/RTDS endpoint 或 endpoint digest |
| `raw_json` | parseable 原始 payload 或等价可还原原始证据引用 |
| `parquet_raw_layout_version` | parquet raw layout version |
| `parquet_raw_source_kind` | 与 `source_kind` 一致，用于 data-pipeline raw mirror 识别 |

### 逐源 typed raw 格式

| Source | Parquet source kind | 必须保留的 typed 字段 | 必须保留的 raw/metadata | 消费边界 |
| --- | --- | --- | --- | --- |
| Polymarket market channel | `polymarket_market` | `event_type`、`market` 或 condition/market id、`asset_id` / token id 集合、平台 `timestamp`、`recv_time_utc`、price/size/orderbook delta 字段中该事件实际存在的字段 | raw subscription JSON、`assets_ids`、`type: "market"`、`custom_feature_enabled`、订阅确认或首个匹配业务事件、`raw_json` | data-pipeline 可作为外部市场 typed raw 输入；paper/live 只能作为外部市场证据和 coverage 输入，不能替代运行期 append-only log |
| Polymarket market metadata / `new_market` | `polymarket_market_metadata` | `condition_id`、token id / asset id、CLOB market id、slug、start/end、active、acceptingOrders、`market_lifecycle_state`、tick size、min size、fee/reward 原始字段 | `new_market` raw payload、metadata snapshot raw_json、candidate slug、active scan evidence、clock drift evidence | 支撑 Market Context Builder 和 data-pipeline 的市场上下文；不授权 submit path，不生成 ModelRegistry 状态 |
| Binance BTCUSDT depth update | `binance_depth_update` | `e`、`E`、`s`、`U`、`u`、`b`、`a`、`recv_time_utc`、本地 `local_u` / stitch evidence 引用 | official WS stream name、combined stream wrapper `stream/data`、raw_json、stale/duplicate/gap evidence | data-pipeline 可重建 source integrity；不得直接生成 labels/features/tick_stream |
| Binance BTCUSDT depth snapshot | `binance_depth_snapshot` | `lastUpdateId`、`bids`、`asks`、`snapshot_fetch_start_utc`、`snapshot_recv_time_utc` | official REST endpoint、raw_json、no exchange timestamp evidence | 只用于 depth stitch 和 source gap 恢复；不得伪造 exchange timestamp |
| Binance BTCUSDT aggTrade | `binance_aggtrade` | `e`、`E`、`s`、`a`、`p`、`q`、`f`、`l`、`T`、`m`、`buyer_is_maker`、`recv_time_utc` | official WS stream name、raw_json、trade id 连续性可解释性 | 满足 data-pipeline 对 aggTrade typed raw 的字段完整性，避免缩水 schema；训练可见性由 data-pipeline 后续处理决定 |
| Polymarket RTDS Chainlink BTC/USD | `polymarket_rtds_chainlink_btc_usd` | RTDS message timestamp、payload timestamp、symbol、value、source topic、`recv_time_utc` | subscription raw JSON、`action: "subscribe"`、`topic: "crypto_prices_chainlink"`、`type: "*"`、symbol filter、ack/first message 或 close/error evidence | Chainlink 价格证据，不替代 Binance 官方 orderbook/depth 或 aggTrade |

### 格式对消费者的兼容性要求

1. 对 `data-pipeline`：typed raw parquet 必须保留 `raw_json`、`parquet_raw_layout_version`、`parquet_raw_source_kind`、source timestamp、`recv_time_utc`、source coverage 和 manifest；`data-pipeline` 后续自行生成 labels、validation、tick_stream、features 和训练切分。
2. 对训练：训练消费者只能在 `data-pipeline` 完成 labels/features/tick_stream 和 split 后消费训练数据；`data-collect` 的 parquet 完整只证明外部市场源 raw mirror 完整，不等于训练批准。
3. 对 paper/live：paper/live 可以消费 `data-collect` 的外部市场源证据、coverage 和 manifest，但运行期 source of truth 必须仍由 paper/live 自身 append-only event log 或等价不可丢事件流、Market Context Builder、NOE/Ledger projection 承接；非爱尔兰采集主机 `recv_time_utc` 不能冒充 paper/live 主机接收延迟。
4. 对三类消费者共用合同：source kind、schema version、manifest、coverage window、causal timestamp 语义必须一致；允许字段为空但必须有 nullable 语义或 unavailable reason，禁止不同消费者各自定义影子 raw schema。

### 运行时校验分级与失败动作

校验结果必须分级处理，不能把数据质量信号默认升级为 collector 进程失败：

| 等级 | 触发条件 | 运行时动作 | 产物语义 |
| --- | --- | --- | --- |
| `ProcessFatal` | 运行根或 artifact 根越界、未授权路径、持久化边界完全不可写、partial artifact 被发布、SDK-first 被绕过、硬资源阈值持续越线且无法 finalize 已采数据 | fail closed，停止当前危险动作，保护已采 segment，写入 fatal record，等待用户或运维处理 | 不发布伪完整产物 |
| `ArtifactIncomplete` | 单个 segment/window 缺少 row count、sha256、schema version、manifest 一致性证据，或 source coverage 不完整 | finalize 可保全数据，manifest 标记 incomplete；collector 继续采集后续 window/source | 消费者不得把该 artifact 当 complete 使用 |
| `SourceDegraded` | 连接失败、rate limit、空 payload、业务静默、snapshot 缺失、source stale、source gap、订阅证据不足 | 按源记录 health/gap/reconnect trace，独立 backoff/circuit breaker；其他源继续运行 | coverage/health 降级，不代表全局采集失败 |
| `BackpressureDegraded` | bounded queue、writer lag、memory/disk/resource soft watermark 越线 | 记录 backpressure、source health 降级、恢复观察和 source gap；只有 hard threshold 连续越线且无法恢复时，才暂停新市场或单源 intake 并进入保全停采 | manifest 记录水位、动作和恢复状态 |
| `PhaseGateBlocked` | 阶段 0 技术路径、SDK 能力、吞吐、服务器角色或需求边界未通过 | 阻止进入下一阶段或正式开发，提交事实和选项给用户 | 不代表已有采集 artifact 失效 |

默认规则：`source gap`、`stale`、`coverage drop`、`incomplete manifest` 是质量和覆盖证据，不得单独触发 collector 进程退出；只有 `ProcessFatal` 或持续硬阈值保全条件才能停止危险动作。所有降级必须带恢复条件、连续采样窗口或 hysteresis，避免短暂尖峰导致频繁停采。

## 四、阶段总览

| 阶段 | 目标 | 主要产物 | 进入下一阶段条件 |
| --- | --- | --- | --- |
| 阶段 0 | 技术测试与性能验证 | SDK 能力报告、吞吐/写入/网络/资源测试报告、phase0 manifest | 用户审查通过，且满足阶段推进通用 gate |
| 阶段 1 | 合同与 schema 冻结 | event envelope、逐源数据格式、manifest、health/gap、ResourceWatermark 合同测试 | 合同测试通过，需求范围未越界，且满足阶段推进通用 gate |
| 阶段 2 | source adapter 实现 | Polymarket、Binance depth/aggTrade、RTDS 采集和 gap 证据 | 四源 contract/integration 测试通过，且满足阶段推进通用 gate |
| 阶段 3 | writer 与发布 | typed raw parquet、running/temp、finalize、atomic rename、manifest | partial artifact 防误读测试通过，且满足阶段推进通用 gate |
| 阶段 4 | runtime 韧性与 backpressure | bounded queue、retry/backoff+jitter、circuit breaker、resource hard action | 网络异常和资源越线测试通过，且满足阶段推进通用 gate |
| 阶段 5 | 消费者边界与分区 | 训练 UTC day 分区、paper/live artifact 边界声明、finalized-only discovery | 不生成 labels/features/trading logic，且满足阶段推进通用 gate |
| 阶段 6 | 端到端验收与交付 | soak test、source coverage、ResourceWatermark、final review、changelog | 需求验收项逐条可追溯，且满足阶段推进通用 gate |

### 阶段推进通用 gate

每个阶段完成后、进入下一阶段前都必须执行：

1. 自审 diff、需求覆盖、测试输出、artifact/manifest 证据、边界和剩余风险。
2. 更新 `changelog/data-collect-changelog.md`，记录阶段事实、命令、退出码、artifact 路径、sha256、与计划不一致处和最终决策。
3. 派生独立审查 subagent，使用 `gpt-5.5` 和 `xhigh`，检查需求覆盖、正确性、边界、过度设计、遗漏、测试和与 `rebuild-core-demand` 的一致性。
4. 修复审查发现的范围内问题，并重复审查直到通过；若反馈要求改变 SDK-first、服务器角色、吞吐验收、运行根、artifact 根、消费者边界或非范围，必须先请求用户决策。
5. 阶段审查通过后，在本地实现仓库 `/home/chillchill/btc-updown-marketmaker` 创建 commit 并 push；未 commit/push 不进入下一阶段。
6. 阶段 0 额外要求用户审查通过；`PASS_WITH_LIMITS_NEEDS_USER_APPROVAL` 或 `BLOCKED_NEEDS_DECISION` 不得自动进入阶段 1。

## 五、阶段 0：技术测试与性能验证

本阶段是正式开发前的硬 gate。目标是先证明技术路径可靠，尤其是数据采集服务器和交易服务器性能/角色边界可承受，再继续推进开发。

### 任务 0.1：冻结验证范围和运行边界

**文件：**

- 创建：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [x] **步骤 0.1.1：记录阶段 0 验证矩阵**

  在 `data-collect/docs/phase0-technical-validation.md` 写入验证矩阵，至少包含：

  - Polymarket Rust SDK market channel、RTDS、market discovery、metadata 覆盖情况。
  - Polymarket `btc-updown-5m` market channel、Binance `BTCUSDT` depth、Binance `BTCUSDT` aggTrade、RTDS Chainlink `btc/usd` 四源。
  - `recv_time_utc`、source timestamp、collector host/region、raw_json、typed raw parquet、source coverage、source health、source gap、reconnect trace、manifest。
  - `2k-5k events/s` 四源合并吞吐、Polymarket p95/max 事件量、writer queue、flush latency、ResourceWatermark。
  - 数据采集服务器 `ubuntu@3.255.156.183` 与 AWS 爱尔兰 live-trade 服务器 `ubuntu@108.130.74.143` 的 hardware tier；live 服务器只允许低影响运行边界探测，不允许 Rust 编译或训练式重处理。
  - `data_collect_run_root`、`data_collect_artifact_root`、训练 UTC day 分区根、paper/live handoff artifact 根、owner/group、权限模式、路径越界 fail-closed 行为；生产根路径已由用户批准为各服务器本机 `~/btc-updown-marketmaker/datasets/`，Phase 0 receipt 必须验证该路径口径和权限边界。

- [x] **步骤 0.1.2：运行只读环境核对**

  计划执行命令：

  ```bash
  git status --short -- demand/data-collect-demand.md plan/data-collect-plan.md changelog/data-collect-changelog.md
  find data-collect -maxdepth 3 -type f -printf '%p\n' | sort
  ```

  预期：确认当前文档状态和 `data-collect/` 初始文件状态；若已有实现文件与本计划候选结构冲突，停止并记录冲突。

- [x] **步骤 0.1.3：写入阶段 0 决策门**

  在阶段 0 报告和 changelog 中记录：阶段 0 未通过前不得创建正式 collector runtime、不得部署、不得接入 paper/live、不得把未验证技术路径写成生产实现。

- [x] **步骤 0.1.4：生成运行根与 artifact 根 receipt**

  阶段 0 报告必须生成路径 receipt，字段至少包括：

  - `data_collect_run_root`
  - `data_collect_artifact_root`
  - `training_utc_day_partition_root`
  - `paper_live_handoff_artifact_root`
  - `allowed_reader_roles`
  - `allowed_writer_roles`
  - owner/group/mode
  - path canonicalization result
  - fail-closed policy
  - 用户审查 verdict

  预期：receipt 中任何路径缺失、相对路径、权限不可验证、解析到未授权根、或试图把 live 交易小主机当作构建/重处理主机时，阶段 0 verdict 为 `BLOCKED_NEEDS_DECISION`；不得再以最终生产根路径未决作为阻塞理由。

### 任务 0.2：Polymarket 官方 Rust SDK 能力核对

**文件：**

- 创建：`data-collect/docs/polymarket-sdk-gap-report.md`
- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`
- 修改：`plan/data-collect-plan.md`

- [x] **步骤 0.2.1：核对 SDK 覆盖范围**

  检查官方 Polymarket Rust SDK 是否支持：

  - market channel 订阅，且能传递官方字段 `assets_ids`、`type: "market"`、`custom_feature_enabled: true`。
  - `book`、`price_change`、`last_trade_price`、`best_bid_ask`、`tick_size_change`、`new_market`、`market_resolved` 业务事件。
  - market discovery / metadata 查询，能追溯 condition id、token id / asset id、CLOB market id、slug、start/end、active/acceptingOrders、tick size、min size、fee/reward。
  - RTDS Chainlink `btc/usd` subscription，能追溯 `action: "subscribe"`、`topic: "crypto_prices_chainlink"`、`type: "*"` 和 symbol filter。

- [x] **步骤 0.2.2：生成 SDK gap verdict**

  写入 `data-collect/docs/polymarket-sdk-gap-report.md`：

  - `PASS`：SDK 覆盖需求，可继续技术测试。
  - `BLOCKED_SDK_GAP`：SDK 不覆盖必要能力；必须列出缺口、影响范围、可选方案、风险和需要用户批准的旁路范围。
  - `NEEDS_USER_DECISION`：资料不充分或 SDK 行为不确定；不得自行切换 raw REST、manual signing、自建 SDK 或 shadow implementation。

- [x] **步骤 0.2.3：验证 SDK-first 边界**

  计划执行命令：

  ```bash
  rg -n "raw REST|manual signing|shadow implementation|自建 SDK|SDK 缺口" data-collect/docs/polymarket-sdk-gap-report.md data-collect/docs/phase0-technical-validation.md
  ```

  预期：报告明确 SDK-first 边界，并且任何缺口方案都指向用户审批。

### 任务 0.3：writer 与 artifact 发布技术测试

**文件：**

- 创建：`data-collect/benches/parquet_writer_probe.rs`
- 创建：`data-collect/tests/writer_publish_contract.rs`
- 修改：`data-collect/docs/phase0-technical-validation.md`

- [x] **步骤 0.3.1：实现最小 writer probe**

  只实现测试用 probe，不接生产 source。probe 输入合成事件，字段覆盖：

  - `source_kind`
  - `source_event_type`
  - `source_identifier`
  - `source_ts`
  - `recv_time_utc`
  - `collector_host_id`
  - `collector_region`
  - `raw_json`
  - `parquet_raw_layout_version`
  - `parquet_raw_source_kind`

- [x] **步骤 0.3.2：验证 running/temp segment**

  计划执行命令：

  ```bash
  cargo test -p data-collect writer_running_file_is_not_discoverable -- --nocapture
  cargo test -p data-collect writer_finalize_atomic_rename_publishes_only_finalized -- --nocapture
  ```

  预期：`*.tmp`、`*-running.parquet`、`.rsync-partial` 不进入 discovery；close/finalize 后通过 atomic rename 发布 finalized `.parquet`。

- [x] **步骤 0.3.3：验证 manifest 完整性**

  计划执行命令：

  ```bash
  cargo test -p data-collect manifest_requires_row_count_sha256_schema_source_coverage -- --nocapture
  ```

  预期：manifest 包含 artifact path、row count、sha256、schema version、code version / git commit、collector binary sha256、source config digest、validation verdict、source coverage、source gap、ResourceWatermark、writer queue 高水位、flush latency、collector host/region 和 `required_files[]`；缺任一关键字段时 verdict 为 incomplete。

### 任务 0.4：吞吐、队列与 backpressure 技术测试

**文件：**

- 创建：`data-collect/benches/throughput_probe.rs`
- 创建：`data-collect/tests/backpressure_contract.rs`
- 修改：`data-collect/docs/phase0-technical-validation.md`

- [x] **步骤 0.4.1：建立吞吐基线 probe**

  使用合成四源事件模拟：

  - Polymarket p95 约 302k events / 5.5 分钟窗口。
  - Polymarket max 约 433k events / 5.5 分钟窗口。
  - 四源合并 `2k-5k events/s` 解析、排队、写入和 health 统计。

- [x] **步骤 0.4.2：验证 bounded queue 和 backpressure**

  计划执行命令：

  ```bash
  cargo test -p data-collect bounded_queue_records_backpressure_observation_without_source_gap -- --nocapture
  cargo test -p data-collect queue_watermarks_include_rows_bytes_age_writer_lag -- --nocapture
  cargo bench -p data-collect --bench throughput_probe -- --sample-size 20
  ```

  预期：所有跨任务队列都是 bounded queue；row count、byte count、oldest event age、writer lag 和 memory watermark 越线时，先按 soft/hard threshold、连续采样窗口和 hysteresis 写入 backpressure、source health 降级或 source gap；只有持续 hard threshold 且无法通过 backpressure 恢复时，才受控暂停新市场或单源 intake，并保护已采 segment；无 unbounded queue、silent loss 或短暂尖峰导致的进程级频繁失败。

- [x] **步骤 0.4.3：记录通过/失败阈值**

  阶段 0 报告必须记录：

  - events/s p50/p95/max。
  - writer queue row/byte 高水位。
  - queue age 和 writer lag。
  - flush latency p50/p95/max。
  - finalize latency 和失败次数。
  - CPU/RSS/disk/resource watermark。
  - 未达到 `2k-5k events/s` 时的瓶颈位置和选项。

### 任务 0.5：网络异常与 freshness 技术测试

**文件：**

- 创建：`data-collect/tests/network_recovery_contract.rs`
- 修改：`data-collect/docs/phase0-technical-validation.md`

- [x] **步骤 0.5.1：覆盖异常场景**

  测试必须覆盖：

  - 连接失败。
  - 断链或异常 close。
  - server shutdown。
  - 订阅返回错误、HTTP/WS 返回错误或协议错误。
  - 返回空 payload、空数组或空 snapshot。
  - 长时间无业务消息。
  - ping/pong 正常但业务数据静默。
  - rate limit、连接数限制、订阅数限制或 throttling。
  - snapshot 缺失、snapshot 过旧、update id 缺口。
  - source stale、source gap、source coverage drop。

- [x] **步骤 0.5.2：验证恢复策略**

  计划执行命令：

  ```bash
  cargo test -p data-collect retry_uses_bounded_backoff_jitter_and_records_trace -- --nocapture
  cargo test -p data-collect source_circuit_breakers_are_independent -- --nocapture
  cargo test -p data-collect pong_without_business_data_marks_stale_not_healthy -- --nocapture
  cargo test -p data-collect reconnect_requires_subscription_snapshot_and_fresh_business_data -- --nocapture
  ```

  预期：有限重试、指数 backoff + jitter、max attempts、max elapsed、backoff ceiling 和 retry exhausted 都有记录；每个 source 独立 open、half-open、recovered、exhausted；连接 open 或 PONG 正常不能单独恢复为 healthy。

### 任务 0.6：服务器资源与部署角色验证

**文件：**

- 创建：`data-collect/docs/hardware-tier-validation.md`
- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [x] **步骤 0.6.1：数据采集服务器低影响资源核对**

  计划执行只读/低影响命令：

  ```bash
  ssh ubuntu@3.255.156.183 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
  ```

  预期：记录 data collection server hardware tier、CPU、内存、磁盘、open files、PSI 可用性；不得启动生产采集或重处理。

- [x] **步骤 0.6.2：live 交易服务器只做运行边界核对**

  计划执行只读命令：

  ```bash
  ssh ubuntu@108.130.74.143 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
  ```

  预期：记录 live trade server hardware tier；确认该主机不运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`、训练式重处理、历史全量 parquet 扫描或归档压缩。

- [x] **步骤 0.6.3：验证 ResourceWatermark schema**

  ResourceWatermark 技术测试必须覆盖：

  - RSS、CPU、disk free。
  - artifact dir bytes、journal/log bytes、journal 限额状态。
  - open files、queue bytes、queue age。
  - parquet writer memory estimate。
  - cgroup `memory.current`、`memory.stat`、`memory.events`。
  - PSI、anon/file/slab、stage latency / TTL。
  - 指标不可用时的 unavailable reason。

  计划执行命令：

	  ```bash
	  cargo test -p data-collect resource_watermark_records_required_metrics_or_unavailable_reason -- --nocapture
	  cargo test -p data-collect stateless_hard_threshold_records_observation_without_stop_action -- --nocapture
	  cargo test -p data-collect resource_hard_threshold_requires_consecutive_samples_before_stop_action -- --nocapture
	  cargo test -p data-collect soft_threshold_records_degraded_without_process_fatal -- --nocapture
	  ```

  预期：soft threshold 只记录降级、backpressure 和恢复观察；连续 hard threshold 越线时记录停止新市场采集、保护已采数据、优先 finalize/sha256/manifest 的动作与结果；不得因短暂 resource spike 产生进程级频繁失败。

- [x] **步骤 0.6.4：清理后生成数据采集服务器测试根 receipt**

  清理磁盘只是解除远端验证阻塞，不等于 Phase 0 通过。执行已获用户授权的清理动作前，必须先记录待终止进程和待删除路径；执行后必须记录清理前后 receipt。

  计划执行命令：

  ```bash
  ssh ubuntu@3.255.156.183 'set -eu
  date -u
  hostname
  ps -eo pid,ppid,etime,cmd | grep -Ei "data-collect|collector|polymarket|binance|rtds|market" | grep -v grep || true
  df -h /
  du -sh ~/btc-updown-marketmaker 2>/dev/null || true
  find ~/btc-updown-marketmaker -maxdepth 4 -type d \( -name "*phase0*" -o -name "*collect*" -o -name "*artifact*" -o -name "*parquet*" \) -print 2>/dev/null | sort
  '
  ```

  清理规则：

  - 不使用裸 `rm -rf /`、不使用未展开的宽泛 glob、不删除 `~/wallets`、不删除 `/home/ubuntu/live-trade`。
  - 每个待删除绝对路径必须先写入 `changelog/data-collect-changelog.md` 或远端 receipt，再执行删除。
  - 只终止确认属于旧数据采集程序的进程；不能按宽泛关键词误杀无关系统进程。
  - 清理后创建临时测试根，推荐 `~/btc-updown-marketmaker/data-collect/phase0-runs`；该路径不是最终生产数据根。

  预期：`data-collect/docs/phase0-technical-validation.md` 记录清理前后 `df -h`、进程处理、删除路径、测试根 canonical path、owner/group/mode、可用空间和 `BLOCKED` / `READY_FOR_REMOTE_PHASE0` verdict。

  执行结果：已记录清理前后 receipt；旧 `poly-tx_v1.1.5` 采集进程已终止，旧目录 `/home/ubuntu/ron/collect-orderbook` 已删除；根盘从 `93%` 使用降至 `6%` 使用；Phase 0 临时测试根为 `~/btc-updown-marketmaker/data-collect/phase0-runs`。

- [x] **步骤 0.6.5：本地构建并上传远端 probe，不在服务器编译 Rust**

  数据采集服务器硬件 tier 只有约 `2 vCPU / 1.9GiB RAM / no swap`。远端 Phase 0 验证测的是采集运行路径，不把 Rust 编译耗时和内存计入采集能力；因此 probe binary 必须在本地构建后上传。

  计划执行命令：

  ```bash
  cd /home/chillchill/btc-updown-marketmaker
  TARGET_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-target.XXXXXX)"
  UPLOAD_DIR="$(mktemp -d /tmp/data-collect-phase0-upload.XXXXXX)"
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p data-collect --release --benches
  WRITER_BIN="$(find "$TARGET_DIR/release/deps" -maxdepth 1 -type f -perm -111 -name "parquet_writer_probe-*" | sort | tail -n 1)"
  THROUGHPUT_BIN="$(find "$TARGET_DIR/release/deps" -maxdepth 1 -type f -perm -111 -name "throughput_probe-*" | sort | tail -n 1)"
  test -n "$WRITER_BIN"
  test -n "$THROUGHPUT_BIN"
  install -m 0755 "$WRITER_BIN" "$UPLOAD_DIR/parquet_writer_probe"
  install -m 0755 "$THROUGHPUT_BIN" "$UPLOAD_DIR/throughput_probe"
  sha256sum "$UPLOAD_DIR/parquet_writer_probe" "$UPLOAD_DIR/throughput_probe"
  ssh ubuntu@3.255.156.183 'mkdir -p ~/btc-updown-marketmaker/data-collect/phase0-runs/bin ~/btc-updown-marketmaker/data-collect/phase0-runs/logs ~/btc-updown-marketmaker/data-collect/phase0-runs/tmp'
  rsync -av "$UPLOAD_DIR/parquet_writer_probe" "$UPLOAD_DIR/throughput_probe" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  ssh ubuntu@3.255.156.183 'chmod 0755 ~/btc-updown-marketmaker/data-collect/phase0-runs/bin/parquet_writer_probe ~/btc-updown-marketmaker/data-collect/phase0-runs/bin/throughput_probe && sha256sum ~/btc-updown-marketmaker/data-collect/phase0-runs/bin/parquet_writer_probe ~/btc-updown-marketmaker/data-collect/phase0-runs/bin/throughput_probe'
  ```

  预期：本地和远端 sha256 一致；远端不运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt` 或 `cargo bench`；changelog 记录 commit/source snapshot、binary sha256、上传路径和目标主机。

  执行结果：`parquet_writer_probe` sha256 为 `629d2d221819206db25251e26e22e50d8c442311b272efd148b5068365e9687c`；`throughput_probe` sha256 为 `ab17ffd3b609db03d5ec5f061643fbcca89f1300e3f7382f0600665c8c49c105`；本地和远端 sha256 一致，远端未运行 cargo 编译命令。

- [x] **步骤 0.6.6：远端 5-10 分钟 smoke test**

  smoke test 只验证远端路径、权限、DNS/TLS、低风险 endpoint 连通、writer publish 和短时资源采样，不接 paper/live，不写最终生产数据根。

  计划执行命令：

  ```bash
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  mkdir -p "$ROOT/logs" "$ROOT/tmp"
  date -u | tee "$ROOT/logs/smoke-start.txt"

  probe_http() {
    name="$1"
    url="$2"
    body="$ROOT/logs/$name.body"
    meta="$ROOT/logs/$name.meta"
    err="$ROOT/logs/$name.err"
    set +e
    curl -sS --max-time 10 -o "$body" -w "http_code=%{http_code}\nsize_download=%{size_download}\ntime_total=%{time_total}\nremote_ip=%{remote_ip}\n" "$url" > "$meta" 2> "$err"
    code=$?
    set -e
    printf "curl_exit=%s\n" "$code" >> "$meta"
    if [ ! -s "$body" ]; then
      printf "empty_response=true\n" >> "$meta"
    else
      printf "empty_response=false\n" >> "$meta"
    fi
  }

  probe_http "binance-depth-smoke" "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5"
  probe_http "binance-aggtrade-smoke" "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&limit=1"
  probe_http "polymarket-gamma-smoke" "https://gamma-api.polymarket.com/markets?limit=1"
  probe_http "polymarket-clob-smoke" "https://clob.polymarket.com/markets?next_cursor="

  WRITER="$ROOT/bin/parquet_writer_probe"
  test -x "$WRITER"
  END=$((SECONDS + 480))
  ITER=0
  while [ "$SECONDS" -lt "$END" ]; do
    ITER=$((ITER + 1))
    timeout 30s env TMPDIR="$ROOT/tmp" "$WRITER" | tee "$ROOT/logs/writer-smoke-$ITER.log"
    df -h / "$ROOT" | tee "$ROOT/logs/smoke-df-$ITER.txt"
    free -h | tee "$ROOT/logs/smoke-free-$ITER.txt"
    sleep 30
  done
  df -h / "$ROOT" | tee "$ROOT/logs/smoke-df-after.txt"
  free -h | tee "$ROOT/logs/smoke-free-after.txt"
  date -u | tee "$ROOT/logs/smoke-end.txt"
  REMOTE
  ```

  预期：writer smoke 产生 finalized parquet 与 sha256；Binance 与 Polymarket HTTP reachability 成功则记录响应摘要，失败则记录 endpoint、exit code 和错误类别，不把网络失败伪装为 source stale 或 collector 成功；5-10 分钟内无 `ProcessFatal`、无路径越界、无 partial finalized artifact。

  执行结果：8 分钟 smoke 完成；Binance depth/aggTrade、Polymarket Gamma/CLOB 均 `http_code=200` 且非空；writer smoke 16 轮，每轮 `rows=10000`，sha256 为 `e824d0e63506fbcefaba6721902a1828822269fe625a06247cfdd89fc5ac47e1`。

- [x] **步骤 0.6.7：远端 30-60 分钟 synthetic throughput / ResourceWatermark 验收跑**

  默认执行 `45m`；如果资源曲线边缘或 source/network smoke 不稳定，可改为 `60m`。该验收跑只用于容量预检，并决定是否继续执行 0.6.8-0.6.10；低于 `30m` 只能算 smoke，不得作为 Phase 0 远端验收通过证据。

  计划执行命令：

  ```bash
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  RUN_ID="$(date -u +phase0-remote-%Y%m%dT%H%M%SZ)"
  RUN_DIR="$ROOT/$RUN_ID"
  mkdir -p "$RUN_DIR/tmp" "$RUN_DIR/logs"
  PROBE="$ROOT/bin/throughput_probe"
  test -x "$PROBE"
  (
    while true; do
      date -u
      df -h / "$RUN_DIR"
      free -h
      ps -eo pid,ppid,%cpu,%mem,rss,vsz,etime,cmd | grep -E "throughput_probe|parquet_writer_probe" | grep -v grep || true
      cat /proc/pressure/cpu 2>/dev/null || true
      cat /proc/pressure/memory 2>/dev/null || true
      cat /proc/pressure/io 2>/dev/null || true
      sleep 30
    done
  ) > "$RUN_DIR/logs/resource-sampler.log" 2>&1 &
  SAMPLER_PID=$!
  trap "kill $SAMPLER_PID 2>/dev/null || true" EXIT
  END=$((SECONDS + 2700))
  ITER=0
  while [ "$SECONDS" -lt "$END" ]; do
    ITER=$((ITER + 1))
    timeout 120s env TMPDIR="$RUN_DIR/tmp" "$PROBE" | tee "$RUN_DIR/logs/throughput-$ITER.log"
    sleep 5
  done
  kill "$SAMPLER_PID" 2>/dev/null || true
  find "$RUN_DIR" -maxdepth 4 -type f -printf "%p\t%s\n" | sort > "$RUN_DIR/logs/file-list.tsv"
  sha256sum "$RUN_DIR"/logs/* > "$RUN_DIR/logs/logs.sha256"
  df -h / "$RUN_DIR" > "$RUN_DIR/logs/final-df.txt"
  REMOTE
  ```

  通过条件：

  - 每轮 synthetic probe 的 `events_per_sec_total` 稳定高于 `2k-5k events/s` 工程基线，且任何低于基线的轮次都有可解释瓶颈。
  - `finalize_failures=0`；不存在 finalized discovery 可见的 partial artifact。
  - writer queue、flush latency、finalize latency、RSS、CPU、disk free、PSI 和 open files 证据可解释；没有无法恢复的 hard threshold。
  - 普通 source gap / stale / backpressure 只作为质量信号记录，不触发进程级频繁失败。
  - 验收结束后 artifact/log 总量、剩余磁盘和 cleanup 需求写入 Phase 0 报告。

  执行结果：45 分钟验收完成，run dir 为 `~/btc-updown-marketmaker/data-collect/phase0-runs/phase0-remote-20260705T225836Z`；共 415 轮，两个 synthetic case 全程高于 `2k-5k events/s` 工程基线，`finalize_failures=0`，PSI 无压力，未触发 2-4 小时加强 soak 条件。

- [x] **步骤 0.6.8：远端真实网络 reachability smoke 与可选 2-4 小时加强 soak**

  真实网络 smoke 只验证数据采集服务器到官方 endpoint 的连通性与错误分类，不提前实现生产 source adapter；生产级 Polymarket/Binance/RTDS source adapter 仍在后续阶段按 SDK-first 和官方协议实现。

  必须记录：

  - Binance REST depth / aggTrade reachability。
  - Polymarket Gamma/CLOB metadata reachability。
  - Polymarket market channel / RTDS WebSocket 若已有 SDK probe 或成熟 WebSocket 工具可用，则记录 handshake / subscribe / close 证据；若工具不可用，记录 `TOOL_UNAVAILABLE`，不得把未测项写成通过。
  - DNS/TLS/connect timeout、HTTP status、rate limit、empty response、server shutdown、source stale 的分类。

  计划执行命令：

  ```bash
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  RUN_ID="$(date -u +phase0-network-%Y%m%dT%H%M%SZ)"
  RUN_DIR="$ROOT/$RUN_ID"
  mkdir -p "$RUN_DIR/logs"

  probe_http() {
    name="$1"
    url="$2"
    body="$RUN_DIR/logs/$name.body"
    meta="$RUN_DIR/logs/$name.meta"
    err="$RUN_DIR/logs/$name.err"
    set +e
    curl -sS --max-time 15 -o "$body" -w "http_code=%{http_code}\nsize_download=%{size_download}\ntime_total=%{time_total}\nremote_ip=%{remote_ip}\n" "$url" > "$meta" 2> "$err"
    code=$?
    set -e
    printf "curl_exit=%s\n" "$code" >> "$meta"
    if [ ! -s "$body" ]; then
      printf "empty_response=true\n" >> "$meta"
    else
      printf "empty_response=false\n" >> "$meta"
    fi
  }

  probe_http "binance-depth" "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5"
  probe_http "binance-aggtrade" "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&limit=1"
  probe_http "polymarket-gamma" "https://gamma-api.polymarket.com/markets?limit=1"
  probe_http "polymarket-clob" "https://clob.polymarket.com/markets?next_cursor="

  WS_TOOL="$(command -v websocat || command -v wscat || true)"
  if [ -z "$WS_TOOL" ]; then
    printf "TOOL_UNAVAILABLE websocket_client\n" | tee "$RUN_DIR/logs/websocket-tool-unavailable.txt"
  else
    printf "ws_tool=%s\n" "$WS_TOOL" | tee "$RUN_DIR/logs/websocket-tool.txt"
    set +e
    if [ "$(basename "$WS_TOOL")" = "websocat" ]; then
      timeout 20s "$WS_TOOL" "wss://ws-subscriptions-clob.polymarket.com/ws/market" > "$RUN_DIR/logs/polymarket-market-ws.out" 2> "$RUN_DIR/logs/polymarket-market-ws.err"
      printf "polymarket_market_ws_exit=%s\n" "$?" > "$RUN_DIR/logs/polymarket-market-ws.meta"
      printf '{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}\n' | timeout 20s "$WS_TOOL" "wss://ws-live-data.polymarket.com" > "$RUN_DIR/logs/polymarket-rtds-ws.out" 2> "$RUN_DIR/logs/polymarket-rtds-ws.err"
      printf "polymarket_rtds_ws_exit=%s\n" "$?" > "$RUN_DIR/logs/polymarket-rtds-ws.meta"
    else
      timeout 20s "$WS_TOOL" -c "wss://ws-subscriptions-clob.polymarket.com/ws/market" > "$RUN_DIR/logs/polymarket-market-ws.out" 2> "$RUN_DIR/logs/polymarket-market-ws.err"
      printf "polymarket_market_ws_exit=%s\n" "$?" > "$RUN_DIR/logs/polymarket-market-ws.meta"
      timeout 20s "$WS_TOOL" -c "wss://ws-live-data.polymarket.com" -x '{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}' > "$RUN_DIR/logs/polymarket-rtds-ws.out" 2> "$RUN_DIR/logs/polymarket-rtds-ws.err"
      printf "polymarket_rtds_ws_exit=%s\n" "$?" > "$RUN_DIR/logs/polymarket-rtds-ws.meta"
    fi
    set -e
  fi

  find "$RUN_DIR" -maxdepth 3 -type f -printf "%p\t%s\n" | sort > "$RUN_DIR/logs/file-list.tsv"
  sha256sum "$RUN_DIR"/logs/* > "$RUN_DIR/logs/logs.sha256"
  REMOTE
  ```

  加强 soak 触发条件：

  - 30-60 分钟验收跑中 RSS 持续增长、磁盘下降不可解释、PSI 长时间偏高、writer latency 持续恶化、reconnect/source gap 频繁，或任何指标接近 hard threshold。
  - 触发后执行 `2h` 起步、最长 `4h` 的同类远端 soak；未触发时不强制长跑，避免把 Phase 0 变成过度测试。

  加强 soak 命令模板：

  ```bash
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  RUN_ID="$(date -u +phase0-soak-%Y%m%dT%H%M%SZ)"
  RUN_DIR="$ROOT/$RUN_ID"
  SOAK_SECONDS="${SOAK_SECONDS:-7200}"
  if [ "$SOAK_SECONDS" -lt 7200 ]; then
    SOAK_SECONDS=7200
  fi
  if [ "$SOAK_SECONDS" -gt 14400 ]; then
    SOAK_SECONDS=14400
  fi
  mkdir -p "$RUN_DIR/tmp" "$RUN_DIR/logs"
  PROBE="$ROOT/bin/throughput_probe"
  test -x "$PROBE"
  (
    while true; do
      date -u
      df -h / "$RUN_DIR"
      free -h
      ps -eo pid,ppid,%cpu,%mem,rss,vsz,etime,cmd | grep -E "throughput_probe|parquet_writer_probe" | grep -v grep || true
      cat /proc/pressure/cpu 2>/dev/null || true
      cat /proc/pressure/memory 2>/dev/null || true
      cat /proc/pressure/io 2>/dev/null || true
      sleep 30
    done
  ) > "$RUN_DIR/logs/resource-sampler.log" 2>&1 &
  SAMPLER_PID=$!
  trap "kill $SAMPLER_PID 2>/dev/null || true" EXIT
  END=$((SECONDS + SOAK_SECONDS))
  ITER=0
  while [ "$SECONDS" -lt "$END" ]; do
    ITER=$((ITER + 1))
    timeout 120s env TMPDIR="$RUN_DIR/tmp" "$PROBE" | tee "$RUN_DIR/logs/throughput-$ITER.log"
    sleep 5
  done
  kill "$SAMPLER_PID" 2>/dev/null || true
  find "$RUN_DIR" -maxdepth 4 -type f -printf "%p\t%s\n" | sort > "$RUN_DIR/logs/file-list.tsv"
  sha256sum "$RUN_DIR"/logs/* > "$RUN_DIR/logs/logs.sha256"
  df -h / "$RUN_DIR" > "$RUN_DIR/logs/final-df.txt"
  REMOTE
  ```

	  预期：真实网络 smoke 的失败会形成 `SourceDegraded` / `PhaseGateBlocked` 证据和用户决策项，而不是静默通过；该步骤只证明 endpoint reachability，不证明 `btc-updown-5m` active market discovery、market rollover 或四源时间线对齐；阶段 0 还必须通过 0.6.9 和 0.6.10 才能进入用户审查 gate。

	  执行结果：HTTP reachability 完成，Binance depth/aggTrade、Polymarket Gamma/CLOB 均 `http_code=200` 且 `empty_response=false`。第一次命令行 WebSocket smoke 因无 `websocat`/`wscat` 记录 `TOOL_UNAVAILABLE websocket_client`；随后按用户批准补跑官方 Polymarket Rust SDK-first `websocket_smoke_probe`，market channel 与 RTDS 均 `Connected`、`subscription_count=1`，且各自收到业务消息。后续审查确认该 smoke 使用的是 Gamma 返回的 generic open market token，并不证明 `btc-updown-5m` active market discovery 或 rollover；该结果只能作为 reachability evidence，不能支撑阶段 0 通过。

- [x] **步骤 0.6.9：远端真实 `btc-updown-5m` 冷启动 market discovery 验证**

  本步骤必须实现或运行最小 SDK-first discovery probe，验证从空状态冷启动时能找到当前 `btc-updown-5m` active market 和 planned next market。该 probe 不是生产 collector，但必须使用与后续 source adapter 一致的 discovery 语义；不得 fallback 到 generic BTC/Bitcoin open market。

  **文件：**

  - 创建或修改：`data-collect/benches/market_discovery_probe.rs`
  - 修改：`data-collect/Cargo.toml`，必须新增 `[[bench]] name = "market_discovery_probe"` 且 `harness = false`
  - 更新：`data-collect/docs/phase0-technical-validation.md`
  - 更新：`changelog/data-collect-changelog.md`

  通过条件：

  - discovery target 明确为 Polymarket `btc-updown-5m`，记录 series/slug/prefix 匹配规则、query time、collector host/region 和 clock sync status；匹配规则必须同时证明 BTC、up/down 和 5 minute cadence，generic `btc` / `bitcoin` slug 或任意 open CLOB market 只能进入 rejected candidate。
  - discovery receipt 必须包含 `discovery_run_id`、endpoint/query params 或 source config digest、metadata `raw_json` 或 digest、`fetch_start_utc`、`recv_time_utc`、snapshot freshness、current 与 next 的 same condition/market proof 或明确 unavailable reason。
  - 当前 active market 必须有 condition id、CLOB market id 或可追溯 market id、token/asset id 集合、slug、start/end、active、acceptingOrders、tick/min size、fee/reward 原始字段或 unavailable reason。
  - next market 必须有 planned start、preload start `T-30s`、prefetched 状态和完整 token/asset id 证据；如果 next market 尚未发布，必须写 source gap / discovery miss，不得把任意 BTC market 当作 next。
  - slug-only discovery 只能进入 candidate，不得作为完整 discovery 成功；metadata snapshot 或 active scan 必须补齐同一 condition/market 的 id 与生命周期证据。
  - 过期 target 或 discovery miss 必须记录 miss rate、过期原因和按当前时间重算的 target；不得无限卡住同一 market。

  计划执行命令：

  ```bash
  set -euo pipefail
  cd /home/chillchill/btc-updown-marketmaker
  TARGET_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-target.XXXXXX)"
  UPLOAD_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-upload.XXXXXX)"
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p data-collect --release --bench market_discovery_probe
  DISCOVERY_BIN="$(find "$TARGET_DIR/release/deps" -maxdepth 1 -type f -perm -111 -name "market_discovery_probe-*" | sort | tail -n 1)"
  test -n "$DISCOVERY_BIN"
  install -m 0755 "$DISCOVERY_BIN" "$UPLOAD_DIR/market_discovery_probe"
  sha256sum "$UPLOAD_DIR/market_discovery_probe" > "$UPLOAD_DIR/market_discovery_probe.sha256"
  ssh ubuntu@3.255.156.183 'mkdir -p ~/btc-updown-marketmaker/data-collect/phase0-runs/bin'
  rsync -av "$UPLOAD_DIR/market_discovery_probe" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  rsync -av "$UPLOAD_DIR/market_discovery_probe.sha256" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  RUN_ID="$(date -u +phase0-market-discovery-%Y%m%dT%H%M%SZ)"
  RUN_DIR="$ROOT/$RUN_ID"
  mkdir -p "$ROOT/bin" "$RUN_DIR/logs" "$RUN_DIR/artifacts" "$RUN_DIR/tmp"
  PROBE="$ROOT/bin/market_discovery_probe"
  test -x "$PROBE"
  sha256sum "$PROBE" | tee "$RUN_DIR/logs/binary.sha256"
  set +e
  timeout 120s "$PROBE" --artifact-dir "$RUN_DIR/artifacts" 2>&1 | tee "$RUN_DIR/logs/market-discovery-probe.log"
  probe_exit=${PIPESTATUS[0]}
  set -e
  echo "probe_exit=$probe_exit" | tee "$RUN_DIR/logs/probe-exit.txt"
  find "$RUN_DIR" -maxdepth 5 -type f -printf "%p\t%s\n" | sort | tee "$RUN_DIR/logs/file-list.tsv"
  find "$RUN_DIR" -type f ! -path "$RUN_DIR/logs/file-sha256.tsv" ! -path "$RUN_DIR/logs/file-sha256.tsv.tmp" -exec sha256sum {} + | sort > "$RUN_DIR/logs/file-sha256.tsv.tmp"
  mv "$RUN_DIR/logs/file-sha256.tsv.tmp" "$RUN_DIR/logs/file-sha256.tsv"
  exit "$probe_exit"
  REMOTE
  ```

  预期：输出 cold-start discovery receipt。缺少 current active、next prefetched、token/asset id、condition/market id、start/end 或 active/acceptingOrders 任一关键证据时，阶段 0 verdict 仍为 `BLOCKED_NEEDS_DECISION`。

  执行结果：已完成。远端 run dir 为 `/home/ubuntu/btc-updown-marketmaker/data-collect/phase0-runs/phase0-market-discovery-20260706T011058Z`，`probe_exit=0`，receipt verdict 为 `PASS`。当前 market 为 `btc-updown-5m-1783300200`，next market 为 `btc-updown-5m-1783300500`，均有 condition id、Up/Down asset ids、start/end、active/acceptingOrders 和 source_gaps `[]` 证据；binary sha256 为 `3240d55af3000564805f39548008820c41e9847d385a0ba4942a4680794ba5d6`。

- [x] **步骤 0.6.10：远端真实四源长时间采集、rollover 和时间线对齐验收**

  本步骤必须在数据采集服务器运行真实四源 probe，至少覆盖冷启动后的一个完整 `btc-updown-5m` market、下一 market 的 `T-30s` prefetched window，以及至少一个 active market rollover。该 probe 仍是最小验证 probe，不是生产 collector；必须走 async source -> bounded queue -> writer/publish -> manifest/coverage/resource 证据链，但不得在 Phase 0 中实现超出证据链所需的生产 collector 功能。

  **文件：**

  - 创建或修改：`data-collect/benches/real_source_alignment_probe.rs`
  - 修改：`data-collect/Cargo.toml`，必须新增 `[[bench]] name = "real_source_alignment_probe"` 且 `harness = false`；如现有 SDK/依赖不足以接入 Binance HTTP/WS 或 RTDS，优先选择成熟 Rust HTTP/WebSocket crate，并在 changelog 记录取舍，不得引入 Polymarket SDK 旁路。
  - 更新：`data-collect/docs/phase0-technical-validation.md`
  - 更新：`changelog/data-collect-changelog.md`

  必须同时接入：

  - Polymarket `btc-updown-5m` market channel：current active + prefetched next，记录 business event、真实发出的 subscription JSON/digest 或 SDK request evidence、`custom_feature_enabled: true`、real current/prefetched asset ids、source timestamp、`recv_time_utc`、rollover 边界；若官方 SDK 不暴露 actual outbound frame receipt，必须写明 unavailable reason，并在正式 adapter 阶段补齐 request evidence。
  - Binance `BTCUSDT` depth：官方 WS depth + REST snapshot stitch，记录 `lastUpdateId`、`U/u` 连续性、resnapshot、stale/duplicate/gap。
  - Binance `BTCUSDT` aggTrade：官方 WS aggTrade，记录 event time、trade id、source stale/gap。
  - RTDS Chainlink `btc/usd`：记录 RTDS message timestamp、payload timestamp、symbol、value、source topic、`recv_time_utc`、symbol filter、business freshness 和 stale/gap。

  最小 source loop / output schema：

  - Polymarket 只能通过官方 Rust SDK-first market channel / RTDS 能力接入；如 SDK 不能暴露所需订阅帧或 raw evidence，先记录缺口并请求用户决策。
  - Binance depth 使用官方 WS stream 与官方 REST depth snapshot stitch；aggTrade 使用官方 WS stream；endpoint、stream name、snapshot fetch/recv time、update/trade id 必须写入 artifact。
  - 四源事件必须进入同一 bounded queue，输出共用 envelope 字段、逐源 coverage JSON、source health/gap/reconnect trace、ResourceWatermark JSON、finalized parquet、manifest 和 file sha256 list。

  长跑通过条件：

  - 运行 `45-60` 分钟，且至少覆盖 `6` 个连续 `btc-updown-5m` market window；如果启动时间导致不足 `6` 个 window，必须延长到满足 window 数或记录阻塞原因。
  - 至少观测一个 `current -> retired` 与 `prefetched -> active` rollover；若 Polymarket 未发布 next market 或 metadata/source stale 导致无法验证，阶段 0 不能通过，只能记录 source gap / decision item。
  - 四源在每个 5 分钟 UTC window 内都有 coverage verdict：healthy、degraded with explicit source gap、或 blocked with reason；缺失源不得被其他源健康状态掩盖。
  - 记录逐源 events/s、四源合并 events/s、writer queue row/byte/age 高水位、flush latency p50/p95/max、finalize latency、RSS/CPU/disk/PSI/open files、artifact dir bytes 和 source gap/stale/reconnect trace。
  - Polymarket 使用真实 `btc-updown-5m` active/prefetched markets 的事件量验证；synthetic p95/max 只能作为对照，不得替代真实吞吐和资源验收。
  - 验收结束后 finalized parquet、manifest、coverage、ResourceWatermark、source gap/reconnect trace 均通过 atomic publish 证据；没有 finalized partial artifact 或 unexplained data loss。

  计划执行命令：

  ```bash
  set -euo pipefail
  cd /home/chillchill/btc-updown-marketmaker
  TARGET_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-target.XXXXXX)"
  UPLOAD_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-upload.XXXXXX)"
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p data-collect --release --bench real_source_alignment_probe
  REAL_SOURCE_BIN="$(find "$TARGET_DIR/release/deps" -maxdepth 1 -type f -perm -111 -name "real_source_alignment_probe-*" | sort | tail -n 1)"
  test -n "$REAL_SOURCE_BIN"
  install -m 0755 "$REAL_SOURCE_BIN" "$UPLOAD_DIR/real_source_alignment_probe"
  (cd "$UPLOAD_DIR" && sha256sum real_source_alignment_probe > real_source_alignment_probe.sha256)
  ssh ubuntu@3.255.156.183 'mkdir -p ~/btc-updown-marketmaker/data-collect/phase0-runs/bin'
  rsync -av "$UPLOAD_DIR/real_source_alignment_probe" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  rsync -av "$UPLOAD_DIR/real_source_alignment_probe.sha256" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  BIN_ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  DATASET_ROOT="$(realpath -m "$HOME/btc-updown-marketmaker/datasets")"
  case "$DATASET_ROOT" in
    "$HOME/btc-updown-marketmaker/datasets"|"$HOME/btc-updown-marketmaker/datasets/"*) ;;
    *) echo "dataset root escapes approved root: $DATASET_ROOT" >&2; exit 2 ;;
  esac
  RUN_ID="$(date -u +phase0-real-source-%Y%m%dT%H%M%SZ)"
  RUN_DIR="$DATASET_ROOT/phase0-validation/$RUN_ID"
  ARTIFACT_DIR="$RUN_DIR/artifacts"
  mkdir -p "$BIN_ROOT/bin" "$RUN_DIR/logs" "$ARTIFACT_DIR" "$RUN_DIR/tmp"
  PROBE="$BIN_ROOT/bin/real_source_alignment_probe"
  test -x "$PROBE"
  sha256sum "$PROBE" | tee "$RUN_DIR/logs/binary.sha256"
  {
    echo "run_id=$RUN_ID"
    echo "dataset_root=$DATASET_ROOT"
    echo "run_dir=$RUN_DIR"
    echo "artifact_dir=$ARTIFACT_DIR"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } | tee "$RUN_DIR/logs/run-receipt.txt"
  set +e
  REAL_SOURCE_RUN_SECS=3600 REAL_SOURCE_ARTIFACT_DIR="$ARTIFACT_DIR" timeout 3900s "$PROBE" 2>&1 | tee "$RUN_DIR/logs/real-source-alignment-probe.log"
  probe_exit=${PIPESTATUS[0]}
  set -e
  echo "probe_exit=$probe_exit" | tee "$RUN_DIR/logs/probe-exit.txt"
  find "$RUN_DIR" -maxdepth 6 -type f -printf "%p\t%s\n" | sort | tee "$RUN_DIR/logs/file-list.tsv"
  find "$RUN_DIR" -type f ! -path "$RUN_DIR/logs/file-sha256.tsv" ! -path "$RUN_DIR/logs/file-sha256.tsv.tmp" -exec sha256sum {} + | sort > "$RUN_DIR/logs/file-sha256.tsv.tmp"
  mv "$RUN_DIR/logs/file-sha256.tsv.tmp" "$RUN_DIR/logs/file-sha256.tsv"
  exit "$probe_exit"
  REMOTE
  ```

  预期：只有真实 `btc-updown-5m` discovery、rollover、四源 coverage 对齐、writer/resource 证据同时满足时，远端 Phase 0 才能重新进入 `PASS_TO_PHASE1_REVIEW`。

  执行结果：已完成。第一轮 60 分钟真实长跑 `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T011412Z` 采集 `180161` 行并覆盖 14 个 Polymarket window，但因 aggTrade 使用 `5s` business freshness timeout 产生 5 次 `binance_aggtrade_business_message_timeout`，verdict 为 `SOURCE_GAP`；复核判定该阈值不符合“长时间无业务消息”语义，属于过度校验。已将 Binance depth freshness 调整为 `30s`、aggTrade freshness 调整为 `60s`，RTDS freshness 保持 `60s`。

  修复后第二轮远端真实四源 60 分钟长跑通过。run dir 为 `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-real-source-20260706T021650Z`，binary sha256 为 `9a233dbb3fda705a3954b013b262543ad220791f31e08dd2343d8c352d2d7cb9`，`probe_exit=0`，manifest verdict 为 `PASS`，row_count 为 `177573`，finalized parquet sha256 为 `f561a27b39191d1636a12f61386a7f02089ab4e8bf3f408582f9bab1707ec775`，manifest sha256 为 `45440a6595584546633bffb617c4cf05475980df11c8254fffaa0b5f42f9a60d`，source_gaps 为 `[]`。source coverage：Polymarket market `115074` 行并覆盖 `14` 个 `btc-updown-5m` window（`1783304100` 到 `1783308000`），Binance depth `35991` 行，Binance aggTrade `23211` 行，RTDS Chainlink `3297` 行；ResourceWatermark 记录 `rss_bytes=77373440`、`queue_rows_high=94`、`queue_bytes_high=3568670`、`artifact_dir_bytes=81570811`，CPU/disk free/journal/PSI 在 Phase 0 probe 中以 unavailable reason 记录。

  阶段审查指出第二轮 60 分钟长跑缺少显式 rollover edge receipt；该反馈成立。首次补证 run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-rollover-edge-20260706T033425Z` 因 exact start 校验过严失败，暴露 Gamma exact slug 的 `candidate_start_ts_s` 可能早于 slug 目标窗口；已修复为保留 `candidate_start_ts_s` / `candidate_end_ts_s`，slug/end 必须匹配，candidate start 允许早于 logical start。修复后 15 分钟 targeted rollover run `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-rollover-edge-20260706T033741Z` 通过，binary sha256 为 `f49fdb369a8f4f39c83921a85afb562314138c495d0af18e4a0737c9d92eec52`，`probe_exit=0`，manifest verdict 为 `PASS`，row_count 为 `39570`，finalized parquet sha256 为 `21bc3066abcb9d7b301b1a3125a7c2a86cf8e9cfb9a3ce9e929e65214da19405`，manifest sha256 为 `6b9480164db171810eb37109984653cb829f09aaa32de64e0104b651ef653dd8`，rollover_edges sha256 为 `7d405c5f4336b1f5a15cb3479801ca94796a5bb1d83be74c170bbf73c4d31a89`，source_gaps 为 `[]`。该 run 写出 `3` 条显式 rollover edge，均满足 `prefetched_promoted_to_current=true`、`previous_current_retired_by_time=true`、`windows_contiguous=true`；边界为 `1783308900 -> 1783309200`、`1783309200 -> 1783309500`、`1783309500 -> 1783309800`。

  0.6.10 当前判定是组合证据通过：60 分钟长跑证明真实四源长时间吞吐和 source coverage；targeted rollover run 补足显式 rollover edge；0.6.7 synthetic run 仍作为 ResourceWatermark / throughput 容量预检。Phase 0 不等于生产 collector 完成，正式 adapter 仍必须补齐 actual request evidence、完整 market channel event contract、完整 ResourceWatermark 采样和生产 source health / reconnect trace。

  注意：短诊断 run 若只用于连通性或参数修复复核，必须显式设置 `PHASE0_REAL_SOURCE_MIN_ROLLOVER_EDGES=0`，不得把短 smoke 因未跨 market 边界而失败误判为正式 gate 失败；45-60 分钟 Phase 0 gate 保持默认至少 `1` 条 rollover edge。

- [x] **步骤 0.6.11：远端真实四源 60 分钟 hot path / parquet path 延迟补证**

  目标：补齐 0.6.10 未回答的消费路径问题，明确共用 `data-collect` 不是让训练、paper、live 都等待 finalized parquet。Phase 0 必须额外证明同一套 source adapter / event envelope 可以分成两条路径：paper/live 需要的实时 handoff hot path，以及训练/审计需要的 durable parquet mirror path。本步骤仍是验证 probe，不接入真实 paper/live，不生成 paper/live append-only event log、rotation artifact、NOE/ledger、order evidence、execution feedback 或交易逻辑。

  **文件：**

  - 创建：`data-collect/src/latency.rs`
  - 修改：`data-collect/src/lib.rs`
  - 修改：`data-collect/src/writer.rs`
  - 修改：`data-collect/benches/real_source_alignment_probe.rs`
  - 创建或修改：`data-collect/tests/real_source_latency_contract.rs`
  - 更新：`data-collect/docs/phase0-technical-validation.md`
  - 更新：`changelog/data-collect-changelog.md`

  **设计逻辑：**

  - source adapter 保持 0.6.10 的真实四源：Polymarket `btc-updown-5m` market channel、Binance `BTCUSDT` depth WS + REST snapshot、Binance `BTCUSDT` aggTrade WS、RTDS Chainlink `btc/usd`。
  - 每条 source 事件在 source task 收到业务消息时生成 probe-only `TimedRawEvent`，至少携带 `source_kind`、`source_identifier`、`source_event_type`、`source_ts`、`recv_time_utc_ms`、source receive monotonic time 和原始 `RawEvent`；source task 不分配 `event_seq`。
  - collector 从 source bounded queue 取出 `TimedRawEvent` 后分配单调递增的全局 `event_seq`，再进入 fanout router；fanout router 必须先把事件送入 realtime bounded handoff queue，再把同一事件送入 durable parquet batching path。hot path 不得等待 parquet batch 攒满、`write_events()`、sha256 或 finalize。
  - realtime handoff 使用 probe-only fake consumer，不连接 paper/live runtime。fake consumer 只记录 handoff receipt，不做下单、撤单、模型推理、NOE/ledger 或交易状态解释。
  - durable path 保留单 writer worker 和 running/digesting/final atomic publish；新增 `PHASE0_REAL_SOURCE_MAX_FLUSH_INTERVAL_MS=1000` 时间触发 flush，避免低速真实流量因 `flush_rows=5000` 被攒批等待掩盖。
  - durable batching path 只能在 collector 已分配 `event_seq` 后组 batch；每个 batch 必须记录 `event_seqs`，或同时记录 `first_event_seq`、`last_event_seq`、`rows` 并证明 batch 是 collector 顺序中的连续片段。本阶段优先写 `event_seqs`，避免只用范围掩盖乱序或丢事件。
  - writer worker 每个 batch 写出 `write_batch_receipts.jsonl`：`batch_seq`、`event_seqs`、`first_event_seq`、`last_event_seq`、`rows`、`bytes_estimate`、`batch_first_recv_time_utc_ms`、`batch_last_recv_time_utc_ms`、`batch_send_time_utc_ms`、`writer_dequeue_time_utc_ms`、`write_start_time_utc_ms`、`write_done_time_utc_ms`、`write_events_us`、`writer_channel_wait_ms`、`batch_wait_ms`、`flush_reason`、`source_recv_to_write_done_ms`。
  - fake consumer 写出 `hot_path_receipts.jsonl`：`event_seq`、`source_kind`、`recv_time_utc_ms`、`collector_dequeue_time_utc_ms`、`hot_send_time_utc_ms`、`hot_consumer_recv_time_utc_ms`、`source_recv_to_hot_delivery_ms`、`source_recv_to_collector_dequeue_ms`、`hot_queue_wait_ms`。
  - collector 每秒写出 `queue_samples.jsonl`：source event queue rows/bytes、hot handoff queue rows/bytes、writer chunk queue rows/bytes、各 queue high watermark、各 queue above-threshold cumulative ms、longest contiguous above-threshold ms、backpressure wait count、backpressure timeout count、dropped rows。
  - `data-collect/src/latency.rs` 承载可单测的 probe-only fanout / receipt / latency summary / queue duration helper；`real_source_alignment_probe.rs` 只负责编排真实源与写 artifact，不在 bench 私有代码中保留不可测的核心延迟计算。
  - `data-collect/src/writer.rs` 在现有 `ParquetSegmentWriter` 上新增 instrumented finalize API，记录 close、running->digesting rename、sha256、digesting->final rename 和 total finalize 耗时；不得在 probe 中另写一套 shadow parquet writer。
  - 远端 shell sidecar 每 `5s` 写出 `resource_samples.tsv`：UTC time、probe PID、CPU%、RSS、open files、artifact dir bytes、disk free bytes、PSI cpu/io/memory。如果 Linux 指标不可读，必须写出 `unavailable:<reason>`；单个采样点缺失只记为 degraded sample，整段关键指标不可读或无法解释时本步骤不能给出 `PASS`。

  **本地合同测试：**

  计划执行命令：

  ```bash
  set -euo pipefail
  cd /home/chillchill/btc-updown-marketmaker
  cargo fmt --all -- --check
  CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture
  CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings
  CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo build -p data-collect --release --bench real_source_alignment_probe
  ```

  `real_source_latency_contract` 必须断言：

  - `TimedRawEvent` 在 collector/fanout 处分配全局 `event_seq` 后可以同时进入 hot path 和 durable path，且两个 path 使用同一 `event_seq` 对齐。
  - hot path receipt 不依赖 parquet finalized path、manifest 或 sha256。
  - durable path receipt 可以把 batch 内每条事件映射到同一个 `write_done_time_utc_ms`，从而计算 `source_recv_to_write_done_ms`。
  - `PHASE0_REAL_SOURCE_MAX_FLUSH_INTERVAL_MS` 到期会触发 time flush；低流量时不会无限等待 `flush_rows=5000`。
  - queue sample 记录 high watermark、累计持续时间、最长连续持续时间、backpressure wait、timeout 和 drop；短暂 high watermark 不会被单独判为进程级失败。
  - writer instrumented finalize 能以微秒报告 close、sha256、atomic rename 和 total finalize 本地耗时，且仍通过现有 running/digesting/final 发布边界。
  - fake consumer 不生成 paper/live append-only log、rotation artifact、NOE/ledger、order evidence 或 execution feedback。

  **远端 60 分钟执行命令：**

  ```bash
  set -euo pipefail
  cd /home/chillchill/btc-updown-marketmaker
  TARGET_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-target.XXXXXX)"
  UPLOAD_DIR="$(mktemp -d /tmp/btc-updown-marketmaker-phase0-upload.XXXXXX)"
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p data-collect --release --bench real_source_alignment_probe
  REAL_SOURCE_BIN="$(find "$TARGET_DIR/release/deps" -maxdepth 1 -type f -perm -111 -name "real_source_alignment_probe-*" | sort | tail -n 1)"
  test -n "$REAL_SOURCE_BIN"
  install -m 0755 "$REAL_SOURCE_BIN" "$UPLOAD_DIR/real_source_alignment_probe"
  sha256sum "$UPLOAD_DIR/real_source_alignment_probe" > "$UPLOAD_DIR/real_source_alignment_probe.sha256"
  ssh ubuntu@3.255.156.183 'mkdir -p ~/btc-updown-marketmaker/data-collect/phase0-runs/bin'
  rsync -av "$UPLOAD_DIR/real_source_alignment_probe" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  rsync -av "$UPLOAD_DIR/real_source_alignment_probe.sha256" ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/phase0-runs/bin/
  ssh ubuntu@3.255.156.183 'bash -se' <<'REMOTE'
  set -euo pipefail
  BIN_ROOT="$HOME/btc-updown-marketmaker/data-collect/phase0-runs"
  DATASET_ROOT="$(realpath -m "$HOME/btc-updown-marketmaker/datasets")"
  case "$DATASET_ROOT" in
    "$HOME/btc-updown-marketmaker/datasets"|"$HOME/btc-updown-marketmaker/datasets/"*) ;;
    *) echo "dataset root escapes approved root: $DATASET_ROOT" >&2; exit 2 ;;
  esac
  RUN_ID="$(date -u +phase0-hot-parquet-latency-%Y%m%dT%H%M%SZ)"
  RUN_DIR="$DATASET_ROOT/phase0-validation/$RUN_ID"
  ARTIFACT_DIR="$RUN_DIR/artifacts"
  mkdir -p "$BIN_ROOT/bin" "$RUN_DIR/logs" "$ARTIFACT_DIR" "$RUN_DIR/tmp"
  PROBE="$BIN_ROOT/bin/real_source_alignment_probe"
  test -x "$PROBE"
  sha256sum "$PROBE" | tee "$RUN_DIR/logs/binary.sha256"
  (cd "$BIN_ROOT/bin" && sha256sum -c real_source_alignment_probe.sha256) | tee "$RUN_DIR/logs/binary-sha256-check.txt"
  {
    echo "run_id=$RUN_ID"
    echo "dataset_root=$DATASET_ROOT"
    echo "run_dir=$RUN_DIR"
    echo "artifact_dir=$ARTIFACT_DIR"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hot_path_probe=true"
    echo "flush_rows=5000"
    echo "max_flush_interval_ms=1000"
  } | tee "$RUN_DIR/logs/run-receipt.txt"
  set +e
  probe_timeout=0
  deadline_epoch=$(($(date +%s) + 3900))
  PHASE0_REAL_SOURCE_ENABLE_HOT_PATH_PROBE=1 \
  PHASE0_REAL_SOURCE_FLUSH_ROWS=5000 \
  PHASE0_REAL_SOURCE_MAX_FLUSH_INTERVAL_MS=1000 \
  REAL_SOURCE_RUN_SECS=3600 \
  DATASET_ROOT="$DATASET_ROOT" \
  REAL_SOURCE_RUN_ID="$RUN_ID" \
  REAL_SOURCE_ARTIFACT_DIR="$ARTIFACT_DIR" \
  "$PROBE" >"$RUN_DIR/logs/real-source-alignment-probe.log" 2>&1 &
  probe_pid=$!
  {
    printf 'sample_utc\tpid\tcpu_percent\trss_bytes\topen_files\tartifact_dir_bytes\tdisk_free_bytes\tpsi_cpu\tpsi_io\tpsi_memory\n'
    while kill -0 "$probe_pid" 2>/dev/null; do
      sample_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      cpu_percent="$(ps -p "$probe_pid" -o %cpu= 2>/dev/null | tr -d " " || true)"
      [ -n "$cpu_percent" ] || cpu_percent="unavailable:ps_cpu_failed"
      rss_kb="$(ps -p "$probe_pid" -o rss= 2>/dev/null | tr -d " " || true)"
      if [ -n "$rss_kb" ]; then rss_bytes=$((rss_kb * 1024)); else rss_bytes="unavailable:ps_rss_failed"; fi
      if [ -d "/proc/$probe_pid/fd" ]; then
        open_files="$(find "/proc/$probe_pid/fd" -maxdepth 1 -type l 2>/dev/null | wc -l | tr -d " " || true)"
        [ -n "$open_files" ] || open_files="unavailable:proc_fd_unreadable"
      else
        open_files="unavailable:proc_fd_missing"
      fi
      artifact_dir_bytes="$(du -sb "$ARTIFACT_DIR" 2>/dev/null | awk '{print $1}' || true)"
      [ -n "$artifact_dir_bytes" ] || artifact_dir_bytes="unavailable:du_artifact_failed"
      disk_free_bytes="$(df -B1 "$DATASET_ROOT" 2>/dev/null | awk 'NR==2 {print $4}' || true)"
      [ -n "$disk_free_bytes" ] || disk_free_bytes="unavailable:df_dataset_root_failed"
      psi_cpu="$(tr '\n' ' ' </proc/pressure/cpu 2>/dev/null || true)"
      [ -n "$psi_cpu" ] || psi_cpu="unavailable:psi_cpu_missing"
      psi_io="$(tr '\n' ' ' </proc/pressure/io 2>/dev/null || true)"
      [ -n "$psi_io" ] || psi_io="unavailable:psi_io_missing"
      psi_memory="$(tr '\n' ' ' </proc/pressure/memory 2>/dev/null || true)"
      [ -n "$psi_memory" ] || psi_memory="unavailable:psi_memory_missing"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sample_utc" "$probe_pid" "$cpu_percent" "$rss_bytes" "$open_files" "$artifact_dir_bytes" "$disk_free_bytes" "$psi_cpu" "$psi_io" "$psi_memory"
      sleep 5
    done
  } >"$RUN_DIR/logs/resource_samples.tsv" &
  sampler_pid=$!
  while kill -0 "$probe_pid" 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline_epoch" ]; then
      probe_timeout=1
      kill "$probe_pid" 2>/dev/null || true
      sleep 5
      kill -9 "$probe_pid" 2>/dev/null || true
      break
    fi
    sleep 1
  done
  wait "$probe_pid"
  probe_wait_exit=$?
  wait "$sampler_pid" 2>/dev/null || true
  set -e
  if [ "$probe_timeout" -eq 1 ]; then
    probe_exit=124
  else
    probe_exit="$probe_wait_exit"
  fi
  echo "probe_exit=$probe_exit" | tee "$RUN_DIR/logs/probe-exit.txt"
  find "$RUN_DIR" -maxdepth 6 -type f -printf "%p\t%s\n" | sort | tee "$RUN_DIR/logs/file-list.tsv"
  find "$RUN_DIR" -type f ! -path "$RUN_DIR/logs/file-sha256.tsv" ! -path "$RUN_DIR/logs/file-sha256.tsv.tmp" -exec sha256sum {} + | sort > "$RUN_DIR/logs/file-sha256.tsv.tmp"
  mv "$RUN_DIR/logs/file-sha256.tsv.tmp" "$RUN_DIR/logs/file-sha256.tsv"
  exit "$probe_exit"
  REMOTE
  ```

  **验收输出：**

  远端 run 必须生成并在 `data-collect/docs/phase0-technical-validation.md` 汇总以下文件和 sha256：

  - `artifacts/manifest.json`
  - `artifacts/source_coverage.json`
  - `artifacts/source_gaps.json`
  - `artifacts/rollover_edges.json`
  - `artifacts/latency_summary.json`
  - `artifacts/hot_path_receipts.jsonl`
  - `artifacts/write_batch_receipts.jsonl`
  - `artifacts/queue_samples.jsonl`
  - `artifacts/resource_watermark.json`
  - `logs/resource_samples.tsv`
  - `artifacts/typed_raw/*.parquet`
  - `logs/file-sha256.tsv`

  `latency_summary.json` 至少包含：

  - `source_recv_to_hot_delivery_ms` p50 / p95 / p99 / max。
  - `source_recv_to_write_done_ms` p50 / p95 / p99 / max。
  - `source_recv_to_collector_dequeue_ms` p50 / p95 / p99 / max。
  - `collector_dequeue_to_hot_send_ms` p50 / p95 / p99 / max。
  - `batch_wait_ms` p50 / p95 / p99 / max。
  - `writer_channel_wait_ms` p50 / p95 / p99 / max。
  - `write_events_us` p50 / p95 / p99 / max。
  - `finalize_total_us`、`finalize_close_us`、`sha256_us`、`atomic_rename_us`。
  - hot queue、source event queue、writer chunk queue 的 row/byte high watermark、累计非空 ms、超过 `10` / `100` / `1000` rows 的累计 ms 和最长连续 ms。
  - `backpressure_wait_count`、`backpressure_timeout_count`、`dropped_rows`、`hot_path_drop_count`、`writer_backlog_monotonic_growth_observed`。

  **通过条件：**

  - run 时长为 `3600s`，且至少覆盖 `6` 个连续 `btc-updown-5m` window 和至少 `1` 个有效 rollover edge；若 Polymarket 源未发布必要 market metadata，必须记录 source gap / decision item，不能伪装通过。
  - 四源均有 coverage verdict；每个 UTC 5 分钟 window 内每个源都给出 healthy、degraded with explicit source gap 或 blocked with reason。不得要求每个源在每个 window 都必然产生业务事件；无业务事件必须有 source gap、blocked reason 或平台语义说明。
  - `hot_path_receipts` 与 parquet durable path 能通过 `event_seq` 对齐；hot path receipt 数量必须等于进入 fanout 的事件数，或对每个缺失事件写出 drop/backpressure reason。
  - `source_recv_to_hot_delivery_ms` 与 `source_recv_to_write_done_ms` 必须分开报告；不得用 finalized parquet 可见时间代表 paper/live 实时接收延迟。
  - `batch_wait_ms` 必须和 `write_events_us` 分开报告；不得把 `flush_rows=5000` 造成的攒批等待解释为本地 CPU/磁盘写入慢。网络/业务路径延迟和 batch/queue 等待使用毫秒，本地 write/finalize/sha256/rename 耗时使用微秒。
  - `queue_rows_high` 必须报告持续时间；短暂 high watermark 只能记为 burst observation。只有 send timeout、drop、writer backlog 连续至少 `3` 个 `5s` sample 不回落、hot queue lag 连续至少 `3` 个 `5s` sample 不回落、run 结束仍未 drain，或无法保全 segment 时，才判定为拥堵或阻塞。
  - `logs/resource_samples.tsv` 必须覆盖 CPU、RSS、disk free、open files、artifact dir bytes 和 PSI；数据采集服务器 Linux 指标缺失时，本步骤 verdict 为 `PASS_WITH_LIMITS_NEEDS_USER_APPROVAL` 或 `BLOCKED_NEEDS_DECISION`，不得给 `PASS`。
  - durable parquet 仍必须 finalized-only、sha256、manifest complete；不得发布 partial artifact。
  - 本步骤不能生成或修改 paper/live append-only event log、rotation artifact、NOE/ledger、order evidence、execution feedback 或交易逻辑。

  **报告必须回答的问题：**

  - 不拥堵时，真实四源 `source_recv_to_hot_delivery_ms` 和 `source_recv_to_write_done_ms` 的 p50 / p95 / p99 / max 是多少；本地写入 `write_events_us`、`finalize_total_us`、`sha256_us`、`atomic_rename_us` 的 p50 / p95 / p99 / max 或单次值是多少。
  - `queue_rows_high` 的峰值是多少，持续了多少 ms，是 source event queue、hot handoff queue 还是 writer chunk queue。
  - 0.6.10 的 `queue_rows_high=94`、targeted run 的 `queue_rows_high=61` 与 synthetic `writer_queue_rows_high=5000` 是否是同一种指标；若不是，分别代表什么。
  - 单 writer worker 是否出现持续 backlog；如果没有，为什么可以判定不是写入拥堵。
  - parquet writer 变慢、sha256 或 finalize 是否影响 hot path；如果影响，阶段 0 不得进入阶段 1，必须先调整 fanout / isolation 设计。
  - 训练冷路径和 paper/live 热路径的接入边界是否被证据证明：训练消费 finalized typed raw parquet；paper/live 应消费 realtime handoff 并写自身 runtime artifact，不等 finalized parquet。

  当前执行结果：0.6.11 本地实现与合同验证已完成。新增 `data-collect/src/latency.rs`、`data-collect/tests/real_source_latency_contract.rs`，并修改 `data-collect/src/writer.rs` 与 `data-collect/benches/real_source_alignment_probe.rs`；本地 `cargo fmt --all -- --check`、`cargo test -p data-collect --lib --tests -- --nocapture`、`cargo clippy -p data-collect --all-targets -- -D warnings`、`cargo build -p data-collect --release --bench real_source_alignment_probe`、`git diff --check` 均通过。用户指出本地写入耗时应使用微秒后，已将本地 `write_events/finalize/sha256/rename` 字段改为 `*_us`；单位修正后的 120 秒远端短 smoke `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-units-smoke-20260706T091716Z` 通过。

  60 分钟正式远端补证已完成。run dir 为 `/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-hot-parquet-latency-20260706T092424Z`，binary sha256 为 `4e066002d33c0ec8d285b9d5e86549611052a669d3c2354d4103195e7c2a5aff`，`probe_exit=0`，manifest verdict 为 `PASS`，row_count 为 `181091`，finalized parquet sha256 为 `e78c6712eb23a2f02bc0425b319ca027ffefb25c2249d6562b0ed51068f37e21`，source_gaps 为 `[]`。source coverage：Polymarket market `117578` 行并覆盖 `14` 个 `btc-updown-5m` market window；Binance depth `35989` 行；Binance aggTrade `24003` 行；RTDS Chainlink `3521` 行。按 `recv_time_utc_ms` 归入 `13` 个 UTC 5 分钟 bucket 时，四源均有事件，没有 source 缺失 bucket。

  hot/durable 对齐结果：`hot_path_receipts.jsonl` 为 `181091` 行，`event_seq` 从 `1` 到 `181091` 连续；`write_batch_receipts.jsonl` 为 `3354` 个 batch，覆盖同一连续 `event_seq`，flush reason 为 `time=3353`、`shutdown=1`；finalized 目录只包含 `real-source-alignment-typed-raw.parquet`，无 running/digesting/tmp parquet 残留。

  延迟结果：`source_recv_to_hot_delivery_ms` p50/p95/p99/max 为 `0/1/1/191ms`；`source_recv_to_write_done_ms` 为 `533/1004/1061/1102ms`；`batch_wait_ms` 为 `1011/1090/1099/1101ms`；`writer_channel_wait_ms` 为 `0/0/1/6ms`；本地 `write_events_us` 为 `112/382/1122/26476us`；`finalize_total_us=480767`、`finalize_close_us=63033`、`sha256_us=417658`、`atomic_rename_us=75`。这证明 hot path 未等待 parquet batch、write、sha256 或 finalize；durable write-done 约 `1s` 的 p95 主要来自 `max_flush_interval_ms=1000` 的 time flush 等待，而不是本地写入 CPU/磁盘耗时。

  queue/resource 结果：source event queue rows high `15`，超过 `10` rows 累计 `6102ms`，最长连续 `1031ms`；hot handoff queue rows high `1`；writer chunk queue rows high `472`，超过 `100` rows 累计 `177118ms`，最长连续 `2035ms`，未超过 `1000` rows。`backpressure_wait_count=0`、`backpressure_timeout_count=0`、`dropped_rows=0`、`hot_path_drop_count=0`、`writer_backlog_monotonic_growth_observed=false`。远端 sidecar 采样 `717` 条，RSS 峰值 `142856192` bytes，CPU 首个启动采样峰值 `100%`、稳定段约 `6-7%`，open files 峰值 `24`，disk free 最低 `57630863360` bytes，CPU/IO/memory PSI full avg10 均未出现非零采样。

  后续补证：0.6.11 首次正式 run 的 manifest 记录了 `collector_host_id=ip-172-26-10-0`，但 `collector_region=unknown`。根因是 Phase 0 probe 只读取 `COLLECTOR_REGION` 环境变量，未设置时默认 `unknown`；该缺口已在 0.6.12 修复并验证。0.6.11 的性能结论仍有效，0.6.12 证明后续 Phase 0/正式 probe 在同一数据采集服务器上可自动写出 `collector_region=eu-west-1`。

- [x] **步骤 0.6.12：collector region 自动识别补证**

  目标：在阶段 0 内修复 `collector_region=unknown`，不把该证据缺口留到阶段 1；验证 `recv_time_utc` 的采集主机/区域元数据可以由 artifact 自身证明。

  **文件：**

  - 创建：`data-collect/src/collector_identity.rs`
  - 修改：`data-collect/src/lib.rs`
  - 修改：`data-collect/benches/market_discovery_probe.rs`
  - 修改：`data-collect/benches/real_source_alignment_probe.rs`
  - 创建：`data-collect/tests/collector_identity_contract.rs`
  - 更新：`data-collect/docs/phase0-technical-validation.md`
  - 更新：`changelog/data-collect-changelog.md`

  **实现逻辑：**

  - collector region resolution 顺序为：非空 `COLLECTOR_REGION` env override 优先；否则读取 AWS IMDS placement region；仍不可用时才写 `unknown`。
  - Phase 0 的 `market_discovery_probe` 和 `real_source_alignment_probe` 均使用同一 `data_collect::collector_identity::resolve_collector_region()`，避免 bench 私有逻辑分叉。
  - `collector_region` 只标识采集主机区域，不替代 source timestamp、`recv_time_utc_ms`、paper/live runtime receive time 或 decision time。

  **本地 TDD 验证：**

  - RED：新增 `collector_identity_contract` 后，`cargo test -p data-collect --test collector_identity_contract -- --nocapture` 失败于缺少 `data_collect::collector_identity`。
  - GREEN：实现后，`collector_region_prefers_explicit_env_value`、`collector_region_uses_imds_when_env_is_missing`、`collector_region_uses_imds_when_env_is_blank`、`collector_region_is_unknown_only_when_no_source_is_available` 均通过；`data-collect` 本地 `--lib --tests` 当前共 `30` 条测试通过。

  **远端 smoke 验证：**

  - run dir：`/home/ubuntu/btc-updown-marketmaker/datasets/phase0-validation/phase0-region-smoke-20260706T104540Z`
  - binary sha256：`8459e763ff12921fc478a34c79ced1872235035690213991da408ebb24554b60`
  - 执行时显式 `env -u COLLECTOR_REGION`，证明不是依赖手工环境变量。
  - `probe_exit=0`，manifest verdict `PASS`，row_count `4430`，source_gaps `[]`。
  - manifest 与 initial discovery receipt 均记录 `collector_host_id=ip-172-26-10-0`、`collector_region=eu-west-1`。
  - finalized parquet sha256 `ecf7347aa3b47b3342e83a76008efe9583c4d02e3b45d115da69379a831dda5f`，manifest sha256 与实际 parquet sha256 一致。
  - source coverage：Polymarket market `2568` 行并覆盖 `2` 个 market window；Binance depth `1189` 行；Binance aggTrade `554` 行；RTDS Chainlink `119` 行。

  **验收结论：**

  - `collector_region=unknown` 已在阶段 0 修复并验证通过。
  - 0.6.11 的 60 分钟 hot/durable 性能证据不需要因该字段缺口重跑；0.6.12 是针对 collector identity evidence 的真实源短 smoke 补证。
  - 正式 collector 阶段仍必须记录 host clock sync status 和生产级 ResourceWatermark manifest，但这属于后续生产合同实现，不再阻塞阶段 0 技术 gate 进入用户审查。

### 任务 0.7：阶段 0 审查与用户决策

**文件：**

- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [x] **步骤 0.7.1：汇总本地 Phase 0 probe verdict**

  本地 Phase 0 报告必须先给出以下 verdict 之一，并明确不能替代远端 Phase 0 验收：

  - `LOCAL_PROBE_PASS`：本地 SDK、writer、synthetic throughput、network recovery contract、ResourceWatermark contract 通过；仍需执行 0.6.4-0.6.8 的远端验证。
  - `LOCAL_PROBE_PASS_WITH_LIMITS`：本地技术路径局部可行但有限制；必须列出限制、阈值和远端验证重点。
  - `LOCAL_PROBE_BLOCKED`：本地 SDK 缺口、性能不可达或合同测试失败；不得进入远端验证。

- [x] **步骤 0.7.2：重新汇总远端 Phase 0 verdict**

  远端 Phase 0 报告必须基于步骤 0.6.4-0.6.12 给出以下 verdict 之一：

  - `PASS_TO_PHASE1_REVIEW`：本地 probe、远端清理 receipt、远端 writer smoke、30-60 分钟 synthetic throughput / ResourceWatermark 预检、真实网络 reachability smoke、真实 `btc-updown-5m` 冷启动 discovery、真实四源长时间采集、rollover、时间线对齐、hot path realtime handoff 延迟、durable parquet 写入延迟和资源采样均满足进入用户审查的条件。
  - `PASS_WITH_LIMITS_NEEDS_USER_APPROVAL`：技术路径可行但存在限制；必须列出限制、阈值、风险、是否需要 2-4 小时加强 soak，以及后续阶段必须修复的事项，经用户明确批准后才能进入阶段 1。hot path 或 resource sidecar 指标缺失时，不得给无条件 `PASS_TO_PHASE1_REVIEW`。
  - `BLOCKED_NEEDS_DECISION`：远端服务器资源不匹配、磁盘空间不足、网络 reachability 不可解释、真实 active market discovery 不完整、rollover 未验证、四源时间线无法对齐、真实吞吐不可达、hot path 依赖 parquet finalized、hot queue 或 writer queue 出现持续不可恢复拥堵、ResourceWatermark hard threshold 无法恢复、artifact 无法保全，或出现需求边界冲突。

  当前执行结果：0.6.9 cold-start discovery、0.6.10 真实四源 60 分钟长跑、审查后 targeted rollover 补证、0.6.11 远端真实四源 60 分钟 hot path / parquet path 延迟补证和 0.6.12 collector region 自动识别补证均已完成。0.6.11 证明 hot path receipt 与 durable parquet path 可用同一 `event_seq` 对齐，hot path p95 为 `1ms`，durable write-done p95 为 `1004ms` 且主要受 `max_flush_interval_ms=1000` 影响，本地 `write_events_us` p95 为 `382us`。远端 sidecar 补齐 CPU/RSS/disk/open files/artifact dir bytes/PSI 采样且无 hard-threshold 压力；0.6.12 证明同一数据采集服务器在未设置 `COLLECTOR_REGION` 时可通过 AWS IMDS 写出 `collector_region=eu-west-1`。

  远端 Phase 0 当前 verdict：`PASS_TO_PHASE1_REVIEW`。原因是技术路径已足以进入用户审查，且 0.6.12 已在阶段 0 内修复并验证 `collector_region=unknown` 缺口。完整 host clock sync status 与生产级 ResourceWatermark runtime manifest 仍必须在后续正式 collector 阶段实现，但不再阻塞阶段 0 技术 gate 进入用户审查；0.7.3 用户审查 gate 与阶段收尾 commit/push 已完成。

- [x] **步骤 0.7.3：用户审查 gate**

  用户在会话中明确要求“派生 subagents 替我完成用户审查 gate”。已按该授权派生 4 个独立审查代理，分别审查需求/计划/报告一致性、代码/测试/SDK-first 证据、远端性能/资源/验收证据、范围纪律/过度设计/路径规则。四个审查结论均为 `PASS_WITH_NOTES`，gate recommendation 均为 `ACCEPT_WITH_NOTES`，未发现 Critical 或阻塞 Important 问题。

  综合结论：0.7.3 用户审查 gate 已接受，阶段收尾 commit/push 已完成，Phase 0 当前状态更新为 `PHASE0_COMPLETE_READY_FOR_PHASE1`。注意项不阻塞阶段 0：正式 collector 阶段仍需补齐 host clock sync status、生产级 ResourceWatermark runtime manifest、actual Polymarket outbound request evidence 或明确 unavailable reason、完整 source health/reconnect trace，以及 paper/live 主机 runtime receive timestamp 证据。后续从阶段 1 开始时仍不得自行扩大范围、切换 SDK 旁路、降低吞吐验收、跳过后续阶段审查、或把 live 小主机改成构建/重处理机器。

## 六、阶段 1：合同与 schema 冻结

### 任务 1.1：定义 event envelope 合同

**文件：**

- 创建：`data-collect/src/event.rs`
- 创建：`data-collect/tests/event_envelope_contract.rs`

- [x] **步骤 1.1.1：写 event envelope contract 测试**

  测试必须断言每条源事件都包含：

  - `source_kind`
  - `source_event_type`
  - `source_identifier`
  - `source_ts` 或 no-source-timestamp 证据
  - `recv_time_utc`
  - `collector_host_id`
  - `collector_region`
  - `source_endpoint` 或 endpoint digest
  - `host_clock_sync_status`
  - `raw_json` 或等价原始证据引用

- [x] **步骤 1.1.2：实现最小 envelope 类型**

  实现只承载采集证据；不得加入 label、feature、tick_stream、reward、PnL、NAV、ledger 或模型输入字段。

- [x] **步骤 1.1.3：运行合同测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test event_envelope_contract -- --nocapture
  ```

  预期：缺少 `recv_time_utc`、collector host/region 或 source timestamp 证据时失败；Polymarket platform `timestamp` 不会覆盖 `recv_time_utc`。

  执行结果：已新增 `data-collect/src/event.rs` 和 `data-collect/tests/event_envelope_contract.rs`；`cargo test -p data-collect --test event_envelope_contract -- --nocapture` 通过，5 条测试覆盖 `recv_time_utc`、source timestamp / no-source-timestamp evidence、collector host/region、`source_endpoint`、clock sync、raw_json / equivalent reference、numeric `parquet_raw_layout_version`，以及 Polymarket platform `timestamp` 不覆盖 `recv_time_utc`。

### 任务 1.2：定义逐源数据格式与消费者兼容合同

**文件：**

- 创建：`data-collect/tests/source_format_contract.rs`
- 创建：`data-collect/tests/consumer_format_compatibility_contract.rs`
- 创建：`data-collect/src/source_format.rs`

- [x] **步骤 1.2.1：写逐源 typed raw 格式测试**

  测试必须逐源断言：

  - Polymarket market channel 保留 `event_type`、平台 `timestamp`、`recv_time_utc`、market/condition/asset/token 标识、subscription payload、`custom_feature_enabled`、raw_json；`custom_feature_enabled` 缺失、subscription 不可证明或 slug-only discovery 时 manifest 必须为 source gap / incomplete。
  - Polymarket market metadata / `new_market` 保留 `condition_id`、token id / asset id、CLOB market id、slug、start/end、active、acceptingOrders、`market_lifecycle_state`、tick size、min size、fee/reward、candidate slug、active scan evidence、metadata snapshot evidence 和 clock drift evidence。
  - Binance depth update 保留 `e,E,s,U,u,b,a`、combined stream wrapper、`recv_time_utc`、stale/duplicate/gap/resnapshot evidence。
  - Binance depth snapshot 保留 `lastUpdateId,bids,asks,snapshot_fetch_start_utc,snapshot_recv_time_utc`，并证明没有伪造 exchange timestamp。
  - Binance aggTrade 保留 `e,E,s,a,p,q,f,l,T,m,buyer_is_maker,recv_time_utc`，防止只写 4 列的缩水 schema。
  - RTDS Chainlink BTC/USD 保留 RTDS message timestamp、payload timestamp、symbol、value、source topic、subscription raw JSON、ack/first message 或 close/error evidence 和 `recv_time_utc`。

- [x] **步骤 1.2.2：写 data-pipeline 格式兼容测试**

  测试断言所有 JSON-like typed raw parquet 都带 parseable `raw_json`、`parquet_raw_layout_version`、`parquet_raw_source_kind`、schema version、source coverage 和 manifest；`data-pipeline` labels、validation、tick_stream、features 和训练切分不由 `data-collect` 生成。

- [x] **步骤 1.2.3：写训练、paper、live 消费边界格式测试**

  测试断言：

  - 训练只能把 `data-collect` 输出当作外部市场 typed raw mirror，必须经 `data-pipeline` 生成 labels/features/tick_stream/split 后才能训练。
  - paper/live 只能把 `data-collect` 输出当作外部市场源证据、coverage 和 manifest，不得替代 paper/live 自身 append-only event log、runtime receive timestamp、Market Context Builder、NOE/Ledger 或交易逻辑。
  - source kind、schema version、manifest、coverage window 和 timestamp namespace 对训练、paper、live 保持一致；字段为空时必须有 nullable 语义或 unavailable reason。

- [x] **步骤 1.2.4：运行格式兼容测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test source_format_contract -- --nocapture
  cargo test -p data-collect --test consumer_format_compatibility_contract -- --nocapture
  ```

  预期：每个 source 的字段级格式可被测试固定；data-pipeline、训练、paper、live 的消费边界不会被 `data-collect` 改写。

  执行结果：已新增 `data-collect/src/source_format.rs`、`data-collect/tests/source_format_contract.rs` 和 `data-collect/tests/consumer_format_compatibility_contract.rs`；`cargo test -p data-collect --test source_format_contract -- --nocapture` 通过 6 条逐源格式测试，`cargo test -p data-collect --test consumer_format_compatibility_contract -- --nocapture` 通过 4 条消费者边界测试。测试固定 Polymarket market / metadata / `market_resolved`、Binance depth update / snapshot、Binance aggTrade、RTDS Chainlink BTC/USD 字段，并证明 `data-collect` 不生成 labels/features/tick_stream/split、paper/live append-only log、NOE/ledger 或交易决策。Binance depth direct WS stream 不要求 combined wrapper；只有使用 combined stream 时才保留 wrapper 证据。

### 任务 1.3：定义 manifest、coverage 和 required_files[] 合同

**文件：**

- 修改/扩展：`data-collect/src/manifest.rs`
- 创建：`data-collect/tests/manifest_contract.rs`

- [x] **步骤 1.3.1：写 manifest 合同测试**

  测试必须覆盖 manifest 字段：

  - artifact path、row count、sha256。
  - schema version、code version / git commit、collector binary sha256、source config digest。
  - validation verdict、source coverage、source gap。
  - health/gap/reconnect artifact path 或 digest。
  - start/end time、coverage window、collector host/region。
  - ResourceWatermark、writer queue 高水位、flush latency。
  - `required_files[]` 的 file role、path、finalized status、row count、sha256/digest、schema version、missing reason。

- [x] **步骤 1.3.2：实现 manifest 类型和 incomplete verdict**

  当 atomic rename 失败、sha256 缺失、row count 缺失、schema version 缺失或 manifest 不一致时，manifest 必须为 incomplete，且不得发布为 complete。source coverage 缺失或不完整时，manifest 必须记录 `coverage_incomplete`、source gap 和 missing reason；这属于 `ArtifactIncomplete` / `SourceDegraded`，不得单独触发 collector 进程退出。

- [x] **步骤 1.3.3：运行 manifest 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test manifest_contract -- --nocapture
  cargo test -p data-collect incomplete_manifest_does_not_imply_process_fatal -- --nocapture
  ```

  预期：complete/incomplete verdict 可由测试数据稳定复现；incomplete manifest 会阻止消费者当作 complete 使用，但不会默认把 collector 标记为进程级失败。

  执行结果：已扩展 Phase 0 既有 `data-collect/src/manifest.rs`，补齐 health/gap/reconnect artifact reference、start/end、coverage window、manifest source gap、coverage incomplete reason、source coverage 字段有效性校验和 failure class；`cargo test -p data-collect --test manifest_contract -- --nocapture` 通过 3 条测试。`incomplete manifest` 当前归类为 `ArtifactIncomplete`，不会默认升级为 `ProcessFatal`。

### 任务 1.4：定义 source health、source gap 和 reconnect trace 合同

**文件：**

- 修改/扩展：`data-collect/src/health.rs`
- 创建：`data-collect/tests/health_gap_contract.rs`

- [x] **步骤 1.4.1：写异常语义测试**

  测试覆盖需求文档 5.1 的全部异常，并断言每个异常至少进入 source health、source gap 或 reconnect trace 之一。

- [x] **步骤 1.4.2：实现最小 health/gap/reconnect 类型**

  类型必须包含 source、attempt、sleep、错误类别、最终状态、open/half-open/recovered/exhausted、freshness verdict 和 coverage verdict。

- [x] **步骤 1.4.3：运行 health/gap 合同测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test health_gap_contract -- --nocapture
  cargo test -p data-collect source_gap_and_stale_are_degraded_not_process_fatal -- --nocapture
  ```

  预期：异常不会被静默吞掉；ping/pong 正常但业务数据静默时写入 stale 或 gap；单源 degraded 不会掩盖其他源健康状态，也不会默认导致 collector 进程退出。

  执行结果：已扩展 Phase 0 既有 `data-collect/src/health.rs`，新增需求级异常清单、`SourceHealthEvent`、`CoverageVerdict` 和 `RuntimeFailureClass`；`cargo test -p data-collect --test health_gap_contract -- --nocapture` 通过 4 条测试。writer queue/backpressure/resource watermark 进入 `BackpressureDegraded`，source gap/stale 进入 `SourceDegraded`，均不默认触发进程级失败。

## 七、阶段 2：source adapter 实现

### 任务 2.0：四源启动调度与覆盖对齐

**文件：**

- 创建：`data-collect/src/scheduler.rs`
- 创建：`data-collect/tests/source_scheduler_contract.rs`

- [x] **步骤 2.0.1：写辅助源独立启动测试**

  测试必须复现 poly-tx 旧问题：首个 Polymarket worker 因 asset ids / metadata discovery 延迟或失败而未启动时，Binance depth、Binance aggTrade 和 RTDS Chainlink 仍必须按计划独立启动或记录自身 source gap，不得等待 Polymarket worker 成功。

- [x] **步骤 2.0.2：写四源 preload 覆盖对齐测试**

  测试断言 current active market 和 planned next market `T-30s` preload window 中，Polymarket market channel、Binance depth、Binance aggTrade、RTDS Chainlink 都有各自 coverage window、first business event 或明确 source gap；不能只证明 Polymarket 单源 T-30s。

- [x] **步骤 2.0.3：运行 source scheduler 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect source_scheduler_starts_aux_sources_independently -- --nocapture
  cargo test -p data-collect four_sources_have_aligned_t30_preload_coverage_or_gap -- --nocapture
  ```

  预期：辅助源启动不依赖首个 Polymarket worker；任一源缺失必须进入 source coverage/source gap/reconnect trace。

  执行结果：已新增 `data-collect/src/scheduler.rs` 和 `data-collect/tests/source_scheduler_contract.rs`。`SourceScheduler::four_source()` 明确 Polymarket market、Binance depth、Binance aggTrade、RTDS Chainlink 四源计划；Polymarket asset ids / metadata 未就绪时，仅 Polymarket market 记录 `polymarket_asset_ids_or_metadata_not_ready` source gap，三个辅助源仍返回 `Start`。`four_sources_have_aligned_t30_preload_coverage_or_gap` 同时检查 current active 与 planned next `T-30s` window，每个源每个 window 必须绑定目标 market slug，且有覆盖 required window end 的 coverage + first business event；source gap 不能只有 reason 字符串，必须附带 gap/reconnect/freshness evidence。审查后新增 late-start preload、gap-without-evidence、missing planned next window 和 gap slug mismatch 反例，防止 missed `T-30s` preload、缺证据 gap、缺少 next preload window 或错误 market 的 gap 被误判完整。`cargo test -p data-collect --test source_scheduler_contract -- --nocapture` 通过 4 条测试。

### 任务 2.1：Polymarket market channel 与 market lifecycle

**文件：**

- 修改：`data-collect/src/scheduler.rs`
- 创建：`data-collect/src/sources/polymarket.rs`
- 创建：`data-collect/tests/polymarket_contract.rs`

- [x] **步骤 2.1.1：写 Polymarket 合同测试**

  测试覆盖：

  - subscription payload 记录 `assets_ids`、`type: "market"`、`custom_feature_enabled: true`。
  - `book`、`price_change`、`last_trade_price`、`best_bid_ask`、`tick_size_change`、`new_market`、`market_resolved`。
  - `new_market` metadata、condition id、token id / asset id、active/acceptingOrders、tick/min size、fee/reward 原始字段。
  - candidate slug + active scan / metadata snapshot 双路径；slug-only discovery 必须 incomplete，不能作为完整 discovery 成功。
  - `custom_feature_enabled` 缺失、订阅无法证明、SDK 支持状态不明或首个业务事件缺失时，对应 source/window manifest 必须标记 source gap / incomplete；其他 source 继续采集，collector 不得因此全局退出。
  - candidate、prefetched、active、retired 四态。
  - current active market 与 planned market start 前 `T-30s` prefetched next market 覆盖边界。
  - retired predicate 的事件路径与 fallback metadata 路径；事件路径必须断言 `subscribed_asset_ids_for_market`、`resolved_event_asset_ids`、`winning_asset_id`、`winning_outcome`、nullable `close_reason_if_present`，fallback 路径必须断言 `market_resolved_missing_reason`、`fallback_metadata_asset_ids`、fallback freshness、same condition/market 校验和 close/resolution 证据；单腿 resolved、asset set 不等于/不覆盖、winning asset 不属于目标 market、fallback stale 或 same condition 校验失败时，只能记录 stale/gap，不得进入 retired。
  - discovery / asset_ids miss 过期后必须记录 miss、`discovery_miss_rate`、跳过过期 target 或按当前时间重算目标；不得无限卡住同一 market。

- [x] **步骤 2.1.2：实现 SDK-first adapter**

  只使用阶段 0 已批准的 SDK 能力。若执行时发现 SDK 行为与阶段 0 报告不一致，立即停止并更新 SDK gap 报告，不自行启用 raw WS/REST 旁路。

- [x] **步骤 2.1.3：运行 Polymarket 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect polymarket_market_channel_contract -- --nocapture
  cargo test -p data-collect polymarket_retired_predicate_contract -- --nocapture
  cargo test -p data-collect polymarket_t30_prefetch_coverage_contract -- --nocapture
  cargo test -p data-collect polymarket_discovery_requires_active_scan_or_metadata_snapshot -- --nocapture
  cargo test -p data-collect polymarket_discovery_miss_does_not_block_later_markets -- --nocapture
  ```

  预期：`recv_time_utc` 与 Polymarket platform `timestamp` 同时保留；非爱尔兰 collector 延迟不会被标成 paper/live 爱尔兰接收延迟。

  执行结果：已新增 `data-collect/src/sources/polymarket.rs` 和 `data-collect/tests/polymarket_contract.rs`。adapter 只复用官方 `polymarket_client_sdk_v2` 生成 market subscription evidence，未实现 raw WebSocket / raw REST / manual signing 旁路；记录的官方 market channel endpoint 为 `wss://ws-subscriptions-clob.polymarket.com/ws/market`，但 SDK `Client::new()` 仍使用 base endpoint 并由 SDK 自动 append channel path。支持事件集固定为 `book`、`price_change`、`last_trade_price`、`best_bid_ask`、`tick_size_change`、`new_market`、`market_resolved`。测试覆盖 SDK subscription JSON 的 `type=market`、`assets_ids`、`custom_feature_enabled=true`，active scan / metadata snapshot 双路径、slug-only discovery incomplete、candidate/prefetched/active/retired 四态、`T-30s` prefetch window、resolved event 与 fallback metadata 两条 retired predicate 路径、过期 discovery miss 不阻塞后续 market。审查后修复 time-only retired false positive：`classify_lifecycle()` 只有在 retired predicate 已给出 evidence 时才返回 `Retired`；事件路径要求 `winning_asset_id` 属于目标 subscribed asset set；fallback metadata 要求非空目标 subscribed asset set、`closed=true`、`active=false`、`accepting_orders=false`、`closed_time_utc_ms` 和非空 resolution evidence。`cargo test -p data-collect --test polymarket_contract -- --nocapture` 通过 5 条测试。

### 任务 2.2：Binance BTCUSDT depth 和 aggTrade

**文件：**

- 创建：`data-collect/src/sources/binance.rs`
- 创建：`data-collect/tests/binance_contract.rs`

- [x] **步骤 2.2.1：写 Binance depth stitch 测试**

  测试覆盖：

  - WS depth raw 字段 `e,E,s,U,u,b,a`。
  - REST snapshot 字段 `lastUpdateId,bids,asks,snapshot_fetch_start_utc,snapshot_recv_time_utc`。
  - combined stream wrapper `stream/data` 保留。
  - 丢弃 `u <= lastUpdateId`。
  - 首个 update 满足 `U <= lastUpdateId + 1 <= u`。
  - stale/duplicate update 记录并忽略。
  - `U > local_u + 1` 记录 source gap 并重新 snapshot。

- [x] **步骤 2.2.2：写 aggTrade 测试**

  测试断言保存原始字段 `e,E,s,a,p,q,f,l,T,m`，并把 `m` 保存为 `buyer_is_maker` 或等价原始布尔字段。

- [x] **步骤 2.2.3：实现 Binance official source adapter**

  live Binance 源只来自 Binance 官方 WebSocket stream 和官方 REST depth snapshot；RTDS、archive、relay、proxy、cached feed 或历史 backfill 只能作为旁证或补数，不能冒充 live official source。

- [x] **步骤 2.2.4：运行 Binance 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect binance_depth_stitch_contract -- --nocapture
  cargo test -p data-collect binance_aggtrade_raw_fields_contract -- --nocapture
  ```

  预期：snapshot 缺失、update id 缺口、rate limit、server shutdown 和 aggTrade 长时间无业务消息都会写入 source health/gap/reconnect 证据。

  执行结果：已新增 `data-collect/src/sources/binance.rs` 和 `data-collect/tests/binance_contract.rs`。adapter 解析 Binance 官方 depth WS raw 字段 `e,E,s,U,u,b,a`，保留 combined stream `stream/data` wrapper，解析官方 REST snapshot `lastUpdateId,bids,asks` 与 fetch/recv 边界；`stitch_depth_update()` 覆盖 `u <= lastUpdateId` 丢弃、首个 update `U <= lastUpdateId + 1 <= u`、stale/duplicate 忽略、`U > local_u + 1` 触发 resnapshot gap。审查后修复 false complete：单条 depth WS update parse 后不会自动写入 `local_u` / `stitch_evidence`，必须通过 `apply_depth_stitch_evidence()` 才能成为 complete typed raw；diff depth update 允许单侧 bid 或 ask 为空，但双侧同时为空判 incomplete；REST snapshot bids/asks 仍必须双侧非空。aggTrade 解析并保存 `e,E,s,a,p,q,f,l,T,m`，其中 `m` 保存为 `buyer_is_maker`。`BinanceOfficialSourceConfig::validate_official()` 改为按 URL scheme/host/path/query allowlist 校验官方 WS host `stream.binance.com`、`data-stream.binance.vision` 和官方 REST host `api.binance.com`、`api-gcp.binance.com`、`api1-4.binance.com`、`data-api.binance.vision`，拒绝 `binance.com.evil` 等 relay/proxy；parser 也拒绝非 `BTCUSDT` 或错误 stream。新增 snapshot missing、rate limit、server shutdown、no business messages 的 health/gap/reconnect 分类测试。`cargo test -p data-collect --test binance_contract -- --nocapture` 通过 3 条测试。

### 任务 2.3：Polymarket RTDS Chainlink BTC/USD

**文件：**

- 创建：`data-collect/src/sources/rtds.rs`
- 创建：`data-collect/tests/rtds_contract.rs`

- [x] **步骤 2.3.1：写 RTDS subscription 测试**

  测试断言 subscription raw JSON 可追溯：

  - `action: "subscribe"`
  - `topic: "crypto_prices_chainlink"`
  - `type: "*"`
  - `filters: "{\"symbol\":\"btc/usd\"}"`
  - 重连后必须重新验证 topic、symbol filter、payload timestamp、业务消息 freshness 和 source stale 状态。

- [x] **步骤 2.3.2：实现 RTDS adapter**

  adapter 保存 RTDS message timestamp、payload timestamp、symbol、value、source topic 和 `recv_time_utc`；RTDS Chainlink BTC/USD 只能作为 Chainlink 价格证据，不替代 Binance 官方 depth 或 aggTrade。

- [x] **步骤 2.3.3：运行 RTDS 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect rtds_chainlink_contract -- --nocapture
  cargo test -p data-collect rtds_reconnect_revalidates_topic_symbol_payload_timestamp -- --nocapture
  ```

  预期：RTDS ping/pong 或连接 open 不能单独证明 coverage 完整；业务数据 freshness 才能恢复 healthy。

  执行结果：已新增 `data-collect/src/sources/rtds.rs` 和 `data-collect/tests/rtds_contract.rs`。adapter 使用官方 SDK `Subscription::chainlink_prices(Some("btc/usd"))` 与 `SubscriptionRequest::subscribe(...)` 生成 subscription raw JSON；`source_format` 的 RTDS validator 已兼容官方 SDK 实际 `subscriptions[]` 结构，同时保留既有扁平测试样例。测试断言 `action=subscribe`、`topic=crypto_prices_chainlink`、`type=*`、`filters="{\"symbol\":\"btc/usd\"}"`，并要求重连恢复必须重新验证 topic、symbol filter、payload timestamp 和业务消息 freshness；仅连接 open / ping-pong 不足以恢复 healthy。`cargo test -p data-collect --test rtds_contract -- --nocapture` 通过 2 条测试。

### 阶段 2 收口

第一轮阶段 2 独立审查为 3 个 `FAIL`；完成修复后第二轮审查为 1 个 `PASS_WITH_NOTES`、2 个 `FAIL`。第二轮问题已按最小范围修复：四源 coverage 现在强制 current active 与 planned next preload 两个 phase 均存在，source gap 也必须绑定目标 market slug；Polymarket fallback retired predicate 拒绝空目标 subscribed asset set；Binance diff depth update 不再要求 bid/ask 双侧同时非空，官方 endpoint allowlist 补齐官方备用 host。当前本地验证通过 `cargo fmt --all -- --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture`（66 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings` 和 release bench targets 构建；最终阶段 2 复审结果为 3 个 `PASS_WITH_NOTES` 且无阻塞项，结果已记录在 changelog。阶段 2 完成 commit/push 后进入阶段 3。

## 八、阶段 3：writer、分区与 atomic publish

### 任务 3.1：typed raw parquet writer

**文件：**

- 修改/扩展：`data-collect/src/writer.rs`
- 创建：`data-collect/tests/parquet_writer_contract.rs`

- [x] **步骤 3.1.1：写 parquet schema metadata 测试**

  测试断言每个 source parquet 带 `parquet_raw_layout_version` 和 `parquet_raw_source_kind`，并保存 parseable `raw_json` 或等价原始证据引用。

- [x] **步骤 3.1.2：实现 async 调度与 blocking 隔离**

  WebSocket、HTTP、timer、channel、manifest 写入、health 写入和 writer 调度不得阻塞主事件循环；文件 I/O、parquet close/finalize、data-collect 内部 parquet compression、local directory scan、sha256、large manifest aggregation、本地旧 segment 清理必须进入后台任务或 blocking pool。

- [x] **步骤 3.1.3：运行 writer 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect parquet_writer_schema_metadata_contract -- --nocapture
  cargo test -p data-collect blocking_work_is_isolated_from_main_runtime -- --nocapture
  ```

  预期：主事件循环没有阻塞 I/O 或重 CPU；writer 记录 row group/buffer 内存估算、flush latency、bytes written 和 finalize 结果。

  执行结果：已新增 `data-collect/tests/parquet_writer_contract.rs`，并扩展 `data-collect/src/writer.rs`。`ParquetSegmentWriter::create_for_source()` 写入 parquet file key-value metadata：`parquet_raw_layout_version`、`parquet_raw_source_kind`、`data_collect_artifact_kind=typed_raw_parquet`；`write_events_with_stats()` 返回 rows、estimated bytes、write latency 和 row group memory estimate，并拒绝与单源 segment 不匹配的 `parquet_raw_source_kind`；parseable `raw_json` 仍由 event/source-format 合同保证，writer 不在热写入路径重复 JSON parse。`write_segment_on_blocking_pool_for_source()` 保留文件 I/O、parquet close/finalize、sha256 和 rename 在 blocking pool 中执行的边界。`cargo test -p data-collect --test parquet_writer_contract -- --nocapture` 通过 3 条测试。

### 任务 3.2：按 UTC day 分区与 paper/live 边界

**文件：**

- 创建：`data-collect/src/publish.rs`
- 创建：`data-collect/tests/partition_publish_contract.rs`

- [x] **步骤 3.2.1：写按天分区测试**

  训练用原始采集数据在数据服务器本机 `~/btc-updown-marketmaker/datasets/` 下按 UTC day 分区保存；测试验证 server-local 根路径、partition key、manifest 和 sha256 能证明按天边界。

- [x] **步骤 3.2.2：写 paper/live 边界测试**

  测试断言 `data-collect` 只发布外部市场源 finalized mirror、coverage、health、gap 和 manifest；不生成 paper/live append-only log、rotation artifact、order evidence、NOE/ledger 或 execution feedback ingestion。

- [x] **步骤 3.2.3：运行分区与边界测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect utc_day_partition_contract -- --nocapture
  cargo test -p data-collect paper_live_boundary_excludes_runtime_trade_artifacts -- --nocapture
  ```

  预期：训练、paper/live 消费边界清晰，`data-pipeline` 才生成 labels/features/tick_stream。

  执行结果：已新增 `data-collect/src/publish.rs` 和 `data-collect/tests/partition_publish_contract.rs`。`training_utc_day_partition()` 要求 server-local absolute dataset root，使用成熟 `time` crate 按 `recv_time_utc_ms` 生成 UTC day 分区路径 `training/typed_raw/source_kind=.../utc_day=YYYY-MM-DD`，并拒绝 source kind 路径穿越。`paper_live_handoff_boundary()` 只允许 finalized typed raw parquet、manifest、source coverage、source health、source gap 和 reconnect trace 等 data-collect 外部市场源 artifact；明确禁止 paper/live append-only event log、rotation artifact、order evidence、NOE/ledger、execution feedback ingestion 和 trading decision。`cargo test -p data-collect --test partition_publish_contract -- --nocapture` 通过 3 条测试。

### 任务 3.3：atomic rename 和 finalized-only discovery

**文件：**

- 修改：`data-collect/src/publish.rs`
- 创建：`data-collect/tests/finalized_discovery_contract.rs`

- [x] **步骤 3.3.1：写 partial 排除测试**

  测试断言消费者 discovery 排除：

  - temp/running 文件。
  - `.rsync-partial`。
  - 外部同步/发布链路生成的未完成标记文件。
  - 未 finalize parquet。

- [x] **步骤 3.3.2：实现 atomic publish**

  close/finalize 后才 atomic rename；rename、sha256、row count、schema version 任一失败时 artifact verdict 为 incomplete，且不得发布为 complete。source coverage 不完整时记录 `coverage_incomplete`、source gap 和 missing reason；它限制消费者使用语义，但不得单独导致 collector 进程失败。只有 artifact 根不可写、路径越界、partial artifact 被发布或已采 segment 无法保全时才进入 `ProcessFatal`。

- [x] **步骤 3.3.3：运行 publish 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect finalized_discovery_excludes_partial_files -- --nocapture
  cargo test -p data-collect atomic_rename_failure_marks_manifest_incomplete -- --nocapture
  cargo test -p data-collect coverage_incomplete_does_not_publish_as_complete_or_stop_collector -- --nocapture
  ```

  预期：消费者只读 finalized parquet 和对应 manifest；coverage incomplete 产物可保全但不能被发现为 complete。

  执行结果：`discover_finalized_artifacts()` 只发现 finalized parquet 与对应 `.manifest.json` sidecar，排除 temp/running/digesting、`.rsync-partial`、`.partial`、`.incomplete`、无 manifest 的 parquet、garbage manifest 和 effective verdict incomplete manifest。discovery 会反序列化 `ManifestDraft`，要求 manifest effective verdict complete，且 manifest artifact path / required file 指向同一 parquet；旧的 writer-level `discover_finalized_parquet()` 已移除，避免消费者绕过 manifest sidecar 合同。`failed_atomic_publish_decision()` 将 rename/sha256/row count/schema version 等 publish 失败映射为 `ArtifactIncomplete` 和 `RequiredFile.missing_reason`，不会发布为 complete；`manifest_discoverable_as_complete()` 要求 manifest effective verdict complete。`coverage_incomplete_does_not_publish_as_complete_or_stop_collector` 证明 source coverage incomplete 会限制消费者使用，但不触发 process fatal。`cargo test -p data-collect --test finalized_discovery_contract -- --nocapture` 通过 3 条测试。

### 阶段 3 当前本地验证

本地验证通过：`cargo fmt --all`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests -- --nocapture`（75 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings` 和 release bench targets `market_discovery_probe`、`real_source_alignment_probe`、`parquet_writer_probe`、`throughput_probe`、`websocket_smoke_probe` 构建。阶段 3 第一轮独立复审为 2 个 `PASS_WITH_NOTES`、1 个 `FAIL`；修复 discovery 旁路后，针对性复审为 2 个 `PASS_WITH_NOTES` 且无阻塞项。阶段 3 完成 commit/push 后进入阶段 4。

## 九、阶段 4：runtime 韧性、bounded queue 与 ResourceWatermark

### 任务 4.1：retry/backoff+jitter 和按源熔断

**文件：**

- 修改：`data-collect/src/health.rs`
- 创建：`data-collect/tests/reconnect_trace_contract.rs`

- [x] **步骤 4.1.1：写重试和熔断测试**

  测试断言有限重试、指数 backoff + jitter、max attempts、max elapsed、backoff ceiling、retry exhausted、attempt、sleep、错误类别和最终状态全部进入 reconnect trace。

- [x] **步骤 4.1.2：实现按源独立状态机**

  Polymarket、Binance depth、Binance aggTrade、RTDS Chainlink 任一源故障不得被其他源健康状态掩盖；源级状态覆盖 open、half-open、recovered、exhausted。

- [x] **步骤 4.1.3：运行 reconnect 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test reconnect_trace_contract -- --nocapture
  ```

  预期：重连后重新校验订阅、snapshot、新鲜业务消息和 source coverage。

### 任务 4.2：bounded queue 和 backpressure 策略

**文件：**

- 修改：`data-collect/src/config.rs`
- 修改：`data-collect/src/writer.rs`
- 创建：`data-collect/tests/bounded_queue_contract.rs`

- [x] **步骤 4.2.1：写 bounded queue 合同测试**

  测试断言所有跨任务队列受 row count、byte count、oldest event age、writer lag 和 memory watermark 约束。

- [x] **步骤 4.2.2：实现 backpressure 动作**

  达到 soft watermark 时必须触发 backpressure、source health 降级或 source gap 记录；达到 hard watermark 且连续采样窗口内无法恢复时，才允许受控暂停新市场或单源 intake。禁止 unbounded queue 导致 OOM 或 silent loss，也禁止短暂水位尖峰直接造成进程级失败。
  正式实现不得复用 Phase 0 probe 的简化 verdict 语义：可恢复 queue full / bounded wait 成功必须进入 `backpressure_observations` 或等价资源观察字段，只有 send timeout、drop、持续 hard threshold 或无法保全 segment 时，才进入阻塞 complete verdict 的 `source_gap` / `ArtifactIncomplete` / `ProcessFatal` 路径。

- [x] **步骤 4.2.3：运行 bounded queue 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test bounded_queue_contract -- --nocapture
  cargo test -p data-collect backpressure_hysteresis_prevents_flapping_stop_start -- --nocapture
  ```

  预期：queue 越线可复现，且 manifest 记录 writer queue 高水位、backpressure 次数、连续采样窗口、暂停/恢复动作和是否触发 hard threshold。

### 任务 4.3：ResourceWatermark 与 hard threshold action

**文件：**

- 修改/扩展：`data-collect/src/resource.rs`
- 创建：`data-collect/tests/resource_watermark_contract.rs`

- [x] **步骤 4.3.1：写 ResourceWatermark 合同测试**

  测试覆盖需求文档第七节全部指标，并要求 threshold policy receipt 记录 soft/hard 阈值、单位、采样窗口、hysteresis、触发动作、恢复条件和结果。

- [x] **步骤 4.3.2：实现 hard threshold action**

  超过 soft 阈值时只记录降级、backpressure 和恢复观察；连续超过 hard 阈值且无法恢复时，才停止新市场采集或暂停单源 intake，保护已采数据，优先 atomic publish 和 sha256 manifest。只有无法保全已采 segment、artifact 根不可写或路径越界时，才进入 `ProcessFatal`。

- [x] **步骤 4.3.3：运行 ResourceWatermark 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test resource_watermark_contract -- --nocapture
  cargo test -p data-collect resource_watermark_soft_threshold_degrades_without_process_fatal -- --nocapture
  ```

  预期：Linux 指标不可用时记录 unavailable reason；soft threshold 不触发进程级失败；live 交易小主机不会被计划为编译或重处理机器。

Phase 4 执行结果：

1. 已新增 `data-collect/tests/reconnect_trace_contract.rs`、`data-collect/tests/bounded_queue_contract.rs`，并扩展 `data-collect/tests/resource_watermark_contract.rs` 与 manifest 合同测试。TDD RED 先失败于缺少 `ErrorClass`、reconnect trace 字段、`config` 模块、backpressure receipt、recoverable wait observation、`ResourcePolicyMonitor` 和 resume 动作。
2. 已实现 `data-collect/src/config.rs`、扩展 `health.rs` / `backpressure.rs` / `resource.rs` / `manifest.rs`：reconnect trace 记录 error class、backoff ceiling、final state 和重连后 subscription/snapshot/business freshness/coverage recheck requirement；每个 source 的 circuit state 和 failure 独立保存；cross-task queue config 必须有 rows/bytes/age/writer lag 上限；recoverable bounded wait 进入 backpressure receipt，不进入 source gap；send timeout/drop 才记录 source gap；hard watermark 需要连续样本触发 pause，恢复需要健康样本触发 resume；manifest complete 需要 backpressure receipt。
3. 成熟工具评估：`cargo info backoff` 显示 `backoff 0.4.0` 支持 exponential backoff 和 tokio feature，且 `cargo tree -p data-collect` 显示它已通过官方 Polymarket Rust SDK 作为传递依赖存在；`cargo info tokio-retry` 显示 `tokio-retry 0.3.2` 可用。Phase 4 当前实现是可审计 trace/policy 合同和状态 receipt，不是实际 sleep executor；为避免增加第二套 retry 执行路径和隐藏证据字段，本阶段不新增直接 retry crate。后续真实 source loop 若需要执行重试 sleep，应优先复用 SDK/backoff 能力或在 changelog 记录直接依赖取舍。
4. 修正本阶段计划命令：`cargo test -p data-collect reconnect_trace_contract` / `bounded_queue_contract` / `resource_watermark_contract` 会按测试名过滤并可能执行 0 条测试；已改为 `cargo test -p data-collect --test ... -- --nocapture`，并用正确命令重新验证。
5. 第一轮 Phase 4 独立复审为 3 个 `FAIL`，无 Critical。成立问题已修复：`ReconnectGate` 增加 source coverage gate；`ResourcePolicy::evaluate()` 收敛为无状态分类/观察，不再单次 hard threshold 停采；`ResourcePolicyMonitor` 只在首次进入 hard-active 时触发 finalize / incomplete manifest，持续 hard 只保持 pause + observation；`record_queue_full()` 改为可恢复 backpressure observation，不写 source gap；manifest complete 要求 `BackpressureReceipt` 覆盖所有 `source_coverage.source_kind`，且 `send_timeout_count` / `drop_count` / `pause_intake_count` / `process_fatal` 均为 0；manifest complete 新增 `ResourceThresholdPolicyReceipt` 条件；cross-task queue config 增加 `max_memory_watermark_bytes` 且要求 bytes 上限不超过 memory watermark。
6. 针对性复审结果：1 个 targeted reviewer 先给 `FAIL`，指出重连时旧 business timestamp 可能泄漏到新连接；已修复 `SourceHealthTracker::record_connected()` 清空旧 pong/business，并新增 `reconnect_recovery_does_not_reuse_business_message_from_previous_connection` 反例测试。另 1 个 targeted reviewer 为 `PASS_WITH_NOTES`，仅剩 minor：Phase 0 技术验证文档 queue full 旧语义、manifest 多源 coverage 反例缺失；已同步文档并新增 `manifest_backpressure_receipt_must_cover_all_source_coverage_kinds`。
7. 本地指定验证通过：`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --test reconnect_trace_contract -- --nocapture`（4 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --test bounded_queue_contract -- --nocapture`（5 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --test resource_watermark_contract -- --nocapture`（7 条测试）。命名单测过滤验证也通过：`backpressure_hysteresis_prevents_flapping_stop_start`（1 条）和 `resource_watermark_soft_threshold_degrades_without_process_fatal`（1 条）。
8. 最终窄范围复审为 `PASS_WITH_NOTES`，无 Critical/Important blocker；notes 已修复：新增旧 pong 不可复用反例测试，并修正 Phase 0 技术验证文档残留旧 backpressure 测试名。
9. 本地全量验证通过：`cargo fmt --all -- --check`、`git diff --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests --quiet`（89 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings` 和 release bench targets `market_discovery_probe`、`real_source_alignment_probe`、`parquet_writer_probe`、`throughput_probe`、`websocket_smoke_probe` 构建。进入下一阶段前仍需完成最终状态检查、commit/push。

## 十、阶段 5：消费者边界与接入合同

### 任务 5.1：训练消费者接入边界

**文件：**

- 创建：`data-collect/tests/training_consumer_boundary_contract.rs`
- 修改：`data-collect/src/manifest.rs`

- [x] **步骤 5.1.1：写训练边界测试**

  测试断言训练消费者只能读取按 UTC day 分区的 finalized typed raw parquet、manifest、source coverage 和后续 `data-pipeline` 生成的 labels/features/tick_stream。

- [x] **步骤 5.1.2：实现 manifest 中的训练边界声明**

  manifest 可以声明 source coverage 和 finalized mirror，不提供 labels、features、tick_stream、canonical split 或 T-30 样本加工。

- [x] **步骤 5.1.3：运行训练边界测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test training_consumer_boundary_contract -- --nocapture
  ```

  预期：采集成功不等于训练批准，finalized parquet 完整不等于 labels/features/tick_stream 已完成。

  执行结果：已新增 `ConsumerBoundaryDeclaration`、`ConsumerBoundaryKind`、`training_consumer_boundary_declaration()` 和 `required_consumer_boundary_declarations()`；complete manifest 现在必须包含 canonical 训练与 paper/live 边界声明，否则 `effective_verdict()` 为 incomplete。`cargo test -p data-collect --test training_consumer_boundary_contract -- --nocapture` 通过 4 条测试。

### 任务 5.2：paper/live 消费者接入边界

**文件：**

- 创建：`data-collect/tests/paper_live_boundary_contract.rs`
- 修改：`data-collect/src/manifest.rs`

- [x] **步骤 5.2.1：写 paper/live 非范围测试**

  测试断言 `data-collect` 不生成或解释：

  - paper/live append-only log。
  - rotation artifact。
  - live artifacts。
  - order evidence。
  - NormalizedOrderEvent。
  - ledger。
  - execution feedback ingestion。
  - 交易决策、风控、submit/cancel path。

- [x] **步骤 5.2.2：实现接入边界声明**

  `data-collect` 只为 paper/live 提供外部市场源事件、timestamp namespace、coverage、health/gap、reconnect/backpressure 状态、manifest 和 hot path handoff 边界证据；paper/live Market Context Builder / runtime 必须通过 realtime handoff、bounded channel、IPC、local stream 或等价低延迟事件流消费外部市场源事件，不得把 finalized parquet 当作 paper/live 热路径唯一事实来源。paper/live 热路径延迟必须来自对应 paper/live 主机 artifact，不能使用非爱尔兰采集主机 `recv_time_utc` 冒充。

- [x] **步骤 5.2.3：运行 paper/live 边界测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test paper_live_boundary_contract -- --nocapture
  ```

  预期：计划和实现不把 paper/live 交易逻辑写入 `data-collect`；hot path handoff 边界可验证，但 paper/live append-only log、NOE/ledger、模型输入组装和 execution feedback 仍由对应模块承接。

  执行结果：已新增 `paper_live_consumer_boundary_declaration()`，声明 `data-collect` 只提供外部市场源事件、timestamp namespace、coverage、health/gap、reconnect/backpressure、manifest 与 realtime handoff 边界证据；paper/live runtime receive timestamp、append-only log、Market Context Builder 与 NOE/ledger 属于外部角色，交易决策、风控、submit/cancel path 等属于 forbidden data-collect roles。第一轮独立审查为 NO-GO 后，已收紧 manifest complete gate：consumer boundary declarations 必须恰好为 canonical Training + PaperLive 两项，不允许额外冲突声明；publish boundary 已补齐 `backpressure_state`、`realtime_handoff_boundary_evidence` 和 paper/live forbidden aliases。旧 manifest 缺少 consumer boundary declaration 会被判 `ArtifactIncomplete`，这是 Phase 5 后的消费者门禁，不是 collector process fatal。`cargo test -p data-collect --test paper_live_boundary_contract -- --nocapture` 通过 4 条测试。

Phase 5 执行结果：

1. 本地全量验证通过：`cargo fmt --all -- --check`、`git diff --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests --quiet`（97 条测试）、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings` 和 release bench targets `market_discovery_probe`、`real_source_alignment_probe`、`parquet_writer_probe`、`throughput_probe`、`websocket_smoke_probe` 构建。
2. 第一轮独立审查为 3 个 NO-GO/FAIL，无 Critical；成立问题已修复：manifest consumer boundary gate 不再允许缩水/冲突声明，paper/live publish boundary 与 manifest/demand role 表已对齐。
3. 针对性复审结果为 2 个 `PASS_WITH_NOTES`、1 个 `PASS`，无 Critical/Important blocker，允许进入 commit/push gate。

## 十一、阶段 6：端到端验收与交付

### 任务 6.1：端到端采集 dry run

**文件：**

- 创建：`data-collect/tests/e2e_collect_contract.rs`
- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [x] **步骤 6.1.1：运行短窗口四源采集 dry run**

  计划执行命令：

  ```bash
  cargo test -p data-collect --test e2e_collect_contract -- --nocapture
  ```

  预期：四源均生成 typed raw parquet、source coverage、source health、source gap、reconnect trace 和 manifest；artifact finalized-only。

  执行结果：新增 `data-collect/tests/e2e_collect_contract.rs`，本地 synthetic 四源 dry run 写出四个 typed raw parquet、manifest sidecar、health/gap/reconnect evidence，验证 discovery 只发现 finalized parquet；同一测试也验证 current active 与 planned next preload 的四源 coverage 对齐。`cargo test -p data-collect --test e2e_collect_contract -- --nocapture` 通过 1 条测试。

  审查修复：第一轮 Phase 6 审查指出 e2e coverage report 曾独立于写出的 parquet/manifest 构造，存在假阳性；已改为 manifest sidecar 写入 current active 与 planned next preload 两段 coverage，test 通过 finalized discovery 读回 sidecar 后再构造 scheduler report。同时加入 running / rsync-partial fixture，确认 discovery 只返回 finalized parquet + complete manifest。针对性复审又指出 manifest `end_time_utc_ms` 未覆盖 preload coverage；已修成 end time 覆盖 max source coverage，并在读回 sidecar 后断言 manifest start/end 包住全部 `source_coverage`。

- [x] **步骤 6.1.2：运行长窗口 soak test**

  soak test 至少覆盖一个完整 `btc-updown-5m` market 与下一市场 `T-30s` prefetched window。

  预期：manifest 可证明 current -> retired、prefetched -> active、coverage window、first business event、subscription confirmation evidence 和 source freshness。

  执行结果：Phase 6 不重复启动数据服务器长跑；复核 Phase 0 已完成真实远端证据：0.6.10 真实四源 60 分钟 run、targeted rollover run、0.6.11 hot/durable path 60 分钟 run 和 0.6.12 collector region smoke。Phase 6 新增 `data-collect/docs/phase0-technical-validation.md` 12.12 交叉引用，说明本地 e2e contract 是交付核对，不替代 Phase 0 真实四源远端长跑。

### 任务 6.2：需求验收矩阵复核

**文件：**

- 创建：`data-collect/docs/acceptance-matrix.md`
- 修改：`changelog/data-collect-changelog.md`

- [x] **步骤 6.2.1：逐条映射需求验收**

  `data-collect/docs/acceptance-matrix.md` 必须逐条映射 `demand/data-collect-demand.md` 第九节 24 个验收标准到测试、artifact 或报告。

  执行结果：已新增 `data-collect/docs/acceptance-matrix.md`，逐条映射 24 个验收标准到本地合同测试、Phase 0 远端 artifact/report、Phase 6 e2e contract 或非范围扫描。审查后补正第 14 条、第 21 条和第 23 条证据口径：paper/live runtime receive timestamp 属于外部 runtime artifact；SDK 能力核对以 `polymarket-sdk-gap-report.md` 和 Phase 0 §6 为主；Phase 0 WebSocket smoke 只作为 reachability evidence。

- [x] **步骤 6.2.2：检查非范围没有泄漏**

  计划执行命令：

  ```bash
  rg -n "labels|features|tick_stream|canonical split|purged|embargo|reward|PnL|NAV|ledger|NormalizedOrderEvent|submit|cancel|manual signing|shadow implementation" data-collect/src data-collect/tests data-collect/docs
  ```

  预期：命中只能出现在边界声明、非范围测试或 SDK 缺口说明中；不得出现生产逻辑实现。

  执行结果：扫描命中均位于文档、边界声明、非范围测试、SDK 缺口说明，或 Polymarket metadata `reward_raw` 原始字段；未发现 `data-collect` 生产逻辑生成 labels/features/tick_stream、NOE/ledger、submit/cancel、manual signing 或 shadow implementation。

- [x] **步骤 6.2.3：运行最终验证**

  计划执行命令：

  ```bash
  cargo fmt --all -- --check
  git diff --check
  CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings
  CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests --quiet
  CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo bench -p data-collect --bench throughput_probe -- --sample-size 20
  ```

  预期：格式、lint、测试和吞吐 bench 均通过；若 bench 未达到需求基线，不能宣称完成。

  执行结果：第二轮审查修复后重新验证通过：`cargo fmt --all -- --check`、`git diff --check`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo clippy -p data-collect --all-targets -- -D warnings`、`CARGO_TARGET_DIR=/tmp/btc-updown-marketmaker-target cargo test -p data-collect --lib --tests --quiet`（98 条测试）均通过。`throughput_probe` bench 通过：`polymarket_p95_window` 302000 rows，`events_per_sec_total=1703182.50`，`finalize_failures=0`；`polymarket_max_window` 433000 rows，`events_per_sec_total=1771398.44`，`finalize_failures=0`。

### 任务 6.3：阶段审查、changelog 和交付 gate

**文件：**

- 修改：`changelog/data-collect-changelog.md`

- [x] **步骤 6.3.1：自审 diff 与验证输出**

  检查每一行改动是否直接追溯到 `demand/data-collect-demand.md`；记录测试命令、退出码、artifact 路径、sha256 和剩余风险。

  执行结果：自审确认 Phase 6 修改集中在 `data-collect/tests/e2e_collect_contract.rs`、`data-collect/docs/acceptance-matrix.md`、`data-collect/docs/phase0-technical-validation.md`、`plan/data-collect-plan.md` 和 `changelog/data-collect-changelog.md`；没有改生产采集逻辑，没有把无关未跟踪资料纳入。验证输出已记录在本节与 changelog。

- [x] **步骤 6.3.2：派生独立审查**

  按仓库规则在阶段完成后派生子代理审查，检查需求覆盖、正确性、边界、过度设计、遗漏、测试和与 `rebuild-core-demand` 的一致性。

- [x] **步骤 6.3.3：修复审查反馈或请求用户决策**

  可执行且在范围内的问题修复后重新审查；如果反馈要求改变 SDK-first、服务器角色、吞吐验收、消费者边界或非范围，必须先请求用户决策。

  执行结果：第一轮 Phase 6 审查为 1 个 NO-GO、1 个 NEEDS_DOC_FIXES、1 个 PASS_WITH_NOTES，无 Critical；成立问题均已修复并复审。最终复审结果为 `PASS`、`PASS`、`PASS_WITH_NOTES`，无 Critical/Important blocker，允许进入 commit/push gate。

## 十二、方案不一致决策门

执行中出现以下任一计划级不一致或 fatal 条件，必须停止当前方向，记录事实、影响、可选项和推荐选项，并让用户决定。普通 `source gap`、`stale`、`coverage drop`、单个 artifact incomplete 或可恢复 backpressure 属于运行时质量/覆盖信号，按“运行时校验分级与失败动作”记录和恢复，不得直接升级为本决策门：

1. 官方 Polymarket Rust SDK 与阶段 0 能力报告不一致，或不能覆盖 market channel、RTDS、market discovery、metadata 必要能力。
2. Binance 官方 WS/REST、Polymarket RTDS 或 Polymarket market channel 当前行为与需求文档字段/订阅语义不一致。
3. 阶段 0 或持续 soak evidence 证明数据采集服务器无法达到四源合并 `2k-5k events/s` 或 ResourceWatermark 基线；单次 spike、短窗口 source gap 或可恢复 backpressure 不触发本项。
4. live 交易小主机被证明无法承受计划中的运行边界，或任何步骤需要在 live 服务器编译 Rust、跑 cargo test/check/fmt、训练式重处理或历史全量 parquet 扫描。
5. 为满足 writer、manifest、atomic rename、source coverage 或 health/gap/reconnect 需求，需要扩大到 archive/upload/listing/gzip 或对象存储完整性。
6. 实现需要把 labels、features、tick_stream、训练切分、T-30 样本加工、paper/live append-only log、order evidence、NOE/ledger、execution feedback ingestion 或交易逻辑写进 `data-collect`。
7. 测试或 runtime 结果系统性显示需求验收标准不成立，且不能通过本计划范围内的分级降级、重试、gap 记录、manifest incomplete 或 source-level 恢复满足验收。

## 十三、最小验证命令清单

计划文档自身验证：

```bash
rg -n "/home/chillchill/btc-updown-marketmaker|~/btc-updown-marketmaker|datasets|路径执行口径|data_collect_run_root|data_collect_artifact_root|阶段推进通用 gate|数据格式合同|运行时校验分级|ProcessFatal|ArtifactIncomplete|SourceDegraded|BackpressureDegraded|hysteresis|source_format_contract|consumer_format_compatibility_contract|demand/data-collect-demand.md|阶段 0|技术测试|性能验证|远端 Phase 0|30-60 分钟|5-10 分钟|2-4 小时|真实网络 reachability|market_discovery_probe|real_source_alignment_probe|custom_feature_enabled|PASS_TO_PHASE1_REVIEW|2k-5k events/s|bounded queue|backpressure|ResourceWatermark|atomic rename|source gap|recv_time_utc|Polymarket market channel|Binance BTCUSDT aggTrade|RTDS Chainlink|SDK 缺口|用户审查" plan/data-collect-plan.md changelog/data-collect-changelog.md
rg -n "[T]ODO|[T]BD|待[补]|以后再[填]|[p]laceholder" plan/data-collect-plan.md changelog/data-collect-changelog.md
awk '/[ \t]$/ { print FILENAME ":" FNR ": trailing whitespace"; bad=1 } END { exit bad }' plan/data-collect-plan.md changelog/data-collect-changelog.md
```

后续实现完成前最低验证：

```bash
cd /home/chillchill/btc-updown-marketmaker
cargo fmt --check -p data-collect
cargo clippy -p data-collect --all-targets -- -D warnings
cargo test -p data-collect --lib --tests -- --nocapture
cargo bench -p data-collect --bench throughput_probe -- --sample-size 20
```

## 十四、需求覆盖索引

| 需求第九节编号 | 需求主题 | 覆盖阶段 / 任务 | 证明方式 |
| --- | --- | --- | --- |
| 1 | `./data-collect/` 职责边界 | 阶段 1、5、6 | event envelope、消费者边界测试、非范围泄漏扫描 |
| 2 | `recv_time_utc` 与 Polymarket platform `timestamp` 同时保留 | 任务 1.1、2.1、6.1 | event envelope contract、Polymarket contract、e2e dry run |
| 3 | 非爱尔兰采集主机延迟不冒充 paper/live 爱尔兰延迟 | 任务 1.1、5.2 | timestamp namespace、paper/live boundary contract |
| 4 | active market、T-30s prefetched next market、rollover 边界 | 任务 2.0、2.1、6.1 | source scheduler、Polymarket T-30、soak test |
| 5 | `new_market`、metadata、condition/token/asset id、lifecycle 可追溯 | 任务 1.2、2.1 | source format contract、Polymarket metadata contract |
| 6 | candidate slug + active scan / metadata snapshot 双路径 | 任务 2.1 | discovery active scan/metadata snapshot 测试，slug-only incomplete 负例 |
| 7 | 四源逐源 coverage / health / gap / reconnect | 任务 1.2、2.0、2.1、2.2、2.3、6.1 | source format contract、source scheduler、e2e four source contract |
| 8 | `market_resolved` / retired predicate | 任务 2.1 | retired predicate 字段级事件路径与 fallback metadata 路径测试 |
| 9 | `custom_feature_enabled` 与订阅确认证据 | 任务 0.2、1.2、2.1 | SDK gap report、source format contract、Polymarket negative tests |
| 10 | Binance official WS/REST、depth stitch、aggTrade 字段 | 任务 1.2、2.2 | source format contract、Binance depth stitch、aggTrade raw fields test |
| 11 | running/temp parquet、finalize、atomic rename、source kind metadata | 任务 0.3、3.1、3.3 | writer publish contract、parquet schema metadata、finalized-only discovery |
| 12 | manifest 字段、`required_files[]`、complete/incomplete verdict | 任务 0.3、1.3、3.3 | manifest contract、atomic rename failure test |
| 13 | paper/live runtime artifact 非范围 | 任务 3.2、5.2、6.2 | paper/live boundary tests、非范围扫描 |
| 14 | 训练 cold path 与 paper/live hot path 接入边界 | 任务 0.6.11、5.1、5.2、6.2 | hot path / parquet path latency probe、training boundary、paper/live boundary tests |
| 15 | 网络异常、空返回、业务静默、rate limit、stale | 任务 0.5、1.4、4.1、运行时校验分级 | network recovery、health/gap contract、reconnect trace、source degraded 非进程级失败测试 |
| 16 | 有限重试、backoff、jitter、按源独立熔断/恢复 | 任务 0.5、4.1 | retry/circuit breaker/reconnect trace tests |
| 17 | reconnect trace attempt/sleep/error/final state | 任务 1.4、4.1 | health/gap schema、reconnect trace contract |
| 18 | Rust async/non-blocking、blocking pool、bounded queue、backpressure | 阶段 0、3、4、运行时校验分级 | blocking isolation、bounded queue、backpressure hysteresis tests |
| 19 | 性能验收、`2k-5k events/s`、writer queue、ResourceWatermark、hardware tier、hot/durable latency | 任务 0.4、0.6、4.2、4.3、6.2 | throughput bench、本地/远端 hardware validation、0.6.11 hot/parquet latency run、ResourceWatermark soft/hard threshold contract |
| 20 | 真实 `btc-updown-5m` Phase 0 discovery、rollover、四源长跑、时间线对齐和 hot/durable path 验证 | 任务 0.6.9、0.6.10、0.6.11、0.7.2 | market discovery receipt、real-source long-run manifest、hot/parquet latency summary、coverage/resource/gap evidence、远端 verdict |
| 21 | ResourceWatermark hard threshold policy receipt | 任务 0.6、4.3、运行时校验分级 | threshold policy receipt、soft/hard threshold action tests |
| 22 | archive/upload/listing/gzip 非范围 | 路径执行口径、第十二节决策门、6.2 | 非范围声明、决策门、非范围泄漏扫描 |
| 23 | Polymarket Rust SDK-first 和 SDK 缺口审批 | 任务 0.2、2.1、第十二节决策门 | SDK gap report、SDK-first adapter gate |
| 24 | 不把 data-pipeline、paper/live 交易逻辑、SDK 旁路写进 data-collect | 任务 1.2、5.1、5.2、6.2 | consumer compatibility contract、training/paper/live boundary tests、非范围扫描 |

## 十五、数据格式与消费者兼容性检查

| 消费方 | 是否直接消费 `data-collect` typed raw parquet | 必须满足的数据格式 | 不允许的误用 |
| --- | --- | --- | --- |
| `data-pipeline` | 是，作为外部市场源 typed raw mirror 输入 | 逐源 parquet source kind、parseable `raw_json`、schema version、source timestamp、`recv_time_utc`、source coverage、manifest、UTC day 分区 | 把 `data-collect` 当成 labels/features/tick_stream/训练切分生成器 |
| 训练 | 不直接把 raw parquet 当训练样本；通过 `data-pipeline` 后消费 | `data-pipeline` 生成 labels/features/tick_stream/split 后，训练再消费；raw parquet 完整只证明外部源可处理 | parquet 完整即训练批准、source coverage 完整即 label 完整 |
| paper | hot path 消费 realtime handoff 或等价低延迟事件流；cold mirror 只能作外部市场源证据、coverage、manifest | 与训练/live 共用 source kind、schema version、timestamp namespace；paper runtime receive timestamp 由 paper 主机 artifact 提供 | 用非爱尔兰 collector `recv_time_utc` 冒充 paper runtime receive timestamp，用 finalized parquet 作为 paper 热路径唯一事实来源，或用 parquet 替代 append-only log |
| live | hot path 消费 realtime handoff 或等价低延迟事件流；cold mirror 只能作外部市场源证据、coverage、manifest | 与训练/paper 共用 source kind、schema version、timestamp namespace；live runtime receive timestamp 由 live 主机 artifact 提供 | 在 live 小主机编译/重处理，用 finalized parquet 作为 live 热路径唯一事实来源，或用 `data-collect` 生成交易决策、NOE/ledger、execution feedback label |
