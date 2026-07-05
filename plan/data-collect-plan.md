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

> 状态：待用户审查。本文档只制定模块计划、验证 gate 和后续执行步骤；不授权代码执行、部署、paper、live、真实下单、撤单或资金操作。

## 零、路径执行口径

1. 当前本地开发、文档落盘、代码编写、本地构建、`cargo` 测试和 bench 的仓库是 `/home/chillchill/btc-updown-marketmaker`；本文档、对应需求和 changelog 也在该仓库维护。
2. 各远端服务器上的源码/工作目录统一写作 `~/btc-updown-marketmaker`；在 `ubuntu` 用户服务器上该路径通常解析为 `/home/ubuntu/btc-updown-marketmaker`，但计划、命令和 changelog 不得把展开后的远端路径误写成本地开发路径。
3. 下文所有 `data-collect/...` 代码、测试、bench 和运行文档路径，除非明确写成 `demand/`、`plan/`、`changelog/`，均表示本地 `/home/chillchill/btc-updown-marketmaker/data-collect/...` 下的实现路径；远端验证时对应服务器 `~/btc-updown-marketmaker/data-collect/...`。
4. 下文命令块中的 `cargo ...` 为可读性省略了工作目录；本地执行时必须以 `/home/chillchill/btc-updown-marketmaker` 为 workdir。只有在明确进入数据采集服务器验证时，才允许在该服务器 `~/btc-updown-marketmaker` 下执行；不得在 AWS live-trade 小主机上运行。
5. AWS 爱尔兰 live-trade 服务器只允许低影响运行边界核对和已构建二进制运行；不得在该服务器运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`、训练式重处理、历史全量 parquet 扫描或归档压缩。
6. 阶段 0 必须产出 `data_collect_run_root`、`data_collect_artifact_root`、训练 UTC day 分区根、paper/live handoff artifact 根和权限边界 receipt；本文不发明最终数据服务器落盘根路径，receipt 未经用户审查通过时 verdict 必须是 `BLOCKED_NEEDS_DECISION`。
7. 所有运行根和 artifact 根必须是绝对路径，必须解析在用户批准的根目录内；路径越界、权限不匹配、owner/group 不符合 receipt、或解析到 `/home/ubuntu/live-trade` 非授权子路径时必须 fail closed。

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

## 二、前置成功标准

正式开发前必须先通过阶段 0。阶段 0 不通过时，不进入阶段 1-6。

阶段 0 通过标准：

1. 官方 Polymarket Rust SDK 能力核对完成，明确 market channel、RTDS、market discovery、metadata 是否覆盖需求；缺口已经形成报告并获得用户批准，或不存在阻塞缺口。
2. 技术测试证明四源合并 `2k-5k events/s` 的解析、排队、写入、health/gap 统计路径在目标 hardware tier 上可达到需求基线，或明确给出达不到时的瓶颈和备选方案。
3. 技术测试证明 parquet running/temp segment、close/finalize、sha256、manifest、required_files[] 和 atomic rename 发布路径可恢复、可校验、不会被消费者读取 partial artifact。
4. 技术测试证明长时间无业务消息、ping/pong 正常但数据静默、rate limit、server shutdown、snapshot 缺失、source stale、writer queue 堵塞和资源水位越线都会写入 source health、source gap 或 reconnect trace，并按“运行时校验分级与失败动作”处理；普通 gap/stale 不得默认触发 collector 进程退出。
5. 数据采集服务器和交易服务器分别记录 hardware tier；live 交易小主机只做运行边界评估，不承担 Rust 编译、训练式重处理、历史全量 parquet 扫描或归档压缩。
6. ResourceWatermark 技术测试覆盖 RSS、CPU、disk free、artifact dir bytes、journal/log bytes、journal 限额状态、open files、queue bytes、queue age、parquet writer memory estimate、cgroup memory、PSI、anon/file/slab、stage latency / TTL 和 unavailable reason。
7. 阶段 0 产物写入后续 changelog，包含命令、commit 或源码快照、artifact 路径、sha256、硬件 tier、通过/失败 verdict、剩余风险和用户决策点。

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
6. 阶段 0 额外要求用户审查通过；`PASS_WITH_LIMITS` 或 `BLOCKED_NEEDS_DECISION` 不得自动进入阶段 1。

## 五、阶段 0：技术测试与性能验证

本阶段是正式开发前的硬 gate。目标是先证明技术路径可靠，尤其是数据采集服务器和交易服务器性能/角色边界可承受，再继续推进开发。

### 任务 0.1：冻结验证范围和运行边界

**文件：**

- 创建：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [ ] **步骤 0.1.1：记录阶段 0 验证矩阵**

  在 `data-collect/docs/phase0-technical-validation.md` 写入验证矩阵，至少包含：

  - Polymarket Rust SDK market channel、RTDS、market discovery、metadata 覆盖情况。
  - Polymarket `btc-updown-5m` market channel、Binance `BTCUSDT` depth、Binance `BTCUSDT` aggTrade、RTDS Chainlink `btc/usd` 四源。
  - `recv_time_utc`、source timestamp、collector host/region、raw_json、typed raw parquet、source coverage、source health、source gap、reconnect trace、manifest。
  - `2k-5k events/s` 四源合并吞吐、Polymarket p95/max 事件量、writer queue、flush latency、ResourceWatermark。
  - 数据采集服务器 `ubuntu@3.255.156.183` 与 AWS 爱尔兰 live-trade 服务器 `ubuntu@108.130.74.143` 的 hardware tier；live 服务器只允许低影响运行边界探测，不允许 Rust 编译或训练式重处理。
  - `data_collect_run_root`、`data_collect_artifact_root`、训练 UTC day 分区根、paper/live handoff artifact 根、owner/group、权限模式、路径越界 fail-closed 行为；路径未获用户批准时阶段 0 verdict 必须是 `BLOCKED_NEEDS_DECISION`。

- [ ] **步骤 0.1.2：运行只读环境核对**

  计划执行命令：

  ```bash
  git status --short -- demand/data-collect-demand.md plan/data-collect-plan.md changelog/data-collect-changelog.md
  find data-collect -maxdepth 3 -type f -printf '%p\n' | sort
  ```

  预期：确认当前文档状态和 `data-collect/` 初始文件状态；若已有实现文件与本计划候选结构冲突，停止并记录冲突。

- [ ] **步骤 0.1.3：写入阶段 0 决策门**

  在阶段 0 报告和 changelog 中记录：阶段 0 未通过前不得创建正式 collector runtime、不得部署、不得接入 paper/live、不得把未验证技术路径写成生产实现。

- [ ] **步骤 0.1.4：生成运行根与 artifact 根 receipt**

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

  预期：receipt 中任何路径缺失、相对路径、权限不可验证、解析到未授权根、或试图把 live 交易小主机当作构建/重处理主机时，阶段 0 verdict 为 `BLOCKED_NEEDS_DECISION`。

### 任务 0.2：Polymarket 官方 Rust SDK 能力核对

**文件：**

- 创建：`data-collect/docs/polymarket-sdk-gap-report.md`
- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [ ] **步骤 0.2.1：核对 SDK 覆盖范围**

  检查官方 Polymarket Rust SDK 是否支持：

  - market channel 订阅，且能传递官方字段 `assets_ids`、`type: "market"`、`custom_feature_enabled: true`。
  - `book`、`price_change`、`last_trade_price`、`best_bid_ask`、`tick_size_change`、`new_market`、`market_resolved` 业务事件。
  - market discovery / metadata 查询，能追溯 condition id、token id / asset id、CLOB market id、slug、start/end、active/acceptingOrders、tick size、min size、fee/reward。
  - RTDS Chainlink `btc/usd` subscription，能追溯 `action: "subscribe"`、`topic: "crypto_prices_chainlink"`、`type: "*"` 和 symbol filter。

- [ ] **步骤 0.2.2：生成 SDK gap verdict**

  写入 `data-collect/docs/polymarket-sdk-gap-report.md`：

  - `PASS`：SDK 覆盖需求，可继续技术测试。
  - `BLOCKED_SDK_GAP`：SDK 不覆盖必要能力；必须列出缺口、影响范围、可选方案、风险和需要用户批准的旁路范围。
  - `NEEDS_USER_DECISION`：资料不充分或 SDK 行为不确定；不得自行切换 raw REST、manual signing、自建 SDK 或 shadow implementation。

- [ ] **步骤 0.2.3：验证 SDK-first 边界**

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

- [ ] **步骤 0.3.1：实现最小 writer probe**

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

- [ ] **步骤 0.3.2：验证 running/temp segment**

  计划执行命令：

  ```bash
  cargo test -p data-collect writer_running_file_is_not_discoverable -- --nocapture
  cargo test -p data-collect writer_finalize_atomic_rename_publishes_only_finalized -- --nocapture
  ```

  预期：`*.tmp`、`*-running.parquet`、`.rsync-partial` 不进入 discovery；close/finalize 后通过 atomic rename 发布 finalized `.parquet`。

- [ ] **步骤 0.3.3：验证 manifest 完整性**

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

- [ ] **步骤 0.4.1：建立吞吐基线 probe**

  使用合成四源事件模拟：

  - Polymarket p95 约 302k events / 5.5 分钟窗口。
  - Polymarket max 约 433k events / 5.5 分钟窗口。
  - 四源合并 `2k-5k events/s` 解析、排队、写入和 health 统计。

- [ ] **步骤 0.4.2：验证 bounded queue 和 backpressure**

  计划执行命令：

  ```bash
  cargo test -p data-collect bounded_queue_records_backpressure_and_source_gap -- --nocapture
  cargo test -p data-collect queue_watermarks_include_rows_bytes_age_writer_lag -- --nocapture
  cargo bench -p data-collect throughput_probe -- --sample-size 20
  ```

  预期：所有跨任务队列都是 bounded queue；row count、byte count、oldest event age、writer lag 和 memory watermark 越线时，先按 soft/hard threshold、连续采样窗口和 hysteresis 写入 backpressure、source health 降级或 source gap；只有持续 hard threshold 且无法通过 backpressure 恢复时，才受控暂停新市场或单源 intake，并保护已采 segment；无 unbounded queue、silent loss 或短暂尖峰导致的进程级频繁失败。

- [ ] **步骤 0.4.3：记录通过/失败阈值**

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

- [ ] **步骤 0.5.1：覆盖异常场景**

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

- [ ] **步骤 0.5.2：验证恢复策略**

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

- [ ] **步骤 0.6.1：数据采集服务器低影响资源核对**

  计划执行只读/低影响命令：

  ```bash
  ssh ubuntu@3.255.156.183 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
  ```

  预期：记录 data collection server hardware tier、CPU、内存、磁盘、open files、PSI 可用性；不得启动生产采集或重处理。

- [ ] **步骤 0.6.2：live 交易服务器只做运行边界核对**

  计划执行只读命令：

  ```bash
  ssh ubuntu@108.130.74.143 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
  ```

  预期：记录 live trade server hardware tier；确认该主机不运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`、训练式重处理、历史全量 parquet 扫描或归档压缩。

- [ ] **步骤 0.6.3：验证 ResourceWatermark schema**

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
	  cargo test -p data-collect hard_threshold_records_degrade_stop_and_finalize_actions -- --nocapture
	  cargo test -p data-collect soft_threshold_records_degraded_without_process_fatal -- --nocapture
	  ```

  预期：soft threshold 只记录降级、backpressure 和恢复观察；连续 hard threshold 越线时记录停止新市场采集、保护已采数据、优先 finalize/sha256/manifest 的动作与结果；不得因短暂 resource spike 产生进程级频繁失败。

### 任务 0.7：阶段 0 审查与用户决策

**文件：**

- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [ ] **步骤 0.7.1：汇总阶段 0 verdict**

  阶段 0 报告必须给出以下 verdict 之一：

  - `PASS_TO_DESIGN`：SDK、吞吐、writer、网络、ResourceWatermark 和服务器角色全部满足进入正式开发条件。
  - `PASS_WITH_LIMITS`：技术路径局部可行但存在限制；必须列出限制、阈值和后续阶段必须修复的风险，并经用户明确批准后才能进入阶段 1。
  - `BLOCKED_NEEDS_DECISION`：存在 SDK 缺口、性能不可达、服务器资源不匹配或需求边界冲突，必须由用户决定。

- [ ] **步骤 0.7.2：用户审查 gate**

  阶段 0 未经用户审查通过，不进入阶段 1。若 verdict 不是 `PASS_TO_DESIGN`，不得自行扩大范围、切换 SDK 旁路、降低吞吐验收或把 live 小主机改成构建/重处理机器。

## 六、阶段 1：合同与 schema 冻结

### 任务 1.1：定义 event envelope 合同

**文件：**

- 创建：`data-collect/src/event.rs`
- 创建：`data-collect/tests/event_envelope_contract.rs`

- [ ] **步骤 1.1.1：写 event envelope contract 测试**

  测试必须断言每条源事件都包含：

  - `source_kind`
  - `source_event_type`
  - `source_identifier`
  - `source_ts` 或 no-source-timestamp 证据
  - `recv_time_utc`
  - `collector_host_id`
  - `collector_region`
  - `host_clock_sync_status`
  - `raw_json` 或等价原始证据引用

- [ ] **步骤 1.1.2：实现最小 envelope 类型**

  实现只承载采集证据；不得加入 label、feature、tick_stream、reward、PnL、NAV、ledger 或模型输入字段。

- [ ] **步骤 1.1.3：运行合同测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect event_envelope_requires_recv_time_and_source_timestamp_boundary -- --nocapture
  ```

  预期：缺少 `recv_time_utc`、collector host/region 或 source timestamp 证据时失败；Polymarket platform `timestamp` 不会覆盖 `recv_time_utc`。

### 任务 1.2：定义逐源数据格式与消费者兼容合同

**文件：**

- 创建：`data-collect/tests/source_format_contract.rs`
- 创建：`data-collect/tests/consumer_format_compatibility_contract.rs`

- [ ] **步骤 1.2.1：写逐源 typed raw 格式测试**

  测试必须逐源断言：

  - Polymarket market channel 保留 `event_type`、平台 `timestamp`、`recv_time_utc`、market/condition/asset/token 标识、subscription payload、`custom_feature_enabled`、raw_json；`custom_feature_enabled` 缺失、subscription 不可证明或 slug-only discovery 时 manifest 必须为 source gap / incomplete。
  - Polymarket market metadata / `new_market` 保留 `condition_id`、token id / asset id、CLOB market id、slug、start/end、active、acceptingOrders、`market_lifecycle_state`、tick size、min size、fee/reward、candidate slug、active scan evidence、metadata snapshot evidence 和 clock drift evidence。
  - Binance depth update 保留 `e,E,s,U,u,b,a`、combined stream wrapper、`recv_time_utc`、stale/duplicate/gap/resnapshot evidence。
  - Binance depth snapshot 保留 `lastUpdateId,bids,asks,snapshot_fetch_start_utc,snapshot_recv_time_utc`，并证明没有伪造 exchange timestamp。
  - Binance aggTrade 保留 `e,E,s,a,p,q,f,l,T,m,buyer_is_maker,recv_time_utc`，防止只写 4 列的缩水 schema。
  - RTDS Chainlink BTC/USD 保留 RTDS message timestamp、payload timestamp、symbol、value、source topic、subscription raw JSON、ack/first message 或 close/error evidence 和 `recv_time_utc`。

- [ ] **步骤 1.2.2：写 data-pipeline 格式兼容测试**

  测试断言所有 JSON-like typed raw parquet 都带 parseable `raw_json`、`parquet_raw_layout_version`、`parquet_raw_source_kind`、schema version、source coverage 和 manifest；`data-pipeline` labels、validation、tick_stream、features 和训练切分不由 `data-collect` 生成。

- [ ] **步骤 1.2.3：写训练、paper、live 消费边界格式测试**

  测试断言：

  - 训练只能把 `data-collect` 输出当作外部市场 typed raw mirror，必须经 `data-pipeline` 生成 labels/features/tick_stream/split 后才能训练。
  - paper/live 只能把 `data-collect` 输出当作外部市场源证据、coverage 和 manifest，不得替代 paper/live 自身 append-only event log、runtime receive timestamp、Market Context Builder、NOE/Ledger 或交易逻辑。
  - source kind、schema version、manifest、coverage window 和 timestamp namespace 对训练、paper、live 保持一致；字段为空时必须有 nullable 语义或 unavailable reason。

- [ ] **步骤 1.2.4：运行格式兼容测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect source_format_contract -- --nocapture
  cargo test -p data-collect consumer_format_compatibility_contract -- --nocapture
  ```

  预期：每个 source 的字段级格式可被测试固定；data-pipeline、训练、paper、live 的消费边界不会被 `data-collect` 改写。

### 任务 1.3：定义 manifest、coverage 和 required_files[] 合同

**文件：**

- 创建：`data-collect/src/manifest.rs`
- 创建：`data-collect/tests/manifest_contract.rs`

- [ ] **步骤 1.3.1：写 manifest 合同测试**

  测试必须覆盖 manifest 字段：

  - artifact path、row count、sha256。
  - schema version、code version / git commit、collector binary sha256、source config digest。
  - validation verdict、source coverage、source gap。
  - health/gap/reconnect artifact path 或 digest。
  - start/end time、coverage window、collector host/region。
  - ResourceWatermark、writer queue 高水位、flush latency。
  - `required_files[]` 的 file role、path、finalized status、row count、sha256/digest、schema version、missing reason。

- [ ] **步骤 1.3.2：实现 manifest 类型和 incomplete verdict**

  当 atomic rename 失败、sha256 缺失、row count 缺失、schema version 缺失或 manifest 不一致时，manifest 必须为 incomplete，且不得发布为 complete。source coverage 缺失或不完整时，manifest 必须记录 `coverage_incomplete`、source gap 和 missing reason；这属于 `ArtifactIncomplete` / `SourceDegraded`，不得单独触发 collector 进程退出。

- [ ] **步骤 1.3.3：运行 manifest 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect manifest_contract -- --nocapture
  cargo test -p data-collect incomplete_manifest_does_not_imply_process_fatal -- --nocapture
  ```

  预期：complete/incomplete verdict 可由测试数据稳定复现；incomplete manifest 会阻止消费者当作 complete 使用，但不会默认把 collector 标记为进程级失败。

### 任务 1.4：定义 source health、source gap 和 reconnect trace 合同

**文件：**

- 创建：`data-collect/src/health.rs`
- 创建：`data-collect/tests/health_gap_contract.rs`

- [ ] **步骤 1.4.1：写异常语义测试**

  测试覆盖需求文档 5.1 的全部异常，并断言每个异常至少进入 source health、source gap 或 reconnect trace 之一。

- [ ] **步骤 1.4.2：实现最小 health/gap/reconnect 类型**

  类型必须包含 source、attempt、sleep、错误类别、最终状态、open/half-open/recovered/exhausted、freshness verdict 和 coverage verdict。

- [ ] **步骤 1.4.3：运行 health/gap 合同测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect health_gap_contract -- --nocapture
  cargo test -p data-collect source_gap_and_stale_are_degraded_not_process_fatal -- --nocapture
  ```

  预期：异常不会被静默吞掉；ping/pong 正常但业务数据静默时写入 stale 或 gap；单源 degraded 不会掩盖其他源健康状态，也不会默认导致 collector 进程退出。

## 七、阶段 2：source adapter 实现

### 任务 2.0：四源启动调度与覆盖对齐

**文件：**

- 创建：`data-collect/src/scheduler.rs`
- 创建：`data-collect/tests/source_scheduler_contract.rs`

- [ ] **步骤 2.0.1：写辅助源独立启动测试**

  测试必须复现 poly-tx 旧问题：首个 Polymarket worker 因 asset ids / metadata discovery 延迟或失败而未启动时，Binance depth、Binance aggTrade 和 RTDS Chainlink 仍必须按计划独立启动或记录自身 source gap，不得等待 Polymarket worker 成功。

- [ ] **步骤 2.0.2：写四源 preload 覆盖对齐测试**

  测试断言 current active market 和 planned next market `T-30s` preload window 中，Polymarket market channel、Binance depth、Binance aggTrade、RTDS Chainlink 都有各自 coverage window、first business event 或明确 source gap；不能只证明 Polymarket 单源 T-30s。

- [ ] **步骤 2.0.3：运行 source scheduler 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect source_scheduler_starts_aux_sources_independently -- --nocapture
  cargo test -p data-collect four_sources_have_aligned_t30_preload_coverage_or_gap -- --nocapture
  ```

  预期：辅助源启动不依赖首个 Polymarket worker；任一源缺失必须进入 source coverage/source gap/reconnect trace。

### 任务 2.1：Polymarket market channel 与 market lifecycle

**文件：**

- 修改：`data-collect/src/scheduler.rs`
- 创建：`data-collect/src/sources/polymarket.rs`
- 创建：`data-collect/tests/polymarket_contract.rs`

- [ ] **步骤 2.1.1：写 Polymarket 合同测试**

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

- [ ] **步骤 2.1.2：实现 SDK-first adapter**

  只使用阶段 0 已批准的 SDK 能力。若执行时发现 SDK 行为与阶段 0 报告不一致，立即停止并更新 SDK gap 报告，不自行启用 raw WS/REST 旁路。

- [ ] **步骤 2.1.3：运行 Polymarket 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect polymarket_market_channel_contract -- --nocapture
  cargo test -p data-collect polymarket_retired_predicate_contract -- --nocapture
  cargo test -p data-collect polymarket_t30_prefetch_coverage_contract -- --nocapture
  cargo test -p data-collect polymarket_discovery_requires_active_scan_or_metadata_snapshot -- --nocapture
  cargo test -p data-collect polymarket_discovery_miss_does_not_block_later_markets -- --nocapture
  ```

  预期：`recv_time_utc` 与 Polymarket platform `timestamp` 同时保留；非爱尔兰 collector 延迟不会被标成 paper/live 爱尔兰接收延迟。

### 任务 2.2：Binance BTCUSDT depth 和 aggTrade

**文件：**

- 创建：`data-collect/src/sources/binance.rs`
- 创建：`data-collect/tests/binance_contract.rs`

- [ ] **步骤 2.2.1：写 Binance depth stitch 测试**

  测试覆盖：

  - WS depth raw 字段 `e,E,s,U,u,b,a`。
  - REST snapshot 字段 `lastUpdateId,bids,asks,snapshot_fetch_start_utc,snapshot_recv_time_utc`。
  - combined stream wrapper `stream/data` 保留。
  - 丢弃 `u <= lastUpdateId`。
  - 首个 update 满足 `U <= lastUpdateId + 1 <= u`。
  - stale/duplicate update 记录并忽略。
  - `U > local_u + 1` 记录 source gap 并重新 snapshot。

- [ ] **步骤 2.2.2：写 aggTrade 测试**

  测试断言保存原始字段 `e,E,s,a,p,q,f,l,T,m`，并把 `m` 保存为 `buyer_is_maker` 或等价原始布尔字段。

- [ ] **步骤 2.2.3：实现 Binance official source adapter**

  live Binance 源只来自 Binance 官方 WebSocket stream 和官方 REST depth snapshot；RTDS、archive、relay、proxy、cached feed 或历史 backfill 只能作为旁证或补数，不能冒充 live official source。

- [ ] **步骤 2.2.4：运行 Binance 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect binance_depth_stitch_contract -- --nocapture
  cargo test -p data-collect binance_aggtrade_raw_fields_contract -- --nocapture
  ```

  预期：snapshot 缺失、update id 缺口、rate limit、server shutdown 和 aggTrade 长时间无业务消息都会写入 source health/gap/reconnect 证据。

### 任务 2.3：Polymarket RTDS Chainlink BTC/USD

**文件：**

- 创建：`data-collect/src/sources/rtds.rs`
- 创建：`data-collect/tests/rtds_contract.rs`

- [ ] **步骤 2.3.1：写 RTDS subscription 测试**

  测试断言 subscription raw JSON 可追溯：

  - `action: "subscribe"`
  - `topic: "crypto_prices_chainlink"`
  - `type: "*"`
  - `filters: "{\"symbol\":\"btc/usd\"}"`
  - 重连后必须重新验证 topic、symbol filter、payload timestamp、业务消息 freshness 和 source stale 状态。

- [ ] **步骤 2.3.2：实现 RTDS adapter**

  adapter 保存 RTDS message timestamp、payload timestamp、symbol、value、source topic 和 `recv_time_utc`；RTDS Chainlink BTC/USD 只能作为 Chainlink 价格证据，不替代 Binance 官方 depth 或 aggTrade。

- [ ] **步骤 2.3.3：运行 RTDS 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect rtds_chainlink_contract -- --nocapture
  cargo test -p data-collect rtds_reconnect_revalidates_topic_symbol_payload_timestamp -- --nocapture
  ```

  预期：RTDS ping/pong 或连接 open 不能单独证明 coverage 完整；业务数据 freshness 才能恢复 healthy。

## 八、阶段 3：writer、分区与 atomic publish

### 任务 3.1：typed raw parquet writer

**文件：**

- 创建：`data-collect/src/writer.rs`
- 创建：`data-collect/tests/parquet_writer_contract.rs`

- [ ] **步骤 3.1.1：写 parquet schema metadata 测试**

  测试断言每个 source parquet 带 `parquet_raw_layout_version` 和 `parquet_raw_source_kind`，并保存 parseable `raw_json` 或等价原始证据引用。

- [ ] **步骤 3.1.2：实现 async 调度与 blocking 隔离**

  WebSocket、HTTP、timer、channel、manifest 写入、health 写入和 writer 调度不得阻塞主事件循环；文件 I/O、parquet close/finalize、data-collect 内部 parquet compression、local directory scan、sha256、large manifest aggregation、本地旧 segment 清理必须进入后台任务或 blocking pool。

- [ ] **步骤 3.1.3：运行 writer 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect parquet_writer_schema_metadata_contract -- --nocapture
  cargo test -p data-collect blocking_work_is_isolated_from_main_runtime -- --nocapture
  ```

  预期：主事件循环没有阻塞 I/O 或重 CPU；writer 记录 row group/buffer 内存估算、flush latency、bytes written 和 finalize 结果。

### 任务 3.2：按 UTC day 分区与 paper/live 边界

**文件：**

- 创建：`data-collect/src/publish.rs`
- 创建：`data-collect/tests/partition_publish_contract.rs`

- [ ] **步骤 3.2.1：写按天分区测试**

  训练用原始采集数据按 UTC day 分区保存；本文不发明最终落盘根路径，测试只验证 partition key、manifest 和 sha256 能证明按天边界。

- [ ] **步骤 3.2.2：写 paper/live 边界测试**

  测试断言 `data-collect` 只发布外部市场源 finalized mirror、coverage、health、gap 和 manifest；不生成 paper/live append-only log、rotation artifact、order evidence、NOE/ledger 或 execution feedback ingestion。

- [ ] **步骤 3.2.3：运行分区与边界测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect utc_day_partition_contract -- --nocapture
  cargo test -p data-collect paper_live_boundary_excludes_runtime_trade_artifacts -- --nocapture
  ```

  预期：训练、paper/live 消费边界清晰，`data-pipeline` 才生成 labels/features/tick_stream。

### 任务 3.3：atomic rename 和 finalized-only discovery

**文件：**

- 修改：`data-collect/src/publish.rs`
- 创建：`data-collect/tests/finalized_discovery_contract.rs`

- [ ] **步骤 3.3.1：写 partial 排除测试**

  测试断言消费者 discovery 排除：

  - temp/running 文件。
  - `.rsync-partial`。
  - 外部同步/发布链路生成的未完成标记文件。
  - 未 finalize parquet。

- [ ] **步骤 3.3.2：实现 atomic publish**

  close/finalize 后才 atomic rename；rename、sha256、row count、schema version 任一失败时 artifact verdict 为 incomplete，且不得发布为 complete。source coverage 不完整时记录 `coverage_incomplete`、source gap 和 missing reason；它限制消费者使用语义，但不得单独导致 collector 进程失败。只有 artifact 根不可写、路径越界、partial artifact 被发布或已采 segment 无法保全时才进入 `ProcessFatal`。

- [ ] **步骤 3.3.3：运行 publish 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect finalized_discovery_excludes_partial_files -- --nocapture
  cargo test -p data-collect atomic_rename_failure_marks_manifest_incomplete -- --nocapture
  cargo test -p data-collect coverage_incomplete_does_not_publish_as_complete_or_stop_collector -- --nocapture
  ```

  预期：消费者只读 finalized parquet 和对应 manifest；coverage incomplete 产物可保全但不能被发现为 complete。

## 九、阶段 4：runtime 韧性、bounded queue 与 ResourceWatermark

### 任务 4.1：retry/backoff+jitter 和按源熔断

**文件：**

- 修改：`data-collect/src/health.rs`
- 创建：`data-collect/tests/reconnect_trace_contract.rs`

- [ ] **步骤 4.1.1：写重试和熔断测试**

  测试断言有限重试、指数 backoff + jitter、max attempts、max elapsed、backoff ceiling、retry exhausted、attempt、sleep、错误类别和最终状态全部进入 reconnect trace。

- [ ] **步骤 4.1.2：实现按源独立状态机**

  Polymarket、Binance depth、Binance aggTrade、RTDS Chainlink 任一源故障不得被其他源健康状态掩盖；源级状态覆盖 open、half-open、recovered、exhausted。

- [ ] **步骤 4.1.3：运行 reconnect 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect reconnect_trace_contract -- --nocapture
  ```

  预期：重连后重新校验订阅、snapshot、新鲜业务消息和 source coverage。

### 任务 4.2：bounded queue 和 backpressure 策略

**文件：**

- 修改：`data-collect/src/config.rs`
- 修改：`data-collect/src/writer.rs`
- 创建：`data-collect/tests/bounded_queue_contract.rs`

- [ ] **步骤 4.2.1：写 bounded queue 合同测试**

  测试断言所有跨任务队列受 row count、byte count、oldest event age、writer lag 和 memory watermark 约束。

- [ ] **步骤 4.2.2：实现 backpressure 动作**

  达到 soft watermark 时必须触发 backpressure、source health 降级或 source gap 记录；达到 hard watermark 且连续采样窗口内无法恢复时，才允许受控暂停新市场或单源 intake。禁止 unbounded queue 导致 OOM 或 silent loss，也禁止短暂水位尖峰直接造成进程级失败。

- [ ] **步骤 4.2.3：运行 bounded queue 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect bounded_queue_contract -- --nocapture
  cargo test -p data-collect backpressure_hysteresis_prevents_flapping_stop_start -- --nocapture
  ```

  预期：queue 越线可复现，且 manifest 记录 writer queue 高水位、backpressure 次数、连续采样窗口、暂停/恢复动作和是否触发 hard threshold。

### 任务 4.3：ResourceWatermark 与 hard threshold action

**文件：**

- 创建：`data-collect/src/resource.rs`
- 创建：`data-collect/tests/resource_watermark_contract.rs`

- [ ] **步骤 4.3.1：写 ResourceWatermark 合同测试**

  测试覆盖需求文档第七节全部指标，并要求 threshold policy receipt 记录 soft/hard 阈值、单位、采样窗口、hysteresis、触发动作、恢复条件和结果。

- [ ] **步骤 4.3.2：实现 hard threshold action**

  超过 soft 阈值时只记录降级、backpressure 和恢复观察；连续超过 hard 阈值且无法恢复时，才停止新市场采集或暂停单源 intake，保护已采数据，优先 atomic publish 和 sha256 manifest。只有无法保全已采 segment、artifact 根不可写或路径越界时，才进入 `ProcessFatal`。

- [ ] **步骤 4.3.3：运行 ResourceWatermark 测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect resource_watermark_contract -- --nocapture
  cargo test -p data-collect resource_watermark_soft_threshold_degrades_without_process_fatal -- --nocapture
  ```

  预期：Linux 指标不可用时记录 unavailable reason；soft threshold 不触发进程级失败；live 交易小主机不会被计划为编译或重处理机器。

## 十、阶段 5：消费者边界与接入合同

### 任务 5.1：训练消费者接入边界

**文件：**

- 创建：`data-collect/tests/training_consumer_boundary_contract.rs`
- 修改：`data-collect/src/manifest.rs`

- [ ] **步骤 5.1.1：写训练边界测试**

  测试断言训练消费者只能读取按 UTC day 分区的 finalized typed raw parquet、manifest、source coverage 和后续 `data-pipeline` 生成的 labels/features/tick_stream。

- [ ] **步骤 5.1.2：实现 manifest 中的训练边界声明**

  manifest 可以声明 source coverage 和 finalized mirror，不提供 labels、features、tick_stream、canonical split 或 T-30 样本加工。

- [ ] **步骤 5.1.3：运行训练边界测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect training_consumer_boundary_contract -- --nocapture
  ```

  预期：采集成功不等于训练批准，finalized parquet 完整不等于 labels/features/tick_stream 已完成。

### 任务 5.2：paper/live 消费者接入边界

**文件：**

- 创建：`data-collect/tests/paper_live_boundary_contract.rs`
- 修改：`data-collect/src/manifest.rs`

- [ ] **步骤 5.2.1：写 paper/live 非范围测试**

  测试断言 `data-collect` 不生成或解释：

  - paper/live append-only log。
  - rotation artifact。
  - live artifacts。
  - order evidence。
  - NormalizedOrderEvent。
  - ledger。
  - execution feedback ingestion。
  - 交易决策、风控、submit/cancel path。

- [ ] **步骤 5.2.2：实现接入边界声明**

  `data-collect` 只为 paper/live 提供外部市场源证据和 coverage 边界；paper/live 热路径延迟必须来自对应 paper/live 主机 artifact，不能使用非爱尔兰采集主机 `recv_time_utc` 冒充。

- [ ] **步骤 5.2.3：运行 paper/live 边界测试**

  计划执行命令：

  ```bash
  cargo test -p data-collect paper_live_boundary_contract -- --nocapture
  ```

  预期：计划和实现不把 paper/live 交易逻辑写入 `data-collect`。

## 十一、阶段 6：端到端验收与交付

### 任务 6.1：端到端采集 dry run

**文件：**

- 创建：`data-collect/tests/e2e_collect_contract.rs`
- 修改：`data-collect/docs/phase0-technical-validation.md`
- 修改：`changelog/data-collect-changelog.md`

- [ ] **步骤 6.1.1：运行短窗口四源采集 dry run**

  计划执行命令：

  ```bash
  cargo test -p data-collect e2e_four_source_collect_contract -- --nocapture
  ```

  预期：四源均生成 typed raw parquet、source coverage、source health、source gap、reconnect trace 和 manifest；artifact finalized-only。

- [ ] **步骤 6.1.2：运行长窗口 soak test**

  soak test 至少覆盖一个完整 `btc-updown-5m` market 与下一市场 `T-30s` prefetched window。

  预期：manifest 可证明 current -> retired、prefetched -> active、coverage window、first business event、subscription confirmation evidence 和 source freshness。

### 任务 6.2：需求验收矩阵复核

**文件：**

- 创建：`data-collect/docs/acceptance-matrix.md`
- 修改：`changelog/data-collect-changelog.md`

- [ ] **步骤 6.2.1：逐条映射需求验收**

  `data-collect/docs/acceptance-matrix.md` 必须逐条映射 `demand/data-collect-demand.md` 第九节 22 个验收标准到测试、artifact 或报告。

- [ ] **步骤 6.2.2：检查非范围没有泄漏**

  计划执行命令：

  ```bash
  rg -n "labels|features|tick_stream|canonical split|purged|embargo|reward|PnL|NAV|ledger|NormalizedOrderEvent|submit|cancel|manual signing|shadow implementation" data-collect/src data-collect/tests data-collect/docs
  ```

  预期：命中只能出现在边界声明、非范围测试或 SDK 缺口说明中；不得出现生产逻辑实现。

- [ ] **步骤 6.2.3：运行最终验证**

  计划执行命令：

  ```bash
  cargo fmt --check -p data-collect
  cargo clippy -p data-collect --all-targets -- -D warnings
  cargo test -p data-collect --all-targets
  cargo bench -p data-collect throughput_probe -- --sample-size 20
  ```

  预期：格式、lint、测试和吞吐 bench 均通过；若 bench 未达到需求基线，不能宣称完成。

### 任务 6.3：阶段审查、changelog 和交付 gate

**文件：**

- 修改：`changelog/data-collect-changelog.md`

- [ ] **步骤 6.3.1：自审 diff 与验证输出**

  检查每一行改动是否直接追溯到 `demand/data-collect-demand.md`；记录测试命令、退出码、artifact 路径、sha256 和剩余风险。

- [ ] **步骤 6.3.2：派生独立审查**

  按仓库规则在阶段完成后派生子代理审查，检查需求覆盖、正确性、边界、过度设计、遗漏、测试和与 `rebuild-core-demand` 的一致性。

- [ ] **步骤 6.3.3：修复审查反馈或请求用户决策**

  可执行且在范围内的问题修复后重新审查；如果反馈要求改变 SDK-first、服务器角色、吞吐验收、消费者边界或非范围，必须先请求用户决策。

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
rg -n "/home/chillchill/btc-updown-marketmaker|~/btc-updown-marketmaker|路径执行口径|data_collect_run_root|data_collect_artifact_root|阶段推进通用 gate|数据格式合同|运行时校验分级|ProcessFatal|ArtifactIncomplete|SourceDegraded|BackpressureDegraded|hysteresis|source_format_contract|consumer_format_compatibility_contract|demand/data-collect-demand.md|阶段 0|技术测试|性能验证|2k-5k events/s|bounded queue|backpressure|ResourceWatermark|atomic rename|source gap|recv_time_utc|Polymarket market channel|Binance BTCUSDT aggTrade|RTDS Chainlink|SDK 缺口|用户审查" plan/data-collect-plan.md changelog/data-collect-changelog.md
rg -n "[T]ODO|[T]BD|待[补]|以后再[填]|[p]laceholder" plan/data-collect-plan.md changelog/data-collect-changelog.md
awk '/[ \t]$/ { print FILENAME ":" FNR ": trailing whitespace"; bad=1 } END { exit bad }' plan/data-collect-plan.md changelog/data-collect-changelog.md
```

后续实现完成前最低验证：

```bash
cd /home/chillchill/btc-updown-marketmaker
cargo fmt --check -p data-collect
cargo clippy -p data-collect --all-targets -- -D warnings
cargo test -p data-collect --all-targets
cargo bench -p data-collect throughput_probe -- --sample-size 20
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
| 14 | 网络异常、空返回、业务静默、rate limit、stale | 任务 0.5、1.4、4.1、运行时校验分级 | network recovery、health/gap contract、reconnect trace、source degraded 非进程级失败测试 |
| 15 | 有限重试、backoff、jitter、按源独立熔断/恢复 | 任务 0.5、4.1 | retry/circuit breaker/reconnect trace tests |
| 16 | reconnect trace attempt/sleep/error/final state | 任务 1.4、4.1 | health/gap schema、reconnect trace contract |
| 17 | Rust async/non-blocking、blocking pool、bounded queue、backpressure | 阶段 0、3、4、运行时校验分级 | blocking isolation、bounded queue、backpressure hysteresis tests |
| 18 | 性能验收、`2k-5k events/s`、writer queue、ResourceWatermark、hardware tier | 任务 0.4、0.6、4.2、4.3、6.2 | throughput bench、hardware validation、ResourceWatermark soft/hard threshold contract |
| 19 | ResourceWatermark hard threshold policy receipt | 任务 0.6、4.3、运行时校验分级 | threshold policy receipt、soft/hard threshold action tests |
| 20 | archive/upload/listing/gzip 非范围 | 路径执行口径、第十二节决策门、6.2 | 非范围声明、决策门、非范围泄漏扫描 |
| 21 | Polymarket Rust SDK-first 和 SDK 缺口审批 | 任务 0.2、2.1、第十二节决策门 | SDK gap report、SDK-first adapter gate |
| 22 | 不把 data-pipeline、paper/live 交易逻辑、SDK 旁路写进 data-collect | 任务 1.2、5.1、5.2、6.2 | consumer compatibility contract、training/paper/live boundary tests、非范围扫描 |

## 十五、数据格式与消费者兼容性检查

| 消费方 | 是否直接消费 `data-collect` typed raw parquet | 必须满足的数据格式 | 不允许的误用 |
| --- | --- | --- | --- |
| `data-pipeline` | 是，作为外部市场源 typed raw mirror 输入 | 逐源 parquet source kind、parseable `raw_json`、schema version、source timestamp、`recv_time_utc`、source coverage、manifest、UTC day 分区 | 把 `data-collect` 当成 labels/features/tick_stream/训练切分生成器 |
| 训练 | 不直接把 raw parquet 当训练样本；通过 `data-pipeline` 后消费 | `data-pipeline` 生成 labels/features/tick_stream/split 后，训练再消费；raw parquet 完整只证明外部源可处理 | parquet 完整即训练批准、source coverage 完整即 label 完整 |
| paper | 可消费外部市场源证据、coverage、manifest | 与训练/live 共用 source kind、schema version、timestamp namespace；paper runtime receive timestamp 由 paper 主机 artifact 提供 | 用非爱尔兰 collector `recv_time_utc` 冒充 paper runtime receive timestamp，或用 parquet 替代 append-only log |
| live | 可消费外部市场源证据、coverage、manifest | 与训练/paper 共用 source kind、schema version、timestamp namespace；live runtime receive timestamp 由 live 主机 artifact 提供 | 在 live 小主机编译/重处理，或用 `data-collect` 生成交易决策、NOE/ledger、execution feedback label |
