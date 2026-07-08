# data-collect PMXT-inspired Source-specific Hourly Layout and Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 参考 PMXT v2 Polymarket orderbook archive 的数据结构和压缩方法，修复 `data-collect` 的 Polymarket `btc-updown-5m` orderbook 完整性，并把 Polymarket、Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 全部训练源改为 UTC day 文件夹 + 每小时 finalized typed parquet，同时为训练采集增加 50GB 滚动磁盘保护。

**Architecture:** 训练 cold path 新增按 `source_kind` 分离的 typed hourly layout：Polymarket orderbook 采用 PMXT-inspired 结构，Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 分别采用各自官方字段的 typed schema，不再把所有源都写成共用 `raw_json` 宽表。paper/live hot path 继续走实时 handoff，不等待 hourly parquet。存储层按 UTC day/hour 管理所有 source kind 的 finalized artifact，manifest 记录 PMXT 参照字段、逐源覆盖窗口、row count、sha256、source coverage 和 retention policy/evidence；磁盘保护由独立 retention 组件在后台/blocking pool 中执行 dry-run/no-op receipt，真实删除只限最老 finalized complete UTC day。

**Tech Stack:** Rust、Tokio async runtime、官方 Polymarket Rust SDK-first、Apache Arrow Rust parquet writer、ZSTD parquet compression、bounded queue、blocking pool、sha256 manifest、filesystem atomic rename。

---

> 对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

> 对应既有计划文档：`/home/chillchill/btc-updown-marketmaker/plan/data-collect-plan.md`

> 对应 changelog：`/home/chillchill/btc-updown-marketmaker/changelog/data-collect-changelog.md`

> PMXT 参考文档：`https://archive.pmxt.dev/docs/v2-data-overview#overview`

> Polymarket market channel 参考文档：`https://docs.polymarket.com/market-data/websocket/market-channel`

> 状态：`APPROVED_EXECUTING`。用户已批准执行；实现、验证和远端采集必须继续遵守本文的 SDK-first、hot/cold path、retention dry-run/default 和删除决策门。

## 一、背景与问题

用户人工检查 Phase 7 拉回的 Polymarket parquet 后发现：当前 `data-collect` 的 `book` 事件 `raw_json` 只保存 `bids_len`、`asks_len`、`hash`、`market`、`asset_id`、`timestamp` 等摘要，未保存完整 `bids` / `asks` price-size 层级。因此当前产物不能还原某个时刻的完整 Polymarket orderbook。

进一步核对现有 `data-collect/src/writer.rs` 可见，当前 writer 的核心 schema 是所有 source 共用的 `RawEvent` 宽表：`source_kind`、`source_event_type`、`source_identifier`、`source_ts`、`recv_time_utc`、collector 字段、`source_endpoint`、`raw_json` 和 layout metadata。这能保留证据，但不适合长期训练 cold path：Polymarket 会丢失完整盘口，Binance/RTDS 也缺少可直接读取的 typed 列，文件体积和 data-pipeline 读取效率都会受影响。

对 PMXT 范例文件 `/home/chillchill/btc-updown-marketmaker/polymarket_orderbook_2026-07-07T07.parquet` 的本地分析显示：

1. 原始全市场小时文件 `87,171,696` 行，约 `467M`。
2. 过滤只保留 `2026-07-07T07` 这一小时的 12 个 `btc-updown-5m` market、24 个 token 后，文件为 `3,027,851` 行，约 `23M`。
3. 过滤文件中 `book` 行 `55,906`，全部带非空 `bids` + `asks`，其值为 `[[price, size], ...]` 形态的完整 depth。
4. `book` 不是只在首次订阅时出现；Polymarket 官方文档说明 `book` 在首次订阅和有 trade 影响 book 时都会发出。

这说明 Phase 7 证明了连接、market discovery、runtime、writer、manifest 和 2 小时部署路径，但没有证明 Polymarket 完整 orderbook 持久化正确，也没有完成所有训练源的 hourly typed layout。该问题必须在进入长期训练采集前修复。

## 二、PMXT 采纳与不采纳分析

本计划采用的核心思路是：

> Polymarket 和 Binance depth 借鉴 PMXT 的 orderbook typed parquet 方法；Binance aggTrade 和 RTDS Chainlink 借鉴 PMXT 的 typed columnar hourly parquet、双时间轴、nullable event fields、compression、manifest 和 finalized publish 方法，但使用各自 source-specific schema。

因此，PMXT 不是要被复制成一个通用 schema，而是作为两类方法参考：

| 数据源 | PMXT 借鉴方式 | 不做的事 |
| --- | --- | --- |
| Polymarket market channel | 高度借鉴 PMXT orderbook schema：`timestamp_received`、source `timestamp`、`market`、`asset_id`、`event_type`、`bids`、`asks`、`price`、`size`、`side`、`best_bid`、`best_ask`，并补充本项目 collector/slug/coverage 字段。 | 不订阅 every live asset，不把 PMXT archive 当 live source，不丢弃 source health/gap/reconnect。 |
| Binance depth snapshot/update | 类比 PMXT 的 orderbook typed parquet：snapshot 保存完整 `bids/asks`，update 保存 `U/u/b/a`，以 Binance 官方 sequence/stitch 语义重建 Binance orderbook。 | 不使用 Polymarket 字段名冒充 Binance 字段，不用 PMXT market/asset_id schema。 |
| Binance aggTrade | 借鉴 PMXT 的 typed columnar hourly parquet 和压缩/manifest 方法，字段使用 Binance 官方 aggTrade：`E/T/a/p/q/f/l/m`。 | 不把成交流伪装成 orderbook，不生成 `bids/asks`。 |
| RTDS Chainlink BTC/USD | 借鉴 PMXT 的 typed time-series hourly parquet、双时间轴和 finalized publish 方法，字段使用 RTDS topic/symbol/value/message timestamp/payload timestamp。 | 不把 Chainlink price 当 Binance depth 或 Polymarket book，不生成盘口字段。 |

### 2.1 应采纳

| PMXT 做法 | 是否采纳 | 原因与本项目落点 |
| --- | --- | --- |
| Polymarket orderbook 使用 typed parquet，而不是每行大 `raw_json` | 采纳 | typed 列可显著降低体积，并让 data-pipeline 读取更稳定。新增 Polymarket orderbook hourly schema，保留完整 raw-equivalent native fields。 |
| typed columnar parquet 替代全源共用 `raw_json` 宽表 | 采纳 | Polymarket 采用 PMXT-inspired orderbook schema；Binance depth/snapshot、aggTrade、RTDS Chainlink 分别采用官方字段 typed schema。共用 envelope 继续通过 metadata/manifest 保留。 |
| `timestamp_received` + source `timestamp` 双时间轴 | 采纳 | 映射为 `recv_time_utc` / `timestamp_received` 和各 source 原始时间戳，符合需求文档时间语义。Polymarket 是平台 `timestamp`，Binance 是 `E/T` 或 snapshot boundary，RTDS 是 message/payload timestamp。 |
| `market` 为 condition id，`asset_id` 为 outcome token id | 采纳 | 这是重建每个 Up/Down token orderbook 的关键主键。manifest 继续记录 slug、market window、current/prefetched lifecycle。 |
| `book` 行保存完整 `bids` / `asks` depth | 必须采纳 | 当前缺口的直接修复点；禁止只保存 `bids_len` / `asks_len`。 |
| `price_change` 行保存 `price`、`size`、`side`、`best_bid`、`best_ask` | 采纳 | 用于在 `book` 快照之间推进本地 orderbook 状态。 |
| event-specific nullable columns | 采纳 | Polymarket、Binance、RTDS 都按事件类型填充对应列；不相关字段保持 null，不写错误默认值。 |
| UTC hourly parquet | 采纳 | 用户新要求为 UTC day 文件夹、每小时落盘；本计划解释为每个 source kind 每小时一个逻辑 partition，正常一个 finalized parquet，保护性 soft rollover 可产生 `part=N`。 |
| parquet finalized 后供消费者读取 | 采纳 | 继续保留 running/digesting/final atomic rename 和 manifest 完整性。 |
| ZSTD compression、dictionary/RLE、typed decimal | 采纳但要 benchmark | 目标是接近 PMXT 的体积优势；必须在本地和数据采集服务器验证 CPU/RSS/flush/finalize 成本。 |
| source-specific sort order | 条件采纳 | Polymarket 优先测试 `(market, asset_id, timestamp_received)`；Binance depth 测试 `(symbol, stream/update id, timestamp_received)`；aggTrade 测试 `(symbol, aggregate_trade_id, timestamp_received)`；RTDS 测试 `(symbol, timestamp_received)`。排序可能增加内存/CPU，必须 benchmark。 |

### 2.2 不应采纳

| PMXT 做法 | 不采纳原因 |
| --- | --- |
| 订阅 every live asset | 本项目范围只采集 `btc-updown-5m` current active 和 prefetched next market，不采全 Polymarket。 |
| 把 PMXT archive 当作 runtime source | PMXT 只能作为结构参考和离线校验样本，不能替代 live Polymarket market channel。 |
| 只发布小时归档、不提供实时路径 | paper/live 需要 hot path handoff；不能等 hourly parquet finalize 才给交易模块。 |
| 只保留 archive 数据、不记录 source health/gap/reconnect/ResourceWatermark | 本项目需求要求 source coverage、health/gap、reconnect trace、ResourceWatermark、collector host/region 和 manifest。 |
| 公共 R2/object storage 发布 | 当前 `data-collect` 非范围包含 archive/upload/listing/gzip；本计划只做本机 datasets。 |
| 单 Polymarket schema 承载 Binance/RTDS | Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 必须进入修改范围，但仍使用各自 typed schema；不强行合并成 PMXT orderbook schema。 |
| 用 finalized parquet 替代 paper/live append-only log、NOE/ledger 或交易证据 | 违反消费者边界；data-collect 只提供外部市场源证据和 handoff。 |

### 2.3 需要执行前确认的设计口径

本文采用以下口径写入计划，用户审查时如不同意，需要先改计划再执行：

1. “每天创建一个文件夹，每小时落盘一个文件”解释为：每个 `source_kind` 在每个 UTC day 文件夹下每小时一个逻辑 partition；正常情况下一个 finalized parquet，若 `max_rows` / `max_bytes` 触发保护性 soft rollover，则同一小时可生成多个 `part=N` finalized parquet。所有训练源都纳入：`polymarket_market`、`polymarket_market_metadata`、`binance_depth_snapshot`、`binance_depth_update`、`binance_aggtrade`、`polymarket_rtds_chainlink_btc_usd`。各源不强行写进同一个 parquet，因为 schema 不同。
2. Polymarket `btc-updown-5m` 的 hourly parquet 文件包含该 UTC 小时内命中的 5 分钟 market 和 token orderbook；不能假设每小时恒定 12 个市场，必须以 manifest 覆盖窗口和 market discovery 结果为准。
3. Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 都按同一 UTC day/hour 规则落盘，并保留各自需求文档列出的官方字段。
4. 训练数据根路径仍为服务器本机 `~/btc-updown-marketmaker/datasets/`；计划使用 day-first 路径：
   `~/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH.parquet`
5. 磁盘保护的 50GB 解释为训练数据集根目录 `~/btc-updown-marketmaker/datasets/training` 的滚动容量上限 `50_000_000_000` bytes；同时记录 filesystem free bytes。触发后只允许删除 `training/typed_raw/utc_day=*` 下最老的 finalized complete UTC day，不删除当前 UTC day、running/digesting/tmp/`.rsync-partial`、manual-inspection、run manifests、startup receipts 或 incomplete day。
6. 若磁盘超过 50GB 但没有可删除的 finalized complete UTC day，collector 必须写 ResourceWatermark hard-threshold evidence 并受控停止新采集，避免把磁盘写满。

## 三、目标

1. Polymarket `book` 事件必须完整保存 `bids` / `asks` price-size depth，能从 finalized parquet 还原某个 token 的 orderbook。
2. Polymarket `price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` 必须按事件语义保存 typed 字段和 source evidence；`polymarket_market_metadata` 必须保存 condition id、CLOB market id、token/asset id、slug、start/end、active/acceptingOrders、lifecycle、tick/min size、fee/reward 等字段；不能因为采用 PMXT 结构而丢失需求字段。
3. Binance depth snapshot/update 必须保留官方 `lastUpdateId`、`bids`、`asks`、`e`、`E`、`s`、`U`、`u`、`b`、`a`、snapshot fetch/recv boundary 和 stitch evidence。
4. Binance aggTrade 必须保留官方 `e`、`E`、`s`、`a`、`p`、`q`、`f`、`l`、`T`、`m` / `buyer_is_maker`。
5. RTDS Chainlink BTC/USD 必须保留 message timestamp、payload timestamp、symbol、value、topic、subscription filter 和 `recv_time_utc`。
6. 训练 cold path 改为 UTC day 文件夹，每个 source kind 每小时一个逻辑 partition；正常一个 finalized parquet，保护性 soft rollover 可产生多个 `part=N`，manifest 记录 hour window、UTC day、source coverage、row count、sha256、schema version、hour part count 和 retention policy/evidence。
7. paper/live hot path 不等待 hourly parquet；继续通过实时 handoff 提供外部市场源事件和 health/gap/status。
8. 训练采集增加 50GB 滚动磁盘保护：超过阈值时默认写 dry-run/no-op policy receipt；真实删除只限最老 finalized complete UTC day，并写 deletion receipt。
9. 本地和数据采集服务器验证必须证明：四源 typed schema、完整 orderbook、小时文件、day 文件夹、atomic publish、manifest、压缩体积、资源水位和 retention 行为都满足需求。

## 四、文件结构变更计划

| 路径 | 动作 | 责任 |
| --- | --- | --- |
| `demand/data-collect-demand.md` | 修改 | 在用户批准本计划后同步新增 PMXT-like Polymarket hourly layout、全源 UTC day/hour 文件、50GB retention 要求。 |
| `changelog/data-collect-changelog.md` | 修改 | 记录本计划创建、用户审批、执行事实、测试命令、远端 artifact 和决策变化。 |
| `plan/data-collect-pmxt-hourly-retention-plan.md` | 新增 | 本计划文档。 |
| `data-collect/src/polymarket_orderbook_layout.rs` | 新增 | Polymarket orderbook hourly typed schema、row struct、SDK event -> row 转换、PMXT-compatible field mapping；同时定义 Polymarket event-specific nullable 字段。 |
| `data-collect/src/polymarket_market_metadata_layout.rs` | 新增 | `polymarket_market_metadata` hourly typed schema、market discovery / `new_market` / fallback metadata -> row 转换。 |
| `data-collect/src/source_hourly_layout.rs` | 新增 | Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink hourly typed schema、row struct、source event -> row 转换。 |
| `data-collect/src/hourly_partition.rs` | 新增 | UTC day/hour bucket、hourly filename、running/digesting/final path、day-first partition path。 |
| `data-collect/src/retention.rs` | 新增 | 50GB training dataset retention policy、oldest finalized complete day discovery、safe delete、run-level retention policy/no-op receipt 和可选 deletion receipt。 |
| `data-collect/src/config.rs` | 修改 | 增加 hourly layout、compression/sort、retention threshold、manual review complete-market run mode 配置。 |
| `data-collect/src/bin/data_collect_training_collector.rs` | 修改 | 增加人工审查运行参数，例如 `--stop-after-complete-polymarket-market 1` 或等价 config，采完一个完整 5 分钟 market 后受控停止并 finalize。 |
| `data-collect/src/runtime.rs` | 修改 | 扩展 CLI/config 解析，接入人工审查 complete-market run mode、retention delete mode 和 day-first path 迁移。 |
| `data-collect/src/publish.rs` | 修改 | 将训练 cold path 从 source-first 迁移为 day-first partition，更新 finalized discovery 和 path validation。 |
| `data-collect/src/writer.rs` | 修改 | 支持按 source kind 选择 schema；增加 Polymarket、Binance、RTDS typed hourly writer；配置 ZSTD/dictionary/decimal metadata。 |
| `data-collect/src/live_runtime.rs` | 修改 | Polymarket book 写完整 `bids/asks`；接入 Polymarket 全目标事件和 metadata rows；Binance/RTDS 写入对应 typed row；writer worker 按 hourly partition rollover；集成 retention trigger。 |
| `data-collect/src/sources/binance.rs` | 修改 | 将 depth snapshot/update parser 和 stitch evidence 接入真实采集写入链路。 |
| `data-collect/src/sources/polymarket.rs` | 修改 | 使用官方 SDK-first 能力覆盖 `book`、`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` 和 metadata evidence。 |
| `data-collect/src/manifest.rs` | 修改 | 增加 hourly file manifest、PMXT mapping receipt、source layout receipt、retention policy/no-op receipt 引用、可选 deletion receipt、compression/sort receipt。 |
| `data-collect/src/source_format.rs` | 修改 | 增加全源 hourly typed layout contract，不再接受 Polymarket `book` 只有 len 的缩水证据，也不接受 Binance/RTDS 只有 opaque raw_json 而缺少必填 typed 字段。 |
| `data-collect/src/resource.rs` | 修改 | 增加 dataset bytes、retention threshold、retention action、post-delete disk evidence。 |
| `data-collect/docs/training-collector-deploy-readme.md` | 修改 | 写明 UTC day/hour 目录、50GB retention、人工检查命令。 |
| `data-collect/docs/paper-live-hot-path-integration.md` | 修改 | 明确 PMXT-like hourly parquet 只属于训练 cold path，paper/live 仍走 hot path。 |
| `data-collect/tests/pmxt_orderbook_layout_contract.rs` | 新增 | PMXT-like schema、完整 depth、event-specific nullable 字段、PMXT sample 兼容性测试。 |
| `data-collect/tests/polymarket_metadata_layout_contract.rs` | 新增 | Polymarket market metadata、`new_market`、retired/fallback metadata typed row 和 manifest counters 合同。 |
| `data-collect/tests/source_hourly_layout_contract.rs` | 新增 | Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink hourly typed schema 与必填字段测试。 |
| `data-collect/tests/binance_depth_stitch_contract.rs` | 新增 | Binance snapshot/update stitch predicate、`local_u` 单调推进、gap/resnapshot receipt 合同。 |
| `data-collect/tests/hourly_partition_contract.rs` | 新增 | UTC day/hour path、hour boundary rollover、finalized-only discovery。 |
| `data-collect/tests/training_retention_contract.rs` | 新增 | 50GB threshold、删除最老 finalized complete UTC day、不删 current/running/incomplete/manual-inspection/run manifests。 |
| `data-collect/tests/manual_review_complete_market_contract.rs` | 新增 | 完整 5 分钟 market 审查运行模式、受控停止、shutdown finalize 和 artifact bundle 合同。 |
| `data-collect/benches/source_hourly_layout_probe.rs` | 新增 | 全源 hourly typed layout 压缩/排序/资源 benchmark。 |

## 五、执行步骤

### Task 1: 需求与审计文档同步

**Files:**
- Modify: `demand/data-collect-demand.md`
- Modify: `changelog/data-collect-changelog.md`
- Modify: `data-collect/docs/acceptance-matrix.md`

- [x] **Step 1.1: 更新需求文档**

  在 `demand/data-collect-demand.md` 中增加：

  - Polymarket `book` 必须保存完整 `bids` / `asks` price-size depth。
  - Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 必须进入 hourly typed layout 修改范围。
  - 训练 cold path 使用 UTC day 文件夹、每 source kind 每小时一个逻辑 partition；正常一个 finalized parquet，保护性 soft rollover 可产生 `part=N`。
  - PMXT-like schema 是参考实现，不引入 PMXT archive source。
  - 训练 dataset retention threshold 为 `50_000_000_000` bytes，默认 dry-run/no-op receipt；真实删除只限最老 finalized complete UTC day。

  验证：

  ```bash
  rg -n "PMXT|bids|asks|binance_depth|binance_aggtrade|rtds|utc_day|hour|50_000_000_000|retention|finalized complete UTC day|dry-run" demand/data-collect-demand.md
  ```

- [x] **Step 1.2: 更新 changelog**

  在 `changelog/data-collect-changelog.md` 记录用户批准的设计口径、PMXT 采纳/不采纳结论和后续执行边界。

  验证：

  ```bash
  rg -n "PMXT|hourly|50GB|retention|完整 orderbook|Binance|RTDS|全源" changelog/data-collect-changelog.md
  ```

- [x] **Step 1.3: 更新 acceptance matrix**

  在 `data-collect/docs/acceptance-matrix.md` 增加 PMXT-like orderbook、Binance/RTDS hourly typed schema、hourly logical partition、day-first path、legacy source-first read-only、no mixed publish、retention 验收项。

  验证：

  ```bash
  rg -n "PMXT|hourly|day-first|source-first|no mixed publish|retention|bids|asks|binance_depth|binance_aggtrade|rtds" data-collect/docs/acceptance-matrix.md
  ```

### Task 2: Polymarket orderbook typed layout 合同

**Files:**
- Create: `data-collect/src/polymarket_orderbook_layout.rs`
- Modify: `data-collect/src/lib.rs`
- Create: `data-collect/tests/pmxt_orderbook_layout_contract.rs`

- [x] **Step 2.1: 先写失败测试，证明当前缩水 book 不合格**

  新增测试断言：

  - `book` 行必须有非空 `bids` / `asks`，且能解析成 `[[price, size], ...]`。
  - 只包含 `bids_len` / `asks_len` 的 `book` raw evidence 必须失败。
  - `price_change` 行必须有 `price`、`size`、`side`、`best_bid`、`best_ask`。
  - `tick_size_change`、`last_trade_price`、`new_market`、`market_resolved` 必须有 event-specific nullable typed 字段或等价 raw evidence 引用。
  - schema 必须包含 `timestamp_received`、`timestamp`、`market`、`event_type`、`asset_id`、`bids`、`asks`。

  验证 RED：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test pmxt_orderbook_layout_contract -- --nocapture
  ```

  预期：失败于缺少 `polymarket_orderbook_layout` 模块或当前 `book` depth 校验不通过。

- [x] **Step 2.2: 实现 Polymarket orderbook row 类型**

  `PolymarketOrderbookRow` 至少包含：

  | 字段 | 类型口径 | 事件 |
  | --- | --- | --- |
  | `timestamp_received` | `TimestampMillisecond(UTC)` | all |
  | `timestamp` | `TimestampMillisecond(UTC)` nullable | source 有 timestamp 的事件 |
  | `market` | condition id / market id string；可在 writer 中转 fixed-size binary | all |
  | `event_type` | string | all |
  | `asset_id` | string | token-level events |
  | `bids` | JSON depth string `[[price,size],...]` nullable | book |
  | `asks` | JSON depth string `[[price,size],...]` nullable | book |
  | `price` | decimal-compatible string / Decimal128 | price_change / last_trade_price |
  | `size` | decimal-compatible string / Decimal128 | price_change / last_trade_price |
  | `side` | BUY/SELL nullable | price_change / last_trade_price |
  | `best_bid` | decimal-compatible nullable | price_change / best_bid_ask |
  | `best_ask` | decimal-compatible nullable | price_change / best_bid_ask |
  | `hash` | string nullable | book / price_change |
  | `fee_rate_bps` | decimal-compatible nullable | price_change / last_trade_price / new_market 如源提供 |
  | `transaction_hash` | string nullable | last_trade_price / trade-related event 如源提供 |
  | `old_tick_size` | decimal-compatible nullable | tick_size_change |
  | `new_tick_size` | decimal-compatible nullable | tick_size_change |
  | `winning_asset_id` | string nullable | market_resolved |
  | `winning_outcome` | string nullable | market_resolved |
  | `close_reason_if_present` | string nullable | market_resolved / fallback close evidence |
  | `subscribed_asset_ids_for_market` | JSON string nullable | market_resolved / fallback metadata |
  | `resolved_event_asset_ids` | JSON string nullable | market_resolved |
  | `slug` | string nullable | project metadata |
  | `collector_host_id` | string | all |
  | `collector_region` | string | all |
  | `source_endpoint` | string | all |
  | `schema_version` | string / metadata | all |

  注意：PMXT 没有 `collector_host_id`、`collector_region`、`slug`，但本项目需求必须保留。

- [x] **Step 2.3: 实现 SDK event -> row 转换**

  对 Polymarket `book` 事件，转换必须把 SDK `event.bids` / `event.asks` 序列化为 JSON depth 数组，不能只写长度。

  验证：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test pmxt_orderbook_layout_contract -- --nocapture
  ```

  预期：所有 layout contract 通过。

- [x] **Step 2.4: 增加 Polymarket market metadata layout**

  `PolymarketMarketMetadataRow` 至少包含：

  | 字段 | 类型口径 |
  | --- | --- |
  | `timestamp_received` | `TimestampMillisecond(UTC)` |
  | `metadata_fetch_start_utc` / `metadata_recv_time_utc` | `TimestampMillisecond(UTC)` nullable |
  | `slug` | string |
  | `condition_id` | string |
  | `clob_market_id` | string nullable |
  | `asset_ids` / `token_ids` | JSON string / list-compatible string |
  | `market_start_utc` / `market_end_utc` | source timestamp nullable |
  | `active` / `accepting_orders` | bool nullable |
  | `market_lifecycle_state` | candidate / prefetched / active / retired |
  | `tick_size` / `min_size` | decimal-compatible nullable |
  | `fee_schedule_raw` / `reward_eligibility_raw` | JSON string nullable |
  | `metadata_source` | new_market / active_scan / metadata_snapshot / fallback_close |
  | `same_condition_market_verified` | bool nullable |

  验证：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test polymarket_metadata_layout_contract -- --nocapture
  ```

  预期：`new_market`、active scan metadata、fallback close metadata 均能写出 typed metadata row；slug-only candidate 不得被标记为 complete discovery。

### Task 3: 非 Polymarket 数据源 hourly typed layout 合同

**Files:**
- Create: `data-collect/src/source_hourly_layout.rs`
- Modify: `data-collect/src/lib.rs`
- Modify: `data-collect/src/source_format.rs`
- Create: `data-collect/tests/source_hourly_layout_contract.rs`

- [x] **Step 3.1: 先写失败测试，证明非 Polymarket 不能只靠 opaque raw_json**

  新增测试断言：

  - `binance_depth_snapshot` hourly row 必须有 `last_update_id`、`bids`、`asks`、`snapshot_fetch_start_utc`、`snapshot_recv_time_utc`、`recv_time_utc`、`source_endpoint`。
  - `binance_depth_update` hourly row 必须有 `event_type=e`、`event_time=E`、`symbol=s`、`first_update_id=U`、`final_update_id=u`、`bids=b`、`asks=a`、`recv_time_utc`、`snapshot_last_update_id`、`local_u_before`、`local_u_after`、`stitch_verdict`。
  - `binance_aggtrade` hourly row 必须有 `e`、`E`、`s`、`a`、`p`、`q`、`f`、`l`、`T`、`m` / `buyer_is_maker`、`recv_time_utc`。
  - `polymarket_rtds_chainlink_btc_usd` hourly row 必须有 RTDS message timestamp、payload timestamp、symbol、value、topic、subscription filter、`recv_time_utc`。
  - 只有 `source_kind/raw_json` 而缺少上述 typed 字段时，contract 必须失败。

  验证 RED：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test source_hourly_layout_contract -- --nocapture
  ```

- [x] **Step 3.2: 实现非 Polymarket source row 类型**

  `source_hourly_layout.rs` 至少定义：

  | Row 类型 | 必须字段 |
  | --- | --- |
  | `BinanceDepthSnapshotRow` | `timestamp_received`、`snapshot_fetch_start_utc`、`snapshot_recv_time_utc`、`last_update_id`、`bids`、`asks`、`symbol`、`source_endpoint`、snapshot status、collector 字段、schema metadata |
  | `BinanceDepthUpdateRow` | `timestamp_received`、`event_time`、`symbol`、`first_update_id`、`final_update_id`、`bids`、`asks`、`stream`、`snapshot_last_update_id`、`local_u_before`、`local_u_after`、`stitch_verdict`、`stitch_gap_reason` nullable、collector 字段、schema metadata |
  | `BinanceAggTradeRow` | `timestamp_received`、`event_time`、`trade_time`、`symbol`、`aggregate_trade_id`、`price`、`quantity`、`first_trade_id`、`last_trade_id`、`buyer_is_maker`、collector 字段、schema metadata |
  | `RtdsChainlinkBtcUsdRow` | `timestamp_received`、message timestamp、payload timestamp、`symbol`、`value`、`topic`、subscription filter、collector 字段、schema metadata |

  数组字段 `bids` / `asks` 必须保留 price-size levels，不能只保存长度。

- [x] **Step 3.3: 实现 source event -> row 转换**

  从现有 Binance / RTDS 采集路径产出的 event 中转换 typed rows。保留需求要求的 source evidence；如某源没有 exchange timestamp，必须写 snapshot boundary 或 unavailable reason，不得伪造。

  验证：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test source_hourly_layout_contract -- --nocapture
  ```

- [x] **Step 3.4: 接入 Binance depth stitch evidence**

  将现有 Binance depth snapshot/update parser 与 stitch predicate 接入训练采集写入链路。必须记录：

  - snapshot `lastUpdateId`。
  - buffered updates 中 `u <= lastUpdateId` 的 stale/drop 计数。
  - 首个可用 update 是否满足 `U <= lastUpdateId + 1 <= u`。
  - 后续 update 是否满足 `U <= local_u + 1 <= u` 或可解释为 stale/duplicate。
  - `U > local_u + 1` 时记录 source gap、resnapshot request 和新 snapshot receipt。
  - `local_u_after` 单调推进。

  验证：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test binance_depth_stitch_contract -- --nocapture
  ```

### Task 4: 全源 hourly parquet writer

**Files:**
- Modify: `data-collect/src/writer.rs`
- Modify: `data-collect/src/manifest.rs`
- Create or modify: `data-collect/tests/parquet_writer_contract.rs`
- Create: `data-collect/benches/source_hourly_layout_probe.rs`

- [x] **Step 4.1: 写 writer schema 测试**

  测试必须读取实际写出的 parquet schema，并断言：

  - Polymarket orderbook 文件不再只有共用 `raw_json` 宽表。
  - Polymarket 存在 `timestamp_received`、`timestamp`、`market`、`event_type`、`asset_id`、`bids`、`asks`、`price`、`size`、`side`、`best_bid`、`best_ask`。
  - `polymarket_market_metadata` 存在 condition/token/asset/CLOB ids、start/end、active/acceptingOrders、lifecycle、tick/min size、fee/reward metadata 字段。
  - Binance depth snapshot/update、aggTrade、RTDS Chainlink 文件分别存在 Task 3 定义的 typed 字段。
  - `book` 样例行的 `bids/asks` 非空。
  - 每个 source parquet metadata 包含对应 `parquet_raw_layout_version`、`parquet_raw_source_kind`、`data_collect_artifact_kind=typed_raw_parquet`。

  验证 RED：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect parquet_writer_writes_pmxt_like_orderbook_schema -- --nocapture
  ```

- [x] **Step 4.2: 增加 schema-specific writer**

  在不破坏既有 atomic publish 语义的前提下，为所有训练 source kind 增加 schema-specific writer。实现要求：

  - file I/O、compression、close/finalize、sha256 仍在 writer worker 或 blocking pool 中执行。
  - running -> digesting -> final 的 atomic publish 语义不变。
  - event-specific nullable columns 不得把缺失字段写成错误默认值。
  - Decimal128 / string decimal 方案以 Apache Arrow Rust 支持和测试结果为准；若 Decimal128 实现导致大范围复杂化，先用 string decimal 保留原始精度并在 changelog 记录，不能手写不可靠 decimal parser。

- [x] **Step 4.3: 压缩与排序 benchmark**

  新增 bench 生成或读取一小时四源样本，分别测试：

  - 全源 streaming receive order + ZSTD。
  - Polymarket `(market, asset_id, timestamp_received)` sorted + ZSTD。
  - Binance / RTDS source-specific sort order + ZSTD。
  - ZSTD level 3 / 9 的 CPU、RSS、finalize latency、file size。

  命令：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo bench -p data-collect --bench source_hourly_layout_probe -- --sample-size 10
  ```

  验收：

  - 如果 sorted + ZSTD(9) 在数据采集服务器上 RSS/CPU/finalize latency 可接受，则训练 cold path 采纳排序。
  - 如果排序需要历史全量扫描、内存峰值不可接受或显著影响采集主循环，则不采纳排序，manifest 记录 `sort_order=receive_order`，并请求用户决策是否接受更大文件。

### Task 5: UTC day/hour partition 与 hourly finalize

**Files:**
- Create: `data-collect/src/hourly_partition.rs`
- Modify: `data-collect/src/publish.rs`
- Modify: `data-collect/src/runtime.rs`
- Modify: `data-collect/src/live_runtime.rs`
- Modify: `data-collect/docs/training-collector-deploy-readme.md`
- Modify: `data-collect/deploy/systemd/data-collect-training-collector.service.example`
- Modify: `data-collect/tests/partition_publish_contract.rs`
- Modify: `data-collect/tests/causal_time_contract.rs`
- Create: `data-collect/tests/hourly_partition_contract.rs`
- Modify: `data-collect/tests/training_collector_runtime_contract.rs`
- Create: `data-collect/tests/manual_review_complete_market_contract.rs`

- [x] **Step 5.1: 写 hourly partition 测试**

  测试路径口径：

  ```text
  <dataset_root>/training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market/polymarket_market_2026-07-07T07.parquet
  <dataset_root>/training/typed_raw/utc_day=2026-07-07/source_kind=binance_depth_update/binance_depth_update_2026-07-07T07.parquet
  <dataset_root>/training/typed_raw/utc_day=2026-07-07/source_kind=binance_aggtrade/binance_aggtrade_2026-07-07T07.parquet
  <dataset_root>/training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_rtds_chainlink_btc_usd/polymarket_rtds_chainlink_btc_usd_2026-07-07T07.parquet
  ```

  测试断言：

  - day 文件夹按 `recv_time_utc` UTC day 创建。
  - 新路径为 day-first：`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/...`。
  - 旧路径 `training/typed_raw/source_kind=<source_kind>/utc_day=YYYY-MM-DD/...` 不再作为新产物写入路径；若需要兼容历史只读数据，必须在 discovery 中显式标记 legacy，不得与新产物混写。
  - `07:59:59.999` 写入 `2026-07-07T07`。
  - `08:00:00.000` soft rollover 到 `2026-07-07T08`。
  - running/digesting 文件不被 finalized discovery 返回。

  验证 RED：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test hourly_partition_contract -- --nocapture
  ```

- [x] **Step 5.2: 实现 hourly bucket manager**

  `HourlyPartitionPolicy` 必须输出：

  - `utc_day`
  - `utc_hour`
  - `hour_start_utc_ms`
  - `hour_end_utc_ms`
  - `running_path`
  - `digesting_path`
  - `final_path`
  - `manifest_path`

  文件名必须安全，不包含用户输入 path traversal。

- [x] **Step 5.3: runtime 接入 hour boundary rollover**

  writer worker 当前按 `max_segment_duration` 和 `utc_day` soft rollover；修改为训练 cold path 默认按 UTC hour 逻辑 partition finalize。保留 `max_rows` / `max_bytes` 作为保护性 soft rollover，但正常目标是每 source kind 每小时一个 finalized file。

  如果保护性 `max_rows` / `max_bytes` 在同一小时内触发，文件名增加 `part=N`，manifest 写 `hour_part_count` 和 `rollover_reason=max_rows|max_bytes`，不能停机。

- [x] **Step 5.4: 增加完整 5 分钟 market 人工审查运行模式**

  为训练 collector 增加只用于验收的受控运行模式，必须接入 `data-collect/src/runtime.rs` 的 CLI/config 解析、训练部署 README、systemd 示例和 release artifact 文档。CLI 形式固定为：

  ```bash
  data-collect-training-collector \
    --dataset-root ~/btc-updown-marketmaker/datasets \
    --run-id <run_id> \
    --collector-region eu-west-1 \
    --stop-after-complete-polymarket-market 1
  ```

  config 文件可提供等价字段，但 CLI 必须有合同测试。该模式必须：

  - 等待并采集一个完整 `btc-updown-5m` market，不把启动时已经过半的 partial market 当作完整 market。
  - 从目标 market planned start 前 `T-30s` prefetched window 开始，采到 market end 后至少 `10s` 或观测到同一 market 的 retired/resolution/fallback close evidence。
  - 四源都继续采集同一 UTC window：Polymarket market、Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink。
  - 完成后走正常 graceful shutdown，把当前 hourly running part 以 `rollover_reason=manual_review_complete_market_shutdown` 或等价 reason finalize，避免人工审查必须等待完整 UTC hour。
  - manifest 明确标记这是 `manual_review_complete_market` run，不把该测试模式误当作生产 hourly cadence。
  - run manifest 记录 `selected_market_slug`、`selected_condition_id`、`subscribed_asset_ids_for_market`、`planned_start_utc`、`planned_end_utc`、`preload_start_utc` 和 stop reason。

  验证：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test manual_review_complete_market_contract -- --nocapture
  ```

  执行记录：Task 5.4 已接入 CLI/config、run manifest mode、manual review shutdown rollover reason、README 和 systemd 示例。当前本地合同只验证受控模式入口与 manifest 标记；真实 live 源“等待并采集一个完整 5 分钟 market 后自动停止”的状态机仍需在后续 live runtime/远端人工审查 gate 中用真实 artifact 证明，不能仅凭本地 synthetic 合同宣称完成真实市场采集。

### Task 6: Polymarket live runtime 完整 depth 与目标事件修复

**Files:**
- Modify: `data-collect/src/live_runtime.rs`
- Verify: `data-collect/tests/polymarket_contract.rs`
- Verify: `data-collect/tests/polymarket_metadata_layout_contract.rs`
- Verify: `data-collect/tests/source_format_contract.rs`
- Verify: `data-collect/tests/pmxt_orderbook_layout_contract.rs`

- [x] **Step 6.1: 写回归测试锁定当前 bug**

  测试构造 `book` event 后写入 parquet，读取后断言：

  - `bids` / `asks` 字段存在且非空。
  - `raw_json` 或 typed raw evidence 中不得只出现 `bids_len` / `asks_len`。
  - 第一档 bid/ask price 和 size 可读。

  验证 RED：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect polymarket_book_persists_full_depth -- --nocapture
  ```

- [x] **Step 6.2: 修复 `book` 序列化**

  `collect_polymarket_pair()` 的 `book` 分支必须保存完整 depth。若 SDK `event.bids` / `event.asks` 类型无法直接序列化为 PMXT-like `[["price","size"],...]`，必须先查 SDK 类型并写转换函数；不得改用 raw REST、manual signing 或自建 SDK 旁路。

- [x] **Step 6.3: 保留 `best_bid_ask` 但不把它当完整 book**

  `best_bid_ask` 继续作为轻量 quote 事件保存，但 manifest 和 schema 不得把它标记为完整 orderbook snapshot。

- [x] **Step 6.4: 覆盖 Polymarket 全目标事件与 metadata**

  在 SDK-first 前提下接入 market channel 可用事件：`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved`。实现要求：

  - `price_change` 写入 price/size/side/best_bid/best_ask/hash 等 typed fields。
  - `last_trade_price` 写入 price/size/side/transaction hash 或等价 raw evidence。
  - `tick_size_change` 写入 old/new tick size 或等价 raw evidence。
  - `new_market` 写入 `polymarket_market_metadata` row，不被过滤。
  - `market_resolved` 写入同一 condition/market 的 resolved/subscribed asset ids、winning asset/outcome、nullable close reason；缺失时只允许 fallback metadata close evidence，不伪造事件字段。
  - 若官方 SDK 当前 event type 不暴露某字段，必须写 SDK gap / nullable missing reason；不得改用 raw REST、manual signing 或自建 SDK 旁路。

  验证：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test polymarket_contract -- --nocapture
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test polymarket_metadata_layout_contract -- --nocapture
  ```

  执行记录：已核对当前官方 SDK 源码中 `BookUpdate`、`PriceChange`、`LastTradePrice`、`TickSizeChange`、`NewMarket`、`MarketResolved` typed event 和对应订阅方法；`collect_polymarket_pair()` 继续 SDK-first，不引入 raw REST/manual signing/self-SDK。新增 live runtime 库内回归测试覆盖完整 `book` depth raw evidence 和目标事件字段，验证命令为 `TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect live_runtime --lib -- --nocapture`。

### Task 7: 50GB training retention

**Files:**
- Create: `data-collect/src/retention.rs`
- Modify: `data-collect/src/runtime.rs`
- Modify: `data-collect/src/live_runtime.rs`
- Modify: `data-collect/src/resource.rs`
- Modify: `data-collect/src/manifest.rs`
- Create: `data-collect/tests/training_retention_contract.rs`

- [x] **Step 7.1: 写 retention 合同测试**

  用临时目录模拟：

  - `utc_day=2026-07-05` finalized complete，包含多个 `source_kind` 和 `part=N`，所有 manifest verdict 为 complete。
  - `utc_day=2026-07-06` finalized complete。
  - `utc_day=2026-07-07` current day，包含 running parquet。
  - `utc_day=2026-07-04` 含 finalized parquet 但 manifest incomplete。
  - `manual-inspection/`、`run_manifests/`、startup receipts 和非 `typed_raw/utc_day=*` 目录。
  - dataset bytes 超过测试阈值。

  断言：

  - 删除 `2026-07-05`。
  - 不删除 current day。
  - 不删除 running/digesting。
  - 不删除 incomplete day。
  - 不删除 `manual-inspection`、`run_manifests`、startup receipts 或非 `training/typed_raw/utc_day=*` 目录。
  - 被删除路径 canonicalize 后必须位于 dataset root 内，且 path shape 必须匹配 `training/typed_raw/utc_day=*`。
  - 写出 run-level retention policy/no-op receipt；真实删除时另写 deletion receipt。

  验证 RED：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test training_retention_contract -- --nocapture
  ```

- [x] **Step 7.2: 实现 retention policy**

  默认策略：

  ```text
  training_dataset_max_bytes = 50_000_000_000
  retention_scan_interval_secs = 60
  delete_unit = oldest finalized complete UTC day under training/typed_raw/utc_day=*
  protected_units = current UTC day, running files, digesting files, incomplete day
  production_delete_mode = dry_run by default; enabled only by explicit config or separate user approval
  ```

  `finalized complete UTC day` 判定必须同时满足：

  - day 目录 canonical path 在 `~/btc-updown-marketmaker/datasets/training/typed_raw/` 内。
  - 目录名形如 `utc_day=YYYY-MM-DD`。
  - 不等于当前 UTC day。
  - 递归扫描无 `*-running.parquet`、`*-digesting.parquet`、`*.tmp`、`.rsync-partial`。
  - 该 day 下所有 source/part manifest 均存在且 verdict 为 complete，且 required files 指向 finalized artifact。
  - 目录不包含 unknown file role；未知文件只能使 day 变为 non-deletable，不能被静默删除。

  deletion receipt 字段至少包含：

  - `threshold_bytes`
  - `dataset_bytes_before`
  - `filesystem_free_bytes_before`
  - `deleted_utc_day`
  - `deleted_path`
  - `deleted_bytes`
  - `dataset_bytes_after`
  - `filesystem_free_bytes_after`
  - `reason`
  - `verdict`
  - `delete_mode=enabled`
  - `candidate_complete_day_verdict`

- [x] **Step 7.3: runtime 集成**

  retention 在以下时点触发，但默认生产根只写 dry-run/no-op policy receipt；只有显式配置 `retention_delete_mode=enabled` 或用户单独批准的运行才允许真实删除：

  - collector startup 后。
  - 每个 hourly file finalize 后。
  - 每 `60s` 的低频后台检查。

  retention 的目录扫描和删除必须在 blocking pool 或后台 worker 执行，不能阻塞 source event loop。

  执行记录：已完成训练 runtime 启动时默认 dry-run/no-op run-level receipt，并在 synthetic/live run manifest 中引用 receipt path；live 主循环每 `60s` 通过 blocking pool 执行 retention scan；writer blocking worker 每次 finalized segment 后写 source-specific retention receipt。默认仍为 dry-run，不做真实删除。后续 Step 10.4 已将 source-specific retention receipt 判定为过度产物，默认生产路径必须删除该写法。

- [x] **Step 7.4: run-level retention policy / no-op receipt**

  retention 不要求每个 hourly manifest 都必须有 deletion receipt。实现必须写 run-level `retention_policy_receipt`：

  - 未触发删除时记录 `action=noop`、threshold、dataset bytes、filesystem free bytes、delete mode 和 next scan time。
  - dry-run 时记录 candidates、would_delete paths、would_free bytes，但不删除。
  - 真实删除时记录 deletion receipt，并由 affected hourly/run manifest 可选引用。
  - hourly manifest 缺少 deletion receipt 不得导致 incomplete；只有 retention policy receipt 缺失或 retention hard threshold 未记录才影响 run verdict。

- [x] **Step 7.5: 无可删 complete day 的 fail-closed**

  若超过 50GB 且没有可删除 finalized complete UTC day：

  - 写 ResourceWatermark hard threshold。
  - 当前 running segment 尽量 finalize 或标记 incomplete。
  - 停止接收新 market window，避免写满磁盘。
  - manifest verdict 不得伪装为 complete。

### Task 8: manifest、文档与消费者边界

**Files:**
- Modify: `data-collect/src/manifest.rs`
- Modify: `data-collect/docs/training-collector-deploy-readme.md`
- Modify: `data-collect/docs/paper-live-hot-path-integration.md`
- Modify: `data-collect/docs/acceptance-matrix.md`
- Modify: `data-collect/tests/manifest_contract.rs`
- Modify: `data-collect/tests/paper_live_boundary_contract.rs`

- [x] **Step 8.1: manifest 增加 PMXT mapping receipt**

  每个 Polymarket hourly parquet manifest 增加：

  - `layout_family=pmxt_inspired_polymarket_orderbook`
  - `pmxt_reference_url`
  - `schema_version=polymarket_orderbook_v1`
  - `hour_start_utc`
  - `hour_end_utc`
  - `utc_day`
  - `sort_order`
  - `compression`
  - `book_rows`
  - `book_rows_with_bids_asks`
  - `price_change_rows`
  - `metadata_rows`
  - `new_market_rows`
  - `market_resolved_rows`
  - `tick_size_change_rows`
  - `covered_market_slugs`
  - `covered_condition_ids`
  - `covered_asset_ids`
  - `retention_policy_digest`
  - `retention_policy_receipt_path`
  - `deletion_receipt_path` nullable；无删除动作时不得要求非空

  每个非 Polymarket hourly parquet manifest 增加：

  - `layout_family=source_specific_hourly_typed_raw`
  - `schema_version`
  - `source_kind`
  - `hour_start_utc`
  - `hour_end_utc`
  - `utc_day`
  - `sort_order`
  - `compression`
  - `required_typed_fields_present=true|false`
  - source-specific row counters，例如 `depth_update_rows`、`snapshot_rows`、`aggtrade_rows`、`rtds_rows`
  - Binance depth manifest 还必须包含 stitch counters：`stitch_applied_rows`、`stale_update_rows`、`gap_rows`、`resnapshot_count`、`local_u_min/max`。
  - `retention_policy_digest`
  - `retention_policy_receipt_path`
  - `deletion_receipt_path` nullable；无删除动作时不得要求非空

  所有 hourly/run manifest 必须保留既有证据合同，不得因为新增 PMXT-like layout 而丢失：

  - `required_files[]`
  - complete/incomplete verdict
  - source health artifact path/digest
  - source gap artifact path/digest
  - reconnect trace artifact path/digest
  - code version / git commit
  - collector binary sha256
  - source config digest
  - collector host/region
  - ResourceWatermark

- [x] **Step 8.2: 更新训练部署 README**

  README 必须说明：

  - 目录结构。
  - 每小时逻辑 partition 与 `part=N` 文件如何人工检查。
  - 如何确认 `book` 行有完整 depth。
  - retention 删除最老 complete day 的行为、dry-run 默认、启用真实删除的显式配置和 receipt 路径。
  - 50GB 阈值是 training dataset root 阈值。

- [x] **Step 8.3: 更新 paper/live hot path 文档**

  明确：

  - PMXT-like hourly parquet 属于训练 cold path。
  - paper/live hot path 仍消费实时 handoff。
  - paper/live 可读取 hourly parquet 做审计或离线对照，但不能作为实时唯一数据来源。
  - 本计划不生成 labels、features、tick_stream、replay context 或训练切分；`data-pipeline` 只消费 finalized typed raw parquet + manifest + coverage 作为输入。

### Task 9: 本地验证

**Files:**
- No production file edits except tests/docs already listed.

- [x] **Step 9.1: 格式与静态检查**

  ```bash
  cargo fmt --all -- --check
  git diff --check
  CARGO_TARGET_DIR=target/data-collect-ci cargo clippy -p data-collect --all-targets -- -D warnings
  ```

- [x] **Step 9.2: 合同测试**

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --lib --tests --quiet
  ```

- [x] **Step 9.3: PMXT 范例对照测试**

  使用本地范例：

  ```text
  /home/chillchill/btc-updown-marketmaker/polymarket_orderbook_2026-07-07T07.parquet
  ```

  验证：

  - 我们生成的 Polymarket orderbook hourly parquet schema 能被同一检查脚本读取。
  - 12 个 `btc-updown-5m` market 的 `book` 行均有 `bids/asks`。
  - 对至少一个 Up/Down market 的两个 subscribed asset id，读取第一条 `book` 完整 depth，并应用后续 `price_change` 形成最小本地 orderbook replay；校验 replay 后 best bid/ask 与对应 event 字段可解释或记录差异 reason。
  - 过滤后的目标小时文件体积、row count、book rows、price_change rows 写入测试报告；不要求与 PMXT 完全相同，因为采集窗口和订阅范围不同。

- [x] **Step 9.4: 非 Polymarket 源 schema 对照测试**

  使用本地 synthetic fixture 或短时真实样本验证：

  - `binance_depth_snapshot` 文件有 `last_update_id` 和完整 `bids/asks`。
  - `binance_depth_update` 文件有 `U/u/b/a`。
  - Binance depth stitch receipt 证明首个 update 覆盖 snapshot `lastUpdateId`、后续 `local_u` 单调推进；如有 gap，必须记录 resnapshot。
  - `binance_aggtrade` 文件有 `a/p/q/f/l/T/m`。
  - `polymarket_rtds_chainlink_btc_usd` 文件有 symbol/value/topic/timestamps。

  命令：

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --test source_hourly_layout_contract -- --nocapture
  ```

- [x] **Step 9.5: retention 小阈值模拟**

  用临时目录设置 `training_dataset_max_bytes=10MB`，验证删除最老 finalized complete day，并确认 current/running/incomplete/manual-inspection/run manifests 不被删除。

### Task 10: 数据采集服务器验证

**Files:**
- Modify: `data-collect/docs/training-collector-deploy-readme.md`
- Modify: `changelog/data-collect-changelog.md`

- [x] **Step 10.1: 本地构建 release 二进制**

  ```bash
  CARGO_TARGET_DIR=target/data-collect-ci cargo build -p data-collect --release --bin data-collect-training-collector
  data-collect/scripts/export-release-artifacts.sh
  ```

  记录 binary sha256、git commit、release path。

  执行记录：最终用于正式 smoke 的 release 为 `a35582b5488a-pmxt-stitchfix-20260707T120616Z`，`git_commit=a35582b5488a3b71f8505f391e7465b074e0e97c-dirty`，`data-collect-training-collector` sha256 为 `29a427773611394150677f8cdf4fc9dd2ce8673b4fea9108681e82334d4405c9`。

- [x] **Step 10.2: 上传到数据采集服务器**

  只允许数据采集服务器运行二进制，不在远端运行 cargo 编译。

  执行记录：已上传到数据采集服务器 `ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/releases/a35582b5488a-pmxt-stitchfix-20260707T120616Z/`，并将 `~/btc-updown-marketmaker/data-collect/current` 指向该 release；远端只执行 binary 与 `sha256sum -c SHA256SUMS`，未运行 cargo 编译。

- [x] **Step 10.3: 15 分钟 smoke**

  验证：

  - 至少生成当前 UTC hour running/finalized evidence。
  - `book` 行保存完整 `bids/asks`。
  - Polymarket metadata rows 存在或 manifest 写明未观测 reason；不得把未观测 `new_market` 伪装为 complete metadata。
  - Binance depth snapshot/update、aggTrade、RTDS Chainlink 都生成 typed hourly rows。
  - Binance depth stitch receipt 存在。
  - source gaps 为空或可解释。
  - retention policy/no-op dry-run receipt 存在。

  执行记录：正式 15 分钟 smoke run id 为 `training-smoke-typed-20260707T121004Z`，run manifest verdict `PASS`，`source_gaps=[]`，`source_coverage` 覆盖 `binance_aggtrade`、`binance_depth_snapshot`、`binance_depth_update`、`polymarket_market`、`polymarket_rtds_chainlink_btc_usd`。对应 finalized manifests 均为 source-specific typed schema、`compression=zstd_level_3`、`required_typed_fields_present=true`；Polymarket 三个 part 合计 `book_rows=19808` 且 `book_rows_with_bids_asks=19808`。run-level `retention_policy_receipt.json` 存在，`delete_mode=dry_run`、`verdict=within_threshold`、`action=noop`、`dataset_bytes_before=1744007339`。

- [x] **Step 10.4: 跨小时 2 小时 systemd run**

  验证：

  - 至少跨一个 UTC hour boundary。
  - 连续事件源按 UTC day/hour 生成 finalized parquet；`binance_depth_snapshot` 只要求在 bootstrap、UTC day boundary、reconnect/resync、stitch gap/resync 发生时生成 snapshot evidence，不要求每小时强制生成。
  - Polymarket hourly file 覆盖对应小时内 `btc-updown-5m` windows。
  - manifest 中 `book_rows_with_bids_asks == book_rows`。
  - Polymarket metadata/new_market/market_resolved coverage 以 observed count 或 unavailable/gap reason 进入 manifest。
  - Binance / RTDS hourly manifests 的 `required_typed_fields_present=true`。
  - Binance depth stitch counters 进入 manifest；`local_u` 单调推进或 gap/resnapshot 可解释。
  - partial/running/digesting 不被 discovery 发现。
  - ResourceWatermark 记录 CPU、RSS、disk/dataset bytes、writer queue、flush/finalize latency。

  历史失败记录：`training-systemd-20260707T123357Z` 已在数据采集服务器运行约 2 小时并跨过 `13:00`、`14:00` UTC hour boundary，但该 gate 未通过，不能作为完成证据。失败事实：`systemctl --user stop` 后服务未在默认 stop timeout 内完成退出，被 systemd `SIGKILL`，状态为 `Result=timeout`、`ExecMainStatus=9`，且没有写出当前 run manifest `training/run_manifests/training-systemd-20260707T123357Z.manifest.json`。同时，hourly segment 证据显示 `binance_aggtrade`、`binance_depth_update`、`polymarket_market`、`polymarket_rtds_chainlink_btc_usd` 在 T12/T13/T14 均有 typed finalized manifests，`source_gaps=[]`；但 `binance_depth_snapshot` 只有启动小时 T12 的 `1` 行 snapshot，T13/T14 没有 snapshot manifest。用户已确认修复口径：Binance depth snapshot 不按小时强制刷新；只在 bootstrap、UTC 换天、reconnect/resync、stitch gap/resync 时重新写 snapshot。当前 resolution 由后续 Step 10.4.1-10.4.8 跟踪，最终完成状态以后续 Step 10.4.8 gate 为准。

#### Step 10.4 修复子计划：删除过度运行产物、收敛 manifest 证据，并修复 systemd graceful shutdown

**旧计划复审结论**

- `training-systemd-20260707T123357Z` 的 journal 显示 `systemctl --user stop` 后进入 `stop-sigterm`，`90s` 后 systemd `SIGKILL`，最终 `Result=timeout`、`ExecMainStatus=9`。
- 当前 run 没有写出 `training/run_manifests/training-systemd-20260707T123357Z.manifest.json`，但 runtime artifacts 已写出 `source_coverage.json`、`source_gaps.json`、`retention_policy_receipt.json`、source-specific `retention_after_finalize_*.json` 和多数 hourly segment manifests。
- source-specific shutdown finalize 实际已发生：T14 的 `binance_aggtrade`、`binance_depth_update`、`polymarket_market`、`polymarket_rtds_chainlink_btc_usd` manifest 都以 `shutdown_finalize` 结束，且 `source_gaps=[]`。
- `runtime_artifacts/training-systemd-20260707T123357Z/write_receipts.jsonl` 达 `384452607` bytes，mtime 到 `SIGKILL` 时刻；代码路径为 `source_writer_worker()` 对每个 raw event push 一条 `LiveWriteReceipt`，`write_runtime_artifacts()` 在 run manifest 前 clone/collect 全部 receipts 并写 JSONL。因此根因判断为：shutdown 关键路径存在 O(events) 巨型 receipt 聚合写盘，超过 systemd stop timeout，导致 run manifest 没机会写出。
- 旧 10.4 子计划仍保留 “bounded sample / optional debug artifact” 思路，不符合用户最新决策。新目标不是压缩这些文件，而是在默认生产路径中删除这些过度产物，把必要证据收敛进 run manifest、segment manifest、run-level retention receipt 和小型聚合字段。
- Binance 官方语义结论保持不变：`<symbol>@depth` diff depth stream 不会自动定期发送完整 snapshot；snapshot 由 REST `/api/v3/depth` bootstrap/resync 获取，后续以 `depthUpdate` 增量衔接。故 T13/T14 没有 `binance_depth_snapshot` manifest 不是 10.4 失败根因；需要修正验收和实现口径，而不是每小时强制 snapshot。

**删除目标**

- 默认生产运行不得生成 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`；对应内容必须进入 run manifest 字段。
- 默认生产运行不得生成 `write_receipts.jsonl`；不得在内存中长期保存 per-event `LiveWriteReceipt`。
- 默认生产运行不得生成 `queue_samples.jsonl`；queue 证据只保留聚合高水位、等待/超时/drop 计数、age/latency stats。
- 不再生成 source-specific `retention_after_finalize_*` receipt；默认只保留 run-level `retention_policy_receipt.json`。只有真实 delete mode 发生删除时，才允许额外写 deletion receipt，并在 run manifest 引用。
- `resource_watermark` 删除大批 `not measured by ...` 占位字段，只记录实际测得指标；不可用指标仅在影响验收时写 compact `unavailable_metrics_summary`。
- 保留的必要证据：hourly typed parquet、每个 finalized parquet 的 segment manifest、run manifest、run-level retention receipt、manifest 内的 source coverage/source gaps/rollover/stitch/queue/writer/shutdown stage 聚合字段。

**Files:**
- Modify: `demand/data-collect-demand.md`
- Modify: `data-collect/src/live_runtime.rs`
- Modify: `data-collect/src/manifest.rs`
- Modify: `data-collect/tests/manifest_contract.rs`
- Modify: `data-collect/tests/training_collector_runtime_contract.rs`
- Modify: `data-collect/tests/deploy_readme_contract.rs`
- Modify: `data-collect/tests/binance_depth_stitch_contract.rs`
- Modify: `data-collect/docs/training-collector-deploy-readme.md`
- Modify: `data-collect/deploy/systemd/data-collect-training-collector.service.example`
- Modify: `changelog/data-collect-changelog.md`
- Modify: `plan/data-collect-pmxt-hourly-retention-plan.md`

- [x] **Step 10.4.1: 写“删除过度产物”合同测试**

  扩展 `data-collect/tests/training_collector_runtime_contract.rs`，构造一个可触发 source coverage、source gaps、rollover evidence、writer latency、queue watermark 和 retention dry-run 的短运行合同，先证明当前实现违反新目标：

  - run artifact dir 不得包含 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`、`write_receipts.jsonl`、`queue_samples.jsonl`。
  - run artifact dir 不得包含 `retention_after_finalize_*.json`。
  - run manifest 必须包含 `source_coverage`、`source_gaps`、`rollover_edges`、`finalized_segments`、`source_writer_stats`、`queue_summary`、`shutdown_stage_timings`、`resource_watermark`。
  - segment manifest 不得再通过 `health_artifact_path`、`gap_artifact_path`、`reconnect_artifact_path` 指向已删除的 `source_coverage.json` / `source_gaps.json`；必须改为内嵌 compact summary、run manifest reference/digest，或在 schema 允许时置空且不影响 complete verdict。
  - run manifest 中 `resource_watermark.unavailable` 不得是大批 `not measured by ...` 占位数组；若存在不可用指标，只允许 compact summary。
  - `source_writer_stats` 必须拆分 event 口径和 typed row 口径：`events_received == events_written + events_dropped`，`typed_rows_written == sum(finalized_segments.row_count for source)`；不得再用 `source_coverage.rows == typed_rows_written + dropped_rows` 这种会误判 Polymarket 多行展开的公式。

  验证命令：

  ```bash
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test training_collector_runtime_contract -- --nocapture
  ```

  预期 RED：当前实现仍写 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`、`queue_samples.jsonl`、`write_receipts.jsonl`，且 writer worker 仍保存 per-event receipt。

  执行记录：已新增/收紧 `training_collector_runtime_contract`、`manifest_contract`、`deploy_readme_contract` 和 `live_runtime` 内部单测。RED 先落在 `SourceWriterOutput` 仍只有 `write_receipts`、没有 `stats` 字段，证明当前实现仍保留 per-event receipt 形态。

- [x] **Step 10.4.2: 删除 per-event write receipt 与 queue sample 数据流**

  修改 `data-collect/src/live_runtime.rs`：

  - 删除 `SourceWriterOutput.write_receipts: Vec<LiveWriteReceipt>` 或将其替换为 `SourceWriterStats` 聚合结构；不得保留 per-event receipt Vec。
  - writer worker 每次写入事件后只更新聚合统计：`source_kind`、`events_received`、`events_written`、`events_dropped`、`typed_rows_written`、`typed_rows_dropped`、`first_recv_time_utc_ms`、`last_recv_time_utc_ms`、`write_events_us` latency stats、`finalized_segment_rows`。
  - `write_events_us` latency stats 使用固定结构：`count`、`min`、`p50`、`p95`、`p99`、`max`。若 count 为 `0`，manifest 必须写明确空统计而不是省略字段。
  - 删除 `write_jsonl(... write_receipts.jsonl ...)` 调用；生产路径不得创建 `write_receipts.jsonl`。
  - 删除 `queue_samples: Vec<QueueSampleReceipt>` 默认收集与 `queue_samples.jsonl` 写出；queue 只保留 `QueueDurationSummary` / tracker 聚合字段。
  - 未使用的 `LiveWriteReceipt`、`QueueSampleReceipt`、`write_jsonl()` 若只服务被删除产物，随本步骤删除。

  验证：

  ```bash
  cargo fmt --all
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test training_collector_runtime_contract -- --nocapture
  ```

  预期 GREEN：合同不再发现 `write_receipts.jsonl` / `queue_samples.jsonl`，manifest 中仍有 writer latency 和 queue 聚合统计。

  执行记录：已删除默认生产路径中的 `LiveWriteReceipt`、`QueueSampleReceipt`、`write_jsonl()` 和 `write_runtime_artifacts()`；`SourceWriterOutput` 改为 `SourceWriterStats`，只保留 event/typed row 计数和固定桶 latency stats，不再保存每事件 receipt。

- [x] **Step 10.4.3: 收敛 run manifest，删除重复 runtime sidecar JSON**

  修改 live runtime shutdown 写出顺序：

  - `source_coverage`、`source_gaps`、`rollover_edges` 只写入 run manifest，不再写独立 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`。
  - run manifest 必须使用 temp file + atomic rename 写出，避免 `fs::write` 半截文件。
  - run manifest 必须包含足以判断本 run 的最小字段：`verdict`、`source_coverage`、`source_gaps`、`rollover_edges`、`finalized_segments`、`source_writer_stats`、`queue_summary`、`retention_policy_receipt_path`、`shutdown_stage_timings`、`resource_watermark`。
  - `queue_summary` 使用固定结构：`source_event_queue` 和 `writer_chunk_queue` 两个对象；每个对象至少包含 `rows_high`、`bytes_high`、`backpressure_wait_count`、`backpressure_timeout_count`、`dropped_rows`、`longest_contiguous_above_threshold_ms`，以及当前 `QueueDurationSummary` 已有的 age/latency 统计字段。
  - `shutdown_stage_timings` 使用按 stage name keyed object，stage 至少包括 `source_task_stop`、`event_queue_drain`、`flush_batches`、`writer_finalize`、`retention_shutdown_scan`、`run_manifest_atomic_write`、`post_manifest_cleanup`；每个 stage 至少包含 `started_utc_ms`、`ended_utc_ms`、`duration_ms`、`status=success|skipped|timeout|failed`、`error` nullable。
  - run manifest 必须记录 `run_manifest_atomic_write.status=success`、`duration_ms` 和最终 manifest path；远端 gate 不得只检查字段存在。
  - `shutdown_stage_timings` 作为 run manifest 字段写入，不再默认生成 `shutdown_stage_timings.json` sidecar。
  - `resource_watermark` 只保留实际测得指标，例如 RSS、artifact dir bytes、queue rows/bytes high、source/writer queue summary；删除 `not measured by phase7...` 这类占位数组。
  - 修改 `data-collect/src/manifest.rs` 的 complete 判定：删除 sidecar 后，segment manifest 不得继续要求 `health_artifact_path` / `gap_artifact_path` / `reconnect_artifact_path` 指向文件；改为接受 compact summary、run manifest reference/digest 或显式 nullable 字段，且合同测试必须证明不会留下指向不存在 sidecar 的路径。

  验证：

  ```bash
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test training_collector_runtime_contract -- --nocapture
  ```

  新增断言：即使没有任何 runtime sidecar JSON，run manifest 仍完整、可 parse、可判定 verdict，且能定位 coverage/gap/rollover/writer/queue/shutdown stage。

  执行记录：run manifest 现在包含 `source_coverage`、`source_gaps`、`rollover_edges`、`finalized_segments`、`source_writer_stats`、`queue_summary`、`shutdown_stage_timings`、`shutdown_retention_scan_budget_ms`、`resource_watermark`；segment manifest 默认不再引用 `source_coverage.json` / `source_gaps.json`，manifest complete 判定允许显式 nullable 字段并拒绝旧 sidecar 路径。复审后已将 run manifest 改为单次 atomic publish：`run_manifest_atomic_write` 预声明在最终 payload 内，只有最终 `tmp -> rename` 成功后 manifest 才可见，不再先写一份中间 manifest 再覆盖。二轮复审后进一步收紧 `ManifestDraft::effective_verdict()`：只要 `source_gaps` 或 `manifest_source_gaps` 非空，即使其他字段完整，也必须判为 `Incomplete`，避免成功 resync/retry 把源缺口掩盖成完整。

- [x] **Step 10.4.4: 删除 source-specific retention_after_finalize receipt**

  修改 retention 集成：

  - writer finalize 后不再按 source 写 `retention_after_finalize_<source>.json` 或同类文件。
  - 默认 dry-run/no-op retention 只保留 run-level `retention_policy_receipt.json`，路径由 run manifest 的 `retention_policy_receipt_path` 指向。
  - 如果未来启用真实删除，只有实际删除发生时才写 deletion receipt；deletion receipt 必须记录 deleted day、deleted files、bytes、delete mode、operator approval/config，并由 run manifest 引用。
  - final retention scan 不得无限阻塞 run manifest 写出。实现必须二选一：在固定预算 `shutdown_retention_scan_budget_ms` 内完成并把结果写入 run manifest / `retention_policy_receipt`；或先写可判定 run manifest，并记录 `retention_shutdown_scan.status=skipped|timeout`，依赖 startup/periodic run-level receipt 作为当前 retention 证据。
  - `shutdown_retention_scan_budget_ms` 必须进入 run manifest；超过预算时不得等到 systemd SIGKILL。

  验证：

  ```bash
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test training_retention_contract -- --nocapture
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test training_collector_runtime_contract -- --nocapture
  ```

  预期：dry-run/no-op 路径只有 run-level retention receipt；真实删除合同仍能 fail closed 并保留 deletion evidence。

  执行记录：writer finalize 后不再写 `retention_after_finalize_<source>.json`；periodic retention scan 改为 scanner 内部协作 deadline，且等待 scan 时继续监听 shutdown signal；超预算只记录 `retention_periodic_scan_error:*`，不得绕过最终 run manifest。retention complete-day discovery 和真实删除路径都已改为 deadline-aware，不再调用无 deadline 的 `discover_finalized_artifacts()` 或 `remove_dir_all()`。shutdown stop 路径不再启动新的全盘 retention scan，而是记录 `retention_shutdown_scan.status=skipped`，依赖 startup/periodic run-level `retention_policy_receipt.json` 作为 retention 证据，避免不可 abort 的 blocking scan 拖到 systemd SIGKILL。writer worker 已从 `tokio::task::spawn_blocking` join handle 改为独立 `std::thread` + result channel；shutdown 先 drop 所有 writer sender，再在 `WRITER_FINALIZE_BUDGET_MS=30000` 总预算内收集结果。writer finalize timeout/失败时不再直接 `?` 跳过 run manifest，而是写 `writer_finalize_failed:*` source gap、`shutdown_stage_timings.writer_finalize.status=failed` 和可判定 run manifest。

- [x] **Step 10.4.5: 修正 Binance depth snapshot 计划与合同口径**

  修改测试和实现口径：

  - `binance_depth_snapshot` 是 bootstrap / UTC day boundary / reconnect-resync / stitch gap-resync evidence，不要求每小时都有 snapshot manifest。
  - 当前 source 是 Binance Spot `stream.binance.com` + `/api/v3/depth`；Spot diff depth payload 没有 `pu`，不得把 futures 的 `pu` 当成 Spot 硬验收。Spot 只检查 `U/u/local_u` 连续性；若未来支持 futures，`pu` 只能作为 futures source-specific optional field。
  - UTC day boundary 到达时，depth source 必须重新执行官方 snapshot bridge 流程：先保留或暂停处理 WS updates，获取 REST snapshot，丢弃 `u <= lastUpdateId` 的 stale update，要求首个可应用 update 满足 `U <= lastUpdateId + 1 <= u` 或等价 bridge predicate，然后记录新的 `binance_depth_snapshot` row，并以新的 `snapshot_last_update_id` 继续 stitch 后续 `depth_update`。
  - 若 WebSocket reconnect、`U > local_u + 1`、连续性检查失败或其他 stitch gap 触发 resync，也必须重新执行 snapshot bridge 流程；bridge 失败时不得写 applied update，必须 fail closed 记录 `binance_depth_resync_failed:<reason>` source gap。
  - 每小时必须有 `binance_depth_update` typed rows，且 rows/manifest 必须能追溯到当前有效 `snapshot_last_update_id`、`local_u_before`、`local_u_after` 和 stitch verdict。
  - Segment manifest 或 run manifest 必须记录 per-hour stitch counters/receipt：`stitch_applied_rows`、`stitch_gap_count`、`resnapshot_count`、`local_u_min`、`local_u_max`、当前 `snapshot_last_update_id` 覆盖范围；否则只看 row count 无法证明 depth stitch 语义。
  - 不实现“每小时强制 REST refresh”，避免不必要 REST weight、额外延迟和非官方要求。

  验证：

  ```bash
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test binance_depth_stitch_contract -- --nocapture
  ```

  新增断言：跨小时不强制 snapshot；跨 UTC day 必须重新 snapshot 且后续 update 引用新的 `snapshot_last_update_id`；gap/reconnect 后必须重新 snapshot 或 fail closed 记录 source gap；Spot 合同不得要求 `pu`。

  执行记录：`binance_depth_stitch_contract` 在全包测试中通过；本次 10.4 修复保留既定口径：`binance_depth_snapshot` 不按小时强制。runtime 已将 depth source 改为 bootstrap / UTC day boundary / reconnect / stitch gap 后重新 REST snapshot，写入新的 `binance_depth_snapshot` event 并重置 `local_u`；snapshot 获取失败时记录 `binance_depth_resync_failed:*` 并 fail closed。segment manifest 内嵌 `binance_depth_stitch` compact evidence，记录 `snapshot_last_update_id`、`stitch_applied_rows`、`gap_rows`、`resnapshot_count`、`local_u_min/max` 和 `first_update_bridged_snapshot`，不新增 sidecar JSON。复审后修正了 `gap_rows/resnapshot_count` 假完整问题：resnapshot trigger 会写入后续首个 applied update 的 compact raw evidence，segment manifest 由真实事件累加；reconnect 增加固定 retry budget/backoff。二轮复审后进一步修正成功路径：WS reconnect 成功也记录 `binance_depth_reconnect_required:*` source gap；`U > local_u + 1` 等 stitch gap 即使成功 REST resnapshot，也先记录 `binance_depth_update_stitch_gap:*` source gap，最终 run manifest 不得把该 run 判为 PASS。第三轮复审后补齐 bootstrap WS connect error/timeout 的 retry budget/backoff，不再单次失败退出；同时把 `stitch_gap:*` 和 `reconnect:*` resnapshot trigger 映射进 segment manifest `source_gaps`，使局部 segment manifest 也不能把 stitch/reconnect gap 判为 `Complete`。

- [x] **Step 10.4.6: systemd stop 回归测试与部署文档同步**

  修改部署 README、service 示例和 changelog：

  - README 写明 2 小时 gate 的 stop 验收必须看到 `Result=success`、`ExecMainStatus=0`、run manifest 存在、无 `.running` / `.digesting` 残留属于当前 run。
  - README 明确新产物口径：不要检查 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`、`write_receipts.jsonl`、`queue_samples.jsonl` 或 `retention_after_finalize_*`；这些文件在默认生产路径中应不存在。
  - README 的远端检查脚本改为只解析 run manifest、segment manifest、run-level retention receipt 和 parquet 文件。
  - `data-collect/tests/deploy_readme_contract.rs` 必须新增断言：README 明确这些 sidecar/debug 文件默认不存在，远端检查命令不依赖这些文件。
  - service 示例可保留默认 systemd timeout，但不得依赖提高 `TimeoutStopSec` 掩盖 shutdown O(events) 问题；如果增加 `TimeoutStopSec`，只能作为安全余量，并必须在 changelog 说明不是根因修复。
  - 本地测试增加 shutdown 关键路径 fault injection：writer finalize 慢、final retention scan 慢、manifest atomic write 失败时，状态必须明确；不得通过写额外 debug sidecar 证明。

  本地验证：

  ```bash
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --test deploy_readme_contract -- --nocapture
  ```

  执行记录：`training-collector-deploy-readme.md` 已明确默认不检查/不生成旧 sidecar，远端验收改查 run manifest 聚合字段；`deploy_readme_contract` 已覆盖这些文档要求。二轮复审后补充 `writer_finalize_timeout_returns_error_without_waiting_for_worker` fault-injection 测试，证明 hung writer result 不会让 stop 路径无限等待到 systemd `SIGKILL`。

- [x] **Step 10.4.7: 完整本地验证**

  ```bash
  cargo fmt --all -- --check
  git diff --check
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo clippy -p data-collect --all-targets -- -D warnings
  TMPDIR=/home/chillchill/btc-updown-marketmaker/target/tmp \
  CARGO_TARGET_DIR=target/data-collect-ci \
  cargo test -p data-collect --lib --tests --quiet
  ```

  预期：全部通过；不得引入 raw REST/manual signing/自建 SDK 旁路；默认生产路径不得创建被删除的 runtime sidecar/debug 文件。

  执行记录：二轮复审修复后已通过 `cargo fmt -p data-collect`、`git diff --check`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect live_runtime --lib -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect --test manifest_contract -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect --test training_collector_runtime_contract -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect --test deploy_readme_contract -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo clippy -p data-collect --all-targets -- -D warnings`。第三轮 Binance 审查修复后，新增 `binance_depth_bootstrap_connect_failure_uses_retry_budget`，并将 `binance_depth_manifest_counts_resnapshot_trigger_evidence` 扩展为断言 stitch gap segment manifest `effective_verdict=Incomplete`；随后重新通过 `cargo fmt -p data-collect`、`git diff --check`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo clippy -p data-collect --all-targets -- -D warnings`。第三轮聚焦复审返回 `PASS_WITH_NOTES`，确认两个 blocker 已闭合，剩余 note 均不阻塞远端 gate。尾随空白扫描无匹配。由于 `/tmp` tmpfs 对 C 编译临时文件返回 `Disk quota exceeded`，本地验证统一使用 `TMPDIR=/home/chillchill/tmp`。

- [x] **Step 10.4.8: 重新导出、上传并重跑 2 小时 systemd gate**

  重新导出 binary-only release，上传到 `ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/releases/<release>/`，远端只运行二进制，不运行 cargo。

  远端验收必须同时满足：

  - `systemctl --user stop data-collect-training-collector.service` 后 `ActiveState=inactive`、`Result=success`、`ExecMainStatus=0`；`ActiveState=failed` 或 `Result=timeout` 不可接受。
  - 当前 run manifest 存在：`~/btc-updown-marketmaker/datasets/training/run_manifests/<run_id>.manifest.json`。
  - run manifest `source_gaps=[]`，`source_coverage` 覆盖五个 runtime `source_kind`，并能聚合回四个逻辑源。
  - run manifest 包含 `rollover_edges`、`source_writer_stats`、`queue_summary`、`shutdown_stage_timings`、`resource_watermark`，且这些字段足以替代旧 sidecar JSON。
  - run manifest `source_writer_stats` 同时给出 event 口径和 typed row 口径；`events_received == events_written + events_dropped`，`typed_rows_written == sum(finalized_segments.row_count for source)`。
  - run manifest `queue_summary` 至少包含 `source_event_queue` / `writer_chunk_queue`，并记录 high watermark、wait/timeout/drop 和 age/latency summary。
  - run manifest `shutdown_stage_timings.run_manifest_atomic_write.status=success`，并记录各 shutdown stage 的 `duration_ms`、`status` 和错误字段；`shutdown_retention_scan_budget_ms` 必须存在，`retention_shutdown_scan.status=success` 时必须满足 `duration_ms <= shutdown_retention_scan_budget_ms`，超过预算时必须是 `timeout` 或 `skipped`，不得阻塞到 systemd kill。
  - runtime artifact dir 不存在 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`、`write_receipts.jsonl`、`queue_samples.jsonl`、`retention_after_finalize_*.json`。
  - T12/T13/T14 或实际跨越小时中，`binance_aggtrade`、`binance_depth_update`、`polymarket_market`、`polymarket_rtds_chainlink_btc_usd` 有 typed finalized manifests，`required_typed_fields_present=true`。
  - segment manifest 不得引用不存在的 `source_coverage.json` / `source_gaps.json`；health/gap/reconnect 证据必须以内嵌 compact summary、run manifest reference/digest 或显式 nullable 字段表达。
  - `polymarket_market` 每个 manifest 满足 `book_rows_with_bids_asks == book_rows`。
  - `binance_depth_snapshot` 至少有 bootstrap snapshot；若跨 UTC day，必须有 day-boundary snapshot；若发生 reconnect/resync/gap，必须有对应 snapshot 或 source gap evidence。
  - 当前 run 不残留 `.running` / `.digesting` 文件。
  - retention receipt 仍为默认 `dry_run`，`delete_mode=dry_run`，真实删除没有发生。

  执行记录：已重新导出并上传 release `a35582b5488a-step104fix-20260707T202452Z` 到数据采集服务器，`current` symlink 指向该 release；远端 `sha256sum -c SHA256SUMS` 通过，training binary sha256 为 `9763400d5c715e1a13da974835a8e3d46690d89fba059c50e5da7eda99e54671`，service 示例 sha256 为 `d47c37cd484c65fd904a565d7ed424ffe7a8cf6e4ea2b9b104ff9870850fd1f0`，远端未运行 cargo。

  第一次修复后 gate run `training-systemd-20260707T202555Z` 从 `2026-07-07T20:26:00Z` 运行到 `2026-07-07T22:26:00Z`，stop 后 `ActiveState=inactive`、`Result=success`、`ExecMainStatus=0`，run manifest 成功写出，`writer_finalize.duration_ms=70`，`run_manifest_atomic_write.status=success`，runtime artifact dir 只有 `retention_policy_receipt.json`，旧 sidecar 数量为 `0`。但 run manifest verdict 为 `SOURCE_GAP`，`source_gaps=["rtds_business_message_timeout"]`。本地拉回 RTDS parquet 后按 `timestamp_received` 复算，存在真实接收断档 `647241ms`，超过当前 `RTDS_FRESHNESS_TIMEOUT_SECS=60`，因此本 run 不能通过 `source_gaps=[]` 验收。

  第二次复跑 gate run `training-systemd-20260707T223219Z` 从 `2026-07-07T22:32:19Z` 运行到 `2026-07-08T00:32:57Z`，跨 `23:00Z` hour boundary 和 `2026-07-08T00:00:00Z` UTC day boundary。运行中 `NRestarts=0`；stop 后 `ActiveState=inactive`、`SubState=dead`、`Result=success`、`ExecMainStatus=0`，journal 显示正常 `Stopped`，CPU time `13min 53.729s`、memory peak `263.2M`、swap peak `0B`。run manifest 写出到 `training/run_manifests/training-systemd-20260707T223219Z.manifest.json`，`writer_finalize.duration_ms=78`，`retention_shutdown_scan.status=skipped`，`run_manifest_atomic_write.status=success`，`resource_watermark` 只包含实测 `artifact_dir_bytes=474`、`queue_bytes_high=798560`、`queue_rows_high=1944`、`rss_bytes=91078656`。runtime artifact dir 只有 `retention_policy_receipt.json`，旧 sidecar 和旧 sidecar 引用均为 `0`；retention receipt 为 `delete_mode=dry_run`、`action=noop`、`verdict=within_threshold`、`dataset_bytes_before=2790372200`。

  第二次 run 的数据面验证：`source_coverage` 覆盖 `binance_aggtrade=45996` events、`binance_depth_snapshot=2` snapshots、`binance_depth_update=72367` events、`polymarket_market=3331376` raw events / `6395175` typed rows、`polymarket_rtds_chainlink_btc_usd=3772` events；`source_writer_stats` 全部满足 `events_received == events_written`、`events_dropped=0`、`typed_rows_dropped=0`。`binance_depth_snapshot` 有启动 bootstrap snapshot 和 `2026-07-08T00:00:00Z` day-boundary snapshot；`polymarket_market` segment manifest 合计 `book_rows=101297` 且 `book_rows_with_bids_asks=101297`。当前 run 没有新的 `.running` / `.digesting` 残留；远端仅存在一个历史 `2026-07-07T11` 的 `polymarket_market_2026-07-07T11--part=3-running.parquet`，mtime 早于本次 run，未计入本 gate。

  历史第二次 run 仍不能标记 Step 10.4.8 完成：run manifest verdict 仍为 `SOURCE_GAP`，唯一 `source_gaps=["rtds_business_message_timeout"]`。按本 run manifest 精确选取 RTDS parquet 后复算，`timestamp_received` 存在 `7` 次 `>60000ms` 静默，最大 `1018919ms`；断档集中在 `2026-07-07T22` / `T23` RTDS segment，`2026-07-08T00` segment 最大接收间隔 `8719ms`。当时结论是：Step 10.4 的 systemd stop timeout、run manifest 写出、过度产物删除、Binance depth snapshot 语义和换天 resnapshot 均已远端验证；但当前 `RTDS_FRESHNESS_TIMEOUT_SECS=60` 与真实 RTDS Chainlink BTC/USD 静默间隔冲突，导致 `source_gaps=[]` 验收连续两次失败。Polymarket 官方 RTDS 文档只要求客户端每 5 秒发送 `PING` 保持连接，并定义 Chainlink `crypto_prices_chainlink` 的 update/payload 字段；未给出 Chainlink BTC/USD 每 60 秒必须有业务 update 的 SLA。该历史状态已由后续用户决策覆盖：保持 60s source gap 判定，并增加 10s 循环重发与每 2 次重发后的 SDK client 重建。

  用户后续决策与本地修复：保持 `RTDS_FRESHNESS_TIMEOUT_SECS=60` 作为 `rtds_business_message_timeout` source gap 判定线，不放宽为 heartbeat-only 验收；RTDS Chainlink `btc/usd` 业务消息静默期间，必须每 `10s` 对同一个 `Subscription::chainlink_prices(Some("btc/usd"))` 执行主动重发订阅，一个 `60s` source gap 窗口内最多重发 `6` 次；每累计 `2` 次重发后仍无业务消息，就主动重建 RTDS SDK client/connection 并重新订阅；只有累计静默到 `60s` 仍无业务消息时才记录 `rtds_business_message_timeout`。SDK 对已订阅同 topic 再次 `subscribe_raw` 只会复用本地 refcount，不会真正发送网络 subscribe，因此同 session 内的主动重发采用 `unsubscribe_chainlink_prices()` 后立即用同一订阅重新 `subscribe_raw()` 并替换 stream；SDK 没有公开 `disconnect()` / `reconnect()` API，主动重连采用 SDK-first 的新建 `RtdsClient` + 同一 subscription 重新订阅，不走 raw WebSocket。上一版错误理解的 release `a35582b5488a-rtdsresub-20260708T021150Z` 曾启动 run `training-systemd-20260708T021249Z`，发现语义错误后已中止，服务 stop 后 `ActiveState=inactive`、`Result=success`，该 run 不作为 Step 10.4.8 gate 证据。新增本地回归 `rtds_freshness_schedule_resubscribes_every_10s_reconnects_every_2_and_gaps_at_60` 锁定 `10/20/30/40/50/60s` 六次重发、`20/40/60s` 三次主动重连、`60s` source gap 时间线；已通过 `git diff --check`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect rtds_freshness_schedule_resubscribes_every_10s_reconnects_every_2_and_gaps_at_60 --lib -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo clippy -p data-collect --all-targets -- -D warnings`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect -- --nocapture`。最终以当前实现重新导出的 release、run manifest `source_gaps=[]` 和 RTDS parquet 复算结果作为 Step 10.4.8 完成依据。

  旧复跑记录，已被后续审查覆盖：release `a35582b5488a-rtdsloop-20260708T022532Z` 的 2 小时 systemd run `training-systemd-20260708T022618Z` 曾验证 `verdict=PASS`、`source_gaps=[]`、RTDS 最大接收间隔 `9083ms`、`>60000ms` 断档数 `0`，但审查发现该版本在 `20/40/60s` reconnect 边界会先对旧 client `unsubscribe + subscribe`，随后新建 client 再 subscribe，违反“60s 窗口内最多 6 次重发”的语义。因此该 run 只能作为旧实现诊断证据，不能作为当前代码最终 gate。已新增回归 `rtds_reconnect_action_does_not_resubscribe_old_client_before_new_client` 并修复为：非 reconnect 节拍在当前 client 上 `unsubscribe + subscribe`；reconnect 节拍只对旧 client `unsubscribe`，让新 `RtdsClient` 的初始 `subscribe_raw()` 成为该 10s 节拍的唯一重发。本地已通过 `TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect rtds_reconnect_action_does_not_resubscribe_old_client_before_new_client --lib -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect live_runtime --lib -- --nocapture`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo clippy -p data-collect --all-targets -- -D warnings`、`TMPDIR=/home/chillchill/tmp CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target cargo test -p data-collect -- --nocapture`。该修复已由下方 `a35582b5488a-rtdsloop2-20260708T044122Z` 重新导出和 2 小时 systemd gate 覆盖。

  最终复跑执行记录：重新导出并上传 release `a35582b5488a-rtdsloop2-20260708T044122Z`，`RELEASE_INFO.env` 为 `git_commit=a35582b5488a3b71f8505f391e7465b074e0e97c-dirty`，远端 `sha256sum -c SHA256SUMS` 通过，training binary sha256 为 `87fb8ba7c35f13e12d04323f9d7b2cd70bb196d9cd1bba4b5cf9958de103d2af`。2 小时 systemd run `training-systemd-20260708T044217Z` 从 `2026-07-08T04:42:17Z` 运行到 `2026-07-08T06:42:20Z`，stop 后 `ActiveState=inactive`、`SubState=dead`、`Result=success`、`ExecMainStatus=0`、`NRestarts=0`，journal 记录 CPU time `14min 52.929s`、memory peak `270.8M`、swap peak `0B`。run manifest 写出到 `training/run_manifests/training-systemd-20260708T044217Z.manifest.json`，`verdict=PASS`、`source_gaps=[]`；runtime artifact dir 只有 `retention_policy_receipt.json`，旧 sidecar 数量为 `0`，当前 run `.running` / `.digesting` 残留为 `0`。source coverage 覆盖五个 runtime source：`binance_aggtrade=62483`、`binance_depth_snapshot=1`、`binance_depth_update=72018`、`polymarket_market=3639843` raw events / `6957348` typed rows、`polymarket_rtds_chainlink_btc_usd=7033`。本 run 未跨 UTC day，因此 `binance_depth_snapshot=1` 是启动 bootstrap snapshot，符合不按小时强制 snapshot 的口径。所有 `source_writer_stats` 满足 `events_received == events_written + events_dropped` 且 `typed_rows_written == finalized_segment_rows`；所有 segment manifest `required_typed_fields_present=true`、`validation_verdict=Complete`，`source_gaps=[]`，且不引用已删除 runtime sidecar。Polymarket manifest 汇总 `book_rows=120044` 且 `book_rows_with_bids_asks=120044`。本地拉回 3 个 RTDS parquet 复算 `timestamp_received`：共 `7033` 行，最大接收间隔 `9504ms`，`>60000ms` 断档数为 `0`。retention receipt 为 `delete_mode=dry_run`、`action=noop`、`verdict=within_threshold`、`dataset_bytes_before=3220402879`。Step 10.4.8 与父级 Step 10.4 完成。

- [ ] **Step 10.5: 完整 5 分钟 market 人工审查 run 并拉回本地**

  在数据采集服务器运行本阶段实现后的训练 collector，使用 Step 5.4 的人工审查运行模式，采集一个完整 `btc-updown-5m` market。运行要求：

  - 不使用启动时已经过半的 partial market；必须选择下一完整 market 或明确记录 `selected_market_slug`、`planned_start_utc`、`planned_end_utc`、`preload_start_utc`。
  - 覆盖 `preload_start_utc = planned_start_utc - 30s` 到 `planned_end_utc + 10s` 或 resolved/fallback close evidence。
  - Polymarket `book_rows > 0`，且 `book_rows_with_bids_asks == book_rows`。
  - 目标 market 的两个 subscribed asset id 都必须有可读取的完整 book depth；缺一方只能判 incomplete 或给出 source gap/unavailable reason。
  - 对目标 market 做最小 replay：从第一条 `book` 开始应用后续 `price_change`，校验本地 best bid/ask 与事件字段可解释；不能只检查字段非空。
  - `polymarket_market_metadata` 必须覆盖 selected condition/market 的 condition id、token/asset ids、start/end、active/acceptingOrders、lifecycle、tick/min size；缺字段必须写 unavailable reason，不得伪装完整。
  - Binance depth snapshot/update 在目标 market window 内有 typed rows 和 stitch receipt；`local_u` 单调推进或 gap/resnapshot 可解释。
  - Binance aggTrade、RTDS Chainlink 在目标 market window 内都有 typed rows。
  - source gaps 为空；如不为空，必须逐条解释，不得宣称完整通过。
  - 当前 hourly part 必须通过受控 shutdown finalize，manifest 标记 `manual_review_complete_market`，并记录 `rollover_reason=manual_review_complete_market_shutdown` 或等价 reason。

  拉回本地要求：

  - 从数据采集服务器把该 run 的 parquet、segment manifest、run manifest、run-level retention policy/no-op receipt、可选 deletion receipt、SHA256SUMS 拉回；source coverage/source gaps 只从 run manifest 字段读取，不再拉回 `source_coverage.json` / `source_gaps.json` sidecar：

  ```text
  /home/chillchill/btc-updown-marketmaker/target/manual-inspection/<run_id>/
  ```

  - 本地执行 sha256 校验，生成 `filelist.txt` 和人工检查说明。
  - 最终向用户汇报本地目录、总大小、各 source parquet 行数、Polymarket book 完整 depth 计数、metadata 覆盖、最小 replay 检查、Binance stitch receipt、run manifest 内的 source coverage/source gaps、四源覆盖窗口和未解决风险。

- [ ] **Step 10.6: retention 远端安全验证**

  不直接制造 50GB 大文件。使用独立临时测试根和小阈值验证 deletion 行为；生产 datasets 根首次只做 dry-run 统计。真实删除只允许在 `training/typed_raw/utc_day=*` 且 day complete predicate 通过的路径执行，并且需要显式配置 `retention_delete_mode=enabled` 或单独用户批准。

## 六、验收标准

计划执行完成后必须满足：

1. `polymarket_market` 的 `book` rows 全部保留完整 `bids` / `asks` depth；不存在只保存 `bids_len` / `asks_len` 却标记为 complete book 的产物。
2. Polymarket orderbook hourly parquet 至少包含 `timestamp_received`、`timestamp`、`market`、`event_type`、`asset_id`、`bids`、`asks`、`price`、`size`、`side`、`best_bid`、`best_ask`，并覆盖 `price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`market_resolved` 的 typed fields 或 nullable unavailable reason。
3. `polymarket_market_metadata` hourly parquet 至少包含 condition id、CLOB market id、slug、token/asset ids、start/end、active/acceptingOrders、market lifecycle、tick/min size、fee/reward metadata 或 source unavailable reason。
4. `binance_depth_snapshot` 只在 bootstrap、UTC day boundary、reconnect/resync、stitch gap/resync 时要求生成；生成时对应 parquet 至少包含 `timestamp_received`、`snapshot_fetch_start_utc`、`snapshot_recv_time_utc`、`last_update_id`、`bids`、`asks`、`symbol`、`source_endpoint`。
5. `binance_depth_update` hourly parquet 至少包含 `timestamp_received`、`event_time`、`symbol`、`first_update_id`、`final_update_id`、`bids`、`asks`、`stream`、`snapshot_last_update_id`、`local_u_before`、`local_u_after`、`stitch_verdict`，并有 stitch receipt 证明 snapshot/update 衔接、`local_u` 单调推进或 gap/resnapshot 可解释。
6. `binance_aggtrade` hourly parquet 至少包含 `timestamp_received`、`event_time`、`trade_time`、`symbol`、`aggregate_trade_id`、`price`、`quantity`、`first_trade_id`、`last_trade_id`、`buyer_is_maker`。
7. `polymarket_rtds_chainlink_btc_usd` hourly parquet 至少包含 `timestamp_received`、message timestamp、payload timestamp、`symbol`、`value`、`topic`、subscription filter。
8. 训练 cold path 按 `utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH[--part=N].parquet` 生成 finalized hourly logical partitions；正常每小时一个文件，保护性 soft rollover 可产生多个 part。
9. 每个 hourly file 都有 manifest，manifest 包含 row count、sha256、schema version、source coverage、covered markets/assets 或 source-specific ids、compression/sort receipt、ResourceWatermark、`required_files[]`、complete/incomplete verdict、source health/gap/reconnect compact summary 或 run manifest reference/digest、code version / git commit、collector binary sha256、source config digest、collector host/region 和 retention policy digest；deletion receipt 只在真实删除发生时要求非空。segment manifest 不得引用默认已删除的 `source_coverage.json`、`source_gaps.json` 或 `rollover_edges.json`。
10. running/digesting/tmp/rsync-partial 不进入 finalized discovery。
11. 50GB retention 只会删除最老 finalized complete UTC day；不会删除 current day、running/digesting、incomplete day、manual-inspection、run manifests 或 startup receipts；无可删 day 时 fail closed 并保护已采数据。
12. paper/live hot path 不被改成等待 hourly parquet；hot path 文档明确实时 handoff 仍是运行期消费边界。
13. 本计划不生成 labels、features、tick_stream、replay context 或训练切分；`data-pipeline` 只消费 finalized typed raw parquet、manifest 和 coverage 作为输入。
14. 本地 `cargo fmt`、`git diff --check`、`cargo clippy -D warnings`、`cargo test -p data-collect --lib --tests --quiet` 全部通过。
15. 数据采集服务器 15 分钟 smoke 和跨小时 2 小时 run 通过；若排序/压缩导致 CPU、RSS、finalize latency 或 writer queue 不可接受，必须停止并给用户决策，不得强行宣称通过。
16. 数据采集服务器完成一个完整 `btc-updown-5m` market 人工审查 run，并把 parquet、segment manifest、run manifest、run-level retention policy/no-op receipt、可选 deletion receipt 和 SHA256SUMS 拉回本地 `target/manual-inspection/<run_id>/`；coverage/gap 必须从 run manifest 字段读取，不再依赖 sidecar JSON。本地 sha256 校验、Polymarket 双 token 完整 depth、最小 orderbook replay、metadata 覆盖和 Binance stitch 检查通过后，用户可人工审查数据产物。

## 七、执行风险与决策门

1. **排序风险：** PMXT 的排序对压缩有利，但排序/压缩是否可接受必须以数据采集服务器当前 hardware receipt 和远端 benchmark 为准；AWS paper/live 小主机只保留 hot path 边界，不承担训练 cold path、排序、压缩或重处理。若排序实现需要缓存整小时全部事件并导致 RSS 不可接受，必须改为 receive-order hourly parquet 或请求用户批准更高资源方案。
2. **Decimal128 风险：** 如果 Arrow Decimal128 写入需要大量转换复杂度，优先保证原始价格/size 精度和可读性；可用 string decimal 先通过验收，再单独计划 decimal 优化。
3. **raw_json 边界风险：** PMXT typed fields 不等于逐行原始 JSON。本文计划用完整 typed native fields 作为 raw-equivalent evidence；如果用户要求逐行 byte-identical raw_json，也必须单独评估体积和 CPU 影响。
4. **retention 删除风险：** 默认只做 dry-run/no-op receipt。真实删除只限 `training/typed_raw/utc_day=*` 下 complete day，且必须有显式配置或单独用户批准；任何删除生产数据服务器已有历史数据的动作，除非 receipt 明确证明边界成立，否则必须停止。
5. **hourly file 与 soft rollover 冲突：** 本计划口径是“一小时一个逻辑 partition”，不是“一小时绝对一个物理文件”。若同一小时因 max_rows/max_bytes 产生多个 part 文件，manifest 必须如实记录；不能为追求单文件而停机或丢数据。

## 八、用户审查点

请重点审查以下口径：

1. 是否同意“每 source kind 每小时一个逻辑 partition，正常一个文件但允许保护性 `part=N`”，而不是四源强行合并到一个 parquet。
2. 是否同意 day-first 路径：`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/...`。
3. 是否同意 50GB 以 `datasets/training` 目录大小为阈值，真实删除只限 `training/typed_raw/utc_day=*` 下 finalized complete day，默认生产根先 dry-run。
4. 是否同意 PMXT sort order 先 benchmark，资源不可接受时回到用户决策。
5. 是否同意 Polymarket orderbook hourly 文件用完整 typed native fields 作为 raw-equivalent evidence，而不强制每行保存完整原始 raw_json。
6. 是否同意 Binance depth/snapshot、aggTrade、RTDS Chainlink 也从共用 `raw_json` 宽表升级为各自 source-specific typed hourly parquet。
7. 是否同意实现完成并本地验证通过后，在数据采集服务器运行一次完整 5 分钟 market 人工审查采集，并把产物拉回本地供人工审查。
