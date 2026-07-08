# data-collect-demand 共用数据采集模块需求

事项编号：`data-collect-demand`。

本文定义当前项目 `./data-collect/` 的共用数据采集模块需求。`./data-collect/` 只负责把外部市场源转化为可审计、可回放、可验证覆盖率的原始采集证据，服务训练、paper trade 和 live trade 三类消费者；本文不包含实现计划，不修改代码，也不定义 `data-pipeline` 的训练样本加工逻辑。

## 一、范围与非范围

### 1.1 data-collect 范围

`./data-collect/` 必须负责：

1. 采集 Polymarket `btc-updown-5m` market channel、Binance `BTCUSDT` orderbook/depth、Binance `BTCUSDT` aggTrade、Polymarket RTDS Chainlink `btc/usd` 四类源。
2. 保存每条原始事件的 raw evidence，包括原始 payload 或等价可还原 `raw_json`、source kind、source event type、source identifier、采集主机、采集区域和 `recv_time_utc`。
3. 为每条源事件记录本地接收时间 `recv_time_utc`，同时保留各 source 原始 timestamp；Polymarket market event 必须保留平台原始 `timestamp` 字段，两者不得互相覆盖。
4. 保存 market discovery / market metadata，包括 `new_market`、condition id、token id / asset id、CLOB market id、slug、start/end、active/acceptingOrders、`market_lifecycle_state`（candidate / prefetched / active / retired）、tick size、min size、fee / rebate / reward eligibility 相关原始字段；该状态只描述 market lifecycle，不得与 ModelRegistry 的 candidate / active / retired 混用。
5. 输出 typed raw parquet、run/segment manifest、source coverage、source health、source gap、reconnect trace、schema version、row count、sha256、ResourceWatermark 证据；source coverage/gap/reconnect 证据默认收敛进 manifest 或小型 receipt，不默认生成重复 sidecar JSON 或逐事件 debug 日志。
6. 通过 temp/running 分段文件、close/finalize、atomic rename 发布 finalized artifact，禁止消费者读取 partial artifact。
7. 记录网络韧性、资源水位、writer queue、flush latency、RSS/CPU/disk watermark、backpressure 和异常恢复证据。
8. 覆盖当前 active market，并从目标 market start 前 `T-30s` 开始采集 prefetched next market；`T-30s` 采集覆盖必须与 `T-30` 样本加工非范围明确区分。
9. 技术验证必须覆盖真实 `btc-updown-5m` 冷启动 market discovery、current active market、prefetched next market、market rollover、以及 Polymarket / Binance depth / Binance aggTrade / RTDS Chainlink 四源在同一时间线上的长时间采集；generic BTC/Bitcoin open market smoke、HTTP reachability、单条业务消息或 synthetic throughput 不能替代该验收。
10. 按训练、paper trade、live trade 三种独立运行拓扑导出独立二进制和部署/接入文档；训练二进制用于数据采集服务器持续写训练 raw mirror，paper/live 二进制用于各自交易服务器本机 hot path handoff 和外部市场源证据，不得混入交易决策、下单、撤单、NOE/ledger 或模型输入组装。

### 1.2 data-collect 非范围

`./data-collect/` 不负责：

1. `data-pipeline` 的 labels、features、tick_stream、训练切分、canonical split、purged/embargo split 或 T-30 样本加工。
2. 训练标签、execution feedback label、reward、PnL、NAV、ledger、订单生命周期归一化或策略评估。
3. paper/live 交易决策、下单、撤单、风控、模型 intent 改写或 Polymarket submit/cancel 路径。
4. 用 parquet 存在性替代 runtime append-only event log，或用 manifest 替代 label / validation / tick_stream 的消费合同。
5. 在 Polymarket Rust SDK 未覆盖能力时，自行引入 raw REST、manual signing、自建 SDK 或 shadow implementation；后续如发现 SDK 缺口，必须先形成缺口说明、影响范围和可选方案，获得用户同意后才能进入实现。
6. 发布后归档、远端上传、gzip 压缩或对象存储 listing 验证；若未来需要纳入，必须另开 archive demand/plan/changelog 并补充 archive integrity 验收。
7. paper/live append-only log、rotation artifact、live artifacts、order evidence、NormalizedOrderEvent、ledger、execution feedback ingestion 或训练反馈标签；这些由 paper/live、NOE/Ledger 或后续 data-pipeline/feedback 需求承接。

### 1.3 本地 Polymarket 网络环境约束

1. 本地电脑网络环境不能直接访问 Polymarket；后续如果需要在本地电脑测试或采集 Polymarket 数据，必须使用用户已在本地电脑环境中设置好的香港 VPN 线路。
2. 本地直连 Polymarket 失败不得被误判为 Polymarket Rust SDK 缺口、采集程序错误或 source stale；必须先记录网络线路、VPN 状态、endpoint、错误类别和重试证据。
3. 该约束只影响本地 Polymarket live-source 测试/采集路径；不改变数据采集服务器、AWS paper/live 服务器或本地 synthetic probe 的验收语义。

## 二、时间语义与延迟证据

1. 每条源事件必须保留 source timestamp 或可证明该源没有 exchange timestamp 的快照边界；每条源事件必须记录 `recv_time_utc`。
2. `recv_time_utc` 表示当前采集进程所在主机收到业务消息的 UTC 时间，不等于 Polymarket 平台时间、Binance event time、RTDS payload timestamp，也不等于爱尔兰交易服务器收到消息的时间。
3. 延迟统计必须记录 `collector_host_id`、`collector_region`、`host_clock_sync_status`、source endpoint、source timestamp、`recv_time_utc` 和计算口径版本。
4. 非爱尔兰采集主机的 `recv_time_utc` 只能用于该采集主机视角的接收延迟分析，不得冒充 AWS 爱尔兰 paper/live 交易服务器的真实接收延迟。
5. 当需要评估 paper/live 交易视角延迟时，必须使用对应 paper/live 主机采集或运行 artifact 中的接收时间；跨区域数据只能作为对照证据。
6. `data-collect` 必须保留双时间轴：`recv_time_utc` / runtime receive time 表示数据对当前进程可见的时间，source/platform timestamp 表示源平台声明的事件时间；两者都必须进入 typed raw 和 manifest 可追溯证据，不得互相替代。
7. 训练数据服务器的存储分区和训练冷路径默认使用数据采集服务器本机 `recv_time_utc` 作为 availability time；source/platform timestamp 只能作为源事实字段、排序辅助和延迟分析输入，不能让实际晚收到的事件提前进入训练样本 causal cutoff。
8. paper/live hot path 必须在对应 paper/live 服务器本机记录 runtime receive timestamp；paper/live 模型的因果可见性必须以本机 runtime receive timestamp / handoff receive time 为准，不能使用训练数据服务器的 `recv_time_utc` 冒充。
9. source-specific hourly parquet 中的 collector receive time 列统一命名为 `timestamp_received`；不得同时写入含义相同的 `recv_time_utc` 别名列。`recv_time_utc` 仍可作为 envelope、manifest、partition/coverage 和语义说明中的概念名。

## 三、数据源覆盖要求

### 3.1 Polymarket market channel

1. 必须覆盖 `book`、`price_change`、`last_trade_price`、`best_bid_ask`、`tick_size_change`、`new_market`、`market_resolved` 等 market channel 业务事件。
2. 订阅必须显式记录 token / asset id 列表、raw subscription JSON、custom feature 开关、订阅确认证据或异常返回；market channel subscription payload 必须使用官方字段 `assets_ids`、`type: "market"`，验收 `best_bid_ask`、`new_market`、`market_resolved` 时必须启用 `custom_feature_enabled: true`。订阅确认证据可以是官方定义的 ack/response，也可以是首个匹配订阅资产的业务事件；缺少独立 ack 本身不能单独判定 incomplete，但无法证明订阅已发送、custom feature 已启用、SDK 支持状态或首个业务事件/明确异常时，manifest 必须标记 source gap / incomplete。
3. `new_market` 不能被过滤；其 payload 中的 market metadata、condition id、asset id / token id、active 状态、fee schedule、tick size 等必须落盘为 raw evidence 和 typed raw parquet 字段或可追溯 raw_json。
4. market discovery 必须支持 candidate slug 与 active scan / metadata snapshot 双路径；slug 只能生成 candidate，active scan 和 metadata snapshot 必须补齐 condition id、token id / asset id、start/end、active/acceptingOrders、tick/min size、fee/reward、clock drift 证据。
5. market lifecycle 必须至少表达 `market_lifecycle_state`（candidate、prefetched、active、retired）四态；prefetched 只能表示预加载数据上下文，不能授权交易 submit path。
6. 必须记录目标 market、planned market start、preload start、subscription confirmation evidence、first business event、coverage window，并证明 current active market 与 `T-30s` prefetched next market 的覆盖边界；每次 market lifecycle / rollover 必须记录 current -> retired、prefetched -> active 的边界证据。
7. `market_resolved` 是目标覆盖项，但不得把未观测到该事件直接解释为未 resolved；必须记录订阅配置、未观测计数、fallback metadata 校验、source gap 或 stale 状态。
8. market retired 判定必须拆分为两个互斥证据路径：`market_resolved` 事件路径必须记录同一 target condition/market 的 `subscribed_asset_ids_for_market`、`resolved_event_asset_ids`、`winning_asset_id`、`winning_outcome`、`close_reason_if_present` 或 nullable close reason，并校验事件 `market` / condition 与目标 market 匹配；fallback metadata 路径只在 `market_resolved` 缺失、stale 或 source gap 时启用，必须记录 `market_resolved_missing_reason`、同一 target condition/market 的 `fallback_metadata_asset_ids`、fallback freshness、same condition/market 校验和 close/resolution 证据，且不得伪造 `resolved_event_asset_ids`。
9. retired predicate 必须满足：事件路径中 `resolved_event_asset_ids` 等于或覆盖 `subscribed_asset_ids_for_market`，`winning_asset_id` 属于该 target condition/market 的 subscribed/resolved asset set；fallback metadata 路径中 `fallback_metadata_asset_ids` 等于或覆盖 `subscribed_asset_ids_for_market`，metadata 必须与同一 condition/market 匹配、fresh，并包含 `closed`、`active`、`acceptingOrders`、`closedTime`、`resolution` 或等价关闭证据；任一路径证据不满足时只能记录 stale/gap，不能进入 retired。
10. Polymarket 官方文档和社区 issue 已显示 ping/pong 正常不等于业务数据新鲜；market channel freshness 必须以业务消息、snapshot 到达、source gap 和 stale 检测为准。
11. Polymarket 相关采集路径必须先核对官方 Polymarket Rust SDK 能力；若 SDK 不覆盖 market channel、RTDS、market discovery 或相关 metadata 能力，必须先记录缺口、影响范围和可选方案，经用户同意后才能实现缺口方案。

### 3.2 Binance BTCUSDT orderbook/depth 与 aggTrade

1. 必须采集 Binance 官方 `BTCUSDT` orderbook/depth 和 `BTCUSDT` aggTrade 源，并记录 WebSocket stream name、event type、source sequence / trade id、source event time 和 `recv_time_utc`。
2. live Binance 源只能来自 Binance 官方 WebSocket stream，例如 `btcusdt@depth`、`btcusdt@aggTrade`；depth snapshot 必须来自 Binance 官方 REST `/api/v3/depth` 或当前官方等价端点。RTDS、archive、relay、proxy、cached feed 或历史 backfill 只能作为旁证或补数，不能冒充 live official source。
3. 本地 orderbook 最小重建状态只用于 source integrity / gap detection；不得生成 labels、features、tick_stream 或模型输入。
4. depth update 必须保留 WS raw/typed 字段 `e`、`E`、`s`、`U`、`u`、`b`、`a`；REST snapshot parquet 必须保留 `timestamp_received`、`lastUpdateId`、`bids`、`asks`、`snapshot_fetch_start_utc`，不得再写与 `timestamp_received` 等价的 `snapshot_recv_time_utc` 别名列，不得伪造 exchange timestamp；若使用 combined stream，必须保留 wrapper `stream` / `data`。
5. 本地 orderbook source integrity 检测必须能记录 snapshot 获取、buffered updates、缺序、重建开始、重建成功和重建失败证据。
6. depth stitch predicate 必须验证：丢弃 `u <= lastUpdateId` 的 buffered update，首个可用 update 必须满足 `U <= lastUpdateId + 1 <= u`；后续 update 必须先处理 `u <= local_u` 的 stale/duplicate update 并记录/忽略，再处理 `U > local_u + 1` 的 source gap 并重新 snapshot，其余 update 才能应用并推进 `local_u`。
7. aggTrade raw evidence/parser 必须能校验原始字段 `e`、`E`、`s`、`a`、`p`、`q`、`f`、`l`、`T`、`m`；hourly parquet 只输出 `timestamp_received`、`event_time`、`symbol`、`aggregate_trade_id`、`price`、`quantity`、`first_trade_id`、`last_trade_id`、`buyer_is_maker`，不得再额外输出与 `event_time` 近似重复的 `trade_time` 列。
8. 当 update id 不连续、snapshot 缺失、服务器 shutdown、rate limit、订阅错误、aggTrade 长时间无业务消息或 source stale 时，必须写入 source health、source gap 和 reconnect trace。

### 3.3 Polymarket RTDS Chainlink BTC/USD

1. 必须采集 Polymarket RTDS Chainlink `btc/usd` 更新，hourly parquet 保留 `timestamp_received`、RTDS message timestamp、payload timestamp、symbol、value、source topic 和 subscription filter，不再额外写 `recv_time_utc` 别名列。
2. 初始订阅必须记录 raw subscription JSON、subscribe send time、ack/first message 或 close/error 证据；Chainlink BTC/USD subscription payload 必须能追溯 `action: "subscribe"`、`topic: "crypto_prices_chainlink"`、`type: "*"` 和 `filters: "{\"symbol\":\"btc/usd\"}"`。
3. RTDS Chainlink BTC/USD 是 Chainlink 价格证据，不是 Binance 官方 orderbook/depth 的替代源。
4. RTDS 也必须使用业务数据 freshness 检测；ping/pong 或连接 open 不能单独证明 source coverage 完整。
5. RTDS Chainlink BTC/USD 保持 `RTDS_FRESHNESS_TIMEOUT_SECS=60` 作为 `rtds_business_message_timeout` source gap 判定线；业务消息静默期间必须每 `10s` 主动重发同一个 Chainlink `btc/usd` subscription，在一个 `60s` source gap 窗口内最多重发 `6` 次；每累计 `2` 次重发后仍无业务消息，必须主动重建 RTDS SDK client/connection 并重新订阅；`10s` 重发和 `2` 次后的主动重连本身不记录 `rtds_business_message_timeout`，只有累计静默到 `60s` 仍无业务消息时才记录 `rtds_business_message_timeout`。

## 四、存储、分区与发布

1. 数据保存格式必须是 `.parquet` typed raw mirror；每条 JSON-like 源必须保留 parseable `raw_json` 或等价原始证据引用。
2. 生产数据根路径由用户决定为各运行服务器本机 `~/btc-updown-marketmaker/datasets/`；数据服务器上的训练用原始采集数据必须在该根路径下按 UTC day 分区保存，其中 `~` 按当前服务器用户解析，不得写成本地开发机路径。
3. paper/live 交易服务器如需接收 `data-collect` handoff mirror、coverage 或 manifest，也必须放在对应交易服务器本机 `~/btc-updown-marketmaker/datasets/` 下；该根路径只承载外部市场源证据，不替代 paper/live 自身 append-only event log、rotation artifact、order evidence、NOE/ledger 或 compact mirror。
4. `data-collect` 只提供外部市场源 cold mirror（finalized parquet / coverage / health / gap / manifest）和 hot path handoff 边界证据；paper/live 自身 append-only event log、rotation artifact、order evidence、NOE/ledger、Market Context Builder 输出和 compact mirror 由对应模块需求定义，本文只声明接入边界。
5. 训练 cold mirror 按 `source_kind + recv_time_utc UTC day` 分区；每个 `source_kind/utc_day` 下允许生成多个 finalized parquet segment，不承诺一天固定四个 `.parquet` 文件，也不要求四源写入同一个 parquet 文件。
6. 每个 segment 的 manifest 必须记录 `recv_time_utc` 覆盖窗口、source/platform timestamp 范围、market slug/window、source kind、segment seq、rollover reason、row count、sha256 和 complete/incomplete verdict。
7. `max_rows`、`max_bytes`、`max_segment_duration`、UTC day boundary 和 shutdown 只能作为 segment soft rollover / finalize 条件；行情暴增时应生成更多 segment，并记录 burst、queue watermark、flush latency 和 backpressure observation，不得因触发 soft rollover 自动停机。
8. Parquet 连续写入必须写入 `*.tmp`、`*-running.parquet` 或等价 running segment；close/finalize 后才能通过 atomic rename 发布 finalized `.parquet`。
9. 每个 source parquet 必须写入 typed raw schema version / source kind metadata，至少包括 `parquet_raw_layout_version` 和 `parquet_raw_source_kind`；兼容当前 V2 typed raw mirror 是迁移基线，但本文不把 V2 data-pipeline schema 变成 `data-collect` 的加工职责。
10. manifest 必须记录 artifact path、row count、sha256、schema version、code version / git commit、collector binary sha256、source config digest、validation verdict、source coverage、source gap、health/gap/reconnect 摘要或 compact receipt digest、start/end time、coverage window、collector host/region、ResourceWatermark、writer queue 高水位和 flush latency；非空 `source_gaps` 或 `manifest_source_gaps` 不得被判为 complete/PASS；不得为了重复这些字段默认写 `source_coverage.json`、`source_gaps.json`、`rollover_edges.json`、`write_receipts.jsonl` 或 `queue_samples.jsonl`。
11. manifest 必须包含 `required_files[]` 或等价文件角色清单；每项至少记录 file role、path、finalized status、row count、sha256/digest、schema version、missing reason，总 manifest 必须给出 complete/incomplete verdict。
12. parquet / manifest 文件发现路径的消费者只能读取 finalized parquet 和对应 manifest；temp/running、`.rsync-partial`、外部同步/发布链路生成的未完成标记文件、未 finalize 文件不得进入 discovery，`data-collect` 不负责 upload/listing。
13. atomic rename 失败、sha256 缺失、row count 缺失、schema version 缺失、source coverage 缺失或 manifest 不一致时，artifact 必须标记为 incomplete，不得假装完整。
14. 当前需求只要求 finalized artifact 不被 partial、running 或 `.rsync-partial` 误发现。
15. 训练 cold path 必须迁移为 UTC day 文件夹 + 每 source kind 每小时一个逻辑 partition：`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH[--part=N].parquet`。正常目标是一小时一个 finalized parquet；`max_rows` / `max_bytes` 只允许作为保护性 soft rollover 生成同一小时的 `part=N`，不得停机或丢数据。
16. Polymarket `book` 事件必须保存完整 `bids` / `asks` price-size depth，能还原 token orderbook；不得只保存 `bids_len` / `asks_len` 却标记为 complete book。PMXT-like schema 只作为 Polymarket orderbook typed layout、hourly parquet、compression/sort 和 manifest 参考，不引入 PMXT archive source。
17. Polymarket hourly typed layout 必须覆盖 `book`、`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` 的 typed 字段或 nullable unavailable reason，并保留 collector host/region、slug、condition id、asset id、coverage 和 source evidence。
18. Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 必须进入 hourly typed layout 修改范围，分别使用官方字段的 source-specific schema：`binance_depth_snapshot` 保留 `timestamp_received`、`lastUpdateId` 和完整 `bids/asks`，`binance_depth_update` 保留 `timestamp_received`、`e/E/s/U/u/b/a` 与 stitch evidence，`binance_aggtrade` 保留 `timestamp_received`、`event_time`、`e/s/a/p/q/f/l/m` 的 typed 字段并通过 raw evidence/parser 校验官方 `T`，`polymarket_rtds_chainlink_btc_usd` 保留 `timestamp_received`、message timestamp、payload timestamp、symbol、value、topic 和 subscription filter。
19. 训练 dataset retention threshold 为 `50_000_000_000` bytes，作用域是 `datasets/training`。默认生产策略只写 run-level dry-run/no-op retention policy receipt；真实删除只允许在显式启用 delete mode 或用户单独批准后删除 `training/typed_raw/utc_day=*` 下最老 finalized complete UTC day。
20. finalized complete UTC day 的删除判定必须 fail closed：current day、running/digesting/tmp/`.rsync-partial`、incomplete day、manual-inspection、run manifests、startup receipts、未知文件角色和非 `training/typed_raw/utc_day=*` 路径均不得删除；超过阈值但无可删 complete day 时必须写 ResourceWatermark hard-threshold evidence 并受控停止新采集。

## 五、网络异常与恢复要求

### 5.1 必须显式记录的异常

以下情况不得静默丢失，不得把数据写成完整覆盖：

1. 连接失败。
2. 断链或异常 close。
3. server shutdown。
4. 订阅返回错误、HTTP/WS 返回错误或协议错误。
5. 返回空 payload、空数组或空 snapshot。
6. 长时间无业务消息。
7. ping/pong 正常但业务数据静默。
8. rate limit、连接数限制、订阅数限制或服务端 throttling。
9. snapshot 缺失、snapshot 过旧、update id 缺口。
10. source stale、source gap、source coverage drop。
11. writer queue 堵塞、backpressure 触发、磁盘/内存/CPU 水位越线。

每个异常必须形成 source health、source gap 或 reconnect trace 记录，且能进入 manifest 或 compact health receipt；不得默认生成无限增长或与 manifest 重复的逐事件 debug artifact。

### 5.2 恢复策略需求

1. 网络恢复必须使用有限重试，不允许无限 tight loop。
2. 重试必须使用指数 backoff + jitter，并有可配置且带默认值的 max attempts、max elapsed、backoff ceiling 和 retry exhausted 状态。
3. 必须记录每次 attempt、sleep、错误类别和最终状态；源级状态至少覆盖 open、half-open、recovered、exhausted。
4. 各 source 独立熔断和恢复；Polymarket、Binance、RTDS Chainlink 任一源故障不得被其他源健康状态掩盖。
5. 重连后必须重新校验订阅、snapshot、新鲜业务消息和 source coverage；不能只因连接 open 或 PONG 正常就恢复为 healthy。
6. Polymarket 静默连接必须使用业务数据 freshness 检测；ping/pong 只作为连接层证据，不能作为 market data 完整性证据。
7. Binance orderbook/depth 重连后必须重新获取 snapshot 并验证 update id 衔接；后续 depth update 必须记录 stale/duplicate 忽略和 gap/resnapshot 证据；Binance aggTrade 重连后必须验证 stream name、event time、trade id 连续性可解释性和 source stale 状态；缺口必须记录为 source gap。
8. RTDS Chainlink 重连后必须重新验证 topic、symbol filter、payload timestamp、业务消息 freshness 和 source stale 状态。

## 六、Rust async/non-blocking 运行要求

1. 实现必须使用 Rust，并基于 Tokio 或等价 async runtime。
2. WebSocket、HTTP、timer、channel、manifest 写入、health 写入和 parquet writer 调度不得阻塞主事件循环；需求层验收词为 async 和 non-blocking。
3. 文件 I/O、parquet close/finalize、data-collect 内部 parquet compression、local directory scan、sha256、large manifest aggregation、本地旧 segment 清理等可能阻塞或重 CPU 工作必须隔离到后台任务或 blocking pool；不包含 gzip 归档压缩、远端上传、对象存储 listing 或归档保留清理。
4. 所有跨任务队列必须是 bounded queue，并同时受 row count、byte count、oldest event age、writer lag 和 memory watermark 约束。
5. 当 bounded queue 达到 soft watermark 或短暂水位尖峰时，系统必须记录 backpressure、source health 降级和恢复观察；只有 send timeout、drop、不可恢复 source 缺失，或 hard threshold 连续越线且无法恢复时，才记录 source gap 或执行受控停采；禁止 unbounded queue 导致 OOM 或 silent loss。
6. Parquet writer 必须记录 row group / buffer 的内存估算、flush latency、bytes written 和 finalize 结果；未 finalize 不得发布。
7. HTTP client 必须有 connect/read/total timeout 或等价超时语义；默认无超时不能作为生产需求。

## 七、性能与资源验收基线

1. Polymarket `btc-updown-5m` 事件量验收以现有估算为基线：单 5 分钟市场 p95 约 275k events，max 约 394k events；加 T-30 秒预加载后 p95 约 302k events，max 约 433k events。
2. 工程吞吐基线必须按四源合并 `2k-5k events/s` 解析、排队、写入和 health 统计能力设计，并在 manifest 中分别记录 Polymarket、Binance depth、Binance aggTrade、RTDS Chainlink 与四源合并的实际吞吐、p95/max burst、drop/gap 情况。
3. writer queue 必须记录 row/byte 高水位、queue age、backpressure 次数、flush latency p50/p95/max、finalize latency 和失败次数。
4. ResourceWatermark 必须记录当前运行环境实际测得的 RSS、CPU、disk free、artifact dir bytes、journal/log bytes、journal 限额状态、open files、queue bytes、queue age、parquet writer memory estimate、cgroup `memory.current` / `memory.stat` / `memory.events`、PSI、anon/file/slab 和 stage latency / TTL；无法测得的指标不得生成大批 `not measured` 占位字段，只允许在影响验收判断时用 compact unavailable summary 说明缺口。
5. 数据采集服务器和交易服务器必须分别记录 hardware tier；live 交易小主机不得承担训练式重处理、历史全量 parquet 扫描、压缩归档批处理或 Rust 编译。
6. AWS 爱尔兰 live-trade 服务器是运行目标，不是构建机；`data-collect` 相关 Rust 编译、cargo test/check/fmt 和重处理必须在具备硬件条件的构建/数据服务器完成。
7. ResourceWatermark 超过硬阈值时，必须记录降级、停止新市场采集、保护已采数据、优先 atomic publish 和 sha256 manifest 的保全动作。
8. 后续计划或实现必须给出 threshold policy receipt，至少覆盖 RSS、CPU、disk free、artifact bytes、journal bytes/limit、open files、queue bytes/age、cgroup memory events 和 PSI window 的阈值、单位、采样窗口、触发动作与结果。

## 八、消费者边界

1. 训练消费者走 cold path：读取按天分区的 finalized typed raw parquet、manifest、source coverage，并由后续 `data-pipeline` 生成 labels/features/tick_stream/replay context/训练切分；`data-collect` 不直接生成这些训练消费产物。
2. paper/live 消费者走 hot path：paper/live 的 Market Context Builder / runtime 必须通过各自服务器本机运行的 `data-collect` paper/live 二进制提供的实时 handoff、内存 bounded channel、IPC、local stream 或等价低延迟事件流消费外部市场源事件和 source health/gap 状态；不得把等待 finalized parquet、manifest 或离线 discovery 作为 paper/live 热路径唯一事实来源。
3. `data-collect` 的 hot path 职责只到外部市场源事件、timestamp namespace、source coverage、source health/gap、reconnect/backpressure 状态和 handoff 边界证据；paper/live append-only event log、rotation artifact、live artifacts、order evidence、NOE/ledger、Market Context Builder 输出、模型输入组装和 execution feedback ingestion 不由 `data-collect` 生成或解释。
4. paper/live 延迟验收必须区分 `collector_recv_time_utc(host,region)`、handoff send time、paper/live runtime receive time、decision time 和 causal cutoff；非爱尔兰 collector 的 `recv_time_utc` 不能冒充 paper/live 爱尔兰主机实时接收延迟。
5. data-pipeline 可以消费 `data-collect` 的 typed raw parquet 和 manifest，但必须自行生成 labels、validation、tick_stream、features 和训练切分证据。
6. 采集成功不等于训练批准，finalized parquet 完整不等于 labels/features/tick_stream 已完成，source coverage 完整不等于 live/paper 交易逻辑健康，hot path handoff 健康也不等于 paper/live 交易决策、NOE/ledger 或订单执行健康。
7. 训练、paper、live 三类运行形态必须导出独立二进制：训练二进制只服务数据采集服务器训练 raw mirror；paper 二进制只服务 paper 服务器热路径外部市场源 handoff；live 二进制只服务 live 服务器热路径外部市场源 handoff。三者共享 schema/manifest/source adapter 合同，但不得共享运行根、钱包、交易状态或消费者 artifact 语义。

## 九、验收标准

`data-collect-demand` 后续实现或计划至少必须能验证：

1. `./data-collect/` 的职责边界不得超出外部市场源采集证据边界；允许范围包括 raw evidence、`recv_time_utc`、typed raw parquet、source coverage、source health、source gap、reconnect trace、manifest、atomic rename、paper/live 消费者接入边界声明、网络韧性和 ResourceWatermark。
2. 每条源事件都保留 `recv_time_utc`；Polymarket 每条 market event 同时保留平台 `timestamp` 和本地 `recv_time_utc`，并记录采集主机/区域。
3. 非爱尔兰采集主机延迟不会被报告成爱尔兰 paper/live 交易主机接收延迟。
4. 启动、重启和每次 market lifecycle / rollover 必须覆盖当前 active market；next market 必须从 planned market start 前 `T-30s` 开始采集，并记录 target market、planned market start、preload start、subscription confirmation evidence、first business event、coverage window、current -> retired、prefetched -> active 边界证据。
5. `new_market`、market metadata、condition id、token id / asset id、start/end、`market_lifecycle_state`（candidate / prefetched / active / retired）全部可从 artifact 追溯；该状态不得与 ModelRegistry 状态混用。
6. market discovery 必须证明 candidate slug + active scan / metadata snapshot 双路径，slug-only 不能作为完整 discovery 成功。
7. Polymarket、Binance BTCUSDT orderbook/depth、Binance BTCUSDT aggTrade、RTDS Chainlink BTC/USD 四源均有 source coverage、source health、source gap 和 reconnect trace；验收必须逐源检查 required event types、required ids/timestamps、raw_json/typed fields 覆盖。
8. `market_resolved` / retired 验收必须证明事件路径或 fallback metadata 路径之一满足 retired predicate；事件路径必须记录同一 target condition/market 的 `subscribed_asset_ids_for_market`、`resolved_event_asset_ids`、`winning_asset_id`、`winning_outcome`、nullable `close_reason_if_present`，fallback metadata 路径必须记录 `market_resolved_missing_reason`、同一 target condition/market 的 `fallback_metadata_asset_ids`、freshness、same condition/market 校验和 close/resolution 证据。
9. `best_bid_ask`、`new_market`、`market_resolved` 验收必须证明 `custom_feature_enabled: true`、订阅确认证据（官方 ack/response 或首个匹配订阅资产的业务事件）和 SDK 支持状态；未满足时 manifest 必须为 source gap / incomplete。
10. Binance live 源必须来自 Binance 官方 WS/REST；aggTrade 必须保留原始 `e,E,s,a,p,q,f,l,T,m`，depth update 必须保留 `e,E,s,U,u,b,a`，snapshot 必须保留 `lastUpdateId,bids,asks` 且不得伪造 exchange timestamp，并且 depth stitch predicate 必须可审计。
11. 连续 parquet 写入通过 running/temp segment 承载，close/finalize 后 atomic rename；parquet / manifest 文件发现路径只读 finalized parquet；每个 source parquet 必须带 typed raw schema version 和 source kind metadata，至少包括 `parquet_raw_layout_version` 和 `parquet_raw_source_kind`。
12. manifest 至少包含 artifact path、row count、sha256、schema version、code version / git commit、collector binary sha256、source config digest、validation verdict、source coverage、source gap、health/gap/reconnect 摘要或 compact receipt digest、start/end time、coverage window、ResourceWatermark、writer queue 高水位、flush latency、采集主机/区域、`required_files[]` 和 complete/incomplete verdict；默认不得生成与 manifest 重复的 source coverage/gap/rollover sidecar JSON。
13. paper/live 自身 append-only event log、rotation artifact、live artifacts、order evidence、NOE/ledger 和 execution feedback ingestion 必须由其他模块需求承接；`data-collect` 只能提供外部市场源 finalized mirror、coverage、health、gap、manifest 和 hot path handoff 边界证据，不能私自解释交易运行证据。
14. 训练 cold path 与 paper/live hot path 的接入边界必须可验证：训练消费 finalized typed raw parquet + manifest + `data-pipeline` 输出；paper/live 消费 realtime handoff 或等价低延迟事件流，并在对应 paper/live 主机记录 runtime receive timestamp；Phase 0 或后续验收不得用 finalized parquet 可见时间替代 paper/live 实时接收延迟。
15. 网络异常、返回空、长时间业务静默、ping/pong 正常但无数据、rate limit、server shutdown、snapshot 缺失和 source stale 都会写入 health/gap/reconnect 证据。
16. 恢复策略使用有限重试、backoff、jitter、max attempts、max elapsed、backoff ceiling，按源独立熔断/恢复，并在重连后验证订阅、snapshot、新鲜业务消息、source coverage、RTDS topic/symbol/payload timestamp 和 Binance update/trade id 状态；RTDS Chainlink BTC/USD 必须验收每 `10s` 循环主动重发同一 subscription、每 `2` 次重发后主动重建 RTDS SDK client/connection、且 `60s` 才判 `rtds_business_message_timeout`。
17. reconnect trace 必须记录每次 attempt、sleep、错误类别、最终状态，以及 open、half-open、recovered、exhausted 源级状态。
18. Rust async/non-blocking 边界成立：主事件循环不做阻塞 I/O、重 CPU、data-collect 内部 parquet compression、local directory scan、sha256 或本地旧 segment 清理；所有队列都是 bounded queue，并具备 backpressure。archive/upload/listing/gzip 不属于当前 `data-collect` 验收项。
19. 性能验收覆盖 Polymarket p95/max 事件量、四源合并 `2k-5k events/s` 工程吞吐、逐源吞吐/burst/drop/gap、writer queue、flush latency、RSS/CPU/disk watermark、cgroup/PSI/journal ResourceWatermark、hot path handoff latency、durable parquet write latency 和 hardware tier。
20. 阶段 0 技术验收必须真实验证 `btc-updown-5m` 冷启动发现、跨至少一个 market rollover 的 current -> retired / prefetched -> active 切换、四源真实数据流、同一 UTC window 的 coverage 对齐、逐源吞吐、ResourceWatermark、训练 cold path / paper-live hot path 边界、`source_recv_to_hot_delivery_ms` 和 `source_recv_to_write_done_ms`；只通过 synthetic throughput、generic orderbook smoke、HTTP endpoint smoke、RTDS 单消息 smoke 或 finalized parquet 证据时，不得宣称阶段 0 技术路径通过。
21. ResourceWatermark 硬阈值验收必须包含 threshold policy receipt，并证明触发条件、降级或停止新市场采集、保护已采数据、优先 finalize/sha256/manifest 的动作与结果。
22. archive/upload/listing/gzip 不属于当前 `data-collect` 验收项；若未来改变范围，以 1.2 非范围的决策门为准。
23. Polymarket 相关采集路径必须先有官方 Rust SDK 能力核对记录；SDK 缺口方案必须先获用户同意。
24. 文档、计划或实现不得把 data-pipeline 的 labels、features、tick_stream、训练切分、T-30 样本加工、paper/live 交易逻辑或 SDK 旁路实现写进 `data-collect` 范围。
25. 训练、paper、live 三类运行形态均能独立导出 release 二进制、sha256 和运行说明；训练数据采集必须有独立部署 README，paper/live hot path 必须有独立接入技术文档。
26. 训练 cold mirror 按 `source_kind + recv_time_utc UTC day` 分区，每个 source/day 可包含多个 finalized segment；manifest 能证明 segment 的 recv window、source timestamp range、market/window、soft rollover reason、row count 和 sha256。
27. `max_rows`、`max_bytes`、`max_segment_duration`、UTC day boundary 和 shutdown finalize 只触发 soft rollover 或保全发布；正常行情放大不得因 soft rollover 自动停机，只有不可写、无法保全、持续 hard threshold、drop/send timeout 或 partial artifact 被发布等条件才能进入 fatal 动作。
28. 训练样本因果可见性必须以采集服务器 `recv_time_utc` / availability time 为默认主轴；paper/live 因果可见性必须以对应 paper/live 服务器本机 runtime receive timestamp 为主轴；source/platform timestamp 不能替代 receive/available time。
29. paper/live 二进制的 hot path 接入文档必须明确 IPC/local stream/bounded handoff 协议、event seq、timestamp namespace、backpressure/status 消息、runtime receive timestamp 记录点和非范围边界，且不得生成交易决策、NOE/ledger、order evidence 或 execution feedback。
30. PMXT-inspired Polymarket orderbook hourly parquet 必须证明 `book` rows 全部保留可解析 `bids` / `asks` 数组字段；真实市场中单侧空数组可以是盘口状态，必须在人工检查报告中单独计数，不得把完整 price-size 数组缩水成 `bids_len` / `asks_len` 后伪装完整。`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` 必须有对应 typed 字段、raw-equivalent evidence 或 nullable unavailable reason。
31. Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink hourly parquet 必须分别证明 required typed fields 存在，且不能只依赖 opaque `source_kind/raw_json` 宽表；Binance depth 还必须证明 snapshot/update stitch predicate、`local_u` 单调推进或 gap/resnapshot 可解释。
32. 训练 cold path finalized discovery 必须使用 day-first `utc_day=YYYY-MM-DD/source_kind=<source_kind>` 路径；legacy source-first 路径只能作为历史只读兼容被显式标记，不能与新产物混写。
33. 50GB retention 验收必须证明 dry-run/no-op receipt、可选 deletion receipt、最老 finalized complete UTC day 删除边界、受保护目录不删除，以及无可删 complete day 时 fail closed。

## 十、证据来源

本需求依据以下本地证据与外部资料整理：

1. `demand/rebuild-core-demand.md`
2. `mistake-log/poly-tx/数据采集处理消费问题总报告.md`
3. `mistake-log/poly-tx/证据来源台账.tsv`
4. `mistake-log/2026-07-03-重构前错误汇总与优先级.md`
5. `5分钟持续加载市场数据的硬件消耗预估.md`
6. Polymarket WebSocket docs：`https://docs.polymarket.com/market-data/websocket/overview`、`https://docs.polymarket.com/market-data/websocket/market-channel`、`https://docs.polymarket.com/market-data/websocket/rtds`，核对日期：2026-07-06。
7. Binance WebSocket Streams docs：`https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams`，核对日期：2026-07-06。
8. Polymarket GitHub issues：`https://github.com/Polymarket/py-clob-client/issues/292`、`https://github.com/Polymarket/real-time-data-client/issues/26`，核对日期：2026-07-06。
9. Rust docs：`https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html`、`https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.channel.html`、Apache Arrow Rust parquet writer close/finalize docs，核对日期：2026-07-06。
