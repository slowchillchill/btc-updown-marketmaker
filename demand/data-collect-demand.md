# data-collect-demand 共用数据采集模块需求

事项编号：`data-collect-demand`。

> 2026-07-18 authority同步：训练collector的外部availability、业务静默、连接生命周期、daily manifest coverage、SDK patch及Binance depth因果顺序边界，现以`demand/data-collect-source-availability-lifecycle-repair-demand.md`为准。普通业务静默、stale、空候选以及外部DNS/TCP/TLS/WS/HTTP故障不再自动构成confirmed gap、异常告警或进程退出；aggTrade ID前跳不再触发REST backfill；depth继续保留官方snapshot/update-id stitch，阶段6已把两个既有artifact升级为snapshot v3/update v4以区分raw ingress与collector admission。可靠operator stop/run deadline关闭共同intake后，尚未取得对应raw-day permit与queue ownership的depth generation-local pending或跨日deferred staging可以清空；正常snapshot unavailable、Close/EOF、reconnect、UTC rollover仍中性release，已取得permit并进入queue的evidence仍drain。跨日staging只在正常rollover flush时分配admission，受控清空不生成row/gap/告警/`dropped_rows`。专项阶段7已完成三个批准的完整collector场景、CLOB/Gamma各100-cycle、受控cutoff与跨日pre-admission staging验证并通过三路独立终审；阶段8及后续实施状态、验证证据和完成结论以专项需求、计划与changelog的最新记录为准。本文件未被该专项需求取代的source字段、typed raw、完整性、SDK-first与模块边界继续有效。专项计划不授权生产构建、部署、远端服务操作或数据清理。

> 2026-07-22 Polymarket resolution-followup authority同步：`demand/data-collect-polymarket-resolution-followup-demand.md`已获用户批准为本事项的窄语义authority。它只取代本文中纯时间`current -> retired`、Gamma fallback要求`active=false`、expired-slug无例外以及普通transport证据绝不进入metadata typed row的冲突边界；冻结`resolution_pending`、E+3..E+8六个slot、metadata v3、terminal-once、restart/fail-closed与无future-skew上界合同。本事项AC-12真实source验收固定在本地原生Ubuntu环境通过已配置香港VPN线路执行，不使用AWS爱尔兰数据采集服务器；该测试拓扑不改变production部署拓扑。用户已后续授权完整执行专项阶段1至阶段4、每阶段独立审查后的commit/push与本地AC-12；production部署、服务操作和数据删除仍未授权。
>
> 2026-07-25 Polymarket source-v3/metadata-v4 authority同步：用户批准`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`。metadata v3 schema/version立即冻结；current source v2 + metadata v3在SDK070总计划阶段3原子activation门通过前保持不变。activation后，同一五row-kind/attempt窄例外由metadata v4继承，source-v3 WS claimant recovery只使用v4末尾追加的精确nullable claimant pair，source v2与metadata v3转为只读。禁止source v3 + metadata v3、source v2 + metadata v4、dual-write或半切换。用户随后授予本事项持续执行/发布授权，已覆盖Stage3本地Rust/schema/test、审查、commit和push，但不把尚未完成的实现写成已完成，也不授权production cutover、release、部署、远端操作、migration或删除。
>
> 2026-07-28 Polymarket metadata-v5 current-schema 窄 supersede：用户批准
> `demand/data-collect-redundant-typed-column-removal-demand.md`。source v3
> 不变，future current metadata new-write 升为
> `polymarket_market_metadata_v5`，唯一变化是删除与
> `source_evidence_raw_sha256` 在共同非空行恒等的
> `resolution_evidence_raw_sha256`；本文后续把 metadata v4 写作 current/future
> current 的段落仅保留为当时合同与实施事实。metadata v3/v4 文件保持原 schema
> 只读，不迁移、不重写；claimant pair、五种 resolution attempt row、source
> payload/hash 校验及其他字段语义不变。
>
> 2026-07-27 RTDS窄authority supersede：用户批准`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`的共同RTDS恢复合同及阶段2方案A。当前代码只有training的`polymarket_rtds_chainlink_btc_usd`拥有真实adapter；阶段1已在该training adapter完成实现、收窄验证、三路独立终审及分离commit/push：source task曾收到合格fresh后，steady静默`300s`可完整替换runtime；后续runtime/generation以`Connected`与latest desired本地applied的后到monotonic观测为anchor等待`60s`，仍无fresh则在下一次完整替换前使用`1s × 2 ±20%`、单次最长`300s`、fresh重置且总尝试不限的outer backoff。首次fresh前禁用该business watchdog；严格唯一owner/socket、break-before-make。Attempt1因精确network-error服务对象被归为terminal parser error而固定为`FAIL + INCOMPLETE_8H`；方案A只把该数据外对象映射到SDK既有自然重连。Attempt2由用户终止；Attempt3因Gamma格式误判、Attempt4因Polymarket market Decimal空串parser fatal分别固定为`FAIL + INCOMPLETE_8H`。Attempt4没有保存失败raw frame/variant/path，不能归因具体字段。SDK070三件套单点定义的`P-B1E`已由authority/Rust commits `15feb12`/`9d8e59b`分离闭合：仅九个官方nullable Decimal把精确`""`解码为`None`，required、非空malformed和其他parser/typed/schema错误继续terminal；定向验证与三路独立终审均通过，它不是transport重连或availability例外。承载本closure的纯文档commit成功push并完成三方ref核对后，专项从该新clean/pushed HEAD执行完整六源8小时Attempt5。future paper/live真实RTDS adapter由独立事项实施、测试和部署，但必须继承同一合同，不再独立冻结另一套语义；当前paper/live仍只有heartbeat/status。除P-B1E这个typed decode规范化外，CLOB、Gamma、Binance及其他source行为不变。
>
> 2026-07-28 Attempt5 与当前游标 supersede：Attempt5 已按用户指令向精确
> 进程组发送 SIGINT 后停止，固定为
> `USER_TERMINATED_FOR_APPROVED_SCHEMA_CHANGE + INCOMPLETE_8H`，不是正常完成或
> 新 schema 验收。当前实现 authority 为
> `demand/data-collect-redundant-typed-column-removal-demand.md`；依次闭合
> aggTrade v7 与 metadata v5 后，唯一长跑游标是从新 clean/pushed HEAD 和空
> dataset root 执行完整六源 8 小时 Attempt6。

本文定义当前项目 `./data-collect/` 的共用数据采集模块需求。`./data-collect/` 只负责把外部市场源转化为可审计、可回放、可验证覆盖率的原始采集证据，服务训练、paper trade 和 live trade 三类消费者；本文不包含实现计划，不修改代码，也不定义 `data-pipeline` 的训练样本加工逻辑。

## DC-INCIDENT-20260709-AGGTRADE-T08 增量需求

1. Binance aggTrade 收到 raw 或 combined `serverShutdown` 时，不得把控制事件送入业务 writer，必须立即建立新连接并继续采集。
2. Binance aggTrade WebSocket收到`Close`、EOF、协议/网络错误、serverShutdown，或连续30秒完全没有任何合法入站frame时，必须在同一PID的source-local owner内使用有界单次等待/capped backoff持续重连；持续收到PING/PONG但无aggTrade属于业务静默，不触发重连、lag或gap。
3. Recoverable external availability不得结束source task；parser/schema、SDK/local lag、queue/writer/manifest或内部不变量等真正terminal task仍必须被主进程发现并fail closed。systemd新PID不得作为单source正常恢复机制。
4. writer channel 已断开，或 writer parser/finalize 返回错误时，不得只记录 gap 后返回成功，错误必须传播到主循环并使进程失败退出。
5. 修复不得伪造或补写事故文件；`binance_aggtrade_2026-07-09T08-running.parquet` 仍按不可读 partial artifact 处理，历史缺口另行标记或回补。

验收标准：故障注入测试必须证明raw/combined `serverShutdown`、`Close`、EOF和全frame inactivity后会同PID重连，而持续PING但无业务消息跨越旧阈值仍保持同一socket并消费后续合法aggTrade；控制事件写入业务队列为`0`，真正terminal source、writer channel断开和writer parser/finalize失败均返回错误。生产部署验收已移出本专项，须另行批准。

## DC-INCREMENT-20260710-P2B-T-V3-ARCHIVE 增量需求

1. Binance aggTrade hourly typed parquet 必须同时保存官方 `E` 为 `event_time` 和官方 `T` 为 `trade_time`；两列不得合并或相互代替，schema version 升为 `binance_aggtrade_v3`。
2. `binance_aggtrade_v3`继续以新finalized segment中`event_time`和`trade_time`的真实可读值验收，不得修改或伪装旧v2产物；本增量当时的部署/重启事实只由对应旧changelog记录。2026-07-18首次正式冷启动已由`demand/data-collect-training-collector-direct-deployment-demand.md`独立批准并取代旧M8 UTC边界编排；未来same-day不同identity升级/回滚仍须另行批准。
3. future paper/live hot path 的真实 external market event handoff 必须显式传输同一 slug 下可空 decimal-string `price_to_beat` 和 `final_price`。缺少任一字段不得阻断 handoff，也不得增加 closed/resolved/winner 门；当前 paper/live 二进制仅实现 heartbeat/status，该字段合同本轮只写入需求与计划，不冒充已实现。
4. 经用户批准的日级运维辅助脚本只在 D+1 UTC 02:00 处理昨天 D：每个目标 slug 发起一个逻辑 Gamma 读取，仅以 `eventMetadata.priceToBeat` 和 `eventMetadata.finalPrice` 同时存在作为该 market 的回填成功条件。TLS/timeout/connection reset/`URLError` 只在本次任务内有界重试，最多 3 attempts 且单次等待不超过 5 秒；HTTP 已到达但字段缺失不做语义重试。P2B artifact 使用 `price-to-beat-gamma-v2`，每 row 写入 `gamma_http_attempt_count`。已有完整 D 文件时只重请其中的瞬时传输失败 rows；不设 D+2，closed/resolved/winner 及结果一致性不得成为阻断门。
5. 日级辅助脚本发现历史 running/digesting/partial 文件时只写入诊断报告并且绝不读取；不得因一个孤立历史 partial 使昨天整日归档提前终止。上传同名昨天包时必须核对当前本地与云端内容 digest，不得仅因云端已有同名文件就报告成功。
6. 完整 canonical/feature/label/trajectory 处理不属于日级归档脚本；必须在本仓库 `data-pipeline/` 实现，再部署到 5090 服务器的 `~/btc-updown-marketmaker/data-pipeline` 执行。

验收标准：本地测试和真实新 segment 证明 schema v3 同时保留不同的 `event_time/trade_time`；日级归档合同测试证明 D 的 288 个逻辑 slug 全部覆盖、瞬时传输断线可在第二次 attempt 成功、字段缺失仍只请求一次、已有 D 文件只重请瞬时失败 row、Gamma 只用两个目标字段判定回填、历史 partial 不进入 reader、新同名包按 digest 覆盖并验证；热路径两字段保持明确的未实现状态。

## DC-INCREMENT-20260711-ARCHIVE-DEPTH-CHECKPOINT 增量需求

1. 本增量只修复日级归档，不修改或重新部署采集器；数据采集服务器与本地 `typed_raw` 中的 Binance snapshot/update 原始证据不得删除、改写或伪造。
2. 归档必须按 `timestamp_received` 重放 Binance depth：从市场起点前最近的有效 REST snapshot 开始，依次应用起点前全部连续 update，并为市场生成派生起点 checkpoint。checkpoint 的 `last_update_id` 必须表示重放后的 `local_u`，`timestamp_received` 必须是实际最后应用事件的接收时间，不得伪造成 market start。
3. 市场窗口 `[market_start, market_end)` 内出现的每个再次 snapshot 必须随归档保存；其后的第一条 update 必须满足 `U <= lastUpdateId + 1 <= u`，同一链后续 update 的 `local_u_before/local_u_after` 与 `U/u` 必须连续。缺失 snapshot anchor、ID gap 或 local-u 不一致时必须拒绝该 market，不能只凭接收时间 gap 放行。
4. 归档 snapshot/checkpoint 投影必须删除 `snapshot_fetch_start_utc`，只保留 `timestamp_received` 作为该产物的接收时间列；该裁剪不得反向修改 `typed_raw` 原始 snapshot schema。
5. `binance_depth_update_selected_window.parquet` 及归档 snapshot 文件必须显式按 `timestamp_received` 排序，不能依赖 hourly/part 文件名字典序。
6. 修复后的 `2026-07-10` 日归档必须在本地使用既有数据重新校验和打包，明确设置 skip-pull 与 skip-upload；在用户人工审核前不得上传阿里云，不得以本次归档修复为由重启远端采集服务。

验收标准：合同测试必须覆盖市场前 snapshot + bridge update、起点 checkpoint 直连首条 selected update、窗口内第二个 snapshot、缺失 anchor 与错误 `local_u_before` 的拒绝路径，以及归档删除但 raw 保留 `snapshot_fetch_start_utc`；真实 `2026-07-10` 重跑必须报告每个 accepted market 的 checkpoint ID、首条 update ID 连续性、窗口内 resnapshot 数，生成本地 tar 与 SHA-256 后停在用户审核 gate，上传次数为 `0`。

## 一、范围与非范围

### 1.1 data-collect 范围

`./data-collect/` 必须负责：

1. 采集 Polymarket `btc-updown-5m` market channel、Binance `BTCUSDT` orderbook/depth、Binance `BTCUSDT` aggTrade、Polymarket RTDS Chainlink `btc/usd` 四类源。
2. 保存每条原始事件的 raw evidence，包括原始 payload 或等价可还原 `raw_json`、source kind、source event type、source identifier、采集主机、采集区域和 `recv_time_utc`。
3. 为每条源事件记录本地接收时间 `recv_time_utc`，同时保留各 source 原始 timestamp；Polymarket market event 必须保留平台原始 `timestamp` 字段，两者不得互相覆盖。
4. 保存 market discovery / market metadata，包括 `new_market`、condition id、token id / asset id、CLOB market id、slug、start/end、active/acceptingOrders、`market_lifecycle_state`（candidate / prefetched / active / retired）与独立`market_runtime_role`（current / next / resolution_pending / none）、tick size、min size、fee / rebate / reward eligibility 相关原始字段；lifecycle与runtime role必须命名空间隔离，也不得与 ModelRegistry 的 candidate / active / retired 混用。
5. 输出 typed raw parquet、每日唯一manifest、schema version、row count、sha256、既有逐source counter、confirmed integrity gap与顶层单一`collection_day_coverage`；普通transport availability只进入source-local运行态和有界operator日志，不生成receipt、重复sidecar、transport episode或逐事件debug artifact。唯一窄例外是已批准resolution-followup固定E+3..E+8业务slot的每slot最多一条metadata attempt row：activation前由current `polymarket_market_metadata_v3`承载，source v3 + metadata v4原子activation后由v4继承；它进入同一既有Parquet并只由既有manifest file facts覆盖，不扩张manifest schema、artifact、sidecar或通用transport episode，也不双写。
6. 通过 temp/running 分段文件、close/finalize、atomic rename 发布 finalized artifact，禁止消费者读取 partial artifact。
7. 记录网络韧性、资源水位、writer queue、flush latency、RSS/CPU/disk watermark、backpressure 和异常恢复证据。
8. Polymarket market channel覆盖当前active market，并从目标market start前`T-30s`开始采集prefetched next market；Binance depth/aggTrade与RTDS持续独立运行，不取得current/next/prefetched role，也不按market slug或T-30重启/切换。`T-30s`采集覆盖必须与`T-30`样本加工非范围明确区分。
9. 技术验证必须覆盖真实`btc-updown-5m`冷启动market discovery、current active market、prefetched next market、market rollover，以及Polymarket/Binance depth/Binance aggTrade/RTDS Chainlink四个logical producer在同一墙钟期间的长时间独立采集；这不表示把Binance/RTDS绑定到Polymarket market role。generic BTC/Bitcoin open market smoke、HTTP reachability、单条业务消息或synthetic throughput不能替代该验收。
10. 按训练、paper trade、live trade 三种独立运行拓扑导出独立二进制和部署/接入文档；训练二进制用于数据采集服务器持续写训练 raw mirror，paper/live 二进制用于各自交易服务器本机 hot path handoff 和外部市场源证据，不得混入交易决策、下单、撤单、NOE/ledger 或模型输入组装。

本文所有“每条JSON-like源保存parseable `raw_json`/raw evidence”及Polymarket
`new_market` raw evidence条款，对future `source_kind=polymarket_market` v3
persisted artifact/data-pipeline合同收敛为官方SDK解码并通过target admission后的
完整消费typed evidence：唯一canonical domain、每条SDK message一条nested row，
`PriceChange`不逐change fan-out；不持久化`source_native_payload_json`或venue
`hash`，不恢复payload fingerprint。该窄化不改变其他source的raw JSON/evidence、
metadata v3/v4自身payload/digest traceability、历史source v2只读事实或
`HandoffFrame v1.raw_json`。

### 1.2 data-collect 非范围

`./data-collect/` 不负责：

1. `data-pipeline` 的 labels、features、tick_stream、训练切分、canonical split、purged/embargo split 或 T-30 样本加工。
2. 训练标签、execution feedback label、reward、PnL、NAV、ledger、订单生命周期归一化或策略评估。
3. paper/live 交易决策、下单、撤单、风控、模型 intent 改写或 Polymarket submit/cancel 路径。
4. 用 parquet 存在性替代 runtime append-only event log，或用 manifest 替代 label / validation / tick_stream 的消费合同。
5. 在 Polymarket Rust SDK 未覆盖能力时，自行引入 raw REST、manual signing、自建 SDK 或 shadow implementation；后续如发现 SDK 缺口，必须先形成缺口说明、影响范围和可选方案，获得用户同意后才能进入实现。
6. collector runtime 内的发布后归档、远端上传、gzip 压缩或对象存储 listing 验证；唯一已批准例外是 `DC-INCREMENT-20260710-P2B-T-V3-ARCHIVE` 所定义的独立日级运维脚本，不得将其并入采集热路径。
7. paper/live append-only log、rotation artifact、live artifacts、order evidence、NormalizedOrderEvent、ledger、execution feedback ingestion 或训练反馈标签；这些由 paper/live、NOE/Ledger 或后续 data-pipeline/feedback 需求承接。

### 1.3 本地 Polymarket 网络环境约束

1. 本地电脑网络环境不能直接访问 Polymarket；后续如果需要在本地电脑测试或采集 Polymarket 数据，必须使用用户已在本地电脑环境中设置好的香港 VPN 线路。
2. 本地直连 Polymarket 失败不得被误判为 Polymarket Rust SDK 缺口、采集程序错误或 confirmed source gap；必须先记录网络线路、VPN 状态、endpoint和错误类别，并由source-local owner持续恢复。
3. 该通用约束不自行迁移其他事项的数据采集服务器、AWS paper/live服务器或本地synthetic probe验收；本文件顶部已单独同步的resolution-followup AC-12是明确窄例外，其真实source验收固定在本地香港VPN路径。

## 二、时间语义与延迟证据

1. 每条源事件必须保留 source timestamp 或可证明该源没有 exchange timestamp 的快照边界；每条源事件必须记录 `recv_time_utc`。
2. `recv_time_utc` 表示当前采集进程所在主机收到业务消息的 UTC 时间，不等于 Polymarket 平台时间、Binance event time、RTDS payload timestamp，也不等于爱尔兰交易服务器收到消息的时间。
3. 延迟统计必须记录 `collector_host_id`、`collector_region`、`host_clock_sync_status`、source endpoint、source timestamp、`recv_time_utc` 和计算口径版本。
4. 非爱尔兰采集主机的 `recv_time_utc` 只能用于该采集主机视角的接收延迟分析，不得冒充 AWS 爱尔兰 paper/live 交易服务器的真实接收延迟。
5. 当需要评估 paper/live 交易视角延迟时，必须使用对应 paper/live 主机采集或运行 artifact 中的接收时间；跨区域数据只能作为对照证据。
6. `data-collect` 必须保留双时间轴：`recv_time_utc` / runtime receive time 表示数据对当前进程可见的时间，source/platform timestamp 表示源平台声明的事件时间；两者都必须进入 typed raw 和 manifest 可追溯证据，不得互相替代。
7. 训练数据服务器的存储分区默认使用数据采集服务器本机`recv_time_utc`；训练冷路径对没有独立admission合同的legacy/source schema也以它作为availability time。对已批准保存collector admission的current schema，模型causal availability必须使用`collector_admission_time_utc_ms/sequence`，`recv_time_utc/timestamp_received`只保留更早raw观察事实；source/platform timestamp只能作为源事实字段、排序辅助和延迟分析输入。任何字段都不能让尚未完成准入的事件提前进入训练样本causal cutoff。
8. paper/live hot path 必须在对应paper/live服务器本机记录runtime receive timestamp；paper/live模型的因果可见性必须以版本化handoff中的collector admission及本机handoff receive time为准，不能使用训练数据服务器的`recv_time_utc`冒充。对于D6 depth，snapshot等待期的raw ingress不在release/admission前触发模型可见事件。
9. source-specific hourly parquet 中的 collector receive time 列统一命名为 `timestamp_received`；不得同时写入含义相同的 `recv_time_utc` 别名列。`recv_time_utc` 仍可作为 envelope、manifest、partition/coverage 和语义说明中的概念名。

## 三、数据源覆盖要求

### 3.1 Polymarket market channel

1. 必须覆盖 `book`、`price_change`、`last_trade_price`、`best_bid_ask`、`tick_size_change`、`new_market`、`market_resolved` 等 market channel 业务事件。
2. 订阅必须显式记录 token / asset id 列表、raw subscription JSON、custom feature开关、发送事实与直接协议错误；market channel subscription payload必须使用官方字段`assets_ids`、`type: "market"`，验收`best_bid_ask`、`new_market`、`market_resolved`时必须启用`custom_feature_enabled: true`。缺少独立ack或首个业务事件只表示尚无对应业务事实，不得单独写confirmed gap、驱动重订阅或把结构完整artifact判为incomplete。
3. `new_market` 不能被过滤；其 payload 中的 market metadata、condition id、asset id / token id、active 状态、fee schedule、tick size 等必须落盘为 raw evidence 和 typed raw parquet 字段或可追溯 raw_json。
4. market discovery 必须支持 candidate slug 与 active scan / metadata snapshot 双路径；slug 只能生成 candidate，active scan 和 metadata snapshot 必须补齐 condition id、token id / asset id、start/end、active/acceptingOrders、tick/min size、fee/reward、clock drift 证据。
5. market lifecycle 必须至少表达 `market_lifecycle_state`（candidate、prefetched、active、retired）四态；独立`market_runtime_role`表达current、next、resolution_pending或none。prefetched只能表示预加载数据上下文，resolution_pending只表示E后有界结算追踪资格，两者都不能授权交易submit path。
6. 必须记录目标 market、planned market start、preload start、subscription confirmation evidence、first business event、coverage window，并证明 current active market 与 `T-30s` prefetched next market 的覆盖边界；该current/next/T-30生命周期只属于Polymarket market discovery与subscription，不把连续运行的Binance或RTDS source绑定到market slug。T时记录`prefetched -> active`与`next -> current`，E时只记录`current -> resolution_pending`并在无terminal证据时保持active，取得合格WS/Gamma terminal后才记录`active -> retired`与role退出；预算耗尽只退出role且不得伪造retired。
7. `market_resolved`是目标覆盖项，但不得把未观测到该事件直接解释为未resolved；E后必须保留旧market terminal观察资格与assets，优先处理合格WS terminal，并仅在其缺失时按固定identity于E+3 initial及E+4..E+8五次retry执行Gamma fallback。业务静默/stale本身不构成confirmed gap或网络动作；预算耗尽写`resolution_followup_exhausted`而不再新写`unresolved_window_end_observation`。
8. market retired判定必须拆分为两个互斥证据路径：`market_resolved`事件路径必须记录同一target condition/market的`subscribed_asset_ids_for_market`、`resolved_event_asset_ids`、`winning_asset_id`、`winning_outcome`、`close_reason_if_present`或nullable close reason，并校验事件`market`/condition与目标market匹配；fallback metadata路径可在事件尚未观测或当前不可用时读取同一target condition/market，必须记录`market_resolved_missing_reason`、`fallback_metadata_asset_ids`、metadata可见时间、same condition/market校验和close/resolution证据，且不得伪造`resolved_event_asset_ids`。
9. retired predicate必须满足：事件路径中`resolved_event_asset_ids`等于或覆盖`subscribed_asset_ids_for_market`，`winning_asset_id`属于该target condition/market的subscribed/resolved asset set；fallback metadata路径使用活跃窗内固定并验证的exact identity与asset set，fresh attempt必须包含`closed=true`、`acceptingOrders=false`、存在且可解析并不早于E的`closedTime`、精确`resolved`状态及one-hot winner。Gamma`active`只原样保存为nullable字段，无论true/false/null都不参与terminal truth-value判定；closedTime与receive分别保存，但不得增加future-skew上界门。任一路径证据不满足时不得进入retired，identity/schema矛盾仍按完整性边界失败。
10. Ping/Pong只证明transport层交互，不证明业务数据持续产生；collector保留业务消息接收时间和last-qualifying事实供下游因果派生missingness，但不在collector内设置业务静默watchdog、重订阅阈值或退出预算。
11. Polymarket 相关采集路径必须先核对官方 Polymarket Rust SDK 能力；若 SDK 不覆盖 market channel、RTDS、market discovery 或相关 metadata 能力，必须先记录缺口、影响范围和可选方案，经用户同意后才能实现缺口方案。

### 3.2 Binance BTCUSDT orderbook/depth 与 aggTrade

1. 必须采集 Binance 官方 `BTCUSDT` orderbook/depth 和 `BTCUSDT` aggTrade 源，并记录 WebSocket stream name、event type、source sequence / trade id、source event time 和 `recv_time_utc`。
2. live Binance 源只能来自 Binance 官方 WebSocket stream，例如 `btcusdt@depth`、`btcusdt@aggTrade`；depth snapshot 必须来自 Binance 官方 REST `/api/v3/depth` 或当前官方等价端点。RTDS、archive、relay、proxy、cached feed 或历史 backfill 只能作为旁证或补数，不能冒充 live official source。
3. collector只维护snapshot anchor、`local_u`与stitch verdict组成的最小continuity state，用于source integrity/gap detection；它不维护price-level orderbook。snapshot/update中的完整`bids/asks`与`b/a`必须原样保留给后续共享处理核心按独立合同重建，不得在collector生成第二reconstructor、labels、features、tick_stream或模型输入。
4. depth继续使用两个既有source-specific artifact：snapshot从`binance_depth_snapshot_v2`升级为v3，update从`binance_depth_update_v3`升级为v4。depth update必须保留WS raw/typed字段`e`、`E`、`s`、`U`、`u`、`b`、`a`；REST snapshot必须保留`timestamp_received`、`lastUpdateId`、`bids`、`asks`、`snapshot_fetch_start_utc`，不得再写与`timestamp_received`等价的`snapshot_recv_time_utc`别名列，不得伪造exchange timestamp；若使用combined stream，必须保留wrapper `stream`/`data`。
5. snapshot v3与update v4必须共同保存非空`source_sequence_epoch_id`、checked非零`connection_generation`、共享域checked非零`depth_ingress_sequence`、checked非零`collector_admission_sequence`与`collector_admission_time_utc_ms`。depth ingress按collector真实完整观察snapshot响应/update frame的先后冻结；collector admission在snapshot已验证、raw fact从有界buffer按协议release并转移既有bounded queue ownership时冻结。两者不得混用，Parquet物理行序和单独`timestamp_received`都不得冒充模型causal order。
6. 每次WebSocket成功generation必须由唯一direct owner同时运行WS reader和至多一个active REST snapshot future；snapshot pending期间已观察update进入单一row/byte有界buffer。可用snapshot先admit，随后buffered updates按depth ingress顺序admit。正常运行中的snapshot最终不可用、连接提前关闭、普通reconnect/UTC day reset或generation放弃时，结构可验证的buffered updates以中性`not_applied_snapshot_unavailable`最终verdict有界release，不得静默丢失或冒充continuity-ready/gap。可靠operator stop/run deadline关闭共同intake后，尚未取得对应raw-day permit与queue ownership的generation-local pending或跨日deferred staging可以清空且不生成row/gap/告警/`dropped_rows`/terminal；截止前已接纳evidence必须drain/finalize，staging关联的gap副作用只能随最终admission发布。
7. depth source integrity必须能记录snapshot获取、buffered updates、缺序、continuity重建开始、成功和失败证据，但raw观察和continuity projection分层。`u <= lastUpdateId/local_u`的stale/duplicate及`U > local_u + 1`的confirmed-hole update都必须保留v4 raw row和not-applied verdict；前者不记gap，后者使旧snapshot anchor/continuity失效并重新snapshot。同一hole update不得在新snapshot后改写或应用。
8. depth stitch predicate固定为：首个可用update满足`U <= lastUpdateId + 1 <= u`；后续先处理`u <= local_u` stale/duplicate，再以`U > local_u + 1`识别confirmed source gap，其余update才应用并推进`local_u`。raw update已保存不表示continuity-ready；只有合法snapshot与连续应用链可以推进continuity及其last-qualifying事实，其他raw row只更新真实artifact接收事实。这里的“应用”只更新continuity state，不在collector应用`b/a`形成价位簿。
9. aggTrade raw evidence/parser 必须能校验原始字段 `e`、`E`、`s`、`a`、`p`、`q`、`f`、`l`、`T`、`m`；hourly parquet 必须输出 `timestamp_received`、`event_time`、`trade_time`、`symbol`、`aggregate_trade_id`、`price`、`quantity`、`first_trade_id`、`last_trade_id`、`buyer_is_maker`。`event_time` 映射官方 `E`，`trade_time` 映射官方 `T`，两者是不同语义并必须同时保留。
10. Binance depth可证明的update-ID hole、payload conflict、parser/schema或本地continuity state不变量破坏继续写confirmed continuity evidence并按既有scope处理；aggTrade原始`a`数值前跳只保存观察事实，不触发REST backfill、pending、gap、重连或退出。服务器shutdown、rate limit、外部snapshot暂不可用与业务静默只形成source-local availability并持续恢复，不自动进入`source_gaps`。
11. Binance生产实现继续复用现有`reqwest + tokio-tungstenite + Tokio + Serde`和每source唯一direct WebSocket owner，不新增Binance SDK、Barter、Nautilus或其他market-data engine、第二socket/snapshot owner、第三depth artifact或shadow protocol path。未来改变该依赖/owner、使用partial-depth stream或新增consumer schema必须另立决策门。

### 3.3 Polymarket RTDS Chainlink BTC/USD

1. 必须采集 Polymarket RTDS Chainlink `btc/usd` 更新，hourly parquet 保留 `timestamp_received`、RTDS message timestamp、payload timestamp、symbol、value、source topic 和 subscription filter，不再额外写 `recv_time_utc` 别名列。
2. 初始订阅必须分别记录 raw subscription JSON、本地applied观测时间、首条匹配消息或直接SDK/close/error证据；当前本地applied只证明actor把latest desired交给SDK本地发送队列，不得命名为wire write completion、handshake或服务端ACK。Chainlink BTC/USD subscription payload 必须能追溯 `action: "subscribe"`、`topic: "crypto_prices_chainlink"`、`type: "*"` 和 `filters: "{\"symbol\":\"btc/usd\"}"`。
3. RTDS Chainlink BTC/USD 是 Chainlink 价格证据，不是 Binance 官方 orderbook/depth 的替代源。
4. RTDS ping/pong或connection open不能证明业务数据持续产生；collector只保存真实payload、fresh/stale typed分类、接收时间与last-qualifying事实，不因业务freshness写gap、lag、异常告警或结束source。唯一窄例外是第5项共同RTDS合同允许业务静默在同一owner内串行替换整个runtime；stale不直接触发也不重置fresh watchdog。
5. 训练cold path复用唯一官方SDK connection/receiver/actor与latest complete filtered desired；每个真实transport新generation以及每个新runtime的初始connection都只应用一次完整`crypto_prices_chainlink`/`btc/usd`订阅。删除业务静默驱动的同socket refresh、one-shot reconnect、generation预算和进程退出；真实transport故障在同一source-local owner持续恢复，总attempt与总elapsed不构成存活预算。source task曾收到合格fresh后，按RTDS专项冻结的`300s steady + 60s后到monotonic first-fresh + cold-start disabled + 1s × 2 ±20%/300s cap/unlimited`合同执行break-before-make完整runtime replacement。future paper/live真实RTDS adapter仍由各自独立事项实施、测试、验收和部署，但必须继承相同行为合同，不得重新冻结另一套连接生命周期。
6. 正常SDK typed parse失败后，只有顶层object、`message`精确等于`Network error communicating with endpoint`且不存在`topic/type/timestamp/payload`时，才通过crate-private typed marker进入既有recoverable connection exit；不要求精确字段集合或`connectionId/requestId`。该对象不是业务数据，不写row、不计fresh、不直接改变业务freshness/recovery episode或持久化；transport state/generation按现有自然reconnect/backoff变化并重放latest desired。`Invalid request body`、其他未匹配service message、array及目标typed/parser/schema错误继续sticky terminal；仅有未typed的`Too Many Requests`字符串不构成已确认rate-limit，真实HTTP/protocol/typed rate-limit继续按availability恢复。collector不得增加raw JSON parser、第二parser或公共旁路。

## 四、存储、分区与发布

1. 数据保存格式必须是 `.parquet` typed raw mirror；每条 JSON-like 源必须保留 parseable `raw_json` 或等价原始证据引用。
2. 生产数据根路径由用户决定为各运行服务器本机 `~/btc-updown-marketmaker/datasets/`；数据服务器上的训练用原始采集数据必须在该根路径下按 UTC day 分区保存，其中 `~` 按当前服务器用户解析，不得写成本地开发机路径。
3. paper/live 交易服务器如需接收 `data-collect` handoff mirror、coverage 或 manifest，也必须放在对应交易服务器本机 `~/btc-updown-marketmaker/datasets/` 下；该根路径只承载外部市场源证据，不替代 paper/live 自身 append-only event log、rotation artifact、order evidence、NOE/ledger 或 compact mirror。
4. `data-collect` 只提供外部市场源 cold mirror（finalized parquet / coverage / health / gap / manifest）和 hot path handoff 边界证据；paper/live 自身 append-only event log、rotation artifact、order evidence、NOE/ledger、Market Context Builder 输出和 compact mirror 由对应模块需求定义，本文只声明接入边界。
5. 训练 cold mirror 按 `source_kind + recv_time_utc UTC day` 分区；每个 `source_kind/utc_day` 下允许生成多个 finalized parquet segment，不承诺一天固定四个 `.parquet` 文件，也不要求四源写入同一个 parquet 文件。
6. 每个finalized file的路径、source/hour/part、接收时间事实、row count、bytes、sha256与schema事实由每日唯一manifest记录；不再生成per-segment manifest或把外部业务覆盖率当作file结构完整性的先决条件。
7. 正常运行只在UTC hour boundary执行file-level rollover；协调shutdown可以提前finalize当前hour。同小时启动/重启发现finalized/running路径碰撞时选择`part=N`，该碰撞不是运行中rollover。`50,000 rows / 16 MiB logical bytes`只结束当前Parquet row group，不创建新文件或`part=N`；不再设置独立的`max_rows`、`max_bytes`或`max_segment_duration`文件切分门。行情暴增继续记录burst、queue watermark、flush latency和backpressure observation，不得因普通row-group flush自动停机。
8. Parquet 连续写入必须写入 `*.tmp`、`*-running.parquet` 或等价 running segment；close/finalize 后才能通过 atomic rename 发布 finalized `.parquet`。
9. 每个 source parquet 必须写入 typed raw schema version / source kind metadata，至少包括 `parquet_raw_layout_version` 和 `parquet_raw_source_kind`；兼容当前 V2 typed raw mirror 是迁移基线，但本文不把 V2 data-pipeline schema 变成 `data-collect` 的加工职责。
10. 每日唯一顶层`data_collect_daily_manifest_v3`保留既有build/binary/config/host/region、file facts、六个source counters、continuity、confirmed gaps和operational counters，并直接嵌套一个`data_collect_collection_day_coverage_v1`对象。coverage对象只含`not_started|active|ended_known|unclean_end_unknown`、`first_started_utc_ms|last_started_utc_ms|ended_utc_ms`三个nullable `i64` UTC毫秒及必填布尔值`reactivated_within_day`；首次activation为`false`，任一同一UTC日后续activation为`true`且当日保持单调，不统计activation或unclean次数。对象不得复制进六个counter，不新增transport scope/reason/episode、extensions容器、receipt或sidecar。非空confirmed gap不自动否定file/schema/provenance完整性，训练准入由data-pipeline决定。
11. 每日manifest的既有file facts必须逐个记录实际finalized文件的role/path、row count、bytes、sha256/digest与schema事实；不得新增`required_files[]`、missing-reason gate或collector级complete/incomplete validation verdict。文件/schema/provenance事实不完整仍按第13项fail closed，训练内容准入由data-pipeline决定。
12. parquet / manifest 文件发现路径的消费者只能读取 finalized parquet 和对应 manifest；temp/running、`.rsync-partial`、外部同步/发布链路生成的未完成标记文件、未 finalize 文件不得进入 discovery，`data-collect` 不负责 upload/listing。
13. atomic rename失败、sha256/row count/schema version缺失、manifest不一致，或新writer缺少/损坏`collection_day_coverage`时必须fail closed；外部业务coverage不足、nullable last-qualifying或legacy v3缺coverage对象不等于artifact结构损坏。legacy缺对象只读为unknown且禁止回填。
14. 当前需求只要求 finalized artifact 不被 partial、running 或 `.rsync-partial` 误发现。
15. 训练 cold path 必须迁移为 UTC day 文件夹 + 每 source kind 每小时一个逻辑 partition：`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH[--part=N].parquet`。正常目标是一小时一个 finalized parquet；`part=N`只用于同一小时已存在 finalized/running 路径时的启动或重启碰撞，不由 row count、logical bytes或segment duration触发。
16. Polymarket `book` 事件必须保存完整 `bids` / `asks` price-size depth，能还原 token orderbook；不得只保存 `bids_len` / `asks_len` 却标记为 complete book。PMXT-like schema 只作为 Polymarket orderbook typed layout、hourly parquet、compression/sort 和 manifest 参考，不引入 PMXT archive source。
17. Polymarket hourly typed layout 必须覆盖 `book`、`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` 的 typed 字段或 nullable unavailable reason，并保留 collector host/region、slug、condition id、asset id、coverage 和 source evidence。
18. Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink 必须进入 hourly typed layout 修改范围，分别使用官方字段的 source-specific schema：`binance_depth_snapshot_v3`保留`timestamp_received`、`lastUpdateId`、完整`bids/asks`、snapshot boundary及共同depth因果坐标，`binance_depth_update_v4`保留`timestamp_received`、`e/E/s/U/u/b/a`、stitch/not-applied verdict及同一坐标域；future current `binance_aggtrade_v7`按独立专项合同保留trade/invalid判别、官方trade字段与WS因果坐标，并只删除与`timestamp_received`恒等的`websocket_timestamp_received`；`polymarket_rtds_chainlink_btc_usd`保留`timestamp_received`、message timestamp、payload timestamp、symbol、value、topic和subscription filter。旧depth snapshot v2/update v3与aggTrade v3/v5/v6只读兼容，不得伪造新ordering字段。
19. 训练dataset hard threshold固定为`50_000_000_000` bytes，作用域是`datasets/training`。达到阈值时collector按既有协调finalize合同保护已接收数据并以保留退出码75结束，不生成retention/threshold receipt，也不在collector内自动删除raw。
20. 归档、retention和真实删除由独立运维事项处理；本需求与当前专项不授权删除任何UTC day、远端临时验证或生产数据清理。未来若批准删除，必须另建需求/计划/changelog和精确路径、安全门及验收，不得从50GB阈值或旧部署事实推导删除权限。
21. 资源硬阈值的触发值、观测值、协调finalize与退出结果只进入有界operator日志、测试输出和对应changelog证据；不得为此扩张每日manifest或生成run-level policy/ResourceWatermark artifact。

## 五、网络异常与恢复要求

### 5.1 必须显式记录的异常

以下情况必须先按证据分为外部availability、业务missingness基础或confirmed integrity failure；不得把尚未观测到业务数据写成“完整外部覆盖”，也不得把普通availability伪造成confirmed gap：

1. DNS/TCP/TLS/WebSocket/HTTP连接失败、断链、server shutdown、rate limit和合法空候选属于外部availability，由对应source-local owner持续恢复。
2. 长时间无业务消息、持续stale、ping/pong正常但业务静默属于下游missingness的因果事实；collector保留接收时间、last-qualifying和真实空档，不由此记录gap/lag/异常告警。唯一网络动作例外是第3.3节已冻结的RTDS完整runtime replacement；它不适用于CLOB、Gamma或Binance。
3. 订阅/HTTP/WS直接协议错误须区分可恢复transport错误与无法验证的parser/schema错误；前者持续恢复，后者按既有受影响范围fail closed。第3.3节第6项获批的数据外RTDS network-error对象是精确transport分类例外，不属于parser实现或typed-schema失败，且不得泛化到其他service message。SDK070 `P-B1E`只把九个官方nullable Decimal的精确空串规范化为`None`，不是错误重试或恢复；required、非空malformed和其余parser/schema错误仍fail closed。
4. Binance depth update-ID中可证明的sequence hole、payload conflict、SDK `Lagged(n)`、queue drop/timeout、writer/manifest/atomic publish和内部不变量才进入confirmed integrity路径；aggTrade ID数值前跳不证明hole，只保留原始观察。
5. 资源硬边界继续按既有合同保护已接收数据；普通诊断日志失败、计数饱和或缺少专用reason不得结束source/collector。

外部availability只进入source-local运行态和有界、去重operator日志；每日manifest不保存transport current/open、scope、reason或episode，也不生成compact health receipt或逐事件debug artifact。唯一已批准例外是resolution-followup固定E+3..E+8业务slot的metadata attempt row：每slot最多一条，activation前写current metadata v3，source v3 + metadata v4原子activation后写v4，并进入同一既有metadata Parquet；manifest仍只保留既有file row count/bytes/sha256/schema facts。该例外不得双写或泛化到普通connect/read/backoff episode。

### 5.2 恢复策略需求

1. 各source的单次connect/HTTP/read/backoff等待必须有界，使用capped exponential backoff与既有jitter，禁止tight loop；source外层总attempt和总elapsed无限，不存在availability耗尽即退出的存活预算。
2. 各source只有一个source-local transport owner并独立恢复；Polymarket、Binance、RTDS任一源故障不得阻塞健康源，也不得建设第二套health/reconnect控制面。
3. 真实新connection generation按各协议重放最新desired：Polymarket只发送真实asset diff或完整latest set，RTDS每个transport generation及每个新runtime的初始connection只应用一次完整filtered desired，Binance depth重新snapshot并完成官方stitch。
4. Connection open或PONG只表示transport事实；业务消息只更新last-qualifying事实。业务静默/stale不触发同socket refresh、gap、lag、告警或退出；仅RTDS在满足第3.3节冻结条件后可由同一owner串行完整替换runtime，其他source不变。
5. Binance depth stale/duplicate和confirmed-hole update都保存v4 raw row但不得错误推进continuity：前者只写not-applied且不记gap，后者保留confirmed continuity evidence、使旧snapshot anchor/`local_u`失效并重新snapshot。aggTrade ID前跳保持中性且不创建REST backfill、pending、merge、range retry或continuity epoch。
6. RTDS自然重连或完整runtime replacement后都验证topic、symbol filter和payload typed语义；stale payload可落盘但不直接触发或重置business watchdog。第3.3节第6项的精确数据外network-error对象只触发现有自然重连；SDK `Lagged(n)`、其他parser/schema与目标payload不变量仍按完整性边界处理。

## 六、Rust async/non-blocking 运行要求

1. 实现必须使用 Rust，并基于 Tokio 或等价 async runtime。
2. WebSocket、HTTP、timer、channel、manifest 写入、health 写入和 parquet writer 调度不得阻塞主事件循环；需求层验收词为 async 和 non-blocking。
3. 文件 I/O、parquet close/finalize、data-collect 内部 parquet compression、local directory scan、sha256、large manifest aggregation、本地旧 segment 清理等可能阻塞或重 CPU 工作必须隔离到后台任务或 blocking pool；不包含 gzip 归档压缩、远端上传、对象存储 listing 或归档保留清理。
4. 所有跨任务channel必须有有限capacity。current runtime以全局inflight row count、contract-owned-byte硬预算、单行上限和有界acquire/send wait限制跨queue/await的ownership，并使用既有批量/周期flush。queue非空持续时间、writer lag、flush latency、RSS和资源水位只作operator观测；不得据此为每个队列重建oldest-event-age、writer-lag或独立memory-watermark硬配置/第二控制面。既有全局hard-resource threshold仍按第七节独立验收。
5. 当bounded queue达到soft watermark或短暂水位尖峰时，系统必须在有界operator证据中记录backpressure；只有可证明的send timeout/drop、queue/receiver不变量、writer失败或hard resource threshold才进入confirmed gap/受控失败。外部source永久不可达或业务静默不属于“不可恢复source缺失”；禁止unbounded queue导致OOM或silent loss。
6. Parquet writer 必须记录 row group / buffer 的内存估算、flush latency、bytes written 和 finalize 结果；未 finalize 不得发布。
7. HTTP client 必须有 connect/read/total timeout 或等价超时语义；默认无超时不能作为生产需求。

## 七、性能与资源验收基线

1. Polymarket `btc-updown-5m` 事件量验收以现有估算为基线：单 5 分钟市场 p95 约 275k events，max 约 394k events；加 T-30 秒预加载后 p95 约 302k events，max 约 433k events。
2. 工程吞吐基线必须按四源合并`2k-5k events/s`解析、排队和写入能力设计；性能验证使用测试输出/changelog，不把逐源吞吐、transport health或完整episode复制进每日manifest。
3. writer queue 必须记录 row/byte 高水位、queue age、backpressure 次数、flush latency p50/p95/max、finalize latency 和失败次数。
4. 资源压力测试和有界operator诊断必须覆盖当前环境可取得的RSS、CPU、disk free、artifact dir bytes、open files、queue bytes/age、writer memory estimate及cgroup/PSI关键证据；汇总结论进入测试输出/changelog，不生成ResourceWatermark artifact，也不把无法测得的字段批量写入manifest。
5. 数据采集服务器和交易服务器必须分别记录 hardware tier；live 交易小主机不得承担训练式重处理、历史全量 parquet 扫描、压缩归档批处理或 Rust 编译。
6. AWS 爱尔兰 live-trade 服务器是运行目标，不是构建机；`data-collect` 相关 Rust 编译、cargo test/check/fmt 和重处理必须在具备硬件条件的构建/数据服务器完成。
7. 资源hard threshold触发时必须记录观测值，停止新接收、保护已接收数据、完成可行的atomic publish与每日manifest保全，并按既有协调退出码75结束；不得生成ResourceWatermark artifact或自动删除raw。
8. 实现中的资源threshold常量/配置及测试必须可追溯阈值、单位、采样窗口、触发动作与结果；证据只进入代码、测试输出、有界日志和changelog，不生成threshold policy receipt。

## 八、消费者边界

1. 训练消费者走cold path：读取按天分区的finalized typed raw parquet、每日manifest、既有source counters与单一`collection_day_coverage`，由后续`data-pipeline`因果派生age/mask并生成labels/features/replay context/训练切分；`data-collect`不直接生成这些训练消费产物或模型wake。
2. paper/live消费者走hot path：其Market Context Builder/runtime通过各自服务器本机实时handoff、bounded channel、IPC/local stream或等价低延迟事件流消费外部市场源事件、last-qualifying与直接可观测availability事实；不得把等待finalized parquet/manifest作为唯一热路径，也不得从训练collector的transport episode推断交易动作。
3. `data-collect`的hot path职责只到外部市场源事件、timestamp namespace、last-qualifying、confirmed integrity evidence、直接transport事实和handoff边界；paper/live append-only log、NOE/ledger、模型输入组装、调度和execution feedback不由训练collector生成或解释。
4. paper/live 延迟验收必须区分 `collector_recv_time_utc(host,region)`、handoff send time、paper/live runtime receive time、decision time 和 causal cutoff；非爱尔兰 collector 的 `recv_time_utc` 不能冒充 paper/live 爱尔兰主机实时接收延迟。
5. data-pipeline 可以消费 `data-collect` 的 typed raw parquet 和 manifest，但必须自行生成 labels、validation、tick_stream、features 和训练切分证据。
6. 采集artifact结构完整不等于外部业务coverage 100%，也不等于训练批准、labels/features已完成或paper/live交易逻辑健康；共同collection coverage只表达collector observation eligibility，不表达具体transport原因或模型动作。
7. 训练、paper、live 三类运行形态必须导出独立二进制：训练二进制只服务数据采集服务器训练 raw mirror；paper 二进制只服务 paper 服务器热路径外部市场源 handoff；live 二进制只服务 live 服务器热路径外部市场源 handoff。三者共享 schema/manifest/source adapter 合同，但不得共享运行根、钱包、交易状态或消费者 artifact 语义。
8. 当前paper/live binary仍只有heartbeat/status server，不宣称已接入真实RTDS。future真实RTDS adapter接入时，网络生命周期必须继承第3.3节共同RTDS合同；其handoff sink、runtime receive、背压、append-only log、长跑和部署由各自独立三件套验收，且不得启动training的Parquet/manifest/dedupe-restore路径。

## 九、验收标准

`data-collect-demand` 后续实现或计划至少必须能验证：

1. `./data-collect/`的职责边界不得超出外部市场源采集证据边界；允许范围包括raw evidence、`recv_time_utc`、typed raw parquet、last-qualifying、confirmed integrity gap、每日manifest、单一collection coverage、atomic publish、source-local网络韧性和有界operator/资源证据，不包括transport episode tape或模型调度。固定resolution业务slot的metadata attempt row只按第一节第5项current-v3/future-v4同一窄例外验收，不得泛化或双写。
2. 每条源事件都保留 `recv_time_utc`；Polymarket 每条 market event 同时保留平台 `timestamp` 和本地 `recv_time_utc`，并记录采集主机/区域。
3. 非爱尔兰采集主机延迟不会被报告成爱尔兰 paper/live 交易主机接收延迟。
4. Polymarket market channel在启动、重启和每次market lifecycle/role切换时必须覆盖当前active market；next market必须在planned market start前`T-30s`具备覆盖，并记录target market、planned market start、preload start、subscription confirmation evidence、first business event、coverage window、`prefetched -> active`、`next -> current`、`current -> resolution_pending`及terminal后的`active -> retired`边界证据。Binance depth/aggTrade和RTDS持续独立运行，不取得这些market role，也不按slug/T-30切换。预算耗尽路径必须证明只退出pending role而不伪造retired。
5. `new_market`、market metadata、condition id、token id / asset id、start/end、`market_lifecycle_state`（candidate / prefetched / active / retired）与独立`market_runtime_role`（current / next / resolution_pending / none）全部可从artifact追溯；两者不得跨域比较，也不得与ModelRegistry状态混用。
6. market discovery 必须证明 candidate slug + active scan / metadata snapshot 双路径，slug-only 不能作为完整 discovery 成功。
7. Polymarket、Binance BTCUSDT orderbook/depth、Binance BTCUSDT aggTrade、RTDS Chainlink BTC/USD四个logical producer都保留required event types、IDs/timestamps、raw_json/typed fields、last-qualifying和confirmed integrity evidence；每日manifest只保存一个共同collection coverage，不复制四份producer或六份source coverage。
8. `market_resolved`/retired验收必须证明事件路径或fallback metadata路径之一满足retired predicate；事件路径必须记录同一target condition/market的`subscribed_asset_ids_for_market`、`resolved_event_asset_ids`、`winning_asset_id`、`winning_outcome`、nullable`close_reason_if_present`。fallback路径必须覆盖固定identity、exact assets、fresh attempt、`closed=true`、`acceptingOrders=false`、可解析且不早于E的closedTime、精确resolved状态与one-hot winner；`active`值不得成为truth-value gate，closedTime与receive不得增加future-skew上界。另须验证E+3..E+8六slot、metadata v3的既有五种row/19项source failure/terminal-once/drain/exhausted/current source-v2 restart矩阵，以及future metadata v4对同一五种row的不可变继承、WS claimant pair null matrix、source-v3 recovery与cross-pair fail-closed矩阵。本事项真实source验收必须在本地原生Ubuntu环境通过已配置香港VPN线路执行，并按专项AC-12保存测试前、运行期间及结束后的线路证据；不得静默回退本地直连，也不得把本地接收延迟写成AWS爱尔兰paper/live延迟。
9. `best_bid_ask`、`new_market`、`market_resolved`验收必须证明`custom_feature_enabled: true`、订阅发送事实/直接错误和SDK支持状态；没有独立ack或尚无首个匹配业务事件只保留为未观测事实，不自动把manifest写成source gap/incomplete。
10. Binance live源必须来自Binance官方WS/REST并继续由现有唯一direct owner实现；Cargo/lock和生产调用图不得新增Binance SDK/market-data engine。aggTrade必须保留原始`e,E,s,a,p,q,f,l,T,m`；depth update v4必须保留`e,E,s,U,u,b,a`及共同source epoch/generation/ingress/admission坐标，snapshot v3必须保留`lastUpdateId,bids,asks`及同一坐标域且不得伪造exchange timestamp；snapshot pending/success/failure release与depth stitch predicate必须可审计。
11. 连续 parquet 写入通过 running/temp segment 承载，close/finalize 后 atomic rename；parquet / manifest 文件发现路径只读 finalized parquet；每个 source parquet 必须带 typed raw schema version 和 source kind metadata，至少包括 `parquet_raw_layout_version` 和 `parquet_raw_source_kind`。
12. 每日唯一顶层v3 manifest至少包含既有build/binary/config/host/region、file facts、六个source counters、continuity、confirmed gap与operational counter，并直接嵌套需求第四节冻结的单一`data_collect_collection_day_coverage_v1`；不保存validation verdict、required-files gate、ResourceWatermark大字段、transport scope/reason/episode、逐producer/source coverage复制、receipt或重复sidecar。
13. paper/live 自身 append-only event log、rotation artifact、live artifacts、order evidence、NOE/ledger 和 execution feedback ingestion 必须由其他模块需求承接；`data-collect` 只能提供外部市场源 finalized mirror、coverage、health、gap、manifest 和 hot path handoff 边界证据，不能私自解释交易运行证据。
14. 训练 cold path 与 paper/live hot path 的接入边界必须可验证：训练消费 finalized typed raw parquet + manifest + `data-pipeline` 输出；paper/live 消费 realtime handoff 或等价低延迟事件流，并在对应 paper/live 主机记录 runtime receive timestamp；Phase 0 或后续验收不得用 finalized parquet 可见时间替代 paper/live 实时接收延迟。
15. 网络异常、rate limit、server shutdown和外部snapshot暂不可用由source-local owner持续恢复；返回空、长时间业务静默、ping/pong正常但无业务数据与source stale只保留真实空档/last-qualifying，不进入`source_gaps`、业务lag、异常告警或退出。仅RTDS按第3.3节共同合同在曾有fresh后可完整替换runtime；confirmed sequence/local drop/parser/writer等完整性事实继续保留。
16. 恢复策略对单次等待与backoff有界并使用jitter，但按源外层总attempt/elapsed无限；真实重连后按协议恢复subscription/snapshot，业务新鲜度不作为transport恢复门。RTDS每个真实generation和每个新runtime初始connection只应用一次完整filtered desired，生产/测试/vendor均删除business-silence one-shot `request_reconnect`能力；完整runtime replacement必须证明单owner/socket、break-before-make和`300s/60s/cold-start disabled/outer cap 300s`，并以连续两次replacement的精确顺序测试覆盖重复路径，不新增100-cycle或进程资源矩阵。
17. Transport诊断使用有界、去重operator日志或现有counter；日志失败、专用reason缺失、计数饱和或episode持续时间不改变运行结果，daily manifest不充当完整reconnect trace。
18. Rust async/non-blocking 边界成立：主事件循环不做阻塞 I/O、重 CPU、data-collect 内部 parquet compression、local directory scan、sha256 或本地旧 segment 清理；所有队列都是 bounded queue，并具备 backpressure。archive/upload/listing/gzip 不属于当前 `data-collect` 验收项。
19. 性能验收覆盖Polymarket p95/max事件量、四源合并`2k-5k events/s`工程吞吐、writer queue、flush latency、RSS/CPU/disk、hot path handoff latency、durable parquet write latency和hardware tier；测试/运维证据进入changelog或有界日志，不扩张daily manifest schema。
20. live技术验收应尽力验证`btc-updown-5m`冷启动发现、market rollover和四源真实数据，但平台当时没有产生/交付某源业务数据不构成collector实现失败；确定性测试harness必须覆盖单源/多源/全源静默与transport故障，生产零行由active共同coverage、nullable source counter接收事实和typed raw真实空档表达。
21. ResourceWatermark硬阈值验收必须证明触发条件、保护已接收数据和finalize/manifest动作；不新增threshold receipt，普通availability、业务静默和诊断容量不得冒充资源硬阈值。
22. archive/upload/listing/gzip 不属于 collector runtime 验收项；已批准的 D+1 独立日级脚本仅按 `DC-INCREMENT-20260710-P2B-T-V3-ARCHIVE` 验收，不得扩张进 runtime。
23. Polymarket 相关采集路径必须先有官方 Rust SDK 能力核对记录；SDK 缺口方案必须先获用户同意。
24. 文档、计划或实现不得把 data-pipeline 的 labels、features、tick_stream、训练切分、T-30 样本加工、paper/live 交易逻辑或 SDK 旁路实现写进 `data-collect` 范围。
25. 训练、paper、live 三类运行形态均能独立导出 release 二进制、sha256 和运行说明；训练数据采集必须有独立部署 README，paper/live hot path 必须有独立接入技术文档。
26. 训练cold mirror按`source_kind + recv_time_utc UTC day`分区，每个source/day可包含多个finalized file；每日唯一manifest保存每个file的source/hour/part、接收事实、row count、bytes和sha256，并保存共同collection coverage，不生成segment manifest。
27. 正常运行只因UTC hour boundary执行file-level rollover；协调shutdown可以提前finalize当前hour，同小时启动/重启路径碰撞只选择新`part=N`。`50,000 rows / 16 MiB logical bytes`只切Parquet row group。正常行情放大不得因row-group flush自动停机；只有不可写、无法保全、持续hard threshold、drop/send timeout或partial artifact被发布等条件才能进入fatal动作。
28. 训练样本因果可见性对legacy/无独立admission schema以采集服务器`recv_time_utc`为默认主轴，对current aggTrade/depth等已批准schema以collector admission time/sequence为主轴并保留更早raw ingress审计；paper/live以版本化handoff admission及对应服务器本机runtime receive timestamp为主轴。source/platform timestamp和Parquet物理行序不能替代receive/admission available time。
29. paper/live 二进制的 hot path 接入文档必须明确 IPC/local stream/bounded handoff 协议、event seq、timestamp namespace、backpressure/status 消息、runtime receive timestamp 记录点和非范围边界，且不得生成交易决策、NOE/ledger、order evidence 或 execution feedback。
30. PMXT-inspired Polymarket orderbook hourly parquet 必须证明 `book` rows 全部保留可解析 `bids` / `asks` 数组字段；真实市场中单侧空数组可以是盘口状态，必须在人工检查报告中单独计数，不得把完整 price-size 数组缩水成 `bids_len` / `asks_len` 后伪装完整。`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`、`market_resolved` 必须有对应 typed 字段、raw-equivalent evidence 或 nullable unavailable reason。
31. Binance depth snapshot/update、Binance aggTrade、RTDS Chainlink hourly parquet必须分别证明required typed fields存在，且不能只依赖opaque `source_kind/raw_json`宽表；Binance aggTrade current v7及legacy v3/v5/v6必须按各自合同证明`event_time=E`与`trade_time=T`等字段没有被归档输出丢弃。Binance depth current snapshot v3/update v4必须证明共享depth ingress与collector admission坐标、snapshot先admit/buffered updates按ingress release、stale/hole与正常运行中snapshot-unavailable的已接纳raw evidence不丢失且不错误应用；跨日deferred在新日permit前不得提前分配admission，正常rollover按staging顺序flush；可靠operator stop/run deadline截止后的未接纳depth pending或跨日staging不生成raw row、gap、告警、`dropped_rows`或terminal，以及`local_u`单调推进或gap/resnapshot可解释；legacy snapshot v2/update v3只能标为ordering unknown。
32. 训练 cold path finalized discovery 必须使用 day-first `utc_day=YYYY-MM-DD/source_kind=<source_kind>` 路径；legacy source-first 路径只能作为历史只读兼容被显式标记，不能与新产物混写。
33. 50GB hard-threshold验收只证明触发后停止新接收、协调finalize/manifest保全并以退出码75结束；collector不生成retention/threshold receipt且不删除raw。任何归档、远端验证或真实删除都必须等待独立需求、计划和用户批准。

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
