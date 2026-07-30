# 数据采集可靠性修复需求

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文件保留早期可靠性修复历史。其旧 RTDS 业务静默语义已被当前
> 300s/60s 完整 runtime replacement 合同取代，不得作为当前实现依据。
>
> 事项编号：`DC-REPAIR-20260713-COLLECTOR-RELIABILITY`
>
> 对应计划：`plan/data-collect-reliability-repair-plan.md`
>
> 对应 changelog：`changelog/data-collect-reliability-repair-changelog.md`
>
> 状态：`SUPERSEDED_IN_PART_BY_SOURCE_AVAILABILITY_LIFECYCLE`。已完成并通过审查的CLOB `10s/20s/require_text_pong=true`继续作为基线；旧RTDS `5s/15s PING/PONG`基线已由`demand/data-collect-source-availability-lifecycle-repair-demand.md`阶段9 `RTDS-PONG-A`取代，该阶段已于2026-07-19完成本地实现、全量门禁、香港线路复测及独立终审，当前RTDS为固定`5s/require_text_pong=false`。用户于2026-07-17批准新专项后，本文件的业务静默恢复预算、D2 one-shot reconnect、CLOB freshness退出、普通网络/空结果gap、有限总恢复、旧SDK patch白名单、daily coverage和同日生产切换/清理语义均被取代。2026-07-25独立SDK070事项已在当前Stage1 worktree把SDK身份迁移为官方0.7.0底座的唯一`0.7.0-p1`vendor；本文件只继承仍有效的SDK-first、唯一owner、四路径patch与heartbeat/teardown语义，不再拥有版本/path/provenance authority。本文其余已实现数据完整性合同继续有效；旧执行事实只由原changelog保留。SDK070 Stage1独立审查已为`Critical 0 / Important 0 / Minor 0 / PASS`，用户已另行批准发布；其闭合以承载记录的commit成功推送且远端SHA与本地HEAD/upstream一致为生效条件。本同步不授权生产构建、部署、远端服务操作或数据清理。
>
> 当前训练collector首次正式冷启动的独立部署authority为`demand/data-collect-training-collector-direct-deployment-demand.md`；本文件及旧生产阶段不得用于执行`Restart=no`、gate、UTC边界等待或数据清理。
>
> 2026-07-25 Polymarket source-v3窄化：本文件中canonical JSON、
> `source_native_payload_json`、payload fingerprint、逐change fan-out及其restart
> recovery只继续描述current/historical `polymarket_orderbook_v2`。future
> `polymarket_orderbook_v3`的唯一authority为
> `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`及对应
> plan/changelog；其本地实施、测试、审查、commit和push已由用户持续执行授权覆盖，
> production activation/cutover、release、部署、远端操作、migration和删除不在该
> 授权范围。

## 一、目标与权威边界

本事项只修复训练冷路径的 `data-collect-training-collector`，使它能够长期、连续地采集 Polymarket、Binance 与 RTDS 原始市场数据，并把生产数据目录收敛到最少且明确的必要产物。

本需求是用户 2026-07-13 指令形成的聚焦增量。它只在“数据采集服务器上的训练 collector”范围内取代 `demand/data-collect-demand.md` 下列旧合同；未列出的 source 字段、SDK-first、训练/paper/live 独立运行角色与 data-pipeline 边界继续有效：

- 第 11 行的训练 service `Restart=always`，改为 `Restart=on-failure`，为受控退出码 75/76 禁止重启，并增加明确 StartLimit；
- 第 49、51、128、132-135、163、189-191、217、219、222、225、227、229、231、236 行的 per-segment/run/receipt、complete/PASS、ResourceWatermark、重复 reconnect/coverage/queue 证据合同，改为每 UTC 日唯一事实 manifest 与 journald；
- 第 129、137、237 行的 row/byte protective file rollover，改为只按 UTC hour 与同小时进程重启分 part，row-group 上限只约束内存；
- 第 141-143、243 行的 collector 自动 retention/delete/receipt 合同，改为 50GB 时退出 75、由独立归档运维处理；
- 第167-169、226行的恢复合同改为：单次connect/read/HTTP/backoff等待有界并使用capped exponential backoff+jitter，训练source外层总attempt与总elapsed无限；availability不存在`Exhausted`终止态，且禁止tight loop；
- 第 228 行的“所有队列 bounded”在项目自有 data-bearing 路径继续严格成立，并升级为全进程 row+byte 双上限；用户 2026-07-14 基于当时官方SDK 0.6.0明确批准其内部outgoing/ping控制通道作为唯一类型例外，SDK070迁移后的`0.7.0-p1`只继承同一有限生产者不变量，禁止把项目数据放入该例外；
- 第 229 行的四源合并 `2k-5k events/s` 处理能力继续有效；仅其独立 profiler/ResourceWatermark/validation artifact 被取代。吞吐使用自动删除临时目录中的既有 benchmark/fixture 验证，汇总结论只写 changelog；`5k events/s` 同时只是容量 sizing 上界，不代表平台实际持续速率；
- 第 110、140、220、241 行关于 Binance aggTrade `e/E/s` 与 `event_time=E` 的非空要求继续约束 WebSocket 行；本事项新增的 REST-only 行允许 WS 专属 `event_type/event_time/stream` 为空，merged 行按本需求 3.2 的字段与 availability 规则执行；
- 第 235 行的三类独立 binary 要求继续有效；本事项的“只部署一个训练 binary”只约束数据采集服务器本次 release，不取消未来 paper/live 在各自服务器上的独立 binary。
- 第119、226行的训练RTDS周期refresh、业务静默主动reconnect与进程重启合同全部被新专项取代；RTDS只在真实transport generation变化后replay一次latest complete filtered desired，future paper/live仍由各自独立需求冻结。
- `demand/rebuild-core-demand.md` 数据与证据要求第 10 项中原有的 `data-collect` per-run manifest、validation verdict 与 `required_files` gate 被本事项取代：collector 每 UTC 日只维护一个事实 manifest；内容校验、语义去重和训练准入由 `data-pipeline` 完成。training、offline-evaluation、paper、live 与 sim-calibration 的 run manifest 合同不变。

`plan/data-collect-reliability-repair-plan.md`曾在相同范围内取代旧主计划；其未执行的生产阶段现已被新专项M8-A边界取代，不再提供可执行部署/清理授权。原changelog仍保留历史事实。

本事项不实现`data-pipeline`的canonical、语义去重、feature、label、split、训练准入或模型调度，不实现paper/live采集二进制，也不重建每日拉取、打包和上传脚本。

## 二、已核实问题与责任判断

1. 2026-07-11、2026-07-12 的拉取文件与远端对应文件一致，归档传输不是这两日重复和缺口的制造者。
2. Polymarket 两日存在大量完全相同的业务 payload 重复行；当前证据不能把原始重复唯一归因于平台或 SDK，但采集器没有保守的传输级去重，必须防止重复写入。
3. Polymarket discovery 当前把 current 与 next 绑定成全有或全无；next 暂时不存在时会放弃有效 current，这是本程序策略错误。
4. 当前程序把 synthetic `market_discovery_switch` 伪装成 `new_market`，同时又保存原生 WS `new_market`，这是本程序语义与重复路径错误。
5. 2026-07-13审计时，官方 Polymarket Rust SDK 0.6.0 的 CLOB 与 RTDS 高层 typed streams 都会隐藏 broadcast lag；当时程序无法把 `Lagged(n)` 作为可恢复故障处理。这是历史缺陷基线，不是对当前SDK版本身份的声明；现行能力由source-availability第4.7节四路径patch语义与SDK070当前vendor共同提供。
6. Binance aggTrade 在 2026-07-12 缺少连续 aggregate trade ID。官方 REST 能确认缺失成交存在，说明丢失发生在平台到本地落盘之间；当前程序没有 ID 连续性检测、重连后续接或 REST 回补，这是本程序韧性缺口。
7. RTDS 出现接收时间很新但 payload timestamp 已陈旧的消息；陈旧推送可能来自平台或网络重放，但当前程序会把任意成功消息当作 freshness 恢复，这是本程序判断错误。
8. metadata 与 Binance snapshot 的 fetch-start 被 receive time 回填，`metadata_recv_time_utc`/`timestamp_received`、`token_ids`/`asset_ids`、`local_u_after`/`u` 等还存在等义字段，这是本程序 schema 错误或冗余。
9. bounded queue 超时、writer channel 关闭及部分 writer 错误会被记录后继续运行，可能静默丢行；segment manifest 又硬编码 `Complete`，这是本程序错误。
10. 历史事实：2026-07-13审计时远端旧采集器仍在运行并写入约14GB旧数据，且若干日期没有完整本地镜像。此后远端状态已变化；当时的清除方案没有执行且不构成当前授权。生产数据清理必须重新核实实时状态并取得独立需求/计划与用户批准。

## 三、采集正确性需求

### 3.1 Polymarket

1. market discovery 必须以有效 current 为最低条件；next 独立发现、独立预订阅，暂时不存在不得阻断 current。切窗时已有 next 晋升为 current，过期资产才退订。
2. 只允许官方 SDK 原生 `WsMessage::NewMarket` 生成 `new_market`。Gamma discovery 只能生成 `market_discovery`/`active_scan`，并保留 `current|next` slot。
3. SDK-first低层组合当前使用由`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`管理的官方0.7.0底座、唯一`third_party/polymarket_client_sdk_v2-0.7.0-p1`vendor，并继续保持唯一`ConnectionManager`/receiver/actor、sticky parser fault、CLOB interested-array all-or-error、`SimpleParser::Default`、generation隔离能力和RTDS有界脱敏诊断目标。版本、Cargo/lock、vendor路径与provenance以SDK070三件套及Stage1 artifact为authority；当前archive SHA-256为`ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a`，官方VCS为`222143d321eba97d5711a848265eb9aab3bc7ff4`。具体保留/删除/新增语义仍只以`demand/data-collect-source-availability-lifecycle-repair-demand.md`第4.7节最终获批逐项矩阵为authority，不以固定patch数量为合同；相对官方0.7.0的源码差异路径仍恰好限于`src/ws/connection.rs`、`src/ws/config.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`。历史0.6.0实施、B trace决定及阶段2/4接入事实不回写。
   项目层补充合同：该唯一 SDK receiver 必须由 fast-drain task 持有；每次 recv 成功后立即冻结 collector receive wall-clock 与 monotonic time、取得共享 row/byte permits，再按 FIFO 转交给唯一串行 processor。canonical JSON、dedupe、target filter、fan-out 与 ingress 都在 processor 执行，不能复制第二套业务语义；动态订阅等待、Gamma 请求、resubscribe 与 writer 背压不得阻止 fast-drain 持续排空 SDK broadcast。不得创建第二个 SDK receiver；decoded queue 满、预算等待超时或 SDK `Lagged(n)` 都必须显式 fail closed。
4. 单 receiver 可以解析全部 SDK 消息，但 `polymarket_market` 只能写入 current/next target：单 asset variant 必须精确匹配订阅 asset；`PriceChange` 只规范化其中匹配 target 的 source-native entries；`NewMarket`/`MarketResolved` 至少一个 `asset_ids` 匹配 target 才能写入 market/metadata。unmatched/global 消息不得写入目标数据集、不得刷新 freshness。
5. 官方SDK普通transport故障继续走唯一connection owner的自然重连；删除业务静默专用`request_reconnect()`及其runtime wrapper、生产调用和专属测试。只有`require_text_pong=true`的CLOB才以heartbeat timeout/旧PONG结束旧socket并进入既有recoverable loop；RTDS no-PONG不重连。最后一个manager/bound sender关闭必须normal teardown且不得建立新连接。不得创建第二个client、connection loop、receiver或subscription actor。
   lifecycle/订阅集合切换、UTC 日 barrier 与 SIGTERM/故障停机必须通过 fast-drain FIFO 的 source fence：先处理 marker 前全部 decoded message，再允许更新 processor 上下文、发送 barrier 或停止 source；停机不得直接 abort 并丢弃已经进入项目 decoded queue 的消息。
6. 若第4.7节批准矩阵仍不能满足单receiver、动态增删asset、transport自然重连、lag/parser fault/terminal state可见或manager drop后的socket/task收敛，必须停止并报告具体SDK缺口、影响和选项；不得扩大vendor路径、改用raw WS/REST或手写订阅。
7. Transport connected、Ping/Pong、unmatched/global event和其他asset的消息都不能替目标asset产生业务事实。collector按asset保留nullable first/last-qualifying receive time，但不设置30/60秒业务静默watchdog，不因静默unsubscribe/resubscribe、切generation、记录gap/lag/异常告警或退出。
8. 采集层只对“同 source、同 market lifecycle、SDK 解码后所有 source-native 字段完全一致”的 payload 做结构去重。每个 SDK 消息先生成稳定 canonical JSON：覆盖 variant、market/asset、平台时间及该 variant 的全部 SDK typed 字段，不含本地接收时间和派生 slug；该 JSON 作为 `source_native_payload_json` 落入 Parquet，并以其 SHA-256 作为内存去重 fingerprint。fingerprint 的 check/insert 必须在 target row fan-out 前按消息执行：首次出现的多 target `PriceChange` 必须保留全部匹配 rows，整条消息重放时才全部丢弃。这样 `spread`、`NewMarket`/`MarketResolved` 的文本、outcomes 与 event message 等当前被 typed layout 丢失的字段也能参与比较并由进程重启恢复；仅这些字段不同的两条消息必须保留。缓存跨网络 reconnect 保留，并完整覆盖最多 current+next 两个活跃 lifecycle，lifecycle 退役前禁止 TTL/capacity 淘汰。生产常量固定为每 lifecycle 最多 `2_000_000` 个 SHA-256 fingerprint、同时最多两个 lifecycle；达到上限必须记录 gap、coordinated finalize 后退出 76，禁止淘汰后静默放行重复。进程重启时必须用 Arrow bounded RecordBatch projection 流式读取最近 finalized Parquet 的 `slug/source_native_payload_json`，重算 fingerprint 并恢复 current/next 集合，不增加 sidecar，也不得把完整列一次性载入内存。`0.1/0.10` 只有在 SDK 无损解码后生成相同 canonical JSON 且其他字段完全一致时才可去重；更广泛的精度/业务等价由 data-pipeline 处理。

以上第3项项目层canonical/fan-out文字、第4项row grain、第8项去重/restart文字及
第3.4、第四节中的相关JSON/fingerprint预算，只约束source v2直至Stage3原子
activation，并在此后作为legacy只读事实保留。source v3先完成target admission，
再构造一次唯一canonical typed domain；每条通过准入的SDK message写一条nested
typed row，`PriceChange`只保留匹配entries且保持SDK相对顺序。v3不构造或持久化
`source_native_payload_json`和venue `hash`，不按payload equality丢弃platform
delivery，也不恢复跨进程fingerprint；相同platform delivery各自保留，内部retry
不得额外制造记录。既有全局row/byte、单行和decoded queue上限继续有效，但按domain
及其nested owned buffers计费，不再为canonical JSON或fan-out副本重复计费。

### 3.2 Binance

1. aggTrade 必须按 `aggregate_trade_id` 连续处理。同一 transport source 的同 ID 重放按该 source 的完整 native payload 比较：在进程内及恢复的最近 25,000 个 ID 窗口内，完全相同丢弃，不同则记录 conflict gap 并保留冲突事实；更老的 `id<=last` 无比较 payload 时必须保留该原始行并记录 `replay_outside_recovery_window`，交由 data-pipeline 全量判断，禁止静默丢弃或声称已验证相同。
2. 发现跳号时，必须使用 Binance 官方 `/api/v3/aggTrades?fromId=` 分页回补。请求上下文固定并校验为 `BTCUSDT`，响应只校验 ID、顺序、数量、边界和官方 payload 字段；typed row 的 symbol 来自请求上下文，因为 REST 响应没有 symbol。WS/REST 对同 ID 只按双方语义一致的官方字段 `a,p,q,f,l,T,m` 判断；同名 `M` 在 WS 表示 ignore、在 REST 表示 best-price-match，禁止纳入跨 transport 等价 key。
3. WS/REST 共有字段完全一致时只写一条：`ingest_source=websocket_rest_merged`，保留 WS 的 source-native `event_type/event_time/stream/symbol/ws_ignore`、REST 的 `rest_was_best_price_match`，并分别保留真实 `websocket_timestamp_received` 与 `rest_backfill_timestamp_received`；二者不是等义别名。`timestamp_received=max(websocket_timestamp_received,rest_backfill_timestamp_received)` 表示整条 merged row 所有字段均已可见的 availability，并据此选择 UTC hour/day 分区。共有字段不一致必须记录 conflict gap，并分别保留 `ingest_source=websocket|rest_backfill` 两条原始事实。
4. REST aggTrade 没有 WS 专属 `e/E/stream`。typed schema 中 `event_type/event_time/stream` 必须 nullable，REST-only 行三者为空，真实 `trade_time=T`，不得伪造；WS-only 行显式使用 `ingest_source=websocket`，merged 行只使用真实 WS 值。
5. REST-only 行的 `rest_backfill_timestamp_received` 与 `timestamp_received` 都是本机完整收到该回补响应的时间，`websocket_timestamp_received` 为空；WS-only 行则相反。不得把任何接收时间写成历史 `T` 或请求开始时间。
6. 回补期间后续 WS 行进入 25,000 行 bounded pending buffer，回补完成后按 ID 合并。429、超时、空响应、乱序、buffer overflow 或回补后仍不连续时按有界退避重试；仍未恢复则在日 manifest 留下未恢复 gap，继续接收未来数据，不伪报连续。
7. 网络重连时内存游标保持。游标固定为最新 continuity epoch 的 `epoch_start_id`、只由向前接受的 live/backfill 行单调推进的 `last_observed_id` 与该 epoch 的 `last_contiguous_id`；窗口外旧 replay 不得推进或回退任何游标。回补最终失败时，旧 epoch 以精确 unresolved range 结束，从下一条向前接受的 ID 开启新 epoch，历史 gap 保留但后续重启不再从该永久旧 gap 回补到当前。进程重启时优先从当日 open manifest 和最近 finalized aggTrade Parquet 尾部恢复；若当日尚无 manifest/Parquet，则回退到最近一个已 closed 的前一 UTC 日 manifest 与其 finalized aggTrade 尾部，再叠加当日已存在的 finalized tail。恢复最新 epoch 三个游标及最近 25,000 个 ID 的 source-native 比较字段后，只在接受第一条新 WS 行前回补最新 epoch 尾部 `(last_contiguous_id, first_ws_id)`；全新空数据根只记录明确的初始 coverage boundary。
8. depth 继续使用官方 WS/REST snapshot + update ID stitch。gap 必须 resnapshot；`snapshot_fetch_start_utc` 在请求发送前采样，`timestamp_received` 在完整响应收到后采样。

### 3.3 RTDS

1. 每条消息必须保存真实 `message_type`，并校验 `topic=crypto_prices_chainlink`、`message_type=update`、`symbol=btc/usd`、毫秒 payload timestamp 和 payload age；`update` 是 Polymarket 官方 RTDS 文档中 Chainlink price payload 的允许业务类型，SDK 订阅使用 `type=*` 不代表其他 type 可以恢复健康。以本机 receive time 计算 `age_ms=received-payload`；`-5_000 <= age_ms <= 30_000` 为 fresh，等于边界仍 fresh，超出范围为 stale。
2. 陈旧、未知type或不匹配topic/symbol的消息不得刷新freshness/last-qualifying。可解析原始行可以保留，但必须有typed `freshness_status=fresh|stale|invalid_type|invalid_topic|invalid_symbol|invalid_timestamp`供data-pipeline明确排除；持续无效消息、stale或没有新业务消息不得触发或影响watchdog、subscription、generation、gap、lag、告警或退出。
3. 使用 filter 的官方 RTDS 订阅会先下发历史 snapshot；本轮真实 SDK receiver 证据为 `topic=crypto_prices,type=subscribe,payload={symbol,data:[...]}`，它不是 Chainlink 单价 typed payload。此类非目标 envelope 的 Chainlink 单价 decode 失败必须在升级为 schema fatal 前归类为 ignored，不写伪造 typed 行、不刷新 freshness、不制造 gap，也不得令进程退出；但 `topic=crypto_prices_chainlink,type=update` 的 payload 缺字段仍是 schema fatal。price-shaped 的 wrong-topic/wrong-type 行继续按上一项保留 typed 无效状态。
4. 生产WebSocket配置按通道分离：CLOB使用`10s/20s/require_text_pong=true`，RTDS使用固定`5s/require_text_pong=false`；SDK默认`5s/15s/true`只用于兼容，RTDS结构中的15秒timeout不参与判死。RTDS只保留一个`ConnectionManager<RtdsMessage, SimpleParser>`、一个receiver与一个项目订阅actor；desired使用latest-value完整filtered subscription，每个真实新`Connected` generation从空server state只replay一次latest desired。业务静默、持续stale、snapshot、unmatched、duplicate或no-PONG不调用subscribe/unsubscribe、one-shot reconnect、改变generation或结束source；真实Close/EOF/read/write/PING-send等transport故障由SDK/source-local owner持续恢复，总attempt/elapsed无限。`Lagged(n)`、service/parser/schema fault继续按完整性合同处理。
5. 精确 typed payload 去重使用固定滚动 60 分钟窗口。窗口基准只使用该 key 第一次被接受时的 collector `timestamp_received`，重复命中不延长；年龄 `<60:00` 保留，达到 `60:00` 即可淘汰，禁止使用可陈旧、未来偏移或无效的 payload timestamp。生产常量固定为窗口内最多 `100_000` 个 key；窗口未到期而达到上限时记录 gap、coordinated finalize 后退出 76，禁止提前淘汰并放行重复。集合跨网络 reconnect；进程重启时从最近 finalized RTDS Parquet 以相同 receive-time 口径恢复窗口，不增加 manifest key 列表或 sidecar。超过 60 分钟的业务等价或重放由 data-pipeline 的全量语义去重处理。

### 3.4 writer 与错误传播

1. 所有项目自有、尚未进入 Parquet row group 的 data-bearing 缓冲必须共享编译常量 `GLOBAL_INFLIGHT_MAX_ROWS=100_000`、`GLOBAL_INFLIGHT_MAX_BYTES=134_217_728`、`MAX_TYPED_ROW_BYTES=1_048_576` 与 `BUFFER_ACQUIRE_TIMEOUT=2s`，覆盖 source ingress、source batch、writer command、Binance 25,000 行 pending 及其他待写 payload 容器。每个 fan-out 后待写 row 在入队前按结构本体及所有 owned `String/Vec` 实际 capacity（必须包含 `source_native_payload_json`）计算；超过 1 MiB 必须记录 gap并协调 exit 76，禁止进入 queue/writer。event 第一次进入时取得 row/byte permits，跨 ingress -> batch -> writer 只移动 permits，不重复计数；复制 owned payload 必须另计，内容移入 Arrow builder并释放原 event 后才归还。全局预算不足等待超过 2 秒也必须协调失败，禁止 drop 后继续。当前SDK070 `0.7.0-p1`内部outgoing/ping控制通道继承用户已批准的唯一unbounded类型例外：订阅 desired state 必须合并、断线期间不得反复 enqueue、heartbeat最多一个未完成ping，并以压力测试证明实际积压有限；版本升级不扩大该例外。SDK broadcast、网络栈及固定小容量 manifest 控制消息不占上述 data budget，项目数据不得进入例外。channel closed、parser/schema/finalize/atomic rename 失败必须返回错误并使进程非零退出。
   Polymarket decoded queue 也属于上述项目 data-bearing 路径：固定附加上限为 `25_000 messages/67_108_864 bytes`，但每条消息仍必须同时按 1 row 加 enum/variant 本体及全部 owned `String/Vec` capacity 持有同一全局 permits；附加上限不能代替全局预算。原 decoded message permits 保留到 canonical/dedupe/fan-out 完成；canonical JSON 与 fan-out row 等同时存在的 owned copy 分别计 byte permits，从而为 fan-out 保留至少一半全局 byte 预算，避免 decoded queue 独占 128 MiB 后自锁。
2. 每个网络source的connect/HTTP/read/backoff单次等待有界，使用capped exponential backoff+jitter；source-local外层总attempt与总elapsed无限，普通外部availability不得结束source或collector。runtime writer、SDK lag、parser/schema和manifest/路径等完整性错误保持原受影响scope的协调失败，不由systemd重启冒充source恢复。
3. 未完整close的`*.running`不是训练数据；同日恢复时只清理已证明无进程持有的旧running，并通过`collection_day_coverage`的同日四态恢复与sticky `reactivated_within_day`事实保留共同观察资格边界，不把普通transport availability伪造成gap。
4. 收到 SIGTERM 后必须停止接受新 source 事件、按顺序 drain ingress/batch/writer、finalize 当前 Parquet、由 manifest owner 更新仍为 open 的当日 manifest、写入一条 `coordinated_shutdown_complete` journal，并在应用内部 90 秒截止前退出 0；systemd `TimeoutStopSec=120` 只提供外层余量。对 operator/systemd SIGTERM 停服而言，超时、SIGKILL、非零 ExecMainStatus 或残留 running/tmp 都不能算优雅停止；明确的 fault/capacity 路径可以在完成同一 drain/finalize 后返回其非零退出码，但仍必须被 systemd 记为失败或受控停止。
5. 数据根达到 50GB 时必须复用同一 coordinated shutdown，完成 drain/finalize/open-manifest 更新且无 running/tmp 后再退出 75；journal 必须区分 `dataset_capacity_reached`，systemd 不得重启。
6. 不可恢复的 CLI/config、路径 containment、prior-day manifest/Parquet 对账和 active-lifecycle dedupe hard-bound 错误使用保留退出码 76 `operator_intervention_required`；运行中触发时先 coordinated finalize，启动前触发时不得创建生产数据。systemd 对 75/76 都不得重启，其他进程级失败仍受明确 StartLimit 约束。

## 四、schema 去冗余边界

1. 普通 source row 只保留一个 collector receive time `timestamp_received`；删除等义的 `metadata_recv_time_utc` 等别名。Binance aggTrade merged row 必须额外保留语义不同的 `websocket_timestamp_received` 与 `rest_backfill_timestamp_received`，并以二者最大值作为 `timestamp_received` availability；不得把这三个字段当成等义冗余。
   Polymarket market row 的 `timestamp_received` 必须是唯一 SDK receiver 成功取得 typed message 后立即冻结的 wall-clock，不得改写为 decoded queue 下游开始 canonical/fan-out 的时间；同刻 monotonic time只在内存中供 freshness 使用，早于该 asset `subscribed_at` 的排队消息不得刷新健康。
2. Polymarket metadata 只保留 `asset_ids`；删除内容恒等的 `token_ids`。Gamma fetch-start 必须是真实请求开始时间；原生 WS metadata 的 HTTP fetch-start 必须为空。
3. Binance depth update 保留官方 `U/u` 与重建所需 `local_u_before`；删除恒等于已应用 `u` 的 `local_u_after`。
4. `event_time` 与 `trade_time`、RTDS message timestamp 与 payload timestamp、snapshot fetch-start 与 receive time 语义不同，必须保留，不能因为部分样本恰好相同而合并。
5. data-collect 只做 source-native ID 去重、同一 `source_kind` 内已明确定义的 WS/REST overlap，以及 SDK typed source-native 字段完全相同的结构去重。跨 `source_kind`、跨 schema 或需要业务推断的精度等价仍由 data-pipeline 完成，不能在 typed raw 中猜测性删行。

## 五、最小生产产物合同

生产数据根固定为：

```text
~/btc-updown-marketmaker/datasets/training/typed_raw/
└── utc_day=YYYY-MM-DD/
    ├── manifest.json
    └── source_kind=<source>/
        ├── <source>_HH.parquet
        └── <source>_HH-part-N.parquet
```

1. 正常情况每 source/hour 一个 finalized Parquet；只有同一小时进程重启导致文件名碰撞时才使用 `part-N`。稀疏 snapshot source 在某小时存在行时，必须由 source-ordered UTC-hour watermark 在边界后关闭该小时主文件，即使此后没有下一条 snapshot；该小时没有行时不得生成空文件。同一进程内的正常小时关闭不得使用 `part-N`。
2. writer 用编译常量 `PARQUET_ROW_GROUP_MAX_ROWS=50_000` 与 `PARQUET_ROW_GROUP_MAX_BYTES=16_777_216` 控制每个 writer 的逻辑 row group；利用入队前已知且不超过 1 MiB 的 row size，在加入会越界时先结束当前 row group，因此 group 逻辑 payload 不得超过 16 MiB，达到上限不创建新 part 文件。恢复扫描只投影所需列，固定 `RECOVERY_BATCH_MAX_ROWS=16`；新数据根中的每行都已受 1 MiB hard cap，故 projected logical payload 最多 16 MiB。若恢复读取发现单行超过 1 MiB，必须在网络 ingress 前 exit 76；每批处理后立即释放，不得一次载入完整列。上述边界不增加产物，仍不因 300k rows 或 128MB 产生大量小时 part 文件。
3. 写入时只允许同目录 `*.running` 临时文件；持久化顺序固定为写完并 fsync 临时文件、atomic rename、fsync 父目录。`manifest.json.tmp` 使用相同顺序且只能瞬时存在。
4. 每个UTC日只允许一个顶层`data_collect_daily_manifest_v3`且只有一个ManifestOwner有写权限。它保留既有build/file/六source counters/continuity/confirmed gaps/operational facts，并直接嵌套一个必填`data_collect_collection_day_coverage_v1`：`not_started|active|ended_known|unclean_end_unknown`、三个nullable `i64` UTC毫秒时间及必填布尔值`reactivated_within_day`。对象不复制进source counters，不保存transport scope/reason/episode，不新增extensions/receipt/sidecar；legacy v3缺对象只读为unknown，新writer缺失或malformed fail closed。
5. UTC日关闭由四个mandatory logical producer、六writer和共同ingress的owner-owned day/generation gate协调：关闭旧raw permit、drain、收齐producer barrier与writer finalize ack后，原子提交旧nested end和顶层closed；随后才可原子激活新日并在ack后释放网络/raw。普通外部断线/backoff或业务静默不写gap。可靠close候选first-close-wins，但共同fence前的非协调panic/abort/join failure执行unclean-dominates；PendingExit与两个持久化切点按新专项第4.2节执行。
6. 任何current-day读取/恢复前先取得并持续持有dataset-root级OS-backed独占实例lease。current-day四态按新专项第4.2节激活：首次activation写`reactivated_within_day=false`，同一UTC日后续activation写`true`且当日保持单调；unclean、known end及prior-day reconciliation均保留该值。prior open manifest按state-aware规则关闭：active/既有unclean才使用unclean closure，not-started/ended-known/legacy missing中性关闭且不伪造gap。current closed、identity mismatch、新writer缺coverage或非法nested均fail closed；所有ManifestOwner mutation保留coverage对象。
7. 删除并禁止重新生成 per-segment manifest、run manifest、startup/retention/resource/backpressure receipt、coverage/gap/reconnect/rollover sidecar、逐事件 debug/queue samples、accepted/rejected report 和独立 validation report。详细运行错误只进入 journald；不记录逐事件日志。
8. data-pipeline 只消费 `state=closed` 日期；它自行做内容校验、语义去重、排序、market acceptance/rejection 与训练准入。
9. collector 不再自动删除 raw，也不生成 retention receipt。数据根达到 50GB 时按 3.4 的 coordinated shutdown 合同以保留退出码 75 fail closed，等待独立归档运维清理，禁止静默填满磁盘。

## 六、生产边界（历史条款已被直接部署事项取代）

1. 本文件原同日D2 running-upgrade、当日删除、立即启动、gate与26小时长跑步骤不再是可执行authority；历史批准、未执行状态和远端只读事实保留在原changelog。
2. 2026-07-18首次正式冷启动已经由`demand/data-collect-training-collector-direct-deployment-demand.md`独立批准：多日停服、旧PID/lease退出且当前UTC日无任何collector产物时，任意时刻直接启用唯一`Restart=on-failure` unit。
3. 旧UTC边界首次启用编排不再可执行；future coverage-aware升级/回滚、same-day binary/version/identity takeover、identity migration或旧binary接管open v1日仍须基于当时事实另行批准。
4. 本旧专项自身不授权生产构建、部署、远端服务操作或数据清理。相同identity持有dataset独占lease的同日进程恢复属于collector状态机，不是部署takeover。
## 七、当前验收authority

1. 本文件已完成的分源heartbeat配置与既有parser、dedupe、continuity、writer、schema和资源完整性测试继续作为保持回归。
2. 业务静默、transport持续恢复、SDK heartbeat/teardown、one-shot删除、CLOB/Gamma、Binance、daily coverage、资源100-cycle与全量门禁，以`demand/data-collect-source-availability-lifecycle-repair-demand.md`第六节28项验收为唯一当前authority。
3. 每个新writer必须写顶层v3直接嵌套v1 coverage；legacy missing=unknown，新writer missing/malformed fail closed，所有ManifestOwner mutation、同日四态恢复、prior state-aware closure、rollover/PendingExit/abort因果证明均须按新专项测试。
4. 本文件不授权生产操作；当前首次正式冷启动只由独立直接部署三件套授权。
