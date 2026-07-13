# 数据采集可靠性修复需求

> 事项编号：`DC-REPAIR-20260713-COLLECTOR-RELIABILITY`
>
> 对应计划：`plan/data-collect-reliability-repair-plan.md`
>
> 对应 changelog：`changelog/data-collect-reliability-repair-changelog.md`
>
> 状态：`REVIEW_PASSED`。第六次修订已通过三路独立审查，可以进入采集代码实现；实现复审通过前不得停止远端服务或删除、部署远端程序与数据。

## 一、目标与权威边界

本事项只修复训练冷路径的 `data-collect-training-collector`，使它能够长期、连续地采集 Polymarket、Binance 与 RTDS 原始市场数据，并把生产数据目录收敛到最少且明确的必要产物。

本需求是用户 2026-07-13 指令形成的聚焦增量。它只在“数据采集服务器上的训练 collector”范围内取代 `demand/data-collect-demand.md` 下列旧合同；未列出的 source 字段、SDK-first、训练/paper/live 独立运行角色与 data-pipeline 边界继续有效：

- 第 11 行的训练 service `Restart=always`，改为 `Restart=on-failure`，为受控退出码 75/76 禁止重启，并增加明确 StartLimit；
- 第 49、51、128、132-135、163、189-191、217、219、222、225、227、229、231、236 行的 per-segment/run/receipt、complete/PASS、ResourceWatermark、重复 reconnect/coverage/queue 证据合同，改为每 UTC 日唯一事实 manifest 与 journald；
- 第 129、137、237 行的 row/byte protective file rollover，改为只按 UTC hour 与同小时进程重启分 part，row-group 上限只约束内存；
- 第 141-143、243 行的 collector 自动 retention/delete/receipt 合同，改为 50GB 时退出 75、由独立归档运维处理；
- 第 167-169、226 行的恢复合同改为两层：单次连接恢复周期仍使用有限 attempts/max elapsed/capped exponential backoff+jitter；训练 source 外层 supervisor 可无限期启动新的有限恢复周期，禁止 tight loop；
- 第 228 行的“所有队列 bounded”在项目自有 data-bearing 路径继续严格成立，并升级为全进程 row+byte 双上限；用户 2026-07-14 明确批准官方 SDK 0.6.0 内部 outgoing/ping 控制通道作为唯一类型例外，但必须维持有限生产者不变量，禁止把项目数据放入该例外；
- 第 229 行的四源合并 `2k-5k events/s` 处理能力继续有效；仅其独立 profiler/ResourceWatermark/validation artifact 被取代。吞吐使用自动删除临时目录中的既有 benchmark/fixture 验证，汇总结论只写 changelog；`5k events/s` 同时只是容量 sizing 上界，不代表平台实际持续速率；
- 第 110、140、220、241 行关于 Binance aggTrade `e/E/s` 与 `event_time=E` 的非空要求继续约束 WebSocket 行；本事项新增的 REST-only 行允许 WS 专属 `event_type/event_time/stream` 为空，merged 行按本需求 3.2 的字段与 availability 规则执行；
- 第 235 行的三类独立 binary 要求继续有效；本事项的“只部署一个训练 binary”只约束数据采集服务器本次 release，不取消未来 paper/live 在各自服务器上的独立 binary。
- `demand/rebuild-core-demand.md` 数据与证据要求第 10 项中原有的 `data-collect` per-run manifest、validation verdict 与 `required_files` gate 被本事项取代：collector 每 UTC 日只维护一个事实 manifest；内容校验、语义去重和训练准入由 `data-pipeline` 完成。training、offline-evaluation、paper、live 与 sim-calibration 的 run manifest 合同不变。

`plan/data-collect-reliability-repair-plan.md` 在相同范围内取代 `plan/data-collect-plan.md` 与 `plan/data-collect-pmxt-hourly-retention-plan.md` 的旧生产产物、release、retention 和远端切换步骤，但不改写其中历史执行记录。原 changelog 仍保留为历史事实。

本事项不实现 `data-pipeline` 的 canonical、语义去重、feature、label、split 或训练准入，不实现 paper/live 采集二进制，也不重建每日拉取、打包和阿里云盘上传脚本。归档脚本应在新采集器稳定后作为独立事项处理。

## 二、已核实问题与责任判断

1. 2026-07-11、2026-07-12 的拉取文件与远端对应文件一致，归档传输不是这两日重复和缺口的制造者。
2. Polymarket 两日存在大量完全相同的业务 payload 重复行；当前证据不能把原始重复唯一归因于平台或 SDK，但采集器没有保守的传输级去重，必须防止重复写入。
3. Polymarket discovery 当前把 current 与 next 绑定成全有或全无；next 暂时不存在时会放弃有效 current，这是本程序策略错误。
4. 当前程序把 synthetic `market_discovery_switch` 伪装成 `new_market`，同时又保存原生 WS `new_market`，这是本程序语义与重复路径错误。
5. 官方 Polymarket Rust SDK 0.6.0 的 CLOB 与 RTDS 高层 typed streams 都会隐藏 broadcast lag；当前程序无法把 `Lagged(n)` 作为可恢复故障处理。这是 SDK 高层接口限制叠加当前接入方式造成的缺口。
6. Binance aggTrade 在 2026-07-12 缺少连续 aggregate trade ID。官方 REST 能确认缺失成交存在，说明丢失发生在平台到本地落盘之间；当前程序没有 ID 连续性检测、重连后续接或 REST 回补，这是本程序韧性缺口。
7. RTDS 出现接收时间很新但 payload timestamp 已陈旧的消息；陈旧推送可能来自平台或网络重放，但当前程序会把任意成功消息当作 freshness 恢复，这是本程序判断错误。
8. metadata 与 Binance snapshot 的 fetch-start 被 receive time 回填，`metadata_recv_time_utc`/`timestamp_received`、`token_ids`/`asset_ids`、`local_u_after`/`u` 等还存在等义字段，这是本程序 schema 错误或冗余。
9. bounded queue 超时、writer channel 关闭及部分 writer 错误会被记录后继续运行，可能静默丢行；segment manifest 又硬编码 `Complete`，这是本程序错误。
10. 远端当前仍在运行旧采集器并写入约 14GB 旧数据。旧数据中 2026-07-06、07、08、10 和正在写入的 13 日没有完整本地镜像；用户已经明确授权在实现复审通过后清除旧程序与旧数据，因此该清除是不可逆删除，不额外制造备份包或验证报告，本地既有备份不得删除。

## 三、采集正确性需求

### 3.1 Polymarket

1. market discovery 必须以有效 current 为最低条件；next 独立发现、独立预订阅，暂时不存在不得阻断 current。切窗时已有 next 晋升为 current，过期资产才退订。
2. 只允许官方 SDK 原生 `WsMessage::NewMarket` 生成 `new_market`。Gamma discovery 只能生成 `market_discovery`/`active_scan`，并保留 `current|next` slot。
3. 必须优先组合官方 Rust SDK 公共的 `ConnectionManager`、`SubscriptionManager`、`InterestTracker` 与单一 `WsMessage` receiver，使 `Lagged(n)` 对采集器可见，并同时监听 `state_receiver()` 与获批 patch 的 sticky `fault_receiver()`；有限 retry 结束进入 terminal `Disconnected` 时必须结束本轮 source recovery，而不能永久阻塞在 broadcast receiver。不得使用 raw tungstenite、手写 Polymarket subscription 或 raw REST 旁路。唯一获批例外是把官方 `polymarket_client_sdk_v2 0.6.0` 原始 crate 固定为 `third_party/polymarket_client_sdk_v2-0.6.0-p1/`，保留许可证与来源，并通过 workspace-root `[patch.crates-io]` 唯一引用；原始 `.crate` SHA-256 为 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`、官方 VCS commit 为 `3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`。实质 patch 只增加结构化 parser error 出口，并把 CLOB 数组中 interested invalid element 的静默跳过改成整帧解析失败；不得借此改变认证、签名、请求、订阅或平台语义。
4. 单 receiver 可以解析全部 SDK 消息，但 `polymarket_market` 只能写入 current/next target：单 asset variant 必须精确匹配订阅 asset；`PriceChange` 只规范化其中匹配 target 的 source-native entries；`NewMarket`/`MarketResolved` 至少一个 `asset_ids` 匹配 target 才能写入 market/metadata。unmatched/global 消息不得写入目标数据集、不得刷新 freshness。
5. 官方 SDK 0.6.0 没有公开主动 close/force-reconnect。普通 transport 断线使用 SDK 自动重连；业务静默先在同一官方 `SubscriptionManager` 上做有限退订/重订阅，仍未恢复或发生 `Lagged(n)` 时必须协调 drain 后让进程非零退出，由 systemd 启动全新进程，禁止反复创建无法终止的 orphan SDK connection/task。
6. 若官方公共组件加上述获批最小 patch 后仍不能维持单 receiver、动态增删 asset、transport 自动重连、lag/parser error/terminal disconnected 可见或进程退出后的 socket/task 释放，必须停止实现并向用户报告新增 SDK 缺口、影响和选项；不得自行扩大 patch 或改变 SDK-first 路线。
7. transport connected、Ping/Pong、unmatched/global event 和其他 asset 的消息都不能替目标 asset 恢复健康。每个已订阅 market/asset 独立维护 first-business-event 与 freshness：订阅后 30 秒内必须收到首个匹配业务事件，此后连续 60 秒无匹配事件必须记录 gap 并恢复；next 尚不存在不影响 current，next 一旦订阅就独立检查。
8. 采集层只对“同 source、同 market lifecycle、SDK 解码后所有 source-native 字段完全一致”的 payload 做结构去重。每个 SDK 消息先生成稳定 canonical JSON：覆盖 variant、market/asset、平台时间及该 variant 的全部 SDK typed 字段，不含本地接收时间和派生 slug；该 JSON 作为 `source_native_payload_json` 落入 Parquet，并以其 SHA-256 作为内存去重 fingerprint。fingerprint 的 check/insert 必须在 target row fan-out 前按消息执行：首次出现的多 target `PriceChange` 必须保留全部匹配 rows，整条消息重放时才全部丢弃。这样 `spread`、`NewMarket`/`MarketResolved` 的文本、outcomes 与 event message 等当前被 typed layout 丢失的字段也能参与比较并由进程重启恢复；仅这些字段不同的两条消息必须保留。缓存跨网络 reconnect 保留，并完整覆盖最多 current+next 两个活跃 lifecycle，lifecycle 退役前禁止 TTL/capacity 淘汰。生产常量固定为每 lifecycle 最多 `2_000_000` 个 SHA-256 fingerprint、同时最多两个 lifecycle；达到上限必须记录 gap、coordinated finalize 后退出 76，禁止淘汰后静默放行重复。进程重启时必须用 Arrow bounded RecordBatch projection 流式读取最近 finalized Parquet 的 `slug/source_native_payload_json`，重算 fingerprint 并恢复 current/next 集合，不增加 sidecar，也不得把完整列一次性载入内存。`0.1/0.10` 只有在 SDK 无损解码后生成相同 canonical JSON 且其他字段完全一致时才可去重；更广泛的精度/业务等价由 data-pipeline 处理。

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
2. 陈旧、未知 type 或不匹配 topic/symbol 的消息不得刷新 freshness；持续无效消息不能饿死 watchdog。可解析原始行可以保留，但必须有 typed `freshness_status=fresh|stale|invalid_type|invalid_topic|invalid_symbol|invalid_timestamp`，data-pipeline 才能明确排除。
3. RTDS 必须像 CLOB market channel 一样组合官方 SDK 公共的 `ConnectionManager<RtdsMessage, SimpleParser>`、RTDS `SubscriptionManager`、单一 broadcast receiver、`state_receiver()` 与 sticky `fault_receiver()`，禁止继续使用会吞掉 `Lagged(n)` 的高层 filtered stream。健康期间不做固定 3 秒周期重发；60 秒无 fresh matching payload 后，最多重发同一官方 subscription 两次、间隔 3 秒，第二次后再等 3 秒仍未恢复就协调退出进程，由 systemd 新进程重建 SDK client。这一合同明确取代旧需求第 226 行“每 3 秒持续重发/每 2 次在进程内重建 client”的行为。发生 lag 或 terminal disconnected 也协调退出；parser fault 必须优先分类为 schema fatal、不得当作普通 transport 重连。恢复只由新的、匹配的业务 payload 证明；若固定 patch 后公共组件仍不成立，进入 3.1 第 6 项同一 SDK 决策门。
4. 精确 typed payload 去重使用固定滚动 60 分钟窗口。窗口基准只使用该 key 第一次被接受时的 collector `timestamp_received`，重复命中不延长；年龄 `<60:00` 保留，达到 `60:00` 即可淘汰，禁止使用可陈旧、未来偏移或无效的 payload timestamp。生产常量固定为窗口内最多 `100_000` 个 key；窗口未到期而达到上限时记录 gap、coordinated finalize 后退出 76，禁止提前淘汰并放行重复。集合跨网络 reconnect；进程重启时从最近 finalized RTDS Parquet 以相同 receive-time 口径恢复窗口，不增加 manifest key 列表或 sidecar。超过 60 分钟的业务等价或重放由 data-pipeline 的全量语义去重处理。

### 3.4 writer 与错误传播

1. 所有项目自有、尚未进入 Parquet row group 的 data-bearing 缓冲必须共享编译常量 `GLOBAL_INFLIGHT_MAX_ROWS=100_000`、`GLOBAL_INFLIGHT_MAX_BYTES=134_217_728`、`MAX_TYPED_ROW_BYTES=1_048_576` 与 `BUFFER_ACQUIRE_TIMEOUT=2s`，覆盖 source ingress、source batch、writer command、Binance 25,000 行 pending 及其他待写 payload 容器。每个 fan-out 后待写 row 在入队前按结构本体及所有 owned `String/Vec` 实际 capacity（必须包含 `source_native_payload_json`）计算；超过 1 MiB 必须记录 gap并协调 exit 76，禁止进入 queue/writer。event 第一次进入时取得 row/byte permits，跨 ingress -> batch -> writer 只移动 permits，不重复计数；复制 owned payload 必须另计，内容移入 Arrow builder并释放原 event 后才归还。全局预算不足等待超过 2 秒也必须协调失败，禁止 drop 后继续。SDK 0.6.0 内部 outgoing/ping 控制通道是用户已批准的唯一 unbounded 类型例外：订阅 desired state 必须合并、断线期间不得反复 enqueue、heartbeat 最多一个未完成 ping，并以压力测试证明实际积压有限；SDK broadcast、网络栈及固定小容量 manifest 控制消息不占上述 data budget，项目数据不得进入例外。channel closed、parser/schema/finalize/atomic rename 失败必须返回错误并使进程非零退出。
2. 每个网络 source 使用两层恢复：单次连接周期只允许有限 attempts/max elapsed，并使用 capped exponential backoff + jitter；独立外层 supervisor 可无限期启动新的有限恢复周期，禁止 tight loop。普通网络故障不得永久结束 source；runtime writer 错误和 Polymarket SDK lag 由协调退出后让 systemd 在 StartLimit 内重启整个进程，不可恢复的 startup/path/manifest/dedupe 不变量按第 6 项退出 76，不得重启。
3. 未完整 close 的 `*.running` 不是训练数据；同日重启时删除无进程持有的旧 running 文件，并把对应时间段记为 gap，不长期保留 partial 验证产物。
4. 收到 SIGTERM 后必须停止接受新 source 事件、按顺序 drain ingress/batch/writer、finalize 当前 Parquet、由 manifest owner 更新仍为 open 的当日 manifest、写入一条 `coordinated_shutdown_complete` journal，并在应用内部 90 秒截止前退出 0；systemd `TimeoutStopSec=120` 只提供外层余量。对 operator/systemd SIGTERM 停服而言，超时、SIGKILL、非零 ExecMainStatus 或残留 running/tmp 都不能算优雅停止；明确的 fault/capacity 路径可以在完成同一 drain/finalize 后返回其非零退出码，但仍必须被 systemd 记为失败或受控停止。
5. 数据根达到 50GB 时必须复用同一 coordinated shutdown，完成 drain/finalize/open-manifest 更新且无 running/tmp 后再退出 75；journal 必须区分 `dataset_capacity_reached`，systemd 不得重启。
6. 不可恢复的 CLI/config、路径 containment、prior-day manifest/Parquet 对账和 active-lifecycle dedupe hard-bound 错误使用保留退出码 76 `operator_intervention_required`；运行中触发时先 coordinated finalize，启动前触发时不得创建生产数据。systemd 对 75/76 都不得重启，其他进程级失败仍受明确 StartLimit 约束。

## 四、schema 去冗余边界

1. 普通 source row 只保留一个 collector receive time `timestamp_received`；删除等义的 `metadata_recv_time_utc` 等别名。Binance aggTrade merged row 必须额外保留语义不同的 `websocket_timestamp_received` 与 `rest_backfill_timestamp_received`，并以二者最大值作为 `timestamp_received` availability；不得把这三个字段当成等义冗余。
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

1. 正常情况每 source/hour 一个 finalized Parquet；只有同一小时进程重启导致文件名碰撞时才使用 `part-N`。稀疏 snapshot source 不生成空文件。
2. writer 用编译常量 `PARQUET_ROW_GROUP_MAX_ROWS=50_000` 与 `PARQUET_ROW_GROUP_MAX_BYTES=16_777_216` 控制每个 writer 的逻辑 row group；利用入队前已知且不超过 1 MiB 的 row size，在加入会越界时先结束当前 row group，因此 group 逻辑 payload 不得超过 16 MiB，达到上限不创建新 part 文件。恢复扫描只投影所需列，固定 `RECOVERY_BATCH_MAX_ROWS=16`；新数据根中的每行都已受 1 MiB hard cap，故 projected logical payload 最多 16 MiB。若恢复读取发现单行超过 1 MiB，必须在网络 ingress 前 exit 76；每批处理后立即释放，不得一次载入完整列。上述边界不增加产物，仍不因 300k rows 或 128MB 产生大量小时 part 文件。
3. 写入时只允许同目录 `*.running` 临时文件；持久化顺序固定为写完并 fsync 临时文件、atomic rename、fsync 父目录。`manifest.json.tmp` 使用相同顺序且只能瞬时存在。
4. 每个 UTC 日只允许一个 `manifest.json`，且只有一个 manifest owner task 有写权限。各 source writer 通过 bounded channel 提交 finalized-file delta；owner 串行合并、fsync、atomic rename。它只记录采集事实：schema 版本、UTC 日、`open|closed`、build/binary/config digest/host/region、文件路径/source/hour/part/rows/bytes/sha256、逐 source 首末接收时间、写入/结构去重/backfill/lag/reconnect/drop 计数、紧凑 gap 列表，以及 Binance 最新 `epoch_start_id/last_observed_id/last_contiguous_id`。`config_digest` 只覆盖规范化的非 secret CLI/env/subscription 配置。
5. UTC 日关闭使用与业务事件相同 ingress queue 的 per-producer day watermark/barrier。barrier 必须由四个稳定 logical producer 的外层 supervisor 发送，独立于 inner connection 状态；source 正在断线、backoff 或没有业务事件也必须在全部已产生旧日事件之后发送一次 barrier，同时把断线事实记为 gap，barrier 不得伪造 coverage。只有四个 producer 的旧日事件均位于各自 barrier 之前、主 queue/batch 已 drain、六个 writer 都返回 finalize ack 后，owner 才可原子写 `state=closed`；跨日 gap 在两个日期分别记录重叠部分。异常退出或日内 SIGTERM 只能更新 open manifest，不得伪造 closed；closed 后不得再出现旧日行。
6. 重启时 owner 合并既有 open manifest 与磁盘 finalized Parquet，恢复 build/file/sequence 事实；不得覆盖既有 finalized 文件。若 open manifest 的 UTC 日早于当前 receive day，启动流程必须在接受任何网络 ingress 前确认旧进程不存在、删除 stale running/tmp 并记录保守 coverage gap、以 finalized Parquet 重建文件事实，然后以 `closure_reason=unclean_exit_recovery` 原子关闭旧日；对账失败必须 fail closed。当前 UTC 日仍保持 open。manifest 不得包含训练 `PASS/FAIL`、validation verdict、required-files gate、ResourceWatermark 大字段或未测占位值。
7. 删除并禁止重新生成 per-segment manifest、run manifest、startup/retention/resource/backpressure receipt、coverage/gap/reconnect/rollover sidecar、逐事件 debug/queue samples、accepted/rejected report 和独立 validation report。详细运行错误只进入 journald；不记录逐事件日志。
8. data-pipeline 只消费 `state=closed` 日期；它自行做内容校验、语义去重、排序、market acceptance/rejection 与训练准入。
9. collector 不再自动删除 raw，也不生成 retention receipt。数据根达到 50GB 时按 3.4 的 coordinated shutdown 合同以保留退出码 75 fail closed，等待独立归档运维清理，禁止静默填满磁盘。

## 六、简单构建与部署合同

1. 本地只构建一个训练二进制：`target/release/data-collect-training-collector`；仓库内唯一 unit 权威源固定为 `data-collect/deploy/systemd/data-collect-training-collector.service`。production binary 必须从已 push 实现 commit 的临时 detached、clean worktree 构建，设置 `DATA_COLLECT_GIT_COMMIT=<完整 commit>`，使用该 worktree 独立 target 执行 `cargo build --release --locked -p data-collect --bin data-collect-training-collector`；构建后再次确认 HEAD 未变化且 worktree clean，在隔离产物上计算 SHA，再经本地 digest-named staging 原子放到上述固定路径。不得复用主仓库 target cache 作为 production 构建输入，最后删除临时 worktree/target。
2. 远端只部署一个二进制：`~/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector`。
3. 数据采集服务器只使用一个 unit：`~/.config/systemd/user/data-collect-training-collector.service`。repo-tracked 部署源和服务器都不得保留训练 release bundle/export 脚本、binary 别名、`current` symlink、独立 gate unit、retention-admin 或 release receipts；未来 paper/live 的源码 binary 与独立运行角色合同不变，各角色可直接用自己的单 `--bin` cargo 命令构建，但不得再被训练部署路径捆绑导出。
4. 上传只使用带本轮 SHA 的瞬时独占名：binary 为 `data-collect-training-collector.upload-<sha256>`，unit 为仍以 `.service` 结尾的 `data-collect-training-collector.staging-<sha256>.service`。创建前必须对 staging path 及逐级父目录做不跟随 symlink 的 `lstat`、owner/type/containment 检查，staging path 必须不存在并以 exclusive-create 语义创建；核对本地/远端 SHA-256 后 atomic rename。SHA 与 commit 记录在 changelog，不生成部署 receipt 或 README 副本。
5. 远端禁止编译；所有 Rust build/test 均在本地仓库完成。
6. 本地既存 ignored 用户产物 `data-collect/releases/train-data-collect-ver1-0/`（约 81MB）及用于保持 ignored 的 `data-collect/releases/.gitignore` 不在本次远端清理授权内，必须原样保留、不得 stage、不得作为构建输入或上传来源；它们不是权威 release path。删除它们需要用户另行授权。
7. 新 binary 必须支持无网络、无数据写入副作用的 `--version`/`--help`。停旧服务前必须在远端直接执行并退出 0，同时检查动态 loader/依赖无 `not found` 且所需 GLIBC 不高于远端；不通过不得停旧服务。
8. 新 binary 与 gate unit 在阶段 4 全部预检通过前只能停留在 digest-named staging 路径，不得修改 final binary/unit。旧主服务尚未停止前任何 abort 只删除本轮 staging；空的本轮新建 `bin/` 才可 `rmdir`，不得触碰旧 release、旧数据或旧 unit。
9. 首次切换要求最终新 binary 路径不存在。失败重试只接受 changelog 已完整记录且当前 service inactive、MainPID=0、无句柄的 `failed_gate`、`failed_long_running` 或 `partial_replace` 状态；实际 old/new binary SHA、old/new unit digest 与 Restart 策略必须逐项匹配记录，随后才可 `reset-failed`、预置新 staging 并跳过旧对象删除。任何身份不符必须停止请求用户决策。阶段 4 全部门通过且服务 inactive 后才依次原子替换 final binary 与 unit；两次替换间中断必须记录实际组合并按 `partial_replace` 恢复，不得猜测回滚。
10. 停止旧主 unit 前必须重新核验当下 `FragmentPath`、unit 文件类型/owner/digest、effective `ExecStart`、MainPID/cgroup、`/proc/<pid>/exe` 和 binary SHA。MainPID 可以因旧服务既有重启而变化，但其 unit、ExecStart、cgroup 与 executable 身份必须仍是已批准的旧 collector；任一不符不得停止或覆盖。

## 七、验收标准

1. 本地合同和故障注入测试覆盖 current-only discovery、next 延迟出现、CLOB/RTDS SDK lag、whole-frame parser error、CLOB partial-array invalid element 与 terminal disconnected 可见、target/unmatched WS 分流、首次多 target 消息完整 fan-out/整条重放删除、业务静默、PM lifecycle 与 RTDS 60 分钟窗口的跨网络/跨进程精确去重和 hard bound、仅被旧 typed layout 丢弃字段不同的 PM payload、aggTrade continuity epoch/25,000-ID 跨进程同源重放与冲突/WS-REST overlap/分页回补/失败、WS/REST 双 ingress 时间、当日无文件时从最近 closed 日恢复、RTDS wrong-type/陈旧消息、真实 fetch-start、1 MiB 单 row/全局 row+byte/16 MiB row-group 与 16-row recovery 边界、writer 背压与失败传播、SDK 内部控制通道有限生产压力、断线/backoff 跨午夜 barrier、UTC 日关闭、`23:59 crash -> 00:01 recovery-close`、exit 75/76 和重启碰撞；测试只使用自动删除的临时目录，不落盘测试报告。
2. workspace 与 excluded vendored SDK 各自的 fmt、全量/必要 lib tests 和 clippy `-D warnings`、SDK patch contract tests、`cargo tree -p data-collect -i polymarket_client_sdk_v2`、原始 crate allowlist diff、自动删除临时目录中的四源 `2k-5k events/s` benchmark 与 `git diff --check` 全部通过；dependency tree 只能出现仓库内 p1 实例，汇总只写 changelog。
3. 独立 `gpt-5.6-sol`/`ultra` 实现审查通过后，才允许停止并永久删除远端旧程序与旧数据；删除严格限制在计划的逐项绝对白名单内。停服前必须一次性核对全部目标的路径、类型、owner、持有者，以及每个递归目标自身和所有 descendant mount；停服后且任何删除前必须再次完成同一 mount/身份检查并确认全部目标无打开句柄。发现 bind/submount 或 mount namespace 无法确认时立即停止；本地既有备份与整机 journal 不得删除。
4. 新程序在非 23 UTC 的小时边界前启动于干净生产路径，以最终 `--run-forever` ExecStart 和 `Restart=no` 跨边界运行至少 15 分钟；直接检查已 finalized 的生产 Parquet、唯一 open/closed 日 manifest 与 journald，不生成 smoke 数据根或验证 sidecar。gate 和破坏性切换经阶段独立审查通过后，才只原子更新 unit 为 `Restart=on-failure`、reload/enable，不重启正在运行的进程。
5. gate 或长期运行版本失败必须停止服务，在 changelog 记录 build/时间/原因、`failed_gate|failed_long_running|partial_replace` 状态、binary SHA、unit digest、Restart 策略与数据清理结果；确认目录均由该失败 build 独占后立即删除其全部新 UTC-day 目录并创建干净根，不等待下一修复版，不移动、不打包、不保留失败 sidecar。若目录已混入获批长期数据或 build identity 不能证明则停止并请求用户决策；任何修复版仍须重新完成实现审查，并按第六节第 9 项替换已知失败版本。
6. 长期 unit 至少连续运行 26 小时且继续保持 enabled/running；任何 systemd 进程退出或 MainPID 变化都从新 MainPID 重置 26 小时窗口。验收还必须实际覆盖一个 Binance WS connection 的 24 小时平台生命周期及随后 source 内恢复；该 Binance connection 若提前重连则只重置其 24 小时子窗口。最终同时覆盖至少一个 UTC 日切换和至少 312 个 nominal 5 分钟时槽。平台实际存在且可发现的 market 必须全部按合同采集；平台未创建/未返回的 nominal market 记录 discovery gap 与查询事实，但不单独判定 collector 实现失败。
7. 连续 source 每小时持续产生 finalized Parquet，snapshot 按事件稀疏产生；至少一个日 manifest 从 open 原子转为 closed；没有持续超过一个小时的 running/tmp 文件。
8. 长跑期间不得出现未记录 aggTrade ID gap、writer drop、panic、restart loop 或虚假 day close；depth gap 必须 resnapshot，RTDS stale 与 Polymarket 静默必须触发恢复。systemd unit 必须配置 `StartLimitIntervalSec=600`、`StartLimitBurst=5`、`RestartPreventExitStatus=75 76`。
9. 只读重复扫描必须按可保证范围判定：PM current/next lifecycle 内为零 exact duplicate，RTDS 任意滚动 60 分钟内为零 exact duplicate，Binance 最近恢复的 25,000 ID 内同源重放可判定；窗口外重复只如实记录并交给 data-pipeline。结论直接追加到 changelog，不生成 JSON/TSV/Markdown 验证报告。验收完成后服务继续长期运行；但阶段 5 发现 code、unit、数据正确性或产物合同问题时必须立即 `disable --now`，完成协调停止并检查句柄/tmp，再按失败 build/day 身份清理或在身份混合时请求用户决策，禁止错误版本继续写入或自动重启。
