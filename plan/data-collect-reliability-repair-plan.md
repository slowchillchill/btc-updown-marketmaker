# 数据采集可靠性修复计划

> 对应需求：`demand/data-collect-reliability-repair-demand.md`
>
> 对应 changelog：`changelog/data-collect-reliability-repair-changelog.md`
>
> 事项编号：`DC-REPAIR-20260713-COLLECTOR-RELIABILITY`
>
> 状态：`TASK_2_1_RTDS_BOUNDED_DIAGNOSTIC_REVIEW_PASSED_READY_FOR_COMMIT`。步骤 4.4.2 未通过：PID=`1046442` 从 `2026-07-14T15:46:21.496539Z` 运行至 `16:01:16.948313Z`，跨过 16:00 UTC，但距连续 15 分钟验收仍差约 5 秒时因 RTDS SDK parser fault 非零退出；NRestarts=0。步骤 4.4.3 的 build-exclusive 核验、失败日删除、空根重建与 failed pair 停机保留已完成。用户 2026-07-15 批准的步骤 2.1.2 失败时有界诊断已实现；首轮独立审查发现重复必填字段的数组索引诊断不准确，现已改为直接使用 `RtdsMessage` typed decode 定位并通过全量本地门。独立复审 PASS，Critical/Important/Minor 均为 0；可以精确提交后从已 push commit 干净构建，再进入 4.2.1。

## 一、执行原则与成功定义

本任务严格串行执行。阶段门依次为：计划落盘与审查、代码实现与审查、新产物远端预置、旧环境清理与切换、26 小时持续运行。任何 gate 失败都停在当前阶段修复，不绕过审查，也不制作额外的 validation artifact。本计划只在训练 collector 生产合同范围内取代旧主计划和 PMXT retention 子计划；旧执行记录仍是历史事实。

成功只由仓库测试输出、生产 Parquet、每 UTC 日唯一 `manifest.json`、systemd journal、git commit 与本 changelog 证明。测试使用临时目录并自动删除；生产数据目录不得出现独立验证报告、receipt 或 debug sidecar。

## 二、已冻结路径与命名

```text
本地仓库                  /home/chillchill/btc-updown-marketmaker
本地 release binary       target/release/data-collect-training-collector
本地 unit 权威源          data-collect/deploy/systemd/data-collect-training-collector.service
远端 binary               ~/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector
远端 unit                 ~/.config/systemd/user/data-collect-training-collector.service
远端 CLI dataset root     /home/ubuntu/btc-updown-marketmaker/datasets
远端 effective output     /home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw
UTC 日目录                utc_day=YYYY-MM-DD
source 目录               source_kind=<source>
正常小时文件              <source>_HH.parquet
同小时重启文件            <source>_HH-part-N.parquet
日清单                    utc_day=YYYY-MM-DD/manifest.json
```

权威 build/deploy 流程不使用版本名二进制别名、多 binary release bundle、`current` symlink、run-id 目录、gate 数据根或 validation 输出目录。需求明确保留的本地 ignored legacy bundle 不属于权威流程。部署只使用带本轮 SHA 的 `upload-<sha256>` binary 与 `staging-<sha256>.service` unit；它们和写入中的 `.running`/`manifest.json.tmp` 都是瞬时文件，成功后不得残留。

## 三、阶段 1：冻结合同并通过计划审查

- 任务 1.1：核对真实代码、旧需求、远端运行与两日数据审查证据。
  - 步骤 1.1.1：列出程序错误、平台/网络诱因、未知来源与当前补救缺口。 -> 验证：需求第二节逐项区分事实、责任与剩余未知，不把推断写成事实。
  - 步骤 1.1.2：列出旧产物合同被取代的位置，并按用户 2026-07-14 决策同步 `demand/rebuild-core-demand.md` 数据与证据第 10 项。 -> 验证：新需求明确取代 data-collect per-segment/run/receipt/ResourceWatermark/PASS/required-files 合同；核心需求同步为每日单一事实 manifest，其他模块 run manifest 不变，旧 changelog 不被篡改。
- 任务 1.2：冻结最小 production contract。
  - 步骤 1.2.1：冻结 hourly Parquet + 每日唯一 manifest + journald。 -> 验证：manifest 只含采集事实，data-pipeline 负责内容校验、训练准入和语义去重；没有新增 sidecar/report/receipt。
  - 步骤 1.2.2：冻结单 binary、单 unit、单数据根和远端删除白名单。 -> 验证：所有路径都是明确绝对/`~` 路径，无宽泛父目录删除。
- 任务 1.3：独立计划审查。
  - 步骤 1.3.1：派生至少两名独立子代理，使用已确认的 `gpt-5.6-sol`/`ultra`，分别审查正确性/SDK-first 与简化程度/部署安全。 -> 验证：两者都明确返回 PASS，Critical/Important 为 0。
  - 步骤 1.3.2：修复审查问题后重复审查。 -> 验证：最终计划状态改为 `REVIEW_PASSED`，changelog 记录审查结论而不生成独立报告文件。
  - 步骤 1.3.3：只 stage 本事项文档，运行 `git diff --check`，创建 commit 并 push。 -> 验证：远端分支包含同一 commit，用户既有 dirty files 未被纳入。

## 四、阶段 2：按依赖顺序修复采集器

### 任务 2.1：锁定获批的官方 SDK 最小 patch 并证明单 receiver 路线

- 步骤 2.1.1：校验 crates.io `polymarket_client_sdk_v2 0.6.0` `.crate` SHA-256=`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e` 后，把其原始解包内容固定为 `third_party/polymarket_client_sdk_v2-0.6.0-p1/`，保留 `LICENSE`、`.cargo_vcs_info.json` 与官方 VCS commit=`3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`。根 `Cargo.toml` 通过 `[patch.crates-io]` 指向该 path 并从 workspace members 排除；data-collect 版本收紧为 `=0.6.0`。在自动删除的临时目录重新解包同一 `.crate`，逐路径/内容比较 vendored tree，只允许 `src/ws/connection.rs`、`src/clob/ws/types/response.rs` 与 `src/rtds/subscription.rs` 不同，任何新增/缺失/第四处修改立即失败。 -> 验证：allowlist diff 恰为三个文件，`cargo tree -p data-collect -i polymarket_client_sdk_v2` 只有该实例，`Cargo.lock --locked` 可复现；没有持久 diff 报告、运行时套 patch、registry cache 修改或第二份 SDK。
- 步骤 2.1.2：只修改 vendored SDK 的 `src/ws/connection.rs`、`src/clob/ws/types/response.rs` 与 `src/rtds/subscription.rs`：增加 sticky `fault_receiver()`/`ConnectionFault::Parser`；whole-frame parser error 发布 fault 后终止 heartbeat、进入 terminal `Disconnected` 并停止该 connection loop；CLOB 数组对 interested element 的 typed decode 采用 all-or-error，禁止合法前缀部分广播；为 `#[non_exhaustive] SimpleParser` 增加 `Default`；SDK outbound request 携带 connection generation，初次成功连接保留 generation 0 预连接请求；后续成功握手时，在 published generation 仍为旧值期间只 drain 当时 `receiver.len()` 的有限快照，再递增并 Release-store generation、最后发布 `Connected`；socket writer 继续丢弃切代后才入队的旧 generation 请求，同 generation 保持 FIFO。仅当 RTDS `parse_messages` 失败时，在保留原始 parser error 的基础上附加最多 `2,048` UTF-8 bytes 的结构诊断：frame byte length、SHA-256、顶层 JSON shape、数组首个 contract mismatch index、字段名，以及标量白名单 `message/status/code/topic/type/connectionId/requestId/error`；数组索引必须直接使用原始 frame 的 `RtdsMessage` typed decode 定位，不得用手写字段存在性规则代替。禁止输出任意 payload、完整 frame 或 frame 前缀。成功帧、fatal/sticky fault/terminal disconnected、unknown/uninterested filtering 与 generation 语义均不变；每个失败连接最多经既有 error/journald 路径输出一次，不写 Parquet，不新增 sidecar/report/receipt。 -> 验证：合法 interested + malformed interested 数组整帧错误且广播 0 条；whole-frame CLOB/RTDS 错误均可见 sticky fault 并不自动重连；合法 RTDS 单对象/数组结果不变；服务端错误信封只暴露白名单标量、字段名、长度、hash 和 shape，数组由真实 typed decode 报告首个 contract mismatch index，且重复必填字段回归必须命中正确索引，畸形 JSON保留原始 line/column；包含多字节、控制字符和超长白名单值的诊断安全转义/截断到 `2,048` UTF-8 bytes，任意 payload 内容不出现；错误 kind 与 source chain 保持不变；真实 loopback `ConnectionManager` 只产生 sticky terminal parser fault；下游 integration test 可用 `SimpleParser::default()` 构造官方 RTDS 低层组合；初连预连接请求、同 generation FIFO、CLOB/RTDS 与 authenticated subscription 重订阅不退化，旧 generation 请求在新 socket 写入 0 条；私有 queue 单元测试证明 10,000 条旧请求快照归零、late-old 仍被识别、10,000 次切代 high-water 不随 epoch 增长、generation overflow terminal 不 wrap。
- 步骤 2.1.3：CLOB 继续组合官方精确类型 `ConnectionManager<WsMessage, Arc<InterestTracker>>`、CLOB `SubscriptionManager`、一个 `broadcast::Receiver<WsMessage>`、`state_receiver()` 与 `fault_receiver()`；RTDS 同样组合 `ConnectionManager<RtdsMessage, SimpleParser>`、RTDS `SubscriptionManager` 和单 receiver/state/fault。CLOB 与 RTDS 各只有一个项目串行订阅 actor；RTDS 不启动官方 self-spawn reconnection handler，而是以 latest-value desired watch 保存单一订阅，每个新的 `Connected { since }` 在任何发送前重新读取并消费 watch 最新值，再创建新的官方 RTDS manager 并从空 server state 重放一次 latest desired；同 epoch 的业务重订阅也经该 actor 串行执行。collector 即使先观察到 `Disconnected` 也必须再检查 sticky fault，将 parser fault 分类为 schema fatal 并协调非零退出。 -> 验证：官方 subscription、动态增删 CLOB assets、transport 自动重连/重订阅、两条连接的 `RecvError::Lagged(n)`、parser fault 与 retry exhausted terminal state 全部可见，不永久等待 receiver；RTDS desired 在 actor 首次 poll 前设置仍能应用；CLOB/RTDS 真实 actor 先发送并由 loopback server 确认缓存 A，再同步提交 desired B 与新 Connected epoch；state-first 后该 epoch 只能收到一条 B 请求，删除生产 refresh 点必须失败；连续 immediate-close flapping 与 pre-connect 10,000 次 desired 更新后稳定 epoch 只收到一次最新订阅；RTDS 同 epoch `resubscribe()` 的首次同步 poll 必须为 Pending并保持 `unsubscribe -> subscribe` FIFO，另在 actor 无 connected epoch 时至少 100ms 不得返回，恢复后才完成。
- 步骤 2.1.4：SDK 内部 outgoing/ping unbounded channel 只按用户批准保留为控制通道例外；collector 的 CLOB/RTDS actor 都用 bounded/latest-value desired-state watch 合并状态，断线期间不重复 enqueue，heartbeat 最多一个未完成 ping，RTDS 业务重订阅次数有限；connection-generation patch 必须阻止旧 epoch 控制请求进入新 socket。 -> 验证：断线压力测试中大量 desired-state 更新恢复后最多执行一次最小订阅变更；旧请求滞留、快速 transport flap 与 generation 切换压力下，旧 generation 写入为 0、同 generation FIFO 不变、连接/task/控制消息积压不增长，任何项目 data-bearing payload 都不进入该例外。
- 步骤 2.1.5：若上述固定 patch 后仍不能满足单 receiver、动态订阅、自动重连、显式 lag/parser fault/terminal state 或进程退出释放，立即停止并向用户报告新增 SDK 缺口；不得扩大 vendored patch、改用 raw WS/REST 或手写订阅。 -> 验证：新增决策前不实现旁路。

### 任务 2.2：修复 Polymarket discovery、订阅和语义

- 步骤 2.2.1：将 discovery contract 改为 current 必需、next optional，next 独立发现和动态订阅。 -> 验证：current-only、next 延迟出现、rollover 晋升、过期退订测试通过。
- 步骤 2.2.2：把 `MarketSdkRuntime` 已有的唯一 SDK receiver 一次性移交给项目 fast-drain task，禁止再次 `subscribe()`；recv 后立即冻结 wall-clock/monotonic receive time、取得共享全局及 decoded 局部 permits，再按 FIFO 交给唯一串行 processor。processor 再执行 canonical、dedupe、current/next target filter 与 fan-out；订阅/Gamma/resubscribe/writer 等慢路径不得阻止 SDK receiver 排水。`Lagged(n)`、decoded queue/budget timeout 都先按顺序 drain 已接受消息，再记录 gap并非零退出。 -> 验证：第二次 take receiver 明确失败，1024 以上 burst 不触发 SDK lag，mixed-target fan-out 与 unmatched 过滤不变；排队后 row 仍使用 recv 时刻，freshness 不接受早于订阅的 queued event。
- 步骤 2.2.3：按 market/asset 增加 30 秒 first-event 与 60 秒 freshness watchdog；Ping/Pong、global event、其他 asset 不刷新。先做有限退订/重订阅，仍失败则进程重启。 -> 验证：一个健康 asset 不能掩盖另一个，next 尚不存在不影响 current。
- 步骤 2.2.4：删除 synthetic `market_discovery_switch -> new_market` 路径；Gamma 只写 discovery metadata。 -> 验证：synthetic 事件产生 0 个 `new_market`，原生 WS 事件仍产生 1 条。

### 任务 2.3：增加保守的传输级去重

- 步骤 2.3.1：为 Polymarket 每个 SDK 消息生成覆盖 source/variant/market/asset/platform time/全部 SDK decoded fields、排除 receive time 与派生 slug 的稳定 canonical JSON，作为 `source_native_payload_json` 落入每条规范化 Parquet row，并只把 `[u8;32]` SHA-256 fingerprint 保存为内存 key。fingerprint check/insert 在 row fan-out 前按消息执行；首次多 target 消息保留全部匹配 rows，整条重放才全部丢弃。集合最多保留 current+next，每 lifecycle 上限固定 `2_000_000`，next 晋升只移动集合、不复制；退役前不淘汰，第 2,000,001 个 unique key 记 gap、coordinated finalize、exit 76。启动订阅前用每批最多 16 行的 Arrow projection 流式读取最近 finalized Parquet 的 `slug/source_native_payload_json`，重算 fingerprint；单行超过 1 MiB 时在 ingress 前 exit 76，不一次性加载完整列。 -> 验证：旧 layout 丢失字段不同的消息均保留；首次 mixed-target `PriceChange` 全部 target rows 存在，整个消息重放写 0 行；覆盖所有 variant、两 lifecycle 上限、退役清理、进程 A/B 重放与小 limit 注入的 hard-bound fail closed。
- 步骤 2.3.2：Binance 同源按 native ID+该 source 完整 payload 并恢复最近 25,000 ID 比较窗口；WS/REST overlap 只按共有同义字段 `a,p,q,f,l,T,m`。RTDS 按完整 typed source-native payload，以 key 第一次被接受的 collector receive time 建立 `[first_received, first_received+60m)` 窗口，重复不延长，固定上限 `100_000`；从 finalized Parquet 以相同口径恢复。 -> 验证：Binance 进程 A/B 下同 ID 同 payload 丢弃、不同 payload 记 conflict、窗口外重放保留并记 gap；WS/REST shared fields 一致合并、不一致保留两行；RTDS 在 59:59 重放只写一次、60:00 重新保留，stale/future/invalid payload timestamp 不改变窗口，100,001st 未过期 key 触发 gap/finalize/exit 76；仅数值相近或 receive time 相同的不同 payload 均保留。
- 步骤 2.3.3：明确不做跨 source 或业务等价猜测。 -> 验证：测试证明仅 receive time 相同但业务 payload 不同不会被删除。

### 任务 2.4：修复 Binance aggTrade 连续性

- 步骤 2.4.1：抽出跨网络重连状态；明确最新 continuity epoch 的 `epoch_start_id/last_observed_id/last_contiguous_id`，只有向前接受的 live/backfill 行可单调推进，旧 replay 不改变游标。回补最终失败时以 unresolved range 关闭旧 epoch，从下一条向前 ID 开新 epoch。启动时先处理所有早于当前日的 open manifest：完成磁盘对账并原子转为 closed，失败则 ingress 前 exit 76；之后只允许当前日 open manifest或此前最近一个已 closed manifest作为 continuity base，叠加 finalized 文件，再跨日反向恢复最近 25,000 ID，只回补最新 epoch 尾部。新日首次 manifest 继承该 epoch；只有全根无 manifest/aggTrade 才记录 initial boundary。 -> 验证：前一日 open 必须先 closed；上一日 closed/当日 manifest 尚未创建、当日 manifest 已创建但无 Parquet、Parquet 已 rename 但 manifest 未更新三种崩溃点均恢复同一 epoch 与跨日窗口，不追逐永久旧 gap。
- 步骤 2.4.2：用现有 `reqwest` 调用 Binance 官方 aggTrades REST，固定请求上下文 `BTCUSDT`，按官方 limit 分页补齐；响应校验 ID/顺序/边界/payload，symbol 从请求上下文注入。 -> 验证：100 到 103 能补 101/102，超过 1000 个 ID 能分页；429、timeout、空/乱序响应走失败路径。
- 步骤 2.4.3：回补期间把后续 WS 行放入全局 row/byte 预算内、另有 25,000 行 hard cap 的 pending buffer，完成后按 ID 合并；cross-source 只比较 `a,p,q,f,l,T,m`。一致时写一条 merged row，同时保留 `ws_ignore`、`rest_was_best_price_match`、真实 `websocket_timestamp_received` 与 `rest_backfill_timestamp_received`，并以二者最大值作为 `timestamp_received` availability/分区时间；不一致时两行都保留并记 conflict。测试 byte/row overflow、跨小时/午夜回补、REST 返回 pending WS ID、shared fields 一致/不一致。 -> 验证：正常 overlap 只写一行且两种 `M` 和两个 ingress 时间不混淆，后到字段不会被赋予更早 availability 或写回 closed 分区，overflow 留 unresolved gap且开启新 continuity epoch，不伪报连续。
- 步骤 2.4.4：schema 升级，`event_type/event_time/stream` nullable；REST-only 行三者为空、`trade_time=T`、`rest_backfill_timestamp_received=timestamp_received=本机收到完整响应时间`、`websocket_timestamp_received=null`、`ingest_source=rest_backfill`，WS-only 使用相反的 ingress 时间空值，merged 为 `websocket_rest_merged`。同步明确旧需求非空 `e/E/s` 与 `event_time=E` 只约束 WS/merged 的 WS 字段。 -> 验证：没有伪造 WS `e/E/stream`、历史 receive time 或未来字段提前可见，row readback 保留两个真实 ingress 时间、source-specific `M` 与 manifest backfill/merge/continuity-epoch 计数。

### 任务 2.5：修复 RTDS 与时间语义

- 步骤 2.5.1：把 RTDS 从高层 filtered stream 改为阶段 2.1 已证明的官方公共单 receiver/state/fault 路线；保留真实 `message_type`，只允许 `topic=crypto_prices_chainlink,type=update,symbol=btc/usd` 恢复健康；按 `-5_000 <= age_ms <= 30_000` 判 fresh。真实 SDK receiver 会在 filter 订阅后先收到 `topic=crypto_prices,type=subscribe,payload={symbol,data:[...]}` 历史 snapshot；该非目标 envelope 的 Chainlink 单价 decode 失败必须在升级为 fatal 前归类为 ignored，目标 `topic+update` 的 schema 错误仍 fatal。健康期间不周期重发；60 秒无 fresh matching payload 后最多重订阅两次、间隔 3 秒，第二次后再等 3 秒仍无恢复即协调进程退出。`Lagged(n)`/terminal disconnected 同样退出，parser fault 分类为 schema fatal。 -> 验证：真实 snapshot 回归先于正常 update 且不写行、不记 gap、不恢复健康、不退出；目标 update malformed 仍 fatal；lag/parser 注入、重订阅次数/间隔/最终退出，以及 age 两个等号边界、future skew、持续 stale、price-shaped wrong-type/topic/symbol 全部覆盖。
- 步骤 2.5.2：typed `freshness_status` 区分 fresh/stale/invalid_type/invalid_topic/invalid_symbol/invalid_timestamp，不能伪装 current price。 -> 验证：typed readback 保留真实 message type 并能排除全部无效状态。
- 步骤 2.5.3：在 Gamma/depth HTTP 请求前后采样真实 start/receive；WS metadata fetch-start 为空。 -> 验证：mock delay 证明 start < receive，缺值不再回填 receive time。
- 步骤 2.5.4：删除 `metadata_recv_time_utc`、`token_ids`、`local_u_after` 等义列并更新 schema/测试。 -> 验证：保留的不同语义时间与 depth stitch 字段仍完整。
- 步骤 2.5.5：增加 mock REST+WS depth runtime 测试，覆盖 bootstrap snapshot、buffered bridge、midstream gap、resnapshot、首条 bridge、顺序与日 manifest 计数，以及 snapshot 429/timeout/恢复失败。 -> 验证：writer/schema/day manifest 改造未破坏完整 resnapshot 链。

### 任务 2.6：修复 writer 和 supervision

- 步骤 2.6.1：为所有项目自有待写数据实现共享 row+byte permits：编译常量 `100_000 rows + 134_217_728 bytes`、每个 fan-out 后 row hard cap `1_048_576 bytes`、acquire timeout 2 秒；row 入队前按结构/owned capacity（含 canonical JSON）计算并拒绝超限，首次进入时申请 permits，跨 ingress/batch/writer move 时携带，复制另计，移入 Arrow builder并释放 event 后归还。Polymarket decoded message 另受 `25_000 messages/67_108_864 bytes` 附加局部 cap，但同时继续持有同一全局 permits；原 message、canonical copy 与 fan-out rows 的并存 owned memory 分别计数。Binance pending 计入同一预算；row/budget 超限、timeout、closed channel、parser/finalize/atomic rename 全链路返回 `Result` 并协调失败。 -> 验证：1MiB row、100,000/128MiB 全局边界、decoded local cap、owned payload 复制、permits 释放、短背压和超限故障注入通过，无 drop 后继续或 decoded queue 独占全局预算；SDK 控制通道例外与项目数据路径隔离。
- 步骤 2.6.2：每个 source 的单次恢复周期有 finite attempts/max elapsed/capped backoff+jitter，外层 supervisor 可无限启动新周期；runtime writer/PM lag 失败退出进程并受 systemd StartLimit 约束，不可恢复 startup/path/manifest/dedupe 不变量转 76。 -> 验证：故障注入证明无 tight loop、网络恢复后继续，内部错误不会假装运行正常，也不会把持久不变量错误变成 restart loop。
- 步骤 2.6.3：启动时只清理本数据根内无进程持有的 stale running/tmp，并记录 gap。 -> 验证：路径 containment 与 symlink 负例 fail closed，临时目录自动删除。
- 步骤 2.6.4：实现 SIGTERM coordinated shutdown：对 Polymarket 先向 fast-drain FIFO 插入 stop fence、停止 SDK intake并处理 marker 前全部 decoded message，禁止直接 abort source；随后停止其他 ingress、drain batch/writer、finalize、更新 open day manifest、journal 完成标记并在应用内部 90 秒截止前退出 0。 -> 验证：operator/systemd SIGTERM 正常停止且已接受 PM message 不丢失、无 detached receiver task或 running/tmp；超时/SIGKILL/非零状态不能通过，fault 路径完成同一 drain 后仍保留非零状态。
- 步骤 2.6.5：50GB threshold 复用同一 drain/finalize 流程，完成后返回保留退出码 75。 -> 验证：无 running/tmp、open manifest 已更新、journal 为 `dataset_capacity_reached`、ExecMainStatus=75 且 systemd 不重启。
- 步骤 2.6.6：把不可恢复 startup config/path/manifest reconciliation 与 active-lifecycle dedupe hard-bound 映射到 exit 76；unit 使用 `RestartPreventExitStatus=75 76`、`StartLimitIntervalSec=600`、`StartLimitBurst=5`。 -> 验证：exit 75/76 都不重启，其他连续进程失败最多 5 次/600 秒后进入 failed，不形成无限 loop。

### 任务 2.7：收敛 writer 与生产产物

- 步骤 2.7.1：删除 300k rows/128MB 文件 rollover，利用已知的 per-row size 在加入前判断，用 Arrow 59 的 `50_000 rows/16_777_216 logical bytes` per-writer row-group hard limit 保持有界；恢复 projection 固定每批最多 16 行，每行继续校验 1MiB hard cap。达到 row-group 上限只结束 group，文件仍只按 UTC hour 关闭。 -> 验证：高流量 fixture 在同小时只有一个正常 Parquet、多 row groups 均不越界，恢复 batch logical payload 不超过16MiB、处理后释放且不加载完整列。
- 步骤 2.7.2：实现同小时重启 `part-N`；四个稳定 logical producer outer supervisor 通过同一 ingress queue 发送 UTC-day watermark/barrier，独立于 inner connection/backoff，旧日事件必须位于各自 barrier 之前。Polymarket outer supervisor 必须先请求 fast-drain FIFO fence，等待 processor 处理 marker 前全部 decoded message并暂停，再发送 day barrier后恢复；lifecycle/订阅集合切换使用同一 fence，marker 后消息只能由新 context 处理。depth outer supervisor 还必须在 UTC-hour 边界先 fence source，再通过同一 ingress 顺序只对 `binance_depth_snapshot` 发送小时 finalize；旧小时有 snapshot 时关闭主文件，无 snapshot 时 no-op，同一进程内不得因此生成 `part-N`。 -> 验证：PM backlog 不被 day/lifecycle barrier 超越；断线/backoff barrier 与 snapshot 小时关闭合同保持成立，closed 后不再出现旧日行。
- 步骤 2.7.3：实现唯一 manifest owner actor；writer 只通过 bounded channel 提交 finalize delta，owner 等 queue/batch drain 与六个 writer finalize ack 后串行执行“fsync tmp -> atomic rename -> fsync parent dir”。日 manifest 的紧凑 Binance 状态保存最新 `epoch_start_id/last_observed_id/last_contiguous_id` 与 unresolved ranges，不创建游标 sidecar。 -> 验证：并发小时/日切无 last-write-wins，跨日 gap 分割，异常退出不 closed，断电恢复只见旧完整文件或新完整文件；continuity epoch 可从 manifest+Parquet 尾部重建。
- 步骤 2.7.4：重启时必须先处理所有早于当前 UTC 日的 open manifest：确认旧进程不存在、清理 stale running、记录 crash gap、以 finalized Parquet 对账并用 `unclean_exit_recovery` 原子 closed；完成前不得选择 continuity base 或启动网络。随后 owner 才合并当前日 open manifest/磁盘 finalized 文件，或在当日无 manifest/Parquet 时使用最近前一 closed 日并让新日首次 manifest 继承状态；恢复 build/config/file/last sequence、Binance epoch 与 dedupe 窗口。日内 SIGTERM 只 finalize 并保持当日 open。 -> 验证：不覆盖/重开 closed 日，前一日 open 必先关闭；`23:59 crash -> 00:01 restart` 与三种 manifest/rename 空窗都恢复同一 epoch，只回补最新尾部，对账失败不启动 source。
- 步骤 2.7.5：删除 segment/run manifest、runtime receipts、retention admin/policy 与重复 sidecar 生产路径；dataset 达到 50GB 只执行 coordinated finalize、journal 与 exit 75。 -> 验证：临时生产根只含 Parquet、每日一个 manifest 和瞬时文件，禁止项均不存在。
- 步骤 2.7.6：简化训练 CLI，删除无用途的 `--run-id` 与 retention delete 参数，只保留 `--dataset-root`、`--collector-region`、`--run-forever`、`--max-dataset-bytes` 等实际参数。 -> 验证：effective output 只出现一次 `training/typed_raw`。
- 步骤 2.7.7：同步 `data-collect/docs/training-collector-deploy-readme.md`、`acceptance-matrix.md`、`paper-live-hot-path-integration.md` 中训练 cold path 的旧 receipt 描述与相关合同测试。 -> 验证：训练文档不再要求旧 receipt/release bundle，未来 paper/live 源码 binary 与独立角色仍保留。
- 步骤 2.7.8：把唯一 repo-tracked unit 源收敛为 `data-collect/deploy/systemd/data-collect-training-collector.service`；删除 `.service.example`、独立 gate example、`export-release-artifacts.sh`、retention-admin binary/module/Cargo entry/合同测试。保留用户既有 ignored `data-collect/releases/train-data-collect-ver1-0/` 与 `.gitignore`，但任何代码/文档/命令不得读取或部署它。 -> 验证：repo-tracked 路径不存在能生成多 binary bundle、训练别名、gate unit、`RELEASE_INFO.env`、`SHA256SUMS` 或 retention receipt 的 shadow path；ignored bundle 未被 stage/修改，训练 build 只产生目标 binary，paper/live 源码 binary 仍可分别构建。
- 步骤 2.7.9：为 training binary 增加无副作用 `--version`/`--help` fast path，在创建 runtime、连接网络或解析数据根前返回。 -> 验证：临时 HOME/不存在 dataset root 下两命令 exit 0 且没有新文件/网络连接。

## 五、阶段 3：实现自审与独立复审

- 任务 3.1：本地自审和验证。
  - 步骤 3.1.1：检查 diff 只覆盖本事项，删除由改动产生的孤立代码/依赖，不清理无关旧代码。 -> 验证：每个改动可追溯到需求条款。
  - 步骤 3.1.2：运行 workspace fmt、data-collect 全量 tests/clippy；对 excluded vendored SDK 单独运行 fmt、`--locked` lib tests，以及官方 Rust 1.88 CI 的 default/all-features 两组 `--all-targets` clippy；校验原始 `.crate` SHA 后在自动删除临时目录解包并比较全树，差异路径必须恰为获批三个 SDK 文件；再运行 SDK patch contract test、既有 generic writer microbenchmark，以及复用生产 helper 的真实 `SDK receiver -> decoded queue -> canonical/dedupe/fan-out -> shared ingress/writer` 四源 `2k-5k source-native messages/s` 持续加 burst 测试、`cargo tree -p data-collect -i polymarket_client_sdk_v2` 与 `git diff --check`。 -> 验证：1024 以上 burst 无 SDK lag，message/row 数精确一致且无单调积压/drop/未完成 row；其余全部退出 0，dependency tree 只有仓库内 p1，无第四处 vendor 差异；结果只记 changelog，不生成报告。
- 任务 3.2：独立实现审查。
  - 步骤 3.2.1：派生至少两名 `gpt-5.6-sol`/`ultra` 子代理，分别审查 source 正确性/SDK-first 与 writer/schema/部署安全。 -> 验证：两者均 PASS，Critical/Important 为 0。
  - 步骤 3.2.2：有问题则修复、重跑测试和复审。 -> 验证：最终审查通过前不构建 production release，不触碰远端。
- 任务 3.3：提交实现。
  - 步骤 3.3.1：精确 stage 本事项文件，commit 并 push。 -> 验证：远端分支含 commit，用户原有 dirty files 未纳入。

## 六、阶段 4：本地构建、远端预置与破坏性切换

### 任务 4.1：本地构建唯一产物

- 步骤 4.1.1：从阶段 3 已 push 的完整 commit 创建 `/tmp/data-collect-build-<short-commit>` detached worktree，确认 `HEAD` 精确等于该 commit 且 worktree clean；设置 `DATA_COLLECT_GIT_COMMIT=<完整 commit>`，使用该 worktree 专属 `/tmp/data-collect-target-<short-commit>` 执行 `cargo build --release --locked -p data-collect --bin data-collect-training-collector`。构建后再次确认 HEAD 未变且 `git status --porcelain` 为空，在隔离 binary 上计算 SHA；复制到主仓库不存在的 `target/release/data-collect-training-collector.local-staging-<sha256>`，复核 SHA 后 atomic rename 为固定 `target/release/data-collect-training-collector`，最后删除临时 worktree/target。 -> 验证：所有编译输入和嵌入的 build identity 来自已 push commit，production build 不复用主 target cache；记录 commit、size、SHA-256 到 changelog，不生成 release receipt，主 worktree用户 dirty 文件未参与构建。

### 任务 4.2：在停止旧服务前预置新产物并完成全量预检

- 步骤 4.2.1：先判定部署状态。首次切换要求最终新 binary 不存在且旧主 service 身份仍等于冻结基线；失败重试只接受 changelog 已记录的 `failed_gate`（binary SHA + `Restart=no` unit digest）、`failed_long_running`（binary SHA + `Restart=on-failure` unit digest）或 `partial_replace`（old/new binary SHA 与 old/new unit digest 实际组合），并要求 service inactive、MainPID=0、无句柄、实际 identity 逐项匹配，随后执行 `reset-failed`。两种入口都对 `data-collect/bin`、user unit 目录及逐级父目录做不跟随 symlink 的 owner/type/containment 检查；本轮 digest-named staging 必须不存在并 exclusive-create 上传。所有 binary SHA/ELF/owner/mode、无副作用 `--version`/`--help`、loader/`ldd`/GLIBC 检查直接针对 staging binary；以 repo 权威 unit 机械生成 `Restart=no` gate staging unit并 verify。4.2 全部门通过前不得修改 final binary/unit。 -> 验证：staging binary 已证明能在远端加载但未连接网络/写数据，staging unit 未生效；未知对象或 identity 不匹配均未被覆盖，首次入口旧服务继续运行，重试入口原失败 pair 保持 inactive 原状。
- 步骤 4.2.2：冻结 gate/long 共用的最终 ExecStart：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector \
  --dataset-root /home/ubuntu/btc-updown-marketmaker/datasets \
  --collector-region eu-west-1 \
  --run-forever \
  --max-dataset-bytes 50000000000
```

  -> 验证：CLI parent root 与 effective `training/typed_raw` 不重复嵌套；gate unit 同时设置 `TimeoutStopSec=120`、`KillSignal=SIGTERM`、`Restart=no`、`RestartSec=10`、`RestartPreventExitStatus=75 76`、`StartLimitIntervalSec=600`、`StartLimitBurst=5`，应用内部 shutdown deadline 为 90 秒。
- 步骤 4.2.3：首次切换在停服前重新采样旧主服务身份，并与 changelog 冻结的 FragmentPath、unit owner/type/SHA、规范化 ExecStart argv、ControlGroup、current target、`/proc/<pid>/exe` 路径/SHA 比较；只允许 MainPID/启动时间因旧服务既有重启变化。随后一次性预检全部删除目标的绝对路径、类型、owner=`ubuntu` 与打开句柄；对每个递归删除目录读取 mount table，枚举目标自身及全部 descendant mount，发现 bind/submount 或 namespace 结果无法确认立即停止。`current` 只用 `lstat/readlink` 验证唯一允许 target，绝不跟随。此时只允许核验后的旧主 cgroup/MainPID 持有旧 executable/当前数据文件。失败重试不再删除旧对象，只复核 inactive/no-handle、已知失败状态与干净数据根，4.2 全部通过后跳过 4.3。 -> 验证：首次切换身份、目标结构/持有者/descendant-mount 同时通过才允许进入 4.3；重试状态逐项匹配才进入 4.4；任一不符请求用户决策，不能边检边删。
- 步骤 4.2.4：在本地开发机执行 `systemctl --user disable --now data-collect-new-datasets-archive.timer` 并停止失败的 archive service。 -> 验证：旧 timer disabled/inactive、service inactive；不删除 `/home/chillchill/new-datasets`，也不让旧 schema 脚本接触新数据。
- 步骤 4.2.5：4.2 任一门失败都只删除本轮 digest-named staging；本轮创建且为空的 `data-collect/bin` 才 `rmdir`。首次入口的旧主 service/PID/release/data/unit 保持不变；失败重试的既有 failed/partial pair 保持停服原状。 -> 验证：final binary/unit 从未被本轮预检修改，两种入口都不遗留本轮 staging，也不按模糊文件名删除对象。

### 任务 4.3：停止旧服务并执行清理白名单

- 步骤 4.3.1：执行 `disable --now` 主 unit，并对旧 gate unit 执行 disable/stop；确认两个 unit inactive、MainPID=0、default.target enable symlink 已消失且没有 data-collect 进程。 -> 验证：旧 running 文件无打开句柄；记录旧服务 Result/ExecMainStatus，若 SIGKILL/timeout 则如实记 changelog。
- 步骤 4.3.2：停服后且任何删除前，再一次性复核全部白名单目标的路径、类型、owner 与无打开句柄，并重新读取 mount table 枚举每个递归目标自身及全部 descendant mount；发现 bind/submount 或 namespace 无法确认立即停止。`--one-file-system` 只能作为额外删除防护，不能替代枚举。 -> 验证：全部目标同时通过才允许删除；任一不通过都保持停服并请求用户决策，不能边复核边删。
- 步骤 4.3.3：记录清理前顶层路径和大小到 changelog；不创建备份或 inventory 文件。 -> 验证：终端只读输出足以核对目标，明确承认未完整备份日将不可恢复。
- 步骤 4.3.4：先 unlink 已验证的 `current` symlink，再只删除下列旧程序对象：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/current
/home/ubuntu/btc-updown-marketmaker/data-collect/releases/train-data-collect-ver1-0
/home/ubuntu/btc-updown-marketmaker/data-collect/releases/5c0d8907b54b-aggtrade-20260710T062005Z
/home/ubuntu/btc-updown-marketmaker/data-collect/releases/3a1b7254fc1c-aggtrade-v3-20260710T143830Z
/home/ubuntu/btc-updown-marketmaker/data-collect/latest_pmxt_hourly_release_name
/home/ubuntu/.config/systemd/user/data-collect-training-collector-gate.service
/home/ubuntu/btc-updown-marketmaker/data-collect/releases
```

  `current` 必须是 symlink，唯一允许 target 为 `/home/ubuntu/btc-updown-marketmaker/data-collect/releases/3a1b7254fc1c-aggtrade-v3-20260710T143830Z`，只 unlink symlink 本身。白名单最后一项不是递归删除目标，只允许在三个 release 删除后确认目录完全为空时执行 `rmdir`。 -> 验证：三个 release 子目录与旧 marker/current/gate unit 不存在；绝对路径 `releases` 完全为空才删除，并执行 daemon-reload。
- 步骤 4.3.5：只删除下列旧数据对象并重新创建干净 `typed_raw`：

```text
/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw
/home/ubuntu/btc-updown-marketmaker/datasets/training/run_manifests
/home/ubuntu/btc-updown-marketmaker/datasets/training/runtime_artifacts
/home/ubuntu/btc-updown-marketmaker/datasets-retention-validation
```

  -> 验证：不得删除 `~/btc-updown-marketmaker`、`datasets` 父目录、整机 journal 或本地 `/home/chillchill/new-datasets`；干净 training 根没有旧 Parquet/manifest/receipt。

### 任务 4.4：原子启用已预置的新 binary 与 unit

- 步骤 4.4.1：确认 service inactive、MainPID=0、无句柄后，先 atomic rename 已校验 staging binary 到 final 并复核 SHA，再 atomic rename staging gate unit 到 final 并复核 digest，然后 daemon-reload，在除 23 UTC 之外某小时的 45–50 分之间启动最终 `--run-forever` 进程。两次 rename 之间中断时保持 inactive，立即记录 `partial_replace` 的 old/new binary SHA 与 old/new unit digest；只有实际组合完全匹配记录时才继续补完替换，否则请求用户决策。 -> 验证：远端唯一 binary SHA 与本地一致，unit ExecStart/Restart=no 正确，本轮 staging 全部消失；任何部分状态都可由 digest 机械识别。
- 步骤 4.4.2：让 `Restart=no` gate 跨 UTC 小时边界运行至少 15 分钟。 -> 验证：四类外部源/六个 source kind 有真实数据或合法稀疏说明；至少一组小时 Parquet finalized、日 manifest 唯一且状态合法，journald 无 lag/drop/writer fatal；不创建额外数据根或报告。
- 步骤 4.4.3：gate 失败则停止服务，在 changelog 记录 build/时间/原因、`failed_gate`、final binary SHA、`Restart=no` unit digest 与数据清理结果；确认 build identity 后立即删除失败 gate 独占的全部新 UTC-day 目录并重建干净根，不等待下一修复版。失败 pair 保持 inactive，作为下一修复版 4.2.1 可精确识别和替换的已知状态。 -> 验证：不移动/打包/生成 sidecar；若混入获批长期数据或 identity 不可证明则停止请求决策，代码修复仍返回阶段 2并完整复审。

### 任务 4.5：破坏性切换后的阶段审查

- 步骤 4.5.1：gate 进程保持 `Restart=no` 运行，派生 `gpt-5.6-sol`/`ultra` 子代理只读复核删除白名单执行事实、新 binary/unit/SHA、干净数据根、小时 Parquet/日 manifest/journal 与无额外产物。 -> 验证：Critical/Important 为 0 且明确 PASS；结论只追加 changelog，不生成报告。
- 步骤 4.5.2：审查发现 code、unit、数据正确性或产物合同问题时，必须立即 SIGTERM 停止 gate，并按 4.4.3 清理 build-exclusive UTC-day 后回到相应阶段；只有纯 changelog/证据文字问题才可保持进程运行并修订复审。通过后精确提交 changelog 并 push，再进入阶段 5。 -> 验证：FAIL 不继续写已判有问题的数据；PASS 时阶段 4 commit 已在远端分支且 MainPID 未变化。

## 七、阶段 5：长期运行与最终审查

- 任务 5.1：启用长期 unit。
  - 步骤 5.1.1：阶段 4 审查通过后，先比较阶段 4 已记录的 gate unit digest、MainPID 与 executable SHA；完全匹配后从同一 repo 权威 unit 取得 `Restart=on-failure` 版本，对 unit 父目录做不跟随 symlink 的 owner/type/containment 检查，用不存在的 digest-named staging unit exclusive-create 上传并 verify，再 atomic rename、daemon-reload/enable，禁止 restart 正在运行的进程。 -> 验证：MainPID 不变，Restart 为 `on-failure`，PID executable SHA 不变，最终长期 unit digest 已记录，本轮 staging 消失。
  - 步骤 5.1.2：持续运行至少 26 小时并继续运行；任何 systemd 进程退出/MainPID 变化都从新 MainPID 重置整个 26 小时窗口。 -> 验证：同一 MainPID 覆盖 26 小时；至少一个 Binance WS connection 实际覆盖平台 24 小时 lifecycle 并由 source 内恢复，若该 connection 提前重连则只重置其 24 小时子窗口；同时覆盖 UTC 日切换和至少 312 个 nominal 5 分钟时槽。
- 任务 5.2：在不生成报告文件的前提下检查生产事实。
  - 步骤 5.2.1：按小时检查 service state、NRestarts、journal error/reconnect、finalized file 增长和 stale running。 -> 验证：无 restart loop、panic、writer fatal，running/tmp 不跨越一个完整小时。
  - 步骤 5.2.2：日切后直接读取 closed manifest 和 Parquet metadata/必要列，检查 row count、SHA、source-native ID、freshness 与 structure duplicate。 -> 验证：至少一个 manifest closed，文件计数一致，无未记录 aggTrade gap；PM active lifecycle 和 RTDS 任意 60 分钟窗口内无 exact duplicate，Binance 最近 25,000-ID 恢复窗口的同源重放可判定，窗口外结论不夸大并交给 data-pipeline。
  - 步骤 5.2.3：将命令、时间和汇总结论追加到本 changelog，不生成 JSON/TSV/Markdown 报告。 -> 验证：生产目录仍只含合同产物。
  - 步骤 5.2.4：任一轮监控若确认 code、unit、数据正确性或产物合同错误，立即执行与 5.3.2 相同的 `disable --now`、协调停服、句柄/tmp 检查和 build/day 身份清理门，并记录 `failed_long_running` 的 binary SHA、`Restart=on-failure` unit digest 与数据清理结果，不等待 26 小时或最终审查。 -> 验证：已确认错误的版本不会继续写入或自动重启；普通、已记录且可恢复的平台/网络瞬态不被误判为代码错误。
- 任务 5.3：最终独立审查与交付。
  - 步骤 5.3.1：派生 `gpt-5.6-sol`/`ultra` 子代理复核实现 commit、远端 unit/path、26 小时事实和最小产物合同。 -> 验证：PASS，Critical/Important 为 0。
  - 步骤 5.3.2：若只需修正文档/changelog，精确提交并 push；若发现 code、unit、数据正确性或产物合同问题，立即 `disable --now` 并协调 SIGTERM，确认 MainPID=0、无句柄及无残留 running/tmp，记录 `failed_long_running` identity，再按 manifest build identity 判断受影响 UTC-day 是否由失败 build 独占：独占则删除并重建干净根，混合或无法证明则停止请求用户决策；随后回到阶段 2 与完整实现审查。 -> 验证：错误版本不会继续写入或被 systemd 重启，下一次 4.2 能按已记录 identity 进入重试，需求、计划、changelog 与保留事实一致。
  - 步骤 5.3.3：服务保持 enabled/running，向用户报告最终 commit、binary SHA、远端路径、起止时间和剩余平台风险。 -> 验证：不以验收结束为由停止采集。

## 八、停止条件与禁止事项

以下任一情况立即停止对应阶段并向用户报告：

1. 官方 Polymarket Rust SDK 公共组件不能实现单 receiver、动态订阅、自动重连或显式 lag；
2. 文档、代码、官方 API 或用户指令出现不能同时成立的语义；
3. 独立审查未 PASS；
4. 删除目标 realpath/type 与白名单不一致；停服前除已核验旧主 unit cgroup/MainPID 外存在其他持有者，或停服后仍存在任何持有者；
5. 远端 SHA 不一致、unit 指向错误路径、生产 gate 失败或数据目录出现未批准产物；
6. 需要使用 raw Polymarket REST/WS、手写 subscription、manual signing，或需要扩大 `3.1`/`2.1.2` 已批准的五项最小 patch 边界；
7. 需要删除整机 journal、本地既有备份或白名单之外对象；
8. 新 collector 部署前，本地旧 `data-collect-new-datasets-archive.timer` 仍未 disabled；该旧脚本依赖被取代 schema，不得继续处理新数据。旧归档脚本的极简替换仍是独立后续事项。
9. 旧主服务尚未停止前发生 abort，但本轮 digest-named staging 或最终路径上等于本轮 SHA 的新 binary 未按身份精确清除。
