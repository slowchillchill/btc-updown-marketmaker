# 训练采集全数据源可用性与连接生命周期修复需求

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应计划：`plan/data-collect-source-availability-lifecycle-repair-plan.md`
>
> 对应 changelog：`changelog/data-collect-source-availability-lifecycle-repair-changelog.md`
>
> 状态：`PHASE_9_COMPLETE_RTDS_PONG_A`。用户已于2026-07-17批准本文、对应计划及第4.7节SDK-first逐项patch矩阵，并授权按计划分阶段实施、验证、独立审查、commit和push；阶段0至阶段8均已完成实现、最终门禁与三路独立终审。用户于2026-07-19在香港线路15分钟wire验证后明确回复“批准 RTDS-PONG-A”；阶段9现已完成RTDS固定PING/no-PONG实现、定向与全量门禁、香港线路修复后15分钟SDK复测、官方crate provenance核验及两路独立终审，Critical/Important/Minor均为0。此前批准的`CT-B`、`D6-SCHEMA-R`、`M1-A / M2-A0 / M4-A-R / M5-N/A / M6-A+ / M7-A-R`及修订后的`M3-A-R`实现语义继续有效；M8-A旧UTC边界首次启用候选已由用户2026-07-18批准的独立直接长期部署事项取代。本文只提供实现 authority；用户2026-07-19后续批准的production release构建、精确current-day删除、部署与远端服务操作只以`demand/data-collect-rtds-pong-a-reset-deployment-demand.md`及对应计划/changelog为authority。

> 2026-07-22窄authority同步：已批准的`demand/data-collect-polymarket-resolution-followup-demand.md`只对固定pending-resolution identity的E+3..E+8 exact-slug查询、每个业务slot最多一条metadata v3 attempt row，以及旧market assets保留到terminal claim后的bounded drain或budget完成，建立本文的有界例外。desired asset set因此扩展为current + prefetched next + resolution_pending旧market assets；current/next discovery、普通availability、manifest schema/artifact及本文其他生命周期合同不变。用户已后续授权按专项计划执行阶段1至阶段4及每阶段commit/push；该授权不重开本文其他已完成阶段，也不授权production部署、服务操作或数据删除。
>
> 2026-07-25窄authority supersede：用户批准`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`。上述唯一业务slot例外在activation前仍由current source v2 + metadata v3承载；未来source v3 + metadata v4通过SDK070总计划阶段3原子activation后，同一五row-kind/attempt例外由metadata v4继承，并只追加精确nullable WS claimant pair。metadata v3 schema/version立即冻结且activation后只读；不新增artifact、sidecar、第二writer、dual-write或普通transport持久化。本次只同步文档，不授权Rust/schema/test实施或activation。
>
> 2026-07-25 SDK身份supersede：本事项以官方0.6.0和`0.6.0-p1`实施的阶段事实保持历史；其“采用更新release必须另建SDK版本升级决策门”已由独立`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`满足。当前Stage1 worktree使用官方0.7.0底座的唯一`third_party/polymarket_client_sdk_v2-0.7.0-p1`，版本、Cargo/lock、路径与provenance以SDK070三件套为authority；本事项第4.7节的四路径patch语义、SDK-first、唯一owner/no-shadow及heartbeat/teardown合同继续有效。SDK070 Stage1独立审查已为`Critical 0 / Important 0 / Minor 0 / PASS`且用户已另行批准发布，其闭合以承载记录的commit成功推送且远端SHA与本地HEAD/upstream一致为生效条件；本同步不修改本事项已完成生产行为。
>
> 2026-07-26 SDK070 Stage5窄authority supersede：本文第2.4节与第3.1节对
> `health.rs`及其最小衔接的描述只保留为当时实施基线。后续真实consumer审计证明
> `RetryPolicy/plan_retry_trace`、circuit registry、`ReconnectGate`及三个专属合同
> 没有接入production；用户批准`U06-A`后已删除该shadow model。current transport
> 恢复authority是各source-local supervisor的有界单次等待、capped+jitter backoff
> 与无限外层总attempt，business freshness不成为transport门；该删除不改变本文已
> 验收的source availability、heartbeat、SDK-first、manifest或完整性边界，也不授权
> production、远端、部署或数据操作。
>
> 2026-07-26 RTDS业务静默窄authority supersede：用户批准`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`。本文“业务静默不控制网络”继续适用于CLOB、Gamma、Binance及RTDS首次fresh之前；唯一例外是RTDS source task曾收到合格fresh后，按`300s steady + 60s后到monotonic first-fresh + cold-start disabled + 1s × 2 ±20%/300s cap/unlimited`共同合同，由同一owner串行break-before-make替换整个runtime。它不恢复同socketrefresh、one-shot `request_reconnect`、generation预算、第二owner/socket、gap/lag/告警或进程退出。当前代码只有training拥有真实RTDS adapter；阶段1已在training完成实现、收窄验证、三路独立终审及分离commit/push。future paper/live真实RTDS adapter由独立事项实施，但必须继承同一恢复合同。其他source不变。

## 一、目标

修复训练数据采集器把外部业务消息静默、stale、暂时不可达或服务端暂时没有数据错误升级为订阅操作、连接操作、`source_gaps`、告警、source 退出或整个 collector 退出的行为。

修复后的 `data-collect-training-collector` 必须满足：

1. Polymarket market/metadata、RTDS、Binance depth/snapshot与Binance aggTrade中任意一个、多个或全部外部I/O路径不可达或业务流静默时，collector都在同一PID内持续运行，健康producer继续采集和写盘。六条外部I/O路径全部不可达时进程保持有直接证据的degraded并逐路恢复；Gamma已返回有效稳定市场、depth已取得合法初始snapshot且四条WebSocket按协议存活但均无合格业务事件时，进程保持active/idle。本事项的availability摘要只使用单一collection coverage并复用六个既有artifact/source counter，其他既有manifest字段及职责保持不变；nullable last-qualifying只能从typed raw或source-local运行事实因果取得，不是新增manifest字段。除2026-07-22批准的固定resolution-followup业务slot typed attempt窄例外外，当前transport事实只存在于source-local运行态及有界operator日志，不进入daily manifest，也不得虚构degraded或silence状态转换。
2. 业务消息静默、持续 stale、暂时无候选市场、DNS/TCP/TLS/WebSocket/HTTP 不可达、协议明确要求PONG时的heartbeat timeout、rate limit 和服务端关闭是外部availability与下游missingness因果基础，不得仅因持续时间或恢复次数升级为confirmed gap、异常告警、run failure、source退出、collector退出或systemd新PID；RTDS没有文本PONG不得被归入heartbeat timeout，missingness mask只由后续`ObservationContract`在求值时派生。
3. 业务静默原则上不控制网络：不得仅因没有业务消息而重复subscribe/unsubscribe、调用one-shot reconnect、主动切换同一runtime的connection generation或结束source/collector。唯一窄例外是上述已批准RTDS共同合同允许在此前已收到fresh后，由同一owner串行完整替换runtime；首次fresh前以及CLOB、Gamma、Binance继续保持原行为。
4. 真实 transport 故障在同一进程的 source-local transport owner 内持续恢复；单次等待、退避、并发和资源必须有界，但 availability episode 的总时长与总尝试次数不得成为进程存活预算。
5. 各源共享同一结果合同和错误分类，但保留各自协议要求；不得把 RTDS 的 subscription/reconnect 实现机械复制到 CLOB 或 Binance。
6. SDK 或本地 queue 的确定性丢失、具有官方连续性合同的confirmed sequence hole（例如Binance depth）、writer/manifest/atomic publish、路径、磁盘、资源硬边界、parser实现故障和内部不变量故障不属于 availability 豁免，继续按既有受影响范围 quarantine 或 fail closed。aggTrade ID数值前跳不属于confirmed hole；第4.5节已批准且能够完整形成并接纳v6 evidence的普通外部aggTrade应用层无效输入是模型必须能够重放的source evidence，不再单凭payload/schema无效令source或collector退出。
7. collector必须保留typed raw接收时间、真实数据空档、last qualifying time与中性有界运行诊断，使后续训练能够从真实/模拟missingness学习暂停新增交易、取消未成交BUY订单，以及通过BUY对侧outcome中和已有方向风险；本事项的availability持久化只新增第4.2节单一coverage并复用既有source counters，其他manifest字段保持既有职责，manifest整体不得被伪称为transport episode event tape，collector和data-pipeline也不得替模型生成这些策略动作。

“同一进程内持续恢复”不等于关闭旧进程再由 systemd 启动新进程。进程重启只允许来自真实进程级完整性故障、operator 明确操作或后续单独批准的部署流程，不能作为单个外部源的正常恢复机制。

## 二、已核实事实

### 2.1 当前训练采集器的数据源清单

当前生产 collector 有四个逻辑 source task：

| 逻辑 source task | 产生的事实/产物 | 本事项边界 |
|---|---|---|
| Polymarket market source | CLOB market 事件与 Gamma market metadata | 包含CLOB动态desired-state actor与Gamma discovery；业务静默不控制subscription或连接 |
| RTDS source | `crypto_prices_chainlink` / `btc/usd` | 包含 desired subscription、SDK transport 与 fresh/stale 消费 |
| Binance depth source | depth WebSocket 与 REST snapshot | 包含 transport liveness、snapshot stitch 与 update-id continuity |
| Binance aggTrade source | aggTrade WebSocket | 包含业务静默、transport recovery、原始ID观测与invalid evidence；不做REST补齐或aggregate-trade-id continuity控制 |

manifest 中可以表现为六类 source artifact，但它们不是六个独立生命周期 owner。四个owner实际包含六条外部I/O路径：Polymarket owner包含Gamma HTTP与CLOB WebSocket，RTDS owner包含RTDS WebSocket，Binance depth owner包含snapshot HTTP与depth WebSocket，Binance aggTrade owner包含aggTrade WebSocket。训练 collector 当前没有 Polymarket CLOB user-channel source；本文不得虚构或实现该数据源。

### 2.2 官方合同边界

截至 2026-07-16：

- Polymarket 官方 [CLOB WebSocket 文档](https://docs.polymarket.com/market-data/websocket/overview) 允许在已有连接上动态增删订阅，并规定每 10 秒发送 PING；没有承诺每个 asset 必须在 30 秒或 60 秒内产生业务事件，也没有规定业务静默必须重订阅或重连。
- Polymarket 官方 [RTDS 文档](https://docs.polymarket.com/market-data/websocket/rtds) 的客户端指引允许不断线修改订阅，规定每5秒发送PING，并说明connection error会触发automatic reconnection attempts；文档没有要求或承诺文本/控制PONG。2026-07-19经香港SOCKS5h线路完成的15分钟wire验证在同一connection持续收到业务更新、固定发送181次大写文本PING，但文本/控制PONG均为0且服务端没有Close；因此no-PONG不能被本项目自行升级为connection error。官方也没有把业务静默或stale定义为connection error，没有重复相同subscribe的确认语义或3秒/6秒恢复SLA。
- Binance 官方 [WebSocket Streams 文档](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md) 把 aggTrade 定义为事件驱动流，只把`a`定义为Aggregate Trade ID，没有规定逐一`+1`、缺洞判定或REST恢复算法；同一文档却为depth snapshot/update-id stitch明确规定连续性与重建流程。单连接TCP/WebSocket有序只证明已经发送的字节按发送顺序交付，不证明服务端生成/发送了全部业务事件，也不跨连接generation。Binance还明确规定单连接最长24小时并可能发送`serverShutdown`，因此transport重连必须保留；`GET /api/v3/aggTrades`是Database历史查询能力，不是官方WS gap-recovery合同。

因此，业务 freshness、subscription state 与 transport liveness 是三种不同状态，不能用一个业务消息 deadline 同时控制。

### 2.3 修复前历史缺陷基线（截至2026-07-16）

下表记录本事项开始前的已核实行为，用于解释修复来源；阶段0--6完成后的当前实现状态以本事项changelog及对应阶段commit为准。

| 数据源 | 修复前行为 | 已确认问题 |
|---|---|---|
| RTDS | 60 秒无 fresh 后调用 generation-aware `request_reconnect()`；观察两个新 generation、3 秒窗口/12 秒总期限后退出 | 业务静默错误控制 transport，并最终结束 RTDS source 与 collector |
| CLOB market | 单 asset 首条 30 秒、后续 60 秒静默后 unsubscribe/subscribe；只允许两次，第三次返回错误 | 与 RTDS 同类的有限业务静默预算；修复前测试还明确要求第三次强制进程重启 |
| Binance aggTrade | 60 秒无 aggTrade 即断线重连、增加 lag 并写 gap；收到 PING 不重置业务 deadline | 通常不会最终停机，但把正常无成交误判为 lag/gap 并制造无必要连接 churn |
| Binance depth | 约 30 秒完全无入站 frame 后重连并重新 snapshot；update-id hole 后重建订单簿 | 恢复机制基本正确，但 transport availability 被写成 gap；必须与“有 PING、无业务更新”区分 |
| Gamma metadata | 网络/发现失败通常经外层 supervisor 继续恢复；暂时无候选可能形成错误 gap 语义 | 持续运行基本具备，但 availability/coverage 分类需要统一 |

顶层 coordinator 当前把任一 source task 在运行 deadline 前结束视为 `source_task_ended_unexpectedly` 并终止 collector。这个保护本身不是缺陷：recoverable availability 必须在 source 内部持续等待/恢复，真正的本地完整性错误仍应返回顶层；不得通过让顶层吞掉所有 source 错误来实现“持续运行”。

### 2.4 本事项实施时的 health 抽象（历史基线，已由Stage5 supersede）

本事项实施时，`data-collect/src/health.rs`已有`SourceHealthEvent`、
`SourceHealthRegistry`、`ReconnectGate`和有限retry trace，但它没有完整接入
production `live_runtime`，而且`CircuitState::Exhausted`、固定
`max_attempts/max_elapsed`及部分`CoverageVerdict`仍表达旧有限恢复合同。

本事项当时必须优先复用并最小修正已有health类型，不能另建第二套
health/reconnect控制面；但也不能因为抽象测试通过就宣称production source
lifecycle满足本需求。current终态由文首2026-07-26 overlay接管：该未接production
的shadow model已删除，真实source-local owner保持。

### 2.5 原事项pinned基线与当前SDK身份

本事项开始和阶段0--9实施时通过`[patch.crates-io]`使用官方0.6.0底座的
`third_party/polymarket_client_sdk_v2-0.6.0-p1`。这是本事项历史scope与验证身份，
不能回写成当时已经使用0.7.0。

独立SDK070升级门现已在当前Stage1 worktree从官方0.7.0 crate全新建立唯一
`third_party/polymarket_client_sdk_v2-0.7.0-p1`，并完成Cargo/lock切换、旧tracked
vendor删除与离线runtime矩阵。相对官方0.7.0的源码差异仍恰好是第4.7节四个批准
路径；因此当前版本/path/provenance由SDK070管理，本事项继续管理这些patch的业务
语义，不能整体恢复官方原版或建立shadow fallback。

本事项实施前核对的上游入口包括：`SimpleParser::Default`
[PR #81](https://github.com/Polymarket/rs-clob-client-v2/pull/81)、heartbeat传播
[Issue #60](https://github.com/Polymarket/rs-clob-client-v2/issues/60) /
[PR #63](https://github.com/Polymarket/rs-clob-client-v2/pull/63)，以及连接任务回收
[Issue #39](https://github.com/Polymarket/rs-clob-client-v2/issues/39) /
[PR #40](https://github.com/Polymarket/rs-clob-client-v2/pull/40)。这些链接继续只是
历史审查入口，不冻结未来merge状态，也不取代SDK070的0.7.0固定provenance。

## 三、范围

### 3.1 本事项包含

- 四个训练 source task 的 availability、业务静默、transport recovery 和 terminal error 边界；
- 顶层 source-task 生命周期分类，不改变真正 terminal task 结束必须被发现的原则；
- 本事项实施阶段对production `live_runtime`与当时`health.rs`的最小衔接
  （current终态已由文首2026-07-26 Stage5 overlay supersede）；
- RTDS desired subscription、CLOB market-lifecycle desired assets、Binance depth双artifact原始观察/可用book分层与snapshot stitch、Binance aggTrade WebSocket因果接纳及Gamma discovery的源特定恢复；
- pinned 官方 Rust SDK 的逐项 patch 审计、删除失去依据的 one-shot reconnect 能力，以及 heartbeat/teardown 的最小修复；
- 每日唯一manifest中的顶层、单owner、版本化`collection_day_coverage`，并保持六个既有source counter只记录artifact事实与既有操作计数；
- 单源、多源、全部源退化、恢复、停止、完整性负例和资源有界测试；
- 既有`binance_aggtrade` artifact内部升级为v6判别联合，保存普通外部invalid input、原始接收顺序、collector接纳顺序及对应realtime handoff evidence合同；
- 对旧 demand/plan 中相反恢复语义的精确同步。

### 3.2 本事项不包含

- 新增 CLOB user channel 或其他新外部数据源；
- 除第4.5节已批准的`binance_aggtrade_v6`判别联合及其handoff evidence schema、第4.6节已批准的`binance_depth_snapshot_v3`/`binance_depth_update_v4`因果坐标扩展、2026-07-22独立批准的`polymarket_market_metadata_v3` resolution-followup五类row与每slot唯一attempt合同，以及2026-07-25批准的future `polymarket_market_metadata_v4`不可变前缀与两个nullable WS claimant字段外，修改RTDS/CLOB/Binance/Polymarket业务payload schema、价格含义、dedupe identity或Parquet行布局；这些例外都复用各自既有artifact、writer和manifest mapping，不新增artifact、quarantine sidecar、第二写入路径或dual-write；第4.2节对每日manifest schema的有界扩展仍属于本事项，其他新artifact/typed raw/consumer schema必须停在新决策门；
- 在 `data-pipeline`、model-train、offline-evaluation、paper、live 或 execution 中实现模型策略；
- 决定或实现`data-pipeline`/Model Runtime的`DecisionWakeEvent`、deadline数量/阈值、离散`next_wake_bucket`、timer排序/抢占、最小/最大间隔、变量持续时间Reward/SMDP归因或模型调度策略；这些属于整体审核中的`demand/rebuild-core-demand.md`，不属于collector修复；
- fixed/periodic decision tick、周期性合成 market row、用 PING/PONG 填充业务 coverage，或用 stale 值冒充 current；
- 为Polymarket新增raw WebSocket/manual protocol、第二个官方SDK session、第二个receiver/actor、shadow client或旁路官方SDK；Binance阶段6继续复用现有`reqwest + tokio-tungstenite + Tokio + Serde`及唯一direct WebSocket owner，不引入Binance SDK、Barter、Nautilus或其他market-data engine，不新增或替换第二raw-WS实现/socket owner；第三方实现只能作为只读研究或fixture参考，不能成为生产协议authority；
- 生产构建、部署、systemd 修改、远端服务操作、数据清理或失败日期重采。

## 四、需求

### 4.1 共同分类与长期运行合同

1. 每个 source 必须明确区分：
   - `business availability`：无业务消息、stale、空候选、当前无市场事实；
   - `transport availability`：DNS/TCP/TLS/HTTP/WebSocket connect/read/write/close、heartbeat、rate limit、server shutdown；
   - `confirmed integrity failure`：SDK/local lag、序列洞、payload conflict、parser实现或本地typed-schema不变量、writer/manifest、确定性 drop。第4.5节可完整保存的普通外部aggTrade应用层invalid input是source evidence，不在这一terminal分类中。
2. 前两类只形成中性 availability/recovery 事实，不得仅因持续时间或次数进入 `source_gaps`、异常告警、source terminal、collector terminal 或 systemd restart。
3. source-local 恢复允许每次 connect/request/read/backoff 有明确 timeout 和最大 backoff；外层循环必须可无限继续且无 tight loop，总 attempt、总 elapsed 和 circuit exhausted 不得结束 source。
4. operator stop、测试 deadline 和协调 shutdown 必须能中断永久离线、永久静默、rate-limit backoff 和连接尝试；“持续运行”不得造成无法停止。
5. 任一source退化不得阻塞其他source的队列、writer或market lifecycle；全部源退化时coordinator仍存活。source-local transport恢复状态与有界operator诊断不得被冒充模型输入或完整episode tape，每日manifest只按第4.2节保存单一collector observation-eligibility coverage及既有source counters。
6. 真正 terminal source error 仍返回顶层并触发现有 fail-closed coordinator；不得把parser实现故障、v6 evidence编码/接纳失败、typed-schema不变量、writer或内部不变量错误放进无限网络重试。普通外部aggTrade invalid input必须按第4.5节保存并继续，不得再用笼统的`parser/schema`标签把两者混为一类。

### 4.2 中性 availability/missingness 证据与 manifest coverage

1. source-local transport owner仍必须根据真实connect/read/write/HTTP/WebSocket/heartbeat结果区分连接中、重试、恢复与terminal完整性错误；这些状态只控制对应协议恢复并可形成有界、去重的operator日志，不得因持续时间、累计尝试、日志失败或既有诊断计数饱和结束source/collector。固定resolution-followup业务slot可按独立已批准需求每slot写至多一条metadata attempt row：activation前写current metadata v3，source v3 + metadata v4原子activation后写v4，不得双写；仍不得在daily manifest新增transport scope、reason矩阵、current/open摘要或episode。除此之外现有reconnect/lag/drop等source counter保持既有字段和职责。
2. 纯业务静默本身没有新的协议事件，也没有官方cadence可定义开始点。CLOB、aggTrade和depth不得新增或保留business-silence timer、离散silence-start事件或阈值；RTDS只允许专项冻结的source-task内monotonic `300s/60s` watchdog，不持久化silence-start或transport episode。collector继续只保留typed raw接收时间、既有source counter中的首末接收事实、可因果取得的last qualifying business/fresh receive time和本节冻结的collection-day coverage。
3. 每个UTC日仍只有一个由唯一ManifestOwner原子维护的`manifest.json`。顶层schema继续是`data_collect_daily_manifest_v3`，只新增一个直接嵌套、独立版本化的`collection_day_coverage`对象；不得新增通用`extensions`容器、health/gap/reconnect receipt、sidecar、逐事件availability日志或把coverage复制进六个`source_counters`。
4. `collection_day_coverage.schema_version`固定为`data_collect_collection_day_coverage_v1`。该对象只表达四个mandatory logical producer supervisor、六个writer及coordinator-owned bounded event queue/唯一receiver/ingress处理路径已经全部接纳且仍共同受collector监督的观察资格交集，不表达transport、subscription或业务健康，也不承诺任一artifact应按某cadence产生行。实现前必须在任何网络动作前验证且测试冻结以下映射，但不在每个manifest重复保存：`polymarket -> polymarket_market, polymarket_market_metadata`；`polymarket_rtds -> polymarket_rtds_chainlink_btc_usd`；`binance_depth -> binance_depth_snapshot, binance_depth_update`；`binance_aggtrade -> binance_aggtrade`。未来若出现optional producer、producer独立暂停/重启或任一mandatory成员terminal后collector继续运行，必须升级嵌套schema并重新取得用户批准。
5. `collection_day_coverage`只含：封闭`state = not_started|active|ended_known|unclean_end_unknown`、nullable有符号64位Unix epoch毫秒`first_started_utc_ms`、`last_started_utc_ms`、`ended_utc_ms`，以及布尔值`reactivated_within_day`。该布尔值只回答“在首次成功activation之后，本UTC日是否又成功原子持久化过至少一次activation”，不统计activation或unclean事故次数。不变量为：
   - `not_started`：三个时间均为null，`reactivated_within_day=false`；
   - `active`：first/last非null、end为null且`first <= last`；
   - `ended_known`：first/last/end非null且`first <= last <= end`；
   - `unclean_end_unknown`：first/last非null、end为null且`first <= last`；
   - `first_started_utc_ms`与`last_started_utc_ms`必须位于对应UTC日的`[day_start, day_end)`，`ended_utc_ms`必须位于`[day_start, day_end]`，只有准确day-end的`ended_utc_ms`允许等于下一日`00:00:00Z`。首次activation写`false`；任一同日后续activation原子提交时写`true`且当日保持单调为`true`。运行中标记unclean、known end、prior-day reconcile、普通load、spawn、ready或重复failure report均不得改变该布尔值。
6. 在读取或修改current-day manifest前，collector必须取得并在整个runtime期间持有dataset-root级OS-backed独占实例lease；相同identity不等于旧进程已退出，不能取得lease或lease丢失属于本地完整性冲突并fail closed，禁止自研stale-PID猜测或并发恢复。随后启动顺序固定为：coordinator同步构造bounded event queue、取得唯一event receiver，并完成进入唯一ingress处理循环所需的现有本地状态初始化；该queue/receiver/ingress路径是coordinator-owned同步前置条件，不是独立spawn task。再spawn唯一ManifestOwner、六个writer worker与四个producer wrapper。每个writer只在worker及command receiver完成内部初始化后发送ready ack；每个producer wrapper只在接管source future/control channel、确认尚未poll source future并开始等待共同start gate后发送ready ack。上述ingress前置条件与十个writer/producer ready都成立后，唯一ManifestOwner才可发送一次不重试的activation mutation，原子持久化`active`并返回ack；只有收到ack后才释放producer网络poll并进入正常ingress循环。active atomic publish必须区分三个结果：临时文件写入/`fsync`或最终`rename`前失败为`NotPublished`，最终manifest字节与digest必须保持不变；`rename`成功但父目录`fsync`失败为`PublishedDurabilityUncertain`，最终路径在崩溃后的old/new结果不可安全预言，当前进程必须fail closed、收敛资源、禁止ack、网络/raw release、重试和rollback，下个取得独占lease的进程只能读取实际manifest并按第9项既有`not_started|active`状态恢复；父目录`fsync`成功才是`DurablyCommitted`并允许activation ack。写入成功但ack丢失时本进程同样fail closed，不得重发activation；下个进程按第9项读取持久状态。任何lease、前置、ready或`NotPublished`失败都必须在网络尚未开始的条件下fail closed、收敛已经spawn的资源并保持既有持久状态；`PublishedDurabilityUncertain`不得冒充旧状态未变。`active`因此证明全部mandatory路径已被接纳，即使一个或全部artifact始终零行。
7. 每个active UTC day/activation generation只有一个owner持有的authoritative共同gate。producer、writer、coordinator ingress、operator stop与rollover对同一generation竞争时，第一次可靠compare-and-set只关闭raw intake并冻结`known-close candidate = (generation, close_id, cause, local/logical time)`，在全部mandatory成员完成fence/rendezvous前不得把`ended_known`视为不可变最终状态；后到的可靠known-close candidate必须幂等且不得把候选end向后移动。任一在该候选最终确认前发现、且无法由该候选对应的已完成mandatory boundary fence证明发生于旧日coverage结束之后的非协调性panic/abort/join failure、未知先后终止或未可靠持久化结束，必须以`unclean-dominates`覆盖候选：最终写`unclean_end_unknown`并清空end；已由boundary fence证明只影响下一日activation资格的failure按第8项处理，不能污染旧日也不能制造新日active。协调abort/cancel豁免必须有task级因果证据，不能只比较generation或cause字符串：共同gate关闭时由coordinator生成进程内唯一`close_id`；协作停止只有本次close创建的精确oneshot reply成功且wrapper按预期正常返回才成立；强制abort必须先登记一次性`(task identity, generation, close_id)` permit，随后同一handle返回`JoinError::is_cancelled()`并精确消费permit才成立。`JoinError::is_panic()`、无/错permit、ack丢失或不匹配、以及无法证明因果关系的退出始终不得套用豁免，即使发生在stop/abort发出之后；source返回`Err`继续进入可靠terminal或unclean判定。`close_id`与permit只存在于runtime内存，不写manifest、sidecar或通用registry。双producer return、terminal对stop/rollover、先panic后stop、stop已发但cancel生效前panic、协调abort和迟到命令都必须验证该优先级。
8. 每条raw event必须在collector receive/final enqueue临界区持有并校验共同UTC day/generation permit；control/barrier消息必须绕开raw intake fence以免死锁。本节“已接纳”仅指已经取得对应raw-day permit，并把ownership转移到既有bounded queue admission的event；共同inflight reservation只提供内存上限，不单独构成admission。新日activation前进入`DeferredRawBatch`的event尚无新日permit，只是有界pre-admission staging，不分配collector admission坐标；正常rollover必须在新日activation成功、flush取得新日permit和queue ownership后才按staging顺序冻结admission。仍只存在于depth generation-local pending buffer的update同样不是已接纳event。没有pending exit的正常rollover稳定顺序为：boundary关闭旧generation raw intake并冻结boundary known candidate；drain旧日已接纳事件；收齐四producer barrier和六writer finalize ack并确认无unclean-dominates；在同一次旧日最终写入中把nested coverage置`ended_known(end=boundary)`并关闭旧日顶层manifest；再创建新日`active(first=last=boundary, reactivated_within_day=false)`；owner ack后才释放新generation。任何中间失败都不得发布新日事件。stop/terminal与逾期boundary竞争时，gate owner必须在generation切换前原子保存并只消费一次独立`PendingExit(candidate id, original generation, classification, observed time)`，关闭raw intake，不能让它退化成旧generation迟到命令：
   - 若可靠stop/terminal在旧日final-close atomic commit前被观察且candidate time已达到boundary，完成mandatory fence后旧日写`ended_known(end=boundary)`并正常关闭；普通运行只可为新日创建顶层`open`加nested `not_started`，不得写瞬时`active`、不得释放新raw permit，随后消费PendingExit并结束collector。M8的`no-new-day-activation`部署路径连新日`not_started` manifest也不得创建。
   - 旧日final-close atomic commit是旧日不可逆归属切点。在该commit前发现且无法由mandatory fence排除的非协调panic/abort/join failure必须unclean-dominates：旧日写`unclean_end_unknown`并按unclean路径关闭，不创建新日manifest。若旧日关闭后仍收到声称发生在其mandatory fence之前的failure report，说明fence合同实现错误，必须按本地完整性fail closed，禁止重写closed manifest。
   - 旧日final-close已经提交、但新日atomic activation commit尚未成立时才观察到的任何可靠exit或unclean failure，都不得改写旧日；尚未创建的新日manifest可保持不存在，已经原子创建的只能保持顶层`open`加nested `not_started`。collector退出，不改变`reactivated_within_day`，不释放raw permit。
   - 新日atomic activation commit一旦成功，无论ack是否送达，持久状态与`reactivated_within_day`都已归属新generation且不得回滚；随后观察到的可靠stop/terminal或unclean failure分别形成该日known candidate或unknown tail。activation ack只控制是否释放网络/raw permit：ack丢失时不得释放、当前进程fail closed，下一进程读取遗留`active`并按第9项执行新的activation；ack成功后才可正常release。当前日顶层保持`open`以允许同identity恢复。
   PendingExit、boundary与source completion无论poll顺序如何都必须产生相同manifest digest/disposition；`B-1ms`事件只能drain进旧日，`B`及以后事件在新日activation ack前不得进入任一writer。协调stop/test deadline及可靠terminal仍需收齐mandatory termination报告并确认无unclean才持久化known end；wrapper只可提交其首次观察future/worker返回的本地时刻，不得冒充外部错误真实发生时刻或coordinator随后最多约250ms的发现时刻。进程或ManifestOwner先失败时只能由后续recovery形成unknown tail。
9. recovery必须在第6项独占实例lease与全部ready成立后，由唯一ManifestOwner执行一次原子activation mutation；generic load/reconcile及owner内存插入不得改变`reactivated_within_day`。设本次start为当前UTC日内的`s`，时钟倒退不得钳制或伪造而应fail closed，current-day顶层必须为`open`且identity完全匹配：
   - `not_started -> active`：`first=last=s, end=null, reactivated_within_day=false`；
   - 遗留`active -> active`：保留first，令`last=s, end=null, reactivated_within_day=true`，表示本UTC日发生了后续activation；
   - `unclean_end_unknown -> active`：保留first，令`last=s, end=null, reactivated_within_day=true`；
   - `ended_known -> active`：保留first，令`last=s`并清空end，令`reactivated_within_day=true`。
   除`not_started`外必须有`s >= old_last`，`ended_known -> active`还必须有`s >= old_end`；同日任何转换不得覆盖first，且一旦为`true`不得恢复为`false`。current-day顶层`closed`、identity不匹配、相同新identity但缺v1对象、malformed或unknown nested都必须拒绝。运行中首次把当前日`active`写成`unclean_end_unknown`时只更新state/end并保持顶层`open`；重复failure report幂等。prior-day顶层`open`的reconciliation必须state-aware：nested `active`转unclean且不改变布尔值，既有`unclean_end_unknown`保持对象且只确保unclean gap/closure，二者使用`unclean_exit_recovery`；`not_started`、`ended_known`和legacy缺coverage保持nested语义，分别使用中性`reconciled_collection_not_started`、`reconciled_collection_ended_known`、`reconciled_legacy_coverage_unknown`且不得新增unclean gap；顶层已closed只读，新v1出现`closed + nested active`属于状态完整性错误。
10. 该摘要不保存每次本地run segment。只要`reactivated_within_day=true`，最后一次`last_started_utc_ms`之前无法证明连续coverage的区间只可作为finalized artifact的replay-eligibility隔离边界：typed raw事实仍保留，但该区间不得生成要求training/offline/paper/live parity的历史模型Observation；不得用日终才知道的restart事实回写较早决策时点的模型可见`unknown_not_observed`。restart之后的新求值可因果使用当前last start后的coverage。无逐producer coverage或共同liveness checkpoint时，任何`unclean_end_unknown`日的共同coverage最后可证明边界只能是`last_started_utc_ms`；某一健康source更晚的last recv不得证明其他mandatory成员仍存活。没有官方cadence、sequence或本地lag证据时，不得推断具体缺失条数、silence-start时刻或confirmed gap。
11. legacy `data_collect_daily_manifest_v3`缺少`collection_day_coverage`时必须可读，coverage定义为unknown且禁止猜测、回填或改写旧前缀；新binary自己创建或以相同identity继续写入的manifest必须包含合法v1对象，对象存在但版本未知、字段非法或违反第5、9项不变量时属于完整性错误并fail closed。reconcile、finalized-file merge、operational counters、continuity、gaps、barrier state、close和recovery的每次ManifestOwner mutation都必须原样保留该对象。current legacy manifest与新binary identity不同时继续由既有identity gate拒绝，不得以“legacy缺对象可读”绕过；current-day顶层open/closed与nested四态兼容矩阵、prior-day state-aware closure及dataset独占lease必须共同验证。
12. `M8-A`是本专项当时冻结的未来部署候选，现已由用户2026-07-18批准的`demand/data-collect-training-collector-direct-deployment-demand.md`在首次正式冷启动范围内取代：多日停服、旧PID/lease已退出且当前UTC日无任何collector产物时，可在任意时刻直接启动v1 binary，并从第一次启动起使用唯一`Restart=on-failure` unit；不需要`Restart=no`、独立gate、数据删除或等待UTC边界。prior legacy open日仍只按第9项state-aware recovery中性关闭。发现任何当前日产物必须原样保留并停止；future coverage-aware升级、回滚、same-day不同identity takeover或identity migration仍须按当时事实另行批准。
13. 中性availability不进入`source_gaps`，不增加业务`lag_events`，不触发异常告警，也不使artifact因外部coverage不足自动失败；真实SDK`Lagged(n)`、bounded queue drop及Binance depth confirmed update-id hole等具有直接合同证据的确定性缺失仍保留为明确gap/continuity evidence。aggTrade ID数值前跳只是原始观测，不进入该集合。固定mandatory mapping违约、coverage非法状态或版本、以及atomic manifest write失败属于本地完整性边界并fail closed；legacy v3缺对象是已定义unknown，不是完整性失败。
14. coverage只在已有真实事件、bootstrap或稀疏`DecisionWakeEvent`求值时参与后续`ObservationContract`生成known/unknown mask；它自身不创建模型trigger、wake或synthetic row。本事项不决定下游何时调用模型。核心需求已把有界离散`next_wake_bucket`批准为目标候选并保留确定性age-crossing对照基线，两者都不得反向控制subscription/transport。当前不建设精确transport episode artifact；只有未来出现经证明的cause-specific模型需求且现有事实不足时，才可另建typed availability artifact/schema与新的`ObservationContract`决策门。暂停、`MODEL_NO_SUBMIT`、取消未成交BUY和BUY对侧outcome风险中和均由模型或bundle状态机决定，不得被实现或描述成SELL、真实减仓或释放现金。

### 4.3 RTDS

1. RTDS source任意时刻只持有一个active `RtdsSdkRuntime`，只通过latest-value desired-state接口保存一次完整`crypto_prices_chainlink` / `btc/usd` subscription；初始未连接时desired也必须保留。真实transport自然恢复仍由当前runtime负责；只有专项冻结的business条件成立时，同一owner才可在旧runtime真实完成后创建新runtime。
2. 删除 60 秒 freshness timeout 驱动的 `request_reconnect()`、两个 generation 上限、3 秒观察窗、12 秒总期限、相应 lag/gap/告警和 fatal 分支。
3. 业务静默、持续stale、没有fresh payload不得调用subscribe/unsubscribe、改变desired revision或调用one-shot generation切换。source task从未收到fresh时不运行business watchdog；曾收到fresh后，最后fresh连续`300s`且当前generation仍`Connected + latest desired local applied`时可立即提交第一次完整runtime replacement。新runtime/generation以上述两个观测的后到monotonic时刻为anchor等待`60s`，仍无fresh则先等待`1s × 2 ±20%`、单次最长`300s`的outer backoff再完整替换；fresh重置，总尝试不限。等待或提交前同一socket到达的合法fresh必须被消费并取消business动作。
4. 生产RTDS配置固定为`heartbeat_interval=5s`、`require_text_pong=false`和有界max backoff；共享配置结构中的`heartbeat_timeout=15s`只保留SDK默认兼容值，在该模式下不得启动PONG等待或参与连接判死。首条大写文本`PING`沿用Tokio interval首tick在连接建立后立即发送；每次必须由唯一connection owner完成WebSocket write后确认，确认前不得创建第二个PING，确认后重置完整5秒周期，写延迟不得触发catch-up burst。SDK transport owner 的总 reconnect attempts 必须为无限。启动不得以旧 20 秒 connected wait 或 3 秒 subscription-applied wait 作为 source 存活门。
5. 每个真实新generation及每个新runtime的初始connection都由唯一actor创建空subscription manager，并只应用一次最新完整filtered desired；不发送丢失`btc/usd` filter的unsubscribe，不复用旧generation applied acknowledgement。
6. stale payload继续按typed freshness事实落盘；stale本身不增加lag/gap、不直接触发或重置business watchdog。只有SDK `Lagged(n)`等确定性本地丢失进入完整性处理。
7. RTDS没有PONG、迟到PONG或主动收到PONG均不得改变PING cadence、关闭socket、切换generation或触发replay；真实WebSocket Close/EOF/read failure、subscription/outbound write failure或PING write failure继续作为recoverable transport exit，parser sticky fault继续terminal，最后一个manager sender关闭继续normal teardown且不重连。

### 4.4 Polymarket CLOB market 与 Gamma metadata

1. 删除 `MarketAssetFreshnessWatchdog`、首条30秒/后续60秒业务静默阈值、unsubscribe/subscribe恢复、两次resubscribe budget和第三次错误退出；不得把旧阈值保留为collector内的中性silence detector。后续`source_information_age`和availability mask由版本化`ObservationContract`计算，模型稀疏唤醒由独立`DecisionScheduleContract`处理；两者都不由collector选择阈值或生成timer。
2. CLOB desired asset set由current market、prefetched next market与全部resolution_pending旧market的assets并集驱动；terminal claim后仍保留对应pending/assets直到accepted-event bounded drain完成，只有drain后的terminal或budget完成才精确移除。只有desired assets实际变化时，现有唯一actor才执行dynamic additions/removals。
3. 自然 transport reconnect 后，每个新 generation replay 当前最新完整 desired asset set；业务静默不得制造 desired revision 或 generation churn。
4. CLOB 继续使用 `10s/20s` heartbeat。SDK 单次 connection loop 可以有限尝试后返回现有外层 source-local supervisor重建 runtime，但该外层总 cycle 和总 elapsed 不得耗尽 collector 存活预算。
5. Gamma DNS/HTTP/rate limit、暂时无候选、当前市场尚未出现或rollover discovery暂时失败只形成metadata availability；当前可用market数据与其他source继续运行。服务恢复后按恢复时当前时钟重新计算current/next discovery window，禁止继续追逐已过期slug，也不回放伪造metadata。唯一例外是目标market已在自身活跃窗内固定并验证identity且仍处于`market_runtime_role=resolution_pending`时，可按已批准需求对该exact slug执行E+3..E+8有界resolution follow-up；process restart还必须具有finalized durable v3 identity，且不得补做错过slot、历史backfill或第7次probe。
6. Gamma/SDK 返回无法验证的 schema、矛盾 market identity、asset mapping 或 lifecycle 不变量错误仍按既有受影响范围 fail closed，不得作为普通空结果吞掉。

### 4.5 Binance aggTrade

1. 删除 60 秒无 aggTrade 驱动的主动断线、`lag_events += 1`、`source_gaps` 和 freshness reconnect reason。业务静默时继续读取同一 WebSocket，并允许正常 PING/PONG。
2. aggTrade复用depth现有30秒“完全无任何入站frame”transport-liveness边界：任意合法业务或控制frame/PING都会重新武装该边界；有PING但无trade只是business idle，不控制网络；连续30秒完全无frame才形成中性transport availability并触发同PID reconnect。该数值依据Binance官方20秒server PING合同留出余量，不是trade cadence或coverage SLA。
3. WebSocket connect error、close、EOF、read/write/protocol error、server shutdown 与服务端连接生命周期结束可以触发同 PID source-local reconnect；总次数/总 elapsed 不得成为 source 退出预算。
4. 每条合法WebSocket aggTrade必须按真实接收顺序立即进入当前日有界queue admission，或在跨日activation未完成时进入有界pre-admission staging；相邻已观察`aggregate_trade_id`的数值关系不控制上述接收。正常rollover在新日permit和queue ownership成立后按staging顺序保存、handoff；受控stop/deadline先获胜时未取得新日permit的staging保持缺失。`100 -> 103`时`103`不得因ID关系等待、pending、重排或被后续数据回插，ID前跳不得产生confirmed gap、告警、主动重连、source退出或collector退出。
5. 生产aggTrade source不得调用`GET /api/v3/aggTrades`做实时补齐，也不得保留range retry、REST-only/merged新行、unresolved range、active continuity epoch或第二backfill task。连接切代窗口中没有实际收到的数据保持缺失；training、paper和live只消费各自因果时点真实可用且由同一合同编码的WebSocket事实。若未来另行批准事后完整行情、标签真相或审计数据集，必须与模型`ObservationContract`隔离并另建需求，不得回插本实时观察路径。
6. 长期无成交后同一 socket 到达的首条合法 aggTrade 必须正常消费，不经过为静默而制造的 reconnect。
7. 新writer把既有artifact内schema升级为`binance_aggtrade_v6`，使用封闭判别字段`record_kind=trade|invalid_input`。新`trade`行只允许`ingest_source=websocket`，保存官方原始`a`及既有WebSocket source-native字段；v6不得生产REST-only/merged行或非空REST ingress evidence。为读取已经形成的legacy布局，既有REST nullable列可以保留为兼容列，但新生产行必须全部为null且不得参与排序、恢复或控制。`invalid_input`行的成交ID、价格、数量、成交时间、maker及其他trade-only字段必须为null，禁止用0、空串、默认值或伪造行情填充。两种row共享同一artifact、小时分区、writer与manifest source mapping；不得增加sidecar、第七artifact或第二writer。
8. 每条上限内的完整外部invalid application input必须逐条、可逆地保存原始bytes及`payload_encoding`、真实接收时间、ingest transport、endpoint、原始长度、SHA-256、`capture_status=complete`和稳定有界的`invalid_reason_code`；连续相同坏帧不得hash去重或折叠为计数。raw bytes可在`source_native_payload_json`的版本化evidence envelope中编码，但必须能精确恢复，且不得把parser Display文本冒充原始输入。
9. v6必须区分source task、连接代次、原始接收顺序与模型因果可见顺序。每行保存非空`source_sequence_epoch_id`；该epoch在同一aggTrade source task跨socket reconnect和UTC rollover保持不变，进程/source task重启时更换。每次WebSocket成功连接时从checked-u64域把`connection_generation`递增一次，同一连接的全部trade/invalid携带同一非零generation，重连后严格增加。collector完整观察到每条WebSocket Text application frame后、任何JSON/schema parse前立即从同一checked-u64 source计数域冻结`websocket_ingress_sequence`。invalid消息不得去重；合法Unique/Conflict/OutsideWindow trade携带已冻结coordinate，合法Duplicate仍按既有结构去重、不产生typed/model evidence，但其真实到达已消耗的ingress sequence不得回收或重排，因此允许出现由既有`structural_dedupe_dropped_events`解释的编号空洞。generation或sequence溢出属于本地完整性terminal，禁止饱和、回绕或跨epoch伪称连续；ingress sequence空洞本身不得被解释成source消息缺失或模型事件。
10. 每个实际取得对应raw-day permit并进入现有有界collector queue ownership的v6 row还必须保存checked-u64 `collector_admission_sequence`与`collector_admission_time_utc_ms`；它们在permit与queue ownership都成立时冻结，是offline replay与realtime handoff的因果可见顺序及同毫秒tie-break。受预算约束但尚无新日permit的跨日deferred只属于pre-admission staging，不得提前分配这两个坐标；正常rollover flush时按staging顺序分配。`timestamp_received`继续表示原始输入完整收到的时间；合法WS trade不得因ID数值关系延迟接收。当前日的`trade 100 -> trade 103 -> invalid -> trade 104`必须以相同顺序进入admission与handoff；跨日序列必须先按相同真实顺序进入pre-admission staging，并且只在正常flush取得新日permit与queue ownership后按该顺序进入admission与handoff，受控截止先获胜时未接纳staging保持缺失。Parquet物理行序、原始接收时间或任一单独transport sequence均不能替代admission coordinate。
11. 上限内的invalid WebSocket application message证明transport确有完整入站frame，因此重新武装第2项全frame inactivity边界；它不得更新trade last-qualifying或dedupe state。`serverShutdown`、PING/PONG、Close及没有完整application payload的transport error仍按各自协议分类，不伪装为invalid trade evidence。
12. source只等待既有global inflight row/byte预算、raw-day permit与有界队列容量；取得容量并转移evidence所有权后立即继续读，不等待Parquet append、row-group flush、finalize、atomic rename或`fsync`。writer继续独立批量落盘；禁止逐坏帧spawn task、无界队列或串行磁盘ACK。队列饱和时允许现有最多2秒的有界背压；queue接纳timeout/closed、writer失败或ownership丢失仍terminal。本合同不声称当前共享enqueue满载时其他source永不受阻，跨源洪峰公平性若成为新要求必须另建决策门。
13. 原始`aggregate_trade_id`必须保留给后续`ObservationContract`；若下游决定准入，可由共享canonicalizer因果派生nullable、中性的`observed_id_delta`，它只描述相邻已观察ID的数值差，不得命名或解释为`gap_count`、`missing_count`、confirmed hole或缺失原因。本阶段不在collector新增该派生字段。
14. `BINANCE_INVALID_INPUT_CAPTURE_MAX_BYTES`固定为`524_288`，并继续受`MAX_TYPED_ROW_BYTES=1_048_576`约束。完整WebSocket输入超过capture cap时只保存实际可证明的`capture_status=oversize_not_captured`、transport、endpoint、receive time、已知长度与配置上限；未取得完整bytes时payload、完整digest及未知长度必须保持null/unknown，禁止伪造或声称可逆。oversize WebSocket可结束当前socket并由同PID恢复；仅该外部超限事实不得结束collector。
15. v5 finalized文件保持只读兼容并全部解释为legacy `trade`，包括历史REST-only/merged行；它们不得回放进当前实时WS观察流。旧manifest已有`binance_aggtrade_continuity`字段只做合法性校验和原样保留，新binary不得创建、更新、从Parquet推导或用其控制aggTrade。v6恢复扫描只让WebSocket `record_kind=trade`参与WebSocket结构去重，必须跳过`invalid_input`的nullable ID。v5没有source epoch、connection generation、ingress或admission coordinate，不得回填伪造字段或宣称历史具备精确invalid replay。进程在queue接纳后、durable publish前异常退出可能丢失未发布尾部，必须由既有`unclean_end_unknown` coverage界定，不能以已分配sequence冒充durable evidence。
16. typed raw中的raw bytes、hash、parser诊断和审计detail不得直接进入模型。后续共享semantic canonicalizer只把获准的`invalid` marker、source、因果admission coordinate以及由ObservationContract准入的age/mask字段变成模型可见事件。training从v6 finalized typed raw重放；paper/live的版本化handoff必须携带同一evidence语义并产生相同canonical event/total-order digest，现有heartbeat handoff `event_seq`不得冒充这些source/admission coordinate。本阶段实现并验证collector typed evidence与可关联handoff编码边界，不声称已实现shared canonicalizer或真实paper/live外部source runtime。

### 4.6 Binance depth 与 snapshot

1. 必须区分“没有 depth 业务 update”和“完全没有任何入站 frame”。正常收到 PING 或其他合法控制 frame时，业务静默不得触发 reconnect、lag 或 gap。
2. 完全无入站 frame、connect/read/write/close/protocol failure可以作为 transport availability 触发 reconnect；该事实不得仅因断连写入 `source_gaps` 或异常告警。
3. 每次WebSocket成功握手后只递增一次checked非零`connection_generation`，并在该generation内同时启动唯一WS reader和至多一个active REST snapshot future。reader不得等snapshot HTTP完成后才开始消费socket；snapshot未形成可用结果时，已观察depth update进入同时受row count与byte count硬上限约束的单一generation-local buffer。不得per-frame spawn、启动第二snapshot owner、建立第二socket或使用无界buffer。
4. 继续只使用两个既有artifact和writer/manifest mapping：`binance_depth_snapshot_v2 -> binance_depth_snapshot_v3`、`binance_depth_update_v3 -> binance_depth_update_v4`。两个新schema共同增加非空`source_sequence_epoch_id`、checked非零`connection_generation`、checked非零`depth_ingress_sequence`、checked非零`collector_admission_sequence`与`collector_admission_time_utc_ms`；既有`timestamp_received`和全部官方payload/stitch字段继续保留。source epoch在同一depth source task跨socket reconnect与UTC rollover稳定，source task/进程重启时更换；`depth_ingress_sequence`在同一epoch内由snapshot/update共享一个checked-u64域，按collector完整观察到REST snapshot响应或WS application frame的真实先后冻结，Parquet物理行序不得冒充该顺序。
5. `depth_ingress_sequence`只证明raw观察顺序，不等于模型已经因果可见。snapshot成功且schema可验证时，collector先把snapshot转移到既有bounded admission并冻结其collector admission坐标，再按`depth_ingress_sequence`依次分类、释放此前buffered update并各自冻结collector admission坐标；模型、handoff和offline replay只能使用collector admission定义的可见时点，不得把snapshot返回后才知道的stitch结果回写到update原始接收时点。buffered update的`timestamp_received/depth_ingress_sequence`仍保留真实较早观察事实。
6. 正常运行中的snapshot本轮最终不可用、连接在可用snapshot前关闭、普通reconnect/UTC day reset或generation放弃时，已观察且结构可验证的buffered update必须按`depth_ingress_sequence`以版本化中性`not_applied_snapshot_unavailable`最终verdict有界release并保存；它们不得伪装成continuity-ready、confirmed hole或已应用update，也不得因恢复循环静默丢失。唯一例外是可靠collector-wide operator stop或run deadline已经关闭共同raw intake：尚位于generation-local pending，或虽进入跨日deferred staging但尚未取得新日raw-day permit及queue ownership的event，可以有界丢弃，不生成row、gap、告警、`dropped_rows`或terminal；截止前已经取得对应permit并把ownership转移到queue的evidence仍必须drain/finalize。staging关联的confirmed-gap副作用只能随最终admission发布，不能在staging被清空后单独残留。无法形成可信typed row的snapshot/update parser/schema错误仍按第9项既有完整性边界处理，本批准不为depth新增invalid判别联合。
7. raw evidence与continuity projection必须分层。`u <= snapshot lastUpdateId`或`u <= local_u`属于stale/duplicate：对应update v4 row仍按真实坐标保存并标记not-applied，但不推进continuity、不记gap或fatal。首个可应用update固定使用`U <= snapshot lastUpdateId + 1 <= u`；后续`U > local_u + 1`才是confirmed continuity hole：hole update自身仍保存为not-applied confirmed-hole evidence，旧snapshot anchor/`local_u` continuity立即失效并重新snapshot，禁止把同一hole update在新snapshot后改写、回排或错误应用。其余连续update才可应用并推进`local_u`。
8. 本节`book-ready`/`book projection`只指collector可验证的snapshot anchor、`local_u`与stitch continuity state，不表示collector维护或发布逐价位订单簿。collector必须原样保留snapshot/update的`bids/asks`与`b/a`供后续共享处理核心按独立合同重建；本阶段不得新增price-level book、第二reconstructor或冒充shared canonicalizer/model book。只有已验证snapshot与按第7项连续应用的update可以推进continuity-ready及其last-qualifying事实；stale/confirmed-hole/snapshot-unavailable raw row只更新真实artifact接收事实，不得冒充qualifying continuity update。raw row已保存不表示continuity-ready，continuity未ready也不得阻止保存raw row。training finalized typed raw与realtime handoff必须能携带相同source identity、raw observation和collector admission语义，但本阶段不实现shared canonicalizer、Observation或模型book逻辑。
9. snapshot HTTP网络失败、timeout或rate limit在同一source内持续恢复；Binance `418/429`响应中可解析的`Retry-After`秒数必须更新source-task级monotonic not-before gate，且不得被snapshot future取消、UTC day reset或WebSocket reconnect清除，也不得持久化到manifest。`U/u`字段无法解析、`U > u`等已冻结结构不变量、snapshot/schema无法验证、sequence溢出、buffer/admission ownership丢失或本地continuity state不变量破坏继续按受影响scope fail closed。上游文档叙述差异不改变本项目冻结的首桥`+1`谓词；如未来要改变该谓词、改用partial-depth stream、增加第三artifact/consumer schema、引入新Binance SDK/engine或替换唯一owner，必须停止并建立新的协议/schema/依赖决策门。

### 4.7 SDK-first 最小 patch 决策门

本事项实施时保持pinned官方`0.6.0`与
`third_party/polymarket_client_sdk_v2-0.6.0-p1`根，并要求采用更新release时建立
独立SDK版本升级决策门。该独立门现已由SDK070事项建立并在当前Stage1 worktree完成
0.7.0身份、Cargo/lock/vendor路径、API兼容和全量离线回归核验；因此当前身份为官方
0.7.0底座的唯一`third_party/polymarket_client_sdk_v2-0.7.0-p1`，精确provenance
和runtime/typed终态由SDK070三件套及Stage1 artifact管理。以下逐项patch语义矩阵
继续是本事项authority，不因版本迁移被重开，也不得仅因为“官方原版”就放弃已证明
的数据完整性边界。

当前拟议 patch 语义矩阵如下：

| patch 语义 | 与错误业务静默策略的关系 | 本需求决定 |
|---|---|---|
| parser failure通过 typed sticky fault暴露并终止连接 | 无关 | 保留；原0.6.0实施为历史，当前由0.7.0-p1同一批准路径承载 |
| CLOB interested array按 all-or-error解析 | 无关 | 保留，禁止静默跳过损坏元素 |
| `SimpleParser: Default` | 无关 | 保留；原0.6.0实施为历史，当前由0.7.0-p1同一批准路径承载 |
| outbound request绑定 connection generation并隔离旧 generation | 无关 | 保留；自然 reconnect 同样需要。RTDS已接入，CLOB生产actor必须在阶段4补齐同等绑定与发送后复核 |
| RTDS parser failure有界、脱敏诊断 | 无关 | 保留目标并在阶段2补齐：当前wrapper仍把原始serde Display拼入外显错误，非法scalar可泄漏；必须保留kind/source chain但让sticky fault、Display与parser-failure warn只含有界allowlist摘要，不改变成功解析语义；pre-parse TRACE仅按下一行B例外处理 |
| 官方pre-parse `tracing::trace!(%text, ...)`是否恢复 | 无关；当前项目未启用SDK `tracing` feature，但这是现存且未被原获批矩阵命名的vendor hunk | **B已批准**：恢复官方0.6.0原hunk。启用该feature时会在parse前记录全部文本帧，包含随后解析失败的完整raw frame；用户已明确批准该failure-trace边界放宽 |
| generation-aware one-shot `request_reconnect()` | **直接服务旧 60 秒业务静默策略** | 删除 vendor 公共能力、runtime wrapper、生产调用及专属测试 |
| heartbeat dead向 connection owner传播并结束旧 socket | 新确认 transport 缺口 | 仅对`require_text_pong=true`的连接保留；CLOB no/old-PONG继续结束旧socket并recoverable重连，RTDS no-PONG不进入该路径 |
| application-level文本PONG是否必需 | 旧generic SDK把不同频道合同压平；真实RTDS不返回PONG | **RTDS-PONG-A已批准**：`Config::require_text_pong`默认`true`，CLOB显式`true`，RTDS显式`false`；false模式以实际PING write ack串行调度且最多一个未完成PING |
| 最后一个 manager sender关闭后的 normal teardown | 新确认生命周期缺口 | 优先复用/审查官方修复方案；未发布时允许最小 vendor patch |
| 单次TCP/TLS/WebSocket建连与握手有界 | **与健康连接上的业务静默无关**；原0.6.0官方基线直接等待`connect_async()`，当前0.7.0-p1继续由批准patch提供该能力 | **CT-B已批准**：SDK `ws::config::Config`增加默认`None`的`connect_timeout`；RTDS生产显式设为20秒，超时作为recoverable transport failure，之后有界backoff并无限重试。不得把该期限用于已连接socket、业务frame等待或subscription refresh |

因此：

1. 不采用“整体恢复官方原版 SDK”。
2. 不再写“六项扩成八项”或其他固定数量合同；以上逐项语义矩阵才是 authority。
3. 删除 one-shot reconnect 后，connection generation 的自然重连隔离必须继续存在，二者不得混为一个补丁。
4. 每个CLOB与RTDS subscription actor必须先观察当前connected generation，再通过既有`bind_generation(observed_generation)`构造该generation专属manager；发送后必须复核generation与connected state仍匹配，只有复核通过才可发布applied ack。CLOB applied ack必须改为项目私有的generation-tagged类型，waiter同时校验ack generation等于当前connection generation、revision满足且state仍connected；这样即使在发送后复核与ack发布之间切代，旧ack也不能满足新代wait。旧actor/旧generation延迟发送不得被重新标记为新generation、写入新socket或被误认已应用。该要求只补齐既有generation隔离能力的生产调用，不新增第二actor、第二receiver或公共SDK API。
5. 只有`require_text_pong=true`时，heartbeat timeout或旧PONG才必须使当前`handle_connection`返回既有recoverable transport error、关闭旧socket并进入现有connection loop；`false`模式不得等待PONG，且不得新增第二个socket owner或公共shadow API。
6. connection owner必须使用互斥的内部退出语义：只有最后一个manager/bound outbound sender关闭是normal shutdown并停止outer loop；read EOF、WebSocket Close、read/write/PING failure及strict模式heartbeat timeout/旧PONG继续是recoverable transport exit；parser sticky fault继续terminal。可以增加私有内部结果类型或等价判断，但不得增加公共API或把其他`Ok(())`路径误判为normal shutdown。
7. 按用户批准的B恢复官方0.6.0 parse前`tracing::trace!(%text, ...)`原hunk；启用SDK `tracing` feature时，完整成功/失败文本帧均可进入该TRACE记录，这是明确获批的日志边界放宽，不得把它误报为脱敏。parser错误的operator-visible `Display`、sticky fault和warn仍不得额外原样拼接serde错误中的非法scalar或nested payload/secret-like字段，只含2,048-byte上限、hash与allowlist字段摘要；底层error kind/source chain保留供typed inspection。不得另造success-only raw trace或扩大其他日志行为。
8. vendor 允许修改的源码路径集合按用户批准的`CT-B`限于以下四个文件：
   - `third_party/polymarket_client_sdk_v2-0.7.0-p1/src/ws/connection.rs`
   - `third_party/polymarket_client_sdk_v2-0.7.0-p1/src/ws/config.rs`
   - `third_party/polymarket_client_sdk_v2-0.7.0-p1/src/clob/ws/types/response.rs`
   - `third_party/polymarket_client_sdk_v2-0.7.0-p1/src/rtds/subscription.rs`
9. `CT-B`批准`Config::connect_timeout: Option<Duration>`且默认`None`；用户2026-07-19批准的`RTDS-PONG-A`另授权同一`config.rs`新增`Config::require_text_pong: bool`且默认`true`。除此之外，如果Polymarket实现还需要第五个vendor文件、其他新公共协议API、raw WebSocket、manual signing、第二SDK session或改变parser/fatal语义，必须立即停止并向用户提交缺口、影响和选项。

### 4.8 完整性与顶层协调边界

1. 下列事实继续保留为 confirmed gap、quarantine 或 fail-closed 信号：SDK `Lagged(n)`、bounded queue timeout/drop、Binance depth confirmed update-id hole、payload conflict、parser实现或本地typed-schema失败、dedupe conflict、writer/manifest/atomic publish、磁盘/资源硬边界、sequence/generation溢出、evidence编码/ownership失败和内部不变量。depth confirmed-hole update必须先按第4.6节保存raw not-applied evidence，再使旧continuity state失效并resnapshot；保存raw不把hole降级为普通availability。aggTrade ID数值前跳以及第4.5节已完整形成并接纳的普通外部aggTrade `invalid_input`不在该terminal集合中；前者只保留原始`a`观测，后者作为invalid source evidence继续运行，两者都不得被误记为有效缺失条数。
2. 顶层“任一mandatory producer/writer/ingress在运行deadline前意外结束即失败”保护继续存在；测试必须证明availability及已保存的普通aggTrade invalid路径不会返回，而本地完整性注入仍会关闭共同gate并返回正确terminal error。
3. 不得把source/writer/ingress panic、join error、control channel意外关闭或writer失败重新分类为普通external availability；能由wrapper在退出前可靠观察并持久化的terminal按第4.2节写known end，其他异常只保留unknown tail。
4. confirmed gap 可以保留模型可见 missingness，但不得伪造行情、自动backfill无官方依据的数据或用后续恢复抹除缺失区间；aggTrade实时路径明确不做REST backfill。

### 4.9 配置、资源与停止

1. 分源heartbeat合同冻结为三种明确身份：CLOB生产为`10s/20s/require_text_pong=true`；RTDS生产为`5s/require_text_pong=false`，共享结构中保留的`15s`在false模式下不生效；SDK默认为`5s/15s/require_text_pong=true`以保持其他consumer兼容。`20s/15s`不得冒充官方共同保证，不同频道不得再共用错误行为。
2. 各 source 的单次 connect/HTTP/read/backoff timeout保持有界，最大 backoff有上限并加入既有 jitter；外层恢复无总次数/总 elapsed退出预算且禁止 tight loop。
3. 每个 Polymarket source最多一个 active connection generation、一个 actor、一个 receiver和一个 desired-state owner；每个 Binance source最多一个 active WebSocket，只有depth可以持有官方stitch所需的单一active snapshot future及单一有界generation-local buffer，aggTrade不得创建backfill任务。
4. 本事项已完成的各协议恢复路径及其100-cycle历史证据保持原样，不因RTDS后继专项重跑或删除。RTDS business replacement只是同一owner中的顺序外层动作，不另建第二transport owner、并行恢复loop或shadow control plane；新路径以wrapper state-channel closure测试、生产源码中`shutdown_and_wait(self).await`后创建新runtime的直接顺序，以及连续两次replacement、共三个runtime epoch的聚焦测试组成最小证据。不得为重复证明该源码顺序增加test-only lifecycle hook；新路径不新增或重跑100-cycle，也不新增task/FD/watch/refcount资源矩阵。其他路径不得另建第二恢复循环，aggTrade还必须证明不存在REST/backfill/pending任务。
5. deadline、SIGTERM/operator stop必须先关闭新admission，再在永久离线/静默时按既有期限drain/finalize全部已接纳evidence；depth generation-local且尚未接纳的pending update不阻塞该受控退出，也不生成row、gap、告警或terminal。正常UTC day rollover与hour/segment finalize不属于该丢弃例外，继续按第4.2、4.6节执行fence或中性release。

## 五、规范衔接

本文已于2026-07-17最终获批；阶段0必须按语义同步：

- `demand/data-collect-demand.md` 中“空结果、长时间静默、stale、断线一律写 source gap”、所有 source 必须有限 attempts/max elapsed/exhausted、non-empty external availability gap 导致 artifact 失败的旧语义，以及旧daily manifest允许字段清单；该清单必须加入第4.2节顶层、单owner、版本化`collection_day_coverage`，保持六个既有source counter职责不变，且仍只有每日唯一manifest；
- `demand/data-collect-demand.md`与既有旧合同中aggTrade v3/v5布局、WebSocket字段始终非空、外部payload/schema负例一律terminal的语义，只在第4.5节`binance_aggtrade_v6`范围内由本轮批准精确取代；其他source和本地typed-schema失败不变；
- `demand/data-collect-reliability-repair-demand.md` 中 RTDS 两层有限恢复、CLOB 静默退出、旧六项 vendor patch 白名单、断线/静默 gap、systemd 新 PID恢复语义、旧daily manifest“只记录”字段清单，以及把普通外部aggTrade invalid与parser/schema内部失败合并为terminal的旧语义；其row/byte hard bound、queue/writer/manifest failure和内部不变量合同继续有效；
- `plan/data-collect-reliability-repair-plan.md` 中对应有限 recovery budget、RTDS D2、vendor patch步骤，以及daily manifest字段与coverage验证步骤；reliability demand/plan都必须采用第4.2节同一顶层v3/嵌套v1与restart/cross-day语义；旧UTC边界部署候选已由独立直接部署事项取代；
- `demand/rebuild-core-demand.md` 已按本轮用户批准同步训练采集退化、aggTrade invalid evidence、模型输入和完成验收的对应条款：RTDS-only网络动作已改为全部训练外部源的source-local协议恢复，daily manifest只提供单一collector observation-eligibility coverage与既有source counters，不持久化transport current/open/episode；完整invalid evidence由shared canonicalizer转换为模型可见invalid marker，raw bytes/parser detail不进模型，也不能降低模型学习真实或模拟missingness的目标。核心需求中已批准的离散`next_wake`目标候选及其`DecisionScheduleContract`只作为下游边界引用，不得反向扩入本collector修复。实施阶段只复核并保持这些语义一致，不重复扩大核心草案范围。

同步必须遵守：

1. 旧 changelog 是当时批准、实现和事故的历史事实，不回写或删除；新 changelog记录取代关系。
2. 当前代码只有training拥有真实RTDS adapter；本专项阶段1只修改training collector。paper/live共享availability/missingness语义；future paper/live真实RTDS adapter的实施、权限、handoff、测试和部署必须由各自需求另行批准，但其RTDS连接生命周期已由共同专项冻结，不得另行选择不同语义。
3. “完整 artifact”指 schema、manifest、provenance、因果时间和缺失编码完整，不等于所有外部源全时段有行情。
4. 当前 `demand/rebuild-core-demand.md` 仍整体处于审核中；同步这里只消除本事项已由用户决定的网络语义冲突，不借机批准其余核心重建内容。
5. 用户已批准本文、计划与SDK矩阵；阶段0完成authority同步和SDK基线审查、commit/push前，不得进入阶段1代码实现。

## 六、验收标准

### 6.1 共同生命周期

1. **单源静默：** CLOB、aggTrade和depth业务流长期无事件但WebSocket按协议存活，Gamma持续返回合法空候选时，跨越旧freshness/budget周期仍不产生静默驱动网络动作、gap、lag或告警。RTDS拆成两例：从未收到fresh时保持同runtime/socket；此前收到fresh时，最后fresh达`300s`后按共同合同完整替换runtime，但仍不重复同socket订阅、不写gap/lag/告警、不结束source/collector。
2. **单路不可达：** 分别让Gamma HTTP、CLOB WebSocket、RTDS WebSocket、depth snapshot HTTP、depth WebSocket和aggTrade WebSocket连续失败超过旧attempts/max elapsed预算；同一PID保持运行，其他健康producer继续写盘；恢复后对应source-local owner自行恢复。本验收复用阶段3--6已冻结的单路径与100-cycle累计证据，不在阶段7完整collector中逐路重复六次。
3. **多源退化：** 同时断开至少两个不同协议源，未断开的source继续采集和finalize；collector不出现`source_task_ended_unexpectedly`。阶段7完整collector只新增一个代表性Polymarket＋Binance跨平台多源退化场景，不建立组合笛卡尔积。
4. **全部外部I/O不可达或流式业务静默：** 阶段7既有完整collector三例保持历史验收，不重写其当时结果。②的RTDS fixture明确为source task从未收到fresh，因此cold-start watchdog禁用并保持同runtime/socket；另由RTDS专项聚焦fixture覆盖“此前收到fresh”后`300s`触发完整runtime replacement。两者都不得新增transport scope/episode、虚假degraded/silence事件或manifest字段。
5. **coverage与缺失保留：** current-day恢复前取得并持续持有dataset独占实例lease；coordinator再同步建立bounded event queue、唯一receiver并确认ingress循环可进入，六writer与四producer wrapper全部spawn、初始化ready并阻塞在共同gate；ManifestOwner只执行一次不重试的atomic activation，ack后才释放网络poll。零行业务源仍有共同`active` coverage，未达到共同起点保持`not_started`。无PendingExit的正常rollover必须覆盖新日frame与boundary timer竞态、逐event day/generation permit、旧日drain、barrier/finalize ack、旧日顶层关闭、新日active commit和owner ack；跨界PendingExit按两个持久化切点与一个release切点验收：旧日final-close前无法由fence排除的unclean令旧日unknown；旧日已经closed而新日activation commit未成立时不重写旧日，新日只能missing/not-started且布尔值不变；activation commit后状态与`reactivated_within_day`已经归属新日，即使ack丢失也不得回滚，ack只控制是否release。任何pending exit都不得伪造新日active或提前resume。可靠close候选之间first-close-wins，但最终fence前发现的非协调unclean必须覆盖known candidate；协调退出必须有精确task级ack/permit证据。四种current-day nested状态到active的转换必须证明首次activation为`false`、任一同日后续activation为`true`且保持单调。`reactivated_within_day=true`只隔离最后start前不能证明连续coverage的replay区间，不得回写历史Observation；unclean共同尾界只能是last start，另一健康源的更晚行不得延长。已经取得对应raw-day permit并转移queue ownership的typed raw接收事实不得被抹除；尚无新日permit的跨日deferred与depth generation-local pending都不是已接纳raw row，受控operator stop/run deadline截止后在training、paper、live中一致缺失，不得伪造成gap、unknown row或drop evidence。collector仍不得生成silence-start timer或伪行情。
6. **availability与真实terminal：** 已确认发生在外部connect/read/write/HTTP/WebSocket边界的故障必须在对应source-local owner内持续恢复，不能仅因缺少专门诊断reason、持续时间、尝试次数、既有诊断计数饱和或日志失败结束source/collector；parser/schema、SDK lag、queue drop、writer/manifest和内部不变量故障注入不得被恢复循环吞掉。producer/writer task的可靠normal/terminal return必须按共同gate合同冻结known candidate；coordinator ingress路径提前退出、唯一receiver ownership丢失或queue不变量失败也必须结束coverage，并按能否可靠完成共同fence决定known end或unknown tail。非协调panic/abort/join failure为unknown tail，顶层仍发现terminal task；stop已发但cancel生效前panic、无/错abort permit、ack丢失与具体handle `is_cancelled()`必须证明只有可因果归属本次close的协调退出获得豁免。不得把同步构造的queue或ingress路径冒充独立spawn task。

### 6.2 RTDS 与 SDK

7. **RTDS cold-start长期静默：** source task从未收到fresh时，server保持socket开放但不发送任何文本/控制PONG和业务frame；client首条立即、后续按固定间隔持续发送大写文本PING，跨过至少三个旧15秒等价timeout和`300s`后generation/runtime不因business watchdog改变、wire subscribe总数为一、source不退出。随后同一socket发送stale/fresh payload均被正确消费，stale不增加lag/gap；收到首条fresh后才启用共同business watchdog。
8. **RTDS启动离线：** 连续连接失败超过旧3次/20秒门后启动server；source未提前返回，首个generation只发送一次完整含 `btc/usd` filter的subscription。
9. **RTDS运行中恢复：** 真实Close/EOF/read或write/PING-send failure后连续失败再恢复；每个成功generation最多一次latest desired replay，无业务静默one-shot ticket。no-PONG不得进入transport恢复路径。曾有fresh后的完整runtime replacement是同一owner串行break-before-make层，不得实现为one-shot ticket、第二owner或并行socket。
   单次TCP/TLS/WebSocket建连与握手必须受20秒期限约束；超时只结束本次attempt并进入既有有界backoff，不能结束source或成为业务静默计时器。loopback必须覆盖TCP已接受但不完成WebSocket握手，随后下一次连接成功并只replay一次完整desired。
10. **删除 one-shot能力：** 生产和测试代码不存在 `request_reconnect` 公共/包装/调用路径；vendor差异中不存在该专用票据状态。
11. **RTDS no-PONG与单pending：** `require_text_pong=false`的loopback server不PONG，client仍以实际write completion ack驱动固定PING；write ack未返回时最多一个未完成PING，解除阻塞后不得burst补发。跨过旧timeout后socket/generation不变且业务frame继续消费。
12. **manager drop与反向transport回归：** 分别在裸`ConnectionManager`和`MarketSdkRuntime/RtdsSdkRuntime`集成层drop全部manager/actor持有者；server观察旧socket关闭、task收敛且不建立新连接。反向测试必须证明read EOF、Close、outbound write/PING write failure仍进入reconnect，false模式PING write failure同样recoverable，parser fault仍terminal。
13. **CLOB共享heartbeat回归：** generic manager对`require_text_pong=true`的CLOB parser从no/old-text-PONG恢复；WebSocket protocol Pong不能冒充精确文本PONG，同时CLOB `10s/20s/true`、parser/fault和dynamic desired语义不变。

### 6.3 CLOB 与 metadata

14. **CLOB静默：** current/prefetched asset跨越至少三个旧30/60秒等价周期仍不unsubscribe/resubscribe、不退出；随后业务事件由同一generation消费。
15. **CLOB lifecycle更新与generation隔离：** desired set为current + prefetched next + resolution_pending旧market assets并集；terminal claim后到accepted-event drain完成前不得移除pending assets，drain后的terminal或budget完成才发送精确remove。自然reconnect只replay最新完整set。CLOB actor必须按观察到的generation绑定manager并在发送后复核state/generation；私有applied ack携带generation，waiter同时校验当前generation、revision与connected。旧generation延迟发送不得落入新socket；发送后复核与ack发布之间切代时，旧ack也不得满足wait。
16. **Gamma不可达/空候选：** 连续网络失败或暂时无候选不结束market source/collector、不写confirmed gap；服务恢复后按当前时钟重算current/next window，且current/next discovery不继续请求已过期slug。测试另证明只有活跃窗内已固定并验证identity的resolution-pending market可执行E+3..E+8 exact-slug例外；restart无finalized durable v3 identity、错过slot、budget结束或第7次probe均被拒绝。
17. **metadata完整性：** 矛盾identity/schema/asset mapping负例仍按既有合同失败，不能被空候选恢复循环吞掉。

### 6.4 Binance

18. **aggTrade静默：** server持续PING但不发aggTrade，跨越多个旧60秒周期不主动断线、不增加lag/gap；随后同socket首条合法aggTrade正常消费。
19. **aggTrade transport、WS因果接纳与invalid replay：** 每个完整application frame或合法PING重置30秒全frame inactivity边界，完全无任何frame才触发中性transport reconnect；close/EOF/server shutdown/连接失败后同PID持续恢复，每次成功连接的`connection_generation`严格增加而`source_sequence_epoch_id`保持不变。ID数值前跳不得启动REST、pending、gap、reconnect或退出。至少冻结并通过以下v6实际路径：同毫秒`trade A -> invalid B -> invalid C -> trade D`四条evidence按admission sequence唯一重放，B/C逐条保存且可逆、同socket/source/collector不退出；当前日的`trade 100 -> trade 103 -> invalid -> trade 104`按同一真实顺序立即进入queue admission，跨日activation未完成时则按同一真实顺序进入pre-admission staging、只在正常flush取得新日permit与queue ownership后进入admission，受控截止先获胜时未接纳staging保持缺失；自动静态回归证明生产代码、依赖与配置不存在aggTrade REST endpoint/client/task，gap为空且connection generation不变；大幅ID前跳同样中性；invalid中的假ID不推进dedupe/last-qualifying；payload conflict仍保留双方事实与confirmed conflict。
20. **depth业务静默与唯一owner：** 持续收到PING但没有depth update时不重连、不写gap；完全无入站frame时进入transport恢复并重新snapshot。生产调用图继续只使用现有`reqwest + tokio-tungstenite + Tokio + Serde`和唯一direct WebSocket owner，不出现Binance SDK/market-data engine、第二socket owner、第二snapshot owner或per-frame task。
21. **depth双artifact因果顺序与update-id分支：** 实际Parquet round-trip证明snapshot v3与update v4共同保存source epoch、connection generation、depth ingress及collector admission坐标，旧snapshot v2/update v3只读为ordering unknown且不伪造字段。loopback必须让update在t1被WS reader观察、snapshot在t2返回：raw update保留t1的`timestamp_received/depth_ingress_sequence`，但snapshot先取得collector admission，buffered updates再按ingress顺序release，任何t2 stitch结果都不回写成t1模型可见。`u <= snapshot/local_u` raw row保存为not-applied stale/duplicate且不记gap；首桥满足`U <= lastUpdateId + 1 <= u`；`U > local_u + 1` raw row保存为not-applied confirmed-hole、旧continuity state失效并重新snapshot，同一hole row不得在新snapshot后改写或应用；无法解析`U/u`、`U > u`、sequence overflow和本地continuity不变量负例按冻结scope失败。验收还必须证明collector只维护snapshot anchor/`local_u` continuity，原样保留价位数组但不新增price-level book。
22. **snapshot恢复、失败释放与受控截止：** HTTP timeout/rate limit超过旧预算后source仍运行并重试；`418/429 Retry-After`在同一source task内跨future replacement、day reset和reconnect继续约束下一次HTTP请求。每个成功generation只有一个snapshot future且WS reader同时推进，有界buffer不丢不重排。合法snapshot先admit并恢复stitch；正常运行中的snapshot最终不可用、可用前连接关闭、普通reconnect/day reset或generation放弃时，结构可验证的buffered updates按ingress顺序以中性`not_applied_snapshot_unavailable`verdict有界release，不冒充continuity-ready/gap。可靠operator stop或run deadline关闭共同intake后，未取得对应raw-day permit与queue ownership的generation-local pending或跨日deferred staging可以清空，且不生成row/gap/告警/`dropped_rows`/terminal；截止前已接纳evidence必须drain。非法snapshot/update schema仍按完整性合同处理。

### 6.5 资源、产物与总门禁

23. **manifest coverage、同artifact v6与训练基础：** 每日唯一顶层v3 manifest只新增直接嵌套的`data_collect_collection_day_coverage_v1`，不新增transport scope/reason/episode、通用extensions容器或逐source coverage复制。六个既有source counter继续保存artifact首末接收事实及既有操作计数；历史`backfill_rows`兼容计数保留，但新WS-only生产必须始终为零。`binance_aggtrade_v6`必须仍位于既有source目录和同一小时artifact/writer中；实际Parquet round-trip证明`trade|invalid_input`判别、nullable互斥、exact payload evidence、source epoch、connection generation、WS ingress/admission coordinate及同毫秒排序，目录中不存在invalid sidecar或第七artifact。兼容矩阵必须逐项验证：真实v5文件仍按legacy trade读取，包括历史REST-only/merged行；v6 invalid恢复时不被当作null ID损坏且不参与WebSocket结构去重，v5不伪造sequence或generation；既有REST nullable列只作读取兼容，新生产行全部为null；旧manifest已有aggTrade continuity字段时原样保留，新binary不创建、更新、推导或用其控制运行；prior legacy manifest缺对象始终可读且不插入coverage；相同新identity的current manifest缺对象按新writer损坏失败；不同identity current legacy继续被既有gate拒绝；unknown nested version/field/type/time/state失败；所有ManifestOwner mutation保留对象。dataset独占lease、四种current-day activation、顶层open/closed、写成功但ack丢失和时钟倒退必须逐项测试；首次activation为`reactivated_within_day=false`，任一同日后续activation原子提交后为`true`且不被unclean/known-end/reconcile转换清除。prior active/unclean-end-unknown/not-started/ended-known/legacy missing分别验证nested state、顶层state、closure reason与gap：active转unclean但不改变布尔值，既有unclean幂等确保unclean gap/closure，其他三类中性关闭。状态/时间不变量、ready/activation ack、可靠known-close候选first-close-wins、unclean-dominates、task级协调abort证明、PendingExit跨日归属、正常rollover rendezvous及replay eligibility均按第4.2节通过测试。typed raw、last qualifying和known coverage让后续模块因果派生missingness；不得生成额外receipt/sidecar，不进入`source_gaps`，也不因外部coverage不足自动把结构完整的训练artifact判失败。首次正式冷启动的部署authority现为独立直接部署需求；这不冒充已经具备same-day不同identity takeover能力。
24. **collector静默timer窄例外：** CLOB、Gamma、Binance仍不得在data-collect中生成business-silence timer、离散silence-start事件、周期性合成market row、周期subscription refresh、`DecisionWakeEvent`或fixed model decision tick；RTDS只允许共同专项冻结的source-task内monotonic watchdog，且不持久化silence-start或模型事件。模型可见age/mask计算、确定性调度基线和离散`next_wake`目标候选仍不由本事项实现。
25. **资源与异步接纳有界：** 本事项已完成的协议恢复路径及阶段9 RTDS自然Close 100-cycle证据保持原样，不因后继专项重跑。RTDS专项不新增或重跑100-cycle完整runtime replacement或进程资源矩阵；它以wrapper state-channel closure、生产源码直接顺序及连续两次replacement/三个runtime epoch的确定性loopback共同证明同一owner串行outer层、每连接一次filtered subscription且无`unsubscribe`。不得增加test-only lifecycle hook或跨端事件先后断言，也不得为其他source重建第二恢复路径或额外多源压力组合。aggTrade不得存在REST、backfill、pending或range-retry任务。aggTrade writer/flush被测试阻塞但现有queue仍有容量时，连续`trade/invalid/invalid/trade`必须全部转移ownership且source不等待磁盘ACK；writer恢复后全部落盘。depth snapshot等待、成功释放和正常运行中的失败中性释放均不得使用无界buffer或per-frame task；受控operator stop/run deadline只可丢弃尚未取得对应raw-day permit与queue ownership的generation-local pending或跨日deferred staging。短时queue满允许有界背压且不丢行，queue timeout/closed及writer failure仍terminal；invalid burst不逐条spawn、不越过global row/byte或512KiB capture cap。oversize只写真实有界证据；queue接纳后durable publish前模拟崩溃必须落入unknown tail，不能宣称缺失row已落盘。
26. **配置回归：** CLOB生产为`10s/20s/require_text_pong=true`，RTDS生产为`5s/require_text_pong=false`，SDK默认为`5s/15s/require_text_pong=true`；RTDS `connect_timeout=20s`、transport总尝试无限，各源单次backoff有界。SDK默认`connect_timeout=None`保持上游兼容；CLOB在阶段4显式启用同一20秒单次建连/握手期限，不得把它解释为heartbeat或业务静默期限。
27. **全量门禁：** 定向loopback/contract测试、`cargo test -p data-collect --locked -- --test-threads=1`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo fmt --all -- --check`与`git diff --check`全部通过。阶段7完整collector默认集合严格只有验收4指定的三例；invalid、terminal、UTC rollover、hour finalize、single-source和100-cycle证据按前序组件/owner测试累计复用，不得为它们扩张第四个完整collector场景。默认全量集合中的同一loopback不得先逐项重复执行；只有非默认关键验收、最终修复直接影响的测试或需要单独保存资源采样的场景才额外定向运行。
28. **范围、依赖与handoff parity门禁：** 没有新增Polymarket raw WebSocket/shadow SDK或任何source的第二socket owner；Binance只保留现有唯一direct WebSocket owner与`reqwest + tokio-tungstenite + Tokio + Serde`依赖底座，Cargo/lock和生产调用图没有新增Binance SDK、Barter、Nautilus或其他market-data engine。使用同一批可关联aggTrade evidence验证finalized typed raw与版本化handoff编码能承载相同record kind、source evidence identity、causal admission与total-order输入；depth snapshot v3/update v4同样能承载一致的source identity、raw ingress、collector admission及continuity-ready分层语义，但现有heartbeat-only handoff `event_seq`不得被冒充为已完成online source parity。没有CLOB user源、第三depth artifact、collector price-level book、shared canonicalizer/data-pipeline/model策略或真实paper/live source runtime实现、部署或数据清理；vendor源码路径集合精确为第4.7节用户最终批准的四个文件，实质差异与逐项矩阵一致。

## 七、完成定义

只有同时满足下列条件，本事项实现阶段才可标记完成：

1. 本需求、对应计划和 changelog 已获用户明确批准并完成 authority 同步；
2. 第六节28项验收均有测试名、命令、结果或artifact证据；
3. 每个实现阶段自审和独立审查均无未解决的 Critical/Important；
4. 相关commit已精确stage并push，未混入用户现有无关改动；
5. 生产构建、部署、服务操作和数据处理仍保持未授权，等待独立用户批准。
