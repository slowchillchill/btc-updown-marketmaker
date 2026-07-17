# 训练采集全数据源可用性与连接生命周期修复需求

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应计划：`plan/data-collect-source-availability-lifecycle-repair-plan.md`
>
> 对应 changelog：`changelog/data-collect-source-availability-lifecycle-repair-changelog.md`
>
> 状态：`APPROVED_IMPLEMENTATION_PHASE_4_COMPLETE_PHASE_5_AUTHORIZED_CT_B_APPROVED`。用户已于2026-07-17批准本文、对应计划及第4.7节SDK-first逐项patch矩阵，并授权按计划分阶段实施、验证、独立审查、commit和push；同日又对官方pre-parse完整文本帧trace差异选择B，批准恢复官方0.6.0原hunk并显式放宽启用SDK `tracing` feature时的失败trace脱敏边界。阶段3实施前发现pinned SDK的单次TCP/TLS/WebSocket建连与握手没有期限；用户随后批准`CT-B`，允许把vendor路径集合扩至四个文件、增加默认`None`的SDK `connect_timeout`配置，并由本项目为RTDS显式设为20秒。该期限只约束单次建连/握手，绝不是连接寿命、业务静默期限或周期性重连；成功连接仍在健康PING/PONG下无限等待业务数据，总重试次数仍无限。阶段0 authority/provenance、阶段1共同classification/coverage lifecycle、阶段2 vendored SDK heartbeat/teardown、阶段3 RTDS长期运行与one-shot收缩及阶段4 CLOB/Gamma长期运行与identity完整性均已实现、完成全量验证并分别通过独立终审；阶段4精确commit/push后进入已授权阶段5。此前批准的`M1-A / M2-A0 / M4-A-R / M5-N/A / M6-A+ / M7-A-R / M8-A`继续有效；`M3-A-R`使用单一`reactivated_within_day`布尔语义，原子发布采用用户批准的R三态语义。本授权不包括模型调度实现、生产release构建、部署、远端服务操作或数据清理；这些仍须独立批准。

## 一、目标

修复训练数据采集器把外部业务消息静默、stale、暂时不可达或服务端暂时没有数据错误升级为订阅操作、连接操作、`source_gaps`、告警、source 退出或整个 collector 退出的行为。

修复后的 `data-collect-training-collector` 必须满足：

1. Polymarket market/metadata、RTDS、Binance depth/snapshot与Binance aggTrade中任意一个、多个或全部外部源transport不可达或业务流静默时，collector都在同一PID内持续运行，健康源继续采集和写盘。全部transport不可达时进程保持有直接证据的degraded并逐源恢复；transport按协议存活但全部业务流静默时进程保持active/idle，保存active coverage与nullable last-qualifying；当前transport事实只存在于source-local运行态及有界operator日志，不进入daily manifest，也不得虚构degraded或silence状态转换。
2. 业务消息静默、持续 stale、暂时无候选市场、DNS/TCP/TLS/WebSocket/HTTP 不可达、heartbeat timeout、rate limit 和服务端关闭是外部availability与下游missingness因果基础，不得仅因持续时间或恢复次数升级为confirmed gap、异常告警、run failure、source退出、collector退出或systemd新PID；missingness mask只由后续`ObservationContract`在求值时派生。
3. 业务静默不控制网络：不得仅因没有业务消息而重复 subscribe/unsubscribe、主动切换 connection generation 或主动断开仍然存活的 socket。
4. 真实 transport 故障在同一进程的 source-local transport owner 内持续恢复；单次等待、退避、并发和资源必须有界，但 availability episode 的总时长与总尝试次数不得成为进程存活预算。
5. 各源共享同一结果合同和错误分类，但保留各自协议要求；不得把 RTDS 的 subscription/reconnect 实现机械复制到 CLOB 或 Binance。
6. parser/schema、SDK 或本地 queue 的确定性丢失、confirmed sequence hole、writer/manifest/atomic publish、路径、磁盘、资源硬边界和内部不变量故障不属于 availability 豁免，继续按既有受影响范围 quarantine 或 fail closed。
7. collector必须保留typed raw接收时间、真实数据空档、last qualifying time与中性有界运行诊断，使后续训练能够从真实/模拟missingness学习暂停新增交易、取消未成交BUY订单，以及通过BUY对侧outcome中和已有方向风险；daily manifest只包含既有source counters与第4.2节单一coverage，不得被伪称为transport episode event tape，collector和data-pipeline也不得替模型生成这些策略动作。

“同一进程内持续恢复”不等于关闭旧进程再由 systemd 启动新进程。进程重启只允许来自真实进程级完整性故障、operator 明确操作或后续单独批准的部署流程，不能作为单个外部源的正常恢复机制。

## 二、已核实事实

### 2.1 当前训练采集器的数据源清单

当前生产 collector 有四个逻辑 source task：

| 逻辑 source task | 产生的事实/产物 | 本事项边界 |
|---|---|---|
| Polymarket market source | CLOB market 事件与 Gamma market metadata | 包含 CLOB 动态 asset subscription、业务静默 watchdog、Gamma discovery |
| RTDS source | `crypto_prices_chainlink` / `btc/usd` | 包含 desired subscription、SDK transport 与 fresh/stale 消费 |
| Binance depth source | depth WebSocket 与 REST snapshot | 包含 transport liveness、snapshot stitch 与 update-id continuity |
| Binance aggTrade source | aggTrade WebSocket 与 REST backfill | 包含业务静默、transport recovery 与 aggregate-trade-id continuity |

manifest 中可以表现为六类 source artifact，但它们不是六个独立生命周期 owner。训练 collector 当前没有 Polymarket CLOB user-channel source；本文不得虚构或实现该数据源。

### 2.2 官方合同边界

截至 2026-07-16：

- Polymarket 官方 [CLOB WebSocket 文档](https://docs.polymarket.com/market-data/websocket/overview) 允许在已有连接上动态增删订阅，并规定每 10 秒发送 PING；没有承诺每个 asset 必须在 30 秒或 60 秒内产生业务事件，也没有规定业务静默必须重订阅或重连。
- Polymarket 官方 [RTDS 文档](https://docs.polymarket.com/market-data/websocket/rtds) 的客户端指引允许不断线修改订阅，规定每5秒发送PING，并说明connection error会触发automatic reconnection attempts；当前Rust SDK的connection loop提供具体重连实现。官方没有把业务静默或stale定义为connection error，也没有重复相同subscribe的确认语义或3秒/6秒恢复SLA。
- Binance 官方 [WebSocket Streams 文档](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md) 把 aggTrade 定义为事件驱动流，并为 depth snapshot/update-id stitch 给出明确连续性规则。没有成交或没有 depth 更新不能自动证明连接已坏；可证明的 depth update-id hole 则是真实序列缺口，必须重新建立本地订单簿。

因此，业务 freshness、subscription state 与 transport liveness 是三种不同状态，不能用一个业务消息 deadline 同时控制。

### 2.3 当前代码中的已确认缺陷

| 数据源 | 当前行为 | 已确认问题 |
|---|---|---|
| RTDS | 60 秒无 fresh 后调用 generation-aware `request_reconnect()`；观察两个新 generation、3 秒窗口/12 秒总期限后退出 | 业务静默错误控制 transport，并最终结束 RTDS source 与 collector |
| CLOB market | 单 asset 首条 30 秒、后续 60 秒静默后 unsubscribe/subscribe；只允许两次，第三次返回错误 | 与 RTDS 同类的有限业务静默预算；现有测试还明确要求第三次强制进程重启 |
| Binance aggTrade | 60 秒无 aggTrade 即断线重连、增加 lag 并写 gap；收到 PING 不重置业务 deadline | 通常不会最终停机，但把正常无成交误判为 lag/gap 并制造无必要连接 churn |
| Binance depth | 约 30 秒完全无入站 frame 后重连并重新 snapshot；update-id hole 后重建订单簿 | 恢复机制基本正确，但 transport availability 被写成 gap；必须与“有 PING、无业务更新”区分 |
| Gamma metadata | 网络/发现失败通常经外层 supervisor 继续恢复；暂时无候选可能形成错误 gap 语义 | 持续运行基本具备，但 availability/coverage 分类需要统一 |

顶层 coordinator 当前把任一 source task 在运行 deadline 前结束视为 `source_task_ended_unexpectedly` 并终止 collector。这个保护本身不是缺陷：recoverable availability 必须在 source 内部持续等待/恢复，真正的本地完整性错误仍应返回顶层；不得通过让顶层吞掉所有 source 错误来实现“持续运行”。

### 2.4 当前 health 抽象不能直接视为生产修复

`data-collect/src/health.rs` 已有 `SourceHealthEvent`、`SourceHealthRegistry`、`ReconnectGate` 和有限 retry trace，但它没有完整接入生产 `live_runtime`，而且 `CircuitState::Exhausted`、固定 `max_attempts/max_elapsed` 及部分 `CoverageVerdict` 仍表达旧有限恢复合同。

本事项必须优先复用并最小修正已有 health 类型，不能另建第二套 health/reconnect 控制面；但也不能因为抽象测试通过就宣称生产 source 生命周期已经满足本需求。

### 2.5 pinned SDK 现状

当前项目通过 `[patch.crates-io]` 使用 `third_party/polymarket_client_sdk_v2-0.6.0-p1`。现有 vendor 差异同时包含数据完整性修复、API 可构造性修复、自然重连隔离、诊断增强和旧业务策略专用能力，不能整体归因于错误的 RTDS 业务逻辑。

直接恢复官方原版会令当前 runtime 缺失 `ConnectionFault`、`fault_receiver()`、`SimpleParser::default()`、connection-generation 隔离接口等能力，并恢复官方当前仍存在的 parser 错误静默继续、CLOB 数组损坏元素被跳过以及高层 subscription stream 吞掉 `Lagged(n)` 等行为。因此，本事项采用逐项 patch 审计，不采用整体恢复官方原版。

实施前必须重新核对的官方上游证据至少包括：`SimpleParser::Default` [PR #81](https://github.com/Polymarket/rs-clob-client-v2/pull/81)、heartbeat 传播 [Issue #60](https://github.com/Polymarket/rs-clob-client-v2/issues/60) / [PR #63](https://github.com/Polymarket/rs-clob-client-v2/pull/63)，以及连接任务回收 [Issue #39](https://github.com/Polymarket/rs-clob-client-v2/issues/39) / [PR #40](https://github.com/Polymarket/rs-clob-client-v2/pull/40)。这些链接是当前审查入口，不冻结其未来 merge 状态；阶段0必须按当时官方状态复核。

## 三、范围

### 3.1 本事项包含

- 四个训练 source task 的 availability、业务静默、transport recovery 和 terminal error 边界；
- 顶层 source-task 生命周期分类，不改变真正 terminal task 结束必须被发现的原则；
- 生产 `live_runtime` 与已有 `health.rs` 的最小衔接；
- RTDS desired subscription、CLOB market-lifecycle desired assets、Binance depth snapshot stitch、Binance aggTrade ID/backfill 与 Gamma discovery 的源特定恢复；
- pinned 官方 Rust SDK 的逐项 patch 审计、删除失去依据的 one-shot reconnect 能力，以及 heartbeat/teardown 的最小修复；
- 每日唯一manifest中的顶层、单owner、版本化`collection_day_coverage`，并保持六个既有source counter只记录artifact事实与既有操作计数；
- 单源、多源、全部源退化、恢复、停止、完整性负例和资源有界测试；
- 对旧 demand/plan 中相反恢复语义的精确同步。

### 3.2 本事项不包含

- 新增 CLOB user channel 或其他新外部数据源；
- 修改RTDS/CLOB/Binance业务payload schema、价格含义、dedupe identity或Parquet行布局；第4.2节对每日manifest schema的有界扩展属于本事项，超出该字段集的新artifact/typed raw/consumer schema必须停在新决策门；
- 在 `data-pipeline`、model-train、offline-evaluation、paper、live 或 execution 中实现模型策略；
- 决定或实现`data-pipeline`/Model Runtime的`DecisionWakeEvent`、deadline数量/阈值、离散`next_wake_bucket`、timer排序/抢占、最小/最大间隔、变量持续时间Reward/SMDP归因或模型调度策略；这些属于整体审核中的`demand/rebuild-core-demand.md`，不属于collector修复；
- fixed/periodic decision tick、周期性合成 market row、用 PING/PONG 填充业务 coverage，或用 stale 值冒充 current；
- 为Polymarket新增raw WebSocket/manual protocol、第二个官方SDK session、第二个receiver/actor、shadow client或旁路官方SDK；Binance保留现有唯一`tokio_tungstenite` WebSocket owner，但不得新增第二raw-WS实现或第二socket owner；
- 生产构建、部署、systemd 修改、远端服务操作、数据清理或失败日期重采。

## 四、需求

### 4.1 共同分类与长期运行合同

1. 每个 source 必须明确区分：
   - `business availability`：无业务消息、stale、空候选、当前无市场事实；
   - `transport availability`：DNS/TCP/TLS/HTTP/WebSocket connect/read/write/close、heartbeat、rate limit、server shutdown；
   - `confirmed integrity failure`：SDK/local lag、序列洞、parser/schema、不变量、writer/manifest、确定性 drop。
2. 前两类只形成中性 availability/recovery 事实，不得仅因持续时间或次数进入 `source_gaps`、异常告警、source terminal、collector terminal 或 systemd restart。
3. source-local 恢复允许每次 connect/request/read/backoff 有明确 timeout 和最大 backoff；外层循环必须可无限继续且无 tight loop，总 attempt、总 elapsed 和 circuit exhausted 不得结束 source。
4. operator stop、测试 deadline 和协调 shutdown 必须能中断永久离线、永久静默、rate-limit backoff 和连接尝试；“持续运行”不得造成无法停止。
5. 任一source退化不得阻塞其他source的队列、writer或market lifecycle；全部源退化时coordinator仍存活。source-local transport恢复状态与有界operator诊断不得被冒充模型输入或完整episode tape，每日manifest只按第4.2节保存单一collector observation-eligibility coverage及既有source counters。
6. 真正 terminal source error 仍返回顶层并触发现有 fail-closed coordinator；不得把 parser/schema、writer 或内部不变量错误放进无限网络重试。

### 4.2 中性 availability/missingness 证据与 manifest coverage

1. source-local transport owner仍必须根据真实connect/read/write/HTTP/WebSocket/heartbeat结果区分连接中、重试、恢复与terminal完整性错误；这些状态只控制对应协议恢复并可形成有界、去重的operator日志，不得因持续时间、累计尝试、日志失败或既有诊断计数饱和结束source/collector。本事项不在daily manifest新增transport scope、reason矩阵、current/open摘要或episode；现有reconnect/lag/drop等source counter保持既有字段和职责。
2. 纯业务静默本身没有新的协议事件，也没有官方cadence可定义开始点。本事项不得为RTDS、CLOB、aggTrade或depth新增或保留business-silence timer、离散silence-start事件或阈值；collector只保留typed raw接收时间、既有source counter中的首末接收事实、可因果取得的last qualifying business/fresh receive time和本节冻结的collection-day coverage。
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
8. 每条raw event必须在collector receive/final enqueue临界区持有并校验共同UTC day/generation permit；control/barrier消息必须绕开raw intake fence以免死锁。没有pending exit的正常rollover稳定顺序为：boundary关闭旧generation raw intake并冻结boundary known candidate；drain旧日已接纳事件；收齐四producer barrier和六writer finalize ack并确认无unclean-dominates；在同一次旧日最终写入中把nested coverage置`ended_known(end=boundary)`并关闭旧日顶层manifest；再创建新日`active(first=last=boundary, reactivated_within_day=false)`；owner ack后才释放新generation。任何中间失败都不得发布新日事件。stop/terminal与逾期boundary竞争时，gate owner必须在generation切换前原子保存并只消费一次独立`PendingExit(candidate id, original generation, classification, observed time)`，关闭raw intake，不能让它退化成旧generation迟到命令：
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
12. `M8-A`只冻结未来部署兼容策略，不表示当前runtime已经具备无竞态切换能力。首次启用必须由另行批准的部署编排在UTC边界前停止legacy service、确认旧PID退出且未创建新日manifest，并在下一UTC日启动v1 binary；旧日coverage为legacy unknown。回滚或coverage-aware升级必须由v1-aware出站路径在边界前停止采集、把当前coverage保存为`ended_known`，并通过另行实现和验证的`stop-at-boundary/no-new-day-activation`或等价编排把coverage-bearing顶层manifest按state-aware规则关闭；只有确认未创建新日manifest后，才可在下一UTC日启动入站binary。旧binary不得接管仍open的coverage-bearing日；same-day binary/version/identity takeover或identity migration不在本事项内，第9项相同identity、持有独占lease的systemd同日恢复不是部署takeover。上述部署编排或边界停止能力未另行批准、实现和验证前，不得声称代码已支持M8生产切换。
13. 中性availability不进入`source_gaps`，不增加业务`lag_events`，不触发异常告警，也不使artifact因外部coverage不足自动失败；真实SDK`Lagged(n)`、bounded queue drop、Binance confirmed sequence/ID hole等确定性缺失仍保留为明确gap/continuity evidence。固定mandatory mapping违约、coverage非法状态或版本、以及atomic manifest write失败属于本地完整性边界并fail closed；legacy v3缺对象是已定义unknown，不是完整性失败。
14. coverage只在已有真实事件、bootstrap或稀疏`DecisionWakeEvent`求值时参与后续`ObservationContract`生成known/unknown mask；它自身不创建模型trigger、wake或synthetic row。本事项不决定下游何时调用模型。核心需求已把有界离散`next_wake_bucket`批准为目标候选并保留确定性age-crossing对照基线，两者都不得反向控制subscription/transport。当前不建设精确transport episode artifact；只有未来出现经证明的cause-specific模型需求且现有事实不足时，才可另建typed availability artifact/schema与新的`ObservationContract`决策门。暂停、`MODEL_NO_SUBMIT`、取消未成交BUY和BUY对侧outcome风险中和均由模型或bundle状态机决定，不得被实现或描述成SELL、真实减仓或释放现金。

### 4.3 RTDS

1. RTDS source 构造一个长生命周期 `RtdsSdkRuntime`，只通过 latest-value desired-state 接口保存一次完整 `crypto_prices_chainlink` / `btc/usd` subscription；初始未连接时 desired 也必须保留。
2. 删除 60 秒 freshness timeout 驱动的 `request_reconnect()`、两个 generation 上限、3 秒观察窗、12 秒总期限、相应 lag/gap/告警和 fatal 分支。
3. 业务静默、持续 stale、没有 fresh payload不得调用 subscribe/unsubscribe、改变 desired revision 或切换 generation。长期静默后同一 socket 到达的合法 payload 必须继续被消费。
4. 生产配置保持 RTDS `5s/15s` heartbeat 和有界 max backoff；SDK transport owner 的总 reconnect attempts 必须为无限。启动不得以旧 20 秒 connected wait 或 3 秒 subscription-applied wait 作为 source 存活门。
5. 每个真实新 generation 由唯一 actor 创建空 subscription manager，并只 replay 一次最新完整 filtered desired；不发送丢失 `btc/usd` filter 的 unsubscribe，不复用旧 generation applied acknowledgement。
6. stale payload继续按 typed freshness 事实落盘，但 stale 本身不增加 lag、gap 或控制网络；只有 SDK `Lagged(n)` 等确定性本地丢失进入完整性处理。

### 4.4 Polymarket CLOB market 与 Gamma metadata

1. 删除 `MarketAssetFreshnessWatchdog`、首条30秒/后续60秒业务静默阈值、unsubscribe/subscribe恢复、两次resubscribe budget和第三次错误退出；不得把旧阈值保留为collector内的中性silence detector。后续`source_information_age`和availability mask由版本化`ObservationContract`计算，模型稀疏唤醒由独立`DecisionScheduleContract`处理；两者都不由collector选择阈值或生成timer。
2. CLOB desired asset set 仍由 current/prefetched/retired market lifecycle 驱动；只有 desired assets 实际变化时，现有唯一 actor 才执行 dynamic additions/removals。
3. 自然 transport reconnect 后，每个新 generation replay 当前最新完整 desired asset set；业务静默不得制造 desired revision 或 generation churn。
4. CLOB 继续使用 `10s/20s` heartbeat。SDK 单次 connection loop 可以有限尝试后返回现有外层 source-local supervisor重建 runtime，但该外层总 cycle 和总 elapsed 不得耗尽 collector 存活预算。
5. Gamma DNS/HTTP/rate limit、暂时无候选、当前市场尚未出现或rollover discovery暂时失败只形成metadata availability；当前可用market数据与其他source继续运行。服务恢复后按恢复时当前时钟重新计算current/next discovery window，禁止继续追逐已过期slug，也不回放伪造metadata。
6. Gamma/SDK 返回无法验证的 schema、矛盾 market identity、asset mapping 或 lifecycle 不变量错误仍按既有受影响范围 fail closed，不得作为普通空结果吞掉。

### 4.5 Binance aggTrade

1. 删除 60 秒无 aggTrade 驱动的主动断线、`lag_events += 1`、`source_gaps` 和 freshness reconnect reason。业务静默时继续读取同一 WebSocket，并允许正常 PING/PONG。
2. aggTrade复用depth现有30秒“完全无任何入站frame”transport-liveness边界：任意合法业务或控制frame/PING都会重新武装该边界；有PING但无trade只是business idle，不控制网络；连续30秒完全无frame才形成中性transport availability并触发同PID reconnect。该数值依据Binance官方20秒server PING合同留出余量，不是trade cadence或coverage SLA。
3. WebSocket connect error、close、EOF、read/write/protocol error、server shutdown 与服务端连接生命周期结束可以触发同 PID source-local reconnect；总次数/总 elapsed 不得成为 source 退出预算。
4. aggregate-trade-id discontinuity、payload conflict、outside recovery window 与 REST backfill 继续使用既有 continuity 合同；可证明的 ID hole 不能因“训练允许缺失”而静默忽略。
5. 每个REST backfill range/cycle继续使用既有有界重试。网络/rate-limit耗尽后记录该range为unresolved、开启新continuity epoch并继续未来live数据；不得为追逐同一个永久旧range阻塞source或无限重试。后续新请求与新尾部的网络availability继续恢复；无法验证的payload/schema、重复冲突或内部coordinator不变量仍按既有合同处理。
6. 长期无成交后同一 socket 到达的首条合法 aggTrade 必须正常消费，不经过为静默而制造的 reconnect。

### 4.6 Binance depth 与 snapshot

1. 必须区分“没有 depth 业务 update”和“完全没有任何入站 frame”。正常收到 PING 或其他合法控制 frame时，业务静默不得触发 reconnect、lag 或 gap。
2. 完全无入站 frame、connect/read/write/close/protocol failure可以作为 transport availability 触发 reconnect；该事实不得仅因断连写入 `source_gaps` 或异常告警。
3. 每次真实新连接后仍必须重新获取 REST snapshot，并按 Binance 官方规则完成 snapshot/update-id stitch 后才发布可用本地订单簿。
4. `u <= snapshot lastUpdateId`或`u <= local_u`属于官方stale/duplicate路径，必须丢弃旧帧且不得记gap或fatal；`U > local_u + 1`等可证明的update-id hole才是confirmed continuity gap，必须丢弃旧本地簿、记录确定性缺失证据并重新snapshot。
5. snapshot HTTP网络失败、timeout或rate limit在同一source内持续恢复；`U/u`字段无法解析、`U > u`等已冻结结构不变量、snapshot/schema无法验证或本地book不变量破坏继续fail closed。

### 4.7 SDK-first 最小 patch 决策门

本事项保持pinned官方`0.6.0`与现有`third_party/polymarket_client_sdk_v2-0.6.0-p1`根，不在同一事项升级SDK版本。实现前必须把vendored SDK与官方`0.6.0`原版、当前官方release/main及相关已合并/未合并PR再比较一次；上游已有且满足本地合同的修复优先按官方hunk/设计最小回移到pinned vendor，未吸收的真实缺口才保留本地最小实现。若希望采用更新官方release，必须停止并建立独立SDK版本升级决策门，重新冻结Cargo/lock/vendor路径、API兼容和全量回归范围。不得仅因为“官方原版”就放弃已证明的数据完整性边界，也不得仅因为“当前已有补丁”就永久保留。

当前拟议 patch 语义矩阵如下：

| patch 语义 | 与错误业务静默策略的关系 | 本需求决定 |
|---|---|---|
| parser failure通过 typed sticky fault暴露并终止连接 | 无关 | 在pinned 0.6.0 vendor保留；可用官方等价hunk替换本地hunk |
| CLOB interested array按 all-or-error解析 | 无关 | 保留，禁止静默跳过损坏元素 |
| `SimpleParser: Default` | 无关 | 在pinned 0.6.0 vendor保留；可回移官方等价hunk |
| outbound request绑定 connection generation并隔离旧 generation | 无关 | 保留；自然 reconnect 同样需要。RTDS已接入，CLOB生产actor必须在阶段4补齐同等绑定与发送后复核 |
| RTDS parser failure有界、脱敏诊断 | 无关 | 保留目标并在阶段2补齐：当前wrapper仍把原始serde Display拼入外显错误，非法scalar可泄漏；必须保留kind/source chain但让sticky fault、Display与parser-failure warn只含有界allowlist摘要，不改变成功解析语义；pre-parse TRACE仅按下一行B例外处理 |
| 官方pre-parse `tracing::trace!(%text, ...)`是否恢复 | 无关；当前项目未启用SDK `tracing` feature，但这是现存且未被原获批矩阵命名的vendor hunk | **B已批准**：恢复官方0.6.0原hunk。启用该feature时会在parse前记录全部文本帧，包含随后解析失败的完整raw frame；用户已明确批准该failure-trace边界放宽 |
| generation-aware one-shot `request_reconnect()` | **直接服务旧 60 秒业务静默策略** | 删除 vendor 公共能力、runtime wrapper、生产调用及专属测试 |
| heartbeat dead向 connection owner传播并结束旧 socket | 新确认 transport 缺口 | 优先复用/审查官方修复方案；未发布时允许最小 vendor patch |
| 最后一个 manager sender关闭后的 normal teardown | 新确认生命周期缺口 | 优先复用/审查官方修复方案；未发布时允许最小 vendor patch |
| 单次TCP/TLS/WebSocket建连与握手有界 | **与健康连接上的业务静默无关**；pinned SDK直接等待`connect_async()`，TCP已接受但握手不完成时不会启动heartbeat或进入下一次重试 | **CT-B已批准**：SDK `ws::config::Config`增加默认`None`的`connect_timeout`；RTDS生产显式设为20秒，超时作为recoverable transport failure，之后有界backoff并无限重试。不得把该期限用于已连接socket、业务frame等待或subscription refresh |

因此：

1. 不采用“整体恢复官方原版 SDK”。
2. 不再写“六项扩成八项”或其他固定数量合同；以上逐项语义矩阵才是 authority。
3. 删除 one-shot reconnect 后，connection generation 的自然重连隔离必须继续存在，二者不得混为一个补丁。
4. 每个CLOB与RTDS subscription actor必须先观察当前connected generation，再通过既有`bind_generation(observed_generation)`构造该generation专属manager；发送后必须复核generation与connected state仍匹配，只有复核通过才可发布applied ack。CLOB applied ack必须改为项目私有的generation-tagged类型，waiter同时校验ack generation等于当前connection generation、revision满足且state仍connected；这样即使在发送后复核与ack发布之间切代，旧ack也不能满足新代wait。旧actor/旧generation延迟发送不得被重新标记为新generation、写入新socket或被误认已应用。该要求只补齐既有generation隔离能力的生产调用，不新增第二actor、第二receiver或公共SDK API。
5. heartbeat timeout或旧 PONG 必须使当前 `handle_connection` 返回既有 recoverable transport error、关闭旧 socket并进入现有 connection loop；不得新增第二个 socket owner或公共 shadow API。
6. connection owner必须使用互斥的内部退出语义：只有最后一个manager/bound outbound sender关闭是normal shutdown并停止outer loop；read EOF、WebSocket Close、read/write/PING failure、heartbeat timeout或旧PONG继续是recoverable transport exit；parser sticky fault继续terminal。可以增加私有内部结果类型或等价判断，但不得增加公共API或把其他`Ok(())`路径误判为normal shutdown。
7. 按用户批准的B恢复官方0.6.0 parse前`tracing::trace!(%text, ...)`原hunk；启用SDK `tracing` feature时，完整成功/失败文本帧均可进入该TRACE记录，这是明确获批的日志边界放宽，不得把它误报为脱敏。parser错误的operator-visible `Display`、sticky fault和warn仍不得额外原样拼接serde错误中的非法scalar或nested payload/secret-like字段，只含2,048-byte上限、hash与allowlist字段摘要；底层error kind/source chain保留供typed inspection。不得另造success-only raw trace或扩大其他日志行为。
8. vendor 允许修改的源码路径集合按用户批准的`CT-B`限于以下四个文件：
   - `third_party/polymarket_client_sdk_v2-0.6.0-p1/src/ws/connection.rs`
   - `third_party/polymarket_client_sdk_v2-0.6.0-p1/src/ws/config.rs`
   - `third_party/polymarket_client_sdk_v2-0.6.0-p1/src/clob/ws/types/response.rs`
   - `third_party/polymarket_client_sdk_v2-0.6.0-p1/src/rtds/subscription.rs`
9. `CT-B`只批准新增上述`config.rs`及默认`None`的公开配置字段；如果Polymarket实现还需要第五个vendor文件、其他新公共协议API、raw WebSocket、manual signing、第二SDK session或改变parser/fatal语义，必须立即停止并向用户提交缺口、影响和选项。

### 4.8 完整性与顶层协调边界

1. 下列事实继续保留为 confirmed gap、quarantine 或 fail-closed 信号：SDK `Lagged(n)`、bounded queue timeout/drop、Binance sequence/ID hole、payload conflict、parser/schema failure、dedupe conflict、writer/manifest/atomic publish、磁盘/资源硬边界和内部不变量。
2. 顶层“任一mandatory producer/writer/ingress在运行deadline前意外结束即失败”保护继续存在；测试必须证明availability路径不会返回，而完整性注入仍会关闭共同gate并返回正确terminal error。
3. 不得把source/writer/ingress panic、join error、control channel意外关闭或writer失败重新分类为普通external availability；能由wrapper在退出前可靠观察并持久化的terminal按第4.2节写known end，其他异常只保留unknown tail。
4. confirmed gap 可以保留模型可见 missingness，但不得伪造行情、自动 backfill无官方依据的数据或用后续恢复抹除缺失区间。

### 4.9 配置、资源与停止

1. 已提交的分源heartbeat修复作为本事项实现基线和回归合同保留：CLOB按官方client PING cadence使用`10s` interval并保持项目`20s` timeout，RTDS按官方client PING cadence使用`5s` interval并保持pinned SDK默认`15s` timeout；`20s/15s`不得冒充官方共同保证，两者也不得再共用错误数值或在本计划中重复实现同一split。
2. 各 source 的单次 connect/HTTP/read/backoff timeout保持有界，最大 backoff有上限并加入既有 jitter；外层恢复无总次数/总 elapsed退出预算且禁止 tight loop。
3. 每个 Polymarket source最多一个 active connection generation、一个 actor、一个 receiver和一个 desired-state owner；每个 Binance source最多一个 active WebSocket和一个必要的 snapshot/backfill任务。
4. 至少 100 个恢复周期后 task、FD、socket、queue、watch、actor、receiver、subscription refcount、pending snapshot/backfill不得单调增长。
5. deadline、SIGTERM/operator stop和既有 day/segment finalize必须在永久离线/静默时按既有期限完成 drain/finalize。

## 五、规范衔接

本文已于2026-07-17最终获批；阶段0必须按语义同步：

- `demand/data-collect-demand.md` 中“空结果、长时间静默、stale、断线一律写 source gap”、所有 source 必须有限 attempts/max elapsed/exhausted、non-empty external availability gap 导致 artifact 失败的旧语义，以及旧daily manifest允许字段清单；该清单必须加入第4.2节顶层、单owner、版本化`collection_day_coverage`，保持六个既有source counter职责不变，且仍只有每日唯一manifest；
- `demand/data-collect-reliability-repair-demand.md` 中 RTDS 两层有限恢复、CLOB 静默退出、旧六项 vendor patch 白名单、断线/静默 gap、systemd 新 PID恢复语义和旧daily manifest“只记录”字段清单；
- `plan/data-collect-reliability-repair-plan.md` 中对应有限 recovery budget、RTDS D2、vendor patch步骤，以及daily manifest字段与coverage验证步骤；reliability demand/plan都必须采用第4.2节同一顶层v3/嵌套v1兼容、restart/cross-day和UTC日边界部署语义；
- `demand/rebuild-core-demand.md` 已按本轮用户批准同步训练采集退化、模型输入和完成验收的对应条款：RTDS-only网络动作已改为全部训练外部源的source-local协议恢复，daily manifest只提供单一collector observation-eligibility coverage与既有source counters，不持久化transport current/open/episode，也不能降低模型学习真实或模拟missingness的目标；核心需求中已批准的离散`next_wake`目标候选及其`DecisionScheduleContract`只作为下游边界引用，不得反向扩入本collector修复。实施阶段只复核并保持这些语义一致，不重复扩大核心草案范围。

同步必须遵守：

1. 旧 changelog 是当时批准、实现和事故的历史事实，不回写或删除；新 changelog记录取代关系。
2. 本文只修改 training collector；paper/live共享 availability/missingness语义，但连接生命周期和权限必须由各自需求另行批准。
3. “完整 artifact”指 schema、manifest、provenance、因果时间和缺失编码完整，不等于所有外部源全时段有行情。
4. 当前 `demand/rebuild-core-demand.md` 仍整体处于审核中；同步这里只消除本事项已由用户决定的网络语义冲突，不借机批准其余核心重建内容。
5. 用户已批准本文、计划与SDK矩阵；阶段0完成authority同步和SDK基线审查、commit/push前，不得进入阶段1代码实现。

## 六、验收标准

### 6.1 共同生命周期

1. **单源静默：** 分别让 RTDS、CLOB、aggTrade 和 depth业务流长期无事件但 transport按协议存活；跨越多个旧 freshness/budget周期后，source与collector仍运行，不产生静默驱动 subscription/reconnect、gap、lag或告警。
2. **单源不可达：** 分别让每个外部 source连续 connect/HTTP失败超过旧 attempts/max elapsed预算；同一 PID保持运行，其他健康源继续写盘；恢复后该 source自行恢复。
3. **多源退化：** 同时断开至少两个不同协议源，未断开的 source继续采集和 finalize；collector不出现 `source_task_ended_unexpectedly`。
4. **全部源不可达或业务静默：** 四个逻辑source task全部transport不可达时，collector保持同一PID和有直接证据的degraded、逐源恢复；四个transport按协议存活但全部业务流无数据时，collector保持同一PID active/idle，只保存active coverage与nullable last-qualifying，transport事实仅在source-local运行态/有界operator日志中可见且不写daily manifest，也不创建虚假degraded/silence事件。两种场景下operator stop都可完成。
5. **coverage与缺失保留：** current-day恢复前取得并持续持有dataset独占实例lease；coordinator再同步建立bounded event queue、唯一receiver并确认ingress循环可进入，六writer与四producer wrapper全部spawn、初始化ready并阻塞在共同gate；ManifestOwner只执行一次不重试的atomic activation，ack后才释放网络poll。零行业务源仍有共同`active` coverage，未达到共同起点保持`not_started`。无PendingExit的正常rollover必须覆盖新日frame与boundary timer竞态、逐event day/generation permit、旧日drain、barrier/finalize ack、旧日顶层关闭、新日active commit和owner ack；跨界PendingExit按两个持久化切点与一个release切点验收：旧日final-close前无法由fence排除的unclean令旧日unknown；旧日已经closed而新日activation commit未成立时不重写旧日，新日只能missing/not-started且布尔值不变；activation commit后状态与`reactivated_within_day`已经归属新日，即使ack丢失也不得回滚，ack只控制是否release。任何pending exit都不得伪造新日active或提前resume。可靠close候选之间first-close-wins，但最终fence前发现的非协调unclean必须覆盖known candidate；协调退出必须有精确task级ack/permit证据。四种current-day nested状态到active的转换必须证明首次activation为`false`、任一同日后续activation为`true`且保持单调。`reactivated_within_day=true`只隔离最后start前不能证明连续coverage的replay区间，不得回写历史Observation；unclean共同尾界只能是last start，另一健康源的更晚行不得延长。typed raw接收事实不得被抹除，也不得生成collector silence-start timer或伪行情。
6. **availability与真实terminal：** 已确认发生在外部connect/read/write/HTTP/WebSocket边界的故障必须在对应source-local owner内持续恢复，不能仅因缺少专门诊断reason、持续时间、尝试次数、既有诊断计数饱和或日志失败结束source/collector；parser/schema、SDK lag、queue drop、writer/manifest和内部不变量故障注入不得被恢复循环吞掉。producer/writer task的可靠normal/terminal return必须按共同gate合同冻结known candidate；coordinator ingress路径提前退出、唯一receiver ownership丢失或queue不变量失败也必须结束coverage，并按能否可靠完成共同fence决定known end或unknown tail。非协调panic/abort/join failure为unknown tail，顶层仍发现terminal task；stop已发但cancel生效前panic、无/错abort permit、ack丢失与具体handle `is_cancelled()`必须证明只有可因果归属本次close的协调退出获得豁免。不得把同步构造的queue或ingress路径冒充独立spawn task。

### 6.2 RTDS 与 SDK

7. **RTDS长期静默：** server持续正确PONG但不发业务frame；generation不变、wire subscribe总数为一、source不退出。随后同一 socket发送stale/fresh payload均被正确消费，stale不增加lag/gap。
8. **RTDS启动离线：** 连续连接失败超过旧3次/20秒门后启动server；source未提前返回，首个generation只发送一次完整含 `btc/usd` filter的subscription。
9. **RTDS运行中恢复：** 真实close/no-PONG后连续失败再恢复；每个成功generation最多一次latest desired replay，无业务静默主动ticket。
   单次TCP/TLS/WebSocket建连与握手必须受20秒期限约束；超时只结束本次attempt并进入既有有界backoff，不能结束source或成为业务静默计时器。loopback必须覆盖TCP已接受但不完成WebSocket握手，随后下一次连接成功并只replay一次完整desired。
10. **删除 one-shot能力：** 生产和测试代码不存在 `request_reconnect` 公共/包装/调用路径；vendor差异中不存在该专用票据状态。
11. **heartbeat无PONG：** loopback server保持socket开放但不PONG；旧socket在timeout后关闭并进入既有reconnect loop，generation增加。
12. **manager drop与反向transport回归：** 正常PONG时分别在裸`ConnectionManager`和`MarketSdkRuntime/RtdsSdkRuntime`集成层drop全部manager/actor持有者；server观察旧socket关闭、task收敛且不建立新连接。反向测试必须证明read EOF、Close、write/PING failure和heartbeat failure仍进入reconnect，parser fault仍terminal。
13. **CLOB共享heartbeat回归：** generic manager对CLOB parser同样能从no-PONG恢复，同时CLOB `10s/20s`、parser/fault和dynamic desired语义不变。

### 6.3 CLOB 与 metadata

14. **CLOB静默：** current/prefetched asset跨越至少三个旧30/60秒等价周期仍不unsubscribe/resubscribe、不退出；随后业务事件由同一generation消费。
15. **CLOB lifecycle更新与generation隔离：** 只有current/prefetched/retired desired set真实变化才发送精确add/remove；自然reconnect后只replay最新完整set。CLOB actor必须按观察到的generation绑定manager并在发送后复核state/generation；私有applied ack携带generation，waiter同时校验当前generation、revision与connected。旧generation延迟发送不得落入新socket；发送后复核与ack发布之间切代时，旧ack也不得满足wait。
16. **Gamma不可达/空候选：** 连续网络失败或暂时无候选不结束market source/collector、不写confirmed gap；服务恢复后按当前时钟重算current/next window，且不继续请求已过期slug。
17. **metadata完整性：** 矛盾identity/schema/asset mapping负例仍按既有合同失败，不能被空候选恢复循环吞掉。

### 6.4 Binance

18. **aggTrade静默：** server持续PING但不发aggTrade，跨越多个旧60秒周期不主动断线、不增加lag/gap；随后同socket首条合法aggTrade正常消费。
19. **aggTrade transport与continuity：** 每个合法PING/frame重置30秒全frame inactivity边界，完全无任何frame才触发中性transport reconnect；close/EOF/server shutdown/连接失败后同PID持续恢复。ID hole触发既有有界backfill，单range耗尽后保留unresolved并开启新epoch继续未来live数据，不永久追逐旧range；payload/schema负例仍terminal。
20. **depth业务静默：** 持续收到PING但没有depth update时不重连、不写gap；完全无入站frame时进入transport恢复并重新snapshot。
21. **depth update-id三分支：** `u <= snapshot/local_u`按stale/duplicate丢弃且不记gap；`U > local_u + 1`记录confirmed continuity evidence、丢弃旧book并重新snapshot；无法解析`U/u`、`U > u`和本地book不变量负例必须按冻结scope失败。
22. **snapshot恢复：** HTTP timeout/rate limit超过旧预算后source仍运行并重试；合法snapshot恢复stitch，非法schema仍按完整性合同处理。

### 6.5 资源、产物与总门禁

23. **manifest coverage与训练基础：** 每日唯一顶层v3 manifest只新增直接嵌套的`data_collect_collection_day_coverage_v1`，不新增transport scope/reason/episode、通用extensions容器或逐source coverage复制。六个既有source counter继续保存artifact首末接收事实及既有操作计数。兼容矩阵必须逐项验证：prior legacy缺对象始终可读且不插入coverage；相同新identity的current manifest缺对象按新writer损坏失败；不同identity current legacy继续被既有gate拒绝；unknown nested version/field/type/time/state失败；所有ManifestOwner mutation保留对象。dataset独占lease、四种current-day activation、顶层open/closed、写成功但ack丢失和时钟倒退必须逐项测试；首次activation为`reactivated_within_day=false`，任一同日后续activation原子提交后为`true`且不被unclean/known-end/reconcile转换清除。prior active/unclean-end-unknown/not-started/ended-known/legacy missing分别验证nested state、顶层state、closure reason与gap：active转unclean但不改变布尔值，既有unclean幂等确保unclean gap/closure，其他三类中性关闭。状态/时间不变量、ready/activation ack、可靠known-close候选first-close-wins、unclean-dominates、task级协调abort证明、PendingExit跨日归属、正常rollover rendezvous及replay eligibility均按第4.2节通过测试。typed raw、last qualifying和known coverage让后续模块因果派生missingness；不得生成额外receipt/sidecar，不进入`source_gaps`，也不因外部coverage不足自动把结构完整的训练artifact判失败。M8只作为未来部署前置合同；当前实现验收不得冒充已经具备stop-at-boundary或生产切换能力。
24. **无collector静默timer：** 本事项不在data-collect中生成business-silence timer、离散silence-start事件、周期性合成market row、周期subscription refresh、`DecisionWakeEvent`或fixed model decision tick；模型可见age/mask计算、确定性调度基线和离散`next_wake`目标候选均不由本事项实现。
25. **资源有界：** 每个协议至少100次恢复后task、FD、socket、receiver、actor、queue、watch、subscription refcount、snapshot/backfill任务无单调增长。
26. **配置回归：** CLOB为 `10s/20s`，RTDS为 `5s/15s`；RTDS `connect_timeout=20s`、transport总尝试无限，各源单次backoff有界。SDK默认`connect_timeout=None`保持上游兼容；CLOB在阶段4显式启用同一20秒单次建连/握手期限，不得提前把它解释为heartbeat或业务静默期限。
27. **全量门禁：** 定向loopback/contract测试、`cargo test -p data-collect --locked`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo fmt --all -- --check` 与 `git diff --check`全部通过。
28. **范围门禁：** 没有新增Polymarket raw WebSocket/shadow SDK或任何source的第二socket owner；Binance只保留现有唯一direct WebSocket owner；没有CLOB user源、data-pipeline/model策略实现、部署或数据清理；vendor源码路径集合精确为第4.7节用户最终批准的四个文件，实质差异与逐项矩阵一致。

## 七、完成定义

只有同时满足下列条件，本事项实现阶段才可标记完成：

1. 本需求、对应计划和 changelog 已获用户明确批准并完成 authority 同步；
2. 第六节28项验收均有测试名、命令、结果或artifact证据；
3. 每个实现阶段自审和独立审查均无未解决的 Critical/Important；
4. 相关commit已精确stage并push，未混入用户现有无关改动；
5. 生产构建、部署、服务操作和数据处理仍保持未授权，等待独立用户批准。
