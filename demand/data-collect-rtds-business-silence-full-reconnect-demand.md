# RTDS 业务静默彻底换连接恢复需求

> 事项编号：`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`
>
> 对应计划：`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`
>
> 对应 changelog：`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_SHARED_RTDS_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FAIL_INCOMPLETE_8H_P_B1E_IMPLEMENTATION_CLOSED_ATTEMPT5_FULL_COLLECTOR_PENDING`。其中`P-B1E`是本次获批的“nullable Decimal精确空串兼容”唯一标识，用于避免与Attempt1辅助文件污染处置的历史`E-A`重名；该状态仅在承载本记录的纯文档closure commit成功push且本地HEAD、upstream与远端ref一致后生效。RTDS network-error方案A的authority/Rust commits `366fed7d`/`415470e`、Gamma `closedTime`方案A的`3a367386`/`e086017f`及P-B1E的`15feb12`/`9d8e59b`均已分离发布并完成三方ref核对。阶段0和阶段1均已完成；Attempt1、Attempt3、Attempt4永久为`FAIL + INCOMPLETE_8H`，Attempt2永久为`USER_TERMINATED + INCOMPLETE_8H`。P-B1E的定向验证与三路终审已经通过；当前唯一下一步是从新clean/pushed HEAD、新artifact和空dataset root执行完整六源8小时Attempt5。该本地部分日前向验证不等于production binary激活、完整UTC日前向长跑或canonical cutover。当前代码只有training拥有真实RTDS adapter；future paper/live真实RTDS adapter须由独立事项接入并继承同一恢复合同。production release、部署、远端操作和数据清理仍不在本需求范围。

## 一、目标与已决定方向

本事项不补回静默期间缺失的历史数据。当前实现目标是：训练 collector 已收到过合格的 Polymarket RTDS Chainlink `btc/usd` 实时更新，之后在连接仍处于可用状态时持续没有新合格更新并达到批准阈值，则在同一 collector PID 内彻底结束旧 RTDS 网络生命周期，再创建一个新的官方 Polymarket Rust SDK RTDS runtime，以尽量缩短未来实时数据恢复时间。

同一网络生命周期行为同时冻结为 training、paper、live 三种独立程序的未来共同 RTDS 合同。paper/live 当前只有 heartbeat/status server，没有真实 RTDS adapter；本事项不新增该 adapter、handoff producer、模型接线或部署。以后由独立三件套接入真实 paper/live RTDS adapter 时，必须继承本需求的合格 fresh 分类、四项主要参数、outer backoff、单 owner/socket、break-before-make、触发/撤销/重置和恢复成功语义，不得再选择另一套恢复政策。training 的 Parquet、manifest、dedupe restore 和 `SourceEventSink` 不属于共同合同。

恢复路径必须满足：

1. 不在同一 WebSocket 连接中重复订阅，也不执行 `unsubscribe + subscribe`；
2. 旧 runtime、订阅 actor、receiver、heartbeat 和 SDK connection loop 完成退出后，才创建新 runtime；
3. 任意时刻最多一个活动 RTDS socket；
4. 每条新连接只应用一次最新完整的 `crypto_prices_chainlink` / `btc/usd` filtered subscription；
5. 只有收到第一条新的合格实时更新才算业务恢复。

该机制只重建 RTDS source-local 网络生命周期，不结束 collector、不触发 systemd restart、不影响其他健康 source，也不替模型放宽 missing、stale、invalid 或执行安全门。本需求不承诺盈利，也不承诺彻底换连接能够消除共享上游停流。

## 二、已核实事实与 authority 边界

### 2.1 协议、事故与外部证据

- Polymarket 官方 [RTDS 文档](https://docs.polymarket.com/market-data/websocket/rtds) 规定客户端每 5 秒发送 `PING`，但没有规定 Chainlink `btc/usd` 的业务更新 cadence、业务静默 SLA、订阅 ACK 或缺口回补合同。
- 官方 TypeScript 客户端在 WebSocket `error` / `close` 后重连；其订阅回调只能证明本地 send 路径，不是服务端 ACK。
- 2026-07-20 本项目核实到 RTDS 在 `2026-07-20T00:32:36.493Z` 至 `2026-07-20T02:13:51.836Z` 业务静默，随后在原客户端路径自然恢复，并直接出现当时的新鲜更新，没有补发静默区间。该事实不能区分共享上游、服务端转发、单后端、单连接或本地接收故障，也不能证明主动换连接必然更早恢复。
- Polymarket [real-time-data-client Issue #26](https://github.com/Polymarket/real-time-data-client/issues/26) 是未关闭的 `activity` feed 用户报告，只支持“socket 保持开放但业务数据停止、用户以重连作为 workaround”这一相似症状；它不是 Chainlink 专项证据，也没有维护者根因或自然恢复证明。
- 不同端点的 [py-clob-client Issue #292](https://github.com/Polymarket/py-clob-client/issues/292) 只能作为“反复新连接仍可能无数据”的跨端点反例，不能证明本次 RTDS 故障根因。

### 2.2 阶段0代码快照与阶段1结果

- `RtdsSdkRuntime::replace_subscription()` 更新 latest desired；同一真实连接已应用相同订阅时，不会用 revision 变化冒充网络重订阅。
- pinned 官方 Rust SDK 对同 topic/type 再次 `subscribe()` 只增加本地引用计数；`unsubscribe()` 请求不保留原 `btc/usd` filter，因此二者均不适合作为恢复路径。
- 阶段0快照中，`RtdsSdkRuntime::Drop`、最后一个 `ConnectionManager` owner 释放及已有 `dropping_rtds_runtime_closes_socket_without_reconnect` loopback 测试只提供收敛基础，vendor SDK没有公开异步shutdown。阶段1已在项目wrapper实现消费式`shutdown_and_wait(self)`，并以state-channel closure loopback证明返回时actor和connection loop已经结束；没有修改vendor SDK。
- 当前 `applied` 事件发生在项目 actor 调用 SDK `subscribe()` 并完成本地发送入队之后；SDK `send()` 本身是向内部 mpsc 入队，不是 wire write completion 或服务端 ACK。因此生产指标不得把 `applied` 命名为 wire send、握手完成或 ACK。
- 阶段0快照中，`RtdsSdkRuntime`的state/applied receiver仍是wrapper私有字段，source owner尚未收到完整观测。阶段1已把initial及后续generation的state/local-applied纳入唯一owner消费的同一`RtdsRuntimeEvent`流，并保留queued-message freshness fence；没有第二receiver、actor、socket或vendor patch。
- 当前 coordinator 对 RTDS source task 使用现有 abort permit，并在随后等待 source task 结束。pinned Tokio 的取消语义保证 task 结束被观察到前会销毁 task-local 变量；这足以保持现有 collector stop 合同。本事项只为业务 replacement 增加可等待的 wrapper shutdown，不把 operator stop 扩张成新的 typed control channel、completion reply或 stop/create 线性化事务。
- 当前 `require_text_pong=false`；`PING` 写成功既不是业务健康证据，也不是必须具备 PONG 的完整往返证据。本事项不改变该合同。
- 阶段2 Attempt1运行`16851391ms`后收到127-byte顶层服务错误对象`message="Network error communicating with endpoint"`。正常RTDS typed parse将其作为parser error结束source，故probe在8小时之前失败。该对象没有`topic`、`type`、`timestamp`或`payload`，不能形成RTDS业务消息；它证明当前SDK缺少一个已获用户批准的服务端网络错误窄分类，不证明应放宽其他typed/parser/schema错误。

### 2.3 阶段0识别的 authority 冲突及已完成同步

阶段0在当时执行authority中识别出以下必须先同步的旧语义：

1. `demand/data-collect-demand.md` 的 RTDS 观测说明当时把 subscribe send、ACK/首条消息混在同一证据链，且第 3.3 节第 4--5 项要求业务静默不切换连接；
2. `demand/data-collect-source-availability-lifecycle-repair-demand.md` 的第一节第3项、第4.2/4.3节及验收AC1/AC24/AC25当时与新路线直接冲突；AC4没有说明RTDS此前是否收到过fresh而存在分支歧义。AC7的cold-start同socket、AC9的真实transport恢复且无one-shot ticket、AC10删除`request_reconnect`均与新路线相容，必须保留；
3. `demand/data-collect-reliability-repair-demand.md` 已被部分取代，但其 SDK-first、唯一 owner、数据完整性等仍有效合同必须保留；
4. `demand/rebuild-core-demand.md` 尚未整体批准，只作为未来一致性上下文；既有 plan、test 和 changelog 中“长期静默保持同一连接”是历史事实，不得删除或改写成从未存在。

阶段0已按精确矩阵同步前两份current authority中相反的行为和证据语义。本需求对training collector的`polymarket_rtds_chainlink_btc_usd`形成精确实现例外，并为future paper/live真实RTDS adapter冻结同一网络生命周期合同；没有批量扩张到CLOB、Gamma、Binance，也没有把共同合同误写成当前paper/live adapter已实现或已验证。

## 三、术语与可观测时间

1. **合格实时更新**：沿用当前分类、freshness 和结构去重路径，同时满足 `topic=crypto_prices_chainlink`、`type=update`、`payload.symbol=btc/usd`、timestamp 合法、`freshness_status=fresh`，且不是重复 payload。
2. **业务静默**：当前连接曾收到合格实时更新后，按 monotonic clock 从最后一条合格实时更新起持续没有下一条合格实时更新。snapshot、wrong topic/type/symbol、invalid timestamp、stale 或 duplicate 均不重置计时。
3. **`rtds_runtime_epoch`**：source 每次创建或替换整个 `RtdsSdkRuntime` 时递增的本地编号。采用该名称是为了避免与 raw-day `RuntimeEpoch` 同名异义；它只用于所有权、日志和测试断言，不进入 typed row/schema，也不冒充服务端 generation。
4. **`sdk_connection_generation`**：同一 runtime 内由 SDK 管理的自然 transport 连接代次。创建新 runtime 后该值可以重新从 SDK 初始值开始；不得把它当成跨 runtime 单调编号。
5. **本地订阅 applied 观测**：复用并最小扩展既有 applied 事实，使 owner 能观察当前 `rtds_runtime_epoch` / `sdk_connection_generation` 的 latest desired 已由 actor 交给 SDK 本地发送队列及其 monotonic 时刻；这不要求另建一套 `LocalApplied` domain type。它不是 wire write completion 或服务端 ACK。
6. **彻底关闭**：项目订阅 actor 的 `JoinHandle` 已完成，旧 receiver 和全部 manager owner 已释放，保留用于等待的 SDK state receiver 已观察到 state channel 关闭，因而 connection loop 已完成且不再重连，底层 WebSocket 已关闭。`Disconnected` 只可作为中间状态，不能单独证明 task 完成；实现不要求服务端 Close ACK。
7. **恢复成功**：目标 runtime/generation 在本地 applied 后收到第一条新的合格实时更新。Connected、`PING` 写成功、本地 applied、snapshot 或历史/重复消息都不能单独证明恢复。
8. **最小恢复日志**：每次实际 business replacement 只记录 `trigger_kind=steady_silence|first_fresh_window_timeout`、`rtds_runtime_epoch`、当时可见的 `sdk_connection_generation`、`attempt_index`，以及 replacement start/runtime created 两个顺序事件。不为诊断增加四段专用计时状态、counter、schema或test seam；没有真实 replacement 时不推导恢复耗时，也不得把 local-applied 命名为 handshake、wire subscribe 或 ACK。
9. **recovery episode**：当前没有 episode 时，由 steady threshold 命中，或一个已启用的 `first_fresh_observation_window` 首次到期且仍无 fresh，二者之一启动；到下一条合格实时更新到达结束。一个 episode 可以按观察窗口和 outer backoff 顺序执行多轮 runtime replacement，但任意时刻最多一个在途恢复动作；后续窗口到期或仅创建新 runtime 都不重启或结束 episode。

## 四、范围

### 4.1 包含

- training collector 的 `polymarket_rtds_chainlink_btc_usd` source；
- future paper/live 真实 RTDS adapter 必须继承的共同网络生命周期与验收合同；本事项不实现这些 adapter；
- business watchdog 与 SDK transport 状态的串行仲裁；
- 最小 wrapper-only state/generation/local-applied observation surface与业务 replacement 专用的 awaitable shutdown capability；
- break-before-make teardown、新 runtime 创建、一次 filtered subscription 和重复静默的有界退避；
- `rtds_runtime_epoch` / `sdk_connection_generation` 的日志和测试语义；
- 定向单元/loopback，以及一个连续两次 replacement、共三个 runtime epoch 的聚焦场景；
- 实现与测试完成后，在本地香港 VPN 出口运行连续 8 小时完整`data-collect-training-collector`，让全部六类正式数据源写入新的本地隔离dataset root及正式Parquet/manifest；
- 后续获批实现所需的本地开发构建、测试和静态检查。

### 4.2 不包含

- 同连接重复 `subscribe`、`unsubscribe + subscribe`、并行备用连接、shadow client、make-before-break 或第二 writer；
- raw WebSocket、manual protocol、manual signing 或绕过官方 Polymarket Rust SDK；
- 新的统计故障检测框架、通用状态机框架、配置系统、数据路径 epoch validator/fence 或 test-only 资源计数基础设施；
- 回补、伪造、删除静默区间数据，或修改 typed row、manifest、ObservationContract、模型与交易语义；
- 本RTDS事项内的CLOB、Gamma、Binance行为修改；Attempt4暴露的P-B1E由
  `DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`三件套单点定义和实现，本事项只把
  其闭合作为Attempt5前置，不建立第二份parser authority；
- 当前 paper/live RTDS adapter、external-event handoff producer、模型接线、长跑与部署实现；
- production release 构建、上传、部署、systemd、远端服务操作或生产数据清理。

## 五、功能、安全与验证需求

### 5.1 单 owner 与 transport 优先仲裁

1. 使用一个结构化 async ownership loop 管理唯一 `RtdsSdkRuntime`、source-task 级 `ever_received_fresh`、最后一条 fresh 的 monotonic 时刻、首条 fresh 窗口和外层 business backoff；除非测试证明必要，不新增大型形式化状态机或恢复框架。
2. 项目 wrapper 必须通过唯一 source owner 消费的同一 `recv_event` 流提供 current state + generation transition，以及 initial 和后续 generation 的 local-applied 事实；该观测不改变 queued-message freshness fence，不修改 vendor SDK，也不增加第二 RTDS message receiver、subscription actor或网络 owner。
3. business deadline 只在当前 runtime 的 SDK 状态为 `Connected`、latest desired 对当前 `sdk_connection_generation` 已本地 applied，且没有 transport fault 时消耗时间。
4. 在 owner 开始消费式 `shutdown_and_wait(self)` 之前，进入 `Connecting`、`Reconnecting` 或 `Disconnected` 时立即撤销旧 business deadline 和正在等待的 outer backoff，transport 恢复交给 SDK 现有 inner reconnect/backoff；不得因业务计时器销毁一个正在自然恢复的 runtime，也不得重置 SDK inner backoff。shutdown 调用开始后按普通顺序控制流等待完成，不再恢复旧 runtime。
5. `ever_received_fresh` 在 source task 启动时为 `false`，收到第一条合格实时更新后置为 `true`，跨同一 source task 内的 runtime/generation replacement 保持为 `true`，不写 schema 或持久化 sidecar。
6. SDK 自然进入新的 `sdk_connection_generation` 后，不得恢复上一 generation 的 steady deadline。若 `ever_received_fresh=true`，待 `Connected + local applied` 后无条件进入首条 fresh 窗口；若仍为 `false`，保持 transport-only 等待直到首条合格 fresh 到达，不运行任何 business watchdog。
7. 当前 generation 收到第一条合格实时更新后才进入 steady-state；之后只有新的合格实时更新重置 steady deadline。
8. `require_text_pong=false` 保持不变；不得增加 PONG 依赖来证明业务健康。
9. 方案A只在vendor SDK正常`RtdsMessage`解析失败后检查原始帧：只有顶层JSON object、`message`精确等于`Network error communicating with endpoint`，且不存在RTDS数据身份字段`topic`、`type`、`timestamp`、`payload`时，才生成crate-private可恢复远端网络marker。不得要求对象字段集合完全相等，也不得要求`connectionId`或`requestId`存在；marker只保留原parser source与既有bounded diagnostic，不携带raw frame或未获既有allowlist允许的额外字段值；不得向collector公开raw JSON parser、第二parser或新公共API。
10. `ConnectionManager`只以typed downcast识别该marker，停止本连接heartbeat并返回既有`ConnectionExit::Recoverable`；旧socket关闭后沿用SDK现有自然重连/backoff并重放latest desired。该对象不得形成`ConnectionFault::Parser`、typed row、fresh、业务freshness/recovery episode变化或持久化；transport state/generation按既有自然重连变化。`Invalid request body`、其他未匹配service message、数组及形似目标但字段/typed内容错误的frame继续走既有sticky terminal parser路径。只有未typed的`message="Too Many Requests"`字符串不构成已确认rate-limit；真实HTTP/protocol/typed rate-limit继续按既有availability合同恢复。

### 5.2 静默触发、teardown 与新 runtime

1. steady threshold 命中时，同一 recovery episode 任意时刻最多一个在途恢复动作；fresh 到达前允许按首条 fresh 窗口和 outer backoff 顺序重试。第一次静默触发的完整替换可以立即开始，runtime replacement 本身不结束 episode；已启用的首条 fresh 窗口首次到期时则按 5.3 启动 episode 和 outer backoff。
2. business deadline/backoff只负责唤醒唯一owner。已 ready 的message/state/applied分支优先；timer分支只读取一次可能异步变化的current SDK generation、state和local-applied并更新恢复状态。仍满足`Connected + local-applied`且恢复状态已到期时，owner直接消费current runtime并调用`shutdown_and_wait(self)`；不另建candidate对象、commit标记、active-slot状态机、CAS、mutex或第二控制owner，也不重复核对只由该owner修改/消费的`rtds_runtime_epoch`和fresh状态。
3. `shutdown_and_wait(self)` 停止并 await 项目 actor、释放旧 receiver和全部 manager owner，最后等待保留的 state receiver 观察到 channel closure。旧 receiver 队列不会转移到新 runtime，不另建跨 runtime 数据路径 fence。
4. 只有 teardown 完成后才可创建新的 `RtdsSdkRuntime` 并递增 `rtds_runtime_epoch`。任何时刻最多一个活动 RTDS socket，旧 connection loop 不得与新 runtime 并存或继续自然重连。
5. coordinator stop 继续复用现有 `abort_with_permit()` 与 source task join，不新增 RTDS typed stop/control channel或 completion reply，也不要求 stop 请求的墙钟时刻与 runtime constructor 形成事务级线性化。business replacement 的 break-before-make 只约束同一 source task 创建后继 production runtime 的顺序。
6. wrapper 无法取得真实 shutdown completion signal 时，必须在 business replacement 路径实现前报告 SDK 缺口和最小官方 SDK patch 方案并等待用户决定，不得旁路到 raw WebSocket。不得用固定 sleep、诊断超时或 `Disconnected` 单项冒充完成。
7. 新 runtime 继续复用 `bounded_rtds_ws_config()`、官方 SDK `Subscription::chainlink_prices(Some("btc/usd"))` 和 latest-complete desired actor。

### 5.3 首条 fresh、重复静默与外层退避

1. 当 `ever_received_fresh=true` 时，`first_fresh_observation_window` 的锚点是：当前 `rtds_runtime_epoch` / `sdk_connection_generation` 首次同时满足 `Connected` 和 latest desired 本地 applied 时的 monotonic 时刻；若两个事件先后到达，使用后到事件的观测时刻。窗口时长固定为 60 秒。
2. 新 runtime/generation 在首条 fresh 窗口内收到合格实时更新时，立即取消外层 business backoff、重置其尝试级别并进入 steady-state。
3. 任一已启用的窗口到期仍无 fresh 时：若尚无 episode，先以 `first_fresh_window_timeout` 启动；无论 episode 是刚创建还是已经存在，都进入当前 episode 的 outer capped exponential backoff + jitter。等待期间当前静默但仍 `Connected + local applied` 的连接保持活动，fresh 到达即结束 episode并重置 backoff，不人为制造“先断线再等待”的无连接窗口。
4. 在消费式 shutdown 调用开始前出现 transport 转换时取消当前 backoff wait，但不把同一 episode 的尝试级别重置为初始值。被取消的 wait 不算一次 replacement、不升级 outer level；自然新 generation 在下一次窗口超时后复用该 level，只有实际开始业务 replacement 才递增 replacement `attempt_index`。自然新 generation 只有在 `ever_received_fresh=true` 时才重新进入首条 fresh 窗口，否则保持 transport-only 等待。
5. backoff 到期时按 5.2.2 处理；只有timer分支读取到current SDK仍为`Connected + local applied`且恢复状态仍到期，才开始下一轮 break-before-make。
6. 永久共享上游静默时允许外层退避达到上限后继续等待和尝试，但不得 tight loop、不得结束 source/collector、不得虚报恢复。operator stop 继续服从现有 coordinator 合同。

### 5.4 订阅、数据、日志与 counter

1. loopback wire 证据必须证明每条实际连接只出现一次完整 filtered subscription，filter 精确为 `btc/usd`，且没有 `unsubscribe`。生产 operator 日志只能把现有事件称为 local applied。
2. 换连接前后继续使用同一个 RTDS typed raw writer、分类和结构去重，不增加 synthetic/backfill/sidecar/第二写入路径；静默区间保持真实缺失。
3. `rtds_runtime_epoch` 仅进入有界、去重的 operator 日志和测试断言；不为它增加 row/manifest/counter schema。
4. 现有 `reconnect_events` 继续只表示同一 runtime 内 SDK `sdk_connection_generation` 前进的自然 transport reconnect。整个 runtime 的 business replacement 不复用或扩张该 counter，以有界 operator 日志记录即可。
5. RTDS 恢复不得阻塞共同 queue/writer，不得停止或重建其他健康 source；SIGTERM、run deadline、UTC rollover 和 coordinated shutdown 继续服从现有合同。

### 5.5 必要而有界的测试

1. 复用现有 fresh/snapshot/stale/wrong/invalid/duplicate 分类测试；只新增暂停时间的聚焦状态测试，覆盖首次 fresh 前无 business watchdog、后到 monotonic anchor、transport 转换取消候选、outer backoff 上限和 fresh reset。
2. 一个 wrapper shutdown loopback证明`shutdown_and_wait(self)`返回时旧connection-loop state channel已关闭；生产源码由同一owner直接表达“await该调用返回后才构造新runtime”。不为重复证明相邻源码顺序新增test-only lifecycle event、channel、counter或hook，也不统计进程级FD、RSS、task、watch、receiver或refcount。
3. 一个连续两次 replacement、共三个 runtime epoch 的确定性 loopback：runtime 0 曾收到 fresh 后 stuck并完成第一次替换；runtime 1 在60秒窗口和当前outer delay后仍无fresh并完成第二次替换；runtime 2 收到 fresh并重置episode/backoff。该loopback证明生产分支实际完成两次顺序替换、每连接只有一次完整filtered subscription且没有`unsubscribe`；服务端只需最终观察到旧连接关闭且没有自然重连，不要求该观察事件早于新runtime创建，也不等待Close ACK。
4. 历史自然transport 100-cycle测试和既有结果保留原样，不删除、不改写，但本事项不重新执行，也不把它们作为新business replacement的验收依据。本事项不新增100-cycle完整runtime replacement、完整multi-source fixture或资源计数框架。
5. 开发和最终闭合只运行受影响的RTDS lib测试；另用`cargo test --workspace --locked --no-run`编译全部测试target，再运行workspace全target Clippy、fmt check和diff check。不得用默认workspace test重新执行历史100-cycle；若后续代码修复，只重跑直接受影响的测试或静态检查。
6. 方案A只新增一个聚焦runtime loopback：第一条连接收到精确网络错误对象后进入既有自然重连，第二条连接收到一次完整filtered subscription并能消费fresh；同一测试随后发送“相同精确message但带一个RTDS数据身份字段”的对象并证明它不进入recoverable marker。该测试负责正负分类；既有自然重连once-replay、malformed typed-frame sticky/no-reconnect和两项bounded parser诊断测试继续复用，共同证明完整行为。`Invalid request body`沿用既有诊断测试，不新增service-message表格矩阵、重复等待、资源计数或100-cycle。

### 5.6 本地香港 VPN 8 小时完整 training collector 验证

1. Attempt3直接运行正式`data-collect-training-collector`，不再运行RTDS-only ignored harness。它必须使用`LiveRuntimeSourceSpecs::production()`、四个既有logical producer、六个既有`LIVE_SOURCES`、正式`ManifestOwner`和正式typed Parquet writer；不得新增JSONL、第二writer、第二RTDS owner/socket或probe专用production seam。当前调用图会写`polymarket_market` source v3与`polymarket_market_metadata` v4，因此本需求、对应计划和changelog共同构成用户本次明确批准的首次真实本地部分日v3/v4前向验证三件套；不得把它冒充完整UTC日前向长跑或production/canonical cutover。
2. 六个落盘source kind固定为`polymarket_market`、`polymarket_market_metadata`、`binance_depth_snapshot`、`binance_depth_update`、`binance_aggtrade`和`polymarket_rtds_chainlink_btc_usd`。数据只写入新的本地隔离绝对dataset root下的`training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/`与每日唯一`manifest.json`；不得指向生产dataset、已有拉取数据、`/home/chillchill/new-datasets`、paper/live或远端路径。
3. 从承载本authority且已push的clean HEAD设置精确`DATA_COLLECT_GIT_COMMIT`重建本地release-profile binary，记录完整commit、构建命令、binary SHA-256和执行命令。运行参数固定包含`--dataset-root <本attempt新建的绝对隔离路径> --collector-region local-hong-kong-route --max-runtime-secs 28800 --max-dataset-bytes 50000000000`，不得同时使用`--run-forever`或外层强杀`timeout`。
4. 开始和结束各记录一次不含凭据的公网出口与香港区域事实。该边界样本只支持“从香港线路开始/结束”的表述，不冒充8小时逐秒路由证明；全部HTTP/WebSocket连接继续使用已验证的本地香港proxychains路径，不静默回退直连。
5. 记录开始/结束UTC、真实进程退出状态、正式日志、六源manifest counter、finalized Parquet数量/行数、RTDS fresh与最大相邻接收间隔、自然重连和business replacement。只有真实发生replacement时，才根据已有replacement日志与RTDS Parquet如实报告可直接证明的事件顺序；不新增四段专用计时、周期路由、资源矩阵或100-cycle。
6. 只有binary退出0、完成至少8小时bounded runtime、协调finalize完成、六个source kind均有非空finalized Parquet、manifest file facts与文件一致、每个manifest登记的finalized Parquet均可由PyArrow打开且实际`num_rows`等于manifest `rows`、并且无`.running`/`.tmp`残留时，才满足本次完整全源验收。同UTC日bounded停止后顶层manifest保持`state=open`且`collection_day_coverage.state=ended_known`是当前正式语义，不得错误要求当日manifest变为`closed`；只有跨UTC日完成barrier的旧日才为`closed`。某source为零表示用户要求的全源数据结果未满足，必须如实报告，但不能仅凭这一事实把collector进程或其他source误判为代码失败。
7. Attempt1证据目录为`target/rtds-stage2-hk-probe-20260726T113120Z`：起止香港边界均成立，进程内运行`16851391ms`，fresh `16127`、最大相邻fresh间隔`8521ms`、自然重连`2`、business replacement `0`、runtime error `1`，test exit `101`。2026-07-27一次已披露的本地`xxd`误操作事后覆盖了辅助文件`probe-tee-exit.txt`；按用户批准的E-A保持污染文件原样且不再写入，以未变的`wrapper-result.json`及覆盖前独立读取证明tee exit为`0`。该单文件污染不改变`STAGE2_ATTEMPT1_FAIL_INCOMPLETE_8H`。
8. Attempt2目录为`target/rtds-stage2-hk-probe-attempt2-20260727T032407Z`。它从clean HEAD/upstream `0685fcab51a2b7c644ba7c5aca9c873134f88fd4`及香港`8.210.176.1 / HK / HKG`启动旧RTDS-only内存probe，并在约2.347秒收到首条合格fresh；用户随后明确要求终止并纠正测试应运行完整training collector。终止后进程已不存在，但没有形成结束route、test/tee exit、wrapper result或最终summary，故固定为`USER_TERMINATED + INCOMPLETE_8H`，不得补造精确elapsed、最终计数或本地数据文件，也不得标为代码FAIL。
9. Attempt3只能在本次authority完成独立审查、纯文档commit/push及三方ref核对后，从新clean HEAD、新artifact目录和新隔离dataset root启动。Attempt1/Attempt2与Attempt3不是相同测试拓扑；最终报告必须并列保留三次事实，不能用Attempt3覆盖前两次。
10. 报告可将Attempt3 RTDS最长相邻接收间隔与历史`6075.343s`作跨环境数量级参考，但不得冒充香港本地与AWS爱尔兰的同环境因果对照。若没有真实business replacement，只能结论为“完整collector八小时窗口机械稳定，真实replacement效果未观测”。
11. Attempt3 artifact固定为`target/rtds-stage2-attempt3-full-training-20260727T042402Z`。它从clean/pushed `c05db6ba58676bec3e20878375608e395ad0971a`构建并运行正式release binary，起止香港route样本均为`8.210.176.1 / HK / HKG`；wrapper从`2026-07-27T04:27:37.565691017Z`运行至`2026-07-27T05:32:01.509592989Z`，monotonic elapsed为`3863.950s`，collector exit为`1`。直接错误是`polymarket_market`的`Gamma resolution integrity failure:slug=btc-updown-5m-1785129600:ordinal=5`，不是RTDS failure；固定结论为`FAIL + INCOMPLETE_8H`。
12. Attempt3错误退出仍协调finalize了11个manifest登记Parquet，六源文件数/行数分别为`binance_aggtrade 2/20259`、`binance_depth_snapshot 1/1`、`binance_depth_update 2/38521`、`polymarket_market 2/1311849`、`polymarket_market_metadata 2/58`、`polymarket_rtds_chainlink_btc_usd 2/3365`。逐文件bytes、SHA-256、PyArrow schema/metadata/rows与manifest一致，无额外或临时文件；同日manifest正确为`open + ended_known`。这些只证明约64分钟内六源正式落盘和错误退出后的finalize，不满足8小时验收。
13. Attempt3 RTDS在部分窗口内记录一次`steady_silence` business replacement及对应一次runtime-created，runtime epoch从0到1；自然reconnect为0，3365行均为fresh，最大相邻接收间隔`304.875s`。replacement日志没有时间戳，故不能计算teardown、runtime创建或恢复耗时，也不能把部分证据写成完整8小时replacement效果验收。
14. Gamma `closedTime`方案A的唯一语义authority为`demand/data-collect-polymarket-resolution-followup-demand.md`：现有RFC3339第一路径失败且raw精确以`+00`结尾时，只把该后缀规范化为`+00:00`并再次交给同一parser；真实触发值为`2026-07-27 05:25:52+00`，其他完整性错误继续fail closed。修复完成目标测试、三路审查、独立Rust commit/push及三方ref核对后，Attempt4必须从新clean/pushed HEAD设置新`DATA_COLLECT_GIT_COMMIT`重建release binary，使用新artifact和空dataset root完整重跑同一六源28800秒；不得续写Attempt3或拼接剩余时间。
15. Attempt4 artifact固定为
`target/rtds-stage2-attempt4-full-training-20260727T071429Z`。它从clean/pushed
`a37cf1181597296b9b2d87f8bc2c93700bda8f48`构建正式release binary，SHA-256为
`b451b2b53aace314e28503ff4f983c20a7468d8175abfd5c7450d8fa7942cc0a`；起止
route均为`8.210.176.1 / HK / HKG`。wrapper从
`2026-07-27T07:20:35.073560320Z`运行至
`2026-07-27T09:37:07.171376781Z`，monotonic elapsed为`8192.100s`，collector
exit为1，故固定为`FAIL + INCOMPLETE_8H`。
16. Attempt4首因是`polymarket_market`某个Decimal字段收到精确字符串`""`后触发
SDK parser fatal；manifest明确记录market source inner return。随后Binance depth和
RTDS碰到已关闭的同日gate只是连锁后果。失败raw frame、event variant和字段path均未
保存，所以不得把失败归因给F04、`price_change`或任一具体字段。
17. Attempt4错误退出仍协调finalize了16个manifest登记Parquet，共`1,884,425`行、
`115,390,221` bytes。六源文件数/行数为aggTrade `3/74380`、depth snapshot
`1/1`、depth update `3/81751`、Polymarket market `3/1720377`、metadata `3/141`、
RTDS `3/7775`；逐文件path/bytes/SHA/PyArrow rows与manifest一致，无额外或临时
文件，manifest SHA-256为
`ee43925d1ebe3f9b8d83555cd08a40e287832c049cda89f348ead95d7d2acab5`。这些只
证明约2小时16分钟内六源正式落盘和错误退出后的finalize，不满足8小时验收。
18. P-B1E的唯一语义authority为SDK070三件套：只对九个官方nullable Decimal把精确
`""`解码为`None`；missing/null/官方合法非空DecimalString保持，nullable非空malformed及required
`price_change.size=""`继续fatal。不增加raw recorder、第二parser、诊断框架、
schema/counter、100-cycle或第五个vendor生产路径。P-B1E最小实现、定向验证、三路
审查、独立Rust commit/push和三方ref核对闭合后，Attempt5必须从新clean/pushed HEAD
设置新`DATA_COLLECT_GIT_COMMIT`，用新artifact和空dataset root完整重跑同一六源
28800秒；不得续写Attempt4或自动扩大空串容忍范围。

## 六、用户已冻结的高影响参数

阶段0已按现有fresh/typed/dedupe语义扫描2026-07-23至2026-07-25三个完整UTC日、本地物理完整的RTDS typed raw artifacts；统计已排除manifest明确记录的collector停机相邻项，artifact物理完整不冒充source业务连续。用户于2026-07-26冻结：

1. `steady_silence_threshold = 300s`；
2. `first_fresh_observation_window = 60s`；
3. `first_fresh_anchor = max(monotonic Connected observation, monotonic latest desired local-applied observation)`，即同一 current runtime/generation 内两个观测事件的后到 monotonic 时刻；
4. `cold_start_watchdog = DISABLED until ever_received_fresh`：source task 从未收到合格 fresh 前不启动首条 fresh watchdog；收到首条合格 fresh 后，后续 runtime/generation 才启用该窗口。

5 秒只能作为候选观测点，不能因为等于 `PING` cadence 自动成为业务阈值。本事项不再冻结额外 teardown 诊断周期或 probe 周期采样常量；实现只需记录实际开始/完成事件，probe只需起止边界与进程内连续时长。

外层 backoff 复用现有私有 `retry_delay()` 的初始 `1s`、倍率 `2`、稳定 jitter `±20%`，hard cap 固定为 `300s`，总 attempts 和总 episode 时长不设上限；收到合格 fresh 后重置。不复用完整通用 supervisor，也不新增依赖。jitter seed 在 source task 启动时按现有 source-local supervisor 做法从一次 `now_ms()` 取得，并在该 task 的全部 outer attempts 中保持不变；这只决定同一 attempt 的确定性抖动，不改变上限或触发语义。`300s` 是控制永久停流时 socket/subscription churn 的工程政策取舍，不是三日 typed raw 推导出的外部 RTDS SLA。

上述值适用于共同 RTDS 行为合同。当前代码只有 training 拥有真实 RTDS adapter；阶段1已在该路径完成实现、本地验证、三路独立终审及分离commit/push。future paper/live adapter 接入时必须使用同一值和语义，但须在各自独立事项中补充进程重启后的 `ever_received_fresh=false`、本机 handoff 时序/背压及资源验收，且不得复用 training 的 Parquet/manifest writer。

## 七、验收标准

1. 三件套编号、引用、状态和术语一致；完整参数、需求、wrapper-first SDK路线、authority精确修改矩阵和本地持续执行均已获批，当前authority的subscription证据与业务静默旧语义按最小差异同步，历史事实保留。
2. wrapper-only observation surface 能分别交付 initial/后续 generation 的 state、generation 和 local-applied；只有合格 fresh update 重置计时。消费式 shutdown 调用开始前transport转换优先并撤销旧business deadline/backoff；调用开始后按顺序等待完成。`ever_received_fresh=true` 后的自然 SDK generation 无条件从首条 fresh 窗口重新开始；首次 fresh 之前不运行 business watchdog。
3. 业务替换实现前的 capability subgate 证明旧 runtime 的 actor JoinHandle 完成、receiver/全部 manager owner 释放且 state channel 关闭，本进程已释放旧socket；该本地`shutdown_and_wait(self)`完成后才创建新 runtime。`Disconnected` 单项不能放行；服务端只需最终观察到旧连接关闭且没有自然重连，不要求该观察事件早于新runtime创建，也不等待Close ACK。
4. 每条 wire connection 恰有一次 `btc/usd` filtered subscription、无 `unsubscribe`；local applied 不被称作 wire completion 或 ACK。
5. recovery episode 可由 steady threshold 或已启用的首条 fresh 窗口首次到期启动；任一后续合格窗口到期都进入同一 episode 的下一轮 outer backoff而不重启 episode。同一 episode 任意时刻最多一个在途动作，fresh 结束 episode 并取消/重置退避。
6. `rtds_runtime_epoch` 与 `sdk_connection_generation` 含义分离；现有 `reconnect_events` 语义不变，未新增 schema/counter/fence 框架。
7. typed rows、分类、去重、timestamp/freshness、正式单writer与真实gap语义不回退；Attempt3、Attempt4及P-B1E后的Attempt5均直接使用完整正式训练采集代码路径和各自隔离dataset root，没有补写、伪造、JSONL sidecar或第二数据路径。
8. cold start首次fresh前无business watchdog、首次fresh后跨runtime/generation保持`ever_received_fresh=true`、两种recovery-episode入口、transport取消候选、wrapper shutdown和现有transport/parser/shutdown回归通过。
9. wrapper loopback证明`shutdown_and_wait(self)`返回时旧connection-loop state channel已关闭；生产源码明确await该调用后才创建新runtime；连续两次replacement、共三个runtime epoch的loopback证明该分支实际顺序执行两次、每连接一次filtered subscription、无`unsubscribe`，并在第三个runtime收到fresh后重置episode/backoff。不得为重复证明源码顺序新增test-only lifecycle hook；本事项不新增或重跑100-cycle，也不新增进程资源矩阵。
10. 每次实际replacement只记录trigger kind、runtime epoch、SDK generation、attempt index及replacement start/runtime created事件；不增加四段专用计时状态、counter、schema或test seam，不虚构handshake/wire/ACK指标。
11. Attempt3因非RTDS的Gamma格式误判在`3863.950s`退出1；修复后的Attempt4因Polymarket market Decimal空串parser fatal在`8192.100s`退出1，二者均固定为`FAIL + INCOMPLETE_8H`。P-B1E闭合后的香港线路Attempt5必须使用新clean/pushed HEAD重建的正式training binary、全部六个`LIVE_SOURCES`、正式Parquet writer与每日manifest，在全新隔离本地dataset root运行`28800`秒；退出0、六源非空finalized Parquet、manifest/file/PyArrow一致和无临时残留共同证明完整采集。起止各记录一次香港出口边界；没有可比真实故障时不声称replacement效果已验证。
12. 共同合同明确区分“适用角色”和“当前实现”：training在本事项实现并完成全部聚焦测试与既有回归；paper/live当前仍为heartbeat-only，future真实RTDS adapter由独立三件套实施、测试、验收和部署，但其恢复语义不得重新冻结。除SDK070三件套单点实施的P-B1E typed decode规范化外，CLOB与Binance业务及恢复行为不变；Gamma只允许resolution专项批准的精确`closedTime`序列化兼容，不改变identity、terminal predicate或fatal边界。
13. 用户的持续执行授权覆盖本地authority同步、文档审查/commit/push、P-B1E的最小vendor修复和定向验证，以及从新clean HEAD重建release binary后执行阶段2本地香港线路连续8小时Attempt5。不得新增格式矩阵、RTDS-only probe、100-cycle、资源矩阵或Decimal诊断框架。capability失败、SDK缺口或新上下文/事实冲突必须停止。production release、systemd、部署、远端操作、上传、migration和数据清理保持独立边界。
14. 方案A的精确网络错误对象只触发SDK既有recoverable connection exit；它不进入collector数据、fresh、业务freshness/recovery episode或持久化路径，transport state/generation按既有自然重连变化。其余未匹配service message和目标typed/parser/schema错误继续sticky terminal；新增loopback与既有once/sticky/diagnostic测试共同证明正负边界。
15. Attempt1以未受污染的核心日志、wrapper结果、已记录哈希、test exit和不足8小时事实永久记为失败；`probe-tee-exit.txt`的事后污染按历史E-A原样保留并披露。Attempt2以用户终止且无最终summary的事实永久记为`USER_TERMINATED + INCOMPLETE_8H`。Attempt3以完整collector的部分六源数据、Gamma错误和不足8小时事实永久记为`FAIL + INCOMPLETE_8H`。Attempt4以完整collector的部分六源数据、Decimal空串parser fatal和不足8小时事实永久记为`FAIL + INCOMPLETE_8H`。Attempt5使用P-B1E闭合后的新clean HEAD、新artifact和空dataset root；最终阶段报告明确区分五次运行及不同测试拓扑，不覆盖或续写前四次。
16. P-B1E只证明九个官方nullable字段的精确空串兼容边界，不得写成已经唯一定位或保证消除Attempt4首因。若Attempt5同一错误复发，按新证据停止；required Decimal、非空malformed及其他parser/typed/schema错误继续fail closed。

## 八、当前授权边界

完整参数、本需求、wrapper-first SDK路线、authority精确修改矩阵及持续执行至阶段2本地香港线路连续8小时验证均已获用户批准。两份当前authority同步、阶段0发布、阶段1实现/验证/终审/发布及P-B1E最小实现均已闭合；Attempt1按历史E-A永久记录为`FAIL + INCOMPLETE_8H`，Attempt2按用户终止和缺失最终summary永久记录为`USER_TERMINATED + INCOMPLETE_8H`，Attempt3按Gamma格式误判、Attempt4按Decimal空串parser fatal分别永久记录为`FAIL + INCOMPLETE_8H`。P-B1E authority/Rust commits `15feb12`/`9d8e59b`已分离发布，定向验证与三路终审均通过；承载本closure的纯文档commit成功push并完成三方ref核对后，当前唯一游标为从该新clean/pushed HEAD、新artifact和空dataset root执行完整六源8小时Attempt5。无需重复申请普通实现授权。新的文档/代码/事实冲突仍必须停止；production release、systemd、部署、远端服务操作、上传、migration和数据清理不在当前授权边界。
