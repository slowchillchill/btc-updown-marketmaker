# RTDS 阶段 0 authority 修改矩阵与 wrapper 路线

> 事项编号：`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`
>
> 对应需求：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
>
> 对应计划：`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`
>
> 对应 changelog：`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`STAGE0_CLOSED_AUTHORITY_SYNC_REVIEW_COMMIT_PUSH_REF_MATCH_COMPLETE`
>
> 代码快照：`e9c91c0a19a6d6a9f13105955f9c47480cba3b34`
>
> 阶段0闭合commit：`0a2153e76dd9824b675a54cd93cc7f92a08e300f`

## 1. 结论

1. 完整参数门已经完成：`300s + 60s/后到 monotonic anchor + cold-start disabled + outer 1s × 2 ±20% / hard cap 300s / fresh reset / unlimited attempts`。
2. 用户已批准本报告的精确修改矩阵、wrapper-first路线与本地持续执行；两份当前执行authority已按矩阵同步，阶段0自审、独立终审、commit/push及三方ref核对已以闭合commit完成。阶段1候选实现与收窄验证门现已通过，正在闭合独立终审。
3. 在上述阶段0代码快照中，官方 SDK 与项目 wrapper 的 ownership primitive 没有静态证明该路线不可能，但当时wrapper没有awaitable shutdown，也不能向source owner完整交付state/local-applied观测。因此阶段0路线状态只能是：

```text
WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE
```

4. 阶段0该结论既不是capability PASS，也不是已确认SDK gap；它要求阶段1先以最小loopback动态证明actor join、全部manager owner释放和state-channel closure。阶段1现已通过该能力门且没有修改vendor SDK。
5. 只读核对发现仓库已经用 `RuntimeEpoch` 表示 raw-day 生命周期代次。整个 RTDS runtime 替换代次因此统一采用内部名称 `rtds_runtime_epoch`，不再保留同名异义决策门。
6. 当前只有training binary拥有真实RTDS adapter；paper/live当前只运行heartbeat/status server。用户冻结的是三种角色未来共同遵守的RTDS恢复合同，不是在本事项新增paper/live adapter。

## 2. authority 精确修改矩阵

下表描述用户已批准并在阶段0同步的最小差异。

| 当前 authority | 当前语义与冲突 | 允许加入的唯一窄例外 | 必须保留 |
|---|---|---|---|
| `demand/data-collect-demand.md` 3.3 第2项 | 把 subscribe send、ACK 和 first message 混成一条证据链；当前代码只能证明 SDK 本地发送队列入队/local applied，不能证明 wire completion或服务端 ACK | 分开记录 raw subscription JSON、复用并扩展既有 applied 观测得到的 generation/revision/monotonic time、首条匹配消息和直接 SDK/close/error；没有独立 ACK 时禁止命名为 ACK/wire send | subscription payload 的 action/topic/type/filter精确字段；typed row字段；Chainlink不是Binance depth替代源 |
| 同文件 3.3 第4--5项 | 明确要求 freshness 不控制 subscription/generation，并删除全部 business-silence reconnect | 只有training的`polymarket_rtds_chainlink_btc_usd`拥有真实adapter；阶段1已完成该路径候选实现与收窄验证门。future paper/live真实RTDS adapter必须继承：source task 曾收到合格 fresh 后，steady silence 达300秒，或后续 runtime/generation 的60秒 first-fresh窗口到期；timer分支只读取一次current SDK generation/state/local-applied，恢复状态仍到期时按冻结outer backoff break-before-make替换整个 runtime | 不做同socket subscribe/unsubscribe；不恢复 one-shot ticket、generation budget或进程退出；不写gap/lag/告警；真实transport无限恢复；当前paper/live仍heartbeat-only，其他source不变 |
| 同文件 5.1 第2项、5.2 第2--4项 | 长静默/stale不得操作socket；RTDS只在自然generation重放desired | 在第5.1第2项和5.2第4项引用同一training RTDS窄例外；第5.2第3项补充每个新runtime的初始connection也只应用一次完整desired；stale本身不直接触发，但不重置fresh watchdog | 唯一source owner、无第二socket/actor/receiver/shadow client；所有其他source行为不变；有界单次等待与无限总attempt保持 |
| 同文件验收第15--17项 | 验收仍要求静默完全不影响网络，且只描述真实transport generation replay | 第15--16项引用同一完整runtime replacement例外和每新runtime一次完整desired；第17项继续使用有界、去重operator日志 | one-shot `request_reconnect`仍删除；平台无数据不算实现失败；SDK-first缺口门和所有完整性边界保持 |
| `demand/data-collect-source-availability-lifecycle-repair-demand.md` 第一节第3项 | 全局禁止业务静默主动断开仍活socket | 增加共同RTDS专项交叉引用；authority同步后允许阶段1在training由同一PID、同一owner串行完整换runtime，future paper/live真实RTDS adapter必须使用同一合同；阶段1候选实现与收窄验证门已通过 | 不gap/告警/退出/systemd；健康source继续；真实transport无限恢复；其他source不变 |
| 同文件4.2第2项 | 禁止RTDS/CLOB/aggTrade/depth的business-silence timer和阈值 | 只允许RTDS唯一source owner中的monotonic 300秒/60秒watchdog；不持久化silence-start事件 | CLOB/Binance/Gamma继续无business timer；manifest、coverage、schema和模型调度不扩张 |
| 同文件4.3第1、3、5--6项 | 一个长生命周期runtime；静默/stale/no fresh不得切generation；只有自然generation重放desired | 改成“任一时刻一个active RTDS runtime，但批准条件下可完整替换”；仍禁止修改desired revision和同socket重订阅；每个新runtime初始connection只应用一次完整desired；stale不重置watchdog但不直接成为trigger | 第2项旧60秒one-shot/预算/fatal删除；第4项5秒/no-PONG/无限transport；第7项真实transport与normal teardown |
| 同文件4.9第3--4项 | 唯一owner成立，但“不得为任何路径另建第二恢复循环”会排除business replacement层 | 只允许同一个RTDS owner内串行的business runtime-replacement/backoff层；它不是第二transport owner、并行恢复循环或shadow control plane | 单active runtime/socket/generation/actor/receiver/desired owner；operator stop、drain/finalize保持 |
| 同文件AC1、AC4、AC7、AC9、AC10、AC24--25 | AC1/AC24禁止静默reconnect/timer；AC4未说明RTDS是否曾有fresh；AC25禁止任何第二恢复路径并记录既有自然恢复100-cycle | AC1/AC24只加入曾有fresh后的RTDS窄例外；AC4拆清“从未fresh”和“此前有fresh”；AC25只允许同一owner内串行业务runtime-replacement/backoff层，并明确新路径只做连续两次replacement的精确顺序测试，不新增100-cycle或资源矩阵 | AC7保留cold-start disabled首次fresh前同socket；AC9保留真实transport恢复且无business one-shot ticket；AC10完整保留`request_reconnect`删除；既有自然transport 100-cycle只保留为历史证据，不删除、不由本次专项重跑，也不作为新路径依据 |

### 2.1 只保留、不同步的合同

- `demand/data-collect-reliability-repair-demand.md` 只保留仍有效的 SDK-first、唯一owner、无shadow、manager teardown、typed freshness/分类/去重和完整性合同；旧静默行为与历史 changelog 不回写。
- `demand/rebuild-core-demand.md` 整体仍未批准，只作未来一致性上下文；本事项不得借机批准或编辑其余内容。
- 所有历史 plan/test/changelog 保留“当时长期静默保持同一连接”的真实记录，不改写成从未存在。

## 3. AC4 fixture 的必须澄清语义

`data-collect-source-availability-lifecycle-repair-demand.md` 当前 AC4 的“四条WebSocket均静默”没有说明 RTDS 是否已经收到过 fresh，冻结后的两个结果不同：

| fixture前置 | 正确行为 |
|---|---|
| RTDS source task 从未收到合格 fresh | cold-start watchdog禁用；RTDS保持transport-only，同socket等待；其他source也保持原行为 |
| RTDS source task 此前已经收到合格 fresh | 最后fresh后300秒可启动第一次完整runtime replacement；后续runtime/generation按60秒窗口和outer backoff重试 |

authority同步时必须显式拆成这两个fixture，不能用一个含糊场景同时要求“generation不变”和“300秒后完整替换”。

## 4. generation、counter 与命名

1. SDK `connection_generation` 属于单一 `ConnectionManager`：新manager从0开始，第一次成功连接仍是0，只有同runtime后续成功连接才递增。
2. `reconnect_events` 当前只在观察到同runtime内 generation 前进时递增。business replacement必须为新runtime重新初始化其 `recorded_generation`，不能把新runtime的0与旧runtime比较，也不能把business replacement计入该counter。
3. 仓库现有 `data-collect/src/live_runtime/raw_day.rs::RuntimeEpoch` 表示 `{ utc_day, generation }` 的raw-day生命周期；它与专项所说的整个RTDS runtime代次不同。
4. 专项概念统一命名为 `rtds_runtime_epoch`，只进入有界operator日志和测试断言；raw-day `RuntimeEpoch`保持原义。

## 5. wrapper observation 缺口

当前事实：

- `RtdsRuntimeEvent`只有 `Applied` 和 `Message`；
- wrapper内部持有私有 state/applied receiver与actor；
- initial-generation applied被现有freshness fence吸收，不向source owner形成 `Applied`；
- `recv_event_until()`会消费state变化，但不把 `Connecting/Reconnecting/Connected` transition交付owner；
- local applied只是项目actor调用SDK send并进入内部队列后的事实，不是wire completion或ACK。

阶段1允许的最小 wrapper-only observation surface如下。这里描述的是owner必须取得的事实，不冻结必须新增两个同名domain struct；优先复用并最小扩展既有`RtdsAppliedSubscription`及state观测：

```text
TransportObservation {
    state: ConnectionState,
    sdk_connection_generation: u64,
    monotonic_observed_at: Instant,
}

AppliedObservation {
    sdk_connection_generation: u64,
    revision: u64,
    monotonic_observed_at: Instant,
}
```

initial和后续generation都必须交付，且与现有message freshness fence分离。不修改vendor、不增加第二actor/receiver、不把local applied重命名为ACK。

## 6. wrapper-first teardown 候选路线

当前 `RtdsSdkRuntime::Drop`只调用 `actor.abort()`，不await；SDK connection task是detached spawn，没有公开JoinHandle。另一方面，最后一个manager sender释放会让SDK active handler进入normal shutdown，保留的state receiver可观察sender全部释放后的channel closure。因此静态可构造以下候选顺序：

1. 预先保留独立state receiver；
2. 显式终止并await项目subscription actor；
3. 丢弃旧message receiver和wrapper中的全部manager owner；actor结束也释放其generation-bound manager；
4. 等待保留的state receiver返回channel closure；允许先观察到 `Disconnected`，但它单独不能放行；
5. 只有actor join、owner释放、state-channel closure全部成立后，才创建新runtime。

阶段1不预设字段重排：保留一个独立state receiver，abort并await现有actor后整体`drop(self)`，再等待state channel关闭即可。只有该最小路线经动态测试失败时，才报告具体ownership缺口。

### 6.1 capability gate

阶段1第一项必须是最小loopback：

- 证明`shutdown_and_wait(self)`返回时state channel已关闭，旧socket最终关闭且没有自然重连；
- 生产源码由同一owner直接表达`shutdown_and_wait(self).await`返回后才创建新runtime；
- 不为重复证明该相邻源码顺序增加人为延迟路径、test-only lifecycle event/channel/counter/hook或跨端时序断言；
- capability通过前不实现business watchdog或新runtime创建。

失败时停止，报告具体缺口与最小官方SDK patch；不得旁路raw WebSocket。

## 7. coordinator stop 保持现状

当前coordinator对RTDS先登记raw-day abort permit，再调用source task `JoinHandle::abort()`，随后await该task。pinned Tokio `1.52.3`明确说明：abort在task下一个yield点生效，取消会析构全部task-local变量，`JoinHandle.await`观察到完成前析构已经结束。当前RTDS runtime只属于该source task，不会逃逸成第二production owner；因此本事项不新增typed control channel、completion reply、CAS、mutex或stop/create竞态状态机。

这一裁决不把普通collector stop冒充business replacement的完整connection-loop shutdown证明：极窄时序下，task-local runtime可能在下一个await前完成同步构造，随后随task取消立即析构，detached SDK connection loop再异步收敛。但stop后不存在后继production runtime；raw-day admission已由此前形成的prepared runtime exit关闭，abort permit只证明本次取消身份。当前需求只要求业务replacement严格break-before-make，不要求stop请求发出的墙钟瞬间与constructor形成事务级线性化。

## 8. 已完成裁决与执行边界

1. 用户已批准 outer hard cap=`300s`、`rtds_runtime_epoch`内部命名、精确authority矩阵及`WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE`路线。applied只表示SDK本地发送队列接纳，不得称为wire completion或ACK；AC4按是否曾有fresh拆分，AC7/AC9/AC10完整保留，AC25只加入同一owner串行业务replacement层。新路径使用wrapper channel-closure测试、生产源码顺序和连续两次replacement的三个runtime聚焦测试组成最小证据，不增加test-only lifecycle hook、100-cycle或资源矩阵。历史100-cycle在阶段1早期、最终简化裁决前曾误跑一次；裁决后的收窄门没有再次执行，也未把它作为新路径验收。
2. 用户已明确持续推进至阶段2本地香港VPN连续8小时只读probe，只在capability失败、SDK缺口或新上下文/事实冲突时停止。阶段0同步、自审和独立审查已通过；文档commit/push及三方ref一致后按顺序进入阶段1。不合并authority与Rust diff/commit。production release/deployment、远端操作、migration和删除保持独立边界。
3. 用户进一步澄清共同合同边界：当前只有training拥有真实RTDS adapter；阶段1已在training完成候选实现与收窄验证门，正在闭合独立终审。future paper/live真实RTDS adapter由各自独立三件套实施、测试、验收和部署，但恢复语义必须继承本专项，不得再次冻结。当前paper/live Rust调用图不在阶段0/1修改范围。
