# RTDS 业务静默彻底换连接恢复计划

> 事项编号：`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`
>
> 对应需求：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
>
> 对应 changelog：`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_SHARED_RTDS_CONTRACT_STAGE0_CLOSED_STAGE1_CAPABILITY_PENDING`。该状态只在承载本记录的阶段0文档commit成功push且本地HEAD、upstream与远端ref一致后生效。用户已于 2026-07-26 冻结完整行为参数并批准本需求、wrapper-first SDK路线、authority精确修改矩阵和本地持续执行。当前代码只有 training 拥有真实 RTDS adapter；新恢复行为尚未实现，阶段1只在 training 实现与验证。future paper/live 真实 RTDS adapter 一旦由独立事项接入，必须继承同一行为合同，不得重新冻结另一套恢复政策。

本计划以最小source-local ownership loop为边界，不预设大型状态机、通用配置框架、新production probe seam、额外schema/counter或第二数据路径。每个阶段完成后先自审，再由继承`gpt-5.6-sol`/`ultra`的独立subagent审查，成立问题闭环后才发布。本地authority同步、Rust/build/test/review/commit/push及阶段2本地香港VPN连续8小时只读probe按既定阶段顺序持续执行，不重复申请普通实现授权，也不合并阶段、diff或commit；capability失败、SDK缺口或新冲突仍必须停止。production release/部署保持独立边界。

## 阶段 0：冻结参数、authority 修改矩阵与 wrapper 路线

### 任务 0.1：建立 authority 精确修改矩阵

> 状态：只读矩阵及用户裁决已完成，见 `data-collect/docs/rtds-stage0-authority-wrapper-route-2026-07-26.md`。矩阵发现的相反语义、AC4 fixture歧义及AC25窄例外均按用户批准同步；既有AC7/AC9/AC10保留。

- 步骤 0.1.1：只读核对以下当前执行 authority：
  - `demand/data-collect-demand.md` 中 subscribe send、ACK/首条消息混用的证据语义，以及第 3.3 节第 4--5 项“业务静默不切连接”的语义；
  - `demand/data-collect-source-availability-lifecycle-repair-demand.md` 中对应的“业务静默不控制网络”语义。
  - 验证：形成文件位置、旧语义、新例外语义和修改条件矩阵；不得批量替换其他 source。
- 步骤 0.1.2：分别核对仍有效的旧需求合同、未批准 rebuild-core 草案和历史 plan/test/changelog。
  - 验证：标明“当前执行 authority / 保留合同 / 未批准上下文 / 历史事实”；只准备同步第一类，保留 SDK-first、唯一 owner、数据完整性和历史事实。

### 任务 0.2：用真实数据提交三个高影响决定

> 状态：`COMPLETE`。三日真实 typed raw 报告见 `data-collect/docs/rtds-stage0-parameter-candidates-2026-07-23-25.md`；用户已冻结 `300s + 60s/后到 monotonic anchor + cold-start disabled`及outer `1s × 2 ±20% / 300s cap / fresh reset / unlimited attempts`。不再增加teardown诊断周期或probe周期采样常量。

- 步骤 0.2.1：按当前 `fresh`、topic/type/symbol、timestamp 和 dedupe 语义扫描可用 RTDS typed raw，显式排除 manifest 已知 collector 停机相邻项，再计算候选 steady threshold 的正常触发次数、每小时频率、最长正常间隔和已知 `6075.343s` 静默触发时刻。
  - 验证：记录输入日、manifest/文件 SHA、行数、接收区间、过滤合同及可复核命令；不把 exact-second missing 当成 confirmed loss。
- 步骤 0.2.2：为 `first_fresh_observation_window` 计算候选时长，并验证需求 5.3.1 的 anchor 可由 minimal wrapper observation surface 唯一表达。
  - 验证：候选只使用 current state/generation 与 local applied；不把 local applied 冒充 wire send/ACK。
- 步骤 0.2.3：向用户只提交 `steady_silence_threshold`、首条 fresh 窗口时长及 anchor、cold-start 是否适用。
  - 验证：事实、推断、建议分开；参数回复本身不授权同步 authority、commit/push 或进入阶段 1。
- 步骤 0.2.4：选择最小 outer capped exponential backoff + jitter。
  - 验证：优先复用仓库已有成熟 backoff 形态，记录常量、上界和理由；不新增retry依赖、teardown诊断定时器或probe周期采样器。
  - 当前结果：`COMPLETE`。outer复用现有`retry_delay()`的`1s × 2`、稳定jitter `±20%`，hard cap=`300s`，总attempts/elapsed不设上限，fresh后重置。jitter seed沿用现有source-local做法：source task启动时读取一次`now_ms()`并在task内保持不变。

### 任务 0.3：只读冻结 generation、wrapper 观测缺口与 teardown 路线

> 状态：只读证据和内部命名裁决已完成。路线为 `WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE`：现阶段既不能写成 capability PASS，也没有证据宣告 SDK gap。整个 RTDS runtime 替换代次统一命名为 `rtds_runtime_epoch`，避免与 raw-day `RuntimeEpoch` 同名异义。

- 步骤 0.3.1：核对 `rtds_runtime_epoch` 与 SDK `sdk_connection_generation`，以及当前 `reconnect_events` 的递增位置。
  - 验证：前者跨整个 runtime replacement 单调递增且只进日志/测试；后者只在单一 runtime 内表达自然 transport generation；business replacement 不改 schema/counter。
- 步骤 0.3.2：记录当前 wrapper 观测缺口：state/applied receiver 为私有；`recv_event_until()` 不返回 `Connecting/Reconnecting`；initial-generation applied 被 freshness fence 吸收。
  - 验证：冻结最小 wrapper-only 路线，向 owner 暴露 current state + generation transition，以及 initial/后续 generation local-applied；不改变 freshness fence、不 patch vendor、不增加第二 actor/receiver。
- 步骤 0.3.3：只读核对 `RtdsSdkRuntime::Drop`、actor manager clone、最后 sender 释放、connection-loop 返回顺序、state channel 生命周期和已有 drop loopback。
  - 验证：候选 wrapper-first completion 合同为“actor JoinHandle 完成 + 旧 receiver/全部 manager owner 释放 + 保留的 state receiver 观察到 channel closure”；`Disconnected` 仅是中间状态，不要求服务端 Close ACK。
- 步骤 0.3.4：若只读证据已证明 wrapper 路线不可能，立即生成 SDK gap 报告并等待用户决定；若路线可行但必须动态证明，只记录 `WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE`，不得冒充已经通过。
  - 验证：阶段 0 不修改 wrapper/vendor、不新增测试，不产生与当前 authority 相反的代码；不得改用 raw WebSocket 或第二客户端。

### 任务 0.4：授权范围裁决后同步 authority

> 状态：`COMPLETE_PENDING_STAGE0_COMMIT_PUSH_REF_MATCH`。authority同步、自审和三路`gpt-5.6-sol`/`ultra`独立终审已通过，Critical/Important/Minor均为0；承载本记录的commit成功push且三方ref一致后闭合阶段0并进入任务1.0。

- 步骤 0.4.1：按已冻结的完整参数、已批准需求/SDK路线/精确矩阵与本地持续执行授权，修改两份当前authority和本事项三件套。
  - 验证：`rg` 和人工核对证明当前 authority 不再同时要求本 source 达阈值后“永不换连接”和“完整换连接”；subscription 证据不再虚构 ACK；历史 changelog 不改写。
- 步骤 0.4.1a：在主authority与paper/live集成说明中冻结“合同三角色共用、当前只有training拥有真实adapter且阶段1只在training实现新恢复行为”的边界。
  - 验证：当前paper/live仍明确为heartbeat-only；future真实RTDS adapter必须继承相同classifier、四项主要参数、outer backoff、唯一owner/socket和break-before-make；不新增paper/live Rust、测试、handoff或部署任务。
- 步骤 0.4.2：自审后派生独立 subagent 审查需求追溯、SDK-first、授权范围和过度设计。
  - 验证：成立问题闭环；阶段文档commit/push后按已批准的持续执行边界进入阶段1。
- 步骤 0.4.3：阶段0完成并commit/push后，按已批准的持续执行边界切换阶段1。
  - 验证：无需重复申请普通本地实现授权；capability失败、SDK缺口或新冲突必须停止。

## 阶段 1：capability gate 后测试先行实现

### 任务 1.0：先证明 wrapper awaitable shutdown capability

- 步骤 1.0.1：阶段0闭合后，按已批准的阶段1持续执行边界只编写最小失败loopback和wrapper shutdown支撑，不实现business watchdog/replacement：显式结束并await actor，丢弃旧receiver，释放全部manager owner，保留独立state receiver等待channel closure。
  - 验证：故意延迟 connection-loop 返回时，`Disconnected` 单项不算通过；channel closure 后旧 socket 已关闭且不重连；不依赖固定 sleep或服务端 Close ACK。
- 步骤 1.0.2：若 wrapper-only capability 测试不能通过，立即停止阶段 1，报告具体缺口和最小官方 SDK patch，等待用户决定。
  - 验证：capability 未通过前不得实现business replacement或新runtime创建；不得旁路官方 SDK。
- 步骤 1.0.3：只有 capability 证据通过并完成自审，才解除阶段 1 内部 gate，进入任务 1.1。
  - 验证：记录测试名、旧socket lifecycle、actor join、owner release、state-channel closure及剩余风险；该内部gate不改变本轮已裁决的授权范围。

### 任务 1.1：增加最小观测并冻结仲裁行为

- 步骤 1.1.1：先写失败测试，证明 owner 能分别观察 initial/后续 generation 的 current state、generation transition 和 local-applied，同时现有 queued-message freshness fence 保持不变。
  - 验证：minimal wrapper-only API/event 不改变 vendor SDK、消息分类或第二 receiver/actor；initial applied 不再对 owner 不可见。
- 步骤 1.1.2：写失败测试覆盖：只有合格 fresh 重置计时；首次 fresh 前没有 business watchdog、首次 fresh 后跨 runtime/generation 保持 `ever_received_fresh=true`；两种 recovery-episode 入口；`Connecting/Reconnecting/Disconnected` 在消费式shutdown开始前撤销 business deadline/backoff；`require_text_pong=false` 保持。
  - 验证：monotonic clock 决定 deadline，UTC/payload timestamp 跳变不改变它；首次 fresh 前不会因无数据主动换 runtime。
- 步骤 1.1.3：在现有 RTDS source loop 中实现最小 ownership control flow：current runtime、source-task 级 `ever_received_fresh`、last-fresh/first-fresh deadline、outer backoff 和 shutdown await。
  - 验证：不新增大型 enum/state-machine、通用配置系统或与需求无关抽象；每个字段均可追溯到失败测试。

### 任务 1.2：实现顺序 break-before-make

- 步骤 1.2.1：business deadline/backoff到期只形成候选。唯一owner在执行前重新读取`rtds_runtime_epoch`、SDK generation、state、local-applied和fresh；条件变化则取消，条件仍成立则消费current runtime并调用任务1.0证明的`shutdown_and_wait(self)`。
  - 验证：transport transition在shutdown调用开始前获胜并取消business action；调用开始后普通顺序控制流等待完成。不新增active-slot状态机、CAS、mutex、typed stop/control channel或completion reply。
- 步骤 1.2.2：复用任务1.0已证明的wrapper shutdown：结束并await actor、释放receiver和全部manager owner、等待state-channel closure。
  - 验证：`Disconnected`或固定sleep不能放行；coordinator stop继续使用既有`abort_with_permit()`和source task join，不要求stop墙钟时刻与constructor形成事务级线性化。
- 步骤 1.2.3：teardown completion通过后递增`rtds_runtime_epoch`并创建一个新`RtdsSdkRuntime`，继续复用`bounded_rtds_ws_config()`、official SDK subscription和latest-complete desired actor。
  - 验证：每条 wire connection 恰有一次 `crypto_prices_chainlink` / `btc/usd` filtered subscription，无重复 subscribe、unsubscribe、并行 runtime 或 shadow path。
- 步骤 1.2.4：当 `ever_received_fresh=true` 时，以 current generation 同时 `Connected + local applied` 的后到事件启动 60 秒首条 fresh 窗口；否则保持 transport-only 等待。
  - 验证：记录 `episode_trigger_kind` 和真实可观测分段；不出现未经能力证明的 handshake/wire/ACK 指标。

### 任务 1.3：实现同一 episode 的重复静默退避与数据边界

- 步骤 1.3.1：任一已启用的 first-fresh 窗口到期仍无 fresh 时，无 episode 则以 `first_fresh_window_timeout` 创建；无论 episode 是新建还是已经存在，都进入当前 episode 的 outer capped backoff。
  - 验证：后续 timeout 不重启 episode，但仍进入下一轮 `1s × 2 ±20%`、单次最长`300s`的backoff/replacement；source-task-stable jitter seed只在task启动读取一次；同一 episode 任意时刻最多一个在途动作，fresh重置level，总尝试次数不限。
- 步骤 1.3.2：backoff/观察期间在消费式shutdown开始前发生transport转换时取消当前wait，允许SDK inner reconnect，但不重置或升级episode outer level。被取消的wait不算replacement；新generation下一次窗口超时复用同一级，只有实际开始business replacement才递增replacement `attempt_index`。新generation仅在`ever_received_fresh=true`时进入首条fresh窗口，否则保持transport-only。
  - 验证：Close/EOF/PING-send failure 与 business deadline 同时发生只产生一个恢复路径；定向断言被取消wait后level和attempt index不变，下一窗口复用同一级；outer logic不重置SDK inner backoff，fresh才结束episode并重置outer backoff。
- 步骤 1.3.3：保持当前分类、dedupe、typed raw writer、manifest 和 counter 语义；`rtds_runtime_epoch` 只用于日志与测试。
  - 验证：没有 synthetic/backfill/sidecar/第二 writer、跨 runtime validator/fence 或 `reconnect_events` 语义扩张。

### 任务 1.4：以最少测试覆盖新增风险

- 步骤 1.4.1：复用现有分类测试，只新增暂停时间的聚焦状态测试，覆盖wrapper观测、首次fresh前无business watchdog、两种episode入口、anchor、transport取消候选、backoff cap/reset。
  - 验证：失败测试先失败、实现后通过；不重复已有fresh/snapshot/stale/wrong/invalid/duplicate矩阵。
- 步骤 1.4.2：运行一个连续两次replacement、共三个runtime epoch的确定性loopback。
  - 验证：runtime 0曾有fresh后stuck并完成第一次替换；runtime 1在60秒窗口和outer delay后仍无fresh并完成第二次替换；runtime 2收到fresh并重置episode/backoff。逐次断言本地`shutdown_and_wait(self)`完成记录早于`runtime_created(n+1)`、每连接只订阅一次完整`btc/usd` filtered desired且无`unsubscribe`；服务端只需最终观察到旧连接关闭且没有自然重连，不要求该观察事件早于新runtime创建，也不等待Close ACK。
- 步骤 1.4.3：不新增100-cycle完整runtime replacement、完整multi-source fixture或FD/RSS/task/watch/receiver/refcount资源矩阵。
  - 验证：现有自然transport、完整runtime/source隔离、parser、UTC rollover、SIGTERM/run-deadline/coordinated-shutdown由已有测试及任务1.5的一次workspace suite回归。

### 任务 1.5：一次最终全量验证与阶段审查

- 步骤 1.5.1：代码稳定后只执行一次：
  - `cargo test --workspace --locked`
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo fmt --all -- --check`
  - `git diff --check`
  - 验证：workspace 当前唯一 member 已覆盖，不重复运行等价的 `-p data-collect` 全量 suite；最终检查后若代码变化，仅重跑受影响检查和必要全量门。
- 步骤 1.5.2：逐项把测试名、输入故障、预期与实际结果映射到需求验收标准，自审 diff 后派生独立 subagent 审查正确性、所有权、竞态、SDK-first、数据隔离和过度设计。
  - 验证：成立问题修复并复审，三件套同步实际实现和剩余风险；阶段1 commit/push后按已批准边界进入阶段2。

### 任务 1.6：保持 future paper/live 共同合同而不提前实现

- 步骤 1.6.1：阶段1只实现并测试training owner；不得为尚不存在的paper/live RTDS consumer提前抽象通用adapter、trait或handoff framework。
  - 验证：`data-collect-paper-hot`与`data-collect-live-hot`生产调用图仍只有既有heartbeat/status路径；没有新增外部RTDS socket、training writer或假在线验收。
- 步骤 1.6.2：把future adapter的强制验收入口保留在paper/live集成说明。
  - 验证：未来独立事项必须继承相同fresh classifier、参数/backoff、唯一owner/socket、break-before-make与cold-start重启语义，并另测本机handoff时序/背压；不得复用training Parquet/manifest/dedupe-restore。

## 阶段 2：本地香港 VPN 连续 8 小时只读 probe

### 任务 2.1：最小隔离 preflight

- 步骤 2.1.1：阶段1通过后，按用户已明确覆盖的这一次8小时香港VPN只读公开网络probe授权，在`run_rtds_source_with_spec()`同模块增加/使用`#[cfg(test)]`、`#[ignore]`长运行harness，直接调用该私有runner；传入新建的隔离临时dataset root、持续drain的in-memory `SourceEventSink`，并安装纯内存active `RawDayGate`。
  - 验证：不公开 production API，不重构 `emit_rtds_message()`，不新增 production 分类 seam/validator；不启动 `ManifestOwner`、Parquet/canonical writer，不传入 canonical dataset root。
- 步骤 2.1.2：记录命令、clean `HEAD`、冻结参数、UTC/monotonic起点和初始香港公网出口/区域。
  - 验证：不修改全局 VPN、不记录凭据、不接触钱包、paper/live、远端 service 或第二 RTDS owner。

### 任务 2.2：运行与证据

- 步骤 2.2.1：连续运行8小时并持续drain sink；记录进程内monotonic elapsed、退出状态、fresh数量、最大相邻fresh间隔、runtime错误、自然重连和business replacement次数。
  - 验证：提前退出、crash或monotonic elapsed不足8小时为未完成；观察到协议、顺序或恢复错误直接记FAIL。
- 步骤 2.2.2：结束时再记录一次公网出口/香港区域；只有真实发生replacement时，追加trigger、旧runtime关闭完成、新订阅和first fresh耗时。
  - 验证：起止任一确认不在香港时不得标记为香港线路；不增加周期route/PID/boot/boottime/socket/FD采样或独立supervisor。

### 任务 2.3：报告与阶段审查

- 步骤 2.3.1：报告最长业务静默、替换次数、按 trigger kind 分组的各分段/总恢复耗时、自然恢复、永久无 fresh 和错误事件，并与历史 `6075.343s` 作跨环境数量级比较。
  - 验证：香港本地不冒充 AWS 爱尔兰同环境对照；没有可比真实事件时只写“8 小时窗口稳定，真实事件效果未验证”。
- 步骤 2.3.2：同步三件套实际 probe 证据与剩余风险，自审后派生独立 subagent 终审。
  - 验证：机械正确性与真实事件效果分别标记；成立问题闭环，按本轮裁决后的适用授权范围发布commit/push。

## 阶段 3：production release 与部署保持独立授权

- 任务 3.1：本计划不执行 production release 构建、上传、部署、systemd 修改、远端服务操作或生产数据清理。
  - 验证：若用户后续批准，另建部署三件套，冻结 commit、binary SHA256、构建机与命令、artifact 路径、回滚、远端验收和数据边界。

## 本轮文档执行结果

三日真实 RTDS typed raw 参数分析、任务0.1--0.4及全部用户裁决已完成，阶段0自审与三路独立终审均已通过，等待文档commit/push及三方ref一致后闭合。冻结合同为 `steady=300s`、`first-fresh=60s/后到 monotonic anchor`、cold-start首次fresh前禁用，以及 outer `1s × 2 ±20%`、hard cap=`300s`、fresh重置、总尝试不限；整个runtime替换代次统一命名为`rtds_runtime_epoch`。共同合同适用于training及future paper/live真实RTDS adapter；当前只有training拥有真实RTDS adapter，新恢复行为将在阶段1仅对training实现与验证。Rust/vendor尚未修改，香港VPN probe、production release和部署未执行。
