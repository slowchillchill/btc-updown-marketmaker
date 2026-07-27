# RTDS 业务静默彻底换连接恢复计划

> 事项编号：`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`
>
> 对应需求：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
>
> 对应 changelog：`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_SHARED_RTDS_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FAIL_INCOMPLETE_8H_P_B1E_IMPLEMENTATION_CLOSED_ATTEMPT5_FULL_COLLECTOR_PENDING`。其中`P-B1E`是本次获批的“nullable Decimal精确空串兼容”唯一标识，用于避免与Attempt1辅助文件污染处置的历史`E-A`重名；该状态仅在承载本记录的纯文档closure commit成功push且本地HEAD、upstream与远端ref一致后生效。RTDS network-error方案A的`366fed7d`/`415470e`、Gamma `closedTime`方案A的`3a367386`/`e086017f`及P-B1E的`15feb12`/`9d8e59b`均已按authority/Rust分离发布并完成三方ref核对。阶段0与阶段1均已完成；Attempt1、Attempt3、Attempt4永久为`FAIL + INCOMPLETE_8H`，Attempt2永久为`USER_TERMINATED + INCOMPLETE_8H`。P-B1E定向验证与三路独立终审均已通过；当前唯一下一步是从新clean/pushed HEAD、新artifact和空dataset root执行完整六源8小时Attempt5。该本地部分日前向验证不等于production binary激活、完整UTC日前向长跑或canonical cutover。future paper/live真实RTDS adapter一旦由独立事项接入，必须继承同一行为合同。

本计划以最小source-local ownership loop为边界，不预设大型状态机、通用配置框架、新production probe seam、额外schema/counter或第二数据路径。每个阶段完成后先自审，再由继承`gpt-5.6-sol`/`ultra`的独立subagent审查，成立问题闭环后才发布。本地authority同步、review/commit/push、从clean HEAD重建release-profile binary及阶段2本地香港VPN连续8小时完整training collector落盘验证按既定阶段顺序持续执行，不重复申请普通实现授权。当前完整collector能力已经存在；本轮Rust只允许按SDK070三件套在既有vendor `response.rs`实施P-B1E及其紧凑定向测试，不改变RTDS实现或建立第二解析路径。capability失败、SDK缺口或新冲突仍必须停止。production release、systemd、部署、远端、上传、migration和数据清理保持独立边界。

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

- 步骤 1.0.1：阶段0闭合后，按已批准的阶段1持续执行边界先编写最小wrapper shutdown loopback和支撑：显式结束并await actor，丢弃旧receiver，释放全部manager owner，保留独立state receiver等待channel closure。
  - 验证：`shutdown_and_wait(self)`返回时state channel已关闭，旧socket最终关闭且不重连；不增加人为延迟路径、固定sleep、test-only lifecycle hook或服务端Close ACK要求。
- 步骤 1.0.2：若 wrapper-only capability 测试不能通过，立即停止阶段 1，报告具体缺口和最小官方 SDK patch，等待用户决定。
  - 验证：capability 未通过前不得实现business replacement或新runtime创建；不得旁路官方 SDK。
- 步骤 1.0.3：只有 capability 证据通过并完成自审，才解除阶段 1 内部 gate，进入任务 1.1。
  - 验证：wrapper测试证明state-channel closure；生产源码用同一owner中的`shutdown_and_wait().await`后顺序构造新runtime表达break-before-make，不为重复证明相邻代码顺序增加completion record。

### 任务 1.1：增加最小观测并冻结仲裁行为

- 步骤 1.1.1：先写失败测试，证明 owner 能通过同一`recv_event`流分别观察 initial/后续 generation 的 current state、generation transition 和 local-applied，同时现有 queued-message freshness fence 保持不变。
  - 验证：minimal wrapper-only event不改变vendor SDK、消息分类，不增加第二RTDS message receiver、subscription actor或网络owner；initial applied不再对owner不可见。
- 步骤 1.1.2：写失败测试覆盖：只有合格 fresh 重置计时；首次 fresh 前没有 business watchdog、首次 fresh 后跨 runtime/generation 保持 `ever_received_fresh=true`；两种 recovery-episode 入口；`Connecting/Reconnecting/Disconnected` 在消费式shutdown开始前撤销 business deadline/backoff；`require_text_pong=false` 保持。
  - 验证：monotonic clock 决定 deadline，UTC/payload timestamp 跳变不改变它；首次 fresh 前不会因无数据主动换 runtime。
- 步骤 1.1.3：在现有 RTDS source loop 中实现最小 ownership control flow：current runtime、source-task 级 `ever_received_fresh`、last-fresh/first-fresh deadline、outer backoff 和 shutdown await。
  - 验证：不新增大型 enum/state-machine、通用配置系统或与需求无关抽象；每个字段均可追溯到失败测试。

### 任务 1.2：实现顺序 break-before-make

- 步骤 1.2.1：business deadline/backoff只唤醒唯一owner；已ready的message/state/applied分支优先，timer分支只读取一次可能异步变化的current SDK generation、state和local-applied并更新恢复状态。仍满足`Connected + local-applied`且恢复状态到期时，消费current runtime并调用任务1.0证明的`shutdown_and_wait(self)`。
  - 验证：transport transition在shutdown调用开始前获胜并取消business action；调用开始后普通顺序控制流等待完成。不重复核对owner-local epoch/fresh，不新增candidate对象、commit标记、active-slot状态机、CAS、mutex、typed stop/control channel或completion reply。
- 步骤 1.2.2：复用任务1.0已证明的wrapper shutdown：结束并await actor、释放receiver和全部manager owner、等待state-channel closure。
  - 验证：`Disconnected`或固定sleep不能放行；coordinator stop继续使用既有`abort_with_permit()`和source task join，不要求stop墙钟时刻与constructor形成事务级线性化。
- 步骤 1.2.3：teardown completion通过后递增`rtds_runtime_epoch`并创建一个新`RtdsSdkRuntime`，继续复用`bounded_rtds_ws_config()`、official SDK subscription和latest-complete desired actor。
  - 验证：每条 wire connection 恰有一次 `crypto_prices_chainlink` / `btc/usd` filtered subscription，无重复 subscribe、unsubscribe、并行 runtime 或 shadow path。
- 步骤 1.2.4：当 `ever_received_fresh=true` 时，以 current generation 同时 `Connected + local applied` 的后到事件启动 60 秒首条 fresh 窗口；否则保持 transport-only 等待。
  - 验证：实际replacement只记录trigger kind、runtime epoch、SDK generation、attempt index及replacement start/runtime created事件；不增加四段专用计时状态、counter、schema或test seam。

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
  - 验证：runtime 0曾有fresh后stuck并完成第一次替换；runtime 1在60秒窗口和outer delay后仍无fresh并完成第二次替换；runtime 2收到fresh并重置episode/backoff。结合生产源码的`shutdown_and_wait().await`后顺序构造、wrapper channel-closure测试，证明两次替换均为break-before-make；每连接只订阅一次完整`btc/usd` filtered desired且无`unsubscribe`。不增加test-only lifecycle hook或跨端时序断言。
- 步骤 1.4.3：不新增100-cycle完整runtime replacement、完整multi-source fixture或FD/RSS/task/watch/receiver/refcount资源矩阵。
  - 验证：历史自然transport 100-cycle测试和既有结果保留原样，但本事项不重新执行或作为新路径验收；完整测试target只编译不运行。

### 任务 1.5：精确验证与阶段审查

- 步骤 1.5.1：代码稳定后执行：
  - `cargo test -p data-collect --locked --lib rtds_ -- --test-threads=1`
  - `cargo test --workspace --locked --no-run`
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo fmt --all -- --check`
  - `git diff --check`
  - 验证：第一条只执行直接受影响且不含历史100-cycle的RTDS lib测试；第二条只编译全部测试target。不得通过默认workspace test重跑历史压力测试；最终检查后若代码变化，仅重跑直接受影响的测试或检查。
- 步骤 1.5.2：逐项把测试名、输入故障、预期与实际结果映射到需求验收标准，自审 diff 后派生独立 subagent 审查正确性、所有权、竞态、SDK-first、数据隔离和过度设计。
  - 验证：成立问题修复并复审，三件套同步实际实现和剩余风险；阶段1 commit/push后按已批准边界进入阶段2。

### 任务 1.6：保持 future paper/live 共同合同而不提前实现

- 步骤 1.6.1：阶段1只实现并测试training owner；不得为尚不存在的paper/live RTDS consumer提前抽象通用adapter、trait或handoff framework。
  - 验证：`data-collect-paper-hot`与`data-collect-live-hot`生产调用图仍只有既有heartbeat/status路径；没有新增外部RTDS socket、training writer或假在线验收。
- 步骤 1.6.2：把future adapter的强制验收入口保留在paper/live集成说明。
  - 验证：未来独立事项必须继承相同fresh classifier、参数/backoff、唯一owner/socket、break-before-make与cold-start重启语义，并另测本机handoff时序/背压；不得复用training Parquet/manifest/dedupe-restore。

### 阶段1执行结果

> 状态：`COMPLETE`。

- wrapper使用唯一source owner消费的同一`RtdsRuntimeEvent`流交付message/state/local-applied；既有queued-message freshness fence保留，vendor SDK未修改。
- `shutdown_and_wait(self)`消费旧runtime、结束并await subscription actor、释放wrapper owner并等待state channel关闭；production同一owner随后才创建新runtime。
- source-local最小恢复状态实现冻结的`300s steady + 60s后到monotonic anchor + cold-start disabled + 1s × 2 ±20%/300s cap/unlimited`合同。已ready事件先处理；每次事件处理后若deadline到期，timer只读取一次current SDK generation/state/local-applied，因此fresh优先且连续非fresh消息不会饿死watchdog。
- 三-runtime loopback完成两次replacement、每连接一次完整filtered subscription、无`unsubscribe`，第三runtime fresh重置episode/backoff。历史100-cycle在本阶段早期、最终简化裁决前曾误跑一次；裁决后的收窄验证门没有再次执行，也未把该结果作为新路径验收依据。
- `cargo test -p data-collect --locked --lib rtds_ -- --test-threads=1`通过20项；`cargo test --workspace --locked --no-run`完成全部测试target编译；workspace全target Clippy `-D warnings`、fmt check和diff check均通过。runtime、测试和authority三路独立终审均为Critical/Important/Minor 0并PASS。
- 阶段1文档验收commit为`ef200a6e89eaaee9f19ff49a00232b5004825ce7`，Rust实现commit为`a29486babe2682481549cfc0b95b010784d653f0`；二者均已push且本地HEAD、upstream与远端ref核对一致。闭合文档生效后的下一游标为阶段2。

## 阶段 2：本地香港 VPN 连续 8 小时完整 training collector

### 任务 2.1：最小隔离 preflight

- 步骤 2.1.1：从承载本authority且已push的clean HEAD设置精确`DATA_COLLECT_GIT_COMMIT`，用`cargo build --release --locked -p data-collect --bin data-collect-training-collector`重建正式training binary，并记录commit、命令和SHA-256。
  - 验证：不得复用早于方案A/`415470e`且内嵌`unknown-build-git-commit`的旧release binary；构建不产生源码diff，也不新增测试入口、JSONL、writer或schema。
- 步骤 2.1.2：新建attempt artifact目录及其下独立、绝对、当前用户拥有的空dataset root，记录clean `HEAD`/upstream、冻结参数、binary SHA、UTC起点和初始香港公网出口/区域。
  - 验证：dataset root不指向生产dataset、已有拉取数据、`/home/chillchill/new-datasets`、paper/live或远端路径；不修改全局VPN、不记录凭据、不接触钱包、远端service或第二RTDS owner。
- 步骤 2.1.3：通过既有香港proxychains启动完整binary，参数固定包含`--dataset-root <步骤2.1.2的新绝对隔离路径> --collector-region local-hong-kong-route --max-runtime-secs 28800 --max-dataset-bytes 50000000000`。
  - 验证：不得同时使用`--run-forever`或外层强杀`timeout`；启动的是`LiveRuntimeSourceSpecs::production()`、正式四个logical producer、六个writer和`ManifestOwner`，不是RTDS-only ignored test。

### 任务 2.2：运行与证据

- 步骤 2.2.1：运行完整collector 8小时，让`polymarket_market`、`polymarket_market_metadata`、`binance_depth_snapshot`、`binance_depth_update`、`binance_aggtrade`和`polymarket_rtds_chainlink_btc_usd`通过正式writer落盘；保存collector日志和真实退出状态。
  - 验证：提前退出、crash、非零退出或bounded runtime不足8小时为未完成/失败；不因单source正常恢复日志自动判整体失败。方案A精确对象成功分类并完成既有自然重连属于预期transport恢复。
- 步骤 2.2.2：结束后记录第二次公网出口/香港区域，读取每日manifest及全部finalized Parquet，汇总六源文件数/行数、RTDS最大相邻接收间隔、自然重连和business replacement。
  - 验证：起止任一确认不在香港时不得标记为香港线路；六个source kind均须有非空finalized Parquet，manifest file facts与路径/行数/bytes/SHA一致；逐个用PyArrow打开manifest登记的finalized Parquet，确认schema/metadata可读且实际`num_rows`等于manifest `rows`；并确认无`.running`/`.tmp`残留。同UTC日bounded停止的顶层manifest应保持`open`、coverage为`ended_known`，不得错误要求当日`closed`；不增加周期route、资源矩阵或独立supervisor。
- Attempt1执行结果：clean HEAD/upstream均为`8db5b9bad683586c55fa49be80fa2aab73444646`，起止香港边界成立；运行`16851391ms`后因127-byte `Network error communicating with endpoint`对象被现行typed parser归为terminal而提前结束。fresh `16127`、最大相邻fresh间隔`8521ms`、自然重连`2`、business replacement `0`、runtime error `1`、test exit `101`。固定结论为`STAGE2_ATTEMPT1_FAIL_INCOMPLETE_8H`。
- 步骤 2.2.3：先以独立纯文档diff同步方案A：正常typed parse失败后，仅把顶层object、精确network-error message且无`topic/type/timestamp/payload`的数据外对象标为crate-private recoverable remote-network marker；其余parser错误保持sticky terminal。
  - 验证：authority独立审查、commit/push和三方ref一致后才进入Rust；按E-A保持已污染的`probe-tee-exit.txt`现状并披露替代证据，不再写入Attempt1目录或改写历史测试结果。
- 步骤 2.2.4：在既有四文件vendor allowlist中的`ws/connection.rs`与`rtds/subscription.rs`实现方案A，并新增一个正负合并runtime loopback。
  - 验证：typed downcast进入既有`ConnectionExit::Recoverable`并重放latest desired；同一测试的负例使用“相同精确message但带一个RTDS数据身份字段”，证明它不进入marker。既有测试继续证明once-replay、sticky/no-reconnect、`Invalid request body`诊断和bounded error；不得增加collector raw parser、公共API、第五个vendor路径、第二owner/socket、service-message矩阵、重复等待、资源计数或100-cycle。完成聚焦测试、必要静态门、独立审查、独立Rust commit/push和三方ref核对。
- 步骤 2.2.5：只在步骤2.2.3--2.2.4全部闭合后，从新的clean HEAD、新artifact目录执行连续8小时Attempt2。
  - 验证：这是旧RTDS-only内存probe的历史启动门。不得复用或覆盖Attempt1目录；程序自身提前失败时记`FAIL + INCOMPLETE_8H`，用户明确终止则如实记`USER_TERMINATED + INCOMPLETE_8H`，不冒充代码失败。
- Attempt2执行结果：从clean HEAD/upstream `0685fcab51a2b7c644ba7c5aca9c873134f88fd4`及香港`8.210.176.1 / HK / HKG`启动，约2.347秒观察到首条合格fresh。用户随后终止并纠正应运行完整training collector；进程已不存在，但未形成结束route、退出码、wrapper result或最终summary。固定结论为`USER_TERMINATED + INCOMPLETE_8H`，不补造精确elapsed、最终计数或数据文件。
- 步骤 2.2.6：以本次纯文档diff把Attempt3验证拓扑改为完整training collector，明确正式六源Parquet/manifest、隔离dataset root和本地香港线路边界。
  - 验证：三件套与七份current pointer一致；明确当前collector会写source v3 + metadata v4，本专项三件套是用户本次批准的首次真实本地部分日v3/v4前向验证authority，但不授权完整UTC日前向长跑、production binary激活或canonical dataset-root cutover。独立审查、纯文档commit/push及三方ref核对完成前不得启动Attempt3；无需Rust代码commit。
- 步骤 2.2.7：从步骤2.2.6发布后的新clean HEAD重建release binary，并按任务2.1与步骤2.2.1--2.2.2运行Attempt3。
  - 验证：Attempt3使用新artifact目录和新隔离dataset root；不得写Attempt1/Attempt2目录，不得运行RTDS-only harness，不得新增JSONL sidecar、100-cycle、资源矩阵或默认workspace完整测试。
- Attempt3执行结果：从clean/pushed `c05db6ba58676bec3e20878375608e395ad0971a`构建SHA-256为`b73f224b1217cef00592422d5dceedea786e5a5eaf70b154c5f2c0340b4b3337`的正式release binary；artifact为`target/rtds-stage2-attempt3-full-training-20260727T042402Z`，起止香港route样本均为`8.210.176.1 / HK / HKG`。wrapper运行`3863.950s`后collector exit 1，直接错误为`Gamma resolution integrity failure:slug=btc-updown-5m-1785129600:ordinal=5`，固定为`FAIL + INCOMPLETE_8H`。11个finalized Parquet覆盖六源且逐文件bytes/SHA/PyArrow rows与manifest一致，无临时文件；这些部分证据不能替代8小时验收。
- 步骤 2.2.8：以独立纯文档diff同步Gamma `closedTime`方案A及Attempt3失败事实。
  - 验证：格式语义只由resolution三件套单点定义；RTDS三件套和七份current pointer只消费该裁决。authority完成独立审查、commit/push和三方ref一致后才进入Rust；不改写Attempt3 artifact或历史Attempt1/2。
- 步骤 2.2.9：只修改共享Gamma `closedTime` parser helper并增加一个Attempt3精确值回归。
  - 验证：既有RFC3339第一路径不变；只把精确`+00`后缀规范化为`+00:00`后再次交给同一Chrono parser；raw原样保存，非法、无时区、其他未批准格式及早于E继续fail closed。只运行直接受影响的Polymarket合同测试和必要静态门，不修改SDK/schema/evaluator/adapter/fatal路径，不新增格式矩阵、RTDS-only probe、100-cycle或资源矩阵。三路审查通过后以独立Rust commit/push并核对三方ref。
- 步骤 2.2.10：从步骤2.2.9及其闭合文档发布后的新clean/pushed HEAD设置新`DATA_COLLECT_GIT_COMMIT`重建release binary，按任务2.1与步骤2.2.1--2.2.2运行Attempt4。
  - 验证：使用新artifact和空dataset root，不续写Attempt3、不拼接剩余时间、不以短smoke替代8小时；同一正式四logical-producer/六source/ManifestOwner/Parquet路径连续运行至少28800秒并满足退出0及全部文件门。
- Attempt4执行结果：从clean/pushed
  `a37cf1181597296b9b2d87f8bc2c93700bda8f48`构建SHA-256为
  `b451b2b53aace314e28503ff4f983c20a7468d8175abfd5c7450d8fa7942cc0a`的正式
  release binary；artifact为
  `target/rtds-stage2-attempt4-full-training-20260727T071429Z`，起止香港route均为
  `8.210.176.1 / HK / HKG`。wrapper运行`8192.100s`后collector exit 1，首因是
  Polymarket market某个Decimal字段收到精确`""`后的parser fatal，固定为
  `FAIL + INCOMPLETE_8H`。16个finalized Parquet覆盖六源且逐文件path/bytes/SHA/
  PyArrow rows与manifest一致，无临时文件；失败raw frame/variant/path未保存，不能
  唯一定位具体字段。
- 步骤 2.2.11：以独立纯文档diff同步Attempt4失败事实和P-B1E；该兼容语义只由
  SDK070三件套单点定义，RTDS三件套及current pointers只消费。
  - 验证：九个官方nullable Decimal精确`"" -> None`；missing/null/official legal
    DecimalString保持，nullable非空malformed及required `price_change.size=""`
    继续fatal；authority完成自审、三路独立审查、纯文档commit/push和三方ref核对后
    才进入Rust。不改写Attempt4 artifact。
- 步骤 2.2.12：只在既有vendor `src/clob/ws/types/response.rs`复用
  `NoneAsEmptyString`实施P-B1E，并新增一个紧凑九字段fixture、一个nullable非空
  malformed负例和一个required size空串负例。
  - 验证：不使用`DefaultOnError`，不新增依赖、raw recorder、第二parser、第五个
    vendor生产路径、schema/counter、诊断框架、100-cycle或资源矩阵；完成直接受影响
    测试、package tests编译、目标Clippy、fmt、diff check、自审、三路独立终审及独立
    Rust commit/push和三方ref核对。
- 步骤 2.2.12执行结果：authority/Rust commits `15feb12`/`9d8e59b`已分离发布，
  三方ref一致；response测试`32/32`、现行v3直接消费者`1/1`及vendor合法非空值
  WebSocket测试`4/4`通过，全feature编译、data-collect `--lib` check、Clippy基线外
  `-D warnings`、fmt和diff-check通过。三路独立终审均为Critical 0 /
  Important 0 / Minor 0。用户批准将陈旧`current-v2`验证措辞最小纠正为现行v3
  直接消费者与vendor F04合法值测试；没有恢复v2或新增测试矩阵。
- 步骤 2.2.13：从P-B1E实现闭合后的新clean/pushed HEAD设置新
  `DATA_COLLECT_GIT_COMMIT`重建release binary，按任务2.1与步骤2.2.1--2.2.2运行
  Attempt5。
  - 验证：使用新artifact和空dataset root，不续写Attempt4、不拼接剩余时间、不以
    短smoke替代8小时；同一正式四logical-producer/六source/ManifestOwner/Parquet
    路径连续运行至少28800秒并满足退出0及全部文件门。若同一Decimal空串错误复发，
    停止报告新证据，不自动扩大nullable范围。

### 任务 2.3：报告与阶段审查

- 步骤 2.3.1：报告Attempt5完整collector六源文件/行数、manifest/file一致性、RTDS最长相邻接收间隔、replacement/自然重连/error计数，以及真实发生replacement时已有日志和Parquet能直接证明的事件顺序，并与历史`6075.343s`作跨环境数量级比较；不强制形成四段耗时或分布。
  - 验证：香港本地不冒充AWS爱尔兰同环境对照；没有真实replacement或样本过少时不推导恢复耗时分布，只写证据实际支持的结论。
- 步骤 2.3.2：同步三件套实际collector证据与剩余风险，自审后派生独立subagent终审。
  - 验证：完整collector机械正确性与真实RTDS replacement效果分别标记；最终报告并列保留Attempt1失败、Attempt2用户终止、Attempt3失败、Attempt4失败和Attempt5实际结果，并明确五次测试拓扑与终态；成立问题闭环后发布commit/push。

## 阶段 3：production release 与部署保持独立授权

- 任务 3.1：本计划不执行 production release 构建、上传、部署、systemd 修改、远端服务操作或生产数据清理。
  - 验证：若用户后续批准，另建部署三件套，冻结 commit、binary SHA256、构建机与命令、artifact 路径、回滚、远端验收和数据边界。

## 本轮文档执行结果

三日真实RTDS typed raw参数分析、任务0.1--0.4及全部参数裁决已完成。阶段0与阶段1均已完成并分离发布；冻结合同仍为`steady=300s`、`first-fresh=60s/后到 monotonic anchor`、cold-start首次fresh前禁用，以及outer `1s × 2 ±20%`、hard cap=`300s`、fresh重置、总尝试不限。历史100-cycle只保留为旧阶段事实。香港线路Attempt1固定为`FAIL + INCOMPLETE_8H`，历史E-A污染已披露；RTDS network-error方案A已由`366fed7d`/`415470e`分离闭合；Attempt2固定为`USER_TERMINATED + INCOMPLETE_8H`；Attempt3因Gamma `closedTime`格式误判在`3863.950s`退出1，Attempt4因Polymarket market Decimal空串parser fatal在`8192.100s`退出1，均固定为`FAIL + INCOMPLETE_8H`并保留六源部分Parquet/manifest。P-B1E已由`15feb12`/`9d8e59b`分离闭合，定向验证与三路终审均通过；当前游标为步骤2.2.13，从承载本closure的新clean/pushed HEAD、新artifact和空dataset root执行完整六源8小时Attempt5。production release、systemd、部署、远端、上传、migration和数据清理仍未执行。
