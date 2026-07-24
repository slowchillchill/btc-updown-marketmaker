# 训练采集全数据源可用性与连接生命周期修复计划

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应需求：`demand/data-collect-source-availability-lifecycle-repair-demand.md`
>
> 对应 changelog：`changelog/data-collect-source-availability-lifecycle-repair-changelog.md`
>
> 状态：`PHASE_9_COMPLETE_RTDS_PONG_A`。用户已于2026-07-17批准本计划、对应需求及需求第4.7节SDK-first逐项patch矩阵，并授权依次执行阶段0--8的本地文档/代码修改、测试、独立审查、commit和push；阶段0至阶段8已经实现、完成最终门禁并通过独立终审。2026-07-18批准的`D7/8-R`、`D7-TIME-R`、`D7-MIN-R`、`D7-DEFER-B-R`、`D8-R`与`D8-STATUS-A`继续作为历史实现authority。2026-07-19香港线路wire验证证明RTDS持续响应业务数据但不返回文本/控制PONG，用户明确回复“批准 RTDS-PONG-A”；阶段9现已按本计划完成RTDS固定PING/no-PONG实现、全量门禁、香港线路SDK复测及独立终审。此前批准的`CT-B`与`D6-SCHEMA-R`继续有效。本计划只提供实现步骤；用户后续批准的生产重置部署以`plan/data-collect-rtds-pong-a-reset-deployment-plan.md`为唯一执行authority。

## 一、成功标准与实施原则

实现完成必须直接满足需求第六节全部28项验收，并同时证明：

- 任意一条、多条或全部六条外部I/O路径不可达，或者四条WebSocket业务流静默时，collector保持同一PID运行；全部I/O不可达才记录有证据的degraded，Gamma已返回有效市场、depth已取得初始snapshot且四条WebSocket协议存活但业务静默时保持active/idle，不虚构状态转换；
- 业务静默不再控制subscription或transport，不制造gap、lag、异常告警或退出；
- transport故障在各自source-local owner内持续恢复，总attempt和总elapsed不构成存活预算；
- heartbeat配置身份明确分离：CLOB生产为`10s/20s/require_text_pong=true`，RTDS生产为固定`5s/require_text_pong=false`，SDK默认保持`5s/15s/true`；RTDS no-PONG不重连，PING实际write确认前最多一个pending且确认后无burst补发；
- RTDS、CLOB、Binance aggTrade、Binance depth/snapshot和Gamma保留各自协议语义；depth另外以snapshot v3/update v4的双坐标明确区分raw观察与continuity可用，不以stitch处理顺序改写原始观察顺序；
- typed raw接收时间、真实空档和last qualifying time可供后续因果派生missingness；aggTrade v6另外保存普通外部invalid、source epoch、connection generation、WS ingress与collector admission coordinate，使训练能按模型实际可见顺序重放；合法WS trade不因原始`a`跳变等待或回填；daily manifest只新增单一collector observation-eligibility coverage并保持既有source counters，不保存transport scope/reason/episode；data-collect不决定模型age/mask、确定性调度基线、离散`next_wake`目标候选或任何decision tick；
- 上限内完整的普通外部aggTrade invalid逐条进入同artifact v6并继续；parser实现/typed-schema内部失败、本地lag/drop、queue接纳失败、sequence hole/overflow、writer/manifest、evidence ownership和内部不变量仍按既有scope quarantine或fail closed；
- vendored SDK只保留经逐项证明仍必要的最小差异，删除旧业务静默策略专用的one-shot reconnect；
- 没有第二套health、reconnect、socket owner、SDK session、receiver或subscription actor；Binance继续复用现有`reqwest + tokio-tungstenite + Tokio + Serde`，不引入新SDK或market-data engine。

用户已于2026-07-17完成需求、计划及原SDK patch矩阵的批准，并于2026-07-19批准`RTDS-PONG-A`修订。任何真实代码、现行已批准文档或官方协议与本计划发生新冲突时，必须停止相关阶段，在changelog登记事实、影响和选项并请求用户决定。

每个阶段均执行：

1. 先写能复现旧错误或冻结保持行为的测试；
2. 完成最小实现；
3. 逐条对照本阶段需求和验收自审；
4. 使用`gpt-5.6-sol`、Ultra Reasoning Level派生独立审查；
5. 修复可执行问题并重复测试/审查；
6. 审查通过后只stage本阶段相关文件，commit并push，再进入下一阶段。

不得把失败测试单独push到共享分支；不得混入现有`AGENTS.md`、`demand/rebuild-core-demand.md`中与本次已批准aggTrade子边界无关的内容或`推荐开发顺序.md`的无关改动。

## 阶段0：最终批准、authority同步与SDK基线

### 任务0.1：冻结最终用户决定

- 步骤0.1.1：用户审查本需求、本计划和新changelog，明确批准、要求修改或拒绝。
  - 验证：changelog记录日期、批准范围和仍未授权事项；“用户批准改写文档”不得冒充“批准开始实现”。
- 步骤0.1.2：逐项确认需求4.7的SDK patch矩阵，尤其是删除one-shot `request_reconnect()`、保留generation隔离、heartbeat-dead recoverable exit和manager-drop normal teardown。
  - 验证：不得用补丁总数代替逐项决定；任一新增vendor路径或公共协议API都触发新决策门。
- 步骤0.1.3：冻结四个逻辑source task与六类artifact的映射：`polymarket -> polymarket_market, polymarket_market_metadata`；`polymarket_rtds -> polymarket_rtds_chainlink_btc_usd`；`binance_depth -> binance_depth_snapshot, binance_depth_update`；`binance_aggtrade -> binance_aggtrade`，并确认训练collector无CLOB user source。
  - 验证：代码常量、mapping测试与计划完全一致，没有虚构user-channel或新增外部源；任何网络动作前先验证mandatory集合。
- 步骤0.1.4：落实用户于2026-07-17明确批准的manifest/coverage决策表，不再保留旧动态scope、16项hard bound、transport reason矩阵或逐source coverage复制。
  - 验证：需求、计划和changelog逐项一致；阶段1只能实现下表已冻结字段与转换，任何新增字段、producer独立生命周期或same-day migration必须停在新决策门。

#### 已批准的 manifest/coverage 决策（2026-07-17）

| 编号 | 批准结论 | 计划约束 |
|---|---|---|
| `M1-A` | collection coverage only | manifest不新增transport scope、reason、current/open摘要或episode；现有source counter与有界operator日志保持各自职责 |
| `M2-A0` | 单一collector coverage | 顶层只保存一个`collection_day_coverage`；不保存四份producer coverage，也不复制进六个source counter |
| `M3-A-R` | 修订后的摘要状态机 | v1只含四态、三个nullable `i64` UTC毫秒时间和单一`reactivated_within_day`布尔值；不保存activation或unclean次数，完整状态/时间不变量与replay eligibility见需求4.2 |
| `M4-A-R` | lifecycle时序修订 | 全成员ready gate、网络前activation ack、每event day/generation permit、rollover owner rendezvous、可靠候选first-close-wins、unclean-dominates、task级close/abort因果证明、共同intake fence、wrapper观察terminal时刻及state-aware recovery |
| `M5-N/A` | 不建设transport summary | 若未来需要持久运维取证，另建需求与schema决策门 |
| `M6-A+` | 顶层v3加直接嵌套v1 | legacy缺对象为unknown；新writer必有；malformed fail closed；merge/recovery保留；不新增通用extensions容器 |
| `M7-A-R` | 修订后的失败边界 | 外部静默/网络、诊断日志失败和既有诊断计数饱和不终止；mapping/schema/state/atomic write完整性故障fail closed；coverage不创建trigger/wake/row |
| `M8-A` | 历史UTC日边界候选 | 已由`DC-DEPLOY-20260718-DIRECT-LONG-RUN`在首次正式冷启动范围内取代；多日停服、旧PID/lease退出且当前UTC日无任何collector产物时任意时刻直接启用唯一`Restart=on-failure` unit，same-day不同identity takeover仍不在本事项内 |

固定生命周期合同：当前四个logical producer、六个writer及coordinator-owned bounded event queue/唯一receiver/ingress路径全部mandatory；任一producer/writer terminal，或queue/receiver/ingress路径失效，都会使共同collector coverage失效并结束collector。任何current-day读取/恢复前先取得并持续持有dataset-root级OS-backed独占实例lease；相同identity不能替代旧进程退出证明。coordinator再完成queue/receiver/ingress前置条件，全部writer/producer wrapper spawn、ready并阻塞共同gate，随后才可发送一次不重试的activation mutation；owner原子写`active`并ack后释放网络poll。未来出现optional producer、独立暂停/重启或任一mandatory terminal后collector继续运行时，必须升级嵌套schema并重新批准。

### 任务0.2：消除旧规范冲突

- 步骤0.2.1：按语义更新`demand/data-collect-demand.md`，删除或改写“空结果/业务静默/stale/断线一律source gap”“每个source总attempt/max elapsed必须耗尽”和“非空外部availability必然令结构完整artifact失败”的旧合同；同步daily manifest允许字段清单，只新增顶层直接嵌套、版本化`collection_day_coverage`。
  - 验证：保留confirmed sequence/local drop/parser/writer等真实gap；明确complete artifact不等于外部coverage 100%；旧manifest“只记录”字段清单包含需求4.2的全部字段，六个既有source counter不承载coverage，且仍只有每日唯一manifest、无receipt/sidecar/transport episode。
- 步骤0.2.2：同步`demand/data-collect-reliability-repair-demand.md`与`plan/data-collect-reliability-repair-plan.md`中的RTDS D2、CLOB silence budget、Binance silence分类、有限总恢复、旧vendor patch白名单，以及“daily manifest只记录”的字段清单与coverage验证步骤。
  - 验证：语义检索`60秒无fresh|两次无fresh|第三次.*重启|business_message_timeout|有限attempt|max_elapsed|六项|request_reconnect`不再存在互斥当前合同；reliability demand/plan都显式采用需求4.2同一顶层v3/嵌套v1和restart/cross-day语义，并明确旧UTC边界首次启用候选由独立直接部署事项取代；仍禁止receipt/sidecar/逐事件transport artifact，旧changelog保持原文。
- 步骤0.2.3：复核并保持本轮已完成的`demand/rebuild-core-demand.md`语义同步：训练采集退化按全部训练外部源的source-local协议恢复，daily manifest只新增单一collector observation-eligibility coverage且保持既有source counters，typed raw空档支持missingness派生；后续批准的aggTrade v6保存逐条invalid及原始/因果接纳coordinate，shared canonicalizer只向模型暴露版本化invalid marker而不暴露raw bytes/parser detail；当前业务不需要精确transport episode artifact且没有默认建设计划。保留BUY-only对侧风险中和语义，并只把用户已批准的离散`next_wake`写成核心目标候选及独立`DecisionScheduleContract`，不得把调度实现扩入collector计划；若实施前core发生漂移，只修复与本事项直接冲突的条款。
  - 验证：core不再把当前manifest摘要或day coverage envelope误称为完整episode历史，也不降低模型学习真实/模拟missingness的目标；明确区分collector availability事实、Observation age/mask和decision scheduler；离散`next_wake`未被冒充champion、active模型或实现授权。若现有dirty diff与本事项重叠且无法手术式合并，立即停止请用户决定。
- 步骤0.2.4：在新changelog记录每份旧文档的被取代内容锚点，不修改旧历史changelog。
  - 验证：需求、计划、changelog使用同一事项编号和路径，且旧事故/批准事实仍可追溯。

### 任务0.3：重建SDK provenance与逐项差异基线

- 步骤0.3.1：记录pinned crate版本、crates.io原始SHA、官方VCS commit、当前官方release/main和相关PR状态。
  - 验证：所有上游结论附访问日期和官方链接；不能只凭旧记忆。
- 步骤0.3.2：把官方原版与vendored树逐文件比较，将每个hunk映射到需求4.7矩阵。
  - 验证：只有`connection.rs`、`clob/ws/types/response.rs`和`rtds/subscription.rs`三个源码路径；每个hunk最终有用户批准的保留、删除或上游替换结论。官方pre-parse完整文本帧trace按用户B决定恢复原hunk并记录failure-trace边界放宽；调用链审计明确记录RTDS已使用bound manager而CLOB生产actor仍须在阶段4补齐generation绑定，不能把vendor能力误报为全部生产接入完成。
- 步骤0.3.3：本事项保持pinned 0.6.0与现有vendor根，检查官方已合并/未合并修复能否按官方hunk/设计最小回移。
  - 验证：不升级Cargo/lock/vendor版本，不整体回退导致已证明的数据完整性能力消失，也不保留无调用依据的补丁；采用更新release必须另建SDK版本升级决策门。

阶段0完成条件：最终文档获批、已批准的需求4.2 manifest/coverage合同完成authority同步、旧authority冲突消除、SDK逐项矩阵与provenance冻结。完成自审、独立审查、commit和push后才进入阶段1。

## 阶段1：共同availability合同与生产测试seam

### 任务1.1：建立生产分类矩阵

- 步骤1.1.1：逐项列出`live_runtime`中所有`record_gap`、`record_lag`、source return、reconnect和subscription调用，映射到business availability、transport availability或confirmed integrity failure。
  - 验证：RTDS、CLOB/Gamma、aggTrade、depth/snapshot和顶层coordinator全部有归类；未知项不得自行归类。
- 步骤1.1.2：审计`health.rs`与实际四个source-local owner，只复用仍符合长期恢复合同的最小类型；删除`CircuitState::Exhausted`作为source终止依据，不把旧registry接成第二套生产控制面，也不为manifest建设通用transport状态机。外部I/O错误使用现有错误链和有界、去重operator日志诊断，不在manifest新增scope、reason、current/open或episode。
  - 验证：新的外部I/O错误不因缺少专用reason、持续时间、尝试次数、既有诊断计数饱和或日志失败而结束source/collector；parser/schema/本地完整性错误不被恢复循环吞掉；没有第二health registry/writer/artifact。
- 步骤1.1.3：严格按已批准的M2-A0/M3-A-R/M6-A+实现顶层单一`collection_day_coverage`：四态、三个nullable `i64` UTC毫秒、单一`reactivated_within_day`布尔值、全部状态/时间不变量、legacy missing=unknown、新writer required、malformed fail-closed与字段保留。首次activation写`false`，任一同日后续activation原子提交时写`true`且当日保持单调；unclean、known end、prior-day reconcile和generic load不得改变。
  - 验证：六个source counter无coverage复制；全部mandatory成员ready但零行时为active，未达到共同起点为not-started；current-day `not_started|active|unclean_end_unknown|ended_known -> active`逐项断言first/last/end与布尔值，证明首次为`false`、三种同日后续activation均为`true`，unclean/known-end/reconcile不清除该值，时钟倒退失败。current top-level closed、identity mismatch、同identity缺对象与非法nested均拒绝；prior-day四态/legacy按state-aware规则关闭。`reactivated_within_day=true`只隔离last start前无法证明连续coverage的历史replay区间，不生成要求parity的Observation且不倒灌unknown mask；unclean共同尾界仅为last start。兼容矩阵另覆盖unknown nested version/field、错误布尔类型、越界时间及每种非法状态。阶段5的`binance_aggtrade_v6` typed raw变化已由用户后续明确批准，视为该决策门已经满足；除此之外若需要producer独立生命周期、run segments、新artifact、typed raw变化或额外consumer职责，仍须停止阶段并提交新schema决策。
- 步骤1.1.4：实现M4-A-R的时序：coordinator先同步构造bounded event queue、取得唯一receiver并初始化现有ingress状态；再spawn ManifestOwner、六writer和四producer wrapper。writer/producer完成内部初始化，producer确认尚未poll source future，十个wrapper ready后共同阻塞gate；ingress前置确认与ready齐备后由owner写active并ack，随后才释放网络poll并进入正常ingress。共同gate按UTC day/activation generation生成进程内唯一`close_id`并冻结第一次可靠known-close candidate，但只在mandatory fence/rendezvous确认无非协调failure后才最终写`ended_known`；此前无法由已完成boundary fence证明只发生在旧日coverage结束之后的未知先后panic/abort/join failure执行unclean-dominates。协作停止复用本次close的精确oneshot ack；强制abort只允许预先登记并由同一handle `JoinError::is_cancelled()`精确消费一次性`(task identity, generation, close_id)` permit。raw event在receive/final enqueue临界区校验day/generation permit，control/barrier绕过raw fence。
  - 验证：dataset独占lease必须先于current manifest恢复取得并持有；并发相同identity实例、lease丢失和写入成功但activation ack丢失均fail closed且不重发mutation。正常启动严格满足queue/receiver/ingress前置→十个ready→active atomic commit→ack→source首次poll；producer在gate前poll count为0。active publish按三个切点验收：临时文件写入/`fsync`或最终`rename`前失败为`NotPublished`，保持最终manifest原始字节与digest不变、无网络动作且全部已spawn资源收敛；`rename`成功但父目录`fsync`失败为`PublishedDurabilityUncertain`，必须返回稳定分类并禁止ack、网络/raw release、重试和rollback，下一独占lease持有者只按实际读到的`not_started|active`恢复；父目录`fsync`成功才是`DurablyCommitted`并允许ack。不得为精确注入父目录`fsync`失败增加生产hook；`NotPublished`用真实临时路径冲突测试，durable commit后ack丢失沿用真实owner/recovery测试，post-rename不确定边界由稳定错误分类与写入顺序审计证明。active commit成功但ack丢失时磁盘active/`reactivated_within_day`不得回滚或冒充not-started，网络/raw permit保持关闭，下一进程按遗留active恢复。双producer terminal、terminal对stop/rollover、先panic后stop、stop已发但cancel生效前panic、匹配/缺失/错误permit、协作ack丢失、`is_cancelled()`与`is_panic()`分别证明可靠候选first-close-wins、unclean-dominates及协调abort不污染。无PendingExit的正常rollover必须依次关闭旧raw generation、drain、收齐四barrier/六finalize ack、原子写旧nested end并关闭旧顶层、创建新日active commit、ack后释放，不能在旧日顶层closed后再merge。overdue可靠stop/terminal在旧日final-close前必须保存一次性PendingExit、把旧日known-close于boundary、普通路径只创建新日open/not-started且不激活/不resume；M8路径不创建新日manifest。旧日final-close提交前无法由fence排除的unclean必须令旧日unknown；旧日已closed而新日activation commit未发生时的failure不得重写旧日，新日保持missing/not-started且布尔值不变；activation commit后的known/unclean已经归属新generation，ack只控制release。若closed后收到声称早于旧fence的failure report必须判fence实现错误并fail closed。分别在`B-1ms/B/B+1ms`、旧日final-close前、close后/新日not-started前后、activation commit后/ack前及ack后注入可靠/unclean结果，以不同poll顺序断言digest/disposition一致且PendingExit不会被当成迟到命令吞掉。producer/writer task的可靠normal/terminal return与非协调panic/abort/join failure分别产生known candidate或unknown tail；coordinator ingress路径提前退出、唯一receiver ownership丢失或queue不变量失败必须按可否可靠完成共同fence区分known end与unknown tail，不得把queue冒充独立spawn task。所有ManifestOwner mutation均不丢coverage。

### 任务1.2：建立可复现的私有测试seam

- 步骤1.2.1：为四个生产source loop提供最小私有测试注入点，复用真实解析、状态机、queue和stop路径，只允许替换endpoint、毫秒级timing与测试deadline。
  - 验证：生产endpoint/config保持硬编码权威；没有公共test-only API、第二条生产实现或大而全mock框架。
- 步骤1.2.2：为coordinator建立四source handle测试harness，能分别控制business silence、transport failure、recovery和terminal integrity error。
  - 验证：测试直接经过生产task完成检测，不以只测`health.rs`抽象冒充生产验收。
- 步骤1.2.3：先冻结顶层保持行为：terminal source error仍被`source_task_ended_unexpectedly`发现；用测试harness记录当前RTDS/CLOB错误返回的复现命令和断言位置，正式目标红灯分别留在阶段3、4与对应实现同阶段转绿。
  - 验证：阶段1提交不包含失败测试；writer/parser terminal保持测试通过，RTDS/CLOB旧错误有可重复复现证据且没有被误写为正确合同。

### 任务1.3：最小接入中性availability恢复

- 步骤1.3.1：把当前已经具备的真实transport恢复路径映射到各source既有唯一owner；纯业务静默不创建collector状态转换，恢复状态不持久化为manifest scope或episode。本阶段只建立分类、coverage lifecycle与测试seam，不提前宣称阶段2--6尚未修复的协议路径已满足无限恢复。
  - 验证：现有恢复路径不回退且可停止；有界operator日志不虚构消息数、silence-start或完整episode，日志失败不改变运行结果。heartbeat-dead、RTDS/CLOB/各Binance正式持续恢复断言分别留在阶段2--6转绿。
- 步骤1.3.2：只对阶段1新增的共同classification/coverage路径和fixture冻结business/transport availability不进入`source_gaps`或业务`lag_events`、confirmed integrity仍进入原路径；不提前改写阶段3--6各协议现有生产分支。
  - 验证：共同fixture正反例覆盖单一`collection_day_coverage`、外部availability与parser/schema/sequence/local drop边界，且不存在动态scope容量导致的fail-closed；RTDS、CLOB、aggTrade与depth生产端到端无gap/lag断言分别在阶段3--6转绿并由阶段7统一验收。

阶段1完成条件：共同分类、coverage lifecycle与测试seam可执行，当前已有恢复路径不回退，中性availability不与gap混淆；不要求阶段2--6尚未实施的协议修复提前通过。阶段独立审查重点检查是否过度抽象或新增影子控制面；通过后commit/push。

阶段1执行结果（2026-07-17）：`COMPLETE`。实现与全量验证通过，三路独立终审均为Critical 0、Important 0；用户批准的单一`reactivated_within_day`和原子发布R三态语义已同步实现。阶段3--6才修复的旧RTDS/CLOB/Binance协议行为继续作为计划明确的复现基线，不冒充阶段1缺陷或已完成能力。

## 阶段2：最小修复vendored SDK transport生命周期

### 任务2.1：先建立SDK红灯与保持测试

- 步骤2.1.1：loopback server接受PING但不返回PONG且保持socket开放。
  - 验证：修复前明确失败为旧socket/generation未退出，而不是server主动close或测试自身race。
- 步骤2.1.2：正常PONG时分别在裸`ConnectionManager`与`MarketSdkRuntime/RtdsSdkRuntime`集成层drop全部manager/actor持有者。
  - 验证：修复前能观察旧socket/task不收敛；目标是normal teardown且无新handshake。
- 步骤2.1.3：建立互斥退出反向测试：sender closed、read EOF、Close、read/write/PING failure、heartbeat timeout/旧PONG和parser fault。
  - 验证：只有sender closed不重连；transport类全部重连；parser fault保持terminal。
- 步骤2.1.4：保留parser sticky fault、CLOB array all-or-error、`SimpleParser::Default`、generation isolation、bounded RTDS diagnostics和`Lagged(n)`暴露测试。
  - 验证：transport生命周期修复不回退独立合同；pre-parse raw trace按用户B决定恢复。先用非法type scalar与nested sentinel证明当前`RtdsParseDiagnosticError::Display`、sticky fault和tracing泄漏边界，再让目标外显Display/sticky fault/warn只保留2,048-byte有界allowlist摘要；原error kind与source chain仍可typed检查。测试和文档明确官方pre-parse trace会记录完整失败帧，且把它计为获批边界放宽而非脱敏通过。
- 步骤2.1.5：盘点所有使用one-shot reconnect的测试，区分ticket coalescing/current-generation专属断言与自然generation隔离、资源、select公平性、每代replay/freshness fence回归。
  - 验证：阶段2暂时保留one-shot API，不先删除生产依赖；需要保留的自然generation测试规划改用server Close/no-PONG驱动，不能随ticket测试一并删除。

### 任务2.2：实施heartbeat与normal teardown

- 步骤2.2.1：按官方方案或经审查的最小等价方案，把heartbeat timeout/旧PONG结果交回当前connection owner，使旧socket退出并进入现有recoverable loop。
  - 验证：一个active socket只有一个heartbeat task，没有新公共API或第二owner。
- 步骤2.2.2：增加私有互斥退出结果或等价判断；只有最后一个outbound sender关闭令outer loop正常终止，其他`Ok(())`/transport退出不得被误判。
  - 验证：裸manager及两个runtime drop测试不重连，read/write/EOF/Close/PING/heartbeat反向测试仍重连，parser仍terminal。
- 步骤2.2.3：比较原始crate、修改前vendor和修改后vendor。
  - 验证：源码路径集合不扩大，heartbeat/teardown及RTDS外显错误脱敏差异映射需求4.7；one-shot补丁在本阶段有意保持到阶段3原子删除，vendor目录无target/临时文件。
- 步骤2.2.4：用CLOB parser和generic manager运行no-PONG loopback回归，保持socket开放直到heartbeat timeout，并同时覆盖parser/fault与dynamic desired保持行为。
  - 验证：旧CLOB generation因heartbeat结果传播而退出并进入现有恢复路径；既有CLOB `10s/20s` split只作为已完成配置回归基线被断言，本阶段不重复实现或改写该split。
- 步骤2.2.5：为SDK `tracing` feature安装测试subscriber，捕获RTDS parser失败的pre-parse trace、warn与sticky fault；用非法type scalar和nested payload sentinel覆盖外显Display/log，并冻结用户B决定的官方raw trace恢复行为。
  - 验证：Display/sticky fault/warn不含未列入allowlist的scalar或nested sentinel，摘要长度与UTF-8边界成立，kind/source chain断言保留；官方pre-parse trace明确包含完整失败sentinel，并被记为用户批准的日志边界放宽而非脱敏通过。

阶段2完成条件：需求验收11--13通过，独立审查确认heartbeat/socket/task生命周期及退出分类正确；one-shot API和旧RTDS消费者仍同时存在，不形成不可运行中间commit。通过后commit/push。

阶段2执行结果（2026-07-17）：`COMPLETE`。heartbeat结果由唯一connection owner直接消费，last-sender normal teardown与recoverable/terminal退出分类已经闭合；RTDS Display/sticky/WARN保持有界脱敏，用户B批准的官方pre-parse raw TRACE已恢复。no/old-PONG、sender/runtime drop、EOF/Close、outbound/PING write、parser与heartbeat panic反向矩阵通过；one-shot API、wrapper和旧RTDS消费者按计划完整保留到阶段3。workspace与vendored SDK全量测试、lint、fmt及provenance/路径检查通过，三路独立终审均为Critical 0、Important 0；阶段2精确commit/push完成后进入阶段3。

阶段2历史说明（2026-07-19修订）：上述no/old-PONG recoverable结论继续适用于`require_text_pong=true`的CLOB；其被机械应用到RTDS的部分已由阶段9 `RTDS-PONG-A`取代。阶段2当时的实现、测试和审查事实不回写删除。

## 阶段3：原子删除RTDS主动恢复并收缩SDK

### 任务3.1：冻结RTDS目标测试

- 步骤3.1.1：server持续PONG但长期不发业务frame，随后在同一socket发送stale和fresh payload。
  - 验证：修复前以主动generation切换/退出或lag副作用失败；目标为一次subscribe、generation不变、payload正常落盘。
- 步骤3.1.2：初始连续连接失败超过旧3次/20秒，再启动server；另测运行中close后失败超过旧预算再恢复。
  - 验证：source不提前返回，每个成功generation只replay一次完整`btc/usd` desired；另让server接受TCP但不完成WebSocket握手，单次`connect_timeout`后下一次attempt成功，证明期限取消卡住的future而不终止source。
- 步骤3.1.3：保留SDK `Lagged(n)`、parser/schema、actor和writer terminal负例。
  - 验证：这些错误不进入无限transport重试。
- 步骤3.1.4：先把依赖one-shot API但实际验证自然generation隔离、资源、select公平性、每代replay和freshness fence的测试改用server Close/no-PONG驱动。
  - 验证：只删除ticket专属断言，不丢失自然reconnect回归覆盖。

### 任务3.2：同一阶段原子删除旧消费者与SDK能力

- 步骤3.2.1：保留`5s/15s`和有界max backoff，把transport总attempts恢复为无限；删除调用方20秒connected和3秒applied存活门。按用户批准的`CT-B`在SDK `Config`增加默认`None`的`connect_timeout`，RTDS生产显式设为20秒，并在唯一connection owner中用它包围完整`connect_async()`。
  - 验证：未连接时desired留在watch；单次DNS/TCP/TLS/WebSocket建连/握手超时只形成recoverable attempt并进入有界backoff，总循环继续；成功握手后不再存在该timer，健康PING/PONG下业务静默无限运行；stop/deadline在剩余单次建连期限与有界backoff之和内收敛，不遗留无限卡住的SDK task。
- 步骤3.2.2：删除RTDS freshness episode常量、状态、`request_reconnect`生产调用、两generation预算、business-timeout gap/lag/alert/fatal分支。
  - 验证：source loop只持续receive、处理stop/deadline和真实terminal error，无等待永远不会发生的主动generation，也无tight loop。
- 步骤3.2.3：在同一未提交工作阶段删除`RtdsSdkRuntime::request_reconnect` wrapper、vendored ticket/API与ticket专属测试。
  - 验证：对`data-collect/src/**/*.rs`、`data-collect/tests/**/*.rs`及vendored `src/**/*.rs`做符号定义/调用检索为0；需求、计划和历史changelog允许保留文字证据。
- 步骤3.2.4：保留fresh/stale typed分类、dedupe、raw payload、generation isolation和每代一次actor replay。
  - 验证：stale不控制网络或lag，`Lagged(n)`仍明确失败，自然reconnect回归全部通过。
- 步骤3.2.5：保留RTDS source-local transport/subscription generation事实与last qualifying fresh time，不新增RTDS silence timer，也不写入manifest transport scope/reason/episode。
  - 验证：generation advance只驱动自然reconnect/replay并可形成有界operator诊断，不进入`source_gaps`；业务missingness只由后续`ObservationContract`根据last qualifying time、known/unknown coverage与当前逻辑求值时间共同因果派生，collector不实现mask或调度。

阶段3完成条件：需求验收7--10及共同availability断言通过；旧“两代无fresh必须退出”测试改为长期静默不退出，one-shot消费者与SDK能力在同一commit中消失。独立审查通过后commit/push。

阶段3执行结果（2026-07-17）：`COMPLETE`。生产RTDS已改为一次desired订阅后在健康PING/PONG连接上无限等待业务数据；业务静默/stale不再触发lag、gap、subscription、generation切换或退出。自然transport恢复总attempt无限，`CT-B`只限制单次建连/握手并通过半握手loopback；one-shot生产consumer、runtime wrapper和vendor能力已原子删除。首轮范围审查发现的旧公共business-gap合同与`deadline=None`覆盖缺口均已修复，定向、workspace全量、vendored SDK、lint、fmt和四路径provenance验证通过，三路独立终审最终均为Critical 0、Important 0；精确commit/push后进入阶段4。

阶段3历史说明（2026-07-19修订）：其中“RTDS健康连接必须PING/PONG”及`5s/15s`会等待PONG的部分已由阶段9取代；一次desired、业务静默不控制网络、真实Close/EOF/读写失败自然恢复、无限总attempt与`CT-B`单次握手期限继续有效。阶段3使用PONG维持loopback和以no-PONG驱动重连的RTDS测试不得作为阶段9当前证据复用。

## 阶段4：CLOB market与Gamma metadata

阶段4必须在`bounded_ws_config()`显式启用`connect_timeout=20s`，复用阶段3已批准的同一SDK配置能力；这只补齐CLOB单次建连/握手期限，不改变`10s/20s` heartbeat，也不得成为CLOB业务静默期限。

### 任务4.1：冻结CLOB/Gamma目标测试

- 步骤4.1.1：current/prefetched assets跨越至少三个旧silence budget等价周期，期间只向其中一个asset发送合法业务事件，最后再向另一个asset发送事件。
  - 验证：修复前第三次返回错误；目标为无unsubscribe/resubscribe、无generation变化、事件正常消费，两个asset的last qualifying time按identifier独立且不会互相恢复。
- 步骤4.1.2：执行current/prefetched/retired lifecycle切换和自然transport reconnect。
  - 验证：只有desired set真实变化才发送精确diff，新generation只replay最新完整set；分别在旧actor已观察connected后切代，以及发送后复核与ack发布之间切代，旧generation延迟请求不得写入新socket，generation-tagged旧ack不得满足wait。
- 步骤4.1.3：Gamma连续网络失败、rate limit和空候选后恢复；另注入矛盾identity/schema。
  - 验证：availability不结束source；恢复时按当前时钟重算current/next window且不再请求过期slug，完整性负例仍失败。

### 任务4.2：移除业务静默控制权

- 步骤4.2.1：删除`MarketAssetFreshnessWatchdog`、30/60秒业务静默阈值、resubscribe attempt、unsubscribe/subscribe和fatal budget，不在collector中保留替代silence detector。
  - 验证：全仓不存在“第三次silence必须process restart”或collector业务静默阈值的当前合同、常量或测试；last qualifying market receive time仍可追溯。
- 步骤4.2.2：保持market lifecycle actor、desired asset diff、`10s/20s` heartbeat和自然reconnect replay；让CLOB actor像RTDS actor一样先观察generation，以既有`bind_generation(observed_generation)`构造该代manager，发送后复核connected state/generation。把项目私有applied watch值改为generation-tagged类型，发布ack时携带observed generation，waiter要求ack generation等于当前generation、revision满足且state connected。
  - 验证：没有第二subscription actor、公共SDK API或按静默修改desired revision；旧代manager不能借当前atomic generation把延迟请求重标为新代，post-check/ack间切代的旧ack也不能被revision-only误收，两个精确竞态与RTDS同类bound-generation回归均通过。
- 步骤4.2.3：Gamma外部availability统一进入既有source-local持续恢复；空候选不伪造metadata或market row，也不创建manifest transport/candidate scope。
  - 验证：服务恢复后按当前时钟重算current/next discovery window、不追逐过期slug，旧失败不阻塞其他source；HTTP成功与合法candidate只更新对应运行路径和last qualifying事实。

阶段4完成条件：需求验收14--17通过，CLOB业务语义除silence控制与availability分类外不漂移。独立审查通过后commit/push。

阶段4执行结果（2026-07-17）：`COMPLETE`。生产CLOB已删除30/60秒business-silence watchdog、主动refresh/resubscribe、generation切换与有限退出预算；健康`10s/20s` PING/PONG socket在一次desired订阅后无限等待业务数据，CT-B只限制单次建连/握手。真实current/prefetched/retired变化仍发送精确desired diff，自然reconnect只replay最新完整set；generation-bound manager、发送后generation/state复核与私有generation-tagged applied ack的两处切代竞态均已封闭。Gamma网络/HTTP/rate-limit/合法空候选按availability持续恢复，延迟响应在真正fence后安装点按处理时钟复核current window；expired initial/next/rollover不写伪gap或安装旧context。相同slug的Gamma/SDK market id、condition、Up/Down资产与outcome映射在inner rollover及outer transport恢复间保持连续，矛盾schema/identity/mapping继续fail closed；跨过多个旧slug的恢复不被误拒。需求验收14--17、data-collect全量测试、lint、fmt、diff、SDK contract与四路径provenance验证通过，三路独立终审均为Critical 0、Important 0；精确commit/push后进入阶段5。

## 阶段5：Binance aggTrade

### 任务5.1：冻结静默、transport与WS因果直录测试

- 步骤5.1.1：server持续PING但跨多个旧60秒等价周期不发aggTrade，随后同socket发送合法事件。
  - 验证：每个PING重置30秒全frame inactivity边界、socket保持且事件正常消费；业务静默不产生lag/gap/reconnect。
- 步骤5.1.2：对照完全无任何frame超过加速后的30秒边界，并分别注入connect error、close、EOF、server shutdown和长期恢复。
  - 验证：只有全frame inactivity/真实transport故障触发中性reconnect，同PID持续恢复且total cycles不结束source；每次成功连接的`connection_generation`严格增加，source epoch保持不变。
- 步骤5.1.3：注入`100 -> 103 -> 104`、更大ID前跳、相同/下降ID、payload conflict、完整application payload无效及parser/typed-schema内部失败。
  - 验证：每条合法WS trade不得因ID关系等待；当前日event按接收顺序进入bounded queue并保存/handoff，跨日activation未完成时进入pre-admission staging并只在正常flush后取得admission。不得调用REST、不进入ID-hole pending、不记gap、不主动重连且generation不变；payload conflict保留双方事实，普通外部invalid保存v6 evidence并继续，parser实现、证据编码、typed-schema及本地queue/writer不变量仍按冻结scope失败。
- 步骤5.1.4：先建立同毫秒`trade A -> invalid B -> invalid C -> trade D`与`trade 100 -> trade 103 -> invalid -> trade 104`红灯测试。
  - 验证：B/C不去重、不合并且raw bytes可逆；两组消息的WS ingress与collector admission均保持真实因果顺序，不能由receive timestamp或Parquet物理行序回排；自动静态回归证明生产代码、依赖与配置不存在aggTrade REST endpoint/client/task。

### 任务5.2：删除业务deadline与ID-hole恢复控制

- 步骤5.2.1：删除`business_message_deadline`对socket生命周期的控制，以及`business_message_timeout`对应lag/gap/reconnect。
  - 验证：复用depth同一30秒全frame inactivity transport边界；任意完整application frame或合法PING重新武装，不从无成交推断连接坏。
- 步骤5.2.2：删除aggTrade实时REST client/backfill task、pending/merge/range retry、unresolved range和active continuity coordinator；合法WS trade不因ID关系等待，当前日直接走现有有界enqueue/handoff，跨日activation未完成时遵循pre-admission staging例外。
  - 验证：自动静态回归证明生产调用图、依赖与配置不存在`GET /api/v3/aggTrades`、aggTrade REST client及第二task，ID数值关系不影响接纳；历史`backfill_rows`只作兼容计数且新运行始终为零。
- 步骤5.2.3：保留真实close/error/server shutdown恢复、PING/PONG、source-local transport事实、last qualifying receive time与checked连接代次。
  - 验证：网络恢复使用既有唯一source-local loop；成功握手后generation递增，同连接全部trade/invalid共享非零值，失败attempt不虚构generation；无成交和ID跳变均不进入`source_gaps`。

### 任务5.3：同artifact v6 invalid evidence、排序与异步接纳

- 步骤5.3.1：把现有`binance_aggtrade`新writer升级为`binance_aggtrade_v6`判别联合，并先增加layout、Parquet round-trip与真实legacy v5恢复测试。
  - 验证：`record_kind=trade|invalid_input`封闭；新trade只来自WS，invalid的trade-only字段为null且exact evidence完整；既有REST nullable列只作兼容且新行全部为null。v5历史WS/REST/merged行只读为legacy trade，不伪造sequence/generation；仍只有既有artifact、writer和manifest mapping，无sidecar/第七artifact。
- 步骤5.3.2：为aggTrade source task创建稳定跨reconnect/UTC rollover、重启即更换的`source_sequence_epoch_id`；每次成功WebSocket连接分配checked非零`connection_generation`，并在每个完整Text frame parse前冻结checked `websocket_ingress_sequence`。
  - 验证：同socket的trade/invalid共享generation，重连后严格增加；invalid逐条编号，sequence/generation overflow fail closed；合法duplicate消耗真实到达时已经冻结的ingress sequence但不产生typed/model evidence，既有有界`structural_dedupe_dropped_events`解释该编号空洞，空洞不得冒充source缺失或模型事件。
- 步骤5.3.3：在现有global row/byte预算下，event取得对应raw-day permit并把ownership转移到bounded queue时冻结`collector_admission_sequence`与`collector_admission_time_utc_ms`。尚无新日permit的跨日deferred只是有界pre-admission staging，正常rollover flush取得permit与queue ownership后才按staging顺序冻结坐标。
  - 验证：writer/flush暂停但queue有容量时source可接纳完整`trade/invalid/invalid/trade`且不等待Parquet append/finalize/fsync；writer恢复后全部落盘。短时queue满仅有界背压且不丢，timeout/closed仍terminal；不声称满载global enqueue对其他source完全隔离。
- 步骤5.3.4：WebSocket Text/Binary application input在`524_288` bytes内按原bytes可逆编码；连接配置在cap处拒绝更大message并保存真实oversize metadata。invalid只重置全frame inactivity，不更新last qualifying或dedupe。
  - 验证：malformed JSON、缺字段、错误event/symbol/stream、binary与连续相同invalid均逐条保存并继续同socket；oversize不无界分配、不伪造payload/digest，可重连但不结束collector。
- 步骤5.3.5：删除新生产对旧manifest `binance_aggtrade_continuity`的创建、更新、Parquet推导和运行控制；读取既有manifest时原样保留该字段。
  - 验证：current writer不产生或改变该字段，现存合法字段经其他ManifestOwner mutation仍保留；v6恢复只让WS trade参与WS结构去重，invalid和历史REST evidence不参与。
- 步骤5.3.6：升级可关联handoff evidence编码及contract测试，但不接入尚不存在的真实paper/live source runtime或shared canonicalizer。
  - 验证：同一typed source evidence分别编码为v6 finalized row与handoff frame时，record kind、source epoch、connection generation、WS ingress、causal admission和total-order输入等价；现有heartbeat `event_seq`不充当source/admission sequence。queue接纳后durable publish前的crash测试保持unknown-tail边界。

阶段5完成条件：需求验收18--19、23、25及28中aggTrade适用断言全部通过；独立审查确认生产调用图没有aggTrade REST/backfill/pending/merge/active continuity控制，ID前跳保持中性，WS因果顺序与legacy读取兼容成立，没有伪造durability、busy loop/无界分配或把raw坏payload直接暴露给模型。通过后commit/push。

阶段5执行结果（2026-07-18）：`COMPLETE`。生产aggTrade路径已经收敛为单一WebSocket owner。合法trade与上限内invalid frame均按真实接收顺序取得ingress coordinate，在既有有界queue取得ownership时取得admission coordinate，并异步写入同一`binance_aggtrade_v6` artifact；contract test证明相同source evidence可编码为等价handoff frame。ID前跳不等待、不回填、不写gap且不控制socket。实时REST backfill、pending、merge、range retry、unresolved range和active continuity控制已经删除；成功握手才递增非零`connection_generation`，source epoch跨同一task重连稳定，Close/EOF/protocol error/server shutdown及全frame inactivity继续在同一PID内无限恢复。历史v5 REST/merged row、v6 nullable REST列与旧manifest continuity只读兼容，历史`backfill_rows`兼容计数保留且新运行恒为零，当前writer拒绝生成REST evidence。首轮独立审查发现并修复“legacy REST-only row先占用dedupe恢复窗口”和“writer未调用完整row validator”两项缺陷；相应负例与limit=1恢复回归均已加入。真实loopback覆盖握手失败不消耗generation、全frame inactivity、reserved-opcode protocol error、payload conflict、`100 -> 103 -> invalid -> 104`原序接纳及100次Close恢复/资源有界；writer暂停测试证明producer在queue容量内不等待Parquet确认，恢复后全部v6 row落盘。`cargo test -p data-collect --locked -- --test-threads=1`通过224项lib及全部integration/doc tests，新增自动静态回归证明生产代码、依赖与配置不存在aggTrade REST endpoint/client/task；`cargo check -p data-collect --all-targets --locked`、严格Clippy、fmt与diff check均通过。阶段5单source的100次恢复验收已经完成；其后批准的`D7-MIN-R`已把阶段7新增范围精确收敛为三个完整collector场景、CLOB/Gamma各100-cycle及受控cutoff的depth pending/跨日staging聚焦回归，不再保留额外多source组合或系统级FD/task/queue压力场景。三路独立终审最终均为Critical 0、Important 0；精确commit/push后进入已授权阶段6。

## 阶段6：Binance depth与snapshot

### 任务6.1：先冻结双artifact schema与因果顺序红灯测试

- 步骤6.1.1：为`binance_depth_snapshot_v2 -> v3`和`binance_depth_update_v3 -> v4`先写schema、validator、Parquet round-trip及legacy读取测试。
  - 验证：两个新schema共同保存非空source epoch、非零connection generation、共享域depth ingress sequence及collector admission sequence/time；snapshot/update既有官方字段与stitch evidence不丢失。真实旧snapshot v2/update v3仍可读，但ordering明确为unknown，不补默认值、不伪造epoch/generation/sequence，也不得与新schema混称精确causal replay。
- 步骤6.1.2：建立确定性loopback：WebSocket成功后snapshot HTTP保持pending，依次在t1/t1'发送两条depth update，再在t2返回snapshot。
  - 验证：唯一WS reader在HTTP pending期间继续读取；两条update按真实观察先后冻结`timestamp_received/depth_ingress_sequence`并进入单一有界buffer。snapshot成功后先取得collector admission，buffered update再按ingress顺序取得admission；t2后形成的stitch verdict不回写成t1模型可见事实，Parquet物理写入/flush顺序不被当成任一权威顺序。
- 步骤6.1.3：分别注入`u <= snapshot lastUpdateId`、`u <= local_u`、首桥`U <= lastUpdateId + 1 <= u`、后续连续update及`U > local_u + 1`。
  - 验证：每条结构合法raw update都只保存一次；stale/duplicate和confirmed hole写v4 not-applied verdict、只更新artifact接收事实，只有首桥/连续update推进snapshot anchor/`local_u` continuity及last-qualifying事实。hole使旧continuity state失效并触发新snapshot，但同一hole update不得在新snapshot后被改写、回排或应用。
- 步骤6.1.4：覆盖snapshot HTTP timeout/rate limit持续超过旧单cycle预算、`418/429 Retry-After`期间future replacement/day reset/reconnect、可用snapshot前Close/EOF、generation放弃，以及无法解析`U/u`、`U > u`、非法snapshot/schema、sequence overflow和本地continuity state不变量。
  - 验证：外部snapshot不可用不耗尽source存活预算；source-task级monotonic not-before gate不得被snapshot future取消或连接切代清除。正常运行中的snapshot unavailable、Close/EOF、普通reconnect/generation放弃和UTC day reset都把结构可验证的buffered update按ingress以`not_applied_snapshot_unavailable`中性verdict有界release；只有可靠operator stop/run deadline已关闭共同intake时，未取得共同bounded admission ownership的pending可以清空且不生成row/gap/告警/terminal。无法形成可信typed row及本地完整性负例仍按冻结scope失败。
- 步骤6.1.5：持续发送PING但不发depth update；另一个场景完全不发送任何frame。
  - 验证：前者不重连，后者按transport inactivity重连并为真实新generation重新snapshot。

### 任务6.2：用现有owner实现最小并行协调

- 步骤6.2.1：保留现有唯一`tokio-tungstenite` direct WebSocket owner和`reqwest + Tokio + Serde`，在每次成功握手后分配connection generation，启动唯一WS reader与至多一个active snapshot future；增加单一generation-local有界buffer和共享depth ingress allocator。
  - 验证：Cargo/lock不新增Binance SDK、Barter、Nautilus或其他market-data engine；生产调用图没有第二socket/snapshot owner、第三artifact、per-frame task或无界容器。buffer同时受row/byte硬上限约束，stop/deadline/day fence能取消/收敛当前generation且不丢弃已admit evidence。
- 步骤6.2.2：在snapshot/update两个既有writer路径接入v3/v4字段；raw观察时冻结depth ingress，snapshot已验证并从buffer release时才在既有bounded admission取得collector admission。
  - 验证：snapshot先admit、buffered updates按ingress release；writer/flush暂停但容量存在时不要求Parquet ACK。正常运行中的snapshot最终不可用或generation放弃执行中性最终release；可靠operator stop/run deadline只截断未取得对应raw-day permit与queue ownership的generation-local pending或跨日deferred staging，已经取得permit并进入queue admission的evidence仍drain。queue接纳后durable publish前崩溃继续落入unknown tail，不宣称已发布。
- 步骤6.2.3：把raw保存与continuity projection分开，更新stitch状态机并保留confirmed hole resnapshot。
  - 验证：stale/hole raw row不会因continuity不应用而消失；raw row已保存也不自动表示continuity-ready。首桥固定`U <= lastUpdateId + 1 <= u`，后续仅`U > local_u + 1`为confirmed hole；collector只推进snapshot anchor/`local_u`与verdict，原样保留`bids/asks`和`b/a`，没有新增或发布price-level book。
- 步骤6.2.4：把完全无入站frame与有控制frame但无业务update分开，并统一snapshot外部availability与schema/invariant terminal边界。
  - 验证：单纯transport断开不进入`source_gaps`或业务lag；transport evidence只更新source-local recovery state，合法snapshot+stitch才更新continuity state，两者都不持久化为manifest scope。可解析的`Retry-After`只保存在source-task内存not-before gate并跨day/reconnect生效，不写manifest。网络无限恢复，损坏payload/ownership/sequence/continuity不变量按既有范围失败。

### 任务6.3：兼容、范围与资源终审

- 步骤6.3.1：复跑既有archive/source-hourly/writer/manifest/handoff contract，并增加v2/v3 legacy与v3/v4 current矩阵。
  - 验证：旧日级归档仍只读真实legacy字段；新消费者不把legacy `timestamp_received`或文件顺序冒充精确ingress/admission。snapshot/update仍是两个既有source目录与manifest source counter，不新增artifact或coverage复制。
- 步骤6.3.2：执行depth snapshot pending/success/failure、100-cycle snapshot/reconnect与FD/task/socket/buffer采样。
  - 验证：reader、snapshot future、buffer、FD、socket、queue与writer ownership不单调增长；没有tight loop、busy wait、正常运行恢复路径中的raw静默丢失或因snapshot慢阻塞全部健康source。受控operator stop/run deadline未接纳pending按第4.6节例外验收，不冒充raw row。
- 步骤6.3.3：自审后派生protocol/schema因果审查与范围/资源审查。
  - 验证：两路审查均确认官方snapshot/update-id continuity算法没有被“允许缺失”弱化，`+1`首桥、raw/continuity分层、legacy unknown、无collector price-level book、依赖/owner边界及全部需求20--22逐项闭环；Critical/Important为0。

阶段6完成条件：需求验收20--22及25/28中depth适用断言通过；没有新增第三artifact、SDK/engine或第二owner。独立审查通过后commit/push。

阶段6执行结果（2026-07-18）：`COMPLETE`。生产depth路径继续只有一个`tokio-tungstenite` owner和两个既有artifact，schema已升级为snapshot v3/update v4；两个schema与版本化handoff共同承载source epoch、connection generation、共享depth ingress和collector admission坐标，真实legacy snapshot v2/update v3 Parquet只读为ordering unknown。唯一WS reader与至多一个snapshot future在同一`select!` owner内并行，generation-local pending buffer同时受row/byte上限约束；snapshot先admit，buffered updates再按ingress分类/release。canonical状态机只产生`applied`、`not_applied_stale_duplicate`、`not_applied_confirmed_hole`和`not_applied_snapshot_unavailable`四种verdict，stale/hole/raw失败释放都只保存一次，同一hole不会在新snapshot后重分类或应用，collector未新增price-level book。

HTTP snapshot路径使用有界全body读取和完整typed-row预检；408/418/429/5xx保持外部availability，其他4xx与schema/invariant错误保持terminal。合法`418/429 Retry-After`更新source-task级monotonic not-before gate，snapshot future替换、UTC day reset和WebSocket reconnect都不能清除；跨future loopback已证明期限前没有第二请求、期限后恢复。100次真实WebSocket reconnect到generation 101及100次真实HTTP resnapshot均通过，证明同一owner持续运行、坐标唯一、pending中性释放及FD/socket有界；实现静态只有一个局部snapshot future和一个有界buffer，没有per-cycle/per-frame spawn。

最终门禁通过：`cargo test -p data-collect --locked -- --test-threads=1`为245项lib及全部integration/doc tests；depth定向31/31通过；`cargo check -p data-collect --all-targets --locked`、严格Clippy、fmt与diff check均退出0。protocol/schema因果终审为Critical 0、Important 0、Minor 0；异步/资源终审为Critical 0、Important 0，并记录两个不影响生产读者、数据、manifest或恢复行为的非阻断机械清理候选。第三路authority/范围审查确认阶段6没有提前实现阶段7多源压力、阶段8全量交付或shared canonicalizer/model范围。精确commit/push后按既有授权进入阶段7。

## 阶段7：多源/全源协调、停止与资源压力

### 任务7.0：冻结累计证据与实现边界

- 步骤7.0.1：建立需求1--6、23--26的累计证据矩阵，逐项引用阶段1--6已经通过的测试、命令、changelog与commit；阶段7完整collector新增证据固定为三个场景。
  - 验证：阶段7新增工作只包含三个完整collector场景、CLOB production actor与Gamma discovery/outer-supervisor各100-cycle，以及受控stop/deadline未接纳depth pending和跨日pre-admission staging的聚焦回归；不得重建manifest状态机、source recovery、artifact schema、公开测试endpoint、第二coordinator或组合笛卡尔积。
- 步骤7.0.2：冻结真实拓扑：四个producer owner为`polymarket`、`polymarket_rtds`、`binance_depth`、`binance_aggtrade`；六条外部I/O路径为Gamma HTTP、CLOB WebSocket、RTDS WebSocket、depth snapshot HTTP、depth WebSocket、aggTrade WebSocket；六个manifest counter仍对应既有六个artifact/source事实。
  - 验证：测试与诊断不把四个owner误称为四条transport，不新增第五owner、第七counter或transport episode字段。
- 步骤7.0.3：为完整collector loopback只允许增加最小私有、必要时仅`cfg(test)`存在的runtime source-spec/factory与operator-stop注入seam，把四个现有production source future接入同一个真实coordinator；生产入口继续只构造production specs。
  - 验证：不修改公开`LiveTrainingRuntimeConfig`、CLI或endpoint环境变量，不导出test API，不复制第二coordinator/source实现，也不绕过owner直接向coordinator注入合成事件。

### 任务7.1：组合退化场景

- 步骤7.1.1：复用阶段3--6分别覆盖Gamma HTTP、CLOB WebSocket、RTDS WebSocket、depth snapshot HTTP、depth WebSocket和aggTrade WebSocket的单路径长期静默/不可达、恢复与资源证据，不在完整collector中逐路重复六次。
  - 验证：累计证据证明退化路径的DNS/connect/read/HTTP/backoff不持有shared queue、enqueue lock或writer资源；阶段7只通过步骤7.1.2的代表性跨平台场景复核健康typed row、counter和最终Parquet在同一真实coordinator中推进。
- 步骤7.1.2：只运行一个代表性Polymarket＋Binance跨平台多源退化场景；固定退化CLOB WebSocket与depth snapshot HTTP，RTDS、Gamma/depth WebSocket及aggTrade保留真实健康或可观察路径。
  - 验证：availability等待不占有共享资源，健康producer在容量范围内产生typed row；共享queue真正饱和仍复用既有有界背压与terminal timeout组件证据，本阶段不新增跨source公平调度合同或第二多源组合。
- 步骤7.1.3：运行六条外部I/O全部配置不可达与流式业务全静默两个operator-stop场景。全不可达例只证明四个owner在全部外部能力不可用时仍受同一collector监督；Gamma或depth前置失败阻断的CLOB/snapshot请求不得冒充实际连接失败，逐路证据引用步骤7.1.1。全静默必须先由Gamma返回有效稳定市场、depth取得合法初始snapshot并完成CLOB subscription，再让四条WebSocket只维持协议存活而无合格业务事件。
  - 验证：全不可达时同PID持续source-local恢复并可由operator stop收敛；全静默允许初始metadata/snapshot已经产生typed row，只要求进入静默后counter不再伪增长并可由operator stop收敛。两者都不出现systemd restart假恢复，不在manifest生成transport scope/episode、`active|idle|degraded`新状态或通用last-qualifying字段。
- 步骤7.1.4：第三个且最后一个完整collector场景使用步骤7.1.2的跨平台多源退化，同时保留健康typed row，由run deadline触发shutdown segment finalize。三个完整collector场景合计覆盖两次operator stop、一次run deadline和真实segment finalize；UTC day rollover与hour finalize复用阶段1/6真实owner/component证据，不新增clock seam。
  - 验证：共同gate把operator stop/run deadline作为新admission截止；已取得对应raw-day permit并进入queue ownership的event在既有期限内drain/finalize，depth generation-local pending及未取得新日permit的跨日deferred staging可以清空且不生成row/gap/告警/`dropped_rows`/terminal。正常rollover只在新日activation后flush staging、分配admission并发布关联confirmed-gap副作用。累计复跑PendingExit/activation ack/rendezvous/unclean-dominates、depth hour fence及运行中snapshot unavailable/EOF中性release，证明受控截止没有扩大到普通运行边界。

### 任务7.2：中性产物与模型边界

- 步骤7.2.1：只检查步骤7.1指定的三个完整collector场景产生的真实产物，不重新实施阶段1 manifest矩阵。
  - 验证：全不可达时共同coverage仍为active且六counter保持真实零/null；跨平台退化时只有真实artifact counter推进并有健康row；全静默在初始metadata/snapshot后不再伪造行；两次operator stop和一次run deadline得到与阶段1合同一致的known/unknown closure与finalized segment。无额外receipt/sidecar、transport episode、collector silence timer、缺失条数或行情回填。
- 步骤7.2.2：复跑并引用阶段1、5、6既有manifest、state-aware recovery、legacy compatibility及artifact合同测试，确认阶段7diff没有改变schema或恢复语义。
  - 验证：每日仍只有顶层v3直接嵌套coverage v1及六个既有counter；`active|idle|degraded`不成为manifest状态；legacy aggTrade continuity只读、depth legacy/current矩阵和sticky bool语义不变，availability不进入`source_gaps`。
- 步骤7.2.3：检查范围。
  - 验证：没有collector business-silence timer、`DecisionWakeEvent`、data-pipeline/model策略代码、fixed decision tick、合成market row或price-level book；collector只保存因果事实、已批准aggTrade v6 evidence及depth snapshot v3/update v4的raw observation/collector admission/continuity-ready分层证据，raw invalid bytes/parser detail不进入模型。后续shared canonicalizer/Observation age/mask、depth价位簿重建和核心的确定性调度基线、离散`next_wake`目标候选由各自合同处理，模型学习暂停、撤单和BUY对侧风险中和。当前不建设精确transport episode artifact；只有未来出现经证明的cause-specific模型需求且现有事实不足时，才触发独立artifact/schema与ObservationContract决策门。

### 任务7.3：资源与完整性压力

- 步骤7.3.1：完成累计100-cycle证据矩阵：复用阶段3 SDK/RTDS、阶段5 aggTrade、阶段6 depth WebSocket reconnect与HTTP resnapshot证据；新增CLOB production market actor和Gamma discovery/outer-supervisor各100-cycle。不得另建任何第二恢复循环。
  - 验证：每条独立恢复路径累计至少100次后task、FD、socket、receiver、actor、queue、watch、refcount、depth buffer/snapshot future无单调增长；CLOB每代只replay latest desired且旧generation ack不能满足新代，Gamma按当前时钟重新发现而不保留过期slug；阶段5静态门继续证明aggTrade没有REST/backfill/pending/range-retry任务。
- 步骤7.3.2：复用阶段1、5、6已经冻结的aggTrade invalid、source-local parser/sequence terminal、shared writer/manifest terminal与共同gate证据，不再把availability退化、invalid burst、健康source和terminal拼成第四个完整collector场景。
  - 验证：普通aggTrade invalid逐条进入v6且collector不退出，真实terminal不会被availability循环吞掉；这些结论由既有component/owner fault矩阵提供，不在同一已fail-closed运行中继续注入不可观测fault。
- 步骤7.3.3：复用阶段5 invalid burst、writer暂停、oversize与阶段6 depth buffer、queue/writer ownership证据；只新增共同gate受控截止和真实depth pending的聚焦component回归，不新增cross-source shared-queue完整collector组合。
  - 验证：operator stop与已到点run deadline都拒绝新的unadmitted enqueue；CapacityLimit/terminal仍保持错误，截止前已取得对应raw-day permit并转移queue ownership的event继续drain。聚焦回归同时证明跨日deferred在新日permit前没有collector admission，正常rollover按staging顺序flush后才分配，受控PendingExit清空后queue无新row且admission sequence、gap、`dropped_rows`、terminal均不伪增长；snapshot pending update适用相同截止，普通snapshot unavailable、Close/EOF、reconnect、UTC rollover仍中性release。

阶段7完成条件：需求1--6、23--26由“前序阶段冻结证据＋阶段7新增差异证据”共同闭环。阶段7新增范围严格限定为三个指定完整collector场景、CLOB/Gamma各100-cycle及受控stop/deadline depth pending与跨日pre-admission staging聚焦回归；single-source、invalid、terminal、queue/writer、UTC rollover与hour finalize均复用累计组件/owner证据。不得新增第四个完整collector场景、public test endpoint、shadow coordinator、第二owner、manifest字段或artifact。自审和独立审查通过后精确commit/push。

阶段7执行结果（2026-07-18）：`COMPLETE`。默认集合中的完整collector严格只有三例：全部外部I/O不可达由operator stop收敛、protocol存活但业务静默由operator stop收敛、跨平台退化保留健康typed row并由run deadline触发segment finalize。CLOB production market actor与Gamma discovery/outer-supervisor各完成100-cycle；阶段3 RTDS、阶段5 aggTrade及阶段6 depth累计恢复证据继续复用，没有新增第二恢复循环或组合矩阵。受控cutoff聚焦回归同时覆盖depth generation-local pending、activation前PendingExit及activation commit后deferred flush期间的SameDay stop；跨日staging只有在取得新日permit与queue ownership后才分配admission，受控清空不生成row、gap、告警、`dropped_rows`、operational counter或terminal。admission关联gap按各event真实接收UTC日归档。

最终门禁通过：`cargo check -p data-collect --all-targets --locked`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo test -p data-collect --locked -- --test-threads=1`、`cargo fmt --all -- --check`及`git diff --check`均退出0；全量测试为254项lib及全部integration/doc tests。三路`gpt-5.6-sol/ultra`终审分别覆盖生产并发/所有权、测试最小性及文档authority；发现的post-activation cutoff、gap UTC日归属与旧绝对措辞均已修复并由原审查者复核，最终Critical 0、Important 0、Minor 0。阶段8已获授权但尚未执行。

## 阶段8：全量验证、追溯与本地交付

### 任务8.0：冻结authority与审查边界

- 步骤8.0.1：以`e1b368561779b7f67d006b0dabc8a40632267d44`为批准累计基线，以`f53946537b7c3089971cbb667556704fbe75d6b0`为阶段7最终HEAD；阶段8开发期间审查`f539465..working tree/index`，提交后审查`f539465..HEAD`。
  - 验证：累计实现范围固定为`e1b3685..f539465`；用户现有`AGENTS.md`与`推荐开发顺序.md`始终排除在本事项pathspec、stage和commit之外。
- 步骤8.0.2：按用户批准的`D8-R`同步需求验收19、depth current schema、阶段8测试顺序和D7-DEFER追溯边界。
  - 验证：当前日立即admission与跨日pre-admission staging不再冲突；current depth唯一写入schema明确为snapshot v3/update v4，snapshot v2/update v3只读兼容。

### 任务8.1：聚焦交叉验收与静态审计

- 步骤8.1.1：重新计算官方crate hash、四个批准vendor源码路径diff，并核对根`[patch.crates-io]`映射、SDK版本、批准features、vendor Cargo/lock及SDK dependency/feature图。冻结阶段3 vendor基线`e7d766dd7d7b3642b619746c8fb2a54857527e4a`、阶段3/current vendor tree`676ea8a0669166dafeee88c368bb20f42fc44c7d`与官方crate SHA-256 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`。只有这些身份与图自阶段3后均未变化时，才复用阶段3已记录的批准生产feature集合`clob,gamma,rtds,ws`、`--all-features`、integration/doc、Clippy与vendor fmt证据；任一变化都必须重跑受影响vendor矩阵，非批准变化先触发决策门。
  - 验证：需求4.7矩阵、源码路径allowlist和provenance一致，并在changelog明确记录复用或重跑依据；不冻结受Cargo展示版本影响的`cargo tree`文本hash。如需重跑，使用仓库外临时target或确认vendor树不存在`target/`残留。
- 步骤8.1.2：比较根/data-collect Cargo manifests、lockfile、Polymarket/Binance生产调用图及共同admission/deferred路径。
  - 验证：逐项解释Cargo/lock变化；没有新增Binance SDK、Barter、Nautilus或其他market-data engine。aggTrade REST/backfill/pending/merge/range-retry不得复活；legacy aggTrade continuity只能存在于读取兼容路径；current depth不得写snapshot v2/update v3，snapshot v3/update v4是唯一current写入schema，且不得出现第二WebSocket/snapshot owner、第三artifact或price-level book；`request_reconnect`及旧business-silence预算生产调用为零。Polymarket fanout与通用Binance发送路径只能经过共同unadmitted enqueue；`DeferredRawBatch`在最终flush前不得拥有raw-day permit或collector admission，只有取得对应permit与bounded queue ownership后才能冻结source admission；admission gap只随最终admission发布并按event `recv_time_utc_ms`归日。受控截止可中性清空pre-admission staging，已经admit的event必须drain/finalize；不得出现第二deferred owner、旁路admission stamp或绕过queue的发布路径。
- 步骤8.1.3：使用同一批可关联、已经接纳的source rows执行finalized typed raw与handoff编码交叉验收：aggTrade覆盖`trade + invalid_input`，depth覆盖`snapshot + update`。handoff必须经过`HandoffFrame::to_json_line()`及反序列化，再与finalized Parquet读取结果比较。
  - 验证：只比较双方共享字段。aggTrade覆盖record kind、source epoch、connection generation、WS ingress、collector admission sequence/time、receive time、host/region、source-native payload与invalid reason；depth覆盖record kind、source epoch、connection generation、depth ingress、collector admission sequence/time、receive time及update stitch verdict。Parquet专属审计列、depth book payload和其他非共享字段不进入比较；handoff `event_seq`不得冒充collector admission sequence。该步骤只证明同一已接纳source row的两种编码一致，不宣称存在或已经验证真实paper/live online source fanout、shared canonicalizer或完整runtime parity，不新增生产写入路径、完整collector、network loopback、absence frame或100-cycle。

### 任务8.2：需求追溯与独立终审

- 步骤8.2.1：逐条把需求第六节28项验收映射到测试名、命令、结果和必要artifact，写入changelog。
  - 验证：没有用“全量测试通过”代替逐项证据；D7-DEFER显式复用阶段7聚焦回归，证明depth generation-local pending及尚无新日permit的跨日staging在受控截止后没有admission、queue row、Parquet row、gap、告警、`dropped_rows`、operational counter或terminal，截止前已经admit的event继续drain。该absence边界不得通过伪造handoff frame或新增生产path验收。
- 步骤8.2.2：分别自审`e1b3685..f539465`累计实现和阶段8 working-tree diff，检查跨阶段组合、范围、孤立代码、旧测试/常量、authority、Cargo/vendor身份、D7-DEFER唯一owner路径和changelog；不重复逐行复审已经关闭的单阶段问题。
  - 验证：每一行改动可追溯本需求，没有遗留或复活旧silence budget、`request_reconnect`、aggTrade backfill、旧depth current writer、shadow owner、旁路admission或第二deferred owner。
- 步骤8.2.3：派生正确性终审和范围/资源/authority终审。
  - 验证：Critical/Important均为0；可执行反馈修复后只重复直接受影响的组件或合同测试。只有完整runtime fixture/path发生变化才重复对应完整collector；纯文档修复不强制重复100-cycle、完整collector或阶段7场景。所有代码/测试反馈修复结束后才进入任务8.3最终门禁。

### 任务8.3：最终门禁与本地开发交付

- 步骤8.3.1：在最终Rust树运行`cargo fmt --all -- --check`，并对`e1b3685..f539465`累计diff及使用本事项pathspec的阶段8 working-tree diff执行whitespace检查。
  - 验证：均退出0；用户无关dirty文件不影响本事项working-tree检查。
- 步骤8.3.2：运行`cargo clippy -p data-collect --locked --all-targets -- -D warnings`。
  - 验证：退出0且无warning。
- 步骤8.3.3：由于步骤8.1.3新增Rust合同测试，在全部代码/测试反馈修复完成后只运行一次`cargo test -p data-collect --locked -- --test-threads=1`；开发期间只运行新增或受影响的定向测试。
  - 验证：最终树全量通过，无ignored关键验收；不在补交叉测试前预跑全量，也不逐项重复默认集合中的完整collector或100-cycle。最终门禁后的纯文档结果记录不使该次最终Rust树证据失效。
- 步骤8.3.4：精确stage本事项文件并复核cached diff。
  - 验证：运行`git diff --cached --check`并复核cached文件清单；用户现有无关dirty文件不进入commit。
- 步骤8.3.5：创建commit并push当前批准分支。
  - 验证：changelog记录阶段0--7 commit映射、最终测试、审查、scope与阶段8 commit subject；push后在交付报告验证并记录本地HEAD、upstream、`origin/after-data-plan`及远端ref SHA一致，不要求commit在自身内容中记录自身SHA。

阶段8执行结果（2026-07-18）：`COMPLETE`。实施严格限于两项test-only finalized Parquet/handoff交叉合同、`D8-R`/`D8-STATUS-A`文档同步、SDK/Cargo/生产调用图静态身份审计及需求1--28证据追溯；没有生产Rust、Cargo、vendor或schema变更。三路独立终审均为Critical 0、Important 0、Minor 0。最终`cargo fmt --all -- --check`、累计/本事项`git diff --check`、data-collect all-targets Clippy `-D warnings`及串行全量测试均退出0；全量测试为254项lib与40个integration test binary共231项通过，无失败或ignored。阶段8 commit subject为`test(data-collect): complete source lifecycle phase 8`，push后SHA一致性在交付报告中记录。

阶段8完成只表示本地代码与文档闭环，不授权生产操作。

## 阶段9：RTDS固定PING与no-PONG合同修复

### 任务9.1：冻结真实wire事实、冲突与红灯

- 步骤9.1.1：保存香港SOCKS5h路线前后回执、15分钟RTDS wire统计、两次无PING空帧对照及四个SHA-256；明确官方文档只要求每5秒PING，没有PONG要求。
  - 验证：同一socket持续约900秒，181次PING的180个间隔为4999--5001ms，业务更新持续且文本/控制PONG与服务端Close均为0；空文本在无PING对照中同样出现，不能冒充PONG。
- 步骤9.1.2：同步主demand/plan/changelog及旧reliability demand/plan的当前authority指针，并先写RTDS no-PONG loopback红灯。
  - 验证：旧“RTDS no-PONG必须重连”和“5s/15s PING/PONG”只保留为历史事实；红灯因缺少`Config::require_text_pong`或旧socket被PONG timeout关闭而失败，不由测试server主动Close。

### 任务9.2：实施最小SDK配置与单pending状态机

- 步骤9.2.1：在既有`ws::config::Config`新增`require_text_pong: bool`且默认`true`；CLOB生产显式`true`并保持`10s/20s`，RTDS生产显式`false`并保持`5s` cadence，`15s`字段在false模式下不生效。
  - 验证：三种配置身份均有精确断言；不改Cargo/lock、SDK版本、parser/fatal语义或第五个vendor文件。
- 步骤9.2.2：在现有唯一connection owner与heartbeat task之间使用私有PING write ack；false模式每次只在实际`write.send(Text("PING"))`成功后确认，确认前不创建第二个请求，确认后`Interval::reset()`并等待完整周期。
  - 验证：阻塞write跨多个interval时channel不积压；解除后不会catch-up burst。true模式继续精确文本PONG、old/no-PONG timeout与recoverable重连，WebSocket控制Pong不冒充文本PONG。
- 步骤9.2.3：保持真实Close、EOF、read/write/PING-send failure recoverable，parser sticky fault terminal，last-manager drop normal且不重连。
  - 验证：现有反向分类矩阵与false模式PING write failure定向测试全部通过；没有第二owner、raw WebSocket或shadow SDK。

### 任务9.3：回归、资源与香港线路复测

- 步骤9.3.1：运行配置、vendor heartbeat、生产RTDS长期no-PONG、CLOB strict-PONG、自然Close/replay及RTDS 100-cycle定向测试。
  - 验证：RTDS跨至少三个旧timeout仍同socket/generation、一次subscribe并消费后续stale/fresh；100次真实Close后task/FD/actor/subscription不单调增长；CLOB no/old-PONG合同不变。
- 步骤9.3.2：完成workspace与vendored SDK适用全量测试、check/Clippy、root/vendor fmt、diff、四路径allowlist、Cargo/lock与官方crate provenance核验。
  - 验证：所有命令退出0；阶段8因vendor身份未变化而复用旧矩阵的条件已失效，本阶段必须重跑受影响vendor矩阵。
- 步骤9.3.3：通过香港VPN/SOCKS5h路线，以使用本次vendored SDK的本地测试程序运行15分钟，不新增生产代理路径。
  - 验证：真实WSS层证明零no-PONG重连、同一connection generation/revision且业务数据持续；由于SDK应用帧在外部SOCKS隧道处为TLS密文且公共runtime不暴露出站帧，修复后的精确PING计数/cadence由同一实现的loopback直接计数，服务端no-PONG合同由修复前香港raw-wire的181次PING/零PONG证据冻结，不把推算值冒充修复后wire观测。记录前后HK/HKG出口、服务状态、分层统计和SHA-256。

### 任务9.4：阶段审查与交付

- 步骤9.4.1：自审全部diff及需求验收7、9、11--13、25--28，再由`gpt-5.6-sol`/`ultra`独立审查生产状态机、测试/资源及文档authority。
  - 验证：Critical/Important均为0；可执行反馈修复后重复直接受影响验证和审查。
- 步骤9.4.2：同步最终命令/结果与范围，精确stage阶段9文件，创建commit并push当前分支。
  - 验证：cached diff/check与文件清单正确，本地HEAD、upstream和remote ref一致；不包含生产部署、远端服务或数据写操作。

阶段9完成条件：需求的RTDS固定PING/no-PONG、CLOB保持、真实transport分类、单pending资源与SDK-first范围全部有测试和香港线路证据，独立审查Critical/Important为0，commit/push完成。阶段9完成前不得把当前修订标记为complete。

阶段9执行结果（2026-07-19）：`COMPLETE`。SDK `Config`新增默认`true`的`require_text_pong`；CLOB生产显式`10s/20s/true`，RTDS生产显式`5s/15s/false`，其中RTDS `15s`仅为false模式不读取的兼容字段。false模式由唯一connection owner实际写入大写文本`PING`后通过私有oneshot确认，确认前最多一个pending，确认后重置完整interval；no-PONG不再触发RTDS重连，Close、EOF、read/write/PING-send failure仍recoverable，parser sticky fault与last-manager normal teardown保持不变。测试先行红灯退出101；候选实现后的生产no-PONG、固定cadence、CLOB strict-PONG、write failure、自然Close与100-cycle资源矩阵均通过。最终workspace locked全量、check、Clippy、root/vendor fmt与diff均退出0；vendor在Rust `+1.88`下core 170/170、all-features 624通过/20个doc示例ignored，default/all-features Clippy均退出0。香港线路修复后SDK复测持续`900001ms`、收到897条消息/896条Chainlink update，generation始终0、revision始终1且SOCKS隧道只有1个TCP连接；TLS证据边界没有把不可见的修复后PING数量冒充wire观测。官方crate SHA-256、四路径allowlist、Cargo/lock与无vendor target均复核通过；两路`gpt-5.6-sol`/`ultra`独立终审均为Critical 0、Important 0、Minor 0。阶段9精确commit/push完成后由交付报告记录SHA；本阶段没有执行release构建、部署、远端服务/数据写操作、数据清理或重采。

## 本计划之外的生产边界

本计划本身仍不授权生产release构建、产物复制、远端部署、服务启停、生产验收、回滚、数据清理或失败日期重采。用户已另行批准`demand/data-collect-training-collector-direct-deployment-demand.md`及对应计划，专门授权2026-07-18多日停服、旧PID/lease已退出、当前UTC日无任何collector产物这一首次正式冷启动：任意时刻原子安装并直接启用唯一`Restart=on-failure` unit，不使用`Restart=no`、独立gate或历史数据删除。

发现任何当前日产物必须原样保留并停止。该独立批准不表示当前代码具备无竞态same-day不同identity切换能力；future coverage-aware升级、回滚、identity migration或旧binary接管open v1日仍需基于当时状态另行批准。相同identity、持有dataset独占lease的systemd同日恢复属于需求4.2状态机，不得与部署takeover混淆。

## 2026-07-25：current SDK依赖身份说明

本计划阶段0--9中的0.6.0路径、provenance、命令和执行结果均是已完成阶段的历史事实，
不回写。其要求“采用更新release必须另建SDK版本升级决策门”已由独立
`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`满足；当前
Stage1 worktree使用官方0.7.0底座的唯一
`third_party/polymarket_client_sdk_v2-0.7.0-p1`，版本、Cargo/lock、路径与provenance
由SDK070三件套管理。需求第4.7节四路径patch语义、SDK-first、唯一owner/no-shadow
与heartbeat/teardown合同继续有效，不因身份迁移重新执行本计划阶段0--9。

SDK070 Stage1当前为`SDK_070_RUNTIME_USABLE`与
`SDK_070_TYPED_CONTRACT_BLOCKED`，独立审查已为
`Critical 0 / Important 0 / Minor 0 / PASS`且用户已另行批准发布；其闭合以承载
记录的commit成功推送且远端SHA与本地HEAD/upstream一致为生效条件。本说明不修改
本事项生产Rust/schema/RTDS行为，也不授权公开网络smoke、release、部署、远端操作
或数据清理。
