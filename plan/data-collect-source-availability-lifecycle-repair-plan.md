# 训练采集全数据源可用性与连接生命周期修复计划

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应需求：`demand/data-collect-source-availability-lifecycle-repair-demand.md`
>
> 对应 changelog：`changelog/data-collect-source-availability-lifecycle-repair-changelog.md`
>
> 状态：`APPROVED_IMPLEMENTATION_PHASE_2_COMPLETE_PHASE_3_AUTHORIZED`。用户已于2026-07-17批准本计划、对应需求及需求第4.7节SDK-first逐项patch矩阵，并授权依次执行阶段0--8的本地文档/代码修改、测试、独立审查、commit和push；同日对官方pre-parse完整文本帧trace差异选择B，批准恢复官方0.6.0原hunk及其failure-trace边界放宽。阶段0 authority/provenance、阶段1共同classification/coverage lifecycle及阶段2 vendored SDK transport生命周期均已实现、完成全量验证并分别通过三路独立终审；阶段3可在阶段2精确commit/push后开始。本授权不包括模型调度实现、生产release构建、部署、远端服务操作或数据清理。

## 一、成功标准与实施原则

实现完成必须直接满足需求第六节全部28项验收，并同时证明：

- 任意一个、多个或全部训练外部源transport不可达或业务流静默时，collector保持同一PID运行；全部transport不可达才记录有证据的degraded，协议存活但业务静默保持active/idle且不虚构状态转换；
- 业务静默不再控制subscription或transport，不制造gap、lag、异常告警或退出；
- transport故障在各自source-local owner内持续恢复，总attempt和总elapsed不构成存活预算；
- RTDS、CLOB、Binance aggTrade、Binance depth/snapshot和Gamma保留各自协议语义；
- typed raw接收时间、真实空档和last qualifying time可供后续因果派生missingness；daily manifest只新增单一collector observation-eligibility coverage并保持既有source counters，不保存transport scope/reason/episode；data-collect不决定模型age/mask、确定性调度基线、离散`next_wake`目标候选或任何decision tick；
- parser/schema、本地lag/drop、sequence hole、writer/manifest和内部不变量仍按既有scope quarantine或fail closed；
- vendored SDK只保留经逐项证明仍必要的最小差异，删除旧业务静默策略专用的one-shot reconnect；
- 没有第二套health、reconnect、socket owner、SDK session、receiver或subscription actor。

用户已于2026-07-17完成需求、计划及SDK patch矩阵的最终批准。任何真实代码、现行已批准文档或官方协议与本计划发生新冲突时，必须停止相关阶段，在changelog登记事实、影响和选项并请求用户决定。

每个阶段均执行：

1. 先写能复现旧错误或冻结保持行为的测试；
2. 完成最小实现；
3. 逐条对照本阶段需求和验收自审；
4. 使用`gpt-5.6-sol`、Ultra Reasoning Level派生独立审查；
5. 修复可执行问题并重复测试/审查；
6. 审查通过后只stage本阶段相关文件，commit并push，再进入下一阶段。

不得把失败测试单独push到共享分支；不得混入现有`AGENTS.md`、`demand/rebuild-core-demand.md`或`推荐开发顺序.md`的无关改动。

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
| `M8-A` | UTC日边界部署 | 只冻结未来部署策略；首次启用与回滚使用不同边界流程，same-day binary/version/identity takeover不在本事项内；相同identity加独占lease的同日恢复属于M4状态机，当前代码不得冒充已具备无竞态切换能力 |

固定生命周期合同：当前四个logical producer、六个writer及coordinator-owned bounded event queue/唯一receiver/ingress路径全部mandatory；任一producer/writer terminal，或queue/receiver/ingress路径失效，都会使共同collector coverage失效并结束collector。任何current-day读取/恢复前先取得并持续持有dataset-root级OS-backed独占实例lease；相同identity不能替代旧进程退出证明。coordinator再完成queue/receiver/ingress前置条件，全部writer/producer wrapper spawn、ready并阻塞共同gate，随后才可发送一次不重试的activation mutation；owner原子写`active`并ack后释放网络poll。未来出现optional producer、独立暂停/重启或任一mandatory terminal后collector继续运行时，必须升级嵌套schema并重新批准。

### 任务0.2：消除旧规范冲突

- 步骤0.2.1：按语义更新`demand/data-collect-demand.md`，删除或改写“空结果/业务静默/stale/断线一律source gap”“每个source总attempt/max elapsed必须耗尽”和“非空外部availability必然令结构完整artifact失败”的旧合同；同步daily manifest允许字段清单，只新增顶层直接嵌套、版本化`collection_day_coverage`。
  - 验证：保留confirmed sequence/local drop/parser/writer等真实gap；明确complete artifact不等于外部coverage 100%；旧manifest“只记录”字段清单包含需求4.2的全部字段，六个既有source counter不承载coverage，且仍只有每日唯一manifest、无receipt/sidecar/transport episode。
- 步骤0.2.2：同步`demand/data-collect-reliability-repair-demand.md`与`plan/data-collect-reliability-repair-plan.md`中的RTDS D2、CLOB silence budget、Binance silence分类、有限总恢复、旧vendor patch白名单，以及“daily manifest只记录”的字段清单与coverage验证步骤。
  - 验证：语义检索`60秒无fresh|两次无fresh|第三次.*重启|business_message_timeout|有限attempt|max_elapsed|六项|request_reconnect`不再存在互斥当前合同；reliability demand/plan都显式采用需求4.2同一顶层v3/嵌套v1、restart/cross-day和UTC边界部署语义，并仍禁止receipt/sidecar/逐事件transport artifact；旧changelog保持原文。
- 步骤0.2.3：复核并保持本轮已完成的`demand/rebuild-core-demand.md`语义同步：训练采集退化按全部训练外部源的source-local协议恢复，daily manifest只新增单一collector observation-eligibility coverage且保持既有source counters，typed raw空档支持missingness派生；当前业务不需要精确transport episode artifact且没有默认建设计划。保留BUY-only对侧风险中和语义，并只把用户已批准的离散`next_wake`写成核心目标候选及独立`DecisionScheduleContract`，不得把调度实现扩入collector计划；若实施前core发生漂移，只修复与本事项直接冲突的条款。
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
  - 验证：六个source counter无coverage复制；全部mandatory成员ready但零行时为active，未达到共同起点为not-started；current-day `not_started|active|unclean_end_unknown|ended_known -> active`逐项断言first/last/end与布尔值，证明首次为`false`、三种同日后续activation均为`true`，unclean/known-end/reconcile不清除该值，时钟倒退失败。current top-level closed、identity mismatch、同identity缺对象与非法nested均拒绝；prior-day四态/legacy按state-aware规则关闭。`reactivated_within_day=true`只隔离last start前无法证明连续coverage的历史replay区间，不生成要求parity的Observation且不倒灌unknown mask；unclean共同尾界仅为last start。兼容矩阵另覆盖unknown nested version/field、错误布尔类型、越界时间及每种非法状态。若需要producer独立生命周期、run segments、新artifact、typed raw变化或额外consumer职责，停止阶段并提交新schema决策。
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

## 阶段3：原子删除RTDS主动恢复并收缩SDK

### 任务3.1：冻结RTDS目标测试

- 步骤3.1.1：server持续PONG但长期不发业务frame，随后在同一socket发送stale和fresh payload。
  - 验证：修复前以主动generation切换/退出或lag副作用失败；目标为一次subscribe、generation不变、payload正常落盘。
- 步骤3.1.2：初始连续连接失败超过旧3次/20秒，再启动server；另测运行中close后失败超过旧预算再恢复。
  - 验证：source不提前返回，每个成功generation只replay一次完整`btc/usd` desired。
- 步骤3.1.3：保留SDK `Lagged(n)`、parser/schema、actor和writer terminal负例。
  - 验证：这些错误不进入无限transport重试。
- 步骤3.1.4：先把依赖one-shot API但实际验证自然generation隔离、资源、select公平性、每代replay和freshness fence的测试改用server Close/no-PONG驱动。
  - 验证：只删除ticket专属断言，不丢失自然reconnect回归覆盖。

### 任务3.2：同一阶段原子删除旧消费者与SDK能力

- 步骤3.2.1：保留`5s/15s`和有界max backoff，把transport总attempts恢复为无限；删除20秒connected和3秒applied存活门。
  - 验证：未连接时desired留在watch，stop/deadline仍可中断。
- 步骤3.2.2：删除RTDS freshness episode常量、状态、`request_reconnect`生产调用、两generation预算、business-timeout gap/lag/alert/fatal分支。
  - 验证：source loop只持续receive、处理stop/deadline和真实terminal error，无等待永远不会发生的主动generation，也无tight loop。
- 步骤3.2.3：在同一未提交工作阶段删除`RtdsSdkRuntime::request_reconnect` wrapper、vendored ticket/API与ticket专属测试。
  - 验证：对`data-collect/src/**/*.rs`、`data-collect/tests/**/*.rs`及vendored `src/**/*.rs`做符号定义/调用检索为0；需求、计划和历史changelog允许保留文字证据。
- 步骤3.2.4：保留fresh/stale typed分类、dedupe、raw payload、generation isolation和每代一次actor replay。
  - 验证：stale不控制网络或lag，`Lagged(n)`仍明确失败，自然reconnect回归全部通过。
- 步骤3.2.5：保留RTDS source-local transport/subscription generation事实与last qualifying fresh time，不新增RTDS silence timer，也不写入manifest transport scope/reason/episode。
  - 验证：generation advance只驱动自然reconnect/replay并可形成有界operator诊断，不进入`source_gaps`；业务missingness只由后续`ObservationContract`根据last qualifying time、known/unknown coverage与当前逻辑求值时间共同因果派生，collector不实现mask或调度。

阶段3完成条件：需求验收7--10及共同availability断言通过；旧“两代无fresh必须退出”测试改为长期静默不退出，one-shot消费者与SDK能力在同一commit中消失。独立审查通过后commit/push。

## 阶段4：CLOB market与Gamma metadata

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

## 阶段5：Binance aggTrade

### 任务5.1：冻结静默、transport与continuity测试

- 步骤5.1.1：server持续PING但跨多个旧60秒等价周期不发aggTrade，随后同socket发送合法事件。
  - 验证：修复前产生business timeout、lag和gap；目标为每个PING重置30秒全frame inactivity边界、socket保持且事件正常消费。
- 步骤5.1.2：对照完全无任何frame超过加速后的30秒边界，并分别注入connect error、close、EOF、server shutdown和长期恢复。
  - 验证：只有全frame inactivity/真实transport故障触发中性reconnect，同PID持续恢复且total cycles不结束source。
- 步骤5.1.3：注入aggregate-trade-id hole、payload conflict、REST backfill网络失败耗尽和非法schema。
  - 验证：单range backfill有界，耗尽后记录unresolved、释放pending、开启新epoch并继续未来live事件；新洞仍可启动新backfill，完整性错误不被availability循环吞掉。

### 任务5.2：删除业务deadline网络动作

- 步骤5.2.1：删除`business_message_deadline`对socket生命周期的控制，以及`business_message_timeout`对应lag/gap/reconnect。
  - 验证：改为复用depth同一30秒全frame inactivity transport边界；任意合法frame重新武装，不从无成交推断连接坏。
- 步骤5.2.2：保留真实close/error/server shutdown恢复、PING/PONG和ID/backfill coordinator。
  - 验证：网络恢复使用既有source-local loop，无第二supervisor或并发backfill；旧range耗尽按既有unresolved/new-epoch合同结束，不永久追逐。
- 步骤5.2.3：保留aggTrade source-local transport/recovery事实与last qualifying aggTrade receive time，不新增无成交silence timer或manifest transport scope。
  - 验证：合法frame只影响transport liveness，合法aggTrade更新业务last qualifying事实；无成交不进入`source_gaps`且不产生collector silence-start事件，confirmed ID hole/unresolved range仍进入continuity evidence。

阶段5完成条件：需求验收18--19通过，独立审查确认没有削弱ID continuity或制造busy loop；通过后commit/push。

## 阶段6：Binance depth与snapshot

### 任务6.1：冻结transport和book continuity测试

- 步骤6.1.1：持续发送PING但不发depth update；另一个场景完全不发送任何frame。
  - 验证：前者不重连，后者按transport inactivity重连并重新snapshot。
- 步骤6.1.2：分别注入`u <= snapshot lastUpdateId`、`u <= local_u`、`U > local_u + 1`、无法解析`U/u`、`U > u`和其他非法schema。
  - 验证：前两类只drop stale/duplicate且不记gap；confirmed hole重建book；结构/schema/本地book不变量错误不继续发布。
- 步骤6.1.3：snapshot HTTP timeout/rate limit超过旧单cycle预算后再恢复。
  - 验证：source持续运行，外层重试可停止、无tight loop。

### 任务6.2：修正分类而不破坏恢复

- 步骤6.2.1：把完全无入站frame与有控制frame但无业务update分开，transport failure只写中性availability/reconnect evidence。
  - 验证：单纯transport断开不进入`source_gaps`或业务lag。
- 步骤6.2.2：保留每个真实连接的REST snapshot、stitch gate和confirmed sequence-gap resnapshot。
  - 验证：stale/duplicate旧帧可安全drop；没有在无snapshot/不连续book状态下发送depth事件。
- 步骤6.2.3：统一snapshot外部availability与schema/invariant terminal边界。
  - 验证：transport evidence只更新source-local transport recovery state，合法snapshot+stitch才更新book continuity state；两者都不持久化为manifest scope。网络可无限恢复，损坏payload按既有受影响范围失败。

阶段6完成条件：需求验收20--22通过；独立审查重点核对Binance官方snapshot/update-id算法未被“允许缺失”错误弱化。通过后commit/push。

## 阶段7：多源/全源协调、停止与资源压力

### 任务7.1：组合退化场景

- 步骤7.1.1：依次运行每个source单独静默、单独不可达场景。
  - 验证：同PID、健康source继续写盘、退化source不返回。
- 步骤7.1.2：同时退化RTDS+CLOB、aggTrade+depth以及至少一个Polymarket+一个Binance组合。
  - 验证：source互不阻塞，coordinator和writer继续推进。
- 步骤7.1.3：分别运行四个逻辑source全部transport不可达，以及四个transport按协议存活但全部业务流静默两种场景。
  - 验证：前者同PID持续source-local恢复并有去重operator证据，但manifest不生成transport scope/episode；后者同PID保持active/idle，只保留active collection coverage、nullable last-qualifying和真实source counter事实，不创建虚假degraded/silence事件；两者都不出现systemd restart假恢复。
- 步骤7.1.4：在上述每种状态触发operator stop/deadline/day finalize。
  - 验证：在既有shutdown期限内drain/finalize，无永久等待或丢弃已接受事件。

### 任务7.2：中性产物与模型边界

- 步骤7.2.1：检查每日唯一manifest的顶层`collection_day_coverage`、六个职责不变的source counters，以及typed raw接收时间和last qualifying time保留的真实空档。
  - 验证：全mandatory set零行active、never-started、known end与unclean unknown end可区分；正常rollover必须经owner ack按day boundary衔接。同identity同日恢复逐项覆盖`not_started|active|unclean_end_unknown|ended_known -> active`，保留first、以真实start更新last，首次activation令`reactivated_within_day=false`、任一同日后续activation令其为`true`且保持单调；current顶层始终open，closed拒绝重开。`reactivated_within_day=true`时，last start前无法证明连续coverage的区间只降低finalized replay eligibility，不生成要求四runtime parity的历史Observation，也不倒灌unknown mask；typed raw仍保留。任何unclean共同coverage最后可证明边界只能是`last_started_utc_ms`；一个producer/writer突然结束而另一源随后产生的行不得延长共同coverage。无额外receipt/sidecar、transport episode、collector silence timer、虚构silence-start/end、缺失条数或行情回填。
- 步骤7.2.2：检查manifest完整性、state-aware recovery与兼容语义。
  - 验证：prior legacy缺coverage始终可读、不插入对象且coverage unknown；相同新identity的current manifest缺对象按损坏失败，不同identity current legacy仍由identity gate拒绝；缺失bool、错误布尔类型、残留任一旧计数字段、unknown nested version/field、越界时间与状态组合失败。finalized merge、operational counters、continuity、gaps、barrier state、close和startup reconcile均保留嵌套v1与sticky bool；prior active/unclean才有unclean closure/gap，prior not-started/ended-known/legacy missing使用需求4.2中性closure且无假gap；availability不进入`source_gaps`，artifact结构/schema/provenance完整时可完成，即使coverage有缺失。
- 步骤7.2.3：检查范围。
  - 验证：没有collector business-silence timer、`DecisionWakeEvent`、data-pipeline/model策略代码、fixed decision tick或合成market row；文档明确collector只保存因果事实，后续Observation age/mask和核心的确定性调度基线、离散`next_wake`目标候选由各自合同处理，模型学习暂停、撤单和BUY对侧风险中和。当前不建设精确transport episode artifact；只有未来出现经证明的cause-specific模型需求且现有事实不足时，才触发独立artifact/schema与ObservationContract决策门。

### 任务7.3：资源与完整性压力

- 步骤7.3.1：每个协议至少100次connect/failure/recovery或snapshot/backfill cycle。
  - 验证：task、FD、socket、receiver、actor、queue、watch、refcount和pending任务无单调增长。
- 步骤7.3.2：在组合availability场景中注入parser/schema、SDK lag、queue drop、writer/manifest和内部不变量错误。
  - 验证：正确scope仍quarantine/fail closed，顶层发现terminal task。

阶段7完成条件：需求验收1--6、23--26通过；生成可重复的进程级与资源证据。独立审查通过后commit/push。

## 阶段8：全量验证、追溯与本地交付

### 任务8.1：静态和全量门禁

- 步骤8.1.1：运行`cargo fmt --all -- --check`与`git diff --check`。
  - 验证：均退出0。
- 步骤8.1.2：运行`cargo clippy -p data-collect --locked --all-targets -- -D warnings`。
  - 验证：退出0且无warning。
- 步骤8.1.3：运行全部定向loopback/contract测试与`cargo test -p data-collect --locked`。
  - 验证：全部通过，无ignored关键验收。
- 步骤8.1.4：运行vendored SDK适用的default/all-features测试与lint，并重新比较vendor差异。
  - 验证：需求4.7矩阵、源码路径allowlist和provenance一致。

### 任务8.2：需求追溯与独立终审

- 步骤8.2.1：逐条把需求第六节28项验收映射到测试名、命令、结果和必要artifact，写入changelog。
  - 验证：没有用“全量测试通过”代替逐项证据。
- 步骤8.2.2：自审最终diff，检查范围、孤立代码、旧测试/常量、authority和changelog。
  - 验证：每一行改动可追溯本需求，没有遗留旧silence budget或`request_reconnect`。
- 步骤8.2.3：派生正确性终审和范围/资源终审。
  - 验证：Critical/Important均为0；可执行反馈修复后重复阶段7--8。

### 任务8.3：本地开发交付

- 步骤8.3.1：精确stage本事项文件并复核cached diff。
  - 验证：用户现有无关dirty文件不进入commit。
- 步骤8.3.2：创建commit并push当前批准分支。
  - 验证：本地HEAD、upstream和remote SHA一致，changelog记录commit与测试证据。

阶段8完成只表示本地代码与文档闭环，不授权生产操作。

## 本计划之外的生产边界

生产release构建、产物复制、远端部署、服务启停、生产验收、回滚、数据清理和失败日期重采均不属于本需求或本计划。阶段8完成后，如用户希望进入生产，必须另行明确批准对应需求/计划及精确操作范围；不得把本计划的本地完成或push解释为生产授权。

用户已经通过`M8-A`冻结未来部署兼容策略，但当前代码和本地实施计划不得声称已经具备无竞态生产切换能力：

- 首次启用：未来部署编排必须在UTC边界前停止legacy service，确认旧PID退出且未创建新日manifest；下一UTC日才启动v1 binary，旧日coverage保持legacy unknown。
- coverage-aware升级或回滚：v1-aware出站路径必须先保存`ended_known`，并通过另行批准、实现和验证的`stop-at-boundary/no-new-day-activation`或等价编排按state-aware规则关闭coverage-bearing顶层manifest，确认未创建新日manifest后，下一UTC日才可启动入站binary。
- same-day binary/version/identity takeover、identity migration和旧binary接管仍open的v1日均不在本事项内；相同identity、持有dataset独占lease的systemd同日恢复属于需求4.2状态机，不得与部署takeover混淆。“compatible”至少表示能读取并保留closed manifest中的嵌套v1。未来生产计划不得临场绕过这些边界。
