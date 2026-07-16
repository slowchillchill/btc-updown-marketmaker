# 训练采集全数据源可用性与连接生命周期修复 Changelog

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应需求：`demand/data-collect-source-availability-lifecycle-repair-demand.md`
>
> 对应计划：`plan/data-collect-source-availability-lifecycle-repair-plan.md`
>
> 状态：`DRAFT_AWAITING_FINAL_USER_REVIEW`。用户已于2026-07-17批准manifest/coverage决策并授权同步需求、计划与本文；当前仍只有文档修改，没有本事项代码实现、vendor patch、生产构建、部署、远端服务写操作或数据清理。

## 2026-07-16：建立前置回滚检查点

- 按用户要求，已把先前完成的CLOB/RTDS分源heartbeat最小修复精确提交为`fbee270d96886ae948de65474c122bcee3ea73b6`，commit subject为`fix(data-collect): split RTDS heartbeat configuration`。
- 已push到`origin/after-data-plan`；该检查点只包含原事项相关路径。
- `AGENTS.md`、整体审核中的`demand/rebuild-core-demand.md`和未跟踪`推荐开发顺序.md`未进入该commit，现有工作树内容保留。
- 提交前`git diff --check`、`cargo fmt --all -- --check`、`cargo test -p data-collect --locked`和`cargo clippy -p data-collect --locked --all-targets -- -D warnings`均通过；data-collect lib为133/133。

## 2026-07-16：复审RTDS refresh/reconnect与进程重启

- 官方RTDS客户端指引允许不断线修改subscription，规定每5秒PING，并说明connection error会触发automatic reconnection attempts；当前Rust SDK connection loop提供具体实现。官方没有把业务静默定义为connection error，也没有重复subscribe确认或3秒/6秒恢复SLA。
- 当前代码不是“每3秒refresh、每6秒reconnect”：真实行为是60秒无fresh后主动请求generation reconnect，每个新generation观察3秒、最多两个新generation、episode总期限12秒，耗尽后RTDS source与collector退出。
- 进程重启不是重新订阅或重连的必要机制；新PID来自错误传播到整个collector后由systemd重新启动。
- 已确认现有唯一RTDS actor会在每个真实新generation创建空subscription manager并只replay一次latest complete filtered desired；业务静默不需要same-socket refresh或主动generation churn。
- 同时确认pinned SDK的heartbeat timeout/旧PONG只结束heartbeat子任务，连接owner没有消费该结果；仅删除业务层主动重连仍不足，需要SDK-first最小修复。

## 2026-07-16：复审vendored SDK是否应整体恢复官方原版

- 审计结论是“不能整体恢复官方原版，但应删除一项旧业务策略专用补丁”。
- 现有vendor差异中，parser sticky fault、CLOB array all-or-error、`SimpleParser::Default`、connection-generation隔离和RTDS有界诊断均有独立数据完整性/API/自然重连依据，不由错误业务静默策略产生。
- generation-aware one-shot `request_reconnect()`直接服务旧“60秒无fresh后强制换连接”策略；新需求下失去生产依据，应删除vendor公共能力、runtime wrapper、生产调用和专属测试。
- 官方当前实现仍存在parser错误后继续、CLOB数组跳过损坏元素、高层stream吞`Lagged(n)`和heartbeat结果未可靠传播等缺口。官方Issue #39/PR #40是相关的高层Client Arc生命周期证据，但不等同于本项目已确认的低层last-manager-sender关闭后active handler不退出；两者不得混称。
- 新SDK决策门已从“六项扩为八项”改为逐项语义矩阵：本事项保持pinned 0.6.0，保留五类独立能力、删除one-shot reconnect，对heartbeat-dead与manager-drop审查并最小回移官方hunk/设计或使用等价最小patch。升级官方release必须另建版本升级决策门；补丁数量不再是authority。

## 2026-07-16：审计全部训练外部源

- 训练collector真实包含四个逻辑source task：Polymarket market/metadata、RTDS、Binance depth/snapshot、Binance aggTrade；当前没有CLOB user source。
- CLOB market存在与RTDS同类的确定性缺陷：单asset首条30秒、后续60秒静默后unsubscribe/subscribe，只允许两次，第三次返回普通错误；现有测试明确要求第三次迫使process restart。
- Binance aggTrade通常不会因恢复预算最终停机，但60秒无成交会主动断线、增加lag并写gap；收到PING也不重置business deadline。这是相同的“业务静默错误控制网络”，应修正。
- Binance depth外层恢复和snapshot重建基本正确。持续收到PING但无depth update不能视为坏连接；完全无入站frame可以触发transport recovery；confirmed update-id hole仍必须记录并重新snapshot。
- Gamma metadata网络失败通常持续恢复，但外部不可达/空候选与confirmed gap的分类仍需统一。
- 顶层source task提前结束即collector失败的保护应保留；正确修复是recoverable availability不从source返回，而不是顶层吞掉所有错误。
- 现有`health.rs`抽象测试不能证明生产修复：它未完整接入`live_runtime`，并仍包含`CircuitState::Exhausted`与有限`max_attempts/max_elapsed`旧合同。

## 2026-07-16：用户批准扩大文档范围

- 用户明确同意按上述复审结论修改完善需求与计划。
- 该决定批准的是文档改写范围，不等于批准最终文档或开始实现。
- 原尚未提交的`DC-REPAIR-20260716-RTDS-LIFECYCLE`草案在获批前被统一事项取代，不保留两个并行authority入口。
- 新事项编号为`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`，三份文档路径统一改为：
  - `demand/data-collect-source-availability-lifecycle-repair-demand.md`
  - `plan/data-collect-source-availability-lifecycle-repair-plan.md`
  - `changelog/data-collect-source-availability-lifecycle-repair-changelog.md`

## 2026-07-16：创建全数据源待审查需求与计划

- 新需求冻结共同结果合同：任意一个、多个或全部外部源退化时保持同一PID；业务静默不控制subscription/transport，不生成gap、lag、异常告警或退出；transport总恢复次数/时长不构成source/collector存活预算。
- 新需求分别冻结RTDS、CLOB/Gamma、Binance aggTrade和Binance depth/snapshot的协议边界，明确“共享结果合同”不等于“复制RTDS恢复实现”。
- availability/missingness/recovery使用每日唯一manifest中的有界逐source/scope摘要；不得新增receipt/sidecar、周期重复、伪造行情或把外部coverage不足当作结构artifact失败。confirmed SDK/local lag、queue drop、sequence/ID hole和完整性错误继续保留。
- 模型学习边界被明确保留：后续模型或bundle状态机决定暂停、`MODEL_NO_SUBMIT`、取消未成交BUY，以及BUY对侧outcome中和已有方向风险；collector/data-pipeline不替模型生成策略。
- 新计划按最终批准/authority同步、共同classification与测试seam、SDK transport修复、RTDS与one-shot原子删除、CLOB/Gamma、aggTrade、depth/snapshot、多源/全源压力和本地全量终审组织；生产构建、部署与远端操作明确不在本计划内。
- 每个实施阶段都要求先测、最小实现、自审、`gpt-5.6-sol` Ultra独立审查、精确commit/push；当前没有执行这些实施步骤。

## 2026-07-16：根据独立审查消除不可执行条款

- 纯业务静默没有协议事件或官方cadence，collector不再生成silence timer/episode；只保存last qualifying receive time与coverage边界，后续data-pipeline/Observation用另行批准的版本化阈值派生一次性模型可见missingness转换。
- 有显式证据的availability episode允许永久或跨日保持open；只有同scope协议正向证据才能关闭，不得在day/run/stop边界伪造recovery。
- availability状态增加`availability_scope + source_identifier`，冻结transport、subscription、RTDS fresh、CLOB asset、Gamma window/candidate、aggTrade和depth book各自的恢复证据；每日唯一manifest只保存最多16个active/open scopes与饱和聚合计数，不保存无界episode列表。
- 独立审查确认上述有界manifest无法还原同一scope多次closed episode的完整顺序/start/end。草案明确选择最小边界：manifest只作为current/open与聚合运行诊断，训练从typed raw接收时间、last qualifying time和真实空档派生missingness；不得把摘要冒充episode tape。若未来模型必须区分每次真实transport原因与精确恢复边界，另建typed availability artifact/schema决策门。
- 修正raw WebSocket范围：Polymarket继续禁止raw/SDK旁路；Binance保留现有唯一`tokio_tungstenite` owner，但禁止第二raw-WS实现或第二socket owner。
- aggTrade改为任意合法frame重置30秒全frame inactivity transport边界；有PING无trade不重连，完全无frame才重连。单backfill range保持有界，耗尽后记录unresolved、开启新epoch并继续未来live数据。
- depth明确三分支：`u <= snapshot/local_u`只drop stale/duplicate；`U > local_u + 1`记录confirmed gap并resnapshot；无法解析`U/u`、`U > u`或本地book不变量才按冻结scope失败。
- SDK阶段边界改为：阶段2只修heartbeat与sender normal teardown并暂留one-shot API；阶段3在同一未提交工作阶段先删除RTDS生产consumer，再删除wrapper/vendor ticket并把自然generation测试改用Close/no-PONG驱动，避免不可运行中间commit。
- sender关闭与transport退出被明确分离：只有last outbound sender关闭是normal shutdown；EOF/Close/read/write/PING/heartbeat仍recoverable，parser sticky fault仍terminal。
- `request_reconnect`零匹配只检查Rust生产/测试/vendor源码的符号定义与调用，需求和历史changelog允许保留决策文字。
- 删除原计划中的生产阶段；生产构建、部署、服务操作、回滚与数据清理必须另行批准对应需求/计划。
- authority同步门已补充旧daily manifest“只记录”字段清单及core episode粒度：旧reliability demand/plan必须加入版本化`source_availability`诊断摘要，同时core不得再把该摘要描述为完整逐episode历史，也不得降低模型学习真实/模拟missingness的目标。
- 治理终审发现仅靠逐source首末业务接收时间无法区分“source已运行但全程零行”和“source从未运行”。草案因此在每日唯一manifest内冻结顶层`collector_day_coverage`及六个既有source summary的`source_day_coverage`：只记录封闭状态、首/末start、nullable known end及饱和activation/unclean计数；正常跨日按day boundary衔接，已观测terminal return也可记录known end，manifest owner失败或进程突然消失的unclean end保持unknown/null，同日restart不覆盖first start。该最小coverage envelope不新增sidecar，也不声称还原完整run episode tape。
- 同一终审指出需求中的authority同步清单漏列manifest字段与core episode粒度。需求第五节现已与计划0.2完全对齐：旧data-collect/reliability demand/plan必须同步`source_availability`和day coverage字段及其restart/cross-day语义；core只可把它们作为有界诊断/envelope，精确transport/run episode仍需后续typed artifact/schema决策门。
- 全数据源复审进一步发现，同UTC日unclean restart后用首start到最终end作为连续coverage会把collector downtime误标为业务静默。草案已冻结保守mask：`unclean_recovery_count > 0`时，最后一次start之前所有无行业务空档一律unknown/not-observed；只有最后一次start后的连续known-active区间可参与observed-silence推导。跨日unclean从既有last start、last raw receive与last availability transition的非null最大值之后保持unknown，不为获得精确episode而新增sidecar或无界run列表。

## 截至2026-07-16待用户审查的关键决定

1. 是否批准需求中的全source共同结果合同和四组源特定边界；
2. 是否批准第4.2节每日唯一manifest中的有界`source_availability`字段集、16项active/open scope hard bound、collector/六source day coverage envelope、这些摘要不承诺完整episode tape，以及“外部coverage不足不等于结构artifact失败”的语义；
3. 是否批准第4.7节逐项SDK patch矩阵，尤其删除one-shot `request_reconnect()`；
4. 是否批准在实施阶段0精确同步旧data-collect demand/reliability plan和core草案中的冲突语义；
5. 是否批准需求第六节28项验收与计划分阶段实施顺序。

以上是当时的待决状态；后续已批准或仍待决的范围以本文较晚日期记录为准，且在最终文档与剩余SDK矩阵获批前不开始代码实现。

## 2026-07-16：用户批准离散 next_wake 目标候选及文档修订

- 用户明确批准把有界离散`next_wake`作为核心模型框架竞赛的目标候选，并批准据此修改核心需求与本事项文档；该批准不表示该候选已经成为champion、active模型，也不授权实现或部署模型调度。
- 核心需求现把collector availability事实、模型可见Observation与decision scheduler分开：本地时间表示精度不产生固定频率调用；稀疏真实事件与上一轮调度产生的一次性`DecisionWakeEvent`才形成决策机会；确定性单次age-crossing scheduler保留为同合同基线。
- 离散目标候选使用独立`decision_schedule_intent`/`next_wake_bucket`，要求单scope至多一个pending wake、真实事件抢占、稳定排序、timer generation、anti-Zeno、min/max bucket、restart parity和审计；训练与simulator按变量持续时间/SMDP、抢占/删失及决策资源约束处理。它不得生成BUY、撤单、暂停或BUY对侧风险中和等交易/风控动作。
- 核心需求明确禁止按微秒或其他固定网格物化“时间+空市场+空ledger”上下文；training/offline-evaluation使用确定性虚拟逻辑时钟，paper/live使用monotonic调度并记录lateness，四者运行同一版本化调度状态机。
- 本collector修复只负责长期运行、source-local协议恢复、typed raw时间/真实空档、last qualifying time、known/unknown day coverage及有界运行诊断；不决定或实现age/mask、`DecisionWakeEvent`、确定性调度基线、离散`next_wake`或模型策略。
- 本节取代前文“data-pipeline用一个版本化阈值派生一次性missingness转换”和“精确episode默认留待建设”的旧草案表述：age/mask由版本化`ObservationContract`在实际求值时因果计算，`DecisionScheduleContract`只安排稀疏求值；当前不默认建设精确episode artifact。
- 当前业务需求不需要精确transport episode artifact，也没有默认建设计划。只有未来出现经证明的cause-specific模型需求且现有因果事实不足时，才可新建typed availability artifact/schema与ObservationContract决策门；daily manifest摘要不得被伪造成episode tape。
- manifest reason合同补充仅适用于已确认外部I/O边界的`external_transport_other`或等价中性兜底，避免新外部错误仅因缺少专用枚举而结束source/collector；parser/schema/本地完整性错误仍不能被该兜底吞掉。
- 已提交的heartbeat split继续作为回归基线：CLOB采用官方10秒PING cadence和项目20秒timeout，RTDS采用官方5秒PING cadence和pinned SDK默认15秒timeout；timeout不得冒充官方共同保证，本事项不重复实现同一split。
- 对应计划已同步上述边界。三份修复文档仍为`DRAFT_AWAITING_USER_REVIEW`，第4.2 manifest字段/16项bound、第4.7 SDK patch矩阵、最终验收与开始代码实现仍等待用户后续明确批准。
- 前文“待用户审查的关键决定”第4项现已部分落实：核心草案的相关语义已完成本轮文档同步；旧`data-collect`/reliability demand与plan仍只列为最终批准后的authority同步任务，尚未修改，也未获得实施授权。
- 修改后跨文档审查发现：若把collector transport/subscription current/open状态直接当作模型trigger，当前无逐事件episode tape的设计无法在offline与online间精确重放。核心需求已据此收紧：只有被`ObservationContract`准入、进入共享canonical tape且四种runtime可因果重放的source evidence可以进入Observation或抢占timer；daily manifest中的transport/subscription诊断默认不对模型可见，reconnect parity只比较其后真正进入canonical tape的证据与状态影响。未来若要让具体transport原因成为模型输入，仍须新的typed evidence/ObservationContract决策门。
- 核心调度复审补齐了目标候选的有界性和根触发：`decision_scope_id`由runtime有限registry分配、模型只能回显，scope retire取消timer且单runtime scopes/timers有冻结总bound；schedule contract还必须冻结base/due-time、logical-to-monotonic映射、checkpoint与overdue单次late wake。cold start/restore无有效pending wake且存在risk-bearing状态时只产生一次`DecisionBootstrapEvent`，不得演变为周期tick。
- 同一复审把非法schedule输出改为scope-specific失败：training令对应trial/candidate无效，offline-evaluation拒绝receipt，只有paper/live映射执行权限fail-closed；scheduler始终不得替模型生成撤单、暂停或对侧BUY。确定性age-crossing基线在risk-bearing状态必须有限重武装，连续因果输出相同bucket不等于runtime外生fixed tick。
- collector复审纠正“全部源无数据必然degraded”的不可观测表述：全部transport不可达才记录有直接证据的degraded并逐源恢复；transport按协议存活但全部业务流静默时保持active/idle，只保存coverage、nullable last-qualifying与当前transport事实，不创建虚假degraded或silence scope。
- 需求4.2.5的具体manifest schema仍未获得用户最终批准，也尚不足以直接实施。阶段0现新增明确用户决策门，必须先冻结四logical producer与六`LIVE_SOURCES` counter间的owner/mapping、共享producer引用/复制规则、scope/recovery矩阵、16项计数域、聚合state、day coverage/restart/cross-day状态转换、完整reason枚举、字段类型与兼容版本；未全部批准不得进入阶段1。
- 计划补齐了RTDS missingness的known/unknown coverage边界、CLOB parser+generic manager no-PONG回归，以及跨日`last_persisted_alive_evidence_at_ms`精确定义；正式mask枚举统一为`unknown_not_observed`。

## 2026-07-17：批准并落盘 manifest/coverage 决策

- 用户明确批准`M1-A / M2-A0 / M3-A-R / M4-A-R / M5-N/A / M6-A+ / M7-A-R / M8-A`，并授权把决定同步到核心需求、当前修复需求、计划和changelog；该批准不授权代码实现、SDK patch、构建或部署。
- 三路独立审查一致否决旧`M1-C`动态transport scope、每logical producer 16项hard bound、transport reason/episode矩阵和`M2-B`向六个source counter复制coverage。诊断容量、计数饱和或日志失败不得转化为collector停机条件。
- 三路对coverage粒度的唯一分歧是“单一collector coverage”与“collector+四producer coverage”。用户采用两路审查及主审推荐的`M2-A0`：当前四producer全部mandatory、任一terminal结束collector，因此只保存共同观察资格窗口；未来若允许optional/独立producer，必须升级嵌套schema并重新决策。
- 顶层唯一新增对象固定为`collection_day_coverage`，版本`data_collect_collection_day_coverage_v1`；六个既有source counter继续只记录artifact首末接收事实及既有操作计数。对象使用`not_started|active|ended_known|unclean_end_unknown`、三个nullable `i64` UTC毫秒字段、饱和`u32 activation_count`与`u32 unclean_discontinuity_count`及完整状态不变量。
- 生命周期修订冻结为：取得并持续持有dataset-root级OS-backed独占实例lease后，coordinator同步构造bounded queue、取得唯一receiver并确认ingress处理循环可进入；ManifestOwner、六writer与四producer wrapper完成内部ready后阻塞共同gate，owner执行一次不重试的atomic activation并ack后才释放网络poll。可靠known-close候选之间first-close-wins，但候选在mandatory fence/rendezvous完成前不是最终`ended_known`；此前发现的非协调panic/abort/join failure执行unclean-dominates。协调退出不能只靠generation/cause字符串，必须由本次close的精确oneshot ack，或预登记后由同一handle `is_cancelled()`精确消费的task/generation/close-id permit证明；panic、错/缺permit和无法证明因果的退出仍为unclean。wrapper只能记录其首次观察future/worker返回的本地时刻；进程或owner消失不得伪造known end。
- `activation_count > 1`只把最后start前无法证明连续coverage的区间隔离出要求四runtime parity的finalized replay，typed raw仍保留；不得用日终信息倒灌历史`unknown_not_observed`。任何unclean共同coverage的最后可证明边界只能是`last_started_utc_ms`，某一健康source更晚的last recv不能证明其他mandatory成员仍存活。
- 顶层manifest继续为`data_collect_daily_manifest_v3`，直接嵌套v1而不新增通用extensions容器。legacy v3缺对象可读且coverage为unknown、禁止回填；新writer必须写对象，malformed/未知nested version/状态违约fail closed；reconcile、merge、close与recovery必须保留该对象。
- coverage自身不创建模型trigger、`DecisionWakeEvent`或synthetic row；它只在已有真实事件、bootstrap或稀疏wake求值时参与known/unknown mask。当前不建设transport summary、精确transport episode或本地run-segment artifact。
- `M8-A`只冻结未来策略：首次启用由部署编排在边界前停止legacy进程、确认未创建新日manifest，下一UTC日启动v1 binary；回滚/coverage-aware升级还需要另行批准并验证的v1-aware `stop-at-boundary/no-new-day-activation`或等价编排关闭coverage-bearing open日。same-day binary/version/identity takeover、identity migration或旧binary接管open v1日不在本事项内；相同identity加独占lease的systemd同日恢复属于M4状态机，不是部署takeover。当前代码不得冒充已具备该生产能力。
- 本决定取代本文2026-07-16草案中“逐source/scope摘要、16项active/open、collector/六source coverage envelope、`unclean_recovery_count > 0`才触发保守unknown、manifest schema仍待批准”等当前方案；这些旧段落保留为决策演进历史，不再是实施authority。

## 2026-07-17：落盘后三路独立审查与修正

- 三路审查均确认`M1-A`、`M5-N/A`、顶层v3直接嵌套v1、不复制六source counters及“不建设transport episode/sidecar”的方向正确；当前authority没有残留动态manifest scope、16项hard bound或逐source coverage。
- 审查发现原落盘起点把“future已构造”误当作“supervisor已接纳”，并把同步构造的ingress/queue误写成spawned task。需求和计划已改为coordinator先构造bounded queue、取得唯一receiver并确认ingress循环可进入；ManifestOwner、六writer和四mandatory producer wrapper再spawn、内部ready并阻塞共同gate，owner持久化active并ack后才释放网络poll。terminal测试也区分producer/writer task return与coordinator ingress提前退出、receiver ownership丢失或queue不变量失败，不再把queue冒充可return/panic的独立task。
- 审查发现单靠rollover resume ack不能处理新日frame与boundary timer竞争，也不能在旧顶层manifest关闭后再merge旧日事件。合同已增加owner-owned UTC day/activation generation、每event permit与control/barrier旁路：无PendingExit时先关闭旧raw gate并冻结候选，drain旧日已接纳事件，收齐四producer barrier与六writer finalize ack，确认没有unclean-dominates后同次最终写入nested end并关闭旧顶层，再创建新日active commit并等待ack释放。overdue stop/terminal必须在generation切换前保存一次性PendingExit：旧日final-close提交前的可靠候选可令旧日于boundary known-close，普通路径新日只能open/not-started且不resume；final-close前无法由fence排除的unclean令旧日unknown。旧日已经closed而新日activation commit尚未发生时的failure不得重写旧日，新日保持missing/not-started且count不变；commit一旦成功，状态/count已经归属新日，即使ack丢失也不得回滚，ack只控制network/raw release。M8路径不创建任何新日manifest。
- M3状态不变量已补齐`first <= last`、ended activation非零和仅end允许等于下一日零点；prior active转unclean时必须饱和增加unclean count，无PendingExit的正常rollover新日active明确从boundary和activation=1开始。
- 最终复审发现同日恢复原文会漏记或双记unclean。现已冻结四态current-day activation：`not_started`首次激活；遗留`active`的activation与unclean各加一；已持久化`unclean_end_unknown`只增加activation；`ended_known`只增加activation并清空end。每个中断只能在退出时或下次发现遗留active时二选一计数；current顶层保持open，closed拒绝重开。相同identity不能证明旧实例已退出，因此current-day恢复前必须持有dataset独占实例lease。
- 同一复审收紧了协调abort：现有source stop oneshot作为精确协作ack，强制abort必须按具体handle预登记并消费一次性permit；全局`aborting_source_tasks`、generation/cause相同或stop已经发出都不是豁免证据，`JoinError::is_panic()`始终优先进入unclean-dominates。
- prior-open顶层closure改为state-aware：只有active/既有unclean使用`unclean_exit_recovery`及gap；not-started、ended-known与legacy missing分别使用中性closure且不伪造unclean gap。兼容测试扩展到identity、unknown version/field/type/time/state及全部ManifestOwner mutation字段保留。
- 阶段1完成条件已收窄为classification、coverage lifecycle、测试seam与既有恢复不回退；heartbeat、RTDS/CLOB/Binance正式持续恢复分别在阶段2--6和阶段7验收，不再要求未实现能力提前通过。
- 核心需求同步了M1--M8批准状态、source-local transport事实边界、replay eligibility、M8未来部署前置条件，以及既有daily reconnect counter不能伪造具体reconnect原因/episode的限制。
- 最新四份文档经core、修复计划与跨文档三路最终只读复审，结果均为PASS，Critical与Important均为0；复审确认旧日final-close commit、新日activation commit、ack-only release、四态同日恢复、unclean唯一计数、PendingExit、task级abort证明、dataset独占lease与prior-state矩阵已经闭环。本次仍只有文档落盘，不构成代码实现或部署批准。
