# 训练采集全数据源可用性与连接生命周期修复 Changelog

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应需求：`demand/data-collect-source-availability-lifecycle-repair-demand.md`
>
> 对应计划：`plan/data-collect-source-availability-lifecycle-repair-plan.md`
>
> 状态：`APPROVED_IMPLEMENTATION_PHASE_1_COMPLETE_PHASE_2_AUTHORIZED`。用户已于2026-07-17批准最终需求、计划及当时SDK-first逐项patch矩阵，并授权按计划执行本地文档/代码修改、测试、独立审查、commit和push；阶段0与阶段1均已完成并通过独立审查，阶段2可在阶段1 commit/push后开始。生产构建、部署、远端服务写操作和数据清理仍未获授权。

## 2026-07-17：最终批准并开始阶段0

- 用户明确指令“批准按当前计划文档执行”。本次批准覆盖`demand/data-collect-source-availability-lifecycle-repair-demand.md`、`plan/data-collect-source-availability-lifecycle-repair-plan.md`及需求第4.7节逐项SDK patch矩阵，授权阶段0--8的本地实施、验证、独立审查、commit和push。
- 批准不扩张计划范围：模型调度实现、生产release构建、部署、远端服务启停/写操作、生产数据清理与重采仍保持未授权。
- 阶段0首先同步旧authority并重建SDK provenance/diff基线；如果官方协议、真实代码或既有批准文档出现无法同时成立的新冲突，按决策门停止，而不是自行调和。

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

## 2026-07-17：阶段0 authority同步

- `demand/data-collect-demand.md`已删除或改写普通业务静默/空候选/stale/外部网络故障自动升级为gap、lag、异常告警、freshness网络动作或systemd新PID恢复的当前合同；保留confirmed ID/update hole、SDK/local lag、queue drop/timeout、parser/schema、writer/manifest和资源硬边界。
- `demand/data-collect-reliability-repair-demand.md`已标记为只在heartbeat及其他未被取代的数据完整性合同上继续有效。固定patch数量、D2 one-shot reconnect、CLOB 30/60秒watchdog、有限总恢复、旧daily coverage和同日生产切换/清理均改由本事项取代。
- `plan/data-collect-reliability-repair-plan.md`已停止其未执行阶段2--5，不再提供生产构建、远端预置、数据删除、同日替换或26小时长跑授权；旧changelog保持原文。文件现只保留历史基线和指向本计划的同步摘要。
- 三份旧authority统一采用顶层`data_collect_daily_manifest_v3`直接嵌套单一`data_collect_collection_day_coverage_v1`，六个既有source counters职责不变；未新增transport scope/reason/episode、逐producer/source coverage、extensions、receipt或sidecar。legacy缺对象只读为unknown，新writer缺失/malformed fail closed。
- 旧authority同步了dataset-root OS独占lease、十个mandatory wrapper ready gate、atomic activation commit与ack-only release、正常rollover rendezvous、PendingExit、task级abort证明、同日四态恢复、prior state-aware closure和M8 UTC边界部署前置条件。
- `demand/rebuild-core-demand.md`在前一文档批准commit中已经与本事项一致，本阶段复核未发现需要再次编辑的漂移；用户现有`AGENTS.md`和`推荐开发顺序.md`仍不在本事项范围。

## 2026-07-17：阶段0 SDK provenance与逐hunk基线

- pinned crate仍为`polymarket_client_sdk_v2 0.6.0`；registry `.crate` SHA-256为`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`，发布时间为`2026-07-02T14:29:42.824907Z`且未yank。vendor `.cargo_vcs_info.json`与官方`main`均指向`3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb`。截至核验时crates.io最新稳定仍为0.6.0，官方仓库没有tag或GitHub Release对象。
- 真实`.crate`解包树与vendor全树比较只命中三个源码路径：`src/ws/connection.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`；没有Cargo、依赖、许可证、README或第四个源码路径漂移。Cargo解包缓存自身的`.cargo-ok`不属于原始crate或vendor差异。
- `connection.rs`的14个diff hunk已逐项拆分：sticky parser fault、generation-tagged outbound、`connection_generation()`与`bind_generation()`保留；one-shot ticket字段/channel/reset/select branch、`RequestedReconnect`与`request_reconnect()`删除。CLOB response的2个hunk均属于interested-array all-or-error并保留；RTDS subscription的3个hunk属于`SimpleParser::Default`、2,048-byte有界脱敏诊断及其测试并保留。
- 官方PR状态复核：[#63](https://github.com/Polymarket/rs-clob-client-v2/pull/63) heartbeat owner-propagation、[#40](https://github.com/Polymarket/rs-clob-client-v2/pull/40) high-level task drop和[#81](https://github.com/Polymarket/rs-clob-client-v2/pull/81) `SimpleParser::Default`截至2026-07-17均为open/unmerged，不得冒充已发布或已合并修复。官方release commit为[`3ae1aae`](https://github.com/Polymarket/rs-clob-client-v2/commit/3ae1aae5e9ded38f984464c9fc0f307f8a9f41fb)。
- 阶段2可以只在既有`connection.rs`内适配PR #63的owner-propagation方向，同时保持本地parser terminal语义；原PR不能直接apply。PR #40已混入无关auth等改动，且abort-on-drop不等于last outbound sender关闭后的normal teardown，因此不整体回移。PR #81的一行源码hunk已与vendor一致，无需重复应用或新增vendor测试文件。
- 除官方pre-parse完整文本帧trace差异外，SDK矩阵无需新决策：不升级版本、不增加第四个vendor文件；阶段2新增heartbeat-dead recoverable exit和last-sender normal teardown，阶段3再原子删除one-shot能力与生产消费者。本阶段没有修改vendor、`Cargo.toml`或`Cargo.lock`。
- 可复现核验包括：`sha256sum ~/.cargo/registry/cache/.../polymarket_client_sdk_v2-0.6.0.crate`、`git ls-remote --symref https://github.com/Polymarket/rs-clob-client-v2.git HEAD refs/heads/main`、`git ls-remote --tags ...`、临时解包后`diff -qr`，以及GitHub API读取PR #40/#63/#81；全部为只读操作。

## 2026-07-17：阶段0 SDK逐hunk复核补正

- 独立调用链审查确认vendor差异仍只有三个源码文件，但发现`connection.rs`中删除的`tracing::trace!(%text, ...)`位于`parser.parse()`之前，是一个此前未被第4.7节单独命名的hunk。当前项目未启用SDK `tracing` feature，但阶段0仍必须逐hunk闭合；现列出A“保留当前移除hunk”和B“恢复官方0.6.0原hunk”两个选项。B会记录全部文本帧、包括随后解析失败的完整raw frame，因此等于显式放宽已批准的失败trace脱敏边界；另造success-only raw trace不是官方回移且无当前需求，不作为第三选项。
- 同一审查确认vendor具备`bind_generation()`，RTDS生产actor已经按观察到的generation构造bound manager并在发送后复核，但CLOB market actor当前仍把未绑定的`connection.clone()`交给`MarketSubscriptionManager`。快速重连竞态下，旧actor迭代可能借新的atomic generation把延迟请求标记为新代；因此不能把vendor generation能力冒充CLOB生产调用已经完成。
- 需求与计划已把CLOB补齐明确放入阶段4：按observed generation绑定manager，发送后复核connected state/generation后才发布applied ack，并增加“旧代已观察connected、随后快速切代”的竞态测试，证明旧代请求既不写入新socket也不产生applied ack。该补正使用现有公共能力、同一actor/receiver/socket owner，不新增vendor文件或API，属于已批准generation隔离语义的完整生产接入。
- 第4.7节已经明确区分“vendor能力已存在”“RTDS生产已接入”和“CLOB阶段4待接入”；raw trace hunk尚未获最终结论，因此阶段0不得标记完成、commit/push或进入阶段1。阶段0仍未修改vendor或生产代码。

- 第二路SDK复审进一步证明当前“RTDS有界脱敏诊断”只对自建摘要成立，`RtdsParseDiagnosticError::Display`仍先原样输出底层serde错误，非法type scalar可进入sticky fault和`tracing::warn!`。这与已批准脱敏目标不一致，阶段2现增加operator-visible Display/fault/log脱敏和tracing subscriber负例；error kind与source chain继续保留供typed inspection，但生产日志不得无脱敏展开。
- 同一复审证明CLOB仅在发送后复核generation仍不足：当前applied watch值是`DesiredMarketAssets`，waiter只比较revision；如果在post-check与`send_replace`之间切代，旧ack仍可被误收。阶段4现明确使用项目私有generation-tagged applied ack，并让waiter同时校验current generation、revision和connected；新增两个独立竞态seam。两项补正均落实原获批脱敏/generation隔离语义，不增加vendor路径、公共SDK API、actor或receiver。

## 2026-07-17：阶段0独立审查发现决策门与旧authority残留

- 第三路独立审查认定raw trace hunk不在用户此前批准的矩阵中，不能由实现者直接冻结；需求、计划和changelog已改为明确等待用户决定，其他SDK条目和CLOB generation补齐不受影响。
- 同一审查发现`demand/data-collect-demand.md`仍残留`required_files[]`/complete verdict、retention/threshold receipt、collector自动删除、远端删除验证和“立即部署重启”等旧活动条款，与新专项M8、无receipt及无数据清理授权冲突。现已统一为每日实际file facts、50GB协调finalize后退出75、独立运维另行批准，且历史部署事实不得重新触发当前部署。
- authority审查还发现旧reliability demand保留“持续无效消息不能饿死watchdog”的当前措辞，已改为无效/stale只更新typed分类且不触发watchdog或网络/退出动作；阶段1原计划提前要求阶段3--6端到端无gap/lag，也已收窄为只冻结新增共同classification/coverage fixture，各协议生产断言留在对应阶段实现并于阶段7组合验收。
- SDK复审纠正了最初把官方`%text` hunk称为“成功帧trace”的错误：它实际位于parse前，恢复后会记录成功与失败的全部文本帧。A/B决策及条件测试已按这一事实重写；B明确覆盖失败trace脱敏，不能与原脱敏合同同时宣称成立。
- 在用户决定raw trace前，阶段0保持blocked；不会提交文档、修改vendor、开始阶段1或执行任何生产操作。

## 2026-07-17：用户批准raw trace选项B

- 用户明确回复“我选择B”。最终矩阵因此恢复官方0.6.0在`parser.parse()`前执行的`tracing::trace!(%text, ...)`原hunk；当前`data-collect`未启用SDK `tracing` feature，所以该决定不改变当前生产构建行为。
- 如果未来启用该feature，完整成功帧与随后解析失败的完整文本帧都会进入pre-parse TRACE。用户本次选择明确批准这一failure-trace边界放宽；不得在验收或报告中把该TRACE称为脱敏。
- 阶段2仍须修复`RtdsParseDiagnosticError::Display`、sticky fault和warn额外拼入原始serde scalar的问题，并保留error kind/source chain；B只恢复官方pre-parse trace，不授权另造success-only trace或扩大其他日志行为。
- raw trace决策门已关闭。阶段0继续完成最终独立审查、精确commit/push；生产代码和vendor仍须等阶段2按测试先行实施。

## 2026-07-17：阶段0最终审查通过

- 三路独立只读终审分别覆盖authority/M1--M8与生产边界、SDK逐hunk/provenance与阶段2/4目标、vendor allowlist/状态门与无关dirty保护；最终结果均为PASS，Critical 0、Important 0。全局配置已核实为`gpt-5.6-sol`与Ultra reasoning。
- 最终矩阵精确区分用户B批准的pre-parse完整文本帧TRACE例外，以及仍须在阶段2脱敏的`RtdsParseDiagnosticError::Display`、sticky fault与parser-failure warn；CLOB阶段4使用私有generation-tagged applied ack覆盖两个切代竞态。
- `git diff --check`通过；阶段0只修改六份需求/计划/changelog文档，没有修改生产代码、vendor、`Cargo.toml`或`Cargo.lock`。用户现有`AGENTS.md`和未跟踪`推荐开发顺序.md`保持在提交范围外。
- 阶段0完成条件已经满足；本阶段commit/push成功后进入阶段1。生产构建、部署、远端服务操作与数据清理继续未授权。

## 2026-07-17：阶段1共同availability合同与生产测试seam

- 本阶段生产/测试代码仅修改`data-collect/src/daily_manifest.rs`、`data-collect/src/health.rs`、`data-collect/src/live_runtime.rs`、`data-collect/tests/network_recovery_contract.rs`和`data-collect/tests/reconnect_trace_contract.rs`；本changelog先记录候选实现和验证证据，需求、计划与changelog的顶层阶段状态只在终审通过后原子同步。未修改vendored SDK、`Cargo.toml`、`Cargo.lock`、模型、部署配置或远端运行态；用户现有`AGENTS.md`和未跟踪`推荐开发顺序.md`继续排除在本事项提交之外。
- `SourceFactClass`只提供保守的共同分类fixture：只有具备足够上下文的既有variant才返回business availability、transport availability或confirmed integrity；语义含混的HTTP、subscription、empty/snapshot、queue、资源等variant返回`None`，由真实调用点上下文决定。它不是第二个health registry、transport状态机、reconnect owner或生产控制面。`CircuitState::Exhausted`已删除，重试规划不再以耗尽态表达source终止，但本阶段未提前改写阶段2--6的协议专属生产恢复分支。

| 生产调用族 | 阶段1冻结的分类/行为 | 后续阶段仍须完成 |
| --- | --- | --- |
| manifest、writer、bounded queue/drop、parser/schema、SDK `Lagged(n)`、sequence/update-ID或payload冲突 | confirmed integrity；保留既有gap、terminal、quarantine或fail-closed边界 | 不得被协议恢复放宽 |
| 外部connect/read/write、EOF/Close、WebSocket/HTTP/rate-limit边界 | transport availability；由既有唯一source-local owner处理，本阶段只建立共同分类与不回退证据 | 阶段2--6分别证明无总attempt/elapsed退出预算 |
| business silence、stale、合法空candidate | business availability；共同fixture不写`source_gaps`或业务`lag_events` | 阶段3--6删除各协议旧watchdog/reconnect/退出分支 |
| subscription或reconnect调用 | 必须按真实cause与owner上下文分类，不能只按函数名归类 | RTDS one-shot删除、CLOB generation desired-state等分别在阶段3/4完成 |
| producer/writer/ingress返回或join结果 | 可靠返回、精确协作stop、非协调panic/abort/join failure按共同gate与task identity处理 | parser/writer/internal terminal继续属于完整性失败 |

阶段1还按生产函数族审计了全部`record_gap`、`record_lag`、reconnect/subscription和return入口；下表是有界调用清单，不把`SourceFactClass`冒充自动调用点分类器：

| 生产函数族 | 代表调用与当前分类边界 | 阶段门 |
| --- | --- | --- |
| `run_live_training_runtime`、`complete_pending_exit_rollover`、`drive_normal_rollover` | 顶层`record_gap`只覆盖lease/cleanup、runtime/rollover/control channel、ingress/唯一receiver、manifest mutation/ack、writer/source fence/join、capacity scan与其他内部不变量；operator stop、run deadline和capacity limit是显式可靠close候选，不是external availability。顶层没有subscription/reconnect owner。 | 阶段1冻结共同gate、known/unclean与收敛；任何unexpected member return继续terminal。 |
| `run_polymarket_source_with_spec`、Gamma discovery、`collect_polymarket_single_receiver`、`schedule_polymarket_discovery_retry`、`polymarket_drain_terminal_error` | Gamma合法空candidate和asset business silence属于business availability；Gamma/SDK connect、subscription、Close/EOF等属于transport availability；SDK `Lagged(n)`、parser/schema、decoded/global capacity、queue、dedupe冲突属于confirmed integrity。当前`polymarket_discovery_source_gap`、silence `record_lag/gap`和resubscribe/reconnect调用仍是旧分支。 | 阶段4删除silence驱动网络/退出并补generation-tagged desired/applied；完整性分支不放宽。 |
| `run_rtds_source_with_spec`、`wait_for_fresh_rtds_message`、`emit_rtds_message` | 初始`subscribe`是唯一desired-state入口；业务stale/无fresh属于business availability，SDK connection generation/Close/EOF属于transport availability；SDK `Lagged(n)`、parser/schema、dedupe冲突属于confirmed integrity。当前freshness `record_lag/gap`、`request_reconnect`、两代/总deadline退出仍明确保留。 | 阶段2先修SDK transport owner；阶段3删除one-shot与business-silence退出。 |
| `run_binance_depth_source_with_spec_and_control`、`fetch_binance_depth_snapshot`、`run_binance_depth_update_ws_source_with_control` | WS connect/read/Close/EOF、HTTP transport/rate-limit属于transport availability；`no_business_messages`属于business availability；malformed snapshot/update schema、queue failure和confirmed update-ID hole属于confirmed integrity，ID hole继续gap+resnapshot。当前no-business与普通reconnect仍写lag/gap。 | 阶段6拆除business-silence网络动作并保留transport恢复、snapshot与ID-hole合同。 |
| `run_binance_ws_source`、aggTrade backfill/continuity | WS connect/read/Close/EOF/server shutdown及REST transport失败属于transport availability；`business_message_timeout`属于business availability；ID/payload冲突、continuity/backfill不变量、pending/queue硬边界和schema错误属于confirmed integrity。当前business timeout仍`record_lag`并reconnect，transport reconnect仍写gap。 | 阶段5改为全frame transport inactivity和无总预算恢复；完整性与有界单range backfill边界保留。 |
| ManifestOwner、`flush_source_batch`、writer worker、`send_event` | manifest schema/identity/mutation/atomic publish、writer parser/finalize/report、bounded queue timeout/drop、磁盘/资源hard bound全部是confirmed integrity；这些路径没有subscription或external reconnect语义。 | 阶段1已接入共同coverage lifecycle；阶段2--6不得以availability恢复吞掉这些错误。 |
| `SourceTaskHandle`、source wrapper observation、`finished_source_task`/`source_task_ended_unexpectedly` | 可证明的inner/wrapper可靠return冻结known candidate；同一close identity的精确Stop或由同一handle消费的abort permit才是coordinated；panic、wrapper error、缺/错permit和无法证明的join结果为unclean。运行deadline前任何unexpected source完成仍由顶层发现并失败。 | 阶段1完成共同终止分类；后续各source availability loop必须持续运行而不能返回到此terminal边界。 |

- daily manifest保持顶层`data_collect_daily_manifest_v3`并直接嵌套唯一`data_collect_collection_day_coverage_v1`。实现覆盖四态、三个nullable UTC毫秒、两个饱和`u32`计数、状态/时间不变量、legacy缺对象只读为unknown、新writer必写、未知nested version/field/type及非法转换fail closed；六个既有source counter没有复制coverage。ManifestOwner的全部mutation都必须保留并重新验证nested coverage，generic reconcile/load不能暗改activation或unclean计数。
- runtime先取得并持续持有dataset-root OS独占lease，再同步建立bounded queue与唯一receiver；ManifestOwner、六writer和四mandatory producer wrapper分别ready并阻塞共同gate。owner一次性持久化active并ack后，coordinator才原子释放raw/network permit。lease被主runtime及所有生产writer、owner和source task共同持有；hung writer、live owner与live source测试证明这些任务返回前第二实例不能取得lease。
- 正常UTC rollover使用同一个transition临界区和day/generation permit：关闭旧raw gate、drain、收齐四producer barrier及六writer finalize ack、最终提交旧日nested close与顶层close、创建并提交新日active，ack后才resume。可靠stop/deadline与rollover竞争通过一次性`PendingExit`和确定性`RunDeadline`候选收敛；旧日final-close与runtime exit在同一临界区线性化。activation已提交但ack前退出返回`PreparedExitWon`，不得把已知持久化结果误报为`OutcomeUncertain`或回滚。
- startup在active commit前失败时不poll source；commit后deadline/stop向四producer发送同一精确`Stop(task identity, generation, close_id)`。writer只在本地命令循环接收`Run`，随后由集中shutdown各关闭一次。跨日PendingExit让source保持停止、writer继续消费synthetic barrier并完成旧日finalize；测试持有每个reply receiver并断言收到旧generation的精确`StopWithClose`，不再因receiver提前drop产生假失败。
- source wrapper在transition锁内取得本地观察时间和generation ownership；typed inner return、wrapper panic、预登记且由同一handle消费的abort permit、错误/缺失permit都通过生产完成检测路径测试。writer lifecycle与共同gate/day epoch绑定，coordinator ingress、唯一receiver与queue不变量仍保留完整性失败边界。
- 私有测试seam直接调用生产`drive_normal_rollover`、真实ManifestOwner、四producer token、六writer、synthetic barrier和精确stop路径。新增覆盖包括：两种poll顺序得到相同旧日manifest digest；old-day commit与runtime exit线性化；source terminal与activation只有一个generation owner；deadline先于后续source return且跨UTC日一致；activation commit后同日/跨日deadline；activation后退出等待全部flush、向四producer发送同一新generation stop且绝不resume；owner/source/writer持有lease；owner mutation拒绝coverage digest漂移。
- 阶段1刻意保留并继续通过四个旧行为复现：RTDS两代one-shot恢复预算耗尽、CLOB两次resubscribe后退出、Binance depth reconnect仍写gap、aggTrade PING不重置旧business freshness/reconnect。它们不是正确目标合同，分别是阶段3、4、6、5的实现门；本阶段没有把未完成的协议修复冒充通过。
- 本轮实现验证为：`cargo check -p data-collect --tests --locked`通过；`cargo clippy -p data-collect --locked --all-targets -- -D warnings`零告警；`cargo test -p data-collect live_runtime::tests --lib -- --test-threads=1`为174/174通过；`cargo test -p data-collect --lib --locked -- --test-threads=1`为203/203通过；`cargo test -p data-collect --locked -- --test-threads=1`全部unit、integration和doc tests通过；`cargo fmt --all -- --check`与`git diff --check`通过。阶段1顶层状态、最终review结论与commit/push证据须在新的独立终审通过后补记；在此之前不得进入阶段2。

## 2026-07-17：阶段1独立终审阻塞与coverage计数决策门

- 新的三路独立终审中，范围/文档审查在补齐七类生产调用族清单后PASS；manifest/lifecycle审查报告3项Important，runtime资源/终止审查报告5项Important，Critical均为0。因此阶段1保持`AUTHORIZED`而非`COMPLETE`，没有commit/push，也没有进入阶段2。
- 两项manifest测试/接线缺口可直接按既有批准合同修复：启动active commit和网络release前必须验证并冻结四producer到六source的完整双向mapping；exit-first与commit-first必须分别走真实RawDayGate、ManifestOwner和rollover路径，并比较旧日字节digest、最终disposition与新日状态，不能用预选owner mode或原子计数helper冒充poll-order验收。
- 终审发现已批准v1计数不变量与状态转换无法同时成立：需求只规定所有状态`unclean_discontinuity_count <= activation_count`，因此`active(activation=5, unclean=5)`当前是合法输入；但下一次独立unclean必须把unclean饱和加一，得到`6 > 5`并被同一validator拒绝。保持count不变会漏记尚未饱和的独立中断，增加activation又会虚构一次成功active持久化。该冲突必须由用户决定后才能修改需求、计划、schema validator和测试。
- 推荐选项`C1-A`：对`active`和`ended_known`增加`unclean < activation`，仅当两者均已是`u32::MAX`时允许相等；`unclean_end_unknown`继续允许`unclean <= activation`，因为最后一次activation已经被计为unclean。这样保留v1字段与饱和语义，并使所有可接受状态都能执行下一次转换。备选`C1-B`是继续接受非饱和的active相等状态并在下一次unclean不增count，代价是正式放弃“每个独立中断计一次”的语义；`C1-C`是升级nested schema重新设计overflow/计数表达，范围和迁移成本显著更大。主实现不自行选择。
- runtime终审的5项Important均已有批准合同、无需新业务决策：为runtime-exit watcher建立abort-on-drop与正常await owner；保存writer OS thread handle并在共同budget内等待report后真实join，partial spawn失败收敛已启动线程，hung writer仍fail closed并持lease；把source control ack与最终task proof联合分类以消除stop/可靠return poll-order差异；在writer report保存lifecycle epoch/terminal class，让可靠terminal作为known proof而panic/missing/wrong epoch保持unclean；把所有mandatory task spawn后的fallible cutpoint统一导向best-effort unclean convergence，禁止裸`?`/return detach后台task。
- 为避免在决策门前留下未验证半成品，主实现已撤回刚开始的watcher/writer局部修改；随后`cargo fmt --all -- --check`、`git diff --check`和`cargo check -p data-collect --tests`重新通过。工作树仍是此前188/188及完整data-collect测试通过的阶段1候选，加上本次changelog审查记录；已知Important尚未修复，因此不得提交。

## 2026-07-17：用户批准以单一同日再激活布尔值取代coverage计数

- 用户批准删除`collection_day_coverage`中的`activation_count`与`unclean_discontinuity_count`，改为必填`reactivated_within_day: bool`。首次成功原子activation写`false`；任一同一UTC日后续成功原子activation写`true`且当日保持单调；新UTC日首次activation重新为`false`。unclean、known end、prior-day reconciliation和普通ManifestOwner mutation不得改变该值。
- 本决定只修订`M3-A-R`的字段、状态转换、replay隔离条件和相应验收；`M1-A / M2-A0 / M4-A-R / M5-N/A / M6-A+ / M7-A-R / M8-A`保持不变。`reactivated_within_day=true`取代`activation_count > 1`，用于隔离最后一次start之前无法证明连续coverage的replay区间；`unclean_end_unknown`继续单独表达未知尾部，不记录或推断中断次数。
- 先前记录的两个饱和计数字段、精确unclean计数、大小关系以及`C1-A/B/C`仅保留为决策演进历史，不再是实施authority；计数冲突决策门随本批准关闭。schema/type、四态时间nullability、UTC日范围、时间顺序、顶层/嵌套状态关系、未知字段/version fail closed等既有结构校验继续保留，不新增布尔值与时间之间的推测性关系校验。
- 审计确认该nested v1的schema需求已在既有文档commit中存在，但生产实现仍只存在于未提交、未部署的阶段1工作树，从未进入已提交生产代码或真实manifest；因此继续使用`data_collect_collection_day_coverage_v1`，无需迁移或升级到v2。当前专项需求/计划、`data-collect`需求、reliability需求/计划及核心需求草案已同步；旧changelog保持原文。

## 2026-07-17：用户批准原子发布结果语义R

- 阶段1补充启动失败验收时发现真实合同冲突：专项需求第4.2.6项与计划步骤1.1.4原本要求“任何active写入失败都保持既有持久状态”，但既有`write_json_atomic`按临时文件写入/`fsync`、最终`rename`、父目录`fsync`执行；若`rename`成功后父目录`fsync`失败，函数返回错误但最终路径已经可能指向新manifest，无法安全保证旧状态不变。两路独立只读审查均确认该冲突。
- 用户明确批准推荐R。active publish现冻结为三种结果：`NotPublished`只覆盖最终`rename`前失败并保证最终manifest原始字节与digest不变；`PublishedDurabilityUncertain`表示`rename`成功但父目录`fsync`失败，当前进程必须fail closed并禁止ack、网络/raw release、重试或rollback，下个独占lease持有者只按实际读到的`not_started|active`恢复；父目录`fsync`成功才是`DurablyCommitted`并允许activation ack。
- R不改变`M7-A-R`的本地完整性fail-closed边界，不忽略父目录`fsync`错误，也不引入无法可靠保证的rename后rollback、双manifest、WAL、数据库或生产测试hook。生产写入顺序保持不变，仅为rename后的父目录`fsync`错误增加稳定`published_durability_uncertain_after_rename`分类；临时路径冲突测试只证明`NotPublished`，不得冒充覆盖全部I/O阶段。

## 2026-07-17：阶段1最终终审通过

- 阶段1先前终审发现的manifest/lifecycle与runtime Important 已全部按批准合同修复；coverage计数冲突由用户批准的单一`reactivated_within_day`消除，原子发布冲突由用户批准的R三态语义消除。本节取代此前“阶段1保持AUTHORIZED、不得提交”的临时阻塞状态。
- manifest/R终审、runtime终审、范围/文档终审均PASS，三路分别为Critical 0、Important 0。审查确认没有fail-open、旧日重写、影子gate/receiver、越界vendor/依赖/部署修改，也没有把pre-rename测试冒充post-rename父目录`fsync`故障注入覆盖。
- 最终验证为：`cargo check -p data-collect --tests --locked`通过；`cargo clippy -p data-collect --locked --all-targets -- -D warnings`零告警；`cargo test -p data-collect live_runtime::tests --lib -- --test-threads=1`为174/174通过；`cargo test -p data-collect --lib --locked -- --test-threads=1`为203/203通过；`cargo test -p data-collect --locked -- --test-threads=1`全部unit、integration和doc tests通过；`cargo fmt --all -- --check`与`git diff --check`通过。独立runtime终审另复跑174项`live_runtime`测试并通过，F-R Binance depth跨UTC日resnapshot/reprocess测试包含在内且单独通过。
- 阶段1共同classification、coverage lifecycle与生产测试seam达到计划完成条件；阶段3/4才翻转的旧RTDS/CLOB有限恢复测试仍按计划保留为可复现基线，不构成本阶段缺陷。阶段1 commit/push完成后可进入已授权的阶段2；生产构建、部署、远端操作与数据清理仍未授权。
