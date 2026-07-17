# 训练采集全数据源可用性与连接生命周期修复 Changelog

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应需求：`demand/data-collect-source-availability-lifecycle-repair-demand.md`
>
> 对应计划：`plan/data-collect-source-availability-lifecycle-repair-plan.md`
>
> 状态：`APPROVED_IMPLEMENTATION_PHASE_5_COMPLETE_PHASE_6_AUTHORIZED_CT_B_APPROVED`。用户已于2026-07-17批准最终需求、计划及SDK-first逐项patch矩阵，并授权按计划执行本地文档/代码修改、测试、独立审查、commit和push；阶段0至阶段5均已完成全量验证和独立终审，阶段5精确commit/push后进入已授权阶段6。同日用户批准阶段5增加同artifact aggTrade v6 invalid-evidence、顺序与handoff evidence子边界。用户于2026-07-18进一步批准以WS因果直录取代aggTrade ID-hole/REST backfill/pending/merge/active continuity控制，先同步文档再继续实现。生产构建、部署、远端服务写操作和数据清理仍未获授权。

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

## 2026-07-17：阶段2 vendored SDK transport生命周期候选实现

- 阶段2先按计划建立红灯。修改vendor前，CLOB no-PONG测试在2秒内没有第二次连接；最后一个裸manager drop、`MarketSdkRuntime` drop和`RtdsSdkRuntime` drop均在2秒内未观察到旧socket关闭；非法`timestamp` scalar负例还证明`RtdsParseDiagnosticError::Display`会把非allowlist原始值传播到sticky fault。红灯证明的是heartbeat结果未传回owner、active handler未观察last-sender关闭及外显serde错误泄漏，不是loopback server主动close或测试自身超时。
- 官方[#63](https://github.com/Polymarket/rs-clob-client-v2/pull/63)截至实施时仍未合并，只复用其heartbeat结果应回传connection owner的方向；没有原样套用其把所有ping channel关闭都视为timeout的实现，因为这会吞掉heartbeat panic/cancel。官方[#40](https://github.com/Polymarket/rs-clob-client-v2/pull/40)处理另一套high-level `Client` Arc/task生命周期，当前两个runtime wrapper没有使用该reconnection handler，故未回移或扩大阶段范围。
- `connection.rs`现使用私有互斥退出分类：只有outbound receiver在最后一个manager/bound sender释放后返回`NormalShutdown`并停止outer loop；read EOF、WebSocket Close、read/write/PING失败、no/old-PONG返回`Recoverable`并进入原有单owner reconnect loop；one-shot仍单独返回`RequestedReconnect`；parser sticky fault保持terminal；reconnect控制channel不可能关闭、generation overflow以及heartbeat意外正常退出或`JoinError`归为内部terminal，不得冒充普通transport恢复。
- 每个active socket仍只有一个heartbeat task和一个connection owner。owner直接select唯一heartbeat `JoinHandle`；heartbeat用私有typed结果区分owner已停止与timeout。no-PONG或早于当前PING的旧PONG返回typed timeout，owner随后退出旧socket；heartbeat panic/cancel不被改写成timeout。last-sender normal teardown直接在active handler内收敛，不依赖返回后的竞态性`is_closed()`推断。
- `RtdsParseDiagnosticError::Display`不再展开底层serde Display，只外显稳定错误类别与既有2,048-byte allowlist/hash摘要；原`Error` kind、wrapper source字段和`Error::source()`链保留供typed inspection。按用户批准的B，官方0.6.0在parse前的`tracing::trace!(%text, "Received WebSocket text message")`原hunk已精确恢复；vendor测试subscriber逐事件证明完整非法scalar与nested secret只出现在该获批raw TRACE，parser WARN、Display与迟到订阅者看到的sticky fault均不包含这些值。
- 反向矩阵新增并补强真实loopback证据：CLOB no-PONG先观察旧socket Close/error/EOF，再接受第二次handshake与generation增加；裸manager和两个runtime均先完成两轮PING/PONG，再drop并证明socket/task收敛且不重连；bound-generation clone在最后一个sender释放前保持socket；TCP EOF/read failure、server Close、heartbeat timeout、旧PONG、outbound write与heartbeat PING write均保持recoverable；parser fault和heartbeat task panic保持terminal。write/PING两个测试在vendor私有current-thread单测中关闭client本地write half，直接调用私有`handle_connection`并要求typed `WsError::Connection`，不使用RST竞态、公共API或生产test hook。
- 阶段2刻意保留one-shot API、runtime wrapper、生产consumer和全部旧ticket测试，`rg request_reconnect data-collect/src data-collect/tests third_party/.../src`仍能找到定义与调用；它们只在阶段3同一未提交工作阶段原子删除。当前实现没有形成“旧生产consumer存在但SDK能力已删除”的不可运行中间状态。
- provenance复核仍以registry crate SHA-256 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`为原版。registry `src`与修改后vendor `src`的逐文件比较仍只命中批准的`connection.rs`、`clob/ws/types/response.rs`和`rtds/subscription.rs`；相对阶段1 commit，本阶段只修改前者与后者。没有修改`Cargo.toml`、`Cargo.lock`、依赖、生产runtime wrapper、模型、部署或远端状态；vendor本地`target`已清除。
- 聚焦验证当前通过：`polymarket_sdk_patch_contract` 15/15，`polymarket_sdk_runtime_contract` 20/20，RTDS diagnostic 12/12，raw TRACE、old-PONG、outbound-write与PING-write定向单测全部通过。vendored SDK `clob gamma rtds ws` lib测试168/168通过；`--all-features` lib测试184/184、全部integration与doc tests通过；vendor `--all-targets --all-features -D warnings`零告警。单独的`rtds,ws,tracing`组合会命中原版`serde_helpers.rs`既有feature-gate缺口，因此trace测试使用同时启用`clob`的适用组合及`--all-features`验证，没有为测试修改第四个vendor文件或生产feature。
- workspace验证当前通过：`cargo check -p data-collect --tests --locked`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`及`cargo test -p data-collect --locked -- --test-threads=1`（203项lib测试、全部integration和doc tests）。最终fmt/diff、三路独立终审、顶层状态同步及commit/push仍待完成；在这些门禁通过前不把阶段2标记为complete。

## 2026-07-17：阶段2最终终审通过

- heartbeat/退出分类、RTDS隐私/TRACE与vendor provenance、文档/范围三路独立终审均PASS，Critical 0、Important 0。终审确认唯一owner/heartbeat task、互斥退出分类、旧socket先关闭再重连、last-sender task/state收敛、write/PING组合证据、parser与heartbeat panic terminal、迟到sticky fault及CLOB共享generic manager合同成立。
- 三路独立复跑覆盖`polymarket_sdk_patch_contract` 15/15、`polymarket_sdk_runtime_contract` 20/20、vendor connection定向9/9、RTDS diagnostic 12/12及raw TRACE定向测试；主线程最终验证还包括vendored SDK core 168/168、all-features lib 184/184及全部integration/doc tests、workspace 203项lib及全部integration/doc tests、两套clippy `-D warnings`、root/vendor fmt和`git diff --check`。
- 审查期间某一路未外置target的vendor测试曾重新生成ignored `third_party/.../target`；这只造成changelog磁盘事实暂时漂移，不影响源码。三路命令结束后已用vendor manifest `cargo clean`删除6421个文件/5.7GiB，并由三路复核`target`检索为零；最终提交前仍再次检查。
- 最终vendor与registry原版比较仍只命中批准的三个源码路径，本阶段相对阶段1 commit只修改`connection.rs`与`rtds/subscription.rs`；无Cargo/lock、第四vendor文件、公共shadow API、第二socket owner、生产feature、模型、部署或远端修改。one-shot vendor API、runtime wrapper、生产consumer及旧ticket测试继续同时存在，只在阶段3原子删除。
- 用户现有`AGENTS.md`和未跟踪`推荐开发顺序.md`继续排除。阶段2达到计划完成条件；精确commit/push成功后方可进入已授权阶段3。

## 2026-07-17：阶段3单次建连/握手期限缺口与用户批准CT-B

- 阶段3删除RTDS调用方20秒`wait_connected`存活门并把transport总attempts改为无限前，生产路径审查发现pinned SDK的唯一connection owner直接等待`connect_async(&endpoint).await`；SDK `Config`没有connect/handshake timeout。`tokio-tungstenite 0.29`的该future依次等待DNS/TCP、TLS与WebSocket HTTP upgrade，没有内建期限。现有调用方timeout只能drop wrapper，不能让仍卡在后台握手中的connection task观察last-sender关闭。
- 这不推翻“同一健康socket只订阅一次并在PING/PONG正常时无限等待业务数据”。缺口发生在WebSocket尚未建立、subscription和heartbeat都尚未启动的阶段：如果peer接受TCP但不完成TLS/HTTP upgrade，单次attempt永久pending，既不会触发heartbeat，也不会返回现有reconnect/backoff循环。
- 已批准需求同时要求每次connect有明确timeout、外层无限恢复、stop/deadline可收敛，并把vendor路径限制为三个文件；实现真实缺口因此触发第4.7.9项决策门。硬编码generic 20秒会隐式改变所有SDK消费者；复用heartbeat timeout或max backoff会混淆配置语义；调用方周期重建会遗留卡住的SDK task/socket，均未采用。
- 用户明确批准推荐`CT-B`：vendor路径集合扩为`connection.rs`、`config.rs`、`clob/ws/types/response.rs`、`rtds/subscription.rs`四个文件；`Config`新增默认`None`的`connect_timeout`保持上游默认兼容，RTDS生产显式设为20秒；唯一owner用Tokio timeout包围完整`connect_async()`，超时复用recoverable WebSocket timeout分类并在有界backoff后无限重试。该20秒绝不是连接寿命、业务frame等待、subscription refresh或周期重连期限。
- `CT-B`不新增独立cancel owner：若last sender在单次握手或既有backoff中释放，后台connection task的最坏收敛边界是剩余`connect_timeout`与有界backoff之和；连接attempt返回后、进入backoff前必须再次检查sender已关闭并直接停止，不能无条件多等一次backoff。该有界收敛取代此前可能永久卡在握手中的行为。
- 阶段3必须补TCP已接受但不完成WebSocket握手的loopback回归，并证明超时后下一次generation只replay一次完整`btc/usd` desired；阶段4再为CLOB显式启用同一20秒期限并保留`10s/20s` heartbeat。除这一获批配置字段和第4个vendor文件外，任何第5个vendor文件、其他新公共协议API、raw WebSocket、第二SDK owner或parser/fatal语义变化仍须重新停工决策。
- 决策前主线程只新增了两项未提交的Phase 3 production-source目标测试，没有修改生产或vendor实现、没有commit/push。用户批准后继续按测试先行实施；既有`AGENTS.md`与未跟踪`推荐开发顺序.md`仍属用户改动并排除。

## 2026-07-17：阶段3 RTDS 长期运行候选实现

- 阶段3先以生产source loopback建立红灯：旧实现面对启动期连续连接失败会耗尽SDK三次attempt并结束source；健康socket持续PONG但不发业务消息时会进入60秒等价freshness恢复流程，主动切换generation并最终结束source。两项失败分别证明transport总attempt预算和business-silence恢复预算仍在生产路径生效，不是测试server主动终止source。
- RTDS生产现在只写入一次完整`crypto_prices_chainlink` / `btc/usd` desired state，然后长期消费SDK事件。健康连接由`5s/15s` PING/PONG维护；stale与业务静默不再写业务`lag_events`、`source_gaps`，不刷新订阅、不主动切generation也不结束source。只有自然Close/EOF/heartbeat等transport故障由SDK唯一owner恢复；每个成功generation由既有actor replay一次当前完整desired。
- RTDS transport总attempt改为无限，max backoff保持5秒。用户批准的`CT-B`在SDK `Config`增加默认`None`的`connect_timeout`，RTDS生产显式设为20秒；唯一connection owner只对当次`connect_async()`施加期限，成功socket不携带该timer。超时进入既有recoverable/backoff循环，attempt返回后在backoff前再次检查last sender，从而不遗留永久卡住的握手future。
- 旧freshness episode常量、两generation/总期限状态机、business-timeout gap/lag/fatal、生产`request_reconnect`调用、runtime wrapper、vendor ticket/channel/API和纯ticket测试已在同一未提交阶段原子删除。原先实际验证generation隔离、资源、select公平、每代replay和freshness fence的测试改用真实server Close驱动；对生产、contract和vendor源码检索旧符号为0。
- 新生产回归覆盖四条路径：启动期失败超过旧预算后恢复并只发送一次完整desired；TCP已接受但不完成WebSocket握手时，单次期限关闭旧socket并在下一attempt成功replay；运行中Close后连续五次失败仍恢复且不写gap/lag；健康同一socket跨业务静默只收PONG，随后消费stale与fresh且generation/subscribe/reconnect计数不变。SDK contract另证明owner在半握手期间被drop后有界收敛、自然100次reconnect不增长task/FD或重复replay。
- 当前验证通过：`polymarket_sdk_patch_contract` 16/16、`polymarket_sdk_runtime_contract` 19/19、data-collect lib 200/200及全部integration/doc tests；`cargo check -p data-collect --tests --locked`、workspace `cargo clippy -p data-collect --locked --all-targets -- -D warnings`、root/vendor fmt和`git diff --check`均通过。vendored SDK `clob,gamma,rtds,ws` lib为168/168；`--all-features`为184/184 lib、全部integration及49项通过/20项ignored doctest。vendor all-features/all-targets Clippy在显式豁免原版既有`derivable_impls`与`let_underscore_untyped`两类基线告警后以其余warning deny通过；本阶段没有为清除无关基线修改额外vendor文件。
- 探索性运行不完整的`clob,gamma,rtds,ws --all-targets` feature组合会因上游示例直接引用未启用的`tracing`而失败；独立`rtds,ws,tracing`组合会命中原版`serde_helpers.rs`的`Value` feature-gate缺口；给Criterion bench传递libtest参数也会被bench runner拒绝。这些都不是适用的验收命令，已改用通过的`--all-features`默认test target集合及all-targets Clippy编译矩阵，没有修改`Cargo.toml`、示例、bench或第5个vendor文件。
- registry原版与vendor `src`逐文件比较只命中获批的四个路径：`clob/ws/types/response.rs`、`rtds/subscription.rs`、`ws/config.rs`和`ws/connection.rs`。本阶段没有新增依赖、Cargo/lock变化、第二socket owner、raw WebSocket、模型、部署或远端操作；用户现有`AGENTS.md`和未跟踪`推荐开发顺序.md`继续排除。三路独立终审、最终状态同步、vendor target清理及commit/push仍待完成。

## 2026-07-17：阶段3首轮范围终审修正

- 首轮生产语义与SDK生命周期终审均为Critical 0、Important 0；范围/文档终审发现两项Important。第一，未接生产调用图的公共`data-collect/src/sources/rtds.rs`仍保留`RtdsReconnectValidation` / `validate_reconnect_business_freshness`，并由`rtds_contract`正向要求stale产生`rtds_business_freshness_not_revalidated` gap；`source_scheduler_contract`也仍把`rtds_business_message_timeout`作为合法gap fixture。第二，四项新生产source测试都传入有限deadline，没有执行训练生产使用的`deadline=None -> pending`长期等待分支。
- 已手术式删除无生产caller、且只服务旧业务freshness-gap合同的公共helper与专属测试；`rtds_contract`因此从2项变为1项，保留subscription/raw typed合同。scheduler通用confirmed-gap能力不变，只把旧业务timeout fixture改为真实允许的SDK receiver `Lagged(1)` fatal gap及对应证据；当前生产`src`、contract tests和vendor `src`中的旧business-gap/helper/one-shot符号检索为0。
- 长期静默生产source测试现显式传入`deadline=None`，在同一socket完成PONG-only静默与stale/fresh消费后确认source未自行返回，再显式abort并断言task cancelled；server继续证明旧socket收敛且没有第二connection。其他三项source测试继续保留bounded deadline，分别覆盖启动离线、半握手超时和运行中自然恢复。
- 修正后复验通过：四项RTDS production source 4/4、`rtds_contract` 1/1、`source_scheduler_contract` 4/4、patch contract 16/16、runtime contract 19/19；workspace全量为data-collect lib 200/200及全部integration/doc tests通过，check/clippy、root/vendor fmt和`git diff --check`也均通过。范围终审二次结论在本节后续终审记录补齐。
- 聚焦审计另确认`data-collect/benches/real_source_alignment_probe.rs`是`harness=false`的独立Phase0验证可执行target，旧authority明确称其“不是生产collector”，本专项需求也明确“只修改training collector”；它不在training collector/live runtime调用图或默认test集合中。因此该probe现存的RTDS 60秒gap、`10s/20s`、三次attempt与20秒启动wait不属于阶段3修改范围。未来若重新把该probe作为当前有效工具使用，必须整体同步其配置、恢复、deadline和verdict并另行批准，不能只删一行timeout；阶段3不得声称bench源码已经迁移。

## 2026-07-17：阶段3最终终审通过

- 生产RTDS语义终审与SDK生命周期/provenance终审首轮即PASS；范围/文档终审首轮两项Important按上一节修正后复审PASS。最终三路均为Critical 0、Important 0；审查确认健康socket一次desired后无限等待、自然transport无限恢复、CT-B只限制单次握手、完整性错误不被吞掉、generation/freshness fence与每代一次replay保留。
- 最终主线程验证为：data-collect lib 200/200及全部integration/doc tests通过；patch contract 16/16、runtime contract 19/19、RTDS production source 4/4、`rtds_contract` 1/1和scheduler 4/4；workspace check与all-targets Clippy零告警，root/vendor fmt及`git diff --check`通过。vendored SDK `clob,gamma,rtds,ws` lib 168/168，all-features lib 184/184、全部integration及49项通过/20项ignored doctest；all-features/all-targets Clippy除明确豁免两类上游既有基线告警外其余warning deny通过。
- registry与vendor源码比较精确命中获批四路径，本阶段实际新增差异只在`ws/config.rs`和`ws/connection.rs`；旧one-shot、business-gap helper/fixture及生产freshness episode符号在training collector `src`、当前contract tests与vendor `src`中为0。无Cargo/lock、第五vendor文件、第二owner、raw Polymarket WebSocket、模型、部署或远端修改。
- 所有审查/验证结束后已用vendor manifest执行`cargo clean`，删除23,258个ignored构建文件/32.4GiB；vendor树内`target`目录复核为0。用户现有`AGENTS.md`与未跟踪`推荐开发顺序.md`继续排除。阶段3达到完成条件，精确commit/push后进入已授权阶段4。

## 2026-07-17：阶段4 CLOB/Gamma 长期运行候选实现

- CLOB生产路径删除`MarketAssetFreshnessWatchdog`、30/60秒业务静默阈值、主动subscription refresh、两次恢复/第三次退出预算及对应lag/gap/fatal。current与prefetched资产跨三个旧周期等价时长仍保持同一socket、同一generation和一次完整subscribe；业务消息只作为原始市场事实，不再重置或控制Gamma/transport recovery supervisor。成功安装完整subscription context才重置恢复backoff并清除recovery-active。
- CLOB继续使用`10s/20s` heartbeat，生产显式`connect_timeout=20s`。CT-B只包围单次TCP/TLS/WebSocket建连与握手；健康已订阅socket没有连接寿命或业务frame等待期限。SDK内部每轮有限attempt可以返回既有source-local supervisor，但外层总cycle和总elapsed无限，不构成collector存活预算。
- market subscription actor继续是唯一owner：同desired set不发wire操作，真实set变化只发精确add/remove；每个自然新generation只replay最新完整set。actor先绑定观察到的generation再取得manager，发送后复核connected state/generation；私有applied ack携带generation，waiter要求当前connected generation与revision同时匹配。两个精确竞态回归分别覆盖manager bind后/发送前切代及发送后复核/ack发布间切代。
- Gamma DNS/HTTP/rate-limit、合法`[]`、当前候选尚未出现及rollover暂时不可用统一为中性availability，按有界backoff持续恢复且不写confirmed gap。request使用20秒HTTP期限；每次响应完成、dedupe restore后及真正fence完成的context安装点都按处理时钟复核current/next window。initial expired返回可恢复availability；next expired重新调度；rollover expired保持既有window-end observation状态并立即重取，不安装旧context或伪造metadata。
- 独立审查发现并修正多项完整性漏口：retryable cleanup terminal不再追加伪gap；延迟Gamma响应必须在fence后最后安装点复核；metadata unavailable reason不能豁免`same_condition_market_verified=true`；NewMarket/MarketResolved必须整体核对Gamma market id、condition、canonical slug、完整且无重复/额外的二元asset set、Up/Down位置映射、winning asset/outcome，并在resolved缺slug时写已验证canonical slug。普通Book/BestBidAsk/LastTrade/TickSize/PriceChange的目标asset也必须属于消息envelope condition，外部coentry仍可过滤忽略。
- current/next Gamma pair现在要求四个asset跨窗口两两不重叠，防止`discovery_asset_to_slug`覆盖。inner rollover把旧prefetched提升为同slug current时，id/condition/up/down必须不变；outer CLOB transport cycle也保留有界`last_discovery`并对任意同slug current/next执行相同连续性检查。Gamma不可用跨过多个window且新旧slug不重叠时允许恢复，不把未知历史误判成identity conflict。
- 已删除公共`DiscoveryMissStats.source_gap_reason`及其只读测试断言，因为合法Gamma miss不再拥有或制造business source gap；schema、mapping、dedupe、queue、writer、JoinError及真实SDK Lagged等既有本地/确定性完整性边界未被availability循环吞掉。没有新增公开SDK API、第二socket owner、raw WebSocket、依赖、Cargo/lock、vendor源码或模型/部署路径。

## 2026-07-17：阶段4验证与最终终审

- 主线程最终验证通过：`cargo check -p data-collect --tests --locked`；`cargo clippy -p data-collect --locked --all-targets -- -D warnings`零告警；`cargo test -p data-collect --locked -- --test-threads=1`为210项lib、全部integration和doc tests通过；`polymarket_sdk_patch_contract` 16/16、`polymarket_sdk_runtime_contract` 18/18、`market_discovery_probe_contract` 7/7均包含在全量结果中；`cargo fmt --all -- --check`与`git diff --check`通过。
- 四source性能门在多个审查代理并发运行重测试期间曾单次出现一个2秒采样窗内Polymarket socket-to-processor backlog从0单调增至1,589；停止并发测试后，未修改阈值或生产代码的同一测试定向复跑为约4,684 native msg/s并通过，随后主线程独占的data-collect完整全量测试再次通过。现有证据与调度干扰一致，但不足以单凭一次失败证明因果；当前未发现可复现的生产吞吐回归，也没有为通过测试放宽门槛。
- registry 0.6.0与vendor `src`比较仍精确只命中已批准四路径：`clob/ws/types/response.rs`、`rtds/subscription.rs`、`ws/config.rs`、`ws/connection.rs`；阶段4没有修改其中任何文件。vendor本地`target`不存在，Cargo/lock无差异；旧CLOB watchdog、resubscribe和business-recovery fatal符号在当前生产/contract源码检索为0。
- CLOB/恢复终审、Gamma/metadata终审及SDK generation/范围终审最终均为PASS，Critical 0、Important 0。用户现有`AGENTS.md`与未跟踪`推荐开发顺序.md`继续排除；生产构建、部署、远端服务与数据清理仍未获授权。阶段4达到完成条件，精确commit/push后进入已授权阶段5。

## 2026-07-17：用户批准aggTrade v6 invalid evidence与阶段5扩展

- 用户复审了“外部无效输入丢弃后只记录缺失状态”的建议，明确否决静默丢弃：训练必须能够还原问题输入前后的因果队列，模型看到的是版本化invalid状态而不是伪造成交。用户随后批准先同步文档，再执行阶段5实现。
- 当前代码事实确认旧路径在WebSocket malformed JSON、缺失/错误event type及字段schema失败时直接返回terminal error，尚未构造`RawEvent`，因此原payload没有进入Parquet；现有v5 trade字段全部non-null，无法表达invalid。该事实与新批准目标及需求3.2“不修改aggTrade布局”、验收19“payload/schema负例仍terminal”直接冲突，本节决定精确取代这些旧authority。
- 新writer固定为既有`binance_aggtrade` artifact内部的`binance_aggtrade_v6`判别联合，`record_kind=trade|invalid_input`；invalid的trade-only字段为null，禁止0/空串/默认值伪造。连续相同坏帧逐条保存，不hash去重、不折叠计数；不新增sidecar、第七artifact、第二writer或训练进程dual-write。
- 上限内完整invalid保存exact raw bytes、encoding、长度、SHA-256、transport/endpoint/receive time、capture status与稳定reason code。capture cap固定`524_288` bytes并继续受1MiB typed-row hard bound；超限只保存真实已知的有界metadata，未取得完整bytes时不得伪造payload或digest。
- 三路只读审查共同指出，单一receive timestamp或单一sequence不足以重放。WS trade可先因ID hole进入pending，之后invalid先被queue接纳，旧trade在backfill失败后才释放；merged row又同时代表WS和REST两个接收事实。因此v6分别保存source sequence epoch、WS/REST response ingress coordinate、REST item index，以及queue/deferred ownership真正转移时冻结的collector admission time/sequence。前者审计原始输入，后者是offline replay与handoff的模型因果可见顺序；Parquet物理行序不得冒充任一者。
- 异步边界固定为“只等待现有global inflight预算、raw-day permit和bounded queue容量；ownership转移后继续读取，不等待Parquet append、flush、finalize、rename或fsync”。队列饱和仍允许现有最多2秒有界背压，queue timeout/closed、writer/manifest失败、sequence overflow、evidence ownership丢失和内部不变量仍terminal。当前`send_event`满队列时持有共享enqueue lock，故本轮不虚构“坏帧洪峰下其他source永不受阻”；若未来要求满载跨源公平，另建admission设计门。
- REST body将改为streaming hard cap。网络/HTTP/rate-limit仍走有界retry；已取得完整body但JSON/payload/数量/sequence损坏时不盲目重试同一坏响应。多页range后页损坏不能丢弃前页合法rows：先处理已取得rows，再保存当前invalid response、标记剩余range unresolved、开启新continuity epoch并继续未来live。
- ordinary invalid application input只重置全frame transport inactivity，不更新last qualifying、dedupe、aggregate-trade-id continuity或启动backfill。已解析的同ID内容冲突继续保留双方真实trade事实并记录confirmed conflict。v5 finalized文件只读为legacy trade，不能伪造epoch/sequence；v6恢复跳过invalid的nullable ID。queue接纳后、durable publish前崩溃仍由`unclean_end_unknown`界定，不得声称已落盘。
- typed raw中的raw bytes、hash、parser detail不直接进入模型；后续shared semantic canonicalizer只产生获准的canonical invalid marker、因果coordinate与ObservationContract字段。阶段5只实现/验证collector evidence和可关联handoff编码合同；当前paper/live hot仍只有heartbeat server，不冒充已经完成真实online source runtime或canonicalizer parity。
- 本决定已同步当前demand/plan及仍处审核状态的`demand/rebuild-core-demand.md`对应子边界；核心草案整体仍未批准，不创建其plan/changelog，也不借本轮批准其余重构内容。用户现有`AGENTS.md`与未跟踪`推荐开发顺序.md`继续排除；生产构建、部署、远端服务操作和数据清理仍未获授权。

## 2026-07-17：阶段5实现发现WS ingress sequence时点冲突并完成决策

- 实现接线与独立只读审查确认，已批准需求第4.5.9项要求合法WebSocket消息沿用既有结构去重后才进入ingress sequence域；已批准计划步骤5.3.2却要求WebSocket ingress coordinate在parse前冻结。对合法duplicate而言，两者不能同时满足：前者不应分配，后者会消耗一个不落盘的sequence并形成由本地去重制造的编号空洞。
- 与该冲突相关的Text生产分支已按方案不一致决策门暂停，没有擅自选边。进一步按双transport并发顺序复核后，推荐保留计划的parse前语义并修订需求：完整Text frame一经collector观察就立即取得真实ingress coordinate；invalid逐条留证，Unique/Conflict/OutsideWindow合法trade携带该coordinate，合法Duplicate仍按既有结构去重不产生typed/model evidence，但会留下由真实到达消息占用的编号空洞及既有有界dedupe计数。若改为dedupe后分配，WS parse期间并发完成的REST response可能先取得编号，使所谓ingress sequence实际退化为parse/dedupe完成顺序；要无空洞又保留跨transport真实顺序则需新增reservation/commit arbiter，属于不必要复杂化。
- 不受冲突影响的文档、v6 layout/writer、REST bounded evidence、handoff schema、collector admission ownership、oversize/Binary evidence及v5/v6恢复兼容候选实现已经继续推进；但阶段5尚未完成或获准commit/push。当前`cargo check -p data-collect --lib`唯一错误正是Text分支尚未传入待决的ingress coordinate；旧Text terminal测试和阶段5集成验收也必须在决策后改写并完整复验。
- 用户批准保留计划的parse前语义并修订需求第4.5.9项。最终合同明确：完整Text frame在任何parse前冻结coordinate；REST在有界body outcome形成后、application parse前冻结coordinate；合法Duplicate不产生typed/model evidence但不回收已消费编号，编号空洞由既有有界结构去重计数解释且不代表source缺失。该批准解除Text生产接线决策门，阶段5继续实施、测试和终审。

## 2026-07-18：用户批准aggTrade WS因果直录并删除实时ID-hole恢复

- 用户基于TCP/WebSocket同连接有序交付与Binance公开aggTrade字段合同，复审并否决了把`aggregate_trade_id`数值前跳自动解释为可恢复confirmed gap的旧设计。协议有序交付不证明服务端ID对单个订阅连接连续，官方也没有提供以`a+1`作为REST回填触发器的公开合同；因此保留旧backfill并不是必要可靠性措施，反而会把collector推断的完整性回插到真实可用观察流。
- 新批准合同是WS因果直录：每条合法WS aggTrade按真实到达顺序立即保存并handoff，`100 -> 103`不得等待、pending、重排或回插。原始`a`继续完整保存，供下游在另有ObservationContract批准时派生中性`observed_id_delta`；ID前跳本身不命名为gap，不告警、不主动重连、不结束source或collector。
- 生产路径删除`GET /api/v3/aggTrades`实时请求、REST backfill task、range retry、pending/merge/unresolved range和active aggTrade continuity coordinator。Close、EOF、read/write/protocol error、serverShutdown、24小时连接生命周期和全frame inactivity仍由同一source task持续重连；连接窗口中未实际收到的数据保持真实缺失。
- v6新生产只写WS `trade|invalid_input`。为避免无必要schema迁移，既有REST nullable列保留为读取兼容列，但新行必须全部为null；历史v5 REST-only/merged行继续只读。旧manifest已存在的`binance_aggtrade_continuity`字段原样保留并接受既有合法性校验，新binary不得创建、更新、从Parquet推导或用于控制运行。历史`backfill_rows`计数保留兼容但新运行始终为零。
- 三路代码/文档映射复核指出，既有`source_sequence_epoch_id`跨socket reconnect稳定，不能冒充连接代次；因此v6新增checked非零`connection_generation`。它只在成功WebSocket握手后递增，同一socket全部trade/invalid共享该值，失败attempt不分配，重连后严格增加；source epoch继续跨同一task的reconnect和UTC rollover稳定。
- invalid evidence、parse前WS ingress sequence、collector admission sequence、bounded queue/byte ownership、writer/manifest完整性与legacy读取保护不因删除backfill而放宽。新的因果示例固定为`trade 100 -> trade 103 -> invalid -> trade 104`原序接纳；合法duplicate仍消耗已冻结WS ingress sequence但不产生typed row，payload conflict仍保留双方真实事实。
- 本决定精确取代2026-07-17阶段5扩展中关于REST response ingress、pending trade后释放、unresolved range、new continuity epoch和REST body bounded retry的设计；历史条目作为决策过程保留，不再是当前实施authority。需求、计划和核心需求的aggTrade子边界已经同步；阶段5继续实现、测试、独立审查、commit和push。

## 2026-07-18：阶段5 WS-only实现与首轮审查修复

- 生产aggTrade已经改为单一WebSocket因果直录：合法trade及能够形成有界evidence的invalid frame立即进入既有bounded admission，并由contract test证明可编码为等价handoff frame，不因原始`a`前跳等待、回排、写gap、告警或切换连接。`GET /api/v3/aggTrades`、REST client/backfill task、pending/merge/range retry、unresolved range和active continuity coordinator已经从生产调用图删除；新运行的`backfill_rows`恒为零。
- `binance_aggtrade_v6`在同一artifact中封闭表达`trade|invalid_input`，保存source task epoch、成功握手后才分配的非零`connection_generation`、parse前WebSocket ingress coordinate及queue ownership转移时的collector admission coordinate。新writer只接受WebSocket evidence；历史v5 REST-only/merged row、v6 nullable REST列与旧manifest continuity只读兼容，不伪造legacy sequence/generation，也不参与当前运行控制。
- 接纳与写盘保持异步边界：source只等待既有global inflight预算、raw-day permit和bounded queue容量，不等待Parquet append、flush、finalize、rename或fsync。测试先暂停writer确认producer可把queue容量内的`trade/invalid/invalid/trade`全部转移ownership，再恢复writer并验证全部v6 rows落盘；queue timeout/closed、writer/manifest/schema/ownership等本地完整性失败仍按原scope terminal。
- 首轮独立审查发现两个真实缺陷并完成修复。其一，legacy恢复曾在识别`websocket:null`以前让REST-only row占用有界dedupe窗口，可能令较旧WS事实被limit截断；恢复现在先提取真实WebSocket payload再进入窗口，并增加limit=1回归。其二，writer只校验generation/admission而没有执行完整`row.validate()`，可能写出非法REST兼容列；共享writer校验现在调用完整validator，并增加拒绝REST compatibility值的负例。修复后的对应独立复审为Critical 0、Important 0。
- 新增真实loopback合同覆盖：失败WebSocket握手不消耗generation、全frame inactivity重连、reserved-opcode protocol error重连、同ID不同payload conflict双方留存、`100 -> 103 -> invalid -> 104`原序接纳无gap，以及连续100次TCP/WebSocket Close后仍由同一source owner恢复到generation 101且FD采样有界。100次恢复测试使用真实时间和仅在`cfg(test)`存在的1ms退避覆盖；最初把Tokio虚拟时间与真实TCP混用会令虚拟inactivity deadline抢先触发，该测试方案已撤销，生产退避与期限未改变。
- 自动静态回归扫描`data-collect/src`及crate依赖配置，确认生产路径没有aggTrade REST endpoint/client/task、backfill coordinator、pending或active continuity控制符号；legacy manifest字段及测试fixture中的兼容符号不属于生产控制路径。阶段5未增加第二socket owner、sidecar、第七artifact或shared canonicalizer，也未执行release构建、部署、远端服务操作或数据清理；用户现有`AGENTS.md`和未跟踪`推荐开发顺序.md`继续排除。

## 2026-07-18：阶段5验证与最终终审

- `cargo test -p data-collect --locked -- --test-threads=1`通过224项lib测试及全部integration/doc tests；新增自动静态回归所在`binance_aggtrade_continuity_contract`定向复跑5/5通过。`cargo check -p data-collect --all-targets --locked`与非测试lib检查通过；`cargo clippy -p data-collect --locked --all-targets -- -D warnings`零告警；`cargo fmt --all -- --check`和`git diff --check`通过。首轮Clippy发现的`needless_return`已改为尾表达式并复验通过。
- 阶段5单一aggTrade source的100次自然恢复与资源有界验收已经完成；计划阶段7仍负责多source组合、全部transport不可达和系统级FD/task/socket/queue压力，不重新引入aggTrade backfill或第二恢复路径。
- 三路独立终审分别覆盖生产调用图与恢复边界、v6 writer/legacy dedupe恢复，以及schema/generation/docs/自动静态回归；handoff生产接入范围和REST trap验收措辞两项Important均已按真实实现修正并复核关闭。最终结果为Critical 0、Important 0，需求、计划与changelog顶层状态已原子同步为阶段5完成；精确commit/push后进入已授权阶段6。
