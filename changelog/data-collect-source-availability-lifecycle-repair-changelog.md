# 训练采集全数据源可用性与连接生命周期修复 Changelog

> 事项编号：`DC-REPAIR-20260716-SOURCE-AVAILABILITY-LIFECYCLE`
>
> 对应需求：`demand/data-collect-source-availability-lifecycle-repair-demand.md`
>
> 对应计划：`plan/data-collect-source-availability-lifecycle-repair-plan.md`
>
> 状态：`PHASE_9_COMPLETE_RTDS_PONG_A`。阶段0至阶段8的实现、最终门禁、独立终审与历史批准均保持完成事实。2026-07-19香港线路wire验证发现真实RTDS不返回PONG，旧当前合同与运行事实冲突；用户明确回复“批准 RTDS-PONG-A”。阶段9现已完成本地文档、代码、测试、香港线路SDK复测、provenance核验及两路独立终审；用户后续批准的生产release构建、精确current-day删除、部署与远端服务操作改由`changelog/data-collect-rtds-pong-a-reset-deployment-changelog.md`记录。

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

## 2026-07-18：用户批准D6-SCHEMA-R并同步阶段6后续计划

- 阶段6实施前对真实代码和既有schema复核确认：生产depth路径在WebSocket握手后同步等待REST snapshot完成，之后才开始poll socket；snapshot为`binance_depth_snapshot_v2`，update为`binance_depth_update_v3`。当前stale/duplicate update在writer前直接continue，confirmed-hole update会先同步重新snapshot再重新判断同一update，因此现状既不能显式证明“snapshot pending时raw update先到”的观察顺序，也会丢失stale raw事实，并存在把hole update按后来的snapshot重新分类/应用的风险。这些是阶段6待修事实，不代表阶段5回退。
- 用户批准推荐`D6-SCHEMA-R`：继续使用`binance_depth_snapshot`和`binance_depth_update`两个既有artifact、writer与manifest mapping，不增加第三artifact或sidecar；schema分别升级为snapshot v3和update v4。两个schema共同新增`source_sequence_epoch_id`、`connection_generation`、`depth_ingress_sequence`、`collector_admission_sequence`及`collector_admission_time_utc_ms`，并保留既有官方payload、receive time与stitch字段。legacy snapshot v2/update v3只读为ordering unknown，不回填伪造坐标。
- raw observation与continuity availability明确分层。depth ingress在collector完整观察REST snapshot响应或WS application frame时按共享checked-u64域冻结；它保存真实观察顺序但不等于模型可见。snapshot已验证后先取得collector admission，snapshot pending期间buffered updates再按ingress顺序分类/release并取得admission；模型/handoff/offline replay不得把t2 snapshot才确定的stitch结果回写成t1模型可见事实，Parquet物理行序也不是authority。
- 每个成功WebSocket generation继续只有一个direct owner；唯一WS reader与至多一个active snapshot future并行，snapshot pending update进入同时受row/byte hard bound约束的单一generation-local buffer。snapshot最终不可用、可用前Close/EOF或generation放弃时，已观察且结构可验证的buffered updates必须以中性`not_applied_snapshot_unavailable`最终verdict按ingress有界release，不得静默丢失、虚构continuity-ready或伪造confirmed gap；无法形成可信typed row的parser/schema错误仍按既有完整性scope失败，本批准不新增depth invalid union。
- stale/duplicate与confirmed-hole update都必须保留update v4 raw row和not-applied verdict，但不得错误推进continuity或更新last-qualifying事实：前者不记gap，后者保留confirmed continuity evidence、立即失效旧snapshot anchor/`local_u`并重新snapshot；同一hole update不得在新snapshot后改写、回排或应用。首桥固定使用`U <= lastUpdateId + 1 <= u`，后续`U > local_u + 1`才是confirmed hole；公开文档叙述差异不改变本项目冻结的`+1`合同。
- 代码审计进一步确认collector当前只维护snapshot anchor与`local_u`连续性，不维护价位簿。D6中的`book-ready/book projection`因此统一澄清为continuity-ready/continuity projection：snapshot/update继续完整保存`bids/asks`与`b/a`供后续唯一shared canonicalizer重建，但阶段6不得新增collector price-level book、第二reconstructor或模型book实现。
- 成熟工具边界冻结为继续复用现有`reqwest + tokio-tungstenite + Tokio + Serde`及唯一Binance direct WebSocket owner。不引入官方/第三方Binance SDK、Barter、Nautilus或其他market-data engine；外部实现只作为只读研究/fixture参考。若未来改用partial-depth stream、改变首桥谓词、增加第三artifact/consumer schema、新依赖或替换owner，必须另建协议/schema/依赖决策门。
- 阶段7资源计划改为累计证据矩阵：阶段3已有SDK/RTDS 100-cycle，阶段5已有aggTrade 100-cycle，阶段6完成depth 100-cycle后均直接复用；阶段7只补尚未覆盖的CLOB生产actor与多源系统压力，不重建aggTrade或depth第二恢复路径。阶段8增加Cargo/lock与生产调用图静态门，证明没有新增Binance SDK/engine或第二owner。
- authority同步同时发现并修正`demand/data-collect-demand.md`残留的“aggTrade ID hole/REST backfill range”旧句；它与2026-07-18已批准并完成的阶段5 WS-only合同冲突。历史changelog中的旧backfill设计仍原样保留为决策演进，不是当前authority；superseded reliability demand/plan/changelog未修改。
- 本次只同步`demand/data-collect-source-availability-lifecycle-repair-demand.md`、对应plan/changelog、`demand/data-collect-demand.md`及核心需求中已批准的data-collect子边界；没有修改代码、阶段0--5执行结果、next_wake/模型策略、生产构建、部署、远端服务或数据清理范围。

## 2026-07-18：阶段6 D6-SCHEMA-R实施与审查修复

- 生产depth继续使用`binance_depth_snapshot`与`binance_depth_update`两个既有artifact、writer和manifest source mapping，schema分别升级为v3/v4。source task epoch、成功连接generation、snapshot/update共享depth ingress及collector admission sequence/time均为checked坐标；current writer拒绝版本/schema/部分坐标不一致，真实legacy v2/v3 Parquet fixture读取为ordering unknown，不补造current坐标。
- 每个成功generation仍只有一个direct WebSocket owner。唯一reader和至多一个active snapshot future在同一`select!`循环并行，pending buffer同时受row/byte hard bound约束；snapshot完整收到并验证后先admit，buffered updates再按ingress顺序分类/admit。snapshot不可用、Close/EOF、day/stop reset及terminal前的结构有效pending row均以中性`not_applied_snapshot_unavailable`只release一次，hour finalize只暂停而不破坏已有snapshot/continuity。
- stitch authority收敛为单一canonical状态机和四种封闭verdict。首桥固定`U <= lastUpdateId + 1 <= u`，后续先处理`u <= local_u` stale/duplicate，再以`U > local_u + 1`确认hole；stale与hole raw row都保存一次，只有applied推进`local_u`，hole立即令旧continuity失效且不在新snapshot后重分类。snapshot/update完整保留价位数组，但collector没有新增price-level book、第二reconstructor或shared canonicalizer。
- snapshot HTTP改为增量读取完整response body并在JSON parse前执行硬上限，坐标只在完整body观察后冻结；enriched typed row还经过真实owned-byte上限预检。408/418/429/5xx属于外部availability，其他4xx及snapshot/update schema、sequence、ownership和continuity invariant保持terminal；availability cycle使用有界内部jitter/backoff且不会耗尽source总生命。
- 独立审查发现`Retry-After`最初只保存在generation-local future，UTC day reset、Close/EOF或inactivity取消future后可能提前请求。修复为source-task级monotonic not-before gate：每次请求前等待，合法418/429秒数以max更新，future replacement与connection generation切换不清除，且不写manifest。跨取消真实HTTP回归证明429后abort旧future，800ms内无第二请求，期限后新future恢复。该边界依据[Binance官方REST限流说明](https://developers.binance.com/en/docs/products/spot/rest-api)。
- 其他独立审查修复包括：hour fence不得误取消Ready snapshot；terminal/owner-drop路径先中性release pending；snapshot/update必须完整validate；v4 verdict必须由raw事实重算而非信任输入；writer内部factory不能绕过schema/version；合法PING/PONG/Text按transport-positive处理且control resume后重武装persistent inactivity timer；外层control disconnect保持terminal。所有修复都位于既有owner/artifact范围内。

## 2026-07-18：阶段6验证与最终终审

- 真实loopback覆盖snapshot pending期间两个update先到、snapshot后到且先admit；stale、首桥、连续、confirmed hole、snapshot unavailable、buffer row/byte cap、Close/EOF、day/stop/hour fence、PING业务静默、全frame inactivity、serverShutdown、HTTP timeout/408/418/429/5xx/terminal 4xx、chunked body上限、sequence overflow及handoff parity。真实current Parquet round-trip保留全部五类坐标与官方/stitch字段，真实legacy文件不伪造ordering。
- 100次WebSocket reconnect测试由同一source owner达到generation 101，每代结构有效update都完成中性释放，FD采样有界；100次真实HTTP resnapshot测试在单一WebSocket generation中得到101个snapshot、100个applied update、100个confirmed-hole update及301个唯一ingress/admission事件，HTTP FD/socket有界。实现没有per-cycle/per-frame spawn，snapshot future和pending buffer都是同一source task内的单一局部owner。
- `cargo test -p data-collect --locked -- --test-threads=1`通过245项lib及全部integration/doc tests，depth定向31/31通过；`cargo check -p data-collect --all-targets --locked`、`cargo clippy -p data-collect --all-targets --locked -- -D warnings`、`cargo fmt --all -- --check`与`git diff --check`均通过。
- protocol/schema因果终审为Critical 0、Important 0、Minor 0；异步/资源终审为Critical 0、Important 0，并保留两个不影响任何生产读者、数据、manifest或恢复行为的非阻断机械清理候选；第三路authority/范围终审确认没有新增第三artifact、Binance SDK/engine、第二owner、price-level book、阶段7多源压力、阶段8全量交付或模型/runtime实现。阶段6因此标记完成，精确commit/push后进入已授权阶段7。

## 2026-07-18：用户批准D7/8-R并修订阶段7--8执行边界

- 阶段6完成后，用户要求重新核对较早编写的阶段7和阶段8是否仍符合当前实现。三路`gpt-5.6-sol/ultra`只读审查分别覆盖阶段7累计证据、阶段8最终门禁以及需求/计划/changelog跨文档一致性，结果均为Critical 0；用户随后明确批准推荐`D7/8-R`并同意继续执行。
- 审查确认当前拓扑必须分成三个不同计数域：四个producer owner为`polymarket`、`polymarket_rtds`、`binance_depth`、`binance_aggtrade`；六条外部I/O路径为Gamma HTTP、CLOB WebSocket、RTDS WebSocket、depth snapshot HTTP、depth WebSocket、aggTrade WebSocket；六个manifest counter继续对应既有artifact/source事实。需求与计划中把四个owner写成“四个transport”的旧表述已经修正。
- 阶段7改用“前序冻结证据＋新增差异证据”累计口径。阶段1--6已经完成的manifest lifecycle/compatibility、heartbeat与SDK配置、单source恢复及其fault/资源证据不重新实现；阶段7只新增完整collector下健康producer持续推进、代表性多源/全I/O退化、流式业务全静默、有界stop/deadline/day/hour控制矩阵、CLOB production actor与Gamma discovery/outer-supervisor各100-cycle，以及多源资源/terminal交互。测试不得增加public endpoint、shadow coordinator、第二owner、manifest字段或artifact。
- “业务全静默”不再被错误描述为四个transport简单不发数据：Gamma必须先返回有效稳定市场，depth必须先取得合法初始snapshot并完成CLOB subscription，随后CLOB/RTDS/depth/aggTrade四条WebSocket只维持协议存活而不产生合格业务事件。初始metadata/snapshot可以已经产生typed row，静默后counter停止增长即可。`active|idle|degraded`仍只是运行行为/诊断；nullable last-qualifying从typed raw或source-local事实因果取得，不是manifest字段。本事项的availability摘要只使用单一coverage并复用六个既有counter，其他既有manifest字段及职责保持不变。
- 阶段7不再把每个退化组合与每种停止方式做笛卡尔积，也不承诺共享queue已满时的跨source公平性。网络等待不得持有shared queue/enqueue lock/writer资源；queue真正饱和时继续采用既有有界背压与terminal timeout。terminal组合每次只注入一个可观测fault family，其他单source完整矩阵直接复用前序阶段证据。
- 需求原有“每条独立恢复协议至少100-cycle”与旧计划“只缺CLOB”发生不一致。用户批准保留较强需求：阶段7除CLOB production market actor外，还必须补Gamma discovery/outer-supervisor 100-cycle；阶段3 SDK/RTDS、阶段5 aggTrade和阶段6 depth WebSocket/HTTP证据继续复用，不另建第二恢复循环。
- 阶段8保留最终fmt、Clippy、串行全量`cargo test -p data-collect --locked -- --test-threads=1`、Cargo/lock与生产调用图检查、需求1--28追溯、累计独立终审及精确commit/push。vendor SDK改为条件门：始终核对官方crate hash、四路径diff、根patch映射、SDK版本/features、vendor Cargo/lock及dependency/feature图；全部身份未变才复用阶段3的批准生产feature集合`clob,gamma,rtds,ws`与`--all-features`矩阵，任一变化都重跑受影响矩阵，非批准变化先触发决策门。
- 阶段8新增最小的需求28交叉验收：同一批aggTrade `trade + invalid_input`及depth `snapshot + update` evidence必须同时进入finalized Parquet与对应handoff frame；handoff经过`to_json_line()`及反序列化后，与Parquet读取结果的全部共同语义字段一致。Parquet专属审计列无需复制，handoff `event_seq`不得冒充collector admission sequence。本验收只补测试，不批准新生产写入路径。
- 完整collector组合loopback只允许增加最小私有、必要时仅`cfg(test)`存在的runtime source-spec/factory与operator-stop注入seam；四个既有source future必须继续进入同一个真实production coordinator，公开runtime config、CLI、endpoint环境变量和生产source构造不变，禁止shadow coordinator、test API或绕过owner的合成事件注入。
- 旧计划要求“changelog记录阶段8自身commit SHA”存在Git自引用矛盾，现改为changelog记录阶段0--7 commit映射、最终测试/审查/scope与阶段8 commit subject；阶段8 push后由交付报告记录并验证`HEAD == upstream == remote`。审查修复只重跑受影响的阶段7场景和全部阶段8最终门禁，不因纯文档修改重复全部100-cycle。
- 需求第2.3节已改名为“修复前历史缺陷基线（截至2026-07-16）”，避免把阶段0--6已经修复的行为继续描述成当前事实。历史changelog中的旧方案与当时证据继续原样保留，用于记录决策演进，不作为当前authority。

## 2026-07-18：用户批准D7-TIME-R并关闭组合时钟冲突

- 阶段7实施审查发现，原步骤7.1.4要求全不可达和全流式静默的完整collector fixture都确定性跨越UTC day rollover，但同一已批准步骤7.0.3只允许私有source-spec/factory与operator-stop seam。真实runtime、四个producer wrapper及depth hour调度直接读取`SystemTime`；Tokio虚拟时间不推进该时钟，等待真实午夜/整点也不能形成确定性门禁。两项要求无法在不扩张范围时同时成立，因此按方案不一致决策门暂停。
- 用户批准`D7-TIME-R`：完整collector组合新增operator stop、run deadline与shutdown segment finalize证据；UTC day rollover和hour finalize分别复用阶段1/6已经冻结的真实owner/component测试及其PendingExit、activation ack、rollover rendezvous和unclean-dominates证据。不得新增全局或per-runtime clock seam，不得把组件累计证据冒充为组合fixture真实跨界。
- 该选择不降低需求第六节业务验收：两种全源场景仍必须完成operator stop，至少一个永久退化完整collector场景还要完成run deadline，至少一个存在健康typed row的退化场景必须在shutdown时真实finalize segment。它只删除计划中超出核心需求且与最小seam冲突的重复时间组合。

## 2026-07-18：用户批准D7-MIN-R并收缩阶段7组合测试

- 用户质疑阶段7完整collector组合测试及depth snapshot pending关闭校验是否过度设计，并要求先核对`/home/chillchill/poly-tx`与成熟实践。真实代码审计确认`poly-tx`在停止时直接结束收集循环并丢弃尚在内存中的snapshot pending，不存在避免该问题的隐藏机制；Binance官方只规定运行中snapshot/update-id同步流程，没有规定operator stop时必须保存应用内pending。Tokio同时支持“停止入口后drain已经接纳ownership”与应用明确允许的受控取消；这些证据支持缩小测试，而不支持删除全部关闭边界。
- 用户批准`D7-MIN-R`：阶段7完整collector新增证据从十三个case收敛为且只能为三个case：①全部外部I/O配置不可达＋operator stop；②protocol存活但业务全静默＋operator stop；③代表性Polymarket＋Binance跨平台多源退化＋健康typed row＋run deadline及shutdown segment finalize。全不可达case只证明四个owner在全部外部能力不可用时仍受共同collector监督；被Gamma/depth前置失败阻断的CLOB/snapshot I/O不得冒充实际连接失败，逐路恢复引用阶段3--6累计证据。
- 同时撤销未获批准的`D7-PENDING-A`全量pending持久化方案。新的最小边界以共同admission ownership为切点：已经取得raw-day permit并把ownership转移到既有bounded inflight/queue/deferred admission的event必须drain/finalize；仅位于Binance depth generation-local pending buffer、尚未取得共同admission ownership的update，在可靠collector-wide operator stop或run deadline关闭intake后可以清空，不生成row、gap、告警或terminal。training、paper、live因而都只看到真实已接纳事实，不补造unknown row。
- 该例外不得扩张到正常运行。snapshot unavailable、Close/EOF、普通reconnect/generation abandon、UTC day rollover与terminal前的结构有效pending仍按阶段6合同中性release；CapacityLimit、queue/writer/manifest、parser/schema内部不变量与ownership失败继续terminal。run deadline在watcher尚未调度但已到点的窗口也必须拒绝新unadmitted enqueue；若已有terminal close，后到的deadline不得把它静默改写为受控丢弃。
- 计划第7.3.2/7.3.3已同步改为复用阶段1/5/6的invalid、terminal、queue/writer与depth运行中恢复证据，不再拼接第四个完整collector场景；只新增共同gate截止和真实depth pending的聚焦stop/deadline回归。`demand/data-collect-demand.md`与`demand/rebuild-core-demand.md`同步采用相同admitted/pre-admission边界，避免专项需求与核心authority冲突；阶段6历史条目保留原样，本节只明确取代其中`stop reset`无条件中性release的旧范围。

## 2026-07-18：用户批准D7-DEFER-B-R并修正跨日staging边界

- 阶段7终审发现，跨日event在旧实现中会于新日activation前进入`DeferredRawBatch`并提前取得`collector_admission`，但该event尚无新日raw-day permit；若此时operator stop/run deadline形成PendingExit，producer stop路径又会删除该batch并增加`dropped_rows`。这同时违反“已接纳evidence必须drain”和“新日activation未提交时不得发布新日event”，不能靠简单增加组合测试调和。
- 用户批准`D7-DEFER-B-R`：共同inflight reservation与`DeferredRawBatch`只构成有界pre-admission staging；event必须同时取得对应raw-day permit与bounded queue ownership才成为已接纳事实并冻结collector admission坐标。正常rollover在新日activation后按staging顺序flush；PendingExit先获胜时不激活新日，可以清空staging且不生成row、gap、告警、`dropped_rows`或terminal。此前阶段5/6把deferred ownership本身称为admission的现行计划措辞由本节精确取代，历史记录保留不改写。
- 生产实现删除deferred入口的提前admission分配，把分配移动到flush取得新日permit和queue permits之后。aggTrade payload conflict与depth confirmed-hole gap改为随event携带的pre-admission副作用：当前日queue admission立即发布，跨日staging只在正常flush成功后发布，受控清空不会留下“没有raw row却存在gap”的伪证据。terminal flush/drop仍保留既有完整性计数。
- 聚焦回归覆盖两条边界：正常rollover证明staging内无permit/admission、flush保持顺序并在新日分配坐标；operator-stop PendingExit证明staging清空后inflight预算全部释放，queue、admission sequence、gap、`dropped_rows`及operational counters均不增长。阶段7旧“多源系统压力”残留措辞同步删除，完整collector仍严格只有三个批准场景。

## 2026-07-18：阶段7实现、终审修复与完成

- 完整collector默认测试严格保持三例：全部外部I/O不可达＋operator stop、protocol存活但业务全静默＋operator stop、跨平台多源退化＋健康typed row＋run deadline及shutdown segment finalize。四个production source future继续进入同一个真实coordinator；没有新增public endpoint、shadow coordinator、第四个组合场景、manifest字段或artifact。CLOB production market actor与Gamma discovery/outer-supervisor分别完成100-cycle，阶段3 RTDS、阶段5 aggTrade和阶段6 depth证据按批准累计复用。
- `D7-DEFER-B-R`生产边界已经落地：共同inflight reservation及尚无新日permit的`DeferredRawBatch`只提供有界pre-admission staging；正常rollover在新日activation commit后按staging顺序取得permit与queue ownership，再冻结collector admission并发布随event携带的aggTrade conflict/depth confirmed-hole gap。operator stop/run deadline在activation前或activation commit后的deferred flush期间先关闭unadmitted intake时，尚未取得最终permit的当前及剩余staging均中性释放；已经admit的event继续drain/finalize，terminal/CapacityLimit不使用该豁免。
- 首轮终审发现并关闭两个代码Important。其一，activation commit后、deferred flush尚未完成时到达受控截止，旧严格enqueue会把未接纳staging误报为terminal；close-bound与active-day admission现在都在持有enqueue fence后识别authoritative cutoff并返回中性结果，原聚焦测试增加真实SameDay operator-stop flush分支。其二，admission gap原来使用发布时`now_ms()`归档，队列/rollover延迟可能令row与gap跨UTC日；immediate与deferred路径现在都使用各event的`recv_time_utc_ms`调用`record_gap_at()`，同一rollover测试分别断言旧日immediate gap与新日deferred gap归属。
- 文档终审发现并修正核心需求与阶段5计划中“立即admission”的绝对措辞、旧系统级多source压力范围以及阶段7完成条件漏列跨日staging回归；当前authority统一为“当前日queue admission／跨日pre-admission staging／正常flush后admission／受控清空保持缺失”，历史changelog仍按日期保留并由后续批准明确取代。
- 最终验证：`cargo check -p data-collect --all-targets --locked`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo test -p data-collect --locked -- --test-threads=1`、`cargo fmt --all -- --check`及`git diff --check`均退出0；全量为254项lib及全部integration/doc tests。三路`gpt-5.6-sol/ultra`原审查者复核后均为Critical 0、Important 0、Minor 0。阶段7标记完成；阶段8已授权但尚未执行，生产构建、部署、远端服务和数据清理仍未获授权。

## 2026-07-18：用户批准D8-R并修订阶段8执行顺序

- 阶段8执行前的三路`gpt-5.6-sol/ultra`只读审查确认总体交付目标仍然有效，但发现两处现行文字歧义：专项验收19及aggTrade第10项仍把示例绝对写成“立即admission”，没有显式保留`D7-DEFER-B-R`跨日pre-admission staging例外；计划第8.1.5把depth current禁止版本笼统写成“v2/v3”，会字面误伤合法snapshot v3。用户批准`D8-R`：当前日立即queue admission，跨日只先staging并在正常flush取得新日permit与queue ownership后admission，受控截止保持缺失；depth current只禁写snapshot v2/update v3，snapshot v3/update v4是唯一current写入schema。
- 阶段8累计基线冻结为`e1b368561779b7f67d006b0dabc8a40632267d44`，阶段3 vendor基线为`e7d766dd7d7b3642b619746c8fb2a54857527e4a`，阶段7最终HEAD为`f53946537b7c3089971cbb667556704fbe75d6b0`。阶段3/current vendor tree均为`676ea8a0669166dafeee88c368bb20f42fc44c7d`，官方crate SHA-256期望为`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`；身份不变时条件复用阶段3vendor矩阵，不冻结Cargo展示文本hash。
- 同批evidence交叉验收收敛为两个聚焦合同测试：aggTrade `trade + invalid_input`与depth `snapshot + update`分别读取finalized Parquet，并将同一已接纳row编码为handoff、经过JSON roundtrip后只比较批准的共享字段。该测试只证明两种编码一致，不宣称已实现真实paper/live online source fanout、shared canonicalizer或完整runtime parity；不新增完整collector、network loopback、absence frame、生产路径或100-cycle。
- D7-DEFER必须显式进入生产调用图与28项追溯：pre-admission staging在最终flush前没有raw-day permit/admission，取得permit与bounded queue ownership后才冻结坐标和发布随event携带的gap；受控截止可以中性清空未接纳staging，已接纳event继续drain/finalize。复用阶段7现有聚焦测试，不补造不存在的handoff frame或drop/gap marker。
- 阶段8顺序改为先完成聚焦测试、静态审计、28项证据映射、自审、独立终审和反馈修复，再在最终Rust树运行一次fmt、Clippy及串行全量测试；开发期只跑受影响的定向测试。精确staging前审查working-tree diff，staging后再审查cached diff；用户现有`AGENTS.md`与`推荐开发顺序.md`始终排除。生产构建、部署、远端服务和数据清理仍未获授权。

## 2026-07-18：阶段8任务8.1聚焦测试与静态身份审计

- 新增且只新增两个test-only合同：`parquet_handoff_aggtrade_same_evidence_round_trips`把同一批`trade + invalid_input`写入并读取finalized v6 Parquet，再从同一rows构造handoff、执行`to_json_line()`与反序列化，并比较record kind、epoch、generation、WS ingress、admission sequence/time、receive time、host/region、source-native payload和invalid reason；`parquet_handoff_depth_same_evidence_round_trips`对两个既有artifact的snapshot v3/update v4执行相同流程，比较artifact record kind、epoch、generation、depth ingress、admission sequence/time、receive time及update stitch verdict。depth fixture保留“update先观察、snapshot先admission”的批准因果关系；handoff transport `event_seq`与admission sequence明确不同。
- 定向命令`cargo test -p data-collect --locked --test parquet_writer_contract parquet_handoff_ -- --test-threads=1`退出0，2项通过、0失败、0 ignored。测试只读取临时目录内已finalize的真实Parquet；没有修改生产API、writer、schema、artifact mapping、collector runtime或online fanout，也没有增加完整collector、network loopback、absence frame或恢复循环。
- 官方`polymarket_client_sdk_v2-0.6.0.crate`重新计算SHA-256为`e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`；阶段3与当前vendor tree均为`676ea8a0669166dafeee88c368bb20f42fc44c7d`，vendor `Cargo.toml/Cargo.lock`与官方原版逐字节相同，完整`src`差异仍恰为批准的`clob/ws/types/response.rs`、`rtds/subscription.rs`、`ws/config.rs`和`ws/connection.rs`四路径，vendor内没有`target/`。根patch、SDK `=0.6.0`及features `clob,gamma,rtds,ws`未变化，因此按计划条件复用阶段3 vendor production/all-features/integration/doc/Clippy/fmt矩阵。
- 阶段3后Cargo差异只有阶段5直接增加`base64 = 0.22.1`及data-collect lock dependency edge；该package/checksum此前已作为传递依赖存在，SDK package/feature图未变化。生产静态扫描确认`request_reconnect`、旧business-silence预算和aggTrade REST/backfill/pending/range-retry符号均为0。
- D7-DEFER生产调用图仍只有一个`DeferredRawBatch`/flush owner：Polymarket fanout与通用Binance路径先经过共同`begin_unadmitted_event_enqueue()`；deferred flush或current-day immediate路径取得对应raw-day permit与bounded queue ownership后才调用唯一`stamp_source_admission()`；两个路径的admission gap都随最终admission发布，并使用event `recv_time_utc_ms`归日。没有提前stamp、第二deferred owner或绕过bounded queue的发布路径。

## 2026-07-18：阶段8需求1--28逐项证据追溯

本节以阶段7最终树`f539465`已经通过的`cargo test -p data-collect --locked -- --test-threads=1`（254项lib及全部integration/doc tests）为累计结果，以本阶段上述2项定向通过为新增差异证据；阶段8最终统一复验在终审反馈关闭后执行并另记结果。测试位置按以下精确规则解读：除另标文件外，source/runtime测试位于`data-collect/src/live_runtime.rs::tests`；`dropping_market_runtime_closes_socket_without_reconnect`、`dropping_rtds_runtime_closes_socket_without_reconnect`、`connected_market_actor_sends_only_real_asset_set_diffs`、`disconnected_desired_updates_coalesce_to_one_final_subscription`及其余`market_*`/`rtds_*_natural_reconnect*` actor合同位于`data-collect/tests/polymarket_sdk_runtime_contract.rs`；`market_actor_drops_a_request_when_generation_changes_after_bind`与`market_waiter_rejects_ack_published_after_generation_cutover`位于`data-collect/src/polymarket_sdk_runtime.rs::tests`；manager/parser/heartbeat合同位于`data-collect/tests/polymarket_sdk_patch_contract.rs`；`depth_assessment_preserves_all_four_frozen_verdicts_without_reclassifying_holes`位于`data-collect/tests/binance_depth_stitch_contract.rs`；manifest合同位于`data-collect/src/daily_manifest.rs::tests`；Parquet合同位于`data-collect/tests/parquet_writer_contract.rs`。测试中的manifest/Parquet/JSON/FD等artifact均由真实临时文件、socket或采样值读取并在断言中验证，不以“全量通过”替代具体合同。

第10、20、24、26项使用的阶段8静态审计与结果如下，均在仓库根执行并取得预期结果；下列每项分别记录真实退出语义，不把“发现批准差异”或“零匹配”误写成原始命令退出0：

- 旧路径零匹配：外层执行`if rg -n 'request_reconnect|MarketAssetFreshnessWatchdog|rtds_business_message_timeout|rtds_business_freshness|BINANCE_AGGTRADE_REST|/api/v3/aggTrades|AggTradeBackfill|BudgetedAggTradeBackfill|PendingAggTrade|AggTradeCoordinator|fetch_binance_aggtrade_range|active_backfill' data-collect/src third_party/polymarket_client_sdk_v2-0.6.0-p1/src Cargo.toml data-collect/Cargo.toml; then exit 1; fi`，内层`rg`返回0行/退出1，外层审计退出0。
- collector静默/模型timer零匹配：分别对`sed -n '1,18522p' data-collect/src/live_runtime.rs`和其他`data-collect/src/*.rs`执行同样的否定断言，检索`DecisionWakeEvent|subscription_refresh|business_silence|business.*silence|fixed.*tick|silence.*timer`；两个生产pathset均返回0行，外层审计退出0。
- D7-DEFER生产调用图：对`data-collect/src/live_runtime.rs`测试模块以前的`1..18522`行检索`DeferredRawBatch|defer_raw_batch|flush_deferred_raw_events|begin_unadmitted_event_enqueue|stamp_source_admission|record_gap_at`；结果为单一定义/flush owner、两个最终stamp调用点和按event receive time发布gap，详细行号已记录于本节任务8.1结果。
- SDK身份：以`/home/chillchill/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/polymarket_client_sdk_v2-0.6.0.crate`为官方crate，执行`mkdir -p /tmp/polymarket-sdk-official-phase8`与`tar -xzf /home/chillchill/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/polymarket_client_sdk_v2-0.6.0.crate -C /tmp/polymarket-sdk-official-phase8`后，`sha256sum`退出0并得到`e0ed9ca...ed5e`；`git rev-parse e7d766d:third_party/polymarket_client_sdk_v2-0.6.0-p1 f539465:third_party/polymarket_client_sdk_v2-0.6.0-p1`退出0且两项均得到`676ea8a...c7d`；`cmp /tmp/polymarket-sdk-official-phase8/polymarket_client_sdk_v2-0.6.0/Cargo.toml third_party/polymarket_client_sdk_v2-0.6.0-p1/Cargo.toml`与同样两个`Cargo.lock`路径均退出0；`git diff --no-index --name-only /tmp/polymarket-sdk-official-phase8/polymarket_client_sdk_v2-0.6.0/src third_party/polymarket_client_sdk_v2-0.6.0-p1/src`按“存在差异”语义退出1，输出恰为批准的`clob/ws/types/response.rs`、`rtds/subscription.rs`、`ws/config.rs`与`ws/connection.rs`四路径；`cargo tree -p data-collect --locked -e features -i polymarket_client_sdk_v2`退出0，只显示本地`0.6.0`及`default,clob,gamma,rtds,ws`。
- depth唯一owner与成熟依赖边界：`if rg -ni '(^|[^[:alnum:]_])(binance[-_ ]?(sdk|connector|api)|barter|nautilus)([^[:alnum:]_]|$)' Cargo.toml data-collect/Cargo.toml Cargo.lock; then exit 1; fi`的内层`rg`返回0行/退出1，外层审计退出0；`sed -n '1,18522p' data-collect/src/live_runtime.rs | rg -n 'connect_async\(&spec\.websocket_endpoint\)|async fn run_binance_depth_update_ws_source_with_control|let mut snapshot_future = Some\('`退出0且恰返回三个生产锚点：一个depth direct WebSocket connect（第13103行）、一个update reader/owner定义（第13489行）和该owner内唯一`snapshot_future`声明（第13502行）。`18522`是本阶段冻结树中顶层`mod tests`前一行，因此pathset排除该顶层大测试模块内的调用与fixture。

1. **全部源持续运行：** `rtds_source_keeps_one_socket_through_silence_and_consumes_stale_then_fresh`、`clob_current_and_prefetched_assets_survive_three_legacy_silence_cycles_on_one_socket`、`binance_ping_keeps_socket_alive_during_business_silence`及`binance_depth_ping_frames_keep_a_business_silent_socket_alive`直接冻结四条WebSocket业务静默。Gamma采用批准的组件组合证明而不冒充单一长loopback：`gamma_candidate_selection_separates_missing_from_contradictory_identity`证明合法空候选进入`Unavailable`，生产调用图证明该结果进入同一outer availability分支，`gamma_outer_supervisor_survives_one_hundred_outer_cycles_with_bounded_fds`证明该分支持续100轮，`polymarket_source_recovers_through_gamma_network_rate_limit_and_empty_response`另以一次真实`[]`证明不产row/gap且随后可恢复有效市场。
2. **逐源同PID恢复：** `rtds_source_survives_startup_unavailable_then_applies_one_complete_desired`、`rtds_one_hundred_natural_reconnects_keep_one_actor_and_replay_per_generation`、`gamma_outer_supervisor_survives_one_hundred_outer_cycles_with_bounded_fds`、`market_one_hundred_natural_reconnects_keep_one_actor_and_replay_per_generation`、`binance_one_hundred_reconnects_keep_one_source_loop_and_reach_generation_101`、`binance_depth_one_hundred_reconnects_reach_generation_101_with_bounded_fds`及`binance_depth_one_hundred_resnapshots_keep_one_owner_and_bounded_fds`覆盖各owner累计恢复。
3. **跨平台退化：** `full_runtime_cross_platform_degradation_converges_at_run_deadline_and_finalizes_rows`证明健康typed row继续进入最终segment，并由run deadline收敛。
4. **三例完整collector：** 默认集合严格只有`full_runtime_all_io_unavailable_stays_live_until_injected_operator_stop`、`full_runtime_protocol_alive_business_silence_does_not_fabricate_rows_or_reconnect`及第3项跨平台场景；没有第四个完整collector。
5. **coverage、activation与截止：** `startup_gate_collects_ten_ready_before_activation_and_first_source_poll`、`dataset_root_lock_allows_only_one_manifest_owner_process`、`activation_transitions_all_four_states_and_sets_reactivation_fact`、`normal_rollover_waits_for_four_producers_and_six_writers`及`pending_exit_rollover_closes_old_day_without_activating_new_day`共同覆盖manifest；`controlled_rollover_cutoff_discards_pre_admission_staging_without_drop_evidence`、`controlled_runtime_cutoff_discards_only_unadmitted_events`、`binance_depth_operator_cutoff_discards_unadmitted_pending_and_stops_cleanly`及`binance_depth_run_deadline_discards_unadmitted_pending_without_gap`证明未接纳staging没有admission或queue row，因此没有writer输入或可finalize Parquet row，同时不生成gap、告警、drop/operational counter或terminal；已接纳event继续drain。handoff侧absence只源于未接纳事实及当前没有production source fanout，不是online parity验收。
6. **错误分类边界：** `source_recovery_outer_cycle_is_nonterminal_and_gap_neutral`证明外部故障不耗尽生命；`rtds_whole_frame_parser_fault_is_sticky_and_terminal`、`disconnected_writer_channel_is_a_runtime_error`、`writer_parser_failure_is_reported_when_workers_stop`、`source_abort_requires_one_exact_task_generation_close_permit`、`matching_abort_permit_never_masks_a_source_task_panic`及`startup_pre_rename_manifest_failure_keeps_disk_unmodified_and_converges`证明parser/schema、writer channel、panic/abort及manifest/atomic publish负例继续按受影响scope terminal或unclean。
7. **RTDS业务静默：** 第1项RTDS测试跨越旧静默周期仍消费同generation的stale后fresh事件。
8. **RTDS启动不可达：** `rtds_source_survives_startup_unavailable_then_applies_one_complete_desired`证明恢复后只表达一次最新完整desired。
9. **RTDS运行中transport失败：** `rtds_source_recovers_after_close_and_failures_without_gap_or_replay_duplication`与`rtds_source_retries_after_a_stalled_websocket_handshake`证明同一owner持续恢复且不耗尽总预算。
10. **删除one-shot reconnect：** 阶段8对`data-collect/src`、批准vendor路径及Cargo执行静态`rg`，`request_reconnect`匹配为0；阶段3删除合同继续由SDK patch contract覆盖。
11. **generic heartbeat：** `clob_connection_reconnects_when_heartbeat_pong_is_missing`及vendor `ws::connection::tests::old_pong_is_a_recoverable_heartbeat_timeout`证明heartbeat timeout仍recoverable，迟到PONG不能复活旧连接。
12. **manager drop与反向transport：** `dropping_last_connection_manager_closes_socket_without_reconnect`、`dropping_market_runtime_closes_socket_without_reconnect`及`dropping_rtds_runtime_closes_socket_without_reconnect`证明旧socket关闭且不重连；`one_hundred_natural_reconnects_keep_one_connection_loop_and_heartbeat`明确覆盖server `Close`后的自然恢复，`server_tcp_eof_is_recoverable_and_advances_generation`覆盖EOF，vendor `outbound_write_failure_is_recoverable`及`heartbeat_ping_write_failure_is_recoverable`覆盖反向write/PING恢复；`heartbeat_task_panic_is_terminal_and_does_not_reconnect`明确覆盖heartbeat内部失败的terminal反例，`rtds_whole_frame_parser_fault_is_sticky_and_terminal`继续覆盖parser terminal反例。
13. **CLOB heartbeat：** `clob_connection_reconnects_when_heartbeat_pong_is_missing`与`websocket_heartbeat_configs_are_channel_specific`共同证明CLOB `10s/20s`未被RTDS `5s/15s`覆盖。
14. **CLOB静默：** 第1项CLOB测试跨三个旧周期保留current/prefetched订阅和同一socket。
15. **CLOB lifecycle/generation：** `connected_market_actor_sends_only_real_asset_set_diffs`、`disconnected_desired_updates_coalesce_to_one_final_subscription`、`market_natural_reconnect_replays_only_latest_complete_desired_set_once`、`market_actor_drops_a_request_when_generation_changes_after_bind`及`market_waiter_rejects_ack_published_after_generation_cutover`共同覆盖applied ack隔离。
16. **Gamma不可达/空候选：** `polymarket_source_recovers_through_gamma_network_rate_limit_and_empty_response`、`gamma_late_current_or_next_response_cannot_be_installed_after_window_advance`及`gamma_outer_supervisor_survives_one_hundred_outer_cycles_with_bounded_fds`共同覆盖过期slug淘汰与当前窗口重算。
17. **metadata完整性：** `gamma_rate_limit_is_availability_but_malformed_json_is_integrity`、`gamma_candidate_selection_separates_missing_from_contradictory_identity`及`polymarket_lifecycle_identity_contradictions_are_terminal`保持fail closed。
18. **aggTrade业务静默：** 第1项aggTrade PING测试证明有protocol frame但无trade不重连、不写gap，首条后续trade正常消费。
19. **aggTrade因果接纳/invalid：** `binance_handshake_failure_recovers_without_allocating_a_connection_generation`、`binance_frame_inactivity_reconnects_with_stable_epoch_and_no_gap`、`binance_protocol_error_reconnects_in_process_without_gap`、`binance_aggtrade_id_jump_is_forwarded_immediately_without_reconnect_or_gap`、`binance_aggtrade_payload_conflict_preserves_both_facts_and_records_conflict`、`binance_aggtrade_invalid_text_frames_are_preserved_and_same_socket_continues`及`data-collect/tests/binance_aggtrade_continuity_contract.rs::production_has_no_aggtrade_rest_recovery_path`覆盖生产语义；本阶段`parquet_handoff_aggtrade_same_evidence_round_trips`补上同一已接纳row的finalized Parquet/handoff编码一致性。
20. **depth静默与唯一owner：** `binance_depth_ping_frames_keep_a_business_silent_socket_alive`、`binance_depth_reconnect_is_transport_only_and_preserves_snapshot_trigger`及`binance_depth_one_hundred_reconnects_reach_generation_101_with_bounded_fds`覆盖运行；本节静态审计的Cargo/lock否定断言与三锚点生产调用图精确确认仍只有既有`reqwest + tokio-tungstenite + Tokio + Serde`、一个depth direct WebSocket owner及该owner内一个active `snapshot_future`，没有新Binance SDK/engine或第二owner。
21. **depth双artifact因果顺序：** `typed_writer_depth_v3_v4_round_trip_preserves_ingress_and_admission_coordinates`、`real_legacy_depth_v2_v3_parquet_reads_without_fabricated_ordering_coordinates`、`binance_depth_snapshot_admits_before_two_earlier_ingress_updates`、`depth_assessment_preserves_all_four_frozen_verdicts_without_reclassifying_holes`及`binance_depth_ingress_and_admission_sequences_fail_closed_on_overflow`覆盖raw/stitch；本阶段depth交叉测试补上同一evidence的handoff编码侧，不比较未获批准的depth payload词法。
22. **snapshot恢复与受控截止：** `binance_depth_snapshot_timeout_exhaustion_is_availability`、`binance_depth_snapshot_honors_retry_after_before_next_request`、`binance_depth_retry_after_survives_snapshot_future_replacement`、`binance_depth_eof_releases_pending_update_once`、`controlled_runtime_cutoff_discards_only_unadmitted_events`、`binance_depth_operator_cutoff_discards_unadmitted_pending_and_stops_cleanly`与`binance_depth_run_deadline_discards_unadmitted_pending_without_gap`共同覆盖snapshot pending与可靠截止；跨日staging另由`controlled_rollover_cutoff_discards_pre_admission_staging_without_drop_evidence`证明无伪证据，没有新增absence handoff。
23. **manifest、v6与训练基础：** `collection_day_coverage_strictly_validates_schema_state_and_day_bounds`、`activation_transitions_all_four_states_and_sets_reactivation_fact`、`prior_day_reconciliation_is_state_aware_and_legacy_neutral`、`typed_writer_binance_aggtrade_schema_keeps_distinct_event_and_trade_times`、`typed_writer_binance_aggtrade_same_timestamp_preserves_admission_order_across_batches`、`binance_aggtrade_v5_recovery_reads_real_legacy_schema_without_synthetic_coordinates`及`binance_aggtrade_v6_recovery_skips_invalid_input_rows`已覆盖；本阶段`parquet_handoff_aggtrade_same_evidence_round_trips`补强真实`trade + invalid_input` finalized Parquet与handoff的共享编码。M8仍只是未来部署合同。
24. **无collector静默timer：** 上述静默测试证明运行行为；阶段8静态扫描确认生产调用图没有business-silence watchdog、周期refresh、`DecisionWakeEvent`或fixed model tick。heartbeat status不冒充业务行情。
25. **资源边界：** 第2项各owner 100-cycle及generic SDK `one_hundred_natural_reconnects_keep_one_connection_loop_and_heartbeat`，连同`global_inflight_budget_enforces_exact_production_boundaries_and_releases`、`short_source_queue_backpressure_waits_without_dropping`、`aggtrade_max_websocket_invalid_releases_constrained_budget_after_queue`、`binance_depth_pending_buffer_enforces_byte_cap_independently_of_row_cap`、`binance_aggtrade_binary_is_exact_and_oversize_is_bounded_before_reconnect`及D7 cutoff测试共同覆盖；阶段8不另跑独立100-cycle矩阵，最终默认全量只自然执行一次。
26. **SDK生产身份：** `websocket_heartbeat_configs_are_channel_specific`、`websocket_handshake_timeout_retries_without_exhausting_the_owner`及vendor `ws::config::tests::default_heartbeat_is_five_seconds`已覆盖；阶段8重新核对crate SHA、四路径allowlist、patch/version/features、vendor Cargo/lock及feature图均未变，因此合法复用阶段3矩阵。
27. **最终门禁：** 阶段7门禁已经退出0；阶段8在全部代码/测试反馈关闭且三路终审Critical/Important均为0后，最终`fmt`、累计/本事项diff、Clippy及串行全量测试均退出0，具体结果记录于本节后的阶段8完成记录。
28. **finalized typed raw/handoff交叉验收：** 既有row-to-evidence和独立JSON测试由本阶段两个`parquet_handoff_*_same_evidence_round_trips`闭环；两项定向命令2/2通过，只证明已接纳row共享字段的编码一致，不宣称真实online producer fanout、shared canonicalizer或完整paper/live runtime parity。

## 2026-07-18：用户批准D8-STATUS-A并关闭核心入口状态冲突

- 阶段8生产/authority终审发现`demand/data-collect-demand.md`的当前authority顶注仍写“阶段8尚未执行”，与专项demand、plan、changelog已经进入`PHASE_8_IN_PROGRESS`及本changelog已记录任务8.1事实冲突；阶段7结果段中的同类文字是历史事实，不属于冲突。
- 用户在明确获知两个选项后批准推荐A：核心入口不再复制易过时的当前阶段状态，改为“阶段8及后续实施状态、验证证据和完成结论以专项需求、计划与changelog的最新记录为准”。该选择只修正文档authority，不改变任何代码、测试、运行或部署语义；后续阶段转换无需反复改写核心入口。

## 2026-07-18：阶段8最终门禁、审查与本地交付完成

- 阶段8只新增`data-collect/tests/parquet_writer_contract.rs`中两个test-only交叉合同，并同步需求、计划与changelog；没有修改`data-collect/src`、Cargo manifest/lock、vendor/third-party、生产runtime、schema或artifact mapping。两项测试只证明同一已接纳source row的finalized Parquet与handoff JSON共享字段一致，不声称online fanout、shared canonicalizer或完整paper/live runtime parity已实现。
- 最终门禁按批准顺序在最终Rust树执行一次：`cargo fmt --all -- --check`退出0；`git diff --check e1b368561779b7f67d006b0dabc8a40632267d44..f53946537b7c3089971cbb667556704fbe75d6b0 -- .`与本事项五文件pathspec的working-tree `git diff --check`均退出0；`cargo clippy -p data-collect --locked --all-targets -- -D warnings`退出0且无warning；`cargo test -p data-collect --locked -- --test-threads=1`退出0。
- 最终串行全量测试的外层harness结果为254项lib与40个integration test binary共231项全部通过（0失败、0 ignored）；三个binary unit harness与doc-tests均退出0且各有0项。`parquet_writer_contract`为17/17，包含本阶段两项`parquet_handoff_*_same_evidence_round_trips`。
- 最终门禁前的生产/范围、测试/证据、文档/authority三路`gpt-5.6-sol`/`ultra`独立终审均为Critical 0、Important 0、Minor 0。反馈修正仅收紧文档证据的真实退出码、路径、测试名与组合证明边界；没有因审查新增生产实现或重复扩张完整collector/100-cycle矩阵。
- 阶段0--7 commit映射为：阶段0 `ab43d05054705e21973c491fb0e9b966c046543f`，阶段1 `b1172fe52261b412dfd26471b080f7ccbaafbd0e`，阶段2 `75aeb543aaecbd3cd135f754d8c09135c54d2441`，阶段3 `e7d766dd7d7b3642b619746c8fb2a54857527e4a`，阶段4 `387ba0394d94b37fcfe0a2c8d09078e5b03a532d`，阶段5 `f40177d103e9529a586c798624c2e2958e039dba`，阶段6 `982e92f457300fb63914ef499491c78b8a683f88`，阶段7/8门禁细化文档 `689273f9f02539bcc1d07ecb4eadaef742807340`，阶段7 `f53946537b7c3089971cbb667556704fbe75d6b0`。本阶段提交subject冻结为`test(data-collect): complete source lifecycle phase 8`；commit SHA与push后本地/upstream/remote ref一致性由交付报告记录，不在commit自身伪写尚未存在的SHA。
- 阶段8完成只表示本地需求、计划、实现差异测试、证据追溯与门禁闭环。本次没有执行release构建、生产部署、远程服务/数据写操作或数据清理，也不授权这些后续操作。

## 2026-07-18：独立直接部署事项取代M8-A首次启用编排

- 用户在阶段8完成后独立批准`DC-DEPLOY-20260718-DIRECT-LONG-RUN`。已确认数据采集服务器多日停服、旧PID/lease退出且当前UTC日没有manifest，因此首次正式冷启动可在任意时刻直接启用唯一`Restart=on-failure` unit，不等待UTC边界，不使用`Restart=no`或独立gate，也不删除历史数据。
- 该后续批准只取代M8-A的首次启用编排；本专项阶段0--8实现、测试与历史批准记录不变。same-day不同identity takeover、回滚或identity migration仍不是已实现能力。
- 当前执行authority为`demand/data-collect-training-collector-direct-deployment-demand.md`、`plan/data-collect-training-collector-direct-deployment-plan.md`及其changelog。

## 2026-07-19：香港线路15分钟RTDS wire事实与合同冲突

- 一次性诊断程序通过显式`socks5h://127.0.0.1:20810`连接官方RTDS，代理DNS、TLS SNI和官方subscription payload保持目标域名语义；该程序只用于线级观察，位于gitignored `target/`，未进入生产调用图、Cargo/lock或提交范围。路线回执在测试前后均为`8.210.176.1 / HK / HKG`，WebSocket Upgrade为101；`poly-arb-second-proxy.service`测试后仍为`active/running`、PID `7729`、`NRestarts=0`，启动时间未变化。
- 主测试在一个WebSocket上持续`900001ms`，立即发送首条、之后固定每5秒发送大写文本`PING`：共181次、180个间隔，min/p50/max分别为`4999/5000/5001ms`，均值`5000.006ms`。收到891个文本帧：1个空文本、1个subscribe snapshot、889个`crypto_prices_chainlink/update`；889个update共享同一`connection_id=gUhj7C6s_WeIKEh-DA==`。文本`PONG`、文本`pong`、WebSocket控制Ping/Pong、binary、服务端Close/error/EOF均为0。
- 两次3秒对照把首条client PING延后到5秒，因此实际发送PING为0；两次仍分别在约238ms和243ms收到同样空文本。结合vendored RTDS parser已把空/whitespace定义为server keepalive，空文本不能被解释成PING响应或PONG。短对照只用于空帧归因，不外推“长期无需PING”。
- 原始证据摘要：主JSONL `target/rtds-wire-evidence/rtds_wire_hk_1784442709972.jsonl`为1077行/362149 bytes，SHA-256 `9e5b13a1b3209c55ec6b26c6a61727c6f38a105daf696f8506db8dc2b5715462`；无PING对照分别为`bdef9ae5a10eefed1a38ab8d20fc7c2c6fd2c359849135c5d59277231eef2391`和`5e2a3f1e7195b8470434cbcdd8adf9e71280894265def90ca89630dad2621a78`；路线回执SHA-256为`4533b4def323eb37cb962e346aac07e4bf012528fcbc725768822aeb7415c90d`。独立`gpt-5.6-sol/ultra`线级审查重算全部统计后PASS，Critical/Important/Minor均为0。
- 真实程序的旧状态机在每个heartbeat tick发送一次PING后串行等待最多15秒精确文本PONG，因此等待期间不会在第5秒和第10秒继续PING；no-PONG后将健康socket错误分类为recoverable timeout。此前UTC日3068个成功RTDS reconnect generation平均约16.147秒，与15秒timeout加约1秒初始backoff吻合。该代码事实与官方“每5秒PING”、上述同socket业务持续且零PONG的wire事实不能同时满足。
- 当时当前demand/plan仍要求RTDS no-PONG关闭重连，并把`5s/15s`写成RTDS PING/PONG基线；`CT-B`又只批准过`connect_timeout`公开字段。因此主线程按冲突门停在本地，没有改生产代码、没有操作远端或数据。旧changelog中2026-07-17关于RTDS PING/PONG与no-PONG重连的文字继续作为当时历史事实，不回写删除；其CLOB部分继续有效。

## 2026-07-19：用户批准RTDS-PONG-A并启动阶段9

- 用户在收到冲突位置、wire统计、实现选项和范围后明确回复：“批准 RTDS-PONG-A。”新合同为：SDK `Config`新增`require_text_pong: bool`且默认`true`；CLOB生产显式`true`并保持`10s/20s`、精确文本PONG及no/old-PONG恢复；RTDS生产显式`false`并固定每5秒发送大写文本PING，不等待PONG、不因no-PONG重连。共享结构中的RTDS `heartbeat_timeout=15s`保留为不生效兼容值，不得写成官方SLA。
- false模式每条PING必须由唯一connection owner实际完成WebSocket write后通过私有oneshot确认；确认前heartbeat不创建第二个PING，确认后重置完整interval，防止写阻塞期间无界积压和解除阻塞后的catch-up burst。Close、EOF、read/write/PING-send failure仍recoverable；parser sticky fault仍terminal；last-manager drop仍normal且不重连。
- 批准范围限于既有四文件vendor allowlist中的`ws/config.rs`与`ws/connection.rs`、生产`live_runtime.rs`、直接合同测试及同事项文档；不新增第五个vendor文件、raw Polymarket WebSocket、manual signing、第二SDK owner/session、shadow API、Cargo依赖或parser/fatal变化。
- 测试先行红灯真实退出101：新增loopback首先因`Config`不存在`require_text_pong`而编译失败。候选实现后，RTDS direct manager no-PONG固定cadence测试转绿；生产source no-PONG静默后同socket消费stale/fresh转绿；vendor两个send-only测试证明跨多个interval只保留一个未完成PING、ack后无burst，且false模式PING write failure recoverable。完整矩阵、香港线路修复后15分钟复测、独立阶段审查、最终状态与commit/push仍待完成，当前不得标记阶段9 complete。
- 修复后香港线路SDK复测运行`900001ms`并退出0：共收到897条消息，其中896条为Chainlink更新；`initial_generation=final_generation=0`、`subscription_revision=1`，本地SOCKS隧道只接受1个TCP连接。前后Cloudflare回执均为`8.210.176.1 / HK / HKG`，`poly-arb-second-proxy.service`前后均为PID `7729`、`NRestarts=0`。回执`target/rtds-sdk-hk-probe/postfix_receipt.json`的SHA-256为`424321c0658c348a7cfc954cfd7eb19e859948f35cf35861263ed4946add1c37`。官方SDK WSS应用帧在外部SOCKS隧道处为TLS密文且公共runtime不暴露出站帧，因此本轮不把约181次推算值写成修复后wire观测：精确PING计数/cadence由同一实现的loopback直接计数，服务端no-PONG事实由修复前香港raw-wire的181次PING/零PONG证据冻结，真实WSS层只证明15分钟业务连续、单generation/revision和零no-PONG重连。
- 只修正旧`data-collect-reliability-repair` demand/plan的当前authority指针；其changelog历史不改。阶段9授权不包含生产构建、部署、远端服务启停/写操作、数据清理或重采。

## 2026-07-19：阶段9 RTDS-PONG-A实现、门禁与独立终审完成

- SDK `Config`新增`require_text_pong: bool`并默认`true`。CLOB生产显式`10s/20s/true`，继续只把精确应用层文本`PONG`作为响应；WebSocket protocol Pong不能冒充文本PONG，no/old-PONG继续按recoverable transport恢复。RTDS生产显式`5s/15s/false`，其中`15s`字段在false模式不生效；RTDS立即发送首条、之后每次由唯一connection owner实际完成大写文本`PING` write后经私有oneshot确认，确认前最多一个pending，确认后`Interval::reset()`并等待完整周期，不等待PONG、不因no-PONG重连，也不在解除写阻塞后catch-up burst。
- Close、EOF、read/write/PING-send failure继续recoverable；parser sticky fault继续terminal；last-manager drop继续normal且不重连。PING/PONG channel、watch和heartbeat task均为generation-local，没有新增raw Polymarket WebSocket、第二SDK owner/session、shadow API、Cargo依赖或第五个vendor文件。本阶段vendor只修改已批准四路径中的`src/ws/config.rs`与`src/ws/connection.rs`；生产只在`data-collect/src/live_runtime.rs`分流配置。
- 测试先行红灯因`Config`尚无`require_text_pong`编译失败并退出101。实现后，production RTDS no-PONG测试跨过至少三个旧timeout仍在同socket消费stale/fresh、只发送一次订阅且至少观察7次PING；direct manager测试同样跨旧timeout保持generation；send-only单pending测试证明write ack前不积压、ack与迟到PONG不触发burst；false模式PING write failure保持recoverable。CLOB protocol-Pong反例、自然Close/replay与RTDS 100-cycle测试分别证明strict文本PONG不回归、101个generation每代只replay一次、subscription/task/FD有界。独立测试审查又将两个no-PONG测试各重复20/20，并复跑CLOB strict与100-cycle，全部通过。
- 提交前连续两次运行workspace全量时，先后暴露`rtds_source_survives_startup_unavailable_then_applies_one_complete_desired`与`rtds_source_recovers_after_close_and_failures_without_gap_or_replay_duplication`两个既有RTDS production-source测试在collector按run deadline关闭socket后仍尝试回写无必要文本PONG并对发送结果`unwrap()`，分别以`Connection reset by peer`使测试server panic；审查同时发现`rtds_source_retries_after_a_stalled_websocket_handshake`存在同型但尚未触发失败的PONG回写。该问题只发生在loopback server teardown：生产RTDS已经是`require_text_pong=false`，这些PONG既不被消费也不是测试断言。最小修正删除前两个实际失败测试及第三个主动清理测试中的PONG回写，让三个既有测试与批准的no-PONG合同一致；没有再改生产状态机。四个`rtds_source_*`测试随后以单线程组合连续10轮、共40项全部通过；根Clippy曾因中间写法触发`collapsible_if`，合并条件后精确命令退出0。上述两次实际失败、第三项主动清理及修正没有被隐藏或冒充为首轮全量通过。
- 最终本地门禁全部退出0：`cargo test -p data-collect --locked -- --test-threads=1`中254项lib及全部integration/doc harness通过，直接patch/runtime contract分别17/17与19/19；`cargo check -p data-collect --all-targets --locked`、`cargo clippy -p data-collect --locked --all-targets -- -D warnings`、`cargo fmt --all -- --check`及`git diff --check`通过。vendored SDK以`cargo +1.88`运行：core approved features lib 170/170；all-features共624项通过、20个doc示例ignored；default与all-features两组all-targets Clippy `-D warnings`均通过；固定`nightly-2025-11-24` vendor fmt check通过。本机另有名为`1.88.0`且未安装Clippy的重复toolchain，本阶段没有安装组件，也没有把该失败别名冒充通过；上述`+1.88`与其均为同一`rustc 1.88.0 (6b00bc388 2025-06-23)`，门禁使用已安装Clippy的`+1.88`。
- 修复后香港线路SDK复测运行`900001ms`并退出0：共897条消息、896条Chainlink update，`initial_generation=final_generation=0`、`subscription_revision=1`，本地SOCKS隧道仅1个TCP连接；前后出口均为`8.210.176.1 / HK / HKG`，代理服务均为PID `7729`、`NRestarts=0`。回执`target/rtds-sdk-hk-probe/postfix_receipt.json` SHA-256为`424321c0658c348a7cfc954cfd7eb19e859948f35cf35861263ed4946add1c37`。修复后WSS层只证明业务连续、单generation/revision与零no-PONG重连；精确PING cadence继续由同实现loopback计数，服务端no-PONG由修复前香港raw-wire 181次PING、零PONG/Close证据证明，不把TLS隧道不可见的修复后出站帧冒充wire观测。
- 官方crate缓存SHA-256 `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`与crates.io sparse index一致；registry与vendor源码差异仍精确为批准的四个文件，本阶段仅涉及其中两个；root/vendor Cargo与lock均未变化，vendor目录无`target/`。代码/状态机与测试/资源两路`gpt-5.6-sol`/`ultra`独立终审均为PASS，Critical 0、Important 0、Minor 0。
- 阶段9提交subject冻结为`fix(data-collect): keep RTDS heartbeat on fixed cadence`；commit SHA与push后本地/upstream/remote ref一致性由交付报告记录，不在commit自身伪写尚未存在的SHA。阶段9完成只表示本地文档、代码、测试、香港线路诊断复测、provenance与审查闭环；没有执行生产release构建、部署、远端服务/数据写操作、数据清理或重采，也不授权这些操作。

## 2026-07-22：Polymarket resolution-followup有界例外同步

- 用户独立批准`DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`需求语义与阶段0主authority文档同步。本专项关于source-local持续恢复、current/next按当前时钟重算、普通transport不进入daily manifest或typed episode的既有合同继续有效。
- expired-slug规则只增加一个有界例外：目标market必须已在自身活跃窗内固定并验证identity且仍为`market_runtime_role=resolution_pending`，才可对该exact slug执行E+3..E+8 resolution follow-up。process restart还要求finalized durable v3 identity；错过slot不补做，budget结束不追逐，不允许历史backfill或第7次probe。
- desired asset set同步扩展为current + prefetched next + resolution_pending旧market assets；terminal claim后继续保留对应pending/assets，直到accepted-event bounded drain完成，随后才按terminal或budget完成精确移除。该变化只服务已批准的旧market terminal观察，不改变current/next discovery。
- transport持久化规则只增加固定业务slot例外：每slot最多一条`polymarket_market_metadata_v3` attempt row，仍写入既有metadata Parquet并由manifest既有file facts覆盖；不新增manifest transport scope/reason/current/open/episode、artifact、sidecar或第二writer。普通connect/read/backoff availability仍只进入source-local运行态与有界operator日志。
- 本次只同步demand与历史changelog的supersede关系，没有修改本专项已完成的Rust、测试、SDK、schema产物或运行证据，也没有执行构建/测试、commit、push、release、远端或服务操作。
- 用户随后明确授权独立resolution-followup专项完整执行阶段1至阶段4、每阶段独立审查后的commit/push及本地香港VPN AC-12；该后续授权supersede上一条“本次只同步”的执行边界，但不重开source-availability其他已完成阶段，也不授权production部署、服务操作或数据删除。

## 2026-07-25：metadata v4继承resolution-followup窄例外

- 用户批准`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`。本文件2026-07-22记录的metadata v3 attempt例外及阶段事实保持历史有效，不回写。
- activation前，固定E+3..E+8业务slot的每slot最多一条attempt row仍由current source v2 + metadata v3路径承载；metadata v3 schema/version立即冻结。
- future source v3 + metadata v4只可按SDK070总计划阶段3原子activation。activation后同一五row-kind/attempt例外由metadata v4继承，source v2与metadata v3只读；不得cross-pair、dual-write或半切换。
- v4只在v3不可变前缀末尾追加两个nullable WS claimant字段，复用同一artifact、writer owner、partition与manifest mapping，不扩大普通availability持久化，也不新增artifact、sidecar或第二writer。
- 本次只同步demand/changelog authority；source-availability计划没有metadata版本或attempt exception执行条款，因此不新增重复步骤。没有修改Rust/schema/tests、构建、网络、commit、push、部署、服务或数据。

## 2026-07-25：current SDK身份由独立SDK070升级门接管

- 本事项阶段0--9以官方0.6.0与`0.6.0-p1`完成的实施、测试、香港线路证据和
  commit/push均保持历史事实，不回写。本事项原先要求采用更新release必须建立独立
  SDK升级门；该门已由
  `DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`建立。
- SDK070 Stage1当前worktree已从官方0.7.0 crate全新建立唯一
  `third_party/polymarket_client_sdk_v2-0.7.0-p1`，切换Cargo/lock并删除旧tracked
  vendor。相对官方0.7.0的源码差异仍恰好为本事项第4.7节批准的四路径；SDK070管理
  当前版本/path/provenance，本事项继续管理patch业务语义、SDK-first、唯一owner、
  no-shadow及heartbeat/teardown合同。
- SDK070离线runtime矩阵已通过并标记`SDK_070_RUNTIME_USABLE`；其typed
  missing/null fixture确认七类public market variant存在admission前整帧parser gap，
  因而标记`SDK_070_TYPED_CONTRACT_BLOCKED`。该typed阻断只停止SDK070 v3入口，不
  重开或改变本事项已完成source lifecycle行为。
- SDK070 Stage1独立审查已为`Critical 0 / Important 0 / Minor 0 / PASS`，当前仍待
  另行commit/push授权。本同步没有修改本事项生产Rust/schema/RTDS行为，没有运行
  公开网络smoke、release、部署、远端服务/数据操作或清理。

### SDK070 Stage1发布授权同步

- 用户已另行批准SDK070 Stage1 commit/push；其闭合以承载SDK070闭合记录的commit
  成功推送且远端SHA与本地HEAD/upstream一致为生效条件。
- 本记录精确取代同日此前等待发布授权状态；旧段仅保留为历史事实。
- 本同步不修改本事项既有生产行为，也不授权公开网络smoke、release、部署、远端
  服务/数据操作或清理。

## 2026-07-26：SDK070 Stage5删除未接production的health shadow model

- SDK070 Stage5首次独立终审确认：本事项阶段1保留的`health.rs`
  `RetryPolicy/plan_retry_trace`、circuit registry、`ReconnectGate`与三个专属
  integration contracts仍只被自身test和production `retry_delay`间接取最后一次
  sleep使用，没有成为真实source lifecycle owner。
- 用户批准`U06-A`：删除该有限attempt/elapsed与fresh-business reconnect shadow
  model，把确定性capped exponential + stable jitter数学收窄到现有私有
  `live_runtime::retry_delay`；真实`SourceRecoverySupervisor`继续使用有界单次
  等待、无限外层总attempt，business silence/stale不驱动transport恢复。
- U06本轮增量为`+24/-1,108`；source recovery、business-silence、hourly rollover、
  backoff、Binance定向合同及最终package/check/clippy/fmt/diff门均通过。该变更只
  删除失去consumer的旧控制模型，不回写本事项历史阶段证据，不改变SDK-first、
  heartbeat、manifest或完整性边界，也未执行production、远端、部署或数据操作。

## 2026-07-26：RTDS业务静默共同合同形成窄例外

- 用户批准独立`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`专项，精确取代本文对RTDS的全局“业务静默永不控制网络”当前语义；本文阶段0--9当时的实现、测试、commit/push及自然Close/PING/no-PONG证据保持历史事实。
- 例外只允许同一RTDS owner在source task此前收到fresh后，按`300s steady + 60s后到monotonic first-fresh + cold-start disabled + 1s × 2 ±20%/300s cap/unlimited`串行break-before-make替换整个runtime。它不恢复同socketrefresh、one-shot `request_reconnect`、generation预算、第二owner/socket、gap/lag/告警或进程退出。
- AC4明确拆成从未fresh与此前有fresh两个fixture；AC7保留cold-start同socket，AC9保留真实transport恢复且无one-shot ticket，AC10继续要求删除该能力。用户随后明确否决新增100-cycle与完整资源矩阵；AC25改为保留既有自然Close历史/现存证据，新business replacement只以连续两次replacement、共三个runtime epoch的精确顺序测试验收。
- 当前代码只有training拥有真实RTDS adapter；新恢复行为尚未实现，专项阶段1只在training实现与验证。future paper/live真实RTDS adapter的实现、handoff、测试和部署仍须独立三件套，但连接生命周期必须继承相同共同合同，不再独立冻结；当前paper/live仍heartbeat-only。CLOB、Gamma、Binance不变。

## 2026-07-26：后继RTDS专项移除历史100-cycle执行门

- 用户进一步确认既有100-cycle本身也是过度校验。本文已完成阶段3/9的自然Close 100-cycle测试与结果继续作为当时历史事实保留，不删除、不改写；后继RTDS business replacement专项不重新执行，也不把它作为新路径的验收依据。
- 后继专项的最小证据改为wrapper state-channel closure测试、production同一owner中`shutdown_and_wait(self).await`后创建新runtime的直接源码顺序，以及连续两次replacement/三个runtime epoch聚焦测试。不得增加test-only lifecycle hook、四段专用恢复耗时或进程资源矩阵；该收窄不改变本文其他source已完成的历史验收。

## 2026-07-26：后继RTDS专项阶段1候选实现通过收窄验证

- training唯一RTDS adapter已实现冻结的business replacement窄例外；同一owner消费单一事件流，旧runtime完成消费式shutdown后才创建新runtime。该实现不恢复同socket refresh或one-shot `request_reconnect`，不增加第二owner/socket、gap/lag/告警或进程退出。
- 既有聚焦测试覆盖wrapper state-channel closure、initial/后续generation观测、queued-message freshness fence、backoff增长/reset/cap及三个runtime两次replacement。终审成立项以现有测试内的最小断言和删除timer重复事实读取闭环，没有新增100-cycle、资源矩阵或test-only hook。
- 修正后RTDS lib精确测试20项全部通过。历史100-cycle在后继专项早期、最终简化裁决前曾误跑一次；裁决后的收窄门没有再次执行，也未把它作为新business replacement验收依据。
- 当前专项为`STAGE1_IMPLEMENTED_VALIDATION_PASSED_REVIEW_PENDING`；future paper/live共同合同与其他source保持不变。

## 2026-07-26：后继RTDS专项阶段1闭合

- 后继专项runtime、测试和authority三路独立终审均为PASS，Critical/Important/Minor均为0；收窄最终门全部通过，裁决后未再执行历史100-cycle。
- 文档验收commit `ef200a6e89eaaee9f19ff49a00232b5004825ce7`与Rust实现commit `a29486babe2682481549cfc0b95b010784d653f0`已分离push并核对三方ref一致。
- 承载本记录的闭合文档commit成功push且三方ref一致后，后继专项状态为`STAGE1_CLOSED_STAGE2_PROBE_PENDING`。本文既有阶段事实、future paper/live共同合同、CLOB/Gamma/Binance及其他source均不变。

## 2026-07-27：后继RTDS专项Attempt1与方案A窄supersede

- 后继专项阶段2 Attempt1从clean HEAD运行`16851391ms`后，因顶层`Network error communicating with endpoint`数据外对象被现行typed parser归为terminal而提前结束；固定结论为`FAIL + INCOMPLETE_8H`。辅助`probe-tee-exit.txt`的已披露事后污染按E-A原样保留，未受污染的核心日志、frame/script哈希和运行计数继续由专项changelog记录；Attempt2使用新目录。
- 用户批准方案A：正常typed parse失败后，只把顶层object、精确network-error message且无`topic/type/timestamp/payload`的对象以crate-private typed marker映射到既有recoverable connection exit。它不写row、不计fresh、不直接改变业务freshness/recovery episode或持久化；transport state/generation按既有自然重连变化，其他未匹配service message及typed/parser/schema错误仍sticky terminal。
- 方案A只使用既有vendor allowlist中的`rtds/subscription.rs`和`ws/connection.rs`，配一个正负合并runtime loopback；不修改本文阶段0--9历史结果，不新增collector parser、公共API、第五个vendor路径、第二owner/socket、service-message矩阵、资源计数或100-cycle。authority与Rust分离发布后才从新clean HEAD执行Attempt2。

## 2026-07-27：后继RTDS专项方案A实现闭合

- 后继专项authority commit `366fed7d19ff9ce2f855f362c81de5641b63f9c4`与Rust commit `415470eda0b2d629c19c2609e6610a253703d684`已分离发布，聚焦验证、三路独立终审和三方ref核对均已闭合；详细证据由专项changelog记录。
- 本事项阶段0--9历史结果及CLOB/Gamma/Binance边界均未修改。承载本记录的纯文档closure commit成功push且三方ref一致后，后继专项下一游标仅为从新clean HEAD和新artifact目录执行Attempt2；历史100-cycle没有重跑。
