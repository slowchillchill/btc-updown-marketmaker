# RTDS 业务静默彻底换连接恢复 Changelog

> 事项编号：`DC-REPAIR-20260721-RTDS-BUSINESS-SILENCE-FULL-RECONNECT`
>
> 对应需求：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
>
> 对应计划：`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`
>
> 状态：`APPROVED_SHARED_RTDS_CONTRACT_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_USER_TERMINATED_INCOMPLETE_ATTEMPT3_FAIL_INCOMPLETE_8H_GAMMA_CLOSED_TIME_A_IMPLEMENTATION_CLOSED_ATTEMPT4_FAIL_INCOMPLETE_8H_P_B1E_IMPLEMENTATION_CLOSED_ATTEMPT5_USER_TERMINATED_SCHEMA_CHANGE_INCOMPLETE_8H_REDUNDANT_TYPED_COLUMNS_STAGE0_CLOSED_ON_PUBLISH_STAGE1_NEXT_ATTEMPT6_PENDING_AFTER_V7_V5`。其中`P-B1E`是本次获批的“nullable Decimal精确空串兼容”唯一标识，用于避免与Attempt1辅助文件污染处置的历史`E-A`重名；该状态仅在承载本记录的纯文档closure commit成功push且本地HEAD、upstream与远端ref一致后生效。RTDS network-error方案A的`366fed7d`/`415470e`、Gamma `closedTime`方案A的`3a367386`/`e086017f`及P-B1E的`15feb12`/`9d8e59b`均已按authority/Rust分离发布并完成三方ref核对；Attempt1、Attempt3、Attempt4永久保持`FAIL + INCOMPLETE_8H`，Attempt2永久记录为`USER_TERMINATED + INCOMPLETE_8H`。Attempt5已按用户指令停止；下一游标是v7/v5闭合后的完整六源8小时Attempt6。

## 2026-07-28：Attempt5 用户终止，游标移至 v7/v5 后的 Attempt6

- 用户批准删除两个重复 typed 字段，并明确批准先停止当前测试再执行新 schema。
- Attempt5 在 clean/pushed `92fb71883cad19285ae0a291e2a8ee94698133dd`
  上运行 `16233.700s` 后收到精确进程组 `SIGINT`；wrapper 记录
  `collector_exit=101`、`tee_exit=130`、`end_route_exit=0`；前两者为非零，
  不记作正常完成。
- 每日顶层 manifest 保持 `state=open`，仅
  `collection_day_coverage.state=ended_known`；保留 `27` 个 finalized Parquet、
  `297,802,741` bytes，`.running=0`。旧 root 不删除、不改写。
- Attempt5 固定为
  `USER_TERMINATED_FOR_APPROVED_SCHEMA_CHANGE + INCOMPLETE_8H`。下一游标不是
  续跑 Attempt5，而是在
  `DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL` 的 v7/v5 阶段分别闭合后，
  从新 clean/pushed HEAD 和空 root 执行完整六源 8 小时 Attempt6。
- 顶部状态段中“Attempt5 为下一游标”的旧句是停止前历史描述，由本条精确取代。

## 2026-07-21：用户选择彻底换连接方向

- 用户在完成TCP/WebSocket、Polymarket/Chainlink官方材料、开发者社区与相关论文分析后，明确选择“彻底关闭旧连接，再建立新连接”的思路，并要求创建新的需求、计划和changelog，写完后停止供其自行审核。
- 本次选择否决把“同一连接重复订阅”或“先取消再订阅”作为生产恢复路径。当前SDK对相同topic/type重复subscribe只增加本地refcount，不实际发第二个网络订阅；unsubscribe又不保留原`btc/usd` filter，且官方RTDS没有ACK、幂等或事务保证。
- “彻底换连接”被限定为：训练collector同一PID内，RTDS唯一owner先完整停止并释放旧runtime/actor/receiver/heartbeat/WebSocket，证明旧连接退出后才创建一个新official SDK runtime，并只replay一次最新完整filtered desired。
- 本次选择不引入并行备用生产连接、第二receiver/actor、第二writer、raw WebSocket或manual protocol；不复制到CLOB、Gamma、Binance、paper或live。

## 2026-07-21：保留事实、不把推断写成根因

- 已知RTDS在`2026-07-20T00:32:36.493Z`至`2026-07-20T02:13:51.836Z`业务静默后，在没有客户端处理的情况下自然恢复；恢复数据为当时新鲜更新且没有补发静默区间。
- 上述事实证明原连接最终可以继续收到未来数据，但不能证明服务端执行了哪种重试，不能区分全局上游、Polymarket转发链、单后端、单连接或本地接收任务，也不能证明主动换连接必然有效。
- Polymarket官方文档只规定5秒`PING`，没有固定业务update cadence、业务静默SLA、重复subscribe确认或Chainlink crypto backfill合同。5秒因此不能从heartbeat cadence直接升级为业务静默阈值。
- 社区Issue支持“开放连接可能业务静默、自然恢复或换连接workaround”是现实故障模式，但没有维护者根因或受控对照；另有不同Polymarket端点在反复新连接后仍无数据的反例。因此新机制被表述为扩大连接级故障覆盖面，不表述为解决共享上游停流。

## 2026-07-21：发现并记录authority冲突

- 当前执行authority是`demand/data-collect-demand.md`及其指向的`demand/data-collect-source-availability-lifecycle-repair-demand.md`，两者明确要求RTDS业务静默不调用subscribe/unsubscribe、不切连接代次、不主动断开仍存活的连接。
- `demand/data-collect-reliability-repair-demand.md`已被新专项部分取代，只保留仍有效合同；`demand/rebuild-core-demand.md`整体仍在审核中，只是未来一致性上下文；旧计划和changelog记录历史执行事实。它们不得与当前执行authority混称为同一层级。
- 新决定与当前执行authority不能同时成立。三件套因此标记为`DRAFT_AWAITING_USER_REVIEW_AND_PARAMETER_FREEZE`；在用户批准新文档与具体参数后，未来计划阶段0必须精确同步相反语义，逐项保护旧需求的保留合同和草案上下文，旧changelog只保留为历史执行事实。
- 新例外只覆盖training RTDS Chainlink `btc/usd`；现有“其他source业务静默不控制网络”“外部availability不结束collector”“真实transport故障持续恢复”“唯一owner/SDK-first”仍继续有效。

## 2026-07-21：参数与SDK能力保持显式决策门

- 用户本次只选择恢复动作，没有重新批准`steady_silence_threshold`、新连接首条fresh观察窗口、teardown报告期限、连续重连退避/jitter或冷启动适用范围；用户新增了连续8小时香港VPN实测方向，但出口/路由/liveness/socket证据的采样间隔与最大允许缺口仍待阶段0提出候选并由用户冻结。
- 先前讨论的5秒不能自动继承：官方没有该业务SLA，且既有正常数据出现过超过5秒的合格更新间隔。需求第六节与计划阶段0要求先重扫真实数据、计算候选阈值误触发和长静默重连风暴上界，再由用户冻结参数。
- 当前`RtdsSdkRuntime::Drop`与SDK sender关闭具有loopback收敛证据，但SDK没有公开异步`shutdown_and_wait()`。未来实现必须先证明旧runtime的全部clone/task/socket在新runtime创建前退出；wrapper无法证明时必须停在SDK gap门，报告最小官方SDK patch选项，不能旁路到raw WebSocket。
- 换连接只尝试恢复未来数据，不补回已经缺失的静默区间；本事项不新增backfill、synthetic row、availability sidecar或manifest schema。

## 2026-07-21：三件套草案创建完成，等待用户审核

- 新建：
  - `demand/data-collect-rtds-business-silence-full-reconnect-demand.md`；
  - `plan/data-collect-rtds-business-silence-full-reconnect-plan.md`；
  - `changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`。
- 三份文档使用同一事项编号并互相引用；需求冻结目标、范围、状态、验收与参数门；计划按阶段给出测试先行、SDK能力门、完整teardown/new runtime、持续静默退避、资源测试和独立部署授权；changelog记录事实、冲突、用户决定和未决参数。
- 本轮没有修改旧authority、Rust代码、vendor SDK、测试、binary、service或数据；没有构建、commit、push、远端probe、部署或清理。按用户要求，完成文档一致性检查后停止。

## 2026-07-21：用户澄清业务目标是尽快恢复未来实时传输

- 用户明确说明，本事项不是为了补回静默期间缺失的数据，而是希望通过有效手段尽快恢复网络连接和未来实时数据传输，避免类似1小时40分钟的静默长期压缩模型可交易时间与交易机会。
- 需求已把业务目标改为缩短端到端恢复耗时、提高合格实时数据可用时间，并要求拆分记录静默识别、旧连接teardown、新连接握手、订阅至首条fresh及总恢复耗时。只证明新连接建立不能冒充效果成立。
- 计划新增效果验证边界：用同一确定性 loopback 脚本分别运行不并发的被动基线和主动完整换连接场景；真实 8 小时 probe 保持单连接。未复现真实静默时，必须报告“机械正确但真实可用时间改善未验证”。
- data-collect仍只恢复真实数据供给，不决定模型暂停、恢复或交易，不因追求更多交易机会而放宽缺失、stale、invalid数据及执行安全门；恢复不会补写历史缺口。

## 2026-07-21：用户指定本地香港 VPN 长稳实测方向

- 用户提出未来可在本地电脑通过香港 VPN 网络运行长时间测试，例如连续 8 小时，用于检查新方法能否有效避免再次出现接近 1 小时 40 分钟的业务数据静默。
- 需求与计划据此把连续 8 小时本地香港 VPN 只读 probe 纳入未来验收：必须记录香港出口确认、连续性、commit、binary SHA256、参数、合格更新间隔、连接代次、每次 teardown/new connect/subscribe/首条 fresh 及分段和总恢复耗时，并与历史 `6075.343s` 静默作跨环境数量级比较；本地香港 VPN 与历史 AWS 爱尔兰环境不同，不冒充同环境对照。
- 为避免把偶然没有故障误判为机制有效，同时加入两类可控测试：旧 socket 仍开放但停止合格更新、新连接可恢复 fresh；以及所有新连接均无 fresh 的共享上游停流。前者验证机制能摆脱单连接 stuck，后者验证彻底换连接不是所有上游故障的保证且必须安全退避。
- 8 小时内没有复现同类静默时，只能报告该窗口稳定。独立 loopback 对照只证明所模拟单连接 stuck 故障的机械效果；只有可比真实事件证明新连接更早获得 fresh，才能声称真实 RTDS 可用时间改善。该证据边界不削弱用户“尽快恢复未来实时数据”的目标，而是防止把没有发生事故误写成因果效果。
- 本轮仍只修改三件套草案，没有运行 probe、切换 VPN、修改代码、构建、commit、push、部署、远端操作或数据清理。

## 2026-07-21：独立终审问题已在草案中修正

- 补全 teardown 报告期限到达后的合法后继：进入“旧连接关闭未确认”，旧代次保持隔离，不创建新连接、不结束 source/collector，继续等待真实完成信号或 operator stop；无法取得可等待信号时在实现前触发 SDK 能力门。
- 区分三种证据：独立 loopback 被动/主动场景只证明被模拟的单连接 stuck 故障；本地香港 VPN 8 小时无事故只证明该窗口稳定；真实 RTDS 可用时间改善仍需可比真实事件。
- 删除当前计划直接临时批准并行被动观察连接的入口。若未来需要同一真实事件的并发对照，必须另建诊断三件套并显式修改排除项；本事项保持单活动 RTDS 连接。
- 精确区分当前执行 authority、部分被取代但仍有保留合同的旧需求、尚未整体批准的 rebuild-core 草案和历史执行事实。
- 资源验收改为离散资源回到基线、RSS预热后有界平台且无持续正增长斜率；计划新增逐项回归映射，并标明香港 VPN 与历史 AWS 爱尔兰事故只能作跨环境数量级参考。
- 补全 8 小时连续性证据：开始、批准的固定有界间隔和结束均记录同一网络命名空间的香港出口/RTDS路由，并由独立 supervisor 记录monotonic liveness、boot/suspend、PID、日志连续性和活动RTDS socket数；任一证据超限即整段无效并从零重跑。
- 当前`emit_rtds_message()`在返回fresh前必经`send_event()`，不能直接作为无写入probe。计划因此加入一个私有最小准备入口，让正式路径继续进入`send_event()`、probe只进入有界内存诊断sink和隔离临时artifact根；禁止ManifestOwner、Parquet writer、canonical ingress或第二套validator，并要求canonical根前后清单及运行文件句柄不变。
- 修正阶段追溯：阶段1只关闭状态机与probe接缝，阶段3关闭显式回归并只完成资源验收的一部分，100轮离散资源/RSS平台证据在阶段4.1才最终闭环。
- 阶段2不再提前宣称需求验收第5项的持续静默重试已经完成；该部分明确由阶段3闭环。关闭参数改名为`teardown_report_deadline`，并澄清只有单次主动操作和每段等待/诊断有界，“旧连接关闭未确认”状态不得因期限到达而冒充关闭完成。

## 2026-07-21：用户批准按第二轮审查建议修订文档

- 用户明确同意按审查建议修复 demand、plan、changelog。该授权只覆盖文档修订，不等于冻结业务参数或批准代码、构建、测试、commit、push、8 小时 probe、production release、部署或远端操作；三件套状态据此从 `DRAFT_AWAITING_USER_REVIEW_AND_PARAMETER_FREEZE` 收敛为 `DRAFT_PARAMETER_FREEZE_PENDING`。
- 本条是当前有效修订记录。上方同日条目保留草案演进事实，但其中与本条冲突的私有 probe 接缝、资源硬门和重型 VPN 证明链不再是当前需求或计划。

### Authority 与证据语义修正

- 阶段 0 的 authority 同步范围补入 `demand/data-collect-demand.md` 中 subscribe send、ACK/首条消息混用的问题，不再只同步“业务静默不切连接”的第 4--5 项。当前实现只能观测 local applied，不能据此声称 wire write、握手完成或服务端 ACK。
- 收窄 Polymarket real-time-data-client Issue #26 的证据等级：它是未关闭的 `activity` feed 用户报告，只支持开放 socket 无业务更新及用户重连 workaround 的相似症状；不是 Chainlink 专项证据，也没有维护者根因或自然恢复证明。本项目 2026-07-20 事故才是原客户端路径自然恢复的本地证据。CLOB Issue #292 只保留为不同端点的反例。
- 修复范围冲突：未来获批实现所需的本地开发构建、测试和静态检查属于实施范围；排除项只保留 production release 构建、部署、systemd、远端服务和生产数据清理。本轮仍不执行任何构建。

### 最小可靠控制流

- 将原“连接代次”拆为两个不会混淆的概念：`runtime_epoch` 表示整个 `RtdsSdkRuntime` 的本地替换编号；`sdk_connection_generation` 仅表示同一 runtime 内 SDK 的自然 transport generation。前者只进有界日志/测试，后者继续承载现有 `reconnect_events` 语义，不增加 row、manifest 或 counter schema。
- 新增明确仲裁：business deadline 只在 current generation 同时 `Connected + local applied` 且无 transport fault 时运行；进入 `Connecting/Reconnecting/Disconnected` 即撤销旧 business deadline 和 outer backoff，让 SDK inner reconnect/backoff 优先。自然新 generation 从首条 fresh 窗口重新开始，不继承旧 steady deadline，`require_text_pong=false` 保持不变。
- `first_fresh_observation_window` 的候选 anchor 收敛为 current runtime/generation 首次同时观测到 `Connected` 与 latest desired local applied 的 monotonic 时刻。指标只保留 `threshold-to-teardown-complete`、`runtime-create-to-local-applied`、`local-applied-to-first-fresh` 和 `threshold-to-first-fresh`，删除未经能力证明的 handshake/wire subscribe/ACK 指标。
- teardown 继续坚持 literal break-before-make，但完成证据改为项目 actor 已结束、旧 receiver/manager owner 已释放、SDK connection loop 状态/channel 已完成；不要求服务端 Close ACK。若 wrapper 无法等待真实 completion signal，仍触发 SDK gap 决策门。
- 第一次 business-silence replacement 可以立即执行；新 generation 首条 fresh 窗口到期后，outer capped backoff 在当前静默连接仍活动时等待，fresh 可取消并重置。backoff 到期且目标/transport 状态仍匹配才 teardown；transport 转换会取消外层动作，避免主动制造无连接窗口或重置 SDK inner backoff。

### 删除过度设计与过度校验

- 否决上方第一轮草案提出的 `emit_rtds_message()` 私有“prepare”接缝。8 小时 probe 直接复用现有 `run_rtds_source_with_spec()`，使用隔离临时 dataset root 和持续 drain 的 in-memory `SourceEventSink`；不启动 ManifestOwner/Parquet/canonical writer，也不新增第二 validator 或 production seam。
- 100-cycle 完整 runtime replacement loopback 保留，因为新 break-before-make 路径不同于既有自然 reconnect。硬门缩减为活动 socket 最大 1、旧 socket 先关闭、每连接一次 filtered subscription、FD 有界无单调增长、shutdown completion 后无可观察 orphan actor/task；删除 test-only watch/broadcast/refcount 计数器要求，RSS 只作诊断趋势。
- 删除由服务端脚本预设结论的独立被动 baseline；保留一个主动 stuck-connection 场景和一个永久共享上游静默反例。开发期只跑定向测试，代码稳定后一次性运行 workspace test、workspace clippy、fmt 和 diff check，不再重复 package/workspace 等价全量 suite。
- 香港 VPN 8 小时 probe 保留，但证据链收敛为起止与低频公网出口/香港区域样本、PID、boot ID、monotonic/boottime 和 socket high-water。删除独立 supervisor、逐间隔 route/log-cursor、canonical mtime/FD attestation。只有确认 restart、suspend、离开香港或活动 socket 超过 1 才使窗口无效；单个环境样本缺失只标记环境证据不完整。

### 剩余决策门

- 用户仍需冻结三个高影响决定：`steady_silence_threshold`、`first_fresh_observation_window` 的时长与 anchor、cold-start 是否启用同一 watchdog。outer backoff、teardown 诊断间隔和 probe 采样间隔改为有测试上界的实现常量，不再默认扩大为产品审批门；若数据分析证明会实质改变恢复频率或风险，再提交用户决定。
- 本轮没有修改旧 authority、Rust 代码、vendor SDK、测试、binary、service 或数据；没有构建、commit、push、VPN probe、production release、部署或清理。

## 2026-07-21：修订后独立终审问题闭环

- 修订后的三件套经独立 subagent 终审发现 `0 Critical / 4 Important / 1 Minor`；问题均属于文档内部精度或执行顺序，不否定 break-before-make 核心方向。成立项已按最小范围修复并进入复审。
- 新增 source-task 级 `ever_received_fresh`：初始为 `false`，首条合格 fresh 后置为 `true` 并跨同一 task 的 runtime/generation replacement 保持。只有该值为 `true`，或用户明确启用 cold-start watchdog，才在 `Connected + local applied` 后启动首条 fresh 窗口；cold-start 禁用时首次无数据只走 transport 恢复，不主动换 runtime。
- 修正 recovery episode 语义：当前没有 episode 时，由 steady threshold 命中或已启用的首条 fresh 窗口首次到期启动，到下一条合格 fresh 结束；同一 episode 任意时刻最多一个在途恢复动作，但 fresh 到达前允许按窗口和 outer backoff 串行执行多轮 replacement。后续 timeout 或创建新 runtime 本身不重启/结束 episode。
- 收紧 teardown 完成门：actor `JoinHandle` 必须完成，旧 receiver 与全部 manager owner 必须释放，保留的 state receiver 必须观察到 channel closure；SDK 提前发布的 `Disconnected` 只作为中间状态，不能单项放行新 runtime，也不要求服务端 Close ACK。
- 修正计划顺序：阶段 0 只读核对 teardown ownership/state-channel 路线，不修改 wrapper/vendor、不新增相反行为测试；用户冻结参数并先同步冲突 authority、完成阶段审查后，阶段 1 才测试先行实现 wrapper teardown。只读已证明能力缺口时仍直接触发 SDK 决策门。
- 去除剩余测试重复：开发期定向运行新增 multi-source 和实际受影响的既有测试；其余既有 transport/parser/shutdown 回归由最终一次 `cargo test --workspace --locked` 覆盖，不在此前整组重跑。
- 第一轮修复复审确认原 `4 Important / 1 Minor` 已正确落地，同时发现 `2 Important` 相邻冲突并继续闭环：episode 定义已补入启用的 first-fresh timeout 入口；operator stop 在 teardown 前可取消恢复，但 teardown 一旦开始必须等待真实 completion，stop 只把后继从“建新 runtime”改为“退出 source”。全局 hard-shutdown 若最终强制结束 task/process，不得记为 teardown 成功。
- 第二轮修复完成后的最终独立复审为 `PASS：0 Critical / 0 Important / 0 Minor`；确认两种 episode 入口、operator-stop/teardown 后继、三件套追溯、验证去重和简单性边界均一致，未发现新增问题。

## 2026-07-21：用户批准修复更广范围的上下文冲突

- 用户随后要求多路 subagent 从 demand/plan 内部一致性、现行 authority/真实代码和阶段授权边界重新审查。交叉去重结果为 `0 Critical / 8 Important / 2 Minor`；上条 `PASS` 如实保留为前一轮指定问题的复审结果，但不覆盖本轮新增的代码拓扑与授权上下文。用户已批准按本轮建议修订三件套，未批准参数或执行计划。
- 授权门被拆开：参数冻结不再触发 authority 修改；必须另行最终批准本需求、SDK 路线和精确 authority 修改矩阵并授权阶段 0。阶段 0 commit/push 授权不继承为阶段 1 Rust 修改/构建/测试授权；阶段 1、阶段 2 probe和production release/部署仍分别授权。
- 阶段 0 新增只读代码事实：current `recv_event_until()`不向 source owner 返回 `Connecting/Reconnecting`，initial-generation applied也被 freshness fence 吸收；当前 coordinator没有RTDS typed control channel，coordinated shutdown会直接abort RTDS。计划据此冻结最小 wrapper-only state/generation/local-applied observation surface，保持既有 freshness fence和vendor SDK不变。
- teardown 路线改为明确的两级门：阶段 0 只能记录 wrapper-first provisional route，不得冒充能力通过；阶段 1 获得独立授权后，最先以最小 loopback 实现并证明 actor join、全部 manager owner释放和state-channel closure。该 capability subgate 失败即停止并进入SDK gap门，未通过前不得实现business replacement。
- 新增内存内 `teardown_committed` 串行提交点：business候选提交前重新核对 runtime epoch、SDK generation、state、local-applied和fresh；提交前transport transition获胜，提交后状态只用于completion，旧runtime不可恢复。该标记不进入schema、sidecar或通用状态机。
- 为满足已冻结的operator-stop语义，计划沿用现有source-control模式增加一个bounded RTDS typed stop/control channel和completion reply。stop在business commit前转为shutdown-only，在commit后只改变completion后的后继；两者都等待真实wrapper completion。全局hard-shutdown强制abort仍可作为既有失败fallback，但不得记为teardown/completion成功。
- 修复cold-start和episode追溯：自然新generation只有在`ever_received_fresh=true`或cold-start watchdog启用时才进入首条fresh窗口；否则保持transport-only。任一已启用窗口到期，无episode则创建、已有episode则沿用，但两者都进入当前episode的下一轮outer backoff。
- 时间指标从`threshold-to-*`改为带`episode_trigger_kind`与attempt index的`episode-trigger-to-*`，避免steady threshold与first-fresh timeout两种入口混用起点；仍不虚构handshake、wire send或ACK。
- 8小时probe明确使用`run_rtds_source_with_spec()`同模块的`#[cfg(test)] #[ignore]`长运行harness、隔离临时root、持续drain内存sink和纯内存active `RawDayGate`。不公开production API、不启动ManifestOwner/writer、不恢复被否决的private production seam。
- 本轮仍只修改demand/plan/changelog；未修改旧authority、Rust代码、vendor SDK、测试、binary、service或数据，未构建、commit、push、运行VPN probe、production release、部署或清理。
- 本轮修订完成后的独立终审为 `PASS：0 Critical / 0 Important / 0 Minor`；确认上一轮 `8 Important / 2 Minor` 全部闭环，未新增过度设计、额外校验硬门、内部语义冲突或与当前代码事实不符的主张。

## 2026-07-26：用户冻结 RTDS 高影响参数合同

- 用户批准冻结 `steady_silence_threshold = 300s`。
- 用户批准冻结 `first_fresh_observation_window = 60s`，anchor 为同一 current runtime/generation 内 `Connected` 与 latest desired local-applied 两个事件的后到 monotonic 观测时刻。
- 用户批准冻结 `cold_start_watchdog = DISABLED until ever_received_fresh`：source task 从未收到合格 fresh 前不以首条 fresh 超时主动替换 runtime；收到首条合格 fresh 后，后续 runtime/generation 才启用 60 秒窗口。
- 冻结依据是 `data-collect/docs/rtds-stage0-parameter-candidates-2026-07-23-25.md` 对2026-07-23至2026-07-25三个完整UTC日、本地物理完整RTDS typed raw artifacts的只读分析；统计已排除manifest明确的collector停机相邻项，artifact完整不冒充source业务连续。该数据能给出候选episode-entry上界，但缺少逐generation的Connected/local-applied轨迹，不能确认实际误触发数或实际恢复收益。
- 参数冻结完成计划步骤 0.2.1--0.2.3，但不自动批准 SDK 路线或 authority 精确修改矩阵，也不授权把当前“业务静默不得控制网络”的相反 authority 静默改写。阶段 0 继续完成步骤 0.2.4、任务 0.1/0.3 的证据，并在触及 authority 修改前提交冲突矩阵由用户决定。
- 步骤 0.2.4 只读核对确认可直接固定两个低影响常量：teardown 每 `30s` 报告一次未完成诊断，真实 completion 前不放行新 runtime；8 小时 probe 每 `1h` 采样一次环境，另记录开始与结束。它们只影响日志/证据密度。
- outer backoff 选择复用现有私有 `retry_delay()` 的初始 `1s`、倍率 `2`、稳定 jitter `±20%` 和无限总 attempts，不引入 retry crate或通用 supervisor。hard cap 暂不冻结：`30s` 与推荐 `300s` 在 60 秒首条 fresh 窗口下，永久停流时分别约为 40 与 10 次完整 replacement/小时，已构成需求第六节规定的实质恢复频率/风险差异，必须由用户决定。
- transport transition在`teardown_committed`前取消outer wait时不消费该级、不算replacement且不递增`attempt_index`；自然新generation下一次窗口超时复用同一级，只有实际commit才递增attempt index。fresh仍是结束episode并把outer level重置为0的唯一业务事件。
- 任务 0.1/0.3 的只读证据已落盘为 `data-collect/docs/rtds-stage0-authority-wrapper-route-2026-07-26.md`。两份现行 authority 仍直接禁止业务静默控制 RTDS 网络；尚未修改。
- wrapper路线状态为 `WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE`：现有primitive允许静态构造“await actor -> 释放receiver/全部manager owner -> 等state channel closure”的候选顺序，但当前wrapper没有awaitable shutdown，现有测试也未证明完整completion；现阶段既不能记PASS，也不能宣告SDK gap。
- 只读核对发现拟议 `runtime_epoch` 与 `data-collect/src/live_runtime/raw_day.rs::RuntimeEpoch` 同名异义。建议改称 `rtds_runtime_epoch`，但命名和authority语义均等待用户决定。
- 当前source-availability AC4没有说明RTDS是否曾收到过fresh：从未fresh时必须保持cold-start disabled的同socket等待；此前有fresh时300秒后允许replacement。现有AC7首次fresh前同socket、AC9真实transport恢复且无business one-shot ticket、AC10完整删除`request_reconnect`均与冻结合同相容，不能被批量删除或整体 supersede。AC25则必须加入“只允许同一owner内串行业务replacement/backoff并执行新100-cycle资源门”的精确例外，仍禁止第二owner或并行恢复loop。
- 另发现授权上下文冲突：专项草案仍要求阶段0/1/2逐次另行授权，而SDK070 umbrella已记录用户standing execution覆盖后继本地执行并只在上下文/事实冲突时停止。8小时香港VPN probe会访问公开RTDS网络，不是既有standing范围的当然推论；最终批准时必须由用户明确决定是否把本专项的本地authority同步、Rust/build/test/review/commit/push及这一次8小时香港VPN只读公开网络probe一并纳入standing execution。production/部署/远端边界保持排除。

## 2026-07-26：完整参数、共同合同与阶段0同步获批

- 用户批准 outer backoff 合同：新runtime以后到monotonic anchor等待`60s`仍无fresh时，下一次完整换连接前使用逐次增长等待；复用现有`retry_delay()`的`1s × 2 ±20%`形态，单次hard cap=`300s`，收到合格fresh后重置，总尝试次数和总episode时长不设上限。
- 补全此前报告遗漏的计算前提：第一小时/8小时replacement数量上界采用尚未触顶attempt的最短`-20%` jitter、teardown/connect/apply耗时为零且始终`Connected + local applied`；达到cap后按helper先jitter再硬截断。该频率是保守工程估算，不是三日数据证明的外部SLA。
- jitter seed不再留作隐含假设：阶段1沿用当前source-local supervisor做法，在RTDS source task启动时读取一次`now_ms()`并在task内保持稳定；不引入新随机依赖或通用retry supervisor。
- 用户批准本需求、wrapper-first provisional路线、精确authority矩阵及本地持续执行。整个RTDS runtime替换代次采用内部名称`rtds_runtime_epoch`，避免与raw-day `RuntimeEpoch`同名异义；applied事实优先复用并最小扩展既有观测，不把建议术语误写成必须新增的domain type。
- 用户先指出paper/live RTDS应与training一致，随后精确澄清为“只冻结未来共同合同”。只读代码和两路独立审查确认：当前training进入真实`run_live_training_runtime()`/RTDS adapter，paper/live只进入heartbeat/status server，没有真实RTDS adapter。因此本专项只实现和测试training；future paper/live真实RTDS adapter由各自独立三件套实施、测试、验收和部署，但必须继承相同fresh classifier、四项主要参数、outer backoff、唯一owner/socket、break-before-make及触发/撤销/重置语义，不得重新冻结另一套合同。
- CLOB、Gamma、Binance及其他source保持不变。没有新增paper/live Rust adapter、共享framework、handoff producer、模型接线或部署任务，避免把共同政策合同误扩张成当前三个binary的虚假实现。
- 阶段0开始同步`data-collect-demand`与source-availability当前authority中的相反语义，同时保留历史plan/test/changelog的当时事实。用户随后将持续执行授权明确扩展到本专项阶段0、阶段1及阶段2本地香港VPN连续8小时只读probe；阶段0审查、独立复核、commit/push后按顺序进入阶段1，无需重复申请。production release、部署和远端操作保持独立边界。

## 2026-07-26：用户否决过严校验并收敛为最小实现

- 用户明确指出上一轮审查结论重新变得过严，目标应是代码简单、准确、可靠，并把新增100-cycle列为过度校验。三名`gpt-5.6-sol`/`ultra`独立subagent分别从stop所有权、测试范围和8小时probe复审，结论一致：先前的typed stop/completion、CAS或mutex线性化、100-cycle完整runtime replacement、完整资源矩阵和probe socket high-water都不是本事项核心正确性所必需。
- 代码复核确认当前RTDS只有source task局部的一个`RtdsSdkRuntime`；coordinator已先登记abort permit、调用`JoinHandle::abort()`并await source task。pinned Tokio `1.52.3`保证取消会析构task-local变量，且`JoinHandle.await`观察完成前析构已经结束。因此operator stop继续复用现有路径，不新增RTDS typed control channel、completion reply、CAS、mutex或stop/create竞态状态机。
- 需要新增的唯一shutdown能力收窄为业务replacement专用的消费式`shutdown_and_wait(self)`：结束并await项目actor、释放旧receiver和全部manager owner、等待state channel关闭；该调用返回后才顺序创建新runtime。Rust移动所有权和同一owner中的顺序`await`直接表达break-before-make，不增加active-slot或通用状态机。
- 新路径的100-cycle和FD/RSS/task/watch/receiver/refcount矩阵删除。最小确定性证据改为连续两次replacement、共三个runtime epoch：runtime 0曾有fresh后stuck，runtime 1在60秒窗口和outer delay后仍无fresh，runtime 2收到fresh并重置episode/backoff；每轮精确断言本地`shutdown_and_wait(self)`完成记录早于`runtime_created(n+1)`、每连接一次完整filtered subscription且无`unsubscribe`。服务端只需最终观察到旧连接关闭且没有自然重连，不要求该观察事件早于新runtime创建，也不等待Close ACK。既有自然transport 100-cycle保持历史/现存回归，不删除，也不作为本次新路径的验收依据。
- 不新增完整multi-source fixture；现有完整runtime/source隔离测试及最终一次workspace suite继续回归其他source、writer、parser、UTC rollover和coordinated shutdown。
- 8小时香港VPN probe只保留真实行为证据：clean HEAD与命令、冻结参数、起止UTC、进程内monotonic elapsed、退出状态、起止香港出口边界、fresh数量/最大间隔、错误/自然重连/business replacement计数；只有真实发生replacement时记录其顺序和first fresh耗时。不再要求PID、boot ID、boottime、周期route采样、socket/FD high-water或证据缺失后整段作废重跑。未发生可比真实事件时仍只能结论“8小时运行稳定；真实静默恢复效果未验证”。
- 该最小化裁决同步进入专项demand/plan、两份current authority、authority矩阵、参数报告和umbrella记录；上方旧条目保留当时审查与决策演进事实，不再代表当前实现要求。

## 2026-07-26：阶段0最小合同终审闭合

- 主代理完成15个阶段0文档的范围、自洽、引用、authority与`git diff --check`自审；工作树没有Rust、Cargo、vendor或测试代码改动。
- 三名`gpt-5.6-sol`/`ultra`独立subagent分别审查stop/ownership、测试范围和8小时probe。最终Critical/Important/Minor均为0。
- 终审唯一成立问题是把服务端观察旧socket关闭的时刻错误地设为新runtime创建前置条件。现行合同已改为只断言本地`shutdown_and_wait(self)`完成记录早于`runtime_created(n+1)`；服务端只需最终观察旧连接关闭且没有自然重连，不承担跨端事件先后断言，也不等待Close ACK。
- 承载本记录的阶段0文档commit成功push且本地HEAD、upstream与远端ref一致后，状态升级为`APPROVED_SHARED_RTDS_CONTRACT_STAGE0_CLOSED_STAGE1_CAPABILITY_PENDING`，并按既有持续授权直接进入阶段1任务1.0。若push或ref核对失败，该条件式闭合不生效。

## 2026-07-26：阶段1再次删除过度校验

- 阶段1初版实现曾引入重复candidate核对、test-only lifecycle记录、短时间负断言及额外观测receiver；主代理已在用户本次裁决前把这些实现删除，当前wrapper只通过唯一source owner消费的同一`RtdsRuntimeEvent`流交付message/state/local-applied，production没有第二RTDS message receiver、subscription actor或网络owner。
- 用户确认“简单、准确、可靠”优先，并明确把既有100-cycle也判定为本事项的过度校验。最终裁决是保留历史100-cycle测试文件与既有结果，不删除、不改写，但本事项不重新执行，也不把它们作为新business replacement的验收依据。
- 四段专用恢复耗时、owner-local `rtds_runtime_epoch`/fresh重复核对和test-only lifecycle completion record均从当前需求与计划删除。timer分支只读取一次可能异步变化的SDK generation/state/local-applied；replacement日志只保留trigger、runtime epoch、SDK generation、attempt index及replacement start/runtime created事件。
- break-before-make采用最小组合证据：wrapper loopback证明`shutdown_and_wait(self)`返回时state channel已关闭；生产源码直接await该调用后才构造新runtime；三个runtime聚焦loopback实际执行两次replacement并在第三个runtime收到fresh后重置episode/backoff。不得为复述相邻源码顺序增加人为延迟或test-only hook。
- 阶段1最终门收窄为RTDS lib受影响测试、`cargo test --workspace --locked --no-run`、workspace全target Clippy、fmt和diff check。默认workspace test不执行，避免重跑历史100-cycle；阶段2只报告已有日志和fresh row能够直接证明的事实，不强制构造四段耗时分布。

## 2026-07-26：阶段1候选实现与收窄验证门通过

- wrapper新增单一`RtdsRuntimeEvent`流中的state/local-applied观测，保留既有message freshness fence；source没有克隆第二message/state/applied receiver，也没有新增actor、socket、vendor patch或test-only lifecycle channel。
- `RtdsBusinessRecovery`只保存冻结行为直接需要的timing、稳定jitter seed、generation、Connected/applied后到观测、一个带kind的deadline、`ever_received_fresh`、episode trigger、replacement attempt和下一outer level。启动、普通message和新runtime创建后的重复SDK核对已删除，只在deadline到期时读取一次current SDK generation/state/local-applied。
- 自审发现原biased事件优先可能被连续非fresh消息饿死timer；实现改为每次事件处理后检查到期状态。三-runtime测试在runtime 0持续发送ignored消息，证明fresh仍优先且非fresh流量不能阻止300秒等价deadline。
- fresh的monotonic观测时刻在owner取得消息时冻结，不被下游queue等待向后推迟。实际replacement只输出两条有界日志，并包含trigger、runtime epoch、SDK generation和attempt index。
- `cargo test -p data-collect --locked --lib rtds_ -- --test-threads=1`通过20项、331项被精确filter；`cargo test --workspace --locked --no-run`完成全部测试target编译；workspace全target Clippy `-D warnings`、fmt check和diff check均退出0。历史100-cycle在本阶段早期、最终简化裁决前曾误跑一次；裁决后的收窄验证门没有再次执行，也未把该结果作为新路径验收依据。独立阶段终审仍待闭合。

## 2026-07-26：阶段1终审成立项完成最小修正

- wrapper后续generation测试在既有queued-message场景内补充`State + Applied`断言；paused-time恢复测试先实际提交一次outer backoff replacement使级别增长，再证明fresh把新episode恢复到初始级别，并由既有`retry_delay()`单点边界断言证明`300s`单次上限。没有新增测试文件、循环、资源矩阵或框架。
- timer分支改为各读取一次current state、generation、desired revision和applied observation，再用同一快照判断local-applied；删除会二次读取state/generation的source-facing predicate，避免重复校验及跨generation瞬时组合。
- 两条有界operator日志键精确统一为`trigger_kind`、`rtds_runtime_epoch`、`sdk_connection_generation`和`attempt_index`，与冻结术语及阶段2解析入口一致。
- 修正后RTDS lib精确测试仍为20项全部通过、331项被filter；最终workspace target编译、Clippy、fmt和diff gate也均退出0。
- 聚焦测试与需求AC的最小映射如下：`rtds_receive_deadline_returns_none_instead_of_a_fatal_timeout`及`rtds_old_queued_message_is_preserved_but_cannot_recover_new_generation`以initial/后续观测和排队旧消息覆盖AC2/AC7；`rtds_shutdown_waits_for_connection_loop_completion`以state-channel closure覆盖AC3/AC9；`rtds_business_timer_uses_late_anchor_and_preserves_cancelled_backoff_level`以cold start、后到anchor、transport取消、实际level增长、fresh reset和单点cap覆盖AC2/AC5/AC8；`rtds_business_silence_replaces_two_runtimes_then_fresh_resets_the_episode`以连续非fresh流量、三个runtime、两次replacement、每连接一次filtered subscription和第三runtime fresh覆盖AC3--AC7及AC9。上述输入、预期与实际均通过，未新增重复分类矩阵。

## 2026-07-26：阶段1三路终审通过并发布闭合

- runtime、测试和authority三路`gpt-5.6-sol`/Ultra独立终审在全部成立项修正后均为`PASS`，Critical/Important/Minor均为0；没有剩余SDK gap、上下文冲突或过度校验阻断。
- 最终门通过：RTDS lib精确测试20项通过、331项被filter；全部workspace测试target编译完成；workspace全target Clippy `-D warnings`、fmt check和diff check均退出0。最终简化裁决后没有再次运行历史100-cycle。
- 为遵守authority与Rust不得混入同一commit，阶段1文档验收先以commit `ef200a6e89eaaee9f19ff49a00232b5004825ce7`发布，Rust实现再以commit `a29486babe2682481549cfc0b95b010784d653f0`发布；两次push后本地HEAD、upstream与远端ref均一致。
- 承载本记录的闭合文档commit成功push且三方ref一致后，阶段1状态升级为`STAGE1_CLOSED_STAGE2_PROBE_PENDING`。阶段2只执行已批准的本地香港VPN连续8小时只读probe；production release、部署、远端操作和数据清理仍不在范围内。

## 2026-07-26：阶段2 Attempt1提前失败并永久保留

- Attempt1 artifact固定为`target/rtds-stage2-hk-probe-20260726T113120Z`；启动时clean HEAD与upstream均为`8db5b9bad683586c55fa49be80fa2aab73444646`，命令由该目录的`run-probe.sh`以`cargo test -p data-collect --locked --lib live_runtime::tests::rtds_source::rtds_hong_kong_vpn_eight_hour_probe -- --exact --ignored --nocapture --test-threads=1`执行。冻结参数仍为`300s + 60s/后到monotonic anchor + cold-start disabled + outer 1s × 2 ±20%/300s cap/unlimited`。
- probe自身开始UTC为`2026-07-26T11:36:01.101Z`，结束UTC为`2026-07-26T16:16:52.493Z`；香港route边界样本分别为`2026-07-26T11:36:00.300483874Z`与`2026-07-26T16:16:52.508616757Z`，两次均为Hong Kong/HK，二者不是同一字段。probe进程内运行`16851391ms`（`4h40m51.391s`），fresh `16127`、最大相邻fresh间隔`8521ms`、自然重连`2`、business replacement `0`、runtime error `1`、row error为null；test exit `101`、tee exit `0`、结束route采样exit `0`。
- terminal输入为127-byte顶层object，`message="Network error communicating with endpoint"`且不含`topic/type/timestamp/payload`；frame SHA-256为`5761fe3e33411429da70476c354075a9fff6816157636d173138eb0e362f6b69`。`probe.log` SHA-256为`409c82c936aab3ea0b489f67aed1ffa56b5f3da443371ae9a5d8d9550227fc6d`，`run-probe.sh` SHA-256为`5dc3f428e513414c62bc2ba704524e021d95e2bcd7fec31cfc0f3ea2577a90cb`。
- 因elapsed不足8小时且出现协议分类错误，Attempt1终态永久固定为`STAGE2_ATTEMPT1_FAIL_INCOMPLETE_8H`。未发生business replacement，因此它没有验证真实业务静默replacement效果；后续运行不得改写该结论。

## 2026-07-27：Attempt1辅助exit文件事后污染按E-A保留

- 主代理误执行`xxd -g 1 probe-test-exit.txt probe-tee-exit.txt`；`xxd`把第二个路径解释为输出文件，将运行结束时2-byte的`0\n`覆盖为64-byte十六进制文本。覆盖发生于`2026-07-27T11:13:54.650992513+09:00`，污染后SHA-256为`0e8b1f557d3d10e28d15f8626e890ea4c9015a048cc4e75099da78fa0b52a8cb`。
- 覆盖前本轮目录清单已确认该文件为2 bytes，独立审查曾读取原值`0`；未变的`wrapper-result.json`仍记录`probe_tee_exit=0`，其SHA-256为`b03aa139a791446bc66e9c90872d1bd36116bd09ad87d567b08138005836d87d`。`probe.log`、`run-probe.sh`及frame既有哈希继续匹配，因此Attempt1的`FAIL + INCOMPLETE_8H`核心结论不变。
- 用户批准E-A：保持污染文件现状，不静默恢复或重建，不再写入Attempt1目录；专项三件套及引用该证据的current changelog明确披露该单文件污染，不再声称整个目录逐文件保持运行结束时原样。

## 2026-07-27：用户批准方案A及修复后Attempt2

- 方案A只修改既有vendor allowlist内的`src/rtds/subscription.rs`与`src/ws/connection.rs`：正常typed parse失败后，仅将顶层object、精确`Network error communicating with endpoint`且无`topic/type/timestamp/payload`的数据外对象标为crate-private recoverable remote-network marker；不要求精确字段集合，也不要求`connectionId/requestId`存在。
- connection owner以typed downcast把该marker映射到既有`ConnectionExit::Recoverable`，关闭旧socket后沿用自然reconnect/backoff与latest desired replay。该对象不写row、不计fresh、不改变业务freshness/recovery episode或持久化；transport state/generation按既有自然重连变化。其他未匹配service message、array及目标typed/parser/schema错误继续terminal；仅有未typed的`Too Many Requests`字符串不等于已确认rate-limit，真实HTTP/protocol/typed rate-limit继续按availability恢复。
- 最小验证只增加一个正负合并runtime loopback：正例为精确对象，负例为相同message附带一个RTDS数据身份字段；既有once-replay、malformed sticky/no-reconnect、`Invalid request body`与bounded diagnostic测试继续复用。不增加collector raw parser、公共API、第五个vendor生产路径、第二owner/socket、新状态机/counter/schema、service-message表格矩阵、重复等待、资源计数或100-cycle。
- 当前状态为`STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_AUTHORITY_SYNC_PENDING`。纯文档authority完成独立审查、commit/push及三方ref核对后，才进入独立Rust实现/验证/审查/commit/push；全部闭合后从新clean HEAD、新artifact目录运行Attempt2。最终报告必须并列保留两次attempt。

## 2026-07-27：方案A实现、验证与分离发布闭合

- 纯文档authority commit `366fed7d19ff9ce2f855f362c81de5641b63f9c4`先行发布；其直接子提交、独立Rust commit `415470eda0b2d629c19c2609e6610a253703d684`随后发布。Rust commit只修改`rtds/subscription.rs`、`ws/connection.rs`和`polymarket_sdk_runtime_contract.rs`，共`161 insertions / 11 deletions`，没有混入文档、collector生产parser、公共API、第五个vendor路径或新状态机。
- 正常typed parse失败后，只有顶层object、精确network-error message且不存在`topic/type/timestamp/payload`时才生成crate-private marker；允许其他元数据字段且不要求`connectionId/requestId`。marker只保留原parser source与既有bounded diagnostic。connection owner以typed downcast进入既有`ConnectionExit::Recoverable`，旧socket收敛后沿用既有backoff、generation前进和latest desired replay；其他service message、array及typed/parser/schema错误继续sticky terminal。
- 新增正负合并runtime loopback使用当前毫秒的typed fresh frame，证明精确对象触发一次自然重连、第二连接收到同一完整filtered subscription、generation前进且可消费fresh；随后相同message加`topic`仍返回parser terminal。生产fresh classifier、既有once replay、whole-frame sticky terminal、vendor bounded-contract diagnostic及source-chain diagnostic精确测试均各`1/1`通过。
- `cargo test --workspace --locked --no-run`、workspace全target Clippy `-D warnings`、workspace与vendor fmt check及`git diff --check`均退出0。额外vendor direct Clippy先发现并修正本次`renamed_function_params`；最终只对未修改的`subscription.rs:650`既有`clippy::let_underscore_untyped`使用窄allow后退出0，没有豁免方案A新增代码问题。历史100-cycle、资源矩阵和默认workspace完整测试均未运行。
- runtime、测试和authority三路独立终审最终均为`Critical 0 / Important 0 / Minor 0 / PASS`。本地HEAD、upstream和远端`origin/fix-rtds-silents`在Rust发布后均精确为`415470eda0b2d629c19c2609e6610a253703d684`；Attempt1按E-A保留的污染文件仍为64 bytes、mtime `2026-07-27 11:13:54.650992513 +0900`、SHA-256 `0e8b1f557d3d10e28d15f8626e890ea4c9015a048cc4e75099da78fa0b52a8cb`，未被改写。
- 承载本记录的纯文档closure commit成功push且三方ref一致后，条件式状态升级为`STAGE2_ATTEMPT1_RECORDED_SOLUTION_A_IMPLEMENTATION_CLOSED_ATTEMPT2_PENDING`；唯一下一游标为从该新clean HEAD和新artifact目录执行Attempt2，最终报告继续并列保留两次attempt。

## 2026-07-27：Attempt2用户终止，Attempt3改为完整训练采集程序

- Attempt2从clean HEAD/upstream `0685fcab51a2b7c644ba7c5aca9c873134f88fd4`及香港`8.210.176.1 / HK / HKG`启动旧RTDS-only内存probe，约2.347秒观察到首条合格fresh。用户随后主动终止，并明确纠正测试对象应为完整`data-collect-training-collector`，对全部正式数据源采集并通过正式Parquet/manifest路径在本地落盘。旧进程已不存在；该attempt没有结束route、程序退出码、wrapper result或最终summary，因此永久记为`USER_TERMINATED + INCOMPLETE_8H`，不补造精确elapsed、最终计数或数据文件，也不记为代码FAIL。
- 曾考虑的RTDS-only JSONL方向与用户真实要求不符，已在形成Rust或已提交文档改动前停止。当前正式training binary已经原生组合四个logical producer、六个`LIVE_SOURCES`、`ManifestOwner`及typed Parquet writer，并支持`--max-runtime-secs 28800`；Attempt3无需新增Rust代码、JSONL、第二writer、第二RTDS owner/socket或probe专用production seam。
- 用户本次明确要求构成本专项三件套的窄范围扩展：Attempt3从承载本authority且已push的clean HEAD设置精确`DATA_COLLECT_GIT_COMMIT`重建本地release-profile binary，经既有香港proxychains路径及显式`--dataset-root <新绝对隔离路径>`连续运行完整训练采集代码路径8小时。它会真实写出当前source v3 + metadata v4及其他四类current schema，因此属于首次真实本地部分日v3/v4前向验证；不得把它误称为完整UTC日前向长跑、production binary激活、服务器/canonical dataset-root cutover、部署、migration或历史数据操作。
- Attempt3的本次全源验收只检查程序正常完成bounded runtime并协调finalize、六类source均产生非空finalized Parquet、manifest file facts与文件一致、每个manifest登记文件可由PyArrow打开且实际行数匹配，并且没有临时文件残留。若某source为零，必须如实报告该source未完成用户要求，但不能仅据此把collector进程或其他source误判为代码失败；不新增周期路由、资源矩阵或100-cycle。

## 2026-07-27：Attempt3完整collector提前失败，Gamma方案A获批

- Attempt3 artifact为`target/rtds-stage2-attempt3-full-training-20260727T042402Z`。构建和运行均来自clean/pushed `c05db6ba58676bec3e20878375608e395ad0971a`；正式release binary SHA-256为`b73f224b1217cef00592422d5dceedea786e5a5eaf70b154c5f2c0340b4b3337`，命令包含新绝对dataset root、`local-hong-kong-route`、`--max-runtime-secs 28800`和`--max-dataset-bytes 50000000000`。
- wrapper从`2026-07-27T04:27:37.565691017Z`运行至`2026-07-27T05:32:01.509592989Z`，monotonic elapsed为`3863.950s`；起止route样本均为`8.210.176.1 / HK / HKG`。collector exit为1、tee exit和结束route exit均为0。直接错误为`source_task_error:polymarket_market:Gamma resolution integrity failure:slug=btc-updown-5m-1785129600:ordinal=5`，不是RTDS failure；Attempt3固定为`FAIL + INCOMPLETE_8H`。
- 错误退出仍协调finalize了11个manifest登记Parquet。六源文件数/行数为：aggTrade `2/20259`、depth snapshot `1/1`、depth update `2/38521`、Polymarket market `2/1311849`、metadata `2/58`、RTDS `2/3365`。逐文件bytes、SHA-256、PyArrow schema/metadata/rows与manifest一致，无额外、`.running`或`.tmp`文件；同日manifest为正确的`open + ended_known`，并精确记录Polymarket source unexpected end和Gamma integrity error。六源非空只证明约64分钟内正式落盘，不满足8小时门。
- RTDS部分记录一次`steady_silence` replacement start及对应runtime-created，epoch从0到1；自然reconnect为0，3365行均为fresh，最大相邻接收间隔`304.875s`。日志没有可计算四段耗时的时间戳，不能宣称已完整验证replacement恢复耗时或八小时稳定性。
- offending metadata v4 row保留顶层`closed_time_raw="2026-07-27 05:25:52+00"`，其余terminal字段为`closed=true`、`acceptingOrders=false`、`umaResolutionStatus=resolved`及`outcome_prices_raw=["1","0"]`；因closedTime gate先失败，`winning_asset_id/winning_outcome`仍为null。市场结束为`05:25:00Z`。当前RFC3339-only helper将这个`E+52s`值误判为`closed_time_parse_error`，随后既有fail-closed路径正确终止collector。
- 用户批准Gamma `closedTime`方案A：只把真实顶层格式的精确`+00`后缀规范化为`+00:00`并交给同一Chrono parser，raw原样保存；非法、无时区、其他未批准格式及早于E继续fatal。不得使用nested event兜底或把integrity error降级。
- authority与最小Rust修复必须分离完成测试、三路审查、commit/push和三方ref核对。闭合后从新clean/pushed HEAD设置新`DATA_COLLECT_GIT_COMMIT`，用新release binary、新artifact和空dataset root执行完整六源8小时Attempt4；不得续写Attempt3、拼接剩余时间或恢复RTDS-only/100-cycle/资源矩阵。

## 2026-07-27：Gamma方案A实现闭合，Attempt4待启动

- Gamma authority commit `3a367386cb1768867e20a8190d2f13617b65fe67`先行发布；独立Rust commit `e086017fc6d376b739e7a5146dacc18f674f1407`随后只修改共享helper和一个Attempt3精确回归。精确测试`1/1`、完整`polymarket_contract` `7/7`、package tests check、目标Clippy `-D warnings`、fmt和diff-check全部通过。
- 三路独立代码终审均为Critical 0 / Important 0 / Minor 0；曾有一处审查报告把slug/start `05:20Z`误称为市场E，已复核更正为E=`05:25Z`、closedTime=`05:25:52Z`即E+52秒，不影响代码或验收。Rust发布后本地HEAD、upstream和远端ref均为`e086017fc6d376b739e7a5146dacc18f674f1407`。
- 本轮没有修改RTDS、SDK、Cargo、schema、writer、evaluator、adapter、fatal路径、raw持久化或Attempt3 artifact，也没有运行RTDS-only probe、100-cycle或资源矩阵。承载本closure的纯文档commit成功push并完成三方ref核对后，唯一下一游标为步骤2.2.10：从该新clean/pushed HEAD设置精确`DATA_COLLECT_GIT_COMMIT`，用新release binary、新artifact和空dataset root运行完整六源8小时Attempt4。

## 2026-07-27：Attempt4完整collector提前失败

- Attempt4 artifact为`target/rtds-stage2-attempt4-full-training-20260727T071429Z`。
  构建和运行均来自clean/pushed
  `a37cf1181597296b9b2d87f8bc2c93700bda8f48`；正式release binary SHA-256为
  `b451b2b53aace314e28503ff4f983c20a7468d8175abfd5c7450d8fa7942cc0a`，命令
  包含新绝对dataset root、`local-hong-kong-route`、`--max-runtime-secs 28800`
  和`--max-dataset-bytes 50000000000`。
- wrapper从`2026-07-27T07:20:35.073560320Z`运行至
  `2026-07-27T09:37:07.171376781Z`，monotonic elapsed为`8192.100s`；起止route
  均为`8.210.176.1 / HK / HKG`。collector exit 1、tee exit与结束route exit均为0，
  因此Attempt4固定为`FAIL + INCOMPLETE_8H`。
- manifest首因是
  `polymarket_sdk_stream_fatal:parser error: Internal: invalid value: string "", expected a Decimal type representing a fixed-point number`，
  并记录`SourceInnerReturn { source_kind: "polymarket_market" }`。collector最终打印
  的Binance depth after-close和manifest中的RTDS after-close发生在同日gate关闭之后，
  都是连锁后果，不是首因；当前RTDS恢复合同没有因此失败或改变。
- 错误退出仍协调finalize了16个manifest登记Parquet，共`1,884,425`行、
  `115,390,221` bytes。六源文件数/行数为：aggTrade `3/74380`、depth snapshot
  `1/1`、depth update `3/81751`、Polymarket market `3/1720377`、metadata
  `3/141`、RTDS `3/7775`。逐文件path/bytes/SHA-256/PyArrow rows与manifest一致，
  无额外、`.running`、`.tmp`或`.partial`文件；manifest SHA-256为
  `ee43925d1ebe3f9b8d83555cd08a40e287832c049cda89f348ead95d7d2acab5`。
  同日`open + ended_known`符合当前错误退出合同。这些只证明约2小时16分钟内六源
  正式落盘，不满足8小时门。
- RTDS的`7775/7775`行均为`fresh`，按`timestamp_received`计算的最大相邻接收间隔为
  `9382ms`；manifest记录`reconnect_events=1`，wrapper记录business replacement
  start/runtime-created为`0/0`。这些是本次部分窗口的实际计数，不冒充完整8小时
  稳定性或真实business replacement效果验收。
- 失败raw frame、event variant和字段path没有保存；最后成功typed row也不能唯一证明
  下一raw frame。因此不能把首因归给F04、`price_change`或任一具体Decimal字段，
  也不能声称已经唯一修复Attempt4。

## 2026-07-27：用户批准P-B1E并冻结Attempt5游标

- 用户批准本次E-A语义；为避免与Attempt1辅助文件污染处置的历史`E-A`重名，当前
  唯一代号为`P-B1E`。其唯一技术authority是SDK070三件套：仅九个官方nullable
  Decimal把精确`""`解码为`None`；missing/null/official legal DecimalString保持，
  nullable非空malformed及required `price_change.size=""`继续parser fatal。
- P-B1E只修改既有vendor生产差异路径`src/clob/ws/types/response.rs`并复用现有
  `serde_with::NoneAsEmptyString`。不使用`DefaultOnError`，不把空串改成零，不新增
  raw recorder、第二parser、第五个vendor生产路径、依赖、schema/counter、诊断框架、
  RTDS-only probe、100-cycle或资源矩阵。
- authority与最小Rust实现继续分离完成三路审查、commit/push和三方ref核对。承载
  本记录的纯文档authority commit成功push且三方ref一致后，条件式状态为
  `P_B1E_AUTHORITY_SYNC_CLOSED_IMPLEMENTATION_PENDING`；实现闭合后从新
  clean/pushed HEAD设置新`DATA_COLLECT_GIT_COMMIT`，使用新release binary、
  新artifact和空dataset root执行完整六源8小时Attempt5。不得续写Attempt4、拼接
  剩余时间或自动扩大空串容忍范围。

## 2026-07-27：P-B1E实现闭合，Attempt5待运行

- authority commit
  `15feb1217d182eea3ef909f51f3ee0a700b918db`与独立Rust commit
  `9d8e59b1fbeb9db04b20c2a00402b36b45f228f7`已按边界分离发布；Rust发布后
  本地HEAD、upstream和`origin/fix-rtds-silents`均指向`9d8e59b1`。
- 实现只修改既有vendor生产差异路径`src/clob/ws/types/response.rs`：九个获批
  `Option<Decimal>`字段复用`NoneAsEmptyString`，missing/null/合法非空值保持；
  nullable非空malformed及required `price_change.size=""`继续fatal。没有新增依赖、
  parser、生产路径、schema/counter、诊断、raw recorder、100-cycle或资源矩阵。
- TDD RED精确复现Decimal空串错误类；response聚焦测试`32/32`、现行v3直接消费者
  `1/1`及vendor合法非空WebSocket测试`4/4`通过。vendor全feature
  lib/tests/examples编译、data-collect `--lib` check、fmt与diff-check通过。
  Clippy首次只命中两类已记录且未修改的vendor基线；精确豁免这两类后，其余warning
  继续以`-D warnings`通过。
- 三路`gpt-5.6-sol`/`ultra`独立终审均为
  `PASS / CLOSURE_UNBLOCKED`，Critical 0 / Important 0 / Minor 0。Stage3已删除
  current-v2 current consumer，用户批准把P-B1E计划中的陈旧验证措辞最小纠正为
  现行v3直接消费者与vendor F04合法值测试；没有恢复v2或增加测试矩阵。
- P-B1E只证明九个官方nullable字段的精确空串边界，仍不证明已定位Attempt4的未知
  字段，也不保证Attempt5不会出现新的parser证据。承载本closure的纯文档commit
  成功push且三方ref一致后，当前唯一游标为从该新clean/pushed HEAD、新artifact和
  空dataset root运行完整六源8小时Attempt5；若同类Decimal空串错误复发，立即停止并
  报告新证据，不自动扩张nullable范围。
