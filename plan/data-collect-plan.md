# data-collect 当前主计划

> 事项编号：`DATA-COLLECT-CURRENT`
>
> 生命周期：`CURRENT_MASTER_PLAN_CLOSED`
>
> 当前执行 authority：`plan/data-collect-plan.md`
>
> 对应需求：`demand/data-collect-demand.md`
>
> 对应 changelog：`changelog/data-collect-changelog.md`
>
> 状态：`COMPLETE_NO_EXECUTION_CURSOR`

本计划记录当前已实现模块的验证闭环，不含待执行代码、部署或远端操作任务。
历史开发步骤由归档清单指定的 Git 对象保留，不在本文件重放。

本模块继续复用官方 Polymarket Rust SDK 及仓库唯一补丁、现有 Tokio 运行时、Arrow/Parquet
写入链、当前 schema 和既有合同测试，不引入第二套 SDK、协议客户端、存储框架或部署系统。
这是已完成模块的当前复用结论，不需要为本轮文档与测试职责清理新增技术试验；任何未来
生产能力变化必须在新计划中重新调研成熟工具可行性并定义必要的最小试验。

## 1. 已完成实施单元

| 单元 | 当前结果 | 派生参考 |
| --- | --- | --- |
| 二进制程序与职责隔离 | 训练程序连接六个真实数据源；模拟盘和实盘的心跳、状态已实现，完整循环尚无端到端测试 | `current/interfaces.md` |
| SDK 与类型化 schema | SDK 0.7.0 加唯一补丁；六个当前 schema | `current/contracts.md` |
| 存储 | 以接收时间划分 UTC 日、数据源和小时，写最终化 Parquet 与每日清单 | `current/contracts.md` |
| 有界运行时 | 采集器数据队列有硬上限；SDK 控制队列仅有逻辑约束；协调停止和 UTC 日切换 | `current/contracts.md` |
| 容量 | 当前生产仍按默认 50GB 在启动和运行期扫描；本地候选只保留启动扫描，并新增尚未部署的 30GB 每日外部清理 | `current/operations.md` |
| RTDS | 300/60 秒替换、×2/±20%/300 秒退避、无独立耗尽门、有效新鲜消息后重置 | `current/contracts.md` |
| 身份与市场判定 | 固定身份信息、E+3..E+8、终态仅一次、重启时失败即关闭 | `current/contracts.md` |
| 运行 | 干净且已推送的提交在本地构建、固定路径和单元、远端不编译 | `current/operations.md` |
| 跨模块顺序 | 当前后继为 `data-pipeline(training)` 的获批需求、规范计划和最小真实纵向链路；随后以黄金事件和真实管道事件验收 `sim-system`，再完成训练管道闭环；模拟盘和实盘适配器延期门保持不变 | `data-collect/docs/CURRENT.md`、主三件套 |
| 低频平台规则边界 | `sim-system` 当前时点运行通过固定官方公开 HTTPS 窄加载器查询并冻结，历史运行只绑定适用的 `HISTORICAL_AS_OF` 快照；该模拟器例外不改变 `data-collect` 官方 Rust SDK 合同，当前 schema 不增规则专用列，原始证据 JSON 不删键，初始与动态 tick 证据都经 `data-pipeline` 进入规范事件链 | `current/contracts.md`、`current/known-limitations.md` |

## 2. 当前业务与技术合同

1. Cargo 保持训练、模拟盘热路径和实盘热路径三个隔离的生产角色二进制程序；只有训练程序
   连接真实 Polymarket、RTDS 和 Binance 数据源。`real_source_alignment_probe` 是只由操作员
   显式运行的本地验证二进制程序，不是生产角色，也不会作为 bench 被普通 Cargo 回归执行。
   模拟盘和实盘当前只提供回执、Unix 套接字、心跳和状态，其真实适配器必须在
   `model-train` 完成且用户决定返回本模块后另立事项。
2. Polymarket 路径只使用官方 Rust SDK `0.7.0` 和仓库唯一 `0.7.0-p1` 补丁。当前新写入
   schema 精确为 `polymarket_orderbook_v3`、`polymarket_market_metadata_v5`、
   `polymarket_rtds_chainlink_btc_usd_v4`、`binance_depth_snapshot_v3`、
   `binance_depth_update_v4` 和 `binance_aggtrade_v7`；旧 schema 只服务代码明确保留的恢复
   读取，不得用于新写入。
3. 数据按采集器接收时间划分 UTC 日、数据源和小时；`*.running` 不是最终化产物。每个 UTC
   日只有一个每日清单 v3 和一个归属方，UTC 日切换必须先完成旧日生产者围栏、写入器
   最终化、清单会合和持久关闭，再启用新日。
4. 采集器拥有的数据、准入、延后处理和写入器队列或批次均有硬上限。SDK 出站请求与
   `heartbeat PING` 控制通道只有逻辑调用约束，不冒充容量硬上限。当前生产部署的容量默认
   `50_000_000_000` 字节：启动已达上限时在创建运行产物前退出 `75`；运行中每 60 秒检查，
   达到上限后停止准入、排空、最终化并退出 `75`。本地已验证候选只保留启动扫描，运行期间
   不再递归扫描；同时新增根文件系统 30GB 水位的独立每日清理，每次只删除一个严格早于 UTC
   昨日的最早日期。该候选已提交并推送但尚未部署，当前生产行为尚未改变。
5. 普通数据源静默不终止整体进程。RTDS 在首次有效新鲜消息前禁用看门狗，之后使用稳态
   300 秒或首次有效消息 60 秒替换门；外层退避从 1 秒按 ×2、±20% 确定性抖动增长，单次
   上限 300 秒，有效新鲜消息后重置，无独立尝试次数或累计耗时耗尽门，并严格先断开再连接。
6. 市场固定身份信息在当前、下一、重启和判定流程中不可变。判定查询固定为 E+3 至 E+8；
   合格终态只关闭一次，耗尽不得伪造胜方。多重权威、固定身份或 schema 矛盾及终态胜方
   冲突失败即关闭；不属于当前角色或超过预算的迟到终态事件忽略。
7. `sim-system` 的低频平台规则由其当前时点运行通过获批公开 HTTPS 窄加载器查询并冻结，
   历史运行只绑定适用的 `HISTORICAL_AS_OF` 快照。采集原始证据键不删除；初始
   `orderPriceMinTickSize` 和动态 `tick_size_change` 都经 `data-pipeline` 进入事件链，
   不属于模拟器冷启动规则快照。
8. 发布二进制文件只在本地从干净且已推送的提交构建并记录提交与 SHA-256；远端不编译、
   不走服务器 `git pull`。本计划不授权上传、部署、启停服务、替换产物、删除数据或改变
   远端状态。

以上条款由本计划拥有；`data-collect/docs/current/` 只按使用场景重排这些内容并提供命令或
接口参考，不得新增、删减或改变业务与技术语义。

## 3. 当前验证方式

1. 对需求每项验收核对本计划、当前主题归属文档、Cargo、代码和测试。
2. 运行格式、检查、严格 Clippy、当前集成目标和库回归测试。
3. 对唯一依赖实际时钟的测试夹具使用精确的具名跳过，并明确记录限制；不得扩大跳过
   范围，或把排除后的结果写成无条件全套测试通过。
4. `data-collect/docs/current/acceptance.md` 只维护当前测试命令、职责边界和已知限制；每次
   实际验证结果只记录到 changelog，不在当前验收页累积带日期快照。
5. 跨模块顺序变更还必须检索活动的当前文档、主三件套和全部跨模块草案/记录中的旧“下一
   模块”措辞，运行 `git diff --check`，且不得改写历史
   治理记录。
6. 低频规则责任变化只做字段所有权、当前 schema 和只读空间反事实核验；除非未来另立并
   批准新的 `data-collect` 事项，不修改采集代码/schema，也不重写既有 Parquet 或证据摘要。

当前精确命令由 `data-collect/docs/current/acceptance.md` 提供；实际运行结果由 changelog
按发生时间记录，不构成当前规范。

## 4. 完成门

本计划已满足：

- 当前模块合同与代码、测试一致；
- 用户已完成事故和模块最终验收；
- 未取得的真实 UTC 日证据没有被伪造成通过，也不再阻断完成；
- 模拟盘和实盘的真实适配器明确延期；
- 历史长文已冻结并移出活动搜索面；
- 当前文档不携带旧执行游标。

因此状态为 `COMPLETE_NO_EXECUTION_CURSOR`。

## 5. 未来变更规则

任何新增数据源、schema、SDK 路线、模拟盘和实盘适配器、部署、远端状态变更或原始数据
清理都必须：

1. 先核对 `data-collect/docs/CURRENT.md`、当前代码和测试；
2. 新建简洁业务需求和同一事项编号的规范计划；开发事实发生时再写入对应 changelog；
3. 在计划中写出成熟工具复用调研、必要技术试验、逐步实施和符合需求的测试；
4. 遇到当前文档、代码、环境或用户描述冲突时停止并由用户决定；
5. 完成后更新当前主题归属文档，而不是把历史计划恢复成执行入口。
