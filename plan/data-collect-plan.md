# data-collect 当前主计划

> 事项编号：`DATA-COLLECT-CURRENT`
>
> 生命周期：`CURRENT_MASTER_PLAN_CLOSED`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-demand.md`
>
> 对应 changelog：`changelog/data-collect-changelog.md`
>
> 状态：`COMPLETE_NO_EXECUTION_CURSOR`

本计划记录当前已实现模块的验证闭环，不含待执行代码、部署或远端操作任务。
历史开发步骤由归档清单指定的 Git 对象保留，不在本文件重放。

## 1. 已完成实施单元

| 单元 | 当前结果 | 验证归属文档 |
| --- | --- | --- |
| 二进制程序与职责隔离 | 训练程序连接六个真实数据源；模拟盘和实盘的心跳、状态已实现，完整循环尚无端到端测试 | `current/interfaces.md` |
| SDK 与类型化 schema | SDK 0.7.0 加唯一补丁；六个当前 schema | `current/contracts.md` |
| 存储 | 以接收时间划分 UTC 日、数据源和小时，写最终化 Parquet 与每日清单 | `current/contracts.md` |
| 有界运行时 | 采集器数据队列有硬上限；SDK 控制队列仅有逻辑约束；协调停止和 UTC 日切换 | `current/contracts.md` |
| 容量 | 默认 50GB；启动时直接退出 75；运行时协调最终化并退出 75；不自动删除原始数据 | `current/operations.md` |
| RTDS | 300/60 秒替换、×2/±20%/300 秒退避、无独立耗尽门、有效新鲜消息后重置 | `current/contracts.md` |
| 身份与市场判定 | 固定身份信息、E+3..E+8、终态仅一次、重启时失败即关闭 | `current/contracts.md` |
| 运行 | 干净且已推送的提交在本地构建、固定路径和单元、远端不编译 | `current/operations.md` |
| 文档权威 | 单一 `CURRENT`、主题归属文档、标签和清单隔离历史 | `current/document-authority-audit.md` |
| 跨模块顺序 | 当前后继为 `data-pipeline(training)` 的需求/合同和最小真实纵向链路；随后以黄金事件和真实管道事件验收 `sim-system`，再完成训练管道闭环；模拟盘和实盘适配器延期门保持不变 | `data-collect/docs/CURRENT.md`、主三件套 |
| 低频平台规则边界 | `sim-system` 新运行通过固定官方公开 HTTPS 窄加载器冷启动查询并冻结；该模拟器例外不改变 `data-collect` 官方 Rust SDK 合同，当前 schema 不增规则专用列，原始证据 JSON 不删键，动态 tick 事件继续保留 | `current/contracts.md`、`current/known-limitations.md` |

## 2. 当前验证方式

1. 对需求每项合同核对当前主题归属文档、Cargo、代码和测试。
2. 运行格式、检查、严格 Clippy、当前集成目标、库回归测试和文档权威守卫。
3. 对唯一依赖实际时钟的测试夹具使用精确的具名跳过，并明确记录限制；不得扩大跳过
   范围，或把排除后的结果写成无条件全套测试通过。
4. 每次当前文档、代码或测试变化都必须更新
   `data-collect/docs/current/acceptance.md` 的真实日期快照。
5. 跨模块顺序变更还必须检索活动的当前文档、主三件套和全部跨模块草案/记录中的旧“下一
   模块”措辞，运行文档权威守卫与 `git diff --check`，且不得改写历史
   治理记录。
6. 低频规则责任变化只做字段所有权、当前 schema 和只读空间反事实核验；除非未来另立并
   批准新的 `data-collect` 事项，不修改采集代码/schema，也不重写既有 Parquet 或证据摘要。

精确命令和最新结果由 `data-collect/docs/current/acceptance.md` 单独拥有。

## 3. 完成门

本计划已满足：

- 当前模块合同与代码、测试一致；
- 用户已完成事故和模块最终验收；
- 未取得的真实 UTC 日证据没有被伪造成通过，也不再阻断完成；
- 模拟盘和实盘的真实适配器明确延期；
- 历史长文已冻结并移出活动搜索面；
- 当前文档不携带旧执行游标。

因此状态为 `COMPLETE_NO_EXECUTION_CURSOR`。

## 4. 未来变更规则

任何新增数据源、schema、SDK 路线、模拟盘和实盘适配器、部署、远端状态变更或原始数据
清理都必须：

1. 先核对 `data-collect/docs/CURRENT.md`、当前代码和测试；
2. 新建同一事项编号的需求、计划和变更记录；
3. 写出需求与验收标准、逐步实施和测试；
4. 遇到当前文档、代码、环境或用户描述冲突时停止并由用户决定；
5. 完成后更新当前主题归属文档，而不是把历史计划恢复成执行入口。
