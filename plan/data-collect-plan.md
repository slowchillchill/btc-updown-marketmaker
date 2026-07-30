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
历史开发步骤由 archive manifest 指定的 Git 对象保留，不在本文件重放。

## 1. 已完成实施单元

| 单元 | 当前结果 | 验证 owner |
| --- | --- | --- |
| binary 与职责隔离 | training 真实六源；paper/live heartbeat/status 实现，完整 loop 尚无 E2E | `current/interfaces.md` |
| SDK 与 typed schema | SDK 0.7.0 + 唯一 patch；六个 current schema | `current/contracts.md` |
| storage | receive-time UTC day/source/hour、finalized Parquet、daily manifest | `current/contracts.md` |
| bounded runtime | collector data queues 有 hard bound；SDK control queue 仅逻辑约束；协调 stop/rollover | `current/contracts.md` |
| capacity | 默认 50GB；startup direct 75；runtime 协调 finalize/75；无自动 raw 删除 | `current/operations.md` |
| RTDS | 300/60 replacement、×2/±20%/300s、无独立 exhaustion、fresh reset | `current/contracts.md` |
| identity/resolution | fixed identity、E+3..E+8、terminal once、restart fail closed | `current/contracts.md` |
| operations | clean pushed commit 本地构建、固定路径/unit、无远端编译 | `current/operations.md` |
| document authority | 单一 CURRENT、主题 owner、tag/manifest 历史隔离 | `current/document-authority-audit.md` |

## 2. 当前验证方式

1. 对需求每项合同核对 current owner、Cargo/代码和测试。
2. 运行 fmt、check、strict clippy、current integration targets、lib regression 和
   document authority guard。
3. 对唯一 wall-clock fixture 使用精确 named skip，并明确记录限制；不得扩大 skip 或
   把排除后的结果写成无条件 full-suite PASS。
4. 每次 current 文档、代码或测试变化都必须更新
   `data-collect/docs/current/acceptance.md` 的真实 dated snapshot。

精确命令和最新结果由 `data-collect/docs/current/acceptance.md` 单独拥有。

## 3. 完成门

本计划已满足：

- current module contract 与代码/测试一致；
- 用户已完成事故和模块最终验收；
- 未取得的真实 UTC 日证据没有被伪造成 PASS，也不再阻断完成；
- paper/live 真实 adapter 明确延期；
- 历史长文已冻结并移出活动搜索面；
- 当前文档不携带旧执行游标。

因此状态为 `COMPLETE_NO_EXECUTION_CURSOR`。

## 4. 未来变更规则

任何新增 source、schema、SDK 路线、paper/live adapter、部署、远端状态变更或 raw
清理都必须：

1. 先核对 `data-collect/docs/CURRENT.md`、当前代码和测试；
2. 新建同一事项编号的 demand/plan/changelog；
3. 写出需求与验收标准、逐步实施和测试；
4. 遇到 current 文档、代码、环境或用户描述冲突时停止并由用户决定；
5. 完成后更新 current owner，而不是把历史计划恢复成执行入口。
