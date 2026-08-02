# data-collect 当前权威与历史隔离整理需求

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 生命周期：`GOVERNANCE_RECORD`
>
> 当前执行 authority：`none`
>
> superseded_by：`data-collect/docs/CURRENT.md`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 对应变更日志：`changelog/data-collect-current-authority-consolidation-changelog.md`
>
> 状态：`COMPLETE`

## 用户批准的目标

1. 将 2026-07-28 事故事项更新为用户已验收；真实 UTC 跨日切换和完整健康 UTC 日
   不再作为完成阻断门，但不得伪称未记录的证据已经发生。
2. 模拟盘和实盘的真实外部数据源适配器不属于 `model-train`，保持延期；等下一
   模块 `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成
   之后，只有在用户决定返回 `data-collect` 时才另立三件套。
3. 全面审计 data-collect 相关文档，以当前代码、测试和最新决策修正冲突。
4. 建立单一当前入口；历史事实完整保留，但不能从活动搜索片段取得执行权威。

## 验收标准

1. 模块完成、移除的运行证据门和模拟盘、实盘延期边界在当前层一致。
2. 当前 SDK、数据模式、运行时、存储和部署边界可由 Cargo、代码和测试核实。
3. `data-collect/docs/CURRENT.md` 是唯一当前入口，主三件套和六个主题归属文档
   唯一。
4. 45 份历史长文可由带注释标签（`annotated tag`）、标签对象（`tag object`）、
   解引用提交（`peeled commit`）、原路径和 blob 完整恢复，并从活动树移除。
5. README 和旧入口仅用于路由，不复制当前规范或可执行命令。
6. 四份跨模块错误记录保持仅作证据、无执行权威。
7. 局部读取 `kind=historical` 行时要求核对同一 blob 的首屏标记；不保证含首屏标记的
   `kind=snapshot` 行由清单中的 `kind=snapshot`、活动树同路径文件和 `CURRENT`
   主题归属文档判为已替代且无执行权威；自动检查固定两类协议，并能拒绝重复 CURRENT、
   错误的三件套引用、错误的归档 blob、恢复历史路径和证据首屏标记缺失。
8. 每个阶段完成独立审查、提交和推送；最终只以常规快进方式更新
   `origin/main`。

## 非范围

- 不修改 data-collect 的 Rust 运行时、数据模式或交易逻辑；允许同步 Markdown 契约测试。
- 不部署、启停或操作远端服务，不删除远端数据。
- 不提前实现模拟盘和实盘的真实适配器。
- 不改写历史事实，只隔离其当前执行权限。

详细实施证据保存在对应变更日志；本文件不再拥有执行权威。
