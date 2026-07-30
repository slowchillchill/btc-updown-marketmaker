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
> 对应 changelog：`changelog/data-collect-current-authority-consolidation-changelog.md`
>
> 状态：`COMPLETE`

## 用户批准的目标

1. 将 2026-07-28 事故事项更新为用户已验收；真实 UTC rollover 和完整健康 UTC 日
   不再作为完成阻断门，但不得伪称未记录的证据已经发生。
2. paper/live 真实 external-source adapter 不属于 `model-train`，保持延期；等下一
   模块 `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成
   之后，只有在用户决定返回 `data-collect` 时才另立三件套。
3. 全面审计 data-collect 相关文档，以当前代码、测试和最新决策修正冲突。
4. 建立单一 current 入口；历史事实完整保留，但不能从活动搜索片段取得执行
   authority。

## 验收标准

1. 模块完成、移除的运行证据门和 paper/live 延期边界在 current 层一致。
2. 当前 SDK/schema/runtime/存储/部署边界可由 Cargo、代码和测试核实。
3. `data-collect/docs/CURRENT.md` 是唯一 current 入口，master 三件套和六个主题 owner
   唯一。
4. 45 份历史长文由 annotated tag、commit、原路径和 blob 完整恢复，并从活动树移除。
5. README 和旧入口仅作 route-only，不复制 current 规范或可执行命令。
6. 四份跨模块 mistake-log 保持 evidence-only、authority none。
7. `historical` 局部读取要求同 blob banner；不保证有 banner 的 `snapshot` 由
   manifest kind、活动同路径和 CURRENT owner 判为已替代且 authority none；自动
   guard 固定两类协议，并能拒绝重复 CURRENT、坏三件套引用、坏 archive blob、
   恢复历史路径和 evidence banner 缺失。
8. 每个阶段完成独立审查、commit 和 push；最终只以 ordinary fast-forward 更新
   `origin/main`。

## 非范围

- 不修改 data-collect Rust runtime、schema 或交易逻辑；允许同步 Markdown 契约测试。
- 不部署、启停或操作远端服务，不删除远端数据。
- 不提前实现 paper/live 真实 adapter。
- 不改写历史事实，只隔离其当前执行权限。

详细实施证据保存在对应 changelog；本文件不再拥有执行 authority。
