# data-collect 当前权威与历史隔离整理计划

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 生命周期：`GOVERNANCE_RECORD`
>
> 当前执行 authority：`none`
>
> superseded_by：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应 changelog：`changelog/data-collect-current-authority-consolidation-changelog.md`
>
> 状态：`COMPLETE`

## 阶段 1：完成旧验收门与延期边界

- 事故三件套统一为用户最终验收。
- UTC rollover/full healthy day 从 completion gate 移除，不伪造运行证据。
- paper/live 真实 adapter 不属于 `model-train`，保持延期；等下一模块
  `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成之后，
  只有在用户决定返回 `data-collect` 时才另立三件套。
- 独立审查通过后 commit/push。

## 阶段 2：完成全量文档审计

- 以 Cargo、代码、测试和最新决策建立 current 基线。
- 审计 61 份相关 Markdown，处置为 12 current/active、45 historical、4
  evidence-only。
- 修正旧 SDK/schema、RTDS、retention、部署、Attempt 游标与 paper/live 能力冲突。
- 独立复审通过后 commit/push。

## 阶段 3：完成

- annotated tag 冻结阶段 2 commit，并推送远端。
- archive manifest 记录 45 个历史对象和 11 个被 current 层替换的 snapshot。
- 分开固定读取协议：historical 要求同 blob 首屏 banner；snapshot 不依赖旧 blob
  banner，以 manifest kind、活动同路径和 CURRENT owner 判定 authority none。
- 建立唯一 CURRENT、六个主题 owner、精简 README、route-only 兼容页和 current
  master triplet。
- 从活动树移除 45 份历史长文；四份跨模块 evidence input 保留但无 authority。
- 更新 AGENTS 路由和跨模块 draft 边界。
- 新增 document authority contract 及不改工作树的负例 fixture。

## 当前完成门

1. 运行 document authority、部署文档、binary、SDK、schema、layout、manifest 和
   handoff integration targets。
2. 运行 lib regression，精确处理已知 wall-clock fixture。
3. 运行 fmt、check、strict clippy 与 diff check。
4. 自审通过后由 `gpt-5.6-sol/ultra` 独立审查；修复问题并复审。
5. 阶段 3 commit/push 后做最终独立审查，再 ordinary fast-forward 到 main。

本计划不含 Rust 实现、部署、远端操作或数据删除游标。
