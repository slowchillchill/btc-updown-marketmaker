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
> 对应变更日志：`changelog/data-collect-current-authority-consolidation-changelog.md`
>
> 状态：`COMPLETE`

## 阶段 1：完成旧验收门与延期边界

- 事故三件套统一为用户最终验收。
- UTC 跨日切换和完整健康日不再作为完成门，不伪造运行证据。
- 模拟盘和实盘的真实适配器不属于 `model-train`，保持延期；等下一模块
  `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成之后，
  只有在用户决定返回 `data-collect` 时才另立三件套。
- 独立审查通过后提交并推送。

## 阶段 2：完成全量文档审计

- 以 Cargo、代码、测试和最新决策建立当前基线。
- 审计 61 份相关 Markdown，处置为 12 份当前或活动文档、45 份历史文档、4 份
  仅作证据的文档。
- 修正旧 SDK、数据模式、RTDS、保留策略、部署、尝试游标与模拟盘、实盘能力冲突。
- 独立复审通过后提交并推送。

## 阶段 3：完成

- 用带注释标签（`annotated tag`）冻结阶段 2 的解引用提交（`peeled commit`），并推送
  远端。
- 归档清单记录 45 条 `kind=historical` 和 11 条被当前层替换的 `kind=snapshot`，
  每条均对应精确路径与 blob。
- 分开固定读取协议：`kind=historical` 行要求核对同一 blob 的首屏标记；
  `kind=snapshot` 行不依赖旧 blob 的首屏标记，以清单中的 `kind=snapshot`、活动树
  同路径文件和 `CURRENT` 主题归属文档判定为无执行权威。
- 建立唯一 CURRENT、六个主题归属文档、精简 README、仅路由兼容页和当前主三件套。
- 从活动树移除 45 份历史长文；四份跨模块证据输入保留但无执行权威。
- 更新 AGENTS 路由和跨模块草案边界。
- 新增文档权威契约及不改工作树的负例夹具。

## 当前完成门

1. 运行文档权威、部署文档、二进制、SDK、数据模式、布局、清单和交接集成测试目标。
2. 运行库回归测试，精确处理已知的挂钟时间夹具。
3. 运行格式化检查、编译检查、严格 Clippy 检查与差异检查。
4. 自审通过后由 `gpt-5.6-sol/ultra` 独立审查；修复问题并复审。
5. 阶段 3 提交并推送后做最终独立审查，再以常规快进方式更新 `main` 分支。

本计划不含 Rust 实现、部署、远端操作或数据删除游标。
