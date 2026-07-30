# data-collect 当前权威与历史隔离整理 Changelog

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 状态：`PHASE_1_COMPLETE_PHASE_2_PENDING`

## 2026-07-30：用户批准范围与执行顺序

- 用户确认接受“当前权威层 + 隔离历史层 + 权威校验门”的文档治理方向。
- 用户明确要求先关闭 `ROLLOVER_PENDING`：2026-07-28 事故事项已经验收，UTC day
  rollover 和完整健康 UTC 日不再作为完成阻断门。
- 该决策只移除阻断门，不把尚未记录的 rollover/full-day 运行证据改写为已经发生。
- 用户决定 paper/live 真实外部市场源 adapter 延后到训练模块完成后再返回实施；当前
  heartbeat/status 能力保持不变，该延期不阻断本轮 `data-collect` 收尾。
- 第二步必须全面审查所有 `data-collect` 相关文档，以当前代码、测试和最新决策修正
  上下文冲突；历史事实保留，但旧状态不能继续冒充当前 authority。
- 上述两步完成并分别通过独立阶段审查后，才实施当前权威层、Git 历史冻结、活动搜索
  面隔离和自动一致性检查。

## 2026-07-30：阶段 1 实施与自审

- 2026-07-28 事故三件套顶部状态统一为
  `CLOSED_BY_USER_ACCEPTANCE_20260730`。
- 事故需求取消 UTC day rollover 与完整健康 UTC 日的 completion gate；事故计划把
  6.3/6.4 标记为由用户移除的原计划任务；事故 changelog 保留当时 pending 事实，并
  追加用户最终验收章节。
- 主需求、主计划、主 changelog、README、acceptance matrix 和 paper/live 接入说明
  已同步 `DEFERRED_UNTIL_TRAINING_MODULE_COMPLETE` 排期边界。
- 未修改 Rust、Cargo、schema、测试或远端运行状态。
- `git diff --check` 通过。
- 本地合同测试通过：
  - `deploy_readme_contract`：5 passed；
  - `hot_path_handoff_contract`：16 passed；
  - `paper_live_boundary_contract`：4 passed。
- 自审确认没有把未取得的 rollover/full-day 证据写成已发生，也没有改变 UTC fence
  实现合同或 future paper/live 公共技术合同。当前等待独立阶段审查。

## 2026-07-30：阶段 1 首轮独立审查修复

- `gpt-5.6-sol/ultra` 首轮独立审查结论为 `FAIL`，发现一个 Important：
  事故 changelog 更早的历史检查点仍有无限定的“待执行、下一步、当前仍须”语气，
  单独切片时可能继续冒充当前游标。
- 审查另指出一项职责措辞风险：主 changelog 把 paper/live runtime 自有的
  runtime receive、消费背压和 append-only log 混写为 future `data-collect`
  adapter 必须继承的合同。
- 已做最小修复：
  - 对所有被指出的旧检查点增加“截至当时、历史、后续已完成或已取代”限定；
  - 把 future `data-collect` adapter 的 handoff/共同 RTDS 合同，与 paper/live
    runtime 自有的 receive、背压处理和 append-only log 责任分开。
- 未改变任何历史时间、运行数据、SHA、测试结果或代码语义。当前等待同一审查者复审。

## 2026-07-30：阶段 1 独立复审通过

- 同一 `gpt-5.6-sol/ultra` 审查者复核最新 diff，结论为 `PASS`：
  Critical 0、Important 0、Minor 0。
- 审查确认事故三件套、历史检查点限定、paper/live 延期和职责边界均与用户决策及
  当前代码能力一致。
- 审查复跑 `deploy_readme_contract`、`hot_path_handoff_contract` 和
  `paper_live_boundary_contract`，共 25 项全部通过；该结果只覆盖阶段 1 文档边界，
  不冒充 workspace 全量测试通过。
- 阶段 1 完成，创建并 push 本阶段 commit 后进入阶段 2。
