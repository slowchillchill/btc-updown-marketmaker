# data-collect 文档权威全量审计（2026-07-30）

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 生命周期：`CURRENT_AUDIT_EVIDENCE`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 对应 changelog：`changelog/data-collect-current-authority-consolidation-changelog.md`

## 1. 范围与结论

本次审计以阶段开始时确定的 `60` 份相关 Markdown 为输入，并新增本审计报告；
阶段 2 工作树最终共 `61` 份相关 Markdown：

- `53` 份位于 data-collect 主需求/计划/changelog、模块 README/docs 和本次治理
  triplet；
- `7` 份直接约束或提供 data-collect 历史输入的关联文档；
- 新增 `1` 份本审计报告。

范围边界：

- 根 `AGENTS.md` 是仓库级行为与路由政策，不计入产品状态文档；本阶段核对其
  data-collect 约束，阶段 3 再增加 current/archive 读取路由。
- `data-collect/third_party/**` 是固定第三方 SDK 源码及其随附说明，不属于本项目
  demand/plan/changelog 或运行状态记录，不纳入历史处置。
- 与 data-collect 没有直接合同、执行游标或历史输入关系的其他模块文档不纳入数量；
  发现跨模块直接引用时则纳入上述 `7` 份关联文档逐项处置。

处置结果：

| 类别 | 数量 | 当前用途 |
| --- | ---: | --- |
| current / active | 12 | 当前模块合同、运维说明、验收、整理事项、审计报告和跨模块草案 |
| historical | 45 | 旧事项三件套、旧独立计划、阶段报告、旧开发顺序和旧硬件估算 |
| evidence-only input | 4 | 与 data-collect 直接相关的 mistake-log 历史输入 |
| 合计 | 61 | 包含本审计输出，无遗漏 |

旧历史事实没有改写成“从未发生”。所有 historical 文件首屏均标明
`HISTORICAL_EVIDENCE_ONLY`、`最终快照`、`当前执行 authority: none` 和
`superseded_by`；阶段 3 将把它们从活动搜索面移出并由 Git tag/manifest 路由。

## 2. 当前代码与决策基线

| 主题 | 当前事实 |
| --- | --- |
| binary | Cargo 定义 training、paper-hot、live-hot 三个 binary；只有 training 连接真实外部源 |
| paper/live | 当前只有 heartbeat/status；真实 adapter 延后到训练模块完成后另立三件套 |
| SDK | `polymarket_client_sdk_v2 = 0.7.0`，使用仓库唯一 `0.7.0-p1` patch |
| schema | orderbook v3、metadata v5、depth snapshot v3、depth update v4、aggTrade v7、RTDS v4 |
| layout | UTC day / source / hour finalized Parquet；碰撞时使用 `part-N`；每日唯一 manifest |
| 容量 | `50,000,000,000` bytes 后协调 finalize 并退出 75；不运行 retention admin，不自动删除 raw |
| RTDS | 300s steady、60s first-fresh、cold-start disabled、1s 指数 backoff、300s cap、fresh reset、break-before-make |
| resolution | fixed identity；E+3..E+8；qualified terminal only once；restart fail closed |
| 事故 | `DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR` 已由用户最终验收；旧 Attempt6 失效 |
| 移除的门 | 不要求再取得真实 UTC rollover 或完整健康 UTC 日；也不伪称该证据已经发生 |

代码事实由 `data-collect/Cargo.toml`、workspace `Cargo.toml` / `Cargo.lock`、
`data-collect/src/live_runtime/`、`data-collect/src/daily_manifest.rs`、
`data-collect/src/runtime.rs` 和 current contract tests 交叉核对。

## 3. 发现并修正的活动冲突

1. 主 demand/plan/changelog 仍把旧 Attempt6 写成 current cursor。
   - 处置：明确它只属 2026-07-28 历史检查点，已被 B2 修复与用户最终验收取代。
2. 主 plan Phase 8 仍含 3 秒 RTDS、production retention delete、
   retention admin、metadata v4、release alias 和远端删除指令。
   - 处置：整阶段和危险子任务就地标为历史、禁止重新执行，并写入 current 替代语义。
3. 主需求的规范段落仍把 metadata v3/v4 和 aggTrade v7 写成 current/future 过渡态。
   - 处置：current 改为 source v3 + metadata v5、aggTrade v7 和 RTDS v4；
     v3/v4/v5/v6 只按精确 legacy reader 兼容。
4. PMXT 旧计划仍是 `APPROVED_EXECUTING`，包含自动删除路线。
   - 处置：整份降级为 historical；当前为 50GB exit 75 且不删除 raw。
5. 旧 Phase 0、RTDS 和 rebuild-core 条款把所有业务静默都写成不得替换 runtime。
   - 处置：普通 source 继续中性；显式保留 training RTDS 300s/60s 完整 replacement
     作为唯一窄例外。
6. `推荐开发顺序.md` 引用已删除的 `data-collect/tofix.md`，并把已关闭冷路径问题写成
   当前下一步。
   - 处置：整份标记为 2026-07-22 历史推荐稿，不再拥有执行 authority。
7. `rebuild-core-demand.md` 仍使用 aggTrade v6、旧 RTDS 与当前 paper/live 已部署语气。
   - 处置：该文件仍是审核中的跨模块草案，不整份归档；仅把 data-collect 活动合同
     修为 v7、300s/60s、当前只有 training 真实源和 paper/live future 阶段。
8. training 部署说明仍把 2026-07-18 专项三件套作为当前入口。
   - 处置：改为 current master triplet，并把旧部署明确为历史证据。
9. paper/live future adapter 与 paper/live runtime 自有职责混写。
   - 处置：data-collect 负责真实 source + handoff sink + 共同 RTDS；paper/live runtime
     负责本机 receive、consumer backpressure 和自身 append-only log。
10. acceptance matrix 混合旧 Phase、旧 SDK、旧 attempt 与 current 结论。
    - 处置：完整重写为只含当前可验证合同、future/deferred 边界和已知测试限制。

## 4. Current / active 文件（12）

| 文件 | 状态 | authority / 用途 |
| --- | --- | --- |
| `demand/data-collect-demand.md` | `CURRENT_MASTER_CONTRACT` | 当前模块需求摘要；阶段 3 将进一步压缩 |
| `plan/data-collect-plan.md` | `CURRENT_MASTER_PLAN_CLOSED` | 已关闭主计划；无实现/部署游标 |
| `changelog/data-collect-changelog.md` | current log | 最新实现与关闭状态在首屏；旧章节是历史 |
| `demand/data-collect-current-authority-consolidation-demand.md` | executing | 本次文档治理需求 |
| `plan/data-collect-current-authority-consolidation-plan.md` | executing | 本次唯一 data-collect 执行计划 |
| `changelog/data-collect-current-authority-consolidation-changelog.md` | executing | 本次治理证据 |
| `data-collect/README.md` | current interface | 当前模块入口；阶段 3 与 CURRENT 层对齐 |
| `data-collect/docs/acceptance-matrix.md` | current acceptance | 当前代码/测试验收，不承载历史 |
| `data-collect/docs/training-collector-deploy-readme.md` | current operations | 当前训练运行合同；不授权新部署 |
| `data-collect/docs/paper-live-hot-path-integration.md` | deferred future contract | heartbeat/status current；真实 adapter future |
| `data-collect/docs/data-collect-document-authority-audit-20260730.md` | current audit evidence | 阶段 2 全量清单、冲突和处置证据 |
| `demand/rebuild-core-demand.md` | draft under review | 跨模块未来需求；不得覆盖 current data-collect |

## 5. Historical 文件（45）

所有下列文件的 `superseded_by` 至少包含
`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`；更具体后继关系记录如下。

### H1：2026-07-28 lag/fixed identity 事故（3）

最终快照：`CLOSED_BY_USER_ACCEPTANCE_20260730`。

- `demand/data-collect-20260728-lag-fixed-identity-repair-demand.md`
- `plan/data-collect-20260728-lag-fixed-identity-repair-plan.md`
- `changelog/data-collect-20260728-lag-fixed-identity-repair-changelog.md`

### H2：resolution follow-up（3）

最终快照：Phase 4、AC-12/13 与 Gamma format A 已闭合；current schema 后续变为 v5。

- `demand/data-collect-polymarket-resolution-followup-demand.md`
- `plan/data-collect-polymarket-resolution-followup-plan.md`
- `changelog/data-collect-polymarket-resolution-followup-changelog.md`

### H3：redundant typed column removal（3）

最终快照：aggTrade v7、metadata v5 已发布；Attempt6 游标已失效。

- `demand/data-collect-redundant-typed-column-removal-demand.md`
- `plan/data-collect-redundant-typed-column-removal-plan.md`
- `changelog/data-collect-redundant-typed-column-removal-changelog.md`

### H4：collector reliability（3）

最终快照：早期 heartbeat/恢复事项完成或被 source availability、RTDS、SDK070 取代。

- `demand/data-collect-reliability-repair-demand.md`
- `plan/data-collect-reliability-repair-plan.md`
- `changelog/data-collect-reliability-repair-changelog.md`

### H5：RTDS full reconnect（3）

最终快照：300s/60s runtime replacement 已实现；attempt 时间线只作审计。

- `demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
- `plan/data-collect-rtds-business-silence-full-reconnect-plan.md`
- `changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`

### H6：RTDS-PONG-A reset deployment（3）

最终快照：`COMPLETE_DEPLOYED_AND_VALIDATED_20260719T110131Z`；旧删除/部署命令失效。

- `demand/data-collect-rtds-pong-a-reset-deployment-demand.md`
- `plan/data-collect-rtds-pong-a-reset-deployment-plan.md`
- `changelog/data-collect-rtds-pong-a-reset-deployment-changelog.md`

### H7：SDK070 typed/live runtime（3）

最终快照：core、2-PM、2-RTDS、Stage3/5/6 和 2-REST 已闭合；旧 v4 游标失效。

- `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
- `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
- `changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`

### H8：source availability lifecycle（3）

最终快照：`PHASE_9_COMPLETE_RTDS_PONG_A`；current 行为由后继事项共同覆盖。

- `demand/data-collect-source-availability-lifecycle-repair-demand.md`
- `plan/data-collect-source-availability-lifecycle-repair-plan.md`
- `changelog/data-collect-source-availability-lifecycle-repair-changelog.md`

### H9：training direct deployment（3）

最终快照：`COMPLETE_SUPERSEDED_FOR_CURRENT_DEPLOYMENT_BY_RTDS_PONG_A_RESET`。

- `demand/data-collect-training-collector-direct-deployment-demand.md`
- `plan/data-collect-training-collector-direct-deployment-plan.md`
- `changelog/data-collect-training-collector-direct-deployment-changelog.md`

### P1--P2：旧独立计划（2）

- `plan/data-collect-pmxt-hourly-retention-plan.md`：
  hourly layout 已吸收；自动 retention/admin 路线失效。
- `plan/data-collect-source-writer-supervision-repair-plan.md`：
  `DC-INCIDENT-20260709-AGGTRADE-T08` 已部署验证，后继 owner 接管。

### R1--R14：阶段报告（14）

- `data-collect/docs/hardware-tier-validation.md`
- `data-collect/docs/phase0-technical-validation.md`
- `data-collect/docs/polymarket-resolution-followup-ac12-validation.md`
- `data-collect/docs/polymarket-sdk-gap-report.md`
- `data-collect/docs/rtds-stage0-authority-wrapper-route-2026-07-26.md`
- `data-collect/docs/rtds-stage0-parameter-candidates-2026-07-23-25.md`
- `data-collect/docs/sdk070-stage0-baseline.md`
- `data-collect/docs/sdk070-stage1-sdk-runtime-and-typed-contract.md`
- `data-collect/docs/sdk070-stage2-observable-and-owner-baseline.md`
- `data-collect/docs/sdk070-stage2-rest-modularization.md`
- `data-collect/docs/sdk070-stage3-future-typed-handoff-mapping.md`
- `data-collect/docs/sdk070-stage3-storage-ab-measurement.md`
- `data-collect/docs/sdk070-stage5-cleanup-ledger.md`
- `data-collect/docs/sdk070-stage6-v3-integration-parity.md`

它们分别是硬件、Phase 0、AC-12、SDK 0.6、RTDS 参数/路线和 SDK070 各阶段的最终
证据快照；current 结论由代码与 current acceptance 取代。

### O1：旧开发顺序（1）

- `推荐开发顺序.md`：2026-07-22 历史推荐稿；已删除 `tofix.md` 与旧第 1 步均失效。

### O2：旧硬件消耗估算（1）

- `5分钟持续加载市场数据的硬件消耗预估.md`：2026-06-28 实现前估算；事件量样本只作
  研究线索，旧 cadence、SELL、action coalescing 和部署建议不构成 current 合同。

## 6. Evidence-only mistake-log（4）

以下文件继续保留原文，用于证明历史问题覆盖范围；它们不是 demand/plan/changelog，
内部建议不得直接转为当前需求或执行 authority。四份文件的最终快照均为
`EVIDENCE_INPUT_FROZEN_AT_20260730`，当前执行 authority 均为 `none`，
`execution_superseded_by` 均为
`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`：

- `mistake-log/2026-07-03-重构前错误汇总与优先级.md`
- `mistake-log/V2/2026-06-28-V2错误记录汇总执行计划.md`
- `mistake-log/never-full-load/5090训练服务器硬件利用率问题高维报告.md`
- `mistake-log/poly-tx/数据采集处理消费问题总报告.md`

这些文件是跨模块原始问题输入，不属于 data-collect 状态沿革长文，阶段 3 不把它们
作为 data-collect 历史删除；它们继续留在 `mistake-log/`，但必须保持 evidence-only
banner，并由 current 路由与自动检查阻止其取得执行 authority。

## 7. 验证范围与已知限制

- current lib regression 可在精确跳过已知动态夹具后执行。
- 已知夹具
  `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`
  使用运行当日 12:00 UTC 作为 receive time；UTC 12:00 前运行会被真实 admission
  clock 判为未来并超时。该结果是 wall-clock 依赖测试夹具问题，不是已证明的生产
  runtime 失败。
- 因此本审计不得写成“未排除测试的 full suite 无条件通过”。current acceptance
  matrix 提供精确跳过该 named test 的命令和单独复跑条件。

## 8. 读取规则

1. current 结论必须先从 current 层、Cargo/代码和 current tests 共同取得。
2. 历史文件只能回答“当时发生了什么、为何这样决策”，不能回答“现在应执行什么”。
3. 读取任意历史片段后，必须重新核对 current 入口；局部片段中的
   `当前/下一步/PENDING/READY/命令` 不具有独立 authority。
4. 阶段 3 完成后，历史只能通过 archive manifest 指定的 Git tag、原路径和 blob
   hash 显式读取，默认搜索面不再包含这些长文。
