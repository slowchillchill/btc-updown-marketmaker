# data-collect 当前 Changelog

> 事项编号：`DATA-COLLECT-CURRENT`
>
> 生命周期：`CURRENT_MASTER_CHANGELOG`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-demand.md`
>
> 对应计划：`plan/data-collect-plan.md`
>
> 状态：`COMPLETE_CURRENT_SNAPSHOT`

本文件只保留当前最终状态和仍影响后续模块的决策。逐阶段实现、失败尝试、远端命令、
运行采样和审查时间线已由 `data-collect/docs/archive-manifest.tsv` 指定的 Git
tag/path/blob 保留，不在活动文档重复。

## 当前实现快照

- Cargo 定义 training、paper-hot、live-hot 三个 binary。
- 当前只有 training 连接真实 Polymarket、RTDS 和 Binance source；paper/live 只
  提供 startup receipt、Unix socket 与 heartbeat/status。
- Polymarket 使用官方 Rust SDK `0.7.0` 加仓库唯一 `0.7.0-p1` patch。
- current new-write schema 为 orderbook v3、metadata v5、RTDS v4、Binance depth
  snapshot v3、depth update v4、aggTrade v7。
- storage 使用 receive-time UTC day/source/hour、finalized Parquet 和每日唯一
  manifest v3 + collection-day coverage v1。
- 默认容量为 50GB；startup 已达上限时不启动 source/artifact 而直接退出 75，
  runtime scan 达到时协调 finalize 后退出 75；两者都不自动删除 finalized raw。
- RTDS 使用 steady 300s / first-fresh 60s full replacement、base 1s 且每级 ×2、
  ±20% deterministic jitter、单次 cap 300s、无独立 attempt exhaustion、
  fresh reset 和 break-before-make。
- fixed identity、E+3..E+8 resolution、restart fail closed 与 deterministic
  rollover 已实现并有测试。

## 用户最终决策

- 2026-07-28 事故事项已由用户最终验收。
- 真实 UTC rollover 和完整健康 UTC 日不再是 completion gate；文档不反向声称未记录
  的运行证据已经发生。
- 2026-08-02 用户确认当前跨模块开发主线为
  `data-collect(training) -> sim-system -> data-pipeline(training) -> model-train`。
- paper/live 真实 external-source adapter 不属于 `model-train`，保持延期；必须先
  完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后
  仍只有在用户决定返回 `data-collect` 时，才以独立三件套开发。该延期不阻断
  data-collect 完成。
- data-collect 当前没有 Rust 实现、部署或远端操作游标。

## 2026-08-02 跨模块顺序同步

- 复审确认 data-collect current 只需同步跨模块主线，并继续明确 transport/source-level
  coverage、health/gap、recovery/reconnect evidence 与 data-pipeline canonical 层的
  分工；其他 sim/rebuild 澄清不改变 data-collect 运行时职责或完成状态。
- CURRENT、master triplet、current contracts/acceptance/limitations 和两份
  跨模块需求已同步当前主线；paper/live adapter 的既有延期门保持不变。
- 首次运行 document authority contract 为 `3 passed / 1 failed`：活动
  `demand/sim-system-demand.md` 已提及 data-collect，但旧 authority 图只枚举一份
  跨模块草案。随后将该文件明确分类为第二份无 data-collect authority 的审核中草案，
  并同步契约测试；这不是 production runtime、schema 或部署变更。
- 修正后 document authority contract 为 `4 passed / 0 failed`，`git diff --check`
  通过。随后扩展的 deploy README contract 首次为 `5 passed / 1 failed`，原因是同步
  时误删了“`model-train` 不是 `data-collect-training-collector`”的防误读措辞；现已
  恢复该区分而未恢复旧顺序。最终 deploy README contract 为 `6 passed / 0 failed`，
  document authority contract 为 `4 passed / 0 failed`；精确证据记录在
  `data-collect/docs/current/acceptance.md`。
- 独立最终审查发现 `sim-system` 第 3 节仍把 paper/live 写成无条件第 5 至 8 项；已将
  当前主线明确止于 `model-train`，并把后续路径改为需用户届时决定返回 data-collect
  才能启动的 conditional future。复审为 `Critical 0 / Important 0 / Minor 0`。

## 2026-07-30 文档权威整理

- 先完成 61 份相关 Markdown 全量审计，修正旧 SDK/schema、RTDS、retention、部署、
  Attempt 游标与 paper/live 能力冲突。
- annotated tag `data-collect-history-pre-current-authority-20260730` 冻结审计完成状态，
  peeled commit 为 `4ed31ad6abe7237d92cc99a4857979d8ae26d373`。
- 45 份历史沿革长文从活动树移除；archive manifest 保存原路径与 blob。
- 新增唯一 `data-collect/docs/CURRENT.md` 和六个单主题 current owner。
- README 与旧 current 文档原路径压缩为 route-only，不再复制规范或命令。
- 四份跨模块 mistake-log 保持 evidence-only，不能形成当前执行结论。
- 自动 authority guard 覆盖唯一入口、三件套、archive 对象、历史隔离与负例。
- paper/live heartbeat frame/socket helper 只有 component test，完整 binary 长运行
  loop 尚无端到端测试；hot run-id receipt confinement 也尚未 fail-closed，详见
  current acceptance/limitations。

## 当前证据入口

- 当前实现与边界：`data-collect/docs/current/contracts.md`
- 当前 CLI/artifact：`data-collect/docs/current/interfaces.md`
- 当前运行权限边界：`data-collect/docs/current/operations.md`
- 当前验收命令与真实结果：`data-collect/docs/current/acceptance.md`
- 当前限制：`data-collect/docs/current/known-limitations.md`
- 文档治理结论：`data-collect/docs/current/document-authority-audit.md`
