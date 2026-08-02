# data-collect 当前变更记录

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
标签、路径和 blob 保留，不在活动文档重复。

## 当前实现快照

- Cargo 定义训练、模拟盘热路径和实盘热路径三个二进制程序。
- 当前只有训练程序连接真实 Polymarket、RTDS 和 Binance 数据源；模拟盘和实盘程序只
  提供启动回执、Unix 套接字与心跳、状态。
- Polymarket 使用官方 Rust SDK `0.7.0` 加仓库唯一 `0.7.0-p1` 补丁。
- 当前新写入 schema 为订单簿 v3、元数据 v5、RTDS v4、Binance 深度快照 v3、
  深度更新 v4、`aggTrade` v7。
- 存储按接收时间划分 UTC 日、数据源和小时，写最终化 Parquet、每日唯一清单 v3 与
  采集日覆盖信息 v1。
- 默认容量为 50GB；启动时已达上限则不启动数据源或产物而直接退出 75，运行时扫描达到
  上限则协调最终化后退出 75；两者都不自动删除最终化原始数据。
- RTDS 使用稳态 300 秒、首次有效消息 60 秒的完整替换；退避基数为 1 秒且每级 ×2，
  应用 ±20% 确定性抖动、单次上限 300 秒、无独立尝试耗尽门，有效新鲜消息后重置，
  并严格先断开再连接。
- 固定身份信息、E+3..E+8 市场判定、重启时失败即关闭与确定性 UTC 日切换已实现并有测试。

## 用户最终决策

- 2026-07-28 事故事项已由用户最终验收。
- 真实 UTC 日切换和完整健康 UTC 日不再是完成门；文档不反向声称未记录
  的运行证据已经发生。
- 2026-08-02 用户确认当前跨模块开发主线为
  `data-collect(training) -> sim-system -> data-pipeline(training) -> model-train`。
- 模拟盘和实盘的真实外部数据源适配器不属于 `model-train`，保持延期；必须先
  完成 `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后
  仍只有在用户决定返回 `data-collect` 时，才以独立三件套开发。该延期不阻断
  `data-collect` 完成。
- `data-collect` 当前没有 Rust 实现、部署或远端操作游标。

## 2026-08-02 跨模块顺序同步

- 复审确认 `data-collect` 当前只需同步跨模块主线，并继续明确传输层和数据源层的覆盖、
  健康状态、缺口、恢复与重连证据，与 `data-pipeline` 规范层之间的分工；其他模拟和
  重建澄清不改变 `data-collect` 运行时职责或完成状态。
- `CURRENT`、主三件套、当前合同、验收、限制文档和两份跨模块需求已同步当前主线；
  模拟盘和实盘适配器的既有延期门保持不变。
- 首次运行文档权威契约测试为 `3 passed / 1 failed`：活动文档
  `demand/sim-system-demand.md` 已提及 `data-collect`，但旧权威图只枚举一份
  跨模块草案。随后将该文件明确分类为第二份无 `data-collect` 权威的审核中草案，
  并同步契约测试；这不是生产运行时、schema 或部署变更。
- 修正后文档权威契约测试为 `4 passed / 0 failed`，`git diff --check`
  通过。随后扩展的部署说明契约测试首次为 `5 passed / 1 failed`，原因是同步
  时误删了“`model-train` 不是 `data-collect-training-collector`”的防误读措辞；现已
  恢复该区分而未恢复旧顺序。最终部署说明契约测试为 `6 passed / 0 failed`，
  文档权威契约测试为 `4 passed / 0 failed`；精确证据记录在
  `data-collect/docs/current/acceptance.md`。
- 独立最终审查发现 `sim-system` 第 3 节仍把模拟盘和实盘写成无条件第 5 至 8 项；已将
  当前主线明确止于 `model-train`，并把后续路径改为只有用户届时决定返回
  `data-collect` 才能启动的条件性后续路线。复审结果为
  `Critical 0 / Important 0 / Minor 0`（三个等级均无问题）。

## 2026-07-30 文档权威整理

- 先完成 61 份相关 Markdown 全量审计，修正旧 SDK、schema、RTDS、保留策略、部署、
  尝试游标与模拟盘、实盘能力冲突。
- 带注释标签 `data-collect-history-pre-current-authority-20260730` 冻结审计完成状态，
  解引用提交为 `4ed31ad6abe7237d92cc99a4857979d8ae26d373`。
- 45 份历史沿革长文从活动树移除；归档清单保存原路径与 blob。
- 新增唯一 `data-collect/docs/CURRENT.md` 和六个单主题当前归属文档。
- README 与旧当前文档原路径压缩为仅路由文档，不再复制规范或命令。
- 四份跨模块错误记录保持仅作证据，不能形成当前执行结论。
- 自动权威守卫覆盖唯一入口、三件套、归档对象、历史隔离与负例。
- 模拟盘和实盘的心跳帧、套接字辅助函数只有组件测试，完整二进制程序长运行循环尚无
  端到端测试；热路径运行标识的回执路径限制也尚未失败即关闭，详见当前验收与限制文档。

## 当前证据入口

- 当前实现与边界：`data-collect/docs/current/contracts.md`
- 当前 CLI 与产物：`data-collect/docs/current/interfaces.md`
- 当前运行权限边界：`data-collect/docs/current/operations.md`
- 当前验收命令与真实结果：`data-collect/docs/current/acceptance.md`
- 当前限制：`data-collect/docs/current/known-limitations.md`
- 文档治理结论：`data-collect/docs/current/document-authority-audit.md`
