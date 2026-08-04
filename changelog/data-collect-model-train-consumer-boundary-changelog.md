# data-collect 模型训练消费边界修正变更记录

> 事项编号：`DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804`
>
> 生命周期：`COMPLETED_IMPLEMENTATION_CHANGELOG`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-model-train-consumer-boundary-demand.md`
>
> 对应计划：`plan/data-collect-model-train-consumer-boundary-plan.md`
>
> 执行授权：`none`
>
> 状态：`窄修正已完成；无后续执行游标；未授权推送`

## 2026-08-04：用户裁定与事项建立

### 已核实事实

- `ConsumerKind::DataPipeline` 与 `ConsumerKind::Training` 当前都声明可以直接读取
  `typed_raw` Parquet。
- `Training` 同时要求 `data-pipeline` 输出；现有测试名称和用户最新裁定确认它表示下游
  `model-train`，不是 DPT 自身。
- 已批准 DPT 需求只允许唯一规范化核心直接解释原始数据，下游 `model-train` 不得形成
  第二条原始消费路径。

### 用户决定

- 用户明确裁定 `ConsumerKind::Training` 指 `model-train`，此前属于命名不规范。
- 用户批准建立本窄范围三件套，取消其直接读取 `typed_raw` 的权限，并同步 DPT 已批准状态
  与文档权威守卫。

### 范围与权限

- 只修改消费边界声明、直接相关测试和当前文档治理；不修改采集器运行时、Parquet/清单
  schema、既有数据、paper/live、部署或远端状态。
- 本地实现和本地提交已授权；推送、发布和部署未授权。

### 当前游标

`任务 1：建立并路由窄事项三件套` 执行中。真实实现、测试结果和独立审查结论将在完成后
追加，不提前写成通过。

## 2026-08-04：实现与定向验证

### 实现结果

- 保留 `ConsumerKind::Training` 和 `ConsumerBoundaryKind::Training` 精确标识，并以中文
  注释明确其历史兼容含义是下游 `model-train`。
- `ConsumerBoundaryContract::training()` 的 `consumes_typed_raw_parquet` 已改为 `false`；
  验证逻辑分别约束 `DataPipeline` 必须读取原始输入、`Training` 不得读取原始输入。
- `finalized_typed_raw_parquet` 已从 `TRAINING_ALLOWED_DATA_COLLECT_ROLES` 移入
  `TRAINING_FORBIDDEN_DATA_COLLECT_ROLES`；没有修改生产最终化、每日清单或发布路径。
- DPT 三件套已同步为获批活动状态；`rebuild-core` 与 `sim-system` 六份文档继续保持草案。
  本窄事项已由 `CURRENT.md` 显式路由，自动守卫逐文件校验生命周期、权威、执行授权、状态
  和三件套互链。

### 测试隔离修正

- 文档守卫首次得到 `3 passed; 1 failed`，原因是正式实施工作树内两份被 Git 忽略的旧发布
  包文档被全文件系统递归误计为活动 Markdown；它们不在 `/home/chillchill/sim-system`
  工作树，也不是本轮文件。
- 用户指出工作树边界后，守卫改为只统计 Git 已跟踪或未被忽略的新 Markdown；没有修改或
  删除发布包。重跑守卫得到 `4 passed; 0 failed`。

### 已取得验证证据

- 七组定向合同测试合计 `31 passed; 0 failed`。
- `cargo clippy -p data-collect --locked --tests -- -D warnings` 通过。
- `cargo fmt --all -- --check` 与 `git diff --check` 通过。

### 当前游标

任务 1 至任务 4 已完成自审，任务 5 进入独立审查；独立审查通过前不关闭本事项，
不创建完成提交，也不推送。

## 2026-08-04：独立审查与事项关闭

- 独立审查代理使用 `gpt-5.6-sol` 和 `ultra` 推理等级完成只读复核，结论为
  `Critical 0 / Important 0 / Minor 0`。
- 审查代理独立重跑七组定向测试，结果为 `31 passed; 0 failed`；定向 Clippy、Rust 格式
  和差异格式检查均通过。
- 审查确认 Git 文件枚举包含三份未提交且未忽略的新三件套，排除两份被忽略的发布副本；
  正式实施工作树与 `sim-system` 工作树使用不同索引，没有跨工作树污染。
- 未跟踪二进制 `data-collect-training-collector` 的 SHA-256 仍为
  `5359b4cfcd397f8fd83a648808950e635df54b7fff5ee7ab584238d2c5784b0e`，未被修改或暂存。
- 本窄事项需求已经实现，计划和本变更记录现已关闭，执行授权回到 `none`。该关闭不代表
  DPT 阶段 0 完成，也不授权推送、发布、部署或远端操作。

### 关闭状态复审修正

- 首次关闭状态复审得到 `Critical 0 / Important 0 / Minor 3`：完成首屏与正文的本地提交
  授权时态不清；`CURRENT.md` 已完成章节仍使用“当前活动修正”字段名；计划与 changelog
  的完成生命周期缺少逐字段损坏负例。
- 已把实施授权改为历史时态，并把当前剩余动作严格限制为一次不增改内容的本地收尾提交；
  `CURRENT.md` 与守卫字段统一改为“已完成修正”；新增
  `APPROVED_PLAN_COMPLETED` 与 `COMPLETED_IMPLEMENTATION_CHANGELOG` 损坏负例。
- 修正后文档权威守卫为 `4 passed; 0 failed`，Rust 格式和差异格式检查通过；最终关闭复审
  结论为 `Critical 0 / Important 0 / Minor 0`。

## 2026-08-04：DPT 任务 0.9 后续语义澄清

- 用户批准推荐方案 2：`model-train` 进程继续直接读取 `manifest`、`source_coverage`、
  `source_health`、`source_gap`、`reconnect_trace`、`backpressure_state` 六类角色，但只
  用于血缘、审计和质量诊断；不得用于训练准入、切分、样本选择、模型输入或决策触发，也
  不得进入 Observation、特征、标签或训练样本。版本化 DPT 输出是唯一训练数据和模型输入。
- DPT 任务 0.9 只为现有常量增加用途注释，并在原
  `training_consumer_boundary_contract` 中直接断言六类允许集合、DPT 必需输出集合和禁止
  模型输入集合；没有新增字段、抽象、运行时路径或 `model-train` 占位实现。
- 上述断言在现有行为上首次运行即通过，只属于特征化绿灯，不冒充 TDD 红灯。本同步不重开
  已关闭窄事项的执行授权；实际完整验证结果记录在 DPT 变更记录和 Task 0.9 报告中。
