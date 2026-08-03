# data-collect 模型训练消费边界修正需求

> 事项编号：`DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804`
>
> 生命周期：`APPROVED_REQUIREMENTS`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应计划：`plan/data-collect-model-train-consumer-boundary-plan.md`
>
> 对应 changelog：`changelog/data-collect-model-train-consumer-boundary-changelog.md`
>
> 执行授权：`none`
>
> 状态：`用户已批准；需求已实现并验证；窄事项已关闭`

## 1. 用户决定

用户于 2026-08-04 明确裁定：现有 `ConsumerKind::Training` 表示下游
`model-train`，不是 `data-pipeline(training)`。此前同时允许
`ConsumerKind::DataPipeline` 与 `ConsumerKind::Training` 直接读取 `typed_raw`
Parquet，属于早期命名与消费边界不规范。

用户批准本窄事项：

1. 保留现有 `ConsumerKind::Training` 精确标识以避免无必要的序列化/API 改名，但在代码、
   测试和当前文档中明确它只表示 `model-train`；
2. 取消 `ConsumerKind::Training` 直接读取 `typed_raw` Parquet 的权限；
3. 保持 `ConsumerKind::DataPipeline` 为离线训练路径中直接读取最终化 `typed_raw` 的唯一
   消费种类；
4. 同步 `DATA-PIPELINE-TRAINING-20260803` 已批准状态和文档权威守卫。

## 2. 修正前已核实冲突

- 修正前，`data-collect/src/source_format.rs` 中 `DataPipeline` 与 `Training` 的
  `consumes_typed_raw_parquet` 曾均为 `true`。
- 修正前，`data-collect/src/manifest.rs` 曾将 `finalized_typed_raw_parquet` 列入
  `Training` 允许的数据采集角色。
- 修正前，`data-collect/tests/training_consumer_boundary_contract.rs` 曾明确断言
  `Training` 可以读取最终化原始镜像，同时又要求外部 `data_pipeline_*` 产物。
- 已批准 DPT 数据流要求 `typed_raw` 只进入唯一 `data-pipeline(training)` 规范化路径，
  再由其产物进入 `model-train`；下游不得建立第二套原始数据解释路径。

## 3. 必须实现的行为

1. `ConsumerKind::Training` 与 `ConsumerBoundaryKind::Training` 必须有明确代码注释：历史名称
   表示下游 `model-train`。
2. `ConsumerBoundaryContract::training().consumes_typed_raw_parquet` 必须为 `false`，且
   `requires_data_pipeline_outputs` 继续为 `true`。
3. `training_consumer_boundary_declaration()` 不得允许
   `finalized_typed_raw_parquet`；该角色必须进入禁止集合。
4. `ConsumerKind::DataPipeline` 继续直接读取最终化 `typed_raw`，且不得生成标签、特征、
   切分、训练批准、NOE、Ledger 或交易决策。
5. 现有 `Training` 精确枚举名和序列化身份保持不变；本事项不引入第二个兼容枚举、别名或
   迁移层。
6. 文档权威守卫必须同时机械验证：`rebuild-core` 与 `sim-system` 三件套仍是草案；DPT
   三件套已获批准并只拥有自身事项的本地实施权威；本窄事项由
   `data-collect/docs/CURRENT.md` 路由。

## 4. 明确非目标

本事项不修改采集器数据源、写入器、Parquet schema、每日清单 schema、既有数据、
paper/live 热路径、部署、远端服务、钱包、交易、模型训练或 DPT 规范化实现。它也不把
`model-train` 改成 `data-collect-training-collector`，不删除历史枚举，不重写任何已发布
数据或清单。

## 5. 验收标准

1. 定向合同测试证明 `DataPipeline` 可以读取 `typed_raw`，`Training` 不可以读取
   `typed_raw` 且必须依赖 `data-pipeline` 输出。
2. 清单边界声明测试证明 `finalized_typed_raw_parquet` 不在 `Training` 允许集合、而在禁止
   集合；其他允许、必需和禁止角色不发生无关变化。
3. 现有清单、最终化发现和写入发布相关合同测试通过，证明窄修正没有破坏相邻合同。
4. 文档权威测试同时通过正例和逐字段损坏负例；唯一 `CURRENT`、当前主三件套、归档标签与
   历史对象数量不改变。
5. `cargo fmt --all -- --check`、相关定向 Clippy、`git diff --check` 通过。
6. 差异中不包含采集运行时、schema、数据、部署或远端状态改动。

## 6. 授权边界

本需求和对应计划由用户在 2026-08-04 对上述精确语义作出的批准建立；实施期间曾只授权
本地文档、代码、测试和本地提交。实现、验证和独立审查完成后，该实施授权已经关闭；只可
将已审查且不再增加内容的完成差异记录为一次本地收尾提交，不由此重新取得修改游标。
推送、发布、部署、远端操作和任何数据改写仍须具体授权。
