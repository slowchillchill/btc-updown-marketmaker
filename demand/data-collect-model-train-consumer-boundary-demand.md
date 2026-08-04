# data-collect 模型训练消费边界需求

> 事项编号：`DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804`
>
> 生命周期：`APPROVED_REQUIREMENTS`
>
> 当前执行 authority：`plan/data-collect-model-train-consumer-boundary-plan.md`
>
> 对应计划：`plan/data-collect-model-train-consumer-boundary-plan.md`
>
> 对应 changelog：`changelog/data-collect-model-train-consumer-boundary-changelog.md`
>
> 执行授权：`none`
>
> 状态：`用户已批准`

## 1. 业务目标

修正历史名称 `ConsumerKind::Training` 的业务含义：它表示下游 `model-train`，不是
`data-pipeline(training)`。模型训练不得直接读取或重新解释 `typed_raw`；离线训练链路中，
只有 `data-pipeline(training)` 可以读取最终化 `typed_raw` 并生成版本化训练输入。

`model-train` 进程可以读取采集清单、覆盖、健康、缺口、重连和背压元数据，但只能用于
血缘、审计和质量诊断。这些元数据不得参与训练准入、样本选择、切分或决策触发，也不得进入
Observation、特征、标签、训练样本或模型输入。

## 2. 范围与非目标

- 保留既有历史兼容标识，不新增第二套名称、别名或迁移路径。
- 不修改采集数据源、写入器、Parquet、每日清单、既有数据、paper/live、部署或远端状态。
- 不实现模型训练或 DPT 规范化，也不把 `model-train` 与采集训练进程混为同一模块。

## 3. 验收结果

1. `model-train` 无法直接消费最终化 `typed_raw`，并且必须依赖版本化 DPT 输出。
2. `data-pipeline(training)` 继续是离线训练链路中唯一直接消费最终化 `typed_raw` 的模块。
3. 六类诊断元数据保持进程可读，但与模型可达数据严格隔离。
4. 相邻清单、最终化发现和发布行为不受影响，差异不包含采集运行时、schema、数据、部署或
   远端状态变化。
5. 具体代码标识、复用路线、执行顺序和必要测试以对应计划为唯一规范来源。
