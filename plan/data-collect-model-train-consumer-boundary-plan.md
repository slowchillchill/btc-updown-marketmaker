# data-collect 模型训练消费边界修正计划

> 事项编号：`DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804`
>
> 生命周期：`APPROVED_PLAN_COMPLETED`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应需求：`demand/data-collect-model-train-consumer-boundary-demand.md`
>
> 对应 changelog：`changelog/data-collect-model-train-consumer-boundary-changelog.md`
>
> 执行授权：`none`
>
> 状态：`本地修正、验证与独立审查已完成；计划已关闭`

## 1. 成功标准与停止条件

成功标准是：`ConsumerKind::Training` 继续作为兼容标识明确表示 `model-train`，不再具有
直接读取 `typed_raw` 的权限；`ConsumerKind::DataPipeline` 仍是该离线训练路径唯一直接
原始消费种类；DPT 批准状态和当前权威守卫一致，全部定向回归通过。

出现以下情况必须停止并交由用户决定：

- 取消原始读取权限要求修改每日清单 schema、既有 Parquet 或历史清单；
- 现有生产采集路径实际依赖 `Training` 直接读取原始数据；
- 修正必须改变 `DataPipeline`、paper/live、采集器运行时或部署语义；
- DPT 三件套、`CURRENT.md`、代码或真实测试结果不能同时成立。

## 2. 执行任务

- 任务 1：建立并路由窄事项三件套。
  - 修改：本需求、对应计划和 changelog，以及 `data-collect/docs/CURRENT.md`。
  - 验证：事项编号、相互引用、执行边界和当前路由唯一。
- 任务 2：同步 DPT 批准状态与文档权威分类。
  - 修改：DPT 三件套、当前文档权威审计和自动守卫。
  - 验证：DPT 为已批准活动事项，`rebuild-core`/`sim-system` 仍为草案；损坏首屏负例失败。
- 任务 3：手术式修正模型训练消费边界。
  - 修改：`source_format.rs`、`manifest.rs` 及直接相关合同测试。
  - 验证：`DataPipeline` 原始读取为真，`Training` 原始读取为假且需要 DPT 输出；
    `finalized_typed_raw_parquet` 从 `Training` 允许集合移到禁止集合。
- 任务 4：执行定向回归与自审。
  - 验证：运行模型训练消费边界、消费者格式兼容、清单、最终化发现、写入发布和文档权威
    测试；再运行格式、定向 Clippy 和差异检查。
- 任务 5：独立审查并关闭窄事项。
  - 验证：独立代理确认需求、计划、实现、测试和范围一致；发现问题时修复并重审。

## 3. 精确文件范围

允许修改：

- 本事项三件套；
- DPT 三件套；
- `data-collect/docs/CURRENT.md`、`current/contracts.md`、`current/interfaces.md`、
  `current/acceptance.md`、`current/document-authority-audit.md`；
- `data-collect/src/source_format.rs`、`data-collect/src/manifest.rs`；
- `data-collect/tests/training_consumer_boundary_contract.rs`、
  `consumer_format_compatibility_contract.rs`、`document_authority_contract.rs`。

若测试证明还需修改其他生产或合同文件，必须先停止并报告，不得自行扩大范围。

## 4. 提交与外部动作

实施期间的授权允许在独立审查通过后创建一个范围明确的本地提交。当前计划已经关闭且
`执行授权=none`；现在只可将已审查且不再增加内容的完成差异记录为一次本地收尾提交，
该提交不得夹带修正、重开实现游标或改变外部状态。用户没有授权推送、发布、部署或任何
远端状态变化；未获具体授权前不得执行这些动作。

## 5. 完成记录

任务 1 至任务 5 已全部完成。七组定向合同测试合计 `31 passed; 0 failed`，定向 Clippy、
Rust 格式和差异格式检查均通过；独立审查结论为
`Critical 0 / Important 0 / Minor 0`。本计划的本地执行游标和执行授权现已关闭；该结果
只完成本窄事项，不代表 DPT 阶段 0 完成，也不授权推送、发布、部署或远端操作。
