# data-collect 当前文档权威审计

> 生命周期：`CURRENT_CONTRACT`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 当前需求：`demand/data-collect-demand.md`
>
> 当前计划：`plan/data-collect-plan.md`
>
> 当前 changelog：`changelog/data-collect-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

## 审计结论

- 当前执行权威只有一个入口：`data-collect/docs/CURRENT.md`。
- 当前主三件套只有
  `demand/data-collect-demand.md`、`plan/data-collect-plan.md` 和
  `changelog/data-collect-changelog.md`。
- 六个 `docs/current/` 文件分别拥有单一主题，不互相发展影子合同。
- `data-collect/README.md` 与四个旧当前文档原路径只作路由兼容。
- `demand/rebuild-core-demand.md`、`plan/rebuild-core-plan.md` 和
  `changelog/rebuild-core-changelog.md` 是同一跨模块事项的审核中三件套，不是当前
  `data-collect` 权威；其中后续要求不得覆盖本入口、当前代码或测试。
- `demand/sim-system-demand.md`、`plan/sim-system-plan.md` 和
  `changelog/sim-system-changelog.md` 是同一独立模块事项的审核中三件套，不是当前
  `data-collect` 权威；其中只可引用本入口的当前事实和已确认跨模块主线，不得覆盖
  `data-collect` 模块合同、代码、测试或完成状态。
- `demand/data-pipeline-training-demand.md`、`plan/data-pipeline-training-plan.md` 和
  `changelog/data-pipeline-training-changelog.md` 是同一已批准跨模块事项的活动三件套；
  它们只拥有 DPT 对应计划范围内的本地实施权威，引用本入口的 `data-collect` 当前事实，
  不得重开或覆盖已完成的采集器运行时、schema、数据或部署合同。
- `demand/data-collect-model-train-consumer-boundary-demand.md`、
  `plan/data-collect-model-train-consumer-boundary-plan.md` 和
  `changelog/data-collect-model-train-consumer-boundary-changelog.md` 是由本 `CURRENT`
  显式路由的已完成窄修正三件套，记录 `model-train` 禁止直接读取 `typed_raw` 及关联文档
  守卫同步结果，不再拥有执行游标。

当前活动树中所有直接出现 `data-collect` / `data_collect` / `DataCollect` 的
Markdown 恰为 35 份，且每份都有明确处置：

| 类别 | 数量 |
| --- | ---: |
| `CURRENT` 入口、六个主题归属文档和当前主三件套 | 10 |
| AGENTS 路由政策、README 和四个仅路由入口 | 6 |
| 已完成治理记录三件套 | 3 |
| 跨模块审核中草案/记录 | 6 |
| 已批准 DPT 跨模块三件套 | 3 |
| 已完成窄修正三件套 | 3 |
| 仅作证据的错误记录 | 4 |
| 合计 | 35 |

## 历史处置

阶段 2 审计覆盖 61 份相关 Markdown，并将其分类为 12 份当前或活动文档、45 份历史文档
和 4 份仅作证据文档。阶段 3 后：

- 45 份历史沿革长文已从活动树移除；
- 带注释标签为 `data-collect-history-pre-current-authority-20260730`；
- 解引用提交为 `4ed31ad6abe7237d92cc99a4857979d8ae26d373`；
- `archive-manifest.tsv` 记录 45 个历史对象和 11 个被当前层替换的阶段 2 快照，
  共 56 个可复核路径和 blob；
- 四份跨模块错误记录继续保留为 `EVIDENCE_ONLY_INPUT`，执行权威为 `none`。

归档保留的是不可变证据，不是备用执行计划。`historical` blob 的局部读取必须连同
其内嵌首屏标记核对；`snapshot` blob 不保证含首屏标记，其执行权威为 `none`
由清单的 `kind=snapshot` 与已替换的活动同路径共同判定。两类读取都必须按提交、路径
和 blob 显式进行，并在形成当前结论前回到活动同路径（如适用）、`CURRENT`、对应主题
归属文档、代码与测试。

## 已消除的冲突类别

1. 已结束事故或阶段仍显示为当前 `PENDING`、`READY` 或下一游标。
2. SDK 0.6、旧 schema 或中间 schema 仍冒充当前新写入版本。
3. 旧 RTDS 3 秒订阅、刷新与当前 300/60 秒完整替换冲突。
4. 自动保留或删除、保留策略管理器、发布别名与当前默认 50GB、退出 75 且不删除原始
   数据的合同冲突。
5. 历史直接部署、同日重置或远端命令仍冒充当前部署权威。
6. 模拟盘和实盘的后续事件类型被误写成当前真实适配器已实现。
7. 未取得真实 UTC 日切换或完整 UTC 日运行证据被误写为模块完成阻断项。

## 自动一致性门

`data-collect/tests/document_authority_contract.rs` 校验当前权威图、主三件套、归档标签、
路径和 blob、`historical` 与 `snapshot` 的不同读取协议、快照对应的活动树替代集合、
历史路径隔离和仅作证据文档的首屏标记，并用临时测试夹具证明重复 `CURRENT`、错误引用、
错误 blob、恢复历史文件或损坏证据首屏标记会失败。

2026-07-30 当前运行结果为 `4 passed / 0 failed`；标签中 45 个历史对象和 11 个快照
对象均通过 blob 校验，45 个历史路径均不存在于活动树。

2026-08-02 将 `demand/sim-system-demand.md` 作为第二份无 `data-collect` 权威的跨模块
审核中草案纳入显式归类后，当前重跑结果仍为 `4 passed / 0 failed`；历史标签、快照与
历史文档数量和读取协议均未修改。

2026-08-03 新增 `DATA-PIPELINE-TRAINING-20260803` 审核中三件套，并把它们作为三份无
`data-collect` 权威、无执行授权的跨模块草案/记录纳入显式归类。活动 Markdown 总数由
25 增为 28；当前主三件套、六个主题归属文档、归档标签、快照和历史文档数量均未修改。
实际测试结果记录在 `current/acceptance.md`，不得从本段预先推导为已通过。

2026-08-04 为满足需求、计划与变更记录一一对应，补齐 `rebuild-core-demand` 和
`SIM-SYSTEM-20260731` 两项的无执行权威计划/变更记录，并把四份新增文件纳入显式归类。
活动 Markdown 总数由 28 增为 32；三套跨模块三件套均保持 `authority=none` 和
`执行授权=none`，当前 `data-collect` 主三件套、唯一 `CURRENT`、归档标签、快照和历史
文档数量均未改变。实际测试结果记录在 `current/acceptance.md`，不得从本段预先推导为已通过。

2026-08-04 用户批准 DPT 需求与计划后，DPT 三件套由审核中草案改为已批准活动事项；随后
用户裁定历史名称 `ConsumerKind::Training` 表示下游 `model-train`，并批准
`DATA-COLLECT-MODEL-TRAIN-BOUNDARY-20260804` 窄修正三件套。活动 Markdown 总数由 32
增为 35；`rebuild-core` 与 `sim-system` 六份文档仍为草案，当前主三件套、唯一
`CURRENT`、归档标签、快照和历史文档数量不变。真实测试结果只记录在
`current/acceptance.md`，不得从本段预先推导为已通过。

运行限制只由 `current/known-limitations.md` 拥有。若本审计、当前主题归属文档、代码、
测试或用户最新决策不一致，必须停止并交由用户判断；当前无已知未解决的权威冲突。
