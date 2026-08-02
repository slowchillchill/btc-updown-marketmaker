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

- current authority 只有一个入口：`data-collect/docs/CURRENT.md`。
- current master triplet 只有
  `demand/data-collect-demand.md`、`plan/data-collect-plan.md` 和
  `changelog/data-collect-changelog.md`。
- 六个 `docs/current/` 文件分别拥有单一主题，不互相发展影子合同。
- `data-collect/README.md` 与四个旧 current 文档原路径只作 route-only 兼容。
- `demand/rebuild-core-demand.md` 是跨模块审核中草案，不是 current data-collect
  authority；其中 future 要求不得覆盖本入口、当前代码或测试。
- `demand/sim-system-demand.md` 是独立模块审核中草案，不是 current data-collect
  authority；其中只可引用本入口的当前事实和已确认跨模块主线，不得覆盖
  data-collect 模块合同、代码、测试或完成状态。

当前活动树中所有直接出现 `data-collect` / `data_collect` / `DataCollect` 的
Markdown 恰为 25 份，且每份都有明确处置：

| 类别 | 数量 |
| --- | ---: |
| CURRENT 入口 + 六个主题 owner + current master triplet | 10 |
| AGENTS 路由政策 + README/四个 route-only 入口 | 6 |
| 已完成治理记录三件套 | 3 |
| 跨模块审核中 draft | 2 |
| evidence-only mistake-log | 4 |
| 合计 | 25 |

## 历史处置

阶段 2 审计覆盖 61 份相关 Markdown，并将其分类为 12 current/active、45 historical
和 4 evidence-only。阶段 3 后：

- 45 份历史沿革长文已从活动树移除；
- annotated tag 为 `data-collect-history-pre-current-authority-20260730`；
- peeled commit 为 `4ed31ad6abe7237d92cc99a4857979d8ae26d373`；
- `archive-manifest.tsv` 记录 45 个历史对象和 11 个被 current 层替换的阶段 2
  snapshot，共 56 个可复核 path/blob；
- 四份跨模块 mistake-log 继续保留为 `EVIDENCE_ONLY_INPUT`，authority 为 `none`。

归档保留的是不可变证据，不是备用执行计划。`historical` blob 的局部读取必须连同
其内嵌首屏 banner 核对；`snapshot` blob 不保证含 banner，其 `authority none`
由 manifest 的 `kind=snapshot` 与已替换的活动同路径共同判定。两类读取都必须按
commit/path/blob 显式进行，并在形成当前结论前回到活动同路径（如适用）、CURRENT、
对应主题 owner、代码与测试。

## 已消除的冲突类别

1. 已结束事故或阶段仍显示为当前 PENDING/READY/next cursor。
2. SDK 0.6、旧 schema 或中间 schema 仍冒充 current new-write。
3. 旧 RTDS 3 秒 subscribe/refresh 与 current 300/60 full replacement 冲突。
4. 自动 retention/delete、retention admin、release alias 与 current 默认 50GB
   exit 75 且不删除 raw 冲突。
5. 历史直接部署、同日 reset 或远端命令仍冒充当前部署 authority。
6. paper/live future event 类型被误写成当前真实 adapter 已实现。
7. 真实 UTC rollover/full-day 未取得证据被误写为模块 completion blocker。

## 自动一致性门

`data-collect/tests/document_authority_contract.rs` 校验 current 图、master triplet、
archive tag/path/blob、`historical` 与 `snapshot` 的不同读取协议、snapshot 活动
替代集合、历史路径隔离和 evidence-only banner，并用临时 fixture 证明重复
CURRENT、坏引用、坏 blob、恢复历史文件或损坏 evidence banner 会失败。

2026-07-30 当前运行结果为 `4 passed / 0 failed`；tag 中 45 个 historical 和 11 个
snapshot 对象均通过 blob 校验，45 个历史路径均不存在于活动树。

2026-08-02 将 `demand/sim-system-demand.md` 作为第二份无 data-collect authority 的
跨模块审核中草案纳入显式 disposition 后，当前重跑结果仍为 `4 passed / 0 failed`；
历史 tag、snapshot/historical 数量和读取协议均未修改。

运行限制只由 `current/known-limitations.md` 拥有。若本审计、current owner、代码、
测试或用户最新决策不一致，必须停止并交由用户判断；当前无已知未解决 authority
冲突。
