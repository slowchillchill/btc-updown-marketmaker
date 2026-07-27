# data-collect 重复 typed 列删除 changelog

> 事项编号：`DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL`
>
> 对应需求：`demand/data-collect-redundant-typed-column-removal-demand.md`
>
> 对应计划：`plan/data-collect-redundant-typed-column-removal-plan.md`
>
> 状态：`APPROVED_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_CLOSED_ON_PUBLISH_ATTEMPT5_USER_TERMINATED_INCOMPLETE_8H_ATTEMPT6_PENDING_AFTER_V7_V5`

## 2026-07-28：阶段 2 三路最终复审通过

方案 B、embedded row version 修复和测试命名修正完成后，原三路
`gpt-5.6-sol`/Ultra 审查者重新检查最新完整 diff：

- 代码正确性、版本传播与 legacy/current recovery：
  `PASS / Critical 0 / Important 0 / Minor 0`；
- 测试充分性、fixture 独立性与过度校验：
  `PASS / Critical 0 / Important 0 / Minor 0`；
- demand/plan/changelog、W-B 历史覆盖、方案 B 与简单性：
  `PASS / Critical 0 / Important 0 / Minor 0`。

独立复跑覆盖 recovery `36/36`、metadata layout `20/20`、v5 raw-event `2/2`
与版本传播 `1/1`；主代理随后完成的完整 package tests、tests check、严格
Clippy、fmt 和 diff check 也全部通过。审查确认：

- public metadata-v3 writer 已删除，source-v2 public writer仍在；
- V3 exact decode/validator、V4 pending/WS claimant recovery、V4+V5 coherent
  lifecycle 和 V5 current recovery均保留；
- 没有第二 writer、schema adapter、sidecar、migration framework 或新增独立
  100-cycle 门。

验收 2--6 与 8 的阶段 2 部分已通过。阶段 2 只在承载本记录的代码+文档 commit
成功 push 且本地 HEAD、upstream 与远端 ref 一致后闭合；发布前状态为
`STAGE2_CLOSED_ON_PUBLISH`。

## 2026-07-28：阶段 2 首轮审查问题、方案 B 与最小修复

三路独立 `gpt-5.6-sol`/Ultra 首轮审查识别出三项需要闭环的事实：

- recovery 接受 footer/物理 schema 为 v5 的文件后，构造 current recovery row
  前没有再次确认行内 `schema_version` 也是 v5，可能让错误的 embedded row version
  进入 v5 wrapper；
- SDK070 的 W-B 仍要求保留 production-compiled public offline metadata-v3
  writer，而阶段 2 current writer 实现已删除该 API，文档和代码不能同时成立；
- 一项测试名称/局部变量仍写成 v4/dual-layout，实际合同已经是“拒绝 v3 +
  current v5”，名称会误导后续维护。

主代理立即停止冲突相关的发布并请求用户裁决。用户选择方案 B，明确只撤销 W-B
中的 public offline metadata-v3 writer 保留条款：

- metadata-v3 writer API 删除；历史 v3 fixture 使用测试内显式冻结的 Arrow
  schema/batch；
- metadata v3 exact schema、reader、legacy validator 与精确文件解码继续保留；
- public offline source-v2 writer、schema 与 legacy row-grain reader不在本次
  裁决范围，继续保留。

其余两项执行最小修复：recovery 在构造 v5 row 前精确拒绝行内版本不是 v5 的文件，
并新增回归测试；误导测试名称/变量已改为 v5 实际语义。另补充 v4 WS claimant
正向 recovery fixture，证明 legacy claimant 路径仍可用。本节只记录首轮问题；
最终结果以上一节复审记录为准。

## 2026-07-28：阶段 2 metadata v5 最小实现与本地验证

阶段 1 commit `470b7cd82d6242cdf9040918ad46993e291b828c` 已成功 push
到 `origin/fix-rtds-silents`，且本地 HEAD、upstream 与远端 ref 核对一致；阶段
1 因此闭合。

阶段 2 先以失败 contract 冻结 v5 精确等于 v4 删除
`resolution_evidence_raw_sha256`，随后完成以下生产修改：

- current schema/writer 升级为 `polymarket_market_metadata_v5`，只从
  raw-event、Arrow/Parquet、current validator、producer 与内存估算删除目标字段；
- crate-private v3 prefix 仅作字段 carrier，旧 digest slot 在构造 v5 时强制为
  `None`，且不进入 v5 raw JSON、物理 schema、公开 current row 或校验合同；
- Gamma/WS terminal 不再生成第二 digest；真实
  `source_evidence_payload_json`/`source_evidence_raw_sha256` 完整性 owner、
  winner/outcome、terminal owner 与 WS claimant pair 保持；
- current writer 只接受 v5；v3/v4 只保留测试 fixture、exact reader/validator 与
  v4 claimant recovery 所需路径；按用户方案 B 不保留 public offline
  metadata-v3 writer，且 public offline source-v2 writer 不受影响；没有 dual
  writer、迁移、schema adapter 或 v4/v5 混用禁令；
- restart recovery 精确识别 v3/v4/v5 footer 与 schema：v3 只保留 exact
  decode/旧 validator 且继续禁止成为 source-v3 claimant authority，v4 保留
  legacy claimant recovery，v5 成为 current recovery；coherent v4/v5 lifecycle
  组合允许恢复。

验证结果：

- metadata layout contract `20/20` 通过，覆盖 v5 exact schema、五类 row
  round-trip、source digest owner、v3/v4 frozen contract；
- event adapter、Polymarket source、writer supervisor、resolution recovery 与
  claimant/version 定向测试通过；新增的单次版本传播测试证明 finalized delta、
  footer、embedded recovery metadata 与 manifest per-file 均为 v5，且物理列不含
  被删字段；
- `cargo test -p data-collect --locked`、`cargo check -p data-collect --tests
  --locked`、严格 Clippy、fmt check 与 `git diff --check` 均通过；
- Attempt5 的五个 metadata v4 文件仍为 `319` 行、`70` 列、footer v4，旧 digest
  列存在且 claimant pair 保持最后两列，PyArrow 全部可读；
- 冻结 AC-12 validator SHA-256 仍为
  `f254dea3db0f6049018d5b22a2595e85ddbdb67e821503ecf7fbe395df2042b8`；
  三份既有报告 SHA-256 分别保持
  `920f472b22d4adc9356ed22f47a68f991d18edad1370a2f665511a5fb519fc59`、
  `72177d42b44257ebce0d1b2ed3f60d728907d2aca09ecf2072a1da721c0f0fc2`
  与
  `14db1525ad17e5e101f25b0c7779af372b64cdba6936499ce8e98d65b7bf6619`，
  均与冻结 inventory 一致。

本轮没有新增独立 100-cycle gate；完整 package tests 只是自然执行仓库既有测试。
本节记录的是首轮审查前状态；当前最终复审结果以上文记录为准。

## 2026-07-28：阶段 1 aggTrade v7 最小实现与本地验证

先写并确认旧代码下失败的 schema/writer 合同后，完成以下生产修改：

- current schema version 从 `binance_aggtrade_v6` 升为
  `binance_aggtrade_v7`；
- 从 current row、三个构造器、validator、Arrow schema、raw event、writer
  parser 和 Parquet batch 删除 `websocket_timestamp_received`；
- 保留 `timestamp_received` 作为唯一 WebSocket transport receive time，并保持
  event/trade time、ingress/admission 坐标及四个 REST nullable 兼容列不变；
- current writer factory 精确只接受 v7，统一将同一版本常量传播到 row、footer、
  published result、embedded recovery metadata、finalized delta 与 manifest
  per-file fact；
- recovery 算法仍只投影 ID、source-native payload 和可选 `record_kind`，没有
  新增 schema adapter、迁移器、旧 writer 或额外运行时 forbidden-key 校验。

兼容与回归测试同时确认：

- v7 trade/invalid raw event、Arrow 与 Parquet 均不存在被删字段，两个 row 的
  `timestamp_received` 值保持；
- v5 手写旧物理 schema 与 v6 冻结旧物理 schema 可恢复且恢复前后文件 SHA-256
  不变；v6 invalid row 不进入成交 ID 恢复窗口；
- v5、v6、未知版本标签在创建 current writer 时立即失败且不产生文件；v7 writer
  也拒绝携带 v6 row version 的写入；
- published result、footer、embedded recovery metadata、finalized delta 与
  manifest file fact 均记录 v7。

验证结果：

- aggTrade layout、writer、handoff 三组集成测试：`56/56` 通过；
- aggTrade runtime/source/recovery 定向库测试与 writer-supervisor 版本传播测试
  通过；
- `cargo test -p data-collect --locked` 全部通过，lib 为 `351 passed / 1
  ignored`，所有 integration 与 doc tests 通过；
- `cargo check -p data-collect --tests --locked`、严格 Clippy、fmt check 与
  `git diff --check` 通过。

没有新增或恢复独立 100-cycle 验收；完整 package test 自然执行仓库已有测试，不把
历史循环测试重新定义为本阶段 gate。本节记录的是阶段 1 首轮审查前状态；阶段 1
最终闭合事实以下文记录为准。

## 2026-07-28：阶段 1 首轮独立审查问题与修复

三路独立审查均确认生产改动范围、版本传播、v5 只读恢复和无独立 100-cycle gate
正确。代码与简单性两路各报告同一个 `Important 1`：v6 fixture 最初仍从 current
v7 writer 的 schema/columns 反向构造，不能独立冻结旧 `DataType`、nullability
和值语义。文档/验收一路报告 `Important 3`：同一个 v6 fixture 问题、本 changelog
仍停留在“阶段 0 待发布”的旧游标，以及 SDK070/RTDS 六份顶部仍保留阶段 0
`STAGE1_NEXT` 状态。三路首轮均为 `CHANGES_REQUESTED`，不存在 Critical 或
Minor。

已执行最小修复：

- v6 fixture 改为测试内独立的显式 Arrow schema 和两行字面量
  `trade|invalid_input`，不再调用 current writer、current schema 或 current row
  constructor；
- 继续验证 footer 为 v6、invalid row 不进入恢复窗口、恢复前后 SHA-256 不变；
- 新增完整 Arrow `Field` 比较，证明 v7 精确等于冻结 v6 schema 删除目标列；
- 修正本 changelog 的旧阶段 0 游标，并把 SDK070/RTDS 六份现行状态同步为
  `STAGE1_IMPLEMENTED_REVIEW_PENDING`。

修复后的 v6 定向恢复测试已通过。阶段 1 仍等待复审，不把首轮问题闭环冒充阶段
完成。

## 2026-07-28：阶段 1 三路最终复审通过

修复后由原三路 `gpt-5.6-sol`/Ultra 审查者重新检查完整当前 diff：

- 代码正确性、legacy recovery 与版本传播审查：
  `PASS / Critical 0 / Important 0 / Minor 0`；
- 简单性、过度校验与测试独立性审查：
  `PASS / Critical 0 / Important 0 / Minor 0`；
- demand/plan/changelog、umbrella 游标与验收覆盖审查：
  `PASS / Critical 0 / Important 0 / Minor 0`。

复审确认生产路径没有新增 schema adapter、迁移器、第二 writer、shadow path 或
独立 100-cycle gate；v6 fixture 与 current v7 实现已相互独立；验收 1、3、4、6
的 aggTrade 部分已通过，验收 8 的文档同步与独立审查已通过，commit/push 和三方
ref 核对仍待发布完成。阶段 1 只在承载本记录的代码+文档 commit 成功 push 且本地
HEAD、upstream 与远端 ref 一致后闭合；在此之前状态为
`STAGE1_CLOSED_ON_PUBLISH`。

## 2026-07-28：两个候选重复字段完成消费者审计

用户计划删除：

- aggTrade `websocket_timestamp_received`；
- Polymarket metadata `resolution_evidence_raw_sha256`。

主代理与三路独立 `gpt-5.6-sol`/Ultra 只读审计共同确认：

- current v6 new-write 的 WebSocket timestamp 是 `timestamp_received` 的严格别名；
- 历史 v5 merged row 形成反例，因此只能在新 v7 删除；
- resolution digest 在 owned terminal row 严格等于 source digest，其余 row 为空；
- 没有 training、data-pipeline、paper 或 live 按字段名消费二者；
- metadata recovery 只因 exact schema/完整行校验读取第二 digest，不以其作独立业务
  决策；
- 原地改变 v6/v4 会破坏已存在 Parquet/footer/manifest/schema identity。

审计量化证据：

- aggTrade finalized 五文件：`223,794` 行全部等值；重复列 compressed chunks
  `309,921` bytes，约占文件总大小 `2.95%`；
- metadata finalized 五文件：共 `319` 行，`50/50` 个非空 resolution digest 与
  source digest 相等，重复列 compressed chunks 共 `2,290` bytes。

一名迁移审查者建议因 metadata 存储收益较小而延期 v5。主代理没有采用该调度建议：
用户目标是消除重复语义和重复校验，且当前没有真实下游字段消费者，现在实施比形成更多
消费者后迁移更简单。风险通过 v7/v5 分阶段、分 commit 和 legacy exact reader 隔离。

## 2026-07-28：用户批准新 schema 裁决

用户批准：

- future new-write 分别升级为 `binance_aggtrade_v7` 和
  `polymarket_market_metadata_v5`；
- 只删除上述两个字段；
- 历史 aggTrade v5/v6、metadata v3/v4 只读，不迁移、不重写；
- 两项分阶段实施、验证、独立审查、commit 和 push。

该决定精确取代以下 current new-write 条款：

- source-availability v6 的字段保留边界；
- resolution-followup 中 owned terminal 必须持有第二 digest 及 metadata v4 不可变
  前缀只用于 current new-write 的部分；
- SDK070 中 source v3 + metadata v4 current writer/recovery 组合。

旧文件、旧 schema 和历史 changelog 事实不被回写。

文档自审同时识别并修正两个边界：

- 既有 AC-12 offline validator 位于冻结 validation artifact，不能原地改成 v5；
  v5 必须新增独立验证入口；
- 未整体批准的 rebuild-core draft 仍写旧 v6 名称，但它没有对应 plan/changelog，
  不是 current collector authority；本事项只把它记录为后继 watch item，不单独修改。

## 2026-07-28：Attempt5 按用户指令停止

停止前核实：

- 精确 collector PID `1091669`，wrapper/进程组 `1091647`；
- binary 与 dataset root 均属于
  `target/rtds-stage2-attempt5-full-training-20260727T105900Z`；
- run/build commit 为 `92fb71883cad19285ae0a291e2a8ee94698133dd`，工作树干净。

执行与结果：

- 向精确进程组发送 `SIGINT`，collector、wrapper、tee 均退出；
- wrapper 记录 `collector_exit=101`、`tee_exit=130`、`end_route_exit=0`；
  collector 与 tee 均为非零退出，不记作正常完成；
- monotonic elapsed `16233.700s`；manifest coverage 从
  `2026-07-27T11:02:38.074Z` 到 `2026-07-27T15:33:10.149Z`，
  顶层 manifest 为 `state=open`，仅
  `collection_day_coverage.state=ended_known`；
- 结束香港路由仍为与开始相同的 `8.210.176.1 / HKG / HK`；
- 数据 root 留下 `27` 个 finalized Parquet、总计 `297,802,741` bytes；
- `.running` 文件为零；旧数据未删除、未改写。

Attempt5 固定为
`USER_TERMINATED_FOR_APPROVED_SCHEMA_CHANGE + INCOMPLETE_8H`。它只提供旧
v6/v4 部分日前向与本次重复字段审计证据，不计为 v7/v5 或 RTDS 阶段 2 完成。
新 schema 闭合后的唯一长跑游标为新 root 的完整六源 Attempt6。

## 2026-07-28：阶段 0 三路独立终审通过

- Attempt5 运行证据审查、authority/scope 审查及 demand/plan/changelog 合同审查
  均由独立 `gpt-5.6-sol`/Ultra 子代理完成。
- 成立问题已闭环：精确区分顶层 manifest `open` 与 coverage `ended_known`；修正
  metadata source-only 矩阵；移除无必要的 v4/v5 跨文件混用门；限定 AC-12 为
  source v2 + metadata v3 历史证据；同步 Attempt6 游标；补齐 metadata v5
  版本出口验证。
- 最终三路审查均为 `PASS / Critical 0 / Important 0 / Minor 0`，且
  `git diff --check` 通过。承载本记录的文档 commit 成功 push 且本地 HEAD、
  upstream 与远端 ref 一致时，阶段 0 条件式闭合。

## 当前执行游标

阶段 0 已由 commit `3d5f37e3f95f82adf2d01f3cd6190944867bff38` 发布并完成
本地 HEAD、upstream 与远端 ref 核对。阶段 1 已由 commit
`470b7cd82d6242cdf9040918ad46993e291b828c` 发布并完成三方 ref 核对。阶段 2
的 metadata v5 实现、定向与完整本地门、三路最终复审已通过；发布阶段 2 commit
并完成三方 ref 核对后，进入新 root 的完整六源 8 小时 Attempt6 与最终收口。

本事项没有执行 production activation、canonical cutover、远端部署、历史迁移或数据
清理。
