# data-collect 重复 typed 列删除 changelog

> 事项编号：`DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL`
>
> 对应需求：`demand/data-collect-redundant-typed-column-removal-demand.md`
>
> 对应计划：`plan/data-collect-redundant-typed-column-removal-plan.md`
>
> 状态：`APPROVED_STAGE0_CLOSED_ON_PUBLISH_STAGE1_NEXT_ATTEMPT5_USER_TERMINATED_INCOMPLETE_8H`

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

阶段 0 将随本次文档 commit 成功发布而闭合；随后依次进入：

1. `binance_aggtrade_v7`；
2. `polymarket_market_metadata_v5`；
3. 完整六源 8 小时 Attempt6 与最终收口。

本事项没有执行 production activation、canonical cutover、远端部署、历史迁移或数据
清理。
