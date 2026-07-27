# data-collect 重复 typed 列删除需求

> 事项编号：`DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL`
>
> 对应计划：`plan/data-collect-redundant-typed-column-removal-plan.md`
>
> 对应 changelog：`changelog/data-collect-redundant-typed-column-removal-changelog.md`
>
> 状态：`APPROVED_STAGE0_CLOSED_STAGE1_CLOSED_STAGE2_CLOSED_ON_PUBLISH_ATTEMPT5_USER_TERMINATED_INCOMPLETE_8H_ATTEMPT6_PENDING_AFTER_V7_V5`

## 一、目标与用户裁决

用户于 2026-07-28 批准从未来 current new-write schema 删除两个可无损派生的重复字段：

1. 从 `binance_aggtrade_v7` 删除 `websocket_timestamp_received`；
2. 从 `polymarket_market_metadata_v5` 删除 `resolution_evidence_raw_sha256`。

两个 schema 分阶段实施、验证、审查和发布。历史 aggTrade v5/v6 与 metadata v3/v4
保持原物理 schema、原语义和只读兼容；不迁移、不重写、不删除历史 Parquet。

本需求只删除上述两个字段。其他列、artifact、source mapping、writer owner、manifest
schema、业务事件、恢复策略和 source 行为保持不变。

用户于 2026-07-28 在阶段 2 首轮独立审查暴露旧 W-B 与当前实现冲突后选择方案 B：
撤销 W-B 中“保留 public offline metadata-v3 writer”的窄条款并删除该写入 API。
metadata v3 的冻结 schema、exact reader、legacy validator 与精确文件解码继续保留；
历史 v3 fixture 改由测试内显式冻结的 Arrow schema/batch 构造，不借用 current
writer。该裁决不涉及 public offline source-v2 writer；source-v2 writer、schema 与
legacy row-grain reader继续保留。

## 二、已核实事实

### 2.1 aggTrade

- 当前 `binance_aggtrade_v6` 只生产 WebSocket `trade|invalid_input` 行。
- 所有 v6 构造路径都把 `websocket_timestamp_received` 赋为
  `timestamp_received`；validator 也要求二者相等。
- Attempt5 停止后的五个 finalized aggTrade 文件共有 `223,794` 行，两个字段
  `223,794/223,794` 完全相等且无 null；重复列的 compressed column chunks
  共 `309,921` bytes，占五文件总大小约 `2.95%`。
- 历史 v5 曾有 WebSocket、REST-only 和 merged 行；merged 行的
  `timestamp_received=max(websocket_timestamp_received,
  rest_backfill_timestamp_received)`，因此 v5 的 WebSocket 接收时间不能从
  availability 时间逆推。
- 当前 restart recovery 不消费该字段，只投影成交 ID、source-native payload 和
  可选 `record_kind`。

### 2.2 Polymarket metadata

- owned terminal producer 从同一个 source payload digest 同时赋值
  `source_evidence_raw_sha256` 与 `resolution_evidence_raw_sha256`。
- 当前 validator 要求后者等于前者；recovery 的业务决定不独立消费后者。
- Attempt5 停止后的五个 finalized metadata 文件共有 `319` 行；`50/50` 个非空
  `resolution_evidence_raw_sha256` 都与 source digest 相等；不存在
  resolution-only 行，双非空行也不存在不一致。其余矩阵为 source-only `266`
  行、双空 `3` 行。
- 唯一真实完整性 owner 是 `source_evidence_payload_json` 与
  `source_evidence_raw_sha256` 的 payload/hash 对应关系。删除第二个别名不删除
  source payload、source digest、winner、outcome、evidence source、claim owner
  或 claimant pair。
- metadata v3 的 exact schema、旧行 validator 与精确文件解码继续识别并验证该
  字段；metadata v4 另保留 source-v3 claimant 的 legacy recovery。v3 不因此
  重新获得 claimant recovery authority。

## 三、冻结的新写 schema

### 3.1 `binance_aggtrade_v7`

`binance_aggtrade_v7` 等于当前 v6 物理列合同删除
`websocket_timestamp_received` 后的精确版本：

- `timestamp_received` 继续表示 collector 完整收到原始 WebSocket application
  input 的 wall-clock UTC 毫秒，是唯一 transport receive timestamp；
- Binance `event_time`、`trade_time`、`websocket_ingress_sequence`、
  `collector_admission_sequence` 和 `collector_admission_time_utc_ms` 保持各自
  不同语义；
- `ingest_source` 以及四个既有 REST nullable 兼容列保持原样；本事项不顺手删除
  其他字段；
- trade 与 invalid input 的 raw-event 编码、canonical row、Arrow schema 和
  Parquet batch 都不得再包含被删字段；
- current writer 只接受精确 `binance_aggtrade_v7` new-write，不得把 v7 物理布局
  标成 v5/v6；v5/v6 只能进入 legacy read/recovery。

### 3.2 `polymarket_market_metadata_v5`

`polymarket_market_metadata_v5` 等于当前 v4 物理列合同删除
`resolution_evidence_raw_sha256` 后的精确版本：

- v4 末尾两个 nullable WS claimant 字段保留原顺序、类型、nullability 和语义；
- `source_evidence_raw_sha256` 继续由实际存储的 source payload bytes 计算，并在
  source-bearing row 上执行现有 payload/hash 完整性验证；
- owned terminal 继续要求完整 winner、outcome、resolution evidence source、
  terminal claim owner 及适用的 source evidence；不再要求或生成第二个相同 digest；
- 非 owned-terminal row 不再执行“第二 digest 必须为空”的重复矩阵校验；
- current writer 只接受精确 metadata v5 new-write。v3 保留 exact reader 和
  legacy validator，v4 另保留 exact reader、legacy validator 与 claimant
  recovery；public offline metadata-v3 writer 不再存在，v3/v4 fixture 必须由测试
  内冻结的旧 Arrow schema/batch 显式构造，不得用 v5 writer 伪造。
- 为避免复制 50 多个冻结字段，crate-private current 实现可以复用 v3 prefix 作为
  compatibility carrier，但其中旧 resolution digest slot 必须恒为 `None`，不得进入
  v5 raw-event JSON、Arrow schema、Parquet、公开 current row 或 v5 校验合同；这不
  构成保留 v5 字段或第二 writer。

### 3.3 版本与激活边界

- 两个 source kind 分别升级版本，不建立全局 schema 版本或通用 schema adapter。
- Parquet footer、embedded recovery metadata、published result 和每日 manifest
  per-file fact 必须记录对应 v7/v5；每日 manifest 顶层 schema 不升级。
- current 组合固定为 `polymarket_orderbook_v3 +
  polymarket_market_metadata_v5`。metadata v3/v4 保持历史只读并由各自 exact
  per-file reader 识别；不增加跨文件 v4/v5 lifecycle 混用禁令。
- 首次完整前向验证使用新的空 dataset root。不得续写 Attempt5 root，不得对其
  `.parquet`、manifest 或旧 footer 做迁移。
- production activation、canonical dataset-root cutover、远端部署和历史清理仍不在
  本事项范围。
- `/home/chillchill/validation-artifacts` 下既有 AC-12 validator 与报告只属于冻结
  的 source v2 + metadata v3 历史验收证据，不得原地修改，也不冒充 v4/v5
  验收。v4 由现有 fixture 与 Attempt5 文件保护；v5 使用新的 repo-local contract
  test 或新的独立 validation artifact。
- 未整体批准且没有对应 plan/changelog 的 `demand/rebuild-core-demand.md` 只保留为
  downstream watch item；其中旧 v6 名称不构成 current collector authority，待该事项
  恢复完整审核时再在其自身文档体系同步。

## 四、简单性与可靠性边界

- 不为删除两列新增 sidecar、dual-write、shadow writer、第二 canonicalizer、通用
  schema migration framework 或新配置项。
- 只保留版本分派、legacy exact reader 和 current exact writer 所需的最小分支。
- 删除重复 equality/required/null 校验，但保留真实 payload/hash、identity、
  ownership、claimant、时间因果、Arrow 类型/nullability 和恢复完整性校验。
- 不增加固定 100-cycle、任意性能百分比或重复全量测试。验证只覆盖受影响 schema、
  producer、writer、round-trip、legacy recovery 和一次完整六源本地前向采集。

## 五、Attempt5 与后继验证

用户批准先停止当时运行中的 Attempt5。该运行：

- 使用 clean/pushed commit `92fb71883cad19285ae0a291e2a8ee94698133dd`；
- 从 `2026-07-27T11:02:38.074Z` 运行到 `2026-07-27T15:33:10.149Z`，
  顶层 manifest 保持 `state=open`，其中
  `collection_day_coverage.state=ended_known`，约 `4.509` 小时；
- 收到用户停止指令后向精确进程组发送 `SIGINT`，wrapper 记录
  `collector_exit=101`、`tee_exit=130`、`end_route_exit=0`；前两者均为非零，
  因而不是正常零退出；
- 留下 `27` 个 finalized Parquet、`297,802,741` bytes，且没有 `.running` 文件。

Attempt5 永久记录为
`USER_TERMINATED_FOR_APPROVED_SCHEMA_CHANGE + INCOMPLETE_8H`，不是新 schema
验收证据。v7/v5 阶段闭合后从新 clean/pushed HEAD、新 binary、新 artifact root 和空
dataset root 执行完整六源 8 小时 Attempt6。

## 六、验收标准

1. v7 Arrow/Parquet/raw-event 中不存在 `websocket_timestamp_received`，且
   `timestamp_received` 语义、trade/invalid 行、ingress/admission 坐标保持不变。
2. v5 Arrow/Parquet/raw-event 中不存在 `resolution_evidence_raw_sha256`，且
   `source_evidence_payload_json` 与 `source_evidence_raw_sha256` 完整性验证保持。
3. current writer 精确拒绝用旧版本标签写新物理布局；footer、embedded metadata、
   published result 和 manifest per-file version 一致。
4. aggTrade v5/v6 的 legacy recovery、metadata v3 的 exact
   schema/read-validation，以及 metadata v4 的 legacy claimant recovery 继续按
   原合同工作；public offline metadata-v3 new-write API 不存在，source-v2 public
   offline writer 保持可用，历史文件不被重写。
5. metadata v5 的 WS claimant pair、Gamma/WS terminal ownership、local reference
   和 source v3 claimant recovery 与 v4 保持等价，除被删别名外无行为变化。
6. 受影响 contract/round-trip/recovery 测试、package tests、check、严格 Clippy、
   fmt 和 diff check 通过。
7. Attempt6 使用完整 training collector、全部六个 `LIVE_SOURCES`、正式 Parquet
   writer 与每日 manifest 在新本地 root 连续运行 `28800` 秒并退出 0；六源非空
   finalized Parquet、manifest/file/PyArrow 一致且无临时残留。
8. demand、plan、changelog 与当前 umbrella/RTDS 游标一致；每阶段经独立
   `gpt-5.6-sol`/Ultra 审查、commit 并 push。
