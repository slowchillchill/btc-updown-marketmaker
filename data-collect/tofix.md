# Data Collect 待修复问题

## DC-TOFIX-20260709-001：Binance depth snapshot 缺少及时 finalized artifact 与专用重试

状态：`已确认、观察中、未授权修复`。

首次发现：`2026-07-09 UTC`。

当前复核：`2026-07-10T20:31:03Z..2026-07-10T21:03:57Z`。

优先级：`P1 数据耐久性`；不是“主连续采集已停止”的 P1 运行故障。

用户决定：只更新本文档并继续只读观察；暂不修改代码、重启、重新部署或改变数据准入。

### 1. 当前适用版本与运行身份

- 本地仓库：`/home/chillchill/btc-updown-marketmaker`
- 本地分支：`data-collect-pmxt-hourly-retention`
- 当前本地 HEAD：`b83654a0fca40de29972bf20fda9917591e78866`
- 远端运行 release：`3a1b7254fc1c-aggtrade-v3-20260710T143830Z`
- 远端 release commit：`3a1b7254fc1cef00ab950acb906181ad7cbd1e3d`
- 远端运行 binary SHA256：`78c59f31ce8a9803d72c791c1ff066ced6660f7072137e6719b0dd9570c3058e`
- 远端服务：`data-collect-training-collector.service`
- 当前运行实例：`training-longrun-20260710T144045Z`
- 当前 PID：`951266`
- 当前启动时间：`2026-07-10T14:40:45Z`

本地 HEAD 包含归档重试和运行回执等后续提交；远端采集 binary 来自 `3a1b725...`。两者必须分开记录，不能把本地 HEAD 冒充为远端部署 commit。

### 2. 问题边界

本问题只涉及：

1. `binance_depth_snapshot` 是稀疏事件源，成功 snapshot row 可能长期停留在开放的 Arrow/Parquet writer 内，没有及时形成 finalized parquet 和 manifest。
2. Binance REST snapshot 当前是单次请求，没有 snapshot 专用的 bounded retry/backoff/retry-exhausted 处理。
3. 已 finalized 的 `binance_depth_update` 可以引用一个尚未 finalized 的 bootstrap snapshot ID，存在硬崩溃后的独立重放风险。

本问题不等于整个 collector 停止，也不表示连续 depth update 当前存在 gap。判断采集器整体健康时必须把 snapshot 耐久性与连续源新鲜度分开。

### 3. 观测事实

#### 3.1 历史首次发现

`2026-07-09T04:57Z..2026-07-09T05:13Z` 观察到：

- `binance_depth_update`、`binance_aggtrade`、RTDS、metadata 和 Polymarket market 继续推进；
- 最新完整小时的连续源 manifest 为 `Complete`、`source_gaps=[]`；
- `binance_depth_snapshot_2026-07-09T00-running.parquet` 长期保持 `0B`，由采集进程打开且没有对应 finalized artifact/manifest。

该观察是问题首次发现证据，不再代表当前部署版本。

#### 3.2 当前 v3 release 再确认

在 `2026-07-10T20:31:03Z..2026-07-10T21:03:57Z` 只读检查中：

- 服务为 `active/running`，`NRestarts=0`、`ExecMainStatus=0`；
- 进程 executable、`current` binary 与 release SHA256 一致；
- 近 12 小时没有 warning/error/panic/OOM/自动重启；
- 六个 source writer 均由 PID `951266` 持有；20 秒内 CPU、写调用计数和 Binance socket 接收字节继续增长；
- `21:00 UTC` rollover 后，T20 aggTrade=`30256` rows、depth update=`36000` rows、RTDS=`3554` rows、metadata=`36` rows，均为 `Complete`、`required_typed_fields_present=true`、`source_gaps=[]`；
- Polymarket market 在观察期间继续完成 part，T20 最终 part 也为 `Complete/source_gaps=[]`。

当前 snapshot artifact：

```text
/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw/
  utc_day=2026-07-10/source_kind=binance_depth_snapshot/
  binance_depth_snapshot_2026-07-10T14--part=1-running.parquet
```

截至 `2026-07-10T21:03:57Z`：

- 文件创建时间：`2026-07-10T14:40:47Z`
- 文件大小：`0B`
- 文件仍由当前采集 PID 打开
- 最新 finalized snapshot ID：`97250195255`
- 当前 T17/T18/T19/T20 finalized depth-update manifest 引用的 snapshot ID：`97250276077`
- 对应 depth update 每小时 `36000` rows、`gap_rows=0`、`source_gaps=[]`

因此当前事实是：连续 depth update 正常，但它引用的新 bootstrap snapshot 尚未成为 finalized artifact。

### 4. 当前代码为什么会出现该现象

#### 4.1 Snapshot 不是固定频率抓取

REST snapshot 只在以下事件触发：

- depth WebSocket 首次连接后的 bootstrap；
- depth WebSocket 断开并重连成功；
- 收到新 UTC 日第一条 depth update；
- stitch gap 要求 resnapshot。

当前没有“每小时抓一次 snapshot”的 timer，也不需要为修复本问题新增这种 timer。

#### 4.2 每秒 batch flush 不等于 Parquet durable flush

运行时参数为：

- 每 source batch 最多 `5000` events；
- batch 最大等待约 `1000ms`；
- writer channel 容量为 `8` batches；
- segment 保护门为 `300000` typed rows 或估算 `128MiB`。

一条 snapshot 通常在正常负载下约一秒内从 source batch 发送到独立 writer 线程。这里的 flush 只是：

```text
source batch -> WriterCommand::Events -> snapshot writer thread
```

它不是 `ArrowWriter::flush()`、文件 `sync/fsync` 或 segment finalize。

当前 `parquet 59.0.0` 默认 row group 上限为 `1048576` rows；项目没有显式调用 `ArrowWriter::flush()` 或 `sync()`。单行 snapshot 不会达到 row-group 门，因此可能完整缓存在 ArrowWriter 内存中，开放的 running 文件仍为 `0B`。

#### 4.3 Hour rollover 也是下一事件驱动

writer 只在处理“下一条同 source 事件”之前检查：

- 下一事件的 UTC hour 是否不同；
- active segment 是否达到 `300000` rows；
- active segment 是否达到估算 `128MiB`。

正常 shutdown 会 close active writer 并发布 manifest，但单纯墙钟跨整点不会唤醒一个没有新事件的 snapshot writer。代码中的 `15min` 常量没有进入 segment rollover 判断，不存在 15 分钟强制 finalize。同一 UTC hour 内的 reconnect/stitch-gap snapshot 只会追加到原 active segment；只有下一 snapshot 属于不同 UTC hour、旧 segment 达到 rows/bytes 门，或发生 graceful shutdown 时才会 finalize。

如果没有 reconnect 或 stitch gap，下一次 snapshot 通常在 UTC 日边界出现。该事件跨 UTC hour，会使旧 snapshot finalize，而新日 snapshot 又会进入新的 running segment；问题只是向最新 snapshot 迁移，并未从机制上消失。

### 5. 当前影响判断

#### 5.1 已确认不受影响

- collector 主进程当前正常运行；
- Polymarket、Binance depth update、Binance aggTrade、RTDS 连续采集当前未停止；
- 最新完整小时 manifest 没有 source gap；
- depth stitch 当前 `gap_rows=0`；
- 下一条属于不同 UTC hour 的 snapshot、rows/bytes 门或 graceful shutdown 可以把旧 active snapshot 发布出来；同小时 snapshot 本身不能。

#### 5.2 仍存在的风险

1. 在 active snapshot finalize 前发生进程强杀、机器断电或 writer finalize 失败时，最新 bootstrap snapshot 可能丢失。
2. 已 finalized depth-update manifest 当前可能引用无法在 finalized snapshot artifact 中找到的 ID，不能保证独立重放。
3. snapshot REST 请求当前一次失败即返回 `None`。depth source task 随后结束，当前 supervisor 会将其判为 `source_task_ended_unexpectedly`，collector 非零退出；systemd `Restart=always` 会在 `10s` 后重启全部来源。
4. `0B running` 本身不能证明 snapshot 没有被抓取，但它明确不是可审计、可发现、可校验的 finalized artifact。

### 6. 代码位置

- `data-collect/src/live_runtime.rs`
  - `fetch_binance_depth_snapshot(...)`：单次 REST snapshot 请求与事件发送；
  - `run_binance_depth_source(...)`：bootstrap/reconnect snapshot；
  - `run_binance_depth_update_ws_source(...)`：UTC 日边界和 stitch-gap resnapshot；
  - `source_writer_worker(...)`：下一事件到达时才检查 rollover；
  - `live_segment_rollover_reason(...)`：只有 hour/rows/bytes 三个条件。
- `data-collect/src/writer.rs`
  - `TypedParquetSegmentWriter::write_batch(...)`：写入 ArrowWriter；
  - `TypedParquetSegmentWriter::finalize(...)`：close、running->digesting、SHA256、digesting->final。

### 7. 候选最小修复方案（仅记录，未授权实施）

#### 7.1 Snapshot 单事件立即 finalize

原先的“方案 A/方案 B”不需要保留为两套实现。最小正确方案是：

> 保留现有 hourly 路径、文件命名、typed schema、manifest publisher 和 artifact discovery；`binance_depth_snapshot` 每成功写入一行后，立即复用现有 `finalize_segment(..., "snapshot_immediate_finalize", ...)` close 当前 segment 并写 manifest。同一小时再次产生 snapshot 时沿用 `--part=N`。

不得只调用 `ArrowWriter::flush()` 作为完成条件；核心结果必须是可读 finalized parquet、SHA256 和 manifest。不要新增 snapshot 专用目录、第二套 publisher、第二套 discovery 或固定每小时 snapshot 请求。

#### 7.2 Snapshot REST 有界重试

- 复用已有 retry policy/trace 语义，不建立第二套 circuit-breaker 框架；
- 使用有默认值的 max attempts、max elapsed、backoff ceiling 和 jitter，并由现有 policy 对象承载；本事项没有真实调参需求前，不新增多组 CLI/env 配置表面；
- 每次 attempt 先验证 HTTP 成功、body 非空、JSON 可解析，以及 `lastUpdateId/bids/asks` 可用；
- 中间失败进入 compact retry/health evidence；后续恢复成功时不得把瞬时错误写成永久 `source_gap`；
- 预算耗尽后只写一次最终 gap 和 retry-exhausted 状态，再进入现有失败/恢复路径。

#### 7.3 Snapshot 与 depth update 引用完整性

必须新增核心验收，但不要求复制字段：

> 每个 finalized depth-update manifest 的 `binance_depth_stitch.snapshot_last_update_id`，必须能在一个 finalized snapshot parquet/manifest 中找到；找不到时，该 depth-update 数据不得被标记为可独立重放或进入正式训练 cohort。

#### 7.4 保持不变的边界

- 不改变 Binance depth update stitch predicate；
- 不改变 continuous depth update 的 hourly layout；
- 不增加固定小时 snapshot fetch；
- 不重写或伪造已有 running/finalized artifact；
- 不把 snapshot durability issue 扩大成第二套采集平台或训练 pipeline。

### 8. Manifest/Parquet 字段去重

候选实现不得重复增加已有证据：

| 证据 | 当前位置 | 处理 |
|---|---|---|
| snapshot ID | manifest `binance_depth_stitch.snapshot_last_update_id` | 复用，不新增同义顶层字段 |
| REST endpoint | snapshot typed parquet `source_endpoint` | 复用，不复制到 manifest |
| first/last receive time | manifest `source_coverage`、start/end、recv window | 复用 |
| close reason | manifest `rollover_reason` | 写为 `snapshot_immediate_finalize` |
| retry attempt/状态 | 当前缺失 | 写 compact retry/health evidence，不作为数据 Complete 的额外门 |
| snapshot trigger reason | raw event 已有、typed/manifest 未保留 | 可选诊断字段，不作为 Complete 条件 |

### 9. 候选验收标准（修复获批后使用）

1. bootstrap 成功后，不等待 UTC 日边界即可看到一行可读 finalized snapshot parquet 和 `Complete` manifest。
2. finalized snapshot SHA256、row count、schema、`last_update_id` 与 manifest 一致。
3. snapshot 成功发布后不长期遗留对应的 `*-running.parquet`。
4. 同一小时第二次 snapshot 使用 `--part=N`，不覆盖旧 artifact。
5. 首次 transient failure、后续成功：最终 artifact 正常，永久 source gap 不包含已恢复瞬时错误。
6. retry budget exhausted：记录一次最终 gap/retry-exhausted，collector 进入现有受控失败/恢复路径。
7. 后续 finalized depth-update 引用的 snapshot ID 可在 finalized snapshot artifact 中找到。
8. depth update 继续 `gap_rows=0`、`source_gaps=[]`，除非真实上游 gap 发生。
9. 不需要等待完整 UTC 日边界；部署重启后的 bootstrap 即可完成主要线上验收。

### 10. 当前观察方案

在用户重新批准修复前，只做只读观察，不自动重启或改变运行状态：

1. 检查 service `active/running`、PID、`NRestarts`、`ExecMainStatus` 和 binary SHA。
2. 检查最新完整小时的 Polymarket、depth update、aggTrade、RTDS、metadata manifest 是否按期闭合且 `Complete/source_gaps=[]`。
3. 检查 Polymarket 当前小时是否继续 part rollover；检查进程 I/O 和 Binance socket 接收字节是否增长。
4. 单独记录 snapshot running/finalized 状态、mtime、fd、finalized snapshot ID，以及 depth-update 引用 ID。
5. 下一次 snapshot 发生时先判断是否跨 UTC hour：跨小时 snapshot、UTC 日边界或 graceful shutdown 后，应检查当前 `97250276077` 是否成为 finalized artifact；同小时 reconnect/stitch-gap snapshot 只记录追加事实，不能误报为应当 finalize。
6. 在 snapshot ID 引用闭环完成前，不把受影响时间段直接判为正式训练可重放数据。

出现以下任一事实时升级报告，但仍不自动修复：

- collector PID 改变或 `NRestarts` 增加，并且 journal/部署回执不能证明这是已批准的受控部署或重启；
- 到整点后 `5min` 仍缺少上一完整小时的连续源 manifest，或 manifest 非 `Complete`/出现 `source_gaps`；
- 在目标市场活跃期间，连续两次、间隔至少 `5min` 的样本都没有新的 Polymarket receive/write/part 进展；不能只因某段时间没有触发 rows/bytes rollover 就升级；
- depth update 出现 `gap_rows>0`、resnapshot 或引用新的未 finalized snapshot ID；
- 下一次跨 UTC hour 的 snapshot、UTC 日边界 snapshot 或 graceful shutdown 后，前一 snapshot 仍未生成 finalized parquet/manifest；
- OOM、磁盘/ inode 水位或 NTP 状态异常。

### 11. 当前决策门

- 已批准：更新本文档、继续只读观察。
- 未批准：代码修改、测试实现、commit/push、重新构建、部署、重启、数据重写或删除。
- 后续若批准实施，必须先为本事项建立同 slug 的 demand/plan/changelog，并按需求、测试、独立审查、干净 release、部署回执顺序执行；不得复用已完成的 aggTrade T08 修复计划冒充 snapshot 修复计划。
