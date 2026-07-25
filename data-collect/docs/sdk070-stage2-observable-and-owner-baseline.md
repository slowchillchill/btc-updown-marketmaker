# SDK070 阶段 2：observable contract 与 owner 基线

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：`changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 基线 commit：`b818e436a115eb61859fa281773342ee58f54f6a`
>
> 证据日期：`2026-07-25`
>
> 状态：`STAGE_2_COMMON_BASELINE_COMPLETE`

## 1. 范围与结论

本文件冻结阶段 2 任务 2.1、2.2 要求的共同基线。它记录当前生产入口、4 个
logical producer、6 个 artifact、day/hour/manifest/shutdown 合同、resolution
状态、paper/live 边界、测试集合、跨 owner 消息和 `#[cfg(test)]` seam；不改变
任何 Rust、schema、payload、fingerprint、admission、RTDS 或部署行为。

阶段 1 的两个终态继续独立成立：

- runtime：`SDK_070_RUNTIME_USABLE`；
- typed contract：`SDK_070_TYPED_CONTRACT_BLOCKED`。

typed blocker 只阻断阶段 3 的 v3 schema/reader/writer/local new-write，不阻断
阶段 2 行为保持的机械拆分。公开网络 smoke 仍为
`NOT_AUTHORIZED / NOT_RUN`。

三份既存未跟踪 RTDS 草案只作为专项游标存在，本阶段没有修改、暂存或删除：

```text
changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md
demand/data-collect-rtds-business-silence-full-reconnect-demand.md
plan/data-collect-rtds-business-silence-full-reconnect-plan.md
```

## 2. 生产入口、producer 与 artifact

### 2.1 唯一 training 调用链

```text
data-collect/src/bin/data_collect_training_collector.rs::main
  -> data-collect/src/runtime.rs::run_training_collector
  -> data-collect/src/live_runtime.rs::run_live_training_runtime
  -> data-collect/src/live_runtime.rs::run_live_training_runtime_with_sources_and_stop
```

`run_live_training_runtime_with_sources_and_stop` 是当前 composition owner。它建立
dataset lease、startup gate、RawDayGate、manifest owner、6 个 writer、4 个
logical producer、runtime-exit watcher 和 rollover coordinator。

### 2.2 四个 logical producer 与六个 artifact

`LOGICAL_PRODUCERS`、`LIVE_SOURCES`、`producer_writer_sources` 与
`logical_producer_for_source_kind` 在当前代码中形成双向映射：

| logical producer | 当前 source owner 入口 | artifact / current schema |
|---|---|---|
| `polymarket` | `run_polymarket_source_with_spec` | `polymarket_market` / `polymarket_orderbook_v2`；`polymarket_market_metadata` / `polymarket_market_metadata_v3` |
| `polymarket_rtds` | `run_rtds_source_with_spec` | `polymarket_rtds_chainlink_btc_usd` / `polymarket_rtds_chainlink_btc_usd_v4` |
| `binance_depth` | `run_binance_depth_source_with_spec_and_control` | `binance_depth_snapshot` / `binance_depth_snapshot_v3`；`binance_depth_update` / `binance_depth_update_v4` |
| `binance_aggtrade` | `run_binance_ws_source` | `binance_aggtrade` / `binance_aggtrade_v6` |

current schema 的唯一代码 owner：

| artifact | schema owner |
|---|---|
| `polymarket_market` | `polymarket_orderbook_layout::polymarket_orderbook_schema` |
| `polymarket_market_metadata` | `polymarket_market_metadata_layout::polymarket_market_metadata_v3_schema` |
| `binance_depth_snapshot` | `source_hourly_layout::binance_depth_snapshot_schema` |
| `binance_depth_update` | `source_hourly_layout::binance_depth_update_schema` |
| `binance_aggtrade` | `source_hourly_layout::binance_aggtrade_schema` |
| `polymarket_rtds_chainlink_btc_usd` | `source_hourly_layout::rtds_chainlink_btc_usd_schema` |

`live_runtime::typed_schema_version` 选择 current writer version，
`writer::TypedParquetSegmentWriter::create_for_typed_source` 验证 schema/version 后建立
writer。阶段 2 只移动 owner，不修改该映射。

### 2.3 代表性真实 artifact

已只读确认以下目录存在于同一个完整 UTC 日：

```text
/home/chillchill/new-datasets/2026-07-22/typed_raw/utc_day=2026-07-22/
```

其下恰好有六个 `source_kind=*` 目录：`binance_aggtrade` 24 个 finalized
Parquet、`binance_depth_snapshot` 1 个、`binance_depth_update` 24 个、
`polymarket_market` 24 个、`polymarket_market_metadata` 24 个、
`polymarket_rtds_chainlink_btc_usd` 24 个。该目录只作 current artifact
代表性证据；其 manifest记录的build commit为
`1d95823614ceed78288ce34cb95fbf23e58ef64d`，不是 SDK070 当前 HEAD 的 live
smoke。阶段 2 不修改它，也不把它冒充任务 1.5 的公开网络验证。

resolution current/next/pending/terminal的代表性真实采集artifact另位于：

```text
/home/chillchill/validation-artifacts/
data-collect-resolution-followup-ac12-20260723T121902Z/
dataset/training/typed_raw/utc_day=2026-07-23/
```

其 manifest记录的build commit为
`a32226863428dcbd82b016538f36855240a41892`。它只证明该历史build的代表性
resolution行为，不证明SDK070当前HEAD的公开网络smoke。

### 2.4 source identity、时间与 admission

所有 producer 进入共同 ingress 前使用 `writer::RawEvent`：

| 语义 | current 字段/owner |
|---|---|
| artifact 与事件身份 | `source_kind`、`source_event_type`、`source_identifier` |
| source 时间 | nullable `source_ts`，具体平台时间另保存在 typed row |
| collector receive | `recv_time_utc_ms` |
| collector identity | `collector_host_id`、`collector_region`、`source_endpoint` |
| current payload carrier | `raw_json`；阶段 2 保持，不提前实施 v3 |
| admission | nullable `RawEventAdmission { sequence, time_utc_ms }` |

`source_admission_authority` 当前只为 `binance_aggtrade`、两个 depth artifact 和
`polymarket_market_metadata` 分配 admission sequence；`polymarket_market` 与
RTDS current row 不在该函数的 admission authority 中。这个不对称是 current
observable fact，不在阶段 2 被“顺手修复”。未来 source v3 的统一坐标仍由阶段 3
合同处理。

| source | current identifier / source timestamp |
|---|---|
| Polymarket market | asset事件使用`slug:asset_id`，`new_market`使用slug；source timestamp来自SDK事件 |
| Polymarket metadata | identifier为slug；没有通用source timestamp，由metadata admission authority排序 |
| RTDS | identifier为`btc/usd`；source timestamp为payload timestamp |
| depth snapshot | identifier为`BTCUSDT`；无source timestamp，另保留fetch/receive、epoch、generation与ingress |
| depth update | identifier为`BTCUSDT`；source timestamp为Binance `E` |
| aggTrade | identifier为symbol；source timestamp为trade time |

代表性合同测试：

- `training_collector_runtime_contract`：CLI、training 调用边界与 artifact；
- `source_hourly_layout_contract`：Binance/RTDS typed row；
- `pmxt_orderbook_layout_contract`：Polymarket v2 row；
- `polymarket_metadata_layout_contract`：metadata v3；
- `parquet_writer_contract`：schema、writer、finalized file；
- `mandatory_producer_source_mapping_is_exact_and_bidirectional`：4/6 双向映射。

## 3. UTC day/hour、writer、manifest 与 shutdown

### 3.1 RawDayGate 状态与正常 rollover

`RawDayGate` 是 raw admission 的唯一进程内 day/generation owner：

```text
Inactive
  -> manifest activation commit
  -> Active(utc_day, generation)
  -> prepare_rollover(boundary)
  -> Closing(old_day, next_day, generation, next_generation, close_id)
  -> four producer fences + six writer finalize acks
  -> old-day manifest durable commit
  -> new-day manifest activation
  -> deferred accepted batches flush
  -> Active(next_day, next_generation)
```

关键 owner 与 durable effect：

| 合同 | owner | durable effect |
|---|---|---|
| startup 10-participant rendezvous | `activate_and_release_startup` | manifest activation 成功后才 release 4 producer + 6 writer |
| raw-day admission/fence | `RawDayGate` | closing 后拒绝或 defer 不属于 active epoch 的 admission |
| producer day fence | 四个 `run_*_logical_producer_with_*barriers` | producer barrier 与旧日 operational counters |
| writer finalize | `source_writer_worker_with_startup` / `send_writer_day_finalize` | running -> digesting -> finalized Parquet，row/bytes/SHA/schema facts |
| rollover rendezvous | `ManifestOwnerState` | 4 个 producer barrier + 6 个 writer ack 后发 `ManifestRolloverReady` |
| commit/activation order | `drive_normal_rollover` / `RolloverCoordinatorState` | 旧日先 commit，再激活新日并 release |
| hourly boundary | `HourlyPartitionPolicy`、depth hour fence、`FinalizeSourceHour` | 按 receive time 的 UTC hour finalized partition |

### 3.2 停止与失败矩阵

| 触发 | current 结果 |
|---|---|
| operator stop / run deadline / capacity limit | `RuntimeExitWatcher` 或 coordinator 先冻结 `PreparedRuntimeExit`，停止新 raw admission，收敛 source、writer、manifest |
| UTC boundary 恰逢 exit | `PendingExit` 完成旧日，不激活新日 |
| source inner/wrapper return或失败 | 记录精确 `RuntimeCloseCause`；可靠边界与不确定边界分别处理 |
| source panic / 无 permit abort | 标为 unclean，禁止伪造 clean closure |
| writer return/panic/result disconnect | 停止对应 writer authority，记录 failure 并收敛全部 runtime resource |
| manifest owner/command/reply失败 | fail closed；未知 durable outcome 不重试同一 mutation |
| queue/writer/permit failure | admission/drop/gap facts按当前 owner记录；不得越过 day gate继续持久化 |
| reply loss | 已 commit 的 activation/rollover mutation不因 reply 丢失而重放；gate/manifest durable state仍是 authority |

当前 coordinated shutdown 顺序冻结为：

1. 建立精确 close identity并stop/fence source；
2. join source期间继续drain ingress并flush；
3. 收齐source control结果并判定clean/unclean；
4. seal `RawDayGate`；
5. drain剩余event queue；
6. flush全部batch；
7. shutdown并证明6个writer已经终止；
8. 只有writer全部终止后才提交manifest shutdown；
9. 等待manifest owner终止，再返回receipt或error。

代表性测试：

- `raw_day_gate_fences_boundary_events_until_normal_rollover_ack`
- `normal_rollover_waits_for_four_producers_and_six_writers`
- `manifest_owner_requires_rendezvous_then_two_normal_rollover_cutpoints`
- `committed_activation_with_dropped_reply_is_not_retried_or_released`
- `committed_rollover_with_dropped_ack_does_not_reopen_raw_gate`
- `pending_exit_rollover_closes_old_day_without_activating_new_day`
- `failed_manifest_shutdown_aborts_a_blocked_owner_before_a_late_mutation`
- `post_spawn_failure_converges_all_owned_runtime_resources`
- `hourly_partition_rolls_at_utc_hour_boundary_and_supports_restart_parts`

## 4. Polymarket current/next/pending/terminal/restart

当前唯一运行时状态 owner 是 `PolymarketProcessingContext`：

| 状态/责任 | current symbol |
|---|---|
| current / next discovery与固定 identity | `discover_initial_polymarket_context`、`install_polymarket_context*`、`switch_polymarket_context` |
| T 时 promotion与 E 时进入 pending | `advance_polymarket_boundaries` |
| pending identity 与六个绝对 slot | `PendingPolymarketResolution`；E+3、E+4、E+5、E+6、E+7、E+8 |
| WS/Gamma terminal-once | `PolymarketTerminalClaim` 与单一 processing actor |
| Gamma attempt transport/integrity | `spawn_due_gamma_resolution_polls`、`record_gamma_resolution_completion` |
| fence/drain/role exit | `arbitrate_polymarket_*`、`quiesce_polymarket_resolution_actor` |
| restart事实恢复 | `polymarket_resolution_recovery::restore_polymarket_resolution_recovery` |

current restart 只消费 finalized、manifest-verified 的 metadata v3 与现行 v2 raw
事实，并按既有矩阵 fail closed；running/corrupt/unknown、source-only、重复终态或
矛盾 identity 不恢复 pending。阶段 3 的 source v3 + metadata v4 claimant 合同会
替代这条 current 恢复边，但阶段 2 不预先修改它。

当前PM v2的`market_resolved`仍包含raw-first路径。已批准的v3
validate-first、source claimant coordinate和metadata v4都是阶段3 future delta，
不能倒写成阶段2当前行为。

当前 terminal 语义保持：

- WS 或 Gamma 只允许一个 claimant 和一个 lifecycle/role transition；
- WS-first 后的合格 Gamma completion只可成为 post-terminal observation；
- Gamma-first accepted drain中的合格 late WS不成为第二 owner；
- exhausted 只在六个 slot闭合且没有 terminal时生成；
- restart只恢复仍有未来 slot 的已验证 pending，不补做过去 slot，也没有第七次
  probe。

## 5. paper/live binary 与 handoff 边界

training、paper、live 是三个独立 binary：

| binary | current runtime |
|---|---|
| `data-collect-training-collector` | 完整 training cold collector |
| `data-collect-paper-hot` | `run_hot_collector(PaperHot)` |
| `data-collect-live-hot` | `run_hot_collector(LiveHot)` |

两个 hot binary 当前都只调用 `run_heartbeat_handoff_server`，产生
`SourceStatus` heartbeat；它们没有接入 training `live_runtime` 的四个 producer，
也没有实现真实 external-market-event adapter。`HandoffFrame` 当前版本为
`data_collect_handoff_v1`，其 external-event 路径仍含 nullable `raw_json`。

因此阶段 2/3：

- 不修改 paper/live binary、role、startup receipt或 handoff frame；
- 不把 future external-event adapter写成已实现；
- 不宣称 paper/live runtime parity；
- 只在后继 `NONBLOCKING_FUTURE_SPEC` 记录静态字段映射。

`run_id` 只属于 hot startup receipt/部署辅助路径，不是 training source owner、
artifact owner或阶段 2 拆分前置。本阶段不新增 `run_id` 校验，也不以三个 binary
分服务器部署这一事实扩大 SDK070 的实现范围。

## 6. 测试名称与 symbol 基线

### 6.1 自动测试集合

对 `data-collect/src` 与 `data-collect/tests` 的 Rust 文件按路径排序，再按文件内
source order提取 `#[test]` / `#[tokio::test(...)]` 后的函数名：

```text
src tests:         347
integration tests: 246
total:             593
duplicate names:     0
#[ignore]:            0
```

path + name source-order SHA-256：

```text
1f6aa451444523fb664909a5eb6c69aa568704fa5509338af77475faf018614e
```

将同一集合编码成 `path:name\n` 并按 C locale 排序后的 SHA-256：

```text
f038056715f0a130dcba90ee5168300ed5b57f962303376725ede7341dbfc221
```

排序后的 name-only SHA-256：

```text
1b25ed8f6e51e6a6696338e890c6e3a5ca84129abd6f140ca1ce9a03b2eea812
```

提取命令：

```bash
perl -0777 -ne '
  while (/[#]\[(?:tokio::)?test(?:\([^\]]*\))?\]
          (?:\s*[#]\[[^\]]+\])*\s*
          (?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)/gx) {
    print "$ARGV\t$1\n";
  }
' $(rg --files data-collect/src data-collect/tests -g '*.rs' | LC_ALL=C sort)
```

`593`与上述 hash 是 before/after 对账证据，不是固定测试数量门。移动测试时保留
名称集合；只有真实新增/删除需求才允许集合变化并必须解释。

### 6.2 lexical symbol 集合

对同一 tracked 范围逐行抽取行首可见的
`fn|struct|enum|trait|type|const|static|mod`，编码成
`path:kind:name\n`并按 C locale 排序：

```text
total: 2808
fn: 2091
struct: 269
const: 253
enum: 134
mod: 41
type: 17
static: 3
path:kind:name SHA-256:
d4a2ff3cc10c480e2278ec670f4153c7ff3583e7f7c7575ee515b16b4b853706
kind:name SHA-256:
e2c6ad75c153040241bd3897338a37bb7b6c18efcded4b6d73fe3ebd86632e17
```

使用的 lexical 正则为：

```text
^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?(?:async[ \t]+)?
(fn|struct|enum|trait|type|const|static|mod)[ \t]+
([A-Za-z_][A-Za-z0-9_]*)
```

该集合不是 Rust AST 或 public API 清单，只用于发现机械迁移、意外增删和重复
owner；path hash会因计划内移动而变化，必须同时用 `kind:name` 集合和 diff解释。

### 6.3 高风险 owner 与 public 边界

只对下列项目人工维护 owner，不为全部 593 个测试建立第二台账：

| 范围 | current owner / public boundary |
|---|---|
| runtime composition/day/shutdown | `live_runtime.rs` |
| manifest durable state | `daily_manifest.rs` + `live_runtime::ManifestOwnerState` |
| writer/Parquet publish | `writer.rs` + `live_runtime` writer supervisor |
| Polymarket runtime source | `live_runtime.rs` |
| reusable Polymarket typed helpers | `sources/polymarket.rs` |
| Polymarket v2/metadata v3 layout | 两个 `polymarket_*_layout.rs` |
| restart recovery | `polymarket_resolution_recovery.rs` |
| SDK actor/runtime | `polymarket_sdk_runtime.rs` |
| paper/live handoff | `handoff.rs`、`runtime.rs` |
| crate public modules | `lib.rs` |

阶段 2 不为移动测试扩大 `lib.rs` public API；只允许模块内部
`pub(super)`/`pub(crate)` 的最小编译适配，并在每个小步自审。

## 7. 跨 owner 消息流

| message/channel | sender -> receiver | 语义 | durable effect |
|---|---|---|---|
| `InflightRawEvent` / bounded `event_tx` | 4 producer -> runtime coordinator | data或`IngressControl` | 无；writer接受并finalize后才durable |
| `PolymarketSourceControl`、`DepthSourceControl`、`AggTradeSourceControl` | day/hour wrapper -> source | fence、stop、resume、depth reset | 无；reply只证明source cutpoint |
| `WriterCommand` / per-source sync channel | coordinator -> 6 writer | `Events`、hour/day finalize、shutdown及writer report | finalized Parquet及其file facts |
| `ManifestCommand` / `ManifestRolloverReady` | coordinator/writer <-> manifest owner | activation、finalized delta、barrier、commit、shutdown；反向通知4 producer + 6 writer rendezvous已满足 | command产生atomic manifest mutation；ready不新增mutation，只授权coordinator进入commit |
| `StartupParticipant` + startup release | 4 producer/6 writer <-> coordinator | 本地初始化完成后统一release | manifest activation先于source poll |
| `RuntimeExitRequest` | exit watcher -> coordinator | operator/deadline close候选 | 无；由gate/manifest后续commit |
| `ProducerReleasePhase/Ack` | coordinator <-> 4 producer wrappers | arm、flush deferred、resume或精确stop | 无；控制新日release顺序 |
| source task completion | source task -> supervision/coordinator | reliable return、coordinated abort或unclean事实 | 无；只决定最终shutdown disposition |

任何 reply/channel closure都不能自行冒充 durable commit；manifest file和finalized
Parquet事实仍由各自唯一 owner决定。

SDK subscription actor、Polymarket fast-drain pump和Gamma resolution `JoinSet`都在
各自 source owner内部，不重复登记为跨 owner消息。

## 8. `#[cfg(test)]` seam

基线commit时 `data-collect/src` 共出现 105 个 `#[cfg(test)]` 属性：9 个普通
test-module gate与96个implementation hook。96个hook分布为
`live_runtime.rs` 54、`polymarket_resolution_recovery.rs` 29、
`polymarket_sdk_runtime.rs` 10、`inflight.rs` 2、`dedupe.rs` 1。人工核对后的
生产结构影响类别如下：

| 文件/类别 | test-only能力 | production影响 |
|---|---|---|
| `live_runtime.rs` | fake clock、recovery wait override、bounded pending limits、dropped-ack mode、test-only source/writer constructors与row helpers | production build全部裁掉；生产endpoint、timeout、queue、state transition不变 |
| `polymarket_sdk_runtime.rs` | actor apply/ack pause hooks | 只控制确定性竞态测试；production actor无hook字段 |
| `polymarket_resolution_recovery.rs` | bounded scan counters、test-only projection字段与断言helper | production scan/filter/validation路径不增加状态或输出 |
| `inflight.rs` | permit可用量与立即acquire helper | production acquire/release合同不变 |
| `dedupe.rs` | fingerprint计数观察 | production dedupe窗口不变 |
| 其余 `mod tests` | unit tests | 不进入production build |

机械移动必须让这些 seam跟随真实 owner；不得把它们提升成 public API，也不得为了
测试合并建立第二 runtime/source abstraction。

## 9. 阶段 2 owner map 与下一小步

| 分支 | owner | 当前主要 symbol | 本阶段边界 |
|---|---|---|---|
| `2-PM` step 1 | Polymarket source | source spec、SDK pump、discovery/context、single processing actor、Gamma resolution transport与source control | 只移动 source及直接helpers/tests |
| `2-PM` step 2 | adapter/row conversion | `prepare_polymarket_message`、`visit/build_polymarket_events`及v2/metadata row construction | step 1不得移动 |
| `2-PM` step 3 | writer leaf | PM current schema version value、PM rows-to-writer leaf及one-row guard | 六源共享dispatch与logical-byte accounting留root |
| `2-PM` step 4 | restart recovery | 既有 `polymarket_resolution_recovery.rs` | 先核验唯一owner；无必要不得再重构 |
| `2-RTDS` | RTDS source | classify/runtime/subscription/emit及直接测试 | 保持“业务静默不重连” |
| `2-REST` | 其余 owner | Binance、writer supervisor、manifest/day/rollover、coordinator/composition | 按真实owner逐项移动 |

`data-collect/src/sources/polymarket.rs` 已经是 reusable typed adapter/helper的
public module，不是 training runtime source owner。`2-PM` step 1不能覆盖、复制或
建立第二个同名 public source；目标应是 `live_runtime` 内部的最小 runtime
submodule。

共同基线完成后允许的下一步只有：

1. 提取 Polymarket runtime source owner及其直接持有的 Gamma
   discovery/identity/resolution transport/helpers和直接测试；
2. 只做编译所需 path/visibility适配；
3. 运行 owner定向、受影响跨模块测试及 locked all-targets check；
4. 主代理自审 behavior/schema/payload/hash/fingerprint/admission均无变化。

本小步不得提前移动 adapter、writer seam或restart recovery，不得修改 schema、
RTDS、paper/live、`HandoffFrame v1`、部署路径或 `run_id` 校验。

## 10. 共同基线自审结果

2026-07-25 主代理完成自审：

- 需求、计划、changelog与本文件路径存在且事项引用一致，`git diff --check`
  退出0；
- 两个历史artifact根存在，完整UTC日六类Parquet文件数复算为
  `24 / 1 / 24 / 24 / 24 / 24`，manifest build commit分别为
  `1d95823614ceed78288ce34cb95fbf23e58ef64d`与
  `a32226863428dcbd82b016538f36855240a41892`；
- 文内提取命令复算`593`个测试、0个重复leaf name、0个`#[ignore]`，三个测试
  hash均与第6.1节一致；测试清单harness成功列出`593`个test；
- lexical symbol复算`2808`个，分类数量及两个hash均与第6.2节一致；
- 跨owner表为8类边界，`#[cfg(test)]`总数复算为105。

未发现需求、计划与当前代码语义冲突，也未发现需要阶段2新增校验或抽象的事实。
任务2.1/2.2共同基线记为完成；下一游标为`2-PM` step 1。

## 11. `2-PM` step 1 执行证据

2026-07-25在共同基线完成后，Polymarket source owner及73个直接测试分别机械迁入
私有`live_runtime/polymarket_source.rs`与
`live_runtime/tests/polymarket_source.rs`。root保留adapter/row conversion、
writer seam、restart recovery和跨owner测试；`lib.rs`未修改。

主代理对Stage1 HEAD与迁移后代码做三组归一化对账：

| 对账范围 | before / after SHA-256 |
|---|---|
| production owner | `2c78da1d4a2c1bce1de218ed35680af0a1237692dcba3186f93dab992b07ff34` |
| 73个直接测试函数 | `0b5f550295731a59896698280913346569296fe2fbe7f18994dadc3b5cd292db` |
| root remainder | `2ee0aa51666072cc0b3ca4c1b8785f86bcc366d761ebea8a00367318ddda24e7` |

归一化只忽略空白、rustfmt可选尾逗号、编译所需`pub(super)`和唯一隔离测试模块
路径变化。全局测试名称集合与name-only hash不变；lexical symbol仅新增两个计划内
私有module symbol。owner 73/73、跨owner 8/8、五组integration 52/52、fmt、
locked all-targets check与diff check全部通过。

本小步没有改变schema、payload、hash、fingerprint、admission、RTDS、paper/live、
`run_id`或部署路径。当前游标为`2-PM_STEP_1_SELF_REVIEW_PASSED`；下一小步是
step 2 adapter/row conversion owner。

## 12. `2-PM` step 2 执行证据

38个纯event adapter/row conversion items与12个直接测试分别机械迁入私有
`live_runtime/polymarket_event_adapter.rs`及同名私有测试子模块。相邻但属于
RawDay/admission、dedupe/recovery、writer dispatch或cross-owner合同的代码与测试
保持root原位，未按连续行粗搬。

主代理对Stage1 HEAD与迁移后代码做两组归一化对账：

| 对账范围 | before / after SHA-256 |
|---|---|
| 38个production items | `0e75f87c0802b05f9cb0621fae16d1dddf9af33917697885539930169b12f393` |
| 12个直接测试函数 | `ff8b15361618359a8c557a42dca48c5f55972e3821a43b090315efe61139037e` |

全局测试仍为593且name-only hash不变；lexical symbol只新增两个
`mod polymarket_event_adapter`声明，剔除source/adapter两组私有module声明后
`kind:name`集合与基线一致。adapter 12/12、受影响unit 100/100、六组integration
50/50、fmt、locked all-targets check与diff check全部通过。

本小步没有改变schema、payload、hash、fingerprint、admission、RTDS、paper/live、
`run_id`或部署路径。当前游标为`2-PM_STEP_2_SELF_REVIEW_PASSED`；下一小步是
step 3 writer integration seam。

## 13. `2-PM` step 3 `Writer-A`裁决

Step 3实现前的两路只读审计确认：

- `LiveSegmentWriter`与`ActiveSegment`共同持有六类artifact的writer生命周期；
- `typed_schema_version`与`write_typed_live_event`各自只有一个六源match，
  PM只是其中两个arm；
- 可完整机械移动的PM-only symbol只有`require_at_most_one_typed_row`，单独移动
  不能形成文档原写法所称的PM dispatcher owner；
- 整体搬走共享函数会侵犯`2-REST` owner；新建handled协议或重复match属于额外设计。

用户批准`Writer-A`后，本表把step 3收窄为：root保留唯一共享source-kind dispatch
与统一logical-byte accounting；既有PM adapter取得current orderbook schema
version value、两个rows-to-writer leaf函数及one-row guard。只新增一个此前缺失的
fan-out fail-closed直接测试，不重复已覆盖的0/1-row正常路径。该裁决不改变任何
schema value、payload/hash、fingerprint、admission或public API。

## 14. `2-PM` step 3执行证据

`Writer-A`按批准范围完成：

- root继续唯一持有`LiveSegmentWriter`生命周期、六源schema/write match及一次
  `logical_bytes`计算；两个PM write arm只做对应leaf委派；
- PM adapter取得一个current orderbook schema value、两个rows-to-writer leaf及
  原one-row guard；metadata v3常量继续留在原layout owner；
- adapter没有source-kind match、handled协议、第二writer abstraction或新增
  public API；
- 新增一个两row未拆分事件在write前fail closed且final row count为0的直接测试。

五文件总行数由Step2的50,352变为分支收口最终50,390，净增38。Step3自审时曾为
50,391；分支Clippy门随后只删除Stage1 HEAD既有lint attribute与其函数之间因迁移
产生的空行，attribute保持不变，不改变函数体或runtime语义。自动测试集合由593变为594只因
该直接测试：src/integration为348/246，无重复name、无`#[ignore]`，source-order、
排序`path:name`、name-only SHA-256依次为
`c6067bb80a9717e7f6392a798070f27b6728d10be4b4e076a48dcdd263153766`、
`23e3805c21c385831f905799e3de3c74a2417d6c7f8cccf94bd725a55f31faaa`、
`a67ca19fb9c84e7b5b9a70bfef6cc8d869d53ea5277b4a0e146a96ee05161470`。
lexical symbol为2,816，相对Step2只增加两个leaf、一个direct test与一个constant；
`path:kind:name`和`kind:name` SHA-256为
`dbd8cad3f5b2d02ac610becb7750d3d0afc324f343f014af79f5b186c87346d9`与
`311b52bfd2da4c02616c62040354a2646316d01164362bb943a439cb5e77b1bb`。

新guard测试1/1、adapter 13/13、PM unit 101/101、共享writer统计测试1/1、六组
integration 53/53、fmt、locked all-targets check、594-test harness list与diff
check全部通过。独立审查`PASS_WITH_MINOR`，唯一复数措辞Minor已修正。schema、
payload/hash、fingerprint、admission、recovery、RTDS、paper/live、`run_id`和
部署路径未改变。当前游标为`2-PM_STEP_3_SELF_REVIEW_PASSED`；下一小步只审计
step 4既有resolution recovery owner。

## 15. `2-PM` step 4 recovery owner审计证据

Step 4只读审计结论为`ALREADY_OWNED_NO_MOVE`：

- `polymarket_resolution_recovery.rs`已唯一持有production restore入口、恢复结果
  类型、metadata/raw bounded scan、schema/payload/reference validation、
  finalized路径排序与32个同文件直接测试；
- root stable startup recovery只调用该入口并继续持有UTC/manifest稳定门，PM
  source只消费恢复结果构造pending运行态，adapter没有恢复路径；
- root lifecycle fingerprint restore只恢复`LifecycleFingerprintSets`，不是
  resolution-state shadow owner；
- 既有`lib.rs` public边界不收窄，也不因一个输出字段当前没有production source
  消费者而删除恢复合同。

recovery、`lib.rs`、Rust与测试均没有修改；测试/lexical集合继续沿用Step3的
594/2,816事实。recovery直接测试32/32、startup crossing-midnight与clock-regression
2/2通过，PM unit 101/101已覆盖restart crash-cut和recovered-pending消费者。
四个`2-PM`小步至此完成，当前游标为`2-PM_BRANCH_CLOSE_GATE`。

## 16. `2-PM`分支收口门

一次完整串行deterministic suite通过：348 unit、246 integration、0 doctest。
最终行为代码上的locked all-targets check已通过；all-targets Clippy首轮仅发现
Stage1 HEAD既有的`too_many_arguments` lint attribute与其函数之间因迁移出现空行；
只删除该空行并保持attribute，Clippy随即通过。最终fmt与diff check通过。

lint-only空白修正不改变函数体、调用图、测试/lexical集合或artifact合同，因此不
重复完整suite。最终五文件总行数为50,390；测试594、lexical symbol 2,816及第14节
hash保持不变。

独立全面审查首轮仅发现一项Minor：Stage1 HEAD既有的
`#[allow(clippy::too_many_arguments)]`被误删，且三份记录把它错误写成迁移新增。
恢复该attribute、保持Clippy要求的无空行布局、修正文档并复跑Clippy/check/fmt/
diff门后，同一独立审查最小复审以Critical/Important/Minor均为0通过。当前游标为
`2-PM_CLOSED`，以承载本状态的closure commit成功推送且本地HEAD、upstream与
远端ref一致为生效条件。

## 17. P-B1独立前置handoff

P-B1是Stage1闭合后的SDK typed/nullability前置，不属于Stage2 owner迁移。本节
不改变第1至16节的`2-PM`实现、测试、LOC、symbol、审查或closure事实。

- 最终字段裁决为F04 `book.lastTradePrice = INCLUDED_OPTIONAL`，F01–F03与
  F05–F18 `EXCLUDED`；
- P-B1只扩展既有vendor `response.rs`生产patch及必要conformance/consumer测试，
  没有把typed修复混入PM owner边界；
- `2-PM_CLOSED`继续成立；共享dispatcher、writer、resolution recovery owner和
  current source v2 + metadata v3组合保持不变；
- P-B1最终本地门及独立审查已通过
  （`PASS, Critical 0 / Important 0 / Minor 0`），closure commit
  `d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`已推送且三方ref一致，故
  `P_B1_CLOSED`与`SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`成立；
- P-B1发布后已按总计划启动独立`2-RTDS`机械分支；用户随后授予本事项持续执行/
  发布授权，阶段3等后继本地门不再逐次申请授权。

## 18. `2-RTDS`唯一owner小步与分支收口handoff

`2-RTDS`只移动当前RTDS source owner及其直接测试，不实施未批准专项草案：

- `live_runtime/rtds_source.rs`唯一持有RTDS endpoint/config/spec、classify、
  subscription evidence、source-native canonical、dedupe restore、SDK runtime
  loop、generation accounting和emit；
- `live_runtime/tests/rtds_source.rs`持有九个直接测试；
- root保留composition、共享sink/day/coordinator、writer/schema/
  `rtds_row_from_event`、跨channel config、跨四源业务静默和四源load测试；
- 生产代码与`d6a87fd`基线规范化后逐项hash一致；九个测试在移动时函数体逐字
  一致，最终文本差异仅为标准rustfmt布局。没有新增timer、watchdog、refresh、
  连接owner、行为或public API。

迁移前root为33,965行；最终root 32,589行、source 359行、tests 1,019行，
合计33,967行，净增2行module wiring。owner定向`9/9`、三个root affected test
`3/3`、locked all-targets check、完整suite
`351 unit / 246 integration / 0 doctest`、Clippy、fmt与diff门通过。首次fmt
check只发现移动后的标准布局与三处孤立空行，formatter修正后未重复完整suite。

`gpt-5.6-sol`/`ultra`独立审查以
`PASS (Critical 0 / Important 0 / Minor 0)`通过，并独立确认production/root
remainder/九个测试的规范化迁移哈希一致。用户随后授予持续执行/发布授权；closure
commit `b7f83704cf2176de61573c6b11d792df73d68b53`已推送到
`origin/fix-rtds-silents`，且本地HEAD、upstream与远端ref一致，故
`2-RTDS_CLOSED`正式成立。public smoke、vendor suite、未批准RTDS草案的
100-cycle/8小时门及4A/4B不在本分支。
