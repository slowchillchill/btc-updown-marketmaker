# SDK070 Stage5 cleanup ledger

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：
> `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：
> `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：
> `changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 状态：`STAGE5_REVIEW_PASSED_AWAITING_PUBLISH`

## 1. 审计边界

本台账处理两类已批准Stage5清理：

- Stage3本地activation后已失去current-runtime consumer的旧Polymarket private
  路径和由此产生的空plumbing（C01–C08）；
- 用户于2026-07-26裁决由current production owner替代的旧public/test-only模型
  （U01、U02、U03、U04a、U04b），并在首次终审后追加批准U05-A与U06-A。

审计根覆盖：

- `data-collect`的lib、binary、tests、benches及Cargo targets；
- public/crate-private API、serde/trait间接入口；
- repo scripts、需求、计划、changelog与legacy artifact reader；
- Stage3 S-B probe和W-B明确保留范围。

当前基线命令为：

```bash
cargo check -p data-collect --locked --all-targets
```

结果为成功退出并报告37项dead-code warning。Stage5不通过`allow`、下划线参数、
新增test seam或抽象层隐藏这些warning。

C01–C08删除后，同一`--all-targets`命令成功退出且不再报告warning；这不是把
warning改名或抑制，而是删除下文登记的旧private owner及空plumbing。

## 2. DELETE候选

| ID | 候选与当前消费者 | 终态 | 删除证明与最小验证 |
|---|---|---|---|
| C01 | `polymarket_event_adapter.rs`旧source-v2 canonical JSON、逐change fan-out、raw row转换及v2 live-writer leaf；只有同模块旧测试和C02旧恢复测试消费 | `DELETE_PROVEN_REPLACED_BEHAVIOR` | current dispatch只接收一次target-admitted v3 projection；由v3 admission/codec、writer和source tests验证，不删除public offline v2 row/schema/writer |
| C02 | `polymarket_message_*dedupe`、`restore_polymarket_dedupe*`、`wait_for_polymarket_dedupe_restore`及四项旧restore/replay测试；无production caller | `DELETE_PROVEN_REPLACED_BEHAVIOR` | 用户已决定不做collector跨进程payload-equality drop；v3平台重复取不同source key，internal retry复用key；验证duplicate/retry与source sequence合同 |
| C03 | Polymarket source中的`LifecycleFingerprintSets`初始化、replace-only state、跨函数参数及`startup_restored_dedupe_lifecycles`；activation后不再读取payload或作drop决定 | `DELETE_PROVEN_REPLACED_BEHAVIOR` | 删除空state/参数传递，不改变source FIFO、target admission、resolution或Gamma；受影响source tests移除无语义的dedupe setup后保持原断言 |
| C04 | `PendingPolymarketResolution.ws_fingerprints`、`observe_ws_payload`与limit常量；method无caller，field只影响无意义owned-byte估算 | `DELETE_PROVEN_UNREACHABLE_PRIVATE` | 全targets/public API/scripts无consumer；删除field、constructor初始化和owned-byte分支后compile/check |
| C05 | `POLYMARKET_PROCESSOR_STAGING_MAX_BYTES`；定义后无引用 | `DELETE_PROVEN_UNREACHABLE_PRIVATE` | 全targets只有定义；直接删除并compile/check |
| C06 | `RecoveryPair::LegacyV2MetadataV3`、`restore_legacy_polymarket_resolution_recovery`、旧raw projection/parser/validator和仅保护该test-only recovery的tests | `DELETE_PROVEN_REPLACED_BEHAVIOR` | production recovery固定source-v3/metadata-v4；保留并测试source-v2/metadata-v3 cross-pair检测及legacy row-grain audit reader，不把v2重建为v3 |
| C07 | internal `MetadataRecord.schema_version`字段；只赋值、不参与current恢复判断 | `DELETE_PROVEN_UNREACHABLE_PRIVATE` | metadata layout/footer及exact schema已在构造record前验证；删除字段与旧test-only mutation后current recovery矩阵通过 |
| C08 | `write_polymarket_orderbook_rows_with_logical_bytes`与`write_polymarket_market_metadata_v3_rows_with_logical_bytes`；只有C01旧adapter和writer内部旧测试消费 | `DELETE_PROVEN_UNREACHABLE_PRIVATE` | W-B public offline `write_polymarket_orderbook_rows`/`write_polymarket_market_metadata_v3_rows`保持不变；旧测试改用公开入口或由现有public writer contract覆盖 |

## 3. KEEP边界

| ID | 保留项 | 理由 |
|---|---|---|
| K01 | public offline source-v2/metadata-v3 writer、v2 schema与row-grain reader | W-B明确用于历史fixture、兼容测试和bounded A/B；不得因无current-runtime caller删除 |
| K02 | `polymarket_storage_ab_probe`及Stage3报告 | Stage6按hash消费且明确`NO_RERUN / NO_REIMPLEMENTATION / NO_SECOND_REVIEW` |
| K03 | current source-v3/metadata-v4 adapter、writer、claimant与recovery；legacy layout cross-pair拒绝扫描 | Stage3正式authority与fail-closed门 |
| K04 | Binance/RTDS dedupe、`structural_dedupe_dropped_events`通用计数及其manifest字段 | 仍有独立真实consumer；Polymarket删除不得扩大到其他source |
| K05 | current行为测试所需的test-only便捷constructor | 只把确属测试支持的入口收窄到`cfg(test)`；不删除其保护的current合同 |

## 4. RESOLVED USER_DECISION

初次建议曾把U01/U02/U03/U04b列为`DEFER`、U04a列为`KEEP`。三名独立
`gpt-5.6-sol`/`ultra`审查者与root重新核对真实调用图、需求语义和current owner后，
确认这些结论错误地把“public + 自引用测试”当成保留理由。用户于2026-07-26批准四项
authority裁决，覆盖前五个终态条目；首次终审发现另外两套shadow authority后，用户
继续批准U05-A与U06-A：

| ID | 删除候选 | 终态 | 唯一替代owner与边界 |
|---|---|---|---|
| U01 | `sources/rtds.rs`及`rtds_contract` | `DELETE_PROVEN_REPLACED_BEHAVIOR` | current RTDS owner为`live_runtime/rtds_source.rs`与SDK runtime；删除shadow adapter不改变Stage4参数或专项行为 |
| U02 | `config.rs::{CrossTaskQueueConfig, validate_cross_task_queue_configs}`及静态自测 | `DELETE_PROVEN_REPLACED_BEHAVIOR` | 有限channel capacity、全局inflight row/contract-owned-byte预算、单行上限和有界acquire/send wait共同限制真实ownership；batch/periodic flush沿用writer owner，queue duration、writer lag、flush latency与RSS只作观测 |
| U03 | `scheduler.rs`、`source_scheduler_contract`及旧synthetic e2e | `DELETE_PROVEN_REPLACED_BEHAVIOR` | current runtime按四logical producer启动六source；只有Polymarket拥有current+prefetched lifecycle/T-30，Binance与RTDS连续独立运行 |
| U04a | `segment.rs` availability/causal/paper-live policy及`causal_time_contract` | `DELETE_PROVEN_REPLACED_BEHAVIOR` | legacy/无独立admission schema使用collector recv time；批准保存独立admission字段的current schema使用admission time/sequence；paper/live使用handoff admission与本机runtime receive |
| U04b | `segment.rs` naming/rollover policy及`segment_rollover_contract` | `DELETE_PROVEN_REPLACED_BEHAVIOR` | `HourlyPartitionPolicy`与hourly writer是文件owner；正常运行只按UTC hour rollover，协调shutdown可finalize当前小时，同小时启动/重启路径碰撞选择`part=N`，50,000 rows/16MiB只切row group |
| U05 | `BackpressureConfig/Monitor`、逐queue age/lag阈值、`PauseIntake/ResumeIntake`及专属test/bench控制路径 | `DELETE_PROVEN_REPLACED_BEHAVIOR` | 真实控制owner为bounded channel、global inflight row/owned-byte、单行上限和有界acquire/send wait；只保留`QueueSample`纯观测DTO与`BackpressureReceipt`兼容字段，不由DTO产生控制决策 |
| U06 | `health.rs`有限retry trace/circuit registry/`ReconnectGate`及三个专属合同 | `DELETE_PROVEN_REPLACED_BEHAVIOR` | production只需要确定性capped+jitter sleep数学，已收窄到现有私有`live_runtime::retry_delay`；真实`SourceRecoverySupervisor`保持有界单次等待、无限外层总attempt，business freshness不成为transport恢复门 |

W-B public legacy writer/reader不是本次U裁决对象，继续`KEEP`。Stage5不会改变production
cutover、历史artifact、paper/live、部署、远端状态或数据。

## 5. 执行顺序

1. C01/C08：删除旧runtime adapter小簇，保持public legacy writer；
2. C02/C03/C04/C05：删除payload fingerprint restore及空source plumbing；
3. C06/C07：删除test-only legacy raw recovery，保留cross-pair fail-closed reader；
4. U01–U04b：按已批准真实owner原子删除四个旧module、四个专属自引用integration
   tests、一个旧synthetic e2e和静态queue config自测；
5. 首次终审后补删I1/I2遗漏，并按U05-A/U06-A删除两套shadow control model；
6. 运行替代owner targeted，最终只运行一次完整deterministic
   suite、check/clippy/fmt/diff门；
7. 更新本台账为实际终态、LOC和验证证据，再做重复独立Stage5审查。

## 6. 实际删除与当前验证

C01–C08均已按上表终态执行。下表是private cleanup完成、进入clippy修正前的
删除checkpoint：

| 小簇 | 实际LOC | 结果 |
|---|---:|---|
| C01 adapter与其模块测试 | `+3 / -1,528` | 旧v2/raw/fan-out/current writer leaf及10项旧测试删除；target-admission与v3/v4测试保留 |
| C02–C05 runtime/source/source tests | `+16 / -985` | payload fingerprint restore/drop、空lifecycle参数链、无caller pending fingerprint与staging常量删除；Binance/RTDS dedupe及source状态测试保留 |
| C06–C07 recovery | `+164 / -1,126` | 旧raw recovery及test-only pair删除；current v3/v4 typed recovery、metadata真实schema校验和v2 cross-pair拒绝保留 |
| C08 writer | `+0 / -93` | 两个crate-private logical-bytes入口及其孤立test/helper删除；W-B public offline writer保留 |
| **合计** | **`+183 / -3,732`，净删除`3,549`行** | 只统计两类`DELETE_PROVEN_*`及为保留current合同而迁移的最小测试行 |

随后all-target clippy暴露两个大enum variant以及机械lint；按最小范围把
`DeferredIngressBatch::PolymarketOrderbookV3`与两类terminal claim的较大pointee
放入`Box`，删除始终为`MARKET_WS_ENDPOINT`的重复参数，并修正六处测试/bench
机械lint。首次独立预算审查同时发现`PendingPolymarketResolution::owned_bytes`
把inline identity fixed size重复计入、遗漏buffered `Vec` backing，并用JSON
长度代理String capacity。修复后按7个metadata `String`、11个
`Option<String>`、ticket、claimant epoch、两类Box pointee、buffered `Vec`和
recovery `VecDeque`逐项精确计量；focused spare-capacity回归通过，重复独立审查为
Critical/Important/Minor均0。

包含上述clippy与正确性修复后的U簇执行前tracked Rust diff为
`+541 / -3,796`，净删除`3,255`行。该总数不会把为准确预算新增的helper和测试误称
为cleanup净删除，也不会改变C01–C08的终态分类。

U01–U04b随后按用户裁决原子删除`+0 / -1,585`行：四个旧module、四个专属
自引用integration tests、一个旧synthetic e2e、对应exports，以及
`bounded_queue_contract`中的静态config自测。

首次独立阶段终审随后为`FAIL / CLOSURE_BLOCKED`，Blocker 0、Important 4、
Minor 1。已在既有C01/C02/C03批准范围内补删metadata-v2 test-only runtime
adapter、两个旧sentinel正向测试和`LifecycleFingerprintSets`自引用模型；并按
U05-A/U06-A完成下列修复：

- U05涉及的四文件当前累计diff为`+17 / -511`；该数包含U02先前在
  `bounded_queue_contract`删除的静态config自测，因此只作为受影响文件累计值，
  不冒充U05纯删除量；
- U06本轮增量为`+24 / -1,108`，删除`health.rs`、三个专属integration
  contracts及旧export/test依赖，并把生产所需delay数学收窄到现有runtime helper；
- Minor wording已纠正为：运行中只按UTC hour rollover；shutdown可finalize；
  同小时启动/重启碰撞选择初始`part=N`；只有已批准保存独立collector admission
  字段的current schema使用admission time/sequence。

上述I1/I2/U05/U06修复完成时的整个Stage5 tracked Rust checkpoint为
`+584 / -7,499`，净删除`6,915`行；它不是最终终审后的LOC。

重复独立代码/consumer终审又发现1个Important：`pending_owned_bytes()`遍历
`pending.values()`时遗漏`BTreeMap`独立克隆key的`size_of::<String>() + capacity()`，
并按已用元素而非`Vec` backing capacity计算`source_authorities`。这会让严格
1MiB pending-state门在计数值略低于阈值时实际超界。最小修复改为：

- 对每个pending map entry计独立key fixed size与真实capacity，再计value；
- 对`source_authorities`先计`capacity * size_of<Record>`，再只加已初始化元素的
  动态String capacity，避免fixed size重复；
- 扩展同一个spare-capacity回归，使用独立手工枚举而非production helper计算期望。

修复并格式化后的整个Stage5 tracked Rust diff为`+641 / -7,509`，净删除
`6,868`行。该总数同时包含C簇/U簇删除、两轮精确owned-byte正确性修复、必要current
测试迁移与机械clippy修正，不把总净值冒充纯删除量。

all-target clippy还把S-B probe中的四处等价奇偶判断改为`is_multiple_of(2)`，使
probe源码hash从Stage3 closure时的`369ee423...`漂移。按计划6.2.1，这一漂移作为
新diff处理并唯一重跑bounded probe；当前probe SHA-256为
`70c5f2dae2085d5c3584a49eb1311b2e54b81bcec08e52372dad019bd4e51dd4`，
更新后报告SHA-256为
`527c10d79d95803640980d94aacccaf79605dd565a3ce32526820b3c08380ed3`。
fixture digest、两侧rows/finalized bytes/finalized SHA、column chunks和全部硬门
与Stage3首次运行逐项相同，只有非门槛性能报告项按本次实测更新；没有重扫完整UTC日。

U簇执行前，C01–C08与精确预算修复已通过：

```text
cargo check -p data-collect --locked --lib
cargo check -p data-collect --locked --all-targets
cargo test -p data-collect --locked --lib live_runtime::tests::polymarket_event_adapter -- --test-threads=1
  10 passed
cargo test -p data-collect --locked --lib live_runtime::tests::polymarket_source -- --test-threads=1
  89 passed
cargo test -p data-collect --locked --lib polymarket_resolution_recovery::tests -- --test-threads=1
  31 passed
cargo test -p data-collect --locked --lib polymarket_lifecycle_identity_contradictions -- --test-threads=1
  1 passed
cargo test -p data-collect --locked --lib writer::tests -- --test-threads=1
  1 passed
cargo test -p data-collect --locked -- --test-threads=1
  package suite exit 0; lib 351 passed; all integration/doc targets passed
cargo bench -p data-collect --locked --bench polymarket_storage_ab_probe
  PASS; hash-drift revalidation only
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

首次终审前的U01–U04b门：

```text
7个替代owner integration targets
  bounded_queue_contract 4 passed
  market_discovery_probe_contract 7 passed
  polymarket_sdk_runtime_contract 19 passed
  source_hourly_layout_contract 14 passed
  hourly_partition_contract 3 passed
  hot_path_handoff_contract 16 passed
  paper_live_boundary_contract 4 passed
  合计67 passed
6个关键unit合同
  Polymarket resolution timeline、same-hour former-max-bytes、
  exact global inflight budget、short queue backpressure、
  closed queue release、writer finalize timeout
  合计6 passed
cargo test -p data-collect --locked -- --test-threads=1
  package suite exit 0; lib 351 passed; all remaining integration/doc targets passed
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
active source/tests/benches/Cargo target deleted-symbol scan
  0 matches
```

首次终审修复后的定向门：

```text
bounded_queue_contract 2 passed
manifest_contract 5 passed
dedupe_contract 2 passed
binance_contract 2 passed
live_runtime::tests::polymarket_event_adapter 7 passed
polymarket_outer_recovery_advances_current_across_e_before_retry 1 passed
cargo check -p data-collect --locked --bench throughput_probe
  exit 0
```

首次终审修复后的完整门：

```text
cargo test -p data-collect --locked -- --test-threads=1
  package suite exit 0; lib 348 passed; all remaining integration/doc targets passed
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
  all exit 0
deleted U05/U06/I1/I2 named-symbol and deleted-target scan
  0 active matches
```

重复终审的pending-budget修复后，上述完整package/check/clippy/fmt/diff门已再次
运行并保持同一结果；另有两个focused测试分别1项通过：

```text
polymarket_pending_owned_bytes_counts_each_allocation_once 1 passed
polymarket_pending_union_and_terminal_claim_stay_within_frozen_bounds 1 passed
```

旧adapter的三项正向测试被批准删除，因此lib计数从351降至348；这不是测试遗漏。
`ResourceAction::ResumeIntake`仍属于独立global hard-resource watermark owner，
offline metadata layout中的`fallback_close`仍属于W-B保留边界，二者均不是
U05/I2残留。

SDK070三件套、base authority overlay、source-availability三件套的U06-A窄
supersede、acceptance matrix与本台账已同步。文档/authority与代码/consumer两路
`gpt-5.6-sol`/Ultra终审均为`PASS / CLOSURE_UNBLOCKED`，Blocker 0 /
Important 0 / Minor 0。剩余门只有：

1. 精确staging，且继续排除三个受保护RTDS草案；
2. commit、push及本地HEAD/upstream/远端ref一致性。
