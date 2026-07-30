# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR Changelog

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文件保留完整事故时间线和用户最终验收；各历史检查点不能作为当前
> 游标或执行命令。
>
> 状态：`CLOSED_BY_USER_ACCEPTANCE_20260730`
>
> 对应需求：`demand/data-collect-20260728-lag-fixed-identity-repair-demand.md`
>
> 对应计划：`plan/data-collect-20260728-lag-fixed-identity-repair-plan.md`

## 2026-07-29：用户批准执行

- 用户批准执行经审查收窄的完整方案：先拉回 UTC28，随后修复 bounded recovery、
  paused fence/primary cause 和 prospective fixed identity，测试与独立审查后清空
  远端数据和程序目录，部署新程序并长跑。
- 用户明确否决生产级复杂替换流程。远端执行采用测试开发阶段的简单路径：停止服务后
  直接删除 `/home/ubuntu/btc-updown-marketmaker/datasets` 与
  `/home/ubuntu/btc-updown-marketmaker/data-collect`，随后重新部署。
- “修复版尚不存在”只描述当前开发状态，不是方案制定阻断条件。

## Stage 0 authority

- 当前实现工作树：
  `/home/chillchill/fix-rtds-silents`。
- 当前 HEAD `97523977859408f0a60442a7812de3c782beb706`，与
  `origin/main`、`origin/fix-rtds-silents` 一致，工作树在本事项落盘前 clean。
- `/home/chillchill/btc-updown-marketmaker` 当前 checkout 位于旧
  `after-data-plan`，HEAD `20aaabf1284003c14a784bfd93bcd8de268fcdf3`，落后当前
  implementation branch 36 个提交；本事项不从该旧 checkout 开发。
- 远端运行 commit 为 `ec92bd3999cce646e88f2c8f0f281b88d8a7f14e`，binary
  SHA256 为
  `3e04f1b46ee8890f25db26b6bb1a3ae90e894173620b19e916ce611b7d34b9c5`。

## UTC28 拉取与验收

- 使用香港 SOCKS `127.0.0.1:20810` 和固定远端路径拉取到：
  `/home/chillchill/new-datasets/2026-07-28-deploy-ec92bd3999cc/typed_raw/utc_day=2026-07-28`。
- rsync 使用 partial 续传语义，最终传输 10 个文件、82,725,865 bytes。
- 拉取完成后远端与本地逐路径 size/SHA 对比无差异。
- manifest SHA256：
  `0c08c357d3abbc901903efc83316d50edb92aadac51031903c2e66ed7f84c221`。
- manifest 记录 9 个 Parquet、82,699,349 bytes、1,345,977 rows；本地逐文件验证
  bytes、SHA、PAR1 header/footer 和 Parquet footer rows 全部通过。
- 故障事实保持为：
  - 顶层 `state=open`；
  - coverage `unclean_end_unknown`；
  - 13 个唯一 gap；
  - Polymarket market `lag_events=2145`；
  - Polymarket market `dropped_rows=45`。
- 该副本只证明完整复制故障现场，不是健康完整训练日。

## 原因审查结论

- 两次运行分别因 SDK receiver `Lagged(1364)` 与 `Lagged(781)` 直接返回；manifest
  无时间戳，不能把两个计数各自绑定到两个退出时刻。
- 第一次 UTC rollover paused fence 是高置信触发因素而非已证唯一根因。
- 第二次存在 receive 到 admission 的端到端处理迟滞，但现有证据不能定位具体环节
  或证明其促成 lag；source 返回后的 writer flush timeout 是次生错误。
- 全列、batch 64、全量 source-key `BTreeSet` 是已确认的恢复内存缺陷和启动延迟
  原因，但不冒充第二次 lag 的直接证据。
- fixed identity 失败链已确认；严格 equality 保留，修复 active current/next 首次
  identity authority 的重启恢复。

## 当时待执行（历史 Stage 0 检查点，后续已完成）

- 当时下一步为 Stage 0 commit/push；后续已完成。
- 当时下一步为 Stage 1 bounded recovery 实现；后续已完成。

## Stage 0 审查

- 主代理自审确认三件套互引、UTC28 数据事实、根因证据等级和用户直接清空决策一致。
- 首轮 `gpt-5.6-sol/ultra` 独立审查发现两项问题：
  - 第二次 lag 只能证明端到端处理迟滞，不能定位 writer/queue 或声称因果；
  - 合法业务静默和稀疏 snapshot 不能被“六源持续写入”验收误判。
- 三件套已同步修正。复审结论为 `PASS`，无剩余可执行 finding。

## Stage 0 发布

- 文档 authority commit：
  `eeaacc2d411c9eb6b4718d195d2896e5f81b176f`。
- commit 已推送到 `origin/fix-rtds-silents`，本地 HEAD 与远端 ref 一致后进入
  Stage 1。

## Stage 1 bounded recovery 实现

- 首先新增 projection/batch 和 sequence cursor 失败测试；未实现时编译按预期因
  `RECOVERY_BATCH_MAX_ROWS`、`TYPED_V3_RESOLUTION_PROJECTION` 与
  `validate_recovery_source_sequence` 缺失而退出 101。
- 新增固定 `RECOVERY_BATCH_MAX_ROWS=16`；本模块五个 recovery reader 全部由 batch
  64 改为该常量。
- v3 source recovery 只投影 13 列：
  `timestamp_received/event_type/market/slug/schema_version/source_sequence_epoch_id/`
  `collector_admission_sequence/collector_admission_time_utc_ms/clob_market_id/asset_ids/`
  `outcomes/winning_asset_id/winning_outcome`。
- 删除全量 `source_keys`；只保留每个 epoch 的最后 sequence。epoch 只在首次出现时
  分配 String，sequence gap 合法，zero/duplicate/regression fail closed。
- 普通 `book/price_change/tick_size_change/last_trade_price/best_bid_ask/new_market`
  只验证 projected envelope 和 discriminator 后跳过，不再构造完整 canonical
  message；仅 `market_resolved` 读取 identity/winner 列并沿用既有 claimant 校验。
- 保留完整 Parquet schema/footer preflight、metadata terminal/summary、claimant、
  winner、source-only、late row 和 cross-pair 矩阵；未新增 manifest authority、
  sidecar、migration 或 gapless sequence。

### Stage 1 测试与资源证据

- `cargo test -p data-collect polymarket_resolution_recovery --lib`：
  39 passed、0 failed、322 filtered。
- 新测试覆盖：
  - projection 恰好 13 列且不含 bids/asks/price_changes/question/description/event_message；
  - `[1,3]` sequence gap 接受；
  - empty epoch、sequence 0、duplicate/regression 拒绝；
  - 错 schema、非正 receive/admission 及 receive-after-admission 拒绝。
- `cargo check -p data-collect --all-targets --locked`：通过。
- 使用临时 probe 和只读 symlink 对本地完整 UTC27 执行真实 recovery scan；
  probe 运行后已删除，原数据未修改：
  - 输入 Polymarket v3 约 7,558,641 rows；
  - debug probe 成功，`pending=1`；
  - elapsed `32.13s`；
  - maximum RSS `194,296 KiB`；
  - exit status 0。
- 该 RSS 低于远端约 1.9 GiB 内存包络；它是本地 debug/热缓存只读测量，不冒充远端
  生产时间或长期 RSS。

## Stage 1 审查

- 主代理自审确认改动只收窄 recovery scan 的列、batch 和常驻状态；没有改变
  claimant/terminal/source-only/cross-pair 语义，也没有新增第二套恢复 authority。
- `gpt-5.6-sol/ultra` 独立审查结论为 `PASS`，无需要修改的 finding。
- 独立审查再次运行真实 UTC27 probe，得到 `31.84s / 194,856 KiB / exit 0`；
  与上述本地 debug/热缓存测量同量级，且没有改变其证据边界。

## Stage 1 发布

- bounded recovery commit：
  `b00ec45919b373886251a23cc950517ca3e30675`。
- commit 已推送到 `origin/fix-rtds-silents`，本地 HEAD 与远端 ref 一致后进入
  Stage 2。

## Stage 2 模块 authority 审计

- 用户发现 `data-collect/src/live_runtime.rs` 与 `data-collect/src/live_runtime/`
  同时存在，要求先核实是否为重构后未删除的旧、新两套代码，以及本轮是否误改旧路径。
  Stage 2 随即暂停写代码并执行只读审计。
- 审计确认两者是 Rust 的单一父模块与私有子模块布局：
  - `lib.rs` 只声明一次 `pub mod live_runtime`；
  - root `live_runtime.rs` 声明并组合 28 个 production owner；
  - 不存在第二个 `live_runtime/mod.rs`；
  - Cargo dep-info 同时列出 root 与 28 个子模块；
  - 唯一生产调用链仍为 training binary -> `runtime::run_training_collector` ->
    `live_runtime::run_live_training_runtime`。
- 权威拆分 commit `e9c91c0a19a6d6a9f13105955f9c47480cba3b34` 在同一提交中把
  root 从 31,904 行降至 4,966 行并新增 owner 文件；当前 root 与子模块 production
  顶层符号精确重名为 0。当前 Stage 2 diff 命中的 root composition、
  `polymarket_source`、`runtime_rollover_driver`、`writer_supervisor` 及对应测试都是
  当前编译活路径，不是旧副本。
- 审计同时发现本轮未提交补丁有三项真实 owner 问题：root 手写 finished handle
  查找绕过既有 source-supervision owner；新增 source error formatter 与既有
  assessment 局部重复；新增 source close-cause match 重复枚举 RawDay 的五种 cause。
- 已最小修正：
  - `finished_source_task_index` 归 `source_task_supervision` 并由其 owner 测试覆盖；
  - root 直接复用 `assess_source_shutdown`；
  - `RuntimeCloseCause::source_task_kind` 归 `raw_day`，由 raw-day、root 和 rollover
    共同复用；
  - 跨 source/writer/rollover 的 primary/secondary 收敛 helper 保留 root
    composition，没有再建新模块。
- 修正后独立复核结论 `PASS`；all-targets check 零 warning，source supervision
  8/8 和 raw-day 10/10 通过。无需也不得删除 `live_runtime.rs` 或
  `live_runtime/`。

## Stage 2 paused fence 与 primary cause 实现

- `PolymarketDrainRuntime` 仍只有一个 SDK receiver、一个 fast-drain pump 和一个
  FIFO。processor 收到 paused Fence 后，pump 在等待 resume 期间继续调用同一
  receiver 并将后续消息排入同一 FIFO。
- decoded row/byte budget 保持 `25,000 / 67,108,864` 不变；物理 channel 只增加
  `POLYMARKET_DRAIN_CONTROL_SLOTS=2`，分别容纳 Fence 与 Terminal，不扩大业务数据
  admission。
- fatal SDK terminal、capacity 或 admission failure 在 Terminal 入 FIFO 前先调用
  既有 `RawDayGate` first-close authority。Connection/Subscription/
  TerminalDisconnected/Timeout 仍走原 transport recovery，不制造 false close。
- paused fatal 路径保持 `Fence -> fence 后已接纳消息 -> Terminal`。如果外层收到的
  `StopWithClose` 与 producer、RawDay 已准备 identity 和首次 source cause 精确一致，
  则使用既有 resume sender 释放同一 pump，并直接 await 原 source future；最终返回
  精确 SDK 错误，而不是丢失 future 或把 `CoordinatedSourceStop` 冒充 primary。
- source resume 后再次执行既有 accepted/deferred flush，收敛 Fence 后才取得的
  raw-day ticket；若 source 已有精确错误，后续 flush failure 只记 gap，不覆盖
  source primary。
- subscription change drain 将 FIFO Terminal 与其前面的已接纳 message 分开处理。
  同批或晚一批到达都先处理 Terminal 前的 message，并沿用既有
  retryable-to-fatal primary replacement 规则；sink 已不可用时，剩余已接纳 message
  按 UTC 日明确记录 `polymarket_accepted_decoded_messages_discarded`，不静默丢弃。
- source handle 完成后，same-day root 直接无阻塞 join 既有
  `SourceTaskReport/SourceTaskTermination`；rollover 继续由既有 pending-exit driver
  join，以保留 synthetic producer fence。
- `RawDay` first close cause 仍是唯一 primary authority。同一 source 的精确
  termination report 可把 coarse failure 单向提升为 primary；后续
  writer/finalize/shutdown 错误只按
  `; shutdown_secondary:<error>` 追加，不能覆盖 primary。没有新增 cause registry、
  latch、sidecar、receipt、manifest 字段或第二 shutdown 状态机。
- pending-exit driver 在拿到 writer outcome 时只记录一次非空 errors，root 消费缓存
  outcome 时只提取 outputs/terminal/close-proven，避免重复 gap 和 secondary；
  `all_terminal=true/all_close_proven=true` 但 errors 非空的组合也不会漏报。
- 上述 late writer failure 在 manifest finalize 调用前写入进程内
  `SourceGapStore` 和 `runtime_failure`。当前 old-day manifest finalize API 不接收这组
  late gaps，因此不能声称该事实已持久化进旧日 daily manifest；本阶段没有为此扩展
  schema 或新增 sidecar。

### Stage 2 测试与本地门

- pre-fix paused regression 先失败：旧 channel capacity 为 25,000 而不是要求的
  25,002，Fence 后 pump 不继续排空；capacity terminal 测试也确认 RawDay close
  尚未在 FIFO delivery 前准备。
- 新 paused 测试在 processor 未 resume 时发送 2,048 条消息，超过 SDK
  broadcast capacity 1,024；全部按 timestamp `2..2049` FIFO 到达，无 Lagged。
- fatal parser terminal 测试确认 RawDay close 先可见，随后同一 FIFO 交付已接纳
  message 和 Terminal；drop resume 后 pump 收敛，global/decoded budget 恢复基线。
- 完整组合测试走真实 control handler、observer、`SourceTaskHandle`、外层 Fence 和
  RawDay `StopWithClose`：`Lagged(1364)` 仍为最终错误，只产生一个 DayBarrier，
  Fence 后 row 写入，deferred 归零，预算回基线。
- subscription drain 回归测试覆盖 `Message A -> Message B -> Lagged Terminal`
  同批与跨批两种调度，并故意令先入队 A 的 wall-clock receive time 比后入队 B
  晚 1ms：两者都按真实 FIFO 保持 A 的 fatal processing error 为 primary，B 有
  精确 receive time 的 `messages=1` 弃置证据，Lagged gap 保留，pump 与预算完整
  收敛。subscription drain 不再按可回退 wall clock 重排；UTC day boundary 的既有
  专用时间排序保持不变。
- post-activation 测试走真实生产顺序：
  `Armed -> manifest activation committed -> FlushDeferred -> Flushed ->
  next-generation SameDay Stop -> resume -> exact source join`，覆盖 flush 后的第二个
  Stop 分支，而不是 Held 状态的早期 Stop 分支。
- 两次真实计数 fixture `Lagged(1364)`、`Lagged(781)` 均产生：
  `source_task_error:polymarket_market:receiver lagged by <N> messages`；真实
  writer-finalize helper 只能追加 secondary，二次 shutdown assessment 保持完整
  primary/secondary 字符串不变。
- writer matrix 另覆盖
  `all_terminal=true/all_close_proven=true/errors!=empty`，重复调用记录 helper 后仍
  只有一个 exact secondary 和一个 gap，两个完成布尔值不变。
- 定向测试：
  - Polymarket owner：97 passed；
  - producer day barrier：6 passed；
  - source supervision：8 passed；
  - writer supervision：32 passed；
  - rollover driver：5 passed；
  - runtime shutdown：4 passed；
  - raw-day：10 passed。
- 完整 `cargo test -p data-collect --locked -- --test-threads=1`：
  - lib 367 passed、1 ignored（显式八小时 public RTDS probe）；
  - integration 231 passed；
  - 三个 bin target 与 doctest 均退出 0。
- `cargo check -p data-collect --locked --all-targets`、`cargo clippy -p data-collect
  --locked --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和
  `git diff --check` 全部通过。

## Stage 2 审查修复

- 首轮 `gpt-5.6-sol/ultra` 独立阶段审查结论为 `CHANGES_REQUIRED`：
  - paused fatal Terminal 对应的外层 `StopWithClose` 未 resume/await 原 source，
    可能丢失 future 和精确 SDK error；
  - retryable SDK terminal 测试绕过了真实 close helper；
  - pending-exit writer outcome 在 driver 与 root 被重复记录。
- 修复上述三项后，两轮聚焦只读复核又发现并推动补齐：
  - subscription change 同批已有 message 与 Terminal 时，旧逻辑跳过 message；
  - A 处理失败后同批 B 可能静默丢弃，且 primary 会随 Terminal 是否同批而漂移；
  - post-activation exact-source-join 测试只命中 Held 的早期 Stop，未覆盖
    `FlushDeferred/Flushed` 后的生产 Stop 分支。
- 最终复审又发现一项 Important：subscription drain 复用了 day-boundary 的
  wall-clock receive-time 排序。系统时钟回拨时，同批 A/B 可能被反转，而跨批不反转，
  仍会令 primary 与弃置证据随 batching 漂移。最小修复是 subscription drain 直接保留
  收集 FIFO，并用 `A.recv=t+1 / B.recv=t` 回归同时覆盖同批和跨批。
- 这些问题均已按上述最小路径修复。Polymarket/FIFO 与 writer 聚焦复核当前均为
  `PASS`，并明确确认没有第二 receiver、pump、close authority、registry 或状态机，
  root 的 append/promote 范围符合 RawDay first-close primary 合同。
- clock-regression 修复后的最终 `gpt-5.6-sol/ultra` 独立阶段复审结论为 `PASS`，
  无剩余可执行 finding。复审确认 subscription drain 保留收集 FIFO、day-boundary
  排序独立不变、所有正式测试/静态门通过，且未新增 shadow receiver、registry、
  sidecar 或第二状态机。
- Stage 2 已满足实现、验证和独立审查完成门；本 changelog 与代码在同一阶段提交中
  commit/push 后进入 Stage 3。

## Stage 3 prospective fixed identity 实现

- test-first 的 clean-restart scan 测试先因
  `PolymarketRestartRecovery` 不存在 `fixed_identities` 字段而编译失败，确认旧实现
  只返回 pending，无法保留仍可能作为 current/next 的首次 fixed identity authority。
- `restore_polymarket_resolution_recovery` 复用同一次 finalized metadata bounded
  scan 已构造的 identity map，只返回两个有界 `Option`：
  `RecoveredPolymarketFixedIdentities::{current,next}`。没有第二次 scan、第二个
  persisted map、artifact、receipt 或 runtime state 恢复路径。
- current/next 候选同时按 canonical slug 与 start/end window 选择；canonical slug
  与 window 任一侧竞争、同一 window 多 authority、非法首次 verified time，以及
  同 slug 只有首次 verified time 不同，均继续 fail closed。恢复的 next 可在时间推进
  后按 slug 成为 fresh current，不读取或恢复旧 `runtime_role`。
- fresh Gamma 仍生成本次完整 discovery 和新的
  `metadata_validation_completed_at_utc_ms`。初始 current、稍后 next attach 和
  rollover discovery 只有在 slug、condition、CLOB、ordered Up/Down assets、
  start/end 全部精确相等时，才把已恢复的首次
  `fixed_identity_verified_at_utc_ms` 写回 fresh market；候选成功后即从两个
  `Option` 中取走，但跨 fence 安装返回 `Expired` 时保留候选供 next-to-current
  重试。
- fresh Gamma 的 multiple candidate、slug/condition/CLOB/assets/window 等
  immutable selection 冲突，只有在对应 recovered authority 仍存在时才添加既有
  `operator_intervention_required:` sentinel 并映射退出码 76。无 recovered
  authority，或 `Inactive` 等动态平台状态，保持既有错误分类；本阶段没有把全部
  Gamma integrity 错误扩大成退出 76，也没有增加新的动态状态校验。

## Stage 3 测试与旧数据边界

- recovery 正向覆盖 current/next 两个候选、next 随时间成为 current；负向覆盖
  competing canonical window authority、同结构不同首次 verified time、首次时间
  `0`/`>=end`。
- runtime 正向按真实顺序分两次应用 current 与稍后 attach 的 next，确认只继承首次
  fixed time，并保留两次 fresh metadata validation time；表驱动负向逐项覆盖
  condition、CLOB、Up、Down、ordered asset swap、start、end，全部映射退出码 76。
- 真实 initial Gamma loopback 分别返回 wrong slug、wrong window 和两个 exact-slug
  candidate，三条生产 discovery 路径都映射退出码 76；动态 `Inactive` 与没有
  recovered authority 的相同分类保持退出码 1，冻结不过度分类边界。
- 使用临时只读 symlink 和运行后已删除的临时 probe，对本地完整 UTC28 副本在
  `2026-07-28T00:37:56Z` 执行真实 recovery；原数据未修改。结果仍按预期拒绝：
  `Polymarket restart fixed identity conflict:slug=btc-updown-5m-1785196800`。因此旧
  UTC28 root 没有被自动迁移、猜测修复或变成可部署 recovery authority。
- 定向测试：
  - `polymarket_resolution_recovery::tests`：44 passed；
  - `live_runtime::tests::polymarket_source`：102 passed。
- 完整 `cargo test -p data-collect --locked --quiet -- --test-threads=1`：
  - lib 377 passed、1 ignored（显式八小时 public RTDS probe）；
  - integration 231 passed；
  - 三个 bin target 与 doctest 均退出 0。
- `cargo check -p data-collect --locked --all-targets`、
  `cargo clippy -p data-collect --locked --all-targets -- -D warnings`、
  `cargo fmt --all -- --check` 和 `git diff --check` 全部通过。
- 截至该历史检查点，Stage 3 等待自审与 `gpt-5.6-sol/ultra` 独立阶段审查；审查
  通过前不提交、不推送，也不进入远端清空或部署。该审查和后续阶段现均已完成。

## Stage 3 首轮独立审查与修复

- 首轮 `gpt-5.6-sol/ultra` 独立阶段审查结论为 `CHANGES_REQUIRED`，发现三项：
  - initial/rollover 在错误完成后用当前时钟或 recovered 候选反推 expected slug，
    跨 5 分钟边界可能把错误绑定到错误 authority；
  - next 候选在 `switch_polymarket_context` 返回 `Expired` 前已被 `take()`，可能在
    next 成为 current 时丢失首次 verified time；
  - 全局可观察的 `fixed_identity_selection:` 字符串标签及
    `NonBtcFiveMinuteMarket` 分类扩大了需求列明的 immutable exit-76 边界。
- 最小修复没有新增 scanner、map 或状态机：
  - `PolymarketDiscoveryError::FixedIdentitySelection` 以 typed 字段携带 selection
    当时冻结的 `expected_slug` 和原 detail；Display 保持原 Gamma integrity 文本，
    不向无 recovered authority 的 gap/error 注入额外 marker；
  - 只有 multiple current/next、slug、condition、ordered assets、window，以及
    next pair 中由 slug/condition/assets/window 导出的 relation conflict，才在对应
    recovered slug 存在时映射 76；`NonBtcFiveMinuteMarket`、`Inactive`、SDK decode
    和无 recovered authority 仍保持退出码 1；
  - exact match 先返回最多两个布尔 match，不立即消费候选；initial context 建立或
    next/rollover `Applied` 后才消费，`Expired` 和失败路径保留候选。
- 新回归显式证明：
  - 只有 recovered next 时，prior requested slug 的 immutable error 保持退出 1，
    next requested slug 的同类错误为 76；
  - `NonBtcFiveMinuteMarket` 与 `Inactive` 不会被过度转为 76；
  - delayed fence 跨 E 返回 `Expired` 后 recovered candidate 仍存在，fresh
    replacement 已使用首次 verified time但没有提前消费 authority。
- 修复后 `live_runtime::tests::polymarket_source` 仍为 102 passed；截至该历史检查点，
  Stage 3 等待 `gpt-5.6-sol/ultra` 聚焦复审。该复审后续已通过，原禁止提交、推送和
  远端操作的临时门不再是当前指令。

## Stage 3 最终独立复审

- 同一 `gpt-5.6-sol/ultra` 审查者对三项修复执行聚焦只读复审，最终结论为
  `PASS`，无剩余可执行 finding。
- 复审确认：
  - initial/next/rollover 全部使用 request/selection 时冻结的 typed expected slug；
  - 两个布尔 match 只决定何时消费既有 candidate，不携带 identity/time，不是第二
    authority；
  - initial context 接管后消费，next/rollover 仅 `Applied` 后消费，`Expired` 保留；
  - 可观察 marker 已删除，`NonBtcFiveMinuteMarket`、`Inactive`、SDK decode 和无
    recovered authority 均未被扩大为退出码 76；
  - Polymarket source 102 项和 `git diff --check` 复跑通过。
- Stage 3 已满足实现、全量验证和独立审查完成门；当时下一步是将本 changelog 与代码
  在同一阶段 commit/push 后进入 Stage 4。该发布及后续部署现均已完成；“尚未开始”
  只描述当时状态。

## Stage 3 发布

- prospective fixed identity 修复与本 changelog 已提交为
  `ff8f56014f4e8c83bee9a3e3089eb0d6bb06525d`
  （`fix(data-collect): preserve restart fixed identity authority`）。
- commit 已推送到 `origin/fix-rtds-silents`；提交后本地 HEAD 与 remote-tracking ref
  完全一致，工作树 clean。

## Stage 4 全量门

- 在上述 clean commit 上执行
  `cargo test --workspace --locked --quiet -- --test-threads=1`：
  - lib 377 passed、1 ignored（显式八小时 public RTDS probe）；
  - integration 231 passed；
  - 三个 bin target 与 doctest 均退出 0。
- `cargo check --workspace --locked --all-targets`、
  `cargo clippy --workspace --locked --all-targets -- -D warnings`、
  `cargo fmt --all -- --check` 和 `git diff --check` 全部通过。
- workspace 只有 `data-collect` 一个 member，因此上述 workspace 门同时覆盖当前
  crate 全量门。
- 模块 authority 再次机械核对：
  - `lib.rs` 只声明一次 `pub mod live_runtime`；
  - `live_runtime.rs` 是唯一 root，并直接声明 28 个当前 production 子模块；
  - `live_runtime/` 下不存在第二个 `mod.rs`；
  - production 调用链仍为 training binary ->
    `runtime::run_training_collector` -> `live_runtime::run_live_training_runtime`。
  因此本轮修改的 root composition、`polymarket_source` 和 recovery 均属于同一当前
  编译树，不是重构遗留的第二套旧实现。

## Stage 4 恢复资源与 clean-root 双启动

- 通过运行后已删除源码的临时 probe 和只读 symlink，对 UTC27 本地副本执行当前
  recovery：
  - `polymarket_market` 为 5 个 finalized Parquet、7,558,641 rows、
    454,478,145 bytes；
  - recovery time 为 `2026-07-28T00:02:13.160Z`；
  - 结果为 `pending=1 current=true next=false`；
  - elapsed `31.67s`，maximum RSS `194,516 KiB`，exit 0。
- 同一 current-HEAD probe 随后在 user systemd scope 中以
  `MemoryMax=1900M / MemorySwapMax=0` 复跑；结果仍为
  `pending=1 current=true next=false`，elapsed `31.80s`，maximum RSS
  `195,472 KiB`，无 OOM/timeout，exit 0。
- 该 debug/热缓存完整扫描 RSS 约为 1.9 GiB 包络的十分之一，且与 Stage 1 两次
  `194,296 / 194,856 KiB` 测量一致；原 UTC27 数据只读且未被修改。
- 使用 optimized release 和独立空 root
  `/tmp/data-collect-stage4-release-probe-kep0ypOV` 进行两次真实 source 启动，
  每次 `--max-runtime-secs 20`：
  - 两次都在默认线程栈下 exit 0，maximum RSS 分别为
    `43,332 KiB / 46,256 KiB`；
  - 第一次写出六个 source 的 6 个 finalized Parquet，第二次写出对应
    `part-1`，manifest 为 `reactivated_within_day=true`；
  - 两次累计 Polymarket market 18,357 rows、metadata 4 rows；
  - manifest `gaps=[]`，六个 source 的 `lag_events=0`、`dropped_rows=0`。
- 第一次 current/next 的首次 fixed time 分别为
  `2026-07-28T22:00:54.317Z / 2026-07-28T22:00:55.366Z`。第二次 fresh Gamma
  的 source/validation time 分别推进到约 `22:01:47.682Z` 与
  `22:01:48.539Z / 22:01:48.716Z`，但两组
  `fixed_identity_verified_at_utc` 与 slug、condition、CLOB、ordered assets、
  start/end 全部保持精确一致。该真实双启动没有 fixed-identity conflict、
  receiver lag、deadlock 或 writer backpressure。

## Stage 4 debug 启动诊断边界

- 同一代码的 unoptimized debug training binary 在默认 Tokio worker stack 下对另一
  空 root 启动约 5 至 7 秒后发生 stack overflow 并以 SIGABRT/134 退出；manifest 尚为
  `not_started`，没有 source file。
- 同一 debug binary 设置 `RUST_MIN_STACK=8MiB` 后运行 15 秒正常 exit 0，
  maximum RSS `69,316 KiB`。实际部署形态的 optimized release 不设置该变量，仍在
  默认栈连续两次真实启动成功。
- 独立 A/B 使用 `git archive` 和独立 target 构建 Stage 3 父提交
  `63fed069509960594c564e03557beaba27e1766f`；该父提交的 debug binary 同样在默认栈
  约 6.47 秒后 SIGABRT/134，当前 `ff8f560` 为约 6.61 秒后 SIGABRT/134。两 binary
  SHA 不同，且两提交差异未触碰 training binary、startup coordinator 或 source task
  supervision。GDB 将当前溢出收窄到既有 Polymarket producer 的大型 debug async
  future；因此它不是 Stage 3 fixed-identity 修改引入的回归。
- 当时不把 debug-only 现象转化为远端环境变量或额外 runtime 配置；是否属于本事项
  release 阻断项，交由 Stage 4 `gpt-5.6-sol/ultra` 最终独立审查。该审查后续已
  PASS；“远端仍未删除、上传或重启”只描述本历史检查点。

## Stage 4 最终独立审查

- `gpt-5.6-sol/ultra` 最终独立审查结论为 `PASS`，可执行 finding 为 0。
- 审查逐项确认需求 AC1 至 AC7 已有相符代码、测试与实物证据；截至该历史检查点，
  AC8、AC9 保留为 `PENDING_BY_PLAN`，分别交给 Stage 5 远端部署和 Stage 6 验收，
  不在 Stage 4 提前宣称通过。该 pending 状态后续已由 Stage 5/6 证据及
  2026-07-30 用户最终验收取代。
- 独立复跑：
  - recovery 44/44；
  - Polymarket source 102/102；
  - source supervision 8/8；
  - format 与 diff check 通过。
- 审查确认只有一个 `live_runtime` root 和一条生产调用链，不存在误改旧实现；
  fixed identity 严格 equality、paused FIFO、primary/secondary、bounded recovery
  均未建立 shadow authority 或过度校验。
- debug 默认栈溢出因父提交与当前提交均可复现而判定为非本轮回归；optimized release
  默认栈双启动及 1,900 MiB/无 swap recovery scope 均通过，因此不阻断 release，
  也不允许为远端新增 `RUST_MIN_STACK`。当时计划由 Stage 6 继续监控 release 是否
  出现 `SIGABRT` 或 `stack overflow`；后续运行检查未把它报告为本事故阻断项。
- 当时下一步是提交/push 本阶段事实，再从该 clean commit 本地构建最终 release；
  该构建、远端替换和启动后续均已完成。

## Stage 4 release

- Stage 4 审查事实提交为
  `58c6796ee652d89016556684f191e20ab239c1b2`
  （`docs(data-collect): record final repair validation`），并已推送到
  `origin/fix-rtds-silents`；构建前本地 HEAD、remote-tracking ref 一致且工作树
  clean。
- 从该 commit 本地执行：
  `DATA_COLLECT_GIT_COMMIT=58c6796ee652d89016556684f191e20ab239c1b2 cargo build --release --locked --bin data-collect-training-collector`。
- 最终部署产物：
  - path：
    `/home/chillchill/fix-rtds-silents/target/release/data-collect-training-collector`；
  - size：30,322,304 bytes；
  - SHA256：
    `80fa49a4b2261e21bb58e4466da5d67f0b6f101d986e886d7248de40d1e2e18b`；
  - ELF：x86-64 PIE、dynamic；
  - Build ID：`c830ccb52dcc099fb4d46aa99fdd0dd526a93591`；
  - 最大 GLIBC requirement：`GLIBC_2.38`；远端为 x86_64、glibc 2.39；
  - `--version`：`data-collect-training-collector 0.1.0`；
  - `--help` 与计划中的 dataset root、region、run mode、dataset byte limit 参数完全一致。
- 对这一个最终 SHA 的 release 另以全新隔离 root 运行 15 秒：默认线程栈 exit 0，
  maximum RSS `39,104 KiB`；manifest 中 build commit 与 binary SHA 精确匹配，
  六个 source 均生成 finalized file，`gaps=[]`、所有 `lag_events=0` 和
  `dropped_rows=0`。
- Stage 4 完成。当时下一步严格按用户批准的简单开发流程执行远端替换；该 UTC27/28
  副本复核、两个精确目录清空、release 上传和 unit 启动后续已完成，本段不再构成
  当前执行指令。

## Stage 5 直接清空与部署

- 破坏性操作前再次确认本地副本：
  - UTC27 manifest SHA256：
    `72ed09f2bbbe68d0b7a1d3d169a03f1f2e0f2778815664c912cd6f4154b4c192`，
    目录内 27 个文件；
  - UTC28 manifest SHA256 仍为
    `0c08c357d3abbc901903efc83316d50edb92aadac51031903c2e66ed7f84c221`，
    目录内 10 个文件。
- 远端执行前服务仍为 failed、MainPID 0、exit 76；旧 binary SHA 仍为
  `3e04f1b46ee8890f25db26b6bb1a3ae90e894173620b19e916ce611b7d34b9c5`。
  现有 user unit 的 ExecStart 精确引用两个已批准目标目录。
- 执行 `systemctl --user stop data-collect-training-collector.service` 后再次确认
  MainPID 0，随后直接删除：
  - `/home/ubuntu/btc-updown-marketmaker/datasets`；
  - `/home/ubuntu/btc-updown-marketmaker/data-collect`。
  两个目标均确认不存在；没有 staged rename、旧 root migration 或滚动替换。远端
  删除不可原地恢复，但 UTC27/UTC28 故障数据仍完整保留在本地副本。
- 保留原 user systemd unit；重新创建 `data-collect/bin` 并直接复制最终 release。
  远端文件为 `ubuntu:ubuntu`、mode 0755、30,322,304 bytes；SHA256 和 Build ID
  分别精确为：
  - `80fa49a4b2261e21bb58e4466da5d67f0b6f101d986e886d7248de40d1e2e18b`；
  - `c830ccb52dcc099fb4d46aa99fdd0dd526a93591`。
- `systemctl --user reset-failed` 后于 `2026-07-28T22:18:58Z` 启动现有 unit：
  - MainPID `1225568`；
  - `active/running`；
  - `NRestarts=0`；
  - `/proc/1225568/exe` 路径为新程序，SHA 与上方最终 release 完全一致；
  - 新 dataset root 由程序创建，manifest 的 build commit 为
    `58c6796ee652d89016556684f191e20ab239c1b2`，binary SHA 与部署值一致，
    coverage 为 active、`gaps=[]`。
- 启动约 76 秒时 PID 未变、NRestarts 仍为 0、cgroup memory current/peak 约
  35.6 MiB；Polymarket market running Parquet 从约 1.02 MiB 增长到约 1.58 MiB，
  证明当前高流量 source 正在持续写入。journal 只有 systemd started 行，没有
  receiver lag、backpressure、raw-after-close、fixed-identity conflict、stack
  overflow 或其他 warning/error。
- Stage 5 的路径、停止、删除、上传、SHA、PID 和新 root 门已满足；先完成独立阶段
  审查和 commit/push，再进入 Stage 6 长跑验证。

## Stage 5 独立审查

- `gpt-5.6-sol/ultra` 独立只读审查结论为 `PASS`，可执行 finding 为 0。
- 审查确认：
  - UTC27/UTC28 本地副本仍分别为 27/10 个文件，manifest SHA 未变化；
  - 只直接清空了用户批准的两个目录，unit 位于其外且保留原 mtime、enabled 状态；
  - `data-collect` 新树只有 `bin/` 和最终 binary，没有旧程序残留；
  - 唯一 collector PID 为 `1225568`，远端文件与 `/proc/PID/exe` 的 size、SHA、
    Build ID 均与本地最终 release 匹配；
  - 未增加 `RUST_MIN_STACK` 或其他 unit 环境配置；
  - 新 manifest lineage 与 release 匹配，coverage active、gaps 0，进程同时持有
    六个 source 的 running file descriptor；
  - Polymarket running file 在 5 秒采样中继续增长 88,149 bytes，journal 无本事故
    关键词。
- 截至该历史检查点，Stage 5 部署与初始健康子门通过；需求 AC8 整体仍为
  `PENDING_STAGE6`，六任务长期稳定、受控重启、UTC rollover 和完整 UTC 日尚未
  验收。该 pending 状态后续已由 Stage 6 证据及 2026-07-30 用户最终验收取代；本段
  不能作为当前游标。

## Stage 6 受控重启失败与安全停止

- 首次新程序从 `2026-07-28T22:18:58Z` 运行到 `22:25:06Z`。受控
  `systemctl --user restart` 前 PID `1225568` 已采集：
  - Polymarket market 189,578 rows；
  - Polymarket metadata 9 rows；
  - 六个 source 均无 lag/drop。
  旧进程打印 `coordinated_shutdown_complete` 并成功 finalize。
- 新 PID `1226197` 在约 2 秒后因
  `Polymarket metadata v5 validation failed before admission:candidate_to_active:fixed_identity_not_validation`
  退出 1。由于该错误没有 `operator_intervention_required:`，unit 的
  `Restart=on-failure` 又自动启动三个进程；它们均以同一 primary error 退出。
- 观察到 `NRestarts=3` 后立即执行 `systemctl --user stop` 冻结现场。当前远端：
  - `inactive/dead`；
  - MainPID 0；
  - ExecMainStatus 1；
  - 当前没有继续执行本次 `Restart=on-failure`；unit 仍为 `enabled`，主机或 user
    manager 重启后仍可能启动。
- 第一次 restart 的错误 candidate 在 admission 前被拒绝，没有写入错误 metadata
  row。12 个 manifest 所列 Parquet 的 SHA 与 manifest 一致，PAR1 header/footer
  完整，没有 running/tmp/partial/digesting 残留；文件本身未损坏。
- 当前 UTC28 验证根为 `state=open / coverage=ended_known /
  reactivated_within_day=true`，manifest 有 3 个 gap：
  - 唯一 Polymarket primary 为上述 `fixed_identity_not_validation`；
  - 两个 Binance depth `raw event arrived after same-day intake close` 是 primary
    触发全局关闭后的 secondary。
  所有 source 的 `lag_events=0`、`dropped_rows=0`，但 Polymarket 自受控重启后未恢复
  连续写入，六源连续 coverage 已不可成立；失败重启期间写入的少量 Binance part
  文件不能修复该缺口，因此该日不能成为健康训练日。

## `live_runtime` 模块边界复核

- `data-collect/src/lib.rs` 只声明一次 `pub mod live_runtime`；
  `runtime::run_training_collector` 调用
  `live_runtime::run_live_training_runtime`。`live_runtime.rs` 是父模块和运行时编排入口，
  `live_runtime/*.rs` 是由它逐一 `mod` 声明的子模块；不存在第二个
  `live_runtime/mod.rs` 或 `#[path = ...]` 旁路。
- 重构不是把旧实现复制后遗留。Polymarket 拆分 commit `582eb154` 从
  `live_runtime.rs` 删除 25,589 行并新增 Polymarket 子模块；后续整体拆分 commit
  `e9c91c0` 又从父模块删除 30,895 行并新增其余运行时子模块。父模块相应从 31,904
  行降到 4,966 行，当前父文件与子目录是同一模块树，不是两套并行采集程序。
- 本次修复 commit `ff8f560` 修改的
  `live_runtime.rs`、`live_runtime/polymarket_source.rs` 和
  `polymarket_resolution_recovery.rs` 均列在最终 release 的 Rust dep-info 中。部署
  build commit `58c6796` 是 `ff8f560` 的后代，远端受控重启也实际执行到该恢复路径；
  因此本次不是改到了未生效的旧代码。

## Stage 6 restart 根因与方案冲突

- finalized metadata authority 本身合法。`polymarket_market_metadata_22.parquet`
  0-based row 4 为后来成为 current 的 market
  `btc-updown-5m-1785277500` 当时处于 `prefetched/next` 的
  `metadata_snapshot`：
  - fixed identity verified：`2026-07-28T22:20:00.032Z`；
  - row validation：`2026-07-28T22:20:00.032Z`。
  row 7 才将同一 identity 从 `prefetched/next` 提升为 `active/current`，fixed
  time 保持不变。
- restart 时旧 market 已有 recovered pending，因此
  `PolymarketProcessingContext::from_recovery_with_source_epoch` 返回
  `current=None + pending!=empty`；outer loop 只以 `context.is_none()` 判断是否走
  initial discovery，故跳过普通 current snapshot 路径，进入 async late rollover。
- late rollover 的 fresh Gamma current 与 recovered immutable identity 精确匹配，
  本轮代码正确把 fixed time 保持为 `22:20:00.032Z`，同时保留本次更晚的 fresh
  validation。随后旧路径把它构造成 `candidate_to_active`。
- 既有已批准 candidate 合同要求
  `source receive <= row validation = fixed identity <= applied`；validator 因此正确
  拒绝两个不同时间。新需求又要求 restart exact match 时首次 fixed time 不变、fresh
  validation 前进。当前生产路由把 restart current 错分为普通 late candidate，令两条
  语义在该路径无法同时成立。
- 现有测试分别覆盖了 recovered fixed time 和普通 candidate equality，但遗漏
  `recovered pending + current none + recovered current + fresh validation`
  组合，短时 clean-root 双启动也未跨到 pending 状态。

### 当时等待用户决定的两个选项（历史决策点，后续已批准方案 1）

1. **推荐：保留 candidate 严格合同，修正 restart 路由。** 保持 pending-first
   subscription 与现有 async discovery；仅当 fresh `discovery.current` 精确命中
   任一未消费 recovered fixed authority（包括 recovered current，以及随时间成为
   current 的 recovered next）时，让既有
   `switch_polymarket_context(... emit_current_metadata=true)` 写正常 current
   `metadata_snapshot`，并跳过 `candidate_to_active`。snapshot 既有合同已允许
   `first fixed <= fresh validation`；普通无 recovered authority 的 late candidate
   继续要求 equality。无需 schema、marker、第二状态机或额外校验。
2. **不推荐：放宽所有 candidate。** 把 equality 改成
   `fixed <= validation`，同时保留原有
   `source receive <= validation <= applied`。这会直接修改旧批准需求并扩大所有
   late candidate 的接受面，属于用户此前担心的过度放宽。

- 推荐方案必须增加以下回归门：
  - Gamma discovery 延迟或 unavailable 时，pending-first subscription 已先建立；
  - recovered current 与 recovered-next-to-current 两个正例都输出 V5 current
    snapshot，保留 old fixed 与 fresh validation，并且不输出 candidate；
  - 无 recovered authority 的真正 late discovery 仍输出 candidate，且 equality
    validator 不放宽；
  - immutable mismatch 仍在 row admission 前 exit 76；
  - `Expired` 不写 snapshot/candidate、不提前消费 recovered authority，并保留
    pending；
  - 真实受控重启必须先形成 pending 和 finalized current/next identity，不能再用未
    形成 pending 的短时双启动代替。
- 只有上述测试、独立审查、commit/push 和新 release 完成后，才可再次部署。
- 若用户批准推荐方案，按既有简单开发决策再次直接删除并重建远端
  `/home/ubuntu/btc-updown-marketmaker/datasets` 与
  `/home/ubuntu/btc-updown-marketmaker/data-collect` 后部署；保留现有 systemd unit，
  不做 migration、staged rename 或滚动替换，也不兼容本次已有 3 gap 的验证 root。
  用户决定前保持服务停止，不修改代码，不清空现场。

## Stage 6R B2 独立审查与用户批准

- 2026-07-29，三路 `gpt-5.6-sol/ultra` 子代理分别从故障链、metadata/validator
  合同和状态/重试边界进行只读反证审查：
  - 根因定位通过：recovered pending 令启动保持 pending-first 并跳过 initial
    snapshot，随后已有 fixed authority 的 fresh current 被错误路由为普通
    `candidate_to_active`；
  - B2 语义通过：later current snapshot 已允许
    `first fixed <= fresh validation`，普通 candidate equality 不得放宽；
  - Critical finding 为 0；
  - 实施前必须补齐两个 Important 边界。
- Important 1：`RecoveredPolymarketFixedIdentityMatch.current/next` 表示 recovered
  authority 槽位，不表示 fresh discovery role。B2 必须在既有 exact-match 的同一次
  比较中产出 fresh-current 角色结果；不能使用 `matched.current || matched.next`、
  `fixed != validation` 或 slug-only 作为代理。必须增加“只有 discovery.next 命中”
  的 current-candidate 负例。
- Important 2：现有 `install_polymarket_context` 先
  `replace_discovery`，再等待 subscription 和 metadata admission。可重试安装错误
  可能留下 `current-without-authority`，令下一轮跳过 rollover discovery。B2 必须用
  最小失败恢复保持 pending/evidence，同时让 current/next 回到 pre-switch 状态或提供
  等价可靠重试；`Expired` 和任何错误都不得提前消费 authority。
- 上述两项不是新增业务校验：
  - fresh-current 结果只是同一次 immutable exact-match 的路由输出；
  - 安装失败处理只是状态一致性；
  - 不新增或放宽 V3/V4/V5 validator、schema、marker、sidecar、恢复 map 或第二
    状态机。
- 用户已明确同意并批准按上述审查修订后的 B2 执行。此前的
  `USER_DECISION_REQUIRED` 已解除；实现、测试、独立阶段审查、commit/push 和新
  release 完成前，远端 service 当时继续保持停止，验证 root 不清空。上述工作后续
  均已完成，该临时停止门不再是当前指令。

## Stage 6R B2 实现与自审

- 唯一生产编译链再次核实为
  `lib.rs -> runtime.rs -> live_runtime.rs -> live_runtime/polymarket_source.rs`；
  `live_runtime.rs` 根模块只声明并导入拆分后的子模块，不存在第二套旧
  Polymarket source owner。本次只修改该实际编译子模块与
  `polymarket_sdk_runtime.rs` 的 `#[cfg(test)]` seam。
- recovered immutable exact-match 仍只有原 comparator；其返回值新增
  `fresh discovery.current` 命中 recovered current 或 recovered next 的调用栈内
  角色结果。V3/V4/V5 validator、schema、discovery immutable 比较和普通 candidate
  equality 均未修改。
- late rollover 改为调用栈内
  `ExistingCurrent / CurrentSnapshot / CurrentCandidate` 三态 row 路由：
  - recovered current 或 recovered-next-to-current 写 current snapshot；
  - 普通 late current 写既有严格 candidate；
  - 只附加 next 时不重复 current，只写 next snapshot。
- 首次实现曾把“subscription error 后提交 metadata”错误地扩展到普通冷启动；
  `full_runtime_cross_platform_degradation_converges_at_run_deadline_and_finalizes_rows`
  以“CLOB 不可用却多出 1 条 metadata”真实检出。修订后冷启动恢复原语义：失败时
  在同一 UTC 日内零 metadata；后续外层重连成功时只补写缺失的 current/next
  snapshot。只有 context switch 在 subscription 已开始后提交新 context 对应
  authority，再返回原 retryable error。
- 不能在 subscription error 后盲目回滚：`before_ack` fixture 证明 subscribe 已发送、
  ack 尚未发布时，fresh target Book 已可经唯一 receiver 被接纳；同一 drain 还可能
  更新 pending/terminal evidence。最终实现保留这些事实并补齐 authority，外层新
  连接只重新订阅，不需要 marker、sidecar、恢复 map 或第二状态机。
- 错误返回一律保留 recovered 槽位；只有完整 context switch 成功才消费。该保守门
  明确落实“subscription/processing/metadata error 不得提前消费”，同时不影响已
  注册 authority 保证 context 可重试。
- `register_source_authority` 沿用全部既有业务校验，只把最终 bounds 检查改为事务性：
  检查失败恢复原 vector，防止错误返回后留下假注册状态。pre-subscription
  `desired_assets` 失败同样只恢复本次替换的 current/next、early claim 和 authority，
  pending 及其已处理 evidence 保持。
- 当前验证：
  - Polymarket source 模块：`110 passed; 0 failed`；
  - recovered current/next 正例、next-only 负例、Expired A->B、pre-subscription
    rollback、三类 ack-before-terminal 路由、冷启动失败后单次 snapshot 补写及
    只重订阅不重复 metadata 均通过；
  - 全量退化场景恢复为零 fabricated metadata；
  - `cargo check -p data-collect --all-targets`、`cargo clippy -p data-collect
    --all-targets -- -D warnings`、`cargo fmt --all` 与 `git diff --check` 通过。
- 排除下述既有时间段依赖夹具后，`cargo test -p data-collect --tests` 的库测试结果为
  `384 passed; 0 failed; 1 ignored; 1 filtered out`，全部 integration contract tests
  通过。
- crate 未排除运行的唯一剩余失败是既有负载测试把运行当日 `12:00:00Z` 设为
  receive；在 12:00Z 前执行时，该值晚于真实 admission，触发
  `invalid_real_admission_clock` 并等待负载完成超时。该测试及其动态时间夹具不在
  B2 diff 中，本事项不顺手修改；另一个最初失败的退化测试已由上述范围收窄修复。
- 两轮实现中间审查发现并推动修正了普通 candidate、next-only attach 和冷启动三条
  错误边界；截至该历史检查点仍须完成最终 `gpt-5.6-sol/ultra` 独立阶段审查。该
  审查、commit/push 和 release 构建后续均已完成。

## Stage 6R B2 最终审查发现与 UTC 日界决策

- 最终三路只读审查确认：生产模块 authority 唯一、没有旧实现旁路，没有新增
  validator/schema/shadow comparator；同时发现冷启动失败跨 boundary 的遗漏。
- 同一 UTC 日内若 current/next 尚无 source authority，outer recovery 不再推进
  market boundary，也不让已过期 current 形成零延迟忙循环；成功重订阅后复用既有
  缺失 metadata 补写，再执行 boundary transition。
- UTC day fence 不能只等待：旧日关闭后补 snapshot 会被既有 `RawDayGate` 以
  `raw event UTC day regressed` 正确拒绝。用户已批准推荐方案：在旧日关闭前补写
  真实 Gamma snapshot，再推进 boundary；这是普通冷启动“失败零 metadata”的唯一
  窄范围封盘例外。
- 首次 discovery 在 subscription 前已 `Expired` 时不保留未安装 current/next，
  而是清除后重新 discovery；recovered authority 保留。
- 最小性审查提出的冗余已删除：`fresh_current` 只保留 bool，不再保存无消费方的
  recovered 槽位枚举；重复 classifier 单测删除，其必要负例升级为
  `discovery.next` 命中 recovered next 的真实 candidate + next snapshot 组合测试。
- before-ack 并发夹具增加统一外层 timeout；冷启动跨 market boundary 测试改用已
  完成的过去窗口和注入 actor clock，不依赖 100ms 墙钟调度。
- 当前新增定向结果：
  - 冷启动失败跨 market boundary 后重订阅、snapshot、transition：通过；
  - UTC day fence 旧日 snapshot 后 transition defer：通过；
  - next-only recovered match 仍写严格 current candidate：通过；
  - initial `Expired` 清除未安装 context 且不消费 recovered authority：通过；
  - pending-only before-ack 竞态及统一 timeout：通过。
- 最终全量测试命令为
  `cargo test -p data-collect --tests -- --test-threads=1 --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`：
  library `384 passed; 0 failed; 1 ignored; 1 filtered out`，所有 integration
  contract tests 通过。曾尝试的 `cargo test --workspace --all-targets -- ...`
  已先完成同一批 library/integration tests，随后仅因自定义 bench
  `polymarket_storage_ab_probe` 不接受 test-harness 参数而返回非零；这不是代码或测试
  失败，bench 的编译已由 `cargo check/clippy --all-targets` 覆盖，因此不修改既有
  probe CLI。

## Stage 6R B2 新旧 UTC 日 evidence 竞态与批准修正

- 最后一轮正确性反证发现 1 个 Important：initial discovery 与已排队 UTC fence
  同时 ready 时，原随机 select 可能先返回 discovery。两层 current 检查会排除已
  过期的旧日 23:55 market，因此能留下的是
  `metadata_received_utc_ms >= day_boundary_utc_ms` 的新日 current。
- 原实现会在旧日 fence 无条件补写所有 missing snapshot。新日 row 会被既有
  `RawDayGate` 正确 defer，而 source-authority admission 又必须把 Deferred 视为
  未接纳；结果是 fence reply 失败并令 source 退出。原旧日测试只覆盖
  `metadata_received_utc_ms < boundary`，未覆盖该竞态。
- 用户已批准最小修正：
  - fence 前只补写 source receive 严格早于 boundary 的旧日 missing snapshot；
  - 补写后重新检查 current/next authority 完整性，只有完整才推进 boundary；
  - 新日 context 不写入或 defer 到旧日，完成 seal/resume 后由成功重订阅在新日
    恰好补写一次 snapshot；
  - initial discovery 的同就绪仲裁明确 control-first。
- 实现没有增加 schema、validator、marker、状态机或测试 pause。缺失 metadata
  helper 只增加调用方提供的 UTC fence 上界；普通重订阅仍走无上界的原路径。
- 新增确定性测试结果：
  - 旧日 missing current：fence reply 前 snapshot，随后既有 boundary transition，
    通过；
  - 新日 missing current：旧日零 row、零 defer，seal/activation/resume 后首次
    重订阅恰好一条 snapshot并消费 recovered authority，通过；
  - ready discovery 与已排队 fence：fence 先 reply、resume 前 discovery 不返回、
    resume 后返回同一 discovery，通过。
- 修正后验证：
  - Polymarket source 模块：`112 passed; 0 failed`；
  - `cargo test -p data-collect --tests -- --test-threads=1 --skip
    four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`：
    library `386 passed; 0 failed; 1 ignored; 1 filtered out`，所有 integration
    contract tests 通过；
  - `cargo check -p data-collect --all-targets`、`cargo clippy -p data-collect
    --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和
    `git diff --check` 通过。
- 截至该历史检查点仍须完成最新冻结 diff 的最终三路独立复审；该复审后续已清零并
  完成 commit/push。

## Stage 6R B2 最终阶段审查闭环

- 最新冻结源码 hash：
  - `live_runtime/polymarket_source.rs`：
    `33b8cbad83ca528483019be26f0771dbec660296`；
  - `live_runtime/tests/polymarket_source.rs`：
    `d207ca54a2a3eb716e28013bf08a0a5b0c62810a`；
  - `polymarket_sdk_runtime.rs`：
    `3ff05f12e00efaddebf94cf756e61cad555b1863`。
- 三路 `gpt-5.6-sol/ultra` 最终只读阶段审查全部通过：
  - 正确性：Critical 0 / Important 0 / Minor 0；
  - 最小性：Critical 0 / Important 0 / Minor 0；
  - 需求、计划、changelog 与测试合同：Critical 0 / Important 0 / Minor 0。
- 正确性审查额外反证确认：
  - `metadata_received_utc_ms == day_boundary_utc_ms` 属于新日，严格 `< boundary`
    才能补入旧日；
  - mixed current/next 只补旧日 evidence，任一 authority 缺失时完整性门阻止
    boundary transition；resume 后无上界的普通重订阅补齐，再由主循环推进；
  - control-first 同就绪仲裁确定，seal/resume 后新日只有一条 snapshot，成功后才
    consume recovered authority。
- 最小性审查确认没有新增 validator、shadow comparator、第二 discovery 实现、
  marker、持久状态或第二状态机；generic Future core 与 source-receive 上界均只有
  本事项的必要消费方。
- Stage 6R B2 已满足需求与计划完成门。当时下一步是 commit/push，再从 clean
  commit 构建并记录新 release；这些动作及后续重部署现均已完成，原远端停止门不再
  是当前指令。

## Stage 6R B2 release

- B2 实现提交为 `a3fed4050e5554bb2f24d567d182d4f3ce87aece`
  （`fix(data-collect): repair pending restart metadata routing`），已推送到
  `origin/fix-rtds-silents`；构建前本地 HEAD、remote-tracking ref 一致且工作树
  clean。
- 从该 commit 本地执行：
  `DATA_COLLECT_GIT_COMMIT=a3fed4050e5554bb2f24d567d182d4f3ce87aece cargo build --release --locked --bin data-collect-training-collector`。
- 新部署产物：
  - path：
    `/home/chillchill/fix-rtds-silents/target/release/data-collect-training-collector`；
  - size：30,322,264 bytes；
  - SHA256：
    `5359b4cfcd397f8fd83a648808950e635df54b7fff5ee7ab584238d2c5784b0e`；
  - ELF：x86-64 PIE、dynamic；
  - Build ID：`b4a1f99177577a926e0db1811636c7923d927b2b`；
  - 最大 GLIBC requirement：`GLIBC_2.38`；
  - `--version`：`data-collect-training-collector 0.1.0`；
  - binary strings 包含完整 build commit
    `a3fed4050e5554bb2f24d567d182d4f3ce87aece`，`--help` 参数与部署合同一致。
- 对同一 SHA 的 release 使用全新隔离 root
  `/tmp/data-collect-stage6r-release-probe-h4a7Fm` 运行 15 秒：
  - exit 0，elapsed `15.15s`，maximum RSS `39,660 KiB`；
  - manifest build commit 与 binary SHA 精确匹配；
  - 六个逻辑 source 各生成一个 finalized Parquet，共 5,810 rows；
  - manifest `gaps=[]`，所有 source `lag_events=0`、`dropped_rows=0`；
  - 六个 Parquet 的实际 SHA256 与 manifest 逐文件一致；
  - 当日顶层 manifest 仍为 `open`，短时进程 coverage 为 `ended_known`，符合当日
    dataset root 仍可继续激活的既有合同，不将短时 probe 误写为完整训练日。
- `gpt-5.6-sol/ultra` 独立 release 审查重新核验远端 branch ref、binary 属性、probe
  原始运行结果、manifest 与六个 Parquet，结论为
  `PASS — Critical 0 / Major 0 / Minor 0`。
- 审查发现 demand/plan 顶部状态当时仍停留在 B2 执行中；该检查点把三件套状态同步为
  `STAGE6R_RELEASE_COMPLETE_DEPLOY_PENDING`。该状态后续已被部署、Stage 6 证据和
  2026-07-30 用户最终验收取代。
- 当时计划在本节提交/push 后进入 Stage 5，并复核 UTC27/UTC28 本地副本后执行两个
  远端目录直接清空与重部署；上述动作后续已完成，本段不再是执行指令。

## Stage 5 B2 直接清空与重部署

- 破坏性操作前重新只读核验本地副本：
  - UTC27 manifest SHA256 为
    `72ed09f2bbbe68d0b7a1d3d169a03f1f2e0f2778815664c912cd6f4154b4c192`；
    文件集合为 26 个 manifest 所列 Parquet 加一个 manifest，逐文件
    bytes/SHA256/PAR1 header/footer/footer rows 全部通过，共 7,858,199 rows、
    486,395,408 Parquet bytes；
  - UTC28 manifest SHA256 为
    `0c08c357d3abbc901903efc83316d50edb92aadac51031903c2e66ed7f84c221`；
    文件集合为 9 个 manifest 所列 Parquet 加一个 manifest，逐文件同样全部通过，
    共 1,345,977 rows、82,699,349 Parquet bytes；
  - UTC28 故障副本仍为 `open / unclean_end_unknown`、13 gaps，Polymarket
    `lag_events=2145`、`dropped_rows=45`。
- 远端后来存在的 2026-07-28 22:18--22:25 文件不是原始 10 文件故障副本，而是本文
  已记录的首次 Stage 5 验证 root。该 root 的受控重启复现了
  `candidate_to_active:fixed_identity_not_validation`，已在 Stage 6 固定为不兼容、
  不可作为健康日；用户批准 B2 后的执行边界明确要求再次直接清空该验证 root，因此
  不将它重新拉回或迁移。
- `2026-07-29T05:58:46Z` 远端预检确认：
  - unit 为 `inactive/dead`、`MainPID=0`；
  - x86_64、glibc 2.39；
  - ExecStart 仍精确引用批准的 datasets 与 data-collect 路径；
  - 旧验证 binary SHA256 为
    `80fa49a4b2261e21bb58e4466da5d67f0b6f101d986e886d7248de40d1e2e18b`。
- 再次执行 `systemctl --user stop` 并确认 `MainPID=0` 后，直接删除且只删除：
  - `/home/ubuntu/btc-updown-marketmaker/datasets`；
  - `/home/ubuntu/btc-updown-marketmaker/data-collect`。
  两个路径删除后均不存在；没有 staged rename、migration 或滚动替换。
- 保留 `/home/ubuntu/.config/systemd/user/data-collect-training-collector.service`；
  unit 仍为 `ubuntu:ubuntu`、mode 0644，mtime
  `2026-07-15T18:12:58.479030609Z`。重新创建 `data-collect/bin` 后直接上传 B2
  release，远端 binary 为 `ubuntu:ubuntu`、mode 0755、30,322,264 bytes，SHA256
  与 Build ID 分别为：
  - `5359b4cfcd397f8fd83a648808950e635df54b7fff5ee7ab584238d2c5784b0e`；
  - `b4a1f99177577a926e0db1811636c7923d927b2b`。
  `--version` 为 `0.1.0`，内嵌 build commit 为
  `a3fed4050e5554bb2f24d567d182d4f3ce87aece`，远端 `ldd` 正常解析全部依赖。
- `systemctl --user reset-failed` 后于 `2026-07-29T06:01:30Z` 启动原 unit：
  - MainPID `1229293`，`active/running`、`Result=success`、`NRestarts=0`；
  - `/proc/1229293/exe` 路径与 SHA256 精确匹配上述 release；
  - 新 UTC29 manifest 的 build commit 与 binary SHA 正确，
    coverage=`active`、`reactivated_within_day=false`、`gaps=[]`；
  - 唯一进程同时持有六个 source 的 `.running` 文件。
- 到 `2026-07-29T06:03:45Z`，PID 与 restart count 未变，memory current/peak 约
  39.5/39.6 MiB。Polymarket market `.running` 已增长至 2,221,865 bytes；此前
  20 秒采样从 1,202,128 增长到 1,540,591 bytes。其他 source 的 `.running`
  在该时点仍为零长度；进程已持有对应文件描述符，但仅凭物理长度不能区分 writer
  buffering 与业务静默，不据此误判写入失败或提前宣称其行级健康。
- 启动后的 journal 只有 systemd started 行，未出现 receiver lag、backpressure、
  raw-after-close、fixed-identity、validation failure、stack overflow、panic 或
  source error。
- 独立只读 live-health 从 `2026-07-29T06:08:06.764Z` 连续观察 `120.483s`：
  - user unit 全程保持 PID `1229293`、`active/running`、`NRestarts=0`，`/proc`
    binary 路径和 SHA 均未变化；
  - Polymarket market `.running` 五次采样从 6,989,601 增长到 9,353,708 bytes，
    每段都增长；其余五个 `.running` 全程仍为零长度，但六个文件均由同一 PID 持有；
  - manifest 保持 `open / active / ended_utc_ms=null / reactivated=false /
    gaps=[]`，当前 source counters 仍为零；
  - journal 仍只有 Started；事故关键词为零；
  - memory current 从 75,464,704 增至 93,138,944 bytes，`06:10:42Z` 末检为
  97,275,904 bytes、peak 97,546,240 bytes。短窗口内上升是事实，不能据此判定
  泄漏或长期稳定；当时继续交给 Stage 6 观察，后续未把它认定为本事故阻断项。
- live-health 审查者首次漏用 `systemctl --user`，误查系统级同名 unit 并短暂报告
  `inactive/dead`。直接 PID/cgroup 反证和修正后的 user-unit 查询均确认真实服务
  未停止；该查询作用域误判已撤回，不作为运行异常证据。
- `gpt-5.6-sol/ultra` Stage 5 独立审查结论为
  `PASS — Critical 0 / Major 0 / Minor 0`。审查重新核验本地两日逐文件完整性、远端
  删除范围、unit、binary、唯一 PID、六源文件描述符、manifest lineage、实时增长、
  memory 边界和 journal；确认没有新增复杂部署流程或过度校验。
- 截至该历史检查点，三件套状态同步为
  `STAGE5_B2_REDEPLOY_COMPLETE_STAGE6_PENDING`。该阶段 commit/push 后进入
  Stage 6；当时尚未宣称受控重启、UTC rollover 或完整健康 UTC 日已经通过。本状态
  后续已由 2026-07-30 用户最终验收章节取代，不能作为当前游标。

## Stage 6 B2 真实受控重启进行中

- Stage 5 checkpoint commit
  `e2350c5c89bc4b14a6bff6583151075f65030084` 已推送；进入 Stage 6 前工作树与
  `origin/fix-rtds-silents` 一致。
- 重启前 `2026-07-29T06:14:49.102533144Z`：
  - PID `1229293` 从 `06:01:30Z` 持续 active，`NRestarts=0`；
  - `/proc/PID/exe` SHA 与 B2 release 一致；
  - memory current/peak 约 112.6/113.1 MiB；
  - Polymarket market `.running` 为 12,725,513 bytes，journal 仍只有 Started。
- `systemctl --user restart` 于 `06:15:08.369175501Z` 发起、
  `06:15:08.868052894Z` 返回。旧 PID 在 `06:15:08.853342Z` 输出
  `coordinated_shutdown_complete`；新 PID `1231184` 于
  `06:15:08.865523Z` 启动，`NRestarts=0`，`/proc` 路径和 SHA 均正确。
- 旧 PID 协调退出形成六个 `shutdown_finalize` Parquet，共 378,888 rows：
  - Binance aggTrade 6,112；
  - Binance depth snapshot 1；
  - Binance depth update 8,152；
  - Polymarket market 363,815；
  - Polymarket metadata 13；
  - RTDS 795。
  六个文件的实际 SHA256 均与 manifest 精确一致；manifest 保持
  `open / active / reactivated_within_day=true / gaps=[]`，所有 source
  `lag_events=0`、`dropped_rows=0`。
- finalized metadata 证明本次不是未形成 pending 的短时重启：
  - market `btc-updown-5m-1785305400` 于 `06:15:00.004Z` 进入仍未 terminal 的
    `resolution_pending`，首次 fixed time 为 `06:05:00.049Z`；
  - recovered current `btc-updown-5m-1785305700` 的首次 fixed time 为
    `06:10:00.046Z`，于 `06:15:00.004Z` 从 next 提升为 current；
  - recovered next `btc-updown-5m-1785306000` 的首次 fixed time 为
    `06:15:00.053Z`。
- 到 `06:19:18.924189966Z`，新 PID 保持 `active/running`、`NRestarts=0`，
  memory current/peak 约 88.5/89.0 MiB；Polymarket market
  `06-part-1.parquet.running` 已增长至 4,454,831 bytes。journal 只有本次受控停止、
  `coordinated_shutdown_complete` 和新启动记录，没有
  `candidate_to_active:fixed_identity_not_validation`、receiver lag、backpressure、
  raw-after-close、panic 或其他 source error。
- 旧 B1 在相同 pending-only 路径重启后约两秒即失败；本次新 PID 已持续运行并继续
  写入，证明 runtime 失败门已通过。截至该历史检查点，重启后的 metadata
  `06-part-1.parquet.running` 尚未自然 finalize，当前不能逐行确认 current snapshot、
  零 candidate、首次 fixed time 保留与 fresh validation 前进。
- 为避免第二次人工重启这种不必要的额外扰动，当时保持服务连续运行，等待该小时文件
  自然 finalize 后再做上述内容验收。当时 UTC rollover 与完整 UTC 日尚无通过证据，
  三件套状态只推进为 `STAGE6_CONTROLLED_RESTART_RUNNING_CONTENT_PENDING`；该历史
  pending 状态后续已被 2026-07-30 用户最终验收取代。
- `gpt-5.6-sol/ultra` 独立 checkpoint 审查逐文件复核六个 Parquet 的 SHA、
  PAR1/footer rows、metadata 行级 pending/current/next 关系、重启时间线、新 PID
  存活与 journal，结论为 `PASS — Critical 0 / Major 0 / Minor 0`。审查确认没有
  越界把重启前 finalized metadata 当作重启后内容证据，也没有加入第二次重启或额外
  校验流程。

## Stage 6 B2 真实受控重启内容门通过

- 15 分钟只读监控第 2 个周期于 `2026-07-29T07:07:57Z` 准时触发。07:00 自然小时
  finalize 已完成，服务仍为 PID `1231184`、`active/running`、`NRestarts=0`，
  journal 事故关键词为零。
- UTC29 manifest 从六个文件、378,888 rows 推进为 12 个 finalized Parquet、
  1,664,602 rows；六源累计为：
  - Binance aggTrade 38,905；
  - Binance depth snapshot 2；
  - Binance depth update 35,006；
  - Polymarket market 1,587,245；
  - Polymarket metadata 48；
  - RTDS 3,396。
  manifest 继续为 `open / active / reactivated_within_day=true / gaps=[]`，六源
  `lag_events=0`、`dropped_rows=0`、`reconnect_events=0`。
- 重启后的 finalized metadata 文件
  `polymarket_market_metadata_06-part-1.parquet` 为 40,409 bytes、35 rows，SHA256
  `f719050b32db675cb8f412d967016df36044fff6ee2793cf1f8a01f55ce32af3`；
  远端 manifest、远端实际文件和本地只读副本一致，PAR1 header/footer 与 footer
  row count 全部通过。
- 该文件的前两行直接关闭 B2 内容门：
  - current `btc-updown-5m-1785305700` 写 `metadata_snapshot`，首次 fixed time
    保持 `06:10:00.046Z`，fresh receive/validation 为 `06:15:10.444Z`，
    admission 为 `06:15:10.648Z`；
  - next `btc-updown-5m-1785306000` 写 `metadata_snapshot`，首次 fixed time
    保持 `06:15:00.053Z`，fresh receive 为 `06:15:10.691Z`，
    validation/admission 为 `06:15:10.704Z`。
- 全 35 行的 admission sequence 精确为 `1..35`，row kind 为 10 个 snapshot、
  16 个 state transition、9 个 WS terminal；所有 69 列字符串中没有 `candidate`，
  `candidate_to_active=0`、`none_to_current=0`。B2 前后 strict candidate validator
  未修改，producer admission 前和 writer 落盘前仍执行完整 validation。
- `gpt-5.6-sol/ultra` 独立内容审查结论为
  `PASS — Critical 0 / Major 0 / Minor 0`；immutable identity、fixed time、时间
  因果、role、pending/terminal 链均零失败。证据足以把三件套推进为
  `STAGE6_CONTROLLED_RESTART_CONTENT_PASS_ROLLOVER_PENDING`。这是
  2026-07-29 当时的历史检查点，不是当前状态。
- 该检查点只证明真实受控重启及自然小时 finalize；当时尚无 UTC day rollover 与
  完整健康 UTC 日证据。2026-07-30 用户最终验收已移除这两个 completion gate，
  因此不得再从本段推导当前仍需等待；同时也不得反向声称两项证据已经发生。

## 2026-07-30：用户最终验收并关闭事项

- 用户明确确认 `ROLLOVER_PENDING` 已被其最终验收，本事项不再等待 UTC day
  rollover 或完整健康 UTC 日。
- 最终验收沿用已通过独立审查的真实受控重启、自然小时 finalize、current/next
  snapshot、首次 fixed time 保留、fresh validation 前进和零 candidate 证据。
- UTC day rollover 与完整健康 UTC 日作为 completion gate 被取消，而不是被伪记为
  已获得运行证据；后续同类检查只属于持续运维观察。
- 三件套当前状态统一为 `CLOSED_BY_USER_ACCEPTANCE_20260730`。
