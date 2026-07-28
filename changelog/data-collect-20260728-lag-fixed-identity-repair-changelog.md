# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR Changelog

> 状态：`STAGE5_COMPLETE_STAGE6_RUNNING`
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

## 待执行

- Stage 0 commit/push。
- Stage 1 bounded recovery 实现。

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
- Stage 3 当前等待自审与 `gpt-5.6-sol/ultra` 独立阶段审查；审查通过前不提交、
  不推送，也不进入远端清空或部署。

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
- 修复后 `live_runtime::tests::polymarket_source` 仍为 102 passed；Stage 3 等待
  `gpt-5.6-sol/ultra` 聚焦复审。复审通过前不提交、不推送、不操作远端。

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
- Stage 3 已满足实现、全量验证和独立审查完成门；本 changelog 与代码在同一阶段
  commit/push 后进入 Stage 4。远端清空和部署仍未开始。

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
- 当前不把 debug-only 现象转化为远端环境变量或额外 runtime 配置；是否属于本事项
  release 阻断项，交由 Stage 4 `gpt-5.6-sol/ultra` 最终独立审查。远端仍未删除、
  上传或重启。

## Stage 4 最终独立审查

- `gpt-5.6-sol/ultra` 最终独立审查结论为 `PASS`，可执行 finding 为 0。
- 审查逐项确认需求 AC1 至 AC7 已有相符代码、测试与实物证据；AC8、AC9 正确保留为
  `PENDING_BY_PLAN`，分别由 Stage 5 远端部署和 Stage 6 长跑验收完成，不在 Stage 4
  提前宣称通过。
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
  也不允许为远端新增 `RUST_MIN_STACK`。Stage 6 继续监控 release 是否出现
  `SIGABRT` 或 `stack overflow`。
- 下一步先提交/push 本阶段事实，再从该 clean commit 本地构建最终 release。远端
  删除、上传和启动仍未执行。

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
- Stage 4 完成。下一步严格按用户批准的简单开发流程：再次确认 UTC27/UTC28 本地
  副本，停止远端服务，直接删除两个精确目录，创建程序目录、上传上述 SHA 的
  release，并启动现有 unit。

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
- Stage 5 部署与初始健康子门通过；需求 AC8 整体仍为
  `PENDING_STAGE6`。六任务长期稳定、受控重启、UTC rollover 和完整 UTC 日尚未验收，
  不得提前宣称健康训练日。
