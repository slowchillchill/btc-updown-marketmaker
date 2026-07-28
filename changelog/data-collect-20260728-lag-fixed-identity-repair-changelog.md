# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR Changelog

> 状态：`STAGE2_COMPLETE`
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
