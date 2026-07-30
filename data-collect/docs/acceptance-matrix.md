# data-collect 当前验收矩阵

> 生命周期：`CURRENT`
>
> 阶段 2 当前权威入口：`demand/data-collect-demand.md` + 本矩阵
>
> 阶段 3 完成后入口将切换为：`data-collect/docs/CURRENT.md`
>
> 对应整理事项：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 对应 changelog：`changelog/data-collect-current-authority-consolidation-changelog.md`

本矩阵只描述当前代码可以实现和测试的 `data-collect` 合同。历史需求、计划、
changelog、Stage 报告、Phase 0 probe、旧 SDK gap、远端 Attempt 和长跑记录只能作为
历史证据，不能单独改变本矩阵中的当前结论。

若本矩阵与当前 Cargo、代码或测试发生冲突，必须停止并核实，不得按文件日期或局部
历史片段自行选择 authority。

## 1. 模块边界与三个 binary

| binary | 当前实现 | 明确不负责 |
| --- | --- | --- |
| `data-collect-training-collector` | 连接真实 Polymarket、RTDS、Binance 数据源，写六类 typed raw Parquet 和每日 manifest | labels、features、训练切分、模型输入、下单、撤单、钱包、NOE、ledger、PnL |
| `data-collect-paper-hot` | 在 paper 主机本地创建 startup receipt 和 Unix socket heartbeat/status handoff | 当前不连接真实外部市场源，不提供 `external_market_event` |
| `data-collect-live-hot` | 在 live 主机本地创建 startup receipt 和 Unix socket heartbeat/status handoff | 当前不连接真实外部市场源，不参与交易执行或账本 |

当前只有 training binary 运行真实外部市场源。paper/live 真实 external-source adapter
已延期到训练模块完成后另立 demand/plan/changelog；该延期不阻断本轮
`data-collect` 收尾。

验收证据：

- `data-collect/tests/deployment_binary_contract.rs`
- `data-collect/tests/paper_live_boundary_contract.rs`
- `data-collect/tests/training_collector_runtime_contract.rs`
- `data-collect/tests/hot_path_handoff_contract.rs`

## 2. Polymarket SDK 合同

当前 Polymarket 依赖为：

```toml
polymarket_client_sdk_v2 = { version = "=0.7.0", features = ["clob", "gamma", "rtds", "ws"] }
```

workspace 通过唯一的 `[patch.crates-io]` 路径使用：

```text
third_party/polymarket_client_sdk_v2-0.7.0-p1
```

当前 Polymarket CLOB、Gamma 和 RTDS 路径以官方 `0.7.0` crate 为底座，并由项目
patch contracts 固定所需连接、parser、heartbeat 和 subscription 行为。不得把历史
SDK 0.6.0 状态、旧 gap 报告或未经批准的 raw REST/manual signing/shadow SDK 当成
当前实现。

验收证据：

- `data-collect/Cargo.toml`
- workspace `Cargo.toml`
- `Cargo.lock`
- `data-collect/tests/polymarket_sdk_patch_contract.rs`
- `data-collect/tests/polymarket_sdk_runtime_contract.rs`

## 3. 六个 current new-write schema

| `source_kind` | 当前 new-write schema | 当前内容 |
| --- | --- | --- |
| `polymarket_market` | `polymarket_orderbook_v3` | target-admitted CLOB typed market event、盘口、source/admission 顺序和平台时间 |
| `polymarket_market_metadata` | `polymarket_market_metadata_v5` | discovery、固定 identity、生命周期、resolution attempt/terminal/summary |
| `polymarket_rtds_chainlink_btc_usd` | `polymarket_rtds_chainlink_btc_usd_v4` | Chainlink BTC/USD typed value、topic/type/filter、source/receive timestamp 和 freshness |
| `binance_depth_snapshot` | `binance_depth_snapshot_v3` | REST snapshot、完整 bids/asks、fetch 时间和 admission coordinate |
| `binance_depth_update` | `binance_depth_update_v4` | WS depth update、完整增量、source/admission coordinate 和 stitch verdict |
| `binance_aggtrade` | `binance_aggtrade_v7` | WS aggTrade 或有界 invalid-input evidence、trade/event time 和 ordering coordinate |

旧 schema reader 只用于明确保留的历史读取或恢复兼容；它们不是当前 new-write
authority。当前六源不能降级为只有 `source_kind/raw_json` 的 opaque 宽表。

验收证据：

- `data-collect/src/live_runtime/writer_supervisor.rs`
- `data-collect/tests/polymarket_orderbook_v3_contract.rs`
- `data-collect/tests/polymarket_metadata_layout_contract.rs`
- `data-collect/tests/source_hourly_layout_contract.rs`
- `data-collect/tests/parquet_writer_contract.rs`

## 4. Hourly Parquet 与每日 manifest

当前训练数据布局为：

```text
<dataset-root>/training/typed_raw/
└── utc_day=YYYY-MM-DD/
    ├── manifest.json
    └── source_kind=<source_kind>/
        ├── <source_kind>_HH.parquet
        ├── <source_kind>_HH-part-N.parquet
        └── <source_kind>_HH[-part-N].parquet.running
```

当前合同：

1. 分区日和小时只按 collector `recv_time_utc_ms` 决定。
2. `*.parquet.running` 不是 finalized artifact。
3. 小时变化、UTC 日切换或协调停止会 finalize 当前 segment。
4. `50,000` rows 或 `16,777,216` logical bytes 只结束 row group，不创建新
   `part-N` 文件。
5. `part-N` 只用于同一 source/hour 已存在 finalized 或保留路径时的文件名碰撞。
6. `binance_depth_snapshot` 是稀疏 source，不要求每小时存在文件。
7. 每个 UTC 日只有一个 `manifest.json` 和一个 manifest owner。
8. manifest schema 为 `data_collect_daily_manifest_v3`，记录 finalized file 的
   path、bytes、rows、SHA-256、schema、source coverage、counter、gap、collector
   identity 和 collection-day coverage。
9. 正常 UTC rollover 后旧日变为 `closed`；协调停止不会伪造完整日，当前日仍可保持
   `open` 并记录真实 coverage 终点。
10. 当前 training live writer 不依赖每文件 runtime sidecar、run receipt 或第二套
    manifest authority。

验收证据：

- `data-collect/tests/hourly_partition_contract.rs`
- `data-collect/src/daily_manifest.rs` 的 unit tests
- `data-collect/src/live_runtime/tests/manifest_owner.rs`
- `data-collect/src/live_runtime/tests/writer_supervisor.rs`
- `data-collect/src/live_runtime/tests/runtime_rollover_driver.rs`

## 5. 50GB 容量门、退出码 75 与无 retention 删除

默认容量上限为十进制：

```text
50_000_000_000 bytes
```

容量统计范围为：

```text
<dataset-root>/training
```

当前行为：

1. 启动前检查一次；运行期间每 `60s` 检查一次。
2. 启动时已达到上限，必须在创建 typed-raw manifest 或启动网络 source 前退出。
3. 运行中达到上限，必须先执行协调停止、停止新 admission、drain/finalize 已接纳
   数据并更新 manifest，再返回 `dataset_capacity_reached`。
4. 该错误映射为进程退出码 `75`。
5. collector 不执行 retention，不为释放容量而删除、移动或改写 finalized raw、
   Parquet 或 manifest。
6. 启动时对无 owner 的 stale temporary 文件所做清理，以及 hot binary 对
   `hot.sock` 的生命周期清理，不属于 finalized dataset retention。
7. systemd 必须配置 `RestartPreventExitStatus=75 76`，避免容量门或 operator
   intervention 形成自动重启循环。

验收证据：

- `data-collect/src/runtime.rs`
- `data-collect/src/live_runtime/dataset_capacity.rs`
- `data-collect/tests/training_collector_runtime_contract.rs`
- `data-collect/tests/deploy_readme_contract.rs`
- `data-collect/deploy/systemd/data-collect-training-collector.service`

## 6. RTDS 当前 300/60 full-replacement 例外

RTDS 当前 fresh 必须同时满足：

- `topic=crypto_prices_chainlink`
- `type=update`
- `symbol=btc/usd`
- message 与 payload timestamp 合理
- `recv_time - payload_time` 位于 `-5000ms..=30000ms`

当前业务静默恢复合同：

1. 首次收到 fresh 之前禁用 cold-start business watchdog。
2. 曾经收到 fresh 后，同一 runtime 连续 `300s` 没有 fresh，触发
   `steady_silence` full replacement。
3. replacement 后，以 SDK `Connected` 和 subscription locally applied 两者中较晚的
   时点为锚；`60s` 内仍未收到首次 fresh 时，先进入外层 backoff，再执行下一次 full
   replacement。
4. outer backoff 从 `1s` 开始指数增加，带确定性 jitter，单次最多 `300s`，总
   replacement 次数不限。
5. full replacement 必须先 `shutdown_and_wait()` 完成旧 `RtdsSdkRuntime` teardown，
   再创建新 runtime；不得同时保留两个 RTDS runtime/socket。
6. SDK transport disconnect/reconnect 仍由 SDK owner 处理，并取消当前业务等待；
   fresh 会结束当前 episode 并重置 backoff。
7. stale、wrong topic/type/symbol 或被忽略的 snapshot envelope 不得刷新 fresh
   watchdog。
8. 这是当前获准的 RTDS 业务静默 full-replacement 例外；旧文档中的“所有业务静默
   永远不得驱动 RTDS runtime replacement”不是当前 authority。

验收证据：

- `data-collect/src/live_runtime/rtds_source.rs`
- `rtds_business_timer_uses_late_anchor_and_preserves_cancelled_backoff_level`
- `rtds_business_silence_replaces_two_runtimes_then_fresh_resets_the_episode`
- `rtds_shutdown_waits_for_connection_loop_completion`
- `data-collect/tests/polymarket_sdk_runtime_contract.rs`

## 7. Resolution 合同

当前 Polymarket resolution 合同：

1. fixed identity 在 market active 前建立，包含 slug、condition id、CLOB market
   id、固定 Up/Down asset 顺序、start/end 和首次 verified time。
2. qualified `market_resolved` 必须先完成 target、identity、asset、outcome、winner
   和时间校验。
3. 当前实现保留 target-admitted `polymarket_orderbook_v3` source fact，并由
   `polymarket_market_metadata_v5` 的 `resolution_terminal_ws` 行承担唯一 WS
   terminal claimant/owner。
4. market 在 E 进入 pending；Gamma follow-up slot 固定为
   `E+3m`、`E+4m`、`E+5m`、`E+6m`、`E+7m`、`E+8m`。
5. qualified WS terminal 或 qualified Gamma terminal 只能关闭一次；不一致、
   重复 claimant、winner 冲突、late source 或 schema contradiction 必须 fail closed。
6. 六个 slot 未得到 terminal 时写真实 exhausted/summary 结果，不伪造 winner。
7. restart recovery 只恢复仍在窗口内的 pending，最多两个；在或晚于最后 slot 的
   lifecycle 不重新恢复为 pending。

验收证据：

- `data-collect/src/polymarket_resolution_recovery.rs`
- `data-collect/src/live_runtime/polymarket_source.rs`
- `data-collect/tests/polymarket_metadata_layout_contract.rs`
- `polymarket_resolution_timeline_is_absolute_green`
- `polymarket_terminal_fence_finalizes_three_reverse_races_once`
- `summary_reconstructs_six_ordinals_and_requires_zero_typed_source`

## 8. Fixed identity、restart 与 rollover 合同

### 8.1 Fixed identity 与 restart

1. restart recovery 使用 bounded projection 和最多 `16` rows/batch。
2. 同一次 finalized metadata scan 只恢复可能成为 current/next 的有界 fixed
   identity；不得保存全量 source key。
3. fresh Gamma discovery 只有在 slug、condition id、CLOB market id、固定顺序
   asset ids、start/end 全部精确一致时，才复用首次
   `fixed_identity_verified_at_utc`。
4. fresh source receive、payload digest 和 validation time 必须使用本次真实证据，
   不得用首次 fixed time 覆盖。
5. 多 authority、immutable mismatch 或非法首次时间必须在 row admission 前退出
   `76`。
6. recovered current 或已经变成 fresh current 的 recovered next 写
   `metadata_snapshot`，不得错误写为 `candidate_to_active`。
7. 只有 fresh next 命中 recovered next 时，不得让无 recovered authority 的 fresh
   current 绕过严格 candidate 合同。
8. recovered authority 只有在完整 context switch 成功后才消费；`Expired`、
   subscription、processing 或 admission error 均保留 recovery slot。
9. subscription 尚未开始的 cold-start failure 不写伪 metadata；已开始 subscription
   后，已真实 admission 的 evidence 不得回滚或重复写入。

验收证据：

- `restart_recovery_returns_bounded_current_and_next_fixed_identities`
- `restart_recovery_rejects_ambiguous_current_authority`
- `restart_recovery_rejects_same_structure_with_different_first_fixed_time`
- `pending_only_restart_exact_current_uses_snapshot_for_either_recovered_slot`
- `pending_only_expired_current_then_recovered_next_uses_monotonic_snapshot_retry`
- `switch_subscription_error_commits_candidate_or_new_next_authority`
- `initial_subscription_error_retries_snapshot_without_early_consume`

### 8.2 UTC rollover implementation

1. rollover 使用唯一 `RawDayGate`、四个 logical producer 和六个 writer。
2. 旧日先停止 admission 并完成 producer fence、writer finalize 和 manifest
   rendezvous，随后关闭旧日 manifest。
3. 只有旧日 durable close 成功后才能 activation 新日 manifest 并释放 producer。
4. 旧日 evidence 必须在旧日关闭前处理；新日 evidence 不得写入或 defer 到旧日。
5. initial discovery 与 UTC fence 同时 ready 时，control/fence 必须优先。
6. source/writer terminal、operator stop 和 capacity exit 必须沿用同一个 close
   identity，不能建立第二套 rollover authority。

验收证据：

- `normal_rollover_waits_for_four_producers_and_six_writers`
- `manifest_owner_requires_rendezvous_then_two_normal_rollover_cutpoints`
- `v3_sequence_state_survives_utc_day_rollover`
- `polymarket_outer_recovery_day_fence_admits_missing_snapshot_before_close`
- `polymarket_outer_recovery_day_fence_keeps_new_day_snapshot_for_resubscription`
- `initial_discovery_and_day_fence_ready_together_processes_fence_first`
- `reliable_boundary_close_has_one_manifest_digest_for_both_poll_orders`

2026-07-30 用户已接受 controlled restart、自然小时 finalize、fixed identity 保留和
零错误 candidate 作为事故事项的最终验收，并移除“必须再取得真实 UTC rollover 和
完整健康 UTC 日证据”的 completion gate。当前实现具有 deterministic rollover
tests，但本矩阵不得反向声称此前未记录的真实 UTC rollover/full-day evidence 已经
发生。

## 9. Paper/live 延期边界

当前已实现：

- 独立 paper/live binary；
- 独立 output root；
- startup receipt；
- 本机 Unix socket heartbeat/status；
- 与交易、钱包、NOE、ledger、execution feedback 隔离。

当前尚未实现或验收：

- paper/live 真实 Polymarket、RTDS、Binance adapter；
- 真实 `external_market_event` handoff；
- paper/live runtime 本机 receive timestamp artifact；
- paper/live systemd unit 和生产部署。

上述真实 adapter 延后到训练模块完成后另立事项。现有
`paper-live-hot-path-integration.md` 中的 future event/handoff 技术合同不得被解释为
当前 heartbeat server 已经实现真实市场事件流。

## 10. 部署边界

1. release binary 必须在本地从 clean、已 push 的 commit 构建。
2. 构建时注入并记录 `DATA_COLLECT_GIT_COMMIT`，并记录 binary SHA-256。
3. 远端服务器不执行 Rust 编译。
4. training 生产 binary 路径固定为：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector
```

5. training dataset root 固定为：

```text
/home/ubuntu/btc-updown-marketmaker/datasets
```

6. 仓库当前只有一个 authoritative training user unit：

```text
data-collect-training-collector.service
```

7. unit 使用 `Restart=on-failure`，但对 `75` 和 `76` 禁止自动重启。
8. 仓库当前没有 authoritative paper/live systemd unit。
9. release bundle、archive uploader、retention admin 和服务器 `git pull` 均不是
   当前 data-collect 部署路径。

验收证据：

- `data-collect/docs/training-collector-deploy-readme.md`
- `data-collect/deploy/systemd/data-collect-training-collector.service`
- `data-collect/tests/deploy_readme_contract.rs`
- `data-collect/tests/deployment_binary_contract.rs`

## 11. 当前可运行验证

### 11.1 格式、编译与静态检查

```bash
cargo fmt --all -- --check
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
git diff --check
```

### 11.2 当前 schema、binary、SDK 和部署 targets

```bash
cargo test -p data-collect --locked \
  --test deployment_binary_contract \
  --test paper_live_boundary_contract \
  --test training_collector_runtime_contract \
  --test hourly_partition_contract \
  --test source_hourly_layout_contract \
  --test polymarket_orderbook_v3_contract \
  --test polymarket_metadata_layout_contract \
  --test parquet_writer_contract \
  --test deploy_readme_contract \
  --test polymarket_sdk_patch_contract \
  --test polymarket_sdk_runtime_contract \
  --test collector_identity_contract \
  --test manifest_contract \
  -- --test-threads=1
```

### 11.3 当前 lib regression

```bash
cargo test -p data-collect --locked --lib -- \
  --test-threads=1 \
  --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

该命令覆盖 daily manifest、六 writer、rollover、fixed identity、restart、resolution
和 RTDS full replacement。

### 11.4 2026-07-30 本地验证快照

- 上述 `13` 个 integration targets：`134 passed; 0 failed`。
- lib regression 精确排除下述时间段依赖测试后：
  `386 passed; 0 failed; 1 ignored; 1 filtered out`。
- ignored 项是需要显式授权和外部运行条件的八小时 RTDS public probe，不阻断默认
  本地验收。
- 测试数量只记录本次真实运行快照；后续测试增删时不得把旧数量当成固定验收门。

### 11.5 已知时间段依赖测试

唯一已知会影响未排除 full suite 的测试为：

```text
four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
```

该 fixture 把运行当日 `12:00:00Z` 固定为 receive time。在 UTC 12:00 前运行时，
该值晚于真实 admission clock，会触发 `invalid_real_admission_clock` 并最终超时。
这是测试 fixture 的 wall-clock 时间段依赖，不是当前生产实现失败。

UTC 12:00 后可单独运行：

```bash
cargo test -p data-collect --locked --lib \
  'live_runtime::tests::four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second' \
  -- --exact --test-threads=1
```

默认 full regression 应精确跳过该 named test，不能扩大跳过范围。被 `#[ignore]`
标记的八小时 RTDS public probe 也不属于默认本地 completion gate。

## 12. 当前验收结论

| 合同 | 状态 |
| --- | --- |
| training 六源真实采集 | `IMPLEMENTED_AND_TESTED` |
| 三个独立 binary | `IMPLEMENTED_AND_TESTED` |
| Polymarket SDK 0.7.0 runtime | `IMPLEMENTED_AND_TESTED` |
| 六个 current typed schema | `IMPLEMENTED_AND_TESTED` |
| hourly Parquet 与 daily manifest | `IMPLEMENTED_AND_TESTED` |
| 50GB 协调退出 75、无 retention 删除 | `IMPLEMENTED_AND_TESTED` |
| RTDS 300/60 full replacement | `IMPLEMENTED_AND_TESTED` |
| resolution follow-up/terminal/recovery | `IMPLEMENTED_AND_TESTED` |
| fixed identity restart preservation | `IMPLEMENTED_AND_TESTED` |
| deterministic UTC rollover | `IMPLEMENTED_AND_TESTED` |
| 真实 UTC rollover/full healthy day 运行证据 | `NOT_CLAIMED_AND_NOT_A_COMPLETION_GATE` |
| paper/live heartbeat/status | `IMPLEMENTED_AND_TESTED` |
| paper/live 真实 external-source adapter | `DEFERRED_UNTIL_AFTER_TRAINING_MODULE` |
