# DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR Changelog

> 状态：`STAGE1_IMPLEMENTED_REVIEW_PENDING`
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
