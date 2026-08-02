# data-collect 当前行为合同

> 生命周期：`CURRENT_CONTRACT`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 当前需求：`demand/data-collect-demand.md`
>
> 当前计划：`plan/data-collect-plan.md`
>
> 当前 changelog：`changelog/data-collect-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

## 1. 模块边界

`data-collect` 负责外部市场数据接入、typed raw、实时 handoff 基础类型、source
coverage/gap/recovery、存储和 manifest。它不负责 labels、features、训练切分、模型
输入加工、订单、撤单、钱包、NOE、ledger、PnL 或交易策略。

| binary | 当前能力 |
| --- | --- |
| `data-collect-training-collector` | 连接真实 Polymarket CLOB/Gamma/RTDS 和 Binance，写六类 typed raw 与每日 manifest |
| `data-collect-paper-hot` | 写 startup receipt，运行本机 Unix socket heartbeat/status server |
| `data-collect-live-hot` | 写 startup receipt，运行本机 Unix socket heartbeat/status server |

只有 training binary 当前连接真实外部源。paper/live 真实 adapter 不属于
`model-train`，继续保持延期。必须先完成 `model-train`（模型训练模块，不是
`data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，
才另立三件套开发。

## 2. SDK 与 schema

Polymarket 当前依赖固定为
`polymarket_client_sdk_v2 = "=0.7.0"`，features 为
`clob/gamma/rtds/ws`；workspace 只有
`third_party/polymarket_client_sdk_v2-0.7.0-p1` 一个 patch。
未经用户批准，不得改用 Polymarket raw REST、manual signing、自建 SDK 或 shadow
implementation。

相对 crates.io `0.7.0`，`0.7.0-p1` 的 production `src/` delta 横跨四个文件：

- `clob/ws/types/response.rs`：保留 book、price_change、tick_size_change、
  last_trade_price、best_bid_ask、new_market、market_resolved 的 current
  nullable/compat typed 解析，另含 `MidpointUpdate.timestamp` 的 nullable
  compatibility；并保证 array 中 unknown/uninterested element 可跳过、每个
  interested element 必须全部成功，否则整帧报错且不泄露合法前缀。其中只有九个
  nullable Decimal 字段的精确空串 `""` 解为 `None`：
  `book.last_trade_price`、`price_change.best_bid/best_ask`、
  `tick_size_change.old_tick_size`、`last_trade_price.size/fee_rate_bps`、
  `best_bid_ask.best_bid/best_ask/spread`；非空 malformed（含空白串）、九字段之外的
  optional Decimal 空串和 required Decimal 空串仍报错。
- `rtds/subscription.rs`：提供最大 `2048` bytes、allowlist/fingerprint 且不回显
  非白名单 payload 的 RTDS parse diagnostic。只有顶层 object 的 `message` 精确等于
  `Network error communicating with endpoint` 且不含
  `topic/type/timestamp/payload` 时，才转为可恢复 connection fault；带任一 target
  字段的 target-like frame 仍是 sticky parser terminal。
- `ws/config.rs` 与 `ws/connection.rs`：增加 connect timeout、可选择是否要求精确
  text `PONG`、sticky terminal/parser fault、connection generation、generation-bound
  outbound fencing、FIFO/replay、bounded reconnect ownership 和 last-owner teardown。
  其中 outbound request 与 heartbeat ping 使用 `mpsc::unbounded_channel()`；它们是
  transport-control channel，不具备 memory hard cap。

完整 patch surface 由上述 vendored source、
`polymarket_sdk_patch_contract` 与 `polymarket_sdk_runtime_contract` 共同拥有；不得
只凭版本名删除、缩小或扩大其中任一行为。

| `source_kind` | current new-write schema |
| --- | --- |
| `polymarket_market` | `polymarket_orderbook_v3` |
| `polymarket_market_metadata` | `polymarket_market_metadata_v5` |
| `polymarket_rtds_chainlink_btc_usd` | `polymarket_rtds_chainlink_btc_usd_v4` |
| `binance_depth_snapshot` | `binance_depth_snapshot_v3` |
| `binance_depth_update` | `binance_depth_update_v4` |
| `binance_aggtrade` | `binance_aggtrade_v7` |

旧 schema 只可用于代码明确保留的精确 legacy read/recovery，不得成为 new-write
authority，也不得把六类 source 降级成 opaque 宽表。

## 3. 存储与容量

- collector-owned data-bearing、admission、deferred 和 writer queue/batch 必须有
  明确 hard bound。SDK transport-control unbounded channel 不是数据队列；当前
  wrapper 通过 `watch` latest-desired coalescing、固定小 desired set 和单 outstanding
  heartbeat 约束调用量，但这不等同于 channel 具有 hard capacity。
- UTC 日和小时按 collector receive time 分区。
- 每个 source/hour 通常只有一个 finalized Parquet；仅文件名碰撞时使用 `part-N`。
- `*.parquet.running` 不是 finalized artifact。
- `50,000` rows 或 `16,777,216` logical bytes 结束 row group，不产生新的 part。
- 每个 UTC 日只有一个 `data_collect_daily_manifest_v3`，由单一 owner 维护，并直接
  包含一个 `data_collect_collection_day_coverage_v1`。
- 小时变化、UTC rollover 或协调停止会 finalize 已接纳数据；停止不得伪造完整日。
- configured capacity limit 必须为正整数；CLI 默认值和当前 production unit 均为
  `50_000_000_000` bytes，测试或另行批准配置可以显式覆盖。
- 启动前一次容量扫描若已达到 configured limit，则不启动 source、不创建 typed
  raw/manifest/runtime artifact，直接退出 `75`。
- 运行期间每 `60s` 扫描达到 configured limit 时，停止新 admission、
  drain/finalize 已接纳数据、更新适用 manifest 后退出 `75`。collector 不自动
  删除、移动或改写 finalized raw。

## 4. RTDS 恢复

RTDS fresh 只接受正确 topic/type/symbol、合理 timestamp 且
`recv_time - payload_time` 位于 `-5000ms..=30000ms` 的消息。

- 首次 fresh 前禁用 cold-start business watchdog。
- 曾收到 fresh 后，连续 `300s` 无 fresh 触发完整 runtime replacement。
- 新 generation 的 connected 与 subscription locally applied 较晚锚点之后，
  `60s` 仍无首次 fresh，则进入外层 backoff 后再次 replacement。
- outer backoff base 从 `1s` 开始、每个已消费 level 按 `×2` 增加，应用
  `±20%` deterministic jitter 后单次 sleep cap 为 `300s`；fresh 后重置为首级。
- business replacement episode 无独立 attempt 次数或累计 elapsed exhaustion
  上限；仍受整体 runtime deadline、operator stop 和 hard error 边界约束。
- replacement 必须先完成旧 runtime teardown 再创建新 runtime，严格
  break-before-make，不允许两个 RTDS socket 并存。
- SDK transport disconnect/reconnect 仍由 SDK owner 处理；stale 或错误
  topic/type/symbol 不得刷新 fresh watchdog。

## 5. Identity、resolution、restart 与 rollover

- market fixed identity 包含 slug、condition id、CLOB market id、固定 Up/Down
  asset 顺序、start/end 和首次 verified time；多 authority 或 immutable mismatch
  必须在 admission 前 fail closed。
- market 在 E 进入 resolution pending；Gamma follow-up 固定为
  E+3、E+4、E+5、E+6、E+7、E+8 分钟六个 slot。
- owned role/window 内的 qualified WS 或 Gamma terminal 只能关闭一次；第二个
  Gamma terminal claim、fixed-identity/schema contradiction 或与已有 terminal
  winner 冲突必须 fail closed。
- 不再匹配 current/next/pending role 的 `market_resolved`，以及匹配 pending 但
  `recv_time_utc_ms` 超过 resolution budget horizon 的 late event，当前代码直接
  忽略，不得误写成一律 fail closed。
- restart 只恢复有界 current/next identity 和仍在窗口内的 pending；不得从摘要伪造
  terminal 或重开已结束窗口。
- UTC rollover 先关闭旧 raw admission，完成四 producer fence、六 writer finalize
  和 manifest rendezvous；旧日 durable close 后才能 activation 新日。
- source/writer terminal、operator stop 和运行期 coordinated capacity exit 使用
  同一 close identity，不建立第二套 rollover authority；startup 容量 gate 尚未
  创建 raw-day owner/manifest，直接退出 `75`，不属于该 close 流程。

## 6. 完成与延期

除 `current/acceptance.md` 与 `current/known-limitations.md` 明列的未端到端测试和
非 hard-bound transport-control channel 外，当前实现和确定性测试覆盖上述合同。
2026-07-28 事故事项已由用户最终验收；真实 UTC rollover 与完整健康 UTC 日运行证据
不作已取得声明，但不再是完成门。paper/live 真实 external-source adapter 尚未实现、
不属于 `model-train`，并继续保持延期。必须先完成 `model-train`（模型训练模块，不是
`data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，
才另立三件套开发。
