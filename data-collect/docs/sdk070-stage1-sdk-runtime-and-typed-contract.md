# SDK070 阶段 1：SDK runtime 与 typed contract 证据

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
> 2026-07-30：本文是 SDK070 Stage 1 快照；typed blocked 和后继游标不是当前状态。
>
> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：`changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 状态：`STAGE_1_CLOSED`
>
> 证据日期：2026-07-25

## 1. 结论与范围

阶段 1 当前得到两个彼此独立的终态：

- `SDK_070_RUNTIME_USABLE`
- `SDK_070_TYPED_CONTRACT_BLOCKED`

runtime 终态表示以官方 0.7.0 crate 为底座、仅重放既有四路径 patch 的本地
`0.7.0-p1` vendor 已通过阶段 1 规定的离线构建、测试、Clippy、fmt、依赖树和
路径检查。typed-contract 终态表示七类 public market variant 都存在官方文档允许
missing/null、但 SDK typed parser 在 target admission 前拒绝整帧的路径；它只阻断
v3 schema、reader/writer 与 local new-write 替换，不反向否定 runtime 可用性。

本阶段没有：

- 修改 `data-collect/src`、schema、RTDS 业务逻辑或生产行为；
- 建立 raw WebSocket、raw REST、manual signing、第二解析器或 shadow fallback；
- 运行任务 1.5 的公开网络 smoke；
- 执行 release、部署、远端服务/数据操作、migration 或删除；
- 开始 Stage 2。

三份既存未跟踪 RTDS 草案不属于本阶段 diff。公开只读 smoke 的状态为
`NOT_AUTHORIZED / NOT_RUN`。

## 2. 回滚点与官方 provenance

阶段 0 已发布 checkpoint：

```text
commit: 86d14a9f4225f9b941206b9d47bcfe5a736c7789
branch: origin/fix-rtds-silents
verification: local HEAD == upstream == git ls-remote
```

阶段 1 使用本机 Cargo registry 已缓存的官方 crate，未为取得源码访问公共网络：

```text
archive:
  /home/chillchill/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/
  polymarket_client_sdk_v2-0.7.0.crate
archive_sha256:
  ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a
package_version: 0.7.0
published_at: 2026-07-17T14:56:51Z
license: MIT
license_sha256:
  2ef5f959b834b22b3e2798a5c0f20a4742a9c1bdb85a7171c72603c58a0a0137
rust_version: 1.88.0
vcs_commit: 222143d321eba97d5711a848265eb9aab3bc7ff4
archive_file_count: 132
sorted_archive_file_list_sha256:
  55c093724c71c384cb485cc44bef1cb75d21123035cb8f253308b3d518e0dc74
```

新 vendor 是从该 archive 全新解包后建立的
`third_party/polymarket_client_sdk_v2-0.7.0-p1`，不是旧目录改名。官方未给出的
tag/release object 没有被虚构为 provenance。

## 3. 四路径 patch 与唯一 vendor

官方 0.6.0 与 0.7.0 在以下四个 patch 底文件逐字节相同；阶段 1 从当前
`0.6.0-p1` 相对官方 0.6.0 的 unified diff 逐 hunk 重放到官方 0.7.0。相对官方
0.7.0，新 vendor 源码差异恰好为：

| 路径 | 相对官方 0.7.0 diff stat | patched SHA-256 |
|---|---:|---|
| `src/ws/connection.rs` | `+876/-68` | `90070b2bd438909a44a82209963881b4c70f755bc671ea19b13c7ee2e2d6a223` |
| `src/ws/config.rs` | `+11/-1` | `dc68886999cfd3509c0668f403c6f8953f283f03847b5dbd6ebe6c5b72d20e2d` |
| `src/clob/ws/types/response.rs` | `+26/-21` | `55163a02b9f4c58c0e861043e5b6a109164d6e661aceeb46b24d2dd460ac0073` |
| `src/rtds/subscription.rs` | `+678/-2` | `4e2174a4dcb0e18ad5baf2914ec3270cbff5923915e6dc39ed7f04174fa10e7b` |

没有第五个源码差异路径。原有 patch 语义保持：

- connection generation、bound outbound、handshake timeout、heartbeat 与 normal teardown；
- `Config` 的 `connect_timeout` / `require_text_pong`；
- CLOB interested-array all-or-error 与 sticky parser fault；
- RTDS parser 诊断、subscription/generation owner 合同。

workspace 切换为：

```text
root [patch.crates-io]:
  third_party/polymarket_client_sdk_v2-0.7.0-p1
data-collect version requirement:
  =0.7.0
features:
  clob,gamma,rtds,ws
resolved SDK packages:
  exactly one local polymarket_client_sdk_v2 0.7.0
```

旧 tracked `third_party/polymarket_client_sdk_v2-0.6.0-p1` 在同一 Stage 1 diff
删除；回滚由阶段 0 Git checkpoint 提供，不保留第二 vendor、备份目录或 shadow
fallback。active Cargo/code/bench 中不存在该 SDK 的 0.6 精确版本或旧路径引用；
阶段 0 artifact 和历史 changelog 中的历史引用有意保留。

切换后的关键 workspace 文件 SHA-256：

| 文件 | SHA-256 |
|---|---|
| `Cargo.toml` | `958a8ad78357306090323a0910f8be9d192c6e7ba9f9073c1a1d2937da6a9710` |
| `data-collect/Cargo.toml` | `bdecd2384d1171eb512f5884207208cd64218930b24032c5c4a36b85c3577367` |
| `Cargo.lock` | `ee4dbbffaa68246a53ef71c883bb153c8c8eb47196ecc7704c0eda726a949997` |

## 4. 离线验证矩阵

工具链：

```text
default rustc/cargo: 1.95.0
vendor MSRV toolchain: rustc 1.88.0 / cargo 1.88.0 via +1.88
vendor fmt toolchain: rustc 1.93.0-nightly (2025-11-23)
                     via +nightly-2025-11-24
```

所有测试、check 和 Clippy 均以 `CARGO_NET_OFFLINE=true` 或 Cargo 的
`--offline` 路径完成；没有真实订单、钱包、认证频道或公共网络调用。

| 命令/门 | 结果 |
|---|---|
| `cargo tree -p data-collect --locked -e features -i polymarket_client_sdk_v2 --offline` | 仅本地 `0.7.0`，features 为 `clob,gamma,rtds,ws` |
| vendor `--locked --lib --features clob,gamma,rtds,ws` | `170/170` passed |
| vendor `--locked --all-features` | 全部 unit/integration/doc harness 通过；library `186/186`，doctest `49 passed / 20 ignored` |
| vendor default `--locked --all-targets` Clippy `-D warnings` | exit 0 |
| vendor all-features/all-targets Clippy `-D warnings` | exit 0 |
| fixed-nightly vendor `fmt --all -- --check` | exit 0 |
| `cargo test -p data-collect --locked -- --test-threads=1` | lib `347/347`，全部 integration/doc harness 通过 |
| patch contract / runtime contract（由同一次完整 suite 覆盖） | `17/17` / `19/19` |
| `cargo check -p data-collect --locked --all-targets` | exit 0 |
| `cargo clippy -p data-collect --locked --all-targets -- -D warnings` | exit 0 |
| `cargo fmt --all -- --check` | exit 0 |
| staged / unstaged `git diff --check` | exit 0 |

vendor all-features suite 中官方 0.7 新增/修正的 async execution
`post_order` / `post_orders` mock tests 通过；没有调用真实交易接口。

构建产生的 vendor-local `target/` 已用精确 manifest path 执行 `cargo clean`
清理；Cargo 报告删除 `18,463` 个可再生成文件、约 `26.1 GiB`，随后确认 vendor
目录下没有 `target/`。

## 5. 官方文档与 typed fixture 方法

2026-07-25 对照：

- `https://docs.polymarket.com/market-data/realtime-data`
- 固定 SDK source/CHANGELOG commit
  `222143d321eba97d5711a848265eb9aab3bc7ff4`

临时离线 fixture harness 同时对官方 registry 0.7.0 和当前 patched vendor 调用
`parse_if_interested(bytes, &MessageInterest::ALL)`。每个 case 都分别使用单个 JSON
object 和 `[object]` array envelope。临时 harness 完成后已删除，worktree 没有
留下测试旁路。

共同结果：

- 字段完整的合法 fixture 与 unknown-field fixture：两棵树均
  `OK messages=1`；
- 下面每个官方允许的 missing/null 单 object：官方与 patched vendor 均返回
  parse error；
- 同一非法 element 的 array：官方 0.7.0 返回 `OK messages=0`，即静默跳过
  interested element；patched vendor 将同一 parse error 传播给调用方；
- 四路径 patch 修复了数组静默跳过，但没有添加 typed field，也没有改变这些字段
  的 nullability。

## 6. 字段、nullability 与 parser 矩阵

| variant | 官方允许但 SDK 拒绝整帧的 missing/null | SDK typed field gap |
|---|---|---|
| `book` | `timestamp` | `minOrderSize`、`tickSize`、`negRisk`、`lastTradePrice` |
| `price_change` | `timestamp` | 无额外 single-field gap |
| `last_trade_price` | `timestamp` | `transactionHash` |
| `tick_size_change` | `oldTickSize`、`timestamp` | 无额外 single-field gap |
| `best_bid_ask` | `bestBid`、`bestAsk`、`spread`、`timestamp` | 无额外 single-field gap |
| `new_market` | `question`、`slug`、`description`、asset IDs、`outcomes`、`timestamp`；`event_message`对象存在时，其官方 optional text `ticker/slug/title/description` missing/null | `tags`、`conditionId`、`active`、`clobTokenIds`、`sportsMarketType`、`line`、`gameStartTime`、`orderPriceMinTickSize`、`groupItemTitle`、`takerBaseFee`、`feesEnabled`、`feeSchedule` |
| `market_resolved` | asset IDs、winning asset/outcome、`timestamp`；`event_message`对象存在时，其官方 optional text `ticker/slug/title/description` missing/null | `tags` |

精度边界：

- `event_message` 整体 missing/null 可由 SDK 解析，因为外层是
  `Option + default`；只有对象存在时的 optional 内部文本形成当前 gap。
- `EventMessage.id` 在官方文档与 SDK 中均为 required，不是 gap。
- `MarketResolved.question/slug/description` 已是 `Option`，`outcomes` 也接受
  missing/null，不是当前 parser gap。
- book/price-change venue `hash` 已由用户决定 `EXCLUDED`，不属于 gap，也不能与
  `transactionHash` 混淆。
- authenticated `TradeMessage.transaction_hash` 不得错接到 public market
  `LastTradePrice`。

## 7. 当前消费者与阻断判定

当前 consumer/已批准合同核对的主要代码锚点：

- 五类盘口事件处理：
  `data-collect/src/live_runtime.rs:16056-16198`
- `new_market` admission/identity：
  `data-collect/src/live_runtime.rs:16200-16438`
- `market_resolved` routing/identity/terminal：
  `data-collect/src/live_runtime.rs:11835-11864,12384-12550`
- resolution recovery：
  `data-collect/src/polymarket_resolution_recovery.rs:1318-1442`

七类 variant 的整帧 parser gap 都发生在 target admission 之前；因此不能用
Parquet nullable 值、`EXCLUDED` 或 `nullable_unavailable` 恢复已经丢失的消息。
它们直接触发需求的 `SDK_070_TYPED_CONTRACT_BLOCKED` 停止条件。

single-field gap 的当前审计结果：

| gap | 当前分类 |
|---|---|
| book 四个扩展字段 | optional；当前 WS-v3 无已识别 executable consumer，tick/min-size 等现由 Gamma metadata owner 提供 |
| `last_trade_price.transactionHash` | 当前无非空 producer/语义 reader；v2 有 nullable slot 和 synthetic constructor test，仍须用户逐字段决定 |
| new-market 扩展字段 | optional；当前 WS-v3 无已识别 executable consumer，active/tick/min-size/fee 等现由 Gamma metadata owner 提供 |
| resolved `tags` | optional；当前 identity/terminal/recovery 无消费者 |

这些 single-field gap 只能由用户逐项选择：

1. 要求官方 typed SDK 支持，未满足前继续阻断；
2. `EXCLUDED`；
3. `nullable_unavailable`。

即使所有 single-field gap 都获批排除或 nullable，七类整帧 parser blocker 仍必须
先通过官方 SDK-first 路线修复，v3 才能解除阻断。future paper/live 是
`NONBLOCKING_FUTURE_SPEC`，不能创造新的必需字段或阻断项。

## 8. 阶段游标

```text
Stage 0: CLOSED_AND_PUBLISHED
Stage 1 implementation: COMPLETE
Stage 1 independent review: PASS (Critical 0 / Important 0 / Minor 0)
Stage 1 commit/push: AUTHORIZED / CLOSED_IF_THIS_RECORD_COMMIT_IS_PUSHED_AND_REFS_MATCH
SDK runtime: SDK_070_RUNTIME_USABLE
typed contract: SDK_070_TYPED_CONTRACT_BLOCKED
public SDK smoke: NOT_AUTHORIZED / NOT_RUN
Stage 2: NOT_STARTED
v3 schema/writer/reader/local new-write: BLOCKED
```

## 9. 2026-07-25 post-closure P-B1 addendum

本节是Stage1闭合后的独立前置记录，不回写第1至8节的历史fixture、hash、审查或
`SDK_070_TYPED_CONTRACT_BLOCKED`结论。

用户随后批准`P-B1`以官方SDK-first方式修复七类missing/null整帧decode，并冻结：

- nullable标量/文本使用`Option`；三个nullable数组使用
  `DefaultOnNull -> empty Vec`；
- current-v2在canonical/persist/claim/state之前对旧必填shape fail closed；
- F04 `book.lastTradePrice`最终为`INCLUDED_OPTIONAL`，映射为
  `BookUpdate.last_trade_price: Option<Decimal>`；F01–F03与F05–F18保持
  `EXCLUDED`；
- F04只表示book快照时已知的最近成交价，不替代或合成独立
  `last_trade_price`成交事件。present值进入book canonical/raw-equivalent JSON，
  missing/null省略新key并保持旧current-v2 canonical/fingerprint/row语义；
- 公开network smoke仍为`NOT_AUTHORIZED / NOT_RUN`，所以实际目标市场首次订阅与
  重连book的F04填充率未知。

P-B1没有新增第五个vendor生产源码路径。当前四个生产差异路径及SHA-256为：

```text
src/ws/connection.rs
  90070b2bd438909a44a82209963881b4c70f755bc671ea19b13c7ee2e2d6a223
src/ws/config.rs
  dc68886999cfd3509c0668f403c6f8953f283f03847b5dbd6ebe6c5b72d20e2d
src/clob/ws/types/response.rs
  9bd05887b9190449f75f5cda623a5f2e465c07033900fb1dabf89b72ab2b6aba
src/rtds/subscription.rs
  4e2174a4dcb0e18ad5baf2914ec3270cbff5923915e6dc39ed7f04174fa10e7b
```

公开type直接适配的两个non-production conformance文件为
`tests/websocket.rs`
（`0cadd63b07c2d5a6033f83dbca0c127dc90d71a0c88e38babdeea56e98e182eb`）
和`examples/clob/ws/orderbook.rs`
（`1df005dfbd0d1735ee102323283cf5c9fd644b6a97fda94a57a79f073d60c55c`）。

当前P-B1代码、最终本地门及独立审查已经完成，审查结果为
`PASS (Critical 0 / Important 0 / Minor 0)`。用户随后批准发布；closure commit
`d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`已推送到
`origin/fix-rtds-silents`，且本地HEAD、upstream与远端ref一致。因此
`P_B1_CLOSED`与`SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`已经成立。
该闭合本身不等于public smoke、RTDS参数或4A/4B获得授权；用户随后于
2026-07-25授予本事项持续执行/发布授权，Stage3本地实现、测试、审查、commit和
push不再逐次申请授权。
