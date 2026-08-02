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

`data-collect` 负责外部市场数据接入、类型化原始数据、实时交接基础类型、数据源覆盖、
缺口、恢复、存储和清单。它不负责标签、特征、训练切分、模型输入加工、订单、撤单、
钱包、NOE、账本、PnL 或交易策略。

| 二进制程序 | 当前能力 |
| --- | --- |
| `data-collect-training-collector` | 连接真实 Polymarket CLOB、Gamma、RTDS 和 Binance，写六类类型化原始数据与每日清单 |
| `data-collect-paper-hot` | 写启动回执，运行本机 Unix 套接字心跳、状态服务端 |
| `data-collect-live-hot` | 写启动回执，运行本机 Unix 套接字心跳、状态服务端 |

只有训练二进制程序当前连接真实外部源。模拟盘和实盘的真实适配器不属于
`model-train`，继续保持延期。必须先完成 `model-train`（模型训练模块，不是
`data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，
才另立三件套开发。

## 2. SDK 与 schema

Polymarket 当前依赖固定为
`polymarket_client_sdk_v2 = "=0.7.0"`，启用的功能为
`clob/gamma/rtds/ws`；工作区只有
`third_party/polymarket_client_sdk_v2-0.7.0-p1` 一个补丁。
未经用户批准，不得改用 Polymarket 原始 REST、手动签名、自建 SDK 或影子实现。

相对 crates.io `0.7.0`，`0.7.0-p1` 的生产 `src/` 差异横跨四个文件：

- `clob/ws/types/response.rs`：保留 `book`、`price_change`、`tick_size_change`、
  `last_trade_price`、`best_bid_ask`、`new_market`、`market_resolved` 的当前
  可空、兼容类型化解析，另含 `MidpointUpdate.timestamp` 的可空兼容处理；并保证数组中
  未知或不关心的元素可跳过、每个关心的元素必须全部成功，否则整帧报错且
  不泄露合法前缀。其中只有九个可空 Decimal 字段的精确空串 `""` 解为 `None`：
  `book.last_trade_price`、`price_change.best_bid/best_ask`、
  `tick_size_change.old_tick_size`、`last_trade_price.size/fee_rate_bps`、
  `best_bid_ask.best_bid/best_ask/spread`；非空畸形值（含空白串）、九字段之外的
  可选 Decimal 空串和必填 Decimal 空串仍报错。
- `rtds/subscription.rs`：提供最大 `2048` 字节、白名单和指纹，并且不回显非白名单
  载荷的 RTDS 解析诊断。只有顶层对象的 `message` 精确等于
  `Network error communicating with endpoint` 且不含
  `topic/type/timestamp/payload` 时，才转为可恢复连接故障；
  带任一目标字段的类目标帧仍是粘性解析器终态。
- `ws/config.rs` 与 `ws/connection.rs`：增加连接超时、可选择是否要求精确文本 `PONG`、
  粘性终态或解析器故障、连接代际、绑定代际的出站围栏、FIFO 与重放、有界重连所有权，
  以及最后一个所有者退出时的清理。其中
  SDK 的出站请求与 `heartbeat PING` 传输控制通道使用 `mpsc::unbounded_channel()`；
  这些通道不具备内存硬上限。

完整补丁范围由上述供应商源代码、`polymarket_sdk_patch_contract` 与
`polymarket_sdk_runtime_contract` 共同拥有；不得
只凭版本名删除、缩小或扩大其中任一行为。

| `source_kind` | 当前新写入 schema |
| --- | --- |
| `polymarket_market` | `polymarket_orderbook_v3` |
| `polymarket_market_metadata` | `polymarket_market_metadata_v5` |
| `polymarket_rtds_chainlink_btc_usd` | `polymarket_rtds_chainlink_btc_usd_v4` |
| `binance_depth_snapshot` | `binance_depth_snapshot_v3` |
| `binance_depth_update` | `binance_depth_update_v4` |
| `binance_aggtrade` | `binance_aggtrade_v7` |

旧 schema 只可用于代码明确保留的精确旧版读取和恢复，不得成为新写入权威，
也不得把六类数据源降级成不透明宽表。

## 3. 存储与容量

- 采集器拥有的承载数据、准入、延后处理以及写入器队列或批次必须有明确硬上限。
  SDK 传输控制无界通道不是数据队列；当前封装器通过 `watch` 合并最新期望状态、固定且
  较小的期望状态集合，以及单个待处理心跳来约束调用量，但这不等同于通道具有容量硬上限。
- UTC 日和小时按采集器接收时间分区。
- 每个数据源和小时通常只有一个最终化 Parquet；仅文件名碰撞时使用 `part-N`。
- `*.parquet.running` 不是最终化产物。
- `50,000` 行或 `16,777,216` 逻辑字节结束行组，不产生新的分片。
- 每个 UTC 日只有一个 `data_collect_daily_manifest_v3`，由单一归属方维护，并直接
  包含一个 `data_collect_collection_day_coverage_v1`。
- 小时变化、UTC 日切换或协调停止会最终化已接纳数据；停止不得伪造完整日。
- 配置的容量上限必须为正整数；CLI 默认值和当前生产单元均为
  `50_000_000_000` 字节，测试或另行批准的配置可以显式覆盖。
- 启动容量扫描若已达到配置上限，则不启动数据源、不创建类型化原始数据、清单或
  运行时产物，直接退出 `75`。
- 运行期间每 `60s` 扫描达到配置上限时，停止新准入、排空并最终化已接纳数据、更新
  适用清单后退出 `75`。采集器不自动删除、移动或改写最终化原始数据。

## 4. RTDS 恢复

RTDS 有效新鲜消息（`fresh`）只接受正确主题、类型、交易标的，时间戳合理且
`recv_time - payload_time` 位于 `-5000ms..=30000ms` 的消息。

- 首次有效新鲜消息前禁用冷启动业务看门狗。
- 曾收到有效新鲜消息后，连续 `300s` 无有效新鲜消息会触发完整运行时替换。
- 新代际的已连接时点与本地应用订阅时点取较晚锚点，`60s` 后仍无首次有效新鲜消息，
  则进入外层退避后再次替换。
- 外层退避基数从 `1s` 开始、每个已消费层级按 `×2` 增加，应用 `±20%` 确定性抖动后，
  单次休眠上限为 `300s`；收到有效新鲜消息后重置为首级。
- 业务替换过程没有独立尝试次数或累计耗时耗尽上限；仍受整体运行时截止时点、操作员停止
  和硬错误边界约束。
- 替换必须先完成旧运行时清理再创建新运行时，严格先断开再连接，不允许两个 RTDS
  套接字并存。
- SDK 传输断开和重连仍由 SDK 归属方处理；过期或主题、类型、交易标的错误的消息不得
  刷新有效新鲜消息看门狗。

## 5. 身份、市场判定、重启与 UTC 日切换

- 市场固定身份信息包含 `slug`、条件标识、CLOB 市场标识、固定 Up、Down 资产顺序、
  开始和结束时间，以及首次验证时间；多重权威或不可变信息不匹配时，必须在准入前
  失败即关闭。
- 市场在 E 进入待判定状态；Gamma 判定后续查询固定为 E+3、E+4、E+5、E+6、E+7、E+8
  分钟六个时间点。
- 所属角色和窗口内的合格 WebSocket 或 Gamma 终态只能关闭一次；第二个 Gamma 终态
  声明、固定身份信息或 schema 矛盾，以及与已有终态胜方冲突时必须失败即关闭。
- 不再匹配当前、下一或待判定角色的 `market_resolved`，以及虽匹配待判定角色但
  `recv_time_utc_ms` 超过市场判定预算时限的迟到事件，当前代码直接忽略，不得误写成一律
  失败即关闭。
- 重启只恢复有界的当前、下一身份信息和仍在窗口内的待判定项；不得从摘要伪造终态或
  重开已结束窗口。
- UTC 日切换先关闭旧日原始数据准入，完成四个生产者围栏、六个写入器最终化和清单会合；
  旧日持久关闭后才能启用新日。
- 数据源或写入器终态、操作员停止和运行期协调容量退出使用同一关闭身份，不建立第二套
  UTC 日切换权威；启动容量门尚未创建原始数据日归属方和清单，直接退出 `75`，不属于
  该关闭流程。

## 6. 完成与延期

除 `current/acceptance.md` 与 `current/known-limitations.md` 明列的未端到端测试和
没有硬上限的传输控制通道外，当前实现和确定性测试覆盖上述合同。
2026-07-28 事故事项已由用户最终验收；真实 UTC 日切换与完整健康 UTC 日运行证据
不作已取得声明，但不再是完成门。模拟盘和实盘的真实外部数据源适配器尚未实现、
不属于 `model-train`，并继续保持延期。必须先完成 `model-train`（模型训练模块，不是
`data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，
才另立三件套开发。
