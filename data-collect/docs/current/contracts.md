# data-collect 当前行为参考

> 生命周期：`CURRENT_REFERENCE`
>
> 当前执行 authority：`none`
>
> 当前规范来源：`plan/data-collect-plan.md`
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

本文是从主计划和真实实现派生的非权威参考，不得新增或改变计划语义；出现不一致时按
`data-collect/docs/CURRENT.md` 的冲突门处理。

## 1. 模块边界

`data-collect` 负责外部市场数据接入、类型化原始数据、实时交接基础类型、数据源覆盖、
缺口、恢复、存储和清单。它不负责标签、特征、训练切分、模型输入加工、订单、撤单、
钱包、NOE、账本、PnL 或交易策略。

消费者边界中的历史名称 `ConsumerKind::Training` 明确表示下游 `model-train`，不是
`data-pipeline(training)` 或 `data-collect-training-collector`。它不得直接读取
`typed_raw` Parquet，必须消费 `data-pipeline` 输出；`ConsumerKind::DataPipeline` 是离线
训练路径中唯一直接读取最终化 `typed_raw` 的消费种类。本边界不改变采集器写入内容、
Parquet schema 或每日清单 schema。

`model-train` 进程仍可直接读取 `manifest`、`source_coverage`、`source_health`、
`source_gap`、`reconnect_trace`、`backpressure_state` 六类 `data-collect` 角色，但这些
角色只用于血缘、审计和质量诊断。它们不得参与训练准入或放行，不得进入 Observation、
特征、标签、训练样本、训练切分、样本选择、模型输入或任何决策触发；版本化
`data-pipeline` 输出是训练数据和模型输入的唯一来源。

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

以上是当前供应商源代码、`polymarket_sdk_patch_contract` 与
`polymarket_sdk_runtime_contract` 的证据摘要；规范归属仍是主计划，不得只凭版本名推断
补丁行为。

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

模拟器低频平台规则不属于当前采集 schema：CLOB `min_order_size`、完整
`feeSchedule`、`itode` 由 `sim-system` 面向当前平台时点的新运行在首个业务请求前通过固定的
官方公开 HTTPS 窄加载器查询并冻结；历史运行只绑定适用时点合法的 `HISTORICAL_AS_OF`
快照，不得查询当前平台回填。当前查询响应缺少必需字段、字段非法、身份不一致或来源冲突时，
由模拟器事项失败即关闭。首版未消费的 `oas` 只进入未消费字段清单，也不属于采集任务。
该模拟器例外不授权 `data-collect` 使用原始 REST，且不改变本节前述官方 Rust SDK 合同。
当前 metadata v5 没有这些规则专用列，也不为此新增。其
`source_evidence_payload_json` 中的 `orderMinSize`、`feesEnabled`、旧手续费及其他同名或
相似键仍是采集时点的 Gamma 原始证据；数值可能与模拟器快照相同，但证据时间、来源和用途
不同，不构成可删除的语义冗余。metadata v5 的 `source_evidence_payload_json` 中的
`orderPriceMinTickSize` 是采集时点的初始 tick 源证据，`polymarket_orderbook_v3` 的
`old_tick_size/new_tick_size` 表达运行中
`tick_size_change`。两者都必须由 `data-pipeline` 进入规范事件链，不属于也不得被
`sim-system` 冷启动规则快照替代。

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
- 只读空间反事实仅覆盖 `/home/chillchill/new-datasets` 中 UTC 日不早于 2026-07-30 的
  2026-07-30 至 2026-08-02 四日：491 个 Parquet、151,056,246 行、9,202,902,694 字节；
  metadata 为 97 个 v5 文件、4,620 行、4,119,473 字节。当前没有规则专用列，删除收益为
  0 字节；从 1,167 个 Gamma 证据载荷同时删除十个同名或相似键的 ZSTD-3 反事实约为
  9,317 字节（metadata 约 0.226%，全数据约 0.000101%），但会破坏载荷和关联摘要，故禁止
  以节省空间为由删键或重写既有数据。

上述十个名称都是 `source_evidence_payload_json` 内的 Gamma 原始 JSON 键，不是 metadata v5
的类型化专用列。逐键反事实如下；“估算压缩字节”只表示从这些载荷移除单键后以相同 ZSTD-3
条件重压的近似差额，不能相加后当作可安全回收空间：

语义上必须再分三类：`orderMinSize`、`feesEnabled`、`negRisk` 是与冷启动快照直接对应但
来源时点不同的三个重叠候选；`orderPriceMinTickSize` 是只供上游规范事件链建立初始 tick
的采集证据，不是冷启动规则。`makerBaseFee`、`takerBaseFee`、
`makerRebatesFeeShareBps` 只是名称或主题近似，不能替代当前手续费/返佣参数；
`secondsDelay`、`enableOrderBook`、`rfqEnabled` 不属于本次可执行配置，只为完整说明载荷中的
近似市场规则键及空间上限而纳入反事实。不得把十键合计误写成十个真正重复字段。

| Gamma 原始 JSON 键 | 四日样本中的值 | 未压缩逻辑字节 | 估算压缩字节 | 与冷启动配置的关系及删除判断 |
| --- | --- | ---: | ---: | --- |
| `orderMinSize` | 1,167 项均为字符串 `"5"` | 22,173 | 1,070 | 名称和值可能与 `min_order_size_shares` 重叠，但不是权威 CLOB `mos`；保留原始证据 |
| `orderPriceMinTickSize` | 1,158 项为 `"0.01"`，9 项为 `"0.001"` | 36,186 | 2,054 | 采集时点的初始 tick 源证据；须经 `data-pipeline` 进入规范事件链，不属于模拟器冷启动规则；保留原始证据 |
| `feesEnabled` | 1,167 项均为 `true` | 22,173 | 1,245 | 是冷启动需要交叉绑定的 Gamma 值，但采集时点不同；保留原始证据 |
| `makerBaseFee` | 1,167 项均为 `1000` | 23,340 | 797 | 旧 Gamma 手续费键，不等于 CLOB V2 `fd` 或完整 `feeSchedule`；不纳入可执行配置，保留证据 |
| `takerBaseFee` | 1,167 项均为 `1000` | 23,340 | 1,152 | 旧 Gamma 手续费键，不等于 CLOB V2 `fd` 或完整 `feeSchedule`；不纳入可执行配置，保留证据 |
| `makerRebatesFeeShareBps` | 1,167 项均为 `10000` | 37,344 | 2,940 | 名称近似返佣，但不是当前 `feeSchedule.rebateRate`；不得替代或删除 |
| `secondsDelay` | 1,167 项均为 `null` | 23,340 | 1,852 | 体育/赛事秒级延迟字段，不是 `itode`；本项目不消费，仍保留原始证据 |
| `negRisk` | 1,167 项均为 `false` | 18,672 | 875 | 与冷启动 Gamma `negRisk` 同名但采集时点不同；CLOB `nr` 仅在响应自然提供时作诊断，不作为规则来源；保留原始证据 |
| `enableOrderBook` | 1,167 项均为 `true` | 26,841 | 1,095 | 市场接单/生命周期事实，不是静态模拟规则；保留原始证据 |
| `rfqEnabled` | 1,167 项均为 `false` | 22,173 | 1,118 | RFQ 不在首版范围，且该值仍是采集时点证据；保留原始证据 |
| **十键合计反事实** | 1,167 个载荷 | **255,582** | **约 9,317** | 需改写载荷及其 SHA-256，安全可删除量仍为 **0 字节** |

完整 `feeSchedule`、`feeSchedule.rebateRate`、`itode` 和 `oas` 在这些 v5 载荷中均不存在；因此
它们与采集数据的字段重叠数和删除收益都为 0。公开成交中的 `fee_rate_bps` 在四日范围内有
2,039,650 个非空值且均为字符串 `"0"`，但它是逐成交事件证据，不是低频
`feeSchedule`，不属于本反事实删除集合。

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
没有硬上限的传输控制通道外，当前实现和确定性测试覆盖上述行为参考。
2026-07-28 事故事项已由用户最终验收；真实 UTC 日切换与完整健康 UTC 日运行证据
不作已取得声明，但不再是完成门。模拟盘和实盘的真实外部数据源适配器尚未实现、
不属于 `model-train`，并继续保持延期。必须先完成 `model-train`（模型训练模块，不是
`data-collect-training-collector`）；之后仍只有在用户决定返回 `data-collect` 时，
才另立三件套开发。
