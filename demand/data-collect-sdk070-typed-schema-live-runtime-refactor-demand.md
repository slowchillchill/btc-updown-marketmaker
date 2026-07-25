# data-collect SDK 0.7.0、typed schema、RTDS 恢复与 live runtime 模块化总需求

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应计划：`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：`changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 纳入的 RTDS 专项三件套：`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`、`plan/data-collect-rtds-business-silence-full-reconnect-plan.md`、`changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
>
> 状态：`APPROVED_STANDING_EXECUTION_P_B1_CLOSED_2_RTDS_CLOSED_STAGE3_CLOSED_STAGE5_CLOSED_STAGE6_REVIEW_PASSED_AWAITING_PUBLISH`。阶段0已以commit
> `86d14a9f4225f9b941206b9d47bcfe5a736c7789`闭合并发布，阶段1已以commit
> `b818e436a115eb61859fa281773342ee58f54f6a`闭合并发布；runtime终态为
> `SDK_070_RUNTIME_USABLE`。阶段1闭合时的typed终态为
> `SDK_070_TYPED_CONTRACT_BLOCKED`；用户随后批准并发布`P-B1`修复路线，最终字段裁决为
> F04 `INCLUDED_OPTIONAL`、F01–F03与F05–F18 `EXCLUDED`。本地实现、最终门及
> 独立审查已通过，closure commit
> `d6a87fdaf0a751d6b7c4196aeb936b4c5de4c094`已推送且本地HEAD、upstream与远端
> ref一致，故`P_B1_CLOSED`与
> `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`已经成立。
> 用户于2026-07-25批准继续推进，阶段2任务
> 2.1/2.2共同基线已落盘到
> `data-collect/docs/sdk070-stage2-observable-and-owner-baseline.md`并完成自审；
> `2-PM`四个小步均已完成：step 1/2/3通过实现与自审，step 4确认既有resolution
> recovery已是唯一owner且无需再重构。`2-PM`分支收口完整门与独立审查均已通过，
> 用户已另行批准`2-PM` closure commit/push；`2-PM_CLOSED`以承载本记录的commit
> 成功推送且本地HEAD、upstream与远端ref一致为生效条件。六源共享dispatcher继续
> 留在root。`2-RTDS`唯一owner机械移动、完整/静态门及独立审查已通过；closure
> commit `b7f83704cf2176de61573c6b11d792df73d68b53`已推送，且本地HEAD、
> upstream与远端ref一致，故`2-RTDS_CLOSED`成立。Stage3入口独立只读审计为
> `PASS`；任务3.0已只同步future Polymarket source-v3 payload/fingerprint/v2 raw
> projection现行authority。`STAGE3_AUTHORITY_SYNC_CLOSED`以承载本记录的commit
> 成功push且三方ref一致为生效条件。用户明确批准本事项持续推进直至
> 完成，不再逐阶段申请本地
> 实现、测试、审查、commit或push授权；只有发生上下文冲突或实际情况与需求/计划
> 不一致时才停止请求决策。该持续授权满足阶段3等后继本地执行门，但不替代尚未
> 冻结的RTDS参数，也不把production activation/cutover、release、部署、远端操作、
> migration和删除纳入本计划。既有`market_resolved` validate-first与
> `METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`决定继续有效。
> Stage3任务3.1至3.7、S-B/M-B/W-B修复、本地门与重复独立审查均已通过；
> `STAGE3_CLOSED`以承载本记录的commit成功push到`origin/fix-rtds-silents`且本地
> HEAD、upstream与远端ref一致为生效条件。该闭合只表示本地current new-write
> 调用图已原子切换为source v3 + metadata v4，不授权production activation/cutover。
> 2026-07-26用户批准Stage5四项authority修正：queue收敛为真实bounded
> channel + global row/owned-byte budget + bounded wait/观测；训练文件采用
> hourly-only rollover；训练/paper/live采用精确admission时间语义；Polymarket
> current/next/T-30不扩张到连续Binance/RTDS source。U01–U04b的程序决策门已经
> 解除，均进入`DELETE_PROVEN_REPLACED_BEHAVIOR`实施与验证；该删除不改变RTDS
> 专项参数/行为，也不授权production、远端或数据删除。
> Stage5首次独立终审随后发现两个既有C簇遗漏，以及两套新的public/test-only
> shadow authority。用户批准`U05-A + U06-A`：删除逐queue hard-threshold
> pause/resume控制面，只保留真实bounded/inflight owner与纯观测/兼容DTO；删除
> 未接production的有限health/reconnect circuit模型，把唯一生产所需的确定性
> capped+jitter delay收窄到现有source-local runtime helper。该批准不改变真实
> source恢复、manifest兼容字段或全局hard-resource threshold。
> Stage5实现、最终package/check/clippy/fmt/diff门及两路独立
> `gpt-5.6-sol`/Ultra终审均已通过，最终终审为Blocker 0 / Important 0 /
> Minor 0；终审发现的pending map key与source-authority `Vec` backing
> owned-byte漏算已修复并复审通过。Stage5 closure commit
> `9e0d264058ed64cd63fbd05280f0826afaba0509`已push，且本地HEAD、upstream与远端
> ref一致，故`STAGE5_CLOSED`成立。Stage6已完成v3 AC映射、Stage3/Stage0证据消费、
> canonical/finalized artifact parity与最终报告，当前等待独立阶段终审。用户于
> 2026-07-26批准方案A：Stage6 6.3.1要求七类target-admitted fixture的canonical
> round-trip，以及Stage3另一个固定deterministic fixture经正式writer产生的
> finalized v3 Parquet artifact；两套fixture不得混同，后者也不得冒充production
> real artifact。首次真实v3 forward run继续属于范围外后继三件套。两路独立
> `gpt-5.6-sol`/Ultra终审均为`PASS / CLOSURE_UNBLOCKED`，Blocker 0 /
> Important 0 / Minor 0。

## 一、决策结论与目标

本事项冻结以下总方向：

1. 执行顺序上先以阶段0“官方0.6.0底座的本地`0.6.0-p1` vendor”为历史基线，升级到“官方0.7.0底座、Cargo package version仍为`0.7.0`的本地`0.7.0-p1` vendor”；Stage1当前worktree已完成该切换，并独立验证provenance、现有四文件patch、依赖树、编译和确定性回归。公开只读能力只在任务1.5另行获批后验证，当前为`NOT_AUTHORIZED / NOT_RUN`。升级本身不得与schema、RTDS行为或模块拆分混合。“先升级”是v3和依赖该runtime的RTDS实现前置，不是文档、静态证据审计或SDK无关机械拆分的全局硬阻塞。
2. 对 `source_kind=polymarket_market` 建立下一版完整 typed message schema，删除持久化列 `source_native_payload_json` 和 Polymarket venue validation `hash`，并停止 `PriceChange` 的逐 change 行 fan-out；一条通过现行 target-market admission 的 SDK message projection 对应一条 typed row。
3. “无损”精确定义为：在官方 SDK 成功解码并执行现行 target-market admission 后，对本事项当前可执行消费者和既有已批准合同所需的 retained业务字段、数组顺序、decimal文本精度、source/receive time、market/asset identity和collector provenance无损。当前硬门覆盖 collector canonical domain、v3 writer/reader、resolution recovery、manifest和training cold-consumer contract；future paper/live映射只是非阻断静态兼容附件，不能自行创造必需字段或冒充真实消费者。无损不包含被现行 admission排除的非目标entry、外部metadata join字段、原始WebSocket bytes或用户明确排除的venue `hash`。若要求逐字段重建过滤前SDK message，则不能同时维持当前范围并删除这些字段；本事项选择目标消费语义无损，而不是source-native全量归档。
4. manifest/finalized-file SHA-256、artifact row/byte facts和原子发布校验继续保留。平台 `hash`、当前内存 SHA-256 dedupe fingerprint、manifest file SHA-256 是三种不同对象，不得因名称相似一起删除。
5. `data-collect/src/live_runtime.rs` 按真实 owner 与职责机械拆分；先冻结共同可观察合同，再把最小 Polymarket owner、最小 RTDS owner和其余重构作为三个独立分支。v3只依赖 Polymarket分支收口，RTDS只依赖RTDS分支收口，其余重构不得阻塞二者；死代码仅在对应替代owner完成或不可达证明成立后删除。
6. 将 `demand/data-collect-rtds-business-silence-full-reconnect-demand.md` 作为本总事项的 RTDS 专项合同纳入执行顺序，但保留其 `DRAFT_PARAMETER_FREEZE_PENDING` 状态、参数冻结、authority 同步、SDK teardown 能力门和独立授权边界。本总需求不把该草案静默升级为已批准 authority。
7. `METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`：用户已批准新增`polymarket_market_metadata_v4`作为未来current metadata schema；它复用既有`polymarket_market_metadata` artifact、writer和manifest mapping，保留metadata v3的五种row kind与既有字段语义，只追加两个nullable WS claimant外键字段。metadata v3 schema/version立即冻结且不再追加字段，activation前仍承载current metadata new-write，activation后不再由current runtime new-write；不新增artifact、sidecar、digest或第二writer。公开offline legacy writer仍可用于历史fixture、兼容测试和本事项bounded A/B，但不得接入current runtime或canonical production root。

最终目标不是单纯减少文件行数，而是同时做到：

- 未来采集不再为已经结构化保存的数据重复持久化整段 SDK 重序列化 JSON 和无消费者的 venue `hash`；
- v3 writer/reader只使用一个canonical domain；future typed handoff只保留非阻断静态兼容附件，不在本事项实现；
- training与resolution recovery不依赖被删除列，future paper/live静态附件也不得把被删除列重新设为依赖或阻断当前v3；
- `live_runtime.rs` 只保留 facade、配置和顶层 composition，不再要求 agent 读取五万行单文件；
- 只删除被生产调用图、公共 API、测试、历史 reader 和文档共同证明为无消费者或已被替代的代码。

### 1.1 简单、准确、可靠的实现约束

- **简单**：复用官方SDK、现有source/schema/writer/budget/manifest owner边界和项目已有测试工具；Stage3只在唯一owner建立最小v3 canonical domain type。单次使用逻辑保持局部，不新建generic source/plugin framework、第二canonicalizer、shadow compatibility path、无第二消费者的配置项或通用资源追踪器。
- **准确**：每个保留字段必须具有唯一的“SDK typed field或collector metadata -> canonical domain -> Parquet/current consumer”路径；future handoff仅维护非阻断静态对应关系。不得猜测缺失字段、改变数组顺序/nullability/decimal文本/时间语义，或预先实现第二套排序、explode、过滤和去重。文档、代码、数据或SDK语义冲突时停止对应工作并报告。
- **可靠**：只校验有真实故障模式、authority 或消费者依赖支持的不变量，并在唯一 trust boundary/owner 验证一次。小型机械迁移只运行定向测试、受影响跨模块测试、compile/check和主代理自审；完整确定性回归与独立阶段审查只在阶段收口执行。高风险 coordinator/day/manifest/shutdown 迁移可追加一次完整门，但不得按每个小移动重复。
- **可验收**：不得用固定 LOC、固定测试数量、无数据依据的性能百分比或重复全量门冒充正确性。验收以唯一 owner、合同映射、目标测试、阶段回归、artifact事实和差异审查为准。

## 二、已核实基线

### 2.1 存储问题成立

用户指定文件：

`/home/chillchill/new-datasets/2026-07-22/typed_raw/utc_day=2026-07-22/source_kind=polymarket_market/polymarket_market_01.parquet`

基于 Parquet metadata 汇总全部 row group 的 compressed column-chunk size：

| 项目 | 数值 |
|---|---:|
| 文件大小 | `269,203,640` bytes |
| 行数 | `4,047,003` |
| `source_native_payload_json` compressed size | `140,671,726` bytes，文件的 `52.2548%` |
| `hash` compressed size | `92,754,899` bytes，文件的 `34.4553%` |
| 两列合计 | `233,426,625` bytes，文件的 `86.7101%` |

对同一 UTC 日 24 个 `polymarket_market` 文件的汇总结果约为 `86.6232%`。这证明问题不是单个 row group 的偶然波动；若按现状长期采集，存储、备份、校验扫描和网络同步成本会随数据量线性放大。

当前 `source_native_payload_json` 不是平台原始 wire JSON，而是 `live_runtime.rs` 在官方 SDK 已经解码后重新构造并序列化的 canonical JSON。当前代码又把该 JSON 复制到 fan-out 后的每一行，因此它既不是原始字节证据，也造成重复持久化。

当前独立 `hash` 是 Polymarket book/price-change 消息携带的平台校验值。现有模型输入、post-write consumer、paper/live observation 和 resolution recovery没有被核实到必须消费它。它与 `transaction_hash` 不同；但当前官方 SDK 0.7 public `LastTradePrice` 也没有官方实时文档列出的 `transactionHash` typed field。该字段是已知 SDK gap，不能被写成“v3 已可保留”，也不能从 authenticated `TradeMessage` 错接到 public market artifact。

### 2.2 当前依赖确实存在，但属于可替代设计

当前代码和 authority 把 `source_native_payload_json` 用于：

- fan-out 前 SHA-256 结构去重；
- collector 重启时从 finalized Parquet projection 恢复 fingerprint；
- resolution restart recovery 的辅助路由与交叉校验；
- 补齐当前扁平 typed layout 丢失的 `spread`、market text、outcomes、event message 等字段。

对应位置包括：

- `demand/data-collect-reliability-repair-demand.md` 第 61 行附近；
- `data-collect/src/live_runtime.rs` 的 `polymarket_source_native_payload_json()`、fingerprint 和 restart projection；
- `data-collect/src/polymarket_resolution_recovery.rs` 的 `RAW_PROJECTION` 与 raw validation；
- `data-collect/src/polymarket_orderbook_layout.rs` 的 v2 row/schema。

这些依赖证明“现在直接删列会坏”，不证明“必须永久保留该设计”。本事项要求先用完整typed message、typed-only recovery和current consumer替代这些依赖，并记录非阻断future typed handoff静态附件，再删除列和旧路径；不冒充已实现hot runtime。

`USER_DECIDED_REMOVE_COLLECTOR_CROSS_PROCESS_DEDUPE`：采集层不再把“SDK 解码后字段完全一致”当作跨进程丢弃消息的依据。2026-07-22 完整日 manifest 已记录 `polymarket_market.structural_dedupe_dropped_events=14,664`；这证明删除现有 fingerprint admission 会有意改变事实序列，而不是零语义影响。已被 source owner按现行 target-market admission接纳的重复 platform delivery 必须按到达顺序保留；项目内部重试仍不得额外制造第三条记录。现有v2文件只保存post-dedupe accepted rows，manifest只保存被丢事件总数，不能恢复这`14,664`条消息的字段、顺序或resolution影响。因此：

- 重复platform delivery与内部retry语义只用确定性fixture验收；
- 代表性bounded fixture只对同一target-admitted logical projection输入做v2/v3 schema、storage、resource、canonical-domain、resolution和manifest A/B；现有完整UTC日只保留v2 post-dedupe schema/storage/resource/manifest事实基线，不生成v3语义对照；
- `14,664`只作为不可恢复的历史aggregate披露，不得声称完成pre-dedupe full-day replay或精确row/event差异复原；
- 不为补齐该缺失输入新增录制器、sidecar、双写、持久化JSON/digest或shadow replay path；首次真实v3完整日前向观测属于未来operational cutover/长跑事项，不是本事项硬门。

future mapping只做独立非阻断静态审查，不进入full-day运行。不得恢复持久化 JSON/digest、重建 collector semantic key或重新请求该已决定事项。

### 2.3 SDK 0.7.0 的阶段0基线与阶段1核验结论

- 阶段0发布checkpoint时，workspace精确依赖`polymarket_client_sdk_v2 = 0.6.0`，vendor路径为`third_party/polymarket_client_sdk_v2-0.6.0-p1`；这是历史基线，不再是当前依赖身份。
- Stage1当前worktree精确依赖`polymarket_client_sdk_v2 = 0.7.0`，唯一vendor路径为`third_party/polymarket_client_sdk_v2-0.7.0-p1`。旧tracked vendor已在同一Stage1 diff删除，active Cargo/code/bench中不存在Polymarket SDK旧路径或0.6精确版本引用。
- 官方 `0.7.0` 于 2026-07-17 发布；crate SHA-256 为 `ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a`，VCS commit 为 `222143d321eba97d5711a848265eb9aab3bc7ff4`，MSRV 为 Rust `1.88.0`。
- 官方 0.7.0 主要增加 CLOB async execution 下 `post_order` / `post_orders` 对 transaction hashes 的 best-effort 回填，修复 `tradeIDs` 和 pending transaction hash 反序列化，并调整 Tokio feature。当前 collector 不调用下单路径，但仍需保留上游相关 mock tests。
- 官方 0.6.0 到 0.7.0 之间，当前项目 patch 的四个 SDK 源文件没有上游文本变化，因此可以无冲突重放；这不等于可以删除 patch。当前连接 generation、sticky parser fault、handshake timeout、RTDS PONG 策略、CLOB interested-array all-or-error 与 RTDS parser 诊断仍依赖项目 patch。
- 阶段0 docs-only审计中越权运行的`/tmp`候选`170/170`继续只作为历史事件，不计入验收。Stage1已在真实切换后的vendor/workspace重新运行计划规定的完整离线矩阵：vendor core `170/170`、all-features全部harness、两组Clippy、固定nightly fmt及data-collect完整串行suite、check/Clippy/fmt均通过；runtime终态为`SDK_070_RUNTIME_USABLE`。精确命令与结果见`data-collect/docs/sdk070-stage1-sdk-runtime-and-typed-contract.md`。
- 官方 0.7.0 的 CLOB market WS/RTDS typed message并未因本次release自动补齐当前官方实时数据文档中的全部字段。升级和typed-schema能力是两个独立门；Stage1 fixture和consumer矩阵已把typed终态确定为`SDK_070_TYPED_CONTRACT_BLOCKED`。

已知能力差距不是未来才检查的假设。除“SDK struct没有某字段”外，官方文档允许missing/null而SDK 0.7 struct要求非空时，官方合法帧可能在target admission之前反序列化失败，这同样是parser-level SDK gap：

| public market variant | 当前官方 realtime docs 字段/nullability | SDK 0.7 typed struct | 当前状态 |
|---|---|---|---|
| `book` | `timestamp`可missing/null；另有optional `minOrderSize`、`tickSize`、`negRisk`、`lastTradePrice` | `BookUpdate.timestamp: i64`必填，且无四个optional扩展字段 | `SDK_NULLABILITY_PARSE_GAP + SDK_FIELD_GAP` |
| `price_change` | `timestamp`可missing/null | `PriceChange.timestamp: i64`必填 | `SDK_NULLABILITY_PARSE_GAP` |
| `last_trade_price` | `timestamp`与`transactionHash`可missing/null | `LastTradePrice.timestamp: i64`必填且无`transactionHash`；只有排除在本artifact外的authenticated `TradeMessage`有transaction hash | `SDK_NULLABILITY_PARSE_GAP + SDK_FIELD_GAP`，不得错接user-channel字段 |
| `tick_size_change` | `oldTickSize`、`timestamp`可missing/null | `TickSizeChange.old_tick_size: Decimal`和`timestamp: i64`必填 | `SDK_NULLABILITY_PARSE_GAP` |
| `best_bid_ask` | `bestBid`、`bestAsk`、`spread`、`timestamp`均可missing/null | `BestBidAsk`四项均为必填`Decimal/i64` | `SDK_NULLABILITY_PARSE_GAP` |
| `new_market` | question/slug/description/tokenIds/outcomes/timestamp可missing/null；`eventMessage`整体missing/null可用，但对象存在时其optional ticker/slug/title/description可missing/null；另有tags/condition/active/token/sports/line/game/tick/fee扩展字段 | `NewMarket`把多项基础字段、数组、timestamp及`EventMessage`对象内文本设为必填，并缺少所列扩展字段；`EventMessage.id`在文档与SDK均为required，不是gap | `SDK_NULLABILITY_PARSE_GAP + SDK_FIELD_GAP` |
| `market_resolved` | tokenIds/winningTokenId/winningOutcome/timestamp可missing/null；`eventMessage`整体missing/null可用，但对象存在时其optional ticker/slug/title/description可missing/null；tags可missing/null | `MarketResolved`把asset IDs、winner和timestamp设为必填，`EventMessage`对象内文本必填，且无tags；question/slug/description已为`Option`，outcomes接受missing/null | `SDK_NULLABILITY_PARSE_GAP + SDK_FIELD_GAP` |
| book/price change venue `hash` | 文档与 SDK均存在 | SDK可解码 | `USER_DECIDED_EXCLUDE`，不属于待补 gap且不得再次请求确认 |

2026-07-25已用完全按官方文档构造的missing/null fixture直接验证官方upstream 0.7与当前四文件patch parser。七类variant的单object在上述合法missing/null输入上均被两者拒绝；同一element放入array时，官方0.7静默返回`OK messages=0`，当前patch传播同一parse error。四路径patch因此只消除了数组静默跳过，没有补字段或放宽nullability。整帧拒绝发生在target admission之前，不能降级成普通nullable Parquet值，也不能由`EXCLUDED`或`nullable_unavailable`恢复，故typed终态为`SDK_070_TYPED_CONTRACT_BLOCKED`。该终态只阻断v3 schema及其reader/writer/local new-write替换；SDK runtime验证、与该字段无关的机械拆分、死代码证据审计和已独立获批的RTDS工作仍可按各自依赖继续。future paper/live静态附件本身不创造必需字段或阻断门。book四个扩展字段、`LastTradePrice.transactionHash`、new-market扩展字段与resolved tags仍须用户基于当前真实消费者逐项决定为“要求官方typed支持、`EXCLUDED`或`nullable_unavailable`”；即使全部获批降级，也不能绕过整帧parser blocker。

官方证据：

- `https://github.com/Polymarket/rs-clob-client-v2/pull/84`
- `https://github.com/Polymarket/rs-clob-client-v2/pull/83`
- `https://github.com/Polymarket/rs-clob-client-v2/blob/222143d321eba97d5711a848265eb9aab3bc7ff4/CHANGELOG.md`（固定commit `222143d321eba97d5711a848265eb9aab3bc7ff4`）
- `https://docs.polymarket.com/market-data/realtime-data`（未版本化current page，Stage1实际访问日期：2026-07-25；字段/nullability fixture结果冻结在阶段1artifact，不能把未来页面漂移冒充本次证据）

上述结论是Stage1闭合时的历史事实。2026-07-25用户随后批准`P-B1`：

- vendor生产源码继续只允许既有四个差异路径；typed/nullability补丁只落在既有
  `src/clob/ws/types/response.rs`；
- nullable标量与文本使用`Option`，不得用`0`、空串、零decimal或receive time伪造；
- `NewMarket.asset_ids/outcomes`与`MarketResolved.asset_ids`沿用SDK既有
  `DefaultOnNull -> empty Vec`合同；missing、null与显式空数组对target identity均
  作为不完整集合处理；
- `MidpointUpdate.timestamp`只因其直接复制`BookUpdate.timestamp`而在同一
  `response.rs`内保持同型`Option<i64>`，不新增第五个vendor生产源码路径；
- current source v2在生成canonical payload、fingerprint、row、terminal claim或
  改变业务状态前，对过去由SDK parser强制必填、现改为nullable的字段fail closed；
  完整旧fixture的v2 schema、payload、fingerprint、admission和持久化语义保持；
- 不增加raw WebSocket/REST、手写协议、第二parser、shadow canonicalizer或
  `subscription.rs`补丁。当前data-collect直接消费`ConnectionManager` broadcast；
  `subscribe_market_with_options`返回的filtered stream会立即drop，因此其空数组过滤
  不在当前collector接收链路上。

用户最初把全部single-field gap冻结为`EXCLUDED`；随后核实
`book.lastTradePrice`与独立`last_trade_price`分别是快照状态和成交事件，并非
可互相替代的重复数据。用户据此把F04单独修正为`INCLUDED_OPTIONAL`。最终矩阵为：

| ID | variant.field | 决定 |
|---|---|---|
| F01 | `book.minOrderSize` | `EXCLUDED` |
| F02 | `book.tickSize` | `EXCLUDED` |
| F03 | `book.negRisk` | `EXCLUDED` |
| F04 | `book.lastTradePrice` | `INCLUDED_OPTIONAL` |
| F05 | `last_trade_price.transactionHash` | `EXCLUDED` |
| F06 | `new_market.tags` | `EXCLUDED` |
| F07 | `new_market.conditionId` | `EXCLUDED` |
| F08 | `new_market.active` | `EXCLUDED` |
| F09 | `new_market.clobTokenIds` | `EXCLUDED` |
| F10 | `new_market.sportsMarketType` | `EXCLUDED` |
| F11 | `new_market.line` | `EXCLUDED` |
| F12 | `new_market.gameStartTime` | `EXCLUDED` |
| F13 | `new_market.orderPriceMinTickSize` | `EXCLUDED` |
| F14 | `new_market.groupItemTitle` | `EXCLUDED` |
| F15 | `new_market.takerBaseFee` | `EXCLUDED` |
| F16 | `new_market.feesEnabled` | `EXCLUDED` |
| F17 | `new_market.feeSchedule` | `EXCLUDED` |
| F18 | `market_resolved.tags` | `EXCLUDED` |

F04只能以`BookUpdate.last_trade_price: Option<Decimal>`表示快照时已知的最近成交价。
missing/null保持`None`；实际有值时只作为book快照字段保留，不得伪造，也不得转换成
独立`last_trade_price`成交行。current v2只在该字段实际存在时把它加入canonical与
raw-equivalent JSON；字段缺失时旧canonical字节、fingerprint、row及持久化语义保持
不变。实际目标市场首次订阅/重连book中的填充率尚未经过公开网络smoke验证，不能把
optional typed支持写成非空可用性承诺。

该决定不删除current v2现有列，也不把authenticated
`TradeMessage.transaction_hash`错接到public `LastTradePrice`。只有P-B1完整验证、
独立审查和单独发布均闭合后，typed终态才从历史
`SDK_070_TYPED_CONTRACT_BLOCKED`切换为
`SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`；这只解除阶段3的typed前置，
其闭合本身当时不等于阶段3 Rust/schema/test实施授权；用户随后于2026-07-25授予
持续执行/发布授权，现已覆盖阶段3本地实施及其常规发布门。

### 2.4 `live_runtime.rs` 是生产核心，不是整文件死代码

当前文件：

| 项目 | 数值 |
|---|---:|
| 总行数 | `50,382` |
| 主测试模块起点 | 第 `23,398` 行 |
| 主测试模块 | `26,985` 行 |
| 同步测试 | `83` |
| Tokio 测试 | `190` |
| 测试合计 | `273` |
| 顶层函数 | `335` |
| structs / enums | `86 / 55` |

真实 production 调用链为：

```text
data_collect_training_collector
  -> runtime::run_training_collector
  -> live_runtime::run_live_training_runtime
```

因此正确结论是 `KEEP + SPLIT + AUDIT`，不是删除整文件。paper/live hot collector 走另一条入口；本事项必须保留其产品边界，但不得把 training production core 误写成 paper/live 的直接 runtime。

当前职责至少包含 raw-day/epoch、queue/backpressure、startup/barrier、manifest owner、rollover、runtime composition、六类 writer、Polymarket CLOB/Gamma/resolution、RTDS、Binance depth/aggTrade、discovery、source supervision、JSON/typed adapters 和大型内嵌测试。它们应按 owner 分离，不能搬进一个新的同等巨型文件。

### 2.5 历史远端分支只能作证据

`origin/data-collect-maintainability-cleanup` 曾完成 SDK 0.7.0 升级和部分机械拆分，但当前 branch 与它已从共同祖先分别前进 `7` 和 `16` 个 commit。当前 branch 又加入大量 resolution follow-up 与 RTDS heartbeat 语义，`live_runtime.rs` 已从历史基线显著增长。

因此：

- 可以复用其官方 crate provenance、四文件 patch 矩阵、模块边界经验和已验证测试名称；
- 不得整体 merge、直接 cherry-pick 旧 SDK commit、用旧 branch 的行号/测试数量冒充当前基线；
- 每个旧 patch 必须相对当前 HEAD 重新审计并重放。

### 2.6 阶段 2 共同基线执行记录（2026-07-25）

任务 2.1/2.2 要求的 current observable、owner、测试、symbol、
跨 owner message-flow 与 `#[cfg(test)]` seam 已按 Stage1 closure commit
`b818e436a115eb61859fa281773342ee58f54f6a` 落盘到
`data-collect/docs/sdk070-stage2-observable-and-owner-baseline.md`。文档路径、
artifact事实、测试/symbol数量与hash、`cargo test -- --list`及
`git diff --check`均已复核，任务2.1/2.2共同基线记为完成。

本次共同基线只记录既有事实，没有修改 Rust、schema、payload、fingerprint、
admission、RTDS、paper/live、部署路径或 `run_id` 校验。阶段 1 的
`SDK_070_TYPED_CONTRACT_BLOCKED` 仍只阻断阶段 3 v3，不阻断阶段 2 行为保持的
机械拆分。

`2-PM` step 1随后按该基线完成：Polymarket runtime source owner及其直接Gamma
discovery/identity/resolution transport与helpers迁入私有
`live_runtime/polymarket_source.rs`，73个直接测试迁入私有测试子模块。主代理
复核production、测试函数与root remainder的归一化内容均与Stage1 HEAD一致；
owner、跨owner与integration定向门及locked all-targets check全部通过，schema、
payload、fingerprint、admission与crate public API未变化。下一小步严格限定为
`2-PM` step 2 adapter/row conversion owner。

`2-PM` step 2随后完成：38个纯adapter/row conversion items迁入私有
`live_runtime/polymarket_event_adapter.rs`，12个直接测试迁入同名私有测试
子模块。RawDay/admission、dedupe、writer dispatch、restart recovery与真实
cross-owner测试保持root原位。主代理逐item/逐测试归一化对账与Stage1 HEAD一致；
全局测试仍为593且name-only hash不变，定向、跨owner、integration、fmt与locked
all-targets check全部通过。下一小步严格限定为`2-PM` step 3 writer integration
seam；restart recovery与RTDS仍不提前移动。三份既存未跟踪RTDS草案保持未修改、
未暂存、未删除。

Step 3只读审计确认，`LiveSegmentWriter`、`typed_schema_version`与
`write_typed_live_event`均为六类source共享owner，PM只有其中两个match arm，
不存在可仅靠path/visibility适配完整迁移的PM dispatcher symbol。用户随后批准
`Writer-A`：共享source-kind dispatch与统一`logical_bytes`计算继续留在root；
只把PM当前schema version value及两个rows-to-writer leaf实现归入既有私有
`polymarket_event_adapter` owner，并把PM独占的单行保护随leaf移动。不得新增
`Option/handled`协议、重复source-kind match、第二writer abstraction或把
Binance/RTDS writer逻辑归入`2-PM`。

`Writer-A`随后完成：current orderbook schema value由既有两个同值literal归并为
PM adapter内一个私有常量；两个root PM write arm分别直接委派adapter内对应leaf，
原one-row guard随leaf移动。root仍唯一持有六源schema/write match与
`logical_bytes`计算，metadata v3常量仍由原layout owner持有。只新增一个未拆分
两行事件在写入前fail closed的直接测试；全局测试增至594是该真实新增测试，不是
固定数量门。定向unit、六组integration、fmt、locked all-targets check与独立
审查均通过。下一小步只允许审计既有`polymarket_resolution_recovery.rs`唯一owner；
无必要不得再重构。

Step 4审计确认`polymarket_resolution_recovery.rs`已完整持有唯一production restore
入口、metadata/raw bounded scan、schema/payload/reference validation、finalized
路径排序、恢复结果类型及32个同文件直接测试。root只在stable startup recovery
调用该入口，PM source只消费恢复结果构造运行态；root另有的
`restore_polymarket_dedupe_until_cancelled`只恢复lifecycle fingerprint，不是
resolution-state shadow path。该步终态为`ALREADY_OWNED_NO_MOVE`，没有修改
recovery、`lib.rs`或public API。32个直接测试、两个startup边界测试及既有PM
cross-owner测试均通过。`2-PM`分支收口完整确定性门与独立审查随后均已通过；
用户已单独批准closure commit/push；`2-PM_CLOSED`以承载本记录的commit成功推送
且本地HEAD、upstream与远端ref一致为生效条件。

## 三、authority 与文档关系

### 3.1 当前总需求批准状态与独立门

用户已于2026-07-25明确批准按本需求与对应计划开始执行；本需求不再是“总体未获批草案”。同日用户进一步批准持续推进直至本事项完成：本事项范围内的本地实现、测试、阶段审查、commit和push不再逐次申请授权；只有开发文档、代码语义、运行事实或用户明确描述不能同时成立时才停止请求决策。该持续授权满足阶段3等后继本地执行/发布门，但不使当前代码或旧authority立即失效，不自动通过SDK typed字段gap或未冻结的RTDS参数，也不扩大第九节范围外事项。`market_resolved` v3 validate-first和`METADATA_V4_CLAIMANT_SCHEMA_APPROVED_2026-07-25`继续是已经单独冻结、不得重新置为待决的合同；它们仍不等于Rust/schema/test实现或activation已经完成。current source v2 + metadata v3代码保持有效，只有对应实质入口门和验收通过后，才能在已冻结的原子activation diff中切换。

### 3.2 本需求批准后需要精确取代的旧合同

`market_resolved` v3 validate-first与metadata v4 claimant两个窄决定已经完成文档authority同步，不得在阶段3重新置为待决，也不得据此声称实现完成。总需求虽已获执行批准，阶段0仍只建立以下conflict/delta matrix；除用户本轮另行批准的四路径current authority修正外，其余typed schema旧authority只能在SDK字段门通过、用户批准v3字段集合且2026-07-25持续执行授权有效后，于阶段3代码修改前同步：

- `demand/data-collect-reliability-repair-demand.md` 中以 `source_native_payload_json` 做持久化 evidence、跨进程 fingerprint recovery 和 fan-out 前丢弃的合同；
- `demand/data-collect-demand.md` 中只适用于`source_kind=polymarket_market` persisted artifact/data-pipeline的parseable `raw_json`或等价原始证据条款，收敛为“官方SDK解码后的完整消费typed evidence”；其他source及paper/live Handoff v1/Task 7.4条款均为`KEEP_OUT_OF_SCOPE`；
- `plan/data-collect-plan.md` 中common envelope和Polymarket artifact的`raw_json`/原始证据条款；当前`HandoffFrame v1.raw_json`只登记为`KEEP_OUT_OF_SCOPE`，本事项不得替换；
- `plan/data-collect-pmxt-hourly-retention-plan.md` 中 `polymarket_orderbook_v2` 的 `hash` 与 `source_native_payload_json` schema；
- `demand/rebuild-core-demand.md` 中 raw JSON/equivalent evidence 的未来一致性描述；该文档整体仍在审核，不能冒充当前执行 authority；
- `demand/data-collect-polymarket-resolution-followup-demand.md`、`plan/data-collect-polymarket-resolution-followup-plan.md` 与 `changelog/data-collect-polymarket-resolution-followup-changelog.md` 中“decoded后、terminal校验前持久化”“invalid raw保留”“Gamma-first raw-only corroboration”“post-horizon raw-only corroboration”和raw-to-metadata restart矩阵；这些条款已按第3.2.1节为未来v3精确取代，同时保留历史及切换前`polymarket_orderbook_v2` source artifact的read-only事实，以及fixed identity、slot、terminal-once、typed restart/fail-closed和无future-skew语义。

旧 changelog 必须保留历史事实，不得回写成这些设计从未存在。

### 3.2.1 `market_resolved` v3 validate-first 已批准决定

用户于2026-07-24明确批准以下窄化 authority；该决定无需在阶段3再次请求，实施仍受阶段3其余实质入口门、验收标准和2026-07-25持续执行授权范围约束：

- `matched_resolved_slug` 或等价逻辑只进行 candidate routing，不等于 target admission；
- SDK decode 后，唯一 source owner 必须先完成目标 lifecycle relevance，以及 market id、condition id、nullable slug、exact asset set、outcomes mapping、winning asset 和 winning outcome 的现有完整校验；
- 完全非目标的消息写 `0` 条 source row、`0` 条 terminal metadata且不改变状态；与目标任一 identity key相关但完整校验矛盾的消息在任何 source-row admission、terminal claim或状态变化前 fail closed，只记录既有有界 gap/error/coverage事实。该事实只能使用现有稳定 reason code、已定义的最小身份坐标、计数和时间字段，不得嵌入完整或截断的 source frame、项目重序列化 JSON、payload digest，或足以重建/恢复该被拒消息的字段集合；
- 只有通过完整校验的消息才构造一次 canonical `market_resolved` typed projection并写一条 v3 source row；terminal-once与 `resolution_terminal_ws` 必须从同一个 validated object 派生，不得再次解析 JSON或建立第二 validator；
- Gamma-first drain期间后到且仍在 accepted-event边界内的合格WS可以保留一条 validated typed source observation，但不得取得第二terminal ownership；drain/role-exit之后到达的消息已经不属于 target admission，写 `0` 条 source row，不再保留post-horizon raw-only corroboration；
- 无效、unmatched或post-horizon frame不进入 canonical artifact。若未来要求完整拒绝帧取证，必须另立审计合同并由用户决定；本事项不得增加 rejected-row schema、quarantine/sidecar、generic evidence blob或第二artifact；
- 历史及切换前`polymarket_orderbook_v2` raw-first文件保持read-only兼容和历史事实，不得重写；它们不能授权v3 new-write恢复raw-first。

### 3.3 RTDS 专项的纳入方式

`demand/data-collect-rtds-business-silence-full-reconnect-demand.md`、对应 plan/changelog 继续作为 RTDS 子事项的详细合同。它当前与以下已批准 authority 存在明确冲突：

- `demand/data-collect-demand.md` 中业务静默不驱动连接的条款；
- `demand/data-collect-source-availability-lifecycle-repair-demand.md` 中 RTDS 长静默 generation 不变、无 collector silence timer 的条款；
- 当前“静默不重连”回归测试。

本总事项不裁决该冲突。只有用户冻结RTDS三个高影响参数、最终批准RTDS新需求和精确authority修改矩阵，并单独授权4A后，才能先同步相反authority并停止；替换相反测试和实现行为还必须另获4B Rust修改/构建/测试授权。

RTDS typed row/schema 不属于这次 `polymarket_orderbook_v3` 列删除。不得借 RTDS 子事项删除 RTDS、Binance 或其他 source 的 `source_native_payload_json`。

## 四、范围

### 4.1 包含

- 官方 SDK 0.7.0 provenance、vendor patch rebase、Cargo/lock/feature 和可用性验证；
- `polymarket_market` 下一版 typed message schema、writer、reader、restart recovery，已批准的`polymarket_market_metadata_v4` claimant扩展，data-collect侧training/data-pipeline cold-consumer合同与golden compatibility fixture，以及非阻断future typed handoff静态规格；
- 删除该 source 的 `source_native_payload_json` 和 venue validation `hash`；
- 停止 `PriceChange` fan-out，一条通过现行 target admission 的 message projection 写一条 nested typed row；mixed message 只保留匹配目标的 entries 及其 SDK 原始相对顺序；
- `live_runtime.rs` 的 contract freeze、owner 拆分、测试迁移与逐候选死代码审计；
- RTDS 业务静默完整换 runtime 子事项在上述升级和拆分后的有序接入；
- Stage3内实现、运行并独立审查唯一的代表性同输入bounded fixture v2/v3 storage probe，比较storage、CPU、RSS、finalize和读取性能；完整UTC日只消费Stage0已经冻结的post-dedupe v2事实基线，不重扫、不生成full-day v3；
- 只有metadata v4实现及对应WS claimant/recovery验收通过后，本地代码/test中的current new-write生产调用图才可在同一activation diff中从source v2 + metadata v3原子替换为source v3 + metadata v4；此前只允许未激活的domain/schema/writer/reader、不依赖claimant pair的typed recovery及其测试。不得出现source v3 + metadata v3、source v2 + metadata v4、dual-write或半切换；该代码路径切换不等于production部署或dataset root激活；
- 本地确定性测试、静态检查，以及另行批准后的公开只读网络 smoke/probe。

### 4.2 不包含

- raw Polymarket REST/WebSocket、manual signing、自建 SDK、第二解析器或 shadow feed；
- 钱包、认证 user channel、真实下单、资金测试或任何交易；
- 越过SDK typed字段终态、RTDS参数/4A/4B、公开网络smoke或production相关独立门；阶段3本地实现及常规阶段commit/push已由2026-07-25持续执行授权覆盖；
- 修改其他 source 的 persisted payload/hash schema；
- 新建通用 actor/source/plugin framework；
- 把代码简单搬进新的巨型模块，或仅以测试数量/总 LOC 下降证明正确；
- production release、部署、systemd、远端服务器操作；
- 历史 Parquet 批量重写、删除、production binary激活或服务器 dataset root/canonical new-data cutover；
- 真实data-pipeline adapter/consumer代码改造，以及尚不存在的真实 paper/live 外部事件 adapter、模型 observation 接线、长时间运行验收和部署；这些必须在正确仓库另建并批准对应三件套；
- 为了减行删除 paper/live binary、role、handoff、startup receipt、deployment boundary 或 consumer contract。

## 五、SDK 0.7.0 需求

1. 新 vendor 必须从已核验的官方 0.7.0 crate 重建为 `third_party/polymarket_client_sdk_v2-0.7.0-p1`，不得给旧 0.6 目录改名冒充升级。新vendor完成provenance、四路径patch和workspace依赖切换后，必须在同一阶段1 diff删除tracked旧`0.6.0-p1`源码树；Git checkpoint保留回滚能力，不得把旧树作为无消费者副本或shadow路径留在`third_party`。
2. provenance 至少冻结 crate SHA-256、VCS commit、发布时间、license、MSRV、文件清单和 Cargo metadata。官方没有对应可验证 tag/release object时不得虚构。
3. 相对官方 0.7.0 的项目源码差异必须恰好是当前批准的四个路径；每个 hunk 必须映射到仍有效的需求和确定性测试。出现第五个路径必须停止并由用户决定。
4. workspace 只能解析一个 `polymarket_client_sdk_v2`，精确版本为 `0.7.0`，features 保持 `clob,gamma,rtds,ws` 的当前生产需要；依赖树不得同时保留 0.6。
5. 必须分别验证官方 upstream 行为、四文件 patch 行为、data-collect wrapper 行为和完整 collector 编译/测试；不能只用 `cargo check` 宣称可用。
6. runtime升级终态只能是 `SDK_070_RUNTIME_USABLE` 或 `SDK_070_RUNTIME_NOT_USABLE`。
7. typed-contract终态独立为：
   - `SDK_070_TYPED_CONTRACT_SUFFICIENT`：全部必需字段由官方 SDK typed API提供；
   - `SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`：用户逐项批准已知 gap为排除或 `nullable_unavailable`；
   - `SDK_070_TYPED_CONTRACT_BLOCKED`：任一当前消费必需字段缺失，或SDK拒绝官方允许的target相关missing/null消息且尚无用户批准的官方SDK修复路线；阶段 1报告后只阻断 v3 schema及其 reader/writer/local new-write替换。
8. 阶段 1 必须更新本节已有字段/nullability/parser矩阵，而不是把字段检查写成首次发现。runtime升级可用不等于 typed schema能力足够；不得用`nullable_unavailable`掩盖整帧在SDK parser处已丢失。
9. 已批准`P-B1`只能在既有`src/clob/ws/types/response.rs`生产patch内修复七类
   missing/null typed decode；因`BookUpdate.timestamp`直接传播到`MidpointUpdate`，
   后者必须在同一文件保持同型。vendor tests/example可作必要编译与合同适配，但不得
   冒充第五个生产源码patch。
10. P-B1必须用永久fixture分别覆盖七类variant的missing object、null object、
    `[missing object]`和`[null object]`，全部走真实
    `parse_if_interested(..., MessageInterest::ALL)`并返回恰好一条正确variant。
11. current source v2必须在canonical payload、fingerprint、row、terminal claim和
    业务状态变化前拒绝新nullable但旧v2要求完整的shape；完整旧fixture保持原行为。
    不得把`MarketResolved.question/slug/description/outcomes`误设为新v2必填，因为
    它们在P-B1前已经接受missing/null。
12. F04为`INCLUDED_OPTIONAL`，使用
    `BookUpdate.last_trade_price: Option<Decimal>`并进入未来v3 book字段；F01–F03
    与F05–F18全部`EXCLUDED`，不得加入SDK struct或未来v3 schema。不得用
    authenticated channel字段或Gamma metadata值伪装public WS观察值，也不得把F04
    合成独立成交事件。

## 六、`polymarket_orderbook_v3` typed message 合同

### 6.1 行粒度与结构

下一版 schema version预定为 `polymarket_orderbook_v3`；具体字段集合只有在已知 SDK gap逐项裁决后才能冻结。每条已接纳、属于目标 market lifecycle 的 target-admitted SDK message projection只写一行：

- 共同 envelope：`timestamp_received`、平台 `timestamp`、`event_type`、`market`、可用时的 `slug`、collector identity、region、endpoint、schema version，以及本事项新增并按下述合同冻结的`source_sequence_epoch_id`、`collector_admission_sequence`和`collector_admission_time_utc_ms`；
- `book`：`asset_id`，`bids` / `asks` 使用 Arrow `List<Struct<price: Utf8, size: Utf8>>`，保留顺序和 decimal 文本；另保留nullable decimal-string `last_trade_price`作为快照状态，不合成成交行；
- `price_change`：`price_changes` 使用 Arrow `List<Struct<asset_id, price, size, side, best_bid, best_ask>>`；只保留通过现行 target asset/condition admission的全部 entries及其 SDK原始相对顺序，不再复制共同 payload；mixed message中的非目标 entry继续被过滤；
- `tick_size_change`：asset、old/new tick size；
- `last_trade_price`：asset、price、size、side、fee rate；F05 `transactionHash`保持`EXCLUDED`；
- `best_bid_ask`：asset、best bid、best ask、spread；
- `new_market`：`clob_market_id`、question、slug、description、asset IDs、outcomes、event message typed struct；
- `market_resolved`：new-market identity/text/arrays，加 winning asset/outcome 和 terminal 所需 typed 字段。

不得用 JSON string 保存 bids/asks、asset IDs、outcomes、event message 或 price changes；不得创建新的 generic evidence blob。

用户于2026-07-25选择方案B，冻结项目自有canonical命名：SDK及平台wire顶层
`id`只在typed入口边界映射一次为`clob_market_id`；此后的v3 domain、Parquet、
metadata、recovery和项目自有consumer统一使用`clob_market_id`。共同envelope中的
`market`继续表示condition ID。不得在项目自有canonical合同中同时保留同值的
`id`与`clob_market_id`两列、alias或双路径。第三方SDK struct及activation前
current-v2 canonical/raw JSON继续按其外部或legacy合同保留`id`，不构成第二个v3
canonical命名，也不得借本命名决定提前修改active-v2行为。nested
`event_message.id`表示不同层级的event identity并保持原名；本决定不授权历史
artifact重写、回填或migration。

对 `market_resolved`，candidate routing不是 admission。source owner必须先完成第3.2.1节的目标关联和完整identity/assets/outcomes/winner校验，再构造一条canonical typed projection。SDK decode成功但unmatched、identity/shape/outcomes/winner矛盾的消息不得产生v3 source row；不得把“已解码”误写成“已通过target admission”。

`Trade` / `Order` user-channel variants不属于当前 public market source artifact；未知 SDK variant 必须有显式 terminal/ignore 合同并 fail closed 于“可能属于已订阅 market data 但 schema 未覆盖”的情况，不能静默降级成空 row。

#### 6.1.1 v3 source admission 与 claimant reference

当前`polymarket_orderbook_v2`没有collector admission字段，现行`polymarket_market_metadata_v3.collector_admission_*`只标识metadata row自身，不能冒充跨artifact claimant外键。v3必须复用仓库已有source epoch + admission sequence模式，并冻结以下最小合同：

1. 每个v3 source row保存非空opaque `source_sequence_epoch_id`、checked非零`collector_admission_sequence`和正数UTC毫秒`collector_admission_time_utc_ms`。epoch由唯一Polymarket source owner在source task启动时生成，跨socket reconnect和UTC rollover稳定，在source task/进程重启时更换；不得从payload、venue `hash`或Parquet路径派生。
2. `collector_admission_sequence`在同一`source_sequence_epoch_id`内单调递增且不得回绕；只有完整target validation、对应row/byte permit与bounded source-writer queue ownership全部成立后才能分配。`collector_admission_time_utc_ms`在同一时点冻结，只用于验证`timestamp_received <= collector_admission_time_utc_ms`的实际因果顺序，不参与唯一key。validation失败、permit失败、channel closed或queue admission失败不得消耗一个可持久化source reference后继续claim terminal。
3. v3 source row的逻辑唯一键固定为`(source_sequence_epoch_id, collector_admission_sequence)`。两个内容完全相同的真实platform deliveries必须取得不同sequence；内部retry复用既有ownership且不得取得第二个source key。v3 writer按collector admission order写入，并拒绝同epoch内重复或倒退sequence；sequence允许因失败产生空洞。物理file/part/row index、payload digest、平台timestamp或单独`timestamp_received`均不能代替该key。
4. 既有`polymarket_market_metadata_v3` schema/version保持不可变；它没有可合法复用的source claimant外键，不能静默追加字段，也不能复用metadata row自身的`collector_admission_*`、source digest、平台时间或物理行位置。用户已于2026-07-25批准在同一`source_kind=polymarket_market_metadata` artifact/writer/partition/manifest mapping内新增`polymarket_market_metadata_v4`，以metadata v3全部字段、顺序、类型、nullability和语义作为不可变前缀，只在末尾追加nullable `ws_claimant_source_sequence_epoch_id: Utf8`与`ws_claimant_collector_admission_sequence: UInt64`；metadata v3在activation前仍是current metadata writer，activation后不再由current runtime new-write。其公开offline legacy writer只保留给历史fixture、兼容测试和bounded A/B，不构成current writer。
5. metadata v4中，只有`metadata_row_kind=resolution_terminal_ws`且`terminal_claim_owner=market_resolved_ws`时claimant pair才同时必填；其他row kind必须同时为null。partial pair、空epoch或sequence=0均fail closed。production restart在持有dataset-root lease期间，必须先由startup/manifest owner验证prior-day manifest并reconcile current manifest的path、rows、bytes、SHA和file facts，再调用recovery；recovery随后从同一锁定root扫描final-named v3 source artifact，并只接受恰好匹配一行、且event/market/slug/assets/winner与terminal metadata一致的claimant key。0条、多条、identity不一致、metadata先finalized但source缺失或source-only finalized prefix均fail closed；不得靠补写metadata、猜测物理行序或选择任意agreeing duplicate修复。
6. `M-B`明确以production startup调用链作为manifest trust boundary：公开recovery API继续接收`dataset_root + time`，不新增manifest snapshot参数，也不建立第二套manifest校验。直接调用该API本身不构成production manifest-authority证明；绕过startup preflight或不遵守dataset lease的外部文件突变属于本合同接受的剩余风险，不得反向扩展为通用snapshot框架。

### 6.2 明确删除与明确保留

v3 writer 不得包含：

- `source_native_payload_json`；
- book/price-change venue validation `hash`；
- 由上述 JSON 计算并持久化的 dedupe digest；
- fan-out row reference、message evidence sidecar或第二 artifact。

必须保留或明确裁决：

- finalized Parquet/manifest 的 file SHA-256；
- `transaction_hash` 与 venue `hash` 是不同语义，不能因名称相似而自动删除；但 public `LastTradePrice.transactionHash` 当前不可由 SDK 0.7取得，必须按已知 gap决定为“阻断、明确排除或 `nullable_unavailable`”，不得伪造或从 authenticated channel错接；
- 完整 typed business fields、数组顺序、nullable 语义、receive/source time、provenance和 resolution 所需 identity/winner/outcome；
- v2 legacy schema/reader和历史artifact的read-only兼容；production-compiled public offline v2 writer继续只用于历史fixture、兼容测试和本事项bounded A/B，直到用户另行批准停止兼容。

本节删除范围只针对`source_kind=polymarket_market`的下一版source schema。既有稀疏`polymarket_market_metadata_v3`中的`source_evidence_payload_json`、`source_evidence_raw_sha256`及Gamma/WS terminal traceability字段不因列名相近而自动删除；metadata v4继续保留这些既有字段语义并只追加第6.1.1节的claimant pair，metadata v3 schema/version保持不可变且在activation后退出current runtime new-write。WS terminal字段必须从已经通过第3.2.1节校验的同一个canonical object派生；resolution recovery不得再用metadata digest去要求orderbook raw row或恢复raw-first。W-B保留的offline legacy writer不得接回`LiveSegmentWriter`、current schema dispatch、recovery authority、dual-write或production cutover。

### 6.3 去重与重启

1. `USER_DECIDED_REMOVE_COLLECTOR_CROSS_PROCESS_DEDUPE`：collector 不再对 Polymarket market SDK message做跨进程 payload-equality 丢弃，也不从 Parquet 恢复 fingerprint cache；不得重新请求该决定。
2. 同一进程的 queue ownership、atomic fan-in 和 writer admission 仍必须保证项目内部的一次 admission 不因内部 retry被额外写入；这项 exactly-once ownership不等于丢弃平台重复 delivery。
3. 本事项不新增 data-pipeline hash、semantic-key或强制 duplicate drop。两个字段完全相同、由平台实际连续投递的 target-admitted fixture必须按原始顺序产生两行；内部 retry fixture不得产生第三行。代表性同输入bounded fixture必须报告dedupe删除后的row/event及canonical-domain/resolution/manifest差异；现有full-day只按第2.2节冻结的post-dedupe边界保留v2 artifact/storage/resource/manifest事实基线，不生成v3语义对照。future mapping只做独立非阻断静态审查。
4. 若另一个已批准 consumer合同确实要求 duplicate分类，必须另行列出消费者、业务语义和测试；临时比较必须覆盖 event type、market、平台时间和该 variant 的全部 retained typed fields，不得把 decimal `0.1` / `0.10` 擅自视为相同，且不得持久化新 digest列或反向控制 collector admission。
5. resolution restart recovery 只投影已经通过第3.2.1节 admission的 typed `event_type/market/slug/timestamp_received/winning_asset_id/winning_outcome/asset_ids/schema_version/source_sequence_epoch_id/collector_admission_sequence/collector_admission_time_utc_ms`及冻结的identity字段；`market_resolved`与`new_market`统一使用v3 canonical `asset_ids`，不得保留同值的`resolved_event_asset_ids` alias或双列；历史v2字段名只作reader/audit兼容。不解析 payload JSON，不用 payload digest交叉证明相同事实。
6. production restart consumer结果必须由validated typed identity、metadata v4 authority以及startup/manifest owner在dataset lease内完成的prior-day preflight/current reconcile所建立的finalized facts恢复；recovery函数不重复建立manifest snapshot authority。唯一WS terminal必须通过`(ws_claimant_source_sequence_epoch_id, ws_claimant_collector_admission_sequence)`精确指向实际claimant source row；多条agreeing platform duplicates各有不同source key，不得任选其一或用模糊匹配降级。唯一Gamma terminal可以没有WS source row；若存在WS row，只允许role-exit前accepted drain中的validated late-WS observations，且这些row不得取得terminal ownership或被Gamma metadata引用为claimant。`0`条`market_resolved` source row + 唯一合法exhausted summary及其已finalized attempt metadata是正常预算完成态，不恢复pending或未来slot。
7. 完全unmatched、target-associated contradiction和post-horizon消息在v3中没有source row，因此不得继续执行旧raw-only矩阵。存在validated `market_resolved` source row但没有唯一terminal或summary、validated row与exhausted summary并存、row receive晚于terminal/summary role-exit、terminal与summary并存、重复终态、缺列、unknown schema、typed contradiction或损坏文件时继续fail closed，不恢复pending、不发Gamma、不写exhausted，也不依据source row伪造transition或补写metadata。唯一exhausted summary本身缺失既有required counters/status/attempt一致性时仍按metadata合同fail closed。
8. 历史`polymarket_orderbook_v2` source reader只按旧schema提供read-only consumer/audit兼容；其raw-first row不能合成v3 source row、不能恢复resolution-pending，也不能作为v3 source-reference authority。既有`polymarket_market_metadata_v2`同样只读，不能授权v3 recovery。

### 6.4 canonical domain 与 future handoff 兼容边界

- SDK message进入source并执行现行target-market/asset admission后，只构造一次target-admitted canonical domain projection；
- v3 writer直接编码该对象，v3 reader解码回同一类型；v2 legacy reader只输出独立`LegacyV2Row`或等价row-grain read-only audit view，不得重组为v3 message-shaped domain，不得推断原message边界、admission coordinate或source reference，也不得成为第二sort/explode/canonicalize owner；
- 本事项不修改当前仍含`raw_json`的`HandoffFrame v1`，不让hot binary发送external market event，也不宣称hot runtime或paper/live parity；
- 本事项只记录`NONBLOCKING_FUTURE_SPEC`：“canonical domain字段 -> future typed handoff字段”的当前可知静态对应关系。当前authority要求future handoff携带nullable decimal-string `price_to_beat`/`final_price`；二者属于后继external-event adapter/metadata join的owner、frame version与legacy compatibility设计，不得从CLOB v3伪造。静态附件中的未知项进入后继三件套，不阻断v3；
- future adapter必须是nested `price_changes`展开、排序和过滤的唯一owner；本事项不预先实现一条影子adapter。

必须用同一target-admitted fixture证明canonical domain经v3 round-trip后field-for-field、顺序逐项相同，并用golden fixture核对data-collect侧training/data-pipeline producer contract不依赖被删字段。future mapping附件只检查已知字段与两个外部metadata join责任是否被记录；缺少尚未批准的future字段不构成v3失败。上述证据不能冒充真实data-pipeline adapter、当前handoff或paper/live运行验收。

### 6.5 全局内存与 permit 合同

删除 JSON 和 fan-out 只删除对应重复 owned bytes，不取代仍有效的全局资源边界：

1. v3 domain message的 permit合同固定为：`size_of::<DomainMessage>() + Σ String::capacity() + Σ (Vec<T>::capacity() * size_of::<T>())`，并递归计入各 Vec已初始化元素拥有的 String/Vec动态缓冲区；每个 allocation只计一次。不得把 Vec元素数直接当字节，也不得把该合同值声称为 allocator实际bytes、共享buffer或Arrow内部不可观测capacity，或为此创建通用追踪框架。
2. 每个target-admitted message projection以一行取得`GLOBAL_INFLIGHT_MAX_ROWS=100_000`和按上述公式计算的contract-owned-byte permits；`GLOBAL_INFLIGHT_MAX_BYTES=134_217_728`、`MAX_TYPED_ROW_BYTES=1_048_576`与`BUFFER_ACQUIRE_TIMEOUT=2s`继续适用。
3. 预算表必须逐 owner记录对象、上限、reservation起点、跨 queue/await的存活期和释放点。move/borrow不新增计费；只有与原对象同时存活并跨越 await/queue的 owned copy另取 permits，原对象释放后才归还原 reservation。
4. 当前 Polymarket decoded queue的 `25_000 messages / 67_108_864 bytes` 附加上限继续存在；它不能代替全局预算。停止 fan-out后不得借机删除 decoded queue、staging或 writer的有界性。
5. Arrow writer复用现有 batch/writer bound；如果当前没有可证明的边界，停止并报告缺口，不得用通用 instrumentation掩盖。RSS只作为代表性/full-day报告项，不宣称逐 Arrow allocation精确归因。
6. oversize、预算等待超时、channel closed或任何 permit不变量失败继续记录真实 gap、coordinated finalize并按现行 authority fail closed；不得 drop 后继续。
7. 必须用 exact-limit、oversize、nested-book/price-change burst、queue/writer阻塞和 shutdown测试证明 row/byte permits无泄漏、无重复释放且最终回到基线。

## 七、`live_runtime.rs` 模块化与删除合同

### 7.1 行为保持拆分

在任何 schema 或 RTDS 行为改变前，先冻结当前生产入口、四个 logical producer、六类 artifact、day/rollover、manifest、writer、recovery、shutdown和现有 handoff边界。`273` 只是 2026-07-24 快照，不是未来固定门槛；阶段开始时自动记录实际测试名称集合和归属，人工矩阵只覆盖跨 owner channel、public API、删除候选及高风险 coordinator/day/manifest/shutdown合同。

机械拆分必须形成三个可独立收口的分支：

- `2-PM`：逐owner提取v3直接需要的最小Polymarket source、该source直接持有的Gamma discovery/identity/resolution transport与helpers、adapter/row conversion、PM current orderbook schema version value与rows-to-writer leaf owner和resolution recovery；六源共享source-kind dispatcher继续属于root/`2-REST`，每一步仍只移动一个真实owner；
- `2-RTDS`：只提取RTDS source owner及其测试；
- `2-REST`：其余Binance、shared writer/manifest/day/coordinator/source supervision/root等owner按收益和风险独立迁移。

`2-PM`完成即可进入v3，`2-RTDS`完成即可在其他RTDS入口条件满足后进入专项；`2-REST`不阻断前两者。每个分支内部小步只做定向/受影响测试、compile/check和自审，分支收口才运行一次完整回归和独立审查。审查通过后在用户持续发布授权下直接创建对应closure commit/push并核对ref，随后记为`*_CLOSED`并允许后继阶段消费；无需重复询问，除非触发上下文/方案不一致决策门。

目标模块边界以真实 owner 为准，初始候选为：

- root facade / composition；
- runtime coordinator；
- raw day / epoch / rollover；
- manifest owner；
- writer supervisor；
- source supervision；
- Polymarket source owner、resolution、typed adapter；
- RTDS source owner与其测试，watchdog只在专项获批后加入；
- Binance depth；
- Binance aggTrade；
- 按 owner 分布的 tests。

模块名可以在阶段 2 inventory 后做有证据的小幅调整，但不得改成通用插件框架。每个机械迁移绿点只移动一个 owner及其直接测试，保持外部行为、schema、错误类别、稳定 event name/level/structured fields/counter和 poll/order语义不变；模块路径、文件名和日志源码行不要求相同，除非已有消费者证明确实依赖。

`live_runtime.rs` 或其替代 root `mod.rs` 只保留 public facade、必要 config/re-export和顶层 composition。不得设置缺乏证据的固定行数门；以 owner唯一、依赖方向清楚、测试随 owner归属、无需读取第二个巨型模块才能理解单一职责为验收，并报告各模块及 root LOC、moved/deleted/rewritten/added/net，不能把搬家称为净删除。

### 7.2 死代码与过度设计审计

每个候选的终态只能是：

- `KEEP`：仍有生产、公共 API、reader或合同消费者；
- `MOVE`：行为有效，只需归属正确模块；
- `MERGE`：与权威路径重复，且有可观察等价证明；
- `DELETE_PROVEN_REPLACED_BEHAVIOR`：旧行为已由唯一新 owner替代，具有替代合同和定向回归；
- `DELETE_PROVEN_UNREACHABLE_PRIVATE`：私有实现经全部 features/targets、public API、scripts、legacy reader和间接入口扫描证明无消费者；只需删除证明、package compile/clippy、阶段确定性回归和 diff审查，不得为删除它新增 test seam、抽象层或虚构替代合同；
- `USER_DECISION`：涉及公共 API、legacy schema、产品边界或 authority，等待用户决定。

审计根集合必须覆盖 binaries、lib public API、features/targets、tests、benches、serde/macro/trait 间接入口、deployment scripts、历史 readers和 demand/plan/changelog。只有测试消费者也不能直接推出可删除；必须先判断测试是在保护公共合同、legacy compatibility还是孤立旧实现。

当前结论（2026-07-26用户裁决后）：

- `live_runtime.rs`：`KEEP + MOVE`；
- 26,985 行内嵌测试：按 owner `MOVE`，不能当作净删除；
- 当前“RTDS 静默不重连”测试：在旧 authority 生效期间 `KEEP`；
- `sources/rtds.rs`及其旧合同测试：
  `DELETE_PROVEN_REPLACED_BEHAVIOR`；current唯一owner为
  `live_runtime/rtds_source.rs`、SDK runtime及source/hourly contracts，Stage4如获
  批准也只修改该current owner；
- `config.rs::CrossTaskQueueConfig`及其静态自测：
  `DELETE_PROVEN_REPLACED_BEHAVIOR`；真实owner为bounded channel、global inflight
  row/owned-byte预算、单行上限和bounded acquire/send wait，并沿用writer
  batch/periodic flush；queue duration、writer lag与RSS只作观测，不建立第二逐队列
  五维配置authority；
- `scheduler.rs`、`source_scheduler_contract`与旧synthetic e2e：
  `DELETE_PROVEN_REPLACED_BEHAVIOR`；真实owner为四logical-producer/六source
  startup映射及Polymarket current+prefetched lifecycle，连续Binance/RTDS不绑定
  market slug；
- `segment.rs` availability与naming/rollover两半及其旧测试：
  `DELETE_PROVEN_REPLACED_BEHAVIOR`；legacy/无独立admission schema使用recv time，
  已批准保存独立collector admission字段的current schema使用collector admission，
  paper/live使用handoff admission + runtime receive；运行中只按UTC hour
  rollover，协调shutdown可提前finalize，同小时启动/重启路径碰撞只选择初始
  `part=N`，50,000 rows/16MiB只切row group；
- `backpressure.rs`中的`BackpressureConfig/Monitor`、soft/hard age/lag阈值、
  `PauseIntake/ResumeIntake`及其test/bench控制路径：
  `DELETE_PROVEN_REPLACED_BEHAVIOR`；保留真实`bounded_event_queue`、global
  inflight owner、`QueueSample`纯观测DTO和`BackpressureReceipt`兼容字段，不以这些
  DTO重建控制决策；
- `health.rs`有限`RetryPolicy/plan_retry_trace`、circuit registry、
  `ReconnectGate`及其旧合同：
  `DELETE_PROVEN_REPLACED_BEHAVIOR`；生产只保留现有`live_runtime::retry_delay`
  内部的确定性capped+jitter数学，真实`SourceRecoverySupervisor`继续使用有界单次
  等待与无限外层总attempt，business freshness不成为transport恢复门；
- `dedupe.rs::LifecycleFingerprintSets`及两个Polymarket自引用测试、旧
  metadata-v2 runtime adapter与`fallback_close/unresolved_window_end_observation`
  正向测试：分别作为C02/C03与C01遗漏补删；它们不属于W-B公开legacy
  schema/reader/writer或Stage3 bounded A/B；
- v2 legacy schema/reader，以及
  `TypedParquetSegmentWriter::create_polymarket_orderbook*`、
  `write_polymarket_orderbook_rows`、metadata v3 offline writer等公开兼容入口：
  在停止兼容获得单独批准前`KEEP`；它们只服务历史fixture、兼容测试和Stage3
  bounded A/B，不得重新成为current runtime写入或recovery authority。

## 八、RTDS 子事项接入要求

1. 详细行为、术语、参数候选、测试、长跑、授权和性能门只以纳入的 RTDS demand/plan为准，本总需求不复制第二套合同；typed-contract字段裁决不是其依赖。umbrella只消费专项已记录的状态与入口/出口，不重扫数据、不重算参数候选、不重建delta/test矩阵，也不维护第二执行游标。
2. `4A_AUTHORITY`只在三个参数、RTDS最终需求和精确authority delta获批，专项阶段0.3的wrapper-first路线未被只读证据否定且用户已批准该SDK路线，并明确授权authority同步后执行；同步、自审/独立审查完成即强制停止。该授权不得继承到Rust修改、构建或测试。
3. `4B_IMPLEMENTATION`另需 `SDK_070_RUNTIME_USABLE`、`2-RTDS_CLOSED`、4A完成，以及用户对RTDS阶段1 Rust修改与本地构建/测试的新授权。其第一项工作才是按子计划动态证明官方SDK + 当前最小wrapper能完成真实teardown；该能力不是4A或4B入口的先验条件。
4. teardown能力证明失败时停止4B，不创建新runtime、不旁路raw WebSocket。参数尚未冻结时记录`RTDS_PARAMETERS_DEFERRED`，只阻断4A/4B，不阻断SDK、v3或其他拆分/审计。
5. RTDS authority、Rust实现和`polymarket_orderbook_v3` schema不得混在同一个diff/commit。

## 九、验收标准

### AC-01 SDK provenance 与单版本

workspace只解析“以官方 0.7.0 crate为底座的本地 `0.7.0-p1` vendor”，其 Cargo package version仍为 `0.7.0`；`third_party`中tracked Polymarket SDK vendor目录也只有该一个，旧`0.6.0-p1`目录不存在，active Cargo/code/probe对Polymarket SDK旧路径或其0.6精确版本的引用为0，历史文档记录不回写。crate checksum、VCS、MSRV、license、feature tree和四个允许生产源码差异路径 `src/ws/connection.rs`、`src/ws/config.rs`、`src/clob/ws/types/response.rs`、`src/rtds/subscription.rs` 全部可复核，无第五个vendor生产源码差异。P-B1为公开类型变化直接适配的vendor `tests/websocket.rs`和`examples/clob/ws/orderbook.rs`属于非生产conformance路径，必须单列，不能被错误计算为第五个生产patch，也不能扩张到其他vendor路径。

### AC-02 SDK 可用性

SDK upstream tests、patch contract、runtime contract、data-collect deterministic tests、clippy/fmt/check全部通过；无需 raw REST/WS、manual protocol或 shadow SDK，终态为 `SDK_070_RUNTIME_USABLE`。公开 smoke如获批准，只订阅公开 CLOB/RTDS/Gamma，不认证、不下单。

### AC-03 SDK 字段能力

七类 public market variant具有“官方实时文档字段/nullability -> SDK 0.7 typed field/parser结果 -> target admission -> v3 field -> 当前consumer/已批准合同”的矩阵。`book`、`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask`、`new_market`和`market_resolved`均用官方允许的missing/null fixture直接验证upstream 0.7与当前patch parser；P-B1后的永久fixture必须证明single object与array element都返回恰好一条typed message。nullable标量/文本保留`Option`，三个批准的nullable数组使用`DefaultOnNull -> empty Vec`，current v2兼容门在任何持久化或业务状态变化前fail closed。F04以nullable typed book快照字段保留；其present值进入canonical/raw-equivalent JSON，missing/null不改变旧current-v2 canonical/fingerprint，也不得产生独立成交行。F01–F03与F05–F18全部`EXCLUDED`且不进入未来v3字段集合。任一fixture、完整旧shape兼容门、consumer编译或current-v2零写入/零状态门失败时仍为`SDK_070_TYPED_CONTRACT_BLOCKED`；全部通过并完成独立审查、发布后，终态为`SDK_070_TYPED_CONTRACT_NONBLOCKING_GAPS_APPROVED`。不得用JSON旁路或`nullable_unavailable`掩盖整帧丢失。

### AC-04 v3 schema

用户批准字段矩阵后，实际Parquet schema/version证明没有`source_native_payload_json`和venue `hash`，nested list/struct类型、字段顺序、nullability、UTC timezone和decimal文本合同准确；共同envelope包含非空epoch、正数admission sequence和正数UTC毫秒admission time，满足`timestamp_received <= admission time`，writer按epoch内admission顺序拒绝重复或倒退但不要求gapless。metadata v4 round-trip证明metadata v3完整schema是不可变前缀，末尾只追加两个已批准nullable claimant字段；仅WS-owned `resolution_terminal_ws`同时非空，Gamma-owned terminal、post-terminal observation、snapshot、transition、summary及non-owning late WS同时为null，partial pair、空epoch或sequence=0均失败。批准的`nullable_unavailable`/排除项在schema与文档中一致，未知版本或损坏字段fail closed。

source v3 schema只包含项目canonical字段`clob_market_id`，不再包含同值顶层`id`；
golden fixture必须证明SDK `NewMarket.id`与`MarketResolved.id`在唯一入口映射后
field-for-field进入`clob_market_id`，而`market`仍保持condition ID，nested
`event_message.id`保持不变。

### AC-05 一条 target-admitted projection 一行

七类 fixture round-trip证明一条 target-admitted message projection对应一条 row；多 change `PriceChange`保留全部匹配 target entries的数量、原始相对顺序和 retained fields，不再 fan-out。mixed target/non-target fixture证明非目标 entry仍按现行采集范围过滤。`market_resolved`另以三类fixture证明：qualified target消息完整校验后写一条row；完全unmatched消息写0条且不改变状态；与目标任一identity key相关但market/condition/slug/assets/outcomes/winner任一矛盾时，在row admission、terminal claim和状态变化前fail closed并写0条。后者的既有 gap/error/coverage输出只含有界结构化诊断事实，不含 source frame、重序列化 JSON、payload digest或可恢复消息的等价字段集合。Gamma-first accepted drain中的合格late WS可写一条validated observation但无第二owner；role-exit后的消息写0条。

### AC-06 消费语义无损

除明确排除的 venue `hash`、非目标 entries及用户逐项批准的 SDK gap处置外，target admission后的所有当前消费合同字段 round-trip一致；`transaction_hash`按批准矩阵处理而不是被误删、伪造或从 authenticated channel错接。data-collect侧training/data-pipeline producer golden fixture证明下游所需字段可由canonical domain/v3 reader提供且不依赖被删列；本事项不实现或宣称真实data-pipeline adapter端到端通过。collector和该producer合同均不新增替代hash、semantic-key或强制duplicate drop。该结果不得被表述为过滤前SDK message或原始wire bytes无损。

### AC-07 restart/resolution

collector restart不恢复payload fingerprint；source v3唯一坐标固定为`(source_sequence_epoch_id, collector_admission_sequence)`，admission time只验证receive-before-admission因果顺序且不属于key，平台重复delivery各自取不同坐标，内部retry复用原坐标。production startup在持有dataset-root lease时先完成prior-day manifest preflight和current manifest reconcile，再调用只依赖metadata v4 nullable WS claimant pair、validated typed identity/winner/outcome和metadata authority的recovery；公开recovery API不接收manifest snapshot，也不得复用metadata v3字段、digest或模糊匹配。测试覆盖claimant source row+唯一WS terminal、多条agreeing platform duplicates+唯一claimant coordinate、无WS row或只有role-exit前validated late-WS+唯一Gamma terminal、0 source row+唯一合法exhausted summary及其attempts、validated source-only、validated row加exhausted、row receive晚于role-exit、terminal+summary和重复终态；前四类terminal/exhausted完成态合法，其余均fail closed且不补写。activation前只有source v2 + metadata v3可作为current runtime pair，activation后只有source v3 + metadata v4可作为current runtime pair；两种cross-pair、dual-write、running/unknown/corrupt组合均fail closed。unmatched、target-associated contradiction和post-horizon消息必须证明v3中没有row；旧`polymarket_orderbook_v2` reader只输出独立legacy row-grain audit view，不重建v3 canonical domain、message边界或source reference。W-B保留的offline legacy writer不能作为current pair或recovery authority。

### AC-08 training 与 future paper/live mapping边界

同一target-admitted SDK fixture经v3 round-trip后回到field-for-field、顺序一致的canonical domain；mixed/multi-change消息保持retained entries原始相对顺序。v2 legacy reader只输出独立`LegacyV2Row`或等价row-grain audit view，不适配回v3 canonical domain。future typed handoff为`NONBLOCKING_FUTURE_SPEC`：只记录当前可知retained字段及后继metadata join负责的nullable `price_to_beat`/`final_price`，未知future字段不构成v3失败。本事项不修改`HandoffFrame v1`或hot runtime，不能宣称paper/live parity；真实adapter、frame version/legacy compatibility、模型接线、长跑和部署另建三件套。

### AC-09 storage 与性能

Stage3创建、运行并独立审查唯一repo-local bounded measurement probe；它使用同一target-admitted logical projection序列生成v2 baseline与v3 candidate，证明两列及fan-out消失、v3 finalized bytes小于v2，并报告fixture版本/digest、compression、row-group、硬件、row/event、messages/s、CPU、peak RSS、finalize latency和全列scan。缓存回收只能报告为advisory，不能冒充已证明的物理cold scan。canonical-domain、resolution和manifest语义由同一Stage3正式contract tests/startup manifest测试组合证明，不在probe内实现shadow recovery或manifest simulator。现有完整UTC日只拥有post-dedupe accepted rows且无法无歧义恢复原message边界，因此只消费`data-collect/docs/sdk070-stage0-baseline.md`已冻结的v2 schema/storage/resource/manifest事实；不重扫、不生成v3 full-day语义对照。manifest中的`14,664`仅作不可恢复aggregate披露，不得宣称pre-dedupe replay或精确dedupe row/event差异。除bounded fixture的`v3 bytes < v2`、资源上限和语义正确性外，其他性能均为报告项；Stage6不得重新实现、重跑或重复评审该measurement，首次真实v3完整日前向长跑和production cutover另立三件套。

### AC-10 runtime 模块边界

root只保留 facade/config/composition；职责、测试和 visibility按唯一 owner分布，没有新巨石、generic framework、第二 feed owner或 shadow path。各模块及 root LOC和 moved/deleted/rewritten/added/net分别报告，但不作为固定数值门。

### AC-11 删除真实性

替代行为删除项具有 consumer matrix、唯一替代合同、定向回归和 diff证据；纯私有不可达项具有跨 features/targets/public API/scripts/legacy reader扫描、package compile/clippy、阶段回归和 diff证据，不为证明删除而新增抽象或测试 seam。仍有真实消费者的项目不得标为已证明删除。

### AC-12 RTDS

只有专项参数获批后才可单独授权4A authority同步；4A完成并停止后，必须另获4B Rust修改和本地构建/测试授权，才能按RTDS独立三件套完成其AC。未获批时保持当前行为并记录`RTDS_PARAMETERS_DEFERRED`，不阻塞SDK、v3或其他拆分/审计。

### AC-13 artifact 与 manifest

finalized file row count、bytes、schema version和 SHA-256与 manifest逐项一致；production验证owner是持有dataset-root lease的startup/manifest调用链，先完成prior-day preflight/current reconcile再调用recovery。recovery API不重复接收manifest snapshot。atomic rename、UTC day/hour、lease、writer/manifest failure和 shutdown不变量不回退。

### AC-14 全局内存与 permits

v3按 `size_of::<DomainMessage>() + Σ String::capacity() + Σ (Vec<T>::capacity() * size_of::<T>())`计算permit合同值，并递归计入Vec已初始化元素拥有的动态缓冲区且每个allocation只计一次；不得把Vec元素数当字节，也不声称这是allocator实际bytes或覆盖共享buffer/Arrow内部开销。预算表明确owner、上限、reservation存活期和释放点；move/borrow不重复计费，只有跨await/queue并与原对象并存的owned copy另取permits。现有全局/decoded-queue上限、1 MiB单行上限和2秒预算期限保持；exact-limit、oversize、nested book/price-change burst、channel closed、queue/writer阻塞、permit acquisition failure、admission gap、coordinated finalize和shutdown测试证明失败路径fail closed且permits最终回到基线。

### AC-15 授权边界

本组三件套已获总执行批准；用户也已批准P-B1 Rust/consumer/test/docs实施及最终字段矩阵（F04 `INCLUDED_OPTIONAL`，F01–F03与F05–F18 `EXCLUDED`）。2026-07-25用户进一步授予持续执行/发布授权，因此P-B1闭合且阶段3其余实质入口条件满足后，可直接实现未激活的v3 domain/schema/writer/reader、不依赖claimant pair的typed recovery及其测试，不再重复申请阶段3或commit/push授权；只有出现上下文/方案不一致才停止。只有metadata v4实现且WS claimant/recovery验收通过后，才可在本地代码/test的同一activation diff中把source v2 + metadata v3原子替换为唯一current runtime new-write pair source v3 + metadata v4，并让source v2与metadata v3退出current runtime new-write。W-B明确保留公开offline legacy writer用于历史fixture、兼容测试和bounded A/B；这不授权dual-write、production canonical-root写入或recovery authority。这仍不等于production binary激活、服务器dataset root/canonical new-data cutover、历史migration、旧数据删除、release或部署；这些第九节范围外事项不因持续授权自动纳入本计划。

## 十、停止条件

出现以下任一情况，必须停止对应阶段并向用户报告：

- 官方 SDK 0.7.0 缺少消费必需字段、拒绝官方允许的target相关missing/null帧，或需要第五个未批准 patch；
- 必须使用 raw REST/WS、manual signing或 shadow implementation 才能实现；
- 新 demand、现行 authority、代码语义或真实数据无法同时成立；
- typed schema 无法在删除 `hash` 后满足用户选择的消费语义（仅停止 v3相关工作）；
- in-scope training/resolution/manifest/day-rollover完整性只能通过保留`source_native_payload_json`或新增等价artifact JSON/digest才能成立；非阻断future mapping附件和当前out-of-scope `HandoffFrame v1.raw_json`不触发此停止条件；
- metadata v4只能靠修改既有metadata v3、payload digest、平台时间、物理行位置或模糊匹配实现（停止WS claimant/recovery并报告，不得偏离已批准方案）；
- metadata v4实现和WS claimant/recovery验收尚未完成时尝试激活source v3 + metadata v4 current new-write，或尝试cross-pair、dual-write、半切换（停止激活，但不阻断未激活的domain/schema/writer/reader、不依赖claimant pair的typed recovery及其测试）；
- 机械拆分需要先改变业务行为才能编译；
- 删除候选仍存在真实消费者或只能通过删除测试掩盖回归；
- v3 finalized bytes未小于v2、canonical round-trip或对应阶段回归未达门；`NONBLOCKING_FUTURE_SPEC`未完成只记录为后继附件缺口，不阻断v3。
